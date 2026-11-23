//! A small client for an S3-compatible bucket, such as Hetzner Object
//! Storage, which a node can keep its pieces in instead of a directory.
//!
//! It knows only what the piece store needs: putting an object, getting it
//! whole or a range of it, asking its length, and deleting many at once.
//! Requests are signed with AWS Signature Version 4, which every
//! S3-compatible store accepts, and address the bucket by path
//! (`<endpoint>/<bucket>/<key>`), which works without DNS names per bucket.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, Mac};
use md5::Md5;
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Method, StatusCode, Url};
use sha2::{Digest, Sha256};

/// The most keys one batch delete may name, by the S3 API.
pub const MAX_KEYS_PER_DELETE: usize = 1000;

/// Where a bucket is and the keys to it.
#[derive(Clone, Debug)]
pub struct BucketSettings {
    /// The store's address, such as `https://fsn1.your-objectstorage.com`.
    pub endpoint: String,
    /// The bucket's name.
    pub bucket: String,
    /// The region the signature names, such as `fsn1`.
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

/// A request to the bucket that did not go through.
#[derive(Debug, thiserror::Error)]
pub enum BucketError {
    #[error("the bucket could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("the bucket answered {status}: {body}")]
    Refused { status: StatusCode, body: String },
    #[error("the bucket's endpoint is not a URL with a host: {0}")]
    BadEndpoint(String),
    #[error("the bucket could not delete every object: {0}")]
    NotAllDeleted(String),
}

/// An S3-compatible bucket.
pub struct Bucket {
    http: reqwest::Client,
    settings: BucketSettings,
    /// The `Host` header requests carry, which the signature covers.
    host: String,
}

impl Bucket {
    /// The bucket `settings` describe. Nothing is sent until the first
    /// request.
    pub fn new(settings: BucketSettings) -> Result<Self, BucketError> {
        let url = Url::parse(&settings.endpoint)
            .map_err(|_| BucketError::BadEndpoint(settings.endpoint.clone()))?;
        let Some(host) = url.host_str() else {
            return Err(BucketError::BadEndpoint(settings.endpoint.clone()));
        };
        let host = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        Ok(Bucket {
            http: reqwest::Client::new(),
            settings,
            host,
        })
    }

    /// Stores `bytes` under `key`, replacing what was there.
    pub async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), BucketError> {
        let response = self
            .send(Method::PUT, key, "", bytes.to_vec(), Vec::new())
            .await?;
        expect_success(response).await?;
        Ok(())
    }

    /// The object `key`, or `None` when the bucket has none.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BucketError> {
        let response = self
            .send(Method::GET, key, "", Vec::new(), Vec::new())
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_success(response).await?;
        Ok(Some(response.bytes().await?.to_vec()))
    }

    /// `len` bytes of the object `key` from `start` on, fewer at its end, or
    /// `None` when the bucket has no such object. Only these bytes cross the
    /// network.
    pub async fn get_range(
        &self,
        key: &str,
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BucketError> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        // HTTP ranges name their last byte, not the one after it.
        let range = format!("bytes={start}-{}", start + len - 1);
        let response = self
            .send(Method::GET, key, "", Vec::new(), vec![("range", range)])
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_success(response).await?;
        Ok(Some(response.bytes().await?.to_vec()))
    }

    /// The length of the object `key`, or `None` when the bucket has none.
    pub async fn len_of(&self, key: &str) -> Result<Option<u64>, BucketError> {
        let response = self
            .send(Method::HEAD, key, "", Vec::new(), Vec::new())
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_success(response).await?;
        // `Response::content_length` describes the body, which a HEAD answer
        // has none of, so the header is read instead.
        let len = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        Ok(len)
    }

    /// Deletes every object in `keys`, [`MAX_KEYS_PER_DELETE`] to a
    /// request. Keys the bucket has no object for count as deleted.
    pub async fn delete_many(&self, keys: &[String]) -> Result<(), BucketError> {
        for batch in keys.chunks(MAX_KEYS_PER_DELETE) {
            self.delete_batch(batch).await?;
        }
        Ok(())
    }

    async fn delete_batch(&self, keys: &[String]) -> Result<(), BucketError> {
        // Keys are piece ids in hex, so they need no XML escaping.
        let objects: String = keys
            .iter()
            .map(|key| format!("<Object><Key>{key}</Key></Object>"))
            .collect();
        let body = format!("<Delete><Quiet>true</Quiet>{objects}</Delete>").into_bytes();
        // S3 refuses a batch delete without the body's MD5.
        let content_md5 = BASE64.encode(Md5::digest(&body));
        let headers = vec![("content-md5", content_md5)];
        let response = self
            .send(Method::POST, "", "delete=", body, headers)
            .await?;
        let answer = expect_success(response).await?.text().await?;
        // In quiet mode the answer lists only the keys that failed.
        if answer.contains("<Error>") {
            return Err(BucketError::NotAllDeleted(answer));
        }
        Ok(())
    }

    /// Sends a signed request for `key` in the bucket, or for the bucket
    /// itself when `key` is empty.
    async fn send(
        &self,
        method: Method,
        key: &str,
        query: &str,
        body: Vec<u8>,
        extra_headers: Vec<(&str, String)>,
    ) -> Result<reqwest::Response, BucketError> {
        let path = match key {
            "" => format!("/{}", self.settings.bucket),
            key => format!("/{}/{key}", self.settings.bucket),
        };
        let mut url = format!("{}{path}", self.settings.endpoint.trim_end_matches('/'));
        if !query.is_empty() {
            url = format!("{url}?{query}");
        }
        let payload_hash = hex::encode(Sha256::digest(&body));
        let mut headers: BTreeMap<String, String> = extra_headers
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect();
        headers.insert("host".into(), self.host.clone());
        headers.insert("x-amz-content-sha256".into(), payload_hash.clone());
        headers.insert("x-amz-date".into(), amz_date(unix_now()));
        let request = Signed {
            method: method.as_str(),
            path: &path,
            query,
            headers: &headers,
            payload_hash: &payload_hash,
        };
        let authorization = request.authorization(&self.settings);
        let mut builder = self.http.request(method, url).body(body);
        for (name, value) in &headers {
            // reqwest sets `Host` from the URL, the same value as signed.
            if name != "host" {
                builder = builder.header(name, value);
            }
        }
        Ok(builder
            .header("authorization", authorization)
            .send()
            .await?)
    }
}

/// `response` when it succeeded; otherwise the error it carries.
async fn expect_success(response: reqwest::Response) -> Result<reqwest::Response, BucketError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    Err(BucketError::Refused { status, body })
}

/// The parts of a request that Signature Version 4 covers.
struct Signed<'a> {
    method: &'a str,
    /// The path, already in the form it is sent in.
    path: &'a str,
    /// The query string, already in canonical form: names sorted, each with
    /// an `=`.
    query: &'a str,
    /// Every signed header, by lowercase name; `BTreeMap` keeps them sorted
    /// as the signature needs.
    headers: &'a BTreeMap<String, String>,
    payload_hash: &'a str,
}

impl Signed<'_> {
    /// The `Authorization` header for this request under `settings`' keys.
    fn authorization(&self, settings: &BucketSettings) -> String {
        let amz_date = self
            .headers
            .get("x-amz-date")
            .map(String::as_str)
            .unwrap_or_default();
        let date = &amz_date[..amz_date.len().min(8)];
        let scope = format!("{date}/{}/s3/aws4_request", settings.region);
        let signed_headers = self
            .headers
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers: String = self
            .headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect();
        let canonical_request = format!(
            "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
            self.method, self.path, self.query, self.payload_hash
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let key = format!("AWS4{}", settings.secret_key);
        let key = hmac(key.as_bytes(), date.as_bytes());
        let key = hmac(&key, settings.region.as_bytes());
        let key = hmac(&key, b"s3");
        let key = hmac(&key, b"aws4_request");
        let signature = hex::encode(hmac(&key, string_to_sign.as_bytes()));
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            settings.access_key
        )
    }
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    // HMAC takes keys of any length, so this cannot fail.
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// `seconds` since 1970 as Signature Version 4 writes times:
/// `20130524T000000Z`.
fn amz_date(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let in_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        in_day / 3600,
        in_day / 60 % 60,
        in_day % 60
    )
}

/// The calendar date `days` after 1970-01-01, by Howard Hinnant's
/// `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        // Only a clock set before 1970 fails here, and leases can't work on one.
        .expect("the system clock is after 1970")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example of AWS's Signature Version 4 documentation for S3: a
    /// ranged GET of `test.txt` in `examplebucket`.
    #[test]
    fn signs_like_the_aws_documentation() {
        let settings = BucketSettings {
            endpoint: "https://examplebucket.s3.amazonaws.com".into(),
            bucket: "examplebucket".into(),
            region: "us-east-1".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        };
        let empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers: BTreeMap<String, String> = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", empty_hash),
            ("x-amz-date", "20130524T000000Z"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        let request = Signed {
            method: "GET",
            path: "/test.txt",
            query: "",
            headers: &headers,
            payload_hash: empty_hash,
        };

        assert_eq!(
            request.authorization(&settings),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn writes_dates_as_signatures_want_them() {
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
        assert_eq!(amz_date(1_709_210_096), "20240229T123456Z");
    }
}

//! A stand-in for an S3-compatible object store, in this process, for testing
//! nodes that keep pieces in a bucket.
//!
//! It answers the requests `mesh_node::Bucket` sends: putting, getting whole
//! or by range, asking an object's length, and batch deletes. It checks that
//! every request is signed and that the signed payload hash is the body's,
//! and counts batch deletes. The signature itself is checked against AWS's
//! own example in `bucket.rs`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use mesh_node::BucketSettings;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;

/// What the fake store holds, by bucket and key.
#[derive(Default)]
pub struct Contents {
    pub objects: HashMap<(String, String), Vec<u8>>,
    /// How many batch deletes it was sent.
    pub batch_deletes: usize,
}

/// A running fake store.
pub struct FakeStore {
    pub endpoint: String,
    pub contents: Arc<Mutex<Contents>>,
}

impl FakeStore {
    /// Starts a store on a free port; it stops when the test ends.
    pub async fn start() -> Self {
        let contents = Arc::new(Mutex::new(Contents::default()));
        let app = Router::new()
            .route("/{bucket}", post(delete_batch))
            .route("/{bucket}/{key}", get(get_object).put(put_object))
            .with_state(contents.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        FakeStore { endpoint, contents }
    }

    /// Settings for the bucket `bucket` in this store.
    pub fn settings(&self, bucket: &str) -> BucketSettings {
        BucketSettings {
            endpoint: self.endpoint.clone(),
            bucket: bucket.into(),
            region: "fsn1".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
        }
    }

    /// How many objects the bucket `bucket` holds.
    pub fn object_count(&self, bucket: &str) -> usize {
        let contents = self.contents.lock().unwrap();
        contents
            .objects
            .keys()
            .filter(|(name, _)| name == bucket)
            .count()
    }
}

type Shared = State<Arc<Mutex<Contents>>>;

/// Whether a request is signed, with the body's hash as its signed payload
/// hash. S3 refuses requests that are not.
fn is_signed(headers: &HeaderMap, body: &[u8]) -> bool {
    let signed = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256 Credential=access/"));
    let payload_hash = headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok());
    signed && payload_hash == Some(hex::encode(Sha256::digest(body)).as_str())
}

async fn put_object(
    State(contents): Shared,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_signed(&headers, &body) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut contents = contents.lock().unwrap();
    contents.objects.insert((bucket, key), body.to_vec());
    StatusCode::OK.into_response()
}

/// Answers GET and, through axum, HEAD, with a `Range` of one span if asked.
async fn get_object(
    State(contents): Shared,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if !is_signed(&headers, &[]) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let contents = contents.lock().unwrap();
    let Some(object) = contents.objects.get(&(bucket, key)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let Some(range) = range.and_then(|range| range.strip_prefix("bytes=")) else {
        return object.clone().into_response();
    };
    let Some((first, last)) = range.split_once('-') else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (Ok(first), Ok(last)) = (first.parse::<usize>(), last.parse::<usize>()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if first >= object.len() {
        return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    }
    let end = (last + 1).min(object.len());
    (StatusCode::PARTIAL_CONTENT, object[first..end].to_vec()).into_response()
}

/// `POST /{bucket}?delete`: deletes every `<Key>` in the body.
async fn delete_batch(
    State(contents): Shared,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_signed(&headers, &body) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !headers.contains_key("content-md5") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let text = String::from_utf8_lossy(&body);
    let mut contents = contents.lock().unwrap();
    contents.batch_deletes += 1;
    for part in text.split("<Key>").skip(1) {
        let Some((key, _)) = part.split_once("</Key>") else {
            continue;
        };
        contents.objects.remove(&(bucket.clone(), key.to_string()));
    }
    "<DeleteResult></DeleteResult>".into_response()
}

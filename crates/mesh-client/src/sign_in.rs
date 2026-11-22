//! Signing up and signing in with an email and a password.
//!
//! The password never leaves the device. Argon2id stretches it under the
//! account's salt into an auth key, which the coordinator checks, and a
//! wrapping key, which seals the account's secret. The coordinator keeps the
//! sealed secret and hands it to whoever proves the auth key; only the
//! device that knows the password can open it. `mesh_core::sign_in`
//! describes the requests.

use argon2::{Algorithm, Argon2, Params, Version};
use mesh_core::sign_in::{
    ACCOUNTS_ROUTE, KdfParams, SALT_BYTES, SALT_ROUTE, SIGN_IN_ROUTE, SaltRequest, SaltResponse,
    SignInRequest, SignInResponse, SignUpRequest,
};
use reqwest::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::account::Account;
use crate::encryption::{MessageKey, open_message, seal_message};
use crate::node_client::NodeClient;

/// Why signing up or in failed.
#[derive(Debug, thiserror::Error)]
pub enum SignInError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("this email has an account already")]
    Taken,
    #[error("wrong email or password")]
    WrongPassword,
    #[error("the coordinator answered {status}: {message}")]
    Refused { status: StatusCode, message: String },
    #[error("the coordinator answered in a form this client does not understand")]
    UnexpectedAnswer,
    #[error("the coordinator asks for Argon2 settings this client cannot use")]
    BadKdf,
}

/// Creates an account for `email` on `coordinator`, holding `account`'s
/// secret locked under `password`, with Argon2 at `kdf`.
pub async fn sign_up(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    password: &str,
    account: &Account,
    kdf: KdfParams,
) -> Result<(), SignInError> {
    let mut salt = [0u8; SALT_BYTES];
    getrandom::fill(&mut salt).expect("the operating system's random source works");
    let (auth_key, wrapping_key) = stretch(password, &salt, kdf)?;
    let request = SignUpRequest {
        email: email.to_string(),
        salt,
        kdf,
        auth_key,
        locked_secret: seal_message(&wrapping_key, &account.secret()),
    };
    let response = post(client, coordinator, ACCOUNTS_ROUTE, &request).await?;
    match response.status() {
        status if status.is_success() => Ok(()),
        StatusCode::CONFLICT => Err(SignInError::Taken),
        _ => Err(refused(response).await),
    }
}

/// Signs in as `email` on `coordinator` and returns the account's secret.
pub async fn sign_in(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    password: &str,
) -> Result<Account, SignInError> {
    let (auth_key, wrapping_key) = credentials(client, coordinator, email, password).await?;
    let request = SignInRequest {
        email: email.to_string(),
        auth_key,
    };
    let response = post(client, coordinator, SIGN_IN_ROUTE, &request).await?;
    if response.status() == StatusCode::UNAUTHORIZED {
        return Err(SignInError::WrongPassword);
    }
    let answer: SignInResponse = read(response).await?;
    let secret = open_message(&wrapping_key, &answer.locked_secret)
        .map_err(|_| SignInError::UnexpectedAnswer)?;
    let secret: [u8; 32] = secret
        .try_into()
        .map_err(|_| SignInError::UnexpectedAnswer)?;
    Ok(Account::from_secret(secret))
}

/// The auth key of `email` with `password`, which other requests to the
/// coordinator, such as asking for tokens, prove the account with.
pub async fn auth_key(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    password: &str,
) -> Result<[u8; 32], SignInError> {
    let (auth_key, _) = credentials(client, coordinator, email, password).await?;
    Ok(auth_key)
}

/// The auth key and wrapping key of `email` with `password`, under the salt
/// the coordinator keeps.
async fn credentials(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    password: &str,
) -> Result<([u8; 32], MessageKey), SignInError> {
    let request = SaltRequest {
        email: email.to_string(),
    };
    let salt: SaltResponse = read(post(client, coordinator, SALT_ROUTE, &request).await?).await?;
    stretch(password, &salt.salt, salt.kdf)
}

/// The auth key and wrapping key `password` stretches into under `salt`.
fn stretch(
    password: &str,
    salt: &[u8; SALT_BYTES],
    kdf: KdfParams,
) -> Result<([u8; 32], MessageKey), SignInError> {
    // One lane: a phone has few cores to spare, and more lanes would only
    // help an attacker with many.
    let params = Params::new(kdf.memory_kib, kdf.iterations, 1, Some(64))
        .map_err(|_| SignInError::BadKdf)?;
    let mut stretched = [0u8; 64];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(password.as_bytes(), salt, &mut stretched)
        .map_err(|_| SignInError::BadKdf)?;
    let mut auth_key = [0u8; 32];
    let mut wrapping_key = [0u8; 32];
    auth_key.copy_from_slice(&stretched[..32]);
    wrapping_key.copy_from_slice(&stretched[32..]);
    Ok((auth_key, MessageKey::from_bytes(wrapping_key)))
}

async fn post(
    client: &NodeClient,
    coordinator: &str,
    route: &str,
    request: &impl Serialize,
) -> Result<reqwest::Response, SignInError> {
    let url = format!("{}{route}", coordinator.trim_end_matches('/'));
    // The requests are plain strings and byte arrays, which always serialize.
    let body = serde_json::to_vec(request).expect("a sign-in request serializes");
    Ok(client
        .http()
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?)
}

async fn read<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, SignInError> {
    if !response.status().is_success() {
        return Err(refused(response).await);
    }
    let body = response.bytes().await?;
    serde_json::from_slice(&body).map_err(|_| SignInError::UnexpectedAnswer)
}

async fn refused(response: reqwest::Response) -> SignInError {
    let status = response.status();
    // The reason is for people reading the error; a body that can't be read
    // only loses that.
    let message = response.text().await.unwrap_or_default();
    SignInError::Refused { status, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHEAP: KdfParams = KdfParams {
        memory_kib: 8,
        iterations: 1,
    };

    #[test]
    fn the_same_password_and_salt_stretch_the_same_way() {
        let (auth, wrapping) = stretch("correct horse", &[1; SALT_BYTES], CHEAP).unwrap();
        let (auth_again, wrapping_again) =
            stretch("correct horse", &[1; SALT_BYTES], CHEAP).unwrap();

        assert_eq!(auth, auth_again);
        let sealed = seal_message(&wrapping, b"secret");
        assert_eq!(open_message(&wrapping_again, &sealed).unwrap(), b"secret");
    }

    #[test]
    fn another_password_or_salt_stretches_differently() {
        let (auth, _) = stretch("correct horse", &[1; SALT_BYTES], CHEAP).unwrap();
        let (other_password, _) = stretch("battery staple", &[1; SALT_BYTES], CHEAP).unwrap();
        let (other_salt, _) = stretch("correct horse", &[2; SALT_BYTES], CHEAP).unwrap();

        assert_ne!(auth, other_password);
        assert_ne!(auth, other_salt);
    }
}

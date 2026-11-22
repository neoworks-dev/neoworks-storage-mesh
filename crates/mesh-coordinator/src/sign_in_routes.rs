//! Signing up and signing in over HTTP; `mesh_core::sign_in` describes the
//! requests.

use std::net::{IpAddr, SocketAddr};

use axum::extract::{ConnectInfo, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use mesh_core::sign_in::{
    ACCOUNTS_ROUTE, MAX_LOCKED_SECRET_BYTES, SALT_ROUTE, SIGN_IN_ROUTE, SaltRequest, SaltResponse,
    SignInRequest, SignInResponse, SignUpRequest,
};

use crate::accounts::SignedUp;
use crate::clock::unix_now;
use crate::routes::Coordinator;

/// The longest email address there is, by RFC 5321.
const MAX_EMAIL_BYTES: usize = 254;

/// The sign-in routes.
pub fn routes() -> Router<Coordinator> {
    Router::new()
        .route(ACCOUNTS_ROUTE, post(sign_up))
        .route(SALT_ROUTE, post(salt))
        .route(SIGN_IN_ROUTE, post(sign_in))
}

/// `POST /accounts`: creates an account.
async fn sign_up(
    State(coordinator): State<Coordinator>,
    Json(request): Json<SignUpRequest>,
) -> Response {
    if !is_email(&request.email) {
        return (StatusCode::BAD_REQUEST, "not an email address").into_response();
    }
    // The secret is stored as given; without a limit, sign-ups could fill
    // the coordinator's disk.
    if request.locked_secret.len() > MAX_LOCKED_SECRET_BYTES {
        let reason = format!("a locked secret has at most {MAX_LOCKED_SECRET_BYTES} bytes");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    match coordinator.accounts.sign_up(&request).await {
        Ok(SignedUp::Created) => StatusCode::CREATED.into_response(),
        Ok(SignedUp::Taken) => (StatusCode::CONFLICT, "this email has an account").into_response(),
        Err(error) => internal_error(error),
    }
}

/// `POST /sign-in/salt`: the salt and cost to stretch the password with.
async fn salt(
    State(coordinator): State<Coordinator>,
    Json(request): Json<SaltRequest>,
) -> Response {
    match coordinator.accounts.salt(&request.email).await {
        Ok((salt, kdf)) => Json(SaltResponse { salt, kdf }).into_response(),
        Err(error) => internal_error(error),
    }
}

/// `POST /sign-in`: the locked secret, for the right auth key.
async fn sign_in(
    State(coordinator): State<Coordinator>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<SignInRequest>,
) -> Response {
    let checked = check_sign_in(
        &coordinator,
        address.ip(),
        &request.email,
        &request.auth_key,
    )
    .await;
    match checked {
        Ok(locked_secret) => Json(SignInResponse { locked_secret }).into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

/// Why a sign-in was refused.
pub enum SignInRefusal {
    /// Too many failed sign-ins in a row; the email or address may try again
    /// in this many seconds.
    Locked(u64),
    /// The email has no account or the auth key is not the account's. The two
    /// are one case, so no answer tells which emails have accounts.
    WrongKey,
    Database(rusqlite::Error),
}

impl IntoResponse for SignInRefusal {
    fn into_response(self) -> Response {
        match self {
            SignInRefusal::Locked(seconds) => {
                let reason = format!("too many failed sign-ins; try again in {seconds} seconds");
                let retry_after = [(header::RETRY_AFTER, seconds.to_string())];
                (StatusCode::TOO_MANY_REQUESTS, retry_after, reason).into_response()
            }
            SignInRefusal::WrongKey => {
                (StatusCode::UNAUTHORIZED, "wrong email or password").into_response()
            }
            SignInRefusal::Database(error) => internal_error(error),
        }
    }
}

/// The locked secret of `email` when `auth_key` is the account's. Every route
/// that takes an auth key checks it here, so none of them lets anyone guess
/// past the limits of [`FailedSignIns`].
///
/// [`FailedSignIns`]: crate::sign_in_limits::FailedSignIns
pub async fn check_sign_in(
    coordinator: &Coordinator,
    address: IpAddr,
    email: &str,
    auth_key: &[u8; 32],
) -> Result<Vec<u8>, SignInRefusal> {
    let now = unix_now();
    // Held until the attempt is counted: otherwise a guesser sending many
    // attempts at once would have them all checked before the first failure
    // counted. Accounts are checked one at a time anyway.
    let mut failed_sign_ins = coordinator.failed_sign_ins.lock().await;
    // Checked before the key, so a locked guesser learns nothing from a
    // guess, not even that it was right.
    if let Some(seconds) = failed_sign_ins.wait(email, address, now) {
        return Err(SignInRefusal::Locked(seconds));
    }
    let held = coordinator
        .accounts
        .sign_in(email, auth_key)
        .await
        .map_err(SignInRefusal::Database)?;
    let Some(locked_secret) = held else {
        failed_sign_ins.record_failure(email, address, now);
        return Err(SignInRefusal::WrongKey);
    };
    failed_sign_ins.record_success(email);
    Ok(locked_secret)
}

/// Whether `email` could be an address: short enough, with something on
/// both sides of an `@`. Whether it receives mail is not checked here.
fn is_email(email: &str) -> bool {
    let Some((local, domain)) = email.trim().split_once('@') else {
        return false;
    };
    email.len() <= MAX_EMAIL_BYTES && !local.is_empty() && domain.contains('.')
}

/// A database error: logged on the coordinator, answered without detail.
fn internal_error(error: rusqlite::Error) -> Response {
    eprintln!("accounts: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

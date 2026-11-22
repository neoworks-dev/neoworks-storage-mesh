//! Signing up and signing in over HTTP; `mesh_core::sign_in` describes the
//! requests.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use mesh_core::sign_in::{
    ACCOUNTS_ROUTE, MAX_LOCKED_SECRET_BYTES, SALT_ROUTE, SIGN_IN_ROUTE, SaltRequest, SaltResponse,
    SignInRequest, SignInResponse, SignUpRequest,
};

use crate::accounts::SignedUp;
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
    Json(request): Json<SignInRequest>,
) -> Response {
    match coordinator
        .accounts
        .sign_in(&request.email, &request.auth_key)
        .await
    {
        Ok(Some(locked_secret)) => Json(SignInResponse { locked_secret }).into_response(),
        // The same answer for an unknown email and a wrong password, so it
        // does not tell which emails have accounts.
        Ok(None) => (StatusCode::UNAUTHORIZED, "wrong email or password").into_response(),
        Err(error) => internal_error(error),
    }
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

//! Filing and looking up mailbox keys over HTTP; `mesh_core::mailbox`
//! describes the requests and [`crate::mailbox_keys`] what is kept.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use mesh_core::mailbox::{MAILBOX_KEYS_ROUTE, MailboxKey, MailboxKeyQuery, PublishMailboxKey};
use mesh_core::sign_in::normalize_email;
use mesh_core::signed::Signed;

use crate::routes::Coordinator;

/// The mailbox key routes.
pub fn routes() -> Router<Coordinator> {
    Router::new().route(MAILBOX_KEYS_ROUTE, put(publish).get(look_up))
}

/// `PUT /mailbox-keys`: files a signed-in account's mailbox key.
async fn publish(
    State(coordinator): State<Coordinator>,
    Json(request): Json<PublishMailboxKey>,
) -> Response {
    let signed_in = coordinator
        .accounts
        .sign_in(&request.email, &request.auth_key)
        .await;
    match signed_in {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::UNAUTHORIZED, "wrong email or password").into_response(),
        Err(error) => return internal_error(error),
    }
    match coordinator
        .mailbox_keys
        .publish(&request.email, &request.public_key)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => internal_error(error),
    }
}

/// `GET /mailbox-keys?email=E`: the mailbox key of `E`, signed.
async fn look_up(
    State(coordinator): State<Coordinator>,
    Query(query): Query<MailboxKeyQuery>,
) -> Response {
    match coordinator.mailbox_keys.key_of(&query.email).await {
        Ok(public_key) => {
            let key = MailboxKey {
                email: normalize_email(&query.email),
                public_key,
            };
            Json(Signed::sign(&coordinator.key, key)).into_response()
        }
        Err(error) => internal_error(error),
    }
}

/// A database error: logged on the coordinator, answered without detail.
fn internal_error(error: rusqlite::Error) -> Response {
    eprintln!("mailbox keys: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

//! Filing and looking up mailbox keys over HTTP; `mesh_core::mailbox`
//! describes the requests and [`crate::mailbox_keys`] what is kept.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use mesh_core::mailbox::{MAILBOX_KEYS_ROUTE, MailboxKey, MailboxKeyQuery, PublishMailboxKey};
use mesh_core::sign_in::normalize_email;
use mesh_core::signed::Signed;

use crate::routes::Coordinator;
use crate::sign_in_routes::check_sign_in;

/// The mailbox key routes.
pub fn routes() -> Router<Coordinator> {
    Router::new().route(MAILBOX_KEYS_ROUTE, put(publish).get(look_up))
}

/// `PUT /mailbox-keys`: files a signed-in account's mailbox key.
async fn publish(
    State(coordinator): State<Coordinator>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<PublishMailboxKey>,
) -> Response {
    // The route answers whether an auth key is right, so its failures count
    // toward the same lock as sign-ins; otherwise it would be a way around it.
    let checked = check_sign_in(
        &coordinator,
        address.ip(),
        &request.email,
        &request.auth_key,
    )
    .await;
    if let Err(refusal) = checked {
        return refusal.into_response();
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

//! Mailboxes over HTTP; `mesh_core::mailbox` describes the requests.
//!
//! Mailboxes are free like pointers, so the node bounds them instead: a
//! message is small, a mailbox holds a limited number, all mailboxes
//! together get [`NodeSettings::mailbox_bytes`](crate::NodeSettings), every
//! message has to carry proof of work, and one sending address leaves only
//! so many messages in one contact mailbox a week
//! ([`mailbox_senders`](crate::mailbox_senders)).

use std::net::SocketAddr;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::put;
use mesh_core::mailbox::{
    CONTACT_MAILBOX_ROUTE, MAILBOX_ROUTE, MAX_MESSAGE_BYTES, MAX_MESSAGES_PER_SENDER,
    MESSAGE_LEASE_SECONDS, MailboxContents, MailboxKind, WorkQuery, week_of, work_holds,
};
use mesh_core::tokens::HexBytes;

use crate::clock::unix_now;
use crate::mailbox_store::Appended;
use crate::routes::Node;

/// The mailbox routes.
pub(crate) fn routes() -> Router<Node> {
    Router::new()
        .route(MAILBOX_ROUTE, put(append_personal).get(read_personal))
        .route(CONTACT_MAILBOX_ROUTE, put(append_contact).get(read_contact))
}

/// `PUT /mailboxes/{address}?week=W&nonce=N`: appends the body to a
/// personal mailbox, when it carries enough work.
async fn append_personal(
    State(node): State<Node>,
    Path(address): Path<String>,
    Query(query): Query<WorkQuery>,
    body: Bytes,
) -> Response {
    let Some(address) = parse_address(&address) else {
        return bad_address();
    };
    if let Some(reason) = work_refusal(MailboxKind::Personal, &address, &query, &body) {
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    append(&node, MailboxKind::Personal, &address, &body).await
}

/// `PUT /contact-mailboxes/{address}?week=W&nonce=N`: appends the body to a
/// contact mailbox, when it carries the contact work and its sender has not
/// left its share of messages there this week.
async fn append_contact(
    State(node): State<Node>,
    ConnectInfo(sender): ConnectInfo<SocketAddr>,
    Path(address): Path<String>,
    Query(query): Query<WorkQuery>,
    body: Bytes,
) -> Response {
    let Some(address) = parse_address(&address) else {
        return bad_address();
    };
    if let Some(reason) = work_refusal(MailboxKind::Contact, &address, &query, &body) {
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    // Held until the message is counted, so a sender can't get many
    // messages past the limit by sending them all at once.
    let mut senders = node.contact_senders.lock().await;
    if !senders.has_room(&address, sender.ip(), week_of(unix_now())) {
        let reason = format!(
            "at most {MAX_MESSAGES_PER_SENDER} messages a week from one address to one mailbox"
        );
        return (StatusCode::TOO_MANY_REQUESTS, reason).into_response();
    }
    let response = append(&node, MailboxKind::Contact, &address, &body).await;
    if response.status().is_success() {
        senders.record(&address, sender.ip());
    }
    response
}

/// Why `query` is not enough work for `body` to the mailbox of `kind` at
/// `address`, or `None` when it is.
fn work_refusal(
    kind: MailboxKind,
    address: &[u8; 32],
    query: &WorkQuery,
    body: &[u8],
) -> Option<&'static str> {
    // Last week's too, so a message sent as the week turns is not refused.
    // The work covers the week, so it can't be carried into a later one.
    let this_week = week_of(unix_now());
    if query.week != this_week && query.week + 1 != this_week {
        return Some("the week is not this week or last");
    }
    if !work_holds(kind, address, body, query.week, query.nonce) {
        return Some("the nonce is not enough work for this message");
    }
    None
}

async fn append(node: &Node, kind: MailboxKind, address: &[u8; 32], body: &[u8]) -> Response {
    if body.len() > MAX_MESSAGE_BYTES {
        let reason = format!("a message has at most {MAX_MESSAGE_BYTES} bytes");
        return (StatusCode::PAYLOAD_TOO_LARGE, reason).into_response();
    }
    let expires_at = unix_now() + MESSAGE_LEASE_SECONDS;
    let appended = node
        .mailboxes
        .append(kind, address, body, expires_at, node.mailbox_bytes)
        .await;
    match appended {
        Ok(Appended::Held) => StatusCode::NO_CONTENT.into_response(),
        Ok(Appended::MailboxFull) => {
            (StatusCode::INSUFFICIENT_STORAGE, "the mailbox is full").into_response()
        }
        Ok(Appended::NodeFull) => (
            StatusCode::INSUFFICIENT_STORAGE,
            "this node has no room for more messages",
        )
            .into_response(),
        Err(error) => database_error(error),
    }
}

/// `GET /mailboxes/{address}`: the messages in a personal mailbox.
async fn read_personal(State(node): State<Node>, Path(address): Path<String>) -> Response {
    read(&node, MailboxKind::Personal, &address).await
}

/// `GET /contact-mailboxes/{address}`: the messages in a contact mailbox.
async fn read_contact(State(node): State<Node>, Path(address): Path<String>) -> Response {
    read(&node, MailboxKind::Contact, &address).await
}

async fn read(node: &Node, kind: MailboxKind, address: &str) -> Response {
    let Some(address) = parse_address(address) else {
        return bad_address();
    };
    match node.mailboxes.messages(kind, &address, unix_now()).await {
        Ok(bodies) => {
            let messages = bodies.into_iter().map(HexBytes).collect();
            Json(MailboxContents { messages }).into_response()
        }
        Err(error) => database_error(error),
    }
}

/// The 32 bytes an address of 64 hex digits names.
fn parse_address(address: &str) -> Option<[u8; 32]> {
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(address, &mut bytes).ok()?;
    Some(bytes)
}

fn bad_address() -> Response {
    let reason = "a mailbox address is 64 hex digits";
    (StatusCode::BAD_REQUEST, reason).into_response()
}

/// A mailbox table error: logged on the node, answered without detail.
fn database_error(error: rusqlite::Error) -> Response {
    eprintln!("mailboxes: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

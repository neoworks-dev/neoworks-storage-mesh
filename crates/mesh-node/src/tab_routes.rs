//! Paying for storage with tabs over HTTP; `mesh_core::tabs` describes the
//! requests.
//!
//! The node takes tokens on trust only after spending them at the
//! coordinator: a token that was forged, is from an ended month or was spent
//! before comes back refused there, so the node needs no token keys itself.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use mesh_core::protocol::{BatchRenewRequest, BatchRenewResponse, MAX_BATCH_PIECES};
use mesh_core::tabs::{
    MAX_TOKENS_PER_REQUEST, OpenTabRequest, TAB_PIECES_ROUTE, TAB_ROUTE, is_tab_name,
};
use mesh_core::tokens::{MONTH_SECONDS, SPEND_ROUTE, SpendResponse, TOKEN_BYTES, Token};

use crate::clock::unix_now;
use crate::routes::{Node, internal_error};
use crate::tab_book::ChargeRefused;

/// The tab routes.
pub(crate) fn routes() -> Router<Node> {
    Router::new()
        .route(TAB_ROUTE, post(pay_tab))
        .route(TAB_PIECES_ROUTE, post(move_into_tab))
}

/// `POST /tabs/{name}`: spends the tokens in the body and adds them to the
/// tab.
async fn pay_tab(
    State(node): State<Node>,
    Path(name): Path<String>,
    Json(request): Json<OpenTabRequest>,
) -> Response {
    if !is_tab_name(&name) {
        return (
            StatusCode::BAD_REQUEST,
            "a tab name is 32 lowercase hex digits",
        )
            .into_response();
    }
    if request.tokens.len() > MAX_TOKENS_PER_REQUEST {
        let reason = format!("at most {MAX_TOKENS_PER_REQUEST} tokens per request");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let Some(coordinator) = &node.token_coordinator else {
        return (StatusCode::NOT_FOUND, "this node takes no tokens").into_response();
    };
    let mut paid = 0;
    for token in &request.tokens {
        if spend(&node.http, coordinator, token).await {
            paid += 1;
        }
    }
    if paid == 0 {
        return (
            StatusCode::PAYMENT_REQUIRED,
            "no token was valid and unspent",
        )
            .into_response();
    }
    let now = unix_now();
    match node
        .tabs
        .pay(&name, paid * TOKEN_BYTES, now + MONTH_SECONDS, now)
        .await
    {
        Ok(tab) => Json(tab).into_response(),
        Err(error) => {
            eprintln!("tab {name}: {error}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Whether `token` was valid and is now spent at `coordinator`. A token the
/// coordinator could not be asked about counts as unpaid; the client keeps
/// it and may try again.
async fn spend(http: &reqwest::Client, coordinator: &str, token: &Token) -> bool {
    let url = format!("{}{SPEND_ROUTE}", coordinator.trim_end_matches('/'));
    // A token is plain integers and bytes, which always serialize.
    let body = serde_json::to_vec(token).expect("a token serializes");
    let Ok(response) = http
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
    else {
        return false;
    };
    let Ok(answer) = response.bytes().await else {
        return false;
    };
    serde_json::from_slice::<SpendResponse>(&answer).is_ok_and(|answer| answer.spent_now)
}

/// `POST /tabs/{name}/pieces`: moves the pieces in the body into the tab,
/// which is how pieces are renewed on a node that requires tokens.
async fn move_into_tab(
    State(node): State<Node>,
    Path(name): Path<String>,
    Json(request): Json<BatchRenewRequest>,
) -> Response {
    if request.pieces.len() > MAX_BATCH_PIECES {
        let reason = format!("a batch moves at most {MAX_BATCH_PIECES} pieces");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let now = unix_now();
    let mut lease_ends = Vec::with_capacity(request.pieces.len());
    for id in &request.pieces {
        let len = match node.store.len_of(id, now).await {
            Ok(Some(len)) => len,
            Ok(None) => {
                lease_ends.push(None);
                continue;
            }
            Err(error) => return internal_error(id, error),
        };
        let expires_at = match charge(&node, &name, len).await {
            Ok(expires_at) => expires_at,
            Err(refusal) => return refusal.into_response(),
        };
        match node.store.renew(id, expires_at, now).await {
            Ok(lease_end) => lease_ends.push(lease_end),
            Err(error) => return internal_error(id, error),
        }
    }
    Json(BatchRenewResponse { lease_ends }).into_response()
}

/// Charges `len` bytes to the tab `name` and returns when it ends, or the
/// status and reason that refuse the charge.
pub(crate) async fn charge(node: &Node, name: &str, len: u64) -> Result<u64, (StatusCode, String)> {
    let refused = |status, reason: &str| Err((status, reason.to_string()));
    match node.tabs.charge(name, len, unix_now()).await {
        Ok(Ok(expires_at)) => Ok(expires_at),
        Ok(Err(ChargeRefused::NoTab)) => {
            refused(StatusCode::PAYMENT_REQUIRED, "no open tab of that name")
        }
        Ok(Err(ChargeRefused::Full)) => {
            refused(StatusCode::INSUFFICIENT_STORAGE, "the tab is full")
        }
        Err(error) => {
            eprintln!("tab {name}: {error}");
            refused(StatusCode::INTERNAL_SERVER_ERROR, "")
        }
    }
}

//! Token keys, issuing and spending over HTTP; `mesh_core::tokens` describes
//! the requests.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_core::NodeId;
use mesh_core::signed::Signed;
use mesh_core::tokens::{
    ISSUE_ROUTE, IssueRequest, IssueResponse, MAX_ISSUE_PER_REQUEST, MONTH_TOKEN_KEY_ROUTE,
    SPEND_ROUTE, SpendRequest, SpendResponse, TOKEN_KEY_ROUTE, TokenKey, TokenKind, month_of,
};

use crate::clock::unix_now;
use crate::routes::Coordinator;
use crate::token_issuer::{IssueError, Spent};

/// The token routes.
pub fn routes() -> Router<Coordinator> {
    Router::new()
        .route(TOKEN_KEY_ROUTE, get(current_key))
        .route(MONTH_TOKEN_KEY_ROUTE, get(month_key))
        .route(ISSUE_ROUTE, post(issue))
        .route(SPEND_ROUTE, post(spend))
}

/// `GET /tokens/key/{kind}`: this month's signed key of that kind.
async fn current_key(
    State(coordinator): State<Coordinator>,
    Path(kind): Path<TokenKind>,
) -> Response {
    signed_key(&coordinator, month_of(unix_now()), kind).await
}

/// `GET /tokens/key/{kind}/{month}`: that month's signed key of that kind,
/// while its tokens are valid.
async fn month_key(
    State(coordinator): State<Coordinator>,
    Path((kind, month)): Path<(TokenKind, u64)>,
) -> Response {
    let current = month_of(unix_now());
    // Only months whose tokens can be spent get a key; asking for others
    // would make the coordinator generate keys for any month anyone names.
    if month != current && month + 1 != current {
        return (StatusCode::NOT_FOUND, "no valid tokens for that month").into_response();
    }
    signed_key(&coordinator, month, kind).await
}

async fn signed_key(coordinator: &Coordinator, month: u64, kind: TokenKind) -> Response {
    match coordinator.tokens.public_key(month, kind).await {
        Ok(public_key) => {
            let key = TokenKey {
                month,
                kind,
                public_key,
            };
            Json(Signed::sign(&coordinator.key, key)).into_response()
        }
        Err(error) => internal_error(error),
    }
}

/// `POST /tokens/issue`: blind signatures for a signed-in account.
async fn issue(
    State(coordinator): State<Coordinator>,
    Json(request): Json<IssueRequest>,
) -> Response {
    if request.month != month_of(unix_now()) {
        return (
            StatusCode::BAD_REQUEST,
            "tokens are issued for the current month",
        )
            .into_response();
    }
    if request.blinded.len() > MAX_ISSUE_PER_REQUEST {
        let reason = format!("at most {MAX_ISSUE_PER_REQUEST} tokens per request");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    match coordinator
        .accounts
        .sign_in(&request.email, &request.auth_key)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return (StatusCode::UNAUTHORIZED, "wrong email or password").into_response(),
        Err(error) => return internal_error(error.into()),
    }
    let issued = coordinator
        .tokens
        .issue(
            &request.email,
            request.month,
            request.kind,
            &request.blinded,
            coordinator.monthly_allowance,
        )
        .await;
    match issued {
        Ok(blind_signatures) => Json(IssueResponse { blind_signatures }).into_response(),
        Err(error @ (IssueError::OverAllowance { .. } | IssueError::OverBalance { .. })) => {
            (StatusCode::FORBIDDEN, error.to_string()).into_response()
        }
        Err(error @ IssueError::BadBlindMessage) => {
            (StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
        Err(error) => internal_error(error),
    }
}

/// `POST /tokens/spend`: spends a token once, for the node that signed the
/// request.
async fn spend(
    State(coordinator): State<Coordinator>,
    Json(request): Json<Signed<SpendRequest>>,
) -> Response {
    // Without the node's signature, anyone holding a token could spend it in
    // a node's name, and allowance tokens would reach other nodes.
    if request.verify().is_err() || NodeId::of(&request.signer) != request.body.node_id {
        let reason = "the request is not signed by the node it names";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let SpendRequest { node_id, token } = &request.body;
    // The allowance is storage on the coordinator's own nodes; other nodes
    // must not be able to collect it.
    if token.kind == TokenKind::Allowance && !coordinator.own_nodes.contains(node_id) {
        let reason = "allowance tokens are only taken by the coordinator's own nodes";
        return (StatusCode::FORBIDDEN, reason).into_response();
    }
    match coordinator.tokens.spend(token, month_of(unix_now())).await {
        Ok(Spent::Now) => Json(SpendResponse { spent_now: true }).into_response(),
        Ok(Spent::Before) => Json(SpendResponse { spent_now: false }).into_response(),
        Ok(Spent::Invalid) => (StatusCode::BAD_REQUEST, "not a valid token").into_response(),
        Err(error) => internal_error(error),
    }
}

/// A key or database error: logged on the coordinator, answered without
/// detail.
fn internal_error(error: IssueError) -> Response {
    eprintln!("tokens: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

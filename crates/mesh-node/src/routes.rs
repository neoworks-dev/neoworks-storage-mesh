//! The node's HTTP interface: storing, renewing, fetching pieces and proving
//! the node still holds them, and storing and fetching signed pointers.
//! Paying for storage with tabs is in [`tab_routes`](crate::tab_routes),
//! mailboxes in [`mailbox_routes`](crate::mailbox_routes), and speed tests
//! and usage are in [`speed_test`](crate::speed_test) and
//! [`usage`](crate::usage).
//!
//! `mesh_core::protocol` describes the requests; this module checks them and
//! maps them onto the [`PieceStore`] and the [`PointerStore`].

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use mesh_core::pointer::{Pointer, parse_owner};
use mesh_core::protocol::{
    BATCH_LEASE_ROUTE, BatchRenewRequest, BatchRenewResponse, CHALLENGE_ROUTE, ChallengeRequest,
    ChallengeResponse, IDENTITY_ROUTE, LEASE_ROUTE, LeaseQuery, LeaseResponse, MAX_BATCH_PIECES,
    MAX_CHALLENGE_SEGMENTS, MAX_PIECE_BYTES, NodeIdentity, PIECE_ROUTE, POINTER_ROUTE,
    PointerConflict,
};
use mesh_core::signed::SigningKey;
use mesh_core::tabs::StoreQuery;
use mesh_core::{NodeId, PieceId, merkle};

use crate::clock::unix_now;
use crate::mailbox_routes;
use crate::mailbox_store::MailboxStore;
use crate::own_price::OwnPrice;
use crate::piece_store::{PieceStore, StoreError};
use crate::pointer_store::{PointerPut, PointerStore, PointerStoreError};
use crate::settings::NodeSettings;
use crate::speed_test::{self, SpeedTests};
use crate::tab_book::TabBook;
use crate::tab_routes;
use crate::usage::{self, UsageCount};

/// What every request handler needs.
#[derive(Clone)]
pub(crate) struct Node {
    pub(crate) store: Arc<PieceStore>,
    pub(crate) pointers: Arc<PointerStore>,
    pub(crate) tabs: Arc<TabBook>,
    pub(crate) mailboxes: Arc<MailboxStore>,
    /// The bytes all mailbox messages together may take.
    pub(crate) mailbox_bytes: u64,
    pub(crate) identity: Arc<NodeIdentity>,
    pub(crate) max_lease_seconds: u64,
    /// The coordinator tokens are spent at, when the node requires them.
    pub(crate) token_coordinator: Option<String>,
    /// The node's price there, when the node requires tokens.
    pub(crate) price: Option<Arc<OwnPrice>>,
    /// Signs the node's token spends, so the coordinator knows which node
    /// spent them.
    pub(crate) key: SigningKey,
    /// For spending tokens at the coordinator.
    pub(crate) http: reqwest::Client,
    pub(crate) speed_tests: Arc<SpeedTests>,
    pub(crate) usage: Arc<UsageCount>,
}

/// The router of the node with `key`, serving `store`, `pointers`, `tabs`
/// and `mailboxes`.
///
/// Ids that are not 64 lowercase hex digits and requests without
/// `lease_seconds` are refused by the extractors with `400 Bad Request` before
/// any handler runs.
pub fn router(
    store: Arc<PieceStore>,
    pointers: Arc<PointerStore>,
    tabs: Arc<TabBook>,
    mailboxes: Arc<MailboxStore>,
    key: SigningKey,
    settings: &NodeSettings,
) -> Router {
    let public_key = key.verifying_key().to_bytes();
    let identity = NodeIdentity {
        node_id: NodeId::of(&public_key),
        public_key,
    };
    let http = reqwest::Client::new();
    let price = settings.require_tokens.as_ref().map(|coordinator| {
        Arc::new(OwnPrice::new(
            coordinator.clone(),
            identity.node_id,
            http.clone(),
        ))
    });
    let node = Node {
        store,
        pointers,
        tabs,
        mailboxes,
        mailbox_bytes: settings.mailbox_bytes,
        identity: Arc::new(identity),
        max_lease_seconds: settings.max_lease.as_secs(),
        token_coordinator: settings.require_tokens.clone(),
        price,
        key,
        http,
        speed_tests: Arc::new(SpeedTests::new()),
        usage: Arc::new(UsageCount::default()),
    };
    Router::new()
        .route(PIECE_ROUTE, put(put_piece).get(get_piece))
        .route(LEASE_ROUTE, post(renew_lease))
        .route(BATCH_LEASE_ROUTE, post(renew_leases))
        .route(CHALLENGE_ROUTE, post(answer_challenge))
        .route(POINTER_ROUTE, put(put_pointer).get(get_pointer))
        .route(IDENTITY_ROUTE, get(get_identity))
        .merge(tab_routes::routes())
        .merge(mailbox_routes::routes())
        .merge(speed_test::routes())
        .merge(usage::routes())
        .layer(DefaultBodyLimit::max(MAX_PIECE_BYTES))
        .with_state(node)
}

/// `GET /identity`: the node's id and public key.
async fn get_identity(State(node): State<Node>) -> Json<NodeIdentity> {
    Json(NodeIdentity::clone(&node.identity))
}

/// `PUT /pieces/{id}?lease_seconds=N` or `?tab=NAME`: stores the body as the
/// piece `id`.
async fn put_piece(
    State(node): State<Node>,
    Path(id): Path<PieceId>,
    Query(query): Query<StoreQuery>,
    body: Bytes,
) -> Response {
    // Checked before charging a tab, so bytes that would be refused anyway
    // use up none of what the client paid for.
    if PieceId::of(&body) != id {
        let reason = "the bytes do not hash to the piece id";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let expires_at = match storage_end(&node, &query, body.len() as u64).await {
        Ok(expires_at) => expires_at,
        Err(refusal) => return refusal.into_response(),
    };
    match node.store.put(&id, &body, expires_at).await {
        Ok(expires_at) => (StatusCode::CREATED, Json(LeaseResponse { expires_at })).into_response(),
        Err(StoreError::WrongId) => (
            StatusCode::BAD_REQUEST,
            "the bytes do not hash to the piece id",
        )
            .into_response(),
        Err(error) => internal_error(&id, error),
    }
}

/// When a piece of `len` bytes stored under `query` ends, or the status and
/// reason that refuse it.
async fn storage_end(
    node: &Node,
    query: &StoreQuery,
    len: u64,
) -> Result<u64, (StatusCode, String)> {
    if let Some(tab) = &query.tab {
        return tab_routes::charge(node, tab, len).await;
    }
    if node.token_coordinator.is_some() {
        return Err((StatusCode::PAYMENT_REQUIRED, PAYMENT_REQUIRED.to_string()));
    }
    let Some(lease_seconds) = query.lease_seconds else {
        let reason = "give lease_seconds or a tab".to_string();
        return Err((StatusCode::BAD_REQUEST, reason));
    };
    lease_end(node, &LeaseQuery { lease_seconds })
        .map_err(|reason| (StatusCode::BAD_REQUEST, reason))
}

const PAYMENT_REQUIRED: &str = "this node stores pieces only against a tab paid with tokens";

/// The answer to free storage on a node that requires tokens.
fn payment_required() -> Response {
    (StatusCode::PAYMENT_REQUIRED, PAYMENT_REQUIRED).into_response()
}

/// `POST /pieces/{id}/lease?lease_seconds=N`: renews the lease of `id`.
async fn renew_lease(
    State(node): State<Node>,
    Path(id): Path<PieceId>,
    Query(query): Query<LeaseQuery>,
) -> Response {
    // Free renewals would keep pieces alive without paying; with tokens,
    // renewing is moving pieces into a new tab.
    if node.token_coordinator.is_some() {
        return payment_required();
    }
    let expires_at = match lease_end(&node, &query) {
        Ok(expires_at) => expires_at,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    match node.store.renew(&id, expires_at, unix_now()).await {
        Ok(Some(expires_at)) => Json(LeaseResponse { expires_at }).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(&id, error),
    }
}

/// `POST /leases?lease_seconds=N`: renews every piece in the body.
async fn renew_leases(
    State(node): State<Node>,
    Query(query): Query<LeaseQuery>,
    Json(request): Json<BatchRenewRequest>,
) -> Response {
    if node.token_coordinator.is_some() {
        return payment_required();
    }
    if request.pieces.len() > MAX_BATCH_PIECES {
        let reason = format!("a batch renews at most {MAX_BATCH_PIECES} pieces");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let expires_at = match lease_end(&node, &query) {
        Ok(expires_at) => expires_at,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    let now = unix_now();
    let mut lease_ends = Vec::with_capacity(request.pieces.len());
    for id in &request.pieces {
        match node.store.renew(id, expires_at, now).await {
            Ok(lease_end) => lease_ends.push(lease_end),
            Err(error) => return internal_error(id, error),
        }
    }
    Json(BatchRenewResponse { lease_ends }).into_response()
}

/// `GET /pieces/{id}`: the bytes of the piece `id`, or `404 Not Found`.
async fn get_piece(State(node): State<Node>, Path(id): Path<PieceId>) -> Response {
    match node.store.get(&id, unix_now()).await {
        Ok(Some(bytes)) => bytes.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(&id, error),
    }
}

/// `POST /pieces/{id}/challenge`: the segments asked for, with their proofs.
///
/// The node builds the proofs from the piece itself, so it needs every byte
/// of the piece at hand to answer; that is the point of the challenge.
async fn answer_challenge(
    State(node): State<Node>,
    Path(id): Path<PieceId>,
    Json(request): Json<ChallengeRequest>,
) -> Response {
    if request.segments.is_empty() || request.segments.len() > MAX_CHALLENGE_SEGMENTS {
        let reason = format!("a challenge asks for 1 to {MAX_CHALLENGE_SEGMENTS} segments");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let piece = match node.store.get(&id, unix_now()).await {
        Ok(Some(piece)) => piece,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return internal_error(&id, error),
    };
    let mut proofs = Vec::with_capacity(request.segments.len());
    for &index in &request.segments {
        let Some(proof) = merkle::prove(&piece, index) else {
            let segment_count = merkle::segment_count(piece.len());
            let reason = format!("there is no segment {index}: the piece has {segment_count}");
            return (StatusCode::BAD_REQUEST, reason).into_response();
        };
        proofs.push(proof);
    }
    Json(ChallengeResponse { proofs }).into_response()
}

/// `PUT /pointers/{owner}?lease_seconds=N`: stores the pointer in the body
/// when it is genuine and newer than the one held.
async fn put_pointer(
    State(node): State<Node>,
    Path(owner): Path<String>,
    Query(query): Query<LeaseQuery>,
    Json(pointer): Json<Pointer>,
) -> Response {
    // Without this check a record could be filed under one key in the URL
    // and verified against another in the body.
    if parse_owner(&owner) != Some(pointer.owner) {
        let reason = "the URL names another owner than the pointer";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let expires_at = match lease_end(&node, &query) {
        Ok(expires_at) => expires_at,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    match node.pointers.put(&pointer, expires_at, unix_now()).await {
        Ok(PointerPut::Stored { expires_at }) => {
            (StatusCode::CREATED, Json(LeaseResponse { expires_at })).into_response()
        }
        Ok(PointerPut::Refused { held_version }) => {
            (StatusCode::CONFLICT, Json(PointerConflict { held_version })).into_response()
        }
        Err(PointerStoreError::Invalid(error)) => {
            (StatusCode::BAD_REQUEST, error.to_string()).into_response()
        }
        Err(error) => {
            eprintln!("pointer {owner}: {error}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `GET /pointers/{owner}`: the newest pointer held for `owner`, or
/// `404 Not Found`.
async fn get_pointer(State(node): State<Node>, Path(owner): Path<String>) -> Response {
    let Some(owner_key) = parse_owner(&owner) else {
        let reason = "an owner is 64 lowercase hex digits";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    };
    match node.pointers.get(&owner_key, unix_now()).await {
        Ok(Some(pointer)) => Json(pointer).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            eprintln!("pointer {owner}: {error}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// When a lease of the requested length ends, or why the node refuses it.
fn lease_end(node: &Node, query: &LeaseQuery) -> Result<u64, String> {
    if query.lease_seconds == 0 {
        return Err("a lease lasts at least one second".to_string());
    }
    if query.lease_seconds > node.max_lease_seconds {
        return Err(format!(
            "this node grants leases of at most {} seconds",
            node.max_lease_seconds
        ));
    }
    Ok(unix_now() + query.lease_seconds)
}

/// A disk or database error: logged on the node, answered without detail,
/// since the client can do nothing with the node's internals.
pub(crate) fn internal_error(id: &PieceId, error: StoreError) -> Response {
    eprintln!("piece {id}: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

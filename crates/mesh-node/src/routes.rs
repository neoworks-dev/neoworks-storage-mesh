//! The node's HTTP interface: storing, renewing, fetching pieces and proving
//! the node still holds them, and storing and fetching signed pointers.
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
    CHALLENGE_ROUTE, ChallengeRequest, ChallengeResponse, IDENTITY_ROUTE, LEASE_ROUTE, LeaseQuery,
    LeaseResponse, MAX_CHALLENGE_SEGMENTS, MAX_PIECE_BYTES, NodeIdentity, PIECE_ROUTE,
    POINTER_ROUTE, PointerConflict,
};
use mesh_core::{PieceId, merkle};

use crate::clock::unix_now;
use crate::piece_store::{PieceStore, StoreError};
use crate::pointer_store::{PointerPut, PointerStore, PointerStoreError};
use crate::settings::NodeSettings;

/// What every request handler needs.
#[derive(Clone)]
struct Node {
    store: Arc<PieceStore>,
    pointers: Arc<PointerStore>,
    identity: Arc<NodeIdentity>,
    max_lease_seconds: u64,
}

/// The router of the node `identity`, serving `store` and `pointers`.
///
/// Ids that are not 64 lowercase hex digits and requests without
/// `lease_seconds` are refused by the extractors with `400 Bad Request` before
/// any handler runs.
pub fn router(
    store: Arc<PieceStore>,
    pointers: Arc<PointerStore>,
    identity: NodeIdentity,
    settings: &NodeSettings,
) -> Router {
    let node = Node {
        store,
        pointers,
        identity: Arc::new(identity),
        max_lease_seconds: settings.max_lease.as_secs(),
    };
    Router::new()
        .route(PIECE_ROUTE, put(put_piece).get(get_piece))
        .route(LEASE_ROUTE, post(renew_lease))
        .route(CHALLENGE_ROUTE, post(answer_challenge))
        .route(POINTER_ROUTE, put(put_pointer).get(get_pointer))
        .route(IDENTITY_ROUTE, get(get_identity))
        .layer(DefaultBodyLimit::max(MAX_PIECE_BYTES))
        .with_state(node)
}

/// `GET /identity`: the node's id and public key.
async fn get_identity(State(node): State<Node>) -> Json<NodeIdentity> {
    Json(NodeIdentity::clone(&node.identity))
}

/// `PUT /pieces/{id}?lease_seconds=N`: stores the body as the piece `id`.
async fn put_piece(
    State(node): State<Node>,
    Path(id): Path<PieceId>,
    Query(query): Query<LeaseQuery>,
    body: Bytes,
) -> Response {
    let expires_at = match lease_end(&node, &query) {
        Ok(expires_at) => expires_at,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
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

/// `POST /pieces/{id}/lease?lease_seconds=N`: renews the lease of `id`.
async fn renew_lease(
    State(node): State<Node>,
    Path(id): Path<PieceId>,
    Query(query): Query<LeaseQuery>,
) -> Response {
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
fn internal_error(id: &PieceId, error: StoreError) -> Response {
    eprintln!("piece {id}: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

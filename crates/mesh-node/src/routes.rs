//! The node's HTTP interface: `PUT` and `GET` of pieces by id.
//!
//! `mesh_core::protocol` describes the requests; this module maps them onto
//! the [`PieceStore`].

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use mesh_core::PieceId;
use mesh_core::protocol::{MAX_PIECE_BYTES, PIECE_ROUTE};

use crate::piece_store::{PieceStore, PutError};

/// The router of a node serving `store`.
pub fn router(store: PieceStore) -> Router {
    Router::new()
        .route(PIECE_ROUTE, put(put_piece).get(get_piece))
        .layer(DefaultBodyLimit::max(MAX_PIECE_BYTES))
        .with_state(Arc::new(store))
}

/// `PUT /pieces/{id}`: stores the body as the piece `id`.
///
/// An id that is not 64 lowercase hex digits is refused by the `Path`
/// extractor with `400 Bad Request` before this runs.
async fn put_piece(
    State(store): State<Arc<PieceStore>>,
    Path(id): Path<PieceId>,
    body: Bytes,
) -> Response {
    match store.put(&id, &body).await {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(PutError::WrongId) => (
            StatusCode::BAD_REQUEST,
            "the bytes do not hash to the piece id",
        )
            .into_response(),
        Err(PutError::Io(error)) => internal_error(&id, error),
    }
}

/// `GET /pieces/{id}`: the bytes of the piece `id`, or `404 Not Found`.
async fn get_piece(State(store): State<Arc<PieceStore>>, Path(id): Path<PieceId>) -> Response {
    match store.get(&id).await {
        Ok(Some(bytes)) => bytes.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(&id, error),
    }
}

/// A disk error: logged on the node, answered without detail, since the
/// client can do nothing with the node's file paths.
fn internal_error(id: &PieceId, error: std::io::Error) -> Response {
    eprintln!("piece {id}: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

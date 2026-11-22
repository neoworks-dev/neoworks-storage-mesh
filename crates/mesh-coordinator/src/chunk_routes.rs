//! Registering chunks for repair and looking up where their pieces are, over
//! HTTP; `mesh_core::chunk_registry` describes the requests.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_core::chunk_registry::{
    CHUNK_ROUTE, CHUNKS_ROUTE, ChunkLocations, ChunkRegistration, MAX_CHUNKS_PER_REQUEST,
    RegisterChunks, RegisterChunksResponse, chunk_id, responsible_coordinator,
};

use crate::chunk_book::Booked;
use crate::routes::Coordinator;

/// The chunk routes.
pub fn routes() -> Router<Coordinator> {
    Router::new()
        .route(CHUNKS_ROUTE, post(register))
        .route(CHUNK_ROUTE, get(locations))
}

/// `POST /chunks`: keeps the chunks this coordinator is responsible for.
async fn register(
    State(coordinator): State<Coordinator>,
    Json(request): Json<RegisterChunks>,
) -> Response {
    if request.chunks.len() > MAX_CHUNKS_PER_REQUEST {
        let reason = format!("at most {MAX_CHUNKS_PER_REQUEST} chunks per request");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let mut accepted = Vec::with_capacity(request.chunks.len());
    for registration in &request.chunks {
        if !is_responsible(&coordinator, registration) {
            accepted.push(false);
            continue;
        }
        match coordinator.chunks.register(registration).await {
            Ok(booked) => accepted.push(booked == Booked::Kept),
            Err(error) => return internal_error(error),
        }
    }
    Json(RegisterChunksResponse { accepted }).into_response()
}

/// Whether this coordinator repairs the chunk `registration` names. Two
/// coordinators never keep the same chunk, so they never repair it twice.
fn is_responsible(coordinator: &Coordinator, registration: &ChunkRegistration) -> bool {
    let Some(coordinators) = &coordinator.chunk_owners else {
        return true;
    };
    let id = chunk_id(registration.pieces.iter().map(|piece| &piece.id));
    let own_key = coordinator.key.verifying_key().to_bytes();
    responsible_coordinator(&id, coordinators).is_some_and(|key| key.0 == own_key)
}

/// `GET /chunks/{id}`: where the chunk's pieces are now.
async fn locations(State(coordinator): State<Coordinator>, Path(id): Path<String>) -> Response {
    let mut chunk = [0u8; 32];
    if hex::decode_to_slice(&id, &mut chunk).is_err() {
        return (StatusCode::BAD_REQUEST, "a chunk id is 64 hex digits").into_response();
    }
    match coordinator.chunks.pieces(&chunk).await {
        Ok(Some(pieces)) => Json(ChunkLocations { pieces }).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal_error(error),
    }
}

/// A database error: logged on the coordinator, answered without detail.
fn internal_error(error: rusqlite::Error) -> Response {
    eprintln!("chunks: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

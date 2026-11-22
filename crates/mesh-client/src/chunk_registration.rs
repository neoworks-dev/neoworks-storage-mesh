//! Registering a file's chunks with the coordinator, which repairs them
//! while the owner's devices are off, and taking in where repair moved their
//! pieces (`mesh_core::chunk_registry` describes the requests).
//!
//! A registration names piece ids and nodes, never keys. It carries a secret
//! derived from the chunk's key, so only the owner can change a chunk's
//! registration later, such as to extend it after a renewal.

use mesh_core::chunk_registry::{
    CHUNKS_ROUTE, ChunkLocations, ChunkRegistration, MAX_CHUNKS_PER_REQUEST, RegisterChunks,
    RegisterChunksResponse, RegisteredPiece, chunk_id, chunk_path,
};
use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;

use crate::manifest::{ChunkRecord, Manifest};
use crate::node_client::NodeClient;

/// A registration or lookup that did not go through.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("the coordinator answered {0}")]
    Refused(StatusCode),
    #[error("the coordinator answered in a form this client does not understand")]
    UnexpectedAnswer,
}

/// What the coordinator needs to repair `chunk` until `expires_at`.
pub fn registration_of(chunk: &ChunkRecord, expires_at: u64) -> ChunkRegistration {
    ChunkRegistration {
        secret: chunk.key.registration_secret(),
        layout: chunk.layout,
        regions: chunk.regions,
        sealed_len: chunk.sealed_len,
        expires_at,
        pieces: chunk
            .pieces
            .iter()
            .map(|piece| RegisteredPiece {
                id: piece.id,
                node: piece.node,
            })
            .collect(),
    }
}

/// Registers every chunk of `manifest` with `coordinator` until
/// `expires_at`, and returns how many it keeps. Registering again after a
/// renewal extends them.
///
/// A coordinator keeps only the chunks it is responsible for; the others
/// belong with another coordinator.
pub async fn register(
    client: &NodeClient,
    coordinator: &str,
    manifest: &Manifest,
    expires_at: u64,
) -> Result<usize, RegisterError> {
    let registrations: Vec<ChunkRegistration> = manifest
        .chunks
        .iter()
        .map(|chunk| registration_of(chunk, expires_at))
        .collect();
    send_registrations(client, coordinator, registrations).await
}

/// Sends `registrations` to `coordinator`, as many to a request as it takes,
/// and returns how many it keeps. Many files' chunks go out together this
/// way, rather than in a request per file.
pub async fn send_registrations(
    client: &NodeClient,
    coordinator: &str,
    registrations: Vec<ChunkRegistration>,
) -> Result<usize, RegisterError> {
    let url = format!("{}{CHUNKS_ROUTE}", coordinator.trim_end_matches('/'));
    let mut kept = 0;
    for batch in registrations.chunks(MAX_CHUNKS_PER_REQUEST) {
        let request = RegisterChunks {
            chunks: batch.to_vec(),
        };
        // Ids, numbers and bytes always serialize.
        let body = serde_json::to_vec(&request).expect("a registration serializes");
        let response = client
            .http()
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(RegisterError::Refused(response.status()));
        }
        let answer: RegisterChunksResponse = serde_json::from_slice(&response.bytes().await?)
            .map_err(|_| RegisterError::UnexpectedAnswer)?;
        kept += answer.accepted.iter().filter(|&&accepted| accepted).count();
    }
    Ok(kept)
}

/// Asks `coordinator` where each chunk of `manifest` is now and moves the
/// pieces repair moved, and returns how many moved.
///
/// Only pieces whose id matches are moved: a coordinator can tell the client
/// where to look, but never swap a piece for another.
pub async fn take_new_locations(
    client: &NodeClient,
    coordinator: &str,
    manifest: &mut Manifest,
) -> Result<usize, RegisterError> {
    let mut moved = 0;
    for chunk in &mut manifest.chunks {
        let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
        let url = format!("{}{}", coordinator.trim_end_matches('/'), chunk_path(&id));
        let response = client.http().get(url).send().await?;
        // A chunk the coordinator doesn't keep has not been moved by it.
        if response.status() == StatusCode::NOT_FOUND {
            continue;
        }
        if !response.status().is_success() {
            return Err(RegisterError::Refused(response.status()));
        }
        let locations: ChunkLocations = serde_json::from_slice(&response.bytes().await?)
            .map_err(|_| RegisterError::UnexpectedAnswer)?;
        for (piece, located) in chunk.pieces.iter_mut().zip(&locations.pieces) {
            if piece.id == located.id && piece.node != located.node {
                piece.node = located.node;
                moved += 1;
            }
        }
    }
    Ok(moved)
}

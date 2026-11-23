//! Registering a file's chunks with the coordinators, which repair them
//! while the owner's devices are off, and taking in where repair moved their
//! pieces (`mesh_core::chunk_registry` describes the requests).
//!
//! Each chunk is kept by two coordinators on the coordinator list, and the
//! list says where each is reached, so every registration and lookup goes
//! to both keepers of that chunk; either may be offline.
//!
//! A registration names piece ids and nodes, never keys. It carries a secret
//! derived from the chunk's key, so only the owner can change a chunk's
//! registration later, such as to extend it after a renewal.

use std::collections::BTreeMap;

use mesh_core::chunk_registry::{
    CHUNKS_ROUTE, ChunkLocations, ChunkRegistration, MAX_CHUNKS_PER_REQUEST, RegisterChunks,
    RegisterChunksResponse, RegisteredPiece, chunk_id, chunk_keepers, chunk_path,
};
use mesh_core::node_list::CoordinatorList;
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

/// Registers every chunk of `manifest` until `expires_at`, each with its two
/// keepers on `coordinators`, and returns how many chunks at least one of
/// them keeps. Registering again after a renewal extends them.
pub async fn register(
    client: &NodeClient,
    coordinators: &CoordinatorList,
    manifest: &Manifest,
    expires_at: u64,
) -> Result<usize, RegisterError> {
    let registrations: Vec<ChunkRegistration> = manifest
        .chunks
        .iter()
        .map(|chunk| registration_of(chunk, expires_at))
        .collect();
    send_registrations(client, coordinators, registrations).await
}

/// Sends each of `registrations` to both keepers of its chunk on
/// `coordinators`, as many to a request as it takes, and returns how many
/// chunks at least one keeper keeps. Many files' chunks go out together this
/// way, rather than in a request per file.
///
/// A keeper that can't be reached is no error while the chunk's other
/// keeper keeps it, so registering works while any one coordinator is
/// offline. A chunk neither keeper could be asked about is.
pub async fn send_registrations(
    client: &NodeClient,
    coordinators: &CoordinatorList,
    registrations: Vec<ChunkRegistration>,
) -> Result<usize, RegisterError> {
    // The positions in `registrations` each coordinator gets.
    let mut by_coordinator: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (position, registration) in registrations.iter().enumerate() {
        let id = chunk_id(registration.pieces.iter().map(|piece| &piece.id));
        for keeper in chunk_keepers(&id, coordinators) {
            by_coordinator
                .entry(&keeper.url)
                .or_default()
                .push(position);
        }
    }
    let mut kept = vec![false; registrations.len()];
    let mut unasked = vec![false; registrations.len()];
    let mut last_error = None;
    for (url, positions) in by_coordinator {
        let batch: Vec<ChunkRegistration> = positions
            .iter()
            .map(|&position| registrations[position].clone())
            .collect();
        match send_to(client, url, &batch).await {
            Ok(accepted) => {
                for (&position, accepted) in positions.iter().zip(accepted) {
                    kept[position] |= accepted;
                }
            }
            Err(error) => {
                for &position in &positions {
                    unasked[position] = true;
                }
                last_error = Some(error);
            }
        }
    }
    let kept_count = kept.iter().filter(|&&kept| kept).count();
    let Some(error) = last_error else {
        return Ok(kept_count);
    };
    let unkept = (0..registrations.len()).any(|position| unasked[position] && !kept[position]);
    if unkept {
        return Err(error);
    }
    Ok(kept_count)
}

/// Sends `registrations` to the coordinator at `url` and returns, for each,
/// whether it keeps it.
async fn send_to(
    client: &NodeClient,
    url: &str,
    registrations: &[ChunkRegistration],
) -> Result<Vec<bool>, RegisterError> {
    let url = format!("{}{CHUNKS_ROUTE}", url.trim_end_matches('/'));
    let mut kept = Vec::with_capacity(registrations.len());
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
        // A coordinator answering for fewer chunks than asked keeps none of
        // the rest.
        let mut accepted = answer.accepted;
        accepted.resize(batch.len(), false);
        kept.extend(accepted);
    }
    Ok(kept)
}

/// Asks both keepers of each chunk of `manifest` on `coordinators` where it
/// is now, moves the pieces repair moved, and returns how many moved.
///
/// Only pieces whose id matches are moved: a coordinator can tell the client
/// where to look, but never swap a piece for another. When both keepers
/// moved a piece, the first keeper's word counts, since it is the one that
/// repairs while it is up. A keeper that can't be reached is skipped while
/// the other answers.
pub async fn take_new_locations(
    client: &NodeClient,
    coordinators: &CoordinatorList,
    manifest: &mut Manifest,
) -> Result<usize, RegisterError> {
    let mut moved = 0;
    for chunk in &mut manifest.chunks {
        let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
        let reports = ask_keepers(client, coordinators, &id).await?;
        moved += take_moves(chunk, &reports);
    }
    Ok(moved)
}

/// What the keepers of the chunk `id` that keep it say about it, the first
/// keeper first. An error only when no keeper answered at all.
async fn ask_keepers(
    client: &NodeClient,
    coordinators: &CoordinatorList,
    id: &[u8; 32],
) -> Result<Vec<ChunkLocations>, RegisterError> {
    let mut reports = Vec::new();
    let mut answered = false;
    let mut last_error = None;
    for keeper in chunk_keepers(id, coordinators) {
        match locations_at(client, &keeper.url, id).await {
            Ok(locations) => {
                answered = true;
                reports.extend(locations);
            }
            Err(error) => last_error = Some(error),
        }
    }
    match last_error {
        Some(error) if !answered => Err(error),
        _ => Ok(reports),
    }
}

/// Moves each piece of `chunk` to where the first of `reports` that moved
/// it says, and returns how many moved.
fn take_moves(chunk: &mut ChunkRecord, reports: &[ChunkLocations]) -> usize {
    let mut moved = 0;
    for (position, piece) in chunk.pieces.iter_mut().enumerate() {
        let new_location = reports
            .iter()
            .filter_map(|report| report.pieces.get(position))
            .find(|located| located.id == piece.id && located.node != piece.node);
        if let Some(located) = new_location {
            piece.node = located.node;
            moved += 1;
        }
    }
    moved
}

/// Where the coordinator at `url` says the pieces of the chunk `id` are, or
/// `None` when it doesn't keep the chunk.
pub async fn locations_at(
    client: &NodeClient,
    url: &str,
    id: &[u8; 32],
) -> Result<Option<ChunkLocations>, RegisterError> {
    let url = format!("{}{}", url.trim_end_matches('/'), chunk_path(id));
    let response = client.http().get(url).send().await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(RegisterError::Refused(response.status()));
    }
    let locations = serde_json::from_slice(&response.bytes().await?)
        .map_err(|_| RegisterError::UnexpectedAnswer)?;
    Ok(Some(locations))
}

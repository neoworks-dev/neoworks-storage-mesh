//! Bringing a file back to full strength after nodes lost pieces.
//!
//! A chunk survives the loss of as many pieces as its layout has parity
//! pieces, but not one more. Repair fetches as many healthy pieces as the
//! chunk has data pieces, rebuilds the sealed chunk, cuts it again and stores
//! the pieces that were lost on other nodes. Cutting is deterministic,
//! so a rebuilt piece is byte for byte the piece that was lost: its id and
//! Merkle root stay the same, and only the node it lives on changes.
//!
//! Repair works on sealed bytes only and never needs the file's key, so the
//! owner can later hand it to a helper that repairs while the owner is offline.

use std::time::Duration;

use futures::future::join_all;
use mesh_core::{NodeId, PieceId};

use crate::audit::{AuditReport, ChunkAudit, PieceHealth};
use crate::erasure::{cut, rebuild};
use crate::manifest::{ChunkRecord, Manifest};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::placement::store_pieces;

/// What repair did with one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkRepair {
    /// All pieces were healthy, so nothing was done.
    Intact,
    /// Every unhealthy piece was rebuilt and stored on a new node.
    Repaired { replaced: usize },
    /// Some pieces could not be placed, because no node the rules allow took
    /// them.
    Partly { replaced: usize, unplaced: usize },
    /// Fewer pieces than the chunk has data pieces could be fetched, so it
    /// can't be rebuilt.
    Lost { fetched: usize },
}

/// What repair did with every chunk of a file, in file order.
#[derive(Debug)]
pub struct RepairReport {
    pub chunks: Vec<ChunkRepair>,
}

/// Repairs the chunks of `manifest` that `audit` found to have lost a piece,
/// storing rebuilt
/// pieces on nodes chosen from `list` for `lease`, and updates `manifest`
/// with their new nodes.
///
/// The new nodes follow the placement rules together with the nodes that
/// still hold healthy pieces, and a node that failed a piece is never chosen
/// again for that chunk. The unhealthy pieces left on old nodes are not
/// deleted; nobody renews them, so they expire.
pub async fn repair(
    client: &NodeClient,
    manifest: &mut Manifest,
    audit: &AuditReport,
    list: &NodeList,
    lease: Duration,
) -> RepairReport {
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for (chunk, chunk_audit) in manifest.chunks.iter_mut().zip(&audit.chunks) {
        chunks.push(repair_chunk(client, chunk, chunk_audit, list, lease).await);
    }
    RepairReport { chunks }
}

async fn repair_chunk(
    client: &NodeClient,
    chunk: &mut ChunkRecord,
    chunk_audit: &ChunkAudit,
    list: &NodeList,
    lease: Duration,
) -> ChunkRepair {
    // Run by hand on a device, repair fixes every loss it finds. Waiting for
    // more losses is the coordinator's call, which watches nodes over time.
    if chunk_audit.healthy() == chunk.layout.total() {
        return ChunkRepair::Intact;
    }
    let survivors = fetch_healthy_pieces(client, list, chunk, chunk_audit).await;
    let fetched = survivors.iter().filter(|piece| piece.is_some()).count();
    let Ok(sealed) = rebuild(&survivors, chunk.sealed_len as usize, chunk.layout) else {
        return ChunkRepair::Lost { fetched };
    };
    let rebuilt_pieces = cut(&sealed, chunk.layout);
    let mut lost_indexes = Vec::new();
    let mut unplaced = 0;
    for (index, health) in chunk_audit.pieces.iter().enumerate() {
        if *health == PieceHealth::Healthy {
            continue;
        }
        // Cutting is deterministic, so this holds unless the manifest and the
        // pieces don't belong together. Storing the bytes anyway would put a
        // piece where the manifest would never find it.
        if PieceId::of(&rebuilt_pieces[index]) != chunk.pieces[index].id {
            eprintln!("rebuilt piece {index} does not match the manifest");
            unplaced += 1;
            continue;
        }
        lost_indexes.push(index);
    }
    let lost_pieces: Vec<Vec<u8>> = lost_indexes
        .iter()
        .map(|&index| rebuilt_pieces[index].clone())
        .collect();
    let (holding, failed) = nodes_by_health(chunk, chunk_audit);
    let limits = chunk.spread().limits();
    let placed = store_pieces(client, list, &lost_pieces, &holding, &failed, limits, lease).await;
    let mut replaced = 0;
    for (index, piece) in lost_indexes.into_iter().zip(placed) {
        match piece {
            Some(piece) => {
                chunk.pieces[index].node = piece.node;
                replaced += 1;
            }
            None => unplaced += 1,
        }
    }
    if unplaced == 0 {
        ChunkRepair::Repaired { replaced }
    } else {
        ChunkRepair::Partly { replaced, unplaced }
    }
}

/// The ids of the nodes of `chunk` whose pieces are healthy, and of those
/// whose pieces are not.
fn nodes_by_health(chunk: &ChunkRecord, chunk_audit: &ChunkAudit) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut healthy = Vec::new();
    let mut unhealthy = Vec::new();
    for (piece, health) in chunk.pieces.iter().zip(&chunk_audit.pieces) {
        if *health == PieceHealth::Healthy {
            healthy.push(piece.node);
        } else {
            unhealthy.push(piece.node);
        }
    }
    (healthy, unhealthy)
}

/// The pieces the audit found healthy, fetched again from their nodes'
/// addresses in `list`; every other piece is `None`. A piece that fails to
/// arrive now is `None` too.
async fn fetch_healthy_pieces(
    client: &NodeClient,
    list: &NodeList,
    chunk: &ChunkRecord,
    chunk_audit: &ChunkAudit,
) -> Vec<Option<Vec<u8>>> {
    let fetches = chunk
        .pieces
        .iter()
        .zip(&chunk_audit.pieces)
        .map(|(piece, health)| async move {
            if *health != PieceHealth::Healthy {
                return None;
            }
            let url = list.url_of(&piece.node)?;
            // `get` checks the bytes against the piece id, so a node can't
            // slip wrong bytes into the rebuild.
            client.get(url, &piece.id).await.ok().flatten()
        });
    join_all(fetches).await
}

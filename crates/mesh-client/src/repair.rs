//! Bringing a file back to full strength after nodes lost pieces.
//!
//! A chunk survives the loss of two of its six pieces, but not a third. Repair
//! fetches four healthy pieces, rebuilds the sealed chunk, cuts it again and
//! stores the pieces that were lost on other nodes. Cutting is deterministic,
//! so a rebuilt piece is byte for byte the piece that was lost: its id and
//! Merkle root stay the same, and only the node it lives on changes.
//!
//! Repair works on sealed bytes only and never needs the file's key, so the
//! owner can later hand it to a helper that repairs while the owner is offline.

use std::time::Duration;

use futures::future::join_all;
use mesh_core::PieceId;

use crate::audit::{AuditReport, ChunkAudit, PieceHealth};
use crate::erasure::{cut, rebuild};
use crate::manifest::{ChunkRecord, Manifest};
use crate::node_client::NodeClient;

/// A chunk is repaired once it has this many healthy pieces or fewer.
///
/// Rebuilding even one piece means downloading four, so a larger code would
/// wait for several losses and repair them together. With six pieces, though,
/// the first loss already leaves only one spare; waiting for a second would
/// leave none.
pub const REPAIR_AT: usize = 5;

/// What repair did with one chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkRepair {
    /// All pieces were healthy, so nothing was done.
    Intact,
    /// Every unhealthy piece was rebuilt and stored on a new node.
    Repaired { replaced: usize },
    /// Some pieces could not be placed, because no spare node took them.
    Partly { replaced: usize, unplaced: usize },
    /// Fewer than four pieces could be fetched, so the chunk can't be rebuilt.
    Lost { fetched: usize },
}

/// What repair did with every chunk of a file, in file order.
#[derive(Debug)]
pub struct RepairReport {
    pub chunks: Vec<ChunkRepair>,
}

/// Repairs the chunks of `manifest` that `audit` found weak, storing rebuilt
/// pieces on `spare_nodes` for `lease`, and updates `manifest` with their new
/// nodes.
///
/// A spare node never gets a second piece of the same chunk, and neither does
/// a node that already holds or failed one. The unhealthy pieces left on old
/// nodes are not deleted; nobody renews them, so they expire.
pub async fn repair(
    client: &NodeClient,
    manifest: &mut Manifest,
    audit: &AuditReport,
    spare_nodes: &[String],
    lease: Duration,
) -> RepairReport {
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for (chunk, chunk_audit) in manifest.chunks.iter_mut().zip(&audit.chunks) {
        chunks.push(repair_chunk(client, chunk, chunk_audit, spare_nodes, lease).await);
    }
    RepairReport { chunks }
}

async fn repair_chunk(
    client: &NodeClient,
    chunk: &mut ChunkRecord,
    chunk_audit: &ChunkAudit,
    spare_nodes: &[String],
    lease: Duration,
) -> ChunkRepair {
    if chunk_audit.healthy() > REPAIR_AT {
        return ChunkRepair::Intact;
    }
    let survivors = fetch_healthy_pieces(client, chunk, chunk_audit).await;
    let fetched = survivors.iter().filter(|piece| piece.is_some()).count();
    let Ok(sealed) = rebuild(&survivors, chunk.sealed_len as usize) else {
        return ChunkRepair::Lost { fetched };
    };
    let rebuilt_pieces = cut(&sealed);
    let mut replaced = 0;
    let mut unplaced = 0;
    for (index, health) in chunk_audit.pieces.iter().enumerate() {
        if *health == PieceHealth::Healthy {
            continue;
        }
        let bytes = &rebuilt_pieces[index];
        // Cutting is deterministic, so this holds unless the manifest and the
        // pieces don't belong together. Storing the bytes anyway would put a
        // piece where the manifest would never find it.
        if PieceId::of(bytes) != chunk.pieces[index].id {
            eprintln!("rebuilt piece {index} does not match the manifest");
            unplaced += 1;
            continue;
        }
        match place_piece(client, chunk, bytes, spare_nodes, lease).await {
            Some(node) => {
                chunk.pieces[index].node = node;
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

/// The pieces the audit found healthy, fetched again; every other piece is
/// `None`. A piece that fails to arrive now is `None` too.
async fn fetch_healthy_pieces(
    client: &NodeClient,
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
            // `get` checks the bytes against the piece id, so a node can't
            // slip wrong bytes into the rebuild.
            client.get(&piece.node, &piece.id).await.ok().flatten()
        });
    join_all(fetches).await
}

/// Stores `bytes` on the first spare node that takes them and doesn't already
/// hold a piece of `chunk`, and returns that node.
async fn place_piece(
    client: &NodeClient,
    chunk: &ChunkRecord,
    bytes: &[u8],
    spare_nodes: &[String],
    lease: Duration,
) -> Option<String> {
    let id = PieceId::of(bytes);
    let is_free = |node: &&String| chunk.pieces.iter().all(|piece| piece.node != **node);
    for node in spare_nodes.iter().filter(is_free) {
        match client.put(node, &id, bytes.to_vec(), lease).await {
            Ok(_) => return Some(node.clone()),
            Err(error) => eprintln!("{error}"),
        }
    }
    None
}

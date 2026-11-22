//! Getting a file back: fetch each chunk's pieces, rebuild, open, write.

use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use crate::encryption::open;
use crate::erasure::{DATA_PIECES, RebuildError, rebuild};
use crate::manifest::{Manifest, PlacedPiece};
use crate::node_client::NodeClient;
use crate::node_latency::LatencyTable;

/// How long to wait for any of the pieces asked for before asking one more
/// node. A healthy node on a home connection answers within it, and it is
/// short next to the 30 seconds after which a request gives up.
const HEDGE_DELAY: Duration = Duration::from_secs(1);

/// Why a file could not be read back.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("chunk {index}: {source}")]
    Rebuild { index: u32, source: RebuildError },
    #[error("chunk {index} does not decrypt with the key the manifest holds")]
    Open { index: u32 },
    #[error("writing the file: {0}")]
    Write(#[from] std::io::Error),
    #[error("the file is {actual} bytes, the manifest says {expected}")]
    WrongSize { expected: u64, actual: u64 },
}

/// Reads the file `manifest` describes into `writer`, one chunk at a time, so
/// the file never has to fit in memory.
///
/// A chunk survives as long as four of its six pieces can be fetched. Nodes
/// that are down, have lost a piece or return wrong bytes all count the same:
/// that piece is missing.
///
/// On an error, `writer` may already hold the chunks before the failing one,
/// so whatever it writes to should be discarded.
pub async fn download(
    client: &NodeClient,
    manifest: &Manifest,
    mut writer: impl AsyncWrite + Unpin,
) -> Result<(), DownloadError> {
    let mut written = 0;
    for (index, chunk) in manifest.chunks.iter().enumerate() {
        // The manifest was written by `upload`, which numbers chunks with a u32.
        let index = u32::try_from(index).expect("chunk indexes fit a u32");
        let pieces = fetch_pieces(client, &chunk.pieces).await;
        let sealed = rebuild(&pieces, chunk.sealed_len as usize)
            .map_err(|source| DownloadError::Rebuild { index, source })?;
        let mut plaintext = open(&chunk.key, &sealed).map_err(|_| DownloadError::Open { index })?;
        // Chunks stored before padding was dropped open with zeros after
        // their bytes; cutting to the recorded length drops them, and does
        // nothing to newer chunks.
        plaintext.truncate(chunk.plain_len as usize);
        writer.write_all(&plaintext).await?;
        written += plaintext.len() as u64;
    }
    writer.flush().await?;
    // Every chunk opened, so this only fails for a manifest whose size field
    // was changed after upload.
    if written != manifest.size {
        return Err(DownloadError::WrongSize {
            expected: manifest.size,
            actual: written,
        });
    }
    Ok(())
}

/// Fetches the first [`DATA_PIECES`] valid pieces of a chunk; every other
/// piece is `None`.
///
/// It asks the [`DATA_PIECES`] nodes that answered fastest last time, and
/// one more node each time a piece fails or nothing arrives for
/// [`HEDGE_DELAY`]. Requests still running once enough pieces are in are
/// dropped, which cancels them.
async fn fetch_pieces(client: &NodeClient, pieces: &[PlacedPiece]) -> Vec<Option<Vec<u8>>> {
    let mut not_asked = fastest_first(client.latencies(), pieces).into_iter();
    let mut running = FuturesUnordered::new();
    for index in not_asked.by_ref().take(DATA_PIECES) {
        running.push(fetch_piece(client, pieces, index));
    }
    let mut found = vec![None; pieces.len()];
    let mut arrived = 0;
    while arrived < DATA_PIECES {
        let answer = match timeout(HEDGE_DELAY, running.next()).await {
            Ok(Some(answer)) => Some(answer),
            // Every node has been asked and every request has ended.
            Ok(None) => break,
            Err(_) => None,
        };
        if let Some((index, Some(bytes))) = answer {
            found[index] = Some(bytes);
            arrived += 1;
            continue;
        }
        // A piece failed or none arrived in time: ask another node alongside.
        if let Some(index) = not_asked.next() {
            running.push(fetch_piece(client, pieces, index));
        }
    }
    found
}

/// Piece `index` of `pieces` with its index, or `None` when it cannot be had.
async fn fetch_piece(
    client: &NodeClient,
    pieces: &[PlacedPiece],
    index: usize,
) -> (usize, Option<Vec<u8>>) {
    let piece = &pieces[index];
    match client.get(&piece.node, &piece.id).await {
        Ok(bytes) => (index, bytes),
        Err(error) => {
            eprintln!("{error}");
            (index, None)
        }
    }
}

/// The indexes of `pieces`, those on the nodes that answered fastest last
/// time first.
///
/// A node never asked counts as fast, so it gets measured. Ties keep piece
/// order, which puts the data pieces first: when those arrive, rebuilding
/// needs no decoding.
fn fastest_first(latencies: &LatencyTable, pieces: &[PlacedPiece]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..pieces.len()).collect();
    order.sort_by_key(|&index| {
        latencies
            .last_answer(&pieces[index].node)
            .unwrap_or(Duration::ZERO)
    });
    order
}

#[cfg(test)]
mod tests {
    use mesh_core::PieceId;
    use mesh_core::merkle::root_of;

    use super::*;

    fn pieces_on(nodes: &[&str]) -> Vec<PlacedPiece> {
        nodes
            .iter()
            .map(|node| PlacedPiece {
                id: PieceId::of(node.as_bytes()),
                merkle_root: root_of(node.as_bytes()),
                node: node.to_string(),
            })
            .collect()
    }

    #[test]
    fn unmeasured_nodes_keep_piece_order() {
        let pieces = pieces_on(&["a", "b", "c", "d", "e", "f"]);

        let order = fastest_first(&LatencyTable::default(), &pieces);

        assert_eq!(order, [0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn slow_nodes_go_last_and_unmeasured_ones_first() {
        let pieces = pieces_on(&["a", "b", "c", "d", "e", "f"]);
        let latencies = LatencyTable::default();
        latencies.record("a", Duration::from_millis(900));
        latencies.record("b", Duration::from_millis(20));
        latencies.record("c", Duration::from_millis(30));
        latencies.record("f", Duration::from_millis(10));

        let order = fastest_first(&latencies, &pieces);

        assert_eq!(order, [3, 4, 5, 1, 2, 0]);
    }
}

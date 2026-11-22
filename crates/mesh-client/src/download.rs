//! Getting a file back: fetch each chunk's pieces, rebuild, open, write.
//! Several chunks are fetched at once, so a file uses many nodes in parallel.

use std::time::Duration;

use futures::StreamExt;
use futures::stream::{self, FuturesUnordered};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use crate::encryption::open;
use crate::erasure::{RebuildError, rebuild};
use crate::manifest::{ChunkRecord, Manifest, PlacedPiece};
use crate::node_client::NodeClient;
use crate::node_latency::LatencyTable;
use crate::node_list::NodeList;

/// How many chunks a download fetches at once. More chunks use more nodes at
/// the same time; each one in flight holds up to 4 MiB of pieces and its
/// opened bytes, so four keep a download within a few dozen MiB.
pub const CHUNKS_IN_FLIGHT: usize = 4;

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

/// Reads the file `manifest` describes into `writer`, fetching up to
/// [`CHUNKS_IN_FLIGHT`] chunks at once and writing them in order, so the file
/// never has to fit in memory.
///
/// A chunk survives as long as as many of its pieces as it has data pieces
/// can be fetched, four of six in the default layout. Nodes
/// that are down, have lost a piece or return wrong bytes all count the same:
/// that piece is missing.
///
/// On an error, `writer` may already hold the chunks before the failing one,
/// so whatever it writes to should be discarded.
///
/// Nodes are found at their addresses in `list`; a node it does not have
/// counts as unreachable.
pub async fn download(
    client: &NodeClient,
    list: &NodeList,
    manifest: &Manifest,
    mut writer: impl AsyncWrite + Unpin,
) -> Result<(), DownloadError> {
    // `buffered` runs the fetches side by side but yields them in chunk
    // order, which is the order the file needs them in.
    let mut chunks = stream::iter(manifest.chunks.iter().enumerate())
        .map(|(index, chunk)| fetch_chunk(client, list, index, chunk))
        .buffered(CHUNKS_IN_FLIGHT);
    let mut written = 0;
    while let Some(plaintext) = chunks.next().await {
        let plaintext = plaintext?;
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

/// Fetches, rebuilds and opens chunk `index`, returning its bytes.
async fn fetch_chunk(
    client: &NodeClient,
    list: &NodeList,
    index: usize,
    chunk: &ChunkRecord,
) -> Result<Vec<u8>, DownloadError> {
    // The manifest was written by `upload`, which numbers chunks with a u32.
    let index = u32::try_from(index).expect("chunk indexes fit a u32");
    let pieces = fetch_pieces(client, list, &chunk.pieces, chunk.layout.data()).await;
    let sealed = rebuild(&pieces, chunk.sealed_len as usize, chunk.layout)
        .map_err(|source| DownloadError::Rebuild { index, source })?;
    let mut plaintext = open(&chunk.key, &sealed).map_err(|_| DownloadError::Open { index })?;
    // Chunks stored before padding was dropped open with zeros after their
    // bytes; cutting to the recorded length drops them, and does nothing to
    // newer chunks.
    plaintext.truncate(chunk.plain_len as usize);
    Ok(plaintext)
}

/// Fetches the first `needed` valid pieces of a chunk, as many as it has data
/// pieces; every other piece is `None`.
///
/// It asks the `needed` nodes that answered fastest last time, and one more
/// node each time a piece fails or nothing arrives for [`HEDGE_DELAY`].
/// Requests still running once enough pieces are in are dropped, which
/// cancels them.
async fn fetch_pieces(
    client: &NodeClient,
    list: &NodeList,
    pieces: &[PlacedPiece],
    needed: usize,
) -> Vec<Option<Vec<u8>>> {
    let urls: Vec<Option<&str>> = pieces
        .iter()
        .map(|piece| list.url_of(&piece.node))
        .collect();
    let mut not_asked = fastest_first(client.latencies(), &urls).into_iter();
    let mut running = FuturesUnordered::new();
    for index in not_asked.by_ref().take(needed) {
        running.push(fetch_piece(client, pieces, &urls, index));
    }
    let mut found = vec![None; pieces.len()];
    let mut arrived = 0;
    while arrived < needed {
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
            running.push(fetch_piece(client, pieces, &urls, index));
        }
    }
    found
}

/// Piece `index` of `pieces` with its index, or `None` when it cannot be had.
/// `urls` holds the address of each piece's node, `None` for a node the list
/// does not have.
async fn fetch_piece(
    client: &NodeClient,
    pieces: &[PlacedPiece],
    urls: &[Option<&str>],
    index: usize,
) -> (usize, Option<Vec<u8>>) {
    let piece = &pieces[index];
    let Some(url) = urls[index] else {
        eprintln!("node {} is not in the node list", piece.node);
        return (index, None);
    };
    match client.get(url, &piece.id).await {
        Ok(bytes) => (index, bytes),
        Err(error) => {
            eprintln!("{error}");
            (index, None)
        }
    }
}

/// The indexes of the pieces whose nodes are at `urls`, those on the nodes
/// that answered fastest last time first.
///
/// A node never asked counts as fast, so it gets measured; a node missing from
/// the list goes last, since asking it can't work. Ties keep piece order,
/// which puts the data pieces first: when those arrive, rebuilding needs no
/// decoding.
fn fastest_first(latencies: &LatencyTable, urls: &[Option<&str>]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..urls.len()).collect();
    order.sort_by_key(|&index| match urls[index] {
        Some(url) => latencies.last_answer(url).unwrap_or(Duration::ZERO),
        None => Duration::MAX,
    });
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    const URLS: [Option<&str>; 6] = [
        Some("a"),
        Some("b"),
        Some("c"),
        Some("d"),
        Some("e"),
        Some("f"),
    ];

    #[test]
    fn unmeasured_nodes_keep_piece_order() {
        let order = fastest_first(&LatencyTable::default(), &URLS);

        assert_eq!(order, [0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn slow_nodes_go_last_and_unmeasured_ones_first() {
        let latencies = LatencyTable::default();
        latencies.record("a", Duration::from_millis(900));
        latencies.record("b", Duration::from_millis(20));
        latencies.record("c", Duration::from_millis(30));
        latencies.record("f", Duration::from_millis(10));

        let order = fastest_first(&latencies, &URLS);

        assert_eq!(order, [3, 4, 5, 1, 2, 0]);
    }

    #[test]
    fn nodes_missing_from_the_list_go_last() {
        let urls = [None, Some("b"), Some("c"), Some("d"), Some("e"), Some("f")];

        let order = fastest_first(&LatencyTable::default(), &urls);

        assert_eq!(order, [1, 2, 3, 4, 5, 0]);
    }
}

//! Getting a file back: fetch each chunk's pieces, rebuild, open, write.

use futures::future::join_all;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::encryption::{OpenError, open_chunk};
use crate::erasure::{RebuildError, rebuild};
use crate::manifest::{Manifest, PlacedPiece};
use crate::node_client::NodeClient;

/// Why a file could not be read back.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("chunk {index}: {source}")]
    Rebuild { index: u32, source: RebuildError },
    #[error(transparent)]
    Open(#[from] OpenError),
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
        let plaintext = open_chunk(&manifest.key, index, &sealed)?;
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

/// Fetches every piece of a chunk at once; a piece that cannot be had is `None`.
///
/// Asking all six nodes and waiting for every answer is the simplest correct
/// version. Asking only the fastest few and stopping at four is #8.
async fn fetch_pieces(client: &NodeClient, pieces: &[PlacedPiece]) -> Vec<Option<Vec<u8>>> {
    let fetches = pieces.iter().map(|piece| async move {
        match client.get(&piece.node, &piece.id).await {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("{error}");
                None
            }
        }
    });
    join_all(fetches).await
}

//! Storing a file: chunk, seal, cut into pieces, put each piece on a node.

use std::time::Duration;

use mesh_core::protocol::MAX_PIECE_BYTES;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::encryption::{FileKey, SEAL_OVERHEAD, seal_chunk};
use crate::erasure::{DATA_PIECES, TOTAL_PIECES, cut};
use crate::manifest::{ChunkRecord, Manifest};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::placement::store_pieces;

/// Bytes of the file in each chunk; the last chunk may be shorter.
///
/// A chunk is the unit that is sealed and coded together, so a reader fetches
/// whole chunks. 1 MiB keeps pieces around 256 KiB: small enough to fetch
/// quickly, large enough that per-request overhead stays low.
pub const CHUNK_BYTES: usize = 1024 * 1024;

// Every piece must fit what nodes accept. A piece is a quarter of a sealed
// chunk, plus at most one byte of padding to make its length even.
const _: () = assert!((CHUNK_BYTES + SEAL_OVERHEAD).div_ceil(DATA_PIECES) < MAX_PIECE_BYTES);

/// Why a file was not stored.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("reading the file: {0}")]
    Read(#[from] std::io::Error),
    #[error("chunk {chunk}: only {placed} of {TOTAL_PIECES} pieces found a node that takes them")]
    NotEnoughNodes { chunk: u32, placed: usize },
}

/// Stores the file `reader` yields on nodes chosen from `list` for `lease`,
/// and returns the manifest that gets it back.
///
/// The file is read one chunk at a time, so it never has to fit in memory.
/// Every chunk's six pieces go to six nodes the placement rules allow, chosen
/// anew for each chunk. A node that refuses a piece is replaced by another.
/// The nodes delete the pieces once the lease ends unless [`renew`] extends
/// it.
///
/// [`renew`]: crate::renew
pub async fn upload(
    client: &NodeClient,
    list: &NodeList,
    mut reader: impl AsyncRead + Unpin,
    lease: Duration,
) -> Result<Manifest, UploadError> {
    let key = FileKey::generate();
    let mut chunks = Vec::new();
    let mut size = 0;
    let mut plaintext = Vec::with_capacity(CHUNK_BYTES);
    loop {
        plaintext.clear();
        // `take` stops at one chunk; `read_to_end` keeps reading until then,
        // however few bytes each read returns.
        (&mut reader)
            .take(CHUNK_BYTES as u64)
            .read_to_end(&mut plaintext)
            .await?;
        if plaintext.is_empty() {
            break;
        }
        // `CHUNK_BYTES` is 1 MiB, so a u32 index covers files up to 4 PiB.
        let index = u32::try_from(chunks.len()).expect("files stay below 4 PiB");
        chunks.push(store_chunk(client, list, &key, index, &plaintext, lease).await?);
        size += plaintext.len() as u64;
    }
    Ok(Manifest { key, size, chunks })
}

/// Seals chunk `index`, cuts it and stores its six pieces.
async fn store_chunk(
    client: &NodeClient,
    list: &NodeList,
    key: &FileKey,
    index: u32,
    plaintext: &[u8],
    lease: Duration,
) -> Result<ChunkRecord, UploadError> {
    let sealed = seal_chunk(key, index, plaintext);
    let sealed_len = u32::try_from(sealed.len()).expect("a sealed chunk is about 1 MiB");
    let placed = store_pieces(client, list, &cut(&sealed), &[], &[], lease).await;
    let placed_count = placed.iter().filter(|piece| piece.is_some()).count();
    // A chunk with a piece missing from the start would have one loss less to
    // spare, so the upload fails instead.
    let Some(pieces) = placed.into_iter().collect::<Option<Vec<_>>>() else {
        return Err(UploadError::NotEnoughNodes {
            chunk: index,
            placed: placed_count,
        });
    };
    Ok(ChunkRecord { sealed_len, pieces })
}

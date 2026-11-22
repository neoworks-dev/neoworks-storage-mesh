//! Storing a file: chunk, seal, cut into pieces, put each piece on a node.

use futures::future::try_join_all;
use mesh_core::PieceId;
use mesh_core::protocol::MAX_PIECE_BYTES;

use crate::encryption::{FileKey, SEAL_OVERHEAD, seal_chunk};
use crate::erasure::{DATA_PIECES, TOTAL_PIECES, cut};
use crate::manifest::{ChunkRecord, Manifest, PlacedPiece};
use crate::node_client::{NodeClient, NodeRequestError};

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
    #[error("{given} nodes were given, a file needs exactly {TOTAL_PIECES}")]
    WrongNodeCount { given: usize },
    #[error(transparent)]
    Node(#[from] NodeRequestError),
}

/// Stores `file` on `nodes`, one piece of every chunk on each node, and
/// returns the manifest that gets it back.
///
/// Every piece has to be stored for the upload to succeed. Choosing nodes and
/// retrying on others comes later; for now the caller names exactly
/// [`TOTAL_PIECES`] nodes.
pub async fn upload(
    client: &NodeClient,
    nodes: &[String],
    file: &[u8],
) -> Result<Manifest, UploadError> {
    if nodes.len() != TOTAL_PIECES {
        return Err(UploadError::WrongNodeCount { given: nodes.len() });
    }
    let key = FileKey::generate();
    let mut chunks = Vec::new();
    for (index, plaintext) in file.chunks(CHUNK_BYTES).enumerate() {
        // `CHUNK_BYTES` is 1 MiB, so a u32 index covers files up to 4 PiB.
        let index = u32::try_from(index).expect("files stay below 4 PiB");
        let sealed = seal_chunk(&key, index, plaintext);
        let sealed_len = u32::try_from(sealed.len()).expect("a sealed chunk is about 1 MiB");
        let pieces = store_pieces(client, nodes, cut(&sealed)).await?;
        chunks.push(ChunkRecord { sealed_len, pieces });
    }
    Ok(Manifest {
        key,
        size: file.len() as u64,
        chunks,
    })
}

/// Puts piece `i` on node `i`, all at once.
async fn store_pieces(
    client: &NodeClient,
    nodes: &[String],
    pieces: Vec<Vec<u8>>,
) -> Result<Vec<PlacedPiece>, NodeRequestError> {
    let puts = nodes.iter().zip(pieces).map(|(node, bytes)| async move {
        let id = PieceId::of(&bytes);
        client.put(node, &id, bytes).await?;
        Ok(PlacedPiece {
            id,
            node: node.clone(),
        })
    });
    try_join_all(puts).await
}

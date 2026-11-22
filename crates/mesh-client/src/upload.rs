//! Storing a file: cut it into chunks, seal them, cut each into pieces and put
//! each piece on a node.

use std::collections::HashMap;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use futures::future::join_all;
use mesh_core::layout::{MAX_PIECES, MIN_DATA_PIECES};
use mesh_core::protocol::MAX_PIECE_BYTES;
use tokio::io::AsyncRead;

use crate::chunking::{MAX_CHUNK_BYTES, chunker};
use crate::encryption::{ChunkKey, DedupKey, SEAL_OVERHEAD, seal};
use crate::erasure::cut;
use crate::manifest::{ChunkRecord, Manifest};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::placement::{Spread, store_pieces};

// Every piece must fit what nodes accept. A piece is the sealed chunk's share
// for one data piece, rounded up to an even length, no chunk is larger than
// `MAX_CHUNK_BYTES`, and no layout has fewer than `MIN_DATA_PIECES`.
const _: () =
    assert!((MAX_CHUNK_BYTES + SEAL_OVERHEAD).div_ceil(MIN_DATA_PIECES) < MAX_PIECE_BYTES);

/// Why a file was not stored.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("reading the file: {0}")]
    Read(#[from] std::io::Error),
    #[error("chunk {chunk}: only {placed} of {needed} pieces found a node that takes them")]
    NotEnoughNodes {
        chunk: u32,
        placed: usize,
        needed: usize,
    },
}

/// Stores the file `reader` yields on nodes chosen from `list` for `lease`,
/// in the default layout of 4 + 2 pieces, and returns the manifest that gets
/// it back. [`upload_spread`] stores with another layout.
///
/// The file is read one chunk at a time, so it never has to fit in memory.
/// Chunk keys come from `dedup_key`, so a chunk that is already in one of the
/// `earlier` manifests, or earlier in this file, is not stored again: its
/// pieces are renewed and the new manifest names them too. Every other chunk's
/// six pieces go to six nodes the placement rules allow.
pub async fn upload(
    client: &NodeClient,
    list: &NodeList,
    dedup_key: &DedupKey,
    reader: impl AsyncRead + Unpin,
    lease: Duration,
    earlier: &[Manifest],
) -> Result<Manifest, UploadError> {
    let stored = Stored {
        client,
        list,
        lease,
        spread: Spread::DEFAULT,
    };
    upload_spread(&stored, dedup_key, reader, earlier).await
}

/// Where and how a file is stored: on which nodes, for how long, cut and
/// spread how.
pub struct Stored<'a> {
    pub client: &'a NodeClient,
    pub list: &'a NodeList,
    pub lease: Duration,
    pub spread: Spread,
}

/// Stores the file `reader` yields as `stored` says, like [`upload`] does
/// with the default layout.
///
/// A chunk already stored under another layout, in an `earlier` manifest,
/// keeps that layout: it is already on nodes, and storing it again would pay
/// twice for the same bytes.
pub async fn upload_spread(
    stored: &Stored<'_>,
    dedup_key: &DedupKey,
    reader: impl AsyncRead + Unpin,
    earlier: &[Manifest],
) -> Result<Manifest, UploadError> {
    let Stored {
        client,
        list,
        lease,
        ..
    } = *stored;
    let mut known: HashMap<ChunkKey, ChunkRecord> = earlier
        .iter()
        .flat_map(|manifest| &manifest.chunks)
        .map(|chunk| (chunk.key.clone(), chunk.clone()))
        .collect();
    let mut chunks = Vec::new();
    let mut size = 0;
    let mut chunker = chunker(reader);
    let mut file_chunks = pin!(chunker.as_stream());
    while let Some(chunk) = file_chunks.next().await {
        let plaintext = chunk.map_err(std::io::Error::from)?.data;
        // Chunks are at least 256 KiB except the last, so a u32 index covers
        // files up to a petabyte.
        let index = u32::try_from(chunks.len()).expect("files stay below a petabyte");
        let key = dedup_key.chunk_key(&plaintext);
        let record = match known.get(&key) {
            Some(record) if renew_all(client, list, record, lease).await => record.clone(),
            _ => store_chunk(stored, key, index, &plaintext).await?,
        };
        known.insert(record.key.clone(), record.clone());
        chunks.push(record);
        size += plaintext.len() as u64;
    }
    Ok(Manifest { size, chunks })
}

/// Seals chunk `index` under `key`, cuts it in the layout `stored` asks for
/// and stores its pieces.
async fn store_chunk(
    stored: &Stored<'_>,
    key: ChunkKey,
    index: u32,
    plaintext: &[u8],
) -> Result<ChunkRecord, UploadError> {
    let layout = stored.spread.layout;
    let sealed = seal(&key, plaintext);
    let plain_len = u32::try_from(plaintext.len()).expect("a chunk is at most 4 MiB");
    let sealed_len = u32::try_from(sealed.len()).expect("a sealed chunk is at most 4 MiB");
    let placed = store_pieces(
        stored.client,
        stored.list,
        &cut(&sealed, layout),
        &[],
        &[],
        stored.spread.limits(),
        stored.lease,
    )
    .await;
    let placed_count = placed.iter().filter(|piece| piece.is_some()).count();
    // A chunk with a piece missing from the start would have one loss less to
    // spare, so the upload fails instead.
    let Some(pieces) = placed.into_iter().collect::<Option<Vec<_>>>() else {
        return Err(UploadError::NotEnoughNodes {
            chunk: index,
            placed: placed_count,
            needed: layout.total(),
        });
    };
    Ok(ChunkRecord {
        key,
        plain_len,
        sealed_len,
        layout,
        // More regions than pieces spread no further, and `MAX_PIECES` fits
        // a byte.
        regions: stored.spread.regions.min(MAX_PIECES) as u8,
        pieces,
    })
}

/// Renews every piece of an already stored chunk for `lease`, and whether
/// all of them still were there to renew. A chunk missing a piece, or with a
/// piece on a node `list` no longer has, is stored again instead, rather than
/// starting the new file with a loss.
async fn renew_all(
    client: &NodeClient,
    list: &NodeList,
    record: &ChunkRecord,
    lease: Duration,
) -> bool {
    let Some(urls) = record
        .pieces
        .iter()
        .map(|piece| list.url_of(&piece.node))
        .collect::<Option<Vec<&str>>>()
    else {
        return false;
    };
    let renewals = record
        .pieces
        .iter()
        .zip(urls)
        .map(|(piece, url)| client.renew(url, &piece.id, lease));
    join_all(renewals)
        .await
        .into_iter()
        .all(|result| matches!(result, Ok(Some(_))))
}

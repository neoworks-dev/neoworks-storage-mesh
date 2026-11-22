//! The manifest: everything needed to get a stored file back.
//!
//! It holds, for every chunk, the chunk's key and which piece is on which
//! node. Whoever has the manifest can read the file and nobody else can, so it
//! stays with the owner. Storing manifests in the mesh itself, encrypted,
//! comes with the index (#10).
//!
//! Two manifests may name the same chunk: a chunk that appears in two files,
//! or in two versions of one, is stored once. Each file's renewals keep the
//! shared pieces alive, so deleting one file leaves the other whole.

use mesh_core::PieceId;
use mesh_core::merkle::MerkleHash;
use serde::{Deserialize, Serialize};

use crate::encryption::ChunkKey;

/// How to find and open one stored file.
#[derive(Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// The file's length in bytes.
    pub size: u64,
    /// The file's chunks, in order.
    pub chunks: Vec<ChunkRecord>,
}

/// One chunk: how to open it and where its pieces are.
#[derive(Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    /// The key the chunk is sealed with.
    pub key: ChunkKey,
    /// The chunk's length. Chunks stored before padding was dropped were
    /// padded to a size bucket, and this is their length before padding.
    pub plain_len: u32,
    /// The sealed chunk's length, which the zeros filling the last data piece
    /// would otherwise hide.
    pub sealed_len: u32,
    /// The chunk's pieces in coding order: data pieces first, then parity.
    pub pieces: Vec<PlacedPiece>,
}

/// One piece and the node holding it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlacedPiece {
    pub id: PieceId,
    /// The root of the piece's segment tree, which lets anyone holding the
    /// manifest check that the node still has the piece without downloading
    /// it. It reveals nothing about the piece's contents.
    pub merkle_root: MerkleHash,
    /// The node's base URL, e.g. `http://127.0.0.1:7001`.
    pub node: String,
}

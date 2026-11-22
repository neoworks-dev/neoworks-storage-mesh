//! The manifest: everything needed to get a stored file back.
//!
//! It holds the file's key and, for every chunk, which piece is on which node.
//! Whoever has the manifest can read the file and nobody else can, so it stays
//! with the owner. Storing manifests in the mesh itself, encrypted, comes later.

use mesh_core::PieceId;
use serde::{Deserialize, Serialize};

use crate::encryption::FileKey;

/// How to find and open one stored file.
#[derive(Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// The key every chunk of the file is sealed with.
    pub key: FileKey,
    /// The file's length in bytes.
    pub size: u64,
    /// The file's chunks, in order.
    pub chunks: Vec<ChunkRecord>,
}

/// Where the pieces of one chunk are.
#[derive(Clone, Serialize, Deserialize)]
pub struct ChunkRecord {
    /// The sealed chunk's length, which the padding of the last data piece
    /// would otherwise hide.
    pub sealed_len: u32,
    /// The chunk's pieces in coding order: data pieces first, then parity.
    pub pieces: Vec<PlacedPiece>,
}

/// One piece and the node holding it.
#[derive(Clone, Serialize, Deserialize)]
pub struct PlacedPiece {
    pub id: PieceId,
    /// The node's base URL, e.g. `http://127.0.0.1:7001`.
    pub node: String,
}

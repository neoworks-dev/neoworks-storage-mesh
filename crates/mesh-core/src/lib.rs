//! Types shared by every role in the storage mesh.
//!
//! Nothing here does I/O. Nodes and clients both depend on this crate, so it
//! holds exactly what they have to agree on: how pieces and nodes are
//! addressed, how a node proves it holds a piece, what a signed pointer is,
//! and what the requests between them look like.

mod canonical_hex;
pub mod merkle;
mod node_id;
mod piece_id;
pub mod pointer;
pub mod protocol;

pub use node_id::{NodeId, ParseNodeIdError};
pub use piece_id::{ParsePieceIdError, PieceId};

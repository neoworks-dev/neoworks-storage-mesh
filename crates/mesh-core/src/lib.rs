//! Types shared by every role in the storage mesh.
//!
//! Nothing here does I/O. Nodes and clients both depend on this crate, so it
//! holds exactly what they have to agree on: how pieces are addressed and what
//! the requests between them look like.

mod piece_id;
pub mod protocol;

pub use piece_id::{ParsePieceIdError, PieceId};

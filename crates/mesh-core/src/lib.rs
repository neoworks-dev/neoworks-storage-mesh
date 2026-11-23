//! Types shared by every role in the storage mesh.
//!
//! Nothing here does I/O. Nodes, clients and coordinators all depend on this
//! crate, so it holds exactly what they have to agree on: how pieces and nodes
//! are addressed, how a chunk is cut into pieces, how a node proves it holds
//! a piece, what a signed pointer is, what the signed node list holds, what
//! storage costs, how mailboxes are addressed, and what the requests between
//! them look like.

mod canonical_hex;
pub mod chunk_registry;
pub mod earnings;
pub mod gateway;
pub mod layout;
pub mod mailbox;
pub mod merkle;
mod node_id;
pub mod node_list;
mod piece_id;
pub mod pointer;
pub mod prices;
pub mod protocol;
pub mod sign_in;
pub mod signed;
pub mod tabs;
pub mod tokens;

pub use node_id::{NodeId, ParseNodeIdError};
pub use piece_id::{ParsePieceIdError, PieceId};

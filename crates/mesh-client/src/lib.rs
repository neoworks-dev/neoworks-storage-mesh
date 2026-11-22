//! The device side of the storage mesh.
//!
//! Storing a file runs entirely on the device:
//!
//! 1. The file is cut into chunks of [`CHUNK_BYTES`].
//! 2. Each chunk is sealed with the file's random key ([`encryption`]).
//! 3. Each sealed chunk is cut into six pieces, any four of which rebuild it
//!    ([`erasure`]).
//! 4. Each piece goes to a different node, which sees only noise, and is kept
//!    there for a lease the owner sets.
//!
//! The [`Manifest`] that comes out holds the key and where every piece is.
//! [`download`] uses it to fetch, rebuild and open the file again, [`renew`]
//! to keep it stored, [`audit`] to check that the nodes still hold it, and
//! [`repair`] to replace the pieces they lost. A file nobody renews expires.

mod audit;
mod download;
pub mod encryption;
pub mod erasure;
mod manifest;
mod node_client;
mod renew;
mod repair;
mod upload;

pub use audit::{AUDIT_DEADLINE, AUDIT_SEGMENTS, AuditReport, ChunkAudit, PieceHealth, audit};
pub use download::{DownloadError, download};
pub use manifest::{ChunkRecord, Manifest, PlacedPiece};
pub use node_client::{NodeClient, NodeRequestError};
pub use renew::{RenewReport, renew};
pub use repair::{ChunkRepair, REPAIR_AT, RepairReport, repair};
pub use upload::{CHUNK_BYTES, UploadError, upload};

//! The device side of the storage mesh.
//!
//! Storing a file runs entirely on the device:
//!
//! 1. The file is cut into chunks where its content says, about 1 MiB each,
//!    and each chunk is padded to a size bucket ([`chunking`]).
//! 2. Each chunk is sealed under a key derived from its bytes and the owner's
//!    [`DedupKey`], so a chunk the owner already stored is not stored again
//!    ([`encryption`]).
//! 3. Each sealed chunk is cut into six pieces, any four of which rebuild it
//!    ([`erasure`]).
//! 4. Each piece goes to a different node from a [`NodeList`], chosen so that
//!    no operator or network holds too many ([`placement`]). The node sees
//!    only noise and keeps it for a lease the owner sets.
//!
//! The [`Manifest`] that comes out holds the chunk keys and where every piece
//! is. [`download`] uses it to fetch, rebuild and open the file again,
//! [`renew`] to keep it stored, [`audit`] to check that the nodes still hold
//! it, and [`repair`] to replace the pieces they lost. A file nobody renews
//! expires.
//!
//! A signed pointer ([`pointers`]) is the one place whose address does not
//! change: it sits on nodes picked from the owner's key and says where the
//! newest version of something is.
//!
//! Structured data, such as a calendar, is a [`version_graph`] of commits
//! like git's: each device keeps the whole graph, stores its commits in the
//! mesh, and names its newest one in a pointer of its own ([`commits`]).

pub mod account;
mod audit;
pub mod batch_renew;
pub mod chunking;
pub mod commits;
mod download;
pub mod encryption;
pub mod erasure;
pub mod index;
mod manifest;
pub mod merge;
mod node_client;
mod node_latency;
mod node_list;
pub mod placement;
pub mod pointers;
mod random;
mod renew;
mod repair;
mod upload;
pub mod version_graph;

pub use audit::{AUDIT_DEADLINE, AUDIT_SEGMENTS, AuditReport, ChunkAudit, PieceHealth, audit};
pub use download::{DownloadError, download};
pub use encryption::DedupKey;
pub use manifest::{ChunkRecord, Manifest, PlacedPiece};
pub use node_client::{NodeClient, NodeRequestError, PointerStored};
pub use node_list::{NodeInfo, NodeList, NodeListError};
pub use renew::{RenewReport, renew};
pub use repair::{ChunkRepair, REPAIR_AT, RepairReport, repair};
pub use upload::{UploadError, upload};

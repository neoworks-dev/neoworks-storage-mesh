//! The device side of the storage mesh.
//!
//! Storing a file runs entirely on the device:
//!
//! 1. A file of up to 8 MiB is one chunk; a larger one is cut into chunks
//!    where its content says, about 4 MiB each ([`chunking`]).
//! 2. Each chunk is sealed under a key derived from its bytes and the owner's
//!    [`DedupKey`], so a chunk the owner already stored is not stored again
//!    ([`encryption`]).
//! 3. Each sealed chunk is cut into data and parity pieces, any `data` of
//!    which rebuild it ([`erasure`]): 4 + 2 by default, or the cheapest
//!    layout that meets what the file is worth to the app
//!    ([`layout_choice`]).
//! 4. Each piece goes to a different node from a [`NodeList`], chosen so that
//!    no operator, network or region holds too many ([`placement`]). The node
//!    sees only noise and keeps it for a lease the owner sets.
//!
//! The [`Manifest`] that comes out holds the chunk keys and where every piece
//! is. [`download`] uses it to fetch, rebuild and open the file again,
//! [`renew`] to keep it stored, [`audit`] to check that the nodes still hold
//! it, and [`repair`] to replace the pieces they lost. A file nobody renews
//! expires. Registered with a coordinator ([`chunk_registration`]), a file
//! is repaired there too while the owner's devices are off.
//!
//! A signed pointer ([`pointers`]) is the one place whose address does not
//! change: it sits on nodes picked from the owner's key and says where the
//! newest version of something is.
//!
//! Structured data, such as a calendar, is a [`version_graph`] of commits
//! like git's: each device keeps the whole graph, stores its commits in the
//! mesh, and names its newest one in a pointer ([`commits`]). Each such
//! [`collection`] has keys of its own, kept in the account's [`index`], so
//! one collection can be shared without the rest.

pub mod account;
mod audit;
pub mod batch_renew;
pub mod chunk_registration;
pub mod chunking;
pub mod collection;
pub mod commits;
mod download;
pub mod encryption;
pub mod erasure;
pub mod index;
pub mod layout_choice;
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
pub mod sign_in;
pub mod tabs;
pub mod tokens;
mod upload;
pub mod version_graph;

pub use audit::{AUDIT_DEADLINE, AUDIT_SEGMENTS, AuditReport, ChunkAudit, PieceHealth, audit};
pub use download::{CHUNKS_IN_FLIGHT, DownloadError, download};
pub use encryption::DedupKey;
pub use manifest::{ChunkRecord, Manifest, PlacedPiece};
pub use node_client::{NodeClient, NodeRequestError, PointerStored};
pub use node_list::{FetchedNodeList, NodeInfo, NodeList, NodeListError};
pub use renew::{RenewReport, renew};
pub use repair::{ChunkRepair, RepairReport, repair};
pub use upload::{Stored, UploadError, upload, upload_spread};

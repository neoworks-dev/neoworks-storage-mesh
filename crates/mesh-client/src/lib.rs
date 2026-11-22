//! The device side of the storage mesh.
//!
//! Storing a file runs entirely on the device:
//!
//! 1. The file is cut into chunks of [`CHUNK_BYTES`].
//! 2. Each chunk is sealed with the file's random key ([`encryption`]).
//! 3. Each sealed chunk is cut into six pieces, any four of which rebuild it
//!    ([`erasure`]).
//! 4. Each piece goes to a different node, which sees only noise.
//!
//! The [`Manifest`] that comes out holds the key and where every piece is;
//! [`download`] uses it to fetch, rebuild and open the file again.

mod download;
pub mod encryption;
pub mod erasure;
mod manifest;
mod node_client;
mod upload;

pub use download::{DownloadError, download};
pub use manifest::{ChunkRecord, Manifest, PlacedPiece};
pub use node_client::{NodeClient, NodeRequestError};
pub use upload::{CHUNK_BYTES, UploadError, upload};

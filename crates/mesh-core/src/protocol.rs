//! What a node and a client agree on over HTTP.
//!
//! Every piece is stored under a lease: the node keeps it until the lease
//! ends and then deletes it, unless someone renews it first. Nobody can delete
//! a piece directly, since a node does not know whose piece it is; letting a
//! lease run out is how data is deleted.
//!
//! A node answers three requests:
//!
//! - `PUT /pieces/{id}?lease_seconds=N` with the piece's bytes as the body
//!   stores it for `N` seconds. The node refuses bytes that do not hash to
//!   `id`, and leases longer than it grants.
//! - `POST /pieces/{id}/lease?lease_seconds=N` renews the lease to end no
//!   sooner than `N` seconds from now, or answers `404 Not Found`.
//! - `GET /pieces/{id}` returns the bytes, or `404 Not Found`.
//!
//! Storing and renewing both answer with a [`LeaseResponse`] in JSON.
//! Lease ends are Unix times in seconds on the node's clock; clients send a
//! length rather than an end, so their clocks never have to agree with it.

use serde::{Deserialize, Serialize};

use crate::PieceId;

/// The route of a piece, in the router syntax of `axum`.
pub const PIECE_ROUTE: &str = "/pieces/{id}";

/// The route of a piece's lease, in the router syntax of `axum`.
pub const LEASE_ROUTE: &str = "/pieces/{id}/lease";

/// The largest piece a node accepts. Clients cut data so that every piece
/// stays below it; nodes use it to refuse oversized uploads before reading
/// them.
pub const MAX_PIECE_BYTES: usize = 4 * 1024 * 1024;

/// The query of a store or renew request: how long the piece should be kept
/// from now.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseQuery {
    pub lease_seconds: u64,
}

/// The answer to a store or renew request: when the piece's lease ends now.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseResponse {
    /// Unix time in seconds on the node's clock.
    pub expires_at: u64,
}

/// The path of the piece `id` on a node, e.g. `/pieces/af13…`.
pub fn piece_path(id: &PieceId) -> String {
    format!("/pieces/{id}")
}

/// The path of the lease of the piece `id`, e.g. `/pieces/af13…/lease`.
pub fn lease_path(id: &PieceId) -> String {
    format!("/pieces/{id}/lease")
}

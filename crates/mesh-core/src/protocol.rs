//! What a node and a client agree on over HTTP.
//!
//! A node answers two requests:
//!
//! - `PUT /pieces/{id}` with the piece's bytes as the body stores it. The node
//!   refuses bytes that do not hash to `id`.
//! - `GET /pieces/{id}` returns the bytes, or `404 Not Found`.

use crate::PieceId;

/// The route of a piece, in the router syntax of `axum`.
pub const PIECE_ROUTE: &str = "/pieces/{id}";

/// The largest piece a node accepts. Clients cut data so that every piece
/// stays below it; nodes use it to refuse oversized uploads before reading
/// them.
pub const MAX_PIECE_BYTES: usize = 4 * 1024 * 1024;

/// The path of the piece `id` on a node, e.g. `/pieces/af13…`.
pub fn piece_path(id: &PieceId) -> String {
    format!("/pieces/{id}")
}

//! The HTTPS gateway every coordinator runs for browsers.
//!
//! A web page served over HTTPS may not fetch from a node that serves plain
//! HTTP, which most home nodes do. Until browsers can reach nodes directly,
//! a coordinator forwards their piece and pointer requests to nodes, at the
//! coordinator's URL on the coordinator list, so a web client whose
//! coordinator is down uses another one.
//!
//! The gateway forwards only to nodes in its signed node list, never
//! decrypts anything, and can't change bytes unnoticed: a piece is checked
//! against its id and a pointer against its owner's signature, by the
//! gateway and again by the client.
//!
//! - `GET` and `PUT /gateway/{node}/pieces/{id}` read and store the piece
//!   `id` on the node with id `node`, with the node's own query parameters.
//! - `GET` and `PUT /gateway/{node}/pointers/{owner}` do the same for
//!   pointers.

use crate::pointer::Pointer;
use crate::{NodeId, PieceId};

/// The route of a piece on a node, through the gateway, in the router
/// syntax of `axum`.
pub const GATEWAY_PIECE_ROUTE: &str = "/gateway/{node}/pieces/{id}";
/// The route of a pointer on a node, through the gateway, in the router
/// syntax of `axum`.
pub const GATEWAY_POINTER_ROUTE: &str = "/gateway/{node}/pointers/{owner}";

/// The path of the piece `id` on `node` through a gateway.
pub fn gateway_piece_path(node: &NodeId, id: &PieceId) -> String {
    format!("/gateway/{node}/pieces/{id}")
}

/// The path of the pointer of `owner` on `node` through a gateway.
pub fn gateway_pointer_path(node: &NodeId, owner: &[u8; 32]) -> String {
    format!("/gateway/{node}/pointers/{}", hex::encode(owner))
}

/// Whether `pointer` is genuine and filed under `owner`. Without the second
/// check a gateway or node could answer for one owner with another owner's
/// validly signed pointer.
pub fn is_pointer_of(pointer: &Pointer, owner: &[u8; 32]) -> bool {
    pointer.owner == *owner && pointer.verify().is_ok()
}

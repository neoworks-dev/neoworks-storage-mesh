//! Tabs: how a node takes storage paid with tokens.
//!
//! A client opens a tab at a node under a fresh random name and pays it with
//! tokens. The node spends each token at the coordinator, and every token
//! spent there adds [`TOKEN_BYTES`](crate::tokens::TOKEN_BYTES) to the tab.
//! A tab lasts one token month from when it was opened, and the pieces
//! stored against it live exactly as long. To keep them, the client opens
//! next month's tab and moves the pieces over, in one request per node.
//!
//! A node sees which pieces share a tab, but not whose tab it is. Names are
//! new for every node and every month, so tabs cannot be linked across nodes.
//!
//! - `POST /tabs/{name}` with an [`OpenTabRequest`] opens the tab, or adds to
//!   it, and answers with a [`TabResponse`].
//! - `PUT /pieces/{id}?tab={name}` stores a piece against the tab; a node
//!   that requires tokens answers `402 Payment Required` without one, and
//!   `507 Insufficient Storage` when the tab is full.
//! - `POST /tabs/{name}/pieces` with a
//!   [`BatchRenewRequest`](crate::protocol::BatchRenewRequest) moves pieces
//!   the node holds into the tab, extending their leases to its end, and
//!   answers with a [`BatchRenewResponse`](crate::protocol::BatchRenewResponse).

use serde::{Deserialize, Serialize};

use crate::tokens::Token;

/// The route of a tab, in the router syntax of `axum`.
pub const TAB_ROUTE: &str = "/tabs/{name}";

/// The route that moves pieces into a tab, in the router syntax of `axum`.
pub const TAB_PIECES_ROUTE: &str = "/tabs/{name}/pieces";

/// The most tokens one request may pay a tab with.
pub const MAX_TOKENS_PER_REQUEST: usize = 1000;

/// The path of the tab `name`.
pub fn tab_path(name: &str) -> String {
    format!("/tabs/{name}")
}

/// The path that moves pieces into the tab `name`.
pub fn tab_pieces_path(name: &str) -> String {
    format!("/tabs/{name}/pieces")
}

/// Whether `name` is a tab name: 32 lowercase hex digits, 16 random bytes.
/// A fixed form keeps names unguessable and fit for a URL.
pub fn is_tab_name(name: &str) -> bool {
    name.len() == 32
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The query of a piece upload: a lease length on a node that grants leases
/// freely, or a tab on one that requires tokens.
#[derive(Debug, Serialize, Deserialize)]
pub struct StoreQuery {
    pub lease_seconds: Option<u64>,
    pub tab: Option<String>,
}

/// Pays a tab.
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenTabRequest {
    pub tokens: Vec<Token>,
}

/// What a tab holds now.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabResponse {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    /// When the tab and its pieces end, in Unix seconds on the node's clock.
    pub expires_at: u64,
}

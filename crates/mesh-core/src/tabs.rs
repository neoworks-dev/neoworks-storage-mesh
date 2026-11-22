//! Tabs: how a node takes storage paid with tokens.
//!
//! A client opens a tab at a node under a fresh random name and pays it with
//! tokens. The node spends each token at the coordinator, and every token
//! spent there adds its value to the tab's balance. A tab lasts one token
//! month from when it was opened. Storing a piece against the tab draws what
//! the piece costs until the tab ends, at the node's published price
//! ([`storage_cost`]), and the piece lives exactly as long as the tab. To
//! keep pieces, the client opens next month's tab and moves them over, in
//! one request per node.
//!
//! A node sees which pieces share a tab, but not whose tab it is. Names are
//! new for every node and every month, so tabs cannot be linked across nodes.
//!
//! - `POST /tabs/{name}` with an [`OpenTabRequest`] opens the tab, or adds to
//!   it, and answers with a [`TabResponse`].
//! - `PUT /pieces/{id}?tab={name}` stores a piece against the tab; a node
//!   that requires tokens answers `402 Payment Required` without one, and
//!   `507 Insufficient Storage` when the tab's balance does not cover it.
//! - `POST /tabs/{name}/pieces` with a
//!   [`BatchRenewRequest`](crate::protocol::BatchRenewRequest) moves pieces
//!   the node holds into the tab, extending their leases to its end, and
//!   answers with a [`BatchRenewResponse`](crate::protocol::BatchRenewResponse).

use serde::{Deserialize, Serialize};

use crate::tokens::{MICROTOKENS_PER_TOKEN, MONTH_SECONDS, Token};

/// The route of a tab, in the router syntax of `axum`.
pub const TAB_ROUTE: &str = "/tabs/{name}";

/// The route that moves pieces into a tab, in the router syntax of `axum`.
pub const TAB_PIECES_ROUTE: &str = "/tabs/{name}/pieces";

/// The most tokens one request may pay a tab with.
pub const MAX_TOKENS_PER_REQUEST: usize = 1000;

/// Bytes in a TB, the unit prices are quoted per.
const BYTES_PER_TB: u128 = 1_000_000_000_000;

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

/// What keeping `bytes` for `seconds` costs at `tokens_per_tb_month`, in
/// millionths of a token, rounded up so that no storage is free.
pub fn storage_cost(bytes: u64, tokens_per_tb_month: u64, seconds: u64) -> u64 {
    let numerator = u128::from(bytes)
        * u128::from(tokens_per_tb_month)
        * u128::from(seconds)
        * u128::from(MICROTOKENS_PER_TOKEN);
    let denominator = BYTES_PER_TB * u128::from(MONTH_SECONDS);
    // Below u64::MAX for anything a node could hold: even a petabyte for a
    // year at a million tokens per TB stays far under it.
    u64::try_from(numerator.div_ceil(denominator)).unwrap_or(u64::MAX)
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
    /// What is left to store with, in millionths of a token.
    pub balance_microtokens: u64,
    /// When the tab and its pieces end, in Unix seconds on the node's clock.
    pub expires_at: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tb_for_a_month_costs_the_price() {
        let tb = 1_000_000_000_000;

        assert_eq!(
            storage_cost(tb, 8_000, MONTH_SECONDS),
            8_000 * MICROTOKENS_PER_TOKEN
        );
        assert_eq!(
            storage_cost(tb / 2, 8_000, MONTH_SECONDS / 2),
            2_000 * MICROTOKENS_PER_TOKEN
        );
    }

    #[test]
    fn even_a_tiny_piece_costs_something() {
        assert_eq!(storage_cost(1, 2_000, 1), 1);
        assert_eq!(storage_cost(0, 2_000, MONTH_SECONDS), 0);
    }

    #[test]
    fn a_mib_on_fast_nodes_for_a_month_costs_eight_thousandths_of_a_token() {
        // 1 MiB at 8,000 tokens per TB and month: about 0.0084 tokens.
        assert_eq!(storage_cost(1 << 20, 8_000, MONTH_SECONDS), 8_389);
    }
}

//! Paying nodes for storage with tabs, and renewing by moving pieces into
//! next month's tab.
//!
//! A tab gets a fresh random name per node and month, so nodes cannot link a
//! device's tabs across nodes. Once a tab is paid on a node, the
//! [`NodeClient`] stores every piece for that node against it.

use std::collections::BTreeMap;

use mesh_core::PieceId;
use mesh_core::tokens::Token;

use crate::node_client::{NodeClient, NodeRequestError};

/// A new random tab name: 16 bytes as 32 hex digits.
pub fn new_tab_name() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the operating system's random source works");
    hex::encode(bytes)
}

/// Opens a tab on each of `nodes`, paid with `tokens_per_node` tokens taken
/// from `wallet`, and returns the tabs by node. Tokens a node refused stay
/// spent or invalid; the ones not yet used stay in the wallet.
pub async fn open_tabs(
    client: &NodeClient,
    nodes: &[&str],
    wallet: &mut Vec<Token>,
    tokens_per_node: usize,
) -> Result<BTreeMap<String, String>, OpenTabsError> {
    let needed = nodes.len() * tokens_per_node;
    if wallet.len() < needed {
        return Err(OpenTabsError::TooFewTokens {
            needed,
            held: wallet.len(),
        });
    }
    let mut tabs = BTreeMap::new();
    for node in nodes {
        let name = new_tab_name();
        let payment: Vec<Token> = wallet.drain(..tokens_per_node).collect();
        client.pay_tab(node, &name, &payment).await?;
        tabs.insert(node.to_string(), name);
    }
    Ok(tabs)
}

/// Moves each node's pieces in `pieces` into that node's tab in `tabs`, one
/// request per node, and returns how many pieces are renewed.
pub async fn move_into_tabs(
    client: &NodeClient,
    tabs: &BTreeMap<String, String>,
    pieces: &BTreeMap<String, Vec<PieceId>>,
) -> Result<usize, NodeRequestError> {
    let mut renewed = 0;
    for (node, ids) in pieces {
        let Some(tab) = tabs.get(node) else {
            continue;
        };
        let lease_ends = client.move_to_tab(node, tab, ids).await?;
        renewed += lease_ends.iter().filter(|end| end.is_some()).count();
    }
    Ok(renewed)
}

/// Tabs that could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenTabsError {
    #[error("{needed} tokens are needed, the wallet holds {held}")]
    TooFewTokens { needed: usize, held: usize },
    #[error("{0}")]
    Node(#[from] NodeRequestError),
}

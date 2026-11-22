//! Renewing pieces on nodes that require tokens: one tab per node, sized to
//! what the pieces there cost for a month.
//!
//! A node's price comes from its coordinator: the signed node list says which
//! class the node is in, and the signed price table what that class costs
//! ([`mesh_core::prices`]). For each node, renewal adds up what keeping its
//! pieces costs for a month ([`storage_cost`]), pays a new tab with that many
//! tokens from the wallet, and moves the pieces into it in one request. A
//! token another device already spent comes back refused and is replaced by
//! the next one.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use mesh_core::PieceId;
use mesh_core::node_list::{CoordinatorList, NodeSnapshot, TrustError};
use mesh_core::prices::{PRICES_ROUTE, PriceTable};
use mesh_core::protocol::{MAX_BATCH_PIECES, NODE_LIST_ROUTE};
use mesh_core::signed::{Signed, SignedBody};
use mesh_core::tabs::{MAX_TOKENS_PER_REQUEST, storage_cost};
use mesh_core::tokens::{MICROTOKENS_PER_TOKEN, MONTH_SECONDS, Token};
use reqwest::StatusCode;

use crate::erasure::piece_len;
use crate::manifest::Manifest;
use crate::node_client::{NodeClient, NodeRequestError};
use crate::tabs::new_tab_name;
use crate::wallet::Wallet;

/// What one node charges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodePrice {
    pub tokens_per_tb_month: u64,
    /// Whether the node is the coordinator's own and takes allowance tokens.
    pub takes_allowance: bool,
}

/// What every node the coordinator prices charges, by node URL.
#[derive(Debug, Default)]
pub struct NodePrices {
    by_url: HashMap<String, NodePrice>,
}

/// Prices that could not be had.
#[derive(Debug, thiserror::Error)]
pub enum PricesError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("the coordinator answered {0}")]
    Refused(StatusCode),
    #[error("the coordinator answered in a form this client does not understand")]
    UnexpectedAnswer,
    #[error("the prices are not to be trusted: {0}")]
    Untrusted(#[from] TrustError),
}

impl NodePrices {
    /// The prices of the nodes in the coordinator's newest node list, from
    /// its signed price table, both checked against `coordinators`.
    pub async fn fetch(
        client: &NodeClient,
        coordinator: &str,
        coordinators: &CoordinatorList,
    ) -> Result<Self, PricesError> {
        let snapshot: Signed<NodeSnapshot> =
            fetch_signed(client, coordinator, NODE_LIST_ROUTE).await?;
        let table: Signed<PriceTable> = fetch_signed(client, coordinator, PRICES_ROUTE).await?;
        coordinators.check_snapshot(&snapshot)?;
        coordinators.check_signed(&table)?;
        Ok(NodePrices::from_lists(&snapshot.body, &table.body))
    }

    /// The prices of the nodes in `snapshot` under `table`. Nodes whose record
    /// they did not sign themselves, or that are in no class, have none.
    pub fn from_lists(snapshot: &NodeSnapshot, table: &PriceTable) -> Self {
        let mut by_url = HashMap::new();
        for listed in &snapshot.nodes {
            if listed.record.verify_node().is_err() {
                continue;
            }
            let Some(tokens_per_tb_month) = table.price_of_node(listed) else {
                continue;
            };
            let price = NodePrice {
                tokens_per_tb_month,
                takes_allowance: listed.takes_allowance,
            };
            by_url.insert(listed.record.body.url.clone(), price);
        }
        NodePrices { by_url }
    }

    /// What the node at `url` charges, if the coordinator prices it.
    pub fn of(&self, url: &str) -> Option<NodePrice> {
        self.by_url.get(url).copied()
    }
}

async fn fetch_signed<T: SignedBody>(
    client: &NodeClient,
    coordinator: &str,
    route: &str,
) -> Result<Signed<T>, PricesError> {
    let url = format!("{}{route}", coordinator.trim_end_matches('/'));
    let response = client.http().get(url).send().await?;
    if !response.status().is_success() {
        return Err(PricesError::Refused(response.status()));
    }
    let body = response.bytes().await?;
    serde_json::from_slice(&body).map_err(|_| PricesError::UnexpectedAnswer)
}

/// The pieces of `manifests` on each of `nodes`, with each piece's length.
pub fn pieces_by_node(
    manifests: &[&Manifest],
    nodes: &BTreeSet<String>,
) -> BTreeMap<String, BTreeMap<PieceId, u64>> {
    let mut by_node: BTreeMap<String, BTreeMap<PieceId, u64>> = BTreeMap::new();
    for chunk in manifests.iter().flat_map(|manifest| &manifest.chunks) {
        let len = piece_len(chunk.sealed_len as usize) as u64;
        for piece in chunk
            .pieces
            .iter()
            .filter(|piece| nodes.contains(&piece.node))
        {
            by_node
                .entry(piece.node.clone())
                .or_default()
                .insert(piece.id, len);
        }
    }
    by_node
}

/// What renewing through tabs achieved.
#[derive(Debug, Default)]
pub struct TabRenewal {
    /// Pieces now kept until their new tab ends, by node URL.
    pub renewed: BTreeSet<(String, PieceId)>,
    /// Tokens handed to nodes, whether they were spent now or before; they
    /// leave the wallet for good.
    pub used: Vec<Token>,
}

/// Renews `pieces`, by node URL with their lengths, through a new tab on each
/// node, paid from `wallet` at the node's price in `prices`.
pub async fn renew_through_tabs(
    client: &NodeClient,
    prices: &NodePrices,
    wallet: &mut Wallet,
    pieces: &BTreeMap<String, BTreeMap<PieceId, u64>>,
) -> TabRenewal {
    let mut renewal = TabRenewal::default();
    // One node after another, so each takes what it needs from the wallet
    // before the next one looks.
    for (node, node_pieces) in pieces {
        let Some(price) = prices.of(node) else {
            eprintln!("the coordinator lists no price for {node}");
            continue;
        };
        let name = new_tab_name();
        let needed = month_cost(node_pieces.values().copied(), price.tokens_per_tb_month);
        let (balance, used) = fill_tab(client, node, &name, needed, price, wallet).await;
        renewal.used.extend(used);
        let affordable = affordable(node_pieces, price.tokens_per_tb_month, balance);
        for id in move_into_tab(client, node, &name, &affordable).await {
            renewal.renewed.insert((node.clone(), id));
        }
    }
    renewal
}

/// What keeping pieces of `lens` bytes for a month costs at
/// `tokens_per_tb_month`, in millionths of a token, with 5% to spare.
///
/// Prices move at most 5% a day, and the node may have looked its price up
/// before today's move, so the spare keeps the last pieces from not fitting.
fn month_cost(lens: impl Iterator<Item = u64>, tokens_per_tb_month: u64) -> u64 {
    let cost: u64 = lens
        .map(|len| storage_cost(len, tokens_per_tb_month, MONTH_SECONDS))
        .sum();
    cost + cost.div_ceil(20)
}

/// Pays the tab `name` on `node` from `wallet` until its balance covers
/// `needed`, or the wallet has no tokens the node takes. Returns the balance
/// and the tokens handed over.
async fn fill_tab(
    client: &NodeClient,
    node: &str,
    name: &str,
    needed: u64,
    price: NodePrice,
    wallet: &mut Wallet,
) -> (u64, Vec<Token>) {
    let mut balance = 0;
    let mut used = Vec::new();
    while balance < needed {
        let count = (needed - balance).div_ceil(MICROTOKENS_PER_TOKEN) as usize;
        let tokens = wallet.take(count.min(MAX_TOKENS_PER_REQUEST), price.takes_allowance);
        if tokens.is_empty() {
            eprintln!("the wallet has too few tokens for {node}");
            break;
        }
        match client.pay_tab(node, name, &tokens).await {
            Ok(tab) => {
                balance = tab.balance_microtokens;
                used.extend(tokens);
            }
            // None of them was valid and unspent: another device spent them,
            // or their month is over. They are of no more use.
            Err(NodeRequestError::Refused { status, .. })
                if status == StatusCode::PAYMENT_REQUIRED =>
            {
                used.extend(tokens);
            }
            // The node may not have seen them; if it spent them after all,
            // the next renewal finds them spent.
            Err(error) => {
                eprintln!("{error}");
                wallet.put_back(tokens);
                break;
            }
        }
    }
    (balance, used)
}

/// The pieces among `pieces` that `balance` pays for, in order, so that a
/// tab short of tokens still keeps as many as it can instead of the node
/// refusing the whole move.
fn affordable(
    pieces: &BTreeMap<PieceId, u64>,
    tokens_per_tb_month: u64,
    balance: u64,
) -> Vec<PieceId> {
    let mut left = balance;
    let mut ids = Vec::new();
    for (id, &len) in pieces {
        let cost = month_cost(std::iter::once(len), tokens_per_tb_month);
        if cost > left {
            break;
        }
        left -= cost;
        ids.push(*id);
    }
    ids
}

/// Moves `ids` on `node` into the tab `name` and returns those now in it.
async fn move_into_tab(
    client: &NodeClient,
    node: &str,
    name: &str,
    ids: &[PieceId],
) -> Vec<PieceId> {
    let mut moved = Vec::new();
    for batch in ids.chunks(MAX_BATCH_PIECES) {
        let lease_ends = match client.move_to_tab(node, name, batch).await {
            Ok(lease_ends) => lease_ends,
            Err(error) => {
                eprintln!("{error}");
                continue;
            }
        };
        let held = batch
            .iter()
            .zip(lease_ends)
            .filter(|(_, end)| end.is_some());
        moved.extend(held.map(|(id, _)| *id));
    }
    moved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_month_costs_the_price_and_a_twentieth_more() {
        let tb = 1_000_000_000_000;

        assert_eq!(
            month_cost([tb / 2, tb / 2].into_iter(), 2_000),
            2_100 * MICROTOKENS_PER_TOKEN
        );
    }

    #[test]
    fn a_short_balance_pays_for_the_first_pieces_only() {
        let pieces: BTreeMap<PieceId, u64> = (0u8..4)
            .map(|byte| (PieceId::of(&[byte]), 1_000_000_000))
            .collect();
        let one = month_cost(std::iter::once(1_000_000_000), 2_000);

        assert_eq!(affordable(&pieces, 2_000, one * 2 + 1).len(), 2);
        assert_eq!(affordable(&pieces, 2_000, one * 4).len(), 4);
        assert!(affordable(&pieces, 2_000, 0).is_empty());
    }
}

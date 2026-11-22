//! Choosing which nodes get the pieces of a chunk, and storing them there.
//!
//! A chunk survives the loss of [`PARITY_PIECES`] pieces, so no single thing
//! that can fail must ever hold more than that many. The rules below are hard:
//! a node that breaks one is never chosen, however few nodes are left.
//!
//! - One piece per node.
//! - At most one piece per IPv4 /24 or IPv6 /48, the usual size of one
//!   provider's block of addresses in one place.
//! - At most [`MAX_PER_OPERATOR`] pieces per operator and
//!   [`MAX_PER_NETWORK`] per network.
//!
//! Among the nodes the rules allow, the choice is random, weighted by each
//! node's weight. Choosing the best nodes first would let an attacker predict
//! where pieces go, or attract them by looking good.

use std::net::IpAddr;
use std::time::Duration;

use futures::future::join_all;
use mesh_core::{PieceId, merkle};
use reqwest::Url;

use crate::erasure::PARITY_PIECES;
use crate::manifest::PlacedPiece;
use crate::node_client::NodeClient;
use crate::node_list::{NodeInfo, NodeList};
use crate::random::random_unit;

/// The most pieces of one chunk a single operator may hold: as many as the
/// chunk can lose.
pub const MAX_PER_OPERATOR: usize = PARITY_PIECES;

/// The most pieces of one chunk that may be reached through one network.
pub const MAX_PER_NETWORK: usize = PARITY_PIECES;

/// Chooses up to `count` nodes from `list` for pieces of one chunk; fewer
/// when the rules allow no more.
///
/// `holding` are the URLs of the nodes that already hold pieces of the chunk;
/// the new nodes have to fit the rules together with them. Nodes whose URL is
/// in `excluded` are never chosen, such as nodes that just refused a piece.
pub fn choose<'list>(
    list: &'list NodeList,
    count: usize,
    holding: &[&str],
    excluded: &[&str],
) -> Vec<&'list NodeInfo> {
    let mut chosen: Vec<&NodeInfo> = holding
        .iter()
        .filter_map(|url| list.nodes.iter().find(|node| node.url == *url))
        .collect();
    let first_new = chosen.len();
    let mut candidates: Vec<&NodeInfo> = list
        .nodes
        .iter()
        .filter(|node| node.weight > 0.0)
        .filter(|node| !holding.contains(&node.url.as_str()))
        .filter(|node| !excluded.contains(&node.url.as_str()))
        .collect();
    while chosen.len() - first_new < count {
        candidates.retain(|candidate| fits(candidate, &chosen));
        let Some(index) = weighted_pick(&candidates) else {
            break;
        };
        chosen.push(candidates.swap_remove(index));
    }
    chosen.split_off(first_new)
}

/// Stores each of `pieces` of one chunk on a node chosen from `list` for
/// `lease`, and returns where each went: `None` for a piece no allowed node
/// took.
///
/// A node that refuses or can't be reached is not asked again, and its piece
/// goes to another node in the next round. Every round places or rules out at
/// least one node, so this ends once the list runs out.
pub async fn store_pieces(
    client: &NodeClient,
    list: &NodeList,
    pieces: &[Vec<u8>],
    holding: &[&str],
    excluded: &[&str],
    lease: Duration,
) -> Vec<Option<PlacedPiece>> {
    let mut placed: Vec<Option<PlacedPiece>> = vec![None; pieces.len()];
    let mut excluded: Vec<String> = excluded.iter().map(|url| url.to_string()).collect();
    loop {
        let waiting: Vec<usize> = (0..pieces.len()).filter(|&i| placed[i].is_none()).collect();
        let mut holding_now: Vec<&str> = holding.to_vec();
        holding_now.extend(placed.iter().flatten().map(|piece| piece.node.as_str()));
        let excluded_now: Vec<&str> = excluded.iter().map(String::as_str).collect();
        let nodes = choose(list, waiting.len(), &holding_now, &excluded_now);
        if nodes.is_empty() {
            return placed;
        }
        let attempts = waiting
            .iter()
            .zip(&nodes)
            .map(|(&index, node)| store_one(client, &pieces[index], &node.url, lease));
        let results = join_all(attempts).await;
        for ((&index, node), result) in waiting.iter().zip(&nodes).zip(results) {
            match result {
                Some(piece) => placed[index] = Some(piece),
                None => excluded.push(node.url.clone()),
            }
        }
    }
}

/// Stores `bytes` on `node`, or `None` when the node didn't take them.
async fn store_one(
    client: &NodeClient,
    bytes: &[u8],
    node: &str,
    lease: Duration,
) -> Option<PlacedPiece> {
    let id = PieceId::of(bytes);
    match client.put(node, &id, bytes.to_vec(), lease).await {
        Ok(_) => Some(PlacedPiece {
            id,
            merkle_root: merkle::root_of(bytes),
            node: node.to_string(),
        }),
        Err(error) => {
            eprintln!("{error}");
            None
        }
    }
}

/// Whether `candidate` may hold a piece of a chunk whose pieces are on
/// `chosen`.
fn fits(candidate: &NodeInfo, chosen: &[&NodeInfo]) -> bool {
    let same_operator = chosen
        .iter()
        .filter(|node| node.operator == candidate.operator)
        .count();
    let same_network = chosen
        .iter()
        .filter(|node| node.network == candidate.network)
        .count();
    let candidate_subnet = subnet_of(&candidate.url);
    let same_subnet = candidate_subnet.is_some()
        && chosen
            .iter()
            .any(|node| subnet_of(&node.url) == candidate_subnet);
    chosen.iter().all(|node| node.url != candidate.url)
        && same_operator < MAX_PER_OPERATOR
        && same_network < MAX_PER_NETWORK
        && !same_subnet
}

/// The /24 or /48 of a node's address, or `None` when the rule can't apply.
///
/// Host names are not resolved: which address a name points to is for the
/// coordinator to measure. Loopback addresses are left out so a whole test
/// network can run on one machine; a real node is never reached through one.
fn subnet_of(url: &str) -> Option<String> {
    let url = Url::parse(url).ok()?;
    // URLs write IPv6 addresses in brackets; a host name doesn't parse at all.
    let host = url
        .host_str()?
        .trim_start_matches('[')
        .trim_end_matches(']');
    match host.parse::<IpAddr>().ok()? {
        IpAddr::V4(address) if !address.is_loopback() => {
            let [a, b, c, _] = address.octets();
            Some(format!("{a}.{b}.{c}.0/24"))
        }
        IpAddr::V6(address) if !address.is_loopback() => {
            let [a, b, c, ..] = address.segments();
            Some(format!("{a:x}:{b:x}:{c:x}::/48"))
        }
        _ => None,
    }
}

/// The index of a random candidate, each as likely as its weight, or `None`
/// when there is none.
fn weighted_pick(candidates: &[&NodeInfo]) -> Option<usize> {
    let total: f64 = candidates.iter().map(|node| node.weight).sum();
    if candidates.is_empty() || total <= 0.0 {
        return None;
    }
    let mut remaining = random_unit() * total;
    for (index, node) in candidates.iter().enumerate() {
        remaining -= node.weight;
        if remaining < 0.0 {
            return Some(index);
        }
    }
    // Rounding can leave a sliver of `total` after the last node.
    Some(candidates.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(url: &str, operator: &str, network: &str) -> NodeInfo {
        NodeInfo {
            // Placement compares nodes by URL, so an id made from it does.
            id: mesh_core::NodeId::of(blake3::hash(url.as_bytes()).as_bytes()),
            url: url.to_string(),
            operator: operator.to_string(),
            network: network.to_string(),
            weight: 1.0,
        }
    }

    /// Eight nodes: four operators with two nodes each, all in different
    /// networks and subnets.
    fn diverse_list() -> NodeList {
        let nodes = (0..8)
            .map(|index| {
                node(
                    &format!("http://10.0.{index}.1:7001"),
                    &format!("operator-{}", index / 2),
                    &format!("network-{index}"),
                )
            })
            .collect();
        NodeList { nodes }
    }

    fn urls(nodes: &[&NodeInfo]) -> Vec<String> {
        nodes.iter().map(|node| node.url.clone()).collect()
    }

    #[test]
    fn chooses_distinct_nodes_within_the_operator_limit() {
        let list = diverse_list();
        for _ in 0..100 {
            let chosen = choose(&list, 6, &[], &[]);
            let mut chosen_urls = urls(&chosen);
            chosen_urls.sort();
            chosen_urls.dedup();
            assert_eq!(chosen_urls.len(), 6);
            for operator in 0..4 {
                let name = format!("operator-{operator}");
                let count = chosen.iter().filter(|node| node.operator == name).count();
                assert!(count <= MAX_PER_OPERATOR);
            }
        }
    }

    #[test]
    fn one_operator_alone_gets_only_as_many_pieces_as_a_chunk_can_lose() {
        let nodes = (0..6)
            .map(|index| {
                node(
                    &format!("http://10.0.{index}.1:7001"),
                    "alice",
                    &format!("network-{index}"),
                )
            })
            .collect();
        let list = NodeList { nodes };

        let chosen = choose(&list, 6, &[], &[]);

        assert_eq!(chosen.len(), MAX_PER_OPERATOR);
    }

    #[test]
    fn nodes_in_one_subnet_get_one_piece() {
        let nodes = (0..6)
            .map(|index| {
                node(
                    &format!("http://10.0.0.{index}:7001"),
                    &format!("operator-{index}"),
                    &format!("network-{index}"),
                )
            })
            .collect();
        let list = NodeList { nodes };

        let chosen = choose(&list, 2, &[], &[]);

        assert_eq!(chosen.len(), 1);
    }

    #[test]
    fn new_nodes_fit_the_rules_together_with_the_holding_ones() {
        let list = diverse_list();
        // operator-0 already holds two pieces of this chunk.
        let holding = ["http://10.0.0.1:7001", "http://10.0.1.1:7001"];

        for _ in 0..100 {
            let chosen = choose(&list, 2, &holding, &[]);
            assert_eq!(chosen.len(), 2);
            assert!(chosen.iter().all(|node| node.operator != "operator-0"));
        }
    }

    #[test]
    fn excluded_and_weightless_nodes_are_never_chosen() {
        let mut list = diverse_list();
        list.nodes[3].weight = 0.0;

        for _ in 0..100 {
            let chosen_urls = urls(&choose(&list, 4, &[], &["http://10.0.5.1:7001"]));
            assert!(!chosen_urls.contains(&"http://10.0.3.1:7001".to_string()));
            assert!(!chosen_urls.contains(&"http://10.0.5.1:7001".to_string()));
        }
    }

    #[test]
    fn heavier_nodes_are_chosen_more_often() {
        let mut list = diverse_list();
        list.nodes[0].weight = 10.0;
        let mut heavy = 0;
        let mut light = 0;

        for _ in 0..1_000 {
            match choose(&list, 1, &[], &[])[0].url.as_str() {
                "http://10.0.0.1:7001" => heavy += 1,
                "http://10.0.1.1:7001" => light += 1,
                _ => {}
            }
        }

        assert!(heavy > light * 3, "heavy {heavy}, light {light}");
    }
}

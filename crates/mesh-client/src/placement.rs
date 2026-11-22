//! Choosing which nodes get the pieces of a chunk, and storing them there.
//!
//! A chunk survives the loss of as many pieces as its layout has parity
//! pieces, so no single thing that can fail must ever hold more than that
//! many ([`Limits`]). The rules below are hard: a node that breaks one is
//! never chosen, however few nodes are left.
//!
//! - One piece per node.
//! - At most one piece per IPv4 /24 or IPv6 /48, the usual size of one
//!   provider's block of addresses in one place.
//! - At most `parity` pieces per operator, per network and per region. A
//!   node whose region is unknown is a region of its own.
//!
//! Among the nodes the rules allow, the choice is random, weighted by each
//! node's weight. Choosing the best nodes first would let an attacker predict
//! where pieces go, or attract them by looking good.

use std::net::IpAddr;
use std::time::Duration;

use futures::future::join_all;
use mesh_core::layout::Layout;
use mesh_core::{NodeId, PieceId, merkle};
use reqwest::Url;

use crate::manifest::PlacedPiece;
use crate::node_client::NodeClient;
use crate::node_list::{NodeInfo, NodeList};
use crate::random::random_unit;

/// How many pieces of one chunk may share an operator, a network or a region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub per_operator: usize,
    pub per_network: usize,
    pub per_region: usize,
}

impl Limits {
    /// The limits of the default layout, 4 + 2: two pieces per operator,
    /// network and region.
    pub const DEFAULT: Limits = Limits::for_layout(Layout::DEFAULT, 1);

    /// The limits for a chunk of `layout` spread over at least `regions`
    /// regions: as many pieces per operator, network and region as the chunk
    /// can lose, and per region few enough that the pieces fill at least
    /// `regions` regions.
    pub const fn for_layout(layout: Layout, regions: usize) -> Limits {
        let per_group = layout.max_per_group();
        // With at most `k` pieces per region, `total` pieces fill at least
        // `total / k` regions, rounded up; `k = (total - 1) / (regions - 1)`
        // is the largest `k` that keeps that at `regions` or more.
        let spread_cap = if regions <= 1 {
            per_group
        } else {
            (layout.total() - 1) / (regions - 1)
        };
        let per_region = if spread_cap < per_group {
            spread_cap
        } else {
            per_group
        };
        Limits {
            per_operator: per_group,
            per_network: per_group,
            per_region: if per_region == 0 { 1 } else { per_region },
        }
    }
}

/// How a chunk is cut and how far apart its pieces go: its layout, and the
/// fewest regions its pieces spread over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spread {
    pub layout: Layout,
    pub regions: usize,
}

impl Spread {
    /// The default layout, 4 + 2, with no more than its two-piece limit per
    /// region.
    pub const DEFAULT: Spread = Spread {
        layout: Layout::DEFAULT,
        regions: 1,
    };

    /// The placement limits of this spread.
    pub const fn limits(self) -> Limits {
        Limits::for_layout(self.layout, self.regions)
    }
}

/// Chooses up to `count` nodes from `list` for pieces of one chunk within
/// `limits`; fewer when the rules allow no more.
///
/// `holding` are the ids of the nodes that already hold pieces of the chunk;
/// the new nodes have to fit the rules together with those still in the list.
/// Nodes in `excluded` are never chosen, such as nodes that just refused a
/// piece.
pub fn choose<'list>(
    list: &'list NodeList,
    count: usize,
    holding: &[NodeId],
    excluded: &[NodeId],
    limits: Limits,
) -> Vec<&'list NodeInfo> {
    let mut chosen: Vec<&NodeInfo> = holding
        .iter()
        .filter_map(|id| list.nodes.iter().find(|node| node.id == *id))
        .collect();
    let first_new = chosen.len();
    let mut candidates: Vec<&NodeInfo> = list
        .nodes
        .iter()
        .filter(|node| node.weight > 0.0)
        .filter(|node| !holding.contains(&node.id))
        .filter(|node| !excluded.contains(&node.id))
        .collect();
    while chosen.len() - first_new < count {
        candidates.retain(|candidate| fits(candidate, &chosen, limits));
        let Some(index) = weighted_pick(&candidates) else {
            break;
        };
        chosen.push(candidates.swap_remove(index));
    }
    chosen.split_off(first_new)
}

/// Stores each of `pieces` of one chunk on a node chosen from `list` within
/// `limits` for `lease`, and returns where each went: `None` for a piece no
/// allowed node took.
///
/// A node that refuses or can't be reached is not asked again, and its piece
/// goes to another node in the next round. Every round places or rules out at
/// least one node, so this ends once the list runs out.
pub async fn store_pieces(
    client: &NodeClient,
    list: &NodeList,
    pieces: &[Vec<u8>],
    holding: &[NodeId],
    excluded: &[NodeId],
    limits: Limits,
    lease: Duration,
) -> Vec<Option<PlacedPiece>> {
    let mut placed: Vec<Option<PlacedPiece>> = vec![None; pieces.len()];
    let mut excluded: Vec<NodeId> = excluded.to_vec();
    loop {
        let waiting: Vec<usize> = (0..pieces.len()).filter(|&i| placed[i].is_none()).collect();
        let mut holding_now: Vec<NodeId> = holding.to_vec();
        holding_now.extend(placed.iter().flatten().map(|piece| piece.node));
        let nodes = choose(list, waiting.len(), &holding_now, &excluded, limits);
        if nodes.is_empty() {
            return placed;
        }
        let attempts = waiting
            .iter()
            .zip(&nodes)
            .map(|(&index, node)| store_one(client, &pieces[index], node, lease));
        let results = join_all(attempts).await;
        for ((&index, node), result) in waiting.iter().zip(&nodes).zip(results) {
            match result {
                Some(piece) => placed[index] = Some(piece),
                None => excluded.push(node.id),
            }
        }
    }
}

/// Stores `bytes` on `node`, or `None` when the node didn't take them.
async fn store_one(
    client: &NodeClient,
    bytes: &[u8],
    node: &NodeInfo,
    lease: Duration,
) -> Option<PlacedPiece> {
    let id = PieceId::of(bytes);
    match client.put(&node.url, &id, bytes.to_vec(), lease).await {
        Ok(_) => Some(PlacedPiece {
            id,
            merkle_root: merkle::root_of(bytes),
            node: node.id,
        }),
        Err(error) => {
            eprintln!("{error}");
            None
        }
    }
}

/// Whether `candidate` may hold a piece of a chunk whose pieces are on
/// `chosen`, within `limits`. Pointers spread their copies by the same rules.
pub(crate) fn fits(candidate: &NodeInfo, chosen: &[&NodeInfo], limits: Limits) -> bool {
    let same_operator = chosen
        .iter()
        .filter(|node| node.operator == candidate.operator)
        .count();
    let same_network = chosen
        .iter()
        .filter(|node| node.network == candidate.network)
        .count();
    // An unknown region is a region of its own, so it never counts against
    // another node's.
    let same_region = match &candidate.region {
        Some(region) => chosen
            .iter()
            .filter(|node| node.region.as_ref() == Some(region))
            .count(),
        None => 0,
    };
    let candidate_subnet = subnet_of(&candidate.url);
    let same_subnet = candidate_subnet.is_some()
        && chosen
            .iter()
            .any(|node| subnet_of(&node.url) == candidate_subnet);
    chosen.iter().all(|node| node.id != candidate.id)
        && same_operator < limits.per_operator
        && same_network < limits.per_network
        && same_region < limits.per_region
        && !same_subnet
}

/// Whether `list` has enough nodes to place all pieces of a chunk of
/// `layout` within `limits`, taking nodes in turn rather than at random so
/// the answer doesn't change from one call to the next.
pub fn can_place(list: &NodeList, layout: Layout, limits: Limits) -> bool {
    let mut chosen: Vec<&NodeInfo> = Vec::new();
    for node in list.nodes.iter().filter(|node| node.weight > 0.0) {
        if fits(node, &chosen, limits) {
            chosen.push(node);
        }
    }
    chosen.len() >= layout.total()
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
            // Each test node needs an id of its own; its URL makes one.
            id: NodeId::of(blake3::hash(url.as_bytes()).as_bytes()),
            url: url.to_string(),
            operator: operator.to_string(),
            network: network.to_string(),
            region: None,
            weight: 1.0,
        }
    }

    /// Nine nodes of distinct operators, networks and subnets, three in each
    /// of three regions.
    fn three_regions() -> NodeList {
        let nodes = (0..9)
            .map(|index| {
                let mut info = node(
                    &format!("http://10.0.{index}.1:7001"),
                    &format!("operator-{index}"),
                    &format!("network-{index}"),
                );
                info.region = Some(format!("region-{}", index / 3));
                info
            })
            .collect();
        NodeList { nodes }
    }

    #[test]
    fn a_region_holds_no_more_than_the_chunk_can_lose() {
        let list = three_regions();

        for _ in 0..100 {
            let chosen = choose(&list, 6, &[], &[], Limits::DEFAULT);
            assert_eq!(chosen.len(), 6);
            for region in 0..3 {
                let name = Some(format!("region-{region}"));
                let count = chosen.iter().filter(|node| node.region == name).count();
                assert_eq!(count, 2);
            }
        }
    }

    #[test]
    fn spreading_over_more_regions_lowers_the_share_of_each() {
        let layout = Layout::new(4, 6).unwrap();

        assert_eq!(Limits::for_layout(layout, 1).per_region, 6);
        assert_eq!(Limits::for_layout(layout, 3).per_region, 4);
        assert_eq!(Limits::for_layout(layout, 3).per_operator, 6);
    }

    #[test]
    fn a_layout_is_placeable_only_with_enough_independent_nodes() {
        let list = three_regions();

        assert!(can_place(&list, Layout::DEFAULT, Limits::DEFAULT));
        let wide = Layout::new(6, 2).unwrap();
        assert!(!can_place(&list, wide, Limits::for_layout(wide, 1)));
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
            let chosen = choose(&list, 6, &[], &[], Limits::DEFAULT);
            let mut chosen_urls = urls(&chosen);
            chosen_urls.sort();
            chosen_urls.dedup();
            assert_eq!(chosen_urls.len(), 6);
            for operator in 0..4 {
                let name = format!("operator-{operator}");
                let count = chosen.iter().filter(|node| node.operator == name).count();
                assert!(count <= Limits::DEFAULT.per_operator);
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

        let chosen = choose(&list, 6, &[], &[], Limits::DEFAULT);

        assert_eq!(chosen.len(), Limits::DEFAULT.per_operator);
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

        let chosen = choose(&list, 2, &[], &[], Limits::DEFAULT);

        assert_eq!(chosen.len(), 1);
    }

    #[test]
    fn new_nodes_fit_the_rules_together_with_the_holding_ones() {
        let list = diverse_list();
        // operator-0 already holds two pieces of this chunk.
        let holding = [list.nodes[0].id, list.nodes[1].id];

        for _ in 0..100 {
            let chosen = choose(&list, 2, &holding, &[], Limits::DEFAULT);
            assert_eq!(chosen.len(), 2);
            assert!(chosen.iter().all(|node| node.operator != "operator-0"));
        }
    }

    #[test]
    fn excluded_and_weightless_nodes_are_never_chosen() {
        let mut list = diverse_list();
        list.nodes[3].weight = 0.0;

        for _ in 0..100 {
            let excluded = [list.nodes[5].id];
            let chosen_urls = urls(&choose(&list, 4, &[], &excluded, Limits::DEFAULT));
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
            match choose(&list, 1, &[], &[], Limits::DEFAULT)[0].url.as_str() {
                "http://10.0.0.1:7001" => heavy += 1,
                "http://10.0.1.1:7001" => light += 1,
                _ => {}
            }
        }

        assert!(heavy > light * 3, "heavy {heavy}, light {light}");
    }
}

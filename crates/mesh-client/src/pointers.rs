//! Writing and reading signed pointers on the nodes their owner's key picks.
//!
//! A pointer has no manifest saying where it is, so every device has to find
//! the same nodes from the owner's key alone. Each node in the list scores
//!
//! ```text
//! BLAKE3("neoworks-mesh pointer placement v1" ‖ owner ‖ node id)
//! ```
//!
//! and the [`POINTER_COPIES`] highest scores hold the pointer, spread by the
//! same operator, network and subnet rules as the pieces of a chunk. Two
//! devices with the same node list pick the same nodes. When nodes join or
//! leave, only a few places in the ranking change, and reading copies the
//! newest record to the nodes that should have it but do not.
//!
//! Nodes check signatures, but any of them can hand out an older record or
//! none at all. Reading therefore asks every copy, takes the highest version,
//! and refuses anything older than the last version the device saw.

use std::cmp::Reverse;
use std::time::Duration;

use futures::future::join_all;
use mesh_core::NodeId;
use mesh_core::pointer::Pointer;

use crate::node_client::{NodeClient, PointerStored};
use crate::node_list::{NodeInfo, NodeList};
use crate::placement::fits;

/// How many nodes hold each pointer. A pointer is a few KB, so copies are
/// cheap, and any one of them is enough to find the newest version.
pub const POINTER_COPIES: usize = 10;

const PLACEMENT_CONTEXT: &[u8] = b"neoworks-mesh pointer placement v1";

/// The nodes that hold `owner`'s pointer, highest score first.
pub fn pointer_nodes<'list>(owner: &[u8; 32], list: &'list NodeList) -> Vec<&'list NodeInfo> {
    let mut ranked: Vec<&NodeInfo> = list.nodes.iter().collect();
    ranked.sort_by_key(|node| Reverse(score(owner, &node.id)));
    let mut chosen: Vec<&NodeInfo> = Vec::new();
    for node in ranked {
        if chosen.len() == POINTER_COPIES {
            break;
        }
        if fits(node, &chosen) {
            chosen.push(node);
        }
    }
    chosen
}

/// How strongly `node_id` is drawn to hold `owner`'s pointer.
fn score(owner: &[u8; 32], node_id: &NodeId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PLACEMENT_CONTEXT);
    hasher.update(owner);
    hasher.update(node_id.as_bytes());
    *hasher.finalize().as_bytes()
}

/// A pointer that could not be written.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum WritePointerError {
    #[error("a node holds version {held_version}, so version {offered} is not the newest")]
    NotNewest { held_version: u64, offered: u64 },
    #[error("no node took the pointer")]
    NowhereStored,
}

/// Stores `pointer` on the nodes its owner's key picks from `list`, for
/// `lease`, and returns on how many it is.
///
/// A node holding the same or a higher version means another device wrote
/// one in the meantime; the caller has to read it and write a higher version.
/// The copies stored on the other nodes stay, which is harmless: readers take
/// the highest version anyway.
pub async fn write_pointer(
    client: &NodeClient,
    list: &NodeList,
    pointer: &Pointer,
    lease: Duration,
) -> Result<usize, WritePointerError> {
    let nodes: Vec<&str> = pointer_nodes(&pointer.owner, list)
        .into_iter()
        .map(|node| node.url.as_str())
        .collect();
    let mut stored = 0;
    let mut held_version = None;
    for answer in store_on(client, &nodes, pointer, lease).await {
        match answer {
            Some(PointerStored::Stored { .. }) => stored += 1,
            Some(PointerStored::Refused { held_version: held }) => {
                held_version = held_version.max(Some(held));
            }
            None => {}
        }
    }
    if let Some(held_version) = held_version {
        return Err(WritePointerError::NotNewest {
            held_version,
            offered: pointer.version,
        });
    }
    if stored == 0 {
        return Err(WritePointerError::NowhereStored);
    }
    Ok(stored)
}

/// Stores `pointer` on `nodes` again, such as the ones a read found lagging,
/// and returns on how many it now is.
pub async fn copy_pointer(
    client: &NodeClient,
    nodes: &[String],
    pointer: &Pointer,
    lease: Duration,
) -> usize {
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    store_on(client, &nodes, pointer, lease)
        .await
        .into_iter()
        .filter(|answer| matches!(answer, Some(PointerStored::Stored { .. })))
        .count()
}

/// Stores `pointer` on every node in `nodes` at once; a node that failed is
/// `None`.
async fn store_on(
    client: &NodeClient,
    nodes: &[&str],
    pointer: &Pointer,
    lease: Duration,
) -> Vec<Option<PointerStored>> {
    let stores = nodes.iter().map(|node| async move {
        match client.put_pointer(node, pointer, lease).await {
            Ok(stored) => Some(stored),
            Err(error) => {
                eprintln!("{error}");
                None
            }
        }
    });
    join_all(stores).await
}

/// The newest pointer found, and where it is missing.
#[derive(Debug)]
pub struct PointerRead {
    pub pointer: Pointer,
    /// Nodes that should hold the pointer and answered with none or an older
    /// version. [`copy_pointer`] brings them up to date.
    pub lagging: Vec<String>,
}

/// A pointer that could not be read.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReadPointerError {
    #[error("no node holds a pointer for this key")]
    NotFound,
    #[error(
        "the newest pointer found is version {found}, older than version {last_seen} seen before"
    )]
    Rollback { found: u64, last_seen: u64 },
}

/// The newest genuine pointer of `owner` on the nodes its key picks from
/// `list`.
///
/// `last_seen` is the highest version this device has read or written
/// before. Anything older is refused: the nodes asked are withholding the
/// newest record, by accident or on purpose.
pub async fn read_pointer(
    client: &NodeClient,
    list: &NodeList,
    owner: &[u8; 32],
    last_seen: Option<u64>,
) -> Result<PointerRead, ReadPointerError> {
    let nodes = pointer_nodes(owner, list);
    let reads = nodes.iter().map(|node| async move {
        // A node that cannot be reached or returns a forged record cannot be
        // brought up to date either, so it counts as neither holding nor
        // lagging.
        let answer = client.get_pointer(&node.url, owner).await.ok()?;
        Some((node.url.clone(), answer))
    });
    let answers: Vec<(String, Option<Pointer>)> =
        join_all(reads).await.into_iter().flatten().collect();
    let newest = answers
        .iter()
        .filter_map(|(_, pointer)| pointer.as_ref())
        .max_by_key(|pointer| pointer.version)
        .cloned()
        .ok_or(ReadPointerError::NotFound)?;
    if let Some(last_seen) = last_seen.filter(|&last_seen| newest.version < last_seen) {
        return Err(ReadPointerError::Rollback {
            found: newest.version,
            last_seen,
        });
    }
    let lagging = answers
        .into_iter()
        .filter(|(_, pointer)| {
            pointer
                .as_ref()
                .is_none_or(|held| held.version < newest.version)
        })
        .map(|(url, _)| url)
        .collect();
    Ok(PointerRead {
        pointer: newest,
        lagging,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_of(count: u8) -> NodeList {
        let nodes = (0..count)
            .map(|index| NodeInfo {
                id: NodeId::of(&[index; 32]),
                url: format!("http://127.0.0.1:{}", 7000 + u16::from(index)),
                operator: format!("operator-{index}"),
                network: format!("network-{index}"),
                weight: 1.0,
            })
            .collect();
        NodeList { nodes }
    }

    fn urls(nodes: &[&NodeInfo]) -> Vec<String> {
        nodes.iter().map(|node| node.url.clone()).collect()
    }

    #[test]
    fn the_same_key_and_list_pick_the_same_nodes() {
        let list = list_of(30);

        let first = urls(&pointer_nodes(&[1; 32], &list));
        let again = urls(&pointer_nodes(&[1; 32], &list));

        assert_eq!(first.len(), POINTER_COPIES);
        assert_eq!(first, again);
    }

    #[test]
    fn different_keys_pick_different_nodes() {
        let list = list_of(30);

        let one = urls(&pointer_nodes(&[1; 32], &list));
        let other = urls(&pointer_nodes(&[2; 32], &list));

        assert_ne!(one, other);
    }

    #[test]
    fn a_node_joining_moves_at_most_one_copy() {
        let list = list_of(30);
        let mut grown = list.clone();
        grown.nodes.push(list_of(31).nodes.pop().unwrap());

        let before = urls(&pointer_nodes(&[1; 32], &list));
        let after = urls(&pointer_nodes(&[1; 32], &grown));

        let kept = after.iter().filter(|url| before.contains(url)).count();
        assert!(kept >= POINTER_COPIES - 1);
    }

    #[test]
    fn copies_keep_to_the_operator_limit() {
        let mut list = list_of(30);
        for node in &mut list.nodes {
            node.operator = "everyone".to_string();
        }

        let chosen = pointer_nodes(&[1; 32], &list);

        assert_eq!(chosen.len(), crate::placement::MAX_PER_OPERATOR);
    }
}

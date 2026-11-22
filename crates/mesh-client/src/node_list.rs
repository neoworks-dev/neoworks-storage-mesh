//! The nodes a client may store pieces on, and who runs them.
//!
//! The list comes from a coordinator as a signed snapshot
//! ([`NodeList::fetch`]), checked against the coordinator keys the client
//! starts with. For a network without a coordinator it can also be a JSON
//! file the user writes:
//!
//! ```json
//! [
//!   { "id": "3f1c…", "url": "http://203.0.113.7:7001", "operator": "alice", "network": "AS3320" },
//!   { "id": "9a0e…", "url": "http://198.51.100.2:7001", "operator": "bob", "network": "AS24940", "weight": 2.0 }
//! ]
//! ```
//!
//! A node prints its id when it starts. `operator` and `network` say which
//! nodes can fail together: one person's machines, or one internet provider's
//! network (its AS number). Placement uses them to keep any one of them from
//! holding too much of a chunk.

use std::path::Path;

use mesh_core::NodeId;
use mesh_core::node_list::{CoordinatorList, NodeSnapshot, TrustError};
use mesh_core::protocol::NODE_LIST_ROUTE;
use mesh_core::signed::Signed;
use serde::Deserialize;

use crate::node_client::NodeClient;

/// One node and what placement needs to know about it.
#[derive(Clone, Debug, Deserialize)]
pub struct NodeInfo {
    /// The hash of the node's own key, which stays the same when it moves.
    pub id: NodeId,
    /// The node's base URL, e.g. `http://203.0.113.7:7001`.
    pub url: String,
    /// Who runs the node. Nodes of one operator count as one failure: they
    /// can all go away at once.
    pub operator: String,
    /// The network the node is reached through, such as an AS number.
    pub network: String,
    /// How much more often than others the node is chosen. From a
    /// coordinator it is the node's measured uptime; in a file it is set by
    /// hand.
    #[serde(default = "default_weight")]
    pub weight: f64,
}

fn default_weight() -> f64 {
    1.0
}

/// The nodes to choose from.
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub struct NodeList {
    pub nodes: Vec<NodeInfo>,
}

/// A node list that could not be had.
#[derive(Debug, thiserror::Error)]
pub enum NodeListError {
    #[error("reading the node list: {0}")]
    Read(#[from] std::io::Error),
    #[error("parsing the node list: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("fetching the node list: {0}")]
    Fetch(#[from] reqwest::Error),
    #[error("the coordinator answered {0}")]
    Refused(reqwest::StatusCode),
    #[error("the node list is not to be trusted: {0}")]
    Untrusted(#[from] TrustError),
}

impl NodeList {
    /// Reads a node list from a JSON file.
    pub fn read(path: &Path) -> Result<Self, NodeListError> {
        let json = std::fs::read(path)?;
        Ok(serde_json::from_slice(&json)?)
    }

    /// Fetches the newest snapshot from the coordinator at `coordinator` and
    /// returns it once it is checked against `coordinators`.
    pub async fn fetch(
        client: &NodeClient,
        coordinator: &str,
        coordinators: &CoordinatorList,
    ) -> Result<(NodeList, u64), NodeListError> {
        let url = format!("{}{NODE_LIST_ROUTE}", coordinator.trim_end_matches('/'));
        let response = client.http().get(url).send().await?;
        if !response.status().is_success() {
            return Err(NodeListError::Refused(response.status()));
        }
        let snapshot: Signed<NodeSnapshot> = serde_json::from_slice(&response.bytes().await?)?;
        let list = NodeList::from_snapshot(&snapshot, coordinators)?;
        Ok((list, snapshot.body.published_at))
    }

    /// The nodes in `snapshot`, once a coordinator on `coordinators` is found
    /// to have signed it.
    ///
    /// Records a node did not sign itself are left out. A coordinator could
    /// otherwise list nodes under keys it made up, or move a real node to an
    /// address of its choosing.
    pub fn from_snapshot(
        snapshot: &Signed<NodeSnapshot>,
        coordinators: &CoordinatorList,
    ) -> Result<NodeList, TrustError> {
        coordinators.check_snapshot(snapshot)?;
        let nodes = snapshot
            .body
            .nodes
            .iter()
            .filter(|listed| listed.record.verify_node().is_ok())
            .map(|listed| NodeInfo {
                id: listed.record.body.node_id,
                url: listed.record.body.url.clone(),
                operator: listed.record.body.operator.clone(),
                network: listed.record.body.network.clone(),
                weight: f64::from(listed.uptime_permille) / 1000.0,
            })
            .collect();
        Ok(NodeList { nodes })
    }
}

#[cfg(test)]
mod tests {
    use mesh_core::node_list::{CoordinatorKey, ListedNode, NodeRecord};
    use mesh_core::signed::SigningKey;

    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn listed(signer: &SigningKey, named: &SigningKey, uptime_permille: u16) -> ListedNode {
        let body = NodeRecord {
            node_id: NodeId::of(&named.verifying_key().to_bytes()),
            url: "http://203.0.113.7:7001".into(),
            operator: "alice".into(),
            network: "AS3320".into(),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            signed_at: 1,
        };
        ListedNode {
            record: Signed::sign(signer, body),
            uptime_permille,
            bandwidth_bits_per_second: None,
        }
    }

    #[test]
    fn a_snapshot_becomes_a_list_weighted_by_uptime() {
        let coordinator = key(10);
        let coordinators = CoordinatorList {
            version: 1,
            keys: vec![CoordinatorKey(coordinator.verifying_key().to_bytes())],
        };
        let snapshot = NodeSnapshot {
            published_at: 1,
            nodes: vec![
                listed(&key(1), &key(1), 900),
                // Signed by node 2 but naming node 3: left out.
                listed(&key(2), &key(3), 1000),
            ],
            daily_loss_per_million: None,
        };

        let list =
            NodeList::from_snapshot(&Signed::sign(&coordinator, snapshot), &coordinators).unwrap();

        assert_eq!(list.nodes.len(), 1);
        assert_eq!(
            list.nodes[0].id,
            NodeId::of(&key(1).verifying_key().to_bytes())
        );
        assert_eq!(list.nodes[0].weight, 0.9);
    }
}

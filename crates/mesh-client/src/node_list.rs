//! The nodes a client may store pieces on, and who runs them.
//!
//! The list comes from a coordinator as a signed snapshot
//! ([`NodeList::fetch`]), checked against the coordinator keys the client
//! starts with. The client keeps the newest snapshot it has accepted
//! ([`NodeList::fetch_keeping_newest`]), so that nobody can hand it an older
//! one, and uses it while the coordinator is down. For a network without a
//! coordinator the list can also be a JSON file the user writes:
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

use crate::index::unix_now;
use crate::node_client::NodeClient;

/// How far ahead of this device's clock a snapshot may be dated: generous,
/// since a phone's clock can be minutes off.
const MAX_CLOCK_AHEAD_SECONDS: u64 = 60 * 60;

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
    #[error(
        "the coordinator's node list was published at {offered}, before the one kept here \
         ({kept}); someone may be replaying an old list"
    )]
    Older { offered: u64, kept: u64 },
    #[error(
        "the coordinator's node list was published at {offered}, ahead of this device's clock \
         ({now}); check the clock"
    )]
    FromTheFuture { offered: u64, now: u64 },
    #[error("keeping the node list: {0}")]
    Keep(std::io::Error),
}

impl NodeListError {
    /// Whether the coordinator could not be asked or would not answer, as
    /// opposed to answering with a list that is not to be used.
    fn is_unreachable(&self) -> bool {
        matches!(self, NodeListError::Fetch(_) | NodeListError::Refused(_))
    }
}

/// A node list from a coordinator, or the kept one when the coordinator could
/// not be reached.
#[derive(Debug)]
pub struct FetchedNodeList {
    pub list: NodeList,
    /// When the coordinator published the list, in Unix seconds.
    pub published_at: u64,
    /// Why the coordinator was not asked successfully, when the kept list is
    /// used instead.
    pub unreachable: Option<NodeListError>,
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
        let snapshot = fetch_snapshot(client, coordinator).await?;
        let list = NodeList::from_snapshot(&snapshot, coordinators)?;
        Ok((list, snapshot.body.published_at))
    }

    /// Like [`NodeList::fetch`], but keeps the newest snapshot accepted so far
    /// in the file `kept_path`. A snapshot published before the kept one is
    /// refused, and when the coordinator cannot be reached, the kept one is
    /// used.
    ///
    /// Without this, whoever sits between client and coordinator could serve
    /// an old snapshot that still carries a valid signature, such as one from
    /// before a malicious node was dropped.
    pub async fn fetch_keeping_newest(
        client: &NodeClient,
        coordinator: &str,
        coordinators: &CoordinatorList,
        kept_path: &Path,
    ) -> Result<FetchedNodeList, NodeListError> {
        let kept = read_kept(kept_path, coordinators)?;
        let snapshot = match fetch_snapshot(client, coordinator).await {
            Ok(snapshot) => snapshot,
            Err(error) if error.is_unreachable() => return fall_back(kept, coordinators, error),
            Err(error) => return Err(error),
        };
        let list = NodeList::from_snapshot(&snapshot, coordinators)?;
        let offered = snapshot.body.published_at;
        let kept_at = kept.map(|kept| kept.body.published_at);
        check_date(offered, kept_at, unix_now())?;
        if kept_at != Some(offered) {
            write_kept(kept_path, &snapshot).map_err(NodeListError::Keep)?;
        }
        Ok(FetchedNodeList {
            list,
            published_at: offered,
            unreachable: None,
        })
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

/// The newest snapshot the coordinator at `coordinator` has published, not
/// yet checked.
async fn fetch_snapshot(
    client: &NodeClient,
    coordinator: &str,
) -> Result<Signed<NodeSnapshot>, NodeListError> {
    let url = format!("{}{NODE_LIST_ROUTE}", coordinator.trim_end_matches('/'));
    let response = client.http().get(url).send().await?;
    if !response.status().is_success() {
        return Err(NodeListError::Refused(response.status()));
    }
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

/// Checks that a snapshot published at `offered` may replace the one kept
/// from `kept_at`, at `now`.
fn check_date(offered: u64, kept_at: Option<u64>, now: u64) -> Result<(), NodeListError> {
    if let Some(kept) = kept_at.filter(|&kept| offered < kept) {
        return Err(NodeListError::Older { offered, kept });
    }
    // A snapshot dated far ahead would be kept and make every real one look
    // old, so one stolen coordinator key could freeze the client's list.
    if offered > now + MAX_CLOCK_AHEAD_SECONDS {
        return Err(NodeListError::FromTheFuture { offered, now });
    }
    Ok(())
}

/// The list in the `kept` snapshot, used because the coordinator could not be
/// reached for `error`; the error itself when nothing is kept.
fn fall_back(
    kept: Option<Signed<NodeSnapshot>>,
    coordinators: &CoordinatorList,
    error: NodeListError,
) -> Result<FetchedNodeList, NodeListError> {
    let Some(kept) = kept else {
        return Err(error);
    };
    Ok(FetchedNodeList {
        list: NodeList::from_snapshot(&kept, coordinators)?,
        published_at: kept.body.published_at,
        unreachable: Some(error),
    })
}

/// The snapshot kept in `path` while a coordinator on `coordinators` vouches
/// for it, or `None`.
fn read_kept(
    path: &Path,
    coordinators: &CoordinatorList,
) -> Result<Option<Signed<NodeSnapshot>>, NodeListError> {
    let json = match std::fs::read(path) {
        Ok(json) => json,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(NodeListError::Keep(error)),
    };
    let kept: Signed<NodeSnapshot> = serde_json::from_slice(&json)?;
    // A snapshot from a coordinator since dropped from the list is forgotten.
    // Otherwise one signed with a key dropped for being stolen could keep
    // refusing every real snapshot.
    Ok(coordinators.check_snapshot(&kept).is_ok().then_some(kept))
}

/// Keeps `snapshot` in `path`, through a temporary file so that a client
/// stopped mid-write still has the snapshot it kept before.
fn write_kept(path: &Path, snapshot: &Signed<NodeSnapshot>) -> std::io::Result<()> {
    let json = serde_json::to_vec(snapshot)?;
    let temporary_path = path.with_extension("partial");
    std::fs::write(&temporary_path, json)?;
    std::fs::rename(&temporary_path, path)
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

    #[test]
    fn a_snapshot_as_new_as_the_kept_one_or_newer_replaces_it() {
        assert!(check_date(500, None, 1_000).is_ok());
        assert!(check_date(500, Some(500), 1_000).is_ok());
        assert!(check_date(600, Some(500), 1_000).is_ok());
    }

    #[test]
    fn a_snapshot_older_than_the_kept_one_is_refused() {
        assert!(matches!(
            check_date(400, Some(500), 1_000),
            Err(NodeListError::Older {
                offered: 400,
                kept: 500
            })
        ));
    }

    #[test]
    fn a_snapshot_dated_far_ahead_of_the_clock_is_refused() {
        let now = 1_000;

        assert!(check_date(now + MAX_CLOCK_AHEAD_SECONDS, None, now).is_ok());
        assert!(matches!(
            check_date(now + MAX_CLOCK_AHEAD_SECONDS + 1, None, now),
            Err(NodeListError::FromTheFuture { .. })
        ));
    }
}

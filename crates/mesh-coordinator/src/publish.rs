//! Signing and publishing snapshots of the node list.
//!
//! A snapshot holds every node that answered at least one check in the
//! uptime window, with its record and its uptime. Nodes that stopped
//! answering drop out of the next snapshot; their records stay in the
//! registry, so they come back once they answer again.

use std::sync::Arc;

use mesh_core::node_list::{ListedNode, NodeSnapshot};
use mesh_core::signed::{Signed, SigningKey};
use tokio::sync::RwLock;

use crate::clock::unix_now;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;

/// The newest published snapshot, or `None` before the first.
pub type LatestSnapshot = RwLock<Option<Signed<NodeSnapshot>>>;

/// A snapshot of the nodes that answered a check after `since`, made at
/// `now` and signed with `key`.
pub async fn snapshot(
    registry: &Registry,
    key: &SigningKey,
    since: u64,
    now: u64,
) -> rusqlite::Result<Signed<NodeSnapshot>> {
    let mut nodes = Vec::new();
    for record in registry.records().await? {
        let uptime = registry
            .uptime_permille(&record.body.node_id, since)
            .await?;
        // `None` is a node not checked in the window, 0 one that never
        // answered: neither is any use to a client.
        let Some(uptime_permille) = uptime.filter(|&permille| permille > 0) else {
            continue;
        };
        nodes.push(ListedNode {
            record,
            uptime_permille,
        });
    }
    let body = NodeSnapshot {
        published_at: now,
        nodes,
    };
    Ok(Signed::sign(key, body))
}

/// Publishes a new snapshot into `latest` each publish interval, forever.
pub async fn publish_forever(
    registry: Arc<Registry>,
    key: SigningKey,
    latest: Arc<LatestSnapshot>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.publish_interval);
    loop {
        timer.tick().await;
        let now = unix_now();
        let since = now.saturating_sub(settings.uptime_window.as_secs());
        // A failed round keeps the last snapshot served until the next tick.
        match snapshot(&registry, &key, since, now).await {
            Ok(snapshot) => *latest.write().await = Some(snapshot),
            Err(error) => eprintln!("publish: {error}"),
        }
    }
}

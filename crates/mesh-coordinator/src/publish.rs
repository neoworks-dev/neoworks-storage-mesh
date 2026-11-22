//! Signing and publishing snapshots of the node list.
//!
//! A snapshot holds every node that answered at least one check in the
//! uptime window, with its record, its uptime, its bandwidth and what it
//! says it stores, and the loss rate over all nodes. Nodes that stopped answering drop out of the
//! next snapshot; their records stay in the registry, so they come back once
//! they answer again.

use std::sync::Arc;

use mesh_core::node_list::{ListedNode, NodeSnapshot};
use mesh_core::signed::{Signed, SigningKey};
use tokio::sync::RwLock;

use crate::clock::unix_now;
use crate::measurements::Measurements;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;

/// The newest published snapshot, or `None` before the first.
pub type LatestSnapshot = RwLock<Option<Signed<NodeSnapshot>>>;

/// A snapshot made at `now` and signed with `key`.
pub async fn snapshot(
    registry: &Registry,
    measurements: &Measurements,
    key: &SigningKey,
    settings: &CoordinatorSettings,
    now: u64,
) -> rusqlite::Result<Signed<NodeSnapshot>> {
    let uptime_since = now.saturating_sub(settings.uptime_window.as_secs());
    let bandwidth_since = now.saturating_sub(settings.bandwidth_window.as_secs());
    let mut nodes = Vec::new();
    for record in registry.records().await? {
        let node_id = record.body.node_id;
        let uptime = registry.uptime_permille(&node_id, uptime_since).await?;
        // `None` is a node not checked in the window, 0 one that never
        // answered: neither is any use to a client.
        let Some(uptime_permille) = uptime.filter(|&permille| permille > 0) else {
            continue;
        };
        nodes.push(ListedNode {
            record,
            uptime_permille,
            bandwidth_bits_per_second: measurements.bandwidth(&node_id, bandwidth_since).await?,
            used_bytes: measurements.usage(&node_id, uptime_since).await?,
        });
    }
    let loss_since = now.saturating_sub(settings.loss_window.as_secs());
    let body = NodeSnapshot {
        published_at: now,
        nodes,
        daily_loss_per_million: measurements.daily_loss_per_million(loss_since, now).await?,
    };
    Ok(Signed::sign(key, body))
}

/// Publishes a new snapshot into `latest` each publish interval, forever.
pub async fn publish_forever(
    registry: Arc<Registry>,
    measurements: Arc<Measurements>,
    key: SigningKey,
    latest: Arc<LatestSnapshot>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.publish_interval);
    loop {
        timer.tick().await;
        // A failed round keeps the last snapshot served until the next tick.
        match snapshot(&registry, &measurements, &key, &settings, unix_now()).await {
            Ok(snapshot) => *latest.write().await = Some(snapshot),
            Err(error) => eprintln!("publish: {error}"),
        }
    }
}

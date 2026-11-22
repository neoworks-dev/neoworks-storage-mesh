//! Checking that nodes are up, which is where their uptime comes from.
//!
//! The coordinator asks each node for its identity. A node counts as up when
//! it answers with the id it registered: a different server now listening at
//! the address does not count, so a node cannot inherit another's uptime.

use std::sync::Arc;

use futures::future::join_all;
use mesh_core::NodeId;
use mesh_core::protocol::{IDENTITY_ROUTE, NodeIdentity};

use crate::clock::unix_now;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;

/// Whether the node at `url` answers as `node_id`.
pub async fn answers_as(http: &reqwest::Client, url: &str, node_id: &NodeId) -> bool {
    let identity_url = format!("{}{IDENTITY_ROUTE}", url.trim_end_matches('/'));
    let Ok(response) = http.get(identity_url).send().await else {
        return false;
    };
    let Ok(body) = response.bytes().await else {
        return false;
    };
    let Ok(identity) = serde_json::from_slice::<NodeIdentity>(&body) else {
        return false;
    };
    identity.is_consistent() && identity.node_id == *node_id
}

/// Checks every registered node once, all at the same time, and notes the
/// results.
pub async fn probe_all(http: &reqwest::Client, registry: &Registry) -> rusqlite::Result<()> {
    let records = registry.records().await?;
    let checks = records.iter().map(|record| async move {
        let answered = answers_as(http, &record.body.url, &record.body.node_id).await;
        (record.body.node_id, answered)
    });
    let probed_at = unix_now();
    for (node_id, answered) in join_all(checks).await {
        registry.record_probe(&node_id, probed_at, answered).await?;
    }
    Ok(())
}

/// Checks every node each probe interval, forever, and forgets checks older
/// than the uptime window.
pub async fn probe_forever(
    http: reqwest::Client,
    registry: Arc<Registry>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.probe_interval);
    loop {
        timer.tick().await;
        // A failed round is retried at the next tick; the nodes it missed
        // only lose one check.
        if let Err(error) = probe_all(&http, &registry).await {
            eprintln!("probe: {error}");
        }
        let window_start = unix_now().saturating_sub(settings.uptime_window.as_secs());
        if let Err(error) = registry.forget_probes(window_start).await {
            eprintln!("probe: forgetting old checks: {error}");
        }
    }
}

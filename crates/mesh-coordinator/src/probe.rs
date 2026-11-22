//! Checking that nodes are up, which is where their uptime and the loss rate
//! come from.
//!
//! The coordinator asks each node for its identity. A node counts as up when
//! it answers with the id it registered: a different server now listening at
//! the address does not count, so a node cannot inherit another's uptime.

use std::sync::Arc;

use futures::future::join_all;
use mesh_core::NodeId;
use mesh_core::protocol::{IDENTITY_ROUTE, NodeIdentity};

use crate::clock::unix_now;
use crate::measurements::Measurements;
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
/// results: every check for uptime, and every answer for the node's life.
pub async fn probe_all(
    http: &reqwest::Client,
    registry: &Registry,
    measurements: &Measurements,
) -> rusqlite::Result<()> {
    let records = registry.records().await?;
    let checks = records.iter().map(|record| async move {
        let answered = answers_as(http, &record.body.url, &record.body.node_id).await;
        (record.body.node_id, answered)
    });
    let probed_at = unix_now();
    for (node_id, answered) in join_all(checks).await {
        registry.record_probe(&node_id, probed_at, answered).await?;
        if answered {
            measurements.record_answer(&node_id, probed_at).await?;
        }
    }
    Ok(())
}

/// Checks every node each probe interval, forever. After each round it marks
/// nodes silent for too long as lost, and forgets checks and losses older
/// than their windows.
pub async fn probe_forever(
    http: reqwest::Client,
    registry: Arc<Registry>,
    measurements: Arc<Measurements>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.probe_interval);
    loop {
        timer.tick().await;
        // A failed round is retried at the next tick; the nodes it missed
        // only lose one check.
        if let Err(error) = probe_all(&http, &registry, &measurements).await {
            eprintln!("probe: {error}");
        }
        if let Err(error) = keep_up(&registry, &measurements, &settings).await {
            eprintln!("probe: {error}");
        }
    }
}

/// Marks lost nodes and forgets what fell out of the windows.
async fn keep_up(
    registry: &Registry,
    measurements: &Measurements,
    settings: &CoordinatorSettings,
) -> rusqlite::Result<()> {
    let now = unix_now();
    let silent_since = now.saturating_sub(settings.lost_after.as_secs());
    measurements.mark_lost(silent_since, now).await?;
    let loss_window_start = now.saturating_sub(settings.loss_window.as_secs());
    measurements.forget_losses(loss_window_start).await?;
    let uptime_window_start = now.saturating_sub(settings.uptime_window.as_secs());
    registry.forget_probes(uptime_window_start).await
}

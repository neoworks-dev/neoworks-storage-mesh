//! Checking that nodes are up, which is where their uptime and the loss rate
//! come from, asking them how much they store, and locating them
//! ([`crate::locate`]) from their address and how long they took to answer.
//!
//! The coordinator asks each node for its identity. A node counts as up when
//! it answers with the id it registered: a different server now listening at
//! the address does not count, so a node cannot inherit another's uptime.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use mesh_core::NodeId;
use mesh_core::node_list::NodeRecord;
use mesh_core::protocol::{IDENTITY_ROUTE, NodeIdentity, NodeUsage, USAGE_ROUTE};

use crate::clock::unix_now;
use crate::locate::{Located, Locator};
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

/// What the node at `url` says it stores, or `None` when it doesn't say.
pub async fn reported_usage(http: &reqwest::Client, url: &str) -> Option<u64> {
    let usage_url = format!("{}{USAGE_ROUTE}", url.trim_end_matches('/'));
    let response = http.get(usage_url).send().await.ok()?;
    let body = response.bytes().await.ok()?;
    let usage: NodeUsage = serde_json::from_slice(&body).ok()?;
    Some(usage.used_bytes)
}

/// What one check of a node found.
struct Check {
    node_id: NodeId,
    answered: bool,
    /// What the node says it stores, if it answered and said.
    usage: Option<u64>,
    /// Where the node is, if it answered.
    located: Option<Located>,
}

/// Checks the node `record` names: whether it answers as itself, and if it
/// does, what it stores and where it is.
async fn check(http: &reqwest::Client, locator: &Locator, record: &NodeRecord) -> Check {
    let url = &record.url;
    let asked_at = Instant::now();
    let answered = answers_as(http, url, &record.node_id).await;
    let round_trip = asked_at.elapsed();
    if !answered {
        return Check {
            node_id: record.node_id,
            answered,
            usage: None,
            located: None,
        };
    }
    // Only asked of a node that just proved who it is, so a server that took
    // over its address can't speak for it.
    let usage = reported_usage(http, url).await;
    let located = address_of(url)
        .await
        .map(|address| locator.locate(address, round_trip));
    Check {
        node_id: record.node_id,
        answered,
        usage,
        located,
    }
}

/// The address the coordinator reaches `url` at.
async fn address_of(url: &str) -> Option<IpAddr> {
    let url = reqwest::Url::parse(url).ok()?;
    let port = url.port_or_known_default()?;
    let host = url.host_str()?;
    // An IPv6 host comes in brackets, which address parsing doesn't take.
    if let Ok(address) = host.trim_matches(['[', ']']).parse() {
        return Some(address);
    }
    let mut addresses = tokio::net::lookup_host((host, port)).await.ok()?;
    addresses.next().map(|socket| socket.ip())
}

/// Checks every registered node once, all at the same time, and notes the
/// results: every check for uptime, every answer for the node's life, and
/// what each node that answered says it stores and where it is.
pub async fn probe_all(
    http: &reqwest::Client,
    locator: &Locator,
    registry: &Registry,
    measurements: &Measurements,
) -> rusqlite::Result<()> {
    let records = registry.records().await?;
    let checks = records
        .iter()
        .map(|record| check(http, locator, &record.body));
    let probed_at = unix_now();
    for check in join_all(checks).await {
        let node_id = &check.node_id;
        registry
            .record_probe(node_id, probed_at, check.answered)
            .await?;
        if check.answered {
            measurements.record_answer(node_id, probed_at).await?;
        }
        if let Some(used_bytes) = check.usage {
            measurements
                .record_usage(node_id, used_bytes, probed_at)
                .await?;
        }
        if let Some(located) = &check.located {
            measurements
                .record_place(node_id, located, probed_at)
                .await?;
        }
    }
    Ok(())
}

/// Checks every node each probe interval, forever. After each round it marks
/// nodes silent for too long as lost, and forgets checks and losses older
/// than their windows.
pub async fn probe_forever(
    http: reqwest::Client,
    locator: Locator,
    registry: Arc<Registry>,
    measurements: Arc<Measurements>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.probe_interval);
    loop {
        timer.tick().await;
        // A failed round is retried at the next tick; the nodes it missed
        // only lose one check.
        if let Err(error) = probe_all(&http, &locator, &registry, &measurements).await {
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

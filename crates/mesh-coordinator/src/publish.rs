//! Signing and publishing snapshots of the node list.
//!
//! A snapshot holds every node that answered at least one check in the
//! uptime window, with its record, its uptime, the share of audits it
//! passed, its bandwidth, what it says it stores, the network and country the
//! coordinator found it in, an id of its operator and whether it is down
//! right now, and the loss rate over all nodes. Nodes that stopped
//! answering drop out of the next snapshot; their records stay in the
//! registry, so they come back once they answer again.

use std::sync::Arc;

use mesh_core::node_list::{ListedNode, NodeSnapshot};
use mesh_core::signed::{Signed, SigningKey};
use tokio::sync::RwLock;

use crate::audit_log::AuditLog;
use crate::clock::unix_now;
use crate::earnings::Earnings;
use crate::measurements::Measurements;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;

/// The newest published snapshot, or `None` before the first.
pub type LatestSnapshot = RwLock<Option<Signed<NodeSnapshot>>>;

/// A snapshot made at `now` and signed with `key`.
pub async fn snapshot(
    registry: &Registry,
    measurements: &Measurements,
    earnings: &Earnings,
    audits: &AuditLog,
    key: &SigningKey,
    settings: &CoordinatorSettings,
    now: u64,
) -> rusqlite::Result<Signed<NodeSnapshot>> {
    let uptime_since = now.saturating_sub(settings.uptime_window.as_secs());
    let bandwidth_since = now.saturating_sub(settings.bandwidth_window.as_secs());
    let operator_key = operator_id_key(key);
    let mut nodes = Vec::new();
    for record in registry.records().await? {
        let node_id = record.body.node_id;
        let uptime = registry.uptime_permille(&node_id, uptime_since).await?;
        // `None` is a node not checked in the window, 0 one that never
        // answered: neither is any use to a client.
        let Some(uptime_permille) = uptime.filter(|&permille| permille > 0) else {
            continue;
        };
        let place = measurements.place(&node_id, uptime_since).await?;
        let operator = earnings.operator_of(&node_id).await?;
        nodes.push(ListedNode {
            record,
            uptime_permille,
            audit_pass_permille: audits.pass_permille(&node_id, uptime_since).await?,
            bandwidth_bits_per_second: measurements.bandwidth(&node_id, bandwidth_since).await?,
            used_bytes: measurements.usage(&node_id, uptime_since).await?,
            takes_allowance: settings.own_nodes.contains(&node_id),
            measured_network: place.network,
            measured_country: place.country,
            operator_id: operator.map(|email| operator_id(&operator_key, &email)),
            down: registry.down_since(&node_id).await?.is_some(),
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

/// The key operator ids are made with, derived from the coordinator's own.
fn operator_id_key(key: &SigningKey) -> [u8; 32] {
    blake3::derive_key("neoworks-mesh operator id key v1", &key.to_bytes())
}

/// The id published for the operator `email`: the same for all of that
/// account's nodes, so placement can count them as one operator, but keyed
/// with a secret, so nobody can test which email it stands for.
fn operator_id(key: &[u8; 32], email: &str) -> String {
    // 16 bytes keep ids apart among any number of accounts.
    hex::encode(&blake3::keyed_hash(key, email.as_bytes()).as_bytes()[..16])
}

/// Publishes a new snapshot into `latest` each publish interval, forever.
pub async fn publish_forever(
    registry: Arc<Registry>,
    measurements: Arc<Measurements>,
    earnings: Arc<Earnings>,
    audits: Arc<AuditLog>,
    key: SigningKey,
    latest: Arc<LatestSnapshot>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.publish_interval);
    loop {
        timer.tick().await;
        let made = snapshot(
            &registry,
            &measurements,
            &earnings,
            &audits,
            &key,
            &settings,
            unix_now(),
        )
        .await;
        // A failed round keeps the last snapshot served until the next tick.
        match made {
            Ok(snapshot) => *latest.write().await = Some(snapshot),
            Err(error) => eprintln!("publish: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_operator_has_one_id_that_differs_between_coordinators() {
        let one = operator_id_key(&SigningKey::from_bytes(&[1; 32]));
        let other = operator_id_key(&SigningKey::from_bytes(&[2; 32]));

        assert_eq!(
            operator_id(&one, "op@example.org"),
            operator_id(&one, "op@example.org")
        );
        assert_ne!(
            operator_id(&one, "op@example.org"),
            operator_id(&one, "other@example.org")
        );
        assert_ne!(
            operator_id(&one, "op@example.org"),
            operator_id(&other, "op@example.org")
        );
    }
}

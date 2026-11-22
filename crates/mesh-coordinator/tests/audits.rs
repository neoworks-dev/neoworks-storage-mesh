//! The coordinator auditing nodes with pieces of its own: nodes that keep
//! them pass, nodes that lose them fail, and nodes that require tokens are
//! paid with the coordinator's own.

use std::time::Duration;

use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::node_list::NodeSnapshot;
use mesh_core::signed::Signed;
use mesh_node::{NodeData, NodeSettings, Registration, register};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// A coordinator that probes, audits and publishes every 100 ms.
async fn start_coordinator() -> (TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        probe_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        audit_interval: Duration::from_millis(100),
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url)
}

/// A node with `settings`, registered at `coordinator`; its data directory.
async fn start_registered_node(coordinator: &str, settings: NodeSettings) -> TempDir {
    let directory = tempfile::tempdir().unwrap();
    let data = NodeData::open(directory.path()).await.unwrap();
    let key = data.key.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_node::serve(listener, data, settings));
    let registration = Registration {
        coordinator: coordinator.to_string(),
        url,
        operator: "alice".into(),
        network: "AS3320".into(),
        region: "de".into(),
        capacity_bytes: 1 << 40,
        operator_email: None,
    };
    register(&reqwest::Client::new(), &key, &registration)
        .await
        .unwrap();
    directory
}

/// The audit share of the only node, once a snapshot shows one `wanted`
/// accepts, within ten seconds.
async fn audit_share_where(coordinator: &str, wanted: impl Fn(u16) -> bool) -> u16 {
    for _ in 0..100 {
        let response = reqwest::get(format!("{coordinator}/node-list"))
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            let body = response.bytes().await.unwrap();
            let snapshot: Signed<NodeSnapshot> = serde_json::from_slice(&body).unwrap();
            let share = snapshot
                .body
                .nodes
                .first()
                .and_then(|node| node.audit_pass_permille);
            if let Some(share) = share.filter(|&share| wanted(share)) {
                return share;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no snapshot with the audit share wanted was published");
}

#[tokio::test]
async fn a_node_that_keeps_its_audit_piece_passes() {
    let (_coordinator_data, coordinator) = start_coordinator().await;
    let node_data = start_registered_node(&coordinator, NodeSettings::default()).await;

    let share = audit_share_where(&coordinator, |_| true).await;

    assert_eq!(share, 1000);
    let pieces = std::fs::read_dir(node_data.path().join("pieces"))
        .unwrap()
        .count();
    assert_eq!(pieces, 1);
}

#[tokio::test]
async fn a_node_that_requires_tokens_is_paid_and_audited() {
    let (_coordinator_data, coordinator) = start_coordinator().await;
    let settings = NodeSettings {
        require_tokens: Some(coordinator.clone()),
        ..NodeSettings::default()
    };
    let _node_data = start_registered_node(&coordinator, settings).await;

    let share = audit_share_where(&coordinator, |_| true).await;

    assert_eq!(share, 1000);
}

#[tokio::test]
async fn a_node_that_loses_its_audit_piece_fails() {
    let (_coordinator_data, coordinator) = start_coordinator().await;
    let node_data = start_registered_node(&coordinator, NodeSettings::default()).await;
    audit_share_where(&coordinator, |share| share == 1000).await;

    for entry in std::fs::read_dir(node_data.path().join("pieces")).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }

    let share = audit_share_where(&coordinator, |share| share < 1000).await;
    assert!(share < 1000);
}

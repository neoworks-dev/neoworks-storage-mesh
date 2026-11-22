//! Nodes registering with a coordinator over HTTP, and the snapshot it
//! publishes.

use std::time::Duration;

use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::node_list::{CoordinatorList, NodeSnapshot};
use mesh_core::signed::{Signed, SigningKey};
use mesh_node::{NodeData, NodeSettings, Registration, RegistrationError, register};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// A coordinator that publishes every 100 ms, so tests need not wait.
async fn start_coordinator() -> (TempDir, String, CoordinatorList) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let coordinators = CoordinatorList {
        version: 1,
        keys: vec![data.public_key()],
    };
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url, coordinators)
}

/// A node and its key.
async fn start_node() -> (TempDir, String, SigningKey) {
    let directory = tempfile::tempdir().unwrap();
    let data = NodeData::open(directory.path()).await.unwrap();
    let key = data.key.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_node::serve(listener, data, NodeSettings::default()));
    (directory, url, key)
}

fn registration(coordinator: &str, node_url: &str) -> Registration {
    Registration {
        coordinator: coordinator.to_string(),
        url: node_url.to_string(),
        operator: "alice".into(),
        network: "AS3320".into(),
        region: "de".into(),
        capacity_bytes: 1 << 40,
    }
}

/// The first snapshot with at least `count` nodes, within five seconds.
async fn snapshot_with(coordinator: &str, count: usize) -> Signed<NodeSnapshot> {
    snapshot_where(coordinator, |snapshot| snapshot.nodes.len() >= count).await
}

/// The first snapshot `wanted` accepts, within five seconds.
async fn snapshot_where(
    coordinator: &str,
    wanted: impl Fn(&NodeSnapshot) -> bool,
) -> Signed<NodeSnapshot> {
    for _ in 0..50 {
        let response = reqwest::get(format!("{coordinator}/node-list"))
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            let body = response.bytes().await.unwrap();
            let snapshot: Signed<NodeSnapshot> = serde_json::from_slice(&body).unwrap();
            if wanted(&snapshot.body) {
                return snapshot;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no snapshot as wanted was published");
}

#[tokio::test]
async fn a_registered_nodes_bandwidth_is_measured_and_published() {
    let (_coordinator_data, coordinator, _coordinators) = start_coordinator().await;
    let (_node_data, node_url, node_key) = start_node().await;
    let http = reqwest::Client::new();

    register(&http, &node_key, &registration(&coordinator, &node_url))
        .await
        .unwrap();

    let snapshot = snapshot_where(&coordinator, |snapshot| {
        snapshot
            .nodes
            .iter()
            .any(|node| node.bandwidth_bits_per_second.is_some())
    })
    .await;
    // Over localhost the node is far faster than any real connection.
    let bandwidth = snapshot.body.nodes[0].bandwidth_bits_per_second.unwrap();
    assert!(bandwidth > 100_000_000, "{bandwidth} bits per second");
    // One node watched for moments says nothing about losses yet.
    assert_eq!(snapshot.body.daily_loss_per_million, None);
}

#[tokio::test]
async fn a_registered_node_is_in_the_signed_snapshot() {
    let (_coordinator_data, coordinator, coordinators) = start_coordinator().await;
    let (_node_data, node_url, node_key) = start_node().await;
    let http = reqwest::Client::new();

    register(&http, &node_key, &registration(&coordinator, &node_url))
        .await
        .unwrap();

    let snapshot = snapshot_with(&coordinator, 1).await;
    assert_eq!(coordinators.check_snapshot(&snapshot), Ok(()));
    let listed = &snapshot.body.nodes[0];
    assert_eq!(listed.record.verify_node(), Ok(()));
    assert_eq!(listed.record.body.url, node_url);
    assert_eq!(listed.uptime_permille, 1000);
}

#[tokio::test]
async fn a_node_cannot_register_another_nodes_address() {
    let (_coordinator_data, coordinator, _coordinators) = start_coordinator().await;
    let (_one_data, _one_url, one_key) = start_node().await;
    let (_other_data, other_url, _other_key) = start_node().await;
    let http = reqwest::Client::new();

    let result = register(&http, &one_key, &registration(&coordinator, &other_url)).await;

    assert!(matches!(
        result,
        Err(RegistrationError::Refused {
            status: StatusCode::BAD_REQUEST,
            ..
        })
    ));
}

#[tokio::test]
async fn a_node_that_does_not_answer_cannot_register() {
    let (_coordinator_data, coordinator, _coordinators) = start_coordinator().await;
    let (_node_data, _node_url, node_key) = start_node().await;
    let http = reqwest::Client::new();
    // Nothing listens on port 9 of localhost in a test environment.
    let nowhere = "http://127.0.0.1:9";

    let result = register(&http, &node_key, &registration(&coordinator, nowhere)).await;

    assert!(matches!(
        result,
        Err(RegistrationError::Refused {
            status: StatusCode::BAD_REQUEST,
            ..
        })
    ));
}

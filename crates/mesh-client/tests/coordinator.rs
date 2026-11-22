//! Storing a file on nodes from the signed node list a coordinator publishes.

mod test_nodes;

use std::time::Duration;

use mesh_client::{NodeClient, NodeList, NodeListError};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::node_list::{CoordinatorKey, CoordinatorList, NodeSnapshot, TrustError};
use mesh_core::signed::{Signed, SigningKey};
use mesh_node::{NodeSettings, Registration, register};
use tempfile::TempDir;
use test_nodes::{download_bytes, random_file, start_nodes, store_on};
use tokio::net::TcpListener;

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

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
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url, coordinators)
}

/// The first list with at least `count` nodes, within five seconds.
async fn list_with(
    client: &NodeClient,
    coordinator: &str,
    coordinators: &CoordinatorList,
    count: usize,
) -> NodeList {
    for _ in 0..50 {
        if let Ok((list, _)) = NodeList::fetch(client, coordinator, coordinators).await {
            if list.nodes.len() >= count {
                return list;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no node list with {count} nodes was published");
}

#[tokio::test]
async fn a_file_is_stored_on_nodes_from_the_coordinator() {
    let (_coordinator_data, coordinator, coordinators) = start_coordinator().await;
    let nodes = start_nodes(NodeSettings::default()).await;
    let http = reqwest::Client::new();
    for (index, node) in nodes.iter().enumerate() {
        let registration = Registration {
            coordinator: coordinator.clone(),
            url: node.url.clone(),
            operator: format!("operator-{index}"),
            network: format!("network-{index}"),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            operator_email: None,
        };
        register(&http, &node.key, &registration).await.unwrap();
    }
    let client = NodeClient::new().unwrap();

    let list = list_with(&client, &coordinator, &coordinators, nodes.len()).await;
    let file = random_file(100_000, 3);
    let manifest = store_on(&list, &file, LEASE).await.unwrap();

    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
    let ids: Vec<_> = list.nodes.iter().map(|node| node.id).collect();
    assert!(nodes.iter().all(|node| ids.contains(&node.id)));
}

#[tokio::test]
async fn a_list_signed_by_an_unknown_coordinator_is_refused() {
    let (_coordinator_data, coordinator, _coordinators) = start_coordinator().await;
    let strangers = CoordinatorList {
        version: 1,
        keys: vec![CoordinatorKey([7; 32])],
    };
    let client = NodeClient::new().unwrap();
    // The first snapshot is published right after start.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = NodeList::fetch(&client, &coordinator, &strangers).await;

    assert!(matches!(
        result,
        Err(NodeListError::Untrusted(TrustError::UnknownSigner))
    ));
}

/// Waits for the coordinator's first snapshot, within five seconds.
async fn wait_for_first_snapshot(
    client: &NodeClient,
    coordinator: &str,
    coordinators: &CoordinatorList,
) {
    list_with(client, coordinator, coordinators, 0).await;
}

/// The URL of a port nothing listens on.
async fn unreachable_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    format!("http://{}", listener.local_addr().unwrap())
}

#[tokio::test]
async fn the_kept_list_is_used_while_the_coordinator_is_down() {
    let (_coordinator_data, coordinator, coordinators) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    wait_for_first_snapshot(&client, &coordinator, &coordinators).await;
    let state = tempfile::tempdir().unwrap();
    let kept_path = state.path().join("node-list.json");

    let fetched = NodeList::fetch_keeping_newest(&client, &coordinator, &coordinators, &kept_path)
        .await
        .unwrap();
    let down = unreachable_url().await;
    let fallen_back = NodeList::fetch_keeping_newest(&client, &down, &coordinators, &kept_path)
        .await
        .unwrap();

    assert!(fetched.unreachable.is_none());
    assert!(matches!(
        fallen_back.unreachable,
        Some(NodeListError::Fetch(_))
    ));
    assert_eq!(fallen_back.published_at, fetched.published_at);
}

#[tokio::test]
async fn without_a_kept_list_an_unreachable_coordinator_is_an_error() {
    let client = NodeClient::new().unwrap();
    let state = tempfile::tempdir().unwrap();
    let coordinators = CoordinatorList {
        version: 1,
        keys: vec![CoordinatorKey([7; 32])],
    };

    let down = unreachable_url().await;
    let result = NodeList::fetch_keeping_newest(
        &client,
        &down,
        &coordinators,
        &state.path().join("node-list.json"),
    )
    .await;

    assert!(matches!(result, Err(NodeListError::Fetch(_))));
}

#[tokio::test]
async fn a_list_older_than_the_kept_one_is_refused() {
    let (_coordinator_data, coordinator, mut coordinators) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    wait_for_first_snapshot(&client, &coordinator, &coordinators).await;
    // A second coordinator has signed a snapshot from far in the future,
    // which makes everything the first one publishes now look old.
    let other_coordinator = SigningKey::from_bytes(&[3; 32]);
    coordinators
        .keys
        .push(CoordinatorKey(other_coordinator.verifying_key().to_bytes()));
    let newer = NodeSnapshot {
        published_at: u64::from(u32::MAX),
        nodes: Vec::new(),
        daily_loss_per_million: None,
    };
    let kept_json = serde_json::to_vec(&Signed::sign(&other_coordinator, newer)).unwrap();
    let state = tempfile::tempdir().unwrap();
    let kept_path = state.path().join("node-list.json");
    std::fs::write(&kept_path, &kept_json).unwrap();

    let result =
        NodeList::fetch_keeping_newest(&client, &coordinator, &coordinators, &kept_path).await;

    assert!(matches!(
        result,
        Err(NodeListError::Older {
            kept: 4_294_967_295,
            ..
        })
    ));
    assert_eq!(std::fs::read(&kept_path).unwrap(), kept_json);
}

#[tokio::test]
async fn a_kept_list_from_a_dropped_coordinator_is_forgotten() {
    let (_coordinator_data, coordinator, coordinators) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    wait_for_first_snapshot(&client, &coordinator, &coordinators).await;
    // Signed by a coordinator that is not on the list (any more).
    let dropped_coordinator = SigningKey::from_bytes(&[3; 32]);
    let newer = NodeSnapshot {
        published_at: u64::from(u32::MAX),
        nodes: Vec::new(),
        daily_loss_per_million: None,
    };
    let state = tempfile::tempdir().unwrap();
    let kept_path = state.path().join("node-list.json");
    let kept_json = serde_json::to_vec(&Signed::sign(&dropped_coordinator, newer)).unwrap();
    std::fs::write(&kept_path, kept_json).unwrap();

    let fetched = NodeList::fetch_keeping_newest(&client, &coordinator, &coordinators, &kept_path)
        .await
        .unwrap();

    let kept: Signed<NodeSnapshot> =
        serde_json::from_slice(&std::fs::read(&kept_path).unwrap()).unwrap();
    assert_eq!(kept.body.published_at, fetched.published_at);
}

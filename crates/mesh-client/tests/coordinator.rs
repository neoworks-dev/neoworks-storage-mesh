//! Storing a file on nodes from the signed node list a coordinator publishes.

mod test_nodes;

use std::time::Duration;

use mesh_client::{NodeClient, NodeList, NodeListError};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::node_list::{CoordinatorKey, CoordinatorList, TrustError};
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

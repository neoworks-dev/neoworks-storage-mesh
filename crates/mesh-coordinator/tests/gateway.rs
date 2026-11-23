//! A web page's requests reaching a node through the coordinator's gateway.

use std::time::Duration;

use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::gateway::{gateway_piece_path, gateway_pointer_path};
use mesh_core::node_list::NodeSnapshot;
use mesh_core::pointer::{Pointer, SigningKey};
use mesh_core::signed::Signed;
use mesh_core::{NodeId, PieceId};
use mesh_node::{NodeData, NodeSettings, Registration, register};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// A coordinator that publishes and probes every 100 ms, and a node
/// registered there, once the node list holds it; the coordinator's URL and
/// the node's id.
async fn start() -> (Vec<TempDir>, String, NodeId) {
    let coordinator_directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(coordinator_directory.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator = format!("http://{}", listener.local_addr().unwrap());
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        probe_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        ..CoordinatorSettings::default()
    };
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));

    let node_directory = tempfile::tempdir().unwrap();
    let node = NodeData::open(node_directory.path()).await.unwrap();
    let key = node.key.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_node::serve(listener, node, NodeSettings::default()));
    let registration = Registration {
        coordinator: coordinator.clone(),
        url: node_url,
        operator: "alice".into(),
        network: "AS3320".into(),
        region: "de".into(),
        capacity_bytes: 1 << 40,
        operator_email: None,
    };
    register(&reqwest::Client::new(), &key, &registration)
        .await
        .unwrap();
    let node_id = NodeId::of(&key.verifying_key().to_bytes());
    wait_for_listing(&coordinator).await;
    (
        vec![coordinator_directory, node_directory],
        coordinator,
        node_id,
    )
}

/// Waits up to five seconds for a node list with a node in it.
async fn wait_for_listing(coordinator: &str) {
    for _ in 0..50 {
        let response = reqwest::get(format!("{coordinator}/node-list"))
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            let body = response.bytes().await.unwrap();
            let snapshot: Signed<NodeSnapshot> = serde_json::from_slice(&body).unwrap();
            if !snapshot.body.nodes.is_empty() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the node was never listed");
}

#[tokio::test]
async fn a_piece_is_stored_and_read_through_the_gateway() {
    let (_directories, coordinator, node) = start().await;
    let http = reqwest::Client::new();
    let bytes = b"an encrypted piece".to_vec();
    let id = PieceId::of(&bytes);
    let url = format!("{coordinator}{}", gateway_piece_path(&node, &id));

    let stored = http
        .put(format!("{url}?lease_seconds=3600"))
        .body(bytes.clone())
        .send()
        .await
        .unwrap();
    let fetched = http.get(&url).send().await.unwrap();

    assert_eq!(stored.status(), StatusCode::CREATED);
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(fetched.headers()["access-control-allow-origin"], "*");
    assert_eq!(fetched.bytes().await.unwrap().to_vec(), bytes);
}

#[tokio::test]
async fn the_gateway_refuses_wrong_bytes_and_unlisted_nodes() {
    let (_directories, coordinator, node) = start().await;
    let http = reqwest::Client::new();
    let id = PieceId::of(b"the real piece");
    let stranger = NodeId::of(&[9; 32]);

    let wrong = http
        .put(format!(
            "{coordinator}{}?lease_seconds=3600",
            gateway_piece_path(&node, &id)
        ))
        .body(b"something else".to_vec())
        .send()
        .await
        .unwrap();
    let unlisted = http
        .get(format!(
            "{coordinator}{}",
            gateway_piece_path(&stranger, &id)
        ))
        .send()
        .await
        .unwrap();
    let missing = http
        .get(format!("{coordinator}{}", gateway_piece_path(&node, &id)))
        .send()
        .await
        .unwrap();

    assert_eq!(wrong.status(), StatusCode::BAD_REQUEST);
    assert_eq!(unlisted.status(), StatusCode::NOT_FOUND);
    // The node's own answer passes through.
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_pointer_is_stored_and_read_through_the_gateway() {
    let (_directories, coordinator, node) = start().await;
    let http = reqwest::Client::new();
    let owner_key = SigningKey::from_bytes(&[4; 32]);
    let owner = owner_key.verifying_key().to_bytes();
    let pointer = Pointer::sign(&owner_key, 1, b"sealed target".to_vec()).unwrap();
    let url = format!("{coordinator}{}", gateway_pointer_path(&node, &owner));
    let mut forged = pointer.clone();
    forged.version = 2;

    let put = |pointer: &Pointer| {
        http.put(format!("{url}?lease_seconds=3600"))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(pointer).unwrap())
            .send()
    };

    let refused = put(&forged).await.unwrap();
    let stored = put(&pointer).await.unwrap();
    let fetched = http.get(&url).send().await.unwrap();

    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    assert_eq!(stored.status(), StatusCode::CREATED);
    let fetched: Pointer = serde_json::from_slice(&fetched.bytes().await.unwrap()).unwrap();
    assert_eq!(fetched, pointer);
}

#[tokio::test]
async fn a_browser_may_put_from_any_origin() {
    let (_directories, coordinator, node) = start().await;
    let id = PieceId::of(b"piece");

    let preflight = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!("{coordinator}{}", gateway_piece_path(&node, &id)),
        )
        .header("origin", "https://app.example.org")
        .header("access-control-request-method", "PUT")
        .send()
        .await
        .unwrap();

    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
    let headers = preflight.headers();
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert_eq!(headers["access-control-allow-methods"], "GET, PUT");
}

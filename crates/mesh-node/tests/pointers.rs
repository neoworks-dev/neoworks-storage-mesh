//! Storing and fetching signed pointers on a node over HTTP.

use mesh_core::pointer::{Pointer, SigningKey};
use mesh_core::protocol::{LeaseResponse, PointerConflict, pointer_path};
use mesh_node::{NodeData, NodeSettings};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// A node running in this process; it stops when the test ends.
async fn start_node() -> (TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let data = NodeData::open(directory.path()).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_node::serve(listener, data, NodeSettings::default()));
    (directory, url)
}

fn pointer(version: u64, target: &[u8]) -> Pointer {
    Pointer::sign(&SigningKey::from_bytes(&[1; 32]), version, target.to_vec()).unwrap()
}

/// Stores `pointer` under `owner` for a day.
async fn put(url: &str, owner: &[u8; 32], pointer: &Pointer) -> reqwest::Response {
    reqwest::Client::new()
        .put(format!("{url}{}?lease_seconds=86400", pointer_path(owner)))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(pointer).unwrap())
        .send()
        .await
        .unwrap()
}

async fn get(url: &str, owner: &[u8; 32]) -> reqwest::Response {
    reqwest::get(format!("{url}{}", pointer_path(owner)))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_stored_pointer_comes_back() {
    let (_data, url) = start_node().await;
    let record = pointer(1, b"index");

    let stored = put(&url, &record.owner, &record).await;
    assert_eq!(stored.status(), StatusCode::CREATED);
    let _: LeaseResponse = serde_json::from_slice(&stored.bytes().await.unwrap()).unwrap();

    let fetched = get(&url, &record.owner).await;
    assert_eq!(fetched.status(), StatusCode::OK);
    let fetched: Pointer = serde_json::from_slice(&fetched.bytes().await.unwrap()).unwrap();
    assert_eq!(fetched, record);
}

#[tokio::test]
async fn an_older_version_gets_a_conflict_naming_the_held_one() {
    let (_data, url) = start_node().await;
    let newer = pointer(5, b"newer");
    put(&url, &newer.owner, &newer).await;

    let older = pointer(4, b"older");
    let answer = put(&url, &older.owner, &older).await;

    assert_eq!(answer.status(), StatusCode::CONFLICT);
    let conflict: PointerConflict = serde_json::from_slice(&answer.bytes().await.unwrap()).unwrap();
    assert_eq!(conflict.held_version, 5);
}

#[tokio::test]
async fn refuses_a_pointer_filed_under_another_owner() {
    let (_data, url) = start_node().await;
    let record = pointer(1, b"index");
    let other_owner = SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes();

    let answer = put(&url, &other_owner, &record).await;

    assert_eq!(answer.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        get(&url, &other_owner).await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn refuses_a_forged_pointer() {
    let (_data, url) = start_node().await;
    let mut forged = pointer(1, b"index");
    forged.target = b"junk".to_vec();

    let answer = put(&url, &forged.owner, &forged).await;

    assert_eq!(answer.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        get(&url, &forged.owner).await.status(),
        StatusCode::NOT_FOUND
    );
}

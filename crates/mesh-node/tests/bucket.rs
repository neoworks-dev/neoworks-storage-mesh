//! A node keeping its pieces in an S3-compatible bucket, against a fake
//! store in this process.

mod fake_bucket;

use fake_bucket::FakeStore;
use mesh_core::PieceId;
use mesh_core::merkle::{self, SEGMENT_BYTES};
use mesh_core::protocol::{ChallengeRequest, ChallengeResponse, NodeUsage, USAGE_ROUTE};
use mesh_node::{Bucket, NodeData, NodeSettings, PieceStore};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Bytes that differ from piece to piece.
fn piece_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn a_bucket_keeps_objects_and_hands_back_ranges() {
    let store = FakeStore::start().await;
    let bucket = Bucket::new(store.settings("node-1")).unwrap();
    let bytes = piece_bytes(10_000, 1);

    bucket.put("abc", &bytes).await.unwrap();

    assert_eq!(bucket.get("abc").await.unwrap(), Some(bytes.clone()));
    assert_eq!(
        bucket.get_range("abc", 4_096, 100).await.unwrap(),
        Some(bytes[4_096..4_196].to_vec())
    );
    assert_eq!(bucket.len_of("abc").await.unwrap(), Some(10_000));
    assert_eq!(bucket.get("missing").await.unwrap(), None);
    assert_eq!(bucket.len_of("missing").await.unwrap(), None);
    bucket
        .delete_many(&["abc".to_string(), "missing".to_string()])
        .await
        .unwrap();
    assert_eq!(bucket.get("abc").await.unwrap(), None);
}

#[tokio::test]
async fn ended_pieces_leave_the_bucket_in_one_request() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let bucket = Bucket::new(store.settings("node-1")).unwrap();
    let pieces = PieceStore::open_with_bucket(directory.path(), bucket)
        .await
        .unwrap();
    let kept = piece_bytes(500, 0);
    pieces.put(&PieceId::of(&kept), &kept, 1_000).await.unwrap();
    for seed in 1..=3 {
        let bytes = piece_bytes(100, seed);
        pieces.put(&PieceId::of(&bytes), &bytes, 100).await.unwrap();
    }

    assert_eq!(pieces.remove_ended(200).await.unwrap(), 3);

    assert_eq!(store.object_count("node-1"), 1);
    assert_eq!(store.contents.lock().unwrap().batch_deletes, 1);
    assert_eq!(pieces.stored_bytes().await.unwrap(), 500);
    assert_eq!(
        pieces.get(&PieceId::of(&kept), 200).await.unwrap(),
        Some(kept)
    );
}

/// A node keeping pieces in `bucket` of `store`, with its leases in
/// `directory`; it stops when the test ends.
async fn start_node(store: &FakeStore, bucket: &str, directory: &TempDir) -> String {
    let bucket = Bucket::new(store.settings(bucket)).unwrap();
    let data = NodeData::open_with_bucket(directory.path(), bucket)
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_node::serve(listener, data, NodeSettings::default()));
    url
}

#[tokio::test]
async fn a_node_serves_pieces_from_its_bucket() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let url = start_node(&store, "node-1", &directory).await;
    let http = reqwest::Client::new();
    let bytes = piece_bytes(3 * SEGMENT_BYTES + 10, 7);
    let id = PieceId::of(&bytes);

    let stored = http
        .put(format!("{url}/pieces/{id}?lease_seconds=3600"))
        .body(bytes.clone())
        .send()
        .await
        .unwrap();

    assert!(stored.status().is_success());
    assert_eq!(store.object_count("node-1"), 1);
    // Nothing of the piece is in the data directory.
    assert!(!directory.path().join("pieces").exists());
    let fetched = reqwest::get(format!("{url}/pieces/{id}")).await.unwrap();
    assert_eq!(fetched.bytes().await.unwrap().to_vec(), bytes);
    let challenge = ChallengeRequest {
        segments: vec![0, 3],
    };
    let answer = http
        .post(format!("{url}/pieces/{id}/challenge"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&challenge).unwrap())
        .send()
        .await
        .unwrap();
    let answer: ChallengeResponse = serde_json::from_slice(&answer.bytes().await.unwrap()).unwrap();
    let root = merkle::root_of(&bytes);
    assert_eq!(answer.proofs.len(), 2);
    assert!(answer.proofs.iter().all(|proof| proof.leads_to(&root)));
    let usage = reqwest::get(format!("{url}{USAGE_ROUTE}")).await.unwrap();
    let usage: NodeUsage = serde_json::from_slice(&usage.bytes().await.unwrap()).unwrap();
    assert_eq!(usage.used_bytes, bytes.len() as u64);
}

#[tokio::test]
async fn a_new_node_process_takes_the_bucket_over() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let first = start_node(&store, "node-1", &directory).await;
    let bytes = piece_bytes(1_000, 9);
    let id = PieceId::of(&bytes);
    reqwest::Client::new()
        .put(format!("{first}/pieces/{id}?lease_seconds=3600"))
        .body(bytes.clone())
        .send()
        .await
        .unwrap();

    let second = start_node(&store, "node-1", &directory).await;

    let fetched = reqwest::get(format!("{second}/pieces/{id}")).await.unwrap();
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(fetched.bytes().await.unwrap().to_vec(), bytes);
}

//! A node keeping its pieces in an S3-compatible bucket, against a fake
//! store in this process.

mod fake_bucket;

use fake_bucket::FakeStore;
use mesh_core::PieceId;
use mesh_core::merkle::{self, GROUP_SEGMENTS, SEGMENT_BYTES};
use mesh_core::protocol::{ChallengeRequest, ChallengeResponse, NodeUsage, USAGE_ROUTE};
use mesh_node::{Bucket, ChallengeAnswer, NodeData, NodeSettings, PieceStore};
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

/// A piece store keeping pieces in the bucket `node-1` of `store`, with its
/// leases in `directory`.
async fn bucket_store(store: &FakeStore, directory: &TempDir) -> PieceStore {
    let bucket = Bucket::new(store.settings("node-1")).unwrap();
    PieceStore::open_with_bucket(directory.path(), bucket)
        .await
        .unwrap()
}

/// The object bytes `store` has sent so far.
fn bytes_sent(store: &FakeStore) -> usize {
    store.contents.lock().unwrap().bytes_sent
}

#[tokio::test]
async fn a_challenge_reads_only_the_groups_of_its_segments() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let pieces = bucket_store(&store, &directory).await;
    let bytes = piece_bytes(100 * SEGMENT_BYTES - 5, 3);
    let id = PieceId::of(&bytes);
    pieces.put(&id, &bytes, 1_000).await.unwrap();
    let before = bytes_sent(&store);

    // Segments 70 and 75 share group 4; segment 5 is in group 0.
    let answer = pieces.prove(&id, &[70, 5, 75], 0).await.unwrap();

    let expected = [70, 5, 75].map(|index| merkle::prove(&bytes, index).unwrap());
    assert_eq!(answer, ChallengeAnswer::Proofs(expected.to_vec()));
    assert_eq!(
        bytes_sent(&store) - before,
        2 * GROUP_SEGMENTS * SEGMENT_BYTES
    );
}

#[tokio::test]
async fn a_challenge_reads_a_short_last_group_and_an_empty_piece() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let pieces = bucket_store(&store, &directory).await;
    let short = piece_bytes(20 * SEGMENT_BYTES + 1, 4);
    let empty = Vec::new();
    pieces
        .put(&PieceId::of(&short), &short, 1_000)
        .await
        .unwrap();
    pieces
        .put(&PieceId::of(&empty), &empty, 1_000)
        .await
        .unwrap();

    let last = pieces.prove(&PieceId::of(&short), &[20], 0).await.unwrap();
    let only = pieces.prove(&PieceId::of(&empty), &[0], 0).await.unwrap();

    let expected = merkle::prove(&short, 20).unwrap();
    assert_eq!(last, ChallengeAnswer::Proofs(vec![expected]));
    let expected = merkle::prove(&empty, 0).unwrap();
    assert_eq!(only, ChallengeAnswer::Proofs(vec![expected]));
}

#[tokio::test]
async fn a_challenge_beyond_the_piece_or_its_lease_reads_nothing() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let pieces = bucket_store(&store, &directory).await;
    let bytes = piece_bytes(3 * SEGMENT_BYTES, 5);
    let id = PieceId::of(&bytes);
    pieces.put(&id, &bytes, 1_000).await.unwrap();
    let before = bytes_sent(&store);

    let beyond = pieces.prove(&id, &[0, 3], 0).await.unwrap();
    let ended = pieces.prove(&id, &[0], 1_000).await.unwrap();

    let no_segment = ChallengeAnswer::NoSegment {
        index: 3,
        segment_count: 3,
    };
    assert_eq!(beyond, no_segment);
    assert_eq!(ended, ChallengeAnswer::NotHeld);
    assert_eq!(bytes_sent(&store), before);
}

#[tokio::test]
async fn a_piece_without_group_hashes_is_proven_from_all_of_it() {
    let store = FakeStore::start().await;
    let directory = tempfile::tempdir().unwrap();
    let pieces = bucket_store(&store, &directory).await;
    let bytes = piece_bytes(40 * SEGMENT_BYTES, 6);
    let id = PieceId::of(&bytes);
    pieces.put(&id, &bytes, 1_000).await.unwrap();
    // As a node stored pieces before it recorded group hashes.
    rusqlite::Connection::open(directory.path().join("leases.sqlite3"))
        .unwrap()
        .execute("UPDATE lease SET group_hashes = NULL", [])
        .unwrap();
    let before = bytes_sent(&store);

    let answer = pieces.prove(&id, &[33], 0).await.unwrap();

    let expected = merkle::prove(&bytes, 33).unwrap();
    assert_eq!(answer, ChallengeAnswer::Proofs(vec![expected]));
    assert_eq!(bytes_sent(&store) - before, bytes.len());
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

//! Stores a file on six real nodes over HTTP and reads it back, also after
//! nodes go down, lose pieces or return corrupted ones.

mod test_nodes;

use std::time::Duration;

use mesh_client::erasure::RebuildError;
use mesh_client::{CHUNK_BYTES, DownloadError, Manifest, NodeClient, download, upload};
use mesh_node::NodeSettings;
use test_nodes::{start_nodes, urls};

/// Long enough that no piece expires while a test runs.
const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// Two and a half chunks of bytes that differ from position to position, so a
/// misplaced piece would show.
fn sample_file() -> Vec<u8> {
    let length = CHUNK_BYTES * 5 / 2 + 123;
    (0..length)
        .map(|position| (position * 31 % 251) as u8)
        .collect()
}

/// Reads with a new client, so no connection opened during the upload is reused.
async fn read_back(manifest: &Manifest) -> Result<Vec<u8>, DownloadError> {
    download(&NodeClient::new().unwrap(), manifest).await
}

#[tokio::test]
async fn a_file_comes_back_unchanged() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();

    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file, LEASE)
        .await
        .unwrap();

    assert_eq!(manifest.chunks.len(), 3);
    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn nodes_only_ever_hold_noise() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = vec![b'A'; CHUNK_BYTES];

    upload(&NodeClient::new().unwrap(), &urls(&nodes), &file, LEASE)
        .await
        .unwrap();

    // A megabyte of 'A' would show up as long runs of 'A' in any piece that
    // held plaintext. Sealed bytes have none.
    for node in &nodes {
        for path in node.piece_files() {
            let piece = std::fs::read(path).unwrap();
            assert!(!piece.windows(16).any(|window| window == [b'A'; 16]));
        }
    }
}

#[tokio::test]
async fn survives_two_nodes_going_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file, LEASE)
        .await
        .unwrap();

    nodes[0].stop();
    nodes[4].stop();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn survives_one_node_losing_and_one_corrupting_its_pieces() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file, LEASE)
        .await
        .unwrap();

    nodes[1].lose_all_pieces();
    nodes[2].corrupt_all_pieces();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn fails_when_three_nodes_are_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file, LEASE)
        .await
        .unwrap();

    nodes[0].stop();
    nodes[2].stop();
    nodes[5].stop();

    let result = read_back(&manifest).await;

    assert!(matches!(
        result,
        Err(DownloadError::Rebuild {
            index: 0,
            source: RebuildError::TooFewPieces { found: 3 }
        })
    ));
}

#[tokio::test]
async fn an_empty_file_comes_back_empty() {
    let nodes = start_nodes(NodeSettings::default()).await;

    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &[], LEASE)
        .await
        .unwrap();

    assert!(manifest.chunks.is_empty());
    assert_eq!(read_back(&manifest).await.unwrap(), Vec::<u8>::new());
}

#[tokio::test]
async fn refuses_anything_but_six_nodes() {
    let nodes = start_nodes(NodeSettings::default()).await;

    let result = upload(
        &NodeClient::new().unwrap(),
        &urls(&nodes[..5]),
        b"photo",
        LEASE,
    )
    .await;

    assert!(matches!(
        result,
        Err(mesh_client::UploadError::WrongNodeCount { given: 5 })
    ));
}

//! Stores a file on six real nodes over HTTP and reads it back, also after
//! nodes go down, lose pieces or return corrupted ones.

use mesh_client::erasure::{RebuildError, TOTAL_PIECES};
use mesh_client::{CHUNK_BYTES, DownloadError, Manifest, NodeClient, download, upload};
use mesh_node::PieceStore;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A node running in this process, with its own data directory.
struct TestNode {
    url: String,
    data: TempDir,
    server: JoinHandle<std::io::Result<()>>,
}

impl TestNode {
    async fn start() -> TestNode {
        let data = tempfile::tempdir().unwrap();
        let store = PieceStore::open(data.path()).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(mesh_node::serve(listener, store));
        TestNode { url, data, server }
    }

    /// Stops the node: its port refuses new connections.
    fn stop(&self) {
        self.server.abort();
    }

    /// Deletes every piece the node holds, as if its disk had been wiped.
    fn lose_all_pieces(&self) {
        for entry in std::fs::read_dir(self.data.path()).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
    }

    /// Overwrites every piece the node holds with other bytes of the same length.
    fn corrupt_all_pieces(&self) {
        for entry in std::fs::read_dir(self.data.path()).unwrap() {
            let path = entry.unwrap().path();
            let length = std::fs::metadata(&path).unwrap().len() as usize;
            std::fs::write(&path, vec![0xAB; length]).unwrap();
        }
    }
}

async fn start_nodes() -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..TOTAL_PIECES {
        nodes.push(TestNode::start().await);
    }
    nodes
}

fn urls(nodes: &[TestNode]) -> Vec<String> {
    nodes.iter().map(|node| node.url.clone()).collect()
}

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
    let nodes = start_nodes().await;
    let file = sample_file();

    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file)
        .await
        .unwrap();

    assert_eq!(manifest.chunks.len(), 3);
    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn nodes_only_ever_hold_noise() {
    let nodes = start_nodes().await;
    let file = vec![b'A'; CHUNK_BYTES];

    upload(&NodeClient::new().unwrap(), &urls(&nodes), &file)
        .await
        .unwrap();

    // A megabyte of 'A' would show up as long runs of 'A' in any piece that
    // held plaintext. Sealed bytes have none.
    for node in &nodes {
        for entry in std::fs::read_dir(node.data.path()).unwrap() {
            let piece = std::fs::read(entry.unwrap().path()).unwrap();
            assert!(!piece.windows(16).any(|window| window == [b'A'; 16]));
        }
    }
}

#[tokio::test]
async fn survives_two_nodes_going_down() {
    let nodes = start_nodes().await;
    let file = sample_file();
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file)
        .await
        .unwrap();

    nodes[0].stop();
    nodes[4].stop();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn survives_one_node_losing_and_one_corrupting_its_pieces() {
    let nodes = start_nodes().await;
    let file = sample_file();
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &file)
        .await
        .unwrap();

    nodes[1].lose_all_pieces();
    nodes[2].corrupt_all_pieces();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn fails_when_three_nodes_are_down() {
    let nodes = start_nodes().await;
    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &sample_file())
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
    let nodes = start_nodes().await;

    let manifest = upload(&NodeClient::new().unwrap(), &urls(&nodes), &[])
        .await
        .unwrap();

    assert!(manifest.chunks.is_empty());
    assert_eq!(read_back(&manifest).await.unwrap(), Vec::<u8>::new());
}

#[tokio::test]
async fn refuses_anything_but_six_nodes() {
    let nodes = start_nodes().await;

    let result = upload(&NodeClient::new().unwrap(), &urls(&nodes[..5]), b"photo").await;

    assert!(matches!(
        result,
        Err(mesh_client::UploadError::WrongNodeCount { given: 5 })
    ));
}

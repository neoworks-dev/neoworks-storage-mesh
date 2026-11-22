//! Stores a file on real nodes over HTTP and reads it back, also after nodes
//! go down, lose pieces or return corrupted ones.
//!
//! With exactly six nodes, every chunk has one piece on each of them, so
//! stopping a node takes one piece from every chunk.

mod test_nodes;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use mesh_client::chunking::{AVERAGE_CHUNK_BYTES, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES};
use mesh_client::erasure::RebuildError;
use mesh_client::{DedupKey, DownloadError, Manifest, NodeClient, UploadError, upload};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, download_bytes, node_list, random_file, start_nodes, store_on};
use tokio::io::{AsyncRead, ReadBuf};

/// Long enough that no piece expires while a test runs.
const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// About three chunks of bytes.
fn sample_file() -> Vec<u8> {
    random_file(AVERAGE_CHUNK_BYTES * 3, 1)
}

async fn store(nodes: &[TestNode], file: &[u8]) -> Result<Manifest, UploadError> {
    store_on(&node_list(nodes), file, LEASE).await
}

/// Reads with a new client, so no connection opened during the upload is reused.
async fn read_back(manifest: &Manifest) -> Result<Vec<u8>, DownloadError> {
    download_bytes(&NodeClient::new().unwrap(), manifest).await
}

/// A reader that hands out at most 777 bytes per read, the way a network
/// stream or a pipe does, and then optionally fails.
struct TrickleReader {
    data: Vec<u8>,
    position: usize,
    fail_at_end: bool,
}

impl AsyncRead for TrickleReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.position;
        if remaining == 0 && self.fail_at_end {
            return Poll::Ready(Err(io::Error::other("the disk went away")));
        }
        let count = remaining.min(777).min(buffer.remaining());
        let start = self.position;
        buffer.put_slice(&self.data[start..start + count]);
        self.position += count;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn a_file_comes_back_unchanged() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();

    let manifest = store(&nodes, &file).await.unwrap();

    assert!(manifest.chunks.len() > 1);
    let lengths: Vec<usize> = manifest
        .chunks
        .iter()
        .map(|chunk| chunk.plain_len as usize)
        .collect();
    assert_eq!(lengths.iter().sum::<usize>(), file.len());
    let (last, rest) = lengths.split_last().unwrap();
    assert!(
        rest.iter()
            .all(|&length| (MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&length))
    );
    assert!(*last <= MAX_CHUNK_BYTES);
    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn nodes_only_ever_hold_noise() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = vec![b'A'; AVERAGE_CHUNK_BYTES];

    store(&nodes, &file).await.unwrap();

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
    let manifest = store(&nodes, &file).await.unwrap();

    nodes[0].stop();
    nodes[4].stop();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn survives_one_node_losing_and_one_corrupting_its_pieces() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = store(&nodes, &file).await.unwrap();

    nodes[1].lose_all_pieces();
    nodes[2].corrupt_all_pieces();

    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

#[tokio::test]
async fn fails_when_three_nodes_are_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = store(&nodes, &sample_file()).await.unwrap();

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

    let manifest = store(&nodes, &[]).await.unwrap();

    assert!(manifest.chunks.is_empty());
    assert_eq!(read_back(&manifest).await.unwrap(), Vec::<u8>::new());
}

#[tokio::test]
async fn a_file_read_in_small_pieces_is_cut_like_one_read_at_once() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let dedup_key = DedupKey::generate();
    let client = NodeClient::new().unwrap();
    let list = node_list(&nodes);
    let reader = TrickleReader {
        data: file.clone(),
        position: 0,
        fail_at_end: false,
    };

    let trickled = upload(&client, &list, &dedup_key, reader, LEASE, &[])
        .await
        .unwrap();
    let at_once = upload(&client, &list, &dedup_key, file.as_slice(), LEASE, &[])
        .await
        .unwrap();

    let lengths = |manifest: &Manifest| -> Vec<u32> {
        manifest
            .chunks
            .iter()
            .map(|chunk| chunk.plain_len)
            .collect()
    };
    assert_eq!(lengths(&trickled), lengths(&at_once));
    assert_eq!(read_back(&trickled).await.unwrap(), file);
}

#[tokio::test]
async fn a_failing_reader_fails_the_upload() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let reader = TrickleReader {
        data: sample_file(),
        position: 0,
        fail_at_end: true,
    };

    let result = upload(
        &NodeClient::new().unwrap(),
        &node_list(&nodes),
        &DedupKey::generate(),
        reader,
        LEASE,
        &[],
    )
    .await;

    assert!(matches!(result, Err(UploadError::Read(_))));
}

#[tokio::test]
async fn fails_with_fewer_nodes_than_pieces() {
    let nodes = start_nodes(NodeSettings::default()).await;

    let result = store(&nodes[..5], b"photo").await;

    assert!(matches!(
        result,
        Err(UploadError::NotEnoughNodes {
            chunk: 0,
            placed: 5
        })
    ));
}

#[tokio::test]
async fn nodes_that_are_down_are_passed_over() {
    let mut nodes = start_nodes(NodeSettings::default()).await;
    nodes.push(TestNode::start(NodeSettings::default()).await);
    nodes.push(TestNode::start(NodeSettings::default()).await);
    let file = sample_file();

    nodes[1].stop();
    nodes[6].stop();
    let manifest = store(&nodes, &file).await.unwrap();

    for chunk in &manifest.chunks {
        assert!(
            chunk
                .pieces
                .iter()
                .all(|piece| piece.node != nodes[1].url && piece.node != nodes[6].url)
        );
    }
    assert_eq!(read_back(&manifest).await.unwrap(), file);
}

//! Stores a file on real nodes over HTTP and reads it back, also after nodes
//! go down, lose pieces or return corrupted ones.
//!
//! With exactly six nodes, every chunk has one piece on each of them, so
//! stopping a node takes one piece from every chunk.

mod test_nodes;

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use mesh_client::chunking::{AVERAGE_CHUNK_BYTES, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES};
use mesh_client::erasure::{DATA_PIECES, RebuildError};
use mesh_client::{
    CHUNKS_IN_FLIGHT, DedupKey, DownloadError, Manifest, NodeClient, NodeInfo, NodeList,
    UploadError, upload,
};
use mesh_core::NodeId;
use mesh_node::NodeSettings;
use test_nodes::{
    RequestGauge, TestNode, download_bytes, node_list, random_file, start_nodes, start_silent_node,
    start_slow_proxy, store_on,
};
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

/// Reads from `nodes` with a new client, so no connection opened during the
/// upload is reused.
async fn read_back(nodes: &[TestNode], manifest: &Manifest) -> Result<Vec<u8>, DownloadError> {
    read_back_with(&node_list(nodes), manifest).await
}

/// Reads from the nodes in `list` with a new client.
async fn read_back_with(list: &NodeList, manifest: &Manifest) -> Result<Vec<u8>, DownloadError> {
    download_bytes(&NodeClient::new().unwrap(), list, manifest).await
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
    assert_eq!(read_back(&nodes, &manifest).await.unwrap(), file);
}

#[tokio::test]
async fn nodes_are_found_at_their_new_address_after_they_move() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = store(&nodes, &file).await.unwrap();
    // The node list now has every node at another address, which forwards to
    // the node, as if each had moved there.
    let gauge = Arc::new(RequestGauge::default());
    let mut list = node_list(&nodes);
    for node in &mut list.nodes {
        node.url = start_slow_proxy(node.url.clone(), Duration::ZERO, gauge.clone()).await;
    }

    assert_eq!(read_back_with(&list, &manifest).await.unwrap(), file);
    assert!(gauge.most() > 0);
}

#[tokio::test]
async fn chunks_are_fetched_side_by_side_and_written_in_order() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = store(&nodes, &file).await.unwrap();
    assert!(manifest.chunks.len() > 1);
    // Every node answers only after a delay, so requests for different
    // chunks overlap if the download sends them side by side.
    let gauge = Arc::new(RequestGauge::default());
    let mut list = node_list(&nodes);
    for node in &mut list.nodes {
        let delay = Duration::from_millis(300);
        node.url = start_slow_proxy(node.url.clone(), delay, gauge.clone()).await;
    }

    assert_eq!(read_back_with(&list, &manifest).await.unwrap(), file);

    // One chunk alone never asks more than four nodes at once.
    assert!(
        gauge.most() > DATA_PIECES,
        "at most {} requests at once",
        gauge.most()
    );
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

    assert_eq!(read_back(&nodes, &manifest).await.unwrap(), file);
}

#[tokio::test]
async fn survives_one_node_losing_and_one_corrupting_its_pieces() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = store(&nodes, &file).await.unwrap();

    nodes[1].lose_all_pieces();
    nodes[2].corrupt_all_pieces();

    assert_eq!(read_back(&nodes, &manifest).await.unwrap(), file);
}

#[tokio::test]
async fn nodes_missing_from_the_list_count_as_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let file = sample_file();
    let manifest = store(&nodes, &file).await.unwrap();

    let read_from_four = read_back(&nodes[..4], &manifest).await;
    let read_from_three = read_back(&nodes[..3], &manifest).await;

    assert_eq!(read_from_four.unwrap(), file);
    assert!(matches!(
        read_from_three,
        Err(DownloadError::Rebuild {
            source: RebuildError::TooFewPieces { found: 3, .. },
            ..
        })
    ));
}

#[tokio::test]
async fn a_node_that_never_answers_is_passed_over_and_remembered() {
    let nodes = start_nodes(NodeSettings::default()).await;
    // No chunk is larger than `MAX_CHUNK_BYTES`, so this is more chunks than
    // a download fetches at once.
    let file = random_file(MAX_CHUNK_BYTES * CHUNKS_IN_FLIGHT + 1, 2);
    let mut manifest = store(&nodes, &file).await.unwrap();
    assert!(manifest.chunks.len() > CHUNKS_IN_FLIGHT);
    // Decoding and decrypting take seconds in a debug build, so the time of
    // a read from healthy nodes is the baseline.
    let started = Instant::now();
    read_back(&nodes, &manifest).await.unwrap();
    let healthy = started.elapsed();
    // Piece 0 is a data piece, so an unmeasured client asks for it first.
    let mut list = node_list(&nodes);
    let asked = Arc::new(AtomicUsize::new(0));
    let silent = NodeInfo {
        id: NodeId::of(&[9; 32]),
        url: start_silent_node(asked.clone()).await,
        operator: "silent".into(),
        network: "silent".into(),
        region: None,
        weight: 1.0,
    };
    for chunk in &mut manifest.chunks {
        chunk.pieces[0].node = silent.id;
    }
    list.nodes.push(silent);

    let started = Instant::now();
    let read = read_back_with(&list, &manifest).await.unwrap();
    let with_silent = started.elapsed();

    assert_eq!(read, file);
    // The chunks fetched at once all ask the unmeasured node before any
    // learns it is silent. Once the first of them finishes, the node is
    // recorded as taking the whole request timeout and ranks last, so later
    // chunks ask it only when hedging runs out of other nodes: each chunk
    // has five healthy pieces for four needed, and under load large pieces
    // can take longer than the hedge delay.
    let asked = asked.load(Ordering::SeqCst);
    assert!(
        asked < manifest.chunks.len(),
        "{asked} of {} chunks asked the silent node",
        manifest.chunks.len()
    );
    // Hedging moves on after a second instead of the 30-second request
    // timeout. Rebuilding from a parity piece instead of the silent node's
    // data piece costs a few seconds more in a debug build.
    assert!(
        with_silent < healthy + Duration::from_secs(15),
        "healthy {healthy:?}, with a silent node {with_silent:?}"
    );
}

#[tokio::test]
async fn fails_when_three_nodes_are_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = store(&nodes, &sample_file()).await.unwrap();

    nodes[0].stop();
    nodes[2].stop();
    nodes[5].stop();

    let result = read_back(&nodes, &manifest).await;

    assert!(matches!(
        result,
        Err(DownloadError::Rebuild {
            index: 0,
            source: RebuildError::TooFewPieces {
                found: 3,
                needed: 4
            }
        })
    ));
}

#[tokio::test]
async fn an_empty_file_comes_back_empty() {
    let nodes = start_nodes(NodeSettings::default()).await;

    let manifest = store(&nodes, &[]).await.unwrap();

    assert!(manifest.chunks.is_empty());
    assert_eq!(
        read_back(&nodes, &manifest).await.unwrap(),
        Vec::<u8>::new()
    );
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
    assert_eq!(read_back(&nodes, &trickled).await.unwrap(), file);
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
            placed: 5,
            needed: 6
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
                .all(|piece| piece.node != nodes[1].id && piece.node != nodes[6].id)
        );
    }
    assert_eq!(read_back(&nodes, &manifest).await.unwrap(), file);
}

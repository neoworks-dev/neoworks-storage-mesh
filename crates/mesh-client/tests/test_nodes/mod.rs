//! Nodes running inside the test process, each with its own data directory,
//! and ways to make them fail.

// Every test file compiles this module on its own, and none uses all of it.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::http::{StatusCode, Uri};
use axum::routing::get;
use mesh_client::erasure::TOTAL_PIECES;
use mesh_client::{
    ChunkRecord, DedupKey, DownloadError, Manifest, NodeClient, NodeInfo, NodeList, PlacedPiece,
    UploadError, download, upload,
};
use mesh_core::NodeId;
use mesh_core::signed::SigningKey;
use mesh_node::{NodeData, NodeSettings};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A node running in this process.
pub struct TestNode {
    pub id: NodeId,
    pub key: SigningKey,
    pub url: String,
    data: TempDir,
    server: JoinHandle<std::io::Result<()>>,
}

impl TestNode {
    pub async fn start(settings: NodeSettings) -> TestNode {
        let data = tempfile::tempdir().unwrap();
        let node_data = NodeData::open(data.path()).await.unwrap();
        let id = node_data.identity().node_id;
        let key = node_data.key.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(mesh_node::serve(listener, node_data, settings));
        TestNode {
            id,
            key,
            url,
            data,
            server,
        }
    }

    /// Stops the node: its port refuses new connections.
    pub fn stop(&self) {
        self.server.abort();
    }

    /// The files of every piece the node holds.
    pub fn piece_files(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.data.path().join("pieces"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    /// Deletes every piece file the node holds, as if its disk had been wiped.
    pub fn lose_all_pieces(&self) {
        for path in self.piece_files() {
            std::fs::remove_file(path).unwrap();
        }
    }

    /// Overwrites every piece the node holds with other bytes of the same length.
    pub fn corrupt_all_pieces(&self) {
        for path in self.piece_files() {
            let length = std::fs::metadata(&path).unwrap().len() as usize;
            std::fs::write(&path, vec![0xAB; length]).unwrap();
        }
    }
}

/// The URL of a node that accepts connections and never answers, like one
/// behind a congested link. It runs until the test ends.
pub async fn start_silent_node() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        // Holding the sockets keeps the connections open without a reply.
        let mut connections = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            connections.push(socket);
        }
    });
    url
}

/// Counts the requests a group of slow proxies holds at once.
#[derive(Default)]
pub struct RequestGauge {
    now: AtomicUsize,
    most: AtomicUsize,
}

impl RequestGauge {
    /// The most requests that were held at the same moment.
    pub fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }
}

/// The URL of a proxy in front of the node at `target` that holds every GET
/// for `delay` before forwarding it, counting held requests in `gauge`.
pub async fn start_slow_proxy(target: String, delay: Duration, gauge: Arc<RequestGauge>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let forward = move |uri: Uri| {
        let target = target.clone();
        let gauge = gauge.clone();
        async move {
            let held = gauge.now.fetch_add(1, Ordering::SeqCst) + 1;
            gauge.most.fetch_max(held, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
            gauge.now.fetch_sub(1, Ordering::SeqCst);
            let response = reqwest::get(format!("{target}{uri}")).await.unwrap();
            let status = StatusCode::from_u16(response.status().as_u16()).unwrap();
            (status, response.bytes().await.unwrap())
        }
    };
    let app = Router::new().fallback(get(forward));
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

/// As many nodes as a chunk has pieces, all with `settings`.
pub async fn start_nodes(settings: NodeSettings) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..TOTAL_PIECES {
        nodes.push(TestNode::start(settings.clone()).await);
    }
    nodes
}

pub fn urls(nodes: &[TestNode]) -> Vec<String> {
    nodes.iter().map(|node| node.url.clone()).collect()
}

/// `length` bytes that look random but are the same for the same `seed`, so
/// chunk boundaries fall where they would in a real file and tests repeat.
pub fn random_file(length: usize, seed: u64) -> Vec<u8> {
    // xorshift64*: small, fast and plenty random for chunking.
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..length)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8
        })
        .collect()
}

/// Stores `file` on nodes from `list` with a fresh dedup key and nothing
/// earlier to reuse.
pub async fn store_on(
    list: &NodeList,
    file: &[u8],
    lease: Duration,
) -> Result<Manifest, UploadError> {
    let client = NodeClient::new().unwrap();
    upload(&client, list, &DedupKey::generate(), file, lease, &[]).await
}

/// Downloads the file `manifest` describes into memory.
pub async fn download_bytes(
    client: &NodeClient,
    manifest: &Manifest,
) -> Result<Vec<u8>, DownloadError> {
    let mut file = Vec::new();
    download(client, manifest, &mut file).await?;
    Ok(file)
}

/// The piece of `chunk` that lives on the node at `url`.
pub fn piece_on<'chunk>(chunk: &'chunk ChunkRecord, url: &str) -> &'chunk PlacedPiece {
    chunk
        .pieces
        .iter()
        .find(|piece| piece.node == url)
        .expect("every test node holds one piece of every chunk")
}

/// Where in `chunk` the piece on the node at `url` sits.
pub fn position_on(chunk: &ChunkRecord, url: &str) -> usize {
    chunk
        .pieces
        .iter()
        .position(|piece| piece.node == url)
        .expect("every test node holds one piece of every chunk")
}

/// A node list of `nodes` in which every node has its own operator and
/// network, so placement may use any of them for any chunk.
pub fn node_list<'a>(nodes: impl IntoIterator<Item = &'a TestNode>) -> NodeList {
    let nodes = nodes
        .into_iter()
        .enumerate()
        .map(|(index, node)| NodeInfo {
            id: node.id,
            url: node.url.clone(),
            operator: format!("operator-{index}"),
            network: format!("network-{index}"),
            weight: 1.0,
        })
        .collect();
    NodeList { nodes }
}

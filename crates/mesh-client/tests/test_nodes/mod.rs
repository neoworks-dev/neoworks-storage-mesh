//! Nodes running inside the test process, each with its own data directory,
//! and ways to make them fail.

// Every test file compiles this module on its own, and none uses all of it.
#![allow(dead_code)]

use std::path::PathBuf;

use mesh_client::erasure::TOTAL_PIECES;
use mesh_client::{
    ChunkRecord, DownloadError, Manifest, NodeClient, NodeInfo, NodeList, PlacedPiece, download,
};
use mesh_node::{NodeSettings, PieceStore};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A node running in this process.
pub struct TestNode {
    pub url: String,
    data: TempDir,
    server: JoinHandle<std::io::Result<()>>,
}

impl TestNode {
    pub async fn start(settings: NodeSettings) -> TestNode {
        let data = tempfile::tempdir().unwrap();
        let store = PieceStore::open(data.path()).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(mesh_node::serve(listener, store, settings));
        TestNode { url, data, server }
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
            url: node.url.clone(),
            operator: format!("operator-{index}"),
            network: format!("network-{index}"),
            weight: 1.0,
        })
        .collect();
    NodeList { nodes }
}

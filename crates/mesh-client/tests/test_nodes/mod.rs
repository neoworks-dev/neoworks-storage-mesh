//! Nodes running inside the test process, each with its own data directory,
//! and ways to make them fail.

// Every test file compiles this module on its own, and none uses all of it.
#![allow(dead_code)]

use std::path::PathBuf;

use mesh_client::erasure::TOTAL_PIECES;
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

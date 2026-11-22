//! Audits over real nodes: each way a node can fail shows up as its own kind
//! of unhealthy piece, and a node that never answers is cut off at the
//! deadline.

mod test_nodes;

use std::time::{Duration, Instant};

use mesh_client::erasure::TOTAL_PIECES;
use mesh_client::{AUDIT_DEADLINE, AuditReport, Manifest, NodeClient, PieceHealth, audit};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, node_list, position_on, random_file, start_nodes, store_on};
use tokio::net::TcpListener;

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// About three chunks of bytes on six nodes.
async fn stored_file() -> (Vec<TestNode>, Manifest) {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = store_on(&node_list(&nodes), &random_file(3_000_000, 4), LEASE)
        .await
        .unwrap();
    (nodes, manifest)
}

/// The health of the piece on the node at `url`, in every chunk.
fn health_on(report: &AuditReport, manifest: &Manifest, url: &str) -> Vec<PieceHealth> {
    report
        .chunks
        .iter()
        .zip(&manifest.chunks)
        .map(|(chunk_audit, chunk)| chunk_audit.pieces[position_on(chunk, url)])
        .collect()
}

#[tokio::test]
async fn a_well_kept_file_is_healthy_everywhere() {
    let (_nodes, manifest) = stored_file().await;

    let report = audit(&NodeClient::new().unwrap(), &manifest, AUDIT_DEADLINE).await;

    assert_eq!(report.chunks.len(), manifest.chunks.len());
    assert!(
        report
            .chunks
            .iter()
            .all(|chunk| chunk.healthy() == TOTAL_PIECES)
    );
}

#[tokio::test]
async fn each_kind_of_failure_is_told_apart() {
    let (nodes, manifest) = stored_file().await;
    let chunks = manifest.chunks.len();

    nodes[0].stop();
    nodes[1].lose_all_pieces();
    nodes[2].corrupt_all_pieces();
    let report = audit(&NodeClient::new().unwrap(), &manifest, AUDIT_DEADLINE).await;

    let health = |node: &TestNode| health_on(&report, &manifest, &node.url);
    assert_eq!(health(&nodes[0]), vec![PieceHealth::Unreachable; chunks]);
    assert_eq!(health(&nodes[1]), vec![PieceHealth::Missing; chunks]);
    assert_eq!(health(&nodes[2]), vec![PieceHealth::Failed; chunks]);
    for node in &nodes[3..] {
        assert_eq!(health(node), vec![PieceHealth::Healthy; chunks]);
    }
    assert!(report.chunks.iter().all(|chunk| chunk.healthy() == 3));
}

#[tokio::test]
async fn a_node_that_never_answers_is_cut_off_at_the_deadline() {
    let (_nodes, mut manifest) = stored_file().await;
    let chunks = manifest.chunks.len();
    // A node that accepts connections and then says nothing, like one busy
    // fetching the piece from somewhere else.
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_url = format!("http://{}", silent.local_addr().unwrap());
    let _silent_node = tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = silent.accept().await {
            connections.push(connection);
        }
    });
    for chunk in &mut manifest.chunks {
        chunk.pieces[5].node = silent_url.clone();
    }

    let deadline = Duration::from_millis(300);
    let started = Instant::now();
    let report = audit(&NodeClient::new().unwrap(), &manifest, deadline).await;

    assert_eq!(
        health_on(&report, &manifest, &silent_url),
        vec![PieceHealth::Unreachable; chunks]
    );
    // Chunks are audited one after the other, each waiting for the deadline
    // once; anything near the 30 second request timeout would be a failure.
    let allowance = deadline * chunks as u32 + Duration::from_secs(2);
    assert!(started.elapsed() < allowance);
}

//! Repair over real nodes: lost pieces are rebuilt onto spare nodes, the file
//! survives fresh losses afterwards, and repair says so when it can't help.

mod test_nodes;

use std::time::Duration;

use mesh_client::{
    AUDIT_DEADLINE, ChunkRepair, Manifest, NodeClient, PieceHealth, audit, download, repair, upload,
};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, start_nodes, urls};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// Three chunks' worth of bytes.
fn sample_file() -> Vec<u8> {
    (0..3_000_000u32)
        .map(|position| (position % 251) as u8)
        .collect()
}

async fn stored_file() -> (Vec<TestNode>, Manifest) {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = upload(
        &NodeClient::new().unwrap(),
        &urls(&nodes),
        &sample_file(),
        LEASE,
    )
    .await
    .unwrap();
    (nodes, manifest)
}

async fn start_spares(count: usize) -> Vec<TestNode> {
    let mut spares = Vec::new();
    for _ in 0..count {
        spares.push(TestNode::start(NodeSettings::default()).await);
    }
    spares
}

/// Audits and repairs `manifest` with a fresh client, as a later run would.
async fn audit_and_repair(manifest: &mut Manifest, spare_nodes: &[String]) -> Vec<ChunkRepair> {
    let client = NodeClient::new().unwrap();
    let report = audit(&client, manifest, AUDIT_DEADLINE).await;
    repair(&client, manifest, &report, spare_nodes, LEASE)
        .await
        .chunks
}

#[tokio::test]
async fn an_intact_file_is_left_alone() {
    let (_nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(
        outcome,
        vec![
            ChunkRepair::Intact,
            ChunkRepair::Intact,
            ChunkRepair::Intact
        ]
    );
    assert!(spares[0].piece_files().is_empty());
}

#[tokio::test]
async fn lost_pieces_are_rebuilt_on_spare_nodes() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(2).await;

    nodes[1].stop();
    nodes[4].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(outcome, vec![ChunkRepair::Repaired { replaced: 2 }; 3]);
    for chunk in &manifest.chunks {
        assert_eq!(chunk.pieces[1].node, spares[0].url);
        assert_eq!(chunk.pieces[4].node, spares[1].url);
    }
    let report = audit(&NodeClient::new().unwrap(), &manifest, AUDIT_DEADLINE).await;
    assert!(report.chunks.iter().all(|chunk| chunk.healthy() == 6));
}

#[tokio::test]
async fn a_repaired_file_survives_two_more_losses() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(2).await;
    nodes[0].stop();
    nodes[5].stop();
    audit_and_repair(&mut manifest, &urls(&spares)).await;

    // Without repair, losing two more would leave only two pieces per chunk.
    nodes[2].stop();
    nodes[3].stop();

    let file = download(&NodeClient::new().unwrap(), &manifest).await;
    assert_eq!(file.unwrap(), sample_file());
}

#[tokio::test]
async fn one_loss_is_enough_to_repair() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    nodes[3].corrupt_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(outcome, vec![ChunkRepair::Repaired { replaced: 1 }; 3]);
}

#[tokio::test]
async fn a_spare_node_that_is_down_is_skipped() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(2).await;
    spares[0].stop();

    nodes[2].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(outcome, vec![ChunkRepair::Repaired { replaced: 1 }; 3]);
    assert!(
        manifest
            .chunks
            .iter()
            .all(|chunk| chunk.pieces[2].node == spares[1].url)
    );
}

#[tokio::test]
async fn too_few_spare_nodes_leave_pieces_unplaced() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    nodes[0].lose_all_pieces();
    nodes[1].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(
        outcome,
        vec![
            ChunkRepair::Partly {
                replaced: 1,
                unplaced: 1
            };
            3
        ]
    );
}

#[tokio::test]
async fn a_chunk_with_three_losses_is_reported_lost() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(3).await;
    let nodes_before: Vec<String> = manifest.chunks[0]
        .pieces
        .iter()
        .map(|piece| piece.node.clone())
        .collect();

    nodes[0].stop();
    nodes[1].stop();
    nodes[2].stop();
    let outcome = audit_and_repair(&mut manifest, &urls(&spares)).await;

    assert_eq!(outcome, vec![ChunkRepair::Lost { fetched: 3 }; 3]);
    let nodes_after: Vec<String> = manifest.chunks[0]
        .pieces
        .iter()
        .map(|piece| piece.node.clone())
        .collect();
    assert_eq!(nodes_after, nodes_before);
    let report = audit(&NodeClient::new().unwrap(), &manifest, AUDIT_DEADLINE).await;
    assert_eq!(report.chunks[0].pieces[0], PieceHealth::Unreachable);
}

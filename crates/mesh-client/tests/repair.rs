//! Repair over real nodes: lost pieces are rebuilt onto other nodes from the
//! list, the file survives fresh losses afterwards, and repair says so when it
//! can't help.

mod test_nodes;

use std::time::Duration;

use mesh_client::{
    AUDIT_DEADLINE, ChunkRepair, Manifest, NodeClient, NodeList, PieceHealth, audit, repair,
};
use mesh_node::NodeSettings;
use test_nodes::{
    TestNode, download_bytes, node_list, position_on, random_file, start_nodes, store_on,
};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// About three chunks of bytes.
fn sample_file() -> Vec<u8> {
    random_file(3_000_000, 5)
}

/// A file on six nodes, every chunk with one piece on each.
async fn stored_file() -> (Vec<TestNode>, Manifest) {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = store_on(&node_list(&nodes), &sample_file(), LEASE)
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

/// The original nodes and the spares, as one list to repair from.
fn all_nodes(nodes: &[TestNode], spares: &[TestNode]) -> NodeList {
    node_list(nodes.iter().chain(spares))
}

/// Audits and repairs `manifest` with a fresh client, as a later run would.
async fn audit_and_repair(manifest: &mut Manifest, list: &NodeList) -> Vec<ChunkRepair> {
    let client = NodeClient::new().unwrap();
    let report = audit(&client, manifest, AUDIT_DEADLINE).await;
    repair(&client, manifest, &report, list, LEASE).await.chunks
}

#[tokio::test]
async fn an_intact_file_is_left_alone() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    assert_eq!(outcome, vec![ChunkRepair::Intact; manifest.chunks.len()]);
    assert!(spares[0].piece_files().is_empty());
}

#[tokio::test]
async fn lost_pieces_are_rebuilt_on_spare_nodes() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(2).await;
    let lost_positions: Vec<[usize; 2]> = manifest
        .chunks
        .iter()
        .map(|chunk| {
            [
                position_on(chunk, &nodes[1].url),
                position_on(chunk, &nodes[4].url),
            ]
        })
        .collect();

    nodes[1].stop();
    nodes[4].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    assert_eq!(
        outcome,
        vec![ChunkRepair::Repaired { replaced: 2 }; manifest.chunks.len()]
    );
    // Every other node already holds a piece of each chunk, so the two lost
    // pieces can only have gone to the two spares, one each.
    for (chunk, [first, second]) in manifest.chunks.iter().zip(lost_positions) {
        let mut new_nodes = [
            chunk.pieces[first].node.clone(),
            chunk.pieces[second].node.clone(),
        ];
        new_nodes.sort();
        let mut spare_urls = [spares[0].url.clone(), spares[1].url.clone()];
        spare_urls.sort();
        assert_eq!(new_nodes, spare_urls);
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
    audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    // Without repair, losing two more would leave only two pieces per chunk.
    nodes[2].stop();
    nodes[3].stop();

    let file = download_bytes(&NodeClient::new().unwrap(), &manifest).await;
    assert_eq!(file.unwrap(), sample_file());
}

#[tokio::test]
async fn one_loss_is_enough_to_repair() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    nodes[3].corrupt_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    assert_eq!(
        outcome,
        vec![ChunkRepair::Repaired { replaced: 1 }; manifest.chunks.len()]
    );
}

#[tokio::test]
async fn a_spare_node_that_is_down_is_passed_over() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(2).await;
    let lost_positions: Vec<usize> = manifest
        .chunks
        .iter()
        .map(|chunk| position_on(chunk, &nodes[2].url))
        .collect();
    spares[0].stop();

    nodes[2].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    assert_eq!(
        outcome,
        vec![ChunkRepair::Repaired { replaced: 1 }; manifest.chunks.len()]
    );
    for (chunk, position) in manifest.chunks.iter().zip(lost_positions) {
        assert_eq!(chunk.pieces[position].node, spares[1].url);
    }
}

#[tokio::test]
async fn too_few_spare_nodes_leave_pieces_unplaced() {
    let (nodes, mut manifest) = stored_file().await;
    let spares = start_spares(1).await;

    nodes[0].lose_all_pieces();
    nodes[1].lose_all_pieces();
    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    let partly = ChunkRepair::Partly {
        replaced: 1,
        unplaced: 1,
    };
    assert_eq!(outcome, vec![partly; manifest.chunks.len()]);
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
    let outcome = audit_and_repair(&mut manifest, &all_nodes(&nodes, &spares)).await;

    assert_eq!(
        outcome,
        vec![ChunkRepair::Lost { fetched: 3 }; manifest.chunks.len()]
    );
    let nodes_after: Vec<String> = manifest.chunks[0]
        .pieces
        .iter()
        .map(|piece| piece.node.clone())
        .collect();
    assert_eq!(nodes_after, nodes_before);
    let report = audit(&NodeClient::new().unwrap(), &manifest, AUDIT_DEADLINE).await;
    let position = position_on(&manifest.chunks[0], &nodes[0].url);
    assert_eq!(report.chunks[0].pieces[position], PieceHealth::Unreachable);
}

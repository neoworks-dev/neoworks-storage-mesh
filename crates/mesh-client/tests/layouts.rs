//! Files stored in layouts other than 4 + 2 over real nodes: they come back,
//! survive as many lost nodes as they have parity pieces, keep to the
//! per-region limit, and repair in their own layout.

mod test_nodes;

use std::time::Duration;

use mesh_client::layout_choice::{Worth, choose_layout};
use mesh_client::placement::Spread;
use mesh_client::{
    AUDIT_DEADLINE, ChunkRepair, DedupKey, Manifest, NodeClient, NodeList, Stored, audit, repair,
    upload_spread,
};
use mesh_core::layout::Layout;
use mesh_node::NodeSettings;
use test_nodes::{TestNode, download_bytes, node_list, position_on, random_file};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

async fn start_nodes(count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

async fn store_spread(list: &NodeList, file: &[u8], spread: Spread) -> Manifest {
    let client = NodeClient::new().unwrap();
    let stored = Stored {
        client: &client,
        list,
        lease: LEASE,
        spread,
    };
    upload_spread(&stored, &DedupKey::generate(), file, &[])
        .await
        .unwrap()
}

fn spread_of(data: usize, parity: usize) -> Spread {
    Spread {
        layout: Layout::new(data, parity).unwrap(),
        regions: 1,
    }
}

#[tokio::test]
async fn a_file_in_two_plus_three_survives_three_lost_nodes() {
    let nodes = start_nodes(5).await;
    let list = node_list(&nodes);
    let file = random_file(2_500_000, 11);

    let manifest = store_spread(&list, &file, spread_of(2, 3)).await;
    for node in &nodes[..3] {
        node.stop();
    }

    assert!(
        manifest
            .chunks
            .iter()
            .all(|chunk| chunk.pieces.len() == 5 && chunk.layout == Layout::new(2, 3).unwrap())
    );
    let client = NodeClient::new().unwrap();
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn a_wide_layout_spreads_over_many_nodes_and_comes_back() {
    let nodes = start_nodes(20).await;
    let list = node_list(&nodes);
    let file = random_file(1_500_000, 12);

    let manifest = store_spread(&list, &file, spread_of(16, 4)).await;
    for node in &nodes[..4] {
        node.stop();
    }

    let client = NodeClient::new().unwrap();
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn no_region_holds_more_pieces_than_a_chunk_can_lose() {
    let nodes = start_nodes(9).await;
    let mut list = node_list(&nodes);
    for (index, node) in list.nodes.iter_mut().enumerate() {
        node.region = Some(format!("region-{}", index % 3));
    }
    let file = random_file(500_000, 13);

    let manifest = store_spread(&list, &file, Spread::DEFAULT).await;

    for chunk in &manifest.chunks {
        for region in 0..3 {
            let name = Some(format!("region-{region}"));
            let in_region = chunk
                .pieces
                .iter()
                .filter(|piece| {
                    let node = list.nodes.iter().find(|node| node.id == piece.node);
                    node.unwrap().region == name
                })
                .count();
            assert!(in_region <= 2);
        }
    }
}

#[tokio::test]
async fn repair_rebuilds_pieces_in_the_chunks_own_layout() {
    let nodes = start_nodes(6).await;
    let list = node_list(&nodes);
    let file = random_file(1_500_000, 14);
    let mut manifest = store_spread(&list, &file, spread_of(3, 2)).await;
    let lost: Vec<usize> = manifest
        .chunks
        .iter()
        .map(|chunk| position_on(chunk, &nodes[0].id))
        .collect();

    nodes[0].lose_all_pieces();
    let client = NodeClient::new().unwrap();
    let report = audit(&client, &list, &manifest, AUDIT_DEADLINE).await;
    let outcome = repair(&client, &mut manifest, &report, &list, LEASE).await;

    assert!(
        outcome
            .chunks
            .iter()
            .all(|chunk| *chunk == ChunkRepair::Repaired { replaced: 1 })
    );
    // Five pieces on six nodes: the lost piece goes to the one node left.
    for (chunk, position) in manifest.chunks.iter().zip(lost) {
        assert_ne!(chunk.pieces[position].node, nodes[0].id);
    }
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn the_chosen_layout_stores_and_reads_back() {
    let nodes = start_nodes(10).await;
    let list = node_list(&nodes);
    let worth = Worth {
        max_loss_per_year: 1e-6,
        max_data_pieces: 8,
        min_regions: 1,
    };
    let choice = choose_layout(&worth, &list, Some(1_000)).unwrap();
    let file = random_file(800_000, 15);

    let manifest = store_spread(&list, &file, choice.spread).await;

    assert!(choice.spread.layout.total() <= 10);
    let client = NodeClient::new().unwrap();
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

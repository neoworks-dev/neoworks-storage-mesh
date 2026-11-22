//! Storage challenges over real nodes: an honest node proves it holds a piece
//! with a few segments, and a node that corrupted or lost it cannot.

mod test_nodes;

use std::time::Duration;

use mesh_client::{Manifest, NodeClient, NodeRequestError, PlacedPiece, upload};
use mesh_node::NodeSettings;
use reqwest::StatusCode;
use test_nodes::{TestNode, node_list, piece_on, start_nodes};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// Three chunks' worth of bytes, so every piece has dozens of segments.
fn sample_file() -> Vec<u8> {
    (0..3_000_000u32)
        .map(|position| (position % 251) as u8)
        .collect()
}

async fn stored_file() -> (Vec<TestNode>, Manifest) {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = upload(
        &NodeClient::new().unwrap(),
        &node_list(&nodes),
        sample_file().as_slice(),
        LEASE,
    )
    .await
    .unwrap();
    (nodes, manifest)
}

/// How many segments `piece` has, as its node reports it.
async fn segment_count_of(piece: &PlacedPiece) -> u32 {
    let proofs = NodeClient::new()
        .unwrap()
        .challenge(&piece.node, &piece.id, &[0])
        .await
        .unwrap()
        .unwrap();
    proofs[0].segment_count
}

#[tokio::test]
async fn an_honest_node_proves_it_holds_its_pieces() {
    let (_nodes, manifest) = stored_file().await;
    let piece = &manifest.chunks[1].pieces[4];
    let last = segment_count_of(piece).await - 1;

    let proofs = NodeClient::new()
        .unwrap()
        .challenge(&piece.node, &piece.id, &[last, 0, last / 2])
        .await
        .unwrap()
        .unwrap();

    let indexes: Vec<u32> = proofs.iter().map(|proof| proof.index).collect();
    assert_eq!(indexes, vec![last, 0, last / 2]);
    assert!(
        proofs
            .iter()
            .all(|proof| proof.leads_to(&piece.merkle_root))
    );
}

#[tokio::test]
async fn a_node_with_corrupted_pieces_cannot_prove_them() {
    let (nodes, manifest) = stored_file().await;
    let piece = piece_on(&manifest.chunks[0], &nodes[2].url);

    nodes[2].corrupt_all_pieces();
    let proofs = NodeClient::new()
        .unwrap()
        .challenge(&piece.node, &piece.id, &[0, 1, 2])
        .await
        .unwrap()
        .unwrap();

    assert!(
        proofs
            .iter()
            .all(|proof| !proof.leads_to(&piece.merkle_root))
    );
}

#[tokio::test]
async fn a_node_that_lost_a_piece_says_so() {
    let (nodes, manifest) = stored_file().await;
    let piece = piece_on(&manifest.chunks[0], &nodes[3].url);

    nodes[3].lose_all_pieces();
    let answer = NodeClient::new()
        .unwrap()
        .challenge(&piece.node, &piece.id, &[0])
        .await
        .unwrap();

    assert_eq!(answer, None);
}

#[tokio::test]
async fn nodes_refuse_challenges_beyond_the_piece_or_too_large() {
    let (_nodes, manifest) = stored_file().await;
    let piece = &manifest.chunks[0].pieces[0];
    let client = NodeClient::new().unwrap();
    let count = segment_count_of(piece).await;
    let too_many: Vec<u32> = (0..65).collect();

    for segments in [vec![count], vec![], too_many] {
        let result = client.challenge(&piece.node, &piece.id, &segments).await;
        assert!(
            matches!(
                result,
                Err(NodeRequestError::Refused {
                    status: StatusCode::BAD_REQUEST,
                    ..
                })
            ),
            "segments {segments:?}"
        );
    }
}

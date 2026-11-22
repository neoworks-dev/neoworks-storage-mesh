//! Storage challenges over real nodes: an honest node proves it holds a piece
//! with a few segments, and a node that corrupted or lost it cannot.

mod test_nodes;

use std::time::Duration;

use mesh_client::{Manifest, NodeClient, NodeRequestError, PlacedPiece};
use mesh_node::NodeSettings;
use reqwest::StatusCode;
use test_nodes::{TestNode, node_list, piece_on, random_file, start_nodes, store_on, url_of};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// About three chunks of bytes, so every piece has dozens of segments.
async fn stored_file() -> (Vec<TestNode>, Manifest) {
    let nodes = start_nodes(NodeSettings::default()).await;
    let manifest = store_on(&node_list(&nodes), &random_file(3_000_000, 3), LEASE)
        .await
        .unwrap();
    (nodes, manifest)
}

/// How many segments `piece` has, as its node at `url` reports it.
async fn segment_count_of(url: &str, piece: &PlacedPiece) -> u32 {
    let proofs = NodeClient::new()
        .unwrap()
        .challenge(url, &piece.id, &[0])
        .await
        .unwrap()
        .unwrap();
    proofs[0].segment_count
}

#[tokio::test]
async fn an_honest_node_proves_it_holds_its_pieces() {
    let (nodes, manifest) = stored_file().await;
    let piece = &manifest.chunks[0].pieces[4];
    let url = url_of(&nodes, &piece.node);
    let last = segment_count_of(url, piece).await - 1;

    let proofs = NodeClient::new()
        .unwrap()
        .challenge(url, &piece.id, &[last, 0, last / 2])
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
    let piece = piece_on(&manifest.chunks[0], &nodes[2].id);

    nodes[2].corrupt_all_pieces();
    let proofs = NodeClient::new()
        .unwrap()
        .challenge(&nodes[2].url, &piece.id, &[0, 1, 2])
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
    let piece = piece_on(&manifest.chunks[0], &nodes[3].id);

    nodes[3].lose_all_pieces();
    let answer = NodeClient::new()
        .unwrap()
        .challenge(&nodes[3].url, &piece.id, &[0])
        .await
        .unwrap();

    assert_eq!(answer, None);
}

#[tokio::test]
async fn nodes_refuse_challenges_beyond_the_piece_or_too_large() {
    let (nodes, manifest) = stored_file().await;
    let piece = &manifest.chunks[0].pieces[0];
    let url = url_of(&nodes, &piece.node);
    let client = NodeClient::new().unwrap();
    let count = segment_count_of(url, piece).await;
    let too_many: Vec<u32> = (0..65).collect();

    for segments in [vec![count], vec![], too_many] {
        let result = client.challenge(url, &piece.id, &segments).await;
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

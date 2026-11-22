//! Deduplication over real nodes: an owner's chunks are stored once however
//! many files hold them, an edited file reuses what didn't change, and
//! different owners never share pieces.

mod test_nodes;

use std::collections::HashSet;
use std::time::Duration;

use mesh_client::chunking::AVERAGE_CHUNK_BYTES;
use mesh_client::{DedupKey, Manifest, NodeClient, NodeList, upload};
use mesh_core::PieceId;
use mesh_node::NodeSettings;
use test_nodes::{TestNode, download_bytes, node_list, random_file, start_nodes};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

async fn store(
    list: &NodeList,
    dedup_key: &DedupKey,
    file: &[u8],
    earlier: &[Manifest],
) -> Manifest {
    let client = NodeClient::new().unwrap();
    upload(&client, list, dedup_key, file, LEASE, earlier)
        .await
        .unwrap()
}

fn piece_ids(manifest: &Manifest) -> HashSet<PieceId> {
    manifest
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.pieces)
        .map(|piece| piece.id)
        .collect()
}

fn files_on(nodes: &[TestNode]) -> usize {
    nodes.iter().map(|node| node.piece_files().len()).sum()
}

#[tokio::test]
async fn storing_a_file_again_stores_nothing_new() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let dedup_key = DedupKey::generate();
    let file = random_file(AVERAGE_CHUNK_BYTES * 3, 10);
    let first = store(&list, &dedup_key, &file, &[]).await;
    let files_after_first = files_on(&nodes);

    let second = store(&list, &dedup_key, &file, std::slice::from_ref(&first)).await;

    assert_eq!(piece_ids(&second), piece_ids(&first));
    assert_eq!(files_on(&nodes), files_after_first);
}

#[tokio::test]
async fn an_edited_file_reuses_the_chunks_it_kept() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let dedup_key = DedupKey::generate();
    let original = random_file(AVERAGE_CHUNK_BYTES * 6, 11);
    // A few bytes inserted near the start: with fixed-size chunks every chunk
    // after it would shift and change.
    let mut edited = original[..1000].to_vec();
    edited.extend_from_slice(b"a short insertion");
    edited.extend_from_slice(&original[1000..]);
    let first = store(&list, &dedup_key, &original, &[]).await;

    let second = store(&list, &dedup_key, &edited, std::slice::from_ref(&first)).await;

    let kept = second
        .chunks
        .iter()
        .filter(|chunk| first.chunks.iter().any(|old| old.key == chunk.key))
        .count();
    assert!(
        kept + 2 >= second.chunks.len(),
        "kept {kept} of {}",
        second.chunks.len()
    );
    let client = NodeClient::new().unwrap();
    assert_eq!(download_bytes(&client, &second).await.unwrap(), edited);
}

#[tokio::test]
async fn two_owners_never_share_pieces() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let file = random_file(AVERAGE_CHUNK_BYTES * 2, 12);

    let alice = store(&list, &DedupKey::generate(), &file, &[]).await;
    let bob = store(&list, &DedupKey::generate(), &file, &[]).await;

    assert!(piece_ids(&alice).is_disjoint(&piece_ids(&bob)));
}

#[tokio::test]
async fn a_reused_chunk_that_lost_a_piece_is_stored_again() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let dedup_key = DedupKey::generate();
    let file = random_file(AVERAGE_CHUNK_BYTES * 2, 13);
    let first = store(&list, &dedup_key, &file, &[]).await;

    nodes[2].lose_all_pieces();
    let second = store(&list, &dedup_key, &file, std::slice::from_ref(&first)).await;

    // The pieces are the same bytes, so they keep their ids; node 2 got them
    // back because every chunk needs six nodes and there are only six.
    assert_eq!(piece_ids(&second), piece_ids(&first));
    assert!(!nodes[2].piece_files().is_empty());
    nodes[3].stop();
    nodes[4].stop();
    let client = NodeClient::new().unwrap();
    assert_eq!(download_bytes(&client, &second).await.unwrap(), file);
}

#[tokio::test]
async fn a_small_file_is_padded_to_its_bucket() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);

    store(&list, &DedupKey::generate(), &random_file(10_000, 14), &[]).await;

    // 10,000 bytes pad to the 16 KiB bucket; sealing adds a 16-byte tag, and
    // each of the four data pieces holds a quarter.
    let expected_piece_len = (16 * 1024 + 16) / 4;
    for node in &nodes {
        for path in node.piece_files() {
            assert_eq!(std::fs::metadata(path).unwrap().len(), expected_piece_len);
        }
    }
}

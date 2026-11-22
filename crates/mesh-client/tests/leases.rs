//! Leases over real nodes: a file that is renewed stays, one that is not
//! expires, and nodes refuse leases longer than they grant.
//!
//! These tests wait for leases to run out on the real clock. Lease ends are
//! whole seconds, so a lease of `n` seconds lasts between `n - 1` and `n`
//! seconds; the waits below allow for that.

mod test_nodes;

use std::time::Duration;

use mesh_client::erasure::{RebuildError, TOTAL_PIECES};
use mesh_client::{DownloadError, NodeClient, UploadError, renew};
use mesh_node::NodeSettings;
use test_nodes::{download_bytes, node_list, random_file, start_nodes, store_on};

/// Nodes that sweep often, so expired pieces leave the disk within a test.
fn quickly_sweeping() -> NodeSettings {
    NodeSettings {
        sweep_interval: Duration::from_millis(100),
        ..NodeSettings::default()
    }
}

/// About three chunks of bytes.
fn sample_file() -> Vec<u8> {
    random_file(3_000_000, 2)
}

#[tokio::test]
async fn an_unrenewed_file_expires_and_leaves_the_disk() {
    let nodes = start_nodes(quickly_sweeping()).await;
    let list = node_list(&nodes);
    let manifest = store_on(&list, &sample_file(), Duration::from_secs(1))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let result = download_bytes(&NodeClient::new().unwrap(), &list, &manifest).await;
    assert!(matches!(
        result,
        Err(DownloadError::Rebuild {
            source: RebuildError::TooFewPieces { found: 0, .. },
            ..
        })
    ));
    for node in &nodes {
        assert!(node.piece_files().is_empty());
    }
}

#[tokio::test]
async fn a_renewed_file_outlives_its_first_lease() {
    let nodes = start_nodes(quickly_sweeping()).await;
    let client = NodeClient::new().unwrap();
    let file = sample_file();
    let list = node_list(&nodes);
    let manifest = store_on(&list, &file, Duration::from_secs(2))
        .await
        .unwrap();

    let report = renew(&client, &list, &manifest, Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(3_200)).await;

    assert_eq!(report.renewed, manifest.chunks.len() * TOTAL_PIECES);
    assert!(report.not_renewed.is_empty());
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn renewing_reports_the_pieces_of_a_node_that_is_down() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let manifest = store_on(&list, &sample_file(), Duration::from_secs(60))
        .await
        .unwrap();
    let chunks = manifest.chunks.len();

    nodes[3].stop();
    let report = renew(
        &NodeClient::new().unwrap(),
        &list,
        &manifest,
        Duration::from_secs(120),
    )
    .await;

    assert_eq!(report.renewed, chunks * (TOTAL_PIECES - 1));
    assert_eq!(report.not_renewed.len(), chunks);
    assert!(
        report
            .not_renewed
            .iter()
            .all(|piece| piece.node == nodes[3].id)
    );
}

#[tokio::test]
async fn renewing_skips_the_pieces_of_a_node_missing_from_the_list() {
    let nodes = start_nodes(NodeSettings::default()).await;
    let mut list = node_list(&nodes);
    let manifest = store_on(&list, &sample_file(), Duration::from_secs(60))
        .await
        .unwrap();
    let chunks = manifest.chunks.len();

    list.nodes.retain(|node| node.id != nodes[2].id);
    let report = renew(
        &NodeClient::new().unwrap(),
        &list,
        &manifest,
        Duration::from_secs(120),
    )
    .await;

    assert_eq!(report.renewed, chunks * (TOTAL_PIECES - 1));
    assert!(
        report
            .not_renewed
            .iter()
            .all(|piece| piece.node == nodes[2].id)
    );
}

#[tokio::test]
async fn nodes_refuse_leases_longer_than_they_grant() {
    let settings = NodeSettings {
        max_lease: Duration::from_secs(60),
        ..NodeSettings::default()
    };
    let nodes = start_nodes(settings).await;

    let result = store_on(&node_list(&nodes), b"photo", Duration::from_secs(61)).await;

    // Every node refuses, so no piece finds a node.
    assert!(matches!(
        result,
        Err(UploadError::NotEnoughNodes {
            chunk: 0,
            placed: 0,
            ..
        })
    ));
}

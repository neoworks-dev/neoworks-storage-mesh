//! Writing and reading signed pointers on real nodes over HTTP.

mod test_nodes;

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::pointers::{
    POINTER_COPIES, ReadPointerError, WritePointerError, copy_pointer, pointer_nodes, read_pointer,
    write_pointer,
};
use mesh_core::pointer::{Pointer, SigningKey};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, node_list};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

async fn start(count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

fn owner_key() -> SigningKey {
    SigningKey::from_bytes(&[5; 32])
}

fn pointer(version: u64) -> Pointer {
    Pointer::sign(
        &owner_key(),
        version,
        format!("index v{version}").into_bytes(),
    )
    .unwrap()
}

#[tokio::test]
async fn a_written_pointer_reads_back_from_its_nodes() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();

    let stored = write_pointer(&client, &list, &pointer(1), LEASE)
        .await
        .unwrap();
    let read = read_pointer(&client, &list, &pointer(1).owner, None)
        .await
        .unwrap();

    assert_eq!(stored, POINTER_COPIES);
    assert_eq!(read.pointer, pointer(1));
    assert!(read.lagging.is_empty());
}

#[tokio::test]
async fn the_newest_version_wins_and_older_ones_are_refused() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    write_pointer(&client, &list, &pointer(1), LEASE)
        .await
        .unwrap();
    write_pointer(&client, &list, &pointer(2), LEASE)
        .await
        .unwrap();

    let stale_write = write_pointer(&client, &list, &pointer(1), LEASE).await;
    let read = read_pointer(&client, &list, &pointer(1).owner, None)
        .await
        .unwrap();

    assert_eq!(
        stale_write,
        Err(WritePointerError::NotNewest {
            held_version: 2,
            offered: 1
        })
    );
    assert_eq!(read.pointer.version, 2);
}

#[tokio::test]
async fn a_version_older_than_one_seen_before_is_refused() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    write_pointer(&client, &list, &pointer(3), LEASE)
        .await
        .unwrap();

    let result = read_pointer(&client, &list, &pointer(3).owner, Some(5)).await;

    assert_eq!(
        result.unwrap_err(),
        ReadPointerError::Rollback {
            found: 3,
            last_seen: 5
        }
    );
}

#[tokio::test]
async fn nodes_that_joined_the_list_are_caught_up_on_reading() {
    let mut nodes = start(10).await;
    let client = NodeClient::new().unwrap();
    write_pointer(&client, &node_list(&nodes), &pointer(1), LEASE)
        .await
        .unwrap();
    nodes.extend(start(20).await);
    let list = node_list(&nodes);
    let owner = pointer(1).owner;
    // With three times as many nodes, some of the new ones rank in the top ten.
    let newcomers_picked = pointer_nodes(&owner, &list)
        .iter()
        .filter(|node| nodes[10..].iter().any(|new| new.url == node.url))
        .count();
    assert!(newcomers_picked > 0);

    let read = read_pointer(&client, &list, &owner, None).await.unwrap();
    assert_eq!(read.lagging.len(), newcomers_picked);
    let copied = copy_pointer(&client, &read.lagging, &read.pointer, LEASE).await;
    let again = read_pointer(&client, &list, &owner, None).await.unwrap();

    assert_eq!(copied, newcomers_picked);
    assert!(again.lagging.is_empty());
}

#[tokio::test]
async fn a_pointer_survives_most_of_its_nodes_going_down() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    write_pointer(&client, &list, &pointer(1), LEASE)
        .await
        .unwrap();
    let holders: Vec<String> = pointer_nodes(&pointer(1).owner, &list)
        .iter()
        .map(|node| node.url.clone())
        .collect();
    for node in nodes.iter().filter(|node| holders[1..].contains(&node.url)) {
        node.stop();
    }

    let read = read_pointer(&client, &list, &pointer(1).owner, None)
        .await
        .unwrap();

    assert_eq!(read.pointer, pointer(1));
}

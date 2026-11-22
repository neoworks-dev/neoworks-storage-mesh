//! Two devices syncing a collection through the mesh: one commits and moves
//! its head, the other reads the head and fetches only what it lacks.

mod test_nodes;

use std::time::Duration;

use mesh_client::commits::{fetch_new, publish_head, read_head, store_commit};
use mesh_client::encryption::MessageKey;
use mesh_client::version_graph::{Change, Commit, CommitRef, Graph};
use mesh_client::{DedupKey, NodeClient, NodeList};
use mesh_core::pointer::SigningKey;
use mesh_node::NodeSettings;
use serde_json::json;
use test_nodes::{TestNode, node_list};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

async fn start(count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

/// A device that commits to a collection and publishes its head.
struct Device {
    key: SigningKey,
    graph: Graph,
    head: Option<CommitRef>,
    version: u64,
}

impl Device {
    fn new(seed: u8) -> Device {
        Device {
            key: SigningKey::from_bytes(&[seed; 32]),
            graph: Graph::default(),
            head: None,
            version: 0,
        }
    }

    async fn commit(
        &mut self,
        client: &NodeClient,
        list: &NodeList,
        keys: &(DedupKey, MessageKey),
        changes: Vec<Change>,
    ) {
        let commit = Commit {
            parents: self.head.iter().cloned().collect(),
            device: "laptop".into(),
            made_at: 0,
            changes,
        };
        let reference = store_commit(client, list, &keys.0, &commit, LEASE)
            .await
            .unwrap();
        self.graph.insert(reference.id, commit);
        self.version += 1;
        publish_head(
            client,
            list,
            &self.key,
            &keys.1,
            &reference,
            self.version,
            LEASE,
        )
        .await
        .unwrap();
        self.head = Some(reference);
    }
}

fn set_title(entry: &str, title: &str) -> Change {
    Change::Set {
        entry: entry.into(),
        field: 1,
        value: json!(title),
    }
}

#[tokio::test]
async fn a_second_device_fetches_only_the_commits_it_lacks() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    // The collection's keys, shared by the account's devices.
    let keys = (DedupKey::generate(), MessageKey::generate());
    let mut laptop = Device::new(1);
    let laptop_public = laptop.key.verifying_key().to_bytes();
    for (entry, title) in [("a", "dentist"), ("b", "party"), ("a", "dentist, 10:00")] {
        laptop
            .commit(&client, &list, &keys, vec![set_title(entry, title)])
            .await;
    }
    let mut phone = Graph::default();

    let (head, version) = read_head(&client, &list, &laptop_public, &keys.1, None)
        .await
        .unwrap();
    let fetched = fetch_new(&client, &list, &mut phone, &head).await.unwrap();

    assert_eq!(version, 3);
    assert_eq!(fetched, 3);
    let laptop_head = laptop.head.as_ref().unwrap().id;
    assert_eq!(
        phone.entries_at(&head.id).unwrap(),
        laptop.graph.entries_at(&laptop_head).unwrap()
    );

    laptop
        .commit(&client, &list, &keys, vec![set_title("c", "dinner")])
        .await;
    let (head, _) = read_head(&client, &list, &laptop_public, &keys.1, Some(version))
        .await
        .unwrap();
    let fetched = fetch_new(&client, &list, &mut phone, &head).await.unwrap();

    assert_eq!(fetched, 1);
    assert_eq!(phone.entries_at(&head.id).unwrap().len(), 3);
}

#[tokio::test]
async fn a_head_does_not_open_without_the_collection_key() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let keys = (DedupKey::generate(), MessageKey::generate());
    let mut laptop = Device::new(1);
    laptop
        .commit(&client, &list, &keys, vec![set_title("a", "dentist")])
        .await;
    let laptop_public = laptop.key.verifying_key().to_bytes();

    let result = read_head(
        &client,
        &list,
        &laptop_public,
        &MessageKey::generate(),
        None,
    )
    .await;

    assert!(result.is_err());
}

#[test]
fn a_graph_survives_being_written_to_disk() {
    let mut graph = Graph::default();
    let commit = Commit {
        parents: Vec::new(),
        device: "laptop".into(),
        made_at: 0,
        changes: vec![set_title("a", "dentist")],
    };
    let id = mesh_client::version_graph::CommitId([9; 32]);
    graph.insert(id, commit);

    let json = serde_json::to_vec(&graph).unwrap();
    let again: Graph = serde_json::from_slice(&json).unwrap();

    assert_eq!(
        again.entries_at(&id).unwrap(),
        graph.entries_at(&id).unwrap()
    );
}

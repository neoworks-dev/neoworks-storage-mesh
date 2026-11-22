//! Collections with keys of their own, on real nodes: the account's devices
//! find them through the index, and someone given a collection's access can
//! read that collection and nothing else.

mod test_nodes;

use std::collections::BTreeMap;
use std::time::Duration;

use mesh_client::account::Account;
use mesh_client::collection::{Collection, CollectionAccess, CollectionError, Writer};
use mesh_client::commits::ReadHeadError;
use mesh_client::index::{Index, IndexError, Session};
use mesh_client::version_graph::Change;
use mesh_client::{NodeClient, NodeList};
use mesh_node::NodeSettings;
use serde_json::json;
use test_nodes::{TestNode, node_list};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

async fn start() -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..10 {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

fn session<'a>(client: &'a NodeClient, list: &'a NodeList, account: &'a Account) -> Session<'a> {
    Session {
        client,
        list,
        account,
        device: "laptop".into(),
        lease: LEASE,
        coordinator: None,
    }
}

/// An event in the calendar.
fn event(title: &str) -> Vec<Change> {
    vec![Change::Set {
        entry: "event-1".into(),
        field: 1,
        value: json!(title),
    }]
}

/// Makes a calendar for `account`, writes one event to it and returns the
/// calendar's access.
async fn calendar_with_an_event(
    client: &NodeClient,
    list: &NodeList,
    account: &Account,
) -> CollectionAccess {
    let mut index = Index::default();
    let keys = index
        .create_collection(&session(client, list, account), "calendar")
        .await
        .unwrap();
    let writer = Writer {
        client,
        list,
        keys: keys.clone(),
        dedup_key: account.dedup_key(),
        device: "laptop".into(),
        lease: LEASE,
        coordinator: None,
    };
    let mut calendar = Collection::default();
    calendar.save(&writer, event("dentist")).await.unwrap();
    keys.access()
}

#[tokio::test]
async fn another_device_finds_the_collection_through_the_index() {
    let nodes = start().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    calendar_with_an_event(&client, &list, &account).await;

    let mut phone_index = Index::default();
    phone_index
        .pull(&session(&client, &list, &account))
        .await
        .unwrap();
    let keys = phone_index.collection_keys("calendar").unwrap().unwrap();
    let mut calendar = Collection::default();
    calendar
        .follow(&client, &list, &keys.access())
        .await
        .unwrap();

    assert_eq!(phone_index.collections().unwrap(), vec!["calendar"]);
    assert!(phone_index.files().unwrap().is_empty());
    assert_eq!(calendar.entries().unwrap()["event-1"][&1], json!("dentist"));
}

#[tokio::test]
async fn renewing_the_index_renews_the_collections_handed_in() {
    let nodes = start().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let access = calendar_with_an_event(&client, &list, &account).await;
    let session = session(&client, &list, &account);
    let mut index = Index::default();
    let alone = index.renew(&session).await.unwrap();
    let mut calendar = Collection::default();
    calendar.follow(&client, &list, &access).await.unwrap();
    let mut collections = BTreeMap::from([
        ("calendar".to_string(), calendar),
        ("photos".to_string(), Collection::default()),
    ]);

    let together = index
        .renew_with_collections(&session, &mut collections)
        .await
        .unwrap();

    // The calendar's one commit adds its pieces to the same requests.
    assert!(together.pieces_renewed > alone.pieces_renewed);
    assert_eq!(together.pieces_not_renewed, 0);
    assert_eq!(together.collections_renewed, 1);
    assert_eq!(together.unknown_collections, vec!["photos"]);
}

#[tokio::test]
async fn a_collection_can_be_made_only_once() {
    let nodes = start().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let mut index = Index::default();
    let session = session(&client, &list, &account);
    index.create_collection(&session, "calendar").await.unwrap();

    let again = index.create_collection(&session, "calendar").await;

    assert!(matches!(again, Err(IndexError::CollectionExists(_))));
}

#[tokio::test]
async fn sharing_a_collection_opens_that_collection_and_nothing_else() {
    let nodes = start().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let owner = Account::generate();
    let access = calendar_with_an_event(&client, &list, &owner).await;

    // Someone else, holding only the calendar's access.
    let mut shared = Collection::default();
    shared.follow(&client, &list, &access).await.unwrap();
    assert_eq!(shared.entries().unwrap()["event-1"][&1], json!("dentist"));

    // The calendar's read key does not open the owner's index.
    let index_with_calendar_key = CollectionAccess {
        head_owner: owner.pointer_owner(),
        read_key: access.read_key,
    };
    let result = Collection::default()
        .follow(&client, &list, &index_with_calendar_key)
        .await;
    assert!(matches!(
        result,
        Err(CollectionError::ReadHead(ReadHeadError::Unreadable))
    ));
}

//! Change notices on real nodes: a device that saves the index tells the
//! account's other devices, and nobody else can read the notice.

mod test_nodes;

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::change_notices::{ChangeNotice, read_notices};
use mesh_client::index::{Index, Session, unix_now};
use mesh_client::version_graph::Change;
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

fn set(entry: &str) -> Vec<Change> {
    vec![Change::Set {
        entry: entry.to_string(),
        field: 1,
        value: json!("x"),
    }]
}

#[tokio::test]
async fn saving_the_index_tells_the_other_devices_which_version_it_wrote() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let session = Session {
        client: &client,
        list: &list,
        account: &account,
        device: "laptop".to_string(),
        lease: LEASE,
    };
    let mut index = Index::default();

    index.save(&session, set("one")).await.unwrap();
    index.save(&session, set("two")).await.unwrap();
    let notices = read_notices(&client, &list, &account, unix_now()).await;

    assert_eq!(
        notices,
        [ChangeNotice {
            head_owner: account.pointer_owner(),
            version: 2,
        }]
    );
}

#[tokio::test]
async fn another_account_reads_no_notices() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let session = Session {
        client: &client,
        list: &list,
        account: &account,
        device: "laptop".to_string(),
        lease: LEASE,
    };
    Index::default().save(&session, set("one")).await.unwrap();

    let other = read_notices(&client, &list, &Account::generate(), unix_now()).await;

    assert!(other.is_empty());
}

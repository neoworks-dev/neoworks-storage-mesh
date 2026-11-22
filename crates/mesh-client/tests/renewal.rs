//! Renewing everything an account keeps through its index, one request per
//! node.

mod test_nodes;

use std::time::Duration;

use mesh_client::account::Account;
use mesh_client::index::{Index, Session, unix_now};
use mesh_client::{NodeClient, NodeList, upload};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, download_bytes, node_list, random_file, start_nodes};

async fn start(count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

fn session<'a>(
    client: &'a NodeClient,
    list: &'a NodeList,
    account: &'a Account,
    lease: Duration,
) -> Session<'a> {
    Session {
        client,
        list,
        account,
        device: "laptop".to_string(),
        lease,
        coordinators: None,
    }
}

async fn store(index: &mut Index, session: &Session<'_>, name: &str, file: &[u8]) {
    let manifest = upload(
        session.client,
        session.list,
        &session.account.dedup_key(),
        file,
        session.lease,
        &[],
    )
    .await
    .unwrap();
    let renew_by = unix_now() + session.lease.as_secs();
    index
        .add_file(session, name, &manifest, renew_by)
        .await
        .unwrap();
}

#[tokio::test]
async fn renewed_files_and_the_index_outlive_their_first_lease() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let short = session(&client, &list, &account, Duration::from_secs(2));
    let mut index = Index::default();
    let photo = random_file(50_000, 4);
    store(&mut index, &short, "photo.jpg", &photo).await;
    store(&mut index, &short, "notes.txt", b"milk").await;

    let long = session(&client, &list, &account, Duration::from_secs(24 * 60 * 60));
    let renewal = index.renew(&long).await.unwrap();
    // Past the first lease: without the renewal every piece would be gone.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(renewal.pieces_not_renewed, 0);
    assert!(renewal.at_risk.is_empty());
    let mut other_device = Index::default();
    other_device.pull(&long).await.unwrap();
    let found = other_device.file("photo.jpg").unwrap().unwrap();
    assert!(found.renew_by > unix_now() + 60 * 60);
    assert_eq!(
        download_bytes(&client, &list, &found.manifest)
            .await
            .unwrap(),
        photo
    );
}

#[tokio::test]
async fn files_that_lost_too_many_pieces_are_reported_at_risk() {
    // With six nodes, every chunk has one piece on each.
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let lease = Duration::from_secs(24 * 60 * 60);
    let session = session(&client, &list, &account, lease);
    let mut index = Index::default();
    store(&mut index, &session, "photo.jpg", &random_file(20_000, 5)).await;

    for node in &nodes[..3] {
        node.lose_all_pieces();
    }
    let renewal = index.renew(&session).await.unwrap();

    let at_risk: Vec<&str> = renewal
        .at_risk
        .iter()
        .map(|file| file.name.as_str())
        .collect();
    assert_eq!(at_risk, ["photo.jpg"]);
    assert!(renewal.pieces_not_renewed > 0);
}

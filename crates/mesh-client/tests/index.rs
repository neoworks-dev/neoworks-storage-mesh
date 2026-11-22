//! The account's index on real nodes: a new device finds every file from the
//! account's secret alone, and two devices saving at once both keep their
//! files.

mod test_nodes;

use std::time::Duration;

use mesh_client::account::Account;
use mesh_client::index::{Index, Session, unix_now};
use mesh_client::{NodeClient, NodeList, upload};
use mesh_node::NodeSettings;
use test_nodes::{TestNode, download_bytes, node_list, random_file};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

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
    device: &str,
) -> Session<'a> {
    Session {
        client,
        list,
        account,
        device: device.to_string(),
        lease: LEASE,
        coordinators: None,
    }
}

/// Uploads `file` under the account and adds it to `index` as `name`.
async fn store(index: &mut Index, session: &Session<'_>, name: &str, file: &[u8]) {
    let manifest = upload(
        session.client,
        session.list,
        &session.account.dedup_key(),
        file,
        LEASE,
        &[],
    )
    .await
    .unwrap();
    let renew_by = unix_now() + LEASE.as_secs();
    index
        .add_file(session, name, &manifest, renew_by)
        .await
        .unwrap();
}

fn names(index: &Index) -> Vec<String> {
    index
        .files()
        .unwrap()
        .into_iter()
        .map(|file| file.name)
        .collect()
}

#[tokio::test]
async fn a_new_device_finds_every_file_from_the_account_alone() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let laptop = session(&client, &list, &account, "laptop");
    let mut laptop_index = Index::default();
    let photo = random_file(50_000, 1);
    store(&mut laptop_index, &laptop, "photo.jpg", &photo).await;
    store(
        &mut laptop_index,
        &laptop,
        "notes.txt",
        b"remember the milk",
    )
    .await;

    // A device that has nothing but the account's secret.
    let account_again = Account::from_hex(&account.to_hex()).unwrap();
    let phone = session(&client, &list, &account_again, "phone");
    let mut phone_index = Index::default();
    phone_index.pull(&phone).await.unwrap();

    assert_eq!(names(&phone_index), ["notes.txt", "photo.jpg"]);
    let found = phone_index.file("photo.jpg").unwrap().unwrap();
    assert_eq!(
        download_bytes(&client, &list, &found.manifest)
            .await
            .unwrap(),
        photo
    );
}

#[tokio::test]
async fn two_devices_saving_at_once_both_keep_their_files() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let laptop = session(&client, &list, &account, "laptop");
    let phone = session(&client, &list, &account, "phone");
    let mut laptop_index = Index::default();
    let mut phone_index = Index::default();
    store(&mut laptop_index, &laptop, "shared.txt", b"both know this").await;
    phone_index.pull(&phone).await.unwrap();

    // Neither has seen the other's new file when it saves.
    store(&mut laptop_index, &laptop, "from-laptop.txt", b"laptop").await;
    store(&mut phone_index, &phone, "from-phone.txt", b"phone").await;
    laptop_index.pull(&laptop).await.unwrap();

    let expected = ["from-laptop.txt", "from-phone.txt", "shared.txt"];
    assert_eq!(names(&phone_index), expected);
    assert_eq!(names(&laptop_index), expected);
}

#[tokio::test]
async fn a_removed_file_is_gone_on_every_device() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let laptop = session(&client, &list, &account, "laptop");
    let phone = session(&client, &list, &account, "phone");
    let mut laptop_index = Index::default();
    store(&mut laptop_index, &laptop, "old.txt", b"old").await;
    store(&mut laptop_index, &laptop, "keep.txt", b"keep").await;

    laptop_index.remove_file(&laptop, "old.txt").await.unwrap();
    let mut phone_index = Index::default();
    phone_index.pull(&phone).await.unwrap();

    assert_eq!(names(&phone_index), ["keep.txt"]);
}

#[test]
fn an_index_survives_being_written_to_disk() {
    let index = Index::default();

    let json = serde_json::to_vec(&index).unwrap();
    let again: Index = serde_json::from_slice(&json).unwrap();

    assert!(again.files().unwrap().is_empty());
}

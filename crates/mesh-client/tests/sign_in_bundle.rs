//! Signing in from the mesh: the account's secret is found on real nodes
//! from the email and password alone, with no coordinator involved.

mod test_nodes;

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::index::{Index, Session};
use mesh_client::pointers::read_pointer;
use mesh_client::sign_in_bundle::{
    BundleError, BundlePlace, change_password, open_bundle, store_bundle,
};
use mesh_core::sign_in::KdfParams;
use mesh_node::NodeSettings;
use test_nodes::{TestNode, node_list};

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// Argon2 settings cheap enough for a debug build; real accounts use
/// `BUNDLE_KDF`.
const CHEAP: KdfParams = KdfParams {
    memory_kib: 8,
    iterations: 1,
};

async fn start(count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

fn place(email: &str, password: &str) -> BundlePlace {
    BundlePlace::derive(email, password, CHEAP).unwrap()
}

#[tokio::test]
async fn a_new_device_finds_the_secret_from_email_and_password() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();

    let stored = place("ana@example.org", "correct horse");
    store_bundle(&client, &list, &stored, &account, LEASE)
        .await
        .unwrap();
    let found = open_bundle(&client, &list, &place("Ana@Example.org", "correct horse"))
        .await
        .unwrap();

    assert_eq!(found.to_hex(), account.to_hex());
}

#[tokio::test]
async fn a_wrong_password_and_an_unknown_email_look_the_same() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let stored = place("ana@example.org", "correct horse");
    store_bundle(&client, &list, &stored, &Account::generate(), LEASE)
        .await
        .unwrap();

    let wrong_password = open_bundle(&client, &list, &place("ana@example.org", "guess")).await;
    let unknown_email = open_bundle(&client, &list, &place("bob@example.org", "guess")).await;

    assert!(matches!(wrong_password, Err(BundleError::WrongPassword)));
    assert!(matches!(unknown_email, Err(BundleError::WrongPassword)));
}

#[tokio::test]
async fn after_a_password_change_only_the_new_password_opens_the_secret() {
    let nodes = start(12).await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let old = place("ana@example.org", "correct horse");
    let new = place("ana@example.org", "battery staple");
    store_bundle(&client, &list, &old, &account, LEASE)
        .await
        .unwrap();

    change_password(&client, &list, &account, &old, &new, LEASE)
        .await
        .unwrap();

    let with_new = open_bundle(&client, &list, &new).await.unwrap();
    let with_old = open_bundle(&client, &list, &old).await;
    assert_eq!(with_new.to_hex(), account.to_hex());
    assert!(matches!(with_old, Err(BundleError::WrongPassword)));
}

#[tokio::test]
async fn renewing_the_index_writes_the_bundle_again() {
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
    let stored = place("ana@example.org", "correct horse");
    store_bundle(&client, &list, &stored, &account, LEASE)
        .await
        .unwrap();
    let mut index = Index::default();
    index.keep_sign_in_place(&session, &stored).await.unwrap();

    // Another device knows only the account's secret, not the password.
    let mut other = Index::default();
    other.renew(&session).await.unwrap();

    let read = read_pointer(&client, &list, &stored.owner(), None)
        .await
        .unwrap();
    assert_eq!(read.pointer.version, 2);
    let found = open_bundle(&client, &list, &stored).await.unwrap();
    assert_eq!(found.to_hex(), account.to_hex());
}

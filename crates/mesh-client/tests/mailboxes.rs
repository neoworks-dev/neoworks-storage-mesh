//! Mailboxes over real nodes and a real coordinator: strangers write with
//! proof of work, contacts without, only the recipient opens what arrives,
//! and mailbox keys are found by email.

mod test_nodes;

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::mailboxes::{
    Received, look_up_key, mailbox_public_key, publish_key, receive, send, send_to_contact,
};
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::mailbox::{MailboxKind, personal_address, week_of};
use mesh_core::node_list::CoordinatorList;
use mesh_core::sign_in::KdfParams;
use mesh_node::NodeSettings;
use reqwest::StatusCode;
use test_nodes::{TestNode, node_list};
use tokio::net::TcpListener;

/// Ten nodes, as many as hold a mailbox.
async fn start_nodes() -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for _ in 0..10 {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    nodes
}

fn now() -> u64 {
    mesh_client::index::unix_now()
}

#[tokio::test]
async fn a_stranger_leaves_a_message_only_the_recipient_opens() {
    let nodes = start_nodes().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let bob = Account::generate();
    let bob_key = mailbox_public_key(&bob.mailbox_secret());

    let stored = send(&client, &list, &bob_key, b"a collection key", now())
        .await
        .unwrap();

    assert_eq!(stored, 10);
    let received = receive(&client, &list, &bob.mailbox_secret(), &[], now()).await;
    assert_eq!(
        received,
        vec![Received {
            from_contact: None,
            message: b"a collection key".to_vec(),
        }]
    );
    let eve = Account::generate();
    let for_eve = receive(&client, &list, &eve.mailbox_secret(), &[], now()).await;
    assert!(for_eve.is_empty());
}

#[tokio::test]
async fn a_contact_writes_without_work_and_is_known_as_the_sender() {
    let nodes = start_nodes().await;
    let list = node_list(&nodes);
    let client = NodeClient::new().unwrap();
    let ana = Account::generate();
    let bob = Account::generate();
    let ana_key = mailbox_public_key(&ana.mailbox_secret());
    let bob_key = mailbox_public_key(&bob.mailbox_secret());

    send_to_contact(
        &client,
        &list,
        &ana.mailbox_secret(),
        &bob_key,
        b"hi",
        now(),
    )
    .await
    .unwrap();

    let received = receive(&client, &list, &bob.mailbox_secret(), &[ana_key], now()).await;
    assert_eq!(
        received,
        vec![Received {
            from_contact: Some(ana_key),
            message: b"hi".to_vec(),
        }]
    );
}

#[tokio::test]
async fn a_message_without_work_is_refused_at_a_personal_mailbox() {
    let node = TestNode::start(NodeSettings::default()).await;
    let week = week_of(now());
    let address = personal_address(&[7; 32], week);
    let url = format!(
        "{}{}?week={week}&nonce=0",
        node.url,
        MailboxKind::Personal.path(&address)
    );

    let response = reqwest::Client::new()
        .put(url)
        .body(b"buy now".to_vec())
        .send()
        .await
        .unwrap();

    // A nonce of 0 is enough work with a chance of one in a million.
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mailbox_keys_are_found_by_email_and_unknown_emails_look_alike() {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let coordinators = CoordinatorList {
        version: 1,
        keys: vec![data.public_key()],
    };
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(60),
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    let client = NodeClient::new().unwrap();
    let ana = Account::generate();
    let cheap = KdfParams {
        memory_kib: 8,
        iterations: 1,
    };
    sign_up(&client, &coordinator, "ana@example.org", "pw", &ana, cheap)
        .await
        .unwrap();
    let auth = auth_key(&client, &coordinator, "ana@example.org", "pw")
        .await
        .unwrap();
    let ana_key = mailbox_public_key(&ana.mailbox_secret());

    publish_key(&client, &coordinator, "ana@example.org", auth, ana_key)
        .await
        .unwrap();

    let found = look_up_key(&client, &coordinator, &coordinators, "Ana@Example.org").await;
    assert_eq!(found.unwrap(), ana_key);
    let made_up = look_up_key(&client, &coordinator, &coordinators, "bob+x@example.org").await;
    assert_ne!(made_up.unwrap(), ana_key);
    let wrong = publish_key(&client, &coordinator, "ana@example.org", [0; 32], [1; 32]).await;
    assert!(wrong.is_err());
}

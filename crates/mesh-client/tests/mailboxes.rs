//! Mailboxes over real nodes and a real coordinator: strangers write with
//! proof of work, contacts with a little, one address leaves only so many
//! messages in a contact mailbox, only the recipient opens what arrives, and
//! mailbox keys are found by email.

mod test_nodes;

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::mailboxes::{
    Received, look_up_key, mailbox_public_key, publish_key, receive, send, send_to_contact,
};
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::mailbox::{
    MAX_MESSAGES_PER_SENDER, MailboxKind, contact_address, find_work, personal_address, week_of,
    work_holds,
};
use mesh_core::node_list::{CoordinatorList, ListedCoordinator};
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
async fn a_contact_is_known_as_the_sender() {
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

/// Puts `message` into the contact mailbox `address` on `node`, with the work
/// `nonce` claims.
async fn put_contact(
    node: &TestNode,
    address: &[u8; 32],
    message: &[u8],
    nonce: u64,
) -> StatusCode {
    let week = week_of(now());
    let url = format!(
        "{}{}?week={week}&nonce={nonce}",
        node.url,
        MailboxKind::Contact.path(address)
    );
    let response = reqwest::Client::new()
        .put(url)
        .body(message.to_vec())
        .send()
        .await
        .unwrap();
    response.status()
}

#[tokio::test]
async fn a_message_without_work_is_refused_at_a_contact_mailbox() {
    let node = TestNode::start(NodeSettings::default()).await;
    let address = contact_address(&[7; 32], &[1; 32], week_of(now()));
    let week = week_of(now());
    // The first nonce that is not enough: almost every one.
    let nonce = (0..u64::MAX)
        .find(|&nonce| !work_holds(MailboxKind::Contact, &address, b"junk", week, nonce))
        .unwrap();

    let status = put_contact(&node, &address, b"junk", nonce).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn one_address_leaves_only_its_share_in_a_contact_mailbox() {
    let node = TestNode::start(NodeSettings::default()).await;
    let week = week_of(now());
    let address = contact_address(&[7; 32], &[1; 32], week);
    let mut answers = Vec::new();
    for index in 0..=MAX_MESSAGES_PER_SENDER {
        let message = format!("message {index}").into_bytes();
        let nonce = find_work(MailboxKind::Contact, &address, &message, week);
        answers.push(put_contact(&node, &address, &message, nonce).await);
    }

    let mut expected = vec![StatusCode::NO_CONTENT; MAX_MESSAGES_PER_SENDER as usize];
    expected.push(StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(answers, expected);
}

#[tokio::test]
async fn mailbox_keys_are_found_by_email_and_unknown_emails_look_alike() {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator = format!("http://{}", listener.local_addr().unwrap());
    let coordinators = CoordinatorList {
        version: 1,
        coordinators: vec![ListedCoordinator {
            key: data.public_key(),
            url: coordinator.clone(),
        }],
    };
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(60),
        ..CoordinatorSettings::default()
    };
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

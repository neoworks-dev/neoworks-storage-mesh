//! Storing on nodes that require tokens: paying tabs, storing against them,
//! and renewing by moving pieces into next month's tab.

mod test_nodes;

use std::collections::BTreeMap;
use std::time::Duration;

use mesh_client::account::Account;
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_client::tabs::{move_into_tabs, new_tab_name, open_tabs};
use mesh_client::tokens::get_tokens;
use mesh_client::{NodeClient, NodeRequestError, upload};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::PieceId;
use mesh_core::node_list::{CoordinatorList, ListedCoordinator, NodeSnapshot};
use mesh_core::sign_in::KdfParams;
use mesh_core::signed::Signed;
use mesh_core::tokens::{Token, TokenKind};
use mesh_node::{NodeSettings, Registration, register};
use reqwest::StatusCode;
use tempfile::TempDir;
use test_nodes::{TestNode, download_bytes, node_list, random_file, start_nodes, urls};
use tokio::net::TcpListener;

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

/// A coordinator, six nodes registered there that require its tokens and
/// have a price, and 20 paid tokens; also the coordinator's URL, its list
/// and `ana`'s auth key.
async fn start() -> (TempDir, Vec<TestNode>, Vec<Token>) {
    let (directory, nodes, tokens, _coordinator, _coordinators, _auth) = start_with_account().await;
    (directory, nodes, tokens)
}

async fn start_with_account() -> (
    TempDir,
    Vec<TestNode>,
    Vec<Token>,
    String,
    CoordinatorList,
    [u8; 32],
) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    data.credit("ana@example.org", 20).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator = format!("http://{}", listener.local_addr().unwrap());
    let coordinators = CoordinatorList {
        version: 1,
        coordinators: vec![ListedCoordinator {
            key: data.public_key(),
            url: coordinator.clone(),
        }],
    };
    // Fast rounds, so the nodes are measured, listed and priced at once.
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        probe_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        ..CoordinatorSettings::default()
    };
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    let nodes = start_nodes(NodeSettings {
        require_tokens: Some(coordinator.clone()),
        ..NodeSettings::default()
    })
    .await;
    register_all(&coordinator, &nodes).await;
    let client = NodeClient::new().unwrap();
    let cheap = KdfParams {
        memory_kib: 8,
        iterations: 1,
    };
    sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "pw",
        &Account::generate(),
        cheap,
    )
    .await
    .unwrap();
    let auth = auth_key(&client, &coordinator, "ana@example.org", "pw")
        .await
        .unwrap();
    let tokens = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        TokenKind::Paid,
        20,
    )
    .await
    .unwrap();
    (directory, nodes, tokens, coordinator, coordinators, auth)
}

/// Registers every node at `coordinator` and waits until it prices them all.
async fn register_all(coordinator: &str, nodes: &[TestNode]) {
    let http = reqwest::Client::new();
    for (index, node) in nodes.iter().enumerate() {
        let registration = Registration {
            coordinator: coordinator.to_string(),
            url: node.url.clone(),
            operator: format!("operator-{index}"),
            network: format!("network-{index}"),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            operator_email: Some("op@example.org".into()),
        };
        register(&http, &node.key, &registration).await.unwrap();
    }
    for _ in 0..100 {
        if all_priced(&http, coordinator, nodes.len()).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the coordinator did not price the nodes");
}

/// Whether the coordinator lists `count` nodes and has prices out.
async fn all_priced(http: &reqwest::Client, coordinator: &str, count: usize) -> bool {
    let Ok(list) = http.get(format!("{coordinator}/node-list")).send().await else {
        return false;
    };
    let Ok(list) = list.bytes().await else {
        return false;
    };
    let Ok(snapshot) = serde_json::from_slice::<Signed<NodeSnapshot>>(&list) else {
        return false;
    };
    let prices = http.get(format!("{coordinator}/prices")).send().await;
    snapshot.body.nodes.len() == count
        && prices.is_ok_and(|response| response.status() == StatusCode::OK)
}

#[tokio::test]
async fn a_node_that_requires_tokens_refuses_free_storage() {
    let (_data, nodes, _tokens) = start().await;
    let client = NodeClient::new().unwrap();
    let bytes = b"piece".to_vec();

    let result = client
        .put(&nodes[0].url, &PieceId::of(&bytes), bytes, LEASE)
        .await;

    assert!(matches!(
        result,
        Err(NodeRequestError::Refused {
            status: StatusCode::PAYMENT_REQUIRED,
            ..
        })
    ));
}

#[tokio::test]
async fn a_file_stored_against_paid_tabs_comes_back_and_renews() {
    let (_data, nodes, mut wallet) = start().await;
    let client = NodeClient::new().unwrap();
    let node_urls = urls(&nodes);
    let nodes_by_url: Vec<&str> = node_urls.iter().map(String::as_str).collect();
    open_tabs(&client, &nodes_by_url, &mut wallet, 1)
        .await
        .unwrap();
    let file = random_file(100_000, 6);

    let list = node_list(&nodes);
    let manifest = upload(
        &client,
        &list,
        &Account::generate().dedup_key(),
        file.as_slice(),
        LEASE,
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );

    // Next month: new tabs, and every piece moved into them.
    let next_tabs = open_tabs(&client, &nodes_by_url, &mut wallet, 1)
        .await
        .unwrap();
    let mut pieces: BTreeMap<String, Vec<PieceId>> = BTreeMap::new();
    for piece in manifest.chunks.iter().flat_map(|chunk| &chunk.pieces) {
        let url = list.url_of(&piece.node).unwrap().to_string();
        pieces.entry(url).or_default().push(piece.id);
    }
    let renewed = move_into_tabs(&client, &next_tabs, &pieces).await.unwrap();

    assert_eq!(renewed, manifest.chunks.len() * 6);
    assert_eq!(wallet.len(), 20 - 12);
}

#[tokio::test]
async fn a_node_the_coordinator_does_not_run_refuses_allowance_tokens() {
    let (_data, nodes, _paid, coordinator, coordinators, auth) = start_with_account().await;
    let client = NodeClient::new().unwrap();
    let allowance = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        TokenKind::Allowance,
        1,
    )
    .await
    .unwrap();

    let result = client
        .pay_tab(&nodes[0].url, &new_tab_name(), &allowance)
        .await;

    assert!(matches!(
        result,
        Err(NodeRequestError::Refused {
            status: StatusCode::PAYMENT_REQUIRED,
            ..
        })
    ));
}

#[tokio::test]
async fn a_spent_token_does_not_pay_a_second_tab() {
    let (_data, nodes, wallet) = start().await;
    let client = NodeClient::new().unwrap();
    client
        .pay_tab(&nodes[0].url, &new_tab_name(), &wallet[..1])
        .await
        .unwrap();

    let again = client
        .pay_tab(&nodes[1].url, &new_tab_name(), &wallet[..1])
        .await;

    assert!(matches!(
        again,
        Err(NodeRequestError::Refused {
            status: StatusCode::PAYMENT_REQUIRED,
            ..
        })
    ));
}

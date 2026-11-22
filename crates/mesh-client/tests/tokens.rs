//! Getting blind-signed tokens from a real coordinator and spending them.

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_client::tokens::{TokenError, get_tokens, token_key};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::NodeId;
use mesh_core::node_list::{CoordinatorList, ListedCoordinator};
use mesh_core::sign_in::KdfParams;
use mesh_core::signed::{Signed, SigningKey};
use mesh_core::tokens::{SpendRequest, SpendResponse, Token, TokenKind};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

const CHEAP: KdfParams = KdfParams {
    memory_kib: 8,
    iterations: 1,
};

/// The key of the node the coordinator runs itself.
fn own_node() -> SigningKey {
    SigningKey::from_bytes(&[1; 32])
}

/// The key of a node someone else runs.
fn other_node() -> SigningKey {
    SigningKey::from_bytes(&[2; 32])
}

fn id_of(key: &SigningKey) -> NodeId {
    NodeId::of(&key.verifying_key().to_bytes())
}

/// A coordinator whose accounts get five allowance tokens a month and which
/// runs [`own_node`], with `ana` signed up and three paid tokens on her
/// balance, and her auth key.
async fn start() -> (TempDir, String, CoordinatorList, [u8; 32]) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    data.credit("ana@example.org", 3).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let coordinators = CoordinatorList {
        version: 1,
        coordinators: vec![ListedCoordinator {
            key: data.public_key(),
            url: url.clone(),
        }],
    };
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(60),
        monthly_allowance: 5,
        own_nodes: vec![id_of(&own_node())],
        ..CoordinatorSettings::default()
    };
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    let client = NodeClient::new().unwrap();
    sign_up(
        &client,
        &url,
        "ana@example.org",
        "pw",
        &Account::generate(),
        CHEAP,
    )
    .await
    .unwrap();
    let auth = auth_key(&client, &url, "ana@example.org", "pw")
        .await
        .unwrap();
    (directory, url, coordinators, auth)
}

async fn tokens_of(
    coordinator: &str,
    coordinators: &CoordinatorList,
    auth: [u8; 32],
    kind: TokenKind,
    count: usize,
) -> Result<Vec<Token>, TokenError> {
    let client = NodeClient::new().unwrap();
    get_tokens(
        &client,
        coordinator,
        coordinators,
        "ana@example.org",
        auth,
        kind,
        count,
    )
    .await
}

/// Spends `token` as the node with `key`.
async fn spend(coordinator: &str, key: &SigningKey, token: &Token) -> reqwest::Response {
    let request = SpendRequest {
        node_id: id_of(key),
        token: token.clone(),
    };
    send_spend(coordinator, &Signed::sign(key, request)).await
}

async fn send_spend(coordinator: &str, request: &Signed<SpendRequest>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{coordinator}/tokens/spend"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(request).unwrap())
        .send()
        .await
        .unwrap()
}

async fn spent_now(response: reqwest::Response) -> bool {
    let answer: SpendResponse = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    answer.spent_now
}

fn is_forbidden<T>(result: &Result<T, TokenError>) -> bool {
    matches!(
        result,
        Err(TokenError::Refused {
            status: StatusCode::FORBIDDEN,
            ..
        })
    )
}

#[tokio::test]
async fn a_token_verifies_against_the_month_key_and_spends_once() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let client = NodeClient::new().unwrap();

    let tokens = tokens_of(&coordinator, &coordinators, auth, TokenKind::Allowance, 3)
        .await
        .unwrap();
    let key = token_key(&client, &coordinator, &coordinators, TokenKind::Allowance)
        .await
        .unwrap();

    assert_eq!(tokens.len(), 3);
    assert!(
        tokens
            .iter()
            .all(|token| token.is_signed_by(&key.public_key))
    );
    assert!(spent_now(spend(&coordinator, &own_node(), &tokens[0]).await).await);
    assert!(!spent_now(spend(&coordinator, &own_node(), &tokens[0]).await).await);
}

#[tokio::test]
async fn an_account_gets_no_more_than_its_allowance() {
    let (_data, coordinator, coordinators, auth) = start().await;
    tokens_of(&coordinator, &coordinators, auth, TokenKind::Allowance, 4)
        .await
        .unwrap();

    let more = tokens_of(&coordinator, &coordinators, auth, TokenKind::Allowance, 2).await;

    assert!(is_forbidden(&more));
}

#[tokio::test]
async fn paid_tokens_come_out_of_the_balance() {
    let (_data, coordinator, coordinators, auth) = start().await;

    let paid = tokens_of(&coordinator, &coordinators, auth, TokenKind::Paid, 3).await;
    let more = tokens_of(&coordinator, &coordinators, auth, TokenKind::Paid, 1).await;

    assert_eq!(paid.unwrap().len(), 3);
    assert!(is_forbidden(&more));
}

#[tokio::test]
async fn only_the_coordinators_own_nodes_take_allowance_tokens() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let allowance = tokens_of(&coordinator, &coordinators, auth, TokenKind::Allowance, 1)
        .await
        .unwrap();
    let paid = tokens_of(&coordinator, &coordinators, auth, TokenKind::Paid, 1)
        .await
        .unwrap();

    let refused = spend(&coordinator, &other_node(), &allowance[0]).await;

    assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    // Refusing it did not spend it.
    assert!(spent_now(spend(&coordinator, &own_node(), &allowance[0]).await).await);
    assert!(spent_now(spend(&coordinator, &other_node(), &paid[0]).await).await);
}

#[tokio::test]
async fn a_spend_must_be_signed_by_the_node_it_names() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let allowance = tokens_of(&coordinator, &coordinators, auth, TokenKind::Allowance, 1)
        .await
        .unwrap();
    // Another node claims to be the coordinator's own one.
    let request = SpendRequest {
        node_id: id_of(&own_node()),
        token: allowance[0].clone(),
    };

    let response = send_spend(&coordinator, &Signed::sign(&other_node(), request)).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn tokens_need_the_accounts_password() {
    let (_data, coordinator, coordinators, _auth) = start().await;

    let result = tokens_of(
        &coordinator,
        &coordinators,
        [0; 32],
        TokenKind::Allowance,
        1,
    )
    .await;

    assert!(matches!(result, Err(TokenError::WrongPassword)));
}

#[tokio::test]
async fn a_forged_token_is_refused() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let mut token = tokens_of(&coordinator, &coordinators, auth, TokenKind::Paid, 1)
        .await
        .unwrap()
        .remove(0);
    token.message[0] ^= 1;

    assert_eq!(
        spend(&coordinator, &own_node(), &token).await.status(),
        StatusCode::BAD_REQUEST
    );
}

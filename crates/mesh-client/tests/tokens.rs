//! Getting blind-signed tokens from a real coordinator and spending them.

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_client::tokens::{TokenError, get_tokens, token_key};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::node_list::CoordinatorList;
use mesh_core::sign_in::KdfParams;
use mesh_core::tokens::{SpendResponse, Token};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

const CHEAP: KdfParams = KdfParams {
    memory_kib: 8,
    iterations: 1,
};

/// A coordinator whose accounts get five tokens a month, with `ana`
/// signed up, and her auth key.
async fn start() -> (TempDir, String, CoordinatorList, [u8; 32]) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let coordinators = CoordinatorList {
        version: 1,
        keys: vec![data.public_key()],
    };
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(60),
        monthly_allowance: 5,
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
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

async fn spend(coordinator: &str, token: &Token) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{coordinator}/tokens/spend"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(token).unwrap())
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_token_verifies_against_the_month_key_and_spends_once() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let client = NodeClient::new().unwrap();

    let tokens = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        3,
    )
    .await
    .unwrap();
    let key = token_key(&client, &coordinator, &coordinators)
        .await
        .unwrap();

    assert_eq!(tokens.len(), 3);
    assert!(
        tokens
            .iter()
            .all(|token| token.is_signed_by(&key.public_key))
    );
    let first: SpendResponse =
        serde_json::from_slice(&spend(&coordinator, &tokens[0]).await.bytes().await.unwrap())
            .unwrap();
    let again: SpendResponse =
        serde_json::from_slice(&spend(&coordinator, &tokens[0]).await.bytes().await.unwrap())
            .unwrap();
    assert!(first.spent_now);
    assert!(!again.spent_now);
}

#[tokio::test]
async fn an_account_gets_no_more_than_its_allowance() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let client = NodeClient::new().unwrap();
    get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        4,
    )
    .await
    .unwrap();

    let more = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        2,
    )
    .await;

    assert!(matches!(
        more,
        Err(TokenError::Refused {
            status: StatusCode::FORBIDDEN,
            ..
        })
    ));
}

#[tokio::test]
async fn tokens_need_the_accounts_password() {
    let (_data, coordinator, coordinators, _auth) = start().await;
    let client = NodeClient::new().unwrap();

    let result = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        [0; 32],
        1,
    )
    .await;

    assert!(matches!(result, Err(TokenError::WrongPassword)));
}

#[tokio::test]
async fn a_forged_token_is_refused() {
    let (_data, coordinator, coordinators, auth) = start().await;
    let client = NodeClient::new().unwrap();
    let mut token = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        1,
    )
    .await
    .unwrap()
    .remove(0);
    token.message[0] ^= 1;

    assert_eq!(
        spend(&coordinator, &token).await.status(),
        StatusCode::BAD_REQUEST
    );
}

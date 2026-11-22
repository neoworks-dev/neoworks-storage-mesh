//! Signing up and in against a real coordinator.

use std::time::Duration;

use mesh_client::NodeClient;
use mesh_client::account::Account;
use mesh_client::sign_in::{SignInError, sign_in, sign_up};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::sign_in::KdfParams;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Argon2 settings cheap enough for a debug build; real accounts use more.
const CHEAP: KdfParams = KdfParams {
    memory_kib: 8,
    iterations: 1,
};

async fn start_coordinator() -> (TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(60),
        ..CoordinatorSettings::default()
    };
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url)
}

#[tokio::test]
async fn a_new_device_signs_in_to_the_same_account() {
    let (_data, coordinator) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    let account = Account::generate();

    sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "correct horse",
        &account,
        CHEAP,
    )
    .await
    .unwrap();
    let signed_in = sign_in(&client, &coordinator, "Ana@Example.org", "correct horse")
        .await
        .unwrap();

    assert_eq!(signed_in.pointer_owner(), account.pointer_owner());
}

#[tokio::test]
async fn a_wrong_password_and_an_unknown_email_look_the_same() {
    let (_data, coordinator) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "correct horse",
        &Account::generate(),
        CHEAP,
    )
    .await
    .unwrap();

    let wrong_password = sign_in(&client, &coordinator, "ana@example.org", "guess").await;
    // An unknown email gets a made-up salt and the default cost, which takes
    // a moment in a debug build.
    let unknown_email = sign_in(&client, &coordinator, "bob@example.org", "guess").await;

    assert!(matches!(wrong_password, Err(SignInError::WrongPassword)));
    assert!(matches!(unknown_email, Err(SignInError::WrongPassword)));
}

#[tokio::test]
async fn an_email_signs_up_once() {
    let (_data, coordinator) = start_coordinator().await;
    let client = NodeClient::new().unwrap();
    sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "one",
        &Account::generate(),
        CHEAP,
    )
    .await
    .unwrap();

    let again = sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "two",
        &Account::generate(),
        CHEAP,
    )
    .await;

    assert!(matches!(again, Err(SignInError::Taken)));
}

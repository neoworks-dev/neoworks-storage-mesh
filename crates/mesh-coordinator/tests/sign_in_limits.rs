//! Failed sign-ins over HTTP: a few are answered, then the coordinator
//! refuses for a while, the same way for emails with and without accounts.

use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::sign_in::{
    ACCOUNTS_ROUTE, KdfParams, SALT_BYTES, SIGN_IN_ROUTE, SignInRequest, SignUpRequest,
};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, RETRY_AFTER};
use tempfile::TempDir;
use tokio::net::TcpListener;

const RIGHT_KEY: [u8; 32] = [2; 32];
const WRONG_KEY: [u8; 32] = [7; 32];

async fn start_coordinator() -> (TempDir, String) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(
        listener,
        data,
        CoordinatorSettings::default(),
    ));
    (directory, url)
}

async fn post(coordinator: &str, route: &str, json: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{coordinator}{route}"))
        .header(CONTENT_TYPE, "application/json")
        .body(json)
        .send()
        .await
        .unwrap()
}

async fn sign_up(coordinator: &str, email: &str) {
    let request = SignUpRequest {
        email: email.into(),
        salt: [1; SALT_BYTES],
        kdf: KdfParams {
            memory_kib: 8,
            iterations: 1,
        },
        auth_key: RIGHT_KEY,
        locked_secret: vec![3; 72],
    };
    let json = serde_json::to_vec(&request).unwrap();
    let response = post(coordinator, ACCOUNTS_ROUTE, json).await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn sign_in(coordinator: &str, email: &str, auth_key: [u8; 32]) -> reqwest::Response {
    let request = SignInRequest {
        email: email.into(),
        auth_key,
    };
    post(
        coordinator,
        SIGN_IN_ROUTE,
        serde_json::to_vec(&request).unwrap(),
    )
    .await
}

/// The answers to six wrong guesses and then the right key, on a fresh
/// coordinator where `email` has an account or not.
async fn answers_to_guessing(email: &str, has_account: bool) -> Vec<StatusCode> {
    let (_data, coordinator) = start_coordinator().await;
    if has_account {
        sign_up(&coordinator, email).await;
    }
    let mut answers = Vec::new();
    for _ in 0..6 {
        answers.push(sign_in(&coordinator, email, WRONG_KEY).await.status());
    }
    answers.push(sign_in(&coordinator, email, RIGHT_KEY).await.status());
    answers
}

#[tokio::test]
async fn five_failures_lock_the_email_even_for_the_right_key() {
    let answers = answers_to_guessing("ana@example.org", true).await;

    let mut expected = vec![StatusCode::UNAUTHORIZED; 5];
    expected.extend([StatusCode::TOO_MANY_REQUESTS; 2]);
    assert_eq!(answers, expected);
}

#[tokio::test]
async fn an_unknown_email_is_locked_just_like_a_known_one() {
    let known = answers_to_guessing("ana@example.org", true).await;
    let unknown = answers_to_guessing("nobody@example.org", false).await;

    assert_eq!(unknown, known);
}

#[tokio::test]
async fn a_locked_answer_says_when_to_try_again() {
    let (_data, coordinator) = start_coordinator().await;
    for _ in 0..5 {
        sign_in(&coordinator, "ana@example.org", WRONG_KEY).await;
    }

    let locked = sign_in(&coordinator, "ana@example.org", WRONG_KEY).await;

    assert_eq!(locked.status(), StatusCode::TOO_MANY_REQUESTS);
    let seconds: u64 = locked.headers()[RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=60).contains(&seconds));
}

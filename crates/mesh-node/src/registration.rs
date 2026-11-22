//! Telling a coordinator where this node is, in a record signed with the
//! node's own key.
//!
//! The coordinator accepts the record only once it has asked the URL in it
//! for the node's identity and got this node's id back, so the URL has to be
//! the one the node is reached at from outside.

use std::time::Duration;

use mesh_core::NodeId;
use mesh_core::node_list::NodeRecord;
use mesh_core::protocol::REGISTER_ROUTE;
use mesh_core::signed::{Signed, SigningKey};
use reqwest::StatusCode;

use crate::clock::unix_now;

/// What a node tells its coordinator about itself.
#[derive(Clone, Debug)]
pub struct Registration {
    /// The coordinator's base URL.
    pub coordinator: String,
    /// Where clients reach this node.
    pub url: String,
    pub operator: String,
    pub network: String,
    pub region: String,
    pub capacity_bytes: u64,
}

/// A registration the coordinator did not take.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("the coordinator could not be reached: {0}")]
    Unreachable(#[from] reqwest::Error),
    #[error("the coordinator answered {status}: {message}")]
    Refused { status: StatusCode, message: String },
}

/// Registers the node with `key` as `registration` says, once.
pub async fn register(
    http: &reqwest::Client,
    key: &SigningKey,
    registration: &Registration,
) -> Result<(), RegistrationError> {
    let body = NodeRecord {
        node_id: NodeId::of(&key.verifying_key().to_bytes()),
        url: registration.url.clone(),
        operator: registration.operator.clone(),
        network: registration.network.clone(),
        region: registration.region.clone(),
        capacity_bytes: registration.capacity_bytes,
        signed_at: unix_now(),
    };
    let record = Signed::sign(key, body);
    let url = format!(
        "{}{REGISTER_ROUTE}",
        registration.coordinator.trim_end_matches('/')
    );
    // A record is plain strings and integers, which always serialize.
    let json = serde_json::to_vec(&record).expect("a node record serializes");
    let response = http
        .post(url)
        .header("content-type", "application/json")
        .body(json)
        .send()
        .await?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    // The reason is for the operator reading the log; a body that can't be
    // read only loses that.
    let message = response.text().await.unwrap_or_default();
    Err(RegistrationError::Refused { status, message })
}

/// Registers the node every `interval`, forever, so a coordinator that lost
/// its registry or a node that moved is caught up within one interval.
pub async fn register_forever(key: SigningKey, registration: Registration, interval: Duration) {
    let http = reqwest::Client::new();
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        match register(&http, &key, &registration).await {
            Ok(()) => println!("registered with {}", registration.coordinator),
            Err(error) => eprintln!("registration: {error}"),
        }
    }
}

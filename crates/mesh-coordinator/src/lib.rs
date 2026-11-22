//! A coordinator of the mesh: it tracks the network, never anyone's data.
//!
//! Nodes register a record signed with their own key. The coordinator checks
//! that each node answers at the address it claims, keeps checking that it is
//! up, and every hour publishes a signed snapshot of all nodes with their
//! uptime. Clients download the snapshot whole and check its signature
//! against the coordinator keys they start with.
//!
//! People also sign in here with an email and a password; the coordinator
//! keeps only what checks a sign-in and the account's secret locked under a
//! key it never sees. Signed-in accounts get their monthly allowance of
//! storage tokens, signed blind, and the coordinator keeps the list of spent
//! ones. The `mesh-coordinator` binary runs
//! one; tests start coordinators in-process through [`serve`].

mod accounts;
mod clock;
mod coordinator_key;
mod probe;
mod publish;
mod registry;
mod routes;
mod settings;
mod sign_in_routes;
mod token_issuer;
mod token_routes;

pub use coordinator_key::CoordinatorKeyError;
pub use settings::CoordinatorSettings;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mesh_core::node_list::CoordinatorKey;
use mesh_core::signed::SigningKey;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::accounts::Accounts;
use crate::registry::Registry;
use crate::routes::Coordinator;
use crate::token_issuer::TokenIssuer;

/// How long the coordinator waits for a node to answer a check.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// A coordinator's registry, accounts and key, from its data directory:
///
/// ```text
/// <data>/coordinator.key      the key snapshots are signed with
/// <data>/registry.sqlite3     node records and check results
/// <data>/accounts.sqlite3     what checking a sign-in needs
/// <data>/tokens.sqlite3       token keys, issued counts, spent tokens
/// ```
pub struct CoordinatorData {
    registry: Registry,
    accounts: Accounts,
    tokens: TokenIssuer,
    key: SigningKey,
}

/// A data directory that could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("registry: {0}")]
    Registry(#[from] rusqlite::Error),
    #[error("{0}")]
    Key(#[from] CoordinatorKeyError),
}

impl CoordinatorData {
    /// Opens the data in `directory`, creating the key on first start.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, OpenError> {
        let directory = directory.as_ref();
        std::fs::create_dir_all(directory).map_err(CoordinatorKeyError::from)?;
        let key = coordinator_key::load_or_create(directory)?;
        let fake_salt_key = blake3::derive_key(
            "neoworks-mesh coordinator fake salt key v1",
            &key.to_bytes(),
        );
        Ok(CoordinatorData {
            registry: Registry::open(directory)?,
            accounts: Accounts::open(directory, fake_salt_key)?,
            tokens: TokenIssuer::open(directory)?,
            key,
        })
    }

    /// The public key clients check snapshots against.
    pub fn public_key(&self) -> CoordinatorKey {
        CoordinatorKey(self.key.verifying_key().to_bytes())
    }
}

/// Serves `data` on `listener`, checks nodes and publishes snapshots, until
/// the task is dropped or the server fails.
pub async fn serve(
    listener: TcpListener,
    data: CoordinatorData,
    settings: CoordinatorSettings,
) -> std::io::Result<()> {
    let http = reqwest::Client::builder()
        .timeout(CHECK_TIMEOUT)
        .build()
        .map_err(std::io::Error::other)?;
    let registry = Arc::new(data.registry);
    let latest = Arc::new(RwLock::new(None));
    let coordinator = Coordinator {
        registry: registry.clone(),
        accounts: Arc::new(data.accounts),
        tokens: Arc::new(data.tokens),
        key: data.key.clone(),
        latest: latest.clone(),
        http: http.clone(),
        max_record_age_seconds: settings.max_record_age.as_secs(),
        monthly_allowance: settings.monthly_allowance,
    };
    let server = axum::serve(listener, routes::router(coordinator));
    // All three run on this task, so stopping the coordinator stops them all.
    tokio::select! {
        result = server => result,
        () = probe::probe_forever(http, registry.clone(), settings.clone()) => Ok(()),
        () = publish::publish_forever(registry, data.key, latest, settings) => Ok(()),
    }
}

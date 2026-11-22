//! A coordinator of the mesh: it tracks the network, never anyone's data.
//!
//! Nodes register a record signed with their own key. The coordinator checks
//! that each node answers at the address it claims, keeps checking that it is
//! up, times how fast it uploads and counts how often nodes disappear for
//! good. Every hour it publishes a signed snapshot of all nodes with their
//! uptime and bandwidth, and the loss rate; once a day it works out what
//! storage costs on each class of nodes from that snapshot and signs the
//! prices too. Clients download the snapshot whole and check its signature
//! against the coordinator keys they start with.
//!
//! People also sign in here with an email and a password; the coordinator
//! keeps only what checks a sign-in and the account's secret locked under a
//! key it never sees. Signed-in accounts get their monthly allowance of
//! storage tokens, signed blind, and the coordinator keeps the list of spent
//! ones. An account that stops taking tokens gets warned by email before its
//! data expires. The `mesh-coordinator` binary runs
//! one; tests start coordinators in-process through [`serve`].

mod accounts;
mod clock;
mod coordinator_key;
mod expiry_warnings;
mod measurements;
mod price_book;
mod probe;
mod publish;
mod registry;
mod routes;
mod settings;
mod sign_in_routes;
mod speed_test;
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
use crate::expiry_warnings::ExpiryWarnings;
use crate::measurements::Measurements;
use crate::price_book::PriceBook;
use crate::registry::Registry;
use crate::routes::Coordinator;
use crate::token_issuer::TokenIssuer;

/// How long the coordinator waits for a node to answer a check.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// A coordinator's registry, accounts and key, from its data directory:
///
/// ```text
/// <data>/coordinator.key       the key snapshots are signed with
/// <data>/registry.sqlite3      node records and check results
/// <data>/measurements.sqlite3  speed tests, node losses and usage
/// <data>/prices.sqlite3        the signed price table of every day
/// <data>/accounts.sqlite3      what checking a sign-in needs
/// <data>/tokens.sqlite3        token keys, issued counts, spent tokens
/// <data>/warnings.sqlite3      expiry warnings sent
/// <data>/outbox/               warning emails for the mail tool to send
/// ```
pub struct CoordinatorData {
    registry: Registry,
    measurements: Measurements,
    prices: PriceBook,
    accounts: Accounts,
    tokens: TokenIssuer,
    warnings: ExpiryWarnings,
    key: SigningKey,
}

/// A data directory that could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("{0}")]
    Key(#[from] CoordinatorKeyError),
    #[error("expiry warnings: {0}")]
    Warnings(std::io::Error),
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
            measurements: Measurements::open(directory)?,
            prices: PriceBook::open(directory)?,
            accounts: Accounts::open(directory, fake_salt_key)?,
            tokens: TokenIssuer::open(directory)?,
            warnings: ExpiryWarnings::open(directory).map_err(OpenError::Warnings)?,
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
    let measurements = Arc::new(data.measurements);
    let tokens = Arc::new(data.tokens);
    let latest = Arc::new(RwLock::new(None));
    let prices = Arc::new(RwLock::new(None));
    let coordinator = Coordinator {
        registry: registry.clone(),
        accounts: Arc::new(data.accounts),
        tokens: tokens.clone(),
        key: data.key.clone(),
        latest: latest.clone(),
        prices: prices.clone(),
        http: http.clone(),
        max_record_age_seconds: settings.max_record_age.as_secs(),
        monthly_allowance: settings.monthly_allowance,
    };
    let server = axum::serve(listener, routes::router(coordinator));
    // All of them run on this task, so stopping the coordinator stops them all.
    tokio::select! {
        result = server => result,
        () = probe::probe_forever(http, registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = speed_test::measure_forever(registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = expiry_warnings::warn_forever(data.warnings, tokens, settings.clone()) => Ok(()),
        () = price_book::price_forever(data.prices, data.key.clone(), latest.clone(), prices, settings.clone()) => Ok(()),
        () = publish::publish_forever(registry, measurements, data.key, latest, settings) => Ok(()),
    }
}

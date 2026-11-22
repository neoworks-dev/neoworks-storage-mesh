//! A coordinator of the mesh: it tracks the network, never anyone's data.
//!
//! Nodes register a record signed with their own key. The coordinator checks
//! that each node answers at the address it claims, keeps checking that it is
//! up, times how fast it uploads and counts how often nodes disappear for
//! good. It keeps a small piece of its own on every node and audits it, so a
//! node that answers but threw its pieces away shows. Every hour it
//! publishes a signed snapshot of all nodes with their uptime, audit results
//! and bandwidth, and the loss rate; once a day it works out what
//! storage costs on each class of nodes from that snapshot and signs the
//! prices too. Clients download the snapshot whole and check its signature
//! against the coordinator keys they start with.
//!
//! People also sign in here with an email and a password; the coordinator
//! keeps only what checks a sign-in and the account's secret locked under a
//! key it never sees. Signed-in accounts take tokens, signed blind: free
//! allowance tokens each month, which only the coordinator's own nodes take,
//! and paid ones out of their balance. The coordinator keeps the list of
//! spent ones, and pays what a node was paid to its operator's balance over
//! the month, for as long as the node stays up. An account that stops taking
//! tokens gets warned by email before its data expires. The
//! `mesh-coordinator` binary runs one; tests start coordinators in-process
//! through [`serve`].

mod accounts;
mod audit_log;
mod audits;
mod clock;
mod coordinator_key;
mod earnings;
mod expiry_warnings;
mod locate;
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
use crate::audit_log::AuditLog;
use crate::audits::Auditor;
use crate::earnings::Earnings;
use crate::expiry_warnings::ExpiryWarnings;
use crate::locate::Locator;
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
/// <data>/earnings.sqlite3      what operators earned and the repair pool
/// <data>/audits.sqlite3        audit pieces on nodes and challenge results
/// <data>/accounts.sqlite3      what checking a sign-in needs
/// <data>/tokens.sqlite3        token keys, issued counts, spent tokens
/// <data>/warnings.sqlite3      expiry warnings sent
/// <data>/outbox/               warning emails for the mail tool to send
/// ```
pub struct CoordinatorData {
    registry: Registry,
    measurements: Measurements,
    prices: PriceBook,
    earnings: Earnings,
    audits: AuditLog,
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
            earnings: Earnings::open(directory)?,
            audits: AuditLog::open(directory)?,
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

    /// Adds `tokens` to the paid balance of the account `email` and returns
    /// the new balance. This is where bought tokens come in until billing
    /// exists.
    pub async fn credit(&self, email: &str, tokens: u64) -> rusqlite::Result<u64> {
        self.tokens.credit(email, tokens).await
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
    let earnings = Arc::new(data.earnings);
    let earnings_for_publish = earnings.clone();
    // A coordinator whose tables can't be read still runs; its nodes are
    // listed without network and country, and count as standing alone.
    let locator = Locator::load(
        settings.city_csv.as_deref(),
        settings.asn_tsv.as_deref(),
        settings.location,
    )
    .unwrap_or_else(|error| {
        eprintln!("locating nodes: {error}");
        Locator::empty()
    });
    let tokens = Arc::new(data.tokens);
    let audits = Arc::new(data.audits);
    let auditor = Auditor {
        http: http.clone(),
        registry: registry.clone(),
        log: audits.clone(),
        tokens: tokens.clone(),
    };
    let latest = Arc::new(RwLock::new(None));
    let prices = Arc::new(RwLock::new(None));
    let coordinator = Coordinator {
        registry: registry.clone(),
        earnings: earnings.clone(),
        accounts: Arc::new(data.accounts),
        tokens: tokens.clone(),
        key: data.key.clone(),
        latest: latest.clone(),
        prices: prices.clone(),
        http: http.clone(),
        max_record_age_seconds: settings.max_record_age.as_secs(),
        monthly_allowance: settings.monthly_allowance,
        own_nodes: Arc::new(settings.own_nodes.clone()),
    };
    let server = axum::serve(listener, routes::router(coordinator));
    // All of them run on this task, so stopping the coordinator stops them all.
    tokio::select! {
        result = server => result,
        () = probe::probe_forever(http, locator, registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = speed_test::measure_forever(registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = audits::audit_forever(auditor, settings.clone()) => Ok(()),
        () = earnings::pay_out_forever(earnings, registry.clone(), audits.clone(), tokens.clone(), settings.clone()) => Ok(()),
        () = expiry_warnings::warn_forever(data.warnings, tokens, settings.clone()) => Ok(()),
        () = price_book::price_forever(data.prices, data.key.clone(), latest.clone(), prices, settings.clone()) => Ok(()),
        () = publish::publish_forever(registry, measurements, earnings_for_publish, audits, data.key, latest, settings) => Ok(()),
    }
}

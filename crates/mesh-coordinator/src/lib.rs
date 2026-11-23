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
//! Clients register their chunks here, by piece ids and nodes and without
//! keys, and the coordinator rebuilds the pieces of nodes that go down onto
//! new ones while the owners' devices are off, paid from what the failed
//! nodes no longer earn ([`repairs`]).
//!
//! People also sign in here with an email and a password; the coordinator
//! keeps only what checks a sign-in and the account's secret locked under a
//! key it never sees. Signed-in accounts take tokens, signed blind: free
//! allowance tokens each month, which only the coordinator's own nodes take,
//! and paid ones out of their balance. The coordinator keeps the list of
//! spent ones, and pays what a node was paid to its operator's balance over
//! the month, for as long as the node stays up. An account that stops taking
//! tokens gets warned by email before its data expires. Accounts also file
//! their mailbox key here, so others can look it up by email. Web pages,
//! which can't call plain-HTTP nodes, reach nodes through the coordinator's
//! HTTPS gateway ([`gateway`]). The
//! `mesh-coordinator` binary runs one; tests start coordinators in-process
//! through [`serve`].

mod accounts;
mod audit_log;
mod audits;
mod chunk_book;
mod chunk_routes;
mod clock;
mod coordinator_key;
mod earnings;
mod expiry_warnings;
mod gateway;
mod locate;
mod mailbox_key_routes;
mod mailbox_keys;
mod measurements;
mod price_book;
mod probe;
mod publish;
mod registry;
mod repairs;
mod routes;
mod settings;
mod sign_in_limits;
mod sign_in_routes;
mod speed_test;
mod token_issuer;
mod token_routes;

pub use coordinator_key::CoordinatorKeyError;
pub use settings::CoordinatorSettings;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mesh_client::NodeClient;
use mesh_core::node_list::CoordinatorKey;
use mesh_core::signed::SigningKey;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use crate::accounts::Accounts;
use crate::audit_log::AuditLog;
use crate::audits::Auditor;
use crate::chunk_book::ChunkBook;
use crate::earnings::Earnings;
use crate::expiry_warnings::ExpiryWarnings;
use crate::locate::Locator;
use crate::mailbox_keys::MailboxKeys;
use crate::measurements::Measurements;
use crate::price_book::PriceBook;
use crate::registry::Registry;
use crate::repairs::Repairer;
use crate::routes::Coordinator;
use crate::sign_in_limits::FailedSignIns;
use crate::token_issuer::TokenIssuer;

/// How long the coordinator waits for a node to answer a check.
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the gateway waits for a node. A 9 MiB piece at 2 Mbit/s, a slow
/// home upload, takes about 40 seconds.
const GATEWAY_TIMEOUT: Duration = Duration::from_secs(60);

/// A coordinator's registry, accounts and key, from its data directory:
///
/// ```text
/// <data>/coordinator.key       the key snapshots are signed with
/// <data>/registry.sqlite3      node records and check results
/// <data>/measurements.sqlite3  speed tests, node losses and usage
/// <data>/prices.sqlite3        the signed price table of every day
/// <data>/earnings.sqlite3      what operators earned and the repair pool
/// <data>/chunks.sqlite3        chunks registered for repair
/// <data>/audits.sqlite3        audit pieces on nodes and challenge results
/// <data>/accounts.sqlite3      what checking a sign-in needs
/// <data>/mailbox_keys.sqlite3  mailbox public keys by email
/// <data>/tokens.sqlite3        token keys, issued counts, spent tokens
/// <data>/warnings.sqlite3      expiry warnings sent
/// <data>/outbox/               warning emails for the mail tool to send
/// ```
pub struct CoordinatorData {
    registry: Registry,
    measurements: Measurements,
    prices: PriceBook,
    earnings: Earnings,
    chunks: ChunkBook,
    audits: AuditLog,
    accounts: Accounts,
    mailbox_keys: MailboxKeys,
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
        let fake_mailbox_key_secret = blake3::derive_key(
            "neoworks-mesh coordinator fake mailbox key v1",
            &key.to_bytes(),
        );
        Ok(CoordinatorData {
            registry: Registry::open(directory)?,
            measurements: Measurements::open(directory)?,
            prices: PriceBook::open(directory)?,
            earnings: Earnings::open(directory)?,
            chunks: ChunkBook::open(directory)?,
            audits: AuditLog::open(directory)?,
            accounts: Accounts::open(directory, fake_salt_key)?,
            mailbox_keys: MailboxKeys::open(directory, fake_mailbox_key_secret)?,
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
    let chunks = Arc::new(data.chunks);
    let chunk_owners = settings.coordinators.clone().map(Arc::new);
    let repairer = Repairer {
        client: NodeClient::new().map_err(std::io::Error::other)?,
        registry: registry.clone(),
        chunks: chunks.clone(),
        earnings: earnings.clone(),
        tokens: tokens.clone(),
        latest: latest.clone(),
        prices: prices.clone(),
        own_key: CoordinatorKey(data.key.verifying_key().to_bytes()),
        coordinators: chunk_owners.clone(),
        unreachable_since: Mutex::new(HashMap::new()),
    };
    let coordinator = Coordinator {
        registry: registry.clone(),
        earnings: earnings.clone(),
        accounts: Arc::new(data.accounts),
        chunks,
        chunk_owners,
        failed_sign_ins: Arc::new(Mutex::new(FailedSignIns::new())),
        mailbox_keys: Arc::new(data.mailbox_keys),
        tokens: tokens.clone(),
        key: data.key.clone(),
        latest: latest.clone(),
        prices: prices.clone(),
        http: http.clone(),
        gateway_http: reqwest::Client::builder()
            .timeout(GATEWAY_TIMEOUT)
            .build()
            .map_err(std::io::Error::other)?,
        max_record_age_seconds: settings.max_record_age.as_secs(),
        monthly_allowance: settings.monthly_allowance,
        own_nodes: Arc::new(settings.own_nodes.clone()),
    };
    // Sign-ins are limited per address, so handlers need to see it.
    let service = routes::router(coordinator).into_make_service_with_connect_info::<SocketAddr>();
    let server = axum::serve(listener, service);
    // All of them run on this task, so stopping the coordinator stops them all.
    tokio::select! {
        result = server => result,
        () = probe::probe_forever(http, locator, registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = speed_test::measure_forever(registry.clone(), measurements.clone(), settings.clone()) => Ok(()),
        () = repairs::repair_forever(repairer, settings.clone()) => Ok(()),
        () = audits::audit_forever(auditor, settings.clone()) => Ok(()),
        () = earnings::pay_out_forever(earnings, registry.clone(), audits.clone(), tokens.clone(), settings.clone()) => Ok(()),
        () = expiry_warnings::warn_forever(data.warnings, tokens, settings.clone()) => Ok(()),
        () = price_book::price_forever(data.prices, data.key.clone(), latest.clone(), prices, settings.clone()) => Ok(()),
        () = publish::publish_forever(registry, measurements, earnings_for_publish, audits, data.key, latest, settings) => Ok(()),
    }
}

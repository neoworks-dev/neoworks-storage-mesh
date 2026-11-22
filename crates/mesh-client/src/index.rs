//! The account's index: every stored file's manifest and every collection's
//! keys, kept in the mesh.
//!
//! Without it, losing a manifest loses the file. The index is a
//! [`Collection`](crate::collection::Collection) under keys derived from the
//! account's secret: one entry per file, named by the file's name, with its
//! manifest and when its leases need renewing, and one entry per collection
//! with that collection's own keys. A new device holding only the account's
//! secret reads the account's pointer, then the index, then any file or
//! collection.
//!
//! All devices of an account move the same pointer. When a device finds a
//! version there it has not seen, another device saved in the meantime: it
//! fetches that head, merges it with its own, and writes the merge one
//! version higher. Two devices adding files while apart therefore both keep
//! their files.
//!
//! The index also holds the account's wallet ([`crate::wallet`]): the tokens
//! that pay nodes which require them, one entry per token.

use std::collections::BTreeMap;
use std::time::Duration;

use mesh_core::tokens::{MONTH_SECONDS, Token};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub use crate::collection::unix_now;

use crate::account::Account;
use crate::batch_renew::{BatchRenewal, renew_pieces};
use crate::collection::{Collection, CollectionError, CollectionKeys, Writer};
use crate::commits::{FetchCommitError, PublishHeadError, ReadHeadError};
use crate::manifest::Manifest;
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::tab_renewal::{NodePrices, pieces_by_node, renew_through_tabs};
use crate::upload::UploadError;
use crate::version_graph::{Change, Entries, MissingCommit};
use crate::wallet::Wallet;

/// The field of an entry holding the file's manifest.
pub const FIELD_MANIFEST: u32 = 1;
/// The field holding the file's size in bytes.
pub const FIELD_SIZE: u32 = 2;
/// The field holding when the file's leases need renewing, in Unix seconds.
pub const FIELD_RENEW_BY: u32 = 3;
/// The field of a collection's entry holding its keys, as
/// [`CollectionKeys::to_hex`] writes them.
pub const FIELD_COLLECTION_KEYS: u32 = 4;
/// The field of a wallet entry holding one token. Each token is an entry of
/// its own, named after its message, so devices adding and using tokens at
/// once merge without losing any.
pub const FIELD_TOKEN: u32 = 5;

/// What collection entries' names start with. No file name holds a NUL
/// byte on any system, so a collection can never share an entry with a file.
const COLLECTION_PREFIX: &str = "\0collection/";
/// What wallet entries' names start with, for the same reason.
const TOKEN_PREFIX: &str = "\0token/";

/// One file in the index.
#[derive(Clone)]
pub struct IndexedFile {
    pub name: String,
    pub manifest: Manifest,
    /// When the file's leases end, by the clock of the device that stored or
    /// renewed it; renewing is due before then.
    pub renew_by: u64,
}

/// Where the index is kept and under which keys.
pub struct Session<'a> {
    pub client: &'a NodeClient,
    pub list: &'a NodeList,
    pub account: &'a Account,
    /// This device's name, recorded in its commits.
    pub device: String,
    /// How long commits and the pointer are stored.
    pub lease: Duration,
}

impl Session<'_> {
    /// The index's writer: the account's own keys.
    fn writer(&self) -> Writer<'_> {
        self.writer_for(CollectionKeys {
            head_key: self.account.pointer_key(),
            read_key: self.account.message_key(),
        })
    }

    /// A writer of the collection with `keys`, as this device.
    pub fn writer_for(&self, keys: CollectionKeys) -> Writer<'_> {
        Writer {
            client: self.client,
            list: self.list,
            keys,
            dedup_key: self.account.dedup_key(),
            device: self.device.clone(),
            lease: self.lease,
        }
    }
}

/// Why the index could not be read or saved.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("storing a commit: {0}")]
    Store(#[from] UploadError),
    #[error("{0}")]
    Fetch(#[from] FetchCommitError),
    #[error("reading the account's pointer: {0}")]
    ReadHead(#[from] ReadHeadError),
    #[error("moving the account's pointer: {0}")]
    PublishHead(#[from] PublishHeadError),
    #[error("{0}")]
    Missing(#[from] MissingCommit),
    #[error("the entry of {0} is damaged")]
    Damaged(String),
    #[error("other devices kept moving the pointer; try again")]
    Contended,
    #[error("the index already has a collection called {0}")]
    CollectionExists(String),
}

impl From<CollectionError> for IndexError {
    fn from(error: CollectionError) -> Self {
        match error {
            CollectionError::Store(error) => IndexError::Store(error),
            CollectionError::Fetch(error) => IndexError::Fetch(error),
            CollectionError::ReadHead(error) => IndexError::ReadHead(error),
            CollectionError::PublishHead(error) => IndexError::PublishHead(error),
            CollectionError::Missing(error) => IndexError::Missing(error),
            CollectionError::Contended => IndexError::Contended,
        }
    }
}

/// The index as this device knows it. It is kept on the device between runs.
#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Index {
    collection: Collection,
}

impl Index {
    /// Every file in the index, by name.
    pub fn files(&self) -> Result<Vec<IndexedFile>, IndexError> {
        self.entries()?
            .iter()
            .filter(|(name, _)| !name.starts_with(COLLECTION_PREFIX))
            .filter(|(name, _)| !name.starts_with(TOKEN_PREFIX))
            .map(|(name, fields)| to_file(name, fields))
            .collect()
    }

    /// The file called `name`, if the index has it.
    pub fn file(&self, name: &str) -> Result<Option<IndexedFile>, IndexError> {
        let entries = self.entries()?;
        let Some(fields) = entries.get(name) else {
            return Ok(None);
        };
        Ok(Some(to_file(name, fields)?))
    }

    /// Adds or replaces the file `name`, whose leases end at `renew_by`, and
    /// saves the index.
    pub async fn add_file(
        &mut self,
        session: &Session<'_>,
        name: &str,
        manifest: &Manifest,
        renew_by: u64,
    ) -> Result<(), IndexError> {
        // A manifest is plain strings and numbers, which always serialize.
        let manifest_json = serde_json::to_value(manifest).expect("a manifest serializes");
        let changes = vec![
            set(name, FIELD_MANIFEST, manifest_json),
            set(name, FIELD_SIZE, json!(manifest.size)),
            set(name, FIELD_RENEW_BY, json!(renew_by)),
        ];
        self.save(session, changes).await
    }

    /// Removes the file `name` from the index and saves it. Its pieces expire
    /// once nobody renews them.
    pub async fn remove_file(
        &mut self,
        session: &Session<'_>,
        name: &str,
    ) -> Result<(), IndexError> {
        let changes = vec![Change::Remove {
            entry: name.to_string(),
        }];
        self.save(session, changes).await
    }

    /// Makes a collection called `name` with keys of its own, keeps the keys
    /// in the index, saves it, and returns the keys.
    pub async fn create_collection(
        &mut self,
        session: &Session<'_>,
        name: &str,
    ) -> Result<CollectionKeys, IndexError> {
        self.pull(session).await?;
        if self.collection_keys(name)?.is_some() {
            return Err(IndexError::CollectionExists(name.to_string()));
        }
        let keys = CollectionKeys::generate();
        let entry = format!("{COLLECTION_PREFIX}{name}");
        let changes = vec![set(&entry, FIELD_COLLECTION_KEYS, json!(keys.to_hex()))];
        self.save(session, changes).await?;
        Ok(keys)
    }

    /// The keys of the collection called `name`, if the index has it.
    pub fn collection_keys(&self, name: &str) -> Result<Option<CollectionKeys>, IndexError> {
        let entries = self.entries()?;
        let Some(fields) = entries.get(&format!("{COLLECTION_PREFIX}{name}")) else {
            return Ok(None);
        };
        Ok(Some(to_keys(name, fields)?))
    }

    /// The names of every collection in the index.
    pub fn collections(&self) -> Result<Vec<String>, IndexError> {
        Ok(self
            .entries()?
            .keys()
            .filter_map(|name| name.strip_prefix(COLLECTION_PREFIX))
            .map(str::to_string)
            .collect())
    }

    /// The tokens in the account's wallet.
    pub fn wallet(&self) -> Result<Wallet, IndexError> {
        let mut tokens = Vec::new();
        for (name, fields) in self.entries()? {
            if !name.starts_with(TOKEN_PREFIX) {
                continue;
            }
            let token = fields
                .get(&FIELD_TOKEN)
                .and_then(|token| serde_json::from_value(token.clone()).ok())
                .ok_or_else(|| IndexError::Damaged(name.clone()))?;
            tokens.push(token);
        }
        Ok(Wallet::new(tokens))
    }

    /// Adds `tokens` to the account's wallet and saves the index.
    pub async fn add_tokens(
        &mut self,
        session: &Session<'_>,
        tokens: &[Token],
    ) -> Result<(), IndexError> {
        let changes = tokens
            .iter()
            // A token is plain integers and bytes, which always serialize.
            .map(|token| {
                let value = serde_json::to_value(token).expect("a token serializes");
                set(&token_entry(token), FIELD_TOKEN, value)
            })
            .collect();
        self.save(session, changes).await
    }

    /// Renews everything the account keeps for `session.lease`: every file's
    /// pieces, the index's own commits, and its pointer. Files whose pieces
    /// all held get the new renewal date.
    ///
    /// All pieces go out in one request per node, however many files there
    /// are. The index's commits are renewed with them; letting those expire
    /// would lose the list of files while the files themselves live on.
    pub async fn renew(&mut self, session: &Session<'_>) -> Result<IndexRenewal, IndexError> {
        self.renew_with_collections(session, &mut BTreeMap::new())
            .await
    }

    /// Renews like [`renew`](Self::renew), and also `collections`, this
    /// device's copies of collections by name: their commits go into the same
    /// requests, and their pointers are written again. A collection whose
    /// keys the index doesn't hold is left alone and reported.
    pub async fn renew_with_collections(
        &mut self,
        session: &Session<'_>,
        collections: &mut BTreeMap<String, Collection>,
    ) -> Result<IndexRenewal, IndexError> {
        self.renew_paying(session, collections, None).await
    }

    /// Renews like [`renew`](Self::renew), but pieces on nodes that require
    /// tokens are moved into a new tab on each node, paid from the account's
    /// wallet at the node's price in `prices`. Tabs last a month, so files
    /// renewed this way are due again within one.
    pub async fn renew_through_tabs(
        &mut self,
        session: &Session<'_>,
        prices: &NodePrices,
    ) -> Result<IndexRenewal, IndexError> {
        self.renew_paying(session, &mut BTreeMap::new(), Some(prices))
            .await
    }

    async fn renew_paying(
        &mut self,
        session: &Session<'_>,
        collections: &mut BTreeMap<String, Collection>,
        prices: Option<&NodePrices>,
    ) -> Result<IndexRenewal, IndexError> {
        self.pull(session).await?;
        let files = self.files()?;
        let mut commit_manifests = self.collection.commit_manifests()?;
        let mut kept = Vec::new();
        let mut unknown_collections = Vec::new();
        for (name, collection) in collections.iter_mut() {
            let Some(keys) = self.collection_keys(name)? else {
                unknown_collections.push(name.clone());
                continue;
            };
            commit_manifests.extend(collection.commit_manifests()?);
            kept.push((keys, collection));
        }
        let manifests: Vec<&Manifest> = files
            .iter()
            .map(|file| &file.manifest)
            .chain(&commit_manifests)
            .collect();
        let pieces = manifests
            .iter()
            .flat_map(|manifest| &manifest.chunks)
            .flat_map(|chunk| &chunk.pieces);
        let mut renewal = renew_pieces(session.client, session.list, pieces, session.lease).await;
        let mut lease = session.lease.as_secs();
        let mut changes = Vec::new();
        if let Some(prices) = prices {
            lease = lease.min(MONTH_SECONDS);
            changes = self
                .pay_tabs(session, prices, &manifests, &mut renewal)
                .await?;
        }
        let renewed_until = unix_now() + lease;
        let (safe, at_risk): (Vec<IndexedFile>, Vec<IndexedFile>) = files
            .into_iter()
            .partition(|file| is_readable(&file.manifest, &renewal));
        changes.extend(
            safe.iter()
                .map(|file| set(&file.name, FIELD_RENEW_BY, json!(renewed_until))),
        );
        // Saving writes the pointer anew, which renews it too.
        if changes.is_empty() {
            self.collection.publish(&session.writer()).await?;
        } else {
            self.save(session, changes).await?;
        }
        // Writing a collection's pointer again renews it the same way.
        let collections_renewed = kept.len();
        for (keys, collection) in kept {
            collection.publish(&session.writer_for(keys)).await?;
        }
        Ok(IndexRenewal {
            pieces_renewed: renewal.renewed.len(),
            pieces_not_renewed: renewal.not_renewed.len(),
            at_risk,
            collections_renewed,
            unknown_collections,
        })
    }

    /// Renews the pieces of `manifests` on the nodes `renewal` found to
    /// require tokens through tabs paid from the wallet, counts them as
    /// renewed in `renewal`, and returns the changes that take the tokens
    /// handed over out of the wallet.
    async fn pay_tabs(
        &self,
        session: &Session<'_>,
        prices: &NodePrices,
        manifests: &[&Manifest],
        renewal: &mut BatchRenewal,
    ) -> Result<Vec<Change>, IndexError> {
        let pieces = pieces_by_node(manifests, &renewal.requires_tokens);
        let mut wallet = self.wallet()?;
        let paid =
            renew_through_tabs(session.client, session.list, prices, &mut wallet, &pieces).await;
        for key in paid.renewed {
            renewal.not_renewed.remove(&key);
            renewal.renewed.insert(key);
        }
        let changes = paid
            .used
            .iter()
            .map(|token| Change::Remove {
                entry: token_entry(token),
            })
            .collect();
        Ok(changes)
    }

    /// Brings this device's index up to date with the account's pointer,
    /// merging when both sides saved something the other lacks.
    pub async fn pull(&mut self, session: &Session<'_>) -> Result<(), IndexError> {
        Ok(self.collection.pull(&session.writer()).await?)
    }

    /// Commits `changes` on top of this device's head and moves the
    /// account's pointer to it.
    pub async fn save(
        &mut self,
        session: &Session<'_>,
        changes: Vec<Change>,
    ) -> Result<(), IndexError> {
        Ok(self.collection.save(&session.writer(), changes).await?)
    }

    fn entries(&self) -> Result<Entries, IndexError> {
        Ok(self.collection.entries()?)
    }
}

/// What renewing the index achieved.
pub struct IndexRenewal {
    pub pieces_renewed: usize,
    pub pieces_not_renewed: usize,
    /// Files with a chunk that has fewer renewed pieces than data pieces.
    /// They are unreadable from their old `renew_by` on unless repaired
    /// first.
    pub at_risk: Vec<IndexedFile>,
    /// How many of the collections handed in were renewed.
    pub collections_renewed: usize,
    /// Collections handed in whose keys the index doesn't hold, which were
    /// not renewed.
    pub unknown_collections: Vec<String>,
}

/// Whether every chunk of `manifest` has enough renewed pieces to rebuild it.
fn is_readable(manifest: &Manifest, renewal: &BatchRenewal) -> bool {
    manifest.chunks.iter().all(|chunk| {
        let renewed = chunk
            .pieces
            .iter()
            .filter(|piece| renewal.is_renewed(piece))
            .count();
        renewed >= chunk.layout.data()
    })
}

fn set(entry: &str, field: u32, value: Value) -> Change {
    Change::Set {
        entry: entry.to_string(),
        field,
        value,
    }
}

/// The file `name` from its entry's fields.
fn to_file(name: &str, fields: &BTreeMap<u32, Value>) -> Result<IndexedFile, IndexError> {
    let damaged = || IndexError::Damaged(name.to_string());
    let manifest = fields.get(&FIELD_MANIFEST).ok_or_else(damaged)?;
    let manifest = serde_json::from_value(manifest.clone()).map_err(|_| damaged())?;
    let renew_by = fields
        .get(&FIELD_RENEW_BY)
        .and_then(Value::as_u64)
        .ok_or_else(damaged)?;
    Ok(IndexedFile {
        name: name.to_string(),
        manifest,
        renew_by,
    })
}

/// The keys of the collection `name` from its entry's fields.
/// The name of the wallet entry holding `token`.
fn token_entry(token: &Token) -> String {
    format!("{TOKEN_PREFIX}{}", hex::encode(token.message))
}

fn to_keys(name: &str, fields: &BTreeMap<u32, Value>) -> Result<CollectionKeys, IndexError> {
    fields
        .get(&FIELD_COLLECTION_KEYS)
        .and_then(Value::as_str)
        .and_then(CollectionKeys::from_hex)
        .ok_or_else(|| IndexError::Damaged(name.to_string()))
}

//! The account's index: every stored file's manifest, kept in the mesh.
//!
//! Without it, losing a manifest loses the file. The index is a collection in
//! the [`version_graph`](crate::version_graph): one entry per file, named by
//! the file's name, with its manifest and when its leases need renewing. Its
//! newest commit is named by the account's pointer, so a new device holding
//! only the account's secret reads the pointer, then the index, then any
//! file.
//!
//! All devices of an account move the same pointer. When a device finds a
//! version there it has not seen, another device saved in the meantime: it
//! fetches that head, merges it with its own, and writes the merge one
//! version higher. Two devices adding files while apart therefore both keep
//! their files.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::account::Account;
use crate::batch_renew::{BatchRenewal, renew_pieces};
use crate::commits::{
    FetchCommitError, PublishHeadError, ReadHeadError, fetch_new, publish_head, read_head,
    store_commit,
};
use crate::erasure::DATA_PIECES;
use crate::manifest::Manifest;
use crate::merge::{MergeOutcome, merge};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::pointers::{ReadPointerError, WritePointerError};
use crate::upload::UploadError;
use crate::version_graph::{Change, Commit, CommitRef, Entries, Graph, MissingCommit};

/// The field of an entry holding the file's manifest.
pub const FIELD_MANIFEST: u32 = 1;
/// The field holding the file's size in bytes.
pub const FIELD_SIZE: u32 = 2;
/// The field holding when the file's leases need renewing, in Unix seconds.
pub const FIELD_RENEW_BY: u32 = 3;

/// How often a save retries after another device moved the pointer first.
/// Each retry merges what that device saved, so only devices saving many
/// times a second at once could use them all up.
const MAX_SAVE_ATTEMPTS: usize = 5;

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
}

/// The index as this device knows it: the graph, its head, and the newest
/// pointer version it has seen. It is kept on the device between runs.
#[derive(Default, Serialize, Deserialize)]
pub struct Index {
    graph: Graph,
    head: Option<CommitRef>,
    version: u64,
}

impl Index {
    /// Every file in the index, by name.
    pub fn files(&self) -> Result<Vec<IndexedFile>, IndexError> {
        self.entries()?
            .iter()
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

    /// Renews everything the account keeps for `session.lease`: every file's
    /// pieces, the index's own commits, and its pointer. Files whose pieces
    /// all held get the new renewal date.
    ///
    /// All pieces go out in one request per node, however many files there
    /// are. The index's commits are renewed with them; letting those expire
    /// would lose the list of files while the files themselves live on.
    pub async fn renew(&mut self, session: &Session<'_>) -> Result<IndexRenewal, IndexError> {
        self.pull(session).await?;
        let files = self.files()?;
        let commit_manifests = self.commit_manifests()?;
        let renewed_until = unix_now() + session.lease.as_secs();
        let pieces = files
            .iter()
            .map(|file| &file.manifest)
            .chain(&commit_manifests)
            .flat_map(|manifest| &manifest.chunks)
            .flat_map(|chunk| &chunk.pieces);
        let renewal = renew_pieces(session.client, pieces, session.lease).await;
        let (safe, at_risk): (Vec<IndexedFile>, Vec<IndexedFile>) = files
            .into_iter()
            .partition(|file| is_readable(&file.manifest, &renewal));
        let changes: Vec<Change> = safe
            .iter()
            .map(|file| set(&file.name, FIELD_RENEW_BY, json!(renewed_until)))
            .collect();
        // Saving writes the pointer anew, which renews it too.
        if changes.is_empty() {
            self.publish(session).await?;
        } else {
            self.save(session, changes).await?;
        }
        Ok(IndexRenewal {
            pieces_renewed: renewal.renewed.len(),
            pieces_not_renewed: renewal.not_renewed.len(),
            at_risk,
        })
    }

    /// The manifests of every commit in the index's history.
    fn commit_manifests(&self) -> Result<Vec<Manifest>, IndexError> {
        let Some(head) = &self.head else {
            return Ok(Vec::new());
        };
        let mut manifests = vec![head.manifest.clone()];
        for id in self.graph.history(&head.id)? {
            let commit = self.graph.get(&id).ok_or(MissingCommit(id))?;
            manifests.extend(commit.parents.iter().map(|parent| parent.manifest.clone()));
        }
        Ok(manifests)
    }

    /// Brings this device's index up to date with the account's pointer,
    /// merging when both sides saved something the other lacks.
    pub async fn pull(&mut self, session: &Session<'_>) -> Result<(), IndexError> {
        let Some((remote, version)) = self.read_remote(session).await? else {
            return Ok(());
        };
        if self.integrate(session, remote, version).await? {
            self.publish(session).await?;
        }
        Ok(())
    }

    /// Commits `changes` on top of this device's head and moves the
    /// account's pointer to it.
    pub async fn save(
        &mut self,
        session: &Session<'_>,
        changes: Vec<Change>,
    ) -> Result<(), IndexError> {
        let commit = Commit {
            parents: self.head.iter().cloned().collect(),
            device: session.device.clone(),
            made_at: unix_now(),
            changes,
        };
        self.head = Some(self.store(session, commit).await?);
        self.publish(session).await
    }

    /// Moves the account's pointer to this device's head. When another
    /// device moved it first, merges that device's head in and tries again.
    async fn publish(&mut self, session: &Session<'_>) -> Result<(), IndexError> {
        for _ in 0..MAX_SAVE_ATTEMPTS {
            // `save` and `integrate` set a head before publishing.
            let Some(head) = self.head.clone() else {
                return Ok(());
            };
            let account = session.account;
            let published = publish_head(
                session.client,
                session.list,
                &account.pointer_key(),
                &account.message_key(),
                &head,
                self.version + 1,
                session.lease,
            )
            .await;
            match published {
                Ok(_) => {
                    self.version += 1;
                    return Ok(());
                }
                Err(PublishHeadError::Write(WritePointerError::NotNewest { .. })) => {}
                Err(error) => return Err(error.into()),
            }
            let Some((remote, version)) = self.read_remote(session).await? else {
                continue;
            };
            self.integrate(session, remote, version).await?;
        }
        Err(IndexError::Contended)
    }

    /// The head the account's pointer names and its version, or `None` when
    /// nothing was published yet.
    async fn read_remote(
        &self,
        session: &Session<'_>,
    ) -> Result<Option<(CommitRef, u64)>, IndexError> {
        let account = session.account;
        // A device that has seen version N must never accept less: that is
        // a node holding back newer saves.
        let last_seen = (self.version > 0).then_some(self.version);
        let read = read_head(
            session.client,
            session.list,
            &account.pointer_owner(),
            &account.message_key(),
            last_seen,
        )
        .await;
        match read {
            Ok(remote) => Ok(Some(remote)),
            Err(ReadHeadError::Pointer(ReadPointerError::NotFound)) if last_seen.is_none() => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Takes in the head `remote` at pointer `version`, merging it with this
    /// device's head. Returns whether the result is a head the pointer does
    /// not name yet.
    async fn integrate(
        &mut self,
        session: &Session<'_>,
        remote: CommitRef,
        version: u64,
    ) -> Result<bool, IndexError> {
        fetch_new(session.client, &mut self.graph, &remote).await?;
        self.version = self.version.max(version);
        let Some(local) = &self.head else {
            self.head = Some(remote);
            return Ok(false);
        };
        match merge(&self.graph, local, &remote)? {
            MergeOutcome::FastForward(head) => {
                let is_new = head.id != remote.id;
                self.head = Some(head);
                Ok(is_new)
            }
            MergeOutcome::Merged { commit, .. } => {
                self.head = Some(self.store(session, commit).await?);
                Ok(true)
            }
        }
    }

    /// Stores `commit`, adds it to the graph and returns its reference.
    async fn store(
        &mut self,
        session: &Session<'_>,
        commit: Commit,
    ) -> Result<CommitRef, IndexError> {
        let reference = store_commit(
            session.client,
            session.list,
            &session.account.dedup_key(),
            &commit,
            session.lease,
        )
        .await?;
        self.graph.insert(reference.id, commit);
        Ok(reference)
    }

    fn entries(&self) -> Result<Entries, IndexError> {
        match &self.head {
            Some(head) => Ok(self.graph.entries_at(&head.id)?),
            None => Ok(Entries::new()),
        }
    }
}

/// What renewing the index achieved.
pub struct IndexRenewal {
    pub pieces_renewed: usize,
    pub pieces_not_renewed: usize,
    /// Files with a chunk that has fewer than four renewed pieces. They are
    /// unreadable from their old `renew_by` on unless repaired first.
    pub at_risk: Vec<IndexedFile>,
}

/// Whether every chunk of `manifest` has enough renewed pieces to rebuild it.
fn is_readable(manifest: &Manifest, renewal: &BatchRenewal) -> bool {
    manifest.chunks.iter().all(|chunk| {
        let renewed = chunk
            .pieces
            .iter()
            .filter(|piece| renewal.is_renewed(piece))
            .count();
        renewed >= DATA_PIECES
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

/// The current Unix time in seconds.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        // Only a clock set before 1970 fails here, and leases can't work on one.
        .expect("the system clock is after 1970")
        .as_secs()
}

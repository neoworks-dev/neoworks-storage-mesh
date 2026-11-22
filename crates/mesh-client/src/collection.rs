//! A collection of structured data in the mesh, under keys of its own.
//!
//! A collection is a [`version_graph`](crate::version_graph) whose commits
//! are stored in the mesh and whose newest commit is named by a signed
//! pointer. Two keys belong to it:
//!
//! - the **read key** seals the pointer's target, which names the newest
//!   commit and so, through the commits, every manifest and chunk key in the
//!   collection;
//! - the **head key** signs the pointer, so only those who hold it can move
//!   the collection on.
//!
//! Each collection has random keys of its own, kept in the account's index,
//! so giving someone the read key and where the pointer is
//! ([`CollectionAccess`]) lets them read that collection and nothing else.
//! The index itself is a collection too, under keys derived from the
//! account's secret, since it is what holds every other key.
//!
//! All writers move the same pointer. When one finds a version there it has
//! not seen, another saved in the meantime: it fetches that head, merges it
//! with its own, and writes the merge one version higher.

use std::time::Duration;

use mesh_core::pointer::SigningKey;
use serde::{Deserialize, Serialize};

use crate::chunk_registration::{RegisterError, register};
use crate::commits::{
    FetchCommitError, PublishHeadError, ReadHeadError, fetch_new, publish_head, read_head,
    store_commit,
};
use crate::encryption::{DedupKey, MessageKey};
use crate::manifest::Manifest;
use crate::merge::{MergeOutcome, merge};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::pointers::{ReadPointerError, WritePointerError};
use crate::upload::UploadError;
use crate::version_graph::{Change, Commit, CommitRef, Entries, Graph, MissingCommit};

/// How often a save retries after another writer moved the pointer first.
/// Each retry merges what that writer saved, so only writers saving many
/// times a second at once could use them all up.
const MAX_SAVE_ATTEMPTS: usize = 5;

/// Both keys of a collection: what writing to it needs.
#[derive(Clone)]
pub struct CollectionKeys {
    pub head_key: SigningKey,
    pub read_key: MessageKey,
}

/// What reading a collection needs, and all that sharing it hands over:
/// the read key and the public key the pointer is filed under.
#[derive(Clone, Serialize, Deserialize)]
pub struct CollectionAccess {
    #[serde(with = "hex")]
    pub head_owner: [u8; 32],
    #[serde(with = "hex")]
    pub read_key: [u8; 32],
}

impl CollectionKeys {
    /// New random keys for a new collection.
    pub fn generate() -> Self {
        let mut head = [0u8; 32];
        getrandom::fill(&mut head).expect("the operating system's random source works");
        CollectionKeys {
            head_key: SigningKey::from_bytes(&head),
            read_key: MessageKey::generate(),
        }
    }

    /// What someone needs to read the collection but not write to it.
    pub fn access(&self) -> CollectionAccess {
        CollectionAccess {
            head_owner: self.head_key.verifying_key().to_bytes(),
            read_key: self.read_key.to_bytes(),
        }
    }

    /// The keys as 128 hex digits, head key first, for keeping them.
    pub fn to_hex(&self) -> String {
        format!(
            "{}{}",
            hex::encode(self.head_key.to_bytes()),
            hex::encode(self.read_key.to_bytes())
        )
    }

    /// Keys written by [`to_hex`](Self::to_hex).
    pub fn from_hex(text: &str) -> Option<Self> {
        let mut bytes = [0u8; 64];
        hex::decode_to_slice(text, &mut bytes).ok()?;
        let (head, read) = bytes.split_at(32);
        Some(CollectionKeys {
            head_key: SigningKey::from_bytes(head.try_into().ok()?),
            read_key: MessageKey::from_bytes(read.try_into().ok()?),
        })
    }
}

/// Where a collection is kept and who writes to it.
pub struct Writer<'a> {
    pub client: &'a NodeClient,
    pub list: &'a NodeList,
    pub keys: CollectionKeys,
    /// The writer's own dedup key, which its commits are stored under.
    pub dedup_key: DedupKey,
    /// The writer's device name, recorded in its commits.
    pub device: String,
    /// How long commits and the pointer are stored.
    pub lease: Duration,
    /// The coordinator to register commits with, so it repairs them while
    /// the writer's devices are off; `None` for none.
    pub coordinator: Option<&'a str>,
}

/// Why a collection could not be read or saved.
#[derive(Debug, thiserror::Error)]
pub enum CollectionError {
    #[error("storing a commit: {0}")]
    Store(#[from] UploadError),
    #[error("{0}")]
    Fetch(#[from] FetchCommitError),
    #[error("reading the collection's pointer: {0}")]
    ReadHead(#[from] ReadHeadError),
    #[error("moving the collection's pointer: {0}")]
    PublishHead(#[from] PublishHeadError),
    #[error("{0}")]
    Missing(#[from] MissingCommit),
    #[error("other writers kept moving the pointer; try again")]
    Contended,
    #[error("registering a commit for repair: {0}")]
    Register(#[from] RegisterError),
}

/// A collection as this device knows it: the graph, its head, and the newest
/// pointer version it has seen. It is kept on the device between runs.
#[derive(Default, Serialize, Deserialize)]
pub struct Collection {
    graph: Graph,
    head: Option<CommitRef>,
    version: u64,
}

impl Collection {
    /// Every entry as of this device's head.
    pub fn entries(&self) -> Result<Entries, CollectionError> {
        match &self.head {
            Some(head) => Ok(self.graph.entries_at(&head.id)?),
            None => Ok(Entries::new()),
        }
    }

    /// The manifests of every commit in the collection's history, which
    /// renewing has to keep alive with the data.
    pub fn commit_manifests(&self) -> Result<Vec<Manifest>, CollectionError> {
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

    /// Brings this device's copy up to date with the pointer, merging when
    /// both sides saved something the other lacks.
    pub async fn pull(&mut self, writer: &Writer<'_>) -> Result<(), CollectionError> {
        let access = writer.keys.access();
        let Some((remote, version)) = self
            .read_remote(writer.client, writer.list, &access)
            .await?
        else {
            return Ok(());
        };
        if self.integrate(writer, remote, version).await? {
            self.publish(writer).await?;
        }
        Ok(())
    }

    /// Brings a read-only copy up to date with the pointer of the collection
    /// `access` opens. A reader never writes, so it only ever moves to the
    /// head it finds.
    pub async fn follow(
        &mut self,
        client: &NodeClient,
        list: &NodeList,
        access: &CollectionAccess,
    ) -> Result<(), CollectionError> {
        let Some((remote, version)) = self.read_remote(client, list, access).await? else {
            return Ok(());
        };
        fetch_new(client, list, &mut self.graph, &remote).await?;
        self.head = Some(remote);
        self.version = self.version.max(version);
        Ok(())
    }

    /// Commits `changes` on top of this device's head and moves the pointer
    /// to it.
    pub async fn save(
        &mut self,
        writer: &Writer<'_>,
        changes: Vec<Change>,
    ) -> Result<(), CollectionError> {
        let commit = Commit {
            parents: self.head.iter().cloned().collect(),
            device: writer.device.clone(),
            made_at: unix_now(),
            changes,
        };
        self.head = Some(self.store(writer, commit).await?);
        self.publish(writer).await
    }

    /// Moves the pointer to this device's head. When another writer moved it
    /// first, merges that writer's head in and tries again. Writing the
    /// pointer also renews it.
    pub async fn publish(&mut self, writer: &Writer<'_>) -> Result<(), CollectionError> {
        let access = writer.keys.access();
        for _ in 0..MAX_SAVE_ATTEMPTS {
            // `save` and `integrate` set a head before publishing.
            let Some(head) = self.head.clone() else {
                return Ok(());
            };
            let published = publish_head(
                writer.client,
                writer.list,
                &writer.keys.head_key,
                &writer.keys.read_key,
                &head,
                self.version + 1,
                writer.lease,
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
            let read = self
                .read_remote(writer.client, writer.list, &access)
                .await?;
            let Some((remote, version)) = read else {
                continue;
            };
            self.integrate(writer, remote, version).await?;
        }
        Err(CollectionError::Contended)
    }

    /// The head the pointer names and its version, or `None` when nothing
    /// was published yet.
    async fn read_remote(
        &self,
        client: &NodeClient,
        list: &NodeList,
        access: &CollectionAccess,
    ) -> Result<Option<(CommitRef, u64)>, CollectionError> {
        // A device that has seen version N must never accept less: that is
        // a node holding back newer saves.
        let last_seen = (self.version > 0).then_some(self.version);
        let read_key = MessageKey::from_bytes(access.read_key);
        let read = read_head(client, list, &access.head_owner, &read_key, last_seen).await;
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
        writer: &Writer<'_>,
        remote: CommitRef,
        version: u64,
    ) -> Result<bool, CollectionError> {
        fetch_new(writer.client, writer.list, &mut self.graph, &remote).await?;
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
                self.head = Some(self.store(writer, commit).await?);
                Ok(true)
            }
        }
    }

    /// Stores `commit`, registers it for repair, adds it to the graph and
    /// returns its reference.
    ///
    /// Registering comes before the pointer names the commit, so a failed
    /// registration leaves nothing published that the coordinator doesn't
    /// look after.
    async fn store(
        &mut self,
        writer: &Writer<'_>,
        commit: Commit,
    ) -> Result<CommitRef, CollectionError> {
        let reference = store_commit(
            writer.client,
            writer.list,
            &writer.dedup_key,
            &commit,
            writer.lease,
        )
        .await?;
        if let Some(coordinator) = writer.coordinator {
            let expires_at = unix_now() + writer.lease.as_secs();
            register(writer.client, coordinator, &reference.manifest, expires_at).await?;
        }
        self.graph.insert(reference.id, commit);
        Ok(reference)
    }
}

/// The current Unix time in seconds.
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        // Only a clock set before 1970 fails here, and leases can't work on one.
        .expect("the system clock is after 1970")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_survive_being_kept_as_hex() {
        let keys = CollectionKeys::generate();

        let again = CollectionKeys::from_hex(&keys.to_hex()).unwrap();

        assert_eq!(again.head_key.to_bytes(), keys.head_key.to_bytes());
        assert_eq!(again.read_key.to_bytes(), keys.read_key.to_bytes());
        assert!(CollectionKeys::from_hex("abc").is_none());
    }

    #[test]
    fn every_collection_gets_keys_of_its_own() {
        let one = CollectionKeys::generate().access();
        let other = CollectionKeys::generate().access();

        assert_ne!(one.head_owner, other.head_owner);
        assert_ne!(one.read_key, other.read_key);
    }
}

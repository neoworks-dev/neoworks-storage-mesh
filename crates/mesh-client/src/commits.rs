//! Storing commits in the mesh, fetching the ones a device lacks, and moving
//! each device's head pointer.
//!
//! A commit is stored as one object, encrypted and erasure-coded like a file.
//! Each device has its own signed pointer per collection, whose target is the
//! reference to its newest commit, sealed under the collection's message key
//! so nodes cannot read it. To sync, a device reads another device's pointer
//! and walks back from that head until it reaches commits it already has,
//! like `git fetch`.

use std::time::Duration;

use mesh_core::pointer::{Pointer, PointerError, SigningKey};

use crate::download::{DownloadError, download};
use crate::encryption::{DedupKey, MessageKey, open_message, seal_message};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::pointers::{ReadPointerError, WritePointerError, read_pointer, write_pointer};
use crate::upload::{UploadError, upload};
use crate::version_graph::{Commit, CommitId, CommitRef, Graph};

/// A commit that could not be fetched.
#[derive(Debug, thiserror::Error)]
pub enum FetchCommitError {
    #[error("downloading commit {id:?}: {source}")]
    Download { id: CommitId, source: DownloadError },
    #[error("commit {0:?} came back as other bytes")]
    WrongBytes(CommitId),
    #[error("commit {0:?} is not a commit this client understands")]
    Malformed(CommitId),
}

/// A head that could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ReadHeadError {
    #[error("{0}")]
    Pointer(#[from] ReadPointerError),
    #[error("the head pointer does not open with the collection's key")]
    Unreadable,
}

/// A head that could not be moved.
#[derive(Debug, thiserror::Error)]
pub enum PublishHeadError {
    #[error("{0}")]
    TooLarge(#[from] PointerError),
    #[error("{0}")]
    Write(#[from] WritePointerError),
}

/// Stores `commit` as an object and returns how to find it.
///
/// The id is the hash of the exact bytes stored, so whoever fetches the
/// commit checks those bytes, not a re-serialization that could differ.
pub async fn store_commit(
    client: &NodeClient,
    list: &NodeList,
    dedup_key: &DedupKey,
    commit: &Commit,
    lease: Duration,
) -> Result<CommitRef, UploadError> {
    // A commit is plain strings, numbers and JSON values, which always
    // serialize.
    let bytes = serde_json::to_vec(commit).expect("a commit serializes");
    let manifest = upload(client, list, dedup_key, bytes.as_slice(), lease, &[]).await?;
    Ok(CommitRef {
        id: CommitId::of(&bytes),
        manifest,
    })
}

/// Fetches the commit `reference` names from nodes in `list` and checks it is
/// that commit.
pub async fn fetch_commit(
    client: &NodeClient,
    list: &NodeList,
    reference: &CommitRef,
) -> Result<Commit, FetchCommitError> {
    let id = reference.id;
    let mut bytes = Vec::new();
    download(client, list, &reference.manifest, &mut bytes)
        .await
        .map_err(|source| FetchCommitError::Download { id, source })?;
    if CommitId::of(&bytes) != id {
        return Err(FetchCommitError::WrongBytes(id));
    }
    serde_json::from_slice(&bytes).map_err(|_| FetchCommitError::Malformed(id))
}

/// Fetches every commit `head` follows that `graph` lacks, adds them, and
/// returns how many there were.
pub async fn fetch_new(
    client: &NodeClient,
    list: &NodeList,
    graph: &mut Graph,
    head: &CommitRef,
) -> Result<usize, FetchCommitError> {
    let mut fetched = 0;
    let mut wanted = vec![head.clone()];
    while let Some(reference) = wanted.pop() {
        if graph.contains(&reference.id) {
            continue;
        }
        let commit = fetch_commit(client, list, &reference).await?;
        wanted.extend(commit.parents.iter().cloned());
        graph.insert(reference.id, commit);
        fetched += 1;
    }
    Ok(fetched)
}

/// Moves the head pointer of `device_key` to `head` at `version`.
pub async fn publish_head(
    client: &NodeClient,
    list: &NodeList,
    device_key: &SigningKey,
    message_key: &MessageKey,
    head: &CommitRef,
    version: u64,
    lease: Duration,
) -> Result<usize, PublishHeadError> {
    // A reference is a manifest and an id, which always serialize.
    let reference = serde_json::to_vec(head).expect("a commit reference serializes");
    let target = seal_message(message_key, &reference);
    let pointer = Pointer::sign(device_key, version, target)?;
    Ok(write_pointer(client, list, &pointer, lease).await?)
}

/// The head another device published under `device`, its public key, and
/// the pointer's version.
pub async fn read_head(
    client: &NodeClient,
    list: &NodeList,
    device: &[u8; 32],
    message_key: &MessageKey,
    last_seen: Option<u64>,
) -> Result<(CommitRef, u64), ReadHeadError> {
    let read = read_pointer(client, list, device, last_seen).await?;
    let reference =
        open_message(message_key, &read.pointer.target).map_err(|_| ReadHeadError::Unreadable)?;
    let head = serde_json::from_slice(&reference).map_err(|_| ReadHeadError::Unreadable)?;
    Ok((head, read.pointer.version))
}

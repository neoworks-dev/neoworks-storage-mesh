//! The pieces a node holds and how long it holds them.
//!
//! The bytes live either in the node's data directory
//! ([`crate::piece_directory`]) or in an S3-compatible bucket
//! ([`crate::bucket`]), one object per piece named by its id. Either way the
//! leases stay in the data directory:
//!
//! ```text
//! <data>/leases.sqlite3   when each piece's lease ends, and its length
//! ```
//!
//! The lease table decides what the node holds: a piece whose lease has ended
//! is gone to every request, even before a sweep deletes its bytes. Since
//! only the table is local, a node keeping pieces in a bucket can be moved to
//! another machine with its data directory and take the bucket over. The
//! node never looks inside a piece; to it, every piece is noise of some
//! length.

use std::io;
use std::path::Path;

use mesh_core::PieceId;
use tokio::sync::Mutex;

use crate::bucket::{Bucket, BucketError};
use crate::lease_table::LeaseTable;
use crate::piece_directory::PieceDirectory;

/// Pieces and their leases.
pub struct PieceStore {
    bytes: PieceBytes,
    /// One lock orders every change to a piece's bytes and lease. Without it,
    /// a sweep could delete the bytes of a piece that an upload renewed a
    /// moment before, losing a piece whose lease the node had just confirmed.
    leases: Mutex<LeaseTable>,
}

/// Where a store keeps the pieces' bytes.
enum PieceBytes {
    Directory(PieceDirectory),
    Bucket(Bucket),
}

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("the bytes do not hash to the piece id")]
    WrongId,
    #[error("piece file: {0}")]
    Io(#[from] io::Error),
    #[error("bucket: {0}")]
    Bucket(#[from] BucketError),
    #[error("lease table: {0}")]
    Leases(#[from] rusqlite::Error),
}

impl PieceStore {
    /// Opens the store in `directory`, keeping pieces as files there,
    /// creating what is missing and deleting uploads that a previous run left
    /// unfinished.
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self, StoreError> {
        let directory = directory.as_ref();
        let pieces = PieceDirectory::open(directory).await?;
        Self::with_bytes(directory, PieceBytes::Directory(pieces))
    }

    /// Opens the store with its leases in `directory` and its pieces in
    /// `bucket`.
    pub async fn open_with_bucket(
        directory: impl AsRef<Path>,
        bucket: Bucket,
    ) -> Result<Self, StoreError> {
        let directory = directory.as_ref();
        tokio::fs::create_dir_all(directory).await?;
        Self::with_bytes(directory, PieceBytes::Bucket(bucket))
    }

    fn with_bytes(directory: &Path, bytes: PieceBytes) -> Result<Self, StoreError> {
        let leases = LeaseTable::open(&directory.join("leases.sqlite3"))?;
        Ok(PieceStore {
            bytes,
            leases: Mutex::new(leases),
        })
    }

    /// Stores `bytes` as the piece `id` until at least `expires_at`, and
    /// returns when its lease ends.
    ///
    /// Storing a piece the node already has only extends its lease: the id is
    /// the hash of the bytes, so the bytes are the same.
    pub async fn put(
        &self,
        id: &PieceId,
        bytes: &[u8],
        expires_at: u64,
    ) -> Result<u64, StoreError> {
        // Without this check a client could store any bytes under any id, and
        // other clients would fetch garbage under a name they trust.
        if PieceId::of(bytes) != *id {
            return Err(StoreError::WrongId);
        }
        // The lease is written before the bytes. If the node stops in
        // between, the lease points at missing bytes, which read as "not
        // held" and which the sweep cleans up; the other order would leave
        // bytes no lease ever deletes.
        let lease_end = {
            let leases = self.leases.lock().await;
            let lease_end = leases.extend(id, expires_at)?;
            leases.record_len(id, bytes.len() as u64)?;
            lease_end
        };
        // The bytes are written without the lock, so a slow upload to a
        // bucket holds up no other request. The sweep can't delete them
        // meanwhile: it deletes only what has ended, and this lease runs
        // now, and a sweep already under way finished before the lease was
        // written, since it holds the lock throughout.
        if !self.contains(id).await? {
            self.write(id, bytes).await?;
        }
        Ok(lease_end)
    }

    /// Makes the lease of `id` end no sooner than `expires_at`, and returns
    /// when it ends. `None` when the node does not hold the piece at `now`.
    pub async fn renew(
        &self,
        id: &PieceId,
        expires_at: u64,
        now: u64,
    ) -> Result<Option<u64>, StoreError> {
        let leases = self.leases.lock().await;
        // A renewal confirms the node can return the piece until the new end,
        // so it needs a lease that still runs and the bytes themselves. A
        // lease that has ended is not revived, since its bytes may be gone
        // already.
        let is_held = leases.expires_at(id)?.is_some_and(|end| end > now);
        if !is_held || !self.renewable(id).await? {
            return Ok(None);
        }
        Ok(Some(leases.extend(id, expires_at)?))
    }

    /// The bytes of the piece `id`, or `None` when the node does not hold it
    /// at `now`.
    pub async fn get(&self, id: &PieceId, now: u64) -> Result<Option<Vec<u8>>, StoreError> {
        if !self.is_held(id, now).await? {
            return Ok(None);
        }
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.read(id).await?),
            PieceBytes::Bucket(bucket) => Ok(bucket.get(&id.to_string()).await?),
        }
    }

    /// The length of the piece `id`, or `None` when the node does not hold it
    /// at `now`.
    pub async fn len_of(&self, id: &PieceId, now: u64) -> Result<Option<u64>, StoreError> {
        if !self.is_held(id, now).await? {
            return Ok(None);
        }
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.len_of(id).await?),
            PieceBytes::Bucket(bucket) => {
                // The recorded length saves a request to the bucket; pieces
                // stored before lengths were recorded ask the bucket.
                if let Some(len) = self.leases.lock().await.len_of(id)? {
                    return Ok(Some(len));
                }
                Ok(bucket.len_of(&id.to_string()).await?)
            }
        }
    }

    /// Deletes every piece whose lease ended at or before `now`, and returns
    /// how many there were.
    pub async fn remove_ended(&self, now: u64) -> Result<usize, StoreError> {
        let leases = self.leases.lock().await;
        let ended = leases.ended(now)?;
        match &self.bytes {
            PieceBytes::Directory(directory) => directory.remove(&ended).await?,
            // A busy node ends thousands of leases a day; deleting them in
            // batches of a thousand keeps that to a few requests.
            PieceBytes::Bucket(bucket) => {
                let keys: Vec<String> = ended.iter().map(PieceId::to_string).collect();
                bucket.delete_many(&keys).await?;
            }
        }
        for id in &ended {
            leases.remove(id)?;
        }
        Ok(ended.len())
    }

    /// The bytes all pieces take together. Pieces whose lease ended but which
    /// the sweep has not deleted yet count, since they still take space.
    ///
    /// A directory is counted file by file, so uploads in progress are left
    /// out. A bucket is counted from the lengths in the lease table, since
    /// listing a bucket of millions of objects takes thousands of requests.
    pub async fn stored_bytes(&self) -> Result<u64, StoreError> {
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.stored_bytes().await?),
            PieceBytes::Bucket(_) => Ok(self.leases.lock().await.total_len()?),
        }
    }

    /// Whether the lease of `id` still runs at `now`.
    async fn is_held(&self, id: &PieceId, now: u64) -> Result<bool, StoreError> {
        let lease_end = self.leases.lock().await.expires_at(id)?;
        Ok(lease_end.is_some_and(|end| end > now))
    }

    /// Whether the bytes of `id` are there.
    async fn contains(&self, id: &PieceId) -> Result<bool, StoreError> {
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.contains(id).await?),
            PieceBytes::Bucket(bucket) => Ok(bucket.len_of(&id.to_string()).await?.is_some()),
        }
    }

    /// Whether a renewal of `id` may go ahead as far as its bytes go.
    ///
    /// A directory is checked for the file, since a disk can lose one. A
    /// bucket is not asked: it keeps what it was given, and asking for each
    /// of the thousands of pieces in a batch renewal would take a request
    /// each.
    async fn renewable(&self, id: &PieceId) -> Result<bool, StoreError> {
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.contains(id).await?),
            PieceBytes::Bucket(_) => Ok(true),
        }
    }

    async fn write(&self, id: &PieceId, bytes: &[u8]) -> Result<(), StoreError> {
        match &self.bytes {
            PieceBytes::Directory(directory) => Ok(directory.write(id, bytes).await?),
            PieceBytes::Bucket(bucket) => Ok(bucket.put(&id.to_string(), bytes).await?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (tempfile::TempDir, PieceStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = PieceStore::open(directory.path()).await.unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn stores_and_returns_a_piece_while_its_lease_lasts() {
        let (_directory, store) = store().await;
        let bytes = b"some encrypted piece".to_vec();
        let id = PieceId::of(&bytes);

        assert_eq!(store.put(&id, &bytes, 1_000).await.unwrap(), 1_000);

        assert_eq!(store.get(&id, 999).await.unwrap(), Some(bytes));
        assert_eq!(store.get(&id, 1_000).await.unwrap(), None);
    }

    #[tokio::test]
    async fn refuses_bytes_that_do_not_match_the_id() {
        let (_directory, store) = store().await;
        let id = PieceId::of(b"the real piece");

        let result = store.put(&id, b"something else", 1_000).await;

        assert!(matches!(result, Err(StoreError::WrongId)));
        assert_eq!(store.get(&id, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn renewing_extends_but_never_shortens() {
        let (_directory, store) = store().await;
        let bytes = b"piece".to_vec();
        let id = PieceId::of(&bytes);
        store.put(&id, &bytes, 1_000).await.unwrap();

        assert_eq!(store.renew(&id, 2_000, 500).await.unwrap(), Some(2_000));
        assert_eq!(store.renew(&id, 1_500, 600).await.unwrap(), Some(2_000));
        assert_eq!(store.get(&id, 1_999).await.unwrap(), Some(bytes));
    }

    #[tokio::test]
    async fn an_ended_or_unknown_lease_is_not_renewed() {
        let (_directory, store) = store().await;
        let bytes = b"piece".to_vec();
        let id = PieceId::of(&bytes);
        store.put(&id, &bytes, 1_000).await.unwrap();

        assert_eq!(store.renew(&id, 5_000, 1_000).await.unwrap(), None);
        let unknown = PieceId::of(b"never stored");
        assert_eq!(store.renew(&unknown, 5_000, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_piece_whose_file_is_lost_is_not_renewed() {
        let (directory, store) = store().await;
        let bytes = b"piece".to_vec();
        let id = PieceId::of(&bytes);
        store.put(&id, &bytes, 1_000).await.unwrap();
        std::fs::remove_file(directory.path().join("pieces").join(id.to_string())).unwrap();

        assert_eq!(store.renew(&id, 5_000, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn removing_ended_pieces_deletes_their_files() {
        let (directory, store) = store().await;
        let short = b"short-lived".to_vec();
        let long = b"long-lived".to_vec();
        store.put(&PieceId::of(&short), &short, 100).await.unwrap();
        store.put(&PieceId::of(&long), &long, 200).await.unwrap();

        assert_eq!(store.remove_ended(150).await.unwrap(), 1);

        let files = std::fs::read_dir(directory.path().join("pieces"))
            .unwrap()
            .count();
        assert_eq!(files, 1);
        assert_eq!(
            store.get(&PieceId::of(&long), 150).await.unwrap(),
            Some(long)
        );
    }

    #[tokio::test]
    async fn opening_deletes_unfinished_uploads_and_keeps_pieces() {
        let (directory, store) = store().await;
        let bytes = b"piece".to_vec();
        let id = PieceId::of(&bytes);
        store.put(&id, &bytes, 1_000).await.unwrap();
        drop(store);
        let pieces_directory = directory.path().join("pieces");
        let partial_path = pieces_directory.join(format!("{id}.0.partial"));
        std::fs::write(&partial_path, b"half a pie").unwrap();

        let store = PieceStore::open(directory.path()).await.unwrap();

        assert!(!partial_path.exists());
        assert_eq!(store.get(&id, 0).await.unwrap(), Some(bytes));
    }
}

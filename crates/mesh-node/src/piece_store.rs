//! The pieces a node holds and how long it holds them.
//!
//! A node's data directory looks like this:
//!
//! ```text
//! <data>/pieces/<64 hex digits>             one file per piece, named by its id
//! <data>/pieces/<64 hex digits>.<n>.partial an upload still being written
//! <data>/leases.sqlite3                     when each piece's lease ends
//! ```
//!
//! The lease table decides what the node holds: a piece whose lease has ended
//! is gone to every request, even before a sweep deletes its file. The node
//! never looks inside a piece; to it, every piece is noise of some length.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mesh_core::PieceId;
use tokio::sync::Mutex;

use crate::lease_table::LeaseTable;

/// Pieces on disk and their leases.
pub struct PieceStore {
    pieces_directory: PathBuf,
    /// One lock orders every change to a piece's file and lease. Without it, a
    /// sweep could delete the file of a piece that an upload renewed a moment
    /// before, losing a piece whose lease the node had just confirmed.
    leases: Mutex<LeaseTable>,
    /// Numbers the temporary files of uploads in progress, so two uploads of
    /// the same piece at once never write to the same temporary file.
    next_upload: AtomicU64,
}

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("the bytes do not hash to the piece id")]
    WrongId,
    #[error("piece file: {0}")]
    Io(#[from] io::Error),
    #[error("lease table: {0}")]
    Leases(#[from] rusqlite::Error),
}

impl PieceStore {
    /// Opens the store in `directory`, creating what is missing and deleting
    /// uploads that a previous run left unfinished.
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self, StoreError> {
        let directory = directory.as_ref();
        let pieces_directory = directory.join("pieces");
        tokio::fs::create_dir_all(&pieces_directory).await?;
        remove_partial_uploads(&pieces_directory).await?;
        let leases = LeaseTable::open(&directory.join("leases.sqlite3"))?;
        Ok(PieceStore {
            pieces_directory,
            leases: Mutex::new(leases),
            next_upload: AtomicU64::new(0),
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
        let leases = self.leases.lock().await;
        // The lease is written before the file. If the node stops in between,
        // the lease points at a missing file, which reads as "not held" and
        // which the sweep cleans up; the other order would leave a file no
        // lease ever deletes.
        let lease_end = leases.extend(id, expires_at)?;
        let path = self.path_of(id);
        if !tokio::fs::try_exists(&path).await? {
            self.write_whole(id, &path, bytes).await?;
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
        // so it needs a lease that still runs and the file itself. A lease
        // that has ended is not revived, since its file may be gone already.
        let is_held = leases.expires_at(id)?.is_some_and(|end| end > now);
        if !is_held || !tokio::fs::try_exists(self.path_of(id)).await? {
            return Ok(None);
        }
        Ok(Some(leases.extend(id, expires_at)?))
    }

    /// The bytes of the piece `id`, or `None` when the node does not hold it
    /// at `now`.
    pub async fn get(&self, id: &PieceId, now: u64) -> Result<Option<Vec<u8>>, StoreError> {
        let lease_end = self.leases.lock().await.expires_at(id)?;
        let is_held = lease_end.is_some_and(|end| end > now);
        if !is_held {
            return Ok(None);
        }
        match tokio::fs::read(self.path_of(id)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The length of the piece `id`, or `None` when the node does not hold it
    /// at `now`.
    pub async fn len_of(&self, id: &PieceId, now: u64) -> Result<Option<u64>, StoreError> {
        let lease_end = self.leases.lock().await.expires_at(id)?;
        if !lease_end.is_some_and(|end| end > now) {
            return Ok(None);
        }
        match tokio::fs::metadata(self.path_of(id)).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Deletes every piece whose lease ended at or before `now`, and returns
    /// how many there were.
    pub async fn remove_ended(&self, now: u64) -> Result<usize, StoreError> {
        let leases = self.leases.lock().await;
        let ended = leases.ended(now)?;
        for id in &ended {
            match tokio::fs::remove_file(self.path_of(id)).await {
                Ok(()) => {}
                // A lease written just before the node stopped has no file.
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            leases.remove(id)?;
        }
        Ok(ended.len())
    }

    /// The bytes all piece files take together, counted from the directory.
    /// Uploads in progress are left out. Pieces whose lease ended but which
    /// the sweep has not deleted yet count, since they still take space.
    pub async fn stored_bytes(&self) -> io::Result<u64> {
        let mut entries = tokio::fs::read_dir(&self.pieces_directory).await?;
        let mut total = 0;
        while let Some(entry) = entries.next_entry().await? {
            // Finished pieces are named by their id alone; temporary files
            // of uploads carry a number and `.partial` after a dot.
            if entry.file_name().to_string_lossy().contains('.') {
                continue;
            }
            total += entry.metadata().await?.len();
        }
        Ok(total)
    }

    /// Writes `bytes` to `path` through a temporary file, so a reader sees
    /// either no piece or the whole piece, never half of one, even if the node
    /// stops mid-write.
    async fn write_whole(&self, id: &PieceId, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let upload_number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let temporary_path = self
            .pieces_directory
            .join(format!("{id}.{upload_number}.partial"));
        tokio::fs::write(&temporary_path, bytes).await?;
        tokio::fs::rename(&temporary_path, path).await
    }

    /// The file of the piece `id`. Ids are parsed to their canonical hex form
    /// before they get here, so the name can never contain `/` or `..`.
    fn path_of(&self, id: &PieceId) -> PathBuf {
        self.pieces_directory.join(id.to_string())
    }
}

/// Deletes the temporary files of uploads cut off when the node stopped.
///
/// Only `PieceStore::open` calls this, before the store takes any upload, so
/// every `.partial` file it finds belongs to a run that has ended. No lease
/// points at these files, so without this they would fill the disk for good.
async fn remove_partial_uploads(pieces_directory: &Path) -> io::Result<()> {
    let mut entries = tokio::fs::read_dir(pieces_directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let is_partial = path.extension().is_some_and(|end| end == "partial");
        if is_partial {
            tokio::fs::remove_file(path).await?;
        }
    }
    Ok(())
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

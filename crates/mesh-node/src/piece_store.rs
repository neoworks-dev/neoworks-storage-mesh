//! The pieces a node holds, one file per piece in a data directory.
//!
//! A piece is stored under its id, so the directory is flat:
//! `<data>/<64 hex digits>`. The node never looks inside a piece; to it, every
//! piece is noise of some length.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mesh_core::PieceId;

/// Pieces on disk, in one directory.
pub struct PieceStore {
    directory: PathBuf,
    /// Numbers the temporary files of uploads in progress, so two uploads of
    /// the same piece at once never write to the same temporary file.
    next_upload: AtomicU64,
}

/// Why a piece was not stored.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("the bytes do not hash to the piece id")]
    WrongId,
    #[error("could not write the piece: {0}")]
    Io(#[from] io::Error),
}

impl PieceStore {
    /// Opens the store in `directory`, creating the directory if needed.
    pub async fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&directory).await?;
        Ok(PieceStore {
            directory,
            next_upload: AtomicU64::new(0),
        })
    }

    /// Stores `bytes` as the piece `id`.
    ///
    /// Storing a piece the node already has is a no-op: the id is the hash of
    /// the bytes, so the bytes are the same.
    pub async fn put(&self, id: &PieceId, bytes: &[u8]) -> Result<(), PutError> {
        // Without this check a client could store any bytes under any id, and
        // other clients would fetch garbage under a name they trust.
        if PieceId::of(bytes) != *id {
            return Err(PutError::WrongId);
        }
        let path = self.path_of(id);
        if tokio::fs::try_exists(&path).await? {
            return Ok(());
        }
        // Writing to a temporary file and renaming it means a reader sees
        // either no piece or the whole piece, never half of one, even if the
        // node stops mid-write.
        let upload_number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let temporary_path = self.directory.join(format!("{id}.{upload_number}.partial"));
        tokio::fs::write(&temporary_path, bytes).await?;
        tokio::fs::rename(&temporary_path, &path).await?;
        Ok(())
    }

    /// The bytes of the piece `id`, or `None` when the node does not hold it.
    pub async fn get(&self, id: &PieceId) -> io::Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.path_of(id)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The file of the piece `id`. Ids are parsed to their canonical hex form
    /// before they get here, so the name can never contain `/` or `..`.
    fn path_of(&self, id: &PieceId) -> PathBuf {
        self.directory.join(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_returns_a_piece() {
        let directory = tempfile::tempdir().unwrap();
        let store = PieceStore::open(directory.path()).await.unwrap();
        let bytes = b"some encrypted piece".to_vec();
        let id = PieceId::of(&bytes);

        store.put(&id, &bytes).await.unwrap();

        assert_eq!(store.get(&id).await.unwrap(), Some(bytes));
    }

    #[tokio::test]
    async fn refuses_bytes_that_do_not_match_the_id() {
        let directory = tempfile::tempdir().unwrap();
        let store = PieceStore::open(directory.path()).await.unwrap();
        let id = PieceId::of(b"the real piece");

        let result = store.put(&id, b"something else").await;

        assert!(matches!(result, Err(PutError::WrongId)));
        assert_eq!(store.get(&id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_missing_piece_is_none() {
        let directory = tempfile::tempdir().unwrap();
        let store = PieceStore::open(directory.path()).await.unwrap();

        let piece = store.get(&PieceId::of(b"never stored")).await.unwrap();

        assert_eq!(piece, None);
    }
}

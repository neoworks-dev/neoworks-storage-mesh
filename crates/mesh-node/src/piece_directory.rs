//! Pieces kept as files in the node's data directory, one file per piece:
//!
//! ```text
//! <data>/pieces/<64 hex digits>             one file per piece, named by its id
//! <data>/pieces/<64 hex digits>.<n>.partial an upload still being written
//! ```
//!
//! This only holds bytes; [`crate::piece_store`] decides with the lease table
//! which pieces the node holds.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mesh_core::PieceId;

/// The directory pieces are kept in.
pub struct PieceDirectory {
    path: PathBuf,
    /// Numbers the temporary files of uploads in progress, so two uploads of
    /// the same piece at once never write to the same temporary file.
    next_upload: AtomicU64,
}

impl PieceDirectory {
    /// Opens `<data>/pieces` in `data_directory`, creating it if needed and
    /// deleting uploads that a previous run left unfinished.
    pub async fn open(data_directory: &Path) -> io::Result<Self> {
        let path = data_directory.join("pieces");
        tokio::fs::create_dir_all(&path).await?;
        remove_partial_uploads(&path).await?;
        Ok(PieceDirectory {
            path,
            next_upload: AtomicU64::new(0),
        })
    }

    /// Whether the file of `id` exists.
    pub async fn contains(&self, id: &PieceId) -> io::Result<bool> {
        tokio::fs::try_exists(self.path_of(id)).await
    }

    /// Writes `bytes` as the piece `id` through a temporary file, so a reader
    /// sees either no piece or the whole piece, never half of one, even if
    /// the node stops mid-write.
    pub async fn write(&self, id: &PieceId, bytes: &[u8]) -> io::Result<()> {
        let upload_number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let temporary_path = self.path.join(format!("{id}.{upload_number}.partial"));
        tokio::fs::write(&temporary_path, bytes).await?;
        tokio::fs::rename(&temporary_path, self.path_of(id)).await
    }

    /// The bytes of the piece `id`, or `None` when it has no file.
    pub async fn read(&self, id: &PieceId) -> io::Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.path_of(id)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The length of the piece `id`, or `None` when it has no file.
    pub async fn len_of(&self, id: &PieceId) -> io::Result<Option<u64>> {
        match tokio::fs::metadata(self.path_of(id)).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Deletes the files of `ids`; a missing file counts as deleted.
    pub async fn remove(&self, ids: &[PieceId]) -> io::Result<()> {
        for id in ids {
            match tokio::fs::remove_file(self.path_of(id)).await {
                Ok(()) => {}
                // A lease written just before the node stopped has no file.
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The bytes all piece files take together, counted from the directory.
    /// Uploads in progress are left out.
    pub async fn stored_bytes(&self) -> io::Result<u64> {
        let mut entries = tokio::fs::read_dir(&self.path).await?;
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

    /// The file of the piece `id`. Ids are parsed to their canonical hex form
    /// before they get here, so the name can never contain `/` or `..`.
    fn path_of(&self, id: &PieceId) -> PathBuf {
        self.path.join(id.to_string())
    }
}

/// Deletes the temporary files of uploads cut off when the node stopped.
///
/// Only `PieceDirectory::open` calls this, before the store takes any
/// upload, so every `.partial` file it finds belongs to a run that has ended.
/// No lease points at these files, so without this they would fill the disk
/// for good.
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

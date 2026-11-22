//! The signed pointers a node holds: the newest record per owner, each kept
//! under a lease like a piece.
//!
//! Pointers live in `<data>/pointers.sqlite3`, apart from the pieces. A node
//! cannot read a pointer's target, but it checks every signature, so it only
//! ever holds records their owners wrote. It cannot stop a client from being
//! shown an older record, since it might be the one doing that; clients guard
//! against rollback by asking several nodes.

use std::path::Path;

use mesh_core::pointer::{Pointer, PointerError};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// Pointers and their leases.
pub struct PointerStore {
    /// SQLite connections are not shared between threads; the lock also
    /// makes reading the held record and replacing it one step.
    table: Mutex<Connection>,
}

/// Why storing a pointer failed.
#[derive(Debug, thiserror::Error)]
pub enum PointerStoreError {
    #[error("the pointer is not genuine: {0}")]
    Invalid(#[from] PointerError),
    #[error("pointer table: {0}")]
    Database(#[from] rusqlite::Error),
}

/// What became of a pointer handed to [`PointerStore::put`].
#[derive(Debug, PartialEq, Eq)]
pub enum PointerPut {
    /// The node holds this record until `expires_at`.
    Stored { expires_at: u64 },
    /// The node holds a record it keeps instead: a higher version, or another
    /// record under the same version.
    Refused { held_version: u64 },
}

impl PointerStore {
    /// Opens the store in `directory`, creating its table if needed.
    pub fn open(directory: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.as_ref().join("pointers.sqlite3"))?;
        // The index lets a sweep find ended leases without reading every row.
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS pointer (
                 owner      BLOB PRIMARY KEY,
                 version    INTEGER NOT NULL,
                 target     BLOB NOT NULL,
                 signature  BLOB NOT NULL,
                 expires_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS pointer_by_expiry ON pointer (expires_at);",
        )?;
        Ok(PointerStore {
            table: Mutex::new(connection),
        })
    }

    /// Stores `pointer` until at least `expires_at`, when it is genuine and
    /// newer than the record held at `now`.
    ///
    /// Storing the record the node already holds only extends its lease, so
    /// renewing a pointer and copying it to a node that lost it are the same
    /// request.
    pub async fn put(
        &self,
        pointer: &Pointer,
        expires_at: u64,
        now: u64,
    ) -> Result<PointerPut, PointerStoreError> {
        pointer.verify()?;
        let table = self.table.lock().await;
        let Some((held, held_end)) = held_record(&table, &pointer.owner, now)? else {
            replace(&table, pointer, expires_at)?;
            return Ok(PointerPut::Stored { expires_at });
        };
        // A second record under the version the node holds is refused too.
        // Otherwise anyone holding two records an owner signed with one
        // version, say from two devices, could switch nodes between them.
        if pointer.version < held.version || (pointer.version == held.version && *pointer != held) {
            return Ok(PointerPut::Refused {
                held_version: held.version,
            });
        }
        // A lease only ever grows, as with pieces: a new version that asks for
        // less does not cut short what the node already promised.
        let expires_at = expires_at.max(held_end);
        replace(&table, pointer, expires_at)?;
        Ok(PointerPut::Stored { expires_at })
    }

    /// The pointer of `owner` held at `now`, or `None`.
    pub async fn get(&self, owner: &[u8; 32], now: u64) -> rusqlite::Result<Option<Pointer>> {
        let table = self.table.lock().await;
        let held = held_record(&table, owner, now)?;
        Ok(held.map(|(pointer, _)| pointer))
    }

    /// Deletes every pointer whose lease ended at or before `now`, and returns
    /// how many there were.
    pub async fn remove_ended(&self, now: u64) -> rusqlite::Result<usize> {
        let table = self.table.lock().await;
        table.execute("DELETE FROM pointer WHERE expires_at <= ?1", params![now])
    }
}

/// The record of `owner` and when its lease ends, if the lease runs past
/// `now`. A record whose lease ended counts as gone even before a sweep
/// deletes it, so after expiry any genuine record is accepted again.
fn held_record(
    table: &Connection,
    owner: &[u8; 32],
    now: u64,
) -> rusqlite::Result<Option<(Pointer, u64)>> {
    table
        .query_row(
            "SELECT owner, version, target, signature, expires_at FROM pointer
             WHERE owner = ?1 AND expires_at > ?2",
            params![owner, now],
            |row| {
                let pointer = Pointer {
                    owner: row.get(0)?,
                    version: row.get(1)?,
                    target: row.get(2)?,
                    signature: row.get(3)?,
                };
                Ok((pointer, row.get(4)?))
            },
        )
        .optional()
}

/// Writes `pointer` over whatever record its owner had.
fn replace(table: &Connection, pointer: &Pointer, expires_at: u64) -> rusqlite::Result<()> {
    table.execute(
        "INSERT OR REPLACE INTO pointer (owner, version, target, signature, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            pointer.owner,
            pointer.version,
            pointer.target,
            pointer.signature,
            expires_at
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use mesh_core::pointer::SigningKey;

    use super::*;

    fn store() -> (tempfile::TempDir, PointerStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = PointerStore::open(directory.path()).unwrap();
        (directory, store)
    }

    fn pointer(version: u64, target: &[u8]) -> Pointer {
        Pointer::sign(&SigningKey::from_bytes(&[1; 32]), version, target.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn keeps_the_newest_version() {
        let (_directory, store) = store();
        let first = pointer(1, b"first");
        let second = pointer(2, b"second");

        assert_eq!(
            store.put(&first, 1_000, 0).await.unwrap(),
            PointerPut::Stored { expires_at: 1_000 }
        );
        assert_eq!(
            store.put(&second, 1_000, 0).await.unwrap(),
            PointerPut::Stored { expires_at: 1_000 }
        );
        assert_eq!(
            store.put(&first, 1_000, 0).await.unwrap(),
            PointerPut::Refused { held_version: 2 }
        );
        assert_eq!(store.get(&second.owner, 0).await.unwrap(), Some(second));
    }

    #[tokio::test]
    async fn the_same_record_again_extends_its_lease() {
        let (_directory, store) = store();
        let record = pointer(3, b"index");
        store.put(&record, 1_000, 0).await.unwrap();

        assert_eq!(
            store.put(&record, 2_000, 500).await.unwrap(),
            PointerPut::Stored { expires_at: 2_000 }
        );
        assert_eq!(
            store.put(&record, 1_500, 600).await.unwrap(),
            PointerPut::Stored { expires_at: 2_000 }
        );
        assert_eq!(store.get(&record.owner, 1_999).await.unwrap(), Some(record));
    }

    #[tokio::test]
    async fn another_record_under_the_held_version_is_refused() {
        let (_directory, store) = store();
        store.put(&pointer(3, b"laptop's"), 1_000, 0).await.unwrap();

        let outcome = store.put(&pointer(3, b"phone's"), 1_000, 0).await.unwrap();

        assert_eq!(outcome, PointerPut::Refused { held_version: 3 });
    }

    #[tokio::test]
    async fn refuses_records_that_are_not_genuine() {
        let (_directory, store) = store();
        let mut forged = pointer(9, b"junk");
        forged.version = 10;

        let result = store.put(&forged, 1_000, 0).await;

        assert!(matches!(
            result,
            Err(PointerStoreError::Invalid(PointerError::BadSignature))
        ));
        assert_eq!(store.get(&forged.owner, 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_ended_pointer_is_gone_and_swept() {
        let (_directory, store) = store();
        let record = pointer(5, b"index");
        store.put(&record, 1_000, 0).await.unwrap();

        assert_eq!(store.get(&record.owner, 1_000).await.unwrap(), None);
        // After expiry, any genuine record is accepted again, even an older one.
        let older = pointer(4, b"older");
        assert_eq!(
            store.put(&older, 3_000, 2_000).await.unwrap(),
            PointerPut::Stored { expires_at: 3_000 }
        );
        assert_eq!(store.remove_ended(5_000).await.unwrap(), 1);
        assert_eq!(store.get(&record.owner, 0).await.unwrap(), None);
    }
}

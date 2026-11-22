//! When each piece's lease ends, in an SQLite table.
//!
//! The table is what decides whether a node holds a piece: a piece file with
//! no lease row, or with a lease that has ended, counts as gone. Lease ends
//! are Unix times in seconds on the node's clock.

use std::path::Path;

use mesh_core::PieceId;
use rusqlite::{Connection, OptionalExtension, params};

/// The lease of every piece on a node.
pub struct LeaseTable {
    connection: Connection,
}

impl LeaseTable {
    /// Opens the table in the SQLite file at `path`, creating it if needed.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        // The index lets a sweep find ended leases without reading every row.
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS lease (
                 piece_id   TEXT PRIMARY KEY,
                 expires_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS lease_by_expiry ON lease (expires_at);",
        )?;
        Ok(LeaseTable { connection })
    }

    /// When the lease of `id` ends, or `None` when the piece has no lease.
    pub fn expires_at(&self, id: &PieceId) -> rusqlite::Result<Option<u64>> {
        self.connection
            .query_row(
                "SELECT expires_at FROM lease WHERE piece_id = ?1",
                params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
    }

    /// Makes the lease of `id` end no sooner than `expires_at`, and returns
    /// when it ends now.
    ///
    /// A lease only ever grows. Anyone may store or renew any piece, so if a
    /// request could shorten a lease, anyone who learned a piece id could make
    /// someone else's piece expire.
    pub fn extend(&self, id: &PieceId, expires_at: u64) -> rusqlite::Result<u64> {
        self.connection.query_row(
            "INSERT INTO lease (piece_id, expires_at) VALUES (?1, ?2)
             ON CONFLICT (piece_id) DO UPDATE SET expires_at = max(expires_at, excluded.expires_at)
             RETURNING expires_at",
            params![id.to_string(), expires_at],
            |row| row.get(0),
        )
    }

    /// The pieces whose lease ended at or before `now`.
    pub fn ended(&self, now: u64) -> rusqlite::Result<Vec<PieceId>> {
        let mut statement = self
            .connection
            .prepare("SELECT piece_id FROM lease WHERE expires_at <= ?1")?;
        let ids = statement.query_map(params![now], |row| row.get::<_, String>(0))?;
        let mut ended = Vec::new();
        for id in ids {
            // Only `extend` writes this table, and it writes canonical ids.
            // A row that does not parse was written by something else; it is
            // skipped rather than stopping every sweep from then on.
            if let Ok(id) = id?.parse() {
                ended.push(id);
            }
        }
        Ok(ended)
    }

    /// Removes the lease of `id`.
    pub fn remove(&self, id: &PieceId) -> rusqlite::Result<()> {
        self.connection.execute(
            "DELETE FROM lease WHERE piece_id = ?1",
            params![id.to_string()],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> (tempfile::TempDir, LeaseTable) {
        let directory = tempfile::tempdir().unwrap();
        let table = LeaseTable::open(&directory.path().join("leases.sqlite3")).unwrap();
        (directory, table)
    }

    #[test]
    fn a_lease_only_ever_grows() {
        let (_directory, table) = table();
        let id = PieceId::of(b"piece");

        assert_eq!(table.extend(&id, 200).unwrap(), 200);
        assert_eq!(table.extend(&id, 100).unwrap(), 200);
        assert_eq!(table.extend(&id, 300).unwrap(), 300);
        assert_eq!(table.expires_at(&id).unwrap(), Some(300));
    }

    #[test]
    fn lists_only_the_leases_that_ended() {
        let (_directory, table) = table();
        let early = PieceId::of(b"early");
        let late = PieceId::of(b"late");
        table.extend(&early, 100).unwrap();
        table.extend(&late, 200).unwrap();

        assert_eq!(table.ended(99).unwrap(), vec![]);
        assert_eq!(table.ended(100).unwrap(), vec![early]);
        assert_eq!(table.ended(250).unwrap().len(), 2);
    }

    #[test]
    fn a_removed_lease_is_gone() {
        let (_directory, table) = table();
        let id = PieceId::of(b"piece");
        table.extend(&id, 100).unwrap();

        table.remove(&id).unwrap();

        assert_eq!(table.expires_at(&id).unwrap(), None);
    }
}

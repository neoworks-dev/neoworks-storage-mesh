//! The coordinator's audit pieces and how nodes did when challenged for
//! them, in `<data>/audits.sqlite3`.
//!
//! Two tables:
//!
//! - `audit_piece`: each piece the coordinator stored on a node to audit it
//!   with, by its id, Merkle root and length, and when its lease ends. The
//!   bytes themselves are not kept: the root is all a challenge is checked
//!   against.
//! - `audit_result`: each challenge, when it was made and whether the node
//!   passed it.

use std::path::Path;

use mesh_core::merkle::MerkleHash;
use mesh_core::{NodeId, PieceId};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// A piece of the coordinator's own on one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPiece {
    pub id: PieceId,
    pub merkle_root: MerkleHash,
    pub len: u64,
    /// When the node said the lease ends, in Unix seconds.
    pub expires_at: u64,
}

/// Audit pieces and challenge results.
pub struct AuditLog {
    /// SQLite connections are not shared between threads.
    connection: Mutex<Connection>,
}

impl AuditLog {
    /// Opens the log in `directory`, creating its tables if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("audits.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS audit_piece (
                 node_id     TEXT NOT NULL,
                 piece_id    TEXT NOT NULL,
                 merkle_root BLOB NOT NULL,
                 len         INTEGER NOT NULL,
                 expires_at  INTEGER NOT NULL,
                 PRIMARY KEY (node_id, piece_id)
             );
             CREATE TABLE IF NOT EXISTS audit_result (
                 node_id TEXT NOT NULL,
                 at      INTEGER NOT NULL,
                 passed  INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS audit_result_by_node
                 ON audit_result (node_id, at);",
        )?;
        Ok(AuditLog {
            connection: Mutex::new(connection),
        })
    }

    /// Notes that `node_id` holds `piece`.
    pub async fn add_piece(&self, node_id: &NodeId, piece: &AuditPiece) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT OR REPLACE INTO audit_piece (node_id, piece_id, merkle_root, len, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                node_id.to_string(),
                piece.id.to_string(),
                piece.merkle_root.as_bytes(),
                piece.len,
                piece.expires_at
            ],
        )?;
        Ok(())
    }

    /// The audit piece on `node_id` whose lease lasts longest, if one still
    /// runs at `now`.
    pub async fn newest_piece(
        &self,
        node_id: &NodeId,
        now: u64,
    ) -> rusqlite::Result<Option<AuditPiece>> {
        let connection = self.connection.lock().await;
        let row: Option<(String, [u8; 32], u64, u64)> = connection
            .query_row(
                "SELECT piece_id, merkle_root, len, expires_at FROM audit_piece
                 WHERE node_id = ?1 AND expires_at > ?2
                 ORDER BY expires_at DESC LIMIT 1",
                params![node_id.to_string(), now],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((id, root, len, expires_at)) = row else {
            return Ok(None);
        };
        // Only `add_piece` writes this table, from a parsed id; a row that
        // reads otherwise counts as no piece, and the next round places a
        // new one.
        let Ok(id) = id.parse() else {
            return Ok(None);
        };
        Ok(Some(AuditPiece {
            id,
            merkle_root: MerkleHash::from_bytes(root),
            len,
            expires_at,
        }))
    }

    /// Records that `node_id` passed a challenge at `at`, or failed it.
    pub async fn record(&self, node_id: &NodeId, at: u64, passed: bool) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO audit_result (node_id, at, passed) VALUES (?1, ?2, ?3)",
            params![node_id.to_string(), at, passed],
        )?;
        Ok(())
    }

    /// The share of challenges since `since` that `node_id` passed, in
    /// thousandths, or `None` when it was not challenged since then.
    pub async fn pass_permille(
        &self,
        node_id: &NodeId,
        since: u64,
    ) -> rusqlite::Result<Option<u16>> {
        let connection = self.connection.lock().await;
        let (passed, total): (u64, u64) = connection.query_row(
            "SELECT COALESCE(SUM(passed), 0), COUNT(*) FROM audit_result
             WHERE node_id = ?1 AND at >= ?2",
            params![node_id.to_string(), since],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if total == 0 {
            return Ok(None);
        }
        // At most 1000, since `passed` is at most `total`.
        Ok(Some((passed * 1000 / total) as u16))
    }

    /// Drops pieces whose lease ended and results from before `before`, which
    /// no window looks at any more.
    pub async fn forget(&self, now: u64, before: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "DELETE FROM audit_piece WHERE expires_at <= ?1",
            params![now],
        )?;
        connection.execute("DELETE FROM audit_result WHERE at < ?1", params![before])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mesh_core::merkle::root_of;

    use super::*;

    fn log() -> (tempfile::TempDir, AuditLog) {
        let directory = tempfile::tempdir().unwrap();
        let log = AuditLog::open(directory.path()).unwrap();
        (directory, log)
    }

    fn piece(bytes: &[u8], expires_at: u64) -> AuditPiece {
        AuditPiece {
            id: PieceId::of(bytes),
            merkle_root: root_of(bytes),
            len: bytes.len() as u64,
            expires_at,
        }
    }

    #[tokio::test]
    async fn the_piece_that_lasts_longest_is_used() {
        let (_directory, log) = log();
        let node = NodeId::of(&[1; 32]);
        log.add_piece(&node, &piece(b"old", 100)).await.unwrap();
        log.add_piece(&node, &piece(b"new", 200)).await.unwrap();

        assert_eq!(
            log.newest_piece(&node, 50).await.unwrap(),
            Some(piece(b"new", 200))
        );
        assert_eq!(log.newest_piece(&node, 200).await.unwrap(), None);
    }

    #[tokio::test]
    async fn the_pass_share_counts_challenges_in_the_window() {
        let (_directory, log) = log();
        let node = NodeId::of(&[1; 32]);
        log.record(&node, 10, false).await.unwrap();
        for at in 20..23 {
            log.record(&node, at, true).await.unwrap();
        }
        log.record(&node, 23, false).await.unwrap();

        assert_eq!(log.pass_permille(&node, 20).await.unwrap(), Some(750));
        assert_eq!(log.pass_permille(&node, 30).await.unwrap(), None);
    }

    #[tokio::test]
    async fn ended_pieces_and_old_results_are_forgotten() {
        let (_directory, log) = log();
        let node = NodeId::of(&[1; 32]);
        log.add_piece(&node, &piece(b"ended", 100)).await.unwrap();
        log.record(&node, 10, true).await.unwrap();

        log.forget(100, 20).await.unwrap();

        assert_eq!(log.newest_piece(&node, 0).await.unwrap(), None);
        assert_eq!(log.pass_permille(&node, 0).await.unwrap(), None);
    }
}

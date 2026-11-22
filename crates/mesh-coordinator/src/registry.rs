//! The nodes a coordinator knows and how often they answered, in SQLite.
//!
//! `<data>/registry.sqlite3` holds each node's newest signed record and one
//! row per check of whether the node was up. Uptime is the share of checks a
//! node answered over a recent span; rows older than that span are deleted.

use std::path::Path;

use mesh_core::NodeId;
use mesh_core::node_list::NodeRecord;
use mesh_core::signed::Signed;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// Node records and check results.
pub struct Registry {
    /// SQLite connections are not shared between threads; the lock also
    /// makes comparing a record with the held one and replacing it one step.
    connection: Mutex<Connection>,
}

/// What became of a record handed to [`Registry::register`].
#[derive(Debug, PartialEq, Eq)]
pub enum Registered {
    Stored,
    /// The registry holds a record of the node signed at the same time or
    /// later, so this one is a replay or arrived late.
    Stale,
}

impl Registry {
    /// Opens the registry in `directory`, creating its tables if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("registry.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS node (
                 node_id   TEXT PRIMARY KEY,
                 record    TEXT NOT NULL,
                 signed_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS probe (
                 node_id   TEXT NOT NULL,
                 probed_at INTEGER NOT NULL,
                 answered  INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS probe_by_node ON probe (node_id, probed_at);",
        )?;
        Ok(Registry {
            connection: Mutex::new(connection),
        })
    }

    /// Keeps `record` as the node's record, unless the registry holds one
    /// signed at the same time or later. The record must be verified.
    pub async fn register(&self, record: &Signed<NodeRecord>) -> rusqlite::Result<Registered> {
        let connection = self.connection.lock().await;
        let node_id = record.body.node_id.to_string();
        let held_signed_at: Option<u64> = connection
            .query_row(
                "SELECT signed_at FROM node WHERE node_id = ?1",
                params![node_id],
                |row| row.get(0),
            )
            .optional()?;
        if held_signed_at.is_some_and(|held| held >= record.body.signed_at) {
            return Ok(Registered::Stale);
        }
        // A record is plain strings and integers, which always serialize.
        let json = serde_json::to_string(record).expect("a node record serializes");
        connection.execute(
            "INSERT OR REPLACE INTO node (node_id, record, signed_at) VALUES (?1, ?2, ?3)",
            params![node_id, json, record.body.signed_at],
        )?;
        Ok(Registered::Stored)
    }

    /// Every node's record.
    pub async fn records(&self) -> rusqlite::Result<Vec<Signed<NodeRecord>>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare("SELECT record FROM node ORDER BY node_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut records = Vec::new();
        for json in rows {
            // Only `register` writes this table, with records it serialized
            // itself. A row that does not parse was written by something
            // else; it is skipped rather than breaking every snapshot.
            if let Ok(record) = serde_json::from_str(&json?) {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Notes whether `node_id` answered a check at `probed_at`.
    pub async fn record_probe(
        &self,
        node_id: &NodeId,
        probed_at: u64,
        answered: bool,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO probe (node_id, probed_at, answered) VALUES (?1, ?2, ?3)",
            params![node_id.to_string(), probed_at, answered],
        )?;
        Ok(())
    }

    /// The share of checks `node_id` answered after `since`, in thousandths,
    /// or `None` when it was not checked since then.
    pub async fn uptime_permille(
        &self,
        node_id: &NodeId,
        since: u64,
    ) -> rusqlite::Result<Option<u16>> {
        let connection = self.connection.lock().await;
        let (checks, answered): (u64, u64) = connection.query_row(
            "SELECT count(*), coalesce(sum(answered), 0) FROM probe
             WHERE node_id = ?1 AND probed_at > ?2",
            params![node_id.to_string(), since],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if checks == 0 {
            return Ok(None);
        }
        // At most 1000, since `answered` never exceeds `checks`.
        Ok(Some((answered * 1000 / checks) as u16))
    }

    /// When `node_id` was marked down, if it is down: it failed its most
    /// recent checks, and the first [`FAILURES_TO_BE_DOWN`] of those failures
    /// came within [`DOWN_WINDOW_SECONDS`]. It is marked down at the last of
    /// those, and stays down until it answers a check again.
    pub async fn down_since(&self, node_id: &NodeId) -> rusqlite::Result<Option<u64>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(
            "SELECT probed_at, answered FROM probe WHERE node_id = ?1
             ORDER BY probed_at DESC LIMIT 1000",
        )?;
        let probes = statement.query_map(params![node_id.to_string()], |row| {
            Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?))
        })?;
        // The failures since the last answer, newest first.
        let mut failures = Vec::new();
        for probe in probes {
            let (probed_at, answered) = probe?;
            if answered {
                break;
            }
            failures.push(probed_at);
        }
        Ok(marked_down_at(&failures))
    }

    /// Deletes the checks made at or before `until`, which no uptime is
    /// measured over any more.
    pub async fn forget_probes(&self, until: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute("DELETE FROM probe WHERE probed_at <= ?1", params![until])?;
        Ok(())
    }
}

/// Failed checks in a row that mark a node down.
pub const FAILURES_TO_BE_DOWN: usize = 3;

/// The span those failures have to fall in. With a check every five minutes,
/// three failures take ten; a node that fails rarely is never down.
pub const DOWN_WINDOW_SECONDS: u64 = 10 * 60;

/// When a node whose failed checks since its last answer are `failures`,
/// newest first, was marked down, if it was.
fn marked_down_at(failures: &[u64]) -> Option<u64> {
    if failures.len() < FAILURES_TO_BE_DOWN {
        return None;
    }
    let oldest = failures[failures.len() - 1];
    let marked = failures[failures.len() - FAILURES_TO_BE_DOWN];
    (marked - oldest <= DOWN_WINDOW_SECONDS).then_some(marked)
}

#[cfg(test)]
mod tests {
    use mesh_core::signed::SigningKey;

    use super::*;

    #[test]
    fn three_failures_in_ten_minutes_mark_a_node_down() {
        assert_eq!(marked_down_at(&[600, 300]), None);
        assert_eq!(marked_down_at(&[900, 600, 300, 0]), Some(600));
        assert_eq!(marked_down_at(&[2_000, 1_000, 0]), None);
    }

    #[tokio::test]
    async fn an_answer_ends_being_down() {
        let (_directory, registry) = registry();
        let node = NodeId::of(&[1; 32]);
        for at in [0, 300, 600] {
            registry.record_probe(&node, at, false).await.unwrap();
        }
        assert_eq!(registry.down_since(&node).await.unwrap(), Some(600));

        registry.record_probe(&node, 900, true).await.unwrap();

        assert_eq!(registry.down_since(&node).await.unwrap(), None);
    }

    fn record(signed_at: u64) -> Signed<NodeRecord> {
        let key = SigningKey::from_bytes(&[1; 32]);
        let body = NodeRecord {
            node_id: NodeId::of(&key.verifying_key().to_bytes()),
            url: "http://203.0.113.7:7001".into(),
            operator: "alice".into(),
            network: "AS3320".into(),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            signed_at,
        };
        Signed::sign(&key, body)
    }

    fn registry() -> (tempfile::TempDir, Registry) {
        let directory = tempfile::tempdir().unwrap();
        let registry = Registry::open(directory.path()).unwrap();
        (directory, registry)
    }

    #[tokio::test]
    async fn a_newer_record_replaces_an_older_one() {
        let (_directory, registry) = registry();

        assert_eq!(
            registry.register(&record(100)).await.unwrap(),
            Registered::Stored
        );
        assert_eq!(
            registry.register(&record(200)).await.unwrap(),
            Registered::Stored
        );
        assert_eq!(
            registry.register(&record(150)).await.unwrap(),
            Registered::Stale
        );
        assert_eq!(registry.records().await.unwrap(), vec![record(200)]);
    }

    #[tokio::test]
    async fn uptime_counts_only_checks_in_the_window() {
        let (_directory, registry) = registry();
        let node_id = record(0).body.node_id;
        registry.record_probe(&node_id, 10, false).await.unwrap();
        registry.record_probe(&node_id, 20, true).await.unwrap();
        registry.record_probe(&node_id, 30, true).await.unwrap();
        registry.record_probe(&node_id, 40, false).await.unwrap();

        assert_eq!(
            registry.uptime_permille(&node_id, 0).await.unwrap(),
            Some(500)
        );
        assert_eq!(
            registry.uptime_permille(&node_id, 15).await.unwrap(),
            Some(666)
        );
        assert_eq!(registry.uptime_permille(&node_id, 40).await.unwrap(), None);

        registry.forget_probes(25).await.unwrap();
        assert_eq!(
            registry.uptime_permille(&node_id, 0).await.unwrap(),
            Some(500)
        );
    }
}

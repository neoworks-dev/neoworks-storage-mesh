//! What a coordinator measures about nodes beyond whether they answer: how
//! fast they upload and how often they disappear for good, in SQLite.
//!
//! `<data>/measurements.sqlite3` holds three tables:
//!
//! - `speed_test`: one row per timed speed test. A node's bandwidth is the
//!   median of its recent ones, since one test can hit a busy moment.
//! - `node_life`: for every node that answers, when it first and last
//!   answered in its current life.
//! - `node_loss`: lives that ended. A node silent for longer than the
//!   coordinator's `lost_after` is lost: its life moves here, and if it ever
//!   answers again it starts a new one, so the time it was gone is not
//!   counted as time it was watched.
//!
//! The loss rate is losses per day of watching one node, which is what
//! choosing a layout needs: the chance that a node holding a piece is gone
//! the next day.

use std::path::Path;

use mesh_core::NodeId;
use rusqlite::{Connection, params};
use tokio::sync::Mutex;

const DAY_SECONDS: u64 = 24 * 60 * 60;

/// How many days of watching nodes a loss rate needs before it is published.
/// Below that, one loss among a few new nodes would give a rate far off the
/// real one.
const MIN_WATCHED_NODE_DAYS: u64 = 30;

/// Speed tests and node lives.
pub struct Measurements {
    /// SQLite connections are not shared between threads.
    connection: Mutex<Connection>,
}

impl Measurements {
    /// Opens the measurements in `directory`, creating their tables if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("measurements.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS speed_test (
                 node_id          TEXT NOT NULL,
                 measured_at      INTEGER NOT NULL,
                 bits_per_second  INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS speed_test_by_node
                 ON speed_test (node_id, measured_at);
             CREATE TABLE IF NOT EXISTS node_life (
                 node_id         TEXT PRIMARY KEY,
                 first_answered  INTEGER NOT NULL,
                 last_answered   INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS node_loss (
                 node_id         TEXT NOT NULL,
                 first_answered  INTEGER NOT NULL,
                 last_answered   INTEGER NOT NULL,
                 lost_at         INTEGER NOT NULL
             );",
        )?;
        Ok(Measurements {
            connection: Mutex::new(connection),
        })
    }

    /// Notes a speed test of `node_id` at `measured_at`.
    pub async fn record_speed_test(
        &self,
        node_id: &NodeId,
        measured_at: u64,
        bits_per_second: u64,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO speed_test (node_id, measured_at, bits_per_second) VALUES (?1, ?2, ?3)",
            params![node_id.to_string(), measured_at, bits_per_second],
        )?;
        Ok(())
    }

    /// The median of the speed tests of `node_id` after `since`, in bits per
    /// second, or `None` when there were none.
    pub async fn bandwidth(&self, node_id: &NodeId, since: u64) -> rusqlite::Result<Option<u64>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(
            "SELECT bits_per_second FROM speed_test
             WHERE node_id = ?1 AND measured_at > ?2 ORDER BY bits_per_second",
        )?;
        let speeds = statement
            .query_map(params![node_id.to_string(), since], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<u64>>>()?;
        Ok(median(&speeds))
    }

    /// Deletes the speed tests made at or before `until`.
    pub async fn forget_speed_tests(&self, until: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "DELETE FROM speed_test WHERE measured_at <= ?1",
            params![until],
        )?;
        Ok(())
    }

    /// Notes that `node_id` answered at `answered_at`, starting a new life
    /// for it if it has none.
    pub async fn record_answer(&self, node_id: &NodeId, answered_at: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO node_life (node_id, first_answered, last_answered) VALUES (?1, ?2, ?2)
             ON CONFLICT (node_id) DO UPDATE SET last_answered = excluded.last_answered",
            params![node_id.to_string(), answered_at],
        )?;
        Ok(())
    }

    /// Ends the life of every node that last answered at or before
    /// `silent_since`, noting it as lost at `now`. Returns how many were.
    pub async fn mark_lost(&self, silent_since: u64, now: u64) -> rusqlite::Result<usize> {
        let mut connection = self.connection.lock().await;
        // One transaction, so a life is never in both tables or in neither.
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO node_loss (node_id, first_answered, last_answered, lost_at)
             SELECT node_id, first_answered, last_answered, ?2 FROM node_life
             WHERE last_answered <= ?1",
            params![silent_since, now],
        )?;
        let lost = transaction.execute(
            "DELETE FROM node_life WHERE last_answered <= ?1",
            params![silent_since],
        )?;
        transaction.commit()?;
        Ok(lost)
    }

    /// The chance that a node is lost on any one day, in millionths: the
    /// losses after `window_start` over the days nodes were watched between
    /// `window_start` and `now`. `None` while that is under
    /// [`MIN_WATCHED_NODE_DAYS`].
    pub async fn daily_loss_per_million(
        &self,
        window_start: u64,
        now: u64,
    ) -> rusqlite::Result<Option<u32>> {
        let connection = self.connection.lock().await;
        // A life counts from its first to its last answer, cut to the window.
        let watched_seconds: u64 = connection.query_row(
            "SELECT coalesce(sum(max(0, min(last_answered, ?2) - max(first_answered, ?1))), 0)
             FROM (SELECT first_answered, last_answered FROM node_life
                   UNION ALL
                   SELECT first_answered, last_answered FROM node_loss)",
            params![window_start, now],
            |row| row.get(0),
        )?;
        let losses: u64 = connection.query_row(
            "SELECT count(*) FROM node_loss WHERE lost_at > ?1",
            params![window_start],
            |row| row.get(0),
        )?;
        if watched_seconds < MIN_WATCHED_NODE_DAYS * DAY_SECONDS {
            return Ok(None);
        }
        let per_million = losses * DAY_SECONDS * 1_000_000 / watched_seconds;
        // More than one loss per node and day only happens with nodes that
        // come and go within hours; the chance is then simply all of it.
        Ok(Some(per_million.min(1_000_000) as u32))
    }

    /// Deletes the losses noted at or before `until`.
    pub async fn forget_losses(&self, until: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute("DELETE FROM node_loss WHERE lost_at <= ?1", params![until])?;
        Ok(())
    }
}

/// The middle of `sorted`, or the mean of the two middle values when there
/// is an even number of them.
fn median(sorted: &[u64]) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        return Some(sorted[middle]);
    }
    // Halving each before adding cannot overflow; the lost halves add back.
    let (low, high) = (sorted[middle - 1], sorted[middle]);
    Some(low / 2 + high / 2 + (low % 2 + high % 2) / 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId {
        NodeId::of(&[byte; 32])
    }

    fn measurements() -> (tempfile::TempDir, Measurements) {
        let directory = tempfile::tempdir().unwrap();
        let measurements = Measurements::open(directory.path()).unwrap();
        (directory, measurements)
    }

    #[test]
    fn the_median_of_an_even_count_is_the_mean_of_the_middle_two() {
        assert_eq!(median(&[]), None);
        assert_eq!(median(&[5]), Some(5));
        assert_eq!(median(&[1, 3, 100]), Some(3));
        assert_eq!(median(&[1, 3, 5, 100]), Some(4));
        assert_eq!(median(&[u64::MAX, u64::MAX]), Some(u64::MAX));
    }

    #[tokio::test]
    async fn bandwidth_is_the_median_of_recent_speed_tests() {
        let (_directory, measurements) = measurements();
        let fast = node(1);
        for (at, speed) in [
            (10, 1_000),
            (20, 50_000_000),
            (30, 40_000_000),
            (40, 60_000_000),
        ] {
            measurements
                .record_speed_test(&fast, at, speed)
                .await
                .unwrap();
        }

        // One slow test does not drag the median down the way it would an
        // average.
        assert_eq!(
            measurements.bandwidth(&fast, 0).await.unwrap(),
            Some(45_000_000)
        );
        assert_eq!(
            measurements.bandwidth(&fast, 10).await.unwrap(),
            Some(50_000_000)
        );
        assert_eq!(measurements.bandwidth(&node(2), 0).await.unwrap(), None);

        measurements.forget_speed_tests(30).await.unwrap();
        assert_eq!(
            measurements.bandwidth(&fast, 0).await.unwrap(),
            Some(60_000_000)
        );
    }

    #[tokio::test]
    async fn a_node_silent_too_long_is_lost_and_a_return_starts_a_new_life() {
        let (_directory, measurements) = measurements();
        measurements.record_answer(&node(1), 100).await.unwrap();
        measurements.record_answer(&node(1), 200).await.unwrap();
        measurements.record_answer(&node(2), 500).await.unwrap();

        assert_eq!(measurements.mark_lost(300, 1_000).await.unwrap(), 1);
        assert_eq!(measurements.mark_lost(300, 1_100).await.unwrap(), 0);

        measurements.record_answer(&node(1), 2_000).await.unwrap();
        let connection = measurements.connection.lock().await;
        let first_answered: u64 = connection
            .query_row(
                "SELECT first_answered FROM node_life WHERE node_id = ?1",
                params![node(1).to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_answered, 2_000);
    }

    #[tokio::test]
    async fn the_loss_rate_is_losses_per_day_watched() {
        let (_directory, measurements) = measurements();
        let days = |count: u64| count * DAY_SECONDS;
        // 39 nodes answer for 25 days, 975 node days; node 0 answers once at
        // the start and is then lost.
        measurements.record_answer(&node(0), 0).await.unwrap();
        for byte in 1..40 {
            measurements.record_answer(&node(byte), 0).await.unwrap();
            measurements
                .record_answer(&node(byte), days(25))
                .await
                .unwrap();
        }
        assert_eq!(
            measurements
                .daily_loss_per_million(0, days(25))
                .await
                .unwrap(),
            Some(0)
        );

        measurements.mark_lost(days(1), days(25)).await.unwrap();

        assert_eq!(
            measurements
                .daily_loss_per_million(0, days(25))
                .await
                .unwrap(),
            Some(1_000_000 / 975)
        );
    }

    #[tokio::test]
    async fn too_little_watching_gives_no_loss_rate() {
        let (_directory, measurements) = measurements();
        measurements.record_answer(&node(1), 0).await.unwrap();
        measurements
            .record_answer(&node(1), 10 * DAY_SECONDS)
            .await
            .unwrap();

        assert_eq!(
            measurements
                .daily_loss_per_million(0, 10 * DAY_SECONDS)
                .await
                .unwrap(),
            None
        );
    }
}

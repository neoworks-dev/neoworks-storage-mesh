//! The tabs a node keeps, in `<data>/tabs.sqlite3`: how much each was paid
//! for, how much of that its pieces use, and when it ends.
//!
//! The node does not record which pieces belong to which tab. A piece's lease
//! already ends with the tab it was last stored or moved into, and once a
//! tab ends, what it held does not matter any more.

use std::path::Path;

use mesh_core::tabs::TabResponse;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// Tabs by name.
pub struct TabBook {
    connection: Mutex<Connection>,
}

/// Why a piece could not be charged to a tab.
#[derive(Debug, PartialEq, Eq)]
pub enum ChargeRefused {
    /// No tab of that name is open.
    NoTab,
    /// The tab has less room left than the piece takes.
    Full,
}

impl TabBook {
    /// Opens the tabs in `directory`, creating the table if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("tabs.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS tab (
                 name           TEXT PRIMARY KEY,
                 capacity_bytes INTEGER NOT NULL,
                 used_bytes     INTEGER NOT NULL,
                 expires_at     INTEGER NOT NULL
             );",
        )?;
        Ok(TabBook {
            connection: Mutex::new(connection),
        })
    }

    /// Adds `bytes` of capacity to the tab `name`, opening it to end at
    /// `expires_at` if it is new or ended. Paying more into an open tab
    /// does not move its end.
    pub async fn pay(
        &self,
        name: &str,
        bytes: u64,
        expires_at: u64,
        now: u64,
    ) -> rusqlite::Result<TabResponse> {
        let connection = self.connection.lock().await;
        // An ended tab starts over: its pieces are gone or about to be.
        connection.execute(
            "DELETE FROM tab WHERE name = ?1 AND expires_at <= ?2",
            params![name, now],
        )?;
        connection.query_row(
            "INSERT INTO tab (name, capacity_bytes, used_bytes, expires_at) VALUES (?1, ?2, 0, ?3)
             ON CONFLICT (name) DO UPDATE SET capacity_bytes = capacity_bytes + excluded.capacity_bytes
             RETURNING capacity_bytes, used_bytes, expires_at",
            params![name, bytes, expires_at],
            |row| {
                Ok(TabResponse {
                    capacity_bytes: row.get(0)?,
                    used_bytes: row.get(1)?,
                    expires_at: row.get(2)?,
                })
            },
        )
    }

    /// Charges `bytes` to the open tab `name` and returns when it ends.
    pub async fn charge(
        &self,
        name: &str,
        bytes: u64,
        now: u64,
    ) -> rusqlite::Result<Result<u64, ChargeRefused>> {
        let connection = self.connection.lock().await;
        let tab: Option<(u64, u64, u64)> = connection
            .query_row(
                "SELECT capacity_bytes, used_bytes, expires_at FROM tab
                 WHERE name = ?1 AND expires_at > ?2",
                params![name, now],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((capacity, used, expires_at)) = tab else {
            return Ok(Err(ChargeRefused::NoTab));
        };
        if used + bytes > capacity {
            return Ok(Err(ChargeRefused::Full));
        }
        connection.execute(
            "UPDATE tab SET used_bytes = used_bytes + ?2 WHERE name = ?1",
            params![name, bytes],
        )?;
        Ok(Ok(expires_at))
    }

    /// Forgets the tabs that ended at or before `now`.
    pub async fn remove_ended(&self, now: u64) -> rusqlite::Result<usize> {
        let connection = self.connection.lock().await;
        connection.execute("DELETE FROM tab WHERE expires_at <= ?1", params![now])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> (tempfile::TempDir, TabBook) {
        let directory = tempfile::tempdir().unwrap();
        let book = TabBook::open(directory.path()).unwrap();
        (directory, book)
    }

    #[tokio::test]
    async fn a_tab_takes_pieces_until_it_is_full() {
        let (_directory, book) = book();
        book.pay("a", 100, 1_000, 0).await.unwrap();

        assert_eq!(book.charge("a", 60, 10).await.unwrap(), Ok(1_000));
        assert_eq!(
            book.charge("a", 50, 10).await.unwrap(),
            Err(ChargeRefused::Full)
        );
        assert_eq!(book.charge("a", 40, 10).await.unwrap(), Ok(1_000));
    }

    #[tokio::test]
    async fn paying_more_adds_room_but_keeps_the_end() {
        let (_directory, book) = book();
        book.pay("a", 100, 1_000, 0).await.unwrap();

        let tab = book.pay("a", 100, 5_000, 10).await.unwrap();

        assert_eq!(tab.capacity_bytes, 200);
        assert_eq!(tab.expires_at, 1_000);
    }

    #[tokio::test]
    async fn an_ended_or_unknown_tab_takes_nothing() {
        let (_directory, book) = book();
        book.pay("a", 100, 1_000, 0).await.unwrap();

        assert_eq!(
            book.charge("a", 1, 1_000).await.unwrap(),
            Err(ChargeRefused::NoTab)
        );
        assert_eq!(
            book.charge("b", 1, 0).await.unwrap(),
            Err(ChargeRefused::NoTab)
        );
    }
}

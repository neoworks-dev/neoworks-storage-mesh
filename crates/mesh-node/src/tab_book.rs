//! The tabs a node keeps, in `<data>/tabs.sqlite3`: what each has left to
//! store with, in millionths of a token, and when it ends.
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
    /// The tab has less left than the piece costs.
    Empty,
}

impl TabBook {
    /// Opens the tabs in `directory`, creating the table if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("tabs.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS tab_balance (
                 name                TEXT PRIMARY KEY,
                 balance_microtokens INTEGER NOT NULL,
                 expires_at          INTEGER NOT NULL
             );",
        )?;
        Ok(TabBook {
            connection: Mutex::new(connection),
        })
    }

    /// Adds `microtokens` to the tab `name`, opening it to end at
    /// `expires_at` if it is new or ended. Paying more into an open tab
    /// does not move its end.
    pub async fn pay(
        &self,
        name: &str,
        microtokens: u64,
        expires_at: u64,
        now: u64,
    ) -> rusqlite::Result<TabResponse> {
        let connection = self.connection.lock().await;
        // An ended tab starts over: its pieces are gone or about to be.
        connection.execute(
            "DELETE FROM tab_balance WHERE name = ?1 AND expires_at <= ?2",
            params![name, now],
        )?;
        connection.query_row(
            "INSERT INTO tab_balance (name, balance_microtokens, expires_at) VALUES (?1, ?2, ?3)
             ON CONFLICT (name) DO UPDATE
                 SET balance_microtokens = balance_microtokens + excluded.balance_microtokens
             RETURNING balance_microtokens, expires_at",
            params![name, microtokens, expires_at],
            |row| {
                Ok(TabResponse {
                    balance_microtokens: row.get(0)?,
                    expires_at: row.get(1)?,
                })
            },
        )
    }

    /// When the open tab `name` ends, or `None` when there is none.
    pub async fn end_of(&self, name: &str, now: u64) -> rusqlite::Result<Option<u64>> {
        let connection = self.connection.lock().await;
        connection
            .query_row(
                "SELECT expires_at FROM tab_balance WHERE name = ?1 AND expires_at > ?2",
                params![name, now],
                |row| row.get(0),
            )
            .optional()
    }

    /// Takes `microtokens` from the open tab `name` and returns when it ends.
    pub async fn charge(
        &self,
        name: &str,
        microtokens: u64,
        now: u64,
    ) -> rusqlite::Result<Result<u64, ChargeRefused>> {
        let connection = self.connection.lock().await;
        let tab: Option<(u64, u64)> = connection
            .query_row(
                "SELECT balance_microtokens, expires_at FROM tab_balance
                 WHERE name = ?1 AND expires_at > ?2",
                params![name, now],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((balance, expires_at)) = tab else {
            return Ok(Err(ChargeRefused::NoTab));
        };
        if microtokens > balance {
            return Ok(Err(ChargeRefused::Empty));
        }
        connection.execute(
            "UPDATE tab_balance SET balance_microtokens = balance_microtokens - ?2 WHERE name = ?1",
            params![name, microtokens],
        )?;
        Ok(Ok(expires_at))
    }

    /// Forgets the tabs that ended at or before `now`.
    pub async fn remove_ended(&self, now: u64) -> rusqlite::Result<usize> {
        let connection = self.connection.lock().await;
        connection.execute(
            "DELETE FROM tab_balance WHERE expires_at <= ?1",
            params![now],
        )
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
    async fn a_tab_pays_for_pieces_until_its_balance_runs_out() {
        let (_directory, book) = book();
        book.pay("a", 100, 1_000, 0).await.unwrap();

        assert_eq!(book.charge("a", 60, 10).await.unwrap(), Ok(1_000));
        assert_eq!(
            book.charge("a", 50, 10).await.unwrap(),
            Err(ChargeRefused::Empty)
        );
        assert_eq!(book.charge("a", 40, 10).await.unwrap(), Ok(1_000));
    }

    #[tokio::test]
    async fn paying_more_adds_to_the_balance_but_keeps_the_end() {
        let (_directory, book) = book();
        book.pay("a", 100, 1_000, 0).await.unwrap();

        let tab = book.pay("a", 100, 5_000, 10).await.unwrap();

        assert_eq!(tab.balance_microtokens, 200);
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
        assert_eq!(book.end_of("a", 999).await.unwrap(), Some(1_000));
        assert_eq!(book.end_of("a", 1_000).await.unwrap(), None);
    }
}

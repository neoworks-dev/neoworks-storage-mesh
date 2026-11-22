//! The coordinator's daily price tables, in SQLite.
//!
//! `<data>/prices.sqlite3` keeps every signed table by its day. Once a day
//! the coordinator works out the next table from the newest one it kept and
//! the node list it published last ([`mesh_core::prices::next_table`]), signs
//! it and keeps it, so a restart carries on from the same prices.

use std::path::Path;
use std::sync::Arc;

use mesh_core::node_list::NodeSnapshot;
use mesh_core::prices::{DAY_SECONDS, PriceTable, next_table};
use mesh_core::signed::{Signed, SigningKey};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::{Mutex, RwLock};

use crate::clock::unix_now;
use crate::publish::LatestSnapshot;
use crate::settings::CoordinatorSettings;

/// The newest signed price table, or `None` before the first.
pub type LatestPrices = RwLock<Option<Signed<PriceTable>>>;

/// Signed price tables by day.
pub struct PriceBook {
    /// SQLite connections are not shared between threads.
    connection: Mutex<Connection>,
}

impl PriceBook {
    /// Opens the price book in `directory`, creating its table if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("prices.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS price_table (
                 day    INTEGER PRIMARY KEY,
                 table_ TEXT NOT NULL
             );",
        )?;
        Ok(PriceBook {
            connection: Mutex::new(connection),
        })
    }

    /// The table of the latest day kept.
    pub async fn newest(&self) -> rusqlite::Result<Option<Signed<PriceTable>>> {
        let connection = self.connection.lock().await;
        let json: Option<String> = connection
            .query_row(
                "SELECT table_ FROM price_table ORDER BY day DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        // Only `keep` writes this table, with tables it serialized itself; one
        // that does not parse counts as none, and prices start over.
        Ok(json.and_then(|json| serde_json::from_str(&json).ok()))
    }

    /// Keeps `table` as the table of its day.
    pub async fn keep(&self, table: &Signed<PriceTable>) -> rusqlite::Result<()> {
        // A table is integers and names, which always serialize.
        let json = serde_json::to_string(table).expect("a price table serializes");
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT OR REPLACE INTO price_table (day, table_) VALUES (?1, ?2)",
            params![table.body.day, json],
        )?;
        Ok(())
    }
}

/// The signed table for `day`: the newest kept one if it is for `day`;
/// otherwise the table that follows it, worked out from `snapshot`, signed
/// with `key` and kept. With no table kept, prices start at
/// [`PriceTable::starting`].
pub async fn table_for(
    book: &PriceBook,
    key: &SigningKey,
    snapshot: &NodeSnapshot,
    day: u64,
) -> rusqlite::Result<Signed<PriceTable>> {
    let next = match book.newest().await? {
        Some(newest) if newest.body.day >= day => return Ok(newest),
        Some(newest) => next_table(&newest.body, snapshot, day),
        None => PriceTable::starting(day),
    };
    let signed = Signed::sign(key, next);
    book.keep(&signed).await?;
    Ok(signed)
}

/// Each publish interval, makes sure `latest_prices` holds the table for
/// today, working it out from the newest published node list when the day
/// has changed. Runs forever.
pub async fn price_forever(
    book: PriceBook,
    key: SigningKey,
    latest_snapshot: Arc<LatestSnapshot>,
    latest_prices: Arc<LatestPrices>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.publish_interval);
    loop {
        timer.tick().await;
        // Prices follow the node list, so they wait for the first one.
        let Some(snapshot) = latest_snapshot.read().await.clone() else {
            continue;
        };
        let day = unix_now() / DAY_SECONDS;
        match table_for(&book, &key, &snapshot.body, day).await {
            Ok(table) => *latest_prices.write().await = Some(table),
            // The table served stays the old one until the next tick.
            Err(error) => eprintln!("prices: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use mesh_core::prices::NodeClass;

    use super::*;

    fn empty_snapshot() -> NodeSnapshot {
        NodeSnapshot {
            published_at: 0,
            nodes: Vec::new(),
            daily_loss_per_million: None,
        }
    }

    #[tokio::test]
    async fn the_first_table_starts_prices_and_later_days_follow_it() {
        let directory = tempfile::tempdir().unwrap();
        let book = PriceBook::open(directory.path()).unwrap();
        let key = SigningKey::from_bytes(&[9; 32]);

        let first = table_for(&book, &key, &empty_snapshot(), 100)
            .await
            .unwrap();
        let again = table_for(&book, &key, &empty_snapshot(), 100)
            .await
            .unwrap();
        let next = table_for(&book, &key, &empty_snapshot(), 101)
            .await
            .unwrap();

        assert_eq!(first.verify(), Ok(()));
        assert_eq!(first.body, PriceTable::starting(100));
        assert_eq!(again, first);
        assert_eq!(next.body.day, 101);
        // No nodes, so no class moved.
        assert_eq!(next.body.price_of(NodeClass::Fast), Some(8_000));
    }

    #[tokio::test]
    async fn a_reopened_book_carries_on_from_its_newest_table() {
        let directory = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[9; 32]);
        let kept = {
            let book = PriceBook::open(directory.path()).unwrap();
            table_for(&book, &key, &empty_snapshot(), 7).await.unwrap()
        };

        let book = PriceBook::open(directory.path()).unwrap();

        assert_eq!(book.newest().await.unwrap(), Some(kept));
    }
}

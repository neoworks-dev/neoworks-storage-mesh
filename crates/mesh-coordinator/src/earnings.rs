//! What node operators have earned, in `<data>/earnings.sqlite3`
//! (`mesh_core::earnings` describes the rules).
//!
//! Four tables:
//!
//! - `held`: the value of each paid token a node spent, in millionths of a
//!   token, paid out evenly between `start` and `end`.
//! - `unpaid`: what each node earned but has not been credited yet, because
//!   it is less than a whole token or the node names no operator.
//! - `operator`: the account each node's earnings go to.
//! - `repair_pool`: what nodes that were down did not earn, which pays for
//!   repairing what they held.
//!
//! [`Earnings::settle`] moves what came due since the last time either to
//! the node or to the repair pool, depending on whether the node was up;
//! [`pay_out`] credits whole tokens to operators' balances.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use mesh_core::NodeId;
use mesh_core::sign_in::normalize_email;
use mesh_core::tokens::MICROTOKENS_PER_TOKEN;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

use crate::clock::unix_now;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;
use crate::token_issuer::TokenIssuer;

/// The uptime a node needs over the last day to earn that day, in
/// thousandths: the same as the least reliable price class.
const EARNING_UPTIME_PERMILLE: u16 = 950;

/// Held values, unpaid earnings, operators and the repair pool.
pub struct Earnings {
    /// SQLite connections are not shared between threads.
    connection: Mutex<Connection>,
}

/// What became of an operator claim.
#[derive(Debug, PartialEq, Eq)]
pub enum Linked {
    Stored,
    /// A claim of the node signed at the same time or later is held.
    Stale,
}

/// Whole tokens owed to an operator, taken out of a node's unpaid earnings.
#[derive(Debug, PartialEq, Eq)]
pub struct Payout {
    pub node_id: NodeId,
    pub email: String,
    pub tokens: u64,
}

impl Earnings {
    /// Opens the earnings in `directory`, creating their tables if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("earnings.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS held (
                 id             INTEGER PRIMARY KEY,
                 node_id        TEXT NOT NULL,
                 total_micro    INTEGER NOT NULL,
                 start          INTEGER NOT NULL,
                 end_           INTEGER NOT NULL,
                 released_micro INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS unpaid (
                 node_id TEXT PRIMARY KEY,
                 micro   INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS operator (
                 node_id   TEXT PRIMARY KEY,
                 email     TEXT NOT NULL,
                 signed_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS repair_pool (
                 id    INTEGER PRIMARY KEY CHECK (id = 1),
                 micro INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO repair_pool (id, micro) VALUES (1, 0);",
        )?;
        Ok(Earnings {
            connection: Mutex::new(connection),
        })
    }

    /// Holds `micro` millionths of a token that `node_id` was paid, to be
    /// paid out evenly from `start` to `end`.
    pub async fn hold(
        &self,
        node_id: &NodeId,
        micro: u64,
        start: u64,
        end: u64,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO held (node_id, total_micro, start, end_, released_micro)
             VALUES (?1, ?2, ?3, ?4, 0)",
            params![node_id.to_string(), micro, start, end],
        )?;
        Ok(())
    }

    /// Sends the earnings of `node_id` to the account `email`, unless a
    /// claim signed at `signed_at` or later is held.
    pub async fn link(
        &self,
        node_id: &NodeId,
        email: &str,
        signed_at: u64,
    ) -> rusqlite::Result<Linked> {
        let connection = self.connection.lock().await;
        let changed = connection.execute(
            "INSERT INTO operator (node_id, email, signed_at) VALUES (?1, ?2, ?3)
             ON CONFLICT (node_id) DO UPDATE SET email = excluded.email, signed_at = excluded.signed_at
             WHERE excluded.signed_at > operator.signed_at",
            params![node_id.to_string(), normalize_email(email), signed_at],
        )?;
        Ok(if changed == 1 {
            Linked::Stored
        } else {
            Linked::Stale
        })
    }

    /// The email of the account `node_id` earns for, if it named one.
    pub async fn operator_of(&self, node_id: &NodeId) -> rusqlite::Result<Option<String>> {
        let connection = self.connection.lock().await;
        connection
            .query_row(
                "SELECT email FROM operator WHERE node_id = ?1",
                params![node_id.to_string()],
                |row| row.get(0),
            )
            .optional()
    }

    /// The nodes with value still held.
    pub async fn held_nodes(&self) -> rusqlite::Result<Vec<NodeId>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare("SELECT DISTINCT node_id FROM held")?;
        let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut nodes = Vec::new();
        for id in ids {
            // Only `hold` writes this table, from parsed ids.
            if let Ok(node_id) = id?.parse() {
                nodes.push(node_id);
            }
        }
        Ok(nodes)
    }

    /// Moves what came due by `now` out of the held values: to the node's
    /// unpaid earnings if it is in `up`, to the repair pool otherwise.
    pub async fn settle(&self, now: u64, up: &HashSet<NodeId>) -> rusqlite::Result<()> {
        let mut connection = self.connection.lock().await;
        // One transaction, so a share is never both released and lost, or
        // paid twice.
        let transaction = connection.transaction()?;
        let rows: Vec<(i64, String, u64, u64, u64, u64)> = {
            let mut statement = transaction.prepare(
                "SELECT id, node_id, total_micro, start, end_, released_micro FROM held",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        for (id, node_id, total, start, end, released) in rows {
            let due = due_by(total, start, end, now).saturating_sub(released);
            if due == 0 {
                continue;
            }
            transaction.execute(
                "UPDATE held SET released_micro = released_micro + ?2 WHERE id = ?1",
                params![id, due],
            )?;
            let is_up = node_id.parse().is_ok_and(|parsed| up.contains(&parsed));
            if is_up {
                transaction.execute(
                    "INSERT INTO unpaid (node_id, micro) VALUES (?1, ?2)
                     ON CONFLICT (node_id) DO UPDATE SET micro = micro + excluded.micro",
                    params![node_id, due],
                )?;
            } else {
                transaction.execute(
                    "UPDATE repair_pool SET micro = micro + ?1 WHERE id = 1",
                    params![due],
                )?;
            }
        }
        transaction.execute("DELETE FROM held WHERE released_micro >= total_micro", [])?;
        transaction.commit()
    }

    /// Takes the whole tokens out of the unpaid earnings of every node that
    /// names an operator, and returns who is owed them.
    pub async fn take_payouts(&self) -> rusqlite::Result<Vec<Payout>> {
        let mut connection = self.connection.lock().await;
        let transaction = connection.transaction()?;
        let owed: Vec<(String, String, u64)> = {
            let mut statement = transaction.prepare(
                "SELECT unpaid.node_id, operator.email, unpaid.micro / ?1 FROM unpaid
                 JOIN operator ON operator.node_id = unpaid.node_id
                 WHERE unpaid.micro >= ?1",
            )?;
            let rows = statement.query_map(params![MICROTOKENS_PER_TOKEN], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut payouts = Vec::with_capacity(owed.len());
        for (node_id, email, tokens) in owed {
            transaction.execute(
                "UPDATE unpaid SET micro = micro - ?2 WHERE node_id = ?1",
                params![node_id, tokens * MICROTOKENS_PER_TOKEN],
            )?;
            // Only `settle` writes this table, from parsed ids.
            if let Ok(node_id) = node_id.parse() {
                payouts.push(Payout {
                    node_id,
                    email,
                    tokens,
                });
            }
        }
        transaction.commit()?;
        Ok(payouts)
    }

    /// Puts `payout` back into the node's unpaid earnings, after crediting
    /// it failed.
    pub async fn give_back(&self, payout: &Payout) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO unpaid (node_id, micro) VALUES (?1, ?2)
             ON CONFLICT (node_id) DO UPDATE SET micro = micro + excluded.micro",
            params![
                payout.node_id.to_string(),
                payout.tokens * MICROTOKENS_PER_TOKEN
            ],
        )?;
        Ok(())
    }

    /// Takes up to `micro` millionths of a token out of the repair pool, to
    /// pay a node that took a rebuilt piece, and returns what it took: all of
    /// it, or what the pool held when that was less.
    pub async fn take_from_repair_pool(&self, micro: u64) -> rusqlite::Result<u64> {
        // The lock is held from reading the pool to taking from it, so two
        // repairs never take the same share.
        let connection = self.connection.lock().await;
        let held: u64 =
            connection.query_row("SELECT micro FROM repair_pool WHERE id = 1", [], |row| {
                row.get(0)
            })?;
        let taken = held.min(micro);
        connection.execute(
            "UPDATE repair_pool SET micro = micro - ?1 WHERE id = 1",
            params![taken],
        )?;
        Ok(taken)
    }

    /// What the repair pool holds, in millionths of a token.
    #[cfg(test)]
    pub async fn repair_pool(&self) -> rusqlite::Result<u64> {
        let connection = self.connection.lock().await;
        connection
            .query_row("SELECT micro FROM repair_pool WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map(Option::unwrap_or_default)
    }

    /// What `node_id` earned but was not credited yet, in millionths of a
    /// token.
    #[cfg(test)]
    pub async fn unpaid(&self, node_id: &NodeId) -> rusqlite::Result<u64> {
        let connection = self.connection.lock().await;
        connection
            .query_row(
                "SELECT micro FROM unpaid WHERE node_id = ?1",
                params![node_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map(Option::unwrap_or_default)
    }
}

/// How much of `total` is due by `now` when it is paid out evenly from
/// `start` to `end`.
fn due_by(total: u64, start: u64, end: u64, now: u64) -> u64 {
    if now >= end || end <= start {
        return total;
    }
    let elapsed = now.saturating_sub(start);
    // `elapsed` is below `end - start`, so the share is below `total`.
    (u128::from(total) * u128::from(elapsed) / u128::from(end - start)) as u64
}

/// Settles what came due, counting a node as up when it answered at least
/// [`EARNING_UPTIME_PERMILLE`] of the checks over the uptime window, and
/// credits whole tokens to operators.
pub async fn pay_out(
    earnings: &Earnings,
    registry: &Registry,
    tokens: &TokenIssuer,
    settings: &CoordinatorSettings,
    now: u64,
) -> rusqlite::Result<()> {
    let since = now.saturating_sub(settings.uptime_window.as_secs());
    let mut up = HashSet::new();
    for node_id in earnings.held_nodes().await? {
        let uptime = registry.uptime_permille(&node_id, since).await?;
        if uptime.is_some_and(|permille| permille >= EARNING_UPTIME_PERMILLE) {
            up.insert(node_id);
        }
    }
    earnings.settle(now, &up).await?;
    for payout in earnings.take_payouts().await? {
        if let Err(error) = tokens.credit(&payout.email, payout.tokens).await {
            eprintln!("earnings: crediting {}: {error}", payout.email);
            earnings.give_back(&payout).await?;
        }
    }
    Ok(())
}

/// Pays out each earnings interval, forever.
pub async fn pay_out_forever(
    earnings: Arc<Earnings>,
    registry: Arc<Registry>,
    tokens: Arc<TokenIssuer>,
    settings: CoordinatorSettings,
) {
    let mut timer = tokio::time::interval(settings.earnings_interval);
    loop {
        timer.tick().await;
        // A failed round leaves everything held; the next one catches up.
        if let Err(error) = pay_out(&earnings, &registry, &tokens, &settings, unix_now()).await {
            eprintln!("earnings: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId {
        NodeId::of(&[byte; 32])
    }

    fn earnings() -> (tempfile::TempDir, Earnings) {
        let directory = tempfile::tempdir().unwrap();
        let earnings = Earnings::open(directory.path()).unwrap();
        (directory, earnings)
    }

    #[test]
    fn value_comes_due_evenly() {
        assert_eq!(due_by(1_000, 100, 200, 100), 0);
        assert_eq!(due_by(1_000, 100, 200, 150), 500);
        assert_eq!(due_by(1_000, 100, 200, 250), 1_000);
    }

    #[tokio::test]
    async fn a_node_that_stays_up_earns_and_its_operator_is_credited() {
        let (_directory, earnings) = earnings();
        let up = HashSet::from([node(1)]);
        earnings.hold(&node(1), 3_000_000, 0, 100).await.unwrap();
        earnings.link(&node(1), "op@example.org", 5).await.unwrap();

        earnings.settle(50, &up).await.unwrap();
        let payouts = earnings.take_payouts().await.unwrap();

        assert_eq!(
            payouts,
            vec![Payout {
                node_id: node(1),
                email: "op@example.org".into(),
                tokens: 1,
            }]
        );
        assert_eq!(earnings.unpaid(&node(1)).await.unwrap(), 500_000);
    }

    #[tokio::test]
    async fn what_a_node_that_was_down_did_not_earn_goes_to_repair() {
        let (_directory, earnings) = earnings();
        earnings.hold(&node(1), 1_000, 0, 100).await.unwrap();

        earnings
            .settle(40, &HashSet::from([node(1)]))
            .await
            .unwrap();
        earnings.settle(100, &HashSet::new()).await.unwrap();

        assert_eq!(earnings.unpaid(&node(1)).await.unwrap(), 400);
        assert_eq!(earnings.repair_pool().await.unwrap(), 600);
        assert!(earnings.held_nodes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn repairs_take_no_more_than_the_pool_holds() {
        let (_directory, earnings) = earnings();
        earnings.hold(&node(1), 1_000, 0, 100).await.unwrap();
        earnings.settle(100, &HashSet::new()).await.unwrap();

        assert_eq!(earnings.take_from_repair_pool(600).await.unwrap(), 600);
        assert_eq!(earnings.take_from_repair_pool(600).await.unwrap(), 400);
        assert_eq!(earnings.repair_pool().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn earnings_wait_for_a_node_to_name_its_operator() {
        let (_directory, earnings) = earnings();
        earnings.hold(&node(1), 2_000_000, 0, 10).await.unwrap();
        earnings
            .settle(10, &HashSet::from([node(1)]))
            .await
            .unwrap();

        assert!(earnings.take_payouts().await.unwrap().is_empty());
        earnings.link(&node(1), "op@example.org", 5).await.unwrap();
        assert_eq!(earnings.take_payouts().await.unwrap()[0].tokens, 2);
    }

    #[tokio::test]
    async fn an_older_claim_does_not_replace_a_newer_one() {
        let (_directory, earnings) = earnings();

        assert_eq!(
            earnings
                .link(&node(1), "new@example.org", 20)
                .await
                .unwrap(),
            Linked::Stored
        );
        assert_eq!(
            earnings
                .link(&node(1), "old@example.org", 10)
                .await
                .unwrap(),
            Linked::Stale
        );
        earnings.hold(&node(1), 1_000_000, 0, 1).await.unwrap();
        earnings.settle(1, &HashSet::from([node(1)])).await.unwrap();
        assert_eq!(
            earnings.take_payouts().await.unwrap()[0].email,
            "new@example.org"
        );
    }
}

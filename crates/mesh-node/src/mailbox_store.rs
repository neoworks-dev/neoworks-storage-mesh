//! The mailbox messages a node holds, in `<data>/mailboxes.sqlite3`
//! (`mesh_core::mailbox` describes mailboxes).
//!
//! Each message is filed under its mailbox's kind and address, kept until
//! its lease ends, and never changed. The node cannot read a message and
//! does not know whose mailbox an address is.

use std::path::Path;

use mesh_core::mailbox::{MAX_MESSAGES_PER_MAILBOX, MailboxKind};
use rusqlite::{Connection, params};
use tokio::sync::Mutex;

/// Mailbox messages and their leases.
pub struct MailboxStore {
    /// SQLite connections are not shared between threads; the lock also
    /// makes checking a mailbox's room and appending one step.
    table: Mutex<Connection>,
}

/// What became of a message handed to [`MailboxStore::append`].
#[derive(Debug, PartialEq, Eq)]
pub enum Appended {
    /// The mailbox holds the message now, or held it already.
    Held,
    /// The mailbox holds [`MAX_MESSAGES_PER_MAILBOX`] messages.
    MailboxFull,
    /// The node's mailboxes take up all the room it gives them.
    NodeFull,
}

impl MailboxStore {
    /// Opens the store in `directory`, creating its table if needed.
    pub fn open(directory: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.as_ref().join("mailboxes.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS message (
                 id         INTEGER PRIMARY KEY,
                 kind       INTEGER NOT NULL,
                 address    BLOB NOT NULL,
                 hash       BLOB NOT NULL,
                 body       BLOB NOT NULL,
                 expires_at INTEGER NOT NULL,
                 UNIQUE (kind, address, hash)
             );
             CREATE INDEX IF NOT EXISTS message_by_expiry ON message (expires_at);",
        )?;
        Ok(MailboxStore {
            table: Mutex::new(connection),
        })
    }

    /// Appends `body` to the mailbox `address` of `kind` until `expires_at`,
    /// unless the mailbox or the node is full. `max_total_bytes` is the room
    /// the node gives all mailboxes together.
    pub async fn append(
        &self,
        kind: MailboxKind,
        address: &[u8; 32],
        body: &[u8],
        expires_at: u64,
        max_total_bytes: u64,
    ) -> rusqlite::Result<Appended> {
        let table = self.table.lock().await;
        let hash = blake3::hash(body);
        let held: bool = table.query_row(
            "SELECT EXISTS (SELECT 1 FROM message WHERE kind = ?1 AND address = ?2 AND hash = ?3)",
            params![kind_number(kind), address, hash.as_bytes()],
            |row| row.get(0),
        )?;
        if held {
            return Ok(Appended::Held);
        }
        let count: u64 = table.query_row(
            "SELECT COUNT(*) FROM message WHERE kind = ?1 AND address = ?2",
            params![kind_number(kind), address],
            |row| row.get(0),
        )?;
        if count >= MAX_MESSAGES_PER_MAILBOX as u64 {
            return Ok(Appended::MailboxFull);
        }
        let total: u64 = table.query_row(
            "SELECT COALESCE(SUM(LENGTH(body)), 0) FROM message",
            [],
            |row| row.get(0),
        )?;
        if total + body.len() as u64 > max_total_bytes {
            return Ok(Appended::NodeFull);
        }
        table.execute(
            "INSERT INTO message (kind, address, hash, body, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                kind_number(kind),
                address,
                hash.as_bytes(),
                body,
                expires_at
            ],
        )?;
        Ok(Appended::Held)
    }

    /// The messages in the mailbox `address` of `kind` whose lease runs at
    /// `now`, oldest first.
    pub async fn messages(
        &self,
        kind: MailboxKind,
        address: &[u8; 32],
        now: u64,
    ) -> rusqlite::Result<Vec<Vec<u8>>> {
        let table = self.table.lock().await;
        let mut statement = table.prepare(
            "SELECT body FROM message WHERE kind = ?1 AND address = ?2 AND expires_at > ?3
             ORDER BY id",
        )?;
        let bodies =
            statement.query_map(params![kind_number(kind), address, now], |row| row.get(0))?;
        bodies.collect()
    }

    /// Deletes every message whose lease ended at or before `now`.
    pub async fn remove_ended(&self, now: u64) -> rusqlite::Result<usize> {
        let table = self.table.lock().await;
        table.execute("DELETE FROM message WHERE expires_at <= ?1", params![now])
    }
}

/// How `kind` is written in the table.
fn kind_number(kind: MailboxKind) -> u8 {
    match kind {
        MailboxKind::Personal => 0,
        MailboxKind::Contact => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOM: u64 = 1 << 20;

    fn store() -> (tempfile::TempDir, MailboxStore) {
        let directory = tempfile::tempdir().unwrap();
        let store = MailboxStore::open(directory.path()).unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn messages_come_back_in_order_once_each_and_per_kind() {
        let (_directory, store) = store();
        let address = [1; 32];
        let personal = MailboxKind::Personal;
        store
            .append(personal, &address, b"one", 100, ROOM)
            .await
            .unwrap();
        store
            .append(personal, &address, b"two", 100, ROOM)
            .await
            .unwrap();
        store
            .append(personal, &address, b"one", 100, ROOM)
            .await
            .unwrap();

        assert_eq!(
            store.messages(personal, &address, 0).await.unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
        let contact = store.messages(MailboxKind::Contact, &address, 0).await;
        assert!(contact.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_full_mailbox_and_a_full_node_take_no_more() {
        let (_directory, store) = store();
        let personal = MailboxKind::Personal;
        for number in 0..MAX_MESSAGES_PER_MAILBOX {
            let body = number.to_be_bytes();
            store
                .append(personal, &[1; 32], &body, 100, ROOM)
                .await
                .unwrap();
        }

        let one_more = store.append(personal, &[1; 32], b"more", 100, ROOM).await;
        let elsewhere = store.append(personal, &[2; 32], &[0; 64], 100, 64).await;

        assert_eq!(one_more.unwrap(), Appended::MailboxFull);
        assert_eq!(elsewhere.unwrap(), Appended::NodeFull);
    }

    #[tokio::test]
    async fn ended_messages_are_gone() {
        let (_directory, store) = store();
        let personal = MailboxKind::Personal;
        store
            .append(personal, &[1; 32], b"old", 100, ROOM)
            .await
            .unwrap();

        assert!(
            store
                .messages(personal, &[1; 32], 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.remove_ended(100).await.unwrap(), 1);
    }
}

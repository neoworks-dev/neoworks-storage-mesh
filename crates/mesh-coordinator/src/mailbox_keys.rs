//! Mailbox keys by email, in `<data>/mailbox_keys.sqlite3`, so people can
//! find each other's mailbox (`mesh_core::mailbox`).
//!
//! An account files its X25519 public key after signing in; anyone can look
//! a key up by email. An email without a key gets a made-up one, derived from
//! a secret of the coordinator's, that is a real X25519 public key like any
//! other: answering "no key" would tell anyone which emails have accounts,
//! which sign-in takes care never to reveal.

use std::path::Path;

use mesh_core::sign_in::normalize_email;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

/// Mailbox public keys by normalized email.
pub struct MailboxKeys {
    connection: Mutex<Connection>,
    /// Makes up keys for emails without one. Derived from the coordinator's
    /// key, so a made-up key stays the same across restarts, like a real one.
    fake_key_secret: [u8; 32],
}

impl MailboxKeys {
    /// Opens the keys in `directory`, creating the table if needed.
    pub fn open(directory: &Path, fake_key_secret: [u8; 32]) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("mailbox_keys.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS mailbox_key (
                 email      TEXT PRIMARY KEY,
                 public_key BLOB NOT NULL
             );",
        )?;
        Ok(MailboxKeys {
            connection: Mutex::new(connection),
            fake_key_secret,
        })
    }

    /// Files `public_key` as the mailbox key of `email`, replacing any
    /// earlier one. The caller has checked the sign-in.
    pub async fn publish(&self, email: &str, public_key: &[u8; 32]) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO mailbox_key (email, public_key) VALUES (?1, ?2)
             ON CONFLICT (email) DO UPDATE SET public_key = excluded.public_key",
            params![normalize_email(email), public_key],
        )?;
        Ok(())
    }

    /// The mailbox key of `email`, made up when it has none.
    pub async fn key_of(&self, email: &str) -> rusqlite::Result<[u8; 32]> {
        let email = normalize_email(email);
        let connection = self.connection.lock().await;
        let held: Option<[u8; 32]> = connection
            .query_row(
                "SELECT public_key FROM mailbox_key WHERE email = ?1",
                params![email],
                |row| row.get(0),
            )
            .optional()?;
        Ok(held.unwrap_or_else(|| self.fake_key(&email)))
    }

    fn fake_key(&self, email: &str) -> [u8; 32] {
        let secret = blake3::keyed_hash(&self.fake_key_secret, email.as_bytes());
        // The public half of a secret key, so it can't be told apart from a
        // real one; random bytes could be, since not every 32 bytes are a
        // point on the curve.
        x25519(*secret.as_bytes(), X25519_BASEPOINT_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> (tempfile::TempDir, MailboxKeys) {
        let directory = tempfile::tempdir().unwrap();
        let keys = MailboxKeys::open(directory.path(), [9; 32]).unwrap();
        (directory, keys)
    }

    #[tokio::test]
    async fn a_published_key_is_found_under_its_email_in_any_case() {
        let (_directory, keys) = keys();

        keys.publish("Ana@Example.org", &[5; 32]).await.unwrap();

        assert_eq!(keys.key_of("ana@example.org").await.unwrap(), [5; 32]);
    }

    #[tokio::test]
    async fn an_email_without_a_key_gets_a_steady_made_up_one() {
        let (_directory, keys) = keys();

        let made_up = keys.key_of("bob@example.org").await.unwrap();

        assert_eq!(keys.key_of("bob@example.org").await.unwrap(), made_up);
        assert_ne!(keys.key_of("eve@example.org").await.unwrap(), made_up);
    }
}

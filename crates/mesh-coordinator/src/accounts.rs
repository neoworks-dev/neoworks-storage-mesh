//! Accounts for signing in, in `<data>/accounts.sqlite3`.
//!
//! Per account the coordinator keeps the salt and cost to stretch the
//! password with, the BLAKE3 hash of the auth key, and the account's secret
//! sealed under a key it never sees. It learns that an email has an account
//! and nothing about what the account stores or where.
//!
//! A stolen database still lets an attacker guess passwords offline, one
//! Argon2id run per guess. Deriving the password key through an OPRF with a
//! secret only the coordinator holds would close that; it is a follow-up.

use std::path::Path;

use mesh_core::sign_in::{KdfParams, SALT_BYTES, SignUpRequest, normalize_email};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// Accounts and what checking a sign-in needs.
pub struct Accounts {
    connection: Mutex<Connection>,
    /// Makes up salts for emails without an account, so they look like any
    /// other. Derived from the coordinator's key, so they stay the same
    /// across restarts.
    fake_salt_key: [u8; 32],
}

/// What became of a sign-up.
#[derive(Debug, PartialEq, Eq)]
pub enum SignedUp {
    Created,
    /// The email has an account already.
    Taken,
}

impl Accounts {
    /// Opens the accounts in `directory`, creating the table if needed.
    pub fn open(directory: &Path, fake_salt_key: [u8; 32]) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("accounts.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS account (
                 email         TEXT PRIMARY KEY,
                 salt          BLOB NOT NULL,
                 memory_kib    INTEGER NOT NULL,
                 iterations    INTEGER NOT NULL,
                 auth_hash     BLOB NOT NULL,
                 locked_secret BLOB NOT NULL
             );",
        )?;
        Ok(Accounts {
            connection: Mutex::new(connection),
            fake_salt_key,
        })
    }

    /// Creates the account `request` describes, unless its email has one.
    pub async fn sign_up(&self, request: &SignUpRequest) -> rusqlite::Result<SignedUp> {
        let connection = self.connection.lock().await;
        // Only the hash is kept: whoever reads the database learns nothing
        // they could sign in with.
        let auth_hash = blake3::hash(&request.auth_key);
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO account
                 (email, salt, memory_kib, iterations, auth_hash, locked_secret)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                normalize_email(&request.email),
                request.salt,
                request.kdf.memory_kib,
                request.kdf.iterations,
                auth_hash.as_bytes(),
                request.locked_secret
            ],
        )?;
        Ok(if inserted == 1 {
            SignedUp::Created
        } else {
            SignedUp::Taken
        })
    }

    /// The salt and cost of `email`, made up when it has no account.
    pub async fn salt(&self, email: &str) -> rusqlite::Result<([u8; SALT_BYTES], KdfParams)> {
        let email = normalize_email(email);
        let connection = self.connection.lock().await;
        let held = connection
            .query_row(
                "SELECT salt, memory_kib, iterations FROM account WHERE email = ?1",
                params![email],
                |row| {
                    let kdf = KdfParams {
                        memory_kib: row.get(1)?,
                        iterations: row.get(2)?,
                    };
                    Ok((row.get(0)?, kdf))
                },
            )
            .optional()?;
        Ok(held.unwrap_or_else(|| (self.fake_salt(&email), KdfParams::DEFAULT)))
    }

    /// The locked secret of `email`, when `auth_key` is the account's.
    pub async fn sign_in(
        &self,
        email: &str,
        auth_key: &[u8; 32],
    ) -> rusqlite::Result<Option<Vec<u8>>> {
        let connection = self.connection.lock().await;
        let held: Option<([u8; 32], Vec<u8>)> = connection
            .query_row(
                "SELECT auth_hash, locked_secret FROM account WHERE email = ?1",
                params![normalize_email(email)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((auth_hash, locked_secret)) = held else {
            return Ok(None);
        };
        // `blake3::Hash` compares in constant time, so the time an answer
        // takes says nothing about how much of a guessed key was right.
        if blake3::hash(auth_key) != blake3::Hash::from(auth_hash) {
            return Ok(None);
        }
        Ok(Some(locked_secret))
    }

    fn fake_salt(&self, email: &str) -> [u8; SALT_BYTES] {
        let hash = blake3::keyed_hash(&self.fake_salt_key, email.as_bytes());
        let mut salt = [0u8; SALT_BYTES];
        salt.copy_from_slice(&hash.as_bytes()[..SALT_BYTES]);
        salt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accounts() -> (tempfile::TempDir, Accounts) {
        let directory = tempfile::tempdir().unwrap();
        let accounts = Accounts::open(directory.path(), [9; 32]).unwrap();
        (directory, accounts)
    }

    fn request(email: &str) -> SignUpRequest {
        SignUpRequest {
            email: email.into(),
            salt: [1; SALT_BYTES],
            kdf: KdfParams {
                memory_kib: 8,
                iterations: 1,
            },
            auth_key: [2; 32],
            locked_secret: vec![3; 72],
        }
    }

    #[tokio::test]
    async fn an_email_has_one_account_whatever_its_case() {
        let (_directory, accounts) = accounts();

        assert_eq!(
            accounts.sign_up(&request("ana@example.org")).await.unwrap(),
            SignedUp::Created
        );
        assert_eq!(
            accounts
                .sign_up(&request(" Ana@Example.org"))
                .await
                .unwrap(),
            SignedUp::Taken
        );
    }

    #[tokio::test]
    async fn only_the_right_auth_key_gets_the_locked_secret() {
        let (_directory, accounts) = accounts();
        accounts.sign_up(&request("ana@example.org")).await.unwrap();

        assert_eq!(
            accounts.sign_in("ana@example.org", &[2; 32]).await.unwrap(),
            Some(vec![3; 72])
        );
        assert_eq!(
            accounts.sign_in("ana@example.org", &[4; 32]).await.unwrap(),
            None
        );
        assert_eq!(
            accounts.sign_in("bob@example.org", &[2; 32]).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn an_unknown_email_gets_a_steady_made_up_salt() {
        let (_directory, accounts) = accounts();
        accounts.sign_up(&request("ana@example.org")).await.unwrap();

        let (known, _) = accounts.salt("ana@example.org").await.unwrap();
        let (unknown, kdf) = accounts.salt("bob@example.org").await.unwrap();
        let (again, _) = accounts.salt("bob@example.org").await.unwrap();

        assert_eq!(known, [1; SALT_BYTES]);
        assert_eq!(unknown, again);
        assert_eq!(kdf, KdfParams::DEFAULT);
    }
}

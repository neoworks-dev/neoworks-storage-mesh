//! Issuing storage tokens with blind signatures and keeping the spent list,
//! in `<data>/tokens.sqlite3`.
//!
//! The coordinator makes one RSA key per token month, the first time the
//! month is asked for. Per account and month it counts how many tokens it
//! signed, which is all it learns: the messages it signs are blinded, so it
//! cannot tell later which token it issued to whom. Spending records the
//! token's message, so no token is spent twice; a month's spent list is
//! deleted once its tokens can no longer be spent.

use std::path::Path;

use blind_rsa_signatures::{
    BlindMessage, DefaultRng, KeyPairSha384PSSRandomized, SecretKeySha384PSSRandomized,
};
use mesh_core::sign_in::normalize_email;
use mesh_core::tokens::{HexBytes, Token};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// The size of each month's RSA key. RFC 9474 asks for at least 2048 bits.
const KEY_BITS: usize = 2048;

/// Token keys, issued counts and spent tokens.
pub struct TokenIssuer {
    connection: Mutex<Connection>,
}

/// Why tokens could not be issued.
#[derive(Debug, thiserror::Error)]
pub enum IssueError {
    #[error("the account has {left} tokens left this month")]
    OverAllowance { left: u64 },
    #[error("a blinded message is not one this key can sign")]
    BadBlindMessage,
    #[error("token keys: {0}")]
    Key(String),
    #[error("token table: {0}")]
    Database(#[from] rusqlite::Error),
}

/// What became of a token handed in to be spent.
#[derive(Debug, PartialEq, Eq)]
pub enum Spent {
    /// It was valid and is spent now.
    Now,
    /// It was spent before.
    Before,
    /// Its month is over, or its signature does not hold.
    Invalid,
}

impl TokenIssuer {
    /// Opens the token tables in `directory`, creating them if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("tokens.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS month_key (
                 month      INTEGER PRIMARY KEY,
                 secret_key BLOB NOT NULL,
                 public_key BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS issued (
                 email TEXT NOT NULL,
                 month INTEGER NOT NULL,
                 count INTEGER NOT NULL,
                 PRIMARY KEY (email, month)
             );
             CREATE TABLE IF NOT EXISTS spent (
                 month   INTEGER NOT NULL,
                 message BLOB NOT NULL,
                 PRIMARY KEY (month, message)
             );",
        )?;
        Ok(TokenIssuer {
            connection: Mutex::new(connection),
        })
    }

    /// The public key of `month` in DER, made the first time it is asked for.
    pub async fn public_key(&self, month: u64) -> Result<Vec<u8>, IssueError> {
        let connection = self.connection.lock().await;
        let (_, public_key) = key_of(&connection, month)?;
        Ok(public_key)
    }

    /// Signs `blinded` for `email` in `month`, if the account has that many
    /// tokens of its `allowance` left. The caller has checked the sign-in.
    pub async fn issue(
        &self,
        email: &str,
        month: u64,
        blinded: &[HexBytes],
        allowance: u64,
    ) -> Result<Vec<HexBytes>, IssueError> {
        let connection = self.connection.lock().await;
        let email = normalize_email(email);
        let issued: u64 = connection
            .query_row(
                "SELECT count FROM issued WHERE email = ?1 AND month = ?2",
                params![email, month],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let left = allowance.saturating_sub(issued);
        if blinded.len() as u64 > left {
            return Err(IssueError::OverAllowance { left });
        }
        let (secret_key, _) = key_of(&connection, month)?;
        let secret_key = SecretKeySha384PSSRandomized::from_der(&secret_key)
            .map_err(|error| IssueError::Key(error.to_string()))?;
        let mut signatures = Vec::with_capacity(blinded.len());
        for message in blinded {
            let signature = secret_key
                .blind_sign(BlindMessage(message.0.clone()))
                .map_err(|_| IssueError::BadBlindMessage)?;
            signatures.push(HexBytes(signature.0));
        }
        connection.execute(
            "INSERT INTO issued (email, month, count) VALUES (?1, ?2, ?3)
             ON CONFLICT (email, month) DO UPDATE SET count = count + excluded.count",
            params![email, month, blinded.len() as u64],
        )?;
        Ok(signatures)
    }

    /// The accounts that took tokens in the month before `month` but none in
    /// `month`: their data runs out unless they pay again.
    pub async fn lapsed_accounts(&self, month: u64) -> rusqlite::Result<Vec<String>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(
            "SELECT email FROM issued WHERE month + 1 = ?1
             AND email NOT IN (SELECT email FROM issued WHERE month = ?1)
             ORDER BY email",
        )?;
        let emails = statement.query_map(params![month], |row| row.get(0))?;
        emails.collect()
    }

    /// Notes that `email` took `count` tokens in `month`, without signing
    /// anything; for tests of what follows from issued counts.
    #[cfg(test)]
    pub async fn record_issued(&self, email: &str, month: u64, count: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO issued (email, month, count) VALUES (?1, ?2, ?3)",
            params![email, month, count],
        )?;
        Ok(())
    }

    /// Spends `token` at `current_month`, once.
    pub async fn spend(&self, token: &Token, current_month: u64) -> Result<Spent, IssueError> {
        let connection = self.connection.lock().await;
        // A token is valid in its month and the next.
        let is_valid_month = token.month == current_month || token.month + 1 == current_month;
        if !is_valid_month {
            return Ok(Spent::Invalid);
        }
        let (_, public_key) = key_of(&connection, token.month)?;
        if !token.is_signed_by(&public_key) {
            return Ok(Spent::Invalid);
        }
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO spent (month, message) VALUES (?1, ?2)",
            params![token.month, token.message],
        )?;
        // Tokens of older months can no longer be spent, so neither can
        // their spent list say anything.
        connection.execute(
            "DELETE FROM spent WHERE month + 1 < ?1",
            params![current_month],
        )?;
        Ok(if inserted == 1 {
            Spent::Now
        } else {
            Spent::Before
        })
    }
}

/// The secret and public key of `month` in DER, made if there is none yet.
fn key_of(connection: &Connection, month: u64) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    let held = connection
        .query_row(
            "SELECT secret_key, public_key FROM month_key WHERE month = ?1",
            params![month],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some(keys) = held {
        return Ok(keys);
    }
    let key_pair = KeyPairSha384PSSRandomized::generate(&mut DefaultRng, KEY_BITS)
        .map_err(|error| IssueError::Key(error.to_string()))?;
    let secret_key = key_pair
        .sk
        .to_der()
        .map_err(|error| IssueError::Key(error.to_string()))?;
    let public_key = key_pair
        .pk
        .to_der()
        .map_err(|error| IssueError::Key(error.to_string()))?;
    connection.execute(
        "INSERT INTO month_key (month, secret_key, public_key) VALUES (?1, ?2, ?3)",
        params![month, secret_key, public_key],
    )?;
    Ok((secret_key, public_key))
}

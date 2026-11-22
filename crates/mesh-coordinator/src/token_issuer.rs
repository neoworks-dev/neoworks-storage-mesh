//! Issuing storage tokens with blind signatures, the accounts' balances, and
//! the spent list, in `<data>/tokens.sqlite3`.
//!
//! The coordinator makes one RSA key per token month and kind, the first
//! time it is asked for. Paid tokens come out of an account's balance, which
//! buying and hosting fill; allowance tokens are free up to the monthly
//! allowance. Per account, month and kind the coordinator counts how many
//! tokens it signed, which is all it learns: the messages it signs are
//! blinded, so it cannot tell later which token it issued to whom. Spending
//! records the token's message, so no token is spent twice; a month's spent
//! list is deleted once its tokens can no longer be spent.

use std::path::Path;

use blind_rsa_signatures::{
    BlindMessage, DefaultRng, KeyPairSha384PSSRandomized, PublicKeySha384PSSRandomized,
    SecretKeySha384PSSRandomized,
};
use mesh_core::sign_in::normalize_email;
use mesh_core::tokens::{HexBytes, Token, TokenKind};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// The size of each month's RSA key. RFC 9474 asks for at least 2048 bits.
const KEY_BITS: usize = 2048;

/// Token keys, balances, issued counts and spent tokens.
pub struct TokenIssuer {
    connection: Mutex<Connection>,
}

/// Why tokens could not be issued.
#[derive(Debug, thiserror::Error)]
pub enum IssueError {
    #[error("the account has {left} allowance tokens left this month")]
    OverAllowance { left: u64 },
    #[error("the account's balance holds {balance} tokens")]
    OverBalance { balance: u64 },
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
            "CREATE TABLE IF NOT EXISTS token_key (
                 month      INTEGER NOT NULL,
                 kind       TEXT NOT NULL,
                 secret_key BLOB NOT NULL,
                 public_key BLOB NOT NULL,
                 PRIMARY KEY (month, kind)
             );
             CREATE TABLE IF NOT EXISTS issued_tokens (
                 email TEXT NOT NULL,
                 month INTEGER NOT NULL,
                 kind  TEXT NOT NULL,
                 count INTEGER NOT NULL,
                 PRIMARY KEY (email, month, kind)
             );
             CREATE TABLE IF NOT EXISTS balance (
                 email  TEXT PRIMARY KEY,
                 tokens INTEGER NOT NULL
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

    /// The public key of `month` and `kind` in DER, made the first time it is
    /// asked for.
    pub async fn public_key(&self, month: u64, kind: TokenKind) -> Result<Vec<u8>, IssueError> {
        let connection = self.connection.lock().await;
        let (_, public_key) = key_of(&connection, month, kind)?;
        Ok(public_key)
    }

    /// Adds `tokens` to the balance of `email`, for tokens bought or earned.
    pub async fn credit(&self, email: &str, tokens: u64) -> rusqlite::Result<u64> {
        let connection = self.connection.lock().await;
        connection.query_row(
            "INSERT INTO balance (email, tokens) VALUES (?1, ?2)
             ON CONFLICT (email) DO UPDATE SET tokens = tokens + excluded.tokens
             RETURNING tokens",
            params![normalize_email(email), tokens],
            |row| row.get(0),
        )
    }

    /// The paid tokens `email` can still take.
    #[cfg(test)]
    pub async fn balance(&self, email: &str) -> rusqlite::Result<u64> {
        let connection = self.connection.lock().await;
        balance_of(&connection, &normalize_email(email))
    }

    /// Signs `blinded` as tokens of `kind` for `email` in `month`, if the
    /// account has that many left: of its `allowance` for allowance tokens,
    /// of its balance for paid ones. The caller has checked the sign-in.
    pub async fn issue(
        &self,
        email: &str,
        month: u64,
        kind: TokenKind,
        blinded: &[HexBytes],
        allowance: u64,
    ) -> Result<Vec<HexBytes>, IssueError> {
        let mut connection = self.connection.lock().await;
        let email = normalize_email(email);
        let count = blinded.len() as u64;
        // One transaction, so the balance is taken only if the tokens are
        // signed, and the other way round.
        let transaction = connection.transaction()?;
        match kind {
            TokenKind::Allowance => {
                let left =
                    allowance.saturating_sub(issued_count(&transaction, &email, month, kind)?);
                if count > left {
                    return Err(IssueError::OverAllowance { left });
                }
            }
            TokenKind::Paid => {
                let balance = balance_of(&transaction, &email)?;
                if count > balance {
                    return Err(IssueError::OverBalance { balance });
                }
                transaction.execute(
                    "UPDATE balance SET tokens = tokens - ?2 WHERE email = ?1",
                    params![email, count],
                )?;
            }
        }
        let signatures = sign_blinded(&transaction, month, kind, blinded)?;
        transaction.execute(
            "INSERT INTO issued_tokens (email, month, kind, count) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (email, month, kind) DO UPDATE SET count = count + excluded.count",
            params![email, month, kind.as_str(), count],
        )?;
        transaction.commit()?;
        Ok(signatures)
    }

    /// A paid token of `month` for the coordinator's own use: paying nodes to
    /// keep its audit pieces.
    ///
    /// It goes through blinding like any account's token, so it looks the
    /// same to the node; no balance pays for it, since the coordinator is
    /// the one that issues paid tokens. When the node spends it, its value
    /// goes to the node's operator like any other.
    pub async fn mint_own(&self, month: u64) -> Result<Token, IssueError> {
        let connection = self.connection.lock().await;
        let (secret_key, public_key) = key_of(&connection, month, TokenKind::Paid)?;
        let key_error = |error: blind_rsa_signatures::Error| IssueError::Key(error.to_string());
        let secret_key = SecretKeySha384PSSRandomized::from_der(&secret_key).map_err(key_error)?;
        let public_key = PublicKeySha384PSSRandomized::from_der(&public_key).map_err(key_error)?;
        let mut message = [0u8; 32];
        getrandom::fill(&mut message).expect("the operating system's random source works");
        let blinding = public_key
            .blind(&mut DefaultRng, message)
            .map_err(key_error)?;
        let blind_signature = secret_key
            .blind_sign(&blinding.blind_message)
            .map_err(key_error)?;
        let signature = public_key
            .finalize(&blind_signature, &blinding, message)
            .map_err(key_error)?;
        // The randomized variant always sets a randomizer when blinding.
        let randomizer = blinding
            .msg_randomizer
            .ok_or_else(|| IssueError::Key("blinding set no randomizer".into()))?;
        Ok(Token {
            month,
            kind: TokenKind::Paid,
            message,
            randomizer: randomizer.0,
            signature: signature.0,
        })
    }

    /// The accounts that took tokens in the month before `month` but none in
    /// `month`: their data runs out unless they take tokens again.
    pub async fn lapsed_accounts(&self, month: u64) -> rusqlite::Result<Vec<String>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(
            "SELECT DISTINCT email FROM issued_tokens WHERE month + 1 = ?1
             AND email NOT IN (SELECT email FROM issued_tokens WHERE month = ?1)
             ORDER BY email",
        )?;
        let emails = statement.query_map(params![month], |row| row.get(0))?;
        emails.collect()
    }

    /// Notes that `email` took `count` paid tokens in `month`, without
    /// signing anything; for tests of what follows from issued counts.
    #[cfg(test)]
    pub async fn record_issued(&self, email: &str, month: u64, count: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "INSERT INTO issued_tokens (email, month, kind, count) VALUES (?1, ?2, 'paid', ?3)",
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
        let (_, public_key) = key_of(&connection, token.month, token.kind)?;
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

/// How many tokens of `kind` `email` took in `month`.
fn issued_count(
    connection: &Connection,
    email: &str,
    month: u64,
    kind: TokenKind,
) -> rusqlite::Result<u64> {
    Ok(connection
        .query_row(
            "SELECT count FROM issued_tokens WHERE email = ?1 AND month = ?2 AND kind = ?3",
            params![email, month, kind.as_str()],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

/// The balance of the normalized `email`.
fn balance_of(connection: &Connection, email: &str) -> rusqlite::Result<u64> {
    Ok(connection
        .query_row(
            "SELECT tokens FROM balance WHERE email = ?1",
            params![email],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

/// The blind signatures on `blinded` with the key of `month` and `kind`.
fn sign_blinded(
    connection: &Connection,
    month: u64,
    kind: TokenKind,
    blinded: &[HexBytes],
) -> Result<Vec<HexBytes>, IssueError> {
    let (secret_key, _) = key_of(connection, month, kind)?;
    let secret_key = SecretKeySha384PSSRandomized::from_der(&secret_key)
        .map_err(|error| IssueError::Key(error.to_string()))?;
    let mut signatures = Vec::with_capacity(blinded.len());
    for message in blinded {
        let signature = secret_key
            .blind_sign(BlindMessage(message.0.clone()))
            .map_err(|_| IssueError::BadBlindMessage)?;
        signatures.push(HexBytes(signature.0));
    }
    Ok(signatures)
}

/// The secret and public key of `month` and `kind` in DER, made if there is
/// none yet.
fn key_of(
    connection: &Connection,
    month: u64,
    kind: TokenKind,
) -> Result<(Vec<u8>, Vec<u8>), IssueError> {
    let held = connection
        .query_row(
            "SELECT secret_key, public_key FROM token_key WHERE month = ?1 AND kind = ?2",
            params![month, kind.as_str()],
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
        "INSERT INTO token_key (month, kind, secret_key, public_key) VALUES (?1, ?2, ?3, ?4)",
        params![month, kind.as_str(), secret_key, public_key],
    )?;
    Ok((secret_key, public_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issuer() -> (tempfile::TempDir, TokenIssuer) {
        let directory = tempfile::tempdir().unwrap();
        let issuer = TokenIssuer::open(directory.path()).unwrap();
        (directory, issuer)
    }

    #[tokio::test]
    async fn paid_tokens_come_out_of_the_balance() {
        let (_directory, issuer) = issuer();
        assert_eq!(issuer.credit("Ana@Example.org", 3).await.unwrap(), 3);
        // Not valid blinded messages, but the balance is checked first.
        let four = vec![HexBytes(vec![1]); 4];

        let result = issuer
            .issue("ana@example.org", 1, TokenKind::Paid, &four, 0)
            .await;

        assert!(matches!(
            result,
            Err(IssueError::OverBalance { balance: 3 })
        ));
        assert_eq!(issuer.balance("ana@example.org").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn a_refused_signature_takes_nothing_from_the_balance() {
        let (_directory, issuer) = issuer();
        issuer.credit("ana@example.org", 3).await.unwrap();
        let not_blinded = vec![HexBytes(vec![1])];

        let result = issuer
            .issue("ana@example.org", 1, TokenKind::Paid, &not_blinded, 0)
            .await;

        assert!(matches!(result, Err(IssueError::BadBlindMessage)));
        assert_eq!(issuer.balance("ana@example.org").await.unwrap(), 3);
    }
}

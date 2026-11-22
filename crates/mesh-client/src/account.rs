//! An account's keys, all derived from one secret.
//!
//! Every device of an account holds the same 32-byte secret, and derives
//! from it, each under its own BLAKE3 context:
//!
//! - the key that signs the account's index pointer,
//! - the dedup key its files' and commits' chunk keys come from,
//! - the message key that seals where the pointer leads,
//! - the mailbox key others seal messages to the account with.
//!
//! Knowing one derived key reveals nothing about the secret or the others.
//! The secret reaches a new device by signing in with an email and password
//! ([`sign_in`](crate::sign_in)), or as a hex string in a file.

use mesh_core::pointer::SigningKey;

use crate::encryption::{DedupKey, MessageKey};

/// An account's secret and the keys derived from it.
#[derive(Clone)]
pub struct Account {
    secret: [u8; 32],
}

/// A secret that is not 64 hex digits.
#[derive(Debug, thiserror::Error)]
#[error("an account secret is 64 hex digits")]
pub struct ParseAccountError;

impl Account {
    /// A new account with a random secret.
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).expect("the operating system's random source works");
        Account { secret }
    }

    /// The account whose secret is `secret`.
    pub fn from_secret(secret: [u8; 32]) -> Self {
        Account { secret }
    }

    /// The secret itself, for sealing it under a password.
    pub(crate) fn secret(&self) -> [u8; 32] {
        self.secret
    }

    /// The secret as 64 hex digits, for keeping it in a file.
    pub fn to_hex(&self) -> String {
        hex::encode(self.secret)
    }

    /// An account written by [`to_hex`](Self::to_hex).
    pub fn from_hex(text: &str) -> Result<Self, ParseAccountError> {
        let mut secret = [0u8; 32];
        hex::decode_to_slice(text.trim(), &mut secret).map_err(|_| ParseAccountError)?;
        Ok(Account { secret })
    }

    /// The key that signs the account's index pointer.
    pub fn pointer_key(&self) -> SigningKey {
        SigningKey::from_bytes(&self.derive("neoworks-mesh account pointer key v1"))
    }

    /// The public half of [`pointer_key`](Self::pointer_key), under which the
    /// pointer is filed.
    pub fn pointer_owner(&self) -> [u8; 32] {
        self.pointer_key().verifying_key().to_bytes()
    }

    /// The dedup key the account's chunk keys come from.
    pub fn dedup_key(&self) -> DedupKey {
        DedupKey::from_bytes(self.derive("neoworks-mesh account dedup key v1"))
    }

    /// The key that seals where the index pointer leads.
    pub fn message_key(&self) -> MessageKey {
        MessageKey::from_bytes(self.derive("neoworks-mesh account message key v1"))
    }

    /// The X25519 secret key messages to the account's mailboxes are sealed
    /// to ([`mailboxes`](crate::mailboxes)).
    pub fn mailbox_secret(&self) -> [u8; 32] {
        self.derive("neoworks-mesh account mailbox key v1")
    }

    fn derive(&self, context: &str) -> [u8; 32] {
        blake3::derive_key(context, &self.secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_secret_derives_the_same_keys() {
        let account = Account::generate();
        let again = Account::from_hex(&account.to_hex()).unwrap();

        assert_eq!(account.pointer_owner(), again.pointer_owner());
        assert!(account.dedup_key().chunk_key(b"x") == again.dedup_key().chunk_key(b"x"));
    }

    #[test]
    fn two_accounts_derive_different_keys() {
        assert_ne!(
            Account::generate().pointer_owner(),
            Account::generate().pointer_owner()
        );
    }
}

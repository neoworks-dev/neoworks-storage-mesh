//! What a device and the sign-in coordinator exchange.
//!
//! People sign in with an email and a password, though no server holds their
//! data. The device stretches the password with Argon2id under a salt the
//! coordinator keeps, and splits the result in two:
//!
//! ```text
//! Argon2id(password, salt) = auth key ‖ wrapping key
//! ```
//!
//! The auth key goes to the coordinator, which keeps only its hash and so can
//! check a sign-in without being able to make one. The wrapping key never
//! leaves the device: it seals the account's secret, and the coordinator
//! keeps that locked secret without being able to open it.
//!
//! - `POST /accounts` with a [`SignUpRequest`] creates an account, or answers
//!   `409 Conflict` when the email has one.
//! - `POST /sign-in/salt` with a [`SaltRequest`] returns a [`SaltResponse`].
//!   An email without an account gets a made-up salt that stays the same,
//!   so the answer does not tell who has an account.
//! - `POST /sign-in` with a [`SignInRequest`] returns a [`SignInResponse`],
//!   or `401 Unauthorized` when the auth key is wrong.

use serde::{Deserialize, Serialize};

/// The route that creates accounts.
pub const ACCOUNTS_ROUTE: &str = "/accounts";
/// The route that hands out an account's salt.
pub const SALT_ROUTE: &str = "/sign-in/salt";
/// The route that checks an auth key and hands out the locked secret.
pub const SIGN_IN_ROUTE: &str = "/sign-in";

/// The bytes of a salt.
pub const SALT_BYTES: usize = 16;

/// The most bytes a locked secret may have: a 32-byte secret sealed with a
/// nonce and tag takes 72, and the rest is room for a later format.
pub const MAX_LOCKED_SECRET_BYTES: usize = 256;

/// How hard Argon2id works on a password. Kept with the salt, so the cost can
/// rise for new accounts without breaking old ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KdfParams {
    /// Memory in KiB.
    pub memory_kib: u32,
    /// Passes over that memory.
    pub iterations: u32,
}

impl KdfParams {
    /// 64 MiB and three passes, which takes a phone about half a second and
    /// makes each guess at a stolen hash cost the same.
    pub const DEFAULT: KdfParams = KdfParams {
        memory_kib: 64 * 1024,
        iterations: 3,
    };
}

/// A new account.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignUpRequest {
    pub email: String,
    #[serde(with = "hex")]
    pub salt: [u8; SALT_BYTES],
    pub kdf: KdfParams,
    #[serde(with = "hex")]
    pub auth_key: [u8; 32],
    /// The account's secret, sealed under the wrapping key.
    #[serde(with = "hex")]
    pub locked_secret: Vec<u8>,
}

/// Asks for the salt of an email.
#[derive(Debug, Serialize, Deserialize)]
pub struct SaltRequest {
    pub email: String,
}

/// The salt and cost to stretch a password with.
#[derive(Debug, Serialize, Deserialize)]
pub struct SaltResponse {
    #[serde(with = "hex")]
    pub salt: [u8; SALT_BYTES],
    pub kdf: KdfParams,
}

/// A sign-in.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignInRequest {
    pub email: String,
    #[serde(with = "hex")]
    pub auth_key: [u8; 32],
}

/// The account's secret, still sealed under the wrapping key.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignInResponse {
    #[serde(with = "hex")]
    pub locked_secret: Vec<u8>,
}

/// An email in the one form accounts are kept under, so `Ana@Example.org `
/// and `ana@example.org` are one account.
pub fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

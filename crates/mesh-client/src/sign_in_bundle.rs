//! The account's secret locked under the password, kept in the mesh at a
//! place only the email and password lead to.
//!
//! Signing in on a new device needs the account's secret. Instead of one
//! coordinator keeping it, it sits in a signed pointer whose key, like the
//! key that locks it, comes from the email and password:
//!
//! ```text
//! Argon2id(password, salt = BLAKE3 key of the email) = pointer key ‖ bundle key
//! ```
//!
//! The pointer's target is the secret sealed under the bundle key. Any
//! device with the node list finds it from the email and password alone, so
//! signing in works through any coordinator's node list, or none. Without
//! the password nobody can tell where a person's bundle is, or whether they
//! have one.
//!
//! Anyone who collects every pointer can still guess passwords offline, as
//! with a leaked server database. [`BUNDLE_KDF`] makes each guess cost about
//! a second and 256 MiB on a phone.
//!
//! The salt depends on the email only, since there is no server to keep a
//! random one. Two people with the same password therefore still get
//! different places, but the cost settings are fixed: raising them later
//! moves every bundle, under a new salt context.

use std::time::Duration;

use argon2::{Algorithm, Argon2, Params, Version};
use mesh_core::pointer::{Pointer, SigningKey};
use mesh_core::sign_in::{KdfParams, normalize_email};

use crate::account::Account;
use crate::encryption::{MessageKey, open_message, seal_message};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;
use crate::pointers::{ReadPointerError, WritePointerError, read_pointer, write_pointer};

/// How hard Argon2id works on the password: 256 MiB and three passes, about
/// a second on a phone. Every device must use the same settings, since they
/// decide where the bundle is.
pub const BUNDLE_KDF: KdfParams = KdfParams {
    memory_kib: 256 * 1024,
    iterations: 3,
};

const SALT_CONTEXT: &str = "neoworks-mesh sign-in bundle salt v1";

/// How often storing retries after a node held a newer version, which only
/// happens when two devices write the bundle at the same moment.
const MAX_STORE_ATTEMPTS: usize = 3;

/// Where an account's bundle is, and the key that opens it.
#[derive(Clone)]
pub struct BundlePlace {
    pointer_key: SigningKey,
    bundle_key: MessageKey,
}

/// Why the bundle could not be found, opened or stored.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("these Argon2 settings cannot be used")]
    BadKdf,
    /// No bundle at the place, a bundle that does not open, or the empty
    /// one a password change leaves behind. All three look the same, so an
    /// answer never tells whether the email has an account.
    #[error("wrong email or password")]
    WrongPassword,
    #[error("reading the bundle's pointer: {0}")]
    Read(ReadPointerError),
    #[error("writing the bundle's pointer: {0}")]
    Write(#[from] WritePointerError),
}

impl BundlePlace {
    /// The place `email` and `password` lead to, with Argon2id at `kdf`.
    /// Real accounts use [`BUNDLE_KDF`].
    pub fn derive(email: &str, password: &str, kdf: KdfParams) -> Result<Self, BundleError> {
        let salt = blake3::derive_key(SALT_CONTEXT, normalize_email(email).as_bytes());
        // One lane, as for the coordinator's sign-in: a phone has few cores
        // to spare, and more lanes would only help an attacker with many.
        let params = Params::new(kdf.memory_kib, kdf.iterations, 1, Some(64))
            .map_err(|_| BundleError::BadKdf)?;
        let mut stretched = [0u8; 64];
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password_into(password.as_bytes(), &salt, &mut stretched)
            .map_err(|_| BundleError::BadKdf)?;
        let mut pointer_key = [0u8; 32];
        let mut bundle_key = [0u8; 32];
        pointer_key.copy_from_slice(&stretched[..32]);
        bundle_key.copy_from_slice(&stretched[32..]);
        Ok(BundlePlace {
            pointer_key: SigningKey::from_bytes(&pointer_key),
            bundle_key: MessageKey::from_bytes(bundle_key),
        })
    }

    /// The public key the bundle's pointer is filed under.
    pub fn owner(&self) -> [u8; 32] {
        self.pointer_key.verifying_key().to_bytes()
    }

    /// Both keys as 128 hex digits, pointer key first, for keeping them in
    /// the account's index so any device can renew the bundle.
    pub fn to_hex(&self) -> String {
        format!(
            "{}{}",
            hex::encode(self.pointer_key.to_bytes()),
            hex::encode(self.bundle_key.to_bytes())
        )
    }

    /// A place written by [`to_hex`](Self::to_hex).
    pub fn from_hex(text: &str) -> Option<Self> {
        let mut bytes = [0u8; 64];
        hex::decode_to_slice(text, &mut bytes).ok()?;
        let (pointer_key, bundle_key) = bytes.split_at(32);
        Some(BundlePlace {
            pointer_key: SigningKey::from_bytes(pointer_key.try_into().ok()?),
            bundle_key: MessageKey::from_bytes(bundle_key.try_into().ok()?),
        })
    }
}

/// Stores `account`'s secret at `place` for `lease`. Storing it again, at a
/// higher version, is also how the bundle is renewed.
pub async fn store_bundle(
    client: &NodeClient,
    list: &NodeList,
    place: &BundlePlace,
    account: &Account,
    lease: Duration,
) -> Result<(), BundleError> {
    let sealed = seal_message(&place.bundle_key, &account.secret());
    write_newest(client, list, place, sealed, lease).await
}

/// The account whose bundle is at `place`.
pub async fn open_bundle(
    client: &NodeClient,
    list: &NodeList,
    place: &BundlePlace,
) -> Result<Account, BundleError> {
    let read = match read_pointer(client, list, &place.owner(), None).await {
        Ok(read) => read,
        Err(ReadPointerError::NotFound) => return Err(BundleError::WrongPassword),
        Err(error) => return Err(BundleError::Read(error)),
    };
    let secret = open_message(&place.bundle_key, &read.pointer.target)
        .map_err(|_| BundleError::WrongPassword)?;
    let secret: [u8; 32] = secret.try_into().map_err(|_| BundleError::WrongPassword)?;
    Ok(Account::from_secret(secret))
}

/// Moves `account`'s bundle from `old`, the place of the old password, to
/// `new`, and leaves an empty pointer at `old`.
///
/// Nodes keep the old bundle until its lease ends unless it is replaced, so
/// the empty pointer goes out at a higher version. The device still knows
/// the old password at this moment, which is what lets it sign that.
pub async fn change_password(
    client: &NodeClient,
    list: &NodeList,
    account: &Account,
    old: &BundlePlace,
    new: &BundlePlace,
    lease: Duration,
) -> Result<(), BundleError> {
    // The new bundle goes first: if the device stops in between, both
    // passwords still work, rather than neither.
    store_bundle(client, list, new, account, lease).await?;
    write_newest(client, list, old, Vec::new(), lease).await
}

/// Writes `target` at `place`, one version above the newest one found.
async fn write_newest(
    client: &NodeClient,
    list: &NodeList,
    place: &BundlePlace,
    target: Vec<u8>,
    lease: Duration,
) -> Result<(), BundleError> {
    let mut version = match read_pointer(client, list, &place.owner(), None).await {
        Ok(read) => read.pointer.version + 1,
        Err(ReadPointerError::NotFound) => 1,
        Err(error) => return Err(BundleError::Read(error)),
    };
    let mut attempts = 0;
    loop {
        attempts += 1;
        // A sealed 32-byte secret is 72 bytes, far below the target limit.
        let pointer = Pointer::sign(&place.pointer_key, version, target.clone())
            .expect("a bundle fits in a pointer");
        match write_pointer(client, list, &pointer, lease).await {
            Ok(_) => return Ok(()),
            Err(WritePointerError::NotNewest { held_version, .. })
                if attempts < MAX_STORE_ATTEMPTS =>
            {
                version = held_version + 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHEAP: KdfParams = KdfParams {
        memory_kib: 8,
        iterations: 1,
    };

    #[test]
    fn the_same_email_and_password_lead_to_the_same_place() {
        let place = BundlePlace::derive("Ana@Example.org ", "correct horse", CHEAP).unwrap();
        let again = BundlePlace::derive("ana@example.org", "correct horse", CHEAP).unwrap();

        assert_eq!(place.owner(), again.owner());
        assert_eq!(place.to_hex(), again.to_hex());
    }

    #[test]
    fn another_email_or_password_leads_elsewhere() {
        let place = BundlePlace::derive("ana@example.org", "correct horse", CHEAP).unwrap();
        let other_password =
            BundlePlace::derive("ana@example.org", "battery staple", CHEAP).unwrap();
        let other_email = BundlePlace::derive("bob@example.org", "correct horse", CHEAP).unwrap();

        assert_ne!(place.owner(), other_password.owner());
        assert_ne!(place.owner(), other_email.owner());
    }

    #[test]
    fn a_place_survives_being_kept_as_hex() {
        let place = BundlePlace::derive("ana@example.org", "correct horse", CHEAP).unwrap();

        let again = BundlePlace::from_hex(&place.to_hex()).unwrap();

        assert_eq!(again.to_hex(), place.to_hex());
        assert!(BundlePlace::from_hex("abc").is_none());
    }
}

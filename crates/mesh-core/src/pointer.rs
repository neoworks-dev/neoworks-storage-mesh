//! Signed pointers: a fixed place to find the newest version of something.
//!
//! Everything else in the mesh is addressed by its hash, so every new version
//! of an object, such as a device's index, gets a new address. A pointer is
//! the one record whose place does not change: it is filed under its owner's
//! public key and says where the newest version is. The owner replaces it
//! with a higher version after every change.
//!
//! The record is signed with the owner's key, so nodes, which are not trusted,
//! cannot write pointers of their own. A node can still hand out an older
//! record that was validly signed back then; the version number is what lets
//! nodes and clients tell which record is newer.
//!
//! The signature covers:
//!
//! ```text
//! "neoworks-mesh pointer v1" ‖ owner ‖ version as u64, big-endian ‖ target
//! ```
//!
//! The leading text keeps a pointer signature from being valid as any other
//! message the same key might sign. Owner and version have fixed lengths, so
//! the target is simply the rest.

use ed25519_dalek::{Signature, Signer, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::canonical_hex::parse_canonical_hex;

/// The key a pointer's owner signs with.
pub use ed25519_dalek::SigningKey;

/// The most bytes a pointer's target may have.
///
/// A manifest takes about 1.2 KB per chunk, so this holds the manifest of an
/// object of about 50 chunks. Anything larger is reached in two steps: the
/// target names a manifest stored as an object of its own. The limit keeps
/// nodes from being made to store large blobs as pointers.
pub const MAX_TARGET_BYTES: usize = 64 * 1024;

const SIGNATURE_CONTEXT: &[u8] = b"neoworks-mesh pointer v1";

/// A signed record saying where the newest version of its owner's object is.
///
/// A pointer that comes from outside, such as from a node, is unchecked until
/// [`Pointer::verify`] succeeds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    /// The owner's Ed25519 public key, under which the pointer is filed.
    #[serde(with = "hex")]
    pub owner: [u8; 32],
    /// Goes up by at least one with every new record of the same owner.
    pub version: u64,
    /// Where the newest version is, encrypted by the owner. Nodes cannot
    /// read it.
    #[serde(with = "hex")]
    pub target: Vec<u8>,
    /// The owner's signature over the other fields.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

/// A pointer that cannot be made or is not genuine.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PointerError {
    #[error("the target is {len} bytes, more than the {MAX_TARGET_BYTES} a pointer may hold")]
    TargetTooLarge { len: usize },
    #[error("the owner is not a valid Ed25519 public key")]
    InvalidOwner,
    #[error("the signature was not made by the owner over this record")]
    BadSignature,
}

impl Pointer {
    /// A pointer to `target` at `version`, signed with `key`.
    pub fn sign(key: &SigningKey, version: u64, target: Vec<u8>) -> Result<Pointer, PointerError> {
        check_target_len(&target)?;
        let owner = key.verifying_key().to_bytes();
        let signature = key.sign(&signed_message(&owner, version, &target));
        Ok(Pointer {
            owner,
            version,
            target,
            signature: signature.to_bytes(),
        })
    }

    /// Checks that the owner signed exactly this record and that it is within
    /// the size limit.
    pub fn verify(&self) -> Result<(), PointerError> {
        check_target_len(&self.target)?;
        let owner =
            VerifyingKey::from_bytes(&self.owner).map_err(|_| PointerError::InvalidOwner)?;
        let message = signed_message(&self.owner, self.version, &self.target);
        // Strict verification also rejects weak keys and signatures that were
        // altered into another valid encoding, so a record has exactly one
        // valid signature and cannot be passed off in a second form.
        owner
            .verify_strict(&message, &Signature::from_bytes(&self.signature))
            .map_err(|_| PointerError::BadSignature)
    }
}

/// The owner key written as 64 lowercase hex digits, as it is in URLs, or
/// `None` for any other text.
pub fn parse_owner(text: &str) -> Option<[u8; 32]> {
    parse_canonical_hex(text)
}

fn check_target_len(target: &[u8]) -> Result<(), PointerError> {
    if target.len() > MAX_TARGET_BYTES {
        return Err(PointerError::TargetTooLarge { len: target.len() });
    }
    Ok(())
}

/// The bytes a pointer's signature covers.
fn signed_message(owner: &[u8; 32], version: u64, target: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(SIGNATURE_CONTEXT.len() + 32 + 8 + target.len());
    message.extend_from_slice(SIGNATURE_CONTEXT);
    message.extend_from_slice(owner);
    message.extend_from_slice(&version.to_be_bytes());
    message.extend_from_slice(target);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn a_signed_pointer_verifies() {
        let pointer = Pointer::sign(&key(1), 7, b"index manifest".to_vec()).unwrap();

        assert_eq!(pointer.owner, key(1).verifying_key().to_bytes());
        assert_eq!(pointer.verify(), Ok(()));
    }

    #[test]
    fn changing_any_field_breaks_the_signature() {
        let pointer = Pointer::sign(&key(1), 7, b"index manifest".to_vec()).unwrap();

        let mut newer = pointer.clone();
        newer.version = 8;
        let mut retargeted = pointer.clone();
        retargeted.target = b"somewhere else".to_vec();
        let mut other_owner = pointer.clone();
        other_owner.owner = key(2).verifying_key().to_bytes();

        assert_eq!(newer.verify(), Err(PointerError::BadSignature));
        assert_eq!(retargeted.verify(), Err(PointerError::BadSignature));
        assert_eq!(other_owner.verify(), Err(PointerError::BadSignature));
    }

    #[test]
    fn a_node_cannot_sign_for_someone_else() {
        // A node signs with its own key and claims the owner's.
        let mut forged = Pointer::sign(&key(9), 100, b"junk".to_vec()).unwrap();
        forged.owner = key(1).verifying_key().to_bytes();

        assert_eq!(forged.verify(), Err(PointerError::BadSignature));
    }

    #[test]
    fn an_owner_that_is_no_key_is_rejected() {
        let mut pointer = Pointer::sign(&key(1), 1, Vec::new()).unwrap();
        // The y coordinate 2 has no point on the curve.
        pointer.owner = [0; 32];
        pointer.owner[0] = 2;

        assert_eq!(pointer.verify(), Err(PointerError::InvalidOwner));
    }

    #[test]
    fn targets_over_the_limit_are_refused() {
        let too_large = vec![0; MAX_TARGET_BYTES + 1];

        assert_eq!(
            Pointer::sign(&key(1), 1, too_large).unwrap_err(),
            PointerError::TargetTooLarge {
                len: MAX_TARGET_BYTES + 1
            }
        );
    }

    #[test]
    fn owners_parse_only_in_canonical_form() {
        let owner = key(1).verifying_key().to_bytes();
        let text = hex::encode(owner);

        assert_eq!(parse_owner(&text), Some(owner));
        assert_eq!(parse_owner(&text.to_uppercase()), None);
        assert_eq!(parse_owner(&text[1..]), None);
        assert_eq!(parse_owner("../etc/passwd"), None);
    }

    #[test]
    fn survives_a_json_round_trip() {
        let pointer = Pointer::sign(&key(1), 7, b"index manifest".to_vec()).unwrap();

        let json = serde_json::to_string(&pointer).unwrap();
        let parsed: Pointer = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, pointer);
        assert_eq!(parsed.verify(), Ok(()));
    }
}

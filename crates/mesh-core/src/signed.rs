//! Records signed with an Ed25519 key: what nodes say about themselves, the
//! node list a coordinator publishes, and the list of coordinators.
//!
//! The signature covers:
//!
//! ```text
//! context ‖ the body as JSON
//! ```
//!
//! Each kind of body has its own context, such as
//! `"neoworks-mesh node record v1"`, so a signature made for one kind is never
//! valid for another. Serializing a body with `serde_json` always gives the
//! same bytes for the same values, so a verifier can rebuild exactly what was
//! signed from the parsed body. Bodies therefore hold no floats and no maps,
//! whose JSON could come out differently.

use ed25519_dalek::{Signature, Signer, VerifyingKey};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The key records are signed with.
pub use ed25519_dalek::SigningKey;

/// A body that can be signed, with the context that sets its signatures
/// apart from every other kind.
pub trait SignedBody: Serialize + DeserializeOwned {
    /// Text prepended to the body before signing, unique per kind.
    const CONTEXT: &'static str;
}

/// A body, the public key of whoever signed it, and the signature.
///
/// A record that comes from outside is unchecked until [`Signed::verify`]
/// succeeds; it says nothing about whether the signer is anyone to trust.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "T: SignedBody")]
pub struct Signed<T: SignedBody> {
    pub body: T,
    /// The signer's Ed25519 public key.
    #[serde(with = "hex")]
    pub signer: [u8; 32],
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

/// A signature that does not hold.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SignatureError {
    #[error("the signer is not a valid Ed25519 public key")]
    InvalidSigner,
    #[error("the signature was not made by the signer over this record")]
    BadSignature,
}

impl<T: SignedBody> Signed<T> {
    /// `body`, signed with `key`.
    pub fn sign(key: &SigningKey, body: T) -> Self {
        let signature = key.sign(&signed_message(&body));
        Signed {
            body,
            signer: key.verifying_key().to_bytes(),
            signature: signature.to_bytes(),
        }
    }

    /// Checks that the signer signed exactly this body.
    pub fn verify(&self) -> Result<(), SignatureError> {
        let signer =
            VerifyingKey::from_bytes(&self.signer).map_err(|_| SignatureError::InvalidSigner)?;
        // Strict verification also rejects weak keys and signatures altered
        // into a second valid encoding.
        signer
            .verify_strict(
                &signed_message(&self.body),
                &Signature::from_bytes(&self.signature),
            )
            .map_err(|_| SignatureError::BadSignature)
    }
}

/// The bytes a signature over `body` covers.
fn signed_message<T: SignedBody>(body: &T) -> Vec<u8> {
    let mut message = T::CONTEXT.as_bytes().to_vec();
    // Bodies are plain structs of strings, integers and lists, which always
    // serialize.
    let json = serde_json::to_vec(body).expect("a signed body serializes");
    message.extend_from_slice(&json);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Note {
        text: String,
    }

    impl SignedBody for Note {
        const CONTEXT: &'static str = "neoworks-mesh test note v1";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct OtherNote {
        text: String,
    }

    impl SignedBody for OtherNote {
        const CONTEXT: &'static str = "neoworks-mesh other test note v1";
    }

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[3; 32])
    }

    #[test]
    fn a_signed_body_verifies_after_a_json_round_trip() {
        let signed = Signed::sign(&key(), Note { text: "hi".into() });

        let json = serde_json::to_string(&signed).unwrap();
        let parsed: Signed<Note> = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.verify(), Ok(()));
    }

    #[test]
    fn a_changed_body_does_not_verify() {
        let mut signed = Signed::sign(&key(), Note { text: "hi".into() });
        signed.body.text = "bye".into();

        assert_eq!(signed.verify(), Err(SignatureError::BadSignature));
    }

    #[test]
    fn a_signature_is_not_valid_for_another_kind_of_body() {
        let signed = Signed::sign(&key(), Note { text: "hi".into() });

        let relabeled = Signed {
            body: OtherNote {
                text: signed.body.text.clone(),
            },
            signer: signed.signer,
            signature: signed.signature,
        };

        assert_eq!(relabeled.verify(), Err(SignatureError::BadSignature));
    }
}

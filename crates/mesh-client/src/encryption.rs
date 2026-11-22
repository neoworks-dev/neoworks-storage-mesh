//! Encrypting chunks before they leave the device, so that the same chunk
//! always encrypts the same way for its owner.
//!
//! Each chunk is sealed with XChaCha20-Poly1305 under its own key, and that key
//! is a keyed hash of the chunk's bytes under the owner's [`DedupKey`]:
//!
//! ```text
//! chunk key = BLAKE3-keyed(dedup key, chunk)
//! sealed    = XChaCha20-Poly1305(chunk key, fixed nonce, padded chunk)
//! ```
//!
//! The same chunk therefore always becomes the same sealed bytes and the same
//! pieces, so it is stored once however many files or versions hold it. The
//! dedup key belongs to the owner alone, so two people storing the same chunk
//! get unrelated ciphertexts, and nobody without the key can tell whether
//! someone holds a known file.
//!
//! A sealed chunk is the ciphertext followed by a 16-byte tag; any change to
//! it makes opening fail.

use chacha20poly1305::aead::{Aead, Generate, Key, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Bytes sealing adds to a chunk: the authentication tag.
pub const SEAL_OVERHEAD: usize = 16;

/// Bound into every sealed chunk, so the format can change without old and new
/// chunks ever being mistaken for one another.
const ASSOCIATED_DATA: &[u8] = b"neoworks-storage-mesh/chunk/v2";

/// The owner's secret for deriving chunk keys. Whoever holds it can test
/// whether the owner stored a given chunk, so it never leaves the owner's
/// devices, not even inside a manifest.
#[derive(Clone)]
pub struct DedupKey([u8; 32]);

/// The key of one chunk. Whoever holds it can read that chunk, so it only ever
/// leaves the device inside a manifest the owner keeps.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ChunkKey([u8; 32]);

/// A sealed chunk that does not open: the key is wrong or the bytes changed.
#[derive(Debug, thiserror::Error)]
#[error("the chunk does not decrypt")]
pub struct OpenError;

/// A dedup key that is not 64 hex digits.
#[derive(Debug, thiserror::Error)]
#[error("a dedup key is 64 hex digits")]
pub struct ParseDedupKeyError;

impl DedupKey {
    /// A new random key from the operating system's random source.
    pub fn generate() -> Self {
        DedupKey(Key::<XChaCha20Poly1305>::generate().into())
    }

    /// The key as 64 hex digits, for keeping it in a file.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// A key written by [`to_hex`](Self::to_hex).
    pub fn from_hex(text: &str) -> Result<Self, ParseDedupKeyError> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(text.trim(), &mut bytes).map_err(|_| ParseDedupKeyError)?;
        Ok(DedupKey(bytes))
    }

    /// The key of a chunk with these bytes.
    pub fn chunk_key(&self, chunk: &[u8]) -> ChunkKey {
        ChunkKey(*blake3::keyed_hash(&self.0, chunk).as_bytes())
    }
}

impl ChunkKey {
    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&Key::<XChaCha20Poly1305>::from(self.0))
    }
}

/// Seals a padded chunk under its key.
pub fn seal(key: &ChunkKey, padded_chunk: &[u8]) -> Vec<u8> {
    key.cipher()
        .encrypt(&fixed_nonce(), payload(padded_chunk))
        // Encryption only fails for messages beyond 256 GiB; chunks are a few MiB.
        .expect("a chunk is far below the cipher's length limit")
}

/// Opens a sealed chunk, returning the padded chunk.
pub fn open(key: &ChunkKey, sealed: &[u8]) -> Result<Vec<u8>, OpenError> {
    key.cipher()
        .decrypt(&fixed_nonce(), payload(sealed))
        .map_err(|_| OpenError)
}

/// The nonce every chunk is sealed with.
///
/// A nonce must never be used twice with one key for different messages.
/// Each chunk key is derived from the chunk's own bytes, so a key only ever
/// seals that one chunk, and sealing it again gives the same bytes on purpose.
fn fixed_nonce() -> XNonce {
    XNonce::from([0u8; 24])
}

fn payload(message: &[u8]) -> Payload<'_, 'static> {
    Payload {
        msg: message,
        aad: ASSOCIATED_DATA,
    }
}

impl Serialize for ChunkKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ChunkKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(&text, &mut bytes).map_err(serde::de::Error::custom)?;
        Ok(ChunkKey(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sealed_chunk_opens_with_its_key() {
        let key = DedupKey::generate().chunk_key(b"holiday photo bytes");
        let sealed = seal(&key, b"holiday photo bytes");

        assert_eq!(open(&key, &sealed).unwrap(), b"holiday photo bytes");
        assert_eq!(sealed.len(), b"holiday photo bytes".len() + SEAL_OVERHEAD);
    }

    #[test]
    fn opening_fails_with_another_key_or_changed_bytes() {
        let dedup_key = DedupKey::generate();
        let key = dedup_key.chunk_key(b"holiday photo bytes");
        let mut sealed = seal(&key, b"holiday photo bytes");

        assert!(open(&dedup_key.chunk_key(b"other bytes"), &sealed).is_err());
        sealed[3] ^= 1;
        assert!(open(&key, &sealed).is_err());
    }

    #[test]
    fn one_owner_seals_a_chunk_the_same_way_every_time() {
        let dedup_key = DedupKey::generate();
        let first = seal(&dedup_key.chunk_key(b"chunk"), b"chunk");
        let second = seal(&dedup_key.chunk_key(b"chunk"), b"chunk");

        assert_eq!(first, second);
    }

    #[test]
    fn two_owners_seal_the_same_chunk_differently() {
        let alice = DedupKey::generate();
        let bob = DedupKey::generate();

        let from_alice = seal(&alice.chunk_key(b"chunk"), b"chunk");
        let from_bob = seal(&bob.chunk_key(b"chunk"), b"chunk");

        assert_ne!(from_alice, from_bob);
    }

    #[test]
    fn a_dedup_key_survives_being_written_down() {
        let key = DedupKey::generate();
        let again = DedupKey::from_hex(&key.to_hex()).unwrap();

        // Keys have no `Debug`, so they can't end up in logs; compare directly.
        assert!(key.chunk_key(b"chunk") == again.chunk_key(b"chunk"));
    }
}

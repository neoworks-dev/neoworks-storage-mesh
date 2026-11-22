//! Encrypting a file's chunks before they leave the device.
//!
//! Every file gets its own random key. Each chunk is sealed with
//! XChaCha20-Poly1305 under that key and a fresh random nonce, so the sealed
//! bytes look random and any change to them makes opening fail. A sealed chunk
//! is laid out as `nonce (24 bytes) ‖ ciphertext ‖ tag (16 bytes)`.

use chacha20poly1305::aead::{Aead, Generate, Key, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Bytes of the nonce at the start of every sealed chunk.
const NONCE_BYTES: usize = 24;

/// Bytes sealing adds to a chunk: the nonce and the authentication tag.
pub const SEAL_OVERHEAD: usize = NONCE_BYTES + 16;

/// The random key of one file. Whoever holds it can read the file, so it only
/// ever leaves the device inside a manifest the owner keeps.
#[derive(Clone, PartialEq, Eq)]
pub struct FileKey([u8; 32]);

/// A sealed chunk that does not open: the key is wrong, or the bytes or their
/// position were changed.
#[derive(Debug, thiserror::Error)]
#[error("chunk {index} does not decrypt")]
pub struct OpenError {
    pub index: u32,
}

impl FileKey {
    /// A new random key from the operating system's random source.
    pub fn generate() -> Self {
        FileKey(Key::<XChaCha20Poly1305>::generate().into())
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&Key::<XChaCha20Poly1305>::from(self.0))
    }
}

/// Seals chunk number `index` of a file.
pub fn seal_chunk(key: &FileKey, index: u32, plaintext: &[u8]) -> Vec<u8> {
    // Random 192-bit nonces never repeat in practice, so one key can seal any
    // number of chunks.
    let nonce = XNonce::generate();
    let associated_data = chunk_associated_data(index);
    let ciphertext = key
        .cipher()
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &associated_data,
            },
        )
        // Encryption only fails for messages beyond 256 GiB; chunks are a few MiB.
        .expect("a chunk is far below the cipher's length limit");
    let mut sealed = Vec::with_capacity(NONCE_BYTES + ciphertext.len());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&ciphertext);
    sealed
}

/// Opens chunk number `index` of a file.
pub fn open_chunk(key: &FileKey, index: u32, sealed: &[u8]) -> Result<Vec<u8>, OpenError> {
    if sealed.len() < SEAL_OVERHEAD {
        return Err(OpenError { index });
    }
    let (nonce, ciphertext) = sealed.split_at(NONCE_BYTES);
    let nonce = XNonce::try_from(nonce).map_err(|_| OpenError { index })?;
    let associated_data = chunk_associated_data(index);
    key.cipher()
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &associated_data,
            },
        )
        .map_err(|_| OpenError { index })
}

/// The associated data binds a sealed chunk to its position in the file: chunk
/// 3 sealed as chunk 3 does not open as chunk 2. Pieces are already addressed by
/// their hash, so nodes cannot reorder them; this guards against a manifest
/// that lists chunks out of order.
fn chunk_associated_data(index: u32) -> Vec<u8> {
    let mut associated_data = b"neoworks-storage-mesh/chunk/v1".to_vec();
    associated_data.extend_from_slice(&index.to_be_bytes());
    associated_data
}

impl Serialize for FileKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for FileKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(&text, &mut bytes).map_err(serde::de::Error::custom)?;
        Ok(FileKey(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sealed_chunk_opens_with_its_key_and_index() {
        let key = FileKey::generate();
        let sealed = seal_chunk(&key, 7, b"holiday photo bytes");

        assert_eq!(
            open_chunk(&key, 7, &sealed).unwrap(),
            b"holiday photo bytes"
        );
        assert_eq!(sealed.len(), b"holiday photo bytes".len() + SEAL_OVERHEAD);
    }

    #[test]
    fn opening_fails_with_another_key_index_or_changed_bytes() {
        let key = FileKey::generate();
        let mut sealed = seal_chunk(&key, 7, b"holiday photo bytes");

        assert!(open_chunk(&FileKey::generate(), 7, &sealed).is_err());
        assert!(open_chunk(&key, 8, &sealed).is_err());
        sealed[30] ^= 1;
        assert!(open_chunk(&key, 7, &sealed).is_err());
    }
}

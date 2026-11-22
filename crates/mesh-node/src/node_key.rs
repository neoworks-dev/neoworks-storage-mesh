//! The node's own Ed25519 key pair, which its id is derived from.
//!
//! The key is created on first start and kept in `<data>/node.key` as 64 hex
//! digits, readable by the node's user only. Losing it gives the node a new
//! id, and every piece and pointer stored under the old one has to be found
//! again by repair. The key is the node's alone: it is never a user's key, so
//! running a node does not link its operator to anything it stores.

use std::io::{self, Write};
use std::path::Path;

use mesh_core::pointer::SigningKey;

/// The node key file that could not be used.
#[derive(Debug, thiserror::Error)]
pub enum NodeKeyError {
    #[error("node key file: {0}")]
    Io(#[from] io::Error),
    #[error("the node key file does not hold 64 hex digits")]
    Malformed,
}

/// The key in `<directory>/node.key`, or a new one written there.
pub fn load_or_create(directory: &Path) -> Result<SigningKey, NodeKeyError> {
    let path = directory.join("node.key");
    match std::fs::read_to_string(&path) {
        Ok(text) => parse(&text),
        Err(error) if error.kind() == io::ErrorKind::NotFound => create(&path),
        Err(error) => Err(error.into()),
    }
}

fn parse(text: &str) -> Result<SigningKey, NodeKeyError> {
    let mut seed = [0u8; 32];
    hex::decode_to_slice(text.trim(), &mut seed).map_err(|_| NodeKeyError::Malformed)?;
    Ok(SigningKey::from_bytes(&seed))
}

fn create(path: &Path) -> Result<SigningKey, NodeKeyError> {
    let mut seed = [0u8; 32];
    // A node that cannot draw randomness cannot make a safe key either.
    getrandom::fill(&mut seed).expect("the operating system's random source works");
    let mut options = std::fs::OpenOptions::new();
    // `create_new` fails rather than overwrite a key another start wrote in
    // the meantime, which would give the node two ids.
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path)?;
    writeln!(file, "{}", hex::encode(seed))?;
    Ok(SigningKey::from_bytes(&seed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_survives_a_restart() {
        let directory = tempfile::tempdir().unwrap();

        let first = load_or_create(directory.path()).unwrap();
        let again = load_or_create(directory.path()).unwrap();

        assert_eq!(first.to_bytes(), again.to_bytes());
    }

    #[test]
    fn every_node_gets_its_own_key() {
        let one = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();

        let one_key = load_or_create(one.path()).unwrap();
        let other_key = load_or_create(other.path()).unwrap();

        assert_ne!(one_key.to_bytes(), other_key.to_bytes());
    }

    #[test]
    fn a_damaged_key_file_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("node.key"), "not a key").unwrap();

        let result = load_or_create(directory.path());

        assert!(matches!(result, Err(NodeKeyError::Malformed)));
    }
}

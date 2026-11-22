//! The coordinator's Ed25519 key, which signs the node list.
//!
//! The key is created on first start and kept in `<data>/coordinator.key` as
//! 64 hex digits, readable by the coordinator's user only. Its public half
//! goes into the coordinator list clients start with; losing the key means
//! publishing a new coordinator list signed by another coordinator.

use std::io::{self, Write};
use std::path::Path;

use mesh_core::signed::SigningKey;

/// The key file that could not be used.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorKeyError {
    #[error("coordinator key file: {0}")]
    Io(#[from] io::Error),
    #[error("the coordinator key file does not hold 64 hex digits")]
    Malformed,
}

/// The key in `<directory>/coordinator.key`, or a new one written there.
pub fn load_or_create(directory: &Path) -> Result<SigningKey, CoordinatorKeyError> {
    let path = directory.join("coordinator.key");
    match std::fs::read_to_string(&path) {
        Ok(text) => parse(&text),
        Err(error) if error.kind() == io::ErrorKind::NotFound => create(&path),
        Err(error) => Err(error.into()),
    }
}

fn parse(text: &str) -> Result<SigningKey, CoordinatorKeyError> {
    let mut seed = [0u8; 32];
    hex::decode_to_slice(text.trim(), &mut seed).map_err(|_| CoordinatorKeyError::Malformed)?;
    Ok(SigningKey::from_bytes(&seed))
}

fn create(path: &Path) -> Result<SigningKey, CoordinatorKeyError> {
    let mut seed = [0u8; 32];
    // A coordinator that cannot draw randomness cannot make a safe key either.
    getrandom::fill(&mut seed).expect("the operating system's random source works");
    let mut options = std::fs::OpenOptions::new();
    // `create_new` fails rather than overwrite a key another start wrote in
    // the meantime, which would leave a key nobody trusts.
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
}

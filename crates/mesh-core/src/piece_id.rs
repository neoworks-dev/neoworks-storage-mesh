//! The address of a piece: the BLAKE3 hash of its bytes.
//!
//! Content addressing is what lets nodes stay untrusted. A node is asked for a
//! piece by its hash and the client hashes whatever comes back, so a node can
//! lose or withhold a piece but never pass off different bytes as it.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::canonical_hex::parse_canonical_hex;

/// The BLAKE3 hash of a piece's bytes, written as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PieceId([u8; 32]);

impl PieceId {
    /// The id of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        PieceId(*blake3::hash(bytes).as_bytes())
    }

    /// The 32 hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for PieceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for PieceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PieceId({self})")
    }
}

/// A string that is not 64 lowercase hex digits.
#[derive(Debug, thiserror::Error)]
#[error("a piece id is 64 lowercase hex digits")]
pub struct ParsePieceIdError;

impl FromStr for PieceId {
    type Err = ParsePieceIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // Nodes name piece files by this spelling, so only the canonical one
        // is accepted: two spellings of one id would be two files.
        let bytes = parse_canonical_hex(text).ok_or(ParsePieceIdError)?;
        Ok(PieceId(bytes))
    }
}

impl Serialize for PieceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PieceId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_parse_round_trip() {
        let id = PieceId::of(b"hello");
        let parsed: PieceId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn rejects_uppercase_and_wrong_length() {
        let uppercase = PieceId::of(b"hello").to_string().to_uppercase();
        assert!(uppercase.parse::<PieceId>().is_err());
        assert!("abc".parse::<PieceId>().is_err());
        assert!("../etc/passwd".parse::<PieceId>().is_err());
    }
}

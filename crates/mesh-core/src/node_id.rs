//! The identity of a node: the BLAKE3 hash of its Ed25519 public key.
//!
//! A node's URL changes when it moves to a new address or port; its key does
//! not. Naming nodes by the hash of their key keeps a node the same node
//! wherever it runs, and anyone handed a public key can check that it belongs
//! to the id.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::canonical_hex::parse_canonical_hex;

/// The BLAKE3 hash of a node's public key, written as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId([u8; 32]);

impl NodeId {
    /// The id of the node with `public_key`.
    pub fn of(public_key: &[u8; 32]) -> Self {
        NodeId(*blake3::hash(public_key).as_bytes())
    }

    /// The 32 hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "NodeId({self})")
    }
}

/// A string that is not 64 lowercase hex digits.
#[derive(Debug, thiserror::Error)]
#[error("a node id is 64 lowercase hex digits")]
pub struct ParseNodeIdError;

impl FromStr for NodeId {
    type Err = ParseNodeIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = parse_canonical_hex(text).ok_or(ParseNodeIdError)?;
        Ok(NodeId(bytes))
    }
}

impl Serialize for NodeId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for NodeId {
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
        let id = NodeId::of(&[7; 32]);
        let parsed: NodeId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn different_keys_have_different_ids() {
        assert_ne!(NodeId::of(&[1; 32]), NodeId::of(&[2; 32]));
    }
}

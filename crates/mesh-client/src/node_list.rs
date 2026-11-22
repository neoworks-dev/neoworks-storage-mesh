//! The nodes a client may store pieces on, and who runs them.
//!
//! Until the coordinator publishes a signed list of nodes, the list is a JSON
//! file the user writes:
//!
//! ```json
//! [
//!   { "url": "http://203.0.113.7:7001", "operator": "alice", "network": "AS3320" },
//!   { "url": "http://198.51.100.2:7001", "operator": "bob", "network": "AS24940", "weight": 2.0 }
//! ]
//! ```
//!
//! `operator` and `network` say which nodes can fail together: one person's
//! machines, or one internet provider's network (its AS number). Placement
//! uses them to keep any one of them from holding too much of a chunk.

use std::path::Path;

use serde::Deserialize;

/// One node and what placement needs to know about it.
#[derive(Clone, Debug, Deserialize)]
pub struct NodeInfo {
    /// The node's base URL, e.g. `http://203.0.113.7:7001`.
    pub url: String,
    /// Who runs the node. Nodes of one operator count as one failure: they
    /// can all go away at once.
    pub operator: String,
    /// The network the node is reached through, such as an AS number.
    pub network: String,
    /// How much more often than others the node is chosen. Later this comes
    /// from measured reliability and free space; for now it is set by hand.
    #[serde(default = "default_weight")]
    pub weight: f64,
}

fn default_weight() -> f64 {
    1.0
}

/// The nodes to choose from.
#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub struct NodeList {
    pub nodes: Vec<NodeInfo>,
}

/// A node list file that could not be used.
#[derive(Debug, thiserror::Error)]
pub enum NodeListError {
    #[error("reading the node list: {0}")]
    Read(#[from] std::io::Error),
    #[error("parsing the node list: {0}")]
    Parse(#[from] serde_json::Error),
}

impl NodeList {
    /// Reads a node list from a JSON file.
    pub fn read(path: &Path) -> Result<Self, NodeListError> {
        let json = std::fs::read(path)?;
        Ok(serde_json::from_slice(&json)?)
    }
}

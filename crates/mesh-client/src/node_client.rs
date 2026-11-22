//! Requests from the device to nodes.
//!
//! Nodes are not trusted, so every piece that comes back is hashed and
//! compared with the id it was asked for.

use std::time::Duration;

use mesh_core::PieceId;
use mesh_core::protocol::piece_path;
use reqwest::StatusCode;

/// How long one request to a node may take before the node counts as
/// unreachable.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Talks to nodes over HTTP, reusing connections between requests.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
}

/// A request to a node that did not succeed.
#[derive(Debug, thiserror::Error)]
pub enum NodeRequestError {
    #[error("node {node} could not be reached: {source}")]
    Unreachable {
        node: String,
        source: reqwest::Error,
    },
    #[error("node {node} answered {status}")]
    Refused { node: String, status: StatusCode },
    #[error("node {node} returned bytes that are not piece {id}")]
    WrongBytes { node: String, id: PieceId },
}

impl NodeClient {
    /// A client with the default timeout.
    pub fn new() -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(NodeClient { http })
    }

    /// Stores `bytes` as the piece `id` on `node`.
    pub async fn put(
        &self,
        node: &str,
        id: &PieceId,
        bytes: Vec<u8>,
    ) -> Result<(), NodeRequestError> {
        let response = self
            .http
            .put(piece_url(node, id))
            .body(bytes)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(NodeRequestError::Refused {
                node: node.to_string(),
                status: response.status(),
            });
        }
        Ok(())
    }

    /// The piece `id` from `node`, or `None` when the node does not have it.
    pub async fn get(&self, node: &str, id: &PieceId) -> Result<Option<Vec<u8>>, NodeRequestError> {
        let response = self
            .http
            .get(piece_url(node, id))
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(NodeRequestError::Refused {
                node: node.to_string(),
                status: response.status(),
            });
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|source| unreachable(node, source))?;
        // The id is the hash of the bytes, so a node cannot hand back
        // different bytes under the same id without the client noticing.
        if PieceId::of(&bytes) != *id {
            return Err(NodeRequestError::WrongBytes {
                node: node.to_string(),
                id: *id,
            });
        }
        Ok(Some(bytes.to_vec()))
    }
}

fn piece_url(node: &str, id: &PieceId) -> String {
    format!("{}{}", node.trim_end_matches('/'), piece_path(id))
}

fn unreachable(node: &str, source: reqwest::Error) -> NodeRequestError {
    NodeRequestError::Unreachable {
        node: node.to_string(),
        source,
    }
}

//! Requests from the device to nodes.
//!
//! Nodes are not trusted, so every piece that comes back is hashed and
//! compared with the id it was asked for.

use std::time::Duration;

use mesh_core::PieceId;
use mesh_core::merkle::SegmentProof;
use mesh_core::protocol::{
    ChallengeRequest, ChallengeResponse, LeaseResponse, challenge_path, lease_path, piece_path,
};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

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
    #[error("node {node} answered {status}: {message}")]
    Refused {
        node: String,
        status: StatusCode,
        message: String,
    },
    #[error("node {node} returned bytes that are not piece {id}")]
    WrongBytes { node: String, id: PieceId },
    #[error("node {node} answered in a form this client does not understand")]
    UnexpectedAnswer { node: String },
}

impl NodeClient {
    /// A client with the default timeout.
    pub fn new() -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(NodeClient { http })
    }

    /// Stores `bytes` as the piece `id` on `node` for `lease`, and returns
    /// when the lease ends, in Unix seconds on the node's clock.
    pub async fn put(
        &self,
        node: &str,
        id: &PieceId,
        bytes: Vec<u8>,
        lease: Duration,
    ) -> Result<u64, NodeRequestError> {
        let url = with_lease(node_url(node, &piece_path(id)), lease);
        let response = self
            .http
            .put(url)
            .body(bytes)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        read_lease_end(node, response).await
    }

    /// Renews the lease of `id` on `node` to end no sooner than `lease` from
    /// now, and returns when it ends. `None` when the node does not hold the
    /// piece.
    pub async fn renew(
        &self,
        node: &str,
        id: &PieceId,
        lease: Duration,
    ) -> Result<Option<u64>, NodeRequestError> {
        let url = with_lease(node_url(node, &lease_path(id)), lease);
        let response = self
            .http
            .post(url)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        Ok(Some(read_lease_end(node, response).await?))
    }

    /// Asks `node` for `segments` of the piece `id` with their Merkle proofs.
    /// `None` when the node does not hold the piece.
    ///
    /// The proofs come back unchecked; [`SegmentProof::leads_to`] tells
    /// whether they lead to the root recorded at upload.
    pub async fn challenge(
        &self,
        node: &str,
        id: &PieceId,
        segments: &[u32],
    ) -> Result<Option<Vec<SegmentProof>>, NodeRequestError> {
        let request = ChallengeRequest {
            segments: segments.to_vec(),
        };
        // A list of numbers always serializes.
        let body = serde_json::to_vec(&request).expect("a challenge serializes");
        let response = self
            .http
            .post(node_url(node, &challenge_path(id)))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let answer: ChallengeResponse = read_json(node, response).await?;
        Ok(Some(answer.proofs))
    }

    /// The piece `id` from `node`, or `None` when the node does not have it.
    pub async fn get(&self, node: &str, id: &PieceId) -> Result<Option<Vec<u8>>, NodeRequestError> {
        let response = self
            .http
            .get(node_url(node, &piece_path(id)))
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(refused(node, response).await);
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

/// `path` on `node`, whose base URL may end in a slash or not.
fn node_url(node: &str, path: &str) -> String {
    format!("{}{path}", node.trim_end_matches('/'))
}

/// `url` with the lease length as its query.
fn with_lease(url: String, lease: Duration) -> String {
    format!("{url}?lease_seconds={}", lease.as_secs())
}

/// The lease end in a successful store or renew response.
async fn read_lease_end(node: &str, response: Response) -> Result<u64, NodeRequestError> {
    let lease: LeaseResponse = read_json(node, response).await?;
    Ok(lease.expires_at)
}

/// The JSON body of a successful response.
async fn read_json<T: DeserializeOwned>(
    node: &str,
    response: Response,
) -> Result<T, NodeRequestError> {
    let unexpected = || NodeRequestError::UnexpectedAnswer {
        node: node.to_string(),
    };
    let body = response.bytes().await.map_err(|_| unexpected())?;
    serde_json::from_slice(&body).map_err(|_| unexpected())
}

/// The error for a response that is not a success, with the node's reason.
async fn refused(node: &str, response: Response) -> NodeRequestError {
    let status = response.status();
    // The reason is for people reading the error; a body that can't be read
    // only loses that, not the error itself.
    let message = response.text().await.unwrap_or_default();
    NodeRequestError::Refused {
        node: node.to_string(),
        status,
        message,
    }
}

fn unreachable(node: &str, source: reqwest::Error) -> NodeRequestError {
    NodeRequestError::Unreachable {
        node: node.to_string(),
        source,
    }
}

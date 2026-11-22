//! Requests from the device to nodes.
//!
//! Nodes are not trusted, so every piece that comes back is hashed and
//! compared with the id it was asked for, and every pointer is checked for
//! its owner's signature.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use mesh_core::PieceId;
use mesh_core::merkle::SegmentProof;
use mesh_core::pointer::Pointer;
use mesh_core::protocol::{
    BATCH_LEASE_ROUTE, BatchRenewRequest, BatchRenewResponse, ChallengeRequest, ChallengeResponse,
    IDENTITY_ROUTE, LeaseResponse, NodeIdentity, PointerConflict, challenge_path, lease_path,
    piece_path, pointer_path,
};
use mesh_core::tabs::{OpenTabRequest, TabResponse, tab_path, tab_pieces_path};
use mesh_core::tokens::Token;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

use crate::node_latency::LatencyTable;

/// How long one request to a node may take before the node counts as
/// unreachable.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Talks to nodes over HTTP, reusing connections between requests, and
/// remembers how fast each node answered its last piece fetch. Clones share
/// both.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    latencies: Arc<LatencyTable>,
    /// The tab to store pieces against, by node URL, on nodes that require
    /// tokens. Pieces for other nodes get a lease of the asked length.
    tabs: Arc<Mutex<HashMap<String, String>>>,
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
    #[error("node {node} claims an id that is not the hash of its key")]
    InconsistentIdentity { node: String },
    #[error("node {node} returned a pointer its owner did not sign")]
    ForgedPointer { node: String },
}

/// What a node did with a pointer it was handed.
#[derive(Debug, PartialEq, Eq)]
pub enum PointerStored {
    /// The node holds it until `expires_at`, in Unix seconds on its clock.
    Stored { expires_at: u64 },
    /// The node holds a record it keeps instead: a higher version, or another
    /// record under the same version.
    Refused { held_version: u64 },
}

impl NodeClient {
    /// Who the node at `node` says it is, once its id is checked against its
    /// key.
    pub async fn identity(&self, node: &str) -> Result<NodeIdentity, NodeRequestError> {
        let response = self
            .http
            .get(node_url(node, IDENTITY_ROUTE))
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let identity: NodeIdentity = read_json(node, response).await?;
        if !identity.is_consistent() {
            return Err(NodeRequestError::InconsistentIdentity {
                node: node.to_string(),
            });
        }
        Ok(identity)
    }

    /// A client with the default timeout.
    pub fn new() -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(NodeClient {
            http,
            latencies: Arc::default(),
            tabs: Arc::default(),
        })
    }

    /// Pays the tab `name` on `node` with `tokens`, and stores pieces on that
    /// node against it from now on.
    pub async fn pay_tab(
        &self,
        node: &str,
        name: &str,
        tokens: &[Token],
    ) -> Result<TabResponse, NodeRequestError> {
        let request = OpenTabRequest {
            tokens: tokens.to_vec(),
        };
        // Tokens are plain integers and bytes, which always serialize.
        let body = serde_json::to_vec(&request).expect("tokens serialize");
        let response = self
            .http
            .post(node_url(node, &tab_path(name)))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let tab = read_json(node, response).await?;
        self.lock_tabs().insert(node.to_string(), name.to_string());
        Ok(tab)
    }

    /// Moves `ids` on `node` into its tab `name`, which renews them until the
    /// tab ends. Returns, in order, when each piece ends now, or `None` for a
    /// piece the node does not hold.
    pub async fn move_to_tab(
        &self,
        node: &str,
        name: &str,
        ids: &[PieceId],
    ) -> Result<Vec<Option<u64>>, NodeRequestError> {
        let request = BatchRenewRequest {
            pieces: ids.to_vec(),
        };
        // A list of piece ids always serializes.
        let body = serde_json::to_vec(&request).expect("a batch serializes");
        let response = self
            .http
            .post(node_url(node, &tab_pieces_path(name)))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let answer: BatchRenewResponse = read_json(node, response).await?;
        Ok(answer.lease_ends)
    }

    fn lock_tabs(&self) -> MutexGuard<'_, HashMap<String, String>> {
        // The map holds plain strings, so whatever a thread that panicked
        // while holding the lock left behind is still a valid map.
        self.tabs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Renews the leases of `ids` on `node` to end no sooner than `lease` from
    /// now. Returns, in order, when each ends, or `None` for a piece the node
    /// does not hold. At most
    /// [`MAX_BATCH_PIECES`](mesh_core::protocol::MAX_BATCH_PIECES) at a time.
    pub async fn renew_many(
        &self,
        node: &str,
        ids: &[PieceId],
        lease: Duration,
    ) -> Result<Vec<Option<u64>>, NodeRequestError> {
        let url = with_lease(node_url(node, BATCH_LEASE_ROUTE), lease);
        let request = BatchRenewRequest {
            pieces: ids.to_vec(),
        };
        // A list of piece ids always serializes.
        let body = serde_json::to_vec(&request).expect("a batch serializes");
        let response = self
            .http
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let answer: BatchRenewResponse = read_json(node, response).await?;
        // A node answering for other pieces than asked can't be matched up.
        if answer.lease_ends.len() != ids.len() {
            return Err(NodeRequestError::UnexpectedAnswer {
                node: node.to_string(),
            });
        }
        Ok(answer.lease_ends)
    }

    /// Stores `pointer` on `node` for `lease`.
    pub async fn put_pointer(
        &self,
        node: &str,
        pointer: &Pointer,
        lease: Duration,
    ) -> Result<PointerStored, NodeRequestError> {
        let url = with_lease(node_url(node, &pointer_path(&pointer.owner)), lease);
        // A pointer is plain integers and bytes, which always serialize.
        let body = serde_json::to_vec(pointer).expect("a pointer serializes");
        let response = self
            .http
            .put(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::CONFLICT {
            let conflict: PointerConflict = read_json(node, response).await?;
            return Ok(PointerStored::Refused {
                held_version: conflict.held_version,
            });
        }
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let expires_at = read_lease_end(node, response).await?;
        Ok(PointerStored::Stored { expires_at })
    }

    /// The pointer `node` holds for `owner`, once its signature holds, or
    /// `None` when the node holds none.
    pub async fn get_pointer(
        &self,
        node: &str,
        owner: &[u8; 32],
    ) -> Result<Option<Pointer>, NodeRequestError> {
        let response = self
            .http
            .get(node_url(node, &pointer_path(owner)))
            .send()
            .await
            .map_err(|source| unreachable(node, source))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(refused(node, response).await);
        }
        let pointer: Pointer = read_json(node, response).await?;
        // A node could hand back another owner's record, or one it made up;
        // only a genuine record of the owner asked for counts.
        if pointer.owner != *owner || pointer.verify().is_err() {
            return Err(NodeRequestError::ForgedPointer {
                node: node.to_string(),
            });
        }
        Ok(Some(pointer))
    }

    /// The HTTP client, for requests to coordinators.
    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// How long each node took to answer its last piece fetch.
    pub(crate) fn latencies(&self) -> &LatencyTable {
        &self.latencies
    }

    /// Stores `bytes` as the piece `id` on `node` for `lease`, or against the
    /// node's tab when one was paid, and returns when the lease ends, in Unix
    /// seconds on the node's clock.
    pub async fn put(
        &self,
        node: &str,
        id: &PieceId,
        bytes: Vec<u8>,
        lease: Duration,
    ) -> Result<u64, NodeRequestError> {
        let piece_url = node_url(node, &piece_path(id));
        // A node with a tab paid gets the piece against it; its lease is then
        // the tab's, whatever `lease` asks for.
        let url = match self.lock_tabs().get(node) {
            Some(tab) => format!("{piece_url}?tab={tab}"),
            None => with_lease(piece_url, lease),
        };
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
    ///
    /// The time the request runs, answered, failed or cancelled, is recorded
    /// as the node's latency.
    pub async fn get(&self, node: &str, id: &PieceId) -> Result<Option<Vec<u8>>, NodeRequestError> {
        let _timer = self.latencies.start_timer(node);
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

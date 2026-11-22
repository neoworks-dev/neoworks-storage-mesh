//! Renewing many pieces with one request per node.
//!
//! Renewing piece by piece takes six requests per chunk, millions a month for
//! a terabyte. A device's pieces sit on a few dozen nodes, so grouping them by
//! node turns that into a few dozen requests, all sent at once. A node learns
//! nothing new from it: it already sees the same device renew the same pieces
//! at the same moment.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures::future::join_all;
use mesh_core::PieceId;
use mesh_core::protocol::MAX_BATCH_PIECES;
use reqwest::StatusCode;

use crate::manifest::PlacedPiece;
use crate::node_client::{NodeClient, NodeRequestError};

/// What a batch renewal achieved.
#[derive(Debug, Default)]
pub struct BatchRenewal {
    /// Pieces whose lease now lasts at least the requested time.
    pub renewed: BTreeSet<(String, PieceId)>,
    /// Pieces whose node did not hold them, refused or could not be reached,
    /// by node URL.
    pub not_renewed: BTreeSet<(String, PieceId)>,
    /// The nodes that answered `402 Payment Required`: they renew only by
    /// moving pieces into a tab ([`crate::tab_renewal`]). Their pieces are
    /// in `not_renewed`.
    pub requires_tokens: BTreeSet<String>,
}

impl BatchRenewal {
    /// Whether `piece` was renewed.
    pub fn is_renewed(&self, piece: &PlacedPiece) -> bool {
        self.renewed.contains(&(piece.node.clone(), piece.id))
    }
}

/// Renews every piece in `pieces` for `lease`, one request per node and
/// [`MAX_BATCH_PIECES`]. A piece listed twice is renewed once.
pub async fn renew_pieces<'piece>(
    client: &NodeClient,
    pieces: impl IntoIterator<Item = &'piece PlacedPiece>,
    lease: Duration,
) -> BatchRenewal {
    let mut by_node: BTreeMap<&str, BTreeSet<PieceId>> = BTreeMap::new();
    for piece in pieces {
        by_node.entry(&piece.node).or_default().insert(piece.id);
    }
    let renewals = by_node.into_iter().map(|(node, ids)| async move {
        let ids: Vec<PieceId> = ids.into_iter().collect();
        let answer = renew_on_node(client, node, &ids, lease).await;
        (node, ids, answer)
    });
    let mut renewal = BatchRenewal::default();
    for (node, ids, answer) in join_all(renewals).await {
        let answers = match answer {
            NodeAnswer::Renewed(answers) => answers,
            NodeAnswer::RequiresTokens => {
                renewal.requires_tokens.insert(node.to_string());
                vec![false; ids.len()]
            }
        };
        for (id, renewed) in ids.into_iter().zip(answers) {
            let key = (node.to_string(), id);
            if renewed {
                renewal.renewed.insert(key);
            } else {
                renewal.not_renewed.insert(key);
            }
        }
    }
    renewal
}

/// How a node answered a renewal.
enum NodeAnswer {
    /// For each piece, whether it was renewed.
    Renewed(Vec<bool>),
    /// The node renews only through tabs.
    RequiresTokens,
}

/// Renews `ids` on `node` in batches of [`MAX_BATCH_PIECES`].
async fn renew_on_node(
    client: &NodeClient,
    node: &str,
    ids: &[PieceId],
    lease: Duration,
) -> NodeAnswer {
    let mut answers = Vec::with_capacity(ids.len());
    for batch in ids.chunks(MAX_BATCH_PIECES) {
        match client.renew_many(node, batch, lease).await {
            Ok(lease_ends) => answers.extend(lease_ends.iter().map(Option::is_some)),
            Err(NodeRequestError::Refused { status, .. })
                if status == StatusCode::PAYMENT_REQUIRED =>
            {
                return NodeAnswer::RequiresTokens;
            }
            Err(error) => {
                eprintln!("{error}");
                answers.extend(vec![false; batch.len()]);
            }
        }
    }
    NodeAnswer::Renewed(answers)
}

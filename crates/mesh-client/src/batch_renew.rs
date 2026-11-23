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
use mesh_core::protocol::MAX_BATCH_PIECES;
use mesh_core::{NodeId, PieceId};
use reqwest::StatusCode;

use crate::manifest::PlacedPiece;
use crate::node_client::{NodeClient, NodeRequestError};
use crate::node_list::NodeList;

/// What a batch renewal achieved.
#[derive(Debug, Default)]
pub struct BatchRenewal {
    /// Pieces whose lease now lasts at least the requested time.
    pub renewed: BTreeSet<(NodeId, PieceId)>,
    /// Pieces whose node did not hold them, refused, could not be reached or
    /// is not in the node list.
    pub not_renewed: BTreeSet<(NodeId, PieceId)>,
    /// The nodes that answered `402 Payment Required`: they renew only by
    /// moving pieces into a tab ([`crate::tab_renewal`]). Their pieces are
    /// in `not_renewed`.
    pub requires_tokens: BTreeSet<NodeId>,
}

impl BatchRenewal {
    /// Whether `piece` was renewed.
    pub fn is_renewed(&self, piece: &PlacedPiece) -> bool {
        self.renewed.contains(&(piece.node, piece.id))
    }
}

/// Renews every piece in `pieces` for `lease`, one request per node and
/// [`MAX_BATCH_PIECES`]. A piece listed twice is renewed once. Nodes are
/// found at their addresses in `list`.
pub async fn renew_pieces<'piece>(
    client: &NodeClient,
    list: &NodeList,
    pieces: impl IntoIterator<Item = &'piece PlacedPiece>,
    lease: Duration,
) -> BatchRenewal {
    let mut by_node: BTreeMap<NodeId, BTreeSet<PieceId>> = BTreeMap::new();
    for piece in pieces {
        by_node.entry(piece.node).or_default().insert(piece.id);
    }
    let renewals = by_node.into_iter().map(|(node, ids)| async move {
        let ids: Vec<PieceId> = ids.into_iter().collect();
        let answer = renew_on_node(client, list.url_of(&node), &node, &ids, lease).await;
        (node, ids, answer)
    });
    let mut renewal = BatchRenewal::default();
    for (node, ids, answer) in join_all(renewals).await {
        let answers = match answer {
            NodeAnswer::Renewed(answers) => answers,
            NodeAnswer::RequiresTokens => {
                renewal.requires_tokens.insert(node);
                vec![false; ids.len()]
            }
        };
        for (id, renewed) in ids.into_iter().zip(answers) {
            if renewed {
                renewal.renewed.insert((node, id));
            } else {
                renewal.not_renewed.insert((node, id));
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

/// Renews `ids` on the node `node`, found at `url`, in batches of
/// [`MAX_BATCH_PIECES`]. `url` is `None` for a node the node list does not
/// have, and then nothing is renewed.
async fn renew_on_node(
    client: &NodeClient,
    url: Option<&str>,
    node: &NodeId,
    ids: &[PieceId],
    lease: Duration,
) -> NodeAnswer {
    let Some(url) = url else {
        eprintln!("node {node} is not in the node list");
        return NodeAnswer::Renewed(vec![false; ids.len()]);
    };
    let mut answers = Vec::with_capacity(ids.len());
    for batch in ids.chunks(MAX_BATCH_PIECES) {
        match client.renew_many(url, batch, lease).await {
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

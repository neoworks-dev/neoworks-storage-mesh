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

use crate::manifest::PlacedPiece;
use crate::node_client::NodeClient;
use crate::node_list::NodeList;

/// What a batch renewal achieved.
#[derive(Debug, Default)]
pub struct BatchRenewal {
    /// Pieces whose lease now lasts at least the requested time.
    pub renewed: BTreeSet<(NodeId, PieceId)>,
    /// Pieces whose node did not hold them, refused, could not be reached or
    /// is not in the node list.
    pub not_renewed: BTreeSet<(NodeId, PieceId)>,
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
        let answers = renew_on_node(client, list.url_of(&node), &node, &ids, lease).await;
        (node, ids, answers)
    });
    let mut renewal = BatchRenewal::default();
    for (node, ids, answers) in join_all(renewals).await {
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

/// Renews `ids` on the node `node`, found at `url`, in batches of
/// [`MAX_BATCH_PIECES`]; for each piece, whether it was renewed. `url` is
/// `None` for a node the node list does not have, and then nothing is.
async fn renew_on_node(
    client: &NodeClient,
    url: Option<&str>,
    node: &NodeId,
    ids: &[PieceId],
    lease: Duration,
) -> Vec<bool> {
    let Some(url) = url else {
        eprintln!("node {node} is not in the node list");
        return vec![false; ids.len()];
    };
    let mut answers = Vec::with_capacity(ids.len());
    for batch in ids.chunks(MAX_BATCH_PIECES) {
        answers.extend(renew_batch(client, url, batch, lease).await);
    }
    answers
}

/// Renews one batch on the node at `url`; for each piece, whether it was
/// renewed.
async fn renew_batch(
    client: &NodeClient,
    url: &str,
    ids: &[PieceId],
    lease: Duration,
) -> Vec<bool> {
    match client.renew_many(url, ids, lease).await {
        Ok(lease_ends) => lease_ends.iter().map(Option::is_some).collect(),
        Err(error) => {
            eprintln!("{error}");
            vec![false; ids.len()]
        }
    }
}

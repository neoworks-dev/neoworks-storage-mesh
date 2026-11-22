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

use crate::manifest::PlacedPiece;
use crate::node_client::NodeClient;

/// What a batch renewal achieved.
#[derive(Debug, Default)]
pub struct BatchRenewal {
    /// Pieces whose lease now lasts at least the requested time.
    pub renewed: BTreeSet<(String, PieceId)>,
    /// Pieces whose node did not hold them, refused or could not be reached,
    /// by node URL.
    pub not_renewed: BTreeSet<(String, PieceId)>,
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
        let mut answers = Vec::with_capacity(ids.len());
        for batch in ids.chunks(MAX_BATCH_PIECES) {
            answers.extend(renew_batch(client, node, batch, lease).await);
        }
        (node, ids, answers)
    });
    let mut renewal = BatchRenewal::default();
    for (node, ids, answers) in join_all(renewals).await {
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

/// Renews one batch on `node`; for each piece, whether it was renewed.
async fn renew_batch(
    client: &NodeClient,
    node: &str,
    ids: &[PieceId],
    lease: Duration,
) -> Vec<bool> {
    match client.renew_many(node, ids, lease).await {
        Ok(lease_ends) => lease_ends.iter().map(Option::is_some).collect(),
        Err(error) => {
            eprintln!("{error}");
            vec![false; ids.len()]
        }
    }
}

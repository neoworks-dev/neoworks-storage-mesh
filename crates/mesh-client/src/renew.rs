//! Keeping a stored file: renewing the lease of every piece.
//!
//! Nodes delete pieces whose lease ends, so a file only stays while its owner
//! keeps renewing it. Deleting a file needs no request at all: stop renewing,
//! and the pieces expire.

use std::time::Duration;

use futures::future::join_all;

use crate::manifest::{Manifest, PlacedPiece};
use crate::node_client::NodeClient;
use crate::node_list::NodeList;

/// What a renewal achieved.
#[derive(Debug)]
pub struct RenewReport {
    /// Pieces whose lease now lasts at least the requested time.
    pub renewed: usize,
    /// Pieces whose node did not have them, refused or could not be reached.
    /// Their chunks are fine as long as four pieces each remain; bringing the
    /// rest back is repair's job.
    pub not_renewed: Vec<PlacedPiece>,
}

/// Renews every piece of the file `manifest` describes, so each lasts at least
/// `lease` from now. Nodes are found at their addresses in `list`; a piece on
/// a node it does not have is not renewed.
pub async fn renew(
    client: &NodeClient,
    list: &NodeList,
    manifest: &Manifest,
    lease: Duration,
) -> RenewReport {
    let mut report = RenewReport {
        renewed: 0,
        not_renewed: Vec::new(),
    };
    // One chunk at a time keeps the number of open requests at six, however
    // large the file.
    for chunk in &manifest.chunks {
        let renewals = chunk
            .pieces
            .iter()
            .map(|piece| renew_piece(client, list, piece, lease));
        let results = join_all(renewals).await;
        for (piece, was_renewed) in chunk.pieces.iter().zip(results) {
            if was_renewed {
                report.renewed += 1;
            } else {
                report.not_renewed.push(piece.clone());
            }
        }
    }
    report
}

/// Whether the node of `piece` renewed it for `lease`.
async fn renew_piece(
    client: &NodeClient,
    list: &NodeList,
    piece: &PlacedPiece,
    lease: Duration,
) -> bool {
    let Some(url) = list.url_of(&piece.node) else {
        eprintln!("node {} is not in the node list", piece.node);
        return false;
    };
    match client.renew(url, &piece.id, lease).await {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(error) => {
            eprintln!("{error}");
            false
        }
    }
}

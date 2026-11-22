//! Keeping a stored file: renewing the lease of every piece.
//!
//! Nodes delete pieces whose lease ends, so a file only stays while its owner
//! keeps renewing it. Deleting a file needs no request at all: stop renewing,
//! and the pieces expire.

use std::time::Duration;

use futures::future::join_all;

use crate::manifest::{Manifest, PlacedPiece};
use crate::node_client::NodeClient;

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
/// `lease` from now.
pub async fn renew(client: &NodeClient, manifest: &Manifest, lease: Duration) -> RenewReport {
    let mut report = RenewReport {
        renewed: 0,
        not_renewed: Vec::new(),
    };
    // One chunk at a time keeps the number of open requests at six, however
    // large the file.
    for chunk in &manifest.chunks {
        let renewals = chunk.pieces.iter().map(|piece| async move {
            match client.renew(&piece.node, &piece.id, lease).await {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(error) => {
                    eprintln!("{error}");
                    false
                }
            }
        });
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

//! Deleting pieces whose lease ended, on a timer.
//!
//! This is the only place a node deletes pieces. It is how deleted, abandoned
//! and unpaid data leaves the network: nobody renews it, so it expires.

use std::sync::Arc;
use std::time::Duration;

use crate::clock::unix_now;
use crate::piece_store::PieceStore;

/// Removes ended pieces from `store` every `interval`, forever.
pub async fn sweep_forever(store: Arc<PieceStore>, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        // A failed sweep is retried at the next tick. The pieces it missed are
        // already unreadable; only their disk space waits.
        if let Err(error) = store.remove_ended(unix_now()).await {
            eprintln!("sweep: {error}");
        }
    }
}

//! Deleting pieces and pointers whose lease ended, on a timer.
//!
//! This is the only place a node deletes anything. It is how deleted,
//! abandoned and unpaid data leaves the network: nobody renews it, so it
//! expires.

use std::sync::Arc;
use std::time::Duration;

use crate::clock::unix_now;
use crate::piece_store::PieceStore;
use crate::pointer_store::PointerStore;

/// Removes ended pieces from `store` and ended pointers from `pointers`
/// every `interval`, forever.
pub async fn sweep_forever(
    store: Arc<PieceStore>,
    pointers: Arc<PointerStore>,
    interval: Duration,
) {
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        // A failed sweep is retried at the next tick. What it missed is
        // already unreadable; only its disk space waits.
        let now = unix_now();
        if let Err(error) = store.remove_ended(now).await {
            eprintln!("sweep: {error}");
        }
        if let Err(error) = pointers.remove_ended(now).await {
            eprintln!("sweep: pointers: {error}");
        }
    }
}

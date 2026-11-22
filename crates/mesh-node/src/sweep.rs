//! Deleting pieces, pointers, tabs and mailbox messages whose lease ended, on
//! a timer.
//!
//! This is the only place a node deletes anything. It is how deleted,
//! abandoned and unpaid data leaves the network: nobody renews it or pays
//! its next tab, so it expires.

use std::sync::Arc;
use std::time::Duration;

use crate::clock::unix_now;
use crate::mailbox_store::MailboxStore;
use crate::piece_store::PieceStore;
use crate::pointer_store::PointerStore;
use crate::tab_book::TabBook;

/// Everything the sweep deletes from.
pub struct Swept {
    pub pieces: Arc<PieceStore>,
    pub pointers: Arc<PointerStore>,
    pub tabs: Arc<TabBook>,
    pub mailboxes: Arc<MailboxStore>,
}

/// Removes what ended from each of `stores` every `interval`, forever.
pub async fn sweep_forever(stores: Swept, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    loop {
        timer.tick().await;
        // A failed sweep is retried at the next tick. What it missed is
        // already unreadable; only its disk space waits.
        let now = unix_now();
        if let Err(error) = stores.pieces.remove_ended(now).await {
            eprintln!("sweep: {error}");
        }
        if let Err(error) = stores.pointers.remove_ended(now).await {
            eprintln!("sweep: pointers: {error}");
        }
        if let Err(error) = stores.tabs.remove_ended(now).await {
            eprintln!("sweep: tabs: {error}");
        }
        if let Err(error) = stores.mailboxes.remove_ended(now).await {
            eprintln!("sweep: mailboxes: {error}");
        }
    }
}

//! What a node's operator can tune.

use std::time::Duration;

/// How a node runs.
#[derive(Clone, Debug)]
pub struct NodeSettings {
    /// The longest lease the node grants. Longer requests are refused, so a
    /// client always knows exactly when its piece ends.
    pub max_lease: Duration,
    /// How often the node deletes pieces whose lease ended. A piece whose
    /// lease ended is already gone to every request; the sweep only frees the
    /// disk.
    pub sweep_interval: Duration,
    /// The coordinator to spend tokens at. When set, the node stores pieces
    /// only against tabs paid with tokens; when not, it grants leases to
    /// anyone, as a node for testing or a private network does.
    pub require_tokens: Option<String>,
    /// The bytes all mailbox messages together may take. Mailboxes are free,
    /// and contact mailboxes need no proof of work, so without a limit anyone
    /// could fill the node's disk with messages.
    pub mailbox_bytes: u64,
}

impl Default for NodeSettings {
    /// 30 days, the design's lease for nodes that are not always online, a
    /// sweep every minute, leases without tokens, and 1 GiB for mailboxes.
    fn default() -> Self {
        NodeSettings {
            max_lease: Duration::from_secs(30 * 24 * 60 * 60),
            sweep_interval: Duration::from_secs(60),
            require_tokens: None,
            mailbox_bytes: 1 << 30,
        }
    }
}

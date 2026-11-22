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
}

impl Default for NodeSettings {
    /// 30 days, the design's lease for nodes that are not always online, and a
    /// sweep every minute.
    fn default() -> Self {
        NodeSettings {
            max_lease: Duration::from_secs(30 * 24 * 60 * 60),
            sweep_interval: Duration::from_secs(60),
        }
    }
}

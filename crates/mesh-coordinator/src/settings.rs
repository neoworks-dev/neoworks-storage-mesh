//! What a coordinator's operator can tune.

use std::time::Duration;

/// How a coordinator runs.
#[derive(Clone, Debug)]
pub struct CoordinatorSettings {
    /// How often every node is checked for being up.
    pub probe_interval: Duration,
    /// How often a new snapshot of the node list is signed and published.
    pub publish_interval: Duration,
    /// The span uptime is measured over. A node that answered no check in
    /// this span leaves the list.
    pub uptime_window: Duration,
    /// How far a node record's signing time may be from the coordinator's
    /// clock. Older records are refused, so one captured long ago cannot be
    /// registered again after the node moved.
    pub max_record_age: Duration,
    /// The storage tokens each account gets per month; 100 tokens of
    /// 100 MB are 10 GB.
    pub monthly_allowance: u64,
}

impl Default for CoordinatorSettings {
    /// A check every five minutes and a snapshot every hour, as the design
    /// has it; uptime over one day; records at most ten minutes off; 10 GB
    /// of tokens per account and month.
    fn default() -> Self {
        CoordinatorSettings {
            probe_interval: Duration::from_secs(5 * 60),
            publish_interval: Duration::from_secs(60 * 60),
            uptime_window: Duration::from_secs(24 * 60 * 60),
            max_record_age: Duration::from_secs(10 * 60),
            monthly_allowance: 100,
        }
    }
}

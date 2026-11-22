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
    /// How often the coordinator looks for accounts to warn about expiry.
    pub warning_interval: Duration,
    /// How far into a month an account that has not taken its tokens gets
    /// the first warning. A payment can be a few days late without anyone
    /// being alarmed.
    pub first_warning_after: Duration,
    /// How long before its data runs out such an account gets the final
    /// warning.
    pub final_warning_before: Duration,
}

impl Default for CoordinatorSettings {
    /// A check every five minutes and a snapshot every hour, as the design
    /// has it; uptime over one day; records at most ten minutes off; 10 GB
    /// of tokens per account and month; warnings a week into the month and
    /// three days before the end, looked for every hour.
    fn default() -> Self {
        CoordinatorSettings {
            probe_interval: Duration::from_secs(5 * 60),
            publish_interval: Duration::from_secs(60 * 60),
            uptime_window: Duration::from_secs(24 * 60 * 60),
            max_record_age: Duration::from_secs(10 * 60),
            monthly_allowance: 100,
            warning_interval: Duration::from_secs(60 * 60),
            first_warning_after: Duration::from_secs(7 * 24 * 60 * 60),
            final_warning_before: Duration::from_secs(3 * 24 * 60 * 60),
        }
    }
}

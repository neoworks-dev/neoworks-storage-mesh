//! What a coordinator's operator can tune.

use std::time::Duration;

use mesh_core::NodeId;

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
    /// How often every node's upload bandwidth is measured.
    pub speed_test_interval: Duration,
    /// The span a node's bandwidth is the median of its speed tests over.
    pub bandwidth_window: Duration,
    /// How long a node must not answer before it counts as lost for good.
    /// Long enough that a weekend outage does not count; this only feeds the
    /// loss rate and has nothing to do with when lost pieces are repaired.
    pub lost_after: Duration,
    /// The span the loss rate is measured over.
    pub loss_window: Duration,
    /// The allowance tokens each signed-in account gets per month, which only
    /// the coordinator's own nodes take. 40 tokens are 5 GB for a month at
    /// the starting price of fast nodes, 8,000 tokens per TB.
    pub monthly_allowance: u64,
    /// The nodes the coordinator runs itself, which take allowance tokens.
    pub own_nodes: Vec<NodeId>,
    /// How often what nodes earned is settled and credited to operators.
    pub earnings_interval: Duration,
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
    /// has it; uptime over one day; a speed test every hour, with bandwidth
    /// over a week; nodes lost after a week of silence, counted over 90
    /// days; records at most ten minutes off; 40 allowance tokens per account
    /// and month, and no own nodes; earnings paid out every hour; warnings a week into the month and three
    /// days before the end, looked for every hour.
    fn default() -> Self {
        CoordinatorSettings {
            probe_interval: Duration::from_secs(5 * 60),
            publish_interval: Duration::from_secs(60 * 60),
            uptime_window: Duration::from_secs(24 * 60 * 60),
            speed_test_interval: Duration::from_secs(60 * 60),
            bandwidth_window: Duration::from_secs(7 * 24 * 60 * 60),
            lost_after: Duration::from_secs(7 * 24 * 60 * 60),
            loss_window: Duration::from_secs(90 * 24 * 60 * 60),
            max_record_age: Duration::from_secs(10 * 60),
            monthly_allowance: 40,
            own_nodes: Vec::new(),
            earnings_interval: Duration::from_secs(60 * 60),
            warning_interval: Duration::from_secs(60 * 60),
            first_warning_after: Duration::from_secs(7 * 24 * 60 * 60),
            final_warning_before: Duration::from_secs(3 * 24 * 60 * 60),
        }
    }
}

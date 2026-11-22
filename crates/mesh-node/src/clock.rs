//! The node's time, as Unix seconds, which is how lease ends are written.

use std::time::{SystemTime, UNIX_EPOCH};

/// The current Unix time in seconds.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        // Only a clock set before 1970 fails here, and leases can't work on one.
        .expect("the system clock is after 1970")
        .as_secs()
}

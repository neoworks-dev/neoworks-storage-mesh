//! How long each node took to answer the last piece fetch, so a download can
//! ask the fastest nodes first.
//!
//! Only the last time per node is kept, overwritten by the next. That is
//! crude, but it follows a node that slows down or recovers within one
//! request.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// The time of the last answer from each node, by node URL.
#[derive(Default)]
pub(crate) struct LatencyTable {
    last_answer: Mutex<HashMap<String, Duration>>,
}

impl LatencyTable {
    /// How long `node` took last time, or `None` when it was never asked.
    pub(crate) fn last_answer(&self, node: &str) -> Option<Duration> {
        self.lock().get(node).copied()
    }

    /// Starts timing a request to `node`; the time is recorded when the
    /// returned timer is dropped.
    pub(crate) fn start_timer(&self, node: &str) -> AnswerTimer<'_> {
        AnswerTimer {
            table: self,
            node: node.to_string(),
            started: Instant::now(),
        }
    }

    /// Records that `node` took `time` to answer.
    pub(crate) fn record(&self, node: &str, time: Duration) {
        self.lock().insert(node.to_string(), time);
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Duration>> {
        // The map holds plain durations, so whatever a thread that panicked
        // while holding the lock left behind is still a valid table.
        self.last_answer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Times one request and records the time when dropped.
///
/// Recording on drop catches the requests a download cancels once it has
/// enough pieces. Those come from the slowest nodes, and without a record
/// they would keep the fast time of an earlier answer and be asked first
/// again. A cancelled request records how long it ran, which is less than
/// the node would have taken, but enough to rank it behind the nodes that
/// answered.
pub(crate) struct AnswerTimer<'table> {
    table: &'table LatencyTable,
    node: String,
    started: Instant,
}

impl Drop for AnswerTimer<'_> {
    fn drop(&mut self) {
        self.table.record(&self.node, self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_node_has_no_time() {
        let table = LatencyTable::default();

        assert_eq!(table.last_answer("http://node"), None);
    }

    #[test]
    fn a_dropped_timer_records_the_time_it_ran() {
        let table = LatencyTable::default();
        table.record("http://node", Duration::from_secs(60));

        let timer = table.start_timer("http://node");
        drop(timer);

        assert!(table.last_answer("http://node").unwrap() < Duration::from_secs(60));
    }
}

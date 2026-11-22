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
    /// returned timer is dropped. A request that ends before the node
    /// answered counts as taking at least `no_answer`.
    pub(crate) fn start_timer(&self, node: &str, no_answer: Duration) -> AnswerTimer<'_> {
        AnswerTimer {
            table: self,
            node: node.to_string(),
            started: Instant::now(),
            no_answer,
            answered: false,
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
/// again.
///
/// A node that answered is recorded with the time the request ran, even
/// when cancelled while sending the bytes. A node that never answered, or
/// whose connection failed, is recorded as taking at least the time a
/// request may take. Recording only how long it was waited on would rank a
/// silent node ahead of healthy nodes that are slow under load.
pub(crate) struct AnswerTimer<'table> {
    table: &'table LatencyTable,
    node: String,
    started: Instant,
    no_answer: Duration,
    answered: bool,
}

impl AnswerTimer<'_> {
    /// Notes that the node answered, once its response began to arrive.
    pub(crate) fn answered(&mut self) {
        self.answered = true;
    }
}

impl Drop for AnswerTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        let time = if self.answered {
            elapsed
        } else {
            elapsed.max(self.no_answer)
        };
        self.table.record(&self.node, time);
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
    fn an_answered_request_records_the_time_it_ran() {
        let table = LatencyTable::default();
        table.record("http://node", Duration::from_secs(60));

        let mut timer = table.start_timer("http://node", Duration::from_secs(30));
        timer.answered();
        drop(timer);

        assert!(table.last_answer("http://node").unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn a_request_without_an_answer_counts_as_taking_the_full_time() {
        let table = LatencyTable::default();

        drop(table.start_timer("http://silent", Duration::from_secs(30)));

        assert_eq!(
            table.last_answer("http://silent"),
            Some(Duration::from_secs(30))
        );
    }
}

//! Serving speed tests, which is how coordinators measure a node's upload
//! bandwidth.
//!
//! A speed test is [`SPEED_TEST_BYTES`] bytes that a coordinator downloads
//! and times. Anyone can ask for one, so a node serves only a limited number
//! per hour: without the limit, anybody could make a home node upload
//! without end.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use mesh_core::protocol::{SPEED_TEST_BYTES, SPEED_TEST_ROUTE};

use crate::routes::Node;

/// How many speed tests a node serves per hour. Each coordinator measures
/// once an hour, so this leaves room for 30 coordinators, and caps what
/// strangers can make the node upload at 240 MB an hour.
const SPEED_TESTS_PER_HOUR: usize = 30;

const HOUR: Duration = Duration::from_secs(60 * 60);

/// The speed test body and when the recent speed tests began.
pub(crate) struct SpeedTests {
    /// Bytes from a hash function rather than zeros: a proxy on the way that
    /// compresses would make a slow link look fast with zeros.
    body: Bytes,
    started: Mutex<VecDeque<Instant>>,
}

impl SpeedTests {
    pub(crate) fn new() -> Self {
        let mut body = vec![0; SPEED_TEST_BYTES];
        blake3::Hasher::new_derive_key("neoworks-mesh speed test v1")
            .finalize_xof()
            .fill(&mut body);
        SpeedTests {
            body: Bytes::from(body),
            started: Mutex::new(VecDeque::new()),
        }
    }

    /// The body for a speed test starting at `now`, or `None` when the node
    /// served its share in the hour before.
    fn start(&self, now: Instant) -> Option<Bytes> {
        // The queue holds plain instants, so a panic elsewhere while it was
        // locked cannot have left it half-changed.
        let mut started = self.started.lock().unwrap_or_else(PoisonError::into_inner);
        while started
            .front()
            .is_some_and(|&start| now.duration_since(start) >= HOUR)
        {
            started.pop_front();
        }
        if started.len() >= SPEED_TESTS_PER_HOUR {
            return None;
        }
        started.push_back(now);
        // `Bytes` shares the body, so serving it copies nothing.
        Some(self.body.clone())
    }
}

/// The speed test route.
pub(crate) fn routes() -> Router<Node> {
    Router::new().route(SPEED_TEST_ROUTE, get(get_speed_test))
}

/// `GET /speed-test`: the speed test body, or `429 Too Many Requests`.
async fn get_speed_test(State(node): State<Node>) -> Response {
    let Some(body) = node.speed_tests.start(Instant::now()) else {
        let reason = "this node has served its speed tests for this hour";
        return (StatusCode::TOO_MANY_REQUESTS, reason).into_response();
    };
    body.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serves_its_share_per_hour_and_then_refuses() {
        let tests = SpeedTests::new();
        let start = Instant::now();

        for _ in 0..SPEED_TESTS_PER_HOUR {
            assert_eq!(tests.start(start).unwrap().len(), SPEED_TEST_BYTES);
        }
        assert_eq!(tests.start(start + Duration::from_secs(59 * 60)), None);
        assert!(tests.start(start + HOUR).is_some());
    }
}

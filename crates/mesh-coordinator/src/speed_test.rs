//! Measuring how fast nodes upload, by timing their speed test.
//!
//! The coordinator downloads each node's speed test once per interval, one
//! node after another: timing several at once would measure the
//! coordinator's own connection, shared between them, instead of the nodes'.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mesh_core::protocol::{SPEED_TEST_BYTES, SPEED_TEST_ROUTE};
use tokio::time::timeout_at;

use crate::clock::unix_now;
use crate::measurements::Measurements;
use crate::registry::Registry;
use crate::settings::CoordinatorSettings;

/// How long a speed test may take. A node too slow to finish in it is
/// measured by what arrived until then: 8 MB in a minute is about 1 Mbit/s,
/// slow enough that the exact number no longer matters.
const SPEED_TEST_DEADLINE: Duration = Duration::from_secs(60);

/// How fast the node at `url` sent its speed test, in bits per second, or
/// `None` when it did not send one.
///
/// The time runs from sending the request, so it includes setting up the
/// connection; at 8 MB that is a small share of any connection a client
/// would stream from.
pub async fn measure(http: &reqwest::Client, url: &str) -> Option<u64> {
    let test_url = format!("{}{SPEED_TEST_ROUTE}", url.trim_end_matches('/'));
    let started = Instant::now();
    let deadline = tokio::time::Instant::from_std(started + SPEED_TEST_DEADLINE);
    let mut response = timeout_at(deadline, http.get(test_url).send())
        .await
        .ok()?
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let mut received = 0;
    // Bytes past the speed test's size are not read: a node cannot make the
    // coordinator download more than it asked for.
    while received < SPEED_TEST_BYTES {
        match timeout_at(deadline, response.chunk()).await {
            Ok(Ok(Some(bytes))) => received += bytes.len(),
            // Out of time: what arrived until now is the measurement.
            Err(_) => break,
            // A node that sends a short burst and stops would look fast, so
            // a speed test that ends early does not count.
            Ok(_) => return None,
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    if received == 0 || seconds == 0.0 {
        return None;
    }
    Some((received.min(SPEED_TEST_BYTES) as f64 * 8.0 / seconds) as u64)
}

/// Times every registered node's speed test once and notes the results.
pub async fn measure_all(
    http: &reqwest::Client,
    registry: &Registry,
    measurements: &Measurements,
) -> rusqlite::Result<()> {
    for record in registry.records().await? {
        let Some(bits_per_second) = measure(http, &record.body.url).await else {
            continue;
        };
        measurements
            .record_speed_test(&record.body.node_id, unix_now(), bits_per_second)
            .await?;
    }
    Ok(())
}

/// Times every node each speed test interval, forever, and forgets tests
/// older than the bandwidth window.
pub async fn measure_forever(
    registry: Arc<Registry>,
    measurements: Arc<Measurements>,
    settings: CoordinatorSettings,
) {
    // Without a timeout of its own: `measure` keeps the deadline, and the
    // coordinator's shared client gives up after seconds.
    let http = reqwest::Client::new();
    let mut timer = tokio::time::interval(settings.speed_test_interval);
    loop {
        timer.tick().await;
        if let Err(error) = measure_all(&http, &registry, &measurements).await {
            eprintln!("speed test: {error}");
        }
        let window_start = unix_now().saturating_sub(settings.bandwidth_window.as_secs());
        if let Err(error) = measurements.forget_speed_tests(window_start).await {
            eprintln!("speed test: forgetting old tests: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::get;
    use tokio::net::TcpListener;

    use super::*;

    /// The URL of a fake node whose speed test is `len` bytes.
    async fn node_sending(len: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route(SPEED_TEST_ROUTE, get(move || async move { vec![7u8; len] }));
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    #[tokio::test]
    async fn a_whole_speed_test_is_measured() {
        let url = node_sending(SPEED_TEST_BYTES).await;

        let speed = measure(&reqwest::Client::new(), &url).await;

        assert!(speed.is_some_and(|bits_per_second| bits_per_second > 0));
    }

    #[tokio::test]
    async fn a_speed_test_that_ends_early_does_not_count() {
        let url = node_sending(1_000).await;

        assert_eq!(measure(&reqwest::Client::new(), &url).await, None);
    }

    #[tokio::test]
    async fn a_node_without_a_speed_test_is_not_measured() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, Router::new()).await });

        assert_eq!(measure(&reqwest::Client::new(), &url).await, None);
    }
}

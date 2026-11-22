//! Telling coordinators how much the node stores, which is how full its
//! price class is.
//!
//! Counting means reading every piece file's size, which takes a while on a
//! node with millions of pieces. Anyone can ask, so the count is kept for
//! [`COUNT_KEPT_FOR`] and requests in between get the kept one.

use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mesh_core::protocol::{NodeUsage, USAGE_ROUTE};
use tokio::sync::Mutex;

use crate::routes::Node;

/// How long a count is answered with before the node counts again. Prices
/// change once a day, so a count a few minutes old is as good as a new one.
const COUNT_KEPT_FOR: Duration = Duration::from_secs(10 * 60);

/// The last count and when it was made.
#[derive(Default)]
pub(crate) struct UsageCount {
    last: Mutex<Option<(Instant, u64)>>,
}

/// The usage route.
pub(crate) fn routes() -> Router<Node> {
    Router::new().route(USAGE_ROUTE, get(get_usage))
}

/// `GET /usage`: how many bytes the node's pieces take.
async fn get_usage(State(node): State<Node>) -> Response {
    // Held while counting, so requests arriving meanwhile wait for this count
    // instead of each starting their own.
    let mut last = node.usage.last.lock().await;
    if let Some((counted_at, used_bytes)) = *last
        && counted_at.elapsed() < COUNT_KEPT_FOR
    {
        return Json(NodeUsage { used_bytes }).into_response();
    }
    match node.store.stored_bytes().await {
        Ok(used_bytes) => {
            *last = Some((Instant::now(), used_bytes));
            Json(NodeUsage { used_bytes }).into_response()
        }
        Err(error) => {
            eprintln!("counting stored bytes: {error}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use mesh_core::PieceId;

    use crate::piece_store::PieceStore;

    #[tokio::test]
    async fn stored_bytes_count_pieces_but_not_uploads_in_progress() {
        let directory = tempfile::tempdir().unwrap();
        let store = PieceStore::open(directory.path()).await.unwrap();
        let piece = vec![3u8; 1_234];
        let in_a_while = 4_000_000_000;
        store
            .put(&PieceId::of(&piece), &piece, in_a_while)
            .await
            .unwrap();
        let unfinished = directory.path().join("pieces").join("abc.0.partial");
        std::fs::write(unfinished, [0u8; 99]).unwrap();

        assert_eq!(store.stored_bytes().await.unwrap(), 1_234);
    }
}

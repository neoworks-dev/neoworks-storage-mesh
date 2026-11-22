//! The coordinator's HTTP interface: nodes register, clients fetch the node
//! list.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_core::node_list::NodeRecord;
use mesh_core::protocol::{NODE_LIST_ROUTE, REGISTER_ROUTE};
use mesh_core::signed::Signed;

use crate::clock::unix_now;
use crate::probe::answers_as;
use crate::publish::LatestSnapshot;
use crate::registry::{Registered, Registry};

/// What every request handler needs.
#[derive(Clone)]
pub struct Coordinator {
    pub registry: Arc<Registry>,
    pub latest: Arc<LatestSnapshot>,
    pub http: reqwest::Client,
    /// How far a record's signing time may be from now, in seconds.
    pub max_record_age_seconds: u64,
}

/// The router of `coordinator`.
pub fn router(coordinator: Coordinator) -> Router {
    Router::new()
        .route(REGISTER_ROUTE, post(register))
        .route(NODE_LIST_ROUTE, get(node_list))
        .with_state(coordinator)
}

/// `POST /nodes`: keeps a node's signed record once it holds up.
async fn register(
    State(coordinator): State<Coordinator>,
    Json(record): Json<Signed<NodeRecord>>,
) -> Response {
    if let Err(reason) = check_record(&coordinator, &record).await {
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    match coordinator.registry.register(&record).await {
        Ok(Registered::Stored) => {}
        Ok(Registered::Stale) => {
            let reason = "a record of this node signed at the same time or later is registered";
            return (StatusCode::CONFLICT, reason).into_response();
        }
        Err(error) => return internal_error(error),
    }
    // The check above found the node up, which counts toward its uptime and
    // puts it in the next snapshot without waiting for a probe round.
    let node_id = record.body.node_id;
    match coordinator
        .registry
        .record_probe(&node_id, unix_now(), true)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => internal_error(error),
    }
}

/// Why `record` may not be registered, if it may not.
async fn check_record(
    coordinator: &Coordinator,
    record: &Signed<NodeRecord>,
) -> Result<(), String> {
    record.verify_node().map_err(|error| error.to_string())?;
    let age = unix_now().abs_diff(record.body.signed_at);
    if age > coordinator.max_record_age_seconds {
        return Err(format!(
            "the record was signed {age} seconds from now; at most {} are accepted",
            coordinator.max_record_age_seconds
        ));
    }
    // The coordinator fetches whatever URL a record names. Allowing only
    // HTTP keeps it from being pointed at other schemes; it can still be
    // pointed at addresses on its own network, which is up to its firewall.
    let url = &record.body.url;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("a node's URL starts with http:// or https://".to_string());
    }
    // Without this, anyone could register a record naming another node's
    // address, and clients would send pieces meant for one node to another.
    if !answers_as(&coordinator.http, url, &record.body.node_id).await {
        return Err(format!(
            "{url} does not answer as node {}",
            record.body.node_id
        ));
    }
    Ok(())
}

/// `GET /node-list`: the newest signed snapshot.
async fn node_list(State(coordinator): State<Coordinator>) -> Response {
    let latest = coordinator.latest.read().await;
    match latest.as_ref() {
        Some(snapshot) => Json(snapshot.clone()).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no node list published yet",
        )
            .into_response(),
    }
}

/// A database error: logged on the coordinator, answered without detail.
fn internal_error(error: rusqlite::Error) -> Response {
    eprintln!("registry: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

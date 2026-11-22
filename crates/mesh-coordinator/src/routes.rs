//! The coordinator's HTTP interface: nodes register and name their operator,
//! clients fetch the node list and the prices and register chunks for repair
//! ([`chunk_routes`]), people sign up and sign in ([`sign_in_routes`]), and
//! they find each other's mailbox keys ([`mailbox_key_routes`]).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_core::NodeId;
use mesh_core::earnings::{OPERATOR_ROUTE, OperatorClaim};
use mesh_core::node_list::{CoordinatorList, NodeRecord};
use mesh_core::prices::PRICES_ROUTE;
use mesh_core::protocol::{NODE_LIST_ROUTE, REGISTER_ROUTE};
use mesh_core::signed::{Signed, SigningKey};
use tokio::sync::Mutex;

use crate::accounts::Accounts;
use crate::chunk_book::ChunkBook;
use crate::chunk_routes;
use crate::clock::unix_now;
use crate::earnings::{Earnings, Linked};
use crate::mailbox_key_routes;
use crate::mailbox_keys::MailboxKeys;
use crate::price_book::LatestPrices;
use crate::probe::answers_as;
use crate::publish::LatestSnapshot;
use crate::registry::{Registered, Registry};
use crate::sign_in_limits::FailedSignIns;
use crate::sign_in_routes;
use crate::token_issuer::TokenIssuer;
use crate::token_routes;

/// What every request handler needs.
#[derive(Clone)]
pub struct Coordinator {
    pub registry: Arc<Registry>,
    pub earnings: Arc<Earnings>,
    pub accounts: Arc<Accounts>,
    pub chunks: Arc<ChunkBook>,
    /// All coordinators, when there are several, among which this one keeps
    /// only the chunks it is responsible for.
    pub chunk_owners: Option<Arc<CoordinatorList>>,
    pub failed_sign_ins: Arc<Mutex<FailedSignIns>>,
    pub mailbox_keys: Arc<MailboxKeys>,
    pub tokens: Arc<TokenIssuer>,
    /// The coordinator's key, which signs token keys and mailbox keys.
    pub key: SigningKey,
    pub latest: Arc<LatestSnapshot>,
    pub prices: Arc<LatestPrices>,
    pub http: reqwest::Client,
    /// How far a record's signing time may be from now, in seconds.
    pub max_record_age_seconds: u64,
    /// The allowance tokens each account gets per month.
    pub monthly_allowance: u64,
    /// The nodes the coordinator runs, which take allowance tokens.
    pub own_nodes: Arc<Vec<NodeId>>,
}

/// The router of `coordinator`.
pub fn router(coordinator: Coordinator) -> Router {
    Router::new()
        .route(REGISTER_ROUTE, post(register))
        .route(OPERATOR_ROUTE, post(claim_operator))
        .route(NODE_LIST_ROUTE, get(node_list))
        .route(PRICES_ROUTE, get(prices))
        .merge(sign_in_routes::routes())
        .merge(token_routes::routes())
        .merge(chunk_routes::routes())
        .merge(mailbox_key_routes::routes())
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

/// `POST /nodes/operator`: sends a node's earnings to the account it names.
async fn claim_operator(
    State(coordinator): State<Coordinator>,
    Json(claim): Json<Signed<OperatorClaim>>,
) -> Response {
    // Only the node's own key may say where its earnings go.
    if claim.verify().is_err() || NodeId::of(&claim.signer) != claim.body.node_id {
        let reason = "the claim is not signed by the node it names";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let age = unix_now().abs_diff(claim.body.signed_at);
    if age > coordinator.max_record_age_seconds {
        let reason = format!("the claim was signed {age} seconds from now");
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let OperatorClaim {
        node_id,
        email,
        signed_at,
    } = &claim.body;
    match coordinator.earnings.link(node_id, email, *signed_at).await {
        Ok(Linked::Stored) => StatusCode::NO_CONTENT.into_response(),
        Ok(Linked::Stale) => {
            let reason = "a claim of this node signed at the same time or later is held";
            (StatusCode::CONFLICT, reason).into_response()
        }
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

/// `GET /prices`: today's signed price table.
async fn prices(State(coordinator): State<Coordinator>) -> Response {
    let prices = coordinator.prices.read().await;
    match prices.as_ref() {
        Some(table) => Json(table.clone()).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "no prices published yet").into_response(),
    }
}

/// A database error: logged on the coordinator, answered without detail.
fn internal_error(error: rusqlite::Error) -> Response {
    eprintln!("registry: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

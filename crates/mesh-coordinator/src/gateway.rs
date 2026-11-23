//! The HTTPS gateway for browsers: forwards reading and storing pieces and
//! pointers to nodes in this coordinator's node list
//! (`mesh_core::gateway` describes the routes).
//!
//! The coordinator itself is reached over HTTPS, usually behind a reverse
//! proxy, so a web page can call it where it can't call a plain-HTTP node.
//! The gateway checks what passes through it the way a client would: a
//! piece must hash to its id and a pointer must carry its owner's
//! signature. It answers any origin, since everything it carries is
//! encrypted or signed and no request carries a credential.

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_MAX_AGE, CONTENT_TYPE,
};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router, middleware};
use mesh_core::gateway::{GATEWAY_PIECE_ROUTE, GATEWAY_POINTER_ROUTE, is_pointer_of};
use mesh_core::pointer::{Pointer, parse_owner};
use mesh_core::protocol::{MAX_PIECE_BYTES, piece_path, pointer_path};
use mesh_core::{NodeId, PieceId};

use crate::routes::Coordinator;

/// The gateway routes.
pub fn routes() -> Router<Coordinator> {
    Router::new()
        .route(
            GATEWAY_PIECE_ROUTE,
            get(get_piece).put(put_piece).options(preflight),
        )
        .route(
            GATEWAY_POINTER_ROUTE,
            get(get_pointer).put(put_pointer).options(preflight),
        )
        .layer(DefaultBodyLimit::max(MAX_PIECE_BYTES))
        .layer(middleware::map_response(allow_any_origin))
}

/// `GET /gateway/{node}/pieces/{id}`: the piece from the node, once it
/// hashes to its id.
async fn get_piece(
    State(coordinator): State<Coordinator>,
    Path((node, id)): Path<(NodeId, PieceId)>,
) -> Response {
    let Some(url) = node_url(&coordinator, &node).await else {
        return not_listed();
    };
    let request = coordinator
        .gateway_http
        .get(format!("{url}{}", piece_path(&id)));
    let answer = send(request).await;
    // A node that hands back other bytes would have a browser blame the
    // gateway, or a careless web app take them.
    if answer.status == StatusCode::OK && PieceId::of(&answer.body) != id {
        let reason = "the node answered with bytes that do not hash to the piece id";
        return (StatusCode::BAD_GATEWAY, reason).into_response();
    }
    answer.into_response()
}

/// `PUT /gateway/{node}/pieces/{id}`: stores the body on the node, with the
/// request's query, once it hashes to the id.
async fn put_piece(
    State(coordinator): State<Coordinator>,
    Path((node, id)): Path<(NodeId, PieceId)>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    // Checked here too, so the gateway carries no bytes the node would
    // refuse anyway.
    if PieceId::of(&body) != id {
        let reason = "the bytes do not hash to the piece id";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let Some(url) = node_url(&coordinator, &node).await else {
        return not_listed();
    };
    let target = format!("{url}{}{}", piece_path(&id), query_suffix(query));
    let request = coordinator.gateway_http.put(target).body(body);
    send(request).await.into_response()
}

/// `GET /gateway/{node}/pointers/{owner}`: the pointer from the node, once
/// it carries the owner's signature.
async fn get_pointer(
    State(coordinator): State<Coordinator>,
    Path((node, owner)): Path<(NodeId, String)>,
) -> Response {
    let Some(owner) = parse_owner(&owner) else {
        return bad_owner();
    };
    let Some(url) = node_url(&coordinator, &node).await else {
        return not_listed();
    };
    let request = coordinator
        .gateway_http
        .get(format!("{url}{}", pointer_path(&owner)));
    let answer = send(request).await;
    if answer.status != StatusCode::OK {
        return answer.into_response();
    }
    let pointer = serde_json::from_slice::<Pointer>(&answer.body);
    if !pointer.is_ok_and(|pointer| is_pointer_of(&pointer, &owner)) {
        let reason = "the node answered with a pointer its owner did not sign";
        return (StatusCode::BAD_GATEWAY, reason).into_response();
    }
    answer.into_response()
}

/// `PUT /gateway/{node}/pointers/{owner}`: stores the pointer in the body on
/// the node, with the request's query, once it carries the owner's
/// signature.
async fn put_pointer(
    State(coordinator): State<Coordinator>,
    Path((node, owner)): Path<(NodeId, String)>,
    RawQuery(query): RawQuery,
    Json(pointer): Json<Pointer>,
) -> Response {
    let Some(owner) = parse_owner(&owner) else {
        return bad_owner();
    };
    if !is_pointer_of(&pointer, &owner) {
        let reason = "the pointer is not signed by the owner the URL names";
        return (StatusCode::BAD_REQUEST, reason).into_response();
    }
    let Some(url) = node_url(&coordinator, &node).await else {
        return not_listed();
    };
    let target = format!("{url}{}{}", pointer_path(&owner), query_suffix(query));
    // A pointer is plain bytes and numbers, which always serialize.
    let body = serde_json::to_vec(&pointer).expect("a pointer serializes");
    let request = coordinator
        .gateway_http
        .put(target)
        .header(CONTENT_TYPE, "application/json")
        .body(body);
    send(request).await.into_response()
}

/// The URL of `node`, when it is in the newest node list and not down.
///
/// Only listed nodes are reached, so the gateway can't be used to send
/// requests to any address on the internet or inside the coordinator's
/// network.
async fn node_url(coordinator: &Coordinator, node: &NodeId) -> Option<String> {
    let latest = coordinator.latest.read().await;
    let listed = latest
        .as_ref()?
        .body
        .nodes
        .iter()
        .find(|listed| listed.record.body.node_id == *node && !listed.down)?;
    Some(listed.record.body.url.trim_end_matches('/').to_string())
}

/// What a node answered.
struct NodeAnswer {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Bytes,
}

impl IntoResponse for NodeAnswer {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.body).into_response();
        if let Some(content_type) = self.content_type {
            response.headers_mut().insert(CONTENT_TYPE, content_type);
        }
        response
    }
}

/// Sends `request` to a node and returns its answer, or a `502 Bad
/// Gateway` that stands for a node that could not be reached.
async fn send(request: reqwest::RequestBuilder) -> NodeAnswer {
    match try_send(request).await {
        Ok(answer) => answer,
        Err(error) => NodeAnswer {
            status: StatusCode::BAD_GATEWAY,
            content_type: None,
            body: Bytes::from(format!("the node could not be reached: {error}")),
        },
    }
}

async fn try_send(request: reqwest::RequestBuilder) -> Result<NodeAnswer, reqwest::Error> {
    let response = request.send().await?;
    let status = response.status();
    let content_type = response.headers().get(CONTENT_TYPE).cloned();
    let body = response.bytes().await?;
    Ok(NodeAnswer {
        status,
        content_type,
        body,
    })
}

/// `?query`, or nothing without a query.
fn query_suffix(query: Option<String>) -> String {
    match query {
        Some(query) => format!("?{query}"),
        None => String::new(),
    }
}

fn not_listed() -> Response {
    let reason = "no such node is up in this coordinator's node list";
    (StatusCode::NOT_FOUND, reason).into_response()
}

fn bad_owner() -> Response {
    let reason = "an owner is 64 lowercase hex digits";
    (StatusCode::BAD_REQUEST, reason).into_response()
}

/// Answers a browser's preflight for a `PUT` with a JSON or byte body.
async fn preflight() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    let headers = response.headers_mut();
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, PUT"),
    );
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("content-type"),
    );
    // A day, so a web app storing many pieces asks once.
    headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    response
}

/// Lets pages of any origin read the gateway's answers.
async fn allow_any_origin(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    response
}

//! A node keeps its id across restarts, wherever it listens.

use mesh_core::protocol::NodeIdentity;
use mesh_node::{NodeData, NodeSettings};
use tokio::net::TcpListener;

/// Serves the data in `directory` on a new port and asks it who it is.
async fn identity_served_from(directory: &std::path::Path) -> NodeIdentity {
    let data = NodeData::open(directory).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/identity", listener.local_addr().unwrap());
    let server = tokio::spawn(mesh_node::serve(listener, data, NodeSettings::default()));
    let body = reqwest::get(url).await.unwrap().bytes().await.unwrap();
    server.abort();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn a_restarted_node_on_another_port_has_the_same_id() {
    let directory = tempfile::tempdir().unwrap();

    let first = identity_served_from(directory.path()).await;
    let after_restart = identity_served_from(directory.path()).await;

    assert!(first.is_consistent());
    assert_eq!(after_restart, first);
}

#[tokio::test]
async fn nodes_with_their_own_data_have_their_own_ids() {
    let one = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();

    let one_identity = identity_served_from(one.path()).await;
    let other_identity = identity_served_from(other.path()).await;

    assert_ne!(one_identity.node_id, other_identity.node_id);
}

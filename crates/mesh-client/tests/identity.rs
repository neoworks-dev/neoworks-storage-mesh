//! Asking nodes who they are.

mod test_nodes;

use mesh_client::NodeClient;
use mesh_node::NodeSettings;
use test_nodes::TestNode;

#[tokio::test]
async fn a_node_answers_with_its_id_and_key() {
    let node = TestNode::start(NodeSettings::default()).await;

    let identity = NodeClient::new()
        .unwrap()
        .identity(&node.url)
        .await
        .unwrap();

    assert_eq!(identity.node_id, node.id);
    assert!(identity.is_consistent());
}

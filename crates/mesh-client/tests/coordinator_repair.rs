//! The coordinator repairing registered chunks while the owner's device is
//! off: at once when a chunk has no spare piece left, not before the delay
//! while it has one, paying nodes that require tokens, and only for the
//! chunks it is responsible for.

mod test_nodes;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mesh_client::account::Account;
use mesh_client::chunk_registration::{register, registration_of, take_new_locations};
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_client::tabs::open_tabs;
use mesh_client::tokens::get_tokens;
use mesh_client::{Manifest, NodeClient, NodeList};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::chunk_registry::{
    CHUNKS_ROUTE, RegisterChunks, RegisterChunksResponse, chunk_id, responsible_coordinator,
};
use mesh_core::node_list::{CoordinatorKey, CoordinatorList};
use mesh_core::sign_in::KdfParams;
use mesh_core::tokens::TokenKind;
use mesh_node::{NodeSettings, Registration};
use tempfile::TempDir;
use test_nodes::{TestNode, download_bytes, random_file, store_on, urls};
use tokio::net::TcpListener;

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A coordinator that probes, publishes and looks for repairs every 100 ms,
/// and waits `repair_delay` while a chunk has a spare piece. With `other`,
/// it shares the chunks with a second coordinator of that key.
async fn start_coordinator(
    repair_delay: Duration,
    other: Option<CoordinatorKey>,
) -> (TempDir, String, CoordinatorList) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    data.credit("ana@example.org", 50).await.unwrap();
    let coordinators = CoordinatorList {
        version: 1,
        keys: vec![data.public_key()],
    };
    let coordinators_for_chunks = other.map(|other| CoordinatorList {
        version: 1,
        keys: vec![data.public_key(), other],
    });
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        probe_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        repair_interval: Duration::from_millis(100),
        repair_delay,
        coordinators: coordinators_for_chunks,
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url, coordinators)
}

/// `count` nodes with `settings`, registered at `coordinator`, once its node
/// list holds them all.
async fn start_registered_nodes(
    coordinator: &str,
    coordinators: &CoordinatorList,
    count: usize,
    settings: NodeSettings,
) -> (Vec<TestNode>, NodeList) {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(settings.clone()).await);
    }
    let http = reqwest::Client::new();
    for (index, node) in nodes.iter().enumerate() {
        let registration = Registration {
            coordinator: coordinator.to_string(),
            url: node.url.clone(),
            operator: format!("operator-{index}"),
            network: format!("network-{index}"),
            region: "de".into(),
            capacity_bytes: 1 << 40,
            operator_email: None,
        };
        mesh_node::register(&http, &node.key, &registration)
            .await
            .unwrap();
    }
    let list = list_where(coordinator, coordinators, |list| list.nodes.len() == count).await;
    (nodes, list)
}

/// The first node list `wanted` accepts, within ten seconds.
async fn list_where(
    coordinator: &str,
    coordinators: &CoordinatorList,
    wanted: impl Fn(&NodeList) -> bool,
) -> NodeList {
    let client = NodeClient::new().unwrap();
    for _ in 0..100 {
        if let Ok((list, _)) = NodeList::fetch(&client, coordinator, coordinators).await {
            if wanted(&list) {
                return list;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no node list as wanted was published");
}

/// Takes in new locations until no piece of `manifest` is on `gone`, within
/// ten seconds, and returns how many moved.
async fn wait_for_repair(coordinator: &str, manifest: &mut Manifest, gone: &[&TestNode]) -> usize {
    let client = NodeClient::new().unwrap();
    let mut moved = 0;
    for _ in 0..100 {
        moved += take_new_locations(&client, coordinator, manifest)
            .await
            .unwrap();
        let on_gone = manifest
            .chunks
            .iter()
            .flat_map(|chunk| &chunk.pieces)
            .any(|piece| gone.iter().any(|node| node.id == piece.node));
        if !on_gone {
            return moved;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the coordinator did not repair the chunks");
}

#[tokio::test]
async fn a_chunk_without_a_spare_is_repaired_at_once() {
    let hour = Duration::from_secs(60 * 60);
    let (_data, coordinator, coordinators) = start_coordinator(hour, None).await;
    let (nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 8, NodeSettings::default()).await;
    let file = random_file(1_500_000, 21);
    let mut manifest = store_on(&list, &file, LEASE).await.unwrap();
    let client = NodeClient::new().unwrap();
    let kept = register(&client, &coordinator, &manifest, now() + 3_600)
        .await
        .unwrap();
    assert_eq!(kept, manifest.chunks.len());
    // Two nodes that hold pieces of the first chunk leave: no spare is left.
    let holders: Vec<&TestNode> = nodes
        .iter()
        .filter(|node| {
            manifest.chunks[0]
                .pieces
                .iter()
                .any(|piece| piece.node == node.id)
        })
        .take(2)
        .collect();
    for node in &holders {
        node.stop();
    }

    let moved = wait_for_repair(&coordinator, &mut manifest, &holders).await;

    assert!(moved >= 2);
    // With the repaired pieces in place, two more nodes can go.
    let remaining: Vec<&TestNode> = nodes
        .iter()
        .filter(|node| !holders.iter().any(|gone| gone.id == node.id))
        .collect();
    remaining[0].stop();
    remaining[1].stop();
    let list = list_where(&coordinator, &coordinators, |list| list.nodes.len() <= 6).await;
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn a_chunk_with_a_spare_waits_for_the_delay() {
    let hour = Duration::from_secs(60 * 60);
    let (_data, coordinator, coordinators) = start_coordinator(hour, None).await;
    let (nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 7, NodeSettings::default()).await;
    let mut manifest = store_on(&list, &random_file(300_000, 22), LEASE)
        .await
        .unwrap();
    let client = NodeClient::new().unwrap();
    register(&client, &coordinator, &manifest, now() + 3_600)
        .await
        .unwrap();
    let holder = nodes
        .iter()
        .find(|node| manifest.chunks[0].pieces[0].node == node.id)
        .unwrap();

    holder.stop();
    // Long enough for the node to be marked down and several repair rounds.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let moved = take_new_locations(&client, &coordinator, &mut manifest)
        .await
        .unwrap();
    assert_eq!(moved, 0);
}

#[tokio::test]
async fn nodes_that_require_tokens_are_paid_for_rebuilt_pieces() {
    let (_data, coordinator, coordinators) = start_coordinator(Duration::ZERO, None).await;
    let settings = NodeSettings {
        require_tokens: Some(coordinator.clone()),
        ..NodeSettings::default()
    };
    let (nodes, _) = start_registered_nodes(&coordinator, &coordinators, 7, settings).await;
    // Nodes take tokens only once the coordinator prices them.
    let list = list_where(&coordinator, &coordinators, |list| list.nodes.len() == 7).await;
    wait_for_prices(&coordinator).await;
    let client = NodeClient::new().unwrap();
    let cheap = KdfParams {
        memory_kib: 8,
        iterations: 1,
    };
    let account = Account::generate();
    sign_up(
        &client,
        &coordinator,
        "ana@example.org",
        "pw",
        &account,
        cheap,
    )
    .await
    .unwrap();
    let auth = auth_key(&client, &coordinator, "ana@example.org", "pw")
        .await
        .unwrap();
    let mut wallet = get_tokens(
        &client,
        &coordinator,
        &coordinators,
        "ana@example.org",
        auth,
        TokenKind::Paid,
        7,
    )
    .await
    .unwrap();
    let node_urls = urls(&nodes);
    let by_url: Vec<&str> = node_urls.iter().map(String::as_str).collect();
    open_tabs(&client, &by_url, &mut wallet, 1).await.unwrap();
    let file = random_file(300_000, 23);
    let mut manifest = mesh_client::upload(
        &client,
        &list,
        &account.dedup_key(),
        file.as_slice(),
        LEASE,
        &[],
    )
    .await
    .unwrap();
    register(&client, &coordinator, &manifest, now() + 3_600)
        .await
        .unwrap();
    let holder = nodes
        .iter()
        .find(|node| manifest.chunks[0].pieces[0].node == node.id)
        .unwrap();

    holder.stop();
    let moved = wait_for_repair(&coordinator, &mut manifest, &[holder]).await;

    assert!(moved >= 1);
    let list = list_where(&coordinator, &coordinators, |list| list.nodes.len() == 6).await;
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

/// Waits up to ten seconds for the coordinator's first price table.
async fn wait_for_prices(coordinator: &str) {
    for _ in 0..100 {
        let response = reqwest::get(format!("{coordinator}/prices")).await;
        if response.is_ok_and(|response| response.status().is_success()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the coordinator published no prices");
}

#[tokio::test]
async fn a_coordinator_keeps_only_the_chunks_it_is_responsible_for() {
    let other = CoordinatorKey([7; 32]);
    let (_data, coordinator, own) = start_coordinator(Duration::ZERO, Some(other)).await;
    let both = CoordinatorList {
        version: 1,
        keys: vec![own.keys[0], other],
    };
    let (_nodes, list) =
        start_registered_nodes(&coordinator, &own, 6, NodeSettings::default()).await;
    // About eight chunks, so both coordinators are responsible for some.
    let manifest = store_on(&list, &random_file(8_000_000, 24), LEASE)
        .await
        .unwrap();
    let request = RegisterChunks {
        chunks: manifest
            .chunks
            .iter()
            .map(|chunk| registration_of(chunk, now() + 3_600))
            .collect(),
    };

    let response = reqwest::Client::new()
        .post(format!("{coordinator}{CHUNKS_ROUTE}"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&request).unwrap())
        .send()
        .await
        .unwrap();
    let answer: RegisterChunksResponse =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();

    let expected: Vec<bool> = manifest
        .chunks
        .iter()
        .map(|chunk| {
            let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
            responsible_coordinator(&id, &both) == Some(own.keys[0])
        })
        .collect();
    assert_eq!(answer.accepted, expected);
}

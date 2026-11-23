//! The coordinator repairing registered chunks while the owner's device is
//! off: at once when a chunk has no spare piece left, not before the delay
//! while it has one, paying nodes that require tokens, and only for the
//! chunks it is a keeper of; the second keeper of a chunk repairing while
//! the first is offline; and an account's index registering its files and
//! commits and taking in where the coordinator moved them.

mod test_nodes;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mesh_client::account::Account;
use mesh_client::chunk_registration::{
    locations_at, register, registration_of, take_new_locations,
};
use mesh_client::commits::read_head;
use mesh_client::index::{Index, Session};
use mesh_client::sign_in::{auth_key, sign_up};
use mesh_client::tabs::open_tabs;
use mesh_client::tokens::get_tokens;
use mesh_client::{Manifest, NodeClient, NodeList, upload};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::chunk_registry::{
    CHUNKS_ROUTE, ChunkRegistration, RegisterChunks, RegisterChunksResponse, RegisteredPiece,
    chunk_id, chunk_keepers, chunk_path,
};
use mesh_core::layout::Layout;
use mesh_core::node_list::{CoordinatorKey, CoordinatorList, ListedCoordinator};
use mesh_core::sign_in::KdfParams;
use mesh_core::tokens::TokenKind;
use mesh_core::{NodeId, PieceId};
use mesh_node::{NodeSettings, Registration};
use tempfile::TempDir;
use test_nodes::{TestNode, download_bytes, node_list, random_file, start_nodes, store_on, urls};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const LEASE: Duration = Duration::from_secs(24 * 60 * 60);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Settings that probe, publish and look for repairs every 100 ms, wait
/// `repair_delay` while a chunk has a spare piece, and share the chunks
/// among `coordinators` if given.
fn fast_settings(
    repair_delay: Duration,
    coordinators: Option<CoordinatorList>,
) -> CoordinatorSettings {
    CoordinatorSettings {
        publish_interval: Duration::from_millis(100),
        probe_interval: Duration::from_millis(100),
        speed_test_interval: Duration::from_millis(100),
        repair_interval: Duration::from_millis(100),
        repair_delay,
        coordinators,
        ..CoordinatorSettings::default()
    }
}

/// A coordinator with [`fast_settings`]. With `others`, it shares the
/// chunks with those coordinators.
async fn start_coordinator(
    repair_delay: Duration,
    others: &[ListedCoordinator],
) -> (TempDir, String, CoordinatorList) {
    let directory = tempfile::tempdir().unwrap();
    let data = CoordinatorData::open(directory.path()).unwrap();
    data.credit("ana@example.org", 50).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let coordinators = CoordinatorList {
        version: 1,
        coordinators: vec![ListedCoordinator {
            key: data.public_key(),
            url: url.clone(),
        }],
    };
    let coordinators_for_chunks = (!others.is_empty()).then(|| {
        let mut all = coordinators.clone();
        all.coordinators.extend_from_slice(others);
        all
    });
    let settings = fast_settings(repair_delay, coordinators_for_chunks);
    tokio::spawn(mesh_coordinator::serve(listener, data, settings));
    (directory, url, coordinators)
}

/// `count` coordinators with [`fast_settings`] that share the chunks among
/// them, the list of all of them, and their tasks, which stop a coordinator
/// when aborted.
async fn start_coordinators(
    count: usize,
    repair_delay: Duration,
) -> (Vec<TempDir>, CoordinatorList, Vec<JoinHandle<()>>) {
    let mut directories = Vec::new();
    let mut started = Vec::new();
    let mut listed = Vec::new();
    for _ in 0..count {
        let directory = tempfile::tempdir().unwrap();
        let data = CoordinatorData::open(directory.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listed.push(ListedCoordinator {
            key: data.public_key(),
            url: format!("http://{}", listener.local_addr().unwrap()),
        });
        directories.push(directory);
        started.push((data, listener));
    }
    let all = CoordinatorList {
        version: 1,
        coordinators: listed,
    };
    let mut tasks = Vec::new();
    for (data, listener) in started {
        let settings = fast_settings(repair_delay, Some(all.clone()));
        tasks.push(tokio::spawn(async move {
            let _ = mesh_coordinator::serve(listener, data, settings).await;
        }));
    }
    (directories, all, tasks)
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
    register_nodes(&nodes, coordinator).await;
    let list = list_where(coordinator, coordinators, |list| list.nodes.len() == count).await;
    (nodes, list)
}

/// Registers each of `nodes` at `coordinator`.
async fn register_nodes(nodes: &[TestNode], coordinator: &str) {
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
}

/// `count` default nodes registered at every coordinator on `coordinators`,
/// once each one's node list holds them all, and the first's list.
async fn start_nodes_at_all(
    coordinators: &CoordinatorList,
    count: usize,
) -> (Vec<TestNode>, NodeList) {
    let mut nodes = Vec::new();
    for _ in 0..count {
        nodes.push(TestNode::start(NodeSettings::default()).await);
    }
    let mut lists = Vec::new();
    for coordinator in &coordinators.coordinators {
        register_nodes(&nodes, &coordinator.url).await;
        let list = list_where(&coordinator.url, coordinators, |list| {
            list.nodes.len() == count
        })
        .await;
        lists.push(list);
    }
    (nodes, lists.swap_remove(0))
}

/// The nodes of `nodes` that hold pieces of the first chunk of `manifest`,
/// at most `count` of them.
fn holders<'a>(nodes: &'a [TestNode], manifest: &Manifest, count: usize) -> Vec<&'a TestNode> {
    nodes
        .iter()
        .filter(|node| {
            manifest.chunks[0]
                .pieces
                .iter()
                .any(|piece| piece.node == node.id)
        })
        .take(count)
        .collect()
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
async fn wait_for_repair(
    coordinators: &CoordinatorList,
    manifest: &mut Manifest,
    gone: &[&TestNode],
) -> usize {
    let client = NodeClient::new().unwrap();
    let mut moved = 0;
    for _ in 0..100 {
        moved += take_new_locations(&client, coordinators, manifest)
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
    let (_data, coordinator, coordinators) = start_coordinator(hour, &[]).await;
    let (nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 8, NodeSettings::default()).await;
    // Smaller than the smallest chunk, so the file is one chunk. With more,
    // the nodes leaving could take only one piece of another chunk, which
    // then waits the hour for its spare.
    let file = random_file(200_000, 21);
    let mut manifest = store_on(&list, &file, LEASE).await.unwrap();
    assert_eq!(manifest.chunks.len(), 1);
    let client = NodeClient::new().unwrap();
    let kept = register(&client, &coordinators, &manifest, now() + 3_600)
        .await
        .unwrap();
    assert_eq!(kept, manifest.chunks.len());
    // Two nodes that hold pieces of the first chunk leave: no spare is left.
    let holders = holders(&nodes, &manifest, 2);
    for node in &holders {
        node.stop();
    }

    let moved = wait_for_repair(&coordinators, &mut manifest, &holders).await;

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
    let (_data, coordinator, coordinators) = start_coordinator(hour, &[]).await;
    let (nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 7, NodeSettings::default()).await;
    let mut manifest = store_on(&list, &random_file(300_000, 22), LEASE)
        .await
        .unwrap();
    let client = NodeClient::new().unwrap();
    register(&client, &coordinators, &manifest, now() + 3_600)
        .await
        .unwrap();
    let holder = nodes
        .iter()
        .find(|node| manifest.chunks[0].pieces[0].node == node.id)
        .unwrap();

    holder.stop();
    // Long enough for the node to be marked down and several repair rounds.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let moved = take_new_locations(&client, &coordinators, &mut manifest)
        .await
        .unwrap();
    assert_eq!(moved, 0);
}

#[tokio::test]
async fn nodes_that_require_tokens_are_paid_for_rebuilt_pieces() {
    let (_data, coordinator, coordinators) = start_coordinator(Duration::ZERO, &[]).await;
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
    register(&client, &coordinators, &manifest, now() + 3_600)
        .await
        .unwrap();
    let holder = nodes
        .iter()
        .find(|node| manifest.chunks[0].pieces[0].node == node.id)
        .unwrap();

    holder.stop();
    let moved = wait_for_repair(&coordinators, &mut manifest, &[holder]).await;

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
async fn a_coordinator_keeps_only_the_chunks_it_is_a_keeper_of() {
    // Two more coordinators that never answer; this test only asks the
    // first. With three, each keeps about two thirds of the chunks.
    let others = [7, 8].map(|byte| ListedCoordinator {
        key: CoordinatorKey([byte; 32]),
        url: "http://127.0.0.1:9".into(),
    });
    let (_data, coordinator, own) = start_coordinator(Duration::ZERO, &others).await;
    let mut all = own.clone();
    all.coordinators.extend_from_slice(&others);
    // Registering checks no nodes, so made-up pieces do. Sixty chunks, so
    // that some are kept elsewhere whatever the coordinator's key.
    let chunks: Vec<ChunkRegistration> = (0..60u8)
        .map(|number| ChunkRegistration {
            secret: [number; 32],
            layout: Layout::new(2, 1).unwrap(),
            regions: 1,
            sealed_len: 100,
            expires_at: now() + 3_600,
            pieces: (0..3u8)
                .map(|position| RegisteredPiece {
                    id: PieceId::of(&[number, position]),
                    node: NodeId::of(&[position; 32]),
                })
                .collect(),
        })
        .collect();
    let request = RegisterChunks { chunks };

    let response = reqwest::Client::new()
        .post(format!("{coordinator}{CHUNKS_ROUTE}"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&request).unwrap())
        .send()
        .await
        .unwrap();
    let answer: RegisterChunksResponse =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();

    let expected: Vec<bool> = request
        .chunks
        .iter()
        .map(|chunk| {
            let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
            chunk_keepers(&id, &all).contains(&&own.coordinators[0])
        })
        .collect();
    assert_eq!(answer.accepted, expected);
    assert!(expected.contains(&false) && expected.contains(&true));
}

#[tokio::test]
async fn each_chunk_is_registered_with_its_two_keepers() {
    let hour = Duration::from_secs(60 * 60);
    let (_directories, all, tasks) = start_coordinators(3, hour).await;
    let nodes = start_nodes(NodeSettings::default()).await;
    let list = node_list(&nodes);
    // About eight chunks, so every coordinator keeps some and not others.
    let mut manifest = store_on(&list, &random_file(8_000_000, 28), LEASE)
        .await
        .unwrap();
    let client = NodeClient::new().unwrap();

    let kept = register(&client, &all, &manifest, now() + 3_600)
        .await
        .unwrap();

    assert_eq!(kept, manifest.chunks.len());
    for chunk in &manifest.chunks {
        let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
        let keepers = chunk_keepers(&id, &all);
        for coordinator in &all.coordinators {
            let response = reqwest::get(format!("{}{}", coordinator.url, chunk_path(&id)))
                .await
                .unwrap();
            let is_keeper = keepers.contains(&coordinator);
            assert_eq!(response.status().is_success(), is_keeper);
        }
    }
    // With any one coordinator offline, registering and looking up still
    // work, since every chunk has another keeper.
    tasks[0].abort();
    let kept = register(&client, &all, &manifest, now() + 3_600)
        .await
        .unwrap();
    assert_eq!(kept, manifest.chunks.len());
    let moved = take_new_locations(&client, &all, &mut manifest)
        .await
        .unwrap();
    assert_eq!(moved, 0);
}

#[tokio::test]
async fn the_second_keeper_repairs_while_the_first_is_offline() {
    let (_directories, both, tasks) = start_coordinators(2, Duration::ZERO).await;
    let (nodes, list) = start_nodes_at_all(&both, 8).await;
    let file = random_file(200_000, 29);
    let mut manifest = store_on(&list, &file, LEASE).await.unwrap();
    let client = NodeClient::new().unwrap();
    register(&client, &both, &manifest, now() + 3_600)
        .await
        .unwrap();
    let id = chunk_id(manifest.chunks[0].pieces.iter().map(|piece| &piece.id));
    let keepers = chunk_keepers(&id, &both);
    let first = both
        .coordinators
        .iter()
        .position(|coordinator| coordinator == keepers[0])
        .unwrap();

    tasks[first].abort();
    let holders = holders(&nodes, &manifest, 2);
    for node in &holders {
        node.stop();
    }

    let moved = wait_for_repair(&both, &mut manifest, &holders).await;

    assert!(moved >= 2);
    let second = &keepers[1].url;
    let list = list_where(second, &both, |list| list.nodes.len() == 6).await;
    assert_eq!(
        download_bytes(&client, &list, &manifest).await.unwrap(),
        file
    );
}

#[tokio::test]
async fn the_second_keeper_repairs_a_chunk_the_first_does_not_keep() {
    let hour = Duration::from_secs(60 * 60);
    let (_directories, both, _tasks) = start_coordinators(2, hour).await;
    let (nodes, list) = start_nodes_at_all(&both, 8).await;
    let mut manifest = store_on(&list, &random_file(200_000, 31), LEASE)
        .await
        .unwrap();
    let id = chunk_id(manifest.chunks[0].pieces.iter().map(|piece| &piece.id));
    let second = chunk_keepers(&id, &both)[1].url.clone();
    // As when the first keeper was offline while the owner registered.
    let request = RegisterChunks {
        chunks: vec![registration_of(&manifest.chunks[0], now() + 3_600)],
    };
    reqwest::Client::new()
        .post(format!("{second}{CHUNKS_ROUTE}"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&request).unwrap())
        .send()
        .await
        .unwrap();

    // No spare is left, so the chunk is due at once despite the hour.
    let holders = holders(&nodes, &manifest, 2);
    for node in &holders {
        node.stop();
    }

    assert!(wait_for_repair(&both, &mut manifest, &holders).await >= 2);
}

#[tokio::test]
async fn the_second_keeper_takes_in_what_the_first_repaired() {
    let (_directories, both, _tasks) = start_coordinators(2, Duration::ZERO).await;
    let (nodes, list) = start_nodes_at_all(&both, 8).await;
    let mut manifest = store_on(&list, &random_file(200_000, 30), LEASE)
        .await
        .unwrap();
    let client = NodeClient::new().unwrap();
    register(&client, &both, &manifest, now() + 3_600)
        .await
        .unwrap();
    let id = chunk_id(manifest.chunks[0].pieces.iter().map(|piece| &piece.id));
    let keepers = chunk_keepers(&id, &both);
    let holder = holders(&nodes, &manifest, 1);

    holder[0].stop();
    wait_for_repair(&both, &mut manifest, &holder).await;

    // The first repaired; the second, asking it before repairing, takes in
    // the same new location instead of rebuilding the piece elsewhere.
    let repaired = locations_at(&client, &keepers[0].url, &id).await.unwrap();
    for _ in 0..100 {
        let second = locations_at(&client, &keepers[1].url, &id).await.unwrap();
        if second == repaired {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the second keeper did not take in the first's repair");
}

/// An index session of `account` on `list`, registering with
/// `coordinators` if given.
fn session<'a>(
    client: &'a NodeClient,
    list: &'a NodeList,
    account: &'a Account,
    coordinators: Option<&'a CoordinatorList>,
) -> Session<'a> {
    Session {
        client,
        list,
        account,
        device: "laptop".into(),
        lease: LEASE,
        coordinators,
    }
}

/// Uploads `file` under the session's account and adds it to `index` as
/// `name`.
async fn store_in_index(index: &mut Index, session: &Session<'_>, name: &str, file: &[u8]) {
    let manifest = upload(
        session.client,
        session.list,
        &session.account.dedup_key(),
        file,
        LEASE,
        &[],
    )
    .await
    .unwrap();
    index
        .add_file(session, name, &manifest, now() + LEASE.as_secs())
        .await
        .unwrap();
}

/// The manifest of the commit the account's pointer names.
async fn head_commit(client: &NodeClient, list: &NodeList, account: &Account) -> Manifest {
    let owner = account.pointer_key().verifying_key().to_bytes();
    let (head, _) = read_head(client, list, &owner, &account.message_key(), None)
        .await
        .unwrap();
    head.manifest
}

/// Whether `coordinator` keeps every chunk of `manifest`.
async fn all_registered(coordinator: &str, manifest: &Manifest) -> bool {
    for chunk in &manifest.chunks {
        let id = chunk_id(chunk.pieces.iter().map(|piece| &piece.id));
        let response = reqwest::get(format!("{coordinator}{}", chunk_path(&id)))
            .await
            .unwrap();
        if !response.status().is_success() {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn files_and_commits_stored_through_the_index_are_registered() {
    let hour = Duration::from_secs(60 * 60);
    let (_data, coordinator, coordinators) = start_coordinator(hour, &[]).await;
    let (_nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 6, NodeSettings::default()).await;
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let session = session(&client, &list, &account, Some(&coordinators));
    let mut index = Index::default();

    store_in_index(&mut index, &session, "notes.txt", &random_file(300_000, 25)).await;

    let file = index.file("notes.txt").unwrap().unwrap();
    assert!(all_registered(&coordinator, &file.manifest).await);
    let commit = head_commit(&client, &list, &account).await;
    assert!(all_registered(&coordinator, &commit).await);
}

#[tokio::test]
async fn renewing_the_index_registers_what_was_stored_without_a_coordinator() {
    let hour = Duration::from_secs(60 * 60);
    let (_data, coordinator, coordinators) = start_coordinator(hour, &[]).await;
    let (_nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 6, NodeSettings::default()).await;
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let mut index = Index::default();
    let without = session(&client, &list, &account, None);
    store_in_index(&mut index, &without, "notes.txt", &random_file(300_000, 26)).await;
    let file = index.file("notes.txt").unwrap().unwrap();
    assert!(!all_registered(&coordinator, &file.manifest).await);
    let first_commit = head_commit(&client, &list, &account).await;

    index
        .renew(&session(&client, &list, &account, Some(&coordinators)))
        .await
        .unwrap();

    assert!(all_registered(&coordinator, &file.manifest).await);
    assert!(all_registered(&coordinator, &first_commit).await);
    let renewal_commit = head_commit(&client, &list, &account).await;
    assert!(all_registered(&coordinator, &renewal_commit).await);
}

#[tokio::test]
async fn reading_the_index_takes_in_pieces_the_coordinator_moved() {
    let (_data, coordinator, coordinators) = start_coordinator(Duration::ZERO, &[]).await;
    let (nodes, list) =
        start_registered_nodes(&coordinator, &coordinators, 8, NodeSettings::default()).await;
    let client = NodeClient::new().unwrap();
    let account = Account::generate();
    let mut laptop = Index::default();
    let laptop_session = session(&client, &list, &account, Some(&coordinators));
    store_in_index(
        &mut laptop,
        &laptop_session,
        "notes.txt",
        &random_file(200_000, 27),
    )
    .await;
    let mut stored = laptop.file("notes.txt").unwrap().unwrap().manifest;
    let holder = nodes
        .iter()
        .find(|node| stored.chunks[0].pieces[0].node == node.id)
        .unwrap();
    holder.stop();
    wait_for_repair(&coordinators, &mut stored, &[holder]).await;
    let list = list_where(&coordinator, &coordinators, |list| list.nodes.len() == 7).await;

    let mut phone = Index::default();
    phone
        .pull(&session(&client, &list, &account, Some(&coordinators)))
        .await
        .unwrap();

    let on_phone = phone.file("notes.txt").unwrap().unwrap().manifest;
    assert_eq!(nodes_of(&on_phone), nodes_of(&stored));
    // The phone saved the move, so a device without the coordinator sees it.
    let mut tablet = Index::default();
    tablet
        .pull(&session(&client, &list, &account, None))
        .await
        .unwrap();
    let on_tablet = tablet.file("notes.txt").unwrap().unwrap().manifest;
    assert_eq!(nodes_of(&on_tablet), nodes_of(&stored));
}

/// The node of every piece of `manifest`, chunk by chunk.
fn nodes_of(manifest: &Manifest) -> Vec<NodeId> {
    manifest
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.pieces)
        .map(|piece| piece.node)
        .collect()
}

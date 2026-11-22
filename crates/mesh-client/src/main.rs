//! `mesh`: stores a file on nodes from a list, keeps it stored and gets it
//! back.
//!
//! ```sh
//! mesh put photo.jpg --nodes nodes.json --manifest photo.manifest.json
//! mesh renew photo.manifest.json --nodes nodes.json --days 30
//! mesh audit photo.manifest.json --nodes nodes.json
//! mesh repair photo.manifest.json --nodes nodes.json
//! mesh get photo.manifest.json photo-again.jpg --nodes nodes.json
//! ```
//!
//! `nodes.json` lists the nodes to choose from; `mesh_client::NodeList`
//! describes it. Manifests name nodes by id, so every command looks up where
//! the nodes are now in the list. Instead of `--nodes`, every command takes
//! `--coordinator http://… --coordinators coordinators.json` to fetch the
//! signed node list, where `coordinators.json` holds the coordinator list
//! whose keys it must be signed with:
//!
//! ```json
//! { "version": 1, "keys": ["5c0f…"] }
//! ```
//!
//! The manifest holds the chunk keys: anyone with it can read the file. `put`
//! derives those keys from a dedup key kept in `mesh.key`, which it creates
//! on first use; until there are accounts, that file is what makes the same
//! chunk encrypt the same way every time.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use mesh_client::erasure::DATA_PIECES;
use mesh_client::{
    AUDIT_DEADLINE, ChunkRepair, DedupKey, Manifest, NodeClient, NodeList, PieceHealth,
};
use mesh_core::node_list::CoordinatorList;

use crate::account_commands::AccountArguments;

mod account_commands;

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Encrypts a file, spreads it over nodes from the list and writes its
    /// manifest.
    Put(PutArguments),
    /// Makes the nodes keep a file for another number of days from now.
    Renew {
        manifest: PathBuf,
        #[command(flatten)]
        nodes: NodeListArguments,
        #[arg(long, default_value_t = 30)]
        days: u64,
    },
    /// Checks that the nodes still hold every piece, without downloading them.
    Audit {
        manifest: PathBuf,
        #[command(flatten)]
        nodes: NodeListArguments,
    },
    /// Audits a file, rebuilds the pieces that are lost onto other nodes from
    /// the list and updates the manifest.
    Repair {
        manifest: PathBuf,
        #[command(flatten)]
        nodes: NodeListArguments,
        /// How many days the new nodes keep the rebuilt pieces.
        #[arg(long, default_value_t = 30)]
        days: u64,
    },
    /// Reads a file back from the nodes its manifest names.
    Get {
        manifest: PathBuf,
        output: PathBuf,
        #[command(flatten)]
        nodes: NodeListArguments,
    },
    /// Creates a new account: a secret every device of it shares.
    AccountNew {
        #[arg(long, default_value = "account.key")]
        account: PathBuf,
    },
    /// Creates a sign-in for the account with an email and a password, read
    /// from standard input; creates the account first if needed.
    SignUp {
        email: String,
        /// The sign-in coordinator.
        #[arg(long)]
        coordinator: String,
        #[arg(long, default_value = "account.key")]
        account: PathBuf,
    },
    /// Signs in on a new device and writes the account's secret.
    SignIn {
        email: String,
        /// The sign-in coordinator.
        #[arg(long)]
        coordinator: String,
        #[arg(long, default_value = "account.key")]
        account: PathBuf,
    },
    /// Stores a file and records it in the account's index.
    Store {
        file: PathBuf,
        /// The name to store it under; its file name if not given.
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        account: AccountArguments,
    },
    /// Lists the files in the account's index.
    Ls {
        #[command(flatten)]
        account: AccountArguments,
    },
    /// Renews everything the account keeps, one request per node; meant to
    /// run from a timer, such as weekly.
    RenewAll {
        #[command(flatten)]
        account: AccountArguments,
    },
    /// Reads a file from the account's index back.
    Fetch {
        name: String,
        output: PathBuf,
        #[command(flatten)]
        account: AccountArguments,
    },
}

/// Where the nodes to choose from come from: a file, or a coordinator.
#[derive(Args)]
#[group(required = true, multiple = true)]
struct NodeListArguments {
    /// A JSON list of nodes to choose from.
    #[arg(long, conflicts_with = "coordinator")]
    nodes: Option<PathBuf>,
    /// A coordinator to fetch the signed node list from.
    #[arg(long, requires = "coordinators")]
    coordinator: Option<String>,
    /// The JSON coordinator list whose signatures the node list must carry.
    #[arg(long, requires = "coordinator")]
    coordinators: Option<PathBuf>,
}

#[derive(Args)]
struct PutArguments {
    file: PathBuf,
    #[command(flatten)]
    nodes: NodeListArguments,
    /// Where to write the manifest. Keep it private: it holds the keys.
    #[arg(long)]
    manifest: PathBuf,
    /// How many days the nodes keep the file unless it is renewed.
    #[arg(long, default_value_t = 30)]
    days: u64,
    /// The file holding your dedup key; created if missing. Keep it private.
    #[arg(long, default_value = "mesh.key")]
    key_file: PathBuf,
    /// A manifest whose chunks may be reused, such as an earlier version of
    /// the same file; give as many as you like.
    #[arg(long = "earlier")]
    earlier_manifests: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = NodeClient::new()?;
    match Arguments::parse().command {
        Command::Put(arguments) => put(&client, &arguments).await,
        Command::Renew {
            manifest,
            nodes,
            days,
        } => renew(&client, &manifest, &nodes, days).await,
        Command::Audit { manifest, nodes } => audit(&client, &manifest, &nodes).await,
        Command::Repair {
            manifest,
            nodes,
            days,
        } => repair(&client, &manifest, &nodes, days).await,
        Command::Get {
            manifest,
            output,
            nodes,
        } => get(&client, &manifest, &output, &nodes).await,
        Command::AccountNew { account } => account_commands::account_new(&account),
        Command::SignUp {
            email,
            coordinator,
            account,
        } => account_commands::sign_up(&client, &coordinator, &email, &account).await,
        Command::SignIn {
            email,
            coordinator,
            account,
        } => account_commands::sign_in(&client, &coordinator, &email, &account).await,
        Command::Store {
            file,
            name,
            account,
        } => account_commands::store(&client, &file, name, &account).await,
        Command::Ls { account } => account_commands::list_files(&client, &account).await,
        Command::RenewAll { account } => account_commands::renew_all(&client, &account).await,
        Command::Fetch {
            name,
            output,
            account,
        } => account_commands::fetch(&client, &name, &output, &account).await,
    }
}

/// The node list from the file or coordinator the arguments name.
async fn load_node_list(
    client: &NodeClient,
    arguments: &NodeListArguments,
) -> anyhow::Result<NodeList> {
    if let Some(path) = &arguments.nodes {
        return NodeList::read(path).with_context(|| format!("reading {}", path.display()));
    }
    // clap requires either `--nodes` or both of these.
    let (Some(coordinator), Some(coordinators_path)) =
        (&arguments.coordinator, &arguments.coordinators)
    else {
        anyhow::bail!("give --nodes, or --coordinator with --coordinators");
    };
    let json = std::fs::read(coordinators_path)
        .with_context(|| format!("reading {}", coordinators_path.display()))?;
    let coordinators: CoordinatorList = serde_json::from_slice(&json)
        .with_context(|| format!("parsing {}", coordinators_path.display()))?;
    let (list, published_at) = NodeList::fetch(client, coordinator, &coordinators).await?;
    println!(
        "{} nodes from {coordinator}, published at {published_at}",
        list.nodes.len()
    );
    Ok(list)
}

async fn put(client: &NodeClient, arguments: &PutArguments) -> anyhow::Result<()> {
    let list = load_node_list(client, &arguments.nodes).await?;
    let dedup_key = load_or_create_dedup_key(&arguments.key_file)?;
    let earlier = arguments
        .earlier_manifests
        .iter()
        .map(|path| read_manifest(path))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let reader = tokio::fs::File::open(&arguments.file)
        .await
        .with_context(|| format!("opening {}", arguments.file.display()))?;
    let lease = days_to_duration(arguments.days);
    let manifest = mesh_client::upload(client, &list, &dedup_key, reader, lease, &earlier).await?;
    write_manifest(&arguments.manifest, &manifest)?;
    println!(
        "stored {} bytes in {} chunks for {} days, manifest in {}",
        manifest.size,
        manifest.chunks.len(),
        arguments.days,
        arguments.manifest.display()
    );
    Ok(())
}

/// The dedup key in `path`, or a new one written there, readable by its
/// owner only.
fn load_or_create_dedup_key(path: &Path) -> anyhow::Result<DedupKey> {
    if path.exists() {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        return DedupKey::from_hex(&text).with_context(|| format!("parsing {}", path.display()));
    }
    let key = DedupKey::generate();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    writeln!(file, "{}", key.to_hex())?;
    println!("created a new dedup key in {}", path.display());
    Ok(key)
}

async fn renew(
    client: &NodeClient,
    manifest_path: &Path,
    nodes: &NodeListArguments,
    days: u64,
) -> anyhow::Result<()> {
    let list = load_node_list(client, nodes).await?;
    let manifest = read_manifest(manifest_path)?;
    let report = mesh_client::renew(client, &list, &manifest, days_to_duration(days)).await;
    println!(
        "renewed {} pieces for {days} days, {} not renewed",
        report.renewed,
        report.not_renewed.len()
    );
    Ok(())
}

/// Prints one line per chunk, the unhealthy pieces under it, and whether the
/// file can still be read.
async fn audit(
    client: &NodeClient,
    manifest_path: &Path,
    nodes: &NodeListArguments,
) -> anyhow::Result<()> {
    let list = load_node_list(client, nodes).await?;
    let manifest = read_manifest(manifest_path)?;
    let report = mesh_client::audit(client, &list, &manifest, AUDIT_DEADLINE).await;
    for (index, (chunk, chunk_audit)) in manifest.chunks.iter().zip(&report.chunks).enumerate() {
        println!(
            "chunk {index}: {} of {} pieces healthy",
            chunk_audit.healthy(),
            chunk_audit.pieces.len()
        );
        for (piece, health) in chunk.pieces.iter().zip(&chunk_audit.pieces) {
            if *health != PieceHealth::Healthy {
                println!("  {:?} on {}", health, piece.node);
            }
        }
    }
    let unreadable = report
        .chunks
        .iter()
        .filter(|chunk_audit| chunk_audit.healthy() < DATA_PIECES)
        .count();
    if unreadable == 0 {
        println!("every chunk can be read");
    } else {
        println!("{unreadable} chunks have fewer than {DATA_PIECES} healthy pieces");
    }
    Ok(())
}

/// Prints what happened to each chunk and saves the manifest when any piece
/// moved.
async fn repair(
    client: &NodeClient,
    manifest_path: &Path,
    nodes: &NodeListArguments,
    days: u64,
) -> anyhow::Result<()> {
    let list = load_node_list(client, nodes).await?;
    let mut manifest = read_manifest(manifest_path)?;
    let audit = mesh_client::audit(client, &list, &manifest, AUDIT_DEADLINE).await;
    let lease = days_to_duration(days);
    let report = mesh_client::repair(client, &mut manifest, &audit, &list, lease).await;
    let mut any_moved = false;
    for (index, outcome) in report.chunks.iter().enumerate() {
        match outcome {
            ChunkRepair::Intact => println!("chunk {index}: intact"),
            ChunkRepair::Repaired { replaced } => {
                println!("chunk {index}: rebuilt {replaced} pieces");
                any_moved = true;
            }
            ChunkRepair::Partly { replaced, unplaced } => {
                println!("chunk {index}: rebuilt {replaced} pieces, no node took {unplaced}");
                any_moved |= *replaced > 0;
            }
            ChunkRepair::Lost { fetched } => {
                println!("chunk {index}: lost, only {fetched} of {DATA_PIECES} pieces needed");
            }
        }
    }
    if any_moved {
        write_manifest(manifest_path, &manifest)?;
        println!("manifest updated");
    }
    Ok(())
}

async fn get(
    client: &NodeClient,
    manifest_path: &Path,
    output: &Path,
    nodes: &NodeListArguments,
) -> anyhow::Result<()> {
    let list = load_node_list(client, nodes).await?;
    let manifest = read_manifest(manifest_path)?;
    download_to(client, &list, &manifest, output).await
}

/// Downloads the file `manifest` describes from nodes in `list` into a
/// temporary file next to `output` and renames it once the whole file
/// arrived, so a failed download never leaves half a file under the name
/// asked for.
async fn download_to(
    client: &NodeClient,
    list: &NodeList,
    manifest: &Manifest,
    output: &Path,
) -> anyhow::Result<()> {
    let temporary_path = output.with_extension("partial");
    let writer = tokio::fs::File::create(&temporary_path)
        .await
        .with_context(|| format!("creating {}", temporary_path.display()))?;
    if let Err(error) = mesh_client::download(client, list, manifest, writer).await {
        // The partial file is useless and may be large; its removal failing
        // only leaves it behind.
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error.into());
    }
    std::fs::rename(&temporary_path, output)
        .with_context(|| format!("replacing {}", output.display()))?;
    println!("read {} bytes into {}", manifest.size, output.display());
    Ok(())
}

fn read_manifest(path: &Path) -> anyhow::Result<Manifest> {
    let json = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&json).with_context(|| format!("parsing {}", path.display()))
}

/// Writes the manifest through a temporary file next to it. Repair overwrites
/// an existing manifest, and a half-written one would lose the file's key.
fn write_manifest(path: &Path, manifest: &Manifest) -> anyhow::Result<()> {
    let json = serde_json::to_vec_pretty(manifest)?;
    let temporary_path = path.with_extension("partial");
    std::fs::write(&temporary_path, json)
        .with_context(|| format!("writing {}", temporary_path.display()))?;
    std::fs::rename(&temporary_path, path)
        .with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

fn days_to_duration(days: u64) -> Duration {
    Duration::from_secs(days * 24 * 60 * 60)
}

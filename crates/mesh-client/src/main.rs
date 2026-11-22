//! `mesh`: stores a file on nodes from a list, keeps it stored and gets it
//! back.
//!
//! ```sh
//! mesh put photo.jpg --nodes nodes.json --manifest photo.manifest.json
//! mesh renew photo.manifest.json --days 30
//! mesh audit photo.manifest.json
//! mesh repair photo.manifest.json --nodes nodes.json
//! mesh get photo.manifest.json photo-again.jpg
//! ```
//!
//! `nodes.json` lists the nodes to choose from; `mesh_client::NodeList`
//! describes it. The manifest holds the file's key: anyone with it can read
//! the file.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use mesh_client::erasure::DATA_PIECES;
use mesh_client::{AUDIT_DEADLINE, ChunkRepair, Manifest, NodeClient, NodeList, PieceHealth};

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Encrypts a file, spreads it over nodes from the list and writes its
    /// manifest.
    Put {
        file: PathBuf,
        /// The JSON list of nodes to choose from.
        #[arg(long)]
        nodes: PathBuf,
        /// Where to write the manifest. Keep it private: it holds the key.
        #[arg(long)]
        manifest: PathBuf,
        /// How many days the nodes keep the file unless it is renewed.
        #[arg(long, default_value_t = 30)]
        days: u64,
    },
    /// Makes the nodes keep a file for another number of days from now.
    Renew {
        manifest: PathBuf,
        #[arg(long, default_value_t = 30)]
        days: u64,
    },
    /// Checks that the nodes still hold every piece, without downloading them.
    Audit { manifest: PathBuf },
    /// Audits a file, rebuilds the pieces that are lost onto other nodes from
    /// the list and updates the manifest.
    Repair {
        manifest: PathBuf,
        /// The JSON list of nodes to choose from.
        #[arg(long)]
        nodes: PathBuf,
        /// How many days the new nodes keep the rebuilt pieces.
        #[arg(long, default_value_t = 30)]
        days: u64,
    },
    /// Reads a file back from the nodes its manifest names.
    Get { manifest: PathBuf, output: PathBuf },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = NodeClient::new()?;
    match Arguments::parse().command {
        Command::Put {
            file,
            nodes,
            manifest,
            days,
        } => put(&client, &file, &nodes, &manifest, days).await,
        Command::Renew { manifest, days } => renew(&client, &manifest, days).await,
        Command::Audit { manifest } => audit(&client, &manifest).await,
        Command::Repair {
            manifest,
            nodes,
            days,
        } => repair(&client, &manifest, &nodes, days).await,
        Command::Get { manifest, output } => get(&client, &manifest, &output).await,
    }
}

async fn put(
    client: &NodeClient,
    file: &Path,
    nodes_path: &Path,
    manifest_path: &Path,
    days: u64,
) -> anyhow::Result<()> {
    let list = NodeList::read(nodes_path)?;
    let bytes = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let manifest = mesh_client::upload(client, &list, &bytes, days_to_duration(days)).await?;
    write_manifest(manifest_path, &manifest)?;
    println!(
        "stored {} bytes in {} chunks for {days} days, manifest in {}",
        manifest.size,
        manifest.chunks.len(),
        manifest_path.display()
    );
    Ok(())
}

async fn renew(client: &NodeClient, manifest_path: &Path, days: u64) -> anyhow::Result<()> {
    let manifest = read_manifest(manifest_path)?;
    let report = mesh_client::renew(client, &manifest, days_to_duration(days)).await;
    println!(
        "renewed {} pieces for {days} days, {} not renewed",
        report.renewed,
        report.not_renewed.len()
    );
    Ok(())
}

/// Prints one line per chunk, the unhealthy pieces under it, and whether the
/// file can still be read.
async fn audit(client: &NodeClient, manifest_path: &Path) -> anyhow::Result<()> {
    let manifest = read_manifest(manifest_path)?;
    let report = mesh_client::audit(client, &manifest, AUDIT_DEADLINE).await;
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
    nodes_path: &Path,
    days: u64,
) -> anyhow::Result<()> {
    let list = NodeList::read(nodes_path)?;
    let mut manifest = read_manifest(manifest_path)?;
    let audit = mesh_client::audit(client, &manifest, AUDIT_DEADLINE).await;
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

async fn get(client: &NodeClient, manifest_path: &Path, output: &Path) -> anyhow::Result<()> {
    let manifest = read_manifest(manifest_path)?;
    let bytes = mesh_client::download(client, &manifest).await?;
    std::fs::write(output, &bytes).with_context(|| format!("writing {}", output.display()))?;
    println!("read {} bytes into {}", bytes.len(), output.display());
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

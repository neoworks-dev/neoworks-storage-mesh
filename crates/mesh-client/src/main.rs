//! `mesh`: stores a file on six nodes, keeps it stored and gets it back.
//!
//! ```sh
//! mesh put photo.jpg --manifest photo.manifest.json \
//!     --node http://127.0.0.1:7001 --node http://127.0.0.1:7002 … (six in all)
//! mesh renew photo.manifest.json --days 30
//! mesh get photo.manifest.json photo-again.jpg
//! ```
//!
//! The manifest holds the file's key: anyone with it can read the file.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use mesh_client::{Manifest, NodeClient};

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Encrypts a file, spreads it over six nodes and writes its manifest.
    Put {
        file: PathBuf,
        /// Where to write the manifest. Keep it private: it holds the key.
        #[arg(long)]
        manifest: PathBuf,
        /// A node's base URL; give exactly six.
        #[arg(long = "node", required = true)]
        nodes: Vec<String>,
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
    /// Reads a file back from the nodes its manifest names.
    Get { manifest: PathBuf, output: PathBuf },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = NodeClient::new()?;
    match Arguments::parse().command {
        Command::Put {
            file,
            manifest,
            nodes,
            days,
        } => put(&client, &file, &manifest, &nodes, days).await,
        Command::Renew { manifest, days } => renew(&client, &manifest, days).await,
        Command::Get { manifest, output } => get(&client, &manifest, &output).await,
    }
}

async fn put(
    client: &NodeClient,
    file: &Path,
    manifest_path: &Path,
    nodes: &[String],
    days: u64,
) -> anyhow::Result<()> {
    let bytes = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let manifest = mesh_client::upload(client, nodes, &bytes, days_to_duration(days)).await?;
    let json = serde_json::to_vec_pretty(&manifest)?;
    std::fs::write(manifest_path, json)
        .with_context(|| format!("writing {}", manifest_path.display()))?;
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

fn days_to_duration(days: u64) -> Duration {
    Duration::from_secs(days * 24 * 60 * 60)
}

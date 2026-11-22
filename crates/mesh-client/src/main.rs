//! `mesh`: stores a file on six nodes and gets it back.
//!
//! ```sh
//! mesh put photo.jpg --manifest photo.manifest.json \
//!     --node http://127.0.0.1:7001 --node http://127.0.0.1:7002 … (six in all)
//! mesh get photo.manifest.json photo-again.jpg
//! ```
//!
//! The manifest holds the file's key: anyone with it can read the file.

use std::path::PathBuf;

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
        } => put(&client, file, manifest, nodes).await,
        Command::Get { manifest, output } => get(&client, manifest, output).await,
    }
}

async fn put(
    client: &NodeClient,
    file: PathBuf,
    manifest_path: PathBuf,
    nodes: Vec<String>,
) -> anyhow::Result<()> {
    let bytes = std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
    let manifest = mesh_client::upload(client, &nodes, &bytes).await?;
    let json = serde_json::to_vec_pretty(&manifest)?;
    std::fs::write(&manifest_path, json)
        .with_context(|| format!("writing {}", manifest_path.display()))?;
    println!(
        "stored {} bytes in {} chunks, manifest in {}",
        manifest.size,
        manifest.chunks.len(),
        manifest_path.display()
    );
    Ok(())
}

async fn get(client: &NodeClient, manifest_path: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    let json = std::fs::read(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest: Manifest = serde_json::from_slice(&json)?;
    let bytes = mesh_client::download(client, &manifest).await?;
    std::fs::write(&output, &bytes).with_context(|| format!("writing {}", output.display()))?;
    println!("read {} bytes into {}", bytes.len(), output.display());
    Ok(())
}

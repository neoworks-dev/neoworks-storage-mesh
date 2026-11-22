//! `mesh-node`: runs a storage node.
//!
//! ```sh
//! mesh-node --listen 127.0.0.1:7001 --data ./data/node-1
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use mesh_node::{NodeSettings, PieceStore};
use tokio::net::TcpListener;

/// Runs a storage node that keeps pieces in a directory.
#[derive(Parser)]
struct Arguments {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: SocketAddr,
    /// Directory the pieces and their leases are kept in; created if missing.
    #[arg(long)]
    data: PathBuf,
    /// The longest lease the node grants, in days.
    #[arg(long, default_value_t = 30)]
    max_lease_days: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let settings = NodeSettings {
        max_lease: Duration::from_secs(arguments.max_lease_days * 24 * 60 * 60),
        ..NodeSettings::default()
    };
    let store = PieceStore::open(&arguments.data)
        .await
        .with_context(|| format!("opening {}", arguments.data.display()))?;
    let listener = TcpListener::bind(arguments.listen)
        .await
        .with_context(|| format!("listening on {}", arguments.listen))?;
    println!("node listening on http://{}", arguments.listen);
    mesh_node::serve(listener, store, settings).await?;
    Ok(())
}

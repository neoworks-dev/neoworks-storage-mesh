//! `mesh-coordinator`: runs a coordinator.
//!
//! ```sh
//! mesh-coordinator --listen 127.0.0.1:7000 --data ./data/coordinator
//! ```
//!
//! It prints its public key on start; clients need it in their coordinator
//! list to accept its node list. Until billing exists, bought tokens are
//! credited to an account by hand:
//!
//! ```sh
//! mesh-coordinator --data ./data/coordinator credit ana@example.org 8000
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use mesh_coordinator::{CoordinatorData, CoordinatorSettings};
use mesh_core::NodeId;
use tokio::net::TcpListener;

/// Runs a coordinator that publishes the signed node list.
#[derive(Parser)]
struct Arguments {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: SocketAddr,
    /// Directory the coordinator's key and registry are kept in; created if
    /// missing.
    #[arg(long)]
    data: PathBuf,
    /// How often to publish the node list, in seconds.
    #[arg(long, default_value_t = 60 * 60)]
    publish_seconds: u64,
    /// The id of a node the coordinator runs itself, which takes the free
    /// allowance tokens; repeat for several.
    #[arg(long = "own-node")]
    own_nodes: Vec<NodeId>,
    #[command(subcommand)]
    command: Option<Command>,
}

/// Something to do instead of running.
#[derive(Subcommand)]
enum Command {
    /// Adds tokens to an account's paid balance, and prints the new balance.
    Credit { email: String, tokens: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let data = CoordinatorData::open(&arguments.data)
        .with_context(|| format!("opening {}", arguments.data.display()))?;
    if let Some(Command::Credit { email, tokens }) = &arguments.command {
        let balance = data.credit(email, *tokens).await?;
        println!("{email} now has {balance} tokens");
        return Ok(());
    }
    let settings = CoordinatorSettings {
        publish_interval: Duration::from_secs(arguments.publish_seconds),
        own_nodes: arguments.own_nodes,
        ..CoordinatorSettings::default()
    };
    let listener = TcpListener::bind(arguments.listen)
        .await
        .with_context(|| format!("listening on {}", arguments.listen))?;
    println!("coordinator key {}", hex::encode(data.public_key().0));
    println!("listening on http://{}", arguments.listen);
    mesh_coordinator::serve(listener, data, settings).await?;
    Ok(())
}

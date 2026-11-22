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
    /// DB-IP's "IP to City Lite" CSV, to find which country nodes are in.
    #[arg(long)]
    city_csv: Option<PathBuf>,
    /// iptoasn.com's `ip2asn-combined.tsv`, to find which network nodes are
    /// in.
    #[arg(long)]
    asn_tsv: Option<PathBuf>,
    /// Where the coordinator is, as `latitude,longitude`, to rule out node
    /// locations its round trips are too short for.
    #[arg(long, value_parser = parse_location)]
    location: Option<(f64, f64)>,
    #[command(subcommand)]
    command: Option<Command>,
}

/// Something to do instead of running.
#[derive(Subcommand)]
enum Command {
    /// Adds tokens to an account's paid balance, and prints the new balance.
    Credit { email: String, tokens: u64 },
}

/// A location written as `latitude,longitude`, such as `50.11,8.68`.
fn parse_location(text: &str) -> Result<(f64, f64), String> {
    let invalid = || format!("{text} is not latitude,longitude");
    let (latitude, longitude) = text.split_once(',').ok_or_else(invalid)?;
    let latitude: f64 = latitude.trim().parse().map_err(|_| invalid())?;
    let longitude: f64 = longitude.trim().parse().map_err(|_| invalid())?;
    if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
        return Err(invalid());
    }
    Ok((latitude, longitude))
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
        city_csv: arguments.city_csv,
        asn_tsv: arguments.asn_tsv,
        location: arguments.location,
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

//! `mesh-node`: runs a storage node.
//!
//! ```sh
//! mesh-node --listen 127.0.0.1:7001 --data ./data/node-1
//! ```
//!
//! With a coordinator, the node registers there every hour:
//!
//! ```sh
//! mesh-node --listen 0.0.0.0:7001 --data ./data/node-1 \
//!     --coordinator http://127.0.0.1:7000 --public-url http://203.0.113.7:7001 \
//!     --operator alice --network AS3320 --region de --capacity-gb 500
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser};
use mesh_node::{NodeData, NodeSettings, Registration};
use tokio::net::TcpListener;

/// How often the node registers again, so a coordinator that lost its
/// registry or missed a move catches up within the hour.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Runs a storage node that keeps pieces in a directory.
#[derive(Parser)]
struct Arguments {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: SocketAddr,
    /// Directory the node's key, pieces, pointers and leases are kept in;
    /// created if missing.
    #[arg(long)]
    data: PathBuf,
    /// The longest lease the node grants, in days.
    #[arg(long, default_value_t = 30)]
    max_lease_days: u64,
    #[command(flatten)]
    registration: RegistrationArguments,
}

/// Where and as what to register; all or nothing.
#[derive(Args)]
#[group(requires_all = ["coordinator", "public_url", "operator", "network", "region", "capacity_gb"], multiple = true)]
struct RegistrationArguments {
    /// The coordinator to register with.
    #[arg(long)]
    coordinator: Option<String>,
    /// The URL clients reach this node at from outside.
    #[arg(long)]
    public_url: Option<String>,
    /// Who runs the node.
    #[arg(long)]
    operator: Option<String>,
    /// The network the node is reached through, such as its AS number.
    #[arg(long)]
    network: Option<String>,
    /// Where the node is, such as `de`.
    #[arg(long)]
    region: Option<String>,
    /// How many gigabytes the node offers to store.
    #[arg(long)]
    capacity_gb: Option<u64>,
}

impl RegistrationArguments {
    /// The registration, when the arguments ask for one. The group above
    /// makes clap refuse any but all or none of them.
    fn registration(self) -> Option<Registration> {
        Some(Registration {
            coordinator: self.coordinator?,
            url: self.public_url?,
            operator: self.operator?,
            network: self.network?,
            region: self.region?,
            capacity_bytes: self.capacity_gb? * 1_000_000_000,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let settings = NodeSettings {
        max_lease: Duration::from_secs(arguments.max_lease_days * 24 * 60 * 60),
        ..NodeSettings::default()
    };
    let data = NodeData::open(&arguments.data)
        .await
        .with_context(|| format!("opening {}", arguments.data.display()))?;
    let listener = TcpListener::bind(arguments.listen)
        .await
        .with_context(|| format!("listening on {}", arguments.listen))?;
    println!("node {}", data.identity().node_id);
    println!("listening on http://{}", arguments.listen);
    let key = data.key.clone();
    let server = mesh_node::serve(listener, data, settings);
    let Some(registration) = arguments.registration.registration() else {
        server.await?;
        return Ok(());
    };
    // The listener is bound already, so the coordinator's check of the node
    // reaches it even if it comes before the server starts accepting.
    tokio::select! {
        result = server => result?,
        () = mesh_node::register_forever(key, registration, REGISTRATION_INTERVAL) => {}
    }
    Ok(())
}

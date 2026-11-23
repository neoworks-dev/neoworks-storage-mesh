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
//!
//! Pieces can live in an S3-compatible bucket instead of the data directory,
//! which still holds the node's key and its leases:
//!
//! ```sh
//! MESH_BUCKET_ACCESS_KEY=… MESH_BUCKET_SECRET_KEY=… mesh-node --data ./data/node-1 \
//!     --bucket-endpoint https://fsn1.your-objectstorage.com --bucket node-1 --bucket-region fsn1
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser};
use mesh_node::{Bucket, BucketSettings, NodeData, NodeSettings, Registration};
use tokio::net::TcpListener;

/// How often the node registers again, so a coordinator that lost its
/// registry or missed a move catches up within the hour.
const REGISTRATION_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Runs a storage node that keeps pieces in a directory or a bucket.
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
    /// Store pieces only against tabs paid with tokens, spent at this
    /// coordinator.
    #[arg(long)]
    require_tokens: Option<String>,
    #[command(flatten)]
    bucket: BucketArguments,
    #[command(flatten)]
    registration: RegistrationArguments,
}

/// An S3-compatible bucket to keep pieces in instead of the data directory;
/// all or nothing. The keys come from the environment, so they never show
/// up in the process list: `MESH_BUCKET_ACCESS_KEY` and
/// `MESH_BUCKET_SECRET_KEY`.
#[derive(Args)]
#[group(requires_all = ["bucket_endpoint", "bucket", "bucket_region"], multiple = true)]
struct BucketArguments {
    /// The object store's address, such as
    /// `https://fsn1.your-objectstorage.com`.
    #[arg(long)]
    bucket_endpoint: Option<String>,
    /// The bucket's name. Give each node a bucket of its own.
    #[arg(long)]
    bucket: Option<String>,
    /// The region requests are signed for, such as `fsn1`.
    #[arg(long)]
    bucket_region: Option<String>,
}

impl BucketArguments {
    /// The bucket's settings, when the arguments name one.
    fn settings(self) -> anyhow::Result<Option<BucketSettings>> {
        let (Some(endpoint), Some(bucket), Some(region)) =
            (self.bucket_endpoint, self.bucket, self.bucket_region)
        else {
            return Ok(None);
        };
        Ok(Some(BucketSettings {
            endpoint,
            bucket,
            region,
            access_key: std::env::var("MESH_BUCKET_ACCESS_KEY")
                .context("reading MESH_BUCKET_ACCESS_KEY")?,
            secret_key: std::env::var("MESH_BUCKET_SECRET_KEY")
                .context("reading MESH_BUCKET_SECRET_KEY")?,
        }))
    }
}

/// Where and as what to register; all or nothing, except the operator's
/// email, which is optional.
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
    /// The email of the account the node's earnings for hosting go to.
    #[arg(long)]
    operator_email: Option<String>,
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
            operator_email: self.operator_email,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let settings = NodeSettings {
        max_lease: Duration::from_secs(arguments.max_lease_days * 24 * 60 * 60),
        require_tokens: arguments.require_tokens.clone(),
        ..NodeSettings::default()
    };
    let data = match arguments.bucket.settings()? {
        Some(settings) => {
            let bucket = Bucket::new(settings)?;
            NodeData::open_with_bucket(&arguments.data, bucket).await
        }
        None => NodeData::open(&arguments.data).await,
    };
    let data = data.with_context(|| format!("opening {}", arguments.data.display()))?;
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

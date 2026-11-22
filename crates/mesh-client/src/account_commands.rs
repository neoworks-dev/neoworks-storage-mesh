//! The `mesh` commands that work through an account's index instead of
//! manifest files: `account-new`, `sign-up`, `sign-in`, `store`, `ls`,
//! `fetch` and `renew-all`.
//!
//! The account's secret lives in a file, and this device's copy of the index
//! in another, so that only what changed since the last run is fetched.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use mesh_client::account::Account;
use mesh_client::index::{Index, Session, unix_now};
use mesh_client::{NodeClient, NodeList, upload};
use mesh_core::sign_in::KdfParams;

use crate::{NodeListArguments, days_to_duration, download_to, load_node_list};

/// What every account command needs.
#[derive(Args)]
pub struct AccountArguments {
    /// The file holding the account's secret. Keep it private: it opens
    /// every file of the account.
    #[arg(long, default_value = "account.key")]
    account: PathBuf,
    /// This device's copy of the index; created if missing.
    #[arg(long, default_value = "mesh-index.json")]
    index: PathBuf,
    /// This device's name, recorded in the index's history.
    #[arg(long, default_value = "mesh")]
    device: String,
    #[command(flatten)]
    nodes: NodeListArguments,
    /// How many days the nodes keep what is stored now.
    #[arg(long, default_value_t = 30)]
    days: u64,
}

/// Writes a new account secret to `path`, readable by its owner only.
pub fn account_new(path: &Path) -> anyhow::Result<()> {
    write_account(path, &Account::generate())?;
    println!("created a new account in {}", path.display());
    Ok(())
}

/// Writes `account`'s secret to `path`, readable by its owner only.
fn write_account(path: &Path, account: &Account) -> anyhow::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    // Never overwrite an account: its secret is the only way to its files.
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    writeln!(file, "{}", account.to_hex())?;
    Ok(())
}

/// Creates an account for `email` on `coordinator`, with the secret in
/// `account_path`, made first if it does not exist.
pub async fn sign_up(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    account_path: &Path,
) -> anyhow::Result<()> {
    if !account_path.exists() {
        account_new(account_path)?;
    }
    let account = read_account(account_path)?;
    let password = read_password()?;
    mesh_client::sign_in::sign_up(
        client,
        coordinator,
        email,
        &password,
        &account,
        KdfParams::DEFAULT,
    )
    .await?;
    println!("signed up as {email}");
    Ok(())
}

/// Signs in as `email` on `coordinator` and writes the account's secret to
/// `account_path`, which must not exist yet.
pub async fn sign_in(
    client: &NodeClient,
    coordinator: &str,
    email: &str,
    account_path: &Path,
) -> anyhow::Result<()> {
    let password = read_password()?;
    let account = mesh_client::sign_in::sign_in(client, coordinator, email, &password).await?;
    write_account(account_path, &account)?;
    println!(
        "signed in as {email}; the account is in {}",
        account_path.display()
    );
    Ok(())
}

/// A password, as one line on standard input, so it never shows up in the
/// process list or the shell's history.
fn read_password() -> anyhow::Result<String> {
    eprint!("password: ");
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading the password")?;
    Ok(line.trim_end_matches(['\n', '\r']).to_string())
}

/// Stores `file` under `name` and records it in the index.
pub async fn store(
    client: &NodeClient,
    file: &Path,
    name: Option<String>,
    arguments: &AccountArguments,
) -> anyhow::Result<()> {
    let name = match name {
        Some(name) => name,
        None => file_name(file)?,
    };
    let account = read_account(&arguments.account)?;
    let list = load_node_list(client, &arguments.nodes).await?;
    let lease = days_to_duration(arguments.days);
    let session = Session {
        client,
        list: &list,
        account: &account,
        device: arguments.device.clone(),
        lease,
        coordinator: arguments.nodes.coordinator.as_deref(),
    };
    let mut index = read_index(&arguments.index)?;
    index.pull(&session).await?;
    // An earlier version under the same name lends its chunks, so only what
    // changed is uploaded.
    let earlier: Vec<_> = index
        .file(&name)?
        .map(|file| file.manifest)
        .into_iter()
        .collect();
    let reader = tokio::fs::File::open(file)
        .await
        .with_context(|| format!("opening {}", file.display()))?;
    let renew_by = unix_now() + lease.as_secs();
    let manifest = upload(client, &list, &account.dedup_key(), reader, lease, &earlier).await?;
    index.add_file(&session, &name, &manifest, renew_by).await?;
    write_index(&arguments.index, &index)?;
    println!(
        "stored {name}, {} bytes, for {} days",
        manifest.size, arguments.days
    );
    Ok(())
}

/// Prints every file in the index.
pub async fn list_files(client: &NodeClient, arguments: &AccountArguments) -> anyhow::Result<()> {
    let list = load_node_list(client, &arguments.nodes).await?;
    let index = pulled_index(client, &list, arguments).await?;
    for file in index.files()? {
        println!(
            "{}\t{} bytes\trenew by {}",
            file.name, file.manifest.size, file.renew_by
        );
    }
    Ok(())
}

/// Downloads the file `name` from the index to `output`.
pub async fn fetch(
    client: &NodeClient,
    name: &str,
    output: &Path,
    arguments: &AccountArguments,
) -> anyhow::Result<()> {
    let list = load_node_list(client, &arguments.nodes).await?;
    let index = pulled_index(client, &list, arguments).await?;
    let Some(file) = index.file(name)? else {
        anyhow::bail!("the index has no file called {name}");
    };
    download_to(client, &list, &file.manifest, output).await
}

/// Renews every file of the account, the index's history and its pointer,
/// and warns about files that will become unreadable.
pub async fn renew_all(client: &NodeClient, arguments: &AccountArguments) -> anyhow::Result<()> {
    let account = read_account(&arguments.account)?;
    let list = load_node_list(client, &arguments.nodes).await?;
    let session = Session {
        client,
        list: &list,
        account: &account,
        device: arguments.device.clone(),
        lease: days_to_duration(arguments.days),
        coordinator: arguments.nodes.coordinator.as_deref(),
    };
    let mut index = read_index(&arguments.index)?;
    let renewal = index.renew(&session).await?;
    write_index(&arguments.index, &index)?;
    println!(
        "renewed {} pieces for {} days, {} not renewed",
        renewal.pieces_renewed, arguments.days, renewal.pieces_not_renewed
    );
    let now = unix_now();
    for file in &renewal.at_risk {
        let hours_left = file.renew_by.saturating_sub(now) / 3600;
        println!(
            "warning: {} lost too many pieces and is unreadable in {hours_left} hours unless repaired",
            file.name
        );
    }
    Ok(())
}

/// This device's index, brought up to date from nodes in `list` and saved.
async fn pulled_index(
    client: &NodeClient,
    list: &NodeList,
    arguments: &AccountArguments,
) -> anyhow::Result<Index> {
    let account = read_account(&arguments.account)?;
    let session = Session {
        client,
        list,
        account: &account,
        device: arguments.device.clone(),
        lease: days_to_duration(arguments.days),
        coordinator: arguments.nodes.coordinator.as_deref(),
    };
    let mut index = read_index(&arguments.index)?;
    index.pull(&session).await?;
    write_index(&arguments.index, &index)?;
    Ok(index)
}

fn file_name(path: &Path) -> anyhow::Result<String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| {
            format!(
                "{} has no name to store it under; give --name",
                path.display()
            )
        })?;
    Ok(name.to_string())
}

fn read_account(path: &Path) -> anyhow::Result<Account> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Account::from_hex(&text).with_context(|| format!("parsing {}", path.display()))
}

/// This device's copy of the index, or an empty one on first use.
fn read_index(path: &Path) -> anyhow::Result<Index> {
    match std::fs::read(path) {
        Ok(json) => {
            serde_json::from_slice(&json).with_context(|| format!("parsing {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Index::default()),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

/// Saves this device's copy of the index through a temporary file, so a
/// crash never leaves half of it.
fn write_index(path: &Path, index: &Index) -> anyhow::Result<()> {
    // An index is plain strings and numbers, which always serialize.
    let json = serde_json::to_vec(index).expect("an index serializes");
    let temporary_path = path.with_extension("partial");
    std::fs::write(&temporary_path, json)
        .with_context(|| format!("writing {}", temporary_path.display()))?;
    std::fs::rename(&temporary_path, path).with_context(|| format!("replacing {}", path.display()))
}

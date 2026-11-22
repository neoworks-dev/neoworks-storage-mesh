//! Everything a node keeps in its data directory, opened together.
//!
//! ```text
//! <data>/node.key                 the node's own key; its id is the hash
//! <data>/pieces/<64 hex digits>   one file per piece
//! <data>/leases.sqlite3           when each piece's lease ends
//! <data>/pointers.sqlite3         signed pointers and their leases
//! <data>/tabs.sqlite3             paid tabs and what they hold
//! <data>/mailboxes.sqlite3        mailbox messages and their leases
//! ```

use std::path::Path;

use mesh_core::NodeId;
use mesh_core::pointer::SigningKey;
use mesh_core::protocol::NodeIdentity;

use crate::mailbox_store::MailboxStore;
use crate::node_key::{self, NodeKeyError};
use crate::piece_store::{PieceStore, StoreError};
use crate::pointer_store::PointerStore;
use crate::tab_book::TabBook;

/// A node's pieces, pointers, tabs, mailboxes and key.
pub struct NodeData {
    pub pieces: PieceStore,
    pub pointers: PointerStore,
    pub tabs: TabBook,
    pub mailboxes: MailboxStore,
    pub key: SigningKey,
}

/// A data directory that could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("pieces: {0}")]
    Pieces(#[from] StoreError),
    #[error("pointers, tabs or mailboxes: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("{0}")]
    Key(#[from] NodeKeyError),
}

impl NodeData {
    /// Opens the data in `directory`, creating whatever is missing, including
    /// the node's key on first start.
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self, OpenError> {
        let directory = directory.as_ref();
        let pieces = PieceStore::open(directory).await?;
        let pointers = PointerStore::open(directory)?;
        let tabs = TabBook::open(directory)?;
        let mailboxes = MailboxStore::open(directory)?;
        let key = node_key::load_or_create(directory)?;
        Ok(NodeData {
            pieces,
            pointers,
            tabs,
            mailboxes,
            key,
        })
    }

    /// The node's id and public key.
    pub fn identity(&self) -> NodeIdentity {
        let public_key = self.key.verifying_key().to_bytes();
        NodeIdentity {
            node_id: NodeId::of(&public_key),
            public_key,
        }
    }
}

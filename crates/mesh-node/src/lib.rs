//! A storage node of the mesh.
//!
//! A node keeps pieces under their hash for as long as their lease lasts and
//! hands them back. It knows nothing about whose pieces they are or what they
//! hold: every piece arrives encrypted and erasure-coded by a client. It also
//! keeps signed pointers, the newest record per owner key, which tell devices
//! where the newest version of something is. A node is known by its id, the
//! hash of its own public key, so it stays the same node when its address
//! changes, and tells a coordinator where it is with a record signed by that
//! key ([`register`]). The `mesh-node` binary runs one; tests start nodes
//! in-process through [`serve`].

mod clock;
mod lease_table;
mod node_data;
mod node_key;
mod piece_store;
mod pointer_store;
mod registration;
mod routes;
mod settings;
mod sweep;

pub use node_data::{NodeData, OpenError};
pub use node_key::NodeKeyError;
pub use piece_store::{PieceStore, StoreError};
pub use pointer_store::{PointerPut, PointerStore, PointerStoreError};
pub use registration::{Registration, RegistrationError, register, register_forever};
pub use routes::router;
pub use settings::NodeSettings;

use std::sync::Arc;

use tokio::net::TcpListener;

/// Serves `data` on `listener` and sweeps ended leases, until the task is
/// dropped or the server fails.
pub async fn serve(
    listener: TcpListener,
    data: NodeData,
    settings: NodeSettings,
) -> std::io::Result<()> {
    let identity = data.identity();
    let pieces = Arc::new(data.pieces);
    let pointers = Arc::new(data.pointers);
    let app = router(pieces.clone(), pointers.clone(), identity, &settings);
    let server = axum::serve(listener, app);
    // Both run on this task, so stopping the node stops its sweep too.
    tokio::select! {
        result = server => result,
        () = sweep::sweep_forever(pieces, pointers, settings.sweep_interval) => Ok(()),
    }
}

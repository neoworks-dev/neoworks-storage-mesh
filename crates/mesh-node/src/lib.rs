//! A storage node of the mesh.
//!
//! A node keeps pieces under their hash for as long as their lease lasts and
//! hands them back. It knows nothing about whose pieces they are or what they
//! hold: every piece arrives encrypted and erasure-coded by a client. It also
//! keeps signed pointers, the newest record per owner key, which tell devices
//! where the newest version of something is. The `mesh-node` binary runs one;
//! tests start nodes in-process through [`serve`].

mod clock;
mod lease_table;
mod piece_store;
mod pointer_store;
mod routes;
mod settings;
mod sweep;

pub use piece_store::{PieceStore, StoreError};
pub use pointer_store::{PointerPut, PointerStore, PointerStoreError};
pub use routes::router;
pub use settings::NodeSettings;

use std::sync::Arc;

use tokio::net::TcpListener;

/// Serves `store` and `pointers` on `listener` and sweeps ended leases, until
/// the task is dropped or the server fails.
pub async fn serve(
    listener: TcpListener,
    store: PieceStore,
    pointers: PointerStore,
    settings: NodeSettings,
) -> std::io::Result<()> {
    let store = Arc::new(store);
    let pointers = Arc::new(pointers);
    let server = axum::serve(listener, router(store.clone(), pointers.clone(), &settings));
    // Both run on this task, so stopping the node stops its sweep too.
    tokio::select! {
        result = server => result,
        () = sweep::sweep_forever(store, pointers, settings.sweep_interval) => Ok(()),
    }
}

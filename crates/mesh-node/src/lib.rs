//! A storage node of the mesh.
//!
//! A node keeps pieces under their hash and hands them back. It knows nothing
//! about whose pieces they are or what they hold: every piece arrives
//! encrypted and erasure-coded by a client. The `mesh-node` binary runs one;
//! tests start nodes in-process through [`serve`].

mod piece_store;
mod routes;

pub use piece_store::{PieceStore, PutError};
pub use routes::router;

use tokio::net::TcpListener;

/// Serves `store` on `listener` until the task is dropped or fails.
pub async fn serve(listener: TcpListener, store: PieceStore) -> std::io::Result<()> {
    axum::serve(listener, router(store)).await
}

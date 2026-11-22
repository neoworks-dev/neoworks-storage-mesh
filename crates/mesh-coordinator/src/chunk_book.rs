//! The chunks this coordinator repairs, in `<data>/chunks.sqlite3`
//! (`mesh_core::chunk_registry` describes registering them).
//!
//! Two tables: `chunk` holds each chunk's layout, sealed length, expiry and
//! the hash of its owner's secret; `chunk_piece` where each of its pieces is.
//! Repair moves a piece by changing its node.

use std::path::Path;

use mesh_core::chunk_registry::{ChunkRegistration, RegisteredPiece, chunk_id};
use mesh_core::layout::Layout;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::sync::Mutex;

/// A registered chunk, as repair needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookedChunk {
    pub id: [u8; 32],
    pub layout: Layout,
    pub regions: u8,
    pub sealed_len: u32,
    pub expires_at: u64,
    /// The pieces in coding order.
    pub pieces: Vec<RegisteredPiece>,
}

/// What became of a registration.
#[derive(Debug, PartialEq, Eq)]
pub enum Booked {
    /// The chunk is new, or its owner updated it.
    Kept,
    /// The chunk is registered under another secret.
    OtherOwner,
    /// The pieces don't fit the layout.
    Malformed,
}

/// Registered chunks and where their pieces are.
pub struct ChunkBook {
    /// SQLite connections are not shared between threads.
    connection: Mutex<Connection>,
}

impl ChunkBook {
    /// Opens the book in `directory`, creating its tables if needed.
    pub fn open(directory: &Path) -> rusqlite::Result<Self> {
        let connection = Connection::open(directory.join("chunks.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS chunk (
                 id         BLOB PRIMARY KEY,
                 verifier   BLOB NOT NULL,
                 data       INTEGER NOT NULL,
                 parity     INTEGER NOT NULL,
                 regions    INTEGER NOT NULL,
                 sealed_len INTEGER NOT NULL,
                 expires_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS chunk_piece (
                 chunk_id BLOB NOT NULL,
                 position INTEGER NOT NULL,
                 piece_id TEXT NOT NULL,
                 node_id  TEXT NOT NULL,
                 PRIMARY KEY (chunk_id, position)
             );",
        )?;
        Ok(ChunkBook {
            connection: Mutex::new(connection),
        })
    }

    /// Keeps `registration`, unless its chunk was registered with another
    /// secret: then whoever sent it is not the chunk's owner, and could
    /// otherwise point the coordinator, and through it the owner, at the
    /// wrong nodes.
    pub async fn register(&self, registration: &ChunkRegistration) -> rusqlite::Result<Booked> {
        if registration.pieces.len() != registration.layout.total() {
            return Ok(Booked::Malformed);
        }
        let id = chunk_id(registration.pieces.iter().map(|piece| &piece.id));
        let verifier = blake3::hash(&registration.secret);
        let mut connection = self.connection.lock().await;
        let transaction = connection.transaction()?;
        let held: Option<[u8; 32]> = transaction
            .query_row(
                "SELECT verifier FROM chunk WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if held.is_some_and(|held| held != *verifier.as_bytes()) {
            return Ok(Booked::OtherOwner);
        }
        let layout = registration.layout;
        transaction.execute(
            "INSERT OR REPLACE INTO chunk
                 (id, verifier, data, parity, regions, sealed_len, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                verifier.as_bytes(),
                layout.data,
                layout.parity,
                registration.regions,
                registration.sealed_len,
                registration.expires_at
            ],
        )?;
        for (position, piece) in registration.pieces.iter().enumerate() {
            transaction.execute(
                "INSERT OR REPLACE INTO chunk_piece (chunk_id, position, piece_id, node_id)
                 VALUES (?1, ?2, ?3, ?4)",
                params![id, position, piece.id.to_string(), piece.node.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(Booked::Kept)
    }

    /// The pieces of the chunk `id`, in coding order, if it is registered.
    pub async fn pieces(&self, id: &[u8; 32]) -> rusqlite::Result<Option<Vec<RegisteredPiece>>> {
        let connection = self.connection.lock().await;
        let known: bool = connection.query_row(
            "SELECT EXISTS (SELECT 1 FROM chunk WHERE id = ?1)",
            params![id],
            |row| row.get(0),
        )?;
        if !known {
            return Ok(None);
        }
        Ok(Some(pieces_of(&connection, id)?))
    }

    /// Every chunk still kept at `now`.
    pub async fn live_chunks(&self, now: u64) -> rusqlite::Result<Vec<BookedChunk>> {
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(
            "SELECT id, data, parity, regions, sealed_len, expires_at FROM chunk
             WHERE expires_at > ?1",
        )?;
        let rows = statement.query_map(params![now], |row| {
            Ok((
                row.get::<_, [u8; 32]>(0)?,
                row.get::<_, u8>(1)?,
                row.get::<_, u8>(2)?,
                row.get::<_, u8>(3)?,
                row.get::<_, u32>(4)?,
                row.get::<_, u64>(5)?,
            ))
        })?;
        let mut chunks = Vec::new();
        for row in rows {
            let (id, data, parity, regions, sealed_len, expires_at) = row?;
            chunks.push(BookedChunk {
                id,
                layout: Layout { data, parity },
                regions,
                sealed_len,
                expires_at,
                pieces: pieces_of(&connection, &id)?,
            });
        }
        Ok(chunks)
    }

    /// Notes that piece `position` of the chunk `id` is on `piece.node` now.
    pub async fn move_piece(
        &self,
        id: &[u8; 32],
        position: usize,
        piece: &RegisteredPiece,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "UPDATE chunk_piece SET node_id = ?3 WHERE chunk_id = ?1 AND position = ?2",
            params![id, position, piece.node.to_string()],
        )?;
        Ok(())
    }

    /// Forgets the chunks that ended at or before `now`.
    pub async fn forget_ended(&self, now: u64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().await;
        connection.execute(
            "DELETE FROM chunk_piece WHERE chunk_id IN (SELECT id FROM chunk WHERE expires_at <= ?1)",
            params![now],
        )?;
        connection.execute("DELETE FROM chunk WHERE expires_at <= ?1", params![now])?;
        Ok(())
    }
}

/// The pieces of the chunk `id`, in coding order.
fn pieces_of(connection: &Connection, id: &[u8; 32]) -> rusqlite::Result<Vec<RegisteredPiece>> {
    let mut statement = connection.prepare(
        "SELECT piece_id, node_id FROM chunk_piece WHERE chunk_id = ?1 ORDER BY position",
    )?;
    let rows = statement.query_map(params![id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut pieces = Vec::new();
    for row in rows {
        let (piece_id, node_id) = row?;
        // Only `register` and `move_piece` write this table, from parsed ids.
        let (Ok(id), Ok(node)) = (piece_id.parse(), node_id.parse()) else {
            continue;
        };
        pieces.push(RegisteredPiece { id, node });
    }
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use mesh_core::{NodeId, PieceId};

    use super::*;

    fn registration(secret: u8, expires_at: u64) -> ChunkRegistration {
        ChunkRegistration {
            secret: [secret; 32],
            layout: Layout::new(2, 1).unwrap(),
            regions: 1,
            sealed_len: 100,
            expires_at,
            pieces: (0..3u8)
                .map(|byte| RegisteredPiece {
                    id: PieceId::of(&[byte]),
                    node: NodeId::of(&[byte; 32]),
                })
                .collect(),
        }
    }

    fn book() -> (tempfile::TempDir, ChunkBook) {
        let directory = tempfile::tempdir().unwrap();
        let book = ChunkBook::open(directory.path()).unwrap();
        (directory, book)
    }

    #[tokio::test]
    async fn only_the_owner_updates_a_chunk() {
        let (_directory, book) = book();

        assert_eq!(
            book.register(&registration(1, 100)).await.unwrap(),
            Booked::Kept
        );
        assert_eq!(
            book.register(&registration(1, 200)).await.unwrap(),
            Booked::Kept
        );
        assert_eq!(
            book.register(&registration(2, 300)).await.unwrap(),
            Booked::OtherOwner
        );

        let chunks = book.live_chunks(0).await.unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].expires_at, 200);
    }

    #[tokio::test]
    async fn pieces_that_do_not_fit_the_layout_are_refused() {
        let (_directory, book) = book();
        let mut malformed = registration(1, 100);
        malformed.pieces.pop();

        assert_eq!(book.register(&malformed).await.unwrap(), Booked::Malformed);
    }

    #[tokio::test]
    async fn a_moved_piece_is_found_at_its_new_node_and_ended_chunks_go() {
        let (_directory, book) = book();
        let registered = registration(1, 100);
        book.register(&registered).await.unwrap();
        let id = chunk_id(registered.pieces.iter().map(|piece| &piece.id));
        let moved = RegisteredPiece {
            id: registered.pieces[1].id,
            node: NodeId::of(&[9; 32]),
        };

        book.move_piece(&id, 1, &moved).await.unwrap();

        assert_eq!(book.pieces(&id).await.unwrap().unwrap()[1], moved);
        book.forget_ended(100).await.unwrap();
        assert_eq!(book.pieces(&id).await.unwrap(), None);
    }
}

//! Registering chunks with the coordinator that repairs them while their
//! owner's devices are off.
//!
//! A client tells a coordinator, for each chunk, which piece ids belong
//! together, on which nodes they are, how the chunk is cut and until when it
//! is kept. It sends no key: pieces are erasure-coded after encryption, so
//! any `data` of them rebuild the sealed chunk without opening it. The
//! coordinator learns which pieces belong together and how much an account
//! stores, and nothing about what.
//!
//! A chunk is named by the hash of its piece ids ([`chunk_id`]), which only
//! someone who knows every piece id can compute; no single node can. The
//! first registration also files a verifier of a secret derived from the
//! chunk's key, and later registrations of the chunk, such as after a
//! renewal, must carry the same secret.
//!
//! Each chunk is kept by two coordinators ([`chunk_keepers`]), so it is
//! still repaired while either is offline. The first repairs it; the second
//! only once the first has been unreachable for the repair delay, so the two
//! don't rebuild the same pieces twice. Clients send each chunk's
//! registration to, and ask its locations at, both keepers' URLs on the
//! coordinator list.
//!
//! - `POST /chunks` with a [`RegisterChunks`] registers or updates chunks
//!   and answers with a [`RegisterChunksResponse`].
//! - `GET /chunks/{id}` returns the chunk's [`ChunkLocations`], where repair
//!   may have moved pieces since, or `404 Not Found`.

use serde::{Deserialize, Serialize};

use crate::layout::Layout;
use crate::node_list::{CoordinatorList, ListedCoordinator};
use crate::{NodeId, PieceId};

/// The route chunks are registered at.
pub const CHUNKS_ROUTE: &str = "/chunks";
/// The route of one chunk's locations, in the router syntax of `axum`.
pub const CHUNK_ROUTE: &str = "/chunks/{id}";
/// The most chunks one request may register, which keeps a request below a
/// few megabytes.
pub const MAX_CHUNKS_PER_REQUEST: usize = 1000;

/// The path of the locations of the chunk `id`.
pub fn chunk_path(id: &[u8; 32]) -> String {
    format!("/chunks/{}", hex::encode(id))
}

/// The id of the chunk whose pieces are `piece_ids`, in coding order.
pub fn chunk_id<'a>(piece_ids: impl IntoIterator<Item = &'a PieceId>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("neoworks-mesh chunk id v1");
    for id in piece_ids {
        hasher.update(id.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// How many coordinators keep each chunk's registration. Two, so a chunk
/// is still repaired while any one coordinator is offline.
pub const KEEPERS_PER_CHUNK: usize = 2;

/// The coordinators on `coordinators` that keep the chunk `id`, the one that
/// repairs it first: the [`KEEPERS_PER_CHUNK`] whose keys score highest with
/// it, fewer on a shorter list. The second repairs only while the first is
/// unreachable.
///
/// Scoring every key against the chunk (rendezvous hashing) needs no table
/// that coordinators would have to agree on. Every coordinator and client
/// with the same list picks the same ones, and when a coordinator joins or
/// leaves, only the chunks it wins or held move; the rest stay where they
/// are.
///
/// Only keys are scored, so a coordinator moving to another URL keeps its
/// chunks. A key listed twice counts once, so a chunk always has two
/// different keepers when the list has two different keys.
pub fn chunk_keepers<'list>(
    id: &[u8; 32],
    coordinators: &'list CoordinatorList,
) -> Vec<&'list ListedCoordinator> {
    let mut ranked: Vec<&ListedCoordinator> = coordinators.coordinators.iter().collect();
    ranked.sort_by_key(|coordinator| std::cmp::Reverse(keeper_score(id, coordinator)));
    ranked.dedup_by_key(|coordinator| coordinator.key);
    ranked.truncate(KEEPERS_PER_CHUNK);
    ranked
}

/// How high `coordinator` ranks for keeping the chunk `id`.
fn keeper_score(id: &[u8; 32], coordinator: &ListedCoordinator) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("neoworks-mesh chunk owner v1");
    hasher.update(id);
    hasher.update(&coordinator.key.0);
    *hasher.finalize().as_bytes()
}

/// One piece of a registered chunk and the node holding it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredPiece {
    pub id: PieceId,
    pub node: NodeId,
}

/// What a coordinator needs to repair one chunk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkRegistration {
    /// Derived from the chunk's key; only its owner knows it. The coordinator
    /// keeps a hash of it, and only a registration with the same secret
    /// changes the chunk later.
    #[serde(with = "hex")]
    pub secret: [u8; 32],
    pub layout: Layout,
    /// The fewest regions the pieces spread over.
    pub regions: u8,
    /// The sealed chunk's length, which rebuilding cuts the padding off to.
    pub sealed_len: u32,
    /// Until when the owner keeps the chunk, in Unix seconds. Repair stops
    /// then, and new pieces are stored until then at most.
    pub expires_at: u64,
    /// The pieces in coding order.
    pub pieces: Vec<RegisteredPiece>,
}

/// Chunks to register or update.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterChunks {
    pub chunks: Vec<ChunkRegistration>,
}

/// For each chunk asked, whether the coordinator now keeps it: `false` for
/// a chunk registered with another secret, one this coordinator is not a
/// keeper of, or one whose pieces don't fit its layout.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterChunksResponse {
    pub accepted: Vec<bool>,
}

/// Where a registered chunk's pieces are now.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLocations {
    pub pieces: Vec<RegisteredPiece>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_list::CoordinatorKey;

    fn list_of(count: u8) -> CoordinatorList {
        CoordinatorList {
            version: 1,
            coordinators: (0..count)
                .map(|byte| ListedCoordinator {
                    key: CoordinatorKey([byte; 32]),
                    url: format!("https://coordinator-{byte}.example.org"),
                })
                .collect(),
        }
    }

    /// The keys of the keepers of `id` on `list`, the first first.
    fn keepers(id: &[u8; 32], list: &CoordinatorList) -> Vec<CoordinatorKey> {
        chunk_keepers(id, list)
            .into_iter()
            .map(|coordinator| coordinator.key)
            .collect()
    }

    /// The key of the first keeper of `id` on `list`.
    fn owner(id: &[u8; 32], list: &CoordinatorList) -> Option<CoordinatorKey> {
        keepers(id, list).first().copied()
    }

    fn ids() -> Vec<[u8; 32]> {
        (0..1000u32)
            .map(|number| *blake3::hash(&number.to_be_bytes()).as_bytes())
            .collect()
    }

    #[test]
    fn a_chunk_is_named_by_all_its_pieces_in_order() {
        let first = PieceId::of(b"one");
        let second = PieceId::of(b"two");

        assert_eq!(chunk_id(&[first, second]), chunk_id(&[first, second]));
        assert_ne!(chunk_id(&[first, second]), chunk_id(&[second, first]));
        assert_ne!(chunk_id(&[first, second]), chunk_id(&[first]));
    }

    #[test]
    fn chunks_spread_over_coordinators_and_mostly_stay_when_one_joins() {
        let three = list_of(3);
        let four = list_of(4);
        let ids = ids();

        let moved = ids
            .iter()
            .filter(|id| owner(id, &three) != owner(id, &four))
            .count();
        let on_first = ids
            .iter()
            .filter(|id| owner(id, &three) == Some(CoordinatorKey([0; 32])))
            .count();

        // A fourth coordinator takes about a quarter, all from the others.
        assert!((150..350).contains(&moved), "{moved}");
        assert!((250..420).contains(&on_first), "{on_first}");
    }

    #[test]
    fn a_coordinator_that_moves_keeps_its_chunks() {
        let before = list_of(3);
        let mut after = before.clone();
        after.coordinators[0].url = "https://elsewhere.example.org".into();

        for id in ids() {
            assert_eq!(keepers(&id, &before), keepers(&id, &after));
        }
    }

    #[test]
    fn a_chunk_has_two_different_keepers_ranked_by_score() {
        let mut three = list_of(3);
        // A key listed twice still counts as one keeper.
        three.coordinators.push(three.coordinators[0].clone());

        for id in ids() {
            let keepers = keepers(&id, &three);
            assert_eq!(keepers.len(), 2);
            assert_ne!(keepers[0], keepers[1]);
            assert_eq!(Some(keepers[0]), owner(&id, &list_of(3)));
        }
        assert_eq!(keepers(&ids()[0], &list_of(1)).len(), 1);
        assert!(keepers(&ids()[0], &list_of(0)).is_empty());
    }

    #[test]
    fn when_the_first_keeper_leaves_the_second_moves_up() {
        let three = list_of(3);
        for id in ids() {
            let before = keepers(&id, &three);
            let mut two = three.clone();
            two.coordinators
                .retain(|coordinator| coordinator.key != before[0]);

            assert_eq!(keepers(&id, &two)[0], before[1]);
        }
    }
}

//! Checking that the nodes still hold a file's pieces, without downloading
//! them.
//!
//! For every piece, the client asks its node for a few random segments and
//! checks their Merkle proofs against the root recorded at upload. A node only
//! passes if it has those exact bytes at hand, and since it can't know in
//! advance which segments will be asked for, it has to keep all of them.
//!
//! The audit only reports. Deciding what to do about unhealthy pieces is
//! repair's job.

use std::collections::BTreeSet;
use std::time::Duration;

use futures::future::join_all;
use mesh_core::merkle::{self, MerkleHash, SegmentProof};

use crate::manifest::{Manifest, PlacedPiece};
use crate::node_client::{NodeClient, NodeRequestError};
use crate::node_list::NodeList;
use crate::random::random_u32;

/// Segments asked for per piece. A node that kept only half of a piece passes
/// one audit with a chance of (1/2)^8, about 0.4%, and audits repeat.
pub const AUDIT_SEGMENTS: usize = 8;

/// How long a node gets to answer. A node that fetches pieces from elsewhere
/// only when challenged, instead of storing them, would need longer than an
/// honest node reading its own disk.
pub const AUDIT_DEADLINE: Duration = Duration::from_secs(5);

/// The state of one piece, as an audit found it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceHealth {
    /// The node proved it holds the piece.
    Healthy,
    /// The node said it does not hold the piece.
    Missing,
    /// The node answered, but its proofs were wrong or its answer made no
    /// sense, as when the piece is corrupted on its disk.
    Failed,
    /// The node did not answer within the deadline.
    Unreachable,
}

/// The health of one chunk's pieces.
#[derive(Debug)]
pub struct ChunkAudit {
    /// One entry per piece, in the order the manifest lists them.
    pub pieces: Vec<PieceHealth>,
}

/// The health of every chunk of a file.
#[derive(Debug)]
pub struct AuditReport {
    /// One entry per chunk, in file order.
    pub chunks: Vec<ChunkAudit>,
}

impl ChunkAudit {
    /// How many of the chunk's pieces are healthy. The chunk can be read while
    /// at least as many are as it has data pieces.
    pub fn healthy(&self) -> usize {
        self.pieces
            .iter()
            .filter(|health| **health == PieceHealth::Healthy)
            .count()
    }
}

/// Audits every piece of the file `manifest` describes. Each node gets
/// `deadline` to answer; [`AUDIT_DEADLINE`] is the usual value.
///
/// Nodes are found at their addresses in `list`; a piece on a node it does
/// not have is [`PieceHealth::Unreachable`].
pub async fn audit(
    client: &NodeClient,
    list: &NodeList,
    manifest: &Manifest,
    deadline: Duration,
) -> AuditReport {
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for chunk in &manifest.chunks {
        let piece_len = chunk.layout.piece_len(chunk.sealed_len as usize);
        // Every piece of a chunk has the same length, so one draw of segments
        // serves all of them; each node still can't predict it.
        let segments = random_segments(merkle::segment_count(piece_len));
        let audits = chunk
            .pieces
            .iter()
            .map(|piece| audit_piece(client, list, piece, &segments, deadline));
        chunks.push(ChunkAudit {
            pieces: join_all(audits).await,
        });
    }
    AuditReport { chunks }
}

/// Challenges the node of `piece` for `segments` and judges its answer.
async fn audit_piece(
    client: &NodeClient,
    list: &NodeList,
    piece: &PlacedPiece,
    segments: &[u32],
    deadline: Duration,
) -> PieceHealth {
    let Some(url) = list.url_of(&piece.node) else {
        return PieceHealth::Unreachable;
    };
    let challenge = client.challenge(url, &piece.id, segments);
    let Ok(answer) = tokio::time::timeout(deadline, challenge).await else {
        return PieceHealth::Unreachable;
    };
    match answer {
        Ok(Some(proofs)) if proofs_hold(&proofs, segments, &piece.merkle_root) => {
            PieceHealth::Healthy
        }
        Ok(Some(_)) => PieceHealth::Failed,
        Ok(None) => PieceHealth::Missing,
        Err(NodeRequestError::Unreachable { .. }) => PieceHealth::Unreachable,
        Err(error) => {
            eprintln!("{error}");
            PieceHealth::Failed
        }
    }
}

/// Whether `proofs` answer exactly the `segments` asked for, in order, and all
/// lead to `root`. A proof for a segment that wasn't asked for proves nothing:
/// the node could keep that one segment and hand it out every time.
fn proofs_hold(proofs: &[SegmentProof], segments: &[u32], root: &MerkleHash) -> bool {
    proofs.len() == segments.len()
        && proofs
            .iter()
            .zip(segments)
            .all(|(proof, &asked)| proof.index == asked && proof.leads_to(root))
}

/// [`AUDIT_SEGMENTS`] different random segment numbers below `segment_count`,
/// or all of them when the piece has no more than that.
fn random_segments(segment_count: u32) -> Vec<u32> {
    if segment_count as usize <= AUDIT_SEGMENTS {
        return (0..segment_count).collect();
    }
    let mut segments = BTreeSet::new();
    while segments.len() < AUDIT_SEGMENTS {
        // The remainder slightly favours low numbers, by at most
        // segment_count / 2^32, far too little for a node to make use of.
        segments.insert(random_u32() % segment_count);
    }
    segments.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_pieces_are_audited_in_full() {
        assert_eq!(random_segments(3), vec![0, 1, 2]);
    }

    #[test]
    fn large_pieces_get_distinct_segments_in_range() {
        for _ in 0..100 {
            let segments = random_segments(65);
            assert_eq!(segments.len(), AUDIT_SEGMENTS);
            assert!(segments.iter().all(|&segment| segment < 65));
            assert!(segments.windows(2).all(|pair| pair[0] < pair[1]));
        }
    }

    #[test]
    fn proofs_must_answer_the_segments_asked_for() {
        let piece: Vec<u8> = (0..20_000u32).map(|value| value as u8).collect();
        let root = merkle::root_of(&piece);
        let proof_of = |index| merkle::prove(&piece, index).unwrap();

        assert!(proofs_hold(&[proof_of(1), proof_of(3)], &[1, 3], &root));
        assert!(!proofs_hold(&[proof_of(1), proof_of(1)], &[1, 3], &root));
        assert!(!proofs_hold(&[proof_of(1)], &[1, 3], &root));
    }
}

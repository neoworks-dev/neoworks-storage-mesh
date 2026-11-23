//! A Merkle tree over a piece's 4 KiB segments, for checking that a node
//! still holds a piece without downloading all of it.
//!
//! The piece id already proves a whole piece, but checking it takes the whole
//! piece. The Merkle root lets a client check single segments instead: the
//! node returns a segment and the hashes along its path to the root, and the
//! client recomputes the root. A node that no longer has the piece cannot
//! produce segments that lead to the right root.
//!
//! The tree is built like this:
//!
//! ```text
//! leaf   = BLAKE3(0x00 ‖ segment)
//! parent = BLAKE3(0x01 ‖ left ‖ right)     a level of odd length moves its
//!                                          last hash up unchanged
//! root   = BLAKE3(0x02 ‖ segment count as u32, big-endian ‖ top hash)
//! ```
//!
//! The leading bytes keep leaves, parents and roots from ever being mistaken
//! for one another. The root covers the segment count, so a proof cannot
//! claim a different shape of tree than the one the root was made from.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The bytes in each segment; the last segment of a piece may be shorter.
pub const SEGMENT_BYTES: usize = 4096;

/// The segments one of [`group_hashes`] covers: 16 segments are 64 KiB, so a
/// node keeping pieces elsewhere reads 64 KiB per segment it proves, and
/// keeps 32 bytes per 64 KiB of piece, 0.05%, to do so. A power of two, so
/// groups line up with the tree's pairs.
pub const GROUP_SEGMENTS: usize = 16;

const LEAF_PREFIX: u8 = 0x00;
const PARENT_PREFIX: u8 = 0x01;
const ROOT_PREFIX: u8 = 0x02;

/// A hash in the tree, written as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MerkleHash([u8; 32]);

/// One segment of a piece and the hashes that lead from it to the root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentProof {
    /// How many segments the piece has.
    pub segment_count: u32,
    /// Which segment this is, counting from 0.
    pub index: u32,
    /// The segment's bytes.
    #[serde(with = "hex")]
    pub segment: Vec<u8>,
    /// The sibling hashes from the leaf level up, skipping levels where the
    /// segment's hash moved up unchanged.
    pub siblings: Vec<MerkleHash>,
}

/// The number of segments in a piece of `piece_len` bytes. An empty piece
/// counts as one empty segment, so every piece has a tree.
pub fn segment_count(piece_len: usize) -> u32 {
    let count = piece_len.div_ceil(SEGMENT_BYTES).max(1);
    // Pieces are at most `MAX_PIECE_BYTES`, so they have at most 2304 segments.
    u32::try_from(count).expect("a piece has far fewer than 2^32 segments")
}

/// The Merkle root of `piece`.
pub fn root_of(piece: &[u8]) -> MerkleHash {
    let levels = levels_of(piece);
    let top = levels.last().and_then(|level| level.first());
    // `levels_of` always returns at least one level of at least one hash.
    let top = top.expect("a tree has a top hash");
    root_hash(segment_count(piece.len()), top)
}

/// The proof for segment `index` of `piece`, or `None` when the piece has no
/// such segment.
pub fn prove(piece: &[u8], index: u32) -> Option<SegmentProof> {
    let segment_count = segment_count(piece.len());
    if index >= segment_count {
        return None;
    }
    let mut siblings = Vec::new();
    let mut position = index as usize;
    for level in &levels_of(piece) {
        let sibling_position = position ^ 1;
        if let Some(sibling) = level.get(sibling_position) {
            siblings.push(*sibling);
        }
        position /= 2;
    }
    let start = index as usize * SEGMENT_BYTES;
    let end = (start + SEGMENT_BYTES).min(piece.len());
    Some(SegmentProof {
        segment_count,
        index,
        segment: piece[start..end].to_vec(),
        siblings,
    })
}

impl SegmentProof {
    /// Whether this proof leads to `root`, which proves the node holds this
    /// segment of the piece the root was made from.
    pub fn leads_to(&self, root: &MerkleHash) -> bool {
        if self.index >= self.segment_count || self.segment.len() > SEGMENT_BYTES {
            return false;
        }
        let mut hash = leaf_hash(&self.segment);
        let mut siblings = self.siblings.iter();
        let mut position = self.index as usize;
        let mut level_len = self.segment_count as usize;
        while level_len > 1 {
            let is_right_child = position % 2 == 1;
            let has_sibling = is_right_child || position + 1 < level_len;
            if has_sibling {
                let Some(sibling) = siblings.next() else {
                    return false;
                };
                hash = if is_right_child {
                    parent_hash(sibling, &hash)
                } else {
                    parent_hash(&hash, sibling)
                };
            }
            position /= 2;
            level_len = level_len.div_ceil(2);
        }
        // Left-over siblings mean the proof is for some other tree shape.
        siblings.next().is_none() && root_hash(self.segment_count, &hash) == *root
    }
}

/// The hashes of `piece`'s tree at the level where each covers one group of
/// [`GROUP_SEGMENTS`] segments, the last group perhaps fewer. A piece of one
/// group or less has one hash, its top hash.
///
/// A node that keeps these next to a piece it stores elsewhere can prove any
/// segment from the bytes of that segment's group alone
/// ([`prove_in_group`]), instead of reading the whole piece.
pub fn group_hashes(piece: &[u8]) -> Vec<MerkleHash> {
    let mut levels = levels_of(piece);
    let group_level = GROUP_SEGMENTS.trailing_zeros() as usize;
    if group_level < levels.len() {
        levels.swap_remove(group_level)
    } else {
        // `levels_of` always returns at least one level.
        levels.pop().unwrap_or_default()
    }
}

/// The proof for segment `index` of a piece of `segment_count` segments,
/// from the piece's [`group_hashes`] and the bytes of the segment's group:
/// [`GROUP_SEGMENTS`] segments from segment `index / GROUP_SEGMENTS` on,
/// fewer at the piece's end. `None` when there is no such segment or the
/// group is not as long as the piece's shape says.
///
/// The proof is the same one [`prove`] makes from the whole piece: pairs are
/// always formed within a group below the group level, so the group's own
/// tree gives the lower siblings and the group hashes the upper ones.
pub fn prove_in_group(
    group_hashes: &[MerkleHash],
    segment_count: u32,
    group: &[u8],
    index: u32,
) -> Option<SegmentProof> {
    if index >= segment_count {
        return None;
    }
    let group_number = index as usize / GROUP_SEGMENTS;
    let first_in_group = group_number * GROUP_SEGMENTS;
    let segments_in_group = (segment_count as usize - first_in_group).min(GROUP_SEGMENTS);
    if self::segment_count(group.len()) as usize != segments_in_group {
        return None;
    }
    let offset = index as usize - first_in_group;
    let mut siblings = siblings_in(&levels_of(group), offset);
    siblings.extend(siblings_in(
        &levels_above(group_hashes.to_vec()),
        group_number,
    ));
    let start = offset * SEGMENT_BYTES;
    let end = (start + SEGMENT_BYTES).min(group.len());
    Some(SegmentProof {
        segment_count,
        index,
        segment: group.get(start..end)?.to_vec(),
        siblings,
    })
}

/// The siblings of the hash at `position` on the lowest of `levels`, from
/// there up, skipping levels where it has none.
fn siblings_in(levels: &[Vec<MerkleHash>], mut position: usize) -> Vec<MerkleHash> {
    let mut siblings = Vec::new();
    for level in levels {
        if let Some(sibling) = level.get(position ^ 1) {
            siblings.push(*sibling);
        }
        position /= 2;
    }
    siblings
}

/// Every level of the tree of `piece`, leaves first, ending with the level of
/// one hash.
fn levels_of(piece: &[u8]) -> Vec<Vec<MerkleHash>> {
    let mut level: Vec<MerkleHash> = piece.chunks(SEGMENT_BYTES).map(leaf_hash).collect();
    if level.is_empty() {
        level.push(leaf_hash(&[]));
    }
    levels_above(level)
}

/// `level` and every level built on it, ending with the level of one hash.
fn levels_above(level: Vec<MerkleHash>) -> Vec<Vec<MerkleHash>> {
    let mut levels = vec![level];
    while levels.last().is_some_and(|level| level.len() > 1) {
        let below = levels.last().expect("checked by the loop condition");
        let above = below
            .chunks(2)
            .map(|pair| match pair {
                [left, right] => parent_hash(left, right),
                [only] => *only,
                _ => unreachable!("chunks(2) yields one or two hashes"),
            })
            .collect();
        levels.push(above);
    }
    levels
}

fn leaf_hash(segment: &[u8]) -> MerkleHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[LEAF_PREFIX]);
    hasher.update(segment);
    MerkleHash(*hasher.finalize().as_bytes())
}

fn parent_hash(left: &MerkleHash, right: &MerkleHash) -> MerkleHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[PARENT_PREFIX]);
    hasher.update(&left.0);
    hasher.update(&right.0);
    MerkleHash(*hasher.finalize().as_bytes())
}

fn root_hash(segment_count: u32, top: &MerkleHash) -> MerkleHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[ROOT_PREFIX]);
    hasher.update(&segment_count.to_be_bytes());
    hasher.update(&top.0);
    MerkleHash(*hasher.finalize().as_bytes())
}

impl MerkleHash {
    /// The hash with these 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        MerkleHash(bytes)
    }

    /// The 32 hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for MerkleHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for MerkleHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MerkleHash({self})")
    }
}

impl Serialize for MerkleHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MerkleHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(&text, &mut bytes).map_err(serde::de::Error::custom)?;
        Ok(MerkleHash(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A piece of `length` bytes that differ from position to position.
    fn piece(length: usize) -> Vec<u8> {
        (0..length).map(|position| (position % 251) as u8).collect()
    }

    #[test]
    fn every_segment_proves_for_trees_of_every_shape() {
        // One segment, a power of two, odd counts, and a short last segment.
        for length in [
            10,
            4096,
            8192,
            3 * 4096,
            5 * 4096 + 7,
            64 * 4096,
            257 * 1024,
        ] {
            let piece = piece(length);
            let root = root_of(&piece);
            for index in 0..segment_count(length) {
                let proof = prove(&piece, index).unwrap();
                assert!(proof.leads_to(&root), "length {length}, segment {index}");
            }
        }
    }

    #[test]
    fn a_changed_segment_does_not_prove() {
        let piece = piece(5 * 4096);
        let root = root_of(&piece);
        let mut proof = prove(&piece, 2).unwrap();

        proof.segment[100] ^= 1;

        assert!(!proof.leads_to(&root));
    }

    #[test]
    fn a_proof_does_not_prove_another_position_or_shape() {
        let piece = piece(5 * 4096);
        let root = root_of(&piece);
        let proof = prove(&piece, 2).unwrap();

        let moved = SegmentProof {
            index: 3,
            ..proof.clone()
        };
        let reshaped = SegmentProof {
            segment_count: 6,
            ..proof
        };

        assert!(!moved.leads_to(&root));
        assert!(!reshaped.leads_to(&root));
    }

    /// Checks that segment `index` of `piece` proves the same from its group
    /// as from the whole piece.
    fn assert_group_proof_matches(piece: &[u8], groups: &[MerkleHash], index: u32) {
        let count = segment_count(piece.len());
        let start = index as usize / GROUP_SEGMENTS * GROUP_SEGMENTS * SEGMENT_BYTES;
        let end = (start + GROUP_SEGMENTS * SEGMENT_BYTES).min(piece.len());
        let from_group = prove_in_group(groups, count, &piece[start..end], index);
        assert_eq!(
            from_group,
            prove(piece, index),
            "{count} segments, segment {index}"
        );
    }

    #[test]
    fn a_proof_from_a_group_is_the_proof_from_the_whole_piece() {
        // One group or less, exactly one, and odd numbers of groups on
        // several levels, each with a short last group.
        for segments in [1, 2, 9, 16, 17, 33, 47, 80, 200] {
            let piece = piece(segments * SEGMENT_BYTES - 5);
            let groups = group_hashes(&piece);
            for index in 0..segment_count(piece.len()) {
                assert_group_proof_matches(&piece, &groups, index);
            }
        }
    }

    #[test]
    fn a_full_piece_proves_from_its_groups() {
        // A 9 MiB piece of 144 groups. Proving every segment from the whole
        // piece builds its whole tree 2304 times, so only the first and last
        // groups' edges and one segment in between are checked.
        let piece = piece(2304 * SEGMENT_BYTES);
        let groups = group_hashes(&piece);
        for index in [0, 15, 16, 1_000, 2_287, 2_288, 2_303] {
            assert_group_proof_matches(&piece, &groups, index);
        }
    }

    #[test]
    fn a_group_of_the_wrong_length_proves_nothing() {
        let piece = piece(40 * SEGMENT_BYTES);
        let groups = group_hashes(&piece);

        let short = &piece[..15 * SEGMENT_BYTES];

        assert_eq!(prove_in_group(&groups, 40, short, 3), None);
        assert_eq!(
            prove_in_group(&groups, 40, &piece[..GROUP_SEGMENTS * SEGMENT_BYTES], 40),
            None
        );
    }

    #[test]
    fn there_is_no_proof_beyond_the_last_segment() {
        assert_eq!(prove(&piece(5 * 4096), 5), None);
    }
}

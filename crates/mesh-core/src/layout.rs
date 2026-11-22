//! How a chunk is cut: into how many data pieces, and how many parity pieces
//! are computed from them.
//!
//! Any `data` of a chunk's `data + parity` pieces rebuild it, so the chunk
//! survives the loss of `parity` pieces, and storing it takes
//! `(data + parity) / data` times its size. Each chunk records its layout,
//! so files stored with different layouts live side by side, and the
//! coordinator can repair a chunk from its pieces alone.

use serde::{Deserialize, Serialize};

/// The most pieces a chunk is cut into. Each piece costs a request on every
/// read and renewal, and few networks have more independent nodes per chunk.
pub const MAX_PIECES: usize = 32;

/// The fewest data pieces a chunk is cut into. With one, every piece is a
/// whole copy of the sealed chunk, which nodes accept since the largest
/// chunk sealed stays below their piece limit.
pub const MIN_DATA_PIECES: usize = 1;

/// A chunk's data and parity piece counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Layout {
    pub data: u8,
    pub parity: u8,
}

/// A layout outside what the mesh supports.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("a chunk has at least one data piece")]
    TooFewData,
    #[error("a chunk has at most {MAX_PIECES} pieces")]
    TooManyPieces,
}

impl Layout {
    /// The layout every chunk had before layouts were chosen per file: four
    /// data and two parity pieces, which costs 1.5 times the chunk and
    /// survives two lost nodes.
    pub const DEFAULT: Layout = Layout { data: 4, parity: 2 };

    /// The layout of `data` and `parity` pieces, if the mesh supports it.
    pub fn new(data: usize, parity: usize) -> Result<Layout, LayoutError> {
        if data < MIN_DATA_PIECES {
            return Err(LayoutError::TooFewData);
        }
        if data + parity > MAX_PIECES {
            return Err(LayoutError::TooManyPieces);
        }
        // Both are at most `MAX_PIECES`, which fits a byte.
        Ok(Layout {
            data: data as u8,
            parity: parity as u8,
        })
    }

    /// Data pieces: any this many pieces rebuild the chunk.
    pub const fn data(self) -> usize {
        self.data as usize
    }

    /// Parity pieces: how many pieces the chunk can lose.
    pub const fn parity(self) -> usize {
        self.parity as usize
    }

    /// All pieces of a chunk; each goes to a different node.
    pub const fn total(self) -> usize {
        self.data() + self.parity()
    }

    /// What storing a chunk costs, as a multiple of its size.
    pub fn overhead(self) -> f64 {
        self.total() as f64 / self.data() as f64
    }

    /// The length of each piece of a sealed chunk of `sealed_len` bytes: its
    /// share of the chunk, rounded up to an even number, since the
    /// Reed-Solomon implementation works on pairs of bytes.
    pub fn piece_len(self, sealed_len: usize) -> usize {
        let share = sealed_len.div_ceil(self.data()).max(1);
        share + share % 2
    }

    /// The most pieces of one chunk that may share an operator, a network or
    /// a region, so that losing any one of them leaves the chunk whole. A
    /// layout without parity can lose nothing anyway, and still needs one
    /// piece per node.
    pub const fn max_per_group(self) -> usize {
        if self.parity == 0 { 1 } else { self.parity() }
    }
}

impl Default for Layout {
    /// [`Layout::DEFAULT`], which chunks recorded without a layout have.
    fn default() -> Self {
        Layout::DEFAULT
    }
}

/// The chance of losing a chunk of `layout` within a year, when each node is
/// lost on any one day with a chance of `daily_loss` and lost pieces are
/// repaired within the day.
///
/// The chunk is lost when more than `parity` of its nodes are lost before
/// repair replaces them:
///
/// ```text
/// loss per year ≈ 365 × C(data + parity, parity + 1) × p^(parity + 1)
/// ```
///
/// This is the first term of the binomial sum, which dominates whenever `p`
/// is small; it is capped at 1.
pub fn loss_per_year(layout: Layout, daily_loss: f64) -> f64 {
    let needed = layout.parity() + 1;
    let per_day = choose(layout.total(), needed) * daily_loss.powi(needed as i32);
    (365.0 * per_day).min(1.0)
}

/// The binomial coefficient C(`n`, `k`), as a float: it overflows integers
/// for wide layouts long before it matters.
fn choose(n: usize, k: usize) -> f64 {
    if k > n {
        return 0.0;
    }
    (0..k).fold(1.0, |product, i| product * (n - i) as f64 / (i + 1) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_are_checked() {
        assert_eq!(Layout::new(4, 2), Ok(Layout::DEFAULT));
        assert!(Layout::new(1, 1).is_ok());
        assert_eq!(Layout::new(0, 2), Err(LayoutError::TooFewData));
        assert_eq!(Layout::new(16, 17), Err(LayoutError::TooManyPieces));
    }

    #[test]
    fn the_issues_table_holds() {
        // 1 in 140,000 and 1 in 180 million at one node in 1,000 a day.
        let narrow = loss_per_year(Layout::DEFAULT, 0.001);
        let wide = loss_per_year(Layout::new(16, 4).unwrap(), 0.001);

        assert!((1.0 / narrow - 137_000.0).abs() < 1_000.0, "{narrow}");
        assert!((1.0 / wide - 177_000_000.0).abs() < 2_000_000.0, "{wide}");
    }

    #[test]
    fn pieces_split_the_chunk_evenly() {
        assert_eq!(Layout::DEFAULT.piece_len(10_016), 2_504);
        assert_eq!(Layout::new(2, 1).unwrap().piece_len(5), 4);
        assert_eq!(Layout::DEFAULT.piece_len(0), 2);
    }

    #[test]
    fn a_layout_without_parity_still_places_one_piece_per_group() {
        assert_eq!(Layout::new(4, 0).unwrap().max_per_group(), 1);
        assert_eq!(Layout::new(4, 6).unwrap().max_per_group(), 6);
    }
}

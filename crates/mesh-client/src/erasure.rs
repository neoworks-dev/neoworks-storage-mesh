//! Cutting a sealed chunk into pieces, any `data` of which rebuild it.
//!
//! A sealed chunk is split into `layout.data` equal parts, and Reed-Solomon
//! coding computes `layout.parity` more from them
//! ([`Layout`](mesh_core::layout::Layout)). Any `data` of the pieces are
//! enough to get the chunk back, so up to `parity` nodes can lose theirs.
//!
//! The code is systematic: the first `data` pieces are the chunk itself, cut
//! in order. When those all arrive, rebuilding is just joining them.

use mesh_core::layout::Layout;

/// Data pieces in the default layout, 4 + 2.
pub const DATA_PIECES: usize = Layout::DEFAULT.data();
/// Parity pieces in the default layout.
pub const PARITY_PIECES: usize = Layout::DEFAULT.parity();
/// Pieces per chunk in the default layout; each goes to a different node.
pub const TOTAL_PIECES: usize = Layout::DEFAULT.total();

/// Too few pieces survived to rebuild a chunk, or they did not fit together.
#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("only {found} of the {needed} pieces needed arrived")]
    TooFewPieces { found: usize, needed: usize },
    #[error("the chunk has {pieces} pieces, but its layout has {expected}")]
    WrongPieceCount { pieces: usize, expected: usize },
    #[error("the pieces do not fit together: {0}")]
    Coding(#[from] reed_solomon_simd::Error),
}

/// Cuts `sealed` into the pieces of `layout`, all of equal length: the data
/// pieces first, then the parity pieces.
pub fn cut(sealed: &[u8], layout: Layout) -> Vec<Vec<u8>> {
    let piece_len = layout.piece_len(sealed.len());
    let mut padded = sealed.to_vec();
    // The chunk is padded with zeros to fill the data pieces exactly; the
    // manifest keeps the real length, so the padding is cut off again.
    padded.resize(piece_len * layout.data(), 0);
    let mut pieces: Vec<Vec<u8>> = padded.chunks(piece_len).map(<[u8]>::to_vec).collect();
    // A layout without parity is the data pieces alone; the encoder takes no
    // empty set of parity pieces.
    if layout.parity() == 0 {
        return pieces;
    }
    let parity = reed_solomon_simd::encode(layout.data(), layout.parity(), &pieces)
        // `piece_len` keeps every piece the same even, non-zero length, and a
        // `Layout` has at most 32 pieces: the only inputs encode refuses.
        .expect("pieces of equal even length in a supported shape");
    pieces.extend(parity);
    pieces
}

/// Rebuilds a sealed chunk of `sealed_len` bytes in `layout` from the pieces
/// that arrived.
///
/// `pieces[i]` is piece number `i`, or `None` when it could not be fetched.
pub fn rebuild(
    pieces: &[Option<Vec<u8>>],
    sealed_len: usize,
    layout: Layout,
) -> Result<Vec<u8>, RebuildError> {
    let found = pieces.iter().filter(|piece| piece.is_some()).count();
    let needed = layout.data();
    // A manifest whose layout doesn't match its pieces was changed by hand
    // or damaged; decoding it would index past the pieces.
    if pieces.len() != layout.total() {
        return Err(RebuildError::WrongPieceCount {
            pieces: pieces.len(),
            expected: layout.total(),
        });
    }
    if found < needed {
        return Err(RebuildError::TooFewPieces { found, needed });
    }
    let mut data_pieces: Vec<Option<Vec<u8>>> = pieces[..needed].to_vec();
    if data_pieces.iter().any(Option::is_none) {
        restore_missing_data_pieces(pieces, &mut data_pieces, layout)?;
    }
    let mut sealed: Vec<u8> = data_pieces.into_iter().flatten().flatten().collect();
    sealed.truncate(sealed_len);
    Ok(sealed)
}

/// Fills the gaps in `data_pieces` from the parity pieces.
fn restore_missing_data_pieces(
    pieces: &[Option<Vec<u8>>],
    data_pieces: &mut [Option<Vec<u8>>],
    layout: Layout,
) -> Result<(), RebuildError> {
    let present_data = pieces[..layout.data()]
        .iter()
        .enumerate()
        .filter_map(|(index, piece)| Some((index, piece.as_ref()?)));
    // The decoder numbers parity pieces from 0, not from the data count.
    let present_parity = pieces[layout.data()..]
        .iter()
        .enumerate()
        .filter_map(|(index, piece)| Some((index, piece.as_ref()?)));
    let restored =
        reed_solomon_simd::decode(layout.data(), layout.parity(), present_data, present_parity)?;
    for (index, piece) in restored {
        data_pieces[index] = Some(piece);
    }
    Ok(())
}

/// The length of each piece of a chunk of `sealed_len` bytes in the default
/// layout.
pub fn piece_len(sealed_len: usize) -> usize {
    Layout::DEFAULT.piece_len(sealed_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_chunk() -> Vec<u8> {
        (0..10_001u32)
            .map(|value| (value * 7 % 251) as u8)
            .collect()
    }

    fn all(pieces: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        pieces.iter().cloned().map(Some).collect()
    }

    #[test]
    fn rebuilds_from_all_pieces() {
        let chunk = sample_chunk();
        let pieces = all(&cut(&chunk, Layout::DEFAULT));

        assert_eq!(
            rebuild(&pieces, chunk.len(), Layout::DEFAULT).unwrap(),
            chunk
        );
    }

    #[test]
    fn rebuilds_with_any_two_pieces_missing() {
        let chunk = sample_chunk();
        let all_pieces = cut(&chunk, Layout::DEFAULT);
        for first in 0..TOTAL_PIECES {
            for second in first + 1..TOTAL_PIECES {
                let mut pieces = all(&all_pieces);
                pieces[first] = None;
                pieces[second] = None;

                let rebuilt = rebuild(&pieces, chunk.len(), Layout::DEFAULT);
                assert_eq!(rebuilt.unwrap(), chunk);
            }
        }
    }

    #[test]
    fn a_wide_layout_survives_as_many_losses_as_it_has_parity() {
        let chunk = sample_chunk();
        let layout = Layout::new(16, 4).unwrap();
        let mut pieces = all(&cut(&chunk, layout));
        assert_eq!(pieces.len(), 20);
        for lost in [0, 5, 15, 19] {
            pieces[lost] = None;
        }

        assert_eq!(rebuild(&pieces, chunk.len(), layout).unwrap(), chunk);
        pieces[1] = None;
        assert!(rebuild(&pieces, chunk.len(), layout).is_err());
    }

    #[test]
    fn a_layout_without_parity_is_the_data_alone() {
        let chunk = sample_chunk();
        let layout = Layout::new(4, 0).unwrap();
        let pieces = cut(&chunk, layout);

        assert_eq!(pieces.len(), 4);
        assert_eq!(rebuild(&all(&pieces), chunk.len(), layout).unwrap(), chunk);
    }

    #[test]
    fn a_rebuilt_chunk_cuts_into_the_same_pieces() {
        // Repair relies on this: a lost piece rebuilt from others is byte
        // for byte the piece that was lost, so its id stays valid.
        let chunk = sample_chunk();
        let layout = Layout::new(2, 3).unwrap();
        let original = cut(&chunk, layout);
        let mut survivors = all(&original);
        survivors[0] = None;
        survivors[4] = None;

        let rebuilt = rebuild(&survivors, chunk.len(), layout).unwrap();

        assert_eq!(cut(&rebuilt, layout), original);
    }

    #[test]
    fn fails_with_more_pieces_missing_than_parity() {
        let chunk = sample_chunk();
        let mut pieces = all(&cut(&chunk, Layout::DEFAULT));
        pieces[0] = None;
        pieces[3] = None;
        pieces[5] = None;

        let result = rebuild(&pieces, chunk.len(), Layout::DEFAULT);

        assert!(matches!(
            result,
            Err(RebuildError::TooFewPieces {
                found: 3,
                needed: 4
            })
        ));
    }
}

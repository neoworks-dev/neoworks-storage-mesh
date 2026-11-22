//! Cutting a sealed chunk into pieces, any four of which rebuild it.
//!
//! A sealed chunk is split into [`DATA_PIECES`] equal parts, and Reed-Solomon
//! coding computes [`PARITY_PIECES`] more from them. Any `DATA_PIECES` of the
//! [`TOTAL_PIECES`] pieces are enough to get the chunk back, so up to
//! `PARITY_PIECES` nodes can lose theirs. The pieces together take 1.5 times
//! the chunk's size.
//!
//! The code is systematic: the first `DATA_PIECES` pieces are the chunk itself,
//! cut in order. When those all arrive, rebuilding is just joining them.

/// Pieces holding the chunk itself.
pub const DATA_PIECES: usize = 4;
/// Pieces computed from the data pieces.
pub const PARITY_PIECES: usize = 2;
/// Pieces per chunk; each goes to a different node.
pub const TOTAL_PIECES: usize = DATA_PIECES + PARITY_PIECES;

/// Too few pieces survived to rebuild a chunk, or they did not fit together.
#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("only {found} of the {DATA_PIECES} pieces needed arrived")]
    TooFewPieces { found: usize },
    #[error("the pieces do not fit together: {0}")]
    Coding(#[from] reed_solomon_simd::Error),
}

/// Cuts `sealed` into [`TOTAL_PIECES`] pieces of equal length: the data pieces
/// first, then the parity pieces.
pub fn cut(sealed: &[u8]) -> Vec<Vec<u8>> {
    let piece_len = piece_len(sealed.len());
    let mut padded = sealed.to_vec();
    // The chunk is padded with zeros to fill the data pieces exactly; the
    // manifest keeps the real length, so the padding is cut off again.
    padded.resize(piece_len * DATA_PIECES, 0);
    let mut pieces: Vec<Vec<u8>> = padded.chunks(piece_len).map(<[u8]>::to_vec).collect();
    let parity = reed_solomon_simd::encode(DATA_PIECES, PARITY_PIECES, &pieces)
        // `piece_len` keeps every piece the same even, non-zero length, and
        // 4 + 2 pieces is a supported shape: the only inputs encode refuses.
        .expect("pieces of equal even length in a supported shape");
    pieces.extend(parity);
    pieces
}

/// Rebuilds a sealed chunk of `sealed_len` bytes from the pieces that arrived.
///
/// `pieces[i]` is piece number `i`, or `None` when it could not be fetched.
pub fn rebuild(pieces: &[Option<Vec<u8>>], sealed_len: usize) -> Result<Vec<u8>, RebuildError> {
    let found = pieces.iter().filter(|piece| piece.is_some()).count();
    if found < DATA_PIECES {
        return Err(RebuildError::TooFewPieces { found });
    }
    let mut data_pieces: Vec<Option<Vec<u8>>> = pieces[..DATA_PIECES].to_vec();
    if data_pieces.iter().any(Option::is_none) {
        restore_missing_data_pieces(pieces, &mut data_pieces)?;
    }
    let mut sealed: Vec<u8> = data_pieces.into_iter().flatten().flatten().collect();
    sealed.truncate(sealed_len);
    Ok(sealed)
}

/// Fills the gaps in `data_pieces` from the parity pieces.
fn restore_missing_data_pieces(
    pieces: &[Option<Vec<u8>>],
    data_pieces: &mut [Option<Vec<u8>>],
) -> Result<(), RebuildError> {
    let present_data = pieces[..DATA_PIECES]
        .iter()
        .enumerate()
        .filter_map(|(index, piece)| Some((index, piece.as_ref()?)));
    // The decoder numbers parity pieces from 0, not from DATA_PIECES.
    let present_parity = pieces[DATA_PIECES..]
        .iter()
        .enumerate()
        .filter_map(|(index, piece)| Some((index, piece.as_ref()?)));
    let restored =
        reed_solomon_simd::decode(DATA_PIECES, PARITY_PIECES, present_data, present_parity)?;
    for (index, piece) in restored {
        data_pieces[index] = Some(piece);
    }
    Ok(())
}

/// The length of each piece of a chunk of `sealed_len` bytes: a quarter of
/// it, rounded up to an even number, since the Reed-Solomon implementation
/// works on pairs of bytes.
pub fn piece_len(sealed_len: usize) -> usize {
    let quarter = sealed_len.div_ceil(DATA_PIECES).max(1);
    quarter + quarter % 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_chunk() -> Vec<u8> {
        (0..10_001u32)
            .map(|value| (value * 7 % 251) as u8)
            .collect()
    }

    #[test]
    fn rebuilds_from_all_pieces() {
        let chunk = sample_chunk();
        let pieces: Vec<Option<Vec<u8>>> = cut(&chunk).into_iter().map(Some).collect();

        assert_eq!(rebuild(&pieces, chunk.len()).unwrap(), chunk);
    }

    #[test]
    fn rebuilds_with_any_two_pieces_missing() {
        let chunk = sample_chunk();
        let all_pieces = cut(&chunk);
        for first in 0..TOTAL_PIECES {
            for second in first + 1..TOTAL_PIECES {
                let mut pieces: Vec<Option<Vec<u8>>> =
                    all_pieces.iter().cloned().map(Some).collect();
                pieces[first] = None;
                pieces[second] = None;

                assert_eq!(rebuild(&pieces, chunk.len()).unwrap(), chunk);
            }
        }
    }

    #[test]
    fn a_rebuilt_chunk_cuts_into_the_same_pieces() {
        // Repair relies on this: a lost piece rebuilt from four others is
        // byte for byte the piece that was lost, so its id stays valid.
        let chunk = sample_chunk();
        let original = cut(&chunk);
        let mut survivors: Vec<Option<Vec<u8>>> = original.iter().cloned().map(Some).collect();
        survivors[0] = None;
        survivors[5] = None;

        let rebuilt = rebuild(&survivors, chunk.len()).unwrap();

        assert_eq!(cut(&rebuilt), original);
    }

    #[test]
    fn fails_with_three_pieces_missing() {
        let chunk = sample_chunk();
        let mut pieces: Vec<Option<Vec<u8>>> = cut(&chunk).into_iter().map(Some).collect();
        pieces[0] = None;
        pieces[3] = None;
        pieces[5] = None;

        let result = rebuild(&pieces, chunk.len());

        assert!(matches!(
            result,
            Err(RebuildError::TooFewPieces { found: 3 })
        ));
    }
}

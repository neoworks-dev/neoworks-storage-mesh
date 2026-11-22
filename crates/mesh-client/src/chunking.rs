//! Cutting a file into chunks where its content says, and padding each chunk
//! to a size bucket.
//!
//! Chunk boundaries come from FastCDC, a rolling hash over the bytes, not from
//! fixed positions. Inserting a byte near the start of a file then changes
//! only the chunk around it; every later chunk keeps its bytes and so its key
//! and pieces, which is what lets a new version of a file reuse what the old
//! one stored.
//!
//! Every chunk is padded with zeros to the next power of two from 16 KiB up,
//! so a node learns a chunk's size only to within a factor of two. Smaller
//! buckets than the design doc's 256 KiB keep small files small; the padding
//! never more than doubles a chunk.

use fastcdc::v2020::AsyncStreamCDC;
use tokio::io::AsyncRead;

/// The smallest chunk FastCDC cuts, except for the last chunk of a file.
pub const MIN_CHUNK_BYTES: usize = 256 * 1024;
/// The chunk size FastCDC aims for.
pub const AVERAGE_CHUNK_BYTES: usize = 1024 * 1024;
/// The largest chunk FastCDC cuts.
pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The smallest size bucket. Each next bucket is twice as large, up to
/// [`MAX_CHUNK_BYTES`].
const SMALLEST_BUCKET_BYTES: usize = 16 * 1024;

/// A content-defined chunker over `reader`.
pub fn chunker<R: AsyncRead + Unpin>(reader: R) -> AsyncStreamCDC<R> {
    AsyncStreamCDC::new(
        reader,
        MIN_CHUNK_BYTES,
        AVERAGE_CHUNK_BYTES,
        MAX_CHUNK_BYTES,
    )
}

/// The size a chunk of `chunk_len` bytes is padded to: the smallest power of
/// two from 16 KiB that holds it.
pub fn bucket_for(chunk_len: usize) -> usize {
    chunk_len.max(SMALLEST_BUCKET_BYTES).next_power_of_two()
}

/// `chunk` padded with zeros to its bucket. The manifest keeps the chunk's real
/// length, so the padding is cut off again after opening.
pub fn pad(chunk: &[u8]) -> Vec<u8> {
    let mut padded = chunk.to_vec();
    padded.resize(bucket_for(chunk.len()), 0);
    padded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_powers_of_two_from_16_kib() {
        assert_eq!(bucket_for(0), 16 * 1024);
        assert_eq!(bucket_for(10_000), 16 * 1024);
        assert_eq!(bucket_for(16 * 1024 + 1), 32 * 1024);
        assert_eq!(bucket_for(1024 * 1024), 1024 * 1024);
        assert_eq!(bucket_for(MAX_CHUNK_BYTES), MAX_CHUNK_BYTES);
    }

    #[test]
    fn padding_keeps_the_chunk_and_fills_with_zeros() {
        let padded = pad(b"photo");

        assert_eq!(padded.len(), 16 * 1024);
        assert_eq!(&padded[..5], b"photo");
        assert!(padded[5..].iter().all(|&byte| byte == 0));
    }
}

//! Cutting a file into chunks.
//!
//! A file of up to [`WHOLE_FILE_MAX_BYTES`] is one chunk. Photos and
//! documents are mostly that small and are replaced rather than edited, so
//! cutting them finer would only add a round of requests to every read.
//!
//! Larger files are cut where their content says. Chunk boundaries come from
//! FastCDC, a rolling hash over the bytes, not from fixed positions. Inserting
//! a byte near the start of a file then changes only the chunk around it;
//! every later chunk keeps its bytes and so its key and pieces, which is what
//! lets a new version of a file reuse what the old one stored.
//!
//! Chunks are stored at their real length, so a node learns the size of each
//! chunk it holds a piece of. Padding chunks to size buckets would hide that,
//! but it made every file cost about half as much again on nodes.

use std::io::Cursor;

use fastcdc::v2020::AsyncStreamCDC;
use tokio::io::{AsyncRead, AsyncReadExt, Chain};

/// The largest file stored as a single chunk.
pub const WHOLE_FILE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// The smallest chunk FastCDC cuts, except for the last chunk of a file.
pub const MIN_CHUNK_BYTES: usize = 1024 * 1024;
/// The chunk size FastCDC aims for.
pub const AVERAGE_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// The largest chunk of any file, whole or cut.
pub const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;

// A whole file is one chunk, so it must not be larger than any other chunk.
const _: () = assert!(WHOLE_FILE_MAX_BYTES <= MAX_CHUNK_BYTES);

/// How a file starts, once as much of it was read as decides how it is cut.
pub enum FileStart {
    /// The whole file, which is small enough to be a single chunk.
    Whole(Vec<u8>),
    /// The first bytes of a file too large for one chunk, which the
    /// [`chunker`] cuts together with the rest.
    Larger(Vec<u8>),
}

/// Reads from `reader` until it is known whether the file fits one chunk.
pub async fn read_start<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<FileStart> {
    let mut start = Vec::new();
    // One byte past the limit tells a file of exactly the limit apart from
    // a larger one.
    let limit = WHOLE_FILE_MAX_BYTES as u64 + 1;
    reader.take(limit).read_to_end(&mut start).await?;
    if start.len() <= WHOLE_FILE_MAX_BYTES {
        return Ok(FileStart::Whole(start));
    }
    Ok(FileStart::Larger(start))
}

/// A content-defined chunker over a larger file: the `start` already read,
/// followed by the `rest` of `reader`.
pub fn chunker<R: AsyncRead + Unpin>(
    start: Vec<u8>,
    rest: R,
) -> AsyncStreamCDC<Chain<Cursor<Vec<u8>>, R>> {
    AsyncStreamCDC::new(
        Cursor::new(start).chain(rest),
        MIN_CHUNK_BYTES,
        AVERAGE_CHUNK_BYTES,
        MAX_CHUNK_BYTES,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_file_of_the_limit_is_whole_and_one_byte_more_is_not() {
        let at_limit = vec![7u8; WHOLE_FILE_MAX_BYTES];
        let over = vec![7u8; WHOLE_FILE_MAX_BYTES + 1];

        let whole = read_start(&mut at_limit.as_slice()).await.unwrap();
        let larger = read_start(&mut over.as_slice()).await.unwrap();

        assert!(matches!(whole, FileStart::Whole(bytes) if bytes == at_limit));
        assert!(matches!(larger, FileStart::Larger(bytes) if bytes == over));
    }
}

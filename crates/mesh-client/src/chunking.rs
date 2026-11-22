//! Cutting a file into chunks where its content says.
//!
//! Chunk boundaries come from FastCDC, a rolling hash over the bytes, not from
//! fixed positions. Inserting a byte near the start of a file then changes
//! only the chunk around it; every later chunk keeps its bytes and so its key
//! and pieces, which is what lets a new version of a file reuse what the old
//! one stored.
//!
//! Chunks are stored at their real length, so a node learns the size of each
//! chunk it holds a piece of. Padding chunks to size buckets would hide that,
//! but it made every file cost about half as much again on nodes.

use fastcdc::v2020::AsyncStreamCDC;
use tokio::io::AsyncRead;

/// The smallest chunk FastCDC cuts, except for the last chunk of a file.
pub const MIN_CHUNK_BYTES: usize = 256 * 1024;
/// The chunk size FastCDC aims for.
pub const AVERAGE_CHUNK_BYTES: usize = 1024 * 1024;
/// The largest chunk FastCDC cuts.
pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// A content-defined chunker over `reader`.
pub fn chunker<R: AsyncRead + Unpin>(reader: R) -> AsyncStreamCDC<R> {
    AsyncStreamCDC::new(
        reader,
        MIN_CHUNK_BYTES,
        AVERAGE_CHUNK_BYTES,
        MAX_CHUNK_BYTES,
    )
}

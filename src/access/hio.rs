//! Translated from PostgreSQL src/include/access/hio.h

use crate::access::htup::HeapTuple;
use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, BufferAccessStrategy};
use crate::utils::relcache::Relation;

/// State for bulk inserts (private to heapam.c and hio.c). If current_buf isn't
/// InvalidBuffer, we hold an extra pin on it.
pub struct BulkInsertStateData {
    pub strategy: BufferAccessStrategy, // BULKWRITE strategy object
    pub current_buf: Buffer,            // current insertion target page
    // State for bulk extensions: last_free..next_free are further unused pages
    // (rechecks needed). already_extended_by counts pages this insert extended.
    pub next_free: BlockNumber,
    pub last_free: BlockNumber,
    pub already_extended_by: u32,
}

pub fn RelationPutHeapTuple(
    _relation: Relation,
    _buffer: Buffer,
    _tuple: HeapTuple,
    _token: bool,
) {
    unimplemented!()
}

/// Returns the target buffer plus the (possibly updated) vm buffers (out-params).
pub fn RelationGetBufferForTuple(
    _relation: Relation,
    _len: usize,
    _otherBuffer: Buffer,
    _options: i32,
    _bistate: Option<&mut BulkInsertStateData>,
    _vmbuffer: &mut Buffer,
    _vmbuffer_other: &mut Buffer,
    _num_pages: i32,
) -> Buffer {
    unimplemented!()
}

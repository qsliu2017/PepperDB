//! Translated from PostgreSQL src/include/access/hio.h
//!
//! The bodies live in `crate::backend::access::heap::hio` and are re-exported
//! here. `relation_get_buffer_for_tuple` is `async` (rules.md s5: buffer/FSM I/O
//! leaves) and carries `&Arc<SharedState>`; `relation_put_heap_tuple` is sync (it
//! runs under the caller's exclusive content lock). The M2 forms drop the C
//! `otherBuffer`/`vmbuffer*`/`bistate`/`num_pages` out-params (update / VM / bulk
//! extension, M6/M8 scope). PG names: `RelationGetBufferForTuple`,
//! `RelationPutHeapTuple`.

use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, BufferAccessStrategy};

pub use crate::backend::access::heap::hio::{
    relation_get_buffer_for_tuple, relation_put_heap_tuple,
};

/// State for bulk inserts (private to heapam.c and hio.c). If current_buf isn't
/// InvalidBuffer, we hold an extra pin on it. (M5: bulk insert / COPY.)
pub struct BulkInsertStateData {
    pub strategy: BufferAccessStrategy, // BULKWRITE strategy object
    pub current_buf: Buffer,            // current insertion target page
    // State for bulk extensions: last_free..next_free are further unused pages
    // (rechecks needed). already_extended_by counts pages this insert extended.
    pub next_free: BlockNumber,
    pub last_free: BlockNumber,
    pub already_extended_by: u32,
}

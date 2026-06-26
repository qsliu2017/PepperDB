//! Translated from PostgreSQL src/include/storage/smgr.h
//!
//! Storage manager switch public interface declarations.
//!
//! STUB. smgr.c maintains a table of SMgrRelation objects, essentially cached
//! file handles. An SMgrRelation is created (if absent) by smgropen() and
//! destroyed by smgrclose()/smgrdestroy(); neither implies I/O - they just
//! create or destroy a hashtable entry. The actual reads/writes are deferred to
//! an async I/O backend later. The only built-in storage manager is magnetic
//! disk (md.c), so the C `f_smgr` vtable collapses to a single SMgrImpl variant.
//
// TODO(smgr): implement over async I/O backend later

use crate::common::relpath::ForkNumber;
use crate::common::relpath::MAX_FORKNUM;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::procnumber::ProcNumber;
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorBackend};

// C includes lib/ilist.h (dlist_node, for the unpinned list) and
// storage/aio_types.h (PgAioHandle/PgAioTargetInfo) - both elided here: pinning
// becomes Rust ownership, and async I/O handles are deferred to the I/O backend.

/// Number of forks (MAIN..INIT), i.e. MAX_FORKNUM + 1, used for per-fork arrays.
pub const NUM_FORKS: usize = (MAX_FORKNUM as usize) + 1;

/// Storage manager selector. Only magnetic disk (md.c) exists as a built-in, so
/// the C `f_smgr` method table is a single variant rather than a vtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SMgrImpl {
    /// Magnetic disk storage manager (md.c).
    MagneticDisk = 0,
}

/// An SMgrRelation: a cached file handle for one physical relation. PG's
/// `SMgrRelationData` value struct; the `SMgrRelation` pointer typedef becomes
/// `&SmgrRelation` / `&mut SmgrRelation` at call sites.
pub struct SmgrRelation {
    /// Relation physical identifier; the hashtable lookup key (must be first).
    pub rlocator: RelFileLocatorBackend,

    /// Current insertion target block. Reset to InvalidBlockNumber on a cache
    /// flush event.
    pub targblock: BlockNumber,

    /// Last known size for each fork; reset to InvalidBlockNumber on a cache
    /// flush event. Only reliable during recovery (no inval for fork extension).
    pub cached_nblocks: [BlockNumber; NUM_FORKS],

    // --- Fields below are private to smgr.c and its submodules. ---
    /// Storage manager selector.
    pub which: SMgrImpl,

    /// md.c per-fork open-segment bookkeeping (md_seg_fds / md_num_open_segs):
    /// the open segment files for each fork, lowest segno first. The dlist
    /// pinning link is dropped (Rust ownership replaces the pin GC).
    pub(crate) md_seg_fds: [Vec<crate::backend::storage::smgr::md::MdfdVec>; NUM_FORKS],
}

impl SmgrRelation {
    /// True iff this relation is backend-local (temporary).
    /// Replaces the `SmgrIsTemp` macro.
    pub fn is_temp(&self) -> bool {
        self.rlocator.is_temp()
    }
}


// ---------------------------------------------------------------------------
// Deprecated C-named free-function shims. The real logic is idiomatic methods
// on `SmgrRelation` (see `backend::storage::smgr::smgr`); these preserve the C
// names for cross-reference and mechanical ports. The smgr*v / smgr* I/O ops
// are async (over IoBackend) and take `&Arc<SharedState>` (the FdManager + sync
// queue) plus the caller-owned `&mut SmgrRelation`. New code should call the
// methods directly.
// ---------------------------------------------------------------------------

use std::sync::Arc;

use crate::shared_state::SharedState;
use crate::storage::bufpage::Page;

pub use crate::backend::storage::smgr::smgr::{
    at_eo_xact_smgr, smgr_cache_open, smgr_cache_put, smgr_is_cached, with_smgr_cache,
};

/// Initialize the storage manager subsystem. md has no per-task init under the
/// async model, so this is a no-op (kept for call-site parity).
pub fn smgrinit() {
    // md::mdinit was a MemoryContext setup -> nothing to do.
}

#[deprecated(note = "use `SmgrRelation::open` / `smgr_cache_open`")]
#[inline]
pub fn smgropen(rlocator: RelFileLocator, backend: ProcNumber) -> SmgrRelation {
    SmgrRelation::open(rlocator, backend)
}

#[deprecated(note = "use `reln.exists(shared, forknum).await`")]
#[inline]
pub async fn smgrexists(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) -> bool {
    reln.exists(shared, forknum).await
}

#[deprecated(note = "use `reln.close()`")]
#[inline]
pub fn smgrclose(reln: &mut SmgrRelation) {
    reln.close();
}

#[deprecated(note = "use `reln.release()`")]
#[inline]
pub fn smgrrelease(reln: &mut SmgrRelation) {
    reln.release();
}

#[deprecated(note = "use `reln.create(shared, forknum, is_redo).await`")]
#[inline]
pub async fn smgrcreate(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    is_redo: bool,
) {
    reln.create(shared, forknum, is_redo).await;
}

#[deprecated(note = "use `reln.extend(shared, forknum, blocknum, buffer, skip_fsync).await`")]
#[inline]
pub async fn smgrextend(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &Page,
    skip_fsync: bool,
) {
    reln.extend(shared, forknum, blocknum, buffer, skip_fsync).await;
}

#[deprecated(note = "use `reln.zeroextend(shared, forknum, blocknum, nblocks, skip_fsync).await`")]
#[inline]
pub async fn smgrzeroextend(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) {
    reln.zeroextend(shared, forknum, blocknum, nblocks, skip_fsync).await;
}

#[deprecated(note = "use `reln.prefetch(forknum, blocknum, nblocks)`")]
#[inline]
pub fn smgrprefetch(
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
) -> bool {
    reln.prefetch(forknum, blocknum, nblocks)
}

#[deprecated(note = "use `reln.maxcombine(forknum, blocknum)`")]
#[inline]
pub fn smgrmaxcombine(reln: &mut SmgrRelation, forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
    reln.maxcombine(forknum, blocknum)
}

#[deprecated(note = "use `reln.readv(shared, forknum, blocknum, buffers).await`")]
#[inline]
pub async fn smgrreadv(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut Page],
) {
    reln.readv(shared, forknum, blocknum, buffers).await;
}

#[deprecated(note = "use `reln.read(shared, forknum, blocknum, buffer).await`")]
#[inline]
pub async fn smgrread(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &mut Page,
) {
    reln.read(shared, forknum, blocknum, buffer).await;
}

#[deprecated(note = "use `reln.writev(shared, forknum, blocknum, buffers, skip_fsync).await`")]
#[inline]
pub async fn smgrwritev(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&Page],
    skip_fsync: bool,
) {
    reln.writev(shared, forknum, blocknum, buffers, skip_fsync).await;
}

#[deprecated(note = "use `reln.write(shared, forknum, blocknum, buffer, skip_fsync).await`")]
#[inline]
pub async fn smgrwrite(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &Page,
    skip_fsync: bool,
) {
    reln.write(shared, forknum, blocknum, buffer, skip_fsync).await;
}

#[deprecated(note = "use `reln.writeback(forknum, blocknum, nblocks)`")]
#[inline]
pub fn smgrwriteback(
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
) {
    reln.writeback(forknum, blocknum, nblocks);
}

#[deprecated(note = "use `reln.nblocks(shared, forknum).await`")]
#[inline]
pub async fn smgrnblocks(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) -> BlockNumber {
    reln.nblocks(shared, forknum).await
}

#[deprecated(note = "use `reln.nblocks_cached(forknum)`")]
#[inline]
pub fn smgrnblocks_cached(reln: &mut SmgrRelation, forknum: ForkNumber) -> Option<BlockNumber> {
    let _ = INVALID_BLOCK_NUMBER;
    reln.nblocks_cached(forknum)
}

#[deprecated(note = "use `reln.truncate(shared, &[(fork, old, new)]).await`")]
#[inline]
pub async fn smgrtruncate(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    truncate: &[(ForkNumber, BlockNumber, BlockNumber)],
) {
    reln.truncate(shared, truncate).await;
}

#[deprecated(note = "use `reln.immedsync(shared, forknum).await`")]
#[inline]
pub async fn smgrimmedsync(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) {
    reln.immedsync(shared, forknum).await;
}

#[deprecated(note = "use `reln.registersync(shared, forknum).await`")]
#[inline]
pub async fn smgrregistersync(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) {
    reln.registersync(shared, forknum).await;
}

/// smgrpin() / smgrunpin() -- the pin/unpin GC of the smgr handle cache is
/// tombstoned: handle lifetime is Rust ownership now (the relcache owns its
/// `SmgrRelation`). Kept as no-ops for call-site parity (relcache).
#[inline]
pub fn smgrpin(_reln: &mut SmgrRelation) {}

#[inline]
pub fn smgrunpin(_reln: &mut SmgrRelation) {}

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
const NUM_FORKS: usize = (MAX_FORKNUM as usize) + 1;

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
    pub smgr_rlocator: RelFileLocatorBackend,

    /// Current insertion target block. Reset to InvalidBlockNumber on a cache
    /// flush event.
    pub smgr_targblock: BlockNumber,

    /// Last known size for each fork; reset to InvalidBlockNumber on a cache
    /// flush event. Only reliable during recovery (no inval for fork extension).
    pub smgr_cached_nblocks: [BlockNumber; NUM_FORKS],

    // --- Fields below are private to smgr.c and its submodules. ---
    /// Storage manager selector.
    pub smgr_which: SMgrImpl,
    // md.c per-fork open-segment bookkeeping (md_num_open_segs / md_seg_fds) and
    // the dlist pinning link are dropped: deferred to the I/O backend impl.
}

impl SmgrRelation {
    /// True iff this relation is backend-local (temporary).
    /// Replaces the `SmgrIsTemp` macro.
    pub fn is_temp(&self) -> bool {
        self.smgr_rlocator.is_temp()
    }
}

/// Initialize the storage manager subsystem.
pub fn smgrinit() {
    unimplemented!()
}

/// Return an SMgrRelation handle for `rlocator`/`backend`, creating the cache
/// entry if necessary. No I/O.
pub fn smgropen(_rlocator: RelFileLocator, _backend: ProcNumber) -> SmgrRelation {
    unimplemented!()
}

/// Does the underlying file for `forknum` exist?
pub fn smgrexists(_reln: &SmgrRelation, _forknum: ForkNumber) -> bool {
    unimplemented!()
}

/// Pin an SMgrRelation so it is not destroyed while in use.
pub fn smgrpin(_reln: &mut SmgrRelation) {
    unimplemented!()
}

/// Unpin an SMgrRelation, allowing later destruction.
pub fn smgrunpin(_reln: &mut SmgrRelation) {
    unimplemented!()
}

/// Close an SMgrRelation, releasing the cache entry.
pub fn smgrclose(_reln: &mut SmgrRelation) {
    unimplemented!()
}

/// Destroy all unpinned SMgrRelations.
pub fn smgrdestroyall() {
    unimplemented!()
}

/// Release resources (e.g. OS file descriptors) for one SMgrRelation.
pub fn smgrrelease(_reln: &mut SmgrRelation) {
    unimplemented!()
}

/// Release resources for all SMgrRelations.
pub fn smgrreleaseall() {
    unimplemented!()
}

/// Release the SMgrRelation matching `rlocator`, if cached.
pub fn smgrreleaserellocator(_rlocator: RelFileLocatorBackend) {
    unimplemented!()
}

/// Create the underlying storage for `forknum`. `is_redo` is set during recovery.
pub fn smgrcreate(_reln: &mut SmgrRelation, _forknum: ForkNumber, _is_redo: bool) {
    unimplemented!()
}

/// fsync all forks of the given relations.
pub fn smgrdosyncall(_rels: &mut [SmgrRelation]) {
    unimplemented!()
}

/// Unlink all forks of the given relations. `is_redo` is set during recovery.
pub fn smgrdounlinkall(_rels: &mut [SmgrRelation], _is_redo: bool) {
    unimplemented!()
}

/// Append `buffer` (one block) at `blocknum`, extending the fork.
pub fn smgrextend(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: &[u8],
    _skip_fsync: bool,
) {
    unimplemented!()
}

/// Extend the fork by `nblocks` zero-filled blocks starting at `blocknum`.
pub fn smgrzeroextend(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: i32,
    _skip_fsync: bool,
) {
    unimplemented!()
}

/// Prefetch `nblocks` blocks starting at `blocknum`. Returns false if not
/// possible (e.g. posix_fadvise unsupported).
pub fn smgrprefetch(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: i32,
) -> bool {
    unimplemented!()
}

/// Max number of blocks that can be combined into one I/O starting at `blocknum`.
pub fn smgrmaxcombine(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
) -> u32 {
    unimplemented!()
}

/// Read `buffers.len()` blocks starting at `blocknum` into `buffers`
/// (vectored read). Each buffer is one block.
pub fn smgrreadv(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: &mut [&mut [u8]],
) {
    unimplemented!()
}

/// Read one block at `blocknum` into `buffer`. Inline wrapper over `smgrreadv`.
pub fn smgrread(
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &mut [u8],
) {
    smgrreadv(reln, forknum, blocknum, &mut [buffer]);
}

/// Write `buffers.len()` blocks starting at `blocknum` from `buffers`
/// (vectored write). Each buffer is one block.
pub fn smgrwritev(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: &[&[u8]],
    _skip_fsync: bool,
) {
    unimplemented!()
}

/// Write one block at `blocknum` from `buffer`. Inline wrapper over `smgrwritev`.
pub fn smgrwrite(
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) {
    smgrwritev(reln, forknum, blocknum, &[buffer], skip_fsync);
}

/// Hint the OS to write back `nblocks` blocks starting at `blocknum`.
pub fn smgrwriteback(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: BlockNumber,
) {
    unimplemented!()
}

/// Number of blocks in `forknum`.
pub fn smgrnblocks(_reln: &mut SmgrRelation, _forknum: ForkNumber) -> BlockNumber {
    unimplemented!()
}

/// Cached number of blocks in `forknum`, if known; None if the cache is stale
/// (was InvalidBlockNumber). C returned InvalidBlockNumber as the sentinel.
pub fn smgrnblocks_cached(_reln: &mut SmgrRelation, _forknum: ForkNumber) -> Option<BlockNumber> {
    let _ = INVALID_BLOCK_NUMBER;
    unimplemented!()
}

/// Truncate the listed forks to the given new sizes. C took parallel arrays of
/// (forknum, old_nblocks, nblocks); Rust takes one slice of triples.
pub fn smgrtruncate(_reln: &mut SmgrRelation, _truncate: &[(ForkNumber, BlockNumber, BlockNumber)]) {
    unimplemented!()
}

/// Immediately fsync `forknum`.
pub fn smgrimmedsync(_reln: &mut SmgrRelation, _forknum: ForkNumber) {
    unimplemented!()
}

/// Register a deferred fsync request for `forknum`.
pub fn smgrregistersync(_reln: &mut SmgrRelation, _forknum: ForkNumber) {
    unimplemented!()
}

/// End-of-transaction cleanup of the smgr cache.
pub fn at_eo_xact_smgr() {
    unimplemented!()
}

/// Process a "smgr release" barrier; returns true when handled.
pub fn process_barrier_smgr_release() -> bool {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/storage/bufmgr.h
//!
//! POSTGRES buffer manager definitions.
//!
//! STUB. The public buffer-manager API: pin/unpin, content locks, dirty marks,
//! read/extend. Under the single-process async model the pin/lock primitives map
//! onto Rust ownership + async-aware locks, and reads/extends route through the
//! async I/O backend. The private descriptor layer (buf_internals.h) is deferred.
// TODO(buffer-manager): implement in later pass

use crate::common::relpath::ForkNumber;
use crate::common::relpath::ForkNumber::MAIN_FORKNUM;
use crate::postgres_ext::Oid;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::{buffer_is_local, Buffer, INVALID_BUFFER};
use crate::storage::bufpage::Page;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;
use crate::utils::relcache::Relation;

#[allow(deprecated)]
use crate::storage::buf::BufferAccessStrategy;

use crate::access::xlogdefs::XLogRecPtr;

use bitflags::bitflags;

/// C: `typedef void *Block`. A reference to a disk page image; modeled as a page
/// byte slice rather than a raw pointer.
pub type Block<'a> = Page<'a>;

/// Possible arguments for GetAccessStrategy(). Sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum BufferAccessStrategyType {
    /// Normal random access.
    BAS_NORMAL = 0,
    /// Large read-only scan (hint bit updates are ok).
    BAS_BULKREAD,
    /// Large multi-block write (e.g. COPY IN).
    BAS_BULKWRITE,
    /// VACUUM.
    BAS_VACUUM,
}

/// Possible modes for ReadBufferExtended(). Sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ReadBufferMode {
    /// Normal read.
    RBM_NORMAL = 0,
    /// Don't read from disk, caller will initialize. Also locks the page.
    RBM_ZERO_AND_LOCK,
    /// Like RBM_ZERO_AND_LOCK, but locks the page in "cleanup" mode.
    RBM_ZERO_AND_CLEANUP_LOCK,
    /// Read, but return an all-zeros page on error.
    RBM_ZERO_ON_ERROR,
    /// Don't log page as invalid during WAL replay; otherwise like RBM_NORMAL.
    RBM_NORMAL_NO_LOG,
}

/// Type returned by PrefetchBuffer().
pub struct PrefetchBufferResult {
    /// If valid, a hit (recheck needed!).
    pub recent_buffer: Buffer,
    /// If true, a miss resulting in async I/O.
    pub initiated_io: bool,
}

bitflags! {
    /// Flags influencing the behaviour of ExtendBufferedRel*.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ExtendBufferedFlags: u32 {
        /// Don't acquire extension lock (safe only for unshared rel, AEL held,
        /// or startup process).
        const EB_SKIP_EXTENSION_LOCK = 1 << 0;
        /// Is this extension part of recovery?
        const EB_PERFORMING_RECOVERY = 1 << 1;
        /// Create the fork if it does not currently exist?
        const EB_CREATE_FORK_IF_NEEDED = 1 << 2;
        /// Return the first (possibly only) return buffer locked?
        const EB_LOCK_FIRST = 1 << 3;
        /// Clear the smgr size cache?
        const EB_CLEAR_SIZE_CACHE = 1 << 4;
        /// Internal flag.
        const EB_LOCK_TARGET = 1 << 5;
    }
}

/// Identifies a relation either by relcache entry or by smgr + relpersistence;
/// constructed via `bmr_rel()` / `bmr_smgr()`. Used so one function serves both
/// recovery and normal operation.
pub struct BufferManagerRelation<'a> {
    pub rel: Option<Relation>,
    pub smgr: Option<&'a mut SmgrRelation>,
    pub relpersistence: u8,
}

/// C macro `BMR_REL(p_rel)`.
pub fn bmr_rel(rel: Relation) -> BufferManagerRelation<'static> {
    BufferManagerRelation { rel: Some(rel), smgr: None, relpersistence: 0 }
}

/// C macro `BMR_SMGR(p_smgr, p_relpersistence)`.
pub fn bmr_smgr(smgr: &mut SmgrRelation, relpersistence: u8) -> BufferManagerRelation<'_> {
    BufferManagerRelation { rel: None, smgr: Some(smgr), relpersistence }
}

bitflags! {
    /// Flags for StartReadBuffer(s)().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReadBuffersFlags: i32 {
        /// Zero out page if reading fails.
        const READ_BUFFERS_ZERO_ON_ERROR = 1 << 0;
        /// Call smgrprefetch() if I/O necessary.
        const READ_BUFFERS_ISSUE_ADVICE = 1 << 1;
        /// Don't treat page as invalid due to checksum failures.
        const READ_BUFFERS_IGNORE_CHECKSUM_FAILURES = 1 << 2;
        /// IO will immediately be waited for.
        const READ_BUFFERS_SYNCHRONOUSLY = 1 << 3;
    }
}

/// In-progress read operation, threaded between StartReadBuffers() and
/// WaitReadBuffers(). The aio handle/return fields are deferred to the async I/O
/// backend; only the caller-set members are modeled here.
pub struct ReadBuffersOperation<'a> {
    pub rel: Option<Relation>,
    pub smgr: Option<&'a mut SmgrRelation>,
    pub persistence: u8,
    pub forknum: ForkNumber,
    pub strategy: Option<BufferAccessStrategy>,
    // Private state (blocknum/flags/nblocks/aio handle) deferred to the I/O backend.
}

// in globals.c
pub static mut NBuffers: i32 = 1000;

// in bufmgr.c (GUCs)
// TODO(global): GUCs become session/global config under the async model.
pub static mut zero_damaged_pages: bool = false;
pub static mut bgwriter_lru_maxpages: i32 = 100;
pub static mut bgwriter_lru_multiplier: f64 = 2.0;
pub static mut track_io_timing: bool = false;

pub const DEFAULT_EFFECTIVE_IO_CONCURRENCY: i32 = 16;
pub const DEFAULT_MAINTENANCE_IO_CONCURRENCY: i32 = 16;
pub static mut effective_io_concurrency: i32 = DEFAULT_EFFECTIVE_IO_CONCURRENCY;
pub static mut maintenance_io_concurrency: i32 = DEFAULT_MAINTENANCE_IO_CONCURRENCY;

pub static mut io_combine_limit: i32 = 0;
pub static mut io_combine_limit_guc: i32 = 0;
pub static mut io_max_combine_limit: i32 = 0;

pub static mut checkpoint_flush_after: i32 = 0;
pub static mut backend_flush_after: i32 = 0;
pub static mut bgwriter_flush_after: i32 = 0;

// in buf_init.c
// BufferBlocks (the shared buffer pool base) becomes owned heap state later.

// in localbuf.c
pub static mut NLocBuffer: i32 = 0;

/// Upper limit for effective_io_concurrency.
pub const MAX_IO_CONCURRENCY: i32 = 1000;

/// Special block number for ReadBuffer(): grow the file to get a new page.
pub const P_NEW: BlockNumber = INVALID_BLOCK_NUMBER;

// Buffer content lock modes (mode argument for LockBuffer()).
pub const BUFFER_LOCK_UNLOCK: i32 = 0;
pub const BUFFER_LOCK_SHARE: i32 = 1;
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

/// Re-export so callers can name the invalid buffer sentinel here.
pub const InvalidBuffer: Buffer = INVALID_BUFFER;

// === prototypes for functions in bufmgr.c (stubs) ===

pub fn PrefetchSharedBuffer(
    _smgr_reln: &mut SmgrRelation,
    _fork_num: ForkNumber,
    _block_num: BlockNumber,
) -> PrefetchBufferResult {
    unimplemented!()
}

pub fn PrefetchBuffer(
    _reln: Relation,
    _fork_num: ForkNumber,
    _block_num: BlockNumber,
) -> PrefetchBufferResult {
    unimplemented!()
}

/// Returns the recent buffer if it still holds the block, else None.
pub fn ReadRecentBuffer(
    _rlocator: RelFileLocator,
    _fork_num: ForkNumber,
    _block_num: BlockNumber,
    _recent_buffer: Buffer,
) -> Option<Buffer> {
    unimplemented!()
}

pub fn ReadBuffer(_reln: Relation, _block_num: BlockNumber) -> Buffer {
    unimplemented!()
}

pub fn ReadBufferExtended(
    _reln: Relation,
    _fork_num: ForkNumber,
    _block_num: BlockNumber,
    _mode: ReadBufferMode,
    _strategy: Option<BufferAccessStrategy>,
) -> Buffer {
    unimplemented!()
}

pub fn ReadBufferWithoutRelcache(
    _rlocator: RelFileLocator,
    _fork_num: ForkNumber,
    _block_num: BlockNumber,
    _mode: ReadBufferMode,
    _strategy: Option<BufferAccessStrategy>,
    _permanent: bool,
) -> Buffer {
    unimplemented!()
}

/// Returns true iff a read must be performed (the buffer was a miss).
pub fn StartReadBuffer(
    _operation: &mut ReadBuffersOperation,
    _buffer: &mut Buffer,
    _blocknum: BlockNumber,
    _flags: ReadBuffersFlags,
) -> bool {
    unimplemented!()
}

pub fn StartReadBuffers(
    _operation: &mut ReadBuffersOperation,
    _buffers: &mut [Buffer],
    _block_num: BlockNumber,
    _nblocks: &mut i32,
    _flags: ReadBuffersFlags,
) -> bool {
    unimplemented!()
}

pub fn WaitReadBuffers(_operation: &mut ReadBuffersOperation) {
    unimplemented!()
}

pub fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!()
}

pub fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!()
}

pub fn BufferIsExclusiveLocked(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn BufferIsDirty(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!()
}

pub fn IncrBufferRefCount(_buffer: Buffer) {
    unimplemented!()
}

pub fn CheckBufferIsPinnedOnce(_buffer: Buffer) {
    unimplemented!()
}

pub fn ReleaseAndReadBuffer(
    _buffer: Buffer,
    _relation: Relation,
    _block_num: BlockNumber,
) -> Buffer {
    unimplemented!()
}

pub fn ExtendBufferedRel(
    _bmr: BufferManagerRelation,
    _fork_num: ForkNumber,
    _strategy: Option<BufferAccessStrategy>,
    _flags: ExtendBufferedFlags,
) -> Buffer {
    unimplemented!()
}

/// Returns the first new block number (out-params `buffers`/`extended_by` filled).
pub fn ExtendBufferedRelBy(
    _bmr: BufferManagerRelation,
    _fork: ForkNumber,
    _strategy: Option<BufferAccessStrategy>,
    _flags: ExtendBufferedFlags,
    _extend_by: u32,
    _buffers: &mut [Buffer],
    _extended_by: &mut u32,
) -> BlockNumber {
    unimplemented!()
}

pub fn ExtendBufferedRelTo(
    _bmr: BufferManagerRelation,
    _fork: ForkNumber,
    _strategy: Option<BufferAccessStrategy>,
    _flags: ExtendBufferedFlags,
    _extend_to: BlockNumber,
    _mode: ReadBufferMode,
) -> Buffer {
    unimplemented!()
}

pub fn InitBufferManagerAccess() {
    unimplemented!()
}

pub fn AtEOXact_Buffers(_is_commit: bool) {
    unimplemented!()
}

pub fn AssertBufferLocksPermitCatalogRead() {
    unimplemented!()
}

pub fn DebugPrintBufferRefcount(_buffer: Buffer) -> String {
    unimplemented!()
}

pub fn CheckPointBuffers(_flags: i32) {
    unimplemented!()
}

pub fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!()
}

pub fn RelationGetNumberOfBlocksInFork(
    _relation: Relation,
    _fork_num: ForkNumber,
) -> BlockNumber {
    unimplemented!()
}

pub fn FlushOneBuffer(_buffer: Buffer) {
    unimplemented!()
}

pub fn FlushRelationBuffers(_rel: Relation) {
    unimplemented!()
}

pub fn FlushRelationsAllBuffers(_smgrs: &mut [&mut SmgrRelation]) {
    unimplemented!()
}

pub fn CreateAndCopyRelationData(
    _src_rlocator: RelFileLocator,
    _dst_rlocator: RelFileLocator,
    _permanent: bool,
) {
    unimplemented!()
}

pub fn FlushDatabaseBuffers(_dbid: Oid) {
    unimplemented!()
}

pub fn DropRelationBuffers(
    _smgr_reln: &mut SmgrRelation,
    _fork_num: &[ForkNumber],
    _first_del_block: &[BlockNumber],
) {
    unimplemented!()
}

pub fn DropRelationsAllBuffers(_smgr_reln: &mut [&mut SmgrRelation]) {
    unimplemented!()
}

pub fn DropDatabaseBuffers(_dbid: Oid) {
    unimplemented!()
}

/// C macro `RelationGetNumberOfBlocks(reln)`.
pub fn RelationGetNumberOfBlocks(reln: Relation) -> BlockNumber {
    RelationGetNumberOfBlocksInFork(reln, MAIN_FORKNUM)
}

pub fn BufferIsPermanent(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn BufferGetLSNAtomic(_buffer: Buffer) -> XLogRecPtr {
    unimplemented!()
}

/// Returns (rlocator, forknum, blknum) (C out-params).
pub fn BufferGetTag(_buffer: Buffer) -> (RelFileLocator, ForkNumber, BlockNumber) {
    unimplemented!()
}

pub fn MarkBufferDirtyHint(_buffer: Buffer, _buffer_std: bool) {
    unimplemented!()
}

pub fn UnlockBuffers() {
    unimplemented!()
}

pub fn LockBuffer(_buffer: Buffer, _mode: i32) {
    unimplemented!()
}

pub fn ConditionalLockBuffer(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn LockBufferForCleanup(_buffer: Buffer) {
    unimplemented!()
}

pub fn ConditionalLockBufferForCleanup(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn IsBufferCleanupOK(_buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn HoldingBufferPinThatDelaysRecovery() -> bool {
    unimplemented!()
}

/// `wb_context` is a buf_internals.h type (deferred); dropped from the signature.
pub fn BgBufferSync() -> bool {
    unimplemented!()
}

pub fn GetPinLimit() -> u32 {
    unimplemented!()
}

pub fn GetLocalPinLimit() -> u32 {
    unimplemented!()
}

pub fn GetAdditionalPinLimit() -> u32 {
    unimplemented!()
}

pub fn GetAdditionalLocalPinLimit() -> u32 {
    unimplemented!()
}

pub fn LimitAdditionalPins(_additional_pins: &mut u32) {
    unimplemented!()
}

pub fn LimitAdditionalLocalPins(_additional_pins: &mut u32) {
    unimplemented!()
}

/// Returns whether the buffer was flushed (C `*buffer_flushed` out-param folded in).
pub fn EvictUnpinnedBuffer(_buf: Buffer) -> (bool, bool) {
    unimplemented!()
}

/// Returns (buffers_evicted, buffers_flushed, buffers_skipped).
pub fn EvictAllUnpinnedBuffers() -> (i32, i32, i32) {
    unimplemented!()
}

/// Returns (buffers_evicted, buffers_flushed, buffers_skipped).
pub fn EvictRelUnpinnedBuffers(_rel: Relation) -> (i32, i32, i32) {
    unimplemented!()
}

// in buf_init.c -- shmem init collapses under single-process; kept as stubs.
pub fn BufferManagerShmemInit() {
    unimplemented!()
}

pub fn BufferManagerShmemSize() -> usize {
    unimplemented!()
}

// in localbuf.c
pub fn AtProcExit_LocalBuffers() {
    unimplemented!()
}

// in freelist.c
pub fn GetAccessStrategy(_btype: BufferAccessStrategyType) -> Option<BufferAccessStrategy> {
    unimplemented!()
}

pub fn GetAccessStrategyWithSize(
    _btype: BufferAccessStrategyType,
    _ring_size_kb: i32,
) -> Option<BufferAccessStrategy> {
    unimplemented!()
}

pub fn GetAccessStrategyBufferCount(_strategy: &BufferAccessStrategy) -> i32 {
    unimplemented!()
}

pub fn GetAccessStrategyPinLimit(_strategy: &BufferAccessStrategy) -> i32 {
    unimplemented!()
}

pub fn FreeAccessStrategy(_strategy: BufferAccessStrategy) {
    unimplemented!()
}

// === inline functions (translated in full) ===

/// True iff the given buffer number is valid (shared or local).
pub fn BufferIsValid(bufnum: Buffer) -> bool {
    debug_assert!(bufnum <= unsafe { NBuffers });
    debug_assert!(bufnum >= -(unsafe { NLocBuffer }));
    bufnum != INVALID_BUFFER
}

/// Returns a reference to the disk page image associated with a buffer.
/// Assumes buffer is valid. Backing storage (shared pool / local buffers) is
/// deferred to the buffer-manager impl, so the access itself is stubbed.
pub fn BufferGetBlock<'a>(buffer: Buffer) -> Block<'a> {
    debug_assert!(BufferIsValid(buffer));
    let _ = buffer_is_local(buffer);
    unimplemented!()
}

/// Returns the page size within a buffer (always BLCKSZ).
pub fn BufferGetPageSize(buffer: Buffer) -> usize {
    debug_assert!(BufferIsValid(buffer));
    crate::pg_config::BLCKSZ as usize
}

/// Returns the page associated with a buffer.
pub fn BufferGetPage<'a>(buffer: Buffer) -> Page<'a> {
    BufferGetBlock(buffer)
}

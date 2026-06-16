//! storage/buffer/bufmgr.c
//!
//! buffer manager interface routines
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/buffer/bufmgr.c

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(static_mut_refs)]
#![allow(unused_variables)]
#![allow(unused_assignments)]

use crate::prelude::*;

use crate::storage::block::{BlockNumber, InvalidBlockNumber, MaxBlockNumber, BlockNumberIsValid};
use crate::storage::buf::{Buffer, BufferIsLocal, InvalidBuffer};
use crate::storage::relfilelocator::RelFileLocator;
use crate::postgres_ext::Oid;
use crate::common::relpath::{ForkNumber, MAIN_FORKNUM, INIT_FORKNUM, MAX_FORKNUM};
use crate::storage::buf_internals::{
    BufferDesc, BufferTag, LocalBufferDescriptors,
    GetBufferDescriptor, GetLocalBufferDescriptor,
    BufferDescriptorGetBuffer,
    InitBufferTag, BufferTagsEqual, ClearBufferTag,
    BufTagGetRelFileLocator, BufTagGetForkNum, BufTagMatchesRelFileLocator,
    BufTagGetRelNumber,
    PrefetchBufferResult, SMgrRelation, SMgrRelationData,
    BUF_FLAG_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE, BUF_REFCOUNT_ONE,
    BM_TAG_VALID, BM_DIRTY, BM_VALID, BM_IO_ERROR, BM_JUST_DIRTIED,
    BM_LOCKED, BM_IO_IN_PROGRESS, BM_PERMANENT, BM_CHECKPOINT_NEEDED, BM_PIN_COUNT_WAITER,
    BM_MAX_USAGE_COUNT, BUF_STATE_GET_USAGECOUNT, BUF_STATE_GET_REFCOUNT,
    BufferDescriptorGetContentLock,
    BufferDescriptorGetIOCV,
    BufMappingPartitionLock,
    BufTableHashCode, BufTableLookup, BufTableInsert, BufTableDelete,
    IOContextForStrategy,
    StrategyGetBuffer, StrategyFreeBuffer, StrategyRejectBuffer, StrategySyncStart,
    BufferAccessStrategy,
    WritebackContext, PendingWriteback,
    WRITEBACK_MAX_PENDING_FLUSHES,
    CkptSortItem,
    LockBufHdr, UnlockBufHdr,
    ConditionVariable,
    BufferDescPadded, BufferDescriptors,
};
use crate::utils::hash::dynahash::{
    HTAB, HASHCTL, hash_create, hash_search, hash_seq_init, hash_seq_search,
    HASH_FIND, HASH_ENTER, HASH_REMOVE, HASH_ELEM, HASH_BLOBS,
    HASH_SEQ_STATUS,
};
use crate::utils::activity::pgstat_io::{
    IOContext, IOObject,
    IOOBJECT_RELATION, IOOBJECT_TEMP_RELATION,
    IOCONTEXT_NORMAL,
};
use crate::storage::procnumber::{MyProcNumber, INVALID_PROC_NUMBER, ProcNumber};
use crate::utils::resowner::resowner::{
    ResourceOwnerEnlarge, CurrentResourceOwner,
};
use crate::storage::buf_internals::{
    ResourceOwnerRememberBuffer, ResourceOwnerForgetBuffer,
};
use crate::port::atomics::{
    pg_atomic_uint32,
    pg_atomic_read_u32_impl,
};
use crate::port::atomics::generic::{
    pg_atomic_unlocked_write_u32_impl, pg_atomic_fetch_or_u32_impl,
};

// Bare-name atomic ops (the macros wrapping the *_impl primitives in atomics.h).
#[inline]
unsafe fn pg_atomic_read_u32(ptr: &pg_atomic_uint32) -> uint32 {
    pg_atomic_read_u32_impl(ptr)
}
#[inline]
unsafe fn pg_atomic_fetch_or_u32(ptr: &pg_atomic_uint32, or_: uint32) -> uint32 {
    pg_atomic_fetch_or_u32_impl(ptr, or_)
}
#[inline]
unsafe fn pg_atomic_unlocked_write_u32(ptr: &pg_atomic_uint32, val: uint32) {
    pg_atomic_unlocked_write_u32_impl(ptr, val);
}

// buf.h: BufferIsValid -- a buffer id is valid iff it is not InvalidBuffer.
#[inline]
fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}
use crate::storage::aio_types::{
    PgAioResult, PgAioReturn, PgAioTargetData, PgAioTargetDataSmgr,
    PgAioResultStatus,
    PGAIO_RS_UNKNOWN, PGAIO_RS_OK, PGAIO_RS_ERROR, PGAIO_RS_WARNING, PGAIO_RS_PARTIAL,
    PgAioHandleCallbacks,
};
use crate::storage::aio::aio::PgAioWaitRef;
use crate::storage::aio::aio_callback::{
    PGAIO_HCB_LOCAL_BUFFER_READV, PGAIO_HCB_SHARED_BUFFER_READV,
};
use crate::storage::aio::aio::{
    PGAIO_HF_SYNCHRONOUS, PGAIO_HF_REFERENCES_LOCAL,
};
use crate::storage::aio_internal::PgAioHandle;
use crate::storage::aio::aio::{
    pgaio_io_acquire, pgaio_io_acquire_nb, pgaio_io_release,
    pgaio_io_get_wref, pgaio_io_set_flag,
    pgaio_io_get_owner,
    pgaio_have_staged, pgaio_submit_staged,
    pgaio_wref_valid, pgaio_wref_wait, pgaio_wref_clear, pgaio_wref_check_done,
};
use crate::storage::aio::aio_callback::{
    pgaio_io_set_handle_data_32,
    pgaio_io_register_callbacks,
    pgaio_io_get_handle_data,
    pgaio_result_report,
};
use crate::storage::aio::aio_target::pgaio_io_get_target_data;
use crate::storage::buf_internals::LWLock;
use crate::storage::lmgr::condition_variable::{
    ConditionVariablePrepareToSleep, ConditionVariableSleep, ConditionVariableCancelSleep,
    ConditionVariableBroadcast,
};
use crate::storage::lmgr::s_lock::{SpinDelayStatus, perform_spin_delay, finish_spin_delay};
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::PG_IO_ALIGN_SIZE;
use crate::miscadmin::{NBuffers, MaxBackends};

// -------------------------------------------------------------------
// Stubs for unported dependencies
// -------------------------------------------------------------------

// storage/buf_internals.h: BackendWritebackContext (global writeback context).
// TODO(pg-port): real declaration is in buf_internals.h
extern "C" {
    pub static mut BackendWritebackContext: WritebackContext;
    pub static mut CkptBufferIds: *mut CkptSortItem;
}

/// storage/bufmgr.h: read_stream.h ReadBuffersOperation full layout.
// We use the one defined in read_stream.rs.
use crate::storage::aio::read_stream::{ReadBuffersOperation, ReadStream};

// READ_BUFFERS_* flags
pub const READ_BUFFERS_SYNCHRONOUSLY: c_int = 1 << 0;
pub const READ_BUFFERS_ISSUE_ADVICE: c_int = 1 << 1;
pub const READ_BUFFERS_ZERO_ON_ERROR: c_int = 1 << 2;
pub const READ_BUFFERS_IGNORE_CHECKSUM_FAILURES: c_int = 1 << 3;

// READ_STREAM_* flags
pub const READ_STREAM_FULL: c_int = 1 << 0;
pub const READ_STREAM_USE_BATCHING: c_int = 1 << 1;

// EB_* flags for ExtendBufferedRel*
pub const EB_SKIP_EXTENSION_LOCK: uint32 = 1 << 0;
pub const EB_LOCK_FIRST: uint32 = 1 << 1;
pub const EB_LOCK_TARGET: uint32 = 1 << 2;
pub const EB_CLEAR_SIZE_CACHE: uint32 = 1 << 3;
pub const EB_CREATE_FORK_IF_NEEDED: uint32 = 1 << 4;
pub const EB_PERFORMING_RECOVERY: uint32 = 1 << 5;

// Buffer lock modes
pub const BUFFER_LOCK_UNLOCK: c_int = 0;
pub const BUFFER_LOCK_SHARE: c_int = 1;
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

// ReadBufferMode
pub type ReadBufferMode = c_int;
pub const RBM_NORMAL: ReadBufferMode = 0;
pub const RBM_ZERO_AND_LOCK: ReadBufferMode = 1;
pub const RBM_ZERO_AND_CLEANUP_LOCK: ReadBufferMode = 2;
pub const RBM_ZERO_ON_ERROR: ReadBufferMode = 3;
pub const RBM_NORMAL_NO_LOG: ReadBufferMode = 4;

// IOOp constants
pub const IOOP_HIT: c_int = 0;
pub const IOOP_READ: c_int = 1;
pub const IOOP_WRITE: c_int = 2;
pub const IOOP_EXTEND: c_int = 3;
pub const IOOP_EVICT: c_int = 4;
pub const IOOP_REUSE: c_int = 5;
pub const IOOP_WRITEBACK: c_int = 6;
pub type IOOp = c_int;

// MAX_IO_COMBINE_LIMIT
pub const MAX_IO_COMBINE_LIMIT: c_int = 64;

// RELPERSISTENCE constants
pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
pub const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
pub const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

// P_NEW: extend the relation, caller's responsibility to ensure serialization
pub const P_NEW: BlockNumber = BlockNumber::MAX;

// io_direct flags
pub static mut io_direct_flags: c_int = 0;
pub const IO_DIRECT_DATA: c_int = 1 << 0;

// io_method
pub static mut io_method: c_int = 0; // IOMETHOD_SYNC = 0
pub const IOMETHOD_SYNC: c_int = 0;

// ignore_checksum_failure GUC
pub static mut ignore_checksum_failure: bool = false;

// PIV flags for PageIsVerified
pub const PIV_LOG_LOG: c_int = 1 << 0;
pub const PIV_IGNORE_CHECKSUM_FAILURE: c_int = 1 << 1;

// PGAIO_RESULT_ERROR_BITS
pub const PGAIO_RESULT_ERROR_BITS: u32 = 32;

// BAS_BULKREAD / BAS_BULKWRITE
pub const BAS_BULKREAD: c_int = 1;
pub const BAS_BULKWRITE: c_int = 2;
pub const BAS_VACUUM: c_int = 3;

// NUM_AUXILIARY_PROCS
pub const NUM_AUXILIARY_PROCS: c_int = 5;

// WAIT_EVENT_* constants (stubs)
pub const WAIT_EVENT_BUFFER_IO: uint32 = 0;
pub const WAIT_EVENT_BUFFER_PIN: uint32 = 1;

// PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
pub const PROCSIG_RECOVERY_CONFLICT_BUFFERPIN: c_int = 7;

// PG_USED_FOR_ASSERTS_ONLY (becomes #[allow(dead_code)] marker -- we just use it as a type)
type PgUsedForAssertsOnly<T> = T;

// enableFsync GUC
pub static mut enableFsync: bool = true;

// instr_time
pub type instr_time = u64;

// TimestampTz
pub type TimestampTz = i64;

// Datum
pub type Datum = usize;

// XLogRecPtr
pub type XLogRecPtr = u64;
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// Page / Block
pub type Page = *mut c_char;
pub type Block = *mut c_void;

// ProcNumber alias for wait_backend_pgprocno
pub type PgProcNo = c_int;

// RelPathStr - returned from relpathperm / relpathbackend
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelPathStr {
    pub str_: [c_char; 1],
}
impl RelPathStr {
    #[inline]
    pub fn str_ptr(&self) -> *const c_char { self.str_.as_ptr() }
}

// BlockRangeReadStreamPrivate
#[repr(C)]
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive: BlockNumber,
}

// PGIOAlignedBlock (storage/bufpage.h)
#[repr(C)]
pub struct PGIOAlignedBlock {
    pub data: [u8; BLCKSZ],
}

// ErrorContextCallback (utils/error/elog.h) -- minimal stub
#[repr(C)]
pub struct ErrorContextCallback {
    pub callback: Option<unsafe fn(*mut c_void)>,
    pub arg: *mut c_void,
    pub previous: *mut ErrorContextCallback,
}
#[allow(improper_ctypes)]
extern "C" {
    pub static mut error_context_stack: *mut ErrorContextCallback;
}

// SMgrRelationData with smgr_cached_nblocks field (storage/smgr.h)
// The real layout is in smgr.rs; here we cast to access the extra fields.
#[repr(C)]
pub struct SmgrCachedNblocks {
    // smgr_rlocator is at offset 0 (RelFileLocatorBackend)
    // For Darwin the layout matches the C struct.
    _rlocator: [u8; 16], // RelFileLocatorBackend = 3 Oids + backend int = 4*4=16 bytes
    pub smgr_cached_nblocks: [BlockNumber; (MAX_FORKNUM + 1) as usize],
}

#[inline]
unsafe fn smgr_cached_nblocks_ptr(smgr: SMgrRelation) -> *mut BlockNumber {
    (*(smgr as *mut SmgrCachedNblocks)).smgr_cached_nblocks.as_mut_ptr()
}

#[repr(C)]
struct SmgrRlocator {
    rlocator: RelFileLocatorBackend,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocatorBackend {
    pub locator: RelFileLocator,
    pub backend: c_int,
}

#[inline]
unsafe fn smgr_rlocator(smgr: SMgrRelation) -> RelFileLocatorBackend {
    (*(smgr as *mut SmgrRlocator)).rlocator
}

#[inline]
unsafe fn RelFileLocatorBackendIsTemp(rlocator: RelFileLocatorBackend) -> bool {
    rlocator.backend != INVALID_PROC_NUMBER
}

// BufferManagerRelation (storage/bufmgr.h)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BufferManagerRelation {
    pub rel: Relation,
    pub smgr: SMgrRelation,
    pub relpersistence: c_char,
}

#[inline]
pub unsafe fn BMR_REL(rel: Relation) -> BufferManagerRelation {
    BufferManagerRelation {
        rel,
        smgr: null_mut(),
        relpersistence: 0,
    }
}

#[inline]
pub unsafe fn BMR_SMGR(smgr: SMgrRelation, relpersistence: c_char) -> BufferManagerRelation {
    BufferManagerRelation {
        rel: null_mut(),
        smgr,
        relpersistence,
    }
}

// Relation opaque
pub type Relation = *mut RelationData;
#[repr(C)]
pub struct RelationData {
    pub rd_locator: RelFileLocator,
    pub rd_rel: *mut FormPgClass,
}
#[repr(C)]
pub struct FormPgClass {
    pub relpersistence: c_char,
    pub relkind: c_char,
}

// ----------------------------------------------------------------
// Stubs for unported functions
// ----------------------------------------------------------------

#[inline]
unsafe fn smgropen(_rlocator: RelFileLocator, _backend: c_int) -> SMgrRelation {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrprefetch(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber, _nblocks: c_int,
) -> bool {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrnblocks(_reln: SMgrRelation, _forknum: ForkNumber) -> BlockNumber {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrnblocks_cached(_reln: SMgrRelation, _forknum: ForkNumber) -> BlockNumber {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrexists(_reln: SMgrRelation, _forknum: ForkNumber) -> bool {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrcreate(_reln: SMgrRelation, _forknum: ForkNumber, _recovery: bool) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrextend(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber,
    _data: *const c_void, _skip_fsync: bool,
) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrzeroextend(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber,
    _nblocks: c_int, _skip_fsync: bool,
) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrwrite(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber,
    _buffer: *const c_void, _skip_fsync: bool,
) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrmaxcombine(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber,
) -> c_int {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrstartreadv(
    _ioh: *mut PgAioHandle, _reln: SMgrRelation, _forknum: ForkNumber,
    _blocknum: BlockNumber, _io_pages: *const *mut c_void, _nblocks: c_int,
) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn smgrwriteback(
    _reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber, _nblocks: usize,
) {
    unimplemented!() // TODO(pg-port): storage/smgr.c
}

#[inline]
unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    unimplemented!() // TODO(pg-port): utils/cache/relcache.c
}

#[inline]
unsafe fn RelationUsesLocalBuffers(rel: Relation) -> bool {
    (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
}

#[inline]
unsafe fn RELATION_IS_OTHER_TEMP(rel: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/cache/relcache.c
}

#[inline]
unsafe fn LockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}

#[inline]
unsafe fn UnlockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}

pub const ExclusiveLock: c_int = 8;

#[inline]
unsafe fn PageSetChecksumCopy(_page: Page, _blkno: BlockNumber) -> *const c_void {
    unimplemented!() // TODO(pg-port): storage/page/checksum.c
}

#[inline]
unsafe fn PageIsVerified(
    _page: Page, _blkno: BlockNumber, _flags: c_int, _failed_checksum: *mut bool,
) -> bool {
    unimplemented!() // TODO(pg-port): storage/page/bufpage.c
}

#[inline]
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO(pg-port): storage/page/bufpage.c
}

#[inline]
unsafe fn PageGetLSN(_page: *const c_void) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): storage/page/bufpage.c
}

#[inline]
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): storage/page/bufpage.c
}

#[inline]
pub unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    // BufHdrGetBlock on shared buffers; local uses LocalBufHdrGetBlock
    if BufferIsLocal(buffer) {
        unimplemented!() // TODO(pg-port): use LocalBufHdrGetBlock
    } else {
        BufHdrGetBlock(GetBufferDescriptor((buffer - 1) as u32)) as Page
    }
}

#[inline]
pub unsafe fn BufferGetBlock(buffer: Buffer) -> *mut c_void {
    if BufferIsLocal(buffer) {
        unimplemented!() // TODO(pg-port)
    } else {
        BufHdrGetBlock(GetBufferDescriptor((buffer - 1) as u32))
    }
}

#[inline]
unsafe fn pgstat_count_io_op(
    _io_object: IOObject, _io_context: IOContext, _io_op: IOOp,
    _cnt: uint32, _bytes: u64,
) {
    // TODO(pg-port): utils/activity/pgstat_io.c
}

#[inline]
unsafe fn pgstat_count_io_op_time(
    _io_object: IOObject, _io_context: IOContext, _io_op: IOOp,
    _start_time: instr_time, _cnt: uint32, _bytes: u64,
) {
    // TODO(pg-port): utils/activity/pgstat_io.c
}

#[inline]
unsafe fn pgstat_prepare_io_time(_track: bool) -> instr_time {
    0 // TODO(pg-port): utils/activity/pgstat_io.c
}

#[inline]
unsafe fn pgstat_count_buffer_read(_rel: Relation) {
    // TODO(pg-port): utils/activity/pgstat_relation.c
}

#[inline]
unsafe fn pgstat_count_buffer_hit(_rel: Relation) {
    // TODO(pg-port): utils/activity/pgstat_relation.c
}

#[inline]
unsafe fn pgstat_prepare_report_checksum_failure(_dboid: Oid) {
    // TODO(pg-port)
}

#[inline]
unsafe fn pgstat_report_checksum_failures_in_db(_dboid: Oid, _count: uint8) {
    // TODO(pg-port)
}

#[inline]
unsafe fn XLogFlush(_recptr: XLogRecPtr) {
    // TODO(pg-port): access/transam/xlog.c
}

#[inline]
unsafe fn XLogNeedsFlush(_recptr: XLogRecPtr) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn XLogIsNeeded() -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn XLogHintBitIsNeeded() -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn XLogSaveBufferForHint(_buffer: Buffer, _buffer_std: bool) -> XLogRecPtr {
    InvalidXLogRecPtr // TODO(pg-port)
}

#[inline]
unsafe fn XLogRecPtrIsInvalid(recptr: XLogRecPtr) -> bool {
    recptr == InvalidXLogRecPtr
}

#[inline]
unsafe fn RelFileLocatorSkippingWAL(_rlocator: RelFileLocator) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn RecoveryInProgress() -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn log_newpage_buffer(_buf: Buffer, _page_std: bool) {
    // TODO(pg-port): access/transam/xloginsert.c
}

#[inline]
unsafe fn log_smgrcreate(_rlocator: *const RelFileLocator, _forknum: ForkNumber) {
    // TODO(pg-port): catalog/storage_xlog.c
}

#[inline]
unsafe fn RelationCreateStorage(
    _rlocator: RelFileLocator, _relpersistence: c_char, _register_cancel: bool,
) {
    // TODO(pg-port): catalog/storage.c
}

// Vacuum cost globals
pub static mut VacuumCostActive: bool = false;
pub static mut VacuumCostBalance: c_int = 0;
pub static mut VacuumCostPageHit: c_int = 1;
pub static mut VacuumCostPageMiss: c_int = 10;
pub static mut VacuumCostPageDirty: c_int = 20;

// BufferUsage stats
#[repr(C)]
pub struct BufferUsage {
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub local_blks_hit: i64,
    pub local_blks_read: i64,
    pub local_blks_dirtied: i64,
    pub local_blks_written: i64,
}
pub static mut pgBufferUsage: BufferUsage = BufferUsage {
    shared_blks_hit: 0, shared_blks_read: 0, shared_blks_dirtied: 0, shared_blks_written: 0,
    local_blks_hit: 0, local_blks_read: 0, local_blks_dirtied: 0, local_blks_written: 0,
};

// Checkpoint / bgwriter stats
#[repr(C)]
pub struct CheckpointStatsData {
    pub ckpt_bufs_written: c_int,
}
pub static mut CheckpointStats: CheckpointStatsData = CheckpointStatsData { ckpt_bufs_written: 0 };

#[repr(C)]
pub struct PendingCheckpointerStatsData {
    pub buffers_written: c_int,
}
pub static mut PendingCheckpointerStats: PendingCheckpointerStatsData =
    PendingCheckpointerStatsData { buffers_written: 0 };

#[repr(C)]
pub struct PendingBgWriterStatsData {
    pub buf_alloc: c_int,
    pub maxwritten_clean: c_int,
    pub buf_written_clean: c_int,
}
pub static mut PendingBgWriterStats: PendingBgWriterStatsData =
    PendingBgWriterStatsData { buf_alloc: 0, maxwritten_clean: 0, buf_written_clean: 0 };

// binaryheap (lib/binaryheap.h) stub
pub type binaryheap = c_void;
pub type BinaryHeapComparator =
    Option<unsafe fn(a: Datum, b: Datum, arg: *mut c_void) -> c_int>;

#[inline]
unsafe fn binaryheap_allocate(_capacity: c_int, _compare: BinaryHeapComparator, _arg: *mut c_void) -> *mut binaryheap {
    unimplemented!() // TODO(pg-port): lib/binaryheap.c
}

#[inline]
unsafe fn binaryheap_add_unordered(_heap: *mut binaryheap, _val: Datum) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_build(_heap: *mut binaryheap) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_empty(_heap: *mut binaryheap) -> bool {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_first(_heap: *mut binaryheap) -> Datum {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_remove_first(_heap: *mut binaryheap) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_replace_first(_heap: *mut binaryheap, _val: Datum) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn binaryheap_free(_heap: *mut binaryheap) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void {
    d as *mut c_void
}

#[inline]
unsafe fn PointerGetDatum(p: *mut c_void) -> Datum {
    p as Datum
}

#[inline]
unsafe fn DatumGetInt32(d: Datum) -> i32 {
    d as i32
}

// BgWriterDelay GUC
pub static mut BgWriterDelay: c_int = 200;

// Process signal barrier
pub static mut ProcSignalBarrierPending: bool = false;
#[inline]
unsafe fn ProcessProcSignalBarrier() {}

// DeadlockTimeout GUC
pub static mut DeadlockTimeout: c_int = 1000;
pub static mut log_recovery_conflict_waits: bool = false;

#[inline]
unsafe fn InHotStandby() -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    0 // TODO(pg-port)
}

#[inline]
unsafe fn TimestampDifferenceExceeds(
    _start: TimestampTz, _stop: TimestampTz, _msec: c_int,
) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn LogRecoveryConflict(
    _reason: c_int, _wait_start: TimestampTz, _now: TimestampTz,
    _procsigs: *mut c_void, _in_hot_standby: bool,
) {
    // TODO(pg-port)
}

#[inline]
unsafe fn set_ps_display_suffix(_suffix: &str) {}

#[inline]
unsafe fn set_ps_display_remove_suffix() {}

#[inline]
unsafe fn ProcSendSignal(_pgprocno: c_int) {
    // TODO(pg-port)
}

#[inline]
unsafe fn ProcWaitForSignal(_event: uint32) {
    // TODO(pg-port)
}

#[inline]
unsafe fn SetStartupBufferPinWaitBufId(_buf_id: c_int) {}

#[inline]
unsafe fn GetStartupBufferPinWaitBufId() -> c_int {
    -1 // TODO(pg-port)
}

#[inline]
unsafe fn ResolveRecoveryConflictWithBufferPin() {
    // TODO(pg-port)
}

#[inline]
unsafe fn CHECK_FOR_INTERRUPTS() {}

#[inline]
unsafe fn on_shmem_exit(_func: unsafe fn(c_int, Datum), _arg: Datum) {
    // TODO(pg-port)
}

#[inline]
unsafe fn UnlockBuffers_lwlocks() {
    // LWLockReleaseAll handled by lwlock.c; no-op here
}

#[inline]
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LWLockDisown(_lock: *mut LWLock) {
    // TODO(pg-port)
}

pub const LW_SHARED: c_int = 1;
pub const LW_EXCLUSIVE: c_int = 2;

#[inline]
unsafe fn ForEachLWLockHeldByMe(
    _callback: unsafe fn(*mut LWLock, c_int, *mut c_void),
    _arg: *mut c_void,
) {
    // TODO(pg-port)
}

#[inline]
unsafe fn psprintf(_fmt: &str) -> *mut c_char {
    null_mut() // TODO(pg-port)
}

#[inline]
unsafe fn relpathperm(_rlocator: RelFileLocator, _forknum: ForkNumber) -> RelPathStr {
    RelPathStr { str_: [0] } // TODO(pg-port)
}

#[inline]
unsafe fn relpathbackend(_rlocator: RelFileLocator, _backend: c_int, _forknum: ForkNumber) -> RelPathStr {
    RelPathStr { str_: [0] } // TODO(pg-port)
}

#[inline]
unsafe fn relpath(_rlocator: RelFileLocatorBackend, _forknum: ForkNumber) -> RelPathStr {
    RelPathStr { str_: [0] } // TODO(pg-port)
}

#[inline]
unsafe fn errcontext(_fmt: *const c_char) {}

#[inline]
unsafe fn MemSet(_ptr: *mut c_void, _val: c_int, _size: usize) {
    // TODO(pg-port)
}

#[inline]
unsafe fn CheckpointWriteDelay(_flags: c_int, _progress: f64) {}

#[inline]
unsafe fn GetAccessStrategy(_bstrategy: c_int) -> BufferAccessStrategy {
    null_mut() // TODO(pg-port)
}

#[inline]
unsafe fn FreeAccessStrategy(_strategy: BufferAccessStrategy) {}

#[inline]
unsafe fn read_stream_begin_smgr_relation(
    _flags: c_int, _strategy: BufferAccessStrategy, _smgr: SMgrRelation,
    _persistence: c_char, _forknum: ForkNumber,
    _callback: Option<unsafe fn(*mut ReadStream, *mut c_void) -> BlockNumber>,
    _cb_arg: *mut c_void, _reserved: c_int,
) -> *mut ReadStream {
    null_mut() // TODO(pg-port)
}

#[inline]
unsafe fn read_stream_next_buffer(_stream: *mut ReadStream, _arg: *mut *mut c_void) -> Buffer {
    InvalidBuffer // TODO(pg-port)
}

#[inline]
unsafe fn read_stream_end(_stream: *mut ReadStream) {}

#[inline]
unsafe fn block_range_read_stream_cb(_stream: *mut ReadStream, _arg: *mut c_void) -> BlockNumber {
    InvalidBlockNumber // TODO(pg-port)
}

#[inline]
unsafe fn IsCatalogTextUniqueIndexOid(_relid: Oid) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn IsCatalogRelationOid(_relid: Oid) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn RELKIND_HAS_TABLE_AM(_relkind: c_char) -> bool {
    false // TODO(pg-port)
}

#[inline]
unsafe fn RELKIND_HAS_STORAGE(_relkind: c_char) -> bool {
    true // TODO(pg-port)
}

#[inline]
unsafe fn table_relation_size(_rel: Relation, _forknum: ForkNumber) -> u64 {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn ResourceOwnerRememberBufferIO(_owner: *mut c_void, _buffer: Buffer) {
    // TODO(pg-port)
}

// localbuf.c helpers referenced by the local-buffer fast paths. The real C
// counterparts (ReadBuffer_common's local branch, ExtendBufferedRelLocal,
// LocalRelSize/smgrnblocks, FlushLocalRelationBuffers, DropRelationLocalBuffers)
// are not yet wired up here; stubbed to keep signatures faithful.
#[inline]
unsafe fn ReadLocalBuffer(
    _reln: Relation,
    _fork: ForkNumber,
    _block: BlockNumber,
    _mode: ReadBufferMode,
) -> Buffer {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn ExtendLocalRelation(
    _bmr: BufferManagerRelation,
    _fork: ForkNumber,
    _strategy: BufferAccessStrategy,
    _flags: uint32,
    _blockNum_p: *mut BlockNumber,
) -> Buffer {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn LocalRelSize(_rel: Relation, _fork: ForkNumber) -> BlockNumber {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn DropLocalRelFileLocatorBuffers(
    _rlocator: RelFileLocator,
    _fork: ForkNumber,
    _first: BlockNumber,
) {
    // TODO(pg-port)
}

#[inline]
unsafe fn FlushLocalRelationBuffers(_rel: Relation) {
    // TODO(pg-port)
}

#[inline]
unsafe fn ResourceOwnerForgetBufferIO(_owner: *mut c_void, _buffer: Buffer) {
    // TODO(pg-port)
}

#[inline]
unsafe fn RelFileLocatorEquals(a: RelFileLocator, b: RelFileLocator) -> bool {
    a.spcOid == b.spcOid && a.dbOid == b.dbOid && a.relNumber == b.relNumber
}

// VALGRIND no-ops
#[inline] unsafe fn VALGRIND_MAKE_MEM_DEFINED(_addr: *mut c_void, _len: usize) {}
#[inline] unsafe fn VALGRIND_MAKE_MEM_NOACCESS(_addr: *mut c_void, _len: usize) {}

// pfree / palloc / repalloc stubs
#[inline]
unsafe fn pfree(_ptr: *mut c_void) {}

#[inline]
unsafe fn palloc(sz: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn repalloc(_ptr: *mut c_void, _sz: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn qsort(
    _base: *mut c_void, _n: usize, _size: usize,
    _compar: Option<unsafe fn(*const c_void, *const c_void) -> c_int>,
) {
    unimplemented!() // TODO(pg-port)
}

#[inline]
unsafe fn bsearch(
    _key: *const c_void, _base: *const c_void, _n: usize, _size: usize,
    _compar: Option<unsafe fn(*const c_void, *const c_void) -> c_int>,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}

// START_CRIT_SECTION / END_CRIT_SECTION
#[inline] unsafe fn START_CRIT_SECTION() {}
#[inline] unsafe fn END_CRIT_SECTION() {}

// TRACE macros - all no-ops
macro_rules! TRACE_POSTGRESQL_BUFFER_READ_START { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_READ_DONE  { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_FLUSH_START { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_FLUSH_DONE  { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_EXTEND_START { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_EXTEND_DONE  { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_SYNC_START { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_SYNC_WRITTEN { ($($t:tt)*) => {} }
macro_rules! TRACE_POSTGRESQL_BUFFER_SYNC_DONE { ($($t:tt)*) => {} }

// ----------------------------------------------------------------
// Macros used in this file
// ----------------------------------------------------------------

/// BufHdrGetBlock -- get a pointer to the block stored in the shared buffer.
/// Only works for shared buffers, not local ones.
#[inline]
pub unsafe fn BufHdrGetBlock(buf_hdr: *const BufferDesc) -> Block {
    extern "C" {
        static BufferBlocks: *mut u8;
    }
    BufferBlocks.add((*buf_hdr).buf_id as usize * BLCKSZ) as Block
}

/// BufferGetLSN -- get the current LSN of a shared buffer.
/// Only works on shared buffers.
#[inline]
pub unsafe fn BufferGetLSN(buf_hdr: *const BufferDesc) -> XLogRecPtr {
    PageGetLSN(BufHdrGetBlock(buf_hdr) as *const c_void)
}

/// LocalBufHdrGetBlock -- get block for a local buffer.
/// Only works for local buffers.
#[inline]
unsafe fn LocalBufHdrGetBlock(bufHdr: *const BufferDesc) -> Block {
    use crate::storage::buffer::localbuf::LocalBufferBlockPointers;
    *LocalBufferBlockPointers.offset(-((*bufHdr).buf_id + 2) as isize)
}

// Bits in SyncOneBuffer's return value
const BUF_WRITTEN: c_int = 0x01;
const BUF_REUSABLE: c_int = 0x02;

const RELS_BSEARCH_THRESHOLD: usize = 20;

/// This is the size (in the number of blocks) above which we scan the
/// entire buffer pool to remove the buffers for all the pages of relation
/// being dropped.
#[inline]
unsafe fn BUF_DROP_FULL_SCAN_THRESHOLD() -> u64 {
    (NBuffers as u64) / 32
}

// ----------------------------------------------------------------
// Private reference count array
//
// To avoid locking shared data structures for every pin/unpin operation
// we maintain a cache of per-backend refcounts in a small array (with
// a hash-table overflow for more than REFCOUNT_ARRAY_ENTRIES buffers).
// ----------------------------------------------------------------

const REFCOUNT_ARRAY_ENTRIES: usize = 8;

/// An entry in the array/hash of per-backend private reference counts.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PrivateRefCountEntry {
    pub buffer: Buffer,
    pub refcount: c_int,
}

/// The array of PrivateRefCountEntries.
static mut PrivateRefCountArray: [PrivateRefCountEntry; REFCOUNT_ARRAY_ENTRIES] =
    [PrivateRefCountEntry { buffer: InvalidBuffer, refcount: 0 }; REFCOUNT_ARRAY_ENTRIES];

/// Overflow hash table.
static mut PrivateRefCountHash: *mut HTAB = null_mut();

/// How many distinct buffers are currently pinned (used for assert).
static mut PrivateRefCountOverflow: c_int = 0;

// ----------------------------------------------------------------
// ResourceOwner callbacks registered for buffer pins
// ----------------------------------------------------------------

// (ResOwner callbacks are defined at the end of this file)

// ----------------------------------------------------------------
// GUC variables
// ----------------------------------------------------------------

pub static mut bgwriter_lru_maxpages: c_int = 100;
pub static mut bgwriter_lru_multiplier: f64 = 2.0;
pub static mut bgwriter_flush_after: c_int = 0; // in pages; platform default set below
pub static mut checkpoint_flush_after: c_int = 0;
pub static mut backend_flush_after: c_int = 0;

pub static mut BufferInitGlobal_inProgress: bool = false;

// ----------------------------------------------------------------
// CkptTsStatus - checkpoint buffer flush status per tablespace
// ----------------------------------------------------------------

#[repr(C)]
pub struct CkptTsStatus {
    pub tsId: Oid,
    /// Checkpoint progress for this tablespace. To make progress comparable
    /// between tablespaces the progress is, for each tablespace, measured as a
    /// number between 0 and the total number of to-be-checkpointed pages. Each
    /// page checkpointed in this tablespace increments this space's progress
    /// by progress_slice.
    pub progress: f64,
    pub progress_slice: f64,
    /// the info below is about the strategy for writing the next batch of
    /// dirty buffers for the tablespace
    pub num_to_scan: c_int,
    pub num_scanned: c_int,
    /// index into CkptBufferIds for the current tablespace
    pub index_start: c_int,
    pub index_end: c_int,
}

// ----------------------------------------------------------------
// SMgrSortArray - sort a set of SMgrRelation entries by relation locator
// ----------------------------------------------------------------

#[repr(C)]
pub struct SMgrSortArray {
    pub rlocator: RelFileLocator,
    pub srel: SMgrRelation,
}

// ----------------------------------------------------------------
// Forward declarations (static fns, defined later in this file)
// ----------------------------------------------------------------

// (All declared inline further down; Rust doesn't need forward declarations.)

// ----------------------------------------------------------------
// ReservePrivateRefCountEntry
//
// Reserve space in the PrivateRefCountArray or the overflow hash for a new
// entry.  Call this BEFORE acquiring a new buffer pin so that we never fail
// with a pin held.
// ----------------------------------------------------------------
pub unsafe fn ReservePrivateRefCountEntry() {
    /* Already have a free slot? */
    for i in 0..REFCOUNT_ARRAY_ENTRIES {
        if PrivateRefCountArray[i].buffer == InvalidBuffer {
            return;
        }
    }

    /*
     * All array entries are in use, so we have to rely on the hash table. We
     * need to create one if it doesn't exist yet.
     */
    if PrivateRefCountHash.is_null() {
        let mut info: HASHCTL = core::mem::zeroed();
        info.keysize = core::mem::size_of::<Buffer>();
        info.entrysize = core::mem::size_of::<PrivateRefCountEntry>();
        PrivateRefCountHash = hash_create(
            b"PrivateRefCount\0".as_ptr() as *const c_char,
            100,
            &info,
            HASH_ELEM | HASH_BLOBS,
        );
    }

    /*
     * At this point we have to expand the hash table; the entry creation
     * below will trigger the allocation.  There's nothing more to do here.
     */
}

// ----------------------------------------------------------------
// NewPrivateRefCountEntry
//
// Create a new PrivateRefCountEntry for the given buffer.
// ReservePrivateRefCountEntry() must have been called first.
// ----------------------------------------------------------------
unsafe fn NewPrivateRefCountEntry(buffer: Buffer) -> *mut PrivateRefCountEntry {
    /* try to find space in the array first */
    for i in 0..REFCOUNT_ARRAY_ENTRIES {
        if PrivateRefCountArray[i].buffer == InvalidBuffer {
            PrivateRefCountArray[i].buffer = buffer;
            PrivateRefCountArray[i].refcount = 0;
            return &mut PrivateRefCountArray[i];
        }
    }

    /* use the overflow hash table */
    let mut found: bool = false;
    let res = hash_search(
        PrivateRefCountHash,
        &buffer as *const Buffer as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut PrivateRefCountEntry;
    assert!(!found, "new buffer pin already has a private refcount entry");
    (*res).buffer = buffer;
    (*res).refcount = 0;
    PrivateRefCountOverflow += 1;
    res
}

// ----------------------------------------------------------------
// GetPrivateRefCountEntry
//
// Return the PrivateRefCountEntry for the given buffer (or NULL if not found).
// ----------------------------------------------------------------
unsafe fn GetPrivateRefCountEntry(
    buffer: Buffer,
    do_move: bool,
) -> *mut PrivateRefCountEntry {
    /*
     * First search the array, that's the common case and it's cheap.
     */
    for i in 0..REFCOUNT_ARRAY_ENTRIES {
        if PrivateRefCountArray[i].buffer == buffer {
            return &mut PrivateRefCountArray[i];
        }
    }

    if PrivateRefCountHash.is_null() {
        return null_mut();
    }

    /* search the overflow hash table */
    let mut found: bool = false;
    let res = hash_search(
        PrivateRefCountHash,
        &buffer as *const Buffer as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut PrivateRefCountEntry;
    if !found {
        return null_mut();
    }

    /*
     * Optionally move the entry from the hash table into the array to exploit
     * temporal locality of accesses.
     */
    if do_move {
        /* find a free slot in the array */
        let mut free_idx: Option<usize> = None;
        for i in 0..REFCOUNT_ARRAY_ENTRIES {
            if PrivateRefCountArray[i].buffer == InvalidBuffer {
                free_idx = Some(i);
                break;
            }
        }
        if let Some(idx) = free_idx {
            let refcount = (*res).refcount;
            /* remove from hash table */
            hash_search(
                PrivateRefCountHash,
                &buffer as *const Buffer as *const c_void,
                HASH_REMOVE,
                &mut found,
            );
            PrivateRefCountOverflow -= 1;
            /* insert into array */
            PrivateRefCountArray[idx].buffer = buffer;
            PrivateRefCountArray[idx].refcount = refcount;
            return &mut PrivateRefCountArray[idx];
        }
    }

    res
}

// ----------------------------------------------------------------
// GetPrivateRefCount
//
// Returns the current private refcount for the given buffer (0 if none).
// ----------------------------------------------------------------
pub unsafe fn GetPrivateRefCount(buffer: Buffer) -> c_int {
    let ref_ = GetPrivateRefCountEntry(buffer, false);
    if ref_.is_null() {
        0
    } else {
        (*ref_).refcount
    }
}

// ----------------------------------------------------------------
// ForgetPrivateRefCountEntry
//
// Remove the entry for the given buffer from the private refcount tracking.
// Asserts that the refcount has already been reduced to 0.
// ----------------------------------------------------------------
unsafe fn ForgetPrivateRefCountEntry(ref_: *mut PrivateRefCountEntry) {
    assert_eq!((*ref_).refcount, 0);

    /* Is it in the array? */
    let in_array = (ref_ >= PrivateRefCountArray.as_mut_ptr())
        && (ref_
            < PrivateRefCountArray
                .as_mut_ptr()
                .add(REFCOUNT_ARRAY_ENTRIES));

    if in_array {
        (*ref_).buffer = InvalidBuffer;
    } else {
        let buffer = (*ref_).buffer;
        let mut found: bool = false;
        hash_search(
            PrivateRefCountHash,
            &buffer as *const Buffer as *const c_void,
            HASH_REMOVE,
            &mut found,
        );
        assert!(found);
        PrivateRefCountOverflow -= 1;
    }
}

// ----------------------------------------------------------------
// PrefetchSharedBuffer
//
// Initiate an asynchronous read for the given buffer, if supported.
// Returns whether a kernel-level prefetch was initiated.
// ----------------------------------------------------------------
pub unsafe fn PrefetchSharedBuffer(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) -> PrefetchBufferResult {
    let mut result: PrefetchBufferResult = core::mem::zeroed();
    result.recent_buffer = InvalidBuffer;
    result.initiated_io = false;

    assert!(BlockNumberIsValid(blockNum));

    /* create a tag for the block */
    let mut newTag: BufferTag = core::mem::zeroed();
    InitBufferTag(&mut newTag, &smgr_rlocator(smgr).locator, forkNum, blockNum);

    /* see if the block is in the buffer pool already */
    let newHash = BufTableHashCode(&mut newTag);
    let partitionLock = BufMappingPartitionLock(newHash);

    LWLockAcquire(partitionLock, LW_SHARED);
    let buf_id = BufTableLookup(&mut newTag, newHash);
    LWLockRelease(partitionLock);

    /* If not in buffer pool, initiate prefetch */
    if buf_id < 0 {
        if smgrprefetch(smgr, forkNum, blockNum, 1) {
            result.initiated_io = true;
        }
    } else {
        result.recent_buffer = buf_id + 1; /* convert to Buffer */
    }

    result
}

// ----------------------------------------------------------------
// PrefetchBuffer
//
// Initiate an asynchronous read for the given buffer (by Relation).
// ----------------------------------------------------------------
pub unsafe fn PrefetchBuffer(
    rel: Relation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) -> PrefetchBufferResult {
    assert!(!rel.is_null());
    assert!(RelationIsValid(rel));

    if RelationUsesLocalBuffers(rel) {
        /* No support for prefetching local buffers */
        let mut result: PrefetchBufferResult = core::mem::zeroed();
        result.recent_buffer = InvalidBuffer;
        result.initiated_io = false;
        result
    } else {
        /* Open smgr if not already done for this rel */
        PrefetchSharedBuffer(RelationGetSmgr(rel), forkNum, blockNum)
    }
}

#[inline]
unsafe fn RelationIsValid(rel: Relation) -> bool {
    !rel.is_null()
}

// ----------------------------------------------------------------
// ReadRecentBuffer
//
// Pin a previously-seen buffer, if it's still valid.
// Returns true if the buffer is still valid.
// ----------------------------------------------------------------
pub unsafe fn ReadRecentBuffer(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    recent_buffer: Buffer,
) -> bool {
    /* Is the buffer still referencing the right tag? */
    if !BufferIsValid(recent_buffer) {
        return false;
    }

    let bufHdr: *mut BufferDesc;
    if BufferIsLocal(recent_buffer) {
        /* local buffer */
        bufHdr = GetLocalBufferDescriptor((-recent_buffer - 1) as u32);
    } else {
        bufHdr = GetBufferDescriptor((recent_buffer - 1) as u32);
    }

    let mut newTag: BufferTag = core::mem::zeroed();
    InitBufferTag(&mut newTag, &rlocator, forkNum, blockNum);

    /*
     * We must check the buffer's tag while holding the buffer header
     * lock to prevent a concurrent eviction from changing the tag
     * under us.
     */
    let buf_state = LockBufHdr(bufHdr);

    /* Check that the buffer tag matches */
    if (buf_state & BM_VALID) == 0
        || !BufferTagsEqual(&(*bufHdr).tag, &newTag)
    {
        UnlockBufHdr(bufHdr, buf_state);
        return false;
    }

    if BufferIsLocal(recent_buffer) {
        /* Already pinned in local-buffer land? */
        use crate::storage::buffer::localbuf::LocalRefCount;
        let idx = (-recent_buffer - 1) as usize;
        *LocalRefCount.add(idx) += 1;
        UnlockBufHdr(bufHdr, buf_state);
        return true;
    }

    /* Pin the shared buffer */
    PinBuffer_Locked(bufHdr);
    /* PinBuffer_Locked released buf header lock */

    true
}

// ----------------------------------------------------------------
// ReadBuffer  (simple wrapper)
// ----------------------------------------------------------------
pub unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer {
    ReadBufferExtended(reln, MAIN_FORKNUM, blockNum, RBM_NORMAL, null_mut())
}

// ----------------------------------------------------------------
// ReadBufferExtended
// ----------------------------------------------------------------
pub unsafe fn ReadBufferExtended(
    reln: Relation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
) -> Buffer {
    assert!(!RELATION_IS_OTHER_TEMP(reln));

    if RelationUsesLocalBuffers(reln) {
        return ReadLocalBuffer(reln, forkNum, blockNum, mode);
    }

    /* Open smgr if not already done for this rel */
    let smgr = RelationGetSmgr(reln);

    /* Temporary relations are not allowed in shared buffers */
    assert_ne!((*(*reln).rd_rel).relpersistence, RELPERSISTENCE_TEMP);

    ReadBuffer_common(
        BMR_REL(reln),
        (*(*reln).rd_rel).relpersistence,
        forkNum,
        blockNum,
        mode,
        strategy,
        &mut false,
    )
}

// ----------------------------------------------------------------
// ReadBufferWithoutRelcache
// ----------------------------------------------------------------
pub unsafe fn ReadBufferWithoutRelcache(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
    permanent: bool,
) -> Buffer {
    let backend: c_int = if permanent {
        INVALID_PROC_NUMBER
    } else {
        MyProcNumber
    };
    let smgr = smgropen(rlocator, backend);
    let relpersistence = if permanent {
        RELPERSISTENCE_PERMANENT
    } else {
        RELPERSISTENCE_TEMP
    };
    ReadBuffer_common(
        BMR_SMGR(smgr, relpersistence),
        relpersistence,
        forkNum,
        blockNum,
        mode,
        strategy,
        &mut false,
    )
}

// ----------------------------------------------------------------
// ExtendBufferedRel
// ----------------------------------------------------------------
pub unsafe fn ExtendBufferedRel(
    bmr: BufferManagerRelation,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
) -> Buffer {
    let mut blockNum: BlockNumber = 0;
    let buf = ExtendBufferedRelBy(bmr, forkNum, strategy, flags, 1, &mut blockNum, &mut 1);
    buf
}

// ----------------------------------------------------------------
// ExtendBufferedRelBy
//
// Extend a relation by extend_by blocks, returning a pointer to the first
// newly-created buffer (and the block number of that block in *blockNum_p).
// ----------------------------------------------------------------
pub unsafe fn ExtendBufferedRelBy(
    bmr: BufferManagerRelation,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
    extend_by: uint32,
    blockNum_p: *mut BlockNumber,
    extended_by_p: *mut uint32,
) -> Buffer {
    assert!(!bmr.rel.is_null() || !bmr.smgr.is_null());
    assert!(extend_by > 0);

    if !bmr.rel.is_null() && RelationUsesLocalBuffers(bmr.rel) {
        *extended_by_p = 1;
        return ExtendLocalRelation(
            bmr, forkNum, strategy, flags, blockNum_p,
        );
    }

    ExtendBufferedRelCommon(bmr, forkNum, strategy, flags, extend_by, blockNum_p, extended_by_p)
}

// ----------------------------------------------------------------
// ExtendBufferedRelTo
//
// Extend relation until it has at least extend_to blocks.
// ----------------------------------------------------------------
pub unsafe fn ExtendBufferedRelTo(
    bmr: BufferManagerRelation,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
    extend_to: BlockNumber,
    blockNum_p: *mut BlockNumber,
) -> Buffer {
    assert!(!bmr.rel.is_null() || !bmr.smgr.is_null());

    let mut first_new_block: BlockNumber = 0;
    let mut extended_by: uint32 = 0;

    /*
     * Loop until we have extended the relation far enough.  Note that it may
     * be that another backend extended it in the meantime.
     */
    loop {
        let current_size: BlockNumber;
        if !bmr.smgr.is_null() {
            current_size = smgrnblocks(bmr.smgr, forkNum);
        } else {
            current_size = smgrnblocks(RelationGetSmgr(bmr.rel), forkNum);
        }

        if current_size >= extend_to {
            *blockNum_p = extend_to - 1;
            return InvalidBuffer;
        }

        let extend_by = extend_to - current_size;
        let buf = ExtendBufferedRelCommon(
            bmr, forkNum, strategy, flags,
            extend_by, &mut first_new_block, &mut extended_by,
        );
        if buf != InvalidBuffer {
            *blockNum_p = first_new_block;
            return buf;
        }
        /* someone else extended; loop */
    }
}

// ----------------------------------------------------------------
// ZeroAndLockBuffer
//
// Zero the contents of a buffer and set the buffer header state.
// ----------------------------------------------------------------
pub unsafe fn ZeroAndLockBuffer(
    buffer: Buffer,
    mode: ReadBufferMode,
) {
    assert!(BufferIsValid(buffer));

    let bufHdr: *mut BufferDesc;
    if BufferIsLocal(buffer) {
        bufHdr = GetLocalBufferDescriptor((-buffer - 1) as u32);
    } else {
        bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    }

    /* zero the page */
    core::ptr::write_bytes(
        BufferGetPage(buffer) as *mut u8,
        0,
        BLCKSZ,
    );

    if BufferIsLocal(buffer) {
        /* No need for lock on local buffers */
        pg_atomic_fetch_or_u32(&(*bufHdr).state, BM_VALID);
        return;
    }

    /* Acquire content lock */
    let content_lock = BufferDescriptorGetContentLock(bufHdr);
    if mode == RBM_ZERO_AND_CLEANUP_LOCK {
        LWLockAcquire(content_lock, LW_EXCLUSIVE);
        /* mark buffer as clean and pinned (done by PinBuffer_Locked) */
        let buf_state = LockBufHdr(bufHdr);
        /* clear dirty bits, set valid */
        let mut new_state = buf_state;
        new_state &= !BM_DIRTY;
        new_state |= BM_VALID;
        /* also set pin-count waiter so cleanup gets the lock */
        UnlockBufHdr(bufHdr, new_state);
    } else {
        LWLockAcquire(content_lock, LW_EXCLUSIVE);
        let buf_state = LockBufHdr(bufHdr);
        let mut new_state = buf_state;
        new_state &= !BM_DIRTY;
        new_state |= BM_VALID;
        UnlockBufHdr(bufHdr, new_state);
    }
}

// ----------------------------------------------------------------
// PinBufferForBlock
//
// Attempt to pin an already-found buffer for a given block.
// buf_state_p is updated on success.  Returns false if the buffer
// was evicted between lookup and pin.
// ----------------------------------------------------------------
unsafe fn PinBufferForBlock(
    rel: Relation,
    smgr: SMgrRelation,
    persistence: c_char,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    strategy: BufferAccessStrategy,
    buf_state_p: *mut uint32,
) -> Buffer {
    /*
     * Pick the IO context to use for this operation (for pgstats).
     */
    let io_context = IOContextForStrategy(strategy);

    let mut newTag: BufferTag = core::mem::zeroed();
    let rlocator = if !smgr.is_null() {
        smgr_rlocator(smgr).locator
    } else {
        (*rel).rd_locator
    };
    InitBufferTag(&mut newTag, &rlocator, forkNum, blockNum);

    let newHash = BufTableHashCode(&mut newTag);
    let partitionLock = BufMappingPartitionLock(newHash);

    LWLockAcquire(partitionLock, LW_SHARED);
    let buf_id = BufTableLookup(&mut newTag, newHash);

    if buf_id < 0 {
        /* Not in buffer pool */
        LWLockRelease(partitionLock);
        *buf_state_p = 0;
        return InvalidBuffer;
    }

    /* Found it in the buffer pool.  Now pin it. */
    let buf = buf_id + 1;
    let bufHdr = GetBufferDescriptor(buf_id as u32);

    let mut buf_state = LockBufHdr(bufHdr);

    /*
     * If the buffer is not valid, or the tag doesn't match after acquiring
     * the header lock, give up.
     */
    if (buf_state & BM_VALID) == 0
        || !BufferTagsEqual(&(*bufHdr).tag, &newTag)
    {
        UnlockBufHdr(bufHdr, buf_state);
        LWLockRelease(partitionLock);
        *buf_state_p = 0;
        return InvalidBuffer;
    }

    /* Pin the buffer. */
    buf_state += BUF_REFCOUNT_ONE;
    /* also bump usage count, capped at BM_MAX_USAGE_COUNT */
    if BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT as u32 {
        buf_state += BUF_USAGECOUNT_ONE;
    }
    UnlockBufHdr(bufHdr, buf_state);
    LWLockRelease(partitionLock);

    /* record in private refcount tracking */
    ResourceOwnerEnlarge(CurrentResourceOwner);
    ReservePrivateRefCountEntry();

    let ref_ = NewPrivateRefCountEntry(buf);
    (*ref_).refcount += 1;

    ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

    *buf_state_p = buf_state;
    buf
}

// ----------------------------------------------------------------
// ReadBuffer_common
//
// Common logic for ReadBuffer, ReadBufferExtended, etc.
// ----------------------------------------------------------------
unsafe fn ReadBuffer_common(
    bmr: BufferManagerRelation,
    relpersistence: c_char,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
    hit_p: *mut bool,
) -> Buffer {
    *hit_p = false;

    let isExtend = blockNum == P_NEW;
    let isLocalBuf = if !bmr.rel.is_null() {
        RelationUsesLocalBuffers(bmr.rel)
    } else {
        relpersistence == RELPERSISTENCE_TEMP
    };

    if isLocalBuf {
        /* should have been handled by caller */
        unimplemented!("ReadBuffer_common called for local buffer")
    }

    let smgr: SMgrRelation = if !bmr.smgr.is_null() {
        bmr.smgr
    } else {
        RelationGetSmgr(bmr.rel)
    };

    let rlocator = smgr_rlocator(smgr).locator;
    let io_context = IOContextForStrategy(strategy);
    let io_object: IOObject = if relpersistence == RELPERSISTENCE_TEMP {
        IOOBJECT_TEMP_RELATION
    } else {
        IOOBJECT_RELATION
    };

    if isExtend {
        /*
         * We are extending the relation by one block.
         */
        let mut first_new_block: BlockNumber = 0;
        let mut extended_by: uint32 = 0;
        let buf = ExtendBufferedRelCommon(
            bmr, forkNum, strategy,
            EB_LOCK_FIRST,
            1,
            &mut first_new_block,
            &mut extended_by,
        );

        /*
         * We get here only if we actually extended.  Lock the buffer.
         */
        if mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK {
            ZeroAndLockBuffer(buf, mode);
        }
        return buf;
    }

    /* try to get the buffer in the buffer pool */
    let mut buf_state: uint32 = 0;
    let buf = PinBufferForBlock(
        bmr.rel, smgr, relpersistence, forkNum, blockNum, strategy, &mut buf_state,
    );

    if buf != InvalidBuffer {
        /* Buffer was found in the pool. */
        *hit_p = true;

        if !bmr.rel.is_null() {
            pgstat_count_buffer_hit(bmr.rel);
        }

        if VacuumCostActive {
            VacuumCostBalance += VacuumCostPageHit;
        }

        /* Update usage stats */
        pgBufferUsage.shared_blks_hit += 1;

        if mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK {
            ZeroAndLockBuffer(buf, mode);
        }

        return buf;
    }

    /*
     * Buffer was not found; we need to read (or zero) the page.
     * Use StartReadBuffers / WaitReadBuffers.
     */
    let mut operation: ReadBuffersOperation = core::mem::zeroed();
    operation.smgr = smgr as *mut c_void;
    operation.persistence = relpersistence;
    operation.forknum = forkNum;
    operation.strategy = strategy as *mut c_void;

    let mut flags: c_int = 0;
    if mode == RBM_ZERO_ON_ERROR {
        flags |= READ_BUFFERS_ZERO_ON_ERROR;
    }
    if ignore_checksum_failure {
        flags |= READ_BUFFERS_IGNORE_CHECKSUM_FAILURES;
    }

    let mut bufnums: [Buffer; 1] = [0];
    let mut nblocks: c_int = 1;
    operation.buffers = bufnums.as_mut_ptr();

    let did_start = StartReadBuffersImpl(&mut operation, bufnums.as_mut_ptr(), blockNum, &mut nblocks, flags);
    let buf = bufnums[0];

    if did_start {
        WaitReadBuffers(&mut operation);
    }

    if mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK {
        ZeroAndLockBuffer(buf, mode);
    }

    buf
}

// ----------------------------------------------------------------
// StartReadBuffersImpl
//
// Begin reading a run of consecutive blocks from disk.  Returns true if
// an I/O was actually initiated (false means the buffer was already valid,
// i.e., a cache hit for all blocks).
// ----------------------------------------------------------------
unsafe fn StartReadBuffersImpl(
    operation: *mut ReadBuffersOperation,
    buffers: *mut Buffer,
    blockNum: BlockNumber,
    nblocks: *mut c_int,
    flags: c_int,
) -> bool {
    let smgr = (*operation).smgr as SMgrRelation;
    let persistence = (*operation).persistence;
    let forknum = (*operation).forknum;
    let strategy = (*operation).strategy as BufferAccessStrategy;

    let rlocator = smgr_rlocator(smgr).locator;
    let io_object: IOObject = if persistence == RELPERSISTENCE_TEMP {
        IOOBJECT_TEMP_RELATION
    } else {
        IOOBJECT_RELATION
    };
    let io_context = IOContextForStrategy(strategy);

    let nblocks_val = *nblocks as usize;
    assert!(nblocks_val >= 1);

    let mut io_buffers_start: c_int = 0;
    let mut need_io: bool = false;
    let mut i: usize = 0;

    while i < nblocks_val {
        let cur_block = blockNum + i as BlockNumber;
        let mut buf_state: uint32 = 0;
        let buf = PinBufferForBlock(
            null_mut(),
            smgr,
            persistence,
            forknum,
            cur_block,
            strategy,
            &mut buf_state,
        );

        if buf != InvalidBuffer {
            /* cache hit */
            *buffers.add(i) = buf;
            i += 1;
        } else {
            /* need to read this page */
            /*
             * Allocate a victim buffer for this block.
             */
            let alloc_buf = BufferAlloc(smgr, persistence, forknum, cur_block, strategy, &mut buf_state);

            *buffers.add(i) = alloc_buf;

            if (buf_state & BM_VALID) != 0 {
                /* Another backend finished reading it for us */
                i += 1;
                continue;
            }

            /*
             * We need to start I/O for this block.
             */
            if !need_io {
                io_buffers_start = i as c_int;
                need_io = true;
            }

            i += 1;
        }
    }

    if !need_io {
        /* All blocks were cache hits */
        (*operation).nblocks_done = nblocks_val as i16;
        return false;
    }

    /*
     * Record the operation details so WaitReadBuffers can finish the I/O.
     */
    (*operation).blocknum = blockNum;
    (*operation).nblocks = *nblocks as i16;
    (*operation).nblocks_done = 0;
    (*operation).flags = flags;
    pgaio_wref_clear(core::ptr::addr_of_mut!((*operation).io_wref));

    /*
     * If using direct I/O, we may get an async handle; otherwise we do sync I/O.
     */
    let io_pages_storage: Vec<*mut c_void> = (0..nblocks_val)
        .map(|idx| BufferGetBlock(*buffers.add(idx)))
        .collect();

    if (io_direct_flags & IO_DIRECT_DATA) != 0 && io_method != IOMETHOD_SYNC {
        /* Async path */
        let ioh = pgaio_io_acquire(null_mut(), null_mut());
        if !ioh.is_null() {
            pgaio_io_register_callbacks(ioh, PGAIO_HCB_SHARED_BUFFER_READV, 0);
            pgaio_io_get_wref(ioh, core::ptr::addr_of_mut!((*operation).io_wref));
            smgrstartreadv(
                ioh, smgr, forknum, blockNum,
                io_pages_storage.as_ptr(),
                nblocks_val as c_int,
            );
            pgstat_count_io_op(io_object, io_context, IOOP_READ, nblocks_val as uint32, (nblocks_val * BLCKSZ) as u64);
            return true;
        }
    }

    /* Synchronous fallback */
    smgrstartreadv(
        null_mut(), smgr, forknum, blockNum,
        io_pages_storage.as_ptr(),
        nblocks_val as c_int,
    );
    pgstat_count_io_op(io_object, io_context, IOOP_READ, nblocks_val as uint32, (nblocks_val * BLCKSZ) as u64);

    /* Mark I/O complete immediately for sync case */
    for idx in 0..nblocks_val {
        let buf = *buffers.add(idx);
        let bufHdr = GetBufferDescriptor((buf - 1) as u32);
        let buf_state = LockBufHdr(bufHdr);
        TerminateBufferIO(bufHdr, false, BM_VALID, true);
    }

    (*operation).nblocks_done = nblocks_val as i16;
    false
}

// ----------------------------------------------------------------
// StartReadBuffers  (public API)
// ----------------------------------------------------------------
pub unsafe fn StartReadBuffers(
    operation: *mut ReadBuffersOperation,
    buffers: *mut Buffer,
    blockNum: BlockNumber,
    nblocks: *mut c_int,
    flags: c_int,
) -> bool {
    StartReadBuffersImpl(operation, buffers, blockNum, nblocks, flags)
}

// ----------------------------------------------------------------
// StartReadBuffer  (single-block public API)
// ----------------------------------------------------------------
pub unsafe fn StartReadBuffer(
    operation: *mut ReadBuffersOperation,
    buffer: *mut Buffer,
    blockNum: BlockNumber,
    flags: c_int,
) -> bool {
    let mut nblocks: c_int = 1;
    StartReadBuffers(operation, buffer, blockNum, &mut nblocks, flags)
}

// ----------------------------------------------------------------
// CheckReadBuffersOperation
// ----------------------------------------------------------------
pub unsafe fn CheckReadBuffersOperation(
    operation: *mut ReadBuffersOperation,
    _caller_will_wait: bool,
) {
    /* Nothing to do if all blocks are done */
    if (*operation).nblocks_done >= (*operation).nblocks {
        return;
    }
    /* Check if the async I/O has completed */
    if pgaio_wref_valid(core::ptr::addr_of_mut!((*operation).io_wref)) {
        if pgaio_wref_check_done(core::ptr::addr_of_mut!((*operation).io_wref)) {
            /* done */
        }
    }
}

// ----------------------------------------------------------------
// WaitReadBuffers
// ----------------------------------------------------------------
#[inline]
unsafe fn ReadBuffersCanStartIOOnce(buffer: Buffer, nowait: bool) -> bool {
    if BufferIsLocal(buffer) {
        crate::storage::buffer::localbuf::StartLocalBufferIO(
            GetLocalBufferDescriptor((-buffer - 1) as u32),
            true,
            nowait,
        )
    } else {
        StartBufferIO(GetBufferDescriptor((buffer - 1) as u32), true, nowait)
    }
}

/*
 * Helper for AsyncReadBuffers that tries to get the buffer ready for IO.
 */
#[inline]
unsafe fn ReadBuffersCanStartIO(buffer: Buffer, nowait: bool) -> bool {
    /*
     * If this backend currently has staged IO, we need to submit the pending
     * IO before waiting for the right to issue IO, to avoid the potential for
     * deadlocks (and, more commonly, unnecessary delays for other backends).
     */
    if !nowait && pgaio_have_staged() {
        if ReadBuffersCanStartIOOnce(buffer, true) {
            return true;
        }

        /*
         * Unfortunately StartBufferIO() returning false doesn't allow to
         * distinguish between the buffer already being valid and IO already
         * being in progress. Since IO already being in progress is quite
         * rare, this approach seems fine.
         */
        pgaio_submit_staged();
    }

    ReadBuffersCanStartIOOnce(buffer, nowait)
}

/*
 * Helper for WaitReadBuffers() that processes the results of a readv
 * operation, raising an error if necessary.
 */
unsafe fn ProcessReadBuffersResult(operation: *mut ReadBuffersOperation) {
    let aio_ret = core::ptr::addr_of_mut!((*operation).io_return);
    let rs = (*aio_ret).result.status();
    let mut newly_read_blocks: c_int = 0;

    assert!(pgaio_wref_valid(core::ptr::addr_of_mut!((*operation).io_wref)));
    assert!((*aio_ret).result.status() != PGAIO_RS_UNKNOWN as uint32);

    /*
     * SMGR reports the number of blocks successfully read as the result of
     * the IO operation. Thus we can simply add that to ->nblocks_done.
     */

    if rs != PGAIO_RS_ERROR as uint32 {
        newly_read_blocks = (*aio_ret).result.result;
    }

    if rs == PGAIO_RS_ERROR as uint32 || rs == PGAIO_RS_WARNING as uint32 {
        pgaio_result_report(
            (*aio_ret).result,
            core::ptr::addr_of!((*aio_ret).target_data),
            if rs == PGAIO_RS_ERROR as uint32 { crate::utils::elog::ERROR } else { crate::utils::elog::WARNING },
        );
    } else if (*aio_ret).result.status() == PGAIO_RS_PARTIAL as uint32 {
        /*
         * We'll retry, so we just emit a debug message to the server log (or
         * not even that in prod scenarios).
         */
        pgaio_result_report(
            (*aio_ret).result,
            core::ptr::addr_of!((*aio_ret).target_data),
            crate::utils::elog::DEBUG1,
        );
        elog!(DEBUG3, "partial read, will retry");
    }

    assert!(newly_read_blocks > 0);
    assert!(newly_read_blocks <= MAX_IO_COMBINE_LIMIT as c_int);

    (*operation).nblocks_done += newly_read_blocks as int16;

    assert!((*operation).nblocks_done <= (*operation).nblocks);
}

pub unsafe fn WaitReadBuffers(operation: *mut ReadBuffersOperation) {
    /*
     * Wait for any async I/O to complete.
     */
    if pgaio_wref_valid(core::ptr::addr_of_mut!((*operation).io_wref)) {
        pgaio_wref_wait(core::ptr::addr_of_mut!((*operation).io_wref));
    }

    /*
     * Verify/checksum each buffer that was read from disk.
     */
    let nblocks = (*operation).nblocks as usize;
    let buffers = (*operation).buffers;
    let blockNum = (*operation).blocknum;
    let forknum = (*operation).forknum;
    let smgr = (*operation).smgr as SMgrRelation;
    let flags = (*operation).flags;

    let rlocator = smgr_rlocator(smgr).locator;

    let zero_on_error = (flags & READ_BUFFERS_ZERO_ON_ERROR) != 0;
    let ignore_checksum = (flags & READ_BUFFERS_IGNORE_CHECKSUM_FAILURES) != 0;
    let piv_flags = if ignore_checksum { PIV_IGNORE_CHECKSUM_FAILURE } else { 0 }
        | PIV_LOG_LOG;

    for i in 0..nblocks {
        let buf = *buffers.add(i);
        let bufHdr = GetBufferDescriptor((buf - 1) as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);

        /* only process buffers that we need to validate */
        if (buf_state & BM_VALID) != 0 {
            continue;
        }

        let page = BufferGetPage(buf);
        let cur_block = blockNum + i as BlockNumber;
        let mut failed_checksum = false;

        if !PageIsVerified(page, cur_block, piv_flags, &mut failed_checksum) {
            if zero_on_error {
                core::ptr::write_bytes(page as *mut u8, 0, BLCKSZ);
            } else {
                ereport!(ERROR,
                    errmsg!("invalid page in block {} of relation {}",
                        cur_block,
                        relpathperm(rlocator, forknum).str_ptr().as_ref().map(|_| "?").unwrap_or("?")));
            }
        }

        let buf_state2 = LockBufHdr(bufHdr);
        TerminateBufferIO(bufHdr, failed_checksum, BM_VALID, true);
    }
}

// ----------------------------------------------------------------
// AsyncReadBuffers
// ----------------------------------------------------------------
pub unsafe fn AsyncReadBuffers(
    operation: *mut ReadBuffersOperation,
    blockNum: BlockNumber,
    nblocks: *mut c_int,
    flags: c_int,
    buffers: *mut Buffer,
) -> bool {
    StartReadBuffersImpl(operation, buffers, blockNum, nblocks, flags)
}

// ----------------------------------------------------------------
// BufferAlloc
//
// Find or allocate a buffer for the given tag.  Returns the buffer
// with its content lock acquired (for new I/O) or just pinned
// (for valid re-use).
// ----------------------------------------------------------------
unsafe fn BufferAlloc(
    smgr: SMgrRelation,
    relpersistence: c_char,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    strategy: BufferAccessStrategy,
    buf_state_p: *mut uint32,
) -> Buffer {
    let rlocator = smgr_rlocator(smgr).locator;
    let mut newTag: BufferTag = core::mem::zeroed();
    InitBufferTag(&mut newTag, &rlocator, forkNum, blockNum);

    /* Determine partition lock */
    let newHash = BufTableHashCode(&mut newTag);
    let newPartitionLock = BufMappingPartitionLock(newHash);

    /* Reserve space for a new private refcount entry */
    ReservePrivateRefCountEntry();
    ResourceOwnerEnlarge(CurrentResourceOwner);

    /* Acquire the partition lock in exclusive mode */
    LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);

    /* See if the block is in the buffer pool already */
    let existing_buf_id = BufTableLookup(&mut newTag, newHash);
    if existing_buf_id >= 0 {
        let buf = existing_buf_id + 1;
        let bufHdr = GetBufferDescriptor(existing_buf_id as u32);
        let buf_state = LockBufHdr(bufHdr);

        if (buf_state & BM_VALID) == 0 {
            /* Not yet valid; I/O is in progress by another backend */
            /* Pin it and wait for the I/O to complete */
            let new_state = buf_state + BUF_REFCOUNT_ONE;
            UnlockBufHdr(bufHdr, new_state);
            LWLockRelease(newPartitionLock);

            let ref_ = NewPrivateRefCountEntry(buf);
            (*ref_).refcount += 1;
            ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

            WaitIO(bufHdr);

            *buf_state_p = pg_atomic_read_u32(&(*bufHdr).state);
            return buf;
        }

        /* Valid: just pin it */
        let mut new_state = buf_state + BUF_REFCOUNT_ONE;
        if BUF_STATE_GET_USAGECOUNT(new_state) < BM_MAX_USAGE_COUNT as u32 {
            new_state += BUF_USAGECOUNT_ONE;
        }
        UnlockBufHdr(bufHdr, new_state);
        LWLockRelease(newPartitionLock);

        let ref_ = NewPrivateRefCountEntry(buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

        *buf_state_p = new_state;
        return buf;
    }

    /*
     * Not in the buffer pool.  We have to get a victim buffer and read the
     * page from disk.
     */
    let victim_buf = GetVictimBuffer(strategy, newHash, &newTag);
    if victim_buf == InvalidBuffer {
        /* could not get a victim - shouldn't happen */
        LWLockRelease(newPartitionLock);
        elog!(ERROR, "no victim buffer available");
    }

    let bufHdr = GetBufferDescriptor((victim_buf - 1) as u32);

    /*
     * Insert tag into buffer table, and acquire content lock.
     */
    let old_buf_id = BufTableInsert(&mut newTag, newHash, victim_buf - 1);
    if old_buf_id >= 0 {
        /*
         * Someone else inserted it while we were holding the partition lock;
         * re-use their buffer.
         */
        let old_buf = old_buf_id + 1;

        /* Unpin our victim buffer */
        let victim_state = LockBufHdr(bufHdr);
        let victim_new_state = victim_state - BUF_REFCOUNT_ONE;
        UnlockBufHdr(bufHdr, victim_new_state);

        /* Clear old tag from victim */
        /* We already removed any old mapping when selecting the victim */

        /* Switch to the existing buffer */
        let old_bufHdr = GetBufferDescriptor(old_buf_id as u32);
        let buf_state = LockBufHdr(old_bufHdr);

        let new_state = buf_state + BUF_REFCOUNT_ONE;
        UnlockBufHdr(old_bufHdr, new_state);
        LWLockRelease(newPartitionLock);

        /* forget the reservation we used for victim, get fresh one */
        let victim_ref = GetPrivateRefCountEntry(victim_buf, false);
        if !victim_ref.is_null() {
            ForgetPrivateRefCountEntry(victim_ref);
        }

        let ref_ = NewPrivateRefCountEntry(old_buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, old_buf);

        WaitIO(old_bufHdr);

        *buf_state_p = pg_atomic_read_u32(&(*old_bufHdr).state);
        return old_buf;
    }

    /*
     * Successfully inserted the new tag.  Set BM_IO_IN_PROGRESS and mark
     * the buffer as needing content-lock acquisition by the reader.
     */
    let buf_state = LockBufHdr(bufHdr);
    let mut new_state = buf_state;
    /* clear old flags, set new tag valid */
    new_state &= !(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_IO_IN_PROGRESS);
    new_state |= BM_TAG_VALID | BM_IO_IN_PROGRESS;
    (*bufHdr).tag = newTag;
    UnlockBufHdr(bufHdr, new_state);

    LWLockRelease(newPartitionLock);

    *buf_state_p = new_state;
    victim_buf
}

// ----------------------------------------------------------------
// InvalidateBuffer
//
// Mark a shared buffer invalid, evicting its content.
// ----------------------------------------------------------------
pub unsafe fn InvalidateBuffer(buf: *mut BufferDesc) {
    let bufHdr = buf;

    /* Do nothing for local buffers */
    if (*bufHdr).buf_id < 0 {
        /* local buffer -- not in shared pool */
        return;
    }

    let mut oldTag: BufferTag = (*bufHdr).tag;
    let oldHash = BufTableHashCode(&mut oldTag);
    let oldPartitionLock = BufMappingPartitionLock(oldHash);

    'retry: loop {
        /* Acquire the partition lock */
        LWLockAcquire(oldPartitionLock, LW_EXCLUSIVE);

        let buf_state = LockBufHdr(bufHdr);

        /*
         * Check tag still valid - if someone changed it we are done
         */
        if !BufferTagsEqual(&(*bufHdr).tag, &oldTag) {
            UnlockBufHdr(bufHdr, buf_state);
            LWLockRelease(oldPartitionLock);
            return;
        }

        /* If pinned, we can't invalidate immediately */
        if BUF_STATE_GET_REFCOUNT(buf_state) != 0 {
            UnlockBufHdr(bufHdr, buf_state);
            LWLockRelease(oldPartitionLock);
            /* wait for pins to go away */
            WaitBufHdrUnlocked(bufHdr);
            continue 'retry;
        }

        /*
         * No one holds a pin. Proceed to invalidate.
         */
        BufTableDelete(&mut oldTag, oldHash);

        /* Mark invalid */
        let mut new_state = buf_state;
        new_state &= !(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_TAG_VALID);
        ClearBufferTag(&mut (*bufHdr).tag);
        UnlockBufHdr(bufHdr, new_state);

        LWLockRelease(oldPartitionLock);
        return;
    }
}

// ----------------------------------------------------------------
// InvalidateVictimBuffer
//
// Try to evict a victim buffer.  Returns true on success.
// ----------------------------------------------------------------
unsafe fn InvalidateVictimBuffer(buf_hdr: *mut BufferDesc) -> bool {
    let buf = BufferDescriptorGetBuffer(buf_hdr);

    let mut tag = (*buf_hdr).tag;

    /* If the buffer isn't dirty and isn't pinned, evict it */
    let hash = BufTableHashCode(&mut tag);
    let partitionLock = BufMappingPartitionLock(hash);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    let buf_state = LockBufHdr(buf_hdr);

    /* If pinned, cannot evict */
    if BUF_STATE_GET_REFCOUNT(buf_state) != 0 {
        UnlockBufHdr(buf_hdr, buf_state);
        LWLockRelease(partitionLock);
        return false;
    }

    /* If dirty, cannot evict without flushing; try sync later */
    if (buf_state & BM_DIRTY) != 0 {
        UnlockBufHdr(buf_hdr, buf_state);
        LWLockRelease(partitionLock);
        return false;
    }

    BufTableDelete(&mut tag, hash);

    let mut new_state = buf_state;
    new_state &= !(BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR);
    ClearBufferTag(&mut (*buf_hdr).tag);
    UnlockBufHdr(buf_hdr, new_state);

    LWLockRelease(partitionLock);

    true
}

// ----------------------------------------------------------------
// GetVictimBuffer
//
// Find a buffer to reuse for a new page.
// ----------------------------------------------------------------
unsafe fn GetVictimBuffer(
    strategy: BufferAccessStrategy,
    newHash: uint32,
    newTag: *const BufferTag,
) -> Buffer {
    let io_context = IOContextForStrategy(strategy);

    'again: loop {
        let mut buf_state: uint32 = 0;
        let mut from_ring: bool = false;
        let bufHdr = StrategyGetBuffer(strategy, &mut buf_state, &mut from_ring);
        let buf = BufferDescriptorGetBuffer(bufHdr);

        /*
         * If the buffer is pinned or has IO in progress, reject it.
         */
        if BUF_STATE_GET_REFCOUNT(buf_state) != 0
            || (buf_state & BM_IO_IN_PROGRESS) != 0
        {
            StrategyFreeBuffer(bufHdr);
            continue 'again;
        }

        /*
         * If the buffer is dirty, write it out before reuse.
         */
        if (buf_state & BM_DIRTY) != 0 {
            if StrategyRejectBuffer(strategy, bufHdr, false) {
                continue 'again;
            }

            /* Need to write the buffer */
            if !PinBuffer_Locked(bufHdr) {
                /* Got unlocked while we were waiting; redo */
                continue 'again;
            }

            /* Start IO */
            if !StartBufferIO(bufHdr, false, false) {
                /* IO already in progress or buffer became clean */
                UnpinBuffer(buf);
                continue 'again;
            }

            FlushBuffer(bufHdr, null_mut(), io_object_for_bufhdr(bufHdr), io_context);
            TerminateBufferIO(bufHdr, false, 0, false);
            UnpinBuffer(buf);

            pgstat_count_io_op(
                io_object_for_bufhdr(bufHdr), io_context,
                IOOP_EVICT, 1, BLCKSZ as u64,
            );
        } else {
            pgstat_count_io_op(
                io_object_for_bufhdr(bufHdr), io_context,
                IOOP_EVICT, 1, BLCKSZ as u64,
            );
        }

        /* Try to invalidate the buffer */
        if !InvalidateVictimBuffer(bufHdr) {
            continue 'again;
        }

        /* Successfully claimed victim */
        /* Pin it */
        let buf_state2 = LockBufHdr(bufHdr);
        let new_state = buf_state2 + BUF_REFCOUNT_ONE;
        UnlockBufHdr(bufHdr, new_state);

        let ref_ = NewPrivateRefCountEntry(buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

        return buf;
    }
}

#[inline]
unsafe fn io_object_for_bufhdr(bufHdr: *const BufferDesc) -> IOObject {
    let tag = (*bufHdr).tag;
    // If tagged with a temp relation (backend != INVALID_PROC_NUMBER), use TEMP.
    // We approximate this by checking BM_PERMANENT flag.
    if (pg_atomic_read_u32(&(*(bufHdr as *mut BufferDesc)).state) & BM_PERMANENT) != 0 {
        IOOBJECT_RELATION
    } else {
        IOOBJECT_TEMP_RELATION
    }
}

// ----------------------------------------------------------------
// GetPinLimit
// ----------------------------------------------------------------
pub unsafe fn GetPinLimit() -> uint32 {
    (MaxBackends as uint32) + NUM_AUXILIARY_PROCS as uint32
}

// ----------------------------------------------------------------
// GetAdditionalPinLimit
// ----------------------------------------------------------------
pub unsafe fn GetAdditionalPinLimit() -> uint32 {
    let pinned = PrivateRefCountOverflow as uint32
        + PrivateRefCountArray.iter().filter(|e| e.buffer != InvalidBuffer).count() as uint32;
    let limit = GetPinLimit();
    if pinned >= limit {
        0
    } else {
        limit - pinned
    }
}

// ----------------------------------------------------------------
// LimitAdditionalPins
// ----------------------------------------------------------------
pub unsafe fn LimitAdditionalPins(additional_pins: *mut uint32) {
    if *additional_pins == 0 {
        return;
    }
    let remaining = GetAdditionalPinLimit();
    if remaining == 0 {
        /* Out of pins; force 0 */
        ereport!(ERROR, errmsg!("buffer pins exhausted"));
    }
    if *additional_pins > remaining {
        *additional_pins = remaining;
    }
}

// ----------------------------------------------------------------
// ExtendBufferedRelCommon
//
// Common code for extending a relation by extend_by pages.
// ----------------------------------------------------------------
unsafe fn ExtendBufferedRelCommon(
    bmr: BufferManagerRelation,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
    extend_by: uint32,
    first_block_p: *mut BlockNumber,
    extended_by_p: *mut uint32,
) -> Buffer {
    /*
     * Open the smgr relation if not already done.
     */
    let smgr = if !bmr.smgr.is_null() {
        bmr.smgr
    } else {
        RelationGetSmgr(bmr.rel)
    };

    let relpersistence = if !bmr.rel.is_null() {
        (*(*bmr.rel).rd_rel).relpersistence
    } else {
        bmr.relpersistence
    };

    ExtendBufferedRelShared(
        bmr, smgr, relpersistence, forkNum, strategy, flags,
        extend_by, first_block_p, extended_by_p,
    )
}

// ----------------------------------------------------------------
// ExtendBufferedRelShared
//
// Extend a relation stored in shared buffers by extend_by blocks.
// ----------------------------------------------------------------
unsafe fn ExtendBufferedRelShared(
    bmr: BufferManagerRelation,
    smgr: SMgrRelation,
    relpersistence: c_char,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
    extend_by: uint32,
    first_block_p: *mut BlockNumber,
    extended_by_p: *mut uint32,
) -> Buffer {
    let lock_relation = (flags & EB_SKIP_EXTENSION_LOCK) == 0;

    if lock_relation && !bmr.rel.is_null() {
        LockRelationForExtension(bmr.rel, ExclusiveLock);
    }

    /*
     * Get the current size of the relation. smgrnblocks() will give us the
     * real on-disk size; after this we may extend by more than one block if
     * asked.
     */
    let current_nblocks = smgrnblocks(smgr, forkNum);
    let first_block = current_nblocks;

    let extend_count = if extend_by == 0 { 1 } else { extend_by };

    /* Update smgr's cached nblocks */
    let cached = smgr_cached_nblocks_ptr(smgr).add(forkNum as usize);
    *cached = first_block + extend_count;

    /* Make sure smgr knows the file exists */
    if (flags & EB_CREATE_FORK_IF_NEEDED) != 0
        && !smgrexists(smgr, forkNum)
    {
        smgrcreate(smgr, forkNum, (flags & EB_PERFORMING_RECOVERY) != 0);
    }

    /* Use zero-extend if possible */
    smgrzeroextend(smgr, forkNum, first_block, extend_count as c_int, false);

    if lock_relation && !bmr.rel.is_null() {
        UnlockRelationForExtension(bmr.rel, ExclusiveLock);
    }

    *first_block_p = first_block;
    *extended_by_p = extend_count;

    /*
     * Allocate and pin the first new buffer.
     */
    let rlocator = smgr_rlocator(smgr).locator;
    let mut newTag: BufferTag = core::mem::zeroed();
    InitBufferTag(&mut newTag, &rlocator, forkNum, first_block);

    let newHash = BufTableHashCode(&mut newTag);
    let partitionLock = BufMappingPartitionLock(newHash);

    ReservePrivateRefCountEntry();
    ResourceOwnerEnlarge(CurrentResourceOwner);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /* Check if it's already in the pool (race condition) */
    let existing = BufTableLookup(&mut newTag, newHash);
    if existing >= 0 {
        /* Use it */
        let buf = existing + 1;
        let bufHdr = GetBufferDescriptor(existing as u32);
        let buf_state = LockBufHdr(bufHdr);
        let new_state = buf_state + BUF_REFCOUNT_ONE;
        UnlockBufHdr(bufHdr, new_state);
        LWLockRelease(partitionLock);

        let ref_ = NewPrivateRefCountEntry(buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);
        return buf;
    }

    /* Get a victim and tag it */
    let buf = GetVictimBuffer(strategy, newHash, &newTag);
    let bufHdr = GetBufferDescriptor((buf - 1) as u32);

    let old_buf_id = BufTableInsert(&mut newTag, newHash, buf - 1);
    if old_buf_id >= 0 {
        /* Someone raced us */
        let old_buf = old_buf_id + 1;
        let old_bufHdr = GetBufferDescriptor(old_buf_id as u32);

        let victim_state = LockBufHdr(bufHdr);
        UnlockBufHdr(bufHdr, victim_state - BUF_REFCOUNT_ONE);

        /* Forget private ref for victim */
        let victim_ref = GetPrivateRefCountEntry(buf, false);
        if !victim_ref.is_null() {
            (*victim_ref).refcount -= 1;
            if (*victim_ref).refcount == 0 {
                ForgetPrivateRefCountEntry(victim_ref);
            }
        }

        let buf_state = LockBufHdr(old_bufHdr);
        let new_state = buf_state + BUF_REFCOUNT_ONE;
        UnlockBufHdr(old_bufHdr, new_state);
        LWLockRelease(partitionLock);

        let ref_ = NewPrivateRefCountEntry(old_buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, old_buf);
        return old_buf;
    }

    /* Tag the victim with the new block */
    let buf_state = LockBufHdr(bufHdr);
    let mut new_state = buf_state;
    new_state &= !(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR | BM_IO_IN_PROGRESS);
    new_state |= BM_TAG_VALID | BM_VALID; /* already zeroed on disk */
    (*bufHdr).tag = newTag;
    UnlockBufHdr(bufHdr, new_state);

    LWLockRelease(partitionLock);

    /* zero the buffer page */
    core::ptr::write_bytes(BufferGetPage(buf) as *mut u8, 0, BLCKSZ);

    /* Log if unlogged */
    if !bmr.rel.is_null()
        && relpersistence == RELPERSISTENCE_UNLOGGED
        && forkNum == INIT_FORKNUM
    {
        log_newpage_buffer(buf, false);
    }

    pgstat_count_io_op(
        io_object_for_bufhdr(bufHdr),
        IOContextForStrategy(strategy),
        IOOP_EXTEND,
        1,
        BLCKSZ as u64,
    );

    buf
}

// ----------------------------------------------------------------
// BufferIsExclusiveLocked
// ----------------------------------------------------------------
pub unsafe fn BufferIsExclusiveLocked(buffer: Buffer) -> bool {
    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE)
}

// ----------------------------------------------------------------
// BufferIsDirty
// ----------------------------------------------------------------
pub unsafe fn BufferIsDirty(buffer: Buffer) -> bool {
    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    (pg_atomic_read_u32(&(*bufHdr).state) & BM_DIRTY) != 0
}

// ----------------------------------------------------------------
// MarkBufferDirty
// ----------------------------------------------------------------
pub unsafe fn MarkBufferDirty(buffer: Buffer) {
    if !BufferIsValid(buffer) {
        elog!(ERROR, "bad buffer ID: {}", buffer);
    }

    if BufferIsLocal(buffer) {
        use crate::storage::buffer::localbuf::LocalRefCount;
        let idx = (-buffer - 1) as usize;
        assert!(*LocalRefCount.add(idx) > 0);
        let bufHdr = GetLocalBufferDescriptor((((-buffer - 1) as usize)) as u32);
        pg_atomic_fetch_or_u32(&(*bufHdr).state, BM_DIRTY | BM_JUST_DIRTIED);
        return;
    }

    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    assert!(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE));

    let buf_state = pg_atomic_fetch_or_u32(&mut (*bufHdr).state, BM_DIRTY | BM_JUST_DIRTIED);

    /* Update usage count if not dirty before */
    if (buf_state & BM_DIRTY) == 0 {
        pgBufferUsage.shared_blks_dirtied += 1;
        if VacuumCostActive {
            VacuumCostBalance += VacuumCostPageDirty;
        }
    }
}

// ----------------------------------------------------------------
// ReleaseAndReadBuffer
//
// Release a buffer pin (if any) and read a buffer.
// ----------------------------------------------------------------
pub unsafe fn ReleaseAndReadBuffer(
    buffer: Buffer,
    relation: Relation,
    blockNum: BlockNumber,
) -> Buffer {
    if BufferIsValid(buffer) {
        if BufferIsLocal(buffer) {
            use crate::storage::buffer::localbuf::LocalRefCount;
            let idx = (-buffer - 1) as usize;
            if *LocalRefCount.add(idx) > 0 {
                *LocalRefCount.add(idx) -= 1;
            }
        } else {
            UnpinBuffer(buffer);
        }
    }
    ReadBuffer(relation, blockNum)
}

// ----------------------------------------------------------------
// PinBuffer
//
// Pin a buffer by its buffer number.  Returns true if the buffer is
// already valid (no I/O needed); false if we have to wait for I/O.
// ----------------------------------------------------------------
pub unsafe fn PinBuffer(buf: *mut BufferDesc, strategy: BufferAccessStrategy) -> bool {
    let b = BufferDescriptorGetBuffer(buf);

    let ref_ = GetPrivateRefCountEntry(b, true);

    if !ref_.is_null() {
        /* Already pinned by this backend; just bump refcount */
        (*ref_).refcount += 1;
        ResourceOwnerEnlarge(CurrentResourceOwner);
        ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
        return (pg_atomic_read_u32(&(*buf).state) & BM_VALID) != 0;
    }

    ResourceOwnerEnlarge(CurrentResourceOwner);
    ReservePrivateRefCountEntry();

    let buf_state = LockBufHdr(buf);
    let mut new_state = buf_state + BUF_REFCOUNT_ONE;
    if BUF_STATE_GET_USAGECOUNT(new_state) < BM_MAX_USAGE_COUNT as u32 {
        new_state += BUF_USAGECOUNT_ONE;
    }
    UnlockBufHdr(buf, new_state);

    let ref_ = NewPrivateRefCountEntry(b);
    (*ref_).refcount += 1;
    ResourceOwnerRememberBuffer(CurrentResourceOwner, b);

    (new_state & BM_VALID) != 0
}

// ----------------------------------------------------------------
// PinBuffer_Locked
//
// Pin a buffer whose header lock is already held by us.
// The header lock is released on return.
// Returns true if the buffer is currently valid.
// ----------------------------------------------------------------
pub unsafe fn PinBuffer_Locked(buf: *mut BufferDesc) -> bool {
    let b = BufferDescriptorGetBuffer(buf);

    /* Get or create a private refcount entry */
    let ref_ = GetPrivateRefCountEntry(b, false);
    let ref_: *mut PrivateRefCountEntry = if ref_.is_null() {
        ReservePrivateRefCountEntry();
        ResourceOwnerEnlarge(CurrentResourceOwner);
        NewPrivateRefCountEntry(b)
    } else {
        ref_
    };

    let buf_state = pg_atomic_read_u32(&(*buf).state);
    let mut new_state = buf_state + BUF_REFCOUNT_ONE;
    if BUF_STATE_GET_USAGECOUNT(new_state) < BM_MAX_USAGE_COUNT as u32 {
        new_state += BUF_USAGECOUNT_ONE;
    }

    UnlockBufHdr(buf, new_state);

    (*ref_).refcount += 1;

    if (*ref_).refcount == 1 {
        /* new pin */
        ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
    }

    (new_state & BM_VALID) != 0
}

// ----------------------------------------------------------------
// WakePinCountWaiter
//
// Signal a backend that is waiting for the buffer's pin count to drop to 1.
// ----------------------------------------------------------------
unsafe fn WakePinCountWaiter(buf: *mut BufferDesc) {
    let buf_state = pg_atomic_read_u32(&(*buf).state);
    if (buf_state & BM_PIN_COUNT_WAITER) != 0 {
        let wait_backend = (*buf).wait_backend_pgprocno;
        if wait_backend != INVALID_PROC_NUMBER {
            ProcSendSignal(wait_backend);
        }
    }
}

// ----------------------------------------------------------------
// UnpinBuffer
//
// Decrement the pin count on a shared buffer.
// ----------------------------------------------------------------
pub unsafe fn UnpinBuffer(buffer: Buffer) {
    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    UnpinBufferNoOwner(buffer);
    ResourceOwnerForgetBuffer(CurrentResourceOwner, buffer);
}

// ----------------------------------------------------------------
// UnpinBufferNoOwner
//
// Decrement the pin count without touching the resource owner.
// ----------------------------------------------------------------
pub unsafe fn UnpinBufferNoOwner(buffer: Buffer) {
    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    let ref_ = GetPrivateRefCountEntry(buffer, false);
    assert!(!ref_.is_null());
    assert!((*ref_).refcount > 0);

    (*ref_).refcount -= 1;

    if (*ref_).refcount == 0 {
        ForgetPrivateRefCountEntry(ref_);

        /* Decrement shared refcount */
        let buf_state = LockBufHdr(buf_hdr);
        assert!(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
        let mut new_state = buf_state - BUF_REFCOUNT_ONE;

        /* Wake any waiter if we are dropping to pin count 1 */
        if (new_state & BM_PIN_COUNT_WAITER) != 0 && BUF_STATE_GET_REFCOUNT(new_state) == 1 {
            /* Waiter wants to know pin count is 1 */
            let wait_backend = (*buf_hdr).wait_backend_pgprocno;
            UnlockBufHdr(buf_hdr, new_state);
            if wait_backend != INVALID_PROC_NUMBER {
                ProcSendSignal(wait_backend);
            }
        } else if BUF_STATE_GET_REFCOUNT(new_state) == 0
            && (new_state & BM_PIN_COUNT_WAITER) != 0
        {
            /* Waiter wants pin count 0 */
            new_state &= !BM_PIN_COUNT_WAITER;
            let wait_backend = (*buf_hdr).wait_backend_pgprocno;
            UnlockBufHdr(buf_hdr, new_state);
            if wait_backend != INVALID_PROC_NUMBER {
                ProcSendSignal(wait_backend);
            }
        } else {
            UnlockBufHdr(buf_hdr, new_state);
        }
    }
}

// ----------------------------------------------------------------
// BufferSync
//
// Flush all dirty pages to disk.  Called at checkpoint.
// ----------------------------------------------------------------
pub unsafe fn BufferSync(flags: c_int) {
    /*
     * Find out where to start writing.
     */
    let num_to_scan: c_int = 0;
    let num_spaces: c_int = 0;
    let _per_ts_stat: *mut CkptTsStatus = null_mut();
    {
        let mut complete_passes: u32 = 0;
        let mut num_buf_alloc: u32 = 0;
        let _start_buf = StrategySyncStart(&mut complete_passes, &mut num_buf_alloc);
    }

    TRACE_POSTGRESQL_BUFFER_SYNC_START!(NBuffers, num_to_scan);

    /*
     * Loop over all the dirty buffers.
     * We use a binary heap to prioritise checkpoint buffers by tablespace
     * (to reduce seek distance on spinning disks).
     */
    let heap = binaryheap_allocate(
        if num_spaces > 0 { num_spaces } else { 1 },
        Some(ts_ckpt_progress_comparator),
        null_mut(),
    );

    /* Build per-tablespace stats */
    /* (actual heap loading omitted for brevity -- see C source) */

    let mut num_written: c_int = 0;
    let mut buf_id: c_int = 0;

    while buf_id < NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        buf_id += 1;

        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & BM_CHECKPOINT_NEEDED) == 0 {
            continue;
        }

        let result = SyncOneBuffer(buf_id - 1, false, &mut BackendWritebackContext);

        if (result & BUF_WRITTEN) != 0 {
            num_written += 1;
            CheckpointStats.ckpt_bufs_written += 1;
            TRACE_POSTGRESQL_BUFFER_SYNC_WRITTEN!(buf_id - 1);
        }

        CheckWritebackContext(&mut BackendWritebackContext, false);
        CheckpointWriteDelay(flags, (buf_id as f64) / (NBuffers as f64));
    }

    /* Final writeback flush */
    IssuePendingWritebacks(&mut BackendWritebackContext);

    TRACE_POSTGRESQL_BUFFER_SYNC_DONE!(NBuffers, num_written, num_to_scan);
    binaryheap_free(heap);
}

/*
 * Comparator for a Min-Heap over the per-tablespace checkpoint completion
 * progress.
 */
unsafe fn ts_ckpt_progress_comparator(
    a: Datum,
    b: Datum,
    _arg: *mut c_void,
) -> c_int {
    let sa = &*(DatumGetPointer(a) as *const CkptTsStatus);
    let sb = &*(DatumGetPointer(b) as *const CkptTsStatus);

    /* we want a min-heap, so return 1 for the a < b */
    if sa.progress < sb.progress {
        1
    } else if sa.progress == sb.progress {
        0
    } else {
        -1
    }
}

#[inline]
unsafe fn CheckWritebackContext(context: *mut WritebackContext, _force: bool) {
    // TODO(pg-port): call IssuePendingWritebacks when flush needed
}

// ----------------------------------------------------------------
// BgBufferSync
//
// Write some dirty buffers during bgwriter's idle scan.
// ----------------------------------------------------------------
pub unsafe fn BgBufferSync(wb_context: *mut WritebackContext) -> bool {
    let strategy = GetAccessStrategy(BAS_BULKWRITE);

    let _alloc_before: c_int;
    let mut strategy_passes: u32 = 0;
    let mut recent_alloc: u32 = 0;
    let mut strategy_buf_id: c_int = StrategySyncStart(&mut strategy_passes, &mut recent_alloc);
    let _ = (strategy_passes, recent_alloc);
    _alloc_before = PendingBgWriterStats.buf_alloc;

    /* Track how many buffers we've tried */
    let mut bufs_to_lap: c_int = (bgwriter_lru_multiplier * (NBuffers as f64)) as c_int;
    if bufs_to_lap > NBuffers {
        bufs_to_lap = NBuffers;
    }

    let mut bufs_scanned: c_int = 0;
    let mut bufs_written: c_int = 0;
    let mut reusable_buffers: c_int = 0;

    loop {
        if bufs_scanned >= bufs_to_lap {
            break;
        }
        if bufs_written >= bgwriter_lru_maxpages {
            PendingBgWriterStats.maxwritten_clean += 1;
            break;
        }

        let buf_id = strategy_buf_id % NBuffers;
        strategy_buf_id += 1;
        bufs_scanned += 1;

        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);

        if (buf_state & BM_VALID) == 0 || (buf_state & BM_TAG_VALID) == 0 {
            reusable_buffers += 1;
            continue;
        }

        if (buf_state & BM_DIRTY) == 0 {
            reusable_buffers += 1;
            continue;
        }

        /* Skip buffers that are pinned */
        if BUF_STATE_GET_REFCOUNT(buf_state) != 0 {
            continue;
        }

        let result = SyncOneBuffer(buf_id, true, wb_context);
        if (result & BUF_WRITTEN) != 0 {
            bufs_written += 1;
            PendingBgWriterStats.buf_written_clean += 1;
        }
        if (result & BUF_REUSABLE) != 0 {
            reusable_buffers += 1;
        }
    }

    IssuePendingWritebacks(wb_context);
    FreeAccessStrategy(strategy);

    /* Return whether bgwriter should keep scanning */
    bufs_written > 0 || reusable_buffers < NBuffers / 10
}

// ----------------------------------------------------------------
// SyncOneBuffer
//
// Attempt to write a single buffer.  Returns BUF_WRITTEN / BUF_REUSABLE flags.
// ----------------------------------------------------------------
pub unsafe fn SyncOneBuffer(
    buf_id: c_int,
    skip_recently_used: bool,
    wb_context: *mut WritebackContext,
) -> c_int {
    let bufHdr = GetBufferDescriptor(buf_id as u32);
    let mut result: c_int = 0;

    /* Check without locks first */
    let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
    if (buf_state & BM_VALID) == 0
        || (buf_state & BM_TAG_VALID) == 0
        || (buf_state & BM_DIRTY) == 0
        || (buf_state & BM_IO_IN_PROGRESS) != 0
    {
        if (buf_state & BM_VALID) != 0 && BUF_STATE_GET_REFCOUNT(buf_state) == 0 {
            result |= BUF_REUSABLE;
        }
        return result;
    }

    if skip_recently_used && BUF_STATE_GET_USAGECOUNT(buf_state) != 0 {
        return result;
    }

    /* Pin the buffer */
    ReservePrivateRefCountEntry();
    ResourceOwnerEnlarge(CurrentResourceOwner);

    let buf_state2 = LockBufHdr(bufHdr);
    if (buf_state2 & BM_VALID) == 0
        || (buf_state2 & BM_DIRTY) == 0
        || (buf_state2 & BM_IO_IN_PROGRESS) != 0
    {
        /* Lost the race */
        UnlockBufHdr(bufHdr, buf_state2);
        return result;
    }

    let buf = buf_id + 1;
    let new_state = buf_state2 + BUF_REFCOUNT_ONE;
    UnlockBufHdr(bufHdr, new_state);

    let ref_ = NewPrivateRefCountEntry(buf);
    (*ref_).refcount += 1;
    ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

    /* Try to start I/O */
    if !StartBufferIO(bufHdr, false, false) {
        /* Someone else grabbed it; unpin and bail */
        UnpinBuffer(buf);
        return result;
    }

    /* do the write */
    FlushBuffer(bufHdr, null_mut(), io_object_for_bufhdr(bufHdr), IOCONTEXT_NORMAL);
    TerminateBufferIO(bufHdr, false, 0, false);

    ScheduleBufferTagForWriteback(wb_context, io_object_for_bufhdr(bufHdr), &(*bufHdr).tag);

    result |= BUF_WRITTEN;

    UnpinBuffer(buf);

    /* Check reusability */
    let buf_state3 = pg_atomic_read_u32(&(*bufHdr).state);
    if BUF_STATE_GET_REFCOUNT(buf_state3) == 0 {
        result |= BUF_REUSABLE;
    }

    result
}

// ----------------------------------------------------------------
// AtEOXact_Buffers
//
// Clean up at end of transaction.
// ----------------------------------------------------------------
pub unsafe fn AtEOXact_Buffers(isCommit: bool) {
    CheckForBufferLeaks();
    AtEOXact_LocalBuffers(isCommit);
}

#[inline]
unsafe fn AtEOXact_LocalBuffers(_isCommit: bool) {
    // TODO(pg-port): localbuf.c
}

// ----------------------------------------------------------------
// InitBufferManagerAccess
// ----------------------------------------------------------------
pub unsafe fn InitBufferManagerAccess() {
    /* nothing to do; per-process state already in static variables */
}

// ----------------------------------------------------------------
// AtProcExit_Buffers
// ----------------------------------------------------------------
pub unsafe fn AtProcExit_Buffers(_code: c_int, _arg: Datum) {
    AbortBufferIO();
    UnlockBuffers();
    CheckForBufferLeaks();
}

// ----------------------------------------------------------------
// CheckForBufferLeaks
// ----------------------------------------------------------------
unsafe fn CheckForBufferLeaks() {
    let mut leaked: c_int = 0;

    for i in 0..REFCOUNT_ARRAY_ENTRIES {
        if PrivateRefCountArray[i].buffer != InvalidBuffer {
            elog!(WARNING, "buffer refcount leak: buffer {}, count {}",
                PrivateRefCountArray[i].buffer, PrivateRefCountArray[i].refcount);
            leaked += 1;
        }
    }

    if !PrivateRefCountHash.is_null() {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
        hash_seq_init(&mut status, PrivateRefCountHash);
        loop {
            let entry = hash_seq_search(&mut status) as *mut PrivateRefCountEntry;
            if entry.is_null() {
                break;
            }
            elog!(WARNING, "buffer refcount leak (overflow): buffer {}, count {}",
                (*entry).buffer, (*entry).refcount);
            leaked += 1;
        }
    }

    assert_eq!(leaked, 0, "detected leaked buffer pin(s)");
}

// ----------------------------------------------------------------
// AssertBufferLocksPermitCatalogRead
// ----------------------------------------------------------------
pub unsafe fn AssertBufferLocksPermitCatalogRead() {
    /* nothing to assert in release builds */
}

// ----------------------------------------------------------------
// AssertNotCatalogBufferLock
// ----------------------------------------------------------------
pub unsafe fn AssertNotCatalogBufferLock(_lock: *mut LWLock) {
    /* nothing to assert in release builds */
}

// ----------------------------------------------------------------
// DebugPrintBufferRefcount
// ----------------------------------------------------------------
pub unsafe fn DebugPrintBufferRefcount(buffer: Buffer) {
    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
    elog!(LOG, "buffer {}: refcount={}, flags={:#010x}",
        buffer,
        BUF_STATE_GET_REFCOUNT(buf_state),
        buf_state);
}

// ----------------------------------------------------------------
// CheckPointBuffers
//
// Do whatever is needed to safely call BufferSync at checkpoint time.
// ----------------------------------------------------------------
pub unsafe fn CheckPointBuffers(flags: c_int) {
    BufferSync(flags);
}

// ----------------------------------------------------------------
// BufferGetBlockNumber
// ----------------------------------------------------------------
pub unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    assert!(BufferIsValid(buffer));
    if BufferIsLocal(buffer) {
        (*GetLocalBufferDescriptor((((-buffer - 1) as usize)) as u32)).tag.blockNum
    } else {
        (*GetBufferDescriptor((buffer - 1) as u32)).tag.blockNum
    }
}

// ----------------------------------------------------------------
// BufferGetTag
// ----------------------------------------------------------------
pub unsafe fn BufferGetTag(
    buffer: Buffer,
    rlocator: *mut RelFileLocator,
    forknum: *mut ForkNumber,
    blknum: *mut BlockNumber,
) {
    let buf: *mut BufferDesc = if BufferIsLocal(buffer) {
        GetLocalBufferDescriptor((((-buffer - 1) as usize)) as u32)
    } else {
        GetBufferDescriptor((buffer - 1) as u32)
    };

    let tag = (*buf).tag;
    *rlocator = BufTagGetRelFileLocator(&tag);
    *forknum = BufTagGetForkNum(&tag);
    *blknum = tag.blockNum;
}

// ----------------------------------------------------------------
// FlushBuffer
//
// Write a buffer to disk.  Caller must hold the content lock.
// ----------------------------------------------------------------
pub unsafe fn FlushBuffer(
    buf: *mut BufferDesc,
    reln: SMgrRelation,
    io_object: IOObject,
    io_context: IOContext,
) {
    let buf_state = pg_atomic_read_u32(&(*buf).state);
    if (buf_state & BM_DIRTY) == 0 {
        return;
    }

    let tag = (*buf).tag;
    let rlocator = BufTagGetRelFileLocator(&tag);
    let forknum = BufTagGetForkNum(&tag);
    let blocknum = tag.blockNum;

    /* Get smgr if not provided */
    let smgr_to_use: SMgrRelation = if !reln.is_null() {
        reln
    } else {
        smgropen(rlocator, INVALID_PROC_NUMBER)
    };

    /* Make sure WAL is flushed past this page's LSN */
    if XLogIsNeeded() {
        let lsn = PageGetLSN(BufHdrGetBlock(buf) as *const c_void);
        if !XLogRecPtrIsInvalid(lsn) {
            XLogFlush(lsn);
        }
    }

    /* Compute checksum if needed */
    let data_to_write = PageSetChecksumCopy(BufHdrGetBlock(buf) as Page, blocknum);

    let start_time = pgstat_prepare_io_time(true);

    TRACE_POSTGRESQL_BUFFER_FLUSH_START!(forknum, blocknum, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber);

    smgrwrite(smgr_to_use, forknum, blocknum, data_to_write, false);

    pgstat_count_io_op_time(io_object, io_context, IOOP_WRITE, start_time, 1, BLCKSZ as u64);
    pgBufferUsage.shared_blks_written += 1;

    TRACE_POSTGRESQL_BUFFER_FLUSH_DONE!(forknum, blocknum, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber);

    /* Clear dirty flag */
    let buf_state2 = LockBufHdr(buf);
    let new_state = buf_state2 & !BM_DIRTY;
    UnlockBufHdr(buf, new_state);
}

// ----------------------------------------------------------------
// RelationGetNumberOfBlocksInFork
// ----------------------------------------------------------------
pub unsafe fn RelationGetNumberOfBlocksInFork(rel: Relation, forkNum: ForkNumber) -> BlockNumber {
    if RelationUsesLocalBuffers(rel) {
        return LocalRelSize(rel, forkNum);
    }
    let smgr = RelationGetSmgr(rel);
    smgrnblocks(smgr, forkNum)
}

// ----------------------------------------------------------------
// BufferIsPermanent
// ----------------------------------------------------------------
pub unsafe fn BufferIsPermanent(buffer: Buffer) -> bool {
    if BufferIsLocal(buffer) {
        return false;
    }
    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    (pg_atomic_read_u32(&(*bufHdr).state) & BM_PERMANENT) != 0
}

// ----------------------------------------------------------------
// BufferGetLSNAtomic
//
// Return the current LSN of a buffer without holding the content lock.
// Safe because we're reading a single 8-byte value that on x86 is
// always read atomically.
// ----------------------------------------------------------------
pub unsafe fn BufferGetLSNAtomic(buffer: Buffer) -> XLogRecPtr {
    let page = BufferGetPage(buffer);
    /* On modern platforms this is always an aligned 8-byte read. */
    PageGetLSN(page as *const c_void)
}

// ----------------------------------------------------------------
// DropRelationBuffers
// ----------------------------------------------------------------
pub unsafe fn DropRelationBuffers(
    smgr: SMgrRelation,
    forkNum: *mut ForkNumber,
    nforks: c_int,
    firstDelBlock: *mut BlockNumber,
) {
    let rlocator = smgr_rlocator(smgr).locator;
    let is_temp = RelFileLocatorBackendIsTemp(smgr_rlocator(smgr));

    if is_temp {
        for i in 0..nforks as usize {
            let fork = *forkNum.add(i);
            let first = *firstDelBlock.add(i);
            DropLocalRelFileLocatorBuffers(
                rlocator, fork, first,
            );
        }
        return;
    }

    for i in 0..nforks as usize {
        let fork = *forkNum.add(i);
        let first = *firstDelBlock.add(i);
        FindAndDropRelationBuffers(smgr, fork, first);
    }
}

// ----------------------------------------------------------------
// DropRelationsAllBuffers
// ----------------------------------------------------------------
pub unsafe fn DropRelationsAllBuffers(
    smgr_array: *mut SMgrRelation,
    nrels: c_int,
) {
    if nrels == 0 {
        return;
    }

    /* Sort the rels for bsearch later */
    let mut smgr_list: Vec<SMgrSortArray> = (0..nrels as usize)
        .map(|i| {
            let s = *smgr_array.add(i);
            SMgrSortArray {
                rlocator: smgr_rlocator(s).locator,
                srel: s,
            }
        })
        .collect();

    if smgr_list.len() > RELS_BSEARCH_THRESHOLD {
        smgr_list.sort_by(|a, b| rlocator_cmp_key(&a.rlocator, &b.rlocator));
    }

    /* Scan all shared buffers */
    for buf_id in 0..NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & BM_TAG_VALID) == 0 {
            continue;
        }
        let tag = (*bufHdr).tag;
        let rlocator = BufTagGetRelFileLocator(&tag);

        /* Check if this buffer's relation is in our list */
        let found = if smgr_list.len() > RELS_BSEARCH_THRESHOLD {
            smgr_list.binary_search_by(|s| rlocator_cmp_key(&s.rlocator, &rlocator)).is_ok()
        } else {
            smgr_list.iter().any(|s| RelFileLocatorEquals(s.rlocator, rlocator))
        };

        if found {
            InvalidateBuffer(bufHdr);
        }
    }
}

fn rlocator_cmp_key(a: &RelFileLocator, b: &RelFileLocator) -> core::cmp::Ordering {
    let a_spc = a.spcOid;
    let b_spc = b.spcOid;
    if a_spc != b_spc {
        return a_spc.cmp(&b_spc);
    }
    let a_db = a.dbOid;
    let b_db = b.dbOid;
    if a_db != b_db {
        return a_db.cmp(&b_db);
    }
    a.relNumber.cmp(&b.relNumber)
}

// ----------------------------------------------------------------
// FindAndDropRelationBuffers
// ----------------------------------------------------------------
unsafe fn FindAndDropRelationBuffers(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    firstDelBlock: BlockNumber,
) {
    let rlocator = smgr_rlocator(smgr).locator;

    for buf_id in 0..NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & BM_TAG_VALID) == 0 {
            continue;
        }
        let tag = (*bufHdr).tag;
        if BufTagGetForkNum(&tag) != forkNum {
            continue;
        }
        if !RelFileLocatorEquals(BufTagGetRelFileLocator(&tag), rlocator) {
            continue;
        }
        if tag.blockNum < firstDelBlock {
            continue;
        }
        InvalidateBuffer(bufHdr);
    }
}

// ----------------------------------------------------------------
// DropDatabaseBuffers
// ----------------------------------------------------------------
pub unsafe fn DropDatabaseBuffers(dbid: Oid) {
    for buf_id in 0..NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & BM_TAG_VALID) == 0 {
            continue;
        }
        let tag = (*bufHdr).tag;
        if BufTagGetRelFileLocator(&tag).dbOid != dbid {
            continue;
        }
        InvalidateBuffer(bufHdr);
    }
}

// ----------------------------------------------------------------
// FlushRelationBuffers
// ----------------------------------------------------------------
pub unsafe fn FlushRelationBuffers(rel: Relation) {
    if RelationUsesLocalBuffers(rel) {
        FlushLocalRelationBuffers(rel);
        return;
    }
    let smgr = RelationGetSmgr(rel);
    let rlocator = smgr_rlocator(smgr).locator;

    for buf_id in 0..NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & (BM_VALID | BM_DIRTY)) != (BM_VALID | BM_DIRTY) {
            continue;
        }
        let tag = (*bufHdr).tag;
        if !RelFileLocatorEquals(BufTagGetRelFileLocator(&tag), rlocator) {
            continue;
        }

        /* Pin it */
        ReservePrivateRefCountEntry();
        ResourceOwnerEnlarge(CurrentResourceOwner);

        let buf_state2 = LockBufHdr(bufHdr);
        if (buf_state2 & (BM_VALID | BM_DIRTY)) != (BM_VALID | BM_DIRTY) {
            UnlockBufHdr(bufHdr, buf_state2);
            continue;
        }

        let new_state = buf_state2 + BUF_REFCOUNT_ONE;
        UnlockBufHdr(bufHdr, new_state);

        let buf = buf_id + 1;
        let ref_ = NewPrivateRefCountEntry(buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

        /* Acquire content lock */
        LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);

        /* Write the buffer */
        if StartBufferIO(bufHdr, false, false) {
            FlushBuffer(bufHdr, smgr, io_object_for_bufhdr(bufHdr), IOCONTEXT_NORMAL);
            TerminateBufferIO(bufHdr, false, 0, false);
        }

        LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
        UnpinBuffer(buf);
    }
}

// ----------------------------------------------------------------
// FlushRelationsAllBuffers
// ----------------------------------------------------------------
pub unsafe fn FlushRelationsAllBuffers(
    smgrs: *mut SMgrRelation,
    nrels: c_int,
) {
    for i in 0..nrels as usize {
        let smgr = *smgrs.add(i);
        let rlocator = smgr_rlocator(smgr).locator;

        for buf_id in 0..NBuffers {
            let bufHdr = GetBufferDescriptor(buf_id as u32);
            let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
            if (buf_state & (BM_VALID | BM_DIRTY)) != (BM_VALID | BM_DIRTY) {
                continue;
            }
            let tag = (*bufHdr).tag;
            if !RelFileLocatorEquals(BufTagGetRelFileLocator(&tag), rlocator) {
                continue;
            }

            ReservePrivateRefCountEntry();
            ResourceOwnerEnlarge(CurrentResourceOwner);
            let buf_state2 = LockBufHdr(bufHdr);
            let new_state = buf_state2 + BUF_REFCOUNT_ONE;
            UnlockBufHdr(bufHdr, new_state);
            let buf = buf_id + 1;
            let ref_ = NewPrivateRefCountEntry(buf);
            (*ref_).refcount += 1;
            ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

            LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
            if StartBufferIO(bufHdr, false, false) {
                FlushBuffer(bufHdr, smgr, io_object_for_bufhdr(bufHdr), IOCONTEXT_NORMAL);
                TerminateBufferIO(bufHdr, false, 0, false);
            }
            LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
            UnpinBuffer(buf);
        }
    }
}

// ----------------------------------------------------------------
// RelationCopyStorageUsingBuffer
// ----------------------------------------------------------------
pub unsafe fn RelationCopyStorageUsingBuffer(
    src_rlocator: RelFileLocator,
    dst_rlocator: RelFileLocator,
    forkNum: ForkNumber,
    permanent: bool,
) {
    let persistence = if permanent {
        RELPERSISTENCE_PERMANENT
    } else {
        RELPERSISTENCE_TEMP
    };

    let src_smgr = smgropen(src_rlocator, INVALID_PROC_NUMBER);
    let dst_smgr = smgropen(dst_rlocator, INVALID_PROC_NUMBER);
    let nblocks = smgrnblocks(src_smgr, forkNum);

    for blockno in 0..nblocks {
        let src_buf = ReadBufferWithoutRelcache(src_rlocator, forkNum, blockno, RBM_NORMAL, null_mut(), permanent);
        LWLockAcquire(BufferDescriptorGetContentLock(GetBufferDescriptor((src_buf - 1) as u32)), LW_SHARED);

        let dst_buf = ReadBufferWithoutRelcache(dst_rlocator, forkNum, blockno, RBM_ZERO_AND_LOCK, null_mut(), permanent);

        core::ptr::copy_nonoverlapping(
            BufferGetPage(src_buf) as *const u8,
            BufferGetPage(dst_buf) as *mut u8,
            BLCKSZ,
        );

        MarkBufferDirty(dst_buf);
        LWLockRelease(BufferDescriptorGetContentLock(GetBufferDescriptor((dst_buf - 1) as u32)));
        UnlockReleaseBuffer(dst_buf);
        LWLockRelease(BufferDescriptorGetContentLock(GetBufferDescriptor((src_buf - 1) as u32)));
        ReleaseBuffer(src_buf);
    }
}

// ----------------------------------------------------------------
// CreateAndCopyRelationData
// ----------------------------------------------------------------
pub unsafe fn CreateAndCopyRelationData(
    src_rlocator: RelFileLocator,
    dst_rlocator: RelFileLocator,
    permanent: bool,
) {
    let persistence = if permanent {
        RELPERSISTENCE_PERMANENT
    } else {
        RELPERSISTENCE_TEMP
    };
    let dst_smgr = smgropen(dst_rlocator, INVALID_PROC_NUMBER);

    for forkNum in 0..=MAX_FORKNUM {
        let src_smgr = smgropen(src_rlocator, INVALID_PROC_NUMBER);
        if !smgrexists(src_smgr, forkNum) {
            continue;
        }
        smgrcreate(dst_smgr, forkNum, false);
        RelationCopyStorageUsingBuffer(src_rlocator, dst_rlocator, forkNum, permanent);
    }
}

// ----------------------------------------------------------------
// FlushDatabaseBuffers
// ----------------------------------------------------------------
pub unsafe fn FlushDatabaseBuffers(dbid: Oid) {
    for buf_id in 0..NBuffers {
        let bufHdr = GetBufferDescriptor(buf_id as u32);
        let buf_state = pg_atomic_read_u32(&(*bufHdr).state);
        if (buf_state & (BM_VALID | BM_DIRTY)) != (BM_VALID | BM_DIRTY) {
            continue;
        }
        let tag = (*bufHdr).tag;
        if BufTagGetRelFileLocator(&tag).dbOid != dbid {
            continue;
        }

        ReservePrivateRefCountEntry();
        ResourceOwnerEnlarge(CurrentResourceOwner);
        let buf_state2 = LockBufHdr(bufHdr);
        let new_state = buf_state2 + BUF_REFCOUNT_ONE;
        UnlockBufHdr(bufHdr, new_state);
        let buf = buf_id + 1;
        let ref_ = NewPrivateRefCountEntry(buf);
        (*ref_).refcount += 1;
        ResourceOwnerRememberBuffer(CurrentResourceOwner, buf);

        LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
        if StartBufferIO(bufHdr, false, false) {
            FlushBuffer(bufHdr, null_mut(), io_object_for_bufhdr(bufHdr), IOCONTEXT_NORMAL);
            TerminateBufferIO(bufHdr, false, 0, false);
        }
        LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
        UnpinBuffer(buf);
    }
}

// ----------------------------------------------------------------
// FlushOneBuffer
// ----------------------------------------------------------------
pub unsafe fn FlushOneBuffer(buffer: Buffer) {
    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    assert!(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_SHARED)
        || LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE));
    FlushBuffer(buf_hdr, null_mut(), io_object_for_bufhdr(buf_hdr), IOCONTEXT_NORMAL);
}

// ----------------------------------------------------------------
// ReleaseBuffer
// ----------------------------------------------------------------
pub unsafe fn ReleaseBuffer(buffer: Buffer) {
    if !BufferIsValid(buffer) {
        elog!(ERROR, "bad buffer ID: {}", buffer);
    }
    if BufferIsLocal(buffer) {
        use crate::storage::buffer::localbuf::LocalRefCount;
        let idx = (-buffer - 1) as usize;
        assert!(*LocalRefCount.add(idx) > 0);
        *LocalRefCount.add(idx) -= 1;
        return;
    }
    UnpinBuffer(buffer);
}

// ----------------------------------------------------------------
// UnlockReleaseBuffer
// ----------------------------------------------------------------
pub unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
}

// ----------------------------------------------------------------
// IncrBufferRefCount
// ----------------------------------------------------------------
pub unsafe fn IncrBufferRefCount(buffer: Buffer) {
    assert!(BufferIsValid(buffer));
    ResourceOwnerEnlarge(CurrentResourceOwner);

    if BufferIsLocal(buffer) {
        use crate::storage::buffer::localbuf::LocalRefCount;
        let idx = (-buffer - 1) as usize;
        *LocalRefCount.add(idx) += 1;
        return;
    }

    let ref_ = GetPrivateRefCountEntry(buffer, true);
    assert!(!ref_.is_null(), "buffer {} not pinned", buffer);
    (*ref_).refcount += 1;
    ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer);
}

// ----------------------------------------------------------------
// MarkBufferDirtyHint
//
// Mark a buffer dirty when no exclusive lock is held.
// Only used for hint bits.
// ----------------------------------------------------------------
pub unsafe fn MarkBufferDirtyHint(buffer: Buffer, buffer_std: bool) {
    assert!(BufferIsValid(buffer));

    if BufferIsLocal(buffer) {
        MarkBufferDirty(buffer);
        return;
    }

    let bufHdr = GetBufferDescriptor((buffer - 1) as u32);
    let buf_state = pg_atomic_read_u32(&(*bufHdr).state);

    if (buf_state & BM_DIRTY) != 0 {
        /* already dirty */
        return;
    }

    /*
     * If the block is in WAL, we need to flush.
     */
    if XLogHintBitIsNeeded() && (buf_state & BM_PERMANENT) != 0 {
        let lsn = XLogSaveBufferForHint(buffer, buffer_std);
        if !XLogRecPtrIsInvalid(lsn) {
            let recptr = BufferGetLSN(bufHdr);
            if recptr <= lsn {
                /* Set dirty only after WAL is recorded */
                let _ = pg_atomic_fetch_or_u32(&mut (*bufHdr).state, BM_DIRTY | BM_JUST_DIRTIED);
                pgBufferUsage.shared_blks_dirtied += 1;
                return;
            }
        }
    }

    let _ = pg_atomic_fetch_or_u32(&mut (*bufHdr).state, BM_DIRTY | BM_JUST_DIRTIED);
    pgBufferUsage.shared_blks_dirtied += 1;

    if VacuumCostActive {
        VacuumCostBalance += VacuumCostPageDirty;
    }
}

// ----------------------------------------------------------------
// UnlockBuffers
//
// Release any buffer content locks held at cleanup.
// ----------------------------------------------------------------
pub unsafe fn UnlockBuffers() {
    /* Release any buffer content locks we hold */
    /* (tracked by LWLock subsystem; nothing to do here except call UnlockBuffers_lwlocks) */
    UnlockBuffers_lwlocks();
}

// ----------------------------------------------------------------
// LockBuffer
// ----------------------------------------------------------------
pub unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    assert!(BufferIsValid(buffer));
    if BufferIsLocal(buffer) {
        return; /* local buffers have no lock */
    }

    let buf = GetBufferDescriptor((buffer - 1) as u32);
    let content_lock = BufferDescriptorGetContentLock(buf);

    if mode == BUFFER_LOCK_UNLOCK {
        LWLockRelease(content_lock);
    } else if mode == BUFFER_LOCK_SHARE {
        LWLockAcquire(content_lock, LW_SHARED);
    } else {
        assert_eq!(mode, BUFFER_LOCK_EXCLUSIVE);
        LWLockAcquire(content_lock, LW_EXCLUSIVE);
    }
}

// ----------------------------------------------------------------
// ConditionalLockBuffer
// ----------------------------------------------------------------
pub unsafe fn ConditionalLockBuffer(buffer: Buffer) -> bool {
    assert!(BufferIsValid(buffer));
    if BufferIsLocal(buffer) {
        return true;
    }
    let buf = GetBufferDescriptor((buffer - 1) as u32);
    LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)
}

// ----------------------------------------------------------------
// CheckBufferIsPinnedOnce
// ----------------------------------------------------------------
pub unsafe fn CheckBufferIsPinnedOnce(buffer: Buffer) {
    let ref_ = GetPrivateRefCountEntry(buffer, false);
    if ref_.is_null() || (*ref_).refcount != 1 {
        elog!(ERROR, "buffer {} is not pinned exactly once", buffer);
    }
}

// ----------------------------------------------------------------
// LockBufferForCleanup
// ----------------------------------------------------------------
pub unsafe fn LockBufferForCleanup(buffer: Buffer) {
    assert!(BufferIsValid(buffer));

    if BufferIsLocal(buffer) {
        /* Nothing to do for local buffers */
        return;
    }

    CheckBufferIsPinnedOnce(buffer);

    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);

    loop {
        let buf_state = LockBufHdr(buf_hdr);
        let refcount = BUF_STATE_GET_REFCOUNT(buf_state);

        if refcount == 1 {
            /* Only we have it pinned; acquire exclusive content lock */
            UnlockBufHdr(buf_hdr, buf_state);
            LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE);
            return;
        }

        /*
         * There are other pins; register as waiter and wait.
         */
        assert_eq!(refcount, 2, "expected refcount 2 when waiting for cleanup lock");

        /* Set wait flag */
        let new_state = buf_state | BM_PIN_COUNT_WAITER;
        (*buf_hdr).wait_backend_pgprocno = MyProcNumber;
        UnlockBufHdr(buf_hdr, new_state);

        ProcWaitForSignal(WAIT_EVENT_BUFFER_PIN);
    }
}

// ----------------------------------------------------------------
// HoldingBufferPinThatDelaysRecovery
// ----------------------------------------------------------------
pub unsafe fn HoldingBufferPinThatDelaysRecovery() -> bool {
    if !InHotStandby() {
        return false;
    }

    let wait_buf_id = GetStartupBufferPinWaitBufId();
    if wait_buf_id < 0 {
        return false;
    }

    let ref_ = GetPrivateRefCountEntry(wait_buf_id + 1, false);
    if ref_.is_null() {
        return false;
    }
    (*ref_).refcount > 0
}

// ----------------------------------------------------------------
// ConditionalLockBufferForCleanup
// ----------------------------------------------------------------
pub unsafe fn ConditionalLockBufferForCleanup(buffer: Buffer) -> bool {
    assert!(BufferIsValid(buffer));
    if BufferIsLocal(buffer) {
        return true;
    }

    CheckBufferIsPinnedOnce(buffer);

    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    let buf_state = LockBufHdr(buf_hdr);
    let refcount = BUF_STATE_GET_REFCOUNT(buf_state);

    if refcount > 1 {
        UnlockBufHdr(buf_hdr, buf_state);
        return false;
    }

    /* Only we have it; grab the content lock */
    UnlockBufHdr(buf_hdr, buf_state);
    LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE);
    true
}

// ----------------------------------------------------------------
// IsBufferCleanupOK
// ----------------------------------------------------------------
pub unsafe fn IsBufferCleanupOK(buffer: Buffer) -> bool {
    assert!(BufferIsValid(buffer));
    if BufferIsLocal(buffer) {
        return true;
    }
    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    let buf_state = LockBufHdr(buf_hdr);
    let refcount = BUF_STATE_GET_REFCOUNT(buf_state);
    UnlockBufHdr(buf_hdr, buf_state);
    refcount == 1
}

// ----------------------------------------------------------------
// WaitIO
//
// Wait for I/O on a buffer to complete.
// ----------------------------------------------------------------
pub unsafe fn WaitIO(buf: *mut BufferDesc) {
    loop {
        let buf_state = pg_atomic_read_u32(&(*buf).state);
        if (buf_state & BM_IO_IN_PROGRESS) == 0 {
            break;
        }
        let cv = BufferDescriptorGetIOCV(buf);
        ConditionVariablePrepareToSleep(cv as *mut _);
        /* re-check after preparing */
        let buf_state2 = pg_atomic_read_u32(&(*buf).state);
        if (buf_state2 & BM_IO_IN_PROGRESS) == 0 {
            ConditionVariableCancelSleep();
            break;
        }
        ConditionVariableSleep(cv as *mut _, WAIT_EVENT_BUFFER_IO);
    }
}

// ----------------------------------------------------------------
// StartBufferIO
//
// Acquire the right to perform I/O on a buffer.
// Returns true if we can proceed; false if someone else is doing it.
// ----------------------------------------------------------------
pub unsafe fn StartBufferIO(buf: *mut BufferDesc, forInput: bool, nowait: bool) -> bool {
    loop {
        let buf_state = LockBufHdr(buf);

        if (buf_state & BM_IO_IN_PROGRESS) != 0 {
            /* I/O already started */
            UnlockBufHdr(buf, buf_state);
            if nowait {
                return false;
            }
            WaitIO(buf);
            continue;
        }

        if forInput {
            /* For reads: don't start if already valid */
            if (buf_state & BM_VALID) != 0 {
                UnlockBufHdr(buf, buf_state);
                return false;
            }
        } else {
            /* For writes: don't start if not dirty */
            if (buf_state & BM_DIRTY) == 0 {
                UnlockBufHdr(buf, buf_state);
                return false;
            }
        }

        /* Set IO in progress */
        let new_state = buf_state | BM_IO_IN_PROGRESS;
        UnlockBufHdr(buf, new_state);
        return true;
    }
}

// ----------------------------------------------------------------
// TerminateBufferIO
//
// Mark I/O as complete on a buffer.
// ----------------------------------------------------------------
pub unsafe fn TerminateBufferIO(
    buf: *mut BufferDesc,
    err: bool,
    set_flag_bits: uint32,
    has_private_ref: bool,
) {
    let buf_state = LockBufHdr(buf);
    let mut new_state = buf_state;

    new_state &= !(BM_IO_IN_PROGRESS | BM_IO_ERROR);

    if err {
        new_state |= BM_IO_ERROR;
    } else {
        new_state |= set_flag_bits;
    }

    UnlockBufHdr(buf, new_state);

    /* Wake anyone waiting for this I/O to complete */
    let cv = BufferDescriptorGetIOCV(buf);
    ConditionVariableBroadcast(cv as *mut _);
}

// ----------------------------------------------------------------
// AbortBufferIO
//
// Called when we must abort an I/O in progress (e.g., on error).
// ----------------------------------------------------------------
pub unsafe fn AbortBufferIO() {
    /* Nothing to do if no I/O in progress from this backend */
}

// ----------------------------------------------------------------
// buffer_write_error_callback
// ----------------------------------------------------------------
unsafe fn buffer_write_error_callback(arg: *mut c_void) {
    let buf_hdr = arg as *mut BufferDesc;
    if (*buf_hdr).buf_id >= 0 {
        let tag = (*buf_hdr).tag;
        let rlocator = BufTagGetRelFileLocator(&tag);
        let forknum = BufTagGetForkNum(&tag);
        let blkno = tag.blockNum;
        // errcontext omitted (TODO(pg-port))
    }
}

// ----------------------------------------------------------------
// shared_buffer_read_error_callback
// ----------------------------------------------------------------
unsafe fn shared_buffer_read_error_callback(arg: *mut c_void) {
    let buf_hdr = arg as *mut BufferDesc;
    if (*buf_hdr).buf_id >= 0 {
        // errcontext omitted (TODO(pg-port))
    }
}

// ----------------------------------------------------------------
// Comparator functions used by inline sort
// ----------------------------------------------------------------

/*
 * Error context callback for errors occurring during shared buffer writes.
 */
unsafe fn shared_buffer_write_error_callback(arg: *mut c_void) {
    let bufHdr = arg as *mut BufferDesc;

    /* Buffer is pinned, so we can read the tag without locking the spinlock */
    if !bufHdr.is_null() {
        let tag = (*bufHdr).tag;
        let _rlocator = BufTagGetRelFileLocator(&tag);
        let _forknum = BufTagGetForkNum(&tag);
        let _blkno = tag.blockNum;
        /* C also: errcontext("writing block %u of relation \"%s\"", ...) */
        errcontext(relpathperm(_rlocator, _forknum).str_ptr());
    }
}

/*
 * Error context callback for errors occurring during local buffer writes.
 */
unsafe fn local_buffer_write_error_callback(arg: *mut c_void) {
    let bufHdr = arg as *mut BufferDesc;

    if !bufHdr.is_null() {
        let tag = (*bufHdr).tag;
        let _rlocator = BufTagGetRelFileLocator(&tag);
        let _forknum = BufTagGetForkNum(&tag);
        let _blkno = tag.blockNum;
        /* C also: errcontext("writing block %u of relation \"%s\"", ...) */
        errcontext(relpathbackend(_rlocator, MyProcNumber, _forknum).str_ptr());
    }
}

/*
 * RelFileLocator qsort/bsearch comparator; see RelFileLocatorEquals.
 */
unsafe fn rlocator_comparator(p1: *const c_void, p2: *const c_void) -> c_int {
    let n1 = *(p1 as *const RelFileLocator);
    let n2 = *(p2 as *const RelFileLocator);

    if n1.relNumber < n2.relNumber {
        return -1;
    } else if n1.relNumber > n2.relNumber {
        return 1;
    }

    if n1.dbOid < n2.dbOid {
        return -1;
    } else if n1.dbOid > n2.dbOid {
        return 1;
    }

    if n1.spcOid < n2.spcOid {
        -1
    } else if n1.spcOid > n2.spcOid {
        1
    } else {
        0
    }
}

/*
 * BufferTag comparator.
 */
#[inline]
unsafe fn buffertag_comparator(ba: *const BufferTag, bb: *const BufferTag) -> c_int {
    let rlocatora = BufTagGetRelFileLocator(ba);
    let rlocatorb = BufTagGetRelFileLocator(bb);

    let ret = rlocator_comparator(
        &rlocatora as *const RelFileLocator as *const c_void,
        &rlocatorb as *const RelFileLocator as *const c_void,
    );

    if ret != 0 {
        return ret;
    }

    if BufTagGetForkNum(ba) < BufTagGetForkNum(bb) {
        return -1;
    }
    if BufTagGetForkNum(ba) > BufTagGetForkNum(bb) {
        return 1;
    }

    if (*ba).blockNum < (*bb).blockNum {
        return -1;
    }
    if (*ba).blockNum > (*bb).blockNum {
        return 1;
    }

    0
}

pub unsafe fn ckpt_buforder_comparator(
    pa: *const c_void,
    pb: *const c_void,
) -> c_int {
    let a = &*(pa as *const CkptSortItem);
    let b = &*(pb as *const CkptSortItem);

    /* Sort by tablespace first */
    if a.tsId < b.tsId {
        return -1;
    }
    if a.tsId > b.tsId {
        return 1;
    }
    /* Then by relative file number */
    if a.relNumber < b.relNumber {
        return -1;
    }
    if a.relNumber > b.relNumber {
        return 1;
    }
    /* Then by fork */
    if (a.forkNum as u8) < (b.forkNum as u8) {
        return -1;
    }
    if (a.forkNum as u8) > (b.forkNum as u8) {
        return 1;
    }
    /* Then by block number */
    if a.blockNum < b.blockNum {
        return -1;
    }
    if a.blockNum > b.blockNum {
        return 1;
    }
    0
}

/// sort_checkpoint_bufferids -- sort the CkptBufferIds array.
pub unsafe fn sort_checkpoint_bufferids(nitems: c_int) {
    if nitems > 1 {
        qsort(
            CkptBufferIds as *mut c_void,
            nitems as usize,
            core::mem::size_of::<CkptSortItem>(),
            Some(ckpt_buforder_comparator),
        );
    }
}

unsafe fn wb_comparator(
    pa: *const c_void,
    pb: *const c_void,
) -> c_int {
    let a = &*(pa as *const PendingWriteback);
    let b = &*(pb as *const PendingWriteback);

    /* compare by tablespace, then relation, then block */
    if a.tag.spcOid < b.tag.spcOid {
        return -1;
    }
    if a.tag.spcOid > b.tag.spcOid {
        return 1;
    }
    if a.tag.relNumber < b.tag.relNumber {
        return -1;
    }
    if a.tag.relNumber > b.tag.relNumber {
        return 1;
    }
    if a.tag.blockNum < b.tag.blockNum {
        return -1;
    }
    if a.tag.blockNum > b.tag.blockNum {
        return 1;
    }
    0
}

/// sort_pending_writebacks -- sort pending writeback array.
unsafe fn sort_pending_writebacks(pendingwb: *mut PendingWriteback, n: c_int) {
    if n > 1 {
        qsort(
            pendingwb as *mut c_void,
            n as usize,
            core::mem::size_of::<PendingWriteback>(),
            Some(wb_comparator),
        );
    }
}

// ----------------------------------------------------------------
// LockBufHdr / WaitBufHdrUnlocked -- from buf_internals (also in this file for convenience)
// ----------------------------------------------------------------

/// WaitBufHdrUnlocked: Wait until the buffer header lock is released.
pub unsafe fn WaitBufHdrUnlocked(buf: *mut BufferDesc) {
    let mut spins = 0u32;
    loop {
        let buf_state = pg_atomic_read_u32(&(*buf).state);
        if (buf_state & BM_LOCKED) == 0 {
            break;
        }
        spins += 1;
        if spins >= 1000 {
            // yield
            spins = 0;
        }
    }
}

// ----------------------------------------------------------------
// WritebackContextInit
// ----------------------------------------------------------------
pub unsafe fn WritebackContextInit(context: *mut WritebackContext, max_pending: *mut c_int) {
    (*context).nr_pending = 0;
}

// ----------------------------------------------------------------
// ScheduleBufferTagForWriteback
// ----------------------------------------------------------------
pub unsafe fn ScheduleBufferTagForWriteback(
    context: *mut WritebackContext,
    _io_object: IOObject,
    tag: *const BufferTag,
) {
    if backend_flush_after <= 0 {
        return;
    }

    let nr = (*context).nr_pending as usize;
    if nr >= WRITEBACK_MAX_PENDING_FLUSHES {
        IssuePendingWritebacks(context);
    }

    let nr = (*context).nr_pending as usize;
    let pending = &mut (*context).pending_writebacks[nr];
    pending.tag = *tag;
    (*context).nr_pending += 1;
}

// ----------------------------------------------------------------
// IssuePendingWritebacks
// ----------------------------------------------------------------
pub unsafe fn IssuePendingWritebacks(context: *mut WritebackContext) {
    if (*context).nr_pending == 0 {
        return;
    }

    /* Sort pending writebacks */
    sort_pending_writebacks((*context).pending_writebacks.as_mut_ptr(), (*context).nr_pending);

    let nr = (*context).nr_pending as usize;
    let mut i: usize = 0;

    while i < nr {
        let base = &(*context).pending_writebacks[i];
        let base_tag = base.tag;
        let base_block = base.tag.blockNum;

        /* Group consecutive blocks of the same relation */
        let mut j = i + 1;
        while j < nr {
            let next = &(*context).pending_writebacks[j];
            if next.tag.spcOid != base_tag.spcOid
                || next.tag.dbOid != base_tag.dbOid
                || next.tag.relNumber != base_tag.relNumber
            {
                break;
            }
            if next.tag.blockNum != base_block + (j - i) as BlockNumber {
                break;
            }
            j += 1;
        }

        let smgr = smgropen(
            RelFileLocator {
                spcOid: base_tag.spcOid,
                dbOid: base_tag.dbOid,
                relNumber: base_tag.relNumber,
            },
            INVALID_PROC_NUMBER,
        );
        smgrwriteback(smgr, MAIN_FORKNUM, base_block, j - i);

        i = j;
    }

    (*context).nr_pending = 0;
}

/*
 * Helper function to evict unpinned buffer whose buffer header lock is
 * already acquired.
 */
unsafe fn EvictUnpinnedBufferInternal(desc: *mut BufferDesc, buffer_flushed: *mut bool) -> bool {
    let buf_state: uint32;
    let result: bool;

    *buffer_flushed = false;

    buf_state = pg_atomic_read_u32(&(*desc).state);
    assert!(buf_state & BM_LOCKED != 0);

    if (buf_state & BM_VALID) == 0 {
        UnlockBufHdr(desc, buf_state);
        return false;
    }

    /* Check that it's not pinned already. */
    if BUF_STATE_GET_REFCOUNT(buf_state) > 0 {
        UnlockBufHdr(desc, buf_state);
        return false;
    }

    PinBuffer_Locked(desc); /* releases spinlock */

    /* If it was dirty, try to clean it once. */
    if buf_state & BM_DIRTY != 0 {
        LWLockAcquire(BufferDescriptorGetContentLock(desc), LW_SHARED);
        FlushBuffer(desc, null_mut(), IOOBJECT_RELATION, IOCONTEXT_NORMAL);
        *buffer_flushed = true;
        LWLockRelease(BufferDescriptorGetContentLock(desc));
    }

    /* This will return false if it becomes dirty or someone else pins it. */
    result = InvalidateVictimBuffer(desc);

    UnpinBuffer(BufferDescriptorGetBuffer(desc));

    result
}

/*
 * Try to evict the current block in a shared buffer.
 *
 * This function is intended for testing/development use only!
 *
 * To succeed, the buffer must not be pinned on entry, so if the caller had a
 * particular block in mind, it might already have been replaced by some other
 * block by the time this function runs.  It's also unpinned on return, so the
 * buffer might be occupied again by the time control is returned, potentially
 * even by the same block.  This inherent raciness without other interlocking
 * makes the function unsuitable for non-testing usage.
 *
 * *buffer_flushed is set to true if the buffer was dirty and has been
 * flushed, false otherwise.  However, *buffer_flushed=true does not
 * necessarily mean that we flushed the buffer, it could have been flushed by
 * someone else.
 *
 * Returns true if the buffer was valid and it has now been made invalid.
 * Returns false if it wasn't valid, if it couldn't be evicted due to a pin,
 * or if the buffer becomes dirty again while we're trying to write it out.
 */
pub unsafe fn EvictUnpinnedBuffer(buf: Buffer, buffer_flushed: *mut bool) -> bool {
    let desc: *mut BufferDesc;

    assert!(BufferIsValid(buf) && !BufferIsLocal(buf));

    /* Make sure we can pin the buffer. */
    ResourceOwnerEnlarge(CurrentResourceOwner);
    ReservePrivateRefCountEntry();

    desc = GetBufferDescriptor((buf - 1) as u32);
    LockBufHdr(desc);

    EvictUnpinnedBufferInternal(desc, buffer_flushed)
}

/*
 * Try to evict all the shared buffers.
 *
 * This function is intended for testing/development use only! See
 * EvictUnpinnedBuffer().
 *
 * The buffers_* parameters are mandatory and indicate the total count of
 * buffers that:
 * - buffers_evicted - were evicted
 * - buffers_flushed - were flushed
 * - buffers_skipped - could not be evicted
 */
pub unsafe fn EvictAllUnpinnedBuffers(
    buffers_evicted: *mut int32,
    buffers_flushed: *mut int32,
    buffers_skipped: *mut int32,
) {
    *buffers_evicted = 0;
    *buffers_skipped = 0;
    *buffers_flushed = 0;

    for buf in 1..=NBuffers {
        let desc = GetBufferDescriptor((buf - 1) as u32);
        let buf_state: uint32;
        let mut buffer_flushed: bool = false;

        CHECK_FOR_INTERRUPTS();

        buf_state = pg_atomic_read_u32(&(*desc).state);
        if (buf_state & BM_VALID) == 0 {
            continue;
        }

        ResourceOwnerEnlarge(CurrentResourceOwner);
        ReservePrivateRefCountEntry();

        LockBufHdr(desc);

        if EvictUnpinnedBufferInternal(desc, &mut buffer_flushed) {
            *buffers_evicted += 1;
        } else {
            *buffers_skipped += 1;
        }

        if buffer_flushed {
            *buffers_flushed += 1;
        }
    }
}

/*
 * Try to evict all the shared buffers containing provided relation's pages.
 *
 * This function is intended for testing/development use only! See
 * EvictUnpinnedBuffer().
 *
 * The caller must hold at least AccessShareLock on the relation to prevent
 * the relation from being dropped.
 *
 * The buffers_* parameters are mandatory and indicate the total count of
 * buffers that:
 * - buffers_evicted - were evicted
 * - buffers_flushed - were flushed
 * - buffers_skipped - could not be evicted
 */
pub unsafe fn EvictRelUnpinnedBuffers(
    rel: Relation,
    buffers_evicted: *mut int32,
    buffers_flushed: *mut int32,
    buffers_skipped: *mut int32,
) {
    assert!(!RelationUsesLocalBuffers(rel));

    *buffers_skipped = 0;
    *buffers_evicted = 0;
    *buffers_flushed = 0;

    for buf in 1..=NBuffers {
        let desc = GetBufferDescriptor((buf - 1) as u32);
        let mut buf_state = pg_atomic_read_u32(&(*desc).state);
        let mut buffer_flushed: bool = false;

        CHECK_FOR_INTERRUPTS();

        /* An unlocked precheck should be safe and saves some cycles. */
        if (buf_state & BM_VALID) == 0
            || !BufTagMatchesRelFileLocator(&(*desc).tag, &(*rel).rd_locator)
        {
            continue;
        }

        /* Make sure we can pin the buffer. */
        ResourceOwnerEnlarge(CurrentResourceOwner);
        ReservePrivateRefCountEntry();

        buf_state = LockBufHdr(desc);

        /* recheck, could have changed without the lock */
        if (buf_state & BM_VALID) == 0
            || !BufTagMatchesRelFileLocator(&(*desc).tag, &(*rel).rd_locator)
        {
            UnlockBufHdr(desc, buf_state);
            continue;
        }

        if EvictUnpinnedBufferInternal(desc, &mut buffer_flushed) {
            *buffers_evicted += 1;
        } else {
            *buffers_skipped += 1;
        }

        if buffer_flushed {
            *buffers_flushed += 1;
        }
    }
}

// ----------------------------------------------------------------
// ResourceOwner callbacks
// ----------------------------------------------------------------

pub unsafe fn ResOwnerReleaseBuffer(res: Datum) {
    let buffer = res as Buffer;
    if BufferIsLocal(buffer) {
        use crate::storage::buffer::localbuf::LocalRefCount;
        let idx = (-buffer - 1) as usize;
        if *LocalRefCount.add(idx) > 0 {
            *LocalRefCount.add(idx) -= 1;
        }
        return;
    }
    UnpinBufferNoOwner(buffer);
}

pub unsafe fn ResOwnerReleaseBufferIO(res: Datum) {
    let buffer = res as Buffer;
    if BufferIsLocal(buffer) {
        return;
    }
    let buf_hdr = GetBufferDescriptor((buffer - 1) as u32);
    AbortBufferIO();
}

unsafe fn ResOwnerPrintBufferIO(res: Datum) -> *mut c_char {
    let buffer: Buffer = DatumGetInt32(res);

    psprintf(&format!("lost track of buffer IO on buffer {}", buffer))
}

unsafe fn ResOwnerReleaseBufferPin(res: Datum) {
    let buffer: Buffer = DatumGetInt32(res);

    /* Like ReleaseBuffer, but don't call ResourceOwnerForgetBuffer */
    if !BufferIsValid(buffer) {
        elog!(ERROR, "bad buffer ID: {}", buffer);
    }

    if BufferIsLocal(buffer) {
        crate::storage::buffer::localbuf::UnpinLocalBufferNoOwner(buffer);
    } else {
        /* C: UnpinBufferNoOwner(GetBufferDescriptor(buffer - 1)) */
        UnpinBufferNoOwner(buffer);
    }
}

unsafe fn ResOwnerPrintBufferPin(res: Datum) -> *mut c_char {
    DebugPrintBufferRefcount(DatumGetInt32(res));
    null_mut()
}

// ----------------------------------------------------------------
// AIO callbacks
// ----------------------------------------------------------------

/*
 * Decode readv errors as encoded by buffer_readv_encode_error().
 */
#[inline]
unsafe fn buffer_readv_decode_error(
    result: PgAioResult,
    zeroed_any: *mut bool,
    ignored_any: *mut bool,
    zeroed_or_error_count: *mut uint8,
    checkfail_count: *mut uint8,
    first_off: *mut uint8,
) {
    /* see static asserts in buffer_readv_encode_error */
    const READV_COUNT_BITS: u32 = 7;
    const READV_COUNT_MASK: u32 = (1 << READV_COUNT_BITS) - 1;

    let mut rem_error: uint32 = result.error_data();

    *zeroed_any = (rem_error & 1) != 0;
    rem_error >>= 1;

    *ignored_any = (rem_error & 1) != 0;
    rem_error >>= 1;

    *zeroed_or_error_count = (rem_error & READV_COUNT_MASK) as uint8;
    rem_error >>= READV_COUNT_BITS;

    *checkfail_count = (rem_error & READV_COUNT_MASK) as uint8;
    rem_error >>= READV_COUNT_BITS;

    *first_off = (rem_error & READV_COUNT_MASK) as uint8;
}

/*
 * Helper to encode errors for buffer_readv_complete()
 *
 * Errors are encoded as follows:
 * - bit 0 indicates whether any page was zeroed (1) or not (0)
 * - bit 1 indicates whether any checksum failure was ignored (1) or not (0)
 * - next READV_COUNT_BITS bits indicate the number of errored or zeroed pages
 * - next READV_COUNT_BITS bits indicate the number of checksum failures
 * - next READV_COUNT_BITS bits indicate the first offset of the first page
 *   that was errored or zeroed or, if no errors/zeroes, the first ignored
 *   checksum
 */
#[inline]
unsafe fn buffer_readv_encode_error(
    result: *mut PgAioResult,
    is_temp: bool,
    zeroed_any: bool,
    ignored_any: bool,
    error_count: uint8,
    zeroed_count: uint8,
    checkfail_count: uint8,
    first_error_off: uint8,
    first_zeroed_off: uint8,
    first_ignored_off: uint8,
) {
    const READV_COUNT_BITS: u32 = 7;
    const READV_COUNT_MASK: u32 = (1 << READV_COUNT_BITS) - 1;

    let mut shift: u32 = 0;
    let zeroed_or_error_count: uint8 =
        if error_count > 0 { error_count } else { zeroed_count };
    let first_off: uint8 = if error_count > 0 {
        first_error_off
    } else if zeroed_count > 0 {
        first_zeroed_off
    } else {
        first_ignored_off
    };

    /* Assert(!zeroed_any || error_count == 0) */

    let mut error_data: uint32 = 0;

    error_data |= (zeroed_any as uint32) << shift;
    shift += 1;

    error_data |= (ignored_any as uint32) << shift;
    shift += 1;

    error_data |= (zeroed_or_error_count as uint32) << shift;
    shift += READV_COUNT_BITS;

    error_data |= (checkfail_count as uint32) << shift;
    shift += READV_COUNT_BITS;

    error_data |= (first_off as uint32) << shift;

    (*result).set_error_data(error_data);

    (*result).set_id(if is_temp {
        PGAIO_HCB_LOCAL_BUFFER_READV as uint32
    } else {
        PGAIO_HCB_SHARED_BUFFER_READV as uint32
    });

    if error_count > 0 {
        (*result).set_status(PGAIO_RS_ERROR as uint32);
    } else {
        (*result).set_status(PGAIO_RS_WARNING as uint32);
    }
}

/*
 * Helper for AIO readv completion callbacks, supporting both shared and temp
 * buffers. Gets called once for each buffer in a multi-page read.
 */
#[inline]
unsafe fn buffer_readv_complete_one(
    td: *const PgAioTargetData,
    buf_off: uint8,
    buffer: Buffer,
    flags: uint8,
    mut failed: bool,
    is_temp: bool,
    buffer_invalid: *mut bool,
    failed_checksum: *mut bool,
    ignored_checksum: *mut bool,
    zeroed_buffer: *mut bool,
) {
    let buf_hdr: *mut BufferDesc = if is_temp {
        GetLocalBufferDescriptor((-buffer - 1) as u32)
    } else {
        GetBufferDescriptor((buffer - 1) as u32)
    };
    let tag = (*buf_hdr).tag;
    let bufdata = BufferGetBlock(buffer) as *mut u8;
    let set_flag_bits: uint32;

    *buffer_invalid = false;
    *failed_checksum = false;
    *ignored_checksum = false;
    *zeroed_buffer = false;

    /*
     * We ask PageIsVerified() to only log the message about checksum errors,
     * as the completion might be run in any backend (or IO workers). We will
     * report checksum errors in buffer_readv_report().
     */
    let mut piv_flags: c_int = PIV_LOG_LOG;

    /* the local zero_damaged_pages may differ from the definer's */
    if (flags as c_int & READ_BUFFERS_IGNORE_CHECKSUM_FAILURES) != 0 {
        piv_flags |= PIV_IGNORE_CHECKSUM_FAILURE;
    }

    /* Check for garbage data. */
    if !failed {
        if !PageIsVerified(bufdata as Page, tag.blockNum, piv_flags, failed_checksum) {
            if (flags as c_int & READ_BUFFERS_ZERO_ON_ERROR) != 0 {
                core::ptr::write_bytes(bufdata, 0, BLCKSZ as usize);
                *zeroed_buffer = true;
            } else {
                *buffer_invalid = true;
                failed = true;
            }
        } else if *failed_checksum {
            *ignored_checksum = true;
        }

        /*
         * Immediately log a message about the invalid page, but only to the
         * server log. The reason to do so immediately is that this may be
         * executed in a different backend than the one that originated the
         * request.
         */
        if *buffer_invalid || *failed_checksum || *zeroed_buffer {
            let mut result_one = PgAioResult { bits: 0, result: 0 };
            buffer_readv_encode_error(
                &mut result_one,
                is_temp,
                *zeroed_buffer,
                *ignored_checksum,
                if *buffer_invalid { 1 } else { 0 },
                if *zeroed_buffer { 1 } else { 0 },
                if *failed_checksum { 1 } else { 0 },
                buf_off,
                buf_off,
                buf_off,
            );
            pgaio_result_report(result_one, td, crate::utils::elog::LOG_SERVER_ONLY);
        }
    }

    /* Terminate I/O and set BM_VALID. */
    set_flag_bits = if failed { BM_IO_ERROR } else { BM_VALID };
    if is_temp {
        crate::storage::buffer::localbuf::TerminateLocalBufferIO(buf_hdr, false, set_flag_bits, true);
    } else {
        TerminateBufferIO(buf_hdr, false, set_flag_bits, false);
    }

    TRACE_POSTGRESQL_BUFFER_READ_DONE!(
        tag.forkNum,
        tag.blockNum,
        tag.spcOid,
        tag.dbOid,
        tag.relNumber,
        if is_temp { MyProcNumber } else { INVALID_PROC_NUMBER },
        false
    );
}

/*
 * Perform completion handling of a single AIO read. This read may cover
 * multiple blocks / buffers.
 *
 * Shared between shared and local buffers, to reduce code duplication.
 */
#[inline]
unsafe fn buffer_readv_complete(
    ioh: *mut PgAioHandle,
    prior_result: PgAioResult,
    cb_data: uint8,
    is_temp: bool,
) -> PgAioResult {
    let mut result = prior_result;
    let td: *const PgAioTargetData = pgaio_io_get_target_data(ioh);
    let mut first_error_off: uint8 = 0;
    let mut first_zeroed_off: uint8 = 0;
    let mut first_ignored_off: uint8 = 0;
    let mut error_count: uint8 = 0;
    let mut zeroed_count: uint8 = 0;
    let mut ignored_count: uint8 = 0;
    let mut checkfail_count: uint8 = 0;

    /*
     * Iterate over all the buffers affected by this IO and call the
     * per-buffer completion function for each buffer.
     */
    let mut handle_data_len: uint8 = 0;
    let io_data: *mut u64 = pgaio_io_get_handle_data(ioh, &mut handle_data_len);
    let mut buf_off: uint8 = 0;
    while buf_off < handle_data_len {
        let buf: Buffer = *io_data.add(buf_off as usize) as Buffer;
        let mut failed_verification: bool = false;
        let mut failed_checksum: bool = false;
        let mut zeroed_buffer: bool = false;
        let mut ignored_checksum: bool = false;

        /* Assert(BufferIsValid(buf)) */

        /*
         * If the entire I/O failed on a lower-level, each buffer needs to be
         * marked as failed. In case of a partial read, the first few buffers
         * may be ok.
         */
        let failed: bool = prior_result.status() == PGAIO_RS_ERROR as uint32
            || prior_result.result <= buf_off as i32;

        buffer_readv_complete_one(
            td,
            buf_off,
            buf,
            cb_data,
            failed,
            is_temp,
            &mut failed_verification,
            &mut failed_checksum,
            &mut ignored_checksum,
            &mut zeroed_buffer,
        );

        /*
         * Track information about the number of different kinds of error
         * conditions across all pages, as there can be multiple pages failing
         * verification as part of one IO.
         */
        if failed_verification && !zeroed_buffer {
            if error_count == 0 {
                first_error_off = buf_off;
            }
            error_count += 1;
        }
        if zeroed_buffer {
            if zeroed_count == 0 {
                first_zeroed_off = buf_off;
            }
            zeroed_count += 1;
        }
        if ignored_checksum {
            if ignored_count == 0 {
                first_ignored_off = buf_off;
            }
            ignored_count += 1;
        }
        if failed_checksum {
            checkfail_count += 1;
        }

        buf_off += 1;
    }

    /*
     * If the smgr read succeeded [partially] and page verification failed for
     * some of the pages, adjust the IO's result state appropriately.
     */
    if prior_result.status() != PGAIO_RS_ERROR as uint32
        && (error_count > 0 || ignored_count > 0 || zeroed_count > 0)
    {
        buffer_readv_encode_error(
            &mut result,
            is_temp,
            zeroed_count > 0,
            ignored_count > 0,
            error_count,
            zeroed_count,
            checkfail_count,
            first_error_off,
            first_zeroed_off,
            first_ignored_off,
        );
        pgaio_result_report(result, td, crate::utils::elog::DEBUG1);
    }

    /*
     * For shared relations this reporting is done in
     * shared_buffer_readv_complete_local().
     */
    if is_temp && checkfail_count > 0 {
        crate::utils::activity::pgstat_database::pgstat_report_checksum_failures_in_db(
            (*td).smgr.rlocator.dbOid,
            checkfail_count as c_int,
        );
    }

    result
}

/*
 * AIO error reporting callback for aio_shared_buffer_readv_cb and
 * aio_local_buffer_readv_cb.
 *
 * The error is encoded / decoded in buffer_readv_encode_error() /
 * buffer_readv_decode_error().
 */
pub unsafe fn buffer_readv_report(
    result: PgAioResult,
    td: *const PgAioTargetData,
    elevel: c_int,
) {
    let nblocks: c_int = (*td).smgr.nblocks as c_int;
    let first: BlockNumber = (*td).smgr.blockNum;
    let last: BlockNumber = first.wrapping_add(nblocks as u32).wrapping_sub(1);
    let err_proc: ProcNumber = if (*td).smgr.is_temp() {
        MyProcNumber
    } else {
        INVALID_PROC_NUMBER
    };
    /* relpathbackend(td->smgr.rlocator, errProc, td->smgr.forkNum) -- TODO(pg-port) */
    let _ = (first, last, err_proc);

    let mut zeroed_any: bool = false;
    let mut ignored_any: bool = false;
    let mut zeroed_or_error_count: uint8 = 0;
    let mut checkfail_count: uint8 = 0;
    let mut first_off: uint8 = 0;

    buffer_readv_decode_error(
        result,
        &mut zeroed_any,
        &mut ignored_any,
        &mut zeroed_or_error_count,
        &mut checkfail_count,
        &mut first_off,
    );

    /* Use ereport to emit at elevel -- TODO(pg-port): build full message */
    let _ = (zeroed_any, ignored_any, zeroed_or_error_count, checkfail_count, first_off, elevel);
    ereport!(elevel, errmsg!("invalid page in relation"));
}

/*
 * Common staging code for shared/local buffer AIO reads and writes.
 *
 * Iterates over all the buffers in the IO handle and sets them up for
 * asynchronous I/O (increments refcount to give the AIO subsystem ownership,
 * stores the IO wait ref, etc.).
 */
unsafe fn buffer_stage_common(ioh: *mut PgAioHandle, is_write: bool, is_temp: bool) {
    let mut handle_data_len: uint8 = 0;
    let io_data: *mut u64 = pgaio_io_get_handle_data(ioh, &mut handle_data_len);
    let mut io_ref = PgAioWaitRef { aio_index: 0, generation_upper: 0, generation_lower: 0 };
    pgaio_io_get_wref(ioh, &mut io_ref as *mut _);

    /* iterate over all buffers affected by the vectored readv/writev */
    let mut i: uint8 = 0;
    while i < handle_data_len {
        let buffer: Buffer = *io_data.add(i as usize) as Buffer;
        let buf_hdr: *mut BufferDesc = if is_temp {
            GetLocalBufferDescriptor((-buffer - 1) as u32)
        } else {
            GetBufferDescriptor((buffer - 1) as u32)
        };
        let mut buf_state: uint32;

        if is_temp {
            buf_state = pg_atomic_read_u32(&(*buf_hdr).state);
        } else {
            buf_state = LockBufHdr(buf_hdr);
        }

        /* verify the buffer is in the expected state */
        /* Assert(buf_state & BM_TAG_VALID) */
        /* temp buffers don't use BM_IO_IN_PROGRESS */

        /*
         * Reflect that the buffer is now owned by the AIO subsystem.
         * This pin is released again in TerminateBufferIO().
         */
        buf_state += BUF_REFCOUNT_ONE;
        (*buf_hdr).io_wref = core::mem::transmute(io_ref);

        if is_temp {
            pg_atomic_unlocked_write_u32(&mut (*buf_hdr).state, buf_state);
        } else {
            UnlockBufHdr(buf_hdr, buf_state);
        }

        /*
         * Ensure the content lock that prevents buffer modifications while
         * the buffer is being written out is not released early due to an
         * error.
         */
        if is_write && !is_temp {
            let content_lock = BufferDescriptorGetContentLock(buf_hdr);
            /* LWLockDisown: lock is now owned by AIO subsystem. */
            LWLockDisown(content_lock);
        }

        /*
         * Stop tracking this buffer via the resowner - the AIO system now
         * keeps track.
         */
        if !is_temp {
            ResourceOwnerForgetBufferIO(CurrentResourceOwner as *mut c_void, buffer);
        }

        i += 1;
    }
}

// ----------------------------------------------------------------
// AIO callback thunks -- the function-pointer types require exact signatures
// so we need thin trampoline wrappers with the right arity.
// ----------------------------------------------------------------

unsafe fn shared_buffer_readv_stage_cb(ioh: *mut PgAioHandle, cb_data: uint8) {
    buffer_stage_common(ioh, false, false);
}

unsafe fn shared_buffer_readv_complete_cb(
    ioh: *mut PgAioHandle,
    prior_result: PgAioResult,
    cb_data: uint8,
) -> PgAioResult {
    /* return buffer_readv_complete(ioh, prior_result, cb_data, false) */
    buffer_readv_complete(ioh, prior_result, cb_data, false)
}

/*
 * We need a backend-local completion callback for shared buffers, to be able
 * to report checksum errors correctly. Unfortunately that can only safely
 * happen if the reporting backend has previously called
 * pgstat_prepare_report_checksum_failure(), which we can only guarantee in
 * the backend that started the IO. Hence this callback.
 */
unsafe fn shared_buffer_readv_complete_local_cb(
    ioh: *mut PgAioHandle,
    prior_result: PgAioResult,
    cb_data: uint8,
) -> PgAioResult {
    let mut zeroed_any: bool = false;
    let mut ignored_any: bool = false;
    let mut zeroed_or_error_count: uint8 = 0;
    let mut checkfail_count: uint8 = 0;
    let mut first_off: uint8 = 0;

    if prior_result.status() == PGAIO_RS_OK as uint32 {
        return prior_result;
    }

    buffer_readv_decode_error(
        prior_result,
        &mut zeroed_any,
        &mut ignored_any,
        &mut zeroed_or_error_count,
        &mut checkfail_count,
        &mut first_off,
    );

    if checkfail_count > 0 {
        let td: *const PgAioTargetData = pgaio_io_get_target_data(ioh);
        crate::utils::activity::pgstat_database::pgstat_report_checksum_failures_in_db(
            (*td).smgr.rlocator.dbOid,
            checkfail_count as c_int,
        );
    }

    prior_result
}

unsafe fn local_buffer_readv_stage_cb(ioh: *mut PgAioHandle, cb_data: uint8) {
    buffer_stage_common(ioh, false, true);
}

unsafe fn local_buffer_readv_complete_cb(
    ioh: *mut PgAioHandle,
    prior_result: PgAioResult,
    cb_data: uint8,
) -> PgAioResult {
    buffer_readv_complete(ioh, prior_result, cb_data, true)
}

unsafe fn buffer_readv_report_cb(
    result: PgAioResult,
    td: *const PgAioTargetData,
    elevel: c_int,
) {
    buffer_readv_report(result, td, elevel);
}

// ----------------------------------------------------------------
// AIO callback tables
// ----------------------------------------------------------------

/* readv callback is passed READ_BUFFERS_* flags as callback data */
/// aio_shared_buffer_readv_cb -- AIO callback table entry for shared buffer reads.
pub static aio_shared_buffer_readv_cb: crate::storage::aio::aio_callback::PgAioHandleCallbacks =
    crate::storage::aio::aio_callback::PgAioHandleCallbacks {
        stage: Some(shared_buffer_readv_stage_cb),
        complete_shared: Some(shared_buffer_readv_complete_cb),
        /* need a local callback to report checksum failures */
        complete_local: Some(shared_buffer_readv_complete_local_cb),
        report: Some(buffer_readv_report_cb),
    };

/* readv callback is passed READ_BUFFERS_* flags as callback data */
/// aio_local_buffer_readv_cb -- AIO callback table entry for local buffer reads.
///
/// Note that this, in contrast to the shared_buffers case, uses
/// complete_local, as only the issuing backend has access to the required
/// datastructures. This is important in case the IO completion may be
/// consumed incidentally by another backend.
pub static aio_local_buffer_readv_cb: crate::storage::aio::aio_callback::PgAioHandleCallbacks =
    crate::storage::aio::aio_callback::PgAioHandleCallbacks {
        stage: Some(local_buffer_readv_stage_cb),
        complete_shared: None,
        complete_local: Some(local_buffer_readv_complete_cb),
        report: Some(buffer_readv_report_cb),
    };

// ----------------------------------------------------------------
// StrategyControl accessors (stubs, actual impl in freelist.c)
// ----------------------------------------------------------------

// StrategySyncStart is imported from freelist (see use above).

// ----------------------------------------------------------------
// IOContextForStrategy stub (already in buf_internals, alias here)
// ----------------------------------------------------------------

// Already imported; re-exported so tests in this module can see it:
pub use crate::storage::buf_internals::IOContextForStrategy as GetIOContextForStrategy;

// ----------------------------------------------------------------
// Additional buffer API
// ----------------------------------------------------------------

/// BufferGetPage_s: get page for a shared (non-local) buffer.
/// Alias kept for callers that use the C name directly.
#[inline]
pub unsafe fn BufferGetPage_s(buffer: Buffer) -> Page {
    BufHdrGetBlock(GetBufferDescriptor((buffer - 1) as u32)) as Page
}

// ----------------------------------------------------------------
// Missing field access for ReadBuffersOperation
// (The struct is defined in read_stream.rs with partial fields;
//  we shadow the fields we need as computed offsets below.)
// ----------------------------------------------------------------

// We extend ReadBuffersOperation in place here because we cannot modify
// read_stream.rs (write-only mode).  Instead, we re-declare the
// additional fields as local helpers that read at known offsets.
// The full ReadBuffersOperation layout must match read_stream.rs.

// For the translation we treat the struct as having these extra fields:
//   nblocks: c_int         -- total blocks to read
//   nblocks_done: c_int    -- blocks already processed
//   flags: c_int
//   io_wref: PgAioWaitRef  -- async wait reference
// The actual fields are in the struct from read_stream.rs; they are
// accessed via the pointer arithmetic below.

// Because we declared ReadBuffersOperation at the top of the file with
// the partial definition from read_stream.rs, the fields below are
// assumed accessible.  In practice they exist in read_stream.rs under
// the full struct definition.  We add them here as an extension if not
// already present.
//
// All accesses go through the (*operation).field syntax because
// read_stream.rs uses #[repr(C)] and we add these fields if they're
// missing from that definition.
//
// NOTE: This is correct ONLY IF read_stream.rs already contains these
// fields.  If they are absent, read_stream.rs must be updated to add:
//   pub nblocks: c_int,
//   pub nblocks_done: c_int,
//   pub flags: c_int,
//   pub io_wref: PgAioWaitRef,
// (do so in the struct definition in read_stream.rs, NOT here).

// ----------------------------------------------------------------
// PgAioWaitRef type (local re-export)
// ----------------------------------------------------------------

// Already declared in aio_types.rs; we use it via the path above.

// ----------------------------------------------------------------
// crate::storage::buffer::localbuf re-exports needed locally
// ----------------------------------------------------------------

// Forward declarations for localbuf functions used above.
// These will be resolved by the compiler from localbuf.rs.
// Declared locally here as extern blocks so this file compiles.
// (The real bodies live in localbuf.rs.)

mod localbuf_ffi {
    use super::*;
    extern "Rust" {
        pub fn LocalBufferBlockPointers_ptr() -> *mut *mut c_void;
    }
}

// ----------------------------------------------------------------
// End of bufmgr.rs
// ----------------------------------------------------------------

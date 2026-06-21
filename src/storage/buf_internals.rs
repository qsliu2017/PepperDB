//! storage/buf_internals.h - Internal definitions for buffer manager and buffer replacement strategy.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;

// ---- Imports from canonical homes ----------------------------------------

use crate::storage::block::{BlockNumber, InvalidBlockNumber}; // storage/block.h
use crate::storage::buf::Buffer; // storage/buf.h
use crate::storage::relfilelocator::RelFileLocator; // storage/relfilelocator.h
use crate::common::relpath::{ForkNumber, InvalidForkNumber, RelFileNumber}; // common/relpath.h
use crate::storage::aio_types::PgAioWaitRef; // storage/aio_types.h
use crate::port::atomics::pg_atomic_uint32; // port/atomics.h
use crate::utils::resowner::resowner::{
    ResourceOwner, ResourceOwnerDesc, ResourceOwnerForget, ResourceOwnerRemember,
}; // utils/resowner.h
use crate::utils::activity::pgstat_io::IOContext; // pgstat.h (IOContext)

// ---- Local stubs for types whose canonical home isn't ported yet ----------

/// storage/relfilelocator.h: InvalidRelFileNumber.
// TODO: dedup (also defined in storage/buffer/buf_table.rs)
pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

/// storage/lwlock.h: LWLock. The lightweight lock structure.
// TODO: dedup (stubbed in several modules until storage/lwlock.h is ported)
#[repr(C)]
pub struct LWLock {
    _opaque: [u8; 0],
}

/// storage/lwlock.h: LWLockPadded - cache-line padded LWLock wrapper.
// TODO: dedup
#[repr(C)]
pub union LWLockPadded {
    pub lock: std::mem::ManuallyDrop<LWLock>,
    pub pad: [c_char; 64],
}

/// storage/condition_variable.h: ConditionVariable.
// TODO: dedup (also stubbed in nodes/execnodes.rs)
#[repr(C)]
pub struct ConditionVariable {
    _opaque: [u8; 0],
}

/// storage/condition_variable.h: ConditionVariableMinimallyPadded.
// TODO: dedup
#[repr(C)]
pub union ConditionVariableMinimallyPadded {
    pub cv: std::mem::ManuallyDrop<ConditionVariable>,
    pub pad: [c_char; 64],
}

/// storage/smgr.h: SMgrRelationData / SMgrRelation.
// TODO: dedup (storage/smgr.h not ported yet)
pub type SMgrRelation = *mut SMgrRelationData;
#[repr(C)]
pub struct SMgrRelationData {
    _opaque: [u8; 0],
}

/// storage/bufmgr.h: BufferAccessStrategy.
// TODO: dedup (also defined in storage/buf.rs)
pub type BufferAccessStrategy = *mut BufferAccessStrategyData;
#[repr(C)]
pub struct BufferAccessStrategyData {
    _opaque: [u8; 0],
}

/// storage/bufmgr.h: PrefetchBufferResult.
// TODO: dedup (storage/bufmgr.h not ported yet)
#[repr(C)]
pub struct PrefetchBufferResult {
    pub recent_buffer: Buffer,
    pub initiated_io: bool,
}

/// storage/bufmgr.h: BufferManagerRelation.
// TODO: dedup (storage/bufmgr.h not ported yet)
#[repr(C)]
pub struct BufferManagerRelation {
    _opaque: [u8; 0],
}

/// pg_config_manual.h / lwlock.h: NUM_BUFFER_PARTITIONS. Must be a power of 2.
// TODO: dedup (also defined in storage/buffer/buf_table.rs)
pub const NUM_BUFFER_PARTITIONS: uint32 = 128;

/// lwlock.h: offset into MainLWLockArray for the buffer-mapping partition locks.
// TODO: dedup (storage/lwlock.h not ported yet)
pub const BUFFER_MAPPING_LWLOCK_OFFSET: c_int = 0;

// lwlock.h: the array of named, individually-tracked LWLocks in shared memory.
// TODO: dedup (storage/lwlock.h not ported yet)
#[allow(improper_ctypes)]
extern "C" {
    pub static mut MainLWLockArray: *mut LWLockPadded;
}

/// bufmgr.h: WRITEBACK_MAX_PENDING_FLUSHES.
// TODO: dedup (also defined in pg_config_manual.rs)
pub const WRITEBACK_MAX_PENDING_FLUSHES: usize = 256;

// ---- Atomic / barrier helper stubs ---------------------------------------
//
// These wrappers are normally pulled in from port/atomics.h.  Only the ones
// used by inline functions below are provided here.  // TODO: dedup

#[inline]
unsafe fn pg_write_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
}

#[inline]
unsafe fn pg_atomic_write_u32(_ptr: *mut pg_atomic_uint32, _val: uint32) {
    (*_ptr).value.store(_val, core::sync::atomic::Ordering::Release);
}

// ==========================================================================
// Buffer state: a single 32-bit variable combining refcount, usagecount and
// flags.
// ==========================================================================

pub const BUF_REFCOUNT_BITS: u32 = 18;
pub const BUF_USAGECOUNT_BITS: u32 = 4;
pub const BUF_FLAG_BITS: u32 = 10;

// StaticAssertDecl(BUF_REFCOUNT_BITS + BUF_USAGECOUNT_BITS + BUF_FLAG_BITS == 32)
const _: () = assert!(
    BUF_REFCOUNT_BITS + BUF_USAGECOUNT_BITS + BUF_FLAG_BITS == 32,
    "parts of buffer state space need to equal 32"
);

pub const BUF_REFCOUNT_ONE: u32 = 1;
pub const BUF_REFCOUNT_MASK: u32 = (1u32 << BUF_REFCOUNT_BITS) - 1;
pub const BUF_USAGECOUNT_MASK: u32 = ((1u32 << BUF_USAGECOUNT_BITS) - 1) << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_ONE: u32 = 1u32 << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_SHIFT: u32 = BUF_REFCOUNT_BITS;
pub const BUF_FLAG_MASK: u32 =
    ((1u32 << BUF_FLAG_BITS) - 1) << (BUF_REFCOUNT_BITS + BUF_USAGECOUNT_BITS);

/// Get refcount from buffer state.
#[inline]
pub fn BUF_STATE_GET_REFCOUNT(state: u32) -> u32 {
    state & BUF_REFCOUNT_MASK
}

/// Get usagecount from buffer state.
#[inline]
pub fn BUF_STATE_GET_USAGECOUNT(state: u32) -> u32 {
    (state & BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT
}

// ---- Flags for buffer descriptors ----------------------------------------

pub const BM_LOCKED: u32 = 1u32 << 22; /* buffer header is locked */
pub const BM_DIRTY: u32 = 1u32 << 23; /* data needs writing */
pub const BM_VALID: u32 = 1u32 << 24; /* data is valid */
pub const BM_TAG_VALID: u32 = 1u32 << 25; /* tag is assigned */
pub const BM_IO_IN_PROGRESS: u32 = 1u32 << 26; /* read or write in progress */
pub const BM_IO_ERROR: u32 = 1u32 << 27; /* previous I/O failed */
pub const BM_JUST_DIRTIED: u32 = 1u32 << 28; /* dirtied since write started */
pub const BM_PIN_COUNT_WAITER: u32 = 1u32 << 29; /* have waiter for sole pin */
pub const BM_CHECKPOINT_NEEDED: u32 = 1u32 << 30; /* must write for checkpoint */
pub const BM_PERMANENT: u32 = 1u32 << 31; /* permanent buffer (not unlogged, or init fork) */

/// The maximum allowed value of usage_count.
pub const BM_MAX_USAGE_COUNT: u32 = 5;

// StaticAssertDecl(BM_MAX_USAGE_COUNT < (1 << BUF_USAGECOUNT_BITS))
const _: () = assert!(
    BM_MAX_USAGE_COUNT < (1u32 << BUF_USAGECOUNT_BITS),
    "BM_MAX_USAGE_COUNT doesn't fit in BUF_USAGECOUNT_BITS bits"
);
// NB: the MAX_BACKENDS_BITS <= BUF_REFCOUNT_BITS StaticAssertDecl is omitted
// here because MAX_BACKENDS_BITS lives in storage/procnumber.h (not yet ported).

// ==========================================================================
// BufferTag
// ==========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BufferTag {
    pub spcOid: Oid,             /* tablespace oid */
    pub dbOid: Oid,              /* database oid */
    pub relNumber: RelFileNumber, /* relation file number */
    pub forkNum: ForkNumber,     /* fork number */
    pub blockNum: BlockNumber,   /* blknum relative to begin of reln */
}

#[inline]
pub unsafe fn BufTagGetRelNumber(tag: *const BufferTag) -> RelFileNumber {
    (*tag).relNumber
}

#[inline]
pub unsafe fn BufTagGetForkNum(tag: *const BufferTag) -> ForkNumber {
    (*tag).forkNum
}

#[inline]
pub unsafe fn BufTagSetRelForkDetails(
    tag: *mut BufferTag,
    relnumber: RelFileNumber,
    forknum: ForkNumber,
) {
    (*tag).relNumber = relnumber;
    (*tag).forkNum = forknum;
}

#[inline]
pub unsafe fn BufTagGetRelFileLocator(tag: *const BufferTag) -> RelFileLocator {
    let mut rlocator: RelFileLocator = std::mem::zeroed();

    rlocator.spcOid = (*tag).spcOid;
    rlocator.dbOid = (*tag).dbOid;
    rlocator.relNumber = BufTagGetRelNumber(tag);

    rlocator
}

#[inline]
pub unsafe fn ClearBufferTag(tag: *mut BufferTag) {
    (*tag).spcOid = InvalidOid;
    (*tag).dbOid = InvalidOid;
    BufTagSetRelForkDetails(tag, InvalidRelFileNumber, InvalidForkNumber);
    (*tag).blockNum = InvalidBlockNumber;
}

#[inline]
pub unsafe fn InitBufferTag(
    tag: *mut BufferTag,
    rlocator: *const RelFileLocator,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) {
    (*tag).spcOid = (*rlocator).spcOid;
    (*tag).dbOid = (*rlocator).dbOid;
    BufTagSetRelForkDetails(tag, (*rlocator).relNumber, forkNum);
    (*tag).blockNum = blockNum;
}

#[inline]
pub unsafe fn BufferTagsEqual(tag1: *const BufferTag, tag2: *const BufferTag) -> bool {
    (*tag1).spcOid == (*tag2).spcOid
        && (*tag1).dbOid == (*tag2).dbOid
        && (*tag1).relNumber == (*tag2).relNumber
        && (*tag1).blockNum == (*tag2).blockNum
        && (*tag1).forkNum == (*tag2).forkNum
}

#[inline]
pub unsafe fn BufTagMatchesRelFileLocator(
    tag: *const BufferTag,
    rlocator: *const RelFileLocator,
) -> bool {
    (*tag).spcOid == (*rlocator).spcOid
        && (*tag).dbOid == (*rlocator).dbOid
        && BufTagGetRelNumber(tag) == (*rlocator).relNumber
}

// ==========================================================================
// Shared buffer mapping table partitioning
// ==========================================================================

#[inline]
pub fn BufTableHashPartition(hashcode: uint32) -> uint32 {
    hashcode % NUM_BUFFER_PARTITIONS
}

#[inline]
pub unsafe fn BufMappingPartitionLock(hashcode: uint32) -> *mut LWLock {
    let idx = BUFFER_MAPPING_LWLOCK_OFFSET as usize + BufTableHashPartition(hashcode) as usize;
    let entry = MainLWLockArray.add(idx);
    &mut *(*entry).lock as *mut LWLock
}

#[inline]
pub unsafe fn BufMappingPartitionLockByIndex(index: uint32) -> *mut LWLock {
    let idx = BUFFER_MAPPING_LWLOCK_OFFSET as usize + index as usize;
    let entry = MainLWLockArray.add(idx);
    &mut *(*entry).lock as *mut LWLock
}

// ==========================================================================
// BufferDesc -- shared descriptor/state data for a single shared buffer.
// ==========================================================================

#[repr(C)]
pub struct BufferDesc {
    pub tag: BufferTag, /* ID of page contained in buffer */
    pub buf_id: c_int,  /* buffer's index number (from 0) */

    /* state of the tag, containing flags, refcount and usagecount */
    pub state: pg_atomic_uint32,

    pub wait_backend_pgprocno: c_int, /* backend of pin-count waiter */
    pub freeNext: c_int,              /* link in freelist chain */

    pub io_wref: PgAioWaitRef, /* set iff AIO is in progress */
    pub content_lock: LWLock,  /* to lock access to buffer contents */
}

/// BUFFERDESC_PAD_TO_SIZE = (SIZEOF_VOID_P == 8 ? 64 : 1)
pub const BUFFERDESC_PAD_TO_SIZE: usize = if std::mem::size_of::<*const c_void>() == 8 {
    64
} else {
    1
};

#[repr(C)]
pub union BufferDescPadded {
    pub bufferdesc: std::mem::ManuallyDrop<BufferDesc>,
    pub pad: [c_char; BUFFERDESC_PAD_TO_SIZE],
}

// ==========================================================================
// PendingWriteback / WritebackContext
// ==========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PendingWriteback {
    /* could store different types of pending flushes here */
    pub tag: BufferTag,
}

/* struct forward declared in bufmgr.h */
#[repr(C)]
pub struct WritebackContext {
    /* pointer to the max number of writeback requests to coalesce */
    pub max_pending: *mut c_int,

    /* current number of pending writeback requests */
    pub nr_pending: c_int,

    /* pending requests */
    pub pending_writebacks: [PendingWriteback; WRITEBACK_MAX_PENDING_FLUSHES],
}

// ---- Globals (in buf_init.c / localbuf.c) --------------------------------

#[allow(improper_ctypes)]
extern "C" {
    /* in buf_init.c */
    pub static mut BufferDescriptors: *mut BufferDescPadded;
    pub static mut BufferIOCVArray: *mut ConditionVariableMinimallyPadded;
    pub static mut BackendWritebackContext: WritebackContext;
}

/* in localbuf.c (canonical home) */
#[no_mangle]
pub static mut LocalBufferDescriptors: *mut BufferDesc = core::ptr::null_mut();

#[inline]
pub unsafe fn GetBufferDescriptor(id: uint32) -> *mut BufferDesc {
    let entry = BufferDescriptors.add(id as usize);
    &mut *(*entry).bufferdesc as *mut BufferDesc
}

#[inline]
pub unsafe fn GetLocalBufferDescriptor(id: uint32) -> *mut BufferDesc {
    LocalBufferDescriptors.add(id as usize)
}

#[inline]
pub unsafe fn BufferDescriptorGetBuffer(bdesc: *const BufferDesc) -> Buffer {
    ((*bdesc).buf_id + 1) as Buffer
}

#[inline]
pub unsafe fn BufferDescriptorGetIOCV(bdesc: *const BufferDesc) -> *mut ConditionVariable {
    let entry = BufferIOCVArray.add((*bdesc).buf_id as usize);
    &mut *(*entry).cv as *mut ConditionVariable
}

#[inline]
pub unsafe fn BufferDescriptorGetContentLock(bdesc: *const BufferDesc) -> *mut LWLock {
    &(*bdesc).content_lock as *const LWLock as *mut LWLock
}

// ---- freeNext special values ---------------------------------------------

pub const FREENEXT_END_OF_LIST: c_int = -1;
pub const FREENEXT_NOT_IN_LIST: c_int = -2;

// ---- Buffer header spinlock ----------------------------------------------

pub unsafe fn LockBufHdr(desc: *mut BufferDesc) -> uint32 {
    let old = (*desc).state.value.fetch_or(BM_LOCKED, core::sync::atomic::Ordering::Acquire);
    old | BM_LOCKED
}

#[inline]
pub unsafe fn UnlockBufHdr(desc: *mut BufferDesc, buf_state: uint32) {
    pg_write_barrier();
    pg_atomic_write_u32(&mut (*desc).state, buf_state & (!BM_LOCKED));
}

// ==========================================================================
// CkptSortItem -- structure to sort buffers per file on checkpoints.
// ==========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CkptSortItem {
    pub tsId: Oid,
    pub relNumber: RelFileNumber,
    pub forkNum: ForkNumber,
    pub blockNum: BlockNumber,
    pub buf_id: c_int,
}

#[allow(improper_ctypes)]
extern "C" {
    pub static mut CkptBufferIds: *mut CkptSortItem;

    /* ResourceOwner callbacks to hold buffer I/Os and pins */
    pub static buffer_io_resowner_desc: ResourceOwnerDesc;
    pub static buffer_pin_resowner_desc: ResourceOwnerDesc;
}

// ---- Convenience wrappers over ResourceOwnerRemember/Forget --------------

#[inline]
pub unsafe fn ResourceOwnerRememberBuffer(owner: ResourceOwner, buffer: Buffer) {
    ResourceOwnerRemember(
        owner,
        Int32GetDatum(buffer),
        &buffer_pin_resowner_desc,
    );
}

#[inline]
pub unsafe fn ResourceOwnerForgetBuffer(owner: ResourceOwner, buffer: Buffer) {
    ResourceOwnerForget(
        owner,
        Int32GetDatum(buffer),
        &buffer_pin_resowner_desc,
    );
}

#[inline]
pub unsafe fn ResourceOwnerRememberBufferIO(owner: ResourceOwner, buffer: Buffer) {
    ResourceOwnerRemember(
        owner,
        Int32GetDatum(buffer),
        &buffer_io_resowner_desc,
    );
}

#[inline]
pub unsafe fn ResourceOwnerForgetBufferIO(owner: ResourceOwner, buffer: Buffer) {
    ResourceOwnerForget(
        owner,
        Int32GetDatum(buffer),
        &buffer_io_resowner_desc,
    );
}

// ==========================================================================
// Internal buffer management routines (prototypes)
// ==========================================================================

/* bufmgr.c */
pub unsafe fn WritebackContextInit(context: *mut WritebackContext, max_pending: *mut c_int) {
    crate::storage::buffer::bufmgr::WritebackContextInit(context as _, max_pending)
}
pub unsafe fn IssuePendingWritebacks(wb_context: *mut WritebackContext, io_context: IOContext) {
    let _ = (wb_context, io_context);
    unimplemented!()
}
pub unsafe fn ScheduleBufferTagForWriteback(
    wb_context: *mut WritebackContext,
    io_context: IOContext,
    tag: *mut BufferTag,
) {
    let _ = (wb_context, io_context, tag);
    unimplemented!()
}

/* solely to make it easier to write tests */
pub unsafe fn StartBufferIO(buf: *mut BufferDesc, forInput: bool, nowait: bool) -> bool {
    crate::storage::buffer::bufmgr::StartBufferIO(buf as _, forInput, nowait)
}
pub unsafe fn TerminateBufferIO(
    buf: *mut BufferDesc,
    clear_dirty: bool,
    set_flag_bits: uint32,
    forget_owner: bool,
    release_aio: bool,
) {
    crate::storage::buffer::bufmgr::TerminateBufferIO(buf as _, clear_dirty, set_flag_bits, forget_owner)
}

/* freelist.c */
pub unsafe fn IOContextForStrategy(strategy: BufferAccessStrategy) -> IOContext {
    core::mem::transmute(crate::storage::buffer::freelist::IOContextForStrategy(strategy as _))
}
pub unsafe fn StrategyGetBuffer(
    strategy: BufferAccessStrategy,
    buf_state: *mut uint32,
    from_ring: *mut bool,
) -> *mut BufferDesc {
    crate::storage::buffer::freelist::StrategyGetBuffer(strategy as _, buf_state, from_ring) as _
}
pub unsafe fn StrategyFreeBuffer(buf: *mut BufferDesc) {
    crate::storage::buffer::freelist::StrategyFreeBuffer(buf as _)
}
pub unsafe fn StrategyRejectBuffer(
    strategy: BufferAccessStrategy,
    buf: *mut BufferDesc,
    from_ring: bool,
) -> bool {
    crate::storage::buffer::freelist::StrategyRejectBuffer(strategy as _, buf as _, from_ring)
}

pub unsafe fn StrategySyncStart(complete_passes: *mut uint32, num_buf_alloc: *mut uint32) -> c_int {
    crate::storage::buffer::freelist::StrategySyncStart(complete_passes, num_buf_alloc)
}
pub unsafe fn StrategyNotifyBgWriter(bgwprocno: c_int) {
    crate::storage::buffer::freelist::StrategyNotifyBgWriter(bgwprocno)
}

pub unsafe fn StrategyShmemSize() -> Size {
    crate::storage::buffer::freelist::StrategyShmemSize()
}
pub unsafe fn StrategyInitialize(init: bool) {
    crate::storage::buffer::freelist::StrategyInitialize(init)
}
pub unsafe fn have_free_buffer() -> bool {
    crate::storage::buffer::freelist::have_free_buffer()
}

/* buf_table.c */
pub unsafe fn BufTableShmemSize(size: c_int) -> Size {
    crate::storage::buffer::buf_table::BufTableShmemSize(size)
}
pub unsafe fn InitBufTable(size: c_int) {
    crate::storage::buffer::buf_table::InitBufTable(size)
}
pub unsafe fn BufTableHashCode(tagPtr: *mut BufferTag) -> uint32 {
    crate::storage::buffer::buf_table::BufTableHashCode(tagPtr as _)
}
pub unsafe fn BufTableLookup(tagPtr: *mut BufferTag, hashcode: uint32) -> c_int {
    crate::storage::buffer::buf_table::BufTableLookup(tagPtr as _, hashcode)
}
pub unsafe fn BufTableInsert(tagPtr: *mut BufferTag, hashcode: uint32, buf_id: c_int) -> c_int {
    crate::storage::buffer::buf_table::BufTableInsert(tagPtr as _, hashcode, buf_id)
}
pub unsafe fn BufTableDelete(tagPtr: *mut BufferTag, hashcode: uint32) {
    crate::storage::buffer::buf_table::BufTableDelete(tagPtr as _, hashcode)
}

/* localbuf.c */
pub unsafe fn PinLocalBuffer(buf_hdr: *mut BufferDesc, adjust_usagecount: bool) -> bool {
    crate::storage::buffer::localbuf::PinLocalBuffer(buf_hdr as _, adjust_usagecount)
}
pub unsafe fn UnpinLocalBuffer(buffer: Buffer) {
    crate::storage::buffer::localbuf::UnpinLocalBuffer(buffer)
}
pub unsafe fn UnpinLocalBufferNoOwner(buffer: Buffer) {
    crate::storage::buffer::localbuf::UnpinLocalBufferNoOwner(buffer)
}
pub unsafe fn PrefetchLocalBuffer(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) -> PrefetchBufferResult {
    crate::storage::buffer::localbuf::PrefetchLocalBuffer(smgr as _, forkNum, blockNum)
}
pub unsafe fn LocalBufferAlloc(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    foundPtr: *mut bool,
) -> *mut BufferDesc {
    crate::storage::buffer::localbuf::LocalBufferAlloc(smgr as _, forkNum, blockNum, foundPtr) as _
}
pub unsafe fn ExtendBufferedRelLocal(
    bmr: BufferManagerRelation,
    fork: ForkNumber,
    flags: uint32,
    extend_by: uint32,
    extend_upto: BlockNumber,
    buffers: *mut Buffer,
    extended_by: *mut uint32,
) -> BlockNumber {
    let _ = (bmr, fork, flags, extend_by, extend_upto, buffers, extended_by);
    unimplemented!()
}
pub unsafe fn MarkLocalBufferDirty(buffer: Buffer) {
    crate::storage::buffer::localbuf::MarkLocalBufferDirty(buffer)
}
pub unsafe fn TerminateLocalBufferIO(
    bufHdr: *mut BufferDesc,
    clear_dirty: bool,
    set_flag_bits: uint32,
    release_aio: bool,
) {
    crate::storage::buffer::localbuf::TerminateLocalBufferIO(bufHdr as _, clear_dirty, set_flag_bits, release_aio)
}
pub unsafe fn StartLocalBufferIO(bufHdr: *mut BufferDesc, forInput: bool, nowait: bool) -> bool {
    crate::storage::buffer::localbuf::StartLocalBufferIO(bufHdr as _, forInput, nowait)
}
pub unsafe fn FlushLocalBuffer(bufHdr: *mut BufferDesc, reln: SMgrRelation) {
    crate::storage::buffer::localbuf::FlushLocalBuffer(bufHdr as _, reln as _)
}
pub unsafe fn InvalidateLocalBuffer(bufHdr: *mut BufferDesc, check_unreferenced: bool) {
    crate::storage::buffer::localbuf::InvalidateLocalBuffer(bufHdr as _, check_unreferenced)
}
pub unsafe fn DropRelationLocalBuffers(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    firstDelBlock: BlockNumber,
) {
    crate::storage::buffer::localbuf::DropRelationLocalBuffers(rlocator, forkNum, firstDelBlock)
}
pub unsafe fn DropRelationAllLocalBuffers(rlocator: RelFileLocator) {
    crate::storage::buffer::localbuf::DropRelationAllLocalBuffers(rlocator)
}
pub unsafe fn AtEOXact_LocalBuffers(isCommit: bool) {
    crate::storage::buffer::localbuf::AtEOXact_LocalBuffers(isCommit)
}

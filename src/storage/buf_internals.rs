//! Translated from PostgreSQL src/include/storage/buf_internals.h
//!
//! Internal definitions for the buffer manager and replacement strategy.
//!
//! foundation-rewrite (buffer-manager): the real shared buffer pool is redesigned
//! later. Here we stub the public API enough for cross-references: the on-disk-
//! style hash key (BufferTag), the BM_* state flags as bitflags over the u32
//! state word, the BufferDesc descriptor (shmem -> in-memory), WritebackContext,
//! and the internal routines as `// TODO(buffer-manager)` stubs.

use bitflags::bitflags;

use crate::common::relpath::{ForkNumber, RelFileNumber, InvalidRelFileNumber};
use crate::pg_config_manual::WRITEBACK_MAX_PENDING_FLUSHES;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::aio_types::PgAioWaitRef;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::relfilelocator::RelFileLocator;

/// Buffer state packing: a single 32-bit word combining 18 bits refcount, 4 bits
/// usagecount, 10 bits of flags, manipulated by CAS to avoid header locking.
/// NOTE(buffer-manager): in the rewrite this word is an `AtomicU32`; the bit
/// layout below is preserved so the accessor consts stay valid.
pub const BUF_REFCOUNT_BITS: u32 = 18;
pub const BUF_USAGECOUNT_BITS: u32 = 4;
pub const BUF_FLAG_BITS: u32 = 10;

const _: () = assert!(BUF_REFCOUNT_BITS + BUF_USAGECOUNT_BITS + BUF_FLAG_BITS == 32);

pub const BUF_REFCOUNT_ONE: u32 = 1;
pub const BUF_REFCOUNT_MASK: u32 = (1u32 << BUF_REFCOUNT_BITS) - 1;
pub const BUF_USAGECOUNT_MASK: u32 = ((1u32 << BUF_USAGECOUNT_BITS) - 1) << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_ONE: u32 = 1u32 << BUF_REFCOUNT_BITS;
pub const BUF_USAGECOUNT_SHIFT: u32 = BUF_REFCOUNT_BITS;
pub const BUF_FLAG_MASK: u32 =
    ((1u32 << BUF_FLAG_BITS) - 1) << (BUF_REFCOUNT_BITS + BUF_USAGECOUNT_BITS);

pub const fn buf_state_get_refcount(state: u32) -> u32 {
    state & BUF_REFCOUNT_MASK
}
pub const fn buf_state_get_usagecount(state: u32) -> u32 {
    (state & BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT
}

bitflags! {
    /// BM_* flag bits living in the high 10 bits of the u32 buffer state word.
    /// Single-bit independent flags -> bitflags; byte-compatible with the C
    /// `#define BM_*`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BufFlags: u32 {
        const LOCKED            = 1 << 22; // buffer header is locked
        const DIRTY             = 1 << 23; // data needs writing
        const VALID             = 1 << 24; // data is valid
        const TAG_VALID         = 1 << 25; // tag is assigned
        const IO_IN_PROGRESS    = 1 << 26; // read or write in progress
        const IO_ERROR          = 1 << 27; // previous I/O failed
        const JUST_DIRTIED      = 1 << 28; // dirtied since write started
        const PIN_COUNT_WAITER  = 1 << 29; // have waiter for sole pin
        const CHECKPOINT_NEEDED = 1 << 30; // must write for checkpoint
        const PERMANENT         = 1 << 31; // permanent (not unlogged/init fork)
    }
}

/// Max usage_count for the clock-sweep algorithm.
pub const BM_MAX_USAGE_COUNT: u32 = 5;
const _: () = assert!(BM_MAX_USAGE_COUNT < (1 << BUF_USAGECOUNT_BITS));

/// BufferTag: identifies which disk block a buffer contains. Used as a hash key
/// in the buffer mapping table, so it is `#[repr(C)]` + `Hash`/`Eq` and must
/// have no padding (5 x 4-byte fields = 20 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct BufferTag {
    pub spc_oid: Oid,            // tablespace oid
    pub db_oid: Oid,             // database oid
    pub rel_number: RelFileNumber, // relation file number
    pub fork_num: ForkNumber,    // fork number
    pub block_num: BlockNumber,  // block relative to start of reln
}

const _: () = assert!(core::mem::size_of::<BufferTag>() == 20);

impl BufferTag {
    pub fn rel_number(&self) -> RelFileNumber {
        self.rel_number
    }
    pub fn fork_num(&self) -> ForkNumber {
        self.fork_num
    }
    pub fn set_rel_fork_details(&mut self, relnumber: RelFileNumber, forknum: ForkNumber) {
        self.rel_number = relnumber;
        self.fork_num = forknum;
    }
    pub fn rel_file_locator(&self) -> RelFileLocator {
        RelFileLocator {
            spcOid: self.spc_oid,
            dbOid: self.db_oid,
            relNumber: self.rel_number(),
        }
    }
    pub fn clear(&mut self) {
        self.spc_oid = InvalidOid;
        self.db_oid = InvalidOid;
        self.set_rel_fork_details(InvalidRelFileNumber, ForkNumber::InvalidForkNumber);
        self.block_num = INVALID_BLOCK_NUMBER;
    }
    pub fn init(rlocator: &RelFileLocator, fork_num: ForkNumber, block_num: BlockNumber) -> Self {
        BufferTag {
            spc_oid: rlocator.spcOid,
            db_oid: rlocator.dbOid,
            rel_number: rlocator.relNumber,
            fork_num,
            block_num,
        }
    }
    /// Compares everything except the fork+block on a locator-only match.
    pub fn matches_rel_file_locator(&self, rlocator: &RelFileLocator) -> bool {
        self.spc_oid == rlocator.spcOid
            && self.db_oid == rlocator.dbOid
            && self.rel_number() == rlocator.relNumber
    }
}

/// BufferDesc: per-buffer descriptor.
///
/// NOTE(buffer-manager): in C this is a 64-byte cache-line-aligned shmem struct
/// whose `state` is a `pg_atomic_uint32` packing flags+refcount+usagecount, with
/// an embedded `content_lock` LWLock and a header spinlock (BM_LOCKED). Under the
/// single-process port it is plain in-memory: `state` is an `AtomicU32`, the
/// content lock a `parking_lot` RwLock added in the rewrite. No layout contract.
pub struct BufferDesc {
    pub tag: BufferTag,
    pub buf_id: i32,
    /// Packed flags/refcount/usagecount (see BUF_* / BufFlags). -> AtomicU32 later.
    pub state: std::sync::atomic::AtomicU32,
    pub wait_backend_pgprocno: i32,
    pub free_next: i32,
    pub io_wref: PgAioWaitRef,
    // content_lock: LWLock -> parking_lot::RwLock added in the rewrite.
}

/// Special freeNext sentinels.
pub const FREENEXT_END_OF_LIST: i32 = -1;
pub const FREENEXT_NOT_IN_LIST: i32 = -2;

/// One pending OS writeback request.
pub struct PendingWriteback {
    pub tag: BufferTag,
}

/// Accumulated pending writeback requests to coalesce before issuing to the OS.
pub struct WritebackContext {
    /// Max number of writeback requests to coalesce (C: `int *max_pending`).
    pub max_pending: i32,
    pub nr_pending: i32,
    pub pending_writebacks: Vec<PendingWriteback>,
}

const _: () = assert!(WRITEBACK_MAX_PENDING_FLUSHES == 256);

/// Per-buffer checkpoint sort key.
pub struct CkptSortItem {
    pub ts_id: Oid,
    pub rel_number: RelFileNumber,
    pub fork_num: ForkNumber,
    pub block_num: BlockNumber,
    pub buf_id: i32,
}

// --- Internal buffer management routines: TODO(buffer-manager) stubs ---

// bufmgr.c
pub fn writeback_context_init(_context: &mut WritebackContext, _max_pending: i32) {
    unimplemented!() // TODO(buffer-manager)
}
pub fn lock_buf_hdr(_desc: &mut BufferDesc) -> u32 {
    unimplemented!() // TODO(buffer-manager)
}
pub fn unlock_buf_hdr(_desc: &mut BufferDesc, _buf_state: u32) {
    unimplemented!() // TODO(buffer-manager)
}
pub fn start_buffer_io(_buf: &mut BufferDesc, _for_input: bool, _nowait: bool) -> bool {
    unimplemented!() // TODO(buffer-manager)
}
pub fn terminate_buffer_io(
    _buf: &mut BufferDesc,
    _clear_dirty: bool,
    _set_flag_bits: u32,
    _forget_owner: bool,
    _release_aio: bool,
) {
    unimplemented!() // TODO(buffer-manager)
}

// buf_table.c
pub fn buf_table_shmem_size(_size: i32) -> usize {
    unimplemented!() // TODO(buffer-manager)
}
pub fn init_buf_table(_size: i32) {
    unimplemented!() // TODO(buffer-manager)
}
pub fn buf_table_hash_code(_tag: &BufferTag) -> u32 {
    unimplemented!() // TODO(buffer-manager)
}
/// C returns buf_id or -1; -1 -> None.
pub fn buf_table_lookup(_tag: &BufferTag, _hashcode: u32) -> Option<i32> {
    unimplemented!() // TODO(buffer-manager)
}
pub fn buf_table_insert(_tag: &BufferTag, _hashcode: u32, _buf_id: i32) -> Option<i32> {
    unimplemented!() // TODO(buffer-manager)
}
pub fn buf_table_delete(_tag: &BufferTag, _hashcode: u32) {
    unimplemented!() // TODO(buffer-manager)
}

pub fn buffer_descriptor_get_buffer(bdesc: &BufferDesc) -> Buffer {
    bdesc.buf_id + 1
}

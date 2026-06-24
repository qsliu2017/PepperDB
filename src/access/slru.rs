//! Translated from PostgreSQL src/include/access/slru.h
//! Simple LRU buffering for transaction status logfiles.
//!
//! Shared-memory note: SlruSharedData lived in shmem and was protected by
//! LWLock banks. Under the single-process async model the shmem indirection
//! collapses: the buffer arrays become owned Vecs and the LWLocks become
//! `parking_lot`/`std` locks (translation addendum). Kept in-memory (NOT
//! `#[repr(C)]`): SLRU pages are written to disk individually, but these control
//! structs are not themselves on-disk layouts.

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::storage::sync::{FileTag, SyncRequestHandler};

/// Max number of buffers, to avoid overflowing size_t arithmetic.
pub const SLRU_MAX_ALLOWED_BUFFERS: usize = (1024 * 1024 * 1024) / BLCKSZ as usize;

/// SLRU segment size in pages (32 pages = 256Kb).
pub const SLRU_PAGES_PER_SEGMENT: i32 = 32;

/// Page status codes (does not include the "dirty" bit).
pub enum SlruPageStatus {
    Empty,             // buffer is not in use
    ReadInProgress,    // page is being read in
    Valid,             // page is valid and not being written
    WriteInProgress,   // page is being written out
}

/// Shared-memory state (single-process: owned arrays + std/parking_lot locks).
pub struct SlruSharedData {
    /// Number of buffers managed by this SLRU structure.
    pub num_slots: i32,

    /// Per-slot arrays. Page number is undefined when status is Empty.
    pub page_buffer: Vec<Vec<u8>>,
    pub page_status: Vec<SlruPageStatus>,
    pub page_dirty: Vec<bool>,
    pub page_number: Vec<i64>,
    pub page_lru_count: Vec<i32>,

    /// I/O lock per buffer slot (was LWLockPadded *buffer_locks).
    pub buffer_locks: Vec<std::sync::Mutex<()>>,
    /// In-memory buffer slot access lock per SLRU bank (was bank_locks).
    pub bank_locks: Vec<std::sync::Mutex<()>>,

    /// Bank-wise LRU counter (one per bank).
    pub bank_cur_lru_count: Vec<i32>,

    /// Optional WAL flush LSNs per entry; if empty, no WAL flush needed.
    pub group_lsn: Vec<XLogRecPtr>,
    pub lsn_groups_per_page: i32,

    /// Page number of the current end of the log (was pg_atomic_uint64).
    pub latest_page_number: std::sync::atomic::AtomicU64,

    /// SLRU's index for statistics purposes (might not be unique).
    pub slru_stats_idx: i32,
}

pub type SlruShared = *mut SlruSharedData; // TODO(ptr)

/// SlruCtlData points to the active shared information.
pub struct SlruCtlData {
    pub shared: SlruShared,

    /// Number of banks in this SLRU.
    pub nbanks: u16,

    /// If true, use long segment file names.
    pub long_segment_names: bool,

    /// Sync handler to use when handing sync requests to the checkpointer.
    pub sync_handler: SyncRequestHandler,

    /// Decide whether a page is "older" for truncation / LRU eviction.
    /// Returns true if every entry of the first arg is older than the second.
    pub PagePrecedes: fn(i64, i64) -> bool,

    /// Directory; set during SimpleLruInit and constant thereafter.
    pub Dir: [u8; 64],
}

pub type SlruCtl = *mut SlruCtlData; // TODO(ptr)

// SimpleLruGetBankLock(ctl, pageno) returned &ctl->shared->bank_locks[bankno];
// translate as an accessor once SlruCtl/SlruShared are real owned types.
// (Original: bankno = pageno % ctl->nbanks.)

pub fn SimpleLruShmemSize(_nslots: i32, _nlsns: i32) -> usize {
    unimplemented!()
}

pub fn SimpleLruAutotuneBuffers(_divisor: i32, _max: i32) -> i32 {
    unimplemented!()
}

pub fn SimpleLruInit(
    _ctl: SlruCtl,
    _name: &str,
    _nslots: i32,
    _nlsns: i32,
    _subdir: &str,
    _buffer_tranche_id: i32,
    _bank_tranche_id: i32,
    _sync_handler: SyncRequestHandler,
    _long_segment_names: bool,
) {
    unimplemented!()
}

pub fn SimpleLruZeroPage(_ctl: SlruCtl, _pageno: i64) -> i32 {
    unimplemented!()
}

pub fn SimpleLruReadPage(_ctl: SlruCtl, _pageno: i64, _write_ok: bool, _xid: TransactionId) -> i32 {
    unimplemented!()
}

pub fn SimpleLruReadPage_ReadOnly(_ctl: SlruCtl, _pageno: i64, _xid: TransactionId) -> i32 {
    unimplemented!()
}

pub fn SimpleLruWritePage(_ctl: SlruCtl, _slotno: i32) {
    unimplemented!()
}

pub fn SimpleLruWriteAll(_ctl: SlruCtl, _allow_redirtied: bool) {
    unimplemented!()
}

/// No-op unless USE_ASSERT_CHECKING; kept as a stub.
pub fn SlruPagePrecedesUnitTests(_ctl: SlruCtl, _per_page: i32) {}

pub fn SimpleLruTruncate(_ctl: SlruCtl, _cutoff_page: i64) {
    unimplemented!()
}

pub fn SimpleLruDoesPhysicalPageExist(_ctl: SlruCtl, _pageno: i64) -> bool {
    unimplemented!()
}

/// SlruScanCallback: the `void *data` opaque arg becomes a closure.
pub type SlruScanCallback<'a> = &'a mut dyn FnMut(SlruCtl, &str, i64) -> bool;

pub fn SlruScanDirectory(_ctl: SlruCtl, _callback: SlruScanCallback) -> bool {
    unimplemented!()
}

pub fn SlruDeleteSegment(_ctl: SlruCtl, _segno: i64) {
    unimplemented!()
}

/// out-param `char *path` -> returned String alongside the status code.
pub fn SlruSyncFileTag(_ctl: SlruCtl, _ftag: &FileTag) -> (i32, String) {
    unimplemented!()
}

/// SlruScanDirectory public callbacks.
pub fn SlruScanDirCbReportPresence(_ctl: SlruCtl, _filename: &str, _segpage: i64) -> bool {
    unimplemented!()
}

pub fn SlruScanDirCbDeleteAll(_ctl: SlruCtl, _filename: &str, _segpage: i64) -> bool {
    unimplemented!()
}

/// GUC check hook; out-param `int *newval` -> returned value on success.
pub fn check_slru_buffers(_name: &str, _newval: &mut i32) -> bool {
    unimplemented!()
}

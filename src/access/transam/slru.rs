//! src/backend/access/transam/slru.c
//!
//! Simple LRU buffering for wrap-around-able permanent metadata
//!
//! This module is used to maintain various pieces of transaction status
//! indexed by TransactionId (such as commit status, parent transaction ID,
//! commit timestamp), as well as storage for multixacts, serializable
//! isolation locks and NOTIFY traffic.  Extensions can define their own
//! SLRUs, too.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/transam/slru.c

use crate::prelude::*;

use crate::c::{int64, uint32, Size, TransactionId};
use crate::pg_config::BLCKSZ;
use crate::access::transam::xlogdefs::XLogRecPtr;

use core::ffi::{c_char, c_int, c_void, CStr};

// ----------------------------------------------------------------------------
// access/slru.h
//
// src/include/access/slru.h
// ----------------------------------------------------------------------------

/*
 * To avoid overflowing internal arithmetic and the size_t data type, the
 * number of buffers must not exceed this number.
 */
pub const SLRU_MAX_ALLOWED_BUFFERS: c_int = (1024 * 1024 * 1024) / BLCKSZ as c_int;

/*
 * Define SLRU segment size.  A page is the same BLCKSZ as is used everywhere
 * else in Postgres.  The segment size can be chosen somewhat arbitrarily;
 * we make it 32 pages by default, or 256Kb, i.e. 1M transactions for CLOG
 * or 64K transactions for SUBTRANS.
 */
pub const SLRU_PAGES_PER_SEGMENT: int64 = 32;

/*
 * Page status codes.  Note that these do not include the "dirty" bit.
 * page_dirty can be true only in the VALID or WRITE_IN_PROGRESS states;
 * in the latter case it implies that the page has been re-dirtied since
 * the write started.
 */
pub type SlruPageStatus = c_int;
pub const SLRU_PAGE_EMPTY: SlruPageStatus = 0; /* buffer is not in use */
pub const SLRU_PAGE_READ_IN_PROGRESS: SlruPageStatus = 1; /* page is being read in */
pub const SLRU_PAGE_VALID: SlruPageStatus = 2; /* page is valid and not being written */
pub const SLRU_PAGE_WRITE_IN_PROGRESS: SlruPageStatus = 3; /* page is being written out */

/*
 * Shared-memory state
 *
 * SLRU bank locks are used to protect access to the other fields, except
 * latest_page_number, which uses atomics; see comment in slru.c.
 */
#[repr(C)]
pub struct SlruSharedData {
    /* Number of buffers managed by this SLRU structure */
    pub num_slots: c_int,

    /*
     * Arrays holding info for each buffer slot.  Page number is undefined
     * when status is EMPTY, as is page_lru_count.
     */
    pub page_buffer: *mut *mut c_char,
    pub page_status: *mut SlruPageStatus,
    pub page_dirty: *mut bool,
    pub page_number: *mut int64,
    pub page_lru_count: *mut c_int,

    /* The buffer_locks protects the I/O on each buffer slots */
    pub buffer_locks: *mut LWLockPadded,

    /* Locks to protect the in memory buffer slot access in SLRU bank. */
    pub bank_locks: *mut LWLockPadded,

    /*
     * A bank-wise LRU counter is maintained because we do a victim buffer
     * search within a bank.
     */
    pub bank_cur_lru_count: *mut c_int,

    /*
     * Optional array of WAL flush LSNs associated with entries in the SLRU
     * pages.
     */
    pub group_lsn: *mut XLogRecPtr,
    pub lsn_groups_per_page: c_int,

    /*
     * latest_page_number is the page number of the current end of the log;
     * this is not critical data, since we use it only to avoid swapping out
     * the latest page.
     */
    pub latest_page_number: pg_atomic_uint64,

    /* SLRU's index for statistics purposes (might not be unique) */
    pub slru_stats_idx: c_int,
}

pub type SlruShared = *mut SlruSharedData;

pub type SlruPagePrecedesFunction = unsafe extern "C" fn(int64, int64) -> bool;

/*
 * SlruCtlData is an unshared structure that points to the active information
 * in shared memory.
 */
#[repr(C)]
pub struct SlruCtlData {
    pub shared: SlruShared,

    /* Number of banks in this SLRU. */
    pub nbanks: u16,

    /*
     * If true, use long segment file names.  Otherwise, use short file names.
     */
    pub long_segment_names: bool,

    /*
     * Which sync handler function to use when handing sync requests over to
     * the checkpointer.  SYNC_HANDLER_NONE to disable fsync (eg pg_notify).
     */
    pub sync_handler: SyncRequestHandler,

    /*
     * Decide whether a page is "older" for truncation and as a hint for
     * evicting pages in LRU order.
     */
    pub PagePrecedes: Option<SlruPagePrecedesFunction>,

    /*
     * Dir is set during SimpleLruInit and does not change thereafter.
     */
    pub Dir: [c_char; 64],
}

pub type SlruCtl = *mut SlruCtlData;

pub type SlruScanCallback =
    unsafe extern "C" fn(SlruCtl, *mut c_char, int64, *mut c_void) -> bool;

/*
 * Get the SLRU bank lock for given SlruCtl and the pageno.
 *
 * This lock needs to be acquired to access the slru buffer slots in the
 * respective bank.
 */
#[inline]
pub unsafe fn SimpleLruGetBankLock(ctl: SlruCtl, pageno: int64) -> *mut LWLock {
    let bankno: c_int;

    if std::env::var_os("PDB_BT").is_some() && (*ctl).nbanks == 0 {
        eprintln!("PDB_BT SimpleLruGetBankLock ctl={:p} nbanks=0 dir={:?}",
            ctl, std::ffi::CStr::from_ptr((*ctl).Dir.as_ptr()));
    }
    bankno = (pageno % (*ctl).nbanks as int64) as c_int;
    &mut (*(*(*ctl).shared).bank_locks.add(bankno as usize)).lock
}

// ----------------------------------------------------------------------------
// Locally stubbed dependencies (defined in other .c files not yet ported).
// ----------------------------------------------------------------------------

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn unlink(path: *const c_char) -> c_int;
    fn lseek(fd: c_int, offset: i64, whence: c_int) -> i64;
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// errno.h
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;

// fcntl.h
const O_RDONLY: c_int = 0x0000;
const O_RDWR: c_int = 0x0002;
const O_CREAT: c_int = 0x0200;
const PG_BINARY: c_int = 0;

// unistd.h / stdio.h
const SEEK_END: c_int = 2;

// LWLock and friends from storage/lwlock.h: use the canonical LWLock and give
// LWLockPadded its real cache-line size so the SLRU bank/buffer lock arrays are
// correctly sized, strided, and initialized (a zero-size stub aliased all locks
// at offset 0 -> garbage state -> LWLockAcquire blocked forever).
pub use crate::storage::lmgr::lwlock::LWLock;
#[repr(C)]
pub struct LWLockPadded {
    pub lock: LWLock,
    _pad: [u8; crate::storage::lmgr::lwlock::LWLOCK_PADDED_SIZE - core::mem::size_of::<LWLock>()],
}
pub const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): storage/lwlock.h
pub const LW_SHARED: c_int = 1; // TODO(pg-port): storage/lwlock.h

#[inline]
fn lwlock_mode(mode: c_int) -> crate::storage::lmgr::lwlock::LWLockMode {
    match mode {
        LW_EXCLUSIVE => crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE,
        _ => crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED,
    }
}
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, lwlock_mode(_mode))
}
unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockConditionalAcquire(_lock as _, lwlock_mode(_mode))
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as _)
}
unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    crate::storage::lmgr::lwlock::LWLockInitialize(_lock as _, _tranche_id)
}
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    crate::storage::lmgr::lwlock::LWLockHeldByMe(_lock as _)
}
unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockHeldByMeInMode(_lock as _, lwlock_mode(_mode))
}

// pg_atomic_uint64 and ops from port/atomics.h (not ported yet).
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}
unsafe fn pg_atomic_init_u64(_ptr: *mut pg_atomic_uint64, _val: u64) {
    crate::port::atomics::generic::pg_atomic_init_u64_impl(
        &*(_ptr as *const crate::port::atomics::pg_atomic_uint64),
        _val,
    )
}
unsafe fn pg_atomic_read_u64(_ptr: *mut pg_atomic_uint64) -> u64 {
    crate::port::atomics::generic::pg_atomic_read_u64_impl(&*(_ptr
        as *const crate::port::atomics::pg_atomic_uint64))
}
unsafe fn pg_atomic_write_u64(_ptr: *mut pg_atomic_uint64, _val: u64) {
    crate::port::atomics::generic::pg_atomic_write_u64_impl(
        &*(_ptr as *const crate::port::atomics::pg_atomic_uint64),
        _val,
    )
}

// SyncRequestHandler and sync requests from storage/sync.h (not ported yet).
pub type SyncRequestHandler = c_int;
pub const SYNC_HANDLER_NONE: SyncRequestHandler = -1; // TODO(pg-port): storage/sync.h
pub const SYNC_REQUEST: c_int = 0; // TODO(pg-port): storage/sync.h
pub const SYNC_FORGET_REQUEST: c_int = 1; // TODO(pg-port): storage/sync.h

#[repr(C)]
pub struct FileTag {
    pub handler: i16,
    pub segno: u64,
    // ... TODO(pg-port): storage/sync.h
}
unsafe fn RegisterSyncRequest(_ftag: *const FileTag, r#type: c_int, _retryOnError: bool) -> bool {
    let req = match r#type {
        SYNC_FORGET_REQUEST => crate::storage::sync::sync::SyncRequestType::SYNC_FORGET_REQUEST,
        _ => crate::storage::sync::sync::SyncRequestType::SYNC_REQUEST,
    };
    crate::storage::sync::sync::RegisterSyncRequest(_ftag as _, req, _retryOnError)
}

// fd / dir helpers --- TODO(pg-port): real defs live in storage/file/fd.c
#[repr(C)]
pub struct DIR {
    _private: [u8; 0],
}
#[repr(C)]
pub struct dirent {
    pub d_name: [c_char; 256],
    // ... TODO(pg-port): <dirent.h>
}
unsafe fn OpenTransientFile(_fileName: *const c_char, _fileFlags: c_int) -> c_int {
    crate::storage::file::fd::OpenTransientFile(_fileName, _fileFlags)
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    crate::storage::file::fd::CloseTransientFile(_fd)
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    crate::storage::file::fd::AllocateDir(_dirname) as _
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    crate::storage::file::fd::ReadDir(_dir as _, _dirname) as _
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    crate::storage::file::fd::FreeDir(_dir as _)
}
unsafe fn pg_fsync(_fd: c_int) -> c_int {
    crate::storage::file::fd::pg_fsync(_fd)
}
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) {
    crate::storage::file::fd::fsync_fname(_fname, _isdir)
}
unsafe fn pg_pread(_fd: c_int, _buf: *mut c_void, _amount: usize, _offset: i64) -> isize {
    crate::port::port_api::pg_pread(_fd, _buf, _amount, _offset)
}
unsafe fn pg_pwrite(_fd: c_int, _buf: *const c_void, _amount: usize, _offset: i64) -> isize {
    crate::port::port_api::pg_pwrite(_fd, _buf, _amount, _offset)
}

// shmem.h
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _found)
}

// port/strlcpy.c, port/strtoi64.c
unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    crate::port::strlcpy::strlcpy(_dst, _src, _siz)
}
unsafe fn strtoi64(_str: *const c_char, _endptr: *mut *mut c_char, _base: c_int) -> int64 {
    unimplemented!() // TODO(pg-port): common/string.c
}

// pgstat counters --- TODO(pg-port): pgstat.h / pgstat_slru.c
unsafe fn pgstat_get_slru_index(_name: *const c_char) -> c_int {
    crate::utils::activity::pgstat_slru::pgstat_get_slru_index(_name)
}
unsafe fn pgstat_count_slru_page_zeroed(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_page_zeroed(_idx)
}
unsafe fn pgstat_count_slru_page_hit(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_page_hit(_idx)
}
unsafe fn pgstat_count_slru_page_read(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_page_read(_idx)
}
unsafe fn pgstat_count_slru_page_written(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_page_written(_idx)
}
unsafe fn pgstat_count_slru_page_exists(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_page_exists(_idx)
}
unsafe fn pgstat_count_slru_flush(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_flush(_idx)
}
unsafe fn pgstat_count_slru_truncate(_idx: c_int) {
    crate::utils::activity::pgstat_slru::pgstat_count_slru_truncate(_idx)
}

// wait-event reporting --- TODO(pg-port): utils/activity/wait_event.c
const WAIT_EVENT_SLRU_READ: uint32 = 0;
const WAIT_EVENT_SLRU_WRITE: uint32 = 0;
const WAIT_EVENT_SLRU_SYNC: uint32 = 0;
const WAIT_EVENT_SLRU_FLUSH_SYNC: uint32 = 0;
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {
    // wait-event telemetry; no-op for bring-up
}
unsafe fn pgstat_report_wait_end() {
    // wait-event telemetry; no-op for bring-up
}

// xlog interactions --- TODO(pg-port): access/xlog.c
unsafe fn XLogFlush(_record: XLogRecPtr) {
    crate::access::transam::xlog::XLogFlush(_record)
}
unsafe fn XLogRecPtrIsInvalid(record: XLogRecPtr) -> bool {
    record == 0 // InvalidXLogRecPtr
}

// crit section macros --- TODO(pg-port): miscadmin.h
unsafe fn START_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h
}

// Checkpoint stats --- TODO(pg-port): access/xlog.c / pgstat.h
#[repr(C)]
pub struct CheckpointStatsData {
    pub ckpt_slru_written: u64,
    // ... TODO(pg-port): access/xlog.h
}
pub static mut CheckpointStats: CheckpointStatsData = CheckpointStatsData {
    ckpt_slru_written: 0,
};
#[repr(C)]
pub struct PgStat_CheckpointerStats {
    pub slru_written: i64,
    // ... TODO(pg-port): pgstat.h
}
pub static mut PendingCheckpointerStats: PgStat_CheckpointerStats =
    PgStat_CheckpointerStats { slru_written: 0 };

// Globals from other modules.
extern "C" {
    static mut NBuffers: c_int; // miscadmin.h / globals.c
}
pub static mut IsUnderPostmaster: bool = false; // TODO(pg-port): miscadmin.h
pub static mut InRecovery: bool = false; // TODO(pg-port): access/xlogutils.h

unsafe fn data_sync_elevel(_elevel: c_int) -> c_int {
    _elevel // TODO(pg-port): storage/file/fd.c
}

unsafe fn errcode_for_file_access() -> c_int {
    0 // TODO(pg-port): utils/error/elog.c
}

unsafe fn GUC_check_errdetail_fmt(_fmt: *const c_char) {
    // TODO(pg-port): utils/misc/guc.c
}

// ----------------------------------------------------------------------------
// slru.c
// ----------------------------------------------------------------------------

/*
 * Bank size for the slot array.  Pages are assigned a bank according to their
 * page number, with each bank being this size.  We want a power of 2 so that
 * we can determine the bank number for a page with just bit shifting; we also
 * want to keep the bank size small so that LRU victim search is fast.  16
 * buffers per bank seems a good number.
 */
const SLRU_BANK_BITSHIFT: c_int = 4;
const SLRU_BANK_SIZE: c_int = 1 << SLRU_BANK_BITSHIFT;

/*
 * Macro to get the bank number to which the slot belongs.
 */
#[inline]
fn SlotGetBankNumber(slotno: c_int) -> c_int {
    slotno >> SLRU_BANK_BITSHIFT
}

/*
 * During SimpleLruWriteAll(), we will usually not need to write more than one
 * or two physical files, but we may need to write several pages per file.  We
 * can consolidate the I/O requests by leaving files open until control returns
 * to SimpleLruWriteAll().  This data structure remembers which files are open.
 */
const MAX_WRITEALL_BUFFERS: usize = 16;

#[repr(C)]
pub struct SlruWriteAllData {
    pub num_files: c_int,                    /* # files actually open */
    pub fd: [c_int; MAX_WRITEALL_BUFFERS],   /* their FD's */
    pub segno: [int64; MAX_WRITEALL_BUFFERS], /* their log seg#s */
}

pub type SlruWriteAll = *mut SlruWriteAllData;

/*
 * Populate a file tag describing a segment file.  We only use the segment
 * number, since we can derive everything else we need by having separate
 * sync handler functions for clog, multixact etc.
 */
unsafe fn INIT_SLRUFILETAG(a: *mut FileTag, xx_handler: SyncRequestHandler, xx_segno: int64) {
    ptr::write_bytes(a as *mut u8, 0, core::mem::size_of::<FileTag>());
    (*a).handler = xx_handler as i16;
    (*a).segno = xx_segno as u64;
}

/* Saved info for SlruReportIOError */
type SlruErrorCause = c_int;
const SLRU_OPEN_FAILED: SlruErrorCause = 0;
const SLRU_SEEK_FAILED: SlruErrorCause = 1;
const SLRU_READ_FAILED: SlruErrorCause = 2;
const SLRU_WRITE_FAILED: SlruErrorCause = 3;
const SLRU_FSYNC_FAILED: SlruErrorCause = 4;
const SLRU_CLOSE_FAILED: SlruErrorCause = 5;

static mut slru_errcause: SlruErrorCause = SLRU_OPEN_FAILED;
static mut slru_errno: c_int = 0;

use core::ptr;

/*
 * Converts segment number to the filename of the segment.
 *
 * "path" should point to a buffer at least MAXPGPATH characters long.
 *
 * If ctl->long_segment_names is true, segno can be in the range [0, 2^60-1].
 * The resulting file name is made of 15 characters, e.g. dir/123456789ABCDEF.
 *
 * If ctl->long_segment_names is false, segno can be in the range [0, 2^24-1].
 * The resulting file name is made of 4 to 6 characters.
 */
#[inline]
unsafe fn SlruFileName(ctl: SlruCtl, path: *mut c_char, segno: int64) -> c_int {
    if (*ctl).long_segment_names {
        /*
         * We could use 16 characters here but the disadvantage would be that
         * the SLRU segments will be hard to distinguish from WAL segments.
         *
         * For this reason we use 15 characters. It is enough but also means
         * that in the future we can't decrease SLRU_PAGES_PER_SEGMENT easily.
         */
        Assert!(segno >= 0 && segno <= 0xFFFFFFFFFFFFFFF);
        snprintf(
            path,
            MAXPGPATH,
            c"%s/%015llX".as_ptr(),
            (*ctl).Dir.as_ptr(),
            segno as core::ffi::c_ulonglong,
        )
    } else {
        /*
         * Despite the fact that %04X format string is used up to 24 bit
         * integers are allowed. See SlruCorrectSegmentFilenameLength()
         */
        Assert!(segno >= 0 && segno <= 0xFFFFFF);
        snprintf(
            path,
            MAXPGPATH,
            c"%s/%04X".as_ptr(),
            (*ctl).Dir.as_ptr(),
            segno as core::ffi::c_uint,
        )
    }
}

/*
 * Initialization of shared memory
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruShmemSize(nslots: c_int, nlsns: c_int) -> Size {
    let nbanks: c_int = nslots / SLRU_BANK_SIZE;
    let mut sz: Size;

    Assert!(nslots <= SLRU_MAX_ALLOWED_BUFFERS);
    Assert!(nslots % SLRU_BANK_SIZE == 0);

    /* we assume nslots isn't so large as to risk overflow */
    sz = MAXALIGN(core::mem::size_of::<SlruSharedData>());
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<*mut c_char>()); /* page_buffer[] */
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<SlruPageStatus>()); /* page_status[] */
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<bool>()); /* page_dirty[] */
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<int64>()); /* page_number[] */
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<c_int>()); /* page_lru_count[] */
    sz += MAXALIGN(nslots as usize * core::mem::size_of::<LWLockPadded>()); /* buffer_locks[] */
    sz += MAXALIGN(nbanks as usize * core::mem::size_of::<LWLockPadded>()); /* bank_locks[] */
    sz += MAXALIGN(nbanks as usize * core::mem::size_of::<c_int>()); /* bank_cur_lru_count[] */

    if nlsns > 0 {
        sz += MAXALIGN(nslots as usize * nlsns as usize * core::mem::size_of::<XLogRecPtr>());
        /* group_lsn[] */
    }

    BUFFERALIGN(sz) + BLCKSZ as usize * nslots as usize
}

/*
 * Determine a number of SLRU buffers to use.
 *
 * We simply divide shared_buffers by the divisor given and cap
 * that at the maximum given; but always at least SLRU_BANK_SIZE.
 * Round down to the nearest multiple of SLRU_BANK_SIZE.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruAutotuneBuffers(divisor: c_int, max: c_int) -> c_int {
    Min(
        max - (max % SLRU_BANK_SIZE),
        Max(
            SLRU_BANK_SIZE,
            NBuffers / divisor - (NBuffers / divisor) % SLRU_BANK_SIZE,
        ),
    )
}

/*
 * Initialize, or attach to, a simple LRU cache in shared memory.
 *
 * ctl: address of local (unshared) control structure.
 * name: name of SLRU.  (This is user-visible, pick with care!)
 * nslots: number of page slots to use.
 * nlsns: number of LSN groups per page (set to zero if not relevant).
 * subdir: PGDATA-relative subdirectory that will contain the files.
 * buffer_tranche_id: tranche ID to use for the SLRU's per-buffer LWLocks.
 * bank_tranche_id: tranche ID to use for the bank LWLocks.
 * sync_handler: which set of functions to use to handle sync requests
 * long_segment_names: use short or long segment names
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruInit(
    ctl: SlruCtl,
    name: *const c_char,
    nslots: c_int,
    nlsns: c_int,
    subdir: *const c_char,
    buffer_tranche_id: c_int,
    bank_tranche_id: c_int,
    sync_handler: SyncRequestHandler,
    long_segment_names: bool,
) {
    let shared: SlruShared;
    let mut found: bool = false;
    let nbanks: c_int = nslots / SLRU_BANK_SIZE;

    Assert!(nslots <= SLRU_MAX_ALLOWED_BUFFERS);

    shared = ShmemInitStruct(name, SimpleLruShmemSize(nslots, nlsns), &mut found) as SlruShared;

    if !IsUnderPostmaster {
        /* Initialize locks and shared memory area */
        let mut ptr_buf: *mut c_char;
        let mut offset: Size;

        Assert!(!found);

        ptr::write_bytes(shared as *mut u8, 0, core::mem::size_of::<SlruSharedData>());

        (*shared).num_slots = nslots;
        (*shared).lsn_groups_per_page = nlsns;

        pg_atomic_init_u64(&mut (*shared).latest_page_number, 0);

        (*shared).slru_stats_idx = pgstat_get_slru_index(name);

        ptr_buf = shared as *mut c_char;
        offset = MAXALIGN(core::mem::size_of::<SlruSharedData>());
        (*shared).page_buffer = ptr_buf.add(offset) as *mut *mut c_char;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<*mut c_char>());
        (*shared).page_status = ptr_buf.add(offset) as *mut SlruPageStatus;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<SlruPageStatus>());
        (*shared).page_dirty = ptr_buf.add(offset) as *mut bool;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<bool>());
        (*shared).page_number = ptr_buf.add(offset) as *mut int64;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<int64>());
        (*shared).page_lru_count = ptr_buf.add(offset) as *mut c_int;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<c_int>());

        /* Initialize LWLocks */
        (*shared).buffer_locks = ptr_buf.add(offset) as *mut LWLockPadded;
        offset += MAXALIGN(nslots as usize * core::mem::size_of::<LWLockPadded>());
        (*shared).bank_locks = ptr_buf.add(offset) as *mut LWLockPadded;
        offset += MAXALIGN(nbanks as usize * core::mem::size_of::<LWLockPadded>());
        (*shared).bank_cur_lru_count = ptr_buf.add(offset) as *mut c_int;
        offset += MAXALIGN(nbanks as usize * core::mem::size_of::<c_int>());

        if nlsns > 0 {
            (*shared).group_lsn = ptr_buf.add(offset) as *mut XLogRecPtr;
            offset += MAXALIGN(nslots as usize * nlsns as usize * core::mem::size_of::<XLogRecPtr>());
        }

        ptr_buf = ptr_buf.add(BUFFERALIGN(offset));
        for slotno in 0..nslots {
            LWLockInitialize(
                &mut (*(*shared).buffer_locks.add(slotno as usize)).lock,
                buffer_tranche_id,
            );

            *(*shared).page_buffer.add(slotno as usize) = ptr_buf;
            *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_EMPTY;
            *(*shared).page_dirty.add(slotno as usize) = false;
            *(*shared).page_lru_count.add(slotno as usize) = 0;
            ptr_buf = ptr_buf.add(BLCKSZ as usize);
        }

        /* Initialize the slot banks. */
        for bankno in 0..nbanks {
            LWLockInitialize(
                &mut (*(*shared).bank_locks.add(bankno as usize)).lock,
                bank_tranche_id,
            );
            *(*shared).bank_cur_lru_count.add(bankno as usize) = 0;
        }

        /* Should fit to estimated shmem size */
        Assert!(
            ptr_buf.offset_from(shared as *mut c_char) as Size
                <= SimpleLruShmemSize(nslots, nlsns)
        );
    } else {
        Assert!(found);
        Assert!((*shared).num_slots == nslots);
    }

    /*
     * Initialize the unshared control struct, including directory path. We
     * assume caller set PagePrecedes.
     */
    (*ctl).shared = shared;
    (*ctl).sync_handler = sync_handler;
    (*ctl).long_segment_names = long_segment_names;
    (*ctl).nbanks = nbanks as u16;
    strlcpy((*ctl).Dir.as_mut_ptr(), subdir, core::mem::size_of_val(&(*ctl).Dir));
}

/*
 * Helper function for GUC check_hook to check whether slru buffers are in
 * multiples of SLRU_BANK_SIZE.
 */
#[no_mangle]
pub unsafe extern "C" fn check_slru_buffers(name: *const c_char, newval: *mut c_int) -> bool {
    /* Valid values are multiples of SLRU_BANK_SIZE */
    if *newval % SLRU_BANK_SIZE == 0 {
        return true;
    }

    GUC_check_errdetail_fmt(c"\"%s\" must be a multiple of %d.".as_ptr());
    /* C also: GUC_check_errdetail("\"%s\" must be a multiple of %d.", name, SLRU_BANK_SIZE); */
    let _ = name;
    false
}

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Bank lock must be held at entry, and will be held at exit.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruZeroPage(ctl: SlruCtl, pageno: int64) -> c_int {
    let shared: SlruShared = (*ctl).shared;
    let slotno: c_int;

    Assert!(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(ctl, pageno),
        LW_EXCLUSIVE
    ));

    /* Find a suitable buffer slot for the page */
    slotno = SlruSelectLRUPage(ctl, pageno);
    Assert!(
        *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY
            || (*(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID
                && !*(*shared).page_dirty.add(slotno as usize))
            || *(*shared).page_number.add(slotno as usize) == pageno
    );

    /* Mark the slot as containing this page */
    *(*shared).page_number.add(slotno as usize) = pageno;
    *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_VALID;
    *(*shared).page_dirty.add(slotno as usize) = true;
    SlruRecentlyUsed(shared, slotno);

    /* Set the buffer to zeroes */
    MemSet(
        *(*shared).page_buffer.add(slotno as usize) as *mut c_void,
        0,
        BLCKSZ as Size,
    );

    /* Set the LSNs for this new page to zero */
    SimpleLruZeroLSNs(ctl, slotno);

    /*
     * Assume this page is now the latest active page.
     *
     * Note that because both this routine and SlruSelectLRUPage run with a
     * SLRU bank lock held, it is not possible for this to be zeroing a page
     * that SlruSelectLRUPage is going to evict simultaneously.  Therefore,
     * there's no memory barrier here.
     */
    pg_atomic_write_u64(&mut (*shared).latest_page_number, pageno as u64);

    /* update the stats counter of zeroed pages */
    pgstat_count_slru_page_zeroed((*shared).slru_stats_idx);

    slotno
}

/*
 * Zero all the LSNs we store for this slru page.
 *
 * This should be called each time we create a new page, and each time we read
 * in a page from disk into an existing buffer.  (Such an old page cannot
 * have any interesting LSNs, since we'd have flushed them before writing
 * the page in the first place.)
 *
 * This assumes that InvalidXLogRecPtr is bitwise-all-0.
 */
unsafe fn SimpleLruZeroLSNs(ctl: SlruCtl, slotno: c_int) {
    let shared: SlruShared = (*ctl).shared;

    if (*shared).lsn_groups_per_page > 0 {
        MemSet(
            (*shared)
                .group_lsn
                .add((slotno * (*shared).lsn_groups_per_page) as usize) as *mut c_void,
            0,
            (*shared).lsn_groups_per_page as Size * core::mem::size_of::<XLogRecPtr>() as Size,
        );
    }
}

/*
 * Wait for any active I/O on a page slot to finish.  (This does not
 * guarantee that new I/O hasn't been started before we return, though.
 * In fact the slot might not even contain the same page anymore.)
 *
 * Bank lock must be held at entry, and will be held at exit.
 */
unsafe fn SimpleLruWaitIO(ctl: SlruCtl, slotno: c_int) {
    let shared: SlruShared = (*ctl).shared;
    let bankno: c_int = SlotGetBankNumber(slotno);

    Assert!(*(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY);

    /* See notes at top of file */
    LWLockRelease(&mut (*(*shared).bank_locks.add(bankno as usize)).lock);
    LWLockAcquire(
        &mut (*(*shared).buffer_locks.add(slotno as usize)).lock,
        LW_SHARED,
    );
    LWLockRelease(&mut (*(*shared).buffer_locks.add(slotno as usize)).lock);
    LWLockAcquire(
        &mut (*(*shared).bank_locks.add(bankno as usize)).lock,
        LW_EXCLUSIVE,
    );

    /*
     * If the slot is still in an io-in-progress state, then either someone
     * already started a new I/O on the slot, or a previous I/O failed and
     * neglected to reset the page state.  That shouldn't happen, really, but
     * it seems worth a few extra cycles to check and recover from it. We can
     * cheaply test for failure by seeing if the buffer lock is still held (we
     * assume that transaction abort would release the lock).
     */
    if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_READ_IN_PROGRESS
        || *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_WRITE_IN_PROGRESS
    {
        if LWLockConditionalAcquire(
            &mut (*(*shared).buffer_locks.add(slotno as usize)).lock,
            LW_SHARED,
        ) {
            /* indeed, the I/O must have failed */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_READ_IN_PROGRESS {
                *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_EMPTY;
            } else {
                /* write_in_progress */
                *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_VALID;
                *(*shared).page_dirty.add(slotno as usize) = true;
            }
            LWLockRelease(&mut (*(*shared).buffer_locks.add(slotno as usize)).lock);
        }
    }
}

// ----------------------------------------------------------------------------
// Additional stubbed dependencies needed by the functions below.
// ----------------------------------------------------------------------------

// access/transam.h --- TODO(pg-port): real values live in access/transam.h
const MAXPGPATH: usize = 1024; // TODO(pg-port): pg_config_manual.h
pub const InvalidTransactionId: TransactionId = 0; // TODO(pg-port): access/transam.h
pub const FirstNormalTransactionId: TransactionId = 3; // TODO(pg-port): access/transam.h

// transam.c --- TODO(pg-port): access/transam/transam.c
unsafe fn TransactionIdPrecedes(_id1: TransactionId, _id2: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdPrecedes(_id1, _id2)
}
unsafe fn TransactionIdFollowsOrEquals(_id1: TransactionId, _id2: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdFollowsOrEquals(_id1, _id2)
}

/*
 * Mark a buffer slot "most recently used".
 */
#[inline]
unsafe fn SlruRecentlyUsed(shared: SlruShared, slotno: c_int) {
    let bankno: c_int = SlotGetBankNumber(slotno);
    let mut new_lru_count: c_int = *(*shared).bank_cur_lru_count.add(bankno as usize);

    Assert!(*(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY);

    /*
     * The reason for the if-test is that there are often many consecutive
     * accesses to the same page (particularly the latest page).  By
     * suppressing useless increments of bank_cur_lru_count, we reduce the
     * probability that old pages' counts will "wrap around" and make them
     * appear recently used.
     *
     * We allow this code to be executed concurrently by multiple processes
     * within SimpleLruReadPage_ReadOnly().  As long as int reads and writes
     * are atomic, this should not cause any completely-bogus values to enter
     * the computation.  However, it is possible for either bank_cur_lru_count
     * or individual page_lru_count entries to be "reset" to lower values than
     * they should have, in case a process is delayed while it executes this
     * function.  With care in SlruSelectLRUPage(), this does little harm, and
     * in any case the absolute worst possible consequence is a nonoptimal
     * choice of page to evict.  The gain from allowing concurrent reads of
     * SLRU pages seems worth it.
     */
    if new_lru_count != *(*shared).page_lru_count.add(slotno as usize) {
        new_lru_count += 1;
        *(*shared).bank_cur_lru_count.add(bankno as usize) = new_lru_count;
        *(*shared).page_lru_count.add(slotno as usize) = new_lru_count;
    }
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the relevant page is enabled, if that's not always
 * the case.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Bank control lock must be held at entry, and will be held at exit.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruReadPage(
    ctl: SlruCtl,
    pageno: int64,
    write_ok: bool,
    xid: TransactionId,
) -> c_int {
    let shared: SlruShared = (*ctl).shared;
    let banklock: *mut LWLock = SimpleLruGetBankLock(ctl, pageno);

    Assert!(LWLockHeldByMeInMode(banklock, LW_EXCLUSIVE));

    /* Outer loop handles restart if we must wait for someone else's I/O */
    loop {
        let slotno: c_int;
        let ok: bool;

        /* See if page already is in memory; if not, pick victim slot */
        slotno = SlruSelectLRUPage(ctl, pageno);

        /* Did we find the page in memory? */
        if *(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY
            && *(*shared).page_number.add(slotno as usize) == pageno
        {
            /*
             * If page is still being read in, we must wait for I/O.  Likewise
             * if the page is being written and the caller said that's not OK.
             */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_READ_IN_PROGRESS
                || (*(*shared).page_status.add(slotno as usize) == SLRU_PAGE_WRITE_IN_PROGRESS
                    && !write_ok)
            {
                SimpleLruWaitIO(ctl, slotno);
                /* Now we must recheck state from the top */
                continue;
            }
            /* Otherwise, it's ready to use */
            SlruRecentlyUsed(shared, slotno);

            /* update the stats counter of pages found in the SLRU */
            pgstat_count_slru_page_hit((*shared).slru_stats_idx);

            return slotno;
        }

        /* We found no match; assert we selected a freeable slot */
        Assert!(
            *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY
                || (*(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID
                    && !*(*shared).page_dirty.add(slotno as usize))
        );

        /* Mark the slot read-busy */
        *(*shared).page_number.add(slotno as usize) = pageno;
        *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_READ_IN_PROGRESS;
        *(*shared).page_dirty.add(slotno as usize) = false;

        /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
        LWLockAcquire(
            &mut (*(*shared).buffer_locks.add(slotno as usize)).lock,
            LW_EXCLUSIVE,
        );

        /* Release bank lock while doing I/O */
        LWLockRelease(banklock);

        /* Do the read */
        ok = SlruPhysicalReadPage(ctl, pageno, slotno);

        /* Set the LSNs for this newly read-in page to zero */
        SimpleLruZeroLSNs(ctl, slotno);

        /* Re-acquire bank control lock and update page state */
        LWLockAcquire(banklock, LW_EXCLUSIVE);

        Assert!(
            *(*shared).page_number.add(slotno as usize) == pageno
                && *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_READ_IN_PROGRESS
                && !*(*shared).page_dirty.add(slotno as usize)
        );

        *(*shared).page_status.add(slotno as usize) =
            if ok { SLRU_PAGE_VALID } else { SLRU_PAGE_EMPTY };

        LWLockRelease(&mut (*(*shared).buffer_locks.add(slotno as usize)).lock);

        /* Now it's okay to ereport if we failed */
        if !ok {
            SlruReportIOError(ctl, pageno, xid);
        }

        SlruRecentlyUsed(shared, slotno);

        /* update the stats counter of pages not found in SLRU */
        pgstat_count_slru_page_read((*shared).slru_stats_idx);

        return slotno;
    }
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Bank control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruReadPage_ReadOnly(
    ctl: SlruCtl,
    pageno: int64,
    xid: TransactionId,
) -> c_int {
    let shared: SlruShared = (*ctl).shared;
    let banklock: *mut LWLock = SimpleLruGetBankLock(ctl, pageno);
    let bankno: c_int = (pageno % (*ctl).nbanks as int64) as c_int;
    let bankstart: c_int = bankno * SLRU_BANK_SIZE;
    let bankend: c_int = bankstart + SLRU_BANK_SIZE;

    /* Try to find the page while holding only shared lock */
    LWLockAcquire(banklock, LW_SHARED);

    /* See if page is already in a buffer */
    let mut slotno: c_int = bankstart;
    while slotno < bankend {
        if *(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY
            && *(*shared).page_number.add(slotno as usize) == pageno
            && *(*shared).page_status.add(slotno as usize) != SLRU_PAGE_READ_IN_PROGRESS
        {
            /* See comments for SlruRecentlyUsed() */
            SlruRecentlyUsed(shared, slotno);

            /* update the stats counter of pages found in the SLRU */
            pgstat_count_slru_page_hit((*shared).slru_stats_idx);

            return slotno;
        }
        slotno += 1;
    }

    /* No luck, so switch to normal exclusive lock and do regular read */
    LWLockRelease(banklock);
    LWLockAcquire(banklock, LW_EXCLUSIVE);

    SimpleLruReadPage(ctl, pageno, true, xid)
}

/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).  However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Bank lock must be held at entry, and will be held at exit.
 */
unsafe fn SlruInternalWritePage(ctl: SlruCtl, slotno: c_int, fdata: SlruWriteAll) {
    let shared: SlruShared = (*ctl).shared;
    let pageno: int64 = *(*shared).page_number.add(slotno as usize);
    let bankno: c_int = SlotGetBankNumber(slotno);
    let ok: bool;

    Assert!(*(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY);
    Assert!(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(ctl, pageno),
        LW_EXCLUSIVE
    ));

    /* If a write is in progress, wait for it to finish */
    while *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_WRITE_IN_PROGRESS
        && *(*shared).page_number.add(slotno as usize) == pageno
    {
        SimpleLruWaitIO(ctl, slotno);
    }

    /*
     * Do nothing if page is not dirty, or if buffer no longer contains the
     * same page we were called for.
     */
    if !*(*shared).page_dirty.add(slotno as usize)
        || *(*shared).page_status.add(slotno as usize) != SLRU_PAGE_VALID
        || *(*shared).page_number.add(slotno as usize) != pageno
    {
        return;
    }

    /*
     * Mark the slot write-busy, and clear the dirtybit.  After this point, a
     * transaction status update on this page will mark it dirty again.
     */
    *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_WRITE_IN_PROGRESS;
    *(*shared).page_dirty.add(slotno as usize) = false;

    /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
    LWLockAcquire(
        &mut (*(*shared).buffer_locks.add(slotno as usize)).lock,
        LW_EXCLUSIVE,
    );

    /* Release bank lock while doing I/O */
    LWLockRelease(&mut (*(*shared).bank_locks.add(bankno as usize)).lock);

    /* Do the write */
    ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata);

    /* If we failed, and we're in a flush, better close the files */
    if !ok && !fdata.is_null() {
        let mut i: c_int = 0;
        while i < (*fdata).num_files {
            CloseTransientFile((*fdata).fd[i as usize]);
            i += 1;
        }
    }

    /* Re-acquire bank lock and update page state */
    LWLockAcquire(
        &mut (*(*shared).bank_locks.add(bankno as usize)).lock,
        LW_EXCLUSIVE,
    );

    Assert!(
        *(*shared).page_number.add(slotno as usize) == pageno
            && *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_WRITE_IN_PROGRESS
    );

    /* If we failed to write, mark the page dirty again */
    if !ok {
        *(*shared).page_dirty.add(slotno as usize) = true;
    }

    *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_VALID;

    LWLockRelease(&mut (*(*shared).buffer_locks.add(slotno as usize)).lock);

    /* Now it's okay to ereport if we failed */
    if !ok {
        SlruReportIOError(ctl, pageno, InvalidTransactionId);
    }

    /* If part of a checkpoint, count this as a SLRU buffer written. */
    if !fdata.is_null() {
        CheckpointStats.ckpt_slru_written += 1;
        PendingCheckpointerStats.slru_written += 1;
    }
}

/*
 * Wrapper of SlruInternalWritePage, for external callers.
 * fdata is always passed a NULL here.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruWritePage(ctl: SlruCtl, slotno: c_int) {
    Assert!(*(*(*ctl).shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY);

    SlruInternalWritePage(ctl, slotno, ptr::null_mut());
}

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruDoesPhysicalPageExist(ctl: SlruCtl, pageno: int64) -> bool {
    let segno: int64 = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno: c_int = (pageno % SLRU_PAGES_PER_SEGMENT) as c_int;
    let offset: c_int = rpageno * BLCKSZ as c_int;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;
    let result: bool;
    let endpos: i64;

    /* update the stats counter of checked pages */
    pgstat_count_slru_page_exists((*(*ctl).shared).slru_stats_idx);

    SlruFileName(ctl, path.as_mut_ptr(), segno);

    fd = OpenTransientFile(path.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        /* expected: file doesn't exist */
        if get_errno() == ENOENT {
            return false;
        }

        /* report error normally */
        slru_errcause = SLRU_OPEN_FAILED;
        slru_errno = get_errno();
        SlruReportIOError(ctl, pageno, 0);
    }

    endpos = lseek(fd, 0, SEEK_END);
    if endpos < 0 {
        slru_errcause = SLRU_SEEK_FAILED;
        slru_errno = get_errno();
        SlruReportIOError(ctl, pageno, 0);
    }

    result = endpos >= (offset + BLCKSZ as c_int) as i64;

    if CloseTransientFile(fd) != 0 {
        slru_errcause = SLRU_CLOSE_FAILED;
        slru_errno = get_errno();
        return false;
    }

    result
}

/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
unsafe fn SlruPhysicalReadPage(ctl: SlruCtl, pageno: int64, slotno: c_int) -> bool {
    let shared: SlruShared = (*ctl).shared;
    let segno: int64 = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno: c_int = (pageno % SLRU_PAGES_PER_SEGMENT) as c_int;
    let offset: i64 = rpageno as i64 * BLCKSZ as i64;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;

    SlruFileName(ctl, path.as_mut_ptr(), segno);

    /*
     * In a crash-and-restart situation, it's possible for us to receive
     * commands to set the commit status of transactions whose bits are in
     * already-truncated segments of the commit log (see notes in
     * SlruPhysicalWritePage).  Hence, if we are InRecovery, allow the case
     * where the file doesn't exist, and return zeroes instead.
     */
    fd = OpenTransientFile(path.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        if get_errno() != ENOENT || !InRecovery {
            slru_errcause = SLRU_OPEN_FAILED;
            slru_errno = get_errno();
            return false;
        }

        ereport!(
            LOG,
            errmsg!(
                "file \"{}\" doesn't exist, reading as zeroes",
                CStr::from_ptr(path.as_ptr()).to_string_lossy()
            )
        );
        MemSet(
            *(*shared).page_buffer.add(slotno as usize) as *mut c_void,
            0,
            BLCKSZ as Size,
        );
        return true;
    }

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_SLRU_READ);
    if pg_pread(
        fd,
        *(*shared).page_buffer.add(slotno as usize) as *mut c_void,
        BLCKSZ as usize,
        offset,
    ) != BLCKSZ as isize
    {
        pgstat_report_wait_end();
        slru_errcause = SLRU_READ_FAILED;
        slru_errno = get_errno();
        CloseTransientFile(fd);
        return false;
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        slru_errcause = SLRU_CLOSE_FAILED;
        slru_errno = get_errno();
        return false;
    }

    true
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * SimpleLruWriteAll, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * SimpleLruWriteAll.
 */
unsafe fn SlruPhysicalWritePage(
    ctl: SlruCtl,
    pageno: int64,
    slotno: c_int,
    mut fdata: SlruWriteAll,
) -> bool {
    let shared: SlruShared = (*ctl).shared;
    let segno: int64 = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno: c_int = (pageno % SLRU_PAGES_PER_SEGMENT) as c_int;
    let offset: i64 = rpageno as i64 * BLCKSZ as i64;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut fd: c_int = -1;

    /* update the stats counter of written pages */
    pgstat_count_slru_page_written((*shared).slru_stats_idx);

    /*
     * Honor the write-WAL-before-data rule, if appropriate, so that we do not
     * write out data before associated WAL records.  This is the same action
     * performed during FlushBuffer() in the main buffer manager.
     */
    if !(*shared).group_lsn.is_null() {
        /*
         * We must determine the largest async-commit LSN for the page. This
         * is a bit tedious, but since this entire function is a slow path
         * anyway, it seems better to do this here than to maintain a per-page
         * LSN variable (which'd need an extra comparison in the
         * transaction-commit path).
         */
        let mut max_lsn: XLogRecPtr;
        let mut lsnindex: c_int;

        lsnindex = slotno * (*shared).lsn_groups_per_page;
        max_lsn = *(*shared).group_lsn.add(lsnindex as usize);
        lsnindex += 1;
        let mut lsnoff: c_int = 1;
        while lsnoff < (*shared).lsn_groups_per_page {
            let this_lsn: XLogRecPtr = *(*shared).group_lsn.add(lsnindex as usize);
            lsnindex += 1;

            if max_lsn < this_lsn {
                max_lsn = this_lsn;
            }
            lsnoff += 1;
        }

        if !XLogRecPtrIsInvalid(max_lsn) {
            /*
             * As noted above, elog(ERROR) is not acceptable here, so if
             * XLogFlush were to fail, we must PANIC.  This isn't much of a
             * restriction because XLogFlush is just about all critical
             * section anyway, but let's make sure.
             */
            START_CRIT_SECTION();
            XLogFlush(max_lsn);
            END_CRIT_SECTION();
        }
    }

    /*
     * During a SimpleLruWriteAll, we may already have the desired file open.
     */
    if !fdata.is_null() {
        let mut i: c_int = 0;
        while i < (*fdata).num_files {
            if (*fdata).segno[i as usize] == segno {
                fd = (*fdata).fd[i as usize];
                break;
            }
            i += 1;
        }
    }

    if fd < 0 {
        /*
         * If the file doesn't already exist, we should create it.  It is
         * possible for this to need to happen when writing a page that's not
         * first in its segment; we assume the OS can cope with that. (Note:
         * it might seem that it'd be okay to create files only when
         * SimpleLruZeroPage is called for the first page of a segment.
         * However, if after a crash and restart the REDO logic elects to
         * replay the log from a checkpoint before the latest one, then it's
         * possible that we will get commands to set transaction status of
         * transactions that have already been truncated from the commit log.
         * Easiest way to deal with that is to accept references to
         * nonexistent files here and in SlruPhysicalReadPage.)
         *
         * Note: it is possible for more than one backend to be executing this
         * code simultaneously for different pages of the same file. Hence,
         * don't use O_EXCL or O_TRUNC or anything like that.
         */
        SlruFileName(ctl, path.as_mut_ptr(), segno);
        fd = OpenTransientFile(path.as_ptr(), O_RDWR | O_CREAT | PG_BINARY);
        if fd < 0 {
            slru_errcause = SLRU_OPEN_FAILED;
            slru_errno = get_errno();
            return false;
        }

        if !fdata.is_null() {
            if (*fdata).num_files < MAX_WRITEALL_BUFFERS as c_int {
                (*fdata).fd[(*fdata).num_files as usize] = fd;
                (*fdata).segno[(*fdata).num_files as usize] = segno;
                (*fdata).num_files += 1;
            } else {
                /*
                 * In the unlikely event that we exceed MAX_WRITEALL_BUFFERS,
                 * fall back to treating it as a standalone write.
                 */
                fdata = ptr::null_mut();
            }
        }
    }

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
    if pg_pwrite(
        fd,
        *(*shared).page_buffer.add(slotno as usize) as *const c_void,
        BLCKSZ as usize,
        offset,
    ) != BLCKSZ as isize
    {
        pgstat_report_wait_end();
        /* if write didn't set errno, assume problem is no disk space */
        if get_errno() == 0 {
            set_errno(ENOSPC);
        }
        slru_errcause = SLRU_WRITE_FAILED;
        slru_errno = get_errno();
        if fdata.is_null() {
            CloseTransientFile(fd);
        }
        return false;
    }
    pgstat_report_wait_end();

    /* Queue up a sync request for the checkpointer. */
    if (*ctl).sync_handler != SYNC_HANDLER_NONE {
        let mut tag: FileTag = core::mem::zeroed();

        INIT_SLRUFILETAG(&mut tag, (*ctl).sync_handler, segno);
        if !RegisterSyncRequest(&tag, SYNC_REQUEST, false) {
            /* No space to enqueue sync request.  Do it synchronously. */
            pgstat_report_wait_start(WAIT_EVENT_SLRU_SYNC);
            if pg_fsync(fd) != 0 {
                pgstat_report_wait_end();
                slru_errcause = SLRU_FSYNC_FAILED;
                slru_errno = get_errno();
                CloseTransientFile(fd);
                return false;
            }
            pgstat_report_wait_end();
        }
    }

    /* Close file, unless part of flush request. */
    if fdata.is_null() {
        if CloseTransientFile(fd) != 0 {
            slru_errcause = SLRU_CLOSE_FAILED;
            slru_errno = get_errno();
            return false;
        }
    }

    true
}

/*
 * Issue the error message after failure of SlruPhysicalReadPage or
 * SlruPhysicalWritePage.  Call this after cleaning up shared-memory state.
 */
unsafe fn SlruReportIOError(ctl: SlruCtl, pageno: int64, xid: TransactionId) {
    let segno: int64 = pageno / SLRU_PAGES_PER_SEGMENT;
    let rpageno: c_int = (pageno % SLRU_PAGES_PER_SEGMENT) as c_int;
    let offset: c_int = rpageno * BLCKSZ as c_int;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    SlruFileName(ctl, path.as_mut_ptr(), segno);
    set_errno(slru_errno);
    let pathstr = CStr::from_ptr(path.as_ptr()).to_string_lossy();
    match slru_errcause {
        SLRU_OPEN_FAILED => {
            ereport!(
                ERROR,
                errmsg!("could not access status of transaction {}", xid)
            );
            /* C also: errcode_for_file_access(), errdetail("Could not open file \"%s\": %m.", path) */
            let _ = (offset, &pathstr);
        }
        SLRU_SEEK_FAILED => {
            ereport!(
                ERROR,
                errmsg!("could not access status of transaction {}", xid)
            );
            /* C also: errcode_for_file_access(), errdetail("Could not seek in file \"%s\" to offset %d: %m.", path, offset) */
            let _ = (offset, &pathstr);
        }
        SLRU_READ_FAILED => {
            if get_errno() != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not access status of transaction {}", xid)
                );
                /* C also: errcode_for_file_access(), errdetail("Could not read from file \"%s\" at offset %d: %m.", path, offset) */
            } else {
                ereport!(
                    ERROR,
                    errmsg!("could not access status of transaction {}", xid)
                );
                /* C also: errdetail("Could not read from file \"%s\" at offset %d: read too few bytes.", path, offset) */
            }
            let _ = (offset, &pathstr);
        }
        SLRU_WRITE_FAILED => {
            if get_errno() != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not access status of transaction {}", xid)
                );
                /* C also: errcode_for_file_access(), errdetail("Could not write to file \"%s\" at offset %d: %m.", path, offset) */
            } else {
                ereport!(
                    ERROR,
                    errmsg!("could not access status of transaction {}", xid)
                );
                /* C also: errdetail("Could not write to file \"%s\" at offset %d: wrote too few bytes.", path, offset) */
            }
            let _ = (offset, &pathstr);
        }
        SLRU_FSYNC_FAILED => {
            ereport!(
                data_sync_elevel(ERROR),
                errmsg!("could not access status of transaction {}", xid)
            );
            /* C also: errcode_for_file_access(), errdetail("Could not fsync file \"%s\": %m.", path) */
            let _ = (offset, &pathstr);
        }
        SLRU_CLOSE_FAILED => {
            ereport!(
                ERROR,
                errmsg!("could not access status of transaction {}", xid)
            );
            /* C also: errcode_for_file_access(), errdetail("Could not close file \"%s\": %m.", path) */
            let _ = (offset, &pathstr);
        }
        _ => {
            /* can't get here, we trust */
            elog!(
                ERROR,
                "unrecognized SimpleLru error cause: {}",
                slru_errcause as c_int
            );
        }
    }
    let _ = errcode_for_file_access();
}

/*
 * Select the slot to re-use when we need a free slot for the given page.
 *
 * The target page number is passed not only because we need to know the
 * correct bank to use, but also because we need to consider the possibility
 * that some other process reads in the target page while we are doing I/O to
 * free a slot.  Hence, check or recheck to see if any slot already holds the
 * target page, and return that slot if so.  Thus, the returned slot is
 * *either* a slot already holding the pageno (could be any state except
 * EMPTY), *or* a freeable slot (state EMPTY or CLEAN).
 *
 * The correct bank lock must be held at entry, and will be held at exit.
 */
unsafe fn SlruSelectLRUPage(ctl: SlruCtl, pageno: int64) -> c_int {
    let shared: SlruShared = (*ctl).shared;

    /* Outer loop handles restart after I/O */
    loop {
        let cur_count: c_int;
        let mut bestvalidslot: c_int = 0; /* keep compiler quiet */
        let mut best_valid_delta: c_int = -1;
        let mut best_valid_page_number: int64 = 0; /* keep compiler quiet */
        let mut bestinvalidslot: c_int = 0; /* keep compiler quiet */
        let mut best_invalid_delta: c_int = -1;
        let mut best_invalid_page_number: int64 = 0; /* keep compiler quiet */
        let bankno: c_int = (pageno % (*ctl).nbanks as int64) as c_int;
        let bankstart: c_int = bankno * SLRU_BANK_SIZE;
        let bankend: c_int = bankstart + SLRU_BANK_SIZE;

        Assert!(LWLockHeldByMe(SimpleLruGetBankLock(ctl, pageno)));

        /* See if page already has a buffer assigned */
        let mut slotno: c_int = bankstart;
        while slotno < bankend {
            if *(*shared).page_status.add(slotno as usize) != SLRU_PAGE_EMPTY
                && *(*shared).page_number.add(slotno as usize) == pageno
            {
                return slotno;
            }
            slotno += 1;
        }

        /*
         * If we find any EMPTY slot, just select that one. Else choose a
         * victim page to replace.  We normally take the least recently used
         * valid page, but we will never take the slot containing
         * latest_page_number, even if it appears least recently used.  We
         * will select a slot that is already I/O busy only if there is no
         * other choice: a read-busy slot will not be least recently used once
         * the read finishes, and waiting for an I/O on a write-busy slot is
         * inferior to just picking some other slot.  Testing shows the slot
         * we pick instead will often be clean, allowing us to begin a read at
         * once.
         *
         * Normally the page_lru_count values will all be different and so
         * there will be a well-defined LRU page.  But since we allow
         * concurrent execution of SlruRecentlyUsed() within
         * SimpleLruReadPage_ReadOnly(), it is possible that multiple pages
         * acquire the same lru_count values.  In that case we break ties by
         * choosing the furthest-back page.
         *
         * Notice that this next line forcibly advances cur_lru_count to a
         * value that is certainly beyond any value that will be in the
         * page_lru_count array after the loop finishes.  This ensures that
         * the next execution of SlruRecentlyUsed will mark the page newly
         * used, even if it's for a page that has the current counter value.
         * That gets us back on the path to having good data when there are
         * multiple pages with the same lru_count.
         */
        cur_count = *(*shared).bank_cur_lru_count.add(bankno as usize);
        *(*shared).bank_cur_lru_count.add(bankno as usize) = cur_count + 1;
        let mut slotno: c_int = bankstart;
        while slotno < bankend {
            let mut this_delta: c_int;
            let this_page_number: int64;

            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY {
                return slotno;
            }

            this_delta = cur_count - *(*shared).page_lru_count.add(slotno as usize);
            if this_delta < 0 {
                /*
                 * Clean up in case shared updates have caused cur_count
                 * increments to get "lost".  We back off the page counts,
                 * rather than trying to increase cur_count, to avoid any
                 * question of infinite loops or failure in the presence of
                 * wrapped-around counts.
                 */
                *(*shared).page_lru_count.add(slotno as usize) = cur_count;
                this_delta = 0;
            }

            /*
             * If this page is the one most recently zeroed, don't consider it
             * an eviction candidate. See comments in SimpleLruZeroPage for an
             * explanation about the lack of a memory barrier here.
             */
            this_page_number = *(*shared).page_number.add(slotno as usize);
            if this_page_number as u64 == pg_atomic_read_u64(&mut (*shared).latest_page_number) {
                slotno += 1;
                continue;
            }

            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID {
                if this_delta > best_valid_delta
                    || (this_delta == best_valid_delta
                        && (*ctl).PagePrecedes.unwrap()(this_page_number, best_valid_page_number))
                {
                    bestvalidslot = slotno;
                    best_valid_delta = this_delta;
                    best_valid_page_number = this_page_number;
                }
            } else {
                if this_delta > best_invalid_delta
                    || (this_delta == best_invalid_delta
                        && (*ctl).PagePrecedes.unwrap()(this_page_number, best_invalid_page_number))
                {
                    bestinvalidslot = slotno;
                    best_invalid_delta = this_delta;
                    best_invalid_page_number = this_page_number;
                }
            }
            slotno += 1;
        }

        /*
         * If all pages (except possibly the latest one) are I/O busy, we'll
         * have to wait for an I/O to complete and then retry.  In that
         * unhappy case, we choose to wait for the I/O on the least recently
         * used slot, on the assumption that it was likely initiated first of
         * all the I/Os in progress and may therefore finish first.
         */
        if best_valid_delta < 0 {
            SimpleLruWaitIO(ctl, bestinvalidslot);
            continue;
        }

        /*
         * If the selected page is clean, we're set.
         */
        if !*(*shared).page_dirty.add(bestvalidslot as usize) {
            return bestvalidslot;
        }

        /*
         * Write the page.
         */
        SlruInternalWritePage(ctl, bestvalidslot, ptr::null_mut());

        /*
         * Now loop back and try again.  This is the easiest way of dealing
         * with corner cases such as the victim page being re-dirtied while we
         * wrote it.
         */
    }
}

/*
 * Write dirty pages to disk during checkpoint or database shutdown.  Flushing
 * is deferred until the next call to ProcessSyncRequests(), though we do fsync
 * the containing directory here to make sure that newly created directory
 * entries are on disk.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruWriteAll(ctl: SlruCtl, allow_redirtied: bool) {
    let shared: SlruShared = (*ctl).shared;
    let mut fdata: SlruWriteAllData = core::mem::zeroed();
    let mut pageno: int64 = 0;
    let mut prevbank: c_int = SlotGetBankNumber(0);
    let mut ok: bool;

    /* update the stats counter of flushes */
    pgstat_count_slru_flush((*shared).slru_stats_idx);

    /*
     * Find and write dirty pages
     */
    fdata.num_files = 0;

    LWLockAcquire(
        &mut (*(*shared).bank_locks.add(prevbank as usize)).lock,
        LW_EXCLUSIVE,
    );

    let mut slotno: c_int = 0;
    while slotno < (*shared).num_slots {
        let curbank: c_int = SlotGetBankNumber(slotno);

        /*
         * If the current bank lock is not same as the previous bank lock then
         * release the previous lock and acquire the new lock.
         */
        if curbank != prevbank {
            LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
            LWLockAcquire(
                &mut (*(*shared).bank_locks.add(curbank as usize)).lock,
                LW_EXCLUSIVE,
            );
            prevbank = curbank;
        }

        /* Do nothing if slot is unused */
        if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY {
            slotno += 1;
            continue;
        }

        SlruInternalWritePage(ctl, slotno, &mut fdata);

        /*
         * In some places (e.g. checkpoints), we cannot assert that the slot
         * is clean now, since another process might have re-dirtied it
         * already.  That's okay.
         */
        Assert!(
            allow_redirtied
                || *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY
                || (*(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID
                    && !*(*shared).page_dirty.add(slotno as usize))
        );
        slotno += 1;
    }

    LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);

    /*
     * Now close any files that were open
     */
    ok = true;
    let mut i: c_int = 0;
    while i < fdata.num_files {
        if CloseTransientFile(fdata.fd[i as usize]) != 0 {
            slru_errcause = SLRU_CLOSE_FAILED;
            slru_errno = get_errno();
            pageno = fdata.segno[i as usize] * SLRU_PAGES_PER_SEGMENT;
            ok = false;
        }
        i += 1;
    }
    if !ok {
        SlruReportIOError(ctl, pageno, InvalidTransactionId);
    }

    /* Ensure that directory entries for new files are on disk. */
    if (*ctl).sync_handler != SYNC_HANDLER_NONE {
        fsync_fname((*ctl).Dir.as_ptr(), true);
    }
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * All SLRUs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the SLRU is quite full, SimpleLruTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
#[no_mangle]
pub unsafe extern "C" fn SimpleLruTruncate(ctl: SlruCtl, cutoffPage: int64) {
    let shared: SlruShared = (*ctl).shared;
    let mut prevbank: c_int;

    /* update the stats counter of truncates */
    pgstat_count_slru_truncate((*shared).slru_stats_idx);

    /*
     * Scan shared memory and remove any pages preceding the cutoff page, to
     * ensure we won't rewrite them later.  (Since this is normally called in
     * or just after a checkpoint, any dirty pages should have been flushed
     * already ... we're just being extra careful here.)
     */
    'restart: loop {
        /*
         * An important safety check: the current endpoint page must not be
         * eligible for removal.  This check is just a backstop against wraparound
         * bugs elsewhere in SLRU handling, so we don't care if we read a slightly
         * outdated value; therefore we don't add a memory barrier.
         */
        if (*ctl).PagePrecedes.unwrap()(
            pg_atomic_read_u64(&mut (*shared).latest_page_number) as int64,
            cutoffPage,
        ) {
            ereport!(
                LOG,
                errmsg!(
                    "could not truncate directory \"{}\": apparent wraparound",
                    CStr::from_ptr((*ctl).Dir.as_ptr()).to_string_lossy()
                )
            );
            return;
        }

        prevbank = SlotGetBankNumber(0);
        LWLockAcquire(
            &mut (*(*shared).bank_locks.add(prevbank as usize)).lock,
            LW_EXCLUSIVE,
        );
        let mut slotno: c_int = 0;
        while slotno < (*shared).num_slots {
            let curbank: c_int = SlotGetBankNumber(slotno);

            /*
             * If the current bank lock is not same as the previous bank lock then
             * release the previous lock and acquire the new lock.
             */
            if curbank != prevbank {
                LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
                LWLockAcquire(
                    &mut (*(*shared).bank_locks.add(curbank as usize)).lock,
                    LW_EXCLUSIVE,
                );
                prevbank = curbank;
            }

            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY {
                slotno += 1;
                continue;
            }
            if !(*ctl).PagePrecedes.unwrap()(*(*shared).page_number.add(slotno as usize), cutoffPage)
            {
                slotno += 1;
                continue;
            }

            /*
             * If page is clean, just change state to EMPTY (expected case).
             */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID
                && !*(*shared).page_dirty.add(slotno as usize)
            {
                *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_EMPTY;
                slotno += 1;
                continue;
            }

            /*
             * Hmm, we have (or may have) I/O operations acting on the page, so
             * we've got to wait for them to finish and then start again. This is
             * the same logic as in SlruSelectLRUPage.  (XXX if page is dirty,
             * wouldn't it be OK to just discard it without writing it?
             * SlruMayDeleteSegment() uses a stricter qualification, so we might
             * not delete this page in the end; even if we don't delete it, we
             * won't have cause to read its data again.  For now, keep the logic
             * the same as it was.)
             */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, ptr::null_mut());
            } else {
                SimpleLruWaitIO(ctl, slotno);
            }

            LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
            continue 'restart;
        }

        LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
        break;
    }

    /* Now we can remove the old segment(s) */
    let mut cutoffPage = cutoffPage;
    SlruScanDirectory(
        ctl,
        SlruScanDirCbDeleteCutoff,
        &mut cutoffPage as *mut int64 as *mut c_void,
    );
}

/*
 * Delete an individual SLRU segment.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
unsafe fn SlruInternalDeleteSegment(ctl: SlruCtl, segno: int64) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /* Forget any fsync requests queued for this segment. */
    if (*ctl).sync_handler != SYNC_HANDLER_NONE {
        let mut tag: FileTag = core::mem::zeroed();

        INIT_SLRUFILETAG(&mut tag, (*ctl).sync_handler, segno);
        RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
    }

    /* Unlink the file. */
    SlruFileName(ctl, path.as_mut_ptr(), segno);
    ereport!(
        DEBUG2,
        errmsg!(
            "removing file \"{}\"",
            CStr::from_ptr(path.as_ptr()).to_string_lossy()
        )
    );
    /* C also: errmsg_internal */
    unlink(path.as_ptr());
}

/*
 * Delete an individual SLRU segment, identified by the segment number.
 */
#[no_mangle]
pub unsafe extern "C" fn SlruDeleteSegment(ctl: SlruCtl, segno: int64) {
    let shared: SlruShared = (*ctl).shared;
    let mut prevbank: c_int = SlotGetBankNumber(0);
    let mut did_write: bool;

    /* Clean out any possibly existing references to the segment. */
    LWLockAcquire(
        &mut (*(*shared).bank_locks.add(prevbank as usize)).lock,
        LW_EXCLUSIVE,
    );
    loop {
        did_write = false;
        let mut slotno: c_int = 0;
        while slotno < (*shared).num_slots {
            let pagesegno: int64;
            let curbank: c_int = SlotGetBankNumber(slotno);

            /*
             * If the current bank lock is not same as the previous bank lock then
             * release the previous lock and acquire the new lock.
             */
            if curbank != prevbank {
                LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
                LWLockAcquire(
                    &mut (*(*shared).bank_locks.add(curbank as usize)).lock,
                    LW_EXCLUSIVE,
                );
                prevbank = curbank;
            }

            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_EMPTY {
                slotno += 1;
                continue;
            }

            pagesegno = *(*shared).page_number.add(slotno as usize) / SLRU_PAGES_PER_SEGMENT;
            /* not the segment we're looking for */
            if pagesegno != segno {
                slotno += 1;
                continue;
            }

            /* If page is clean, just change state to EMPTY (expected case). */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID
                && !*(*shared).page_dirty.add(slotno as usize)
            {
                *(*shared).page_status.add(slotno as usize) = SLRU_PAGE_EMPTY;
                slotno += 1;
                continue;
            }

            /* Same logic as SimpleLruTruncate() */
            if *(*shared).page_status.add(slotno as usize) == SLRU_PAGE_VALID {
                SlruInternalWritePage(ctl, slotno, ptr::null_mut());
            } else {
                SimpleLruWaitIO(ctl, slotno);
            }

            did_write = true;
            slotno += 1;
        }

        /*
         * Be extra careful and re-check. The IO functions release the control
         * lock, so new pages could have been read in.
         */
        if did_write {
            continue;
        }
        break;
    }

    SlruInternalDeleteSegment(ctl, segno);

    LWLockRelease(&mut (*(*shared).bank_locks.add(prevbank as usize)).lock);
}

/*
 * Determine whether a segment is okay to delete.
 *
 * segpage is the first page of the segment, and cutoffPage is the oldest (in
 * PagePrecedes order) page in the SLRU containing still-useful data.  Since
 * every core PagePrecedes callback implements "wrap around", check the
 * segment's first and last pages:
 *
 * first<cutoff  && last<cutoff:  yes
 * first<cutoff  && last>=cutoff: no; cutoff falls inside this segment
 * first>=cutoff && last<cutoff:  no; wrap point falls inside this segment
 * first>=cutoff && last>=cutoff: no; every page of this segment is too young
 */
unsafe fn SlruMayDeleteSegment(ctl: SlruCtl, segpage: int64, cutoffPage: int64) -> bool {
    let seg_last_page: int64 = segpage + SLRU_PAGES_PER_SEGMENT - 1;

    Assert!(segpage % SLRU_PAGES_PER_SEGMENT == 0);

    (*ctl).PagePrecedes.unwrap()(segpage, cutoffPage)
        && (*ctl).PagePrecedes.unwrap()(seg_last_page, cutoffPage)
}

#[cfg(debug_assertions)]
unsafe fn SlruPagePrecedesTestOffset(ctl: SlruCtl, per_page: c_int, offset: uint32) {
    let lhs: TransactionId;
    let rhs: TransactionId;
    let mut newestPage: int64;
    let mut oldestPage: int64;
    let mut newestXact: TransactionId;
    let mut oldestXact: TransactionId;

    /*
     * Compare an XID pair having undefined order (see RFC 1982), a pair at
     * "opposite ends" of the XID space.  TransactionIdPrecedes() treats each
     * as preceding the other.  If RHS is oldestXact, LHS is the first XID we
     * must not assign.
     */
    lhs = per_page as TransactionId + offset; /* skip first page to avoid non-normal XIDs */
    rhs = lhs.wrapping_add(1u32 << 31);
    Assert!(TransactionIdPrecedes(lhs, rhs));
    Assert!(TransactionIdPrecedes(rhs, lhs));
    Assert!(!TransactionIdPrecedes(lhs.wrapping_sub(1), rhs));
    Assert!(TransactionIdPrecedes(rhs, lhs.wrapping_sub(1)));
    Assert!(TransactionIdPrecedes(lhs.wrapping_add(1), rhs));
    Assert!(!TransactionIdPrecedes(rhs, lhs.wrapping_add(1)));
    Assert!(!TransactionIdFollowsOrEquals(lhs, rhs));
    Assert!(!TransactionIdFollowsOrEquals(rhs, lhs));
    Assert!(!(*ctl).PagePrecedes.unwrap()(
        lhs as int64 / per_page as int64,
        lhs as int64 / per_page as int64
    ));
    Assert!(!(*ctl).PagePrecedes.unwrap()(
        lhs as int64 / per_page as int64,
        rhs as int64 / per_page as int64
    ));
    Assert!(!(*ctl).PagePrecedes.unwrap()(
        rhs as int64 / per_page as int64,
        lhs as int64 / per_page as int64
    ));
    Assert!(!(*ctl).PagePrecedes.unwrap()(
        (lhs as int64 - per_page as int64) / per_page as int64,
        rhs as int64 / per_page as int64
    ));
    Assert!((*ctl).PagePrecedes.unwrap()(
        rhs as int64 / per_page as int64,
        (lhs as int64 - 3 * per_page as int64) / per_page as int64
    ));
    Assert!((*ctl).PagePrecedes.unwrap()(
        rhs as int64 / per_page as int64,
        (lhs as int64 - 2 * per_page as int64) / per_page as int64
    ));
    Assert!(
        (*ctl).PagePrecedes.unwrap()(
            rhs as int64 / per_page as int64,
            (lhs as int64 - 1 * per_page as int64) / per_page as int64
        ) || (1u32 << 31) % per_page as uint32 != 0
    ); /* See CommitTsPagePrecedes() */
    Assert!(
        (*ctl).PagePrecedes.unwrap()(
            (lhs as int64 + 1 * per_page as int64) / per_page as int64,
            rhs as int64 / per_page as int64
        ) || (1u32 << 31) % per_page as uint32 != 0
    );
    Assert!((*ctl).PagePrecedes.unwrap()(
        (lhs as int64 + 2 * per_page as int64) / per_page as int64,
        rhs as int64 / per_page as int64
    ));
    Assert!((*ctl).PagePrecedes.unwrap()(
        (lhs as int64 + 3 * per_page as int64) / per_page as int64,
        rhs as int64 / per_page as int64
    ));
    Assert!(!(*ctl).PagePrecedes.unwrap()(
        rhs as int64 / per_page as int64,
        (lhs as int64 + per_page as int64) / per_page as int64
    ));

    /*
     * GetNewTransactionId() has assigned the last XID it can safely use, and
     * that XID is in the *LAST* page of the second segment.  We must not
     * delete that segment.
     */
    newestPage = 2 * SLRU_PAGES_PER_SEGMENT - 1;
    newestXact = (newestPage * per_page as int64) as TransactionId + offset;
    Assert!(newestXact as int64 / per_page as int64 == newestPage);
    oldestXact = newestXact.wrapping_add(1);
    oldestXact = oldestXact.wrapping_sub(1u32 << 31);
    oldestPage = oldestXact as int64 / per_page as int64;
    Assert!(!SlruMayDeleteSegment(
        ctl,
        newestPage - newestPage % SLRU_PAGES_PER_SEGMENT,
        oldestPage
    ));

    /*
     * GetNewTransactionId() has assigned the last XID it can safely use, and
     * that XID is in the *FIRST* page of the second segment.  We must not
     * delete that segment.
     */
    newestPage = SLRU_PAGES_PER_SEGMENT;
    newestXact = (newestPage * per_page as int64) as TransactionId + offset;
    Assert!(newestXact as int64 / per_page as int64 == newestPage);
    oldestXact = newestXact.wrapping_add(1);
    oldestXact = oldestXact.wrapping_sub(1u32 << 31);
    oldestPage = oldestXact as int64 / per_page as int64;
    Assert!(!SlruMayDeleteSegment(
        ctl,
        newestPage - newestPage % SLRU_PAGES_PER_SEGMENT,
        oldestPage
    ));
}

/*
 * Unit-test a PagePrecedes function.
 *
 * This assumes every uint32 >= FirstNormalTransactionId is a valid key.  It
 * assumes each value occupies a contiguous, fixed-size region of SLRU bytes.
 * (MultiXactMemberCtl separates flags from XIDs.  NotifyCtl has
 * variable-length entries, no keys, and no random access.  These unit tests
 * do not apply to them.)
 */
#[cfg(debug_assertions)]
#[no_mangle]
pub unsafe extern "C" fn SlruPagePrecedesUnitTests(ctl: SlruCtl, per_page: c_int) {
    /* Test first, middle and last entries of a page. */
    SlruPagePrecedesTestOffset(ctl, per_page, 0);
    SlruPagePrecedesTestOffset(ctl, per_page, (per_page / 2) as uint32);
    SlruPagePrecedesTestOffset(ctl, per_page, (per_page - 1) as uint32);
}

/*
 * SlruScanDirectory callback
 *		This callback reports true if there's any segment wholly prior to the
 *		one containing the page passed as "data".
 */
#[no_mangle]
pub unsafe extern "C" fn SlruScanDirCbReportPresence(
    ctl: SlruCtl,
    _filename: *mut c_char,
    segpage: int64,
    data: *mut c_void,
) -> bool {
    let cutoffPage: int64 = *(data as *mut int64);

    if SlruMayDeleteSegment(ctl, segpage, cutoffPage) {
        return true; /* found one; don't iterate any more */
    }

    false /* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
unsafe extern "C" fn SlruScanDirCbDeleteCutoff(
    ctl: SlruCtl,
    _filename: *mut c_char,
    segpage: int64,
    data: *mut c_void,
) -> bool {
    let cutoffPage: int64 = *(data as *mut int64);

    if SlruMayDeleteSegment(ctl, segpage, cutoffPage) {
        SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);
    }

    false /* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
#[no_mangle]
pub unsafe extern "C" fn SlruScanDirCbDeleteAll(
    ctl: SlruCtl,
    _filename: *mut c_char,
    segpage: int64,
    _data: *mut c_void,
) -> bool {
    SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);

    false /* keep going */
}

/*
 * An internal function used by SlruScanDirectory().
 *
 * Returns true if a file with a name of a given length may be a correct
 * SLRU segment.
 */
#[inline]
unsafe fn SlruCorrectSegmentFilenameLength(ctl: SlruCtl, len: usize) -> bool {
    if (*ctl).long_segment_names {
        len == 15 /* see SlruFileName() */
    } else {
        /*
         * Commit 638cf09e76d allowed 5-character lengths. Later commit
         * 73c986adde5 allowed 6-character length.
         *
         * Note: There is an ongoing plan to migrate all SLRUs to 64-bit page
         * numbers, and the corresponding 15-character file names, which may
         * eventually deprecate the support for 4, 5, and 6-character names.
         */
        len == 4 || len == 5 || len == 6
    }
}

/*
 * Scan the SimpleLru directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the SlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
#[no_mangle]
pub unsafe extern "C" fn SlruScanDirectory(
    ctl: SlruCtl,
    callback: SlruScanCallback,
    data: *mut c_void,
) -> bool {
    let mut retval: bool = false;
    let cldir: *mut DIR;
    let mut clde: *mut dirent;

    cldir = AllocateDir((*ctl).Dir.as_ptr());
    loop {
        clde = ReadDir(cldir, (*ctl).Dir.as_ptr());
        if clde.is_null() {
            break;
        }

        let len: usize;

        len = strlen((*clde).d_name.as_ptr());

        if SlruCorrectSegmentFilenameLength(ctl, len)
            && strspn(
                (*clde).d_name.as_ptr(),
                c"0123456789ABCDEF".as_ptr(),
            ) == len
        {
            let segno = strtoi64((*clde).d_name.as_ptr(), ptr::null_mut(), 16);
            let segpage = segno * SLRU_PAGES_PER_SEGMENT;

            elog!(
                DEBUG2,
                "SlruScanDirectory invoking callback on {}/{}",
                CStr::from_ptr((*ctl).Dir.as_ptr()).to_string_lossy(),
                CStr::from_ptr((*clde).d_name.as_ptr()).to_string_lossy()
            );
            retval = callback(ctl, (*clde).d_name.as_mut_ptr(), segpage, data);
            if retval {
                break;
            }
        }
    }
    FreeDir(cldir);

    retval
}

/*
 * Individual SLRUs (clog, ...) have to provide a sync.c handler function so
 * that they can provide the correct "SlruCtl" (otherwise we don't know how to
 * build the path), but they just forward to this common implementation that
 * performs the fsync.
 */
#[no_mangle]
pub unsafe extern "C" fn SlruSyncFileTag(
    ctl: SlruCtl,
    ftag: *const FileTag,
    path: *mut c_char,
) -> c_int {
    let fd: c_int;
    let save_errno: c_int;
    let result: c_int;

    SlruFileName(ctl, path, (*ftag).segno as int64);

    fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
    if fd < 0 {
        return -1;
    }

    pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
    result = pg_fsync(fd);
    pgstat_report_wait_end();
    save_errno = get_errno();

    CloseTransientFile(fd);

    set_errno(save_errno);
    result
}

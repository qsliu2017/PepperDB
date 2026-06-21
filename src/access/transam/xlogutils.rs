//! PostgreSQL write-ahead log manager utility routines.
//!
//! This file contains support routines that are used by XLOG replay functions.
//! None of this code is used during normal system operation.
//!
//! Translated 1:1 from postgres/src/backend/access/transam/xlogutils.c
//! Companion header: postgres/src/include/access/xlogutils.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::attnum::AttrNumber; // not used directly; kept for parity
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::nodes::pg_list::List;
use crate::pg_config_manual::MAXPGPATH;
use crate::storage::block::BlockNumber;

// ----------------------------------------------------------------------------
// Types from xlogutils.h
// ----------------------------------------------------------------------------

/// Like InRecovery, standbyState is only valid in the startup process.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HotStandbyState {
    STANDBY_DISABLED,
    STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING,
    STANDBY_SNAPSHOT_READY,
}
pub use HotStandbyState::*;

/// #define InHotStandby (standbyState >= STANDBY_SNAPSHOT_PENDING)
#[inline]
pub unsafe fn InHotStandby() -> bool {
    standbyState >= STANDBY_SNAPSHOT_PENDING
}

/// Result codes for XLogReadBufferForRedo[Extended]
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum XLogRedoAction {
    BLK_NEEDS_REDO, /* changes from WAL record need to be applied */
    BLK_DONE,       /* block is already up-to-date */
    BLK_RESTORED,   /* block was restored from a full-page image */
    BLK_NOTFOUND,   /* block was not found (and hence does not need to be replayed) */
}
pub use XLogRedoAction::*;

/// Private data of the read_local_xlog_page_no_wait callback.
#[repr(C)]
pub struct ReadLocalXLogPageNoWaitPrivate {
    pub end_of_wal: bool, /* true, when end of WAL is reached */
}

// ----------------------------------------------------------------------------
// Stub type aliases for unported dependencies.
// ----------------------------------------------------------------------------

pub type Oid = crate::postgres_ext::Oid;
pub type ForkNumber = c_int;
pub type Buffer = c_int;
pub type Page = *mut c_char;
pub type RelFileLocator = XlogutilsRelFileLocator;
pub type SMgrRelation = *mut c_void;
pub type Relation = *mut RelationData;
pub type ReadBufferMode = c_int;
pub type XLogReaderState = c_void;
pub type WALReadError = WALReadErrorStub;
pub type HTAB = c_void;
pub type HASH_SEQ_STATUS = c_void;
pub type HASHCTL = HashCtlStub;
pub type RelPathStr = RelPathStrStub;
pub type uint8 = crate::c::uint8;
pub type uint32 = crate::c::uint32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct XlogutilsRelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
}

#[repr(C)]
pub struct RelPathStrStub {
    pub str: [c_char; MAXPGPATH as usize],
}

#[repr(C)]
pub struct HashCtlStub {
    pub keysize: Size,
    pub entrysize: Size,
}

#[repr(C)]
pub struct WALOpenSegment {
    pub ws_file: c_int,
    pub ws_segno: XLogSegNo,
    pub ws_tli: TimeLineID,
}

#[repr(C)]
pub struct WALReadErrorStub {
    pub wre_errno: c_int,
    pub wre_off: c_int,
    pub wre_req: c_int,
    pub wre_read: c_int,
    pub wre_seg: WALOpenSegment,
}

#[repr(C)]
pub struct RelationData {
    pub rd_rel: *mut FormData_pg_class,
    pub rd_locator: RelFileLocator,
    pub rd_backend: c_int,
    pub rd_smgr: SMgrRelation,
    pub rd_lockInfo: LockInfoData,
}

#[repr(C)]
pub struct FormData_pg_class {
    pub relpersistence: c_char,
    pub relname: [c_char; 64],
}

#[repr(C)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}

#[repr(C)]
pub struct LockRelId {
    pub relId: Oid,
    pub dbId: Oid,
}

// Constants used here (stubbed when not yet imported elsewhere).
pub const INVALID_PROC_NUMBER: c_int = -1;
pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
pub const P_NEW: BlockNumber = BlockNumber::MAX; // InvalidBlockNumber
pub const InvalidBuffer: Buffer = 0;
pub const InvalidXLogRecPtr: XLogRecPtr = 0;
pub const INIT_FORKNUM: ForkNumber = 3;
pub const RBM_NORMAL: ReadBufferMode = 0;
pub const RBM_ZERO_AND_LOCK: ReadBufferMode = 1;
pub const RBM_ZERO_AND_CLEANUP_LOCK: ReadBufferMode = 2;
pub const RBM_NORMAL_NO_LOG: ReadBufferMode = 4;
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40;
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_ENTER: c_int = 1;
pub const HASH_REMOVE: c_int = 2;
pub const O_RDONLY: c_int = 0;
pub const PG_BINARY: c_int = 0;
pub const ENOENT: c_int = 2;
pub const ERRCODE_INTERNAL_ERROR: c_int = 0;
pub const ERRCODE_DATA_CORRUPTED: c_int = 0;
pub const XLOG_BLCKSZ: u32 = 8192;
pub const MAXFNAMELEN: usize = 64;
pub const EB_PERFORMING_RECOVERY: u32 = 0x08;
pub const EB_SKIP_EXTENSION_LOCK: u32 = 0x01;

// ----------------------------------------------------------------------------
// GUC / global state
// ----------------------------------------------------------------------------

/* GUC variable */
pub static mut ignore_invalid_pages: bool = false;

/*
 * Are we doing recovery from XLOG?
 *
 * This is only ever true in the startup process; it should be read as meaning
 * "this process is replaying WAL records", rather than "the system is in
 * recovery mode".  It should be examined primarily by functions that need
 * to act differently when called from a WAL redo function (e.g., to skip WAL
 * logging).  To check whether the system is in recovery regardless of which
 * process you're running in, use RecoveryInProgress() but only after shared
 * memory startup and lock initialization.
 *
 * This is updated from xlog.c and xlogrecovery.c, but lives here because
 * it's mostly read by WAL redo functions.
 */
pub static mut InRecovery: bool = false;

/* Are we in Hot Standby mode? Only valid in startup process, see xlogutils.h */
pub static mut standbyState: HotStandbyState = STANDBY_DISABLED;

/*
 * During XLOG replay, we may see XLOG records for incremental updates of
 * pages that no longer exist, because their relation was later dropped or
 * truncated.  (Note: this is only possible when full_page_writes = OFF,
 * since when it's ON, the first reference we see to a page should always
 * be a full-page rewrite not an incremental update.)  Rather than simply
 * ignoring such records, we make a note of the referenced page, and then
 * complain if we don't actually see a drop or truncate covering the page
 * later in replay.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_invalid_page_key {
    pub locator: RelFileLocator, /* the relation */
    pub forkno: ForkNumber,      /* the fork number */
    pub blkno: BlockNumber,      /* the page */
}

#[repr(C)]
pub struct xl_invalid_page {
    pub key: xl_invalid_page_key, /* hash key ... must be first */
    pub present: bool,            /* page existed but contained zeroes */
}

static mut invalid_page_tab: *mut HTAB = null_mut();

// ----------------------------------------------------------------------------
// Local stubs for unported helper functions / macros.
// ----------------------------------------------------------------------------

unsafe fn relpathperm(locator: RelFileLocator, forkno: ForkNumber) -> RelPathStr {
    let _ = (locator, forkno);
    unimplemented!() // TODO: common/relpath.c
}
unsafe fn message_level_is_interesting(elevel: c_int) -> bool { crate::utils::error::elog_impl::message_level_is_interesting(elevel as _) }
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_long,
    info: *mut HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    let _ = (tabname, nelem, info, flags);
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    let _ = (hashp, keyPtr, action, foundPtr);
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) { crate::utils::hash::dynahash::hash_seq_init(status as _, hashp as _) }
unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void { crate::utils::hash::dynahash::hash_seq_search(status as _) }
unsafe fn hash_get_num_entries(hashp: *mut HTAB) -> c_long { crate::utils::hash::dynahash::hash_get_num_entries(hashp as _) }
unsafe fn hash_destroy(hashp: *mut HTAB) { crate::utils::hash::dynahash::hash_destroy(hashp as _) }
unsafe fn RelFileLocatorEquals(a: RelFileLocator, b: RelFileLocator) -> bool {
    a.relNumber == b.relNumber && a.dbOid == b.dbOid && a.spcOid == b.spcOid
}
unsafe fn XLogRecGetBlockTagExtended(
    record: *mut XLogReaderState,
    block_id: uint8,
    rlocator: *mut RelFileLocator,
    forknum: *mut ForkNumber,
    blknum: *mut BlockNumber,
    prefetch_buffer: *mut Buffer,
) -> bool {
    let _ = (record, block_id, rlocator, forknum, blknum, prefetch_buffer);
    unimplemented!() // TODO: access/transam/xlogreader.c
}
unsafe fn XLogRecGetBlock(record: *mut XLogReaderState, block_id: uint8) -> *mut DecodedBkpBlock { unimplemented!() }
unsafe fn XLogRecBlockImageApply(record: *mut XLogReaderState, block_id: uint8) -> bool { crate::access::transam::xlogreader::XLogRecBlockImageApply(record as _, block_id as _) }
unsafe fn XLogRecHasBlockImage(record: *mut XLogReaderState, block_id: uint8) -> bool { crate::access::transam::xlogreader::XLogRecHasBlockImage(record as _, block_id as _) }
unsafe fn RestoreBlockImage(
    record: *mut XLogReaderState,
    block_id: uint8,
    page: Page,
) -> bool {
    let _ = (record, block_id, page);
    unimplemented!() // TODO: access/transam/xlogreader.c
}
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    let _ = buffer;
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn PageIsNew(page: Page) -> bool { crate::storage::bufpage::PageIsNew(page as _) }
unsafe fn PageSetLSN(page: Page, lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(page as _, lsn as _) }
unsafe fn PageGetLSN(page: Page) -> XLogRecPtr { crate::storage::bufpage::PageGetLSN(page as _) }
unsafe fn MarkBufferDirty(buffer: Buffer) {
    let _ = buffer;
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn FlushOneBuffer(buffer: Buffer) { crate::storage::buffer::bufmgr::FlushOneBuffer(buffer as _) }
unsafe fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    let _ = (buffer, mode);
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn LockBufferForCleanup(buffer: Buffer) {
    let _ = buffer;
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn ReleaseBuffer(buffer: Buffer) {
    let _ = buffer;
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn ReadRecentBuffer(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    recent_buffer: Buffer,
) -> bool {
    let _ = (rlocator, forknum, blkno, recent_buffer);
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn ReadBufferWithoutRelcache(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    strategy: *mut c_void,
    permanent: bool,
) -> Buffer {
    let _ = (rlocator, forknum, blkno, mode, strategy, permanent);
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn ExtendBufferedRelTo(
    bmr: BufferManagerRelation,
    fork: ForkNumber,
    strategy: *mut c_void,
    flags: u32,
    extend_to: BlockNumber,
    mode: ReadBufferMode,
) -> Buffer {
    let _ = (bmr, fork, strategy, flags, extend_to, mode);
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn smgropen(rlocator: RelFileLocator, backend: c_int) -> SMgrRelation {
    let _ = (rlocator, backend);
    unimplemented!() // TODO: storage/smgr/smgr.c
}
unsafe fn smgrcreate(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool) { crate::storage::smgr::smgr::smgrcreate(reln as _, forknum as _, isRedo as _) }
unsafe fn smgrnblocks(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    let _ = (reln, forknum);
    unimplemented!() // TODO: storage/smgr/smgr.c
}
unsafe fn smgrdestroyall() { crate::storage::smgr::smgr::smgrdestroyall() }
unsafe fn BMR_SMGR(smgr: SMgrRelation, relpersistence: c_char) -> BufferManagerRelation {
    let _ = relpersistence;
    BufferManagerRelation { smgr }
}
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }
unsafe fn GetFlushRecPtr(insertTLI: *mut TimeLineID) -> XLogRecPtr { crate::access::transam::xlog::GetFlushRecPtr(insertTLI as _) }
unsafe fn GetXLogReplayRecPtr(replayTLI: *mut TimeLineID) -> XLogRecPtr { crate::access::transam::xlogrecovery::GetXLogReplayRecPtr(replayTLI as _) }
unsafe fn readTimeLineHistory(targetTLI: TimeLineID) -> *mut List {
    let _ = targetTLI;
    unimplemented!() // TODO: access/transam/timeline.c
}
unsafe fn tliOfPointInHistory(ptr: XLogRecPtr, history: *mut List) -> TimeLineID { crate::access::transam::timeline::tliOfPointInHistory(ptr as _, history as _) }
unsafe fn tliSwitchPoint(
    tli: TimeLineID,
    history: *mut List,
    nextTLI: *mut TimeLineID,
) -> XLogRecPtr {
    let _ = (tli, history, nextTLI);
    unimplemented!() // TODO: access/transam/timeline.c
}
unsafe fn list_free_deep(list: *mut List) {
    let _ = list;
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn XLogFilePath(
    path: *mut c_char,
    tli: TimeLineID,
    logSegNo: XLogSegNo,
    wal_segsz_bytes: c_int,
) {
    let _ = (path, tli, logSegNo, wal_segsz_bytes);
    unimplemented!() // TODO: access/transam/xlog_internal.h
}
unsafe fn XLogFileName(
    fname: *mut c_char,
    tli: TimeLineID,
    logSegNo: XLogSegNo,
    wal_segsz_bytes: c_int,
) {
    let _ = (fname, tli, logSegNo, wal_segsz_bytes);
    unimplemented!() // TODO: access/transam/xlog_internal.h
}
unsafe fn BasicOpenFile(fileName: *const c_char, fileFlags: c_int) -> c_int { crate::storage::file::fd::BasicOpenFile(fileName as _, fileFlags as _) }
unsafe fn WALRead(
    state: *mut XLogReaderState,
    buf: *mut c_char,
    startptr: XLogRecPtr,
    count: Size,
    tli: TimeLineID,
    errinfo: *mut WALReadError,
) -> bool {
    let _ = (state, buf, startptr, count, tli, errinfo);
    unimplemented!() // TODO: access/transam/xlogreader.c
}
unsafe fn pg_usleep(microsec: c_long) {
    let _ = microsec;
    unimplemented!() // TODO: port/pgsleep.c
}
unsafe fn errcode_for_file_access() -> c_int { crate::utils::error::elog_impl::errcode_for_file_access() }

extern "C" {
    fn close(fd: c_int) -> c_int;
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
}

#[repr(C)]
pub struct DecodedBkpBlock {
    pub flags: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BufferManagerRelation {
    pub smgr: SMgrRelation,
}

/// wal_segment_size GUC.
pub static mut wal_segment_size: c_int = 16 * 1024 * 1024;

// Helpers that mirror C macros over XLogReaderState. Since XLogReaderState is an
// opaque stub here, accessor functions stand in for direct field access.
unsafe fn XLogReader_EndRecPtr(record: *mut XLogReaderState) -> XLogRecPtr {
    let _ = record;
    unimplemented!() // TODO: access/transam/xlogreader.h
}

// ----------------------------------------------------------------------------
// Functions
// ----------------------------------------------------------------------------

/* Report a reference to an invalid page */
unsafe fn report_invalid_page(
    elevel: c_int,
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) {
    let path: RelPathStr = relpathperm(locator, forkno);

    if present {
        elog!(
            elevel,
            "page {} of relation {} is uninitialized",
            blkno,
            CStr_str(path.str.as_ptr())
        );
    } else {
        elog!(
            elevel,
            "page {} of relation {} does not exist",
            blkno,
            CStr_str(path.str.as_ptr())
        );
    }
}

/// Helper to render a NUL-terminated C string for elog! formatting.
unsafe fn CStr_str(p: *const c_char) -> &'static str {
    if p.is_null() {
        return "";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}

/* Log a reference to an invalid page */
unsafe fn log_invalid_page(
    locator: RelFileLocator,
    forkno: ForkNumber,
    blkno: BlockNumber,
    present: bool,
) {
    let mut key: xl_invalid_page_key = core::mem::zeroed();
    let hentry: *mut xl_invalid_page;
    let mut found: bool = false;

    /*
     * Once recovery has reached a consistent state, the invalid-page table
     * should be empty and remain so. If a reference to an invalid page is
     * found after consistency is reached, PANIC immediately. This might seem
     * aggressive, but it's better than letting the invalid reference linger
     * in the hash table until the end of recovery and PANIC there, which
     * might come only much later if this is a standby server.
     */
    if reachedConsistency {
        report_invalid_page(WARNING, locator, forkno, blkno, present);
        elog!(
            if ignore_invalid_pages { WARNING } else { PANIC },
            "WAL contains references to invalid pages"
        );
    }

    /*
     * Log references to invalid pages at DEBUG1 level.  This allows some
     * tracing of the cause (note the elog context mechanism will tell us
     * something about the XLOG record that generated the reference).
     */
    if message_level_is_interesting(DEBUG1) {
        report_invalid_page(DEBUG1, locator, forkno, blkno, present);
    }

    if invalid_page_tab.is_null() {
        /* create hash table when first needed */
        let mut ctl: HASHCTL = core::mem::zeroed();

        ctl.keysize = core::mem::size_of::<xl_invalid_page_key>() as Size;
        ctl.entrysize = core::mem::size_of::<xl_invalid_page>() as Size;

        invalid_page_tab = hash_create(
            c"XLOG invalid-page table".as_ptr(),
            100,
            &mut ctl,
            HASH_ELEM | HASH_BLOBS,
        );
    }

    /* we currently assume xl_invalid_page_key contains no padding */
    key.locator = locator;
    key.forkno = forkno;
    key.blkno = blkno;
    hentry = hash_search(
        invalid_page_tab,
        &key as *const _ as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut xl_invalid_page;

    if !found {
        /* hash_search already filled in the key */
        (*hentry).present = present;
    } else {
        /* repeat reference ... leave "present" as it was */
    }
}

/* Forget any invalid pages >= minblkno, because they've been dropped */
unsafe fn forget_invalid_pages(
    locator: RelFileLocator,
    forkno: ForkNumber,
    minblkno: BlockNumber,
) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut xl_invalid_page;

    if invalid_page_tab.is_null() {
        return; /* nothing to do */
    }

    hash_seq_init(&mut status, invalid_page_tab);

    loop {
        hentry = hash_seq_search(&mut status) as *mut xl_invalid_page;
        if hentry.is_null() {
            break;
        }
        if RelFileLocatorEquals((*hentry).key.locator, locator)
            && (*hentry).key.forkno == forkno
            && (*hentry).key.blkno >= minblkno
        {
            elog!(
                DEBUG2,
                "page {} of relation {} has been dropped",
                (*hentry).key.blkno,
                CStr_str(relpathperm((*hentry).key.locator, forkno).str.as_ptr())
            );

            if hash_search(
                invalid_page_tab,
                &(*hentry).key as *const _ as *const c_void,
                HASH_REMOVE,
                null_mut(),
            )
            .is_null()
            {
                elog!(ERROR, "hash table corrupted");
            }
        }
    }
}

/* Forget any invalid pages in a whole database */
unsafe fn forget_invalid_pages_db(dbid: Oid) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut xl_invalid_page;

    if invalid_page_tab.is_null() {
        return; /* nothing to do */
    }

    hash_seq_init(&mut status, invalid_page_tab);

    loop {
        hentry = hash_seq_search(&mut status) as *mut xl_invalid_page;
        if hentry.is_null() {
            break;
        }
        if (*hentry).key.locator.dbOid == dbid {
            elog!(
                DEBUG2,
                "page {} of relation {} has been dropped",
                (*hentry).key.blkno,
                CStr_str(
                    relpathperm((*hentry).key.locator, (*hentry).key.forkno)
                        .str
                        .as_ptr()
                )
            );

            if hash_search(
                invalid_page_tab,
                &(*hentry).key as *const _ as *const c_void,
                HASH_REMOVE,
                null_mut(),
            )
            .is_null()
            {
                elog!(ERROR, "hash table corrupted");
            }
        }
    }
}

/* Are there any unresolved references to invalid pages? */
pub unsafe fn XLogHaveInvalidPages() -> bool {
    if !invalid_page_tab.is_null() && hash_get_num_entries(invalid_page_tab) > 0 {
        return true;
    }
    false
}

/* Complain about any remaining invalid-page entries */
pub unsafe fn XLogCheckInvalidPages() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut hentry: *mut xl_invalid_page;
    let mut foundone: bool = false;

    if invalid_page_tab.is_null() {
        return; /* nothing to do */
    }

    hash_seq_init(&mut status, invalid_page_tab);

    /*
     * Our strategy is to emit WARNING messages for all remaining entries and
     * only PANIC after we've dumped all the available info.
     */
    loop {
        hentry = hash_seq_search(&mut status) as *mut xl_invalid_page;
        if hentry.is_null() {
            break;
        }
        report_invalid_page(
            WARNING,
            (*hentry).key.locator,
            (*hentry).key.forkno,
            (*hentry).key.blkno,
            (*hentry).present,
        );
        foundone = true;
    }

    if foundone {
        elog!(
            if ignore_invalid_pages { WARNING } else { PANIC },
            "WAL contains references to invalid pages"
        );
    }

    hash_destroy(invalid_page_tab);
    invalid_page_tab = null_mut();
}

/*
 * XLogReadBufferForRedo
 *		Read a page during XLOG replay
 *
 * (see header comment in C source for full semantics)
 */
pub unsafe fn XLogReadBufferForRedo(
    record: *mut XLogReaderState,
    block_id: uint8,
    buf: *mut Buffer,
) -> XLogRedoAction {
    XLogReadBufferForRedoExtended(record, block_id, RBM_NORMAL, false, buf)
}

/*
 * Pin and lock a buffer referenced by a WAL record, for the purpose of
 * re-initializing it.
 */
pub unsafe fn XLogInitBufferForRedo(record: *mut XLogReaderState, block_id: uint8) -> Buffer {
    let mut buf: Buffer = 0;

    XLogReadBufferForRedoExtended(record, block_id, RBM_ZERO_AND_LOCK, false, &mut buf);
    buf
}

/*
 * XLogReadBufferForRedoExtended
 *		Like XLogReadBufferForRedo, but with extra options.
 */
pub unsafe fn XLogReadBufferForRedoExtended(
    record: *mut XLogReaderState,
    block_id: uint8,
    mode: ReadBufferMode,
    get_cleanup_lock: bool,
    buf: *mut Buffer,
) -> XLogRedoAction {
    let lsn: XLogRecPtr = XLogReader_EndRecPtr(record);
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut forknum: ForkNumber = 0;
    let mut blkno: BlockNumber = 0;
    let mut prefetch_buffer: Buffer = 0;
    let page: Page;
    let zeromode: bool;
    let willinit: bool;

    if !XLogRecGetBlockTagExtended(
        record,
        block_id,
        &mut rlocator,
        &mut forknum,
        &mut blkno,
        &mut prefetch_buffer,
    ) {
        /* Caller specified a bogus block_id */
        elog!(
            PANIC,
            "failed to locate backup block with ID {} in WAL record",
            block_id
        );
    }

    /*
     * Make sure that if the block is marked with WILL_INIT, the caller is
     * going to initialize it. And vice versa.
     */
    zeromode = mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK;
    willinit = ((*XLogRecGetBlock(record, block_id)).flags & BKPBLOCK_WILL_INIT) != 0;
    if willinit && !zeromode {
        elog!(
            PANIC,
            "block with WILL_INIT flag in WAL record must be zeroed by redo routine"
        );
    }
    if !willinit && zeromode {
        elog!(
            PANIC,
            "block to be initialized in redo routine must be marked with WILL_INIT flag in the WAL record"
        );
    }

    /* If it has a full-page image and it should be restored, do it. */
    if XLogRecBlockImageApply(record, block_id) {
        Assert!(XLogRecHasBlockImage(record, block_id));
        *buf = XLogReadBufferExtended(
            rlocator,
            forknum,
            blkno,
            if get_cleanup_lock {
                RBM_ZERO_AND_CLEANUP_LOCK
            } else {
                RBM_ZERO_AND_LOCK
            },
            prefetch_buffer,
        );
        page = BufferGetPage(*buf);
        if !RestoreBlockImage(record, block_id, page) {
            ereport!(ERROR, "RestoreBlockImage failed");
            unreachable!();
        }

        /*
         * The page may be uninitialized. If so, we can't set the LSN because
         * that would corrupt the page.
         */
        if !PageIsNew(page) {
            PageSetLSN(page, lsn);
        }

        MarkBufferDirty(*buf);

        /*
         * At the end of crash recovery the init forks of unlogged relations
         * are copied, without going through shared buffers. So we need to
         * force the on-disk state of init forks to always be in sync with the
         * state in shared buffers.
         */
        if forknum == INIT_FORKNUM {
            FlushOneBuffer(*buf);
        }

        BLK_RESTORED
    } else {
        *buf = XLogReadBufferExtended(rlocator, forknum, blkno, mode, prefetch_buffer);
        if BufferIsValid(*buf) {
            if mode != RBM_ZERO_AND_LOCK && mode != RBM_ZERO_AND_CLEANUP_LOCK {
                if get_cleanup_lock {
                    LockBufferForCleanup(*buf);
                } else {
                    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
                }
            }
            if lsn <= PageGetLSN(BufferGetPage(*buf)) {
                BLK_DONE
            } else {
                BLK_NEEDS_REDO
            }
        } else {
            BLK_NOTFOUND
        }
    }
}

/*
 * XLogReadBufferExtended
 *		Read a page during XLOG replay
 *
 * (see header comment in C source for full semantics)
 */
pub unsafe fn XLogReadBufferExtended(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    recent_buffer: Buffer,
) -> Buffer {
    let lastblock: BlockNumber;
    let buffer: Buffer;
    let smgr: SMgrRelation;

    Assert!(blkno != P_NEW);

    /* Do we have a clue where the buffer might be already? */
    if BufferIsValid(recent_buffer)
        && mode == RBM_NORMAL
        && ReadRecentBuffer(rlocator, forknum, blkno, recent_buffer)
    {
        buffer = recent_buffer;
        // goto recent_buffer_fast_path;
        return recent_buffer_fast_path(buffer, rlocator, forknum, blkno, mode);
    }

    /* Open the relation at smgr level */
    smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

    /*
     * Create the target file if it doesn't already exist.  This lets us cope
     * if the replay sequence contains writes to a relation that is later
     * deleted.  (The original coding of this routine would instead suppress
     * the writes, but that seems like it risks losing valuable data if the
     * filesystem loses an inode during a crash.  Better to write the data
     * until we are actually told to delete the file.)
     */
    smgrcreate(smgr, forknum, true);

    lastblock = smgrnblocks(smgr, forknum);

    if blkno < lastblock {
        /* page exists in file */
        buffer = ReadBufferWithoutRelcache(rlocator, forknum, blkno, mode, null_mut(), true);
    } else {
        /* hm, page doesn't exist in file */
        if mode == RBM_NORMAL {
            log_invalid_page(rlocator, forknum, blkno, false);
            return InvalidBuffer;
        }
        if mode == RBM_NORMAL_NO_LOG {
            return InvalidBuffer;
        }
        /* OK to extend the file */
        /* we do this in recovery only - no rel-extension lock needed */
        Assert!(InRecovery);
        buffer = ExtendBufferedRelTo(
            BMR_SMGR(smgr, RELPERSISTENCE_PERMANENT),
            forknum,
            null_mut(),
            EB_PERFORMING_RECOVERY | EB_SKIP_EXTENSION_LOCK,
            blkno + 1,
            mode,
        );
    }

    recent_buffer_fast_path(buffer, rlocator, forknum, blkno, mode)
}

/* Body following the `recent_buffer_fast_path:` label in the C source. */
unsafe fn recent_buffer_fast_path(
    buffer: Buffer,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
) -> Buffer {
    if mode == RBM_NORMAL {
        /* check that page has been initialized */
        let page: Page = BufferGetPage(buffer) as Page;

        /*
         * We assume that PageIsNew is safe without a lock. During recovery,
         * there should be no other backends that could modify the buffer at
         * the same time.
         */
        if PageIsNew(page) {
            ReleaseBuffer(buffer);
            log_invalid_page(rlocator, forknum, blkno, true);
            return InvalidBuffer;
        }
    }

    buffer
}

/*
 * Struct actually returned by CreateFakeRelcacheEntry, though the declared
 * return type is Relation.
 */
#[repr(C)]
pub struct FakeRelCacheEntryData {
    pub reldata: RelationData, /* Note: this must be first */
    pub pgc: FormData_pg_class,
}

pub type FakeRelCacheEntry = *mut FakeRelCacheEntryData;

/*
 * Create a fake relation cache entry for a physical relation
 *
 * (see header comment in C source for full semantics)
 */
pub unsafe fn CreateFakeRelcacheEntry(rlocator: RelFileLocator) -> Relation {
    let fakeentry: FakeRelCacheEntry;
    let rel: Relation;

    /* Allocate the Relation struct and all related space in one block. */
    fakeentry = palloc0(core::mem::size_of::<FakeRelCacheEntryData>()) as FakeRelCacheEntry;
    rel = fakeentry as Relation;

    (*rel).rd_rel = &mut (*fakeentry).pgc;
    (*rel).rd_locator = rlocator;

    /*
     * We will never be working with temp rels during recovery or while
     * syncing WAL-skipped files.
     */
    (*rel).rd_backend = INVALID_PROC_NUMBER;

    /* It must be a permanent table here */
    (*(*rel).rd_rel).relpersistence = RELPERSISTENCE_PERMANENT;

    /* We don't know the name of the relation; use relfilenumber instead */
    sprintf(
        RelationGetRelationName(rel),
        c"%u".as_ptr(),
        rlocator.relNumber,
    );

    /*
     * We set up the lockRelId in case anything tries to lock the dummy
     * relation.  Note that this is fairly bogus since relNumber may be
     * different from the relation's OID.  It shouldn't really matter though.
     * In recovery, we are running by ourselves and can't have any lock
     * conflicts.  While syncing, we already hold AccessExclusiveLock.
     */
    (*rel).rd_lockInfo.lockRelId.dbId = rlocator.dbOid;
    (*rel).rd_lockInfo.lockRelId.relId = rlocator.relNumber;

    /*
     * Set up a non-pinned SMgrRelation reference, so that we don't need to
     * worry about unpinning it on error.
     */
    (*rel).rd_smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

    rel
}

unsafe fn RelationGetRelationName(rel: Relation) -> *mut c_char {
    (*(*rel).rd_rel).relname.as_mut_ptr()
}

/*
 * Free a fake relation cache entry.
 */
pub unsafe fn FreeFakeRelcacheEntry(fakerel: Relation) {
    pfree(fakerel as *mut c_void);
}

/*
 * Drop a relation during XLOG replay
 *
 * This is called when the relation is about to be deleted; we need to remove
 * any open "invalid-page" records for the relation.
 */
pub unsafe fn XLogDropRelation(rlocator: RelFileLocator, forknum: ForkNumber) {
    forget_invalid_pages(rlocator, forknum, 0);
}

/*
 * Drop a whole database during XLOG replay
 *
 * As above, but for DROP DATABASE instead of dropping a single rel
 */
pub unsafe fn XLogDropDatabase(dbid: Oid) {
    /*
     * This is unnecessarily heavy-handed, as it will close SMgrRelation
     * objects for other databases as well. DROP DATABASE occurs seldom enough
     * that it's not worth introducing a variant of smgrdestroy for just this
     * purpose.
     */
    smgrdestroyall();

    forget_invalid_pages_db(dbid);
}

/*
 * Truncate a relation during XLOG replay
 *
 * We need to clean up any open "invalid-page" records for the dropped pages.
 */
pub unsafe fn XLogTruncateRelation(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    nblocks: BlockNumber,
) {
    forget_invalid_pages(rlocator, forkNum, nblocks);
}

/*
 * Determine which timeline to read an xlog page from and set the
 * XLogReaderState's currTLI to that timeline ID.
 *
 * (see header comment in C source for full semantics)
 */
pub unsafe fn XLogReadDetermineTimeline(
    state: *mut XLogReaderState,
    wantPage: XLogRecPtr,
    wantLength: uint32,
    currTLI: TimeLineID,
) {
    let lastReadPage: XLogRecPtr =
        XLogReader_seg_ws_segno(state) * XLogReader_segcxt_ws_segsize(state)
            + XLogReader_segoff(state);

    Assert!(wantPage != InvalidXLogRecPtr && wantPage % (XLOG_BLCKSZ as XLogRecPtr) == 0);
    Assert!(wantLength <= XLOG_BLCKSZ);
    Assert!(XLogReader_readLen(state) == 0 || XLogReader_readLen(state) <= XLOG_BLCKSZ as c_int);
    Assert!(currTLI != 0);

    /*
     * If the desired page is currently read in and valid, we have nothing to
     * do.
     */
    if lastReadPage == wantPage
        && XLogReader_readLen(state) != 0
        && lastReadPage + XLogReader_readLen(state) as XLogRecPtr
            >= wantPage + Min(wantLength, XLOG_BLCKSZ - 1) as XLogRecPtr
    {
        return;
    }

    /*
     * If we're reading from the current timeline, it hasn't become historical
     * and the page we're reading is after the last page read, we can again
     * just carry on.
     */
    if XLogReader_currTLI(state) == currTLI && wantPage >= lastReadPage {
        Assert!(XLogReader_currTLIValidUntil(state) == InvalidXLogRecPtr);
        return;
    }

    /*
     * If we're just reading pages from a previously validated historical
     * timeline and the timeline we're reading from is valid until the end of
     * the current segment we can just keep reading.
     */
    if XLogReader_currTLIValidUntil(state) != InvalidXLogRecPtr
        && XLogReader_currTLI(state) != currTLI
        && XLogReader_currTLI(state) != 0
        && ((wantPage + wantLength as XLogRecPtr) / XLogReader_segcxt_ws_segsize(state))
            < (XLogReader_currTLIValidUntil(state) / XLogReader_segcxt_ws_segsize(state))
    {
        return;
    }

    /*
     * If we reach this point we're either looking up a page for random
     * access, the current timeline just became historical, or we're reading
     * from a new segment containing a timeline switch.
     */
    {
        /*
         * We need to re-read the timeline history in case it's been changed
         * by a promotion or replay from a cascaded replica.
         */
        let timelineHistory: *mut List = readTimeLineHistory(currTLI);
        let endOfSegment: XLogRecPtr;

        endOfSegment = ((wantPage / XLogReader_segcxt_ws_segsize(state)) + 1)
            * XLogReader_segcxt_ws_segsize(state)
            - 1;
        Assert!(
            wantPage / XLogReader_segcxt_ws_segsize(state)
                == endOfSegment / XLogReader_segcxt_ws_segsize(state)
        );

        /*
         * Find the timeline of the last LSN on the segment containing
         * wantPage.
         */
        XLogReader_set_currTLI(state, tliOfPointInHistory(endOfSegment, timelineHistory));
        let mut nextTLI: TimeLineID = 0;
        let validUntil = tliSwitchPoint(
            XLogReader_currTLI(state),
            timelineHistory,
            &mut nextTLI,
        );
        XLogReader_set_nextTLI(state, nextTLI);
        XLogReader_set_currTLIValidUntil(state, validUntil);

        Assert!(
            XLogReader_currTLIValidUntil(state) == InvalidXLogRecPtr
                || (wantPage + wantLength as XLogRecPtr) < XLogReader_currTLIValidUntil(state)
        );

        list_free_deep(timelineHistory);

        elog!(
            DEBUG3,
            "switched to timeline {} valid until {:X}/{:X}",
            XLogReader_currTLI(state),
            (XLogReader_currTLIValidUntil(state) >> 32) as u32,
            XLogReader_currTLIValidUntil(state) as u32
        );
    }
}

/* XLogReaderRoutine->segment_open callback for local pg_wal files */
pub unsafe fn wal_segment_open(
    state: *mut XLogReaderState,
    nextSegNo: XLogSegNo,
    tli_p: *mut TimeLineID,
) {
    let tli: TimeLineID = *tli_p;
    let mut path: [c_char; MAXPGPATH as usize] = [0; MAXPGPATH as usize];

    XLogFilePath(
        path.as_mut_ptr(),
        tli,
        nextSegNo,
        XLogReader_segcxt_ws_segsize(state) as c_int,
    );
    XLogReader_set_seg_ws_file(state, BasicOpenFile(path.as_ptr(), O_RDONLY | PG_BINARY));
    if XLogReader_seg_ws_file(state) >= 0 {
        return;
    }

    if errno_get() == ENOENT {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            "requested WAL segment has already been removed"
        );
    } else {
        let _ = errcode_for_file_access();
        ereport!(ERROR, "could not open file");
    }
}

/* stock XLogReaderRoutine->segment_close callback */
pub unsafe fn wal_segment_close(state: *mut XLogReaderState) {
    close(XLogReader_seg_ws_file(state));
    /* need to check errno? */
    XLogReader_set_seg_ws_file(state, -1);
}

/*
 * XLogReaderRoutine->page_read callback for reading local xlog files
 */
pub unsafe fn read_local_xlog_page(
    state: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    cur_page: *mut c_char,
) -> c_int {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, true)
}

/*
 * Same as read_local_xlog_page except that it doesn't wait for future WAL
 * to be available.
 */
pub unsafe fn read_local_xlog_page_no_wait(
    state: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    cur_page: *mut c_char,
) -> c_int {
    read_local_xlog_page_guts(state, targetPagePtr, reqLen, targetRecPtr, cur_page, false)
}

/*
 * Implementation of read_local_xlog_page and its no wait version.
 */
unsafe fn read_local_xlog_page_guts(
    state: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    cur_page: *mut c_char,
    wait_for_wal: bool,
) -> c_int {
    let mut read_upto: XLogRecPtr;
    let loc: XLogRecPtr;
    let mut tli: TimeLineID;
    let count: c_int;
    let mut errinfo: WALReadError = core::mem::zeroed();
    let mut currTLI: TimeLineID = 0;

    let _ = targetRecPtr;

    loc = targetPagePtr + reqLen as XLogRecPtr;

    /*
     * Loop waiting for xlog to be available if necessary
     */
    loop {
        /*
         * Determine the limit of xlog we can currently read to, and what the
         * most recent timeline is.
         */
        if !RecoveryInProgress() {
            read_upto = GetFlushRecPtr(&mut currTLI);
        } else {
            read_upto = GetXLogReplayRecPtr(&mut currTLI);
        }
        tli = currTLI;

        /*
         * Check which timeline to get the record from.
         */
        XLogReadDetermineTimeline(state, targetPagePtr, reqLen as uint32, tli);

        if XLogReader_currTLI(state) == currTLI {
            if loc <= read_upto {
                break;
            }

            /* If asked, let's not wait for future WAL. */
            if !wait_for_wal {
                let private_data: *mut ReadLocalXLogPageNoWaitPrivate;

                /*
                 * Inform the caller of read_local_xlog_page_no_wait that the
                 * end of WAL has been reached.
                 */
                private_data =
                    XLogReader_private_data(state) as *mut ReadLocalXLogPageNoWaitPrivate;
                (*private_data).end_of_wal = true;
                break;
            }

            CHECK_FOR_INTERRUPTS();
            pg_usleep(1000);
        } else {
            /*
             * We're on a historical timeline, so limit reading to the switch
             * point where we moved to the next timeline.
             */
            read_upto = XLogReader_currTLIValidUntil(state);

            /*
             * Setting tli to our wanted record's TLI is slightly wrong; the
             * page might begin on an older timeline if it contains a timeline
             * switch, since its xlog segment will have been copied from the
             * prior timeline.
             */
            tli = XLogReader_currTLI(state);

            /* No need to wait on a historical timeline */
            break;
        }
    }

    if targetPagePtr + XLOG_BLCKSZ as XLogRecPtr <= read_upto {
        /*
         * more than one block available; read only that block, have caller
         * come back if they need more.
         */
        count = XLOG_BLCKSZ as c_int;
    } else if targetPagePtr + reqLen as XLogRecPtr > read_upto {
        /* not enough data there */
        return -1;
    } else {
        /* enough bytes available to satisfy the request */
        count = (read_upto - targetPagePtr) as c_int;
    }

    if !WALRead(
        state,
        cur_page,
        targetPagePtr,
        count as Size,
        tli,
        &mut errinfo,
    ) {
        WALReadRaiseError(&mut errinfo);
    }

    /* number of valid bytes in the buffer */
    count
}

/*
 * Backend-specific convenience code to handle read errors encountered by
 * WALRead().
 */
pub unsafe fn WALReadRaiseError(errinfo: *mut WALReadError) {
    let seg: *mut WALOpenSegment = &mut (*errinfo).wre_seg;
    let mut fname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

    XLogFileName(
        fname.as_mut_ptr(),
        (*seg).ws_tli,
        (*seg).ws_segno,
        wal_segment_size,
    );

    if (*errinfo).wre_read < 0 {
        errno_set((*errinfo).wre_errno);
        let _ = errcode_for_file_access();
        ereport!(ERROR, "could not read from WAL segment");
    } else if (*errinfo).wre_read == 0 {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            "could not read from WAL segment: short read"
        );
    }
}

// ----------------------------------------------------------------------------
// XLogReaderState accessor stubs (mirror direct field access in the C source).
// ----------------------------------------------------------------------------

unsafe fn XLogReader_seg_ws_segno(state: *mut XLogReaderState) -> XLogRecPtr {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->seg.ws_segno)
}
unsafe fn XLogReader_segcxt_ws_segsize(state: *mut XLogReaderState) -> XLogRecPtr {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->segcxt.ws_segsize)
}
unsafe fn XLogReader_segoff(state: *mut XLogReaderState) -> XLogRecPtr {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->segoff)
}
unsafe fn XLogReader_readLen(state: *mut XLogReaderState) -> c_int {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->readLen)
}
unsafe fn XLogReader_currTLI(state: *mut XLogReaderState) -> TimeLineID {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->currTLI)
}
unsafe fn XLogReader_set_currTLI(state: *mut XLogReaderState, v: TimeLineID) {
    let _ = (state, v);
    unimplemented!() // TODO: access/transam/xlogreader.h (state->currTLI)
}
unsafe fn XLogReader_currTLIValidUntil(state: *mut XLogReaderState) -> XLogRecPtr {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->currTLIValidUntil)
}
unsafe fn XLogReader_set_currTLIValidUntil(state: *mut XLogReaderState, v: XLogRecPtr) {
    let _ = (state, v);
    unimplemented!() // TODO: access/transam/xlogreader.h (state->currTLIValidUntil)
}
unsafe fn XLogReader_set_nextTLI(state: *mut XLogReaderState, v: TimeLineID) {
    let _ = (state, v);
    unimplemented!() // TODO: access/transam/xlogreader.h (state->nextTLI)
}
unsafe fn XLogReader_seg_ws_file(state: *mut XLogReaderState) -> c_int {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->seg.ws_file)
}
unsafe fn XLogReader_set_seg_ws_file(state: *mut XLogReaderState, v: c_int) {
    let _ = (state, v);
    unimplemented!() // TODO: access/transam/xlogreader.h (state->seg.ws_file)
}
unsafe fn XLogReader_private_data(state: *mut XLogReaderState) -> *mut c_void {
    let _ = state;
    unimplemented!() // TODO: access/transam/xlogreader.h (state->private_data)
}

// Min macro (c.h) on uint32 values.
#[inline]
fn Min(a: uint32, b: uint32) -> uint32 {
    if a < b {
        a
    } else {
        b
    }
}

// reachedConsistency global (defined in xlogrecovery.c).
extern "C" {
    static reachedConsistency: bool;
}

// errno helpers (the C source manipulates the libc errno directly).
unsafe fn errno_get() -> c_int {
    *errno_location()
}
unsafe fn errno_set(v: c_int) {
    *errno_location() = v;
}
extern "C" {
    #[cfg_attr(target_os = "macos", link_name = "__error")]
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    fn errno_location() -> *mut c_int;
}

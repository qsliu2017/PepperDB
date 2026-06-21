//! xlog_internal.h - PostgreSQL write-ahead log internal declarations.
//!
//! Declarations useful for manipulating XLOG files directly. Includable in
//! both frontend and backend contexts (e.g. pg_receivewal). The XLogRecord
//! typedef lives in xlogrecord.h.

use crate::c::{int64, uint16, uint32, uint64, uint8};
use std::ffi::{c_char, c_int, c_void};

use crate::access::rmgrlist::RmgrId;
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::lib::stringinfo::StringInfo;
use crate::pgtime::pg_time_t;
use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// Referenced-but-not-(yet)-canonically-here types. Stubbed/aliased locally.
// ---------------------------------------------------------------------------

// XLogReaderState lives in xlogreader.rs.
// TODO: dedup - use crate::access::transam::xlogreader::XLogReaderState once stable.
pub use crate::access::transam::xlogreader::XLogReaderState;

// TimestampTz: datatype/timestamp.h. Not yet canonically ported here.
// TODO: dedup - canonical home is utils/timestamp (datatype/timestamp.h).
pub type TimestampTz = int64;

// Forward-declared structs in the C header (only ever used by pointer).
// TODO: dedup - LogicalDecodingContext (replication/logical/logical.h),
//               XLogRecordBuffer (replication/decode.c).
pub type LogicalDecodingContext = c_void;
pub type XLogRecordBuffer = c_void;

// MAXPGPATH from pg_config_manual.h
pub const MAXPGPATH: usize = crate::pg_config_manual::MAXPGPATH;
// XLOG_BLCKSZ from pg_config.h
pub const XLOG_BLCKSZ: usize = crate::pg_config::XLOG_BLCKSZ;

use crate::c::MAXALIGN;

// ---------------------------------------------------------------------------
// XLOG page headers
// ---------------------------------------------------------------------------

/* can be used as WAL version indicator */
pub const XLOG_PAGE_MAGIC: uint16 = 0xD118;

/*
 * Each page of XLOG file has a header like this:
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogPageHeaderData {
    pub xlp_magic: uint16,     /* magic value for correctness checks */
    pub xlp_info: uint16,      /* flag bits, see below */
    pub xlp_tli: TimeLineID,   /* TimeLineID of first record on page */
    pub xlp_pageaddr: XLogRecPtr, /* XLOG address of this page */

    /*
     * When there is not enough space on current page for whole record, we
     * continue on the next page.  xlp_rem_len is the number of bytes
     * remaining from a previous page; it tracks xl_tot_len in the initial
     * header.  Note that the continuation data isn't necessarily aligned.
     */
    pub xlp_rem_len: uint32, /* total len of remaining data for record */
}

/* #define SizeOfXLogShortPHD MAXALIGN(sizeof(XLogPageHeaderData)) */
pub const SizeOfXLogShortPHD: usize = MAXALIGN(core::mem::size_of::<XLogPageHeaderData>());

pub type XLogPageHeader = *mut XLogPageHeaderData;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)  The additional fields serve to identify the file accurately.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogLongPageHeaderData {
    pub std: XLogPageHeaderData, /* standard header fields */
    pub xlp_sysid: uint64,       /* system identifier from pg_control */
    pub xlp_seg_size: uint32,    /* just as a cross-check */
    pub xlp_xlog_blcksz: uint32, /* just as a cross-check */
}

/* #define SizeOfXLogLongPHD MAXALIGN(sizeof(XLogLongPageHeaderData)) */
pub const SizeOfXLogLongPHD: usize = MAXALIGN(core::mem::size_of::<XLogLongPageHeaderData>());

pub type XLogLongPageHeader = *mut XLogLongPageHeaderData;

/* When record crosses page boundary, set this flag in new page's header */
pub const XLP_FIRST_IS_CONTRECORD: uint16 = 0x0001;
/* This flag indicates a "long" page header */
pub const XLP_LONG_HEADER: uint16 = 0x0002;
/* This flag indicates backup blocks starting in this page are optional */
pub const XLP_BKP_REMOVABLE: uint16 = 0x0004;
/* Replaces a missing contrecord; see CreateOverwriteContrecordRecord */
pub const XLP_FIRST_IS_OVERWRITE_CONTRECORD: uint16 = 0x0008;
/* All defined flag bits in xlp_info (used for validity checking of header) */
pub const XLP_ALL_FLAGS: uint16 = 0x000F;

/*
 * #define XLogPageHeaderSize(hdr) \
 *   (((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)
 */
#[inline]
pub unsafe fn XLogPageHeaderSize(hdr: XLogPageHeader) -> usize {
    if (*hdr).xlp_info & XLP_LONG_HEADER != 0 {
        SizeOfXLogLongPHD
    } else {
        SizeOfXLogShortPHD
    }
}

// ---------------------------------------------------------------------------
// wal_segment_size limits & helpers
// ---------------------------------------------------------------------------

/* wal_segment_size can range from 1MB to 1GB */
pub const WalSegMinSize: c_int = 1024 * 1024;
pub const WalSegMaxSize: c_int = 1024 * 1024 * 1024;
/* default number of min and max wal segments */
pub const DEFAULT_MIN_WAL_SEGS: c_int = 5;
pub const DEFAULT_MAX_WAL_SEGS: c_int = 64;

/* check that the given size is a valid wal_segment_size */
/* #define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0) */
#[inline]
pub fn IsPowerOf2(x: c_int) -> bool {
    x > 0 && (x & (x - 1)) == 0
}

/*
 * #define IsValidWalSegSize(size) \
 *   (IsPowerOf2(size) && ((size) >= WalSegMinSize && (size) <= WalSegMaxSize))
 */
#[inline]
pub fn IsValidWalSegSize(size: c_int) -> bool {
    IsPowerOf2(size) && (size >= WalSegMinSize && size <= WalSegMaxSize)
}

/*
 * #define XLogSegmentsPerXLogId(wal_segsz_bytes) \
 *   (UINT64CONST(0x100000000) / (wal_segsz_bytes))
 */
#[inline]
pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: c_int) -> uint64 {
    0x100000000u64 / (wal_segsz_bytes as uint64)
}

/*
 * #define XLogSegNoOffsetToRecPtr(segno, offset, wal_segsz_bytes, dest) \
 *   (dest) = (segno) * (wal_segsz_bytes) + (offset)
 */
#[inline]
pub fn XLogSegNoOffsetToRecPtr(
    segno: XLogSegNo,
    offset: uint32,
    wal_segsz_bytes: c_int,
    dest: &mut XLogRecPtr,
) {
    *dest = segno * (wal_segsz_bytes as XLogRecPtr) + (offset as XLogRecPtr);
}

/*
 * #define XLogSegmentOffset(xlogptr, wal_segsz_bytes) \
 *   ((xlogptr) & ((wal_segsz_bytes) - 1))
 */
#[inline]
pub fn XLogSegmentOffset(xlogptr: XLogRecPtr, wal_segsz_bytes: c_int) -> XLogRecPtr {
    xlogptr & ((wal_segsz_bytes as XLogRecPtr) - 1)
}

/*
 * Compute a segment number from an XLogRecPtr.
 *
 * For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 *
 * #define XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes) \
 *   logSegNo = (xlrp) / (wal_segsz_bytes)
 */
#[inline]
pub fn XLByteToSeg(xlrp: XLogRecPtr, logSegNo: &mut XLogSegNo, wal_segsz_bytes: c_int) {
    *logSegNo = xlrp / (wal_segsz_bytes as XLogRecPtr);
}

/*
 * #define XLByteToPrevSeg(xlrp, logSegNo, wal_segsz_bytes) \
 *   logSegNo = ((xlrp) - 1) / (wal_segsz_bytes)
 */
#[inline]
pub fn XLByteToPrevSeg(xlrp: XLogRecPtr, logSegNo: &mut XLogSegNo, wal_segsz_bytes: c_int) {
    *logSegNo = (xlrp - 1) / (wal_segsz_bytes as XLogRecPtr);
}

/*
 * Convert values of GUCs measured in megabytes to equiv. segment count.
 * Rounds down.
 *
 * #define XLogMBVarToSegs(mbvar, wal_segsz_bytes) \
 *   ((mbvar) / ((wal_segsz_bytes) / (1024 * 1024)))
 */
#[inline]
pub fn XLogMBVarToSegs(mbvar: c_int, wal_segsz_bytes: c_int) -> c_int {
    mbvar / (wal_segsz_bytes / (1024 * 1024))
}

/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * #define XLByteInSeg(xlrp, logSegNo, wal_segsz_bytes) \
 *   (((xlrp) / (wal_segsz_bytes)) == (logSegNo))
 */
#[inline]
pub fn XLByteInSeg(xlrp: XLogRecPtr, logSegNo: XLogSegNo, wal_segsz_bytes: c_int) -> bool {
    (xlrp / (wal_segsz_bytes as XLogRecPtr)) == logSegNo
}

/*
 * #define XLByteInPrevSeg(xlrp, logSegNo, wal_segsz_bytes) \
 *   ((((xlrp) - 1) / (wal_segsz_bytes)) == (logSegNo))
 */
#[inline]
pub fn XLByteInPrevSeg(xlrp: XLogRecPtr, logSegNo: XLogSegNo, wal_segsz_bytes: c_int) -> bool {
    ((xlrp - 1) / (wal_segsz_bytes as XLogRecPtr)) == logSegNo
}

/*
 * Check if an XLogRecPtr value is in a plausible range.
 *
 * #define XRecOffIsValid(xlrp) \
 *   ((xlrp) % XLOG_BLCKSZ >= SizeOfXLogShortPHD)
 */
#[inline]
pub fn XRecOffIsValid(xlrp: XLogRecPtr) -> bool {
    (xlrp % (XLOG_BLCKSZ as XLogRecPtr)) >= (SizeOfXLogShortPHD as XLogRecPtr)
}

// ---------------------------------------------------------------------------
// XLog directory / control file
// ---------------------------------------------------------------------------

/*
 * The XLog directory and control file (relative to $PGDATA)
 */
pub const XLOGDIR: &str = "pg_wal";
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";

/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
pub const MAXFNAMELEN: usize = 64;

/* Length of XLog file name */
pub const XLOG_FNAME_LEN: usize = 24;

// ---------------------------------------------------------------------------
// XLog file name / path helpers (static inline in C)
// ---------------------------------------------------------------------------

/*
 * Generate a WAL segment file name.  Do not use this function in a helper
 * function allocating the result generated.
 */
#[inline]
pub unsafe fn XLogFileName(
    _fname: *mut c_char,
    _tli: TimeLineID,
    _logSegNo: XLogSegNo,
    _wal_segsz_bytes: c_int,
) {
    let segs = XLogSegmentsPerXLogId(_wal_segsz_bytes);
    libc::snprintf(
        _fname,
        MAXFNAMELEN,
        b"%08X%08X%08X\0".as_ptr() as *const c_char,
        _tli,
        (_logSegNo / segs) as uint32,
        (_logSegNo % segs) as uint32,
    );
}

#[inline]
pub unsafe fn XLogFileNameById(
    _fname: *mut c_char,
    _tli: TimeLineID,
    _log: uint32,
    _seg: uint32,
) {
    unimplemented!()
}

#[inline]
pub unsafe fn IsXLogFileName(_fname: *const c_char) -> bool {
    unimplemented!()
}

/*
 * XLOG segment with .partial suffix.  Used by pg_receivewal and at end of
 * archive recovery, when we want to archive a WAL segment but it might not
 * be complete yet.
 */
#[inline]
pub unsafe fn IsPartialXLogFileName(_fname: *const c_char) -> bool {
    unimplemented!()
}

#[inline]
pub unsafe fn XLogFromFileName(
    _fname: *const c_char,
    _tli: *mut TimeLineID,
    _logSegNo: *mut XLogSegNo,
    _wal_segsz_bytes: c_int,
) {
    unimplemented!()
}

#[inline]
pub unsafe fn XLogFilePath(
    _path: *mut c_char,
    _tli: TimeLineID,
    _logSegNo: XLogSegNo,
    _wal_segsz_bytes: c_int,
) {
    let segs = XLogSegmentsPerXLogId(_wal_segsz_bytes);
    libc::snprintf(
        _path,
        MAXPGPATH,
        b"pg_wal/%08X%08X%08X\0".as_ptr() as *const c_char,
        _tli,
        (_logSegNo / segs) as uint32,
        (_logSegNo % segs) as uint32,
    );
}

#[inline]
pub unsafe fn TLHistoryFileName(_fname: *mut c_char, _tli: TimeLineID) {
    unimplemented!()
}

#[inline]
pub unsafe fn IsTLHistoryFileName(_fname: *const c_char) -> bool {
    unimplemented!()
}

#[inline]
pub unsafe fn TLHistoryFilePath(_path: *mut c_char, _tli: TimeLineID) {
    unimplemented!()
}

#[inline]
pub unsafe fn StatusFilePath(
    _path: *mut c_char,
    _xlog: *const c_char,
    _suffix: *const c_char,
) {
    unimplemented!()
}

#[inline]
pub unsafe fn BackupHistoryFileName(
    _fname: *mut c_char,
    _tli: TimeLineID,
    _logSegNo: XLogSegNo,
    _startpoint: XLogRecPtr,
    _wal_segsz_bytes: c_int,
) {
    unimplemented!()
}

#[inline]
pub unsafe fn IsBackupHistoryFileName(_fname: *const c_char) -> bool {
    unimplemented!()
}

#[inline]
pub unsafe fn BackupHistoryFilePath(
    _path: *mut c_char,
    _tli: TimeLineID,
    _logSegNo: XLogSegNo,
    _startpoint: XLogRecPtr,
    _wal_segsz_bytes: c_int,
) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// WAL record payload structs logged by xlog.c
// ---------------------------------------------------------------------------

/*
 * Information logged when we detect a change in one of the parameters
 * important for Hot Standby.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_parameter_change {
    pub MaxConnections: c_int,
    pub max_worker_processes: c_int,
    pub max_wal_senders: c_int,
    pub max_prepared_xacts: c_int,
    pub max_locks_per_xact: c_int,
    pub wal_level: c_int,
    pub wal_log_hints: bool,
    pub track_commit_timestamp: bool,
}

/* logs restore point */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_restore_point {
    pub rp_time: TimestampTz,
    pub rp_name: [c_char; MAXFNAMELEN],
}

/* Overwrite of prior contrecord */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_overwrite_contrecord {
    pub overwritten_lsn: XLogRecPtr,
    pub overwrite_time: TimestampTz,
}

/* End of recovery mark, when we don't do an END_OF_RECOVERY checkpoint */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_end_of_recovery {
    pub end_time: TimestampTz,
    pub ThisTimeLineID: TimeLineID, /* new TLI */
    pub PrevTimeLineID: TimeLineID, /* previous TLI we forked off from */
    pub wal_level: c_int,
}

/*
 * The functions in xloginsert.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogRecData {
    pub next: *mut XLogRecData, /* next struct in chain, or NULL */
    pub data: *const c_void,    /* start of rmgr data to include */
    pub len: uint32,            /* length of rmgr data to include */
}

/*
 * Recovery target action.
 */
pub type RecoveryTargetAction = c_int;
pub const RECOVERY_TARGET_ACTION_PAUSE: RecoveryTargetAction = 0;
pub const RECOVERY_TARGET_ACTION_PROMOTE: RecoveryTargetAction = 1;
pub const RECOVERY_TARGET_ACTION_SHUTDOWN: RecoveryTargetAction = 2;

// ---------------------------------------------------------------------------
// Resource manager method table
// ---------------------------------------------------------------------------

/*
 * Method table for resource managers.
 *
 * This struct must be kept in sync with the PG_RMGR definition in rmgr.c.
 *
 * rm_identify must return a name for the record based on xl_info (without
 * reference to the rmid).  rm_desc can then be called to obtain additional
 * detail for the record, if available (e.g. the last block).
 *
 * rm_mask takes as input a page modified by the resource manager and masks
 * out bits that shouldn't be flagged by wal_consistency_checking.
 *
 * RmgrTable[] is indexed by RmgrId values (see rmgrlist.h). If rm_name is
 * NULL, the corresponding RmgrTable entry is considered invalid.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RmgrData {
    pub rm_name: *const c_char,
    pub rm_redo: Option<unsafe extern "C" fn(record: *mut XLogReaderState)>,
    pub rm_desc: Option<unsafe extern "C" fn(buf: StringInfo, record: *mut XLogReaderState)>,
    pub rm_identify: Option<unsafe extern "C" fn(info: uint8) -> *const c_char>,
    pub rm_startup: Option<unsafe extern "C" fn()>,
    pub rm_cleanup: Option<unsafe extern "C" fn()>,
    pub rm_mask: Option<unsafe extern "C" fn(pagedata: *mut c_char, blkno: BlockNumber)>,
    pub rm_decode: Option<
        unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer),
    >,
}

extern "C" {
    pub static mut RmgrTable: [RmgrData; 0]; /* RmgrData RmgrTable[] */
}

pub unsafe fn RmgrStartup() { crate::access::transam::rmgr::RmgrStartup() }
pub unsafe fn RmgrCleanup() { crate::access::transam::rmgr::RmgrCleanup() }
pub unsafe fn RmgrNotFound(_rmid: RmgrId) { crate::access::transam::rmgr::RmgrNotFound(_rmid as _) }
pub unsafe fn RegisterCustomRmgr(_rmid: RmgrId, _rmgr: *const RmgrData) { crate::access::transam::rmgr::RegisterCustomRmgr(_rmid as _, _rmgr as _) }

/* #ifndef FRONTEND */
#[inline]
pub unsafe fn RmgrIdExists(rmid: RmgrId) -> bool {
    !(*RmgrTable.as_ptr().add(rmid as usize)).rm_name.is_null()
}

#[inline]
pub unsafe fn GetRmgr(rmid: RmgrId) -> RmgrData {
    if !RmgrIdExists(rmid) {
        RmgrNotFound(rmid);
    }
    *RmgrTable.as_ptr().add(rmid as usize)
}

// ---------------------------------------------------------------------------
// Exported function prototypes & globals
// ---------------------------------------------------------------------------

/*
 * Exported to support xlog switching from checkpointer
 */
pub unsafe fn GetLastSegSwitchData(_lastSwitchLSN: *mut XLogRecPtr) -> pg_time_t { crate::access::transam::xlog::GetLastSegSwitchData(_lastSwitchLSN as _) }
pub unsafe fn RequestXLogSwitch(_mark_unimportant: bool) -> XLogRecPtr { crate::access::transam::xlog::RequestXLogSwitch(_mark_unimportant as _) }

pub unsafe fn GetOldestRestartPoint(_oldrecptr: *mut XLogRecPtr, _oldtli: *mut TimeLineID) { crate::access::transam::xlog::GetOldestRestartPoint(_oldrecptr as _, _oldtli as _) }

pub unsafe fn XLogRecGetBlockRefInfo(
    _record: *mut XLogReaderState,
    _pretty: bool,
    _detailed_format: bool,
    _buf: StringInfo,
    _fpi_len: *mut uint32,
) {
    unimplemented!()
}

extern "C" {
    /*
     * Exported for the functions in timeline.c and xlogarchive.c.  Only valid
     * in the startup process.
     */
    pub static mut ArchiveRecoveryRequested: bool;
    pub static mut InArchiveRecovery: bool;
    pub static mut StandbyMode: bool;
    pub static mut recoveryRestoreCommand: *mut c_char;
}

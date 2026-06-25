//! Translated from PostgreSQL src/include/access/xlog_internal.h
//!
//! Mixed: XLogPageHeaderData/XLogLongPageHeaderData are ON-DISK
//! (`#[repr(C)]` + layout asserts). XLP_* page flags are a bitflags set. The
//! XLogRecPtr/segment macros are `const fn`s. File-naming and WAL-insert
//! internals are `// TODO(wal)` stubs.

use bitflags::bitflags;

use crate::access::rmgr::RmgrId;
use crate::access::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::access::xlogreader::XLogReaderState;
use crate::c::MAXALIGN;
use crate::datatype::timestamp::TimestampTz;
use crate::pgtime::pg_time_t;
use crate::replication::decode::XLogRecordBuffer;
use crate::replication::logical::LogicalDecodingContext;
use crate::storage::block::BlockNumber;

/// XLOG page magic; can be used as a WAL version indicator.
pub const XLOG_PAGE_MAGIC: u16 = 0xD118;

/// On-disk XLOG page header.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XLogPageHeaderData {
    pub xlp_magic: u16,           // magic value for correctness checks
    pub xlp_info: u16,            // flag bits, see XlpFlags
    pub xlp_tli: TimeLineID,      // TimeLineID of first record on page
    pub xlp_pageaddr: XLogRecPtr, // XLOG address of this page
    // Number of bytes remaining from a previous page (tracks xl_tot_len).
    pub xlp_rem_len: u32,
}
const _: () = assert!(core::mem::size_of::<XLogPageHeaderData>() == 24);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, xlp_info) == 2);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, xlp_tli) == 4);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, xlp_pageaddr) == 8);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, xlp_rem_len) == 16);

pub const SizeOfXLogShortPHD: usize = MAXALIGN(core::mem::size_of::<XLogPageHeaderData>());

pub type XLogPageHeader<'a> = &'a mut XLogPageHeaderData;

/// On-disk long page header (XLP_LONG_HEADER set; first page of an XLOG file).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XLogLongPageHeaderData {
    pub std: XLogPageHeaderData, // standard header fields
    pub xlp_sysid: u64,          // system identifier from pg_control
    pub xlp_seg_size: u32,       // just as a cross-check
    pub xlp_xlog_blcksz: u32,    // just as a cross-check
}
const _: () = assert!(core::mem::size_of::<XLogLongPageHeaderData>() == 40);
const _: () = assert!(core::mem::offset_of!(XLogLongPageHeaderData, xlp_sysid) == 24);
const _: () = assert!(core::mem::offset_of!(XLogLongPageHeaderData, xlp_seg_size) == 32);
const _: () = assert!(core::mem::offset_of!(XLogLongPageHeaderData, xlp_xlog_blcksz) == 36);

pub const SizeOfXLogLongPHD: usize = MAXALIGN(core::mem::size_of::<XLogLongPageHeaderData>());

pub type XLogLongPageHeader<'a> = &'a mut XLogLongPageHeaderData;

bitflags! {
    /// xlp_info flag bits (GOOD; composite `ALL_FLAGS`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlpFlags: u16 {
        const FIRST_IS_CONTRECORD            = 0x0001;
        const LONG_HEADER                    = 0x0002;
        const BKP_REMOVABLE                  = 0x0004;
        const FIRST_IS_OVERWRITE_CONTRECORD  = 0x0008;
        // All defined flag bits (for validity checking of the header).
        const ALL_FLAGS = Self::FIRST_IS_CONTRECORD.bits()
            | Self::LONG_HEADER.bits()
            | Self::BKP_REMOVABLE.bits()
            | Self::FIRST_IS_OVERWRITE_CONTRECORD.bits();
    }
}

impl XLogPageHeaderData {
    /// Header size for this page (long vs short).
    pub fn page_header_size(&self) -> usize {
        if self.xlp_info & XlpFlags::LONG_HEADER.bits() != 0 {
            SizeOfXLogLongPHD
        } else {
            SizeOfXLogShortPHD
        }
    }
}

// wal_segment_size can range from 1MB to 1GB.
pub const WalSegMinSize: usize = 1024 * 1024;
pub const WalSegMaxSize: usize = 1024 * 1024 * 1024;
pub const DEFAULT_MIN_WAL_SEGS: i32 = 5;
pub const DEFAULT_MAX_WAL_SEGS: i32 = 64;

pub const fn IsPowerOf2(x: usize) -> bool {
    x > 0 && (x & (x - 1)) == 0
}
pub const fn IsValidWalSegSize(size: usize) -> bool {
    IsPowerOf2(size) && size >= WalSegMinSize && size <= WalSegMaxSize
}

pub const fn XLogSegmentsPerXLogId(wal_segsz_bytes: u64) -> u64 {
    0x1_0000_0000u64 / wal_segsz_bytes
}

pub const fn XLogSegNoOffsetToRecPtr(segno: u64, offset: u64, wal_segsz_bytes: u64) -> XLogRecPtr {
    XLogRecPtr(segno * wal_segsz_bytes + offset)
}

pub const fn XLogSegmentOffset(xlogptr: u64, wal_segsz_bytes: u64) -> u64 {
    xlogptr & (wal_segsz_bytes - 1)
}

// XLByteToSeg / XLByteToPrevSeg: compute a segment number from an XLogRecPtr.
pub const fn XLByteToSeg(xlrp: u64, wal_segsz_bytes: u64) -> u64 {
    xlrp / wal_segsz_bytes
}
pub const fn XLByteToPrevSeg(xlrp: u64, wal_segsz_bytes: u64) -> u64 {
    (xlrp - 1) / wal_segsz_bytes
}

// Convert a GUC measured in megabytes to an equivalent segment count.
pub const fn XLogMBVarToSegs(mbvar: u64, wal_segsz_bytes: u64) -> u64 {
    mbvar / (wal_segsz_bytes / (1024 * 1024))
}

pub const fn XLByteInSeg(xlrp: u64, log_seg_no: u64, wal_segsz_bytes: u64) -> bool {
    (xlrp / wal_segsz_bytes) == log_seg_no
}
pub const fn XLByteInPrevSeg(xlrp: u64, log_seg_no: u64, wal_segsz_bytes: u64) -> bool {
    ((xlrp - 1) / wal_segsz_bytes) == log_seg_no
}

// XLOG_BLCKSZ comes from pg_config; left to the segment impl. TODO(wal)
pub fn XRecOffIsValid(_xlrp: XLogRecPtr) -> bool {
    unimplemented!() // TODO(wal)
}

// The XLog directory and control file (relative to $PGDATA).
pub const XLOGDIR: &str = "pg_wal";
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";

pub const MAXFNAMELEN: usize = 64;
/// Length of an XLog file name.
pub const XLOG_FNAME_LEN: usize = 24;

// WAL file naming: the C inline helpers format/parse hex names. Stubbed; the
// real impl belongs with the segment manager. TODO(wal)
pub fn XLogFileName(_tli: TimeLineID, _log_seg_no: XLogSegNo, _wal_segsz_bytes: i32) -> String {
    unimplemented!() // TODO(wal)
}
pub fn XLogFileNameById(_tli: TimeLineID, _log: u32, _seg: u32) -> String {
    unimplemented!() // TODO(wal)
}
pub fn IsXLogFileName(_fname: &str) -> bool {
    unimplemented!() // TODO(wal)
}
pub fn IsPartialXLogFileName(_fname: &str) -> bool {
    unimplemented!() // TODO(wal)
}
// XLogFromFileName's tli/logSegNo out-params fold into the return tuple.
pub fn XLogFromFileName(_fname: &str, _wal_segsz_bytes: i32) -> (TimeLineID, XLogSegNo) {
    unimplemented!() // TODO(wal)
}
pub fn XLogFilePath(_tli: TimeLineID, _log_seg_no: XLogSegNo, _wal_segsz_bytes: i32) -> String {
    unimplemented!() // TODO(wal)
}
pub fn TLHistoryFileName(_tli: TimeLineID) -> String {
    unimplemented!() // TODO(wal)
}
pub fn IsTLHistoryFileName(_fname: &str) -> bool {
    unimplemented!() // TODO(wal)
}
pub fn TLHistoryFilePath(_tli: TimeLineID) -> String {
    unimplemented!() // TODO(wal)
}
pub fn StatusFilePath(_xlog: &str, _suffix: &str) -> String {
    unimplemented!() // TODO(wal)
}
pub fn BackupHistoryFileName(
    _tli: TimeLineID,
    _log_seg_no: XLogSegNo,
    _startpoint: XLogRecPtr,
    _wal_segsz_bytes: i32,
) -> String {
    unimplemented!() // TODO(wal)
}
pub fn IsBackupHistoryFileName(_fname: &str) -> bool {
    unimplemented!() // TODO(wal)
}
pub fn BackupHistoryFilePath(
    _tli: TimeLineID,
    _log_seg_no: XLogSegNo,
    _startpoint: XLogRecPtr,
    _wal_segsz_bytes: i32,
) -> String {
    unimplemented!() // TODO(wal)
}

/// Logged when a Hot-Standby-important parameter changes. (in-memory WAL payload)
pub struct xl_parameter_change {
    pub MaxConnections: i32,
    pub max_worker_processes: i32,
    pub max_wal_senders: i32,
    pub max_prepared_xacts: i32,
    pub max_locks_per_xact: i32,
    pub wal_level: i32,
    pub wal_log_hints: bool,
    pub track_commit_timestamp: bool,
}

/// Logs a restore point.
pub struct xl_restore_point {
    pub rp_time: TimestampTz,
    pub rp_name: [u8; MAXFNAMELEN],
}

/// Overwrite of a prior contrecord.
pub struct xl_overwrite_contrecord {
    pub overwritten_lsn: XLogRecPtr,
    pub overwrite_time: TimestampTz,
}

/// End-of-recovery mark (when not doing an END_OF_RECOVERY checkpoint).
pub struct xl_end_of_recovery {
    pub end_time: TimestampTz,
    pub ThisTimeLineID: TimeLineID, // new TLI
    pub PrevTimeLineID: TimeLineID, // previous TLI we forked off from
    pub wal_level: i32,
}

/// The xloginsert.c WAL-record data chain; an intrusive list -> `Vec` of slices.
pub struct XLogRecData<'a> {
    pub data: &'a [u8], // rmgr data to include
}

/// Recovery target action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTargetAction {
    Pause,
    Promote,
    Shutdown,
}

/// Method table for resource managers (routine struct -> trait, per
/// routine-struct.md appendix B; `rm_mask`/`rm_decode` are optional). `rm_name`
/// (a data field) becomes `NAME`; `rm_startup`/`rm_cleanup` default no-ops.
pub trait Rmgr {
    const NAME: &'static str;

    fn redo(record: &mut XLogReaderState);
    fn desc(buf: &mut Vec<u8>, record: &mut XLogReaderState);
    fn identify(info: u8) -> &'static str;

    fn startup() {}
    fn cleanup() {}

    // Optional: mask out non-deterministic bits for wal_consistency_checking.
    fn mask(_pagedata: &mut [u8], _blkno: BlockNumber) {}
    // Optional: logical decoding callback.
    fn decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {}
}

// RmgrTable[] global + the dispatch helpers -> deferred to a closed enum of the
// built-in rmgrs in Phase 2. TODO(wal)
pub fn RmgrStartup() {
    unimplemented!() // TODO(wal)
}
pub fn RmgrCleanup() {
    unimplemented!() // TODO(wal)
}
pub fn RmgrNotFound(_rmid: RmgrId) -> ! {
    unimplemented!() // TODO(wal)
}
// RegisterCustomRmgr / RmgrIdExists / GetRmgr depend on the RmgrTable; deferred.

/// xlog switching support; the `*lastSwitchLSN` out-param folds into the tuple.
pub fn GetLastSegSwitchData() -> (pg_time_t, XLogRecPtr) {
    unimplemented!() // TODO(wal)
}
pub fn RequestXLogSwitch(_mark_unimportant: bool) -> XLogRecPtr {
    unimplemented!() // TODO(wal)
}
// The `*oldrecptr`/`*oldtli` out-params fold into the return tuple.
pub fn GetOldestRestartPoint() -> (XLogRecPtr, TimeLineID) {
    unimplemented!() // TODO(wal)
}
// The `*fpi_len` out-param folds into the return.
pub fn XLogRecGetBlockRefInfo(
    _record: &mut XLogReaderState,
    _pretty: bool,
    _detailed_format: bool,
    _buf: &mut Vec<u8>,
) -> u32 {
    unimplemented!() // TODO(wal)
}

// Startup-process recovery flags/globals -> deferred (Phase 2 Session state).
pub static mut ArchiveRecoveryRequested: bool = false;
pub static mut InArchiveRecovery: bool = false;
pub static mut StandbyMode: bool = false;
pub static mut recoveryRestoreCommand: Option<String> = None;

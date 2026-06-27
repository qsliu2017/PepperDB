//! Translated from PostgreSQL src/include/access/xlog_internal.h
//!
//! Mixed: XLogPageHeaderData/XLogLongPageHeaderData are ON-DISK
//! (`#[repr(C)]` + layout asserts). XLP_* page flags are a bitflags set. The
//! XLogRecPtr/segment macros are `const fn`s. File-naming and WAL-insert
//! internals are `// TODO(wal)` stubs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

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
    pub magic: u16,           // magic value for correctness checks
    pub info: u16,            // flag bits, see XlpFlags
    pub tli: TimeLineID,      // TimeLineID of first record on page
    pub pageaddr: XLogRecPtr, // XLOG address of this page
    // Number of bytes remaining from a previous page (tracks tot_len).
    pub rem_len: u32,
}
const _: () = assert!(core::mem::size_of::<XLogPageHeaderData>() == 24);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, info) == 2);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, tli) == 4);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, pageaddr) == 8);
const _: () = assert!(core::mem::offset_of!(XLogPageHeaderData, rem_len) == 16);

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
    /// info flag bits (GOOD; composite `ALL_FLAGS`).
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
        if self.info & XlpFlags::LONG_HEADER.bits() != 0 {
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
    x > 0 && x.is_power_of_two()
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

// WAL file naming: the C inline helpers format/parse the 24-hex-char names. The
// name is "%08X%08X%08X" of (tli, logSegNo/segsPerId, logSegNo%segsPerId), where
// segsPerId = XLogSegmentsPerXLogId(wal_segsz_bytes); see xlog_internal.h.

/// PG `XLogFileName`: the 24-hex-char WAL segment file name.
pub fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segs_per_id = XLogSegmentsPerXLogId(wal_segsz_bytes as u64);
    XLogFileNameById(tli, (log_seg_no.0 / segs_per_id) as u32, (log_seg_no.0 % segs_per_id) as u32)
}

/// PG `XLogFileNameById`: the name from an explicit (log, seg) high/low split.
pub fn XLogFileNameById(tli: TimeLineID, log: u32, seg: u32) -> String {
    format!("{:08X}{:08X}{:08X}", tli.0, log, seg)
}

/// PG `IsXLogFileName`: exactly 24 hex characters.
pub fn IsXLogFileName(fname: &str) -> bool {
    fname.len() == XLOG_FNAME_LEN && fname.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase())
}

/// PG `IsPartialXLogFileName`: a 24-hex name plus the ".partial" suffix.
pub fn IsPartialXLogFileName(fname: &str) -> bool {
    fname.len() == XLOG_FNAME_LEN + ".partial".len()
        && fname.ends_with(".partial")
        && IsXLogFileName(&fname[..XLOG_FNAME_LEN])
}

/// PG `XLogFromFileName`: parse (tli, logSegNo) from a 24-hex name. The
/// tli/logSegNo out-params fold into the return tuple.
pub fn XLogFromFileName(fname: &str, wal_segsz_bytes: i32) -> (TimeLineID, XLogSegNo) {
    let tli = u32::from_str_radix(&fname[0..8], 16).unwrap();
    let log = u64::from_str_radix(&fname[8..16], 16).unwrap();
    let seg = u64::from_str_radix(&fname[16..24], 16).unwrap();
    let segs_per_id = XLogSegmentsPerXLogId(wal_segsz_bytes as u64);
    (TimeLineID(tli), XLogSegNo(log * segs_per_id + seg))
}

/// PG `XLogFilePath`: pg_wal/<name> (relative to $PGDATA).
pub fn XLogFilePath(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    format!("{}/{}", XLOGDIR, XLogFileName(tli, log_seg_no, wal_segsz_bytes))
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
    pub time: TimestampTz,
    pub name: [u8; MAXFNAMELEN],
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

// The resource-manager dispatch (PG's `RmgrData` struct-of-pointers table ->
// the `Rmgr` trait + a `match`-based `GetRmgr`) lives in the backend module
// `crate::backend::access::transam::rmgr`. Re-export the trait and the dispatch
// helpers so existing call sites that `use crate::access::xlog_internal::{Rmgr,
// GetRmgr, RmgrStartup, ...}` keep resolving. (The old `RmgrData`/`RmgrTable`
// fn-pointer surface is gone -- nothing referenced it -- replaced by the trait.)
pub use crate::backend::access::transam::rmgr::{
    GetRmgr, Rmgr, RmgrCleanup, RmgrIdExists, RmgrStartup,
};
#[allow(deprecated)]
pub use crate::backend::access::transam::rmgr::RmgrNotFound;

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

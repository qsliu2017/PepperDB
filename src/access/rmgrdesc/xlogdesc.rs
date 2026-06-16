//! Translation of postgres/src/backend/access/rmgrdesc/xlogdesc.c
//!                + the CheckPoint struct (catalog/pg_control.h) and the
//!                  xl_parameter_change / xl_restore_point /
//!                  xl_overwrite_contrecord / xl_end_of_recovery structs
//!                  (access/xlog_internal.h) it reads, plus the XLOG_* opcodes.
//!
//! rmgr descriptor routines for the XLOG resource manager (checkpoints, OID
//! advances, log switches, full-page images, parameter changes, etc.), used by
//! pg_waldump. xlog_desc inspects the record's info byte and renders a
//! human-readable summary of the WAL payload; xlog_identify maps an opcode to
//! its name string.
//!
//! Header mapping:
//!   lib/stringinfo.h        -> crate::lib::stringinfo (StringInfo,
//!                              appendStringInfo!, appendStringInfoString)
//!   access/transam.h        -> FullTransactionId, EpochFromFullTransactionId,
//!                              XidFromFullTransactionId, TransactionId
//!                              (crate::access::transam)
//!   access/xlogdefs.h       -> XLogRecPtr (uint64), TimeLineID (uint32),
//!                              MultiXactId/MultiXactOffset (uint32)
//!   catalog/pg_control.h    -> CheckPoint, the XLOG_* opcode values
//!   access/xlog_internal.h  -> xl_parameter_change, xl_restore_point,
//!                              xl_overwrite_contrecord, xl_end_of_recovery,
//!                              MAXFNAMELEN
//!   access/xlog.h           -> WalLevel enum, wal_level_options table
//!   datatype/timestamp.h    -> TimestampTz (int64), pg_time_t (int64)
//!
//! STUBS (access/xlogreader.h not ported):
//!   - XLogReaderState: opaque (`c_void`). TODO: replace with the real reader
//!     state struct once access/xlogreader.rs lands.
//!   - XLogRecGetData / XLogRecGetInfo / XLogRecGetRmid: stubbed to return
//!     null / 0 with a TODO. The desc body reads its record from the stubbed
//!     pointer, so it compiles and is runtime-stubbed (a real reader will feed
//!     it real bytes later).
//!   - timestamptz_to_str: stubbed to a fixed placeholder C-string (the real
//!     implementation lives in utils/adt/timestamp.c, not ported).
//!   - XLogRecGetBlockRefInfo: translated 1:1; uses the real xlogreader
//!     block-tag accessors (XLogRecGetBlockTagExtended / XLogRecGetBlock /
//!     XLogRecHasBlockImage / XLogRecBlockImageApply / XLogRecMaxBlockId).
//!
//! The CheckPoint / xl_* struct layouts, the XLOG_* opcode values, the
//! wal_level name table, and the xlog_identify name table are REAL (faithful to
//! pg_control.h / xlog_internal.h / xlog.h / xlogdesc.c). The desc output text
//! is reproduced exactly (Rust {} formatting; LSNs as "{:X}/{:X}").

use crate::appendStringInfo; // crate-root #[macro_export] macro
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::prelude::*;

use crate::access::transam::{
    EpochFromFullTransactionId, FullTransactionId, XidFromFullTransactionId,
};
use crate::access::transam::xlogreader::{
    BKPIMAGE_COMPRESSED, BKPIMAGE_COMPRESS_LZ4, BKPIMAGE_COMPRESS_PGLZ, BKPIMAGE_COMPRESS_ZSTD,
    RelFileLocator, XLogReaderState, XLogRecBlockImageApply, XLogRecGetBlock,
    XLogRecGetBlockTagExtended, XLogRecGetData, XLogRecGetInfo, XLogRecHasBlockImage,
    XLogRecMaxBlockId, XLR_INFO_MASK,
};
use crate::common::relpath::{ForkNumber, MAIN_FORKNUM};
use crate::pg_config::BLCKSZ;

/// access/block.h: typedef uint32 BlockNumber.
pub type BlockNumber = uint32;

/// The C `forkNames[]` table (relpath's `forkname` is module-private).
#[inline]
fn forkname(fork: ForkNumber) -> *const c_char {
    match fork {
        0 => c"main".as_ptr(),  // MAIN_FORKNUM
        1 => c"fsm".as_ptr(),   // FSM_FORKNUM
        2 => c"vm".as_ptr(),    // VISIBILITYMAP_FORKNUM
        3 => c"init".as_ptr(),  // INIT_FORKNUM
        _ => c"".as_ptr(),
    }
}

// ---------------------------------------------------------------------------
// Base types (from access/xlogdefs.h / datatype/timestamp.h / pgtime.h)
// ---------------------------------------------------------------------------

/// WAL record pointer (access/xlogdefs.h: typedef uint64 XLogRecPtr).
pub type XLogRecPtr = uint64;

/// Timeline identifier (access/xlogdefs.h: typedef uint32 TimeLineID).
pub type TimeLineID = uint32;

/// MultiXact id / offset (access/transam.h: typedef uint32 ...).
pub type MultiXactId = uint32;
pub type MultiXactOffset = uint32;

/// Transaction identifier (c.h: typedef uint32 TransactionId).
pub type TransactionId = uint32;

/// Timestamp with time zone (datatype/timestamp.h: typedef int64 TimestampTz).
pub type TimestampTz = int64;

/// Calendar time (pgtime.h: typedef int64 pg_time_t).
pub type pg_time_t = int64;

/// catalog name field width helper (access/xlog_internal.h: MAXFNAMELEN 64).
pub const MAXFNAMELEN: usize = 64;

// ---------------------------------------------------------------------------
// wal_level enum (access/xlog.h: typedef enum WalLevel)
// ---------------------------------------------------------------------------

pub const WAL_LEVEL_MINIMAL: c_int = 0;
pub const WAL_LEVEL_REPLICA: c_int = 1;
pub const WAL_LEVEL_LOGICAL: c_int = 2;

// ---------------------------------------------------------------------------
// XLOG info values for the XLOG rmgr (catalog/pg_control.h)
// High nibble = opcode; the low nibble (XLR_INFO_MASK) holds general flags.
// ---------------------------------------------------------------------------

pub const XLOG_CHECKPOINT_SHUTDOWN: uint8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: uint8 = 0x10;
pub const XLOG_NOOP: uint8 = 0x20;
pub const XLOG_NEXTOID: uint8 = 0x30;
pub const XLOG_SWITCH: uint8 = 0x40;
pub const XLOG_BACKUP_END: uint8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: uint8 = 0x60;
pub const XLOG_RESTORE_POINT: uint8 = 0x70;
pub const XLOG_FPW_CHANGE: uint8 = 0x80;
pub const XLOG_END_OF_RECOVERY: uint8 = 0x90;
pub const XLOG_FPI_FOR_HINT: uint8 = 0xA0;
pub const XLOG_FPI: uint8 = 0xB0;
/* 0xC0 is used in Postgres 9.5-11 */
pub const XLOG_OVERWRITE_CONTRECORD: uint8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: uint8 = 0xE0;

// ---------------------------------------------------------------------------
// CheckPoint: body of CheckPoint XLOG records (catalog/pg_control.h)
//
// Layout-sensitive: FullTransactionId (8 bytes) and pg_time_t (int64) force
// 8-byte alignment, so the natural C padding after `wal_level` (to align
// nextXid) and after `nextMultiOffset`/around `time` must be preserved by
// #[repr(C)] -- which it is, since field order matches the C struct exactly.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckPoint {
    /// next RecPtr available when we began to create CheckPoint (REDO start).
    pub redo: XLogRecPtr,
    /// current TLI.
    pub ThisTimeLineID: TimeLineID,
    /// previous TLI, if this record begins a new timeline.
    pub PrevTimeLineID: TimeLineID,
    /// current full_page_writes.
    pub fullPageWrites: bool,
    /// current wal_level.
    pub wal_level: c_int,
    /// next free transaction ID.
    pub nextXid: FullTransactionId,
    /// next free OID.
    pub nextOid: Oid,
    /// next free MultiXactId.
    pub nextMulti: MultiXactId,
    /// next free MultiXact offset.
    pub nextMultiOffset: MultiXactOffset,
    /// cluster-wide minimum datfrozenxid.
    pub oldestXid: TransactionId,
    /// database with minimum datfrozenxid.
    pub oldestXidDB: Oid,
    /// cluster-wide minimum datminmxid.
    pub oldestMulti: MultiXactId,
    /// database with minimum datminmxid.
    pub oldestMultiDB: Oid,
    /// time stamp of checkpoint.
    pub time: pg_time_t,
    /// oldest Xid with valid commit timestamp.
    pub oldestCommitTsXid: TransactionId,
    /// newest Xid with valid commit timestamp.
    pub newestCommitTsXid: TransactionId,
    /// oldest XID still running (online checkpoints under wal_level replica).
    pub oldestActiveXid: TransactionId,
}

// ---------------------------------------------------------------------------
// xl_* WAL record structs (access/xlog_internal.h)
// ---------------------------------------------------------------------------

/// Information logged when a Hot-Standby-relevant parameter changes.
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

/// Logs a restore point.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_restore_point {
    pub rp_time: TimestampTz,
    pub rp_name: [c_char; MAXFNAMELEN],
}

/// Overwrite of prior contrecord.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_overwrite_contrecord {
    pub overwritten_lsn: XLogRecPtr,
    pub overwrite_time: TimestampTz,
}

/// End-of-recovery mark, when we don't do an END_OF_RECOVERY checkpoint.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_end_of_recovery {
    pub end_time: TimestampTz,
    pub ThisTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,
    pub wal_level: c_int,
}

// ---------------------------------------------------------------------------
// wal_level name lookup (access/xlog.c: wal_level_options + get_wal_level_string)
// ---------------------------------------------------------------------------

/// One entry of the wal_level GUC enum table (utils/guc.h config_enum_entry):
/// (name, value, hidden).
struct config_enum_entry {
    name: *const c_char,
    val: c_int,
    #[allow(dead_code)]
    hidden: bool,
}

/// wal_level GUC options. The order matches xlogdesc.c so that
/// get_wal_level_string returns the first matching name ("replica" wins over the
/// deprecated "archive"/"hot_standby" aliases for WAL_LEVEL_REPLICA).
fn wal_level_options() -> [config_enum_entry; 5] {
    [
        config_enum_entry { name: c"minimal".as_ptr(), val: WAL_LEVEL_MINIMAL, hidden: false },
        config_enum_entry { name: c"replica".as_ptr(), val: WAL_LEVEL_REPLICA, hidden: false },
        config_enum_entry { name: c"archive".as_ptr(), val: WAL_LEVEL_REPLICA, hidden: true },
        config_enum_entry { name: c"hot_standby".as_ptr(), val: WAL_LEVEL_REPLICA, hidden: true },
        config_enum_entry { name: c"logical".as_ptr(), val: WAL_LEVEL_LOGICAL, hidden: false },
    ]
}

/// Find a string representation for wal_level.
fn get_wal_level_string(wal_level: c_int) -> *const c_char {
    let mut wal_level_str: *const c_char = c"?".as_ptr();
    for entry in wal_level_options().iter() {
        if entry.val == wal_level {
            wal_level_str = entry.name;
            break;
        }
    }
    wal_level_str
}

// ---------------------------------------------------------------------------
// timestamptz_to_str (STUB: utils/adt/timestamp.c not ported)
// ---------------------------------------------------------------------------

/// STUB: render a TimestampTz as a human-readable string. The real conversion
/// lives in utils/adt/timestamp.c (not ported). TODO: replace with the real
/// formatter. Returns a fixed placeholder so the desc body compiles.
#[inline]
fn timestamptz_to_str(_dt: TimestampTz) -> *const c_char {
    // TODO(pg-port): real timestamptz_to_str formats `dt` per DateStyle.
    c"(timestamp)".as_ptr()
}

/// LSN_FORMAT_ARGS(lsn): split a 64-bit LSN into (high uint32, low uint32) for
/// the "{:X}/{:X}" rendering used throughout WAL diagnostics.
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    (((lsn >> 32) & 0xFFFF_FFFF) as uint32, (lsn & 0xFFFF_FFFF) as uint32)
}

// ---------------------------------------------------------------------------
// xlog_desc / xlog_identify
// ---------------------------------------------------------------------------

/// Render a human-readable description of an XLOG-rmgr WAL record into `buf`
/// (a `StringInfo`, i.e. a `*mut StringInfoData`).
///
/// # Safety
/// `record` must be a valid `XLogReaderState` pointer once the reader lands;
/// the stubbed accessors currently yield a null data pointer.
pub unsafe fn xlog_desc(buf: StringInfo, record: *mut XLogReaderState) {
    let rec: *mut c_char = XLogRecGetData(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        let checkpoint = rec as *const CheckPoint;
        let (redo_hi, redo_lo) = LSN_FORMAT_ARGS((*checkpoint).redo);
        let kind = if info == XLOG_CHECKPOINT_SHUTDOWN {
            c"shutdown"
        } else {
            c"online"
        };
        appendStringInfo!(
            buf,
            "redo {:X}/{:X}; \
             tli {}; prev tli {}; fpw {}; wal_level {}; xid {}:{}; oid {}; multi {}; offset {}; \
             oldest xid {} in DB {}; oldest multi {} in DB {}; \
             oldest/newest commit timestamp xid: {}/{}; \
             oldest running xid {}; {}",
            redo_hi,
            redo_lo,
            (*checkpoint).ThisTimeLineID,
            (*checkpoint).PrevTimeLineID,
            cstr_bool_truefalse((*checkpoint).fullPageWrites),
            cstr(get_wal_level_string((*checkpoint).wal_level)),
            EpochFromFullTransactionId((*checkpoint).nextXid),
            XidFromFullTransactionId((*checkpoint).nextXid),
            (*checkpoint).nextOid,
            (*checkpoint).nextMulti,
            (*checkpoint).nextMultiOffset,
            (*checkpoint).oldestXid,
            (*checkpoint).oldestXidDB,
            (*checkpoint).oldestMulti,
            (*checkpoint).oldestMultiDB,
            (*checkpoint).oldestCommitTsXid,
            (*checkpoint).newestCommitTsXid,
            (*checkpoint).oldestActiveXid,
            cstr(kind.as_ptr())
        );
    } else if info == XLOG_NEXTOID {
        let mut nextOid: Oid = 0;
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut nextOid as *mut Oid as *mut u8,
            std::mem::size_of::<Oid>(),
        );
        appendStringInfo!(buf, "{}", nextOid);
    } else if info == XLOG_RESTORE_POINT {
        let xlrec = rec as *const xl_restore_point;
        appendStringInfoString(buf, (*xlrec).rp_name.as_ptr());
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        /* no further information to print */
    } else if info == XLOG_BACKUP_END {
        let mut startpoint: XLogRecPtr = 0;
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut startpoint as *mut XLogRecPtr as *mut u8,
            std::mem::size_of::<XLogRecPtr>(),
        );
        let (hi, lo) = LSN_FORMAT_ARGS(startpoint);
        appendStringInfo!(buf, "{:X}/{:X}", hi, lo);
    } else if info == XLOG_PARAMETER_CHANGE {
        let mut xlrec: xl_parameter_change = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut xlrec as *mut xl_parameter_change as *mut u8,
            std::mem::size_of::<xl_parameter_change>(),
        );
        let wal_level_str = get_wal_level_string(xlrec.wal_level);
        appendStringInfo!(
            buf,
            "max_connections={} max_worker_processes={} \
             max_wal_senders={} max_prepared_xacts={} \
             max_locks_per_xact={} wal_level={} \
             wal_log_hints={} track_commit_timestamp={}",
            xlrec.MaxConnections,
            xlrec.max_worker_processes,
            xlrec.max_wal_senders,
            xlrec.max_prepared_xacts,
            xlrec.max_locks_per_xact,
            cstr(wal_level_str),
            cstr_bool_onoff(xlrec.wal_log_hints),
            cstr_bool_onoff(xlrec.track_commit_timestamp)
        );
    } else if info == XLOG_FPW_CHANGE {
        let mut fpw: bool = false;
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut fpw as *mut bool as *mut u8,
            std::mem::size_of::<bool>(),
        );
        appendStringInfoString(buf, if fpw { c"true".as_ptr() } else { c"false".as_ptr() });
    } else if info == XLOG_END_OF_RECOVERY {
        let mut xlrec: xl_end_of_recovery = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut xlrec as *mut xl_end_of_recovery as *mut u8,
            std::mem::size_of::<xl_end_of_recovery>(),
        );
        appendStringInfo!(
            buf,
            "tli {}; prev tli {}; time {}; wal_level {}",
            xlrec.ThisTimeLineID,
            xlrec.PrevTimeLineID,
            cstr(timestamptz_to_str(xlrec.end_time)),
            cstr(get_wal_level_string(xlrec.wal_level))
        );
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        let mut xlrec: xl_overwrite_contrecord = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut xlrec as *mut xl_overwrite_contrecord as *mut u8,
            std::mem::size_of::<xl_overwrite_contrecord>(),
        );
        let (hi, lo) = LSN_FORMAT_ARGS(xlrec.overwritten_lsn);
        appendStringInfo!(
            buf,
            "lsn {:X}/{:X}; time {}",
            hi,
            lo,
            cstr(timestamptz_to_str(xlrec.overwrite_time))
        );
    } else if info == XLOG_CHECKPOINT_REDO {
        let mut wal_level: c_int = 0;
        std::ptr::copy_nonoverlapping(
            rec as *const u8,
            &mut wal_level as *mut c_int as *mut u8,
            std::mem::size_of::<c_int>(),
        );
        appendStringInfo!(buf, "wal_level {}", cstr(get_wal_level_string(wal_level)));
    }
}

/// Map an XLOG-rmgr info byte to its opcode name, or null if unknown.
pub fn xlog_identify(info: uint8) -> *const c_char {
    let mut id: *const c_char = null();

    match info & !XLR_INFO_MASK {
        XLOG_CHECKPOINT_SHUTDOWN => id = c"CHECKPOINT_SHUTDOWN".as_ptr(),
        XLOG_CHECKPOINT_ONLINE => id = c"CHECKPOINT_ONLINE".as_ptr(),
        XLOG_NOOP => id = c"NOOP".as_ptr(),
        XLOG_NEXTOID => id = c"NEXTOID".as_ptr(),
        XLOG_SWITCH => id = c"SWITCH".as_ptr(),
        XLOG_BACKUP_END => id = c"BACKUP_END".as_ptr(),
        XLOG_PARAMETER_CHANGE => id = c"PARAMETER_CHANGE".as_ptr(),
        XLOG_RESTORE_POINT => id = c"RESTORE_POINT".as_ptr(),
        XLOG_FPW_CHANGE => id = c"FPW_CHANGE".as_ptr(),
        XLOG_END_OF_RECOVERY => id = c"END_OF_RECOVERY".as_ptr(),
        XLOG_OVERWRITE_CONTRECORD => id = c"OVERWRITE_CONTRECORD".as_ptr(),
        XLOG_FPI => id = c"FPI".as_ptr(),
        XLOG_FPI_FOR_HINT => id = c"FPI_FOR_HINT".as_ptr(),
        XLOG_CHECKPOINT_REDO => id = c"CHECKPOINT_REDO".as_ptr(),
        _ => {}
    }

    id
}

/*
 * Returns a string giving information about all the blocks in an
 * XLogRecord.
 */
pub unsafe fn XLogRecGetBlockRefInfo(
    record: *mut XLogReaderState,
    pretty: bool,
    detailed_format: bool,
    buf: StringInfo,
    fpi_len: *mut uint32,
) {
    assert!(!record.is_null());

    if detailed_format && pretty {
        appendStringInfoChar(buf, b'\n' as c_char);
    }

    let mut block_id: c_int = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        let mut rlocator: RelFileLocator = std::mem::zeroed();
        let mut forknum: ForkNumber = 0;
        let mut blk: BlockNumber = 0;

        if !XLogRecGetBlockTagExtended(
            record,
            block_id as uint8,
            &mut rlocator,
            &mut forknum,
            &mut blk as *mut BlockNumber as *mut _,
            null_mut(),
        ) {
            block_id += 1;
            continue;
        }

        if detailed_format {
            /* Get block references in detailed format. */

            if pretty {
                appendStringInfoChar(buf, b'\t' as c_char);
            } else if block_id > 0 {
                appendStringInfoChar(buf, b' ' as c_char);
            }

            appendStringInfo!(
                buf,
                "blkref #{}: rel {}/{}/{} fork {} blk {}",
                block_id,
                rlocator.spcOid,
                rlocator.dbOid,
                rlocator.relNumber,
                cstr(forkname(forknum)),
                blk
            );

            if XLogRecHasBlockImage(record, block_id as uint8) {
                let bimg_info: uint8 = (*XLogRecGetBlock(record, block_id as uint8)).bimg_info;

                /* Calculate the amount of FPI data in the record. */
                if !fpi_len.is_null() {
                    *fpi_len += (*XLogRecGetBlock(record, block_id as uint8)).bimg_len as uint32;
                }

                if BKPIMAGE_COMPRESSED(bimg_info) {
                    let method: *const c_char = if (bimg_info & BKPIMAGE_COMPRESS_PGLZ) != 0 {
                        c"pglz".as_ptr()
                    } else if (bimg_info & BKPIMAGE_COMPRESS_LZ4) != 0 {
                        c"lz4".as_ptr()
                    } else if (bimg_info & BKPIMAGE_COMPRESS_ZSTD) != 0 {
                        c"zstd".as_ptr()
                    } else {
                        c"unknown".as_ptr()
                    };

                    appendStringInfo!(
                        buf,
                        " (FPW{}); hole: offset: {}, length: {}, \
                         compression saved: {}, method: {}",
                        if XLogRecBlockImageApply(record, block_id as uint8) {
                            ""
                        } else {
                            " for WAL verification"
                        },
                        (*XLogRecGetBlock(record, block_id as uint8)).hole_offset,
                        (*XLogRecGetBlock(record, block_id as uint8)).hole_length,
                        BLCKSZ as uint32
                            - (*XLogRecGetBlock(record, block_id as uint8)).hole_length as uint32
                            - (*XLogRecGetBlock(record, block_id as uint8)).bimg_len as uint32,
                        cstr(method)
                    );
                } else {
                    appendStringInfo!(
                        buf,
                        " (FPW{}); hole: offset: {}, length: {}",
                        if XLogRecBlockImageApply(record, block_id as uint8) {
                            ""
                        } else {
                            " for WAL verification"
                        },
                        (*XLogRecGetBlock(record, block_id as uint8)).hole_offset,
                        (*XLogRecGetBlock(record, block_id as uint8)).hole_length
                    );
                }
            }

            if pretty {
                appendStringInfoChar(buf, b'\n' as c_char);
            }
        } else {
            /* Get block references in short format. */

            if forknum != MAIN_FORKNUM {
                appendStringInfo!(
                    buf,
                    ", blkref #{}: rel {}/{}/{} fork {} blk {}",
                    block_id,
                    rlocator.spcOid,
                    rlocator.dbOid,
                    rlocator.relNumber,
                    cstr(forkname(forknum)),
                    blk
                );
            } else {
                appendStringInfo!(
                    buf,
                    ", blkref #{}: rel {}/{}/{} blk {}",
                    block_id,
                    rlocator.spcOid,
                    rlocator.dbOid,
                    rlocator.relNumber,
                    blk
                );
            }

            if XLogRecHasBlockImage(record, block_id as uint8) {
                /* Calculate the amount of FPI data in the record. */
                if !fpi_len.is_null() {
                    *fpi_len += (*XLogRecGetBlock(record, block_id as uint8)).bimg_len as uint32;
                }

                if XLogRecBlockImageApply(record, block_id as uint8) {
                    appendStringInfoString(buf, c" FPW".as_ptr());
                } else {
                    appendStringInfoString(buf, c" FPW for WAL verification".as_ptr());
                }
            }
        }

        block_id += 1;
    }

    if !detailed_format && pretty {
        appendStringInfoChar(buf, b'\n' as c_char);
    }
}

// ---------------------------------------------------------------------------
// Small formatting helpers
// ---------------------------------------------------------------------------

/// Render a `*const c_char` C-string as a Rust `&str` for {} formatting. The
/// desc strings here are all ASCII literals / lookup-table names.
#[inline]
fn cstr<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    // SAFETY: every `s` passed here is a 'static C string literal or a pointer
    // into a fixed-size NUL-terminated buffer.
    unsafe { std::ffi::CStr::from_ptr(s).to_str().unwrap_or("") }
}

/// C `b ? "true" : "false"` rendered for {} formatting.
#[inline]
fn cstr_bool_truefalse(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// C `b ? "on" : "off"` rendered for {} formatting.
#[inline]
fn cstr_bool_onoff(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[test]
    fn xlog_identify_table() {
        // Each opcode maps to its name; the low nibble (flags) is ignored.
        let cases: &[(uint8, &str)] = &[
            (XLOG_CHECKPOINT_SHUTDOWN, "CHECKPOINT_SHUTDOWN"),
            (XLOG_CHECKPOINT_ONLINE, "CHECKPOINT_ONLINE"),
            (XLOG_NOOP, "NOOP"),
            (XLOG_NEXTOID, "NEXTOID"),
            (XLOG_SWITCH, "SWITCH"),
            (XLOG_BACKUP_END, "BACKUP_END"),
            (XLOG_PARAMETER_CHANGE, "PARAMETER_CHANGE"),
            (XLOG_RESTORE_POINT, "RESTORE_POINT"),
            (XLOG_FPW_CHANGE, "FPW_CHANGE"),
            (XLOG_END_OF_RECOVERY, "END_OF_RECOVERY"),
            (XLOG_OVERWRITE_CONTRECORD, "OVERWRITE_CONTRECORD"),
            (XLOG_FPI, "FPI"),
            (XLOG_FPI_FOR_HINT, "FPI_FOR_HINT"),
            (XLOG_CHECKPOINT_REDO, "CHECKPOINT_REDO"),
        ];
        for &(opcode, name) in cases {
            // Plain opcode.
            assert_eq!(cstr(xlog_identify(opcode)), name);
            // Opcode with all flag bits set must still identify the same.
            assert_eq!(cstr(xlog_identify(opcode | 0x0F)), name);
        }
    }

    #[test]
    fn xlog_identify_unknown() {
        // 0xC0 is reserved/unused in modern PG -> null id.
        assert!(xlog_identify(0xC0).is_null());
        // An unknown high nibble.
        assert!(xlog_identify(0xF0).is_null());
    }

    #[test]
    fn checkpoint_field_offsets() {
        // Layout-sensitive: FullTransactionId (8B) and pg_time_t (int64) force
        // 8-byte alignment, inserting padding after wal_level (to align nextXid)
        // and aligning `time`. Verify the C field offsets exactly.
        assert_eq!(offset_of!(CheckPoint, redo), 0);
        assert_eq!(offset_of!(CheckPoint, ThisTimeLineID), 8);
        assert_eq!(offset_of!(CheckPoint, PrevTimeLineID), 12);
        assert_eq!(offset_of!(CheckPoint, fullPageWrites), 16);
        assert_eq!(offset_of!(CheckPoint, wal_level), 20);
        // 4 bytes of padding here to 8-align the FullTransactionId.
        assert_eq!(offset_of!(CheckPoint, nextXid), 24);
        assert_eq!(offset_of!(CheckPoint, nextOid), 32);
        assert_eq!(offset_of!(CheckPoint, nextMulti), 36);
        assert_eq!(offset_of!(CheckPoint, nextMultiOffset), 40);
        assert_eq!(offset_of!(CheckPoint, oldestXid), 44);
        assert_eq!(offset_of!(CheckPoint, oldestXidDB), 48);
        assert_eq!(offset_of!(CheckPoint, oldestMulti), 52);
        assert_eq!(offset_of!(CheckPoint, oldestMultiDB), 56);
        // 4 bytes of padding here to 8-align `time` (pg_time_t = int64).
        assert_eq!(offset_of!(CheckPoint, time), 64);
        assert_eq!(offset_of!(CheckPoint, oldestCommitTsXid), 72);
        assert_eq!(offset_of!(CheckPoint, newestCommitTsXid), 76);
        assert_eq!(offset_of!(CheckPoint, oldestActiveXid), 80);
        // Trailing pad to the 8-byte alignment of the struct.
        assert_eq!(std::mem::size_of::<CheckPoint>(), 88);
        assert_eq!(std::mem::align_of::<CheckPoint>(), 8);
    }

    #[test]
    fn xl_struct_layouts() {
        // xl_parameter_change: 6 ints + 2 bools, no 8-byte members -> 4-align.
        assert_eq!(offset_of!(xl_parameter_change, wal_level), 20);
        assert_eq!(offset_of!(xl_parameter_change, wal_log_hints), 24);
        assert_eq!(offset_of!(xl_parameter_change, track_commit_timestamp), 25);

        // xl_restore_point: TimestampTz (8B) then a 64-byte char array.
        assert_eq!(offset_of!(xl_restore_point, rp_time), 0);
        assert_eq!(offset_of!(xl_restore_point, rp_name), 8);
        assert_eq!(std::mem::size_of::<xl_restore_point>(), 8 + MAXFNAMELEN);

        // xl_overwrite_contrecord: two 8-byte members.
        assert_eq!(offset_of!(xl_overwrite_contrecord, overwritten_lsn), 0);
        assert_eq!(offset_of!(xl_overwrite_contrecord, overwrite_time), 8);

        // xl_end_of_recovery: TimestampTz (8B), then two TimeLineID + int.
        assert_eq!(offset_of!(xl_end_of_recovery, end_time), 0);
        assert_eq!(offset_of!(xl_end_of_recovery, ThisTimeLineID), 8);
        assert_eq!(offset_of!(xl_end_of_recovery, PrevTimeLineID), 12);
        assert_eq!(offset_of!(xl_end_of_recovery, wal_level), 16);
    }

    #[test]
    fn wal_level_string_lookup() {
        assert_eq!(cstr(get_wal_level_string(WAL_LEVEL_MINIMAL)), "minimal");
        // First match wins: "replica" over deprecated "archive"/"hot_standby".
        assert_eq!(cstr(get_wal_level_string(WAL_LEVEL_REPLICA)), "replica");
        assert_eq!(cstr(get_wal_level_string(WAL_LEVEL_LOGICAL)), "logical");
        // Unknown -> "?".
        assert_eq!(cstr(get_wal_level_string(99)), "?");
    }
}

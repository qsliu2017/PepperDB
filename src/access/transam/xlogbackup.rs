//! Translation of postgres/src/backend/access/transam/xlogbackup.c
//!            (+ struct BackupState merged from src/include/access/xlogbackup.h)
//!
//! Internal routines for base backups: `build_backup_content` formats the text
//! contents of a `backup_label` file (or a backup history file) from a
//! `BackupState`.
//!
//! Type aliases below mirror the C headers this unit depends on:
//!   xlogdefs.h  -> XLogRecPtr = uint64, TimeLineID = uint32
//!   pgtime.h    -> pg_time_t  = int64
//!   (path/xlog_internal.h) MAXPGPATH = 1024, MAXFNAMELEN = 64

use crate::appendStringInfo;
use crate::lib::stringinfo::{appendStringInfoString, makeStringInfo};
use crate::prelude::*;

// ===========================================================================
// Types pulled in from the headers (xlogdefs.h / pgtime.h / xlog_internal.h)
// ===========================================================================

/// xlogdefs.h: pointer into the WAL stream (byte position).
pub type XLogRecPtr = uint64;

/// xlogdefs.h: 0 is an invalid XLogRecPtr.
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

/// xlogdefs.h: timeline identifier.
pub type TimeLineID = uint32;

/// xlogdefs.h: segment number within the WAL stream.
pub type XLogSegNo = uint64;

/// pgtime.h: calendar time (int64).
pub type pg_time_t = int64;

/// Maximum length of a file-system path. (port-wide constant.)
pub const MAXPGPATH: usize = 1024;

/// Maximum length of a WAL file name. (access/xlog_internal.h)
pub const MAXFNAMELEN: usize = 64;

/// Default WAL segment size used when none is plumbed through here. PostgreSQL
/// carries this in the global `wal_segment_size` (initialized from a control
/// file); 16 MiB is the compiled-in default (DEFAULT_XLOG_SEG_SIZE).
pub const DEFAULT_XLOG_SEG_SIZE: usize = 16 * 1024 * 1024;

// ===========================================================================
// BackupState (xlogbackup.h)
// ===========================================================================

/// Structure to hold backup state, laid out exactly as the C `BackupState`.
#[repr(C)]
pub struct BackupState {
    // -- Fields saved at backup start --
    /// Backup label name; one extra byte for null-termination.
    pub name: [c_char; MAXPGPATH + 1],
    /// backup start WAL location.
    pub startpoint: XLogRecPtr,
    /// backup start TLI.
    pub starttli: TimeLineID,
    /// last checkpoint location.
    pub checkpointloc: XLogRecPtr,
    /// backup start time.
    pub starttime: pg_time_t,
    /// backup started in recovery?
    pub started_in_recovery: bool,
    /// incremental based on backup at this LSN.
    pub istartpoint: XLogRecPtr,
    /// incremental based on backup on this TLI.
    pub istarttli: TimeLineID,

    // -- Fields saved at the end of backup --
    /// backup stop WAL location.
    pub stoppoint: XLogRecPtr,
    /// backup stop TLI.
    pub stoptli: TimeLineID,
    /// backup stop time.
    pub stoptime: pg_time_t,
}

// ===========================================================================
// Small inline helpers from xlogdefs.h / xlog_internal.h
// ===========================================================================

/// `XLogRecPtrIsInvalid(r)` -- true when the pointer is the invalid sentinel.
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)` (xlog_internal.h).
#[inline]
fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> u64 {
    (0x100000000u64) / (wal_segsz_bytes as u64)
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` (xlog_internal.h):
/// the segment number containing `xlrp`. Returned rather than written through a
/// pointer.
#[inline]
fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: usize) -> XLogSegNo {
    xlrp / (wal_segsz_bytes as u64)
}

/// `XLogFileName(tli, logSegNo, wal_segsz_bytes)` (xlog_internal.h): the
/// canonical WAL file name `TTTTTTTTLLLLLLLLSSSSSSSS`. The C code writes into a
/// `char fname[MAXFNAMELEN]` stack buffer; here we return an owned String.
fn XLogFileName(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: usize) -> String {
    let per = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / per) as uint32,
        (log_seg_no % per) as uint32
    )
}

/// `LSN_FORMAT_ARGS(lsn)` -- split a 64-bit LSN into (high uint32, low uint32)
/// for the classic `%X/%X` rendering.
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    ((lsn >> 32) as uint32, lsn as uint32)
}

// ===========================================================================
// pgtime stubs (pg_strftime / pg_localtime are NOT yet ported)
// ===========================================================================

/// STUB for pgtime's `pg_strftime`/`pg_localtime` timestamp formatting.
///
/// The real PostgreSQL formats `state->starttime`/`stoptime` (a `pg_time_t`) in
/// the log timezone with the strftime spec `"%Y-%m-%d %H:%M:%S %Z"`. pgtime is
/// not ported yet, so we emit a fixed placeholder of the same shape. The LSN /
/// TLI / method / source-string formatting around it is the real, tested logic.
///
/// TODO: replace with a real pg_strftime(pg_localtime(&t, log_timezone), ...)
/// once src/timezone (pgtime) is ported.
fn pg_strftime_timestamp_stub(_t: pg_time_t) -> String {
    // TODO: pgtime not ported; fixed placeholder of the expected layout.
    String::from("1970-01-01 00:00:00 UTC")
}

// ===========================================================================
// build_backup_content
// ===========================================================================

/// Build contents for `backup_label` or a backup history file.
///
/// When `ishistoryfile` is true, it creates the contents for a backup history
/// file, otherwise the contents for a `backup_label` file.
///
/// Returns the result as a palloc'd string (`*mut c_char`), matching the C
/// signature `char *build_backup_content(BackupState *, bool)`.
///
/// # Safety
/// `state` must point to a valid `BackupState`. The returned pointer is owned by
/// the caller (the C code returns `result->data` after pfree-ing the
/// StringInfoData wrapper).
pub unsafe fn build_backup_content(state: *mut BackupState, ishistoryfile: bool) -> *mut c_char {
    // wal_segment_size is a process global in C; use the compiled-in default.
    let wal_segment_size: usize = DEFAULT_XLOG_SEG_SIZE;

    let result = makeStringInfo();

    Assert!(!state.is_null());

    // Use the log timezone here, not the session timezone.
    let startstrbuf = pg_strftime_timestamp_stub((*state).starttime);

    let startsegno: XLogSegNo = XLByteToSeg((*state).startpoint, wal_segment_size);
    let startxlogfile = XLogFileName((*state).starttli, startsegno, wal_segment_size);
    let (sp_hi, sp_lo) = LSN_FORMAT_ARGS((*state).startpoint);
    appendStringInfo!(
        result,
        "START WAL LOCATION: {:X}/{:X} (file {})\n",
        sp_hi,
        sp_lo,
        startxlogfile
    );

    if ishistoryfile {
        let stopsegno: XLogSegNo = XLByteToSeg((*state).stoppoint, wal_segment_size);
        let stopxlogfile = XLogFileName((*state).stoptli, stopsegno, wal_segment_size);
        let (stp_hi, stp_lo) = LSN_FORMAT_ARGS((*state).stoppoint);
        appendStringInfo!(
            result,
            "STOP WAL LOCATION: {:X}/{:X} (file {})\n",
            stp_hi,
            stp_lo,
            stopxlogfile
        );
    }

    let (cp_hi, cp_lo) = LSN_FORMAT_ARGS((*state).checkpointloc);
    appendStringInfo!(result, "CHECKPOINT LOCATION: {:X}/{:X}\n", cp_hi, cp_lo);
    appendStringInfoString(result, b"BACKUP METHOD: streamed\n\0".as_ptr() as *const c_char);
    appendStringInfo!(
        result,
        "BACKUP FROM: {}\n",
        if (*state).started_in_recovery {
            "standby"
        } else {
            "primary"
        }
    );
    appendStringInfo!(result, "START TIME: {}\n", startstrbuf);
    // state->name is a NUL-terminated C string in the fixed buffer.
    appendStringInfo!(result, "LABEL: {}\n", cstr_to_string((*state).name.as_ptr()));
    appendStringInfo!(result, "START TIMELINE: {}\n", (*state).starttli);

    if ishistoryfile {
        // Use the log timezone here, not the session timezone.
        let stopstrfbuf = pg_strftime_timestamp_stub((*state).stoptime);

        appendStringInfo!(result, "STOP TIME: {}\n", stopstrfbuf);
        appendStringInfo!(result, "STOP TIMELINE: {}\n", (*state).stoptli);
    }

    // Either both istartpoint and istarttli should be set, or neither.
    Assert!(XLogRecPtrIsInvalid((*state).istartpoint) == ((*state).istarttli == 0));
    if !XLogRecPtrIsInvalid((*state).istartpoint) {
        let (ist_hi, ist_lo) = LSN_FORMAT_ARGS((*state).istartpoint);
        appendStringInfo!(
            result,
            "INCREMENTAL FROM LSN: {:X}/{:X}\n",
            ist_hi,
            ist_lo
        );
        appendStringInfo!(result, "INCREMENTAL FROM TLI: {}\n", (*state).istarttli);
    }

    let data = (*result).data;
    pfree(result as *mut c_void);

    data
}

/// Read a NUL-terminated C string into an owned Rust String (lossy).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
unsafe fn cstr_to_string(s: *const c_char) -> String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read the palloc'd result back into a Rust String.
    unsafe fn result_to_string(p: *mut c_char) -> String {
        cstr_to_string(p)
    }

    /// Build a zeroed BackupState with a label, then set known fields.
    unsafe fn make_state(label: &[u8]) -> *mut BackupState {
        let st = palloc0(core::mem::size_of::<BackupState>()) as *mut BackupState;
        // copy label into name buffer (leaving the trailing NUL from palloc0)
        for (i, b) in label.iter().enumerate() {
            (*st).name[i] = *b as c_char;
        }
        st
    }

    #[test]
    fn backup_label_contains_wal_location_and_timeline() {
        unsafe {
            let st = make_state(b"pg_basebackup base backup");
            // startpoint 0x0000000A_0B0C0D0E, tli 7.
            (*st).startpoint = 0x0000000A_0B0C0D0Eu64;
            (*st).starttli = 7;
            (*st).checkpointloc = 0x00000000_12345678u64;
            (*st).started_in_recovery = false;
            // no incremental
            (*st).istartpoint = InvalidXLogRecPtr;
            (*st).istarttli = 0;

            let p = build_backup_content(st, false);
            let s = result_to_string(p);

            // %X/%X uppercase hex, no zero padding (LSN_FORMAT_ARGS).
            assert!(
                s.contains("START WAL LOCATION: A/B0C0D0E"),
                "missing/incorrect WAL location line in:\n{}",
                s
            );
            assert!(
                s.contains("START TIMELINE: 7"),
                "missing START TIMELINE line in:\n{}",
                s
            );
            assert!(
                s.contains("CHECKPOINT LOCATION: 0/12345678"),
                "missing CHECKPOINT LOCATION line in:\n{}",
                s
            );
            assert!(s.contains("BACKUP METHOD: streamed\n"));
            assert!(s.contains("BACKUP FROM: primary\n"));
            assert!(s.contains("LABEL: pg_basebackup base backup\n"));
            // No incremental lines when istartpoint is invalid.
            assert!(!s.contains("INCREMENTAL FROM LSN"));
            // backup_label (not history) has no STOP lines.
            assert!(!s.contains("STOP WAL LOCATION"));

            pfree(p as *mut c_void);
            pfree(st as *mut c_void);
        }
    }

    #[test]
    fn standby_source_and_incremental_lines() {
        unsafe {
            let st = make_state(b"lbl");
            (*st).startpoint = 0x00000001_00000000u64;
            (*st).starttli = 2;
            (*st).started_in_recovery = true;
            (*st).istartpoint = 0x00000003_DEADBEEFu64;
            (*st).istarttli = 1;

            let p = build_backup_content(st, false);
            let s = result_to_string(p);

            assert!(s.contains("BACKUP FROM: standby\n"), "in:\n{}", s);
            assert!(
                s.contains("INCREMENTAL FROM LSN: 3/DEADBEEF"),
                "in:\n{}",
                s
            );
            assert!(s.contains("INCREMENTAL FROM TLI: 1\n"), "in:\n{}", s);

            pfree(p as *mut c_void);
            pfree(st as *mut c_void);
        }
    }

    #[test]
    fn history_file_has_stop_lines() {
        unsafe {
            let st = make_state(b"hist");
            (*st).startpoint = 0x00000000_00000100u64;
            (*st).starttli = 1;
            (*st).stoppoint = 0x00000000_00000200u64;
            (*st).stoptli = 1;

            let p = build_backup_content(st, true);
            let s = result_to_string(p);

            assert!(s.contains("STOP WAL LOCATION: 0/200"), "in:\n{}", s);
            assert!(s.contains("STOP TIMELINE: 1\n"), "in:\n{}", s);

            pfree(p as *mut c_void);
            pfree(st as *mut c_void);
        }
    }
}

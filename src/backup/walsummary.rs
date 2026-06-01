//! walsummary.c - Functions for accessing and managing WAL summary data.

use crate::prelude::*;
// appendStringInfoVA (va_list variant) not portable; stub. TODO
unsafe fn appendStringInfoVA(_str: *mut crate::lib::stringinfo::StringInfoData, _fmt: *const std::ffi::c_char) -> std::ffi::c_int { 0 }

use crate::access::transam::xlog_internal::XLOGDIR;
use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, TimeLineID, XLogRecPtr, XLogRecPtrIsInvalid,
};
use crate::backup::walsummaryfuncs::{File, WalSummaryFile, WalSummaryIO};
use crate::common::int::pg_cmp_u64;
use crate::lib::stringinfo::{enlargeStringInfo, initStringInfo, StringInfoData};
use crate::nodes::pg_list::{lappend, lfirst, list_copy, list_sort, List, ListCell, NIL};
use crate::pg_config_manual::MAXPGPATH;
use crate::utils::palloc::palloc;
use crate::{current_cell, ereport, foreach};

// ---------------------------------------------------------------------------
// libc / system call externs used directly by this file.
// ---------------------------------------------------------------------------
unsafe extern "C" {
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn lstat(path: *const c_char, buf: *mut stat) -> c_int;
    fn __error() -> *mut c_int;
}

// errno access (macOS): *__error(). TODO: centralize once a port lands.
#[inline]
unsafe fn errno() -> c_int {
    *__error()
}

// <errno.h>
const ENOENT: c_int = 2;

// <fcntl.h>
const O_RDONLY: c_int = 0;

// time_t is not centrally defined yet (POSIX 64-bit on the platforms we target).
#[allow(non_camel_case_types)]
type time_t = i64;

// off_t mirror (storage/fd.h's File offsets); matches walsummaryfuncs::off_t.
#[allow(non_camel_case_types)]
type off_t = i64;

// Minimal struct stat: we only read st_mtime; lay out enough bytes to be safe.
// The directory walk and lstat trick mirror utils/misc/conffiles.rs. TODO: dedup
// once a shared, field-bearing stat/dirent definition exists.
#[allow(non_camel_case_types)]
#[repr(C)]
struct stat {
    _opaque: [u8; 256],
}

// st_mtime accessor via platform-specific offset into struct stat.
#[inline]
unsafe fn stat_st_mtime(sb: *const stat) -> time_t {
    // macOS: struct stat has st_mtimespec at offset 32 (tv_sec first).
    #[cfg(target_os = "macos")]
    let off: isize = 32;
    #[cfg(not(target_os = "macos"))]
    let off: isize = 88; // Linux x86_64 st_mtim.tv_sec
    *((sb as *const u8).offset(off) as *const time_t)
}

// ---------------------------------------------------------------------------
// struct dirent / DIR.
//
// Opaque types; d_name is read through a platform-specific offset, matching
// utils/misc/conffiles.rs. TODO: dedup once a field-bearing dirent exists.
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
#[repr(C)]
struct dirent {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct DIR {
    _private: [u8; 0],
}

#[inline]
unsafe fn dirent_d_name(de: *const dirent) -> *const c_char {
    #[cfg(target_os = "macos")]
    let off: isize = 21;
    #[cfg(not(target_os = "macos"))]
    let off: isize = 19;
    (de as *const u8).offset(off) as *const c_char
}

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees (storage/fd.h, utils/wait_event.h, elog.h).
// ---------------------------------------------------------------------------

// storage/fd.h: AllocateDir/ReadDir/FreeDir.  TODO: port storage/file/fd.c.
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!()
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!()
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!()
}

// storage/fd.h: PathNameOpenFile.  TODO: port storage/file/fd.c.
unsafe fn PathNameOpenFile(_path: *const c_char, _flags: c_int) -> File {
    unimplemented!()
}

// storage/fd.h: FileRead/FileWrite/FilePathName.  TODO: port storage/file/fd.c.
unsafe fn FileRead(
    _file: File,
    _data: *mut c_void,
    _amount: c_int,
    _offset: off_t,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!()
}
unsafe fn FileWrite(
    _file: File,
    _data: *const c_void,
    _amount: c_int,
    _offset: off_t,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!()
}
unsafe fn FilePathName(_file: File) -> *mut c_char {
    unimplemented!()
}

// utils/wait_event.h wait event identifiers.  TODO: port pgstat_wait_event.
const WAIT_EVENT_WAL_SUMMARY_READ: u32 = 0;
const WAIT_EVENT_WAL_SUMMARY_WRITE: u32 = 0;

// utils/elog.h: errcode_for_file_access().  TODO: port from elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

// errcodes.h classification (errcode() shim ignores the value).
const ERRCODE_DATA_CORRUPTED: c_int = 0;

/*
 * Get a list of WAL summaries.
 *
 * If tli != 0, only WAL summaries with the indicated TLI will be included.
 *
 * If start_lsn != InvalidXLogRecPtr, only summaries that end after the
 * indicated LSN will be included.
 *
 * If end_lsn != InvalidXLogRecPtr, only summaries that start before the
 * indicated LSN will be included.
 *
 * The intent is that you can call GetWalSummaries(tli, start_lsn, end_lsn)
 * to get all WAL summaries on the indicated timeline that overlap the
 * specified LSN range.
 */
pub unsafe fn GetWalSummaries(
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> *mut List {
    let sdir: *mut DIR;
    let mut dent: *mut dirent;
    let mut result: *mut List = NIL;

    let summaries_dir = c"pg_wal/summaries";
    debug_assert_eq!(XLOGDIR, "pg_wal");
    sdir = AllocateDir(summaries_dir.as_ptr());
    loop {
        dent = ReadDir(sdir, summaries_dir.as_ptr());
        if dent.is_null() {
            break;
        }

        let ws: *mut WalSummaryFile;
        let mut tmp: [u32; 5] = [0; 5];
        let file_tli: TimeLineID;
        let file_start_lsn: XLogRecPtr;
        let file_end_lsn: XLogRecPtr;

        let d_name = dirent_d_name(dent);

        /* Decode filename, or skip if it's not in the expected format. */
        if !IsWalSummaryFilename(d_name) {
            continue;
        }
        sscanf_5x08x(
            d_name,
            &mut tmp[0],
            &mut tmp[1],
            &mut tmp[2],
            &mut tmp[3],
            &mut tmp[4],
        );
        file_tli = tmp[0];
        file_start_lsn = ((tmp[1] as uint64) << 32) | tmp[2] as uint64;
        file_end_lsn = ((tmp[3] as uint64) << 32) | tmp[4] as uint64;

        /* Skip if it doesn't match the filter criteria. */
        if tli != 0 && tli != file_tli {
            continue;
        }
        if !XLogRecPtrIsInvalid(start_lsn) && start_lsn >= file_end_lsn {
            continue;
        }
        if !XLogRecPtrIsInvalid(end_lsn) && end_lsn <= file_start_lsn {
            continue;
        }

        /* Add it to the list. */
        ws = palloc(core::mem::size_of::<WalSummaryFile>()) as *mut WalSummaryFile;
        (*ws).tli = file_tli;
        (*ws).start_lsn = file_start_lsn;
        (*ws).end_lsn = file_end_lsn;
        result = lappend(result, ws as *mut c_void);
    }
    FreeDir(sdir);

    result
}

/*
 * Build a new list of WAL summaries based on an existing list, but filtering
 * out summaries that don't match the search parameters.
 *
 * If tli != 0, only WAL summaries with the indicated TLI will be included.
 *
 * If start_lsn != InvalidXLogRecPtr, only summaries that end after the
 * indicated LSN will be included.
 *
 * If end_lsn != InvalidXLogRecPtr, only summaries that start before the
 * indicated LSN will be included.
 */
pub unsafe fn FilterWalSummaries(
    wslist: *mut List,
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> *mut List {
    let mut result: *mut List = NIL;

    /* Loop over input. */
    foreach!(lc, wslist, {
        let ws: *mut WalSummaryFile = lfirst(current_cell!(lc)) as *mut WalSummaryFile;

        /* Skip if it doesn't match the filter criteria. */
        if tli != 0 && tli != (*ws).tli {
            continue;
        }
        if !XLogRecPtrIsInvalid(start_lsn) && start_lsn > (*ws).end_lsn {
            continue;
        }
        if !XLogRecPtrIsInvalid(end_lsn) && end_lsn < (*ws).start_lsn {
            continue;
        }

        /* Add it to the result list. */
        result = lappend(result, ws as *mut c_void);
    });

    result
}

/*
 * Check whether the supplied list of WalSummaryFile objects covers the
 * whole range of LSNs from start_lsn to end_lsn. This function ignores
 * timelines, so the caller should probably filter using the appropriate
 * timeline before calling this.
 *
 * If the whole range of LSNs is covered, returns true, otherwise false.
 * If false is returned, *missing_lsn is set either to InvalidXLogRecPtr
 * if there are no WAL summary files in the input list, or to the first LSN
 * in the range that is not covered by a WAL summary file in the input list.
 */
pub unsafe fn WalSummariesAreComplete(
    mut wslist: *mut List,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    missing_lsn: *mut XLogRecPtr,
) -> bool {
    let mut current_lsn: XLogRecPtr = start_lsn;

    /* Special case for empty list. */
    if wslist == NIL {
        *missing_lsn = InvalidXLogRecPtr;
        return false;
    }

    /* Make a private copy of the list and sort it by start LSN. */
    wslist = list_copy(wslist);
    list_sort(wslist, ListComparatorForWalSummaryFiles);

    /*
     * Consider summary files in order of increasing start_lsn, advancing the
     * known-summarized range from start_lsn toward end_lsn.
     *
     * Normally, the summary files should cover non-overlapping WAL ranges,
     * but this algorithm is intended to be correct even in case of overlap.
     */
    foreach!(lc, wslist, {
        let ws: *mut WalSummaryFile = lfirst(current_cell!(lc)) as *mut WalSummaryFile;

        if (*ws).start_lsn > current_lsn {
            /* We found a gap. */
            break;
        }
        if (*ws).end_lsn > current_lsn {
            /*
             * Next summary extends beyond end of previous summary, so extend
             * the end of the range known to be summarized.
             */
            current_lsn = (*ws).end_lsn;

            /*
             * If the range we know to be summarized has reached the required
             * end LSN, we have proved completeness.
             */
            if current_lsn >= end_lsn {
                return true;
            }
        }
    });

    /*
     * We either ran out of summary files without reaching the end LSN, or we
     * hit a gap in the sequence that resulted in us bailing out of the loop
     * above.
     */
    *missing_lsn = current_lsn;
    false
}

/*
 * Open a WAL summary file.
 *
 * This will throw an error in case of trouble. As an exception, if
 * missing_ok = true and the trouble is specifically that the file does
 * not exist, it will not throw an error and will return a value less than 0.
 */
pub unsafe fn OpenWalSummaryFile(ws: *mut WalSummaryFile, missing_ok: bool) -> File {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let file: File;

    let (sh, sl) = lsn_format_args((*ws).start_lsn);
    let (eh, el) = lsn_format_args((*ws).end_lsn);
    let formatted = format!(
        "pg_wal/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        (*ws).tli,
        sh,
        sl,
        eh,
        el,
    );
    copy_to_cstr_buf(&formatted, &mut path);

    file = PathNameOpenFile(path.as_ptr(), O_RDONLY);
    if file < 0 && (errno() != ENOENT || !missing_ok) {
        let _ = errcode_for_file_access();
        ereport!(ERROR, "could not open WAL summary file");
    }

    file
}

/*
 * Remove a WAL summary file if the last modification time precedes the
 * cutoff time.
 */
pub unsafe fn RemoveWalSummaryIfOlderThan(ws: *mut WalSummaryFile, cutoff_time: time_t) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut statbuf: stat = core::mem::zeroed();

    let (sh, sl) = lsn_format_args((*ws).start_lsn);
    let (eh, el) = lsn_format_args((*ws).end_lsn);
    let formatted = format!(
        "pg_wal/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        (*ws).tli,
        sh,
        sl,
        eh,
        el,
    );
    copy_to_cstr_buf(&formatted, &mut path);

    if lstat(path.as_ptr(), &mut statbuf) != 0 {
        if errno() == ENOENT {
            return;
        }
        let _ = errcode_for_file_access();
        ereport!(ERROR, "could not stat WAL summary file");
    }
    if stat_st_mtime(&statbuf) >= cutoff_time {
        return;
    }
    if unlink(path.as_ptr()) != 0 {
        let _ = errcode_for_file_access();
        ereport!(ERROR, "could not remove WAL summary file");
    }
    ereport!(DEBUG2, "removing WAL summary file");
}

/*
 * Test whether a filename looks like a WAL summary file.
 */
unsafe fn IsWalSummaryFilename(filename: *const c_char) -> bool {
    let accept = c"0123456789ABCDEF";
    strspn(filename, accept.as_ptr()) == 40
        && strcmp(filename.add(40), c".summary".as_ptr()) == 0
}

/*
 * Data read callback for use with CreateBlockRefTableReader.
 */
pub unsafe fn ReadWalSummary(wal_summary_io: *mut c_void, data: *mut c_void, length: c_int) -> c_int {
    let io: *mut WalSummaryIO = wal_summary_io as *mut WalSummaryIO;
    let nbytes: c_int;

    nbytes = FileRead(
        (*io).file,
        data,
        length,
        (*io).filepos,
        WAIT_EVENT_WAL_SUMMARY_READ,
    );
    if nbytes < 0 {
        let _ = errcode_for_file_access();
        let _ = FilePathName((*io).file);
        ereport!(ERROR, "could not read WAL summary file");
    }

    (*io).filepos += nbytes as off_t;
    nbytes
}

/*
 * Data write callback for use with WriteBlockRefTable.
 */
pub unsafe fn WriteWalSummary(
    wal_summary_io: *mut c_void,
    data: *mut c_void,
    length: c_int,
) -> c_int {
    let io: *mut WalSummaryIO = wal_summary_io as *mut WalSummaryIO;
    let nbytes: c_int;

    nbytes = FileWrite(
        (*io).file,
        data,
        length,
        (*io).filepos,
        WAIT_EVENT_WAL_SUMMARY_WRITE,
    );
    if nbytes < 0 {
        let _ = errcode_for_file_access();
        let _ = FilePathName((*io).file);
        ereport!(ERROR, "could not write WAL summary file");
    }
    if nbytes != length {
        let _ = errcode_for_file_access();
        let _ = FilePathName((*io).file);
        ereport!(ERROR, "could not write WAL summary file: short write");
    }

    (*io).filepos += nbytes as off_t;
    nbytes
}

/*
 * Error-reporting callback for use with CreateBlockRefTableReader.
 *
 * The C signature is variadic (char *fmt, ...). We accept the pre-formatted
 * message string directly, since the variadic formatting is performed by the
 * caller machinery in this port.
 */
pub unsafe fn ReportWalSummaryError(_callback_arg: *mut c_void, fmt: *const c_char) -> ! {
    let mut buf: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut buf);
    loop {
        let needed: c_int = appendStringInfoVA(&mut buf, fmt);
        if needed == 0 {
            break;
        }
        enlargeStringInfo(&mut buf, needed);
    }
    let _ = errcode(ERRCODE_DATA_CORRUPTED);
    ereport!(ERROR, "WAL summary error");
    unreachable!()
}

/*
 * Comparator to sort a List of WalSummaryFile objects by start_lsn.
 */
unsafe fn ListComparatorForWalSummaryFiles(a: *const ListCell, b: *const ListCell) -> c_int {
    let ws1: *mut WalSummaryFile = lfirst(a) as *mut WalSummaryFile;
    let ws2: *mut WalSummaryFile = lfirst(b) as *mut WalSummaryFile;

    pg_cmp_u64((*ws1).start_lsn, (*ws2).start_lsn)
}

// ---------------------------------------------------------------------------
// Small local helpers.
// ---------------------------------------------------------------------------

// LSN_FORMAT_ARGS(lsn) -> (high32, low32). Mirrors xlogdefs.h macro without
// importing it (local to keep the printf %08X%08X expansion explicit).
#[inline]
fn lsn_format_args(lsn: XLogRecPtr) -> (u32, u32) {
    (((lsn >> 32) & 0xFFFF_FFFF) as u32, (lsn & 0xFFFF_FFFF) as u32)
}

// Copy a Rust String into a fixed C char buffer as NUL-terminated bytes,
// truncating to fit (mirrors snprintf into a MAXPGPATH buffer).
#[inline]
unsafe fn copy_to_cstr_buf(s: &str, buf: &mut [c_char]) {
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), buf.len().saturating_sub(1));
    for i in 0..n {
        buf[i] = bytes[i] as c_char;
    }
    buf[n] = 0;
}

// sscanf(name, "%08X%08X%08X%08X%08X", ...). The filename is known to be 40 hex
// digits (validated by IsWalSummaryFilename), so parse five fixed-width fields.
#[inline]
unsafe fn sscanf_5x08x(
    name: *const c_char,
    a: *mut u32,
    b: *mut u32,
    c: *mut u32,
    d: *mut u32,
    e: *mut u32,
) {
    let outs = [a, b, c, d, e];
    for (k, out) in outs.into_iter().enumerate() {
        let mut v: u32 = 0;
        for j in 0..8 {
            let ch = *name.add(k * 8 + j) as u8;
            let digit = match ch {
                b'0'..=b'9' => (ch - b'0') as u32,
                b'A'..=b'F' => (ch - b'A' + 10) as u32,
                b'a'..=b'f' => (ch - b'a' + 10) as u32,
                _ => 0,
            };
            v = (v << 4) | digit;
        }
        *out = v;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_format_args_splits_high_low() {
        let lsn: XLogRecPtr = 0x0000_0001_DEAD_BEEF;
        assert_eq!(lsn_format_args(lsn), (0x0000_0001, 0xDEAD_BEEF));
    }

    #[test]
    fn sscanf_parses_five_hex_words() {
        // 40 hex chars: 00000001 DEADBEEF 00000002 CAFEBABE 00000003
        let name = c"00000001DEADBEEF00000002CAFEBABE00000003";
        let (mut a, mut b, mut c, mut d, mut e) = (0u32, 0u32, 0u32, 0u32, 0u32);
        unsafe {
            sscanf_5x08x(name.as_ptr(), &mut a, &mut b, &mut c, &mut d, &mut e);
        }
        assert_eq!(
            (a, b, c, d, e),
            (0x00000001, 0xDEADBEEF, 0x00000002, 0xCAFEBABE, 0x00000003)
        );
    }
}

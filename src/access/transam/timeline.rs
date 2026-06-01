//! src/backend/access/transam/timeline.c
//!
//! timeline.c
//!		Functions for reading and writing timeline history files.
//!
//! A timeline history file lists the timeline changes of the timeline, in
//! a simple text format. They are archived along with the WAL segments.
//!
//! The files are named like "<tli>.history". For example, if the database
//! starts up and switches to timeline 5, the timeline history file would be
//! called "00000005.history".
//!
//! Each line in the file represents a timeline switch:
//!
//! <parentTLI> <switchpoint> <reason>
//!
//!	parentTLI	ID of the parent timeline
//!	switchpoint XLogRecPtr of the WAL location where the switch happened
//!	reason		human-readable explanation of why the timeline was changed
//!
//! The fields are separated by tabs. Lines beginning with # are comments, and
//! are ignored. Empty lines are also ignored.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/transam/timeline.c

use crate::prelude::*;
use crate::{foreach, current_cell};

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint32;
use crate::nodes::pg_list::{List, ListCell};
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::pg_config_manual::MAXPGPATH;

// timeline.h
//
// A list of these structs describes the timeline history of the server. Each
// TimeLineHistoryEntry represents a piece of WAL belonging to the history,
// from newest to oldest. All WAL locations between 'begin' and 'end' belong to
// the timeline represented by the entry. Together the 'begin' and 'end'
// pointers of all the entries form a contiguous line from beginning of time
// to infinity.
#[repr(C)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr,           // inclusive
    pub end: XLogRecPtr,             // exclusive, InvalidXLogRecPtr means infinity
}

// from access/xlog_internal.h
const MAXFNAMELEN: usize = 64;

// from access/xlogdefs.h
const InvalidXLogRecPtr: XLogRecPtr = 0;

// from pg_config.h / xlog_internal.h
const BLCKSZ: usize = 8192;

// from access/xlogdefs.h: #define XLogRecPtrIsInvalid(r) ((r) == InvalidXLogRecPtr)
#[inline]
unsafe fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn getpid() -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn access(path: *const c_char, mode: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
    fn fgets(s: *mut c_char, n: c_int, stream: *mut c_void) -> *mut c_char;
    fn ferror(stream: *mut c_void) -> c_int;
    fn __error() -> *mut c_int; // errno location (darwin)
}

// errno helpers
#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// from errno.h
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;

// from fcntl.h
const O_RDONLY: c_int = 0x0000;
const O_RDWR: c_int = 0x0002;
const O_CREAT: c_int = 0x0200;
const O_EXCL: c_int = 0x0800;

// from unistd.h
const F_OK: c_int = 0;

/*
 * Copies all timeline history files with id's between 'begin' and 'end'
 * from archive to pg_wal.
 */
#[no_mangle]
pub unsafe extern "C" fn restoreTimeLineHistoryFiles(begin: TimeLineID, end: TimeLineID) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut histfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut tli: TimeLineID;

    tli = begin;
    while tli < end {
        if tli == 1 {
            tli += 1;
            continue;
        }

        TLHistoryFileName(histfname.as_mut_ptr(), tli);
        if RestoreArchivedFile(
            path.as_mut_ptr(),
            histfname.as_ptr(),
            c"RECOVERYHISTORY".as_ptr(),
            0,
            false,
        ) {
            KeepFileRestoredFromArchive(path.as_ptr(), histfname.as_ptr());
        }

        tli += 1;
    }
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component TLIs (the given TLI followed by
 * its ancestor TLIs).  If we can't find the history file, assume that the
 * timeline has no parents, and return a list of just the specified timeline
 * ID.
 */
#[no_mangle]
pub unsafe extern "C" fn readTimeLineHistory(targetTLI: TimeLineID) -> *mut List {
    let mut result: *mut List;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut histfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let fd: *mut c_void;
    let mut entry: *mut TimeLineHistoryEntry;
    let mut lasttli: TimeLineID = 0;
    let mut prevend: XLogRecPtr;
    let mut fromArchive: bool = false;

    /* Timeline 1 does not have a history file, so no need to check */
    if targetTLI == 1 {
        entry = palloc(size_of::<TimeLineHistoryEntry>()) as *mut TimeLineHistoryEntry;
        (*entry).tli = targetTLI;
        (*entry).begin = InvalidXLogRecPtr;
        (*entry).end = InvalidXLogRecPtr;
        return list_make1(entry as *mut c_void);
    }

    if ArchiveRecoveryRequested {
        TLHistoryFileName(histfname.as_mut_ptr(), targetTLI);
        fromArchive = RestoreArchivedFile(
            path.as_mut_ptr(),
            histfname.as_ptr(),
            c"RECOVERYHISTORY".as_ptr(),
            0,
            false,
        );
    } else {
        TLHistoryFilePath(path.as_mut_ptr(), targetTLI);
    }

    fd = AllocateFile(path.as_ptr(), c"r".as_ptr());
    if fd.is_null() {
        if get_errno() != ENOENT {
            ereport!(FATAL, "could not open file");
        }
        /* Not there, so assume no parents */
        entry = palloc(size_of::<TimeLineHistoryEntry>()) as *mut TimeLineHistoryEntry;
        (*entry).tli = targetTLI;
        (*entry).begin = InvalidXLogRecPtr;
        (*entry).end = InvalidXLogRecPtr;
        return list_make1(entry as *mut c_void);
    }

    result = NIL();

    /*
     * Parse the file...
     */
    prevend = InvalidXLogRecPtr;
    loop {
        let mut fline: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let res: *mut c_char;
        let mut ptr: *mut c_char;
        let mut tli: TimeLineID = 0;
        let mut switchpoint_hi: uint32 = 0;
        let mut switchpoint_lo: uint32 = 0;
        let nfields: c_int;

        pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_READ);
        res = fgets(fline.as_mut_ptr(), size_of_val(&fline) as c_int, fd);
        pgstat_report_wait_end();
        if res.is_null() {
            if ferror(fd) != 0 {
                ereport!(ERROR, "could not read file");
            }

            break;
        }

        /* skip leading whitespace and check for # comment */
        ptr = fline.as_mut_ptr();
        while *ptr != 0 {
            if isspace(*ptr as c_uchar as c_int) == 0 {
                break;
            }
            ptr = ptr.add(1);
        }
        if *ptr == b'\0' as c_char || *ptr == b'#' as c_char {
            continue;
        }

        nfields = sscanf(
            fline.as_ptr(),
            c"%u\t%X/%X".as_ptr(),
            &mut tli as *mut TimeLineID,
            &mut switchpoint_hi as *mut uint32,
            &mut switchpoint_lo as *mut uint32,
        );

        if nfields < 1 {
            /* expect a numeric timeline ID as first field of line */
            ereport!(FATAL, "syntax error in history file");
        }
        if nfields != 3 {
            ereport!(FATAL, "syntax error in history file");
        }

        if !result.is_null() && tli <= lasttli {
            ereport!(FATAL, "invalid data in history file");
        }

        lasttli = tli;

        entry = palloc(size_of::<TimeLineHistoryEntry>()) as *mut TimeLineHistoryEntry;
        (*entry).tli = tli;
        (*entry).begin = prevend;
        (*entry).end = ((switchpoint_hi as u64) << 32) | (switchpoint_lo as u64);
        prevend = (*entry).end;

        /* Build list with newest item first */
        result = lcons(entry as *mut c_void, result);

        /* we ignore the remainder of each line */
    }

    FreeFile(fd);

    if !result.is_null() && targetTLI <= lasttli {
        ereport!(FATAL, "invalid data in history file");
    }

    /*
     * Create one more entry for the "tip" of the timeline, which has no entry
     * in the history file.
     */
    entry = palloc(size_of::<TimeLineHistoryEntry>()) as *mut TimeLineHistoryEntry;
    (*entry).tli = targetTLI;
    (*entry).begin = prevend;
    (*entry).end = InvalidXLogRecPtr;

    result = lcons(entry as *mut c_void, result);

    /*
     * If the history file was fetched from archive, save it in pg_wal for
     * future reference.
     */
    if fromArchive {
        KeepFileRestoredFromArchive(path.as_ptr(), histfname.as_ptr());
    }

    result
}

/*
 * Probe whether a timeline history file exists for the given timeline ID
 */
#[no_mangle]
pub unsafe extern "C" fn existsTimeLineHistory(probeTLI: TimeLineID) -> bool {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut histfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let fd: *mut c_void;

    /* Timeline 1 does not have a history file, so no need to check */
    if probeTLI == 1 {
        return false;
    }

    if ArchiveRecoveryRequested {
        TLHistoryFileName(histfname.as_mut_ptr(), probeTLI);
        RestoreArchivedFile(
            path.as_mut_ptr(),
            histfname.as_ptr(),
            c"RECOVERYHISTORY".as_ptr(),
            0,
            false,
        );
    } else {
        TLHistoryFilePath(path.as_mut_ptr(), probeTLI);
    }

    fd = AllocateFile(path.as_ptr(), c"r".as_ptr());
    if !fd.is_null() {
        FreeFile(fd);
        true
    } else {
        if get_errno() != ENOENT {
            ereport!(FATAL, "could not open file");
        }
        false
    }
}

/*
 * Find the newest existing timeline, assuming that startTLI exists.
 *
 * Note: while this is somewhat heuristic, it does positively guarantee
 * that (result + 1) is not a known timeline, and therefore it should
 * be safe to assign that ID to a new timeline.
 */
#[no_mangle]
pub unsafe extern "C" fn findNewestTimeLine(startTLI: TimeLineID) -> TimeLineID {
    let mut newestTLI: TimeLineID;
    let mut probeTLI: TimeLineID;

    /*
     * The algorithm is just to probe for the existence of timeline history
     * files.  XXX is it useful to allow gaps in the sequence?
     */
    newestTLI = startTLI;

    probeTLI = startTLI + 1;
    loop {
        if existsTimeLineHistory(probeTLI) {
            newestTLI = probeTLI; /* probeTLI exists */
        } else {
            /* doesn't exist, assume we're done */
            break;
        }
        probeTLI += 1;
    }

    newestTLI
}

/*
 * Create a new timeline history file.
 *
 *	newTLI: ID of the new timeline
 *	parentTLI: ID of its immediate parent
 *	switchpoint: WAL location where the system switched to the new timeline
 *	reason: human-readable explanation of why the timeline was switched
 *
 * Currently this is only used at the end recovery, and so there are no locking
 * considerations.  But we should be just as tense as XLogFileInit to avoid
 * emplacing a bogus file.
 */
#[no_mangle]
pub unsafe extern "C" fn writeTimeLineHistory(
    newTLI: TimeLineID,
    parentTLI: TimeLineID,
    switchpoint: XLogRecPtr,
    reason: *mut c_char,
) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut tmppath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut histfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut buffer: [c_char; BLCKSZ] = [0; BLCKSZ];
    let srcfd: c_int;
    let fd: c_int;
    let mut nbytes: c_int;

    Assert!(newTLI > parentTLI); /* else bad selection of newTLI */

    /*
     * Write into a temp file name.
     */
    snprintf(
        tmppath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/xlogtemp.%d".as_ptr(),
        XLOGDIR.as_ptr(),
        getpid(),
    );

    unlink(tmppath.as_ptr());

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    fd = OpenTransientFile(tmppath.as_ptr(), O_RDWR | O_CREAT | O_EXCL);
    if fd < 0 {
        ereport!(ERROR, "could not create file");
    }

    /*
     * If a history file exists for the parent, copy it verbatim
     */
    if ArchiveRecoveryRequested {
        TLHistoryFileName(histfname.as_mut_ptr(), parentTLI);
        RestoreArchivedFile(
            path.as_mut_ptr(),
            histfname.as_ptr(),
            c"RECOVERYHISTORY".as_ptr(),
            0,
            false,
        );
    } else {
        TLHistoryFilePath(path.as_mut_ptr(), parentTLI);
    }

    srcfd = OpenTransientFile(path.as_ptr(), O_RDONLY);
    if srcfd < 0 {
        if get_errno() != ENOENT {
            ereport!(ERROR, "could not open file");
        }
        /* Not there, so assume parent has no parents */
    } else {
        loop {
            set_errno(0);
            pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_READ);
            nbytes = read(srcfd, buffer.as_mut_ptr() as *mut c_void, size_of_val(&buffer)) as c_int;
            pgstat_report_wait_end();
            if nbytes < 0 || get_errno() != 0 {
                ereport!(ERROR, "could not read file");
            }
            if nbytes == 0 {
                break;
            }
            set_errno(0);
            pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_WRITE);
            if write(fd, buffer.as_ptr() as *const c_void, nbytes as usize) as c_int != nbytes {
                let save_errno: c_int = get_errno();

                /*
                 * If we fail to make the file, delete it to release disk
                 * space
                 */
                unlink(tmppath.as_ptr());

                /*
                 * if write didn't set errno, assume problem is no disk space
                 */
                set_errno(if save_errno != 0 { save_errno } else { ENOSPC });

                ereport!(ERROR, "could not write to file");
            }
            pgstat_report_wait_end();
        }

        if CloseTransientFile(srcfd) != 0 {
            ereport!(ERROR, "could not close file");
        }
    }

    /*
     * Append one line with the details of this timeline split.
     *
     * If we did have a parent file, insert an extra newline just in case the
     * parent file failed to end with one.
     */
    snprintf(
        buffer.as_mut_ptr(),
        size_of_val(&buffer),
        c"%s%u\t%X/%X\t%s\n".as_ptr(),
        (if srcfd < 0 { c"".as_ptr() } else { c"\n".as_ptr() }),
        parentTLI,
        (switchpoint >> 32) as uint32,
        switchpoint as uint32,
        reason,
    );

    nbytes = strlen(buffer.as_ptr()) as c_int;
    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_WRITE);
    if write(fd, buffer.as_ptr() as *const c_void, nbytes as usize) as c_int != nbytes {
        let save_errno: c_int = get_errno();

        /*
         * If we fail to make the file, delete it to release disk space
         */
        unlink(tmppath.as_ptr());
        /* if write didn't set errno, assume problem is no disk space */
        set_errno(if save_errno != 0 { save_errno } else { ENOSPC });

        ereport!(ERROR, "could not write to file");
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(data_sync_elevel(ERROR), "could not fsync file");
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, "could not close file");
    }

    /*
     * Now move the completed history file into place with its final name.
     */
    TLHistoryFilePath(path.as_mut_ptr(), newTLI);
    Assert!(access(path.as_ptr(), F_OK) != 0 && get_errno() == ENOENT);
    durable_rename(tmppath.as_ptr(), path.as_ptr(), ERROR);

    /* The history file can be archived immediately. */
    if XLogArchivingActive() {
        TLHistoryFileName(histfname.as_mut_ptr(), newTLI);
        XLogArchiveNotify(histfname.as_ptr());
    }
}

/*
 * Writes a history file for given timeline and contents.
 *
 * Currently this is only used in the walreceiver process, and so there are
 * no locking considerations.  But we should be just as tense as XLogFileInit
 * to avoid emplacing a bogus file.
 */
#[no_mangle]
pub unsafe extern "C" fn writeTimeLineHistoryFile(
    tli: TimeLineID,
    content: *mut c_char,
    size: c_int,
) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut tmppath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;

    /*
     * Write into a temp file name.
     */
    snprintf(
        tmppath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/xlogtemp.%d".as_ptr(),
        XLOGDIR.as_ptr(),
        getpid(),
    );

    unlink(tmppath.as_ptr());

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    fd = OpenTransientFile(tmppath.as_ptr(), O_RDWR | O_CREAT | O_EXCL);
    if fd < 0 {
        ereport!(ERROR, "could not create file");
    }

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE);
    if write(fd, content as *const c_void, size as usize) as c_int != size {
        let save_errno: c_int = get_errno();

        /*
         * If we fail to make the file, delete it to release disk space
         */
        unlink(tmppath.as_ptr());
        /* if write didn't set errno, assume problem is no disk space */
        set_errno(if save_errno != 0 { save_errno } else { ENOSPC });

        ereport!(ERROR, "could not write to file");
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(data_sync_elevel(ERROR), "could not fsync file");
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, "could not close file");
    }

    /*
     * Now move the completed history file into place with its final name,
     * replacing any existing file with the same name.
     */
    TLHistoryFilePath(path.as_mut_ptr(), tli);
    durable_rename(tmppath.as_ptr(), path.as_ptr(), ERROR);
}

/*
 * Returns true if 'expectedTLEs' contains a timeline with id 'tli'
 */
#[no_mangle]
pub unsafe extern "C" fn tliInHistory(tli: TimeLineID, expectedTLEs: *mut List) -> bool {
    foreach!(cell, expectedTLEs, {
        if (*(lfirst(current_cell!(cell)) as *mut TimeLineHistoryEntry)).tli == tli {
            return true;
        }
    });

    false
}

/*
 * Returns the ID of the timeline in use at a particular point in time, in
 * the given timeline history.
 */
#[no_mangle]
pub unsafe extern "C" fn tliOfPointInHistory(ptr: XLogRecPtr, history: *mut List) -> TimeLineID {
    foreach!(cell, history, {
        let tle: *mut TimeLineHistoryEntry =
            lfirst(current_cell!(cell)) as *mut TimeLineHistoryEntry;

        if (XLogRecPtrIsInvalid((*tle).begin) || (*tle).begin <= ptr)
            && (XLogRecPtrIsInvalid((*tle).end) || ptr < (*tle).end)
        {
            /* found it */
            return (*tle).tli;
        }
    });

    /* shouldn't happen. */
    elog!(ERROR, "timeline history was not contiguous");
    0 /* keep compiler quiet */
}

/*
 * Returns the point in history where we branched off the given timeline,
 * and the timeline we branched to (*nextTLI). Returns InvalidXLogRecPtr if
 * the timeline is current, ie. we have not branched off from it, and throws
 * an error if the timeline is not part of this server's history.
 */
#[no_mangle]
pub unsafe extern "C" fn tliSwitchPoint(
    tli: TimeLineID,
    history: *mut List,
    nextTLI: *mut TimeLineID,
) -> XLogRecPtr {
    if !nextTLI.is_null() {
        *nextTLI = 0;
    }
    foreach!(cell, history, {
        let tle: *mut TimeLineHistoryEntry =
            lfirst(current_cell!(cell)) as *mut TimeLineHistoryEntry;

        if (*tle).tli == tli {
            return (*tle).end;
        }
        if !nextTLI.is_null() {
            *nextTLI = (*tle).tli;
        }
    });

    elog!(ERROR, "requested timeline {} is not in this server's history", tli);
    InvalidXLogRecPtr /* keep compiler quiet */
}

// ----------------------------------------------------------------
// Local stubs for unported helper functions / externs.
// ----------------------------------------------------------------

// access/xlog_internal.h
const XLOGDIR: &core::ffi::CStr = c"pg_wal";

// access/xlog.h
static mut ArchiveRecoveryRequested: bool = false;

// pgstat wait-event identifiers (pgstat.h)
const WAIT_EVENT_TIMELINE_HISTORY_READ: u32 = 0;
const WAIT_EVENT_TIMELINE_HISTORY_WRITE: u32 = 0;
const WAIT_EVENT_TIMELINE_HISTORY_SYNC: u32 = 0;
const WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE: u32 = 0;
const WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC: u32 = 0;

unsafe fn TLHistoryFileName(_fname: *mut c_char, _tli: TimeLineID) {
    unimplemented!() // TODO: access/xlog_internal.h
}

unsafe fn TLHistoryFilePath(_path: *mut c_char, _tli: TimeLineID) {
    unimplemented!() // TODO: access/xlog_internal.h
}

unsafe fn RestoreArchivedFile(
    _path: *mut c_char,
    _xlogfname: *const c_char,
    _recovername: *const c_char,
    _expectedSize: i64,
    _cleanupEnabled: bool,
) -> bool {
    unimplemented!() // TODO: access/xlogarchive.h
}

unsafe fn KeepFileRestoredFromArchive(_path: *const c_char, _xlogfname: *const c_char) {
    unimplemented!() // TODO: access/xlogarchive.h
}

unsafe fn XLogArchiveNotify(_xlog: *const c_char) {
    unimplemented!() // TODO: access/xlogarchive.h
}

unsafe fn XLogArchivingActive() -> bool {
    unimplemented!() // TODO: access/xlog.h
}

unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn FreeFile(_file: *mut c_void) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn OpenTransientFile(_fileName: *const c_char, _fileFlags: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn pg_fsync(_fd: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn durable_rename(_oldfile: *const c_char, _newfile: *const c_char, _elevel: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn data_sync_elevel(_elevel: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.h
}

unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    unimplemented!() // TODO: utils/wait_event.h
}

unsafe fn pgstat_report_wait_end() {
    unimplemented!() // TODO: utils/wait_event.h
}

unsafe fn list_make1(datum: *mut c_void) -> *mut List {
    let _ = datum;
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn lcons(datum: *mut c_void, list: *mut List) -> *mut List {
    let _ = (datum, list);
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn NIL() -> *mut List {
    core::ptr::null_mut() // nodes/pg_list.h: #define NIL ((List *) NULL)
}

unsafe fn lfirst(_cell: *mut ListCell) -> *mut c_void {
    unimplemented!() // TODO: nodes/pg_list.h
}

//! src/backend/access/transam/xlogarchive.c
//!
//! xlogarchive.c
//!     Functions for archiving WAL files and restoring from the archive.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint32;
use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::pg_config_manual::MAXPGPATH;

// ----- externally provided libc functions -----
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn unlink(path: *const c_char) -> c_int;
    fn system(command: *const c_char) -> c_int;
    fn fflush(stream: *mut c_void) -> c_int;
}

// XLOGDIR is "pg_wal"
const XLOGDIR: &[u8] = b"pg_wal\0";

// stat buffer stub; mirrors `struct stat` usage for st_size only.
#[repr(C)]
struct stat {
    st_size: i64,
}

// ----- local stubs for unported dependencies -----

unsafe fn stat(_path: *const c_char, _buf: *mut stat) -> c_int {
    unimplemented!() // TODO: <unistd/sys/stat.h>
}

// errno access stub
unsafe fn get_errno() -> c_int {
    unimplemented!() // TODO: errno
}

const ENOENT: c_int = 2;
const SIGTERM: c_int = 15;

// Log levels (from elog.h) - referenced by ereport/elog
const DEBUG1: c_int = 14; // value placeholder
const DEBUG2: c_int = 13;
const DEBUG3: c_int = 12;
const LOG: c_int = 15;
const WARNING: c_int = 19;
const FATAL: c_int = 21;
const ERROR_LEVEL: c_int = 21;

// recovery state enum value
const RECOVERY_STATE_ARCHIVE: c_int = 1;

// archive_mode values
const ARCHIVE_MODE_ALWAYS: c_int = 2;

// wait event info
const WAIT_EVENT_RESTORE_COMMAND: uint32 = 0;

// MAXFNAMELEN from xlog_internal.h
const MAXFNAMELEN: usize = 64;

// ----- externally referenced globals (stubs) -----

static mut ArchiveRecoveryRequested: bool = false;
static mut recoveryRestoreCommand: *mut c_char = std::ptr::null_mut();
static mut wal_segment_size: c_int = 0;
static mut StandbyMode: bool = false;
static mut XLogArchiveMode: c_int = 0;
static mut IsUnderPostmaster: bool = false;

// ----- unported helper fns (local stubs) -----

unsafe fn GetOldestRestartPoint(_redo: *mut XLogRecPtr, _tli: *mut TimeLineID) {
    unimplemented!() // TODO: xlog.c
}

unsafe fn XLByteToSeg(_ptr: XLogRecPtr, _segno: *mut XLogSegNo, _segsize: c_int) {
    unimplemented!() // TODO: xlog_internal.h
}

unsafe fn XLogFileName(
    _fname: *mut c_char,
    _tli: TimeLineID,
    _segno: XLogSegNo,
    _segsize: c_int,
) {
    unimplemented!() // TODO: xlog_internal.h
}

unsafe fn BuildRestoreCommand(
    _restoreCommand: *const c_char,
    _xlogpath: *const c_char,
    _xlogfname: *const c_char,
    _lastRestartPointFname: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO: common/archive.c
}

unsafe fn replace_percent_placeholders(
    _instr: *const c_char,
    _param_name: *const c_char,
    _letters: *const c_char,
    _r: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO: common/percentrepl.c
}

unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {
    unimplemented!() // TODO: pgstat.c
}

unsafe fn pgstat_report_wait_end() {
    unimplemented!() // TODO: pgstat.c
}

unsafe fn PreRestoreCommand() {
    unimplemented!() // TODO: postmaster/startup.c
}

unsafe fn PostRestoreCommand() {
    unimplemented!() // TODO: postmaster/startup.c
}

unsafe fn proc_exit(_code: c_int) -> ! {
    unimplemented!() // TODO: storage/ipc.c
}

unsafe fn wait_result_is_signal(_exit_status: c_int, _signum: c_int) -> bool {
    unimplemented!() // TODO: common/wait_error.c
}

unsafe fn wait_result_is_any_signal(_exit_status: c_int, _include_command_not_found: bool) -> bool {
    unimplemented!() // TODO: common/wait_error.c
}

unsafe fn wait_result_to_str(_exit_status: c_int) -> *mut c_char {
    unimplemented!() // TODO: common/wait_error.c
}

unsafe fn durable_rename(_oldfile: *const c_char, _newfile: *const c_char, _elevel: c_int) -> c_int {
    unimplemented!() // TODO: storage/fd.c
}

unsafe fn XLogArchivingActive() -> bool {
    unimplemented!() // TODO: access/xlog.h
}

unsafe fn XLogArchivingAlways() -> bool {
    unimplemented!() // TODO: access/xlog.h
}

unsafe fn GetRecoveryState() -> c_int {
    unimplemented!() // TODO: xlog.c
}

unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: storage/fd.c
}

unsafe fn FreeFile(_file: *mut c_void) -> c_int {
    unimplemented!() // TODO: storage/fd.c
}

unsafe fn StatusFilePath(_path: *mut c_char, _xlog: *const c_char, _suffix: *const c_char) {
    unimplemented!() // TODO: xlog_internal.h
}

unsafe fn IsTLHistoryFileName(_fname: *const c_char) -> bool {
    unimplemented!() // TODO: xlog_internal.h
}

unsafe fn PgArchForceDirScan() {
    unimplemented!() // TODO: postmaster/pgarch.c
}

unsafe fn PgArchWakeup() {
    unimplemented!() // TODO: postmaster/pgarch.c
}

unsafe fn WalSndRqstFileReload() {
    unimplemented!() // TODO: replication/walsender.c
}

unsafe fn WalSndWakeup(_physical: bool, _logical: bool) {
    unimplemented!() // TODO: replication/walsender.c
}

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, fill "path" with its complete path (note that this will be
 * a temp file name that doesn't follow the normal naming convention), and
 * return true.
 *
 * If not successful, fill "path" with the name of the normal on-line file
 * (which may or may not actually exist, but we'll try to use it), and return
 * false.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 *
 * When 'cleanupEnabled' is false, refrain from deleting any old WAL segments
 * in the archive. This is used when fetching the initial checkpoint record,
 * when we are not yet sure how far back we need the WAL.
 */
pub unsafe fn RestoreArchivedFile(
    path: *mut c_char,
    xlogfname: *const c_char,
    recovername: *const c_char,
    expectedSize: i64,
    cleanupEnabled: bool,
) -> bool {
    let mut xlogpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let xlogRestoreCmd: *mut c_char;
    let mut lastRestartPointFname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let rc: c_int;
    let mut stat_buf: stat = std::mem::zeroed();
    let mut restartSegNo: XLogSegNo = 0;
    let mut restartRedoPtr: XLogRecPtr = 0;
    let mut restartTli: TimeLineID = 0;

    'not_available: {
        /*
         * Ignore restore_command when not in archive recovery (meaning we are in
         * crash recovery).
         */
        if !ArchiveRecoveryRequested {
            break 'not_available;
        }

        /* In standby mode, restore_command might not be supplied */
        if recoveryRestoreCommand.is_null()
            || strcmp(recoveryRestoreCommand, c"".as_ptr()) == 0
        {
            break 'not_available;
        }

        /*
         * When doing archive recovery, we always prefer an archived log file even
         * if a file of the same name exists in XLOGDIR.  The reason is that the
         * file in XLOGDIR could be an old, un-filled or partly-filled version
         * that was copied and restored as part of backing up $PGDATA.
         *
         * We could try to optimize this slightly by checking the local copy
         * lastchange timestamp against the archived copy, but we have no API to
         * do this, nor can we guarantee that the lastchange timestamp was
         * preserved correctly when we copied to archive. Our aim is robustness,
         * so we elect not to do this.
         *
         * If we cannot obtain the log file from the archive, however, we will try
         * to use the XLOGDIR file if it exists.  This is so that we can make use
         * of log segments that weren't yet transferred to the archive.
         *
         * Notice that we don't actually overwrite any files when we copy back
         * from archive because the restore_command may inadvertently restore
         * inappropriate xlogs, or they may be corrupt, so we may wish to fallback
         * to the segments remaining in current XLOGDIR later. The
         * copy-from-archive filename is always the same, ensuring that we don't
         * run out of disk space on long recoveries.
         */
        snprintf(
            xlogpath.as_mut_ptr(),
            MAXPGPATH,
            c"%s/%s".as_ptr(),
            XLOGDIR.as_ptr() as *const c_char,
            recovername,
        );

        /*
         * Make sure there is no existing file named recovername.
         */
        if stat(xlogpath.as_ptr(), &mut stat_buf) != 0 {
            if get_errno() != ENOENT {
                ereport!(FATAL, "could not stat file");
                unreachable!();
            }
        } else {
            if unlink(xlogpath.as_ptr()) != 0 {
                ereport!(FATAL, "could not remove file");
                unreachable!();
            }
        }

        /*
         * Calculate the archive file cutoff point for use during log shipping
         * replication. All files earlier than this point can be deleted from the
         * archive, though there is no requirement to do so.
         *
         * If cleanup is not enabled, initialise this with the filename of
         * InvalidXLogRecPtr, which will prevent the deletion of any WAL files
         * from the archive because of the alphabetic sorting property of WAL
         * filenames.
         *
         * Once we have successfully located the redo pointer of the checkpoint
         * from which we start recovery we never request a file prior to the redo
         * pointer of the last restartpoint. When redo begins we know that we have
         * successfully located it, so there is no need for additional status
         * flags to signify the point when we can begin deleting WAL files from
         * the archive.
         */
        if cleanupEnabled {
            GetOldestRestartPoint(&mut restartRedoPtr, &mut restartTli);
            XLByteToSeg(restartRedoPtr, &mut restartSegNo, wal_segment_size);
            XLogFileName(
                lastRestartPointFname.as_mut_ptr(),
                restartTli,
                restartSegNo,
                wal_segment_size,
            );
            /* we shouldn't need anything earlier than last restart point */
            Assert!(strcmp(lastRestartPointFname.as_ptr(), xlogfname) <= 0);
        } else {
            XLogFileName(lastRestartPointFname.as_mut_ptr(), 0, 0, wal_segment_size);
        }

        /* Build the restore command to execute */
        xlogRestoreCmd = BuildRestoreCommand(
            recoveryRestoreCommand,
            xlogpath.as_ptr(),
            xlogfname,
            lastRestartPointFname.as_ptr(),
        );

        ereport!(DEBUG3, "executing restore command");

        fflush(std::ptr::null_mut());
        pgstat_report_wait_start(WAIT_EVENT_RESTORE_COMMAND);

        /*
         * PreRestoreCommand() informs the SIGTERM handler for the startup process
         * that it should proc_exit() right away.  This is done for the duration
         * of the system() call because there isn't a good way to break out while
         * it is executing.  Since we might call proc_exit() in a signal handler,
         * it is best to put any additional logic before or after the
         * PreRestoreCommand()/PostRestoreCommand() section.
         */
        PreRestoreCommand();

        /*
         * Copy xlog from archival storage to XLOGDIR
         */
        rc = system(xlogRestoreCmd);

        PostRestoreCommand();

        pgstat_report_wait_end();
        pfree(xlogRestoreCmd as *mut c_void);

        if rc == 0 {
            /*
             * command apparently succeeded, but let's make sure the file is
             * really there now and has the correct size.
             */
            if stat(xlogpath.as_ptr(), &mut stat_buf) == 0 {
                if expectedSize > 0 && stat_buf.st_size != expectedSize {
                    let elevel: c_int;

                    /*
                     * If we find a partial file in standby mode, we assume it's
                     * because it's just being copied to the archive, and keep
                     * trying.
                     *
                     * Otherwise treat a wrong-sized file as FATAL to ensure the
                     * DBA would notice it, but is that too strong? We could try
                     * to plow ahead with a local copy of the file ... but the
                     * problem is that there probably isn't one, and we'd
                     * incorrectly conclude we've reached the end of WAL and we're
                     * done recovering ...
                     */
                    if StandbyMode && stat_buf.st_size < expectedSize {
                        elevel = DEBUG1;
                    } else {
                        elevel = FATAL;
                    }
                    elog!(
                        elevel,
                        "archive file \"{}\" has wrong size: {} instead of {}",
                        "?",
                        stat_buf.st_size as i64,
                        expectedSize as i64
                    );
                    return false;
                } else {
                    elog!(LOG, "restored log file from archive");
                    strcpy(path, xlogpath.as_ptr());
                    return true;
                }
            } else {
                /* stat failed */
                let elevel: c_int = if get_errno() == ENOENT { LOG } else { FATAL };

                elog!(elevel, "could not stat file");
            }
        }

        /*
         * Remember, we rollforward UNTIL the restore fails so failure here is
         * just part of the process... that makes it difficult to determine
         * whether the restore failed because there isn't an archive to restore,
         * or because the administrator has specified the restore program
         * incorrectly.  We have to assume the former.
         *
         * However, if the failure was due to any sort of signal, it's best to
         * punt and abort recovery.  (If we "return false" here, upper levels will
         * assume that recovery is complete and start up the database!) It's
         * essential to abort on child SIGINT and SIGQUIT, because per spec
         * system() ignores SIGINT and SIGQUIT while waiting; if we see one of
         * those it's a good bet we should have gotten it too.
         *
         * On SIGTERM, assume we have received a fast shutdown request, and exit
         * cleanly. It's pure chance whether we receive the SIGTERM first, or the
         * child process. If we receive it first, the signal handler will call
         * proc_exit, otherwise we do it here. If we or the child process received
         * SIGTERM for any other reason than a fast shutdown request, postmaster
         * will perform an immediate shutdown when it sees us exiting
         * unexpectedly.
         *
         * We treat hard shell errors such as "command not found" as fatal, too.
         */
        if wait_result_is_signal(rc, SIGTERM) {
            proc_exit(1);
        }

        elog!(
            if wait_result_is_any_signal(rc, true) {
                FATAL
            } else {
                DEBUG2
            },
            "could not restore file from archive"
        );
    } // not_available:

    /*
     * if an archived file is not available, there might still be a version of
     * this file in XLOGDIR, so return that as the filename to open.
     *
     * In many recovery scenarios we expect this to fail also, but if so that
     * just means we've reached the end of WAL.
     */
    snprintf(
        path,
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        XLOGDIR.as_ptr() as *const c_char,
        xlogfname,
    );
    false
}

/*
 * Attempt to execute an external shell command during recovery.
 *
 * 'command' is the shell command to be executed, 'commandName' is a
 * human-readable name describing the command emitted in the logs. If
 * 'failOnSignal' is true and the command is killed by a signal, a FATAL
 * error is thrown. Otherwise a WARNING is emitted.
 *
 * This is currently used for recovery_end_command and archive_cleanup_command.
 */
pub unsafe fn ExecuteRecoveryCommand(
    command: *const c_char,
    commandName: *const c_char,
    failOnSignal: bool,
    wait_event_info: uint32,
) {
    let xlogRecoveryCmd: *mut c_char;
    let mut lastRestartPointFname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let rc: c_int;
    let mut restartSegNo: XLogSegNo = 0;
    let mut restartRedoPtr: XLogRecPtr = 0;
    let mut restartTli: TimeLineID = 0;

    Assert!(!command.is_null() && !commandName.is_null());

    /*
     * Calculate the archive file cutoff point for use during log shipping
     * replication. All files earlier than this point can be deleted from the
     * archive, though there is no requirement to do so.
     */
    GetOldestRestartPoint(&mut restartRedoPtr, &mut restartTli);
    XLByteToSeg(restartRedoPtr, &mut restartSegNo, wal_segment_size);
    XLogFileName(
        lastRestartPointFname.as_mut_ptr(),
        restartTli,
        restartSegNo,
        wal_segment_size,
    );

    /*
     * construct the command to be executed
     */
    xlogRecoveryCmd = replace_percent_placeholders(
        command,
        commandName,
        c"r".as_ptr(),
        lastRestartPointFname.as_ptr(),
    );

    ereport!(DEBUG3, "executing recovery command");

    /*
     * execute the constructed command
     */
    fflush(std::ptr::null_mut());
    pgstat_report_wait_start(wait_event_info);
    rc = system(xlogRecoveryCmd);
    pgstat_report_wait_end();

    pfree(xlogRecoveryCmd as *mut c_void);

    if rc != 0 {
        /*
         * If the failure was due to any sort of signal, it's best to punt and
         * abort recovery.  See comments in RestoreArchivedFile().
         */
        elog!(
            if failOnSignal && wait_result_is_any_signal(rc, true) {
                FATAL
            } else {
                WARNING
            },
            /*------
               translator: First %s represents a postgresql.conf parameter name like
              "recovery_end_command", the 2nd is the value of that parameter, the
              third an already translated error message. */
            "recovery command failed"
        );
    }
}

/*
 * A file was restored from the archive under a temporary filename (path),
 * and now we want to keep it. Rename it under the permanent filename in
 * pg_wal (xlogfname), replacing any existing file with the same name.
 */
pub unsafe fn KeepFileRestoredFromArchive(path: *const c_char, xlogfname: *const c_char) {
    let mut xlogfpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut reload: bool = false;
    let mut statbuf: stat = std::mem::zeroed();

    snprintf(
        xlogfpath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        XLOGDIR.as_ptr() as *const c_char,
        xlogfname,
    );

    if stat(xlogfpath.as_ptr(), &mut statbuf) == 0 {
        let mut oldpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        /* same-size buffers, so this never truncates */
        strlcpy(oldpath.as_mut_ptr(), xlogfpath.as_ptr(), MAXPGPATH);

        if unlink(oldpath.as_ptr()) != 0 {
            ereport!(FATAL, "could not remove file");
            unreachable!();
        }
        reload = true;
    }

    durable_rename(path, xlogfpath.as_ptr(), ERROR_LEVEL);

    /*
     * Create .done file forcibly to prevent the restored segment from being
     * archived again later.
     */
    if XLogArchiveMode != ARCHIVE_MODE_ALWAYS {
        XLogArchiveForceDone(xlogfname);
    } else {
        XLogArchiveNotify(xlogfname);
    }

    /*
     * If the existing file was replaced, since walsenders might have it open,
     * request them to reload a currently-open segment. This is only required
     * for WAL segments, walsenders don't hold other files open, but there's
     * no harm in doing this too often, and we don't know what kind of a file
     * we're dealing with here.
     */
    if reload {
        WalSndRqstFileReload();
    }

    /*
     * Signal walsender that new WAL has arrived. Again, this isn't necessary
     * if we restored something other than a WAL segment, but it does no harm
     * either.
     */
    WalSndWakeup(true, false);
}

/*
 * XLogArchiveNotify
 *
 * Create an archive notification file
 *
 * The name of the notification file is the message that will be picked up
 * by the archiver, e.g. we write 0000000100000001000000C6.ready
 * and the archiver then knows to archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
pub unsafe fn XLogArchiveNotify(xlog: *const c_char) {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: *mut c_void;

    /* insert an otherwise empty file called <XLOG>.ready */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    fd = AllocateFile(archiveStatusPath.as_ptr(), c"w".as_ptr());
    if fd.is_null() {
        elog!(LOG, "could not create archive status file");
        return;
    }
    if FreeFile(fd) != 0 {
        elog!(LOG, "could not write archive status file");
        return;
    }

    /*
     * Timeline history files are given the highest archival priority to lower
     * the chance that a promoted standby will choose a timeline that is
     * already in use.  However, the archiver ordinarily tries to gather
     * multiple files to archive from each scan of the archive_status
     * directory, which means that newly created timeline history files could
     * be left unarchived for a while.  To ensure that the archiver picks up
     * timeline history files as soon as possible, we force the archiver to
     * scan the archive_status directory the next time it looks for a file to
     * archive.
     */
    if IsTLHistoryFileName(xlog) {
        PgArchForceDirScan();
    }

    /* Notify archiver that it's got something to do */
    if IsUnderPostmaster {
        PgArchWakeup();
    }
}

/*
 * Convenience routine to notify using segment number representation of filename
 */
pub unsafe fn XLogArchiveNotifySeg(segno: XLogSegNo, tli: TimeLineID) {
    let mut xlog: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];

    Assert!(tli != 0);

    XLogFileName(xlog.as_mut_ptr(), tli, segno, wal_segment_size);
    XLogArchiveNotify(xlog.as_ptr());
}

/*
 * XLogArchiveForceDone
 *
 * Emit notification forcibly that an XLOG segment file has been successfully
 * archived, by creating <XLOG>.done regardless of whether <XLOG>.ready
 * exists or not.
 */
pub unsafe fn XLogArchiveForceDone(xlog: *const c_char) {
    let mut archiveReady: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut archiveDone: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut stat_buf: stat = std::mem::zeroed();
    let fd: *mut c_void;

    /* Exit if already known done */
    StatusFilePath(archiveDone.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveDone.as_ptr(), &mut stat_buf) == 0 {
        return;
    }

    /* If .ready exists, rename it to .done */
    StatusFilePath(archiveReady.as_mut_ptr(), xlog, c".ready".as_ptr());
    if stat(archiveReady.as_ptr(), &mut stat_buf) == 0 {
        durable_rename(archiveReady.as_ptr(), archiveDone.as_ptr(), WARNING);
        return;
    }

    /* insert an otherwise empty file called <XLOG>.done */
    fd = AllocateFile(archiveDone.as_ptr(), c"w".as_ptr());
    if fd.is_null() {
        elog!(LOG, "could not create archive status file");
        return;
    }
    if FreeFile(fd) != 0 {
        elog!(LOG, "could not write archive status file");
        return;
    }
}

/*
 * XLogArchiveCheckDone
 *
 * This is called when we are ready to delete or recycle an old XLOG segment
 * file or backup history file.  If it is okay to delete it then return true.
 * If it is not time to delete it, make sure a .ready file exists, and return
 * false.
 *
 * If <XLOG>.done exists, then return true; else if <XLOG>.ready exists,
 * then return false; else create <XLOG>.ready and return false.
 *
 * The reason we do things this way is so that if the original attempt to
 * create <XLOG>.ready fails, we'll retry during subsequent checkpoints.
 */
pub unsafe fn XLogArchiveCheckDone(xlog: *const c_char) -> bool {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut stat_buf: stat = std::mem::zeroed();

    /* The file is always deletable if archive_mode is "off". */
    if !XLogArchivingActive() {
        return true;
    }

    /*
     * During archive recovery, the file is deletable if archive_mode is not
     * "always".
     */
    if !XLogArchivingAlways() && GetRecoveryState() == RECOVERY_STATE_ARCHIVE {
        return true;
    }

    /*
     * At this point of the logic, note that we are either a primary with
     * archive_mode set to "on" or "always", or a standby with archive_mode
     * set to "always".
     */

    /* First check for .done --- this means archiver is done with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    /* check for .ready --- this means archiver is still busy with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return false;
    }

    /* Race condition --- maybe archiver just finished, so recheck */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    /* Retry creation of the .ready file */
    XLogArchiveNotify(xlog);
    false
}

/*
 * XLogArchiveIsBusy
 *
 * Check to see if an XLOG segment file is still unarchived.
 * This is almost but not quite the inverse of XLogArchiveCheckDone: in
 * the first place we aren't chartered to recreate the .ready file, and
 * in the second place we should consider that if the file is already gone
 * then it's not busy.  (This check is needed to handle the race condition
 * that a checkpoint already deleted the no-longer-needed file.)
 */
pub unsafe fn XLogArchiveIsBusy(xlog: *const c_char) -> bool {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut stat_buf: stat = std::mem::zeroed();

    /* First check for .done --- this means archiver is done with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return false;
    }

    /* check for .ready --- this means archiver is still busy with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    /* Race condition --- maybe archiver just finished, so recheck */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return false;
    }

    /*
     * Check to see if the WAL file has been removed by checkpoint, which
     * implies it has already been archived, and explains why we can't see a
     * status file for it.
     */
    snprintf(
        archiveStatusPath.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        XLOGDIR.as_ptr() as *const c_char,
        xlog,
    );
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) != 0 && get_errno() == ENOENT {
        return false;
    }

    true
}

/*
 * XLogArchiveIsReadyOrDone
 *
 * Check to see if an XLOG segment file has a .ready or .done file.
 * This is similar to XLogArchiveIsBusy(), but returns true if the file
 * is already archived or is about to be archived.
 *
 * This is currently only used at recovery.  During normal operation this
 * would be racy: the file might get removed or marked with .ready as we're
 * checking it, or immediately after we return.
 */
pub unsafe fn XLogArchiveIsReadyOrDone(xlog: *const c_char) -> bool {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut stat_buf: stat = std::mem::zeroed();

    /* First check for .done --- this means archiver is done with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    /* check for .ready --- this means archiver is still busy with it */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    /* Race condition --- maybe archiver just finished, so recheck */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    false
}

/*
 * XLogArchiveIsReady
 *
 * Check to see if an XLOG segment file has an archive notification (.ready)
 * file.
 */
pub unsafe fn XLogArchiveIsReady(xlog: *const c_char) -> bool {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut stat_buf: stat = std::mem::zeroed();

    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    if stat(archiveStatusPath.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    false
}

/*
 * XLogArchiveCleanup
 *
 * Cleanup archive notification file(s) for a particular xlog segment
 */
pub unsafe fn XLogArchiveCleanup(xlog: *const c_char) {
    let mut archiveStatusPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /* Remove the .done file */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".done".as_ptr());
    unlink(archiveStatusPath.as_ptr());
    /* should we complain about failure? */

    /* Remove the .ready file if present --- normally it shouldn't be */
    StatusFilePath(archiveStatusPath.as_mut_ptr(), xlog, c".ready".as_ptr());
    unlink(archiveStatusPath.as_ptr());
    /* should we complain about failure? */
}

// strlcpy stub
unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    unimplemented!() // TODO: port/strlcpy.c
}

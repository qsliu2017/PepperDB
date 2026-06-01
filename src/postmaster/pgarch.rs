//! src/backend/postmaster/pgarch.c
//!
//! PostgreSQL WAL archiver
//!
//! All functions relating to archiver are included here
//!
//! - All functions executed by archiver process
//!
//! - archiver is forked from postmaster, and the two
//! processes then communicate using signals. All functions
//! executed by postmaster are included in this file.
//!
//! Initial author: Simon Riggs        simon@2ndquadrant.com
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
const SIG_SETMASK: c_int = 2;
use crate::pg_config_manual::MAXPGPATH;

// ----------
// Exports from postmaster/pgarch.h
//
// Archiver control info.
//
// We expect that archivable files within pg_wal will have names between
// MIN_XFN_CHARS and MAX_XFN_CHARS in length, consisting only of characters
// appearing in VALID_XFN_CHARS.  The status files in archive_status have
// corresponding names with ".ready" or ".done" appended.
// ----------
pub const MIN_XFN_CHARS: usize = 16;
pub const MAX_XFN_CHARS: usize = 40;
pub const VALID_XFN_CHARS: &[u8] = b"0123456789ABCDEF.history.backup.partial\0";

// ----------
// Timer definitions.
// ----------
/// How often to force a poll of the archive status directory; in seconds.
const PGARCH_AUTOWAKE_INTERVAL: c_int = 60;
/// How often to attempt to restart a failed archiver; in seconds.
const PGARCH_RESTART_INTERVAL: c_int = 10;

/// Maximum number of retries allowed when attempting to archive a WAL file.
const NUM_ARCHIVE_RETRIES: c_int = 3;

/// Maximum number of retries allowed when attempting to remove an
/// orphan archive status file.
const NUM_ORPHAN_CLEANUP_RETRIES: c_int = 3;

/// Maximum number of .ready files to gather per directory scan.
const NUM_FILES_PER_DIRECTORY_SCAN: c_int = 64;

/* Shared memory area for archiver process */
#[repr(C)]
pub struct PgArchData {
    /// proc number of archiver process
    pub pgprocno: c_int,

    /// Forces a directory scan in pgarch_readyXlog().
    pub force_dir_scan: pg_atomic_uint32,
}

pub static mut XLogArchiveLibrary: *mut c_char = c"".as_ptr() as *mut c_char;
pub static mut arch_module_check_errdetail_string: *mut c_char = std::ptr::null_mut();

// ----------
// Local data
// ----------
static mut last_sigterm_time: time_t = 0;
static mut PgArch: *mut PgArchData = std::ptr::null_mut();
static mut ArchiveCallbacks: *const ArchiveModuleCallbacks = std::ptr::null();
static mut archive_module_state: *mut ArchiveModuleState = std::ptr::null_mut();
static mut archive_context: MemoryContext = std::ptr::null_mut();

/*
 * Stuff for tracking multiple files to archive from each scan of
 * archive_status.  Minimizing the number of directory scans when there are
 * many files to archive can significantly improve archival rate.
 *
 * arch_heap is a max-heap that is used during the directory scan to track
 * the highest-priority files to archive.  After the directory scan
 * completes, the file names are stored in ascending order of priority in
 * arch_files.  pgarch_readyXlog() returns files from arch_files until it
 * is empty, at which point another directory scan must be performed.
 *
 * We only need this data in the archiver process, so make it a palloc'd
 * struct rather than a bunch of static arrays.
 */
#[repr(C)]
pub struct arch_files_state {
    pub arch_heap: *mut binaryheap,
    /// number of live entries in arch_files[]
    pub arch_files_size: c_int,
    pub arch_files: [*mut c_char; NUM_FILES_PER_DIRECTORY_SCAN as usize],
    /// buffers underlying heap, and later arch_files[], entries:
    pub arch_filenames: [[c_char; MAX_XFN_CHARS + 1]; NUM_FILES_PER_DIRECTORY_SCAN as usize],
}

static mut arch_files: *mut arch_files_state = std::ptr::null_mut();

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static mut ready_to_stop: sig_atomic_t = false as sig_atomic_t;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn rename(old: *const c_char, new: *const c_char) -> c_int;
    fn time(tloc: *mut time_t) -> time_t;
}

/* Report shared memory space needed by PgArchShmemInit */
pub unsafe fn PgArchShmemSize() -> Size {
    let mut size: Size = 0;

    size = add_size(size, std::mem::size_of::<PgArchData>());

    size
}

/* Allocate and initialize archiver-related shared memory */
pub unsafe fn PgArchShmemInit() {
    let mut found: bool = false;

    PgArch = ShmemInitStruct(
        c"Archiver Data".as_ptr(),
        PgArchShmemSize(),
        &mut found,
    ) as *mut PgArchData;

    if !found {
        /* First time through, so initialize */
        MemSet(PgArch as *mut c_void, 0, PgArchShmemSize());
        (*PgArch).pgprocno = INVALID_PROC_NUMBER;
        pg_atomic_init_u32(&mut (*PgArch).force_dir_scan, 0);
    }
}

/*
 * PgArchCanRestart
 *
 * Return true and archiver is allowed to restart if enough time has
 * passed since it was launched last to reach PGARCH_RESTART_INTERVAL.
 * Otherwise return false.
 *
 * This is a safety valve to protect against continuous respawn attempts if the
 * archiver is dying immediately at launch. Note that since we will retry to
 * launch the archiver from the postmaster main loop, we will get another
 * chance later.
 */
pub unsafe fn PgArchCanRestart() -> bool {
    static mut last_pgarch_start_time: time_t = 0;
    let curtime: time_t = time(std::ptr::null_mut());

    /*
     * Return false and don't restart archiver if too soon since last archiver
     * start.
     */
    if ((curtime - last_pgarch_start_time) as u32) < (PGARCH_RESTART_INTERVAL as u32) {
        return false;
    }

    last_pgarch_start_time = curtime;
    true
}

/* Main entry point for archiver process */
pub unsafe fn PgArchiverMain(startup_data: *const c_void, startup_data_len: usize) -> ! {
    Assert!(startup_data_len == 0);
    let _ = startup_data;

    MyBackendType = B_ARCHIVER;
    AuxiliaryProcessMainCommon();

    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except for SIGHUP, SIGTERM, SIGUSR1, SIGUSR2, and SIGQUIT.
     */
    pqsignal(SIGHUP, SignalHandlerForConfigReload as usize);
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest as usize);
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler as usize);
    pqsignal(SIGUSR2, pgarch_waken_stop as usize);

    /* Reset some signals that are accepted by postmaster but not here */
    pqsignal(SIGCHLD, SIG_DFL);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    sigprocmask(SIG_SETMASK, UnBlockSig.as_ptr() as *const c_void, std::ptr::null_mut());

    /* We shouldn't be launched unnecessarily. */
    Assert!(XLogArchivingActive());

    /* Arrange to clean up at archiver exit */
    on_shmem_exit(pgarch_die, 0);

    /*
     * Advertise our proc number so that backends can use our latch to wake us
     * up while we're sleeping.
     */
    (*PgArch).pgprocno = MyProcNumber;

    /* Create workspace for pgarch_readyXlog() */
    arch_files = palloc(std::mem::size_of::<arch_files_state>()) as *mut arch_files_state;
    (*arch_files).arch_files_size = 0;

    /* Initialize our max-heap for prioritizing files to archive. */
    (*arch_files).arch_heap = binaryheap_allocate(
        NUM_FILES_PER_DIRECTORY_SCAN,
        ready_file_comparator,
        std::ptr::null_mut(),
    );

    /* Initialize our memory context. */
    archive_context = AllocSetContextCreate!(
        TopMemoryContext,
        c"archiver".as_ptr(),
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    );

    /* Load the archive_library. */
    LoadArchiveLibrary();

    pgarch_MainLoop();

    proc_exit(0);
    unreachable!()
}

/*
 * Wake up the archiver
 */
pub unsafe fn PgArchWakeup() {
    let arch_pgprocno: c_int = (*PgArch).pgprocno;

    /*
     * We don't acquire ProcArrayLock here.  It's actually fine because
     * procLatch isn't ever freed, so we just can potentially set the wrong
     * process' (or no process') latch.  Even in that case the archiver will
     * be relaunched shortly and will start archiving.
     */
    if arch_pgprocno != INVALID_PROC_NUMBER {
        SetLatch(&mut (*(*ProcGlobal).allProcs.offset(arch_pgprocno as isize)).procLatch);
    }
}

/* SIGUSR2 signal handler for archiver process */
unsafe extern "C" fn pgarch_waken_stop(_postgres_signal_arg: c_int) {
    /* set flag to do a final cycle and shut down afterwards */
    ready_to_stop = true as sig_atomic_t;
    SetLatch(MyLatch);
}

/*
 * pgarch_MainLoop
 *
 * Main loop for archiver
 */
unsafe fn pgarch_MainLoop() {
    let mut time_to_stop: bool;

    /*
     * There shouldn't be anything for the archiver to do except to wait for a
     * signal ... however, the archiver exists to protect our data, so it
     * wakes up occasionally to allow itself to be proactive.
     */
    loop {
        ResetLatch(MyLatch);

        /* When we get SIGUSR2, we do one more archive cycle, then exit */
        time_to_stop = ready_to_stop != 0;

        /* Check for barrier events and config update */
        ProcessPgArchInterrupts();

        /*
         * If we've gotten SIGTERM, we normally just sit and do nothing until
         * SIGUSR2 arrives.  However, that means a random SIGTERM would
         * disable archiving indefinitely, which doesn't seem like a good
         * idea.  If more than 60 seconds pass since SIGTERM, exit anyway, so
         * that the postmaster can start a new archiver if needed.
         */
        if ShutdownRequestPending {
            let curtime: time_t = time(std::ptr::null_mut());

            if last_sigterm_time == 0 {
                last_sigterm_time = curtime;
            } else if (curtime - last_sigterm_time) as u32 >= 60u32 {
                break;
            }
        }

        /* Do what we're here for */
        pgarch_ArchiverCopyLoop();

        /*
         * Sleep until a signal is received, or until a poll is forced by
         * PGARCH_AUTOWAKE_INTERVAL, or until postmaster dies.
         */
        if !time_to_stop {
            /* Don't wait during last iteration */
            let rc: c_int = WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                (PGARCH_AUTOWAKE_INTERVAL as i64) * 1000,
                WAIT_EVENT_ARCHIVER_MAIN,
            );
            if rc & WL_POSTMASTER_DEATH != 0 {
                time_to_stop = true;
            }
        }

        /*
         * The archiver quits either when the postmaster dies (not expected)
         * or after completing one more archiving cycle after receiving
         * SIGUSR2.
         */
        if time_to_stop {
            break;
        }
    }
}

/*
 * pgarch_ArchiverCopyLoop
 *
 * Archives all outstanding xlogs then returns
 */
unsafe fn pgarch_ArchiverCopyLoop() {
    let mut xlog: [c_char; MAX_XFN_CHARS + 1] = [0; MAX_XFN_CHARS + 1];

    /* force directory scan in the first call to pgarch_readyXlog() */
    (*arch_files).arch_files_size = 0;

    /*
     * loop through all xlogs with archive_status of .ready and archive
     * them...mostly we expect this to be a single file, though it is possible
     * some backend will add files onto the list of those that need archiving
     * while we are still copying earlier archives
     */
    while pgarch_readyXlog(xlog.as_mut_ptr()) {
        let mut failures: c_int = 0;
        let mut failures_orphan: c_int = 0;

        loop {
            let mut stat_buf: libc_stat = std::mem::zeroed();
            let mut pathname: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            /*
             * Do not initiate any more archive commands after receiving
             * SIGTERM, nor after the postmaster has died unexpectedly. The
             * first condition is to try to keep from having init SIGKILL the
             * command, and the second is to avoid conflicts with another
             * archiver spawned by a newer postmaster.
             */
            if ShutdownRequestPending || !PostmasterIsAlive() {
                return;
            }

            /*
             * Check for barrier events and config update.  This is so that
             * we'll adopt a new setting for archive_command as soon as
             * possible, even if there is a backlog of files to be archived.
             */
            ProcessPgArchInterrupts();

            /* Reset variables that might be set by the callback */
            arch_module_check_errdetail_string = std::ptr::null_mut();

            /* can't do anything if not configured ... */
            if (*ArchiveCallbacks).check_configured_cb.is_some()
                && !(*ArchiveCallbacks).check_configured_cb.unwrap()(archive_module_state)
            {
                // In C: ereport(WARNING, errmsg(...), arch_module_check_errdetail_string ?
                //   errdetail_internal("%s", ...) : 0).
                // Errdetail is advisory; emit as a note appended to the message when present.
                if !arch_module_check_errdetail_string.is_null() {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "\"archive_mode\" enabled, yet archiving is not configured -- {}",
                            std::ffi::CStr::from_ptr(arch_module_check_errdetail_string)
                                .to_string_lossy()
                        )
                    );
                } else {
                    ereport!(
                        WARNING,
                        errmsg!("\"archive_mode\" enabled, yet archiving is not configured")
                    );
                }
                return;
            }

            /*
             * Since archive status files are not removed in a durable manner,
             * a system crash could leave behind .ready files for WAL segments
             * that have already been recycled or removed.  In this case,
             * simply remove the orphan status file and move on.  unlink() is
             * used here as even on subsequent crashes the same orphan files
             * would get removed, so there is no need to worry about
             * durability.
             */
            snprintf(
                pathname.as_mut_ptr(),
                MAXPGPATH,
                c"%s/%s".as_ptr(),
                XLOGDIR.as_ptr(),
                xlog.as_ptr(),
            );
            if stat(pathname.as_ptr(), &mut stat_buf) != 0 && errno() == ENOENT {
                let mut xlogready: [c_char; MAXPGPATH] = [0; MAXPGPATH];

                StatusFilePath(xlogready.as_mut_ptr(), xlog.as_ptr(), c".ready".as_ptr());
                if unlink(xlogready.as_ptr()) == 0 {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "removed orphan archive status file \"{}\"",
                            std::ffi::CStr::from_ptr(xlogready.as_ptr()).to_string_lossy()
                        )
                    );

                    /* leave loop and move to the next status file */
                    break;
                }

                failures_orphan += 1;
                if failures_orphan >= NUM_ORPHAN_CLEANUP_RETRIES {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "removal of orphan archive status file \"{}\" failed too many times, will try again later",
                            std::ffi::CStr::from_ptr(xlogready.as_ptr()).to_string_lossy()
                        )
                    );

                    /* give up cleanup of orphan status files */
                    return;
                }

                /* wait a bit before retrying */
                pg_usleep(1000000);
                continue;
            }

            if pgarch_archiveXlog(xlog.as_mut_ptr()) {
                /* successful */
                pgarch_archiveDone(xlog.as_mut_ptr());

                /*
                 * Tell the cumulative stats system about the WAL file that we
                 * successfully archived
                 */
                pgstat_report_archiver(xlog.as_ptr(), false);

                break; /* out of inner retry loop */
            } else {
                /*
                 * Tell the cumulative stats system about the WAL file that we
                 * failed to archive
                 */
                pgstat_report_archiver(xlog.as_ptr(), true);

                failures += 1;
                if failures >= NUM_ARCHIVE_RETRIES {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "archiving write-ahead log file \"{}\" failed too many times, will try again later",
                            std::ffi::CStr::from_ptr(xlog.as_ptr()).to_string_lossy()
                        )
                    );
                    return; /* give up archiving for now */
                }
                pg_usleep(1000000); /* wait a bit before retrying */
            }
        }
    }
}

/*
 * pgarch_archiveXlog
 *
 * Invokes archive_file_cb to copy one archive file to wherever it should go
 *
 * Returns true if successful
 */
unsafe fn pgarch_archiveXlog(xlog: *mut c_char) -> bool {
    let mut local_sigjmp_buf: sigjmp_buf = std::mem::zeroed();
    let oldcontext: MemoryContext;
    let mut pathname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut activitymsg: [c_char; MAXFNAMELEN + 16] = [0; MAXFNAMELEN + 16];
    let ret: bool;

    snprintf(
        pathname.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s".as_ptr(),
        XLOGDIR.as_ptr(),
        xlog,
    );

    /* Report archive activity in PS display */
    snprintf(
        activitymsg.as_mut_ptr(),
        std::mem::size_of_val(&activitymsg),
        c"archiving %s".as_ptr(),
        xlog,
    );
    set_ps_display(activitymsg.as_ptr());

    oldcontext = MemoryContextSwitchTo(archive_context);

    /*
     * Since the archiver operates at the bottom of the exception stack,
     * ERRORs turn into FATALs and cause the archiver process to restart.
     * However, using ereport(ERROR, ...) when there are problems is easy to
     * code and maintain.  Therefore, we create our own exception handler to
     * catch ERRORs and return false instead of restarting the archiver
     * whenever there is a failure.
     *
     * We assume ERRORs from the archiving callback are the most common
     * exceptions experienced by the archiver, so we opt to handle exceptions
     * here instead of PgArchiverMain() to avoid reinitializing the archiver
     * too frequently.  We could instead add a sigsetjmp() block to
     * PgArchiverMain() and use PG_TRY/PG_CATCH here, but the extra code to
     * avoid the odd archiver restart doesn't seem worth it.
     */
    if sigsetjmp(&mut local_sigjmp_buf, 1) != 0 {
        /* Since not using PG_TRY, must reset error stack by hand */
        error_context_stack = std::ptr::null_mut();

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log. */
        EmitErrorReport();

        /*
         * Try to clean up anything the archive module left behind.  We try to
         * cover anything that an archive module could conceivably have left
         * behind, but it is of course possible that modules could be doing
         * unexpected things that require additional cleanup.  Module authors
         * should be sure to do any extra required cleanup in a PG_CATCH block
         * within the archiving callback, and they are encouraged to notify
         * the pgsql-hackers mailing list so that we can add it here.
         */
        disable_all_timeouts(false);
        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        pgstat_report_wait_end();
        pgaio_error_cleanup();
        ReleaseAuxProcessResources(false);
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        /*
         * Return to the original memory context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();

        /* Flush any leaked data */
        MemoryContextReset(archive_context);

        /* Remove our exception handler */
        PG_exception_stack = std::ptr::null_mut();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* Report failure so that the archiver retries this file */
        ret = false;
    } else {
        /* Enable our exception handler */
        PG_exception_stack = &mut local_sigjmp_buf;

        /* Archive the file! */
        ret = (*ArchiveCallbacks).archive_file_cb.unwrap()(archive_module_state, xlog, pathname.as_ptr());

        /* Remove our exception handler */
        PG_exception_stack = std::ptr::null_mut();

        /* Reset our memory context and switch back to the original one */
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(archive_context);
    }

    if ret {
        snprintf(
            activitymsg.as_mut_ptr(),
            std::mem::size_of_val(&activitymsg),
            c"last was %s".as_ptr(),
            xlog,
        );
    } else {
        snprintf(
            activitymsg.as_mut_ptr(),
            std::mem::size_of_val(&activitymsg),
            c"failed on %s".as_ptr(),
            xlog,
        );
    }
    set_ps_display(activitymsg.as_ptr());

    ret
}

/*
 * pgarch_readyXlog
 *
 * Return name of the oldest xlog file that has not yet been archived.
 * No notification is set that file archiving is now in progress, so
 * this would need to be extended if multiple concurrent archival
 * tasks were created. If a failure occurs, we will completely
 * re-copy the file at the next available opportunity.
 *
 * It is important that we return the oldest, so that we archive xlogs
 * in order that they were written, for two reasons:
 * 1) to maintain the sequential chain of xlogs required for recovery
 * 2) because the oldest ones will sooner become candidates for
 * recycling at time of checkpoint
 *
 * NOTE: the "oldest" comparison will consider any .history file to be older
 * than any other file except another .history file.  Segments on a timeline
 * with a smaller ID will be older than all segments on a timeline with a
 * larger ID; the net result being that past timelines are given higher
 * priority for archiving.  This seems okay, or at least not obviously worth
 * changing.
 */
unsafe fn pgarch_readyXlog(xlog: *mut c_char) -> bool {
    let mut XLogArchiveStatusDir: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let rldir: *mut DIR;
    let mut rlde: *mut dirent;

    /*
     * If a directory scan was requested, clear the stored file names and
     * proceed.
     */
    if pg_atomic_exchange_u32(&mut (*PgArch).force_dir_scan, 0) == 1 {
        (*arch_files).arch_files_size = 0;
    }

    /*
     * If we still have stored file names from the previous directory scan,
     * try to return one of those.  We check to make sure the status file is
     * still present, as the archive_command for a previous file may have
     * already marked it done.
     */
    while (*arch_files).arch_files_size > 0 {
        let mut st: libc_stat = std::mem::zeroed();
        let mut status_file: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let arch_file: *mut c_char;

        (*arch_files).arch_files_size -= 1;
        arch_file = (*arch_files).arch_files[(*arch_files).arch_files_size as usize];
        StatusFilePath(status_file.as_mut_ptr(), arch_file, c".ready".as_ptr());

        if stat(status_file.as_ptr(), &mut st) == 0 {
            strcpy(xlog, arch_file);
            return true;
        } else if errno() != ENOENT {
            ereport!(
                ERROR,
                errmsg!(
                    "could not stat file \"{}\"",
                    std::ffi::CStr::from_ptr(status_file.as_ptr()).to_string_lossy()
                )
            );
            unreachable!();
        }
    }

    /* arch_heap is probably empty, but let's make sure */
    binaryheap_reset((*arch_files).arch_heap);

    /*
     * Open the archive status directory and read through the list of files
     * with the .ready suffix, looking for the earliest files.
     */
    snprintf(
        XLogArchiveStatusDir.as_mut_ptr(),
        MAXPGPATH,
        c"%s/archive_status".as_ptr(),
        XLOGDIR.as_ptr(),
    );
    rldir = AllocateDir(XLogArchiveStatusDir.as_ptr());

    loop {
        rlde = ReadDir(rldir, XLogArchiveStatusDir.as_ptr());
        if rlde.is_null() {
            break;
        }

        let basenamelen: c_int = strlen((*rlde).d_name.as_ptr()) as c_int - 6;
        let mut basename: [c_char; MAX_XFN_CHARS + 1] = [0; MAX_XFN_CHARS + 1];
        let arch_file: *mut c_char;

        /* Ignore entries with unexpected number of characters */
        if basenamelen < MIN_XFN_CHARS as c_int || basenamelen > MAX_XFN_CHARS as c_int {
            continue;
        }

        /* Ignore entries with unexpected characters */
        if strspn((*rlde).d_name.as_ptr(), VALID_XFN_CHARS.as_ptr() as *const c_char)
            < basenamelen as usize
        {
            continue;
        }

        /* Ignore anything not suffixed with .ready */
        if strcmp(
            (*rlde).d_name.as_ptr().offset(basenamelen as isize),
            c".ready".as_ptr(),
        ) != 0
        {
            continue;
        }

        /* Truncate off the .ready */
        memcpy(
            basename.as_mut_ptr() as *mut c_void,
            (*rlde).d_name.as_ptr() as *const c_void,
            basenamelen as usize,
        );
        basename[basenamelen as usize] = 0;

        /*
         * Store the file in our max-heap if it has a high enough priority.
         */
        if (*(*arch_files).arch_heap).bh_size < NUM_FILES_PER_DIRECTORY_SCAN {
            /* If the heap isn't full yet, quickly add it. */
            arch_file =
                (*arch_files).arch_filenames[(*(*arch_files).arch_heap).bh_size as usize].as_mut_ptr();
            strcpy(arch_file, basename.as_ptr());
            binaryheap_add_unordered((*arch_files).arch_heap, CStringGetDatum(arch_file));

            /* If we just filled the heap, make it a valid one. */
            if (*(*arch_files).arch_heap).bh_size == NUM_FILES_PER_DIRECTORY_SCAN {
                binaryheap_build((*arch_files).arch_heap);
            }
        } else if ready_file_comparator(
            binaryheap_first((*arch_files).arch_heap),
            CStringGetDatum(basename.as_ptr()),
            std::ptr::null_mut(),
        ) > 0
        {
            /*
             * Remove the lowest priority file and add the current one to the
             * heap.
             */
            arch_file = DatumGetCString(binaryheap_remove_first((*arch_files).arch_heap));
            strcpy(arch_file, basename.as_ptr());
            binaryheap_add((*arch_files).arch_heap, CStringGetDatum(arch_file));
        }
    }
    FreeDir(rldir);

    /* If no files were found, simply return. */
    if (*(*arch_files).arch_heap).bh_size == 0 {
        return false;
    }

    /*
     * If we didn't fill the heap, we didn't make it a valid one.  Do that
     * now.
     */
    if (*(*arch_files).arch_heap).bh_size < NUM_FILES_PER_DIRECTORY_SCAN {
        binaryheap_build((*arch_files).arch_heap);
    }

    /*
     * Fill arch_files array with the files to archive in ascending order of
     * priority.
     */
    (*arch_files).arch_files_size = (*(*arch_files).arch_heap).bh_size;
    for i in 0..(*arch_files).arch_files_size {
        (*arch_files).arch_files[i as usize] =
            DatumGetCString(binaryheap_remove_first((*arch_files).arch_heap));
    }

    /* Return the highest priority file. */
    (*arch_files).arch_files_size -= 1;
    strcpy(
        xlog,
        (*arch_files).arch_files[(*arch_files).arch_files_size as usize],
    );

    true
}

/*
 * ready_file_comparator
 *
 * Compares the archival priority of the given files to archive.  If "a"
 * has a higher priority than "b", a negative value will be returned.  If
 * "b" has a higher priority than "a", a positive value will be returned.
 * If "a" and "b" have equivalent values, 0 will be returned.
 */
unsafe extern "C" fn ready_file_comparator(a: Datum, b: Datum, _arg: *mut c_void) -> c_int {
    let a_str: *mut c_char = DatumGetCString(a);
    let b_str: *mut c_char = DatumGetCString(b);
    let a_history: bool = IsTLHistoryFileName(a_str);
    let b_history: bool = IsTLHistoryFileName(b_str);

    /* Timeline history files always have the highest priority. */
    if a_history != b_history {
        return if a_history { -1 } else { 1 };
    }

    /* Priority is given to older files. */
    strcmp(a_str, b_str)
}

/*
 * PgArchForceDirScan
 *
 * When called, the next call to pgarch_readyXlog() will perform a
 * directory scan.  This is useful for ensuring that important files such
 * as timeline history files are archived as quickly as possible.
 */
pub unsafe fn PgArchForceDirScan() {
    pg_atomic_write_membarrier_u32(&mut (*PgArch).force_dir_scan, 1);
}

/*
 * pgarch_archiveDone
 *
 * Emit notification that an xlog file has been successfully archived.
 * We do this by renaming the status file from NNN.ready to NNN.done.
 * Eventually, a checkpoint process will notice this and delete both the
 * NNN.done file and the xlog file itself.
 */
unsafe fn pgarch_archiveDone(xlog: *mut c_char) {
    let mut rlogready: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut rlogdone: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    StatusFilePath(rlogready.as_mut_ptr(), xlog, c".ready".as_ptr());
    StatusFilePath(rlogdone.as_mut_ptr(), xlog, c".done".as_ptr());

    /*
     * To avoid extra overhead, we don't durably rename the .ready file to
     * .done.  Archive commands and libraries must gracefully handle attempts
     * to re-archive files (e.g., if the server crashes just before this
     * function is called), so it should be okay if the .ready file reappears
     * after a crash.
     */
    if rename(rlogready.as_ptr(), rlogdone.as_ptr()) < 0 {
        ereport!(
            WARNING,
            errmsg!(
                "could not rename file \"{}\" to \"{}\"",
                std::ffi::CStr::from_ptr(rlogready.as_ptr()).to_string_lossy(),
                std::ffi::CStr::from_ptr(rlogdone.as_ptr()).to_string_lossy()
            )
        );
    }
}

/*
 * pgarch_die
 *
 * Exit-time cleanup handler
 */
unsafe extern "C" fn pgarch_die(_code: c_int, _arg: Datum) {
    (*PgArch).pgprocno = INVALID_PROC_NUMBER;
}

/*
 * Interrupt handler for WAL archiver process.
 *
 * This is called in the loops pgarch_MainLoop and pgarch_ArchiverCopyLoop.
 * It checks for barrier events, config update and request for logging of
 * memory contexts, but not shutdown request because how to handle
 * shutdown request is different between those loops.
 */
unsafe fn ProcessPgArchInterrupts() {
    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }

    if ConfigReloadPending {
        let archiveLib: *mut c_char = pstrdup(XLogArchiveLibrary);
        let archiveLibChanged: bool;

        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);

        if *XLogArchiveLibrary.offset(0) != 0 && *XLogArchiveCommand.offset(0) != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "both \"archive_command\" and \"archive_library\" set -- \
                     only one of \"archive_command\", \"archive_library\" may be set"
                )
            );
            unreachable!();
        }

        archiveLibChanged = strcmp(XLogArchiveLibrary, archiveLib) != 0;
        pfree(archiveLib as *mut c_void);

        if archiveLibChanged {
            /*
             * Ideally, we would simply unload the previous archive module and
             * load the new one, but there is presently no mechanism for
             * unloading a library (see the comment above
             * internal_load_library()).  To deal with this, we simply restart
             * the archiver.  The new archive module will be loaded when the
             * new archiver process starts up.  Note that this triggers the
             * module's shutdown callback, if defined.
             */
            ereport!(
                LOG,
                errmsg!(
                    "restarting archiver process because value of \"archive_library\" was changed"
                )
            );

            proc_exit(0);
        }
    }
}

/*
 * LoadArchiveLibrary
 *
 * Loads the archiving callbacks into our local ArchiveCallbacks.
 */
unsafe fn LoadArchiveLibrary() {
    let archive_init: ArchiveModuleInit;

    if *XLogArchiveLibrary.offset(0) != 0 && *XLogArchiveCommand.offset(0) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "both \"archive_command\" and \"archive_library\" set -- \
                 only one of \"archive_command\", \"archive_library\" may be set"
            )
        );
        unreachable!();
    }

    /*
     * If shell archiving is enabled, use our special initialization function.
     * Otherwise, load the library and call its _PG_archive_module_init().
     */
    if *XLogArchiveLibrary.offset(0) == 0 {
        archive_init = Some(shell_archive_init as ArchiveModuleInitFn);
    } else {
        archive_init = std::mem::transmute::<*mut c_void, ArchiveModuleInit>(load_external_function(
            XLogArchiveLibrary,
            c"_PG_archive_module_init".as_ptr(),
            false,
            std::ptr::null_mut(),
        ));
    }

    if archive_init.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "archive modules have to define the symbol {}",
                "_PG_archive_module_init"
            )
        );
        unreachable!();
    }

    ArchiveCallbacks = archive_init.unwrap()();

    if (*ArchiveCallbacks).archive_file_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("archive modules must register an archive callback")
        );
        unreachable!();
    }

    archive_module_state =
        palloc0(std::mem::size_of::<ArchiveModuleState>()) as *mut ArchiveModuleState;
    if (*ArchiveCallbacks).startup_cb.is_some() {
        (*ArchiveCallbacks).startup_cb.unwrap()(archive_module_state);
    }

    before_shmem_exit(pgarch_call_module_shutdown_cb, 0);
}

/*
 * Call the shutdown callback of the loaded archive module, if defined.
 */
unsafe extern "C" fn pgarch_call_module_shutdown_cb(_code: c_int, _arg: Datum) {
    if (*ArchiveCallbacks).shutdown_cb.is_some() {
        (*ArchiveCallbacks).shutdown_cb.unwrap()(archive_module_state);
    }
}

// ----------
// Local stubs for unported dependencies.
// ----------

type time_t = i64;
type sig_atomic_t = c_int;
type pg_atomic_uint32 = crate::c::uint32;
type binaryheap = binaryheap_stub;
type sigjmp_buf = [c_void; 0];
type DIR = c_void;
type ArchiveModuleInitFn = unsafe extern "C" fn() -> *const ArchiveModuleCallbacks;
type ArchiveModuleInit = Option<ArchiveModuleInitFn>;

#[repr(C)]
pub struct binaryheap_stub {
    bh_size: c_int,
}

#[repr(C)]
struct ArchiveModuleState {
    private_data: *mut c_void,
}

#[repr(C)]
struct ArchiveModuleCallbacks {
    startup_cb: Option<unsafe extern "C" fn(*mut ArchiveModuleState)>,
    check_configured_cb: Option<unsafe extern "C" fn(*mut ArchiveModuleState) -> bool>,
    archive_file_cb:
        Option<unsafe extern "C" fn(*mut ArchiveModuleState, *const c_char, *const c_char) -> bool>,
    shutdown_cb: Option<unsafe extern "C" fn(*mut ArchiveModuleState)>,
}

#[repr(C)]
struct dirent {
    d_name: [c_char; 256],
}

#[repr(C)]
struct libc_stat {
    _opaque: [u8; 256],
}

const MAXFNAMELEN: usize = 64;
const XLOGDIR: &std::ffi::CStr = c"pg_wal";

unsafe fn errno() -> c_int {
    unimplemented!() // TODO: port errno access
}
const ENOENT: c_int = 2;

unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: shmem.c
}
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: shmem.c
}
unsafe fn pg_atomic_init_u32(_ptr: *mut pg_atomic_uint32, _val: crate::c::uint32) {
    unimplemented!() // TODO: atomics.h
}
unsafe fn pg_atomic_exchange_u32(_ptr: *mut pg_atomic_uint32, _newval: crate::c::uint32) -> crate::c::uint32 {
    unimplemented!() // TODO: atomics.h
}
unsafe fn pg_atomic_write_membarrier_u32(_ptr: *mut pg_atomic_uint32, _val: crate::c::uint32) {
    unimplemented!() // TODO: atomics.h
}
unsafe fn AuxiliaryProcessMainCommon() {
    unimplemented!() // TODO: auxprocess.c
}
unsafe fn pqsignal(_signo: c_int, _func: usize) {
    unimplemented!() // TODO: pqsignal.c
}
unsafe extern "C" fn SignalHandlerForConfigReload(_sig: c_int) {
    unimplemented!() // TODO: interrupt.c
}
unsafe extern "C" fn SignalHandlerForShutdownRequest(_sig: c_int) {
    unimplemented!() // TODO: interrupt.c
}
unsafe extern "C" fn procsignal_sigusr1_handler(_sig: c_int) {
    unimplemented!() // TODO: procsignal.c
}
unsafe fn sigprocmask(_how: c_int, _set: *const c_void, _oldset: *mut c_void) -> c_int {
    unimplemented!() // TODO: signal.h
}
unsafe fn XLogArchivingActive() -> bool {
    unimplemented!() // TODO: xlog.h
}
unsafe fn on_shmem_exit(_function: unsafe extern "C" fn(c_int, Datum), _arg: Datum) {
    unimplemented!() // TODO: ipc.c
}
unsafe fn before_shmem_exit(_function: unsafe extern "C" fn(c_int, Datum), _arg: Datum) {
    unimplemented!() // TODO: ipc.c
}
unsafe fn binaryheap_allocate(
    _capacity: c_int,
    _compare: unsafe extern "C" fn(Datum, Datum, *mut c_void) -> c_int,
    _arg: *mut c_void,
) -> *mut binaryheap {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_reset(_heap: *mut binaryheap) {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_build(_heap: *mut binaryheap) {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_add_unordered(_heap: *mut binaryheap, _d: Datum) {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_add(_heap: *mut binaryheap, _d: Datum) {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_first(_heap: *mut binaryheap) -> Datum {
    unimplemented!() // TODO: binaryheap.c
}
unsafe fn binaryheap_remove_first(_heap: *mut binaryheap) -> Datum {
    unimplemented!() // TODO: binaryheap.c
}
const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE: Size = 8 * 1024 * 1024;

unsafe fn proc_exit(_code: c_int) {
    unimplemented!() // TODO: ipc.c
}
unsafe fn SetLatch(_latch: *mut c_void) {
    unimplemented!() // TODO: latch.c
}
unsafe fn ResetLatch(_latch: *mut c_void) {
    unimplemented!() // TODO: latch.c
}
unsafe fn WaitLatch(_latch: *mut c_void, _wakeEvents: c_int, _timeout: i64, _wait_event_info: u32) -> c_int {
    unimplemented!() // TODO: latch.c
}
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 3;
const WL_POSTMASTER_DEATH: c_int = 1 << 4;
const WAIT_EVENT_ARCHIVER_MAIN: u32 = 0;

unsafe fn PostmasterIsAlive() -> bool {
    unimplemented!() // TODO: pmsignal.c
}
unsafe fn StatusFilePath(_path: *mut c_char, _xlog: *const c_char, _suffix: *const c_char) {
    unimplemented!() // TODO: xlog_internal.h
}
unsafe fn pg_usleep(_microsec: i64) {
    unimplemented!() // TODO: pgsleep.c
}
unsafe fn pgstat_report_archiver(_xlog: *const c_char, _failed: bool) {
    unimplemented!() // TODO: pgstat_archiver.c
}
unsafe fn set_ps_display(_activity: *const c_char) {
    unimplemented!() // TODO: ps_status.c
}
unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    unimplemented!() // TODO: setjmp.h
}
unsafe fn HOLD_INTERRUPTS() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn RESUME_INTERRUPTS() {
    unimplemented!() // TODO: miscadmin.h
}
unsafe fn EmitErrorReport() {
    unimplemented!() // TODO: elog.c
}
unsafe fn disable_all_timeouts(_keep_indicators: bool) {
    unimplemented!() // TODO: timeout.c
}
unsafe fn LWLockReleaseAll() {
    unimplemented!() // TODO: lwlock.c
}
unsafe fn ConditionVariableCancelSleep() {
    unimplemented!() // TODO: condition_variable.c
}
unsafe fn pgstat_report_wait_end() {
    unimplemented!() // TODO: wait_event.c
}
unsafe fn pgaio_error_cleanup() {
    unimplemented!() // TODO: aio.c
}
unsafe fn ReleaseAuxProcessResources(_isCommit: bool) {
    unimplemented!() // TODO: resowner.c
}
unsafe fn AtEOXact_Files(_isCommit: bool) {
    unimplemented!() // TODO: fd.c
}
unsafe fn AtEOXact_HashTables(_isCommit: bool) {
    unimplemented!() // TODO: dynahash.c
}
unsafe fn FlushErrorState() {
    unimplemented!() // TODO: elog.c
}
unsafe fn IsTLHistoryFileName(_fname: *const c_char) -> bool {
    unimplemented!() // TODO: xlog_internal.h
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!() // TODO: fd.c
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!() // TODO: fd.c
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!() // TODO: fd.c
}
unsafe fn ProcessProcSignalBarrier() {
    unimplemented!() // TODO: procsignal.c
}
unsafe fn ProcessLogMemoryContextInterrupt() {
    unimplemented!() // TODO: mcxt.c
}
unsafe fn ProcessConfigFile(_context: c_int) {
    unimplemented!() // TODO: guc.c
}
const PGC_SIGHUP: c_int = 1;

unsafe extern "C" fn shell_archive_init() -> *const ArchiveModuleCallbacks {
    unimplemented!() // TODO: shell_archive.c
}
unsafe fn load_external_function(
    _filename: *const c_char,
    _funcname: *const c_char,
    _signalNotFound: bool,
    _filehandle: *mut *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO: dfmgr.c
}

// Externs / globals referenced from other (unported) modules.
const INVALID_PROC_NUMBER: c_int = -1;
const B_ARCHIVER: c_int = 0;
const SIG_IGN: usize = 1;
const SIG_DFL: usize = 0;
const SIGHUP: c_int = 1;
const SIGINT: c_int = 2;
const SIGQUIT: c_int = 3;
const SIGALRM: c_int = 14;
const SIGTERM: c_int = 15;
const SIGPIPE: c_int = 13;
const SIGUSR1: c_int = 30;
const SIGUSR2: c_int = 31;
const SIGCHLD: c_int = 20;

static mut MyBackendType: c_int = 0;
static mut MyProcNumber: c_int = 0;
static mut MyLatch: *mut c_void = std::ptr::null_mut();
static mut UnBlockSig: [u8; 128] = [0; 128]; // sigset_t stub
static mut ProcGlobal: *mut PROC_HDR = std::ptr::null_mut();
static mut ShutdownRequestPending: bool = false;
static mut ConfigReloadPending: bool = false;
static mut ProcSignalBarrierPending: bool = false;
static mut LogMemoryContextPending: bool = false;
static mut error_context_stack: *mut c_void = std::ptr::null_mut();
static mut PG_exception_stack: *mut sigjmp_buf = std::ptr::null_mut();
static mut XLogArchiveCommand: *mut c_char = std::ptr::null_mut();

#[repr(C)]
struct PROC_HDR {
    allProcs: *mut PGPROC,
}
#[repr(C)]
struct PGPROC {
    procLatch: c_void,
}


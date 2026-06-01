/*-------------------------------------------------------------------------
 *
 * miscinit.c -> miscinit.rs
 *	  miscellaneous initialization support stuff
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/miscinit.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

// ---------------------------------------------------------------------------
// REAL imports from ported modules
// ---------------------------------------------------------------------------

use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
use crate::c::NameStr;
use crate::catalog::pg_authid::Form_pg_authid;
use crate::common::file_perm::{
    pg_dir_create_mode, pg_file_create_mode, pg_mode_mask, SetDataDirectoryCreatePerm,
    PG_MODE_MASK_GROUP,
};
use crate::libpq::libpq::{FeBeWaitSet, FeBeWaitSetLatchPos};
use crate::miscadmin::{
    AmAutoVacuumWorkerProcess, AmBackgroundWorkerProcess, AmLogicalSlotSyncWorkerProcess,
    AmRegularBackendProcess, BackendType, ProcessingMode, B_ARCHIVER, B_AUTOVAC_LAUNCHER,
    B_AUTOVAC_WORKER, B_BACKEND, B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND,
    B_INVALID, B_IO_WORKER, B_LOGGER, B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP,
    B_WAL_RECEIVER, B_WAL_SENDER, B_WAL_SUMMARIZER, B_WAL_WRITER, InitProcessing,
    SECURITY_LOCAL_USERID_CHANGE, SECURITY_NOFORCE_RLS, SECURITY_RESTRICTED_OPERATION,
};
use crate::nodes::pg_list::{lcons, lfirst, list_free_deep, List, NIL};
use crate::storage::file::fd::{pg_fsync, AllocateFile, FreeFile};
use crate::storage::ipc::ipc::{on_exit_reset, on_proc_exit};
use crate::storage::ipc::latch::{InitLatch, InitializeLatchWaitSet, ModifyWaitEvent, SetLatch};
use crate::storage::ipc::pmsignal::PostmasterDeathSignalInit;
use crate::storage::lmgr::proc::{MyProc, PGPROC};
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1};
use crate::utils::elog::{DEBUG1, ERROR, FATAL, LOG, NOTICE};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::mmgr::mcxt::{pfree, MemoryContextStrdup, TopMemoryContext};

use core::ffi::CStr;

// ---------------------------------------------------------------------------
// extern C globals defined in globals.rs / postmaster.rs / parallel.rs
// ---------------------------------------------------------------------------
extern "C" {
    pub static mut DataDir: *mut c_char;
    pub static mut DatabasePath: *mut c_char;
    pub static mut data_directory_mode: c_int;
    pub static mut MyStartTime: pg_time_t;
    pub static mut IsPostmasterEnvironment: bool;
    pub static mut IsUnderPostmaster: bool;
    pub static mut my_exec_path: [c_char; 0];
    pub static mut pkglib_path: [c_char; 0];
    pub static mut BlockSig: sigset_t;
    pub static mut PostPortNumber: c_int;
    pub static mut InitializingParallelWorker: bool;
    pub static mut MyLatch: *mut Latch;
}

// ---------------------------------------------------------------------------
// Type aliases matching C usage in this file.
// ---------------------------------------------------------------------------
type Latch = c_void;
type pid_t = c_int;
type Size = usize;
type Oid = crate::postgres_ext::Oid;
type sigset_t = libc::sigset_t;
type UserAuth = c_int;
type pg_time_t = i64;

// Per-thread errno location. macOS/BSD expose it as __error().
extern "C" {
    #[link_name = "__error"]
    fn __error() -> *mut c_int;
}
unsafe fn errno() -> c_int {
    *__error()
}
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// ---------------------------------------------------------------------------
// TODO(pg-port): dependencies living in other not-yet-ported .c files
// ---------------------------------------------------------------------------

// access/parallel.h - InitializingParallelWorker handled via extern above.

// utils/inval.c
unsafe fn AcceptInvalidationMessages() {
    // TODO(pg-port): translate utils/cache/inval.c AcceptInvalidationMessages
}

// utils/misc/guc.c
const PGC_INTERNAL: c_int = 0;
const PGC_BACKEND: c_int = 6;
const PGC_S_DYNAMIC_DEFAULT: c_int = 5;
const PGC_S_OVERRIDE: c_int = 17;
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: c_int,
) {
    // TODO(pg-port): translate utils/misc/guc.c SetConfigOption
}

// utils/misc/superuser.c
unsafe fn superuser_arg(_roleid: Oid) -> bool {
    // TODO(pg-port): translate utils/misc/superuser.c superuser_arg
    false
}

// utils/cache/syscache.h cache ids
const AUTHOID: c_int = 12;
const AUTHNAME: c_int = 11;

// catalog/pg_authid.h
const BOOTSTRAP_SUPERUSERID: Oid = 10;

// storage/ipc/procarray.c
unsafe fn CountUserBackends(_roleid: Oid) -> c_int {
    // TODO(pg-port): translate storage/ipc/procarray.c CountUserBackends
    0
}

// utils/mmgr/mcxt.c psprintf / pstrdup are in mcxt; pstrdup imported via prelude.
unsafe fn psprintf_2(fmt: *const c_char, a: *const c_char, b: *const c_char) -> *mut c_char {
    // TODO(pg-port): translate utils/mmgr/mcxt.c psprintf (variadic). Used only
    // for "%s:%s" here; build the result directly.
    let fs = CStr::from_ptr(fmt).to_string_lossy();
    let _ = fs;
    let sa = CStr::from_ptr(a).to_string_lossy();
    let sb = CStr::from_ptr(b).to_string_lossy();
    let joined = std::ffi::CString::new(format!("{}:{}", sa, sb)).unwrap();
    crate::utils::palloc::pstrdup(joined.as_ptr())
}

// utils/builtins.h
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    crate::utils::builtins::CStringGetTextDatum(s)
}

// pgstat.h wait-event reporting
unsafe fn pgstat_report_wait_start(_event: u32) {
    // TODO(pg-port): translate utils/activity/wait_event.c pgstat_report_wait_start
}
unsafe fn pgstat_report_wait_end() {
    // TODO(pg-port): translate utils/activity/wait_event.c pgstat_report_wait_end
}

// utils/activity/wait_event_names - WAIT_EVENT_* codes (placeholders)
const WAIT_EVENT_LOCK_FILE_CREATE_READ: u32 = 0;
const WAIT_EVENT_LOCK_FILE_CREATE_WRITE: u32 = 0;
const WAIT_EVENT_LOCK_FILE_CREATE_SYNC: u32 = 0;
const WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ: u32 = 0;
const WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE: u32 = 0;
const WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC: u32 = 0;
const WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ: u32 = 0;

// storage/pg_shmem.h
unsafe fn PGSharedMemoryIsInUse(_id1: c_ulong, _id2: c_ulong) -> bool {
    // TODO(pg-port): translate port/sysv_shmem.c PGSharedMemoryIsInUse
    false
}

// utils/varlena.c
unsafe fn SplitDirectoriesString(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    // TODO(pg-port): translate utils/adt/varlena.c SplitDirectoriesString
    false
}

// port/path.c
unsafe fn first_dir_separator(filename: *const c_char) -> *mut c_char {
    crate::port::path::first_dir_separator(filename)
}
unsafe fn make_absolute_path(path: *const c_char) -> *mut c_char {
    crate::port::path::make_absolute_path(path)
}
unsafe fn get_pkglib_path(my_exec_path: *const c_char, ret_path: *mut c_char) {
    crate::port::path::get_pkglib_path(my_exec_path, ret_path)
}
unsafe fn find_my_exec(argv0: *const c_char, retpath: *mut c_char) -> c_int {
    crate::port::port_api::find_my_exec(argv0, retpath)
}

// utils/fmgr/dfmgr.c
unsafe fn load_file(filename: *const c_char, restricted: bool) {
    crate::utils::fmgr::dfmgr::load_file(filename, restricted)
}

// postmaster/postmaster.c, storage/ipc/* process init helpers
unsafe fn InitProcessGlobals() {
    crate::postmaster::postmaster::InitProcessGlobals()
}
unsafe fn InitializeWaitEventSupport() {
    // TODO(pg-port): translate utils/activity/wait_event.c InitializeWaitEventSupport
}
unsafe fn pqinitmask() {
    // TODO(pg-port): translate libpq/pqsignal.c pqinitmask
}
unsafe fn SignalHandlerForCrashExit(_signo: c_int) {
    // TODO(pg-port): translate postmaster/interrupt.c SignalHandlerForCrashExit
}
const SIGQUIT: c_int = libc::SIGQUIT;
unsafe fn pqsignal(signo: c_int, func: unsafe fn(c_int)) {
    // TODO(pg-port): route through libpq/pqsignal.c pqsignal
    let _ = (signo, func);
}

// storage/ipc.h - postmaster death monitoring pipe (EXEC/UNIX path)
const POSTMASTER_FD_WATCH: usize = 1;
static mut postmaster_alive_fds: [c_int; 2] = [-1, -1];

// utils/pidfile.h
const LOCK_FILE_LINE_SHMEM_KEY: c_int = 6;

// pg_config_manual.h
const MAXPGPATH: usize = 1024;
const BLCKSZ: usize = 8192;

// version string (pg_config.h)
const PG_VERSION: &str = "18.3";

// htup_details accessor for pg_authid form
unsafe fn authid_form(tuple: HeapTuple) -> Form_pg_authid {
    GETSTRUCT(tuple as *const HeapTupleData) as Form_pg_authid
}

#[macro_export]
macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => { $crate::errmsg!($fmt $(, $arg)*) };
}

// ---------------------------------------------------------------------------
// File-scope globals (defined here in miscinit.c).
// ---------------------------------------------------------------------------

const DIRECTORY_LOCK_FILE: &CStr = c"postmaster.pid";

#[no_mangle]
pub static mut Mode: ProcessingMode = InitProcessing;

#[no_mangle]
pub static mut MyBackendType: BackendType = B_INVALID;

/* List of lock files to be removed at proc exit */
static mut lock_files: *mut List = NIL;

static mut LocalLatchData: LatchData = LatchData { _opaque: [0; 1] };

// Concrete storage backing the process-local latch. In C, struct Latch.
#[repr(C)]
struct LatchData {
    _opaque: [u8; 1],
}

/* ----------------------------------------------------------------
 *		ignoring system indexes support stuff
 *
 * NOTE: "ignoring system indexes" means we do not use the system indexes
 * for lookups (either in hardwired catalog accesses or in planner-generated
 * plans).  We do, however, still update the indexes when a catalog
 * modification is made.
 * ----------------------------------------------------------------
 */

#[no_mangle]
pub static mut IgnoreSystemIndexes: bool = false;

/* ----------------------------------------------------------------
 *	common process startup code
 * ----------------------------------------------------------------
 */

/*
 * Initialize the basic environment for a postmaster child
 *
 * Should be called as early as possible after the child's startup. However,
 * on EXEC_BACKEND builds it does need to be after read_backend_variables().
 */
#[no_mangle]
pub unsafe extern "C" fn InitPostmasterChild() {
    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    /*
     * Start our win32 signal implementation. This has to be done after we
     * read the backend variables, because we need to pick up the signal pipe
     * from the parent process.
     */
    // #ifdef WIN32: pgwin32_signal_initialize();

    InitProcessGlobals();

    /*
     * make sure stderr is in binary mode before anything can possibly be
     * written to it, in case it's actually the syslogger pipe, so the pipe
     * chunking protocol isn't disturbed. Non-logpipe data gets translated on
     * redirection (e.g. via pg_ctl -l) anyway.
     */
    // #ifdef WIN32: _setmode(fileno(stderr), _O_BINARY);

    /* We don't want the postmaster's proc_exit() handlers */
    on_exit_reset();

    /* In EXEC_BACKEND case we will not have inherited BlockSig etc values */
    // #ifdef EXEC_BACKEND: pqinitmask();

    /* Initialize process-local latch support */
    InitializeWaitEventSupport();
    InitProcessLocalLatch();
    InitializeLatchWaitSet();

    /*
     * If possible, make this process a group leader, so that the postmaster
     * can signal any child processes too. Not all processes will have
     * children, but for consistency we make all postmaster child processes do
     * this.
     */
    // #ifdef HAVE_SETSID
    if libc::setsid() < 0 {
        elog!(FATAL, "setsid() failed: {}", io_strerror(errno()));
    }
    // #endif

    /*
     * Every postmaster child process is expected to respond promptly to
     * SIGQUIT at all times.  Therefore we centrally remove SIGQUIT from
     * BlockSig and install a suitable signal handler.  (Client-facing
     * processes may choose to replace this default choice of handler with
     * quickdie().)  All other blockable signals remain blocked for now.
     */
    pqsignal(SIGQUIT, SignalHandlerForCrashExit);

    libc::sigdelset(&raw mut BlockSig, SIGQUIT);
    libc::sigprocmask(libc::SIG_SETMASK, &raw const BlockSig, core::ptr::null_mut());

    /* Request a signal if the postmaster dies, if possible. */
    PostmasterDeathSignalInit();

    /* Don't give the pipe to subprograms that we execute. */
    // #ifndef WIN32
    if libc::fcntl(
        postmaster_alive_fds[POSTMASTER_FD_WATCH],
        libc::F_SETFD,
        libc::FD_CLOEXEC,
    ) < 0
    {
        // C also: errcode_for_socket_access()
        ereport!(
            FATAL,
            errmsg_internal!(
                "could not set postmaster death monitoring pipe to FD_CLOEXEC mode: {}",
                io_strerror(errno())
            )
        );
    }
    // #endif
}

/*
 * Initialize the basic environment for a standalone process.
 *
 * argv0 has to be suitable to find the program's executable.
 */
#[no_mangle]
pub unsafe extern "C" fn InitStandaloneProcess(argv0: *const c_char) {
    Assert!(!IsPostmasterEnvironment);

    MyBackendType = B_STANDALONE_BACKEND;

    /*
     * Start our win32 signal implementation
     */
    // #ifdef WIN32: pgwin32_signal_initialize();

    InitProcessGlobals();

    /* Initialize process-local latch support */
    InitializeWaitEventSupport();
    InitProcessLocalLatch();
    InitializeLatchWaitSet();

    /*
     * For consistency with InitPostmasterChild, initialize signal mask here.
     * But we don't unblock SIGQUIT or provide a default handler for it.
     */
    pqinitmask();
    libc::sigprocmask(libc::SIG_SETMASK, &raw const BlockSig, core::ptr::null_mut());

    /* Compute paths, no postmaster to inherit from */
    if *my_exec_path.as_ptr() == 0 {
        if find_my_exec(argv0, my_exec_path.as_mut_ptr()) < 0 {
            elog!(
                FATAL,
                "{}: could not locate my own executable path",
                CStr::from_ptr(argv0).to_string_lossy()
            );
        }
    }

    if *pkglib_path.as_ptr() == 0 {
        get_pkglib_path(my_exec_path.as_ptr(), pkglib_path.as_mut_ptr());
    }
}

#[no_mangle]
pub unsafe extern "C" fn SwitchToSharedLatch() {
    Assert!(MyLatch == core::ptr::addr_of_mut!(LocalLatchData) as *mut Latch);
    Assert!(!MyProc.is_null());

    MyLatch = &raw mut (*MyProc).procLatch as *mut Latch;

    if !FeBeWaitSet.is_null() {
        ModifyWaitEvent(
            FeBeWaitSet,
            FeBeWaitSetLatchPos,
            WL_LATCH_SET,
            MyLatch,
        );
    }

    /*
     * Set the shared latch as the local one might have been set. This
     * shouldn't normally be necessary as code is supposed to check the
     * condition before waiting for the latch, but a bit care can't hurt.
     */
    SetLatch(MyLatch);
}

#[no_mangle]
pub unsafe extern "C" fn InitProcessLocalLatch() {
    MyLatch = core::ptr::addr_of_mut!(LocalLatchData) as *mut Latch;
    InitLatch(MyLatch);
}

#[no_mangle]
pub unsafe extern "C" fn SwitchBackToLocalLatch() {
    Assert!(MyLatch != core::ptr::addr_of_mut!(LocalLatchData) as *mut Latch);
    Assert!(!MyProc.is_null() && MyLatch == &raw mut (*MyProc).procLatch as *mut Latch);

    MyLatch = core::ptr::addr_of_mut!(LocalLatchData) as *mut Latch;

    if !FeBeWaitSet.is_null() {
        ModifyWaitEvent(
            FeBeWaitSet,
            FeBeWaitSetLatchPos,
            WL_LATCH_SET,
            MyLatch,
        );
    }

    SetLatch(MyLatch);
}

// storage/latch.h WL_LATCH_SET
const WL_LATCH_SET: u32 = 1 << 0;

// Format an errno value the way C's %m would (strerror).
unsafe fn io_strerror(err: c_int) -> std::borrow::Cow<'static, str> {
    let p = libc::strerror(err);
    if p.is_null() {
        std::borrow::Cow::Borrowed("unknown error")
    } else {
        std::borrow::Cow::Owned(CStr::from_ptr(p).to_string_lossy().into_owned())
    }
}

/*
 * Return a human-readable string representation of a BackendType.
 *
 * The string is not localized here, but we mark the strings for translation
 * so that callers can invoke _() on the result.
 */
#[no_mangle]
pub unsafe extern "C" fn GetBackendTypeDesc(backendType: BackendType) -> *const c_char {
    let mut backendDesc: &CStr = c"unknown process type";

    match backendType {
        x if x == B_INVALID => backendDesc = c"not initialized",
        x if x == B_ARCHIVER => backendDesc = c"archiver",
        x if x == B_AUTOVAC_LAUNCHER => backendDesc = c"autovacuum launcher",
        x if x == B_AUTOVAC_WORKER => backendDesc = c"autovacuum worker",
        x if x == B_BACKEND => backendDesc = c"client backend",
        x if x == B_DEAD_END_BACKEND => backendDesc = c"dead-end client backend",
        x if x == B_BG_WORKER => backendDesc = c"background worker",
        x if x == B_BG_WRITER => backendDesc = c"background writer",
        x if x == B_CHECKPOINTER => backendDesc = c"checkpointer",
        x if x == B_IO_WORKER => backendDesc = c"io worker",
        x if x == B_LOGGER => backendDesc = c"logger",
        x if x == B_SLOTSYNC_WORKER => backendDesc = c"slotsync worker",
        x if x == B_STANDALONE_BACKEND => backendDesc = c"standalone backend",
        x if x == B_STARTUP => backendDesc = c"startup",
        x if x == B_WAL_RECEIVER => backendDesc = c"walreceiver",
        x if x == B_WAL_SENDER => backendDesc = c"walsender",
        x if x == B_WAL_SUMMARIZER => backendDesc = c"walsummarizer",
        x if x == B_WAL_WRITER => backendDesc = c"walwriter",
        _ => {}
    }

    backendDesc.as_ptr()
}

/* ----------------------------------------------------------------
 *				database path / name support stuff
 * ----------------------------------------------------------------
 */

#[no_mangle]
pub unsafe extern "C" fn SetDatabasePath(path: *const c_char) {
    /* This should happen only once per process */
    Assert!(DatabasePath.is_null());
    DatabasePath = MemoryContextStrdup(TopMemoryContext, path);
}

/*
 * Validate the proposed data directory.
 *
 * Also initialize file and directory create modes and mode mask.
 */
#[no_mangle]
pub unsafe extern "C" fn checkDataDir() {
    let mut stat_buf: libc::stat = core::mem::zeroed();

    Assert!(!DataDir.is_null());

    if libc::stat(DataDir, &mut stat_buf) != 0 {
        if errno() == libc::ENOENT {
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "data directory \"{}\" does not exist",
                    CStr::from_ptr(DataDir).to_string_lossy()
                )
            );
        } else {
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "could not read permissions of directory \"{}\": {}",
                    CStr::from_ptr(DataDir).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }
    }

    /* eventual chdir would fail anyway, but let's test ... */
    if (stat_buf.st_mode & libc::S_IFMT) != libc::S_IFDIR {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(
            FATAL,
            errmsg!(
                "specified data directory \"{}\" is not a directory",
                CStr::from_ptr(DataDir).to_string_lossy()
            )
        );
    }

    /*
     * Check that the directory belongs to my userid; if not, reject.
     *
     * This check is an essential part of the interlock that prevents two
     * postmasters from starting in the same directory (see CreateLockFile()).
     * Do not remove or weaken it.
     *
     * XXX can we safely enable this check on Windows?
     */
    // #if !defined(WIN32) && !defined(__CYGWIN__)
    if stat_buf.st_uid != libc::geteuid() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        // errhint("The server must be started by the user that owns the data directory.")
        ereport!(
            FATAL,
            errmsg!(
                "data directory \"{}\" has wrong ownership",
                CStr::from_ptr(DataDir).to_string_lossy()
            )
        );
    }
    // #endif

    /*
     * Check if the directory has correct permissions.  If not, reject.
     *
     * Only two possible modes are allowed, 0700 and 0750.  The latter mode
     * indicates that group read/execute should be allowed on all newly
     * created files and directories.
     *
     * XXX temporarily suppress check when on Windows, because there may not
     * be proper support for Unix-y file permissions.  Need to think of a
     * reasonable check to apply on Windows.
     */
    // #if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat_buf.st_mode as c_int & PG_MODE_MASK_GROUP) != 0 {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        // errdetail("Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).")
        ereport!(
            FATAL,
            errmsg!(
                "data directory \"{}\" has invalid permissions",
                CStr::from_ptr(DataDir).to_string_lossy()
            )
        );
    }
    // #endif

    /*
     * Reset creation modes and mask based on the mode of the data directory.
     *
     * The mask was set earlier in startup to disallow group permissions on
     * newly created files and directories.  However, if group read/execute
     * are present on the data directory then modify the create modes and mask
     * to allow group read/execute on newly created files and directories and
     * set the data_directory_mode GUC.
     *
     * Suppress when on Windows, because there may not be proper support for
     * Unix-y file permissions.
     */
    // #if !defined(WIN32) && !defined(__CYGWIN__)
    SetDataDirectoryCreatePerm(stat_buf.st_mode as c_int);

    libc::umask(pg_mode_mask as libc::mode_t);
    data_directory_mode = pg_dir_create_mode;
    // #endif

    /* Check for PG_VERSION */
    ValidatePgVersion(DataDir);
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set DataDir directly.
 */
#[no_mangle]
pub unsafe extern "C" fn SetDataDir(dir: *const c_char) {
    Assert!(!dir.is_null());

    /* If presented path is relative, convert to absolute */
    let new: *mut c_char = make_absolute_path(dir);

    libc::free(DataDir as *mut c_void);
    DataDir = new;
}

/*
 * Change working directory to DataDir.  Most of the postmaster and backend
 * code assumes that we are in DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
#[no_mangle]
pub unsafe extern "C" fn ChangeToDataDir() {
    Assert!(!DataDir.is_null());

    if libc::chdir(DataDir) < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            FATAL,
            errmsg!(
                "could not change directory to \"{}\": {}",
                CStr::from_ptr(DataDir).to_string_lossy(),
                io_strerror(errno())
            )
        );
    }
}

/* ----------------------------------------------------------------
 *	User ID state
 *
 * We have to track several different values associated with the concept
 * of "user ID".  (See header comment in miscinit.c for full description.)
 * ----------------------------------------------------------------
 */
static mut AuthenticatedUserId: Oid = InvalidOid;
static mut SessionUserId: Oid = InvalidOid;
static mut OuterUserId: Oid = InvalidOid;
static mut CurrentUserId: Oid = InvalidOid;
static mut SystemUser: *const c_char = core::ptr::null();

/* We also have to remember the superuser state of the session user */
static mut SessionUserIsSuperuser: bool = false;

static mut SecurityRestrictionContext: c_int = 0;

/* We also remember if a SET ROLE is currently active */
static mut SetRoleIsActive: bool = false;

/*
 * GetUserId - get the current effective user ID.
 *
 * Note: there's no SetUserId() anymore; use SetUserIdAndSecContext().
 */
#[no_mangle]
pub unsafe extern "C" fn GetUserId() -> Oid {
    Assert!(OidIsValid(CurrentUserId));
    CurrentUserId
}

/*
 * GetOuterUserId/SetOuterUserId - get/set the outer-level user ID.
 */
#[no_mangle]
pub unsafe extern "C" fn GetOuterUserId() -> Oid {
    Assert!(OidIsValid(OuterUserId));
    OuterUserId
}

unsafe fn SetOuterUserId(userid: Oid, is_superuser: bool) {
    Assert!(SecurityRestrictionContext == 0);
    Assert!(OidIsValid(userid));
    OuterUserId = userid;

    /* We force the effective user ID to match, too */
    CurrentUserId = userid;

    /* Also update the is_superuser GUC to match OuterUserId's property */
    SetConfigOption(
        c"is_superuser".as_ptr(),
        if is_superuser {
            c"on".as_ptr()
        } else {
            c"off".as_ptr()
        },
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );
}

/*
 * GetSessionUserId/SetSessionUserId - get/set the session user ID.
 */
#[no_mangle]
pub unsafe extern "C" fn GetSessionUserId() -> Oid {
    Assert!(OidIsValid(SessionUserId));
    SessionUserId
}

#[no_mangle]
pub unsafe extern "C" fn GetSessionUserIsSuperuser() -> bool {
    Assert!(OidIsValid(SessionUserId));
    SessionUserIsSuperuser
}

unsafe fn SetSessionUserId(userid: Oid, is_superuser: bool) {
    Assert!(SecurityRestrictionContext == 0);
    Assert!(OidIsValid(userid));
    SessionUserId = userid;
    SessionUserIsSuperuser = is_superuser;
}

/*
 * Return the system user representing the authenticated identity.
 * It is defined in InitializeSystemUser() as auth_method:authn_id.
 */
#[no_mangle]
pub unsafe extern "C" fn GetSystemUser() -> *const c_char {
    SystemUser
}

/*
 * GetAuthenticatedUserId/SetAuthenticatedUserId - get/set the authenticated
 * user ID
 */
#[no_mangle]
pub unsafe extern "C" fn GetAuthenticatedUserId() -> Oid {
    Assert!(OidIsValid(AuthenticatedUserId));
    AuthenticatedUserId
}

#[no_mangle]
pub unsafe extern "C" fn SetAuthenticatedUserId(userid: Oid) {
    Assert!(OidIsValid(userid));

    /* call only once */
    Assert!(!OidIsValid(AuthenticatedUserId));

    AuthenticatedUserId = userid;

    /* Also mark our PGPROC entry with the authenticated user id */
    /* (We assume this is an atomic store so no lock is needed) */
    (*MyProc).roleId = userid;
}

/*
 * GetUserIdAndSecContext/SetUserIdAndSecContext - get/set the current user ID
 * and the SecurityRestrictionContext flags.  (See miscinit.c for details.)
 */
#[no_mangle]
pub unsafe extern "C" fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int) {
    *userid = CurrentUserId;
    *sec_context = SecurityRestrictionContext;
}

#[no_mangle]
pub unsafe extern "C" fn SetUserIdAndSecContext(userid: Oid, sec_context: c_int) {
    CurrentUserId = userid;
    SecurityRestrictionContext = sec_context;
}

/*
 * InLocalUserIdChange - are we inside a local change of CurrentUserId?
 */
#[no_mangle]
pub unsafe extern "C" fn InLocalUserIdChange() -> bool {
    (SecurityRestrictionContext & SECURITY_LOCAL_USERID_CHANGE) != 0
}

/*
 * InSecurityRestrictedOperation - are we inside a security-restricted command?
 */
#[no_mangle]
pub unsafe extern "C" fn InSecurityRestrictedOperation() -> bool {
    (SecurityRestrictionContext & SECURITY_RESTRICTED_OPERATION) != 0
}

/*
 * InNoForceRLSOperation - are we ignoring FORCE ROW LEVEL SECURITY ?
 */
#[no_mangle]
pub unsafe extern "C" fn InNoForceRLSOperation() -> bool {
    (SecurityRestrictionContext & SECURITY_NOFORCE_RLS) != 0
}

/*
 * These are obsolete versions of Get/SetUserIdAndSecContext that are
 * only provided for bug-compatibility with some rather dubious code in
 * pljava.  We allow the userid to be set, but only when not inside a
 * security restriction context.
 */
#[no_mangle]
pub unsafe extern "C" fn GetUserIdAndContext(userid: *mut Oid, sec_def_context: *mut bool) {
    *userid = CurrentUserId;
    *sec_def_context = InLocalUserIdChange();
}

#[no_mangle]
pub unsafe extern "C" fn SetUserIdAndContext(userid: Oid, sec_def_context: bool) {
    /* We throw the same error SET ROLE would. */
    if InSecurityRestrictedOperation() {
        // C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        ereport!(
            ERROR,
            errmsg!(
                "cannot set parameter \"{}\" within security-restricted operation",
                "role"
            )
        );
    }
    CurrentUserId = userid;
    if sec_def_context {
        SecurityRestrictionContext |= SECURITY_LOCAL_USERID_CHANGE;
    } else {
        SecurityRestrictionContext &= !SECURITY_LOCAL_USERID_CHANGE;
    }
}

/*
 * Check whether specified role has explicit REPLICATION privilege
 */
#[no_mangle]
pub unsafe extern "C" fn has_rolreplication(roleid: Oid) -> bool {
    let mut result: bool = false;

    /* Superusers bypass all permission checking. */
    if superuser_arg(roleid) {
        return true;
    }

    let utup: HeapTuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if HeapTupleIsValid(utup) {
        result = (*authid_form(utup)).rolreplication;
        ReleaseSysCache(utup);
    }
    result
}

/*
 * Initialize user identity during normal backend startup
 */
#[no_mangle]
pub unsafe extern "C" fn InitializeSessionUserId(
    rolename: *const c_char,
    mut roleid: Oid,
    bypass_login_check: bool,
) {
    let roleTup: HeapTuple;

    /*
     * In a parallel worker, we don't have to do anything here.
     * ParallelWorkerMain already set our output variables, and we aren't
     * going to enforce either rolcanlogin or rolconnlimit.  Furthermore, we
     * don't really want to perform a catalog lookup for the role: we don't
     * want to fail if it's been dropped.
     */
    if InitializingParallelWorker {
        Assert!(bypass_login_check);
        return;
    }

    /*
     * Don't do scans if we're bootstrapping, none of the system catalogs
     * exist yet, and they should be owned by postgres anyway.
     */
    Assert!(!IsBootstrapProcessingMode());

    /*
     * Make sure syscache entries are flushed for recent catalog changes. This
     * allows us to find roles that were created on-the-fly during
     * authentication.
     */
    AcceptInvalidationMessages();

    /*
     * Look up the role, either by name if that's given or by OID if not.
     */
    if !rolename.is_null() {
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename as *const c_void));
        if !HeapTupleIsValid(roleTup) {
            // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            ereport!(
                FATAL,
                errmsg!(
                    "role \"{}\" does not exist",
                    CStr::from_ptr(rolename).to_string_lossy()
                )
            );
        }
    } else {
        roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
        if !HeapTupleIsValid(roleTup) {
            // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            ereport!(
                FATAL,
                errmsg!("role with OID {} does not exist", roleid)
            );
        }
    }

    let rform: Form_pg_authid = authid_form(roleTup);
    roleid = (*rform).oid;
    let rname: *mut c_char = NameStr(&(*rform).rolname) as *mut c_char;
    let is_superuser: bool = (*rform).rolsuper;

    SetAuthenticatedUserId(roleid);

    /*
     * Set SessionUserId and related variables, including "role", via the GUC
     * mechanisms.  (See miscinit.c for the long rationale comment.)
     */
    SetConfigOption(
        c"session_authorization".as_ptr(),
        rname,
        PGC_BACKEND,
        PGC_S_OVERRIDE,
    );

    /*
     * These next checks are not enforced when in standalone mode, so that
     * there is a way to recover from sillinesses like "UPDATE pg_authid SET
     * rolcanlogin = false;".
     */
    if IsUnderPostmaster {
        /*
         * Is role allowed to login at all?  (But background workers can
         * override this by setting bypass_login_check.)
         */
        if !bypass_login_check && !(*rform).rolcanlogin {
            // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            ereport!(
                FATAL,
                errmsg!(
                    "role \"{}\" is not permitted to log in",
                    CStr::from_ptr(rname).to_string_lossy()
                )
            );
        }

        /*
         * Check connection limit for this role.  We enforce the limit only
         * for regular backends, since other process types have their own
         * PGPROC pools.
         *
         * There is a race condition here --- we create our PGPROC before
         * checking for other PGPROCs.  If two backends did this at about the
         * same time, they might both think they were over the limit, while
         * ideally one should succeed and one fail.  Getting that to work
         * exactly seems more trouble than it is worth, however; instead we
         * just document that the connection limit is approximate.
         */
        if (*rform).rolconnlimit >= 0
            && AmRegularBackendProcess()
            && !is_superuser
            && CountUserBackends(roleid) > (*rform).rolconnlimit
        {
            // C also: errcode(ERRCODE_TOO_MANY_CONNECTIONS)
            ereport!(
                FATAL,
                errmsg!(
                    "too many connections for role \"{}\"",
                    CStr::from_ptr(rname).to_string_lossy()
                )
            );
        }
    }

    ReleaseSysCache(roleTup);
}

/*
 * Initialize user identity during special backend startup
 */
#[no_mangle]
pub unsafe extern "C" fn InitializeSessionUserIdStandalone() {
    /*
     * This function should only be called in single-user mode, in autovacuum
     * workers, in slot sync worker and in background workers.
     */
    Assert!(
        !IsUnderPostmaster
            || AmAutoVacuumWorkerProcess()
            || AmLogicalSlotSyncWorkerProcess()
            || AmBackgroundWorkerProcess()
    );

    /* call only once */
    Assert!(!OidIsValid(AuthenticatedUserId));

    AuthenticatedUserId = BOOTSTRAP_SUPERUSERID;

    /*
     * XXX Ideally we'd do this via SetConfigOption("session_authorization"),
     * but we lack the role name needed to do that, and we can't fetch it
     * because one reason for this special case is to be able to start up even
     * if something's happened to the BOOTSTRAP_SUPERUSERID's pg_authid row.
     * Since we don't set the GUC itself, C code will see the value as NULL,
     * and current_setting() will report an empty string within this session.
     */
    SetSessionAuthorization(BOOTSTRAP_SUPERUSERID, true);

    /* We could do SetConfigOption("role"), but let's be consistent */
    SetCurrentRoleId(InvalidOid, false);
}

/*
 * Initialize the system user.
 *
 * This is built as auth_method:authn_id.
 */
#[no_mangle]
pub unsafe extern "C" fn InitializeSystemUser(authn_id: *const c_char, auth_method: *const c_char) {
    /* call only once */
    Assert!(SystemUser.is_null());

    /*
     * InitializeSystemUser should be called only when authn_id is not NULL,
     * meaning that auth_method is valid.
     */
    Assert!(!authn_id.is_null());

    let system_user: *mut c_char = psprintf_2(c"%s:%s".as_ptr(), auth_method, authn_id);

    /* Store SystemUser in long-lived storage */
    SystemUser = MemoryContextStrdup(TopMemoryContext, system_user);
    pfree(system_user as *mut c_void);
}

/*
 * SQL-function SYSTEM_USER
 */
#[no_mangle]
pub unsafe extern "C" fn system_user(_fcinfo: FunctionCallInfo) -> Datum {
    let sysuser: *const c_char = GetSystemUser();

    if !sysuser.is_null() {
        PG_RETURN_DATUM!(CStringGetTextDatum(sysuser))
    } else {
        PG_RETURN_NULL!()
    }
}

/*
 * Change session auth ID while running.  (See miscinit.c for the standard
 * commutativity rationale.)
 */
#[no_mangle]
pub unsafe extern "C" fn SetSessionAuthorization(userid: Oid, is_superuser: bool) {
    SetSessionUserId(userid, is_superuser);

    if !SetRoleIsActive {
        SetOuterUserId(userid, is_superuser);
    }
}

/*
 * Report current role id
 *		This follows the semantics of SET ROLE, ie return the outer-level ID
 *		not the current effective ID, and return InvalidOid when the setting
 *		is logically SET ROLE NONE.
 */
#[no_mangle]
pub unsafe extern "C" fn GetCurrentRoleId() -> Oid {
    if SetRoleIsActive {
        OuterUserId
    } else {
        InvalidOid
    }
}

/*
 * Change Role ID while running (SET ROLE)
 *
 * If roleid is InvalidOid, we are doing SET ROLE NONE: revert to the
 * session user authorization.  In this case the is_superuser argument
 * is ignored.  (See miscinit.c for the failed-transaction caveats.)
 */
#[no_mangle]
pub unsafe extern "C" fn SetCurrentRoleId(mut roleid: Oid, mut is_superuser: bool) {
    /*
     * Get correct info if it's SET ROLE NONE
     *
     * If SessionUserId hasn't been set yet, do nothing beyond updating
     * SetRoleIsActive --- the eventual SetSessionAuthorization call will
     * update the derived state.  This is needed since we will get called
     * during GUC initialization.
     */
    if !OidIsValid(roleid) {
        SetRoleIsActive = false;

        if !OidIsValid(SessionUserId) {
            return;
        }

        roleid = SessionUserId;
        is_superuser = SessionUserIsSuperuser;
    } else {
        SetRoleIsActive = true;
    }

    SetOuterUserId(roleid, is_superuser);
}

/*
 * Get user name from user oid, returns NULL for nonexistent roleid if noerr
 * is true.
 */
#[no_mangle]
pub unsafe extern "C" fn GetUserNameFromId(roleid: Oid, noerr: bool) -> *mut c_char {
    let result: *mut c_char;

    let tuple: HeapTuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if !HeapTupleIsValid(tuple) {
        if !noerr {
            // C also: errcode(ERRCODE_UNDEFINED_OBJECT)
            ereport!(ERROR, errmsg!("invalid role OID: {}", roleid));
        }
        result = core::ptr::null_mut();
    } else {
        result = pstrdup(NameStr(&(*authid_form(tuple)).rolname));
        ReleaseSysCache(tuple);
    }
    result
}

/* ------------------------------------------------------------------------
 *				Client connection state shared with parallel workers
 *
 * ClientConnectionInfo contains pieces of information about the client that
 * need to be synced to parallel workers when they initialize.
 *-------------------------------------------------------------------------
 */

// miscadmin.h ClientConnectionInfo (defined here in miscinit.c)
#[repr(C)]
pub struct ClientConnectionInfo {
    pub authn_id: *const c_char,
    pub auth_method: UserAuth,
}

#[no_mangle]
pub static mut MyClientConnectionInfo: ClientConnectionInfo = ClientConnectionInfo {
    authn_id: core::ptr::null(),
    auth_method: 0,
};

/*
 * Intermediate representation of ClientConnectionInfo for easier
 * serialization.  Variable-length fields are allocated right after this
 * header.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct SerializedClientConnectionInfo {
    authn_id_len: i32, /* strlen(authn_id), or -1 if NULL */
    auth_method: UserAuth,
}

/*
 * Calculate the space needed to serialize MyClientConnectionInfo.
 */
#[no_mangle]
pub unsafe extern "C" fn EstimateClientConnectionInfoSpace() -> Size {
    let mut size: Size = 0;

    size = add_size(size, core::mem::size_of::<SerializedClientConnectionInfo>());

    if !MyClientConnectionInfo.authn_id.is_null() {
        size = add_size(size, libc::strlen(MyClientConnectionInfo.authn_id) + 1);
    }

    size
}

/*
 * Serialize MyClientConnectionInfo for use by parallel workers.
 */
#[no_mangle]
pub unsafe extern "C" fn SerializeClientConnectionInfo(
    mut maxsize: Size,
    mut start_address: *mut c_char,
) {
    let mut serialized = SerializedClientConnectionInfo {
        authn_id_len: 0,
        auth_method: 0,
    };

    serialized.authn_id_len = -1;
    serialized.auth_method = MyClientConnectionInfo.auth_method;

    if !MyClientConnectionInfo.authn_id.is_null() {
        serialized.authn_id_len = libc::strlen(MyClientConnectionInfo.authn_id) as i32;
    }

    /* Copy serialized representation to buffer */
    Assert!(maxsize >= core::mem::size_of::<SerializedClientConnectionInfo>());
    libc::memcpy(
        start_address as *mut c_void,
        &serialized as *const _ as *const c_void,
        core::mem::size_of::<SerializedClientConnectionInfo>(),
    );

    maxsize -= core::mem::size_of::<SerializedClientConnectionInfo>();
    start_address = start_address.add(core::mem::size_of::<SerializedClientConnectionInfo>());

    /* Copy authn_id into the space after the struct */
    if serialized.authn_id_len >= 0 {
        Assert!(maxsize >= (serialized.authn_id_len as Size + 1));
        libc::memcpy(
            start_address as *mut c_void,
            MyClientConnectionInfo.authn_id as *const c_void,
            /* include the NULL terminator to ease deserialization */
            serialized.authn_id_len as Size + 1,
        );
    }
}

/*
 * Restore MyClientConnectionInfo from its serialized representation.
 */
#[no_mangle]
pub unsafe extern "C" fn RestoreClientConnectionInfo(conninfo: *mut c_char) {
    let mut serialized = SerializedClientConnectionInfo {
        authn_id_len: 0,
        auth_method: 0,
    };

    libc::memcpy(
        &mut serialized as *mut _ as *mut c_void,
        conninfo as *const c_void,
        core::mem::size_of::<SerializedClientConnectionInfo>(),
    );

    /* Copy the fields back into place */
    MyClientConnectionInfo.authn_id = core::ptr::null();
    MyClientConnectionInfo.auth_method = serialized.auth_method;

    if serialized.authn_id_len >= 0 {
        let authn_id: *mut c_char =
            conninfo.add(core::mem::size_of::<SerializedClientConnectionInfo>());
        MyClientConnectionInfo.authn_id = MemoryContextStrdup(TopMemoryContext, authn_id);
    }
}

/*-------------------------------------------------------------------------
 *				Interlock-file support
 *
 * These routines are used to create both a data-directory lockfile
 * ($DATADIR/postmaster.pid) and Unix-socket-file lockfiles ($SOCKFILE.lock).
 * (See miscinit.c for the full header comment.)
 *-------------------------------------------------------------------------
 */

/*
 * proc_exit callback to remove lockfiles.
 */
unsafe extern "C" fn UnlinkLockFiles(_status: c_int, _arg: Datum) {
    foreach!(l, lock_files, {
        let curfile: *mut c_char = lfirst(crate::current_cell!(l)) as *mut c_char;

        libc::unlink(curfile);
        /* Should we complain if the unlink fails? */
    });
    /* Since we're about to exit, no need to reclaim storage */
    lock_files = NIL;

    /*
     * Lock file removal should always be the last externally visible action
     * of a postmaster or standalone backend, while we won't come here at all
     * when exiting postmaster child processes.  Therefore, this is a good
     * place to log completion of shutdown.  We could alternatively teach
     * proc_exit() to do it, but that seems uglier.  In a standalone backend,
     * use NOTICE elevel to be less chatty.
     */
    ereport!(
        if IsPostmasterEnvironment { LOG } else { NOTICE },
        errmsg!("database system is shut down")
    );
}

/*
 * Create a lockfile.
 *
 * filename is the path name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * socketDir is the Unix socket directory path to include (possibly empty).
 * isDDLock and refName are used to determine what error message to produce.
 */
unsafe fn CreateLockFile(
    filename: *const c_char,
    amPostmaster: bool,
    socketDir: *const c_char,
    isDDLock: bool,
    refName: *const c_char,
) {
    let mut fd: c_int;
    let mut buffer: [c_char; MAXPGPATH * 2 + 256] = [0; MAXPGPATH * 2 + 256];
    let mut ntries: c_int;
    let mut len: isize;
    let mut encoded_pid: c_int;
    let other_pid: pid_t;
    let my_pid: pid_t;
    let my_p_pid: pid_t;
    let my_gp_pid: pid_t;

    /*
     * If the PID in the lockfile is our own PID or our parent's or
     * grandparent's PID, then the file must be stale.  (See miscinit.c for the
     * full reasoning about PG_GRANDPARENT_PID and kill-test semantics.)
     */
    my_pid = libc::getpid();

    // #ifndef WIN32
    my_p_pid = libc::getppid();
    // #else my_p_pid = 0; #endif

    let envvar: *const c_char = libc::getenv(c"PG_GRANDPARENT_PID".as_ptr());
    if !envvar.is_null() {
        my_gp_pid = libc::atoi(envvar);
    } else {
        my_gp_pid = 0;
    }

    /*
     * We need a loop here because of race conditions.  But don't loop forever
     * (for example, a non-writable $PGDATA directory might cause a failure
     * that won't go away).  100 tries seems like plenty.
     */
    ntries = 0;
    'retry: loop {
        /*
         * Try to create the lock file --- O_EXCL makes this atomic.
         *
         * Think not to make the file protection weaker than 0600/0640.  See
         * comments below.
         */
        fd = libc::open(
            filename,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            pg_file_create_mode as libc::c_uint,
        );
        if fd >= 0 {
            break 'retry; /* Success; exit the retry loop */
        }

        /*
         * Couldn't create the pid file. Probably it already exists.
         */
        if (errno() != libc::EEXIST && errno() != libc::EACCES) || ntries > 100 {
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "could not create lock file \"{}\": {}",
                    CStr::from_ptr(filename).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }

        /*
         * Read the file to get the old owner's PID.  Note race condition
         * here: file might have been deleted since we tried to create it.
         */
        fd = libc::open(filename, libc::O_RDONLY, pg_file_create_mode as libc::c_uint);
        if fd < 0 {
            if errno() == libc::ENOENT {
                ntries += 1;
                continue 'retry; /* race condition; try again */
            }
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "could not open lock file \"{}\": {}",
                    CStr::from_ptr(filename).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }
        pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_READ);
        len = libc::read(
            fd,
            buffer.as_mut_ptr() as *mut c_void,
            core::mem::size_of_val(&buffer) - 1,
        );
        if len < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "could not read lock file \"{}\": {}",
                    CStr::from_ptr(filename).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }
        pgstat_report_wait_end();
        libc::close(fd);

        if len == 0 {
            // C also: errcode(ERRCODE_LOCK_FILE_EXISTS),
            // errhint("Either another server is starting, or the lock file is the remnant of a previous server startup crash.")
            ereport!(
                FATAL,
                errmsg!(
                    "lock file \"{}\" is empty",
                    CStr::from_ptr(filename).to_string_lossy()
                )
            );
        }

        buffer[len as usize] = 0;
        encoded_pid = libc::atoi(buffer.as_ptr());

        /* if pid < 0, the pid is for postgres, not postmaster */
        other_pid = (if encoded_pid < 0 {
            -encoded_pid
        } else {
            encoded_pid
        }) as pid_t;

        if other_pid <= 0 {
            elog!(
                FATAL,
                "bogus data in lock file \"{}\": \"{}\"",
                CStr::from_ptr(filename).to_string_lossy(),
                CStr::from_ptr(buffer.as_ptr()).to_string_lossy()
            );
        }

        /*
         * Check to see if the other process still exists.  (See miscinit.c for
         * the detailed EPERM/ESRCH reasoning.)
         */
        if other_pid != my_pid && other_pid != my_p_pid && other_pid != my_gp_pid {
            if libc::kill(other_pid, 0) == 0 || (errno() != libc::ESRCH && errno() != libc::EPERM) {
                /* lockfile belongs to a live process */
                // C also: errcode(ERRCODE_LOCK_FILE_EXISTS) plus the matching
                // errhint("Is another postgres/postmaster (PID %d) running/using ...")
                ereport!(
                    FATAL,
                    errmsg!(
                        "lock file \"{}\" already exists",
                        CStr::from_ptr(filename).to_string_lossy()
                    )
                );
            }
        }

        /*
         * No, the creating process did not exist.  However, it could be that
         * the postmaster crashed (or more likely was kill -9'd by a clueless
         * admin) but has left orphan backends behind.  Check for this by
         * looking to see if there is an associated shmem segment that is
         * still in use.
         *
         * Note: because postmaster.pid is written in multiple steps, we might
         * not find the shmem ID values in it; we can't treat that as an
         * error.
         */
        if isDDLock {
            let mut ptr: *mut c_char = buffer.as_mut_ptr();
            let mut id1: c_ulong = 0;
            let mut id2: c_ulong = 0;
            let mut lineno: c_int;

            lineno = 1;
            while lineno < LOCK_FILE_LINE_SHMEM_KEY {
                ptr = libc::strchr(ptr, '\n' as c_int);
                if ptr.is_null() {
                    break;
                }
                ptr = ptr.add(1);
                lineno += 1;
            }

            if !ptr.is_null()
                && libc::sscanf(ptr, c"%lu %lu".as_ptr(), &mut id1, &mut id2) == 2
            {
                if PGSharedMemoryIsInUse(id1, id2) {
                    // C also: errcode(ERRCODE_LOCK_FILE_EXISTS),
                    // errhint("Terminate any old server processes associated with data directory \"%s\".")
                    ereport!(
                        FATAL,
                        errmsg!(
                            "pre-existing shared memory block (key {}, ID {}) is still in use",
                            id1,
                            id2
                        )
                    );
                }
            }
        }

        /*
         * Looks like nobody's home.  Unlink the file and try again to create
         * it.  Need a loop because of possible race condition against other
         * would-be creators.
         */
        if libc::unlink(filename) < 0 {
            // C also: errcode_for_file_access(),
            // errhint("The file seems accidentally left over, but it could not be removed. Please remove the file by hand and try again.")
            ereport!(
                FATAL,
                errmsg!(
                    "could not remove old lock file \"{}\": {}",
                    CStr::from_ptr(filename).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }

        ntries += 1;
    }

    /*
     * Successfully created the file, now fill it.  See comment in pidfile.h
     * about the contents.  Note that we write the same first five lines into
     * both datadir and socket lockfiles; although more stuff may get added to
     * the datadir lockfile later.
     */
    libc::snprintf(
        buffer.as_mut_ptr(),
        core::mem::size_of_val(&buffer),
        c"%d\n%s\n%lld\n%d\n%s\n".as_ptr(),
        if amPostmaster {
            my_pid
        } else {
            -my_pid
        },
        DataDir,
        MyStartTime as core::ffi::c_longlong,
        PostPortNumber,
        socketDir,
    );

    /*
     * In a standalone backend, the next line (LOCK_FILE_LINE_LISTEN_ADDR)
     * will never receive data, so fill it in as empty now.
     */
    if isDDLock && !amPostmaster {
        strlcat(buffer.as_mut_ptr(), c"\n".as_ptr(), core::mem::size_of_val(&buffer));
    }

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_WRITE);
    if libc::write(
        fd,
        buffer.as_ptr() as *const c_void,
        libc::strlen(buffer.as_ptr()),
    ) != libc::strlen(buffer.as_ptr()) as isize
    {
        let save_errno: c_int = errno();

        libc::close(fd);
        libc::unlink(filename);
        /* if write didn't set errno, assume problem is no disk space */
        set_errno(if save_errno != 0 { save_errno } else { libc::ENOSPC });
        // C also: errcode_for_file_access()
        ereport!(
            FATAL,
            errmsg!(
                "could not write lock file \"{}\": {}",
                CStr::from_ptr(filename).to_string_lossy(),
                io_strerror(errno())
            )
        );
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_SYNC);
    if pg_fsync(fd) != 0 {
        let save_errno: c_int = errno();

        libc::close(fd);
        libc::unlink(filename);
        set_errno(save_errno);
        // C also: errcode_for_file_access()
        ereport!(
            FATAL,
            errmsg!(
                "could not write lock file \"{}\": {}",
                CStr::from_ptr(filename).to_string_lossy(),
                io_strerror(errno())
            )
        );
    }
    pgstat_report_wait_end();
    if libc::close(fd) != 0 {
        let save_errno: c_int = errno();

        libc::unlink(filename);
        set_errno(save_errno);
        // C also: errcode_for_file_access()
        ereport!(
            FATAL,
            errmsg!(
                "could not write lock file \"{}\": {}",
                CStr::from_ptr(filename).to_string_lossy(),
                io_strerror(errno())
            )
        );
    }

    /*
     * Arrange to unlink the lock file(s) at proc_exit.  If this is the first
     * one, set up the on_proc_exit function to do it; then add this lock file
     * to the list of files to unlink.
     */
    if lock_files == NIL {
        on_proc_exit(UnlinkLockFiles, 0);
    }

    /*
     * Use lcons so that the lock files are unlinked in reverse order of
     * creation; this is critical!
     */
    lock_files = lcons(pstrdup(filename) as *mut c_void, lock_files);
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 *
 * Note that the socket directory path line is initially written as empty.
 * postmaster.c will rewrite it upon creating the first Unix socket.
 */
#[no_mangle]
pub unsafe extern "C" fn CreateDataDirLockFile(amPostmaster: bool) {
    CreateLockFile(
        DIRECTORY_LOCK_FILE.as_ptr(),
        amPostmaster,
        c"".as_ptr(),
        true,
        DataDir,
    );
}

/*
 * Create a lockfile for the specified Unix socket file.
 */
#[no_mangle]
pub unsafe extern "C" fn CreateSocketLockFile(
    socketfile: *const c_char,
    amPostmaster: bool,
    socketDir: *const c_char,
) {
    let mut lockfile: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    libc::snprintf(
        lockfile.as_mut_ptr(),
        core::mem::size_of_val(&lockfile),
        c"%s.lock".as_ptr(),
        socketfile,
    );
    CreateLockFile(lockfile.as_ptr(), amPostmaster, socketDir, false, socketfile);
}

/*
 * TouchSocketLockFiles -- mark socket lock files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * lock files have a recent mod or access date.  That saves them
 * from being removed by overenthusiastic /tmp-directory-cleaner daemons.
 * (Another reason we should never have put the socket file in /tmp...)
 */
#[no_mangle]
pub unsafe extern "C" fn TouchSocketLockFiles() {
    foreach!(l, lock_files, {
        let socketLockFile: *mut c_char = lfirst(crate::current_cell!(l)) as *mut c_char;

        /* No need to touch the data directory lock file, we trust */
        if libc::strcmp(socketLockFile, DIRECTORY_LOCK_FILE.as_ptr()) == 0 {
            continue;
        }

        /* we just ignore any error here */
        libc::utime(socketLockFile, core::ptr::null());
    });
}

/*
 * Add (or replace) a line in the data directory lock file.
 * The given string should not include a trailing newline.
 *
 * (See miscinit.c for the non-truncation/atomicity caveat.)
 */
#[no_mangle]
pub unsafe extern "C" fn AddToDataDirLockFile(target_line: c_int, str_: *const c_char) {
    let fd: c_int;
    let mut len: isize;
    let mut lineno: c_int;
    let mut srcptr: *mut c_char;
    let mut destptr: *mut c_char;
    let mut srcbuffer: [c_char; BLCKSZ] = [0; BLCKSZ];
    let mut destbuffer: [c_char; BLCKSZ] = [0; BLCKSZ];

    fd = libc::open(DIRECTORY_LOCK_FILE.as_ptr(), libc::O_RDWR, 0);
    if fd < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not open file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
        return;
    }
    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ);
    len = libc::read(
        fd,
        srcbuffer.as_mut_ptr() as *mut c_void,
        core::mem::size_of_val(&srcbuffer) - 1,
    );
    pgstat_report_wait_end();
    if len < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not read from file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
        libc::close(fd);
        return;
    }
    srcbuffer[len as usize] = 0;

    /*
     * Advance over lines we are not supposed to rewrite, then copy them to
     * destbuffer.
     */
    srcptr = srcbuffer.as_mut_ptr();
    lineno = 1;
    while lineno < target_line {
        let eol: *mut c_char = libc::strchr(srcptr, '\n' as c_int);

        if eol.is_null() {
            break; /* not enough lines in file yet */
        }
        srcptr = eol.add(1);
        lineno += 1;
    }
    libc::memcpy(
        destbuffer.as_mut_ptr() as *mut c_void,
        srcbuffer.as_ptr() as *const c_void,
        srcptr.offset_from(srcbuffer.as_ptr()) as usize,
    );
    destptr = destbuffer
        .as_mut_ptr()
        .offset(srcptr.offset_from(srcbuffer.as_ptr()));

    /*
     * Fill in any missing lines before the target line, in case lines are
     * added to the file out of order.
     */
    while lineno < target_line {
        if destptr < destbuffer.as_mut_ptr().add(core::mem::size_of_val(&destbuffer)) {
            *destptr = '\n' as c_char;
            destptr = destptr.add(1);
        }
        lineno += 1;
    }

    /*
     * Write or rewrite the target line.
     */
    libc::snprintf(
        destptr,
        destbuffer
            .as_mut_ptr()
            .add(core::mem::size_of_val(&destbuffer))
            .offset_from(destptr) as usize,
        c"%s\n".as_ptr(),
        str_,
    );
    destptr = destptr.add(libc::strlen(destptr));

    /*
     * If there are more lines in the old file, append them to destbuffer.
     */
    srcptr = libc::strchr(srcptr, '\n' as c_int);
    if !srcptr.is_null() {
        srcptr = srcptr.add(1);
        libc::snprintf(
            destptr,
            destbuffer
                .as_mut_ptr()
                .add(core::mem::size_of_val(&destbuffer))
                .offset_from(destptr) as usize,
            c"%s".as_ptr(),
            srcptr,
        );
    }

    /*
     * And rewrite the data.  Since we write in a single kernel call, this
     * update should appear atomic to onlookers.
     */
    len = libc::strlen(destbuffer.as_ptr()) as isize;
    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE);
    if libc::pwrite(fd, destbuffer.as_ptr() as *const c_void, len as usize, 0) != len {
        pgstat_report_wait_end();
        /* if write didn't set errno, assume problem is no disk space */
        if errno() == 0 {
            set_errno(libc::ENOSPC);
        }
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not write to file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
        libc::close(fd);
        return;
    }
    pgstat_report_wait_end();
    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC);
    if pg_fsync(fd) != 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not write to file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
    }
    pgstat_report_wait_end();
    if libc::close(fd) != 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not write to file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
    }
}

/*
 * Recheck that the data directory lock file still exists with expected
 * content.  Return true if the lock file appears OK, false if it isn't.
 *
 * (See miscinit.c for the false-positive avoidance rationale.)
 */
#[no_mangle]
pub unsafe extern "C" fn RecheckDataDirLockFile() -> bool {
    let fd: c_int;
    let len: isize;
    let file_pid: c_long;
    let mut buffer: [c_char; BLCKSZ] = [0; BLCKSZ];

    fd = libc::open(DIRECTORY_LOCK_FILE.as_ptr(), libc::O_RDWR, 0);
    if fd < 0 {
        /*
         * There are many foreseeable false-positive error conditions.  For
         * safety, fail only on enumerated clearly-something-is-wrong
         * conditions.
         */
        match errno() {
            libc::ENOENT | libc::ENOTDIR => {
                /* disaster */
                // C also: errcode_for_file_access()
                ereport!(
                    LOG,
                    errmsg!(
                        "could not open file \"{}\": {}",
                        DIRECTORY_LOCK_FILE.to_string_lossy(),
                        io_strerror(errno())
                    )
                );
                return false;
            }
            _ => {
                /* non-fatal, at least for now */
                // C also: errcode_for_file_access()
                ereport!(
                    LOG,
                    errmsg!(
                        "could not open file \"{}\": {}; continuing anyway",
                        DIRECTORY_LOCK_FILE.to_string_lossy(),
                        io_strerror(errno())
                    )
                );
                return true;
            }
        }
    }
    pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ);
    len = libc::read(
        fd,
        buffer.as_mut_ptr() as *mut c_void,
        core::mem::size_of_val(&buffer) - 1,
    );
    pgstat_report_wait_end();
    if len < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not read from file \"{}\": {}",
                DIRECTORY_LOCK_FILE.to_string_lossy(),
                io_strerror(errno())
            )
        );
        libc::close(fd);
        return true; /* treat read failure as nonfatal */
    }
    buffer[len as usize] = 0;
    libc::close(fd);
    file_pid = libc::atol(buffer.as_ptr());
    if file_pid == libc::getpid() as c_long {
        return true; /* all is well */
    }

    /* Trouble: someone's overwritten the lock file */
    ereport!(
        LOG,
        errmsg!(
            "lock file \"{}\" contains wrong PID: {} instead of {}",
            DIRECTORY_LOCK_FILE.to_string_lossy(),
            file_pid,
            libc::getpid() as c_long
        )
    );
    false
}

/*-------------------------------------------------------------------------
 *				Version checking support
 *-------------------------------------------------------------------------
 */

/*
 * Determine whether the PG_VERSION file in directory `path' indicates
 * a data version compatible with the version of this program.
 *
 * If compatible, return. Otherwise, ereport(FATAL).
 */
#[no_mangle]
pub unsafe extern "C" fn ValidatePgVersion(path: *const c_char) {
    let mut full_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let ret: c_int;
    let file_major: c_long;
    let my_major: c_long;
    let mut endptr: *mut c_char = core::ptr::null_mut();
    let mut file_version_string: [c_char; 64] = [0; 64];
    let my_version_string = std::ffi::CString::new(PG_VERSION).unwrap();

    my_major = libc::strtol(my_version_string.as_ptr(), &mut endptr, 10);

    libc::snprintf(
        full_path.as_mut_ptr(),
        core::mem::size_of_val(&full_path),
        c"%s/PG_VERSION".as_ptr(),
        path,
    );

    let file: *mut c_void = AllocateFile(full_path.as_ptr(), c"r".as_ptr());
    if file.is_null() {
        if errno() == libc::ENOENT {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            // errdetail("File \"%s\" is missing.", full_path)
            ereport!(
                FATAL,
                errmsg!(
                    "\"{}\" is not a valid data directory",
                    CStr::from_ptr(path).to_string_lossy()
                )
            );
        } else {
            // C also: errcode_for_file_access()
            ereport!(
                FATAL,
                errmsg!(
                    "could not open file \"{}\": {}",
                    CStr::from_ptr(full_path.as_ptr()).to_string_lossy(),
                    io_strerror(errno())
                )
            );
        }
    }

    file_version_string[0] = 0;
    ret = libc::fscanf(
        file as *mut libc::FILE,
        c"%63s".as_ptr(),
        file_version_string.as_mut_ptr(),
    );
    file_major = libc::strtol(file_version_string.as_ptr(), &mut endptr, 10);

    if ret != 1 || endptr == file_version_string.as_mut_ptr() {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        // errdetail("File \"%s\" does not contain valid data.", full_path),
        // errhint("You might need to initdb.")
        ereport!(
            FATAL,
            errmsg!(
                "\"{}\" is not a valid data directory",
                CStr::from_ptr(path).to_string_lossy()
            )
        );
    }

    FreeFile(file);

    if my_major != file_major {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        // errdetail("The data directory was initialized by PostgreSQL version %s, which is not compatible with this version %s.", file_version_string, my_version_string)
        ereport!(
            FATAL,
            errmsg!("database files are incompatible with server")
        );
    }
}

/*-------------------------------------------------------------------------
 *				Library preload support
 *-------------------------------------------------------------------------
 */

/*
 * GUC variables: lists of library names to be preloaded at postmaster
 * start and at backend start
 */
#[no_mangle]
pub static mut session_preload_libraries_string: *mut c_char = core::ptr::null_mut();
#[no_mangle]
pub static mut shared_preload_libraries_string: *mut c_char = core::ptr::null_mut();
#[no_mangle]
pub static mut local_preload_libraries_string: *mut c_char = core::ptr::null_mut();

/* Flag telling that we are loading shared_preload_libraries */
#[no_mangle]
pub static mut process_shared_preload_libraries_in_progress: bool = false;
#[no_mangle]
pub static mut process_shared_preload_libraries_done: bool = false;

// shmem_request_hook_type (storage/ipc.h)
pub type shmem_request_hook_type = Option<unsafe extern "C" fn()>;

#[no_mangle]
pub static mut shmem_request_hook: shmem_request_hook_type = None;
#[no_mangle]
pub static mut process_shmem_requests_in_progress: bool = false;

/*
 * load the shared libraries listed in 'libraries'
 *
 * 'gucname': name of GUC variable, for error reports
 * 'restricted': if true, force libraries to be in $libdir/plugins/
 */
unsafe fn load_libraries(libraries: *const c_char, gucname: *const c_char, restricted: bool) {
    let rawstring: *mut c_char;
    let mut elemlist: *mut List = NIL;

    if libraries.is_null() || *libraries == 0 {
        return; /* nothing to do */
    }

    /* Need a modifiable copy of string */
    rawstring = pstrdup(libraries);

    /* Parse string into list of filename paths */
    if !SplitDirectoriesString(rawstring, ',' as c_char, &mut elemlist) {
        /* syntax error in list */
        list_free_deep(elemlist);
        pfree(rawstring as *mut c_void);
        // C also: errcode(ERRCODE_SYNTAX_ERROR)
        ereport!(
            LOG,
            errmsg!(
                "invalid list syntax in parameter \"{}\"",
                CStr::from_ptr(gucname).to_string_lossy()
            )
        );
        return;
    }

    foreach!(l, elemlist, {
        /* Note that filename was already canonicalized */
        let mut filename: *mut c_char = lfirst(crate::current_cell!(l)) as *mut c_char;
        let mut expanded: *mut c_char = core::ptr::null_mut();

        /* If restricting, insert $libdir/plugins if not mentioned already */
        if restricted && first_dir_separator(filename).is_null() {
            expanded = psprintf_2(c"$libdir/plugins/%s".as_ptr(), c"".as_ptr(), filename);
            filename = expanded;
        }
        load_file(filename, restricted);
        ereport!(
            DEBUG1,
            errmsg_internal!(
                "loaded library \"{}\"",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
        if !expanded.is_null() {
            pfree(expanded as *mut c_void);
        }
    });

    list_free_deep(elemlist);
    pfree(rawstring as *mut c_void);
}

/*
 * process any libraries that should be preloaded at postmaster start
 */
#[no_mangle]
pub unsafe extern "C" fn process_shared_preload_libraries() {
    process_shared_preload_libraries_in_progress = true;
    load_libraries(
        shared_preload_libraries_string,
        c"shared_preload_libraries".as_ptr(),
        false,
    );
    process_shared_preload_libraries_in_progress = false;
    process_shared_preload_libraries_done = true;
}

/*
 * process any libraries that should be preloaded at backend start
 */
#[no_mangle]
pub unsafe extern "C" fn process_session_preload_libraries() {
    load_libraries(
        session_preload_libraries_string,
        c"session_preload_libraries".as_ptr(),
        false,
    );
    load_libraries(
        local_preload_libraries_string,
        c"local_preload_libraries".as_ptr(),
        true,
    );
}

/*
 * process any shared memory requests from preloaded libraries
 */
#[no_mangle]
pub unsafe extern "C" fn process_shmem_requests() {
    process_shmem_requests_in_progress = true;
    if let Some(hook) = shmem_request_hook {
        hook();
    }
    process_shmem_requests_in_progress = false;
}

#[no_mangle]
pub unsafe extern "C" fn pg_bindtextdomain(_domain: *const c_char) {
    // #ifdef ENABLE_NLS
    // if (my_exec_path[0] != '\0')
    // {
    //     char locale_path[MAXPGPATH];
    //     get_locale_path(my_exec_path, locale_path);
    //     bindtextdomain(domain, locale_path);
    //     pg_bind_textdomain_codeset(domain);
    // }
    // #endif
}

/*-------------------------------------------------------------------------
 *
 * postmaster.rs
 *   This program acts as a clearing house for requests to the
 *   POSTGRES system.  Frontend programs connect to the Postmaster,
 *   and postmaster forks a new backend process to handle the
 *   connection.
 *
 *   The postmaster also manages system-wide operations such as
 *   startup and shutdown. The postmaster itself doesn't do those
 *   operations, mind you --- it just forks off a subprocess to do them
 *   at the right times.  It also takes care of resetting the system
 *   if a backend crashes.
 *
 *   The postmaster process creates the shared memory and semaphore
 *   pools during startup, but as a rule does not touch them itself.
 *   In particular, it is not a member of the PGPROC array of backends
 *   and so it cannot participate in lock-manager operations.  Keeping
 *   the postmaster away from shared memory operations makes it simpler
 *   and more reliable.  The postmaster is almost always able to recover
 *   from crashes of individual backends by resetting shared memory;
 *   if it did much with shared memory then it would be prone to crashing
 *   along with the backends.
 *
 *   When a request message is received, we now fork() immediately.
 *   The child process performs authentication of the request, and
 *   then becomes a backend if successful.  This allows the auth code
 *   to be written in a simple single-threaded style (as opposed to the
 *   crufty "poor man's multitasking" code that used to be needed).
 *   More importantly, it ensures that blockages in non-multithreaded
 *   libraries like SSL or PAM cannot cause denial of service to other
 *   clients.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/postmaster/postmaster.c
 *
 * NOTES
 *
 * Initialization:
 *     The Postmaster sets up shared memory data structures
 *     for the backends.
 *
 * Synchronization:
 *     The Postmaster shares memory with the backends but should avoid
 *     touching shared memory, so as not to become stuck if a crashing
 *     backend screws up locks or shared memory.  Likewise, the Postmaster
 *     should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *     The Postmaster cleans up after backends if they have an emergency
 *     exit and/or core dump.
 *
 * Error Reporting:
 *     Use write_stderr() only for reporting "interactive" errors
 *     (essentially, bogus arguments on the command line).  Once the
 *     postmaster is launched, use ereport().
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify};

use core::ffi::{c_char, c_int, c_void};
use std::ffi::CStr;

use crate::lib::ilist::{dlist_head, dlist_iter, dlist_mutable_iter};
use crate::miscadmin::{
    BackendType, BACKEND_NUM_TYPES,
    B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND,
    B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND,
    B_INVALID, B_IO_WORKER, B_LOGGER, B_SLOTSYNC_WORKER,
    B_STANDALONE_BACKEND, B_STARTUP, B_WAL_RECEIVER, B_WAL_SENDER,
    B_WAL_SUMMARIZER, B_WAL_WRITER,
    IsBinaryUpgrade, IsPostmasterEnvironment, IsUnderPostmaster,
    MyBackendType, MyProcPid, TimestampTz,
};
use crate::pg_config_manual::MAXPGPATH;
use crate::postmaster::bgworker::{
    BackgroundWorker, BgWorkerStartTime,
    BgWorkerStart_ConsistentState, BgWorkerStart_PostmasterStart,
    BgWorkerStart_RecoveryFinished,
    BGW_NEVER_RESTART,
};
use crate::postmaster::bgworker_internals::RegisteredBgWorker;
use crate::libpq::libpq_be::ClientSocket;
use crate::postmaster::launch_backend::{
    BackendStartupData, PostmasterChildName,
    postmaster_child_launch,
};
use crate::postmaster::pgarch::PgArchCanRestart;
use crate::postmaster::pmchild::{
    ActiveChildList, AllocDeadEndChild, AssignPostmasterChildSlot,
    FindPostmasterChildByPid, InitPostmasterChildSlots, PMChild,
    ReleasePostmasterChildSlot,
};
use crate::nodes::execnodes::WaitEventSet;
use crate::storage::ipc::latch::{
    Latch, ResetLatch, SetLatch, WaitEvent,
    WL_LATCH_SET, WL_SOCKET_ACCEPT,
};
use crate::storage::ipc::pmsignal::{
    CheckPostmasterSignal, IsPostmasterChildWalSender, PMSignalReason,
    SendPostmasterSignal, SetQuitSignalReason,
};

/*
 * pmsignal.h: PMSignalReason enum values.  The pmsignal module models the
 * enum as a plain c_int index and does not export named constants, so the
 * 1:1 values from the C header are defined locally here.
 */
const PMSIGNAL_RECOVERY_STARTED: PMSignalReason = 0;
const PMSIGNAL_RECOVERY_CONSISTENT: PMSignalReason = 1;
const PMSIGNAL_BEGIN_HOT_STANDBY: PMSignalReason = 2;
const PMSIGNAL_ROTATE_LOGFILE: PMSignalReason = 3;
const PMSIGNAL_START_AUTOVAC_LAUNCHER: PMSignalReason = 4;
const PMSIGNAL_START_AUTOVAC_WORKER: PMSignalReason = 5;
const PMSIGNAL_BACKGROUND_WORKER_CHANGE: PMSignalReason = 6;
const PMSIGNAL_START_WALRECEIVER: PMSignalReason = 7;
const PMSIGNAL_ADVANCE_STATE_MACHINE: PMSignalReason = 8;
const PMSIGNAL_XLOG_IS_SHUTDOWN: PMSignalReason = 9;

/* pmsignal.h: QuitSignalReason enum values. */
const PMQUIT_FOR_CRASH: c_int = 1;
const PMQUIT_FOR_STOP: c_int = 2;
use crate::tcop::backend_startup::{
    CAC_state, CAC_OK, CAC_RECOVERY, CAC_NOTHOTSTANDBY,
    CAC_SHUTDOWN, CAC_STARTUP, CAC_TOOMANY,
};
use crate::miscadmin::{AddToDataDirLockFile, RecheckDataDirLockFile};
use crate::utils::pidfile::{
    LOCK_FILE_LINE_LISTEN_ADDR, LOCK_FILE_LINE_PM_STATUS,
    LOCK_FILE_LINE_SOCKET_DIR,
    PM_STATUS_READY, PM_STATUS_STANDBY, PM_STATUS_STARTING, PM_STATUS_STOPPING,
};
use crate::utils::mmgr::mcxt::PostmasterContext;

/* Imports for misc globals */
#[allow(improper_ctypes)]
extern "C" {
    /* miscadmin.h globals */
    pub static mut PostmasterPid: c_int;
    pub static mut MyLatch: *mut c_void;

    /* guc.h */
    static mut MaxConnections: c_int;
    static mut SuperuserReservedConnections: c_int;
    static mut ReservedConnections: c_int;
    static mut Logging_collector: bool;
    static mut EnableHotStandby: bool;
    static mut Log_destination: c_int;
    static mut Log_destination_string: *const c_char;
    static mut max_wal_senders: c_int;
    static mut wal_level: c_int;
    static mut XLogArchiveMode: c_int;
    static mut io_workers: c_int;
    static mut sync_replication_slots: bool;
    static mut summarize_wal: bool;

    /* access/xlog.h / access/xlogrecovery.h */
    static mut reachedConsistency: bool;

    /* pgstat.h */
    fn pgstat_get_crashed_backend_activity(
        pid: c_int,
        buffer: *mut c_char,
        buflen: c_int,
    ) -> *const c_char;

    /* autovacuum.h */
    fn autovac_init();
    fn AutoVacuumingActive() -> bool;
    fn AutoVacWorkerFailed();

    /* libpq/libpq.h */
    fn ListenServerPort(
        family: c_int,
        hostName: *const c_char,
        portNumber: libc::in_port_t,
        unixSocketDir: *const c_char,
        ListenSockets: *mut pgsocket,
        NumListenSockets: *mut c_int,
        MaxListen: c_int,
    ) -> c_int;
    fn RemoveSocketFiles();
    fn TouchSocketFiles();
    fn TouchSocketLockFiles();
    fn AcceptConnection(server_fd: pgsocket, client_sock: *mut ClientSocket) -> c_int;
    fn pg_set_noblock(sock: pgsocket) -> bool;
    fn SplitGUCList(
        rawstring: *mut c_char,
        separator: c_char,
        namelist: *mut *mut List,
    ) -> bool;
    fn SplitDirectoriesString(
        rawstring: *mut c_char,
        separator: c_char,
        namelist: *mut *mut List,
    ) -> bool;

    /* storage/waiteventset.h (stubbed here) */
    fn CreateWaitEventSet(context: *mut c_void, nevents: c_int) -> *mut WaitEventSet;
    fn FreeWaitEventSet(set: *mut WaitEventSet);
    fn FreeWaitEventSetAfterFork(set: *mut WaitEventSet);
    fn AddWaitEventToSet(
        set: *mut WaitEventSet,
        events: uint32,
        fd: pgsocket,
        latch: *mut Latch,
        user_data: *mut c_void,
    ) -> c_int;
    fn WaitEventSetWait(
        set: *mut WaitEventSet,
        timeout: c_int,
        occurred_events: *mut WaitEvent,
        nevents: c_int,
        wait_event_info: uint32,
    ) -> c_int;

    /* utils/timestamp.h */
    fn GetCurrentTimestamp() -> TimestampTz;
    fn TimestampTzPlusMilliseconds(t: TimestampTz, ms: i64) -> TimestampTz;
    fn TimestampDifferenceMilliseconds(start: TimestampTz, stop: TimestampTz) -> i64;
    fn TimestampDifferenceExceeds(
        start: TimestampTz,
        stop: TimestampTz,
        msec: c_int,
    ) -> bool;
    fn timestamptz_to_time_t(t: TimestampTz) -> libc::time_t;

    /* utils/pg_prng.h */
    fn pg_prng_strong_seed(state: *mut PgPrngState) -> bool;
    fn pg_prng_seed(state: *mut PgPrngState, seed: u64);
    fn pg_prng_uint32(state: *mut PgPrngState) -> u32;
    static mut pg_global_prng_state: PgPrngState;

    /* access/xlog.h */
    fn LocalProcessControlFile(reset: bool);
    fn XLogArchivingActive() -> bool;
    fn XLogArchivingAlways() -> bool;

    /* postmaster/bgworker.c internals */
    fn BackgroundWorkerList() -> *mut dlist_head; /* static in bgworker.c - stub */
    fn ForgetBackgroundWorker(rw: *mut RegisteredBgWorker);
    fn ForgetUnstartedBackgroundWorkers();
    fn ResetBackgroundWorkerCrashTimes();
    fn BackgroundWorkerStateChange(allow: bool);
    fn BackgroundWorkerStopNotifications(pid: c_int);
    fn ReportBackgroundWorkerPID(rw: *mut RegisteredBgWorker);
    fn ReportBackgroundWorkerExit(rw: *mut RegisteredBgWorker);

    /* postmaster/syslogger.h */
    fn SysLogger_Start(child_slot: c_int) -> c_int;
    static mut syslogPipe: [c_int; 2];

    /* replication/slotsync.h */
    fn ValidateSlotSyncParams(elevel: c_int) -> bool;
    fn SlotSyncWorkerCanRestart() -> bool;

    /* storage/fd.h */
    fn AllocateDir(dirname: *const c_char) -> *mut c_void;
    fn FreeDir(dir: *mut c_void);
    fn AllocateFile(filename: *const c_char, mode: *const c_char) -> *mut libc::FILE;
    fn FreeFile(fp: *mut libc::FILE) -> c_int;
    fn set_max_safe_fds();
    fn ReleaseExternalFD();
    fn ReserveExternalFD();
    fn RemovePgTempFiles();

    /* storage/ipc.h */
    fn shmem_exit(code: c_int);
    fn on_proc_exit(f: unsafe extern "C" fn(c_int, Datum), arg: Datum);
    fn proc_exit(code: c_int) -> !;

    /* storage/pmsignal.h */
    fn CheckLogrotateSignal() -> bool;
    fn CheckPromoteSignal() -> bool;
    fn RemoveLogrotateSignalFiles();
    fn RemovePromoteSignalFiles();

    /* storage/proc.h */
    fn CreateSharedMemoryAndSemaphores();

    /* storage/aio_subsys.h */
    fn pgaio_workers_enabled() -> bool;

    /* utils/guc.h */
    fn InitializeGUCOptions();
    fn ProcessConfigFile(context: GucContext);
    fn SetConfigOption(name: *const c_char, value: *const c_char, context: GucContext, source: GucSource);
    fn GetConfigOption(name: *const c_char, missing_ok: bool, restrict_superuser: bool) -> *const c_char;
    fn GetConfigOptionFlags(name: *const c_char, missing_ok: bool) -> c_int;
    fn ParseLongOption(
        string: *const c_char,
        name: *mut *mut c_char,
        value: *mut *mut c_char,
    );
    fn SelectConfigFiles(userDoption: *const c_char, progname: *const c_char) -> bool;
    fn set_debug_options(debug_flag: c_int, context: GucContext, source: GucSource);
    fn set_plan_disabling_options(
        arg: *const c_char,
        context: GucContext,
        source: GucSource,
    ) -> bool;
    fn get_stats_option_name(arg: *const c_char) -> *const c_char;
    fn parse_dispatch_option(optarg: *const c_char) -> c_int;
    static mut opterr: c_int;
    static mut optind: c_int;

    /* miscadmin.h */
    fn InitProcessGlobals_impl();  /* see InitProcessGlobals below */
    fn InitializeWaitEventSupport();
    fn InitProcessLocalLatch();
    fn pqinitmask();
    fn checkDataDir();
    fn ChangeToDataDir();
    fn CreateDataDirLockFile(amPostmaster: bool);
    fn CheckDateTokenTables() -> bool;
    fn GetBackendTypeDesc(btype: BackendType) -> *const c_char;
    fn InitializeMaxBackends();
    fn InitializeFastPathLocks();
    fn process_shared_preload_libraries();
    fn process_shmem_requests();
    fn InitializeShmemGUCs();
    fn InitializeWalConsistencyChecking();
    fn ApplyLauncherRegister();
    fn load_hba() -> bool;
    fn load_ident() -> bool;
    fn find_my_exec(argv0: *const c_char, retpath: *mut c_char) -> c_int;
    fn get_pkglib_path(my_exec_path: *const c_char, ret: *mut c_char);
    fn message_level_is_interesting(level: c_int) -> bool;
    fn write_stderr(fmt: *const c_char, ...);
    fn getopt(argc: c_int, argv: *mut *mut c_char, optstring: *const c_char) -> c_int;
    static mut optarg: *mut c_char;

    /* utils/varlena.h */
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn pfree(ptr: *mut c_void);

    /* utils/memutils.h */
    fn AllocSetContextCreate(
        parent: MemoryContext,
        name: *const c_char,
        minContextSize: Size,
        initBlockSize: Size,
        maxBlockSize: Size,
    ) -> MemoryContext;
    fn MemoryContextSwitchTo(cxt: MemoryContext) -> MemoryContext;
    static mut TopMemoryContext: MemoryContext;

    /* utils/datetime.h */
    fn ALLOCSET_DEFAULT_SIZES() -> (Size, Size, Size);  /* macro - stub*/

    /* utils/pidfile.h */
    fn CreateDataDirLockFile_inner();

    /* pgstat.h */
    fn PgStartTime_set(ts: TimestampTz);

    /* lib/stringinfo.h */
    fn initStringInfo(str_: *mut StringInfoData);
    fn appendStringInfo(str_: *mut StringInfoData, fmt: *const c_char, ...);
    fn appendStringInfoString(str_: *mut StringInfoData, s: *const c_char);

    /* XLOG */
    fn XLOG_CONTROL_FILE() -> *const c_char;

    /* from globals.c */
    pub static mut DataDir: *mut c_char;
    pub static mut my_exec_path: [c_char; MAXPGPATH];
    pub static mut pkglib_path: [c_char; MAXPGPATH];
    pub static mut external_pid_file: *mut c_char;
    pub static mut progname: *const c_char;
    pub static mut PgStartTime: TimestampTz;
    pub static mut MyStartTimestamp: TimestampTz;
    pub static mut MyStartTime: libc::time_t;
    pub static mut HbaFileName: *mut c_char;
    pub static mut IdentFileName: *mut c_char;
    pub static mut LOG_METAINFO_DATAFILE: *const c_char;
    pub static mut PG_BINARY_R: *const c_char;
    pub static mut PG_VERSION_STR: *const c_char;
    pub static mut EnableSSL: bool;
    pub static mut PreAuthDelay: c_int;
    pub static mut AuthenticationTimeout: c_int;
    pub static mut log_hostname: bool;
    pub static mut enable_bonjour: bool;
    pub static mut bonjour_name: *mut c_char;
    pub static mut restart_after_crash: bool;
    pub static mut remove_temp_files_after_crash: bool;
    pub static mut send_abort_for_crash: bool;
    pub static mut send_abort_for_kill: bool;
    pub static mut ClientAuthInProgress: bool;
    pub static mut redirection_done: bool;

    /* GUC constants */
    pub static GUC_RUNTIME_COMPUTED: c_int;
    pub static DISPATCH_POSTMASTER: c_int;
    pub static AF_UNSPEC: c_int;
    pub static AF_UNIX: c_int;
    pub static LOG_DESTINATION_STDERR: c_int;
    pub static WAL_LEVEL_MINIMAL: c_int;
    pub static ARCHIVE_MODE_OFF: c_int;
    pub static STATUS_OK: c_int;
    pub static STATUS_ERROR: c_int;
    pub static DEF_PGPORT: c_int;

    /* signal */
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
    fn pqsignal(signo: c_int, handler: Option<unsafe extern "C" fn(c_int)>);
    fn kill(pid: c_int, sig: c_int) -> c_int;
    fn waitpid_sys(pid: c_int, stat_loc: *mut c_int, options: c_int) -> c_int;
    fn srandom(seed: c_uint);

    static BlockSig: sigset_t;
    static UnBlockSig: sigset_t;

    /* secure_initialize (ssl) */
    fn secure_initialize(isServerStart: bool) -> c_int;
    fn secure_destroy();
}

/* ----------------------------------------------------------------
 * Types opaque / stubs not yet ported
 * ---------------------------------------------------------------- */
/// TODO(pg-port): nodes/pg_list.h List
pub type List = c_void;
/// TODO(pg-port): nodes/pg_list.h ListCell
pub type ListCell = c_void;
pub use crate::utils::mmgr::memnodes::{MemoryContext, MemoryContextData};
/// TODO(pg-port): utils/palloc.h Datum
pub type Datum = usize;
/// TODO(pg-port): utils/palloc.h Size
pub type Size = usize;
pub use crate::lib::stringinfo::StringInfoData;
/// TODO(pg-port): common/pg_prng.h PgPrngState
#[repr(C)]
pub struct PgPrngState {
    _opaque: [u64; 4],
}
/// TODO(pg-port): utils/guc.h GucContext
pub type GucContext = c_int;
/// TODO(pg-port): utils/guc.h GucSource
pub type GucSource = c_int;
/// TODO(pg-port): libpq/pqsignal.h sigset_t
pub type sigset_t = c_void;
/// TODO(pg-port): port.h pgsocket
pub type pgsocket = c_int;

/* GUC context constants (utils/guc.h) -- forward decls */
pub const PGC_POSTMASTER: GucContext = 0;
pub const PGC_SUSET: GucContext = 4;
pub const PGC_SIGHUP: GucContext = 1;
pub const PGC_S_ARGV: GucSource = 1;
pub const PGC_S_OVERRIDE: GucSource = 7;

pub const PGINVALID_SOCKET: pgsocket = -1;
pub const MAXLISTEN: c_int = 64;
const SIGKILL_CHILDREN_AFTER_SECS: libc::time_t = 5;
const SECS_PER_MINUTE: libc::time_t = 60;
const MAX_IO_WORKERS: usize = 32; /* storage/io_worker.h */

/* Signal constants (POSIX - Darwin) */
pub const SIGHUP: c_int = 1;
pub const SIGINT: c_int = 2;
pub const SIGQUIT: c_int = 3;
pub const SIGTERM: c_int = 15;
pub const SIGALRM: c_int = 14;
pub const SIGPIPE: c_int = 13;
pub const SIGUSR1: c_int = 30;
pub const SIGUSR2: c_int = 31;
pub const SIGCHLD: c_int = 20;
pub const SIGABRT: c_int = 6;
pub const SIGKILL: c_int = 9;
pub const SIGTTIN: c_int = 21;
pub const SIGTTOU: c_int = 22;
pub const SIGXFSZ: c_int = 25;
pub const SIG_IGN: Option<unsafe extern "C" fn(c_int)> = None;

/* macros to check exit status */
#[inline]
fn wifexited(st: c_int) -> bool { (st & 0x7f) == 0 }
#[inline]
fn wexitstatus(st: c_int) -> c_int { (st >> 8) & 0xff }
#[inline]
fn wifsignaled(st: c_int) -> bool { (st & 0x7f) != 0 && (st & 0x7f) != 0x7f }
#[inline]
fn wtermsig(st: c_int) -> c_int { st & 0x7f }

#[inline]
fn exit_status_0(st: c_int) -> bool { st == 0 }
#[inline]
fn exit_status_1(st: c_int) -> bool { wifexited(st) && wexitstatus(st) == 1 }
#[inline]
fn exit_status_3(st: c_int) -> bool { wifexited(st) && wexitstatus(st) == 3 }

pub const WNOHANG: c_int = 1;
pub const SIG_SETMASK: c_int = 3;

/* NIL list */
pub const NIL: *mut List = core::ptr::null_mut();

/* ----------------------------------------------------------------
 * BackendTypeMask
 * ---------------------------------------------------------------- */

/*
 * CountChildren and SignalChildren take a bitmask argument to represent
 * BackendTypes to count or signal.  Define a separate type and functions to
 * work with the bitmasks, to avoid accidentally passing a plain BackendType
 * in place of a bitmask or vice versa.
 */
#[derive(Copy, Clone)]
pub struct BackendTypeMask {
    pub mask: u32,
}

/* StaticAssertDecl(BACKEND_NUM_TYPES < 32, "too many backend types for uint32"); */

pub static BTYPE_MASK_ALL: BackendTypeMask =
    BackendTypeMask { mask: (1u32 << BACKEND_NUM_TYPES) - 1 };
pub static BTYPE_MASK_NONE: BackendTypeMask = BackendTypeMask { mask: 0 };

#[inline]
fn btmask(t: BackendType) -> BackendTypeMask {
    BackendTypeMask { mask: 1 << (t as u32) }
}

#[inline]
fn btmask_del(mut mask: BackendTypeMask, t: BackendType) -> BackendTypeMask {
    mask.mask &= !(1 << (t as u32));
    mask
}

/* btmask_add - variadic in C; replicate with a slice helper */
#[inline]
fn btmask_add_slice(mut mask: BackendTypeMask, types: &[BackendType]) -> BackendTypeMask {
    for &t in types {
        mask.mask |= 1 << (t as u32);
    }
    mask
}

/*
 * bgworker_internals.rs models RegisteredBgWorker.rw_worker as an opaque
 * c_void (the BackgroundWorker struct is not yet wired into that module).
 * This helper reinterprets the inline field as the real BackgroundWorker so
 * its fields can be accessed 1:1 with the C code.
 */
#[inline]
unsafe fn rw_bgworker(rw: *const RegisteredBgWorker) -> *const BackgroundWorker {
    &(*rw).rw_worker as *const _ as *const c_void as *const BackgroundWorker
}

#[inline]
fn btmask_all_except_slice(types: &[BackendType]) -> BackendTypeMask {
    /* BACKEND_NUM_TYPES drives the "all" mask - use the runtime value */
    let all = BackendTypeMask { mask: (1u32 << (BACKEND_NUM_TYPES as u32)) - 1 };
    let mut mask = all;
    for &t in types {
        mask = btmask_del(mask, t);
    }
    mask
}

#[inline]
fn btmask_contains(mask: BackendTypeMask, t: BackendType) -> bool {
    (mask.mask & (1 << (t as u32))) != 0
}

/* ----------------------------------------------------------------
 * Global variables (public)
 * ---------------------------------------------------------------- */

pub static mut MyBgworkerEntry: *mut BackgroundWorker = core::ptr::null_mut();

/* The socket number we are listening for connections on */
pub static mut PostPortNumber: c_int = 0; /* set to DEF_PGPORT at startup */

/* The directory names for Unix socket(s) */
pub static mut Unix_socket_directories: *mut c_char = core::ptr::null_mut();

/* The TCP listen address(es) */
pub static mut ListenAddresses: *mut c_char = core::ptr::null_mut();

pub static mut SuperuserReservedConnections_var: c_int = 0;
pub static mut ReservedConnections_var: c_int = 0;

/* The socket(s) we're listening to. */
static mut NumListenSockets: c_int = 0;
static mut ListenSockets: *mut pgsocket = core::ptr::null_mut();

pub static mut EnableSSL_var: bool = false;
pub static mut PreAuthDelay_var: c_int = 0;
pub static mut AuthenticationTimeout_var: c_int = 60;
pub static mut log_hostname_var: bool = false;
pub static mut enable_bonjour_var: bool = false;
pub static mut bonjour_name_var: *mut c_char = core::ptr::null_mut();
pub static mut restart_after_crash_var: bool = true;
pub static mut remove_temp_files_after_crash_var: bool = true;
pub static mut send_abort_for_crash_var: bool = false;
pub static mut send_abort_for_kill_var: bool = false;

/* special child processes; NULL when not running */
static mut StartupPMChild: *mut PMChild = core::ptr::null_mut();
static mut BgWriterPMChild: *mut PMChild = core::ptr::null_mut();
static mut CheckpointerPMChild: *mut PMChild = core::ptr::null_mut();
static mut WalWriterPMChild: *mut PMChild = core::ptr::null_mut();
static mut WalReceiverPMChild: *mut PMChild = core::ptr::null_mut();
static mut WalSummarizerPMChild: *mut PMChild = core::ptr::null_mut();
static mut AutoVacLauncherPMChild: *mut PMChild = core::ptr::null_mut();
static mut PgArchPMChild: *mut PMChild = core::ptr::null_mut();
static mut SysLoggerPMChild: *mut PMChild = core::ptr::null_mut();
static mut SlotSyncWorkerPMChild: *mut PMChild = core::ptr::null_mut();

/* Startup process's status */
#[repr(C)]
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum StartupStatusEnum {
    STARTUP_NOT_RUNNING,
    STARTUP_RUNNING,
    STARTUP_SIGNALED,    /* we sent it a SIGQUIT or SIGKILL */
    STARTUP_CRASHED,
}
use StartupStatusEnum::*;

static mut StartupStatus: StartupStatusEnum = STARTUP_NOT_RUNNING;

/* Startup/shutdown state */
const NoShutdown: c_int = 0;
const SmartShutdown: c_int = 1;
const FastShutdown: c_int = 2;
const ImmediateShutdown: c_int = 3;

static mut Shutdown: c_int = NoShutdown;
static mut FatalError: bool = false; /* T if recovering from backend crash */

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 */
#[repr(C)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub enum PMState {
    PM_INIT,                  /* postmaster starting */
    PM_STARTUP,               /* waiting for startup subprocess */
    PM_RECOVERY,              /* in archive recovery mode */
    PM_HOT_STANDBY,           /* in hot standby mode */
    PM_RUN,                   /* normal "database is alive" state */
    PM_STOP_BACKENDS,         /* need to stop remaining backends */
    PM_WAIT_BACKENDS,         /* waiting for live backends to exit */
    PM_WAIT_XLOG_SHUTDOWN,    /* waiting for checkpointer to do shutdown ckpt */
    PM_WAIT_XLOG_ARCHIVAL,    /* waiting for archiver and walsenders to finish */
    PM_WAIT_IO_WORKERS,       /* waiting for io workers to exit */
    PM_WAIT_CHECKPOINTER,     /* waiting for checkpointer to shut down */
    PM_WAIT_DEAD_END,         /* waiting for dead-end children to exit */
    PM_NO_CHILDREN,           /* all important children have exited */
}
use PMState::*;

static mut pmState: PMState = PM_INIT;

/*
 * While performing a "smart shutdown", we restrict new connections but stay
 * in PM_RUN or PM_HOT_STANDBY state until all the client backends are gone.
 */
static mut connsAllowed: bool = true;

/* Start time of SIGKILL timeout during immediate shutdown or child crash */
/* Zero means timeout is not running */
static mut AbortStartTime: libc::time_t = 0;

static mut ReachedNormalRunning: bool = false; /* T if we've reached PM_RUN */

pub static mut ClientAuthInProgress_var: bool = false; /* T during new-client authentication */
pub static mut redirection_done_var: bool = false; /* stderr redirected for syslogger? */

/* received START_AUTOVAC_LAUNCHER signal */
static mut start_autovac_launcher: bool = false;

/* the launcher needs to be signaled to communicate some condition */
static mut avlauncher_needs_signal: bool = false;

/* received START_WALRECEIVER signal */
static mut WalReceiverRequested: bool = false;

/* set when there's a worker that needs to be started up */
static mut StartWorkerNeeded: bool = true;
static mut HaveCrashedWorker: bool = false;

/* "volatile sig_atomic_t" maps to a plain int here. */
#[allow(non_camel_case_types)]
type sig_atomic_t = c_int;

/* set when signals arrive */
static mut pending_pm_pmsignal: sig_atomic_t = 0;
static mut pending_pm_child_exit: sig_atomic_t = 0;
static mut pending_pm_reload_request: sig_atomic_t = 0;
static mut pending_pm_shutdown_request: sig_atomic_t = 0;
static mut pending_pm_fast_shutdown_request: sig_atomic_t = 0;
static mut pending_pm_immediate_shutdown_request: sig_atomic_t = 0;

/* event multiplexing object */
static mut pm_wait_set: *mut WaitEventSet = core::ptr::null_mut();

/* State for IO worker management. */
static mut io_worker_count: c_int = 0;
static mut io_worker_children: [*mut PMChild; MAX_IO_WORKERS] =
    [core::ptr::null_mut(); MAX_IO_WORKERS];

/* postmaster alive pipe fds (non-Windows) */
pub static mut postmaster_alive_fds: [c_int; 2] = [-1, -1];

pub const POSTMASTER_FD_WATCH: usize = 0; /* child reads from this */
pub const POSTMASTER_FD_OWN: usize = 1;  /* postmaster holds write end */

/* ----------------------------------------------------------------
 * Signal handlers
 * ---------------------------------------------------------------- */

/*
 * Child processes use SIGUSR1 to notify us of 'pmsignals'.  pg_ctl uses
 * SIGUSR1 to ask postmaster to check for logrotate and promote files.
 */
unsafe extern "C" fn handle_pm_pmsignal_signal(_signo: c_int) {
    pending_pm_pmsignal = 1;
    SetLatch(MyLatch as *mut Latch);
}

/*
 * pg_ctl uses SIGHUP to request a reload of the configuration files.
 */
unsafe extern "C" fn handle_pm_reload_request_signal(_signo: c_int) {
    pending_pm_reload_request = 1;
    SetLatch(MyLatch as *mut Latch);
}

/*
 * pg_ctl uses SIGTERM, SIGINT and SIGQUIT to request different types of
 * shutdown.
 */
unsafe extern "C" fn handle_pm_shutdown_request_signal(signo: c_int) {
    match signo {
        s if s == SIGTERM => {
            /* smart is implied if the other two flags aren't set */
            pending_pm_shutdown_request = 1;
        }
        s if s == SIGINT => {
            pending_pm_fast_shutdown_request = 1;
            pending_pm_shutdown_request = 1;
        }
        s if s == SIGQUIT => {
            pending_pm_immediate_shutdown_request = 1;
            pending_pm_shutdown_request = 1;
        }
        _ => {}
    }
    SetLatch(MyLatch as *mut Latch);
}

unsafe extern "C" fn handle_pm_child_exit_signal(_signo: c_int) {
    pending_pm_child_exit = 1;
    SetLatch(MyLatch as *mut Latch);
}

/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.
 */
unsafe extern "C" fn dummy_handler(_signo: c_int) {}

/* ----------------------------------------------------------------
 * InitProcessGlobals -- set MyStartTime[stamp], random seeds
 *
 * Called early in the postmaster and every backend.
 * ---------------------------------------------------------------- */
pub unsafe fn InitProcessGlobals() {
    MyStartTimestamp = GetCurrentTimestamp();
    MyStartTime = timestamptz_to_time_t(MyStartTimestamp);

    /*
     * Set a different global seed in every process.  We want something
     * unpredictable, so if possible, use high-quality random bits for the
     * seed.  Otherwise, fall back to a seed based on timestamp and PID.
     */
    if !pg_prng_strong_seed(&mut pg_global_prng_state) {
        /*
         * Since PIDs and timestamps tend to change more frequently in their
         * least significant bits, shift the timestamp left to allow a larger
         * total number of seeds in a given time period.  Since that would
         * leave only 20 bits of the timestamp that cycle every ~1 second,
         * also mix in some higher bits.
         */
        let rseed: u64 = (MyProcPid as u64)
            ^ ((MyStartTimestamp as u64) << 12)
            ^ ((MyStartTimestamp as u64) >> 20);
        pg_prng_seed(&mut pg_global_prng_state, rseed);
    }

    /*
     * Also make sure that we've set a good seed for random(3).  Use of that
     * is deprecated in core Postgres, but extensions might use it.
     */
    srandom(pg_prng_uint32(&mut pg_global_prng_state));
}

/* ----------------------------------------------------------------
 * PostmasterMain -- entry point for postmaster
 * ---------------------------------------------------------------- */

/*
 * Postmaster main entry point
 */
pub unsafe fn PostmasterMain(argc: c_int, argv: *mut *mut c_char) {
    let mut opt: c_int;
    let mut status: c_int;
    let mut userDoption: *mut c_char = core::ptr::null_mut();
    let mut listen_addr_saved: bool = false;
    let mut output_config_variable: *mut c_char = core::ptr::null_mut();

    InitProcessGlobals();

    PostmasterPid = MyProcPid;

    IsPostmasterEnvironment = true;

    /*
     * We should not be creating any files or directories before we check the
     * data directory (see checkDataDir()), but just in case set the umask to
     * the most restrictive (owner-only) permissions.
     *
     * checkDataDir() will reset the umask based on the data directory
     * permissions.
     */
    libc::umask(0o077); /* PG_MODE_MASK_OWNER */

    /*
     * By default, palloc() requests in the postmaster will be allocated in
     * the PostmasterContext, which is space that can be recycled by backends.
     * Allocated data that needs to be available to backends should be
     * allocated in TopMemoryContext.
     */
    PostmasterContext = AllocSetContextCreate(
        TopMemoryContext,
        b"Postmaster\0".as_ptr() as *const c_char,
        0,     /* ALLOCSET_DEFAULT_SIZES minContextSize */
        8192,  /* initBlockSize */
        8388608, /* maxBlockSize */
    );
    MemoryContextSwitchTo(PostmasterContext);

    /* Initialize paths to installation files */
    getInstallationPaths(*argv);

    /*
     * Set up signal handlers for the postmaster process.
     *
     * CAUTION: when changing this list, check for side-effects on the signal
     * handling setup of child processes.
     */
    pqinitmask();
    sigprocmask(SIG_SETMASK, &BlockSig as *const sigset_t, core::ptr::null_mut());

    pqsignal(SIGHUP,  Some(handle_pm_reload_request_signal));
    pqsignal(SIGINT,  Some(handle_pm_shutdown_request_signal));
    pqsignal(SIGQUIT, Some(handle_pm_shutdown_request_signal));
    pqsignal(SIGTERM, Some(handle_pm_shutdown_request_signal));
    pqsignal(SIGALRM, SIG_IGN); /* ignored */
    pqsignal(SIGPIPE, SIG_IGN); /* ignored */
    pqsignal(SIGUSR1, Some(handle_pm_pmsignal_signal));
    pqsignal(SIGUSR2, Some(dummy_handler)); /* unused, reserve for children */
    pqsignal(SIGCHLD, Some(handle_pm_child_exit_signal));

    /* This may configure SIGURG, depending on platform. */
    InitializeWaitEventSupport();
    InitProcessLocalLatch();

    /*
     * No other place in Postgres should touch SIGTTIN/SIGTTOU handling.
     */
    pqsignal(SIGTTIN, SIG_IGN); /* ignored */
    pqsignal(SIGTTOU, SIG_IGN); /* ignored */

    /* ignore SIGXFSZ, so that ulimit violations work like disk full */
    pqsignal(SIGXFSZ, SIG_IGN); /* ignored */

    /* Begin accepting signals. */
    sigprocmask(SIG_SETMASK, &UnBlockSig as *const sigset_t, core::ptr::null_mut());

    /*
     * Options setup
     */
    InitializeGUCOptions();

    opterr = 1;

    /*
     * Parse command-line options.  CAUTION: keep this in sync with
     * tcop/postgres.c and with the common help() function in main/main.c.
     */
    loop {
        opt = getopt(argc, argv, b"B:bC:c:D:d:EeFf:h:ijk:lN:OPp:r:S:sTt:W:-:\0".as_ptr() as *const c_char);
        if opt == -1 { break; }
        match opt as u8 as char {
            'B' => {
                SetConfigOption(
                    b"shared_buffers\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'b' => {
                /* Undocumented flag used for binary upgrades */
                IsBinaryUpgrade = true;
            }
            'C' => {
                output_config_variable = libc::strdup(optarg);
            }
            '-' => {
                /*
                 * Error if the user misplaced a special must-be-first option
                 * for dispatching to a subprogram.
                 */
                if parse_dispatch_option(optarg) != DISPATCH_POSTMASTER {
                    ereport!(ERROR, errmsg!("--{} must be first argument",
                        CStr::from_ptr(optarg).to_string_lossy()));
                }
                /* FALLTHROUGH to 'c' */
                let mut name: *mut c_char = core::ptr::null_mut();
                let mut value: *mut c_char = core::ptr::null_mut();
                ParseLongOption(optarg, &mut name, &mut value);
                if value.is_null() {
                    if opt == b'-' as c_int {
                        ereport!(ERROR, errmsg!("--{} requires a value",
                            CStr::from_ptr(optarg).to_string_lossy()));
                    } else {
                        ereport!(ERROR, errmsg!("-c {} requires a value",
                            CStr::from_ptr(optarg).to_string_lossy()));
                    }
                }
                SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            'c' => {
                let mut name: *mut c_char = core::ptr::null_mut();
                let mut value: *mut c_char = core::ptr::null_mut();
                ParseLongOption(optarg, &mut name, &mut value);
                if value.is_null() {
                    ereport!(ERROR, errmsg!("-c {} requires a value",
                        CStr::from_ptr(optarg).to_string_lossy()));
                }
                SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            'D' => {
                userDoption = libc::strdup(optarg);
            }
            'd' => {
                set_debug_options(
                    libc::atoi(optarg),
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'E' => {
                SetConfigOption(
                    b"log_statement\0".as_ptr() as *const c_char,
                    b"all\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'e' => {
                SetConfigOption(
                    b"datestyle\0".as_ptr() as *const c_char,
                    b"euro\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'F' => {
                SetConfigOption(
                    b"fsync\0".as_ptr() as *const c_char,
                    b"false\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'f' => {
                if !set_plan_disabling_options(optarg, PGC_POSTMASTER, PGC_S_ARGV) {
                    write_stderr(
                        b"%s: invalid argument for option -f: \"%s\"\n\0".as_ptr() as *const c_char,
                        progname, optarg,
                    );
                    ExitPostmaster(1);
                }
            }
            'h' => {
                SetConfigOption(
                    b"listen_addresses\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'i' => {
                SetConfigOption(
                    b"listen_addresses\0".as_ptr() as *const c_char,
                    b"*\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'j' => {
                /* only used by interactive backend */
            }
            'k' => {
                SetConfigOption(
                    b"unix_socket_directories\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'l' => {
                SetConfigOption(
                    b"ssl\0".as_ptr() as *const c_char,
                    b"true\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'N' => {
                SetConfigOption(
                    b"max_connections\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'O' => {
                SetConfigOption(
                    b"allow_system_table_mods\0".as_ptr() as *const c_char,
                    b"true\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'P' => {
                SetConfigOption(
                    b"ignore_system_indexes\0".as_ptr() as *const c_char,
                    b"true\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'p' => {
                SetConfigOption(
                    b"port\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'r' => {
                /* only used by single-user backend */
            }
            'S' => {
                SetConfigOption(
                    b"work_mem\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            's' => {
                SetConfigOption(
                    b"log_statement_stats\0".as_ptr() as *const c_char,
                    b"true\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            'T' => {
                /*
                 * This option used to be defined as sending SIGSTOP after a
                 * backend crash, but sending SIGABRT seems more useful.
                 */
                SetConfigOption(
                    b"send_abort_for_crash\0".as_ptr() as *const c_char,
                    b"true\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            't' => {
                let tmp = get_stats_option_name(optarg);
                if !tmp.is_null() {
                    SetConfigOption(tmp, b"true\0".as_ptr() as *const c_char, PGC_POSTMASTER, PGC_S_ARGV);
                } else {
                    write_stderr(
                        b"%s: invalid argument for option -t: \"%s\"\n\0".as_ptr() as *const c_char,
                        progname, optarg,
                    );
                    ExitPostmaster(1);
                }
            }
            'W' => {
                SetConfigOption(
                    b"post_auth_delay\0".as_ptr() as *const c_char,
                    optarg,
                    PGC_POSTMASTER, PGC_S_ARGV,
                );
            }
            _ => {
                write_stderr(
                    b"Try \"%s --help\" for more information.\n\0".as_ptr() as *const c_char,
                    progname,
                );
                ExitPostmaster(1);
            }
        }
    }

    /*
     * Postmaster accepts no non-option switch arguments.
     */
    if optind < argc {
        write_stderr(
            b"%s: invalid argument: \"%s\"\n\0".as_ptr() as *const c_char,
            progname, *argv.add(optind as usize),
        );
        write_stderr(
            b"Try \"%s --help\" for more information.\n\0".as_ptr() as *const c_char,
            progname,
        );
        ExitPostmaster(1);
    }

    /*
     * Locate the proper configuration files and data directory, and read
     * postgresql.conf for the first time.
     */
    if !SelectConfigFiles(userDoption, progname) {
        ExitPostmaster(2);
    }

    if !output_config_variable.is_null() {
        /*
         * If this is a runtime-computed GUC, it hasn't yet been initialized.
         */
        let flags = GetConfigOptionFlags(output_config_variable, true);

        if (flags & GUC_RUNTIME_COMPUTED) == 0 {
            /*
             * "-C guc" was specified, so print GUC's value and exit.
             */
            let config_val = GetConfigOption(output_config_variable, false, false);
            if !config_val.is_null() {
                libc::puts(config_val);
            } else {
                libc::puts(b"\0".as_ptr() as *const c_char);
            }
            ExitPostmaster(0);
        }

        /*
         * A runtime-computed GUC will be printed later on.  Silence log messages.
         */
        SetConfigOption(
            b"log_min_messages\0".as_ptr() as *const c_char,
            b"FATAL\0".as_ptr() as *const c_char,
            PGC_SUSET,
            PGC_S_OVERRIDE,
        );
    }

    /* Verify that DataDir looks reasonable */
    checkDataDir();

    /* Check that pg_control exists */
    checkControlFile();

    /* And switch working directory into it */
    ChangeToDataDir();

    /*
     * Check for invalid combinations of GUC settings.
     */
    if SuperuserReservedConnections_var + ReservedConnections_var >= MaxConnections {
        write_stderr(
            b"%s: \"superuser_reserved_connections\" (%d) plus \"reserved_connections\" (%d) must be less than \"max_connections\" (%d)\n\0"
                .as_ptr() as *const c_char,
            progname,
            SuperuserReservedConnections_var,
            ReservedConnections_var,
            MaxConnections,
        );
        ExitPostmaster(1);
    }
    if XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level == WAL_LEVEL_MINIMAL {
        ereport!(ERROR, errmsg!("WAL archival cannot be enabled when \"wal_level\" is \"minimal\""));
    }
    if max_wal_senders > 0 && wal_level == WAL_LEVEL_MINIMAL {
        ereport!(ERROR, errmsg!("WAL streaming (\"max_wal_senders\" > 0) requires \"wal_level\" to be \"replica\" or \"logical\""));
    }
    if summarize_wal && wal_level == WAL_LEVEL_MINIMAL {
        ereport!(ERROR, errmsg!("WAL cannot be summarized when \"wal_level\" is \"minimal\""));
    }

    /*
     * Other one-time internal sanity checks can go here, if they are fast.
     */
    if !CheckDateTokenTables() {
        write_stderr(
            b"%s: invalid datetoken tables, please fix\n\0".as_ptr() as *const c_char,
            progname,
        );
        ExitPostmaster(1);
    }

    /*
     * Now that we are done processing the postmaster arguments, reset
     * getopt(3) library so that it will work correctly in subprocesses.
     */
    optind = 1;

    /* For debugging: display postmaster environment */
    if message_level_is_interesting(7 /* DEBUG3 */) {
        /* access environ */
        extern "C" {
            static environ: *mut *mut c_char;
        }
        let mut si = core::mem::zeroed::<StringInfoData>();
        initStringInfo(&mut si);
        appendStringInfoString(&mut si, b"initial environment dump:\0".as_ptr() as *const c_char);
        let mut p = environ;
        while !(*p).is_null() {
            appendStringInfo(&mut si, b"\n%s\0".as_ptr() as *const c_char, *p);
            p = p.add(1);
        }
        ereport!(7 /* DEBUG3 */, errmsg!("{}", CStr::from_ptr(si.data as *const c_char).to_string_lossy()));
        pfree(si.data as *mut c_void);
    }

    /*
     * Create lockfile for data directory.
     */
    CreateDataDirLockFile(true);

    /*
     * Read the control file (for error checking and config info).
     */
    LocalProcessControlFile(false);

    /*
     * Register the apply launcher.
     */
    ApplyLauncherRegister();

    /*
     * process any libraries that should be preloaded at postmaster start
     */
    process_shared_preload_libraries();

    /*
     * Initialize SSL library, if specified.
     */
    if EnableSSL_var {
        let _ = secure_initialize(true);
        /* LoadedSSL = true; -- TODO(pg-port): USE_SSL gated global */
    }

    /*
     * Now that loadable modules have had their chance to alter any GUCs,
     * calculate MaxBackends and initialize the machinery to track child
     * processes.
     */
    InitializeMaxBackends();
    InitPostmasterChildSlots();

    /*
     * Calculate the size of the PGPROC fast-path lock arrays.
     */
    InitializeFastPathLocks();

    /*
     * Give preloaded libraries a chance to request additional shared memory.
     */
    process_shmem_requests();

    /*
     * Now that loadable modules have had their chance to request additional
     * shared memory, determine the value of any runtime-computed GUCs.
     */
    InitializeShmemGUCs();

    /*
     * Now that modules have been loaded, we can process any custom resource
     * managers specified in the wal_consistency_checking GUC.
     */
    InitializeWalConsistencyChecking();

    /*
     * If -C was specified with a runtime-computed GUC, we held off printing
     * the value earlier.
     */
    if !output_config_variable.is_null() {
        let config_val = GetConfigOption(output_config_variable, false, false);
        if !config_val.is_null() {
            libc::puts(config_val);
        } else {
            libc::puts(b"\0".as_ptr() as *const c_char);
        }
        ExitPostmaster(0);
    }

    /*
     * Set up shared memory and semaphores.
     */
    CreateSharedMemoryAndSemaphores();

    /*
     * Estimate number of openable files.
     */
    set_max_safe_fds();

    /*
     * Initialize pipe that allows children to wake up from sleep on
     * postmaster death.
     */
    InitPostmasterDeathWatchHandle();

    /*
     * Forcibly remove the files signaling a standby promotion request.
     */
    RemovePromoteSignalFiles();

    /* Do the same for logrotate signal file */
    RemoveLogrotateSignalFiles();

    /* Remove any outdated file holding the current log filenames. */
    {
        let rc = libc::unlink(LOG_METAINFO_DATAFILE);
        if rc < 0 && *libc::__error() != libc::ENOENT {
            ereport!(LOG, errmsg!("could not remove file \"{}\": {}",
                CStr::from_ptr(LOG_METAINFO_DATAFILE).to_string_lossy(),
                CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()));
        }
    }

    /*
     * If enabled, start up syslogger collection subprocess
     */
    if Logging_collector {
        StartSysLogger();
    }

    /*
     * Reset whereToSendOutput from DestDebug to DestNone.
     */
    if (Log_destination & LOG_DESTINATION_STDERR) == 0 {
        ereport!(LOG, errmsg!("ending log output to stderr")); /* C also: errhint */
    }

    /* whereToSendOutput = DestNone; -- TODO(pg-port): tcop global */

    /*
     * Report server startup in log.
     */
    ereport!(LOG, errmsg!("starting {}", CStr::from_ptr(PG_VERSION_STR).to_string_lossy()));

    /*
     * Establish input sockets.
     */
    ListenSockets = libc::malloc((MAXLISTEN as usize) * core::mem::size_of::<pgsocket>()) as *mut pgsocket;
    on_proc_exit(CloseServerPorts, 0);

    if !ListenAddresses.is_null() {
        let rawstring: *mut c_char = pstrdup(ListenAddresses);
        let mut elemlist: *mut List = core::ptr::null_mut();
        let mut success: c_int = 0;

        /* Parse string into list of hostnames */
        if !SplitGUCList(rawstring, b',' as c_char, &mut elemlist) {
            /* syntax error in list */
            ereport!(FATAL, errmsg!("invalid list syntax in parameter \"listen_addresses\"")); /* C also: errcode */
        }

        /* foreach(l, elemlist) - manual iteration over List */
        let mut l = list_head_ptr(elemlist);
        while !l.is_null() {
            let curhost: *mut c_char = lfirst_ptr(l) as *mut c_char;

            let hostname_arg: *const c_char = if libc::strcmp(curhost, b"*\0".as_ptr() as *const c_char) == 0 {
                core::ptr::null()
            } else {
                curhost
            };

            status = ListenServerPort(
                AF_UNSPEC,
                hostname_arg,
                PostPortNumber as libc::in_port_t,
                core::ptr::null(),
                ListenSockets,
                &mut NumListenSockets,
                MAXLISTEN,
            );

            if status == STATUS_OK {
                success += 1;
                /* record the first successful host addr in lockfile */
                if !listen_addr_saved {
                    AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, curhost as *const c_char);
                    listen_addr_saved = true;
                }
            } else {
                ereport!(WARNING, errmsg!("could not create listen socket for \"{}\"",
                    CStr::from_ptr(curhost).to_string_lossy()));
            }

            l = list_next_ptr(l);
        }

        if success == 0 && !list_is_empty(elemlist) {
            ereport!(FATAL, errmsg!("could not create any TCP/IP sockets"));
        }

        list_free(elemlist);
        pfree(rawstring as *mut c_void);
    }

    if !Unix_socket_directories.is_null() {
        let rawstring: *mut c_char = pstrdup(Unix_socket_directories);
        let mut elemlist: *mut List = core::ptr::null_mut();
        let mut success: c_int = 0;

        /* Parse string into list of directories */
        if !SplitDirectoriesString(rawstring, b',' as c_char, &mut elemlist) {
            /* syntax error in list */
            ereport!(FATAL, errmsg!("invalid list syntax in parameter \"unix_socket_directories\"")); /* C also: errcode */
        }

        let mut l = list_head_ptr(elemlist);
        while !l.is_null() {
            let socketdir: *mut c_char = lfirst_ptr(l) as *mut c_char;

            status = ListenServerPort(
                AF_UNIX,
                core::ptr::null(),
                PostPortNumber as libc::in_port_t,
                socketdir,
                ListenSockets,
                &mut NumListenSockets,
                MAXLISTEN,
            );

            if status == STATUS_OK {
                success += 1;
                /* record the first successful Unix socket in lockfile */
                if success == 1 {
                    AddToDataDirLockFile(LOCK_FILE_LINE_SOCKET_DIR, socketdir as *const c_char);
                }
            } else {
                ereport!(WARNING, errmsg!("could not create Unix-domain socket in directory \"{}\"",
                    CStr::from_ptr(socketdir).to_string_lossy()));
            }

            l = list_next_ptr(l);
        }

        if success == 0 && !list_is_empty(elemlist) {
            ereport!(FATAL, errmsg!("could not create any Unix-domain sockets"));
        }

        list_free_deep(elemlist);
        pfree(rawstring as *mut c_void);
    }

    /*
     * check that we have some socket to listen on
     */
    if NumListenSockets == 0 {
        ereport!(FATAL, errmsg!("no socket created for listening"));
    }

    /*
     * If no valid TCP ports, write an empty line for listen address.
     */
    if !listen_addr_saved {
        AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, b"\0".as_ptr() as *const c_char);
    }

    /*
     * Record postmaster options.
     */
    if !CreateOptsFile(argc, argv, my_exec_path.as_ptr()) {
        ExitPostmaster(1);
    }

    /*
     * Write the external PID file if requested
     */
    if !external_pid_file.is_null() {
        let fpidfile = libc::fopen(external_pid_file, b"w\0".as_ptr() as *const c_char);
        if !fpidfile.is_null() {
            libc::fprintf(fpidfile, b"%d\n\0".as_ptr() as *const c_char, MyProcPid);
            libc::fclose(fpidfile);

            /* Make PID file world readable */
            if libc::chmod(external_pid_file, 0o644) != 0 {
                write_stderr(
                    b"%s: could not change permissions of external PID file \"%s\"\n\0".as_ptr() as *const c_char,
                    progname, external_pid_file,
                );
            }
        } else {
            write_stderr(
                b"%s: could not write external PID file \"%s\"\n\0".as_ptr() as *const c_char,
                progname, external_pid_file,
            );
        }
        on_proc_exit(unlink_external_pid_file, 0);
    }

    /*
     * Remove old temporary files.
     */
    RemovePgTempFiles();

    /*
     * Initialize the autovacuum subsystem (again, no process start yet)
     */
    autovac_init();

    /*
     * Load configuration files for client authentication.
     */
    if !load_hba() {
        ereport!(FATAL, errmsg!("could not load {}", CStr::from_ptr(HbaFileName).to_string_lossy()));
    }
    /* load_ident() failure is non-fatal */
    let _ = load_ident();

    /*
     * On macOS, check that we are not multithreaded.
     * (HAVE_PTHREAD_IS_THREADED_NP is true on macOS/Darwin)
     */
    {
        extern "C" {
            fn pthread_is_threaded_np() -> c_int;
        }
        if pthread_is_threaded_np() != 0 {
            ereport!(FATAL, errmsg!("postmaster became multithreaded during startup")); /* C also: errcode, errhint */
        }
    }

    /*
     * Remember postmaster startup time
     */
    PgStartTime = GetCurrentTimestamp();

    /*
     * Report postmaster status in the postmaster.pid file.
     */
    AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING.as_ptr() as *const c_char);

    UpdatePMState(PM_STARTUP);

    /* Make sure we can perform I/O while starting up. */
    maybe_adjust_io_workers();

    /* Start bgwriter and checkpointer so they can help with recovery */
    if CheckpointerPMChild.is_null() {
        CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
    }
    if BgWriterPMChild.is_null() {
        BgWriterPMChild = StartChildProcess(B_BG_WRITER);
    }

    /*
     * We're ready to rock and roll...
     */
    StartupPMChild = StartChildProcess(B_STARTUP);
    Assert!(!StartupPMChild.is_null());
    StartupStatus = STARTUP_RUNNING;

    /* Some workers may be scheduled to start now */
    maybe_start_bgworkers();

    status = ServerLoop();

    /*
     * ServerLoop probably shouldn't ever return, but if it does, close down.
     */
    ExitPostmaster(if status != STATUS_OK { 1 } else { 0 });
}

/* ----------------------------------------------------------------
 * Part 3: on_proc_exit callbacks, utility fns, ClosePostmasterPorts,
 *         InitPostmasterDeathWatchHandle, ServerLoop
 * ---------------------------------------------------------------- */

/*
 * List utility stubs - TODO(pg-port): nodes/pg_list.h
 */
#[inline]
unsafe fn list_head_ptr(list: *mut List) -> *mut c_void {
    if list.is_null() { core::ptr::null_mut() }
    else { *(list as *mut *mut c_void) }
}
#[inline]
unsafe fn list_next_ptr(cell: *mut c_void) -> *mut c_void {
    if cell.is_null() { core::ptr::null_mut() }
    else { *((cell as *mut *mut c_void).add(1)) }
}
#[inline]
unsafe fn lfirst_ptr(cell: *mut c_void) -> *mut c_void {
    if cell.is_null() { core::ptr::null_mut() }
    else { *(cell as *mut *mut c_void) }
}
#[inline]
unsafe fn list_is_empty(list: *mut List) -> bool { list.is_null() }
extern "C" {
    fn list_free(list: *mut List);
    fn list_free_deep(list: *mut List);
    fn pg_strsignal(signo: c_int) -> *const c_char;
}

/*
 * on_proc_exit callback to close server's listen sockets
 */
unsafe extern "C" fn CloseServerPorts(_status: c_int, _arg: Datum) {
    /*
     * First, explicitly close all the socket FDs.
     */
    for i in 0..NumListenSockets as usize {
        if libc::close(*ListenSockets.add(i)) != 0 {
            elog!(LOG, "could not close listen socket");
        }
    }
    NumListenSockets = 0;

    /*
     * Next, remove any filesystem entries for Unix sockets.
     */
    RemoveSocketFiles();

    /*
     * We don't do anything about socket lock files here; those will be
     * removed in a later on_proc_exit callback.
     */
}

/*
 * on_proc_exit callback to delete external_pid_file
 */
unsafe extern "C" fn unlink_external_pid_file(_status: c_int, _arg: Datum) {
    if !external_pid_file.is_null() {
        libc::unlink(external_pid_file);
    }
}

/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
unsafe fn getInstallationPaths(argv0: *const c_char) {
    /* Locate the postgres executable itself */
    if find_my_exec(argv0, my_exec_path.as_mut_ptr()) < 0 {
        ereport!(FATAL, errmsg!("{}: could not locate my own executable path",
            CStr::from_ptr(argv0).to_string_lossy()));
    }

    /*
     * Locate the pkglib directory.
     */
    get_pkglib_path(my_exec_path.as_ptr(), pkglib_path.as_mut_ptr());

    /*
     * Verify that there's a readable directory there.
     */
    let pdir = AllocateDir(pkglib_path.as_ptr());
    if pdir.is_null() {
        ereport!(ERROR, errmsg!("could not open directory \"{}\": {}",
            CStr::from_ptr(pkglib_path.as_ptr()).to_string_lossy(),
            CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()));
        /* C also: errcode_for_file_access, errhint */
    }
    FreeDir(pdir);

    /*
     * It's not worth checking the share/ directory.
     */
}

/*
 * Check that pg_control exists in the correct location in the data directory.
 *
 * No attempt is made to validate the contents of pg_control here.  This is
 * just a sanity check to see if we are looking at a real data directory.
 */
unsafe fn checkControlFile() {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    libc::snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        b"%s/%s\0".as_ptr() as *const c_char,
        DataDir,
        XLOG_CONTROL_FILE(),
    );

    let fp = AllocateFile(path.as_ptr(), PG_BINARY_R);
    if fp.is_null() {
        write_stderr(
            b"%s: could not find the database system\nExpected to find it in the directory \"%s\",\nbut could not open file \"%s\"\n\0"
                .as_ptr() as *const c_char,
            progname,
            DataDir,
            path.as_ptr(),
        );
        ExitPostmaster(2);
    }
    FreeFile(fp);
}

/*
 * Determine how long should we let ServerLoop sleep, in milliseconds.
 */
unsafe fn DetermineSleepTime() -> c_int {
    let mut next_wakeup: TimestampTz = 0;

    /*
     * Normal case: either there are no background workers at all, or we're in
     * a shutdown sequence.
     */
    if Shutdown > NoShutdown || (!StartWorkerNeeded && !HaveCrashedWorker) {
        if AbortStartTime != 0 {
            /* time left to abort; clamp to 0 in case it already expired */
            let seconds: libc::time_t = SIGKILL_CHILDREN_AFTER_SECS
                - (libc::time(core::ptr::null_mut()) - AbortStartTime);
            return if seconds * 1000 > 0 { (seconds * 1000) as c_int } else { 0 };
        } else {
            return 60 * 1000;
        }
    }

    if StartWorkerNeeded {
        return 0;
    }

    if HaveCrashedWorker {
        /*
         * When there are crashed bgworkers, we sleep just long enough that
         * they are restarted when they request to be.
         */
        let list = BackgroundWorkerList();
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        /* dlist_foreach_modify over BackgroundWorkerList */
        /* Manual iteration since BackgroundWorkerList is a raw pointer */
        /* TODO(pg-port): use dlist_foreach_modify! macro once list ptr is stable */
        if !list.is_null() {
            dlist_foreach_modify!(iter, &mut *list, {
                let rw: *mut RegisteredBgWorker =
                    dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);

                if (*rw).rw_crashed_at == 0 {
                    continue;
                }

                if (*rw_bgworker(rw)).bgw_restart_time == BGW_NEVER_RESTART
                    || (*rw).rw_terminate
                {
                    ForgetBackgroundWorker(rw);
                    continue;
                }

                let this_wakeup = TimestampTzPlusMilliseconds(
                    (*rw).rw_crashed_at,
                    1000i64 * (*rw_bgworker(rw)).bgw_restart_time as i64,
                );
                if next_wakeup == 0 || this_wakeup < next_wakeup {
                    next_wakeup = this_wakeup;
                }
            });
        }
    }

    if next_wakeup != 0 {
        /* result of TimestampDifferenceMilliseconds is in [0, INT_MAX] */
        let ms = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), next_wakeup) as c_int;
        return if 60 * 1000 < ms { 60 * 1000 } else { ms };
    }

    60 * 1000
}

/*
 * Activate or deactivate notifications of server socket events.
 */
unsafe fn ConfigurePostmasterWaitSet(accept_connections: bool) {
    if !pm_wait_set.is_null() {
        FreeWaitEventSet(pm_wait_set);
    }
    pm_wait_set = core::ptr::null_mut();

    pm_wait_set = CreateWaitEventSet(
        core::ptr::null_mut(),
        if accept_connections { 1 + NumListenSockets } else { 1 },
    );
    AddWaitEventToSet(
        pm_wait_set,
        WL_LATCH_SET as u32,
        PGINVALID_SOCKET,
        MyLatch as *mut Latch,
        core::ptr::null_mut(),
    );

    if accept_connections {
        for i in 0..NumListenSockets as usize {
            AddWaitEventToSet(
                pm_wait_set,
                WL_SOCKET_ACCEPT as u32,
                *ListenSockets.add(i),
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );
        }
    }
}

/*
 * Main idle loop of postmaster
 */
unsafe fn ServerLoop() -> c_int {
    let mut last_lockfile_recheck_time: libc::time_t;
    let mut last_touch_time: libc::time_t;
    let mut events: [WaitEvent; MAXLISTEN as usize] = core::mem::zeroed();
    let mut nevents: c_int;

    ConfigurePostmasterWaitSet(true);
    last_lockfile_recheck_time = libc::time(core::ptr::null_mut());
    last_touch_time = last_lockfile_recheck_time;

    loop {
        let now: libc::time_t;

        nevents = WaitEventSetWait(
            pm_wait_set,
            DetermineSleepTime(),
            events.as_mut_ptr(),
            MAXLISTEN,
            0, /* postmaster posts no wait_events */
        );

        /*
         * Latch set by signal handler, or new connection pending on any of
         * our sockets? If the latter, fork a child process to deal with it.
         */
        for i in 0..nevents as usize {
            if (events[i].events & WL_LATCH_SET as u32) != 0 {
                ResetLatch(MyLatch as *mut Latch);
            }

            /*
             * The following requests are handled unconditionally, even if we
             * didn't see WL_LATCH_SET.
             */
            if pending_pm_shutdown_request != 0 {
                process_pm_shutdown_request();
            }
            if pending_pm_reload_request != 0 {
                process_pm_reload_request();
            }
            if pending_pm_child_exit != 0 {
                process_pm_child_exit();
            }
            if pending_pm_pmsignal != 0 {
                process_pm_pmsignal();
            }

            if (events[i].events & WL_SOCKET_ACCEPT as u32) != 0 {
                let mut s: ClientSocket = core::mem::zeroed();

                if AcceptConnection(events[i].fd, &mut s) == STATUS_OK {
                    BackendStartup(&mut s);
                }

                /* We no longer need the open socket in this process */
                if s.sock != PGINVALID_SOCKET {
                    if libc::close(s.sock) != 0 {
                        elog!(LOG, "could not close client socket");
                    }
                }
            }
        }

        /*
         * If we need to launch any background processes after changing state
         * or because some exited, do so now.
         */
        LaunchMissingBackgroundProcesses();

        /* If we need to signal the autovacuum launcher, do so now */
        if avlauncher_needs_signal {
            avlauncher_needs_signal = false;
            if !AutoVacLauncherPMChild.is_null() {
                signal_child(AutoVacLauncherPMChild, SIGUSR2);
            }
        }

        /* Check regularly for appearance of additional threads (Darwin/macOS) */
        {
            extern "C" { fn pthread_is_threaded_np() -> c_int; }
            Assert!(pthread_is_threaded_np() == 0);
        }

        /*
         * Lastly, check to see if it's time to do some things that we don't
         * want to do every single time through the loop.
         */
        now = libc::time(core::ptr::null_mut());

        /*
         * If we already sent SIGQUIT to children and they are slow to shut
         * down, it's time to send them SIGKILL (or SIGABRT if requested).
         */
        if (Shutdown >= ImmediateShutdown || FatalError)
            && AbortStartTime != 0
            && (now - AbortStartTime) >= SIGKILL_CHILDREN_AFTER_SECS
        {
            /* We were gentle with them before. Not anymore */
            ereport!(LOG, errmsg!("issuing {} to recalcitrant children",
                if send_abort_for_kill_var { "SIGABRT" } else { "SIGKILL" }));
            TerminateChildren(if send_abort_for_kill_var { SIGABRT } else { SIGKILL });
            /* reset flag so we don't SIGKILL again */
            AbortStartTime = 0;
        }

        /*
         * Once a minute, verify that postmaster.pid hasn't been removed or
         * overwritten.
         */
        if now - last_lockfile_recheck_time >= 1 * SECS_PER_MINUTE {
            if !RecheckDataDirLockFile() {
                ereport!(LOG, errmsg!("performing immediate shutdown because data directory lock file is invalid"));
                kill(MyProcPid, SIGQUIT);
            }
            last_lockfile_recheck_time = now;
        }

        /*
         * Touch Unix socket and lock files every 58 minutes.
         */
        if now - last_touch_time >= 58 * SECS_PER_MINUTE {
            TouchSocketFiles();
            TouchSocketLockFiles();
            last_touch_time = now;
        }
    }
}

/*
 * canAcceptConnections --- check to see if database state allows connections
 * of the specified type.
 */
unsafe fn canAcceptConnections(backend_type: BackendType) -> CAC_state {
    let result = CAC_OK;

    Assert!(backend_type == B_BACKEND || backend_type == B_AUTOVAC_WORKER);

    /*
     * Can't start backends when in startup/shutdown/inconsistent recovery
     * state.
     */
    if pmState != PM_RUN && pmState != PM_HOT_STANDBY {
        if Shutdown > NoShutdown {
            return CAC_SHUTDOWN;    /* shutdown is pending */
        } else if !FatalError && pmState == PM_STARTUP {
            return CAC_STARTUP;    /* normal startup */
        } else if !FatalError && pmState == PM_RECOVERY {
            return CAC_NOTHOTSTANDBY; /* not yet ready for hot standby */
        } else {
            return CAC_RECOVERY;   /* else must be crash recovery */
        }
    }

    /*
     * "Smart shutdown" restrictions are applied only to normal connections,
     * not to autovac workers.
     */
    if !connsAllowed && backend_type == B_BACKEND {
        return CAC_SHUTDOWN; /* shutdown is pending */
    }

    result
}

/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.
 */
pub unsafe fn ClosePostmasterPorts(am_syslogger: bool) {
    /* Release resources held by the postmaster's WaitEventSet. */
    if !pm_wait_set.is_null() {
        FreeWaitEventSetAfterFork(pm_wait_set);
        pm_wait_set = core::ptr::null_mut();
    }

    /*
     * Close the write end of postmaster death watch pipe.
     */
    if libc::close(postmaster_alive_fds[POSTMASTER_FD_OWN]) != 0 {
        ereport!(FATAL, errmsg!("could not close postmaster death monitoring pipe in child process"));
        /* C also: errcode_for_file_access */
    }
    postmaster_alive_fds[POSTMASTER_FD_OWN] = -1;
    /* Notify fd.c that we released one pipe FD. */
    ReleaseExternalFD();

    /*
     * Close the postmaster's listen sockets.
     * (Not in EXEC_BACKEND mode - sockets are FD_CLOEXEC there.)
     */
    if !ListenSockets.is_null() {
        for i in 0..NumListenSockets as usize {
            if libc::close(*ListenSockets.add(i)) != 0 {
                elog!(LOG, "could not close listen socket");
            }
        }
        pfree(ListenSockets as *mut c_void);
    }
    NumListenSockets = 0;
    ListenSockets = core::ptr::null_mut();

    /*
     * If using syslogger, close the read side of the pipe.
     */
    if !am_syslogger {
        if syslogPipe[0] >= 0 {
            libc::close(syslogPipe[0]);
        }
        syslogPipe[0] = -1;
    }
}

/*
 * Initialize one and only handle for monitoring postmaster death.
 */
unsafe fn InitPostmasterDeathWatchHandle() {
    /*
     * Create a pipe. Postmaster holds the write end of the pipe open
     * (POSTMASTER_FD_OWN), and children hold the read end.
     */
    Assert!(MyProcPid == PostmasterPid);
    if libc::pipe(postmaster_alive_fds.as_mut_ptr()) < 0 {
        ereport!(FATAL, errmsg!("could not create pipe to monitor postmaster death"));
        /* C also: errcode_for_file_access */
    }

    /* Notify fd.c that we've eaten two FDs for the pipe. */
    ReserveExternalFD();
    ReserveExternalFD();

    /*
     * Set O_NONBLOCK to allow testing for the fd's presence with a read() call.
     */
    if libc::fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], libc::F_SETFL, libc::O_NONBLOCK) == -1 {
        ereport!(FATAL, errmsg!("could not set postmaster death monitoring pipe to nonblocking mode"));
        /* C also: errcode_for_socket_access */
    }
}

/* ----------------------------------------------------------------
 * Part 4: signal processing, ExitPostmaster, BackendStartup,
 *         CleanupBackend, HandleChildCrash, LogChildExit
 * ---------------------------------------------------------------- */

/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
pub unsafe fn ExitPostmaster(status: c_int) -> ! {
    /*
     * There is no known cause for a postmaster to become multithreaded after
     * startup.  However, we might reach here via an error exit.
     * (HAVE_PTHREAD_IS_THREADED_NP - Darwin/macOS)
     */
    {
        extern "C" { fn pthread_is_threaded_np() -> c_int; }
        if pthread_is_threaded_np() != 0 {
            ereport!(LOG, errmsg!("postmaster became multithreaded")); /* C also: errcode, errhint */
        }
    }

    /* should cleanup shared memory and kill all backends */

    /*
     * Not sure of the semantics here.  When the Postmaster dies, should the
     * backends all be killed? probably not.
     *
     * MUST		-- vadim 05-10-1999
     */

    proc_exit(status);
}

/*
 * Re-read config files, and tell children to do same.
 */
unsafe fn process_pm_reload_request() {
    pending_pm_reload_request = 0;

    ereport!(DEBUG2, errmsg!("postmaster received reload request signal"));

    if Shutdown <= SmartShutdown {
        ereport!(LOG, errmsg!("received SIGHUP, reloading configuration files"));
        ProcessConfigFile(PGC_SIGHUP);
        SignalChildren(SIGHUP, btmask_all_except_slice(&[B_DEAD_END_BACKEND]));

        /* Reload authentication config files too */
        if !load_hba() {
            ereport!(LOG, errmsg!("{} was not reloaded",
                CStr::from_ptr(HbaFileName).to_string_lossy()));
        }

        if !load_ident() {
            ereport!(LOG, errmsg!("{} was not reloaded",
                CStr::from_ptr(IdentFileName).to_string_lossy()));
        }

        /* Reload SSL configuration as well (USE_SSL path) */
        if EnableSSL_var {
            if secure_initialize(false) == 0 {
                /* LoadedSSL = true; */
            } else {
                ereport!(LOG, errmsg!("SSL configuration was not reloaded"));
            }
        } else {
            secure_destroy();
            /* LoadedSSL = false; */
        }
    }
}

/*
 * Process shutdown request.
 */
unsafe fn process_pm_shutdown_request() {
    let mode: c_int;

    ereport!(DEBUG2, errmsg!("postmaster received shutdown request signal"));

    pending_pm_shutdown_request = 0;

    /*
     * If more than one shutdown request signal arrived since the last server
     * loop, take the one that is the most immediate.
     */
    if pending_pm_immediate_shutdown_request != 0 {
        pending_pm_immediate_shutdown_request = 0;
        pending_pm_fast_shutdown_request = 0;
        mode = ImmediateShutdown;
    } else if pending_pm_fast_shutdown_request != 0 {
        pending_pm_fast_shutdown_request = 0;
        mode = FastShutdown;
    } else {
        mode = SmartShutdown;
    }

    match mode {
        m if m == SmartShutdown => {
            /*
             * Smart Shutdown:
             *
             * Wait for children to end their work, then shut down.
             */
            if Shutdown >= SmartShutdown {
                return;
            }
            Shutdown = SmartShutdown;
            ereport!(LOG, errmsg!("received smart shutdown request"));

            /* Report status */
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING.as_ptr() as *const c_char);

            /*
             * If we reached normal running, we go straight to waiting for
             * client backends to exit.
             */
            if pmState == PM_RUN || pmState == PM_HOT_STANDBY {
                connsAllowed = false;
            } else if pmState == PM_STARTUP || pmState == PM_RECOVERY {
                /* There should be no clients, so proceed to stop children */
                UpdatePMState(PM_STOP_BACKENDS);
            }

            /*
             * Now wait for online backup mode to end and backends to exit.
             */
            PostmasterStateMachine();
        }

        m if m == FastShutdown => {
            /*
             * Fast Shutdown:
             *
             * Abort all children with SIGTERM and shut down when they are gone.
             */
            if Shutdown >= FastShutdown {
                return;
            }
            Shutdown = FastShutdown;
            ereport!(LOG, errmsg!("received fast shutdown request"));

            /* Report status */
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING.as_ptr() as *const c_char);

            if pmState == PM_STARTUP || pmState == PM_RECOVERY {
                /* Just shut down background processes silently */
                UpdatePMState(PM_STOP_BACKENDS);
            } else if pmState == PM_RUN || pmState == PM_HOT_STANDBY {
                /* Report that we're about to zap live client sessions */
                ereport!(LOG, errmsg!("aborting any active transactions"));
                UpdatePMState(PM_STOP_BACKENDS);
            }

            /*
             * PostmasterStateMachine will issue any necessary signals.
             */
            PostmasterStateMachine();
        }

        _ /* ImmediateShutdown */ => {
            /*
             * Immediate Shutdown:
             *
             * abort all children with SIGQUIT, wait for them to exit,
             * terminate remaining ones with SIGKILL.
             */
            if Shutdown >= ImmediateShutdown {
                return;
            }
            Shutdown = ImmediateShutdown;
            ereport!(LOG, errmsg!("received immediate shutdown request"));

            /* Report status */
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING.as_ptr() as *const c_char);

            /* tell children to shut down ASAP */
            /* (note we don't apply send_abort_for_crash here) */
            SetQuitSignalReason(PMQUIT_FOR_STOP);
            TerminateChildren(SIGQUIT);
            UpdatePMState(PM_WAIT_BACKENDS);

            /* set stopwatch for them to die */
            AbortStartTime = libc::time(core::ptr::null_mut());

            /*
             * Now wait for backends to exit.
             */
            PostmasterStateMachine();
        }
    }
}

/*
 * Cleanup after a child process dies.
 */
unsafe fn process_pm_child_exit() {
    let mut pid: c_int;       /* process id of dead child process */
    let mut exitstatus: c_int = 0; /* its exit status */

    pending_pm_child_exit = 0;

    ereport!(DEBUG4, errmsg!("reaping dead processes"));

    loop {
        pid = waitpid_sys(-1, &mut exitstatus, WNOHANG);
        if pid <= 0 { break; }

        /*
         * Check if this child was a startup process.
         */
        if !StartupPMChild.is_null() && pid == (*StartupPMChild).pid {
            ReleasePostmasterChildSlot(StartupPMChild);
            StartupPMChild = core::ptr::null_mut();

            /*
             * Startup process exited in response to a shutdown request.
             */
            if Shutdown > NoShutdown
                && (exit_status_0(exitstatus) || exit_status_1(exitstatus))
            {
                StartupStatus = STARTUP_NOT_RUNNING;
                UpdatePMState(PM_WAIT_BACKENDS);
                /* PostmasterStateMachine logic does the rest */
                continue;
            }

            if exit_status_3(exitstatus) {
                ereport!(LOG, errmsg!("shutdown at recovery target"));
                StartupStatus = STARTUP_NOT_RUNNING;
                Shutdown = if Shutdown > SmartShutdown { Shutdown } else { SmartShutdown };
                TerminateChildren(SIGTERM);
                UpdatePMState(PM_WAIT_BACKENDS);
                /* PostmasterStateMachine logic does the rest */
                continue;
            }

            /*
             * Unexpected exit of startup process during PM_STARTUP is catastrophic.
             */
            if pmState == PM_STARTUP
                && StartupStatus != STARTUP_SIGNALED
                && !exit_status_0(exitstatus)
            {
                LogChildExit(LOG, b"startup process\0".as_ptr() as *const c_char, pid, exitstatus);
                ereport!(LOG, errmsg!("aborting startup due to startup process failure"));
                ExitPostmaster(1);
            }

            /*
             * After PM_STARTUP, any unexpected exit of the startup process is
             * catastrophic.
             */
            if !exit_status_0(exitstatus) {
                if StartupStatus == STARTUP_SIGNALED {
                    StartupStatus = STARTUP_NOT_RUNNING;
                    if pmState == PM_STARTUP {
                        UpdatePMState(PM_WAIT_BACKENDS);
                    }
                } else {
                    StartupStatus = STARTUP_CRASHED;
                }
                HandleChildCrash(pid, exitstatus, b"startup process\0".as_ptr() as *const c_char);
                continue;
            }

            /*
             * Startup succeeded, commence normal operations
             */
            StartupStatus = STARTUP_NOT_RUNNING;
            FatalError = false;
            AbortStartTime = 0;
            ReachedNormalRunning = true;
            UpdatePMState(PM_RUN);
            connsAllowed = true;

            /*
             * At the next iteration of the postmaster's main loop, we will
             * crank up the background tasks.
             */
            StartWorkerNeeded = true;

            /* at this point we are really open for business */
            ereport!(LOG, errmsg!("database system is ready to accept connections"));

            /* Report status */
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY.as_ptr() as *const c_char);

            continue;
        }

        /*
         * Was it the bgwriter?  Normal exit can be ignored.
         */
        if !BgWriterPMChild.is_null() && pid == (*BgWriterPMChild).pid {
            ReleasePostmasterChildSlot(BgWriterPMChild);
            BgWriterPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"background writer process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the checkpointer?
         */
        if !CheckpointerPMChild.is_null() && pid == (*CheckpointerPMChild).pid {
            ReleasePostmasterChildSlot(CheckpointerPMChild);
            CheckpointerPMChild = core::ptr::null_mut();
            if exit_status_0(exitstatus) && pmState == PM_WAIT_CHECKPOINTER {
                /*
                 * OK, we saw normal exit of the checkpointer after it's been
                 * told to shut down.
                 */
                UpdatePMState(PM_WAIT_DEAD_END);
                ConfigurePostmasterWaitSet(false);
                SignalChildren(SIGTERM, btmask_all_except_slice(&[B_LOGGER]));
            } else {
                /*
                 * Any unexpected exit of the checkpointer is treated as a crash.
                 */
                HandleChildCrash(pid, exitstatus, b"checkpointer process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the wal writer?
         */
        if !WalWriterPMChild.is_null() && pid == (*WalWriterPMChild).pid {
            ReleasePostmasterChildSlot(WalWriterPMChild);
            WalWriterPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"WAL writer process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the wal receiver?
         */
        if !WalReceiverPMChild.is_null() && pid == (*WalReceiverPMChild).pid {
            ReleasePostmasterChildSlot(WalReceiverPMChild);
            WalReceiverPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"WAL receiver process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the wal summarizer?
         */
        if !WalSummarizerPMChild.is_null() && pid == (*WalSummarizerPMChild).pid {
            ReleasePostmasterChildSlot(WalSummarizerPMChild);
            WalSummarizerPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"WAL summarizer process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the autovacuum launcher?
         */
        if !AutoVacLauncherPMChild.is_null() && pid == (*AutoVacLauncherPMChild).pid {
            ReleasePostmasterChildSlot(AutoVacLauncherPMChild);
            AutoVacLauncherPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"autovacuum launcher process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /*
         * Was it the archiver?
         */
        if !PgArchPMChild.is_null() && pid == (*PgArchPMChild).pid {
            ReleasePostmasterChildSlot(PgArchPMChild);
            PgArchPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"archiver process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /* Was it the system logger?  If so, try to start a new one */
        if !SysLoggerPMChild.is_null() && pid == (*SysLoggerPMChild).pid {
            ReleasePostmasterChildSlot(SysLoggerPMChild);
            SysLoggerPMChild = core::ptr::null_mut();

            /* for safety's sake, launch new logger *first* */
            if Logging_collector {
                StartSysLogger();
            }

            if !exit_status_0(exitstatus) {
                LogChildExit(LOG, b"system logger process\0".as_ptr() as *const c_char, pid, exitstatus);
            }
            continue;
        }

        /*
         * Was it the slot sync worker?
         */
        if !SlotSyncWorkerPMChild.is_null() && pid == (*SlotSyncWorkerPMChild).pid {
            ReleasePostmasterChildSlot(SlotSyncWorkerPMChild);
            SlotSyncWorkerPMChild = core::ptr::null_mut();
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"slot sync worker process\0".as_ptr() as *const c_char);
            }
            continue;
        }

        /* Was it an IO worker? */
        if maybe_reap_io_worker(pid) {
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"io worker\0".as_ptr() as *const c_char);
            }
            maybe_adjust_io_workers();
            continue;
        }

        /*
         * Was it a backend or a background worker?
         */
        let pmchild: *mut PMChild = FindPostmasterChildByPid(pid);
        if !pmchild.is_null() {
            CleanupBackend(pmchild, exitstatus);
        } else {
            /*
             * We don't know anything about this child process.
             */
            if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
                HandleChildCrash(pid, exitstatus, b"untracked child process\0".as_ptr() as *const c_char);
            } else {
                LogChildExit(LOG, b"untracked child process\0".as_ptr() as *const c_char, pid, exitstatus);
            }
        }
    } /* loop over pending child-death reports */

    /*
     * After cleaning out the SIGCHLD queue, see if we have any state changes
     * or actions to make.
     */
    PostmasterStateMachine();
}

/*
 * CleanupBackend -- cleanup after terminated backend or background worker.
 */
unsafe fn CleanupBackend(bp: *mut PMChild, exitstatus: c_int) {
    let mut namebuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let procname: *const c_char;
    let mut crashed: bool = false;
    let mut logged: bool = false;
    let bp_pid: c_int;
    let bp_bgworker_notify: bool;
    let bp_bkend_type: BackendType;
    let rw: *mut RegisteredBgWorker;

    /* Construct a process name for the log message */
    if (*bp).bkend_type == B_BG_WORKER {
        libc::snprintf(
            namebuf.as_mut_ptr(),
            MAXPGPATH,
            b"background worker \"%s\"\0".as_ptr() as *const c_char,
            (*rw_bgworker((*bp).rw)).bgw_type.as_ptr(),
        );
        procname = namebuf.as_ptr();
    } else {
        procname = GetBackendTypeDesc((*bp).bkend_type);
    }

    /*
     * If a backend dies in an ugly way then we must signal all other backends
     * to quickdie.
     */
    if !exit_status_0(exitstatus) && !exit_status_1(exitstatus) {
        crashed = true;
    }

    /*
     * Release the PMChild entry.
     */
    bp_pid = (*bp).pid;
    bp_bgworker_notify = (*bp).bgworker_notify;
    bp_bkend_type = (*bp).bkend_type;
    rw = (*bp).rw;
    if !ReleasePostmasterChildSlot(bp) {
        /*
         * Uh-oh, the child failed to clean itself up. Treat as a crash.
         */
        crashed = true;
    }

    /*
     * In a crash case, exit immediately without resetting background worker state.
     */
    if crashed {
        HandleChildCrash(bp_pid, exitstatus, procname);
        return;
    }

    /*
     * This backend may have been slated to receive SIGUSR1 when some
     * background worker started or stopped.  Cancel those notifications.
     */
    if bp_bgworker_notify {
        BackgroundWorkerStopNotifications(bp_pid);
    }

    /*
     * If it was a background worker, also update its RegisteredBgWorker entry.
     */
    if bp_bkend_type == B_BG_WORKER {
        if !exit_status_0(exitstatus) {
            /* Record timestamp, so we know when to restart the worker. */
            (*rw).rw_crashed_at = GetCurrentTimestamp();
        } else {
            /* Zero exit status means terminate */
            (*rw).rw_crashed_at = 0;
            (*rw).rw_terminate = true;
        }

        (*rw).rw_pid = 0;
        ReportBackgroundWorkerExit(rw); /* report child death */

        if !logged {
            LogChildExit(
                if exit_status_0(exitstatus) { DEBUG1 } else { LOG },
                procname, bp_pid, exitstatus,
            );
            logged = true;
        }

        /* have it be restarted */
        HaveCrashedWorker = true;
    }

    if !logged {
        LogChildExit(DEBUG2, procname, bp_pid, exitstatus);
    }
}

/*
 * Transition into FatalError state.
 */
unsafe fn HandleFatalError(reason: c_int /* QuitSignalReason */, consider_sigabrt: bool) {
    let sigtosend: c_int;

    Assert!(!FatalError);
    Assert!(Shutdown != ImmediateShutdown);

    SetQuitSignalReason(reason);

    if consider_sigabrt && send_abort_for_crash_var {
        sigtosend = SIGABRT;
    } else {
        sigtosend = SIGQUIT;
    }

    /*
     * Signal all other child processes to exit.
     */
    TerminateChildren(sigtosend);

    FatalError = true;

    /*
     * Choose the appropriate new state to react to the fatal error.
     */
    match pmState {
        PM_INIT => {
            /* shouldn't have any children */
            Assert!(false);
        }
        PM_STARTUP => {
            /* should have been handled in process_pm_child_exit */
            Assert!(false);
        }

        /* wait for children to die */
        PM_RECOVERY | PM_HOT_STANDBY | PM_RUN | PM_STOP_BACKENDS => {
            UpdatePMState(PM_WAIT_BACKENDS);
        }

        PM_WAIT_BACKENDS => {
            /* there might be more backends to wait for */
        }

        PM_WAIT_XLOG_SHUTDOWN | PM_WAIT_XLOG_ARCHIVAL | PM_WAIT_CHECKPOINTER | PM_WAIT_IO_WORKERS => {
            /*
             * NB: Similar code exists in PostmasterStateMachine().
             */
            ConfigurePostmasterWaitSet(false);
            UpdatePMState(PM_WAIT_DEAD_END);
        }

        PM_WAIT_DEAD_END | PM_NO_CHILDREN => {}
    }

    /*
     * .. and if this doesn't happen quickly enough, now the clock is ticking.
     */
    if AbortStartTime == 0 {
        AbortStartTime = libc::time(core::ptr::null_mut());
    }
}

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, etc.
 */
unsafe fn HandleChildCrash(pid: c_int, exitstatus: c_int, procname: *const c_char) {
    /*
     * We only log messages and send signals if this is the first process
     * crash and we're not doing an immediate shutdown.
     */
    if FatalError || Shutdown == ImmediateShutdown {
        return;
    }

    LogChildExit(LOG, procname, pid, exitstatus);
    ereport!(LOG, errmsg!("terminating any other active server processes"));

    /*
     * Switch into error state.
     */
    HandleFatalError(PMQUIT_FOR_CRASH, true);
}

/*
 * Log the death of a child process.
 */
unsafe fn LogChildExit(lev: c_int, procname: *const c_char, pid: c_int, exitstatus: c_int) {
    /*
     * size of activity_buffer is arbitrary, but set equal to default
     * track_activity_query_size
     */
    let mut activity_buffer: [c_char; 1024] = [0; 1024];
    let mut activity: *const c_char = core::ptr::null();

    if !exit_status_0(exitstatus) {
        activity = pgstat_get_crashed_backend_activity(pid, activity_buffer.as_mut_ptr(), 1024);
    }

    let pname = CStr::from_ptr(procname).to_string_lossy();

    if wifexited(exitstatus) {
        ereport!(lev, errmsg!("{} (PID {}) exited with exit code {}",
            pname, pid, wexitstatus(exitstatus)));
        /* C also: activity ? errdetail(...) : 0 */
    } else if wifsignaled(exitstatus) {
        ereport!(lev, errmsg!("{} (PID {}) was terminated by signal {}: {}",
            pname, pid, wtermsig(exitstatus),
            CStr::from_ptr(pg_strsignal(wtermsig(exitstatus))).to_string_lossy()));
        /* C also: activity ? errdetail(...) : 0 */
    } else {
        ereport!(lev, errmsg!("{} (PID {}) exited with unrecognized status {}",
            pname, pid, exitstatus));
        /* C also: activity ? errdetail(...) : 0 */
    }
}

/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 */
unsafe fn BackendStartup(client_sock: *mut ClientSocket) -> c_int {
    let mut bn: *mut PMChild = core::ptr::null_mut();
    let pid: c_int;
    let mut startup_data: BackendStartupData = core::mem::zeroed();
    let mut cac: CAC_state;

    /*
     * Capture time that Postmaster got a socket from accept.
     */
    startup_data.socket_created = GetCurrentTimestamp();

    /*
     * Allocate and assign the child slot.
     */
    cac = canAcceptConnections(B_BACKEND);
    if cac == CAC_OK {
        /* Can change later to B_WAL_SENDER */
        bn = AssignPostmasterChildSlot(B_BACKEND);
        if bn.is_null() {
            /*
             * Too many regular child processes; launch a dead-end child
             * process instead.
             */
            cac = CAC_TOOMANY;
        }
    }
    if bn.is_null() {
        bn = AllocDeadEndChild();
        if bn.is_null() {
            ereport!(LOG, errmsg!("out of memory")); /* C also: errcode */
            return STATUS_ERROR;
        }
    }

    /* Pass down canAcceptConnections state */
    startup_data.canAcceptConnections = cac as c_int;
    (*bn).rw = core::ptr::null_mut();

    /* Hasn't asked to be notified about any bgworkers yet */
    (*bn).bgworker_notify = false;

    pid = postmaster_child_launch(
        (*bn).bkend_type,
        (*bn).child_slot,
        &mut startup_data as *mut BackendStartupData as *mut c_void,
        core::mem::size_of::<BackendStartupData>(),
        client_sock as *mut crate::postmaster::launch_backend::ClientSocket,
    );
    if pid < 0 {
        /* in parent, fork failed */
        let save_errno = *libc::__error();

        let _ = ReleasePostmasterChildSlot(bn);
        *libc::__error() = save_errno;
        ereport!(LOG, errmsg!("could not fork new process for connection"));
        report_fork_failure_to_client(client_sock, save_errno);
        return STATUS_ERROR;
    }

    /* in parent, successful fork */
    ereport!(DEBUG2, errmsg!("forked new {}, pid={} socket={}",
        CStr::from_ptr(GetBackendTypeDesc((*bn).bkend_type)).to_string_lossy(),
        pid, (*client_sock).sock));

    /*
     * Everything's been successful, it's safe to add this backend to our list.
     */
    (*bn).pid = pid;
    STATUS_OK
}

/*
 * Try to report backend fork() failure to client before we close the
 * connection.
 */
unsafe fn report_fork_failure_to_client(client_sock: *mut ClientSocket, errnum: c_int) {
    let mut buffer: [c_char; 1000] = [0; 1000];

    /* Format the error message packet (always V2 protocol) */
    libc::snprintf(
        buffer.as_mut_ptr(),
        1000,
        b"E%s%s\n\0".as_ptr() as *const c_char,
        b"could not fork new process for connection: \0".as_ptr() as *const c_char,
        libc::strerror(errnum),
    );

    /* Set port to non-blocking. Don't do send() if this fails */
    if !pg_set_noblock((*client_sock).sock) {
        return;
    }

    /* We'll retry after EINTR, but ignore all other failures */
    loop {
        let rc = libc::send(
            (*client_sock).sock,
            buffer.as_ptr() as *const c_void,
            libc::strlen(buffer.as_ptr()) + 1,
            0,
        );
        if !(rc < 0 && *libc::__error() == libc::EINTR) {
            break;
        }
    }
}

/* ----------------------------------------------------------------
 * Part 5: PostmasterStateMachine, UpdatePMState, pmstate_name,
 *         LaunchMissingBackgroundProcesses, signal_child, SignalChildren,
 *         TerminateChildren, CountChildren
 * ---------------------------------------------------------------- */

/*
 * Advance the postmaster's state machine and take actions as appropriate.
 */
unsafe fn PostmasterStateMachine() {
    /* If we're doing a smart shutdown, try to advance that state. */
    if pmState == PM_RUN || pmState == PM_HOT_STANDBY {
        if !connsAllowed {
            /*
             * This state ends when we have no normal client backends running.
             * Then we're ready to stop other children.
             */
            if CountChildren(btmask(B_BACKEND)) == 0 {
                UpdatePMState(PM_STOP_BACKENDS);
            }
        }
    }

    /*
     * In the PM_WAIT_BACKENDS state, wait for all the regular backends and
     * processes like autovacuum and background workers to exit.
     *
     * PM_STOP_BACKENDS is a transient state that means the same as
     * PM_WAIT_BACKENDS, but we signal the processes first.
     */
    if pmState == PM_STOP_BACKENDS || pmState == PM_WAIT_BACKENDS {
        let mut target_mask = BTYPE_MASK_NONE;

        /*
         * PM_WAIT_BACKENDS state ends when we have no regular backends, no
         * autovac launcher or workers, and no bgworkers.
         */
        target_mask = btmask_add_slice(target_mask, &[
            B_BACKEND,
            B_AUTOVAC_LAUNCHER,
            B_AUTOVAC_WORKER,
            B_BG_WORKER,
        ]);

        /*
         * No walwriter, bgwriter, slot sync worker, or WAL summarizer either.
         */
        target_mask = btmask_add_slice(target_mask, &[
            B_WAL_WRITER,
            B_BG_WRITER,
            B_SLOTSYNC_WORKER,
            B_WAL_SUMMARIZER,
        ]);

        /* If we're in recovery, also stop startup and walreceiver procs */
        target_mask = btmask_add_slice(target_mask, &[
            B_STARTUP,
            B_WAL_RECEIVER,
        ]);

        /*
         * If we are doing crash recovery or an immediate shutdown then we
         * expect archiver, checkpointer, io workers and walsender to exit as well.
         */
        if FatalError || Shutdown >= ImmediateShutdown {
            target_mask = btmask_add_slice(target_mask, &[
                B_CHECKPOINTER,
                B_ARCHIVER,
                B_IO_WORKER,
                B_WAL_SENDER,
            ]);
        }

        /* If we had not yet signaled the processes to exit, do so now */
        if pmState == PM_STOP_BACKENDS {
            /*
             * Forget any pending requests for background workers.
             */
            ForgetUnstartedBackgroundWorkers();

            SignalChildren(SIGTERM, target_mask);

            UpdatePMState(PM_WAIT_BACKENDS);
        }

        /* Are any of the target processes still running? */
        if CountChildren(target_mask) == 0 {
            if Shutdown >= ImmediateShutdown || FatalError {
                /*
                 * Stop any dead-end children and stop creating new ones.
                 *
                 * NB: Similar code exists in HandleFatalError().
                 */
                UpdatePMState(PM_WAIT_DEAD_END);
                ConfigurePostmasterWaitSet(false);
                SignalChildren(SIGQUIT, btmask(B_DEAD_END_BACKEND));

                /*
                 * We already SIGQUIT'd auxiliary processes (other than
                 * logger), if any, when we started immediate shutdown or
                 * entered FatalError state.
                 */
            } else {
                /*
                 * If we get here, we are proceeding with normal shutdown. All
                 * the regular children are gone, and it's time to tell the
                 * checkpointer to do a shutdown checkpoint.
                 */
                Assert!(Shutdown > NoShutdown);
                /* Start the checkpointer if not running */
                if CheckpointerPMChild.is_null() {
                    CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
                }
                /* And tell it to write the shutdown checkpoint */
                if !CheckpointerPMChild.is_null() {
                    signal_child(CheckpointerPMChild, SIGINT);
                    UpdatePMState(PM_WAIT_XLOG_SHUTDOWN);
                } else {
                    /*
                     * If we failed to fork a checkpointer, just shut down.
                     */
                    HandleFatalError(PMQUIT_FOR_CRASH, false);
                }
            }
        }
    }

    /*
     * The state transition from PM_WAIT_XLOG_SHUTDOWN to
     * PM_WAIT_XLOG_ARCHIVAL is in process_pm_pmsignal(), in response to
     * PMSIGNAL_XLOG_IS_SHUTDOWN.
     */

    if pmState == PM_WAIT_XLOG_ARCHIVAL {
        /*
         * PM_WAIT_XLOG_ARCHIVAL state ends when there are no children other
         * than checkpointer, io workers and dead-end children left.
         */
        if CountChildren(btmask_all_except_slice(&[
            B_CHECKPOINTER, B_IO_WORKER, B_LOGGER, B_DEAD_END_BACKEND,
        ])) == 0 {
            UpdatePMState(PM_WAIT_IO_WORKERS);
            SignalChildren(SIGUSR2, btmask(B_IO_WORKER));
        }
    }

    if pmState == PM_WAIT_IO_WORKERS {
        /*
         * PM_WAIT_IO_WORKERS state ends when there's only checkpointer and
         * dead-end children left.
         */
        if io_worker_count == 0 {
            UpdatePMState(PM_WAIT_CHECKPOINTER);

            /*
             * Tell checkpointer to shut down too.
             */
            if !CheckpointerPMChild.is_null() {
                signal_child(CheckpointerPMChild, SIGUSR2);
            }
        }
    }

    /*
     * The state transition from PM_WAIT_CHECKPOINTER to PM_WAIT_DEAD_END is
     * in process_pm_child_exit().
     */

    if pmState == PM_WAIT_DEAD_END {
        /*
         * PM_WAIT_DEAD_END state ends when all other children are gone except
         * for the logger.
         */
        if CountChildren(btmask_all_except_slice(&[B_LOGGER])) == 0 {
            /* These other guys should be dead already */
            Assert!(StartupPMChild.is_null());
            Assert!(WalReceiverPMChild.is_null());
            Assert!(WalSummarizerPMChild.is_null());
            Assert!(BgWriterPMChild.is_null());
            Assert!(CheckpointerPMChild.is_null());
            Assert!(WalWriterPMChild.is_null());
            Assert!(AutoVacLauncherPMChild.is_null());
            Assert!(SlotSyncWorkerPMChild.is_null());
            /* syslogger is not considered here */
            UpdatePMState(PM_NO_CHILDREN);
        }
    }

    /*
     * If we've been told to shut down, we exit as soon as there are no
     * remaining children.
     */
    if Shutdown > NoShutdown && pmState == PM_NO_CHILDREN {
        if FatalError {
            ereport!(LOG, errmsg!("abnormal database system shutdown"));
            ExitPostmaster(1);
        } else {
            /*
             * Normal exit from the postmaster is here.
             */
            ExitPostmaster(0);
        }
    }

    /*
     * If the startup process failed, or the user does not want an automatic
     * restart after backend crashes, wait for all non-syslogger children to
     * exit, and then exit postmaster.
     */
    if pmState == PM_NO_CHILDREN {
        if StartupStatus == STARTUP_CRASHED {
            ereport!(LOG, errmsg!("shutting down due to startup process failure"));
            ExitPostmaster(1);
        }
        if !restart_after_crash_var {
            ereport!(LOG, errmsg!("shutting down because \"restart_after_crash\" is off"));
            ExitPostmaster(1);
        }
    }

    /*
     * If we need to recover from a crash, wait for all non-syslogger children
     * to exit, then reset shmem and start the startup process.
     */
    if FatalError && pmState == PM_NO_CHILDREN {
        ereport!(LOG, errmsg!("all server processes terminated; reinitializing"));

        /* remove leftover temporary files after a crash */
        if remove_temp_files_after_crash_var {
            RemovePgTempFiles();
        }

        /* allow background workers to immediately restart */
        ResetBackgroundWorkerCrashTimes();

        shmem_exit(1);

        /* re-read control file into local memory */
        LocalProcessControlFile(true);

        /* re-create shared memory and semaphores */
        CreateSharedMemoryAndSemaphores();

        UpdatePMState(PM_STARTUP);

        /* Make sure we can perform I/O while starting up. */
        maybe_adjust_io_workers();

        StartupPMChild = StartChildProcess(B_STARTUP);
        Assert!(!StartupPMChild.is_null());
        StartupStatus = STARTUP_RUNNING;
        /* crash recovery started, reset SIGKILL flag */
        AbortStartTime = 0;

        /* start accepting server socket connection events again */
        ConfigurePostmasterWaitSet(true);
    }
}

unsafe fn pmstate_name(state: PMState) -> &'static str {
    match state {
        PM_INIT              => "PM_INIT",
        PM_STARTUP           => "PM_STARTUP",
        PM_RECOVERY          => "PM_RECOVERY",
        PM_HOT_STANDBY       => "PM_HOT_STANDBY",
        PM_RUN               => "PM_RUN",
        PM_STOP_BACKENDS     => "PM_STOP_BACKENDS",
        PM_WAIT_BACKENDS     => "PM_WAIT_BACKENDS",
        PM_WAIT_XLOG_SHUTDOWN => "PM_WAIT_XLOG_SHUTDOWN",
        PM_WAIT_XLOG_ARCHIVAL => "PM_WAIT_XLOG_ARCHIVAL",
        PM_WAIT_IO_WORKERS   => "PM_WAIT_IO_WORKERS",
        PM_WAIT_CHECKPOINTER => "PM_WAIT_CHECKPOINTER",
        PM_WAIT_DEAD_END     => "PM_WAIT_DEAD_END",
        PM_NO_CHILDREN       => "PM_NO_CHILDREN",
    }
}

/*
 * Simple wrapper for updating pmState.
 */
unsafe fn UpdatePMState(new_state: PMState) {
    elog!(DEBUG1, "updating PMState from {} to {}",
        pmstate_name(pmState), pmstate_name(new_state));
    pmState = new_state;
}

/*
 * Launch background processes after state change, or relaunch after an
 * existing process has exited.
 */
unsafe fn LaunchMissingBackgroundProcesses() {
    /* Syslogger is active in all states */
    if SysLoggerPMChild.is_null() && Logging_collector {
        StartSysLogger();
    }

    /*
     * The number of configured workers might have changed.
     */
    maybe_adjust_io_workers();

    /*
     * The checkpointer and the background writer are active from the start,
     * until shutdown is initiated.
     */
    if pmState == PM_RUN || pmState == PM_RECOVERY
        || pmState == PM_HOT_STANDBY || pmState == PM_STARTUP
    {
        if CheckpointerPMChild.is_null() {
            CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
        }
        if BgWriterPMChild.is_null() {
            BgWriterPMChild = StartChildProcess(B_BG_WRITER);
        }
    }

    /*
     * WAL writer is needed only in normal operation.
     */
    if WalWriterPMChild.is_null() && pmState == PM_RUN {
        WalWriterPMChild = StartChildProcess(B_WAL_WRITER);
    }

    /*
     * We don't want autovacuum to run in binary upgrade mode.
     */
    if !IsBinaryUpgrade
        && AutoVacLauncherPMChild.is_null()
        && (AutoVacuumingActive() || start_autovac_launcher)
        && pmState == PM_RUN
    {
        AutoVacLauncherPMChild = StartChildProcess(B_AUTOVAC_LAUNCHER);
        if !AutoVacLauncherPMChild.is_null() {
            start_autovac_launcher = false; /* signal processed */
        }
    }

    /*
     * If WAL archiving is enabled always, we are allowed to start archiver
     * even during recovery.
     */
    if PgArchPMChild.is_null()
        && ((XLogArchivingActive() && pmState == PM_RUN)
            || (XLogArchivingAlways()
                && (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)))
        && PgArchCanRestart()
    {
        PgArchPMChild = StartChildProcess(B_ARCHIVER);
    }

    /*
     * If we need to start a slot sync worker, try to do that now.
     */
    if SlotSyncWorkerPMChild.is_null()
        && pmState == PM_HOT_STANDBY
        && Shutdown <= SmartShutdown
        && sync_replication_slots
        && ValidateSlotSyncParams(LOG)
        && SlotSyncWorkerCanRestart()
    {
        SlotSyncWorkerPMChild = StartChildProcess(B_SLOTSYNC_WORKER);
    }

    /*
     * If we need to start a WAL receiver, try to do that now.
     */
    if WalReceiverRequested {
        if WalReceiverPMChild.is_null()
            && (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)
            && Shutdown <= SmartShutdown
        {
            WalReceiverPMChild = StartChildProcess(B_WAL_RECEIVER);
            if WalReceiverPMChild != core::ptr::null_mut() {
                WalReceiverRequested = false;
            }
            /* else leave the flag set, so we'll try again later */
        }
    }

    /* If we need to start a WAL summarizer, try to do that now */
    if summarize_wal
        && WalSummarizerPMChild.is_null()
        && (pmState == PM_RUN || pmState == PM_HOT_STANDBY)
        && Shutdown <= SmartShutdown
    {
        WalSummarizerPMChild = StartChildProcess(B_WAL_SUMMARIZER);
    }

    /* Get other worker processes running, if needed */
    if StartWorkerNeeded || HaveCrashedWorker {
        maybe_start_bgworkers();
    }
}

fn pm_signame(signal: c_int) -> &'static str {
    match signal {
        s if s == SIGABRT => "SIGABRT",
        s if s == SIGCHLD => "SIGCHLD",
        s if s == SIGHUP  => "SIGHUP",
        s if s == SIGINT  => "SIGINT",
        s if s == SIGKILL => "SIGKILL",
        s if s == SIGQUIT => "SIGQUIT",
        s if s == SIGTERM => "SIGTERM",
        s if s == SIGUSR1 => "SIGUSR1",
        s if s == SIGUSR2 => "SIGUSR2",
        _ => {
            /* all signals sent by postmaster should be listed here */
            Assert!(false);
            "(unknown)"
        }
    }
}

/*
 * Send a signal to a postmaster child process.
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.
 */
unsafe fn signal_child(pmchild: *mut PMChild, signal: c_int) {
    let pid = (*pmchild).pid;

    ereport!(DEBUG3, errmsg!("sending signal {}/{} to {} process with pid {}",
        signal, pm_signame(signal),
        CStr::from_ptr(GetBackendTypeDesc((*pmchild).bkend_type)).to_string_lossy(),
        pid as c_int));

    if kill(pid, signal) < 0 {
        elog!(DEBUG3, "kill({},{}) failed", pid as c_int, signal);
    }
    /* HAVE_SETSID -- Darwin/macOS always has setsid */
    match signal {
        s if s == SIGINT || s == SIGTERM || s == SIGQUIT || s == SIGKILL || s == SIGABRT => {
            if kill(-pid, signal) < 0 {
                elog!(DEBUG3, "kill({},{}) failed", -(pid as i32), signal);
            }
        }
        _ => {}
    }
}

/*
 * Send a signal to the targeted children.
 */
unsafe fn SignalChildren(signal: c_int, target_mask: BackendTypeMask) -> bool {
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut signaled: bool = false;

    dlist_foreach!(iter, &mut ActiveChildList, {
        let bp: *mut PMChild = dlist_container!(PMChild, elem, iter.cur);

        /*
         * If we need to distinguish between B_BACKEND and B_WAL_SENDER, check
         * if any B_BACKEND backends have recently announced they are walsenders.
         */
        if btmask_contains(target_mask, B_WAL_SENDER) != btmask_contains(target_mask, B_BACKEND)
            && (*bp).bkend_type == B_BACKEND
        {
            if IsPostmasterChildWalSender((*bp).child_slot) {
                (*bp).bkend_type = B_WAL_SENDER;
            }
        }

        if !btmask_contains(target_mask, (*bp).bkend_type) {
            continue;
        }

        signal_child(bp, signal);
        signaled = true;
    });
    signaled
}

/*
 * Send a termination signal to children.  This considers all of our children
 * processes, except syslogger.
 */
unsafe fn TerminateChildren(signal: c_int) {
    SignalChildren(signal, btmask_all_except_slice(&[B_LOGGER]));
    if !StartupPMChild.is_null() {
        if signal == SIGQUIT || signal == SIGKILL || signal == SIGABRT {
            StartupStatus = STARTUP_SIGNALED;
        }
    }
}

/*
 * Count up number of child processes of specified types.
 */
unsafe fn CountChildren(target_mask: BackendTypeMask) -> c_int {
    let mut iter: dlist_iter = core::mem::zeroed();
    let mut cnt: c_int = 0;

    dlist_foreach!(iter, &mut ActiveChildList, {
        let bp: *mut PMChild = dlist_container!(PMChild, elem, iter.cur);

        /*
         * If we need to distinguish between B_BACKEND and B_WAL_SENDER, check
         * if any B_BACKEND backends have recently announced they are walsenders.
         */
        if btmask_contains(target_mask, B_WAL_SENDER) != btmask_contains(target_mask, B_BACKEND)
            && (*bp).bkend_type == B_BACKEND
        {
            if IsPostmasterChildWalSender((*bp).child_slot) {
                (*bp).bkend_type = B_WAL_SENDER;
            }
        }

        if !btmask_contains(target_mask, (*bp).bkend_type) {
            continue;
        }

        ereport!(DEBUG4, errmsg!("{} process {} is still running",
            CStr::from_ptr(GetBackendTypeDesc((*bp).bkend_type)).to_string_lossy(),
            (*bp).pid as c_int));

        cnt += 1;
    });
    cnt
}

/* ----------------------------------------------------------------
 * Part 6: StartChildProcess, StartSysLogger, StartAutovacuumWorker,
 *         CreateOptsFile, StartBackgroundWorker, bgworker_should_start_now,
 *         maybe_start_bgworkers, maybe_reap_io_worker, maybe_adjust_io_workers,
 *         PostmasterMarkPIDForWorkerNotify, process_pm_pmsignal
 * ---------------------------------------------------------------- */

/*
 * StartChildProcess -- start an auxiliary process for the postmaster.
 */
unsafe fn StartChildProcess(btype: BackendType) -> *mut PMChild {
    let pmchild: *mut PMChild;
    let pid: c_int;

    pmchild = AssignPostmasterChildSlot(btype);
    if pmchild.is_null() {
        if btype == B_AUTOVAC_WORKER {
            ereport!(LOG, errmsg!("no slot available for new autovacuum worker process")); /* C also: errcode */
        } else {
            /* shouldn't happen because we allocate enough slots */
            elog!(LOG, "no postmaster child slot available for aux process");
        }
        return core::ptr::null_mut();
    }

    pid = postmaster_child_launch(btype, (*pmchild).child_slot, core::ptr::null_mut(), 0, core::ptr::null_mut());
    if pid < 0 {
        /* in parent, fork failed */
        ReleasePostmasterChildSlot(pmchild);
        ereport!(LOG, errmsg!("could not fork \"{}\" process",
            CStr::from_ptr(PostmasterChildName(btype)).to_string_lossy()));

        /*
         * fork failure is fatal during startup, but there's no need to choke
         * immediately if starting other child types fails.
         */
        if btype == B_STARTUP {
            ExitPostmaster(1);
        }
        return core::ptr::null_mut();
    }

    /* in parent, successful fork */
    (*pmchild).pid = pid;
    pmchild
}

/*
 * StartSysLogger -- start the syslogger process.
 */
pub unsafe fn StartSysLogger() {
    Assert!(SysLoggerPMChild.is_null());

    SysLoggerPMChild = AssignPostmasterChildSlot(B_LOGGER);
    if SysLoggerPMChild.is_null() {
        elog!(PANIC, "no postmaster child slot available for syslogger");
    }
    (*SysLoggerPMChild).pid = SysLogger_Start((*SysLoggerPMChild).child_slot);
    if (*SysLoggerPMChild).pid == 0 {
        ReleasePostmasterChildSlot(SysLoggerPMChild);
        SysLoggerPMChild = core::ptr::null_mut();
    }
}

/*
 * StartAutovacuumWorker
 *   Start an autovac worker process.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
unsafe fn StartAutovacuumWorker() {
    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.
     */
    if canAcceptConnections(B_AUTOVAC_WORKER) == CAC_OK {
        let bn = StartChildProcess(B_AUTOVAC_WORKER);
        if !bn.is_null() {
            (*bn).bgworker_notify = false;
            (*bn).rw = core::ptr::null_mut();
            return;
        } else {
            /*
             * fork failed, fall through to report -- actual error message was
             * logged by StartChildProcess
             */
        }
    }

    /*
     * Report the failure to the launcher, if it's running.
     */
    if !AutoVacLauncherPMChild.is_null() {
        AutoVacWorkerFailed();
        avlauncher_needs_signal = true;
    }
}

/*
 * Create the opts file.
 */
unsafe fn CreateOptsFile(argc: c_int, argv: *mut *mut c_char, fullprogname: *const c_char) -> bool {
    let opts_file = b"postmaster.opts\0".as_ptr() as *const c_char;

    let fp = libc::fopen(opts_file, b"w\0".as_ptr() as *const c_char);
    if fp.is_null() {
        ereport!(LOG, errmsg!("could not create file \"postmaster.opts\"")); /* C also: errcode_for_file_access */
        return false;
    }

    libc::fprintf(fp, b"%s\0".as_ptr() as *const c_char, fullprogname);
    for i in 1..argc {
        libc::fprintf(fp, b" \"%s\"\0".as_ptr() as *const c_char, *argv.add(i as usize));
    }
    libc::fputs(b"\n\0".as_ptr() as *const c_char, fp);

    if libc::fclose(fp) != 0 {
        ereport!(LOG, errmsg!("could not write file \"postmaster.opts\"")); /* C also: errcode_for_file_access */
        return false;
    }

    true
}

/*
 * Start a new bgworker.
 * Starting time conditions must have been checked already.
 *
 * Returns true on success, false on failure.
 * NB -- this code very roughly matches BackendStartup.
 */
unsafe fn StartBackgroundWorker(rw: *mut RegisteredBgWorker) -> bool {
    let bn: *mut PMChild;
    let worker_pid: c_int;

    Assert!((*rw).rw_pid == 0);

    /*
     * Allocate and assign the child slot.
     */
    bn = AssignPostmasterChildSlot(B_BG_WORKER);
    if bn.is_null() {
        ereport!(LOG, errmsg!("no slot available for new background worker process")); /* C also: errcode */
        (*rw).rw_crashed_at = GetCurrentTimestamp();
        return false;
    }
    (*bn).rw = rw;
    (*bn).bkend_type = B_BG_WORKER;
    (*bn).bgworker_notify = false;

    ereport!(DEBUG1, errmsg!("starting background worker process \"{}\"",
        CStr::from_ptr((*rw_bgworker(rw)).bgw_name.as_ptr()).to_string_lossy()));

    worker_pid = postmaster_child_launch(
        B_BG_WORKER,
        (*bn).child_slot,
        rw_bgworker(rw) as *mut c_void,
        core::mem::size_of::<BackgroundWorker>(),
        core::ptr::null_mut(),
    );
    if worker_pid == -1 {
        /* in postmaster, fork failed ... */
        ereport!(LOG, errmsg!("could not fork background worker process"));
        /* undo what AssignPostmasterChildSlot did */
        ReleasePostmasterChildSlot(bn);

        /* mark entry as crashed, so we'll try again later */
        (*rw).rw_crashed_at = GetCurrentTimestamp();
        return false;
    }

    /* in postmaster, fork successful ... */
    (*rw).rw_pid = worker_pid;
    (*bn).pid = (*rw).rw_pid;
    ReportBackgroundWorkerPID(rw);
    true
}

/*
 * Does the current postmaster state require starting a worker with the
 * specified start_time?
 */
unsafe fn bgworker_should_start_now(start_time: BgWorkerStartTime) -> bool {
    match pmState {
        PM_NO_CHILDREN
        | PM_WAIT_CHECKPOINTER
        | PM_WAIT_DEAD_END
        | PM_WAIT_XLOG_ARCHIVAL
        | PM_WAIT_XLOG_SHUTDOWN
        | PM_WAIT_IO_WORKERS
        | PM_WAIT_BACKENDS
        | PM_STOP_BACKENDS => {}

        PM_RUN => {
            if start_time == BgWorkerStart_RecoveryFinished {
                return true;
            }
            /* fall through */
            if start_time == BgWorkerStart_ConsistentState {
                return true;
            }
            /* fall through */
            if start_time == BgWorkerStart_PostmasterStart {
                return true;
            }
        }

        PM_HOT_STANDBY => {
            if start_time == BgWorkerStart_ConsistentState {
                return true;
            }
            /* fall through */
            if start_time == BgWorkerStart_PostmasterStart {
                return true;
            }
        }

        PM_RECOVERY | PM_STARTUP | PM_INIT => {
            if start_time == BgWorkerStart_PostmasterStart {
                return true;
            }
        }
    }

    false
}

/*
 * If the time is right, start background worker(s).
 */
unsafe fn maybe_start_bgworkers() {
    const MAX_BGWORKERS_TO_LAUNCH: c_int = 100;
    let mut num_launched: c_int = 0;
    let mut now: TimestampTz = 0;
    let mut iter: dlist_mutable_iter = core::mem::zeroed();

    /*
     * During crash recovery, we have no need to be called until the state
     * transition out of recovery.
     */
    if FatalError {
        StartWorkerNeeded = false;
        HaveCrashedWorker = false;
        return;
    }

    /* Don't need to be called again unless we find a reason for it below */
    StartWorkerNeeded = false;
    HaveCrashedWorker = false;

    let list = BackgroundWorkerList();
    if list.is_null() { return; }

    dlist_foreach_modify!(iter, &mut *list, {
        let rw: *mut RegisteredBgWorker =
            dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);

        /* ignore if already running */
        if (*rw).rw_pid != 0 {
            continue;
        }

        /* if marked for death, clean up and remove from list */
        if (*rw).rw_terminate {
            ForgetBackgroundWorker(rw);
            continue;
        }

        /*
         * If this worker has crashed previously, maybe it needs to be
         * restarted.
         */
        if (*rw).rw_crashed_at != 0 {
            if (*rw_bgworker(rw)).bgw_restart_time == BGW_NEVER_RESTART {
                let notify_pid = (*rw_bgworker(rw)).bgw_notify_pid;
                ForgetBackgroundWorker(rw);

                /* Report worker is gone now. */
                if notify_pid != 0 {
                    kill(notify_pid, SIGUSR1);
                }
                continue;
            }

            /* read system time only when needed */
            if now == 0 {
                now = GetCurrentTimestamp();
            }

            if !TimestampDifferenceExceeds(
                (*rw).rw_crashed_at, now,
                (*rw_bgworker(rw)).bgw_restart_time * 1000,
            ) {
                /* Set flag to remember that we have workers to start later */
                HaveCrashedWorker = true;
                continue;
            }
        }

        if bgworker_should_start_now((*rw_bgworker(rw)).bgw_start_time) {
            /* reset crash time before trying to start worker */
            (*rw).rw_crashed_at = 0;

            /*
             * Try to start the worker.
             */
            if !StartBackgroundWorker(rw) {
                StartWorkerNeeded = true;
                return;
            }

            /*
             * If we've launched as many workers as allowed, quit.
             */
            num_launched += 1;
            if num_launched >= MAX_BGWORKERS_TO_LAUNCH {
                StartWorkerNeeded = true;
                return;
            }
        }
    });
}

unsafe fn maybe_reap_io_worker(pid: c_int) -> bool {
    for i in 0..MAX_IO_WORKERS {
        if !io_worker_children[i].is_null()
            && (*io_worker_children[i]).pid == pid
        {
            ReleasePostmasterChildSlot(io_worker_children[i]);
            io_worker_count -= 1;
            io_worker_children[i] = core::ptr::null_mut();
            return true;
        }
    }
    false
}

/*
 * Start or stop IO workers, to close the gap between the number of running
 * workers and the number of configured workers.
 */
unsafe fn maybe_adjust_io_workers() {
    if !pgaio_workers_enabled() {
        return;
    }

    /*
     * If we're in final shutting down state, then we're just waiting for all
     * processes to exit.
     */
    if pmState >= PM_WAIT_IO_WORKERS {
        return;
    }

    /* Don't start new workers during an immediate shutdown either. */
    if Shutdown >= ImmediateShutdown {
        return;
    }

    /*
     * Don't start new workers if we're in the shutdown phase of a crash
     * restart. But we *do* need to start if we're already starting up again.
     */
    if FatalError && pmState >= PM_STOP_BACKENDS {
        return;
    }

    Assert!(pmState < PM_WAIT_IO_WORKERS);

    /* Not enough running? */
    while io_worker_count < io_workers {
        let mut slot_idx: usize = MAX_IO_WORKERS;

        /* find unused entry in io_worker_children array */
        for i in 0..MAX_IO_WORKERS {
            if io_worker_children[i].is_null() {
                slot_idx = i;
                break;
            }
        }
        if slot_idx == MAX_IO_WORKERS {
            elog!(ERROR, "could not find a free IO worker slot");
        }

        /* Try to launch one. */
        let child = StartChildProcess(B_IO_WORKER);
        if !child.is_null() {
            io_worker_children[slot_idx] = child;
            io_worker_count += 1;
        } else {
            break; /* try again next time */
        }
    }

    /* Too many running? */
    if io_worker_count > io_workers {
        /* ask the IO worker in the highest slot to exit */
        for i in (0..MAX_IO_WORKERS).rev() {
            if !io_worker_children[i].is_null() {
                kill((*io_worker_children[i]).pid, SIGUSR2);
                break;
            }
        }
    }
}

/*
 * When a backend asks to be notified about worker state changes, we
 * set a flag in its backend entry.
 */
pub unsafe fn PostmasterMarkPIDForWorkerNotify(pid: c_int) -> bool {
    let mut iter: dlist_iter = core::mem::zeroed();

    dlist_foreach!(iter, &mut ActiveChildList, {
        let bp: *mut PMChild = dlist_container!(PMChild, elem, iter.cur);
        if (*bp).pid == pid {
            (*bp).bgworker_notify = true;
            return true;
        }
    });
    false
}

/*
 * Handle pmsignal conditions representing requests from backends,
 * and check for promote and logrotate requests from pg_ctl.
 */
unsafe fn process_pm_pmsignal() {
    let mut request_state_update: bool = false;

    pending_pm_pmsignal = 0;

    ereport!(DEBUG2, errmsg!("postmaster received pmsignal signal"));

    /*
     * RECOVERY_STARTED and BEGIN_HOT_STANDBY signals are ignored in
     * unexpected states.
     */
    if CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED)
        && pmState == PM_STARTUP
        && Shutdown == NoShutdown
    {
        /* WAL redo has started. We're out of reinitialization. */
        FatalError = false;
        AbortStartTime = 0;
        reachedConsistency = false;

        /*
         * Start the archiver if we're responsible for (re-)archiving received files.
         */
        Assert!(PgArchPMChild.is_null());
        if XLogArchivingAlways() {
            PgArchPMChild = StartChildProcess(B_ARCHIVER);
        }

        /*
         * If we aren't planning to enter hot standby mode later, treat
         * RECOVERY_STARTED as meaning we're out of startup.
         */
        if !EnableHotStandby {
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STANDBY.as_ptr() as *const c_char);
        }

        UpdatePMState(PM_RECOVERY);
    }

    if CheckPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT)
        && pmState == PM_RECOVERY
        && Shutdown == NoShutdown
    {
        reachedConsistency = true;
    }

    if CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY)
        && pmState == PM_RECOVERY
        && Shutdown == NoShutdown
    {
        ereport!(LOG, errmsg!("database system is ready to accept read-only connections"));

        /* Report status */
        AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY.as_ptr() as *const c_char);

        UpdatePMState(PM_HOT_STANDBY);
        connsAllowed = true;

        /* Some workers may be scheduled to start now */
        StartWorkerNeeded = true;
    }

    /* Process background worker state changes. */
    if CheckPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE) {
        /* Accept new worker requests only if not stopping. */
        BackgroundWorkerStateChange(pmState < PM_STOP_BACKENDS);
        StartWorkerNeeded = true;
    }

    /* Tell syslogger to rotate logfile if requested */
    if !SysLoggerPMChild.is_null() {
        if CheckLogrotateSignal() {
            signal_child(SysLoggerPMChild, SIGUSR1);
            RemoveLogrotateSignalFiles();
        } else if CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) {
            signal_child(SysLoggerPMChild, SIGUSR1);
        }
    }

    if CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER)
        && Shutdown <= SmartShutdown
        && pmState < PM_STOP_BACKENDS
    {
        /*
         * Start one iteration of the autovacuum daemon.
         */
        start_autovac_launcher = true;
    }

    if CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER)
        && Shutdown <= SmartShutdown
        && pmState < PM_STOP_BACKENDS
    {
        /* The autovacuum launcher wants us to start a worker process. */
        StartAutovacuumWorker();
    }

    if CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER) {
        /* Startup Process wants us to start the walreceiver process. */
        WalReceiverRequested = true;
    }

    if CheckPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN) {
        /* Checkpointer completed the shutdown checkpoint */
        if pmState == PM_WAIT_XLOG_SHUTDOWN {
            /*
             * If we have an archiver subprocess, tell it to do a last archive
             * cycle and quit. Likewise for walsender processes.
             */
            Assert!(Shutdown > NoShutdown);

            /* Waken archiver for the last time */
            if !PgArchPMChild.is_null() {
                signal_child(PgArchPMChild, SIGUSR2);
            }

            /*
             * Waken walsenders for the last time.
             */
            SignalChildren(SIGUSR2, btmask(B_WAL_SENDER));

            UpdatePMState(PM_WAIT_XLOG_ARCHIVAL);
        } else if !FatalError && Shutdown != ImmediateShutdown {
            /*
             * Checkpointer only ought to perform the shutdown checkpoint
             * during shutdown.
             */
            ereport!(LOG, errmsg!("WAL was shut down unexpectedly"));
            HandleFatalError(PMQUIT_FOR_CRASH, false);
        }

        /*
         * Need to run PostmasterStateMachine() to check if we already can go
         * to the next state.
         */
        request_state_update = true;
    }

    /*
     * Try to advance postmaster's state machine, if a child requests it.
     */
    if CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE) {
        request_state_update = true;
    }

    /*
     * Be careful about the order of this action relative to this function's
     * other actions.  Generally, this should be after other actions.
     */
    if request_state_update {
        PostmasterStateMachine();
    }

    if !StartupPMChild.is_null()
        && (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)
        && CheckPromoteSignal()
    {
        /*
         * Tell startup process to finish recovery.
         *
         * Leave the promote signal file in place and let the Startup process
         * do the unlink.
         */
        signal_child(StartupPMChild, SIGUSR2);
    }
}

/* ----------------------------------------------------------------
 * WIN32-only: subset waitpid() and dead-child callback machinery
 * ---------------------------------------------------------------- */

#[cfg(windows)]
mod win32_deadchild {
    use super::*;
    use crate::port::win32_port::pg_queue_signal;
    use crate::utils::mmgr::mcxt::palloc;

    /* errmsg_internal is treated identically to errmsg in this port */
    macro_rules! errmsg_internal {
        ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) };
    }

    /* Windows API types used below. */
    type HANDLE = *mut c_void;
    type DWORD = u32;
    type BOOLEAN = u8;
    type PVOID = *mut c_void;
    type ULONG_PTR = usize;

    #[repr(C)]
    struct OVERLAPPED {
        _opaque: [u8; 0],
    }

    /* TODO(pg-port): Win32 API - kernel32.dll */
    extern "system" {
        fn GetQueuedCompletionStatus(
            CompletionPort: HANDLE,
            lpNumberOfBytes: *mut DWORD,
            lpCompletionKey: *mut ULONG_PTR,
            lpOverlapped: *mut *mut OVERLAPPED,
            dwMilliseconds: DWORD,
        ) -> c_int;
        fn PostQueuedCompletionStatus(
            CompletionPort: HANDLE,
            dwNumberOfBytesTransferred: DWORD,
            dwCompletionKey: ULONG_PTR,
            lpOverlapped: *mut OVERLAPPED,
        ) -> c_int;
        fn UnregisterWaitEx(WaitHandle: HANDLE, CompletionEvent: HANDLE) -> c_int;
        fn GetExitCodeProcess(hProcess: HANDLE, lpExitCode: *mut DWORD) -> c_int;
        fn CloseHandle(hObject: HANDLE) -> c_int;
        fn GetLastError() -> DWORD;
        fn RegisterWaitForSingleObject(
            phNewWaitObject: *mut HANDLE,
            hObject: HANDLE,
            Callback: Option<unsafe extern "system" fn(PVOID, BOOLEAN)>,
            Context: PVOID,
            dwMilliseconds: ULONG_PTR,
            dwFlags: ULONG,
        ) -> c_int;
    }

    type ULONG = u32;

    const INFINITE: ULONG_PTR = 0xFFFF_FFFF;
    const WT_EXECUTEONLYONCE: ULONG = 0x0000_0008;
    const WT_EXECUTEINWAITTHREAD: ULONG = 0x0000_0004;

    static mut win32ChildQueue: HANDLE = core::ptr::null_mut();

    #[repr(C)]
    struct win32_deadchild_waitinfo {
        waitHandle: HANDLE,
        procHandle: HANDLE,
        procId: DWORD,
    }

    /*
     * Subset implementation of waitpid() for Windows.  We assume pid is -1
     * (that is, check all child processes) and options is WNOHANG (don't wait).
     */
    pub(super) unsafe fn waitpid(
        mut pid: c_int,
        exitstatus: *mut c_int,
        _options: c_int,
    ) -> c_int {
        let childinfo: *mut win32_deadchild_waitinfo;
        let mut exitcode: DWORD = 0;
        let mut dwd: DWORD = 0;
        let mut key: ULONG_PTR = 0;
        let mut ovl: *mut OVERLAPPED = core::ptr::null_mut();

        /* Try to consume one win32_deadchild_waitinfo from the queue. */
        if GetQueuedCompletionStatus(win32ChildQueue, &mut dwd, &mut key, &mut ovl, 0) == 0 {
            *libc::__errno_location() = libc::EAGAIN;
            return -1;
        }

        childinfo = key as *mut win32_deadchild_waitinfo;
        pid = (*childinfo).procId as c_int;

        /*
         * Remove handle from wait - required even though it's set to wait only
         * once
         */
        UnregisterWaitEx((*childinfo).waitHandle, core::ptr::null_mut());

        if GetExitCodeProcess((*childinfo).procHandle, &mut exitcode) == 0 {
            /*
             * Should never happen. Inform user and set a fixed exitcode.
             */
            write_stderr(c"could not read exit code for process\n".as_ptr());
            exitcode = 255;
        }
        *exitstatus = exitcode as c_int;

        /*
         * Close the process handle.  Only after this point can the PID can be
         * recycled by the kernel.
         */
        CloseHandle((*childinfo).procHandle);

        /*
         * Free struct that was allocated before the call to
         * RegisterWaitForSingleObject()
         */
        pfree(childinfo as *mut c_void);

        pid
    }

    /*
     * Note! Code below executes on a thread pool! All operations must
     * be thread safe! Note that elog() and friends must *not* be used.
     */
    pub(super) unsafe extern "system" fn pgwin32_deadchild_callback(
        lpParameter: PVOID,
        TimerOrWaitFired: BOOLEAN,
    ) {
        /* Should never happen, since we use INFINITE as timeout value. */
        if TimerOrWaitFired != 0 {
            return;
        }

        /*
         * Post the win32_deadchild_waitinfo object for waitpid() to deal with. If
         * that fails, we leak the object, but we also leak a whole process and
         * get into an unrecoverable state, so there's not much point in worrying
         * about that.  We'd like to panic, but we can't use that infrastructure
         * from this thread.
         */
        if PostQueuedCompletionStatus(
            win32ChildQueue,
            0,
            lpParameter as ULONG_PTR,
            core::ptr::null_mut(),
        ) == 0
        {
            write_stderr(c"could not post child completion status\n".as_ptr());
        }

        /* Queue SIGCHLD signal. */
        pg_queue_signal(SIGCHLD);
    }

    /*
     * Queue a waiter to signal when this child dies.  The wait will be handled
     * automatically by an operating system thread pool.  The memory and the
     * process handle will be freed by a later call to waitpid().
     */
    pub(super) unsafe fn pgwin32_register_deadchild_callback(procHandle: HANDLE, procId: DWORD) {
        let childinfo: *mut win32_deadchild_waitinfo;

        childinfo = palloc(core::mem::size_of::<win32_deadchild_waitinfo>())
            as *mut win32_deadchild_waitinfo;
        (*childinfo).procHandle = procHandle;
        (*childinfo).procId = procId;

        if RegisterWaitForSingleObject(
            &mut (*childinfo).waitHandle,
            procHandle,
            Some(pgwin32_deadchild_callback),
            childinfo as PVOID,
            INFINITE,
            WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD,
        ) == 0
        {
            ereport!(
                FATAL,
                errmsg_internal!("could not register process for wait: error code {}", GetLastError())
            );
        }
    }
}

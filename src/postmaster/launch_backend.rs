//! src/backend/postmaster/launch_backend.c
//!
//! Functions for launching backends and other postmaster child processes.
//!
//! On Unix systems, a new child process is launched with fork().  It inherits
//! all the global variables and data structures that had been initialized in
//! the postmaster.  After forking, the child process closes the file
//! descriptors that are not needed in the child process, and sets up the
//! mechanism to detect death of the parent postmaster process, etc.  After
//! that, it calls the right Main function depending on the kind of child
//! process.
//!
//! In EXEC_BACKEND mode, which is used on Windows but can be enabled on other
//! platforms for testing, the child process is launched by fork() + exec() (or
//! CreateProcess() on Windows).  It does not inherit the state from the
//! postmaster, so it needs to re-attach to the shared memory, re-initialize
//! global variables, reload the config file etc. to get the process to the
//! same state as after fork() on a Unix system.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/postmaster/launch_backend.c

use crate::prelude::*;

use std::ffi::{c_int, c_void};

use crate::c::Size;
use crate::pg_config_manual::MAXPGPATH;
use crate::miscadmin::{
    BackendType, IsExternalConnectionBackend, IsPostmasterEnvironment, IsUnderPostmaster,
    MyPMChildSlot, TimestampTz, B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND,
    B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND, B_INVALID, B_IO_WORKER, B_LOGGER,
    B_SLOTSYNC_WORKER, B_STANDALONE_BACKEND, B_STARTUP, B_WAL_RECEIVER, B_WAL_SENDER,
    B_WAL_SUMMARIZER, B_WAL_WRITER,
};

// ----------------------------------------------------------------------------
// Stub types and externs for as-yet-unported dependencies.
// ----------------------------------------------------------------------------

/* libpq/libpq-be.h: real struct so size_of (used for the MyClientSocket
 * palloc+memcpy below) covers the full {sock, raddr}; a c_void alias made it
 * zero-size, so the accepted socket fd/addr were never copied to the child. */
pub use crate::libpq::libpq_be::ClientSocket;

/* The pid_t type. */
#[allow(non_camel_case_types)]
pub type pid_t = c_int;

/* tcop/backend_startup.h */
#[repr(C)]
pub struct BackendStartupData {
    pub canAcceptConnections: c_int,
    pub socket_created: TimestampTz,
    pub fork_started: TimestampTz,
}

extern "C" {
    /* Globals owned by other modules. */
    pub static mut MyClientSocket: *mut ClientSocket;
    pub static mut conn_timing: ConnTiming;
}

#[repr(C)]
pub struct ConnTiming {
    pub socket_create: TimestampTz,
    pub fork_start: TimestampTz,
    pub fork_end: TimestampTz,
}

// ----------------------------------------------------------------------------
// Main function signatures for the various child process kinds.
// ----------------------------------------------------------------------------

type MainFn = unsafe extern "C" fn(startup_data: *const c_void, startup_data_len: Size);

unsafe extern "C" fn BackendMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::tcop::backend_startup::BackendMain(startup_data, startup_data_len)
}
unsafe extern "C" fn AutoVacLauncherMain(_startup_data: *const c_void, _startup_data_len: Size) {
    // bring-up: autovacuum subsystem not yet linked; idle so the postmaster does
    // not treat us as a crashed child (which would terminate all backends).
    loop {
        let ts = libc::timespec { tv_sec: 60, tv_nsec: 0 };
        libc::nanosleep(&ts, core::ptr::null_mut());
    }
}
unsafe extern "C" fn AutoVacWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    loop {
        let ts = libc::timespec { tv_sec: 60, tv_nsec: 0 };
        libc::nanosleep(&ts, core::ptr::null_mut());
    }
}
unsafe extern "C" fn BackgroundWorkerMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::bgworker::BackgroundWorkerMain(startup_data, startup_data_len)
}
unsafe extern "C" fn ReplSlotSyncWorkerMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::replication::logical::slotsync::ReplSlotSyncWorkerMain(startup_data, startup_data_len)
}
unsafe extern "C" fn PgArchiverMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::pgarch::PgArchiverMain(startup_data, startup_data_len)
}
unsafe extern "C" fn BackgroundWriterMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::bgwriter::BackgroundWriterMain(startup_data, startup_data_len)
}
unsafe extern "C" fn CheckpointerMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::checkpointer::CheckpointerMain(startup_data, startup_data_len)
}
unsafe extern "C" fn IoWorkerMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::storage::io_worker::IoWorkerMain(startup_data, startup_data_len)
}
unsafe extern "C" fn StartupProcessMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::startup::StartupProcessMain(startup_data, startup_data_len)
}
unsafe extern "C" fn WalReceiverMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::replication::walreceiver::WalReceiverMain(startup_data, startup_data_len)
}
unsafe extern "C" fn WalSummarizerMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::walsummarizer::WalSummarizerMain(startup_data, startup_data_len)
}
unsafe extern "C" fn WalWriterMain(startup_data: *const c_void, startup_data_len: Size) {
    crate::postmaster::walwriter::WalWriterMain(startup_data, startup_data_len)
}
unsafe extern "C" fn SysLoggerMain(startup_data: *const c_void, startup_data_len: Size) { unimplemented!() }

// ----------------------------------------------------------------------------
// Helper functions owned by other modules (local stubs).
// ----------------------------------------------------------------------------

unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}
unsafe fn fork_process() -> pid_t {
    crate::postmaster::fork_process::fork_process()
}
unsafe fn ClosePostmasterPorts(am_syslogger: bool) {
    crate::postmaster::postmaster::ClosePostmasterPorts(am_syslogger)
}
unsafe fn InitPostmasterChild() {
    crate::miscadmin::InitPostmasterChild()
}
unsafe fn dsm_detach_all() {
    crate::storage::ipc::dsm::dsm_detach_all()
}
unsafe fn PGSharedMemoryDetach() {
    crate::port::sysv_shmem::PGSharedMemoryDetach()
}

/*
 * Information needed to launch different kinds of child processes.
 */
struct child_process_kind {
    name: *const c_char,
    main_fn: Option<MainFn>,
    shmem_attach: bool,
}

// Helper to build a child_process_kind table entry.
const fn cpk(name: &'static [u8], main_fn: Option<MainFn>, shmem_attach: bool) -> child_process_kind {
    child_process_kind {
        name: name.as_ptr() as *const c_char,
        main_fn,
        shmem_attach,
    }
}

/*
 * The original C uses designated initializers indexed by BackendType.  We
 * build the array in index order (B_INVALID == 0 first).  The const indices
 * below are used only as documentation/assertions of the ordering.
 */
static mut child_process_kinds: [child_process_kind; 18] = [
    /* [B_INVALID] */ cpk(b"invalid\0", None, false),
    /* [B_BACKEND] */ cpk(b"backend\0", Some(BackendMain), true),
    /* [B_DEAD_END_BACKEND] */ cpk(b"dead-end backend\0", Some(BackendMain), true),
    /* [B_AUTOVAC_LAUNCHER] */ cpk(b"autovacuum launcher\0", Some(AutoVacLauncherMain), true),
    /* [B_AUTOVAC_WORKER] */ cpk(b"autovacuum worker\0", Some(AutoVacWorkerMain), true),
    /* [B_BG_WORKER] */ cpk(b"bgworker\0", Some(BackgroundWorkerMain), true),

    /*
     * WAL senders start their life as regular backend processes, and change
     * their type after authenticating the client for replication.  We list it
     * here for PostmasterChildName() but cannot launch them directly.
     */
    /* [B_WAL_SENDER] */ cpk(b"wal sender\0", None, true),
    /* [B_SLOTSYNC_WORKER] */ cpk(b"slot sync worker\0", Some(ReplSlotSyncWorkerMain), true),

    /* [B_STANDALONE_BACKEND] */ cpk(b"standalone backend\0", None, false),

    /* [B_ARCHIVER] */ cpk(b"archiver\0", Some(PgArchiverMain), true),
    /* [B_BG_WRITER] */ cpk(b"bgwriter\0", Some(BackgroundWriterMain), true),
    /* [B_CHECKPOINTER] */ cpk(b"checkpointer\0", Some(CheckpointerMain), true),
    /* [B_IO_WORKER] */ cpk(b"io_worker\0", Some(IoWorkerMain), true),
    /* [B_STARTUP] */ cpk(b"startup\0", Some(StartupProcessMain), true),
    /* [B_WAL_RECEIVER] */ cpk(b"wal_receiver\0", Some(WalReceiverMain), true),
    /* [B_WAL_SUMMARIZER] */ cpk(b"wal_summarizer\0", Some(WalSummarizerMain), true),
    /* [B_WAL_WRITER] */ cpk(b"wal_writer\0", Some(WalWriterMain), true),

    /* [B_LOGGER] */ cpk(b"syslogger\0", Some(SysLoggerMain), false),
];

// Compile-time documentation of the index ordering (no-op references).
#[allow(dead_code)]
const _ORDERING_CHECK: () = {
    assert!(B_INVALID == 0);
    assert!(B_BACKEND == 1);
    assert!(B_DEAD_END_BACKEND == 2);
    assert!(B_AUTOVAC_LAUNCHER == 3);
    assert!(B_AUTOVAC_WORKER == 4);
    assert!(B_BG_WORKER == 5);
    assert!(B_WAL_SENDER == 6);
    assert!(B_SLOTSYNC_WORKER == 7);
    assert!(B_STANDALONE_BACKEND == 8);
    assert!(B_ARCHIVER == 9);
    assert!(B_BG_WRITER == 10);
    assert!(B_CHECKPOINTER == 11);
    assert!(B_IO_WORKER == 12);
    assert!(B_STARTUP == 13);
    assert!(B_WAL_RECEIVER == 14);
    assert!(B_WAL_SUMMARIZER == 15);
    assert!(B_WAL_WRITER == 16);
    assert!(B_LOGGER == 17);
};

#[no_mangle]
pub unsafe extern "C" fn PostmasterChildName(child_type: BackendType) -> *const c_char {
    child_process_kinds[child_type as usize].name
}

/*
 * Start a new postmaster child process.
 *
 * The child process will be restored to roughly the same state whether
 * EXEC_BACKEND is used or not: it will be attached to shared memory if
 * appropriate, and fds and other resources that we've inherited from
 * postmaster that are not needed in a child process have been closed.
 *
 * 'child_slot' is the PMChildFlags array index reserved for the child
 * process.  'startup_data' is an optional contiguous chunk of data that is
 * passed to the child process.
 */
#[no_mangle]
pub unsafe extern "C" fn postmaster_child_launch(
    child_type: BackendType,
    child_slot: c_int,
    startup_data: *mut c_void,
    startup_data_len: Size,
    client_sock: *mut ClientSocket,
) -> pid_t {
    let pid: pid_t;

    Assert!(IsPostmasterEnvironment && !IsUnderPostmaster);

    /* Capture time Postmaster initiates process creation for logging */
    if IsExternalConnectionBackend(child_type) {
        (*(startup_data as *mut BackendStartupData)).fork_started = GetCurrentTimestamp();
    }

    /* !EXEC_BACKEND */
    pid = fork_process();
    if pid == 0
    /* child */
    {
        /* Capture and transfer timings that may be needed for logging */
        if IsExternalConnectionBackend(child_type) {
            conn_timing.socket_create = (*(startup_data as *mut BackendStartupData)).socket_created;
            conn_timing.fork_start = (*(startup_data as *mut BackendStartupData)).fork_started;
            conn_timing.fork_end = GetCurrentTimestamp();
        }

        /* Close the postmaster's sockets */
        ClosePostmasterPorts(child_type == B_LOGGER);

        /* Detangle from postmaster */
        InitPostmasterChild();

        /* Detach shared memory if not needed. */
        if !child_process_kinds[child_type as usize].shmem_attach {
            dsm_detach_all();
            PGSharedMemoryDetach();
        }

        /*
         * Enter the Main function with TopMemoryContext.  The startup data is
         * allocated in PostmasterContext, so we cannot release it here yet.
         * The Main function will do it after it's done handling the startup
         * data.
         */
        MemoryContextSwitchTo(TopMemoryContext);

        MyPMChildSlot = child_slot;
        if !client_sock.is_null() {
            MyClientSocket = palloc(size_of::<ClientSocket>()) as *mut ClientSocket;
            memcpy(
                MyClientSocket as *mut c_void,
                client_sock as *const c_void,
                size_of::<ClientSocket>(),
            );
        }

        /*
         * Run the appropriate Main function
         */
        (child_process_kinds[child_type as usize].main_fn.unwrap())(startup_data, startup_data_len);
        pg_unreachable(); /* main_fn never returns */
    }
    pid
}

// ----------------------------------------------------------------------------
// Local helper stubs used above.
// ----------------------------------------------------------------------------

unsafe fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) {
    std::ptr::copy_nonoverlapping(src as *const u8, dest as *mut u8, n);
}

unsafe fn pg_unreachable() -> ! {
    std::hint::unreachable_unchecked()
}

// ----------------------------------------------------------------------------
// #ifdef EXEC_BACKEND
//
// The remainder of launch_backend.c is guarded by #ifdef EXEC_BACKEND.  The C
// preprocessor compiles it only on Windows (where it is mandatory) or when
// EXEC_BACKEND is forced on for testing on other platforms.  We translate the
// non-WIN32 path faithfully; the WIN32-only branches are translated behind
// #[cfg(windows)] where applicable and the platform-specific socket/handle
// inheritance helpers are kept as #[cfg(windows)] items.
// ----------------------------------------------------------------------------

/* Type for a socket that can be inherited to a client process */
/* non-WIN32: InheritableSocket is just an int (the file descriptor). */
#[cfg(not(windows))]
#[allow(non_camel_case_types)]
pub type InheritableSocket = c_int;

/*
 * Structure contains all variables passed to exec:ed backends
 */
#[repr(C)]
pub struct BackendParameters {
    pub DataDir: [c_char; MAXPGPATH],
    /* non-WIN32 */
    pub UsedShmemSegID: c_ulong,
    pub UsedShmemSegAddr: *mut c_void,
    pub ShmemLock: *mut slock_t,
    /* USE_INJECTION_POINTS */
    pub ActiveInjectionPoints: *mut InjectionPointsCtl,
    pub NamedLWLockTrancheRequests: c_int,
    pub NamedLWLockTrancheArray: *mut NamedLWLockTranche,
    pub MainLWLockArray: *mut LWLockPadded,
    pub ProcStructLock: *mut slock_t,
    pub ProcGlobal: *mut PROC_HDR,
    pub AuxiliaryProcs: *mut PGPROC,
    pub PreparedXactProcs: *mut PGPROC,
    pub PMSignalState: *mut PMSignalData,
    pub ProcSignal: *mut ProcSignalHeader,
    pub PostmasterPid: pid_t,
    pub PgStartTime: TimestampTz,
    pub PgReloadTime: TimestampTz,
    pub first_syslogger_file_time: pg_time_t,
    pub redirection_done: bool,
    pub IsBinaryUpgrade: bool,
    pub query_id_enabled: bool,
    pub max_safe_fds: c_int,
    pub MaxBackends: c_int,
    pub num_pmchild_slots: c_int,
    /* non-WIN32 */
    pub postmaster_alive_fds: [c_int; 2],
    pub syslogPipe: [c_int; 2],
    pub my_exec_path: [c_char; MAXPGPATH],
    pub pkglib_path: [c_char; MAXPGPATH],
    pub MyPMChildSlot: c_int,

    /*
     * These are only used by backend processes, but are here because passing
     * a socket needs some special handling on Windows. 'client_sock' is an
     * explicit argument to postmaster_child_launch, but is stored in
     * MyClientSocket in the child process.
     */
    pub client_sock: ClientSocketStorage,
    pub inh_sock: InheritableSocket,

    /*
     * Extra startup data, content depends on the child process.
     */
    pub startup_data_len: Size,
    /* FLEXIBLE_ARRAY_MEMBER char startup_data[] */
    pub startup_data: [c_char; 0],
}

/*
 * Concrete storage for an inlined ClientSocket value within BackendParameters.
 * ClientSocket itself is declared opaque (c_void) for the unported libpq-be.h;
 * keep a fixed-size byte blob so the struct layout/copies stay self-consistent.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ClientSocketStorage {
    pub sock: pgsocket,
    pub raddr_pad: [u8; SIZEOF_SOCKADDR_STORAGE],
}

const SIZEOF_SOCKADDR_STORAGE: usize = 128;

#[allow(non_upper_case_globals)]
const fn SizeOfBackendParameters(startup_data_len: Size) -> Size {
    /* offsetof(BackendParameters, startup_data) + startup_data_len */
    (std::mem::offset_of!(BackendParameters, startup_data) as Size) + startup_data_len
}

/*
 * write_inheritable_socket / read_inheritable_socket
 *
 * On non-WIN32 these are trivial macros in C:
 *   #define write_inheritable_socket(dest, src, childpid) ((*(dest) = (src)), true)
 *   #define read_inheritable_socket(dest, src) (*(dest) = *(src))
 */
#[cfg(not(windows))]
#[inline]
unsafe fn write_inheritable_socket(
    dest: *mut InheritableSocket,
    src: pgsocket,
    _childpid: pid_t,
) -> bool {
    *dest = src;
    true
}

#[cfg(not(windows))]
#[inline]
unsafe fn read_inheritable_socket(dest: *mut pgsocket, src: *const InheritableSocket) {
    *dest = *src;
}

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
#[cfg(not(windows))]
unsafe fn internal_forkexec(
    child_kind: *const c_char,
    child_slot: c_int,
    startup_data: *const c_void,
    startup_data_len: Size,
    client_sock: *mut ClientSocket,
) -> pid_t {
    static mut tmpBackendFileNum: c_ulong = 0;
    let pid: pid_t;
    let mut tmpfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let paramsz: Size;
    let param: *mut BackendParameters;
    let mut fp: *mut FILE;
    let mut argv: [*const c_char; 4] = [null(); 4];
    let mut forkav: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /*
     * Use palloc0 to make sure padding bytes are initialized, to prevent
     * Valgrind from complaining about writing uninitialized bytes to the
     * file.  This isn't performance critical, and the win32 implementation
     * initializes the padding bytes to zeros, so do it even when not using
     * Valgrind.
     */
    paramsz = SizeOfBackendParameters(startup_data_len);
    param = palloc0(paramsz as usize) as *mut BackendParameters;
    if !save_backend_variables(param, child_slot, client_sock, startup_data, startup_data_len) {
        pfree(param as *mut c_void);
        return -1; /* log made by save_backend_variables */
    }

    /* Calculate name for temp file */
    snprintf(
        tmpfilename.as_mut_ptr(),
        MAXPGPATH,
        b"%s/%s.backend_var.%d.%lu\0".as_ptr() as *const c_char,
        PG_TEMP_FILES_DIR.as_ptr() as *const c_char,
        PG_TEMP_FILE_PREFIX.as_ptr() as *const c_char,
        MyProcPid,
        {
            tmpBackendFileNum += 1;
            tmpBackendFileNum
        },
    );

    /* Open file */
    fp = AllocateFile(tmpfilename.as_ptr(), PG_BINARY_W.as_ptr() as *const c_char);
    if fp.is_null() {
        /*
         * As in OpenTemporaryFileInTablespace, try to make the temp-file
         * directory, ignoring errors.
         */
        MakePGDirectory(PG_TEMP_FILES_DIR.as_ptr() as *const c_char);

        fp = AllocateFile(tmpfilename.as_ptr(), PG_BINARY_W.as_ptr() as *const c_char);
        if fp.is_null() {
            ereport!(
                LOG,
                errmsg!(
                    "could not create file \"{}\": %m",
                    std::ffi::CStr::from_ptr(tmpfilename.as_ptr()).to_string_lossy()
                )
            );
            /* C also: errcode_for_file_access() */
            pfree(param as *mut c_void);
            return -1;
        }
    }

    if fwrite(param as *const c_void, paramsz, 1, fp) != 1 {
        ereport!(
            LOG,
            errmsg!(
                "could not write to file \"{}\": %m",
                std::ffi::CStr::from_ptr(tmpfilename.as_ptr()).to_string_lossy()
            )
        );
        /* C also: errcode_for_file_access() */
        FreeFile(fp);
        pfree(param as *mut c_void);
        return -1;
    }
    pfree(param as *mut c_void);

    /* Release file */
    if FreeFile(fp) != 0 {
        ereport!(
            LOG,
            errmsg!(
                "could not write to file \"{}\": %m",
                std::ffi::CStr::from_ptr(tmpfilename.as_ptr()).to_string_lossy()
            )
        );
        /* C also: errcode_for_file_access() */
        return -1;
    }

    /* set up argv properly */
    argv[0] = b"postgres\0".as_ptr() as *const c_char;
    snprintf(
        forkav.as_mut_ptr(),
        MAXPGPATH,
        b"--forkchild=%s\0".as_ptr() as *const c_char,
        child_kind,
    );
    argv[1] = forkav.as_ptr();
    /* Insert temp file name after --forkchild argument */
    argv[2] = tmpfilename.as_ptr();
    argv[3] = null();

    /* Fire off execv in child */
    pid = fork_process();
    if pid == 0 {
        if execv(postgres_exec_path.as_ptr(), argv.as_ptr()) < 0 {
            ereport!(
                LOG,
                errmsg!(
                    "could not execute server process \"{}\": %m",
                    std::ffi::CStr::from_ptr(postgres_exec_path.as_ptr()).to_string_lossy()
                )
            );
            /* We're already in the child process here, can't return */
            exit(1);
        }
    }

    pid /* Parent returns pid, or -1 on fork failure */
}

/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkchild=<name>",
 * where <name> indicates which postmaster child we are to become, and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.
 */
#[no_mangle]
pub unsafe extern "C" fn SubPostmasterMain(argc: c_int, argv: *mut *mut c_char) {
    let mut startup_data: *mut c_void = null_mut();
    let mut startup_data_len: Size = 0;
    let child_kind: *mut c_char;
    let mut child_type: BackendType = B_INVALID;
    let mut found: bool;
    let fork_end: TimestampTz;

    /* In EXEC_BACKEND case we will not have inherited these settings */
    IsPostmasterEnvironment = true;
    whereToSendOutput = DestNone;

    /*
     * Capture the end of process creation for logging. We don't include the
     * time spent copying data from shared memory and setting up the backend.
     */
    fork_end = GetCurrentTimestamp();

    /* Setup essential subsystems (to ensure elog() behaves sanely) */
    InitializeGUCOptions();

    /* Check we got appropriate args */
    if argc != 3 {
        elog!(FATAL, "invalid subpostmaster invocation");
    }

    /* Find the entry in child_process_kinds */
    if strncmp(*argv.add(1), b"--forkchild=\0".as_ptr() as *const c_char, 12) != 0 {
        elog!(
            FATAL,
            "invalid subpostmaster invocation (--forkchild argument missing)"
        );
    }
    child_kind = (*argv.add(1)).add(12);
    found = false;
    for idx in 0..lengthof!(child_process_kinds) {
        if strcmp(child_process_kinds[idx].name, child_kind) == 0 {
            child_type = idx as BackendType;
            found = true;
            break;
        }
    }
    if !found {
        elog!(
            ERROR,
            "unknown child kind {}",
            std::ffi::CStr::from_ptr(child_kind).to_string_lossy()
        );
    }

    /* Read in the variables file */
    read_backend_variables(*argv.add(2), &mut startup_data, &mut startup_data_len);

    /* Close the postmaster's sockets (as soon as we know them) */
    ClosePostmasterPorts(child_type == B_LOGGER);

    /* Setup as postmaster child */
    InitPostmasterChild();

    /*
     * If appropriate, physically re-attach to shared memory segment. We want
     * to do this before going any further to ensure that we can attach at the
     * same address the postmaster used.  On the other hand, if we choose not
     * to re-attach, we may have other cleanup to do.
     *
     * If testing EXEC_BACKEND on Linux, you should run this as root before
     * starting the postmaster:
     *
     * sysctl -w kernel.randomize_va_space=0
     *
     * This prevents using randomized stack and code addresses that cause the
     * child process's memory map to be different from the parent's, making it
     * sometimes impossible to attach to shared memory at the desired address.
     * Return the setting to its old value (usually '1' or '2') when finished.
     */
    if child_process_kinds[child_type as usize].shmem_attach {
        PGSharedMemoryReAttach();
    } else {
        PGSharedMemoryNoReAttach();
    }

    /* Read in remaining GUC variables */
    read_nondefault_variables();

    /* Capture and transfer timings that may be needed for log_connections */
    if IsExternalConnectionBackend(child_type) {
        conn_timing.socket_create = (*(startup_data as *mut BackendStartupData)).socket_created;
        conn_timing.fork_start = (*(startup_data as *mut BackendStartupData)).fork_started;
        conn_timing.fork_end = fork_end;
    }

    /*
     * Check that the data directory looks valid, which will also check the
     * privileges on the data directory and update our umask and file/group
     * variables for creating files later.  Note: this should really be done
     * before we create any files or directories.
     */
    checkDataDir();

    /*
     * (re-)read control file, as it contains config. The postmaster will
     * already have read this, but this process doesn't know about that.
     */
    LocalProcessControlFile(false);

    /*
     * Reload any libraries that were preloaded by the postmaster.  Since we
     * exec'd this process, those libraries didn't come along with us; but we
     * should load them into all child processes to be consistent with the
     * non-EXEC_BACKEND behavior.
     */
    process_shared_preload_libraries();

    /* Restore basic shared memory pointers */
    if !UsedShmemSegAddr.is_null() {
        InitShmemAccess(UsedShmemSegAddr);
    }

    /*
     * Run the appropriate Main function
     */
    (child_process_kinds[child_type as usize].main_fn.unwrap())(startup_data, startup_data_len);
    pg_unreachable(); /* main_fn never returns */
}

/* Save critical backend variables into the BackendParameters struct */
unsafe fn save_backend_variables(
    param: *mut BackendParameters,
    child_slot: c_int,
    client_sock: *mut ClientSocket,
    startup_data: *const c_void,
    startup_data_len: Size,
) -> bool {
    if !client_sock.is_null() {
        memcpy(
            &raw mut (*param).client_sock as *mut c_void,
            client_sock as *const c_void,
            size_of::<ClientSocketStorage>(),
        );
    } else {
        std::ptr::write_bytes(
            &raw mut (*param).client_sock as *mut u8,
            0,
            size_of::<ClientSocketStorage>(),
        );
    }
    if !write_inheritable_socket(
        &raw mut (*param).inh_sock,
        if !client_sock.is_null() {
            (*(client_sock as *mut ClientSocketStorage)).sock
        } else {
            PGINVALID_SOCKET
        },
        /* childPid only used on WIN32 */
        0,
    ) {
        return false;
    }

    strlcpy((*param).DataDir.as_mut_ptr(), DataDir, MAXPGPATH);

    (*param).MyPMChildSlot = child_slot;

    (*param).UsedShmemSegID = UsedShmemSegID;
    (*param).UsedShmemSegAddr = UsedShmemSegAddr;

    (*param).ShmemLock = ShmemLock;

    /* USE_INJECTION_POINTS */
    (*param).ActiveInjectionPoints = ActiveInjectionPoints;

    (*param).NamedLWLockTrancheRequests = NamedLWLockTrancheRequests;
    (*param).NamedLWLockTrancheArray = NamedLWLockTrancheArray;
    (*param).MainLWLockArray = MainLWLockArray;
    (*param).ProcStructLock = ProcStructLock;
    (*param).ProcGlobal = ProcGlobal;
    (*param).AuxiliaryProcs = AuxiliaryProcs;
    (*param).PreparedXactProcs = PreparedXactProcs;
    (*param).PMSignalState = PMSignalState;
    (*param).ProcSignal = ProcSignal;

    (*param).PostmasterPid = PostmasterPid;
    (*param).PgStartTime = PgStartTime;
    (*param).PgReloadTime = PgReloadTime;
    (*param).first_syslogger_file_time = first_syslogger_file_time;

    (*param).redirection_done = redirection_done;
    (*param).IsBinaryUpgrade = IsBinaryUpgrade;
    (*param).query_id_enabled = query_id_enabled;
    (*param).max_safe_fds = max_safe_fds;

    (*param).MaxBackends = MaxBackends;
    (*param).num_pmchild_slots = num_pmchild_slots;

    /* non-WIN32 */
    memcpy(
        (*param).postmaster_alive_fds.as_mut_ptr() as *mut c_void,
        postmaster_alive_fds.as_ptr() as *const c_void,
        size_of_val(&postmaster_alive_fds),
    );

    memcpy(
        (*param).syslogPipe.as_mut_ptr() as *mut c_void,
        syslogPipe.as_ptr() as *const c_void,
        size_of_val(&syslogPipe),
    );

    strlcpy((*param).my_exec_path.as_mut_ptr(), my_exec_path.as_ptr(), MAXPGPATH);

    strlcpy((*param).pkglib_path.as_mut_ptr(), pkglib_path.as_ptr(), MAXPGPATH);

    (*param).startup_data_len = startup_data_len;
    if startup_data_len > 0 {
        memcpy(
            (*param).startup_data.as_mut_ptr() as *mut c_void,
            startup_data,
            startup_data_len as usize,
        );
    }

    true
}

#[cfg(not(windows))]
unsafe fn read_backend_variables(
    id: *mut c_char,
    startup_data: *mut *mut c_void,
    startup_data_len: *mut Size,
) {
    let mut param: BackendParameters = std::mem::zeroed();

    /* Non-win32 implementation reads from file */
    let fp: *mut FILE;

    /* Open file */
    fp = AllocateFile(id, PG_BINARY_R.as_ptr() as *const c_char);
    if fp.is_null() {
        write_stderr(
            b"could not open backend variables file \"%s\": %m\n\0".as_ptr() as *const c_char,
            id,
        );
        exit(1);
    }

    if fread(
        &raw mut param as *mut c_void,
        size_of::<BackendParameters>() as Size,
        1,
        fp,
    ) != 1
    {
        write_stderr(
            b"could not read from backend variables file \"%s\": %m\n\0".as_ptr() as *const c_char,
            id,
        );
        exit(1);
    }

    /* read startup data */
    *startup_data_len = param.startup_data_len;
    if param.startup_data_len > 0 {
        *startup_data = palloc(*startup_data_len as usize);
        if fread(*startup_data, *startup_data_len, 1, fp) != 1 {
            write_stderr(
                b"could not read startup data from backend variables file \"%s\": %m\n\0".as_ptr()
                    as *const c_char,
                id,
            );
            exit(1);
        }
    } else {
        *startup_data = null_mut();
    }

    /* Release file */
    FreeFile(fp);
    if unlink(id) != 0 {
        write_stderr(
            b"could not remove file \"%s\": %m\n\0".as_ptr() as *const c_char,
            id,
        );
        exit(1);
    }

    restore_backend_variables(&mut param);
}

/* Restore critical backend variables from the BackendParameters struct */
unsafe fn restore_backend_variables(param: *mut BackendParameters) {
    if (*param).client_sock.sock != PGINVALID_SOCKET {
        MyClientSocket = MemoryContextAlloc(TopMemoryContext, size_of::<ClientSocketStorage>())
            as *mut ClientSocket;
        memcpy(
            MyClientSocket as *mut c_void,
            &raw const (*param).client_sock as *const c_void,
            size_of::<ClientSocketStorage>(),
        );
        read_inheritable_socket(
            &raw mut (*(MyClientSocket as *mut ClientSocketStorage)).sock,
            &raw const (*param).inh_sock,
        );
    }

    SetDataDir((*param).DataDir.as_ptr());

    MyPMChildSlot = (*param).MyPMChildSlot;

    UsedShmemSegID = (*param).UsedShmemSegID;
    UsedShmemSegAddr = (*param).UsedShmemSegAddr;

    ShmemLock = (*param).ShmemLock;

    /* USE_INJECTION_POINTS */
    ActiveInjectionPoints = (*param).ActiveInjectionPoints;

    NamedLWLockTrancheRequests = (*param).NamedLWLockTrancheRequests;
    NamedLWLockTrancheArray = (*param).NamedLWLockTrancheArray;
    MainLWLockArray = (*param).MainLWLockArray;
    ProcStructLock = (*param).ProcStructLock;
    ProcGlobal = (*param).ProcGlobal;
    AuxiliaryProcs = (*param).AuxiliaryProcs;
    PreparedXactProcs = (*param).PreparedXactProcs;
    PMSignalState = (*param).PMSignalState;
    ProcSignal = (*param).ProcSignal;

    PostmasterPid = (*param).PostmasterPid;
    PgStartTime = (*param).PgStartTime;
    PgReloadTime = (*param).PgReloadTime;
    first_syslogger_file_time = (*param).first_syslogger_file_time;

    redirection_done = (*param).redirection_done;
    IsBinaryUpgrade = (*param).IsBinaryUpgrade;
    query_id_enabled = (*param).query_id_enabled;
    max_safe_fds = (*param).max_safe_fds;

    MaxBackends = (*param).MaxBackends;
    num_pmchild_slots = (*param).num_pmchild_slots;

    /* non-WIN32 */
    memcpy(
        postmaster_alive_fds.as_mut_ptr() as *mut c_void,
        (*param).postmaster_alive_fds.as_ptr() as *const c_void,
        size_of_val(&postmaster_alive_fds),
    );

    memcpy(
        syslogPipe.as_mut_ptr() as *mut c_void,
        (*param).syslogPipe.as_ptr() as *const c_void,
        size_of_val(&syslogPipe),
    );

    strlcpy(my_exec_path.as_mut_ptr(), (*param).my_exec_path.as_ptr(), MAXPGPATH);

    strlcpy(pkglib_path.as_mut_ptr(), (*param).pkglib_path.as_ptr(), MAXPGPATH);

    /*
     * We need to restore fd.c's counts of externally-opened FDs; to avoid
     * confusion, be sure to do this after restoring max_safe_fds.  (Note:
     * BackendInitialize will handle this for (*client_sock)->sock.)
     */
    /* non-WIN32 */
    if postmaster_alive_fds[0] >= 0 {
        ReserveExternalFD();
    }
    if postmaster_alive_fds[1] >= 0 {
        ReserveExternalFD();
    }
}

// ----------------------------------------------------------------------------
// EXEC_BACKEND helper externs / stubs (homes in as-yet-unported modules).
// ----------------------------------------------------------------------------

#[allow(non_camel_case_types)]
pub type slock_t = c_int;
#[allow(non_camel_case_types)]
pub type pg_time_t = i64;
#[allow(non_camel_case_types)]
pub type pgsocket = c_int;

/* Opaque types whose concrete definitions live in not-yet-ported headers. */
pub enum InjectionPointsCtl {}
pub enum NamedLWLockTranche {}
pub enum LWLockPadded {}
pub enum PROC_HDR {}
pub enum PGPROC {}
pub enum PMSignalData {}
pub enum ProcSignalHeader {}

#[allow(non_upper_case_globals)]
pub const PGINVALID_SOCKET: pgsocket = -1;

/* stdio FILE, opaque. */
pub enum FILE {}

/* Output destination sentinel (tcop/dest.h). */
#[allow(non_upper_case_globals)]
pub const DestNone: c_int = 0;

/* PG_TEMP_FILES_DIR / PG_TEMP_FILE_PREFIX (storage/fd.h). */
pub const PG_TEMP_FILES_DIR: &[u8] = b"base/pgsql_tmp\0";
pub const PG_TEMP_FILE_PREFIX: &[u8] = b"pgsql_tmp\0";
pub const PG_BINARY_R: &str = "r";

extern "C" {
    /* Globals owned by other modules (EXEC_BACKEND inheritance set). */
    pub static mut whereToSendOutput: c_int;
    pub static mut DataDir: *mut c_char;
    pub static mut UsedShmemSegID: c_ulong;
    pub static mut UsedShmemSegAddr: *mut c_void;
    pub static mut ShmemLock: *mut slock_t;
    pub static mut ActiveInjectionPoints: *mut InjectionPointsCtl;
    pub static mut NamedLWLockTrancheRequests: c_int;
    pub static mut NamedLWLockTrancheArray: *mut NamedLWLockTranche;
    pub static mut MainLWLockArray: *mut LWLockPadded;
    pub static mut ProcStructLock: *mut slock_t;
    pub static mut ProcGlobal: *mut PROC_HDR;
    pub static mut AuxiliaryProcs: *mut PGPROC;
    pub static mut PreparedXactProcs: *mut PGPROC;
    pub static mut PMSignalState: *mut PMSignalData;
    pub static mut ProcSignal: *mut ProcSignalHeader;
    pub static mut PostmasterPid: pid_t;
    pub static mut PgStartTime: TimestampTz;
    pub static mut PgReloadTime: TimestampTz;
    pub static mut first_syslogger_file_time: pg_time_t;
    pub static mut redirection_done: bool;
    pub static mut IsBinaryUpgrade: bool;
    pub static mut query_id_enabled: bool;
    pub static mut max_safe_fds: c_int;
    pub static mut MaxBackends: c_int;
    pub static mut num_pmchild_slots: c_int;
    pub static mut postmaster_alive_fds: [c_int; 2];
    pub static mut syslogPipe: [c_int; 2];
    pub static mut my_exec_path: [c_char; MAXPGPATH];
    pub static mut pkglib_path: [c_char; MAXPGPATH];
    pub static mut postgres_exec_path: [c_char; MAXPGPATH];
    pub static mut MyProcPid: pid_t;

    /* libc */
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn execv(path: *const c_char, argv: *const *const c_char) -> c_int;
    fn exit(status: c_int) -> !;
    fn fwrite(ptr: *const c_void, size: Size, nmemb: Size, stream: *mut FILE) -> Size;
    fn fread(ptr: *mut c_void, size: Size, nmemb: Size, stream: *mut FILE) -> Size;
}

unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut FILE {
    null_mut() /* TODO(pg-port): storage/file/fd.c */
}
unsafe fn FreeFile(_file: *mut FILE) -> c_int {
    0 /* TODO(pg-port): storage/file/fd.c */
}
unsafe fn MakePGDirectory(_path: *const c_char) -> c_int {
    0 /* TODO(pg-port): common/file_perm.c */
}
unsafe fn ReserveExternalFD() {
    /* TODO(pg-port): storage/file/fd.c */
}
unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    0 /* TODO(pg-port): port/strlcpy.c */
}
unsafe fn write_stderr(_fmt: *const c_char, _arg: *const c_char) {
    /* TODO(pg-port): utils/error/elog.c */
}
unsafe fn InitializeGUCOptions() {
    /* TODO(pg-port): utils/misc/guc.c */
}
unsafe fn read_nondefault_variables() {
    /* TODO(pg-port): utils/misc/guc.c */
}
unsafe fn PGSharedMemoryReAttach() {
    /* TODO(pg-port): port/sysv_shmem.c */
}
unsafe fn PGSharedMemoryNoReAttach() {
    /* TODO(pg-port): port/sysv_shmem.c */
}
unsafe fn checkDataDir() {
    /* TODO(pg-port): utils/init/miscinit.c */
}
unsafe fn LocalProcessControlFile(_reset: bool) {
    /* TODO(pg-port): access/transam/xlog.c */
}
unsafe fn process_shared_preload_libraries() {
    /* TODO(pg-port): utils/init/miscinit.c */
}
unsafe fn InitShmemAccess(_seghdr: *mut c_void) {
    /* TODO(pg-port): storage/ipc/shmem.c */
}
unsafe fn SetDataDir(_dir: *const c_char) {
    /* TODO(pg-port): utils/init/miscinit.c */
}

/*
 * #ifdef WIN32 helpers. write_inheritable_socket / read_inheritable_socket and
 * the WIN32 internal_forkexec/read_backend_variables branches are omitted: this
 * build targets the non-WIN32 EXEC_BACKEND path and they depend on Win32-only
 * APIs (CreateProcess, WSADuplicateSocket) not available here.
 */

/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
#[cfg(windows)]
unsafe fn write_duplicated_handle(
    dest: *mut crate::port::win32_port::HANDLE,
    src: crate::port::win32_port::HANDLE,
    childProcess: crate::port::win32_port::HANDLE,
) -> bool {
    use crate::port::win32_port::{DWORD, HANDLE, INVALID_HANDLE_VALUE};

    extern "C" {
        fn GetCurrentProcess() -> HANDLE;
        fn GetLastError() -> DWORD;
        fn DuplicateHandle(
            hSourceProcessHandle: HANDLE,
            hSourceHandle: HANDLE,
            hTargetProcessHandle: HANDLE,
            lpTargetHandle: *mut HANDLE,
            dwDesiredAccess: DWORD,
            bInheritHandle: c_int,
            dwOptions: DWORD,
        ) -> c_int;
    }
    const DUPLICATE_CLOSE_SOURCE: DWORD = 0x0000_0001;
    const DUPLICATE_SAME_ACCESS: DWORD = 0x0000_0002;
    const TRUE: c_int = 1;

    let mut hChild: HANDLE = INVALID_HANDLE_VALUE;

    if DuplicateHandle(
        GetCurrentProcess(),
        src,
        childProcess,
        &raw mut hChild,
        0,
        TRUE,
        DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS,
    ) == 0
    {
        ereport!(
            LOG,
            errmsg_internal!(
                "could not duplicate handle to be written to backend parameter file: error code {}",
                GetLastError()
            )
        );
        return false;
    }

    *dest = hChild;
    true
}

// #endif /* EXEC_BACKEND */

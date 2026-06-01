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

/* libpq/libpq-be.h */
pub type ClientSocket = c_void;

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

unsafe extern "C" fn BackendMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: tcop/backend_startup.c
}
unsafe extern "C" fn AutoVacLauncherMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/autovacuum.c
}
unsafe extern "C" fn AutoVacWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/autovacuum.c
}
unsafe extern "C" fn BackgroundWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/bgworker.c
}
unsafe extern "C" fn ReplSlotSyncWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: replication/logical/slotsync.c
}
unsafe extern "C" fn PgArchiverMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/pgarch.c
}
unsafe extern "C" fn BackgroundWriterMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/bgwriter.c
}
unsafe extern "C" fn CheckpointerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/checkpointer.c
}
unsafe extern "C" fn IoWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: storage/aio/method_worker.c
}
unsafe extern "C" fn StartupProcessMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/startup.c
}
unsafe extern "C" fn WalReceiverMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: replication/walreceiver.c
}
unsafe extern "C" fn WalSummarizerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/walsummarizer.c
}
unsafe extern "C" fn WalWriterMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/walwriter.c
}
unsafe extern "C" fn SysLoggerMain(_startup_data: *const c_void, _startup_data_len: Size) {
    unimplemented!() // TODO: postmaster/syslogger.c
}

// ----------------------------------------------------------------------------
// Helper functions owned by other modules (local stubs).
// ----------------------------------------------------------------------------

unsafe fn GetCurrentTimestamp() -> TimestampTz {
    unimplemented!() // TODO: utils/adt/timestamp.c
}
unsafe fn fork_process() -> pid_t {
    unimplemented!() // TODO: postmaster/fork_process.c
}
unsafe fn ClosePostmasterPorts(_am_syslogger: bool) {
    unimplemented!() // TODO: postmaster/postmaster.c
}
unsafe fn InitPostmasterChild() {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn dsm_detach_all() {
    unimplemented!() // TODO: storage/ipc/dsm.c
}
unsafe fn PGSharedMemoryDetach() {
    unimplemented!() // TODO: port/sysv_shmem.c
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

/*
 * NOTE: The remainder of launch_backend.c is guarded by #ifdef EXEC_BACKEND.
 * EXEC_BACKEND is not enabled in this build (Unix fork() path), so the
 * fork+exec machinery -- internal_forkexec(), SubPostmasterMain(),
 * save_backend_variables(), read_backend_variables(),
 * restore_backend_variables(), the BackendParameters struct, and the Windows
 * handle/socket duplication helpers -- is omitted, matching the C
 * preprocessor exclusion.
 */

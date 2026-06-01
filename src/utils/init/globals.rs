//! globals.c - global variable declarations.
//!
//! Globals used all over the place are defined here and declared `extern`
//! elsewhere (e.g. miscadmin.rs, libpq_be.rs). The `#[no_mangle]` attribute
//! keeps the linker symbol names identical to the C originals so those
//! `extern "C"` declarations resolve to these definitions.

use crate::prelude::*;

// pg_time_t / TimestampTz / ProtocolVersion / ProcNumber from their home
// modules; Oid + InvalidOid come in via the prelude.
use crate::common::file_perm::PG_DIR_MODE_OWNER;
use crate::miscadmin::{DATEORDER_MDY, INTSTYLE_POSTGRES, USE_ISO_DATES};
use crate::pg_config_manual::MAXPGPATH;
use crate::pgtime::pg_time_t;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

// storage/procsignal.h
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;

// datatype/timestamp.h
pub type TimestampTz = int64;
// libpq/pqcomm.h
pub type ProtocolVersion = uint32;

// Forward-declared structs in C (referenced only by pointer): struct Latch,
// struct ClientSocket, struct Port. Use opaque c_void here.
pub type Latch = c_void;
pub type ClientSocket = c_void;
pub type Port = c_void;

// pid_t (sys/types.h) - on the platforms we target this is a 32-bit int.
pub type pid_t = c_int;

// C's `volatile sig_atomic_t` is `volatile int`. We model it as a plain
// c_int; the volatile/signal-safety aspect is a memory-model concern handled
// elsewhere in the port.
pub type sig_atomic_t = c_int;

#[no_mangle]
pub static mut FrontendProtocol: ProtocolVersion = 0;

#[no_mangle]
pub static mut InterruptPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut QueryCancelPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut ProcDiePending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut CheckClientConnectionPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut ClientConnectionLost: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut IdleInTransactionSessionTimeoutPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut TransactionTimeoutPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut IdleSessionTimeoutPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut ProcSignalBarrierPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut LogMemoryContextPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut IdleStatsUpdateTimeoutPending: sig_atomic_t = false as sig_atomic_t;
#[no_mangle]
pub static mut InterruptHoldoffCount: uint32 = 0;
#[no_mangle]
pub static mut QueryCancelHoldoffCount: uint32 = 0;
#[no_mangle]
pub static mut CritSectionCount: uint32 = 0;

#[no_mangle]
pub static mut MyProcPid: c_int = 0;
#[no_mangle]
pub static mut MyStartTime: pg_time_t = 0;
#[no_mangle]
pub static mut MyStartTimestamp: TimestampTz = 0;
#[no_mangle]
pub static mut MyClientSocket: *mut ClientSocket = null_mut();
#[no_mangle]
pub static mut MyProcPort: *mut Port = null_mut();
#[no_mangle]
pub static mut MyCancelKey: [uint8; MAX_CANCEL_KEY_LENGTH] = [0; MAX_CANCEL_KEY_LENGTH];
#[no_mangle]
pub static mut MyCancelKeyLength: c_int = 0;
#[no_mangle]
pub static mut MyPMChildSlot: c_int = 0;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
#[no_mangle]
pub static mut MyLatch: *mut Latch = null_mut();

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
#[no_mangle]
pub static mut DataDir: *mut c_char = null_mut();

/*
 * Mode of the data directory.  The default is 0700 but it may be changed in
 * checkDataDir() to 0750 if the data directory actually has that mode.
 */
#[no_mangle]
pub static mut data_directory_mode: c_int = PG_DIR_MODE_OWNER;

#[no_mangle]
pub static mut OutputFileName: [c_char; MAXPGPATH] = [0; MAXPGPATH]; /* debugging output file */

#[no_mangle]
pub static mut my_exec_path: [c_char; MAXPGPATH] = [0; MAXPGPATH]; /* full path to my executable */
#[no_mangle]
pub static mut pkglib_path: [c_char; MAXPGPATH] = [0; MAXPGPATH]; /* full path to lib directory */

// #ifdef EXEC_BACKEND
#[no_mangle]
pub static mut postgres_exec_path: [c_char; MAXPGPATH] = [0; MAXPGPATH]; /* full path to backend */
/* note: currently this is not valid in backend processes */

#[no_mangle]
pub static mut MyProcNumber: ProcNumber = INVALID_PROC_NUMBER;

#[no_mangle]
pub static mut ParallelLeaderProcNumber: ProcNumber = INVALID_PROC_NUMBER;

#[no_mangle]
pub static mut MyDatabaseId: Oid = InvalidOid;

#[no_mangle]
pub static mut MyDatabaseTableSpace: Oid = InvalidOid;

#[no_mangle]
pub static mut MyDatabaseHasLoginEventTriggers: bool = false;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
#[no_mangle]
pub static mut DatabasePath: *mut c_char = null_mut();

#[no_mangle]
pub static mut PostmasterPid: pid_t = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
#[no_mangle]
pub static mut IsPostmasterEnvironment: bool = false;
#[no_mangle]
pub static mut IsUnderPostmaster: bool = false;
#[no_mangle]
pub static mut IsBinaryUpgrade: bool = false;

#[no_mangle]
pub static mut ExitOnAnyError: bool = false;

#[no_mangle]
pub static mut DateStyle: c_int = USE_ISO_DATES;
#[no_mangle]
pub static mut DateOrder: c_int = DATEORDER_MDY;
#[no_mangle]
pub static mut IntervalStyle: c_int = INTSTYLE_POSTGRES;

#[no_mangle]
pub static mut enableFsync: bool = true;
#[no_mangle]
pub static mut allowSystemTableMods: bool = false;
#[no_mangle]
pub static mut work_mem: c_int = 4096;
#[no_mangle]
pub static mut hash_mem_multiplier: f64 = 2.0;
#[no_mangle]
pub static mut maintenance_work_mem: c_int = 65536;
#[no_mangle]
pub static mut max_parallel_maintenance_workers: c_int = 2;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
#[no_mangle]
pub static mut NBuffers: c_int = 16384;
#[no_mangle]
pub static mut MaxConnections: c_int = 100;
#[no_mangle]
pub static mut max_worker_processes: c_int = 8;
#[no_mangle]
pub static mut max_parallel_workers: c_int = 8;
#[no_mangle]
pub static mut MaxBackends: c_int = 0;

/* GUC parameters for vacuum */
#[no_mangle]
pub static mut VacuumBufferUsageLimit: c_int = 2048;

#[no_mangle]
pub static mut VacuumCostPageHit: c_int = 1;
#[no_mangle]
pub static mut VacuumCostPageMiss: c_int = 2;
#[no_mangle]
pub static mut VacuumCostPageDirty: c_int = 20;
#[no_mangle]
pub static mut VacuumCostLimit: c_int = 200;
#[no_mangle]
pub static mut VacuumCostDelay: f64 = 0.0;

#[no_mangle]
pub static mut VacuumCostBalance: c_int = 0; /* working state for vacuum */
#[no_mangle]
pub static mut VacuumCostActive: bool = false;

/* configurable SLRU buffer sizes */
#[no_mangle]
pub static mut commit_timestamp_buffers: c_int = 0;
#[no_mangle]
pub static mut multixact_member_buffers: c_int = 32;
#[no_mangle]
pub static mut multixact_offset_buffers: c_int = 16;
#[no_mangle]
pub static mut notify_buffers: c_int = 16;
#[no_mangle]
pub static mut serializable_buffers: c_int = 32;
#[no_mangle]
pub static mut subtransaction_buffers: c_int = 0;
#[no_mangle]
pub static mut transaction_buffers: c_int = 0;

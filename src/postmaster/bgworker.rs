/*--------------------------------------------------------------------
 * bgworker.rs
 *   POSTGRES pluggable background workers implementation
 *
 * Merges:
 *   postgres/src/backend/postmaster/bgworker.c
 *   postgres/src/include/postmaster/bgworker.h
 *   postgres/src/include/postmaster/bgworker_internals.h
 *
 * A background worker is a process able to run arbitrary, user-supplied code,
 * including normal transactions.
 *
 * Any external module loaded via shared_preload_libraries can register a
 * worker.  Workers can also be registered dynamically at runtime.  In either
 * case, the worker process is forked from the postmaster and runs the
 * user-supplied "main" function.  This code may connect to a database and
 * run transactions.  Workers can remain active indefinitely, but will be
 * terminated if a shutdown or crash occurs.
 *
 * If the fork() call fails in the postmaster, it will try again later.  Note
 * that the failure can only be transient (fork failure due to high load,
 * memory pressure, too many processes, etc); more permanent problems, like
 * failure to connect to a database, are detected later in the worker and dealt
 * with just by having the worker exit normally. A worker which exits with
 * a return code of 0 will never be restarted and will be removed from worker
 * list. A worker which exits with a return code of 1 will be restarted after
 * the configured restart interval (unless that interval is BGW_NEVER_RESTART).
 * The TerminateBackgroundWorker() function can be used to terminate a
 * dynamically registered background worker; the worker will be sent a SIGTERM
 * and will not be restarted after it exits.  Whenever the postmaster knows
 * that a worker will not be restarted, it unregisters the worker, freeing up
 * that worker's slot for use by a new worker.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/postmaster/bgworker.c
 *--------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify};

use crate::lib::ilist::{dlist_head, dlist_iter, dlist_mutable_iter, dlist_node};
use crate::miscadmin::{
    B_BG_WORKER, CHECK_FOR_INTERRUPTS, HOLD_INTERRUPTS, INIT_PG_OVERRIDE_ALLOW_CONNS,
    INIT_PG_OVERRIDE_ROLE_LOGIN, InitPostgres, InitProcessing, InvalidPid, Latch, MyLatch,
    NormalProcessing, SetProcessingMode, TimestampTz, max_parallel_workers, max_worker_processes,
    GetProcessingMode, IsInitProcessingMode, IsPostmasterEnvironment,
    IsUnderPostmaster, MyBackendType, process_shared_preload_libraries_in_progress,
};
use crate::pg_config_manual::MAXPGPATH;
use crate::storage::ipc::pmsignal::{PMSignalReason, SendPostmasterSignal};
use crate::storage::ipc::shmem::{ShmemInitStruct, add_size, mul_size};
use crate::utils::adt::ascii::ascii_safe_strlcpy;
use crate::utils::fmgr::dfmgr::load_external_function;
use crate::utils::misc::ps_status::init_ps_display;
use crate::utils::misc::timeout::InitializeTimeouts;
use crate::utils::palloc::{
    MemoryContextAlloc, MemoryContextAllocExtended, TopMemoryContext,
    MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO,
};
use crate::utils::mmgr::mcxt::PostmasterContext;

use crate::libpq::pqsignal::{
    pqsignal, sigset_t, BlockSig, UnBlockSig, SigHandler,
    SIGALRM, SIGCHLD, SIGFPE, SIGHUP, SIGINT, SIGPIPE, SIGTERM, SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::port::pgsleep::pg_usleep;
use crate::storage::ipc::ipc::proc_exit;
use crate::storage::ipc::procsignal::procsignal_sigusr1_handler;
use crate::tcop::tcopprot::{FloatExceptionHandler, StatementCancelHandler};

// ----------------------------------------------------------------
// bgworker.h - External module API
// ----------------------------------------------------------------

/*
 * Pass this flag to have your worker be able to connect to shared memory.
 * This flag is required.
 */
pub const BGWORKER_SHMEM_ACCESS: c_int = 0x0001;

/*
 * This flag means the bgworker requires a database connection.  The connection
 * is not established automatically; the worker must establish it later.
 * It requires that BGWORKER_SHMEM_ACCESS was passed too.
 */
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: c_int = 0x0002;

/*
 * This class is used internally for parallel queries, to keep track of the
 * number of active parallel workers and make sure we never launch more than
 * max_parallel_workers parallel workers at the same time.  Third party
 * background workers should not use this class.
 */
pub const BGWORKER_CLASS_PARALLEL: c_int = 0x0010;

/*
 * Flags to BackgroundWorkerInitializeConnection et al
 *
 * Allow bypassing datallowconn restrictions and login check when connecting
 * to database
 */
pub const BGWORKER_BYPASS_ALLOWCONN: u32 = 0x0001;
pub const BGWORKER_BYPASS_ROLELOGINCHECK: u32 = 0x0002;

/* The bgworker_main_type function pointer type */
pub type bgworker_main_type = unsafe fn(Datum);

/*
 * Points in time at which a bgworker can request to be started
 */
pub type BgWorkerStartTime = c_int;
pub const BgWorkerStart_PostmasterStart: BgWorkerStartTime = 0;
pub const BgWorkerStart_ConsistentState: BgWorkerStartTime = 1;
pub const BgWorkerStart_RecoveryFinished: BgWorkerStartTime = 2;

pub const BGW_DEFAULT_RESTART_INTERVAL: c_int = 60;
pub const BGW_NEVER_RESTART: c_int = -1;
pub const BGW_MAXLEN: usize = 96;
pub const BGW_EXTRALEN: usize = 128;

#[repr(C)]
pub struct BackgroundWorker {
    pub bgw_name: [c_char; BGW_MAXLEN],
    pub bgw_type: [c_char; BGW_MAXLEN],
    pub bgw_flags: c_int,
    pub bgw_start_time: BgWorkerStartTime,
    pub bgw_restart_time: c_int, /* in seconds, or BGW_NEVER_RESTART */
    pub bgw_library_name: [c_char; MAXPGPATH],
    pub bgw_function_name: [c_char; BGW_MAXLEN],
    pub bgw_main_arg: Datum,
    pub bgw_extra: [c_char; BGW_EXTRALEN],
    pub bgw_notify_pid: pid_t, /* SIGUSR1 this backend on start/stop */
}

#[repr(C)]
pub enum BgwHandleStatus {
    BGWH_STARTED,          /* worker is running */
    BGWH_NOT_YET_STARTED,  /* worker hasn't been started yet */
    BGWH_STOPPED,          /* worker has exited */
    BGWH_POSTMASTER_DIED,  /* postmaster died; worker status unclear */
}
pub use BgwHandleStatus::*;

/* The opaque handle returned to callers of RegisterDynamicBackgroundWorker */
pub struct BackgroundWorkerHandle {
    pub slot: c_int,
    pub generation: u64,
}

/* This is valid in a running worker */
pub static mut MyBgworkerEntry: *mut BackgroundWorker = null_mut();

// ----------------------------------------------------------------
// bgworker_internals.h - Internals
// ----------------------------------------------------------------

/* pid_t: same as miscadmin */
pub type pid_t = c_int;

/*
 * Maximum possible value of parallel workers.
 */
pub const MAX_PARALLEL_WORKER_LIMIT: c_int = 1024;

/*
 * List of background workers, private to postmaster.
 *
 * All workers that are currently running will also have an entry in
 * ActiveChildList.
 */
#[repr(C)]
pub struct RegisteredBgWorker {
    pub rw_worker: BackgroundWorker, /* its registry entry */
    pub rw_pid: pid_t,               /* 0 if not running */
    pub rw_crashed_at: TimestampTz,  /* if not 0, time it last crashed */
    pub rw_shmem_slot: c_int,
    pub rw_terminate: bool,
    pub rw_lnode: dlist_node, /* list link */
}

/*
 * The postmaster's list of registered background workers, in private memory.
 */
pub static mut BackgroundWorkerList: dlist_head = dlist_head {
    head: crate::lib::ilist::dlist_node {
        prev: null_mut(),
        next: null_mut(),
    },
};

// ----------------------------------------------------------------
// bgworker.c - Implementation
// ----------------------------------------------------------------

/*
 * BackgroundWorkerSlots exist in shared memory and can be accessed (via
 * the BackgroundWorkerArray) by both the postmaster and by regular backends.
 * However, the postmaster cannot take locks, even spinlocks, because this
 * might allow it to crash or become wedged if shared memory gets corrupted.
 * Such an outcome is intolerable.  Therefore, we need a lockless protocol
 * for coordinating access to this data.
 *
 * The 'in_use' flag is used to hand off responsibility for the slot between
 * the postmaster and the rest of the system.  When 'in_use' is false,
 * the postmaster will ignore the slot entirely, except for the 'in_use' flag
 * itself, which it may read.  In this state, regular backends may modify the
 * slot.  Once a backend sets 'in_use' to true, the slot becomes the
 * responsibility of the postmaster.  Regular backends may no longer modify it,
 * but the postmaster may examine it.  Thus, a backend initializing a slot
 * must fully initialize the slot - and insert a write memory barrier - before
 * marking it as in use.
 *
 * As an exception, however, even when the slot is in use, regular backends
 * may set the 'terminate' flag for a slot, telling the postmaster not
 * to restart it.  Once the background worker is no longer running, the slot
 * will be released for reuse.
 *
 * In addition to coordinating with the postmaster, backends modifying this
 * data structure must coordinate with each other.  Since they can take locks,
 * this is straightforward: any backend wishing to manipulate a slot must
 * take BackgroundWorkerLock in exclusive mode.  Backends wishing to read
 * data that might get concurrently modified by other backends should take
 * this lock in shared mode.  No matter what, backends reading this data
 * structure must be able to tolerate concurrent modifications by the
 * postmaster.
 */
#[repr(C)]
struct BackgroundWorkerSlot {
    in_use: bool,
    terminate: bool,
    pid: pid_t,       /* InvalidPid = not started yet; 0 = dead */
    generation: u64,  /* incremented when slot is recycled */
    worker: BackgroundWorker,
}

/*
 * In order to limit the total number of parallel workers (according to
 * max_parallel_workers GUC), we maintain the number of active parallel
 * workers.  Since the postmaster cannot take locks, two variables are used for
 * this purpose: the number of registered parallel workers (modified by the
 * backends, protected by BackgroundWorkerLock) and the number of terminated
 * parallel workers (modified only by the postmaster, lockless).  The active
 * number of parallel workers is the number of registered workers minus the
 * terminated ones.  These counters can of course overflow, but it's not
 * important here since the subtraction will still give the right number.
 */
#[repr(C)]
struct BackgroundWorkerArray {
    total_slots: c_int,
    parallel_register_count: u32,
    parallel_terminate_count: u32,
    /* slot[] is a flexible array; we access slots via raw pointer arithmetic */
}

/* Slot accessor: slots immediately follow the fixed-size header in shmem. */
#[inline]
unsafe fn bwa_slot(data: *mut BackgroundWorkerArray, idx: usize) -> *mut BackgroundWorkerSlot {
    let base = data.add(1) as *mut BackgroundWorkerSlot;
    base.add(idx)
}

static mut BackgroundWorkerData: *mut BackgroundWorkerArray = null_mut();

/*
 * List of internal background worker entry points.  We need this for
 * reasons explained in LookupBackgroundWorkerFunction(), below.
 */
struct InternalBGWorkerEntry {
    fn_name: &'static str,
    fn_addr: bgworker_main_type,
}

/* Forward declarations for internal entry points */
// TODO(pg-port): ParallelWorkerMain lives in access/parallel.c
unsafe fn ParallelWorkerMain_stub(main_arg: Datum) {
    unimplemented!("TODO(pg-port): ParallelWorkerMain lives in access/parallel.c")
}

use crate::replication::logicallauncher::ApplyLauncherMain;
use crate::replication::logicalworker::{
    ApplyWorkerMain, ParallelApplyWorkerMain, TablesyncWorkerMain,
};

static INTERNAL_BG_WORKERS: &[InternalBGWorkerEntry] = &[
    InternalBGWorkerEntry {
        fn_name: "ParallelWorkerMain",
        fn_addr: ParallelWorkerMain_stub,
    },
    InternalBGWorkerEntry {
        fn_name: "ApplyLauncherMain",
        fn_addr: ApplyLauncherMain,
    },
    InternalBGWorkerEntry {
        fn_name: "ApplyWorkerMain",
        fn_addr: ApplyWorkerMain,
    },
    InternalBGWorkerEntry {
        fn_name: "ParallelApplyWorkerMain",
        fn_addr: ParallelApplyWorkerMain,
    },
    InternalBGWorkerEntry {
        fn_name: "TablesyncWorkerMain",
        fn_addr: TablesyncWorkerMain,
    },
];

// ----------------------------------------------------------------
// Stubs for not-yet-ported dependencies
// ----------------------------------------------------------------

// LWLock types and helpers (storage/lwlock.h / lwlock.c).
// TODO(pg-port): real LWLock lives in storage/lwlock.h + lwlock.c
pub type LWLock = c_void;
pub type LWLockMode = c_int;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;

// TODO(pg-port): real LWLockAcquire/LWLockRelease live in storage/lwlock.c
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: LWLockMode) -> bool {
    true
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {}

// BackgroundWorkerLock: built-in LWLock (storage/lwlocklist.h id=33).
// TODO(pg-port): GetMainLWLockArray()[BackgroundWorker_LWLOCK_ID] once
//   storage/lwlock.c is ported.
unsafe fn BackgroundWorkerLock() -> *mut LWLock {
    null_mut()
}

// PMSIGNAL_BACKGROUND_WORKER_CHANGE = 6 (storage/pmsignal.h).
const PMSIGNAL_BACKGROUND_WORKER_CHANGE: PMSignalReason = 6;

// WL_LATCH_SET / WL_POSTMASTER_DEATH (storage/latch.h).
// TODO(pg-port): real values live in storage/latch.h
const WL_LATCH_SET: c_int = 1 << 0;
const WL_POSTMASTER_DEATH: c_int = 1 << 4;

// Wait event ids (wait_event.h).
// TODO(pg-port): real WAIT_EVENT_BGWORKER_STARTUP/SHUTDOWN live in wait_event.h
const WAIT_EVENT_BGWORKER_STARTUP: u32 = 0;
const WAIT_EVENT_BGWORKER_SHUTDOWN: u32 = 0;

// WaitLatch / ResetLatch (storage/latch.h / latch.c).
// TODO(pg-port): real WaitLatch/ResetLatch live in storage/latch.c
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout: c_long,
    _wait_event_info: u32,
) -> c_int {
    0
}
unsafe fn ResetLatch(_latch: *mut Latch) {}

// PostmasterMarkPIDForWorkerNotify (postmaster/postmaster.c).
// TODO(pg-port): real PostmasterMarkPIDForWorkerNotify lives in postmaster/postmaster.c
unsafe fn PostmasterMarkPIDForWorkerNotify(_pid: pid_t) -> bool {
    true
}

// SIG_IGN function pointer (signal.h value = 1).
#[inline]
fn SIG_IGN() -> SigHandler {
    Some(unsafe { core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize) })
}

// sigprocmask(2).
// TODO(pg-port): route through a ported port-layer wrapper
extern "C" {
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
}
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

// kill(2).
extern "C" {
    fn kill(pid: c_int, sig: c_int) -> c_int;
}

// procsignal_sigusr1_handler is imported from crate::storage::ipc::procsignal.

// EmitErrorReport (utils/error/elog.c).
// TODO(pg-port): real EmitErrorReport lives in utils/error/elog.c
unsafe fn EmitErrorReport() {
    /* TODO(pg-port): real EmitErrorReport lives in utils/error/elog.c */
}

// error_context_stack / PG_exception_stack (utils/error/elog.c).
// TODO(pg-port): real error_context_stack lives in utils/error/elog.c
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();

// sigjmp_buf / sigsetjmp stubs (setjmp.h).
// TODO(pg-port): wire to real sigsetjmp once elog.c is ported.
type sigjmp_buf = [c_void; 0];
unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    0
}

// InitProcess (storage/ipc/proc.c).
// TODO(pg-port): real InitProcess lives in storage/ipc/proc.c
unsafe fn InitProcess() {
    /* TODO(pg-port): real InitProcess lives in storage/ipc/proc.c */
}

// BaseInit (tcop/postgres.c or similar).
// TODO(pg-port): real BaseInit lives in tcop/postgres.c
unsafe fn BaseInit() {
    /* TODO(pg-port): real BaseInit lives in tcop/postgres.c */
}

// pg_read_barrier / pg_write_barrier / pg_memory_barrier (port/atomics.h).
// TODO(pg-port): real barriers live in port/atomics.h
#[inline]
unsafe fn pg_read_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire);
}
#[inline]
unsafe fn pg_write_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
}
#[inline]
unsafe fn pg_memory_barrier() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);
}

// strcmp (C string compare).
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// USECS_PER_DAY (datatype/timestamp.h).
const USECS_PER_DAY: int64 = 86400000000;

// ----------------------------------------------------------------
// BackgroundWorkerShmemSize
// ----------------------------------------------------------------

/*
 * Calculate shared memory needed.
 */
pub unsafe fn BackgroundWorkerShmemSize() -> Size {
    let mut size: Size;

    /* Array of workers is variably sized.
     * C uses offsetof(BackgroundWorkerArray, slot) for the header size;
     * here we use size_of::<BackgroundWorkerArray>() which is equivalent
     * since the slot[] flexible array contributes 0 bytes. */
    size = core::mem::size_of::<BackgroundWorkerArray>();
    size = add_size(
        size,
        mul_size(
            max_worker_processes as Size,
            core::mem::size_of::<BackgroundWorkerSlot>(),
        ),
    );

    size
}

// ----------------------------------------------------------------
// BackgroundWorkerShmemInit
// ----------------------------------------------------------------

/*
 * Initialize shared memory.
 */
pub unsafe fn BackgroundWorkerShmemInit() {
    let mut found: bool = false;

    BackgroundWorkerData = ShmemInitStruct(
        c"Background Worker Data".as_ptr(),
        BackgroundWorkerShmemSize(),
        &mut found,
    ) as *mut BackgroundWorkerArray;

    if !IsUnderPostmaster {
        let mut iter: dlist_iter = dlist_iter {
            cur: null_mut(),
            end: null_mut(),
        };
        let mut slotno: c_int = 0;

        (*BackgroundWorkerData).total_slots = max_worker_processes;
        (*BackgroundWorkerData).parallel_register_count = 0;
        (*BackgroundWorkerData).parallel_terminate_count = 0;

        /*
         * Copy contents of worker list into shared memory.  Record the shared
         * memory slot assigned to each worker.  This ensures a 1-to-1
         * correspondence between the postmaster's private list and the array
         * in shared memory.
         */
        dlist_foreach!(iter, &mut BackgroundWorkerList, {
            let slot: *mut BackgroundWorkerSlot =
                bwa_slot(BackgroundWorkerData, slotno as usize);
            let rw: *mut RegisteredBgWorker;

            rw = dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);
            Assert!(slotno < max_worker_processes);
            (*slot).in_use = true;
            (*slot).terminate = false;
            (*slot).pid = InvalidPid;
            (*slot).generation = 0;
            (*rw).rw_shmem_slot = slotno;
            (*rw).rw_worker.bgw_notify_pid = 0; /* might be reinit after crash */
            memcpy(
                &raw mut (*slot).worker as *mut c_void,
                &raw const (*rw).rw_worker as *const c_void,
                core::mem::size_of::<BackgroundWorker>(),
            );
            slotno += 1;
        });

        /*
         * Mark any remaining slots as not in use.
         */
        while slotno < max_worker_processes {
            let slot: *mut BackgroundWorkerSlot =
                bwa_slot(BackgroundWorkerData, slotno as usize);
            (*slot).in_use = false;
            slotno += 1;
        }
    } else {
        Assert!(found);
    }
}

// ----------------------------------------------------------------
// FindRegisteredWorkerBySlotNumber
// ----------------------------------------------------------------

/*
 * Search the postmaster's backend-private list of RegisteredBgWorker objects
 * for the one that maps to the given slot number.
 */
unsafe fn FindRegisteredWorkerBySlotNumber(slotno: c_int) -> *mut RegisteredBgWorker {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &mut BackgroundWorkerList, {
        let rw: *mut RegisteredBgWorker;
        rw = dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);
        if (*rw).rw_shmem_slot == slotno {
            return rw;
        }
    });

    null_mut()
}

// ----------------------------------------------------------------
// BackgroundWorkerStateChange
// ----------------------------------------------------------------

/*
 * Notice changes to shared memory made by other backends.
 * Accept new worker requests only if allow_new_workers is true.
 *
 * This code runs in the postmaster, so we must be very careful not to assume
 * that shared memory contents are sane.  Otherwise, a rogue backend could
 * take out the postmaster.
 */
pub unsafe fn BackgroundWorkerStateChange(allow_new_workers: bool) {
    let mut slotno: c_int;

    /*
     * The total number of slots stored in shared memory should match our
     * notion of max_worker_processes.  If it does not, something is very
     * wrong.  Further down, we always refer to this value as
     * max_worker_processes, in case shared memory gets corrupted while we're
     * looping.
     */
    if max_worker_processes != (*BackgroundWorkerData).total_slots {
        ereport!(
            LOG,
            errmsg!(
                "inconsistent background worker state (\"max_worker_processes\"={}, total slots={})",
                max_worker_processes,
                (*BackgroundWorkerData).total_slots
            )
        );
        return;
    }

    /*
     * Iterate through slots, looking for newly-registered workers or workers
     * who must die.
     */
    slotno = 0;
    while slotno < max_worker_processes {
        let slot: *mut BackgroundWorkerSlot = bwa_slot(BackgroundWorkerData, slotno as usize);
        let rw: *mut RegisteredBgWorker;

        if !(*slot).in_use {
            slotno += 1;
            continue;
        }

        /*
         * Make sure we don't see the in_use flag before the updated slot
         * contents.
         */
        pg_read_barrier();

        /* See whether we already know about this worker. */
        rw = FindRegisteredWorkerBySlotNumber(slotno);
        if !rw.is_null() {
            /*
             * In general, the worker data can't change after it's initially
             * registered.  However, someone can set the terminate flag.
             */
            if (*slot).terminate && !(*rw).rw_terminate {
                (*rw).rw_terminate = true;
                if (*rw).rw_pid != 0 {
                    kill((*rw).rw_pid, SIGTERM);
                } else {
                    /* Report never-started, now-terminated worker as dead. */
                    ReportBackgroundWorkerPID(rw);
                }
            }
            slotno += 1;
            continue;
        }

        /*
         * If we aren't allowing new workers, then immediately mark it for
         * termination; the next stanza will take care of cleaning it up.
         * Doing this ensures that any process waiting for the worker will get
         * awoken, even though the worker will never be allowed to run.
         */
        if !allow_new_workers {
            (*slot).terminate = true;
        }

        /*
         * If the worker is marked for termination, we don't need to add it to
         * the registered workers list; we can just free the slot. However, if
         * bgw_notify_pid is set, the process that registered the worker may
         * need to know that we've processed the terminate request, so be sure
         * to signal it.
         */
        if (*slot).terminate {
            let notify_pid: pid_t;

            /*
             * We need a memory barrier here to make sure that the load of
             * bgw_notify_pid and the update of parallel_terminate_count
             * complete before the store to in_use.
             */
            notify_pid = (*slot).worker.bgw_notify_pid;
            if ((*slot).worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0 {
                (*BackgroundWorkerData).parallel_terminate_count += 1;
            }
            (*slot).pid = 0;

            pg_memory_barrier();
            (*slot).in_use = false;

            if notify_pid != 0 {
                kill(notify_pid, SIGUSR1);
            }

            slotno += 1;
            continue;
        }

        /*
         * Copy the registration data into the registered workers list.
         */
        let rw_new: *mut RegisteredBgWorker = MemoryContextAllocExtended(
            PostmasterContext as crate::utils::palloc::MemoryContext,
            core::mem::size_of::<RegisteredBgWorker>(),
            MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO,
        ) as *mut RegisteredBgWorker;
        if rw_new.is_null() {
            /* errcode(ERRCODE_OUT_OF_MEMORY) folded into comment */
            ereport!(LOG, errmsg!("out of memory"));
            return;
        }
        let rw = rw_new;

        /*
         * Copy strings in a paranoid way.  If shared memory is corrupted, the
         * source data might not even be NUL-terminated.
         */
        ascii_safe_strlcpy(
            (*rw).rw_worker.bgw_name.as_mut_ptr(),
            (*slot).worker.bgw_name.as_ptr(),
            BGW_MAXLEN,
        );
        ascii_safe_strlcpy(
            (*rw).rw_worker.bgw_type.as_mut_ptr(),
            (*slot).worker.bgw_type.as_ptr(),
            BGW_MAXLEN,
        );
        ascii_safe_strlcpy(
            (*rw).rw_worker.bgw_library_name.as_mut_ptr(),
            (*slot).worker.bgw_library_name.as_ptr(),
            MAXPGPATH,
        );
        ascii_safe_strlcpy(
            (*rw).rw_worker.bgw_function_name.as_mut_ptr(),
            (*slot).worker.bgw_function_name.as_ptr(),
            BGW_MAXLEN,
        );

        /*
         * Copy various fixed-size fields.
         *
         * flags, start_time, and restart_time are examined by the postmaster,
         * but nothing too bad will happen if they are corrupted.  The
         * remaining fields will only be examined by the child process.  It
         * might crash, but we won't.
         */
        (*rw).rw_worker.bgw_flags = (*slot).worker.bgw_flags;
        (*rw).rw_worker.bgw_start_time = (*slot).worker.bgw_start_time;
        (*rw).rw_worker.bgw_restart_time = (*slot).worker.bgw_restart_time;
        (*rw).rw_worker.bgw_main_arg = (*slot).worker.bgw_main_arg;
        memcpy(
            (*rw).rw_worker.bgw_extra.as_mut_ptr() as *mut c_void,
            (*slot).worker.bgw_extra.as_ptr() as *const c_void,
            BGW_EXTRALEN,
        );

        /*
         * Copy the PID to be notified about state changes, but only if the
         * postmaster knows about a backend with that PID.  It isn't an error
         * if the postmaster doesn't know about the PID, because the backend
         * that requested the worker could have died (or been killed) just
         * after doing so.  Nonetheless, at least until we get some experience
         * with how this plays out in the wild, log a message at a relative
         * high debug level.
         */
        (*rw).rw_worker.bgw_notify_pid = (*slot).worker.bgw_notify_pid;
        if !PostmasterMarkPIDForWorkerNotify((*rw).rw_worker.bgw_notify_pid) {
            elog!(
                DEBUG1,
                "worker notification PID {} is not valid",
                (*rw).rw_worker.bgw_notify_pid as c_int
            );
            (*rw).rw_worker.bgw_notify_pid = 0;
        }

        /* Initialize postmaster bookkeeping. */
        (*rw).rw_pid = 0;
        (*rw).rw_crashed_at = 0;
        (*rw).rw_shmem_slot = slotno;
        (*rw).rw_terminate = false;

        /* Log it! */
        ereport!(
            DEBUG1,
            errmsg!(
                "registering background worker \"{}\"",
                core::ffi::CStr::from_ptr((*rw).rw_worker.bgw_name.as_ptr())
                    .to_string_lossy()
            )
        );

        crate::lib::ilist::dlist_push_head(
            &raw mut BackgroundWorkerList,
            &raw mut (*rw).rw_lnode,
        );

        slotno += 1;
    }
}

// ----------------------------------------------------------------
// ForgetBackgroundWorker
// ----------------------------------------------------------------

/*
 * Forget about a background worker that's no longer needed.
 *
 * NOTE: The entry is unlinked from BackgroundWorkerList.  If the caller is
 * iterating through it, better use a mutable iterator!
 *
 * Caller is responsible for notifying bgw_notify_pid, if appropriate.
 *
 * This function must be invoked only in the postmaster.
 */
pub unsafe fn ForgetBackgroundWorker(rw: *mut RegisteredBgWorker) {
    let slot: *mut BackgroundWorkerSlot;

    Assert!((*rw).rw_shmem_slot < max_worker_processes);
    slot = bwa_slot(BackgroundWorkerData, (*rw).rw_shmem_slot as usize);
    Assert!((*slot).in_use);

    /*
     * We need a memory barrier here to make sure that the update of
     * parallel_terminate_count completes before the store to in_use.
     */
    if ((*rw).rw_worker.bgw_flags & BGWORKER_CLASS_PARALLEL) != 0 {
        (*BackgroundWorkerData).parallel_terminate_count += 1;
    }

    pg_memory_barrier();
    (*slot).in_use = false;

    ereport!(
        DEBUG1,
        errmsg!(
            "unregistering background worker \"{}\"",
            core::ffi::CStr::from_ptr((*rw).rw_worker.bgw_name.as_ptr())
                .to_string_lossy()
        )
    );

    crate::lib::ilist::dlist_delete(&raw mut (*rw).rw_lnode);
    crate::utils::palloc::pfree(rw as *mut c_void);
}

// ----------------------------------------------------------------
// ReportBackgroundWorkerPID
// ----------------------------------------------------------------

/*
 * Report the PID of a newly-launched background worker in shared memory.
 *
 * This function should only be called from the postmaster.
 */
pub unsafe fn ReportBackgroundWorkerPID(rw: *mut RegisteredBgWorker) {
    let slot: *mut BackgroundWorkerSlot;

    Assert!((*rw).rw_shmem_slot < max_worker_processes);
    slot = bwa_slot(BackgroundWorkerData, (*rw).rw_shmem_slot as usize);
    (*slot).pid = (*rw).rw_pid;

    if (*rw).rw_worker.bgw_notify_pid != 0 {
        kill((*rw).rw_worker.bgw_notify_pid, SIGUSR1);
    }
}

// ----------------------------------------------------------------
// ReportBackgroundWorkerExit
// ----------------------------------------------------------------

/*
 * Report that the PID of a background worker is now zero because a
 * previously-running background worker has exited.
 *
 * NOTE: The entry may be unlinked from BackgroundWorkerList.  If the caller
 * is iterating through it, better use a mutable iterator!
 *
 * This function should only be called from the postmaster.
 */
pub unsafe fn ReportBackgroundWorkerExit(rw: *mut RegisteredBgWorker) {
    let slot: *mut BackgroundWorkerSlot;
    let notify_pid: pid_t;

    Assert!((*rw).rw_shmem_slot < max_worker_processes);
    slot = bwa_slot(BackgroundWorkerData, (*rw).rw_shmem_slot as usize);
    (*slot).pid = (*rw).rw_pid;
    notify_pid = (*rw).rw_worker.bgw_notify_pid;

    /*
     * If this worker is slated for deregistration, do that before notifying
     * the process which started it.  Otherwise, if that process tries to
     * reuse the slot immediately, it might not be available yet.  In theory
     * that could happen anyway if the process checks slot->pid at just the
     * wrong moment, but this makes the window narrower.
     */
    if (*rw).rw_terminate || (*rw).rw_worker.bgw_restart_time == BGW_NEVER_RESTART {
        ForgetBackgroundWorker(rw);
    }

    if notify_pid != 0 {
        kill(notify_pid, SIGUSR1);
    }
}

// ----------------------------------------------------------------
// BackgroundWorkerStopNotifications
// ----------------------------------------------------------------

/*
 * Cancel SIGUSR1 notifications for a PID belonging to an exiting backend.
 *
 * This function should only be called from the postmaster.
 */
pub unsafe fn BackgroundWorkerStopNotifications(pid: pid_t) {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &mut BackgroundWorkerList, {
        let rw: *mut RegisteredBgWorker;
        rw = dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);
        if (*rw).rw_worker.bgw_notify_pid == pid {
            (*rw).rw_worker.bgw_notify_pid = 0;
        }
    });
}

// ----------------------------------------------------------------
// ForgetUnstartedBackgroundWorkers
// ----------------------------------------------------------------

/*
 * Cancel any not-yet-started worker requests that have waiting processes.
 *
 * This is called during a normal ("smart" or "fast") database shutdown.
 * After this point, no new background workers will be started, so anything
 * that might be waiting for them needs to be kicked off its wait.  We do
 * that by canceling the bgworker registration entirely, which is perhaps
 * overkill, but since we're shutting down it does not matter whether the
 * registration record sticks around.
 *
 * This function should only be called from the postmaster.
 */
pub unsafe fn ForgetUnstartedBackgroundWorkers() {
    let mut iter: dlist_mutable_iter = dlist_mutable_iter {
        cur: null_mut(),
        next: null_mut(),
        end: null_mut(),
    };

    dlist_foreach_modify!(iter, &mut BackgroundWorkerList, {
        let rw: *mut RegisteredBgWorker;
        let slot: *mut BackgroundWorkerSlot;

        rw = dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);
        Assert!((*rw).rw_shmem_slot < max_worker_processes);
        slot = bwa_slot(BackgroundWorkerData, (*rw).rw_shmem_slot as usize);

        /* If it's not yet started, and there's someone waiting ... */
        if (*slot).pid == InvalidPid && (*rw).rw_worker.bgw_notify_pid != 0 {
            /* ... then zap it, and notify the waiter */
            let notify_pid: pid_t = (*rw).rw_worker.bgw_notify_pid;

            ForgetBackgroundWorker(rw);
            if notify_pid != 0 {
                kill(notify_pid, SIGUSR1);
            }
        }
    });
}

// ----------------------------------------------------------------
// ResetBackgroundWorkerCrashTimes
// ----------------------------------------------------------------

/*
 * Reset background worker crash state.
 *
 * We assume that, after a crash-and-restart cycle, background workers without
 * the never-restart flag should be restarted immediately, instead of waiting
 * for bgw_restart_time to elapse.  On the other hand, workers with that flag
 * should be forgotten immediately, since we won't ever restart them.
 *
 * This function should only be called from the postmaster.
 */
pub unsafe fn ResetBackgroundWorkerCrashTimes() {
    let mut iter: dlist_mutable_iter = dlist_mutable_iter {
        cur: null_mut(),
        next: null_mut(),
        end: null_mut(),
    };

    dlist_foreach_modify!(iter, &mut BackgroundWorkerList, {
        let rw: *mut RegisteredBgWorker;

        rw = dlist_container!(RegisteredBgWorker, rw_lnode, iter.cur);

        if (*rw).rw_worker.bgw_restart_time == BGW_NEVER_RESTART {
            /*
             * Workers marked BGW_NEVER_RESTART shouldn't get relaunched after
             * the crash, so forget about them.  (If we wait until after the
             * crash to forget about them, and they are parallel workers,
             * parallel_terminate_count will get incremented after we've
             * already zeroed parallel_register_count, which would be bad.)
             */
            ForgetBackgroundWorker(rw);
        } else {
            /*
             * The accounting which we do via parallel_register_count and
             * parallel_terminate_count would get messed up if a worker marked
             * parallel could survive a crash and restart cycle. All such
             * workers should be marked BGW_NEVER_RESTART, and thus control
             * should never reach this branch.
             */
            Assert!(((*rw).rw_worker.bgw_flags & BGWORKER_CLASS_PARALLEL) == 0);

            /*
             * Allow this worker to be restarted immediately after we finish
             * resetting.
             */
            (*rw).rw_crashed_at = 0;
            (*rw).rw_pid = 0;

            /*
             * If there was anyone waiting for it, they're history.
             */
            (*rw).rw_worker.bgw_notify_pid = 0;
        }
    });
}

// ----------------------------------------------------------------
// SanityCheckBackgroundWorker (private)
// ----------------------------------------------------------------

/*
 * Complain about the BackgroundWorker definition using error level elevel.
 * Return true if it looks ok, false if not (unless elevel >= ERROR, in
 * which case we won't return at all in the not-OK case).
 */
unsafe fn SanityCheckBackgroundWorker(worker: *mut BackgroundWorker, elevel: c_int) -> bool {
    /* sanity check for flags */

    /*
     * We used to support workers not connected to shared memory, but don't
     * anymore. Thus this is a required flag now. We're not removing the flag
     * for compatibility reasons and because the flag still provides some
     * signal when reading code.
     */
    if ((*worker).bgw_flags & BGWORKER_SHMEM_ACCESS) == 0 {
        /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) folded into comment */
        ereport!(
            elevel,
            errmsg!(
                "background worker \"{}\": background workers without shared memory access are not supported",
                core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
            )
        );
        return false;
    }

    if ((*worker).bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) != 0 {
        if (*worker).bgw_start_time == BgWorkerStart_PostmasterStart {
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) folded into comment */
            ereport!(
                elevel,
                errmsg!(
                    "background worker \"{}\": cannot request database access if starting at postmaster start",
                    core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
                )
            );
            return false;
        }

        /* XXX other checks? */
    }

    if ((*worker).bgw_restart_time < 0
        && (*worker).bgw_restart_time != BGW_NEVER_RESTART)
        || ((*worker).bgw_restart_time > (USECS_PER_DAY / 1000) as c_int)
    {
        /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) folded into comment */
        ereport!(
            elevel,
            errmsg!(
                "background worker \"{}\": invalid restart interval",
                core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
            )
        );
        return false;
    }

    /*
     * Parallel workers may not be configured for restart, because the
     * parallel_register_count/parallel_terminate_count accounting can't
     * handle parallel workers lasting through a crash-and-restart cycle.
     */
    if (*worker).bgw_restart_time != BGW_NEVER_RESTART
        && ((*worker).bgw_flags & BGWORKER_CLASS_PARALLEL) != 0
    {
        /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) folded into comment */
        ereport!(
            elevel,
            errmsg!(
                "background worker \"{}\": parallel workers may not be configured for restart",
                core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
            )
        );
        return false;
    }

    /*
     * If bgw_type is not filled in, use bgw_name.
     */
    if *(*worker).bgw_type.as_ptr() == 0 {
        strcpy(
            (*worker).bgw_type.as_mut_ptr(),
            (*worker).bgw_name.as_ptr(),
        );
    }

    true
}

// ----------------------------------------------------------------
// bgworker_die (private signal handler)
// ----------------------------------------------------------------

/*
 * Standard SIGTERM handler for background workers
 */
unsafe extern "C" fn bgworker_die(_postgres_signal_arg: c_int) {
    sigprocmask(SIG_SETMASK, &raw const BlockSig, null_mut());

    /* errcode(ERRCODE_ADMIN_SHUTDOWN) folded into comment */
    ereport!(
        FATAL,
        errmsg!(
            "terminating background worker \"{}\" due to administrator command",
            core::ffi::CStr::from_ptr((*MyBgworkerEntry).bgw_type.as_ptr()).to_string_lossy()
        )
    );
}

// ----------------------------------------------------------------
// BackgroundWorkerMain
// ----------------------------------------------------------------

/*
 * Main entry point for background worker processes.
 */
pub unsafe fn BackgroundWorkerMain(startup_data: *const c_void, startup_data_len: Size) -> ! {
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let worker: *mut BackgroundWorker;
    let entrypt: bgworker_main_type;

    if startup_data.is_null() {
        elog!(FATAL, "unable to find bgworker entry");
    }
    Assert!(startup_data_len == core::mem::size_of::<BackgroundWorker>());
    worker = MemoryContextAlloc(TopMemoryContext, core::mem::size_of::<BackgroundWorker>())
        as *mut BackgroundWorker;
    memcpy(
        worker as *mut c_void,
        startup_data,
        core::mem::size_of::<BackgroundWorker>(),
    );

    /*
     * Now that we're done reading the startup data, release postmaster's
     * working memory context.
     */
    if !PostmasterContext.is_null() {
        crate::utils::memutils::MemoryContextDelete(PostmasterContext as crate::utils::palloc::MemoryContext);
        PostmasterContext = null_mut();
    }

    MyBgworkerEntry = worker;
    MyBackendType = B_BG_WORKER;
    init_ps_display((*worker).bgw_name.as_ptr());

    Assert!(GetProcessingMode() == InitProcessing);

    /* Apply PostAuthDelay */
    // TODO(pg-port): PostAuthDelay lives in tcop/tcopprot.h
    let post_auth_delay: c_int = 0; /* stub: PostAuthDelay not yet imported */
    if post_auth_delay > 0 {
        pg_usleep(post_auth_delay as c_long * 1000000);
    }

    /*
     * Set up signal handlers.
     */
    if ((*worker).bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) != 0 {
        /*
         * SIGINT is used to signal canceling the current action
         */
        pqsignal(SIGINT, Some(StatementCancelHandler));
        pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
        pqsignal(SIGFPE, Some(core::mem::transmute::<
            unsafe extern "C" fn(c_int) -> !,
            unsafe extern "C" fn(c_int),
        >(FloatExceptionHandler)));

        /* XXX Any other handlers needed here? */
    } else {
        pqsignal(SIGINT, SIG_IGN());
        pqsignal(SIGUSR1, SIG_IGN());
        pqsignal(SIGFPE, SIG_IGN());
    }
    pqsignal(SIGTERM, Some(bgworker_die));
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGHUP, SIG_IGN());

    InitializeTimeouts(); /* establishes SIGALRM handler */

    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGUSR2, SIG_IGN());
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * We just need to clean up, report the error, and go away.
     */
    if sigsetjmp(&raw mut local_sigjmp_buf, 1) != 0 {
        /* Since not using PG_TRY, must reset error stack by hand */
        error_context_stack = null_mut();

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * sigsetjmp will have blocked all signals, but we may need to accept
         * signals while communicating with our parallel leader.  Once we've
         * done HOLD_INTERRUPTS() it should be safe to unblock signals.
         */
        BackgroundWorkerUnblockSignals();

        /* Report the error to the parallel leader and the server log */
        EmitErrorReport();

        /*
         * Do we need more cleanup here?  For shmem-connected bgworkers, we
         * will call InitProcess below, which will install ProcKill as exit
         * callback.  That will take care of releasing locks, etc.
         */

        /* and go away */
        proc_exit(1);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = (&raw mut local_sigjmp_buf) as *mut c_void;

    /*
     * Create a per-backend PGPROC struct in shared memory.  We must do this
     * before we can use LWLocks or access any shared memory.
     */
    InitProcess();

    /*
     * Early initialization.
     */
    BaseInit();

    /*
     * Look up the entry point function, loading its library if necessary.
     */
    entrypt = LookupBackgroundWorkerFunction(
        (*worker).bgw_library_name.as_ptr(),
        (*worker).bgw_function_name.as_ptr(),
    );

    /*
     * Note that in normal processes, we would call InitPostgres here.  For a
     * worker, however, we don't know what database to connect to, yet; so we
     * need to wait until the user code does it via
     * BackgroundWorkerInitializeConnection().
     */

    /*
     * Now invoke the user-defined worker code
     */
    entrypt((*worker).bgw_main_arg);

    /* ... and if it returns, we're done */
    proc_exit(0);
}

// ----------------------------------------------------------------
// BackgroundWorkerInitializeConnection
// ----------------------------------------------------------------

/*
 * Connect background worker to a database.
 */
pub unsafe fn BackgroundWorkerInitializeConnection(
    dbname: *const c_char,
    username: *const c_char,
    flags: u32,
) {
    let worker: *mut BackgroundWorker = MyBgworkerEntry;
    let mut init_flags: bits32 = 0; /* never honor session_preload_libraries */

    /* ignore datallowconn and ACL_CONNECT? */
    if (flags & BGWORKER_BYPASS_ALLOWCONN) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ALLOW_CONNS as bits32;
    }
    /* ignore rolcanlogin? */
    if (flags & BGWORKER_BYPASS_ROLELOGINCHECK) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ROLE_LOGIN as bits32;
    }

    /* XXX is this the right errcode? */
    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) folded into comment */
    if ((*worker).bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) == 0 {
        ereport!(
            FATAL,
            errmsg!("database connection requirement not indicated during registration")
        );
    }

    InitPostgres(
        dbname,
        InvalidOid,  /* database to connect to */
        username,
        InvalidOid,  /* role to connect as */
        init_flags as u32,
        null_mut(),  /* no out_dbname */
    );

    /* it had better not gotten out of "init" mode yet */
    if !IsInitProcessingMode() {
        ereport!(ERROR, errmsg!("invalid processing mode in background worker"));
    }
    SetProcessingMode(NormalProcessing);
}

// ----------------------------------------------------------------
// BackgroundWorkerInitializeConnectionByOid
// ----------------------------------------------------------------

/*
 * Connect background worker to a database using OIDs.
 */
pub unsafe fn BackgroundWorkerInitializeConnectionByOid(dboid: Oid, useroid: Oid, flags: u32) {
    let worker: *mut BackgroundWorker = MyBgworkerEntry;
    let mut init_flags: bits32 = 0; /* never honor session_preload_libraries */

    /* ignore datallowconn and ACL_CONNECT? */
    if (flags & BGWORKER_BYPASS_ALLOWCONN) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ALLOW_CONNS as bits32;
    }
    /* ignore rolcanlogin? */
    if (flags & BGWORKER_BYPASS_ROLELOGINCHECK) != 0 {
        init_flags |= INIT_PG_OVERRIDE_ROLE_LOGIN as bits32;
    }

    /* XXX is this the right errcode? */
    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) folded into comment */
    if ((*worker).bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) == 0 {
        ereport!(
            FATAL,
            errmsg!("database connection requirement not indicated during registration")
        );
    }

    InitPostgres(
        null(),
        dboid,      /* database to connect to */
        null(),
        useroid,    /* role to connect as */
        init_flags as u32,
        null_mut(), /* no out_dbname */
    );

    /* it had better not gotten out of "init" mode yet */
    if !IsInitProcessingMode() {
        ereport!(ERROR, errmsg!("invalid processing mode in background worker"));
    }
    SetProcessingMode(NormalProcessing);
}

// ----------------------------------------------------------------
// BackgroundWorkerBlockSignals / BackgroundWorkerUnblockSignals
// ----------------------------------------------------------------

/*
 * Block/unblock signals in a background worker
 */
pub unsafe fn BackgroundWorkerBlockSignals() {
    sigprocmask(SIG_SETMASK, &raw const BlockSig, null_mut());
}

pub unsafe fn BackgroundWorkerUnblockSignals() {
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, null_mut());
}

// ----------------------------------------------------------------
// RegisterBackgroundWorker
// ----------------------------------------------------------------

/*
 * Register a new static background worker.
 *
 * This can only be called directly from postmaster or in the _PG_init
 * function of a module library that's loaded by shared_preload_libraries;
 * otherwise it will have no effect.
 */
pub unsafe fn RegisterBackgroundWorker(worker: *mut BackgroundWorker) {
    let rw: *mut RegisteredBgWorker;
    static mut numworkers: c_int = 0;

    /*
     * Static background workers can only be registered in the postmaster
     * process.
     */
    if IsUnderPostmaster || !IsPostmasterEnvironment {
        /*
         * In single-user mode (non-EXEC_BACKEND), we process
         * shared_preload_libraries in backend processes too.  We cannot
         * register static background workers at that stage, but many
         * libraries' _PG_init() functions don't distinguish whether they're
         * being loaded in the postmaster or in a backend, they just check
         * process_shared_preload_libraries_in_progress.  It's a bit sloppy,
         * but for historical reasons we tolerate it.
         *
         * Note: EXEC_BACKEND code path is omitted (this port models
         * non-EXEC_BACKEND).
         */
        if process_shared_preload_libraries_in_progress {
            return;
        }
        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) folded into comment */
        ereport!(
            LOG,
            errmsg!(
                "background worker \"{}\": must be registered in \"shared_preload_libraries\"",
                core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
            )
        );
        return;
    }

    /*
     * Cannot register static background workers after calling
     * BackgroundWorkerShmemInit().
     */
    if !BackgroundWorkerData.is_null() {
        elog!(
            ERROR,
            "cannot register background worker \"{}\" after shmem init",
            core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
        );
    }

    ereport!(
        DEBUG1,
        errmsg!(
            "registering background worker \"{}\"",
            core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
        )
    );

    if !SanityCheckBackgroundWorker(worker, LOG) {
        return;
    }

    if (*worker).bgw_notify_pid != 0 {
        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) folded into comment */
        ereport!(
            LOG,
            errmsg!(
                "background worker \"{}\": only dynamic background workers can request notification",
                core::ffi::CStr::from_ptr((*worker).bgw_name.as_ptr()).to_string_lossy()
            )
        );
        return;
    }

    /*
     * Enforce maximum number of workers.  Note this is overly restrictive: we
     * could allow more non-shmem-connected workers, because these don't count
     * towards the MAX_BACKENDS limit elsewhere.  For now, it doesn't seem
     * important to relax this restriction.
     */
    numworkers += 1;
    if numworkers > max_worker_processes {
        /* errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED) folded into comment;
         * errdetail_plural / errhint omitted (not yet ported) */
        ereport!(
            LOG,
            errmsg!(
                "too many background workers -- up to {} can be registered with the current settings (consider increasing max_worker_processes)",
                max_worker_processes
            )
        );
        return;
    }

    /*
     * Copy the registration data into the registered workers list.
     */
    let rw_new: *mut RegisteredBgWorker = MemoryContextAllocExtended(
        PostmasterContext as crate::utils::palloc::MemoryContext,
        core::mem::size_of::<RegisteredBgWorker>(),
        MCXT_ALLOC_NO_OOM,
    ) as *mut RegisteredBgWorker;
    if rw_new.is_null() {
        /* errcode(ERRCODE_OUT_OF_MEMORY) folded into comment */
        ereport!(LOG, errmsg!("out of memory"));
        return;
    }
    let rw = rw_new;

    memcpy(
        &raw mut (*rw).rw_worker as *mut c_void,
        worker as *const c_void,
        core::mem::size_of::<BackgroundWorker>(),
    );
    (*rw).rw_pid = 0;
    (*rw).rw_crashed_at = 0;
    (*rw).rw_terminate = false;

    crate::lib::ilist::dlist_push_head(
        &raw mut BackgroundWorkerList,
        &raw mut (*rw).rw_lnode,
    );
}

// ----------------------------------------------------------------
// RegisterDynamicBackgroundWorker
// ----------------------------------------------------------------

/*
 * Register a new background worker from a regular backend.
 *
 * Returns true on success and false on failure.  Failure typically indicates
 * that no background worker slots are currently available.
 *
 * If handle != NULL, we'll set *handle to a pointer that can subsequently
 * be used as an argument to GetBackgroundWorkerPid().  The caller can
 * free this pointer using pfree(), if desired.
 */
pub unsafe fn RegisterDynamicBackgroundWorker(
    worker: *mut BackgroundWorker,
    handle: *mut *mut BackgroundWorkerHandle,
) -> bool {
    let mut slotno: c_int = 0;
    let mut success: bool = false;
    let parallel: bool;
    let mut generation: u64 = 0;

    /*
     * We can't register dynamic background workers from the postmaster. If
     * this is a standalone backend, we're the only process and can't start
     * any more.  In a multi-process environment, it might be theoretically
     * possible, but we don't currently support it due to locking
     * considerations; see comments on the BackgroundWorkerSlot data
     * structure.
     */
    if !IsUnderPostmaster {
        return false;
    }

    if !SanityCheckBackgroundWorker(worker, ERROR) {
        return false;
    }

    parallel = ((*worker).bgw_flags & BGWORKER_CLASS_PARALLEL) != 0;

    LWLockAcquire(BackgroundWorkerLock(), LW_EXCLUSIVE);

    /*
     * If this is a parallel worker, check whether there are already too many
     * parallel workers; if so, don't register another one.  Our view of
     * parallel_terminate_count may be slightly stale, but that doesn't really
     * matter: we would have gotten the same result if we'd arrived here
     * slightly earlier anyway.  There's no help for it, either, since the
     * postmaster must not take locks; a memory barrier wouldn't guarantee
     * anything useful.
     */
    if parallel
        && ((*BackgroundWorkerData).parallel_register_count
            .wrapping_sub((*BackgroundWorkerData).parallel_terminate_count)) as c_int
            >= max_parallel_workers
    {
        Assert!(
            ((*BackgroundWorkerData).parallel_register_count
                .wrapping_sub((*BackgroundWorkerData).parallel_terminate_count)) as c_int
                <= MAX_PARALLEL_WORKER_LIMIT
        );
        LWLockRelease(BackgroundWorkerLock());
        return false;
    }

    /*
     * Look for an unused slot.  If we find one, grab it.
     */
    let total = (*BackgroundWorkerData).total_slots;
    let mut s = 0;
    while s < total {
        let slot: *mut BackgroundWorkerSlot = bwa_slot(BackgroundWorkerData, s as usize);

        if !(*slot).in_use {
            memcpy(
                &raw mut (*slot).worker as *mut c_void,
                worker as *const c_void,
                core::mem::size_of::<BackgroundWorker>(),
            );
            (*slot).pid = InvalidPid; /* indicates not started yet */
            (*slot).generation += 1;
            (*slot).terminate = false;
            generation = (*slot).generation;
            if parallel {
                (*BackgroundWorkerData).parallel_register_count += 1;
            }

            /*
             * Make sure postmaster doesn't see the slot as in use before it
             * sees the new contents.
             */
            pg_write_barrier();

            (*slot).in_use = true;
            success = true;
            slotno = s;
            break;
        }
        s += 1;
    }

    LWLockRelease(BackgroundWorkerLock());

    /* If we found a slot, tell the postmaster to notice the change. */
    if success {
        SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE);
    }

    /*
     * If we found a slot and the user has provided a handle, initialize it.
     */
    if success && !handle.is_null() {
        *handle = palloc(core::mem::size_of::<BackgroundWorkerHandle>()) as *mut BackgroundWorkerHandle;
        (**handle).slot = slotno;
        (**handle).generation = generation;
    }

    success
}

// ----------------------------------------------------------------
// GetBackgroundWorkerPid
// ----------------------------------------------------------------

/*
 * Get the PID of a dynamically-registered background worker.
 *
 * If the worker is determined to be running, the return value will be
 * BGWH_STARTED and *pidp will get the PID of the worker process.  If the
 * postmaster has not yet attempted to start the worker, the return value will
 * be BGWH_NOT_YET_STARTED.  Otherwise, the return value is BGWH_STOPPED.
 *
 * BGWH_STOPPED can indicate either that the worker is temporarily stopped
 * (because it is configured for automatic restart and exited non-zero),
 * or that the worker is permanently stopped (because it exited with exit
 * code 0, or was not configured for automatic restart), or even that the
 * worker was unregistered without ever starting (either because startup
 * failed and the worker is not configured for automatic restart, or because
 * TerminateBackgroundWorker was used before the worker was successfully
 * started).
 */
pub unsafe fn GetBackgroundWorkerPid(
    handle: *mut BackgroundWorkerHandle,
    pidp: *mut pid_t,
) -> BgwHandleStatus {
    let slot: *mut BackgroundWorkerSlot;
    let pid: pid_t;

    Assert!((*handle).slot < max_worker_processes);
    slot = bwa_slot(BackgroundWorkerData, (*handle).slot as usize);

    /*
     * We could probably arrange to synchronize access to data using memory
     * barriers only, but for now, let's just keep it simple and grab the
     * lock.  It seems unlikely that there will be enough traffic here to
     * result in meaningful contention.
     */
    LWLockAcquire(BackgroundWorkerLock(), LW_SHARED);

    /*
     * The generation number can't be concurrently changed while we hold the
     * lock.  The pid, which is updated by the postmaster, can change at any
     * time, but we assume such changes are atomic.  So the value we read
     * won't be garbage, but it might be out of date by the time the caller
     * examines it (but that's unavoidable anyway).
     *
     * The in_use flag could be in the process of changing from true to false,
     * but if it is already false then it can't change further.
     */
    if (*handle).generation != (*slot).generation || !(*slot).in_use {
        pid = 0;
    } else {
        pid = (*slot).pid;
    }

    /* All done. */
    LWLockRelease(BackgroundWorkerLock());

    if pid == 0 {
        return BGWH_STOPPED;
    } else if pid == InvalidPid {
        return BGWH_NOT_YET_STARTED;
    }
    *pidp = pid;
    BGWH_STARTED
}

// ----------------------------------------------------------------
// WaitForBackgroundWorkerStartup
// ----------------------------------------------------------------

/*
 * Wait for a background worker to start up.
 *
 * This is like GetBackgroundWorkerPid(), except that if the worker has not
 * yet started, we wait for it to do so; thus, BGWH_NOT_YET_STARTED is never
 * returned.  However, if the postmaster has died, we give up and return
 * BGWH_POSTMASTER_DIED, since in that case we know that startup will not
 * take place.
 *
 * The caller *must* have set our PID as the worker's bgw_notify_pid,
 * else we will not be awoken promptly when the worker's state changes.
 */
pub unsafe fn WaitForBackgroundWorkerStartup(
    handle: *mut BackgroundWorkerHandle,
    pidp: *mut pid_t,
) -> BgwHandleStatus {
    let mut status: BgwHandleStatus;

    loop {
        let mut pid: pid_t = 0;

        CHECK_FOR_INTERRUPTS();

        status = GetBackgroundWorkerPid(handle, &mut pid);
        if matches!(status, BGWH_STARTED) {
            *pidp = pid;
        }
        if !matches!(status, BGWH_NOT_YET_STARTED) {
            break;
        }

        let rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_STARTUP,
        );

        if (rc & WL_POSTMASTER_DEATH) != 0 {
            status = BGWH_POSTMASTER_DIED;
            break;
        }

        ResetLatch(MyLatch);
    }

    status
}

// ----------------------------------------------------------------
// WaitForBackgroundWorkerShutdown
// ----------------------------------------------------------------

/*
 * Wait for a background worker to stop.
 *
 * If the worker hasn't yet started, or is running, we wait for it to stop
 * and then return BGWH_STOPPED.  However, if the postmaster has died, we give
 * up and return BGWH_POSTMASTER_DIED, because it's the postmaster that
 * notifies us when a worker's state changes.
 *
 * The caller *must* have set our PID as the worker's bgw_notify_pid,
 * else we will not be awoken promptly when the worker's state changes.
 */
pub unsafe fn WaitForBackgroundWorkerShutdown(
    handle: *mut BackgroundWorkerHandle,
) -> BgwHandleStatus {
    let mut status: BgwHandleStatus;

    loop {
        let mut pid: pid_t = 0;

        CHECK_FOR_INTERRUPTS();

        status = GetBackgroundWorkerPid(handle, &mut pid);
        if matches!(status, BGWH_STOPPED) {
            break;
        }

        let rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            0,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        );

        if (rc & WL_POSTMASTER_DEATH) != 0 {
            status = BGWH_POSTMASTER_DIED;
            break;
        }

        ResetLatch(MyLatch);
    }

    status
}

// ----------------------------------------------------------------
// TerminateBackgroundWorker
// ----------------------------------------------------------------

/*
 * Instruct the postmaster to terminate a background worker.
 *
 * Note that it's safe to do this without regard to whether the worker is
 * still running, or even if the worker may already have exited and been
 * unregistered.
 */
pub unsafe fn TerminateBackgroundWorker(handle: *mut BackgroundWorkerHandle) {
    let slot: *mut BackgroundWorkerSlot;
    let mut signal_postmaster: bool = false;

    Assert!((*handle).slot < max_worker_processes);
    slot = bwa_slot(BackgroundWorkerData, (*handle).slot as usize);

    /* Set terminate flag in shared memory, unless slot has been reused. */
    LWLockAcquire(BackgroundWorkerLock(), LW_EXCLUSIVE);
    if (*handle).generation == (*slot).generation {
        (*slot).terminate = true;
        signal_postmaster = true;
    }
    LWLockRelease(BackgroundWorkerLock());

    /* Make sure the postmaster notices the change to shared memory. */
    if signal_postmaster {
        SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE);
    }
}

// ----------------------------------------------------------------
// LookupBackgroundWorkerFunction (private)
// ----------------------------------------------------------------

/*
 * Look up (and possibly load) a bgworker entry point function.
 *
 * For functions contained in the core code, we use library name "postgres"
 * and consult the InternalBGWorkers array.  External functions are
 * looked up, and loaded if necessary, using load_external_function().
 *
 * The point of this is to pass function names as strings across process
 * boundaries.  We can't pass actual function addresses because of the
 * possibility that the function has been loaded at a different address
 * in a different process.  This is obviously a hazard for functions in
 * loadable libraries, but it can happen even for functions in the core code
 * on platforms using EXEC_BACKEND (e.g., Windows).
 *
 * At some point it might be worthwhile to get rid of InternalBGWorkers[]
 * in favor of applying load_external_function() for core functions too;
 * but that raises portability issues that are not worth addressing now.
 */
unsafe fn LookupBackgroundWorkerFunction(
    libraryname: *const c_char,
    funcname: *const c_char,
) -> bgworker_main_type {
    /*
     * If the function is to be loaded from postgres itself, search the
     * InternalBGWorkers array.
     */
    if strcmp(libraryname, c"postgres".as_ptr()) == 0 {
        for entry in INTERNAL_BG_WORKERS {
            let entry_name = entry.fn_name.as_bytes();
            /* compare funcname C string with entry.fn_name Rust &str */
            let func_bytes = core::slice::from_raw_parts(
                funcname as *const u8,
                /* find NUL terminator */
                {
                    let mut len = 0usize;
                    while *funcname.add(len) != 0 {
                        len += 1;
                    }
                    len
                },
            );
            if func_bytes == entry_name {
                return entry.fn_addr;
            }
        }

        /* We can only reach this by programming error. */
        elog!(
            ERROR,
            "internal function \"{}\" not found",
            core::ffi::CStr::from_ptr(funcname).to_string_lossy()
        );
        /* unreachable after elog!(ERROR), but satisfies the type checker */
        core::hint::unreachable_unchecked()
    }

    /* Otherwise load from external library. */
    let func_ptr = load_external_function(libraryname, funcname, true, null_mut());
    core::mem::transmute::<*mut c_void, bgworker_main_type>(func_ptr)
}

// ----------------------------------------------------------------
// GetBackgroundWorkerTypeByPid
// ----------------------------------------------------------------

/*
 * Given a PID, get the bgw_type of the background worker.  Returns NULL if
 * not a valid background worker.
 *
 * The return value is in static memory belonging to this function, so it has
 * to be used before calling this function again.  This is so that the caller
 * doesn't have to worry about the background worker locking protocol.
 */
pub unsafe fn GetBackgroundWorkerTypeByPid(pid: pid_t) -> *const c_char {
    let mut slotno: c_int;
    let mut found: bool = false;
    static mut result: [c_char; BGW_MAXLEN] = [0; BGW_MAXLEN];

    LWLockAcquire(BackgroundWorkerLock(), LW_SHARED);

    slotno = 0;
    while slotno < (*BackgroundWorkerData).total_slots {
        let slot: *mut BackgroundWorkerSlot = bwa_slot(BackgroundWorkerData, slotno as usize);

        if (*slot).pid > 0 && (*slot).pid == pid {
            strcpy(result.as_mut_ptr(), (*slot).worker.bgw_type.as_ptr());
            found = true;
            break;
        }
        slotno += 1;
    }

    LWLockRelease(BackgroundWorkerLock());

    if !found {
        return null();
    }

    result.as_ptr()
}

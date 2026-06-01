//! Translation of `postgres/src/backend/access/transam/parallel.c`
//!
//! Infrastructure for launching parallel workers.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]

use crate::prelude::*;

use core::ffi::CStr;

use crate::lib::ilist::{dlist_delete, dlist_head, dlist_is_empty, dlist_iter, dlist_node, dlist_push_head};

// ---------------------------------------------------------------------------
// Stub types -- symbols whose real home has not been ported yet.
// ---------------------------------------------------------------------------

/// TODO(pg-port): dsm_segment (storage/dsm.h)
pub enum dsm_segment {}

/// TODO(pg-port): dsm_handle (storage/dsm.h)
pub type dsm_handle = uint32;

/// TODO(pg-port): shm_toc (storage/shm_toc.h)
pub enum shm_toc {}

/// TODO(pg-port): shm_mq (storage/shm_mq.h)
pub enum shm_mq {}

/// TODO(pg-port): shm_mq_handle (storage/shm_mq.h)
pub enum shm_mq_handle {}

/// TODO(pg-port): shm_mq_result (storage/shm_mq.h)
#[repr(C)]
#[derive(PartialEq)]
pub enum shm_mq_result {
    SHM_MQ_SUCCESS,
    SHM_MQ_WOULD_BLOCK,
    SHM_MQ_DETACHED,
}
pub use shm_mq_result::*;

/// TODO(pg-port): shm_toc_estimator (storage/shm_toc.h)
#[repr(C)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/// TODO(pg-port): PGPROC (storage/proc.h)
pub enum PGPROC {}

/// TODO(pg-port): ProcNumber (storage/procnumber.h)
pub type ProcNumber = c_int;

/// TODO(pg-port): SerializableXactHandle (storage/predicate.h)
pub type SerializableXactHandle = *mut c_void;

/// TODO(pg-port): slock_t (storage/spin.h)
pub type slock_t = c_int;

/// TODO(pg-port): SubTransactionId (c.h)
pub type SubTransactionId = uint32;

/// TODO(pg-port): TimestampTz (datatype/timestamp.h)
pub type TimestampTz = int64;

/// TODO(pg-port): XLogRecPtr (access/xlogdefs.h)
pub type XLogRecPtr = uint64;

/// TODO(pg-port): Snapshot (utils/snapshot.h)
pub type Snapshot = *mut SnapshotData;
pub enum SnapshotData {}

/// TODO(pg-port): ErrorContextCallback (utils/elog.h)
pub enum ErrorContextCallback {}

/// TODO(pg-port): BackgroundWorker (postmaster/bgworker.h)
#[repr(C)]
pub struct BackgroundWorker {
    pub bgw_name: [c_char; BGW_MAXLEN],
    pub bgw_type: [c_char; BGW_MAXLEN],
    pub bgw_flags: c_int,
    pub bgw_start_time: BgWorkerStartTime,
    pub bgw_restart_time: c_int,
    pub bgw_library_name: [c_char; MAXPGPATH],
    pub bgw_function_name: [c_char; BGW_MAXLEN],
    pub bgw_main_arg: Datum,
    pub bgw_extra: [c_char; BGW_EXTRALEN],
    pub bgw_notify_pid: pid_t,
}

/// TODO(pg-port): BackgroundWorkerHandle (postmaster/bgworker.h)
pub enum BackgroundWorkerHandle {}

/// TODO(pg-port): BgWorkerStartTime (postmaster/bgworker.h)
pub type BgWorkerStartTime = c_int;

/// TODO(pg-port): BgwHandleStatus (postmaster/bgworker.h)
#[repr(C)]
#[derive(PartialEq)]
pub enum BgwHandleStatus {
    BGWH_STARTED,
    BGWH_NOT_YET_STARTED,
    BGWH_STOPPED,
    BGWH_POSTMASTER_DIED,
}
pub use BgwHandleStatus::*;

/// TODO(pg-port): ErrorData (utils/elog.h)
#[repr(C)]
pub struct ErrorData {
    pub elevel: c_int,
    pub context: *mut c_char,
    // (other fields omitted; TODO(pg-port))
}

/// TODO(pg-port): StringInfo / StringInfoData (lib/stringinfo.h)
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
pub type StringInfo = *mut StringInfoData;

/// TODO(pg-port): pid_t (system header)
pub type pid_t = c_int;

/// TODO(pg-port): DebugParallelMode (utils/guc.h)
pub const DEBUG_PARALLEL_REGRESS: c_int = 2;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const BGW_MAXLEN: usize = 96;
pub const BGW_EXTRALEN: usize = 128;
pub const MAXPGPATH: usize = 1024;

/// TODO(pg-port): bgworker flags (postmaster/bgworker.h)
pub const BGWORKER_SHMEM_ACCESS: c_int = 0x0001;
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: c_int = 0x0002;
pub const BGWORKER_CLASS_PARALLEL: c_int = 0x0010;
pub const BgWorkerStart_ConsistentState: BgWorkerStartTime = 2;
pub const BGW_NEVER_RESTART: c_int = -1;
pub const BGWORKER_BYPASS_ALLOWCONN: c_int = 1;
pub const BGWORKER_BYPASS_ROLELOGINCHECK: c_int = 2;

/// TODO(pg-port): dsm flags (storage/dsm.h)
pub const DSM_HANDLE_INVALID: dsm_handle = 0;
pub const DSM_CREATE_NULL_IF_MAXSEGMENTS: c_int = 0x0001;

/// TODO(pg-port): latch wait flags (storage/latch.h)
pub const WL_LATCH_SET: c_int = 1 << 0;
pub const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

/// TODO(pg-port): wait event ids (utils/wait_event.h)
pub const WAIT_EVENT_BGWORKER_STARTUP: uint32 = 0;
pub const WAIT_EVENT_PARALLEL_FINISH: uint32 = 0;

/// TODO(pg-port): procsignal reason (storage/procsignal.h)
pub const PROCSIG_PARALLEL_MESSAGE: c_int = 0;

/// TODO(pg-port): error codes (utils/errcodes.h)
pub const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
pub const ERRCODE_ADMIN_SHUTDOWN: c_int = 0;

/// TODO(pg-port): protocol message bytes (libpq/protocol.h)
pub const PqMsg_ErrorResponse: c_char = b'E' as c_char;
pub const PqMsg_NoticeResponse: c_char = b'N' as c_char;
pub const PqMsg_NotificationResponse: c_char = b'A' as c_char;
pub const PqMsg_Progress: c_char = b'P' as c_char;
pub const PqMsg_Terminate: c_char = b'X' as c_char;

/*
 * We don't want to waste a lot of memory on an error queue which, most of
 * the time, will process only a handful of small messages.  However, it is
 * desirable to make it large enough that a typical ErrorResponse can be sent
 * without blocking.  That way, a worker that errors out can write the whole
 * message into the queue and terminate without waiting for the user backend.
 */
pub const PARALLEL_ERROR_QUEUE_SIZE: Size = 16384;

/* Magic number for parallel context TOC. */
pub const PARALLEL_MAGIC: uint32 = 0x50477c7c;

/*
 * Magic numbers for per-context parallel state sharing.  Higher-level code
 * should use smaller values, leaving these very large ones for use by this
 * module.
 */
pub const PARALLEL_KEY_FIXED: uint64 = 0xFFFFFFFFFFFF0001;
pub const PARALLEL_KEY_ERROR_QUEUE: uint64 = 0xFFFFFFFFFFFF0002;
pub const PARALLEL_KEY_LIBRARY: uint64 = 0xFFFFFFFFFFFF0003;
pub const PARALLEL_KEY_GUC: uint64 = 0xFFFFFFFFFFFF0004;
pub const PARALLEL_KEY_COMBO_CID: uint64 = 0xFFFFFFFFFFFF0005;
pub const PARALLEL_KEY_TRANSACTION_SNAPSHOT: uint64 = 0xFFFFFFFFFFFF0006;
pub const PARALLEL_KEY_ACTIVE_SNAPSHOT: uint64 = 0xFFFFFFFFFFFF0007;
pub const PARALLEL_KEY_TRANSACTION_STATE: uint64 = 0xFFFFFFFFFFFF0008;
pub const PARALLEL_KEY_ENTRYPOINT: uint64 = 0xFFFFFFFFFFFF0009;
pub const PARALLEL_KEY_SESSION_DSM: uint64 = 0xFFFFFFFFFFFF000A;
pub const PARALLEL_KEY_PENDING_SYNCS: uint64 = 0xFFFFFFFFFFFF000B;
pub const PARALLEL_KEY_REINDEX_STATE: uint64 = 0xFFFFFFFFFFFF000C;
pub const PARALLEL_KEY_RELMAPPER_STATE: uint64 = 0xFFFFFFFFFFFF000D;
pub const PARALLEL_KEY_UNCOMMITTEDENUMS: uint64 = 0xFFFFFFFFFFFF000E;
pub const PARALLEL_KEY_CLIENTCONNINFO: uint64 = 0xFFFFFFFFFFFF000F;

// ---------------------------------------------------------------------------
// parallel.h types
// ---------------------------------------------------------------------------

pub type parallel_worker_main_type = unsafe fn(seg: *mut dsm_segment, toc: *mut shm_toc);

#[repr(C)]
pub struct ParallelWorkerInfo {
    pub bgwhandle: *mut BackgroundWorkerHandle,
    pub error_mqh: *mut shm_mq_handle,
}

#[repr(C)]
pub struct ParallelContext {
    pub node: dlist_node,
    pub subid: SubTransactionId,
    pub nworkers: c_int,            /* Maximum number of workers to launch */
    pub nworkers_to_launch: c_int,  /* Actual number of workers to launch */
    pub nworkers_launched: c_int,
    pub library_name: *mut c_char,
    pub function_name: *mut c_char,
    pub error_context_stack: *mut ErrorContextCallback,
    pub estimator: shm_toc_estimator,
    pub seg: *mut dsm_segment,
    pub private_memory: *mut c_void,
    pub toc: *mut shm_toc,
    pub worker: *mut ParallelWorkerInfo,
    pub nknown_attached_workers: c_int,
    pub known_attached_workers: *mut bool,
}

#[repr(C)]
pub struct ParallelWorkerContext {
    pub seg: *mut dsm_segment,
    pub toc: *mut shm_toc,
}

/* Fixed-size parallel state. */
#[repr(C)]
pub struct FixedParallelState {
    /* Fixed-size state that workers must restore. */
    pub database_id: Oid,
    pub authenticated_user_id: Oid,
    pub session_user_id: Oid,
    pub outer_user_id: Oid,
    pub current_user_id: Oid,
    pub temp_namespace_id: Oid,
    pub temp_toast_namespace_id: Oid,
    pub sec_context: c_int,
    pub session_user_is_superuser: bool,
    pub role_is_superuser: bool,
    pub parallel_leader_pgproc: *mut PGPROC,
    pub parallel_leader_pid: pid_t,
    pub parallel_leader_proc_number: ProcNumber,
    pub xact_ts: TimestampTz,
    pub stmt_ts: TimestampTz,
    pub serializable_xact_handle: SerializableXactHandle,

    /* Mutex protects remaining fields. */
    pub mutex: slock_t,

    /* Maximum XactLastRecEnd of any worker. */
    pub last_xlog_end: XLogRecPtr,
}

// ---------------------------------------------------------------------------
// Module-level globals
// ---------------------------------------------------------------------------

/*
 * Our parallel worker number.  We initialize this to -1, meaning that we are
 * not a parallel worker.  In parallel workers, it will be set to a value >= 0
 * and < the number of workers before any user code is invoked; each parallel
 * worker will get a different parallel worker number.
 */
pub static mut ParallelWorkerNumber: c_int = -1;

/* Is there a parallel message pending which we need to receive? */
pub static mut ParallelMessagePending: sig_atomic_t = 0; /* false */

/* Are we initializing a parallel worker? */
pub static mut InitializingParallelWorker: bool = false;

/* Pointer to our fixed parallel state. */
static mut MyFixedParallelState: *mut FixedParallelState = null_mut();

/* List of active parallel contexts. */
static mut pcxt_list: dlist_head = dlist_head {
    head: dlist_node { prev: null_mut(), next: null_mut() },
};

/* Backend-local copy of data from FixedParallelState. */
static mut ParallelLeaderPid: pid_t = 0;

/// TODO(pg-port): sig_atomic_t (system header)
pub type sig_atomic_t = c_int;

/*
 * List of internal parallel worker entry points.  We need this for
 * reasons explained in LookupParallelWorkerFunction(), below.
 */
struct InternalParallelWorkerEntry {
    fn_name: &'static [u8],
    fn_addr: parallel_worker_main_type,
}

static InternalParallelWorkers: [InternalParallelWorkerEntry; 5] = [
    InternalParallelWorkerEntry { fn_name: b"ParallelQueryMain\0", fn_addr: ParallelQueryMain },
    InternalParallelWorkerEntry { fn_name: b"_bt_parallel_build_main\0", fn_addr: _bt_parallel_build_main },
    InternalParallelWorkerEntry { fn_name: b"_brin_parallel_build_main\0", fn_addr: _brin_parallel_build_main },
    InternalParallelWorkerEntry { fn_name: b"_gin_parallel_build_main\0", fn_addr: _gin_parallel_build_main },
    InternalParallelWorkerEntry { fn_name: b"parallel_vacuum_main\0", fn_addr: parallel_vacuum_main },
];

/*
 * Establish a new parallel context.  This should be done after entering
 * parallel mode, and (unless there is an error) the context should be
 * destroyed before exiting the current subtransaction.
 */
pub unsafe fn CreateParallelContext(
    library_name: *const c_char,
    function_name: *const c_char,
    nworkers: c_int,
) -> *mut ParallelContext {
    let oldcontext: MemoryContext;
    let pcxt: *mut ParallelContext;

    /* It is unsafe to create a parallel context if not in parallel mode. */
    Assert!(IsInParallelMode());

    /* Number of workers should be non-negative. */
    Assert!(nworkers >= 0);

    /* We might be running in a short-lived memory context. */
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    /* Initialize a new ParallelContext. */
    pcxt = palloc0(core::mem::size_of::<ParallelContext>()) as *mut ParallelContext;
    (*pcxt).subid = GetCurrentSubTransactionId();
    (*pcxt).nworkers = nworkers;
    (*pcxt).nworkers_to_launch = nworkers;
    (*pcxt).library_name = pstrdup(library_name);
    (*pcxt).function_name = pstrdup(function_name);
    (*pcxt).error_context_stack = error_context_stack;
    shm_toc_initialize_estimator(&mut (*pcxt).estimator);
    dlist_push_head(&mut pcxt_list, &mut (*pcxt).node);

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);

    pcxt
}

/*
 * Establish the dynamic shared memory segment for a parallel context and
 * copy state and other bookkeeping information that will be needed by
 * parallel workers into it.
 */
pub unsafe fn InitializeParallelDSM(pcxt: *mut ParallelContext) {
    let oldcontext: MemoryContext;
    let mut library_len: Size = 0;
    let mut guc_len: Size = 0;
    let mut combocidlen: Size = 0;
    let mut tsnaplen: Size = 0;
    let mut asnaplen: Size = 0;
    let mut tstatelen: Size = 0;
    let mut pendingsyncslen: Size = 0;
    let mut reindexlen: Size = 0;
    let mut relmapperlen: Size = 0;
    let mut uncommittedenumslen: Size = 0;
    let mut clientconninfolen: Size = 0;
    let mut segsize: Size = 0;
    let mut i: c_int;
    let fps: *mut FixedParallelState;
    let mut session_dsm_handle: dsm_handle = DSM_HANDLE_INVALID;
    let transaction_snapshot: Snapshot = GetTransactionSnapshot();
    let active_snapshot: Snapshot = GetActiveSnapshot();

    /* We might be running in a very short-lived memory context. */
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    /* Allow space to store the fixed-size parallel state. */
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, core::mem::size_of::<FixedParallelState>());
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);

    /*
     * If we manage to reach here while non-interruptible, it's unsafe to
     * launch any workers: we would fail to process interrupts sent by them.
     * We can deal with that edge case by pretending no workers were
     * requested.
     */
    if !INTERRUPTS_CAN_BE_PROCESSED() {
        (*pcxt).nworkers = 0;
    }

    /*
     * Normally, the user will have requested at least one worker process, but
     * if by chance they have not, we can skip a bunch of things here.
     */
    if (*pcxt).nworkers > 0 {
        /* Get (or create) the per-session DSM segment's handle. */
        session_dsm_handle = GetSessionDsmHandle();

        /*
         * If we weren't able to create a per-session DSM segment, then we can
         * continue but we can't safely launch any workers because their
         * record typmods would be incompatible so they couldn't exchange
         * tuples.
         */
        if session_dsm_handle == DSM_HANDLE_INVALID {
            (*pcxt).nworkers = 0;
        }
    }

    if (*pcxt).nworkers > 0 {
        /* Estimate space for various kinds of state sharing. */
        library_len = EstimateLibraryStateSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, library_len);
        guc_len = EstimateGUCStateSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, guc_len);
        combocidlen = EstimateComboCIDStateSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, combocidlen);
        if IsolationUsesXactSnapshot() {
            tsnaplen = EstimateSnapshotSpace(transaction_snapshot);
            shm_toc_estimate_chunk(&mut (*pcxt).estimator, tsnaplen);
        }
        asnaplen = EstimateSnapshotSpace(active_snapshot);
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, asnaplen);
        tstatelen = EstimateTransactionStateSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, tstatelen);
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, core::mem::size_of::<dsm_handle>());
        pendingsyncslen = EstimatePendingSyncsSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, pendingsyncslen);
        reindexlen = EstimateReindexStateSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, reindexlen);
        relmapperlen = EstimateRelationMapSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, relmapperlen);
        uncommittedenumslen = EstimateUncommittedEnumsSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, uncommittedenumslen);
        clientconninfolen = EstimateClientConnectionInfoSpace();
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, clientconninfolen);
        /* If you add more chunks here, you probably need to add keys. */
        shm_toc_estimate_keys(&mut (*pcxt).estimator, 12);

        /* Estimate space need for error queues. */
        StaticAssertStmt(
            BUFFERALIGN(PARALLEL_ERROR_QUEUE_SIZE) == PARALLEL_ERROR_QUEUE_SIZE,
            "parallel error queue size not buffer-aligned",
        );
        shm_toc_estimate_chunk(
            &mut (*pcxt).estimator,
            mul_size(PARALLEL_ERROR_QUEUE_SIZE, (*pcxt).nworkers as Size),
        );
        shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);

        /* Estimate how much we'll need for the entrypoint info. */
        shm_toc_estimate_chunk(
            &mut (*pcxt).estimator,
            strlen((*pcxt).library_name) + strlen((*pcxt).function_name) + 2,
        );
        shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
    }

    /*
     * Create DSM and initialize with new table of contents.  But if the user
     * didn't request any workers, then don't bother creating a dynamic shared
     * memory segment; instead, just use backend-private memory.
     *
     * Also, if we can't create a dynamic shared memory segment because the
     * maximum number of segments have already been created, then fall back to
     * backend-private memory, and plan not to use any workers.  We hope this
     * won't happen very often, but it's better to abandon the use of
     * parallelism than to fail outright.
     */
    segsize = shm_toc_estimate(&(*pcxt).estimator);
    if (*pcxt).nworkers > 0 {
        (*pcxt).seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
    }
    if !(*pcxt).seg.is_null() {
        (*pcxt).toc = shm_toc_create(PARALLEL_MAGIC as uint64, dsm_segment_address((*pcxt).seg), segsize);
    } else {
        (*pcxt).nworkers = 0;
        (*pcxt).private_memory = MemoryContextAlloc(TopMemoryContext, segsize);
        (*pcxt).toc = shm_toc_create(PARALLEL_MAGIC as uint64, (*pcxt).private_memory, segsize);
    }

    /* Initialize fixed-size state in shared memory. */
    fps = shm_toc_allocate((*pcxt).toc, core::mem::size_of::<FixedParallelState>())
        as *mut FixedParallelState;
    (*fps).database_id = MyDatabaseId;
    (*fps).authenticated_user_id = GetAuthenticatedUserId();
    (*fps).session_user_id = GetSessionUserId();
    (*fps).outer_user_id = GetCurrentRoleId();
    GetUserIdAndSecContext(&mut (*fps).current_user_id, &mut (*fps).sec_context);
    (*fps).session_user_is_superuser = GetSessionUserIsSuperuser();
    (*fps).role_is_superuser = current_role_is_superuser;
    GetTempNamespaceState(&mut (*fps).temp_namespace_id, &mut (*fps).temp_toast_namespace_id);
    (*fps).parallel_leader_pgproc = MyProc;
    (*fps).parallel_leader_pid = MyProcPid;
    (*fps).parallel_leader_proc_number = MyProcNumber;
    (*fps).xact_ts = GetCurrentTransactionStartTimestamp();
    (*fps).stmt_ts = GetCurrentStatementStartTimestamp();
    (*fps).serializable_xact_handle = ShareSerializableXact();
    SpinLockInit(&mut (*fps).mutex);
    (*fps).last_xlog_end = 0;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_FIXED, fps as *mut c_void);

    /* We can skip the rest of this if we're not budgeting for any workers. */
    if (*pcxt).nworkers > 0 {
        let libraryspace: *mut c_char;
        let gucspace: *mut c_char;
        let combocidspace: *mut c_char;
        let tsnapspace: *mut c_char;
        let asnapspace: *mut c_char;
        let tstatespace: *mut c_char;
        let pendingsyncsspace: *mut c_char;
        let reindexspace: *mut c_char;
        let relmapperspace: *mut c_char;
        let error_queue_space: *mut c_char;
        let session_dsm_handle_space: *mut c_char;
        let entrypointstate: *mut c_char;
        let uncommittedenumsspace: *mut c_char;
        let clientconninfospace: *mut c_char;
        let lnamelen: Size;

        /* Serialize shared libraries we have loaded. */
        libraryspace = shm_toc_allocate((*pcxt).toc, library_len) as *mut c_char;
        SerializeLibraryState(library_len, libraryspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_LIBRARY, libraryspace as *mut c_void);

        /* Serialize GUC settings. */
        gucspace = shm_toc_allocate((*pcxt).toc, guc_len) as *mut c_char;
        SerializeGUCState(guc_len, gucspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_GUC, gucspace as *mut c_void);

        /* Serialize combo CID state. */
        combocidspace = shm_toc_allocate((*pcxt).toc, combocidlen) as *mut c_char;
        SerializeComboCIDState(combocidlen, combocidspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_COMBO_CID, combocidspace as *mut c_void);

        /*
         * Serialize the transaction snapshot if the transaction isolation
         * level uses a transaction snapshot.
         */
        if IsolationUsesXactSnapshot() {
            tsnapspace = shm_toc_allocate((*pcxt).toc, tsnaplen) as *mut c_char;
            SerializeSnapshot(transaction_snapshot, tsnapspace);
            shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT, tsnapspace as *mut c_void);
        }

        /* Serialize the active snapshot. */
        asnapspace = shm_toc_allocate((*pcxt).toc, asnaplen) as *mut c_char;
        SerializeSnapshot(active_snapshot, asnapspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_ACTIVE_SNAPSHOT, asnapspace as *mut c_void);

        /* Provide the handle for per-session segment. */
        session_dsm_handle_space =
            shm_toc_allocate((*pcxt).toc, core::mem::size_of::<dsm_handle>()) as *mut c_char;
        *(session_dsm_handle_space as *mut dsm_handle) = session_dsm_handle;
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_SESSION_DSM, session_dsm_handle_space as *mut c_void);

        /* Serialize transaction state. */
        tstatespace = shm_toc_allocate((*pcxt).toc, tstatelen) as *mut c_char;
        SerializeTransactionState(tstatelen, tstatespace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TRANSACTION_STATE, tstatespace as *mut c_void);

        /* Serialize pending syncs. */
        pendingsyncsspace = shm_toc_allocate((*pcxt).toc, pendingsyncslen) as *mut c_char;
        SerializePendingSyncs(pendingsyncslen, pendingsyncsspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_PENDING_SYNCS, pendingsyncsspace as *mut c_void);

        /* Serialize reindex state. */
        reindexspace = shm_toc_allocate((*pcxt).toc, reindexlen) as *mut c_char;
        SerializeReindexState(reindexlen, reindexspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_REINDEX_STATE, reindexspace as *mut c_void);

        /* Serialize relmapper state. */
        relmapperspace = shm_toc_allocate((*pcxt).toc, relmapperlen) as *mut c_char;
        SerializeRelationMap(relmapperlen, relmapperspace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_RELMAPPER_STATE, relmapperspace as *mut c_void);

        /* Serialize uncommitted enum state. */
        uncommittedenumsspace = shm_toc_allocate((*pcxt).toc, uncommittedenumslen) as *mut c_char;
        SerializeUncommittedEnums(uncommittedenumsspace as *mut c_void, uncommittedenumslen);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_UNCOMMITTEDENUMS, uncommittedenumsspace as *mut c_void);

        /* Serialize our ClientConnectionInfo. */
        clientconninfospace = shm_toc_allocate((*pcxt).toc, clientconninfolen) as *mut c_char;
        SerializeClientConnectionInfo(clientconninfolen, clientconninfospace);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_CLIENTCONNINFO, clientconninfospace as *mut c_void);

        /* Allocate space for worker information. */
        (*pcxt).worker = palloc0(core::mem::size_of::<ParallelWorkerInfo>() * (*pcxt).nworkers as usize)
            as *mut ParallelWorkerInfo;

        /*
         * Establish error queues in dynamic shared memory.
         *
         * These queues should be used only for transmitting ErrorResponse,
         * NoticeResponse, and NotifyResponse protocol messages.  Tuple data
         * should be transmitted via separate (possibly larger?) queues.
         */
        error_queue_space = shm_toc_allocate(
            (*pcxt).toc,
            mul_size(PARALLEL_ERROR_QUEUE_SIZE, (*pcxt).nworkers as Size),
        ) as *mut c_char;
        i = 0;
        while i < (*pcxt).nworkers {
            let start: *mut c_char;
            let mq: *mut shm_mq;

            start = error_queue_space.add((i as usize) * PARALLEL_ERROR_QUEUE_SIZE);
            mq = shm_mq_create(start as *mut c_void, PARALLEL_ERROR_QUEUE_SIZE);
            shm_mq_set_receiver(mq, MyProc);
            (*(*pcxt).worker.add(i as usize)).error_mqh = shm_mq_attach(mq, (*pcxt).seg, null_mut());
            i += 1;
        }
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_ERROR_QUEUE, error_queue_space as *mut c_void);

        /*
         * Serialize entrypoint information.  It's unsafe to pass function
         * pointers across processes, as the function pointer may be different
         * in each process in EXEC_BACKEND builds, so we always pass library
         * and function name.  (We use library name "postgres" for functions
         * in the core backend.)
         */
        lnamelen = strlen((*pcxt).library_name);
        entrypointstate =
            shm_toc_allocate((*pcxt).toc, lnamelen + strlen((*pcxt).function_name) + 2) as *mut c_char;
        strcpy(entrypointstate, (*pcxt).library_name);
        strcpy(entrypointstate.add(lnamelen + 1), (*pcxt).function_name);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_ENTRYPOINT, entrypointstate as *mut c_void);
    }

    /* Update nworkers_to_launch, in case we changed nworkers above. */
    (*pcxt).nworkers_to_launch = (*pcxt).nworkers;

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);
}

// ---------------------------------------------------------------------------
// Stub dependencies -- functions/globals whose real home has not been ported
// yet.  These carry TODO(pg-port) bodies so the module type-checks in
// isolation; replace with `use` of the real symbol once it is translated.
// ---------------------------------------------------------------------------

/// TODO(pg-port): MyProc (storage/proc.h)
static mut MyProc: *mut PGPROC = null_mut();

/// TODO(pg-port): MyProcPid (miscadmin.h)
static mut MyProcPid: c_int = 0;

/// TODO(pg-port): MyProcNumber (storage/procnumber.h)
static mut MyProcNumber: ProcNumber = 0;

/// TODO(pg-port): MyDatabaseId (miscadmin.h)
static mut MyDatabaseId: Oid = 0;

/// TODO(pg-port): MyLatch (storage/latch.h)
static mut MyLatch: *mut Latch = null_mut();

/// TODO(pg-port): Latch (storage/latch.h)
pub enum Latch {}

/// TODO(pg-port): MyBgworkerEntry (postmaster/bgworker.h)
static mut MyBgworkerEntry: *mut BackgroundWorker = null_mut();

/// TODO(pg-port): ClientConnectionInfo (libpq/libpq-be.h)
#[repr(C)]
pub struct ClientConnectionInfo {
    pub authn_id: *const c_char,
    pub auth_method: c_int,
    // (other fields omitted; TODO(pg-port))
}

/// TODO(pg-port): MyClientConnectionInfo (miscadmin.h)
static mut MyClientConnectionInfo: ClientConnectionInfo = ClientConnectionInfo {
    authn_id: null(),
    auth_method: 0,
};

/// TODO(pg-port): InterruptPending (miscadmin.h)
static mut InterruptPending: bool = false;

/// TODO(pg-port): XactLastRecEnd (access/xlog.h)
static mut XactLastRecEnd: XLogRecPtr = 0;

/// TODO(pg-port): error_context_stack (utils/elog.h)
static mut error_context_stack: *mut ErrorContextCallback = null_mut();

/// TODO(pg-port): current_role_is_superuser (utils/acl.h)
static mut current_role_is_superuser: bool = false;

/// TODO(pg-port): debug_parallel_query (optimizer/optimizer.h)
static mut debug_parallel_query: c_int = 0;

/// TODO(pg-port): TopTransactionContext (utils/memutils.h)
static mut TopTransactionContext: MemoryContext = null_mut();

/// TODO(pg-port): TransactionXmin (utils/snapmgr.h)
static mut TransactionXmin: TransactionId = 0;

/// TODO(pg-port): ParallelLeaderProcNumber (access/parallel.c local)
static mut ParallelLeaderProcNumber: ProcNumber = 0;

/// TODO(pg-port): IsInParallelMode (access/xact.h)
unsafe fn IsInParallelMode() -> bool {
    false
}

/// TODO(pg-port): EnterParallelMode (access/xact.h)
unsafe fn EnterParallelMode() {}

/// TODO(pg-port): ExitParallelMode (access/xact.h)
unsafe fn ExitParallelMode() {}

/// TODO(pg-port): GetCurrentSubTransactionId (access/xact.h)
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId {
    0
}

/// TODO(pg-port): GetCurrentTransactionStartTimestamp (access/xact.h)
unsafe fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    0
}

/// TODO(pg-port): GetCurrentStatementStartTimestamp (access/xact.h)
unsafe fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    0
}

/// TODO(pg-port): SetParallelStartTimestamps (access/xact.h)
unsafe fn SetParallelStartTimestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) {}

/// TODO(pg-port): StartTransactionCommand (access/xact.h)
unsafe fn StartTransactionCommand() {}

/// TODO(pg-port): CommitTransactionCommand (access/xact.h)
unsafe fn CommitTransactionCommand() {}

/// TODO(pg-port): StartParallelWorkerTransaction (access/xact.h)
unsafe fn StartParallelWorkerTransaction(tstatespace: *mut c_char) {}

/// TODO(pg-port): EndParallelWorkerTransaction (access/xact.h)
unsafe fn EndParallelWorkerTransaction() {}

/// TODO(pg-port): EstimateTransactionStateSpace (access/xact.h)
unsafe fn EstimateTransactionStateSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeTransactionState (access/xact.h)
unsafe fn SerializeTransactionState(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): EstimateReindexStateSpace (catalog/index.h)
unsafe fn EstimateReindexStateSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeReindexState (catalog/index.h)
unsafe fn SerializeReindexState(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreReindexState (catalog/index.h)
unsafe fn RestoreReindexState(reindexstate: *mut c_void) {}

/// TODO(pg-port): EstimateRelationMapSpace (utils/relmapper.h)
unsafe fn EstimateRelationMapSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeRelationMap (utils/relmapper.h)
unsafe fn SerializeRelationMap(maxsize: Size, startbuf: *mut c_char) {}

/// TODO(pg-port): RestoreRelationMap (utils/relmapper.h)
unsafe fn RestoreRelationMap(startbuf: *mut c_char) {}

/// TODO(pg-port): EstimatePendingSyncsSpace (catalog/storage.h)
unsafe fn EstimatePendingSyncsSpace() -> Size {
    0
}

/// TODO(pg-port): SerializePendingSyncs (catalog/storage.h)
unsafe fn SerializePendingSyncs(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestorePendingSyncs (catalog/storage.h)
unsafe fn RestorePendingSyncs(startaddress: *mut c_char) {}

/// TODO(pg-port): EstimateComboCIDStateSpace (access/xact.h)
unsafe fn EstimateComboCIDStateSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeComboCIDState (access/xact.h)
unsafe fn SerializeComboCIDState(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreComboCIDState (access/xact.h)
unsafe fn RestoreComboCIDState(comboCIDstate: *mut c_char) {}

/// TODO(pg-port): EstimateLibraryStateSpace (utils/fmgr.h)
unsafe fn EstimateLibraryStateSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeLibraryState (utils/fmgr.h)
unsafe fn SerializeLibraryState(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreLibraryState (utils/fmgr.h)
unsafe fn RestoreLibraryState(start_address: *mut c_char) {}

/// TODO(pg-port): EstimateGUCStateSpace (utils/guc.h)
unsafe fn EstimateGUCStateSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeGUCState (utils/guc.h)
unsafe fn SerializeGUCState(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreGUCState (utils/guc.h)
unsafe fn RestoreGUCState(gucstate: *mut c_void) {}

/// TODO(pg-port): EstimateUncommittedEnumsSpace (catalog/pg_enum.h)
unsafe fn EstimateUncommittedEnumsSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeUncommittedEnums (catalog/pg_enum.h)
unsafe fn SerializeUncommittedEnums(space: *mut c_void, size: Size) {}

/// TODO(pg-port): RestoreUncommittedEnums (catalog/pg_enum.h)
unsafe fn RestoreUncommittedEnums(space: *mut c_void) {}

/// TODO(pg-port): EstimateClientConnectionInfoSpace (libpq/libpq-be.h)
unsafe fn EstimateClientConnectionInfoSpace() -> Size {
    0
}

/// TODO(pg-port): SerializeClientConnectionInfo (libpq/libpq-be.h)
unsafe fn SerializeClientConnectionInfo(maxsize: Size, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreClientConnectionInfo (libpq/libpq-be.h)
unsafe fn RestoreClientConnectionInfo(conninfo: *mut c_char) {}

/// TODO(pg-port): EstimateSnapshotSpace (utils/snapmgr.h)
unsafe fn EstimateSnapshotSpace(snapshot: Snapshot) -> Size {
    0
}

/// TODO(pg-port): SerializeSnapshot (utils/snapmgr.h)
unsafe fn SerializeSnapshot(snapshot: Snapshot, start_address: *mut c_char) {}

/// TODO(pg-port): RestoreSnapshot (utils/snapmgr.h)
unsafe fn RestoreSnapshot(start_address: *mut c_char) -> Snapshot {
    null_mut()
}

/// TODO(pg-port): RestoreTransactionSnapshot (utils/snapmgr.h)
unsafe fn RestoreTransactionSnapshot(snapshot: Snapshot, source_pgproc: *mut PGPROC) {}

/// TODO(pg-port): GetTransactionSnapshot (utils/snapmgr.h)
unsafe fn GetTransactionSnapshot() -> Snapshot {
    null_mut()
}

/// TODO(pg-port): GetActiveSnapshot (utils/snapmgr.h)
unsafe fn GetActiveSnapshot() -> Snapshot {
    null_mut()
}

/// TODO(pg-port): GetLatestSnapshot (utils/snapmgr.h)
unsafe fn GetLatestSnapshot() -> Snapshot {
    null_mut()
}

/// TODO(pg-port): PushActiveSnapshot (utils/snapmgr.h)
unsafe fn PushActiveSnapshot(snapshot: Snapshot) {}

/// TODO(pg-port): PopActiveSnapshot (utils/snapmgr.h)
unsafe fn PopActiveSnapshot() {}

/// TODO(pg-port): IsolationUsesXactSnapshot (utils/snapmgr.h)
unsafe fn IsolationUsesXactSnapshot() -> bool {
    false
}

/// TODO(pg-port): GetSessionDsmHandle (access/session.h)
unsafe fn GetSessionDsmHandle() -> dsm_handle {
    DSM_HANDLE_INVALID
}

/// TODO(pg-port): AttachSession (access/session.h)
unsafe fn AttachSession(handle: dsm_handle) {}

/// TODO(pg-port): DetachSession (access/session.h)
unsafe fn DetachSession() {}

/// TODO(pg-port): GetAuthenticatedUserId (miscadmin.h)
unsafe fn GetAuthenticatedUserId() -> Oid {
    0
}

/// TODO(pg-port): SetAuthenticatedUserId (miscadmin.h)
unsafe fn SetAuthenticatedUserId(userid: Oid) {}

/// TODO(pg-port): GetSessionUserId (miscadmin.h)
unsafe fn GetSessionUserId() -> Oid {
    0
}

/// TODO(pg-port): GetSessionUserIsSuperuser (miscadmin.h)
unsafe fn GetSessionUserIsSuperuser() -> bool {
    false
}

/// TODO(pg-port): SetSessionAuthorization (utils/acl.h)
unsafe fn SetSessionAuthorization(userid: Oid, is_superuser: bool) {}

/// TODO(pg-port): GetCurrentRoleId (utils/acl.h)
unsafe fn GetCurrentRoleId() -> Oid {
    0
}

/// TODO(pg-port): SetCurrentRoleId (utils/acl.h)
unsafe fn SetCurrentRoleId(roleid: Oid, is_superuser: bool) {}

/// TODO(pg-port): GetUserIdAndSecContext (miscadmin.h)
unsafe fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int) {}

/// TODO(pg-port): SetUserIdAndSecContext (miscadmin.h)
unsafe fn SetUserIdAndSecContext(userid: Oid, sec_context: c_int) {}

/// TODO(pg-port): GetTempNamespaceState (catalog/namespace.h)
unsafe fn GetTempNamespaceState(tempNamespaceId: *mut Oid, tempToastNamespaceId: *mut Oid) {}

/// TODO(pg-port): SetTempNamespaceState (catalog/namespace.h)
unsafe fn SetTempNamespaceState(tempNamespaceId: Oid, tempToastNamespaceId: Oid) {}

/// TODO(pg-port): ShareSerializableXact (storage/predicate.h)
unsafe fn ShareSerializableXact() -> SerializableXactHandle {
    null_mut()
}

/// TODO(pg-port): AttachSerializableXact (storage/predicate.h)
unsafe fn AttachSerializableXact(handle: SerializableXactHandle) {}

/// TODO(pg-port): InitializeSystemUser (utils/builtins.h)
unsafe fn InitializeSystemUser(authn_id: *const c_char, auth_method: *const c_char) {}

/// TODO(pg-port): hba_authname (libpq/hba.h)
unsafe fn hba_authname(auth_method: c_int) -> *const c_char {
    null()
}

/// TODO(pg-port): InvalidateSystemCaches (utils/inval.h)
unsafe fn InvalidateSystemCaches() {}

/// TODO(pg-port): GetDatabaseEncoding (mb/pg_wchar.h)
unsafe fn GetDatabaseEncoding() -> c_int {
    0
}

/// TODO(pg-port): SetClientEncoding (mb/pg_wchar.h)
unsafe fn SetClientEncoding(encoding: c_int) -> c_int {
    0
}

/// TODO(pg-port): shm_toc_initialize_estimator (storage/shm_toc.h)
unsafe fn shm_toc_initialize_estimator(e: *mut shm_toc_estimator) {}

/// TODO(pg-port): shm_toc_estimate (storage/shm_toc.h)
unsafe fn shm_toc_estimate(e: *const shm_toc_estimator) -> Size {
    0
}

/// TODO(pg-port): shm_toc_estimate_chunk (storage/shm_toc.h)
unsafe fn shm_toc_estimate_chunk(e: *mut shm_toc_estimator, sz: Size) {}

/// TODO(pg-port): shm_toc_estimate_keys (storage/shm_toc.h)
unsafe fn shm_toc_estimate_keys(e: *mut shm_toc_estimator, cnt: Size) {}

/// TODO(pg-port): shm_toc_create (storage/shm_toc.h)
unsafe fn shm_toc_create(magic: uint64, address: *mut c_void, nbytes: Size) -> *mut shm_toc {
    null_mut()
}

/// TODO(pg-port): shm_toc_attach (storage/shm_toc.h)
unsafe fn shm_toc_attach(magic: uint64, address: *mut c_void) -> *mut shm_toc {
    null_mut()
}

/// TODO(pg-port): shm_toc_allocate (storage/shm_toc.h)
unsafe fn shm_toc_allocate(toc: *mut shm_toc, nbytes: Size) -> *mut c_void {
    null_mut()
}

/// TODO(pg-port): shm_toc_insert (storage/shm_toc.h)
unsafe fn shm_toc_insert(toc: *mut shm_toc, key: uint64, address: *mut c_void) {}

/// TODO(pg-port): shm_toc_lookup (storage/shm_toc.h)
unsafe fn shm_toc_lookup(toc: *mut shm_toc, key: uint64, noError: bool) -> *mut c_void {
    null_mut()
}

/// TODO(pg-port): shm_mq_create (storage/shm_mq.h)
unsafe fn shm_mq_create(address: *mut c_void, size: Size) -> *mut shm_mq {
    null_mut()
}

/// TODO(pg-port): shm_mq_set_receiver (storage/shm_mq.h)
unsafe fn shm_mq_set_receiver(mq: *mut shm_mq, proc: *mut PGPROC) {}

/// TODO(pg-port): shm_mq_set_sender (storage/shm_mq.h)
unsafe fn shm_mq_set_sender(mq: *mut shm_mq, proc: *mut PGPROC) {}

/// TODO(pg-port): shm_mq_get_sender (storage/shm_mq.h)
unsafe fn shm_mq_get_sender(mq: *mut shm_mq) -> *mut PGPROC {
    null_mut()
}

/// TODO(pg-port): shm_mq_get_queue (storage/shm_mq.h)
unsafe fn shm_mq_get_queue(mqh: *mut shm_mq_handle) -> *mut shm_mq {
    null_mut()
}

/// TODO(pg-port): shm_mq_attach (storage/shm_mq.h)
unsafe fn shm_mq_attach(mq: *mut shm_mq, seg: *mut dsm_segment, handle: *mut BackgroundWorkerHandle) -> *mut shm_mq_handle {
    null_mut()
}

/// TODO(pg-port): shm_mq_detach (storage/shm_mq.h)
unsafe fn shm_mq_detach(mqh: *mut shm_mq_handle) {}

/// TODO(pg-port): shm_mq_set_handle (storage/shm_mq.h)
unsafe fn shm_mq_set_handle(mqh: *mut shm_mq_handle, handle: *mut BackgroundWorkerHandle) {}

/// TODO(pg-port): shm_mq_receive (storage/shm_mq.h)
unsafe fn shm_mq_receive(mqh: *mut shm_mq_handle, nbytesp: *mut Size, datap: *mut *mut c_void, nowait: bool) -> shm_mq_result {
    SHM_MQ_DETACHED
}

/// TODO(pg-port): dsm_create (storage/dsm.h)
unsafe fn dsm_create(size: Size, flags: c_int) -> *mut dsm_segment {
    null_mut()
}

/// TODO(pg-port): dsm_attach (storage/dsm.h)
unsafe fn dsm_attach(h: dsm_handle) -> *mut dsm_segment {
    null_mut()
}

/// TODO(pg-port): dsm_detach (storage/dsm.h)
unsafe fn dsm_detach(seg: *mut dsm_segment) {}

/// TODO(pg-port): dsm_segment_address (storage/dsm.h)
unsafe fn dsm_segment_address(seg: *mut dsm_segment) -> *mut c_void {
    null_mut()
}

/// TODO(pg-port): dsm_segment_handle (storage/dsm.h)
unsafe fn dsm_segment_handle(seg: *mut dsm_segment) -> dsm_handle {
    DSM_HANDLE_INVALID
}

/// TODO(pg-port): SpinLockInit (storage/spin.h)
unsafe fn SpinLockInit(lock: *mut slock_t) {}

/// TODO(pg-port): SpinLockAcquire (storage/spin.h)
unsafe fn SpinLockAcquire(lock: *mut slock_t) {}

/// TODO(pg-port): SpinLockRelease (storage/spin.h)
unsafe fn SpinLockRelease(lock: *mut slock_t) {}

/// TODO(pg-port): WaitLatch (storage/latch.h)
unsafe fn WaitLatch(latch: *mut Latch, wakeEvents: c_int, timeout: c_long, wait_event_info: uint32) -> c_int {
    0
}

/// TODO(pg-port): ResetLatch (storage/latch.h)
unsafe fn ResetLatch(latch: *mut Latch) {}

/// TODO(pg-port): SetLatch (storage/latch.h)
unsafe fn SetLatch(latch: *mut Latch) {}

/// TODO(pg-port): BecomeLockGroupLeader (storage/proc.h)
unsafe fn BecomeLockGroupLeader() {}

/// TODO(pg-port): BecomeLockGroupMember (storage/proc.h)
unsafe fn BecomeLockGroupMember(leader: *mut PGPROC, pid: pid_t) -> bool {
    false
}

/// TODO(pg-port): RegisterDynamicBackgroundWorker (postmaster/bgworker.h)
unsafe fn RegisterDynamicBackgroundWorker(worker: *mut BackgroundWorker, handle: *mut *mut BackgroundWorkerHandle) -> bool {
    false
}

/// TODO(pg-port): GetBackgroundWorkerPid (postmaster/bgworker.h)
unsafe fn GetBackgroundWorkerPid(handle: *mut BackgroundWorkerHandle, pidp: *mut pid_t) -> BgwHandleStatus {
    BGWH_STOPPED
}

/// TODO(pg-port): WaitForBackgroundWorkerShutdown (postmaster/bgworker.h)
unsafe fn WaitForBackgroundWorkerShutdown(handle: *mut BackgroundWorkerHandle) -> BgwHandleStatus {
    BGWH_STOPPED
}

/// TODO(pg-port): TerminateBackgroundWorker (postmaster/bgworker.h)
unsafe fn TerminateBackgroundWorker(handle: *mut BackgroundWorkerHandle) {}

/// TODO(pg-port): BackgroundWorkerInitializeConnectionByOid (postmaster/bgworker.h)
unsafe fn BackgroundWorkerInitializeConnectionByOid(dboid: Oid, useroid: Oid, flags: uint32) {}

/// TODO(pg-port): BackgroundWorkerUnblockSignals (postmaster/bgworker.h)
unsafe fn BackgroundWorkerUnblockSignals() {}

/// TODO(pg-port): before_shmem_exit (storage/ipc.h)
unsafe fn before_shmem_exit(function: unsafe fn(c_int, Datum), arg: Datum) {}

/// TODO(pg-port): on_dsm_detach (storage/dsm.h) -- referenced only in comments
unsafe fn on_dsm_detach() {}

/// TODO(pg-port): SendProcSignal (storage/procsignal.h)
unsafe fn SendProcSignal(pid: pid_t, reason: c_int, procNumber: ProcNumber) {}

/// TODO(pg-port): pqsignal (libpq/pqsignal.h)
unsafe fn pqsignal(signo: c_int, func: unsafe extern "C" fn(c_int)) {}

/// TODO(pg-port): die (tcop/postgres.c)
unsafe extern "C" fn die(postgres_signal_arg: c_int) {}

/// TODO(pg-port): load_external_function (utils/fmgr.h)
unsafe fn load_external_function(filename: *const c_char, funcname: *const c_char, signalNotFound: bool, filehandle: *mut c_void) -> *mut c_void {
    null_mut()
}

/// TODO(pg-port): NotifyMyFrontEnd (commands/async.h)
unsafe fn NotifyMyFrontEnd(channel: *const c_char, payload: *const c_char, srcPid: int32) {}

/// TODO(pg-port): ThrowErrorData (utils/elog.h)
unsafe fn ThrowErrorData(edata: *mut ErrorData) {}

/// TODO(pg-port): pgstat_progress_incr_param (utils/backend_progress.h)
unsafe fn pgstat_progress_incr_param(index: c_int, incr: int64) {}

/// TODO(pg-port): initStringInfo (lib/stringinfo.h)
unsafe fn initStringInfo(str: StringInfo) {}

/// TODO(pg-port): appendBinaryStringInfo (lib/stringinfo.h)
unsafe fn appendBinaryStringInfo(str: StringInfo, data: *const c_char, datalen: c_int) {}

/// TODO(pg-port): pq_getmsgbyte (libpq/pqformat.h)
unsafe fn pq_getmsgbyte(msg: StringInfo) -> c_char {
    0
}

/// TODO(pg-port): pq_getmsgint (libpq/pqformat.h)
unsafe fn pq_getmsgint(msg: StringInfo, b: c_int) -> c_uint {
    0
}

/// TODO(pg-port): pq_getmsgint64 (libpq/pqformat.h)
unsafe fn pq_getmsgint64(msg: StringInfo) -> int64 {
    0
}

/// TODO(pg-port): pq_getmsgrawstring (libpq/pqformat.h)
unsafe fn pq_getmsgrawstring(msg: StringInfo) -> *const c_char {
    null()
}

/// TODO(pg-port): pq_getmsgend (libpq/pqformat.h)
unsafe fn pq_getmsgend(msg: StringInfo) {}

/// TODO(pg-port): pq_endmessage (libpq/pqformat.h)
unsafe fn pq_endmessage(msg: StringInfo) {}

/// TODO(pg-port): pq_parse_errornotice (libpq/pqformat.h)
unsafe fn pq_parse_errornotice(msg: StringInfo, edata: *mut ErrorData) {}

/// TODO(pg-port): pq_redirect_to_shm_mq (libpq/pqmq.h)
unsafe fn pq_redirect_to_shm_mq(seg: *mut dsm_segment, mqh: *mut shm_mq_handle) {}

/// TODO(pg-port): pq_set_parallel_leader (libpq/pqmq.h)
unsafe fn pq_set_parallel_leader(pid: pid_t, procNumber: ProcNumber) {}

/// TODO(pg-port): pq_putmessage (libpq/libpq.h)
unsafe fn pq_putmessage(msgtype: c_char, s: *const c_char, len: usize) -> c_int {
    0
}

/// TODO(pg-port): psprintf (lib/psprintf.h)
unsafe fn psprintf(fmt: *const c_char, context: *const c_char, label: *const c_char) -> *mut c_char {
    null_mut()
}

/// TODO(pg-port): mul_size (storage/shmem.h)
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

/// TODO(pg-port): strlen (string.h)
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/// TODO(pg-port): strcpy (string.h)
unsafe fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char {
    let mut i: usize = 0;
    loop {
        let ch = *src.add(i);
        *dest.add(i) = ch;
        if ch == 0 {
            break;
        }
        i += 1;
    }
    dest
}

/// TODO(pg-port): strcmp (string.h)
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i: usize = 0;
    loop {
        let ca = *a.add(i);
        let cb = *b.add(i);
        if ca != cb {
            return (ca as c_int) - (cb as c_int);
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

// The internal parallel worker entry points live in other backend files; stub
// them here so the InternalParallelWorkers table type-checks.

/// TODO(pg-port): ParallelQueryMain (executor/execParallel.c)
unsafe fn ParallelQueryMain(seg: *mut dsm_segment, toc: *mut shm_toc) {}

/// TODO(pg-port): _bt_parallel_build_main (access/nbtree/nbtsort.c)
unsafe fn _bt_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {}

/// TODO(pg-port): _brin_parallel_build_main (access/brin/brin.c)
unsafe fn _brin_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {}

/// TODO(pg-port): _gin_parallel_build_main (access/gin/gininsert.c)
unsafe fn _gin_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {}

/// TODO(pg-port): parallel_vacuum_main (commands/vacuumparallel.c)
unsafe fn parallel_vacuum_main(seg: *mut dsm_segment, toc: *mut shm_toc) {}

// ---------------------------------------------------------------------------
// Stub interrupt/alignment helpers.  In C these are header macros, but the
// already-written code above calls them with function-call syntax, so they are
// rendered as functions here until the real headers are ported.
// ---------------------------------------------------------------------------

/// TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS
unsafe fn CHECK_FOR_INTERRUPTS() {}

/// TODO(pg-port): miscadmin.h HOLD_INTERRUPTS
unsafe fn HOLD_INTERRUPTS() {}

/// TODO(pg-port): miscadmin.h RESUME_INTERRUPTS
unsafe fn RESUME_INTERRUPTS() {}

/// TODO(pg-port): miscadmin.h INTERRUPTS_CAN_BE_PROCESSED
unsafe fn INTERRUPTS_CAN_BE_PROCESSED() -> bool {
    true
}

/// TODO(pg-port): c.h BUFFERALIGN
unsafe fn BUFFERALIGN(len: Size) -> Size {
    (len + 7) & !7
}

/// TODO(pg-port): c.h StaticAssertStmt
unsafe fn StaticAssertStmt(cond: bool, errmessage: &str) {}

/*
 * Reinitialize the dynamic shared memory segment for a parallel context such
 * that we could launch workers for it again.
 */
pub unsafe fn ReinitializeParallelDSM(pcxt: *mut ParallelContext) {
    let oldcontext: MemoryContext;
    let fps: *mut FixedParallelState;

    /* We might be running in a very short-lived memory context. */
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    /* Wait for any old workers to exit. */
    if (*pcxt).nworkers_launched > 0 {
        WaitForParallelWorkersToFinish(pcxt);
        WaitForParallelWorkersToExit(pcxt);
        (*pcxt).nworkers_launched = 0;
        if !(*pcxt).known_attached_workers.is_null() {
            pfree((*pcxt).known_attached_workers as *mut c_void);
            (*pcxt).known_attached_workers = null_mut();
            (*pcxt).nknown_attached_workers = 0;
        }
    }

    /* Reset a few bits of fixed parallel state to a clean state. */
    fps = shm_toc_lookup((*pcxt).toc, PARALLEL_KEY_FIXED, false) as *mut FixedParallelState;
    (*fps).last_xlog_end = 0;

    /* Recreate error queues (if they exist). */
    if (*pcxt).nworkers > 0 {
        let error_queue_space: *mut c_char;
        let mut i: c_int;

        error_queue_space =
            shm_toc_lookup((*pcxt).toc, PARALLEL_KEY_ERROR_QUEUE, false) as *mut c_char;
        i = 0;
        while i < (*pcxt).nworkers {
            let start: *mut c_char;
            let mq: *mut shm_mq;

            start = error_queue_space.add((i as usize) * PARALLEL_ERROR_QUEUE_SIZE);
            mq = shm_mq_create(start as *mut c_void, PARALLEL_ERROR_QUEUE_SIZE);
            shm_mq_set_receiver(mq, MyProc);
            (*(*pcxt).worker.add(i as usize)).error_mqh = shm_mq_attach(mq, (*pcxt).seg, null_mut());
            i += 1;
        }
    }

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);
}

/*
 * Reinitialize parallel workers for a parallel context such that we could
 * launch a different number of workers.  This is required for cases where
 * we need to reuse the same DSM segment, but the number of workers can
 * vary from run-to-run.
 */
pub unsafe fn ReinitializeParallelWorkers(pcxt: *mut ParallelContext, nworkers_to_launch: c_int) {
    /*
     * The number of workers that need to be launched must be less than the
     * number of workers with which the parallel context is initialized.  But
     * the caller might not know that InitializeParallelDSM reduced nworkers,
     * so just silently trim the request.
     */
    (*pcxt).nworkers_to_launch = Min((*pcxt).nworkers, nworkers_to_launch);
}

/*
 * Launch parallel workers.
 */
pub unsafe fn LaunchParallelWorkers(pcxt: *mut ParallelContext) {
    let oldcontext: MemoryContext;
    let mut worker: BackgroundWorker = core::mem::zeroed();
    let mut i: c_int;
    let mut any_registrations_failed: bool = false;

    /* Skip this if we have no workers. */
    if (*pcxt).nworkers == 0 || (*pcxt).nworkers_to_launch == 0 {
        return;
    }

    /* We need to be a lock group leader. */
    BecomeLockGroupLeader();

    /* If we do have workers, we'd better have a DSM segment. */
    Assert!(!(*pcxt).seg.is_null());

    /* We might be running in a short-lived memory context. */
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    /* Configure a worker. */
    // memset(&worker, 0, sizeof(worker)) -- handled by zeroed() above.
    snprintf_bgw_name(
        worker.bgw_name.as_mut_ptr(),
        BGW_MAXLEN,
        c"parallel worker for PID %d".as_ptr(),
        MyProcPid,
    );
    snprintf_bgw_type(worker.bgw_type.as_mut_ptr(), BGW_MAXLEN, c"parallel worker".as_ptr());
    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_CLASS_PARALLEL;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf_str(worker.bgw_library_name.as_mut_ptr(), c"postgres".as_ptr());
    sprintf_str(worker.bgw_function_name.as_mut_ptr(), c"ParallelWorkerMain".as_ptr());
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle((*pcxt).seg));
    worker.bgw_notify_pid = MyProcPid;

    /*
     * Start workers.
     *
     * The caller must be able to tolerate ending up with fewer workers than
     * expected, so there is no need to throw an error here if registration
     * fails.  It wouldn't help much anyway, because registering the worker in
     * no way guarantees that it will start up and initialize successfully.
     */
    i = 0;
    while i < (*pcxt).nworkers_to_launch {
        // memcpy(worker.bgw_extra, &i, sizeof(int));
        core::ptr::copy_nonoverlapping(
            &i as *const c_int as *const c_char,
            worker.bgw_extra.as_mut_ptr(),
            core::mem::size_of::<c_int>(),
        );
        if !any_registrations_failed
            && RegisterDynamicBackgroundWorker(
                &mut worker,
                &mut (*(*pcxt).worker.add(i as usize)).bgwhandle,
            )
        {
            shm_mq_set_handle(
                (*(*pcxt).worker.add(i as usize)).error_mqh,
                (*(*pcxt).worker.add(i as usize)).bgwhandle,
            );
            (*pcxt).nworkers_launched += 1;
        } else {
            /*
             * If we weren't able to register the worker, then we've bumped up
             * against the max_worker_processes limit, and future
             * registrations will probably fail too, so arrange to skip them.
             * But we still have to execute this code for the remaining slots
             * to make sure that we forget about the error queues we budgeted
             * for those workers.  Otherwise, we'll wait for them to start,
             * but they never will.
             */
            any_registrations_failed = true;
            (*(*pcxt).worker.add(i as usize)).bgwhandle = null_mut();
            shm_mq_detach((*(*pcxt).worker.add(i as usize)).error_mqh);
            (*(*pcxt).worker.add(i as usize)).error_mqh = null_mut();
        }
        i += 1;
    }

    /*
     * Now that nworkers_launched has taken its final value, we can initialize
     * known_attached_workers.
     */
    if (*pcxt).nworkers_launched > 0 {
        (*pcxt).known_attached_workers =
            palloc0(core::mem::size_of::<bool>() * (*pcxt).nworkers_launched as usize) as *mut bool;
        (*pcxt).nknown_attached_workers = 0;
    }

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);
}

/// TODO(pg-port): snprintf (port.h) -- specialized for the "parallel worker for
/// PID %d" name; writes into a fixed-size bgw_name buffer.
unsafe fn snprintf_bgw_name(dst: *mut c_char, count: usize, fmt: *const c_char, pid: c_int) {}

/// TODO(pg-port): snprintf (port.h) -- specialized for the constant "parallel
/// worker" type string.
unsafe fn snprintf_bgw_type(dst: *mut c_char, count: usize, s: *const c_char) {}

/// TODO(pg-port): sprintf (port.h) -- copies a constant string into a buffer.
unsafe fn sprintf_str(dst: *mut c_char, s: *const c_char) {
    strcpy(dst, s);
}

/*
 * Wait for all workers to attach to their error queues, and throw an error if
 * any worker fails to do this.
 *
 * Callers can assume that if this function returns successfully, then the
 * number of workers given by pcxt->nworkers_launched have initialized and
 * attached to their error queues.  Whether or not these workers are guaranteed
 * to still be running depends on what code the caller asked them to run;
 * this function does not guarantee that they have not exited.  However, it
 * does guarantee that any workers which exited must have done so cleanly and
 * after successfully performing the work with which they were tasked.
 *
 * If this function is not called, then some of the workers that were launched
 * may not have been started due to a fork() failure, or may have exited during
 * early startup prior to attaching to the error queue, so nworkers_launched
 * cannot be viewed as completely reliable.  It will never be less than the
 * number of workers which actually started, but it might be more.  Any workers
 * that failed to start will still be discovered by
 * WaitForParallelWorkersToFinish and an error will be thrown at that time,
 * provided that function is eventually reached.
 *
 * In general, the leader process should do as much work as possible before
 * calling this function.  fork() failures and other early-startup failures
 * are very uncommon, and having the leader sit idle when it could be doing
 * useful work is undesirable.  However, if the leader needs to wait for
 * all of its workers or for a specific worker, it may want to call this
 * function before doing so.  If not, it must make some other provision for
 * the failure-to-start case, lest it wait forever.  On the other hand, a
 * leader which never waits for a worker that might not be started yet, or
 * at least never does so prior to WaitForParallelWorkersToFinish(), need not
 * call this function at all.
 */
pub unsafe fn WaitForParallelWorkersToAttach(pcxt: *mut ParallelContext) {
    let mut i: c_int;

    /* Skip this if we have no launched workers. */
    if (*pcxt).nworkers_launched == 0 {
        return;
    }

    loop {
        /*
         * This will process any parallel messages that are pending and it may
         * also throw an error propagated from a worker.
         */
        CHECK_FOR_INTERRUPTS();

        i = 0;
        while i < (*pcxt).nworkers_launched {
            let status: BgwHandleStatus;
            let mq: *mut shm_mq;
            let rc: c_int;
            let mut pid: pid_t = 0;

            if *(*pcxt).known_attached_workers.add(i as usize) {
                i += 1;
                continue;
            }

            /*
             * If error_mqh is NULL, then the worker has already exited
             * cleanly.
             */
            if (*(*pcxt).worker.add(i as usize)).error_mqh.is_null() {
                *(*pcxt).known_attached_workers.add(i as usize) = true;
                (*pcxt).nknown_attached_workers += 1;
                i += 1;
                continue;
            }

            status = GetBackgroundWorkerPid((*(*pcxt).worker.add(i as usize)).bgwhandle, &mut pid);
            if status == BGWH_STARTED {
                /* Has the worker attached to the error queue? */
                mq = shm_mq_get_queue((*(*pcxt).worker.add(i as usize)).error_mqh);
                if !shm_mq_get_sender(mq).is_null() {
                    /* Yes, so it is known to be attached. */
                    *(*pcxt).known_attached_workers.add(i as usize) = true;
                    (*pcxt).nknown_attached_workers += 1;
                }
            } else if status == BGWH_STOPPED {
                /*
                 * If the worker stopped without attaching to the error queue,
                 * throw an error.
                 */
                mq = shm_mq_get_queue((*(*pcxt).worker.add(i as usize)).error_mqh);
                if shm_mq_get_sender(mq).is_null() {
                    // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    // C also: errhint("More details may be available in the server log.")
                    ereport!(ERROR, errmsg!("parallel worker failed to initialize"));
                }

                *(*pcxt).known_attached_workers.add(i as usize) = true;
                (*pcxt).nknown_attached_workers += 1;
            } else {
                /*
                 * Worker not yet started, so we must wait.  The postmaster
                 * will notify us if the worker's state changes.  Our latch
                 * might also get set for some other reason, but if so we'll
                 * just end up waiting for the same worker again.
                 */
                rc = WaitLatch(
                    MyLatch,
                    WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                    -1,
                    WAIT_EVENT_BGWORKER_STARTUP,
                );

                if rc & WL_LATCH_SET != 0 {
                    ResetLatch(MyLatch);
                }
            }
            i += 1;
        }

        /* If all workers are known to have started, we're done. */
        if (*pcxt).nknown_attached_workers >= (*pcxt).nworkers_launched {
            Assert!((*pcxt).nknown_attached_workers == (*pcxt).nworkers_launched);
            break;
        }
    }
}

/*
 * Wait for all workers to finish computing.
 *
 * Even if the parallel operation seems to have completed successfully, it's
 * important to call this function afterwards.  We must not miss any errors
 * the workers may have thrown during the parallel operation, or any that they
 * may yet throw while shutting down.
 *
 * Also, we want to update our notion of XactLastRecEnd based on worker
 * feedback.
 */
pub unsafe fn WaitForParallelWorkersToFinish(pcxt: *mut ParallelContext) {
    loop {
        let mut anyone_alive: bool = false;
        let mut nfinished: c_int = 0;
        let mut i: c_int;

        /*
         * This will process any parallel messages that are pending, which may
         * change the outcome of the loop that follows.  It may also throw an
         * error propagated from a worker.
         */
        CHECK_FOR_INTERRUPTS();

        i = 0;
        while i < (*pcxt).nworkers_launched {
            /*
             * If error_mqh is NULL, then the worker has already exited
             * cleanly.  If we have received a message through error_mqh from
             * the worker, we know it started up cleanly, and therefore we're
             * certain to be notified when it exits.
             */
            if (*(*pcxt).worker.add(i as usize)).error_mqh.is_null() {
                nfinished += 1;
            } else if *(*pcxt).known_attached_workers.add(i as usize) {
                anyone_alive = true;
                break;
            }
            i += 1;
        }

        if !anyone_alive {
            /* If all workers are known to have finished, we're done. */
            if nfinished >= (*pcxt).nworkers_launched {
                Assert!(nfinished == (*pcxt).nworkers_launched);
                break;
            }

            /*
             * We didn't detect any living workers, but not all workers are
             * known to have exited cleanly.  Either not all workers have
             * launched yet, or maybe some of them failed to start or
             * terminated abnormally.
             */
            i = 0;
            while i < (*pcxt).nworkers_launched {
                let mut pid: pid_t = 0;
                let mq: *mut shm_mq;

                /*
                 * If the worker is BGWH_NOT_YET_STARTED or BGWH_STARTED, we
                 * should just keep waiting.  If it is BGWH_STOPPED, then
                 * further investigation is needed.
                 */
                if (*(*pcxt).worker.add(i as usize)).error_mqh.is_null()
                    || (*(*pcxt).worker.add(i as usize)).bgwhandle.is_null()
                    || GetBackgroundWorkerPid(
                        (*(*pcxt).worker.add(i as usize)).bgwhandle,
                        &mut pid,
                    ) != BGWH_STOPPED
                {
                    i += 1;
                    continue;
                }

                /*
                 * Check whether the worker ended up stopped without ever
                 * attaching to the error queue.  If so, the postmaster was
                 * unable to fork the worker or it exited without initializing
                 * properly.  We must throw an error, since the caller may
                 * have been expecting the worker to do some work before
                 * exiting.
                 */
                mq = shm_mq_get_queue((*(*pcxt).worker.add(i as usize)).error_mqh);
                if shm_mq_get_sender(mq).is_null() {
                    // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    // C also: errhint("More details may be available in the server log.")
                    ereport!(ERROR, errmsg!("parallel worker failed to initialize"));
                }

                /*
                 * The worker is stopped, but is attached to the error queue.
                 * Unless there's a bug somewhere, this will only happen when
                 * the worker writes messages and terminates after the
                 * CHECK_FOR_INTERRUPTS() near the top of this function and
                 * before the call to GetBackgroundWorkerPid().  In that case,
                 * or latch should have been set as well and the right things
                 * will happen on the next pass through the loop.
                 */
                i += 1;
            }
        }

        WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            -1,
            WAIT_EVENT_PARALLEL_FINISH,
        );
        ResetLatch(MyLatch);
    }

    if !(*pcxt).toc.is_null() {
        let fps: *mut FixedParallelState;

        fps = shm_toc_lookup((*pcxt).toc, PARALLEL_KEY_FIXED, false) as *mut FixedParallelState;
        if (*fps).last_xlog_end > XactLastRecEnd {
            XactLastRecEnd = (*fps).last_xlog_end;
        }
    }
}

/*
 * Wait for all workers to exit.
 *
 * This function ensures that workers have been completely shutdown.  The
 * difference between WaitForParallelWorkersToFinish and this function is
 * that the former just ensures that last message sent by a worker backend is
 * received by the leader backend whereas this ensures the complete shutdown.
 */
unsafe fn WaitForParallelWorkersToExit(pcxt: *mut ParallelContext) {
    let mut i: c_int;

    /* Wait until the workers actually die. */
    i = 0;
    while i < (*pcxt).nworkers_launched {
        let status: BgwHandleStatus;

        if (*pcxt).worker.is_null() || (*(*pcxt).worker.add(i as usize)).bgwhandle.is_null() {
            i += 1;
            continue;
        }

        status = WaitForBackgroundWorkerShutdown((*(*pcxt).worker.add(i as usize)).bgwhandle);

        /*
         * If the postmaster kicked the bucket, we have no chance of cleaning
         * up safely -- we won't be able to tell when our workers are actually
         * dead.  This doesn't necessitate a PANIC since they will all abort
         * eventually, but we can't safely continue this session.
         */
        if status == BGWH_POSTMASTER_DIED {
            // C also: errcode(ERRCODE_ADMIN_SHUTDOWN)
            ereport!(FATAL, errmsg!("postmaster exited during a parallel transaction"));
        }

        /* Release memory. */
        pfree((*(*pcxt).worker.add(i as usize)).bgwhandle as *mut c_void);
        (*(*pcxt).worker.add(i as usize)).bgwhandle = null_mut();
        i += 1;
    }
}

/*
 * Destroy a parallel context.
 *
 * If expecting a clean exit, you should use WaitForParallelWorkersToFinish()
 * first, before calling this function.  When this function is invoked, any
 * remaining workers are forcibly killed; the dynamic shared memory segment
 * is unmapped; and we then wait (uninterruptibly) for the workers to exit.
 */
pub unsafe fn DestroyParallelContext(pcxt: *mut ParallelContext) {
    let mut i: c_int;

    /*
     * Be careful about order of operations here!  We remove the parallel
     * context from the list before we do anything else; otherwise, if an
     * error occurs during a subsequent step, we might try to nuke it again
     * from AtEOXact_Parallel or AtEOSubXact_Parallel.
     */
    dlist_delete(&mut (*pcxt).node);

    /* Kill each worker in turn, and forget their error queues. */
    if !(*pcxt).worker.is_null() {
        i = 0;
        while i < (*pcxt).nworkers_launched {
            if !(*(*pcxt).worker.add(i as usize)).error_mqh.is_null() {
                TerminateBackgroundWorker((*(*pcxt).worker.add(i as usize)).bgwhandle);

                shm_mq_detach((*(*pcxt).worker.add(i as usize)).error_mqh);
                (*(*pcxt).worker.add(i as usize)).error_mqh = null_mut();
            }
            i += 1;
        }
    }

    /*
     * If we have allocated a shared memory segment, detach it.  This will
     * implicitly detach the error queues, and any other shared memory queues,
     * stored there.
     */
    if !(*pcxt).seg.is_null() {
        dsm_detach((*pcxt).seg);
        (*pcxt).seg = null_mut();
    }

    /*
     * If this parallel context is actually in backend-private memory rather
     * than shared memory, free that memory instead.
     */
    if !(*pcxt).private_memory.is_null() {
        pfree((*pcxt).private_memory);
        (*pcxt).private_memory = null_mut();
    }

    /*
     * We can't finish transaction commit or abort until all of the workers
     * have exited.  This means, in particular, that we can't respond to
     * interrupts at this stage.
     */
    HOLD_INTERRUPTS();
    WaitForParallelWorkersToExit(pcxt);
    RESUME_INTERRUPTS();

    /* Free the worker array itself. */
    if !(*pcxt).worker.is_null() {
        pfree((*pcxt).worker as *mut c_void);
        (*pcxt).worker = null_mut();
    }

    /* Free memory. */
    pfree((*pcxt).library_name as *mut c_void);
    pfree((*pcxt).function_name as *mut c_void);
    pfree(pcxt as *mut c_void);
}

/*
 * Are there any parallel contexts currently active?
 */
pub unsafe fn ParallelContextActive() -> bool {
    !dlist_is_empty(&pcxt_list)
}

/*
 * Handle receipt of an interrupt indicating a parallel worker message.
 *
 * Note: this is called within a signal handler!  All we can do is set
 * a flag that will cause the next CHECK_FOR_INTERRUPTS() to invoke
 * ProcessParallelMessages().
 */
pub unsafe fn HandleParallelMessageInterrupt() {
    InterruptPending = true;
    ParallelMessagePending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

/*
 * Process any queued protocol messages received from parallel workers.
 */
pub unsafe fn ProcessParallelMessages() {
    let mut iter: dlist_iter = core::mem::zeroed();
    let oldcontext: MemoryContext;

    static mut hpm_context: MemoryContext = null_mut();

    /*
     * This is invoked from ProcessInterrupts(), and since some of the
     * functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
     * for recursive calls if more signals are received while this runs.  It's
     * unclear that recursive entry would be safe, and it doesn't seem useful
     * even if it is safe, so let's block interrupts until done.
     */
    HOLD_INTERRUPTS();

    /*
     * Moreover, CurrentMemoryContext might be pointing almost anywhere.  We
     * don't want to risk leaking data into long-lived contexts, so let's do
     * our work here in a private context that we can reset on each use.
     */
    if hpm_context.is_null() {
        /* first time through? */
        hpm_context = AllocSetContextCreate!(
            TopMemoryContext,
            c"ProcessParallelMessages".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    } else {
        MemoryContextReset(hpm_context);
    }

    oldcontext = MemoryContextSwitchTo(hpm_context);

    /* OK to process messages.  Reset the flag saying there are more to do. */
    ParallelMessagePending = false as sig_atomic_t;

    crate::dlist_foreach!(iter, &mut pcxt_list, {
        let pcxt: *mut ParallelContext;
        let mut i: c_int;

        pcxt = crate::dlist_container!(ParallelContext, node, iter.cur);
        if (*pcxt).worker.is_null() {
            continue;
        }

        i = 0;
        while i < (*pcxt).nworkers_launched {
            /*
             * Read as many messages as we can from each worker, but stop when
             * either (1) the worker's error queue goes away, which can happen
             * if we receive a Terminate message from the worker; or (2) no
             * more messages can be read from the worker without blocking.
             */
            while !(*(*pcxt).worker.add(i as usize)).error_mqh.is_null() {
                let res: shm_mq_result;
                let mut nbytes: Size = 0;
                let mut data: *mut c_void = null_mut();

                res = shm_mq_receive(
                    (*(*pcxt).worker.add(i as usize)).error_mqh,
                    &mut nbytes,
                    &mut data,
                    true,
                );
                if res == SHM_MQ_WOULD_BLOCK {
                    break;
                } else if res == SHM_MQ_SUCCESS {
                    let mut msg: StringInfoData = core::mem::zeroed();

                    initStringInfo(&mut msg);
                    appendBinaryStringInfo(&mut msg, data as *const c_char, nbytes as c_int);
                    ProcessParallelMessage(pcxt, i, &mut msg);
                    pfree(msg.data as *mut c_void);
                } else {
                    // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    ereport!(ERROR, errmsg!("lost connection to parallel worker"));
                }
            }
            i += 1;
        }
    });

    MemoryContextSwitchTo(oldcontext);

    /* Might as well clear the context on our way out */
    MemoryContextReset(hpm_context);

    RESUME_INTERRUPTS();
}

/*
 * Process a single protocol message received from a single parallel worker.
 */
unsafe fn ProcessParallelMessage(pcxt: *mut ParallelContext, i: c_int, msg: StringInfo) {
    let msgtype: c_char;

    if !(*pcxt).known_attached_workers.is_null()
        && !*(*pcxt).known_attached_workers.add(i as usize)
    {
        *(*pcxt).known_attached_workers.add(i as usize) = true;
        (*pcxt).nknown_attached_workers += 1;
    }

    msgtype = pq_getmsgbyte(msg);

    match msgtype {
        x if x == PqMsg_ErrorResponse || x == PqMsg_NoticeResponse => {
            let mut edata: ErrorData = core::mem::zeroed();
            let save_error_context_stack: *mut ErrorContextCallback;

            /* Parse ErrorResponse or NoticeResponse. */
            pq_parse_errornotice(msg, &mut edata);

            /* Death of a worker isn't enough justification for suicide. */
            edata.elevel = Min(edata.elevel, ERROR);

            /*
             * If desired, add a context line to show that this is a
             * message propagated from a parallel worker.  Otherwise, it
             * can sometimes be confusing to understand what actually
             * happened.  (We don't do this in DEBUG_PARALLEL_REGRESS mode
             * because it causes test-result instability depending on
             * whether a parallel worker is actually used or not.)
             */
            if debug_parallel_query != DEBUG_PARALLEL_REGRESS {
                if !edata.context.is_null() {
                    edata.context = psprintf(
                        c"%s\n%s".as_ptr(),
                        edata.context,
                        gettext(c"parallel worker".as_ptr()),
                    );
                } else {
                    edata.context = pstrdup(gettext(c"parallel worker".as_ptr()));
                }
            }

            /*
             * Context beyond that should use the error context callbacks
             * that were in effect when the ParallelContext was created,
             * not the current ones.
             */
            save_error_context_stack = error_context_stack;
            error_context_stack = (*pcxt).error_context_stack;

            /* Rethrow error or print notice. */
            ThrowErrorData(&mut edata);

            /* Not an error, so restore previous context stack. */
            error_context_stack = save_error_context_stack;
        }

        x if x == PqMsg_NotificationResponse => {
            /* Propagate NotifyResponse. */
            let pid: int32;
            let channel: *const c_char;
            let payload: *const c_char;

            pid = pq_getmsgint(msg, 4) as int32;
            channel = pq_getmsgrawstring(msg);
            payload = pq_getmsgrawstring(msg);
            pq_endmessage(msg);

            NotifyMyFrontEnd(channel, payload, pid);
        }

        x if x == PqMsg_Progress => {
            /*
             * Only incremental progress reporting is currently supported.
             * However, it's possible to add more fields to the message to
             * allow for handling of other backend progress APIs.
             */
            let index: c_int = pq_getmsgint(msg, 4) as c_int;
            let incr: int64 = pq_getmsgint64(msg);

            pq_getmsgend(msg);

            pgstat_progress_incr_param(index, incr);
        }

        x if x == PqMsg_Terminate => {
            shm_mq_detach((*(*pcxt).worker.add(i as usize)).error_mqh);
            (*(*pcxt).worker.add(i as usize)).error_mqh = null_mut();
        }

        _ => {
            elog!(
                ERROR,
                "unrecognized message type received from parallel worker: {} (message length {} bytes)",
                msgtype as u8 as char,
                (*msg).len
            );
        }
    }
}

/// TODO(pg-port): _ (gettext translation marker, c.h) -- pass-through.  The C
/// macro `_(x)` expands to `gettext(x)`; Rust forbids a function literally named
/// `_`, so it is rendered as `gettext` here and at its call sites.
unsafe fn gettext(msgid: *const c_char) -> *const c_char {
    msgid
}

/*
 * End-of-subtransaction cleanup for parallel contexts.
 *
 * Here we remove only parallel contexts initiated within the current
 * subtransaction.
 */
pub unsafe fn AtEOSubXact_Parallel(isCommit: bool, mySubId: SubTransactionId) {
    while !dlist_is_empty(&pcxt_list) {
        let pcxt: *mut ParallelContext;

        pcxt = crate::dlist_head_element!(ParallelContext, node, &mut pcxt_list);
        if (*pcxt).subid != mySubId {
            break;
        }
        if isCommit {
            elog!(WARNING, "leaked parallel context");
        }
        DestroyParallelContext(pcxt);
    }
}

/*
 * End-of-transaction cleanup for parallel contexts.
 *
 * We nuke all remaining parallel contexts.
 */
pub unsafe fn AtEOXact_Parallel(isCommit: bool) {
    while !dlist_is_empty(&pcxt_list) {
        let pcxt: *mut ParallelContext;

        pcxt = crate::dlist_head_element!(ParallelContext, node, &mut pcxt_list);
        if isCommit {
            elog!(WARNING, "leaked parallel context");
        }
        DestroyParallelContext(pcxt);
    }
}

/*
 * Main entrypoint for parallel workers.
 */
pub unsafe fn ParallelWorkerMain(main_arg: Datum) {
    let seg: *mut dsm_segment;
    let toc: *mut shm_toc;
    let fps: *mut FixedParallelState;
    let error_queue_space: *mut c_char;
    let mq: *mut shm_mq;
    let mqh: *mut shm_mq_handle;
    let libraryspace: *mut c_char;
    let entrypointstate: *mut c_char;
    let library_name: *mut c_char;
    let function_name: *mut c_char;
    let entrypt: parallel_worker_main_type;
    let gucspace: *mut c_char;
    let combocidspace: *mut c_char;
    let tsnapspace: *mut c_char;
    let asnapspace: *mut c_char;
    let tstatespace: *mut c_char;
    let pendingsyncsspace: *mut c_char;
    let reindexspace: *mut c_char;
    let relmapperspace: *mut c_char;
    let uncommittedenumsspace: *mut c_char;
    let clientconninfospace: *mut c_char;
    let session_dsm_handle_space: *mut c_char;
    let tsnapshot: Snapshot;
    let asnapshot: Snapshot;

    /* Set flag to indicate that we're initializing a parallel worker. */
    InitializingParallelWorker = true;

    /* Establish signal handlers. */
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* Determine and set our parallel worker number. */
    Assert!(ParallelWorkerNumber == -1);
    core::ptr::copy_nonoverlapping(
        (*MyBgworkerEntry).bgw_extra.as_ptr(),
        &mut ParallelWorkerNumber as *mut c_int as *mut c_char,
        core::mem::size_of::<c_int>(),
    );

    /* Set up a memory context to work in, just for cleanliness. */
    CurrentMemoryContext = AllocSetContextCreate!(
        TopMemoryContext,
        c"Parallel worker".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * Attach to the dynamic shared memory segment for the parallel query, and
     * find its table of contents.
     *
     * Note: at this point, we have not created any ResourceOwner in this
     * process.  This will result in our DSM mapping surviving until process
     * exit, which is fine.  If there were a ResourceOwner, it would acquire
     * ownership of the mapping, but we have no need for that.
     */
    seg = dsm_attach(DatumGetUInt32(main_arg));
    if seg.is_null() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(ERROR, errmsg!("could not map dynamic shared memory segment"));
    }
    toc = shm_toc_attach(PARALLEL_MAGIC as uint64, dsm_segment_address(seg));
    if toc.is_null() {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        ereport!(ERROR, errmsg!("invalid magic number in dynamic shared memory segment"));
    }

    /* Look up fixed parallel state. */
    fps = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false) as *mut FixedParallelState;
    MyFixedParallelState = fps;

    /* Arrange to signal the leader if we exit. */
    ParallelLeaderPid = (*fps).parallel_leader_pid;
    ParallelLeaderProcNumber = (*fps).parallel_leader_proc_number;
    before_shmem_exit(ParallelWorkerShutdown, PointerGetDatum(seg as *const c_void));

    /*
     * Now we can find and attach to the error queue provided for us.  That's
     * good, because until we do that, any errors that happen here will not be
     * reported back to the process that requested that this worker be
     * launched.
     */
    error_queue_space = shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false) as *mut c_char;
    mq = error_queue_space
        .add((ParallelWorkerNumber as usize) * PARALLEL_ERROR_QUEUE_SIZE)
        as *mut shm_mq;
    shm_mq_set_sender(mq, MyProc);
    mqh = shm_mq_attach(mq, seg, null_mut());
    pq_redirect_to_shm_mq(seg, mqh);
    pq_set_parallel_leader((*fps).parallel_leader_pid, (*fps).parallel_leader_proc_number);

    /*
     * Hooray! Primary initialization is complete.  Now, we need to set up our
     * backend-local state to match the original backend.
     */

    /*
     * Join locking group.  We must do this before anything that could try to
     * acquire a heavyweight lock, because any heavyweight locks acquired to
     * this point could block either directly against the parallel group
     * leader or against some process which in turn waits for a lock that
     * conflicts with the parallel group leader, causing an undetected
     * deadlock.  (If we can't join the lock group, the leader has gone away,
     * so just exit quietly.)
     */
    if !BecomeLockGroupMember((*fps).parallel_leader_pgproc, (*fps).parallel_leader_pid) {
        return;
    }

    /*
     * Restore transaction and statement start-time timestamps.  This must
     * happen before anything that would start a transaction, else asserts in
     * xact.c will fire.
     */
    SetParallelStartTimestamps((*fps).xact_ts, (*fps).stmt_ts);

    /*
     * Identify the entry point to be called.  In theory this could result in
     * loading an additional library, though most likely the entry point is in
     * the core backend or in a library we just loaded.
     */
    entrypointstate = shm_toc_lookup(toc, PARALLEL_KEY_ENTRYPOINT, false) as *mut c_char;
    library_name = entrypointstate;
    function_name = entrypointstate.add(strlen(library_name) + 1);

    entrypt = LookupParallelWorkerFunction(library_name, function_name);

    /*
     * Restore current session authorization and role id.  No verification
     * happens here, we just blindly adopt the leader's state.  Note that this
     * has to happen before InitPostgres, since InitializeSessionUserId will
     * not set these variables.
     */
    SetAuthenticatedUserId((*fps).authenticated_user_id);
    SetSessionAuthorization((*fps).session_user_id, (*fps).session_user_is_superuser);
    SetCurrentRoleId((*fps).outer_user_id, (*fps).role_is_superuser);

    /*
     * Restore database connection.  We skip connection authorization checks,
     * reasoning that (a) the leader checked these things when it started, and
     * (b) we do not want parallel mode to cause these failures, because that
     * would make use of parallel query plans not transparent to applications.
     */
    BackgroundWorkerInitializeConnectionByOid(
        (*fps).database_id,
        (*fps).authenticated_user_id,
        (BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ROLELOGINCHECK) as uint32,
    );

    /*
     * Set the client encoding to the database encoding, since that is what
     * the leader will expect.  (We're cheating a bit by not calling
     * PrepareClientEncoding first.  It's okay because this call will always
     * result in installing a no-op conversion.  No error should be possible,
     * but check anyway.)
     */
    if SetClientEncoding(GetDatabaseEncoding()) < 0 {
        elog!(ERROR, "SetClientEncoding({}) failed", GetDatabaseEncoding());
    }

    /*
     * Load libraries that were loaded by original backend.  We want to do
     * this before restoring GUCs, because the libraries might define custom
     * variables.
     */
    libraryspace = shm_toc_lookup(toc, PARALLEL_KEY_LIBRARY, false) as *mut c_char;
    StartTransactionCommand();
    RestoreLibraryState(libraryspace);
    CommitTransactionCommand();

    /* Crank up a transaction state appropriate to a parallel worker. */
    tstatespace = shm_toc_lookup(toc, PARALLEL_KEY_TRANSACTION_STATE, false) as *mut c_char;
    StartParallelWorkerTransaction(tstatespace);

    /*
     * Restore state that affects catalog access.  Ideally we'd do this even
     * before calling InitPostgres, but that has order-of-initialization
     * problems, and also the relmapper would get confused during the
     * CommitTransactionCommand call above.
     */
    pendingsyncsspace = shm_toc_lookup(toc, PARALLEL_KEY_PENDING_SYNCS, false) as *mut c_char;
    RestorePendingSyncs(pendingsyncsspace);
    relmapperspace = shm_toc_lookup(toc, PARALLEL_KEY_RELMAPPER_STATE, false) as *mut c_char;
    RestoreRelationMap(relmapperspace);
    reindexspace = shm_toc_lookup(toc, PARALLEL_KEY_REINDEX_STATE, false) as *mut c_char;
    RestoreReindexState(reindexspace as *mut c_void);
    combocidspace = shm_toc_lookup(toc, PARALLEL_KEY_COMBO_CID, false) as *mut c_char;
    RestoreComboCIDState(combocidspace);

    /* Attach to the per-session DSM segment and contained objects. */
    session_dsm_handle_space = shm_toc_lookup(toc, PARALLEL_KEY_SESSION_DSM, false) as *mut c_char;
    AttachSession(*(session_dsm_handle_space as *mut dsm_handle));

    /*
     * If the transaction isolation level is REPEATABLE READ or SERIALIZABLE,
     * the leader has serialized the transaction snapshot and we must restore
     * it. At lower isolation levels, there is no transaction-lifetime
     * snapshot, but we need TransactionXmin to get set to a value which is
     * less than or equal to the xmin of every snapshot that will be used by
     * this worker. The easiest way to accomplish that is to install the
     * active snapshot as the transaction snapshot. Code running in this
     * parallel worker might take new snapshots via GetTransactionSnapshot()
     * or GetLatestSnapshot(), but it shouldn't have any way of acquiring a
     * snapshot older than the active snapshot.
     */
    asnapspace = shm_toc_lookup(toc, PARALLEL_KEY_ACTIVE_SNAPSHOT, false) as *mut c_char;
    tsnapspace = shm_toc_lookup(toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT, true) as *mut c_char;
    asnapshot = RestoreSnapshot(asnapspace);
    tsnapshot = if !tsnapspace.is_null() {
        RestoreSnapshot(tsnapspace)
    } else {
        asnapshot
    };
    RestoreTransactionSnapshot(tsnapshot, (*fps).parallel_leader_pgproc);
    PushActiveSnapshot(asnapshot);

    /*
     * We've changed which tuples we can see, and must therefore invalidate
     * system caches.
     */
    InvalidateSystemCaches();

    /*
     * Restore GUC values from launching backend.  We can't do this earlier,
     * because GUC check hooks that do catalog lookups need to see the same
     * database state as the leader.  Also, the check hooks for
     * session_authorization and role assume we already set the correct role
     * OIDs.
     */
    gucspace = shm_toc_lookup(toc, PARALLEL_KEY_GUC, false) as *mut c_char;
    RestoreGUCState(gucspace as *mut c_void);

    /*
     * Restore current user ID and security context.  No verification happens
     * here, we just blindly adopt the leader's state.  We can't do this till
     * after restoring GUCs, else we'll get complaints about restoring
     * session_authorization and role.  (In effect, we're assuming that all
     * the restored values are okay to set, even if we are now inside a
     * restricted context.)
     */
    SetUserIdAndSecContext((*fps).current_user_id, (*fps).sec_context);

    /* Restore temp-namespace state to ensure search path matches leader's. */
    SetTempNamespaceState((*fps).temp_namespace_id, (*fps).temp_toast_namespace_id);

    /* Restore uncommitted enums. */
    uncommittedenumsspace =
        shm_toc_lookup(toc, PARALLEL_KEY_UNCOMMITTEDENUMS, false) as *mut c_char;
    RestoreUncommittedEnums(uncommittedenumsspace as *mut c_void);

    /* Restore the ClientConnectionInfo. */
    clientconninfospace = shm_toc_lookup(toc, PARALLEL_KEY_CLIENTCONNINFO, false) as *mut c_char;
    RestoreClientConnectionInfo(clientconninfospace);

    /*
     * Initialize SystemUser now that MyClientConnectionInfo is restored. Also
     * ensure that auth_method is actually valid, aka authn_id is not NULL.
     */
    if !MyClientConnectionInfo.authn_id.is_null() {
        InitializeSystemUser(
            MyClientConnectionInfo.authn_id,
            hba_authname(MyClientConnectionInfo.auth_method),
        );
    }

    /* Attach to the leader's serializable transaction, if SERIALIZABLE. */
    AttachSerializableXact((*fps).serializable_xact_handle);

    /*
     * We've initialized all of our state now; nothing should change
     * hereafter.
     */
    InitializingParallelWorker = false;
    EnterParallelMode();

    /*
     * Time to do the real work: invoke the caller-supplied code.
     */
    entrypt(seg, toc);

    /* Must exit parallel mode to pop active snapshot. */
    ExitParallelMode();

    /* Must pop active snapshot so snapmgr.c doesn't complain. */
    PopActiveSnapshot();

    /* Shut down the parallel-worker transaction. */
    EndParallelWorkerTransaction();

    /* Detach from the per-session DSM segment. */
    DetachSession();

    /* Report success. */
    pq_putmessage(PqMsg_Terminate, null(), 0);
}

/// TODO(pg-port): SIGTERM (system header)
pub const SIGTERM: c_int = 15;

/*
 * Update shared memory with the ending location of the last WAL record we
 * wrote, if it's greater than the value already stored there.
 */
pub unsafe fn ParallelWorkerReportLastRecEnd(last_xlog_end: XLogRecPtr) {
    let fps: *mut FixedParallelState = MyFixedParallelState;

    Assert!(!fps.is_null());
    SpinLockAcquire(&mut (*fps).mutex);
    if (*fps).last_xlog_end < last_xlog_end {
        (*fps).last_xlog_end = last_xlog_end;
    }
    SpinLockRelease(&mut (*fps).mutex);
}

/*
 * Make sure the leader tries to read from our error queue one more time.
 * This guards against the case where we exit uncleanly without sending an
 * ErrorResponse to the leader, for example because some code calls proc_exit
 * directly.
 *
 * Also explicitly detach from dsm segment so that subsystems using
 * on_dsm_detach() have a chance to send stats before the stats subsystem is
 * shut down as part of a before_shmem_exit() hook.
 *
 * One might think this could instead be solved by carefully ordering the
 * attaching to dsm segments, so that the pgstats segments get detached from
 * later than the parallel query one. That turns out to not work because the
 * stats hash might need to grow which can cause new segments to be allocated,
 * which then will be detached from earlier.
 */
unsafe fn ParallelWorkerShutdown(code: c_int, arg: Datum) {
    SendProcSignal(
        ParallelLeaderPid,
        PROCSIG_PARALLEL_MESSAGE,
        ParallelLeaderProcNumber,
    );

    dsm_detach(DatumGetPointer(arg) as *mut dsm_segment);
}

/*
 * Look up (and possibly load) a parallel worker entry point function.
 *
 * For functions contained in the core code, we use library name "postgres"
 * and consult the InternalParallelWorkers array.  External functions are
 * looked up, and loaded if necessary, using load_external_function().
 *
 * The point of this is to pass function names as strings across process
 * boundaries.  We can't pass actual function addresses because of the
 * possibility that the function has been loaded at a different address
 * in a different process.  This is obviously a hazard for functions in
 * loadable libraries, but it can happen even for functions in the core code
 * on platforms using EXEC_BACKEND (e.g., Windows).
 *
 * At some point it might be worthwhile to get rid of InternalParallelWorkers[]
 * in favor of applying load_external_function() for core functions too;
 * but that raises portability issues that are not worth addressing now.
 */
unsafe fn LookupParallelWorkerFunction(
    libraryname: *const c_char,
    funcname: *const c_char,
) -> parallel_worker_main_type {
    /*
     * If the function is to be loaded from postgres itself, search the
     * InternalParallelWorkers array.
     */
    if strcmp(libraryname, c"postgres".as_ptr()) == 0 {
        let mut i: usize;

        i = 0;
        while i < lengthof!(InternalParallelWorkers) {
            if strcmp(
                InternalParallelWorkers[i].fn_name.as_ptr() as *const c_char,
                funcname,
            ) == 0
            {
                return InternalParallelWorkers[i].fn_addr;
            }
            i += 1;
        }

        /* We can only reach this by programming error. */
        elog!(ERROR, "internal function \"{}\" not found", CStr::from_ptr(funcname).to_string_lossy());
    }

    /* Otherwise load from external library. */
    core::mem::transmute::<*mut c_void, parallel_worker_main_type>(load_external_function(
        libraryname,
        funcname,
        true,
        null_mut(),
    ))
}

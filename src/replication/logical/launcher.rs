/*-------------------------------------------------------------------------
 * launcher.rs
 *   PostgreSQL logical replication worker launcher process
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/replication/logical/launcher.c
 *
 * NOTES
 *   This module contains the logical replication worker launcher which
 *   uses the background worker infrastructure to start the logical
 *   replication workers for every enabled subscription.
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::c_char;

use crate::access::htup_details::{HeapTuple, GETSTRUCT};
use crate::access::relscan::TableScanDesc;
use crate::access::sdir::ForwardScanDirection;
use crate::access::table::table::{table_close, table_open};
use crate::access::table::tableam::table_beginscan_catalog;
use crate::access::transam::xlogdefs::InvalidXLogRecPtr;
use crate::c::{uint16, NameStr};
use crate::catalog::catalog_oids::SubscriptionRelationId;
use crate::catalog::pg_subscription::{Form_pg_subscription, FormData_pg_subscription};
use crate::lib::dshash::{
    dsa_area, dshash_create, dshash_attach, dshash_delete_key, dshash_find,
    dshash_find_or_insert, dshash_parameters, dshash_release_lock, dshash_table,
    dshash_table_handle, dshash_memcmp, dshash_memhash, dshash_memcpy,
    DSHASH_HANDLE_INVALID,
};
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, IsBinaryUpgrade, MyProcPid};
use crate::utils::init::globals::MyLatch;
use crate::nodes::pg_list::{lappend, lfirst, lnext, list_head, List, ListCell, NIL};
use crate::postmaster::bgworker_internals::pid_t;
use crate::postmaster::interrupt::{ConfigReloadPending, SignalHandlerForConfigReload};
use crate::replication::logicallauncher::pid_t as _pid_t; // same type; dedup at integration
use crate::replication::worker_internal::{
    am_leader_apply_worker, isParallelApplyWorker, isTablesyncWorker,
    pa_detach_all_error_mq, FileSet, InitializingApplyWorker, LogRepWorkerWalRcvConn,
    LogicalRepWorker, LogicalRepWorkerType, MyLogicalRepWorker, ParallelApplyWorkerInfo,
    ParallelApplyWorkerShared, WalReceiverConn, WORKERTYPE_APPLY, WORKERTYPE_PARALLEL_APPLY,
    WORKERTYPE_TABLESYNC, WORKERTYPE_UNKNOWN, dsm_handle,
};
use crate::storage::ipc::ipc::before_shmem_exit;
use crate::storage::ipc::shmem::{add_size, mul_size, ShmemInitStruct};
use crate::storage::ipc::shm_mq::{shm_mq_detach, shm_mq_handle, BackgroundWorkerHandle,
    BgwHandleStatus, GetBackgroundWorkerPid};
use crate::storage::lockdefs::AccessShareLock;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::utils::adt::timestamp::{
    GetCurrentTimestamp, TimestampDifferenceExceeds, TimestampDifferenceMilliseconds,
};
use crate::utils::memutils::MemoryContextDelete;

// --------------------------------------------------------------------------
// GUC variables (also declared as extern "C" in logicallauncher.rs header)
// --------------------------------------------------------------------------

/* max sleep time between cycles (3min) */
const DEFAULT_NAPTIME_PER_CYCLE: c_long = 180000;

/* GUC variables */
pub static mut max_logical_replication_workers: c_int = 4;
pub static mut max_sync_workers_per_subscription: c_int = 2;
pub static mut max_parallel_apply_workers_per_subscription: c_int = 2;

/* Pointer to this backend's LogicalRepWorker slot, if any. */
pub static mut MyLogicalRepWorker_launcher: *mut LogicalRepWorker = null_mut();

// --------------------------------------------------------------------------
// Shared-memory context structure
// --------------------------------------------------------------------------

#[repr(C)]
struct LogicalRepCtxStruct {
    /* Supervisor process. */
    launcher_pid: pid_t,

    /* Hash table holding last start times of subscriptions' apply workers. */
    last_start_dsa: dsa_handle_t,
    last_start_dsh: dshash_table_handle,

    /*
     * Background workers array.  Follows immediately after the fixed
     * header (flexible array member).
     */
    workers: [LogicalRepWorker; 0],
}

/* dsa_handle - the handle type for a DSA area (from storage/dsa.h). */
// TODO(pg-port): real dsa_handle lives in storage/dsa.h
type dsa_handle_t = uint32;
const DSA_HANDLE_INVALID_VAL: dsa_handle_t = 0;

static mut LogicalRepCtx: *mut LogicalRepCtxStruct = null_mut();

/* an entry in the last-start-times shared hash table */
#[repr(C)]
struct LauncherLastStartTimesEntry {
    subid: Oid,           /* OID of logrep subscription (hash key) */
    last_start_time: TimestampTz, /* last time its apply worker was started */
}

/* TimestampTz - from worker_internal re-export */
use crate::replication::worker_internal::TimestampTz;

/* parameters for the last-start-times shared hash table */
static dsh_params: dshash_parameters = dshash_parameters {
    key_size: core::mem::size_of::<Oid>(),
    entry_size: core::mem::size_of::<LauncherLastStartTimesEntry>(),
    compare_function: Some(dshash_memcmp),
    hash_function: Some(dshash_memhash),
    copy_function: Some(dshash_memcpy),
    tranche_id: LWTRANCHE_LAUNCHER_HASH,
};

static mut last_start_times_dsa: *mut dsa_area = null_mut();
static mut last_start_times: *mut dshash_table = null_mut();

static mut on_commit_launcher_wakeup: bool = false;

// --------------------------------------------------------------------------
// LWLock / tranche stubs
// TODO(pg-port): real LogicalRepWorkerLock lives in storage/lmgr/lwlock.c
// TODO(pg-port): real LWTRANCHE_LAUNCHER_HASH lives in storage/lmgr/lwlock.h
// TODO(pg-port): real LWTRANCHE_LAUNCHER_DSA lives in storage/lmgr/lwlock.h
// --------------------------------------------------------------------------

use crate::lib::dshash::{LWLock, LWLockAcquire, LWLockRelease,
    LWLockHeldByMe, LWLockHeldByMeInMode, LW_EXCLUSIVE, LW_SHARED};

/// LogicalRepWorkerLock - shared LWLock protecting LogicalRepCtx->workers.
// TODO(pg-port): real LogicalRepWorkerLock lives in storage/lmgr/lwlock.c
static mut LogicalRepWorkerLock_storage: LWLock = LWLock { _private: [] };
#[inline]
unsafe fn LogicalRepWorkerLock() -> *mut LWLock {
    &mut LogicalRepWorkerLock_storage as *mut LWLock
}

// TODO(pg-port): real tranche IDs live in storage/lmgr/lwlock.h
const LWTRANCHE_LAUNCHER_HASH: c_int = 0;
const LWTRANCHE_LAUNCHER_DSA: c_int = 0;

// --------------------------------------------------------------------------
// DSA stubs (dsa_create / dsa_attach / dsa_pin / dsa_pin_mapping /
//            dsa_get_handle).
// TODO(pg-port): real dsa_* functions live in utils/mmgr/dsa.c
// --------------------------------------------------------------------------

unsafe fn dsa_create(_tranche_id: c_int) -> *mut dsa_area {
    unimplemented!() // TODO(pg-port): real dsa_create lives in utils/mmgr/dsa.c
}
unsafe fn dsa_attach(_handle: dsa_handle_t) -> *mut dsa_area {
    unimplemented!() // TODO(pg-port): real dsa_attach lives in utils/mmgr/dsa.c
}
unsafe fn dsa_pin(_area: *mut dsa_area) {
    unimplemented!() // TODO(pg-port): real dsa_pin lives in utils/mmgr/dsa.c
}
unsafe fn dsa_pin_mapping(_area: *mut dsa_area) {
    unimplemented!() // TODO(pg-port): real dsa_pin_mapping lives in utils/mmgr/dsa.c
}
unsafe fn dsa_get_handle(_area: *mut dsa_area) -> dsa_handle_t {
    unimplemented!() // TODO(pg-port): real dsa_get_handle lives in utils/mmgr/dsa.c
}

// --------------------------------------------------------------------------
// WaitLatch / SetLatch / ResetLatch stubs
// TODO(pg-port): real WaitLatch lives in storage/ipc/latch.c
// --------------------------------------------------------------------------

use crate::storage::ipc::latch::{
    ResetLatch, SetLatch, WaitLatch, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT,
};

// TODO(pg-port): real WAIT_EVENT_* constants live in utils/activity/wait_event_names.h
const WAIT_EVENT_BGWORKER_STARTUP: uint32 = 0;
const WAIT_EVENT_BGWORKER_SHUTDOWN: uint32 = 0;
const WAIT_EVENT_LOGICAL_LAUNCHER_MAIN: uint32 = 0;

// --------------------------------------------------------------------------
// Signal / process-control stubs
// TODO(pg-port): real pqsignal lives in libpq/pqsignal.c
// TODO(pg-port): real die lives in tcop/tcopprot.c
// --------------------------------------------------------------------------

use crate::libpq::pqsignal::{pqsignal, SigHandler, SIGHUP, SIGUSR1, SIGUSR2, SIGTERM};
use crate::tcop::tcopprot::die;

// TODO(pg-port): real kill() lives in libc / port/port_api.c
unsafe fn kill(pid: pid_t, sig: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): real kill lives in libc
}

// --------------------------------------------------------------------------
// Background worker registration stubs
// TODO(pg-port): real BackgroundWorker types live in postmaster/bgworker.h
// --------------------------------------------------------------------------

// BGW_MAXLEN / MAXPGPATH
use crate::pg_config_manual::MAXPGPATH;
const BGW_MAXLEN: usize = 96;

pub type BgwFlags = c_int;
pub const BGWORKER_SHMEM_ACCESS: BgwFlags = 1 << 0;
pub const BGWORKER_BACKEND_DATABASE_CONNECTION: BgwFlags = 1 << 1;

pub type BgWorkerStartTime = c_int;
pub const BgWorkerStart_RecoveryFinished: BgWorkerStartTime = 2;

pub const BGW_NEVER_RESTART: c_int = -1;

/* DSM_HANDLE_INVALID - from storage/ipc/dsm_impl.h */
// Re-use the definition from shm_mq where it was already stubbed; we just
// need the same sentinel value (0) here.
// TODO(pg-port): canonical home is storage/dsm_impl.h
const DSM_HANDLE_INVALID: dsm_handle = 0;

/* max_active_replication_origins - from replication/origin.h */
// TODO(pg-port): real max_active_replication_origins lives in replication/origin.c
static mut max_active_replication_origins: c_int = 0;

/* wal_receiver_timeout / wal_retrieve_retry_interval - GUCs from recovery.c */
// TODO(pg-port): real wal_receiver_timeout lives in recovery/walreceiver.c
static mut wal_receiver_timeout: c_int = 60000;
// TODO(pg-port): real wal_retrieve_retry_interval lives in recovery/walreceiver.c
static mut wal_retrieve_retry_interval: c_int = 5000;

/* MyProc - storage/lmgr/proc.h */
// TODO(pg-port): real MyProc lives in storage/lmgr/proc.c
use crate::replication::worker_internal::PGPROC;
static mut MyProc: *mut PGPROC = null_mut();

/*
 * A BackgroundWorker descriptor - mirrors the C struct exactly enough for
 * RegisterDynamicBackgroundWorker.
 */
#[repr(C)]
pub struct BackgroundWorker {
    pub bgw_name: [c_char; BGW_MAXLEN],
    pub bgw_type: [c_char; BGW_MAXLEN],
    pub bgw_flags: BgwFlags,
    pub bgw_start_time: BgWorkerStartTime,
    pub bgw_restart_time: c_int,
    pub bgw_library_name: [c_char; MAXPGPATH],
    pub bgw_function_name: [c_char; BGW_MAXLEN],
    pub bgw_main_arg: Datum,
    pub bgw_extra: [c_char; BGW_MAXLEN],
    pub bgw_notify_pid: pid_t,
}

// TODO(pg-port): real RegisterBackgroundWorker lives in postmaster/bgworker.c
unsafe fn RegisterBackgroundWorker(_worker: *mut BackgroundWorker) {
    unimplemented!() // TODO(pg-port): real RegisterBackgroundWorker lives in postmaster/bgworker.c
}

// TODO(pg-port): real RegisterDynamicBackgroundWorker lives in postmaster/bgworker.c
unsafe fn RegisterDynamicBackgroundWorker(
    _worker: *mut BackgroundWorker,
    _handle: *mut *mut BackgroundWorkerHandle,
) -> bool {
    unimplemented!() // TODO(pg-port): real RegisterDynamicBackgroundWorker lives in postmaster/bgworker.c
}

// TODO(pg-port): real BackgroundWorkerUnblockSignals lives in postmaster/bgworker.c
unsafe fn BackgroundWorkerUnblockSignals() {
    unimplemented!() // TODO(pg-port): real BackgroundWorkerUnblockSignals lives in postmaster/bgworker.c
}

// TODO(pg-port): real BackgroundWorkerInitializeConnection lives in postmaster/bgworker.c
unsafe fn BackgroundWorkerInitializeConnection(
    _dbname: *const c_char,
    _username: *const c_char,
    _flags: uint32,
) {
    unimplemented!() // TODO(pg-port): real BackgroundWorkerInitializeConnection lives in postmaster/bgworker.c
}

// --------------------------------------------------------------------------
// Transaction command stubs
// TODO(pg-port): real StartTransactionCommand lives in access/transam/xact.c
// TODO(pg-port): real CommitTransactionCommand lives in access/transam/xact.c
// --------------------------------------------------------------------------

unsafe fn StartTransactionCommand() {
    unimplemented!() // TODO(pg-port): real StartTransactionCommand lives in access/transam/xact.c
}

unsafe fn CommitTransactionCommand() {
    unimplemented!() // TODO(pg-port): real CommitTransactionCommand lives in access/transam/xact.c
}

// --------------------------------------------------------------------------
// Lock management stubs
// TODO(pg-port): real LockReleaseAll lives in storage/lmgr/lock.c
// TODO(pg-port): real DEFAULT_LOCKMETHOD lives in storage/lmgr/lock.h
// --------------------------------------------------------------------------

// TODO(pg-port): real DEFAULT_LOCKMETHOD lives in storage/lmgr/lock.h
const DEFAULT_LOCKMETHOD: c_int = 1;

// TODO(pg-port): real LockReleaseAll lives in storage/lmgr/lock.c
unsafe fn LockReleaseAll(_lock_method: c_int, _all_levels: bool) {
    unimplemented!() // TODO(pg-port): real LockReleaseAll lives in storage/lmgr/lock.c
}

// --------------------------------------------------------------------------
// Process-array stubs
// TODO(pg-port): real IsBackendPid lives in storage/lmgr/procarray.c
// --------------------------------------------------------------------------

// TODO(pg-port): real IsBackendPid lives in storage/lmgr/procarray.c
unsafe fn IsBackendPid(_pid: pid_t) -> bool {
    unimplemented!() // TODO(pg-port): real IsBackendPid lives in storage/lmgr/procarray.c
}

// --------------------------------------------------------------------------
// pgstat stub
// TODO(pg-port): real walrcv_disconnect lives in replication/walreceiver.c
// --------------------------------------------------------------------------

// TODO(pg-port): real walrcv_disconnect lives in replication/walreceiver.c
unsafe fn walrcv_disconnect(_conn: *mut WalReceiverConn) {
    unimplemented!() // TODO(pg-port): real walrcv_disconnect lives in replication/walreceiver.c
}

// --------------------------------------------------------------------------
// FileSet stub
// TODO(pg-port): real FileSetDeleteAll lives in storage/file/fileset.c
// --------------------------------------------------------------------------

use crate::storage::file::fileset::FileSetDeleteAll;

// --------------------------------------------------------------------------
// Config-reload stub
// TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
// --------------------------------------------------------------------------

use crate::utils::misc::guc_funcs::PGC_SIGHUP;

// TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
unsafe fn ProcessConfigFile(_context: c_int) {
    unimplemented!() // TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
}

// --------------------------------------------------------------------------
// pg_stat_get_subscription helper types
// Mirrors logicalfuncs.rs and slotfuncs.rs local stub pattern.
// --------------------------------------------------------------------------

use crate::postgres::Datum;
use crate::utils::fmgr::{FunctionCallInfo, FunctionCallInfoBaseData};
use crate::nodes::execnodes::ReturnSetInfo;
use crate::access::common::tupdesc::TupleDesc;
// TODO(pg-port): real Tuplestorestate lives in utils/sort/tuplestore.c
pub type Tuplestorestate = core::ffi::c_void;

// TODO(pg-port): real InitMaterializedSRF lives in funcapi.c
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO(pg-port): real InitMaterializedSRF lives in funcapi.c
}

// TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tupdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): real tuplestore_putvalues lives in utils/sort/tuplestore.c
}

// TODO(pg-port): real TimestampTzGetDatum lives in utils/adt/timestamp.c
unsafe fn TimestampTzGetDatum(_ts: TimestampTz) -> Datum {
    unimplemented!() // TODO(pg-port): real TimestampTzGetDatum lives in utils/adt/timestamp.c
}

// LSNGetDatum re-export
use crate::utils::adt::pg_lsn::LSNGetDatum;

// CStringGetTextDatum re-export
use crate::utils::builtins::CStringGetTextDatum;

// ObjectIdGetDatum / Int32GetDatum re-export
use crate::postgres::{Int32GetDatum, ObjectIdGetDatum};

// XLogRecPtrIsInvalid re-export
use crate::access::transam::xlogdefs::XLogRecPtrIsInvalid;

// Subscription struct from worker_internal (opaque c_void alias)
use crate::replication::worker_internal::Subscription;

/* InvalidPid from miscadmin.rs */
use crate::miscadmin::InvalidPid;

/* heap_getnext re-export */
// TODO(pg-port): real heap_getnext lives in access/heap/heapam.c (unwired).
unsafe fn heap_getnext(_scan: *mut c_void, _dir: c_int) -> *mut crate::access::htup_details::HeapTupleData { core::ptr::null_mut() }

/* HeapTupleIsValid re-export */
use crate::access::htup_details::HeapTupleIsValid;

/* table_endscan - inline wrapper (no single canonical home yet) */
// TODO(pg-port): real table_endscan lives in access/tableam.h / tableam.c
unsafe fn table_endscan(scan: TableScanDesc) {
    unimplemented!() // TODO(pg-port): real table_endscan lives in access/tableam.h
}

/* snprintf via libc */
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn memset(dest: *mut c_void, c: c_int, n: Size) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

/* Relation type alias */
use crate::utils::rel::Relation;

// TIMESTAMP_NOBEGIN macro translated to inline call
use crate::utils::adt::date::TIMESTAMP_NOBEGIN;

// --------------------------------------------------------------------------
// Helpers to reach into LogicalRepCtx->workers (flexible array member).
// --------------------------------------------------------------------------

/// Return a raw pointer to the i-th worker slot.
#[inline]
unsafe fn worker_slot(i: c_int) -> *mut LogicalRepWorker {
    let base = LogicalRepCtx as *mut u8;
    let off = core::mem::size_of::<LogicalRepCtxStruct>();
    base.add(off)
        .add(i as usize * core::mem::size_of::<LogicalRepWorker>())
        as *mut LogicalRepWorker
}

// --------------------------------------------------------------------------
// get_subscription_list
// --------------------------------------------------------------------------

/*
 * Load the list of subscriptions.
 *
 * Only the fields interesting for worker start/stop functions are filled for
 * each subscription.
 */
unsafe fn get_subscription_list() -> *mut List {
    let mut res: *mut List = NIL;
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tup: HeapTuple;
    let resultcxt: MemoryContext;

    /* This is the context that we will allocate our output data in */
    resultcxt = CurrentMemoryContext;

    /*
     * Start a transaction so we can access pg_subscription.
     */
    StartTransactionCommand();

    rel = table_open(SubscriptionRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 0, null_mut());

    loop {
        tup = heap_getnext(scan as *mut c_void, ForwardScanDirection as c_int);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let subform: Form_pg_subscription = GETSTRUCT(tup) as Form_pg_subscription;
        let sub: *mut Subscription;
        let oldcxt: MemoryContext;

        /*
         * Allocate our results in the caller's context, not the
         * transaction's. We do this inside the loop, and restore the original
         * context at the end, so that leaky things like heap_getnext() are
         * not called in a potentially long-lived context.
         */
        oldcxt = MemoryContextSwitchTo(resultcxt);

        sub = palloc0(core::mem::size_of::<Subscription_local>()) as *mut Subscription;
        let sub_l = sub as *mut Subscription_local;
        (*sub_l).oid = (*subform).oid;
        (*sub_l).dbid = (*subform).subdbid;
        (*sub_l).owner = (*subform).subowner;
        (*sub_l).enabled = (*subform).subenabled;
        (*sub_l).name = pstrdup((*subform).subname.data.as_ptr());

        res = lappend(res, sub as *mut c_void);
        MemoryContextSwitchTo(oldcxt);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    CommitTransactionCommand();

    res
}

/*
 * Local struct mirroring the Subscription fields we actually read.
 * The real Subscription lives in catalog/pg_subscription.h; until that
 * module lands, we use this local layout.
 * TODO(pg-port): real Subscription lives in replication/worker_internal.h
 *               (resolved to actual catalog struct at integration time).
 */
#[repr(C)]
struct Subscription_local {
    oid: Oid,
    dbid: Oid,
    owner: Oid,
    enabled: bool,
    name: *mut c_char,
}

// --------------------------------------------------------------------------
// WaitForReplicationWorkerAttach
// --------------------------------------------------------------------------

/*
 * Wait for a background worker to start up and attach to the shmem context.
 *
 * This is only needed for cleaning up the shared memory in case the worker
 * fails to attach.
 *
 * Returns whether the attach was successful.
 */
unsafe fn WaitForReplicationWorkerAttach(
    worker: *mut LogicalRepWorker,
    generation: uint16,
    handle: *mut BackgroundWorkerHandle,
) -> bool {
    let mut result: bool = false;
    let mut dropped_latch: bool = false;

    loop {
        let status: BgwHandleStatus;
        let mut pid: pid_t = 0;
        let rc: c_int;

        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

        /* Worker either died or has started. Return false if died. */
        if !(*worker).in_use || !(*worker).proc.is_null() {
            result = (*worker).in_use;
            LWLockRelease(LogicalRepWorkerLock());
            break;
        }

        LWLockRelease(LogicalRepWorkerLock());

        /* Check if worker has died before attaching, and clean up after it. */
        status = GetBackgroundWorkerPid(handle, &mut pid);

        if status == BgwHandleStatus::BGWH_STOPPED {
            LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);
            /* Ensure that this was indeed the worker we waited for. */
            if generation == (*worker).generation {
                logicalrep_worker_cleanup(worker);
            }
            LWLockRelease(LogicalRepWorkerLock());
            break; /* result is already false */
        }

        /*
         * We need timeout because we generally don't get notified via latch
         * about the worker attach.  But we don't expect to have to wait long.
         */
        rc = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_STARTUP,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
            CHECK_FOR_INTERRUPTS();
            dropped_latch = true;
        }
    }

    /*
     * If we had to clear a latch event in order to wait, be sure to restore
     * it before exiting.  Otherwise caller may miss events.
     */
    if dropped_latch {
        SetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
    }

    result
}

// --------------------------------------------------------------------------
// logicalrep_worker_find / logicalrep_workers_find
// --------------------------------------------------------------------------

/*
 * Walks the workers array and searches for one that matches given
 * subscription id and relid.
 *
 * We are only interested in the leader apply worker or table sync worker.
 */
pub unsafe fn logicalrep_worker_find(
    subid: Oid,
    relid: Oid,
    only_running: bool,
) -> *mut LogicalRepWorker {
    let mut res: *mut LogicalRepWorker = null_mut();

    Assert!(LWLockHeldByMe(LogicalRepWorkerLock()));

    /* Search for attached worker for a given subscription id. */
    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        let w = worker_slot(i);

        /* Skip parallel apply workers. */
        if isParallelApplyWorker(w) {
            i += 1;
            continue;
        }

        if (*w).in_use
            && (*w).subid == subid
            && (*w).relid == relid
            && (!only_running || !(*w).proc.is_null())
        {
            res = w;
            break;
        }
        i += 1;
    }

    res
}

/*
 * Similar to logicalrep_worker_find(), but returns a list of all workers for
 * the subscription, instead of just one.
 */
pub unsafe fn logicalrep_workers_find(
    subid: Oid,
    only_running: bool,
    acquire_lock: bool,
) -> *mut List {
    let mut res: *mut List = NIL;

    if acquire_lock {
        LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);
    }

    Assert!(LWLockHeldByMe(LogicalRepWorkerLock()));

    /* Search for attached worker for a given subscription id. */
    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        let w = worker_slot(i);

        if (*w).in_use && (*w).subid == subid && (!only_running || !(*w).proc.is_null()) {
            res = lappend(res, w as *mut c_void);
        }
        i += 1;
    }

    if acquire_lock {
        LWLockRelease(LogicalRepWorkerLock());
    }

    res
}

// --------------------------------------------------------------------------
// logicalrep_worker_launch
// --------------------------------------------------------------------------

/*
 * Start new logical replication background worker, if possible.
 *
 * Returns true on success, false on failure.
 */
pub unsafe fn logicalrep_worker_launch(
    wtype: LogicalRepWorkerType,
    dbid: Oid,
    subid: Oid,
    subname: *const c_char,
    userid: Oid,
    relid: Oid,
    subworker_dsm: dsm_handle,
) -> bool {
    let mut bgw: BackgroundWorker = core::mem::zeroed();
    let mut bgw_handle: *mut BackgroundWorkerHandle = null_mut();
    let generation: uint16;
    let mut slot: c_int = 0;
    let mut worker: *mut LogicalRepWorker = null_mut();
    let mut nsyncworkers: c_int;
    let nparallelapplyworkers: c_int;
    let mut now: TimestampTz;
    let is_tablesync_worker = wtype == WORKERTYPE_TABLESYNC;
    let is_parallel_apply_worker = wtype == WORKERTYPE_PARALLEL_APPLY;

    /*----------
     * Sanity checks:
     * - must be valid worker type
     * - tablesync workers are only ones to have relid
     * - parallel apply worker is the only kind of subworker
     */
    Assert!(wtype != WORKERTYPE_UNKNOWN);
    Assert!(is_tablesync_worker == OidIsValid(relid));
    Assert!(is_parallel_apply_worker == (subworker_dsm != DSM_HANDLE_INVALID));

    ereport!(
        DEBUG1,
        errmsg!(
            "starting logical replication worker for subscription \"{}\"",
            core::ffi::CStr::from_ptr(subname).to_string_lossy()
        )
    );

    /* Report this after the initial starting message for consistency. */
    if max_active_replication_origins == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot start logical replication workers when \"max_active_replication_origins\" is 0"
            )
        );
    }

    /*
     * We need to do the modification of the shared memory under lock so that
     * we have consistent view.
     */
    LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);

    /* retry label - simulated via loop */
    'retry: loop {
        /* Find unused worker slot. */
        let mut i: c_int = 0;
        worker = null_mut();
        while i < max_logical_replication_workers {
            let w = worker_slot(i);
            if !(*w).in_use {
                worker = w;
                slot = i;
                break;
            }
            i += 1;
        }

        nsyncworkers = logicalrep_sync_worker_count(subid);

        now = GetCurrentTimestamp();

        /*
         * If we didn't find a free slot, try to do garbage collection.  The
         * reason we do this is because if some worker failed to start up and its
         * parent has crashed while waiting, the in_use state was never cleared.
         */
        if worker.is_null() || nsyncworkers >= max_sync_workers_per_subscription {
            let mut did_cleanup = false;

            let mut i: c_int = 0;
            while i < max_logical_replication_workers {
                let w = worker_slot(i);

                /*
                 * If the worker was marked in use but didn't manage to attach in
                 * time, clean it up.
                 */
                if (*w).in_use
                    && (*w).proc.is_null()
                    && TimestampDifferenceExceeds((*w).launch_time, now, wal_receiver_timeout)
                {
                    elog!(
                        WARNING,
                        "logical replication worker for subscription {} took too long to start; canceled",
                        (*w).subid
                    );

                    logicalrep_worker_cleanup(w);
                    did_cleanup = true;
                }
                i += 1;
            }

            if did_cleanup {
                continue 'retry;
            }
        }

        break;
    }

    /*
     * We don't allow to invoke more sync workers once we have reached the
     * sync worker limit per subscription. So, just return silently as we
     * might get here because of an otherwise harmless race condition.
     */
    if is_tablesync_worker && nsyncworkers >= max_sync_workers_per_subscription {
        LWLockRelease(LogicalRepWorkerLock());
        return false;
    }

    nparallelapplyworkers = logicalrep_pa_worker_count(subid);

    /*
     * Return false if the number of parallel apply workers reached the limit
     * per subscription.
     */
    if is_parallel_apply_worker
        && nparallelapplyworkers >= max_parallel_apply_workers_per_subscription
    {
        LWLockRelease(LogicalRepWorkerLock());
        return false;
    }

    /*
     * However if there are no more free worker slots, inform user about it
     * before exiting.
     */
    if worker.is_null() {
        LWLockRelease(LogicalRepWorkerLock());
        ereport!(
            WARNING,
            errmsg!("out of logical replication worker slots")
        );
        return false;
    }

    /* Prepare the worker slot. */
    (*worker).type_ = wtype;
    (*worker).launch_time = now;
    (*worker).in_use = true;
    (*worker).generation = (*worker).generation.wrapping_add(1);
    (*worker).proc = null_mut();
    (*worker).dbid = dbid;
    (*worker).userid = userid;
    (*worker).subid = subid;
    (*worker).relid = relid;
    (*worker).relstate = b'?' as c_char; /* SUBREL_STATE_UNKNOWN */
    (*worker).relstate_lsn = InvalidXLogRecPtr;
    (*worker).stream_fileset = null_mut();
    (*worker).leader_pid = if is_parallel_apply_worker { MyProcPid } else { InvalidPid };
    (*worker).parallel_apply = is_parallel_apply_worker;
    (*worker).last_lsn = InvalidXLogRecPtr;
    TIMESTAMP_NOBEGIN(&mut (*worker).last_send_time);
    TIMESTAMP_NOBEGIN(&mut (*worker).last_recv_time);
    (*worker).reply_lsn = InvalidXLogRecPtr;
    TIMESTAMP_NOBEGIN(&mut (*worker).reply_time);

    /* Before releasing lock, remember generation for future identification. */
    generation = (*worker).generation;

    LWLockRelease(LogicalRepWorkerLock());

    /* Register the new dynamic worker. */
    memset(&mut bgw as *mut BackgroundWorker as *mut c_void, 0, core::mem::size_of::<BackgroundWorker>());
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(
        bgw.bgw_library_name.as_mut_ptr(),
        MAXPGPATH,
        b"postgres\0".as_ptr() as *const c_char,
    );

    match (*worker).type_ {
        WORKERTYPE_APPLY => {
            snprintf(
                bgw.bgw_function_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"ApplyWorkerMain\0".as_ptr() as *const c_char,
            );
            snprintf(
                bgw.bgw_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication apply worker for subscription %u\0".as_ptr()
                    as *const c_char,
                subid,
            );
            snprintf(
                bgw.bgw_type.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication apply worker\0".as_ptr() as *const c_char,
            );
        }
        WORKERTYPE_PARALLEL_APPLY => {
            snprintf(
                bgw.bgw_function_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"ParallelApplyWorkerMain\0".as_ptr() as *const c_char,
            );
            snprintf(
                bgw.bgw_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication parallel apply worker for subscription %u\0".as_ptr()
                    as *const c_char,
                subid,
            );
            snprintf(
                bgw.bgw_type.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication parallel worker\0".as_ptr() as *const c_char,
            );
            memcpy(
                bgw.bgw_extra.as_mut_ptr() as *mut c_void,
                &subworker_dsm as *const dsm_handle as *const c_void,
                core::mem::size_of::<dsm_handle>(),
            );
        }
        WORKERTYPE_TABLESYNC => {
            snprintf(
                bgw.bgw_function_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"TablesyncWorkerMain\0".as_ptr() as *const c_char,
            );
            snprintf(
                bgw.bgw_name.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication tablesync worker for subscription %u sync %u\0".as_ptr()
                    as *const c_char,
                subid,
                relid,
            );
            snprintf(
                bgw.bgw_type.as_mut_ptr(),
                BGW_MAXLEN,
                b"logical replication tablesync worker\0".as_ptr() as *const c_char,
            );
        }
        _ => {
            /* Should never happen. */
            elog!(ERROR, "unknown worker type");
            unreachable!();
        }
    }

    bgw.bgw_restart_time = BGW_NEVER_RESTART;
    bgw.bgw_notify_pid = MyProcPid;
    bgw.bgw_main_arg = Int32GetDatum(slot);

    if !RegisterDynamicBackgroundWorker(&mut bgw, &mut bgw_handle) {
        /* Failed to start worker, so clean up the worker slot. */
        LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);
        Assert!(generation == (*worker).generation);
        logicalrep_worker_cleanup(worker);
        LWLockRelease(LogicalRepWorkerLock());

        ereport!(WARNING, errmsg!("out of background worker slots"));
        return false;
    }

    /* Now wait until it attaches. */
    WaitForReplicationWorkerAttach(worker, generation, bgw_handle)
}

// --------------------------------------------------------------------------
// logicalrep_worker_stop_internal
// --------------------------------------------------------------------------

/*
 * Internal function to stop the worker and wait until it detaches from the
 * slot.
 */
unsafe fn logicalrep_worker_stop_internal(worker: *mut LogicalRepWorker, signo: c_int) {
    let generation: uint16;

    Assert!(LWLockHeldByMeInMode(LogicalRepWorkerLock(), LW_SHARED));

    /*
     * Remember which generation was our worker so we can check if what we see
     * is still the same one.
     */
    generation = (*worker).generation;

    /*
     * If we found a worker but it does not have proc set then it is still
     * starting up; wait for it to finish starting and then kill it.
     */
    while (*worker).in_use && (*worker).proc.is_null() {
        let rc: c_int;

        LWLockRelease(LogicalRepWorkerLock());

        /* Wait a bit --- we don't expect to have to wait long. */
        rc = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_STARTUP,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
            CHECK_FOR_INTERRUPTS();
        }

        /* Recheck worker status. */
        LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

        /*
         * Check whether the worker slot is no longer used, which would mean
         * that the worker has exited, or whether the worker generation is
         * different, meaning that a different worker has taken the slot.
         */
        if !(*worker).in_use || (*worker).generation != generation {
            return;
        }

        /* Worker has assigned proc, so it has started. */
        if !(*worker).proc.is_null() {
            break;
        }
    }

    /* Now terminate the worker ... */
    let pid = (*((*worker).proc as *mut ProcPidOnly)).pid;
    kill(pid, signo);

    /* ... and wait for it to die. */
    loop {
        let rc: c_int;

        /* is it gone? */
        if (*worker).proc.is_null() || (*worker).generation != generation {
            break;
        }

        LWLockRelease(LogicalRepWorkerLock());

        /* Wait a bit --- we don't expect to have to wait long. */
        rc = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
            CHECK_FOR_INTERRUPTS();
        }

        LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);
    }
}

/*
 * Minimal overlay so we can read proc->pid without the full PGPROC definition.
 * TODO(pg-port): replace with real PGPROC::pid once storage/lmgr/proc.h lands.
 */
#[repr(C)]
struct ProcPidOnly {
    pid: pid_t,
}

// --------------------------------------------------------------------------
// logicalrep_worker_stop
// --------------------------------------------------------------------------

/*
 * Stop the logical replication worker for subid/relid, if any.
 */
pub unsafe fn logicalrep_worker_stop(subid: Oid, relid: Oid) {
    let worker: *mut LogicalRepWorker;

    LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

    worker = logicalrep_worker_find(subid, relid, false);

    if !worker.is_null() {
        Assert!(!isParallelApplyWorker(worker));
        logicalrep_worker_stop_internal(worker, SIGTERM);
    }

    LWLockRelease(LogicalRepWorkerLock());
}

// --------------------------------------------------------------------------
// logicalrep_pa_worker_stop
// --------------------------------------------------------------------------

/*
 * Stop the given logical replication parallel apply worker.
 *
 * Note that the function sends SIGUSR2 instead of SIGTERM to the parallel
 * apply worker so that the worker exits cleanly.
 */
pub unsafe fn logicalrep_pa_worker_stop(winfo: *mut ParallelApplyWorkerInfo) {
    let slot_no: c_int;
    let generation: uint16;
    let worker: *mut LogicalRepWorker;

    SpinLockAcquire(&mut (*(*winfo).shared).mutex);
    generation = (*(*winfo).shared).logicalrep_worker_generation;
    slot_no = (*(*winfo).shared).logicalrep_worker_slot_no;
    SpinLockRelease(&mut (*(*winfo).shared).mutex);

    Assert!(slot_no >= 0 && slot_no < max_logical_replication_workers);

    /*
     * Detach from the error_mq_handle for the parallel apply worker before
     * stopping it. This prevents the leader apply worker from trying to
     * receive the message from the error queue that might already be detached
     * by the parallel apply worker.
     */
    if !(*winfo).error_mq_handle.is_null() {
        shm_mq_detach((*winfo).error_mq_handle as *mut crate::storage::ipc::shm_mq::shm_mq_handle);
        (*winfo).error_mq_handle = null_mut();
    }

    LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

    worker = worker_slot(slot_no);
    Assert!(isParallelApplyWorker(worker));

    /*
     * Only stop the worker if the generation matches and the worker is alive.
     */
    if (*worker).generation == generation && !(*worker).proc.is_null() {
        logicalrep_worker_stop_internal(worker, SIGUSR2);
    }

    LWLockRelease(LogicalRepWorkerLock());
}

// --------------------------------------------------------------------------
// logicalrep_worker_wakeup / logicalrep_worker_wakeup_ptr
// --------------------------------------------------------------------------

/*
 * Wake up (using latch) any logical replication worker for specified sub/rel.
 */
pub unsafe fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) {
    let worker: *mut LogicalRepWorker;

    LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

    worker = logicalrep_worker_find(subid, relid, true);

    if !worker.is_null() {
        logicalrep_worker_wakeup_ptr(worker);
    }

    LWLockRelease(LogicalRepWorkerLock());
}

/*
 * Wake up (using latch) the specified logical replication worker.
 *
 * Caller must hold lock, else worker->proc could change under us.
 */
pub unsafe fn logicalrep_worker_wakeup_ptr(worker: *mut LogicalRepWorker) {
    Assert!(LWLockHeldByMe(LogicalRepWorkerLock()));

    SetLatch(&mut (*((*worker).proc as *mut ProcLatch)).proc_latch);
}

/*
 * Overlay for the procLatch field inside PGPROC.
 * TODO(pg-port): replace with real PGPROC layout once storage/lmgr/proc.h lands.
 */
use crate::storage::ipc::latch::Latch;
#[repr(C)]
struct ProcLatch {
    pid: pid_t,
    proc_latch: Latch,
}

// --------------------------------------------------------------------------
// logicalrep_worker_attach
// --------------------------------------------------------------------------

/*
 * Attach to a slot.
 */
pub unsafe fn logicalrep_worker_attach(slot: c_int) {
    /* Block concurrent access. */
    LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);

    Assert!(slot >= 0 && slot < max_logical_replication_workers);
    MyLogicalRepWorker = worker_slot(slot);

    if !(*MyLogicalRepWorker).in_use {
        LWLockRelease(LogicalRepWorkerLock());
        ereport!(
            ERROR,
            errmsg!(
                "logical replication worker slot {} is empty, cannot attach",
                slot
            )
        );
    }

    if !(*MyLogicalRepWorker).proc.is_null() {
        LWLockRelease(LogicalRepWorkerLock());
        ereport!(
            ERROR,
            errmsg!(
                "logical replication worker slot {} is already used by another worker, cannot attach",
                slot
            )
        );
    }

    (*MyLogicalRepWorker).proc = MyProc;
    before_shmem_exit(logicalrep_worker_onexit, 0 as Datum);

    LWLockRelease(LogicalRepWorkerLock());
}

// --------------------------------------------------------------------------
// logicalrep_worker_detach (static)
// --------------------------------------------------------------------------

/*
 * Stop the parallel apply workers if any, and detach the leader apply worker
 * (cleans up the worker info).
 */
unsafe fn logicalrep_worker_detach() {
    /* Stop the parallel apply workers. */
    if am_leader_apply_worker() {
        let workers: *mut List;
        let mut lc: *mut ListCell;

        /*
         * Detach from the error_mq_handle for all parallel apply workers
         * before terminating them. This prevents the leader apply worker from
         * receiving the worker termination message and sending it to logs
         * when the same is already done by the parallel worker.
         */
        pa_detach_all_error_mq();

        LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

        workers = logicalrep_workers_find((*MyLogicalRepWorker).subid, true, false);
        lc = list_head(workers);
        while !lc.is_null() {
            let w = lfirst(lc) as *mut LogicalRepWorker;

            if isParallelApplyWorker(w) {
                logicalrep_worker_stop_internal(w, SIGTERM);
            }
            lc = lnext(workers, lc);
        }

        LWLockRelease(LogicalRepWorkerLock());
    }

    /* Block concurrent access. */
    LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);

    logicalrep_worker_cleanup(MyLogicalRepWorker);

    LWLockRelease(LogicalRepWorkerLock());
}

// --------------------------------------------------------------------------
// logicalrep_worker_cleanup (static)
// --------------------------------------------------------------------------

/*
 * Clean up worker info.
 */
unsafe fn logicalrep_worker_cleanup(worker: *mut LogicalRepWorker) {
    Assert!(LWLockHeldByMeInMode(LogicalRepWorkerLock(), LW_EXCLUSIVE));

    (*worker).type_ = WORKERTYPE_UNKNOWN;
    (*worker).in_use = false;
    (*worker).proc = null_mut();
    (*worker).dbid = InvalidOid;
    (*worker).userid = InvalidOid;
    (*worker).subid = InvalidOid;
    (*worker).relid = InvalidOid;
    (*worker).leader_pid = InvalidPid;
    (*worker).parallel_apply = false;
}

// --------------------------------------------------------------------------
// logicalrep_launcher_onexit / logicalrep_worker_onexit (static)
// --------------------------------------------------------------------------

/*
 * Cleanup function for logical replication launcher.
 *
 * Called on logical replication launcher exit.
 */
unsafe extern "C" fn logicalrep_launcher_onexit(_code: c_int, _arg: Datum) {
    (*LogicalRepCtx).launcher_pid = 0;
}

/*
 * Cleanup function.
 *
 * Called on logical replication worker exit.
 */
unsafe extern "C" fn logicalrep_worker_onexit(_code: c_int, _arg: Datum) {
    /* Disconnect gracefully from the remote side. */
    if !LogRepWorkerWalRcvConn.is_null() {
        walrcv_disconnect(LogRepWorkerWalRcvConn);
    }

    logicalrep_worker_detach();

    /* Cleanup fileset used for streaming transactions. */
    if !(*MyLogicalRepWorker).stream_fileset.is_null() {
        FileSetDeleteAll((*MyLogicalRepWorker).stream_fileset as *mut crate::storage::file::fileset::FileSet);
    }

    /*
     * Session level locks may be acquired outside of a transaction in
     * parallel apply mode and will not be released when the worker
     * terminates, so manually release all locks before the worker exits.
     *
     * The locks will be acquired once the worker is initialized.
     */
    if !InitializingApplyWorker {
        LockReleaseAll(DEFAULT_LOCKMETHOD, true);
    }

    ApplyLauncherWakeup();
}

// --------------------------------------------------------------------------
// logicalrep_sync_worker_count / logicalrep_pa_worker_count
// --------------------------------------------------------------------------

/*
 * Count the number of registered (not necessarily running) sync workers
 * for a subscription.
 */
pub unsafe fn logicalrep_sync_worker_count(subid: Oid) -> c_int {
    let mut res: c_int = 0;

    Assert!(LWLockHeldByMe(LogicalRepWorkerLock()));

    /* Search for attached worker for a given subscription id. */
    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        let w = worker_slot(i);

        if isTablesyncWorker(w) && (*w).subid == subid {
            res += 1;
        }
        i += 1;
    }

    res
}

/*
 * Count the number of registered (but not necessarily running) parallel apply
 * workers for a subscription.
 */
unsafe fn logicalrep_pa_worker_count(subid: Oid) -> c_int {
    let mut res: c_int = 0;

    Assert!(LWLockHeldByMe(LogicalRepWorkerLock()));

    /*
     * Scan all attached parallel apply workers, only counting those which
     * have the given subscription id.
     */
    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        let w = worker_slot(i);

        if isParallelApplyWorker(w) && (*w).subid == subid {
            res += 1;
        }
        i += 1;
    }

    res
}

// --------------------------------------------------------------------------
// ApplyLauncherShmemSize / ApplyLauncherRegister / ApplyLauncherShmemInit
// --------------------------------------------------------------------------

/*
 * ApplyLauncherShmemSize
 *   Compute space needed for replication launcher shared memory
 */
pub unsafe fn ApplyLauncherShmemSize() -> Size {
    let mut size: Size;

    /*
     * Need the fixed struct and the array of LogicalRepWorker.
     */
    size = core::mem::size_of::<LogicalRepCtxStruct>();
    size = MAXALIGN(size);
    size = add_size(
        size,
        mul_size(
            max_logical_replication_workers as Size,
            core::mem::size_of::<LogicalRepWorker>(),
        ),
    );
    size
}

/*
 * ApplyLauncherRegister
 *   Register a background worker running the logical replication launcher.
 */
pub unsafe fn ApplyLauncherRegister() {
    let mut bgw: BackgroundWorker = core::mem::zeroed();

    /*
     * The logical replication launcher is disabled during binary upgrades, to
     * prevent logical replication workers from running on the source cluster.
     * That could cause replication origins to move forward after having been
     * copied to the target cluster, potentially creating conflicts with the
     * copied data files.
     */
    if max_logical_replication_workers == 0 || IsBinaryUpgrade {
        return;
    }

    memset(
        &mut bgw as *mut BackgroundWorker as *mut c_void,
        0,
        core::mem::size_of::<BackgroundWorker>(),
    );
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(
        bgw.bgw_library_name.as_mut_ptr(),
        MAXPGPATH,
        b"postgres\0".as_ptr() as *const c_char,
    );
    snprintf(
        bgw.bgw_function_name.as_mut_ptr(),
        BGW_MAXLEN,
        b"ApplyLauncherMain\0".as_ptr() as *const c_char,
    );
    snprintf(
        bgw.bgw_name.as_mut_ptr(),
        BGW_MAXLEN,
        b"logical replication launcher\0".as_ptr() as *const c_char,
    );
    snprintf(
        bgw.bgw_type.as_mut_ptr(),
        BGW_MAXLEN,
        b"logical replication launcher\0".as_ptr() as *const c_char,
    );
    bgw.bgw_restart_time = 5;
    bgw.bgw_notify_pid = 0;
    bgw.bgw_main_arg = 0 as Datum;

    RegisterBackgroundWorker(&mut bgw);
}

/*
 * ApplyLauncherShmemInit
 *   Allocate and initialize replication launcher shared memory
 */
pub unsafe fn ApplyLauncherShmemInit() {
    let mut found: bool = false;

    LogicalRepCtx = ShmemInitStruct(
        b"Logical Replication Launcher Data\0".as_ptr() as *const c_char,
        ApplyLauncherShmemSize(),
        &mut found,
    ) as *mut LogicalRepCtxStruct;

    if !found {
        memset(
            LogicalRepCtx as *mut c_void,
            0,
            ApplyLauncherShmemSize(),
        );

        (*LogicalRepCtx).last_start_dsa = DSA_HANDLE_INVALID_VAL;
        (*LogicalRepCtx).last_start_dsh = DSHASH_HANDLE_INVALID;

        /* Initialize memory and spin locks for each worker slot. */
        let mut slot: c_int = 0;
        while slot < max_logical_replication_workers {
            let worker = worker_slot(slot);

            memset(
                worker as *mut c_void,
                0,
                core::mem::size_of::<LogicalRepWorker>(),
            );
            SpinLockInit(&mut (*worker).relmutex);
            slot += 1;
        }
    }
}

// --------------------------------------------------------------------------
// logicalrep_launcher_attach_dshmem (static)
// --------------------------------------------------------------------------

/*
 * Initialize or attach to the dynamic shared hash table that stores the
 * last-start times, if not already done.
 * This must be called before accessing the table.
 */
unsafe fn logicalrep_launcher_attach_dshmem() {
    let oldcontext: MemoryContext;

    /* Quick exit if we already did this. */
    if (*LogicalRepCtx).last_start_dsh != DSHASH_HANDLE_INVALID && !last_start_times.is_null() {
        return;
    }

    /* Otherwise, use a lock to ensure only one process creates the table. */
    LWLockAcquire(LogicalRepWorkerLock(), LW_EXCLUSIVE);

    /* Be sure any local memory allocated by DSA routines is persistent. */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    if (*LogicalRepCtx).last_start_dsh == DSHASH_HANDLE_INVALID {
        /* Initialize dynamic shared hash table for last-start times. */
        last_start_times_dsa = dsa_create(LWTRANCHE_LAUNCHER_DSA);
        dsa_pin(last_start_times_dsa);
        dsa_pin_mapping(last_start_times_dsa);
        last_start_times = dshash_create(last_start_times_dsa, &dsh_params, null_mut());

        /* Store handles in shared memory for other backends to use. */
        (*LogicalRepCtx).last_start_dsa = dsa_get_handle(last_start_times_dsa);
        (*LogicalRepCtx).last_start_dsh = dshash_get_hash_table_handle(last_start_times);
    } else if last_start_times.is_null() {
        /* Attach to existing dynamic shared hash table. */
        last_start_times_dsa = dsa_attach((*LogicalRepCtx).last_start_dsa);
        dsa_pin_mapping(last_start_times_dsa);
        last_start_times = dshash_attach(
            last_start_times_dsa,
            &dsh_params,
            (*LogicalRepCtx).last_start_dsh,
            null_mut(),
        );
    }

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(LogicalRepWorkerLock());
}

// --------------------------------------------------------------------------
// ApplyLauncherSetWorkerStartTime / ApplyLauncherGetWorkerStartTime (static)
// --------------------------------------------------------------------------

/*
 * Set the last-start time for the subscription.
 */
unsafe fn ApplyLauncherSetWorkerStartTime(subid: Oid, start_time: TimestampTz) {
    let mut found: bool = false;

    logicalrep_launcher_attach_dshmem();

    let entry = dshash_find_or_insert(
        last_start_times,
        &subid as *const Oid as *const c_void,
        &mut found,
    ) as *mut LauncherLastStartTimesEntry;
    (*entry).last_start_time = start_time;
    dshash_release_lock(last_start_times, entry as *mut c_void);
}

/*
 * Return the last-start time for the subscription, or 0 if there isn't one.
 */
unsafe fn ApplyLauncherGetWorkerStartTime(subid: Oid) -> TimestampTz {
    let ret: TimestampTz;

    logicalrep_launcher_attach_dshmem();

    let entry = dshash_find(
        last_start_times,
        &subid as *const Oid as *const c_void,
        false,
    ) as *mut LauncherLastStartTimesEntry;
    if entry.is_null() {
        return 0;
    }

    ret = (*entry).last_start_time;
    dshash_release_lock(last_start_times, entry as *mut c_void);

    ret
}

// --------------------------------------------------------------------------
// ApplyLauncherForgetWorkerStartTime
// --------------------------------------------------------------------------

/*
 * Remove the last-start-time entry for the subscription, if one exists.
 *
 * This has two use-cases: to remove the entry related to a subscription
 * that's been deleted or disabled (just to avoid leaking shared memory),
 * and to allow immediate restart of an apply worker that has exited
 * due to subscription parameter changes.
 */
pub unsafe fn ApplyLauncherForgetWorkerStartTime(subid: Oid) {
    logicalrep_launcher_attach_dshmem();

    dshash_delete_key(last_start_times, &subid as *const Oid as *const c_void);
}

// --------------------------------------------------------------------------
// AtEOXact_ApplyLauncher / ApplyLauncherWakeupAtCommit / ApplyLauncherWakeup
// --------------------------------------------------------------------------

/*
 * Wakeup the launcher on commit if requested.
 */
pub unsafe fn AtEOXact_ApplyLauncher(is_commit: bool) {
    if is_commit {
        if on_commit_launcher_wakeup {
            ApplyLauncherWakeup();
        }
    }

    on_commit_launcher_wakeup = false;
}

/*
 * Request wakeup of the launcher on commit of the transaction.
 *
 * This is used to send launcher signal to stop sleeping and process the
 * subscriptions when current transaction commits. Should be used when new
 * tuple was added to the pg_subscription catalog.
*/
pub unsafe fn ApplyLauncherWakeupAtCommit() {
    if !on_commit_launcher_wakeup {
        on_commit_launcher_wakeup = true;
    }
}

unsafe fn ApplyLauncherWakeup() {
    if (*LogicalRepCtx).launcher_pid != 0 {
        kill((*LogicalRepCtx).launcher_pid, SIGUSR1);
    }
}

// --------------------------------------------------------------------------
// ApplyLauncherMain
// --------------------------------------------------------------------------

/*
 * Main loop for the apply launcher process.
 */
pub unsafe fn ApplyLauncherMain(_main_arg: Datum) {
    ereport!(DEBUG1, errmsg!("logical replication launcher started"));

    before_shmem_exit(logicalrep_launcher_onexit, 0 as Datum);

    Assert!((*LogicalRepCtx).launcher_pid == 0);
    (*LogicalRepCtx).launcher_pid = MyProcPid;

    /* Establish signal handlers. */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGTERM, Some(die));
    BackgroundWorkerUnblockSignals();

    /*
     * Establish connection to nailed catalogs (we only ever access
     * pg_subscription).
     */
    BackgroundWorkerInitializeConnection(null(), null(), 0);

    /* Enter main loop */
    loop {
        let rc: c_int;
        let sublist: *mut List;
        let mut lc: *mut ListCell;
        let subctx: MemoryContext;
        let oldctx: MemoryContext;
        let mut wait_time: c_long = DEFAULT_NAPTIME_PER_CYCLE as c_long;

        CHECK_FOR_INTERRUPTS();

        /* Use temporary context to avoid leaking memory across cycles. */
        subctx = AllocSetContextCreate!(
            TopMemoryContext,
            b"Logical Replication Launcher sublist\0".as_ptr() as *const c_char,
            ALLOCSET_DEFAULT_SIZES
        );
        oldctx = MemoryContextSwitchTo(subctx);

        /* Start any missing workers for enabled subscriptions. */
        sublist = get_subscription_list();
        lc = list_head(sublist);
        while !lc.is_null() {
            let sub = lfirst(lc) as *mut Subscription_local;
            let w: *mut LogicalRepWorker;
            let last_start: TimestampTz;
            let now: TimestampTz;
            let elapsed: c_long;

            if !(*sub).enabled {
                lc = lnext(sublist, lc);
                continue;
            }

            LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);
            w = logicalrep_worker_find((*sub).oid, InvalidOid, false);
            LWLockRelease(LogicalRepWorkerLock());

            if !w.is_null() {
                lc = lnext(sublist, lc);
                continue; /* worker is running already */
            }

            /*
             * If the worker is eligible to start now, launch it.  Otherwise,
             * adjust wait_time so that we'll wake up as soon as it can be
             * started.
             *
             * Each subscription's apply worker can only be restarted once per
             * wal_retrieve_retry_interval, so that errors do not cause us to
             * repeatedly restart the worker as fast as possible.  In cases
             * where a restart is expected (e.g., subscription parameter
             * changes), another process should remove the last-start entry
             * for the subscription so that the worker can be restarted
             * without waiting for wal_retrieve_retry_interval to elapse.
             */
            last_start = ApplyLauncherGetWorkerStartTime((*sub).oid);
            now = GetCurrentTimestamp();
            elapsed = TimestampDifferenceMilliseconds(last_start, now);
            if last_start == 0 || elapsed >= wal_retrieve_retry_interval as c_long {
                ApplyLauncherSetWorkerStartTime((*sub).oid, now);
                if !logicalrep_worker_launch(
                    WORKERTYPE_APPLY,
                    (*sub).dbid,
                    (*sub).oid,
                    (*sub).name,
                    (*sub).owner,
                    InvalidOid,
                    DSM_HANDLE_INVALID,
                ) {
                    /*
                     * We get here either if we failed to launch a worker
                     * (perhaps for resource-exhaustion reasons) or if we
                     * launched one but it immediately quit.  Either way, it
                     * seems appropriate to try again after
                     * wal_retrieve_retry_interval.
                     */
                    wait_time = Min(wait_time, wal_retrieve_retry_interval as c_long);
                }
            } else {
                wait_time = Min(wait_time, wal_retrieve_retry_interval as c_long - elapsed as c_long);
            }

            lc = lnext(sublist, lc);
        }

        /* Switch back to original memory context. */
        MemoryContextSwitchTo(oldctx);
        /* Clean the temporary memory. */
        MemoryContextDelete(subctx);

        /* Wait for more work. */
        rc = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            wait_time,
            WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
            CHECK_FOR_INTERRUPTS();
        }

        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
    }

    /* Not reachable */
}

// --------------------------------------------------------------------------
// IsLogicalLauncher / GetLeaderApplyWorkerPid
// --------------------------------------------------------------------------

/*
 * Is current process the logical replication launcher?
 */
pub unsafe fn IsLogicalLauncher() -> bool {
    (*LogicalRepCtx).launcher_pid == MyProcPid
}

/*
 * Return the pid of the leader apply worker if the given pid is the pid of a
 * parallel apply worker, otherwise, return InvalidPid.
 */
pub unsafe fn GetLeaderApplyWorkerPid(pid: pid_t) -> pid_t {
    let mut leader_pid: pid_t = InvalidPid;

    LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        let w = worker_slot(i);

        if isParallelApplyWorker(w)
            && !(*w).proc.is_null()
            && pid == (*((*w).proc as *mut ProcPidOnly)).pid
        {
            leader_pid = (*w).leader_pid;
            break;
        }
        i += 1;
    }

    LWLockRelease(LogicalRepWorkerLock());

    leader_pid
}

// --------------------------------------------------------------------------
// pg_stat_get_subscription
// --------------------------------------------------------------------------

const PG_STAT_GET_SUBSCRIPTION_COLS: usize = 10;

/*
 * Returns state of the subscriptions.
 */
pub unsafe fn pg_stat_get_subscription(fcinfo: FunctionCallInfo) -> Datum {
    let subid: Oid = if PG_ARGISNULL!(fcinfo, 0) {
        InvalidOid
    } else {
        PG_GETARG_OID!(fcinfo, 0)
    };
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    InitMaterializedSRF(fcinfo, 0);

    /* Make sure we get consistent view of the workers. */
    LWLockAcquire(LogicalRepWorkerLock(), LW_SHARED);

    let mut i: c_int = 0;
    while i < max_logical_replication_workers {
        /* for each row */
        let mut values: [Datum; PG_STAT_GET_SUBSCRIPTION_COLS] =
            [0 as Datum; PG_STAT_GET_SUBSCRIPTION_COLS];
        let mut nulls: [bool; PG_STAT_GET_SUBSCRIPTION_COLS] =
            [false; PG_STAT_GET_SUBSCRIPTION_COLS];
        let worker_pid: c_int;
        let mut worker: LogicalRepWorker = core::mem::zeroed();

        memcpy(
            &mut worker as *mut LogicalRepWorker as *mut c_void,
            worker_slot(i) as *const c_void,
            core::mem::size_of::<LogicalRepWorker>(),
        );
        if worker.proc.is_null() || !IsBackendPid((*(worker.proc as *mut ProcPidOnly)).pid) {
            i += 1;
            continue;
        }

        if OidIsValid(subid) && worker.subid != subid {
            i += 1;
            continue;
        }

        worker_pid = (*(worker.proc as *mut ProcPidOnly)).pid;

        values[0] = ObjectIdGetDatum(worker.subid);
        if isTablesyncWorker(&worker) {
            values[1] = ObjectIdGetDatum(worker.relid);
        } else {
            nulls[1] = true;
        }
        values[2] = Int32GetDatum(worker_pid);

        if isParallelApplyWorker(&worker) {
            values[3] = Int32GetDatum(worker.leader_pid);
        } else {
            nulls[3] = true;
        }

        if XLogRecPtrIsInvalid(worker.last_lsn) {
            nulls[4] = true;
        } else {
            values[4] = LSNGetDatum(worker.last_lsn);
        }
        if worker.last_send_time == 0 {
            nulls[5] = true;
        } else {
            values[5] = TimestampTzGetDatum(worker.last_send_time);
        }
        if worker.last_recv_time == 0 {
            nulls[6] = true;
        } else {
            values[6] = TimestampTzGetDatum(worker.last_recv_time);
        }
        if XLogRecPtrIsInvalid(worker.reply_lsn) {
            nulls[7] = true;
        } else {
            values[7] = LSNGetDatum(worker.reply_lsn);
        }
        if worker.reply_time == 0 {
            nulls[8] = true;
        } else {
            values[8] = TimestampTzGetDatum(worker.reply_time);
        }

        match worker.type_ {
            WORKERTYPE_APPLY => {
                values[9] = CStringGetTextDatum(b"apply\0".as_ptr() as *const c_char);
            }
            WORKERTYPE_PARALLEL_APPLY => {
                values[9] = CStringGetTextDatum(b"parallel apply\0".as_ptr() as *const c_char);
            }
            WORKERTYPE_TABLESYNC => {
                values[9] =
                    CStringGetTextDatum(b"table synchronization\0".as_ptr() as *const c_char);
            }
            _ => {
                /* Should never happen. */
                elog!(ERROR, "unknown worker type");
                unreachable!();
            }
        }

        tuplestore_putvalues(
            (*rsinfo).setResult as *mut core::ffi::c_void,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        /*
         * If only a single subscription was requested, and we found it,
         * break.
         */
        if OidIsValid(subid) {
            break;
        }
        i += 1;
    }

    LWLockRelease(LogicalRepWorkerLock());

    0 as Datum
}

// --------------------------------------------------------------------------
// PG_ARGISNULL / PG_GETARG_OID macros (local shims matching other files)
// --------------------------------------------------------------------------

// PG_ARGISNULL!/PG_GETARG_OID! come from the crate-root fmgr macros.
use crate::{PG_ARGISNULL, PG_GETARG_OID};

// dshash_get_hash_table_handle re-export
use crate::lib::dshash::dshash_get_hash_table_handle;

// MAXALIGN re-export from crate::c
use crate::c::MAXALIGN;

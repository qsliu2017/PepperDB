/*-------------------------------------------------------------------------
 * slotsync.c
 *	   Functionality for synchronizing slots to a standby server from the
 *         primary server.
 *
 * Copyright (c) 2024-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/slotsync.c
 *
 * This file contains the code for slot synchronization on a physical standby
 * to fetch logical failover slots information from the primary server, create
 * the slots on the standby and synchronize them periodically.
 *
 * Slot synchronization can be performed either automatically by enabling slot
 * sync worker or manually by calling SQL function pg_sync_replication_slots().
 *
 * If the WAL corresponding to the remote's restart_lsn is not available on the
 * physical standby or the remote's catalog_xmin precedes the oldest xid for
 * which it is guaranteed that rows wouldn't have been removed then we cannot
 * create the local standby slot because that would mean moving the local slot
 * backward and decoding won't be possible via such a slot. In this case, the
 * slot will be marked as RS_TEMPORARY. Once the primary server catches up,
 * the slot will be marked as RS_PERSISTENT (which means sync-ready) after
 * which slot sync worker can perform the sync periodically or user can call
 * pg_sync_replication_slots() periodically to perform the syncs.
 *
 * If synchronized slots fail to build a consistent snapshot from the
 * restart_lsn before reaching confirmed_flush_lsn, they would become
 * unreliable after promotion due to potential data loss from changes
 * before reaching a consistent point. This can happen because the slots can
 * be synced at some random time and we may not reach the consistent point
 * at the same WAL location as the primary. So, we mark such slots as
 * RS_TEMPORARY. Once the decoding from corresponding LSNs can reach a
 * consistent point, they will be marked as RS_PERSISTENT.
 *
 * The slot sync worker waits for some time before the next synchronization,
 * with the duration varying based on whether any slots were updated during
 * the last cycle. Refer to the comments above wait_for_slot_activity() for
 * more details.
 *
 * Any standby synchronized slots will be dropped if they no longer need
 * to be synchronized. See comment atop drop_local_obsolete_slots() for more
 * details.
 *---------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_long, c_void};

use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, LSN_FORMAT_ARGS, XLogRecPtr, XLogSegNo, XLogRecPtrIsValid,
};
use crate::access::transam::xlogreader::XLogRecPtrIsInvalid;
use crate::access::transam::transam::{TransactionIdFollows, TransactionIdPrecedes};
use crate::access::transam::{InvalidTransactionId, TransactionIdIsValid};
use crate::access::transam::xlog::{wal_level, XLogGetLastRemovedSegno,
    XLogGetReplicationSlotMinimumLSN};
use crate::access::rmgrdesc::xlogdesc::WAL_LEVEL_LOGICAL;
use crate::storage::ipc::latch::{
    WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT,
};
use crate::access::transam::xloginsert::GetRedoRecPtr;
use crate::access::transam::xlogrecovery::{PrimaryConnInfo, PrimarySlotName, StandbyMode};
use crate::access::transam::xlogutils::wal_segment_size;
use crate::c::{NameData, NameStr, Size, TransactionId};
use core::ffi::CStr;
use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};
use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGCHLD, SIGFPE, SIGHUP, SIGINT, SIGPIPE,
    SIGTERM, SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::miscadmin::{
    AmLogicalSlotSyncWorkerProcess, BaseInit, GetProcessingMode,
    InitPostgres, MyBackendType, MyLatch, MyProcPid, SetProcessingMode, TimestampTz,
    B_SLOTSYNC_WORKER, HOLD_INTERRUPTS, InitProcessing,
    InvalidPid, NormalProcessing,
};
use crate::utils::misc::ps_status::init_ps_display;
use crate::nodes::execnodes::Tuplestorestate;
use crate::nodes::pg_list::{lappend, list_free_deep, List, NIL};
use crate::postgres::{DatumGetBool, DatumGetPointer, DatumGetTransactionId, PointerGetDatum,
    Datum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::replication::slot::{
    MyReplicationSlot, ReplicationSlot, ReplicationSlotAllocationLock,
    ReplicationSlotSetInactiveSince, SlotIsLogical, GetSlotInvalidationCause,
    ReplicationSlotCleanup, ReplicationSlotCreate,
};
use crate::replication::slotfuncs::{
    max_replication_slots, ReplicationSlotControlLock, ReplicationSlotCtl, LWLock,
    LW_EXCLUSIVE, LW_SHARED, RS_INVAL_NONE, RS_TEMPORARY, ReplicationSlotInvalidationCause,
};
use crate::replication::logical::logical::LogicalSlotAdvanceAndCheckSnapState;
use crate::replication::logical::snapbuild::SnapBuildSnapshotExists;
use crate::storage::ipc::ipc::{before_shmem_exit, proc_exit};
use crate::storage::ipc::shmem::ShmemInitStruct;
use crate::storage::lockdefs::AccessShareLock;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::storage::lmgr::s_lock::slock_t;
use crate::utils::adt::timestamp::GetCurrentTimestamp;
use crate::utils::misc::guc::{ProcessConfigFile, SetConfigOption};
use crate::postmaster::interrupt::ConfigReloadPending;

// foreach_ptr! is #[macro_export]; pull it into textual scope explicitly.
use crate::foreach_ptr;

// Local macro stubs (not #[macro_export]; kept local per the port convention).
macro_rules! CHECK_FOR_INTERRUPTS {
    () => { crate::miscadmin::CHECK_FOR_INTERRUPTS() };
}
macro_rules! Min { ($a:expr, $b:expr) => { core::cmp::min($a, $b) }; }
macro_rules! Max { ($a:expr, $b:expr) => { core::cmp::max($a, $b) }; }
macro_rules! XLByteToSeg {
    ($xlrp:expr, $logSegNo:ident, $wal_segsz:expr) => {
        $logSegNo = ($xlrp / ($wal_segsz as $crate::access::transam::xlogdefs::XLogRecPtr))
            as $crate::access::transam::xlogdefs::XLogSegNo;
    };
}

// ---------------------------------------------------------------------------
// Constants from headers not yet ported (with TODO(pg-port)) and libc bits
// ---------------------------------------------------------------------------

// pid_t (sys/types.h)
#[allow(non_camel_case_types)]
type pid_t = c_int;

// time_t / time(2) (time.h)
#[allow(non_camel_case_types)]
type time_t = i64;

unsafe extern "C" {
    fn time(tloc: *mut time_t) -> time_t;
    fn kill(pid: pid_t, sig: c_int) -> c_int;
    // sigprocmask(2) - libc.
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
}

// SIG_SETMASK (signal.h)
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

// SIG_IGN is the function pointer with value 1; pqsignal.rs models SIG_DFL as
// None and leaves SIG_IGN to callers. We construct it here as a fn-ptr from 1.
fn SIG_IGN() -> SigHandler {
    unsafe { Some(core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize)) }
}

// PGC_* (guc.h)
const PGC_SIGHUP: c_int = 3;
const PGC_SUSET: c_int = 5;
const PGC_S_OVERRIDE: c_int = 13;

// WAIT_EVENT_* (wait_event.h). TODO(pg-port): wire to real wait_event ids.
const WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN: u32 = 0;
const WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN: u32 = 0;

// WalRcvExecResult / WalReceiverConn / WALRCV_OK_TUPLES (libpqwalreceiver).
use crate::replication::libpqwalreceiver::libpqwalreceiver::{
    WalRcvExecResult, WALRCV_OK_TUPLES,
};
use crate::replication::worker_internal::WalReceiverConn;

// ---------------------------------------------------------------------------
// SQL/datum/string OID constants (catalog/pg_type_d.h). TODO(pg-port): import.
// ---------------------------------------------------------------------------
const BOOLOID: Oid = 16;
const TEXTOID: Oid = 25;
const XIDOID: Oid = 28;
const LSNOID: Oid = 3220;

// errcode placeholders folded into comments below ("C also:").

// ---------------------------------------------------------------------------
// Dependencies defined in other .c files - stubbed with TODO(pg-port) bodies.
// ---------------------------------------------------------------------------

// walrcv_* macro wrappers (replication/walreceiver.h dispatch through vtable).
// TODO(pg-port): replace with real walreceiver.rs wrappers once exported.
unsafe fn walrcv_exec(
    _conn: *mut WalReceiverConn,
    _query: *const c_char,
    _nRetTypes: c_int,
    _retTypes: *const Oid,
) -> *mut WalRcvExecResult {
    null_mut() // TODO(pg-port)
}
unsafe fn walrcv_clear_result(_walres: *mut WalRcvExecResult) {
    // TODO(pg-port)
}
unsafe fn walrcv_disconnect(_conn: *mut WalReceiverConn) {
    // TODO(pg-port)
}
unsafe fn walrcv_get_dbname_from_conninfo(_conninfo: *const c_char) -> *mut c_char {
    null_mut() // TODO(pg-port)
}
unsafe fn walrcv_connect(
    _conninfo: *const c_char,
    _replication: bool,
    _logical: bool,
    _must_use_password: bool,
    _appname: *const c_char,
    _err: *mut *mut c_char,
) -> *mut WalReceiverConn {
    null_mut() // TODO(pg-port)
}

// appendStringInfo - variadic printf-style; modeled with explicit formatting.
// TODO(pg-port): use the real stringinfo formatter once ported.
unsafe fn appendStringInfo_fmt(str_: *mut StringInfoData, s: *const c_char) {
    appendStringInfoString(str_, s);
}

// TupleTableSlot / TupleDesc / executor tuple helpers.
use crate::executor::tuptable::TupleTableSlot;
use crate::access::common::tupdesc::TupleDesc;

unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: TupleDesc,
    _tts_ops: *const c_void,
) -> *mut TupleTableSlot {
    null_mut() // TODO(pg-port)
}
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    false // TODO(pg-port)
}
unsafe fn slot_getattr(_slot: *mut TupleTableSlot, _attnum: c_int, _isnull: *mut bool) -> Datum {
    0 // TODO(pg-port)
}
unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    null_mut() // TODO(pg-port)
}
// TTSOpsMinimalTuple - executor/execTuples.c.
// TODO(pg-port): reference real TTSOpsMinimalTuple once visibility allows.
static TTSOpsMinimalTuple: () = ();

// Datum conversion helpers (utils/builtins.h, utils/pg_lsn.h).
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    null_mut() // TODO(pg-port)
}
unsafe fn DatumGetLSN(d: Datum) -> XLogRecPtr {
    d as XLogRecPtr // TODO(pg-port)
}

// String / memory helpers (utils/builtins.h, mb).
unsafe fn quote_literal_cstr(_rawstr: *const c_char) -> *mut c_char {
    null_mut() // TODO(pg-port)
}
unsafe fn namestrcpy(_name: *mut NameData, _str_: *const c_char) {
    // TODO(pg-port)
}

// catalog/database (commands/dbcommands.h).
unsafe fn get_database_oid(_dbname: *const c_char, _missing_ok: bool) -> Oid {
    InvalidOid // TODO(pg-port)
}

// catalog object locks (storage/lmgr.h).
const DatabaseRelationId: Oid = 1262;
unsafe fn LockSharedObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: c_int) {
    // TODO(pg-port)
}
unsafe fn UnlockSharedObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: c_int) {
    // TODO(pg-port)
}

// LWLock helpers (storage/lwlock.h).
unsafe fn LWLockAcquire(_lock: LWLock, _mode: c_int) -> bool {
    false // TODO(pg-port)
}
unsafe fn LWLockRelease(_lock: LWLock) {
    // TODO(pg-port)
}
// ProcArrayLock (storage/lwlocknames.h). TODO(pg-port).
use crate::backend_link_shims::ProcArrayLock;

// procarray.h.
unsafe fn GetOldestSafeDecodingTransactionId(_catalogOnly: bool) -> TransactionId {
    InvalidTransactionId // TODO(pg-port)
}

// replication/slot.c helpers not yet exported from slot.rs.
unsafe fn ReplicationSlotAcquire(_name: *const c_char, _nowait: bool, _error_if_invalid: bool) {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotRelease() {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotDropAcquired() {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotPersist() {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotMarkDirty() {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotSave() {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotsComputeRequiredXmin(_already_locked: bool) {
    // TODO(pg-port)
}
unsafe fn ReplicationSlotsComputeRequiredLSN() {
    // TODO(pg-port)
}
unsafe fn SearchNamedReplicationSlot(_name: *const c_char, _need_lock: bool) -> *mut ReplicationSlot {
    null_mut() // TODO(pg-port)
}

// replication/walsender.c.
unsafe fn GetStandbyFlushRecPtr(_tli: *mut c_void) -> XLogRecPtr {
    InvalidXLogRecPtr // TODO(pg-port)
}

// access/transam/xact.c.
unsafe fn IsTransactionState() -> bool {
    false // TODO(pg-port)
}
unsafe fn StartTransactionCommand() {
    // TODO(pg-port)
}
unsafe fn CommitTransactionCommand() {
    // TODO(pg-port)
}

// storage/ipc/latch.c.
unsafe fn WaitLatch(_latch: *mut c_void, _wakeEvents: c_int, _timeout: c_long, _wait_event_info: u32) -> c_int {
    0 // TODO(pg-port)
}
unsafe fn ResetLatch(_latch: *mut c_void) {
    // TODO(pg-port)
}

// utils/misc/timeout.c.
unsafe fn InitializeTimeouts() {
    // TODO(pg-port)
}

// utils/fmgr/dfmgr.c.
unsafe fn load_file(_filename: *const c_char, _restricted: bool) {
    // TODO(pg-port)
}

// storage/lmgr/proc.c.
unsafe fn InitProcess() {
    // TODO(pg-port)
}

// utils/error/elog.c - error stack and report.
// TODO(pg-port): import real elog state/report once exported.
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();
unsafe fn EmitErrorReport() {
    // TODO(pg-port)
}

// tcop/postgres.c signal handlers and config-reload handler.
unsafe extern "C" fn die(_sig: c_int) {
    // TODO(pg-port)
}
unsafe extern "C" fn StatementCancelHandler(_sig: c_int) {
    // TODO(pg-port)
}
unsafe extern "C" fn FloatExceptionHandler(_sig: c_int) {
    // TODO(pg-port)
}
unsafe extern "C" fn procsignal_sigusr1_handler(_sig: c_int) {
    // TODO(pg-port)
}
unsafe extern "C" fn SignalHandlerForConfigReload(_sig: c_int) {
    // TODO(pg-port)
}

// utils/misc/ps_status.c.
static mut cluster_name: [c_char; 1] = [0];

// pruneheap.c / GUC.
static mut hot_standby_feedback: bool = false;

// sigsetjmp - setjmp.h. TODO(pg-port): wire to real sigsetjmp once available.
#[allow(non_camel_case_types)]
type sigjmp_buf = [c_void; 0];
unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    0 // TODO(pg-port)
}

// strcmp(3) - libc.
unsafe extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// SlotSyncCtxStruct - struct for sharing information to control slot
// synchronization.
//
// The slot sync worker's pid is needed by the startup process to shut it
// down during promotion. The startup process shuts down the slot sync worker
// and also sets stopSignaled=true to handle the race condition when the
// postmaster has not noticed the promotion yet and thus may end up restarting
// the slot sync worker. If stopSignaled is set, the worker will exit in such a
// case. The SQL function pg_sync_replication_slots() will also error out if
// this flag is set. Note that we don't need to reset this variable as after
// promotion the slot sync worker won't be restarted because the pmState
// changes to PM_RUN from PM_HOT_STANDBY and we don't support demoting
// primary without restarting the server. See LaunchMissingBackgroundProcesses.
//
// The 'syncing' flag is needed to prevent concurrent slot syncs to avoid slot
// overwrites.
//
// The 'last_start_time' is needed by postmaster to start the slot sync worker
// once per SLOTSYNC_RESTART_INTERVAL_SEC. In cases where an immediate restart
// is expected (e.g., slot sync GUCs change), slot sync worker will reset
// last_start_time before exiting, so that postmaster can start the worker
// without waiting for SLOTSYNC_RESTART_INTERVAL_SEC.
// ---------------------------------------------------------------------------
#[repr(C)]
struct SlotSyncCtxStruct {
    pid: pid_t,
    stopSignaled: bool,
    syncing: bool,
    last_start_time: time_t,
    mutex: slock_t,
}

static mut SlotSyncCtx: *mut SlotSyncCtxStruct = null_mut();

/* GUC variable */
#[allow(non_upper_case_globals)]
pub static mut sync_replication_slots: bool = false;

/*
 * The sleep time (ms) between slot-sync cycles varies dynamically
 * (within a MIN/MAX range) according to slot activity. See
 * wait_for_slot_activity() for details.
 */
const MIN_SLOTSYNC_WORKER_NAPTIME_MS: c_long = 200;
const MAX_SLOTSYNC_WORKER_NAPTIME_MS: c_long = 30000; /* 30s */

static mut sleep_ms: c_long = MIN_SLOTSYNC_WORKER_NAPTIME_MS;

/* The restart interval for slot sync work used by postmaster */
const SLOTSYNC_RESTART_INTERVAL_SEC: c_int = 10;

/*
 * Flag to tell if we are syncing replication slots. Unlike the 'syncing' flag
 * in SlotSyncCtxStruct, this flag is true only if the current process is
 * performing slot synchronization.
 */
static mut syncing_slots: bool = false;

/*
 * Structure to hold information fetched from the primary server about a logical
 * replication slot.
 */
#[repr(C)]
struct RemoteSlot {
    name: *mut c_char,
    plugin: *mut c_char,
    database: *mut c_char,
    two_phase: bool,
    failover: bool,
    restart_lsn: XLogRecPtr,
    confirmed_lsn: XLogRecPtr,
    two_phase_at: XLogRecPtr,
    catalog_xmin: TransactionId,

    /* RS_INVAL_NONE if valid, or the reason of invalidation */
    invalidated: ReplicationSlotInvalidationCause,
}

/*
 * If necessary, update the local synced slot's metadata based on the data
 * from the remote slot.
 *
 * If no update was needed (the data of the remote slot is the same as the
 * local slot) return false, otherwise true.
 *
 * *found_consistent_snapshot will be true iff the remote slot's LSN or xmin is
 * modified, and decoding from the corresponding LSN's can reach a
 * consistent snapshot.
 *
 * *remote_slot_precedes will be true if the remote slot's LSN or xmin
 * precedes locally reserved position.
 */
unsafe fn update_local_synced_slot(
    remote_slot: *mut RemoteSlot,
    remote_dbid: Oid,
    found_consistent_snapshot: *mut bool,
    remote_slot_precedes: *mut bool,
) -> bool {
    let slot: *mut ReplicationSlot = MyReplicationSlot;
    let mut updated_xmin_or_lsn = false;
    let mut updated_config = false;

    Assert!((*slot).data.invalidated == RS_INVAL_NONE);

    if !found_consistent_snapshot.is_null() {
        *found_consistent_snapshot = false;
    }

    if !remote_slot_precedes.is_null() {
        *remote_slot_precedes = false;
    }

    /*
     * Don't overwrite if we already have a newer catalog_xmin and
     * restart_lsn.
     */
    if (*remote_slot).restart_lsn < (*slot).data.restart_lsn
        || TransactionIdPrecedes((*remote_slot).catalog_xmin, (*slot).data.catalog_xmin)
    {
        /*
         * This can happen in following situations:
         *
         * If the slot is temporary, it means either the initial WAL location
         * reserved for the local slot is ahead of the remote slot's
         * restart_lsn or the initial xmin_horizon computed for the local slot
         * is ahead of the remote slot.
         *
         * If the slot is persistent, both restart_lsn and catalog_xmin of the
         * synced slot could still be ahead of the remote slot. Since we use
         * slot advance functionality to keep snapbuild/slot updated, it is
         * possible that the restart_lsn and catalog_xmin are advanced to a
         * later position than it has on the primary. This can happen when
         * slot advancing machinery finds running xacts record after reaching
         * the consistent state at a later point than the primary where it
         * serializes the snapshot and updates the restart_lsn.
         *
         * We LOG the message if the slot is temporary as it can help the user
         * to understand why the slot is not sync-ready. In the case of a
         * persistent slot, it would be a more common case and won't directly
         * impact the users, so we used DEBUG1 level to log the message.
         */
        let _level = if (*slot).data.persistency == RS_TEMPORARY { LOG } else { DEBUG1 };
        ereport!(
            _level,
            errmsg!(
                "could not synchronize replication slot \"{}\"",
                CStr::from_ptr((*remote_slot).name).to_string_lossy()
            )
        );
        // C also: errdetail("Synchronization could lead to data loss, because
        //   the remote slot needs WAL at LSN %X/%X and catalog xmin %u, but
        //   the standby has LSN %X/%X and catalog xmin %u.",
        //   LSN_FORMAT_ARGS(remote_slot->restart_lsn), remote_slot->catalog_xmin,
        //   LSN_FORMAT_ARGS(slot->data.restart_lsn), slot->data.catalog_xmin);

        if !remote_slot_precedes.is_null() {
            *remote_slot_precedes = true;
        }

        /*
         * Skip updating the configuration. This is required to avoid syncing
         * two_phase_at without syncing confirmed_lsn. Otherwise, the prepared
         * transaction between old confirmed_lsn and two_phase_at will
         * unexpectedly get decoded and sent to the downstream after
         * promotion. See comments in ReorderBufferFinishPrepared.
         */
        return false;
    }

    /*
     * Attempt to sync LSNs and xmins only if remote slot is ahead of local
     * slot.
     */
    if (*remote_slot).confirmed_lsn > (*slot).data.confirmed_flush
        || (*remote_slot).restart_lsn > (*slot).data.restart_lsn
        || TransactionIdFollows((*remote_slot).catalog_xmin, (*slot).data.catalog_xmin)
    {
        /*
         * We can't directly copy the remote slot's LSN or xmin unless there
         * exists a consistent snapshot at that point. Otherwise, after
         * promotion, the slots may not reach a consistent point before the
         * confirmed_flush_lsn which can lead to a data loss. To avoid data
         * loss, we let slot machinery advance the slot which ensures that
         * snapbuilder/slot statuses are updated properly.
         */
        if SnapBuildSnapshotExists((*remote_slot).restart_lsn) {
            /*
             * Update the slot info directly if there is a serialized snapshot
             * at the restart_lsn, as the slot can quickly reach consistency
             * at restart_lsn by restoring the snapshot.
             */
            SpinLockAcquire(&mut (*slot).mutex);
            (*slot).data.restart_lsn = (*remote_slot).restart_lsn;
            (*slot).data.confirmed_flush = (*remote_slot).confirmed_lsn;
            (*slot).data.catalog_xmin = (*remote_slot).catalog_xmin;
            SpinLockRelease(&mut (*slot).mutex);

            if !found_consistent_snapshot.is_null() {
                *found_consistent_snapshot = true;
            }
        } else {
            LogicalSlotAdvanceAndCheckSnapState(
                (*remote_slot).confirmed_lsn,
                found_consistent_snapshot,
            );

            /* Sanity check */
            if (*slot).data.confirmed_flush != (*remote_slot).confirmed_lsn {
                ereport!(
                    ERROR,
                    errmsg!(
                        "synchronized confirmed_flush for slot \"{}\" differs from remote slot",
                        CStr::from_ptr((*remote_slot).name).to_string_lossy()
                    )
                );
                // C also: errdetail_internal("Remote slot has LSN %X/%X but
                //   local slot has LSN %X/%X.",
                //   LSN_FORMAT_ARGS(remote_slot->confirmed_lsn),
                //   LSN_FORMAT_ARGS(slot->data.confirmed_flush));
            }
        }

        updated_xmin_or_lsn = true;
    }

    if remote_dbid != (*slot).data.database
        || (*remote_slot).two_phase != (*slot).data.two_phase
        || (*remote_slot).failover != (*slot).data.failover
        || strcmp((*remote_slot).plugin, NameStr(&(*slot).data.plugin)) != 0
        || (*remote_slot).two_phase_at != (*slot).data.two_phase_at
    {
        let mut plugin_name: NameData = core::mem::zeroed();

        /* Avoid expensive operations while holding a spinlock. */
        namestrcpy(&mut plugin_name, (*remote_slot).plugin);

        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).data.plugin = plugin_name;
        (*slot).data.database = remote_dbid;
        (*slot).data.two_phase = (*remote_slot).two_phase;
        (*slot).data.two_phase_at = (*remote_slot).two_phase_at;
        (*slot).data.failover = (*remote_slot).failover;
        SpinLockRelease(&mut (*slot).mutex);

        updated_config = true;

        /*
         * Ensure that there is no risk of sending prepared transactions
         * unexpectedly after the promotion.
         */
        Assert!((*slot).data.two_phase_at <= (*slot).data.confirmed_flush);
    }

    /*
     * We have to write the changed xmin to disk *before* we change the
     * in-memory value, otherwise after a crash we wouldn't know that some
     * catalog tuples might have been removed already.
     */
    if updated_config || updated_xmin_or_lsn {
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
    }

    /*
     * Now the new xmin is safely on disk, we can let the global value
     * advance. We do not take ProcArrayLock or similar since we only advance
     * xmin here and there's not much harm done by a concurrent computation
     * missing that.
     */
    if updated_xmin_or_lsn {
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).effective_catalog_xmin = (*remote_slot).catalog_xmin;
        SpinLockRelease(&mut (*slot).mutex);

        ReplicationSlotsComputeRequiredXmin(false);
        ReplicationSlotsComputeRequiredLSN();
    }

    updated_config || updated_xmin_or_lsn
}

/*
 * Get the list of local logical slots that are synchronized from the
 * primary server.
 */
unsafe fn get_local_synced_slots() -> *mut List {
    let mut local_slots: *mut List = NIL;

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    for i in 0..max_replication_slots {
        let s: *mut ReplicationSlot =
            &mut (*(*ReplicationSlotCtl).replication_slots.as_mut_ptr().add(i as usize));

        /* Check if it is a synchronized slot */
        if (*s).in_use && (*s).data.synced {
            Assert!(SlotIsLogical(s));
            local_slots = lappend(local_slots, s as *mut c_void);
        }
    }

    LWLockRelease(ReplicationSlotControlLock);

    local_slots
}

/*
 * Helper function to check if local_slot is required to be retained.
 *
 * Return false either if local_slot does not exist in the remote_slots list
 * or is invalidated while the corresponding remote slot is still valid,
 * otherwise true.
 */
unsafe fn local_sync_slot_required(
    local_slot: *mut ReplicationSlot,
    remote_slots: *mut List,
) -> bool {
    let mut remote_exists = false;
    let mut locally_invalidated = false;

    foreach_ptr!(RemoteSlot, remote_slot, remote_slots, {
        if strcmp((*remote_slot).name, NameStr(&(*local_slot).data.name)) == 0 {
            remote_exists = true;

            /*
             * If remote slot is not invalidated but local slot is marked as
             * invalidated, then set locally_invalidated flag.
             */
            SpinLockAcquire(&mut (*local_slot).mutex);
            locally_invalidated = ((*remote_slot).invalidated == RS_INVAL_NONE)
                && ((*local_slot).data.invalidated != RS_INVAL_NONE);
            SpinLockRelease(&mut (*local_slot).mutex);

            break;
        }
    });

    remote_exists && !locally_invalidated
}

/*
 * Drop local obsolete slots.
 *
 * Drop the local slots that no longer need to be synced i.e. these either do
 * not exist on the primary or are no longer enabled for failover.
 *
 * Additionally, drop any slots that are valid on the primary but got
 * invalidated on the standby. This situation may occur due to the following
 * reasons:
 * - The 'max_slot_wal_keep_size' on the standby is insufficient to retain WAL
 *   records from the restart_lsn of the slot.
 * - 'primary_slot_name' is temporarily reset to null and the physical slot is
 *   removed.
 * These dropped slots will get recreated in next sync-cycle and it is okay to
 * drop and recreate such slots as long as these are not consumable on the
 * standby (which is the case currently).
 *
 * Note: Change of 'wal_level' on the primary server to a level lower than
 * logical may also result in slot invalidation and removal on the standby.
 * This is because such 'wal_level' change is only possible if the logical
 * slots are removed on the primary server, so it's expected to see the
 * slots being invalidated and removed on the standby too (and re-created
 * if they are re-created on the primary server).
 */
unsafe fn drop_local_obsolete_slots(remote_slot_list: *mut List) {
    let local_slots: *mut List = get_local_synced_slots();

    foreach_ptr!(ReplicationSlot, local_slot, local_slots, {
        /* Drop the local slot if it is not required to be retained. */
        if !local_sync_slot_required(local_slot, remote_slot_list) {
            let synced_slot: bool;

            /*
             * Use shared lock to prevent a conflict with
             * ReplicationSlotsDropDBSlots(), trying to drop the same slot
             * during a drop-database operation.
             */
            LockSharedObject(
                DatabaseRelationId,
                (*local_slot).data.database,
                0,
                AccessShareLock,
            );

            /*
             * In the small window between getting the slot to drop and
             * locking the database, there is a possibility of a parallel
             * database drop by the startup process and the creation of a new
             * slot by the user. This new user-created slot may end up using
             * the same shared memory as that of 'local_slot'. Thus check if
             * local_slot is still the synced one before performing actual
             * drop.
             */
            SpinLockAcquire(&mut (*local_slot).mutex);
            synced_slot = (*local_slot).in_use && (*local_slot).data.synced;
            SpinLockRelease(&mut (*local_slot).mutex);

            if synced_slot {
                ReplicationSlotAcquire(NameStr(&(*local_slot).data.name), true, false);
                ReplicationSlotDropAcquired();
            }

            UnlockSharedObject(
                DatabaseRelationId,
                (*local_slot).data.database,
                0,
                AccessShareLock,
            );

            ereport!(
                LOG,
                errmsg!(
                    "dropped replication slot \"{}\" of database with OID {}",
                    CStr::from_ptr(NameStr(&(*local_slot).data.name)).to_string_lossy(),
                    (*local_slot).data.database
                )
            );
        }
    });
}

/*
 * Reserve WAL for the currently active local slot using the specified WAL
 * location (restart_lsn).
 *
 * If the given WAL location has been removed or is at risk of removal,
 * reserve WAL using the oldest segment that is non-removable.
 */
unsafe fn reserve_wal_for_local_slot(restart_lsn: XLogRecPtr) {
    let mut slot_min_lsn: XLogRecPtr;
    let mut min_safe_lsn: XLogRecPtr;
    let mut segno: XLogSegNo = 0;
    let slot: *mut ReplicationSlot = MyReplicationSlot;

    Assert!(!slot.is_null());
    Assert!(!XLogRecPtrIsValid((*slot).data.restart_lsn));

    /*
     * Acquire an exclusive lock to prevent the checkpoint process from
     * concurrently calculating the minimum slot LSN (see
     * CheckPointReplicationSlots), ensuring that if WAL reservation occurs
     * first, the checkpoint must wait for the restart_lsn update before
     * calculating the minimum LSN.
     *
     * Note: Unlike ReplicationSlotReserveWal(), this lock does not protect a
     * newly synced slot from being invalidated if a concurrent checkpoint has
     * invoked CheckPointReplicationSlots() before the WAL reservation here.
     * This can happen because the initial restart_lsn received from the
     * remote server can precede the redo pointer. Therefore, when selecting
     * the initial restart_lsn, we consider using the redo pointer or the
     * minimum slot LSN (if those values are greater than the remote
     * restart_lsn) instead of relying solely on the remote value.
     */
    LWLockAcquire(ReplicationSlotAllocationLock, LW_EXCLUSIVE);

    /*
     * Determine the minimum non-removable LSN by comparing the redo pointer
     * with the minimum slot LSN.
     *
     * The minimum slot LSN is considered because the redo pointer advances at
     * every checkpoint, even when replication slots are present on the
     * standby. In such scenarios, the redo pointer can exceed the remote
     * restart_lsn, while WALs preceding the remote restart_lsn remain
     * protected by a local replication slot.
     */
    min_safe_lsn = GetRedoRecPtr();
    slot_min_lsn = XLogGetReplicationSlotMinimumLSN();

    if XLogRecPtrIsValid(slot_min_lsn) && min_safe_lsn > slot_min_lsn {
        min_safe_lsn = slot_min_lsn;
    }

    /*
     * If the minimum safe LSN is greater than the given restart_lsn, use it
     * as the initial restart_lsn for the newly synced slot. Otherwise, use
     * the given remote restart_lsn.
     */
    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).data.restart_lsn = Max!(restart_lsn, min_safe_lsn);
    SpinLockRelease(&mut (*slot).mutex);

    ReplicationSlotsComputeRequiredLSN();

    XLByteToSeg!((*slot).data.restart_lsn, segno, wal_segment_size);
    if XLogGetLastRemovedSegno() >= segno {
        elog!(
            ERROR,
            "WAL required by replication slot {} has been removed concurrently",
            CStr::from_ptr(NameStr(&(*slot).data.name)).to_string_lossy()
        );
    }

    LWLockRelease(ReplicationSlotAllocationLock);
}

/*
 * If the remote restart_lsn and catalog_xmin have caught up with the
 * local ones, then update the LSNs and persist the local synced slot for
 * future synchronization; otherwise, do nothing.
 *
 * Return true if the slot is marked as RS_PERSISTENT (sync-ready), otherwise
 * false.
 */
unsafe fn update_and_persist_local_synced_slot(
    remote_slot: *mut RemoteSlot,
    remote_dbid: Oid,
) -> bool {
    let slot: *mut ReplicationSlot = MyReplicationSlot;
    let mut found_consistent_snapshot = false;
    let mut remote_slot_precedes = false;

    let _ = update_local_synced_slot(
        remote_slot,
        remote_dbid,
        &mut found_consistent_snapshot,
        &mut remote_slot_precedes,
    );

    /*
     * Check if the primary server has caught up. Refer to the comment atop
     * the file for details on this check.
     */
    if remote_slot_precedes {
        /*
         * The remote slot didn't catch up to locally reserved position.
         *
         * We do not drop the slot because the restart_lsn can be ahead of the
         * current location when recreating the slot in the next cycle. It may
         * take more time to create such a slot. Therefore, we keep this slot
         * and attempt the synchronization in the next cycle.
         */
        return false;
    }

    /*
     * Don't persist the slot if it cannot reach the consistent point from the
     * restart_lsn. See comments atop this file.
     */
    if !found_consistent_snapshot {
        ereport!(
            LOG,
            errmsg!(
                "could not synchronize replication slot \"{}\"",
                CStr::from_ptr((*remote_slot).name).to_string_lossy()
            )
        );
        // C also: errdetail("Synchronization could lead to data loss, because
        //   the standby could not build a consistent snapshot to decode WALs
        //   at LSN %X/%X.", LSN_FORMAT_ARGS(slot->data.restart_lsn));

        return false;
    }

    ReplicationSlotPersist();

    ereport!(
        LOG,
        errmsg!(
            "newly created replication slot \"{}\" is sync-ready now",
            CStr::from_ptr((*remote_slot).name).to_string_lossy()
        )
    );

    true
}

/*
 * Synchronize a single slot to the given position.
 *
 * This creates a new slot if there is no existing one and updates the
 * metadata of the slot as per the data received from the primary server.
 *
 * The slot is created as a temporary slot and stays in the same state until the
 * remote_slot catches up with locally reserved position and local slot is
 * updated. The slot is then persisted and is considered as sync-ready for
 * periodic syncs.
 *
 * Returns TRUE if the local slot is updated.
 */
unsafe fn synchronize_one_slot(remote_slot: *mut RemoteSlot, remote_dbid: Oid) -> bool {
    let mut slot: *mut ReplicationSlot;
    let latestFlushPtr: XLogRecPtr;
    let mut slot_updated = false;

    /*
     * Make sure that concerned WAL is received and flushed before syncing
     * slot to target lsn received from the primary server.
     */
    latestFlushPtr = GetStandbyFlushRecPtr(null_mut());
    if (*remote_slot).confirmed_lsn > latestFlushPtr {
        /*
         * Can get here only if GUC 'synchronized_standby_slots' on the
         * primary server was not configured correctly.
         */
        let _level = if AmLogicalSlotSyncWorkerProcess() { LOG } else { ERROR };
        ereport!(
            _level,
            errmsg!(
                "skipping slot synchronization because the received slot sync LSN {:X}/{:X} for slot \"{}\" is ahead of the standby position {:X}/{:X}",
                LSN_FORMAT_ARGS((*remote_slot).confirmed_lsn).0,
                LSN_FORMAT_ARGS((*remote_slot).confirmed_lsn).1,
                CStr::from_ptr((*remote_slot).name).to_string_lossy(),
                LSN_FORMAT_ARGS(latestFlushPtr).0,
                LSN_FORMAT_ARGS(latestFlushPtr).1
            )
        );
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);

        return false;
    }

    /* Search for the named slot */
    slot = SearchNamedReplicationSlot((*remote_slot).name, true);
    if !slot.is_null() {
        let synced: bool;

        SpinLockAcquire(&mut (*slot).mutex);
        synced = (*slot).data.synced;
        SpinLockRelease(&mut (*slot).mutex);

        /* User-created slot with the same name exists, raise ERROR. */
        if !synced {
            ereport!(
                ERROR,
                errmsg!(
                    "exiting from slot synchronization because same name slot \"{}\" already exists on the standby",
                    CStr::from_ptr((*remote_slot).name).to_string_lossy()
                )
            );
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        }

        /*
         * The slot has been synchronized before.
         *
         * It is important to acquire the slot here before checking
         * invalidation. If we don't acquire the slot first, there could be a
         * race condition that the local slot could be invalidated just after
         * checking the 'invalidated' flag here and we could end up
         * overwriting 'invalidated' flag to remote_slot's value. See
         * InvalidatePossiblyObsoleteSlot() where it invalidates slot directly
         * if the slot is not acquired by other processes.
         *
         * XXX: If it ever turns out that slot acquire/release is costly for
         * cases when none of the slot properties is changed then we can do a
         * pre-check to ensure that at least one of the slot properties is
         * changed before acquiring the slot.
         */
        ReplicationSlotAcquire((*remote_slot).name, true, false);

        Assert!(slot == MyReplicationSlot);

        /*
         * Copy the invalidation cause from remote only if local slot is not
         * invalidated locally, we don't want to overwrite existing one.
         */
        if (*slot).data.invalidated == RS_INVAL_NONE
            && (*remote_slot).invalidated != RS_INVAL_NONE
        {
            SpinLockAcquire(&mut (*slot).mutex);
            (*slot).data.invalidated = (*remote_slot).invalidated;
            SpinLockRelease(&mut (*slot).mutex);

            /* Make sure the invalidated state persists across server restart */
            ReplicationSlotMarkDirty();
            ReplicationSlotSave();

            slot_updated = true;
        }

        /* Skip the sync of an invalidated slot */
        if (*slot).data.invalidated != RS_INVAL_NONE {
            ReplicationSlotRelease();
            return slot_updated;
        }

        /* Slot not ready yet, let's attempt to make it sync-ready now. */
        if (*slot).data.persistency == RS_TEMPORARY {
            slot_updated = update_and_persist_local_synced_slot(remote_slot, remote_dbid);
        }
        /* Slot ready for sync, so sync it. */
        else {
            /*
             * Sanity check: As long as the invalidations are handled
             * appropriately as above, this should never happen.
             *
             * We don't need to check restart_lsn here. See the comments in
             * update_local_synced_slot() for details.
             */
            if (*remote_slot).confirmed_lsn < (*slot).data.confirmed_flush {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot synchronize local slot \"{}\"",
                        CStr::from_ptr((*remote_slot).name).to_string_lossy()
                    )
                );
                // C also: errdetail_internal("Local slot's start streaming
                //   location LSN(%X/%X) is ahead of remote slot's LSN(%X/%X).",
                //   LSN_FORMAT_ARGS(slot->data.confirmed_flush),
                //   LSN_FORMAT_ARGS(remote_slot->confirmed_lsn));
            }

            slot_updated = update_local_synced_slot(remote_slot, remote_dbid, null_mut(), null_mut());
        }
    }
    /* Otherwise create the slot first. */
    else {
        let mut plugin_name: NameData = core::mem::zeroed();
        let mut xmin_horizon: TransactionId = InvalidTransactionId;

        /* Skip creating the local slot if remote_slot is invalidated already */
        if (*remote_slot).invalidated != RS_INVAL_NONE {
            return false;
        }

        /*
         * We create temporary slots instead of ephemeral slots here because
         * we want the slots to survive after releasing them. This is done to
         * avoid dropping and re-creating the slots in each synchronization
         * cycle if the restart_lsn or catalog_xmin of the remote slot has not
         * caught up.
         */
        ReplicationSlotCreate(
            (*remote_slot).name,
            true,
            RS_TEMPORARY,
            (*remote_slot).two_phase,
            (*remote_slot).failover,
            true,
        );

        /* For shorter lines. */
        slot = MyReplicationSlot;

        /* Avoid expensive operations while holding a spinlock. */
        namestrcpy(&mut plugin_name, (*remote_slot).plugin);

        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).data.database = remote_dbid;
        (*slot).data.plugin = plugin_name;
        SpinLockRelease(&mut (*slot).mutex);

        reserve_wal_for_local_slot((*remote_slot).restart_lsn);

        LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        xmin_horizon = GetOldestSafeDecodingTransactionId(true);
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).effective_catalog_xmin = xmin_horizon;
        (*slot).data.catalog_xmin = xmin_horizon;
        SpinLockRelease(&mut (*slot).mutex);
        ReplicationSlotsComputeRequiredXmin(true);
        LWLockRelease(ProcArrayLock);
        LWLockRelease(ReplicationSlotControlLock);

        update_and_persist_local_synced_slot(remote_slot, remote_dbid);

        slot_updated = true;
    }

    ReplicationSlotRelease();

    slot_updated
}

/*
 * Synchronize slots.
 *
 * Gets the failover logical slots info from the primary server and updates
 * the slots locally. Creates the slots if not present on the standby.
 *
 * Returns TRUE if any of the slots gets updated in this sync-cycle.
 */
unsafe fn synchronize_slots(wrconn: *mut WalReceiverConn) -> bool {
    const SLOTSYNC_COLUMN_COUNT: c_int = 10;
    let slotRow: [Oid; SLOTSYNC_COLUMN_COUNT as usize] = [
        TEXTOID, TEXTOID, LSNOID, LSNOID, XIDOID, BOOLOID, LSNOID, BOOLOID, TEXTOID, TEXTOID,
    ];

    let res: *mut WalRcvExecResult;
    let tupslot: *mut TupleTableSlot;
    let mut remote_slot_list: *mut List = NIL;
    let mut some_slot_updated = false;
    let mut started_tx = false;
    let query: *const c_char = c"SELECT slot_name, plugin, confirmed_flush_lsn, restart_lsn, catalog_xmin, two_phase, two_phase_at, failover, database, invalidation_reason FROM pg_catalog.pg_replication_slots WHERE failover and NOT temporary".as_ptr();

    /* The syscache access in walrcv_exec() needs a transaction env. */
    if !IsTransactionState() {
        StartTransactionCommand();
        started_tx = true;
    }

    /* Execute the query */
    res = walrcv_exec(wrconn, query, SLOTSYNC_COLUMN_COUNT, slotRow.as_ptr());
    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(
            ERROR,
            errmsg!(
                "could not fetch failover logical slots info from the primary server: {}",
                CStr::from_ptr((*res).err).to_string_lossy()
            )
        );
    }

    /* Construct the remote_slot tuple and synchronize each slot locally */
    tupslot = MakeSingleTupleTableSlot((*res).tupledesc as TupleDesc, &TTSOpsMinimalTuple as *const _ as *const c_void);
    while tuplestore_gettupleslot((*res).tuplestore as *mut Tuplestorestate, true, false, tupslot) {
        let mut isnull: bool = false;
        let remote_slot: *mut RemoteSlot = palloc0(core::mem::size_of::<RemoteSlot>()) as *mut RemoteSlot;
        let mut d: Datum;
        let mut col: c_int = 0;

        col += 1;
        (*remote_slot).name = TextDatumGetCString(slot_getattr(tupslot, col, &mut isnull));
        Assert!(!isnull);

        col += 1;
        (*remote_slot).plugin = TextDatumGetCString(slot_getattr(tupslot, col, &mut isnull));
        Assert!(!isnull);

        /*
         * It is possible to get null values for LSN and Xmin if slot is
         * invalidated on the primary server, so handle accordingly.
         */
        col += 1;
        d = slot_getattr(tupslot, col, &mut isnull);
        (*remote_slot).confirmed_lsn = if isnull { InvalidXLogRecPtr } else { DatumGetLSN(d) };

        col += 1;
        d = slot_getattr(tupslot, col, &mut isnull);
        (*remote_slot).restart_lsn = if isnull { InvalidXLogRecPtr } else { DatumGetLSN(d) };

        col += 1;
        d = slot_getattr(tupslot, col, &mut isnull);
        (*remote_slot).catalog_xmin = if isnull { InvalidTransactionId } else { DatumGetTransactionId(d) };

        col += 1;
        (*remote_slot).two_phase = DatumGetBool(slot_getattr(tupslot, col, &mut isnull));
        Assert!(!isnull);

        col += 1;
        d = slot_getattr(tupslot, col, &mut isnull);
        (*remote_slot).two_phase_at = if isnull { InvalidXLogRecPtr } else { DatumGetLSN(d) };

        col += 1;
        (*remote_slot).failover = DatumGetBool(slot_getattr(tupslot, col, &mut isnull));
        Assert!(!isnull);

        col += 1;
        (*remote_slot).database = TextDatumGetCString(slot_getattr(tupslot, col, &mut isnull));
        Assert!(!isnull);

        col += 1;
        d = slot_getattr(tupslot, col, &mut isnull);
        (*remote_slot).invalidated = if isnull {
            RS_INVAL_NONE
        } else {
            GetSlotInvalidationCause(TextDatumGetCString(d))
        };

        /* Sanity check */
        Assert!(col == SLOTSYNC_COLUMN_COUNT);

        /*
         * If restart_lsn, confirmed_lsn or catalog_xmin is invalid but the
         * slot is valid, that means we have fetched the remote_slot in its
         * RS_EPHEMERAL state. In such a case, don't sync it; we can always
         * sync it in the next sync cycle when the remote_slot is persisted
         * and has valid lsn(s) and xmin values.
         *
         * XXX: In future, if we plan to expose 'slot->data.persistency' in
         * pg_replication_slots view, then we can avoid fetching RS_EPHEMERAL
         * slots in the first place.
         */
        if (XLogRecPtrIsInvalid((*remote_slot).restart_lsn)
            || XLogRecPtrIsInvalid((*remote_slot).confirmed_lsn)
            || !TransactionIdIsValid((*remote_slot).catalog_xmin))
            && (*remote_slot).invalidated == RS_INVAL_NONE
        {
            pfree(remote_slot as *mut c_void);
        } else {
            /* Create list of remote slots */
            remote_slot_list = lappend(remote_slot_list, remote_slot as *mut c_void);
        }

        ExecClearTuple(tupslot);
    }

    /* Drop local slots that no longer need to be synced. */
    drop_local_obsolete_slots(remote_slot_list);

    /* Now sync the slots locally */
    foreach_ptr!(RemoteSlot, remote_slot, remote_slot_list, {
        let remote_dbid: Oid = get_database_oid((*remote_slot).database, false);

        /*
         * Use shared lock to prevent a conflict with
         * ReplicationSlotsDropDBSlots(), trying to drop the same slot during
         * a drop-database operation.
         */
        LockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock);

        some_slot_updated |= synchronize_one_slot(remote_slot, remote_dbid);

        UnlockSharedObject(DatabaseRelationId, remote_dbid, 0, AccessShareLock);
    });

    /* We are done, free remote_slot_list elements */
    list_free_deep(remote_slot_list);

    walrcv_clear_result(res);

    if started_tx {
        CommitTransactionCommand();
    }

    some_slot_updated
}

/*
 * Checks the remote server info.
 *
 * We ensure that the 'primary_slot_name' exists on the remote server and the
 * remote server is not a standby node.
 */
unsafe fn validate_remote_info(wrconn: *mut WalReceiverConn) {
    const PRIMARY_INFO_OUTPUT_COL_COUNT: c_int = 2;
    let res: *mut WalRcvExecResult;
    let slotRow: [Oid; PRIMARY_INFO_OUTPUT_COL_COUNT as usize] = [BOOLOID, BOOLOID];
    let mut cmd: StringInfoData = core::mem::zeroed();
    let mut isnull: bool = false;
    let tupslot: *mut TupleTableSlot;
    let remote_in_recovery: bool;
    let primary_slot_valid: bool;
    let mut started_tx = false;

    initStringInfo(&mut cmd);
    appendStringInfo_fmt(
        &mut cmd,
        // appendStringInfo(&cmd,
        //   "SELECT pg_is_in_recovery(), count(*) = 1"
        //   " FROM pg_catalog.pg_replication_slots"
        //   " WHERE slot_type='physical' AND slot_name=%s",
        //   quote_literal_cstr(PrimarySlotName));
        quote_literal_cstr(PrimarySlotName),
    );

    /* The syscache access in walrcv_exec() needs a transaction env. */
    if !IsTransactionState() {
        StartTransactionCommand();
        started_tx = true;
    }

    res = walrcv_exec(wrconn, cmd.data, PRIMARY_INFO_OUTPUT_COL_COUNT, slotRow.as_ptr());
    pfree(cmd.data as *mut c_void);

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(
            ERROR,
            errmsg!(
                "could not fetch primary slot name \"{}\" info from the primary server: {}",
                CStr::from_ptr(PrimarySlotName).to_string_lossy(),
                CStr::from_ptr((*res).err).to_string_lossy()
            )
        );
        // C also: errhint("Check if \"primary_slot_name\" is configured correctly.");
    }

    tupslot = MakeSingleTupleTableSlot((*res).tupledesc as TupleDesc, &TTSOpsMinimalTuple as *const _ as *const c_void);
    if !tuplestore_gettupleslot((*res).tuplestore as *mut Tuplestorestate, true, false, tupslot) {
        elog!(
            ERROR,
            "failed to fetch tuple for the primary server slot specified by \"primary_slot_name\""
        );
    }

    remote_in_recovery = DatumGetBool(slot_getattr(tupslot, 1, &mut isnull));
    Assert!(!isnull);

    /*
     * Slot sync is currently not supported on a cascading standby. This is
     * because if we allow it, the primary server needs to wait for all the
     * cascading standbys, otherwise, logical subscribers can still be ahead
     * of one of the cascading standbys which we plan to promote. Thus, to
     * avoid this additional complexity, we restrict it for the time being.
     */
    if remote_in_recovery {
        ereport!(
            ERROR,
            errmsg!("cannot synchronize replication slots from a standby server")
        );
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    }

    primary_slot_valid = DatumGetBool(slot_getattr(tupslot, 2, &mut isnull));
    Assert!(!isnull);

    if !primary_slot_valid {
        /* translator: second %s is a GUC variable name */
        ereport!(
            ERROR,
            errmsg!(
                "replication slot \"{}\" specified by \"{}\" does not exist on primary server",
                CStr::from_ptr(PrimarySlotName).to_string_lossy(),
                "primary_slot_name"
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
    }

    ExecClearTuple(tupslot);
    walrcv_clear_result(res);

    if started_tx {
        CommitTransactionCommand();
    }
}

/*
 * Checks if dbname is specified in 'primary_conninfo'.
 *
 * Error out if not specified otherwise return it.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CheckAndGetDbnameFromConninfo() -> *mut c_char {
    let dbname: *mut c_char;

    /*
     * The slot synchronization needs a database connection for walrcv_exec to
     * work.
     */
    dbname = walrcv_get_dbname_from_conninfo(PrimaryConnInfo);
    if dbname.is_null() {
        /*
         * translator: first %s is a connection option; second %s is a GUC
         * variable name
         */
        ereport!(
            ERROR,
            errmsg!(
                "replication slot synchronization requires \"{}\" to be specified in \"{}\"",
                "dbname",
                "primary_conninfo"
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
    }
    dbname
}

/*
 * Return true if all necessary GUCs for slot synchronization are set
 * appropriately, otherwise, return false.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ValidateSlotSyncParams(elevel: c_int) -> bool {
    /*
     * Logical slot sync/creation requires wal_level >= logical.
     */
    if wal_level < WAL_LEVEL_LOGICAL {
        ereport!(
            elevel,
            errmsg!("replication slot synchronization requires \"wal_level\" >= \"logical\"")
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        return false;
    }

    /*
     * A physical replication slot(primary_slot_name) is required on the
     * primary to ensure that the rows needed by the standby are not removed
     * after restarting, so that the synchronized slot on the standby will not
     * be invalidated.
     */
    if PrimarySlotName.is_null() || *PrimarySlotName == 0 {
        /* translator: %s is a GUC variable name */
        ereport!(
            elevel,
            errmsg!(
                "replication slot synchronization requires \"{}\" to be set",
                "primary_slot_name"
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        return false;
    }

    /*
     * hot_standby_feedback must be enabled to cooperate with the physical
     * replication slot, which allows informing the primary about the xmin and
     * catalog_xmin values on the standby.
     */
    if !hot_standby_feedback {
        /* translator: %s is a GUC variable name */
        ereport!(
            elevel,
            errmsg!(
                "replication slot synchronization requires \"{}\" to be enabled",
                "hot_standby_feedback"
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        return false;
    }

    /*
     * The primary_conninfo is required to make connection to primary for
     * getting slots information.
     */
    if PrimaryConnInfo.is_null() || *PrimaryConnInfo == 0 {
        /* translator: %s is a GUC variable name */
        ereport!(
            elevel,
            errmsg!(
                "replication slot synchronization requires \"{}\" to be set",
                "primary_conninfo"
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        return false;
    }

    true
}

/*
 * Re-read the config file.
 *
 * Exit if any of the slot sync GUCs have changed. The postmaster will
 * restart it.
 */
unsafe fn slotsync_reread_config() {
    let old_primary_conninfo: *mut c_char = pstrdup(PrimaryConnInfo);
    let old_primary_slotname: *mut c_char = pstrdup(PrimarySlotName);
    let old_sync_replication_slots: bool = sync_replication_slots;
    let old_hot_standby_feedback: bool = hot_standby_feedback;
    let conninfo_changed: bool;
    let primary_slotname_changed: bool;

    Assert!(sync_replication_slots);

    ConfigReloadPending = false;
    ProcessConfigFile(crate::utils::misc::guc::GucContext::PGC_SIGHUP);

    conninfo_changed = strcmp(old_primary_conninfo, PrimaryConnInfo) != 0;
    primary_slotname_changed = strcmp(old_primary_slotname, PrimarySlotName) != 0;
    pfree(old_primary_conninfo as *mut c_void);
    pfree(old_primary_slotname as *mut c_void);

    if old_sync_replication_slots != sync_replication_slots {
        /* translator: %s is a GUC variable name */
        ereport!(
            LOG,
            errmsg!(
                "replication slot synchronization worker will shut down because \"{}\" is disabled",
                "sync_replication_slots"
            )
        );
        proc_exit(0);
    }

    if conninfo_changed
        || primary_slotname_changed
        || (old_hot_standby_feedback != hot_standby_feedback)
    {
        ereport!(
            LOG,
            errmsg!("replication slot synchronization worker will restart because of a parameter change")
        );

        /*
         * Reset the last-start time for this worker so that the postmaster
         * can restart it without waiting for SLOTSYNC_RESTART_INTERVAL_SEC.
         */
        (*SlotSyncCtx).last_start_time = 0;

        proc_exit(0);
    }
}

/*
 * Interrupt handler for main loop of slot sync worker.
 */
unsafe fn ProcessSlotSyncInterrupts(_wrconn: *mut WalReceiverConn) {
    CHECK_FOR_INTERRUPTS!();

    if (*SlotSyncCtx).stopSignaled {
        ereport!(
            LOG,
            errmsg!("replication slot synchronization worker is shutting down because promotion is triggered")
        );

        proc_exit(0);
    }

    if ConfigReloadPending {
        slotsync_reread_config();
    }
}

/*
 * Connection cleanup function for slotsync worker.
 *
 * Called on slotsync worker exit.
 */
unsafe extern "C" fn slotsync_worker_disconnect(_code: c_int, arg: Datum) {
    let wrconn: *mut WalReceiverConn = DatumGetPointer(arg) as *mut WalReceiverConn;

    walrcv_disconnect(wrconn);
}

/*
 * Cleanup function for slotsync worker.
 *
 * Called on slotsync worker exit.
 */
unsafe extern "C" fn slotsync_worker_onexit(_code: c_int, _arg: Datum) {
    /*
     * We need to do slots cleanup here just like WalSndErrorCleanup() does.
     *
     * The startup process during promotion invokes ShutDownSlotSync() which
     * waits for slot sync to finish and it does that by checking the
     * 'syncing' flag. Thus the slot sync worker must be done with slots'
     * release and cleanup to avoid any dangling temporary slots or active
     * slots before it marks itself as finished syncing.
     */

    /* Make sure active replication slots are released */
    if !MyReplicationSlot.is_null() {
        ReplicationSlotRelease();
    }

    /* Also cleanup the temporary slots. */
    ReplicationSlotCleanup(false);

    SpinLockAcquire(&mut (*SlotSyncCtx).mutex);

    (*SlotSyncCtx).pid = InvalidPid;

    /*
     * If syncing_slots is true, it indicates that the process errored out
     * without resetting the flag. So, we need to clean up shared memory and
     * reset the flag here.
     */
    if syncing_slots {
        (*SlotSyncCtx).syncing = false;
        syncing_slots = false;
    }

    SpinLockRelease(&mut (*SlotSyncCtx).mutex);
}

/*
 * Sleep for long enough that we believe it's likely that the slots on primary
 * get updated.
 *
 * If there is no slot activity the wait time between sync-cycles will double
 * (to a maximum of 30s). If there is some slot activity the wait time between
 * sync-cycles is reset to the minimum (200ms).
 */
unsafe fn wait_for_slot_activity(some_slot_updated: bool) {
    let rc: c_int;

    if !some_slot_updated {
        /*
         * No slots were updated, so double the sleep time, but not beyond the
         * maximum allowable value.
         */
        sleep_ms = Min!(sleep_ms * 2, MAX_SLOTSYNC_WORKER_NAPTIME_MS);
    } else {
        /*
         * Some slots were updated since the last sleep, so reset the sleep
         * time.
         */
        sleep_ms = MIN_SLOTSYNC_WORKER_NAPTIME_MS;
    }

    rc = WaitLatch(
        MyLatch as *mut c_void,
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        sleep_ms,
        WAIT_EVENT_REPLICATION_SLOTSYNC_MAIN,
    );

    if rc & WL_LATCH_SET != 0 {
        ResetLatch(MyLatch as *mut c_void);
    }
}

/*
 * Emit an error if a promotion or a concurrent sync call is in progress.
 * Otherwise, advertise that a sync is in progress.
 */
unsafe fn check_and_set_sync_info(worker_pid: pid_t) {
    SpinLockAcquire(&mut (*SlotSyncCtx).mutex);

    /* The worker pid must not be already assigned in SlotSyncCtx */
    Assert!(worker_pid == InvalidPid || (*SlotSyncCtx).pid == InvalidPid);

    /*
     * Emit an error if startup process signaled the slot sync machinery to
     * stop. See comments atop SlotSyncCtxStruct.
     */
    if (*SlotSyncCtx).stopSignaled {
        SpinLockRelease(&mut (*SlotSyncCtx).mutex);
        ereport!(
            ERROR,
            errmsg!("cannot synchronize replication slots when standby promotion is ongoing")
        );
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    }

    if (*SlotSyncCtx).syncing {
        SpinLockRelease(&mut (*SlotSyncCtx).mutex);
        ereport!(
            ERROR,
            errmsg!("cannot synchronize replication slots concurrently")
        );
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    }

    (*SlotSyncCtx).syncing = true;

    /*
     * Advertise the required PID so that the startup process can kill the
     * slot sync worker on promotion.
     */
    (*SlotSyncCtx).pid = worker_pid;

    SpinLockRelease(&mut (*SlotSyncCtx).mutex);

    syncing_slots = true;
}

/*
 * Reset syncing flag.
 */
unsafe fn reset_syncing_flag() {
    SpinLockAcquire(&mut (*SlotSyncCtx).mutex);
    (*SlotSyncCtx).syncing = false;
    SpinLockRelease(&mut (*SlotSyncCtx).mutex);

    syncing_slots = false;
}

/*
 * The main loop of our worker process.
 *
 * It connects to the primary server, fetches logical failover slots
 * information periodically in order to create and sync the slots.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ReplSlotSyncWorkerMain(_startup_data: *const c_void, startup_data_len: usize) {
    let mut wrconn: *mut WalReceiverConn = null_mut();
    let dbname: *mut c_char;
    let mut err: *mut c_char = null_mut();
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let mut app_name: StringInfoData = core::mem::zeroed();

    Assert!(startup_data_len == 0);

    MyBackendType = B_SLOTSYNC_WORKER;

    init_ps_display(null());

    Assert!(GetProcessingMode() == InitProcessing);

    /*
     * Create a per-backend PGPROC struct in shared memory.  We must do this
     * before we access any shared memory.
     */
    InitProcess();

    /*
     * Early initialization.
     */
    BaseInit();

    Assert!(!SlotSyncCtx.is_null());

    /*
     * If an exception is encountered, processing resumes here.
     *
     * We just need to clean up, report the error, and go away.
     *
     * If we do not have this handling here, then since this worker process
     * operates at the bottom of the exception stack, ERRORs turn into FATALs.
     * Therefore, we create our own exception handler to catch ERRORs.
     */
    if sigsetjmp(&raw mut local_sigjmp_buf, 1) != 0 {
        /* since not using PG_TRY, must reset error stack by hand */
        error_context_stack = null_mut();

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * We can now go away.  Note that because we called InitProcess, a
         * callback was registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = (&raw mut local_sigjmp_buf) as *mut c_void;

    /* Setup signal handling */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGINT, Some(StatementCancelHandler));
    pqsignal(SIGTERM, Some(die));
    pqsignal(SIGFPE, Some(FloatExceptionHandler));
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, SIG_IGN());
    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGCHLD, SIG_DFL);

    check_and_set_sync_info(MyProcPid);

    ereport!(LOG, errmsg!("slot sync worker started"));

    /* Register it as soon as SlotSyncCtx->pid is initialized. */
    before_shmem_exit(slotsync_worker_onexit, 0 as Datum);

    /*
     * Establishes SIGALRM handler and initialize timeout module. It is needed
     * by InitPostgres to register different timeouts.
     */
    InitializeTimeouts();

    /* Load the libpq-specific functions */
    load_file(c"libpqwalreceiver".as_ptr(), false);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, null_mut::<sigset_t>());

    /*
     * Set always-secure search path, so malicious users can't redirect user
     * code (e.g. operators).
     *
     * It's not strictly necessary since we won't be scanning or writing to
     * any user table locally, but it's good to retain it here for added
     * precaution.
     */
    SetConfigOption(c"search_path".as_ptr(), c"".as_ptr(), crate::utils::misc::guc::GucContext::PGC_SUSET, crate::utils::misc::guc::GucSource::PGC_S_OVERRIDE);

    dbname = CheckAndGetDbnameFromConninfo();

    /*
     * Connect to the database specified by the user in primary_conninfo. We
     * need a database connection for walrcv_exec to work which we use to
     * fetch slot information from the remote node. See comments atop
     * libpqrcv_exec.
     *
     * We do not specify a specific user here since the slot sync worker will
     * operate as a superuser. This is safe because the slot sync worker does
     * not interact with user tables, eliminating the risk of executing
     * arbitrary code within triggers.
     */
    InitPostgres(dbname, InvalidOid, null(), InvalidOid, 0, null_mut());

    SetProcessingMode(NormalProcessing);

    initStringInfo(&mut app_name);
    if cluster_name[0] != 0 {
        // appendStringInfo(&app_name, "%s_%s", cluster_name, "slotsync worker");
        appendStringInfo_fmt(&mut app_name, cluster_name.as_ptr());
        appendStringInfoString(&mut app_name, c"_slotsync worker".as_ptr());
    } else {
        appendStringInfoString(&mut app_name, c"slotsync worker".as_ptr());
    }

    /*
     * Establish the connection to the primary server for slot
     * synchronization.
     */
    wrconn = walrcv_connect(PrimaryConnInfo, false, false, false, app_name.data, &mut err);

    if wrconn.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "synchronization worker \"{}\" could not connect to the primary server: {}",
                CStr::from_ptr(app_name.data).to_string_lossy(),
                CStr::from_ptr(err).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_CONNECTION_FAILURE);
    }

    pfree(app_name.data as *mut c_void);

    /*
     * Register the disconnection callback.
     *
     * XXX: This can be combined with previous cleanup registration of
     * slotsync_worker_onexit() but that will need the connection to be made
     * global and we want to avoid introducing global for this purpose.
     */
    before_shmem_exit(slotsync_worker_disconnect, PointerGetDatum(wrconn as *mut c_void));

    /*
     * Using the specified primary server connection, check that we are not a
     * cascading standby and slot configured in 'primary_slot_name' exists on
     * the primary server.
     */
    validate_remote_info(wrconn);

    /* Main loop to synchronize slots */
    loop {
        let mut some_slot_updated = false;

        ProcessSlotSyncInterrupts(wrconn);

        some_slot_updated = synchronize_slots(wrconn);

        wait_for_slot_activity(some_slot_updated);
    }

    /*
     * The slot sync worker can't get here because it will only stop when it
     * receives a stop request from the startup process, or when there is an
     * error.
     */
    // Assert(false);  // unreachable after infinite loop
}

/*
 * Update the inactive_since property for synced slots.
 *
 * Note that this function is currently called when we shutdown the slot
 * sync machinery.
 */
unsafe fn update_synced_slots_inactive_since() {
    let mut now: TimestampTz = 0;

    /*
     * We need to update inactive_since only when we are promoting standby to
     * correctly interpret the inactive_since if the standby gets promoted
     * without a restart. We don't want the slots to appear inactive for a
     * long time after promotion if they haven't been synchronized recently.
     * Whoever acquires the slot, i.e., makes the slot active, will reset it.
     */
    if !StandbyMode {
        return;
    }

    /* The slot sync worker or SQL function mustn't be running by now */
    Assert!(((*SlotSyncCtx).pid == InvalidPid) && !(*SlotSyncCtx).syncing);

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

    for i in 0..max_replication_slots {
        let s: *mut ReplicationSlot =
            &mut (*(*ReplicationSlotCtl).replication_slots.as_mut_ptr().add(i as usize));

        /* Check if it is a synchronized slot */
        if (*s).in_use && (*s).data.synced {
            Assert!(SlotIsLogical(s));

            /* The slot must not be acquired by any process */
            Assert!((*s).active_pid == 0);

            /* Use the same inactive_since time for all the slots. */
            if now == 0 {
                now = GetCurrentTimestamp();
            }

            ReplicationSlotSetInactiveSince(s, now, true);
        }
    }

    LWLockRelease(ReplicationSlotControlLock);
}

/*
 * Shut down the slot sync worker.
 *
 * This function sends signal to shutdown slot sync worker, if required. It
 * also waits till the slot sync worker has exited or
 * pg_sync_replication_slots() has finished.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ShutDownSlotSync() {
    let worker_pid: pid_t;

    SpinLockAcquire(&mut (*SlotSyncCtx).mutex);

    (*SlotSyncCtx).stopSignaled = true;

    /*
     * Return if neither the slot sync worker is running nor the function
     * pg_sync_replication_slots() is executing.
     */
    if !(*SlotSyncCtx).syncing {
        SpinLockRelease(&mut (*SlotSyncCtx).mutex);
        update_synced_slots_inactive_since();
        return;
    }

    worker_pid = (*SlotSyncCtx).pid;

    SpinLockRelease(&mut (*SlotSyncCtx).mutex);

    /*
     * Signal slotsync worker if it was still running. The worker will stop
     * upon detecting that the stopSignaled flag is set to true.
     */
    if worker_pid != InvalidPid {
        kill(worker_pid, SIGUSR1);
    }

    /* Wait for slot sync to end */
    loop {
        let rc: c_int;

        /* Wait a bit, we don't expect to have to wait long */
        rc = WaitLatch(
            MyLatch as *mut c_void,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN,
        );

        if rc & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch as *mut c_void);
            CHECK_FOR_INTERRUPTS!();
        }

        SpinLockAcquire(&mut (*SlotSyncCtx).mutex);

        /* Ensure that no process is syncing the slots. */
        if !(*SlotSyncCtx).syncing {
            break;
        }

        SpinLockRelease(&mut (*SlotSyncCtx).mutex);
    }

    SpinLockRelease(&mut (*SlotSyncCtx).mutex);

    update_synced_slots_inactive_since();
}

/*
 * SlotSyncWorkerCanRestart
 *
 * Returns true if enough time (SLOTSYNC_RESTART_INTERVAL_SEC) has passed
 * since it was launched last. Otherwise returns false.
 *
 * This is a safety valve to protect against continuous respawn attempts if the
 * worker is dying immediately at launch. Note that since we will retry to
 * launch the worker from the postmaster main loop, we will get another
 * chance later.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SlotSyncWorkerCanRestart() -> bool {
    let curtime: time_t = time(null_mut());

    /* Return false if too soon since last start. */
    if ((curtime - (*SlotSyncCtx).last_start_time) as u32)
        < SLOTSYNC_RESTART_INTERVAL_SEC as u32
    {
        return false;
    }

    (*SlotSyncCtx).last_start_time = curtime;

    true
}

/*
 * Is current process syncing replication slots?
 *
 * Could be either backend executing SQL function or slot sync worker.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IsSyncingReplicationSlots() -> bool {
    syncing_slots
}

/*
 * Amount of shared memory required for slot synchronization.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SlotSyncShmemSize() -> Size {
    core::mem::size_of::<SlotSyncCtxStruct>() as Size
}

/*
 * Allocate and initialize the shared memory of slot synchronization.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SlotSyncShmemInit() {
    let size: Size = SlotSyncShmemSize();
    let mut found: bool = false;

    SlotSyncCtx =
        ShmemInitStruct(c"Slot Sync Data".as_ptr(), size, &mut found) as *mut SlotSyncCtxStruct;

    if !found {
        core::ptr::write_bytes(SlotSyncCtx as *mut u8, 0, size as usize);
        (*SlotSyncCtx).pid = InvalidPid;
        SpinLockInit(&mut (*SlotSyncCtx).mutex);
    }
}

/*
 * Error cleanup callback for slot sync SQL function.
 */
unsafe extern "C" fn slotsync_failure_callback(_code: c_int, arg: Datum) {
    let wrconn: *mut WalReceiverConn = DatumGetPointer(arg) as *mut WalReceiverConn;

    /*
     * We need to do slots cleanup here just like WalSndErrorCleanup() does.
     *
     * The startup process during promotion invokes ShutDownSlotSync() which
     * waits for slot sync to finish and it does that by checking the
     * 'syncing' flag. Thus the SQL function must be done with slots' release
     * and cleanup to avoid any dangling temporary slots or active slots
     * before it marks itself as finished syncing.
     */

    /* Make sure active replication slots are released */
    if !MyReplicationSlot.is_null() {
        ReplicationSlotRelease();
    }

    /* Also cleanup the synced temporary slots. */
    ReplicationSlotCleanup(true);

    /*
     * The set syncing_slots indicates that the process errored out without
     * resetting the flag. So, we need to clean up shared memory and reset the
     * flag here.
     */
    if syncing_slots {
        reset_syncing_flag();
    }

    walrcv_disconnect(wrconn);
}

/*
 * Synchronize the failover enabled replication slots using the specified
 * primary server connection.
 */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SyncReplicationSlots(wrconn: *mut WalReceiverConn) {
    // PG_ENSURE_ERROR_CLEANUP(slotsync_failure_callback, PointerGetDatum(wrconn));
    // TODO(pg-port): wrap the body below with the real PG_ENSURE_ERROR_CLEANUP /
    // PG_END_ENSURE_ERROR_CLEANUP macro pair (utils/error/elog.h) once ported.
    {
        check_and_set_sync_info(InvalidPid);

        validate_remote_info(wrconn);

        synchronize_slots(wrconn);

        /* Cleanup the synced temporary slots */
        ReplicationSlotCleanup(true);

        /* We are done with sync, so reset sync flag */
        reset_syncing_flag();
    }
    // PG_END_ENSURE_ERROR_CLEANUP(slotsync_failure_callback, PointerGetDatum(wrconn));
    let _ = slotsync_failure_callback; // referenced by the cleanup macro above
}

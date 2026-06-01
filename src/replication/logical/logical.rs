/*-------------------------------------------------------------------------
 * logical.rs
 *   PostgreSQL logical decoding coordination
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/replication/logical/logical.c
 *
 * NOTES
 *   This file coordinates interaction between the various modules that
 *   together provide logical decoding, primarily by providing so
 *   called LogicalDecodingContexts. The goal is to encapsulate most of the
 *   internal complexity for consumers of logical decoding, so they can
 *   create and consume a changestream with a low amount of code. Builtin
 *   consumers are the walsender and SQL SRF interface, but it's possible to
 *   add further ones without changing core code, e.g. to consume changes in
 *   a bgworker.
 *
 *   The idea is that a consumer provides three callbacks, one to read WAL,
 *   one to prepare a data write, and a final one for actually writing since
 *   their implementation depends on the type of consumer.  Check
 *   logicalfuncs.c for an example implementation of a fairly simple consumer
 *   and an implementation of a WAL reading callback that's suitable for
 *   simple consumers.
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, LSN_FORMAT_ARGS, XLogRecPtr};
use crate::access::transam::xlogreader::{
    XLogBeginRead, XLogReaderAllocate, XLogReaderFree, XLogReaderRoutine, XLogReaderState,
    XLogReadRecord, XLogRecord,
};
use crate::access::transam::{InvalidTransactionId, TransactionIdIsValid};
use crate::access::transam::transam::TransactionIdPrecedesOrEquals;
use crate::c::{int64, NameData, NameStr, Name, TransactionId};
use crate::lib::stringinfo::{makeStringInfo, StringInfoData};
use crate::nodes::pg_list::List;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::replication::logical::decode::LogicalDecodingProcessRecord;
use crate::replication::logical::reorderbuffer::{
    ReorderBuffer, ReorderBufferAllocate, ReorderBufferFree, ReorderBufferTXN,
    ReorderBufferChange,
};
use crate::replication::logical::snapbuild::{
    AllocateSnapshotBuilder, FreeSnapshotBuilder, SnapBuild, SnapBuildCurrentState,
    SnapBuildSetTwoPhaseAt, SNAPBUILD_CONSISTENT,
};
use crate::replication::output_plugin::{OutputPluginCallbacks, OutputPluginOptions};
use crate::replication::slotfuncs::RS_INVAL_NONE;
use crate::replication::slot::{
    CheckSlotRequirements, MyReplicationSlot, ReplicationSlot,
    ReplicationSlotsComputeRequiredLSN, ReplicationSlotsComputeRequiredXmin,
    ReplicationSlotMarkDirty, ReplicationSlotReserveWal, ReplicationSlotSave, SlotIsPhysical,
    };
use crate::storage::ipc::procarray::{
    GetOldestSafeDecodingTransactionId, MyProc, ProcGlobal,
    PROC_IN_LOGICAL_DECODING,
};
use crate::storage::spin::{SpinLockAcquire, SpinLockRelease};
use crate::utils::activity::pgstat::{
    LWLock, LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, PgStat_StatReplSlotEntry,
};
use crate::utils::activity::pgstat_replslot::pgstat_report_replslot;
use crate::utils::builtins::namestrcpy;
use crate::utils::rel::Relation;
use crate::utils::resowner::resowner::{CurrentResourceOwner, ResourceOwner};
use crate::access::transam::xlogdefs::RepOriginId;

// ---------------------------------------------------------------------------
// Callback typedefs (replication/logical.h)
// ---------------------------------------------------------------------------

/// Called to prepare an output plugin write.
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterPrepareWrite = unsafe fn(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    last_write: bool,
);

/// Called to emit an output plugin write.
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterWrite = unsafe fn(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    last_write: bool,
);

/// Called to report progress for a transaction.
#[allow(non_camel_case_types)]
pub type LogicalOutputPluginWriterUpdateProgress = unsafe fn(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    skipped_xact: bool,
);

// ---------------------------------------------------------------------------
// LogicalDecodingContext  (replication/logical.h)
//
// This module owns the authoritative layout.  Stubs in decode.rs and
// logicalfuncs.rs project partial views; we provide the real struct here.
// ---------------------------------------------------------------------------

/// Full LogicalDecodingContext, layout-compatible with the C struct.
#[repr(C)]
pub struct LogicalDecodingContext {
    /// Memory context owned by the decoding context.
    pub context: MemoryContext,
    /// Output plugin callbacks populated by the plugin's init function.
    pub callbacks: OutputPluginCallbacks,
    /// Options set by the output plugin during startup.
    pub options: OutputPluginOptions,
    /// Replication slot associated with this decoding context.
    pub slot: *mut ReplicationSlot,
    /// Snapshot builder state.
    pub snapshot_builder: *mut SnapBuild,
    /// Reorder buffer that reassembles transactions.
    pub reorder: *mut ReorderBuffer,
    /// Underlying WAL reader.
    pub reader: *mut XLogReaderState,
    /// Whether we are fast-forwarding without generating changes.
    pub fast_forward: bool,
    /// Whether two-phase (prepared) transaction decoding is active.
    pub twophase: bool,
    /// Whether two_phase was requested through the streaming start option.
    pub twophase_opt_given: bool,
    /// Set true when a record would have been processed if not for fast_forward.
    pub processing_required: bool,
    /// Whether at least one streaming callback was registered.
    pub streaming: bool,
    /// Whether writes are accepted in the current callback invocation.
    pub accept_writes: bool,
    /// Whether OutputPluginPrepareWrite has been called but Write has not.
    pub prepared_write: bool,
    /// Whether we are at the end of a transaction.
    pub end_xact: bool,
    /// XID of the transaction currently being output.
    pub write_xid: TransactionId,
    /// LSN for the change currently being output.
    pub write_location: XLogRecPtr,
    /// Output buffer.
    pub out: *mut StringInfoData,
    /// Private data for write callbacks (tuplestore, walsender buf, etc.).
    pub output_writer_private: *mut c_void,
    /// Private data for the output plugin.
    pub output_plugin_private: *mut c_void,
    /// Options forwarded to the output plugin.
    pub output_plugin_options: *mut List,
    /// Callback that prepares a write.
    pub prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    /// Callback that performs the write.
    pub write: Option<LogicalOutputPluginWriterWrite>,
    /// Callback that reports progress.
    pub update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
}

// ---------------------------------------------------------------------------
// Local helper types
// ---------------------------------------------------------------------------

/// Data for errcontext callback while inside an output plugin callback.
struct LogicalErrorCallbackState {
    ctx: *mut LogicalDecodingContext,
    callback_name: &'static str,
    report_location: XLogRecPtr,
}

/// PostgreSQL ErrorContextCallback node.
/// TODO(pg-port): real definition lives in utils/error/elog.h
#[repr(C)]
struct ErrorContextCallback {
    callback: Option<unsafe fn(*mut c_void)>,
    arg: *mut c_void,
    previous: *mut ErrorContextCallback,
}

/// Head of the per-backend error context callback stack.
/// TODO(pg-port): real error_context_stack lives in utils/error/elog.c
static mut error_context_stack: *mut ErrorContextCallback = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// Stubs for symbols not yet ported to Rust
// ---------------------------------------------------------------------------

/// Load an output plugin and call its _PG_output_plugin_init entry point.
/// TODO(pg-port): real load_external_function lives in utils/fmgr/dfmgr.c
unsafe fn load_external_function(
    _filename: *const c_char,
    _funcname: *const c_char,
    _signal_not_found: bool,
    _filehandle: *mut *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real load_external_function lives in utils/fmgr/dfmgr.c
}

/// Return the WAL level currently active on a standby.
unsafe fn GetActiveWalLevelOnStandby() -> c_int {
    unimplemented!() // TODO(pg-port): real GetActiveWalLevelOnStandby lives in access/transam/xlogrecovery.c
}

/// GUC wal_level; 2 == logical.
pub static mut wal_level: c_int = 0; // TODO(pg-port): GUC, access/xlog.c
pub const WAL_LEVEL_LOGICAL: c_int = 2; // access/xlog.h

/// True when the backend is in a transaction or transaction block.
unsafe fn IsTransactionOrTransactionBlock() -> bool {
    unimplemented!() // TODO(pg-port): real IsTransactionOrTransactionBlock lives in access/transam/xact.c
}

/// True when we are inside a transaction.
unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO(pg-port): real IsTransactionState lives in access/transam/xact.c
}

/// Return the XID of the top-level transaction if one has been assigned.
unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    unimplemented!() // TODO(pg-port): real GetTopTransactionIdIfAny lives in access/transam/xact.c
}

/// True when the server is in recovery (standby) mode.
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!() // TODO(pg-port): real RecoveryInProgress lives in access/transam/xlog.c
}

/// True when replication slot syncing is underway.
unsafe fn IsSyncingReplicationSlots() -> bool {
    unimplemented!() // TODO(pg-port): real IsSyncingReplicationSlots lives in replication/slotsync.c
}

/// Invalidate all non-timetravel catalog caches.
unsafe fn InvalidateSystemCaches() {
    unimplemented!() // TODO(pg-port): real InvalidateSystemCaches lives in utils/cache/inval.c
}

/// Wait until all specified standbys have confirmed receipt up to moveto.
unsafe fn WaitForStandbyConfirmation(_moveto: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real WaitForStandbyConfirmation lives in replication/walsender.c
}

/// Local WAL page-read callback (XLogReaderRoutine.page_read).
/// TODO(pg-port): real read_local_xlog_page lives in access/transam/xlogutils.c
pub use crate::access::transam::xlogutils::{
    read_local_xlog_page, wal_segment_close, wal_segment_open,
};

/// wal_segment_size GUC.
pub static mut wal_segment_size: c_int = 16 * 1024 * 1024; // TODO(pg-port): GUC

/// OID of the current database (miscadmin.h).
pub static mut MyDatabaseId: Oid = InvalidOid; // TODO(pg-port): utils/init/globals.c

/// Flag cleared on transaction abort (logical streaming state).
pub static mut bsysscan: bool = false; // TODO(pg-port): replication/logical/logical.c

/// ReplicationSlotControlLock (replication/slot.c).
static mut ReplicationSlotControlLock: *mut LWLock = core::ptr::null_mut(); // TODO(pg-port)

/// ProcArrayLock: protects shared proc array. (storage/lmgr/procarray.c)
static mut ProcArrayLock: *mut LWLock = core::ptr::null_mut(); // TODO(pg-port)

/// CHECK_FOR_INTERRUPTS: inline no-op until the real macro is ported.
#[inline(always)]
unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): real CHECK_FOR_INTERRUPTS is a macro in miscadmin.h
}

// ---------------------------------------------------------------------------
// Internal: output plugin loader
// ---------------------------------------------------------------------------

/// Load the output plugin, lookup its init symbol, and validate required callbacks.
unsafe fn LoadOutputPlugin(callbacks: *mut OutputPluginCallbacks, plugin: *const c_char) {
    /*
     * Load the output plugin.
     */
    type LogicalOutputPluginInit =
        unsafe extern "C" fn(cb: *mut OutputPluginCallbacks);

    let plugin_init_raw =
        load_external_function(plugin, c"_PG_output_plugin_init".as_ptr(), false, core::ptr::null_mut());

    if plugin_init_raw.is_null() {
        elog!(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");
    }

    let plugin_init: LogicalOutputPluginInit =
        core::mem::transmute(plugin_init_raw); // TODO(pg-port): load_external_function stub

    /* ask the output plugin to fill the callback struct */
    plugin_init(callbacks);

    if (*callbacks).begin_cb.is_none() {
        elog!(ERROR, "output plugins have to register a begin callback");
    }
    if (*callbacks).change_cb.is_none() {
        elog!(ERROR, "output plugins have to register a change callback");
    }
    if (*callbacks).commit_cb.is_none() {
        elog!(ERROR, "output plugins have to register a commit callback");
    }
}

// ---------------------------------------------------------------------------
// Internal: errcontext callback
// ---------------------------------------------------------------------------

unsafe fn output_plugin_error_callback(arg: *mut c_void) {
    let state = arg as *const LogicalErrorCallbackState;

    /* not all callbacks have an associated LSN */
    if (*state).report_location != InvalidXLogRecPtr {
        // TODO(pg-port): real errcontext macro in utils/error/elog.h adds context;
        // here we log at LOG level as an approximation.
        let (hi, lo) = LSN_FORMAT_ARGS((*state).report_location);
        let slot_name = core::ffi::CStr::from_ptr(NameStr(&(*(*(*state).ctx).slot).data.name))
            .to_string_lossy();
        let plugin_name = core::ffi::CStr::from_ptr(NameStr(&(*(*(*state).ctx).slot).data.plugin))
            .to_string_lossy();
        elog!(
            LOG,
            "slot \"{}\", output plugin \"{}\", in the {} callback, associated LSN {:X}/{:X}",
            slot_name, plugin_name, (*state).callback_name, hi, lo
        );
    } else {
        let slot_name = core::ffi::CStr::from_ptr(NameStr(&(*(*(*state).ctx).slot).data.name))
            .to_string_lossy();
        let plugin_name = core::ffi::CStr::from_ptr(NameStr(&(*(*(*state).ctx).slot).data.plugin))
            .to_string_lossy();
        elog!(
            LOG,
            "slot \"{}\", output plugin \"{}\", in the {} callback",
            slot_name, plugin_name, (*state).callback_name
        );
    }
}

// ---------------------------------------------------------------------------
// CheckLogicalDecodingRequirements
// ---------------------------------------------------------------------------

/*
 * Make sure the current settings & environment are capable of doing logical
 * decoding.
 */
pub unsafe fn CheckLogicalDecodingRequirements() {
    CheckSlotRequirements();

    /*
     * NB: Adding a new requirement likely means that RestoreSlotFromDisk()
     * needs the same check.
     */

    if wal_level < WAL_LEVEL_LOGICAL {
        ereport!(
            ERROR,
            errmsg!("logical decoding requires \"wal_level\" >= \"logical\"")
        );
    }

    if MyDatabaseId == InvalidOid {
        ereport!(
            ERROR,
            errmsg!("logical decoding requires a database connection")
        );
    }

    if RecoveryInProgress() {
        /*
         * This check may have race conditions, but whenever
         * XLOG_PARAMETER_CHANGE indicates that wal_level has changed, we
         * verify that there are no existing logical replication slots. And to
         * avoid races around creating a new slot,
         * CheckLogicalDecodingRequirements() is called once before creating
         * the slot, and once when logical decoding is initially starting up.
         */
        if GetActiveWalLevelOnStandby() < WAL_LEVEL_LOGICAL {
            ereport!(
                ERROR,
                errmsg!("logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary")
            );
        }
    }
}

// ---------------------------------------------------------------------------
// StartupDecodingContext  (internal helper)
// ---------------------------------------------------------------------------

/*
 * Helper function for CreateInitDecodingContext() and
 * CreateDecodingContext() performing common tasks.
 */
unsafe fn StartupDecodingContext(
    output_plugin_options: *mut List,
    start_lsn: XLogRecPtr,
    xmin_horizon: TransactionId,
    need_full_snapshot: bool,
    fast_forward: bool,
    in_create: bool,
    xl_routine: *mut XLogReaderRoutine,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> *mut LogicalDecodingContext {
    let slot: *mut ReplicationSlot;
    let context: MemoryContext;
    let old_context: MemoryContext;
    let ctx: *mut LogicalDecodingContext;

    /* shorter lines... */
    slot = MyReplicationSlot;

    context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Logical decoding context",
        ALLOCSET_DEFAULT_SIZES
    ) as MemoryContext;
    old_context = MemoryContextSwitchTo(context);
    ctx = palloc0(core::mem::size_of::<LogicalDecodingContext>()) as *mut LogicalDecodingContext;

    (*ctx).context = context;

    /*
     * (re-)load output plugins, so we detect a bad (removed) output plugin
     * now.
     */
    if !fast_forward {
        LoadOutputPlugin(&mut (*ctx).callbacks, NameStr(&(*slot).data.plugin));
    }

    /*
     * Now that the slot's xmin has been set, we can announce ourselves as a
     * logical decoding backend which doesn't need to be checked individually
     * when computing the xmin horizon because the xmin is enforced via
     * replication slots.
     *
     * We can only do so if we're outside of a transaction (i.e. the case when
     * streaming changes via walsender), otherwise an already setup
     * snapshot/xid would end up being ignored. That's not a particularly
     * bothersome restriction since the SQL interface can't be used for
     * streaming anyway.
     */
    if !IsTransactionOrTransactionBlock() {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        (*MyProc).statusFlags |= PROC_IN_LOGICAL_DECODING;
        (*(*ProcGlobal).statusFlags.add((*MyProc).pgxactoff as usize)) = (*MyProc).statusFlags;
        LWLockRelease(ProcArrayLock);
    }

    (*ctx).slot = slot;

    (*ctx).reader = XLogReaderAllocate(wal_segment_size, core::ptr::null_mut(), xl_routine, ctx as *mut c_void);
    if (*ctx).reader.is_null() {
        ereport!(
            ERROR,
            errmsg!("out of memory: Failed while allocating a WAL reading processor.")
        );
    }

    (*ctx).reorder = ReorderBufferAllocate();
    (*ctx).snapshot_builder = AllocateSnapshotBuilder(
        (*ctx).reorder as *mut core::ffi::c_void,
        xmin_horizon,
        start_lsn,
        need_full_snapshot,
        in_create,
        (*slot).data.two_phase_at,
    );

    (*(*ctx).reorder).private_data = ctx as *mut c_void;

    /* wrap output plugin callbacks, so we can add error context information */
    (*(*ctx).reorder).begin = Some(core::mem::transmute(begin_cb_wrapper as usize));
    (*(*ctx).reorder).apply_change = Some(core::mem::transmute(change_cb_wrapper as usize));
    (*(*ctx).reorder).apply_truncate = Some(core::mem::transmute(truncate_cb_wrapper as usize));
    (*(*ctx).reorder).commit = Some(core::mem::transmute(commit_cb_wrapper as usize));
    (*(*ctx).reorder).message = Some(core::mem::transmute(message_cb_wrapper as usize));

    /*
     * To support streaming, we require start/stop/abort/commit/change
     * callbacks. The message and truncate callbacks are optional, similar to
     * regular output plugins. We however enable streaming when at least one
     * of the methods is enabled so that we can easily identify missing
     * methods.
     *
     * We decide it here, but only check it later in the wrappers.
     */
    (*ctx).streaming = (*ctx).callbacks.stream_start_cb.is_some()
        || (*ctx).callbacks.stream_stop_cb.is_some()
        || (*ctx).callbacks.stream_abort_cb.is_some()
        || (*ctx).callbacks.stream_commit_cb.is_some()
        || (*ctx).callbacks.stream_change_cb.is_some()
        || (*ctx).callbacks.stream_message_cb.is_some()
        || (*ctx).callbacks.stream_truncate_cb.is_some();

    /*
     * streaming callbacks
     *
     * stream_message and stream_truncate callbacks are optional, so we do not
     * fail with ERROR when missing, but the wrappers simply do nothing. We
     * must set the ReorderBuffer callbacks to something, otherwise the calls
     * from there will crash (we don't want to move the checks there).
     */
    (*(*ctx).reorder).stream_start = Some(core::mem::transmute(stream_start_cb_wrapper as usize));
    (*(*ctx).reorder).stream_stop = Some(core::mem::transmute(stream_stop_cb_wrapper as usize));
    (*(*ctx).reorder).stream_abort = Some(core::mem::transmute(stream_abort_cb_wrapper as usize));
    (*(*ctx).reorder).stream_prepare = Some(core::mem::transmute(stream_prepare_cb_wrapper as usize));
    (*(*ctx).reorder).stream_commit = Some(core::mem::transmute(stream_commit_cb_wrapper as usize));
    (*(*ctx).reorder).stream_change = Some(core::mem::transmute(stream_change_cb_wrapper as usize));
    (*(*ctx).reorder).stream_message = Some(core::mem::transmute(stream_message_cb_wrapper as usize));
    (*(*ctx).reorder).stream_truncate = Some(core::mem::transmute(stream_truncate_cb_wrapper as usize));

    /*
     * To support two-phase logical decoding, we require
     * begin_prepare/prepare/commit-prepare/abort-prepare callbacks. The
     * filter_prepare callback is optional. We however enable two-phase
     * logical decoding when at least one of the methods is enabled so that we
     * can easily identify missing methods.
     *
     * We decide it here, but only check it later in the wrappers.
     */
    (*ctx).twophase = (*ctx).callbacks.begin_prepare_cb.is_some()
        || (*ctx).callbacks.prepare_cb.is_some()
        || (*ctx).callbacks.commit_prepared_cb.is_some()
        || (*ctx).callbacks.rollback_prepared_cb.is_some()
        || (*ctx).callbacks.stream_prepare_cb.is_some()
        || (*ctx).callbacks.filter_prepare_cb.is_some();

    /*
     * Callback to support decoding at prepare time.
     */
    (*(*ctx).reorder).begin_prepare = Some(core::mem::transmute(begin_prepare_cb_wrapper as usize));
    (*(*ctx).reorder).prepare = Some(core::mem::transmute(prepare_cb_wrapper as usize));
    (*(*ctx).reorder).commit_prepared = Some(core::mem::transmute(commit_prepared_cb_wrapper as usize));
    (*(*ctx).reorder).rollback_prepared = Some(core::mem::transmute(rollback_prepared_cb_wrapper as usize));

    /*
     * Callback to support updating progress during sending data of a
     * transaction (and its subtransactions) to the output plugin.
     */
    (*(*ctx).reorder).update_progress_txn = Some(core::mem::transmute(update_progress_txn_cb_wrapper as usize));

    (*ctx).out = makeStringInfo();
    (*ctx).prepare_write = prepare_write;
    (*ctx).write = do_write;
    (*ctx).update_progress = update_progress;

    (*ctx).output_plugin_options = output_plugin_options;

    (*ctx).fast_forward = fast_forward;

    MemoryContextSwitchTo(old_context);

    ctx
}

// ---------------------------------------------------------------------------
// CreateInitDecodingContext
// ---------------------------------------------------------------------------

/*
 * Create a new decoding context, for a new logical slot.
 *
 * plugin -- contains the name of the output plugin
 * output_plugin_options -- contains options passed to the output plugin
 * need_full_snapshot -- if true, must obtain a snapshot able to read all
 *		tables; if false, one that can read only catalogs is acceptable.
 * restart_lsn -- if given as invalid, it's this routine's responsibility to
 *		mark WAL as reserved by setting a convenient restart_lsn for the slot.
 *		Otherwise, we set for decoding to start from the given LSN without
 *		marking WAL reserved beforehand.  In that scenario, it's up to the
 *		caller to guarantee that WAL remains available.
 * xl_routine -- XLogReaderRoutine for underlying XLogReader
 * prepare_write, do_write, update_progress --
 *		callbacks that perform the use-case dependent, actual, work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
pub unsafe fn CreateInitDecodingContext(
    plugin: *const c_char,
    output_plugin_options: *mut List,
    need_full_snapshot: bool,
    restart_lsn: XLogRecPtr,
    xl_routine: *mut XLogReaderRoutine,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> *mut LogicalDecodingContext {
    let mut xmin_horizon: TransactionId = InvalidTransactionId;
    let slot: *mut ReplicationSlot;
    let mut plugin_name: NameData = NameData { data: [0; 64] };
    let ctx: *mut LogicalDecodingContext;
    let old_context: MemoryContext;

    /*
     * On a standby, this check is also required while creating the slot.
     * Check the comments in the function.
     */
    CheckLogicalDecodingRequirements();

    /* shorter lines... */
    slot = MyReplicationSlot;

    /* first some sanity checks that are unlikely to be violated */
    if slot.is_null() {
        elog!(ERROR, "cannot perform logical decoding without an acquired slot");
    }

    if plugin.is_null() {
        elog!(ERROR, "cannot initialize logical decoding without a specified plugin");
    }

    /* Make sure the passed slot is suitable. These are user facing errors. */
    if SlotIsPhysical(slot) {
        ereport!(
            ERROR,
            errmsg!("cannot use physical replication slot for logical decoding")
        );
    }

    if (*slot).data.database != MyDatabaseId {
        ereport!(
            ERROR,
            errmsg!(
                "replication slot \"{}\" was not created in this database",
                core::ffi::CStr::from_ptr(NameStr(&(*slot).data.name)).to_string_lossy()
            )
        );
    }

    if IsTransactionState()
        && GetTopTransactionIdIfAny() != InvalidTransactionId
    {
        ereport!(
            ERROR,
            errmsg!("cannot create logical replication slot in transaction that has performed writes")
        );
    }

    /*
     * Register output plugin name with slot.  We need the mutex to avoid
     * concurrent reading of a partially copied string.  But we don't want any
     * complicated code while holding a spinlock, so do namestrcpy() outside.
     */
    namestrcpy(&mut plugin_name as *mut NameData as Name, plugin);
    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).data.plugin = plugin_name;
    SpinLockRelease(&mut (*slot).mutex);

    if restart_lsn == InvalidXLogRecPtr {
        ReplicationSlotReserveWal();
    } else {
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).data.restart_lsn = restart_lsn;
        SpinLockRelease(&mut (*slot).mutex);
    }

    /* ----
     * This is a bit tricky: We need to determine a safe xmin horizon to start
     * decoding from, to avoid starting from a running xacts record referring
     * to xids whose rows have been vacuumed or pruned
     * already. GetOldestSafeDecodingTransactionId() returns such a value, but
     * without further interlock its return value might immediately be out of
     * date.
     *
     * So we have to acquire both the ReplicationSlotControlLock and the
     * ProcArrayLock to prevent concurrent computation and update of new xmin
     * horizons by other backends, get the safe decoding xid, and inform the
     * slot machinery about the new limit. Once that's done both locks can be
     * released as the slot machinery now is protecting against vacuum.
     *
     * Note that, temporarily, the data, not just the catalog, xmin has to be
     * reserved if a data snapshot is to be exported.  Otherwise the initial
     * data snapshot created here is not guaranteed to be valid. After that
     * the data xmin doesn't need to be managed anymore and the global xmin
     * should be recomputed. As we are fine with losing the pegged data xmin
     * after crash - no chance a snapshot would get exported anymore - we can
     * get away with just setting the slot's
     * effective_xmin. ReplicationSlotRelease will reset it again.
     *
     * ----
     */
    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    xmin_horizon = GetOldestSafeDecodingTransactionId(!need_full_snapshot);

    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).effective_catalog_xmin = xmin_horizon;
    (*slot).data.catalog_xmin = xmin_horizon;
    if need_full_snapshot {
        (*slot).effective_xmin = xmin_horizon;
    }
    SpinLockRelease(&mut (*slot).mutex);

    ReplicationSlotsComputeRequiredXmin(true);

    LWLockRelease(ProcArrayLock);
    LWLockRelease(ReplicationSlotControlLock);

    ReplicationSlotMarkDirty();
    ReplicationSlotSave();

    ctx = StartupDecodingContext(
        core::ptr::null_mut(), // NIL
        restart_lsn,
        xmin_horizon,
        need_full_snapshot,
        false,
        true,
        xl_routine,
        prepare_write,
        do_write,
        update_progress,
    );

    /* call output plugin initialization callback */
    old_context = MemoryContextSwitchTo((*ctx).context);
    if (*ctx).callbacks.startup_cb.is_some() {
        startup_cb_wrapper(ctx, &mut (*ctx).options, true);
    }
    MemoryContextSwitchTo(old_context);

    /*
     * We allow decoding of prepared transactions when the two_phase is
     * enabled at the time of slot creation, or when the two_phase option is
     * given at the streaming start, provided the plugin supports all the
     * callbacks for two-phase.
     */
    (*ctx).twophase &= (*slot).data.two_phase;

    (*(*ctx).reorder).output_rewrites = (*ctx).options.receive_rewrites;

    ctx
}

// ---------------------------------------------------------------------------
// CreateDecodingContext
// ---------------------------------------------------------------------------

/*
 * Create a new decoding context, for a logical slot that has previously been
 * used already.
 *
 * start_lsn
 *		The LSN at which to start decoding.  If InvalidXLogRecPtr, restart
 *		from the slot's confirmed_flush; otherwise, start from the specified
 *		location (but move it forwards to confirmed_flush if it's older than
 *		that, see below).
 *
 * output_plugin_options
 *		options passed to the output plugin.
 *
 * fast_forward
 *		bypass the generation of logical changes.
 *
 * xl_routine
 *		XLogReaderRoutine used by underlying xlogreader
 *
 * prepare_write, do_write, update_progress
 *		callbacks that have to be filled to perform the use-case dependent,
 *		actual work.
 *
 * Needs to be called while in a memory context that's at least as long lived
 * as the decoding context because further memory contexts will be created
 * inside it.
 *
 * Returns an initialized decoding context after calling the output plugin's
 * startup function.
 */
pub unsafe fn CreateDecodingContext(
    mut start_lsn: XLogRecPtr,
    output_plugin_options: *mut List,
    fast_forward: bool,
    xl_routine: *mut XLogReaderRoutine,
    prepare_write: Option<LogicalOutputPluginWriterPrepareWrite>,
    do_write: Option<LogicalOutputPluginWriterWrite>,
    update_progress: Option<LogicalOutputPluginWriterUpdateProgress>,
) -> *mut LogicalDecodingContext {
    let ctx: *mut LogicalDecodingContext;
    let slot: *mut ReplicationSlot;
    let old_context: MemoryContext;

    /* shorter lines... */
    slot = MyReplicationSlot;

    /* first some sanity checks that are unlikely to be violated */
    if slot.is_null() {
        elog!(ERROR, "cannot perform logical decoding without an acquired slot");
    }

    /* make sure the passed slot is suitable, these are user facing errors */
    if SlotIsPhysical(slot) {
        ereport!(
            ERROR,
            errmsg!("cannot use physical replication slot for logical decoding")
        );
    }

    /*
     * We need to access the system tables during decoding to build the
     * logical changes unless we are in fast_forward mode where no changes are
     * generated.
     */
    if (*slot).data.database != MyDatabaseId && !fast_forward {
        ereport!(
            ERROR,
            errmsg!(
                "replication slot \"{}\" was not created in this database",
                core::ffi::CStr::from_ptr(NameStr(&(*slot).data.name)).to_string_lossy()
            )
        );
    }

    /*
     * The slots being synced from the primary can't be used for decoding as
     * they are used after failover. However, we do allow advancing the LSNs
     * during the synchronization of slots. See update_local_synced_slot.
     */
    if RecoveryInProgress() && (*slot).data.synced && !IsSyncingReplicationSlots() {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use replication slot \"{}\" for logical decoding",
                core::ffi::CStr::from_ptr(NameStr(&(*slot).data.name)).to_string_lossy()
            )
        );
    }

    /* slot must be valid to allow decoding */
    Assert!((*slot).data.invalidated == RS_INVAL_NONE);
    Assert!((*slot).data.restart_lsn != InvalidXLogRecPtr);

    if start_lsn == InvalidXLogRecPtr {
        /* continue from last position */
        start_lsn = (*slot).data.confirmed_flush;
    } else if start_lsn < (*slot).data.confirmed_flush {
        /*
         * It might seem like we should error out in this case, but it's
         * pretty common for a client to acknowledge a LSN it doesn't have to
         * do anything for, and thus didn't store persistently, because the
         * xlog records didn't result in anything relevant for logical
         * decoding. Clients have to be able to do that to support synchronous
         * replication.
         *
         * Starting at a different LSN than requested might not catch certain
         * kinds of client errors; so the client may wish to check that
         * confirmed_flush_lsn matches its expectations.
         */
        let (s_hi, s_lo) = LSN_FORMAT_ARGS(start_lsn);
        let (c_hi, c_lo) = LSN_FORMAT_ARGS((*slot).data.confirmed_flush);
        elog!(
            LOG,
            "{:X}/{:X} has been already streamed, forwarding to {:X}/{:X}",
            s_hi, s_lo, c_hi, c_lo
        );

        start_lsn = (*slot).data.confirmed_flush;
    }

    ctx = StartupDecodingContext(
        output_plugin_options,
        start_lsn,
        InvalidTransactionId,
        false,
        fast_forward,
        false,
        xl_routine,
        prepare_write,
        do_write,
        update_progress,
    );

    /* call output plugin initialization callback */
    old_context = MemoryContextSwitchTo((*ctx).context);
    if (*ctx).callbacks.startup_cb.is_some() {
        startup_cb_wrapper(ctx, &mut (*ctx).options, false);
    }
    MemoryContextSwitchTo(old_context);

    /*
     * We allow decoding of prepared transactions when the two_phase is
     * enabled at the time of slot creation, or when the two_phase option is
     * given at the streaming start, provided the plugin supports all the
     * callbacks for two-phase.
     */
    (*ctx).twophase &= (*slot).data.two_phase || (*ctx).twophase_opt_given;

    /* Mark slot to allow two_phase decoding if not already marked */
    if (*ctx).twophase && !(*slot).data.two_phase {
        SpinLockAcquire(&mut (*slot).mutex);
        (*slot).data.two_phase = true;
        (*slot).data.two_phase_at = start_lsn;
        SpinLockRelease(&mut (*slot).mutex);
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
        SnapBuildSetTwoPhaseAt((*ctx).snapshot_builder, start_lsn);
    }

    (*(*ctx).reorder).output_rewrites = (*ctx).options.receive_rewrites;

    {
        let (cf_hi, cf_lo) = LSN_FORMAT_ARGS((*slot).data.confirmed_flush);
        let (rl_hi, rl_lo) = LSN_FORMAT_ARGS((*slot).data.restart_lsn);
        ereport!(
            LOG,
            errmsg!(
                "starting logical decoding for slot \"{}\"",
                core::ffi::CStr::from_ptr(NameStr(&(*slot).data.name)).to_string_lossy()
            )
        );
        // errdetail is not a standalone ereport! variant; log separately.
        elog!(
            LOG,
            "streaming transactions committing after {:X}/{:X}, reading WAL from {:X}/{:X}",
            cf_hi, cf_lo, rl_hi, rl_lo
        );
    }

    ctx
}

// ---------------------------------------------------------------------------
// DecodingContextReady / DecodingContextFindStartpoint / FreeDecodingContext
// ---------------------------------------------------------------------------

/*
 * Returns true if a consistent initial decoding snapshot has been built.
 */
pub unsafe fn DecodingContextReady(ctx: *mut LogicalDecodingContext) -> bool {
    SnapBuildCurrentState((*ctx).snapshot_builder) == SNAPBUILD_CONSISTENT
}

/*
 * Read from the decoding slot, until it is ready to start extracting changes.
 */
pub unsafe fn DecodingContextFindStartpoint(ctx: *mut LogicalDecodingContext) {
    let slot: *mut ReplicationSlot = (*ctx).slot;

    /* Initialize from where to start reading WAL. */
    XLogBeginRead((*ctx).reader, (*slot).data.restart_lsn);

    {
        let (hi, lo) = LSN_FORMAT_ARGS((*slot).data.restart_lsn);
        elog!(
            DEBUG1,
            "searching for logical decoding starting point, starting at {:X}/{:X}",
            hi, lo
        );
    }

    /* Wait for a consistent starting point */
    loop {
        let record: *mut XLogRecord;
        let mut err: *mut c_char = core::ptr::null_mut();

        /* the read_page callback waits for new WAL */
        record = XLogReadRecord((*ctx).reader, &mut err);
        if !err.is_null() {
            elog!(
                ERROR,
                "could not find logical decoding starting point: {}",
                core::ffi::CStr::from_ptr(err).to_string_lossy()
            );
        }
        if record.is_null() {
            elog!(ERROR, "could not find logical decoding starting point");
        }

        LogicalDecodingProcessRecord(ctx as *mut crate::replication::logical::decode::LogicalDecodingContext, (*ctx).reader);

        /* only continue till we found a consistent spot */
        if DecodingContextReady(ctx) {
            break;
        }

        CHECK_FOR_INTERRUPTS();
    }

    SpinLockAcquire(&mut (*slot).mutex);
    (*slot).data.confirmed_flush = (*(*ctx).reader).EndRecPtr;
    if (*slot).data.two_phase {
        (*slot).data.two_phase_at = (*(*ctx).reader).EndRecPtr;
    }
    SpinLockRelease(&mut (*slot).mutex);
}

/*
 * Free a previously allocated decoding context, invoking the shutdown
 * callback if necessary.
 */
pub unsafe fn FreeDecodingContext(ctx: *mut LogicalDecodingContext) {
    if (*ctx).callbacks.shutdown_cb.is_some() {
        shutdown_cb_wrapper(ctx);
    }

    ReorderBufferFree((*ctx).reorder);
    FreeSnapshotBuilder((*ctx).snapshot_builder);
    XLogReaderFree((*ctx).reader);
    MemoryContextDelete((*ctx).context);
}

// ---------------------------------------------------------------------------
// OutputPluginPrepareWrite / OutputPluginWrite / OutputPluginUpdateProgress
// ---------------------------------------------------------------------------

/*
 * Prepare a write using the context's output routine.
 */
pub unsafe fn OutputPluginPrepareWrite(ctx: *mut LogicalDecodingContext, last_write: bool) {
    if !(*ctx).accept_writes {
        elog!(ERROR, "writes are only accepted in commit, begin and change callbacks");
    }

    if let Some(pw) = (*ctx).prepare_write {
        pw(ctx, (*ctx).write_location, (*ctx).write_xid, last_write);
    }
    (*ctx).prepared_write = true;
}

/*
 * Perform a write using the context's output routine.
 */
pub unsafe fn OutputPluginWrite(ctx: *mut LogicalDecodingContext, last_write: bool) {
    if !(*ctx).prepared_write {
        elog!(ERROR, "OutputPluginPrepareWrite needs to be called before OutputPluginWrite");
    }

    if let Some(w) = (*ctx).write {
        w(ctx, (*ctx).write_location, (*ctx).write_xid, last_write);
    }
    (*ctx).prepared_write = false;
}

/*
 * Update progress tracking (if supported).
 */
pub unsafe fn OutputPluginUpdateProgress(
    ctx: *mut LogicalDecodingContext,
    skipped_xact: bool,
) {
    if (*ctx).update_progress.is_none() {
        return;
    }

    if let Some(up) = (*ctx).update_progress {
        up(ctx, (*ctx).write_location, (*ctx).write_xid, skipped_xact);
    }
}

// ---------------------------------------------------------------------------
// Callback wrappers: startup / shutdown
// ---------------------------------------------------------------------------

unsafe fn startup_cb_wrapper(
    ctx: *mut LogicalDecodingContext,
    opt: *mut OutputPluginOptions,
    is_init: bool,
) {
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "startup",
        report_location: InvalidXLogRecPtr,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = false;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.startup_cb {
        cb(ctx as *mut c_void, opt, is_init);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn shutdown_cb_wrapper(ctx: *mut LogicalDecodingContext) {
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "shutdown",
        report_location: InvalidXLogRecPtr,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = false;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.shutdown_cb {
        cb(ctx as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

// ---------------------------------------------------------------------------
// Callbacks for ReorderBuffer: begin / commit
// ---------------------------------------------------------------------------

/*
 * Callbacks for ReorderBuffer which add in some more information and then call
 * output_plugin.h plugins.
 */
unsafe fn begin_cb_wrapper(cache: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "begin",
        report_location: (*txn).first_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).first_lsn;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.begin_cb {
        cb(ctx as *mut c_void, txn as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn commit_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "commit",
        report_location: (*txn).final_lsn, /* beginning of commit record */
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn; /* points to the end of the record */
    (*ctx).end_xact = true;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.commit_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, commit_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

// ---------------------------------------------------------------------------
// Callbacks for ReorderBuffer: two-phase (begin_prepare / prepare /
//   commit_prepared / rollback_prepared)
// ---------------------------------------------------------------------------

/*
 * The functionality of begin_prepare is quite similar to begin with the
 * exception that this will have gid (global transaction id) information which
 * can be used by plugin. Now, we thought about extending the existing begin
 * but that would break the replication protocol and additionally this looks
 * cleaner.
 */
unsafe fn begin_prepare_cb_wrapper(cache: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "begin_prepare",
        report_location: (*txn).first_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when two-phase commits are supported */
    Assert!((*ctx).twophase);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).first_lsn;
    (*ctx).end_xact = false;

    /*
     * If the plugin supports two-phase commits then begin prepare callback is
     * mandatory
     */
    if (*ctx).callbacks.begin_prepare_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "logical replication at prepare time requires a {} callback",
                "begin_prepare_cb"
            )
        );
    }

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.begin_prepare_cb {
        cb(ctx as *mut c_void, txn as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn prepare_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "prepare",
        report_location: (*txn).final_lsn, /* beginning of prepare record */
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when two-phase commits are supported */
    Assert!((*ctx).twophase);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn; /* points to the end of the record */
    (*ctx).end_xact = true;

    /*
     * If the plugin supports two-phase commits then prepare callback is
     * mandatory
     */
    if (*ctx).callbacks.prepare_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "logical replication at prepare time requires a {} callback",
                "prepare_cb"
            )
        );
    }

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.prepare_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, prepare_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn commit_prepared_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "commit_prepared",
        report_location: (*txn).final_lsn, /* beginning of commit record */
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when two-phase commits are supported */
    Assert!((*ctx).twophase);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn; /* points to the end of the record */
    (*ctx).end_xact = true;

    /*
     * If the plugin support two-phase commits then commit prepared callback
     * is mandatory
     */
    if (*ctx).callbacks.commit_prepared_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "logical replication at prepare time requires a {} callback",
                "commit_prepared_cb"
            )
        );
    }

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.commit_prepared_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, commit_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn rollback_prepared_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: i64, /* TimestampTz */
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "rollback_prepared",
        report_location: (*txn).final_lsn, /* beginning of commit record */
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when two-phase commits are supported */
    Assert!((*ctx).twophase);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn; /* points to the end of the record */
    (*ctx).end_xact = true;

    /*
     * If the plugin support two-phase commits then rollback prepared callback
     * is mandatory
     */
    if (*ctx).callbacks.rollback_prepared_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "logical replication at prepare time requires a {} callback",
                "rollback_prepared_cb"
            )
        );
    }

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.rollback_prepared_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, prepare_end_lsn, prepare_time);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

// ---------------------------------------------------------------------------
// Callbacks for ReorderBuffer: change / truncate / message
// ---------------------------------------------------------------------------

unsafe fn change_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "change",
        report_location: (*change).lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this change's lsn so replies from clients can give an up-to-date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    (*ctx).write_location = (*change).lsn;

    (*ctx).end_xact = false;

    if let Some(cb) = (*ctx).callbacks.change_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, relation, change as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn truncate_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    nrelations: c_int,
    relations: *mut Relation,
    change: *mut ReorderBufferChange,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;

    Assert!(!(*ctx).fast_forward);

    if (*ctx).callbacks.truncate_cb.is_none() {
        return;
    }

    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "truncate",
        report_location: (*change).lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this change's lsn so replies from clients can give an up-to-date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    (*ctx).write_location = (*change).lsn;

    (*ctx).end_xact = false;

    if let Some(cb) = (*ctx).callbacks.truncate_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, nrelations, relations, change as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

pub unsafe fn filter_prepare_cb_wrapper(
    ctx: *mut LogicalDecodingContext,
    xid: TransactionId,
    gid: *const c_char,
) -> bool {
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "filter_prepare",
        report_location: InvalidXLogRecPtr,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };
    let ret: bool;

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = false;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    ret = if let Some(cb) = (*ctx).callbacks.filter_prepare_cb {
        cb(ctx as *mut c_void, xid, gid)
    } else {
        false
    };

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    ret
}

pub unsafe fn filter_by_origin_cb_wrapper(
    ctx: *mut LogicalDecodingContext,
    origin_id: RepOriginId,
) -> bool {
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "filter_by_origin",
        report_location: InvalidXLogRecPtr,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };
    let ret: bool;

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = false;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    ret = if let Some(cb) = (*ctx).callbacks.filter_by_origin_cb {
        cb(ctx as *mut c_void, origin_id)
    } else {
        false
    };

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    ret
}

unsafe fn message_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    message_size: usize,
    message: *const c_char,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;

    Assert!(!(*ctx).fast_forward);

    if (*ctx).callbacks.message_cb.is_none() {
        return;
    }

    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "message",
        report_location: message_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = if txn.is_null() {
        InvalidTransactionId
    } else {
        (*txn).xid
    };
    (*ctx).write_location = message_lsn;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.message_cb {
        cb(
            ctx as *mut c_void,
            txn as *mut c_void,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        );
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

// ---------------------------------------------------------------------------
// Streaming callbacks
// ---------------------------------------------------------------------------

unsafe fn stream_start_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    first_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_start",
        report_location: first_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this message's lsn so replies from clients can give an
     * up-to-date answer. This won't ever be enough (and shouldn't be!) to
     * confirm receipt of this transaction, but it might allow another
     * transaction's commit to be confirmed with one message.
     */
    (*ctx).write_location = first_lsn;

    (*ctx).end_xact = false;

    /* in streaming mode, stream_start_cb is required */
    if (*ctx).callbacks.stream_start_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("logical streaming requires a {} callback", "stream_start_cb")
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_start_cb {
        cb(ctx as *mut c_void, txn as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_stop_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    last_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_stop",
        report_location: last_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this message's lsn so replies from clients can give an
     * up-to-date answer. This won't ever be enough (and shouldn't be!) to
     * confirm receipt of this transaction, but it might allow another
     * transaction's commit to be confirmed with one message.
     */
    (*ctx).write_location = last_lsn;

    (*ctx).end_xact = false;

    /* in streaming mode, stream_stop_cb is required */
    if (*ctx).callbacks.stream_stop_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("logical streaming requires a {} callback", "stream_stop_cb")
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_stop_cb {
        cb(ctx as *mut c_void, txn as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_abort_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    abort_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_abort",
        report_location: abort_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = abort_lsn;
    (*ctx).end_xact = true;

    /* in streaming mode, stream_abort_cb is required */
    if (*ctx).callbacks.stream_abort_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("logical streaming requires a {} callback", "stream_abort_cb")
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_abort_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, abort_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_prepare_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_prepare",
        report_location: (*txn).final_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /*
     * We're only supposed to call this when streaming and two-phase commits
     * are supported.
     */
    Assert!((*ctx).streaming);
    Assert!((*ctx).twophase);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn;
    (*ctx).end_xact = true;

    /* in streaming mode with two-phase commits, stream_prepare_cb is required */
    if (*ctx).callbacks.stream_prepare_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "logical streaming at prepare time requires a {} callback",
                "stream_prepare_cb"
            )
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_prepare_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, prepare_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_commit_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_commit",
        report_location: (*txn).final_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;
    (*ctx).write_location = (*txn).end_lsn;
    (*ctx).end_xact = true;

    /* in streaming mode, stream_commit_cb is required */
    if (*ctx).callbacks.stream_commit_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("logical streaming requires a {} callback", "stream_commit_cb")
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_commit_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, commit_lsn);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_change_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_change",
        report_location: (*change).lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this change's lsn so replies from clients can give an up-to-date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    (*ctx).write_location = (*change).lsn;

    (*ctx).end_xact = false;

    /* in streaming mode, stream_change_cb is required */
    if (*ctx).callbacks.stream_change_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!("logical streaming requires a {} callback", "stream_change_cb")
        );
    }

    if let Some(cb) = (*ctx).callbacks.stream_change_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, relation, change as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_message_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    message_size: usize,
    message: *const c_char,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* this callback is optional */
    if (*ctx).callbacks.stream_message_cb.is_none() {
        return;
    }

    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_message",
        report_location: message_lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = if txn.is_null() {
        InvalidTransactionId
    } else {
        (*txn).xid
    };
    (*ctx).write_location = message_lsn;
    (*ctx).end_xact = false;

    /* do the actual work: call callback */
    if let Some(cb) = (*ctx).callbacks.stream_message_cb {
        cb(
            ctx as *mut c_void,
            txn as *mut c_void,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        );
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn stream_truncate_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    nrelations: c_int,
    relations: *mut Relation,
    change: *mut ReorderBufferChange,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;

    Assert!(!(*ctx).fast_forward);

    /* We're only supposed to call this when streaming is supported. */
    Assert!((*ctx).streaming);

    /* this callback is optional */
    if (*ctx).callbacks.stream_truncate_cb.is_none() {
        return;
    }

    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "stream_truncate",
        report_location: (*change).lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = true;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this change's lsn so replies from clients can give an up-to-date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    (*ctx).write_location = (*change).lsn;

    (*ctx).end_xact = false;

    if let Some(cb) = (*ctx).callbacks.stream_truncate_cb {
        cb(ctx as *mut c_void, txn as *mut c_void, nrelations, relations, change as *mut c_void);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

unsafe fn update_progress_txn_cb_wrapper(
    cache: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    lsn: XLogRecPtr,
) {
    let ctx = (*cache).private_data as *mut LogicalDecodingContext;
    let mut state = LogicalErrorCallbackState {
        ctx,
        callback_name: "update_progress_txn",
        report_location: lsn,
    };
    let mut errcallback = ErrorContextCallback {
        callback: Some(output_plugin_error_callback),
        arg: &mut state as *mut LogicalErrorCallbackState as *mut c_void,
        previous: error_context_stack,
    };

    Assert!(!(*ctx).fast_forward);

    /* Push callback + info on the error context stack */
    error_context_stack = &mut errcallback;

    /* set output state */
    (*ctx).accept_writes = false;
    (*ctx).write_xid = (*txn).xid;

    /*
     * Report this change's lsn so replies from clients can give an up-to-date
     * answer. This won't ever be enough (and shouldn't be!) to confirm
     * receipt of this transaction, but it might allow another transaction's
     * commit to be confirmed with one message.
     */
    (*ctx).write_location = lsn;

    (*ctx).end_xact = false;

    OutputPluginUpdateProgress(ctx, false);

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;
}

// ---------------------------------------------------------------------------
// LogicalIncreaseXminForSlot
// ---------------------------------------------------------------------------

/*
 * Set the required catalog xmin horizon for historic snapshots in the current
 * replication slot.
 *
 * Note that in the most cases, we won't be able to immediately use the xmin
 * to increase the xmin horizon: we need to wait till the client has confirmed
 * receiving current_lsn with LogicalConfirmReceivedLocation().
 */
pub unsafe fn LogicalIncreaseXminForSlot(current_lsn: XLogRecPtr, xmin: TransactionId) {
    let mut updated_xmin = false;
    let slot: *mut ReplicationSlot;
    let mut got_new_xmin = false;

    slot = MyReplicationSlot;

    Assert!(slot != core::ptr::null_mut());

    SpinLockAcquire(&mut (*slot).mutex);

    /*
     * don't overwrite if we already have a newer xmin. This can happen if we
     * restart decoding in a slot.
     */
    if TransactionIdPrecedesOrEquals(xmin, (*slot).data.catalog_xmin) {
        // empty branch: do nothing, just fall through to SpinLockRelease
    }

    /*
     * If the client has already confirmed up to this lsn, we directly can
     * mark this as accepted. This can happen if we restart decoding in a
     * slot.
     */
    else if current_lsn <= (*slot).data.confirmed_flush {
        (*slot).candidate_catalog_xmin = xmin;
        (*slot).candidate_xmin_lsn = current_lsn;

        /* our candidate can directly be used */
        updated_xmin = true;
    }

    /*
     * Only increase if the previous values have been applied, otherwise we
     * might never end up updating if the receiver acks too slowly.
     */
    else if (*slot).candidate_xmin_lsn == InvalidXLogRecPtr {
        (*slot).candidate_catalog_xmin = xmin;
        (*slot).candidate_xmin_lsn = current_lsn;

        /*
         * Log new xmin at an appropriate log level after releasing the
         * spinlock.
         */
        got_new_xmin = true;
    }
    SpinLockRelease(&mut (*slot).mutex);

    if got_new_xmin {
        let (hi, lo) = LSN_FORMAT_ARGS(current_lsn);
        elog!(DEBUG1, "got new catalog xmin {} at {:X}/{:X}", xmin, hi, lo);
    }

    /* candidate already valid with the current flush position, apply */
    if updated_xmin {
        LogicalConfirmReceivedLocation((*slot).data.confirmed_flush);
    }
}

// ---------------------------------------------------------------------------
// LogicalIncreaseRestartDecodingForSlot
// ---------------------------------------------------------------------------

/*
 * Mark the minimal LSN (restart_lsn) we need to read to replay all
 * transactions that have not yet committed at current_lsn.
 *
 * Just like LogicalIncreaseXminForSlot this only takes effect when the
 * client has confirmed to have received current_lsn.
 */
pub unsafe fn LogicalIncreaseRestartDecodingForSlot(
    current_lsn: XLogRecPtr,
    restart_lsn: XLogRecPtr,
) {
    let mut updated_lsn = false;
    let slot: *mut ReplicationSlot;

    slot = MyReplicationSlot;

    Assert!(slot != core::ptr::null_mut());
    Assert!(restart_lsn != InvalidXLogRecPtr);
    Assert!(current_lsn != InvalidXLogRecPtr);

    SpinLockAcquire(&mut (*slot).mutex);

    /* don't overwrite if have a newer restart lsn */
    if restart_lsn <= (*slot).data.restart_lsn {
        SpinLockRelease(&mut (*slot).mutex);
    }

    /*
     * We might have already flushed far enough to directly accept this lsn,
     * in this case there is no need to check for existing candidate LSNs
     */
    else if current_lsn <= (*slot).data.confirmed_flush {
        (*slot).candidate_restart_valid = current_lsn;
        (*slot).candidate_restart_lsn = restart_lsn;
        SpinLockRelease(&mut (*slot).mutex);

        /* our candidate can directly be used */
        updated_lsn = true;
    }

    /*
     * Only increase if the previous values have been applied, otherwise we
     * might never end up updating if the receiver acks too slowly. A missed
     * value here will just cause some extra effort after reconnecting.
     */
    else if (*slot).candidate_restart_valid == InvalidXLogRecPtr {
        (*slot).candidate_restart_valid = current_lsn;
        (*slot).candidate_restart_lsn = restart_lsn;
        SpinLockRelease(&mut (*slot).mutex);

        let (rl_hi, rl_lo) = LSN_FORMAT_ARGS(restart_lsn);
        let (cl_hi, cl_lo) = LSN_FORMAT_ARGS(current_lsn);
        elog!(
            DEBUG1,
            "got new restart lsn {:X}/{:X} at {:X}/{:X}",
            rl_hi, rl_lo, cl_hi, cl_lo
        );
    } else {
        let candidate_restart_lsn: XLogRecPtr = (*slot).candidate_restart_lsn;
        let candidate_restart_valid: XLogRecPtr = (*slot).candidate_restart_valid;
        let confirmed_flush: XLogRecPtr = (*slot).data.confirmed_flush;
        SpinLockRelease(&mut (*slot).mutex);

        let (rl_hi, rl_lo) = LSN_FORMAT_ARGS(restart_lsn);
        let (cl_hi, cl_lo) = LSN_FORMAT_ARGS(current_lsn);
        let (crl_hi, crl_lo) = LSN_FORMAT_ARGS(candidate_restart_lsn);
        let (crv_hi, crv_lo) = LSN_FORMAT_ARGS(candidate_restart_valid);
        let (cf_hi, cf_lo) = LSN_FORMAT_ARGS(confirmed_flush);
        elog!(
            DEBUG1,
            "failed to increase restart lsn: proposed {:X}/{:X}, after {:X}/{:X}, current candidate {:X}/{:X}, current after {:X}/{:X}, flushed up to {:X}/{:X}",
            rl_hi, rl_lo, cl_hi, cl_lo,
            crl_hi, crl_lo, crv_hi, crv_lo,
            cf_hi, cf_lo
        );
    }

    /* candidates are already valid with the current flush position, apply */
    if updated_lsn {
        LogicalConfirmReceivedLocation((*slot).data.confirmed_flush);
    }
}

// ---------------------------------------------------------------------------
// LogicalConfirmReceivedLocation
// ---------------------------------------------------------------------------

/*
 * Handle a consumer's confirmation having received all changes up to lsn.
 */
pub unsafe fn LogicalConfirmReceivedLocation(lsn: XLogRecPtr) {
    Assert!(lsn != InvalidXLogRecPtr);

    /* Do an unlocked check for candidate_lsn first. */
    if (*MyReplicationSlot).candidate_xmin_lsn != InvalidXLogRecPtr
        || (*MyReplicationSlot).candidate_restart_valid != InvalidXLogRecPtr
    {
        let mut updated_xmin = false;
        let mut updated_restart = false;
        let restart_lsn: XLogRecPtr; // pg_attribute_unused in C

        SpinLockAcquire(&mut (*MyReplicationSlot).mutex);

        /* remember the old restart lsn */
        restart_lsn = (*MyReplicationSlot).data.restart_lsn;

        /*
         * Prevent moving the confirmed_flush backwards, as this could lead to
         * data duplication issues caused by replicating already replicated
         * changes.
         *
         * This can happen when a client acknowledges an LSN it doesn't have
         * to do anything for, and thus didn't store persistently. After a
         * restart, the client can send the prior LSN that it stored
         * persistently as an acknowledgement, but we need to ignore such an
         * LSN. See similar case handling in CreateDecodingContext.
         */
        if lsn > (*MyReplicationSlot).data.confirmed_flush {
            (*MyReplicationSlot).data.confirmed_flush = lsn;
        }

        /* if we're past the location required for bumping xmin, do so */
        if (*MyReplicationSlot).candidate_xmin_lsn != InvalidXLogRecPtr
            && (*MyReplicationSlot).candidate_xmin_lsn <= lsn
        {
            /*
             * We have to write the changed xmin to disk *before* we change
             * the in-memory value, otherwise after a crash we wouldn't know
             * that some catalog tuples might have been removed already.
             *
             * Ensure that by first writing to ->xmin and only update
             * ->effective_xmin once the new state is synced to disk. After a
             * crash ->effective_xmin is set to ->xmin.
             */
            if TransactionIdIsValid((*MyReplicationSlot).candidate_catalog_xmin)
                && (*MyReplicationSlot).data.catalog_xmin
                    != (*MyReplicationSlot).candidate_catalog_xmin
            {
                (*MyReplicationSlot).data.catalog_xmin =
                    (*MyReplicationSlot).candidate_catalog_xmin;
                (*MyReplicationSlot).candidate_catalog_xmin = InvalidTransactionId;
                (*MyReplicationSlot).candidate_xmin_lsn = InvalidXLogRecPtr;
                updated_xmin = true;
            }
        }

        if (*MyReplicationSlot).candidate_restart_valid != InvalidXLogRecPtr
            && (*MyReplicationSlot).candidate_restart_valid <= lsn
        {
            Assert!((*MyReplicationSlot).candidate_restart_lsn != InvalidXLogRecPtr);

            (*MyReplicationSlot).data.restart_lsn =
                (*MyReplicationSlot).candidate_restart_lsn;
            (*MyReplicationSlot).candidate_restart_lsn = InvalidXLogRecPtr;
            (*MyReplicationSlot).candidate_restart_valid = InvalidXLogRecPtr;
            updated_restart = true;
        }

        SpinLockRelease(&mut (*MyReplicationSlot).mutex);

        /* first write new xmin to disk, so we know what's up after a crash */
        if updated_xmin || updated_restart {
            // #ifdef USE_INJECTION_POINTS  - omitted: feature-flag code, not ported
            // INJECTION_POINT("logical-replication-slot-advance-segment", NULL);

            ReplicationSlotMarkDirty();
            ReplicationSlotSave();
            elog!(
                DEBUG1,
                "updated xmin: {} restart: {}",
                updated_xmin as u32,
                updated_restart as u32
            );
        }

        /*
         * Now the new xmin is safely on disk, we can let the global value
         * advance. We do not take ProcArrayLock or similar since we only
         * advance xmin here and there's not much harm done by a concurrent
         * computation missing that.
         */
        if updated_xmin {
            SpinLockAcquire(&mut (*MyReplicationSlot).mutex);
            (*MyReplicationSlot).effective_catalog_xmin =
                (*MyReplicationSlot).data.catalog_xmin;
            SpinLockRelease(&mut (*MyReplicationSlot).mutex);

            ReplicationSlotsComputeRequiredXmin(false);
            ReplicationSlotsComputeRequiredLSN();
        }
    } else {
        SpinLockAcquire(&mut (*MyReplicationSlot).mutex);

        /*
         * Prevent moving the confirmed_flush backwards. See comments above
         * for the details.
         */
        if lsn > (*MyReplicationSlot).data.confirmed_flush {
            (*MyReplicationSlot).data.confirmed_flush = lsn;
        }

        SpinLockRelease(&mut (*MyReplicationSlot).mutex);
    }
}

// ---------------------------------------------------------------------------
// ResetLogicalStreamingState / UpdateDecodingStats
// ---------------------------------------------------------------------------

/*
 * Clear logical streaming state during (sub)transaction abort.
 */
pub unsafe fn ResetLogicalStreamingState() {
    CheckXidAlive = InvalidTransactionId;
    bsysscan = false;
}

// CheckXidAlive (replication/logical/logical.c global).
// TODO(pg-port): real CheckXidAlive lives in replication/logical/logical.c
pub static mut CheckXidAlive: TransactionId = InvalidTransactionId;

/*
 * Report stats for a slot.
 */
pub unsafe fn UpdateDecodingStats(ctx: *mut LogicalDecodingContext) {
    let rb: *mut ReorderBuffer = (*ctx).reorder;
    let mut repSlotStat = PgStat_StatReplSlotEntry::zeroed();

    /* Nothing to do if we don't have any replication stats to be sent. */
    if (*rb).spillBytes <= 0 && (*rb).streamBytes <= 0 && (*rb).totalBytes <= 0 {
        return;
    }

    elog!(
        DEBUG2,
        "UpdateDecodingStats: updating stats {:p} {} {} {} {} {} {} {} {}",
        rb,
        (*rb).spillTxns,
        (*rb).spillCount,
        (*rb).spillBytes,
        (*rb).streamTxns,
        (*rb).streamCount,
        (*rb).streamBytes,
        (*rb).totalTxns,
        (*rb).totalBytes
    );

    repSlotStat.spill_txns = (*rb).spillTxns;
    repSlotStat.spill_count = (*rb).spillCount;
    repSlotStat.spill_bytes = (*rb).spillBytes;
    repSlotStat.stream_txns = (*rb).streamTxns;
    repSlotStat.stream_count = (*rb).streamCount;
    repSlotStat.stream_bytes = (*rb).streamBytes;
    repSlotStat.total_txns = (*rb).totalTxns;
    repSlotStat.total_bytes = (*rb).totalBytes;

    pgstat_report_replslot((*ctx).slot as *mut core::ffi::c_void, &repSlotStat);

    (*rb).spillTxns = 0;
    (*rb).spillCount = 0;
    (*rb).spillBytes = 0;
    (*rb).streamTxns = 0;
    (*rb).streamCount = 0;
    (*rb).streamBytes = 0;
    (*rb).totalTxns = 0;
    (*rb).totalBytes = 0;
}

// ---------------------------------------------------------------------------
// LogicalReplicationSlotHasPendingWal
// ---------------------------------------------------------------------------

/*
 * Read up to the end of WAL starting from the decoding slot's restart_lsn.
 * Return true if any meaningful/decodable WAL records are encountered,
 * otherwise false.
 */
pub unsafe fn LogicalReplicationSlotHasPendingWal(end_of_wal: XLogRecPtr) -> bool {
    let mut has_pending_wal = false;

    Assert!(MyReplicationSlot != core::ptr::null_mut());

    // PG_TRY();
    {
        let ctx: *mut LogicalDecodingContext;

        // XL_ROUTINE struct for local xlog reading.
        // TODO(pg-port): XL_ROUTINE is a C99 designated-init macro; we build the struct inline.
        let xl_routine = palloc0(core::mem::size_of::<XLogReaderRoutine>()) as *mut XLogReaderRoutine;
        (*xl_routine).page_read = Some(core::mem::transmute(read_local_xlog_page as usize));
        (*xl_routine).segment_open = Some(core::mem::transmute(wal_segment_open as usize));
        (*xl_routine).segment_close = Some(core::mem::transmute(wal_segment_close as usize));

        /*
         * Create our decoding context in fast_forward mode, passing start_lsn
         * as InvalidXLogRecPtr, so that we start processing from the slot's
         * confirmed_flush.
         */
        ctx = CreateDecodingContext(
            InvalidXLogRecPtr,
            core::ptr::null_mut(), // NIL
            true,                  /* fast_forward */
            xl_routine,
            None,
            None,
            None,
        );

        /*
         * Start reading at the slot's restart_lsn, which we know points to a
         * valid record.
         */
        XLogBeginRead((*ctx).reader, (*MyReplicationSlot).data.restart_lsn);

        /* Invalidate non-timetravel entries */
        InvalidateSystemCaches();

        /* Loop until the end of WAL or some changes are processed */
        while !has_pending_wal && (*(*ctx).reader).EndRecPtr < end_of_wal {
            let record: *mut XLogRecord;
            let mut errm: *mut c_char = core::ptr::null_mut();

            record = XLogReadRecord((*ctx).reader, &mut errm);

            if !errm.is_null() {
                elog!(
                    ERROR,
                    "could not find record for logical decoding: {}",
                    core::ffi::CStr::from_ptr(errm).to_string_lossy()
                );
            }

            if !record.is_null() {
                LogicalDecodingProcessRecord(ctx as *mut crate::replication::logical::decode::LogicalDecodingContext, (*ctx).reader);
            }

            has_pending_wal = (*ctx).processing_required;

            CHECK_FOR_INTERRUPTS();
        }

        /* Clean up */
        FreeDecodingContext(ctx);
        InvalidateSystemCaches();
        pfree(xl_routine as *mut c_void);
    }
    // PG_CATCH();
    // {
    //     /* clear all timetravel entries */
    //     InvalidateSystemCaches();
    //     PG_RE_THROW();
    // }
    // PG_END_TRY();

    has_pending_wal
}

// ---------------------------------------------------------------------------
// LogicalSlotAdvanceAndCheckSnapState
// ---------------------------------------------------------------------------

/*
 * Helper function for advancing our logical replication slot forward.
 *
 * The slot's restart_lsn is used as start point for reading records, while
 * confirmed_flush is used as base point for the decoding context.
 *
 * We cannot just do LogicalConfirmReceivedLocation to update confirmed_flush,
 * because we need to digest WAL to advance restart_lsn allowing to recycle
 * WAL and removal of old catalog tuples.  As decoding is done in fast_forward
 * mode, no changes are generated anyway.
 *
 * *found_consistent_snapshot will be true if the initial decoding snapshot has
 * been built; Otherwise, it will be false.
 */
pub unsafe fn LogicalSlotAdvanceAndCheckSnapState(
    moveto: XLogRecPtr,
    found_consistent_snapshot: *mut bool,
) -> XLogRecPtr {
    let ctx: *mut LogicalDecodingContext;
    let old_resowner: ResourceOwner = CurrentResourceOwner;
    let retlsn: XLogRecPtr;

    Assert!(moveto != InvalidXLogRecPtr);

    if !found_consistent_snapshot.is_null() {
        *found_consistent_snapshot = false;
    }

    // PG_TRY();
    {
        // XL_ROUTINE struct for local xlog reading.
        let xl_routine = palloc0(core::mem::size_of::<XLogReaderRoutine>()) as *mut XLogReaderRoutine;
        (*xl_routine).page_read = Some(core::mem::transmute(read_local_xlog_page as usize));
        (*xl_routine).segment_open = Some(core::mem::transmute(wal_segment_open as usize));
        (*xl_routine).segment_close = Some(core::mem::transmute(wal_segment_close as usize));

        /*
         * Create our decoding context in fast_forward mode, passing start_lsn
         * as InvalidXLogRecPtr, so that we start processing from my slot's
         * confirmed_flush.
         */
        ctx = CreateDecodingContext(
            InvalidXLogRecPtr,
            core::ptr::null_mut(), // NIL
            true,                  /* fast_forward */
            xl_routine,
            None,
            None,
            None,
        );

        /*
         * Wait for specified streaming replication standby servers (if any)
         * to confirm receipt of WAL up to moveto lsn.
         */
        WaitForStandbyConfirmation(moveto);

        /*
         * Start reading at the slot's restart_lsn, which we know to point to
         * a valid record.
         */
        XLogBeginRead((*ctx).reader, (*MyReplicationSlot).data.restart_lsn);

        /* invalidate non-timetravel entries */
        InvalidateSystemCaches();

        /* Decode records until we reach the requested target */
        while (*(*ctx).reader).EndRecPtr < moveto {
            let mut errm: *mut c_char = core::ptr::null_mut();
            let record: *mut XLogRecord;

            /*
             * Read records.  No changes are generated in fast_forward mode,
             * but snapbuilder/slot statuses are updated properly.
             */
            record = XLogReadRecord((*ctx).reader, &mut errm);
            if !errm.is_null() {
                elog!(
                    ERROR,
                    "could not find record while advancing replication slot: {}",
                    core::ffi::CStr::from_ptr(errm).to_string_lossy()
                );
            }

            /*
             * Process the record.  Storage-level changes are ignored in
             * fast_forward mode, but other modules (such as snapbuilder)
             * might still have critical updates to do.
             */
            if !record.is_null() {
                LogicalDecodingProcessRecord(ctx as *mut crate::replication::logical::decode::LogicalDecodingContext, (*ctx).reader);
            }

            CHECK_FOR_INTERRUPTS();
        }

        if !found_consistent_snapshot.is_null() && DecodingContextReady(ctx) {
            *found_consistent_snapshot = true;
        }

        /*
         * Logical decoding could have clobbered CurrentResourceOwner during
         * transaction management, so restore the executor's value.  (This is
         * a kluge, but it's not worth cleaning up right now.)
         */
        CurrentResourceOwner = old_resowner;

        if (*(*ctx).reader).EndRecPtr != InvalidXLogRecPtr {
            LogicalConfirmReceivedLocation(moveto);

            /*
             * If only the confirmed_flush LSN has changed the slot won't get
             * marked as dirty by the above. Callers on the walsender
             * interface are expected to keep track of their own progress and
             * don't need it written out. But SQL-interface users cannot
             * specify their own start positions and it's harder for them to
             * keep track of their progress, so we should make more of an
             * effort to save it for them.
             *
             * Dirty the slot so it is written out at the next checkpoint. The
             * LSN position advanced to may still be lost on a crash but this
             * makes the data consistent after a clean shutdown.
             */
            ReplicationSlotMarkDirty();
        }

        retlsn = (*MyReplicationSlot).data.confirmed_flush;

        /* free context, call shutdown callback */
        FreeDecodingContext(ctx);

        InvalidateSystemCaches();
        pfree(xl_routine as *mut c_void);
    }
    // PG_CATCH();
    // {
    //     /* clear all timetravel entries */
    //     InvalidateSystemCaches();
    //     PG_RE_THROW();
    // }
    // PG_END_TRY();

    retlsn
}

//! replication/worker_internal.h - Internal headers shared by logical replication workers.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint16, uint32, Pointer, Size, TransactionId};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::lib::stringinfo::StringInfo;
use crate::nodes::pg_list::List;
use crate::port::atomics::pg_atomic_uint32;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::lockdefs::LOCKMODE;
use crate::utils::mmgr::memnodes::MemoryContext;

// TimestampTz - dedup when datatype/timestamp.h lands.
pub type TimestampTz = int64;

// --- Locally stubbed referenced-but-not-ported types ---
// TODO: dedup when the respective headers land.
pub type PGPROC = c_void; // storage/proc.h
pub type FileSet = c_void; // storage/fileset.h
pub type pid_t = c_int; // system pid_t
pub type ErrorContextCallback = c_void; // utils/elog.h
pub type WalReceiverConn = c_void; // replication/walreceiver.h
pub type Subscription = c_void; // catalog/pg_subscription.h
pub type shm_mq_handle = c_void; // storage/shm_mq.h
pub type dsm_segment = c_void; // storage/dsm.h
pub type dsm_handle = uint32; // storage/dsm_impl.h
pub type WalRcvStreamOptions = c_void; // replication/walreceiver.h
pub type LogicalRepStreamAbortData = c_void; // replication/logicalproto.h

/* Different types of worker */
pub type LogicalRepWorkerType = c_int;
pub const WORKERTYPE_UNKNOWN: LogicalRepWorkerType = 0;
pub const WORKERTYPE_TABLESYNC: LogicalRepWorkerType = 1;
pub const WORKERTYPE_APPLY: LogicalRepWorkerType = 2;
pub const WORKERTYPE_PARALLEL_APPLY: LogicalRepWorkerType = 3;

#[repr(C)]
pub struct LogicalRepWorker {
    /* What type of worker is this? */
    pub type_: LogicalRepWorkerType,

    /* Time at which this worker was launched. */
    pub launch_time: TimestampTz,

    /* Indicates if this slot is used or free. */
    pub in_use: bool,

    /* Increased every time the slot is taken by new worker. */
    pub generation: uint16,

    /* Pointer to proc array. NULL if not running. */
    pub proc: *mut PGPROC,

    /* Database id to connect to. */
    pub dbid: Oid,

    /* User to use for connection (will be same as owner of subscription). */
    pub userid: Oid,

    /* Subscription id for the worker. */
    pub subid: Oid,

    /* Used for initial table synchronization. */
    pub relid: Oid,
    pub relstate: c_char,
    pub relstate_lsn: XLogRecPtr,
    pub relmutex: slock_t,

    /*
     * Used to create the changes and subxact files for the streaming
     * transactions.  Upon the arrival of the first streaming transaction or
     * when the first-time leader apply worker times out while sending changes
     * to the parallel apply worker, the fileset will be initialized, and it
     * will be deleted when the worker exits.  Under this, separate buffiles
     * would be created for each transaction which will be deleted after the
     * transaction is finished.
     */
    pub stream_fileset: *mut FileSet,

    /*
     * PID of leader apply worker if this slot is used for a parallel apply
     * worker, InvalidPid otherwise.
     */
    pub leader_pid: pid_t,

    /* Indicates whether apply can be performed in parallel. */
    pub parallel_apply: bool,

    /* Stats. */
    pub last_lsn: XLogRecPtr,
    pub last_send_time: TimestampTz,
    pub last_recv_time: TimestampTz,
    pub reply_lsn: XLogRecPtr,
    pub reply_time: TimestampTz,
}

/*
 * State of the transaction in parallel apply worker.
 *
 * The enum values must have the same order as the transaction state
 * transitions.
 */
pub type ParallelTransState = c_int;
pub const PARALLEL_TRANS_UNKNOWN: ParallelTransState = 0;
pub const PARALLEL_TRANS_STARTED: ParallelTransState = 1;
pub const PARALLEL_TRANS_FINISHED: ParallelTransState = 2;

/*
 * State of fileset used to communicate changes from leader to parallel
 * apply worker.
 */
pub type PartialFileSetState = c_int;
pub const FS_EMPTY: PartialFileSetState = 0;
pub const FS_SERIALIZE_IN_PROGRESS: PartialFileSetState = 1;
pub const FS_SERIALIZE_DONE: PartialFileSetState = 2;
pub const FS_READY: PartialFileSetState = 3;

/*
 * Struct for sharing information between leader apply worker and parallel
 * apply workers.
 */
#[repr(C)]
pub struct ParallelApplyWorkerShared {
    pub mutex: slock_t,

    pub xid: TransactionId,

    /*
     * State used to ensure commit ordering.
     */
    pub xact_state: ParallelTransState,

    /* Information from the corresponding LogicalRepWorker slot. */
    pub logicalrep_worker_generation: uint16,
    pub logicalrep_worker_slot_no: c_int,

    /*
     * Indicates whether there are pending streaming blocks in the queue. The
     * parallel apply worker will check it before starting to wait.
     */
    pub pending_stream_count: pg_atomic_uint32,

    /*
     * XactLastCommitEnd from the parallel apply worker. This is required by
     * the leader worker so it can update the lsn_mappings.
     */
    pub last_commit_end: XLogRecPtr,

    /*
     * After entering PARTIAL_SERIALIZE mode, the leader apply worker will
     * serialize changes to the file, and share the fileset with the parallel
     * apply worker when processing the transaction finish command.
     */
    pub fileset_state: PartialFileSetState,
    pub fileset: FileSet,
}

/*
 * Information which is used to manage the parallel apply worker.
 */
#[repr(C)]
pub struct ParallelApplyWorkerInfo {
    /*
     * This queue is used to send changes from the leader apply worker to the
     * parallel apply worker.
     */
    pub mq_handle: *mut shm_mq_handle,

    /*
     * This queue is used to transfer error messages from the parallel apply
     * worker to the leader apply worker.
     */
    pub error_mq_handle: *mut shm_mq_handle,

    pub dsm_seg: *mut dsm_segment,

    /*
     * Indicates whether the leader apply worker needs to serialize the
     * remaining changes to a file due to timeout when attempting to send data
     * to the parallel apply worker via shared memory.
     */
    pub serialize_changes: bool,

    /*
     * True if the worker is being used to process a parallel apply
     * transaction. False indicates this worker is available for re-use.
     */
    pub in_use: bool,

    pub shared: *mut ParallelApplyWorkerShared,
}

/* Main memory context for apply worker. Permanent during worker lifetime. */
// MemoryContext embeds fn-pointer methods (not FFI-safe); harmless for these
// extern globals (placeholders until worker.c lands as real Rust statics).
#[allow(improper_ctypes)]
extern "C" {
    pub static mut ApplyContext: MemoryContext;

    pub static mut ApplyMessageContext: MemoryContext;

    pub static mut apply_error_context_stack: *mut ErrorContextCallback;

    pub static mut MyParallelShared: *mut ParallelApplyWorkerShared;

    /* libpqreceiver connection */
    pub static mut LogRepWorkerWalRcvConn: *mut WalReceiverConn;

    /* Worker and subscription objects. */
    pub static mut MySubscription: *mut Subscription;

    pub static mut in_remote_transaction: bool;

    pub static mut InitializingApplyWorker: bool;
}

/* Pointer to this backend's LogicalRepWorker slot (worker.c global). Defined here
 * (rather than extern) so all importers share one symbol; null during bring-up. */
pub static mut MyLogicalRepWorker: *mut LogicalRepWorker = core::ptr::null_mut();

pub unsafe fn logicalrep_worker_attach(slot: c_int) { crate::replication::logical::launcher::logicalrep_worker_attach(slot as _) }

pub unsafe fn logicalrep_worker_find(
    subid: Oid,
    relid: Oid,
    only_running: bool,
) -> *mut LogicalRepWorker { crate::replication::logical::launcher::logicalrep_worker_find(subid as _, relid as _, only_running) }

pub unsafe fn logicalrep_workers_find(
    subid: Oid,
    only_running: bool,
    acquire_lock: bool,
) -> *mut List { crate::replication::logical::launcher::logicalrep_workers_find(subid as _, only_running, acquire_lock) }

pub unsafe fn logicalrep_worker_launch(
    wtype: LogicalRepWorkerType,
    dbid: Oid,
    subid: Oid,
    subname: *const c_char,
    userid: Oid,
    relid: Oid,
    subworker_dsm: dsm_handle,
) -> bool { crate::replication::logical::launcher::logicalrep_worker_launch(wtype, dbid as _, subid as _, subname as _, userid as _, relid as _, subworker_dsm) }

pub unsafe fn logicalrep_worker_stop(subid: Oid, relid: Oid) { crate::replication::logical::launcher::logicalrep_worker_stop(subid as _, relid as _) }

pub unsafe fn logicalrep_pa_worker_stop(winfo: *mut ParallelApplyWorkerInfo) { crate::replication::logical::launcher::logicalrep_pa_worker_stop(winfo as _) }

pub unsafe fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) { crate::replication::logical::launcher::logicalrep_worker_wakeup(subid as _, relid as _) }

pub unsafe fn logicalrep_worker_wakeup_ptr(worker: *mut LogicalRepWorker) { crate::replication::logical::launcher::logicalrep_worker_wakeup_ptr(worker as _) }

pub unsafe fn logicalrep_sync_worker_count(subid: Oid) -> c_int { crate::replication::logical::launcher::logicalrep_sync_worker_count(subid as _) }

pub unsafe fn ReplicationOriginNameForLogicalRep(
    suboid: Oid,
    relid: Oid,
    originname: *mut c_char,
    szoriginname: Size,
) { crate::replication::logical::worker::ReplicationOriginNameForLogicalRep(suboid as _, relid as _, originname as _, szoriginname) }

pub unsafe fn AllTablesyncsReady() -> bool { crate::replication::logical::tablesync::AllTablesyncsReady() }

pub unsafe fn UpdateTwoPhaseState(suboid: Oid, new_state: c_char) { crate::replication::logical::tablesync::UpdateTwoPhaseState(suboid as _, new_state as _) }

pub unsafe fn process_syncing_tables(current_lsn: XLogRecPtr) { crate::replication::logical::tablesync::process_syncing_tables(current_lsn as _) }

pub unsafe fn invalidate_syncing_table_states(arg: Datum, cacheid: c_int, hashvalue: uint32) { crate::replication::logical::tablesync::invalidate_syncing_table_states(arg as _, cacheid as _, hashvalue as _) }

pub unsafe fn stream_start_internal(xid: TransactionId, first_segment: bool) { crate::replication::logical::worker::stream_start_internal(xid as _, first_segment) }

pub unsafe fn stream_stop_internal(xid: TransactionId) { crate::replication::logical::worker::stream_stop_internal(xid as _) }

/* Common streaming function to apply all the spooled messages */
pub unsafe fn apply_spooled_messages(
    stream_fileset: *mut FileSet,
    xid: TransactionId,
    lsn: XLogRecPtr,
) { crate::replication::logical::worker::apply_spooled_messages(stream_fileset as _, xid as _, lsn as _) }

pub unsafe fn apply_dispatch(s: StringInfo) { crate::replication::logical::worker::apply_dispatch(s as _) }

pub unsafe fn maybe_reread_subscription() { crate::replication::logical::worker::maybe_reread_subscription() }

pub unsafe fn stream_cleanup_files(subid: Oid, xid: TransactionId) { crate::replication::logical::worker::stream_cleanup_files(subid as _, xid as _) }

pub unsafe fn set_stream_options(
    options: *mut WalRcvStreamOptions,
    slotname: *mut c_char,
    origin_startpos: *mut XLogRecPtr,
) { crate::replication::logical::worker::set_stream_options(options as _, slotname as _, origin_startpos as _) }

pub unsafe fn start_apply(origin_startpos: XLogRecPtr) { crate::replication::logical::worker::start_apply(origin_startpos as _) }

pub unsafe fn InitializeLogRepWorker() { crate::replication::logical::worker::InitializeLogRepWorker() }

pub unsafe fn SetupApplyOrSyncWorker(worker_slot: c_int) { crate::replication::logical::worker::SetupApplyOrSyncWorker(worker_slot as _) }

pub unsafe fn DisableSubscriptionAndExit() { crate::replication::logical::worker::DisableSubscriptionAndExit() }

pub unsafe fn store_flush_position(remote_lsn: XLogRecPtr, local_lsn: XLogRecPtr) { crate::replication::logical::worker::store_flush_position(remote_lsn as _, local_lsn as _) }

/* Function for apply error callback */
pub unsafe fn apply_error_callback(_arg: *mut c_void) {
    unimplemented!()
}

pub unsafe fn set_apply_error_context_origin(originname: *mut c_char) { crate::replication::logical::worker::set_apply_error_context_origin(originname as _) }

/* Parallel apply worker setup and interactions */
pub unsafe fn pa_allocate_worker(xid: TransactionId) { unimplemented!() }

pub unsafe fn pa_find_worker(xid: TransactionId) -> *mut ParallelApplyWorkerInfo { unimplemented!() }

pub unsafe fn pa_detach_all_error_mq() { unimplemented!() }

pub unsafe fn pa_send_data(
    winfo: *mut ParallelApplyWorkerInfo,
    nbytes: Size,
    data: *const c_void,
) -> bool { unimplemented!() }

pub unsafe fn pa_switch_to_partial_serialize(
    winfo: *mut ParallelApplyWorkerInfo,
    stream_locked: bool,
) { unimplemented!() }

pub unsafe fn pa_set_xact_state(
    wshared: *mut ParallelApplyWorkerShared,
    xact_state: ParallelTransState,
) { unimplemented!() }

pub unsafe fn pa_set_stream_apply_worker(winfo: *mut ParallelApplyWorkerInfo) { unimplemented!() }

pub unsafe fn pa_start_subtrans(current_xid: TransactionId, top_xid: TransactionId) { unimplemented!() }

pub unsafe fn pa_reset_subtrans() { unimplemented!() }

pub unsafe fn pa_stream_abort(abort_data: *mut LogicalRepStreamAbortData) { unimplemented!() }

pub unsafe fn pa_set_fileset_state(
    wshared: *mut ParallelApplyWorkerShared,
    fileset_state: PartialFileSetState,
) { unimplemented!() }

pub unsafe fn pa_lock_stream(xid: TransactionId, lockmode: LOCKMODE) { unimplemented!() }

pub unsafe fn pa_unlock_stream(xid: TransactionId, lockmode: LOCKMODE) { unimplemented!() }

pub unsafe fn pa_lock_transaction(xid: TransactionId, lockmode: LOCKMODE) { unimplemented!() }

pub unsafe fn pa_unlock_transaction(xid: TransactionId, lockmode: LOCKMODE) { unimplemented!() }

pub unsafe fn pa_decr_and_wait_stream_block() { unimplemented!() }

pub unsafe fn pa_xact_finish(winfo: *mut ParallelApplyWorkerInfo, remote_lsn: XLogRecPtr) { unimplemented!() }

#[inline]
pub unsafe fn isParallelApplyWorker(worker: *const LogicalRepWorker) -> bool {
    (*worker).in_use && (*worker).type_ == WORKERTYPE_PARALLEL_APPLY
}

#[inline]
pub unsafe fn isTablesyncWorker(worker: *const LogicalRepWorker) -> bool {
    (*worker).in_use && (*worker).type_ == WORKERTYPE_TABLESYNC
}

#[inline]
pub unsafe fn am_tablesync_worker() -> bool {
    isTablesyncWorker(MyLogicalRepWorker)
}

#[inline]
pub unsafe fn am_leader_apply_worker() -> bool {
    // Assert(MyLogicalRepWorker->in_use);
    (*MyLogicalRepWorker).type_ == WORKERTYPE_APPLY
}

#[inline]
pub unsafe fn am_parallel_apply_worker() -> bool {
    // Assert(MyLogicalRepWorker->in_use);
    isParallelApplyWorker(MyLogicalRepWorker)
}

// Pointer alias kept referenced to satisfy header include of storage/lock.h.
#[allow(unused)]
type _ReferencedPointer = Pointer;

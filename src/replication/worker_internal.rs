//! Translated from PostgreSQL src/include/replication/worker_internal.h
//!
//! Internal headers shared by logical replication workers. Under the
//! single-process async port, the shmem worker-slot array collapses to owned
//! Rust state; `slock_t`/`Latch`/atomics map to `std::sync`/`tokio`/`std::atomic`.

use std::sync::atomic::AtomicU32;
use std::sync::Mutex;

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::catalog::pg_subscription::Subscription;
use crate::datatype::timestamp::TimestampTz;
use crate::lib::stringinfo::StringInfo;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::replication::logicalproto::LogicalRepStreamAbortData;
use crate::replication::walreceiver::WalRcvStreamOptions;
use crate::storage::fileset::FileSet;
use crate::storage::lock::LOCKMODE;
use crate::storage::proc::PGPROC;

/// Different types of worker (C `LogicalRepWorkerType`). Sequential ordinal enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Unknown = 0,
    Tablesync,
    Apply,
    ParallelApply,
}
pub use WorkerState as LogicalRepWorkerType;

/// A logical replication worker slot. In-memory: C kept these in a shmem array;
/// here it is owned state. `slock_t relmutex` -> `std::sync::Mutex<()>`;
/// `PGPROC *proc` is nullable -> Option.
pub struct LogicalRepWorker {
    pub r#type: WorkerState,    // what type of worker is this?
    pub launch_time: TimestampTz, // time at which this worker was launched
    pub in_use: bool,           // is this slot used or free?
    pub generation: u16,        // bumped each time the slot is taken
    pub proc: Option<*mut PGPROC>, // proc array entry, None if not running; TODO(ptr)
    pub dbid: Oid,              // database id to connect to
    pub userid: Oid,           // connection user (subscription owner)
    pub subid: Oid,            // subscription id for the worker
    pub relid: Oid,            // table being synced (tablesync)
    pub relstate: u8,          // char relstate code
    pub relstate_lsn: XLogRecPtr,
    pub relmutex: Mutex<()>,   // was slock_t relmutex
    pub stream_fileset: Option<Box<FileSet>>, // changes/subxact files for streaming
    pub leader_pid: i32,       // leader apply worker pid (parallel apply); InvalidPid otherwise
    pub parallel_apply: bool,  // can apply be done in parallel?
    // Stats.
    pub last_lsn: XLogRecPtr,
    pub last_send_time: TimestampTz,
    pub last_recv_time: TimestampTz,
    pub reply_lsn: XLogRecPtr,
    pub reply_time: TimestampTz,
}

/// State of the transaction in a parallel apply worker. The enum values must
/// keep transaction-state transition order. Sequential ordinal enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelTransState {
    Unknown,
    Started,
    Finished,
}

/// State of the fileset used to communicate changes leader -> parallel apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialFileSetState {
    Empty,
    SerializeInProgress,
    SerializeDone,
    Ready,
}

/// Shared between leader apply worker and parallel apply workers. C placed this
/// in shmem; single-process keeps it as owned state behind a `Mutex`.
/// `slock_t mutex` -> `std::sync::Mutex<()>`; `pg_atomic_uint32` -> `AtomicU32`.
pub struct ParallelApplyWorkerShared {
    pub mutex: Mutex<()>, // was slock_t mutex
    pub xid: TransactionId,
    pub xact_state: ParallelTransState, // ensures commit ordering
    pub logicalrep_worker_generation: u16, // from the LogicalRepWorker slot
    pub logicalrep_worker_slot_no: i32,
    pub pending_stream_count: AtomicU32, // pending streaming blocks in the queue
    pub last_commit_end: XLogRecPtr,     // parallel worker's XactLastCommitEnd
    pub fileset_state: PartialFileSetState,
    pub fileset: FileSet,
}

/// Used by the leader to manage a parallel apply worker. The shmem message
/// queues (`shm_mq_handle`) and dsm segment collapse under single-process; model
/// the leader->worker change channel and worker->leader error channel as tokio
/// channels in Phase 2. TODO(ptr): replace queue handles with channels.
pub struct ParallelApplyWorkerInfo {
    // mq_handle / error_mq_handle / dsm_seg dropped: shmem queues -> channels.
    pub serialize_changes: bool, // serialize remaining changes to a file on timeout?
    pub in_use: bool,            // worker processing a parallel apply txn?
    pub shared: Option<Box<ParallelApplyWorkerShared>>, // TODO(ptr)
}

// Apply-worker globals (C `extern PGDLLIMPORT ...`): process-globals that become
// session/task state in the single-process port. Declared here as the porting
// target; TODO(global): thread through an apply-worker context in Phase 2.
//
//   ApplyContext, ApplyMessageContext        -> MemoryContext
//   apply_error_context_stack                -> ErrorContextCallback chain
//   MyParallelShared                         -> &ParallelApplyWorkerShared
//   LogRepWorkerWalRcvConn                   -> WalReceiverConn
//   MySubscription, MyLogicalRepWorker       -> the active subscription/worker
//   in_remote_transaction, InitializingApplyWorker -> bool

pub fn logicalrep_worker_attach(_slot: i32) {
    unimplemented!()
}

/// Returns the matching worker slot, or None when not found.
pub fn logicalrep_worker_find(
    _subid: Oid,
    _relid: Oid,
    _only_running: bool,
) -> Option<&'static mut LogicalRepWorker> {
    unimplemented!()
}

pub fn logicalrep_workers_find(
    _subid: Oid,
    _only_running: bool,
    _acquire_lock: bool,
) -> Vec<&'static mut LogicalRepWorker> {
    unimplemented!()
}

/// Launch a worker. Returns true on success. `subworker_dsm` (a dsm_handle in C)
/// is dropped: the parallel-worker handoff becomes a channel under single-process.
pub fn logicalrep_worker_launch(
    _wtype: WorkerState,
    _dbid: Oid,
    _subid: Oid,
    _subname: &str,
    _userid: Oid,
    _relid: Oid,
) -> bool {
    unimplemented!()
}

pub fn logicalrep_worker_stop(_subid: Oid, _relid: Oid) {
    unimplemented!()
}

pub fn logicalrep_pa_worker_stop(_winfo: &mut ParallelApplyWorkerInfo) {
    unimplemented!()
}

pub fn logicalrep_worker_wakeup(_subid: Oid, _relid: Oid) {
    unimplemented!()
}

pub fn logicalrep_worker_wakeup_ptr(_worker: &mut LogicalRepWorker) {
    unimplemented!()
}

pub fn logicalrep_sync_worker_count(_subid: Oid) -> i32 {
    unimplemented!()
}

/// Build the replication-origin name; returns the formatted name.
pub fn ReplicationOriginNameForLogicalRep(_suboid: Oid, _relid: Oid) -> String {
    unimplemented!()
}

pub fn AllTablesyncsReady() -> bool {
    unimplemented!()
}

pub fn UpdateTwoPhaseState(_suboid: Oid, _new_state: u8) {
    unimplemented!()
}

pub fn process_syncing_tables(_current_lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn invalidate_syncing_table_states(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    unimplemented!()
}

pub fn stream_start_internal(_xid: TransactionId, _first_segment: bool) {
    unimplemented!()
}

pub fn stream_stop_internal(_xid: TransactionId) {
    unimplemented!()
}

/// Common streaming function to apply all the spooled messages.
pub fn apply_spooled_messages(_stream_fileset: &mut FileSet, _xid: TransactionId, _lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn apply_dispatch(_s: &mut StringInfo) {
    unimplemented!()
}

pub fn maybe_reread_subscription() {
    unimplemented!()
}

pub fn stream_cleanup_files(_subid: Oid, _xid: TransactionId) {
    unimplemented!()
}

/// Set stream options; returns the origin start position.
pub fn set_stream_options(
    _options: &mut WalRcvStreamOptions,
    _slotname: &str,
) -> XLogRecPtr {
    unimplemented!()
}

pub fn start_apply(_origin_startpos: XLogRecPtr) {
    unimplemented!()
}

pub fn InitializeLogRepWorker() {
    unimplemented!()
}

pub fn SetupApplyOrSyncWorker(_worker_slot: i32) {
    unimplemented!()
}

pub fn DisableSubscriptionAndExit() {
    unimplemented!()
}

pub fn store_flush_position(_remote_lsn: XLogRecPtr, _local_lsn: XLogRecPtr) {
    unimplemented!()
}

/// Apply error callback. The C `void *arg` opaque context -> a closure in Phase 2.
pub fn apply_error_callback() {
    unimplemented!()
}

pub fn set_apply_error_context_origin(_originname: &str) {
    unimplemented!()
}

// Parallel apply worker setup and interactions.

pub fn pa_allocate_worker(_xid: TransactionId) {
    unimplemented!()
}

pub fn pa_find_worker(_xid: TransactionId) -> Option<&'static mut ParallelApplyWorkerInfo> {
    unimplemented!()
}

pub fn pa_detach_all_error_mq() {
    unimplemented!()
}

/// Send data to a parallel apply worker. Returns true on success.
pub fn pa_send_data(_winfo: &mut ParallelApplyWorkerInfo, _data: &[u8]) -> bool {
    unimplemented!()
}

pub fn pa_switch_to_partial_serialize(
    _winfo: &mut ParallelApplyWorkerInfo,
    _stream_locked: bool,
) {
    unimplemented!()
}

pub fn pa_set_xact_state(_wshared: &mut ParallelApplyWorkerShared, _xact_state: ParallelTransState) {
    unimplemented!()
}

pub fn pa_set_stream_apply_worker(_winfo: &mut ParallelApplyWorkerInfo) {
    unimplemented!()
}

pub fn pa_start_subtrans(_current_xid: TransactionId, _top_xid: TransactionId) {
    unimplemented!()
}

pub fn pa_reset_subtrans() {
    unimplemented!()
}

pub fn pa_stream_abort(_abort_data: &mut LogicalRepStreamAbortData) {
    unimplemented!()
}

pub fn pa_set_fileset_state(
    _wshared: &mut ParallelApplyWorkerShared,
    _fileset_state: PartialFileSetState,
) {
    unimplemented!()
}

pub fn pa_lock_stream(_xid: TransactionId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn pa_unlock_stream(_xid: TransactionId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn pa_lock_transaction(_xid: TransactionId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn pa_unlock_transaction(_xid: TransactionId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn pa_decr_and_wait_stream_block() {
    unimplemented!()
}

pub fn pa_xact_finish(_winfo: &mut ParallelApplyWorkerInfo, _remote_lsn: XLogRecPtr) {
    unimplemented!()
}

// C macros isParallelApplyWorker/isTablesyncWorker and the am_*_worker inline
// fns query MyLogicalRepWorker (a process-global). Modeled as methods so the
// global lookup becomes session/task state in Phase 2.
impl LogicalRepWorker {
    pub fn is_parallel_apply_worker(&self) -> bool {
        self.in_use && self.r#type == WorkerState::ParallelApply
    }

    pub fn is_tablesync_worker(&self) -> bool {
        self.in_use && self.r#type == WorkerState::Tablesync
    }

    pub fn am_tablesync_worker(&self) -> bool {
        self.is_tablesync_worker()
    }

    pub fn am_leader_apply_worker(&self) -> bool {
        self.r#type == WorkerState::Apply
    }

    pub fn am_parallel_apply_worker(&self) -> bool {
        self.is_parallel_apply_worker()
    }
}

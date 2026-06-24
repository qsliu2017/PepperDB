//! Translated from PostgreSQL src/include/storage/aio_internal.h
//!
//! AIO declarations internal to the AIO subsystem. Shared memory collapses to
//! Arc-shared heap state under the single-process model; intrusive `dlist` links
//! become owned Rust collections (see notes per field).

use crate::port::pg_iovec::IoVec;
use crate::postgres::Datum;
use crate::storage::aio::{PgAioOp, PgAioOpData, PGAIO_HANDLE_MAX_CALLBACKS};
use crate::storage::aio_types::{
    PgAioResult, PgAioResultStatus, PgAioReturn, PgAioTargetData,
};
use crate::storage::condition_variable::ConditionVariable;
use crate::utils::resowner::ResourceOwner;

/// The maximum number of IOs that can be batch submitted at once.
pub const PGAIO_SUBMIT_BATCH_SIZE: usize = 32;

/// State machine for handles. Handles move (mostly) linearly through all states.
/// Stored in a `u8` field; sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PgAioHandleState {
    /// not in use
    Idle = 0,
    /// returned by pgaio_io_acquire(); next is Defined or Idle
    HandedOut,
    /// pgaio_io_start_*() called, IO not yet staged
    Defined,
    /// stage() callbacks done, ready to submit
    Staged,
    /// submitted to the IO method for execution
    Submitted,
    /// IO finished, result not yet processed
    CompletedIo,
    /// shared completion has been called
    CompletedShared,
    /// local completion has been called; handle becomes reusable -> Idle
    CompletedLocal,
}

/// Real definition of the AIO handle. Resolves the level-2 forward decl in
/// `crate::storage::aio_types::PgAioHandle` (and the bare `struct PgAioHandle`
/// uses across the AIO headers).
///
/// In-memory: the C struct keeps state/target/op as raw `u8`s to save space and
/// uses intrusive `dlist_node` links; here the linkage is left to the owning
/// collections in `PgAioBackend`.
pub struct PgAioHandle {
    /// all state updates should go through pgaio_io_update_state()
    pub state: u8,
    /// what are we operating on (PgAioTargetID as u8)
    pub target: u8,
    /// which IO operation (PgAioOp as u8)
    pub op: u8,
    /// bitfield of PgAioHandleFlags
    pub flags: u8,
    pub num_callbacks: u8,
    /// PgAioHandleCallbackID values, kept as u8 to save space
    pub callbacks: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    /// data forwarded to each callback
    pub callbacks_data: [u8; PGAIO_HANDLE_MAX_CALLBACKS],
    /// length of data associated via pgaio_io_set_handle_data_*()
    pub handle_data_len: u8,
    pub owner_procno: i32,
    /// raw result of the IO operation
    pub result: i32,
    // dlist_node node -- linkage tracked by the owning backend list, not embedded.
    pub resowner: ResourceOwner,
    // dlist_node resowner_node -- linkage tracked by the resowner list.
    /// incremented every time the IO handle is reused
    pub generation: u64,
    /// other backends wait on this CV for completion
    pub cv: ConditionVariable,
    /// result of shared callback, passed to issuer callback
    pub distilled_result: PgAioResult,
    /// index into PgAioCtl->iovecs and PgAioCtl->handle_data
    pub iovec_off: u32,
    /// if Some, updated with completion info iff the issuer learns of completion
    pub report_return: Option<Box<PgAioReturn>>,
    /// data necessary for the IO to be performed
    pub op_data: PgAioOpData,
    /// identifies the object undergoing IO (enough to reopen the file)
    pub target_data: PgAioTargetData,
}

/// Per-backend AIO state. Idle/in-flight intrusive `dclist`s become owned
/// `Vec`s of handle indices; the staged array stays a fixed buffer.
pub struct PgAioBackend {
    /// index into PgAioCtl->io_handles
    pub io_handle_off: u32,
    /// IO handles that currently are not used (was dclist_head)
    pub idle_ios: Vec<u32>,
    /// the single handed-out (acquired but not yet defined/released) IO
    pub handed_out_io: Option<Box<PgAioHandle>>,
    /// currently in batchmode? See pgaio_enter_batchmode().
    pub in_batchmode: bool,
    /// IOs that are defined, but not yet submitted
    pub num_staged_ios: u16,
    pub staged_ios: [Option<Box<PgAioHandle>>; PGAIO_SUBMIT_BATCH_SIZE],
    /// in-flight IOs, ordered by submission time (was dclist_head)
    pub in_flight_ios: Vec<u32>,
}

/// Global AIO control. Was a shared-memory segment; collapses to owned heap
/// state (wrap in Arc/lock at the call site for sharing).
pub struct PgAioCtl {
    pub backend_state_count: i32,
    pub backend_state: Vec<PgAioBackend>,
    /// iovec buffers, each owned by a specific backend (was `struct iovec *`).
    pub iovec_count: u32,
    pub iovecs: Vec<Vec<u8>>,
    /// scratch space for, e.g., Buffer IDs needed during completion
    pub handle_data: Vec<u64>,
    pub io_handle_count: u32,
    pub io_handles: Vec<PgAioHandle>,
}

/// Callbacks used to implement an IO method (was the `IoMethodOps` vtable).
/// Routine struct -> trait; per-field `Optional` callbacks -> provided default
/// methods (returning `None`/no-op) so impls override only what they support.
pub trait IoMethodOps {
    /// If an FD is about to be closed, must we wait for in-flight IOs on it?
    const WAIT_ON_FD_BEFORE_CLOSE: bool;

    /// Additional shared memory to reserve for the io_method. Optional.
    fn shmem_size(&self) -> Option<usize> {
        None
    }

    /// Initialize shared memory. `first_time` if AIO shmem was just set up. Optional.
    fn shmem_init(&self, _first_time: bool) {}

    /// Per-backend initialization. Optional.
    fn init_backend(&self) {}

    /// Optional.
    fn needs_synchronous_execution(&self, _ioh: &mut PgAioHandle) -> Option<bool> {
        None
    }

    /// Start executing the passed-in IOs. Required.
    /// Advances state to at least Submitted; returns the IO method's status int.
    fn submit(&self, num_staged_ios: u16, staged_ios: &mut [&mut PgAioHandle]) -> i32;

    /// Wait for the IO to complete. Optional.
    fn wait_one(&self, _ioh: &mut PgAioHandle, _ref_generation: u64) {}
}

/* aio.c */
/// Returns whether the handle was recycled; yields the observed state too.
pub fn pgaio_io_was_recycled(
    _ioh: &mut PgAioHandle,
    _ref_generation: u64,
) -> (bool, PgAioHandleState) {
    unimplemented!()
}
pub fn pgaio_io_stage(_ioh: &mut PgAioHandle, _op: PgAioOp) {
    unimplemented!()
}
pub fn pgaio_io_process_completion(_ioh: &mut PgAioHandle, _result: i32) {
    unimplemented!()
}
pub fn pgaio_io_prepare_submit(_ioh: &mut PgAioHandle) {
    unimplemented!()
}
pub fn pgaio_io_needs_synchronous_execution(_ioh: &mut PgAioHandle) -> bool {
    unimplemented!()
}
pub fn pgaio_io_get_state_name(_ioh: &mut PgAioHandle) -> &'static str {
    unimplemented!()
}
pub fn pgaio_result_status_string(_rs: PgAioResultStatus) -> &'static str {
    unimplemented!()
}
pub fn pgaio_shutdown(_code: i32, _arg: Datum) {
    unimplemented!()
}

/* aio_callback.c */
pub fn pgaio_io_call_stage(_ioh: &mut PgAioHandle) {
    unimplemented!()
}
pub fn pgaio_io_call_complete_shared(_ioh: &mut PgAioHandle) {
    unimplemented!()
}
pub fn pgaio_io_call_complete_local(_ioh: &mut PgAioHandle) -> PgAioResult {
    unimplemented!()
}

/* aio_io.c */
pub fn pgaio_io_perform_synchronously(_ioh: &mut PgAioHandle) {
    unimplemented!()
}
pub fn pgaio_io_get_op_name(_ioh: &mut PgAioHandle) -> &'static str {
    unimplemented!()
}
pub fn pgaio_io_uses_fd(_ioh: &mut PgAioHandle, _fd: i32) -> bool {
    unimplemented!()
}
/// C out-param `struct iovec **iov` + returned length -> (slice, length).
pub fn pgaio_io_get_iovec_length<'a>(_ioh: &'a mut PgAioHandle) -> (&'a mut [IoVec<'a>], i32) {
    unimplemented!()
}

/* aio_target.c */
pub fn pgaio_io_can_reopen(_ioh: &mut PgAioHandle) -> bool {
    unimplemented!()
}
pub fn pgaio_io_reopen(_ioh: &mut PgAioHandle) {
    unimplemented!()
}
pub fn pgaio_io_get_target_name(_ioh: &mut PgAioHandle) -> &'static str {
    unimplemented!()
}

/// The AIO subsystem has verbose debug logging, toggled at build time.
pub const PGAIO_VERBOSE: bool = true;

// pgaio_debug / pgaio_debug_io are ereport() wrappers -> deferred to the elog
// rewrite; the call sites become `log`/`panic!` per the error model.

// pgaio_sync_ops / pgaio_worker_ops / pgaio_uring_ops are the per-method
// IoMethodOps vtable instances; under the trait model each becomes a unit-struct
// impl of IoMethodOps, dispatched over a closed enum (defined with the impls).
// pgaio_method_ops / pgaio_ctl / pgaio_my_backend were process-global pointers;
// they become threaded-through / Arc-shared state. TODO(global).

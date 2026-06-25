//! Translated from PostgreSQL src/include/storage/procsignal.h
//!
//! Type/const surface for inter-task signaling. The registry + per-task slot
//! live in `crate::backend::storage::ipc::procsignal` as idiomatic methods; the
//! C-named free functions below are `#[deprecated]` shims for cross-reference.

// ---------------------------------------------------------------------------
// ProcSignalReason -- reasons for signaling another backend. Translated fully.
// In C the recovery-conflict block uses aliased values (FIRST == DATABASE,
// LAST == STARTUP_DEADLOCK); those alias names become const helpers below.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ProcSignalReason {
    CatchupInterrupt,
    NotifyInterrupt,
    ParallelMessage,
    WalsndInitStopping,
    Barrier,
    LogMemoryContext,
    ParallelApplyMessage,

    // Recovery conflict reasons (RECOVERY_CONFLICT_FIRST aliases DATABASE).
    RecoveryConflictDatabase,
    RecoveryConflictTablespace,
    RecoveryConflictLock,
    RecoveryConflictSnapshot,
    RecoveryConflictLogicalslot,
    RecoveryConflictBufferpin,
    RecoveryConflictStartupDeadlock,

    SlotsyncMessage,
}

pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason =
    ProcSignalReason::RecoveryConflictDatabase;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason =
    ProcSignalReason::RecoveryConflictStartupDeadlock;

pub const NUM_PROCSIGNALS: usize = ProcSignalReason::SlotsyncMessage as usize + 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ProcSignalBarrierType {
    SmgrRelease,
}

// Length of generated query cancel keys.
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// Deprecated C-named shims delegating to the backend registry/slot methods.
// Shmem size/init are dropped under single-process (no shared segment).
// ---------------------------------------------------------------------------

use std::sync::Arc;

use crate::backend::storage::ipc::procsignal::{
    CancelResult, ProcSignal, ProcSignalSlot, SlotKey,
};
use crate::storage::latch::Latch;

// Shmem sizing/init dropped under single-process; retained as no-ops.
#[deprecated(note = "single-process: no shared-memory segment to size")]
#[inline]
pub fn proc_signal_shmem_size() -> usize {
    0
}
#[deprecated(note = "single-process: no shared-memory segment to init")]
#[inline]
pub fn proc_signal_shmem_init() {}

#[deprecated(note = "use ProcSignal::register")]
#[inline]
pub fn proc_signal_init(
    registry: &ProcSignal,
    pid: i32,
    cancel_key: &[u8],
    latch: Arc<Latch>,
) -> (SlotKey, Arc<ProcSignalSlot>) {
    registry.register(pid, cancel_key, latch)
}

#[deprecated(note = "use ProcSignal::send")]
#[inline]
pub fn send_proc_signal(registry: &ProcSignal, target: SlotKey, reason: ProcSignalReason) -> bool {
    registry.send(target, reason)
}

#[deprecated(note = "use ProcSignal::send_cancel")]
#[inline]
pub fn send_cancel_request(registry: &ProcSignal, backend_pid: i32, cancel_key: &[u8]) -> CancelResult {
    registry.send_cancel(backend_pid, cancel_key)
}

#[deprecated(note = "use ProcSignal::emit_barrier")]
#[inline]
pub fn emit_proc_signal_barrier(registry: &ProcSignal, barrier_type: ProcSignalBarrierType) -> u64 {
    registry.emit_barrier(barrier_type)
}
#[deprecated(note = "use ProcSignal::barrier_absorbed (supervisor polls)")]
#[inline]
pub fn wait_for_proc_signal_barrier(registry: &ProcSignal, generation: u64) -> bool {
    registry.barrier_absorbed(generation)
}
#[deprecated(note = "use ProcSignal::process_barrier")]
#[inline]
pub fn process_proc_signal_barrier(registry: &ProcSignal, slot: &ProcSignalSlot) {
    registry.process_barrier(slot)
}

// Tombstone: the OS SIGUSR1 multiplexer is gone. Reasons are delivered by
// `ProcSignal::send` setting a slot reason bit + waking the latch; the owning
// task drains them in its command loop (step 09). No OS handler remains.
#[deprecated(note = "tombstone: SIGUSR1 multiplexing replaced by slot flags + latch")]
#[inline]
pub fn procsignal_sigusr1_handler() {}

// ProcSignalHeader (opaque shmem control struct) is dropped; the Arc-shared
// `ProcSignal` registry replaces it.

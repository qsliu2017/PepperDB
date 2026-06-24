//! Translated from PostgreSQL src/include/storage/procsignal.h
//!
//! Stub. Inter-process signaling collapses under the single-process async model.
//! TODO(procsignal): implement as typed inter-task channels.

use crate::storage::procnumber::ProcNumber;

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
// API stubs. Shmem size/init are dropped under single-process (no shared memory
// segment); kept as no-op signatures for now.
// ---------------------------------------------------------------------------

// Shmem sizing/init dropped under single-process; retained as no-ops.
pub fn proc_signal_shmem_size() -> usize {
    0
}
pub fn proc_signal_shmem_init() {}

pub fn proc_signal_init(_cancel_key: &[u8]) {
    // TODO(procsignal): register this task in the signaling registry.
    unimplemented!()
}

// Returns 0 on success in C; modeled as Result for the eventual channel impl.
pub fn send_proc_signal(
    _pid: i32,
    _reason: ProcSignalReason,
    _proc_number: ProcNumber,
) -> Result<(), ()> {
    // TODO(procsignal): implement as typed inter-task channels.
    unimplemented!()
}

pub fn send_cancel_request(_backend_pid: i32, _cancel_key: &[u8]) {
    // TODO(procsignal): route cancel to the target task.
    unimplemented!()
}

pub fn emit_proc_signal_barrier(_barrier_type: ProcSignalBarrierType) -> u64 {
    // TODO(procsignal): barrier generation counter.
    unimplemented!()
}
pub fn wait_for_proc_signal_barrier(_generation: u64) {
    unimplemented!()
}
pub fn process_proc_signal_barrier() {
    unimplemented!()
}

// SIGNAL_ARGS -> the OS handler signature disappears under task channels.
pub fn procsignal_sigusr1_handler() {
    // TODO(procsignal): replaced by channel receive in the owning task.
    unimplemented!()
}

// ProcSignalHeader is an opaque shmem control struct; dropped under
// single-process (the `ProcSignal` global and EXEC_BACKEND path are gone).
// TODO(procsignal): replace with an Arc-shared registry of task channels.

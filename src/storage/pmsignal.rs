//! Translated from PostgreSQL src/include/storage/pmsignal.h

/// Reasons for signaling the postmaster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PMSignalReason {
    RecoveryStarted = 0,
    RecoveryConsistent,
    BeginHotStandby,
    RotateLogfile,
    StartAutovacLauncher,
    StartAutovacWorker,
    BackgroundWorkerChange,
    StartWalreceiver,
    AdvanceStateMachine,
    XlogIsShutdown,
}

pub const NUM_PMSIGNALS: usize = PMSignalReason::XlogIsShutdown as usize + 1;

/// Reasons why the postmaster would send SIGQUIT to its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum QuitSignalReason {
    NotSent = 0, // postmaster hasn't sent SIGQUIT
    ForCrash,    // some other backend bought the farm
    ForStop,     // immediate stop was commanded
}

// Tombstone: the postmaster-signal multiplexer (PMSignalData shmem segment, the
// shmem size/init, send/check, child-slot bookkeeping, and death-signal machinery)
// is postmaster IPC over shared memory - a non-goal under the single-process model.
// Inter-task signaling is done with tokio (Notify / channels). Only the standalone
// reason enums above carry over.

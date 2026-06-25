//! Translated from PostgreSQL src/backend/postmaster/auxprocess.c
//!
//! `AuxiliaryProcessMainCommon` -- the shared setup an auxiliary task (bgwriter,
//! checkpointer, walwriter, ...) runs before its own loop. Under the
//! single-process async model an aux process is a tokio task, so this is a thin
//! async helper that establishes the per-task basics and hands back the handles
//! the (step-17) aux loops need:
//! - a [`Session`] / identity for the aux `BackendType` (step 08),
//! - a registered proc-signal slot + [`Latch`] (step 04),
//! - a resource owner for buffer pins held outside transactions (step 06).
//!
//! The caller scopes the returned handles with the per-task task-locals
//! (`session::scope` / `procsignal::scope` / `resowner::scope`) around its loop,
//! and services interrupts via [`process_main_loop_interrupts`]. The concrete aux
//! loops are step 17; this only provides their common cradle.

use std::sync::Arc;

use crate::backend::storage::ipc::procsignal::{ProcSignal, ProcSignalSlot, SlotKey};
use crate::backend::utils::init::postinit::backend_task_init;
use crate::backend::utils::resowner::resowner::ResourceOwner;
use crate::miscadmin::BackendType;
use crate::session::Session;
use crate::storage::latch::Latch;

// Re-export the aux main-loop interrupt service entry (step 04 / interrupt.c) so
// step-17 loops call it as `auxprocess::process_main_loop_interrupts()`.
pub use crate::backend::postmaster::interrupt::process_main_loop_interrupts;

/// The per-task handles an auxiliary task holds for its lifetime. The aux loop
/// scopes `session` / `slot` / `owner` as task-locals and rings `slot.latch` /
/// reads `slot.flags` for wakeups; on exit it deregisters via `slot_key`.
pub struct AuxProcess {
    pub session: Arc<Session>,
    pub slot: Arc<ProcSignalSlot>,
    pub slot_key: SlotKey,
    pub owner: ResourceOwner,
}

/// PG `AuxiliaryProcessMainCommon`. Build the aux task's identity + proc-signal
/// slot + resource owner and switch to normal processing mode. No catalog / auth
/// (aux processes skip the full `InitPostgres`); `BaseInit`'s subsystems are
/// deferred (steps 12-15).
///
/// `proc_signal` is the shared registry (from `SharedState`); `backend_type` is
/// the aux flavor (e.g. `BackendType::CHECKPOINTER`).
pub async fn auxiliary_process_main_common(
    proc_signal: &Arc<ProcSignal>,
    backend_type: BackendType,
) -> AuxProcess {
    // Identity slice (step 08) for this aux task.
    let session = backend_task_init(backend_type).await;
    let proc_pid = session.proc_pid();

    // PG's BaseInit() local-subsystem setup -- every subsystem it touches is
    // deferred (steps 12-15); nothing to do yet.
    crate::backend::utils::init::postinit::base_init();

    // Register a proc-signal slot. Aux processes have no query-cancel key
    // (PG passes ProcSignalInit(NULL, 0)); an empty key disables cancellation.
    let latch = Arc::new(Latch::new());
    let (slot_key, slot) = proc_signal.register(proc_pid, &[], latch);

    // Aux processes don't run transactions but may pin buffers outside one
    // (PG's CreateAuxProcessResourceOwner).
    let owner = ResourceOwner::create(None, "AuxiliaryProcess");

    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);

    // TODO(step17): pgstat_beinit / before_shmem_exit(ShutdownAuxiliaryProcess)
    // and the concrete aux loop are wired by the individual aux tasks.
    AuxProcess { session, slot, slot_key, owner }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sets_up_aux_identity_and_slot() {
        let reg = Arc::new(ProcSignal::new());
        let aux = auxiliary_process_main_common(&reg, BackendType::CHECKPOINTER).await;
        assert_eq!(aux.session.backend_type(), BackendType::CHECKPOINTER);
        assert_eq!(reg.len(), 1, "aux slot should be registered");

        // Deregister cleans up.
        reg.deregister(aux.slot_key);
        assert_eq!(reg.len(), 0);
    }
}

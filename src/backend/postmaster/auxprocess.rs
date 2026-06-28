//! Common startup for auxiliary processes. Translated from backend/postmaster/auxprocess.c.
//!
//! Auxiliary processes -- the background writer, checkpointer, WAL writer, WAL
//! receiver, and the startup process -- are not full backends: they never run
//! transactions and skip the complete `InitPostgres` sequence. They still need a
//! handful of subsystems lit up before they enter their own service loop. In
//! PostgreSQL `AuxiliaryProcessMainCommon` performs that shared initialization:
//! it creates a `PGPROC` so the process can take LWLocks and reach shared memory,
//! runs `BaseInit`, registers a proc-signal slot, and creates a resource owner to
//! track buffer pins acquired outside any transaction. It also installs a
//! before-shutdown callback that releases held LWLocks on exit, which matters
//! chiefly on the error-exit path.
//!
//! An auxiliary process in PepperDB is a tokio task rather than a forked child,
//! so the equivalent setup is an async helper that returns the per-task handles
//! the auxiliary loop holds for its lifetime: a session identity carrying the
//! auxiliary `BackendType`, a registered proc-signal slot paired with a wakeup
//! [`Latch`], and a resource owner for out-of-transaction buffer pins. The caller
//! scopes those handles as task-locals around its loop and services interrupts
//! through [`process_main_loop_interrupts`].
//!
//! Two cradles are provided. The plain one suits auxiliaries that are woken only
//! by proc-signal or barrier sends; it allocates a fresh latch as the sole wakeup
//! source. The `_with_proc` variant additionally claims a `PGPROC` slot so that
//! backends can wake the task by its `ProcNumber` (as the checkpointer, WAL
//! writer, and background writer require); it registers the proc-signal slot
//! against that `PGPROC`'s own latch, mirroring PostgreSQL's invariant that an
//! auxiliary process's `MyLatch` is its `MyProc->procLatch`, so every wakeup path
//! lands on a single latch. Process-wide shared memory is replaced by an
//! Arc-shared proc-signal registry and `PGPROC` arena, and the before-shutdown
//! LWLock-release callback is realized as RAII cleanup in the individual auxiliary
//! tasks rather than a registered exit hook.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::sync::Arc;

use crate::backend::storage::ipc::procsignal::{ProcSignal, ProcSignalSlot, SlotKey};
use crate::backend::storage::lmgr::proc::InitAuxiliaryProcess;
use crate::backend::utils::init::postinit::backend_task_init;
use crate::backend::utils::resowner::resowner::ResourceOwner;
use crate::miscadmin::BackendType;
use crate::session::Session;
use crate::storage::latch::Latch;
use crate::storage::proc::{current_proc_number, ProcGlobal};
use crate::storage::procnumber::ProcNumber;

// Re-export the aux main-loop interrupt service entry (step 04 / interrupt.c) so
// step-17 loops call it as `auxprocess::process_main_loop_interrupts()`.
pub use crate::backend::postmaster::interrupt::process_main_loop_interrupts;

/// Test-only serialization across ALL aux-task tests (checkpointer / bgwriter /
/// walwriter). They share the single process-wide `ProcGlobal` arena + the aux
/// PGPROC slots + the `ProcGlobal.<role>_proc` advertisements, so two aux tasks
/// from different modules running concurrently would contend on that shared state
/// (e.g. an aux task claiming/returning slots while a checkpointer WAIT is in
/// flight). Every aux-task test holds this guard for its duration.
#[cfg(test)]
pub(crate) async fn aux_test_serial() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

/// The per-task handles an auxiliary task holds for its lifetime. The aux loop
/// scopes `session` / `slot` / `owner` as task-locals and rings `latch` /
/// reads `slot.flags` for wakeups; on exit it deregisters via `slot_key`.
pub struct AuxProcess {
    pub session: Arc<Session>,
    pub slot: Arc<ProcSignalSlot>,
    pub slot_key: SlotKey,
    pub owner: ResourceOwner,
    /// The task's single wakeup latch -- the loop waits AND resets exactly this
    /// one. For the `_with_proc` cradle it is the claimed PGPROC `proc_latch`
    /// (also the latch the proc-signal slot was registered with: PG's
    /// `MyLatch == MyProc->procLatch` for an aux proc), so a wake by `ProcNumber`
    /// (the PGPROC latch) AND a proc-signal/barrier send hit the same latch -- no
    /// second sticky latch to busy-spin on. For the plain cradle it is the
    /// freshly created slot latch (same object as `slot.latch`).
    pub latch: Arc<Latch>,
    /// Set when this aux flavor claimed a PGPROC (the `_with_proc` variant). The
    /// task advertises this in `ProcGlobal.<role>_proc` so backends can wake it
    /// by `ProcNumber`. `INVALID_PROC_NUMBER` for the plain cradle.
    pub proc_number: ProcNumber,
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
    // Plain cradle: a fresh slot latch is the only wakeup source.
    let latch = Arc::new(Latch::new());
    aux_common(proc_signal, backend_type, latch, crate::storage::procnumber::INVALID_PROC_NUMBER)
        .await
}

/// Shared body of both cradles. Builds identity + resource owner, registers the
/// proc-signal slot WITH the given `latch`, and returns the handles. `latch` is
/// the task's single wakeup latch; for `_with_proc` it is the claimed PGPROC
/// `proc_latch` so the slot and the PGPROC-latch wakeup share one latch.
async fn aux_common(
    proc_signal: &Arc<ProcSignal>,
    backend_type: BackendType,
    latch: Arc<Latch>,
    proc_number: ProcNumber,
) -> AuxProcess {
    // Identity slice (step 08) for this aux task.
    let session = backend_task_init(backend_type).await;
    let proc_pid = session.proc_pid();

    // PG's BaseInit() local-subsystem setup -- every subsystem it touches is
    // deferred (steps 12-15); nothing to do yet.
    crate::backend::utils::init::postinit::base_init();

    // Register a proc-signal slot WITH `latch`. Aux processes have no query-cancel
    // key (PG passes ProcSignalInit(NULL, 0)); an empty key disables cancellation.
    // A `_with_proc` cradle holds a PGPROC, so the slot's proc_number must be its
    // MyProcNumber (PG's psh_slot[MyProcNumber]) -- not the slab index -- so a
    // wake-by-ProcNumber (sinval catchup) targets this slot. The plain cradle has
    // no PGPROC and falls back to the slab index.
    let (slot_key, slot) = if proc_number == crate::storage::procnumber::INVALID_PROC_NUMBER {
        proc_signal.register(proc_pid, &[], latch.clone())
    } else {
        proc_signal.register_at(proc_number, proc_pid, &[], latch.clone())
    };

    // Aux processes don't run transactions but may pin buffers outside one
    // (PG's CreateAuxProcessResourceOwner).
    let owner = ResourceOwner::create(None, "AuxiliaryProcess");

    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);

    // TODO(pgstat): pgstat_beinit. The before_shmem_exit(ShutdownAuxiliaryProcess)
    // cleanup is the aux exit guard (RAII Drop) in the individual aux tasks.
    AuxProcess { session, slot, slot_key, owner, latch, proc_number }
}

/// PG `AuxiliaryProcessMainCommon` for aux tasks that backends wake BY
/// `ProcNumber` (checkpointer / walwriter / bgwriter). Same as
/// [`auxiliary_process_main_common`] but also claims a PGPROC via
/// `InitAuxiliaryProcess` and reports its `proc_number`. The caller advertises
/// that number in `ProcGlobal.<role>_proc`, and waits on the PGPROC `proc_latch`
/// (reached through the arena) as its single wakeup latch -- PG's
/// `MyLatch == MyProc->procLatch` for an aux process.
///
/// Must run inside `my_proc_scope` (the PGPROC `task_local` slot) so
/// `InitAuxiliaryProcess` can publish `MyProcNumber`.
pub async fn auxiliary_process_main_common_with_proc(
    proc_signal: &Arc<ProcSignal>,
    backend_type: BackendType,
) -> AuxProcess {
    // Claim one of the auxiliary PGPROC slots + initialize its proc_latch FIRST,
    // so we can register the proc-signal slot against that SAME latch (one unified
    // wakeup: PG's MyLatch == MyProc->procLatch for an aux proc).
    InitAuxiliaryProcess();
    let proc_number = current_proc_number();
    let g = ProcGlobal::expect();
    // SAFETY: read-only clone of our own freshly claimed slot's proc_latch Arc;
    // proc_latch is internally synchronized and InitAuxiliaryProcess just inited it.
    let proc_latch = unsafe { g.proc(proc_number).expect("our aux PGPROC").proc_latch.clone() };

    aux_common(proc_signal, backend_type, proc_latch, proc_number).await
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
        assert_eq!(
            aux.proc_number,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
            "plain cradle claims no PGPROC"
        );

        // Deregister cleans up.
        reg.deregister(aux.slot_key);
        assert_eq!(reg.len(), 0);
    }

    #[tokio::test]
    async fn with_proc_claims_a_pgproc() {
        // Publish a ProcGlobal so InitAuxiliaryProcess can claim an aux slot.
        let shared = crate::shared_state::SharedState::new(
            crate::shared_state::SharedStateConfig::default(),
        );
        let _ = ProcGlobal::get().is_some() || crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        let g = ProcGlobal::expect().clone();

        crate::storage::proc::my_proc_scope(async {
            let aux = auxiliary_process_main_common_with_proc(
                shared.proc_signal(),
                BackendType::CHECKPOINTER,
            )
            .await;
            assert_ne!(
                aux.proc_number,
                crate::storage::procnumber::INVALID_PROC_NUMBER,
                "the _with_proc cradle claims a PGPROC"
            );
            // The aux's single latch IS the claimed PGPROC's proc_latch AND the
            // proc-signal slot's latch -- one unified wakeup (no second latch).
            // SAFETY: read-only access to our own slot's latch.
            let proc_latch = unsafe { &g.proc(aux.proc_number).expect("aux PGPROC").proc_latch };
            assert!(
                Arc::ptr_eq(&aux.latch, proc_latch),
                "aux.latch is the PGPROC proc_latch"
            );
            assert!(
                Arc::ptr_eq(&aux.latch, &aux.slot.latch),
                "the proc-signal slot was registered with the SAME latch"
            );
            // The unified latch is usable (set/wait).
            aux.latch.set();
            aux.latch.wait().await; // set-before-wait returns immediately
        })
        .await;
    }
}

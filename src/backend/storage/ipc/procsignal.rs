//! Translated from PostgreSQL src/backend/storage/ipc/procsignal.c
//!
//! Interprocess signaling collapses to inter-task signaling under the
//! single-process async model. The shmem `ProcSignalSlots` array becomes a
//! generational slab of per-task [`ProcSignalSlot`]s (each shared as an `Arc`),
//! the SIGUSR1 multiplexer becomes per-flag atomics + a [`Latch`] wakeup, and
//! query-cancel routing becomes a pid -> slot lookup with a constant-time
//! cancel-key compare.
//!
//! DESIGN: per-task interrupt flags must be settable by ANOTHER task (a cancel
//! task, a timeout task). So the flags live in the SHARED slot as atomics; the
//! owning task keeps a `task_local` `Arc` handle to its own slot for fast reads.
//! The cancel key is a separate random token stored in the slot, compared in
//! constant time -- it is NOT the slot identity.
//!
//! RESOLVED (step09): ProcessInterrupts (tcop/postgres.rs) and the miscadmin
//! C-named flag accessors now READ and CLEAR these slot flags. The slot is the
//! canonical per-task interrupt state; the miscadmin `static mut` flags were
//! retired into `#[deprecated]` slot-backed accessors.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::storage::latch::Latch;
use crate::storage::procnumber::ProcNumber;
use crate::storage::procsignal::{
    MAX_CANCEL_KEY_LENGTH, NUM_PROCSIGNALS, ProcSignalBarrierType, ProcSignalReason,
};

/// The set of per-task interrupt flags (PG's `miscadmin.h` signal-handler
/// flags, modeled per-slot so a foreign task can raise them). Each is an
/// independent `AtomicBool`; setters use `Release`, loaders `Acquire`, so a flag
/// observed set is ordered after the setter's prior writes.
#[derive(Debug, Default)]
pub struct InterruptFlags {
    pub interrupt_pending: AtomicBool,
    pub query_cancel_pending: AtomicBool,
    pub proc_die_pending: AtomicBool,
    pub idle_in_transaction_session_timeout_pending: AtomicBool,
    pub transaction_timeout_pending: AtomicBool,
    pub idle_session_timeout_pending: AtomicBool,
    pub proc_signal_barrier_pending: AtomicBool,
    pub log_memory_context_pending: AtomicBool,
    pub idle_stats_update_timeout_pending: AtomicBool,
    pub check_client_connection_pending: AtomicBool,
    pub client_connection_lost: AtomicBool,
}

/// A per-task signaling slot, shared as `Arc<ProcSignalSlot>`. The owning task
/// reads its own flags via the `task_local` handle; foreign tasks set flags and
/// reason bits through the registry, which holds the same `Arc`.
// No `Debug`: `Latch` is not `Debug` (it wraps a `Notify`).
pub struct ProcSignalSlot {
    /// Identity. `pid` is the lookup key for cancel requests; `proc_number` is
    /// the slab index this slot was registered at.
    pub pid: i32,
    pub proc_number: ProcNumber,

    /// Interrupt flags (cross-task settable atomics).
    pub flags: InterruptFlags,

    /// One bit per [`ProcSignalReason`], for `SendProcSignal`. Indexed by the
    /// reason's discriminant.
    reason_bits: [AtomicBool; NUM_PROCSIGNALS],

    /// Query-cancel token + its valid length. A separate random secret, NOT the
    /// slot identity. `cancel_key_len == 0` means cancellation is disabled.
    cancel_key: [u8; MAX_CANCEL_KEY_LENGTH],
    cancel_key_len: usize,

    /// Highest barrier generation this slot has absorbed. Provisional barrier
    /// support (see registry methods).
    barrier_generation: AtomicU64,

    /// Wakeup for the owning task. `set()` after raising a flag/bit makes a
    /// task blocked in `latch.wait()` return and re-check its flags.
    pub latch: Arc<Latch>,
}

impl ProcSignalSlot {
    fn new(
        pid: i32,
        proc_number: ProcNumber,
        cancel_key: &[u8],
        latch: Arc<Latch>,
        initial_generation: u64,
    ) -> Self {
        let mut key = [0u8; MAX_CANCEL_KEY_LENGTH];
        let len = cancel_key.len().min(MAX_CANCEL_KEY_LENGTH);
        key[..len].copy_from_slice(&cancel_key[..len]);
        Self {
            pid,
            proc_number,
            flags: InterruptFlags::default(),
            reason_bits: std::array::from_fn(|_| AtomicBool::new(false)),
            cancel_key: key,
            cancel_key_len: len,
            barrier_generation: AtomicU64::new(initial_generation),
            latch,
        }
    }

    /// Set a reason bit and `interrupt_pending`. `Release` so the owning task's
    /// `Acquire` load sees these writes after observing the flag.
    fn raise_reason(&self, reason: ProcSignalReason) {
        self.reason_bits[reason as usize].store(true, Ordering::Release);
        self.flags.interrupt_pending.store(true, Ordering::Release);
    }

    /// Test-and-clear a reason bit. Returns whether it had been set. (PG's
    /// `CheckProcSignal`.)
    pub fn take_reason(&self, reason: ProcSignalReason) -> bool {
        self.reason_bits[reason as usize].swap(false, Ordering::AcqRel)
    }

    /// Load a reason bit without clearing it.
    pub fn reason_is_set(&self, reason: ProcSignalReason) -> bool {
        self.reason_bits[reason as usize].load(Ordering::Acquire)
    }

    /// Raise query-cancel: set `query_cancel_pending` + `interrupt_pending`.
    fn raise_cancel(&self) {
        self.flags.query_cancel_pending.store(true, Ordering::Release);
        self.flags.interrupt_pending.store(true, Ordering::Release);
    }
}

/// Constant-time byte comparison. Returns true iff `a` and `b` are equal. Folds
/// all byte differences into one accumulator so timing does not reveal the first
/// mismatch position. Length mismatch short-circuits (lengths are not secret).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Outcome of a cancel request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    /// pid matched and the cancel key was correct; cancel was raised.
    Accepted,
    /// pid matched but the cancel key was wrong; nothing was raised.
    WrongKey,
    /// No live slot with that pid.
    NoSuchBackend,
}

/// The signaling registry: a generational slab of slots plus the global barrier
/// generation. Held behind a std `Mutex` for short, non-`.await` critical
/// sections (lookups + slab mutations). Slot flag mutation happens through the
/// cloned `Arc` AFTER the lock is dropped.
#[derive(Default)]
pub struct ProcSignal {
    inner: Mutex<Registry>,
    /// Highest barrier generation in existence. Provisional.
    barrier_generation: AtomicU64,
}

#[derive(Default)]
struct Registry {
    slots: crate::storage::procnumber::GenSlab<Arc<ProcSignalSlot>>,
}

/// A handle to a registered slot: the generational [`Key`] for deregistration
/// and foreign lookup.
pub type SlotKey = crate::storage::procnumber::Key<Arc<ProcSignalSlot>>;

impl ProcSignal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a task in the registry (PG's `ProcSignalInit`). The slab index
    /// becomes the slot's `proc_number`. Returns the key (for deregister/lookup)
    /// and the shared slot the owning task should publish as its `task_local`.
    pub fn register(
        &self,
        pid: i32,
        cancel_key: &[u8],
        latch: Arc<Latch>,
    ) -> (SlotKey, Arc<ProcSignalSlot>) {
        let generation = self.barrier_generation.load(Ordering::Acquire);
        let mut reg = self.inner.lock().unwrap();
        // Reserve the index first so the slot can record its own proc_number.
        let placeholder = Arc::new(ProcSignalSlot::new(
            pid,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
            cancel_key,
            latch.clone(),
            generation,
        ));
        let key = reg.slots.insert(placeholder);
        let proc_number = key.as_proc_number();
        let slot = Arc::new(ProcSignalSlot::new(
            pid,
            proc_number,
            cancel_key,
            latch,
            generation,
        ));
        *reg.slots.get_mut(key).unwrap() = slot.clone();
        (key, slot)
    }

    /// Remove a slot on task exit (PG's `CleanupProcSignalState`). A stale key
    /// (already removed, or generation advanced) is a no-op.
    pub fn deregister(&self, key: SlotKey) {
        self.inner.lock().unwrap().slots.remove(key);
    }

    /// Number of live slots.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Send a signal to a task by its slot key (PG's `SendProcSignal`). Sets the
    /// reason bit + `interrupt_pending`, then wakes the target latch. A stale
    /// key is a no-op (returns false). The latch is woken AFTER the lock drops.
    pub fn send(&self, target: SlotKey, reason: ProcSignalReason) -> bool {
        let slot = {
            let reg = self.inner.lock().unwrap();
            reg.slots.get(target).cloned()
        };
        slot.is_some_and(|slot| {
            slot.raise_reason(reason);
            slot.latch.set();
            true
        })
    }

    /// Find the slot for `pid` and clone its `Arc`. Lock is held only for the
    /// scan + clone.
    fn slot_by_pid(&self, pid: i32) -> Option<Arc<ProcSignalSlot>> {
        let reg = self.inner.lock().unwrap();
        reg.slots
            .iter()
            .find(|(_, slot)| slot.pid == pid)
            .map(|(_, slot)| slot.clone())
    }

    /// Process a query-cancel request (PG's `SendCancelRequest`). Finds the slot
    /// by pid, compares the cancel key in constant time, and on a match raises
    /// `query_cancel_pending` + `interrupt_pending` and wakes the latch.
    pub fn send_cancel(&self, pid: i32, key: &[u8]) -> CancelResult {
        let Some(slot) = self.slot_by_pid(pid) else {
            return CancelResult::NoSuchBackend;
        };
        if slot.cancel_key_len == 0 {
            return CancelResult::WrongKey;
        }
        if constant_time_eq(&slot.cancel_key[..slot.cancel_key_len], key) {
            slot.raise_cancel();
            slot.latch.set();
            CancelResult::Accepted
        } else {
            CancelResult::WrongKey
        }
    }

    // --- Barrier (provisional) ---------------------------------------------
    //
    // PROVISIONAL: a minimal generation-counter barrier. `emit` bumps the global
    // generation and raises the barrier reason + pending bit on every live slot;
    // a task absorbs it via `process_barrier` (advancing its slot generation);
    // `wait_for_barrier` polls until every slot has caught up. The real version
    // (per-type check mask, condition-variable wakeups, retry-on-failure) lands
    // with the barrier-using subsystems; `_barrier_type` is accepted but the
    // per-type mask is not yet modeled.

    /// PROVISIONAL. Raise a barrier on all live slots and return its generation.
    pub fn emit_barrier(&self, _barrier_type: ProcSignalBarrierType) -> u64 {
        let generation = self.barrier_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let slots: Vec<Arc<ProcSignalSlot>> = {
            let reg = self.inner.lock().unwrap();
            reg.slots.iter().map(|(_, slot)| slot.clone()).collect()
        };
        for slot in slots {
            slot.flags
                .proc_signal_barrier_pending
                .store(true, Ordering::Release);
            slot.raise_reason(ProcSignalReason::Barrier);
            slot.latch.set();
        }
        generation
    }

    /// PROVISIONAL. Absorb pending barriers for `slot`, advancing its generation
    /// to the global one. Real per-type processing is deferred.
    pub fn process_barrier(&self, slot: &ProcSignalSlot) {
        if !slot
            .flags
            .proc_signal_barrier_pending
            .swap(false, Ordering::AcqRel)
        {
            return;
        }
        let shared = self.barrier_generation.load(Ordering::Acquire);
        slot.barrier_generation.store(shared, Ordering::Release);
    }

    /// PROVISIONAL. Return whether every live slot has absorbed `generation`.
    /// (The real `WaitForProcSignalBarrier` blocks; here the supervisor polls.)
    pub fn barrier_absorbed(&self, generation: u64) -> bool {
        let reg = self.inner.lock().unwrap();
        reg.slots
            .iter()
            .all(|(_, slot)| slot.barrier_generation.load(Ordering::Acquire) >= generation)
    }
}

// ---------------------------------------------------------------------------
// task_local slot handle
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The current task's own slot. Published by `scope` once registered.
    static MY_PROC_SIGNAL_SLOT: Arc<ProcSignalSlot>;
}

/// The current task's slot. Panics if not inside a [`scope`].
pub fn current() -> Arc<ProcSignalSlot> {
    try_current().expect("no ProcSignalSlot in scope for this task")
}

/// The current task's slot, or `None` if not inside a [`scope`].
pub fn try_current() -> Option<Arc<ProcSignalSlot>> {
    MY_PROC_SIGNAL_SLOT.try_with(std::clone::Clone::clone).ok()
}

/// Run `f` (an async block) with `slot` published as the task-local handle.
/// Test/helper entry point; the real backend task wraps its command loop here.
pub async fn scope<F, T>(slot: Arc<ProcSignalSlot>, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    MY_PROC_SIGNAL_SLOT.scope(slot, f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn key32(seed: u8) -> Vec<u8> {
        (0..MAX_CANCEL_KEY_LENGTH as u8).map(|i| i ^ seed).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_request_wakes_target_and_sets_flags() {
        let reg = Arc::new(ProcSignal::new());
        let latch = Arc::new(Latch::new());
        let ckey = key32(0xAB);
        let (_key, slot) = reg.register(101, &ckey, latch.clone());

        // Task A awaits its latch.
        let a_slot = slot.clone();
        let a = tokio::spawn(async move {
            a_slot.latch.wait().await;
            (
                a_slot.flags.query_cancel_pending.load(Ordering::Acquire),
                a_slot.flags.interrupt_pending.load(Ordering::Acquire),
            )
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!a.is_finished(), "A should be parked on latch.wait()");

        // Task B sends the cancel by pid + key.
        assert_eq!(reg.send_cancel(101, &ckey), CancelResult::Accepted);

        let (qc, ip) = tokio::time::timeout(Duration::from_secs(1), a)
            .await
            .expect("A should wake")
            .unwrap();
        assert!(qc, "query_cancel_pending must be set");
        assert!(ip, "interrupt_pending must be set");
    }

    #[tokio::test]
    async fn cancel_request_wrong_key_sets_nothing() {
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(202, &key32(0x11), latch);

        assert_eq!(reg.send_cancel(202, &key32(0x22)), CancelResult::WrongKey);
        assert!(!slot.flags.query_cancel_pending.load(Ordering::Acquire));
        assert!(!slot.flags.interrupt_pending.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn cancel_unknown_pid() {
        let reg = ProcSignal::new();
        assert_eq!(reg.send_cancel(999, &key32(0)), CancelResult::NoSuchBackend);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_proc_signal_sets_reason_and_wakes() {
        let reg = Arc::new(ProcSignal::new());
        let latch = Arc::new(Latch::new());
        let (key, slot) = reg.register(303, &key32(1), latch);

        let a_slot = slot.clone();
        let a = tokio::spawn(async move {
            a_slot.latch.wait().await;
            (
                a_slot.reason_is_set(ProcSignalReason::NotifyInterrupt),
                a_slot.flags.interrupt_pending.load(Ordering::Acquire),
            )
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(reg.send(key, ProcSignalReason::NotifyInterrupt));

        let (reason, ip) = tokio::time::timeout(Duration::from_secs(1), a)
            .await
            .expect("A should wake")
            .unwrap();
        assert!(reason, "reason bit must be set");
        assert!(ip, "interrupt_pending must be set");
    }

    #[test]
    fn constant_time_eq_accepts_and_rejects() {
        assert!(constant_time_eq(&key32(7), &key32(7)));
        assert!(!constant_time_eq(&key32(7), &key32(8)));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }

    #[tokio::test]
    async fn task_local_current_within_and_outside_scope() {
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(404, &key32(2), latch);

        assert!(try_current().is_none(), "no slot outside a scope");

        let pid = scope(slot.clone(), async { current().pid }).await;
        assert_eq!(pid, 404);
        assert!(try_current().is_none(), "scope must not leak");
    }

    #[tokio::test]
    async fn deregister_removes_slot_and_stale_key_is_noop() {
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (key, _slot) = reg.register(505, &key32(3), latch);
        assert_eq!(reg.len(), 1);

        reg.deregister(key);
        assert_eq!(reg.len(), 0);
        // A send via the stale generational key resolves to nothing.
        assert!(!reg.send(key, ProcSignalReason::NotifyInterrupt));
    }

    #[tokio::test]
    async fn barrier_emit_and_absorb() {
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(606, &key32(4), latch);

        let generation = reg.emit_barrier(ProcSignalBarrierType::SmgrRelease);
        assert!(!reg.barrier_absorbed(generation), "not yet absorbed");
        assert!(slot.reason_is_set(ProcSignalReason::Barrier));

        reg.process_barrier(&slot);
        assert!(reg.barrier_absorbed(generation), "absorbed after processing");
    }
}

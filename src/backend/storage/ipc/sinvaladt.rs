//! Translated from PostgreSQL src/backend/storage/ipc/sinvaladt.c
//!
//! POSTGRES shared cache invalidation data manager.
//!
//! Conceptually the SI messages live in an infinite array; `max_msg_num` is the
//! next subscript to write, `min_msg_num` the smallest not-yet-read-by-all, and
//! each active backend has a `next_msg_num`. In reality they sit in a circular
//! buffer of `MAXNUMMESSAGES` entries (index = MsgNum % MAXNUMMESSAGES). On
//! overflow we set the "reset" flag for backends that fell too far behind.
//!
//! PG protects the SISeg with two LWLocks + one spinlock; we follow that scheme
//! faithfully (the binding step-16 decision):
//!   - `SInvalWriteLock` (exclusive only)  -> `write: Mutex<SIWriteState>`
//!   - `SInvalReadLock`  (shared readers / exclusive cleanup) -> `read_lock: RwLock<()>`
//!   - `msgnumLock` spinlock (a memory barrier on maxMsgNum) -> `max_msg_num: AtomicI32`

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::cell::UnsafeCell;

use crate::c::LocalTransactionId;
use crate::storage::lock::{INVALID_LOCAL_TRANSACTION_ID, local_transaction_id_is_valid};
use crate::storage::proc::current_proc_number;
use crate::storage::procnumber::ProcNumber;
use crate::storage::sinval::SharedInvalidationMessage;

// === Configurable parameters (verbatim from sinvaladt.c) ===

/// Max number of shared-inval messages we can buffer. Power of 2.
const MAXNUMMESSAGES: i32 = 4096;
/// How often to reduce MsgNum variables to avoid overflow. Multiple of MAXNUMMESSAGES.
const MSGNUMWRAPAROUND: i32 = MAXNUMMESSAGES * 262144;
/// Min messages in the buffer before we bother to call SICleanupQueue.
const CLEANUP_MIN: i32 = MAXNUMMESSAGES / 2;
/// How often (in messages) to call SICleanupQueue once we exceed CLEANUP_MIN.
const CLEANUP_QUANTUM: i32 = MAXNUMMESSAGES / 16;
/// Min messages a backend must fall behind before we send it a catchup signal.
const SIG_THRESHOLD: i32 = MAXNUMMESSAGES / 2;
/// Max messages to push per iteration of SIInsertDataEntries.
const WRITE_QUANTUM: i32 = 64;

/// Per-backend state in the shared invalidation structure (PG `ProcState`).
///
/// All fields are interior-mutable atomics: the array is shared and entries are
/// touched under `read_lock` shared mode, where two readers concurrently mutate
/// DISTINCT entries (each only its own). The atomics give the interior
/// mutability that makes those concurrent distinct-entry writes sound.
#[derive(Debug)]
struct ProcState {
    /// PID of backend, for signaling. Zero in an inactive entry.
    proc_pid: AtomicI32,
    /// Next message number to read. Meaningless if `proc_pid == 0` or reset.
    next_msg_num: AtomicI32,
    /// Backend needs to reset its state.
    reset_state: AtomicBool,
    /// Backend has been sent a catchup signal.
    signaled: AtomicBool,
    /// Backend has unread messages.
    has_messages: AtomicBool,
    /// Backend only sends invalidations, never receives them (Startup process).
    send_only: AtomicBool,
    /// Next LocalTransactionId for this idle slot. Meaningless while active.
    next_lxid: AtomicU32,
}

impl ProcState {
    /// Build an inactive slot (PG SharedInvalShmemInit's per-slot init).
    fn inactive() -> Self {
        Self {
            proc_pid: AtomicI32::new(0),
            next_msg_num: AtomicI32::new(0),
            reset_state: AtomicBool::new(false),
            signaled: AtomicBool::new(false),
            has_messages: AtomicBool::new(false),
            send_only: AtomicBool::new(false),
            next_lxid: AtomicU32::new(INVALID_LOCAL_TRANSACTION_ID.0),
        }
    }
}

/// The part of the SISeg serialized by `SInvalWriteLock` (PG: minMsgNum,
/// nextThreshold, numProcs, pgprocnos and the buffer writes). `num_procs` is
/// implicit in `pgprocnos.len()`.
#[derive(Debug)]
struct SIWriteState {
    /// Oldest message still needed.
    min_msg_num: i32,
    /// Number of messages at which to call SICleanupQueue.
    next_threshold: i32,
    /// Dense list of in-use slot indexes (PG `pgprocnos`).
    pgprocnos: Vec<ProcNumber>,
}

/// Shared cache invalidation memory segment (PG `SISeg`), now an `Arc` field on
/// `SharedState` instead of a shmem struct.
pub struct SInvalBuffer {
    /// SInvalWriteLock: serializes inserts, backend init/cleanup, and the
    /// array-wide part of cleanup.
    write: Mutex<SIWriteState>,
    /// SInvalReadLock: shared for readers (each mutates only its own ProcState),
    /// exclusive for the array-wide cleanup recompute.
    read_lock: RwLock<()>,
    /// Next message number to be assigned (PG `maxMsgNum`); the ex-`msgnumLock`
    /// barrier. Release on write, Acquire on read.
    max_msg_num: AtomicI32,
    /// Circular buffer holding shared-inval messages. Cells are written under
    /// `write` and published via `max_msg_num` Release; readers Acquire-load
    /// `max_msg_num` and only read slots `< max`, so they see fully-written cells.
    buffer: Box<[UnsafeCell<SharedInvalidationMessage>]>,
    /// Per-backend invalidation state, indexed by ProcNumber.
    proc_state: Box<[ProcState]>,
}

// SAFETY: SInvalBuffer is shared by `Arc` across the multi-thread runtime, so it
// must be Sync. The only non-atomic interior-mutable field is `buffer`
// (UnsafeCell). It is sound because:
//  - the boxed slice is allocated once and never moves;
//  - a writer fills cell `max % MAXNUMMESSAGES` UNDER the `write` Mutex, then
//    publishes the new `max_msg_num` with Release; readers Acquire-load
//    `max_msg_num` first and only read cells whose index is `< max`, so they
//    never observe a cell mid-write (the Release/Acquire pair orders the cell
//    store before the index store and the index load before the cell load);
//  - SICleanupQueue guarantees an unread cell is never overwritten: it forces a
//    laggard into reset (which makes that backend re-fetch `max` and discard its
//    state) before the buffer can wrap over a slot the backend still needs.
// Every other field is an atomic or a lock, which are Sync. The boxed cell of a
// plain `Copy` message has no `!Send`/`!Sync` interior, so Send follows too.
unsafe impl Sync for SInvalBuffer {}
unsafe impl Send for SInvalBuffer {}

tokio::task_local! {
    /// Next LocalTransactionId to hand out (PG file-static `nextLocalTransactionId`,
    /// per-process). Seeded in SharedInvalBackendInit, written back in
    /// CleanupInvalidationState. `Cell` is fine: a single task owns its counter and
    /// never holds a borrow across `.await`.
    static NEXT_LOCAL_TRANSACTION_ID: Cell<LocalTransactionId>;
}

/// Process-wide handle to the SI buffer (PG's `shmInvalBuffer`). Published by
/// `shared_inval_shmem_init`/`SharedState::new` so sinval.c's Send/Receive reach
/// it without a SharedState handle.
static SINVAL_BUFFER: OnceLock<Arc<SInvalBuffer>> = OnceLock::new();

/// NumProcStateSlots = MaxBackends + NUM_AUXILIARY_PROCS (PG sizing).
fn num_proc_state_slots() -> usize {
    crate::backend::storage::lmgr::proc::num_proc_state_slots()
}

/// PG `SharedInvalShmemSize`: bytes the SI segment would occupy. No segment is
/// allocated under the Arc model; this is an estimate.
pub fn SharedInvalShmemSize() -> usize {
    let slots = num_proc_state_slots();
    std::mem::size_of::<SIWriteState>()
        + (MAXNUMMESSAGES as usize) * std::mem::size_of::<SharedInvalidationMessage>()
        + slots * std::mem::size_of::<ProcState>()
        + slots * std::mem::size_of::<ProcNumber>()
}

impl SInvalBuffer {
    /// PG `SharedInvalShmemInit`: build the SI buffer with all slots inactive.
    fn new() -> Self {
        let slots = num_proc_state_slots();
        let buffer = (0..MAXNUMMESSAGES)
            .map(|_| {
                UnsafeCell::new(SharedInvalidationMessage::Relmap(
                    crate::storage::sinval::SharedInvalRelmapMsg {
                        db_id: crate::postgres_ext::Oid(0),
                    },
                ))
            })
            .collect();
        let proc_state = (0..slots).map(|_| ProcState::inactive()).collect();
        Self {
            write: Mutex::new(SIWriteState {
                min_msg_num: 0,
                next_threshold: CLEANUP_MIN,
                pgprocnos: Vec::with_capacity(slots),
            }),
            read_lock: RwLock::new(()),
            max_msg_num: AtomicI32::new(0),
            buffer,
            proc_state,
        }
    }

    /// PG `SharedInvalBackendInit`: register the current backend on the buffer.
    pub fn shared_inval_backend_init(&self, send_only: bool) {
        let me = current_proc_number();
        assert!(me >= 0, "MyProcNumber not set");
        assert!(
            (me as usize) < self.proc_state.len(),
            "unexpected MyProcNumber {me} in SharedInvalBackendInit (max {})",
            self.proc_state.len()
        );
        let state = &self.proc_state[me as usize];

        // Can run in parallel with readers, but not writers (SIInsertDataEntries
        // relies on pgprocnos to set hasMessages).
        let mut w = self.write.lock().unwrap();

        // PG: elog(ERROR, "sinval slot ... already in use"). TODO(panic).
        let old_pid = state.proc_pid.load(Ordering::Relaxed);
        assert!(old_pid == 0, "sinval slot for backend {me} is already in use by process {old_pid}");

        w.pgprocnos.push(me);

        // Fetch next local transaction ID into local memory.
        let seed = LocalTransactionId(state.next_lxid.load(Ordering::Relaxed));
        let _ = NEXT_LOCAL_TRANSACTION_ID.try_with(|c| c.set(seed));

        // Mark myself active, with all extant messages already read.
        state.proc_pid.store(my_proc_pid(), Ordering::Relaxed);
        state
            .next_msg_num
            .store(self.max_msg_num.load(Ordering::Acquire), Ordering::Relaxed);
        state.reset_state.store(false, Ordering::Relaxed);
        state.signaled.store(false, Ordering::Relaxed);
        state.has_messages.store(false, Ordering::Relaxed);
        state.send_only.store(send_only, Ordering::Relaxed);

        drop(w);
        // PG registers on_shmem_exit(CleanupInvalidationState); we call it
        // explicitly at backend teardown (RAII wrap is a TODO, like ProcKill).
    }

    /// PG `CleanupInvalidationState` (on_shmem_exit cb): mark the current backend
    /// inactive. Called explicitly at backend teardown.
    pub fn cleanup_invalidation_state(&self) {
        let me = current_proc_number();
        let mut w = self.write.lock().unwrap();
        let state = &self.proc_state[me as usize];

        // Update next local transaction ID for the next holder of this slot.
        let lxid = NEXT_LOCAL_TRANSACTION_ID
            .try_with(Cell::get)
            .unwrap_or(INVALID_LOCAL_TRANSACTION_ID);
        state.next_lxid.store(lxid.0, Ordering::Relaxed);

        // Mark myself inactive.
        state.proc_pid.store(0, Ordering::Relaxed);
        state.next_msg_num.store(0, Ordering::Relaxed);
        state.reset_state.store(false, Ordering::Relaxed);
        state.signaled.store(false, Ordering::Relaxed);

        let pos = w.pgprocnos.iter().position(|&p| p == me);
        // PG: elog(PANIC, "could not find entry in sinval array").
        let pos = pos.expect("could not find entry in sinval array");
        w.pgprocnos.swap_remove(pos);
    }

    /// PG `SIInsertDataEntries`: add new invalidation message(s) to the buffer.
    pub fn si_insert_data_entries(&self, data: &[SharedInvalidationMessage]) {
        // N can be arbitrarily large; divide into groups of <= WRITE_QUANTUM so
        // we don't hold the lock too long and so we can consider cleanup often.
        let mut rest = data;
        while !rest.is_empty() {
            let nthistime = rest.len().min(WRITE_QUANTUM as usize);
            let (batch, tail) = rest.split_at(nthistime);
            rest = tail;

            let mut w = self.write.lock().unwrap();

            // If the buffer is full we MUST acquire space; otherwise clean only
            // when past the next threshold. Loop and recheck after any cleanup.
            // Cleanup may pick a catchup target; PG drops both locks before the
            // (possibly slow) SendProcSignal. We collect targets here and deliver
            // them AFTER dropping `write` at the bottom of the batch.
            let mut catchup_targets: Vec<ProcNumber> = Vec::new();
            loop {
                let num_msgs = self.max_msg_num.load(Ordering::Relaxed) - w.min_msg_num;
                if num_msgs + nthistime as i32 > MAXNUMMESSAGES || num_msgs >= w.next_threshold {
                    if let Some(procno) = self.si_cleanup_queue_locked(&mut w, nthistime as i32) {
                        catchup_targets.push(procno);
                    }
                } else {
                    break;
                }
            }

            // Insert new message(s) into the circular buffer.
            let mut max = self.max_msg_num.load(Ordering::Relaxed);
            for msg in batch {
                let idx = (max % MAXNUMMESSAGES) as usize;
                // SAFETY: we hold `write`, so no other writer touches the buffer;
                // readers only read cells `< max_msg_num`, which we have not yet
                // advanced, so no reader observes this cell until the Release below.
                unsafe { *self.buffer[idx].get() = *msg; }
                max += 1;
            }

            // Publish maxMsgNum (Release) so the cell writes above are visible
            // before any reader's Acquire-load sees the new index.
            self.max_msg_num.store(max, Ordering::Release);

            // Kick everyone to read the newly added messages.
            for &procno in &w.pgprocnos {
                self.proc_state[procno as usize]
                    .has_messages
                    .store(true, Ordering::Relaxed);
            }

            drop(w);

            // `write` is dropped: now deliver any catchup signals (PG sends
            // SendProcSignal only after releasing both locks).
            for procno in catchup_targets {
                crate::backend::storage::ipc::sinval::send_catchup_signal(procno);
            }
        }
    }

    /// PG `SIGetDataEntries`: get next SI message(s) for the current backend.
    ///
    /// Returns 0 (none), n>0 (n messages copied into `data`), or -1 (reset).
    pub fn si_get_data_entries(&self, data: &mut [SharedInvalidationMessage]) -> i32 {
        let me = current_proc_number();
        let state = &self.proc_state[me as usize];

        // Quick unlocked test before taking locks (PG's hasMessages fast-path).
        if !state.has_messages.load(Ordering::Acquire) {
            return 0;
        }

        let _r = self.read_lock.read().unwrap();

        // Reset hasMessages BEFORE deciding how many to read, so a concurrent
        // insert re-sets it and we notice the remainder next time.
        state.has_messages.store(false, Ordering::Relaxed);

        let max = self.max_msg_num.load(Ordering::Acquire);

        if state.reset_state.load(Ordering::Relaxed) {
            // Force reset: we have dealt with everything up to max, so clear
            // signaled too.
            state.next_msg_num.store(max, Ordering::Relaxed);
            state.reset_state.store(false, Ordering::Relaxed);
            state.signaled.store(false, Ordering::Relaxed);
            return -1;
        }

        // Retrieve messages, advancing our counter, until data is full or none.
        let mut n = 0usize;
        let mut next = state.next_msg_num.load(Ordering::Relaxed);
        while n < data.len() && next < max {
            let idx = (next % MAXNUMMESSAGES) as usize;
            // SAFETY: `next < max`, and every cell `< max_msg_num` was fully
            // written under `write` before its Release; our Acquire-load of `max`
            // above orders that store before this read. We hold `read_lock` shared,
            // which excludes the cleanup writer that could overwrite this cell.
            data[n] = unsafe { *self.buffer[idx].get() };
            n += 1;
            next += 1;
        }
        state.next_msg_num.store(next, Ordering::Relaxed);

        // Caught up -> clear signaled; else keep hasMessages so we revisit.
        if next >= max {
            state.signaled.store(false, Ordering::Relaxed);
        } else {
            state.has_messages.store(true, Ordering::Relaxed);
        }

        n as i32
    }

    /// PG `SICleanupQueue` entry point taking both locks itself (caller does NOT
    /// hold `write`). `min_free` is the min number of free slots to make. Drops
    /// `write` before delivering any catchup signal (as PG does).
    pub fn si_cleanup_queue(&self, min_free: i32) {
        let target = {
            let mut w = self.write.lock().unwrap();
            self.si_cleanup_queue_locked(&mut w, min_free)
        };
        if let Some(procno) = target {
            crate::backend::storage::ipc::sinval::send_catchup_signal(procno);
        }
    }

    /// PG `SICleanupQueue` with `callerHasWriteLock == true`: caller already holds
    /// `write`. Takes `read_lock` exclusive for the array-wide recompute. Returns
    /// the backend that needs a catchup signal, if any, so the caller can deliver
    /// it AFTER dropping `write` (PG sends SendProcSignal with both locks
    /// released). The `signaled = true` bookkeeping stays UNDER the lock.
    fn si_cleanup_queue_locked(&self, w: &mut SIWriteState, min_free: i32) -> Option<ProcNumber> {
        // Lock out all readers (we already hold the writer lock).
        let r = self.read_lock.write().unwrap();

        // Recompute minMsgNum, identify the furthest-back backend needing a
        // signal, and reset backends that are too far back. sendOnly backends are
        // ignored, so they can keep sending even as the only active backend.
        let mut min = self.max_msg_num.load(Ordering::Relaxed);
        let mut minsig = min - SIG_THRESHOLD;
        let lowbound = min - MAXNUMMESSAGES + min_free;
        let mut need_sig: Option<ProcNumber> = None;

        for &procno in &w.pgprocnos {
            let state = &self.proc_state[procno as usize];
            let n = state.next_msg_num.load(Ordering::Relaxed);

            debug_assert!(state.proc_pid.load(Ordering::Relaxed) != 0);
            if state.reset_state.load(Ordering::Relaxed) || state.send_only.load(Ordering::Relaxed) {
                continue;
            }

            // If we must free space and this backend prevents it, force reset.
            if n < lowbound {
                state.reset_state.store(true, Ordering::Relaxed);
                continue; // no point in signaling him
            }

            if n < min {
                min = n;
            }

            // Furthest back of the unsignaled backends.
            if n < minsig && !state.signaled.load(Ordering::Relaxed) {
                minsig = n;
                need_sig = Some(procno);
            }
        }
        w.min_msg_num = min;

        // When minMsgNum gets large, decrement all counters to forestall overflow.
        if min >= MSGNUMWRAPAROUND {
            w.min_msg_num -= MSGNUMWRAPAROUND;
            self.max_msg_num.fetch_sub(MSGNUMWRAPAROUND, Ordering::Relaxed);
            for &procno in &w.pgprocnos {
                self.proc_state[procno as usize]
                    .next_msg_num
                    .fetch_sub(MSGNUMWRAPAROUND, Ordering::Relaxed);
            }
        }

        // Set the threshold at which we should repeat SICleanupQueue.
        let num_msgs = self.max_msg_num.load(Ordering::Relaxed) - w.min_msg_num;
        w.next_threshold = if num_msgs < CLEANUP_MIN {
            CLEANUP_MIN
        } else {
            (num_msgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM
        };

        // Mark the catchup target signaled (PG sets `signaled` under the lock),
        // then drop the read lock before any delivery (SendProcSignal is slow).
        if let Some(his_procno) = need_sig {
            self.proc_state[his_procno as usize]
                .signaled
                .store(true, Ordering::Relaxed);
        }
        drop(r);
        // PG also drops SInvalWriteLock here, sends, then re-acquires if the
        // caller held it. We cannot drop the `write` guard borrowed as `&mut w`,
        // so we return the target and let the caller (which owns the guard) send
        // it once `write` is released. The `signaled` bookkeeping above is done.
        need_sig
    }
}

#[cfg(test)]
impl SInvalBuffer {
    /// Build a fresh buffer for tests in other modules (`new` is private).
    pub fn new_for_test() -> Self {
        Self::new()
    }

    /// Test accessor: whether backend `procno`'s reset flag is set.
    pub fn proc_state_reset_for_test(&self, procno: ProcNumber) -> bool {
        self.proc_state[procno as usize]
            .reset_state
            .load(Ordering::Relaxed)
    }
}

/// PG `SharedInvalShmemInit`: build + publish the SI buffer; return the `Arc`.
/// `SharedState::new` calls this at the SharedInvalShmemInit marker.
pub fn shared_inval_shmem_init() -> Arc<SInvalBuffer> {
    let buf = Arc::new(SInvalBuffer::new());
    let _ = SINVAL_BUFFER.set(buf.clone());
    buf
}

tokio::task_local! {
    /// Per-task SI-buffer override (tests). When set, `current_sinval_buffer`
    /// returns it instead of the process-wide `SINVAL_BUFFER`, so each test tree
    /// gets a fresh buffer despite the single-set `OnceLock`. Never set in
    /// production (the OnceLock is the source of truth there).
    static SINVAL_BUFFER_OVERRIDE: Arc<SInvalBuffer>;
}

/// The current SI buffer: a task-local override if present (tests), else the
/// process-wide one. Returns an owned `Arc` (cheap clone; the wrappers call one
/// method and drop it -- not a hot per-row path).
pub fn current_sinval_buffer() -> Option<Arc<SInvalBuffer>> {
    SINVAL_BUFFER_OVERRIDE
        .try_with(Arc::clone)
        .ok()
        .or_else(|| SINVAL_BUFFER.get().cloned())
}

/// Run `f` with `buf` as the task-local SI buffer (tests). Mirrors how a backend
/// would reach the process-wide buffer, but scoped to one task tree.
#[cfg(test)]
pub async fn with_sinval_buffer<F, T>(buf: Arc<SInvalBuffer>, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SINVAL_BUFFER_OVERRIDE.scope(buf, f).await
}

/// PG `SharedInvalBackendInit`: register the current backend (process-wide buffer).
pub fn SharedInvalBackendInit(send_only: bool) {
    current_sinval_buffer()
        .expect("sinval buffer initialized")
        .shared_inval_backend_init(send_only);
}

/// PG `CleanupInvalidationState`: mark the current backend inactive.
pub fn cleanup_invalidation_state() {
    if let Some(buf) = current_sinval_buffer() {
        buf.cleanup_invalidation_state();
    }
}

/// PG `SIInsertDataEntries`: add invalidation message(s) (process-wide buffer).
pub fn SIInsertDataEntries(data: &[SharedInvalidationMessage]) {
    current_sinval_buffer()
        .expect("sinval buffer initialized")
        .si_insert_data_entries(data);
}

/// PG `SIGetDataEntries`: get next SI message(s) for the current backend.
pub fn SIGetDataEntries(data: &mut [SharedInvalidationMessage]) -> i32 {
    current_sinval_buffer()
        .expect("sinval buffer initialized")
        .si_get_data_entries(data)
}

/// PG `SICleanupQueue`: remove messages consumed by all active backends.
pub fn SICleanupQueue(caller_has_write_lock: bool, min_free: i32) {
    // The process-wide entry never holds `write` across the C call boundary, so
    // `caller_has_write_lock` is always false here; the in-module insert path
    // calls `si_cleanup_queue_locked` directly.
    debug_assert!(!caller_has_write_lock);
    if let Some(buf) = current_sinval_buffer() {
        buf.si_cleanup_queue(min_free);
    }
}

/// PG `GetNextLocalTransactionId`: allocate a new LocalTransactionId, skipping
/// Invalid(0) at wraparound.
pub fn GetNextLocalTransactionId() -> LocalTransactionId {
    NEXT_LOCAL_TRANSACTION_ID.with(|c| loop {
        let result = c.get();
        c.set(LocalTransactionId(result.0.wrapping_add(1)));
        if local_transaction_id_is_valid(result) {
            return result;
        }
    })
}

/// Run `f` with a fresh `nextLocalTransactionId` task-local (seeded to Invalid),
/// mirroring PG's per-process file static. Used by tests and the backend wrapper.
pub async fn next_lxid_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    NEXT_LOCAL_TRANSACTION_ID
        .scope(Cell::new(INVALID_LOCAL_TRANSACTION_ID), f)
        .await
}

/// This backend's `MyProcPid` via the per-task Session, or 0 outside a session.
fn my_proc_pid() -> i32 {
    crate::session::try_current().map_or(0, |s| s.proc_pid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_ext::Oid;
    use crate::session::{Session, scope};
    use crate::storage::proc::{my_proc_scope, set_current_proc_number};
    use crate::storage::sinval::SharedInvalRelcacheMsg;

    fn relcache_msg(rel: u32) -> SharedInvalidationMessage {
        SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg {
            db_id: Oid(1),
            rel_id: Oid(rel),
        })
    }

    /// Run `body` as a backend with the given ProcNumber: a fresh proc scope, a
    /// session (for MyProcPid), and the lxid task-local.
    async fn as_backend<F, Fut, T>(procno: ProcNumber, body: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        my_proc_scope(scope(
            Arc::new(Session::new(crate::miscadmin::BackendType::BACKEND)),
            next_lxid_scope(async move {
                set_current_proc_number(procno);
                body().await
            }),
        ))
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backend_init_claims_a_slot() {
        let buf = Arc::new(SInvalBuffer::new());
        let b = buf.clone();
        as_backend(0, || async move {
            b.shared_inval_backend_init(false);
            assert_eq!(b.proc_state[0].proc_pid.load(Ordering::Relaxed), my_proc_pid());
            assert_eq!(b.write.lock().unwrap().pgprocnos, vec![0]);
            b.cleanup_invalidation_state();
            assert_eq!(b.proc_state[0].proc_pid.load(Ordering::Relaxed), 0);
            assert!(b.write.lock().unwrap().pgprocnos.is_empty());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_get_round_trip() {
        let buf = Arc::new(SInvalBuffer::new());
        let b = buf.clone();
        as_backend(0, || async move {
            b.shared_inval_backend_init(false);
            b.si_insert_data_entries(&[relcache_msg(42), relcache_msg(43)]);
            let mut out = [relcache_msg(0); 8];
            let n = b.si_get_data_entries(&mut out);
            assert_eq!(n, 2);
            assert_eq!(out[0], relcache_msg(42));
            assert_eq!(out[1], relcache_msg(43));
            // Nothing left.
            assert_eq!(b.si_get_data_entries(&mut out), 0);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_backends_both_receive() {
        let buf = Arc::new(SInvalBuffer::new());
        let b0 = buf.clone();
        as_backend(0, || async move {
            b0.shared_inval_backend_init(false);
        })
        .await;
        let b1 = buf.clone();
        as_backend(1, || async move {
            b1.shared_inval_backend_init(false);
        })
        .await;

        // Insert from backend 0.
        let bi = buf.clone();
        as_backend(0, || async move {
            bi.si_insert_data_entries(&[relcache_msg(7)]);
        })
        .await;

        // Both backends read it.
        for procno in [0, 1] {
            let br = buf.clone();
            as_backend(procno, || async move {
                let mut out = [relcache_msg(0); 4];
                let n = br.si_get_data_entries(&mut out);
                assert_eq!(n, 1, "backend {procno} receives");
                assert_eq!(out[0], relcache_msg(7));
            })
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn laggard_overflow_forces_reset() {
        let buf = Arc::new(SInvalBuffer::new());
        // Backend 0 is the active writer/reader; backend 1 is the laggard.
        for procno in [0, 1] {
            let b = buf.clone();
            as_backend(procno, || async move {
                b.shared_inval_backend_init(false);
            })
            .await;
        }

        // Backend 0 floods the buffer well past MAXNUMMESSAGES, reading as it goes
        // so it never becomes a laggard. Backend 1 never reads -> it gets reset.
        let b = buf.clone();
        as_backend(0, || async move {
            let batch: Vec<_> = (0..256).map(relcache_msg).collect();
            let mut out = [relcache_msg(0); 256];
            for _ in 0..40 {
                b.si_insert_data_entries(&batch);
                // Drain so backend 0 stays caught up.
                while b.si_get_data_entries(&mut out) > 0 {}
            }
        })
        .await;

        // Backend 1 now reads: it fell too far behind, so it must get a reset.
        let b = buf.clone();
        as_backend(1, || async move {
            assert!(b.proc_state[1].reset_state.load(Ordering::Relaxed), "laggard marked reset");
            let mut out = [relcache_msg(0); 4];
            assert_eq!(b.si_get_data_entries(&mut out), -1, "laggard gets reset (-1)");
            // After consuming the reset it is caught up.
            assert_eq!(b.si_get_data_entries(&mut out), 0);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn next_lxid_skips_invalid_and_is_monotonic() {
        as_backend(0, || async {
            // Seed at u32::MAX so the next increment wraps to 0 (Invalid) and is
            // skipped.
            NEXT_LOCAL_TRANSACTION_ID.with(|c| c.set(LocalTransactionId(u32::MAX)));
            let a = GetNextLocalTransactionId();
            assert_eq!(a, LocalTransactionId(u32::MAX));
            // Counter is now 0 -> Invalid; the next call must skip it to 1.
            let b = GetNextLocalTransactionId();
            assert_eq!(b, LocalTransactionId(1));
            let c = GetNextLocalTransactionId();
            assert_eq!(c, LocalTransactionId(2));
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_min_is_the_slower_backend() {
        // Two active, non-laggard backends at different next_msg_num. SICleanupQueue
        // must set min_msg_num to the SLOWER backend's cursor (the min over
        // backends), not the faster one's / max.
        let buf = Arc::new(SInvalBuffer::new());
        for procno in [0, 1] {
            let b = buf.clone();
            as_backend(procno, || async move {
                b.shared_inval_backend_init(false);
            })
            .await;
        }

        // Backend 0 inserts 10 messages.
        let bi = buf.clone();
        as_backend(0, || async move {
            bi.si_insert_data_entries(&(0..10).map(relcache_msg).collect::<Vec<_>>());
        })
        .await;

        // Backend 0 reads all 10 (cursor -> 10); backend 1 reads only 4 (cursor -> 4).
        let b0 = buf.clone();
        as_backend(0, || async move {
            let mut out = [relcache_msg(0); 16];
            assert_eq!(b0.si_get_data_entries(&mut out), 10);
        })
        .await;
        let b1 = buf.clone();
        as_backend(1, || async move {
            let mut out = [relcache_msg(0); 4];
            assert_eq!(b1.si_get_data_entries(&mut out), 4);
        })
        .await;

        // Cleanup: min must be 4 (the slower backend), not 10 (the faster/max).
        assert_eq!(buf.proc_state[0].next_msg_num.load(Ordering::Relaxed), 10);
        assert_eq!(buf.proc_state[1].next_msg_num.load(Ordering::Relaxed), 4);
        buf.si_cleanup_queue(0);
        assert_eq!(buf.max_msg_num.load(Ordering::Relaxed), 10);
        assert_eq!(
            buf.write.lock().unwrap().min_msg_num,
            4,
            "min advances to the slower backend's cursor, not max"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_advances_min_msg_num() {
        let buf = Arc::new(SInvalBuffer::new());
        let b = buf.clone();
        as_backend(0, || async move {
            b.shared_inval_backend_init(false);
            // Insert a handful, read them all, then cleanup: min advances to max.
            b.si_insert_data_entries(&(0..10).map(relcache_msg).collect::<Vec<_>>());
            let mut out = [relcache_msg(0); 16];
            assert_eq!(b.si_get_data_entries(&mut out), 10);
            assert_eq!(b.write.lock().unwrap().min_msg_num, 0);
            b.si_cleanup_queue(0);
            let max = b.max_msg_num.load(Ordering::Relaxed);
            assert_eq!(b.write.lock().unwrap().min_msg_num, max, "min advanced to max");
            assert_eq!(max, 10);
        })
        .await;
    }
}

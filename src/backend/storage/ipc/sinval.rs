//! Shared cache invalidation communication code. Translated from backend/storage/ipc/sinval.c.
//!
//! This is the thin send/receive layer over the shared invalidation (SI) message
//! ring maintained in `sinvaladt`. `SendSharedInvalidMessages` appends messages to
//! the global queue; `ReceiveSharedInvalidMessages` processes every message that was
//! queued before it was entered, invoking a caller-supplied callback per message and
//! a separate reset callback when a backend has fallen so far behind that the queue
//! forced it through a full cache reset. The receive routine may recurse, because a
//! callback can itself trigger invalidation processing, so it drains messages already
//! pulled out of the ring before fetching more.
//!
//! It also carries the catchup-interrupt machinery. Idle backends do not read the SI
//! queue on their own, so a backend that gets too far behind is sent a catchup
//! interrupt; when it next becomes able to process one, it accepts the pending
//! invalidations and daisy-chains the signal on to the next slowest backend so the
//! whole queue keeps draining.
//!
//! PepperDB differs from PostgreSQL in how the shared, per-process state is realized.
//! `SharedInvalidMessageCounter`, a plain process global in C, is a module `AtomicU64`.
//! The C `catchupInterruptPending` flag (set from a signal handler) becomes a per-task
//! `CatchupInterrupt` bit in the task's ProcSignal slot, raised by another task through
//! the process-wide ProcSignal registry rather than by a Unix signal; a task with no
//! slot is simply treated as not pending. The recursion-safe static message buffer and
//! its counters, which C keeps as file-static state, are held in a tokio task-local
//! cell so each backend task has its own copy and the in-task recursion behaves as in
//! the original. `ProcessCatchupInterrupt` is async here because starting and
//! committing the surrounding transaction are async operations.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::storage::ipc::procsignal::current_proc_signal;
use crate::backend::storage::ipc::sinvaladt::{
    SICleanupQueue, current_sinval_buffer,
};
use crate::shared_state::SharedState;
use crate::storage::procnumber::ProcNumber;
use crate::storage::procsignal::ProcSignalReason;
use crate::storage::sinval::SharedInvalidationMessage;

/// PG `MAXINVALMSGS`: how many messages ReceiveSharedInvalidMessages fetches per
/// SIGetDataEntries call (and the recursion-safe buffer size).
const MAXINVALMSGS: usize = 32;

/// PG `SharedInvalidMessageCounter` (uint64 process global, "don't worry about
/// overflow"). Bumped once per processed/reset message.
static SHARED_INVALID_MESSAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Read the running count of SI messages processed (PG's global, for tests/stats).
pub fn shared_invalid_message_counter() -> u64 {
    SHARED_INVALID_MESSAGE_COUNTER.load(Ordering::Relaxed)
}

/// PG `SendSharedInvalidMessages`: add shared-cache-invalidation message(s) to the
/// global SI message queue.
pub fn SendSharedInvalidMessages(msgs: &[SharedInvalidationMessage]) {
    current_sinval_buffer()
        .expect("sinval buffer initialized")
        .si_insert_data_entries(msgs);
}

/// PG's recursion-safe static buffer for ReceiveSharedInvalidMessages, modeled as
/// per-task state. `nextmsg`/`nummsgs` index into `messages`; a recursive call
/// (triggered inside the inval callback) drains the messages already fetched here.
struct ReceiveState {
    messages: [SharedInvalidationMessage; MAXINVALMSGS],
    nextmsg: usize,
    nummsgs: usize,
}

impl ReceiveState {
    fn new() -> Self {
        // A dummy fill value; cells are never read before being written by
        // SIGetDataEntries (nummsgs gates reads). Relmap is the cheapest variant.
        let dummy = SharedInvalidationMessage::Relmap(crate::storage::sinval::SharedInvalRelmapMsg {
            db_id: crate::postgres_ext::Oid(0),
        });
        Self {
            messages: [dummy; MAXINVALMSGS],
            nextmsg: 0,
            nummsgs: 0,
        }
    }
}

tokio::task_local! {
    /// PG's file-static `messages[]`/`nextmsg`/`nummsgs`, per backend task.
    static RECEIVE_STATE: RefCell<ReceiveState>;
}

/// Pop the next pending message from the per-task receive buffer, advancing
/// `nextmsg`. Returns None when caught up. The RefCell borrow is confined to this
/// call so it is never held across the inval/reset callback.
fn pop_pending() -> Option<SharedInvalidationMessage> {
    RECEIVE_STATE
        .try_with(|cell| {
            let mut st = cell.borrow_mut();
            if st.nextmsg < st.nummsgs {
                let msg = st.messages[st.nextmsg];
                st.nextmsg += 1;
                Some(msg)
            } else {
                None
            }
        })
        .ok()
        .flatten()
}

/// Fetch up to MAXINVALMSGS into the per-task buffer via SIGetDataEntries; set
/// `nextmsg = 0`, `nummsgs = result` on success. Returns the SIGetDataEntries
/// result (<0 = reset, else the count). The borrow is confined to this call.
fn fetch_into_buffer() -> i32 {
    RECEIVE_STATE
        .try_with(|cell| {
            let mut st = cell.borrow_mut();
            st.nextmsg = 0;
            st.nummsgs = 0;
            let result = current_sinval_buffer()
                .expect("sinval buffer initialized")
                .si_get_data_entries(&mut st.messages);
            if result >= 0 {
                st.nextmsg = 0;
                st.nummsgs = result as usize;
            }
            result
        })
        .unwrap_or(0)
}

/// PG `ReceiveSharedInvalidMessages`: process all SI messages queued for this
/// backend, calling `inval_fn` per message and `reset_fn` on a queue-reset signal.
///
/// Faithfully reproduces the recursion-safe algorithm: drain any messages still
/// pending from an outer recursion, then loop SIGetDataEntries (a negative result
/// means reset -> call `reset_fn` and stop; otherwise process the batch), looping
/// while the last fetch returned a full buffer. After catching up, if the catchup
/// bit is pending, clear it and call SICleanupQueue(false, 0) to daisy-chain.
pub fn ReceiveSharedInvalidMessages(
    mut inval_fn: impl FnMut(&SharedInvalidationMessage),
    mut reset_fn: impl FnMut(),
) {
    let mut body = || {
        // Deal with any messages still pending from an outer recursion.
        while let Some(msg) = pop_pending() {
            SHARED_INVALID_MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
            inval_fn(&msg);
        }

        loop {
            // Try to get some more messages.
            let get_result = fetch_into_buffer();

            if get_result < 0 {
                // Got a reset message.
                SHARED_INVALID_MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
                reset_fn();
                break; // nothing more to do
            }

            // Process them, being wary that a recursive call might eat some.
            while let Some(msg) = pop_pending() {
                SHARED_INVALID_MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
                inval_fn(&msg);
            }

            // Only loop if the last fetch (possibly inside a recursive call)
            // returned a full buffer.
            let full = RECEIVE_STATE
                .try_with(|cell| cell.borrow().nummsgs == MAXINVALMSGS)
                .unwrap_or(false);
            if !full {
                break;
            }
        }

        // Caught up. If we received a catchup signal, reset that flag and call
        // SICleanupQueue() -- mostly to daisy-chain the catchup to the next
        // slowest backend rather than because we must flush dead messages now.
        if take_catchup_pending() {
            SICleanupQueue(false, 0);
        }
    };

    // Run with the per-task receive buffer in scope. If a buffer is already in
    // scope (a recursive call), reuse it so the recursion-drain semantics hold.
    if RECEIVE_STATE.try_with(|_| ()).is_ok() {
        body();
    } else {
        // No outer scope: establish one for this (sync) call. `sync_scope` runs
        // the closure with the task-local set and no `.await` involved.
        RECEIVE_STATE.sync_scope(RefCell::new(ReceiveState::new()), body);
    }
}

// ---------------------------------------------------------------------------
// catchupInterruptPending -> per-task ProcSignal slot CatchupInterrupt bit
// ---------------------------------------------------------------------------

/// Test-and-clear the current task's CatchupInterrupt bit (PG: read + clear
/// `catchupInterruptPending`). No slot -> not pending.
fn take_catchup_pending() -> bool {
    crate::backend::storage::ipc::procsignal::try_current()
        .is_some_and(|slot| slot.take_reason(ProcSignalReason::CatchupInterrupt))
}

/// Whether the current task's CatchupInterrupt bit is set, without clearing it.
fn catchup_pending() -> bool {
    crate::backend::storage::ipc::procsignal::try_current()
        .is_some_and(|slot| slot.reason_is_set(ProcSignalReason::CatchupInterrupt))
}

/// PG `HandleCatchupInterrupt` (a signal handler that set the flag +
/// SetLatch(MyLatch)): set the CURRENT task's CatchupInterrupt bit and ring its
/// own latch. Under the async model `SendProcSignal`/`send_by_proc_number` already
/// does exactly this, so calling this on oneself is normally redundant; the symbol
/// is kept faithful to the C handler.
// TODO: only exercised once the client-read-interrupt main loop lands.
pub fn HandleCatchupInterrupt() {
    if let Some(slot) = crate::backend::storage::ipc::procsignal::try_current() {
        slot.raise_reason_self(ProcSignalReason::CatchupInterrupt);
        slot.latch.set();
    }
}

/// Send PROCSIG_CATCHUP_INTERRUPT to the backend `procno` (PG's SICleanupQueue ->
/// SendProcSignal(his_procno, PROCSIG_CATCHUP_INTERRUPT)). Reaches the
/// process-wide ProcSignal registry and targets the slot BY proc_number. A no-op
/// (returns false) if there is no registry or no live slot for `procno`.
pub fn send_catchup_signal(procno: ProcNumber) -> bool {
    current_proc_signal()
        .is_some_and(|ps| ps.send_by_proc_number(procno, ProcSignalReason::CatchupInterrupt))
}

/// PG `ProcessCatchupInterrupt`: the portion that runs outside the signal handler,
/// processing pending invalidations. Loops while the catchup bit is pending: if we
/// are in a transaction, call AcceptInvalidationMessages(); otherwise start and
/// immediately end a transaction (AcceptInvalidationMessages runs inside xact
/// start). Async because the xact start/commit commands are async here.
pub async fn ProcessCatchupInterrupt(shared: &Arc<SharedState>) {
    while catchup_pending() {
        if crate::backend::access::transam::xact::IsTransactionOrTransactionBlock() {
            crate::utils::inval::AcceptInvalidationMessages();
        } else {
            crate::backend::access::transam::xact::StartTransactionCommand(shared).await;
            crate::backend::access::transam::xact::CommitTransactionCommand(shared).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::ipc::procsignal::{ProcSignal, scope as ps_scope};
    use crate::backend::storage::ipc::sinvaladt::{
        SInvalBuffer, next_lxid_scope, with_sinval_buffer,
    };
    use crate::postgres_ext::Oid;
    use crate::session::{Session, scope as sess_scope};
    use crate::storage::latch::Latch;
    use crate::storage::proc::{my_proc_scope, set_current_proc_number};
    use crate::storage::procnumber::ProcNumber;
    use crate::storage::sinval::SharedInvalRelcacheMsg;

    fn relcache_msg(rel: u32) -> SharedInvalidationMessage {
        SharedInvalidationMessage::Relcache(SharedInvalRelcacheMsg {
            db_id: Oid(1),
            rel_id: Oid(rel),
        })
    }

    /// Run `body` as a backend: a fresh proc scope, a session (MyProcPid), the
    /// lxid task-local, a registered ProcSignal slot (so catchupInterruptPending
    /// has a home), and `buf` published as the task-local SI buffer so the
    /// `current_sinval_buffer()`-based wrappers reach it.
    async fn as_backend<F, Fut, T>(
        buf: Arc<SInvalBuffer>,
        reg: &Arc<ProcSignal>,
        procno: ProcNumber,
        body: F,
    ) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let latch = Arc::new(Latch::new());
        let (_key, slot) = reg.register(1000 + procno, b"k", latch);
        my_proc_scope(sess_scope(
            Arc::new(Session::new(crate::miscadmin::BackendType::BACKEND)),
            ps_scope(
                slot,
                next_lxid_scope(with_sinval_buffer(buf, async move {
                    set_current_proc_number(procno);
                    body().await
                })),
            ),
        ))
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_receive_round_trip() {
        let buf = Arc::new(SInvalBuffer::new_for_test());
        let reg = Arc::new(ProcSignal::new());

        as_backend(buf.clone(), &reg, 0, || async move {
            shared_inval_backend_init_for(&buf, false);
            SendSharedInvalidMessages(&[relcache_msg(11), relcache_msg(12)]);

            let mut got = Vec::new();
            ReceiveSharedInvalidMessages(|m| got.push(*m), || panic!("unexpected reset"));
            assert_eq!(got, vec![relcache_msg(11), relcache_msg(12)]);

            // Second receive: nothing left, no callbacks.
            let mut again = 0;
            ReceiveSharedInvalidMessages(|_| again += 1, || panic!("unexpected reset"));
            assert_eq!(again, 0);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reset_path_calls_reset_fn() {
        let buf = Arc::new(SInvalBuffer::new_for_test());
        let reg = Arc::new(ProcSignal::new());

        // Backend 0 active reader/writer; backend 1 the laggard.
        for procno in [0, 1] {
            let b = buf.clone();
            as_backend(buf.clone(), &reg, procno, || async move {
                b.shared_inval_backend_init(false);
            })
            .await;
        }

        // Backend 0 floods past the buffer, draining as it goes (stays caught up).
        as_backend(buf.clone(), &reg, 0, || async move {
            let batch: Vec<_> = (0..256).map(relcache_msg).collect();
            for _ in 0..40 {
                SendSharedInvalidMessages(&batch);
                ReceiveSharedInvalidMessages(|_| {}, || {});
            }
        })
        .await;

        // Backend 1 fell too far behind -> Receive must call reset_fn exactly once.
        let b = buf.clone();
        as_backend(buf.clone(), &reg, 1, || async move {
            assert!(
                b.proc_state_reset_for_test(1),
                "laggard should be marked reset"
            );
            let mut reset_calls = 0;
            let mut inval_calls = 0;
            ReceiveSharedInvalidMessages(|_| inval_calls += 1, || reset_calls += 1);
            assert_eq!(reset_calls, 1, "reset_fn called once");
            assert_eq!(inval_calls, 0, "no inval on a reset");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recursion_safe_drain() {
        // An inval_fn that itself calls ReceiveSharedInvalidMessages must drain
        // the messages already fetched into the per-task buffer, not re-enter the
        // ring. Verify every message is delivered exactly once across the outer +
        // recursive calls.
        let buf = Arc::new(SInvalBuffer::new_for_test());
        let reg = Arc::new(ProcSignal::new());

        as_backend(buf.clone(), &reg, 0, || async move {
            shared_inval_backend_init_for(&buf, false);
            SendSharedInvalidMessages(&(0..5).map(relcache_msg).collect::<Vec<_>>());

            let seen = std::cell::RefCell::new(Vec::new());
            let mut recursed = false;
            ReceiveSharedInvalidMessages(
                |m| {
                    seen.borrow_mut().push(*m);
                    if !recursed {
                        recursed = true;
                        // Recursive call: drains the rest of the buffer.
                        ReceiveSharedInvalidMessages(|m2| seen.borrow_mut().push(*m2), || {});
                    }
                },
                || panic!("unexpected reset"),
            );

            let mut got = seen.into_inner();
            got.sort_by_key(|m| match m {
                SharedInvalidationMessage::Relcache(r) => r.rel_id.0,
                _ => u32::MAX,
            });
            let expect: Vec<_> = (0..5).map(relcache_msg).collect();
            assert_eq!(got, expect, "every message delivered exactly once");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catchup_signal_sets_target_bit() {
        // The catchup send sets the target slot's CatchupInterrupt bit. Drive the
        // registry directly (send_catchup_signal just forwards to it via the
        // process-wide accessor), keeping the test self-contained.
        let reg = ProcSignal::new();
        let latch = Arc::new(Latch::new());
        let (_key, target_slot) = reg.register(42, b"k", latch);
        let target_procno = target_slot.proc_number;

        assert!(reg.send_by_proc_number(target_procno, ProcSignalReason::CatchupInterrupt));
        assert!(
            target_slot.reason_is_set(ProcSignalReason::CatchupInterrupt),
            "target's CatchupInterrupt bit must be set"
        );
    }

    /// Test helper: register the current backend on a SPECIFIC buffer (the methods
    /// take `&self`; the wrappers use `current_sinval_buffer()`, which is the same
    /// `buf` here via the task-local override).
    fn shared_inval_backend_init_for(buf: &Arc<SInvalBuffer>, send_only: bool) {
        buf.shared_inval_backend_init(send_only);
    }
}

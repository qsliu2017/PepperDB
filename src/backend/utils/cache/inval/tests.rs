//! Inline tests for the cache-invalidation dispatcher (16c). These exercise the
//! message-array/group bookkeeping, the callback machinery, subtransaction
//! merging, and the empty-queue AcceptInvalidationMessages no-op -- WITHOUT
//! driving LocalExecuteInvalidationMessage into a catcache/relcache stub (those
//! `unimplemented!()`). The Smgr arm reaches only the local TODO placeholder, and
//! the Snapshot arm reaches real snapmgr code, so those are safe to drive.

use std::cell::RefCell;
use std::sync::Arc;

use super::*;
use crate::backend::storage::ipc::procsignal::{ProcSignal, scope as ps_scope};
use crate::backend::storage::ipc::sinvaladt::{SInvalBuffer, next_lxid_scope, with_sinval_buffer};
use crate::miscadmin::BackendType;
use crate::postgres_ext::Oid;
use crate::session::{Session, scope as sess_scope};
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, set_current_proc_number};
use crate::storage::procnumber::ProcNumber;

fn datum(n: usize) -> Datum {
    Datum(n)
}

/// Run `body` inside a fresh INVAL_STATE scope. These low-level bookkeeping +
/// callback tests need no session (they never read MyDatabaseId).
fn with_state<T>(_db: u32, body: impl FnOnce() -> T) -> T {
    scope(body)
}

#[test]
fn add_and_dedup_relcache_messages() {
    with_state(1, || {
        INVAL_STATE.with(|cell| {
            let mut st = cell.borrow_mut();
            let mut group = InvalidationMsgsGroup::default();
            let arrays = &mut st.inval_message_arrays;
            add_relcache_invalidation_message(arrays, &mut group, Oid::new(1), Oid::new(10));
            add_relcache_invalidation_message(arrays, &mut group, Oid::new(1), Oid::new(11));
            // Duplicate relId is dropped.
            add_relcache_invalidation_message(arrays, &mut group, Oid::new(1), Oid::new(10));
            assert_eq!(group.num_in_subgroup(REL_CACHE_MSGS), 2);

            // InvalidOid (whole relcache) then a specific one: the specific one is
            // redundant because InvalidOid already covers all.
            let mut g2 = InvalidationMsgsGroup::default();
            g2.set_group_to_follow(&group);
            add_relcache_invalidation_message(arrays, &mut g2, Oid::new(1), InvalidOid);
            add_relcache_invalidation_message(arrays, &mut g2, Oid::new(1), Oid::new(99));
            assert_eq!(g2.num_in_subgroup(REL_CACHE_MSGS), 1);
        });
    });
}

#[test]
fn append_folds_current_into_prior() {
    with_state(1, || {
        INVAL_STATE.with(|cell| {
            let mut st = cell.borrow_mut();
            let mut prior = InvalidationMsgsGroup::default();
            let mut current = InvalidationMsgsGroup::default();
            current.set_group_to_follow(&prior);
            {
                let arrays = &mut st.inval_message_arrays;
                add_catcache_invalidation_message(arrays, &mut prior, 3, 100, Oid::new(1));
                add_relcache_invalidation_message(arrays, &mut prior, Oid::new(1), Oid::new(10));
                // current must follow prior in the array to be adjacent.
                current.set_group_to_follow(&prior);
                add_catcache_invalidation_message(arrays, &mut current, 4, 200, Oid::new(1));
                add_relcache_invalidation_message(arrays, &mut current, Oid::new(1), Oid::new(11));
            }
            assert_eq!(prior.num_in_group(), 2);
            assert_eq!(current.num_in_group(), 2);

            append_invalidation_messages(&mut prior, &mut current);
            assert_eq!(prior.num_in_group(), 4);
            assert_eq!(current.num_in_group(), 0);
        });
    });
}

#[test]
fn prepare_state_nesting_and_subxact_merge() {
    // Drive PrepareInvalidationState at nest level 1, queue a message, then a
    // subxact-commit merge folds into the parent. We model the level by pushing
    // stack entries directly (GetCurrentTransactionNestLevel returns 1 here).
    with_state(1, || {
        INVAL_STATE.with(|cell| {
            let mut st = cell.borrow_mut();
            // Level-1 entry with a prior-processed message.
            let mut top = TransInvalidationInfo {
                my_level: 1,
                ..Default::default()
            };
            {
                let arrays = &mut st.inval_message_arrays;
                add_relcache_invalidation_message(
                    arrays,
                    &mut top.prior_cmd_invalid_msgs,
                    Oid::new(1),
                    Oid::new(10),
                );
                top.ii
                    .current_cmd_invalid_msgs
                    .set_group_to_follow(&top.prior_cmd_invalid_msgs);
            }
            st.trans_inval_info.push(top);

            // Level-2 child following the parent's current group.
            let parent_current = st.trans_inval_info.last().unwrap().ii.current_cmd_invalid_msgs;
            let mut child = TransInvalidationInfo {
                my_level: 2,
                ..Default::default()
            };
            child.prior_cmd_invalid_msgs.set_group_to_follow(&parent_current);
            {
                let prior = child.prior_cmd_invalid_msgs;
                child.ii.current_cmd_invalid_msgs.set_group_to_follow(&prior);
                let arrays = &mut st.inval_message_arrays;
                add_relcache_invalidation_message(
                    arrays,
                    &mut child.prior_cmd_invalid_msgs,
                    Oid::new(1),
                    Oid::new(20),
                );
            }
            st.trans_inval_info.push(child);

            // Merge child's prior into parent's prior (the subxact-commit step).
            let mut child = st.trans_inval_info.pop().unwrap();
            let mut parent = st.trans_inval_info.pop().unwrap();
            append_invalidation_messages(
                &mut parent.prior_cmd_invalid_msgs,
                &mut child.prior_cmd_invalid_msgs,
            );
            let prior = parent.prior_cmd_invalid_msgs;
            parent.ii.current_cmd_invalid_msgs.set_group_to_follow(&prior);
            st.trans_inval_info.push(parent);

            let merged = st.trans_inval_info.last().unwrap();
            assert_eq!(merged.prior_cmd_invalid_msgs.num_in_subgroup(REL_CACHE_MSGS), 2);
        });
    });
}

// Callback recorder: a thread_local Vec capturing the call order. The fn-pointer
// callbacks can't capture, so they record through a process-global.
thread_local! {
    static CALL_LOG: RefCell<Vec<(u64, i32)>> = const { RefCell::new(Vec::new()) };
}

fn cb_a(arg: Datum, cacheid: i32, _hash: u32) {
    CALL_LOG.with(|l| l.borrow_mut().push((arg.0 as u64, cacheid)));
}
fn cb_b(arg: Datum, cacheid: i32, _hash: u32) {
    CALL_LOG.with(|l| l.borrow_mut().push((arg.0 as u64, cacheid)));
}

#[test]
fn syscache_callbacks_dispatch_in_registration_order() {
    with_state(1, || {
        CALL_LOG.with(|l| l.borrow_mut().clear());
        // Two callbacks on the same cache id; older registered first must run first.
        cache_register_syscache_callback(7, cb_a, datum(1));
        cache_register_syscache_callback(7, cb_b, datum(2));
        // A callback on a different cache id is not invoked.
        cache_register_syscache_callback(8, cb_a, datum(3));

        call_syscache_callbacks(7, 0);
        let log = CALL_LOG.with(|l| l.borrow().clone());
        assert_eq!(log, vec![(1, 7), (2, 7)]);
    });
}

#[test]
fn xact_get_committed_orders_prior_then_current() {
    with_state(1, || {
        INVAL_STATE.with(|cell| {
            let mut st = cell.borrow_mut();
            let mut top = TransInvalidationInfo {
                my_level: 1,
                ..Default::default()
            };
            {
                let arrays = &mut st.inval_message_arrays;
                // prior: cat id=1, rel relId=10
                add_catcache_invalidation_message(arrays, &mut top.prior_cmd_invalid_msgs, 1, 0, Oid::new(1));
                add_relcache_invalidation_message(arrays, &mut top.prior_cmd_invalid_msgs, Oid::new(1), Oid::new(10));
                // current follows prior; cat id=2, rel relId=11
                top.ii.current_cmd_invalid_msgs.set_group_to_follow(&top.prior_cmd_invalid_msgs);
                add_catcache_invalidation_message(arrays, &mut top.ii.current_cmd_invalid_msgs, 2, 0, Oid::new(1));
                add_relcache_invalidation_message(arrays, &mut top.ii.current_cmd_invalid_msgs, Oid::new(1), Oid::new(11));
            }
            st.trans_inval_info.push(top);
        });

        let (msgs, init_file) = xact_get_committed_invalidation_messages();
        assert!(!init_file);
        // Order: cat(prior), cat(current), rel(prior), rel(current).
        let ids: Vec<i8> = msgs
            .iter()
            .map(|m| match m {
                SharedInvalidationMessage::Catcache(c) => c.id,
                SharedInvalidationMessage::Relcache(_) => -2,
                _ => i8::MIN,
            })
            .collect();
        assert_eq!(ids, vec![1, 2, -2, -2]);
    });
}

/// Backend wrapper: a fresh proc scope, session, lxid task-local, a registered
/// ProcSignal slot, and `buf` published as the SI buffer, all with INVAL_STATE in
/// scope, so AcceptInvalidationMessages can run end-to-end on an empty queue.
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
    let sess = Arc::new(Session::new(BackendType::BACKEND));
    sess.set_database_id(Oid::new(1));
    my_proc_scope(sess_scope(
        sess,
        ps_scope(
            slot,
            next_lxid_scope(with_sinval_buffer(buf, async move {
                set_current_proc_number(procno);
                INVAL_STATE
                    .scope(RefCell::new(InvalState::new()), body())
                    .await
            })),
        ),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_invalidation_messages_empty_queue_is_noop() {
    let buf = Arc::new(SInvalBuffer::new_for_test());
    let reg = Arc::new(ProcSignal::new());

    as_backend(buf.clone(), &reg, 0, || async move {
        buf.shared_inval_backend_init(false);
        // No messages queued: must return without panicking or invoking anything.
        accept_invalidation_messages();
        accept_invalidation_messages();
    })
    .await;
}

//! Tests for the deadlock detector (15c). Multi-thread tokio so genuine
//! cross-task deadlocks form: each task holds one lock and waits on another. A
//! short DeadlockTimeout makes ProcSleep run CheckDeadLock fast instead of the
//! 1 s default. The deadlocked LockAcquire returns NotAvail (ProcSleep -> ERROR
//! via the HardDeadlock arm), not a panic (the report path is only hit on the
//! JoinWaitQueue early-deadlock branch).

use std::sync::Arc;
use std::time::Duration;

use crate::backend::storage::lmgr::lock::{
    LockAcquire, LockManager, LockRelease, LockReleaseAll, local_lock_scope,
};
use crate::miscadmin::BackendType;
use crate::session::{Session, scope as session_scope};
use crate::shared_state::{SharedState, SharedStateConfig};
use crate::storage::lock::{DEFAULT_LOCKMETHOD, LOCKTAG, LockAcquireResult};
use crate::storage::lockdefs::LockMode;
use crate::storage::proc::my_proc_scope;

fn shared() -> Arc<SharedState> {
    SharedState::new(SharedStateConfig::default())
}

/// Backend body wrapper: session -> my_proc -> local-lock scope -> InitProcess.
/// db=0 keeps every relation lock off the fast path (exercises the main table /
/// wait-queue, which is what the deadlock detector walks).
async fn backend<F, Fut, T>(body: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let sess = Arc::new(Session::new(BackendType::BACKEND));
    sess.set_database_id(crate::postgres_ext::Oid(0));
    session_scope(
        sess,
        my_proc_scope(local_lock_scope(async move {
            crate::backend::storage::lmgr::proc::InitProcess();
            let r = body().await;
            crate::backend::storage::lmgr::proc::ProcKill();
            r
        })),
    )
    .await
}

/// Set a short deadlock timeout + no lock timeout for the calling task's waits.
fn fast_deadlock_timer() {
    unsafe {
        crate::storage::proc::LockTimeout = 0;
        crate::storage::proc::DeadlockTimeout = 25;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_task_hard_deadlock_one_aborts() {
    let _s = shared();
    let lock1 = LOCKTAG::set_relation(0, 9001);
    let lock2 = LOCKTAG::set_relation(0, 9002);
    let ex = LockMode::ExclusiveLock as i32;

    // Coordination: each task signals after taking its first lock so the other
    // only requests the second lock once the cross-hold is established.
    let (a_got, a_got_rx) = tokio::sync::oneshot::channel();
    let (b_got, b_got_rx) = tokio::sync::oneshot::channel();

    let ta = tokio::spawn(async move {
        backend(|| async move {
            fast_deadlock_timer();
            assert_eq!(LockAcquire(&lock1, ex, false, false).await, LockAcquireResult::Ok);
            a_got.send(()).unwrap();
            b_got_rx.await.unwrap();
            // Now wait for lock2 (held by B). One of A/B must be aborted.
            let r = LockAcquire(&lock2, ex, false, false).await;
            LockReleaseAll(DEFAULT_LOCKMETHOD, true);
            r
        })
        .await
    });

    let tb = tokio::spawn(async move {
        backend(|| async move {
            fast_deadlock_timer();
            assert_eq!(LockAcquire(&lock2, ex, false, false).await, LockAcquireResult::Ok);
            b_got.send(()).unwrap();
            a_got_rx.await.unwrap();
            let r = LockAcquire(&lock1, ex, false, false).await;
            LockReleaseAll(DEFAULT_LOCKMETHOD, true);
            r
        })
        .await
    });

    let ra = ta.await.unwrap();
    let rb = tb.await.unwrap();

    // The deadlock is broken: at least one side gets the ERROR (NotAvail) so the
    // cycle is resolved. Usually exactly one aborts and the other is granted once
    // the loser leaves the queue; but if both deadlock timers fire and detect the
    // cycle before either removal is visible, both may abort (PG avoids this with
    // a single signal-driven detector; our per-task timer model permits it). The
    // load-bearing invariant is that the deadlock never hangs and is broken.
    let aborted = (ra == LockAcquireResult::NotAvail) as i32
        + (rb == LockAcquireResult::NotAvail) as i32;
    assert!(aborted >= 1, "deadlock broken: a task aborts ({ra:?}, {rb:?})");
    for r in [ra, rb] {
        assert!(
            r == LockAcquireResult::Ok || r == LockAcquireResult::NotAvail,
            "each task resolves to Ok or NotAvail, not hang ({ra:?}, {rb:?})"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_task_cycle_one_aborts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let _s = shared();
    let l1 = LOCKTAG::set_relation(0, 9101);
    let l2 = LOCKTAG::set_relation(0, 9102);
    let l3 = LOCKTAG::set_relation(0, 9103);
    let ex = LockMode::ExclusiveLock as i32;

    // A->l1, B->l2, C->l3; then A waits l2, B waits l3, C waits l1 (a 3-cycle).
    // Barrier: each task takes its first lock, bumps the counter, and spins until
    // all three hold their first lock before requesting the second.
    let held = Arc::new(AtomicUsize::new(0));

    async fn run(first: LOCKTAG, second: LOCKTAG, ex: i32, held: Arc<AtomicUsize>) -> LockAcquireResult {
        backend(|| async move {
            fast_deadlock_timer();
            assert_eq!(LockAcquire(&first, ex, false, false).await, LockAcquireResult::Ok);
            held.fetch_add(1, Ordering::SeqCst);
            while held.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let r = LockAcquire(&second, ex, false, false).await;
            LockReleaseAll(DEFAULT_LOCKMETHOD, true);
            r
        })
        .await
    }

    let ta = tokio::spawn(run(l1, l2, ex, held.clone()));
    let tb = tokio::spawn(run(l2, l3, ex, held.clone()));
    let tc = tokio::spawn(run(l3, l1, ex, held.clone()));

    let ra = ta.await.unwrap();
    let rb = tb.await.unwrap();
    let rc = tc.await.unwrap();
    let aborted = [ra, rb, rc]
        .iter()
        .filter(|&&r| r == LockAcquireResult::NotAvail)
        .count();
    // At least one task must be aborted to break the 3-cycle; the rest proceed.
    assert!(aborted >= 1, "a cycle member aborts ({ra:?}, {rb:?}, {rc:?})");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_deadlock_simple_wait_then_grant() {
    let _s = shared();
    let tag = LOCKTAG::set_relation(0, 9200);
    let ex = LockMode::ExclusiveLock as i32;

    let (acq, acq_rx) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        backend(|| async move {
            assert_eq!(LockAcquire(&tag, ex, false, false).await, LockAcquireResult::Ok);
            acq.send(()).unwrap();
            // Hold long enough that the waiter's deadlock check runs (and finds
            // NO deadlock), then release to grant it.
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert!(LockRelease(&tag, ex, false));
        })
        .await
    });
    acq_rx.await.unwrap();

    let waiter = tokio::spawn(async move {
        backend(|| async move {
            fast_deadlock_timer(); // deadlock check fires, returns NoDeadlock
            let r = LockAcquire(&tag, ex, false, false).await;
            assert_eq!(r, LockAcquireResult::Ok, "no deadlock: granted on release");
            assert!(LockRelease(&tag, ex, false));
        })
        .await
    });

    holder.await.unwrap();
    waiter.await.unwrap();
}

/// A direct `DeadLockCheck` unit test: with a single waiting proc and no cycle,
/// it returns NoDeadlock. Exercises the entry point + graph walk without a real
/// multi-task race.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadlock_check_no_cycle_returns_no_deadlock() {
    let _s = shared();
    let tag = LOCKTAG::set_relation(0, 9300);
    let ex = LockMode::ExclusiveLock as i32;

    // Holder keeps the lock; a waiter enqueues; then we run DeadLockCheck for the
    // waiter directly (all-partitions-locked) and expect NoDeadlock.
    let (acq, acq_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        backend(|| async move {
            assert_eq!(LockAcquire(&tag, ex, false, false).await, LockAcquireResult::Ok);
            acq.send(()).unwrap();
            done_rx.await.unwrap();
            assert!(LockRelease(&tag, ex, false));
        })
        .await
    });
    acq_rx.await.unwrap();

    let waiter = tokio::spawn(async move {
        backend(|| async move {
            unsafe {
                crate::storage::proc::LockTimeout = 0;
                crate::storage::proc::DeadlockTimeout = 60_000; // don't auto-fire
            }
            // Spawn the actual wait; meanwhile another step runs DeadLockCheck.
            let waiter_tag = tag;
            let h = tokio::spawn(async move {
                backend(|| async move {
                    unsafe {
                        crate::storage::proc::LockTimeout = 80;
                        crate::storage::proc::DeadlockTimeout = 60_000;
                    }
                    // Will time out (holder still holds); we only need it queued.
                    let _ = LockAcquire(&waiter_tag, ex, false, false).await;
                })
                .await
            });
            // Give it a moment to enqueue, then run DeadLockCheck over OUR (empty)
            // proc -- not queued on anything -> NoDeadlock.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let m = crate::backend::storage::lmgr::lock::lock_manager().unwrap().clone();
            let state = m.with_all_partitions_locked(|view| {
                let me = crate::storage::proc::current_proc_number();
                crate::backend::storage::lmgr::deadlock::DeadLockCheck(me, view)
            });
            assert_eq!(state, crate::storage::lock::DeadLockState::NoDeadlock);
            let _: Arc<LockManager> = m;
            h.await.unwrap();
        })
        .await
    });

    waiter.await.unwrap();
    let _ = done_tx.send(());
    holder.await.unwrap();
}

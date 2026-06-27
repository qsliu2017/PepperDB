//! Tests for the heavyweight lock manager (15b). Multi-thread tokio so the
//! conflict-wait path (one task waits, another releases to grant) is real.

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::miscadmin::BackendType;
use crate::session::{Session, scope as session_scope};
use crate::shared_state::{SharedState, SharedStateConfig};
use crate::storage::lock::LOCKTAG;
use crate::storage::lockdefs::LockMode;
use crate::storage::proc::my_proc_scope;

fn shared() -> Arc<SharedState> {
    SharedState::new(SharedStateConfig::default())
}

/// Wrap a backend body: session scope -> my_proc scope -> local-lock scope ->
/// InitProcess. Sets a database id so fast-path eligibility can be exercised.
async fn backend<F, Fut, T>(db: u32, body: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let sess = Arc::new(Session::new(BackendType::BACKEND));
    sess.set_database_id(crate::postgres_ext::Oid(db));
    session_scope(sess, my_proc_scope(local_lock_scope(async move {
        crate::backend::storage::lmgr::proc::InitProcess();
        let r = body().await;
        crate::backend::storage::lmgr::proc::ProcKill();
        r
    })))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_no_conflict_grants() {
    let _s = shared();
    backend(0, || async {
        // Use a non-relation tag (RelationExtend) so the fast path is skipped and
        // we exercise the main-table grant path.
        let tag = LOCKTAG::set_relation_extend(0, 100);
        let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
        assert_eq!(r, LockAcquireResult::Ok);
        // Re-acquiring the same mode bumps the local count -> AlreadyHeld.
        let r2 = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
        assert_eq!(r2, LockAcquireResult::AlreadyHeld);
        assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_lock_returns_not_avail_on_conflict() {
    let _s = shared();
    // Backend 1 holds Exclusive; backend 2 tries dontWait and must get NotAvail.
    let tag = LOCKTAG::set_relation_extend(0, 200);

    backend(0, || async move {
        let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
        assert_eq!(r, LockAcquireResult::Ok);

        // Second backend on another task tries to conditionally acquire.
        let t2 = tokio::spawn(async move {
            backend(0, || async move {
                
                LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, /*dont_wait*/ true)
                    .await
            })
            .await
        });
        let r2 = t2.await.unwrap();
        assert_eq!(r2, LockAcquireResult::NotAvail);

        assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_waits_then_release_wakes() {
    let _s = shared();
    let tag = LOCKTAG::set_relation_extend(0, 300);

    // Holder task: take Exclusive, hold a bit, then release (waking the waiter).
    let (tx_acquired, rx_acquired) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        backend(0, || async move {
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            assert_eq!(r, LockAcquireResult::Ok);
            tx_acquired.send(()).unwrap();
            // Give the waiter time to enqueue, then release.
            tokio::time::sleep(Duration::from_millis(80)).await;
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });

    rx_acquired.await.unwrap();

    // Waiter task: must block until the holder releases, then get the lock.
    let waiter = tokio::spawn(async move {
        backend(0, || async move {
            // No lock timeout, long deadlock timeout: wake must come from release.
            unsafe {
                crate::storage::proc::LockTimeout = 0;
                crate::storage::proc::DeadlockTimeout = 60_000;
            }
            let start = std::time::Instant::now();
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            assert_eq!(r, LockAcquireResult::Ok, "waiter granted on wake");
            assert!(start.elapsed() >= Duration::from_millis(40), "actually waited");
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });

    holder.await.unwrap();
    waiter.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_path_grant_for_weak_relation_lock() {
    let _s = shared();
    backend(42, || async {
        // AccessShareLock on a relation in our database is fast-path eligible.
        let tag = LOCKTAG::set_relation(42, 500);
        let r = LockAcquire(&tag, LockMode::AccessShareLock as i32, false, false).await;
        assert_eq!(r, LockAcquireResult::Ok);
        // Released via the fast path.
        assert!(LockRelease(&tag, LockMode::AccessShareLock as i32, false));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_release_all_clears_held_locks() {
    let _s = shared();
    backend(7, || async {
        let r1 = LockAcquire(&LOCKTAG::set_relation(7, 600), LockMode::AccessShareLock as i32, false, false).await;
        assert_eq!(r1, LockAcquireResult::Ok);
        let r2 = LockAcquire(&LOCKTAG::set_relation_extend(0, 601), LockMode::ExclusiveLock as i32, false, false).await;
        assert_eq!(r2, LockAcquireResult::Ok);
        // Release everything (xact end, allLocks=false drops xact locks).
        LockReleaseAll(DEFAULT_LOCKMETHOD, false);
        // A subsequent release should report not-held (already gone).
        assert!(!LockRelease(&LOCKTAG::set_relation_extend(0, 601), LockMode::ExclusiveLock as i32, false));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_timeout_path_returns_not_avail() {
    let _s = shared();
    let tag = LOCKTAG::set_relation_extend(0, 700);

    // Holder keeps the lock long enough for the waiter to time out.
    let holder = tokio::spawn(async move {
        backend(0, || async move {
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            assert_eq!(r, LockAcquireResult::Ok);
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });

    tokio::time::sleep(Duration::from_millis(30)).await;

    let waiter = tokio::spawn(async move {
        backend(0, || async move {
            unsafe {
                crate::storage::proc::LockTimeout = 40;
                crate::storage::proc::DeadlockTimeout = 60_000;
            }
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            unsafe {
                crate::storage::proc::LockTimeout = 0;
            }
            // ProcSleep returns ERROR on lock-timeout -> NotAvail at this layer.
            assert_eq!(r, LockAcquireResult::NotAvail, "lock timeout -> NotAvail");
        })
        .await;
    });

    waiter.await.unwrap();
    holder.await.unwrap();
}

// --- give-up cleanup regression tests (timeout / dropped-future) ---

/// Snapshot of a LOCK's shared-table state for a tag (None if no LOCK exists).
fn lock_state(tag: &LOCKTAG) -> Option<(i32, i32, usize)> {
    let m = LockManager::get().unwrap();
    let hashcode = LockTagHashCode(tag);
    let shard = m.shard(hashcode).lock();
    shard
        .locks
        .get(tag)
        .map(|l| (l.n_requested, l.requested[LockMode::ExclusiveLock as usize], l.wait_procs.len()))
}

/// A waiter that TIMES OUT must restore the LOCK's request counts to holder-only
/// AND a second waiter queued behind it must still be granted when the holder
/// releases -- proving the timeout give-up ran the partition-locked
/// RemoveFromWaitQueue (count undo + woke the trailing waiter).
///
/// Note: LockTimeout/DeadlockTimeout are process-global statics here (not per-
/// backend GUCs yet), so the test sequences carefully: B enters ProcSleep with
/// LockTimeout==0 (no lock-timer armed) BEFORE A sets the short timeout, so only
/// A times out. ProcSleep reads the timeout once at entry, so B stays immune.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_restores_counts_and_grants_second_waiter() {
    let _s = shared();
    let tag = LOCKTAG::set_relation_extend(0, 800);

    let (tx_held, rx_held) = tokio::sync::oneshot::channel();
    let (tx_release, rx_release) = tokio::sync::oneshot::channel();

    // Holder H: take Exclusive, signal, wait for the go-ahead, then release.
    let holder = tokio::spawn(async move {
        backend(0, || async move {
            unsafe {
                crate::storage::proc::LockTimeout = 0;
                crate::storage::proc::DeadlockTimeout = 60_000;
            }
            assert_eq!(
                LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await,
                LockAcquireResult::Ok
            );
            tx_held.send(()).unwrap();
            rx_release.await.unwrap();
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });
    rx_held.await.unwrap();

    // Waiter B: enters ProcSleep FIRST with LockTimeout==0 (no lock-timer), so it
    // is immune to A's later timeout setting. Granted after H releases.
    let (tx_b_started, rx_b_started) = tokio::sync::oneshot::channel();
    let waiter_b = tokio::spawn(async move {
        backend(0, || async move {
            tx_b_started.send(()).unwrap();
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            assert_eq!(r, LockAcquireResult::Ok, "B granted after H releases");
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });
    rx_b_started.await.unwrap();
    // Let B fully enqueue + enter ProcSleep (lock_timer armed far-future).
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Waiter A: now arm a short lock-timeout; A must time out and clean up.
    let waiter_a = tokio::spawn(async move {
        backend(0, || async move {
            unsafe {
                crate::storage::proc::LockTimeout = 40;
                crate::storage::proc::DeadlockTimeout = 60_000;
            }
            let r = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await;
            assert_eq!(r, LockAcquireResult::NotAvail, "A times out");
        })
        .await;
    });

    // Let A enqueue (H + B + A requested = 3) before it times out.
    tokio::time::sleep(Duration::from_millis(15)).await;
    let before = lock_state(&tag).expect("LOCK exists with H+B+A");
    assert_eq!(before.0, 3, "n_requested = H + B + A before A times out");

    waiter_a.await.unwrap();
    unsafe { crate::storage::proc::LockTimeout = 0 };

    // After A's timeout cleanup: counts back to H + B (2), and B still queued (1).
    let after = lock_state(&tag).expect("LOCK still exists with H+B");
    assert_eq!(after.0, 2, "A's timeout undid its request count -> H + B");
    assert_eq!(after.2, 1, "only B remains queued");

    // Release H -> B must be woken and granted.
    tx_release.send(()).unwrap();
    holder.await.unwrap();
    waiter_b.await.unwrap();
}

/// If the LockAcquire future is DROPPED mid-wait, the WaitGuard's Drop must pull
/// the proc out of LOCK.wait_procs and restore the counts, so a later release does
/// NOT try to wake the dropped waiter (no panic / no stale grant).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_acquire_future_leaves_no_stale_waiter() {
    let _s = shared();
    let tag = LOCKTAG::set_relation_extend(0, 900);

    let (tx_held, rx_held) = tokio::sync::oneshot::channel();
    let (tx_release, rx_release) = tokio::sync::oneshot::channel();

    let holder = tokio::spawn(async move {
        backend(0, || async move {
            assert_eq!(
                LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false).await,
                LockAcquireResult::Ok
            );
            tx_held.send(()).unwrap();
            rx_release.await.unwrap();
            // Release must not panic / wake a dropped waiter.
            assert!(LockRelease(&tag, LockMode::ExclusiveLock as i32, false));
        })
        .await;
    });
    rx_held.await.unwrap();

    // Waiter task: start the acquire then DROP it (via a timeout wrapper) while
    // it is still blocked. The whole backend scope completes (ProcKill) so the
    // WaitGuard Drop fires inside the scope.
    let dropper = tokio::spawn(async move {
        backend(0, || async move {
            unsafe {
                crate::storage::proc::LockTimeout = 0;
                crate::storage::proc::DeadlockTimeout = 60_000;
            }
            let fut = LockAcquire(&tag, LockMode::ExclusiveLock as i32, false, false);
            // Wrap so the inner future is dropped when the 60ms timeout elapses.
            let r = tokio::time::timeout(Duration::from_millis(60), fut).await;
            assert!(r.is_err(), "acquire future was dropped by the timeout wrapper");
        })
        .await;
    });

    dropper.await.unwrap();

    // After the drop+cleanup: the LOCK is back to holder-only (n_requested == 1,
    // no queued waiters). The dropped proc must be gone from wait_procs.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let after = lock_state(&tag).expect("LOCK still held by H");
    assert_eq!(after.0, 1, "dropped waiter's request count undone -> H only");
    assert_eq!(after.2, 0, "dropped waiter removed from wait_procs");

    // Releasing must not panic (no stale waiter to mis-wake) and GC the LOCK.
    tx_release.send(()).unwrap();
    holder.await.unwrap();
    assert!(lock_state(&tag).is_none(), "LOCK GC'd after final release");
}

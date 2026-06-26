//! Translated from PostgreSQL src/backend/storage/lmgr/condition_variable.c
//!
//! Implemented over [`WaitQueue`]/[`WaitGuard`]. PG tracks the prepared sleep in
//! the process-global `cv_sleep_target` plus one `cvWaitLink` in PGPROC; that
//! per-process state is replaced by a per-task guard on the caller's stack
//! ([`CvSleep`]). The guard survives `.await` and tokio thread migration (no
//! thread-locals), and its `Drop` dequeues the waiter -- the cancellation that PG
//! spells `ConditionVariableCancelSleep`.
//!
//! Protocol (mirrors PG's predicate loop):
//!
//! ```ignore
//! let mut s = ConditionVariablePrepareToSleep(&cv);
//! while !predicate() { s.sleep(info).await; }  // drop(s) on scope exit = cancel
//! ```
//!
//! The enqueue happens up front in `PrepareToSleep`, before the caller's first
//! predicate test, exactly as PG does. A `Signal` arriving between predicate-false
//! and the `.await` cannot be lost: the waiter is already enqueued, and
//! `WaitQueue` sets a sticky woken flag even on a not-yet-polled slot. Each
//! `sleep` re-arms (re-enqueues a fresh `WaitGuard`) before returning, so the
//! waiter is enqueued again across the caller's predicate re-check.

use crate::storage::condition_variable::ConditionVariable;
use crate::storage::wait_guard::WaitGuard;

impl ConditionVariable {
    /// Initialize the condition variable.
    ///
    /// Nothing to do: `ConditionVariable::default()` already yields an empty
    /// queue. Kept for API parity with PG's ConditionVariableInit.
    #[inline]
    pub fn init(&self) {}

    /// Wake the oldest waiter, if any. Synchronous (callable from critical
    /// sections).
    pub fn signal(&self) {
        self.wakeup.wake_one();
    }

    /// Wake every current waiter. Synchronous.
    pub fn broadcast(&self) {
        self.wakeup.wake_all();
    }

    /// Prepare to wait on this cv: enqueue a waiter now and return a guard that
    /// holds it across the caller's predicate loop. Enqueuing before the first
    /// predicate test is what closes the missed-signal window.
    pub fn prepare_to_sleep(&self) -> CvSleep<'_> {
        CvSleep {
            cv: self,
            guard: self.wakeup.enqueue(),
        }
    }
}

/// A per-task prepared sleep on a condition variable.
///
/// Enqueued on construction; holds one armed [`WaitGuard`] at all times between
/// `sleep` calls. Drop dequeues (cancellation-safe), replacing PG's
/// `ConditionVariableCancelSleep`.
pub struct CvSleep<'cv> {
    cv: &'cv ConditionVariable,
    guard: WaitGuard<'cv>,
}

impl CvSleep<'_> {
    /// Await the currently-armed wakeup, then re-arm for the next iteration.
    ///
    /// `wait_event_info` mirrors PG's pg_stat_activity wait-event reporting; it is
    /// accepted for API parity (no wait-event surface yet).
    pub async fn sleep(&mut self, _wait_event_info: u32) {
        // Await the armed guard, then immediately enqueue a fresh one so the
        // waiter is already queued when the caller re-checks its predicate.
        let armed = std::mem::replace(&mut self.guard, self.cv.wakeup.enqueue());
        armed.await;
    }

    /// Like [`sleep`](Self::sleep) but bounded by `timeout_ms`; returns true on
    /// timeout. A negative timeout means no timeout. Re-arms in either case.
    pub async fn timed_sleep(&mut self, timeout_ms: i64, wait_event_info: u32) -> bool {
        if timeout_ms < 0 {
            self.sleep(wait_event_info).await;
            return false;
        }
        let armed = std::mem::replace(&mut self.guard, self.cv.wakeup.enqueue());
        let dur = std::time::Duration::from_millis(timeout_ms as u64);
        // On timeout `armed` is dropped here (dequeued); the fresh guard remains.
        tokio::time::timeout(dur, armed).await.is_err()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    // The headline missed-signal race. The waiter prepares (enqueues) up front,
    // then a producer task flips the condition and signals in the window between
    // the consumer's predicate-false observation and its `.await`. Because the
    // waiter is already enqueued and WaitQueue sets a sticky woken flag, the
    // signal is not lost: the sleep returns promptly. The 5s timeout is a pure
    // failsafe; the body should never hit it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signal_racing_predicate_window_is_not_lost() {
        struct Shared {
            cv: ConditionVariable,
            ready: AtomicBool,
        }
        for _ in 0..200 {
            let shared = Arc::new(Shared {
                cv: ConditionVariable::new(),
                ready: AtomicBool::new(false),
            });

            let mut s = shared.cv.prepare_to_sleep();

            // Producer races the consumer's predicate window.
            let p = shared.clone();
            let producer = tokio::spawn(async move {
                p.ready.store(true, Ordering::Release);
                p.cv.signal();
            });

            // Consumer predicate loop. If the signal landed in the window, the
            // sleep must still return; the loop then sees ready == true.
            let woke = tokio::time::timeout(Duration::from_secs(5), async {
                while !shared.ready.load(Ordering::Acquire) {
                    s.sleep(0).await;
                }
            })
            .await;

            woke.expect("signal in predicate window must wake the waiter");
            producer.await.unwrap();
            assert!(shared.ready.load(Ordering::Acquire));
        }
    }

    // A signal delivered to a freshly-prepared, not-yet-awaited waiter must make
    // the first sleep().await return immediately (sticky woken on unpolled slot).
    #[tokio::test]
    async fn signal_before_first_await_returns_immediately() {
        let cv = ConditionVariable::new();
        let mut s = cv.prepare_to_sleep();
        cv.signal(); // waiter enqueued but never polled

        tokio::time::timeout(Duration::from_millis(500), s.sleep(0))
            .await
            .expect("signal before first poll must wake immediately");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broadcast_wakes_multiple_preparers() {
        let cv = Arc::new(ConditionVariable::new());
        let flag = Arc::new(AtomicBool::new(false));

        let sleepers: Vec<_> = (0..3)
            .map(|_| {
                let cv = cv.clone();
                let flag = flag.clone();
                tokio::spawn(async move {
                    let mut s = cv.prepare_to_sleep();
                    while !flag.load(Ordering::Acquire) {
                        s.sleep(0).await;
                    }
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(20)).await;
        flag.store(true, Ordering::Release);
        cv.broadcast();

        for s in sleepers {
            tokio::time::timeout(Duration::from_secs(1), s)
                .await
                .expect("each preparer wakes")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn timed_sleep_returns_true_on_timeout() {
        let cv = ConditionVariable::new();
        let mut s = cv.prepare_to_sleep();
        let timed_out = s.timed_sleep(30, 0).await;
        assert!(timed_out, "should time out when never signalled");
        drop(s);
        assert!(cv.wakeup.is_empty(), "all guards dequeued after drop");
    }

    // Dropping a CvSleep mid-wait must dequeue its slot and not leak; a later
    // Signal must not target the dropped waiter.
    #[tokio::test]
    async fn drop_mid_wait_dequeues_and_signal_misses() {
        let cv = ConditionVariable::new();
        let survivor = cv.prepare_to_sleep();
        let leaver = cv.prepare_to_sleep();
        assert_eq!(cv.wakeup.len(), 2);

        drop(leaver); // cancel before being woken
        assert_eq!(cv.wakeup.len(), 1, "dropped CvSleep must dequeue");

        // Signal targets the survivor's armed guard; once consumed the queue
        // empties (sleep does not re-arm because we drop the guard instead).
        cv.signal();
        assert!(survivor.guard.is_woken(), "signal hit the surviving waiter");
        drop(survivor);
        assert_eq!(cv.wakeup.len(), 0);
    }
}

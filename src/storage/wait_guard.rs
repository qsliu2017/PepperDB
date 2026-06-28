//! Rust-native wait-queue primitive (no C origin).
//!
//! THE reusable wait queue behind condition variables, buffer IO-waits, lock
//! waits, and sinval. It replaces PG's per-structure `proclist` + spinlock with
//! a generational slab of `Waker`s guarded by a std `Mutex`.
//!
//! Two invariants from the foundation design are enforced by the API shape:
//!
//!  * No std lock guard is held across an `.await`. Every critical section here
//!    (insert/store-waker/remove/take-wakers) locks, mutates, and drops the guard
//!    before any suspension point. `WaitGuard::poll` takes the lock only to write
//!    the current `Waker`, then releases it and returns `Pending`.
//!
//!  * A future waiting in a shared queue removes itself on `Drop`. `WaitGuard`'s
//!    `Drop` removes its slot from the slab, so a cancelled/dropped waiter never
//!    leaves a stale entry that a later `wake_one` would target. This is the
//!    cancellation-safety guarantee.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

use crate::storage::procnumber::{GenSlab, Key};

/// State stored per waiter: an arrival sequence number for FIFO wakeup, a place
/// to park the current `Waker`, and a flag the waker sets so the guard's next
/// poll returns `Ready`.
struct Waiter {
    seq: u64,
    waker: Option<Waker>,
    woken: bool,
}

/// A FIFO wait queue. Waiters enqueue a slot and await a [`WaitGuard`];
/// `wake_one`/`wake_all` flag and wake parked wakers. All methods are sync so
/// they can run inside hot critical sections.
#[derive(Default)]
pub struct WaitQueue {
    slots: Mutex<GenSlab<Waiter>>,
    // Monotonic arrival counter: wakeup order follows `seq`, not slab index,
    // since the slab reuses freed indices LIFO (PG's proclist is strict FIFO).
    next_seq: AtomicU64,
}

impl WaitQueue {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(GenSlab::new()),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Enqueue a slot and return a guard. The guard is a `Future` that completes
    /// once this slot is woken, and removes the slot on `Drop`.
    pub fn enqueue(&self) -> WaitGuard<'_> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let key = self.slots.lock().insert(Waiter {
            seq,
            waker: None,
            woken: false,
        });
        WaitGuard { queue: self, key }
    }

    /// Convenience: enqueue and await. Cancellation-safe (drop dequeues).
    pub async fn wait(&self) {
        self.enqueue().await;
    }

    /// Wake the oldest waiter, if any. Sync.
    pub fn wake_one(&self) {
        let mut slots = self.slots.lock();
        // Oldest = lowest arrival seq (slab index is not arrival order: freed
        // indices are reused LIFO). Mirrors PG's FIFO proclist_pop_head_node.
        let oldest = slots.iter().min_by_key(|(_, w)| w.seq).map(|(k, _)| k);
        if let Some(key) = oldest
            && let Some(w) = slots.get_mut(key) {
                w.woken = true;
                let waker = w.waker.take();
                drop(slots); // release lock before .wake()
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
    }

    /// Wake every waiter currently in the queue. Sync.
    pub fn wake_all(&self) {
        let mut slots = self.slots.lock();
        let wakers: Vec<Waker> = slots
            .iter()
            .map(|(k, _)| k)
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|key| {
                let w = slots.get_mut(key)?;
                w.woken = true;
                w.waker.take()
            })
            .collect();
        drop(slots); // release before waking
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Number of waiters currently enqueued. Mainly for tests/assertions.
    pub fn len(&self) -> usize {
        self.slots.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A waiter's handle into a [`WaitQueue`]. Awaiting it parks until woken;
/// dropping it dequeues the slot (cancellation-safe).
pub struct WaitGuard<'q> {
    queue: &'q WaitQueue,
    key: Key<Waiter>,
}

impl WaitGuard<'_> {
    /// True if this guard's slot has been flagged woken (not yet polled to Ready).
    pub fn is_woken(&self) -> bool {
        self.queue
            .slots
            .lock()
            .get(self.key)
            .is_none_or(|w| w.woken)
    }
}

impl Future for WaitGuard<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut slots = self.queue.slots.lock();
        match slots.get_mut(self.key) {
            Some(w) if w.woken => Poll::Ready(()),
            Some(w) => {
                // Park the latest waker; lock drops at end of scope, before await.
                w.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            None => Poll::Ready(()), // slot gone: treat as woken
        }
    }
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        // Dequeue-on-Drop: a cancelled waiter must not linger in the queue.
        self.queue.slots.lock().remove(self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn wake_one_wakes_exactly_one_of_two() {
        let q = Arc::new(WaitQueue::new());
        let (q1, q2) = (q.clone(), q.clone());
        let w1 = tokio::spawn(async move { q1.wait().await });
        let w2 = tokio::spawn(async move { q2.wait().await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(q.len(), 2);

        q.wake_one();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let f1 = w1.is_finished();
        let f2 = w2.is_finished();
        assert!(f1 ^ f2, "exactly one should finish, got {f1} {f2}");
        assert_eq!(q.len(), 1, "one waiter remains enqueued");

        q.wake_one(); // clean up the other
        let () = tokio::time::timeout(Duration::from_secs(1), async {
            let _ = w1.await;
            let _ = w2.await;
        })
        .await
        .expect("both finish after second wake");
    }

    #[tokio::test]
    async fn wake_all_wakes_both() {
        let q = Arc::new(WaitQueue::new());
        let (q1, q2) = (q.clone(), q.clone());
        let w1 = tokio::spawn(async move { q1.wait().await });
        let w2 = tokio::spawn(async move { q2.wait().await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(q.len(), 2);
        q.wake_all();

        tokio::time::timeout(Duration::from_secs(1), async {
            let _ = w1.await;
            let _ = w2.await;
        })
        .await
        .expect("both wake");
        assert_eq!(q.len(), 0);
    }

    #[tokio::test]
    async fn drop_before_wake_dequeues() {
        let q = WaitQueue::new();
        let g1 = q.enqueue();
        let g2 = q.enqueue();
        assert_eq!(q.len(), 2);

        drop(g1); // cancel before being woken
        assert_eq!(q.len(), 1, "dropped guard must dequeue");

        // wake_one must target the surviving slot, not the dropped one.
        q.wake_one();
        assert!(g2.is_woken(), "the remaining waiter was woken");
        drop(g2);
        assert_eq!(q.len(), 0);
    }
}

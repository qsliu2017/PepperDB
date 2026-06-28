//! Inter-task latches for wait/wake coordination. Translated from backend/storage/ipc/latch.c.
//!
//! A latch is a reliable replacement for the common pattern of sleeping in a
//! loop until a flag variable is set by another party. A waiter blocks until
//! the latch is set; a setter wakes any current or future waiter. The flag is
//! sticky: a set that races just ahead of a wait is not lost, so callers follow
//! the convention of resetting the latch, testing their own predicate, and only
//! then waiting -- guaranteeing no wakeup falls between the test and the wait.
//!
//! In PostgreSQL a latch wraps OS-specific multiplexing (a self-pipe or SIGURG
//! signal feeding an epoll/kqueue/poll/win32 wait set) so that one process can
//! wake another, optionally alongside socket readiness and postmaster-death
//! events. PepperDB runs as a single process with cooperatively scheduled tokio
//! tasks, so that machinery is dropped: a `tokio::sync::Notify` carries the
//! wakeup and an `AtomicBool` holds the sticky set/reset flag. Setting and
//! resetting stay synchronous and so remain callable from inside a critical
//! section, while waiting is `async`. The shared/process-local distinction,
//! latch ownership, and the bundled socket and postmaster-death wait events have
//! no analogue here and are not implemented.

use std::sync::atomic::Ordering;

use tokio::sync::Notify;

use crate::storage::latch::Latch;

impl Latch {
    /// A fresh, unset latch.
    pub fn new() -> Self {
        Self {
            is_set: std::sync::atomic::AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Initialize the latch: clear its bit. (PG's InitLatch/InitSharedLatch.)
    #[inline]
    pub fn init(&self) {
        self.reset();
    }

    /// Wait until the latch is set. Returns immediately if already set.
    ///
    /// Stored-permit + sticky flag: `set` stores `is_set=true` and leaves a
    /// permit on the Notify, so a set that races just before this `.await` is not
    /// lost -- we re-check `is_set` after each wakeup. The caller follows PG's
    /// convention of `reset` then re-testing its predicate at loop bottom.
    pub async fn wait(&self) {
        loop {
            if self.is_set.load(Ordering::Acquire) {
                return;
            }
            // Arm the future before re-checking is_set to avoid a lost wakeup.
            let notified = self.notify.notified();
            if self.is_set.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Set the latch: wake a current or future waiter. Synchronous -- callable
    /// from a sync critical section. Idempotent.
    ///
    /// PG's `maybe_sleeping` signal-skip is intentionally dropped: `Notify`
    /// makes `notify_one()` cheap and idempotent (a no-waiter notify leaves at
    /// most one permit), so we always notify. Dropping it removes the need for
    /// `ResetLatch`'s StoreLoad barrier to gate a signal-skip decision.
    pub fn set(&self) {
        // Quick exit if already set (matches PG, and keeps the permit count at one).
        if self.is_set.swap(true, Ordering::Release) {
            return;
        }
        self.notify.notify_one();
    }

    /// Clear the latch. A later `wait()` will block unless the latch is set again
    /// before that call. Synchronous.
    pub fn reset(&self) {
        // SeqCst mirrors ResetLatch's pg_memory_barrier(): is_set=false must be
        // globally visible before the caller reads any associated flag variable.
        // Required if any set-side sleeping-skip is ever reintroduced.
        self.is_set.store(false, Ordering::SeqCst);
    }
}

impl Default for Latch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn set_before_wait_still_wakes() {
        let latch = Latch::new();
        latch.set(); // set BEFORE any wait
        // Must return immediately (sticky is_set).
        latch.wait().await;
    }

    #[tokio::test]
    async fn wait_pends_then_set_from_other_task_wakes() {
        let latch = Arc::new(Latch::new());
        let l2 = latch.clone();
        let waiter = tokio::spawn(async move { l2.wait().await });

        // Give the waiter a moment to reach the await, then set.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter should still be pending");
        latch.set();

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake within timeout")
            .expect("waiter task panicked");
    }

    #[tokio::test]
    async fn reset_clears_so_next_wait_pends() {
        let latch = Arc::new(Latch::new());
        latch.set();
        latch.wait().await; // consumes nothing; flag still set
        latch.reset();

        let l2 = latch.clone();
        let waiter = tokio::spawn(async move { l2.wait().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "after reset, wait must pend again");

        latch.set();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake")
            .unwrap();
    }
}

//! Translated from PostgreSQL src/backend/storage/ipc/latch.c
//!
//! The OS-specific multiplexing (self-pipe, SIGURG, epoll/kqueue/poll/win32) is
//! deleted -- tokio's Notify replaces the wakeup transport. Only the latch's
//! observable wait/set/reset contract is kept.

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
        self.is_set.store(false, Ordering::Release);
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

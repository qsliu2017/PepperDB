//! Translated from PostgreSQL src/include/storage/condition_variable.h
//! Condition variables.
//!
//! PG's ConditionVariable is a shared-memory wait list guarded by a spinlock
//! (slock_t + proclist_head). Under the single-process async model it collapses
//! to a [`WaitQueue`] (a generational slab of wakers); the shmem/spinlock fields
//! are dropped.
//!
//! The prepared-sleep state PG kept in process globals (`cv_sleep_target` plus
//! one `cvWaitLink`) becomes a per-task guard ([`CvSleep`]) on the caller's
//! stack: it survives `.await` and thread migration, and its `Drop` dequeues
//! (this is PG's `ConditionVariableCancelSleep`). Typical loop:
//!
//! ```ignore
//! let mut s = ConditionVariablePrepareToSleep(&cv);
//! while !predicate() { s.sleep(info).await; }  // drop(s) on scope exit = cancel
//! ```

use crate::storage::wait_guard::WaitQueue;

/// A method of waiting until a condition becomes true.
#[derive(Default)]
pub struct ConditionVariable {
    pub(crate) wakeup: WaitQueue,
}

// CV_MINIMAL_SIZE / ConditionVariableMinimallyPadded existed only to avoid
// cache-line crossing for shmem arrays; irrelevant under single-process.

impl ConditionVariable {
    pub fn new() -> Self {
        Self {
            wakeup: WaitQueue::new(),
        }
    }
}

// CvSleep is a type, re-exported on the condition_variable.h API surface.
pub use crate::backend::storage::lmgr::condition_variable::CvSleep;

// The cv behavior lives as idiomatic methods on `ConditionVariable` (in the
// backend module). The original C-named free functions are kept here as
// deprecated inline shims for cross-reference and mechanical-port compatibility.

#[deprecated(note = "use `cv.init()`")]
#[inline]
pub fn ConditionVariableInit(cv: &ConditionVariable) {
    cv.init()
}

#[deprecated(note = "use `cv.signal()`")]
#[inline]
pub fn ConditionVariableSignal(cv: &ConditionVariable) {
    cv.signal()
}

#[deprecated(note = "use `cv.broadcast()`")]
#[inline]
pub fn ConditionVariableBroadcast(cv: &ConditionVariable) {
    cv.broadcast()
}

#[deprecated(note = "use `cv.prepare_to_sleep()`")]
#[inline]
pub fn ConditionVariablePrepareToSleep(cv: &ConditionVariable) -> CvSleep<'_> {
    cv.prepare_to_sleep()
}

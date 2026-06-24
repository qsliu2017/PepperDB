//! Translated from PostgreSQL src/include/storage/condition_variable.h
//! Condition variables.
//!
//! PG's ConditionVariable is a shared-memory wait list guarded by a spinlock
//! (slock_t + proclist_head). Under the single-process async model the whole
//! thing collapses to a tokio-style notifier; the shmem/spinlock fields are
//! dropped. API shape is preserved so callers port mechanically.

// NOTE: in the async-coloring pass this becomes a `tokio::sync::Notify` wrapper
// (tokio is not yet a dependency, and signatures stay synchronous in this phase).

/// A method of waiting until a condition becomes true.
#[derive(Default)]
pub struct ConditionVariable {
    _private: (), // TODO(async): replace with tokio::sync::Notify
}

// CV_MINIMAL_SIZE / ConditionVariableMinimallyPadded existed only to avoid
// cache-line crossing for shmem arrays; irrelevant under single-process.

impl ConditionVariable {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Initialize a condition variable.
pub fn ConditionVariableInit(_cv: &ConditionVariable) {
    unimplemented!()
}

/// Sleep until signalled. (Will become `async` when the I/O layer is colored.)
pub fn ConditionVariableSleep(_cv: &ConditionVariable, _wait_event_info: u32) {
    unimplemented!()
}

/// Sleep until signalled or `timeout` (ms) elapses; returns true on timeout.
pub fn ConditionVariableTimedSleep(
    _cv: &ConditionVariable,
    _timeout: i64,
    _wait_event_info: u32,
) -> bool {
    unimplemented!()
}

/// Remove the caller from the wait list; returns true if it was waiting.
pub fn ConditionVariableCancelSleep() -> bool {
    unimplemented!()
}

/// Optional pre-loop hook; more efficient if at least one sleep is needed.
pub fn ConditionVariablePrepareToSleep(_cv: &ConditionVariable) {
    unimplemented!()
}

/// Wake up a single waiter.
pub fn ConditionVariableSignal(_cv: &ConditionVariable) {
    unimplemented!()
}

/// Wake up every waiter.
pub fn ConditionVariableBroadcast(_cv: &ConditionVariable) {
    unimplemented!()
}

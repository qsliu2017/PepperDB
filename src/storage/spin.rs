//! storage/spin.h - API for spinlocks (thin wrappers over s_lock.h primitives).
//!
//! The interface to spinlocks is defined by the typedef `slock_t` and the four
//! macros `SpinLockInit`, `SpinLockAcquire`, `SpinLockRelease`, `SpinLockFree`.
//! In C these expand to the hardware-dependent macros `S_INIT_LOCK`, `S_LOCK`,
//! `S_UNLOCK`, `S_LOCK_FREE` supplied by `storage/s_lock.h`.  This header adds
//! no extra functionality of its own; it simply renames the s_lock.h macros.
//!
//! Callers must beware that the C macro argument may be evaluated multiple
//! times.  Load/store operations in calling code are guaranteed not to be
//! reordered with respect to these operations (compiler barrier).

// `bool` here is the Rust primitive - do NOT import it. `SpinLockFree`
// returns that primitive `bool`.
use crate::storage::lmgr::s_lock::slock_t;

// The real s_lock.h primitives used by the macros below.  `S_INIT_LOCK`,
// `S_LOCK`, `S_UNLOCK`, and `S_LOCK_FREE` are platform-specific macros; on the
// generic build they reduce to setting/clearing the `slock_t` and calling
// `tas`.  We delegate to the ported s_lock primitives where they exist.
use crate::storage::lmgr::s_lock::{s_unlock, tas};

/// `#define SpinLockInit(lock) S_INIT_LOCK(lock)`
///
/// Initialize a spinlock to the unlocked (free) state.  On the generic
/// implementation `S_INIT_LOCK` is `S_UNLOCK`, i.e. store the unlocked value.
#[inline]
pub unsafe fn SpinLockInit(lock: *mut slock_t) {
    // S_INIT_LOCK(lock) == S_UNLOCK(lock) on the generic implementation.
    s_unlock(lock);
}

/// `#define SpinLockAcquire(lock) S_LOCK(lock)`
///
/// Acquire a spinlock, waiting if necessary.  Times out and abort()s if unable
/// to acquire the lock in a "reasonable" amount of time (typically ~1 minute).
#[inline]
pub unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    // S_LOCK does TAS_SPIN, falling back to the contended s_lock() backoff path.
    // The generic test-and-set returns 0 on success (lock acquired).
    if tas(lock) != 0 {
        // TODO: dedup when the full S_LOCK contended-backoff path
        // (storage/lmgr/s_lock.rs s_lock) is wired through here.
        unimplemented!("S_LOCK contended path");
    }
}

/// `#define SpinLockRelease(lock) S_UNLOCK(lock)`
///
/// Unlock a previously acquired lock.
#[inline]
pub unsafe fn SpinLockRelease(lock: *mut slock_t) {
    s_unlock(lock);
}

/// `#define SpinLockFree(lock) S_LOCK_FREE(lock)`
///
/// Tests if the lock is free.  Returns true if free, false if locked.  Does
/// *not* change the state of the lock.  On the generic implementation
/// `S_LOCK_FREE(lock)` is `(*(lock) == 0)`.
#[inline]
pub unsafe fn SpinLockFree(lock: *mut slock_t) -> bool {
    *lock == 0
}

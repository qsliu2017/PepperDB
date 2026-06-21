//! src/backend/storage/lmgr/s_lock.c
//!
//! Implementation of spinlocks: contention backoff for waiting on a contended
//! spinlock.  Merges the `SpinDelayStatus` struct + `init_spin_delay` /
//! `DEFAULT_SPINS_PER_DELAY` declarations from `src/include/storage/s_lock.h`.
//!
//! When waiting for a contended spinlock we loop tightly for a while, then
//! delay using `pg_usleep()` and try again.  Once we decide to block we use
//! randomly increasing `pg_usleep()` delays (1ms up to ~1s, then reset), and
//! after `NUM_DELAYS` delays we declare a "stuck spinlock" PANIC.
//!
//! Platform notes for this port:
//! - The hardware-specific TAS / SPIN_DELAY asm is replaced with a portable
//!   Rust `AtomicI32::compare_exchange` (see `tas`) so the high-value
//!   platform-independent backoff logic (`perform_spin_delay`,
//!   `s_lock_stuck`, `finish_spin_delay`) stays faithful.
//! - `pgstat_report_wait_start` / `_end` wait-event reporting is dropped.
//! - `pg_global_prng_state` lives at `crate::common::pg_prng`; if it has not
//!   been seeded we lazily seed it once from the clock here.

use crate::prelude::*;

use core::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use crate::c::{Max, Min};
use crate::common::pg_prng::{
    pg_global_prng_state, pg_prng_double, pg_prng_seed, pg_prng_seed_check,
};
use crate::port::pgsleep::pg_usleep;

// #define from s_lock.c
const MIN_SPINS_PER_DELAY: c_int = 10;
const MAX_SPINS_PER_DELAY: c_int = 1000;
pub const NUM_DELAYS: c_int = 1000;
const MIN_DELAY_USEC: c_long = 1000;
const MAX_DELAY_USEC: c_long = 1000000;

// #define DEFAULT_SPINS_PER_DELAY 100 (from s_lock.h)
pub const DEFAULT_SPINS_PER_DELAY: c_int = 100;

/// slock_t modeled as a plain C int (the real type is platform-specific).
pub type slock_t = c_int;

/// `SpinDelayStatus` (from s_lock.h): accumulator carried across
/// `perform_spin_delay` calls within one `s_lock` wait loop.
#[repr(C)]
pub struct SpinDelayStatus {
    pub spins: c_int,
    pub delays: c_int,
    pub cur_delay: c_int,
    pub file: *const c_char,
    pub line: c_int,
    pub func: *const c_char,
}

/// `static int spins_per_delay = DEFAULT_SPINS_PER_DELAY;`
///
/// Process-local tuning estimate of how many tight-loop tries to do before
/// blocking.  Modeled with an atomic so the `static mut` access is sound.
static SPINS_PER_DELAY: AtomicI32 = AtomicI32::new(DEFAULT_SPINS_PER_DELAY);

/// Tracks whether `pg_global_prng_state` has been seeded for our use.
static PRNG_SEEDED: AtomicBool = AtomicBool::new(false);

#[inline]
fn spins_per_delay() -> c_int {
    SPINS_PER_DELAY.load(Ordering::Relaxed)
}

#[inline]
fn set_spins_per_delay_value(v: c_int) {
    SPINS_PER_DELAY.store(v, Ordering::Relaxed);
}

/// `init_spin_delay` (static inline in s_lock.h): zero the counters and record
/// the caller's source location for the stuck-spinlock message.
#[inline]
pub fn init_spin_delay(
    status: *mut SpinDelayStatus,
    file: *const c_char,
    line: c_int,
    func: *const c_char,
) {
    unsafe {
        (*status).spins = 0;
        (*status).delays = 0;
        (*status).cur_delay = 0;
        (*status).file = file;
        (*status).line = line;
        (*status).func = func;
    }
}

/// Convenience constructor (by-value flavor of `init_spin_delay`).
#[inline]
pub fn make_spin_delay_status(
    file: *const c_char,
    line: c_int,
    func: *const c_char,
) -> SpinDelayStatus {
    SpinDelayStatus {
        spins: 0,
        delays: 0,
        cur_delay: 0,
        file,
        line,
        func,
    }
}

/// `s_lock_stuck()` - complain about a stuck spinlock.  Diverges (PANIC).
fn s_lock_stuck(file: *const c_char, line: c_int, func: *const c_char) -> ! {
    // C substitutes "(unknown)" for a NULL func.  We only have the raw ids to
    // format (a *const c_char cannot be {}-formatted), so report them numerically.
    let _ = (file, func);
    elog!(
        PANIC,
        "stuck spinlock detected at func/file ptr, line {}",
        line
    );
    unreachable!()
}

/// `s_lock(lock)` - platform-independent portion of waiting for a spinlock.
///
/// Spins on `TAS_SPIN` (here: `tas`) until the lock is acquired, performing the
/// backoff delay each contended iteration, then finalizes the tuning estimate.
/// Returns the number of delays incurred.
#[no_mangle]
pub fn s_lock(
    lock: *mut slock_t,
    file: *const c_char,
    line: c_int,
    func: *const c_char,
) -> c_int {
    let mut delay_status = make_spin_delay_status(file, line, func);

    while tas(lock) != 0 {
        perform_spin_delay(&mut delay_status);
    }

    finish_spin_delay(&mut delay_status);

    delay_status.delays
}

/// Default `s_unlock` (USE_DEFAULT_S_UNLOCK): release the lock.
#[inline]
pub fn s_unlock(lock: *mut slock_t) {
    // Atomic store with release ordering to mirror the required memory fence.
    unsafe {
        AtomicI32::from_ptr(lock).store(0, Ordering::Release);
    }
}

/// Atomic test-and-set (`TAS`/`TAS_SPIN`).  Portable stand-in for the
/// platform asm: returns 0 on success (lock acquired), nonzero if already held.
///
/// TODO: the real PostgreSQL `tas()` is a hand-written per-architecture asm
/// sequence; this `compare_exchange` is functionally equivalent on the
/// platforms Rust's atomics target but is not a 1:1 instruction translation.
#[inline]
pub fn tas(lock: *mut slock_t) -> c_int {
    let atomic = unsafe { AtomicI32::from_ptr(lock) };
    match atomic.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// Seed the global PRNG once if nobody has done so yet, so the delay
/// randomization in `perform_spin_delay` produces useful values.
unsafe fn ensure_prng_seeded() {
    if PRNG_SEEDED.swap(true, Ordering::Relaxed) {
        return;
    }
    let state = &raw mut pg_global_prng_state;
    if !pg_prng_seed_check(state) {
        // Seed from the wall clock (mirrors the S_LOCK_TEST main()).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as uint64)
            .unwrap_or(0);
        pg_prng_seed(state, now);
    }
}

/// `perform_spin_delay` - wait while spinning on a contended spinlock.
///
/// Every `spins_per_delay` tight tries, block with an exponentially (randomly
/// 1X-2X) increasing `pg_usleep`, wrapping back to the minimum once the max is
/// exceeded, and PANIC via `s_lock_stuck` after `NUM_DELAYS` delays.
pub fn perform_spin_delay(status: *mut SpinDelayStatus) {
    // SPIN_DELAY() is a CPU pause hint; std::hint::spin_loop is the portable form.
    core::hint::spin_loop();

    unsafe {
        let s = &mut *status;

        s.spins += 1;
        if s.spins >= spins_per_delay() {
            s.delays += 1;
            if s.delays > NUM_DELAYS {
                s_lock_stuck(s.file, s.line, s.func);
            }

            if s.cur_delay == 0 {
                // first time to delay?
                s.cur_delay = MIN_DELAY_USEC as c_int;
            }

            // wait-event reporting (pgstat_report_wait_start/_end) dropped.
            pg_usleep(s.cur_delay as c_long);

            // increase delay by a random fraction between 1X and 2X.
            ensure_prng_seeded();
            let frac = pg_prng_double(&raw mut pg_global_prng_state);
            s.cur_delay += (s.cur_delay as f64 * frac + 0.5) as c_int;
            // wrap back to minimum delay when max is exceeded.
            if (s.cur_delay as c_long) > MAX_DELAY_USEC {
                s.cur_delay = MIN_DELAY_USEC as c_int;
            }

            s.spins = 0;
        }
    }
}

/// `finish_spin_delay` - after acquiring, update spins_per_delay estimate.
///
/// Increase rapidly (+100) when we never had to delay (likely multiprocessor),
/// decrement slowly (-1) when we did (possible uniprocessor).
pub fn finish_spin_delay(status: *mut SpinDelayStatus) {
    unsafe {
        let s = &*status;
        let cur = spins_per_delay();
        if s.cur_delay == 0 {
            // we never had to delay
            if cur < MAX_SPINS_PER_DELAY {
                set_spins_per_delay_value(Min(cur + 100, MAX_SPINS_PER_DELAY));
            }
        } else if cur > MIN_SPINS_PER_DELAY {
            set_spins_per_delay_value(Max(cur - 1, MIN_SPINS_PER_DELAY));
        }
    }
}

/// Set local copy of spins_per_delay during backend startup.
pub fn set_spins_per_delay(shared_spins_per_delay: c_int) {
    set_spins_per_delay_value(shared_spins_per_delay);
}

/// Update shared estimate of spins_per_delay during backend exit (EMA, adaption
/// rate 1/16, truncating).
pub fn update_spins_per_delay(shared_spins_per_delay: c_int) -> c_int {
    (shared_spins_per_delay * 15 + spins_per_delay()) / 16
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests mutate process-global state (the spins_per_delay AtomicI32 and
    // the global PRNG consumed by perform_spin_delay); serialize them so cargo's
    // parallel test threads don't perturb each other's delay sequences.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // A SpinDelayStatus accumulates delays across several perform_spin_delay
    // calls and cur_delay grows then clamps -- staying well under NUM_DELAYS so
    // we never hit the s_lock_stuck PANIC.
    #[test]
    fn cur_delay_grows_then_clamps() {
        let _g = TEST_LOCK.lock().unwrap();
        // Force a block on every single spin so the test runs quickly.
        set_spins_per_delay(1);

        let mut status = make_spin_delay_status(null(), 0, null());

        // First delay: cur_delay is set 0 -> MIN_DELAY_USEC, then immediately
        // grown by the random 1X-2X step in the same call, so it ends >= MIN.
        perform_spin_delay(&mut status);
        assert_eq!(status.delays, 1);
        assert!(status.cur_delay >= MIN_DELAY_USEC as c_int);

        // Drive a bounded number of further delays; cur_delay stays >= MIN and,
        // because the grow happens before the >MAX check wraps it back to MIN,
        // it can transiently reach up to ~2X MAX_DELAY_USEC.
        let mut saw_growth = false;
        let mut saw_wrap = false;
        let mut prev = status.cur_delay;
        // perform_spin_delay does a REAL pg_usleep(cur_delay) and cur_delay grows
        // toward MAX_DELAY_USEC (~1s), so stop as soon as we've seen both a growth
        // and a wrap to keep the test fast (a full cycle to the first wrap is
        // ~17 delays; we never approach NUM_DELAYS).
        for _ in 0..200 {
            if saw_growth && saw_wrap {
                break;
            }
            perform_spin_delay(&mut status);
            assert!(status.cur_delay >= MIN_DELAY_USEC as c_int);
            assert!((status.cur_delay as c_long) <= 2 * MAX_DELAY_USEC);
            if status.cur_delay > prev {
                saw_growth = true;
            }
            if status.cur_delay < prev {
                // only way to decrease is the wrap-to-minimum path
                saw_wrap = true;
                assert_eq!(status.cur_delay, MIN_DELAY_USEC as c_int);
            }
            prev = status.cur_delay;
        }

        assert!(saw_growth, "cur_delay should have grown at least once");
        assert!(saw_wrap, "cur_delay should have wrapped to MIN at least once");
        // Far below the stuck threshold, so no PANIC.
        assert!(status.delays < NUM_DELAYS);

        // restore default for other tests
        set_spins_per_delay(DEFAULT_SPINS_PER_DELAY);
    }

    #[test]
    fn finish_tunes_spins_per_delay() {
        let _g = TEST_LOCK.lock().unwrap();
        // No delay incurred -> spins_per_delay jumps up by 100 (clamped).
        set_spins_per_delay(DEFAULT_SPINS_PER_DELAY);
        let no_delay = make_spin_delay_status(null(), 0, null());
        let before = spins_per_delay();
        finish_spin_delay(&no_delay as *const _ as *mut _);
        assert!(spins_per_delay() > before);
        assert!(spins_per_delay() <= MAX_SPINS_PER_DELAY);

        // Had a delay -> decremented by 1 (clamped at MIN).
        set_spins_per_delay(DEFAULT_SPINS_PER_DELAY);
        let mut delayed = make_spin_delay_status(null(), 0, null());
        delayed.cur_delay = MIN_DELAY_USEC as c_int;
        finish_spin_delay(&delayed as *const _ as *mut _);
        assert_eq!(spins_per_delay(), DEFAULT_SPINS_PER_DELAY - 1);

        set_spins_per_delay(DEFAULT_SPINS_PER_DELAY);
    }

    #[test]
    fn tas_acquires_then_blocks() {
        let _g = TEST_LOCK.lock().unwrap();
        let mut lock: slock_t = 0;
        assert_eq!(tas(&mut lock), 0, "first acquire succeeds");
        assert_ne!(tas(&mut lock), 0, "second acquire is contended");
        s_unlock(&mut lock);
        assert_eq!(tas(&mut lock), 0, "re-acquire after unlock succeeds");
    }
}

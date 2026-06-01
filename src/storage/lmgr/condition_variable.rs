//! storage/lmgr/condition_variable.c - Implementation of condition variables.
//!
//! Condition variables provide a way for one process to wait until a specific
//! condition occurs, without needing to know the specific identity of the
//! process for which they are waiting.  Waits for condition variables can be
//! interrupted, unlike LWLock waits.  Condition variables are safe to use
//! within dynamic shared memory segments.
//!
//! This module is the canonical home of the real `ConditionVariable` struct.

use crate::prelude::*;

use core::ffi::c_int;

use crate::miscadmin::{CHECK_FOR_INTERRUPTS, MyLatch};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_GET_MILLISEC, INSTR_TIME_SET_CURRENT, INSTR_TIME_SUBTRACT,
};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::procnumber::MyProcNumber;
use crate::storage::proclist::{
    proclist_contains_offset, proclist_delete_offset, proclist_is_empty,
    proclist_pop_head_node_offset, proclist_push_tail_offset,
};
use crate::storage::proclist_types::{proclist_head, proclist_node};
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};

// ---------------------------------------------------------------------------
// Real ConditionVariable definition (storage/condition_variable.h).
//
// This is the canonical definition; other modules carry opaque stubs of it.
// ---------------------------------------------------------------------------

/// `typedef struct ConditionVariable` (storage/condition_variable.h).
#[repr(C)]
pub struct ConditionVariable {
    pub mutex: slock_t,          /* spinlock that protects the wakeup list */
    pub wakeup: proclist_head,   /* list of waiting PGPROCs */
}

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported dependencies.
// ---------------------------------------------------------------------------

// storage/proc.h: PGPROC.  Not yet ported (storage::proc module does not yet
// exist).  We mirror the fields this module touches - procLatch and the
// cvWaitLink proclist_node - so that offsetof(PGPROC, cvWaitLink) is correct
// for the proclist helpers.  The leading padding stands in for the earlier
// PGPROC members; only the relative offsets of the fields we use matter here.
// TODO: dedup with storage/proc.rs once it lands.
#[repr(C)]
pub struct PGPROC {
    pub procLatch: Latch,
    pub cvWaitLink: proclist_node,
}

// storage/latch.h: Latch.  Not yet ported; modeled opaque as elsewhere.
// TODO: dedup once latch.c lands.
#[repr(C)]
pub struct Latch {
    _private: [u8; 0],
}

// utils/init/globals.c: MyProc - this backend's PGPROC entry.  TODO: import
// from storage/proc.rs once ported.
static mut MyProc: *mut PGPROC = null_mut();

// storage/latch.h flags.  TODO: import from ported latch.c.
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 2;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// storage/latch.h: WaitLatch / ResetLatch / SetLatch.  TODO: import from the
// ported latch.c.
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout: c_long,
    _wait_event_info: uint32,
) -> c_int {
    /* TODO: not ported */
    0
}

unsafe fn ResetLatch(_latch: *mut Latch) {
    /* TODO: not ported */
}

unsafe fn SetLatch(_latch: *mut Latch) {
    /* TODO: not ported */
}

// offsetof(PGPROC, cvWaitLink) for the proclist helpers below.
#[inline]
fn cvWaitLink_offset() -> Size {
    core::mem::offset_of!(PGPROC, cvWaitLink)
}

// ---------------------------------------------------------------------------

/* Initially, we are not prepared to sleep on any condition variable. */
static mut cv_sleep_target: *mut ConditionVariable = null_mut();

/*
 * Initialize a condition variable.
 */
pub unsafe fn ConditionVariableInit(cv: *mut ConditionVariable) {
    SpinLockInit(&mut (*cv).mutex);
    crate::storage::proclist::proclist_init(&mut (*cv).wakeup);
}

/*
 * Prepare to wait on a given condition variable.
 *
 * This can optionally be called before entering a test/sleep loop.
 * Doing so is more efficient if we'll need to sleep at least once.
 * However, if the first test of the exit condition is likely to succeed,
 * it's more efficient to omit the ConditionVariablePrepareToSleep call.
 * See comments in ConditionVariableSleep for more detail.
 *
 * Caution: "before entering the loop" means you *must* test the exit
 * condition between calling ConditionVariablePrepareToSleep and calling
 * ConditionVariableSleep.  If that is inconvenient, omit calling
 * ConditionVariablePrepareToSleep.
 */
pub unsafe fn ConditionVariablePrepareToSleep(cv: *mut ConditionVariable) {
    let pgprocno: c_int = MyProcNumber;

    /*
     * If some other sleep is already prepared, cancel it; this is necessary
     * because we have just one static variable tracking the prepared sleep,
     * and also only one cvWaitLink in our PGPROC.  It's okay to do this
     * because whenever control does return to the other test-and-sleep loop,
     * its ConditionVariableSleep call will just re-establish that sleep as
     * the prepared one.
     */
    if !cv_sleep_target.is_null() {
        ConditionVariableCancelSleep();
    }

    /* Record the condition variable on which we will sleep. */
    cv_sleep_target = cv;

    /* Add myself to the wait queue. */
    SpinLockAcquire(&mut (*cv).mutex);
    proclist_push_tail_offset(&mut (*cv).wakeup, pgprocno, cvWaitLink_offset());
    SpinLockRelease(&mut (*cv).mutex);
}

/*
 * Wait for the given condition variable to be signaled.
 *
 * This should be called in a predicate loop that tests for a specific exit
 * condition and otherwise sleeps, like so:
 *
 *	 ConditionVariablePrepareToSleep(cv);  // optional
 *	 while (condition for which we are waiting is not true)
 *		 ConditionVariableSleep(cv, wait_event_info);
 *	 ConditionVariableCancelSleep();
 *
 * wait_event_info should be a value from one of the WaitEventXXX enums
 * defined in pgstat.h.  This controls the contents of pg_stat_activity's
 * wait_event_type and wait_event columns while waiting.
 */
pub unsafe fn ConditionVariableSleep(cv: *mut ConditionVariable, wait_event_info: uint32) {
    let _ = ConditionVariableTimedSleep(cv, -1 /* no timeout */, wait_event_info);
}

/*
 * Wait for a condition variable to be signaled or a timeout to be reached.
 *
 * The "timeout" is given in milliseconds.
 *
 * Returns true when timeout expires, otherwise returns false.
 *
 * See ConditionVariableSleep() for general usage.
 */
pub unsafe fn ConditionVariableTimedSleep(
    cv: *mut ConditionVariable,
    timeout: c_long,
    wait_event_info: uint32,
) -> bool {
    let mut cur_timeout: c_long = -1;
    let mut start_time: instr_time = instr_time::default();
    let mut cur_time: instr_time = instr_time::default();
    let wait_events: c_int;

    /*
     * If the caller didn't prepare to sleep explicitly, then do so now and
     * return immediately.  The caller's predicate loop should immediately
     * call again if its exit condition is not yet met.  This will result in
     * the exit condition being tested twice before we first sleep.  The extra
     * test can be prevented by calling ConditionVariablePrepareToSleep(cv)
     * first.  Whether it's worth doing that depends on whether you expect the
     * exit condition to be met initially, in which case skipping the prepare
     * is recommended because it avoids manipulations of the wait list, or not
     * met initially, in which case preparing first is better because it
     * avoids one extra test of the exit condition.
     *
     * If we are currently prepared to sleep on some other CV, we just cancel
     * that and prepare this one; see ConditionVariablePrepareToSleep.
     */
    if cv_sleep_target != cv {
        ConditionVariablePrepareToSleep(cv);
        return false;
    }

    /*
     * Record the current time so that we can calculate the remaining timeout
     * if we are woken up spuriously.
     */
    if timeout >= 0 {
        INSTR_TIME_SET_CURRENT(&mut start_time);
        Assert!(timeout >= 0 && timeout <= c_int::MAX as c_long);
        cur_timeout = timeout;
        wait_events = WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH;
    } else {
        wait_events = WL_LATCH_SET | WL_EXIT_ON_PM_DEATH;
    }

    loop {
        let mut done = false;

        /*
         * Wait for latch to be set.  (If we're awakened for some other
         * reason, the code below will cope anyway.)
         */
        let _ = WaitLatch(MyLatch as *mut Latch, wait_events, cur_timeout, wait_event_info);

        /* Reset latch before examining the state of the wait list. */
        ResetLatch(MyLatch as *mut Latch);

        /*
         * If this process has been taken out of the wait list, then we know
         * that it has been signaled by ConditionVariableSignal (or
         * ConditionVariableBroadcast), so we should return to the caller. But
         * that doesn't guarantee that the exit condition is met, only that we
         * ought to check it.  So we must put the process back into the wait
         * list, to ensure we don't miss any additional wakeup occurring while
         * the caller checks its exit condition.  We can take ourselves out of
         * the wait list only when the caller calls
         * ConditionVariableCancelSleep.
         *
         * If we're still in the wait list, then the latch must have been set
         * by something other than ConditionVariableSignal; though we don't
         * guarantee not to return spuriously, we'll avoid this obvious case.
         */
        SpinLockAcquire(&mut (*cv).mutex);
        if !proclist_contains_offset(&(*cv).wakeup, MyProcNumber, cvWaitLink_offset()) {
            done = true;
            proclist_push_tail_offset(&mut (*cv).wakeup, MyProcNumber, cvWaitLink_offset());
        }
        SpinLockRelease(&mut (*cv).mutex);

        /*
         * Check for interrupts, and return spuriously if that caused the
         * current sleep target to change (meaning that interrupt handler code
         * waited for a different condition variable).
         */
        CHECK_FOR_INTERRUPTS();
        if cv != cv_sleep_target {
            done = true;
        }

        /* We were signaled, so return */
        if done {
            return false;
        }

        /* If we're not done, update cur_timeout for next iteration */
        if timeout >= 0 {
            INSTR_TIME_SET_CURRENT(&mut cur_time);
            INSTR_TIME_SUBTRACT(&mut cur_time, start_time);
            cur_timeout = timeout - INSTR_TIME_GET_MILLISEC(cur_time) as c_long;

            /* Have we crossed the timeout threshold? */
            if cur_timeout <= 0 {
                return true;
            }
        }
    }
}

/*
 * Cancel any pending sleep operation.
 *
 * We just need to remove ourselves from the wait queue of any condition
 * variable for which we have previously prepared a sleep.
 *
 * Do nothing if nothing is pending; this allows this function to be called
 * during transaction abort to clean up any unfinished CV sleep.
 *
 * Return true if we've been signaled.
 */
pub unsafe fn ConditionVariableCancelSleep() -> bool {
    let cv: *mut ConditionVariable = cv_sleep_target;
    let mut signaled = false;

    if cv.is_null() {
        return false;
    }

    SpinLockAcquire(&mut (*cv).mutex);
    if proclist_contains_offset(&(*cv).wakeup, MyProcNumber, cvWaitLink_offset()) {
        proclist_delete_offset(&mut (*cv).wakeup, MyProcNumber, cvWaitLink_offset());
    } else {
        signaled = true;
    }
    SpinLockRelease(&mut (*cv).mutex);

    cv_sleep_target = null_mut();

    signaled
}

/*
 * Wake up the oldest process sleeping on the CV, if there is any.
 *
 * Note: it's difficult to tell whether this has any real effect: we know
 * whether we took an entry off the list, but the entry might only be a
 * sentinel.  Hence, think twice before proposing that this should return
 * a flag telling whether it woke somebody.
 */
pub unsafe fn ConditionVariableSignal(cv: *mut ConditionVariable) {
    let mut proc: *mut PGPROC = null_mut();

    /* Remove the first process from the wakeup queue (if any). */
    SpinLockAcquire(&mut (*cv).mutex);
    if !proclist_is_empty(&(*cv).wakeup) {
        proc = proclist_pop_head_node_offset(&mut (*cv).wakeup, cvWaitLink_offset()) as *mut PGPROC;
    }
    SpinLockRelease(&mut (*cv).mutex);

    /* If we found someone sleeping, set their latch to wake them up. */
    if !proc.is_null() {
        SetLatch(&mut (*proc).procLatch);
    }
}

/*
 * Wake up all processes sleeping on the given CV.
 *
 * This guarantees to wake all processes that were sleeping on the CV
 * at time of call, but processes that add themselves to the list mid-call
 * will typically not get awakened.
 */
pub unsafe fn ConditionVariableBroadcast(cv: *mut ConditionVariable) {
    let pgprocno: c_int = MyProcNumber;
    let mut proc: *mut PGPROC = null_mut();
    let mut have_sentinel = false;

    /*
     * In some use-cases, it is common for awakened processes to immediately
     * re-queue themselves.  If we just naively try to reduce the wakeup list
     * to empty, we'll get into a potentially-indefinite loop against such a
     * process.  The semantics we really want are just to be sure that we have
     * wakened all processes that were in the list at entry.  We can use our
     * own cvWaitLink as a sentinel to detect when we've finished.
     *
     * A seeming flaw in this approach is that someone else might signal the
     * CV and in doing so remove our sentinel entry.  But that's fine: since
     * CV waiters are always added and removed in order, that must mean that
     * every previous waiter has been wakened, so we're done.  We'll get an
     * extra "set" on our latch from the someone else's signal, which is
     * slightly inefficient but harmless.
     *
     * We can't insert our cvWaitLink as a sentinel if it's already in use in
     * some other proclist.  While that's not expected to be true for typical
     * uses of this function, we can deal with it by simply canceling any
     * prepared CV sleep.  The next call to ConditionVariableSleep will take
     * care of re-establishing the lost state.
     */
    if !cv_sleep_target.is_null() {
        ConditionVariableCancelSleep();
    }

    /*
     * Inspect the state of the queue.  If it's empty, we have nothing to do.
     * If there's exactly one entry, we need only remove and signal that
     * entry.  Otherwise, remove the first entry and insert our sentinel.
     */
    SpinLockAcquire(&mut (*cv).mutex);
    /* While we're here, let's assert we're not in the list. */
    Assert!(!proclist_contains_offset(&(*cv).wakeup, pgprocno, cvWaitLink_offset()));

    if !proclist_is_empty(&(*cv).wakeup) {
        proc = proclist_pop_head_node_offset(&mut (*cv).wakeup, cvWaitLink_offset()) as *mut PGPROC;
        if !proclist_is_empty(&(*cv).wakeup) {
            proclist_push_tail_offset(&mut (*cv).wakeup, pgprocno, cvWaitLink_offset());
            have_sentinel = true;
        }
    }
    SpinLockRelease(&mut (*cv).mutex);

    /* Awaken first waiter, if there was one. */
    if !proc.is_null() {
        SetLatch(&mut (*proc).procLatch);
    }

    while have_sentinel {
        /*
         * Each time through the loop, remove the first wakeup list entry, and
         * signal it unless it's our sentinel.  Repeat as long as the sentinel
         * remains in the list.
         *
         * Notice that if someone else removes our sentinel, we will waken one
         * additional process before exiting.  That's intentional, because if
         * someone else signals the CV, they may be intending to waken some
         * third process that added itself to the list after we added the
         * sentinel.  Better to give a spurious wakeup (which should be
         * harmless beyond wasting some cycles) than to lose a wakeup.
         */
        proc = null_mut();
        SpinLockAcquire(&mut (*cv).mutex);
        if !proclist_is_empty(&(*cv).wakeup) {
            proc = proclist_pop_head_node_offset(&mut (*cv).wakeup, cvWaitLink_offset())
                as *mut PGPROC;
        }
        have_sentinel = proclist_contains_offset(&(*cv).wakeup, pgprocno, cvWaitLink_offset());
        SpinLockRelease(&mut (*cv).mutex);

        if !proc.is_null() && proc != MyProc {
            SetLatch(&mut (*proc).procLatch);
        }
    }
}

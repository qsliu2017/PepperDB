//! src/backend/utils/misc/timeout.c
//!
//! timeout.c
//!   Routines to multiplex SIGALRM interrupts for multiple timeout reasons.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/misc/timeout.c

use crate::prelude::*;

use std::ffi::c_int;

use crate::miscadmin::TimestampTz;

// ====================================================================
// From src/include/utils/timeout.h
// ====================================================================

/*
 * Identifiers for timeout reasons.  Note that in case multiple timeouts
 * trigger at the same time, they are serviced in the order of this enum.
 */
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub enum TimeoutId {
    /* Predefined timeout reasons */
    STARTUP_PACKET_TIMEOUT = 0,
    DEADLOCK_TIMEOUT,
    LOCK_TIMEOUT,
    STATEMENT_TIMEOUT,
    STANDBY_DEADLOCK_TIMEOUT,
    STANDBY_TIMEOUT,
    STANDBY_LOCK_TIMEOUT,
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    TRANSACTION_TIMEOUT,
    IDLE_SESSION_TIMEOUT,
    IDLE_STATS_UPDATE_TIMEOUT,
    CLIENT_CONNECTION_CHECK_TIMEOUT,
    STARTUP_PROGRESS_TIMEOUT,
    /* First user-definable timeout reason */
    USER_TIMEOUT,
    /* Maximum number of timeout reasons */
    // MAX_TIMEOUTS = USER_TIMEOUT + 10
}

pub use TimeoutId::*;

/* USER_TIMEOUT is value 13 in the enum above. */
pub const USER_TIMEOUT_VAL: i32 = TimeoutId::USER_TIMEOUT as i32;
/* MAX_TIMEOUTS = USER_TIMEOUT + 10 */
pub const MAX_TIMEOUTS: usize = (USER_TIMEOUT_VAL + 10) as usize;

/// callback function signature
/// typedef void (*timeout_handler_proc) (void);
pub type timeout_handler_proc = unsafe extern "C" fn();

/*
 * Parameter structure for setting multiple timeouts at once
 */
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum TimeoutType {
    TMPARAM_AFTER,
    TMPARAM_AT,
    TMPARAM_EVERY,
}

pub use TimeoutType::*;

#[repr(C)]
pub struct EnableTimeoutParams {
    pub id: TimeoutId,            /* timeout to set */
    pub r#type: TimeoutType,     /* TMPARAM_AFTER or TMPARAM_AT */
    pub delay_ms: c_int,         /* only used for TMPARAM_AFTER/EVERY */
    pub fin_time: TimestampTz,   /* only used for TMPARAM_AT */
}

/*
 * Parameter structure for clearing multiple timeouts at once
 */
#[repr(C)]
pub struct DisableTimeoutParams {
    pub id: TimeoutId,           /* timeout to clear */
    pub keep_indicator: bool,    /* keep the indicator flag? */
}

// ====================================================================
// From src/backend/utils/misc/timeout.c
// ====================================================================

/* Data about any one timeout reason */
#[repr(C)]
struct timeout_params {
    index: TimeoutId, /* identifier of timeout reason */

    /* volatile because these may be changed from the signal handler */
    active: bool,    /* true if timeout is in active_timeouts[] */
    indicator: bool, /* true if timeout has occurred */

    /* callback function for timeout, or NULL if timeout not registered */
    timeout_handler: Option<timeout_handler_proc>,

    start_time: TimestampTz,  /* time that timeout was last activated */
    fin_time: TimestampTz,    /* time it is, or was last, due to fire */
    interval_in_ms: c_int,    /* time between firings, or 0 if just once */
}

impl timeout_params {
    const fn new() -> Self {
        timeout_params {
            index: TimeoutId::STARTUP_PACKET_TIMEOUT,
            active: false,
            indicator: false,
            timeout_handler: None,
            start_time: 0,
            fin_time: 0,
            interval_in_ms: 0,
        }
    }
}

/*
 * List of possible timeout reasons in the order of enum TimeoutId.
 */
static mut all_timeouts: [timeout_params; MAX_TIMEOUTS] =
    [const { timeout_params::new() }; MAX_TIMEOUTS];
static mut all_timeouts_initialized: bool = false;

/*
 * List of active timeouts ordered by their fin_time and priority.
 * This list is subject to change by the interrupt handler, so it's volatile.
 */
static mut num_active_timeouts: c_int = 0;
static mut active_timeouts: [*mut timeout_params; MAX_TIMEOUTS] =
    [core::ptr::null_mut(); MAX_TIMEOUTS];

/*
 * Flag controlling whether the signal handler is allowed to do anything.
 * This is useful to avoid race conditions with the handler.  Note in
 * particular that this lets us make changes in the data structures without
 * tediously disabling and re-enabling the timer signal.  Most of the time,
 * no interrupt would happen anyway during such critical sections, but if
 * one does, this rule ensures it's safe.  Leaving the signal enabled across
 * multiple operations can greatly reduce the number of kernel calls we make,
 * too.  See comments in schedule_alarm() about that.
 *
 * We leave this "false" when we're not expecting interrupts, just in case.
 */
static mut alarm_enabled: sig_atomic_t = 0; /* false */

#[inline]
unsafe fn disable_alarm() {
    alarm_enabled = 0; /* false */
}

#[inline]
unsafe fn enable_alarm() {
    alarm_enabled = 1; /* true */
}

/*
 * State recording if and when we next expect the interrupt to fire.
 * (signal_due_at is valid only when signal_pending is true.)
 * Note that the signal handler will unconditionally reset signal_pending to
 * false, so that can change asynchronously even when alarm_enabled is false.
 */
static mut signal_pending: sig_atomic_t = 0; /* false */
static mut signal_due_at: TimestampTz = 0;

/* sig_atomic_t equivalent */
type sig_atomic_t = c_int;

/*****************************************************************************
 * Internal helper functions
 *
 * For all of these, it is caller's responsibility to protect them from
 * interruption by the signal handler.  Generally, call disable_alarm()
 * first to prevent interruption, then update state, and last call
 * schedule_alarm(), which will re-enable the signal handler if needed.
 *****************************************************************************/

/*
 * Find the index of a given timeout reason in the active array.
 * If it's not there, return -1.
 */
unsafe fn find_active_timeout(id: TimeoutId) -> c_int {
    let mut i: c_int = 0;

    while i < num_active_timeouts {
        if (*active_timeouts[i as usize]).index == id {
            return i;
        }
        i += 1;
    }

    -1
}

/*
 * Insert specified timeout reason into the list of active timeouts
 * at the given index.
 */
unsafe fn insert_timeout(id: TimeoutId, index: c_int) {
    if index < 0 || index > num_active_timeouts {
        elog!(
            FATAL,
            "timeout index {} out of range 0..{}",
            index,
            num_active_timeouts
        );
    }

    Assert!(!all_timeouts[id as usize].active);
    all_timeouts[id as usize].active = true;

    let mut i: c_int = num_active_timeouts - 1;
    while i >= index {
        active_timeouts[(i + 1) as usize] = active_timeouts[i as usize];
        i -= 1;
    }

    active_timeouts[index as usize] = &mut all_timeouts[id as usize];

    num_active_timeouts += 1;
}

/*
 * Remove the index'th element from the timeout list.
 */
unsafe fn remove_timeout_index(index: c_int) {
    if index < 0 || index >= num_active_timeouts {
        elog!(
            FATAL,
            "timeout index {} out of range 0..{}",
            index,
            num_active_timeouts - 1
        );
    }

    Assert!((*active_timeouts[index as usize]).active);
    (*active_timeouts[index as usize]).active = false;

    let mut i: c_int = index + 1;
    while i < num_active_timeouts {
        active_timeouts[(i - 1) as usize] = active_timeouts[i as usize];
        i += 1;
    }

    num_active_timeouts -= 1;
}

/*
 * Enable the specified timeout reason
 */
unsafe fn enable_timeout(
    id: TimeoutId,
    now: TimestampTz,
    fin_time: TimestampTz,
    interval_in_ms: c_int,
) {
    /* Assert request is sane */
    Assert!(all_timeouts_initialized);
    Assert!(all_timeouts[id as usize].timeout_handler.is_some());

    /*
     * If this timeout was already active, momentarily disable it.  We
     * interpret the call as a directive to reschedule the timeout.
     */
    if all_timeouts[id as usize].active {
        remove_timeout_index(find_active_timeout(id));
    }

    /*
     * Find out the index where to insert the new timeout.  We sort by
     * fin_time, and for equal fin_time by priority.
     */
    let mut i: c_int = 0;
    while i < num_active_timeouts {
        let old_timeout = active_timeouts[i as usize];

        if fin_time < (*old_timeout).fin_time {
            break;
        }
        if fin_time == (*old_timeout).fin_time && id < (*old_timeout).index {
            break;
        }
        i += 1;
    }

    /*
     * Mark the timeout active, and insert it into the active list.
     */
    all_timeouts[id as usize].indicator = false;
    all_timeouts[id as usize].start_time = now;
    all_timeouts[id as usize].fin_time = fin_time;
    all_timeouts[id as usize].interval_in_ms = interval_in_ms;

    insert_timeout(id, i);
}

/*
 * Schedule alarm for the next active timeout, if any
 *
 * We assume the caller has obtained the current time, or a close-enough
 * approximation.  (It's okay if a tick or two has passed since "now", or
 * if a little more time elapses before we reach the kernel call; that will
 * cause us to ask for an interrupt a tick or two later than the nearest
 * timeout, which is no big deal.  Passing a "now" value that's in the future
 * would be bad though.)
 */
unsafe fn schedule_alarm(now: TimestampTz) {
    if num_active_timeouts > 0 {
        let mut timeval: itimerval = core::mem::zeroed();
        let nearest_timeout: TimestampTz;
        let mut secs: i64 = 0;
        let mut usecs: c_int = 0;

        MemSet(
            &mut timeval as *mut itimerval as *mut _,
            0,
            core::mem::size_of::<itimerval>(),
        );

        /*
         * If we think there's a signal pending, but current time is more than
         * 10ms past when the signal was due, then assume that the timeout
         * request got lost somehow; clear signal_pending so that we'll reset
         * the interrupt request below.  (10ms corresponds to the worst-case
         * timeout granularity on modern systems.)  It won't hurt us if the
         * interrupt does manage to fire between now and when we reach the
         * setitimer() call.
         */
        if signal_pending != 0 && now > signal_due_at + 10 * 1000 {
            signal_pending = 0; /* false */
        }

        /*
         * Get the time remaining till the nearest pending timeout.  If it is
         * negative, assume that we somehow missed an interrupt, and clear
         * signal_pending.  This gives us another chance to recover if the
         * kernel drops a timeout request for some reason.
         */
        nearest_timeout = (*active_timeouts[0]).fin_time;
        if now > nearest_timeout {
            signal_pending = 0; /* false */
            /* force an interrupt as soon as possible */
            secs = 0;
            usecs = 1;
        } else {
            TimestampDifference(now, nearest_timeout, &mut secs, &mut usecs);

            /*
             * It's possible that the difference is less than a microsecond;
             * ensure we don't cancel, rather than set, the interrupt.
             */
            if secs == 0 && usecs == 0 {
                usecs = 1;
            }
        }

        timeval.it_value.tv_sec = secs as _;
        timeval.it_value.tv_usec = usecs as _;

        /*
         * We must enable the signal handler before calling setitimer(); if we
         * did it in the other order, we'd have a race condition wherein the
         * interrupt could occur before we can set alarm_enabled, so that the
         * signal handler would fail to do anything.
         *
         * Because we didn't bother to disable the timer in disable_alarm(),
         * it's possible that a previously-set interrupt will fire between
         * enable_alarm() and setitimer().  This is safe, however.  There are
         * two possible outcomes:
         *
         * 1. The signal handler finds nothing to do (because the nearest
         * timeout event is still in the future).  It will re-set the timer
         * and return.  Then we'll overwrite the timer value with a new one.
         * This will mean that the timer fires a little later than we
         * intended, but only by the amount of time it takes for the signal
         * handler to do nothing useful, which shouldn't be much.
         *
         * 2. The signal handler executes and removes one or more timeout
         * events.  When it returns, either the queue is now empty or the
         * frontmost event is later than the one we looked at above.  So we'll
         * overwrite the timer value with one that is too soon (plus or minus
         * the signal handler's execution time), causing a useless interrupt
         * to occur.  But the handler will then re-set the timer and
         * everything will still work as expected.
         *
         * Since these cases are of very low probability (the window here
         * being quite narrow), it's not worth adding cycles to the mainline
         * code to prevent occasional wasted interrupts.
         */
        enable_alarm();

        /*
         * If there is already an interrupt pending that's at or before the
         * needed time, we need not do anything more.  The signal handler will
         * do the right thing in the first case, and re-schedule the interrupt
         * for later in the second case.  It might seem that the extra
         * interrupt is wasted work, but it's not terribly much work, and this
         * method has very significant advantages in the common use-case where
         * we repeatedly set a timeout that we don't expect to reach and then
         * cancel it.  Instead of invoking setitimer() every time the timeout
         * is set or canceled, we perform one interrupt and a re-scheduling
         * setitimer() call at intervals roughly equal to the timeout delay.
         * For example, with statement_timeout = 1s and a throughput of
         * thousands of queries per second, this method requires an interrupt
         * and setitimer() call roughly once a second, rather than thousands
         * of setitimer() calls per second.
         *
         * Because of the possible passage of time between when we obtained
         * "now" and when we reach setitimer(), the kernel's opinion of when
         * to trigger the interrupt is likely to be a bit later than
         * signal_due_at.  That's fine, for the same reasons described above.
         */
        if signal_pending != 0 && nearest_timeout >= signal_due_at {
            return;
        }

        /*
         * As with calling enable_alarm(), we must set signal_pending *before*
         * calling setitimer(); if we did it after, the signal handler could
         * trigger before we set it, leaving us with a false opinion that a
         * signal is still coming.
         *
         * Other race conditions involved with setting/checking signal_pending
         * are okay, for the reasons described above.  One additional point is
         * that the signal handler could fire after we set signal_due_at, but
         * still before the setitimer() call.  Then the handler could
         * overwrite signal_due_at with a value it computes, which will be the
         * same as or perhaps later than what we just computed.  After we
         * perform setitimer(), the net effect would be that signal_due_at
         * gives a time later than when the interrupt will really happen;
         * which is a safe situation.
         */
        signal_due_at = nearest_timeout;
        signal_pending = 1; /* true */

        /* Set the alarm timer */
        if setitimer(ITIMER_REAL, &timeval, core::ptr::null_mut()) != 0 {
            /*
             * Clearing signal_pending here is a bit pro forma, but not
             * entirely so, since something in the FATAL exit path could try
             * to use timeout facilities.
             */
            signal_pending = 0; /* false */
            elog!(FATAL, "could not enable SIGALRM timer: %m");
        }
    }
}

/*****************************************************************************
 * Signal handler
 *****************************************************************************/

/*
 * Signal handler for SIGALRM
 *
 * Process any active timeout reasons and then reschedule the interrupt
 * as needed.
 */
unsafe extern "C" fn handle_sig_alarm(_postgres_signal_arg: c_int) {
    /*
     * Bump the holdoff counter, to make sure nothing we call will process
     * interrupts directly. No timeout handler should do that, but these
     * failures are hard to debug, so better be sure.
     */
    HOLD_INTERRUPTS();

    /*
     * SIGALRM is always cause for waking anything waiting on the process
     * latch.
     */
    SetLatch(MyLatch);

    /*
     * Always reset signal_pending, even if !alarm_enabled, since indeed no
     * signal is now pending.
     */
    signal_pending = 0; /* false */

    /*
     * Fire any pending timeouts, but only if we're enabled to do so.
     */
    if alarm_enabled != 0 {
        /*
         * Disable alarms, just in case this platform allows signal handlers
         * to interrupt themselves.  schedule_alarm() will re-enable if
         * appropriate.
         */
        disable_alarm();

        if num_active_timeouts > 0 {
            let mut now: TimestampTz = GetCurrentTimestamp();

            /* While the first pending timeout has been reached ... */
            while num_active_timeouts > 0 && now >= (*active_timeouts[0]).fin_time {
                let this_timeout = active_timeouts[0];

                /* Remove it from the active list */
                remove_timeout_index(0);

                /* Mark it as fired */
                (*this_timeout).indicator = true;

                /* And call its handler function */
                ((*this_timeout).timeout_handler.unwrap())();

                /* If it should fire repeatedly, re-enable it. */
                if (*this_timeout).interval_in_ms > 0 {
                    let mut new_fin_time: TimestampTz;

                    /*
                     * To guard against drift, schedule the next instance of
                     * the timeout based on the intended firing time rather
                     * than the actual firing time. But if the timeout was so
                     * late that we missed an entire cycle, fall back to
                     * scheduling based on the actual firing time.
                     */
                    new_fin_time = TimestampTzPlusMilliseconds(
                        (*this_timeout).fin_time,
                        (*this_timeout).interval_in_ms as i64,
                    );
                    if new_fin_time < now {
                        new_fin_time = TimestampTzPlusMilliseconds(
                            now,
                            (*this_timeout).interval_in_ms as i64,
                        );
                    }
                    enable_timeout(
                        (*this_timeout).index,
                        now,
                        new_fin_time,
                        (*this_timeout).interval_in_ms,
                    );
                }

                /*
                 * The handler might not take negligible time (CheckDeadLock
                 * for instance isn't too cheap), so let's update our idea of
                 * "now" after each one.
                 */
                now = GetCurrentTimestamp();
            }

            /* Done firing timeouts, so reschedule next interrupt if any */
            schedule_alarm(now);
        }
    }

    RESUME_INTERRUPTS();
}

/*****************************************************************************
 * Public API
 *****************************************************************************/

/*
 * Initialize timeout module.
 *
 * This must be called in every process that wants to use timeouts.
 *
 * If the process was forked from another one that was also using this
 * module, be sure to call this before re-enabling signals; else handlers
 * meant to run in the parent process might get invoked in this one.
 */
pub unsafe fn InitializeTimeouts() {
    /* Initialize, or re-initialize, all local state */
    disable_alarm();

    num_active_timeouts = 0;

    for i in 0..MAX_TIMEOUTS {
        all_timeouts[i].index = timeout_id_from_int(i as c_int);
        all_timeouts[i].active = false;
        all_timeouts[i].indicator = false;
        all_timeouts[i].timeout_handler = None;
        all_timeouts[i].start_time = 0;
        all_timeouts[i].fin_time = 0;
        all_timeouts[i].interval_in_ms = 0;
    }

    all_timeouts_initialized = true;

    /* Now establish the signal handler */
    pqsignal(SIGALRM, handle_sig_alarm);
}

/*
 * Register a timeout reason
 *
 * For predefined timeouts, this just registers the callback function.
 *
 * For user-defined timeouts, pass id == USER_TIMEOUT; we then allocate and
 * return a timeout ID.
 */
pub unsafe fn RegisterTimeout(mut id: TimeoutId, handler: timeout_handler_proc) -> TimeoutId {
    Assert!(all_timeouts_initialized);

    /* There's no need to disable the signal handler here. */

    if (id as i32) >= USER_TIMEOUT_VAL {
        /* Allocate a user-defined timeout reason */
        let mut idx = USER_TIMEOUT_VAL;
        while (idx as usize) < MAX_TIMEOUTS {
            if all_timeouts[idx as usize].timeout_handler.is_none() {
                break;
            }
            idx += 1;
        }
        if idx as usize >= MAX_TIMEOUTS {
            ereport!(FATAL, "cannot add more timeout reasons");
            unreachable!();
        }
        id = timeout_id_from_int(idx);
    }

    Assert!(all_timeouts[id as usize].timeout_handler.is_none());

    all_timeouts[id as usize].timeout_handler = Some(handler);

    id
}

/*
 * Reschedule any pending SIGALRM interrupt.
 *
 * This can be used during error recovery in case query cancel resulted in loss
 * of a SIGALRM event (due to longjmp'ing out of handle_sig_alarm before it
 * could do anything).  But note it's not necessary if any of the public
 * enable_ or disable_timeout functions are called in the same area, since
 * those all do schedule_alarm() internally if needed.
 */
pub unsafe fn reschedule_timeouts() {
    /* For flexibility, allow this to be called before we're initialized. */
    if !all_timeouts_initialized {
        return;
    }

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Reschedule the interrupt, if any timeouts remain active. */
    if num_active_timeouts > 0 {
        schedule_alarm(GetCurrentTimestamp());
    }
}

/*
 * Enable the specified timeout to fire after the specified delay.
 *
 * Delay is given in milliseconds.
 */
pub unsafe fn enable_timeout_after(id: TimeoutId, delay_ms: c_int) {
    let now: TimestampTz;
    let fin_time: TimestampTz;

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Queue the timeout at the appropriate time. */
    now = GetCurrentTimestamp();
    fin_time = TimestampTzPlusMilliseconds(now, delay_ms as i64);
    enable_timeout(id, now, fin_time, 0);

    /* Set the timer interrupt. */
    schedule_alarm(now);
}

/*
 * Enable the specified timeout to fire periodically, with the specified
 * delay as the time between firings.
 *
 * Delay is given in milliseconds.
 */
pub unsafe fn enable_timeout_every(id: TimeoutId, fin_time: TimestampTz, delay_ms: c_int) {
    let now: TimestampTz;

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Queue the timeout at the appropriate time. */
    now = GetCurrentTimestamp();
    enable_timeout(id, now, fin_time, delay_ms);

    /* Set the timer interrupt. */
    schedule_alarm(now);
}

/*
 * Enable the specified timeout to fire at the specified time.
 *
 * This is provided to support cases where there's a reason to calculate
 * the timeout by reference to some point other than "now".  If there isn't,
 * use enable_timeout_after(), to avoid calling GetCurrentTimestamp() twice.
 */
pub unsafe fn enable_timeout_at(id: TimeoutId, fin_time: TimestampTz) {
    let now: TimestampTz;

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Queue the timeout at the appropriate time. */
    now = GetCurrentTimestamp();
    enable_timeout(id, now, fin_time, 0);

    /* Set the timer interrupt. */
    schedule_alarm(now);
}

/*
 * Enable multiple timeouts at once.
 *
 * This works like calling enable_timeout_after() and/or enable_timeout_at()
 * multiple times.  Use this to reduce the number of GetCurrentTimestamp()
 * and setitimer() calls needed to establish multiple timeouts.
 */
pub unsafe fn enable_timeouts(timeouts: *const EnableTimeoutParams, count: c_int) {
    let now: TimestampTz;

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Queue the timeout(s) at the appropriate times. */
    now = GetCurrentTimestamp();

    let mut i: c_int = 0;
    while i < count {
        let t = &*timeouts.add(i as usize);
        let id = t.id;
        let fin_time: TimestampTz;

        match t.r#type {
            TimeoutType::TMPARAM_AFTER => {
                fin_time = TimestampTzPlusMilliseconds(now, t.delay_ms as i64);
                enable_timeout(id, now, fin_time, 0);
            }

            TimeoutType::TMPARAM_AT => {
                enable_timeout(id, now, t.fin_time, 0);
            }

            TimeoutType::TMPARAM_EVERY => {
                fin_time = TimestampTzPlusMilliseconds(now, t.delay_ms as i64);
                enable_timeout(id, now, fin_time, t.delay_ms);
            }
        }
        i += 1;
    }

    /* Set the timer interrupt. */
    schedule_alarm(now);
}

/*
 * Cancel the specified timeout.
 *
 * The timeout's I've-been-fired indicator is reset,
 * unless keep_indicator is true.
 *
 * When a timeout is canceled, any other active timeout remains in force.
 * It's not an error to disable a timeout that is not enabled.
 */
pub unsafe fn disable_timeout(id: TimeoutId, keep_indicator: bool) {
    /* Assert request is sane */
    Assert!(all_timeouts_initialized);
    Assert!(all_timeouts[id as usize].timeout_handler.is_some());

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Find the timeout and remove it from the active list. */
    if all_timeouts[id as usize].active {
        remove_timeout_index(find_active_timeout(id));
    }

    /* Mark it inactive, whether it was active or not. */
    if !keep_indicator {
        all_timeouts[id as usize].indicator = false;
    }

    /* Reschedule the interrupt, if any timeouts remain active. */
    if num_active_timeouts > 0 {
        schedule_alarm(GetCurrentTimestamp());
    }
}

/*
 * Cancel multiple timeouts at once.
 *
 * The timeouts' I've-been-fired indicators are reset,
 * unless timeouts[i].keep_indicator is true.
 *
 * This works like calling disable_timeout() multiple times.
 * Use this to reduce the number of GetCurrentTimestamp()
 * and setitimer() calls needed to cancel multiple timeouts.
 */
pub unsafe fn disable_timeouts(timeouts: *const DisableTimeoutParams, count: c_int) {
    Assert!(all_timeouts_initialized);

    /* Disable timeout interrupts for safety. */
    disable_alarm();

    /* Cancel the timeout(s). */
    let mut i: c_int = 0;
    while i < count {
        let t = &*timeouts.add(i as usize);
        let id = t.id;

        Assert!(all_timeouts[id as usize].timeout_handler.is_some());

        if all_timeouts[id as usize].active {
            remove_timeout_index(find_active_timeout(id));
        }

        if !t.keep_indicator {
            all_timeouts[id as usize].indicator = false;
        }
        i += 1;
    }

    /* Reschedule the interrupt, if any timeouts remain active. */
    if num_active_timeouts > 0 {
        schedule_alarm(GetCurrentTimestamp());
    }
}

/*
 * Disable the signal handler, remove all timeouts from the active list,
 * and optionally reset their timeout indicators.
 */
pub unsafe fn disable_all_timeouts(keep_indicators: bool) {
    disable_alarm();

    /*
     * We used to disable the timer interrupt here, but in common usage
     * patterns it's cheaper to leave it enabled; that may save us from having
     * to enable it again shortly.  See comments in schedule_alarm().
     */

    num_active_timeouts = 0;

    for i in 0..MAX_TIMEOUTS {
        all_timeouts[i].active = false;
        if !keep_indicators {
            all_timeouts[i].indicator = false;
        }
    }
}

/*
 * Return true if the timeout is active (enabled and not yet fired)
 *
 * This is, of course, subject to race conditions, as the timeout could fire
 * immediately after we look.
 */
pub unsafe fn get_timeout_active(id: TimeoutId) -> bool {
    all_timeouts[id as usize].active
}

/*
 * Return the timeout's I've-been-fired indicator
 *
 * If reset_indicator is true, reset the indicator when returning true.
 * To avoid missing timeouts due to race conditions, we are careful not to
 * reset the indicator when returning false.
 */
pub unsafe fn get_timeout_indicator(id: TimeoutId, reset_indicator: bool) -> bool {
    if all_timeouts[id as usize].indicator {
        if reset_indicator {
            all_timeouts[id as usize].indicator = false;
        }
        return true;
    }
    false
}

/*
 * Return the time when the timeout was most recently activated
 *
 * Note: will return 0 if timeout has never been activated in this process.
 * However, we do *not* reset the start_time when a timeout occurs, so as
 * not to create a race condition if SIGALRM fires just as some code is
 * about to fetch the value.
 */
pub unsafe fn get_timeout_start_time(id: TimeoutId) -> TimestampTz {
    all_timeouts[id as usize].start_time
}

/*
 * Return the time when the timeout is, or most recently was, due to fire
 *
 * Note: will return 0 if timeout has never been activated in this process.
 * However, we do *not* reset the fin_time when a timeout occurs, so as
 * not to create a race condition if SIGALRM fires just as some code is
 * about to fetch the value.
 */
pub unsafe fn get_timeout_finish_time(id: TimeoutId) -> TimestampTz {
    all_timeouts[id as usize].fin_time
}

// ====================================================================
// Local helpers / stubs for unported dependencies
// ====================================================================

/* Map an integer to a TimeoutId (the enum is contiguous 0..MAX_TIMEOUTS). */
#[inline]
fn timeout_id_from_int(i: c_int) -> TimeoutId {
    unsafe { core::mem::transmute::<u32, TimeoutId>(i as u32) }
}

/* struct itimerval / setitimer from <sys/time.h> */
#[repr(C)]
struct timeval_t {
    tv_sec: i64,
    tv_usec: i64,
}

#[repr(C)]
struct itimerval {
    it_interval: timeval_t,
    it_value: timeval_t,
}

const ITIMER_REAL: c_int = 0;
const SIGALRM: c_int = 14;

extern "C" {
    fn setitimer(which: c_int, new_value: *const itimerval, old_value: *mut itimerval) -> c_int;
}

unsafe fn pqsignal(_signo: c_int, _func: unsafe extern "C" fn(c_int)) {
    unimplemented!() // TODO: src/port/pqsignal.c
}

unsafe fn HOLD_INTERRUPTS() {
    unimplemented!() // TODO: src/include/miscadmin.h
}

unsafe fn RESUME_INTERRUPTS() {
    unimplemented!() // TODO: src/include/miscadmin.h
}

unsafe fn SetLatch(_latch: *mut Latch) {
    unimplemented!() // TODO: src/backend/storage/ipc/latch.c
}

#[allow(non_upper_case_globals)]
static mut MyLatch: *mut Latch = core::ptr::null_mut();

struct Latch;

unsafe fn GetCurrentTimestamp() -> TimestampTz {
    unimplemented!() // TODO: src/backend/utils/adt/timestamp.c
}

unsafe fn TimestampDifference(
    _start_time: TimestampTz,
    _stop_time: TimestampTz,
    _secs: *mut i64,
    _microsecs: *mut c_int,
) {
    unimplemented!() // TODO: src/backend/utils/adt/timestamp.c
}

#[inline]
unsafe fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: i64) -> TimestampTz {
    /* #define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) * (int64) 1000)) */
    tz + ms * 1000
}

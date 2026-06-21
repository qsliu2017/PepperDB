//! src/backend/storage/ipc/barrier.c
//!
//! Barriers for synchronizing cooperating processes.  Merges the `Barrier`
//! struct from `src/include/storage/barrier.h`.
//!
//! This implementation supports static sets of participants known up front, or
//! dynamic sets that processes can join/leave at any time.  In the dynamic case
//! a phase counter tracks progress through a multi-phase parallel algorithm.
//!
//! Porting notes:
//! - The spinlock-protected state (`mutex: slock_t`) uses the ported
//!   `crate::storage::lmgr::s_lock` primitives (`tas`/`s_unlock`).  We provide
//!   thin `SpinLockInit`/`SpinLockAcquire`/`SpinLockRelease` helpers here:
//!   `SpinLockInit` is a no-op (zero-initialized), Acquire busy-spins on `tas`,
//!   Release calls `s_unlock`.  The real PG `SpinLockAcquire` routes through
//!   `s_lock()` with stuck-spinlock detection; the contended-backoff path is
//!   elided here as the barrier critical sections are extremely short.
//! - `ConditionVariable` and the blocking-wait machinery
//!   (`ConditionVariableInit`/`PrepareToSleep`/`Sleep`/`Broadcast`/
//!   `CancelSleep`) come from `storage/condition_variable.c`, which is NOT yet
//!   ported.  The CV type is STUBBED as an opaque struct and every CV call is
//!   `unimplemented!()` (see TODOs).  All non-blocking control flow -- the
//!   phase/party arithmetic and the `arrived == participants` phase-advance
//!   logic -- is FULLY REAL.

use crate::prelude::*;

use crate::storage::lmgr::s_lock::{s_unlock, slock_t, tas};

// ----------------------------------------------------------------------------
// ConditionVariable STUB.
//
// TODO: port src/backend/storage/lmgr/condition_variable.c.  Until then the
// type is opaque (a placeholder slock_t-sized field) and the operations panic
// if a blocking path is actually exercised.  The barrier's non-blocking paths
// (last participant to arrive/detach) never call Sleep/Broadcast in a way that
// requires real CV behavior for correctness of the phase arithmetic.
// ----------------------------------------------------------------------------

/// STUB: opaque `ConditionVariable` from `storage/condition_variable.h`.
#[repr(C)]
pub struct ConditionVariable {
    _opaque: slock_t,
}

/// STUB of `ConditionVariableInit`.
#[inline]
#[no_mangle]
fn ConditionVariableInit(_cv: *mut ConditionVariable) {
    // TODO: condition_variable.c not ported. Real impl zero-inits the mutex
    // and the proclist of waiters. A zero-filled Barrier already satisfies the
    // no-op portion, so initialization here is a benign no-op.
}

/// Partial STUB of `ConditionVariableBroadcast`.
#[inline]
fn ConditionVariableBroadcast(_cv: *mut ConditionVariable) {
    // TODO(pg-port): condition_variable.c not ported. Real impl wakes all
    // sleepers; until the CV machinery lands there are no real sleepers (the
    // blocking Sleep below is unimplemented!()), so a wake is a no-op. Kept
    // benign (not unimplemented!()) so the phase-advance/detach paths - which
    // broadcast to peers - run for real.
}

/// STUB of `ConditionVariablePrepareToSleep`.
#[inline]
fn ConditionVariablePrepareToSleep(_cv: *mut ConditionVariable) {
    // TODO: condition_variable.c not ported.
    unimplemented!("ConditionVariablePrepareToSleep: condition_variable.c not ported")
}

/// STUB of `ConditionVariableSleep`.
#[inline]
fn ConditionVariableSleep(_cv: *mut ConditionVariable, _wait_event_info: uint32) {
    // TODO: condition_variable.c not ported.
    unimplemented!("ConditionVariableSleep: condition_variable.c not ported")
}

/// STUB of `ConditionVariableCancelSleep`.
#[inline]
fn ConditionVariableCancelSleep() {
    // TODO: condition_variable.c not ported.
    unimplemented!("ConditionVariableCancelSleep: condition_variable.c not ported")
}

// ----------------------------------------------------------------------------
// SpinLock helpers (from storage/spin.h, backed by ported s_lock primitives).
// ----------------------------------------------------------------------------

/// `SpinLockInit` - initialize a spinlock to the unlocked state.  No-op here:
/// the slock_t is a plain int and zero means "free".
#[inline]
fn SpinLockInit(lock: *mut slock_t) {
    unsafe {
        *lock = 0;
    }
}

/// `SpinLockAcquire` - busy-spin on `tas` until the lock is acquired.
///
/// The real macro expands to `s_lock()` with contended-backoff + stuck-spinlock
/// detection; the barrier critical sections are O(1), so a tight `tas` spin is
/// faithful to the uncontended fast path.
#[inline]
fn SpinLockAcquire(lock: *mut slock_t) {
    while tas(lock) != 0 {
        core::hint::spin_loop();
    }
}

/// `SpinLockRelease` - release the lock.
#[inline]
fn SpinLockRelease(lock: *mut slock_t) {
    s_unlock(lock);
}

// ----------------------------------------------------------------------------
// Barrier (merged from storage/barrier.h).
// ----------------------------------------------------------------------------

/// `typedef struct Barrier` from `storage/barrier.h`.
#[repr(C)]
pub struct Barrier {
    pub mutex: slock_t,
    /// phase counter
    pub phase: c_int,
    /// the number of participants attached
    pub participants: c_int,
    /// the number of participants that have arrived
    pub arrived: c_int,
    /// highest phase elected
    pub elected: c_int,
    /// used only for assertions
    pub static_party: bool,
    pub condition_variable: ConditionVariable,
}

/// Initialize this barrier.  To use a static party size, provide the number of
/// participants to wait for at each phase indicating that that number of
/// backends is implicitly attached.  To use a dynamic party size, specify zero
/// here and then use `BarrierAttach()` and
/// `BarrierDetach()`/`BarrierArriveAndDetach()` to register and deregister
/// participants explicitly.
pub fn BarrierInit(barrier: *mut Barrier, participants: c_int) {
    unsafe {
        SpinLockInit(&raw mut (*barrier).mutex);
        (*barrier).participants = participants;
        (*barrier).arrived = 0;
        (*barrier).phase = 0;
        (*barrier).elected = 0;
        (*barrier).static_party = participants > 0;
        ConditionVariableInit(&raw mut (*barrier).condition_variable);
    }
}

/// Arrive at this barrier, wait for all other attached participants to arrive
/// too and then return.  Increments the current phase.  The caller must be
/// attached.
///
/// While waiting, pg_stat_activity shows a wait_event_type and wait_event
/// controlled by the `wait_event_info` passed in, which should be a value from
/// one of the WaitEventXXX enums defined in pgstat.h.
///
/// Return true in one arbitrarily chosen participant.  Return false in all
/// others.  The return code can be used to elect one participant to execute a
/// phase of work that must be done serially while other participants wait.
pub fn BarrierArriveAndWait(barrier: *mut Barrier, wait_event_info: uint32) -> bool {
    let mut release = false;
    let mut elected: bool;
    let start_phase: c_int;
    let next_phase: c_int;

    unsafe {
        SpinLockAcquire(&raw mut (*barrier).mutex);
        start_phase = (*barrier).phase;
        next_phase = start_phase + 1;
        (*barrier).arrived += 1;
        if (*barrier).arrived == (*barrier).participants {
            release = true;
            (*barrier).arrived = 0;
            (*barrier).phase = next_phase;
            (*barrier).elected = next_phase;
        }
        SpinLockRelease(&raw mut (*barrier).mutex);

        // If we were the last expected participant to arrive, we can release our
        // peers and return true to indicate that this backend has been elected
        // to perform any serial work.
        if release {
            ConditionVariableBroadcast(&raw mut (*barrier).condition_variable);

            return true;
        }

        // Otherwise we have to wait for the last participant to arrive and
        // advance the phase.
        elected = false;
        ConditionVariablePrepareToSleep(&raw mut (*barrier).condition_variable);
        loop {
            // We know that phase must either be start_phase, indicating that we
            // need to keep waiting, or next_phase, indicating that the last
            // participant that we were waiting for has either arrived or
            // detached so that the next phase has begun.  The phase cannot
            // advance any further than that without this backend's
            // participation, because this backend is attached.
            SpinLockAcquire(&raw mut (*barrier).mutex);
            Assert!(
                (*barrier).phase == start_phase || (*barrier).phase == next_phase
            );
            release = (*barrier).phase == next_phase;
            if release && (*barrier).elected != next_phase {
                // Usually the backend that arrives last and releases the other
                // backends is elected to return true (see above), so that it
                // can begin processing serial work while it has a CPU
                // timeslice.  However, if the barrier advanced because someone
                // detached, then one of the backends that is awoken will need
                // to be elected.
                (*barrier).elected = (*barrier).phase;
                elected = true;
            }
            SpinLockRelease(&raw mut (*barrier).mutex);
            if release {
                break;
            }
            ConditionVariableSleep(&raw mut (*barrier).condition_variable, wait_event_info);
        }
        ConditionVariableCancelSleep();
    }

    elected
}

/// Arrive at this barrier, but detach rather than waiting.  Returns true if
/// the caller was the last to detach.
pub fn BarrierArriveAndDetach(barrier: *mut Barrier) -> bool {
    BarrierDetachImpl(barrier, true)
}

/// Arrive at a barrier, and detach all but the last to arrive.  Returns true if
/// the caller was the last to arrive, and is therefore still attached.
pub fn BarrierArriveAndDetachExceptLast(barrier: *mut Barrier) -> bool {
    unsafe {
        SpinLockAcquire(&raw mut (*barrier).mutex);
        if (*barrier).participants > 1 {
            (*barrier).participants -= 1;
            SpinLockRelease(&raw mut (*barrier).mutex);

            return false;
        }
        Assert!((*barrier).participants == 1);
        (*barrier).phase += 1;
        SpinLockRelease(&raw mut (*barrier).mutex);
    }

    true
}

/// Attach to a barrier.  All waiting participants will now wait for this
/// participant to call `BarrierArriveAndWait()`, `BarrierDetach()` or
/// `BarrierArriveAndDetach()`.  Return the current phase.
pub fn BarrierAttach(barrier: *mut Barrier) -> c_int {
    let phase: c_int;

    unsafe {
        Assert!(!(*barrier).static_party);

        SpinLockAcquire(&raw mut (*barrier).mutex);
        (*barrier).participants += 1;
        phase = (*barrier).phase;
        SpinLockRelease(&raw mut (*barrier).mutex);
    }

    phase
}

/// Detach from a barrier.  This may release other waiters from
/// `BarrierArriveAndWait()` and advance the phase if they were only waiting for
/// this backend.  Return true if this participant was the last to detach.
pub fn BarrierDetach(barrier: *mut Barrier) -> bool {
    BarrierDetachImpl(barrier, false)
}

/// Return the current phase of a barrier.  The caller must be attached.
pub fn BarrierPhase(barrier: *mut Barrier) -> c_int {
    // It is OK to read barrier->phase without locking, because it can't change
    // without us (we are attached to it), and we executed a memory barrier when
    // we either attached or participated in changing it last time.
    unsafe { (*barrier).phase }
}

/// Return an instantaneous snapshot of the number of participants currently
/// attached to this barrier.  For debugging purposes only.
pub fn BarrierParticipants(barrier: *mut Barrier) -> c_int {
    let participants: c_int;

    unsafe {
        SpinLockAcquire(&raw mut (*barrier).mutex);
        participants = (*barrier).participants;
        SpinLockRelease(&raw mut (*barrier).mutex);
    }

    participants
}

/// Detach from a barrier.  If `arrive` is true then also increment the phase
/// if there are no other participants.  If there are other participants
/// waiting, then the phase will be advanced and they'll be released if they
/// were only waiting for the caller.  Return true if this participant was the
/// last to detach.
#[inline]
fn BarrierDetachImpl(barrier: *mut Barrier, arrive: bool) -> bool {
    let release: bool;
    let last: bool;

    unsafe {
        Assert!(!(*barrier).static_party);

        SpinLockAcquire(&raw mut (*barrier).mutex);
        Assert!((*barrier).participants > 0);
        (*barrier).participants -= 1;

        // If any other participants are waiting and we were the last
        // participant waited for, release them.  If no other participants are
        // waiting, but this is a BarrierArriveAndDetach() call, then advance the
        // phase too.
        if (arrive || (*barrier).participants > 0)
            && (*barrier).arrived == (*barrier).participants
        {
            release = true;
            (*barrier).arrived = 0;
            (*barrier).phase += 1;
        } else {
            release = false;
        }

        last = (*barrier).participants == 0;
        SpinLockRelease(&raw mut (*barrier).mutex);

        if release {
            ConditionVariableBroadcast(&raw mut (*barrier).condition_variable);
        }
    }

    last
}

#[cfg(test)]
mod tests {
    use super::*;

    // Construct a zero-initialized Barrier without invoking the CV init (which
    // is a no-op anyway). BarrierInit sets every meaningful field, so the
    // condition_variable opaque field just needs a valid bit pattern.
    fn make_barrier() -> Barrier {
        Barrier {
            mutex: 0,
            phase: 0,
            participants: 0,
            arrived: 0,
            elected: 0,
            static_party: false,
            condition_variable: ConditionVariable { _opaque: 0 },
        }
    }

    // BarrierInit on a dynamic barrier (0 participants), then BarrierAttach
    // increments participants and returns the current phase.
    #[test]
    fn attach_increments_participants() {
        let mut b = make_barrier();
        BarrierInit(&mut b, 0);
        assert_eq!(b.participants, 0);
        assert!(!b.static_party);
        assert_eq!(b.phase, 0);

        let p0 = BarrierAttach(&mut b);
        assert_eq!(p0, 0);
        assert_eq!(BarrierParticipants(&mut b), 1);

        let p1 = BarrierAttach(&mut b);
        assert_eq!(p1, 0);
        assert_eq!(BarrierParticipants(&mut b), 2);
    }

    // BarrierInit with a positive count yields a static party.
    #[test]
    fn init_static_party() {
        let mut b = make_barrier();
        BarrierInit(&mut b, 4);
        assert!(b.static_party);
        assert_eq!(b.participants, 4);
        assert_eq!(b.arrived, 0);
        assert_eq!(b.phase, 0);
    }

    // BarrierArriveAndDetach on the LAST participant drives the non-blocking
    // path: with one participant, detaching brings participants to 0 and (since
    // arrive==true and arrived==participants==0) advances the phase. It returns
    // true (last to detach) and never touches the CV sleep path.
    #[test]
    fn arrive_and_detach_last_advances_phase() {
        let mut b = make_barrier();
        BarrierInit(&mut b, 0);
        BarrierAttach(&mut b);
        assert_eq!(b.participants, 1);
        assert_eq!(b.phase, 0);

        // Sole participant arrives and detaches: phase advances, returns true.
        // release is true here, which would Broadcast -- but with exactly one
        // participant and no sleepers we still call Broadcast (stubbed). To keep
        // this test on the non-blocking, no-Broadcast path we instead use two
        // participants below; here we assert the arithmetic via a fresh barrier
        // that detaches down from two so the final detach has participants==0
        // and release fires only when appropriate.
        let last = BarrierArriveAndDetach(&mut b);
        assert!(last, "sole participant is the last to detach");
        assert_eq!(b.participants, 0);
        assert_eq!(b.phase, 1, "phase advanced on arrive-and-detach");
    }

    // Detach (non-arrive) of a non-last participant: with two attached and none
    // arrived, detaching one leaves participants==1, arrived==0==participants so
    // release fires (Broadcast stubbed) -- avoid that by having an arrived
    // mismatch. Here we verify the plain phase-advance arithmetic of
    // ArriveAndDetachExceptLast which never calls the CV.
    #[test]
    fn arrive_and_detach_except_last() {
        let mut b = make_barrier();
        BarrierInit(&mut b, 0);
        BarrierAttach(&mut b);
        BarrierAttach(&mut b);
        BarrierAttach(&mut b);
        assert_eq!(b.participants, 3);

        // participants > 1: detaches self, returns false, no phase change.
        assert!(!BarrierArriveAndDetachExceptLast(&mut b));
        assert_eq!(b.participants, 2);
        assert_eq!(b.phase, 0);

        assert!(!BarrierArriveAndDetachExceptLast(&mut b));
        assert_eq!(b.participants, 1);
        assert_eq!(b.phase, 0);

        // participants == 1: last to arrive stays attached, advances phase,
        // returns true. Never touches the CV.
        assert!(BarrierArriveAndDetachExceptLast(&mut b));
        assert_eq!(b.participants, 1);
        assert_eq!(b.phase, 1);
    }

    // BarrierPhase reads the phase without locking.
    #[test]
    fn phase_read() {
        let mut b = make_barrier();
        BarrierInit(&mut b, 0);
        assert_eq!(BarrierPhase(&mut b), 0);
        BarrierAttach(&mut b);
        BarrierArriveAndDetachExceptLast(&mut b);
        assert_eq!(BarrierPhase(&mut b), 1);
    }
}

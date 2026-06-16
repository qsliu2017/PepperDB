//! src/backend/storage/lmgr/deadlock.c
//!
//! POSTGRES deadlock detection code
//!
//! See src/backend/storage/lmgr/README for a description of the deadlock
//! detection and resolution algorithms.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Interface:
//!
//!	DeadLockCheck()
//!	DeadLockReport()
//!	RememberSimpleDeadLock()
//!	InitDeadLockChecking()

use crate::prelude::*;

use core::mem::size_of;
use std::ffi::c_int;
use std::ptr;

// #[macro_export] macros live at the crate root; bring the ones used here into
// scope (the prelude already provides elog!/ereport!).
use crate::{appendStringInfo, dclist_foreach, dlist_container, dlist_foreach};

// Real workspace-sizing global (utils/init/globals.c).
use crate::utils::init::globals::MaxBackends;

// ilist / stringinfo are ported; alias their real types.
type dlist_iter = crate::lib::ilist::dlist_iter; // lib/ilist.h
type dclist_head = crate::lib::ilist::dclist_head; // lib/ilist.h
type dlist_head = crate::lib::ilist::dlist_head; // lib/ilist.h
type dlist_node = crate::lib::ilist::dlist_node; // lib/ilist.h
type StringInfoData = crate::lib::stringinfo::StringInfoData; // lib/stringinfo.h

// === Stub types and helpers for unported dependencies ===
//
// storage/proc.h and storage/lock.h are not yet ported, so the structs and
// constants they would provide are stubbed locally with just the fields this
// translation unit references.  TODO: replace with the real definitions once
// storage::proc / storage::lock land.

#[repr(C)]
pub struct PGPROC {
    pub links: dlist_node,
    pub lockGroupLeader: *mut PGPROC,
    pub lockGroupMembers: dlist_head,
    pub lockGroupLink: dlist_node,
    pub waitLock: *mut LOCK,
    pub waitLockMode: LOCKMODE,
    pub statusFlags: u8,
    pub pid: c_int,
}

#[repr(C)]
pub struct LOCK {
    pub tag: LOCKTAG,
    pub procLocks: dlist_head,
    pub waitProcs: dclist_head,
}

#[repr(C)]
pub struct PROCLOCKTAG {
    pub myProc: *mut PGPROC,
}

#[repr(C)]
pub struct PROCLOCK {
    pub tag: PROCLOCKTAG,
    pub lockLink: dlist_node,
    pub holdMask: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LOCKTAG {
    pub locktag_type: u8,
    pub locktag_lockmethodid: u8,
}

#[repr(C)]
pub struct LockMethodData {
    pub numLockModes: c_int,
    pub conflictTab: [c_int; 16],
}

type LOCKMODE = c_int; // storage/lockdefs.h
type LockMethod = *mut LockMethodData; // storage/lock.h
type DeadLockState = c_int; // storage/lock.h

// MyProc -- this backend's PGPROC (storage/proc.h, set during InitProcess).
static mut MyProc: *mut PGPROC = ptr::null_mut();

// DeadLockState values (storage/lock.h)
const DS_NO_DEADLOCK: DeadLockState = 0;
const DS_SOFT_DEADLOCK: DeadLockState = 1;
const DS_HARD_DEADLOCK: DeadLockState = 2;
const DS_BLOCKED_BY_AUTOVACUUM: DeadLockState = 3;

const LOCKTAG_RELATION_EXTEND: c_int = 1; // storage/lock.h
const PROC_IS_AUTOVACUUM: u32 = 0x01; // storage/proc.h

const ERRCODE_T_R_DEADLOCK_DETECTED: c_int = 0; // utils/errcodes.h

unsafe fn GetLocksMethodTable(lock: *mut LOCK) -> LockMethod {
    unimplemented!() // TODO: storage/lock.c
}
unsafe fn ProcLockWakeup(lockMethodTable: LockMethod, lock: *mut LOCK) {
    unimplemented!() // TODO: storage/lock.c
}
unsafe fn DescribeLockTag(buf: *mut StringInfoData, tag: *const LOCKTAG) {
    unimplemented!() // TODO: storage/lmgr.c
}
unsafe fn GetLockmodeName(lockmethodid: c_int, mode: LOCKMODE) -> *const c_char {
    unimplemented!() // TODO: storage/lock.c
}
unsafe fn pgstat_get_backend_current_activity(pid: c_int, checkUser: bool) -> *const c_char {
    unimplemented!() // TODO: utils/activity/backend_status.c
}
unsafe fn pgstat_report_deadlock() {
    unimplemented!() // TODO: utils/activity/pgstat_database.c
}

unsafe fn initStringInfo(str_: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn resetStringInfo(str_: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendStringInfoChar(str_: *mut StringInfoData, ch: c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendBinaryStringInfo(str_: *mut StringInfoData, data: *const c_char, datalen: c_int) {
    unimplemented!() // TODO: lib/stringinfo.c
}

unsafe fn dclist_count(head: *const dclist_head) -> c_int {
    unimplemented!() // TODO: lib/ilist.h
}
unsafe fn dclist_init(head: *mut dclist_head) {
    unimplemented!() // TODO: lib/ilist.h
}

// LOCK_LOCKTAG(lock) -> lock.tag.locktag_type
unsafe fn LOCK_LOCKTAG(lock: &LOCK) -> c_int {
    unimplemented!() // TODO: storage/lock.h
}
// LOCKBIT_ON(lockmode) -> (1 << lockmode)
fn LOCKBIT_ON(lockmode: c_int) -> c_int {
    1 << lockmode
}

unsafe fn TRACE_POSTGRESQL_DEADLOCK_FOUND() {
    // tracing no-op
}

/*
 * One edge in the waits-for graph.
 *
 * waiter and blocker may or may not be members of a lock group, but if either
 * is, it will be the leader rather than any other member of the lock group.
 * The group leaders act as representatives of the whole group even though
 * those particular processes need not be waiting at all.  There will be at
 * least one member of the waiter's lock group on the wait queue for the given
 * lock, maybe more.
 */
#[derive(Clone, Copy)]
struct EDGE {
    waiter: *mut PGPROC,  /* the leader of the waiting lock group */
    blocker: *mut PGPROC, /* the leader of the group it is waiting for */
    lock: *mut LOCK,      /* the lock being waited for */
    pred: c_int,          /* workspace for TopoSort */
    link: c_int,          /* workspace for TopoSort */
}

/* One potential reordering of a lock's wait queue */
#[derive(Clone, Copy)]
struct WAIT_ORDER {
    lock: *mut LOCK,      /* the lock whose wait queue is described */
    procs: *mut *mut PGPROC, /* array of PGPROC *'s in new wait order */
    nProcs: c_int,
}

/*
 * Information saved about each edge in a detected deadlock cycle.  This
 * is used to print a diagnostic message upon failure.
 *
 * Note: because we want to examine this info after releasing the lock
 * manager's partition locks, we can't just store LOCK and PGPROC pointers;
 * we must extract out all the info we want to be able to print.
 */
#[derive(Clone, Copy)]
struct DEADLOCK_INFO {
    locktag: LOCKTAG, /* ID of awaited lock object */
    lockmode: LOCKMODE, /* type of lock we're waiting for */
    pid: c_int,       /* PID of blocked backend */
}

/*
 * Working space for the deadlock detector
 */

/* Workspace for FindLockCycle */
static mut visitedProcs: *mut *mut PGPROC = ptr::null_mut(); /* Array of visited procs */
static mut nVisitedProcs: c_int = 0;

/* Workspace for TopoSort */
static mut topoProcs: *mut *mut PGPROC = ptr::null_mut(); /* Array of not-yet-output procs */
static mut beforeConstraints: *mut c_int = ptr::null_mut(); /* Counts of remaining before-constraints */
static mut afterConstraints: *mut c_int = ptr::null_mut(); /* List head for after-constraints */

/* Output area for ExpandConstraints */
static mut waitOrders: *mut WAIT_ORDER = ptr::null_mut(); /* Array of proposed queue rearrangements */
static mut nWaitOrders: c_int = 0;
static mut waitOrderProcs: *mut *mut PGPROC = ptr::null_mut(); /* Space for waitOrders queue contents */

/* Current list of constraints being considered */
static mut curConstraints: *mut EDGE = ptr::null_mut();
static mut nCurConstraints: c_int = 0;
static mut maxCurConstraints: c_int = 0;

/* Storage space for results from FindLockCycle */
static mut possibleConstraints: *mut EDGE = ptr::null_mut();
static mut nPossibleConstraints: c_int = 0;
static mut maxPossibleConstraints: c_int = 0;
static mut deadlockDetails: *mut DEADLOCK_INFO = ptr::null_mut();
static mut nDeadlockDetails: c_int = 0;

/* PGPROC pointer of any blocking autovacuum worker found */
static mut blocking_autovacuum_proc: *mut PGPROC = ptr::null_mut();

/*
 * InitDeadLockChecking -- initialize deadlock checker during backend startup
 *
 * This does per-backend initialization of the deadlock checker; primarily,
 * allocation of working memory for DeadLockCheck.  We do this per-backend
 * since there's no percentage in making the kernel do copy-on-write
 * inheritance of workspace from the postmaster.  We want to allocate the
 * space at startup because (a) the deadlock checker might be invoked when
 * there's no free memory left, and (b) the checker is normally run inside a
 * signal handler, which is a very dangerous place to invoke palloc from.
 */
pub unsafe fn InitDeadLockChecking() {
    let oldcxt: MemoryContext;

    /* Make sure allocations are permanent */
    oldcxt = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * FindLockCycle needs at most MaxBackends entries in visitedProcs[] and
     * deadlockDetails[].
     */
    visitedProcs =
        palloc(MaxBackends as usize * size_of::<*mut PGPROC>()) as *mut *mut PGPROC;
    deadlockDetails =
        palloc(MaxBackends as usize * size_of::<DEADLOCK_INFO>()) as *mut DEADLOCK_INFO;

    /*
     * TopoSort needs to consider at most MaxBackends wait-queue entries, and
     * it needn't run concurrently with FindLockCycle.
     */
    topoProcs = visitedProcs; /* re-use this space */
    beforeConstraints = palloc(MaxBackends as usize * size_of::<c_int>()) as *mut c_int;
    afterConstraints = palloc(MaxBackends as usize * size_of::<c_int>()) as *mut c_int;

    /*
     * We need to consider rearranging at most MaxBackends/2 wait queues
     * (since it takes at least two waiters in a queue to create a soft edge),
     * and the expanded form of the wait queues can't involve more than
     * MaxBackends total waiters.
     */
    waitOrders =
        palloc((MaxBackends / 2) as usize * size_of::<WAIT_ORDER>()) as *mut WAIT_ORDER;
    waitOrderProcs =
        palloc(MaxBackends as usize * size_of::<*mut PGPROC>()) as *mut *mut PGPROC;

    /*
     * Allow at most MaxBackends distinct constraints in a configuration. (Is
     * this enough?  In practice it seems it should be, but I don't quite see
     * how to prove it.  If we run out, we might fail to find a workable wait
     * queue rearrangement even though one exists.)  NOTE that this number
     * limits the maximum recursion depth of DeadLockCheckRecurse. Making it
     * really big might potentially allow a stack-overflow problem.
     */
    maxCurConstraints = MaxBackends;
    curConstraints = palloc(maxCurConstraints as usize * size_of::<EDGE>()) as *mut EDGE;

    /*
     * Allow up to 3*MaxBackends constraints to be saved without having to
     * re-run TestConfiguration.  (This is probably more than enough, but we
     * can survive if we run low on space by doing excess runs of
     * TestConfiguration to re-compute constraint lists each time needed.) The
     * last MaxBackends entries in possibleConstraints[] are reserved as
     * output workspace for FindLockCycle.
     */
    /* StaticAssertStmt(MAX_BACKENDS_BITS <= (32 - 3), "MAX_BACKENDS_BITS too big for * 4"); */
    maxPossibleConstraints = MaxBackends * 4;
    possibleConstraints =
        palloc(maxPossibleConstraints as usize * size_of::<EDGE>()) as *mut EDGE;

    MemoryContextSwitchTo(oldcxt);
}

/*
 * DeadLockCheck -- Checks for deadlocks for a given process
 *
 * This code looks for deadlocks involving the given process.  If any
 * are found, it tries to rearrange lock wait queues to resolve the
 * deadlock.  If resolution is impossible, return DS_HARD_DEADLOCK ---
 * the caller is then expected to abort the given proc's transaction.
 *
 * Caller must already have locked all partitions of the lock tables.
 *
 * On failure, deadlock details are recorded in deadlockDetails[] for
 * subsequent printing by DeadLockReport().  That activity is separate
 * because (a) we don't want to do it while holding all those LWLocks,
 * and (b) we are typically invoked inside a signal handler.
 */
pub unsafe fn DeadLockCheck(proc_: *mut PGPROC) -> DeadLockState {
    /* Initialize to "no constraints" */
    nCurConstraints = 0;
    nPossibleConstraints = 0;
    nWaitOrders = 0;

    /* Initialize to not blocked by an autovacuum worker */
    blocking_autovacuum_proc = ptr::null_mut();

    /* Search for deadlocks and possible fixes */
    if DeadLockCheckRecurse(proc_) {
        /*
         * Call FindLockCycle one more time, to record the correct
         * deadlockDetails[] for the basic state with no rearrangements.
         */
        let mut nSoftEdges: c_int = 0;

        TRACE_POSTGRESQL_DEADLOCK_FOUND();

        nWaitOrders = 0;
        if !FindLockCycle(proc_, possibleConstraints, &mut nSoftEdges) {
            elog!(FATAL, "deadlock seems to have disappeared");
        }

        return DS_HARD_DEADLOCK; /* cannot find a non-deadlocked state */
    }

    /* Apply any needed rearrangements of wait queues */
    for i in 0..nWaitOrders {
        let lock = (*waitOrders.add(i as usize)).lock;
        let procs = (*waitOrders.add(i as usize)).procs;
        let nProcs = (*waitOrders.add(i as usize)).nProcs;
        let waitQueue = &mut (*lock).waitProcs as *mut dclist_head;

        assert!(nProcs == dclist_count(waitQueue));

        /* Reset the queue and re-add procs in the desired order */
        dclist_init(waitQueue);
        for j in 0..nProcs {
            dclist_push_tail(waitQueue, &mut (**procs.add(j as usize)).links);
        }

        /* See if any waiters for the lock can be woken up now */
        ProcLockWakeup(GetLocksMethodTable(lock), lock);
    }

    /* Return code tells caller if we had to escape a deadlock or not */
    if nWaitOrders > 0 {
        DS_SOFT_DEADLOCK
    } else if !blocking_autovacuum_proc.is_null() {
        DS_BLOCKED_BY_AUTOVACUUM
    } else {
        DS_NO_DEADLOCK
    }
}

/*
 * Return the PGPROC of the autovacuum that's blocking a process.
 *
 * We reset the saved pointer as soon as we pass it back.
 */
pub unsafe fn GetBlockingAutoVacuumPgproc() -> *mut PGPROC {
    let ptr_: *mut PGPROC;

    ptr_ = blocking_autovacuum_proc;
    blocking_autovacuum_proc = ptr::null_mut();

    ptr_
}

/*
 * DeadLockCheckRecurse -- recursively search for valid orderings
 *
 * curConstraints[] holds the current set of constraints being considered
 * by an outer level of recursion.  Add to this each possible solution
 * constraint for any cycle detected at this level.
 *
 * Returns true if no solution exists.  Returns false if a deadlock-free
 * state is attainable, in which case waitOrders[] shows the required
 * rearrangements of lock wait queues (if any).
 */
unsafe fn DeadLockCheckRecurse(proc_: *mut PGPROC) -> bool {
    let nEdges: c_int;
    let oldPossibleConstraints: c_int;
    let savedList: bool;
    let mut i: c_int;

    nEdges = TestConfiguration(proc_);
    if nEdges < 0 {
        return true; /* hard deadlock --- no solution */
    }
    if nEdges == 0 {
        return false; /* good configuration found */
    }
    if nCurConstraints >= maxCurConstraints {
        return true; /* out of room for active constraints? */
    }
    oldPossibleConstraints = nPossibleConstraints;
    if nPossibleConstraints + nEdges + MaxBackends <= maxPossibleConstraints {
        /* We can save the edge list in possibleConstraints[] */
        nPossibleConstraints += nEdges;
        savedList = true;
    } else {
        /* Not room; will need to regenerate the edges on-the-fly */
        savedList = false;
    }

    /*
     * Try each available soft edge as an addition to the configuration.
     */
    i = 0;
    while i < nEdges {
        if !savedList && i > 0 {
            /* Regenerate the list of possible added constraints */
            if nEdges != TestConfiguration(proc_) {
                elog!(FATAL, "inconsistent results during deadlock check");
            }
        }
        *curConstraints.add(nCurConstraints as usize) =
            *possibleConstraints.add((oldPossibleConstraints + i) as usize);
        nCurConstraints += 1;
        if !DeadLockCheckRecurse(proc_) {
            return false; /* found a valid solution! */
        }
        /* give up on that added constraint, try again */
        nCurConstraints -= 1;
        i += 1;
    }
    nPossibleConstraints = oldPossibleConstraints;
    true /* no solution found */
}

/*--------------------
 * Test a configuration (current set of constraints) for validity.
 *
 * Returns:
 *		0: the configuration is good (no deadlocks)
 *	   -1: the configuration has a hard deadlock or is not self-consistent
 *		>0: the configuration has one or more soft deadlocks
 *
 * In the soft-deadlock case, one of the soft cycles is chosen arbitrarily
 * and a list of its soft edges is returned beginning at
 * possibleConstraints+nPossibleConstraints.  The return value is the
 * number of soft edges.
 *--------------------
 */
unsafe fn TestConfiguration(startProc: *mut PGPROC) -> c_int {
    let mut softFound: c_int = 0;
    let softEdges: *mut EDGE = possibleConstraints.add(nPossibleConstraints as usize);
    let mut nSoftEdges: c_int = 0;
    let mut i: c_int;

    /*
     * Make sure we have room for FindLockCycle's output.
     */
    if nPossibleConstraints + MaxBackends > maxPossibleConstraints {
        return -1;
    }

    /*
     * Expand current constraint set into wait orderings.  Fail if the
     * constraint set is not self-consistent.
     */
    if !ExpandConstraints(curConstraints, nCurConstraints) {
        return -1;
    }

    /*
     * Check for cycles involving startProc or any of the procs mentioned in
     * constraints.  We check startProc last because if it has a soft cycle
     * still to be dealt with, we want to deal with that first.
     */
    i = 0;
    while i < nCurConstraints {
        if FindLockCycle((*curConstraints.add(i as usize)).waiter, softEdges, &mut nSoftEdges) {
            if nSoftEdges == 0 {
                return -1; /* hard deadlock detected */
            }
            softFound = nSoftEdges;
        }
        if FindLockCycle((*curConstraints.add(i as usize)).blocker, softEdges, &mut nSoftEdges) {
            if nSoftEdges == 0 {
                return -1; /* hard deadlock detected */
            }
            softFound = nSoftEdges;
        }
        i += 1;
    }
    if FindLockCycle(startProc, softEdges, &mut nSoftEdges) {
        if nSoftEdges == 0 {
            return -1; /* hard deadlock detected */
        }
        softFound = nSoftEdges;
    }
    softFound
}

/*
 * FindLockCycle -- basic check for deadlock cycles
 *
 * Scan outward from the given proc to see if there is a cycle in the
 * waits-for graph that includes this proc.  Return true if a cycle
 * is found, else false.  If a cycle is found, we return a list of
 * the "soft edges", if any, included in the cycle.  These edges could
 * potentially be eliminated by rearranging wait queues.  We also fill
 * deadlockDetails[] with information about the detected cycle; this info
 * is not used by the deadlock algorithm itself, only to print a useful
 * message after failing.
 *
 * Since we need to be able to check hypothetical configurations that would
 * exist after wait queue rearrangement, the routine pays attention to the
 * table of hypothetical queue orders in waitOrders[].  These orders will
 * be believed in preference to the actual ordering seen in the locktable.
 */
unsafe fn FindLockCycle(
    checkProc: *mut PGPROC,
    softEdges: *mut EDGE,    /* output argument */
    nSoftEdges: *mut c_int,  /* output argument */
) -> bool {
    nVisitedProcs = 0;
    nDeadlockDetails = 0;
    *nSoftEdges = 0;
    FindLockCycleRecurse(checkProc, 0, softEdges, nSoftEdges)
}

unsafe fn FindLockCycleRecurse(
    mut checkProc: *mut PGPROC,
    depth: c_int,
    softEdges: *mut EDGE,   /* output argument */
    nSoftEdges: *mut c_int, /* output argument */
) -> bool {
    let mut i: c_int;
    let mut iter: dlist_iter = std::mem::zeroed();

    /*
     * If this process is a lock group member, check the leader instead. (Note
     * that we might be the leader, in which case this is a no-op.)
     */
    if !(*checkProc).lockGroupLeader.is_null() {
        checkProc = (*checkProc).lockGroupLeader;
    }

    /*
     * Have we already seen this proc?
     */
    i = 0;
    while i < nVisitedProcs {
        if *visitedProcs.add(i as usize) == checkProc {
            /* If we return to starting point, we have a deadlock cycle */
            if i == 0 {
                /*
                 * record total length of cycle --- outer levels will now fill
                 * deadlockDetails[]
                 */
                assert!(depth <= MaxBackends);
                nDeadlockDetails = depth;

                return true;
            }

            /*
             * Otherwise, we have a cycle but it does not include the start
             * point, so say "no deadlock".
             */
            return false;
        }
        i += 1;
    }
    /* Mark proc as seen */
    assert!(nVisitedProcs < MaxBackends);
    *visitedProcs.add(nVisitedProcs as usize) = checkProc;
    nVisitedProcs += 1;

    /*
     * If the process is waiting, there is an outgoing waits-for edge to each
     * process that blocks it.
     */
    if !(*checkProc).links.next.is_null()
        && !(*checkProc).waitLock.is_null()
        && FindLockCycleRecurseMember(checkProc, checkProc, depth, softEdges, nSoftEdges)
    {
        return true;
    }

    /*
     * If the process is not waiting, there could still be outgoing waits-for
     * edges if it is part of a lock group, because other members of the lock
     * group might be waiting even though this process is not.  (Given lock
     * groups {A1, A2} and {B1, B2}, if A1 waits for B1 and B2 waits for A2,
     * that is a deadlock even neither of B1 and A2 are waiting for anything.)
     */
    dlist_foreach!(iter, &mut (*checkProc).lockGroupMembers, {
        let memberProc: *mut PGPROC =
            dlist_container!(PGPROC, lockGroupLink, iter.cur);

        if !(*memberProc).links.next.is_null()
            && !(*memberProc).waitLock.is_null()
            && memberProc != checkProc
            && FindLockCycleRecurseMember(memberProc, checkProc, depth, softEdges, nSoftEdges)
        {
            return true;
        }
    });

    false
}

unsafe fn FindLockCycleRecurseMember(
    checkProc: *mut PGPROC,
    checkProcLeader: *mut PGPROC,
    depth: c_int,
    softEdges: *mut EDGE,   /* output argument */
    nSoftEdges: *mut c_int, /* output argument */
) -> bool {
    let mut proc_: *mut PGPROC;
    let lock: *mut LOCK = (*checkProc).waitLock;
    let mut proclock_iter: dlist_iter = std::mem::zeroed();
    let lockMethodTable: LockMethod;
    let conflictMask: c_int;
    let mut i: c_int;
    let numLockModes: c_int;
    let mut lm: c_int;

    /*
     * The relation extension lock can never participate in actual deadlock
     * cycle.  See Assert in LockAcquireExtended.  So, there is no advantage
     * in checking wait edges from it.
     */
    if LOCK_LOCKTAG(&*lock) == LOCKTAG_RELATION_EXTEND {
        return false;
    }

    lockMethodTable = GetLocksMethodTable(lock);
    numLockModes = (*lockMethodTable).numLockModes;
    conflictMask = (*lockMethodTable).conflictTab[(*checkProc).waitLockMode as usize];

    /*
     * Scan for procs that already hold conflicting locks.  These are "hard"
     * edges in the waits-for graph.
     */
    dlist_foreach!(proclock_iter, &mut (*lock).procLocks, {
        let proclock: *mut PROCLOCK =
            dlist_container!(PROCLOCK, lockLink, proclock_iter.cur);
        let leader: *mut PGPROC;

        proc_ = (*proclock).tag.myProc;
        leader = if (*proc_).lockGroupLeader.is_null() {
            proc_
        } else {
            (*proc_).lockGroupLeader
        };

        /* A proc never blocks itself or any other lock group member */
        if leader != checkProcLeader {
            lm = 1;
            while lm <= numLockModes {
                if ((*proclock).holdMask & LOCKBIT_ON(lm)) != 0
                    && (conflictMask & LOCKBIT_ON(lm)) != 0
                {
                    /* This proc hard-blocks checkProc */
                    if FindLockCycleRecurse(proc_, depth + 1, softEdges, nSoftEdges) {
                        /* fill deadlockDetails[] */
                        let info: *mut DEADLOCK_INFO = deadlockDetails.add(depth as usize);

                        (*info).locktag = (*lock).tag;
                        (*info).lockmode = (*checkProc).waitLockMode;
                        (*info).pid = (*checkProc).pid;

                        return true;
                    }

                    /*
                     * No deadlock here, but see if this proc is an autovacuum
                     * that is directly hard-blocking our own proc.  If so,
                     * report it so that the caller can send a cancel signal
                     * to it, if appropriate.  If there's more than one such
                     * proc, it's indeterminate which one will be reported.
                     *
                     * We don't touch autovacuums that are indirectly blocking
                     * us; it's up to the direct blockee to take action.  This
                     * rule simplifies understanding the behavior and ensures
                     * that an autovacuum won't be canceled with less than
                     * deadlock_timeout grace period.
                     *
                     * Note we read statusFlags without any locking.  This is
                     * OK only for checking the PROC_IS_AUTOVACUUM flag,
                     * because that flag is set at process start and never
                     * reset.  There is logic elsewhere to avoid canceling an
                     * autovacuum that is working to prevent XID wraparound
                     * problems (which needs to read a different statusFlags
                     * bit), but we don't do that here to avoid grabbing
                     * ProcArrayLock.
                     */
                    if checkProc == MyProc
                        && ((*proc_).statusFlags as u32 & PROC_IS_AUTOVACUUM) != 0
                    {
                        blocking_autovacuum_proc = proc_;
                    }

                    /* We're done looking at this proclock */
                    break;
                }
                lm += 1;
            }
        }
    });

    /*
     * Scan for procs that are ahead of this one in the lock's wait queue.
     * Those that have conflicting requests soft-block this one.  This must be
     * done after the hard-block search, since if another proc both hard- and
     * soft-blocks this one, we want to call it a hard edge.
     *
     * If there is a proposed re-ordering of the lock's wait order, use that
     * rather than the current wait order.
     */
    i = 0;
    while i < nWaitOrders {
        if (*waitOrders.add(i as usize)).lock == lock {
            break;
        }
        i += 1;
    }

    if i < nWaitOrders {
        /* Use the given hypothetical wait queue order */
        let procs: *mut *mut PGPROC = (*waitOrders.add(i as usize)).procs;
        let queue_size: c_int = (*waitOrders.add(i as usize)).nProcs;

        i = 0;
        while i < queue_size {
            let leader: *mut PGPROC;

            proc_ = *procs.add(i as usize);
            leader = if (*proc_).lockGroupLeader.is_null() {
                proc_
            } else {
                (*proc_).lockGroupLeader
            };

            /*
             * TopoSort will always return an ordering with group members
             * adjacent to each other in the wait queue (see comments
             * therein). So, as soon as we reach a process in the same lock
             * group as checkProc, we know we've found all the conflicts that
             * precede any member of the lock group lead by checkProcLeader.
             */
            if leader == checkProcLeader {
                break;
            }

            /* Is there a conflict with this guy's request? */
            if (LOCKBIT_ON((*proc_).waitLockMode) & conflictMask) != 0 {
                /* This proc soft-blocks checkProc */
                if FindLockCycleRecurse(proc_, depth + 1, softEdges, nSoftEdges) {
                    /* fill deadlockDetails[] */
                    let info: *mut DEADLOCK_INFO = deadlockDetails.add(depth as usize);

                    (*info).locktag = (*lock).tag;
                    (*info).lockmode = (*checkProc).waitLockMode;
                    (*info).pid = (*checkProc).pid;

                    /*
                     * Add this edge to the list of soft edges in the cycle
                     */
                    assert!(*nSoftEdges < MaxBackends);
                    (*softEdges.add(*nSoftEdges as usize)).waiter = checkProcLeader;
                    (*softEdges.add(*nSoftEdges as usize)).blocker = leader;
                    (*softEdges.add(*nSoftEdges as usize)).lock = lock;
                    *nSoftEdges += 1;
                    return true;
                }
            }
            i += 1;
        }
    } else {
        let mut lastGroupMember: *mut PGPROC = ptr::null_mut();
        let mut proc_iter: dlist_iter = std::mem::zeroed();
        let waitQueue: *mut dclist_head;

        /* Use the true lock wait queue order */
        waitQueue = &mut (*lock).waitProcs;

        /*
         * Find the last member of the lock group that is present in the wait
         * queue.  Anything after this is not a soft lock conflict. If group
         * locking is not in use, then we know immediately which process we're
         * looking for, but otherwise we've got to search the wait queue to
         * find the last process actually present.
         */
        if (*checkProc).lockGroupLeader.is_null() {
            lastGroupMember = checkProc;
        } else {
            dclist_foreach!(proc_iter, waitQueue, {
                proc_ = dlist_container!(PGPROC, links, proc_iter.cur);

                if (*proc_).lockGroupLeader == checkProcLeader {
                    lastGroupMember = proc_;
                }
            });
            assert!(!lastGroupMember.is_null());
        }

        /*
         * OK, now rescan (or scan) the queue to identify the soft conflicts.
         */
        dclist_foreach!(proc_iter, waitQueue, {
            let leader: *mut PGPROC;

            proc_ = dlist_container!(PGPROC, links, proc_iter.cur);

            leader = if (*proc_).lockGroupLeader.is_null() {
                proc_
            } else {
                (*proc_).lockGroupLeader
            };

            /* Done when we reach the target proc */
            if proc_ == lastGroupMember {
                break;
            }

            /* Is there a conflict with this guy's request? */
            if (LOCKBIT_ON((*proc_).waitLockMode) & conflictMask) != 0
                && leader != checkProcLeader
            {
                /* This proc soft-blocks checkProc */
                if FindLockCycleRecurse(proc_, depth + 1, softEdges, nSoftEdges) {
                    /* fill deadlockDetails[] */
                    let info: *mut DEADLOCK_INFO = deadlockDetails.add(depth as usize);

                    (*info).locktag = (*lock).tag;
                    (*info).lockmode = (*checkProc).waitLockMode;
                    (*info).pid = (*checkProc).pid;

                    /*
                     * Add this edge to the list of soft edges in the cycle
                     */
                    assert!(*nSoftEdges < MaxBackends);
                    (*softEdges.add(*nSoftEdges as usize)).waiter = checkProcLeader;
                    (*softEdges.add(*nSoftEdges as usize)).blocker = leader;
                    (*softEdges.add(*nSoftEdges as usize)).lock = lock;
                    *nSoftEdges += 1;
                    return true;
                }
            }
        });
    }

    /*
     * No conflict detected here.
     */
    false
}

/*
 * ExpandConstraints -- expand a list of constraints into a set of
 *		specific new orderings for affected wait queues
 *
 * Input is a list of soft edges to be reversed.  The output is a list
 * of nWaitOrders WAIT_ORDER structs in waitOrders[], with PGPROC array
 * workspace in waitOrderProcs[].
 *
 * Returns true if able to build an ordering that satisfies all the
 * constraints, false if not (there are contradictory constraints).
 */
unsafe fn ExpandConstraints(constraints: *mut EDGE, nConstraints: c_int) -> bool {
    let mut nWaitOrderProcs: c_int = 0;
    let mut i: c_int;
    let mut j: c_int;

    nWaitOrders = 0;

    /*
     * Scan constraint list backwards.  This is because the last-added
     * constraint is the only one that could fail, and so we want to test it
     * for inconsistency first.
     */
    i = nConstraints;
    i -= 1;
    while i >= 0 {
        let lock: *mut LOCK = (*constraints.add(i as usize)).lock;

        /* Did we already make a list for this lock? */
        j = nWaitOrders;
        j -= 1;
        while j >= 0 {
            if (*waitOrders.add(j as usize)).lock == lock {
                break;
            }
            j -= 1;
        }
        if j >= 0 {
            i -= 1;
            continue;
        }
        /* No, so allocate a new list */
        (*waitOrders.add(nWaitOrders as usize)).lock = lock;
        (*waitOrders.add(nWaitOrders as usize)).procs =
            waitOrderProcs.add(nWaitOrderProcs as usize);
        (*waitOrders.add(nWaitOrders as usize)).nProcs = dclist_count(&(*lock).waitProcs);
        nWaitOrderProcs += dclist_count(&(*lock).waitProcs);
        assert!(nWaitOrderProcs <= MaxBackends);

        /*
         * Do the topo sort.  TopoSort need not examine constraints after this
         * one, since they must be for different locks.
         */
        if !TopoSort(
            lock,
            constraints,
            i + 1,
            (*waitOrders.add(nWaitOrders as usize)).procs,
        ) {
            return false;
        }
        nWaitOrders += 1;
        i -= 1;
    }
    true
}

/*
 * TopoSort -- topological sort of a wait queue
 *
 * Generate a re-ordering of a lock's wait queue that satisfies given
 * constraints about certain procs preceding others.  (Each such constraint
 * is a fact of a partial ordering.)  Minimize rearrangement of the queue
 * not needed to achieve the partial ordering.
 *
 * This is a lot simpler and slower than, for example, the topological sort
 * algorithm shown in Knuth's Volume 1.  However, Knuth's method doesn't
 * try to minimize the damage to the existing order.  In practice we are
 * not likely to be working with more than a few constraints, so the apparent
 * slowness of the algorithm won't really matter.
 *
 * The initial queue ordering is taken directly from the lock's wait queue.
 * The output is an array of PGPROC pointers, of length equal to the lock's
 * wait queue length (the caller is responsible for providing this space).
 * The partial order is specified by an array of EDGE structs.  Each EDGE
 * is one that we need to reverse, therefore the "waiter" must appear before
 * the "blocker" in the output array.  The EDGE array may well contain
 * edges associated with other locks; these should be ignored.
 *
 * Returns true if able to build an ordering that satisfies all the
 * constraints, false if not (there are contradictory constraints).
 */
unsafe fn TopoSort(
    lock: *mut LOCK,
    constraints: *mut EDGE,
    nConstraints: c_int,
    ordering: *mut *mut PGPROC, /* output argument */
) -> bool {
    let waitQueue: *mut dclist_head = &mut (*lock).waitProcs;
    let queue_size: c_int = dclist_count(waitQueue);
    let mut proc_: *mut PGPROC;
    let mut i: c_int;
    let mut j: c_int;
    let mut jj: c_int;
    let mut k: c_int;
    let mut kk: c_int;
    let mut last: c_int;
    let mut proc_iter: dlist_iter = std::mem::zeroed();

    /* First, fill topoProcs[] array with the procs in their current order */
    i = 0;
    dclist_foreach!(proc_iter, waitQueue, {
        proc_ = dlist_container!(PGPROC, links, proc_iter.cur);
        *topoProcs.add(i as usize) = proc_;
        i += 1;
    });
    assert!(i == queue_size);

    /*
     * Scan the constraints, and for each proc in the array, generate a count
     * of the number of constraints that say it must be before something else,
     * plus a list of the constraints that say it must be after something
     * else. The count for the j'th proc is stored in beforeConstraints[j],
     * and the head of its list in afterConstraints[j].  Each constraint
     * stores its list link in constraints[i].link (note any constraint will
     * be in just one list). The array index for the before-proc of the i'th
     * constraint is remembered in constraints[i].pred.
     *
     * Note that it's not necessarily the case that every constraint affects
     * this particular wait queue.  Prior to group locking, a process could be
     * waiting for at most one lock.  But a lock group can be waiting for
     * zero, one, or multiple locks.  Since topoProcs[] is an array of the
     * processes actually waiting, while constraints[] is an array of group
     * leaders, we've got to scan through topoProcs[] for each constraint,
     * checking whether both a waiter and a blocker for that group are
     * present.  If so, the constraint is relevant to this wait queue; if not,
     * it isn't.
     */
    MemSet(
        beforeConstraints as *mut c_void,
        0,
        queue_size as usize * size_of::<c_int>(),
    );
    MemSet(
        afterConstraints as *mut c_void,
        0,
        queue_size as usize * size_of::<c_int>(),
    );
    i = 0;
    while i < nConstraints {
        /*
         * Find a representative process that is on the lock queue and part of
         * the waiting lock group.  This may or may not be the leader, which
         * may or may not be waiting at all.  If there are any other processes
         * in the same lock group on the queue, set their number of
         * beforeConstraints to -1 to indicate that they should be emitted
         * with their groupmates rather than considered separately.
         *
         * In this loop and the similar one just below, it's critical that we
         * consistently select the same representative member of any one lock
         * group, so that all the constraints are associated with the same
         * proc, and the -1's are only associated with not-representative
         * members.  We select the last one in the topoProcs array.
         */
        proc_ = (*constraints.add(i as usize)).waiter;
        assert!(!proc_.is_null());
        jj = -1;
        j = queue_size;
        j -= 1;
        while j >= 0 {
            let waiter: *mut PGPROC = *topoProcs.add(j as usize);

            if waiter == proc_ || (*waiter).lockGroupLeader == proc_ {
                assert!((*waiter).waitLock == lock);
                if jj == -1 {
                    jj = j;
                } else {
                    assert!(*beforeConstraints.add(j as usize) <= 0);
                    *beforeConstraints.add(j as usize) = -1;
                }
            }
            j -= 1;
        }

        /* If no matching waiter, constraint is not relevant to this lock. */
        if jj < 0 {
            i += 1;
            continue;
        }

        /*
         * Similarly, find a representative process that is on the lock queue
         * and waiting for the blocking lock group.  Again, this could be the
         * leader but does not need to be.
         */
        proc_ = (*constraints.add(i as usize)).blocker;
        assert!(!proc_.is_null());
        kk = -1;
        k = queue_size;
        k -= 1;
        while k >= 0 {
            let blocker: *mut PGPROC = *topoProcs.add(k as usize);

            if blocker == proc_ || (*blocker).lockGroupLeader == proc_ {
                assert!((*blocker).waitLock == lock);
                if kk == -1 {
                    kk = k;
                } else {
                    assert!(*beforeConstraints.add(k as usize) <= 0);
                    *beforeConstraints.add(k as usize) = -1;
                }
            }
            k -= 1;
        }

        /* If no matching blocker, constraint is not relevant to this lock. */
        if kk < 0 {
            i += 1;
            continue;
        }

        assert!(*beforeConstraints.add(jj as usize) >= 0);
        *beforeConstraints.add(jj as usize) += 1; /* waiter must come before */
        /* add this constraint to list of after-constraints for blocker */
        (*constraints.add(i as usize)).pred = jj;
        (*constraints.add(i as usize)).link = *afterConstraints.add(kk as usize);
        *afterConstraints.add(kk as usize) = i + 1;
        i += 1;
    }

    /*--------------------
     * Now scan the topoProcs array backwards.  At each step, output the
     * last proc that has no remaining before-constraints plus any other
     * members of the same lock group; then decrease the beforeConstraints
     * count of each of the procs it was constrained against.
     * i = index of ordering[] entry we want to output this time
     * j = search index for topoProcs[]
     * k = temp for scanning constraint list for proc j
     * last = last non-null index in topoProcs (avoid redundant searches)
     *--------------------
     */
    last = queue_size - 1;
    i = queue_size - 1;
    while i >= 0 {
        let mut c: c_int;
        let mut nmatches: c_int = 0;

        /* Find next candidate to output */
        while (*topoProcs.add(last as usize)).is_null() {
            last -= 1;
        }
        j = last;
        while j >= 0 {
            if !(*topoProcs.add(j as usize)).is_null() && *beforeConstraints.add(j as usize) == 0 {
                break;
            }
            j -= 1;
        }

        /* If no available candidate, topological sort fails */
        if j < 0 {
            return false;
        }

        /*
         * Output everything in the lock group.  There's no point in
         * outputting an ordering where members of the same lock group are not
         * consecutive on the wait queue: if some other waiter is between two
         * requests that belong to the same group, then either it conflicts
         * with both of them and is certainly not a solution; or it conflicts
         * with at most one of them and is thus isomorphic to an ordering
         * where the group members are consecutive.
         */
        proc_ = *topoProcs.add(j as usize);
        if !(*proc_).lockGroupLeader.is_null() {
            proc_ = (*proc_).lockGroupLeader;
        }
        assert!(!proc_.is_null());
        c = 0;
        while c <= last {
            if *topoProcs.add(c as usize) == proc_
                || (!(*topoProcs.add(c as usize)).is_null()
                    && (**topoProcs.add(c as usize)).lockGroupLeader == proc_)
            {
                *ordering.add((i - nmatches) as usize) = *topoProcs.add(c as usize);
                *topoProcs.add(c as usize) = ptr::null_mut();
                nmatches += 1;
            }
            c += 1;
        }
        assert!(nmatches > 0);
        i -= nmatches;

        /* Update beforeConstraints counts of its predecessors */
        k = *afterConstraints.add(j as usize);
        while k > 0 {
            *beforeConstraints.add((*constraints.add((k - 1) as usize)).pred as usize) -= 1;
            k = (*constraints.add((k - 1) as usize)).link;
        }
    }

    /* Done */
    true
}

#[cfg(feature = "DEBUG_DEADLOCK")]
unsafe fn PrintLockQueue(lock: *mut LOCK, info: *const std::os::raw::c_char) {
    let waitQueue = &mut (*lock).waitProcs as *mut dclist_head;
    let mut proc_iter: dlist_iter = std::mem::zeroed();

    print!(
        "{} lock {:p} queue ",
        std::ffi::CStr::from_ptr(info).to_string_lossy(),
        lock
    );

    dclist_foreach!(proc_iter, waitQueue, {
        let proc_ = dlist_container!(PGPROC, links, proc_iter.cur);
        print!(" {}", (*proc_).pid);
    });
    println!();
    use std::io::Write;
    let _ = std::io::stdout().flush();
}

/*
 * Report a detected deadlock, with available details.
 */
pub unsafe fn DeadLockReport() {
    let mut clientbuf: StringInfoData = std::mem::zeroed(); /* errdetail for client */
    let mut logbuf: StringInfoData = std::mem::zeroed(); /* errdetail for server log */
    let mut locktagbuf: StringInfoData = std::mem::zeroed();
    let mut i: c_int;

    initStringInfo(&mut clientbuf);
    initStringInfo(&mut logbuf);
    initStringInfo(&mut locktagbuf);

    /* Generate the "waits for" lines sent to the client */
    i = 0;
    while i < nDeadlockDetails {
        let info: *mut DEADLOCK_INFO = deadlockDetails.add(i as usize);
        let nextpid: c_int;

        /* The last proc waits for the first one... */
        if i < nDeadlockDetails - 1 {
            nextpid = (*info.add(1)).pid;
        } else {
            nextpid = (*deadlockDetails).pid;
        }

        /* reset locktagbuf to hold next object description */
        resetStringInfo(&mut locktagbuf);

        DescribeLockTag(&mut locktagbuf, &(*info).locktag);

        if i > 0 {
            appendStringInfoChar(&mut clientbuf, b'\n' as c_char);
        }

        appendStringInfo!(
            &mut clientbuf,
            "Process {} waits for {} on {}; blocked by process {}.",
            (*info).pid,
            std::ffi::CStr::from_ptr(GetLockmodeName((*info).locktag.locktag_lockmethodid as c_int, (*info).lockmode)).to_string_lossy(),
            std::ffi::CStr::from_ptr(locktagbuf.data).to_string_lossy(),
            nextpid
        );
        i += 1;
    }

    /* Duplicate all the above for the server ... */
    appendBinaryStringInfo(&mut logbuf, clientbuf.data, clientbuf.len);

    /* ... and add info about query strings */
    i = 0;
    while i < nDeadlockDetails {
        let info: *mut DEADLOCK_INFO = deadlockDetails.add(i as usize);

        appendStringInfoChar(&mut logbuf, b'\n' as c_char);

        appendStringInfo!(
            &mut logbuf,
            "Process {}: {}",
            (*info).pid,
            std::ffi::CStr::from_ptr(pgstat_get_backend_current_activity((*info).pid, false)).to_string_lossy()
        );
        i += 1;
    }

    pgstat_report_deadlock();

    ereport!(ERROR, "deadlock detected");
    // errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
    // errdetail_internal("%s", clientbuf.data),
    // errdetail_log("%s", logbuf.data),
    // errhint("See server log for query details.")
}

/*
 * RememberSimpleDeadLock: set up info for DeadLockReport when ProcSleep
 * detects a trivial (two-way) deadlock.  proc1 wants to block for lockmode
 * on lock, but proc2 is already waiting and would be blocked by proc1.
 */
pub unsafe fn RememberSimpleDeadLock(
    proc1: *mut PGPROC,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    proc2: *mut PGPROC,
) {
    let mut info: *mut DEADLOCK_INFO = deadlockDetails;

    (*info).locktag = (*lock).tag;
    (*info).lockmode = lockmode;
    (*info).pid = (*proc1).pid;
    info = info.add(1);
    (*info).locktag = (*(*proc2).waitLock).tag;
    (*info).lockmode = (*proc2).waitLockMode;
    (*info).pid = (*proc2).pid;
    nDeadlockDetails = 2;
}

// Local stubs for unported helpers referenced above.
unsafe fn dclist_push_tail(head: *mut dclist_head, node: *mut crate::lib::ilist::dlist_node) {
    unimplemented!() // TODO: lib/ilist.h
}

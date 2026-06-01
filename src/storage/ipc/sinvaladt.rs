//! POSTGRES shared cache invalidation data manager.
//!
//! src/backend/storage/ipc/sinvaladt.c
//! merged with src/include/storage/sinvaladt.h
//!
//! The shared cache invalidation manager is responsible for transmitting
//! invalidation messages between backends.  Any message sent by any backend
//! must be delivered to all already-running backends before it can be
//! forgotten.  (If we run out of space, we instead deliver a "RESET"
//! message to backends that have fallen too far behind.)
//!
//! The struct type SharedInvalidationMessage, defining the contents of
//! a single message, is defined in sinval.h.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

/*
 * Conceptually, the shared cache invalidation messages are stored in an
 * infinite array, where maxMsgNum is the next array subscript to store a
 * submitted message in, minMsgNum is the smallest array subscript containing
 * a message not yet read by all backends, and we always have maxMsgNum >=
 * minMsgNum.  (They are equal when there are no messages pending.)  For each
 * active backend, there is a nextMsgNum pointer indicating the next message it
 * needs to read; we have maxMsgNum >= nextMsgNum >= minMsgNum for every
 * backend.
 *
 * (In the current implementation, minMsgNum is a lower bound for the
 * per-process nextMsgNum values, but it isn't rigorously kept equal to the
 * smallest nextMsgNum --- it may lag behind.  We only update it when
 * SICleanupQueue is called, and we try not to do that often.)
 *
 * In reality, the messages are stored in a circular buffer of MAXNUMMESSAGES
 * entries.  We translate MsgNum values into circular-buffer indexes by
 * computing MsgNum % MAXNUMMESSAGES (this should be fast as long as
 * MAXNUMMESSAGES is a constant and a power of 2).  As long as maxMsgNum
 * doesn't exceed minMsgNum by more than MAXNUMMESSAGES, we have enough space
 * in the buffer.  If the buffer does overflow, we recover by setting the
 * "reset" flag for each backend that has fallen too far behind.  A backend
 * that is in "reset" state is ignored while determining minMsgNum.  When
 * it does finally attempt to receive inval messages, it must discard all
 * its invalidatable state, since it won't know what it missed.
 *
 * To reduce the probability of needing resets, we send a "catchup" interrupt
 * to any backend that seems to be falling unreasonably far behind.  The
 * normal behavior is that at most one such interrupt is in flight at a time;
 * when a backend completes processing a catchup interrupt, it executes
 * SICleanupQueue, which will signal the next-furthest-behind backend if
 * needed.  This avoids undue contention from multiple backends all trying
 * to catch up at once.  However, the furthest-back backend might be stuck
 * in a state where it can't catch up.  Eventually it will get reset, so it
 * won't cause any more problems for anyone but itself.  But we don't want
 * to find that a bunch of other backends are now too close to the reset
 * threshold to be saved.  So SICleanupQueue is designed to occasionally
 * send extra catchup interrupts as the queue gets fuller, to backends that
 * are far behind and haven't gotten one yet.  As long as there aren't a lot
 * of "stuck" backends, we won't need a lot of extra interrupts, since ones
 * that aren't stuck will propagate their interrupts to the next guy.
 *
 * We would have problems if the MsgNum values overflow an integer, so
 * whenever minMsgNum exceeds MSGNUMWRAPAROUND, we subtract MSGNUMWRAPAROUND
 * from all the MsgNum variables simultaneously.  MSGNUMWRAPAROUND can be
 * large so that we don't need to do this often.  It must be a multiple of
 * MAXNUMMESSAGES so that the existing circular-buffer entries don't need
 * to be moved when we do it.
 *
 * Access to the shared sinval array is protected by two locks, SInvalReadLock
 * and SInvalWriteLock.  Readers take SInvalReadLock in shared mode; this
 * authorizes them to modify their own ProcState but not to modify or even
 * look at anyone else's.  When we need to perform array-wide updates,
 * such as in SICleanupQueue, we take SInvalReadLock in exclusive mode to
 * lock out all readers.  Writers take SInvalWriteLock (always in exclusive
 * mode) to serialize adding messages to the queue.  Note that a writer
 * can operate in parallel with one or more readers, because the writer
 * has no need to touch anyone's ProcState, except in the infrequent cases
 * when SICleanupQueue is needed.  The only point of overlap is that
 * the writer wants to change maxMsgNum while readers need to read it.
 * We deal with that by having a spinlock that readers must take for just
 * long enough to read maxMsgNum, while writers take it for just long enough
 * to write maxMsgNum.  (The exact rule is that you need the spinlock to
 * read maxMsgNum if you are not holding SInvalWriteLock, and you need the
 * spinlock to write maxMsgNum unless you are holding both locks.)
 *
 * Note: since maxMsgNum is an int and hence presumably atomically readable/
 * writable, the spinlock might seem unnecessary.  The reason it is needed
 * is to provide a memory barrier: we need to be sure that messages written
 * to the array are actually there before maxMsgNum is increased, and that
 * readers will see that data after fetching maxMsgNum.  Multiprocessors
 * that have weak memory-ordering guarantees can fail without the memory
 * barrier instructions that are included in the spinlock sequences.
 */

use crate::prelude::*;

use std::ffi::c_int;

// Cross-module dependencies that have already been ported.
use crate::access::rmgrdesc::standbydesc::SharedInvalidationMessage;
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::procnumber::ProcNumber;
use crate::utils::init::globals::{pid_t, MaxBackends, MyProcNumber, MyProcPid};

/*
 * Configurable parameters.
 *
 * MAXNUMMESSAGES: max number of shared-inval messages we can buffer.
 * Must be a power of 2 for speed.
 *
 * MSGNUMWRAPAROUND: how often to reduce MsgNum variables to avoid overflow.
 * Must be a multiple of MAXNUMMESSAGES.  Should be large.
 *
 * CLEANUP_MIN: the minimum number of messages that must be in the buffer
 * before we bother to call SICleanupQueue.
 *
 * CLEANUP_QUANTUM: how often (in messages) to call SICleanupQueue once
 * we exceed CLEANUP_MIN.  Should be a power of 2 for speed.
 *
 * SIG_THRESHOLD: the minimum number of messages a backend must have fallen
 * behind before we'll send it PROCSIG_CATCHUP_INTERRUPT.
 *
 * WRITE_QUANTUM: the max number of messages to push into the buffer per
 * iteration of SIInsertDataEntries.  Noncritical but should be less than
 * CLEANUP_QUANTUM, because we only consider calling SICleanupQueue once
 * per iteration.
 */

const MAXNUMMESSAGES: c_int = 4096;
const MSGNUMWRAPAROUND: c_int = MAXNUMMESSAGES * 262144;
const CLEANUP_MIN: c_int = MAXNUMMESSAGES / 2;
const CLEANUP_QUANTUM: c_int = MAXNUMMESSAGES / 16;
const SIG_THRESHOLD: c_int = MAXNUMMESSAGES / 2;
const WRITE_QUANTUM: c_int = 64;

/* Per-backend state in shared invalidation structure */
#[repr(C)]
pub struct ProcState {
    /* procPid is zero in an inactive ProcState array entry. */
    pub procPid: pid_t, /* PID of backend, for signaling */
    /* nextMsgNum is meaningless if procPid == 0 or resetState is true. */
    pub nextMsgNum: c_int,  /* next message number to read */
    pub resetState: bool,   /* backend needs to reset its state */
    pub signaled: bool,     /* backend has been sent catchup signal */
    pub hasMessages: bool,  /* backend has unread messages */

    /*
     * Backend only sends invalidations, never receives them. This only makes
     * sense for Startup process during recovery because it doesn't maintain a
     * relcache, yet it fires inval messages to allow query backends to see
     * schema changes.
     */
    pub sendOnly: bool, /* backend only sends, never receives */

    /*
     * Next LocalTransactionId to use for each idle backend slot.  We keep
     * this here because it is indexed by ProcNumber and it is convenient to
     * copy the value to and from local memory when MyProcNumber is set. It's
     * meaningless in an active ProcState entry.
     */
    pub nextLXID: LocalTransactionId,
}

/* Shared cache invalidation memory segment */
#[repr(C)]
pub struct SISeg {
    /*
     * General state information
     */
    pub minMsgNum: c_int,     /* oldest message still needed */
    pub maxMsgNum: c_int,     /* next message number to be assigned */
    pub nextThreshold: c_int, /* # of messages to call SICleanupQueue */

    pub msgnumLock: slock_t, /* spinlock protecting maxMsgNum */

    /*
     * Circular buffer holding shared-inval messages
     */
    pub buffer: [SharedInvalidationMessage; MAXNUMMESSAGES as usize],

    /*
     * Per-backend invalidation state info.
     *
     * 'procState' has NumProcStateSlots entries, and is indexed by pgprocno.
     * 'numProcs' is the number of slots currently in use, and 'pgprocnos' is
     * a dense array of their indexes, to speed up scanning all in-use slots.
     *
     * 'pgprocnos' is largely redundant with ProcArrayStruct->pgprocnos, but
     * having our separate copy avoids contention on ProcArrayLock, and allows
     * us to track only the processes that participate in shared cache
     * invalidations.
     */
    pub numProcs: c_int,
    pub pgprocnos: *mut c_int,
    pub procState: [ProcState; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * We reserve a slot for each possible ProcNumber, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time, except for
 * IO workers.)
 */
#[allow(non_snake_case)]
#[inline]
fn NumProcStateSlots() -> c_int {
    unsafe { MaxBackends + NUM_AUXILIARY_PROCS }
}

static mut shmInvalBuffer: *mut SISeg = std::ptr::null_mut(); /* pointer to the shared inval buffer */

static mut nextLocalTransactionId: LocalTransactionId = 0;

/*
 * SharedInvalShmemSize --- return shared-memory space needed
 */
pub unsafe fn SharedInvalShmemSize() -> Size {
    let mut size: Size;

    size = core::mem::offset_of!(SISeg, procState);
    size = add_size(
        size,
        mul_size(
            core::mem::size_of::<ProcState>(),
            NumProcStateSlots() as Size,
        ),
    ); /* procState */
    size = add_size(
        size,
        mul_size(core::mem::size_of::<c_int>(), NumProcStateSlots() as Size),
    ); /* pgprocnos */

    size
}

/*
 * SharedInvalShmemInit
 *		Create and initialize the SI message buffer
 */
pub unsafe fn SharedInvalShmemInit() {
    let i: c_int;
    let mut found: bool = false;

    /* Allocate space in shared memory */
    shmInvalBuffer = ShmemInitStruct(
        c"shmInvalBuffer".as_ptr(),
        SharedInvalShmemSize(),
        &mut found,
    ) as *mut SISeg;
    if found {
        return;
    }

    /* Clear message counters, save size of procState array, init spinlock */
    (*shmInvalBuffer).minMsgNum = 0;
    (*shmInvalBuffer).maxMsgNum = 0;
    (*shmInvalBuffer).nextThreshold = CLEANUP_MIN;
    SpinLockInit(&mut (*shmInvalBuffer).msgnumLock);

    /* The buffer[] array is initially all unused, so we need not fill it */

    /* Mark all backends inactive, and initialize nextLXID */
    let mut j: c_int = 0;
    while j < NumProcStateSlots() {
        let ps = (*shmInvalBuffer).procState.as_mut_ptr().add(j as usize);
        (*ps).procPid = 0; /* inactive */
        (*ps).nextMsgNum = 0; /* meaningless */
        (*ps).resetState = false;
        (*ps).signaled = false;
        (*ps).hasMessages = false;
        (*ps).nextLXID = InvalidLocalTransactionId;
        j += 1;
    }
    i = j;
    (*shmInvalBuffer).numProcs = 0;
    (*shmInvalBuffer).pgprocnos =
        (*shmInvalBuffer).procState.as_mut_ptr().add(i as usize) as *mut c_int;
}

/*
 * SharedInvalBackendInit
 *		Initialize a new backend to operate on the sinval buffer
 */
pub unsafe fn SharedInvalBackendInit(sendOnly: bool) {
    let stateP: *mut ProcState;
    let oldPid: pid_t;
    let segP: *mut SISeg = shmInvalBuffer;

    if MyProcNumber < 0 {
        elog!(ERROR, "MyProcNumber not set");
    }
    if MyProcNumber >= NumProcStateSlots() {
        elog!(
            PANIC,
            "unexpected MyProcNumber {} in SharedInvalBackendInit (max {})",
            MyProcNumber,
            NumProcStateSlots()
        );
    }
    stateP = (*segP).procState.as_mut_ptr().add(MyProcNumber as usize);

    /*
     * This can run in parallel with read operations, but not with write
     * operations, since SIInsertDataEntries relies on the pgprocnos array to
     * set hasMessages appropriately.
     */
    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    oldPid = (*stateP).procPid;
    if oldPid != 0 {
        LWLockRelease(SInvalWriteLock);
        elog!(
            ERROR,
            "sinval slot for backend {} is already in use by process {}",
            MyProcNumber,
            oldPid as c_int
        );
    }

    let np = (*shmInvalBuffer).numProcs;
    *(*shmInvalBuffer).pgprocnos.add(np as usize) = MyProcNumber;
    (*shmInvalBuffer).numProcs = np + 1;

    /* Fetch next local transaction ID into local memory */
    nextLocalTransactionId = (*stateP).nextLXID;

    /* mark myself active, with all extant messages already read */
    (*stateP).procPid = MyProcPid;
    (*stateP).nextMsgNum = (*segP).maxMsgNum;
    (*stateP).resetState = false;
    (*stateP).signaled = false;
    (*stateP).hasMessages = false;
    (*stateP).sendOnly = sendOnly;

    LWLockRelease(SInvalWriteLock);

    /* register exit routine to mark my entry inactive at exit */
    on_shmem_exit(CleanupInvalidationState, PointerGetDatum(segP as *const _));
}

/*
 * CleanupInvalidationState
 *		Mark the current backend as no longer active.
 *
 * This function is called via on_shmem_exit() during backend shutdown.
 *
 * arg is really of type "SISeg*".
 */
unsafe extern "C" fn CleanupInvalidationState(_status: c_int, arg: Datum) {
    let segP: *mut SISeg = DatumGetPointer(arg) as *mut SISeg;
    let stateP: *mut ProcState;
    let mut i: c_int;

    Assert!(PointerIsValid(segP));

    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    stateP = (*segP).procState.as_mut_ptr().add(MyProcNumber as usize);

    /* Update next local transaction ID for next holder of this proc number */
    (*stateP).nextLXID = nextLocalTransactionId;

    /* Mark myself inactive */
    (*stateP).procPid = 0;
    (*stateP).nextMsgNum = 0;
    (*stateP).resetState = false;
    (*stateP).signaled = false;

    i = (*segP).numProcs - 1;
    while i >= 0 {
        if *(*segP).pgprocnos.add(i as usize) == MyProcNumber {
            if i != (*segP).numProcs - 1 {
                *(*segP).pgprocnos.add(i as usize) =
                    *(*segP).pgprocnos.add(((*segP).numProcs - 1) as usize);
            }
            break;
        }
        i -= 1;
    }
    if i < 0 {
        elog!(PANIC, "could not find entry in sinval array");
    }
    (*segP).numProcs -= 1;

    LWLockRelease(SInvalWriteLock);
}

/*
 * SIInsertDataEntries
 *		Add new invalidation message(s) to the buffer.
 */
pub unsafe fn SIInsertDataEntries(data: *const SharedInvalidationMessage, mut n: c_int) {
    let segP: *mut SISeg = shmInvalBuffer;
    let mut data = data;

    /*
     * N can be arbitrarily large.  We divide the work into groups of no more
     * than WRITE_QUANTUM messages, to be sure that we don't hold the lock for
     * an unreasonably long time.  (This is not so much because we care about
     * letting in other writers, as that some just-caught-up backend might be
     * trying to do SICleanupQueue to pass on its signal, and we don't want it
     * to have to wait a long time.)  Also, we need to consider calling
     * SICleanupQueue every so often.
     */
    while n > 0 {
        let mut nthistime: c_int = Min(n, WRITE_QUANTUM);
        let numMsgs: c_int;
        let mut max: c_int;
        let mut i: c_int;

        n -= nthistime;

        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

        /*
         * If the buffer is full, we *must* acquire some space.  Clean the
         * queue and reset anyone who is preventing space from being freed.
         * Otherwise, clean the queue only when it's exceeded the next
         * fullness threshold.  We have to loop and recheck the buffer state
         * after any call of SICleanupQueue.
         */
        loop {
            let numMsgs_inner = (*segP).maxMsgNum - (*segP).minMsgNum;
            if numMsgs_inner + nthistime > MAXNUMMESSAGES
                || numMsgs_inner >= (*segP).nextThreshold
            {
                SICleanupQueue(true, nthistime);
            } else {
                break;
            }
        }
        let _ = numMsgs;

        /*
         * Insert new message(s) into proper slot of circular buffer
         */
        max = (*segP).maxMsgNum;
        while {
            let cont = nthistime > 0;
            nthistime -= 1;
            cont
        } {
            (*segP).buffer[(max % MAXNUMMESSAGES) as usize] = *data;
            data = data.add(1);
            max += 1;
        }

        /* Update current value of maxMsgNum using spinlock */
        SpinLockAcquire(&mut (*segP).msgnumLock);
        (*segP).maxMsgNum = max;
        SpinLockRelease(&mut (*segP).msgnumLock);

        /*
         * Now that the maxMsgNum change is globally visible, we give everyone
         * a swift kick to make sure they read the newly added messages.
         * Releasing SInvalWriteLock will enforce a full memory barrier, so
         * these (unlocked) changes will be committed to memory before we exit
         * the function.
         */
        i = 0;
        while i < (*segP).numProcs {
            let stateP: *mut ProcState = (*segP)
                .procState
                .as_mut_ptr()
                .add(*(*segP).pgprocnos.add(i as usize) as usize);

            (*stateP).hasMessages = true;
            i += 1;
        }

        LWLockRelease(SInvalWriteLock);
    }
}

/*
 * SIGetDataEntries
 *		get next SI message(s) for current backend, if there are any
 *
 * Possible return values:
 *	0:	 no SI message available
 *	n>0: next n SI messages have been extracted into data[]
 * -1:	 SI reset message extracted
 *
 * If the return value is less than the array size "datasize", the caller
 * can assume that there are no more SI messages after the one(s) returned.
 * Otherwise, another call is needed to collect more messages.
 *
 * NB: this can run in parallel with other instances of SIGetDataEntries
 * executing on behalf of other backends, since each instance will modify only
 * fields of its own backend's ProcState, and no instance will look at fields
 * of other backends' ProcStates.  We express this by grabbing SInvalReadLock
 * in shared mode.  Note that this is not exactly the normal (read-only)
 * interpretation of a shared lock! Look closely at the interactions before
 * allowing SInvalReadLock to be grabbed in shared mode for any other reason!
 *
 * NB: this can also run in parallel with SIInsertDataEntries.  It is not
 * guaranteed that we will return any messages added after the routine is
 * entered.
 *
 * Note: we assume that "datasize" is not so large that it might be important
 * to break our hold on SInvalReadLock into segments.
 */
pub unsafe fn SIGetDataEntries(data: *mut SharedInvalidationMessage, datasize: c_int) -> c_int {
    let segP: *mut SISeg;
    let stateP: *mut ProcState;
    let max: c_int;
    let mut n: c_int;

    segP = shmInvalBuffer;
    stateP = (*segP).procState.as_mut_ptr().add(MyProcNumber as usize);

    /*
     * Before starting to take locks, do a quick, unlocked test to see whether
     * there can possibly be anything to read.  On a multiprocessor system,
     * it's possible that this load could migrate backwards and occur before
     * we actually enter this function, so we might miss a sinval message that
     * was just added by some other processor.  But they can't migrate
     * backwards over a preceding lock acquisition, so it should be OK.  If we
     * haven't acquired a lock preventing against further relevant
     * invalidations, any such occurrence is not much different than if the
     * invalidation had arrived slightly later in the first place.
     */
    if !(*stateP).hasMessages {
        return 0;
    }

    LWLockAcquire(SInvalReadLock, LW_SHARED);

    /*
     * We must reset hasMessages before determining how many messages we're
     * going to read.  That way, if new messages arrive after we have
     * determined how many we're reading, the flag will get reset and we'll
     * notice those messages part-way through.
     *
     * Note that, if we don't end up reading all of the messages, we had
     * better be certain to reset this flag before exiting!
     */
    (*stateP).hasMessages = false;

    /* Fetch current value of maxMsgNum using spinlock */
    SpinLockAcquire(&mut (*segP).msgnumLock);
    max = (*segP).maxMsgNum;
    SpinLockRelease(&mut (*segP).msgnumLock);

    if (*stateP).resetState {
        /*
         * Force reset.  We can say we have dealt with any messages added
         * since the reset, as well; and that means we should clear the
         * signaled flag, too.
         */
        (*stateP).nextMsgNum = max;
        (*stateP).resetState = false;
        (*stateP).signaled = false;
        LWLockRelease(SInvalReadLock);
        return -1;
    }

    /*
     * Retrieve messages and advance backend's counter, until data array is
     * full or there are no more messages.
     *
     * There may be other backends that haven't read the message(s), so we
     * cannot delete them here.  SICleanupQueue() will eventually remove them
     * from the queue.
     */
    n = 0;
    while n < datasize && (*stateP).nextMsgNum < max {
        *data.add(n as usize) = (*segP).buffer[((*stateP).nextMsgNum % MAXNUMMESSAGES) as usize];
        n += 1;
        (*stateP).nextMsgNum += 1;
    }

    /*
     * If we have caught up completely, reset our "signaled" flag so that
     * we'll get another signal if we fall behind again.
     *
     * If we haven't caught up completely, reset the hasMessages flag so that
     * we see the remaining messages next time.
     */
    if (*stateP).nextMsgNum >= max {
        (*stateP).signaled = false;
    } else {
        (*stateP).hasMessages = true;
    }

    LWLockRelease(SInvalReadLock);
    n
}

/*
 * SICleanupQueue
 *		Remove messages that have been consumed by all active backends
 *
 * callerHasWriteLock is true if caller is holding SInvalWriteLock.
 * minFree is the minimum number of message slots to make free.
 *
 * Possible side effects of this routine include marking one or more
 * backends as "reset" in the array, and sending PROCSIG_CATCHUP_INTERRUPT
 * to some backend that seems to be getting too far behind.  We signal at
 * most one backend at a time, for reasons explained at the top of the file.
 *
 * Caution: because we transiently release write lock when we have to signal
 * some other backend, it is NOT guaranteed that there are still minFree
 * free message slots at exit.  Caller must recheck and perhaps retry.
 */
pub unsafe fn SICleanupQueue(callerHasWriteLock: bool, minFree: c_int) {
    let segP: *mut SISeg = shmInvalBuffer;
    let mut min: c_int;
    let mut minsig: c_int;
    let lowbound: c_int;
    let numMsgs: c_int;
    let mut i: c_int;
    let mut needSig: *mut ProcState = std::ptr::null_mut();

    /* Lock out all writers and readers */
    if !callerHasWriteLock {
        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
    }
    LWLockAcquire(SInvalReadLock, LW_EXCLUSIVE);

    /*
     * Recompute minMsgNum = minimum of all backends' nextMsgNum, identify the
     * furthest-back backend that needs signaling (if any), and reset any
     * backends that are too far back.  Note that because we ignore sendOnly
     * backends here it is possible for them to keep sending messages without
     * a problem even when they are the only active backend.
     */
    min = (*segP).maxMsgNum;
    minsig = min - SIG_THRESHOLD;
    lowbound = min - MAXNUMMESSAGES + minFree;

    i = 0;
    while i < (*segP).numProcs {
        let stateP: *mut ProcState = (*segP)
            .procState
            .as_mut_ptr()
            .add(*(*segP).pgprocnos.add(i as usize) as usize);
        let nmsg = (*stateP).nextMsgNum;

        /* Ignore if already in reset state */
        Assert!((*stateP).procPid != 0);
        if (*stateP).resetState || (*stateP).sendOnly {
            i += 1;
            continue;
        }

        /*
         * If we must free some space and this backend is preventing it, force
         * him into reset state and then ignore until he catches up.
         */
        if nmsg < lowbound {
            (*stateP).resetState = true;
            /* no point in signaling him ... */
            i += 1;
            continue;
        }

        /* Track the global minimum nextMsgNum */
        if nmsg < min {
            min = nmsg;
        }

        /* Also see who's furthest back of the unsignaled backends */
        if nmsg < minsig && !(*stateP).signaled {
            minsig = nmsg;
            needSig = stateP;
        }
        i += 1;
    }
    (*segP).minMsgNum = min;

    /*
     * When minMsgNum gets really large, decrement all message counters so as
     * to forestall overflow of the counters.  This happens seldom enough that
     * folding it into the previous loop would be a loser.
     */
    if min >= MSGNUMWRAPAROUND {
        (*segP).minMsgNum -= MSGNUMWRAPAROUND;
        (*segP).maxMsgNum -= MSGNUMWRAPAROUND;
        i = 0;
        while i < (*segP).numProcs {
            (*(*segP)
                .procState
                .as_mut_ptr()
                .add(*(*segP).pgprocnos.add(i as usize) as usize))
            .nextMsgNum -= MSGNUMWRAPAROUND;
            i += 1;
        }
    }

    /*
     * Determine how many messages are still in the queue, and set the
     * threshold at which we should repeat SICleanupQueue().
     */
    numMsgs = (*segP).maxMsgNum - (*segP).minMsgNum;
    if numMsgs < CLEANUP_MIN {
        (*segP).nextThreshold = CLEANUP_MIN;
    } else {
        (*segP).nextThreshold = (numMsgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM;
    }

    /*
     * Lastly, signal anyone who needs a catchup interrupt.  Since
     * SendProcSignal() might not be fast, we don't want to hold locks while
     * executing it.
     */
    if !needSig.is_null() {
        let his_pid: pid_t = (*needSig).procPid;
        let his_procNumber: ProcNumber =
            needSig.offset_from((*segP).procState.as_ptr()) as ProcNumber;

        (*needSig).signaled = true;
        LWLockRelease(SInvalReadLock);
        LWLockRelease(SInvalWriteLock);
        elog!(DEBUG4, "sending sinval catchup signal to PID {}", his_pid as c_int);
        SendProcSignal(his_pid, PROCSIG_CATCHUP_INTERRUPT, his_procNumber);
        if callerHasWriteLock {
            LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
        }
    } else {
        LWLockRelease(SInvalReadLock);
        if !callerHasWriteLock {
            LWLockRelease(SInvalWriteLock);
        }
    }
}

/*
 * GetNextLocalTransactionId --- allocate a new LocalTransactionId
 *
 * We split VirtualTransactionIds into two parts so that it is possible
 * to allocate a new one without any contention for shared memory, except
 * for a bit of additional overhead during backend startup/shutdown.
 * The high-order part of a VirtualTransactionId is a ProcNumber, and the
 * low-order part is a LocalTransactionId, which we assign from a local
 * counter.  To avoid the risk of a VirtualTransactionId being reused
 * within a short interval, successive procs occupying the same PGPROC slot
 * should use a consecutive sequence of local IDs, which is implemented
 * by copying nextLocalTransactionId as seen above.
 */
pub unsafe fn GetNextLocalTransactionId() -> LocalTransactionId {
    let mut result: LocalTransactionId;

    /* loop to avoid returning InvalidLocalTransactionId at wraparound */
    loop {
        result = nextLocalTransactionId;
        nextLocalTransactionId = nextLocalTransactionId.wrapping_add(1);
        if LocalTransactionIdIsValid(result) {
            break;
        }
    }

    result
}

/* ---- Local stubs for unported dependencies ---- */

/*
 * Number of auxiliary process slots (proc.h).  Not yet ported as a shared
 * constant; mirror the value used elsewhere in the tree.
 */
const NUM_AUXILIARY_PROCS: c_int = 6;

/* LWLock and its acquisition modes live in storage/lwlock.h (not ported yet). */
pub type LWLock = c_void;
pub type LWLockMode = c_int;
const LW_EXCLUSIVE: LWLockMode = 0;
const LW_SHARED: LWLockMode = 1;

/* Named individual LWLocks (lwlocknames.h, generated; not ported yet). */
static mut SInvalReadLock: *mut LWLock = std::ptr::null_mut();
static mut SInvalWriteLock: *mut LWLock = std::ptr::null_mut();

/* Process-signal reasons (storage/procsignal.h, not ported yet). */
type ProcSignalReason = c_int;
const PROCSIG_CATCHUP_INTERRUPT: ProcSignalReason = 0;

/* Invalid LocalTransactionId sentinel (transam/lmgr defines; not ported yet). */
const InvalidLocalTransactionId: LocalTransactionId = 0;

/* Size arithmetic helpers (storage/shmem.h, not ported yet). */
#[inline]
fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}

#[inline]
fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

unsafe fn ShmemInitStruct(name: *const c_char, size: Size, found_ptr: *mut bool) -> *mut c_void {
    let _ = (name, size, found_ptr);
    unimplemented!() // TODO: storage/ipc/shmem.c
}

unsafe fn SpinLockInit(lock: *mut slock_t) {
    let _ = lock;
    unimplemented!() // TODO: storage/lmgr/s_lock.c
}

unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    let _ = lock;
    unimplemented!() // TODO: storage/lmgr/s_lock.c
}

unsafe fn SpinLockRelease(lock: *mut slock_t) {
    let _ = lock;
    unimplemented!() // TODO: storage/lmgr/s_lock.c
}

unsafe fn LWLockAcquire(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let _ = (lock, mode);
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn LWLockRelease(lock: *mut LWLock) {
    let _ = lock;
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn on_shmem_exit(function: unsafe extern "C" fn(c_int, Datum), arg: Datum) {
    let _ = (function, arg);
    unimplemented!() // TODO: storage/ipc/ipc.c
}

unsafe fn SendProcSignal(pid: pid_t, reason: ProcSignalReason, procNumber: ProcNumber) -> c_int {
    let _ = (pid, reason, procNumber);
    unimplemented!() // TODO: storage/ipc/procsignal.c
}

#[allow(non_snake_case)]
unsafe fn LocalTransactionIdIsValid(lxid: LocalTransactionId) -> bool {
    lxid != InvalidLocalTransactionId
}

#[allow(non_snake_case)]
unsafe fn PointerIsValid<T>(ptr: *const T) -> bool {
    !ptr.is_null()
}

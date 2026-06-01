//! src/backend/storage/ipc/procsignal.c
//!
//! procsignal.c
//!   Routines for interprocess signaling
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/ipc/procsignal.c

use crate::prelude::*;

use std::ffi::{c_int, c_void};

// ---------------------------------------------------------------------------
// procsignal.h
//   src/include/storage/procsignal.h
// ---------------------------------------------------------------------------

/*
 * Reasons for signaling a Postgres child process (a backend or an auxiliary
 * process, like checkpointer).  We can cope with concurrent signals for different
 * reasons.  However, if the same reason is signaled multiple times in quick
 * succession, the process is likely to observe only one notification of it.
 * This is okay for the present uses.
 *
 * Also, because of race conditions, it's important that all the signals be
 * defined so that no harm is done if a process mistakenly receives one.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProcSignalReason {
    PROCSIG_CATCHUP_INTERRUPT = 0, /* sinval catchup interrupt */
    PROCSIG_NOTIFY_INTERRUPT,      /* listen/notify interrupt */
    PROCSIG_PARALLEL_MESSAGE,      /* message from cooperating parallel backend */
    PROCSIG_WALSND_INIT_STOPPING,  /* ask walsenders to prepare for shutdown  */
    PROCSIG_BARRIER,               /* global barrier interrupt  */
    PROCSIG_LOG_MEMORY_CONTEXT,    /* ask backend to log the memory contexts */
    PROCSIG_PARALLEL_APPLY_MESSAGE, /* Message from parallel apply workers */

    /* Recovery conflict reasons */
    /* PROCSIG_RECOVERY_CONFLICT_FIRST == PROCSIG_RECOVERY_CONFLICT_DATABASE */
    PROCSIG_RECOVERY_CONFLICT_DATABASE,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
    PROCSIG_RECOVERY_CONFLICT_LOCK,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
    PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
    /* PROCSIG_RECOVERY_CONFLICT_LAST == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK */
}
use ProcSignalReason::*;

pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason =
    PROCSIG_RECOVERY_CONFLICT_DATABASE;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason =
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK;

pub const NUM_PROCSIGNALS: usize =
    (PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK as usize) + 1;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProcSignalBarrierType {
    PROCSIGNAL_BARRIER_SMGRRELEASE = 0, /* ask smgr to close files */
}
use ProcSignalBarrierType::*;

/*
 * Length of query cancel keys generated.
 *
 * Note that the protocol allows for longer keys, or shorter, but this is the
 * length we actually generate.  Client code, and the server code that handles
 * incoming cancellation packets from clients, mustn't use this hardcoded
 * length.
 */
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;

// ---------------------------------------------------------------------------
// procsignal.c
// ---------------------------------------------------------------------------

/*
 * The SIGUSR1 signal is multiplexed to support signaling multiple event
 * types. The specific reason is communicated via flags in shared memory.
 * We keep a boolean flag for each possible "reason", so that different
 * reasons can be signaled to a process concurrently.  (However, if the same
 * reason is signaled more than once nearly simultaneously, the process may
 * observe it only once.)
 *
 * Each process that wants to receive signals registers its process ID
 * in the ProcSignalSlots array. The array is indexed by ProcNumber to make
 * slot allocation simple, and to avoid having to search the array when you
 * know the ProcNumber of the process you're signaling.  (We do support
 * signaling without ProcNumber, but it's a bit less efficient.)
 *
 * The fields in each slot are protected by a spinlock, pss_mutex. pss_pid can
 * also be read without holding the spinlock, as a quick preliminary check
 * when searching for a particular PID in the array.
 *
 * pss_signalFlags are intended to be set in cases where we don't need to
 * keep track of whether or not the target process has handled the signal,
 * but sometimes we need confirmation, as when making a global state change
 * that cannot be considered complete until all backends have taken notice
 * of it. For such use cases, we set a bit in pss_barrierCheckMask and then
 * increment the current "barrier generation"; when the new barrier generation
 * (or greater) appears in the pss_barrierGeneration flag of every process,
 * we know that the message has been received everywhere.
 */
#[repr(C)]
pub struct ProcSignalSlot {
    pub pss_pid: pg_atomic_uint32,
    pub pss_cancel_key_len: c_int, /* 0 means no cancellation is possible */
    pub pss_cancel_key: [uint8; MAX_CANCEL_KEY_LENGTH],
    pub pss_signalFlags: [sig_atomic_t; NUM_PROCSIGNALS], /* volatile */
    pub pss_mutex: slock_t, /* protects the above fields */

    /* Barrier-related fields (not protected by pss_mutex) */
    pub pss_barrierGeneration: pg_atomic_uint64,
    pub pss_barrierCheckMask: pg_atomic_uint32,
    pub pss_barrierCV: ConditionVariable,
}

/*
 * Information that is global to the entire ProcSignal system can be stored
 * here.
 *
 * psh_barrierGeneration is the highest barrier generation in existence.
 */
#[repr(C)]
pub struct ProcSignalHeader {
    pub psh_barrierGeneration: pg_atomic_uint64,
    pub psh_slot: [ProcSignalSlot; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * We reserve a slot for each possible ProcNumber, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time, except for
 * IO workers.)
 */
#[allow(non_snake_case)]
#[inline]
unsafe fn NumProcSignalSlots() -> c_int {
    MaxBackends + NUM_AUXILIARY_PROCS
}

/* Check whether the relevant type bit is set in the flags. */
macro_rules! BARRIER_SHOULD_CHECK {
    ($flags:expr, $type:expr) => {
        (($flags) & ((1u32) << ($type as u32))) != 0
    };
}
#[allow(unused_imports)]
pub(crate) use BARRIER_SHOULD_CHECK;

/* Clear the relevant type bit from the flags. */
macro_rules! BARRIER_CLEAR_BIT {
    ($flags:expr, $type:expr) => {
        $flags &= !((1u32) << ($type as u32))
    };
}

/* NON_EXEC_STATIC */
pub static mut ProcSignal: *mut ProcSignalHeader = std::ptr::null_mut();
static mut MyProcSignalSlot: *mut ProcSignalSlot = std::ptr::null_mut();

/*
 * ProcSignalShmemSize
 *		Compute space needed for ProcSignal's shared memory
 */
pub unsafe fn ProcSignalShmemSize() -> Size {
    let mut size: Size;

    size = mul_size(NumProcSignalSlots() as Size, size_of::<ProcSignalSlot>());
    size = add_size(size, core::mem::offset_of!(ProcSignalHeader, psh_slot));
    size
}

/*
 * ProcSignalShmemInit
 *		Allocate and initialize ProcSignal's shared memory
 */
pub unsafe fn ProcSignalShmemInit() {
    let size: Size = ProcSignalShmemSize();
    let mut found: bool = false;

    ProcSignal = ShmemInitStruct(
        c"ProcSignal".as_ptr(),
        size,
        &mut found,
    ) as *mut ProcSignalHeader;

    /* If we're first, initialize. */
    if !found {
        pg_atomic_init_u64(&mut (*ProcSignal).psh_barrierGeneration, 0);

        let mut i: c_int = 0;
        while i < NumProcSignalSlots() {
            let slot: *mut ProcSignalSlot =
                (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize);

            SpinLockInit(&mut (*slot).pss_mutex);
            pg_atomic_init_u32(&mut (*slot).pss_pid, 0);
            (*slot).pss_cancel_key_len = 0;
            MemSet(
                (*slot).pss_signalFlags.as_mut_ptr() as *mut c_void,
                0,
                size_of_val(&(*slot).pss_signalFlags),
            );
            pg_atomic_init_u64(&mut (*slot).pss_barrierGeneration, PG_UINT64_MAX);
            pg_atomic_init_u32(&mut (*slot).pss_barrierCheckMask, 0);
            ConditionVariableInit(&mut (*slot).pss_barrierCV);

            i += 1;
        }
    }
}

/*
 * ProcSignalInit
 *		Register the current process in the ProcSignal array
 */
pub unsafe fn ProcSignalInit(cancel_key: *const uint8, cancel_key_len: c_int) {
    let slot: *mut ProcSignalSlot;
    let barrier_generation: uint64;
    let old_pss_pid: uint32;

    Assert!(cancel_key_len >= 0 && cancel_key_len as usize <= MAX_CANCEL_KEY_LENGTH);
    if MyProcNumber < 0 {
        elog!(ERROR, "MyProcNumber not set");
        unreachable!();
    }
    if MyProcNumber >= NumProcSignalSlots() {
        elog!(
            ERROR,
            "unexpected MyProcNumber {} in ProcSignalInit (max {})",
            MyProcNumber,
            NumProcSignalSlots()
        );
        unreachable!();
    }
    slot = (*ProcSignal).psh_slot.as_mut_ptr().add(MyProcNumber as usize);

    SpinLockAcquire(&mut (*slot).pss_mutex);

    /* Value used for sanity check below */
    old_pss_pid = pg_atomic_read_u32(&mut (*slot).pss_pid);

    /* Clear out any leftover signal reasons */
    MemSet(
        (*slot).pss_signalFlags.as_mut_ptr() as *mut c_void,
        0,
        NUM_PROCSIGNALS * size_of::<sig_atomic_t>(),
    );

    /*
     * Initialize barrier state. Since we're a brand-new process, there
     * shouldn't be any leftover backend-private state that needs to be
     * updated. Therefore, we can broadcast the latest barrier generation and
     * disregard any previously-set check bits.
     *
     * NB: This only works if this initialization happens early enough in the
     * startup sequence that we haven't yet cached any state that might need
     * to be invalidated. That's also why we have a memory barrier here, to be
     * sure that any later reads of memory happen strictly after this.
     */
    pg_atomic_write_u32(&mut (*slot).pss_barrierCheckMask, 0);
    barrier_generation =
        pg_atomic_read_u64(&mut (*ProcSignal).psh_barrierGeneration);
    pg_atomic_write_u64(&mut (*slot).pss_barrierGeneration, barrier_generation);

    if cancel_key_len > 0 {
        memcpy(
            (*slot).pss_cancel_key.as_mut_ptr() as *mut c_void,
            cancel_key as *const c_void,
            cancel_key_len as Size,
        );
    }
    (*slot).pss_cancel_key_len = cancel_key_len;
    pg_atomic_write_u32(&mut (*slot).pss_pid, MyProcPid as uint32);

    SpinLockRelease(&mut (*slot).pss_mutex);

    /* Spinlock is released, do the check */
    if old_pss_pid != 0 {
        elog!(
            LOG,
            "process {} taking over ProcSignal slot {}, but it's not empty",
            MyProcPid,
            MyProcNumber
        );
    }

    /* Remember slot location for CheckProcSignal */
    MyProcSignalSlot = slot;

    /* Set up to release the slot on process exit */
    on_shmem_exit(CleanupProcSignalState, 0 as Datum);
}

/*
 * CleanupProcSignalState
 *		Remove current process from ProcSignal mechanism
 *
 * This function is called via on_shmem_exit() during backend shutdown.
 */
unsafe extern "C" fn CleanupProcSignalState(_status: c_int, _arg: Datum) {
    let old_pid: pid_t;
    let slot: *mut ProcSignalSlot = MyProcSignalSlot;

    /*
     * Clear MyProcSignalSlot, so that a SIGUSR1 received after this point
     * won't try to access it after it's no longer ours (and perhaps even
     * after we've unmapped the shared memory segment).
     */
    Assert!(!MyProcSignalSlot.is_null());
    MyProcSignalSlot = std::ptr::null_mut();

    /* sanity check */
    SpinLockAcquire(&mut (*slot).pss_mutex);
    old_pid = pg_atomic_read_u32(&mut (*slot).pss_pid) as pid_t;
    if old_pid != MyProcPid {
        /*
         * don't ERROR here. We're exiting anyway, and don't want to get into
         * infinite loop trying to exit
         */
        SpinLockRelease(&mut (*slot).pss_mutex);
        elog!(
            LOG,
            "process {} releasing ProcSignal slot {}, but it contains {}",
            MyProcPid,
            slot.offset_from((*ProcSignal).psh_slot.as_ptr()) as c_int,
            old_pid as c_int
        );
        return; /* XXX better to zero the slot anyway? */
    }

    /* Mark the slot as unused */
    pg_atomic_write_u32(&mut (*slot).pss_pid, 0);
    (*slot).pss_cancel_key_len = 0;

    /*
     * Make this slot look like it's absorbed all possible barriers, so that
     * no barrier waits block on it.
     */
    pg_atomic_write_u64(&mut (*slot).pss_barrierGeneration, PG_UINT64_MAX);

    SpinLockRelease(&mut (*slot).pss_mutex);

    ConditionVariableBroadcast(&mut (*slot).pss_barrierCV);
}

/*
 * SendProcSignal
 *		Send a signal to a Postgres process
 *
 * Providing procNumber is optional, but it will speed up the operation.
 *
 * On success (a signal was sent), zero is returned.
 * On error, -1 is returned, and errno is set (typically to ESRCH or EPERM).
 *
 * Not to be confused with ProcSendSignal
 */
pub unsafe fn SendProcSignal(
    pid: pid_t,
    reason: ProcSignalReason,
    procNumber: ProcNumber,
) -> c_int {
    let slot: *mut ProcSignalSlot; /* volatile */

    if procNumber != INVALID_PROC_NUMBER {
        Assert!(procNumber < NumProcSignalSlots());
        slot = (*ProcSignal).psh_slot.as_mut_ptr().add(procNumber as usize);

        SpinLockAcquire(&mut (*slot).pss_mutex);
        if pg_atomic_read_u32(&mut (*slot).pss_pid) == pid as uint32 {
            /* Atomically set the proper flag */
            (*slot).pss_signalFlags[reason as usize] = true as sig_atomic_t;
            SpinLockRelease(&mut (*slot).pss_mutex);
            /* Send signal */
            return kill(pid, SIGUSR1);
        }
        SpinLockRelease(&mut (*slot).pss_mutex);
    } else {
        /*
         * procNumber not provided, so search the array using pid.  We search
         * the array back to front so as to reduce search overhead.  Passing
         * INVALID_PROC_NUMBER means that the target is most likely an
         * auxiliary process, which will have a slot near the end of the
         * array.
         */
        let mut i: c_int = NumProcSignalSlots() - 1;
        while i >= 0 {
            let slot: *mut ProcSignalSlot =
                (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize);

            if pg_atomic_read_u32(&mut (*slot).pss_pid) == pid as uint32 {
                SpinLockAcquire(&mut (*slot).pss_mutex);
                if pg_atomic_read_u32(&mut (*slot).pss_pid) == pid as uint32 {
                    /* Atomically set the proper flag */
                    (*slot).pss_signalFlags[reason as usize] = true as sig_atomic_t;
                    SpinLockRelease(&mut (*slot).pss_mutex);
                    /* Send signal */
                    return kill(pid, SIGUSR1);
                }
                SpinLockRelease(&mut (*slot).pss_mutex);
            }

            i -= 1;
        }
    }

    set_errno(ESRCH);
    -1
}

/*
 * EmitProcSignalBarrier
 *		Send a signal to every Postgres process
 *
 * The return value of this function is the barrier "generation" created
 * by this operation. This value can be passed to WaitForProcSignalBarrier
 * to wait until it is known that every participant in the ProcSignal
 * mechanism has absorbed the signal (or started afterwards).
 *
 * Note that it would be a bad idea to use this for anything that happens
 * frequently, as interrupting every backend could cause a noticeable
 * performance hit.
 *
 * Callers are entitled to assume that this function will not throw ERROR
 * or FATAL.
 */
pub unsafe fn EmitProcSignalBarrier(type_: ProcSignalBarrierType) -> uint64 {
    let flagbit: uint32 = 1u32 << (type_ as u32);
    let generation: uint64;

    /*
     * Set all the flags.
     *
     * Note that pg_atomic_fetch_or_u32 has full barrier semantics, so this is
     * totally ordered with respect to anything the caller did before, and
     * anything that we do afterwards. (This is also true of the later call to
     * pg_atomic_add_fetch_u64.)
     */
    let mut i: c_int = 0;
    while i < NumProcSignalSlots() {
        let slot: *mut ProcSignalSlot =
            (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize); /* volatile */

        pg_atomic_fetch_or_u32(&mut (*slot).pss_barrierCheckMask, flagbit);
        i += 1;
    }

    /*
     * Increment the generation counter.
     */
    generation =
        pg_atomic_add_fetch_u64(&mut (*ProcSignal).psh_barrierGeneration, 1);

    /*
     * Signal all the processes, so that they update their advertised barrier
     * generation.
     *
     * Concurrency is not a problem here. Backends that have exited don't
     * matter, and new backends that have joined since we entered this
     * function must already have current state, since the caller is
     * responsible for making sure that the relevant state is entirely visible
     * before calling this function in the first place. We still have to wake
     * them up - because we can't distinguish between such backends and older
     * backends that need to update state - but they won't actually need to
     * change any state.
     */
    let mut i: c_int = NumProcSignalSlots() - 1;
    while i >= 0 {
        let slot: *mut ProcSignalSlot =
            (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize); /* volatile */
        let mut pid: pid_t = pg_atomic_read_u32(&mut (*slot).pss_pid) as pid_t;

        if pid != 0 {
            SpinLockAcquire(&mut (*slot).pss_mutex);
            pid = pg_atomic_read_u32(&mut (*slot).pss_pid) as pid_t;
            if pid != 0 {
                /* see SendProcSignal for details */
                (*slot).pss_signalFlags[PROCSIG_BARRIER as usize] = true as sig_atomic_t;
                SpinLockRelease(&mut (*slot).pss_mutex);
                kill(pid, SIGUSR1);
            } else {
                SpinLockRelease(&mut (*slot).pss_mutex);
            }
        }

        i -= 1;
    }

    generation
}

/*
 * WaitForProcSignalBarrier - wait until it is guaranteed that all changes
 * requested by a specific call to EmitProcSignalBarrier() have taken effect.
 */
pub unsafe fn WaitForProcSignalBarrier(generation: uint64) {
    Assert!(generation <= pg_atomic_read_u64(&mut (*ProcSignal).psh_barrierGeneration));

    elog!(
        DEBUG1,
        "waiting for all backends to process ProcSignalBarrier generation {}",
        generation
    );

    let mut i: c_int = NumProcSignalSlots() - 1;
    while i >= 0 {
        let slot: *mut ProcSignalSlot =
            (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize);
        let mut oldval: uint64;

        /*
         * It's important that we check only pss_barrierGeneration here and
         * not pss_barrierCheckMask. Bits in pss_barrierCheckMask get cleared
         * before the barrier is actually absorbed, but pss_barrierGeneration
         * is updated only afterward.
         */
        oldval = pg_atomic_read_u64(&mut (*slot).pss_barrierGeneration);
        while oldval < generation {
            if ConditionVariableTimedSleep(
                &mut (*slot).pss_barrierCV,
                5000,
                WAIT_EVENT_PROC_SIGNAL_BARRIER,
            ) {
                elog!(
                    LOG,
                    "still waiting for backend with PID {} to accept ProcSignalBarrier",
                    pg_atomic_read_u32(&mut (*slot).pss_pid) as c_int
                );
            }
            oldval = pg_atomic_read_u64(&mut (*slot).pss_barrierGeneration);
        }
        ConditionVariableCancelSleep();

        i -= 1;
    }

    elog!(
        DEBUG1,
        "finished waiting for all backends to process ProcSignalBarrier generation {}",
        generation
    );

    /*
     * The caller is probably calling this function because it wants to read
     * the shared state or perform further writes to shared state once all
     * backends are known to have absorbed the barrier. However, the read of
     * pss_barrierGeneration was performed unlocked; insert a memory barrier
     * to separate it from whatever follows.
     */
    pg_memory_barrier();
}

/*
 * Handle receipt of an interrupt indicating a global barrier event.
 *
 * All the actual work is deferred to ProcessProcSignalBarrier(), because we
 * cannot safely access the barrier generation inside the signal handler as
 * 64bit atomics might use spinlock based emulation, even for reads. As this
 * routine only gets called when PROCSIG_BARRIER is sent that won't cause a
 * lot of unnecessary work.
 */
unsafe fn HandleProcSignalBarrierInterrupt() {
    InterruptPending = true;
    ProcSignalBarrierPending = true;
    /* latch will be set by procsignal_sigusr1_handler */
}

/*
 * Perform global barrier related interrupt checking.
 *
 * Any backend that participates in ProcSignal signaling must arrange to
 * call this function periodically. It is called from CHECK_FOR_INTERRUPTS(),
 * which is enough for normal backends, but not necessarily for all types of
 * background processes.
 */
pub unsafe fn ProcessProcSignalBarrier() {
    let local_gen: uint64;
    let shared_gen: uint64;
    let mut flags: uint32; /* volatile */

    Assert!(!MyProcSignalSlot.is_null());

    /* Exit quickly if there's no work to do. */
    if !ProcSignalBarrierPending {
        return;
    }
    ProcSignalBarrierPending = false;

    /*
     * It's not unlikely to process multiple barriers at once, before the
     * signals for all the barriers have arrived. To avoid unnecessary work in
     * response to subsequent signals, exit early if we already have processed
     * all of them.
     */
    local_gen = pg_atomic_read_u64(&mut (*MyProcSignalSlot).pss_barrierGeneration);
    shared_gen = pg_atomic_read_u64(&mut (*ProcSignal).psh_barrierGeneration);

    Assert!(local_gen <= shared_gen);

    if local_gen == shared_gen {
        return;
    }

    /*
     * Get and clear the flags that are set for this backend. Note that
     * pg_atomic_exchange_u32 is a full barrier, so we're guaranteed that the
     * read of the barrier generation above happens before we atomically
     * extract the flags, and that any subsequent state changes happen
     * afterward.
     *
     * NB: In order to avoid race conditions, we must zero
     * pss_barrierCheckMask first and only afterwards try to do barrier
     * processing. If we did it in the other order, someone could send us
     * another barrier of some type right after we called the
     * barrier-processing function but before we cleared the bit. We would
     * have no way of knowing that the bit needs to stay set in that case, so
     * the need to call the barrier-processing function again would just get
     * forgotten. So instead, we tentatively clear all the bits and then put
     * back any for which we don't manage to successfully absorb the barrier.
     */
    flags = pg_atomic_exchange_u32(&mut (*MyProcSignalSlot).pss_barrierCheckMask, 0);

    /*
     * If there are no flags set, then we can skip doing any real work.
     * Otherwise, establish a PG_TRY block, so that we don't lose track of
     * which types of barrier processing are needed if an ERROR occurs.
     */
    if flags != 0 {
        let mut success: bool = true;

        PG_TRY!(
            {
                /*
                 * Process each type of barrier. The barrier-processing functions
                 * should normally return true, but may return false if the
                 * barrier can't be absorbed at the current time. This should be
                 * rare, because it's pretty expensive.  Every single
                 * CHECK_FOR_INTERRUPTS() will return here until we manage to
                 * absorb the barrier, and that cost will add up in a hurry.
                 *
                 * NB: It ought to be OK to call the barrier-processing functions
                 * unconditionally, but it's more efficient to call only the ones
                 * that might need us to do something based on the flags.
                 */
                while flags != 0 {
                    let type_: ProcSignalBarrierType;
                    let mut processed: bool = true;

                    type_ = std::mem::transmute::<u32, ProcSignalBarrierType>(
                        pg_rightmost_one_pos32(flags),
                    );
                    match type_ {
                        PROCSIGNAL_BARRIER_SMGRRELEASE => {
                            processed = ProcessBarrierSmgrRelease();
                        }
                    }

                    /*
                     * To avoid an infinite loop, we must always unset the bit in
                     * flags.
                     */
                    BARRIER_CLEAR_BIT!(flags, type_);

                    /*
                     * If we failed to process the barrier, reset the shared bit
                     * so we try again later, and set a flag so that we don't bump
                     * our generation.
                     */
                    if !processed {
                        ResetProcSignalBarrierBits((1u32) << (type_ as u32));
                        success = false;
                    }
                }
            },
            {
                /*
                 * If an ERROR occurred, we'll need to try again later to handle
                 * that barrier type and any others that haven't been handled yet
                 * or weren't successfully absorbed.
                 */
                ResetProcSignalBarrierBits(flags);
                PG_RE_THROW!();
            }
        );

        /*
         * If some barrier types were not successfully absorbed, we will have
         * to try again later.
         */
        if !success {
            return;
        }
    }

    /*
     * State changes related to all types of barriers that might have been
     * emitted have now been handled, so we can update our notion of the
     * generation to the one we observed before beginning the updates. If
     * things have changed further, it'll get fixed up when this function is
     * next called.
     */
    pg_atomic_write_u64(&mut (*MyProcSignalSlot).pss_barrierGeneration, shared_gen);
    ConditionVariableBroadcast(&mut (*MyProcSignalSlot).pss_barrierCV);
}

/*
 * If it turns out that we couldn't absorb one or more barrier types, either
 * because the barrier-processing functions returned false or due to an error,
 * arrange for processing to be retried later.
 */
unsafe fn ResetProcSignalBarrierBits(flags: uint32) {
    pg_atomic_fetch_or_u32(&mut (*MyProcSignalSlot).pss_barrierCheckMask, flags);
    ProcSignalBarrierPending = true;
    InterruptPending = true;
}

/*
 * CheckProcSignal - check to see if a particular reason has been
 * signaled, and clear the signal flag.  Should be called after receiving
 * SIGUSR1.
 */
unsafe fn CheckProcSignal(reason: ProcSignalReason) -> bool {
    let slot: *mut ProcSignalSlot = MyProcSignalSlot; /* volatile */

    if !slot.is_null() {
        /*
         * Careful here --- don't clear flag if we haven't seen it set.
         * pss_signalFlags is of type "volatile sig_atomic_t" to allow us to
         * read it here safely, without holding the spinlock.
         */
        if (*slot).pss_signalFlags[reason as usize] != 0 {
            (*slot).pss_signalFlags[reason as usize] = false as sig_atomic_t;
            return true;
        }
    }

    false
}

/*
 * procsignal_sigusr1_handler - handle SIGUSR1 signal.
 */
pub unsafe extern "C" fn procsignal_sigusr1_handler(_postgres_signal_arg: c_int) {
    if CheckProcSignal(PROCSIG_CATCHUP_INTERRUPT) {
        HandleCatchupInterrupt();
    }

    if CheckProcSignal(PROCSIG_NOTIFY_INTERRUPT) {
        HandleNotifyInterrupt();
    }

    if CheckProcSignal(PROCSIG_PARALLEL_MESSAGE) {
        HandleParallelMessageInterrupt();
    }

    if CheckProcSignal(PROCSIG_WALSND_INIT_STOPPING) {
        HandleWalSndInitStopping();
    }

    if CheckProcSignal(PROCSIG_BARRIER) {
        HandleProcSignalBarrierInterrupt();
    }

    if CheckProcSignal(PROCSIG_LOG_MEMORY_CONTEXT) {
        HandleLogMemoryContextInterrupt();
    }

    if CheckProcSignal(PROCSIG_PARALLEL_APPLY_MESSAGE) {
        HandleParallelApplyMessageInterrupt();
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_DATABASE) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_DATABASE);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_TABLESPACE) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_TABLESPACE);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOCK) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_LOCK);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);
    }

    if CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN) {
        HandleRecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);
    }

    SetLatch(MyLatch);
}

/*
 * Send a query cancellation signal to backend.
 *
 * Note: This is called from a backend process before authentication.  We
 * cannot take LWLocks yet, but that's OK; we rely on atomic reads of the
 * fields in the ProcSignal slots.
 */
pub unsafe fn SendCancelRequest(
    backendPID: c_int,
    cancel_key: *const uint8,
    cancel_key_len: c_int,
) {
    if backendPID == 0 {
        ereport!(LOG, "invalid cancel request with PID 0");
        return;
    }

    /*
     * See if we have a matching backend. Reading the pss_pid and
     * pss_cancel_key fields is racy, a backend might die and remove itself
     * from the array at any time.  The probability of the cancellation key
     * matching wrong process is miniscule, however, so we can live with that.
     * PIDs are reused too, so sending the signal based on PID is inherently
     * racy anyway, although OS's avoid reusing PIDs too soon.
     */
    let mut i: c_int = 0;
    while i < NumProcSignalSlots() {
        let slot: *mut ProcSignalSlot =
            (*ProcSignal).psh_slot.as_mut_ptr().add(i as usize);
        let match_: bool;

        if pg_atomic_read_u32(&mut (*slot).pss_pid) != backendPID as uint32 {
            i += 1;
            continue;
        }

        /* Acquire the spinlock and re-check */
        SpinLockAcquire(&mut (*slot).pss_mutex);
        if pg_atomic_read_u32(&mut (*slot).pss_pid) != backendPID as uint32 {
            SpinLockRelease(&mut (*slot).pss_mutex);
            i += 1;
            continue;
        } else {
            match_ = (*slot).pss_cancel_key_len == cancel_key_len
                && timingsafe_bcmp(
                    (*slot).pss_cancel_key.as_ptr() as *const c_void,
                    cancel_key as *const c_void,
                    cancel_key_len as Size,
                ) == 0;

            SpinLockRelease(&mut (*slot).pss_mutex);

            if match_ {
                /* Found a match; signal that backend to cancel current op */
                ereport!(
                    DEBUG2,
                    errmsg_internal!(
                        "processing cancel request: sending SIGINT to process {}",
                        backendPID
                    )
                );

                /*
                 * If we have setsid(), signal the backend's whole process
                 * group
                 */
                #[cfg(unix)]
                {
                    kill(-backendPID, SIGINT);
                }
                #[cfg(not(unix))]
                {
                    kill(backendPID, SIGINT);
                }
            } else {
                /* Right PID, wrong key: no way, Jose */
                elog!(
                    LOG,
                    "wrong key in cancel request for process {}",
                    backendPID
                );
            }
            return;
        }
    }

    /* No matching backend */
    elog!(
        LOG,
        "PID {} in cancel request did not match any process",
        backendPID
    );
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies
// ---------------------------------------------------------------------------

pub type pid_t = c_int;

#[allow(non_camel_case_types)]
pub type sig_atomic_t = c_int;

#[allow(non_camel_case_types)]
pub type slock_t = std::ffi::c_uchar;

#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32, /* volatile */
}

#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64, /* volatile */
}

#[repr(C)]
pub struct ConditionVariable {
    _opaque: [u8; 0],
}

pub const PG_UINT64_MAX: uint64 = u64::MAX;

pub const SIGUSR1: c_int = 10;
pub const SIGINT: c_int = 2;
pub const ESRCH: c_int = 3;

pub const NUM_AUXILIARY_PROCS: c_int = 9;

pub const WAIT_EVENT_PROC_SIGNAL_BARRIER: uint32 = 0;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;

#[allow(non_camel_case_types)]
pub type ProcNumber = c_int;

extern "C" {
    pub static mut MaxBackends: c_int;
    pub static mut MyProcNumber: ProcNumber;
    pub static mut MyProcPid: c_int;
    pub static mut InterruptPending: bool;
    pub static mut ProcSignalBarrierPending: bool;
    pub static mut MyLatch: *mut c_void;

    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn kill(pid: pid_t, sig: c_int) -> c_int;
}

unsafe fn set_errno(_e: c_int) {
    // TODO: storage/ipc/procsignal.c -- set C errno
}

unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1.wrapping_mul(s2) // TODO: storage/ipc/shmem.c (overflow-checked)
}

unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1.wrapping_add(s2) // TODO: storage/ipc/shmem.c (overflow-checked)
}

unsafe fn MemSet(ptr: *mut c_void, val: c_int, len: Size) {
    std::ptr::write_bytes(ptr as *mut u8, val as u8, len);
}

unsafe fn SpinLockInit(_lock: *mut slock_t) {
    // TODO: storage/lmgr/spin.c
}
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    // TODO: storage/lmgr/spin.c
}
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    // TODO: storage/lmgr/spin.c
}

unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    (*ptr).value = val; // TODO: port/atomics.h
}
unsafe fn pg_atomic_init_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    (*ptr).value = val; // TODO: port/atomics.h
}
unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    (*ptr).value // TODO: port/atomics.h
}
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    (*ptr).value // TODO: port/atomics.h
}
unsafe fn pg_atomic_write_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    (*ptr).value = val; // TODO: port/atomics.h
}
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    (*ptr).value = val; // TODO: port/atomics.h
}
unsafe fn pg_atomic_fetch_or_u32(ptr: *mut pg_atomic_uint32, or_: u32) -> u32 {
    let old = (*ptr).value; // TODO: port/atomics.h
    (*ptr).value = old | or_;
    old
}
unsafe fn pg_atomic_add_fetch_u64(ptr: *mut pg_atomic_uint64, add_: i64) -> u64 {
    (*ptr).value = (*ptr).value.wrapping_add(add_ as u64); // TODO: port/atomics.h
    (*ptr).value
}
unsafe fn pg_atomic_exchange_u32(ptr: *mut pg_atomic_uint32, newval: u32) -> u32 {
    let old = (*ptr).value; // TODO: port/atomics.h
    (*ptr).value = newval;
    old
}
unsafe fn pg_memory_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst); // TODO: port/atomics.h
}

unsafe fn pg_rightmost_one_pos32(_word: uint32) -> u32 {
    unimplemented!() // TODO: port/pg_bitutils.h
}

unsafe fn ConditionVariableInit(_cv: *mut ConditionVariable) {
    // TODO: storage/lmgr/condition_variable.c
}
unsafe fn ConditionVariableBroadcast(_cv: *mut ConditionVariable) {
    // TODO: storage/lmgr/condition_variable.c
}
unsafe fn ConditionVariableCancelSleep() -> bool {
    unimplemented!() // TODO: storage/lmgr/condition_variable.c
}
unsafe fn ConditionVariableTimedSleep(
    _cv: *mut ConditionVariable,
    _timeout: c_long,
    _wait_event_info: uint32,
) -> bool {
    unimplemented!() // TODO: storage/lmgr/condition_variable.c
}

unsafe fn on_shmem_exit(_function: unsafe extern "C" fn(c_int, Datum), _arg: Datum) {
    // TODO: storage/ipc/ipc.c
}

unsafe fn timingsafe_bcmp(_b1: *const c_void, _b2: *const c_void, _len: Size) -> c_int {
    unimplemented!() // TODO: port/pg_strong_random.c
}

unsafe fn SetLatch(_latch: *mut c_void) {
    // TODO: storage/ipc/latch.c
}

unsafe fn ProcessBarrierSmgrRelease() -> bool {
    unimplemented!() // TODO: storage/smgr/smgr.c
}

unsafe fn HandleCatchupInterrupt() {
    unimplemented!() // TODO: storage/ipc/sinval.c
}
unsafe fn HandleNotifyInterrupt() {
    unimplemented!() // TODO: commands/async.c
}
unsafe fn HandleParallelMessageInterrupt() {
    unimplemented!() // TODO: access/transam/parallel.c
}
unsafe fn HandleWalSndInitStopping() {
    unimplemented!() // TODO: replication/walsender.c
}
unsafe fn HandleLogMemoryContextInterrupt() {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn HandleParallelApplyMessageInterrupt() {
    unimplemented!() // TODO: replication/logical/applyparallelworker.c
}
unsafe fn HandleRecoveryConflictInterrupt(_reason: ProcSignalReason) {
    unimplemented!() // TODO: tcop/postgres.c
}


// PG_TRY/PG_CATCH emulation stub: faithful structure, runs try body then
// optionally catch on unwind. Real version uses sigsetjmp (utils/elog.h).
macro_rules! PG_TRY {
    ($try_body:block, $catch_body:block) => {{
        $try_body
        // TODO: utils/elog.h -- PG_TRY/PG_CATCH (sigsetjmp); catch on error
        #[allow(unreachable_code)]
        if false {
            $catch_body
        }
    }};
}
use PG_TRY;

macro_rules! PG_RE_THROW {
    () => {
        unimplemented!() // TODO: utils/elog.h -- PG_RE_THROW
    };
}
use PG_RE_THROW;

macro_rules! errmsg_internal {
    ($($arg:tt)*) => {
        format!($($arg)*) // TODO: utils/elog.h -- errmsg_internal
    };
}
use errmsg_internal;

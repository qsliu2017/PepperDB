//! storage/ipc/pmsignal.c - signaling between postmaster and its children.

use crate::prelude::*;

use crate::miscadmin::pid_t;

// miscadmin.h / globals.c externs.
unsafe extern "C" {
    static mut IsUnderPostmaster: bool;
    static mut PostmasterPid: pid_t;
    static mut MyPMChildSlot: c_int;
}

// <signal.h>: kill(2). Returns 0 on success, -1 on error.
unsafe extern "C" {
    fn kill(pid: c_int, sig: c_int) -> c_int;
    // macOS errno location.
    fn __error() -> *mut c_int;
    // <unistd.h>: read(2).
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
}

#[inline]
unsafe fn errno_get() -> c_int {
    *__error()
}

// <signal.h> SIGUSR1.
const SIGUSR1: c_int = 30;
// <errno.h> EAGAIN / EWOULDBLOCK (macOS: both 35).
const EAGAIN: c_int = 35;
const EWOULDBLOCK: c_int = 35;

// storage/ipc.h: POSTMASTER_FD_WATCH index into postmaster_alive_fds[].
const POSTMASTER_FD_WATCH: usize = 1;

// pmsignal.h: PMSignalReason enum (count NUM_PMSIGNALS).
const NUM_PMSIGNALS: usize = 10;

// pmsignal.h: QuitSignalReason values.
pub type QuitSignalReason = c_int;
pub const PMQUIT_NOT_SENT: QuitSignalReason = 0;

// pmsignal.h: PMSignalReason is passed in as a plain index.
pub type PMSignalReason = c_int;

/*
 * Per-child-process flag states.  These values must fit in sig_atomic_t.
 */
const PM_CHILD_UNUSED: sig_atomic_t = 0;
const PM_CHILD_ASSIGNED: sig_atomic_t = 1;
const PM_CHILD_ACTIVE: sig_atomic_t = 2;
const PM_CHILD_WALSENDER: sig_atomic_t = 3;

// "volatile sig_atomic_t" maps to a plain int here.
type sig_atomic_t = c_int;

/*
 * struct PMSignalData (opaque outside pmsignal.c).
 *
 * PMChildFlags is a C flexible array member; in Rust we model it as a
 * zero-length array at the end of the struct.  Accesses go through raw
 * pointer arithmetic from the base of that array.
 */
#[repr(C)]
pub struct PMSignalData {
    /* per-reason flags for signaling the postmaster */
    PMSignalFlags: [sig_atomic_t; NUM_PMSIGNALS],
    /* global flags for signals from postmaster to children */
    sigquit_reason: QuitSignalReason,
    /* per-child-process flags */
    num_child_flags: c_int,
    PMChildFlags: [sig_atomic_t; 0],
}

/* PMSignalState pointer is valid in both postmaster and child processes */
pub static mut PMSignalState: *mut PMSignalData = null_mut();

/*
 * Local copy of PMSignalState->num_child_flags, only valid in the
 * postmaster.
 */
static mut num_child_flags: c_int = 0;

/*
 * On platforms without a parent-death signal mechanism (e.g. macOS, where
 * neither PR_SET_PDEATHSIG nor PROC_PDEATHSIG_CTL is available),
 * USE_POSTMASTER_DEATH_SIGNAL is not defined.  We keep the flag here for
 * completeness; the death-signal-handler paths are compiled out.
 */
const USE_POSTMASTER_DEATH_SIGNAL: bool = false;

/*
 * Signal handler flag to be notified if postmaster dies.
 */
pub static mut postmaster_possibly_dead: sig_atomic_t = 0; // false

// ---------------------------------------------------------------------------
// Local stubs for dependencies not yet ported. // TODO: deps not ported
// ---------------------------------------------------------------------------

// utils/memutils.h - add_size / mul_size (overflow-checking size arithmetic).
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

// storage/shmem.h - ShmemInitStruct.
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found_ptr: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: not ported
}

// storage/ipc.h - on_shmem_exit.
unsafe fn on_shmem_exit(_function: Option<unsafe extern "C" fn(c_int, Datum)>, _arg: Datum) {
    // TODO: not ported
}

// postmaster/pmchild.c - MaxLivePostmasterChildren.
unsafe fn MaxLivePostmasterChildren() -> c_int {
    unimplemented!() // TODO: not ported
}

// replication/walsender.h - am_walsender flag.
static mut am_walsender: bool = false;

// storage/ipc.h - postmaster_alive_fds[].
static mut postmaster_alive_fds: [c_int; 2] = [-1, -1];

// ---------------------------------------------------------------------------

/*
 * PMSignalShmemSize
 *		Compute space needed for pmsignal.c's shared memory
 */
pub unsafe fn PMSignalShmemSize() -> Size {
    let mut size: Size;

    size = core::mem::offset_of!(PMSignalData, PMChildFlags);
    size = add_size(
        size,
        mul_size(
            MaxLivePostmasterChildren() as Size,
            core::mem::size_of::<sig_atomic_t>(),
        ),
    );

    size
}

/*
 * PMSignalShmemInit - initialize during shared-memory creation
 */
pub unsafe fn PMSignalShmemInit() {
    let mut found: bool = false;

    PMSignalState = ShmemInitStruct(
        c"PMSignalState".as_ptr(),
        PMSignalShmemSize(),
        &raw mut found,
    ) as *mut PMSignalData;

    if !found {
        /* initialize all flags to zeroes */
        MemSet(PMSignalState as *mut c_void, 0, PMSignalShmemSize());
        num_child_flags = MaxLivePostmasterChildren();
        (*PMSignalState).num_child_flags = num_child_flags;
    }
}

/*
 * SendPostmasterSignal - signal the postmaster from a child process
 */
pub unsafe fn SendPostmasterSignal(reason: PMSignalReason) {
    /* If called in a standalone backend, do nothing */
    if !IsUnderPostmaster {
        return;
    }
    /* Atomically set the proper flag */
    (*PMSignalState).PMSignalFlags[reason as usize] = true as sig_atomic_t;
    /* Send signal to postmaster */
    kill(PostmasterPid, SIGUSR1);
}

/*
 * CheckPostmasterSignal - check to see if a particular reason has been
 * signaled, and clear the signal flag.  Should be called by postmaster
 * after receiving SIGUSR1.
 */
pub unsafe fn CheckPostmasterSignal(reason: PMSignalReason) -> bool {
    /* Careful here --- don't clear flag if we haven't seen it set */
    if (*PMSignalState).PMSignalFlags[reason as usize] != 0 {
        (*PMSignalState).PMSignalFlags[reason as usize] = false as sig_atomic_t;
        return true;
    }
    false
}

/*
 * SetQuitSignalReason - broadcast the reason for a system shutdown.
 * Should be called by postmaster before sending SIGQUIT to children.
 */
pub unsafe fn SetQuitSignalReason(reason: QuitSignalReason) {
    (*PMSignalState).sigquit_reason = reason;
}

/*
 * GetQuitSignalReason - obtain the reason for a system shutdown.
 * Called by child processes when they receive SIGQUIT.
 */
pub unsafe fn GetQuitSignalReason() -> QuitSignalReason {
    /* This is called in signal handlers, so be extra paranoid. */
    if !IsUnderPostmaster || PMSignalState.is_null() {
        return PMQUIT_NOT_SENT;
    }
    (*PMSignalState).sigquit_reason
}

/*
 * Helper: pointer to the PMChildFlags[idx] element (0-based).
 */
#[inline]
unsafe fn PMChildFlags_ptr(idx: c_int) -> *mut sig_atomic_t {
    (*PMSignalState).PMChildFlags.as_mut_ptr().add(idx as usize)
}

/*
 * MarkPostmasterChildSlotAssigned - mark the given slot as ASSIGNED for a
 * new postmaster child process.
 */
pub unsafe fn MarkPostmasterChildSlotAssigned(slot: c_int) {
    Assert!(slot > 0 && slot <= num_child_flags);
    let slot = slot - 1;

    if *PMChildFlags_ptr(slot) != PM_CHILD_UNUSED {
        elog!(FATAL, "postmaster child slot is already in use");
    }

    *PMChildFlags_ptr(slot) = PM_CHILD_ASSIGNED;
}

/*
 * MarkPostmasterChildSlotUnassigned - release a slot after death of a
 * postmaster child process.  This must be called in the postmaster process.
 *
 * Returns true if the slot had been in ASSIGNED state (the expected case),
 * false otherwise (implying that the child failed to clean itself up).
 */
pub unsafe fn MarkPostmasterChildSlotUnassigned(slot: c_int) -> bool {
    let result: bool;

    Assert!(slot > 0 && slot <= num_child_flags);
    let slot = slot - 1;

    /*
     * Note: the slot state might already be unused, because the logic in
     * postmaster.c is such that this might get called twice when a child
     * crashes.  So we don't try to Assert anything about the state.
     */
    result = *PMChildFlags_ptr(slot) == PM_CHILD_ASSIGNED;
    *PMChildFlags_ptr(slot) = PM_CHILD_UNUSED;
    result
}

/*
 * IsPostmasterChildWalSender - check if given slot is in use by a
 * walsender process.  This is called only by the postmaster.
 */
pub unsafe fn IsPostmasterChildWalSender(slot: c_int) -> bool {
    Assert!(slot > 0 && slot <= num_child_flags);
    let slot = slot - 1;

    if *PMChildFlags_ptr(slot) == PM_CHILD_WALSENDER {
        true
    } else {
        false
    }
}

/*
 * RegisterPostmasterChildActive - mark a postmaster child as about to begin
 * actively using shared memory.  This is called in the child process.
 */
pub unsafe fn RegisterPostmasterChildActive() {
    let slot = MyPMChildSlot;

    Assert!(slot > 0 && slot <= (*PMSignalState).num_child_flags);
    let slot = slot - 1;
    Assert!(*PMChildFlags_ptr(slot) == PM_CHILD_ASSIGNED);
    *PMChildFlags_ptr(slot) = PM_CHILD_ACTIVE;

    /* Arrange to clean up at exit. */
    on_shmem_exit(Some(MarkPostmasterChildInactive), 0);
}

/*
 * MarkPostmasterChildWalSender - mark a postmaster child as a WAL sender
 * process.  This is called in the child process, sometime after marking the
 * child as active.
 */
pub unsafe fn MarkPostmasterChildWalSender() {
    let slot = MyPMChildSlot;

    Assert!(am_walsender);

    Assert!(slot > 0 && slot <= (*PMSignalState).num_child_flags);
    let slot = slot - 1;
    Assert!(*PMChildFlags_ptr(slot) == PM_CHILD_ACTIVE);
    *PMChildFlags_ptr(slot) = PM_CHILD_WALSENDER;
}

/*
 * MarkPostmasterChildInactive - mark a postmaster child as done using
 * shared memory.  This is called in the child process.
 */
unsafe extern "C" fn MarkPostmasterChildInactive(_code: c_int, _arg: Datum) {
    let slot = MyPMChildSlot;

    Assert!(slot > 0 && slot <= (*PMSignalState).num_child_flags);
    let slot = slot - 1;
    Assert!(
        *PMChildFlags_ptr(slot) == PM_CHILD_ACTIVE
            || *PMChildFlags_ptr(slot) == PM_CHILD_WALSENDER
    );
    *PMChildFlags_ptr(slot) = PM_CHILD_ASSIGNED;
}

/*
 * PostmasterIsAliveInternal - check whether postmaster process is still alive
 *
 * This is the slow path of PostmasterIsAlive(), where the caller has already
 * checked 'postmaster_possibly_dead'.  (On platforms that don't support
 * a signal for parent death, PostmasterIsAlive() is just an alias for this.)
 */
pub unsafe fn PostmasterIsAliveInternal() -> bool {
    if USE_POSTMASTER_DEATH_SIGNAL {
        /*
         * Reset the flag before checking, so that we don't miss a signal if
         * postmaster dies right after the check.  If postmaster was indeed
         * dead, we'll re-arm it before returning to caller.
         */
        postmaster_possibly_dead = false as sig_atomic_t;
    }

    // non-WIN32 path
    {
        let mut c: c_char = 0;
        let rc: isize;

        rc = read(
            postmaster_alive_fds[POSTMASTER_FD_WATCH],
            &raw mut c as *mut c_void,
            1,
        );

        /*
         * In the usual case, the postmaster is still alive, and there is no
         * data in the pipe.
         */
        if rc < 0 && (errno_get() == EAGAIN || errno_get() == EWOULDBLOCK) {
            true
        } else {
            /*
             * Postmaster is dead, or something went wrong with the read()
             * call.
             */
            if USE_POSTMASTER_DEATH_SIGNAL {
                postmaster_possibly_dead = true as sig_atomic_t;
            }

            if rc < 0 {
                elog!(FATAL, "read on postmaster death monitoring pipe failed: {}", errno_get());
            } else if rc > 0 {
                elog!(FATAL, "unexpected data in postmaster death monitoring pipe");
            }

            false
        }
    }
}

/*
 * PostmasterDeathSignalInit - request signal on postmaster death if possible
 *
 * On platforms without USE_POSTMASTER_DEATH_SIGNAL (e.g. macOS), this is a
 * no-op.
 */
pub unsafe fn PostmasterDeathSignalInit() {
    // USE_POSTMASTER_DEATH_SIGNAL is not defined on this platform; nothing to
    // do.  (The Linux PR_SET_PDEATHSIG / *BSD PROC_PDEATHSIG_CTL paths are
    // compiled out.)
}

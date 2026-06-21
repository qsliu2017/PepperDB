//! storage/ipc/latch.c - Routines for inter-process latches.
//!
//! The latch interface is a reliable replacement for the common pattern of
//! using pg_usleep() or select() to wait until a signal arrives, where the
//! signal handler sets a flag variable.

use crate::prelude::*;

use crate::miscadmin::sig_atomic_t;
use crate::nodes::execnodes::WaitEventSet;
use crate::port::noblock::pgsocket;
use crate::port::port_api::PGINVALID_SOCKET;
use crate::port::atomics::generic::pg_memory_barrier_impl;
use crate::utils::resowner::resowner::{ResourceOwner, CurrentResourceOwner};

use std::ffi::c_int;

// Globals defined elsewhere (globals.rs / miscadmin.rs). MyLatch is typed
// `*mut c_void` over there (Latch is forward-declared as c_void), so we cast
// it to our real Latch struct pointer where needed.
extern "C" {
    pub static mut MyProcPid: c_int;
    pub static mut IsUnderPostmaster: bool;
    pub static mut MyLatch: *mut std::ffi::c_void;
}

/*
 * Latch structure (canonical definition lives with latch.h). On non-Windows
 * builds there is no `event` HANDLE field.
 */
#[repr(C)]
pub struct Latch {
    pub is_set: sig_atomic_t,
    pub maybe_sleeping: sig_atomic_t,
    pub is_shared: bool,
    pub owner_pid: c_int,
}

/*
 * Bitmasks for events that may wake-up WaitLatch(), WaitLatchOrSocket(), or
 * WaitEventSetWait().
 */
pub const WL_LATCH_SET: c_int = 1 << 0;
pub const WL_SOCKET_READABLE: c_int = 1 << 1;
pub const WL_SOCKET_WRITEABLE: c_int = 1 << 2;
pub const WL_TIMEOUT: c_int = 1 << 3; /* not for WaitEventSetWait() */
pub const WL_POSTMASTER_DEATH: c_int = 1 << 4;
pub const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;
/* avoid having to deal with case on platforms not requiring it */
pub const WL_SOCKET_CONNECTED: c_int = WL_SOCKET_WRITEABLE;
pub const WL_SOCKET_CLOSED: c_int = 1 << 7;
/* avoid having to deal with case on platforms not requiring it */
pub const WL_SOCKET_ACCEPT: c_int = WL_SOCKET_READABLE;
pub const WL_SOCKET_MASK: c_int = WL_SOCKET_READABLE
    | WL_SOCKET_WRITEABLE
    | WL_SOCKET_CONNECTED
    | WL_SOCKET_ACCEPT
    | WL_SOCKET_CLOSED;

/*
 * A reported wait event, returned by WaitEventSetWait().
 */
#[repr(C)]
pub struct WaitEvent {
    pub pos: c_int,             /* position in the event data structure */
    pub events: uint32,         /* triggered events */
    pub fd: pgsocket,           /* socket fd associated with event */
    pub user_data: *mut std::ffi::c_void, /* pointer provided in AddWaitEventToSet */
}

/* A common WaitEventSet used to implement WaitLatch() */
static mut LatchWaitSet: *mut WaitEventSet = null_mut();

/* The positions of the latch and PM death events in LatchWaitSet */
const LatchWaitSetLatchPos: c_int = 0;
const LatchWaitSetPostmasterDeathPos: c_int = 1;

pub unsafe fn InitializeLatchWaitSet() {
    let latch_pos: c_int;

    Assert!(LatchWaitSet.is_null());

    /* Set up the WaitEventSet used by WaitLatch(). */
    LatchWaitSet = CreateWaitEventSet(null_mut(), 2);
    latch_pos = AddWaitEventToSet(
        LatchWaitSet,
        WL_LATCH_SET as uint32,
        PGINVALID_SOCKET,
        MyLatch as *mut Latch,
        null_mut(),
    );
    Assert!(latch_pos == LatchWaitSetLatchPos);

    /*
     * WaitLatch will modify this to WL_EXIT_ON_PM_DEATH or
     * WL_POSTMASTER_DEATH on each call.
     */
    if IsUnderPostmaster {
        let latch_pos = AddWaitEventToSet(
            LatchWaitSet,
            WL_EXIT_ON_PM_DEATH as uint32,
            PGINVALID_SOCKET,
            null_mut(),
            null_mut(),
        );
        Assert!(latch_pos == LatchWaitSetPostmasterDeathPos);
    }

    let _ = latch_pos;
}

/*
 * Initialize a process-local latch.
 */
pub unsafe fn InitLatch(latch: *mut Latch) {
    (*latch).is_set = false as sig_atomic_t;
    (*latch).maybe_sleeping = false as sig_atomic_t;
    (*latch).owner_pid = MyProcPid;
    (*latch).is_shared = false;
}

/*
 * Initialize a shared latch that can be set from other processes. The latch
 * is initially owned by no-one; use OwnLatch to associate it with the
 * current process.
 */
pub unsafe fn InitSharedLatch(latch: *mut Latch) {
    (*latch).is_set = false as sig_atomic_t;
    (*latch).maybe_sleeping = false as sig_atomic_t;
    (*latch).owner_pid = 0;
    (*latch).is_shared = true;
}

/*
 * Associate a shared latch with the current process, allowing it to
 * wait on the latch.
 */
pub unsafe fn OwnLatch(latch: *mut Latch) {
    let owner_pid: c_int;

    /* Sanity checks */
    Assert!((*latch).is_shared);

    owner_pid = (*latch).owner_pid;
    if owner_pid != 0 {
        elog!(PANIC, "latch already owned by PID {}", owner_pid);
    }

    (*latch).owner_pid = MyProcPid;
}

/*
 * Disown a shared latch currently owned by the current process.
 */
pub unsafe fn DisownLatch(latch: *mut Latch) {
    Assert!((*latch).is_shared);
    Assert!((*latch).owner_pid == MyProcPid);

    (*latch).owner_pid = 0;
}

/*
 * Wait for a given latch to be set, or for postmaster death, or until timeout
 * is exceeded. 'wakeEvents' is a bitmask that specifies which of those events
 * to wait for. If the latch is already set (and WL_LATCH_SET is given), the
 * function returns immediately.
 *
 * Returns bit mask indicating which condition(s) caused the wake-up.
 */
pub unsafe fn WaitLatch(
    mut latch: *mut Latch,
    wakeEvents: c_int,
    timeout: c_long,
    wait_event_info: uint32,
) -> c_int {
    let mut event: WaitEvent = std::mem::zeroed();

    /* Postmaster-managed callers must handle postmaster death somehow. */
    Assert!(
        !IsUnderPostmaster
            || (wakeEvents & WL_EXIT_ON_PM_DEATH) != 0
            || (wakeEvents & WL_POSTMASTER_DEATH) != 0
    );

    /*
     * Some callers may have a latch other than MyLatch, or no latch at all,
     * or want to handle postmaster death differently.  It's cheap to assign
     * those, so just do it every time.
     */
    if (wakeEvents & WL_LATCH_SET) == 0 {
        latch = null_mut();
    }
    ModifyWaitEvent(
        LatchWaitSet,
        LatchWaitSetLatchPos,
        WL_LATCH_SET as uint32,
        latch,
    );

    if IsUnderPostmaster {
        ModifyWaitEvent(
            LatchWaitSet,
            LatchWaitSetPostmasterDeathPos,
            (wakeEvents & (WL_EXIT_ON_PM_DEATH | WL_POSTMASTER_DEATH)) as uint32,
            null_mut(),
        );
    }

    if WaitEventSetWait(
        LatchWaitSet,
        if (wakeEvents & WL_TIMEOUT) != 0 {
            timeout
        } else {
            -1
        },
        &mut event,
        1,
        wait_event_info,
    ) == 0
    {
        WL_TIMEOUT
    } else {
        event.events as c_int
    }
}

/*
 * Like WaitLatch, but with an extra socket argument for WL_SOCKET_*
 * conditions.
 *
 * NB: These days this is just a wrapper around the WaitEventSet API.
 */
#[no_mangle]
pub unsafe fn WaitLatchOrSocket(
    latch: *mut Latch,
    wakeEvents: c_int,
    sock: pgsocket,
    mut timeout: c_long,
    wait_event_info: uint32,
) -> c_int {
    let mut ret: c_int = 0;
    let rc: c_int;
    let mut event: WaitEvent = std::mem::zeroed();
    let set: *mut WaitEventSet = CreateWaitEventSet(CurrentResourceOwner, 3);

    if (wakeEvents & WL_TIMEOUT) != 0 {
        Assert!(timeout >= 0);
    } else {
        timeout = -1;
    }

    if (wakeEvents & WL_LATCH_SET) != 0 {
        AddWaitEventToSet(set, WL_LATCH_SET as uint32, PGINVALID_SOCKET, latch, null_mut());
    }

    /* Postmaster-managed callers must handle postmaster death somehow. */
    Assert!(
        !IsUnderPostmaster
            || (wakeEvents & WL_EXIT_ON_PM_DEATH) != 0
            || (wakeEvents & WL_POSTMASTER_DEATH) != 0
    );

    if (wakeEvents & WL_POSTMASTER_DEATH) != 0 && IsUnderPostmaster {
        AddWaitEventToSet(
            set,
            WL_POSTMASTER_DEATH as uint32,
            PGINVALID_SOCKET,
            null_mut(),
            null_mut(),
        );
    }

    if (wakeEvents & WL_EXIT_ON_PM_DEATH) != 0 && IsUnderPostmaster {
        AddWaitEventToSet(
            set,
            WL_EXIT_ON_PM_DEATH as uint32,
            PGINVALID_SOCKET,
            null_mut(),
            null_mut(),
        );
    }

    if (wakeEvents & WL_SOCKET_MASK) != 0 {
        let ev: c_int = wakeEvents & WL_SOCKET_MASK;
        AddWaitEventToSet(set, ev as uint32, sock, null_mut(), null_mut());
    }

    rc = WaitEventSetWait(set, timeout, &mut event, 1, wait_event_info);

    if rc == 0 {
        ret |= WL_TIMEOUT;
    } else {
        ret |= event.events as c_int
            & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    }

    FreeWaitEventSet(set);

    ret
}

/*
 * Sets a latch and wakes up anyone waiting on it.
 *
 * This is cheap if the latch is already set, otherwise not so much.
 *
 * NB: this function is called from critical sections and signal handlers so
 * throwing an error is not a good idea.
 */
pub unsafe fn SetLatch(latch: *mut Latch) {
    let owner_pid: pid_t;

    /*
     * The memory barrier has to be placed here to ensure that any flag
     * variables possibly changed by this process have been flushed to main
     * memory, before we check/set is_set.
     */
    pg_memory_barrier_impl();

    /* Quick exit if already set */
    if (*latch).is_set != 0 {
        return;
    }

    (*latch).is_set = true as sig_atomic_t;

    pg_memory_barrier_impl();
    if (*latch).maybe_sleeping == 0 {
        return;
    }

    /*
     * See if anyone's waiting for the latch. It can be the current process if
     * we're in a signal handler. We use the self-pipe or SIGURG to ourselves
     * to wake up WaitEventSetWaitBlock() without races in that case. If it's
     * another process, send a signal.
     */
    owner_pid = (*latch).owner_pid;
    if owner_pid == 0 {
        return;
    } else if owner_pid == MyProcPid {
        WakeupMyProc();
    } else {
        WakeupOtherProc(owner_pid);
    }
}

/*
 * Clear the latch. Calling WaitLatch after this will sleep, unless
 * the latch is set again before the WaitLatch call.
 */
#[no_mangle]
pub unsafe fn ResetLatch(latch: *mut Latch) {
    /* Only the owner should reset the latch */
    Assert!((*latch).owner_pid == MyProcPid);
    Assert!((*latch).maybe_sleeping == 0);

    (*latch).is_set = false as sig_atomic_t;

    /*
     * Ensure that the write to is_set gets flushed to main memory before we
     * examine any flag variables.  Otherwise a concurrent SetLatch might
     * falsely conclude that it needn't signal us, even though we have missed
     * seeing some flag updates that SetLatch was supposed to inform us of.
     */
    pg_memory_barrier_impl();
}

// pid_t from <sys/types.h>; on the platforms we target this is c_int.
type pid_t = c_int;

// ---------------------------------------------------------------------------
// Local stubs for functions defined in waiteventset.c (not yet ported).
// TODO(pg-port): import these from storage/ipc/waiteventset.rs once ported.
// ---------------------------------------------------------------------------

unsafe fn CreateWaitEventSet(_resowner: ResourceOwner, _nevents: c_int) -> *mut WaitEventSet {
    crate::storage::ipc::waiteventset::CreateWaitEventSet(_resowner as _, _nevents) as _
}

unsafe fn FreeWaitEventSet(_set: *mut WaitEventSet) {
    crate::storage::ipc::waiteventset::FreeWaitEventSet(_set as _)
}

unsafe fn AddWaitEventToSet(
    _set: *mut WaitEventSet,
    _events: uint32,
    _fd: pgsocket,
    _latch: *mut Latch,
    _user_data: *mut std::ffi::c_void,
) -> c_int {
    crate::storage::ipc::waiteventset::AddWaitEventToSet(_set as _, _events, _fd as _, _latch as _, _user_data)
}

unsafe fn ModifyWaitEvent(
    _set: *mut WaitEventSet,
    _pos: c_int,
    _events: uint32,
    _latch: *mut Latch,
) {
    crate::storage::ipc::waiteventset::ModifyWaitEvent(_set as _, _pos, _events, _latch as _)
}

unsafe fn WaitEventSetWait(
    _set: *mut WaitEventSet,
    _timeout: c_long,
    _occurred_events: *mut WaitEvent,
    _nevents: c_int,
    _wait_event_info: uint32,
) -> c_int {
    crate::storage::ipc::waiteventset::WaitEventSetWait(_set as _, _timeout, _occurred_events as _, _nevents, _wait_event_info)
}

unsafe fn WakeupMyProc() {
    crate::storage::ipc::waiteventset::WakeupMyProc()
}

unsafe fn WakeupOtherProc(_pid: c_int) {
    crate::storage::ipc::waiteventset::WakeupOtherProc(_pid)
}

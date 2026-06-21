//! storage/ipc/waiteventset.c - ppoll()/pselect() like abstraction.
//!
//! WaitEvents are an abstraction for waiting for one or more events at a time.
//! The waiting can be done in a race free fashion, similar ppoll() or
//! pselect() (as opposed to plain poll()/select()).
//!
//! You can wait for:
//! - a latch being set from another process or from signal handler in the same
//!   process (WL_LATCH_SET)
//! - data to become readable or writeable on a socket (WL_SOCKET_*)
//! - postmaster death (WL_POSTMASTER_DEATH or WL_EXIT_ON_PM_DEATH)
//! - timeout (WL_TIMEOUT)
//!
//! Implementation
//! --------------
//!
//! The kqueue() implementation waits for SIGURG with EVFILT_SIGNAL.  We
//! translate the WAIT_USE_KQUEUE path (the primitive selected on macOS/BSD).

use crate::prelude::*;

use crate::libpq::pqsignal::{pqsignal, SigHandler};
use crate::port::noblock::pgsocket;
use crate::port::port_api::PGINVALID_SOCKET;
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_GET_MILLISEC, INSTR_TIME_SET_CURRENT, INSTR_TIME_SET_ZERO,
    INSTR_TIME_SUBTRACT,
};
use crate::storage::file::fd::{AcquireExternalFD, ReleaseExternalFD};
use crate::storage::ipc::ipc::proc_exit;
use crate::storage::ipc::latch::{
    Latch, WaitEvent, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_CLOSED,
    WL_SOCKET_MASK, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};
use crate::storage::ipc::pmsignal::PostmasterIsAliveInternal;
use crate::utils::resowner::resowner::{
    ResourceOwner, ResourceOwnerDesc, ResourceOwnerEnlarge, ResourceOwnerForget,
    ResourceOwnerRemember, RELEASE_PRIO_WAITEVENTSETS, RESOURCE_RELEASE_AFTER_LOCKS,
};

use core::ffi::CStr;

extern "C" {
    pub static mut MyProcPid: c_int;
    pub static mut IsUnderPostmaster: bool;
    pub static mut PostmasterPid: c_int;

    /// errno access (thread-local). macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// %m expansion: render the current errno as its strerror() string.
unsafe fn strerror_errno() -> String {
    let s = libc::strerror(errno());
    CStr::from_ptr(s).to_string_lossy().into_owned()
}

// SIG_IGN is the function pointer with value 1; pqsignal.rs models SIG_DFL as
// None and leaves SIG_IGN to callers. We construct it from 1.
fn SIG_IGN() -> SigHandler {
    unsafe { Some(core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1)) }
}

// The public WaitEventSet type is forward-declared as an opaque struct in
// nodes/execnodes.rs (mirroring the typedef in waiteventset.h). The real
// definition lives here; callers hold a pointer to the opaque type, which is
// reinterpreted as `*mut WaitEventSet`.
//
// struct WaitEventSet, WAIT_USE_KQUEUE variant.
#[repr(C)]
pub struct WaitEventSet {
    pub owner: ResourceOwner,

    pub nevents: c_int,        /* number of registered events */
    pub nevents_space: c_int,  /* maximum number of events in this set */

    /*
     * Array, of nevents_space length, storing the definition of events this
     * set is waiting for.
     */
    pub events: *mut WaitEvent,

    /*
     * If WL_LATCH_SET is specified in any wait event, latch is a pointer to
     * said latch, and latch_pos the offset in the ->events array. This is
     * useful because we check the state of the latch before performing doing
     * syscalls related to waiting.
     */
    pub latch: *mut Latch,
    pub latch_pos: c_int,

    /*
     * WL_EXIT_ON_PM_DEATH is converted to WL_POSTMASTER_DEATH, but this flag
     * is set so that we'll exit immediately if postmaster death is detected,
     * instead of returning.
     */
    pub exit_on_postmaster_death: bool,

    pub kqueue_fd: c_int,
    /* kevent returns events in a user provided arrays, allocate once */
    pub kqueue_ret_events: *mut libc::kevent,
    pub report_postmaster_not_running: bool,
}

/* Are we currently in WaitLatch? The signal handler would like to know. */
static mut waiting: bool = false;

/* ResourceOwner support to hold WaitEventSets */

// Raw-pointer `name` field makes ResourceOwnerDesc non-Sync; wrap (codebase SyncDesc pattern).
struct SyncDesc(ResourceOwnerDesc);
unsafe impl Sync for SyncDesc {}
static wait_event_set_resowner_desc: SyncDesc = SyncDesc(ResourceOwnerDesc {
    name: b"WaitEventSet\0".as_ptr() as *const c_char,
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_WAITEVENTSETS,
    ReleaseResource: ResOwnerReleaseWaitEventSet,
    DebugPrint: None,
});

/* Convenience wrappers over ResourceOwnerRemember/Forget */
#[inline]
unsafe fn ResourceOwnerRememberWaitEventSet(owner: ResourceOwner, set: *mut WaitEventSet) {
    ResourceOwnerRemember(
        owner,
        PointerGetDatum(set as *const c_void),
        &wait_event_set_resowner_desc.0,
    );
}
#[inline]
unsafe fn ResourceOwnerForgetWaitEventSet(owner: ResourceOwner, set: *mut WaitEventSet) {
    ResourceOwnerForget(
        owner,
        PointerGetDatum(set as *const c_void),
        &wait_event_set_resowner_desc.0,
    );
}

/*
 * On most BSD family systems, the udata member of struct kevent is of type
 * void *, so we could directly convert to/from WaitEvent *.  Unfortunately,
 * NetBSD has it as intptr_t, so here we wallpaper over that difference with
 * an lvalue cast.  On macOS udata is void *, so the access is a plain cast.
 */
#[inline]
unsafe fn AccessWaitEvent(k_ev: *mut libc::kevent) -> *mut WaitEvent {
    (*k_ev).udata as *mut WaitEvent
}

/*
 * Initialize the process-local wait event infrastructure.
 *
 * This must be called once during startup of any process that can wait on
 * latches, before it issues any InitLatch() or OwnLatch() calls.
 */
pub unsafe fn InitializeWaitEventSupport() {
    /* Ignore SIGURG, because we'll receive it via kqueue. */
    pqsignal(libc::SIGURG, SIG_IGN());
}

/*
 * Create a WaitEventSet with space for nevents different events to wait for.
 *
 * These events can then be efficiently waited upon together, using
 * WaitEventSetWait().
 *
 * The WaitEventSet is tracked by the given 'resowner'.  Use NULL for session
 * lifetime.
 */
pub unsafe fn CreateWaitEventSet(resowner: ResourceOwner, nevents: c_int) -> *mut WaitEventSet {
    let set: *mut WaitEventSet;
    let mut data: *mut c_char;
    let mut sz: Size = 0;

    /*
     * Use MAXALIGN size/alignment to guarantee that later uses of memory are
     * aligned correctly. E.g. epoll_event might need 8 byte alignment on some
     * platforms, but earlier allocations like WaitEventSet and WaitEvent
     * might not be sized to guarantee that when purely using sizeof().
     */
    sz += MAXALIGN(core::mem::size_of::<WaitEventSet>());
    sz += MAXALIGN(core::mem::size_of::<WaitEvent>() * nevents as usize);

    sz += MAXALIGN(core::mem::size_of::<libc::kevent>() * nevents as usize);

    if !resowner.is_null() {
        ResourceOwnerEnlarge(resowner);
    }

    data = MemoryContextAllocZero(TopMemoryContext, sz) as *mut c_char;

    set = data as *mut WaitEventSet;
    data = data.add(MAXALIGN(core::mem::size_of::<WaitEventSet>()));

    (*set).events = data as *mut WaitEvent;
    data = data.add(MAXALIGN(core::mem::size_of::<WaitEvent>() * nevents as usize));

    (*set).kqueue_ret_events = data as *mut libc::kevent;
    data = data.add(MAXALIGN(core::mem::size_of::<libc::kevent>() * nevents as usize));

    (*set).latch = null_mut();
    (*set).nevents_space = nevents;
    (*set).exit_on_postmaster_death = false;

    if !resowner.is_null() {
        ResourceOwnerRememberWaitEventSet(resowner, set);
        (*set).owner = resowner;
    }

    if !AcquireExternalFD() {
        elog!(ERROR, "AcquireExternalFD, for kqueue, failed: {}", strerror_errno());
    }
    (*set).kqueue_fd = libc::kqueue();
    if (*set).kqueue_fd < 0 {
        ReleaseExternalFD();
        elog!(ERROR, "kqueue failed: {}", strerror_errno());
    }
    if libc::fcntl((*set).kqueue_fd, libc::F_SETFD, libc::FD_CLOEXEC) == -1 {
        let save_errno = errno();

        libc::close((*set).kqueue_fd);
        ReleaseExternalFD();
        set_errno(save_errno);
        elog!(ERROR, "fcntl(F_SETFD) failed on kqueue descriptor: {}", strerror_errno());
    }
    (*set).report_postmaster_not_running = false;

    set
}

/*
 * Free a previously created WaitEventSet.
 *
 * Note: preferably, this shouldn't have to free any resources that could be
 * inherited across an exec().  If it did, we'd likely leak those resources in
 * many scenarios.  For the epoll case, we ensure that by setting EPOLL_CLOEXEC
 * when the FD is created.  For the Windows case, we assume that the handles
 * involved are non-inheritable.
 */
pub unsafe fn FreeWaitEventSet(set: *mut WaitEventSet) {
    if !(*set).owner.is_null() {
        ResourceOwnerForgetWaitEventSet((*set).owner, set);
        (*set).owner = null_mut();
    }

    libc::close((*set).kqueue_fd);
    ReleaseExternalFD();

    pfree(set as *mut c_void);
}

/*
 * Free a previously created WaitEventSet in a child process after a fork().
 */
pub unsafe fn FreeWaitEventSetAfterFork(set: *mut WaitEventSet) {
    /* kqueues are not normally inherited by child processes */
    ReleaseExternalFD();

    pfree(set as *mut c_void);
}

/* ---
 * Add an event to the set. Possible events are:
 * - WL_LATCH_SET: Wait for the latch to be set
 * - WL_POSTMASTER_DEATH: Wait for postmaster to die
 * - WL_SOCKET_READABLE: Wait for socket to become readable,
 *	 can be combined in one event with other WL_SOCKET_* events
 * - WL_SOCKET_WRITEABLE: Wait for socket to become writeable,
 *	 can be combined with other WL_SOCKET_* events
 * - WL_SOCKET_CONNECTED: Wait for socket connection to be established,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_WRITEABLE)
 * - WL_SOCKET_ACCEPT: Wait for new connection to a server socket,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_READABLE)
 * - WL_SOCKET_CLOSED: Wait for socket to be closed by remote peer.
 * - WL_EXIT_ON_PM_DEATH: Exit immediately if the postmaster dies
 *
 * Returns the offset in WaitEventSet->events (starting from 0), which can be
 * used to modify previously added wait events using ModifyWaitEvent().
 *
 * In the WL_LATCH_SET case the latch must be owned by the current process,
 * i.e. it must be a process-local latch initialized with InitLatch, or a
 * shared latch associated with the current process by calling OwnLatch.
 *
 * In the WL_SOCKET_READABLE/WRITEABLE/CONNECTED/ACCEPT cases, EOF and error
 * conditions cause the socket to be reported as readable/writable/connected,
 * so that the caller can deal with the condition.
 *
 * The user_data pointer specified here will be set for the events returned
 * by WaitEventSetWait(), allowing to easily associate additional data with
 * events.
 */
pub unsafe fn AddWaitEventToSet(
    set: *mut WaitEventSet,
    mut events: uint32,
    fd: pgsocket,
    latch: *mut Latch,
    user_data: *mut c_void,
) -> c_int {
    let event: *mut WaitEvent;

    /* not enough space */
    Assert!((*set).nevents < (*set).nevents_space);

    if events == WL_EXIT_ON_PM_DEATH as uint32 {
        events = WL_POSTMASTER_DEATH as uint32;
        (*set).exit_on_postmaster_death = true;
    }

    if !latch.is_null() {
        if (*latch).owner_pid != MyProcPid {
            elog!(ERROR, "cannot wait on a latch owned by another process");
        }
        if !(*set).latch.is_null() {
            elog!(ERROR, "cannot wait on more than one latch");
        }
        if (events & WL_LATCH_SET as uint32) != WL_LATCH_SET as uint32 {
            elog!(ERROR, "latch events only support being set");
        }
    } else {
        if (events & WL_LATCH_SET as uint32) != 0 {
            elog!(ERROR, "cannot wait on latch without a specified latch");
        }
    }

    /* waiting for socket readiness without a socket indicates a bug */
    if fd == PGINVALID_SOCKET && (events & WL_SOCKET_MASK as uint32) != 0 {
        elog!(ERROR, "cannot wait on socket event without a socket");
    }

    event = &mut *(*set).events.add((*set).nevents as usize);
    (*event).pos = (*set).nevents;
    (*set).nevents += 1;
    (*event).fd = fd;
    (*event).events = events;
    (*event).user_data = user_data;

    if events == WL_LATCH_SET as uint32 {
        (*set).latch = latch;
        (*set).latch_pos = (*event).pos;
        (*event).fd = PGINVALID_SOCKET;
    } else if events == WL_POSTMASTER_DEATH as uint32 {
        (*event).fd = postmaster_alive_fds[POSTMASTER_FD_WATCH];
    }

    /* perform wait primitive specific initialization, if needed */
    WaitEventAdjustKqueue(set, event, 0);

    (*event).pos
}

/*
 * Change the event mask and, in the WL_LATCH_SET case, the latch associated
 * with the WaitEvent.  The latch may be changed to NULL to disable the latch
 * temporarily, and then set back to a latch later.
 *
 * 'pos' is the id returned by AddWaitEventToSet.
 */
pub unsafe fn ModifyWaitEvent(set: *mut WaitEventSet, pos: c_int, events: uint32, latch: *mut Latch) {
    let event: *mut WaitEvent;
    let old_events: c_int;

    Assert!(pos < (*set).nevents);

    event = &mut *(*set).events.add(pos as usize);
    old_events = (*event).events as c_int;

    /*
     * Allow switching between WL_POSTMASTER_DEATH and WL_EXIT_ON_PM_DEATH.
     *
     * Note that because WL_EXIT_ON_PM_DEATH is mapped to WL_POSTMASTER_DEATH
     * in AddWaitEventToSet(), this needs to be checked before the fast-path
     * below that checks if 'events' has changed.
     */
    if (*event).events == WL_POSTMASTER_DEATH as uint32 {
        if events != WL_POSTMASTER_DEATH as uint32 && events != WL_EXIT_ON_PM_DEATH as uint32 {
            elog!(ERROR, "cannot remove postmaster death event");
        }
        (*set).exit_on_postmaster_death = (events & WL_EXIT_ON_PM_DEATH as uint32) != 0;
        return;
    }

    /*
     * If neither the event mask nor the associated latch changes, return
     * early. That's an important optimization for some sockets, where
     * ModifyWaitEvent is frequently used to switch from waiting for reads to
     * waiting on writes.
     */
    if events == (*event).events
        && (((*event).events & WL_LATCH_SET as uint32) == 0 || (*set).latch == latch)
    {
        return;
    }

    if ((*event).events & WL_LATCH_SET as uint32) != 0 && events != (*event).events {
        elog!(ERROR, "cannot modify latch event");
    }

    /* FIXME: validate event mask */
    (*event).events = events;

    if events == WL_LATCH_SET as uint32 {
        if !latch.is_null() && (*latch).owner_pid != MyProcPid {
            elog!(ERROR, "cannot wait on a latch owned by another process");
        }
        (*set).latch = latch;

        /*
         * On Unix, we don't need to modify the kernel object because the
         * underlying pipe (if there is one) is the same for all latches so we
         * can return immediately.  On Windows, we need to update our array of
         * handles, but we leave the old one in place and tolerate spurious
         * wakeups if the latch is disabled.
         */
        return;
    }

    WaitEventAdjustKqueue(set, event, old_events);
}

/*
 * On most BSD family systems, the udata member of struct kevent is of type
 * void *, so we could directly convert to/from WaitEvent *.  Unfortunately,
 * NetBSD has it as intptr_t, so here we wallpaper over that difference with
 * an lvalue cast.
 */

#[inline]
unsafe fn WaitEventAdjustKqueueAdd(
    k_ev: *mut libc::kevent,
    filter: i16,
    action: u16,
    event: *mut WaitEvent,
) {
    (*k_ev).ident = (*event).fd as libc::uintptr_t;
    (*k_ev).filter = filter;
    (*k_ev).flags = action;
    (*k_ev).fflags = 0;
    (*k_ev).data = 0;
    (*k_ev).udata = event as *mut c_void;
}

#[inline]
unsafe fn WaitEventAdjustKqueueAddPostmaster(k_ev: *mut libc::kevent, event: *mut WaitEvent) {
    /* For now postmaster death can only be added, not removed. */
    (*k_ev).ident = PostmasterPid as libc::uintptr_t;
    (*k_ev).filter = libc::EVFILT_PROC;
    (*k_ev).flags = libc::EV_ADD;
    (*k_ev).fflags = libc::NOTE_EXIT;
    (*k_ev).data = 0;
    (*k_ev).udata = event as *mut c_void;
}

#[inline]
unsafe fn WaitEventAdjustKqueueAddLatch(k_ev: *mut libc::kevent, event: *mut WaitEvent) {
    /* For now latch can only be added, not removed. */
    (*k_ev).ident = libc::SIGURG as libc::uintptr_t;
    (*k_ev).filter = libc::EVFILT_SIGNAL;
    (*k_ev).flags = libc::EV_ADD;
    (*k_ev).fflags = 0;
    (*k_ev).data = 0;
    (*k_ev).udata = event as *mut c_void;
}

/*
 * old_events is the previous event mask, used to compute what has changed.
 */
unsafe fn WaitEventAdjustKqueue(set: *mut WaitEventSet, event: *mut WaitEvent, old_events: c_int) {
    let rc: c_int;
    let mut k_ev: [libc::kevent; 2] = core::mem::zeroed();
    let mut count: c_int = 0;
    let mut new_filt_read = false;
    let mut old_filt_read = false;
    let mut new_filt_write = false;
    let mut old_filt_write = false;

    if old_events == (*event).events as c_int {
        return;
    }

    Assert!((*event).events != WL_LATCH_SET as uint32 || !(*set).latch.is_null());
    Assert!(
        (*event).events == WL_LATCH_SET as uint32
            || (*event).events == WL_POSTMASTER_DEATH as uint32
            || ((*event).events
                & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED) as uint32)
                != 0
    );

    if (*event).events == WL_POSTMASTER_DEATH as uint32 {
        /*
         * Unlike all the other implementations, we detect postmaster death
         * using process notification instead of waiting on the postmaster
         * alive pipe.
         */
        WaitEventAdjustKqueueAddPostmaster(&mut k_ev[count as usize], event);
        count += 1;
    } else if (*event).events == WL_LATCH_SET as uint32 {
        /* We detect latch wakeup using a signal event. */
        WaitEventAdjustKqueueAddLatch(&mut k_ev[count as usize], event);
        count += 1;
    } else {
        /*
         * We need to compute the adds and deletes required to get from the
         * old event mask to the new event mask, since kevent treats readable
         * and writable as separate events.
         */
        if (old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED)) != 0 {
            old_filt_read = true;
        }
        if ((*event).events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED) as uint32) != 0 {
            new_filt_read = true;
        }
        if (old_events & WL_SOCKET_WRITEABLE) != 0 {
            old_filt_write = true;
        }
        if ((*event).events & WL_SOCKET_WRITEABLE as uint32) != 0 {
            new_filt_write = true;
        }
        if old_filt_read && !new_filt_read {
            WaitEventAdjustKqueueAdd(&mut k_ev[count as usize], libc::EVFILT_READ, libc::EV_DELETE, event);
            count += 1;
        } else if !old_filt_read && new_filt_read {
            WaitEventAdjustKqueueAdd(&mut k_ev[count as usize], libc::EVFILT_READ, libc::EV_ADD, event);
            count += 1;
        }
        if old_filt_write && !new_filt_write {
            WaitEventAdjustKqueueAdd(&mut k_ev[count as usize], libc::EVFILT_WRITE, libc::EV_DELETE, event);
            count += 1;
        } else if !old_filt_write && new_filt_write {
            WaitEventAdjustKqueueAdd(&mut k_ev[count as usize], libc::EVFILT_WRITE, libc::EV_ADD, event);
            count += 1;
        }
    }

    /* For WL_SOCKET_READ -> WL_SOCKET_CLOSED, no change needed. */
    if count == 0 {
        return;
    }

    Assert!(count <= 2);

    rc = libc::kevent((*set).kqueue_fd, &k_ev[0], count, null_mut(), 0, null());

    /*
     * When adding the postmaster's pid, we have to consider that it might
     * already have exited and perhaps even been replaced by another process
     * with the same pid.  If so, we have to defer reporting this as an event
     * until the next call to WaitEventSetWaitBlock().
     */

    if rc < 0 {
        if (*event).events == WL_POSTMASTER_DEATH as uint32
            && (errno() == libc::ESRCH || errno() == libc::EACCES)
        {
            (*set).report_postmaster_not_running = true;
        } else {
            // C also: errcode_for_socket_access()
            ereport!(ERROR, errmsg!("{}() failed: {}", "kevent", strerror_errno()));
        }
    } else if (*event).events == WL_POSTMASTER_DEATH as uint32
        && PostmasterPid != libc::getppid()
        && !PostmasterIsAlive()
    {
        /*
         * The extra PostmasterIsAliveInternal() check prevents false alarms
         * on systems that give a different value for getppid() while being
         * traced by a debugger.
         */
        (*set).report_postmaster_not_running = true;
    }
}

/*
 * Wait for events added to the set to happen, or until the timeout is
 * reached.  At most nevents occurred events are returned.
 *
 * If timeout = -1, block until an event occurs; if 0, check sockets for
 * readiness, but don't block; if > 0, block for at most timeout milliseconds.
 *
 * Returns the number of events occurred, or 0 if the timeout was reached.
 *
 * Returned events will have the fd, pos, user_data fields set to the
 * values associated with the registered event.
 */
pub unsafe fn WaitEventSetWait(
    set: *mut WaitEventSet,
    mut timeout: c_long,
    mut occurred_events: *mut WaitEvent,
    nevents: c_int,
    wait_event_info: uint32,
) -> c_int {
    let mut returned_events: c_int = 0;
    let mut start_time: instr_time = core::mem::zeroed();
    let mut cur_time: instr_time = core::mem::zeroed();
    let mut cur_timeout: c_long = -1;

    Assert!(nevents > 0);

    /*
     * Initialize timeout if requested.  We must record the current time so
     * that we can determine the remaining timeout if interrupted.
     */
    if timeout >= 0 {
        INSTR_TIME_SET_CURRENT(&mut start_time);
        Assert!(timeout >= 0 && timeout <= c_int::MAX as c_long);
        cur_timeout = timeout;
    } else {
        INSTR_TIME_SET_ZERO(&mut start_time);
    }

    pgstat_report_wait_start(wait_event_info);

    waiting = true;
    while returned_events == 0 {
        let rc: c_int;

        /*
         * Check if the latch is set already first.  If so, we either exit
         * immediately or ask the kernel for further events available right
         * now without waiting, depending on how many events the caller wants.
         *
         * If someone sets the latch between this and the
         * WaitEventSetWaitBlock() below, the setter will write a byte to the
         * pipe (or signal us and the signal handler will do that), and the
         * readiness routine will return immediately.
         *
         * On unix, If there's a pending byte in the self pipe, we'll notice
         * whenever blocking. Only clearing the pipe in that case avoids
         * having to drain it every time WaitLatchOrSocket() is used. Should
         * the pipe-buffer fill up we're still ok, because the pipe is in
         * nonblocking mode. It's unlikely for that to happen, because the
         * self pipe isn't filled unless we're blocking (waiting = true), or
         * from inside a signal handler in latch_sigurg_handler().
         *
         * On windows, we'll also notice if there's a pending event for the
         * latch when blocking, but there's no danger of anything filling up,
         * as "Setting an event that is already set has no effect.".
         *
         * Note: we assume that the kernel calls involved in latch management
         * will provide adequate synchronization on machines with weak memory
         * ordering, so that we cannot miss seeing is_set if a notification
         * has already been queued.
         */
        if !(*set).latch.is_null() && (*(*set).latch).is_set == 0 {
            /* about to sleep on a latch */
            (*(*set).latch).maybe_sleeping = true as _;
            pg_memory_barrier_impl();
            /* and recheck */
        }

        if !(*set).latch.is_null() && (*(*set).latch).is_set != 0 {
            (*occurred_events).fd = PGINVALID_SOCKET;
            (*occurred_events).pos = (*set).latch_pos;
            (*occurred_events).user_data =
                (*(*set).events.add((*set).latch_pos as usize)).user_data;
            (*occurred_events).events = WL_LATCH_SET as uint32;
            occurred_events = occurred_events.add(1);
            returned_events += 1;

            /* could have been set above */
            (*(*set).latch).maybe_sleeping = false as _;

            if returned_events == nevents {
                break; /* output buffer full already */
            }

            /*
             * Even though we already have an event, we'll poll just once with
             * zero timeout to see what non-latch events we can fit into the
             * output buffer at the same time.
             */
            cur_timeout = 0;
            timeout = 0;
        }

        /*
         * Wait for events using the readiness primitive chosen at the top of
         * this file. If -1 is returned, a timeout has occurred, if 0 we have
         * to retry, everything >= 1 is the number of returned events.
         */
        rc = WaitEventSetWaitBlock(set, cur_timeout as c_int, occurred_events, nevents - returned_events);

        if !(*set).latch.is_null() && (*(*set).latch).maybe_sleeping != 0 {
            (*(*set).latch).maybe_sleeping = false as _;
        }

        if rc == -1 {
            break; /* timeout occurred */
        } else {
            returned_events += rc;
        }

        /* If we're not done, update cur_timeout for next iteration */
        if returned_events == 0 && timeout >= 0 {
            INSTR_TIME_SET_CURRENT(&mut cur_time);
            INSTR_TIME_SUBTRACT(&mut cur_time, start_time);
            cur_timeout = timeout - INSTR_TIME_GET_MILLISEC(cur_time) as c_long;
            if cur_timeout <= 0 {
                break;
            }
        }
    }
    waiting = false;

    pgstat_report_wait_end();

    returned_events
}

/*
 * Wait using kevent(2) on BSD-family systems and macOS.
 *
 * For now this mirrors the epoll code, but in future it could modify the fd
 * set in the same call to kevent as it uses for waiting instead of doing that
 * with separate system calls.
 */
unsafe fn WaitEventSetWaitBlock(
    set: *mut WaitEventSet,
    cur_timeout: c_int,
    mut occurred_events: *mut WaitEvent,
    nevents: c_int,
) -> c_int {
    let mut returned_events: c_int = 0;
    let rc: c_int;
    let mut cur_event: *mut WaitEvent;
    let mut cur_kqueue_event: *mut libc::kevent;
    let mut timeout: libc::timespec = core::mem::zeroed();
    let timeout_p: *const libc::timespec;

    if cur_timeout < 0 {
        timeout_p = null();
    } else {
        timeout.tv_sec = (cur_timeout / 1000) as libc::time_t;
        timeout.tv_nsec = ((cur_timeout % 1000) * 1000000) as _;
        timeout_p = &timeout;
    }

    /*
     * Report postmaster events discovered by WaitEventAdjustKqueue() or an
     * earlier call to WaitEventSetWait().
     */
    if (*set).report_postmaster_not_running {
        if (*set).exit_on_postmaster_death {
            proc_exit(1);
        }
        (*occurred_events).fd = PGINVALID_SOCKET;
        (*occurred_events).events = WL_POSTMASTER_DEATH as uint32;
        return 1;
    }

    /* Sleep */
    rc = libc::kevent(
        (*set).kqueue_fd,
        null(),
        0,
        (*set).kqueue_ret_events,
        Min(nevents, (*set).nevents_space),
        timeout_p,
    );

    /* Check return code */
    if rc < 0 {
        /* EINTR is okay, otherwise complain */
        if errno() != libc::EINTR {
            waiting = false;
            // C also: errcode_for_socket_access()
            ereport!(ERROR, errmsg!("{}() failed: {}", "kevent", strerror_errno()));
        }
        return 0;
    } else if rc == 0 {
        /* timeout exceeded */
        return -1;
    }

    /*
     * At least one event occurred, iterate over the returned kqueue events
     * until they're either all processed, or we've returned all the events
     * the caller desired.
     */
    cur_kqueue_event = (*set).kqueue_ret_events;
    while cur_kqueue_event < (*set).kqueue_ret_events.add(rc as usize) && returned_events < nevents {
        /* kevent's udata points to the associated WaitEvent */
        cur_event = AccessWaitEvent(cur_kqueue_event);

        (*occurred_events).pos = (*cur_event).pos;
        (*occurred_events).user_data = (*cur_event).user_data;
        (*occurred_events).events = 0;

        if (*cur_event).events == WL_LATCH_SET as uint32
            && (*cur_kqueue_event).filter == libc::EVFILT_SIGNAL
        {
            if !(*set).latch.is_null()
                && (*(*set).latch).maybe_sleeping != 0
                && (*(*set).latch).is_set != 0
            {
                (*occurred_events).fd = PGINVALID_SOCKET;
                (*occurred_events).events = WL_LATCH_SET as uint32;
                occurred_events = occurred_events.add(1);
                returned_events += 1;
            }
        } else if (*cur_event).events == WL_POSTMASTER_DEATH as uint32
            && (*cur_kqueue_event).filter == libc::EVFILT_PROC
            && ((*cur_kqueue_event).fflags & libc::NOTE_EXIT) != 0
        {
            /*
             * The kernel will tell this kqueue object only once about the
             * exit of the postmaster, so let's remember that for next time so
             * that we provide level-triggered semantics.
             */
            (*set).report_postmaster_not_running = true;

            if (*set).exit_on_postmaster_death {
                proc_exit(1);
            }
            (*occurred_events).fd = PGINVALID_SOCKET;
            (*occurred_events).events = WL_POSTMASTER_DEATH as uint32;
            occurred_events = occurred_events.add(1);
            returned_events += 1;
        } else if ((*cur_event).events
            & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED) as uint32)
            != 0
        {
            Assert!((*cur_event).fd >= 0);

            if ((*cur_event).events & WL_SOCKET_READABLE as uint32) != 0
                && (*cur_kqueue_event).filter == libc::EVFILT_READ
            {
                /* readable, or EOF */
                (*occurred_events).events |= WL_SOCKET_READABLE as uint32;
            }

            if ((*cur_event).events & WL_SOCKET_CLOSED as uint32) != 0
                && (*cur_kqueue_event).filter == libc::EVFILT_READ
                && ((*cur_kqueue_event).flags & libc::EV_EOF) != 0
            {
                /* the remote peer has shut down */
                (*occurred_events).events |= WL_SOCKET_CLOSED as uint32;
            }

            if ((*cur_event).events & WL_SOCKET_WRITEABLE as uint32) != 0
                && (*cur_kqueue_event).filter == libc::EVFILT_WRITE
            {
                /* writable, or EOF */
                (*occurred_events).events |= WL_SOCKET_WRITEABLE as uint32;
            }

            if (*occurred_events).events != 0 {
                (*occurred_events).fd = (*cur_event).fd;
                occurred_events = occurred_events.add(1);
                returned_events += 1;
            }
        }

        cur_kqueue_event = cur_kqueue_event.add(1);
    }

    returned_events
}

/*
 * Return whether the current build options can report WL_SOCKET_CLOSED.
 */
pub unsafe fn WaitEventSetCanReportClosed() -> bool {
    true
}

/*
 * Get the number of wait events registered in a given WaitEventSet.
 */
pub unsafe fn GetNumRegisteredWaitEvents(set: *mut WaitEventSet) -> c_int {
    (*set).nevents
}

/*
 * SetLatch uses SIGURG to wake up the process waiting on the latch.
 *
 * Wake up WaitLatch, if we're waiting.
 *
 * Part of the WAIT_USE_SELF_PIPE path. On macOS the primitive selected is
 * WAIT_USE_KQUEUE, so the self-pipe globals are not otherwise defined here.
 */
unsafe fn latch_sigurg_handler(_postgres_signal_arg: c_int) {
    if waiting {
        sendSelfPipeByte();
    }
}

/* Send one byte to the self-pipe, to wake up WaitLatch */
unsafe fn sendSelfPipeByte() {
    let mut rc: c_int;
    let dummy: c_char = 0;

    loop {
        // retry:
        rc = libc::write(selfpipe_writefd, &dummy as *const c_char as *const c_void, 1) as c_int;
        if rc < 0 {
            /* If interrupted by signal, just retry */
            if errno() == libc::EINTR {
                continue;
            }

            /*
             * If the pipe is full, we don't need to retry, the data that's there
             * already is enough to wake up WaitLatch.
             */
            if errno() == libc::EAGAIN || errno() == libc::EWOULDBLOCK {
                return;
            }

            /*
             * Oops, the write() failed for some other reason. We might be in a
             * signal handler, so it's not safe to elog(). We have no choice but
             * silently ignore the error.
             */
            return;
        }
        break;
    }
}

/*
 * Read all available data from self-pipe or signalfd.
 *
 * Note: this is only called when waiting = true.  If it fails and doesn't
 * return, it must reset that flag first (though ideally, this will never
 * happen).
 */
unsafe fn drain() {
    let mut buf: [c_char; 1024] = [0; 1024];
    let mut rc: c_int;
    let fd: c_int;

    fd = selfpipe_readfd;

    loop {
        rc = libc::read(fd, buf.as_mut_ptr() as *mut c_void, core::mem::size_of_val(&buf)) as c_int;
        if rc < 0 {
            if errno() == libc::EAGAIN || errno() == libc::EWOULDBLOCK {
                break; /* the descriptor is empty */
            } else if errno() == libc::EINTR {
                continue; /* retry */
            } else {
                waiting = false;
                elog!(ERROR, "read() on self-pipe failed: {}", strerror_errno());
            }
        } else if rc == 0 {
            waiting = false;
            elog!(ERROR, "unexpected EOF on self-pipe");
        } else if (rc as usize) < core::mem::size_of_val(&buf) {
            /* we successfully drained the pipe; no need to read() again */
            break;
        }
        /* else buffer wasn't big enough, so read again */
    }
}

unsafe fn ResOwnerReleaseWaitEventSet(res: Datum) {
    let set: *mut WaitEventSet = DatumGetPointer(res) as *mut WaitEventSet;

    Assert!(!(*set).owner.is_null());
    (*set).owner = null_mut();
    FreeWaitEventSet(set);
}

/*
 * Wake up my process if it's currently sleeping in WaitEventSetWaitBlock()
 *
 * NB: be sure to save and restore errno around it.  (That's standard practice
 * in most signal handlers, of course, but we used to omit it in handlers that
 * only set a flag.) XXX
 *
 * NB: this function is called from critical sections and signal handlers so
 * throwing an error is not a good idea.
 *
 * On Windows, Latch uses SetEvent directly and this is not used.
 */
pub unsafe fn WakeupMyProc() {
    if waiting {
        libc::kill(MyProcPid, libc::SIGURG);
    }
}

/* Similar to WakeupMyProc, but wake up another process */
pub unsafe fn WakeupOtherProc(pid: c_int) {
    libc::kill(pid, libc::SIGURG);
}

// ---------------------------------------------------------------------------
// Dependency stubs (functions defined in OTHER .c files, not yet ported).
// ---------------------------------------------------------------------------

// postmaster.c globals (storage/ipc.h). The watch end of the pipe a child
// reads to detect postmaster death.
const POSTMASTER_FD_WATCH: usize = 0;
static mut postmaster_alive_fds: [c_int; 2] = [-1, -1];

// WAIT_USE_SELF_PIPE globals. Not used on the macOS/kqueue path this file
// translates; present so the self-pipe handler/drain helpers stay self-consistent.
// TODO(pg-port): only meaningful under WAIT_USE_SELF_PIPE.
static mut selfpipe_readfd: c_int = -1;
static mut selfpipe_writefd: c_int = -1;

// utils/activity/pgstat_wait.c.
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {
    // TODO(pg-port): import from utils/activity/wait_event.rs once ported.
}
unsafe fn pgstat_report_wait_end() {
    // TODO(pg-port): import from utils/activity/wait_event.rs once ported.
}

// storage/ipc/pmsignal.c. PostmasterIsAlive() wraps PostmasterIsAliveInternal().
unsafe fn PostmasterIsAlive() -> bool {
    // TODO(pg-port): import from storage/ipc/pmsignal.rs once exported.
    PostmasterIsAliveInternal()
}

// port/atomics.h.
unsafe fn pg_memory_barrier_impl() {
    // TODO(pg-port): import from port/atomics/generic.rs.
    core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);
}

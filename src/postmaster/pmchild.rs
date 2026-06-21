//! postmaster/pmchild.c - track postmaster child processes via per-type PMChild pools.

use crate::prelude::*;

use crate::lib::ilist::{
    dlist_delete, dlist_head, dlist_init, dlist_is_empty, dlist_iter, dlist_pop_head_node,
    dlist_push_head, dlist_push_tail,
};
use crate::miscadmin::{
    BackendType, BACKEND_NUM_TYPES, B_ARCHIVER, B_AUTOVAC_LAUNCHER, B_AUTOVAC_WORKER, B_BACKEND,
    B_BG_WORKER, B_BG_WRITER, B_CHECKPOINTER, B_DEAD_END_BACKEND, B_INVALID, B_IO_WORKER, B_LOGGER,
    B_SLOTSYNC_WORKER, B_STARTUP, B_WAL_RECEIVER, B_WAL_SENDER, B_WAL_SUMMARIZER, B_WAL_WRITER,
};
use crate::postmaster::bgworker_internals::RegisteredBgWorker;
use crate::utils::palloc::{palloc, palloc_extended, pfree, MCXT_ALLOC_NO_OOM};

use crate::{dlist_container, dlist_foreach};

use core::ffi::c_char;

// ----------------------------------------------------------------------------
// External GUCs / globals referenced by this file (defined in globals.c).
// MaxConnections and max_worker_processes are ported in globals.rs; the rest
// (max_wal_senders, autovacuum_worker_slots) are not yet ported -- stub them.
// ----------------------------------------------------------------------------
use crate::miscadmin::{max_worker_processes, MaxConnections};

// STUB: defined in walsender.c (not yet ported). // TODO
static mut max_wal_senders: c_int = 0;
// STUB: defined in autovacuum.c (not yet ported). // TODO
static mut autovacuum_worker_slots: c_int = 0;

// STUB: io_worker.h MAX_IO_WORKERS (not yet ported). // TODO
const MAX_IO_WORKERS: c_int = 32;

// ----------------------------------------------------------------------------
// PMChild struct (from postmaster.h).
// ----------------------------------------------------------------------------
/// One slot tracking a postmaster child process.
#[repr(C)]
pub struct PMChild {
    pub pid: c_int,           /* process id of backend (pid_t) */
    pub child_slot: c_int,    /* PMChildSlot for this backend, if any */
    pub bkend_type: BackendType, /* child process flavor */
    pub rw: *mut RegisteredBgWorker, /* bgworker info, if this is a bgworker */
    pub bgworker_notify: bool, /* gets bgworker start/stop notifications */
    pub elem: crate::lib::ilist::dlist_node, /* list link in ActiveChildList */
}

/*
 * Freelists for different kinds of child processes.  We maintain separate
 * pools for each, so that for example launching a lot of regular backends
 * cannot prevent autovacuum or an aux process from launching.
 */
#[repr(C)]
struct PMChildPool {
    size: c_int,          /* number of PMChild slots reserved for this kind */
    first_slotno: c_int,  /* first slot belonging to this pool */
    freelist: dlist_head, /* currently unused PMChild entries */
}

impl PMChildPool {
    const fn zeroed() -> Self {
        PMChildPool {
            size: 0,
            first_slotno: 0,
            freelist: dlist_head {
                head: crate::lib::ilist::dlist_node {
                    prev: null_mut(),
                    next: null_mut(),
                },
            },
        }
    }
}

static mut pmchild_pools: [PMChildPool; BACKEND_NUM_TYPES as usize] =
    [const { PMChildPool::zeroed() }; BACKEND_NUM_TYPES as usize];

// NON_EXEC_STATIC int num_pmchild_slots = 0;
pub static mut num_pmchild_slots: c_int = 0;

/*
 * List of active child processes.  This includes dead-end children.
 */
pub static mut ActiveChildList: dlist_head = dlist_head {
    head: crate::lib::ilist::dlist_node {
        prev: null_mut(),
        next: null_mut(),
    },
};

/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of postmaster child processes that can be active.
 * It includes all children except for dead-end children.  This allows the
 * array in shared memory (PMChildFlags) to have a fixed maximum size.
 */
pub unsafe fn MaxLivePostmasterChildren() -> c_int {
    if num_pmchild_slots == 0 {
        elog!(ERROR, "PM child array not initialized yet");
    }
    num_pmchild_slots
}

/*
 * Initialize at postmaster startup
 *
 * Note: This is not called on crash restart.  We rely on PMChild entries to
 * remain valid through the restart process.  This is important because the
 * syslogger survives through the crash restart process, so we must not
 * invalidate its PMChild slot.
 */
pub unsafe fn InitPostmasterChildSlots() {
    let mut slotno: c_int;
    let slots: *mut PMChild;

    /*
     * We allow more connections here than we can have backends because some
     * might still be authenticating; they might fail auth, or some existing
     * backend might exit before the auth cycle is completed.  The exact
     * MaxConnections limit is enforced when a new backend tries to join the
     * PGPROC array.
     *
     * WAL senders start out as regular backends, so they share the same pool.
     */
    pmchild_pools[B_BACKEND as usize].size = 2 * (MaxConnections + max_wal_senders);

    pmchild_pools[B_AUTOVAC_WORKER as usize].size = autovacuum_worker_slots;
    pmchild_pools[B_BG_WORKER as usize].size = max_worker_processes;
    pmchild_pools[B_IO_WORKER as usize].size = MAX_IO_WORKERS;

    /*
     * There can be only one of each of these running at a time.  They each
     * get their own pool of just one entry.
     */
    pmchild_pools[B_AUTOVAC_LAUNCHER as usize].size = 1;
    pmchild_pools[B_SLOTSYNC_WORKER as usize].size = 1;
    pmchild_pools[B_ARCHIVER as usize].size = 1;
    pmchild_pools[B_BG_WRITER as usize].size = 1;
    pmchild_pools[B_CHECKPOINTER as usize].size = 1;
    pmchild_pools[B_STARTUP as usize].size = 1;
    pmchild_pools[B_WAL_RECEIVER as usize].size = 1;
    pmchild_pools[B_WAL_SUMMARIZER as usize].size = 1;
    pmchild_pools[B_WAL_WRITER as usize].size = 1;
    pmchild_pools[B_LOGGER as usize].size = 1;

    /* The rest of the pmchild_pools are left at zero size */

    /* Count the total number of slots */
    num_pmchild_slots = 0;
    for i in 0..BACKEND_NUM_TYPES {
        num_pmchild_slots += pmchild_pools[i as usize].size;
    }

    /* Initialize them */
    slots = palloc(num_pmchild_slots as usize * core::mem::size_of::<PMChild>()) as *mut PMChild;
    slotno = 0;
    for btype in 0..BACKEND_NUM_TYPES {
        pmchild_pools[btype as usize].first_slotno = slotno + 1;
        dlist_init(&mut pmchild_pools[btype as usize].freelist);

        for _j in 0..pmchild_pools[btype as usize].size {
            let s = slots.add(slotno as usize);
            (*s).pid = 0;
            (*s).child_slot = slotno + 1;
            (*s).bkend_type = B_INVALID;
            (*s).rw = null_mut();
            (*s).bgworker_notify = false;
            dlist_push_tail(&mut pmchild_pools[btype as usize].freelist, &mut (*s).elem);
            slotno += 1;
        }
    }
    Assert!(slotno == num_pmchild_slots);

    /* Initialize other structures */
    dlist_init(&mut ActiveChildList);
}

/*
 * Allocate a PMChild entry for a postmaster child process of given type.
 *
 * The entry is taken from the right pool for the type.
 *
 * pmchild->child_slot in the returned struct is unique among all active child
 * processes.
 */
pub unsafe fn AssignPostmasterChildSlot(btype: BackendType) -> *mut PMChild {
    let freelist: *mut dlist_head;
    let pmchild: *mut PMChild;

    if pmchild_pools[btype as usize].size == 0 {
        elog!(
            ERROR,
            "cannot allocate a PMChild slot for backend type {}",
            btype
        );
    }

    freelist = &mut pmchild_pools[btype as usize].freelist;
    if dlist_is_empty(freelist) {
        return null_mut();
    }

    pmchild = dlist_container!(PMChild, elem, dlist_pop_head_node(freelist));
    (*pmchild).pid = 0;
    (*pmchild).bkend_type = btype;
    (*pmchild).rw = null_mut();
    (*pmchild).bgworker_notify = true;

    /*
     * pmchild->child_slot for each entry was initialized when the array of
     * slots was allocated.  Sanity check it.
     */
    if !((*pmchild).child_slot >= pmchild_pools[btype as usize].first_slotno
        && (*pmchild).child_slot
            < pmchild_pools[btype as usize].first_slotno + pmchild_pools[btype as usize].size)
    {
        elog!(
            ERROR,
            "pmchild freelist for backend type {} is corrupt",
            (*pmchild).bkend_type
        );
    }

    dlist_push_head(&mut ActiveChildList, &mut (*pmchild).elem);

    /* Update the status in the shared memory array */
    MarkPostmasterChildSlotAssigned((*pmchild).child_slot);

    // PostmasterChildName returns a C string ("%s" in the C source); render it
    // lossily for the log message.
    let name_ptr = PostmasterChildName(btype);
    let name = if name_ptr.is_null() {
        "(null)".to_string()
    } else {
        core::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    elog!(
        DEBUG2,
        "assigned pm child slot {} for {}",
        (*pmchild).child_slot,
        name
    );

    pmchild
}

/*
 * Allocate a PMChild struct for a dead-end backend.  Dead-end children are
 * not assigned a child_slot number.  The struct is palloc'd; returns NULL if
 * out of memory.
 */
pub unsafe fn AllocDeadEndChild() -> *mut PMChild {
    let pmchild: *mut PMChild;

    elog!(DEBUG2, "allocating dead-end child");

    pmchild = palloc_extended(core::mem::size_of::<PMChild>(), MCXT_ALLOC_NO_OOM) as *mut PMChild;
    if !pmchild.is_null() {
        (*pmchild).pid = 0;
        (*pmchild).child_slot = 0;
        (*pmchild).bkend_type = B_DEAD_END_BACKEND;
        (*pmchild).rw = null_mut();
        (*pmchild).bgworker_notify = false;

        dlist_push_head(&mut ActiveChildList, &mut (*pmchild).elem);
    }

    pmchild
}

/*
 * Release a PMChild slot, after the child process has exited.
 *
 * Returns true if the child detached cleanly from shared memory, false
 * otherwise (see MarkPostmasterChildSlotUnassigned).
 */
pub unsafe fn ReleasePostmasterChildSlot(pmchild: *mut PMChild) -> bool {
    dlist_delete(&mut (*pmchild).elem);
    if (*pmchild).bkend_type == B_DEAD_END_BACKEND {
        elog!(DEBUG2, "releasing dead-end backend");
        pfree(pmchild as *mut c_void);
        true
    } else {
        let pool: *mut PMChildPool;

        elog!(DEBUG2, "releasing pm child slot {}", (*pmchild).child_slot);

        /* WAL senders start out as regular backends, and share the pool */
        if (*pmchild).bkend_type == B_WAL_SENDER {
            pool = &mut pmchild_pools[B_BACKEND as usize];
        } else {
            pool = &mut pmchild_pools[(*pmchild).bkend_type as usize];
        }

        /* sanity check that we return the entry to the right pool */
        if !((*pmchild).child_slot >= (*pool).first_slotno
            && (*pmchild).child_slot < (*pool).first_slotno + (*pool).size)
        {
            elog!(
                ERROR,
                "pmchild freelist for backend type {} is corrupt",
                (*pmchild).bkend_type
            );
        }

        dlist_push_head(&mut (*pool).freelist, &mut (*pmchild).elem);
        MarkPostmasterChildSlotUnassigned((*pmchild).child_slot)
    }
}

/*
 * Find the PMChild entry of a running child process by PID.
 */
pub unsafe fn FindPostmasterChildByPid(pid: c_int) -> *mut PMChild {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &mut ActiveChildList, {
        let bp: *mut PMChild = dlist_container!(PMChild, elem, iter.cur);

        if (*bp).pid == pid {
            return bp;
        }
    });
    null_mut()
}

// ----------------------------------------------------------------------------
// Local stubs for not-yet-ported callees.
// ----------------------------------------------------------------------------

unsafe fn MarkPostmasterChildSlotAssigned(slot: c_int) {
    crate::storage::ipc::pmsignal::MarkPostmasterChildSlotAssigned(slot)
}

unsafe fn MarkPostmasterChildSlotUnassigned(slot: c_int) -> bool {
    crate::storage::ipc::pmsignal::MarkPostmasterChildSlotUnassigned(slot)
}

/// launch_backend.c PostmasterChildName: backend-type description for log messages.
/// TODO(pg-port): real GetBackendTypeDesc switch; a placeholder suffices for bring-up logs.
unsafe fn PostmasterChildName(_child_type: BackendType) -> *const c_char {
    c"postmaster child".as_ptr()
}

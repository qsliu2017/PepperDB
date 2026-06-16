//! src/backend/storage/ipc/dsm.c
//!   manage dynamic shared memory segments
//!
//! Merged companion header: src/include/storage/dsm.h
//!
//! This file provides a set of services to make programming with dynamic
//! shared memory segments more convenient.  Unlike the low-level
//! facilities provided by dsm_impl.h and dsm_impl.c, mappings and segments
//! created using this module will be cleaned up automatically.  Mappings
//! will be removed when the resource owner under which they were created
//! is cleaned up, unless dsm_pin_mapping() is used, in which case they
//! have session lifespan.  Segments will be removed when there are no
//! remaining mappings, or at postmaster shutdown in any case.  After a
//! hard postmaster crash, remaining segments will be removed, if they
//! still exist, at the next postmaster startup.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/ipc/dsm.c

use crate::prelude::*;
use crate::{elog, ereport, Assert, DLIST_STATIC_INIT};
use crate::dlist_container;
use crate::dlist_foreach;
use crate::slist_container;
use crate::slist_foreach_modify;

use crate::storage::ipc::dsm_impl::{
    dsm_handle, dsm_impl_op, dsm_impl_pin_segment, dsm_impl_unpin_segment,
    dynamic_shared_memory_type, DSM_HANDLE_INVALID, DSM_IMPL_MMAP,
    DSM_OP_ATTACH, DSM_OP_CREATE, DSM_OP_DESTROY, DSM_OP_DETACH,
};

use crate::storage::pg_shmem::PGShmemHeader;

use crate::utils::resowner::resowner::{
    CurrentResourceOwner, ResourceOwner, ResourceOwnerData, ResourceOwnerDesc,
    ResourceOwnerEnlarge, ResourceOwnerForget, ResourceOwnerRemember,
    ResourceReleasePriority, ResourceReleasePhase, RELEASE_PRIO_DSMS,
    RESOURCE_RELEASE_BEFORE_LOCKS,
};

use crate::lib::ilist::{
    dlist_head, dlist_init, dlist_is_empty, dlist_node, dlist_push_head,
    dlist_delete, dlist_iter,
    slist_head, slist_init, slist_is_empty, slist_node, slist_pop_head_node,
    slist_push_head, slist_delete_current, slist_mutable_iter,
};

use crate::miscadmin::{
    HOLD_INTERRUPTS, RESUME_INTERRUPTS,
    CHECK_FOR_INTERRUPTS,
};

use crate::common::pg_prng::{pg_global_prng_state, pg_prng_uint32};
use crate::port::pg_bitutils::pg_leftmost_one_pos32;
use crate::pg_config_manual::MAXPGPATH;

use std::ffi::{c_char, c_int, c_void};

// ----------------------------------------------------------------
// dsm.h public interface
// ----------------------------------------------------------------

/// Flag for dsm_create: return NULL instead of erroring if the per-backend
/// DSM segment limit is reached.
pub const DSM_CREATE_NULL_IF_MAXSEGMENTS: c_int = 0x0001;

/// Callback type for on-detach hooks.
pub type on_dsm_detach_callback =
    unsafe fn(seg: *mut dsm_segment, arg: Datum);

// ----------------------------------------------------------------
// Internal constants
// ----------------------------------------------------------------

const PG_DYNSHMEM_CONTROL_MAGIC: uint32 = 0x9a503d32;
const PG_DYNSHMEM_FIXED_SLOTS: uint32 = 64;
const PG_DYNSHMEM_SLOTS_PER_BACKEND: uint32 = 5;
const INVALID_CONTROL_SLOT: uint32 = uint32::MAX;

// ----------------------------------------------------------------
// Backend-local callback list node
// ----------------------------------------------------------------

/// Backend-local tracking for on-detach callbacks.
#[repr(C)]
struct dsm_segment_detach_callback {
    function: on_dsm_detach_callback,
    arg: Datum,
    node: slist_node,
}

// ----------------------------------------------------------------
// dsm_segment -- backend-local state for a single mapping
// ----------------------------------------------------------------

/// Backend-local state for a dynamic shared memory segment.
#[repr(C)]
pub struct dsm_segment {
    /// List link in dsm_segment_list.
    node: dlist_node,
    /// Resource owner.
    resowner: ResourceOwner,
    /// Segment name.
    handle: dsm_handle,
    /// Slot in control segment.
    control_slot: uint32,
    /// Implementation-specific private data.
    impl_private: *mut c_void,
    /// Mapping address, or NULL if unmapped.
    mapped_address: *mut c_void,
    /// Size of our mapping.
    mapped_size: Size,
    /// On-detach callbacks.
    on_detach: slist_head,
}

// ----------------------------------------------------------------
// Shared-memory control segment layout
// ----------------------------------------------------------------

/// Shared-memory state for a single dynamic shared memory segment.
#[repr(C)]
struct dsm_control_item {
    handle: dsm_handle,
    /// 2+ = active, 1 = moribund, 0 = gone
    refcnt: uint32,
    first_page: usize,
    npages: usize,
    /// Only needed on Windows.
    impl_private_pm_handle: *mut c_void,
    pinned: bool,
}

/// Layout of the dynamic shared memory control segment.
#[repr(C)]
struct dsm_control_header {
    magic: uint32,
    nitems: uint32,
    maxitems: uint32,
    /// Flexible array member -- access via raw pointer arithmetic.
    item: [dsm_control_item; 0],
}

// ----------------------------------------------------------------
// Module-level statics
// ----------------------------------------------------------------

/// Has this backend initialized the dynamic shared memory system yet?
static mut dsm_init_done: bool = false;

/// Preallocated DSM space in the main shared memory region.
static mut dsm_main_space_begin: *mut c_void = std::ptr::null_mut();

/// List of dynamic shared memory segments used by this backend.
static mut dsm_segment_list: dlist_head = dlist_head {
    head: crate::lib::ilist::dlist_node {
        prev: std::ptr::null_mut(),
        next: std::ptr::null_mut(),
    },
};

/// Handle for the DSM control segment (backend-inherited or re-read).
static mut dsm_control_handle: dsm_handle = 0;
/// Pointer into the control segment.
static mut dsm_control: *mut dsm_control_header = std::ptr::null_mut();
static mut dsm_control_mapped_size: Size = 0;
static mut dsm_control_impl_private: *mut c_void = std::ptr::null_mut();

// ----------------------------------------------------------------
// ResourceOwner integration
// ----------------------------------------------------------------

unsafe fn ResOwnerReleaseDSM(res: Datum) {
    let seg: *mut dsm_segment = DatumGetPointer(res) as *mut dsm_segment;
    (*seg).resowner = std::ptr::null_mut();
    dsm_detach(seg);
}

unsafe fn ResOwnerPrintDSM(res: Datum) -> *mut c_char {
    let seg: *mut dsm_segment = DatumGetPointer(res) as *mut dsm_segment;
    psprintf_segment(dsm_segment_handle(seg))
}

const dsm_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    name: c"dynamic shared memory segment".as_ptr(),
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: RELEASE_PRIO_DSMS,
    ReleaseResource: ResOwnerReleaseDSM,
    DebugPrint: Some(ResOwnerPrintDSM),
};

#[inline]
unsafe fn ResourceOwnerRememberDSM(owner: ResourceOwner, seg: *mut dsm_segment) {
    ResourceOwnerRemember(owner, PointerGetDatum(seg as *const c_void), &dsm_resowner_desc);
}

#[inline]
unsafe fn ResourceOwnerForgetDSM(owner: ResourceOwner, seg: *mut dsm_segment) {
    ResourceOwnerForget(owner, PointerGetDatum(seg as *const c_void), &dsm_resowner_desc);
}

// ----------------------------------------------------------------
// Startup
// ----------------------------------------------------------------

/*
 * Start up the dynamic shared memory system.
 *
 * This is called just once during each cluster lifetime, at postmaster
 * startup time.
 */
pub unsafe fn dsm_postmaster_startup(shim: *mut PGShmemHeader) {
    let mut dsm_control_address: *mut c_void = std::ptr::null_mut();
    let maxitems: uint32;
    let segsize: Size;

    Assert!(!IsUnderPostmaster);

    /*
     * If we're using the mmap implementation, clean up any leftovers.
     * Cleanup isn't needed on Windows, and happens earlier in startup for
     * POSIX and System V shared memory, via a direct call to
     * dsm_cleanup_using_control_segment.
     */
    if dynamic_shared_memory_type == DSM_IMPL_MMAP {
        dsm_cleanup_for_mmap();
    }

    /* Determine size for new control segment. */
    maxitems = PG_DYNSHMEM_FIXED_SLOTS
        + PG_DYNSHMEM_SLOTS_PER_BACKEND * MaxBackends as uint32;
    elog!(
        DEBUG2,
        "dynamic shared memory system will support {} segments",
        maxitems
    );
    segsize = dsm_control_bytes_needed(maxitems) as Size;

    /*
     * Loop until we find an unused identifier for the new control segment. We
     * sometimes use DSM_HANDLE_INVALID as a sentinel value indicating "no
     * control segment", so avoid generating that value for a real handle.
     */
    loop {
        Assert!(dsm_control_address.is_null());
        Assert!(dsm_control_mapped_size == 0);
        /* Use even numbers only. */
        dsm_control_handle = pg_prng_uint32(&raw mut pg_global_prng_state) << 1;
        if dsm_control_handle == DSM_HANDLE_INVALID {
            continue;
        }
        if dsm_impl_op(
            DSM_OP_CREATE,
            dsm_control_handle,
            segsize,
            &raw mut dsm_control_impl_private,
            &mut dsm_control_address,
            &raw mut dsm_control_mapped_size,
            ERROR,
        ) {
            break;
        }
    }
    dsm_control = dsm_control_address as *mut dsm_control_header;
    on_shmem_exit(dsm_postmaster_shutdown, PointerGetDatum(shim as *const c_void));
    elog!(
        DEBUG2,
        "created dynamic shared memory control segment {} ({} bytes)",
        dsm_control_handle,
        segsize
    );
    (*shim).dsm_control = dsm_control_handle;

    /* Initialize control segment. */
    (*dsm_control).magic = PG_DYNSHMEM_CONTROL_MAGIC;
    (*dsm_control).nitems = 0;
    (*dsm_control).maxitems = maxitems;
}

/*
 * Determine whether the control segment from the previous postmaster
 * invocation still exists.  If so, remove the dynamic shared memory
 * segments to which it refers, and then the control segment itself.
 */
pub unsafe fn dsm_cleanup_using_control_segment(old_control_handle: dsm_handle) {
    let mut mapped_address: *mut c_void = std::ptr::null_mut();
    let mut junk_mapped_address: *mut c_void = std::ptr::null_mut();
    let mut impl_private: *mut c_void = std::ptr::null_mut();
    let mut junk_impl_private: *mut c_void = std::ptr::null_mut();
    let mut mapped_size: Size = 0;
    let mut junk_mapped_size: Size = 0;
    let nitems: uint32;
    let old_control: *mut dsm_control_header;

    /*
     * Try to attach the segment.  If this fails, it probably just means that
     * the operating system has been rebooted and the segment no longer
     * exists, or an unrelated process has used the same shm ID.  So just
     * fall out quietly.
     */
    if !dsm_impl_op(
        DSM_OP_ATTACH,
        old_control_handle,
        0,
        &mut impl_private,
        &mut mapped_address,
        &mut mapped_size,
        DEBUG1,
    ) {
        return;
    }

    /*
     * We've managed to reattach it, but the contents might not be sane. If
     * they aren't, we disregard the segment after all.
     */
    old_control = mapped_address as *mut dsm_control_header;
    if !dsm_control_segment_sane(old_control, mapped_size) {
        dsm_impl_op(
            DSM_OP_DETACH,
            old_control_handle,
            0,
            &mut impl_private,
            &mut mapped_address,
            &mut mapped_size,
            LOG,
        );
        return;
    }

    /*
     * OK, the control segment looks basically valid, so we can use it to get
     * a list of segments that need to be removed.
     */
    nitems = (*old_control).nitems;
    let mut i: uint32 = 0;
    while i < nitems {
        let handle: dsm_handle;
        let refcnt: uint32;

        /* If the reference count is 0, the slot is actually unused. */
        refcnt = control_item(old_control, i).refcnt;
        if refcnt == 0 {
            i += 1;
            continue;
        }

        /* If it was using the main shmem area, there is nothing to do. */
        handle = control_item(old_control, i).handle;
        if is_main_region_dsm_handle(handle) {
            i += 1;
            continue;
        }

        /* Log debugging information. */
        elog!(
            DEBUG2,
            "cleaning up orphaned dynamic shared memory with ID {} (reference count {})",
            handle,
            refcnt
        );

        /* Destroy the referenced segment. */
        dsm_impl_op(
            DSM_OP_DESTROY,
            handle,
            0,
            &mut junk_impl_private,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        );
        i += 1;
    }

    /* Destroy the old control segment, too. */
    elog!(
        DEBUG2,
        "cleaning up dynamic shared memory control segment with ID {}",
        old_control_handle
    );
    dsm_impl_op(
        DSM_OP_DESTROY,
        old_control_handle,
        0,
        &mut impl_private,
        &mut mapped_address,
        &mut mapped_size,
        LOG,
    );
}

/*
 * When we're using the mmap shared memory implementation, "shared memory"
 * segments might even manage to survive an operating system reboot.
 * But there's no guarantee as to exactly what will survive: some segments
 * may survive, and others may not, and the contents of some may be out
 * of date.  In particular, the control segment may be out of date, so we
 * can't rely on it to figure out what to remove.  However, since we know
 * what directory contains the files we used as shared memory, we can simply
 * scan the directory and blow everything away that shouldn't be there.
 */
unsafe fn dsm_cleanup_for_mmap() {
    let dir: *mut DIR;
    let mut dent: *mut dirent;

    /* Scan the directory for something with a name of the correct format. */
    dir = AllocateDir(c"pg_dynshmem".as_ptr());

    loop {
        dent = ReadDir(dir, c"pg_dynshmem".as_ptr());
        if dent.is_null() {
            break;
        }
        if strncmp(
            (*dent).d_name.as_ptr(),
            c"mmap.".as_ptr(),
            5, /* strlen("mmap.") */
        ) == 0
        {
            let mut buf: [c_char; MAXPGPATH + 10 /* sizeof("pg_dynshmem") */] =
                [0; MAXPGPATH + 10];

            snprintf(
                buf.as_mut_ptr(),
                buf.len(),
                c"pg_dynshmem/%s".as_ptr(),
                (*dent).d_name.as_ptr(),
            );

            elog!(DEBUG2, "removing file \"{}\"", CStr(buf.as_ptr()));

            /* We found a matching file; so remove it. */
            if unlink(buf.as_ptr()) != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not remove file \"{}\"", CStr(buf.as_ptr()))
                );
            }
        }
    }

    /* Cleanup complete. */
    FreeDir(dir);
}

/*
 * At shutdown time, we iterate over the control segment and remove all
 * remaining dynamic shared memory segments.  We avoid throwing errors here;
 * the postmaster is shutting down either way, and this is just non-critical
 * resource cleanup.
 */
unsafe fn dsm_postmaster_shutdown(code: c_int, arg: Datum) {
    let nitems: uint32;
    let mut dsm_control_address: *mut c_void;
    let mut junk_mapped_address: *mut c_void = std::ptr::null_mut();
    let mut junk_impl_private: *mut c_void = std::ptr::null_mut();
    let mut junk_mapped_size: Size = 0;
    let shim: *mut PGShmemHeader = DatumGetPointer(arg) as *mut PGShmemHeader;

    let _ = code;

    /*
     * If some other backend exited uncleanly, it might have corrupted the
     * control segment while it was dying.  In that case, we warn and ignore
     * the contents of the control segment.  This may end up leaving behind
     * stray shared memory segments, but there's not much we can do about
     * that if the metadata is gone.
     */
    nitems = (*dsm_control).nitems;
    if !dsm_control_segment_sane(dsm_control, dsm_control_mapped_size) {
        ereport!(LOG, "dynamic shared memory control segment is corrupt");
        return;
    }

    /* Remove any remaining segments. */
    let mut i: uint32 = 0;
    while i < nitems {
        let handle: dsm_handle;

        /* If the reference count is 0, the slot is actually unused. */
        if control_item(dsm_control, i).refcnt == 0 {
            i += 1;
            continue;
        }

        handle = control_item(dsm_control, i).handle;
        if is_main_region_dsm_handle(handle) {
            i += 1;
            continue;
        }

        /* Log debugging information. */
        elog!(
            DEBUG2,
            "cleaning up orphaned dynamic shared memory with ID {}",
            handle
        );

        /* Destroy the segment. */
        dsm_impl_op(
            DSM_OP_DESTROY,
            handle,
            0,
            &mut junk_impl_private,
            &mut junk_mapped_address,
            &mut junk_mapped_size,
            LOG,
        );
        i += 1;
    }

    /* Remove the control segment itself. */
    elog!(
        DEBUG2,
        "cleaning up dynamic shared memory control segment with ID {}",
        dsm_control_handle
    );
    dsm_control_address = dsm_control as *mut c_void;
    dsm_impl_op(
        DSM_OP_DESTROY,
        dsm_control_handle,
        0,
        &raw mut dsm_control_impl_private,
        &mut dsm_control_address,
        &raw mut dsm_control_mapped_size,
        LOG,
    );
    dsm_control = dsm_control_address as *mut dsm_control_header;
    (*shim).dsm_control = 0;
}

/*
 * Prepare this backend for dynamic shared memory usage.  Under EXEC_BACKEND,
 * we must reread the state file and map the control segment; in other cases,
 * we'll have inherited the postmaster's mapping and global variables.
 */
unsafe fn dsm_backend_startup() {
    /* EXEC_BACKEND path: re-attach control segment. */
    /* (EXEC_BACKEND-only control-segment re-attach path omitted: this port
     * models the non-EXEC_BACKEND build, like the rest of the codebase.) */

    dsm_init_done = true;
}

/*
 * When running under EXEC_BACKEND, we get a callback here when the main
 * shared memory segment is re-attached, so that we can record the control
 * handle retrieved from it.
 */
#[cfg(EXEC_BACKEND)]
pub unsafe fn dsm_set_control_handle(h: dsm_handle) {
    Assert!(dsm_control_handle == 0 && h != 0);
    dsm_control_handle = h;
}

/*
 * Reserve some space in the main shared memory segment for DSM segments.
 */
pub unsafe fn dsm_estimate_size() -> usize {
    1024 * 1024 * min_dynamic_shared_memory as usize
}

/*
 * Initialize space in the main shared memory segment for DSM segments.
 */
pub unsafe fn dsm_shmem_init() {
    let size: usize = dsm_estimate_size();
    let mut found: bool = false;

    if size == 0 {
        return;
    }

    dsm_main_space_begin =
        ShmemInitStruct(c"Preallocated DSM".as_ptr(), size, &mut found);
    if !found {
        let fpm: *mut FreePageManager = dsm_main_space_begin as *mut FreePageManager;
        let mut first_page: usize = 0;
        let pages: usize;

        /* Reserve space for the FreePageManager. */
        while first_page * FPM_PAGE_SIZE < std::mem::size_of::<FreePageManager>() {
            first_page += 1;
        }

        /* Initialize it and give it all the rest of the space. */
        FreePageManagerInitialize(fpm, dsm_main_space_begin);
        pages = (size / FPM_PAGE_SIZE) - first_page;
        FreePageManagerPut(fpm, first_page, pages);
    }
}

// ----------------------------------------------------------------
// Create / attach / detach
// ----------------------------------------------------------------

/*
 * Create a new dynamic shared memory segment.
 *
 * If there is a non-NULL CurrentResourceOwner, the new segment is associated
 * with it and must be detached before the resource owner releases, or a
 * warning will be logged.  If CurrentResourceOwner is NULL, the segment
 * remains attached until explicitly detached or the session ends.
 * Creating with a NULL CurrentResourceOwner is equivalent to creating
 * with a non-NULL CurrentResourceOwner and then calling dsm_pin_mapping.
 */
pub unsafe fn dsm_create(size: Size, flags: c_int) -> *mut dsm_segment {
    let seg: *mut dsm_segment;
    let mut i: uint32;
    let nitems: uint32;
    let mut npages: usize = 0;
    let mut first_page: usize = 0;
    let dsm_main_space_fpm: *mut FreePageManager =
        dsm_main_space_begin as *mut FreePageManager;
    let mut using_main_dsm_region: bool = false;

    /*
     * Unsafe in postmaster. It might seem pointless to allow use of dsm in
     * single user mode, but otherwise some subsystems will need dedicated
     * single user mode code paths.
     */
    Assert!(IsUnderPostmaster || !IsPostmasterEnvironment);

    if !dsm_init_done {
        dsm_backend_startup();
    }

    /* Create a new segment descriptor. */
    seg = dsm_create_descriptor();

    /*
     * Lock the control segment while we try to allocate from the main shared
     * memory area, if configured.
     */
    if !dsm_main_space_fpm.is_null() {
        npages = size / FPM_PAGE_SIZE;
        if size % FPM_PAGE_SIZE > 0 {
            npages += 1;
        }

        LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
        if FreePageManagerGet(dsm_main_space_fpm, npages, &mut first_page) {
            /* We can carve out a piece of the main shared memory segment. */
            (*seg).mapped_address =
                (dsm_main_space_begin as *mut c_char).add(first_page * FPM_PAGE_SIZE)
                    as *mut c_void;
            (*seg).mapped_size = npages * FPM_PAGE_SIZE;
            using_main_dsm_region = true;
            /* We'll choose a handle below. */
        }
    }

    if !using_main_dsm_region {
        /*
         * We need to create a new memory segment.  Loop until we find an
         * unused segment identifier.
         */
        if !dsm_main_space_fpm.is_null() {
            LWLockRelease(DynamicSharedMemoryControlLock);
        }
        loop {
            Assert!((*seg).mapped_address.is_null() && (*seg).mapped_size == 0);
            /* Use even numbers only. */
            (*seg).handle = pg_prng_uint32(&raw mut pg_global_prng_state) << 1;
            if (*seg).handle == DSM_HANDLE_INVALID {
                /* Reserve sentinel. */
                continue;
            }
            if dsm_impl_op(
                DSM_OP_CREATE,
                (*seg).handle,
                size,
                &mut (*seg).impl_private,
                &mut (*seg).mapped_address,
                &mut (*seg).mapped_size,
                ERROR,
            ) {
                break;
            }
        }
        LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    }

    /* Search the control segment for an unused slot. */
    nitems = (*dsm_control).nitems;
    i = 0;
    while i < nitems {
        if control_item(dsm_control, i).refcnt == 0 {
            if using_main_dsm_region {
                (*seg).handle = make_main_region_dsm_handle(i as c_int);
                control_item_mut(dsm_control, i).first_page = first_page;
                control_item_mut(dsm_control, i).npages = npages;
            } else {
                Assert!(!is_main_region_dsm_handle((*seg).handle));
            }
            control_item_mut(dsm_control, i).handle = (*seg).handle;
            /* refcnt of 1 triggers destruction, so start at 2 */
            control_item_mut(dsm_control, i).refcnt = 2;
            control_item_mut(dsm_control, i).impl_private_pm_handle = std::ptr::null_mut();
            control_item_mut(dsm_control, i).pinned = false;
            (*seg).control_slot = i;
            LWLockRelease(DynamicSharedMemoryControlLock);
            return seg;
        }
        i += 1;
    }

    /* Verify that we can support an additional mapping. */
    if nitems >= (*dsm_control).maxitems {
        if using_main_dsm_region {
            FreePageManagerPut(dsm_main_space_fpm, first_page, npages);
        }
        LWLockRelease(DynamicSharedMemoryControlLock);
        if !using_main_dsm_region {
            dsm_impl_op(
                DSM_OP_DESTROY,
                (*seg).handle,
                0,
                &mut (*seg).impl_private,
                &mut (*seg).mapped_address,
                &mut (*seg).mapped_size,
                WARNING,
            );
        }
        if !(*seg).resowner.is_null() {
            ResourceOwnerForgetDSM((*seg).resowner, seg);
        }
        dlist_delete(&mut (*seg).node);
        pfree(seg as *mut c_void);

        if (flags & DSM_CREATE_NULL_IF_MAXSEGMENTS) != 0 {
            return std::ptr::null_mut();
        }
        ereport!(ERROR, "too many dynamic shared memory segments");
    }

    /* Enter the handle into a new array slot. */
    if using_main_dsm_region {
        (*seg).handle = make_main_region_dsm_handle(nitems as c_int);
        control_item_mut(dsm_control, i).first_page = first_page;
        control_item_mut(dsm_control, i).npages = npages;
    }
    control_item_mut(dsm_control, nitems).handle = (*seg).handle;
    /* refcnt of 1 triggers destruction, so start at 2 */
    control_item_mut(dsm_control, nitems).refcnt = 2;
    control_item_mut(dsm_control, nitems).impl_private_pm_handle = std::ptr::null_mut();
    control_item_mut(dsm_control, nitems).pinned = false;
    (*seg).control_slot = nitems;
    (*dsm_control).nitems += 1;
    LWLockRelease(DynamicSharedMemoryControlLock);

    seg
}

/*
 * Attach a dynamic shared memory segment.
 *
 * See comments for dsm_segment_handle() for an explanation of how this
 * is intended to be used.
 *
 * This function will return NULL if the segment isn't known to the system.
 * This can happen if we're asked to attach the segment, but then everyone
 * else detaches it (causing it to be destroyed) before we get around to
 * attaching it.
 *
 * If there is a non-NULL CurrentResourceOwner, the attached segment is
 * associated with it and must be detached before the resource owner releases,
 * or a warning will be logged.  Otherwise the segment remains attached until
 * explicitly detached or the session ends.  See the note atop dsm_create().
 */
pub unsafe fn dsm_attach(h: dsm_handle) -> *mut dsm_segment {
    let seg: *mut dsm_segment;
    let mut iter: dlist_iter = dlist_iter {
        cur: std::ptr::null_mut(),
        end: std::ptr::null_mut(),
    };
    let nitems: uint32;

    /* Unsafe in postmaster (and pointless in a stand-alone backend). */
    Assert!(IsUnderPostmaster);

    if !dsm_init_done {
        dsm_backend_startup();
    }

    /*
     * Since this is just a debugging cross-check, we could leave it out
     * altogether, or include it only in assert-enabled builds.  But since the
     * list of attached segments should normally be very short, let's include
     * it always for right now.
     *
     * If you're hitting this error, you probably want to attempt to find an
     * existing mapping via dsm_find_mapping() before calling dsm_attach() to
     * create a new one.
     */
    dlist_foreach!(iter, &raw mut dsm_segment_list, {
        let check: *mut dsm_segment = dlist_container!(dsm_segment, node, iter.cur);
        if (*check).handle == h {
            elog!(ERROR, "can't attach the same segment more than once");
        }
    });

    /* Create a new segment descriptor. */
    seg = dsm_create_descriptor();
    (*seg).handle = h;

    /* Bump reference count for this segment in shared memory. */
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    nitems = (*dsm_control).nitems;
    let mut i: uint32 = 0;
    while i < nitems {
        /*
         * If the reference count is 0, the slot is actually unused.  If the
         * reference count is 1, the slot is still in use, but the segment is
         * in the process of going away; even if the handle matches, another
         * slot may already have started using the same handle value by
         * coincidence so we have to keep searching.
         */
        if control_item(dsm_control, i).refcnt <= 1 {
            i += 1;
            continue;
        }

        /* If the handle doesn't match, it's not the slot we want. */
        if control_item(dsm_control, i).handle != (*seg).handle {
            i += 1;
            continue;
        }

        /* Otherwise we've found a match. */
        control_item_mut(dsm_control, i).refcnt += 1;
        (*seg).control_slot = i;
        if is_main_region_dsm_handle((*seg).handle) {
            (*seg).mapped_address = (dsm_main_space_begin as *mut c_char)
                .add(control_item(dsm_control, i).first_page * FPM_PAGE_SIZE)
                as *mut c_void;
            (*seg).mapped_size =
                control_item(dsm_control, i).npages * FPM_PAGE_SIZE;
        }
        break;
    }
    LWLockRelease(DynamicSharedMemoryControlLock);

    /*
     * If we didn't find the handle we're looking for in the control segment,
     * it probably means that everyone else who had it mapped, including the
     * original creator, died before we got to this point. It's up to the
     * caller to decide what to do about that.
     */
    if (*seg).control_slot == INVALID_CONTROL_SLOT {
        dsm_detach(seg);
        return std::ptr::null_mut();
    }

    /* Here's where we actually try to map the segment. */
    if !is_main_region_dsm_handle((*seg).handle) {
        dsm_impl_op(
            DSM_OP_ATTACH,
            (*seg).handle,
            0,
            &mut (*seg).impl_private,
            &mut (*seg).mapped_address,
            &mut (*seg).mapped_size,
            ERROR,
        );
    }

    seg
}

/*
 * At backend shutdown time, detach any segments that are still attached.
 * (This is similar to dsm_detach_all, except that there's no reason to
 * unmap the control segment before exiting, so we don't bother.)
 */
pub unsafe fn dsm_backend_shutdown() {
    while !dlist_is_empty(&raw const dsm_segment_list) {
        let seg: *mut dsm_segment =
            crate::lib::ilist::dlist_head_element_off(&raw mut dsm_segment_list, core::mem::offset_of!(dsm_segment, node)) as *mut dsm_segment;
        dsm_detach(seg);
    }
}

/*
 * Detach all shared memory segments, including the control segments.  This
 * should be called, along with PGSharedMemoryDetach, in processes that
 * might inherit mappings but are not intended to be connected to dynamic
 * shared memory.
 */
pub unsafe fn dsm_detach_all() {
    let mut control_address: *mut c_void = dsm_control as *mut c_void;

    while !dlist_is_empty(&raw const dsm_segment_list) {
        let seg: *mut dsm_segment =
            crate::lib::ilist::dlist_head_element_off(&raw mut dsm_segment_list, core::mem::offset_of!(dsm_segment, node)) as *mut dsm_segment;
        dsm_detach(seg);
    }

    if !control_address.is_null() {
        dsm_impl_op(
            DSM_OP_DETACH,
            dsm_control_handle,
            0,
            &raw mut dsm_control_impl_private,
            &mut control_address,
            &raw mut dsm_control_mapped_size,
            ERROR,
        );
    }
}

/*
 * Detach from a shared memory segment, destroying the segment if we
 * remove the last reference.
 *
 * This function should never fail.  It will often be invoked when aborting
 * a transaction, and a further error won't serve any purpose.  It's not a
 * complete disaster if we fail to unmap or destroy the segment; it means a
 * resource leak, but that doesn't necessarily preclude further operations.
 */
pub unsafe fn dsm_detach(seg: *mut dsm_segment) {
    /*
     * Invoke registered callbacks.  Just in case one of those callbacks
     * throws a further error that brings us back here, pop the callback
     * before invoking it, to avoid infinite error recursion.  Don't allow
     * interrupts while running the individual callbacks in non-error code
     * paths, to avoid leaving cleanup work unfinished if we're interrupted by
     * a statement timeout or similar.
     */
    HOLD_INTERRUPTS();
    while !slist_is_empty(&raw const (*seg).on_detach) {
        let node: *mut slist_node = slist_pop_head_node(&mut (*seg).on_detach);
        let cb: *mut dsm_segment_detach_callback =
            slist_container!(dsm_segment_detach_callback, node, node);
        let function: on_dsm_detach_callback = (*cb).function;
        let arg: Datum = (*cb).arg;
        pfree(cb as *mut c_void);

        function(seg, arg);
    }
    RESUME_INTERRUPTS();

    /*
     * Try to remove the mapping, if one exists.  Normally, there will be, but
     * maybe not, if we failed partway through a create or attach operation.
     * We remove the mapping before decrementing the reference count so that
     * the process that sees a zero reference count can be certain that no
     * remaining mappings exist.  Even if this fails, we pretend that it
     * works, because retrying is likely to fail in the same way.
     */
    if !(*seg).mapped_address.is_null() {
        if !is_main_region_dsm_handle((*seg).handle) {
            dsm_impl_op(
                DSM_OP_DETACH,
                (*seg).handle,
                0,
                &mut (*seg).impl_private,
                &mut (*seg).mapped_address,
                &mut (*seg).mapped_size,
                WARNING,
            );
        }
        (*seg).impl_private = std::ptr::null_mut();
        (*seg).mapped_address = std::ptr::null_mut();
        (*seg).mapped_size = 0;
    }

    /* Reduce reference count, if we previously increased it. */
    if (*seg).control_slot != INVALID_CONTROL_SLOT {
        let refcnt: uint32;
        let control_slot: uint32 = (*seg).control_slot;

        LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
        Assert!(control_item(dsm_control, control_slot).handle == (*seg).handle);
        Assert!(control_item(dsm_control, control_slot).refcnt > 1);
        control_item_mut(dsm_control, control_slot).refcnt -= 1;
        refcnt = control_item(dsm_control, control_slot).refcnt;
        (*seg).control_slot = INVALID_CONTROL_SLOT;
        LWLockRelease(DynamicSharedMemoryControlLock);

        /* If new reference count is 1, try to destroy the segment. */
        if refcnt == 1 {
            /* A pinned segment should never reach 1. */
            Assert!(!control_item(dsm_control, control_slot).pinned);

            /*
             * If we fail to destroy the segment here, or are killed before we
             * finish doing so, the reference count will remain at 1, which
             * will mean that nobody else can attach to the segment.  At
             * postmaster shutdown time, or when a new postmaster is started
             * after a hard kill, another attempt will be made to remove the
             * segment.
             */
            if is_main_region_dsm_handle((*seg).handle)
                || dsm_impl_op(
                    DSM_OP_DESTROY,
                    (*seg).handle,
                    0,
                    &mut (*seg).impl_private,
                    &mut (*seg).mapped_address,
                    &mut (*seg).mapped_size,
                    WARNING,
                )
            {
                LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
                if is_main_region_dsm_handle((*seg).handle) {
                    FreePageManagerPut(
                        dsm_main_space_begin as *mut FreePageManager,
                        control_item(dsm_control, control_slot).first_page,
                        control_item(dsm_control, control_slot).npages,
                    );
                }
                Assert!(control_item(dsm_control, control_slot).handle == (*seg).handle);
                Assert!(control_item(dsm_control, control_slot).refcnt == 1);
                control_item_mut(dsm_control, control_slot).refcnt = 0;
                LWLockRelease(DynamicSharedMemoryControlLock);
            }
        }
    }

    /* Clean up our remaining backend-private data structures. */
    if !(*seg).resowner.is_null() {
        ResourceOwnerForgetDSM((*seg).resowner, seg);
    }
    dlist_delete(&mut (*seg).node);
    pfree(seg as *mut c_void);
}

// ----------------------------------------------------------------
// Resource management
// ----------------------------------------------------------------

/*
 * Keep a dynamic shared memory mapping until end of session.
 *
 * By default, mappings are owned by the current resource owner, which
 * typically means they stick around for the duration of the current query
 * only.
 */
pub unsafe fn dsm_pin_mapping(seg: *mut dsm_segment) {
    if !(*seg).resowner.is_null() {
        ResourceOwnerForgetDSM((*seg).resowner, seg);
        (*seg).resowner = std::ptr::null_mut();
    }
}

/*
 * Arrange to remove a dynamic shared memory mapping at cleanup time.
 *
 * dsm_pin_mapping() can be used to preserve a mapping for the entire
 * lifetime of a process; this function reverses that decision, making
 * the segment owned by the current resource owner.  This may be useful
 * just before performing some operation that will invalidate the segment
 * for future use by this backend.
 */
pub unsafe fn dsm_unpin_mapping(seg: *mut dsm_segment) {
    Assert!((*seg).resowner.is_null());
    ResourceOwnerEnlarge(CurrentResourceOwner);
    (*seg).resowner = CurrentResourceOwner;
    ResourceOwnerRememberDSM((*seg).resowner, seg);
}

/*
 * Keep a dynamic shared memory segment until postmaster shutdown, or until
 * dsm_unpin_segment is called.
 *
 * This function should not be called more than once per segment, unless the
 * segment is explicitly unpinned with dsm_unpin_segment in between calls.
 *
 * Note that this function does not arrange for the current process to
 * keep the segment mapped indefinitely; if that behavior is desired,
 * dsm_pin_mapping() should be used from each process that needs to
 * retain the mapping.
 */
pub unsafe fn dsm_pin_segment(seg: *mut dsm_segment) {
    let mut handle: *mut c_void = std::ptr::null_mut();

    /*
     * Bump reference count for this segment in shared memory. This will
     * ensure that even if there is no session which is attached to this
     * segment, it will remain until postmaster shutdown or an explicit call
     * to unpin.
     */
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    if control_item(dsm_control, (*seg).control_slot).pinned {
        elog!(ERROR, "cannot pin a segment that is already pinned");
    }
    if !is_main_region_dsm_handle((*seg).handle) {
        dsm_impl_pin_segment((*seg).handle, (*seg).impl_private, &mut handle);
    }
    control_item_mut(dsm_control, (*seg).control_slot).pinned = true;
    control_item_mut(dsm_control, (*seg).control_slot).refcnt += 1;
    control_item_mut(dsm_control, (*seg).control_slot).impl_private_pm_handle = handle;
    LWLockRelease(DynamicSharedMemoryControlLock);
}

/*
 * Unpin a dynamic shared memory segment that was previously pinned with
 * dsm_pin_segment.  This function should not be called unless dsm_pin_segment
 * was previously called for this segment.
 *
 * The argument is a dsm_handle rather than a dsm_segment in case you want
 * to unpin a segment to which you haven't attached.  This turns out to be
 * useful if, for example, a reference to one shared memory segment is stored
 * within another shared memory segment.  You might want to unpin the
 * referenced segment before destroying the referencing segment.
 */
pub unsafe fn dsm_unpin_segment(handle: dsm_handle) {
    let mut control_slot: uint32 = INVALID_CONTROL_SLOT;
    let mut destroy: bool = false;

    /* Find the control slot for the given handle. */
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    let mut i: uint32 = 0;
    while i < (*dsm_control).nitems {
        /* Skip unused slots and segments that are concurrently going away. */
        if control_item(dsm_control, i).refcnt <= 1 {
            i += 1;
            continue;
        }

        /* If we've found our handle, we can stop searching. */
        if control_item(dsm_control, i).handle == handle {
            control_slot = i;
            break;
        }
        i += 1;
    }

    /*
     * We should definitely have found the slot, and it should not already be
     * in the process of going away, because this function should only be
     * called on a segment which is pinned.
     */
    if control_slot == INVALID_CONTROL_SLOT {
        elog!(ERROR, "cannot unpin unknown segment handle");
    }
    if !control_item(dsm_control, control_slot).pinned {
        elog!(ERROR, "cannot unpin a segment that is not pinned");
    }
    Assert!(control_item(dsm_control, control_slot).refcnt > 1);

    /*
     * Allow implementation-specific code to run.  We have to do this before
     * releasing the lock, because impl_private_pm_handle may get modified by
     * dsm_impl_unpin_segment.
     */
    if !is_main_region_dsm_handle(handle) {
        dsm_impl_unpin_segment(
            handle,
            &mut control_item_mut(dsm_control, control_slot).impl_private_pm_handle,
        );
    }

    /* Note that 1 means no references (0 means unused slot). */
    control_item_mut(dsm_control, control_slot).refcnt -= 1;
    if control_item(dsm_control, control_slot).refcnt == 1 {
        destroy = true;
    }
    control_item_mut(dsm_control, control_slot).pinned = false;

    /* Now we can release the lock. */
    LWLockRelease(DynamicSharedMemoryControlLock);

    /* Clean up resources if that was the last reference. */
    if destroy {
        let mut junk_impl_private: *mut c_void = std::ptr::null_mut();
        let mut junk_mapped_address: *mut c_void = std::ptr::null_mut();
        let mut junk_mapped_size: Size = 0;

        /*
         * For an explanation of how error handling works in this case, see
         * comments in dsm_detach.  Note that if we reach this point, the
         * current process certainly does not have the segment mapped, because
         * if it did, the reference count would have still been greater than 1
         * even after releasing the reference count held by the pin.  The fact
         * that there can't be a dsm_segment for this handle makes it OK to
         * pass the mapped size, mapped address, and private data as NULL
         * here.
         */
        if is_main_region_dsm_handle(handle)
            || dsm_impl_op(
                DSM_OP_DESTROY,
                handle,
                0,
                &mut junk_impl_private,
                &mut junk_mapped_address,
                &mut junk_mapped_size,
                WARNING,
            )
        {
            LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
            if is_main_region_dsm_handle(handle) {
                FreePageManagerPut(
                    dsm_main_space_begin as *mut FreePageManager,
                    control_item(dsm_control, control_slot).first_page,
                    control_item(dsm_control, control_slot).npages,
                );
            }
            Assert!(control_item(dsm_control, control_slot).handle == handle);
            Assert!(control_item(dsm_control, control_slot).refcnt == 1);
            control_item_mut(dsm_control, control_slot).refcnt = 0;
            LWLockRelease(DynamicSharedMemoryControlLock);
        }
    }
}

/*
 * Find an existing mapping for a shared memory segment, if there is one.
 */
pub unsafe fn dsm_find_mapping(handle: dsm_handle) -> *mut dsm_segment {
    let mut iter: dlist_iter = dlist_iter {
        cur: std::ptr::null_mut(),
        end: std::ptr::null_mut(),
    };

    dlist_foreach!(iter, &raw mut dsm_segment_list, {
        let seg: *mut dsm_segment = dlist_container!(dsm_segment, node, iter.cur);
        if (*seg).handle == handle {
            return seg;
        }
    });

    std::ptr::null_mut()
}

// ----------------------------------------------------------------
// Informational accessors
// ----------------------------------------------------------------

/*
 * Get the address at which a dynamic shared memory segment is mapped.
 */
pub unsafe fn dsm_segment_address(seg: *mut dsm_segment) -> *mut c_void {
    Assert!(!(*seg).mapped_address.is_null());
    (*seg).mapped_address
}

/*
 * Get the size of a mapping.
 */
pub unsafe fn dsm_segment_map_length(seg: *mut dsm_segment) -> Size {
    Assert!(!(*seg).mapped_address.is_null());
    (*seg).mapped_size
}

/*
 * Get a handle for a mapping.
 *
 * To establish communication via dynamic shared memory between two backends,
 * one of them should first call dsm_create() to establish a new shared
 * memory mapping.  That process should then call dsm_segment_handle() to
 * obtain a handle for the mapping, and pass that handle to the
 * coordinating backend via some means (e.g. bgw_main_arg, or via the
 * main shared memory segment).  The recipient, once in possession of the
 * handle, should call dsm_attach().
 */
pub unsafe fn dsm_segment_handle(seg: *mut dsm_segment) -> dsm_handle {
    (*seg).handle
}

// ----------------------------------------------------------------
// Cleanup hooks
// ----------------------------------------------------------------

/*
 * Register an on-detach callback for a dynamic shared memory segment.
 */
pub unsafe fn on_dsm_detach(
    seg: *mut dsm_segment,
    function: on_dsm_detach_callback,
    arg: Datum,
) {
    let cb: *mut dsm_segment_detach_callback = MemoryContextAlloc(
        TopMemoryContext,
        std::mem::size_of::<dsm_segment_detach_callback>(),
    ) as *mut dsm_segment_detach_callback;
    (*cb).function = function;
    (*cb).arg = arg;
    slist_push_head(&mut (*seg).on_detach, &mut (*cb).node);
}

/*
 * Unregister an on-detach callback for a dynamic shared memory segment.
 */
pub unsafe fn cancel_on_dsm_detach(
    seg: *mut dsm_segment,
    function: on_dsm_detach_callback,
    arg: Datum,
) {
    let mut iter: slist_mutable_iter = slist_mutable_iter {
        cur: std::ptr::null_mut(),
        next: std::ptr::null_mut(),
        prev: std::ptr::null_mut(),
    };

    slist_foreach_modify!(iter, &mut (*seg).on_detach, {
        let cb: *mut dsm_segment_detach_callback =
            slist_container!(dsm_segment_detach_callback, node, iter.cur);
        // Suppress "can't compare fn pointer" lint: cast to usize for equality.
        if ((*cb).function as usize) == (function as usize) && (*cb).arg == arg {
            slist_delete_current(&mut iter);
            pfree(cb as *mut c_void);
            break;
        }
    });
}

/*
 * Discard all registered on-detach callbacks without executing them.
 */
pub unsafe fn reset_on_dsm_detach() {
    let mut iter: dlist_iter = dlist_iter {
        cur: std::ptr::null_mut(),
        end: std::ptr::null_mut(),
    };

    dlist_foreach!(iter, &raw mut dsm_segment_list, {
        let seg: *mut dsm_segment = dlist_container!(dsm_segment, node, iter.cur);

        /* Throw away explicit on-detach actions one by one. */
        while !slist_is_empty(&raw const (*seg).on_detach) {
            let node: *mut slist_node = slist_pop_head_node(&mut (*seg).on_detach);
            let cb: *mut dsm_segment_detach_callback =
                slist_container!(dsm_segment_detach_callback, node, node);
            pfree(cb as *mut c_void);
        }

        /*
         * Decrementing the reference count is a sort of implicit on-detach
         * action; make sure we don't do that, either.
         */
        (*seg).control_slot = INVALID_CONTROL_SLOT;
    });
}

// ----------------------------------------------------------------
// Internal helpers
// ----------------------------------------------------------------

/*
 * Create a segment descriptor.
 */
unsafe fn dsm_create_descriptor() -> *mut dsm_segment {
    let seg: *mut dsm_segment;

    if !CurrentResourceOwner.is_null() {
        ResourceOwnerEnlarge(CurrentResourceOwner);
    }

    seg = MemoryContextAlloc(TopMemoryContext, std::mem::size_of::<dsm_segment>())
        as *mut dsm_segment;
    dlist_push_head(&raw mut dsm_segment_list, &mut (*seg).node);

    /* seg->handle must be initialized by the caller */
    (*seg).control_slot = INVALID_CONTROL_SLOT;
    (*seg).impl_private = std::ptr::null_mut();
    (*seg).mapped_address = std::ptr::null_mut();
    (*seg).mapped_size = 0;

    (*seg).resowner = CurrentResourceOwner;
    if !CurrentResourceOwner.is_null() {
        ResourceOwnerRememberDSM(CurrentResourceOwner, seg);
    }

    slist_init(&mut (*seg).on_detach);

    seg
}

/*
 * Sanity check a control segment.
 *
 * The goal here isn't to detect everything that could possibly be wrong with
 * the control segment; there's not enough information for that.  Rather, the
 * goal is to make sure that someone can iterate over the items in the segment
 * without overrunning the end of the mapping and crashing.  We also check
 * the magic number since, if that's messed up, this may not even be one of
 * our segments at all.
 */
unsafe fn dsm_control_segment_sane(
    control: *mut dsm_control_header,
    mapped_size: Size,
) -> bool {
    if mapped_size < core::mem::offset_of!(dsm_control_header, item) {
        return false; /* Mapped size too short to read header. */
    }
    if (*control).magic != PG_DYNSHMEM_CONTROL_MAGIC {
        return false; /* Magic number doesn't match. */
    }
    if dsm_control_bytes_needed((*control).maxitems) > mapped_size as u64 {
        return false; /* Max item count won't fit in map. */
    }
    if (*control).nitems > (*control).maxitems {
        return false; /* Overfull. */
    }
    true
}

/*
 * Compute the number of control-segment bytes needed to store a given
 * number of items.
 */
#[inline]
fn dsm_control_bytes_needed(nitems: uint32) -> u64 {
    (core::mem::offset_of!(dsm_control_header, item) as u64)
        + (std::mem::size_of::<dsm_control_item>() as u64) * (nitems as u64)
}

/*
 * Build a DSM handle for a main-region slot that is odd (so it cannot
 * collide with handles produced by dsm_impl_op(), which always uses even
 * numbers) and encodes the slot number.
 */
#[inline]
unsafe fn make_main_region_dsm_handle(slot: c_int) -> dsm_handle {
    /*
     * We need to create a handle that doesn't collide with any existing extra
     * segment created by dsm_impl_op(), so we'll make it odd.  It also
     * mustn't collide with any other main area pseudo-segment, so we'll
     * include the slot number in some of the bits.  We also want to make an
     * effort to avoid newly created and recently destroyed handles from being
     * confused, so we'll make the rest of the bits random.
     */
    let mut handle: dsm_handle = 1;
    handle |= (slot << 1) as dsm_handle;
    handle |= pg_prng_uint32(&raw mut pg_global_prng_state)
        << (pg_leftmost_one_pos32((*dsm_control).maxitems) + 1);
    handle
}

#[inline]
fn is_main_region_dsm_handle(handle: dsm_handle) -> bool {
    (handle & 1) != 0
}

/// Helper: shared reference to the i-th control item.
#[inline]
unsafe fn control_item(hdr: *const dsm_control_header, i: uint32) -> &'static dsm_control_item {
    let base = hdr as *const c_char;
    let off = core::mem::offset_of!(dsm_control_header, item)
        + i as usize * std::mem::size_of::<dsm_control_item>();
    &*(base.add(off) as *const dsm_control_item)
}

/// Helper: mutable reference to the i-th control item.
#[inline]
unsafe fn control_item_mut(
    hdr: *mut dsm_control_header,
    i: uint32,
) -> &'static mut dsm_control_item {
    let base = hdr as *mut c_char;
    let off = core::mem::offset_of!(dsm_control_header, item)
        + i as usize * std::mem::size_of::<dsm_control_item>();
    &mut *(base.add(off) as *mut dsm_control_item)
}

// ----------------------------------------------------------------
// Local stubs for unported symbols
// ----------------------------------------------------------------

// TODO(pg-port): real LWLockAcquire lives in storage/lmgr/lwlock.c
type LWLock = c_void;
const LW_EXCLUSIVE: c_int = 0;
const LW_SHARED: c_int = 1;
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): storage/lmgr/lwlock.c
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lwlock.c
}

// TODO(pg-port): real DynamicSharedMemoryControlLock lives in storage/lmgr/lwlocklist.h
static mut DynamicSharedMemoryControlLock: *mut LWLock = std::ptr::null_mut();

// TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: Size,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): storage/ipc/shmem.c
}

// TODO(pg-port): real FreePageManager / FPM_PAGE_SIZE live in utils/freepage.c
#[repr(C)]
struct FreePageManager {
    _private: [u8; 0],
}
const FPM_PAGE_SIZE: usize = 4096; // placeholder; real value from utils/freepage.h
unsafe fn FreePageManagerInitialize(_fpm: *mut FreePageManager, _base: *mut c_void) {
    unimplemented!() // TODO(pg-port): utils/freepage.c
}
unsafe fn FreePageManagerGet(
    _fpm: *mut FreePageManager,
    _npages: usize,
    _first_page: *mut usize,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/freepage.c
}
unsafe fn FreePageManagerPut(_fpm: *mut FreePageManager, _first_page: usize, _npages: usize) {
    unimplemented!() // TODO(pg-port): utils/freepage.c
}

// TODO(pg-port): real on_shmem_exit lives in storage/ipc/ipc.c
// (pg_on_exit_callback = unsafe fn(c_int, Datum))
unsafe fn on_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) {
    unimplemented!() // TODO(pg-port): storage/ipc/ipc.c
}

// TODO(pg-port): real psprintf lives in common/psprintf.c; render the one
// format this file needs via Rust formatting into palloc'd storage.
unsafe fn psprintf_segment(handle: dsm_handle) -> *mut c_char {
    let sformatted = format!("dynamic shared memory segment {}\0", handle);
    let out = palloc(sformatted.len()) as *mut c_char;
    core::ptr::copy_nonoverlapping(sformatted.as_ptr() as *const c_char, out, sformatted.len());
    out
}

// TODO(pg-port): real AllocateDir/ReadDir/FreeDir live in storage/file/fd.c
#[repr(C)]
struct DIR {
    _private: [u8; 0],
}
#[repr(C)]
struct dirent {
    d_name: [c_char; 256],
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!() // TODO(pg-port): storage/file/fd.c
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!() // TODO(pg-port): storage/file/fd.c
}
unsafe fn FreeDir(_dir: *mut DIR) {
    unimplemented!() // TODO(pg-port): storage/file/fd.c
}

// TODO(pg-port): real strncmp / snprintf live in libc / port
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
}

// TODO(pg-port): IsUnderPostmaster / IsPostmasterEnvironment / MaxBackends
// live in miscadmin.c -- exposed as extern statics in crate::miscadmin.
extern "C" {
    static mut IsUnderPostmaster: bool;        // TODO(pg-port): miscadmin.c
    static mut IsPostmasterEnvironment: bool;  // TODO(pg-port): miscadmin.c
    static mut MaxBackends: c_int;             // TODO(pg-port): miscadmin.c
    static mut min_dynamic_shared_memory: c_int; // TODO(pg-port): dsm_impl.c (GUC)
}

// TODO(pg-port): DEBUG1 comes from prelude but is re-stated here for clarity.
// FATAL / WARNING / ERROR / LOG / DEBUG2 come from crate::prelude via elog.h.

/// Tiny wrapper to format a C string pointer for elog/ereport messages.
/// Equivalent to the implicit `%s` in C's printf family.
struct CStr(*const c_char);
impl core::fmt::Display for CStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // SAFETY: pointer is a valid NUL-terminated C string from Postgres internals.
        let s = unsafe { std::ffi::CStr::from_ptr(self.0) };
        write!(f, "{}", s.to_string_lossy())
    }
}

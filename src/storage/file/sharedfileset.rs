//-------------------------------------------------------------------------
//
// sharedfileset.rs
//	  Shared temporary file management.
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
// IDENTIFICATION
//	  src/backend/storage/file/sharedfileset.c
//	  src/include/storage/sharedfileset.h
//
// SharedFileSets provide a temporary namespace (think directory) so that
// files can be discovered by name, and a shared ownership semantics so that
// shared files survive until the last user detaches.
//
//-------------------------------------------------------------------------

use crate::prelude::*;

use std::ffi::c_void;

// dsm_segment is an opaque handle (storage/dsm.h); kept local as elsewhere.
pub type dsm_segment = c_void;
use crate::storage::file::fileset::{FileSet, FileSetDeleteAll, FileSetInit};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};

// ---------------------------------------------------------------------------
// sharedfileset.h
// ---------------------------------------------------------------------------

/*
 * A set of temporary files that can be shared by multiple backends.
 */
#[repr(C)]
pub struct SharedFileSet {
    pub fs: FileSet,
    pub mutex: slock_t,      /* mutex protecting the reference count */
    pub refcnt: c_int,       /* number of attached backends */
}

// ---------------------------------------------------------------------------
// sharedfileset.c
// ---------------------------------------------------------------------------

/*
 * Initialize a space for temporary files that can be opened by other backends.
 * Other backends must attach to it before accessing it.  Associate this
 * SharedFileSet with 'seg'.  Any contained files will be deleted when the
 * last backend detaches.
 *
 * Under the covers the set is one or more directories which will eventually
 * be deleted.
 */
pub unsafe fn SharedFileSetInit(fileset: *mut SharedFileSet, seg: *mut dsm_segment) {
    /* Initialize the shared fileset specific members. */
    SpinLockInit(&mut (*fileset).mutex);
    (*fileset).refcnt = 1;

    /* Initialize the fileset. */
    FileSetInit(&mut (*fileset).fs as *mut FileSet as *mut _);

    /* Register our cleanup callback. */
    if !seg.is_null() {
        on_dsm_detach(
            seg,
            SharedFileSetOnDetach,
            PointerGetDatum(fileset as *const c_void),
        );
    }
}

/*
 * Attach to a set of directories that was created with SharedFileSetInit.
 */
pub unsafe fn SharedFileSetAttach(fileset: *mut SharedFileSet, seg: *mut dsm_segment) {
    let success: bool;

    SpinLockAcquire(&mut (*fileset).mutex);
    if (*fileset).refcnt == 0 {
        success = false;
    } else {
        (*fileset).refcnt += 1;
        success = true;
    }
    SpinLockRelease(&mut (*fileset).mutex);

    if !success {
        ereport!(
            ERROR,
            "could not attach to a SharedFileSet that is already destroyed"
        );
    }

    /* Register our cleanup callback. */
    on_dsm_detach(
        seg,
        SharedFileSetOnDetach,
        PointerGetDatum(fileset as *const c_void),
    );
}

/*
 * Delete all files in the set.
 */
pub unsafe fn SharedFileSetDeleteAll(fileset: *mut SharedFileSet) {
    FileSetDeleteAll(&mut (*fileset).fs as *mut FileSet as *mut _);
}

/*
 * Callback function that will be invoked when this backend detaches from a
 * DSM segment holding a SharedFileSet that it has created or attached to.  If
 * we are the last to detach, then try to remove the directories and
 * everything in them.  We can't raise an error on failures, because this runs
 * in error cleanup paths.
 */
unsafe extern "C" fn SharedFileSetOnDetach(_segment: *mut dsm_segment, datum: Datum) {
    let mut unlink_all: bool = false;
    let fileset = DatumGetPointer(datum) as *mut SharedFileSet;

    SpinLockAcquire(&mut (*fileset).mutex);
    Assert!((*fileset).refcnt > 0);
    (*fileset).refcnt -= 1;
    if (*fileset).refcnt == 0 {
        unlink_all = true;
    }
    SpinLockRelease(&mut (*fileset).mutex);

    /*
     * If we are the last to detach, we delete the directory in all
     * tablespaces.  Note that we are still actually attached for the rest of
     * this function so we can safely access its data.
     */
    if unlink_all {
        FileSetDeleteAll(&mut (*fileset).fs as *mut FileSet as *mut _);
    }
}

// on_dsm_detach is declared in storage/dsm.h; provide a stub matching the
// signature used here (callback receives the dsm_segment and a Datum).
type on_dsm_detach_callback = unsafe extern "C" fn(*mut dsm_segment, Datum);

extern "C" {
    fn on_dsm_detach(seg: *mut dsm_segment, function: on_dsm_detach_callback, arg: Datum);
}

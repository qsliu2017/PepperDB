//! Translated from PostgreSQL src/include/storage/sharedfileset.h
//!
//! Shared temporary file management. Under single-process async the shmem
//! refcount machinery collapses: the spinlock-protected `refcnt` becomes a
//! `std::sync::Mutex<i32>`, and the `dsm_segment` parameter is dropped (dsm.h
//! is tombstoned -> Arc-shared heap state).

use std::sync::Mutex;

use crate::storage::fileset::FileSet;

/// A set of temporary files that can be shared by multiple backends.
pub struct SharedFileSet {
    pub fs: FileSet,
    /// refcount, was `slock_t mutex` + `int refcnt` (single-process: a Mutex).
    pub refcnt: Mutex<i32>,
}

// dsm_segment args dropped: shmem is replaced by Arc-shared heap state.
pub fn SharedFileSetInit(_fileset: &mut SharedFileSet) {
    unimplemented!()
}
pub fn SharedFileSetAttach(_fileset: &mut SharedFileSet) {
    unimplemented!()
}
pub fn SharedFileSetDeleteAll(_fileset: &mut SharedFileSet) {
    unimplemented!()
}

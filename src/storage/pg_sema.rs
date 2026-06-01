//! storage/pg_sema.h - Platform-independent API for semaphores.
//!
//! PostgreSQL requires counting semaphores (the kind that keep track of
//! multiple unlock operations, and will allow an equal number of subsequent
//! lock operations before blocking). The underlying implementation is not the
//! same on every platform; this file defines the API each port must provide.

use std::ffi::c_int;

use crate::c::Size;

/// struct PGSemaphoreData and pointer type PGSemaphore are the data structure
/// representing an individual semaphore. The contents of PGSemaphoreData vary
/// across implementations and must never be touched by platform-independent
/// code; hence, PGSemaphoreData is declared as an opaque struct here.
///
/// On Windows (USE_WIN32_SEMAPHORES) PGSemaphore is just defined as HANDLE; we
/// model the non-Windows case (the opaque struct pointer).
#[repr(C)]
pub struct PGSemaphoreData {
    _opaque: [u8; 0],
}

pub type PGSemaphore = *mut PGSemaphoreData;

/// Report amount of shared memory needed
pub unsafe fn PGSemaphoreShmemSize(maxSemas: c_int) -> Size {
    let _ = maxSemas;
    unimplemented!()
}

/// Module initialization (called during postmaster start or shmem reinit)
pub unsafe fn PGReserveSemaphores(maxSemas: c_int) {
    let _ = maxSemas;
    unimplemented!()
}

/// Allocate a PGSemaphore structure with initial count 1
pub unsafe fn PGSemaphoreCreate() -> PGSemaphore {
    unimplemented!()
}

/// Reset a previously-initialized PGSemaphore to have count 0
pub unsafe fn PGSemaphoreReset(sema: PGSemaphore) {
    let _ = sema;
    unimplemented!()
}

/// Lock a semaphore (decrement count), blocking if count would be < 0
pub unsafe fn PGSemaphoreLock(sema: PGSemaphore) {
    let _ = sema;
    unimplemented!()
}

/// Unlock a semaphore (increment count)
pub unsafe fn PGSemaphoreUnlock(sema: PGSemaphore) {
    let _ = sema;
    unimplemented!()
}

/// Lock a semaphore only if able to do so without blocking
pub unsafe fn PGSemaphoreTryLock(sema: PGSemaphore) -> bool {
    let _ = sema;
    unimplemented!()
}

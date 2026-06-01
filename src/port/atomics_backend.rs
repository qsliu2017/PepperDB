//! src/backend/port/atomics.c
//!
//! atomics.c
//!    Non-Inline parts of the atomics implementation
//!
//! Portions Copyright (c) 2013-2025, PostgreSQL Global Development Group
//!
//! Companion header: src/include/port/atomics.h (decls/inline wrappers merged below)

use crate::prelude::*;
use std::ffi::c_int;

// from miscadmin.h, port/atomics.h, storage/spin.h
// The concrete atomic types and slock_t come from the platform-specific
// atomics headers; here they are represented by the stub types below.

/// pg_atomic_uint32 - see port/atomics/generic.h
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32, // volatile
}

/// pg_atomic_uint64 - see port/atomics/fallback.h
///
/// In the spinlock-based simulation (PG_HAVE_ATOMIC_U64_SIMULATION) this
/// carries an embedded spinlock 'sema' alongside the value.
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub sema: slock_t,
    pub value: u64, // volatile
}

/// pg_atomic_flag - see port/atomics/generic.h
#[repr(C)]
pub struct pg_atomic_flag {
    pub value: u32, // volatile
}

// slock_t from storage/s_lock.h
type slock_t = c_int;

unsafe fn SpinLockInit(_lock: *mut slock_t) {
    unimplemented!() // TODO: storage/spin.h
}

unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    unimplemented!() // TODO: storage/spin.h
}

unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    unimplemented!() // TODO: storage/spin.h
}

/*
 * #ifdef PG_HAVE_ATOMIC_U64_SIMULATION
 *
 * The following non-inline functions are only compiled when the platform
 * relies on the spinlock-based simulation of 64-bit atomics.
 */

pub unsafe fn pg_atomic_init_u64_impl(ptr: *mut pg_atomic_uint64, val_: u64) {
    // StaticAssertDecl(sizeof(ptr->sema) >= sizeof(slock_t),
    //                  "size mismatch of atomic_uint64 vs slock_t");
    const _: () = assert!(
        core::mem::size_of::<slock_t>() >= core::mem::size_of::<slock_t>(),
        "size mismatch of atomic_uint64 vs slock_t"
    );

    SpinLockInit(&mut (*ptr).sema as *mut slock_t);
    (*ptr).value = val_;
}

pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    expected: *mut u64,
    newval: u64,
) -> bool {
    let ret: bool;

    /*
     * Do atomic op under a spinlock. It might look like we could just skip
     * the cmpxchg if the lock isn't available, but that'd just emulate a
     * 'weak' compare and swap. I.e. one that allows spurious failures. Since
     * several algorithms rely on a strong variant and that is efficiently
     * implementable on most major architectures let's emulate it here as
     * well.
     */
    SpinLockAcquire(&mut (*ptr).sema as *mut slock_t);

    /* perform compare/exchange logic */
    ret = (*ptr).value == *expected;
    *expected = (*ptr).value;
    if ret {
        (*ptr).value = newval;
    }

    /* and release lock */
    SpinLockRelease(&mut (*ptr).sema as *mut slock_t);

    return ret;
}

pub unsafe fn pg_atomic_fetch_add_u64_impl(ptr: *mut pg_atomic_uint64, add_: i64) -> u64 {
    let oldval: u64;

    SpinLockAcquire(&mut (*ptr).sema as *mut slock_t);
    oldval = (*ptr).value;
    (*ptr).value = (*ptr).value.wrapping_add(add_ as u64);
    SpinLockRelease(&mut (*ptr).sema as *mut slock_t);
    return oldval;
}

/* #endif PG_HAVE_ATOMIC_U64_SIMULATION */

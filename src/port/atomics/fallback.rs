//! port/atomics/fallback.h - Fallback for platforms without 64-bit atomics support.
//!
//! Spinlock-simulated u64 atomics. Slower than native atomics, but not unusably
//! slow. This header is only included via atomics.h when no native 64-bit atomic
//! support is detected (`!defined(PG_HAVE_ATOMIC_U64_SUPPORT)`); it defines the
//! `PG_HAVE_ATOMIC_U64_SIMULATION` path: a `pg_atomic_uint64` carrying a spinlock
//! (`sema`) plus prototypes whose bodies live in atomics.c.
//!
//! NOTE: the real Rust atomics for PepperDB live in `crate::port::atomics` (the
//! mod.rs), which maps the NATIVE path onto `core::sync::atomic`. The native
//! `pg_atomic_uint64` defined there has layout `{ value }` (no `sema`). This file
//! is a faithful 1:1 translation of the SIMULATION variant header; the function
//! bodies (spinlock-guarded) live in atomics.c and depend on storage/spin.h
//! (S_LOCK / SpinLock*), which is not ported - so the prototypes are stubs.

#![allow(non_camel_case_types)]

use crate::prelude::*;
use std::ffi::c_int;

// The C header is `#if !defined(PG_HAVE_ATOMIC_U64_SUPPORT)`; inside it both of
// these markers become defined. Translated as marker consts.

/// `#define PG_HAVE_ATOMIC_U64_SIMULATION`
pub const PG_HAVE_ATOMIC_U64_SIMULATION: bool = true;

/// `#define PG_HAVE_ATOMIC_U64_SUPPORT`
pub const PG_HAVE_ATOMIC_U64_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_INIT_U64`
pub const PG_HAVE_ATOMIC_INIT_U64: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: bool = true;

/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U64`
pub const PG_HAVE_ATOMIC_FETCH_ADD_U64: bool = true;

/// Simulation-path layout of `pg_atomic_uint64`:
/// ```c
/// typedef struct pg_atomic_uint64
/// {
///     int          sema;
///     volatile uint64 value;
/// } pg_atomic_uint64;
/// ```
/// `sema` is a spinlock (slock_t, here `int`) guarding `value`. This differs from
/// the native layout (`{ value }`) used by `crate::port::atomics::pg_atomic_uint64`.
/// Named distinctly to avoid colliding with the canonical native type.
// TODO: dedup - this is the simulation variant of pg_atomic_uint64; the native
// one lives in crate::port::atomics.
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub sema: c_int,
    pub value: uint64,
}

// ---------------------------------------------------------------------------
// Prototypes (bodies in atomics.c). These are the spinlock-simulated u64 ops.
// Their real implementation depends on storage/spin.h (SpinLockInit /
// SpinLockAcquire / SpinLockRelease -> S_LOCK / TAS), which is not ported, so
// the bodies are stubbed. See crate::port::atomics for the *_sim counterparts.
// ---------------------------------------------------------------------------

/// `extern void pg_atomic_init_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 val_);`
pub unsafe fn pg_atomic_init_u64_impl(ptr: *mut pg_atomic_uint64, val_: uint64) {
    let _ = (ptr, val_);
    unimplemented!()
}

/// ```c
/// extern bool pg_atomic_compare_exchange_u64_impl(volatile pg_atomic_uint64 *ptr,
///                                                 uint64 *expected, uint64 newval);
/// ```
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    expected: *mut uint64,
    newval: uint64,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// `extern uint64 pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_);`
pub unsafe fn pg_atomic_fetch_add_u64_impl(ptr: *mut pg_atomic_uint64, add_: int64) -> uint64 {
    let _ = (ptr, add_);
    unimplemented!()
}

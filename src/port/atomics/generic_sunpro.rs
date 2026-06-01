//! port/atomics/generic-sunpro.h - Atomic operations for Solaris' CC (SunPro compiler).
//!
//! This is a per-compiler atomics variant header. The real bodies use SunPro
//! compiler intrinsics (`__compiler_barrier`, `__machine_rw_barrier`, etc. from
//! <mbarrier.h>) and the Solaris libc `atomic.h` routines (`atomic_cas_32`,
//! `atomic_swap_64`, ...). None of these have a Rust form: the actual Rust
//! atomics live in `port/atomics/mod.rs` via `core::sync::atomic`. Here we
//! translate the struct/typedef/#define support markers and render each
//! intrinsic-backed op as an `unimplemented!()` prototype stub.
//!
//! The C header guards every definition behind `HAVE_MBARRIER_H` / `HAVE_ATOMIC_H`;
//! we translate the full set unconditionally (the marker consts stand in for the
//! `#define PG_HAVE_*` feature markers).

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use crate::c::{uint32, uint64};
use crate::port::atomics::{pg_atomic_uint32, pg_atomic_uint64};

// --- HAVE_MBARRIER_H: barrier impls via SunPro <mbarrier.h> intrinsics ---

/// `#define PG_HAVE_COMPILER_BARRIER` is not emitted by the C header, but the
/// compiler barrier impl is. Body: `__compiler_barrier()`.
/// No Rust form; real barrier lives in port/atomics/mod.rs.
#[inline]
pub unsafe fn pg_compiler_barrier_impl() {
    unimplemented!()
}

/// `#define pg_memory_barrier_impl()` -> `__machine_rw_barrier()`.
/// Despite the name this is a full barrier (mfence / membar on x86 / sparc).
#[inline]
pub unsafe fn pg_memory_barrier_impl() {
    unimplemented!()
}

/// `#define pg_read_barrier_impl()` -> `__machine_r_barrier()`.
#[inline]
pub unsafe fn pg_read_barrier_impl() {
    unimplemented!()
}

/// `#define pg_write_barrier_impl()` -> `__machine_w_barrier()`.
#[inline]
pub unsafe fn pg_write_barrier_impl() {
    unimplemented!()
}

// --- HAVE_ATOMIC_H: support markers ---
//
// The atomic struct layouts (pg_atomic_uint32 / pg_atomic_uint64) defined in this
// C header are identical to the ones in port/atomics/mod.rs, so we reuse those
// rather than redefining. The C `typedef struct { volatile uint32 value; }` and
// the 8-byte-aligned u64 variant are captured by the reused types.

/// `#define PG_HAVE_ATOMIC_U32_SUPPORT`
pub const PG_HAVE_ATOMIC_U32_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_U64_SUPPORT`
pub const PG_HAVE_ATOMIC_U64_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32: bool = true;

/// `#define PG_HAVE_ATOMIC_EXCHANGE_U32`
pub const PG_HAVE_ATOMIC_EXCHANGE_U32: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: bool = true;

/// `#define PG_HAVE_ATOMIC_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_EXCHANGE_U64: bool = true;

// --- HAVE_ATOMIC_H: op impls via Solaris libc <atomic.h> ---
//
// Bodies use atomic_cas_32 / atomic_swap_32 / atomic_cas_64 / atomic_swap_64;
// no Rust form, real impls live in port/atomics/mod.rs.

/// `static inline bool pg_atomic_compare_exchange_u32_impl(...)`.
/// Body: `atomic_cas_32(&ptr->value, *expected, newval)` + compare/store-back.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u32_impl(
    _ptr: *mut pg_atomic_uint32,
    _expected: *mut uint32,
    _newval: uint32,
) -> bool {
    unimplemented!()
}

/// `static inline uint32 pg_atomic_exchange_u32_impl(...)`.
/// Body: `atomic_swap_32(&ptr->value, newval)`.
#[inline]
pub unsafe fn pg_atomic_exchange_u32_impl(
    _ptr: *mut pg_atomic_uint32,
    _newval: uint32,
) -> uint32 {
    unimplemented!()
}

/// `static inline bool pg_atomic_compare_exchange_u64_impl(...)`.
/// Body: `AssertPointerAlignment(expected, 8)` +
/// `atomic_cas_64(&ptr->value, *expected, newval)` + compare/store-back.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    _ptr: *mut pg_atomic_uint64,
    _expected: *mut uint64,
    _newval: uint64,
) -> bool {
    unimplemented!()
}

/// `static inline uint64 pg_atomic_exchange_u64_impl(...)`.
/// Body: `atomic_swap_64(&ptr->value, newval)`.
#[inline]
pub unsafe fn pg_atomic_exchange_u64_impl(
    _ptr: *mut pg_atomic_uint64,
    _newval: uint64,
) -> uint64 {
    unimplemented!()
}

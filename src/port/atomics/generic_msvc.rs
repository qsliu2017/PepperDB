//! port/atomics/generic-msvc.h - Atomic operations support when using MSVC.
//!
//! This is a per-compiler atomics-variant header (the MSVC path). In C its
//! `*_impl` functions are `static inline` wrappers over MSVC `Interlocked*`
//! intrinsics (`InterlockedCompareExchange`, `_InterlockedExchange64`, ...),
//! `_ReadWriteBarrier`, and the `MemoryBarrier()` macro. None of those
//! intrinsics / inline-asm-equivalent compiler builtins have a Rust form, so
//! every such function body is rendered here as a `unimplemented!()` prototype
//! stub.
//!
//! The REAL, portable Rust atomics live in `crate::port::atomics` (the mod.rs),
//! implemented on top of `core::sync::atomic` (SeqCst). The struct types
//! `pg_atomic_uint32` / `pg_atomic_uint64` are reused from there rather than
//! redefined, so these stubs stay layout-compatible with the rest of the port.
//!
//! Only the marker consts (`PG_HAVE_*`), the barrier macros, and the prototype
//! stubs are emitted; the structs come from the parent module.

use crate::prelude::*;
use crate::port::atomics::{pg_atomic_uint32, pg_atomic_uint64};

// ---------------------------------------------------------------------------
// Barriers.
//
// C:
//   #pragma intrinsic(_ReadWriteBarrier)
//   #define pg_compiler_barrier_impl()  _ReadWriteBarrier()
//   #ifndef pg_memory_barrier_impl
//   #define pg_memory_barrier_impl()    MemoryBarrier()
//   #endif
//
// `_ReadWriteBarrier()` is a compiler reordering barrier and `MemoryBarrier()`
// is a full hardware fence; neither maps to a Rust expression, so the
// function-like macros become stub fns. The portable equivalents are
// core::sync::atomic::compiler_fence / fence in the real atomics module.
// ---------------------------------------------------------------------------

/// generic-msvc.h `pg_compiler_barrier_impl()` -> `_ReadWriteBarrier()`.
/// STUB: MSVC compiler intrinsic; no Rust form. See core::sync::atomic::compiler_fence.
#[inline]
pub unsafe fn pg_compiler_barrier_impl() {
    unimplemented!()
}

/// generic-msvc.h `pg_memory_barrier_impl()` -> `MemoryBarrier()`.
/// STUB: MSVC full-fence intrinsic; no Rust form. See core::sync::atomic::fence.
#[inline]
pub unsafe fn pg_memory_barrier_impl() {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Support markers (#define PG_HAVE_*). These are bare object-like macros that
// in C act as compile-time presence flags. Rendered as unit consts.
// ---------------------------------------------------------------------------

pub const PG_HAVE_ATOMIC_U32_SUPPORT: () = ();
pub const PG_HAVE_ATOMIC_U64_SUPPORT: () = ();
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32: () = ();
pub const PG_HAVE_ATOMIC_EXCHANGE_U32: () = ();
pub const PG_HAVE_ATOMIC_FETCH_ADD_U32: () = ();
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: () = ();

// The following two are guarded by `#ifdef _WIN64` in the C source (the 64-bit
// exchange / fetch-add intrinsics are only available on 64-bit builds).
pub const PG_HAVE_ATOMIC_EXCHANGE_U64: () = ();
pub const PG_HAVE_ATOMIC_FETCH_ADD_U64: () = ();

// ---------------------------------------------------------------------------
// u32 ops. C bodies use MSVC Interlocked* intrinsics over `&ptr->value`.
// All rendered as stubs (see core::sync::atomic in the real atomics module).
// ---------------------------------------------------------------------------

/// generic-msvc.h pg_atomic_compare_exchange_u32_impl.
/// C: `InterlockedCompareExchange(&ptr->value, newval, *expected)`; sets
/// `*expected` to the observed value and returns whether the swap happened.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u32_impl(
    ptr: *mut pg_atomic_uint32,
    expected: *mut uint32,
    newval: uint32,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// generic-msvc.h pg_atomic_exchange_u32_impl.
/// C: `InterlockedExchange(&ptr->value, newval)`.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_exchange_u32_impl(ptr: *mut pg_atomic_uint32, newval: uint32) -> uint32 {
    let _ = (ptr, newval);
    unimplemented!()
}

/// generic-msvc.h pg_atomic_fetch_add_u32_impl.
/// C: `InterlockedExchangeAdd(&ptr->value, add_)`; returns prior value.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u32_impl(ptr: *mut pg_atomic_uint32, add_: int32) -> uint32 {
    let _ = (ptr, add_);
    unimplemented!()
}

// ---------------------------------------------------------------------------
// u64 ops. C bodies use MSVC `_Interlocked*64` intrinsics over `&ptr->value`.
// All rendered as stubs (see core::sync::atomic in the real atomics module).
// ---------------------------------------------------------------------------

/// generic-msvc.h pg_atomic_compare_exchange_u64_impl.
/// C: `_InterlockedCompareExchange64(&ptr->value, newval, *expected)`; sets
/// `*expected` to the observed value and returns whether the swap happened.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    expected: *mut uint64,
    newval: uint64,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// generic-msvc.h pg_atomic_exchange_u64_impl (`#ifdef _WIN64`).
/// C: `_InterlockedExchange64(&ptr->value, newval)`.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_exchange_u64_impl(ptr: *mut pg_atomic_uint64, newval: uint64) -> uint64 {
    let _ = (ptr, newval);
    unimplemented!()
}

/// generic-msvc.h pg_atomic_fetch_add_u64_impl (`#ifdef _WIN64`).
/// C: `_InterlockedExchangeAdd64(&ptr->value, add_)`; returns prior value.
/// STUB: MSVC intrinsic; no Rust form.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u64_impl(ptr: *mut pg_atomic_uint64, add_: int64) -> uint64 {
    let _ = (ptr, add_);
    unimplemented!()
}

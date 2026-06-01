//! port/atomics/arch-ppc.h - Atomic operations considerations specific to PowerPC
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/include/port/atomics/arch-ppc.h.
//!
//! The C header has intentionally no include guards and is only meant to be
//! included by atomics.h. It defines, gated on `__GNUC__` / `SIZEOF_VOID_P`:
//!   - the memory-barrier #defines (sync / lwsync inline asm),
//!   - the `pg_atomic_uint32` / `pg_atomic_uint64` struct typedefs,
//!   - PG_HAVE_* feature-marker #defines,
//!   - inline-asm implementations of compare-exchange / fetch-add for u32/u64.
//!
//! The inline-asm function bodies (lwarx/stwcx./ldarx/stdcx./sync/lwsync...)
//! have no direct Rust form. Per the port convention they are rendered as
//! `pub unsafe fn ... { unimplemented!() }` prototype stubs; the real Rust
//! atomics live in port/atomics/mod.rs via core::sync::atomic. The barrier
//! #defines are likewise emitted as inline-asm stub fns.
//!
//! PepperDB targets aarch64, not PowerPC; this file exists for completeness of
//! the file-for-file port. The struct layouts mirror the canonical
//! pg_atomic_uint32 / pg_atomic_uint64 in crate::port::atomics.

use crate::c::{int32, int64, uint32, uint64};

/*
 * Memory barriers. The C header (under __GNUC__) defines these as inline asm:
 *   #define pg_memory_barrier_impl() __asm__ __volatile__ ("sync" : : : "memory")
 *   #define pg_read_barrier_impl()   __asm__ __volatile__ ("lwsync" : : : "memory")
 *   #define pg_write_barrier_impl()  __asm__ __volatile__ ("lwsync" : : : "memory")
 *
 * No Rust form for the inline asm; emitted as stubs (real fences come from
 * core::sync::atomic::fence in port/atomics/mod.rs).
 */

/// `pg_memory_barrier_impl()` -> `sync` (full barrier).
#[inline]
pub unsafe fn pg_memory_barrier_impl() {
    unimplemented!()
}

/// `pg_read_barrier_impl()` -> `lwsync`.
#[inline]
pub unsafe fn pg_read_barrier_impl() {
    unimplemented!()
}

/// `pg_write_barrier_impl()` -> `lwsync`.
#[inline]
pub unsafe fn pg_write_barrier_impl() {
    unimplemented!()
}

/// `#define PG_HAVE_ATOMIC_U32_SUPPORT`
pub const PG_HAVE_ATOMIC_U32_SUPPORT: bool = true;

/// C: `typedef struct pg_atomic_uint32 { volatile uint32 value; } pg_atomic_uint32;`
#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/*
 * 64bit atomics are only supported in 64bit mode:
 *   #if SIZEOF_VOID_P >= 8
 * PepperDB targets 64-bit; emit the U64 branch unconditionally.
 */

/// `#define PG_HAVE_ATOMIC_U64_SUPPORT` (SIZEOF_VOID_P >= 8)
pub const PG_HAVE_ATOMIC_U64_SUPPORT: bool = true;

/// C: `typedef struct pg_atomic_uint64 { volatile uint64 value pg_attribute_aligned(8); } pg_atomic_uint64;`
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct pg_atomic_uint64 {
    pub value: uint64,
}

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32: bool = true;

/// C: `pg_atomic_compare_exchange_u32_impl` (lwarx/stwcx. inline asm).
///
/// Inline-asm body has no Rust form; stubbed. Real impl in port/atomics/mod.rs.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u32_impl(
    ptr: *mut pg_atomic_uint32,
    expected: *mut uint32,
    newval: uint32,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U32`
pub const PG_HAVE_ATOMIC_FETCH_ADD_U32: bool = true;

/// C: `pg_atomic_fetch_add_u32_impl` (lwarx/stwcx. inline asm).
///
/// Inline-asm body has no Rust form; stubbed. Real impl in port/atomics/mod.rs.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u32_impl(ptr: *mut pg_atomic_uint32, add_: int32) -> uint32 {
    let _ = (ptr, add_);
    unimplemented!()
}

/* #ifdef PG_HAVE_ATOMIC_U64_SUPPORT */

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: bool = true;

/// C: `pg_atomic_compare_exchange_u64_impl` (ldarx/stdcx. inline asm).
///
/// Inline-asm body has no Rust form; stubbed. Real impl in port/atomics/mod.rs.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    expected: *mut uint64,
    newval: uint64,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U64`
pub const PG_HAVE_ATOMIC_FETCH_ADD_U64: bool = true;

/// C: `pg_atomic_fetch_add_u64_impl` (ldarx/stdcx. inline asm).
///
/// Inline-asm body has no Rust form; stubbed. Real impl in port/atomics/mod.rs.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u64_impl(ptr: *mut pg_atomic_uint64, add_: int64) -> uint64 {
    let _ = (ptr, add_);
    unimplemented!()
}

/* #endif PG_HAVE_ATOMIC_U64_SUPPORT */

/// `#define PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY`
/// (per architecture manual doubleword accesses have single copy atomicity)
pub const PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY: bool = true;

//! port/atomics/generic-gcc.h - Atomic operations via gcc-compatible intrinsics.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/include/port/atomics/generic-gcc.h.
//!
//! This is one of the per-compiler atomics-variant headers. In C its bodies are
//! `static inline` functions built on gcc/clang intrinsics (`__atomic_*`,
//! `__sync_*`) and inline asm (`__asm__ __volatile__`). Those intrinsic/asm
//! bodies have NO direct Rust form, so per the port convention every such op is
//! rendered here as a `pub unsafe fn ... { unimplemented!() }` PROTOTYPE STUB.
//!
//! The REAL, working Rust atomics live in `crate::port::atomics` (the mod.rs of
//! this module), implemented over `core::sync::atomic`. That module also owns
//! the canonical `pg_atomic_flag` / `pg_atomic_uint32` / `pg_atomic_uint64`
//! struct layouts, which we re-use here rather than redefine.
//!
//! The header has intentionally no include guards and `#error`s unless
//! INSIDE_ATOMICS_H is defined; that machinery has no Rust analogue and is
//! omitted. We also emit the default/target feature branch unconditionally
//! (no cfg(...)), as is the project convention for these variant headers: we
//! assume the modern gcc/clang configuration where `HAVE_GCC__ATOMIC_*` and
//! `HAVE_GCC__SYNC_*` are all available and 64-bit atomics are NOT disabled.

use crate::port::atomics::{pg_atomic_flag, pg_atomic_uint32, pg_atomic_uint64};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Feature-detection / capability marker #defines.
//
// In C these are bare `#define PG_HAVE_*` flags that gate which inline ops the
// header provides and that downstream code (atomics.h) keys off of. We assume
// the full modern gcc/clang capability set (the typical __atomic + __sync path)
// and 64-bit atomics enabled. Translated as marker `pub const`s.
// ---------------------------------------------------------------------------

/// `#define PG_HAVE_ATOMIC_FLAG_SUPPORT`
pub const PG_HAVE_ATOMIC_FLAG_SUPPORT: bool = true;
/// `#define PG_HAVE_ATOMIC_U32_SUPPORT`
pub const PG_HAVE_ATOMIC_U32_SUPPORT: bool = true;
/// `#define PG_HAVE_ATOMIC_U64_SUPPORT`
pub const PG_HAVE_ATOMIC_U64_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_TEST_SET_FLAG`
pub const PG_HAVE_ATOMIC_TEST_SET_FLAG: bool = true;
/// `#define PG_HAVE_ATOMIC_UNLOCKED_TEST_FLAG`
pub const PG_HAVE_ATOMIC_UNLOCKED_TEST_FLAG: bool = true;
/// `#define PG_HAVE_ATOMIC_CLEAR_FLAG`
pub const PG_HAVE_ATOMIC_CLEAR_FLAG: bool = true;
/// `#define PG_HAVE_ATOMIC_INIT_FLAG`
pub const PG_HAVE_ATOMIC_INIT_FLAG: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32: bool = true;
/// `#define PG_HAVE_ATOMIC_EXCHANGE_U32`
pub const PG_HAVE_ATOMIC_EXCHANGE_U32: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U32`
pub const PG_HAVE_ATOMIC_FETCH_ADD_U32: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_SUB_U32`
pub const PG_HAVE_ATOMIC_FETCH_SUB_U32: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_AND_U32`
pub const PG_HAVE_ATOMIC_FETCH_AND_U32: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_OR_U32`
pub const PG_HAVE_ATOMIC_FETCH_OR_U32: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: bool = true;
/// `#define PG_HAVE_ATOMIC_EXCHANGE_U64`
pub const PG_HAVE_ATOMIC_EXCHANGE_U64: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U64`
pub const PG_HAVE_ATOMIC_FETCH_ADD_U64: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_SUB_U64`
pub const PG_HAVE_ATOMIC_FETCH_SUB_U64: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_AND_U64`
pub const PG_HAVE_ATOMIC_FETCH_AND_U64: bool = true;
/// `#define PG_HAVE_ATOMIC_FETCH_OR_U64`
pub const PG_HAVE_ATOMIC_FETCH_OR_U64: bool = true;

// ---------------------------------------------------------------------------
// Barrier impls.
//
//   #define pg_compiler_barrier_impl() __asm__ __volatile__("" ::: "memory")
//   #define pg_memory_barrier_impl()   __atomic_thread_fence(__ATOMIC_SEQ_CST)
//   pg_read_barrier_impl():  compiler_barrier + __atomic_thread_fence(ACQUIRE)
//   pg_write_barrier_impl(): compiler_barrier + __atomic_thread_fence(RELEASE)
//
// These expand to inline asm / compiler fence intrinsics with no Rust form;
// emitted as stubs. (Real fences would use core::sync::atomic::{compiler_fence,
// fence} -- handled in the mod.rs / atomics.h translation.)
// ---------------------------------------------------------------------------

/// `#define pg_compiler_barrier_impl() __asm__ __volatile__("" ::: "memory")`.
/// Inline-asm compiler barrier; no Rust form. See mod.rs (compiler_fence).
#[inline]
pub unsafe fn pg_compiler_barrier_impl() {
    unimplemented!()
}

/// `#define pg_memory_barrier_impl() __atomic_thread_fence(__ATOMIC_SEQ_CST)`.
/// Intrinsic full fence; no Rust form. See mod.rs (fence(SeqCst)).
#[inline]
pub unsafe fn pg_memory_barrier_impl() {
    unimplemented!()
}

/// pg_read_barrier_impl: compiler_barrier + `__atomic_thread_fence(__ATOMIC_ACQUIRE)`.
/// Intrinsic acquire fence; no Rust form. See mod.rs (fence(Acquire)).
#[inline]
pub unsafe fn pg_read_barrier_impl() {
    unimplemented!()
}

/// pg_write_barrier_impl: compiler_barrier + `__atomic_thread_fence(__ATOMIC_RELEASE)`.
/// Intrinsic release fence; no Rust form. See mod.rs (fence(Release)).
#[inline]
pub unsafe fn pg_write_barrier_impl() {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Atomic flag ops (built on __sync_lock_test_and_set / __sync_lock_release).
//
// Struct `pg_atomic_flag` (volatile int value) is owned by mod.rs. The real
// behavior is implemented there; these intrinsic-based bodies are stubs.
// ---------------------------------------------------------------------------

/// pg_atomic_test_set_flag_impl:
/// `return __sync_lock_test_and_set(&ptr->value, 1) == 0;` (acquire barrier).
/// Intrinsic TAS; no Rust form. Real impl in mod.rs (AtomicU32::swap, Acquire).
#[inline]
pub unsafe fn pg_atomic_test_set_flag_impl(ptr: *mut pg_atomic_flag) -> bool {
    let _ = ptr;
    unimplemented!()
}

/// pg_atomic_unlocked_test_flag_impl: `return ptr->value == 0;` (no barrier).
/// Real impl in mod.rs (AtomicU32::load, Relaxed).
#[inline]
pub unsafe fn pg_atomic_unlocked_test_flag_impl(ptr: *mut pg_atomic_flag) -> bool {
    let _ = ptr;
    unimplemented!()
}

/// pg_atomic_clear_flag_impl: `__sync_lock_release(&ptr->value);` (release).
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::store, Release).
#[inline]
pub unsafe fn pg_atomic_clear_flag_impl(ptr: *mut pg_atomic_flag) {
    let _ = ptr;
    unimplemented!()
}

/// pg_atomic_init_flag_impl: just calls pg_atomic_clear_flag_impl(ptr).
/// Real impl in mod.rs.
#[inline]
pub unsafe fn pg_atomic_init_flag_impl(ptr: *mut pg_atomic_flag) {
    let _ = ptr;
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Atomic uint32 ops.
//
// Struct `pg_atomic_uint32` (volatile uint32 value) is owned by mod.rs. Bodies
// here are __atomic_* / __sync_* intrinsics -> stubs.
// ---------------------------------------------------------------------------

/// pg_atomic_compare_exchange_u32_impl:
/// `__atomic_compare_exchange_n(&ptr->value, expected, newval, false, SEQ_CST, SEQ_CST)`.
/// Intrinsic CAS; no Rust form. Real impl in mod.rs (AtomicU32::compare_exchange).
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u32_impl(
    ptr: *mut pg_atomic_uint32,
    expected: *mut uint32,
    newval: uint32,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// pg_atomic_exchange_u32_impl:
/// `return __atomic_exchange_n(&ptr->value, newval, __ATOMIC_SEQ_CST);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::swap, SeqCst).
#[inline]
pub unsafe fn pg_atomic_exchange_u32_impl(
    ptr: *mut pg_atomic_uint32,
    newval: uint32,
) -> uint32 {
    let _ = (ptr, newval);
    unimplemented!()
}

/// pg_atomic_fetch_add_u32_impl: `return __sync_fetch_and_add(&ptr->value, add_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::fetch_add, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_add_u32_impl(
    ptr: *mut pg_atomic_uint32,
    add_: int32,
) -> uint32 {
    let _ = (ptr, add_);
    unimplemented!()
}

/// pg_atomic_fetch_sub_u32_impl: `return __sync_fetch_and_sub(&ptr->value, sub_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::fetch_sub, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_sub_u32_impl(
    ptr: *mut pg_atomic_uint32,
    sub_: int32,
) -> uint32 {
    let _ = (ptr, sub_);
    unimplemented!()
}

/// pg_atomic_fetch_and_u32_impl: `return __sync_fetch_and_and(&ptr->value, and_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::fetch_and, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_and_u32_impl(
    ptr: *mut pg_atomic_uint32,
    and_: uint32,
) -> uint32 {
    let _ = (ptr, and_);
    unimplemented!()
}

/// pg_atomic_fetch_or_u32_impl: `return __sync_fetch_and_or(&ptr->value, or_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU32::fetch_or, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_or_u32_impl(
    ptr: *mut pg_atomic_uint32,
    or_: uint32,
) -> uint32 {
    let _ = (ptr, or_);
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Atomic uint64 ops (guarded by `#if !defined(PG_DISABLE_64_BIT_ATOMICS)`).
//
// Struct `pg_atomic_uint64` (volatile uint64 value, aligned 8) is owned by
// mod.rs. Bodies here are __atomic_* / __sync_* intrinsics -> stubs.
// `AssertPointerAlignment(expected, 8)` in the C CAS bodies is a debug-only
// assertion with no Rust analogue here and is omitted.
// ---------------------------------------------------------------------------

/// pg_atomic_compare_exchange_u64_impl:
/// `__atomic_compare_exchange_n(&ptr->value, expected, newval, false, SEQ_CST, SEQ_CST)`.
/// Intrinsic CAS; no Rust form. Real impl in mod.rs (AtomicU64::compare_exchange).
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    expected: *mut uint64,
    newval: uint64,
) -> bool {
    let _ = (ptr, expected, newval);
    unimplemented!()
}

/// pg_atomic_exchange_u64_impl:
/// `return __atomic_exchange_n(&ptr->value, newval, __ATOMIC_SEQ_CST);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU64::swap, SeqCst).
#[inline]
pub unsafe fn pg_atomic_exchange_u64_impl(
    ptr: *mut pg_atomic_uint64,
    newval: uint64,
) -> uint64 {
    let _ = (ptr, newval);
    unimplemented!()
}

/// pg_atomic_fetch_add_u64_impl: `return __sync_fetch_and_add(&ptr->value, add_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU64::fetch_add, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_add_u64_impl(
    ptr: *mut pg_atomic_uint64,
    add_: int64,
) -> uint64 {
    let _ = (ptr, add_);
    unimplemented!()
}

/// pg_atomic_fetch_sub_u64_impl: `return __sync_fetch_and_sub(&ptr->value, sub_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU64::fetch_sub, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_sub_u64_impl(
    ptr: *mut pg_atomic_uint64,
    sub_: int64,
) -> uint64 {
    let _ = (ptr, sub_);
    unimplemented!()
}

/// pg_atomic_fetch_and_u64_impl: `return __sync_fetch_and_and(&ptr->value, and_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU64::fetch_and, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_and_u64_impl(
    ptr: *mut pg_atomic_uint64,
    and_: uint64,
) -> uint64 {
    let _ = (ptr, and_);
    unimplemented!()
}

/// pg_atomic_fetch_or_u64_impl: `return __sync_fetch_and_or(&ptr->value, or_);`.
/// Intrinsic; no Rust form. Real impl in mod.rs (AtomicU64::fetch_or, SeqCst).
#[inline]
pub unsafe fn pg_atomic_fetch_or_u64_impl(
    ptr: *mut pg_atomic_uint64,
    or_: uint64,
) -> uint64 {
    let _ = (ptr, or_);
    unimplemented!()
}

//! port/atomics/arch-x86.h - Atomic operations considerations specific to intel x86
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/include/port/atomics/arch-x86.h.
//!
//! This is a port/atomics variant header: its function-like bodies are all
//! INLINE ASSEMBLY (`__asm__ __volatile__ ("lock; cmpxchgl ...")`, `rep; nop`,
//! `xaddl`, `xchgb`, etc.) which has no direct Rust form. Per the porting
//! convention for these variant headers:
//!   - The struct/typedef/#define and PG_HAVE_* / PG_DISABLE_* markers are
//!     translated (structs reuse the canonical definitions, markers become
//!     `pub const bool`).
//!   - Each inline-asm/intrinsic `*_impl` op is rendered as an `unsafe fn`
//!     prototype stub returning `unimplemented!()`.
//!
//! The REAL Rust atomics live in `crate::port::atomics` (mod.rs), implemented
//! on top of `core::sync::atomic`. The atomic struct types
//! `pg_atomic_flag` / `pg_atomic_uint32` / `pg_atomic_uint64` are defined there
//! and re-exported here, since the C header's `typedef struct pg_atomic_*`
//! declarations describe the same types.
//!
//! The C header has no include guards and #errors unless included from
//! atomics.h; that include-guard machinery has no Rust analogue and is omitted.
//! All branches in the original are gated on compiler (`__GNUC__` /
//! `__INTEL_COMPILER` / `_MSC_VER`) and arch (`__i386__` / `__x86_64__`)
//! preprocessor macros. Per the port convention we emit the target branch
//! (gcc/clang on x86_64) unconditionally rather than using cfg(...).

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use crate::c::{int32, int64, uint32, uint64};

// The atomic struct types are the canonical definitions from the atomics mod.
// In C these are `typedef struct pg_atomic_flag  { volatile char value; }`,
// `typedef struct pg_atomic_uint32 { volatile uint32 value; }`, and (only under
// __x86_64__) `typedef struct pg_atomic_uint64 { volatile uint64 value; }`.
pub use crate::port::atomics::{pg_atomic_flag, pg_atomic_uint32, pg_atomic_uint64};

// ---------------------------------------------------------------------------
// Feature-detection markers.
//
// The C header `#define`s these bare (presence = supported). We translate each
// as a `pub const bool` set to the value for the target branch (gcc/clang
// x86_64), where every one of them is defined.
// ---------------------------------------------------------------------------

/// `#define PG_HAVE_ATOMIC_FLAG_SUPPORT` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_FLAG_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_U32_SUPPORT` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_U32_SUPPORT: bool = true;

/// `#define PG_HAVE_ATOMIC_U64_SUPPORT` (only under `__x86_64__`; target is x86_64).
pub const PG_HAVE_ATOMIC_U64_SUPPORT: bool = true;

/// `#define PG_HAVE_SPIN_DELAY` (gcc/intel branch supplies `pg_spin_delay_impl`).
pub const PG_HAVE_SPIN_DELAY: bool = true;

/// `#define PG_HAVE_ATOMIC_TEST_SET_FLAG` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_TEST_SET_FLAG: bool = true;

/// `#define PG_HAVE_ATOMIC_CLEAR_FLAG` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_CLEAR_FLAG: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32: bool = true;

/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U32` (gcc/intel branch).
pub const PG_HAVE_ATOMIC_FETCH_ADD_U32: bool = true;

/// `#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64` (only under `__x86_64__`).
pub const PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64: bool = true;

/// `#define PG_HAVE_ATOMIC_FETCH_ADD_U64` (only under `__x86_64__`).
pub const PG_HAVE_ATOMIC_FETCH_ADD_U64: bool = true;

/// `#define PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY`.
///
/// 8 byte reads/writes have single-copy atomicity on 32 bit x86 since the 586,
/// and on all x86-64 cpus. Defined unconditionally for the x86_64 target.
pub const PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY: bool = true;

// ---------------------------------------------------------------------------
// Memory / compiler barriers.
//
// `#define pg_memory_barrier_impl()` expands to `__asm__ __volatile__("lock;
// addl $0,0(%%rsp)" : : : "memory", "cc")` on x86_64 (`...%%esp` on i386).
// `pg_read_barrier_impl()` / `pg_write_barrier_impl()` are `#define`d to
// `pg_compiler_barrier_impl()`. These are inline-asm / compiler-barrier macros
// with no Rust form; stubbed. The real barriers live in the atomics mod via
// core::sync::atomic fences.
// ---------------------------------------------------------------------------

/// `pg_memory_barrier_impl()` - `lock; addl $0,0(%rsp)` full fence. Inline-asm stub.
#[inline]
pub unsafe fn pg_memory_barrier_impl() {
    unimplemented!()
}

/// `pg_read_barrier_impl()` -> `pg_compiler_barrier_impl()`. Compiler-barrier stub.
#[inline]
pub unsafe fn pg_read_barrier_impl() {
    unimplemented!()
}

/// `pg_write_barrier_impl()` -> `pg_compiler_barrier_impl()`. Compiler-barrier stub.
#[inline]
pub unsafe fn pg_write_barrier_impl() {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Spin delay.
//
// `pg_spin_delay_impl()` issues the PAUSE instruction (`rep; nop` on gcc,
// `_mm_pause()` on MSVC) as a spin-wait hint. Inline-asm/intrinsic; stubbed.
// ---------------------------------------------------------------------------

/// `pg_spin_delay_impl()` - PAUSE (`rep; nop`) spin-wait hint. Inline-asm stub.
#[inline]
pub unsafe fn pg_spin_delay_impl() {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Inline-asm atomic ops (gcc/intel branch).
//
// Each of these is implemented in C with `__asm__ __volatile__` using
// `lock`-prefixed x86 instructions (xchgb / cmpxchgl / xaddl / cmpxchgq /
// xaddq). No Rust form; rendered as prototype stubs. The real, lock-free
// implementations live in crate::port::atomics on top of core::sync::atomic.
// ---------------------------------------------------------------------------

/// `pg_atomic_test_set_flag_impl` - `lock; xchgb`, returns true if was clear. Stub.
#[inline]
pub unsafe fn pg_atomic_test_set_flag_impl(_ptr: *mut pg_atomic_flag) -> bool {
    unimplemented!()
}

/// `pg_atomic_clear_flag_impl` - compiler barrier then `ptr->value = 0`. Stub.
#[inline]
pub unsafe fn pg_atomic_clear_flag_impl(_ptr: *mut pg_atomic_flag) {
    unimplemented!()
}

/// `pg_atomic_compare_exchange_u32_impl` - `lock; cmpxchgl` + `setz`. Stub.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u32_impl(
    _ptr: *mut pg_atomic_uint32,
    _expected: *mut uint32,
    _newval: uint32,
) -> bool {
    unimplemented!()
}

/// `pg_atomic_fetch_add_u32_impl` - `lock; xaddl`, returns previous value. Stub.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u32_impl(_ptr: *mut pg_atomic_uint32, _add_: int32) -> uint32 {
    unimplemented!()
}

/// `pg_atomic_compare_exchange_u64_impl` - `lock; cmpxchgq` + `setz` (`__x86_64__`). Stub.
#[inline]
pub unsafe fn pg_atomic_compare_exchange_u64_impl(
    _ptr: *mut pg_atomic_uint64,
    _expected: *mut uint64,
    _newval: uint64,
) -> bool {
    unimplemented!()
}

/// `pg_atomic_fetch_add_u64_impl` - `lock; xaddq`, returns previous value (`__x86_64__`). Stub.
#[inline]
pub unsafe fn pg_atomic_fetch_add_u64_impl(_ptr: *mut pg_atomic_uint64, _add_: int64) -> uint64 {
    unimplemented!()
}

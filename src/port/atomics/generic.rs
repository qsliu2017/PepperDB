//! port/atomics/generic.h - higher-level atomic ops built on lower-level primitives.
//!
//! Source: postgres/src/include/port/atomics/generic.h
//!
//! This header "intentionally has no include guards, should only be included by
//! atomics.h". It provides the GENERIC FALLBACK implementations of the higher
//! level atomic operations, derived from a smaller set of platform-provided
//! lower-level primitives (compare_exchange / exchange / fetch_add / read /
//! write). Every fallback here is guarded by `#if !defined(PG_HAVE_...) && ...`,
//! i.e. it is only compiled when the platform-specific header
//! (generic-gcc.h, arch-x86.h, fallback.h, ...) did NOT already provide that
//! operation natively.
//!
//! Translation notes:
//!  - These are `static inline ... _impl(...)` functions. We translate them
//!    1:1 as `#[inline] pub fn ..._impl(...)`, keeping the C identifiers.
//!  - The C parameters are `volatile pg_atomic_uintN *ptr`; the ported lower
//!    level ops (in crate::port::atomics) take `&pg_atomic_uintN` with interior
//!    mutability, so these wrappers take `&` as well and need no `mut`/`unsafe`.
//!  - The C `#define` feature-test macros (PG_HAVE_ATOMIC_*) are NOT data; they
//!    are configuration. We emit the default (generic) branch unconditionally.
//!    On the native gcc path most of these are shadowed by generic-gcc.h, so
//!    the file is effectively a reference of the portable fallback algorithms.
//!  - `ptr->value` direct field accesses ("ok if read is not atomic") are
//!    translated via pg_atomic_read_u32/64_impl, which is the value-load the
//!    native structs expose; the C code reads the raw field for a cheap
//!    (non-atomic) initial guess before the CAS loop.

use crate::c::{int32, int64, uint32, uint64};
use crate::port::atomics::{
    pg_atomic_compare_exchange_u32_impl,
    pg_atomic_compare_exchange_u64_impl_native as pg_atomic_compare_exchange_u64_impl,
    pg_atomic_flag,
    pg_atomic_read_u32_impl, pg_atomic_uint32, pg_atomic_uint64, pg_atomic_write_u32_impl,
};

// ---------------------------------------------------------------------------
// Local stubs for symbols this header relies on but that are provided by other
// (config-selected) atomics headers / macros not represented standalone here.
// TODO: dedup once the corresponding headers are ported.
// ---------------------------------------------------------------------------

/// `pg_memory_barrier_impl()` - full memory barrier. Provided per-platform
/// (generic-gcc.h / arch-*.h). Stubbed to a SeqCst fence locally.
#[inline]
pub fn pg_memory_barrier_impl() {
    core::sync::atomic::fence(core::sync::atomic::Ordering::SeqCst);
}

/// `pg_read_barrier_impl` upgraded to a full barrier when undefined (lines 24-26).
#[inline]
pub fn pg_read_barrier_impl() {
    pg_memory_barrier_impl();
}

/// `pg_write_barrier_impl` upgraded to a full barrier when undefined (lines 27-29).
#[inline]
pub fn pg_write_barrier_impl() {
    pg_memory_barrier_impl();
}

/// `pg_spin_delay_impl()` fallback `((void)0)` (lines 31-34).
#[inline]
pub fn pg_spin_delay_impl() {}

/// `AssertPointerAlignment(ptr, 8)` - compile/runtime alignment assert; no-op port.
#[inline]
fn AssertPointerAlignment<T>(_ptr: *const T, _align: usize) {}

// ---------------------------------------------------------------------------
// PG_HAVE_ATOMIC_FLAG_SUPPORT fallback (lines 38-41):
//   typedef pg_atomic_uint32 pg_atomic_flag;
// pg_atomic_flag already aliases/wraps a u32 in crate::port::atomics; nothing to
// re-typedef here. Documented for completeness.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Basic u32 read/write fallbacks (lines 43-68).
// On native struct layouts these reduce to plain field accesses.
// ---------------------------------------------------------------------------

/// generic.h: `pg_atomic_read_u32_impl` fallback - `return ptr->value;` (43-50).
#[inline]
pub unsafe fn pg_atomic_read_u32_impl_generic(ptr: &pg_atomic_uint32) -> uint32 {
    pg_atomic_read_u32_impl(ptr)
}

/// generic.h: `pg_atomic_write_u32_impl` fallback - `ptr->value = val;` (52-59).
#[inline]
pub unsafe fn pg_atomic_write_u32_impl_generic(ptr: &pg_atomic_uint32, val: uint32) {
    pg_atomic_write_u32_impl(ptr, val);
}

/// generic.h: `pg_atomic_unlocked_write_u32_impl` - `ptr->value = val;` (61-68).
#[inline]
pub unsafe fn pg_atomic_unlocked_write_u32_impl(ptr: &pg_atomic_uint32, val: uint32) {
    pg_atomic_write_u32_impl(ptr, val);
}

// ---------------------------------------------------------------------------
// Flag fallbacks via exchange OR compare_exchange (lines 70-145).
//
// The C header picks ONE of two mutually-exclusive branches at compile time:
//   (A) if PG_HAVE_ATOMIC_EXCHANGE_U32  -> test_set via exchange.
//   (B) elif PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32 -> test_set via cmpxchg.
// init/clear/unlocked_test are identical in both branches. We translate branch
// (B) (compare_exchange) as the portable default, since the lowest-level
// primitive the ported tree exposes is compare_exchange. init/clear/unlocked
// are emitted once.
// ---------------------------------------------------------------------------

/// generic.h: `pg_atomic_init_flag_impl` - `pg_atomic_write_u32_impl(ptr, 0)` (76-80 / 113-117).
#[inline]
pub unsafe fn pg_atomic_init_flag_impl(ptr: &pg_atomic_flag) {
    pg_atomic_write_u32_impl(flag_as_u32(ptr), 0);
}

/// A `pg_atomic_flag` is `#[repr(C)]` with a single `AtomicU32` field, identical
/// in layout to `pg_atomic_uint32`; generic.h's flag ops run on the u32 ops.
#[inline]
unsafe fn flag_as_u32(p: &pg_atomic_flag) -> &pg_atomic_uint32 {
    &*(p as *const pg_atomic_flag as *const pg_atomic_uint32)
}

/// generic.h branch (B): `pg_atomic_test_set_flag_impl` via compare_exchange (119-125).
///   uint32 value = 0; return pg_atomic_compare_exchange_u32_impl(ptr, &value, 1);
#[inline]
pub unsafe fn pg_atomic_test_set_flag_impl(ptr: &pg_atomic_flag) -> bool {
    let mut value: uint32 = 0;
    pg_atomic_compare_exchange_u32_impl(flag_as_u32(ptr), &mut value, 1)
}

/// generic.h: `pg_atomic_unlocked_test_flag_impl` - `read == 0` (89-94 / 127-132).
#[inline]
pub unsafe fn pg_atomic_unlocked_test_flag_impl(ptr: &pg_atomic_flag) -> bool {
    pg_atomic_read_u32_impl(flag_as_u32(ptr)) == 0
}

/// generic.h: `pg_atomic_clear_flag_impl` - barrier then write 0 (97-104 / 134-141).
#[inline]
pub unsafe fn pg_atomic_clear_flag_impl(ptr: &pg_atomic_flag) {
    // XXX: release semantics suffice?
    pg_memory_barrier_impl();
    pg_atomic_write_u32_impl(flag_as_u32(ptr), 0);
}

// ---------------------------------------------------------------------------
// u32 init / exchange / fetch-* / *-fetch / membarrier fallbacks (148-252).
// ---------------------------------------------------------------------------

/// generic.h: `pg_atomic_init_u32_impl` - `ptr->value = val_;` (148-155).
#[inline]
pub unsafe fn pg_atomic_init_u32_impl(ptr: &pg_atomic_uint32, val_: uint32) {
    pg_atomic_write_u32_impl(ptr, val_);
}

/// generic.h: `pg_atomic_exchange_u32_impl` via compare_exchange loop (157-168).
#[inline]
pub unsafe fn pg_atomic_exchange_u32_impl(ptr: &pg_atomic_uint32, xchg_: uint32) -> uint32 {
    let mut old: uint32 = pg_atomic_read_u32_impl(ptr); // ok if read is not atomic
    while !pg_atomic_compare_exchange_u32_impl(ptr, &mut old, xchg_) {
        // skip
    }
    old
}

/// generic.h: `pg_atomic_fetch_add_u32_impl` via compare_exchange loop (170-181).
#[inline]
pub unsafe fn pg_atomic_fetch_add_u32_impl(ptr: &pg_atomic_uint32, add_: int32) -> uint32 {
    let mut old: uint32 = pg_atomic_read_u32_impl(ptr); // ok if read is not atomic
    loop {
        let new = old.wrapping_add(add_ as uint32);
        if pg_atomic_compare_exchange_u32_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_fetch_sub_u32_impl` - `fetch_add(ptr, -sub_)` (183-190).
#[inline]
pub unsafe fn pg_atomic_fetch_sub_u32_impl(ptr: &pg_atomic_uint32, sub_: int32) -> uint32 {
    pg_atomic_fetch_add_u32_impl(ptr, sub_.wrapping_neg())
}

/// generic.h: `pg_atomic_fetch_and_u32_impl` via compare_exchange loop (192-203).
#[inline]
pub unsafe fn pg_atomic_fetch_and_u32_impl(ptr: &pg_atomic_uint32, and_: uint32) -> uint32 {
    let mut old: uint32 = pg_atomic_read_u32_impl(ptr); // ok if read is not atomic
    loop {
        let new = old & and_;
        if pg_atomic_compare_exchange_u32_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_fetch_or_u32_impl` via compare_exchange loop (205-216).
#[inline]
pub unsafe fn pg_atomic_fetch_or_u32_impl(ptr: &pg_atomic_uint32, or_: uint32) -> uint32 {
    let mut old: uint32 = pg_atomic_read_u32_impl(ptr); // ok if read is not atomic
    loop {
        let new = old | or_;
        if pg_atomic_compare_exchange_u32_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_add_fetch_u32_impl` - `fetch_add + add_` (218-225).
#[inline]
pub unsafe fn pg_atomic_add_fetch_u32_impl(ptr: &pg_atomic_uint32, add_: int32) -> uint32 {
    pg_atomic_fetch_add_u32_impl(ptr, add_).wrapping_add(add_ as uint32)
}

/// generic.h: `pg_atomic_sub_fetch_u32_impl` - `fetch_sub - sub_` (227-234).
#[inline]
pub unsafe fn pg_atomic_sub_fetch_u32_impl(ptr: &pg_atomic_uint32, sub_: int32) -> uint32 {
    pg_atomic_fetch_sub_u32_impl(ptr, sub_).wrapping_sub(sub_ as uint32)
}

/// generic.h: `pg_atomic_read_membarrier_u32_impl` - `fetch_add(ptr, 0)` (236-243).
#[inline]
pub unsafe fn pg_atomic_read_membarrier_u32_impl(ptr: &pg_atomic_uint32) -> uint32 {
    pg_atomic_fetch_add_u32_impl(ptr, 0)
}

/// generic.h: `pg_atomic_write_membarrier_u32_impl` - `(void) exchange(ptr, val)` (245-252).
#[inline]
pub unsafe fn pg_atomic_write_membarrier_u32_impl(ptr: &pg_atomic_uint32, val: uint32) {
    let _ = pg_atomic_exchange_u32_impl(ptr, val);
}

// ---------------------------------------------------------------------------
// u64 exchange / write / read / init / fetch-* / *-fetch / membarrier (254-427).
// ---------------------------------------------------------------------------

/// generic.h: `pg_atomic_exchange_u64_impl` via compare_exchange loop (254-265).
#[inline]
pub unsafe fn pg_atomic_exchange_u64_impl(ptr: &pg_atomic_uint64, xchg_: uint64) -> uint64 {
    let mut old: uint64 = pg_atomic_read_u64_raw(ptr); // ok if read is not atomic
    while !pg_atomic_compare_exchange_u64_impl(ptr, &mut old, xchg_) {
        // skip
    }
    old
}

/// generic.h: `pg_atomic_write_u64_impl` (267-298).
///
/// Two compile-time branches:
///   - PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY && !U64_SIMULATION:
///       AssertPointerAlignment(ptr, 8); ptr->value = val;   (aligned store is atomic)
///   - else: implement as an atomic exchange.
/// We translate the native (single-copy-atomicity) branch as the default, since
/// that matches the ported native struct layout; the exchange-based fallback is
/// documented and equivalent in effect.
#[inline]
pub unsafe fn pg_atomic_write_u64_impl(ptr: &pg_atomic_uint64, val: uint64) {
    // Native branch: aligned 64-bit store is atomic on this platform.
    AssertPointerAlignment(ptr as *const pg_atomic_uint64, 8);
    pg_atomic_write_u64_raw(ptr, val);
    // Fallback branch (non-single-copy-atomic platforms):
    //   pg_atomic_exchange_u64_impl(ptr, val);
}

/// generic.h: `pg_atomic_read_u64_impl` (300-334).
///
/// Two compile-time branches:
///   - single-copy-atomicity: AssertPointerAlignment(ptr, 8); return ptr->value;
///   - else: uint64 old = 0; pg_atomic_compare_exchange_u64_impl(ptr, &old, 0); return old;
/// Native branch translated as default.
#[inline]
pub unsafe fn pg_atomic_read_u64_impl(ptr: &pg_atomic_uint64) -> uint64 {
    // Native branch: aligned 64-bit read is atomic on this platform.
    AssertPointerAlignment(ptr as *const pg_atomic_uint64, 8);
    pg_atomic_read_u64_raw(ptr)
    // Fallback branch:
    //   let mut old: uint64 = 0;
    //   pg_atomic_compare_exchange_u64_impl(ptr, &mut old, 0);
    //   old
}

/// generic.h: `pg_atomic_init_u64_impl` - `ptr->value = val_;` (336-343).
#[inline]
pub unsafe fn pg_atomic_init_u64_impl(ptr: &pg_atomic_uint64, val_: uint64) {
    pg_atomic_write_u64_raw(ptr, val_);
}

/// generic.h: `pg_atomic_fetch_add_u64_impl` via compare_exchange loop (345-356).
#[inline]
pub unsafe fn pg_atomic_fetch_add_u64_impl(ptr: &pg_atomic_uint64, add_: int64) -> uint64 {
    let mut old: uint64 = pg_atomic_read_u64_raw(ptr); // ok if read is not atomic
    loop {
        let new = old.wrapping_add(add_ as uint64);
        if pg_atomic_compare_exchange_u64_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_fetch_sub_u64_impl` - `fetch_add(ptr, -sub_)` (358-365).
#[inline]
pub unsafe fn pg_atomic_fetch_sub_u64_impl(ptr: &pg_atomic_uint64, sub_: int64) -> uint64 {
    pg_atomic_fetch_add_u64_impl(ptr, sub_.wrapping_neg())
}

/// generic.h: `pg_atomic_fetch_and_u64_impl` via compare_exchange loop (367-378).
#[inline]
pub unsafe fn pg_atomic_fetch_and_u64_impl(ptr: &pg_atomic_uint64, and_: uint64) -> uint64 {
    let mut old: uint64 = pg_atomic_read_u64_raw(ptr); // ok if read is not atomic
    loop {
        let new = old & and_;
        if pg_atomic_compare_exchange_u64_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_fetch_or_u64_impl` via compare_exchange loop (380-391).
#[inline]
pub unsafe fn pg_atomic_fetch_or_u64_impl(ptr: &pg_atomic_uint64, or_: uint64) -> uint64 {
    let mut old: uint64 = pg_atomic_read_u64_raw(ptr); // ok if read is not atomic
    loop {
        let new = old | or_;
        if pg_atomic_compare_exchange_u64_impl(ptr, &mut old, new) {
            break;
        }
    }
    old
}

/// generic.h: `pg_atomic_add_fetch_u64_impl` - `fetch_add + add_` (393-400).
#[inline]
pub unsafe fn pg_atomic_add_fetch_u64_impl(ptr: &pg_atomic_uint64, add_: int64) -> uint64 {
    pg_atomic_fetch_add_u64_impl(ptr, add_).wrapping_add(add_ as uint64)
}

/// generic.h: `pg_atomic_sub_fetch_u64_impl` - `fetch_sub - sub_` (402-409).
#[inline]
pub unsafe fn pg_atomic_sub_fetch_u64_impl(ptr: &pg_atomic_uint64, sub_: int64) -> uint64 {
    pg_atomic_fetch_sub_u64_impl(ptr, sub_).wrapping_sub(sub_ as uint64)
}

/// generic.h: `pg_atomic_read_membarrier_u64_impl` - `fetch_add(ptr, 0)` (411-418).
#[inline]
pub unsafe fn pg_atomic_read_membarrier_u64_impl(ptr: &pg_atomic_uint64) -> uint64 {
    pg_atomic_fetch_add_u64_impl(ptr, 0)
}

/// generic.h: `pg_atomic_write_membarrier_u64_impl` - `(void) exchange(ptr, val)` (420-427).
#[inline]
pub unsafe fn pg_atomic_write_membarrier_u64_impl(ptr: &pg_atomic_uint64, val: uint64) {
    let _ = pg_atomic_exchange_u64_impl(ptr, val);
}

// ---------------------------------------------------------------------------
// Helpers: raw (non-CAS) 64-bit field load/store.
//
// generic.h reads/writes `ptr->value` directly in several places ("ok if read
// is not atomic", and the single-copy-atomic native store/load). The ported
// crate::port::atomics module exposes pg_atomic_read_u32_impl/write_u32_impl for
// u32 but its u64 accessors are split into _native/_sim variants; to keep this
// header self-contained and faithful to the raw-field semantics, we read/write
// the underlying AtomicU64 with Relaxed ordering (the C code does an explicit
// barrier or CAS where ordering matters).
// TODO: dedup against crate::port::atomics u64 accessors once unified.
// ---------------------------------------------------------------------------

#[inline]
unsafe fn pg_atomic_read_u64_raw(ptr: &pg_atomic_uint64) -> uint64 {
    // pg_atomic_uint64 wraps an AtomicU64 (`value` field). Mirror `ptr->value`.
    let p = ptr as *const pg_atomic_uint64 as *const core::sync::atomic::AtomicU64;
    (*p).load(core::sync::atomic::Ordering::Relaxed)
}

#[inline]
unsafe fn pg_atomic_write_u64_raw(ptr: &pg_atomic_uint64, val: uint64) {
    let p = ptr as *const pg_atomic_uint64 as *const core::sync::atomic::AtomicU64;
    (*p).store(val, core::sync::atomic::Ordering::Relaxed);
}

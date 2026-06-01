//! Non-inline parts of the atomics implementation.
//!
//! Source: postgres/src/backend/port/atomics.c
//! Merged decls from:
//!   - postgres/src/include/port/atomics.h          (public struct usage / API shape)
//!   - postgres/src/include/port/atomics/fallback.h (PG_HAVE_ATOMIC_U64_SIMULATION path)
//!   - postgres/src/include/port/atomics/generic-gcc.h
//!         (pg_atomic_flag / pg_atomic_uint32 / pg_atomic_uint64 struct layouts +
//!          the inline *_impl flag/u32 ops that on most platforms live in the header)
//!
//! atomics.c itself is tiny: on platforms WITHOUT native 64-bit atomics it provides
//! the spinlock-simulated u64 fallbacks (pg_atomic_init_u64_impl,
//! pg_atomic_compare_exchange_u64_impl, pg_atomic_fetch_add_u64_impl), guarded by
//! `#ifdef PG_HAVE_ATOMIC_U64_SIMULATION`. On a platform with native atomics (the
//! common case, e.g. gcc/clang on x86_64/aarch64) the whole file compiles to nothing
//! and the real work is done by the inline header impls.
//!
//! This translation follows the task: define the structs, map the REAL (native)
//! behavior to `core::sync::atomic` with `Ordering::SeqCst`, and STUB the
//! spinlock-SIMULATION branch (which depends on S_LOCK / storage/spin.h, not ported).

use crate::prelude::*;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

// Per-platform atomic primitive headers (postgres/src/include/port/atomics/).
pub mod arch_arm;
pub mod arch_ppc;
pub mod arch_x86;
pub mod fallback;
pub mod generic_gcc;
pub mod generic_msvc;
pub mod generic_sunpro;
pub mod generic;

// ---------------------------------------------------------------------------
// Struct definitions (generic-gcc.h native layouts).
//
// On the native gcc path the structs are thin wrappers around the value. We use
// the Rust atomic types as the wrapped value so the operations below are real,
// lock-free, and `&self`-safe. `#[repr(C)]` mirrors the single-field C structs
// (AtomicU32/AtomicU64 are themselves `#[repr(C)]` over u32/u64, so the layout
// matches `struct { volatile uintN value; }`).
//
// Note: the C API takes `volatile pg_atomic_* *ptr` (a shared mutable pointer);
// the Rust atomic types give us interior mutability, so our ports take `&` and
// need no `unsafe`/`mut` for the value access.
// ---------------------------------------------------------------------------

/// generic-gcc.h: `struct pg_atomic_flag { volatile int value; }`.
/// TAS lock flag. We back it with an AtomicU32 (0 = clear, 1 = set).
#[repr(C)]
pub struct pg_atomic_flag {
    pub value: AtomicU32,
}

/// generic-gcc.h: `struct pg_atomic_uint32 { volatile uint32 value; }`.
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: AtomicU32,
}

/// generic-gcc.h native layout: `struct pg_atomic_uint64 { volatile uint64 value aligned(8); }`.
///
/// NB: in the SIMULATION fallback (fallback.h) this struct instead is
/// `{ int sema; volatile uint64 value; }` where `sema` is a spinlock. That path
/// is not ported (see the simulation stubs below); on the native path the sema
/// field does not exist.
#[repr(C, align(8))]
pub struct pg_atomic_uint64 {
    pub value: AtomicU64,
}

// ===========================================================================
// Native (real) implementations.
//
// These mirror the inline *_impl functions that, in C, live in
// port/atomics/generic-gcc.h and are pulled into atomics.h. They are the REAL
// behavior used whenever the compiler has native atomics. Mapped 1:1 onto
// core::sync::atomic with SeqCst (PostgreSQL's atomics carry full barrier
// semantics for the read-modify-write ops; SeqCst is the safe superset).
// ===========================================================================

// ---- flag ops (generic-gcc.h) --------------------------------------------

/// generic-gcc.h pg_atomic_clear_flag_impl: `__sync_lock_release(&ptr->value)`.
/// Release semantics.
#[inline]
pub fn pg_atomic_clear_flag_impl(ptr: &pg_atomic_flag) {
    ptr.value.store(0, Ordering::Release);
}

/// generic-gcc.h pg_atomic_init_flag_impl: just clears the flag.
#[inline]
pub fn pg_atomic_init_flag_impl(ptr: &pg_atomic_flag) {
    pg_atomic_clear_flag_impl(ptr);
}

/// generic-gcc.h pg_atomic_test_set_flag_impl:
/// `__sync_lock_test_and_set(&ptr->value, 1) == 0`. Acquire semantics; returns
/// true if WE set it (i.e. previous value was 0).
#[inline]
pub fn pg_atomic_test_set_flag_impl(ptr: &pg_atomic_flag) -> bool {
    ptr.value.swap(1, Ordering::Acquire) == 0
}

/// generic-gcc.h pg_atomic_unlocked_test_flag_impl: `ptr->value == 0`.
/// No barrier semantics.
#[inline]
pub fn pg_atomic_unlocked_test_flag_impl(ptr: &pg_atomic_flag) -> bool {
    ptr.value.load(Ordering::Relaxed) == 0
}

// ---- u32 ops (generic-gcc.h) ---------------------------------------------

/// generic-gcc.h: initialize. `ptr->value = val_`. No barrier semantics.
#[inline]
pub fn pg_atomic_init_u32_impl(ptr: &pg_atomic_uint32, val_: uint32) {
    ptr.value.store(val_, Ordering::Relaxed);
}

/// generic-gcc.h: unlocked read. No barrier semantics.
#[inline]
pub fn pg_atomic_read_u32_impl(ptr: &pg_atomic_uint32) -> uint32 {
    ptr.value.load(Ordering::Relaxed)
}

/// generic-gcc.h: write. No barrier semantics (but a whole-value store).
#[inline]
pub fn pg_atomic_write_u32_impl(ptr: &pg_atomic_uint32, val: uint32) {
    ptr.value.store(val, Ordering::Relaxed);
}

/// generic-gcc.h pg_atomic_compare_exchange_u32_impl. Full barrier (SeqCst).
/// Strong CAS: stores newval iff *ptr == *expected; always writes the observed
/// value back into *expected; returns whether the swap happened.
#[inline]
pub fn pg_atomic_compare_exchange_u32_impl(
    ptr: &pg_atomic_uint32,
    expected: &mut uint32,
    newval: uint32,
) -> bool {
    match ptr
        .value
        .compare_exchange(*expected, newval, Ordering::SeqCst, Ordering::SeqCst)
    {
        Ok(_) => true,
        Err(actual) => {
            *expected = actual;
            false
        }
    }
}

/// generic-gcc.h pg_atomic_fetch_add_u32_impl. Returns prior value. Full barrier.
/// `add_` is signed (int32); wrapping add matches C's modular uint arithmetic.
#[inline]
pub fn pg_atomic_fetch_add_u32_impl(ptr: &pg_atomic_uint32, add_: int32) -> uint32 {
    ptr.value.fetch_add(add_ as uint32, Ordering::SeqCst)
}

// ===========================================================================
// 64-bit: native implementations (mirroring generic-gcc.h / generic.h inline
// impls used when PG_HAVE_ATOMIC_U64_SUPPORT is native, i.e. NOT simulated).
//
// atomics.c only defines the *simulated* u64 fallbacks; those are stubbed
// further down. These native versions are the counterparts to the u32 ops above
// and are what the rest of the backend actually links against on this platform.
// ===========================================================================

/// init u64 (native path). No barrier semantics.
#[inline]
pub fn pg_atomic_init_u64_impl_native(ptr: &pg_atomic_uint64, val_: uint64) {
    ptr.value.store(val_, Ordering::Relaxed);
}

/// read u64 (native path). No barrier semantics.
#[inline]
pub fn pg_atomic_read_u64_impl_native(ptr: &pg_atomic_uint64) -> uint64 {
    ptr.value.load(Ordering::Relaxed)
}

/// compare-exchange u64 (native path). Strong CAS, full barrier.
#[inline]
pub fn pg_atomic_compare_exchange_u64_impl_native(
    ptr: &pg_atomic_uint64,
    expected: &mut uint64,
    newval: uint64,
) -> bool {
    match ptr
        .value
        .compare_exchange(*expected, newval, Ordering::SeqCst, Ordering::SeqCst)
    {
        Ok(_) => true,
        Err(actual) => {
            *expected = actual;
            false
        }
    }
}

/// fetch-add u64 (native path). Returns prior value, full barrier.
#[inline]
pub fn pg_atomic_fetch_add_u64_impl_native(ptr: &pg_atomic_uint64, add_: int64) -> uint64 {
    ptr.value.fetch_add(add_ as uint64, Ordering::SeqCst)
}

// ===========================================================================
// SIMULATION fallback path: the actual body of atomics.c, guarded by
// `#ifdef PG_HAVE_ATOMIC_U64_SIMULATION`.
//
// These use a spinlock (`ptr->sema`, a slock_t) via storage/spin.h:
//   SpinLockInit / SpinLockAcquire / SpinLockRelease, ultimately S_LOCK / TAS.
// storage/spin.h is NOT ported, so these are stubbed. They are unreachable on
// any platform that has native 64-bit atomics (the common case here), so the
// native *_native fns above are what's used in practice.
//
// If/when storage/spin.h is ported, replace the unimplemented!() bodies with the
// 1:1 spinlock-guarded logic shown in the C source comments.
// ===========================================================================

/// atomics.c pg_atomic_init_u64_impl (PG_HAVE_ATOMIC_U64_SIMULATION).
/// C body: SpinLockInit(&ptr->sema); ptr->value = val_;
/// STUB: depends on storage/spin.h (SpinLockInit / slock_t), not ported.
#[allow(unused_variables)]
pub fn pg_atomic_init_u64_impl_sim(ptr: &pg_atomic_uint64, val_: uint64) {
    // TODO: port storage/spin.h (SpinLockInit) then implement the simulated body.
    unimplemented!("pg_atomic_init_u64_impl simulation requires storage/spin.h (not ported)");
}

/// atomics.c pg_atomic_compare_exchange_u64_impl (PG_HAVE_ATOMIC_U64_SIMULATION).
/// C body: SpinLockAcquire(&ptr->sema); strong CAS; SpinLockRelease(&ptr->sema);
/// STUB: depends on storage/spin.h (SpinLockAcquire/Release), not ported.
#[allow(unused_variables)]
pub fn pg_atomic_compare_exchange_u64_impl_sim(
    ptr: &pg_atomic_uint64,
    expected: &mut uint64,
    newval: uint64,
) -> bool {
    // TODO: port storage/spin.h then implement the spinlock-guarded strong CAS.
    unimplemented!(
        "pg_atomic_compare_exchange_u64_impl simulation requires storage/spin.h (not ported)"
    );
}

/// atomics.c pg_atomic_fetch_add_u64_impl (PG_HAVE_ATOMIC_U64_SIMULATION).
/// C body: SpinLockAcquire; oldval = ptr->value; ptr->value += add_; SpinLockRelease;
/// STUB: depends on storage/spin.h (SpinLockAcquire/Release), not ported.
#[allow(unused_variables)]
pub fn pg_atomic_fetch_add_u64_impl_sim(ptr: &pg_atomic_uint64, add_: int64) -> uint64 {
    // TODO: port storage/spin.h then implement the spinlock-guarded fetch-add.
    unimplemented!("pg_atomic_fetch_add_u64_impl simulation requires storage/spin.h (not ported)");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u32_init_fetch_add_cas_roundtrip() {
        // Native path round-trip: init + read.
        let a = pg_atomic_uint32 {
            value: AtomicU32::new(0),
        };
        pg_atomic_init_u32_impl(&a, 100);
        assert_eq!(pg_atomic_read_u32_impl(&a), 100);

        // fetch_add returns the PRIOR value, then the stored value advances.
        let prior = pg_atomic_fetch_add_u32_impl(&a, 23);
        assert_eq!(prior, 100);
        assert_eq!(pg_atomic_read_u32_impl(&a), 123);

        // Successful CAS: expected matches, swap happens, expected unchanged.
        let mut expected: uint32 = 123;
        let ok = pg_atomic_compare_exchange_u32_impl(&a, &mut expected, 999);
        assert!(ok);
        assert_eq!(expected, 123);
        assert_eq!(pg_atomic_read_u32_impl(&a), 999);

        // Failing CAS: expected is wrong; no swap; expected gets the observed value.
        let mut expected2: uint32 = 0;
        let ok2 = pg_atomic_compare_exchange_u32_impl(&a, &mut expected2, 1);
        assert!(!ok2);
        assert_eq!(expected2, 999);
        assert_eq!(pg_atomic_read_u32_impl(&a), 999);
    }

    #[test]
    fn flag_test_set_and_clear() {
        let f = pg_atomic_flag {
            value: AtomicU32::new(0),
        };
        pg_atomic_init_flag_impl(&f);
        assert!(pg_atomic_unlocked_test_flag_impl(&f));
        // First TAS succeeds (we set it from 0).
        assert!(pg_atomic_test_set_flag_impl(&f));
        // Now it's set; a second TAS fails and unlocked test reports not-free.
        assert!(!pg_atomic_test_set_flag_impl(&f));
        assert!(!pg_atomic_unlocked_test_flag_impl(&f));
        // Clear releases it.
        pg_atomic_clear_flag_impl(&f);
        assert!(pg_atomic_unlocked_test_flag_impl(&f));
    }
}

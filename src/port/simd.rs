//! port/simd.h - Support for platform-specific vector operations.
//!
//! NOTES (from the C header):
//! - VectorN refers to a register where the element operands are N bits wide.
//!   The vector width is platform-specific, so users that care about it must
//!   inspect `size_of::<VectorN>()`.
//!
//! PORTING NOTE: The C header selects one of three implementations at
//! compile time based on the target architecture:
//!   - `USE_SSE2`  (x86_64): `Vector8`/`Vector32` are `__m128i`, using SSE2
//!     intrinsics from <emmintrin.h>.
//!   - `USE_NEON`  (aarch64 + Neon): `Vector8` = `uint8x16_t`,
//!     `Vector32` = `uint32x4_t`, using arm_neon.h intrinsics.
//!   - `USE_NO_SIMD` (everything else): `Vector8` = `uint64`, and the
//!     Vector32 machinery / vector-returning comparisons are NOT provided.
//!
//! These are genuine architecture selections, not Cargo features. Following the
//! PepperDB convention of emitting the portable default branch unconditionally,
//! and matching the existing `src/port/pg_lfind.rs` port (which already chose
//! the `USE_NO_SIMD` scalar path with `Vector8 = uint64`), this module
//! translates the `USE_NO_SIMD` code paths faithfully. Functions that exist
//! only when SIMD is available (`vector32_*`, `vector8_eq`, `vector8_min`,
//! `vector8_ssub`, `vector32_is_highbit_set`, `vector8_highbit_mask`,
//! `vector8_load`/`vector32_load` SIMD forms) are translated as `unimplemented!()`
//! prototypes, since no portable definition exists in the C source.
//! TODO: dedup the `Vector8`/`Vector32` stubs here against `src/port/pg_lfind.rs`.

use crate::c::{int64, uint32, uint64, uint8, Size, UINT64CONST};

// ---------------------------------------------------------------------------
// Vector types
//
// In the portable USE_NO_SIMD build `Vector8` is a plain `uint64`. `Vector32`
// has no USE_NO_SIMD definition in C (the header notes it's not worthwhile to
// pack two 32-bit ints into a uint64); under SIMD it is the platform's 128-bit
// vector type (`__m128i` / `uint32x4_t`). We stub it as an opaque 128-bit
// register so the SIMD-only prototypes type-check. TODO: dedup.
// ---------------------------------------------------------------------------

/// `Vector8` from port/simd.h. USE_NO_SIMD fallback: `uint64`.
/// (USE_SSE2: `__m128i`; USE_NEON: `uint8x16_t`.)
pub type Vector8 = uint64;

/// `Vector32` from port/simd.h. Only defined under SIMD builds
/// (USE_SSE2: `__m128i`; USE_NEON: `uint32x4_t`). Opaque 128-bit stub here.
/// TODO: dedup against a real SIMD vector type once SIMD paths are ported.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Vector32 {
    _bits: [u8; 16],
}

// ---------------------------------------------------------------------------
// load/store operations
// ---------------------------------------------------------------------------

/// Load a chunk of memory into the given vector.
///
/// C `vector8_load`. USE_NO_SIMD path: `memcpy(v, s, sizeof(Vector8))`.
#[inline]
pub unsafe fn vector8_load(v: *mut Vector8, s: *const uint8) {
    // memcpy(v, s, sizeof(Vector8));
    core::ptr::copy_nonoverlapping(s, v as *mut uint8, core::mem::size_of::<Vector8>());
}

/// C `vector32_load`. SIMD-only (`#ifndef USE_NO_SIMD`); no portable form.
#[inline]
pub unsafe fn vector32_load(_v: *mut Vector32, _s: *const uint32) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// assignment operations
// ---------------------------------------------------------------------------

/// Create a vector with all elements set to the same value.
///
/// C `vector8_broadcast`. USE_NO_SIMD path: `~UINT64CONST(0) / 0xFF * c`.
#[inline]
pub fn vector8_broadcast(c: uint8) -> Vector8 {
    // return ~UINT64CONST(0) / 0xFF * c;
    (!UINT64CONST(0) / 0xFF).wrapping_mul(c as uint64)
}

/// C `vector32_broadcast`. SIMD-only; no portable form.
#[inline]
pub fn vector32_broadcast(_c: uint32) -> Vector32 {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// element-wise comparisons to a scalar
// ---------------------------------------------------------------------------

/// Return true if any elements in the vector are equal to the given scalar.
///
/// C `vector8_has`. USE_NO_SIMD path:
/// `vector8_has_zero(v ^ vector8_broadcast(c))`.
#[inline]
pub fn vector8_has(v: Vector8, c: uint8) -> bool {
    // any bytes in v equal to c will evaluate to zero via XOR
    vector8_has_zero(v ^ vector8_broadcast(c))
}

/// Convenience function equivalent to `vector8_has(v, 0)`.
///
/// C `vector8_has_zero`. USE_NO_SIMD path: `vector8_has_le(v, 0)`
/// (cannot call vector8_has here - that would be circular).
#[inline]
pub fn vector8_has_zero(v: Vector8) -> bool {
    vector8_has_le(v, 0)
}

/// Return true if any elements in the vector are less than or equal to the
/// given scalar.
///
/// C `vector8_has_le`. USE_NO_SIMD path: bit-twiddling fast path (valid when
/// `(int64) v >= 0 && c < 0x80`), else byte-at-a-time scan.
#[inline]
pub fn vector8_has_le(v: Vector8, c: uint8) -> bool {
    let mut result = false;

    // To find bytes <= c, we can use bitwise operations to find bytes < c+1,
    // but it only works if c+1 <= 128 and if the highest bit in v is not set.
    // Adapted from
    // https://graphics.stanford.edu/~seander/bithacks.html#HasLessInWord
    if (v as int64) >= 0 && c < 0x80 {
        result = ((v.wrapping_sub(vector8_broadcast(c + 1))) & !v & vector8_broadcast(0x80)) != 0;
    } else {
        // one byte at a time
        let bytes = v.to_ne_bytes();
        let mut i: Size = 0;
        while i < core::mem::size_of::<Vector8>() {
            if bytes[i] <= c {
                result = true;
                break;
            }
            i += 1;
        }
    }

    result
}

/// Return true if the high bit of any element is set.
///
/// C `vector8_is_highbit_set`. USE_NO_SIMD path: `v & vector8_broadcast(0x80)`.
#[inline]
pub fn vector8_is_highbit_set(v: Vector8) -> bool {
    (v & vector8_broadcast(0x80)) != 0
}

/// Exactly like `vector8_is_highbit_set` except for the input type, so it
/// looks at each byte separately.
///
/// C `vector32_is_highbit_set`. SIMD-only; no portable form.
#[inline]
pub fn vector32_is_highbit_set(_v: Vector32) -> bool {
    unimplemented!()
}

/// Return a bitmask formed from the high-bit of each element.
///
/// C `vector8_highbit_mask`. SIMD-only; no portable form.
#[inline]
pub fn vector8_highbit_mask(_v: Vector8) -> uint32 {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// arithmetic operations
// ---------------------------------------------------------------------------

/// Return the bitwise OR of the inputs.
///
/// C `vector8_or`. USE_NO_SIMD path: `v1 | v2`.
#[inline]
pub fn vector8_or(v1: Vector8, v2: Vector8) -> Vector8 {
    v1 | v2
}

/// C `vector32_or`. SIMD-only; no portable form.
#[inline]
pub fn vector32_or(_v1: Vector32, _v2: Vector32) -> Vector32 {
    unimplemented!()
}

/// Return the result of subtracting the respective elements of the input
/// vectors using saturation.
///
/// C `vector8_ssub`. SIMD-only; no portable form.
#[inline]
pub fn vector8_ssub(_v1: Vector8, _v2: Vector8) -> Vector8 {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// comparisons between vectors
//
// These return a vector rather than boolean, which is why there are no
// non-SIMD implementations.
// ---------------------------------------------------------------------------

/// Return a vector with all bits set in each lane where the corresponding
/// lanes in the inputs are equal.
///
/// C `vector8_eq`. SIMD-only; no portable form.
#[inline]
pub fn vector8_eq(_v1: Vector8, _v2: Vector8) -> Vector8 {
    unimplemented!()
}

/// Given two vectors, return a vector with the minimum element of each.
///
/// C `vector8_min`. SIMD-only; no portable form.
#[inline]
pub fn vector8_min(_v1: Vector8, _v2: Vector8) -> Vector8 {
    unimplemented!()
}

/// C `vector32_eq`. SIMD-only; no portable form.
#[inline]
pub fn vector32_eq(_v1: Vector32, _v2: Vector32) -> Vector32 {
    unimplemented!()
}

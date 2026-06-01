//! port/pg_lfind.h - optimized linear search routines using SIMD intrinsics where available.
//!
//! Portions Copyright (c) 2022-2025, PostgreSQL Global Development Group
//!
//! NOTE: This header `#include`s `port/simd.h`, which has NOT been ported to a
//! standalone Rust module yet (see comments in src/mb/wchar.rs and
//! src/utils/adt/json.rs about the Vector8/Vector32 machinery). We therefore
//! translate the portable scalar (`USE_NO_SIMD`) code paths faithfully and
//! provide the SIMD-only helpers (`pg_lfind32_simd_helper`) as stubs.
//!
//! The Vector8/Vector32 types and `vector*_*` intrinsics referenced by the
//! SIMD path are stubbed locally below so the prototypes type-check.
//! TODO: dedup these against a future port of port/simd.h.

use crate::prelude::*;

// ---------------------------------------------------------------------------
// Stubs for the port/simd.h types/intrinsics referenced by the SIMD path.
// port/simd.h is not yet ported; these mirror the C names so the SIMD-only
// helper prototype type-checks. TODO: dedup once port/simd.h is translated.
// ---------------------------------------------------------------------------

/// `Vector8` from port/simd.h. In the `USE_NO_SIMD` fallback this is `uint64`;
/// with SIMD it is a platform vector type. Stubbed as uint64.
pub type Vector8 = crate::c::uint64;

/// `Vector32` from port/simd.h. In the `USE_NO_SIMD` fallback this is `uint64`;
/// with SIMD it is a platform vector type. Stubbed as uint64.
pub type Vector32 = crate::c::uint64;

/*
 * pg_lfind8
 *
 * Return true if there is an element in 'base' that equals 'key', otherwise
 * return false.
 *
 * Translated from the scalar fallback semantics: the SIMD chunk loop is
 * equivalent in result to the one-by-one scan, so we implement the portable
 * version directly.
 */
#[inline]
pub unsafe fn pg_lfind8(key: uint8, base: *mut uint8, nelem: uint32) -> bool {
    // round down to multiple of vector length
    let tail_idx: uint32 = nelem & !((core::mem::size_of::<Vector8>() as uint32) - 1);
    let mut i: uint32 = 0;

    // for (i = 0; i < tail_idx; i += sizeof(Vector8))
    while i < tail_idx {
        // vector8_load(&chunk, &base[i]); if (vector8_has(chunk, key)) return true;
        // Scalar equivalent: scan each byte in this vector-sized chunk.
        let mut j: uint32 = 0;
        while j < core::mem::size_of::<Vector8>() as uint32 {
            if *base.add((i + j) as usize) == key {
                return true;
            }
            j += 1;
        }
        i += core::mem::size_of::<Vector8>() as uint32;
    }

    // Process the remaining elements one at a time.
    while i < nelem {
        if key == *base.add(i as usize) {
            return true;
        }
        i += 1;
    }

    false
}

/*
 * pg_lfind8_le
 *
 * Return true if there is an element in 'base' that is less than or equal to
 * 'key', otherwise return false.
 */
#[inline]
pub unsafe fn pg_lfind8_le(key: uint8, base: *mut uint8, nelem: uint32) -> bool {
    // round down to multiple of vector length
    let tail_idx: uint32 = nelem & !((core::mem::size_of::<Vector8>() as uint32) - 1);
    let mut i: uint32 = 0;

    // for (i = 0; i < tail_idx; i += sizeof(Vector8))
    while i < tail_idx {
        // vector8_load(&chunk, &base[i]); if (vector8_has_le(chunk, key)) return true;
        // Scalar equivalent: scan each byte in this vector-sized chunk.
        let mut j: uint32 = 0;
        while j < core::mem::size_of::<Vector8>() as uint32 {
            if *base.add((i + j) as usize) <= key {
                return true;
            }
            j += 1;
        }
        i += core::mem::size_of::<Vector8>() as uint32;
    }

    // Process the remaining elements one at a time.
    while i < nelem {
        if *base.add(i as usize) <= key {
            return true;
        }
        i += 1;
    }

    false
}

/*
 * pg_lfind32_one_by_one_helper
 *
 * Searches the array of integers one-by-one.  The caller is responsible for
 * ensuring that there are at least "nelem" integers in the array.
 */
#[inline]
pub unsafe fn pg_lfind32_one_by_one_helper(key: uint32, base: *const uint32, nelem: uint32) -> bool {
    let mut i: uint32 = 0;
    while i < nelem {
        if key == *base.add(i as usize) {
            return true;
        }
        i += 1;
    }

    false
}

/*
 * pg_lfind32_simd_helper
 *
 * Searches one 4-register-block of integers.  The caller is responsible for
 * ensuring that there are at least 4-registers-worth of integers remaining.
 *
 * This is the SIMD-only (`#ifndef USE_NO_SIMD`) helper. port/simd.h is not yet
 * ported, so this is stubbed. TODO: implement once Vector32 intrinsics exist.
 */
#[inline]
pub unsafe fn pg_lfind32_simd_helper(_keys: Vector32, _base: *const uint32) -> bool {
    unimplemented!()
}

/*
 * pg_lfind32
 *
 * Return true if there is an element in 'base' that equals 'key', otherwise
 * return false.
 *
 * port/simd.h is not yet ported, so we translate the `USE_NO_SIMD` branch,
 * which is a one-by-one linear search.
 */
#[inline]
pub unsafe fn pg_lfind32(key: uint32, base: *const uint32, nelem: uint32) -> bool {
    // #else (USE_NO_SIMD): Process the elements one at a time.
    pg_lfind32_one_by_one_helper(key, base, nelem)
}

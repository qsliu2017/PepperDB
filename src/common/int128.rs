//! common/int128.h - Roll-our-own 128-bit integer arithmetic.
//!
//! We make use of the native int128 type if there is one, otherwise implement
//! things the hard way based on two int64 halves.
//!
//! Rust has a native i128/u128 on all supported targets, so this translation
//! follows the USE_NATIVE_INT128 branch of the C header (INT128 == int128 ==
//! i128). The non-native struct-based fallback is therefore not emitted; the
//! semantics of every inline function are preserved using native arithmetic.

use crate::c::{int64, uint64};

/// For testing purposes, use of native int128 can be switched on/off by
/// predefining USE_NATIVE_INT128. We always use the native path.
pub const USE_NATIVE_INT128: c_int_truthy = 1;
// Helper alias only to mirror the C #define value; not used elsewhere.
#[allow(non_camel_case_types)]
type c_int_truthy = i32;

/// typedef int128 INT128;
pub type INT128 = i128;

/// Add an unsigned int64 value into an INT128 variable.
#[inline]
pub unsafe fn int128_add_uint64(i128: *mut INT128, v: uint64) {
    *i128 += v as INT128;
}

/// Add a signed int64 value into an INT128 variable.
#[inline]
pub unsafe fn int128_add_int64(i128: *mut INT128, v: int64) {
    *i128 += v as INT128;
}

/// Add the 128-bit product of two int64 values into an INT128 variable.
///
/// XXX with a stupid compiler, this could actually be less efficient than the
/// other implementation; maybe we should do it by hand always?
#[inline]
pub unsafe fn int128_add_int64_mul_int64(i128: *mut INT128, x: int64, y: int64) {
    *i128 += (x as INT128) * (y as INT128);
}

/// Compare two INT128 values, return -1, 0, or +1.
#[inline]
pub unsafe fn int128_compare(x: INT128, y: INT128) -> i32 {
    if x < y {
        return -1;
    }
    if x > y {
        return 1;
    }
    0
}

/// Widen int64 to INT128.
#[inline]
pub unsafe fn int64_to_int128(v: int64) -> INT128 {
    v as INT128
}

/// Convert INT128 to int64 (losing any high-order bits).
/// This also works fine for casting down to uint64.
#[inline]
pub unsafe fn int128_to_int64(val: INT128) -> int64 {
    val as int64
}

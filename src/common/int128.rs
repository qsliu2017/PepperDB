//! Translated from PostgreSQL src/include/common/int128.h
//! 128-bit integer arithmetic. Rust has a native i128, so we use it directly
//! (the USE_NATIVE_INT128 path); the two-int64-halves fallback is dropped.

/// A signed 128-bit accumulator.
pub type Int128 = i128;

/// Add an unsigned int64 value into an INT128.
pub fn int128_add_uint64(i128: &mut Int128, v: u64) {
    *i128 += i128::from(v);
}

/// Add a signed int64 value into an INT128.
pub fn int128_add_int64(i128: &mut Int128, v: i64) {
    *i128 += i128::from(v);
}

/// Add the 128-bit product of two int64 values into an INT128.
pub fn int128_add_int64_mul_int64(i128: &mut Int128, x: i64, y: i64) {
    *i128 += i128::from(x) * i128::from(y);
}

/// Compare two INT128 values: -1, 0, or +1.
pub fn int128_compare(x: Int128, y: Int128) -> i32 {
    use core::cmp::Ordering::{Less, Greater, Equal};
    match x.cmp(&y) {
        Less => -1,
        Greater => 1,
        Equal => 0,
    }
}

/// Widen int64 to INT128.
pub fn int64_to_int128(v: i64) -> Int128 {
    i128::from(v)
}

/// Convert INT128 to int64, dropping high-order bits.
pub fn int128_to_int64(val: Int128) -> i64 {
    val as i64
}

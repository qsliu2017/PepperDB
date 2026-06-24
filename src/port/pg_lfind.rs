//! Translated from PostgreSQL src/include/port/pg_lfind.h
//
// Optimized linear search. SIMD (port/simd.h) is dropped; these are the scalar
// one-by-one paths over slices, generalized where the C had fixed widths.

/// Return true if any element of `base` equals `key`. (C: pg_lfind8)
pub fn pg_lfind8(key: u8, base: &[u8]) -> bool {
    base.contains(&key)
}

/// Return true if any element of `base` is <= `key`. (C: pg_lfind8_le)
pub fn pg_lfind8_le(key: u8, base: &[u8]) -> bool {
    base.iter().any(|&b| b <= key)
}

/// Return true if any element of `base` equals `key`. (C: pg_lfind32)
pub fn pg_lfind32(key: u32, base: &[u32]) -> bool {
    base.contains(&key)
}

/// Generic linear membership search over any comparable slice.
pub fn pg_lfind<T: PartialEq>(key: &T, base: &[T]) -> bool {
    base.contains(key)
}

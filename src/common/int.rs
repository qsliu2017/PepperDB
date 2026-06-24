//! Translated from PostgreSQL src/include/common/int.h
//! Overflow-aware integer math and comparison routines, translated in full.
//! The C `bool pg_*_overflow(a, b, *result)` (true on overflow) maps to Rust's
//! checked arithmetic: `None` means overflow, `Some(v)` is the result.

// --- Signed overflow-checked arithmetic ---

pub fn pg_add_s16_overflow(a: i16, b: i16) -> Option<i16> {
    a.checked_add(b)
}
pub fn pg_sub_s16_overflow(a: i16, b: i16) -> Option<i16> {
    a.checked_sub(b)
}
pub fn pg_mul_s16_overflow(a: i16, b: i16) -> Option<i16> {
    a.checked_mul(b)
}
pub fn pg_neg_s16_overflow(a: i16) -> Option<i16> {
    a.checked_neg()
}
pub fn pg_abs_s16(a: i16) -> u16 {
    (a as i32).unsigned_abs() as u16
}

pub fn pg_add_s32_overflow(a: i32, b: i32) -> Option<i32> {
    a.checked_add(b)
}
pub fn pg_sub_s32_overflow(a: i32, b: i32) -> Option<i32> {
    a.checked_sub(b)
}
pub fn pg_mul_s32_overflow(a: i32, b: i32) -> Option<i32> {
    a.checked_mul(b)
}
pub fn pg_neg_s32_overflow(a: i32) -> Option<i32> {
    a.checked_neg()
}
pub fn pg_abs_s32(a: i32) -> u32 {
    (a as i64).unsigned_abs() as u32
}

pub fn pg_add_s64_overflow(a: i64, b: i64) -> Option<i64> {
    a.checked_add(b)
}
pub fn pg_sub_s64_overflow(a: i64, b: i64) -> Option<i64> {
    a.checked_sub(b)
}
pub fn pg_mul_s64_overflow(a: i64, b: i64) -> Option<i64> {
    a.checked_mul(b)
}
pub fn pg_neg_s64_overflow(a: i64) -> Option<i64> {
    a.checked_neg()
}
pub fn pg_abs_s64(a: i64) -> u64 {
    a.unsigned_abs()
}

// --- Unsigned overflow-checked arithmetic ---

pub fn pg_add_u16_overflow(a: u16, b: u16) -> Option<u16> {
    a.checked_add(b)
}
pub fn pg_sub_u16_overflow(a: u16, b: u16) -> Option<u16> {
    a.checked_sub(b)
}
pub fn pg_mul_u16_overflow(a: u16, b: u16) -> Option<u16> {
    a.checked_mul(b)
}
/// Negate an unsigned value into a signed result; None on overflow.
pub fn pg_neg_u16_overflow(a: u16) -> Option<i16> {
    let res = -(a as i32);
    i16::try_from(res).ok()
}

pub fn pg_add_u32_overflow(a: u32, b: u32) -> Option<u32> {
    a.checked_add(b)
}
pub fn pg_sub_u32_overflow(a: u32, b: u32) -> Option<u32> {
    a.checked_sub(b)
}
pub fn pg_mul_u32_overflow(a: u32, b: u32) -> Option<u32> {
    a.checked_mul(b)
}
pub fn pg_neg_u32_overflow(a: u32) -> Option<i32> {
    let res = -(a as i64);
    i32::try_from(res).ok()
}

pub fn pg_add_u64_overflow(a: u64, b: u64) -> Option<u64> {
    a.checked_add(b)
}
pub fn pg_sub_u64_overflow(a: u64, b: u64) -> Option<u64> {
    a.checked_sub(b)
}
pub fn pg_mul_u64_overflow(a: u64, b: u64) -> Option<u64> {
    a.checked_mul(b)
}
pub fn pg_neg_u64_overflow(a: u64) -> Option<i64> {
    if a > (i64::MAX as u64) + 1 {
        None
    } else if a == (i64::MAX as u64) + 1 {
        Some(i64::MIN)
    } else {
        Some(-(a as i64))
    }
}

// --- size_t ---

pub fn pg_add_size_overflow(a: usize, b: usize) -> Option<usize> {
    a.checked_add(b)
}
pub fn pg_sub_size_overflow(a: usize, b: usize) -> Option<usize> {
    a.checked_sub(b)
}
pub fn pg_mul_size_overflow(a: usize, b: usize) -> Option<usize> {
    a.checked_mul(b)
}
// pg_neg_size_overflow omitted upstream (no SSIZE_MIN/_MAX reasoning yet).

// --- Comparison routines (qsort-style: <0, 0, >0) ---

pub const fn pg_cmp_s16(a: i16, b: i16) -> i32 {
    a as i32 - b as i32
}
pub const fn pg_cmp_u16(a: u16, b: u16) -> i32 {
    a as i32 - b as i32
}
pub const fn pg_cmp_s32(a: i32, b: i32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}
pub const fn pg_cmp_u32(a: u32, b: u32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}
pub const fn pg_cmp_s64(a: i64, b: i64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}
pub const fn pg_cmp_u64(a: u64, b: u64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}
pub const fn pg_cmp_size(a: usize, b: usize) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

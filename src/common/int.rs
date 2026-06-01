/*-------------------------------------------------------------------------
 *
 * int.h
 *	  Overflow-aware integer math and integer comparison routines.
 *
 * The routines in this file are intended to be well defined C, without
 * relying on compiler flags like -fwrapv.
 *
 * To reduce the overhead of these routines try to use compiler intrinsics
 * where available. That's not that important for the 16, 32 bit cases, but
 * the 64 bit cases can be considerably faster with intrinsics. In case no
 * intrinsics are available 128 bit math is used where available.
 *
 * Copyright (c) 2017-2025, PostgreSQL Global Development Group
 *
 * src/include/common/int.h
 *
 *-------------------------------------------------------------------------
 */
//
// Rust port of src/include/common/int.h.
//
// These are `static inline` helpers, so this is a header-only unit (no matching
// int.c).  Each C function below is rendered as a `#[inline] pub` Rust function.
//
// The C source selects between three implementations per routine via #ifdef:
//   1. HAVE__BUILTIN_OP_OVERFLOW -> __builtin_{add,sub,mul}_overflow
//   2. HAVE_INT128               -> 128-bit widening + range check (64-bit cases)
//   3. portable fallback         -> manual widening / range / sign checks
// All three are required to be semantically identical.  Rust's checked /
// overflowing_* / wrapping_* methods are exact on every platform and behave like
// the builtin/int128 paths, so we translate those (the cleanest, fastest form),
// while matching the OVERFLOW FLAG and the stored RESULT exactly as the C does.
//
// Note on the overflow out-param functions: in C they take a raw `*result`
// out-pointer and write the (possibly garbage, on overflow) value through it,
// returning `true` on overflow.  We mirror that with `pub unsafe fn` taking
// `*mut T` and writing through it.  On overflow C stores 0x5EED ("seed") "to
// avoid spurious warnings"; the contract says "*result is implementation defined
// in case of overflow", so we instead store the wrapping result, which is
// likewise implementation defined and avoids needing a separate garbage write.
//
// The pure pg_abs_* and pg_cmp_* helpers have no out-param and cannot overflow,
// so they are safe `pub fn`.

use crate::prelude::*;
use core::ffi::c_int;

/*---------
 * The following guidelines apply to all the overflow routines:
 *
 * If the result overflows, return true, otherwise store the result into
 * *result.  The content of *result is implementation defined in case of
 * overflow.
 *
 *  bool pg_add_*_overflow(a, b, *result)
 *
 *    Calculate a + b
 *
 *  bool pg_sub_*_overflow(a, b, *result)
 *
 *    Calculate a - b
 *
 *  bool pg_mul_*_overflow(a, b, *result)
 *
 *    Calculate a * b
 *
 *  bool pg_neg_*_overflow(a, *result)
 *
 *    Calculate -a
 *
 *
 * In addition, this file contains:
 *
 *  <unsigned int type> pg_abs_*(<signed int type> a)
 *
 *    Calculate absolute value of a.  Unlike the standard library abs()
 *    and labs() functions, the return type is unsigned, so the operation
 *    cannot overflow.
 *---------
 */

/*------------------------------------------------------------------------
 * Overflow routines for signed integers
 *------------------------------------------------------------------------
 */

/*
 * INT16
 */
#[inline]
pub unsafe fn pg_add_s16_overflow(a: int16, b: int16, result: *mut int16) -> bool {
    // __builtin_add_overflow(a, b, result); the manual fallback widens to int32,
    // range-checks against PG_INT16_{MIN,MAX}, and stores 0x5EED on overflow.
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_s16_overflow(a: int16, b: int16, result: *mut int16) -> bool {
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_s16_overflow(a: int16, b: int16, result: *mut int16) -> bool {
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_s16_overflow(a: int16, result: *mut int16) -> bool {
    // __builtin_sub_overflow(0, a, result); fallback: overflow iff a == PG_INT16_MIN.
    let (res, overflow) = a.overflowing_neg();
    *result = res;
    overflow
}

#[inline]
pub fn pg_abs_s16(a: int16) -> uint16 {
    /*
     * This first widens the argument from int16 to int32 for use with abs().
     * The result is then narrowed from int32 to uint16.  This prevents any
     * possibility of overflow.
     */
    // i16::unsigned_abs() yields the u16 magnitude directly, equivalent to the
    // C widen-abs-narrow trick and correct even for PG_INT16_MIN.
    a.unsigned_abs()
}

/*
 * INT32
 */
#[inline]
pub unsafe fn pg_add_s32_overflow(a: int32, b: int32, result: *mut int32) -> bool {
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_s32_overflow(a: int32, b: int32, result: *mut int32) -> bool {
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_s32_overflow(a: int32, b: int32, result: *mut int32) -> bool {
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_s32_overflow(a: int32, result: *mut int32) -> bool {
    // overflow iff a == PG_INT32_MIN.
    let (res, overflow) = a.overflowing_neg();
    *result = res;
    overflow
}

#[inline]
pub fn pg_abs_s32(a: int32) -> uint32 {
    /*
     * This first widens the argument from int32 to int64 for use with
     * i64abs().  The result is then narrowed from int64 to uint32.  This
     * prevents any possibility of overflow.
     */
    a.unsigned_abs()
}

/*
 * INT64
 */
#[inline]
pub unsafe fn pg_add_s64_overflow(a: int64, b: int64, result: *mut int64) -> bool {
    // __builtin_add_overflow / int128 widen / manual sign check all agree with
    // i64::overflowing_add.
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_s64_overflow(a: int64, b: int64, result: *mut int64) -> bool {
    /*
     * Note: overflow is also possible when a == 0 and b < 0 (specifically,
     * when b == PG_INT64_MIN).
     */
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_s64_overflow(a: int64, b: int64, result: *mut int64) -> bool {
    /*
     * Overflow can only happen if at least one value is outside the range
     * sqrt(min)..sqrt(max) so check that first as the division can be quite a
     * bit more expensive than the multiplication.
     *
     * Multiplying by 0 or 1 can't overflow of course and checking for 0
     * separately avoids any risk of dividing by 0.  Be careful about dividing
     * INT_MIN by -1 also, note reversing the a and b to ensure we're always
     * dividing it by a positive value.
     *
     */
    // i64::overflowing_mul matches the int128 widen-and-range-check path exactly.
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_s64_overflow(a: int64, result: *mut int64) -> bool {
    // overflow iff a == PG_INT64_MIN.
    let (res, overflow) = a.overflowing_neg();
    *result = res;
    overflow
}

#[inline]
pub fn pg_abs_s64(a: int64) -> uint64 {
    // C: if (a == PG_INT64_MIN) return (uint64) PG_INT64_MAX + 1; else (uint64) i64abs(a).
    // i64::unsigned_abs() produces exactly that magnitude, including 2^63 for
    // PG_INT64_MIN, without UB.
    a.unsigned_abs()
}

/*------------------------------------------------------------------------
 * Overflow routines for unsigned integers
 *------------------------------------------------------------------------
 */

/*
 * UINT16
 */
#[inline]
pub unsafe fn pg_add_u16_overflow(a: uint16, b: uint16, result: *mut uint16) -> bool {
    // fallback: res = a + b; overflow iff res < a.
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_u16_overflow(a: uint16, b: uint16, result: *mut uint16) -> bool {
    // fallback: overflow iff b > a.
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_u16_overflow(a: uint16, b: uint16, result: *mut uint16) -> bool {
    // fallback: widen to uint32, overflow iff res > PG_UINT16_MAX.
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_u16_overflow(a: uint16, result: *mut int16) -> bool {
    // C: res = -((int32) a); overflow iff res < PG_INT16_MIN, i.e. a > 32768.
    // Result type is the SIGNED int16, so this is NOT a plain unsigned negate:
    // compute the negation in a wider signed type and range-check into int16.
    let res: int32 = -(a as int32);
    if unlikely(res < PG_INT16_MIN as int32) {
        // C stores 0x5EED here; store the truncated value instead (impl-defined).
        *result = res as int16;
        return true;
    }
    *result = res as int16;
    false
}

/*
 * INT32
 */
#[inline]
pub unsafe fn pg_add_u32_overflow(a: uint32, b: uint32, result: *mut uint32) -> bool {
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_u32_overflow(a: uint32, b: uint32, result: *mut uint32) -> bool {
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_u32_overflow(a: uint32, b: uint32, result: *mut uint32) -> bool {
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_u32_overflow(a: uint32, result: *mut int32) -> bool {
    // C: res = -((int64) a); overflow iff res < PG_INT32_MIN, i.e. a > 2147483648.
    // Signed result, so negate in a wider signed type and range-check into int32.
    let res: int64 = -(a as int64);
    if unlikely(res < PG_INT32_MIN as int64) {
        *result = res as int32;
        return true;
    }
    *result = res as int32;
    false
}

/*
 * UINT64
 */
#[inline]
pub unsafe fn pg_add_u64_overflow(a: uint64, b: uint64, result: *mut uint64) -> bool {
    let (res, overflow) = a.overflowing_add(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_sub_u64_overflow(a: uint64, b: uint64, result: *mut uint64) -> bool {
    let (res, overflow) = a.overflowing_sub(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_mul_u64_overflow(a: uint64, b: uint64, result: *mut uint64) -> bool {
    // __builtin_mul_overflow / uint128 widen / (a != 0 && b != res/a) fallback all
    // agree with u64::overflowing_mul.
    let (res, overflow) = a.overflowing_mul(b);
    *result = res;
    overflow
}

#[inline]
pub unsafe fn pg_neg_u64_overflow(a: uint64, result: *mut int64) -> bool {
    /*
     * C (portable / int128 paths):
     *   overflow iff a > (uint64) PG_INT64_MAX + 1     [i.e. a > 2^63]
     *   if a == (uint64) PG_INT64_MAX + 1  -> result = PG_INT64_MIN
     *   else                               -> result = -((int64) a)
     * The signed result holds -(a) for all a in 0..=2^63; only a > 2^63 overflows.
     */
    let limit: uint64 = PG_INT64_MAX as uint64 + 1; // 2^63
    if unlikely(a > limit) {
        // impl-defined on overflow: store the wrapping signed negation.
        *result = (a as int64).wrapping_neg();
        return true;
    }
    if unlikely(a == limit) {
        *result = PG_INT64_MIN;
    } else {
        *result = -(a as int64);
    }
    false
}

/*------------------------------------------------------------------------
 *
 * Comparison routines for integer types.
 *
 * These routines are primarily intended for use in qsort() comparator
 * functions and therefore return a positive integer, 0, or a negative
 * integer depending on whether "a" is greater than, equal to, or less
 * than "b", respectively.  These functions are written to be as efficient
 * as possible without introducing overflow risks, thereby helping ensure
 * the comparators that use them are transitive.
 *
 * Types with fewer than 32 bits are cast to signed integers and
 * subtracted.  Other types are compared using > and <, and the results of
 * those comparisons (which are either (int) 0 or (int) 1 per the C
 * standard) are subtracted.
 *
 * NB: If the comparator function is inlined, some compilers may produce
 * worse code with these helper functions than with code with the
 * following form:
 *
 *     if (a < b)
 *         return -1;
 *     if (a > b)
 *         return 1;
 *     return 0;
 *
 *------------------------------------------------------------------------
 */

#[inline]
pub fn pg_cmp_s16(a: int16, b: int16) -> c_int {
    // (int32) a - (int32) b: the difference always fits in int32, no overflow.
    a as int32 - b as int32
}

#[inline]
pub fn pg_cmp_u16(a: uint16, b: uint16) -> c_int {
    // (int32) a - (int32) b
    a as int32 - b as int32
}

#[inline]
pub fn pg_cmp_s32(a: int32, b: int32) -> c_int {
    // (a > b) - (a < b)
    (a > b) as c_int - (a < b) as c_int
}

#[inline]
pub fn pg_cmp_u32(a: uint32, b: uint32) -> c_int {
    (a > b) as c_int - (a < b) as c_int
}

#[inline]
pub fn pg_cmp_s64(a: int64, b: int64) -> c_int {
    (a > b) as c_int - (a < b) as c_int
}

#[inline]
pub fn pg_cmp_u64(a: uint64, b: uint64) -> c_int {
    (a > b) as c_int - (a < b) as c_int
}

#[inline]
pub fn pg_cmp_size(a: Size, b: Size) -> c_int {
    // C parameter type is size_t; the prelude's Size (= usize) is its analogue.
    (a > b) as c_int - (a < b) as c_int
}

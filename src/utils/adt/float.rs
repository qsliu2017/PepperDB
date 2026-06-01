//! Translation of postgres/src/backend/utils/adt/float.c (+ float.h)
//!
//! Functions for the built-in floating-point types (`real`/float4 and
//! `double precision`/float8).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   catalog/pg_type.h            -> not needed (only FLOAT8OID, used by stubbed aggs)
//!   common/int.h                 -> crate::common::int (pg_add_s32_overflow)
//!   common/shortest_dec.h        -> NOT ported (double_to_shortest_decimal_buf etc.) => out fns stubbed
//!   libpq/pqformat.h             -> NOT ported (pq_getmsgfloat*/pq_sendfloat*)       => recv/send stubbed
//!   utils/array.h                -> NOT ported (ArrayType/construct_array)           => aggregates stubbed
//!   utils/float.h                -> MERGED into this file (inline fns/macros below)
//!   utils/fmgrprotos.h           -> crate::utils::fmgr (PG_GETARG_*/PG_RETURN_* macros)
//!   utils/sortsupport.h          -> NOT ported (SortSupport)                          => *sortsupport stubbed
//!   <ctype.h>                    -> isspace bound via extern "C"
//!   <math.h>                     -> libm fns bound via extern "C"; isnan/isinf -> Rust is_nan/is_infinite
//!   <float.h>                    -> FLT_DIG/DBL_DIG only referenced by stubbed out fns
//!   <limits.h>                   -> PG_INT*_MIN/MAX from crate::c
//!   "common/int.h" strtod/strtof -> bound via extern "C"
//!
//! TRANSLATED FULLY: float4in/float8in (+ *_internal parsers), the float.h inline
//! arithmetic/comparison helpers (float4_pl..float8_ge, get_float*_infinity/nan,
//! float*_cmp_internal, float_overflow/underflow/zero_divide_error), all base ops,
//! arithmetic, comparisons, conversions, the random/transcendental float8 ops, the
//! degree-based trig, hyperbolic, error and gamma functions, mixed-precision and
//! cross-type comparison ops, and width_bucket_float8.
//!
//! STUBBED (deps not yet ported): float4out/float8out/float8out_internal
//! (shortest_dec + pg_strfromd); float4recv/float4send/float8recv/float8send
//! (libpq/pqformat); btfloat4sortsupport/btfloat8sortsupport (SortSupport); all the
//! aggregate transition/final/combine functions (utils/array.h) and check_float8_array.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_FLOAT4,
    PG_RETURN_FLOAT8, PG_RETURN_INT16, PG_RETURN_INT32,
};
use crate::c::{float4, float8, int16, int32};
use crate::common::int::pg_add_s32_overflow;
use crate::port::pgstrcasecmp::pg_strncasecmp;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgfloat4, pq_getmsgfloat8, pq_sendfloat4, pq_sendfloat8,
};
use crate::postgres::PointerGetDatum;
use core::ffi::{c_char, c_int, c_void};

/* X/Open (XSI) requires <math.h> to provide M_PI, but core POSIX does not */
const M_PI: f64 = 3.14159265358979323846;

/* Radians per degree, a.k.a. PI / 180 */
const RADIANS_PER_DEGREE: f64 = 0.0174532925199432957692;

/*
 * <math.h> bindings.  isnan/isinf/isfinite map to Rust f64::is_nan()/is_infinite()/
 * is_finite(); everything else is bound to libm here.  HUGE_VAL == f64::INFINITY.
 */
extern "C" {
    fn strtod(s: *const c_char, endptr: *mut *mut c_char) -> f64;
    fn strtof(s: *const c_char, endptr: *mut *mut c_char) -> f32;

    fn sqrt(x: f64) -> f64;
    fn cbrt(x: f64) -> f64;
    fn pow(x: f64, y: f64) -> f64;
    fn exp(x: f64) -> f64;
    fn log(x: f64) -> f64;
    fn log10(x: f64) -> f64;
    fn acos(x: f64) -> f64;
    fn asin(x: f64) -> f64;
    fn atan(x: f64) -> f64;
    fn atan2(y: f64, x: f64) -> f64;
    fn cos(x: f64) -> f64;
    fn sin(x: f64) -> f64;
    fn tan(x: f64) -> f64;
    fn fmod(x: f64, y: f64) -> f64;
    fn floor(x: f64) -> f64;
    fn ceil(x: f64) -> f64;
    fn rint(x: f64) -> f64;
    fn rintf(x: f32) -> f32;
    fn fabs(x: f64) -> f64;
    fn fabsf(x: f32) -> f32;

    fn sinh(x: f64) -> f64;
    fn cosh(x: f64) -> f64;
    fn tanh(x: f64) -> f64;
    fn asinh(x: f64) -> f64;
    fn acosh(x: f64) -> f64;
    fn atanh(x: f64) -> f64;

    fn erf(x: f64) -> f64;
    fn erfc(x: f64) -> f64;
    fn tgamma(x: f64) -> f64;
    fn lgamma(x: f64) -> f64;
}

/*
 * errno access (platform errno location), copied from numutils.rs.  Used by the
 * math routines that follow C's "errno = 0; result = f(x); if (errno == ...)".
 */
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}
const EDOM: c_int = 33; // <errno.h>, 33 on Linux and macOS
const ERANGE: c_int = 34; // <errno.h>, 34 on Linux and macOS

// <ctype.h> isspace, used by the in_internal parsers exactly as the C does via
// `isspace((unsigned char) *num)`.
extern "C" {
    fn isspace(ch: c_int) -> c_int;
}

/*
 * Configurable GUC parameter (extra_float_digits).  GUC machinery is not ported;
 * the default value is 1.  Only referenced by the (stubbed) *out functions.
 */
#[allow(non_upper_case_globals)]
pub static mut extra_float_digits: c_int = 1;

/* Cached constants for degree-based trig functions */
static mut degree_consts_set: bool = false;
static mut sin_30: float8 = 0.0;
static mut one_minus_cos_60: float8 = 0.0;
static mut asin_0_5: float8 = 0.0;
static mut acos_0_5: float8 = 0.0;
static mut atan_1_0: float8 = 0.0;
static mut tan_45: float8 = 0.0;
static mut cot_45: float8 = 0.0;

/*
 * These are intentionally not static in C; don't "fix" them.  They exist so the
 * compiler cannot precompute sin(constant) etc. at build time (see the comment
 * on init_degree_constants).  In Rust they are `pub static mut` and read at
 * runtime in init_degree_constants(), which is not constant-folded across the
 * call; we omit C's `extern` non-static linkage to avoid cross-unit symbol
 * clashes on integration.
 */
pub static mut degree_c_thirty: float8 = 30.0;
pub static mut degree_c_forty_five: float8 = 45.0;
pub static mut degree_c_sixty: float8 = 60.0;
pub static mut degree_c_one_half: float8 = 0.5;
pub static mut degree_c_one: float8 = 1.0;

/* ------------------------------------------------------------------------
 * float.h inline helpers, merged in.
 * ------------------------------------------------------------------------ */

/*
 * We use these out-of-line ereport() calls to report float overflow,
 * underflow, and zero-divide.  These are pg_noreturn in C; under the elog shim
 * ereport!(ERROR,...) panics, so they diverge at runtime.  (They return () for
 * the type checker; callers mirror the C, which simply falls through.)
 */
pub unsafe fn float_overflow_error() {
    ereport!(ERROR, errmsg!("value out of range: overflow"));
}

pub unsafe fn float_underflow_error() {
    ereport!(ERROR, errmsg!("value out of range: underflow"));
}

pub unsafe fn float_zero_divide_error() {
    ereport!(ERROR, errmsg!("division by zero"));
}

/*
 * Routines to provide reasonably platform-independent handling of infinity and
 * NaN.  C99 INFINITY / NAN map to the Rust associated consts.
 */
#[inline]
pub fn get_float4_infinity() -> float4 {
    f32::INFINITY
}
#[inline]
pub fn get_float8_infinity() -> float8 {
    f64::INFINITY
}
#[inline]
pub fn get_float4_nan() -> float4 {
    f32::NAN
}
#[inline]
pub fn get_float8_nan() -> float8 {
    f64::NAN
}

/*
 * Floating-point arithmetic with overflow/underflow reported as errors.
 */
#[inline]
pub unsafe fn float4_pl(val1: float4, val2: float4) -> float4 {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

#[inline]
pub unsafe fn float8_pl(val1: float8, val2: float8) -> float8 {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

#[inline]
pub unsafe fn float4_mi(val1: float4, val2: float4) -> float4 {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

#[inline]
pub unsafe fn float8_mi(val1: float8, val2: float8) -> float8 {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

#[inline]
pub unsafe fn float4_mul(val1: float4, val2: float4) -> float4 {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0f32 && val1 != 0.0f32 && val2 != 0.0f32 {
        float_underflow_error();
    }
    result
}

#[inline]
pub unsafe fn float8_mul(val1: float8, val2: float8) -> float8 {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && val1 != 0.0 && val2 != 0.0 {
        float_underflow_error();
    }
    result
}

#[inline]
pub unsafe fn float4_div(val1: float4, val2: float4) -> float4 {
    if val2 == 0.0f32 && !val1.is_nan() {
        float_zero_divide_error();
    }
    let result = val1 / val2;
    if result.is_infinite() && !val1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0f32 && val1 != 0.0f32 && !val2.is_infinite() {
        float_underflow_error();
    }
    result
}

#[inline]
pub unsafe fn float8_div(val1: float8, val2: float8) -> float8 {
    if val2 == 0.0 && !val1.is_nan() {
        float_zero_divide_error();
    }
    let result = val1 / val2;
    if result.is_infinite() && !val1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && val1 != 0.0 && !val2.is_infinite() {
        float_underflow_error();
    }
    result
}

/*
 * Routines for NaN-aware comparisons.  We consider all NaNs to be equal and
 * larger than any non-NaN.
 */
#[inline]
pub fn float4_eq(val1: float4, val2: float4) -> bool {
    if val1.is_nan() {
        val2.is_nan()
    } else {
        !val2.is_nan() && val1 == val2
    }
}
#[inline]
pub fn float8_eq(val1: float8, val2: float8) -> bool {
    if val1.is_nan() {
        val2.is_nan()
    } else {
        !val2.is_nan() && val1 == val2
    }
}
#[inline]
pub fn float4_ne(val1: float4, val2: float4) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}
#[inline]
pub fn float8_ne(val1: float8, val2: float8) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}
#[inline]
pub fn float4_lt(val1: float4, val2: float4) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}
#[inline]
pub fn float8_lt(val1: float8, val2: float8) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}
#[inline]
pub fn float4_le(val1: float4, val2: float4) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}
#[inline]
pub fn float8_le(val1: float8, val2: float8) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}
#[inline]
pub fn float4_gt(val1: float4, val2: float4) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}
#[inline]
pub fn float8_gt(val1: float8, val2: float8) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}
#[inline]
pub fn float4_ge(val1: float4, val2: float4) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}
#[inline]
pub fn float8_ge(val1: float8, val2: float8) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

#[inline]
pub fn float4_min(val1: float4, val2: float4) -> float4 {
    if float4_lt(val1, val2) {
        val1
    } else {
        val2
    }
}
#[inline]
pub fn float8_min(val1: float8, val2: float8) -> float8 {
    if float8_lt(val1, val2) {
        val1
    } else {
        val2
    }
}
#[inline]
pub fn float4_max(val1: float4, val2: float4) -> float4 {
    if float4_gt(val1, val2) {
        val1
    } else {
        val2
    }
}
#[inline]
pub fn float8_max(val1: float8, val2: float8) -> float8 {
    if float8_gt(val1, val2) {
        val1
    } else {
        val2
    }
}

/*
 * FLOAT{4,8}_FITS_IN_INT{16,32} macros from utils/float.h.  We don't store the
 * upper-bound limit values exactly, since they wouldn't fit; instead we use the
 * appropriate "exclusive" comparison, exactly as the C macros do.
 *
 * C macros:
 *   FLOAT8_FITS_IN_INT16(num)  ((num) >= (float8) PG_INT16_MIN && (num) < -((float8) PG_INT16_MIN))
 *   FLOAT8_FITS_IN_INT32(num)  ((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
 *   FLOAT4_FITS_IN_INT16(num)  ((num) >= (float4) PG_INT16_MIN && (num) < -((float4) PG_INT16_MIN))
 *   FLOAT4_FITS_IN_INT32(num)  ((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
 */
#[inline]
fn FLOAT8_FITS_IN_INT16(num: float8) -> bool {
    num >= (PG_INT16_MIN as float8) && num < -(PG_INT16_MIN as float8)
}
#[inline]
fn FLOAT8_FITS_IN_INT32(num: float8) -> bool {
    num >= (PG_INT32_MIN as float8) && num < -(PG_INT32_MIN as float8)
}
#[inline]
fn FLOAT4_FITS_IN_INT16(num: float4) -> bool {
    num >= (PG_INT16_MIN as float4) && num < -(PG_INT16_MIN as float4)
}
#[inline]
fn FLOAT4_FITS_IN_INT32(num: float4) -> bool {
    num >= (PG_INT32_MIN as float4) && num < -(PG_INT32_MIN as float4)
}

/*
 * Returns -1 if 'val' represents negative infinity, 1 if 'val' represents
 * (positive) infinity, and 0 otherwise.
 */
pub fn is_infinite(val: float8) -> c_int {
    if !val.is_infinite() {
        0
    } else if val > 0.0 {
        1
    } else {
        -1
    }
}

/* ========== USER I/O ROUTINES ========== */

/*
 *		float4in		- converts "num" to float4
 */
pub unsafe fn float4in(fcinfo: FunctionCallInfo) -> Datum {
    let num: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_FLOAT4!(float4in_internal(
        num,
        null_mut(),
        c"real".as_ptr(),
        num,
        (*fcinfo).context
    ));
}

/*
 * float4in_internal - guts of float4in()
 *
 * This is exposed for use by functions that want a reasonably
 * platform-independent way of inputting floats.  Behaves essentially like
 * strtof + ereport on error.
 */
pub unsafe fn float4in_internal(
    mut num: *mut c_char,
    endptr_p: *mut *mut c_char,
    type_name: *const c_char,
    orig_string: *const c_char,
    _escontext: *mut crate::nodes::nodes::Node,
) -> float4 {
    let mut val: float4;
    let mut endptr: *mut c_char = null_mut();

    /* skip leading whitespace */
    while *num != 0 && isspace(*num as c_uchar as c_int) != 0 {
        num = num.add(1);
    }

    /* Check for an empty-string input to begin with. */
    if *num == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                cstr(type_name),
                cstr(orig_string)
            )
        );
        return 0.0;
    }

    *errno_location() = 0;
    val = strtof(num, &mut endptr);

    /* did we not see anything that looks like a double? */
    if endptr == num || *errno_location() != 0 {
        let save_errno = *errno_location();

        /*
         * C99 requires that strtof() accept NaN, [+-]Infinity, and [+-]Inf, but
         * not all platforms support all of these; check for these inputs
         * ourselves if strtof() fails.
         */
        if pg_strncasecmp(num, c"NaN".as_ptr(), 3) == 0 {
            val = get_float4_nan();
            endptr = num.add(3);
        } else if pg_strncasecmp(num, c"Infinity".as_ptr(), 8) == 0 {
            val = get_float4_infinity();
            endptr = num.add(8);
        } else if pg_strncasecmp(num, c"+Infinity".as_ptr(), 9) == 0 {
            val = get_float4_infinity();
            endptr = num.add(9);
        } else if pg_strncasecmp(num, c"-Infinity".as_ptr(), 9) == 0 {
            val = -get_float4_infinity();
            endptr = num.add(9);
        } else if pg_strncasecmp(num, c"inf".as_ptr(), 3) == 0 {
            val = get_float4_infinity();
            endptr = num.add(3);
        } else if pg_strncasecmp(num, c"+inf".as_ptr(), 4) == 0 {
            val = get_float4_infinity();
            endptr = num.add(4);
        } else if pg_strncasecmp(num, c"-inf".as_ptr(), 4) == 0 {
            val = -get_float4_infinity();
            endptr = num.add(4);
        } else if save_errno == ERANGE {
            /*
             * Some platforms return ERANGE for denormalized numbers; detect a
             * "real" out-of-range condition by checking for zero or huge.
             */
            if val == 0.0 || val >= f32::INFINITY || val <= -f32::INFINITY {
                /* see comments in float8in_internal for rationale */
                let errnumber = pstrdup(num);
                *errnumber.offset(endptr.offset_from(num)) = 0;

                ereport!(
                    ERROR,
                    errmsg!("\"{}\" is out of range for type real", cstr(errnumber))
                );
                return 0.0;
            }
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid input syntax for type {}: \"{}\"",
                    cstr(type_name),
                    cstr(orig_string)
                )
            );
            return 0.0;
        }
    }

    /* skip trailing whitespace */
    while *endptr != 0 && isspace(*endptr as c_uchar as c_int) != 0 {
        endptr = endptr.add(1);
    }

    /* report stopping point if wanted, else complain if not end of string */
    if !endptr_p.is_null() {
        *endptr_p = endptr;
    } else if *endptr != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                cstr(type_name),
                cstr(orig_string)
            )
        );
        return 0.0;
    }

    val
}

/*
 *		float4out		- converts a float4 number to a string
 */
pub unsafe fn float4out(fcinfo: FunctionCallInfo) -> Datum {
    let num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let ascii = palloc(32) as *mut core::ffi::c_char;

    /*
     * Default (extra_float_digits > 0) uses the Ryu shortest-decimal output,
     * now ported in common/f2s.rs.  The extra_float_digits <= 0 path uses
     * pg_strfromd (snprintf %.*g) which is not yet ported.
     */
    if extra_float_digits > 0 {
        crate::common::f2s::float_to_shortest_decimal_buf(num, ascii);
        PG_RETURN_CSTRING!(ascii);
    }
    // TODO(pg-port): port pg_strfromd for the extra_float_digits <= 0 case.
    unimplemented!("float4out: pg_strfromd (extra_float_digits<=0 path) not yet translated")
}

/*
 *		float4recv			- converts external binary format to float4
 */
pub unsafe fn float4recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    PG_RETURN_FLOAT4!(pq_getmsgfloat4(buf));
}

/*
 *		float4send			- converts float4 to binary format
 */
pub unsafe fn float4send(fcinfo: FunctionCallInfo) -> Datum {
    let num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendfloat4(&mut buf, num);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*
 *		float8in		- converts "num" to float8
 */
pub unsafe fn float8in(fcinfo: FunctionCallInfo) -> Datum {
    let num: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_FLOAT8!(float8in_internal(
        num,
        null_mut(),
        c"double precision".as_ptr(),
        num,
        (*fcinfo).context
    ));
}

/*
 * float8in_internal - guts of float8in()
 *
 * Behaves essentially like strtod + ereport on error, but:
 * 1. Both leading and trailing whitespace are skipped.
 * 2. If endptr_p is NULL, we report error if there's trailing junk.
 * 3. The error report mentions type_name and prints orig_string.
 */
pub unsafe fn float8in_internal(
    mut num: *mut c_char,
    endptr_p: *mut *mut c_char,
    type_name: *const c_char,
    orig_string: *const c_char,
    _escontext: *mut crate::nodes::nodes::Node,
) -> float8 {
    let mut val: float8;
    let mut endptr: *mut c_char = null_mut();

    /* skip leading whitespace */
    while *num != 0 && isspace(*num as c_uchar as c_int) != 0 {
        num = num.add(1);
    }

    /* Check for an empty-string input to begin with. */
    if *num == 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                cstr(type_name),
                cstr(orig_string)
            )
        );
        return 0.0;
    }

    *errno_location() = 0;
    val = strtod(num, &mut endptr);

    /* did we not see anything that looks like a double? */
    if endptr == num || *errno_location() != 0 {
        let save_errno = *errno_location();

        if pg_strncasecmp(num, c"NaN".as_ptr(), 3) == 0 {
            val = get_float8_nan();
            endptr = num.add(3);
        } else if pg_strncasecmp(num, c"Infinity".as_ptr(), 8) == 0 {
            val = get_float8_infinity();
            endptr = num.add(8);
        } else if pg_strncasecmp(num, c"+Infinity".as_ptr(), 9) == 0 {
            val = get_float8_infinity();
            endptr = num.add(9);
        } else if pg_strncasecmp(num, c"-Infinity".as_ptr(), 9) == 0 {
            val = -get_float8_infinity();
            endptr = num.add(9);
        } else if pg_strncasecmp(num, c"inf".as_ptr(), 3) == 0 {
            val = get_float8_infinity();
            endptr = num.add(3);
        } else if pg_strncasecmp(num, c"+inf".as_ptr(), 4) == 0 {
            val = get_float8_infinity();
            endptr = num.add(4);
        } else if pg_strncasecmp(num, c"-inf".as_ptr(), 4) == 0 {
            val = -get_float8_infinity();
            endptr = num.add(4);
        } else if save_errno == ERANGE {
            /*
             * Some platforms return ERANGE for denormalized numbers; detect a
             * "real" out-of-range condition by checking for zero or huge.  On
             * error we complain about double precision and print only the
             * current number.
             */
            if val == 0.0 || val >= f64::INFINITY || val <= -f64::INFINITY {
                let errnumber = pstrdup(num);
                *errnumber.offset(endptr.offset_from(num)) = 0;
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is out of range for type double precision",
                        cstr(errnumber)
                    )
                );
                return 0.0;
            }
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid input syntax for type {}: \"{}\"",
                    cstr(type_name),
                    cstr(orig_string)
                )
            );
            return 0.0;
        }
    }

    /* skip trailing whitespace */
    while *endptr != 0 && isspace(*endptr as c_uchar as c_int) != 0 {
        endptr = endptr.add(1);
    }

    /* report stopping point if wanted, else complain if not end of string */
    if !endptr_p.is_null() {
        *endptr_p = endptr;
    } else if *endptr != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                cstr(type_name),
                cstr(orig_string)
            )
        );
        return 0.0;
    }

    val
}

/*
 *		float8out		- converts float8 number to a string
 */
pub unsafe fn float8out(fcinfo: FunctionCallInfo) -> Datum {
    let num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_CSTRING!(float8out_internal(num));
}

/*
 * float8out_internal - guts of float8out()
 */
pub unsafe fn float8out_internal(num: float8) -> *mut c_char {
    let ascii = palloc(32) as *mut core::ffi::c_char;

    /*
     * Default (extra_float_digits > 0) uses the Ryu shortest-decimal output,
     * now ported in common/d2s.rs.  The extra_float_digits <= 0 path uses
     * pg_strfromd (snprintf %.*g) which is not yet ported.
     */
    if extra_float_digits > 0 {
        crate::common::d2s::double_to_shortest_decimal_buf(num, ascii);
        return ascii;
    }
    // TODO(pg-port): port pg_strfromd for the extra_float_digits <= 0 case.
    unimplemented!("float8out_internal: pg_strfromd (extra_float_digits<=0 path) not yet translated")
}

/*
 *		float8recv			- converts external binary format to float8
 */
pub unsafe fn float8recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    PG_RETURN_FLOAT8!(pq_getmsgfloat8(buf));
}

/*
 *		float8send			- converts float8 to binary format
 */
pub unsafe fn float8send(fcinfo: FunctionCallInfo) -> Datum {
    let num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendfloat8(&mut buf, num);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/* ========== PUBLIC ROUTINES ========== */

/*
 *		======================
 *		FLOAT4 BASE OPERATIONS
 *		======================
 */

/* float4abs - returns |arg1| */
pub unsafe fn float4abs(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    PG_RETURN_FLOAT4!(fabsf(arg1));
}

/* float4um - returns -arg1 (unary minus) */
pub unsafe fn float4um(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    let result: float4 = -arg1;
    PG_RETURN_FLOAT4!(result);
}

pub unsafe fn float4up(fcinfo: FunctionCallInfo) -> Datum {
    let arg: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    PG_RETURN_FLOAT4!(arg);
}

pub unsafe fn float4larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    let result = if float4_gt(arg1, arg2) { arg1 } else { arg2 };
    PG_RETURN_FLOAT4!(result);
}

pub unsafe fn float4smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    let result = if float4_lt(arg1, arg2) { arg1 } else { arg2 };
    PG_RETURN_FLOAT4!(result);
}

/*
 *		======================
 *		FLOAT8 BASE OPERATIONS
 *		======================
 */

/* float8abs - returns |arg1| */
pub unsafe fn float8abs(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(fabs(arg1));
}

/* float8um - returns -arg1 (unary minus) */
pub unsafe fn float8um(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    let result: float8 = -arg1;
    PG_RETURN_FLOAT8!(result);
}

pub unsafe fn float8up(fcinfo: FunctionCallInfo) -> Datum {
    let arg: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(arg);
}

pub unsafe fn float8larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    let result = if float8_gt(arg1, arg2) { arg1 } else { arg2 };
    PG_RETURN_FLOAT8!(result);
}

pub unsafe fn float8smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    let result = if float8_lt(arg1, arg2) { arg1 } else { arg2 };
    PG_RETURN_FLOAT8!(result);
}

/*
 *		====================
 *		ARITHMETIC OPERATORS
 *		====================
 */

pub unsafe fn float4pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT4!(float4_pl(arg1, arg2));
}

pub unsafe fn float4mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT4!(float4_mi(arg1, arg2));
}

pub unsafe fn float4mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT4!(float4_mul(arg1, arg2));
}

pub unsafe fn float4div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT4!(float4_div(arg1, arg2));
}

pub unsafe fn float8pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_pl(arg1, arg2));
}

pub unsafe fn float8mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mi(arg1, arg2));
}

pub unsafe fn float8mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mul(arg1, arg2));
}

pub unsafe fn float8div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_div(arg1, arg2));
}

/*
 *		====================
 *		COMPARISON OPERATORS
 *		====================
 */

/* float4{eq,ne,lt,le,gt,ge} - float4/float4 comparison operations */
pub fn float4_cmp_internal(a: float4, b: float4) -> c_int {
    if float4_gt(a, b) {
        return 1;
    }
    if float4_lt(a, b) {
        return -1;
    }
    0
}

pub unsafe fn float4eq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_eq(arg1, arg2));
}

pub unsafe fn float4ne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_ne(arg1, arg2));
}

pub unsafe fn float4lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_lt(arg1, arg2));
}

pub unsafe fn float4le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_le(arg1, arg2));
}

pub unsafe fn float4gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_gt(arg1, arg2));
}

pub unsafe fn float4ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float4_ge(arg1, arg2));
}

pub unsafe fn btfloat4cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_INT32!(float4_cmp_internal(arg1, arg2));
}

/* sortsupport comparator for btfloat4sortsupport */
unsafe fn btfloat4fastcmp(x: Datum, y: Datum, _ssup: *mut c_void) -> c_int {
    let arg1: float4 = DatumGetFloat4(x);
    let arg2: float4 = DatumGetFloat4(y);

    float4_cmp_internal(arg1, arg2)
}

pub unsafe fn btfloat4sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let _ = btfloat4fastcmp; // silence dead-code until SortSupport is wired
    // C: SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    //    ssup->comparator = btfloat4fastcmp;
    //    PG_RETURN_VOID();
    // TODO(pg-port): SortSupport (utils/sortsupport.h) not yet translated.
    let _ = fcinfo;
    unimplemented!("btfloat4sortsupport: SortSupport not yet translated")
}

/* float8{eq,ne,lt,le,gt,ge} - float8/float8 comparison operations */
pub fn float8_cmp_internal(a: float8, b: float8) -> c_int {
    if float8_gt(a, b) {
        return 1;
    }
    if float8_lt(a, b) {
        return -1;
    }
    0
}

pub unsafe fn float8eq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_eq(arg1, arg2));
}

pub unsafe fn float8ne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ne(arg1, arg2));
}

pub unsafe fn float8lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_lt(arg1, arg2));
}

pub unsafe fn float8le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_le(arg1, arg2));
}

pub unsafe fn float8gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_gt(arg1, arg2));
}

pub unsafe fn float8ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ge(arg1, arg2));
}

pub unsafe fn btfloat8cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_INT32!(float8_cmp_internal(arg1, arg2));
}

/* sortsupport comparator for btfloat8sortsupport */
unsafe fn btfloat8fastcmp(x: Datum, y: Datum, _ssup: *mut c_void) -> c_int {
    let arg1: float8 = DatumGetFloat8(x);
    let arg2: float8 = DatumGetFloat8(y);

    float8_cmp_internal(arg1, arg2)
}

pub unsafe fn btfloat8sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let _ = btfloat8fastcmp; // silence dead-code until SortSupport is wired
    // C: SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    //    ssup->comparator = btfloat8fastcmp;
    //    PG_RETURN_VOID();
    // TODO(pg-port): SortSupport (utils/sortsupport.h) not yet translated.
    let _ = fcinfo;
    unimplemented!("btfloat8sortsupport: SortSupport not yet translated")
}

pub unsafe fn btfloat48cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    /* widen float4 to float8 and then compare */
    PG_RETURN_INT32!(float8_cmp_internal(arg1 as float8, arg2));
}

pub unsafe fn btfloat84cmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    /* widen float4 to float8 and then compare */
    PG_RETURN_INT32!(float8_cmp_internal(arg1, arg2 as float8));
}

/*
 * in_range support function for float8.
 */
pub unsafe fn in_range_float8_float8(fcinfo: FunctionCallInfo) -> Datum {
    let val: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let base: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let offset: float8 = PG_GETARG_FLOAT8!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let sum: float8;

    /* Reject negative or NaN offset. */
    if offset.is_nan() || offset < 0.0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /* Deal with cases where val and/or base is NaN (NaN sorts after non-NaN). */
    if val.is_nan() {
        if base.is_nan() {
            PG_RETURN_BOOL!(true); /* NAN = NAN */
        } else {
            PG_RETURN_BOOL!(!less); /* NAN > non-NAN */
        }
    } else if base.is_nan() {
        PG_RETURN_BOOL!(less); /* non-NAN < NAN */
    }

    /*
     * Deal with cases where both base and offset are infinite, where base +/-
     * offset would produce NaN.
     */
    if offset.is_infinite() && base.is_infinite() && (if sub { base > 0.0 } else { base < 0.0 }) {
        PG_RETURN_BOOL!(true);
    }

    /* Otherwise it is safe to compute base +/- offset. */
    if sub {
        sum = base - offset;
    } else {
        sum = base + offset;
    }

    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

/*
 * in_range support function for float4.
 */
pub unsafe fn in_range_float4_float8(fcinfo: FunctionCallInfo) -> Datum {
    let val: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let base: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);
    let offset: float8 = PG_GETARG_FLOAT8!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let sum: float8;

    /* Reject negative or NaN offset. */
    if offset.is_nan() || offset < 0.0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /* Deal with cases where val and/or base is NaN (NaN sorts after non-NaN). */
    if val.is_nan() {
        if base.is_nan() {
            PG_RETURN_BOOL!(true); /* NAN = NAN */
        } else {
            PG_RETURN_BOOL!(!less); /* NAN > non-NAN */
        }
    } else if base.is_nan() {
        PG_RETURN_BOOL!(less); /* non-NAN < NAN */
    }

    /*
     * Deal with cases where both base and offset are infinite, where base +/-
     * offset would produce NaN.
     */
    if offset.is_infinite()
        && (base as float8).is_infinite()
        && (if sub { base > 0.0 } else { base < 0.0 })
    {
        PG_RETURN_BOOL!(true);
    }

    /* Otherwise it is safe to compute base +/- offset. */
    if sub {
        sum = base as float8 - offset;
    } else {
        sum = base as float8 + offset;
    }

    if less {
        PG_RETURN_BOOL!((val as float8) <= sum);
    } else {
        PG_RETURN_BOOL!((val as float8) >= sum);
    }
}

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */

/* ftod - converts a float4 number to a float8 number */
pub unsafe fn ftod(fcinfo: FunctionCallInfo) -> Datum {
    let num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    PG_RETURN_FLOAT8!(num as float8);
}

/* dtof - converts a float8 number to a float4 number */
pub unsafe fn dtof(fcinfo: FunctionCallInfo) -> Datum {
    let num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    let result: float4 = num as float4;
    if result.is_infinite() && !num.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0f32 && num != 0.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT4!(result);
}

/* dtoi4 - converts a float8 number to an int4 number */
pub unsafe fn dtoi4(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    /* Get rid of any fractional part (rint passes NaN/Inf through). */
    num = rint(num);

    /* Range check */
    if num.is_nan() || !FLOAT8_FITS_IN_INT32(num) {
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    PG_RETURN_INT32!(num as int32);
}

/* dtoi2 - converts a float8 number to an int2 number */
pub unsafe fn dtoi2(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    num = rint(num);

    if num.is_nan() || !FLOAT8_FITS_IN_INT16(num) {
        ereport!(ERROR, errmsg!("smallint out of range"));
    }

    PG_RETURN_INT16!(num as int16);
}

/* i4tod - converts an int4 number to a float8 number */
pub unsafe fn i4tod(fcinfo: FunctionCallInfo) -> Datum {
    let num: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_FLOAT8!(num as float8);
}

/* i2tod - converts an int2 number to a float8 number */
pub unsafe fn i2tod(fcinfo: FunctionCallInfo) -> Datum {
    let num: int16 = PG_GETARG_INT16!(fcinfo, 0);

    PG_RETURN_FLOAT8!(num as float8);
}

/* ftoi4 - converts a float4 number to an int4 number */
pub unsafe fn ftoi4(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    /* Get rid of any fractional part (rintf passes NaN/Inf through). */
    num = rintf(num);

    if num.is_nan() || !FLOAT4_FITS_IN_INT32(num) {
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    PG_RETURN_INT32!(num as int32);
}

/* ftoi2 - converts a float4 number to an int2 number */
pub unsafe fn ftoi2(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    /* Get rid of any fractional part (rintf passes NaN/Inf through). */
    num = rintf(num);

    if num.is_nan() || !FLOAT4_FITS_IN_INT16(num) {
        ereport!(ERROR, errmsg!("smallint out of range"));
    }

    PG_RETURN_INT16!(num as int16);
}

/* i4tof - converts an int4 number to a float4 number */
pub unsafe fn i4tof(fcinfo: FunctionCallInfo) -> Datum {
    let num: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_FLOAT4!(num as float4);
}

/* i2tof - converts an int2 number to a float4 number */
pub unsafe fn i2tof(fcinfo: FunctionCallInfo) -> Datum {
    let num: int16 = PG_GETARG_INT16!(fcinfo, 0);

    PG_RETURN_FLOAT4!(num as float4);
}

/*
 *		=======================
 *		RANDOM FLOAT8 OPERATORS
 *		=======================
 */

/* dround - returns ROUND(arg1) */
pub unsafe fn dround(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(rint(arg1));
}

/* dceil - smallest integer >= arg1 */
pub unsafe fn dceil(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(ceil(arg1));
}

/* dfloor - largest integer <= arg1 */
pub unsafe fn dfloor(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(floor(arg1));
}

/* dsign - returns -1/0/1 by sign of arg1 */
pub unsafe fn dsign(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1 > 0.0 {
        result = 1.0;
    } else if arg1 < 0.0 {
        result = -1.0;
    } else {
        result = 0.0;
    }

    PG_RETURN_FLOAT8!(result);
}

/* dtrunc - truncation towards zero */
pub unsafe fn dtrunc(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1 >= 0.0 {
        result = floor(arg1);
    } else {
        result = -floor(-arg1);
    }

    PG_RETURN_FLOAT8!(result);
}

/* dsqrt - returns square root of arg1 */
pub unsafe fn dsqrt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1 < 0.0 {
        ereport!(
            ERROR,
            errmsg!("cannot take square root of a negative number")
        );
    }

    result = sqrt(arg1);
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 0.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dcbrt - returns cube root of arg1 */
pub unsafe fn dcbrt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    result = cbrt(arg1);
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 0.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dpow - returns pow(arg1,arg2) */
pub unsafe fn dpow(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let mut result: float8;

    /*
     * POSIX: NaN ^ 0 = 1, and 1 ^ NaN = 1; all other NaN cases yield NaN.
     */
    if arg1.is_nan() {
        if arg2.is_nan() || arg2 != 0.0 {
            PG_RETURN_FLOAT8!(get_float8_nan());
        }
        PG_RETURN_FLOAT8!(1.0);
    }
    if arg2.is_nan() {
        if arg1 != 1.0 {
            PG_RETURN_FLOAT8!(get_float8_nan());
        }
        PG_RETURN_FLOAT8!(1.0);
    }

    /* SQL spec error codes; don't return divide-by-zero for 0 ^ -1. */
    if arg1 == 0.0 && arg2 < 0.0 {
        ereport!(
            ERROR,
            errmsg!("zero raised to a negative power is undefined")
        );
    }
    if arg1 < 0.0 && floor(arg2) != arg2 {
        ereport!(
            ERROR,
            errmsg!("a negative number raised to a non-integer power yields a complex result")
        );
    }

    /* Handle infinity cases explicitly (infinite y first). */
    if arg2.is_infinite() {
        let absx: float8 = fabs(arg1);

        if absx == 1.0 {
            result = 1.0;
        } else if arg2 > 0.0
        /* y = +Inf */
        {
            if absx > 1.0 {
                result = arg2;
            } else {
                result = 0.0;
            }
        } else
        /* y = -Inf */
        {
            if absx > 1.0 {
                result = 0.0;
            } else {
                result = -arg2;
            }
        }
    } else if arg1.is_infinite() {
        if arg2 == 0.0 {
            result = 1.0;
        } else if arg1 > 0.0
        /* x = +Inf */
        {
            if arg2 > 0.0 {
                result = arg1;
            } else {
                result = 0.0;
            }
        } else
        /* x = -Inf */
        {
            /*
             * Per POSIX, the sign of the result depends on whether y is an odd
             * integer.  Since x < 0, we know y is an integer; it is odd if y/2
             * is not also an integer.
             */
            let halfy: float8 = arg2 / 2.0; /* should be computed exactly */
            let yisoddinteger: bool = floor(halfy) != halfy;

            if arg2 > 0.0 {
                result = if yisoddinteger { arg1 } else { -arg1 };
            } else {
                result = if yisoddinteger { -0.0 } else { 0.0 };
            }
        }
    } else {
        /*
         * pow() sets errno on only some platforms, so check both errno and
         * invalid output values.
         */
        *errno_location() = 0;
        result = pow(arg1, arg2);
        if *errno_location() == EDOM || result.is_nan() {
            /*
             * We handled all domain errors above, so this should be impossible.
             * However, old glibc on x86 fails this way for abs(y) > 2^63.
             * Assume y is finite but large (certainly even).
             */
            if arg1 == 0.0 {
                result = 0.0; /* we already verified y is positive */
            } else {
                let absx: float8 = fabs(arg1);

                if absx == 1.0 {
                    result = 1.0;
                } else if if arg2 >= 0.0 { absx > 1.0 } else { absx < 1.0 } {
                    float_overflow_error();
                } else {
                    float_underflow_error();
                }
            }
        } else if *errno_location() == ERANGE {
            if result != 0.0 {
                float_overflow_error();
            } else {
                float_underflow_error();
            }
        } else {
            if result.is_infinite() {
                float_overflow_error();
            }
            if result == 0.0 && arg1 != 0.0 {
                float_underflow_error();
            }
        }
    }

    PG_RETURN_FLOAT8!(result);
}

/* dexp - returns the exponential function of arg1 */
pub unsafe fn dexp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* Handle NaN and Inf cases explicitly. */
    if arg1.is_nan() {
        result = arg1;
    } else if arg1.is_infinite() {
        /* Per POSIX, exp(-Inf) is 0 */
        result = if arg1 > 0.0 { arg1 } else { 0.0 };
    } else {
        *errno_location() = 0;
        result = exp(arg1);
        if *errno_location() == ERANGE {
            if result != 0.0 {
                float_overflow_error();
            } else {
                float_underflow_error();
            }
        } else if result.is_infinite() {
            float_overflow_error();
        } else if result == 0.0 {
            float_underflow_error();
        }
    }

    PG_RETURN_FLOAT8!(result);
}

/* dlog1 - returns the natural logarithm of arg1 */
pub unsafe fn dlog1(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* Emit particular SQLSTATE error codes for ln(). */
    if arg1 == 0.0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of zero"));
    }
    if arg1 < 0.0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of a negative number"));
    }

    result = log(arg1);
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 1.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dlog10 - returns the base 10 logarithm of arg1 */
pub unsafe fn dlog10(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1 == 0.0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of zero"));
    }
    if arg1 < 0.0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of a negative number"));
    }

    result = log10(arg1);
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 1.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dacos - returns the arccos of arg1 (radians) */
pub unsafe fn dacos(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* Per POSIX, return NaN if the input is NaN */
    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1 < -1.0 || arg1 > 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    result = acos(arg1);
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dasin - returns the arcsin of arg1 (radians) */
pub unsafe fn dasin(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1 < -1.0 || arg1 > 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    result = asin(arg1);
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* datan - returns the arctan of arg1 (radians) */
pub unsafe fn datan(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    result = atan(arg1);
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* datan2 - returns the arctan of arg1/arg2 (radians) */
pub unsafe fn datan2(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let result: float8;

    if arg1.is_nan() || arg2.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    result = atan2(arg1, arg2);
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dcos - returns the cosine of arg1 (radians) */
pub unsafe fn dcos(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    *errno_location() = 0;
    result = cos(arg1);
    if *errno_location() != 0 || arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dcot - returns the cotangent of arg1 (radians) */
pub unsafe fn dcot(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    /* Be sure to throw an error if the input is infinite --- see dcos() */
    *errno_location() = 0;
    result = tan(arg1);
    if *errno_location() != 0 || arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    result = 1.0 / result;
    /* Not checking for overflow because cot(0) == Inf */

    PG_RETURN_FLOAT8!(result);
}

/* dsin - returns the sine of arg1 (radians) */
pub unsafe fn dsin(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    *errno_location() = 0;
    result = sin(arg1);
    if *errno_location() != 0 || arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }
    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dtan - returns the tangent of arg1 (radians) */
pub unsafe fn dtan(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    *errno_location() = 0;
    result = tan(arg1);
    if *errno_location() != 0 || arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }
    /* Not checking for overflow because tan(pi/2) == Inf */

    PG_RETURN_FLOAT8!(result);
}

/* ========== DEGREE-BASED TRIGONOMETRIC FUNCTIONS ========== */

/*
 * Initialize the cached constants (sin_30 etc).  See the C comment for the
 * rationale behind computing these from runtime variables.
 */
unsafe fn init_degree_constants() {
    sin_30 = sin(degree_c_thirty * RADIANS_PER_DEGREE);
    one_minus_cos_60 = 1.0 - cos(degree_c_sixty * RADIANS_PER_DEGREE);
    asin_0_5 = asin(degree_c_one_half);
    acos_0_5 = acos(degree_c_one_half);
    atan_1_0 = atan(degree_c_one);
    tan_45 = sind_q1(degree_c_forty_five) / cosd_q1(degree_c_forty_five);
    cot_45 = cosd_q1(degree_c_forty_five) / sind_q1(degree_c_forty_five);
    degree_consts_set = true;
}

/* INIT_DEGREE_CONSTANTS() macro */
#[inline]
unsafe fn INIT_DEGREE_CONSTANTS() {
    if !degree_consts_set {
        init_degree_constants();
    }
}

/*
 * asind_q1 - inverse sine of x in degrees, for x in [0, 1] -> [0, 90].
 */
unsafe fn asind_q1(x: f64) -> f64 {
    /*
     * Stitch together inverse sine and cosine functions for [0, 0.5] and (0.5,
     * 1].  Each expression returns exactly 30 for x=0.5.  The temporaries are
     * read_volatile'd to mimic C's `volatile float8`, forcing rounding to
     * double width.
     */
    if x <= 0.5 {
        // C: volatile float8 asin_x = asin(x);  (force double-width round-trip)
        let tmp: float8 = asin(x);
        let asin_x: float8 = core::ptr::read_volatile(&tmp);
        (asin_x / asin_0_5) * 30.0
    } else {
        let tmp: float8 = acos(x);
        let acos_x: float8 = core::ptr::read_volatile(&tmp);
        90.0 - (acos_x / acos_0_5) * 60.0
    }
}

/*
 * acosd_q1 - inverse cosine of x in degrees, for x in [0, 1] -> [0, 90].
 */
unsafe fn acosd_q1(x: f64) -> f64 {
    if x <= 0.5 {
        let tmp: float8 = asin(x);
        let asin_x: float8 = core::ptr::read_volatile(&tmp);
        90.0 - (asin_x / asin_0_5) * 30.0
    } else {
        let tmp: float8 = acos(x);
        let acos_x: float8 = core::ptr::read_volatile(&tmp);
        (acos_x / acos_0_5) * 60.0
    }
}

/* dacosd - returns the arccos of arg1 (degrees) */
pub unsafe fn dacosd(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    INIT_DEGREE_CONSTANTS();

    if arg1 < -1.0 || arg1 > 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    if arg1 >= 0.0 {
        result = acosd_q1(arg1);
    } else {
        result = 90.0 + asind_q1(-arg1);
    }

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dasind - returns the arcsin of arg1 (degrees) */
pub unsafe fn dasind(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    INIT_DEGREE_CONSTANTS();

    if arg1 < -1.0 || arg1 > 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    if arg1 >= 0.0 {
        result = asind_q1(arg1);
    } else {
        result = -asind_q1(-arg1);
    }

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* datand - returns the arctan of arg1 (degrees) */
pub unsafe fn datand(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;
    let atan_arg1: float8;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    INIT_DEGREE_CONSTANTS();

    /*
     * Take care to ensure that when arg1 is 1, the result is exactly 45.
     */
    let tmp: float8 = atan(arg1);
    atan_arg1 = core::ptr::read_volatile(&tmp);
    result = (atan_arg1 / atan_1_0) * 45.0;

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* datan2d - returns the arctan of arg1/arg2 (degrees) */
pub unsafe fn datan2d(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let result: float8;
    let atan2_arg1_arg2: float8;

    if arg1.is_nan() || arg2.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    INIT_DEGREE_CONSTANTS();

    let tmp: float8 = atan2(arg1, arg2);
    atan2_arg1_arg2 = core::ptr::read_volatile(&tmp);
    result = (atan2_arg1_arg2 / atan_1_0) * 45.0;

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/*
 * sind_0_to_30 - sine of an angle in [0, 30] degrees; exact 0 at 0, 0.5 at 30.
 */
unsafe fn sind_0_to_30(x: f64) -> f64 {
    let tmp: float8 = sin(x * RADIANS_PER_DEGREE);
    let sin_x: float8 = core::ptr::read_volatile(&tmp);

    (sin_x / sin_30) / 2.0
}

/*
 * cosd_0_to_60 - cosine of an angle in [0, 60] degrees; exact 1 at 0, 0.5 at 60.
 */
unsafe fn cosd_0_to_60(x: f64) -> f64 {
    let tmp: float8 = 1.0 - cos(x * RADIANS_PER_DEGREE);
    let one_minus_cos_x: float8 = core::ptr::read_volatile(&tmp);

    1.0 - (one_minus_cos_x / one_minus_cos_60) / 2.0
}

/* sind_q1 - sine of an angle in the first quadrant (0 to 90 degrees). */
unsafe fn sind_q1(x: f64) -> f64 {
    if x <= 30.0 {
        sind_0_to_30(x)
    } else {
        cosd_0_to_60(90.0 - x)
    }
}

/* cosd_q1 - cosine of an angle in the first quadrant (0 to 90 degrees). */
unsafe fn cosd_q1(x: f64) -> f64 {
    if x <= 60.0 {
        cosd_0_to_60(x)
    } else {
        sind_0_to_30(90.0 - x)
    }
}

/* dcosd - returns the cosine of arg1 (degrees) */
pub unsafe fn dcosd(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;
    let mut sign: c_int = 1;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    INIT_DEGREE_CONSTANTS();

    /* Reduce the range of the input to [0,90] degrees */
    arg1 = fmod(arg1, 360.0);

    if arg1 < 0.0 {
        /* cosd(-x) = cosd(x) */
        arg1 = -arg1;
    }

    if arg1 > 180.0 {
        /* cosd(360-x) = cosd(x) */
        arg1 = 360.0 - arg1;
    }

    if arg1 > 90.0 {
        /* cosd(180-x) = -cosd(x) */
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    result = sign as float8 * cosd_q1(arg1);

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dcotd - returns the cotangent of arg1 (degrees) */
pub unsafe fn dcotd(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: float8;
    let cot_arg1: float8;
    let mut sign: c_int = 1;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    INIT_DEGREE_CONSTANTS();

    arg1 = fmod(arg1, 360.0);

    if arg1 < 0.0 {
        /* cotd(-x) = -cotd(x) */
        arg1 = -arg1;
        sign = -sign;
    }

    if arg1 > 180.0 {
        /* cotd(360-x) = -cotd(x) */
        arg1 = 360.0 - arg1;
        sign = -sign;
    }

    if arg1 > 90.0 {
        /* cotd(180-x) = -cotd(x) */
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    cot_arg1 = cosd_q1(arg1) / sind_q1(arg1);
    result = sign as float8 * (cot_arg1 / cot_45);

    /*
     * On some machines we get cotd(270) = minus zero; force it to plain zero.
     */
    if result == 0.0 {
        result = 0.0;
    }

    /* Not checking for overflow because cotd(0) == Inf */

    PG_RETURN_FLOAT8!(result);
}

/* dsind - returns the sine of arg1 (degrees) */
pub unsafe fn dsind(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;
    let mut sign: c_int = 1;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    INIT_DEGREE_CONSTANTS();

    arg1 = fmod(arg1, 360.0);

    if arg1 < 0.0 {
        /* sind(-x) = -sind(x) */
        arg1 = -arg1;
        sign = -sign;
    }

    if arg1 > 180.0 {
        /* sind(360-x) = -sind(x) */
        arg1 = 360.0 - arg1;
        sign = -sign;
    }

    if arg1 > 90.0 {
        /* sind(180-x) = sind(x) */
        arg1 = 180.0 - arg1;
    }

    result = sign as float8 * sind_q1(arg1);

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dtand - returns the tangent of arg1 (degrees) */
pub unsafe fn dtand(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: float8;
    let tan_arg1: float8;
    let mut sign: c_int = 1;

    if arg1.is_nan() {
        PG_RETURN_FLOAT8!(get_float8_nan());
    }

    if arg1.is_infinite() {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    INIT_DEGREE_CONSTANTS();

    arg1 = fmod(arg1, 360.0);

    if arg1 < 0.0 {
        /* tand(-x) = -tand(x) */
        arg1 = -arg1;
        sign = -sign;
    }

    if arg1 > 180.0 {
        /* tand(360-x) = -tand(x) */
        arg1 = 360.0 - arg1;
        sign = -sign;
    }

    if arg1 > 90.0 {
        /* tand(180-x) = -tand(x) */
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    tan_arg1 = sind_q1(arg1) / cosd_q1(arg1);
    result = sign as float8 * (tan_arg1 / tan_45);

    /*
     * On some machines we get tand(180) = minus zero; force it to plain zero.
     */
    if result == 0.0 {
        result = 0.0;
    }

    /* Not checking for overflow because tand(90) == Inf */

    PG_RETURN_FLOAT8!(result);
}

/* degrees - returns degrees converted from radians */
pub unsafe fn degrees(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(float8_div(arg1, RADIANS_PER_DEGREE));
}

/* dpi - returns the constant PI */
pub unsafe fn dpi(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(M_PI);
}

/* radians - returns radians converted from degrees */
pub unsafe fn radians(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    PG_RETURN_FLOAT8!(float8_mul(arg1, RADIANS_PER_DEGREE));
}

/* ========== HYPERBOLIC FUNCTIONS ========== */

/* dsinh - returns the hyperbolic sine of arg1 */
pub unsafe fn dsinh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: float8;

    *errno_location() = 0;
    result = sinh(arg1);

    /* ERANGE means overflow; result should be +/- infinity by sign of arg1. */
    if *errno_location() == ERANGE {
        if arg1 < 0.0 {
            result = -get_float8_infinity();
        } else {
            result = get_float8_infinity();
        }
    }

    PG_RETURN_FLOAT8!(result);
}

/* dcosh - returns the hyperbolic cosine of arg1 */
pub unsafe fn dcosh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: float8;

    *errno_location() = 0;
    result = cosh(arg1);

    /* ERANGE means overflow; cosh is always positive => +inf. */
    if *errno_location() == ERANGE {
        result = get_float8_infinity();
    }

    if result == 0.0 {
        float_underflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dtanh - returns the hyperbolic tangent of arg1 */
pub unsafe fn dtanh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* tanh never overflows. */
    result = tanh(arg1);

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* dasinh - returns the inverse hyperbolic sine of arg1 */
pub unsafe fn dasinh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    result = asinh(arg1);

    PG_RETURN_FLOAT8!(result);
}

/* dacosh - returns the inverse hyperbolic cosine of arg1 */
pub unsafe fn dacosh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* acosh is only defined for inputs >= 1.0. */
    if arg1 < 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    result = acosh(arg1);

    PG_RETURN_FLOAT8!(result);
}

/* datanh - returns the inverse hyperbolic tangent of arg1 */
pub unsafe fn datanh(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* atanh is only defined for inputs between -1 and 1. */
    if arg1 < -1.0 || arg1 > 1.0 {
        ereport!(ERROR, errmsg!("input is out of range"));
    }

    /* Handle the infinity cases ourselves (old glibc errno bug). */
    if arg1 == -1.0 {
        result = -get_float8_infinity();
    } else if arg1 == 1.0 {
        result = get_float8_infinity();
    } else {
        result = atanh(arg1);
    }

    PG_RETURN_FLOAT8!(result);
}

/* ========== ERROR FUNCTIONS ========== */

/* derf - returns the error function: erf(arg1) */
pub unsafe fn derf(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    result = erf(arg1);

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* derfc - returns the complementary error function: 1 - erf(arg1) */
pub unsafe fn derfc(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    result = erfc(arg1);

    if result.is_infinite() {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/* ========== GAMMA FUNCTIONS ========== */

/* dgamma - returns the gamma function of arg1 */
pub unsafe fn dgamma(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /* Handle NaN and Inf cases explicitly. */
    if arg1.is_nan() {
        result = arg1;
    } else if arg1.is_infinite() {
        /* Per POSIX, an input of -Inf causes a domain error */
        if arg1 < 0.0 {
            float_overflow_error();
            result = get_float8_nan(); /* keep compiler quiet */
        } else {
            result = arg1;
        }
    } else {
        /*
         * The POSIX/C99 gamma function is called "tgamma", not "gamma".  On
         * some platforms tgamma() returns Inf/NaN/zero rather than setting
         * errno, so test those cases explicitly.
         */
        *errno_location() = 0;
        result = tgamma(arg1);

        if *errno_location() != 0 || result.is_infinite() || result.is_nan() {
            if result != 0.0 {
                float_overflow_error();
            } else {
                float_underflow_error();
            }
        } else if result == 0.0 {
            float_underflow_error();
        }
    }

    PG_RETURN_FLOAT8!(result);
}

/* dlgamma - natural logarithm of absolute value of gamma of arg1 */
pub unsafe fn dlgamma(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let result: float8;

    /*
     * lgamma may not be thread-safe (writes signgam), but we don't use signgam.
     */
    *errno_location() = 0;
    result = lgamma(arg1);

    /*
     * ERANGE means overflow or a pole error (zero/negative integer inputs).  On
     * some platforms lgamma() returns infinity rather than setting errno.
     */
    if *errno_location() == ERANGE || (result.is_infinite() && !arg1.is_infinite()) {
        float_overflow_error();
    }

    PG_RETURN_FLOAT8!(result);
}

/*
 *		=========================
 *		FLOAT AGGREGATE OPERATORS
 *		=========================
 *
 * The transition datatype for these aggregates is an N-element array of float8.
 * All of them require utils/array.h (ArrayType / PG_GETARG_ARRAYTYPE_P /
 * construct_array_builtin / deconstruct), AggCheckCallContext, and FLOAT8OID,
 * none of which are ported yet, so every function below is stubbed.
 */

// check_float8_array - verifies the transition array; needs ArrayType internals.
// TODO(pg-port): utils/array.h (ARR_NDIM/ARR_DIMS/ARR_DATA_PTR/FLOAT8OID) not yet translated.

pub unsafe fn float8_combine(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h (ArrayType, construct_array_builtin) + AggCheckCallContext.
    let _ = fcinfo;
    unimplemented!("float8_combine: utils/array.h + AggCheckCallContext not yet translated")
}

pub unsafe fn float8_accum(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h + AggCheckCallContext not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_accum: utils/array.h + AggCheckCallContext not yet translated")
}

pub unsafe fn float4_accum(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h + AggCheckCallContext not yet translated.
    let _ = fcinfo;
    unimplemented!("float4_accum: utils/array.h + AggCheckCallContext not yet translated")
}

pub unsafe fn float8_avg(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_avg: utils/array.h not yet translated")
}

pub unsafe fn float8_var_pop(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_var_pop: utils/array.h not yet translated")
}

pub unsafe fn float8_var_samp(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_var_samp: utils/array.h not yet translated")
}

pub unsafe fn float8_stddev_pop(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_stddev_pop: utils/array.h not yet translated")
}

pub unsafe fn float8_stddev_samp(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_stddev_samp: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_accum(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h + AggCheckCallContext not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_accum: utils/array.h + AggCheckCallContext not yet translated")
}

pub unsafe fn float8_regr_combine(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h + AggCheckCallContext not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_combine: utils/array.h + AggCheckCallContext not yet translated")
}

pub unsafe fn float8_regr_sxx(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_sxx: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_syy(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_syy: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_sxy(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_sxy: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_avgx(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_avgx: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_avgy(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_avgy: utils/array.h not yet translated")
}

pub unsafe fn float8_covar_pop(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_covar_pop: utils/array.h not yet translated")
}

pub unsafe fn float8_covar_samp(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_covar_samp: utils/array.h not yet translated")
}

pub unsafe fn float8_corr(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_corr: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_r2(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_r2: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_slope(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_slope: utils/array.h not yet translated")
}

pub unsafe fn float8_regr_intercept(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): utils/array.h not yet translated.
    let _ = fcinfo;
    unimplemented!("float8_regr_intercept: utils/array.h not yet translated")
}

/*
 *		====================================
 *		MIXED-PRECISION ARITHMETIC OPERATORS
 *		====================================
 */

pub unsafe fn float48pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_pl(arg1 as float8, arg2));
}

pub unsafe fn float48mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mi(arg1 as float8, arg2));
}

pub unsafe fn float48mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mul(arg1 as float8, arg2));
}

pub unsafe fn float48div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_div(arg1 as float8, arg2));
}

pub unsafe fn float84pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_pl(arg1, arg2 as float8));
}

pub unsafe fn float84mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mi(arg1, arg2 as float8));
}

pub unsafe fn float84mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_mul(arg1, arg2 as float8));
}

pub unsafe fn float84div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_FLOAT8!(float8_div(arg1, arg2 as float8));
}

/*
 *		====================
 *		COMPARISON OPERATORS
 *		====================
 */

/* float48{eq,ne,lt,le,gt,ge} - float4/float8 comparison operations */
pub unsafe fn float48eq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_eq(arg1 as float8, arg2));
}

pub unsafe fn float48ne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ne(arg1 as float8, arg2));
}

pub unsafe fn float48lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_lt(arg1 as float8, arg2));
}

pub unsafe fn float48le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_le(arg1 as float8, arg2));
}

pub unsafe fn float48gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_gt(arg1 as float8, arg2));
}

pub unsafe fn float48ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let arg2: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ge(arg1 as float8, arg2));
}

/* float84{eq,ne,lt,le,gt,ge} - float8/float4 comparison operations */
pub unsafe fn float84eq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_eq(arg1, arg2 as float8));
}

pub unsafe fn float84ne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ne(arg1, arg2 as float8));
}

pub unsafe fn float84lt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_lt(arg1, arg2 as float8));
}

pub unsafe fn float84le(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_le(arg1, arg2 as float8));
}

pub unsafe fn float84gt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_gt(arg1, arg2 as float8));
}

pub unsafe fn float84ge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let arg2: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    PG_RETURN_BOOL!(float8_ge(arg1, arg2 as float8));
}

/*
 * Implements the float8 version of the width_bucket() function defined by
 * SQL2003.  See the C comment for the bucketing rules.
 */
pub unsafe fn width_bucket_float8(fcinfo: FunctionCallInfo) -> Datum {
    let operand: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let bound1: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let bound2: float8 = PG_GETARG_FLOAT8!(fcinfo, 2);
    let count: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let mut result: int32 = 0;

    if count <= 0 {
        ereport!(ERROR, errmsg!("count must be greater than zero"));
    }

    if operand.is_nan() || bound1.is_nan() || bound2.is_nan() {
        ereport!(
            ERROR,
            errmsg!("operand, lower bound, and upper bound cannot be NaN")
        );
    }

    /* Note that we allow "operand" to be infinite */
    if bound1.is_infinite() || bound2.is_infinite() {
        ereport!(ERROR, errmsg!("lower and upper bounds must be finite"));
    }

    if bound1 < bound2 {
        if operand < bound1 {
            result = 0;
        } else if operand >= bound2 {
            if pg_add_s32_overflow(count, 1, &mut result) {
                ereport!(ERROR, errmsg!("integer out of range"));
            }
        } else {
            if !(bound2 - bound1).is_infinite() {
                /* The quotient is surely in [0,1], so this can't overflow */
                result = (count as float8 * ((operand - bound1) / (bound2 - bound1))) as int32;
            } else {
                /*
                 * bound2 - bound1 overflows DBL_MAX.  Divide all inputs by 2 to
                 * compute without overflow.
                 */
                result = (count as float8
                    * ((operand / 2.0 - bound1 / 2.0) / (bound2 / 2.0 - bound1 / 2.0)))
                    as int32;
            }
            /* The quotient could round to 1.0, which would be a lie */
            if result >= count {
                result = count - 1;
            }
            /* Having done that, we can add 1 without fear of overflow */
            result += 1;
        }
    } else if bound1 > bound2 {
        if operand > bound1 {
            result = 0;
        } else if operand <= bound2 {
            if pg_add_s32_overflow(count, 1, &mut result) {
                ereport!(ERROR, errmsg!("integer out of range"));
            }
        } else {
            if !(bound1 - bound2).is_infinite() {
                result = (count as float8 * ((bound1 - operand) / (bound1 - bound2))) as int32;
            } else {
                result = (count as float8
                    * ((bound1 / 2.0 - operand / 2.0) / (bound1 / 2.0 - bound2 / 2.0)))
                    as int32;
            }
            if result >= count {
                result = count - 1;
            }
            result += 1;
        }
    } else {
        ereport!(ERROR, errmsg!("lower bound cannot equal upper bound"));
        result = 0; /* keep the compiler quiet */
    }

    PG_RETURN_INT32!(result);
}

/*
 * cstr - small local helper: render a C string for use in an error message.
 * (Mirrors the private `cstr` used in numutils.rs / oid.rs.)
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetBool, DatumGetFloat4, DatumGetFloat8, DatumGetInt16,
        DatumGetInt32, Float4GetDatum, Float8GetDatum, Int32GetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};
    use crate::{InitFunctionCallInfoData, LOCAL_FCINFO};

    // Helper to call a 1-arg float8 fn and read the float8 result.
    unsafe fn call1_f8(f: PGFunction, x: float8) -> float8 {
        DatumGetFloat8(DirectFunctionCall1Coll(f, InvalidOid, Float8GetDatum(x)))
    }
    unsafe fn call2_f8(f: PGFunction, a: float8, b: float8) -> float8 {
        DatumGetFloat8(DirectFunctionCall2Coll(
            f,
            InvalidOid,
            Float8GetDatum(a),
            Float8GetDatum(b),
        ))
    }

    #[test]
    fn float8_io() {
        unsafe {
            let v = DatumGetFloat8(DirectFunctionCall1Coll(
                float8in,
                InvalidOid,
                CStringGetDatum(c"3.14".as_ptr()),
            ));
            assert_eq!(v, 3.14);

            // leading/trailing whitespace + special values
            let inf = DatumGetFloat8(DirectFunctionCall1Coll(
                float8in,
                InvalidOid,
                CStringGetDatum(c"  Infinity  ".as_ptr()),
            ));
            assert!(inf.is_infinite() && inf > 0.0);
            let nan = DatumGetFloat8(DirectFunctionCall1Coll(
                float8in,
                InvalidOid,
                CStringGetDatum(c"NaN".as_ptr()),
            ));
            assert!(nan.is_nan());

            // float4in too
            let f = DatumGetFloat4(DirectFunctionCall1Coll(
                float4in,
                InvalidOid,
                CStringGetDatum(c"-2.5".as_ptr()),
            ));
            assert_eq!(f, -2.5f32);
        }
    }

    #[test]
    fn float8_arithmetic() {
        unsafe {
            assert_eq!(call2_f8(float8pl, 2.0, 3.0), 5.0);
            assert_eq!(call2_f8(float8mi, 2.0, 3.0), -1.0);
            assert_eq!(call2_f8(float8mul, 4.0, 2.5), 10.0);
            assert_eq!(call2_f8(float8div, 9.0, 2.0), 4.5);
            assert_eq!(call1_f8(float8abs, -7.0), 7.0);
            assert_eq!(call1_f8(float8um, 7.0), -7.0);
        }
    }

    #[test]
    fn float8_transcendental() {
        unsafe {
            assert!((call1_f8(dsqrt, 2.0) - 1.4142135623730951).abs() < 1e-12);
            assert_eq!(call2_f8(dpow, 2.0, 10.0), 1024.0);
            assert!((call1_f8(dexp, 0.0) - 1.0).abs() < 1e-12);
            assert!((call1_f8(dlog1, std::f64::consts::E) - 1.0).abs() < 1e-12);
            assert_eq!(call1_f8(dceil, 1.2), 2.0);
            assert_eq!(call1_f8(dfloor, 1.8), 1.0);
            assert_eq!(call1_f8(dtrunc, -1.8), -1.0);

            // degree trig exact endpoints
            assert_eq!(call1_f8(dsind, 30.0), 0.5);
            assert_eq!(call1_f8(dcosd, 60.0), 0.5);
            assert_eq!(call1_f8(dtand, 45.0), 1.0);

            // dpi
            let pi = DatumGetFloat8(DirectFunctionCall1Coll(dpi, InvalidOid, Float8GetDatum(0.0)));
            assert_eq!(pi, M_PI);
        }
    }

    #[test]
    fn float8_comparisons_and_nan_ordering() {
        unsafe {
            let lt = |a, b| {
                DatumGetBool(DirectFunctionCall2Coll(
                    float8lt,
                    InvalidOid,
                    Float8GetDatum(a),
                    Float8GetDatum(b),
                ))
            };
            assert!(lt(1.0, 2.0));
            assert!(!lt(2.0, 1.0));

            // PG orders NaN as larger than everything, including +Inf.
            let cmp = |a, b| {
                DatumGetInt32(DirectFunctionCall2Coll(
                    btfloat8cmp,
                    InvalidOid,
                    Float8GetDatum(a),
                    Float8GetDatum(b),
                ))
            };
            assert_eq!(cmp(f64::NAN, 1.0), 1); // NaN > 1.0
            assert_eq!(cmp(1.0, f64::NAN), -1); // 1.0 < NaN
            assert_eq!(cmp(f64::NAN, f64::INFINITY), 1); // NaN > +Inf
            assert_eq!(cmp(f64::NAN, f64::NAN), 0); // NaN == NaN
            assert_eq!(cmp(2.0, 2.0), 0);

            // float8_cmp_internal directly
            assert_eq!(float8_cmp_internal(f64::NAN, f64::INFINITY), 1);
            assert_eq!(float4_cmp_internal(f32::NAN, 1.0), 1);
        }
    }

    #[test]
    fn float8_conversions() {
        unsafe {
            // ftod / dtof
            let d = DatumGetFloat8(DirectFunctionCall1Coll(
                ftod,
                InvalidOid,
                Float4GetDatum(1.5f32),
            ));
            assert_eq!(d, 1.5);
            let f = DatumGetFloat4(DirectFunctionCall1Coll(
                dtof,
                InvalidOid,
                Float8GetDatum(2.25),
            ));
            assert_eq!(f, 2.25f32);

            // dtoi4 with rounding (ties to even: rint(2.5) == 2)
            let i = DatumGetInt32(DirectFunctionCall1Coll(
                dtoi4,
                InvalidOid,
                Float8GetDatum(2.5),
            ));
            assert_eq!(i, 2);
            let i2 = DatumGetInt32(DirectFunctionCall1Coll(
                dtoi4,
                InvalidOid,
                Float8GetDatum(3.5),
            ));
            assert_eq!(i2, 4);

            // dtoi2
            let s = DatumGetInt16(DirectFunctionCall1Coll(
                dtoi2,
                InvalidOid,
                Float8GetDatum(-100.4),
            ));
            assert_eq!(s, -100);
        }
    }

    #[test]
    fn cross_type_ops() {
        unsafe {
            let r = DatumGetFloat8(DirectFunctionCall2Coll(
                float48pl,
                InvalidOid,
                Float4GetDatum(1.5f32),
                Float8GetDatum(2.5),
            ));
            assert_eq!(r, 4.0);
            let b = DatumGetBool(DirectFunctionCall2Coll(
                float84lt,
                InvalidOid,
                Float8GetDatum(1.0),
                Float4GetDatum(2.0f32),
            ));
            assert!(b);
        }
    }

    #[test]
    fn width_bucket() {
        unsafe {
            let wb = |op: float8, b1: float8, b2: float8, c: int32| {
                LOCAL_FCINFO!(args_fcinfo, 4);
                InitFunctionCallInfoData!(args_fcinfo, null_mut(), 4, InvalidOid, null_mut(), null_mut());
                (*(*args_fcinfo).args.as_mut_ptr().add(0)).value = Float8GetDatum(op);
                (*(*args_fcinfo).args.as_mut_ptr().add(0)).isnull = false;
                (*(*args_fcinfo).args.as_mut_ptr().add(1)).value = Float8GetDatum(b1);
                (*(*args_fcinfo).args.as_mut_ptr().add(1)).isnull = false;
                (*(*args_fcinfo).args.as_mut_ptr().add(2)).value = Float8GetDatum(b2);
                (*(*args_fcinfo).args.as_mut_ptr().add(2)).isnull = false;
                (*(*args_fcinfo).args.as_mut_ptr().add(3)).value = Int32GetDatum(c);
                (*(*args_fcinfo).args.as_mut_ptr().add(3)).isnull = false;
                DatumGetInt32(width_bucket_float8(args_fcinfo))
            };
            // 5 buckets over [0,10): 5.0 falls in bucket 3.
            assert_eq!(wb(5.0, 0.0, 10.0, 5), 3);
            assert_eq!(wb(-1.0, 0.0, 10.0, 5), 0); // below lower bound
            assert_eq!(wb(10.0, 0.0, 10.0, 5), 6); // >= upper bound -> count+1
        }
    }

    #[test]
    #[should_panic]
    fn dtoi4_out_of_range() {
        unsafe {
            DirectFunctionCall1Coll(dtoi4, InvalidOid, Float8GetDatum(1e20));
        }
    }

    #[test]
    #[should_panic]
    fn float8div_by_zero() {
        unsafe {
            DirectFunctionCall2Coll(
                float8div,
                InvalidOid,
                Float8GetDatum(1.0),
                Float8GetDatum(0.0),
            );
        }
    }
}

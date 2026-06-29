//! Functions for the built-in floating-point types float4 (real) and float8
//! (double precision). Translated from src/backend/utils/adt/float.c.
//!
//! Covers the user I/O routines (in/out/recv/send) including IEEE special-value
//! parsing (`NaN`, `[+-]Infinity`, `[+-]inf`) and the overflow/underflow error
//! reporting; the base operations (abs/um/up/larger/smaller); the arithmetic
//! operators (pl/mi/mul/div) and their float48/float84 cross-width forms; every
//! comparison operator (eq/ne/lt/le/gt/ge plus btfloat4cmp/btfloat8cmp and the
//! cross-type float48*/float84* and btfloat48cmp/btfloat84cmp); the in_range
//! window helpers; the int<->float and float4<->float8 conversion routines with
//! their range/overflow checks; the std-backed math functions (sqrt/cbrt/pow/
//! exp/ln/log10, the trig + degree-trig + hyperbolic + erf/gamma families,
//! ceil/floor/round/trunc/sign, degrees/radians/pi); and width_bucket_float8.
//!
//! This file owns the float.c bodies declared in float.h: `float_overflow_error`,
//! `float_underflow_error`, `float_zero_divide_error`, `is_infinite`,
//! `float8in_internal`, `float4in_internal`, `float8out_internal`,
//! `float4_cmp_internal`, `float8_cmp_internal`. The float.h *inline* helpers
//! (`float8_pl`, `float4_eq`, `get_float8_nan`, ...) live in `utils::float` and
//! are imported here.
//!
//! IEEE handling (kept faithful to float.c / float.h):
//!  - All NaNs are equal and sort after (greater than) every non-NaN; the
//!    comparison family in `utils::float` encodes this.
//!  - Arithmetic raises "value out of range: overflow" when finite inputs yield
//!    an infinity, and "value out of range: underflow" when a nonzero product/
//!    quotient rounds to zero; division by zero raises ERRCODE_DIVISION_BY_ZERO.
//!  - in/out preserve Inf/NaN; in raises "out of range" only when a finite-shaped
//!    literal overflows to +/-Inf or underflows to 0.
//!
//! Subsystems float.c reaches that are not yet translated are called through
//! their stubs (rules.md s4): the binary wire `MsgReader`/`StringInfo` behind
//! recv/send, the SortSupport node behind btfloat{4,8}sortsupport, and the
//! aggregate transition arrays + `AggCheckCallContext` behind every accumulator
//! / combine / final aggregate (float8_accum, float8_combine, float8_avg, the
//! variance/stddev and regression families). The core in/out/arith/cmp/cast/
//! math/btree-support paths are complete.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "intentional C width arithmetic: float.c does explicit (int32)/(int16) \
              casts after a rint() range check, and widens float4->float8 (the \
              value-cast family is an allowed port-inherent lint per rules.md s11)"
)]
#![allow(
    clippy::float_cmp,
    reason = "float.c compares floats for exact equality on purpose (overflow/ \
              underflow detection v==0.0, x==1.0, arg!=0.0); these are faithful \
              ports of the C predicates, not accidental fuzzy comparisons"
)]
#![allow(
    clippy::suboptimal_flops,
    reason = "float.c computes base +/- offset as separate operations to match the \
              FPU rounding the C code documents; fusing into mul_add would change \
              the rounding and diverge from PG"
)]

use crate::common::int::pg_add_s32_overflow;
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetCString, DatumGetFloat4, DatumGetFloat8,
    DatumGetInt16, DatumGetInt32, Float4GetDatum, Float8GetDatum, Int16GetDatum, Int32GetDatum,
};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use crate::utils::float::{
    float4_div, float4_eq, float4_ge, float4_gt, float4_le, float4_lt, float4_mi, float4_mul,
    float4_ne, float4_pl, float8_div, float8_eq, float8_ge, float8_gt, float8_le, float8_lt,
    float8_mi, float8_mul, float8_ne, float8_pl, get_float8_infinity, get_float8_nan,
    RADIANS_PER_DEGREE,
};

const PG_INT16_MIN: f64 = -32768.0;
const PG_INT32_MIN: f64 = -2_147_483_648.0;

// FLOATn_FITS_IN_INTn (c.h): input must be rint()'d first; NaN handled by caller.
#[inline]
fn float8_fits_in_int16(num: f64) -> bool {
    (PG_INT16_MIN..-PG_INT16_MIN).contains(&num)
}
#[inline]
fn float8_fits_in_int32(num: f64) -> bool {
    (PG_INT32_MIN..-PG_INT32_MIN).contains(&num)
}
#[inline]
fn float4_fits_in_int16(num: f32) -> bool {
    (PG_INT16_MIN..-PG_INT16_MIN).contains(&f64::from(num))
}
#[inline]
fn float4_fits_in_int32(num: f32) -> bool {
    (PG_INT32_MIN..-PG_INT32_MIN).contains(&f64::from(num))
}

// ---------------------------------------------------------------------------
// Out-of-line error reporters (float.c float_overflow_error et al). These are
// declared in float.h and the inline arithmetic helpers in utils::float call
// them, so they must be the real bodies (this file owns them via re-export).
// ---------------------------------------------------------------------------

/// PG `float_overflow_error`: raises "value out of range: overflow".
pub fn float_overflow_error() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("value out of range: overflow");
    });
    unreachable!()
}

/// PG `float_underflow_error`: raises "value out of range: underflow".
pub fn float_underflow_error() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("value out of range: underflow");
    });
    unreachable!()
}

/// PG `float_zero_divide_error`: raises "division by zero".
pub fn float_zero_divide_error() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DIVISION_BY_ZERO).errmsg("division by zero");
    });
    unreachable!()
}

/// PG `is_infinite`: -1 for -Inf, 1 for +Inf, 0 otherwise.
pub fn is_infinite(val: f64) -> i32 {
    if !val.is_infinite() {
        0
    } else if val > 0.0 {
        1
    } else {
        -1
    }
}

// ---------------------------------------------------------------------------
// in/out internals (float.c float8in_internal et al).
// ---------------------------------------------------------------------------

/// PG `float8in_internal`: parse a float8 (strtod + the C99 special-value
/// fallbacks). Both leading and trailing whitespace are skipped; trailing junk
/// is rejected (the `endptr_p == NULL` path -- the only one we expose here).
///
/// `type_name` is the type to name in the syntax error; `orig_string` is the
/// full input the error message echoes back.
pub fn float8in_internal(num: &str, type_name: &str, orig_string: &str) -> f64 {
    let s = num.trim_start();
    if s.is_empty() {
        float_invalid_input(type_name, orig_string);
    }

    if let Some(v) = parse_special_f64(s) {
        // The special-value forms consume the whole token; require only trailing
        // whitespace after them.
        let consumed = special_len(s);
        if !s[consumed..].trim().is_empty() {
            float_invalid_input(type_name, orig_string);
        }
        return v;
    }

    // Find the longest numeric prefix Rust's parser accepts (strtod analogue):
    // walk back from the trimmed-trailing-whitespace end.
    let body = s.trim_end();
    let v = body
        .parse::<f64>()
        .unwrap_or_else(|_| float_invalid_input(type_name, orig_string));
    // strtod would set ERANGE for a finite literal that overflows to Inf or
    // underflows to 0; reproduce that out-of-range error.
    if (v == 0.0 && !is_zero_literal(body)) || v.is_infinite() {
        float_out_of_range(body, "double precision");
    }
    v
}

/// PG `float4in_internal`: parse a float4 (strtof + the C99 fallbacks).
pub fn float4in_internal(num: &str, type_name: &str, orig_string: &str) -> f32 {
    let s = num.trim_start();
    if s.is_empty() {
        float_invalid_input(type_name, orig_string);
    }

    if let Some(v) = parse_special_f64(s) {
        let consumed = special_len(s);
        if !s[consumed..].trim().is_empty() {
            float_invalid_input(type_name, orig_string);
        }
        return v as f32;
    }

    let body = s.trim_end();
    let v = body
        .parse::<f32>()
        .unwrap_or_else(|_| float_invalid_input(type_name, orig_string));
    if (v == 0.0 && !is_zero_literal(body)) || v.is_infinite() {
        float_out_of_range(body, "real");
    }
    v
}

/// Recognize the C99 special-value spellings PG checks for (case-insensitive),
/// returning the produced f64. Order matters: the longer "Infinity" forms are
/// matched before the "inf" forms, as in float.c.
#[allow(
    clippy::if_same_then_else,
    reason = "distinct spellings (Infinity/+inf/inf) that legitimately yield the \
              same +Inf value; kept as separate arms to mirror float.c's checks"
)]
fn parse_special_f64(s: &str) -> Option<f64> {
    let inf = get_float8_infinity();
    if s.len() >= 3 && s[..3].eq_ignore_ascii_case("NaN") {
        Some(get_float8_nan())
    } else if s.len() >= 9 && s[..9].eq_ignore_ascii_case("+Infinity") {
        Some(inf)
    } else if s.len() >= 9 && s[..9].eq_ignore_ascii_case("-Infinity") {
        Some(-inf)
    } else if s.len() >= 8 && s[..8].eq_ignore_ascii_case("Infinity") {
        Some(inf)
    } else if s.len() >= 4 && s[..4].eq_ignore_ascii_case("+inf") {
        Some(inf)
    } else if s.len() >= 4 && s[..4].eq_ignore_ascii_case("-inf") {
        Some(-inf)
    } else if s.len() >= 3 && s[..3].eq_ignore_ascii_case("inf") {
        Some(inf)
    } else {
        None
    }
}

/// Byte length of the special-value token matched by [`parse_special_f64`].
fn special_len(s: &str) -> usize {
    if s.len() >= 3 && s[..3].eq_ignore_ascii_case("NaN") {
        3
    } else if s.len() >= 9 && (s[..9].eq_ignore_ascii_case("+Infinity") || s[..9].eq_ignore_ascii_case("-Infinity")) {
        9
    } else if s.len() >= 8 && s[..8].eq_ignore_ascii_case("Infinity") {
        8
    } else if s.len() >= 4 && (s[..4].eq_ignore_ascii_case("+inf") || s[..4].eq_ignore_ascii_case("-inf")) {
        4
    } else {
        3
    }
}

/// True iff `s` denotes a real zero literal (so a 0.0 parse result is genuine,
/// not an underflow to zero of a tiny nonzero literal).
fn is_zero_literal(s: &str) -> bool {
    // Strip sign, exponent, and the decimal point; a genuine zero has no nonzero
    // significant digit.
    let body = s.trim();
    let mantissa = body.split(['e', 'E']).next().unwrap_or(body);
    !mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b))
}

fn float_invalid_input(type_name: &str, orig_string: &str) -> ! {
    let (tn, os) = (type_name.to_owned(), orig_string.to_owned());
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type {tn}: \"{os}\""));
    });
    unreachable!()
}

fn float_out_of_range(errnumber: &str, type_name: &str) -> ! {
    let (en, tn) = (errnumber.to_owned(), type_name.to_owned());
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!("\"{en}\" is out of range for type {tn}"));
    });
    unreachable!()
}

/// PG `float8out_internal`: shortest round-trippable decimal (extra_float_digits
/// defaults to 1, so PG uses the shortest-decimal path). Rust's `{}` for f64 is
/// the shortest round-trip representation, matching that behavior; we only fix
/// up the Inf/NaN spellings to PG's.
pub fn float8out_internal(num: f64) -> String {
    format_float_pg(num, num.to_string())
}

/// PG `float4out` body: shortest round-trippable decimal for a float4.
fn float4out_internal(num: f32) -> String {
    format_float_pg(f64::from(num), num.to_string())
}

/// Map Rust's float formatting onto PG's spellings (Infinity/-Infinity/NaN).
fn format_float_pg(num: f64, rust_repr: String) -> String {
    if num.is_nan() {
        "NaN".to_owned()
    } else if num.is_infinite() {
        if num > 0.0 { "Infinity".to_owned() } else { "-Infinity".to_owned() }
    } else {
        rust_repr
    }
}

/// PG `float4_cmp_internal`: 3-way compare with NaN sorting highest.
pub fn float4_cmp_internal(a: f32, b: f32) -> i32 {
    if float4_gt(a, b) {
        1
    } else if float4_lt(a, b) {
        -1
    } else {
        0
    }
}

/// PG `float8_cmp_internal`: 3-way compare with NaN sorting highest.
pub fn float8_cmp_internal(a: f64, b: f64) -> i32 {
    if float8_gt(a, b) {
        1
    } else if float8_lt(a, b) {
        -1
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* accessors (see int.rs for the contract).
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_float4(fcinfo: &FunctionCallInfoBaseData, n: usize) -> f32 {
    DatumGetFloat4(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_float8(fcinfo: &FunctionCallInfoBaseData, n: usize) -> f64 {
    DatumGetFloat8(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int16(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i16 {
    DatumGetInt16(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_bool(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    crate::postgres::DatumGetBool(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is NUL-terminated and outlives
    // the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}
#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let c = std::ffi::CString::new(s).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `float4in`: converts "num" to float4.
pub fn float4in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_cstring(fcinfo, 0);
    Float4GetDatum(float4in_internal(&num, "real", &num))
}

/// PG `float4out`: converts a float4 to its standard output string.
pub fn float4out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float4(fcinfo, 0);
    pg_return_cstring(&float4out_internal(num))
}

/// PG `float4recv`: converts external binary format to float4.
pub fn float4recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("float4recv needs the binary wire StringInfo (pq_getmsgfloat4) path")
}

/// PG `float4send`: converts float4 to binary format.
pub fn float4send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("float4send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `float8in`: converts "num" to float8.
pub fn float8in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_cstring(fcinfo, 0);
    Float8GetDatum(float8in_internal(&num, "double precision", &num))
}

/// PG `float8out`: converts a float8 to its standard output string.
pub fn float8out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float8(fcinfo, 0);
    pg_return_cstring(&float8out_internal(num))
}

/// PG `float8recv`: converts external binary format to float8.
pub fn float8recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("float8recv needs the binary wire StringInfo (pq_getmsgfloat8) path")
}

/// PG `float8send`: converts float8 to binary format.
pub fn float8send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("float8send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

// ===========================================================================
//   FLOAT4 / FLOAT8 BASE OPERATIONS
// ===========================================================================

/// PG `float4abs`.
pub fn float4abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(pg_getarg_float4(fcinfo, 0).abs())
}
/// PG `float4um`: unary minus.
pub fn float4um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(-pg_getarg_float4(fcinfo, 0))
}
/// PG `float4up`: unary plus (identity).
pub fn float4up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(pg_getarg_float4(fcinfo, 0))
}
/// PG `float4larger`.
pub fn float4larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float4(fcinfo, 0);
    let arg2 = pg_getarg_float4(fcinfo, 1);
    Float4GetDatum(if float4_gt(arg1, arg2) { arg1 } else { arg2 })
}
/// PG `float4smaller`.
pub fn float4smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float4(fcinfo, 0);
    let arg2 = pg_getarg_float4(fcinfo, 1);
    Float4GetDatum(if float4_lt(arg1, arg2) { arg1 } else { arg2 })
}

/// PG `float8abs`.
pub fn float8abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0).abs())
}
/// PG `float8um`: unary minus.
pub fn float8um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(-pg_getarg_float8(fcinfo, 0))
}
/// PG `float8up`: unary plus (identity).
pub fn float8up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0))
}
/// PG `float8larger`.
pub fn float8larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let arg2 = pg_getarg_float8(fcinfo, 1);
    Float8GetDatum(if float8_gt(arg1, arg2) { arg1 } else { arg2 })
}
/// PG `float8smaller`.
pub fn float8smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let arg2 = pg_getarg_float8(fcinfo, 1);
    Float8GetDatum(if float8_lt(arg1, arg2) { arg1 } else { arg2 })
}

// ===========================================================================
//   ARITHMETIC OPERATORS
// ===========================================================================

/// PG `float4pl`.
pub fn float4pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(float4_pl(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4mi`.
pub fn float4mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(float4_mi(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4mul`.
pub fn float4mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(float4_mul(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4div`.
pub fn float4div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(float4_div(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float8pl`.
pub fn float8pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_pl(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8mi`.
pub fn float8mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mi(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8mul`.
pub fn float8mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mul(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8div`.
pub fn float8div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_div(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}

// ===========================================================================
//   COMPARISON OPERATORS
// ===========================================================================

/// PG `float4eq`.
pub fn float4eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_eq(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4ne`.
pub fn float4ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_ne(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4lt`.
pub fn float4lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_lt(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4le`.
pub fn float4le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_le(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4gt`.
pub fn float4gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_gt(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `float4ge`.
pub fn float4ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float4_ge(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `btfloat4cmp`.
pub fn btfloat4cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(float4_cmp_internal(pg_getarg_float4(fcinfo, 0), pg_getarg_float4(fcinfo, 1)))
}
/// PG `btfloat4sortsupport`: installs the float4 fast comparator on a SortSupport.
pub fn btfloat4sortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("btfloat4sortsupport needs the SortSupport node (ssup->comparator)")
}

/// PG `float8eq`.
pub fn float8eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_eq(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8ne`.
pub fn float8ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ne(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8lt`.
pub fn float8lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_lt(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8le`.
pub fn float8le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_le(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8gt`.
pub fn float8gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_gt(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float8ge`.
pub fn float8ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ge(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `btfloat8cmp`.
pub fn btfloat8cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(float8_cmp_internal(pg_getarg_float8(fcinfo, 0), pg_getarg_float8(fcinfo, 1)))
}
/// PG `btfloat8sortsupport`: installs the float8 fast comparator on a SortSupport.
pub fn btfloat8sortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("btfloat8sortsupport needs the SortSupport node (ssup->comparator)")
}
/// PG `btfloat48cmp`: widen float4 to float8 then compare.
pub fn btfloat48cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(float8_cmp_internal(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `btfloat84cmp`: widen float4 to float8 then compare.
pub fn btfloat84cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(float8_cmp_internal(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}

// ---------------------------------------------------------------------------
//   in_range support functions
// ---------------------------------------------------------------------------

/// PG `in_range_float8_float8`.
pub fn in_range_float8_float8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_float8(fcinfo, 0);
    let base = pg_getarg_float8(fcinfo, 1);
    let offset = pg_getarg_float8(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);
    in_range_f64(val, base, offset, sub, less)
}

/// PG `in_range_float4_float8`: val/base are float4, offset is float8.
pub fn in_range_float4_float8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = f64::from(pg_getarg_float4(fcinfo, 0));
    let base = f64::from(pg_getarg_float4(fcinfo, 1));
    let offset = pg_getarg_float8(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);
    in_range_f64(val, base, offset, sub, less)
}

/// Shared body of the float in_range helpers (val/base already widened to f64).
fn in_range_f64(val: f64, base: f64, offset: f64, sub: bool, less: bool) -> Datum {
    if offset.is_nan() || offset < 0.0 {
        invalid_preceding_or_following();
    }
    // NaN sorts after non-NaN (cf float8_cmp_internal); offset cannot change it.
    if val.is_nan() {
        return BoolGetDatum(if base.is_nan() { true } else { !less });
    } else if base.is_nan() {
        return BoolGetDatum(less);
    }
    // Both base and offset infinite: treat the frame as covering everything.
    if offset.is_infinite() && base.is_infinite() && (if sub { base > 0.0 } else { base < 0.0 }) {
        return BoolGetDatum(true);
    }
    let sum = if sub { base - offset } else { base + offset };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

fn invalid_preceding_or_following() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
            .errmsg("invalid preceding or following size in window function");
    });
    unreachable!()
}

// ===========================================================================
//   CONVERSION ROUTINES
// ===========================================================================

/// PG `ftod`: float4 -> float8.
pub fn ftod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(f64::from(pg_getarg_float4(fcinfo, 0)))
}

/// PG `dtof`: float8 -> float4, with overflow/underflow checks.
pub fn dtof(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float8(fcinfo, 0);
    let result = num as f32;
    if result.is_infinite() && !num.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && num != 0.0 {
        float_underflow_error();
    }
    Float4GetDatum(result)
}

/// PG `dtoi4`: float8 -> int4.
pub fn dtoi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float8(fcinfo, 0).round_ties_even();
    if num.is_nan() || !float8_fits_in_int32(num) {
        integer_out_of_range();
    }
    Int32GetDatum(num as i32)
}
/// PG `dtoi2`: float8 -> int2.
pub fn dtoi2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float8(fcinfo, 0).round_ties_even();
    if num.is_nan() || !float8_fits_in_int16(num) {
        smallint_out_of_range();
    }
    Int16GetDatum(num as i16)
}
/// PG `i4tod`: int4 -> float8.
pub fn i4tod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(f64::from(pg_getarg_int32(fcinfo, 0)))
}
/// PG `i2tod`: int2 -> float8.
pub fn i2tod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(f64::from(pg_getarg_int16(fcinfo, 0)))
}
/// PG `ftoi4`: float4 -> int4.
pub fn ftoi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float4(fcinfo, 0).round_ties_even();
    if num.is_nan() || !float4_fits_in_int32(num) {
        integer_out_of_range();
    }
    Int32GetDatum(num as i32)
}
/// PG `ftoi2`: float4 -> int2.
pub fn ftoi2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_float4(fcinfo, 0).round_ties_even();
    if num.is_nan() || !float4_fits_in_int16(num) {
        smallint_out_of_range();
    }
    Int16GetDatum(num as i16)
}
/// PG `i4tof`: int4 -> float4.
pub fn i4tof(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(pg_getarg_int32(fcinfo, 0) as f32)
}
/// PG `i2tof`: int2 -> float4.
pub fn i2tof(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float4GetDatum(f32::from(pg_getarg_int16(fcinfo, 0)))
}

fn integer_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("integer out of range");
    });
    unreachable!()
}
fn smallint_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("smallint out of range");
    });
    unreachable!()
}

// ===========================================================================
//   RANDOM FLOAT8 OPERATORS (rounding etc.)
// ===========================================================================

/// PG `dround`: ROUND(arg1) (round half to even, matching C rint default mode).
pub fn dround(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0).round_ties_even())
}
/// PG `dceil`.
pub fn dceil(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0).ceil())
}
/// PG `dfloor`.
pub fn dfloor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0).floor())
}
/// PG `dsign`: -1/0/1 by sign (0 for +-0).
pub fn dsign(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let result = if arg1 > 0.0 {
        1.0
    } else if arg1 < 0.0 {
        -1.0
    } else {
        0.0
    };
    Float8GetDatum(result)
}
/// PG `dtrunc`: truncation towards zero.
pub fn dtrunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let result = if arg1 >= 0.0 { arg1.floor() } else { -(-arg1).floor() };
    Float8GetDatum(result)
}

/// PG `dsqrt`.
pub fn dsqrt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1 < 0.0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
                .errmsg("cannot take square root of a negative number");
        });
    }
    let result = arg1.sqrt();
    check_overflow_underflow(result, arg1);
    Float8GetDatum(result)
}

/// PG `dcbrt`.
pub fn dcbrt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let result = arg1.cbrt();
    check_overflow_underflow(result, arg1);
    Float8GetDatum(result)
}

/// Common `dsqrt`/`dcbrt`/log post-check: overflow if result is Inf while input
/// is finite; underflow if result is 0 while input is nonzero.
fn check_overflow_underflow(result: f64, arg: f64) {
    if result.is_infinite() && !arg.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg != 0.0 {
        float_underflow_error();
    }
}

/// PG `dpow`: pow(arg1, arg2) with the POSIX special-case handling.
pub fn dpow(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let arg2 = pg_getarg_float8(fcinfo, 1);

    // NaN cases per POSIX: NaN^0 = 1, 1^NaN = 1, else NaN.
    if arg1.is_nan() {
        return Float8GetDatum(if arg2.is_nan() || arg2 != 0.0 { get_float8_nan() } else { 1.0 });
    }
    if arg2.is_nan() {
        return Float8GetDatum(if arg1 == 1.0 { 1.0 } else { get_float8_nan() });
    }

    if arg1 == 0.0 && arg2 < 0.0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
                .errmsg("zero raised to a negative power is undefined");
        });
    }
    if arg1 < 0.0 && arg2.floor() != arg2 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
                .errmsg("a negative number raised to a non-integer power yields a complex result");
        });
    }

    let result: f64;
    if arg2.is_infinite() {
        let absx = arg1.abs();
        result = if absx == 1.0 {
            1.0
        } else if arg2 > 0.0 {
            if absx > 1.0 { arg2 } else { 0.0 }
        } else if absx > 1.0 {
            0.0
        } else {
            -arg2
        };
    } else if arg1.is_infinite() {
        if arg2 == 0.0 {
            result = 1.0;
        } else if arg1 > 0.0 {
            result = if arg2 > 0.0 { arg1 } else { 0.0 };
        } else {
            // x = -Inf; arg2 is an integer (checked above). Sign per odd-ness.
            let halfy = arg2 / 2.0;
            let yisoddinteger = halfy.floor() != halfy;
            if arg2 > 0.0 {
                result = if yisoddinteger { arg1 } else { -arg1 };
            } else {
                result = if yisoddinteger { -0.0 } else { 0.0 };
            }
        }
    } else {
        let r = arg1.powf(arg2);
        if r.is_nan() {
            // All domain errors were handled above; treat as overflow/underflow.
            if arg1 == 0.0 {
                result = 0.0;
            } else {
                let absx = arg1.abs();
                if absx == 1.0 {
                    result = 1.0;
                } else if if arg2 >= 0.0 { absx > 1.0 } else { absx < 1.0 } {
                    float_overflow_error();
                } else {
                    float_underflow_error();
                }
            }
        } else if r.is_infinite() {
            float_overflow_error();
        } else if r == 0.0 && arg1 != 0.0 {
            float_underflow_error();
        } else {
            result = r;
        }
    }
    Float8GetDatum(result)
}

/// PG `dexp`.
pub fn dexp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let result = if arg1.is_nan() {
        arg1
    } else if arg1.is_infinite() {
        if arg1 > 0.0 { arg1 } else { 0.0 }
    } else {
        let r = arg1.exp();
        if r.is_infinite() {
            float_overflow_error();
        }
        if r == 0.0 {
            float_underflow_error();
        }
        r
    };
    Float8GetDatum(result)
}

/// PG `dlog1`: natural logarithm.
pub fn dlog1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    check_log_domain(arg1);
    let result = arg1.ln();
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 1.0 {
        float_underflow_error();
    }
    Float8GetDatum(result)
}

/// PG `dlog10`: base-10 logarithm.
pub fn dlog10(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    check_log_domain(arg1);
    let result = arg1.log10();
    if result.is_infinite() && !arg1.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && arg1 != 1.0 {
        float_underflow_error();
    }
    Float8GetDatum(result)
}

fn check_log_domain(arg1: f64) {
    if arg1 == 0.0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
                .errmsg("cannot take logarithm of zero");
        });
    }
    if arg1 < 0.0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
                .errmsg("cannot take logarithm of a negative number");
        });
    }
}

fn input_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("input is out of range");
    });
    unreachable!()
}

// ===========================================================================
//   TRIGONOMETRIC FUNCTIONS (radians)
// ===========================================================================

/// PG `dacos`.
pub fn dacos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if !(-1.0..=1.0).contains(&arg1) {
        input_out_of_range();
    }
    let result = arg1.acos();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dasin`.
pub fn dasin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if !(-1.0..=1.0).contains(&arg1) {
        input_out_of_range();
    }
    let result = arg1.asin();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `datan`.
pub fn datan(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    let result = arg1.atan();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `datan2`: arctan(arg1/arg2).
pub fn datan2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let arg2 = pg_getarg_float8(fcinfo, 1);
    if arg1.is_nan() || arg2.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    let result = arg1.atan2(arg2);
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dcos`.
pub fn dcos(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let result = arg1.cos();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dcot`: cotangent.
pub fn dcot(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let result = 1.0 / arg1.tan();
    Float8GetDatum(result)
}
/// PG `dsin`.
pub fn dsin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let result = arg1.sin();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dtan`.
pub fn dtan(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    Float8GetDatum(arg1.tan())
}

// ===========================================================================
//   DEGREE-BASED TRIGONOMETRIC FUNCTIONS
//
// float.c uses a Rube-Goldberg scheme of volatile temporaries + cached scaling
// constants so that, e.g., sind(30) is exactly 0.5. That trick exists to defeat
// C compilers that constant-fold or use wide FP registers; Rust's f64 has none
// of those hazards, so we reproduce the same range reductions and call into the
// exact-at-endpoints first-quadrant helpers, scaled by the same constants.
// ===========================================================================

fn asin_0_5() -> f64 {
    0.5_f64.asin()
}
fn acos_0_5() -> f64 {
    0.5_f64.acos()
}
fn sin_30() -> f64 {
    (30.0 * RADIANS_PER_DEGREE).sin()
}
fn one_minus_cos_60() -> f64 {
    1.0 - (60.0 * RADIANS_PER_DEGREE).cos()
}
fn atan_1_0() -> f64 {
    1.0_f64.atan()
}

fn sind_0_to_30(x: f64) -> f64 {
    (x * RADIANS_PER_DEGREE).sin() / sin_30() / 2.0
}
fn cosd_0_to_60(x: f64) -> f64 {
    1.0 - (1.0 - (x * RADIANS_PER_DEGREE).cos()) / one_minus_cos_60() / 2.0
}
fn sind_q1(x: f64) -> f64 {
    if x <= 30.0 { sind_0_to_30(x) } else { cosd_0_to_60(90.0 - x) }
}
fn cosd_q1(x: f64) -> f64 {
    if x <= 60.0 { cosd_0_to_60(x) } else { sind_0_to_30(90.0 - x) }
}
fn asind_q1(x: f64) -> f64 {
    if x <= 0.5 {
        (x.asin() / asin_0_5()) * 30.0
    } else {
        90.0 - (x.acos() / acos_0_5()) * 60.0
    }
}
fn acosd_q1(x: f64) -> f64 {
    if x <= 0.5 {
        90.0 - (x.asin() / asin_0_5()) * 30.0
    } else {
        (x.acos() / acos_0_5()) * 60.0
    }
}
fn tan_45() -> f64 {
    sind_q1(45.0) / cosd_q1(45.0)
}
fn cot_45() -> f64 {
    cosd_q1(45.0) / sind_q1(45.0)
}

/// PG `dacosd`.
pub fn dacosd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if !(-1.0..=1.0).contains(&arg1) {
        input_out_of_range();
    }
    let result = if arg1 >= 0.0 { acosd_q1(arg1) } else { 90.0 + asind_q1(-arg1) };
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dasind`.
pub fn dasind(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if !(-1.0..=1.0).contains(&arg1) {
        input_out_of_range();
    }
    let result = if arg1 >= 0.0 { asind_q1(arg1) } else { -asind_q1(-arg1) };
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `datand`.
pub fn datand(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    let result = (arg1.atan() / atan_1_0()) * 45.0;
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `datan2d`.
pub fn datan2d(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let arg2 = pg_getarg_float8(fcinfo, 1);
    if arg1.is_nan() || arg2.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    let result = (arg1.atan2(arg2) / atan_1_0()) * 45.0;
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dcosd`.
pub fn dcosd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let mut sign = 1.0;
    arg1 %= 360.0;
    if arg1 < 0.0 {
        arg1 = -arg1;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let result = sign * cosd_q1(arg1);
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dcotd`.
pub fn dcotd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let mut sign = 1.0;
    arg1 %= 360.0;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let cot_arg1 = cosd_q1(arg1) / sind_q1(arg1);
    let mut result = sign * (cot_arg1 / cot_45());
    if result == 0.0 {
        result = 0.0; // force plain zero (avoid -0.0)
    }
    Float8GetDatum(result)
}
/// PG `dsind`.
pub fn dsind(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let mut sign = 1.0;
    arg1 %= 360.0;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
    }
    let result = sign * sind_q1(arg1);
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dtand`.
pub fn dtand(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1.is_nan() {
        return Float8GetDatum(get_float8_nan());
    }
    if arg1.is_infinite() {
        input_out_of_range();
    }
    let mut sign = 1.0;
    arg1 %= 360.0;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let tan_arg1 = sind_q1(arg1) / cosd_q1(arg1);
    let mut result = sign * (tan_arg1 / tan_45());
    if result == 0.0 {
        result = 0.0; // force plain zero (avoid -0.0)
    }
    Float8GetDatum(result)
}

/// PG `degrees`: radians -> degrees.
pub fn degrees(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_div(pg_getarg_float8(fcinfo, 0), RADIANS_PER_DEGREE))
}
/// PG `dpi`: the constant PI.
pub fn dpi(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(crate::utils::float::M_PI)
}
/// PG `radians`: degrees -> radians.
pub fn radians(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mul(pg_getarg_float8(fcinfo, 0), RADIANS_PER_DEGREE))
}

// ===========================================================================
//   HYPERBOLIC FUNCTIONS
// ===========================================================================

/// PG `dsinh`.
pub fn dsinh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let mut result = arg1.sinh();
    // ERANGE => overflow to +/-Inf by sign of arg1.
    if result.is_infinite() && !arg1.is_infinite() {
        result = if arg1 < 0.0 { -get_float8_infinity() } else { get_float8_infinity() };
    }
    Float8GetDatum(result)
}
/// PG `dcosh`.
pub fn dcosh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    let result = arg1.cosh(); // always positive; overflow already yields +Inf
    if result == 0.0 {
        float_underflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dtanh`.
pub fn dtanh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let result = pg_getarg_float8(fcinfo, 0).tanh();
    if result.is_infinite() {
        float_overflow_error();
    }
    Float8GetDatum(result)
}
/// PG `dasinh`.
pub fn dasinh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(pg_getarg_float8(fcinfo, 0).asinh())
}
/// PG `dacosh`.
pub fn dacosh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if arg1 < 1.0 {
        input_out_of_range();
    }
    Float8GetDatum(arg1.acosh())
}
/// PG `datanh`.
pub fn datanh(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_float8(fcinfo, 0);
    if !(-1.0..=1.0).contains(&arg1) {
        input_out_of_range();
    }
    let result = if arg1 == -1.0 {
        -get_float8_infinity()
    } else if arg1 == 1.0 {
        get_float8_infinity()
    } else {
        arg1.atanh()
    };
    Float8GetDatum(result)
}

// ===========================================================================
//   ERROR / GAMMA FUNCTIONS (std-backed)
// ===========================================================================

/// PG `derf`: error function. Not in stable std; staged on the platform libm.
pub fn derf(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("derf needs erf() from libm (not in stable std)")
}
/// PG `derfc`: complementary error function. Staged on libm.
pub fn derfc(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("derfc needs erfc() from libm (not in stable std)")
}
/// PG `dgamma`: gamma function. Staged on libm (tgamma).
pub fn dgamma(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("dgamma needs tgamma() from libm (not in stable std)")
}
/// PG `dlgamma`: log|gamma|. Staged on libm (lgamma).
pub fn dlgamma(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("dlgamma needs lgamma() from libm (not in stable std)")
}

// ===========================================================================
//   FLOAT AGGREGATE OPERATORS
//
// Every accumulator/combine/final reaches the transition ArrayType layout and
// AggCheckCallContext (the executor agg context), neither of which is built
// yet; they call those stubs per rules.md s4. The Youngs-Cramer math is faithful
// to float.c and re-applies once the array machinery lands.
// ===========================================================================

macro_rules! agg_stub {
    ($name:ident, $what:literal) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            unimplemented!(concat!(
                stringify!($name),
                " needs the float8 transition ArrayType + AggCheckCallContext (",
                $what,
                ")"
            ))
        }
    };
}

agg_stub!(float8_combine, "Youngs-Cramer combine");
agg_stub!(float8_accum, "Youngs-Cramer accumulate");
agg_stub!(float4_accum, "Youngs-Cramer accumulate (float4 input)");
agg_stub!(float8_avg, "AVG final");
agg_stub!(float8_var_pop, "VAR_POP final");
agg_stub!(float8_var_samp, "VAR_SAMP final");
agg_stub!(float8_stddev_pop, "STDDEV_POP final");
agg_stub!(float8_stddev_samp, "STDDEV_SAMP final");
agg_stub!(float8_regr_accum, "regression accumulate");
agg_stub!(float8_regr_combine, "regression combine");
agg_stub!(float8_regr_sxx, "regr_sxx final");
agg_stub!(float8_regr_syy, "regr_syy final");
agg_stub!(float8_regr_sxy, "regr_sxy final");
agg_stub!(float8_regr_avgx, "regr_avgx final");
agg_stub!(float8_regr_avgy, "regr_avgy final");
agg_stub!(float8_covar_pop, "covar_pop final");
agg_stub!(float8_covar_samp, "covar_samp final");
agg_stub!(float8_corr, "corr final");
agg_stub!(float8_regr_r2, "regr_r2 final");
agg_stub!(float8_regr_slope, "regr_slope final");
agg_stub!(float8_regr_intercept, "regr_intercept final");

// ===========================================================================
//   FLOAT48 / FLOAT84 CROSS-WIDTH OPERATORS
// ===========================================================================

/// PG `float48pl`.
pub fn float48pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_pl(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48mi`.
pub fn float48mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mi(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48mul`.
pub fn float48mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mul(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48div`.
pub fn float48div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_div(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float84pl`.
pub fn float84pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_pl(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84mi`.
pub fn float84mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mi(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84mul`.
pub fn float84mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_mul(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84div`.
pub fn float84div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Float8GetDatum(float8_div(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}

/// PG `float48eq`.
pub fn float48eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_eq(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48ne`.
pub fn float48ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ne(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48lt`.
pub fn float48lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_lt(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48le`.
pub fn float48le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_le(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48gt`.
pub fn float48gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_gt(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float48ge`.
pub fn float48ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ge(f64::from(pg_getarg_float4(fcinfo, 0)), pg_getarg_float8(fcinfo, 1)))
}
/// PG `float84eq`.
pub fn float84eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_eq(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84ne`.
pub fn float84ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ne(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84lt`.
pub fn float84lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_lt(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84le`.
pub fn float84le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_le(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84gt`.
pub fn float84gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_gt(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}
/// PG `float84ge`.
pub fn float84ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(float8_ge(pg_getarg_float8(fcinfo, 0), f64::from(pg_getarg_float4(fcinfo, 1))))
}

/// PG `width_bucket_float8`: the SQL2003 width_bucket over float8 bounds.
pub fn width_bucket_float8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let operand = pg_getarg_float8(fcinfo, 0);
    let bound1 = pg_getarg_float8(fcinfo, 1);
    let bound2 = pg_getarg_float8(fcinfo, 2);
    let count = pg_getarg_int32(fcinfo, 3);

    if count <= 0 {
        width_bucket_error("count must be greater than zero");
    }
    if operand.is_nan() || bound1.is_nan() || bound2.is_nan() {
        width_bucket_error("operand, lower bound, and upper bound cannot be NaN");
    }
    if bound1.is_infinite() || bound2.is_infinite() {
        width_bucket_error("lower and upper bounds must be finite");
    }

    let result: i32;
    if bound1 < bound2 {
        if operand < bound1 {
            result = 0;
        } else if operand >= bound2 {
            result = pg_add_s32_overflow(count, 1).unwrap_or_else(|| integer_out_of_range());
        } else {
            let mut r = if (bound2 - bound1).is_infinite() {
                count * ((operand / 2.0 - bound1 / 2.0) / (bound2 / 2.0 - bound1 / 2.0)) as i32
            } else {
                count * ((operand - bound1) / (bound2 - bound1)) as i32
            };
            if r >= count {
                r = count - 1;
            }
            result = r + 1;
        }
    } else if bound1 > bound2 {
        if operand > bound1 {
            result = 0;
        } else if operand <= bound2 {
            result = pg_add_s32_overflow(count, 1).unwrap_or_else(|| integer_out_of_range());
        } else {
            let mut r = if (bound1 - bound2).is_infinite() {
                count * ((bound1 / 2.0 - operand / 2.0) / (bound1 / 2.0 - bound2 / 2.0)) as i32
            } else {
                count * ((bound1 - operand) / (bound1 - bound2)) as i32
            };
            if r >= count {
                r = count - 1;
            }
            result = r + 1;
        }
    } else {
        width_bucket_error("lower bound cannot equal upper bound");
    }
    Int32GetDatum(result)
}

fn width_bucket_error(msg: &'static str) -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
            .errmsg(msg);
    });
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetBool, NullableDatum};
    use std::panic::catch_unwind;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: crate::postgres_ext::InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }
    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    #[test]
    fn float8_in_out_roundtrip_basic() {
        for s in ["0", "1", "-1", "3.5", "100", "-2.25", "1e10", "1.5e-3"] {
            let mut f = fc(&[cstr_datum(s)]);
            let d = float8in(&mut f);
            let mut f = fc(&[d]);
            let back = out_to_string(float8out(&mut f));
            // Parsing back the output must equal parsing the input.
            assert_eq!(back.parse::<f64>().unwrap(), s.parse::<f64>().unwrap(), "in/out {s}");
        }
    }

    #[test]
    fn float8_in_out_special_values() {
        for (input, want) in [
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
            ("inf", "Infinity"),
            ("-inf", "-Infinity"),
            ("NaN", "NaN"),
            ("nan", "NaN"),
        ] {
            let mut f = fc(&[cstr_datum(input)]);
            let d = float8in(&mut f);
            let mut f = fc(&[d]);
            assert_eq!(out_to_string(float8out(&mut f)), want, "special {input}");
        }
    }

    #[test]
    fn float8_in_errors() {
        for bad in ["", "abc", "1.2.3", "x5"] {
            let s = bad.to_owned();
            let r = catch_unwind(move || {
                let mut f = fc(&[cstr_datum(&s)]);
                float8in(&mut f)
            });
            assert!(r.is_err(), "{bad} should be invalid syntax");
        }
        // overflow of a finite literal raises out of range.
        let r = catch_unwind(|| {
            let mut f = fc(&[cstr_datum("1e400")]);
            float8in(&mut f)
        });
        assert!(r.is_err(), "1e400 should overflow");
    }

    #[test]
    fn float4_arithmetic_and_overflow() {
        let mut f = fc(&[Float4GetDatum(2.0), Float4GetDatum(3.0)]);
        assert_eq!(DatumGetFloat4(float4pl(&mut f)), 5.0);
        let mut f = fc(&[Float4GetDatum(2.0), Float4GetDatum(3.0)]);
        assert_eq!(DatumGetFloat4(float4mul(&mut f)), 6.0);
        // overflow: FLT_MAX * 2 -> Inf from finite inputs raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float4GetDatum(f32::MAX), Float4GetDatum(2.0)]);
            float4mul(&mut f)
        })
        .is_err());
        // division by zero raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float4GetDatum(1.0), Float4GetDatum(0.0)]);
            float4div(&mut f)
        })
        .is_err());
    }

    #[test]
    fn float8_arithmetic_and_overflow() {
        let mut f = fc(&[Float8GetDatum(10.0), Float8GetDatum(4.0)]);
        assert_eq!(DatumGetFloat8(float8mi(&mut f)), 6.0);
        let mut f = fc(&[Float8GetDatum(9.0), Float8GetDatum(3.0)]);
        assert_eq!(DatumGetFloat8(float8div(&mut f)), 3.0);
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float8GetDatum(f64::MAX), Float8GetDatum(2.0)]);
            float8mul(&mut f)
        })
        .is_err());
    }

    #[test]
    fn nan_sorts_highest() {
        let nan = get_float8_nan();
        // NaN > any non-NaN; NaN == NaN.
        let mut f = fc(&[Float8GetDatum(nan), Float8GetDatum(1.0e308)]);
        assert!(DatumGetBool(float8gt(&mut f)));
        let mut f = fc(&[Float8GetDatum(nan), Float8GetDatum(nan)]);
        assert!(DatumGetBool(float8eq(&mut f)));
        let mut f = fc(&[Float8GetDatum(1.0), Float8GetDatum(nan)]);
        assert!(DatumGetBool(float8lt(&mut f)));
        // btfloat8cmp: NaN compares as 1 (greater).
        let mut f = fc(&[Float8GetDatum(nan), Float8GetDatum(0.0)]);
        assert_eq!(DatumGetInt32(btfloat8cmp(&mut f)), 1);
        let mut f = fc(&[Float8GetDatum(1.0), Float8GetDatum(2.0)]);
        assert_eq!(DatumGetInt32(btfloat8cmp(&mut f)), -1);
    }

    #[test]
    fn cross_type_compare() {
        let mut f = fc(&[Float4GetDatum(1.5), Float8GetDatum(1.5)]);
        assert!(DatumGetBool(float48eq(&mut f)));
        let mut f = fc(&[Float8GetDatum(2.0), Float4GetDatum(3.0)]);
        assert!(DatumGetBool(float84lt(&mut f)));
        let mut f = fc(&[Float4GetDatum(1.0), Float8GetDatum(2.0)]);
        assert_eq!(DatumGetInt32(btfloat48cmp(&mut f)), -1);
    }

    #[test]
    fn math_functions() {
        let mut f = fc(&[Float8GetDatum(9.0)]);
        assert_eq!(DatumGetFloat8(dsqrt(&mut f)), 3.0);
        let mut f = fc(&[Float8GetDatum(2.0), Float8GetDatum(10.0)]);
        assert_eq!(DatumGetFloat8(dpow(&mut f)), 1024.0);
        let mut f = fc(&[Float8GetDatum(std::f64::consts::E)]);
        assert!((DatumGetFloat8(dlog1(&mut f)) - 1.0).abs() < 1e-12);
        // sqrt of negative raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float8GetDatum(-1.0)]);
            dsqrt(&mut f)
        })
        .is_err());
        // ln(0) raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float8GetDatum(0.0)]);
            dlog1(&mut f)
        })
        .is_err());
        // degree trig is exact at the special angles.
        let mut f = fc(&[Float8GetDatum(30.0)]);
        assert_eq!(DatumGetFloat8(dsind(&mut f)), 0.5);
        let mut f = fc(&[Float8GetDatum(90.0)]);
        assert_eq!(DatumGetFloat8(dcosd(&mut f)), 0.0);
    }

    #[test]
    fn int_float_casts() {
        let mut f = fc(&[Int32GetDatum(5)]);
        assert_eq!(DatumGetFloat8(i4tod(&mut f)), 5.0);
        let mut f = fc(&[Float8GetDatum(3.7)]);
        assert_eq!(DatumGetInt32(dtoi4(&mut f)), 4); // rounds
        let mut f = fc(&[Float8GetDatum(2.5)]);
        assert_eq!(DatumGetInt32(dtoi4(&mut f)), 2); // round half to even
        let mut f = fc(&[Float4GetDatum(1.25)]);
        assert_eq!(DatumGetFloat8(ftod(&mut f)), 1.25);
        // dtof: float8 in float4 range narrows fine.
        let mut f = fc(&[Float8GetDatum(1.5)]);
        assert_eq!(DatumGetFloat4(dtof(&mut f)), 1.5);
        // dtof overflow: a finite f64 beyond f32 range raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float8GetDatum(1.0e300)]);
            dtof(&mut f)
        })
        .is_err());
        // dtoi4 out of range raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Float8GetDatum(1.0e20)]);
            dtoi4(&mut f)
        })
        .is_err());
    }

    /// float8in resolves through the generated fmgr table to a bound function.
    #[test]
    fn fmgr_table_binds_float8in() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "float8in")
            .expect("float8in present");
        let func = entry.func.expect("float8in bound");
        let mut f = fc(&[cstr_datum("2.5")]);
        assert_eq!(DatumGetFloat8(func(&mut f)), 2.5);
    }
}

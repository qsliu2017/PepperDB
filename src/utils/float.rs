//! Translated from PostgreSQL src/include/utils/float.h

/// PI, from M_PI fallback.
pub const M_PI: f64 = std::f64::consts::PI;
/// Radians per degree, a.k.a. PI / 180.
pub const RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;

// Utility functions in float.c (bare declarations -> stubs).

/// pg_noreturn: raises a "value out of range: overflow" error.
pub fn float_overflow_error() -> ! {
    unimplemented!()
}

/// pg_noreturn: raises a "value out of range: underflow" error.
pub fn float_underflow_error() -> ! {
    unimplemented!()
}

/// pg_noreturn: raises a "division by zero" error.
pub fn float_zero_divide_error() -> ! {
    unimplemented!()
}

/// is_infinite: 1 for +inf, -1 for -inf, 0 otherwise.
pub fn is_infinite(val: f64) -> i32 {
    let _ = val;
    unimplemented!()
}

/// float8in_internal: parse a float8; escontext soft-error folds into Result.
pub fn float8in_internal(
    num: &str,
    type_name: &str,
    orig_string: &str,
) -> Result<(f64, usize), String> {
    let _ = (num, type_name, orig_string);
    unimplemented!()
}

/// float4in_internal: parse a float4; escontext soft-error folds into Result.
pub fn float4in_internal(
    num: &str,
    type_name: &str,
    orig_string: &str,
) -> Result<(f32, usize), String> {
    let _ = (num, type_name, orig_string);
    unimplemented!()
}

/// float8out_internal: client-visible text output.
pub fn float8out_internal(num: f64) -> String {
    let _ = num;
    unimplemented!()
}

pub fn float4_cmp_internal(a: f32, b: f32) -> i32 {
    let _ = (a, b);
    unimplemented!()
}

pub fn float8_cmp_internal(a: f64, b: f64) -> i32 {
    let _ = (a, b);
    unimplemented!()
}

// Inline infinity/NaN constructors.

pub const fn get_float4_infinity() -> f32 {
    f32::INFINITY
}

pub const fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

pub const fn get_float4_nan() -> f32 {
    f32::NAN
}

pub const fn get_float8_nan() -> f64 {
    f64::NAN
}

// Arithmetic with overflow/underflow reported as errors.

pub fn float4_pl(val1: f32, val2: f32) -> f32 {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

pub fn float8_pl(val1: f64, val2: f64) -> f64 {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

pub fn float4_mi(val1: f32, val2: f32) -> f32 {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

pub fn float8_mi(val1: f64, val2: f64) -> f64 {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    result
}

pub fn float4_mul(val1: f32, val2: f32) -> f32 {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && val1 != 0.0 && val2 != 0.0 {
        float_underflow_error();
    }
    result
}

pub fn float8_mul(val1: f64, val2: f64) -> f64 {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 && val1 != 0.0 && val2 != 0.0 {
        float_underflow_error();
    }
    result
}

pub fn float4_div(val1: f32, val2: f32) -> f32 {
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

pub fn float8_div(val1: f64, val2: f64) -> f64 {
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

// NaN-aware comparisons: NaN equals NaN and sorts larger than any non-NaN.

pub fn float4_eq(val1: f32, val2: f32) -> bool {
    if val1.is_nan() { val2.is_nan() } else { !val2.is_nan() && val1 == val2 }
}

pub fn float8_eq(val1: f64, val2: f64) -> bool {
    if val1.is_nan() { val2.is_nan() } else { !val2.is_nan() && val1 == val2 }
}

#[allow(clippy::float_cmp, reason = "PG float comparison semantics, exact equality intended")]
pub fn float4_ne(val1: f32, val2: f32) -> bool {
    if val1.is_nan() { !val2.is_nan() } else { val2.is_nan() || val1 != val2 }
}

#[allow(clippy::float_cmp, reason = "PG float comparison semantics, exact equality intended")]
pub fn float8_ne(val1: f64, val2: f64) -> bool {
    if val1.is_nan() { !val2.is_nan() } else { val2.is_nan() || val1 != val2 }
}

pub fn float4_lt(val1: f32, val2: f32) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

pub fn float8_lt(val1: f64, val2: f64) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

pub fn float4_le(val1: f32, val2: f32) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

pub fn float8_le(val1: f64, val2: f64) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

pub fn float4_gt(val1: f32, val2: f32) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

pub fn float8_gt(val1: f64, val2: f64) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

pub fn float4_ge(val1: f32, val2: f32) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

pub fn float8_ge(val1: f64, val2: f64) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

pub fn float4_min(val1: f32, val2: f32) -> f32 {
    if float4_lt(val1, val2) { val1 } else { val2 }
}

pub fn float8_min(val1: f64, val2: f64) -> f64 {
    if float8_lt(val1, val2) { val1 } else { val2 }
}

pub fn float4_max(val1: f32, val2: f32) -> f32 {
    if float4_gt(val1, val2) { val1 } else { val2 }
}

pub fn float8_max(val1: f64, val2: f64) -> f64 {
    if float8_gt(val1, val2) { val1 } else { val2 }
}

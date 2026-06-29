//! Translated from PostgreSQL src/include/utils/float.h

/// PI, from M_PI fallback.
pub const M_PI: f64 = std::f64::consts::PI;
/// Radians per degree, a.k.a. PI / 180.
pub const RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;

// Utility functions in float.c: the bodies live in the adt/float leaf; the
// header re-exports them (rules.md s3). float_overflow_error/underflow/
// zero_divide are pg_noreturn; is_infinite, the in/out internals, and the
// cmp_internal 3-way comparators round out float.h's float.c declarations.
pub use crate::backend::utils::adt::float::{
    float4_cmp_internal, float4in_internal, float8_cmp_internal, float8in_internal,
    float8out_internal, float_overflow_error, float_underflow_error, float_zero_divide_error,
    is_infinite,
};

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

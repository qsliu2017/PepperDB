//! common/shortest_dec.h - Ryu floating-point shortest decimal output.

use std::ffi::{c_char, c_int};

/// The length of 25 comes from:
///
/// Case 1: -9.9999999999999999e-299  = 24 bytes, plus 1 for null
///
/// Case 2: -0.00099999999999999999   = 23 bytes, plus 1 for null
pub const DOUBLE_SHORTEST_DECIMAL_LEN: c_int = 25;

pub unsafe fn double_to_shortest_decimal_bufn(f: f64, result: *mut c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn double_to_shortest_decimal_buf(f: f64, result: *mut c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn double_to_shortest_decimal(f: f64) -> *mut c_char {
    unimplemented!()
}

/// The length of 16 comes from:
///
/// Case 1: -9.99999999e+29  = 15 bytes, plus 1 for null
///
/// Case 2: -0.000999999999  = 15 bytes, plus 1 for null
pub const FLOAT_SHORTEST_DECIMAL_LEN: c_int = 16;

pub unsafe fn float_to_shortest_decimal_bufn(f: f32, result: *mut c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn float_to_shortest_decimal_buf(f: f32, result: *mut c_char) -> c_int {
    unimplemented!()
}

pub unsafe fn float_to_shortest_decimal(f: f32) -> *mut c_char {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/common/shortest_dec.h
// Ryu shortest-decimal float output. Client-visible text - keep bit-exact.

/// Max buffer length for the shortest decimal repr of a `double` (incl. nul).
pub const DOUBLE_SHORTEST_DECIMAL_LEN: usize = 25;
/// Max buffer length for the shortest decimal repr of a `float` (incl. nul).
pub const FLOAT_SHORTEST_DECIMAL_LEN: usize = 16;

/// Write the shortest decimal of `f` into `result`; returns bytes written.
pub fn double_to_shortest_decimal_bufn(f: f64, result: &mut [u8]) -> i32 {
    let _ = (f, result);
    unimplemented!()
}

/// As `double_to_shortest_decimal_bufn` but nul-terminates `result`.
pub fn double_to_shortest_decimal_buf(f: f64, result: &mut [u8]) -> i32 {
    let _ = (f, result);
    unimplemented!()
}

/// Allocate and return the shortest decimal of `f`.
pub fn double_to_shortest_decimal(f: f64) -> String {
    let _ = f;
    unimplemented!()
}

/// Write the shortest decimal of `f` into `result`; returns bytes written.
pub fn float_to_shortest_decimal_bufn(f: f32, result: &mut [u8]) -> i32 {
    let _ = (f, result);
    unimplemented!()
}

/// As `float_to_shortest_decimal_bufn` but nul-terminates `result`.
pub fn float_to_shortest_decimal_buf(f: f32, result: &mut [u8]) -> i32 {
    let _ = (f, result);
    unimplemented!()
}

/// Allocate and return the shortest decimal of `f`.
pub fn float_to_shortest_decimal(f: f32) -> String {
    let _ = f;
    unimplemented!()
}

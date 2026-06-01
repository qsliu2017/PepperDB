//! utils/bytea.h - Declarations for BYTEA data type support.

use std::ffi::c_int;

// typedef enum { BYTEA_OUTPUT_ESCAPE, BYTEA_OUTPUT_HEX } ByteaOutputType;
// Project convention: enum -> c_int alias plus pub const variants.
pub type ByteaOutputType = c_int;
pub const BYTEA_OUTPUT_ESCAPE: ByteaOutputType = 0;
pub const BYTEA_OUTPUT_HEX: ByteaOutputType = 1;

// extern PGDLLIMPORT int bytea_output; /* ByteaOutputType, but int for GUC enum */
#[no_mangle]
pub static mut bytea_output: c_int = BYTEA_OUTPUT_HEX;

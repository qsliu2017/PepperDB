//! port/cygwin.h - Cygwin platform tweaks.
//!
//! Mostly preprocessor platform config. The only Rust-meaningful symbol is the
//! `HAVE_BUGGY_STRTOF` feature flag. `PGDLLIMPORT` is a Windows/Cygwin DLL
//! linkage marker (`__declspec(dllimport/dllexport)`) with no Rust-level
//! equivalent, so it is not translated.

/// Cygwin has a strtof() which is literally just (float)strtod(), giving
/// misrounding and silent over/underflow; we substitute our own wrapper.
pub const HAVE_BUGGY_STRTOF: c_int = 1;

use std::ffi::c_int;

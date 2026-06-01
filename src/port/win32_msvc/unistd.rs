//! port/win32_msvc/unistd.h - MSVC compatibility shims for POSIX <unistd.h>.
//!
//! MSVC does not define these, nor does _fileno(stdin) etc reliably work
//! (returns -1 if stdin/out/err are closed).

use std::ffi::c_int;

pub const STDIN_FILENO: c_int = 0;
pub const STDOUT_FILENO: c_int = 1;
pub const STDERR_FILENO: c_int = 2;

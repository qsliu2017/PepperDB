//! port/win32_msvc/sys/time.h - empty MSVC compat placeholder for <sys/time.h>
//!
//! The PostgreSQL header `src/include/port/win32_msvc/sys/time.h` contains only
//! its path comment and defines no typedefs, structs, macros, or prototypes.
//! It exists solely so that `#include <sys/time.h>` resolves under the MSVC
//! build (which lacks a POSIX <sys/time.h>); the actual `struct timeval` and
//! `gettimeofday` declarations come from elsewhere on Windows.
//!
//! Faithful translation: an empty module defining 0 symbols.
//!
//! Windows-only: wired `#[cfg(windows)]` by the main agent.

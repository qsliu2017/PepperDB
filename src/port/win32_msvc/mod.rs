//! Windows-MSVC POSIX-compatibility shims (postgres/src/include/port/win32_msvc).
//!
//! MSVC-specific implementations of POSIX system headers; only meaningful on a
//! Windows/MSVC build. The whole tree is `#[cfg(windows)]`-gated by the parent.

pub mod dirent;
pub mod sys;
pub mod unistd;
pub mod utime;

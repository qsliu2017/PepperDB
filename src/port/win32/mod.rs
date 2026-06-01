//! Win32 platform support: the `port/win32.h` umbrella (dynamic-linking markers /
//! minimum Windows version) plus its `port/win32/*` POSIX-compatibility shim
//! subtree. Only meaningful on a Windows build; the whole tree is
//! `#[cfg(windows)]`-gated by the parent so it does not affect non-Windows targets.
//!
//! port/win32.h itself is entirely C preprocessor (WIN32 macro normalization, the
//! <crtdefs.h> errcode workaround, PGDLLIMPORT/PGDLLEXPORT __declspec markers) with
//! no Rust equivalent; only the minimum-version constant is reproduced for fidelity.

/// Minimum required _WIN32_WINNT: Windows 10 (0x0A00). C: `#define _WIN32_WINNT 0x0A00`.
pub const _WIN32_WINNT: u32 = 0x0A00;

pub mod arpa;
pub mod dlfcn;
pub mod grp;
pub mod netdb;
pub mod netinet;
pub mod pwd;
pub mod sys;

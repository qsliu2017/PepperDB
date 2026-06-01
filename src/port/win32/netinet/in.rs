//! port/win32/netinet/in.h - Windows compat shim for <netinet/in.h>.
//!
//! The original C header has no symbols of its own; its entire body is
//! `#include <sys/socket.h>`. On Windows, the POSIX <netinet/in.h>
//! declarations are provided by the winsock headers pulled in transitively
//! via the port `sys/socket.h` shim. This module therefore re-exports that
//! shim and defines no symbols.

#[allow(unused_imports)]
pub use crate::port::win32::sys::socket::*;

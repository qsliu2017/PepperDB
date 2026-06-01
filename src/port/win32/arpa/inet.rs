//! port/win32/arpa/inet.h - Windows-MSVC compat shim for <arpa/inet.h>.
//!
//! The original C header has no declarations of its own; its entire body is:
//!     #include <sys/socket.h>
//! On Windows the inet_* / address-conversion routines live alongside the
//! socket API, so this header simply pulls in the win32 <sys/socket.h> shim.
//!
//! Faithful translation: re-export the win32 sys::socket module so that paths
//! resolving through `arpa::inet` reach the same symbols.
//!
//! Defines 0 symbols of its own.

// #include <sys/socket.h>
// TODO: main agent wires the actual module path for the win32 sys/socket shim.
pub use crate::port::win32::sys::socket::*;

//! port/win32/netdb.h - Windows compatibility shim for <netdb.h>.
//!
//! On Windows there is no POSIX <netdb.h>; this header simply pulls in the
//! Winsock <ws2tcpip.h> system header, which provides getaddrinfo/getnameinfo
//! and related networking declarations. It defines no symbols of its own.
//!
//! C source body (verbatim intent):
//!     #include <ws2tcpip.h>
//!
//! 0 symbols defined. The corresponding Winsock declarations are supplied by
//! the platform's <ws2tcpip.h> at the C/FFI boundary.

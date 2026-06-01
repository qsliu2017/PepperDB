//! port/win32/netinet/tcp.h - Windows MSVC compat shim for <netinet/tcp.h>
//!
//! The original C header defines no symbols of its own; its entire body is a
//! single `#include <sys/socket.h>`, so on Windows the TCP-related declarations
//! are obtained transitively from the socket compat header.
//!
//! Faithful translation: 0 symbols. The re-export is documented here; the main
//! agent wires the actual `sys::socket` module (cfg(windows)-gated).

// C: #include <sys/socket.h>
// (No typedefs, structs, or #define constants are declared by this header.)

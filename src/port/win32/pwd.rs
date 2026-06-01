//! port/win32/pwd.h - Windows placeholder for the POSIX <pwd.h> system header.
//!
//! On Windows there is no <pwd.h>; PostgreSQL ships this empty header purely so
//! that `#include <pwd.h>` resolves to a no-op include path. The actual
//! `struct passwd` / `getpwuid` substitutes (where needed) live elsewhere in the
//! win32 port. This file defines no typedefs, structs, macros, or prototypes.
//!
//! Symbols defined: none (0).

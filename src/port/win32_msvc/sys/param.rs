//! port/win32_msvc/sys/param.h - MSVC stub for <sys/param.h>.
//!
//! On MSVC there is no system <sys/param.h>. PostgreSQL ships this empty
//! header so that `#include <sys/param.h>` resolves on the Windows-MSVC
//! build. It defines no typedefs, structs, macros, or prototypes.
//!
//! Symbols defined: 0 (empty compatibility shim).

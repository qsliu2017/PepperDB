//! port/win32/sys/wait.h - Windows stub for the POSIX <sys/wait.h> header.
//!
//! On Windows there is no <sys/wait.h>. PostgreSQL ships this empty header so
//! that code performing `#include <sys/wait.h>` compiles cleanly on the
//! WINDOWS-MSVC platform. The upstream file contains only its own path comment
//! and defines no typedefs, structs, macros, or prototypes.
//!
//! Symbols defined: 0

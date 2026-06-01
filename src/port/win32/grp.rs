//! port/win32/grp.h - Windows MSVC stub for the POSIX <grp.h> system header.
//!
//! On Windows there is no <grp.h> (group database). PostgreSQL ships this
//! empty header so that `#include <grp.h>` compiles on the Windows MSVC
//! platform. The upstream file contains only a path comment and defines no
//! types, macros, or prototypes.
//!
//! Faithful translation: 0 symbols.

//! port/win32_msvc/sys/file.h - Windows MSVC placeholder for <sys/file.h>.
//!
//! The upstream PostgreSQL header `src/include/port/win32_msvc/sys/file.h`
//! contains only the path comment `/* src/include/port/win32_msvc/sys/file.h */`
//! and defines NO symbols. It exists so that `#include <sys/file.h>` resolves on
//! the Windows MSVC toolchain (which lacks that POSIX header). Faithfully
//! translated as an empty, symbol-free module.

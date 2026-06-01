//! port/win32/dlfcn.h - Windows MSVC placeholder for POSIX <dlfcn.h>.
//!
//! The upstream PostgreSQL header `src/include/port/win32/dlfcn.h` has an
//! empty body (it contains only the `/* src/include/port/win32/dlfcn.h */`
//! path comment). It exists purely so that `#include <dlfcn.h>` resolves on
//! the Windows MSVC platform, where there is no system `<dlfcn.h>`. The actual
//! dynamic-loader entry points (`dlopen`/`dlsym`/`dlclose`/`dlerror`) are
//! supplied by PostgreSQL's own port layer (`src/port/dlopen.c`) and declared
//! in `src/include/port.h`, not here.
//!
//! Therefore this module defines 0 symbols.

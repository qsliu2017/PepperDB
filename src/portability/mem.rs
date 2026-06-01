//! portability/mem.h - portability definitions for various memory operations.
//!
//! This header is purely C preprocessor `#define`s, several of which are gated
//! on platform/system macros (Solaris SHM_SHARE_MMU, BSD MAP_NOSYNC, etc.). The
//! project declares no Cargo features, so we emit the default (non-special)
//! branch unconditionally, matching a Linux build, and note the C gate inline.
//!
//! The mmap()/shmget() flag constants (MAP_SHARED, MAP_ANON, MAP_FAILED, ...)
//! are normally provided by the system <sys/mman.h>. We do not redefine those
//! system constants here; the macros below reference the values the C code
//! would have resolved them to on a typical Linux platform. Consumers that need
//! the real libc values should pull them from a libc crate; here we mirror the
//! header's derived constants faithfully.

use std::ffi::{c_int, c_void};

/// access/modify by user only (0600 octal).
pub const IPCProtection: c_int = 0o600;

// On Solaris with SHM_SHARE_MMU defined, PG_SHMAT_FLAGS == SHM_SHARE_MMU (use
// intimate shared memory). Otherwise it is 0. We emit the default branch.
pub const PG_SHMAT_FLAGS: c_int = 0;

// Linux prefers MAP_ANONYMOUS, but the flag is called MAP_ANON on other
// systems. The C header aliases MAP_ANONYMOUS to MAP_ANON when the former is
// undefined. On Linux MAP_ANONYMOUS == 0x20.
pub const MAP_ANONYMOUS: c_int = 0x20;

// MAP_SHARED is a system constant (<sys/mman.h>); on Linux it is 0x01. The C
// header references it but does not define it. Provided here so PG_MMAP_FLAGS
// can be computed faithfully.
pub const MAP_SHARED: c_int = 0x01;

// BSD-derived systems have MAP_HASSEMAPHORE, but it is not present (or needed)
// on Linux, where the C header defines it as 0. Emit the default branch.
pub const MAP_HASSEMAPHORE: c_int = 0;

// BSD-derived systems use the MAP_NOSYNC flag to prevent dirty mmap(2) pages
// from being gratuitously flushed to disk; on Linux the C header defines it as
// 0. Emit the default branch.
pub const MAP_NOSYNC: c_int = 0;

/// PG_MMAP_FLAGS == (MAP_SHARED | MAP_ANONYMOUS | MAP_HASSEMAPHORE).
pub const PG_MMAP_FLAGS: c_int = MAP_SHARED | MAP_ANONYMOUS | MAP_HASSEMAPHORE;

/// Some really old systems don't define MAP_FAILED; the C header falls back to
/// ((void *) -1). We mirror that fallback value as a raw pointer.
pub const MAP_FAILED: *mut c_void = usize::MAX as *mut c_void;

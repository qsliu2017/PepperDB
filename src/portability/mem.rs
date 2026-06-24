//! Translated from PostgreSQL src/include/portability/mem.h

// Portability definitions for memory operations (sysv/mmap shared memory).
// Mostly obsolete here: the single-process async model replaces SysV/mmap
// shared memory with normal heap + Arc/locks. These constants are kept for the
// few places that still mmap; raw flag values come from libc/nix at the call
// site. Solaris/BSD-only knobs are dropped (Linux x86_64 + macOS aarch64 only).

/// IPC object permissions: access/modify by owner only.
pub const IPC_PROTECTION: u32 = 0o600;

// On both targets these BSD-only mmap flags are absent / unneeded.
pub const PG_SHMAT_FLAGS: i32 = 0;
pub const MAP_HASSEMAPHORE: i32 = 0;
pub const MAP_NOSYNC: i32 = 0;

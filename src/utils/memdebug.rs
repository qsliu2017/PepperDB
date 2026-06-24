//! Translated from PostgreSQL src/include/utils/memdebug.h
//
// Memory debugging support. In C this wraps Valgrind client-request macros and a
// few CLOBBER/MEMORY_CONTEXT_CHECKING/RANDOMIZE build-time helpers. None of the
// Valgrind machinery carries over; the macros were all no-ops without USE_VALGRIND.
// Tombstone note: Valgrind integration dropped. The sentinel/wipe helpers below
// only exist under PG debug build flags and are kept as plain stubs.

/// Wipe freed memory (CLOBBER_FREED_MEMORY). No-op without the debug build flag.
pub fn wipe_mem(_ptr: &mut [u8]) {}

/// Set a one-byte sentinel (MEMORY_CONTEXT_CHECKING).
pub fn set_sentinel(_base: &mut [u8], _offset: usize) {}

/// Check a one-byte sentinel (MEMORY_CONTEXT_CHECKING).
pub fn sentinel_ok(_base: &[u8], _offset: usize) -> bool {
    true
}

/// Fill a buffer with random bytes (RANDOMIZE_ALLOCATED_MEMORY).
pub fn randomize_mem(_ptr: &mut [u8]) {
    unimplemented!()
}

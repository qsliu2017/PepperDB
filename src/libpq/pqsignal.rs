//! Translated from PostgreSQL src/include/libpq/pqsignal.h
//
// Backend signal-mask support. The WIN32 sigset_t emulation block is dropped
// (non-target); on Linux/macOS `sigset_t` comes from libc at the signal boundary.

// Saved signal masks (C: sigset_t globals). Represented as raw machine words;
// the actual sigset_t plumbing lives at the libc boundary.
// TODO(ptr): re-type to libc::sigset_t when the signal layer is implemented.
pub static mut UNBLOCK_SIG: u64 = 0;
pub static mut BLOCK_SIG: u64 = 0;
pub static mut STARTUP_BLOCK_SIG: u64 = 0;

pub fn pqinitmask() {
    unimplemented!()
}

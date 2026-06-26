//! Translated from PostgreSQL src/include/access/slru.h
//! Simple LRU buffering for transaction status logfiles.
//!
//! Shared-memory note: SlruSharedData lived in shmem and was protected by
//! LWLock banks. Under the single-process async model the shmem indirection
//! collapses: the buffer arrays become owned Vecs and the LWLocks become
//! `parking_lot`/`std` locks (translation addendum). Kept in-memory (NOT
//! `#[repr(C)]`): SLRU pages are written to disk individually, but these control
//! structs are not themselves on-disk layouts.

use crate::pg_config::BLCKSZ;

/// Max number of buffers, to avoid overflowing size_t arithmetic.
pub const SLRU_MAX_ALLOWED_BUFFERS: usize = (1024 * 1024 * 1024) / BLCKSZ as usize;

/// SLRU segment size in pages (32 pages = 256Kb).
pub const SLRU_PAGES_PER_SEGMENT: i32 = 32;

/// Page status codes (does not include the "dirty" bit).
pub enum SlruPageStatus {
    Empty,           // buffer is not in use
    ReadInProgress,  // page is being read in
    Valid,           // page is valid and not being written
    WriteInProgress, // page is being written out
}

// ---- definitions live in transam/slru.c; re-exported here (rules s2) ----
//
// The shmem control structs (`SlruSharedData`/`SlruCtlData`) and the C-named
// free functions (`SimpleLruReadPage`, ...) are replaced by the idiomatic
// `SlruCtl` type with async methods in the backend module (design step14 s5):
// the bank `Mutex` + per-slot `WaitQueue` supersede the bank/buffer LWLocks, so
// the old function-pointer API has no remaining call sites. Re-export the type
// and its autotune helper; the consts/enum above are still the on-the-wire
// vocabulary the backend uses.
pub use crate::backend::access::transam::slru::{
    SLRU_BANK_SIZE, SlruCtl, autotune_buffers as SimpleLruAutotuneBuffers,
};

//! Translated from PostgreSQL src/include/libpq/libpq-fs.h
//
// Read/write mode flags for inversion (large object) calls.

use bitflags::bitflags;

bitflags! {
    /// Read/write mode flags for inversion (large object) calls.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InvMode: i32 {
        const WRITE = 0x00020000;
        const READ  = 0x00040000;
    }
}

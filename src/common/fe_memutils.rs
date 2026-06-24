//! Translated from PostgreSQL src/include/common/fe_memutils.h
//! Frontend memory management. The palloc/pg_malloc allocators are subsumed by
//! Rust ownership (Box/Vec/String); only the size constants, allocation flags,
//! and overflow-checked size helpers carry over. Allocator signatures are kept
//! as stubs for skeleton resolution.

use bitflags::bitflags;

/// Assumed maximum allocation request size (1 GB - 1).
pub const MAX_ALLOC_SIZE: usize = 0x3fffffff;

bitflags! {
    /// Flags for the `*_extended` allocators.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct McxtAllocFlags: u32 {
        const HUGE   = 0x01;
        const NO_OOM = 0x02;
        const ZERO   = 0x04;
    }
}

/// Safe addition of allocation sizes (panics in C on overflow).
pub fn add_size(s1: usize, s2: usize) -> usize {
    s1.checked_add(s2).expect("requested size overflows usize")
}

/// Safe multiplication of allocation sizes (panics in C on overflow).
pub fn mul_size(s1: usize, s2: usize) -> usize {
    s1.checked_mul(s2).expect("requested size overflows usize")
}

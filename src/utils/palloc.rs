//! Translated from PostgreSQL src/include/utils/palloc.h
//
// TODO(memory): MemoryContext is dropped in favor of Rust ownership (arena/Box +
// RAII; reset-on-abort maps to scoped drop). This is a MINIMAL STUB SET: signatures
// and the MCXT_ALLOC_* flags only -- NOT a real allocator. The palloc/pfree API is
// kept so dependents type-check; real call sites migrate to ownership in Phase 2.

use bitflags::bitflags;

bitflags! {
    /// Flags for MemoryContextAllocExtended (MCXT_ALLOC_*).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct McxtAllocFlags: i32 {
        const HUGE   = 0x01; // allow huge allocation (> 1 GB)
        const NO_OOM = 0x02; // no failure if out-of-memory
        const ZERO   = 0x04; // zero allocated memory
    }
}

// MemoryContext is an opaque handle in C (struct MemoryContextData *). Under Rust
// ownership it has no real representation; kept as an opaque marker type.
// TODO(struct-forward): real definition in nodes/memnodes.h (MemoryContextData).
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::memnodes in Phase 2")]
pub struct MemoryContextData;
#[allow(deprecated)]
pub type MemoryContext = *mut MemoryContextData;

// A memory-context reset/delete callback. The C form threads a `void *arg`; in Rust
// that opaque context is a captured closure (see function-mapping.md 6.3).
// TODO(memory): model as `impl FnOnce()` at the real registration site.
pub type MemoryContextCallbackFunction = fn();

pub struct MemoryContextCallback {
    pub func: MemoryContextCallbackFunction,
}

// === Fundamental allocation ops (TODO(memory): not a real allocator) ===

pub fn MemoryContextAlloc(_context: MemoryContext, _size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn MemoryContextAllocZero(_context: MemoryContext, _size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn MemoryContextAllocExtended(
    _context: MemoryContext,
    _size: usize,
    _flags: McxtAllocFlags,
) -> *mut u8 {
    unimplemented!()
}
pub fn MemoryContextAllocAligned(
    _context: MemoryContext,
    _size: usize,
    _alignto: usize,
    _flags: McxtAllocFlags,
) -> *mut u8 {
    unimplemented!()
}

pub fn palloc(_size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn palloc0(_size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn palloc_extended(_size: usize, _flags: McxtAllocFlags) -> *mut u8 {
    unimplemented!()
}
pub fn palloc_aligned(_size: usize, _alignto: usize, _flags: McxtAllocFlags) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc(_pointer: *mut u8, _size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc_extended(_pointer: *mut u8, _size: usize, _flags: McxtAllocFlags) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc0(_pointer: *mut u8, _oldsize: usize, _size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn pfree(_pointer: *mut u8) {
    unimplemented!()
}

// === Safe size arithmetic ===

pub fn add_size(_s1: usize, _s2: usize) -> usize {
    unimplemented!()
}
pub fn mul_size(_s1: usize, _s2: usize) -> usize {
    unimplemented!()
}
pub fn palloc_mul(_s1: usize, _s2: usize) -> *mut u8 {
    unimplemented!()
}
pub fn palloc0_mul(_s1: usize, _s2: usize) -> *mut u8 {
    unimplemented!()
}
pub fn palloc_mul_extended(_s1: usize, _s2: usize, _flags: McxtAllocFlags) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc_mul(_p: *mut u8, _s1: usize, _s2: usize) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc_mul_extended(
    _p: *mut u8,
    _s1: usize,
    _s2: usize,
    _flags: McxtAllocFlags,
) -> *mut u8 {
    unimplemented!()
}

// === Higher-limit allocators ===

pub fn MemoryContextAllocHuge(_context: MemoryContext, _size: usize) -> *mut u8 {
    unimplemented!()
}
pub fn repalloc_huge(_pointer: *mut u8, _size: usize) -> *mut u8 {
    unimplemented!()
}

/// Set CurrentMemoryContext, returning the old one.
// TODO(memory): CurrentMemoryContext is a process global; becomes task-local/Session.
pub fn MemoryContextSwitchTo(_context: MemoryContext) -> MemoryContext {
    unimplemented!()
}

pub fn MemoryContextRegisterResetCallback(_context: MemoryContext, _cb: &MemoryContextCallback) {
    unimplemented!()
}

// === Context-allocated string duplication ===

pub fn MemoryContextStrdup(_context: MemoryContext, _string: &str) -> String {
    unimplemented!()
}
pub fn pstrdup(_in_: &str) -> String {
    unimplemented!()
}
pub fn pnstrdup(_in_: &str, _len: usize) -> String {
    unimplemented!()
}
pub fn pchomp(_in_: &str) -> String {
    unimplemented!()
}

// psprintf/pvsnprintf -> Rust `format!`; kept as stubs at this boundary.
pub fn psprintf(_fmt: &str) -> String {
    unimplemented!()
}
pub fn pvsnprintf(_buf: &mut [u8], _fmt: &str) -> usize {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/utils/relptr.h
//
// Relative pointers: an offset into some base (shared-memory segment or address
// space) rather than an absolute pointer. The stored value is always a Size; the C
// macro's pointer field is a type-safety hack. Modeled as a generic newtype over the
// stored offset. Encoding: 0 means NULL, otherwise (offset + 1) -- preserved exactly
// so any on-disk/shmem relptr round-trips. The access/store helpers are inline in C
// and translated in full. TODO(memory): shmem collapses under single-process; these
// helpers stay for layout fidelity where relptrs are persisted.

use std::marker::PhantomData;

/// A relative pointer to `T`. Stores `relptr_off` (0 = NULL, else offset+1).
#[repr(transparent)]
pub struct RelPtr<T> {
    pub relptr_off: usize,
    _marker: PhantomData<*mut T>,
}

impl<T> Clone for RelPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for RelPtr<T> {}

impl<T> RelPtr<T> {
    pub const fn null() -> Self {
        Self { relptr_off: 0, _marker: PhantomData }
    }

    pub const fn is_null(self) -> bool {
        self.relptr_off == 0
    }

    /// relptr_offset: the true offset (stored value minus 1). Undefined if null.
    pub const fn offset(self) -> usize {
        self.relptr_off - 1
    }

    /// relptr_access: resolve against `base`, or None if null.
    /// SAFETY: `base + offset` must point to a valid `T` of the right provenance.
    pub unsafe fn access(self, base: *mut u8) -> Option<*mut T> {
        if self.relptr_off == 0 {
            None
        } else {
            Some(unsafe { base.add(self.relptr_off - 1) }.cast::<T>())
        }
    }

    /// relptr_store: encode `val` (a pointer at/after `base`) into this relptr.
    /// SAFETY: `val`, when non-null, must be >= `base` within the same allocation.
    pub unsafe fn store(&mut self, base: *mut u8, val: *mut T) {
        self.relptr_off = relptr_store_eval(base, val.cast::<u8>());
    }

    /// relptr_copy.
    pub fn copy_from(&mut self, other: Self) {
        self.relptr_off = other.relptr_off;
    }
}

/// Inline helper avoiding double-eval of `val` in relptr_store.
/// SAFETY: when non-null, `val` must be >= `base` within the same allocation.
pub unsafe fn relptr_store_eval(base: *mut u8, val: *mut u8) -> usize {
    if val.is_null() {
        0
    } else {
        debug_assert!(val >= base);
        unsafe { val.offset_from(base) as usize + 1 }
    }
}

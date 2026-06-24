//! Translated from PostgreSQL src/include/utils/freepage.h

use crate::utils::relptr::RelPtr;

// Opaque types defined in freepage.c; only used here as relptr targets.
// TODO(struct-forward): repoint to crate::utils::freepage internals in Phase 2.
#[deprecated(note = "TODO(struct-forward): defined in freepage.c, model in Phase 2")]
pub struct FreePageSpanLeader {
    _private: [u8; 0],
}
#[deprecated(note = "TODO(struct-forward): defined in freepage.c, model in Phase 2")]
pub struct FreePageBtree {
    _private: [u8; 0],
}

/// PG uses 4kB pages for memory allocation here.
pub const FPM_PAGE_SIZE: usize = 4096;

/// All but the last freelist hold spans of one size; larger spans go on the last.
pub const FPM_NUM_FREELISTS: usize = 129;

/// Everything needed to manage free pages (see freepage.c). Lives at the start of
/// the managed region; uses relative pointers for self-relocatability.
#[allow(deprecated)]
pub struct FreePageManager {
    pub self_: RelPtr<FreePageManager>,
    pub btree_root: RelPtr<FreePageBtree>,
    pub btree_recycle: RelPtr<FreePageSpanLeader>,
    pub btree_depth: u32,
    pub btree_recycle_count: u32,
    pub singleton_first_page: usize,
    pub singleton_npages: usize,
    pub contiguous_pages: usize,
    pub contiguous_pages_dirty: bool,
    pub freelist: [RelPtr<FreePageSpanLeader>; FPM_NUM_FREELISTS],
    // FPM_EXTRA_ASSERTS-only `free_pages` field dropped (debug build).
}

/// fpm_page_to_pointer: base + FPM_PAGE_SIZE * page.
/// SAFETY: result must stay within the managed region.
pub unsafe fn fpm_page_to_pointer(base: *mut u8, page: usize) -> *mut u8 {
    unsafe { base.add(FPM_PAGE_SIZE * page) }
}

/// fpm_pointer_to_page: (ptr - base) / FPM_PAGE_SIZE.
/// SAFETY: `ptr` must be >= `base` within the same region.
pub unsafe fn fpm_pointer_to_page(base: *mut u8, ptr: *mut u8) -> usize {
    (unsafe { ptr.offset_from(base) } as usize) / FPM_PAGE_SIZE
}

/// fpm_size_to_pages: ceil(sz / FPM_PAGE_SIZE).
pub const fn fpm_size_to_pages(sz: usize) -> usize {
    (sz + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE
}

/// fpm_pointer_is_page_aligned.
/// SAFETY: `ptr` must be >= `base` within the same region.
pub unsafe fn fpm_pointer_is_page_aligned(base: *mut u8, ptr: *mut u8) -> bool {
    (unsafe { ptr.offset_from(base) } as usize) % FPM_PAGE_SIZE == 0
}

/// fpm_relptr_is_page_aligned.
pub fn fpm_relptr_is_page_aligned<T>(relptr: RelPtr<T>) -> bool {
    relptr.offset() % FPM_PAGE_SIZE == 0
}

/// fpm_segment_base: base address of the segment containing the manager.
/// SAFETY: `fpm` must reside in a region whose start it records via `self_`.
pub unsafe fn fpm_segment_base(fpm: &FreePageManager) -> *mut u8 {
    let p = fpm as *const FreePageManager as *mut u8;
    unsafe { p.sub(fpm.self_.offset()) }
}

/// fpm_largest: the manager's largest consecutive run of pages.
pub fn fpm_largest(fpm: &FreePageManager) -> usize {
    fpm.contiguous_pages
}

pub fn FreePageManagerInitialize(_fpm: &mut FreePageManager, _base: *mut u8) {
    unimplemented!()
}

// Returns the found first page on success; bool->Option per function-mapping.
pub fn FreePageManagerGet(_fpm: &mut FreePageManager, _npages: usize) -> Option<usize> {
    unimplemented!()
}

pub fn FreePageManagerPut(_fpm: &mut FreePageManager, _first_page: usize, _npages: usize) {
    unimplemented!()
}

pub fn FreePageManagerDump(_fpm: &mut FreePageManager) -> String {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/nodes/tidbitmap.h

use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;

/// Per-page bitmap size: typically 256 (8K pages) or 1024 (32K pages).
pub const TBM_MAX_TUPLES_PER_PAGE: i32 = crate::access::htup_details::MaxHeapTuplesPerPage;

/// Opaque bitmap representation (private to tidbitmap.c).
#[derive(Debug)]
pub struct TIDBitmap {
    _private: (),
}

/// Opaque private iterator.
#[derive(Debug)]
pub struct TBMPrivateIterator {
    _private: (),
}

/// Opaque shared iterator.
#[derive(Debug)]
pub struct TBMSharedIterator {
    _private: (),
}

/// Unified iterator over a private or shared bitmap. The C union of the two
/// iterator pointers becomes a Rust enum (single-process: shared collapses).
#[derive(Debug)]
pub enum TBMIterator {
    Private(Option<Box<TBMPrivateIterator>>),
    Shared(Option<Box<TBMSharedIterator>>),
}

/// Result structure for tbm_iterate.
#[derive(Debug, Clone, PartialEq)]
pub struct TBMIterateResult {
    pub blockno: BlockNumber,
    pub lossy: bool,
    /// Whether tuples should be rechecked (always true if the page is lossy).
    pub recheck: bool,
    /// Page containing the bitmap for this block; opaque PagetableEntry view.
    // TODO(ptr): was `void *internal_page`; ownership unclear from header.
    pub internal_page: Option<Box<crate::nodes::nodes::Node>>,
}

// dsa_area / dsa_pointer (utils/dsa.h) are tombstoned under single-process; the
// shared-memory params collapse to owned state. usize placeholders for now.
// TODO(struct-forward): repoint to single-process owned state in Phase 2.

pub fn tbm_create(maxbytes: usize) -> Box<TIDBitmap> {
    unimplemented!()
}

pub fn tbm_free(tbm: &mut TIDBitmap) {
    unimplemented!()
}

pub fn tbm_free_shared_area(dp: usize) {
    unimplemented!()
}

pub fn tbm_add_tuples(tbm: &mut TIDBitmap, tids: &[ItemPointerData], recheck: bool) {
    unimplemented!()
}

pub fn tbm_add_page(tbm: &mut TIDBitmap, pageno: BlockNumber) {
    unimplemented!()
}

pub fn tbm_union(a: &mut TIDBitmap, b: &TIDBitmap) {
    unimplemented!()
}

pub fn tbm_intersect(a: &mut TIDBitmap, b: &TIDBitmap) {
    unimplemented!()
}

pub fn tbm_extract_page_tuple(
    iteritem: &TBMIterateResult,
    offsets: &mut [OffsetNumber],
    max_offsets: u32,
) -> i32 {
    unimplemented!()
}

pub fn tbm_is_empty(tbm: &TIDBitmap) -> bool {
    unimplemented!()
}

pub fn tbm_begin_private_iterate(tbm: &mut TIDBitmap) -> Box<TBMPrivateIterator> {
    unimplemented!()
}

pub fn tbm_prepare_shared_iterate(tbm: &mut TIDBitmap) -> usize {
    unimplemented!()
}

/// Returns the next result, or None when the iteration is exhausted.
pub fn tbm_private_iterate(iterator: &mut TBMPrivateIterator) -> Option<TBMIterateResult> {
    unimplemented!()
}

pub fn tbm_shared_iterate(iterator: &mut TBMSharedIterator) -> Option<TBMIterateResult> {
    unimplemented!()
}

pub fn tbm_end_private_iterate(iterator: &mut TBMPrivateIterator) {
    unimplemented!()
}

pub fn tbm_end_shared_iterate(iterator: &mut TBMSharedIterator) {
    unimplemented!()
}

pub fn tbm_attach_shared_iterate(dp: usize) -> Box<TBMSharedIterator> {
    unimplemented!()
}

pub fn tbm_calculate_entries(maxbytes: usize) -> i32 {
    unimplemented!()
}

pub fn tbm_begin_iterate(tbm: &mut TIDBitmap, dsp: usize) -> TBMIterator {
    unimplemented!()
}

pub fn tbm_end_iterate(iterator: &mut TBMIterator) {
    unimplemented!()
}

pub fn tbm_iterate(iterator: &mut TBMIterator) -> Option<TBMIterateResult> {
    unimplemented!()
}

pub fn tbm_exhausted(iterator: &TBMIterator) -> bool {
    match iterator {
        TBMIterator::Private(it) => it.is_none(),
        TBMIterator::Shared(it) => it.is_none(),
    }
}

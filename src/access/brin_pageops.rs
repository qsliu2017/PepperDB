//! Translated from PostgreSQL src/include/access/brin_pageops.h
//! Prototypes for operating on BRIN pages.

use crate::access::brin_revmap::BrinRevmap;
use crate::access::brin_tuple::BrinTuple;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::Page;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::Relation;

/// Update a BRIN tuple, in-place or by moving it; returns whether it succeeded.
pub fn brin_doupdate(
    _idxrel: Relation,
    _pages_per_range: BlockNumber,
    _revmap: &mut BrinRevmap,
    _heap_blk: BlockNumber,
    _oldbuf: Buffer,
    _oldoff: OffsetNumber,
    _origtup: &BrinTuple,
    _origsz: usize,
    _newtup: &BrinTuple,
    _newsz: usize,
    _samepage: bool,
) -> bool {
    unimplemented!()
}

/// True iff a tuple of newsz can replace one of origsz on the same page.
pub fn brin_can_do_samepage_update(_buffer: Buffer, _origsz: usize, _newsz: usize) -> bool {
    unimplemented!()
}

/// Insert a BRIN tuple; `buffer` is an in/out param updated to the target page,
/// so it is taken `&mut` and the placed offset is returned.
pub fn brin_doinsert(
    _idxrel: Relation,
    _pages_per_range: BlockNumber,
    _revmap: &mut BrinRevmap,
    _buffer: &mut Buffer,
    _heap_blk: BlockNumber,
    _tup: &mut BrinTuple,
    _itemsz: usize,
) -> OffsetNumber {
    unimplemented!()
}

/// Initialize a new BRIN regular page with the given type.
pub fn brin_page_init(_page: &mut Page, _type: u16) {
    unimplemented!()
}

/// Initialize a new BRIN metapage.
pub fn brin_metapage_init(_page: &mut Page, _pages_per_range: BlockNumber, _version: u16) {
    unimplemented!()
}

/// Mark a page as being evacuated; returns whether evacuation was started.
pub fn brin_start_evacuating_page(_idx_rel: Relation, _buf: Buffer) -> bool {
    unimplemented!()
}

/// Move all tuples off a page being evacuated.
pub fn brin_evacuate_page(
    _idx_rel: Relation,
    _pages_per_range: BlockNumber,
    _revmap: &mut BrinRevmap,
    _buf: Buffer,
) {
    unimplemented!()
}

/// Clean up a BRIN page (recycle if empty, mark free space).
pub fn brin_page_cleanup(_idxrel: Relation, _buf: Buffer) {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/access/brin_revmap.h
//! Prototypes for BRIN reverse range maps.
//!
//! `BrinRevmap`'s definition lives in brin_revmap.c (opaque here). All fns are
//! stubs.

use crate::access::brin_tuple::BrinTuple;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::relcache::Relation;

/// Opaque handle; definition lives in brin_revmap.c (C: incomplete struct).
pub struct BrinRevmap {
    _private: [u8; 0],
}

/// C fills `*pagesPerRange` out-param -> return it alongside the revmap.
pub fn brinRevmapInitialize(
    _idxrel: Relation,
) -> (*mut BrinRevmap, BlockNumber) {
    unimplemented!()
}
pub fn brinRevmapTerminate(_revmap: &mut BrinRevmap) {
    unimplemented!()
}

pub fn brinRevmapExtend(_revmap: &mut BrinRevmap, _heap_blk: BlockNumber) {
    unimplemented!()
}
pub fn brinLockRevmapPageForUpdate(_revmap: &mut BrinRevmap, _heap_blk: BlockNumber) -> Buffer {
    unimplemented!()
}
pub fn brinSetHeapBlockItemptr(
    _buf: Buffer,
    _pages_per_range: BlockNumber,
    _heap_blk: BlockNumber,
    _tid: ItemPointerData,
) {
    unimplemented!()
}
/// C returns NULL when no tuple for the block (-> `Option`), and fills the
/// `buf`/`off`/`size` out-params. `mode` is a buffer-lock mode (int).
pub fn brinGetTupleForHeapBlock(
    _revmap: &mut BrinRevmap,
    _heap_blk: BlockNumber,
    _mode: i32,
) -> Option<(*mut BrinTuple, Buffer, OffsetNumber, usize)> {
    unimplemented!()
}
pub fn brinRevmapDesummarizeRange(_idxrel: Relation, _heap_blk: BlockNumber) -> bool {
    unimplemented!()
}

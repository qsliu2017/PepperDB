//! Translated from PostgreSQL src/include/access/visibilitymap.h

use crate::access::visibilitymapdefs::VisibilityMapFlags;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::utils::rel::RelationData;

/// Test for the ALL_VISIBLE bit. vmbuf is pinned/updated in place.
pub fn VM_ALL_VISIBLE(r: &RelationData, b: BlockNumber, v: &mut Buffer) -> bool {
    visibilitymap_get_status(r, b, v).contains(VisibilityMapFlags::ALL_VISIBLE)
}
/// Test for the ALL_FROZEN bit. vmbuf is pinned/updated in place.
pub fn VM_ALL_FROZEN(r: &RelationData, b: BlockNumber, v: &mut Buffer) -> bool {
    visibilitymap_get_status(r, b, v).contains(VisibilityMapFlags::ALL_FROZEN)
}

pub fn visibilitymap_clear(
    _rel: &RelationData,
    _heapBlk: BlockNumber,
    _vmbuf: Buffer,
    _flags: VisibilityMapFlags,
) -> bool {
    unimplemented!()
}
pub fn visibilitymap_pin(_rel: &RelationData, _heapBlk: BlockNumber, _vmbuf: &mut Buffer) {
    unimplemented!()
}
pub fn visibilitymap_pin_ok(_heapBlk: BlockNumber, _vmbuf: Buffer) -> bool {
    unimplemented!()
}
pub fn visibilitymap_set(
    _rel: &RelationData,
    _heapBlk: BlockNumber,
    _heapBuf: Buffer,
    _recptr: XLogRecPtr,
    _vmBuf: Buffer,
    _cutoff_xid: TransactionId,
    _flags: VisibilityMapFlags,
) -> VisibilityMapFlags {
    unimplemented!()
}
pub fn visibilitymap_get_status(
    _rel: &RelationData,
    _heapBlk: BlockNumber,
    _vmbuf: &mut Buffer,
) -> VisibilityMapFlags {
    unimplemented!()
}
/// Returns (all_visible, all_frozen) counts (out-params folded into a tuple).
pub fn visibilitymap_count(_rel: &RelationData) -> (BlockNumber, BlockNumber) {
    unimplemented!()
}
pub fn visibilitymap_prepare_truncate(_rel: &RelationData, _nheapblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}
pub fn visibilitymap_truncation_length(_nheapblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}

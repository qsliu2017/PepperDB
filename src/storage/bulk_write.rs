//! Translated from PostgreSQL src/include/storage/bulk_write.h

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;

// Bulk writer state, contents private to bulk_write.c.
pub struct BulkWriteState {
    _private: (),
}

#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::rel::Relation in Phase 2")]
pub struct Relation; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::smgr::SMgrRelationData in Phase 2")]
pub struct SMgrRelationData; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::PGIOAlignedBlock in Phase 2")]
pub struct PGIOAlignedBlock; // TODO(struct-forward)

// Temporary page-sized buffer reserved via smgr_bulk_get_buf.
pub type BulkWriteBuffer = *mut PGIOAlignedBlock; // TODO(ptr)

#[allow(deprecated)]
pub fn smgr_bulk_start_rel(_rel: &Relation, _forknum: ForkNumber) -> BulkWriteState {
    unimplemented!()
}

#[allow(deprecated)]
pub fn smgr_bulk_start_smgr(
    _smgr: &SMgrRelationData,
    _forknum: ForkNumber,
    _use_wal: bool,
) -> BulkWriteState {
    unimplemented!()
}

pub fn smgr_bulk_get_buf(_bulkstate: &mut BulkWriteState) -> BulkWriteBuffer {
    unimplemented!()
}

pub fn smgr_bulk_write(
    _bulkstate: &mut BulkWriteState,
    _blocknum: BlockNumber,
    _buf: BulkWriteBuffer,
    _page_std: bool,
) {
    unimplemented!()
}

pub fn smgr_bulk_finish(_bulkstate: &mut BulkWriteState) {
    unimplemented!()
}

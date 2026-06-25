//! Translated from PostgreSQL src/include/storage/bulk_write.h

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::utils::rel::Relation;

// Bulk writer state, contents private to bulk_write.c.
pub struct BulkWriteState {
    _private: (),
}

/// Opaque; smgr relation cache not ported yet.
pub struct SMgrRelationData;
/// Opaque; page-aligned IO buffer union not ported yet.
pub struct PGIOAlignedBlock;

// Temporary page-sized buffer reserved via smgr_bulk_get_buf.
pub type BulkWriteBuffer = *mut PGIOAlignedBlock; // TODO(ptr)

pub fn smgr_bulk_start_rel(_rel: &Relation, _forknum: ForkNumber) -> BulkWriteState {
    unimplemented!()
}

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

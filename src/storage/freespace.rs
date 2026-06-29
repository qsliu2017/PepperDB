//! Translated from PostgreSQL src/include/storage/freespace.h
//!
//! POSTGRES free space map for quickly finding free space in relations.

use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;
use std::sync::Arc;
use crate::utils::rel::RelationData;

// The real logic lives in `backend::storage::freespace::freespace` (smgr-level
// args, async, reads the FSM fork through the buffer manager). These C-named,
// `Arc<RelationData>`-based shims stay `unimplemented!()` until the relcache
// (`RelationGetSmgr`) is wired; new code calls the backend functions directly.

#[deprecated(note = "use `backend::storage::freespace::freespace::get_recorded_free_space`")]
pub fn GetRecordedFreeSpace(_rel: &RelationData, _heap_blk: BlockNumber) -> usize {
    unimplemented!("use backend::storage::freespace::freespace::get_recorded_free_space")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::get_page_with_free_space`")]
pub fn GetPageWithFreeSpace(_rel: &RelationData, _space_needed: usize) -> Option<BlockNumber> {
    unimplemented!("use backend::storage::freespace::freespace::get_page_with_free_space")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::record_and_get_page_with_free_space`")]
pub fn RecordAndGetPageWithFreeSpace(
    _rel: &RelationData,
    _old_page: BlockNumber,
    _old_space_avail: usize,
    _space_needed: usize,
) -> Option<BlockNumber> {
    unimplemented!("use backend::storage::freespace::freespace::record_and_get_page_with_free_space")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::record_page_with_free_space`")]
pub fn RecordPageWithFreeSpace(_rel: &RelationData, _heap_blk: BlockNumber, _space_avail: usize) {
    unimplemented!("use backend::storage::freespace::freespace::record_page_with_free_space")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::xlog_record_page_with_free_space`")]
pub fn XLogRecordPageWithFreeSpace(
    _rlocator: RelFileLocator,
    _heap_blk: BlockNumber,
    _space_avail: usize,
) {
    unimplemented!("use backend::storage::freespace::freespace::xlog_record_page_with_free_space")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::free_space_map_prepare_truncate_rel`")]
pub fn FreeSpaceMapPrepareTruncateRel(_rel: &RelationData, _nblocks: BlockNumber) -> BlockNumber {
    unimplemented!("use backend::storage::freespace::freespace::free_space_map_prepare_truncate_rel")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::free_space_map_vacuum`")]
pub fn FreeSpaceMapVacuum(_rel: &RelationData) {
    unimplemented!("use backend::storage::freespace::freespace::free_space_map_vacuum")
}

#[deprecated(note = "use `backend::storage::freespace::freespace::free_space_map_vacuum_range`")]
pub fn FreeSpaceMapVacuumRange(_rel: &RelationData, _start: BlockNumber, _end: BlockNumber) {
    unimplemented!("use backend::storage::freespace::freespace::free_space_map_vacuum_range")
}

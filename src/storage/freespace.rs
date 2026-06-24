//! Translated from PostgreSQL src/include/storage/freespace.h
//!
//! POSTGRES free space map for quickly finding free space in relations.

use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::relcache::Relation;

pub fn GetRecordedFreeSpace(_rel: Relation, _heap_blk: BlockNumber) -> usize {
    unimplemented!()
}

/// InvalidBlockNumber (no page with enough space) -> None.
pub fn GetPageWithFreeSpace(_rel: Relation, _space_needed: usize) -> Option<BlockNumber> {
    unimplemented!()
}

/// InvalidBlockNumber (no page with enough space) -> None.
pub fn RecordAndGetPageWithFreeSpace(
    _rel: Relation,
    _old_page: BlockNumber,
    _old_space_avail: usize,
    _space_needed: usize,
) -> Option<BlockNumber> {
    unimplemented!()
}

pub fn RecordPageWithFreeSpace(_rel: Relation, _heap_blk: BlockNumber, _space_avail: usize) {
    unimplemented!()
}

pub fn XLogRecordPageWithFreeSpace(
    _rlocator: RelFileLocator,
    _heap_blk: BlockNumber,
    _space_avail: usize,
) {
    unimplemented!()
}

pub fn FreeSpaceMapPrepareTruncateRel(_rel: Relation, _nblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}

pub fn FreeSpaceMapVacuum(_rel: Relation) {
    unimplemented!()
}

pub fn FreeSpaceMapVacuumRange(_rel: Relation, _start: BlockNumber, _end: BlockNumber) {
    unimplemented!()
}

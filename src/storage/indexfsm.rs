//! Translated from PostgreSQL src/include/storage/indexfsm.h
//!
//! POSTGRES free space map for quickly finding an unused page in index.

use crate::storage::block::BlockNumber;
use crate::utils::relcache::Relation;

/// InvalidBlockNumber (no free page available) -> None.
pub fn GetFreeIndexPage(_rel: Relation) -> Option<BlockNumber> {
    unimplemented!()
}

pub fn RecordFreeIndexPage(_rel: Relation, _free_block: BlockNumber) {
    unimplemented!()
}

pub fn RecordUsedIndexPage(_rel: Relation, _used_block: BlockNumber) {
    unimplemented!()
}

pub fn IndexFreeSpaceMapVacuum(_rel: Relation) {
    unimplemented!()
}

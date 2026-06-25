//! Translated from PostgreSQL src/include/storage/indexfsm.h
//!
//! POSTGRES free space map for quickly finding an unused page in index.

use crate::storage::block::BlockNumber;
use crate::utils::relcache::Relation;

// The real logic lives in `backend::storage::freespace::indexfsm` (smgr-level
// args, async). These `Relation`-based C-named shims stay `unimplemented!()`
// until the relcache (`RelationGetSmgr`) is wired; new code calls the backend
// functions directly.

#[deprecated(note = "use `backend::storage::freespace::indexfsm::get_free_index_page`")]
pub fn GetFreeIndexPage(_rel: Relation) -> Option<BlockNumber> {
    unimplemented!("use backend::storage::freespace::indexfsm::get_free_index_page")
}

#[deprecated(note = "use `backend::storage::freespace::indexfsm::record_free_index_page`")]
pub fn RecordFreeIndexPage(_rel: Relation, _free_block: BlockNumber) {
    unimplemented!("use backend::storage::freespace::indexfsm::record_free_index_page")
}

#[deprecated(note = "use `backend::storage::freespace::indexfsm::record_used_index_page`")]
pub fn RecordUsedIndexPage(_rel: Relation, _used_block: BlockNumber) {
    unimplemented!("use backend::storage::freespace::indexfsm::record_used_index_page")
}

#[deprecated(note = "use `backend::storage::freespace::indexfsm::index_free_space_map_vacuum`")]
pub fn IndexFreeSpaceMapVacuum(_rel: Relation) {
    unimplemented!("use backend::storage::freespace::indexfsm::index_free_space_map_vacuum")
}

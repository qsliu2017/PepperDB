//! Translated from PostgreSQL src/include/catalog/storage.h

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::smgr::SmgrRelation;

// GUC variable (process-global today; becomes session/config state in Phase 2).
pub static mut wal_skip_threshold: i32 = 0;

pub fn RelationCreateStorage(
    rlocator: RelFileLocator,
    relpersistence: u8,
    register_delete: bool,
) -> &'static mut SmgrRelation {
    unimplemented!()
}

pub fn RelationDropStorage(rel: &crate::utils::rel::RelationData) {
    unimplemented!()
}

pub fn RelationPreserveStorage(rlocator: RelFileLocator, at_commit: bool) {
    unimplemented!()
}

pub fn RelationPreTruncate(rel: &crate::utils::rel::RelationData) {
    unimplemented!()
}

pub fn RelationTruncate(rel: &crate::utils::rel::RelationData, nblocks: BlockNumber) {
    unimplemented!()
}

pub fn RelationCopyStorage(
    src: &mut SmgrRelation,
    dst: &mut SmgrRelation,
    fork_num: ForkNumber,
    relpersistence: u8,
) {
    unimplemented!()
}

pub fn RelFileLocatorSkippingWAL(rlocator: RelFileLocator) -> bool {
    unimplemented!()
}

pub fn EstimatePendingSyncsSpace() -> usize {
    unimplemented!()
}

pub fn SerializePendingSyncs(max_size: usize, start_address: &mut [u8]) {
    unimplemented!()
}

pub fn RestorePendingSyncs(start_address: &[u8]) {
    unimplemented!()
}

// These functions used to be in storage/smgr/smgr.c, which explains the naming.

pub fn smgrDoPendingDeletes(is_commit: bool) {
    unimplemented!()
}

pub fn smgrDoPendingSyncs(is_commit: bool, is_parallel_worker: bool) {
    unimplemented!()
}

/// C: returns count + fills `RelFileLocator **ptr` out-array -> the list itself.
pub fn smgrGetPendingDeletes(for_commit: bool) -> Vec<RelFileLocator> {
    unimplemented!()
}

pub fn AtSubCommit_smgr() {
    unimplemented!()
}

pub fn AtSubAbort_smgr() {
    unimplemented!()
}

pub fn PostPrepare_smgr() {
    unimplemented!()
}

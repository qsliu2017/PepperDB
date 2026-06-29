//! Translated from PostgreSQL src/include/access/syncscan.h

use crate::storage::block::BlockNumber;
use crate::utils::rel::RelationData;

pub fn ss_report_location(_rel: &RelationData, _location: BlockNumber) {
    unimplemented!()
}
pub fn ss_get_location(_rel: &RelationData, _relnblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}
pub fn SyncScanShmemInit() {
    unimplemented!()
}
pub fn SyncScanShmemSize() -> usize {
    unimplemented!()
}

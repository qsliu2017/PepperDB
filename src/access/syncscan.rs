//! Translated from PostgreSQL src/include/access/syncscan.h

use crate::storage::block::BlockNumber;
use crate::utils::relcache::Relation;

pub fn ss_report_location(_rel: Relation, _location: BlockNumber) {
    unimplemented!()
}
pub fn ss_get_location(_rel: Relation, _relnblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}
pub fn SyncScanShmemInit() {
    unimplemented!()
}
pub fn SyncScanShmemSize() -> usize {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/access/xlogbackup.h

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::pg_config_manual::MAXPGPATH;
use crate::pgtime::pg_time_t;

/// In-memory backup state.
#[derive(Debug, Clone)]
pub struct BackupState {
    /// Backup label name; one extra byte for null-termination.
    pub name: [u8; MAXPGPATH + 1],
    pub startpoint: XLogRecPtr,
    pub starttli: TimeLineID,
    pub checkpointloc: XLogRecPtr,
    pub starttime: pg_time_t,
    pub started_in_recovery: bool,
    pub istartpoint: XLogRecPtr,
    pub istarttli: TimeLineID,
    pub stoppoint: XLogRecPtr,
    pub stoptli: TimeLineID,
    pub stoptime: pg_time_t,
}

pub fn build_backup_content(_state: &BackupState, _ishistoryfile: bool) -> String {
    unimplemented!()
}

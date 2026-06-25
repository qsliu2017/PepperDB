//! Translated from PostgreSQL src/include/backup/basebackup.h

use crate::backup::basebackup_incremental::IncrementalBackupInfo;
use crate::nodes::replnodes::BaseBackupCmd;
use crate::postgres_ext::Oid;

/// Minimum and maximum values of MAX_RATE option in BASE_BACKUP command.
pub const MAX_RATE_LOWER: i32 = 32;
pub const MAX_RATE_UPPER: i32 = 1048576;

/// Information about a tablespace.
///
/// In some usages, `path` can be `None` to denote the PGDATA directory itself.
pub struct TablespaceInfo {
    pub oid: Oid,
    pub path: Option<String>,
    /// Relative path if it's within PGDATA, else `None`.
    pub rpath: Option<String>,
    /// Total size as sent; -1 if not known.
    pub size: i64,
}

pub fn SendBaseBackup(_cmd: &BaseBackupCmd, _ib: Option<&IncrementalBackupInfo>) {
    unimplemented!()
}

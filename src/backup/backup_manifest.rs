//! Translated from PostgreSQL src/include/backup/backup_manifest.h

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::backup::basebackup_sink::Bbsink;
use crate::common::checksum_helper::{pg_checksum_context, pg_checksum_type};
use crate::common::cryptohash::PgCryptohashCtx;
use crate::pgtime::pg_time_t;
use crate::postgres_ext::Oid;
use crate::storage::buffile::BufFile;

pub enum backup_manifest_option {
    Yes,
    No,
    ForceEncode,
}

pub struct backup_manifest_info {
    pub buffile: Option<Box<BufFile>>,
    pub checksum_type: pg_checksum_type,
    pub manifest_ctx: Option<Box<PgCryptohashCtx>>,
    pub manifest_size: u64,
    pub force_encode: bool,
    pub first_file: bool,
    pub still_checksumming: bool,
}

pub fn InitializeBackupManifest(
    _manifest: &mut backup_manifest_info,
    _want_manifest: backup_manifest_option,
    _manifest_checksum_type: pg_checksum_type,
) {
    unimplemented!()
}

pub fn AddFileToBackupManifest(
    _manifest: &mut backup_manifest_info,
    _spcoid: Oid,
    _pathname: &str,
    _size: usize,
    _mtime: pg_time_t,
    _checksum_ctx: &mut pg_checksum_context,
) {
    unimplemented!()
}

pub fn AddWALInfoToBackupManifest(
    _manifest: &mut backup_manifest_info,
    _startptr: XLogRecPtr,
    _starttli: TimeLineID,
    _endptr: XLogRecPtr,
    _endtli: TimeLineID,
) {
    unimplemented!()
}

pub fn SendBackupManifest(_manifest: &mut backup_manifest_info, _sink: &mut Bbsink) {
    unimplemented!()
}

pub fn FreeBackupManifest(_manifest: &mut backup_manifest_info) {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/backup/basebackup_incremental.h
//
// API for incremental backup support. IncrementalBackupInfo is opaque (defined
// in the .c); modeled here as an opaque struct + stubbed entry points.

use crate::access::xlogbackup::BackupState;
use crate::common::relpath::{ForkNumber, RelFileNumber};
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::utils::palloc::MemoryContext;

pub const INCREMENTAL_MAGIC: u32 = 0xd3ae1f0d;

/// How a file is to be backed up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBackupMethod {
    FULLY,
    INCREMENTALLY,
}

/// Opaque manifest-tracking state (defined in basebackup_incremental.c).
pub struct IncrementalBackupInfo {
    _opaque: (),
}

pub fn CreateIncrementalBackupInfo(_mcxt: MemoryContext) -> Box<IncrementalBackupInfo> {
    unimplemented!()
}

pub fn AppendIncrementalManifestData(_ib: &mut IncrementalBackupInfo, _data: &[u8]) {
    unimplemented!()
}

pub fn FinalizeIncrementalManifest(_ib: &mut IncrementalBackupInfo) {
    unimplemented!()
}

pub fn PrepareForIncrementalBackup(_ib: &mut IncrementalBackupInfo, _backup_state: &mut BackupState) {
    unimplemented!()
}

pub fn GetIncrementalFilePath(
    _dboid: Oid,
    _spcoid: Oid,
    _relfilenumber: RelFileNumber,
    _forknum: ForkNumber,
    _segno: u32,
) -> String {
    unimplemented!()
}

/// Result of classifying one file for incremental backup. The C signature fills
/// `num_blocks_required`, `relative_block_numbers[]`, and `truncation_block_length`
/// out-params alongside the returned method; folded into a struct here.
pub struct FileBackupDecision {
    pub method: FileBackupMethod,
    pub num_blocks_required: u32,
    pub relative_block_numbers: Vec<BlockNumber>,
    pub truncation_block_length: u32,
}

pub fn GetFileBackupMethod(
    _ib: &mut IncrementalBackupInfo,
    _path: &str,
    _dboid: Oid,
    _spcoid: Oid,
    _relfilenumber: RelFileNumber,
    _forknum: ForkNumber,
    _segno: u32,
    _size: usize,
) -> FileBackupDecision {
    unimplemented!()
}

pub fn GetIncrementalFileSize(_num_blocks_required: u32) -> usize {
    unimplemented!()
}

pub fn GetIncrementalHeaderSize(_num_blocks_required: u32) -> usize {
    unimplemented!()
}

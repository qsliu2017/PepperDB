//! Translated from PostgreSQL src/include/utils/relmapper.h
//! Catalog-to-filenumber mapping.

use crate::access::xlogreader::XLogReaderState;
use crate::common::relpath::RelFileNumber;
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;

/// relmap-related XLOG entry opcode (single rmgr info code, not a flag set).
pub const XLOG_RELMAP_UPDATE: u8 = 0x00;

/// On-disk WAL record. Fixed header followed by `nbytes` of relmap `data`.
/// (FAM `char data[]` lives in the WAL buffer past the header.)
#[repr(C)]
pub struct xl_relmap_update {
    /// database ID, or 0 for shared map
    pub dbid: Oid,
    /// database's tablespace, or pg_global
    pub tsid: Oid,
    /// size of relmap data
    pub nbytes: i32,
    // FAM: char data[nbytes]
}

/// C: `MinSizeOfRelmapUpdate = offsetof(xl_relmap_update, data)`.
pub const MIN_SIZE_OF_RELMAP_UPDATE: usize = core::mem::size_of::<xl_relmap_update>();

const _: () = assert!(core::mem::size_of::<xl_relmap_update>() == 12);
const _: () = assert!(core::mem::offset_of!(xl_relmap_update, nbytes) == 8);

/// InvalidOid when no mapping is present.
pub fn RelationMapOidToFilenumber(_relation_id: Oid, _shared: bool) -> Option<RelFileNumber> {
    unimplemented!()
}

pub fn RelationMapFilenumberToOid(_filenumber: RelFileNumber, _shared: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn RelationMapOidToFilenumberForDatabase(
    _dbpath: &str,
    _relation_id: Oid,
) -> Option<RelFileNumber> {
    unimplemented!()
}

pub fn RelationMapCopy(_dbid: Oid, _tsid: Oid, _srcdbpath: &str, _dstdbpath: &str) {
    unimplemented!()
}

pub fn RelationMapUpdateMap(
    _relation_id: Oid,
    _file_number: RelFileNumber,
    _shared: bool,
    _immediate: bool,
) {
    unimplemented!()
}

pub fn RelationMapRemoveMapping(_relation_id: Oid) {
    unimplemented!()
}

pub fn RelationMapInvalidate(_shared: bool) {
    unimplemented!()
}
pub fn RelationMapInvalidateAll() {
    unimplemented!()
}

pub fn AtCCI_RelationMap() {
    unimplemented!()
}
pub fn AtEOXact_RelationMap(_is_commit: bool, _is_parallel_worker: bool) {
    unimplemented!()
}
pub fn AtPrepare_RelationMap() {
    unimplemented!()
}

pub fn CheckPointRelationMap() {
    unimplemented!()
}

pub fn RelationMapFinishBootstrap() {
    unimplemented!()
}

pub fn RelationMapInitialize() {
    unimplemented!()
}
pub fn RelationMapInitializePhase2() {
    unimplemented!()
}
pub fn RelationMapInitializePhase3() {
    unimplemented!()
}

pub fn EstimateRelationMapSpace() -> usize {
    unimplemented!()
}
pub fn SerializeRelationMap(_max_size: usize, _start_address: &mut [u8]) {
    unimplemented!()
}
pub fn RestoreRelationMap(_start_address: &[u8]) {
    unimplemented!()
}

pub fn relmap_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn relmap_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn relmap_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}

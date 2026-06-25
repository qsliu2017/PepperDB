//! Translated from PostgreSQL src/include/access/multixact.h

use crate::access::xlogreader::XLogReaderState;
use crate::c::{MultiXactId, MultiXactOffset, TransactionId, InvalidMultiXactId, FirstMultiXactId};
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::sync::FileTag;

// InvalidMultiXactId / FirstMultiXactId are exported by crate::c (canonical).
pub const MaxMultiXactId: MultiXactId = TransactionId(0xFFFFFFFF);

pub const fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi.0 != InvalidMultiXactId.0
}

pub const MaxMultiXactOffset: MultiXactOffset = MultiXactOffset(0xFFFFFFFF);

/// MultiXact lock modes ("status"). First four are tuple locks; last two are
/// update/delete modes. Sequential ordinal enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum MultiXactStatus {
    ForKeyShare = 0x00,
    ForShare = 0x01,
    ForNoKeyUpdate = 0x02,
    ForUpdate = 0x03,
    /// an update that doesn't touch "key" columns
    NoKeyUpdate = 0x04,
    /// other updates, and delete
    Update = 0x05,
}

pub const MaxMultiXactStatus: MultiXactStatus = MultiXactStatus::Update;

/// Does a status value correspond to a tuple update?
pub const fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    (status as i32) > (MultiXactStatus::ForUpdate as i32)
}

#[derive(Debug, Clone, Copy)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

// multixact-related XLOG opcodes (info nibble): raw consts.
pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

/// FAM `members: [MultiXactMember]` follows the header in the WAL buffer.
#[repr(C)]
pub struct xl_multixact_create {
    pub mid: MultiXactId,       // new MultiXact's ID
    pub moff: MultiXactOffset,  // its starting offset in members file
    pub nmembers: i32,          // number of member XIDs
    // FAM: members: [MultiXactMember]
}
pub const SizeOfMultiXactCreate: usize = core::mem::size_of::<xl_multixact_create>();

#[repr(C)]
pub struct xl_multixact_truncate {
    pub oldestMultiDB: Oid,
    // to-be-truncated range of multixact offsets
    pub startTruncOff: MultiXactId, // just for completeness' sake
    pub endTruncOff: MultiXactId,
    // to-be-truncated range of multixact members
    pub startTruncMemb: MultiXactOffset,
    pub endTruncMemb: MultiXactOffset,
}
pub const SizeOfMultiXactTruncate: usize = core::mem::size_of::<xl_multixact_truncate>();

pub fn MultiXactIdCreate(
    _xid1: TransactionId,
    _status1: MultiXactStatus,
    _xid2: TransactionId,
    _status2: MultiXactStatus,
) -> MultiXactId {
    unimplemented!()
}
pub fn MultiXactIdExpand(
    _multi: MultiXactId,
    _xid: TransactionId,
    _status: MultiXactStatus,
) -> MultiXactId {
    unimplemented!()
}
pub fn MultiXactIdCreateFromMembers(_members: &[MultiXactMember]) -> MultiXactId {
    unimplemented!()
}

pub fn ReadNextMultiXactId() -> MultiXactId {
    unimplemented!()
}
/// Returns (oldest, next) (out-params folded into a tuple).
pub fn ReadMultiXactIdRange() -> (MultiXactId, MultiXactId) {
    unimplemented!()
}
pub fn MultiXactIdIsRunning(_multi: MultiXactId, _isLockOnly: bool) -> bool {
    unimplemented!()
}
pub fn MultiXactIdSetOldestMember() {
    unimplemented!()
}
/// Returns the member list (out-param + count folded into the return).
pub fn GetMultiXactIdMembers(
    _multi: MultiXactId,
    _from_pgupgrade: bool,
    _isLockOnly: bool,
) -> Vec<MultiXactMember> {
    unimplemented!()
}
pub fn MultiXactIdPrecedes(_multi1: MultiXactId, _multi2: MultiXactId) -> bool {
    unimplemented!()
}
pub fn MultiXactIdPrecedesOrEquals(_multi1: MultiXactId, _multi2: MultiXactId) -> bool {
    unimplemented!()
}

pub fn multixactoffsetssyncfiletag(_ftag: &FileTag, _path: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn multixactmemberssyncfiletag(_ftag: &FileTag, _path: &mut [u8]) -> i32 {
    unimplemented!()
}

pub fn AtEOXact_MultiXact() {
    unimplemented!()
}
pub fn AtPrepare_MultiXact() {
    unimplemented!()
}
pub fn PostPrepare_MultiXact(_xid: TransactionId) {
    unimplemented!()
}

pub fn MultiXactShmemSize() -> usize {
    unimplemented!()
}
pub fn MultiXactShmemInit() {
    unimplemented!()
}
pub fn BootStrapMultiXact() {
    unimplemented!()
}
pub fn StartupMultiXact() {
    unimplemented!()
}
pub fn TrimMultiXact() {
    unimplemented!()
}
pub fn SetMultiXactIdLimit(
    _oldest_datminmxid: MultiXactId,
    _oldest_datoid: Oid,
    _is_startup: bool,
) {
    unimplemented!()
}
/// Checkpoint multixact state (out-params folded into a struct).
pub fn MultiXactGetCheckptMulti(_is_shutdown: bool) -> MultiXactCheckpoint {
    unimplemented!()
}

/// Result of MultiXactGetCheckptMulti (C out-params).
pub struct MultiXactCheckpoint {
    pub nextMulti: MultiXactId,
    pub nextMultiOffset: MultiXactOffset,
    pub oldestMulti: MultiXactId,
    pub oldestMultiDB: Oid,
}

pub fn CheckPointMultiXact() {
    unimplemented!()
}
pub fn GetOldestMultiXactId() -> MultiXactId {
    unimplemented!()
}
pub fn TruncateMultiXact(_newOldestMulti: MultiXactId, _newOldestMultiDB: Oid) {
    unimplemented!()
}
pub fn MultiXactSetNextMXact(_nextMulti: MultiXactId, _nextMultiOffset: MultiXactOffset) {
    unimplemented!()
}
pub fn MultiXactAdvanceNextMXact(_minMulti: MultiXactId, _minMultiOffset: MultiXactOffset) {
    unimplemented!()
}
pub fn MultiXactAdvanceOldest(_oldestMulti: MultiXactId, _oldestMultiDB: Oid) {
    unimplemented!()
}
pub fn MultiXactMemberFreezeThreshold() -> i32 {
    unimplemented!()
}

pub fn multixact_twophase_recover(_xid: TransactionId, _info: u16, _recdata: &[u8], _len: u32) {
    unimplemented!()
}
pub fn multixact_twophase_postcommit(_xid: TransactionId, _info: u16, _recdata: &[u8], _len: u32) {
    unimplemented!()
}
pub fn multixact_twophase_postabort(_xid: TransactionId, _info: u16, _recdata: &[u8], _len: u32) {
    unimplemented!()
}

pub fn multixact_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn multixact_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn multixact_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn mxid_to_string(_multi: MultiXactId, _members: &[MultiXactMember]) -> String {
    unimplemented!()
}

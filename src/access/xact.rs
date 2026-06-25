//! Translated from PostgreSQL src/include/access/xact.h

use bitflags::bitflags;

use crate::access::transam::FullTransactionId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::c::{CommandId, SubTransactionId, TransactionId};
use crate::datatype::timestamp::TimestampTz;
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::sinval::SharedInvalidationMessage;

/// Maximum size of Global Transaction ID (including '\0').
pub const GIDSIZE: usize = 200;

// Xact isolation levels (ordinals).
pub const XACT_READ_UNCOMMITTED: i32 = 0;
pub const XACT_READ_COMMITTED: i32 = 1;
pub const XACT_REPEATABLE_READ: i32 = 2;
pub const XACT_SERIALIZABLE: i32 = 3;

// GUC / session globals. TODO(global-state): move to a threaded Session.
pub static mut DefaultXactIsoLevel: i32 = 0;
pub static mut XactIsoLevel: i32 = 0;
pub static mut DefaultXactReadOnly: bool = false;
pub static mut XactReadOnly: bool = false;
pub static mut xact_is_sampled: bool = false;
pub static mut DefaultXactDeferrable: bool = false;
pub static mut XactDeferrable: bool = false;
pub static mut synchronous_commit: i32 = 0;
pub static mut CheckXidAlive: TransactionId = TransactionId(0);
pub static mut bsysscan: bool = false;
pub static mut MyXactFlags: i32 = 0;

/// XactIsoLevel >= REPEATABLE_READ uses one snapshot per transaction.
pub fn IsolationUsesXactSnapshot() -> bool {
    unsafe { XactIsoLevel >= XACT_REPEATABLE_READ }
}
pub fn IsolationIsSerializable() -> bool {
    unsafe { XactIsoLevel == XACT_SERIALIZABLE }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SyncCommitLevel {
    Off = 0,            // asynchronous commit
    LocalFlush,         // wait for local flush only
    RemoteWrite,        // local flush + remote write
    RemoteFlush,        // local + remote flush
    RemoteApply,        // local + remote flush + remote apply
}
/// Default setting for synchronous_commit.
pub const SYNCHRONOUS_COMMIT_ON: SyncCommitLevel = SyncCommitLevel::RemoteFlush;

bitflags! {
    /// MyXactFlags top-level event bits (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XactFlags: u32 {
        const ACCESSEDTEMPNAMESPACE       = 1 << 0;
        const ACQUIREDACCESSEXCLUSIVELOCK = 1 << 1;
        const NEEDIMMEDIATECOMMIT         = 1 << 2;
        const PIPELINING                  = 1 << 3;
    }
}
pub const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: u32 = 1 << 0;
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: u32 = 1 << 1;
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: u32 = 1 << 2;
pub const XACT_FLAGS_PIPELINING: u32 = 1 << 3;

/// Start/end-of-transaction callback events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum XactEvent {
    Commit,
    ParallelCommit,
    Abort,
    ParallelAbort,
    Prepare,
    PreCommit,
    ParallelPreCommit,
    PrePrepare,
}

/// Xact callback. The C `void *arg` is captured by the closure (see Register).
pub type XactCallback = fn(event: XactEvent);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SubXactEvent {
    StartSub,
    CommitSub,
    AbortSub,
    PreCommitSub,
}

pub type SubXactCallback =
    fn(event: SubXactEvent, my_subid: SubTransactionId, parent_subid: SubTransactionId);

/// Saved characteristics for Save/RestoreTransactionCharacteristics.
pub struct SavedTransactionCharacteristics {
    pub XactIsoLevel: i32,
    pub XactReadOnly: bool,
    pub XactDeferrable: bool,
}

// transaction-related XLOG opcodes (info high nibble): raw consts.
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: u8 = 0x60;

pub const XLOG_XACT_OPMASK: u8 = 0x70;
/// Does this record have a 'xinfo' field?
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

bitflags! {
    /// xinfo flags determining commit/abort record contents (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XactXinfo: u32 {
        const HAS_DBINFO        = 1 << 0;
        const HAS_SUBXACTS      = 1 << 1;
        const HAS_RELFILELOCATORS = 1 << 2;
        const HAS_INVALS        = 1 << 3;
        const HAS_TWOPHASE      = 1 << 4;
        const HAS_ORIGIN        = 1 << 5;
        const HAS_AE_LOCKS      = 1 << 6;
        const HAS_GID           = 1 << 7;
        const HAS_DROPPED_STATS = 1 << 8;
        // XactCompletion* recovery-action bits (also stored in xinfo).
        const COMPLETION_APPLY_FEEDBACK       = 1 << 29;
        const COMPLETION_UPDATE_RELCACHE_FILE = 1 << 30;
        const COMPLETION_FORCE_SYNC_COMMIT    = 1 << 31;
    }
}
pub const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1 << 6;
pub const XACT_XINFO_HAS_GID: u32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;
pub const XACT_COMPLETION_APPLY_FEEDBACK: u32 = 1 << 29;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: u32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: u32 = 1 << 31;

pub const fn XactCompletionApplyFeedback(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_APPLY_FEEDBACK != 0
}
pub const fn XactCompletionRelcacheInitFileInval(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE != 0
}
pub const fn XactCompletionForceSyncCommit(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT != 0
}

/// FAM `xsub: [TransactionId]` assigned subxids.
#[repr(C)]
pub struct xl_xact_assignment {
    pub xtop: TransactionId, // assigned XID's top-level XID
    pub nsubxacts: i32,      // number of subtransaction XIDs
    // FAM: xsub: [TransactionId]
}
pub const MinSizeOfXactAssignment: usize = core::mem::size_of::<xl_xact_assignment>();

// sub-records for commit/abort (all on-disk, int32-aligned).
#[repr(C)]
pub struct xl_xact_xinfo {
    pub xinfo: u32,
}

#[repr(C)]
pub struct xl_xact_dbinfo {
    pub dbId: Oid, // MyDatabaseId
    pub tsId: Oid, // MyDatabaseTableSpace
}

#[repr(C)]
pub struct xl_xact_subxacts {
    pub nsubxacts: i32,
    // FAM: subxacts: [TransactionId]
}
pub const MinSizeOfXactSubxacts: usize = core::mem::size_of::<xl_xact_subxacts>();

#[repr(C)]
pub struct xl_xact_relfilelocators {
    pub nrels: i32,
    // FAM: xlocators: [RelFileLocator]
}
pub const MinSizeOfXactRelfileLocators: usize = core::mem::size_of::<xl_xact_relfilelocators>();

/// A transactionally dropped statistics entry (WAL-readable by frontend).
#[repr(C)]
pub struct xl_xact_stats_item {
    pub kind: i32,
    pub dboid: Oid,
    /// PgStat_HashKey.objid split into two uint32 for int alignment.
    pub objid_lo: u32,
    pub objid_hi: u32,
}

#[repr(C)]
pub struct xl_xact_stats_items {
    pub nitems: i32,
    // FAM: items: [xl_xact_stats_item]
}
pub const MinSizeOfXactStatsItems: usize = core::mem::size_of::<xl_xact_stats_items>();

#[repr(C)]
pub struct xl_xact_invals {
    pub nmsgs: i32,
    // FAM: msgs: [SharedInvalidationMessage]
}
pub const MinSizeOfXactInvals: usize = core::mem::size_of::<xl_xact_invals>();

#[repr(C)]
pub struct xl_xact_twophase {
    pub xid: TransactionId,
}

#[repr(C)]
pub struct xl_xact_origin {
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// Minimal commit record; optional sub-records follow per xinfo flags.
#[repr(C)]
pub struct xl_xact_commit {
    pub xact_time: TimestampTz, // time of commit
}
pub const MinSizeOfXactCommit: usize =
    core::mem::offset_of!(xl_xact_commit, xact_time) + core::mem::size_of::<TimestampTz>();

/// Minimal abort record; optional sub-records follow per xinfo flags.
#[repr(C)]
pub struct xl_xact_abort {
    pub xact_time: TimestampTz, // time of abort
}
pub const MinSizeOfXactAbort: usize = core::mem::size_of::<xl_xact_abort>();

#[repr(C)]
pub struct xl_xact_prepare {
    pub magic: u32,         // format identifier
    pub total_len: u32,     // actual file length
    pub xid: TransactionId, // original transaction XID
    pub database: Oid,      // OID of database it was in
    pub prepared_at: TimestampTz, // time of preparation
    pub owner: Oid,         // user running the transaction
    pub nsubxacts: i32,     // following subxact XIDs
    pub ncommitrels: i32,   // delete-on-commit rels
    pub nabortrels: i32,    // delete-on-abort rels
    pub ncommitstats: i32,  // stats to drop on commit
    pub nabortstats: i32,   // stats to drop on abort
    pub ninvalmsgs: i32,    // cache invalidation messages
    pub initfileinval: bool, // relcache init file needs invalidation?
    pub gidlen: u16,        // length of the GID (GID follows the header)
    pub origin_lsn: XLogRecPtr, // lsn of this record at origin node
    pub origin_timestamp: TimestampTz, // time of prepare at origin node
}

/// Deconstructed commit record (in-memory; produced by ParseCommitRecord).
pub struct xl_xact_parsed_commit {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub dbId: Oid,
    pub tsId: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<xl_xact_stats_item>,
    pub msgs: Vec<SharedInvalidationMessage>,
    pub twophase_xid: TransactionId,     // only for 2PC
    pub twophase_gid: [u8; GIDSIZE],     // only for 2PC
    pub abortlocators: Vec<RelFileLocator>, // only for 2PC
    pub abortstats: Vec<xl_xact_stats_item>, // only for 2PC
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

pub type xl_xact_parsed_prepare = xl_xact_parsed_commit;

/// Deconstructed abort record (in-memory; produced by ParseAbortRecord).
pub struct xl_xact_parsed_abort {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub dbId: Oid,
    pub tsId: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<xl_xact_stats_item>,
    pub twophase_xid: TransactionId,  // only for 2PC
    pub twophase_gid: [u8; GIDSIZE],  // only for 2PC
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

pub fn IsTransactionState() -> bool {
    unimplemented!()
}
pub fn IsAbortedTransactionBlockState() -> bool {
    unimplemented!()
}
pub fn GetTopTransactionId() -> TransactionId {
    unimplemented!()
}
/// None if no top transaction id assigned yet.
pub fn GetTopTransactionIdIfAny() -> Option<TransactionId> {
    unimplemented!()
}
pub fn GetCurrentTransactionId() -> TransactionId {
    unimplemented!()
}
pub fn GetCurrentTransactionIdIfAny() -> Option<TransactionId> {
    unimplemented!()
}
pub fn GetStableLatestTransactionId() -> TransactionId {
    unimplemented!()
}
pub fn GetCurrentSubTransactionId() -> SubTransactionId {
    unimplemented!()
}
pub fn GetTopFullTransactionId() -> FullTransactionId {
    unimplemented!()
}
pub fn GetTopFullTransactionIdIfAny() -> Option<FullTransactionId> {
    unimplemented!()
}
pub fn GetCurrentFullTransactionId() -> FullTransactionId {
    unimplemented!()
}
pub fn GetCurrentFullTransactionIdIfAny() -> Option<FullTransactionId> {
    unimplemented!()
}
pub fn MarkCurrentTransactionIdLoggedIfAny() {
    unimplemented!()
}
pub fn SubTransactionIsActive(_subxid: SubTransactionId) -> bool {
    unimplemented!()
}
pub fn GetCurrentCommandId(_used: bool) -> CommandId {
    unimplemented!()
}
pub fn SetParallelStartTimestamps(_xact_ts: TimestampTz, _stmt_ts: TimestampTz) {
    unimplemented!()
}
pub fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    unimplemented!()
}
pub fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    unimplemented!()
}
pub fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    unimplemented!()
}
pub fn SetCurrentStatementStartTimestamp() {
    unimplemented!()
}
pub fn GetCurrentTransactionNestLevel() -> i32 {
    unimplemented!()
}
pub fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn CommandCounterIncrement() {
    unimplemented!()
}
pub fn ForceSyncCommit() {
    unimplemented!()
}
pub fn StartTransactionCommand() {
    unimplemented!()
}
pub fn SaveTransactionCharacteristics(_s: &mut SavedTransactionCharacteristics) {
    unimplemented!()
}
pub fn RestoreTransactionCharacteristics(_s: &SavedTransactionCharacteristics) {
    unimplemented!()
}
pub fn CommitTransactionCommand() {
    unimplemented!()
}
pub fn AbortCurrentTransaction() {
    unimplemented!()
}
pub fn BeginTransactionBlock() {
    unimplemented!()
}
pub fn EndTransactionBlock(_chain: bool) -> bool {
    unimplemented!()
}
pub fn PrepareTransactionBlock(_gid: &str) -> bool {
    unimplemented!()
}
pub fn UserAbortTransactionBlock(_chain: bool) {
    unimplemented!()
}
pub fn BeginImplicitTransactionBlock() {
    unimplemented!()
}
pub fn EndImplicitTransactionBlock() {
    unimplemented!()
}
pub fn ReleaseSavepoint(_name: &str) {
    unimplemented!()
}
pub fn DefineSavepoint(_name: &str) {
    unimplemented!()
}
pub fn RollbackToSavepoint(_name: &str) {
    unimplemented!()
}
pub fn BeginInternalSubTransaction(_name: &str) {
    unimplemented!()
}
pub fn ReleaseCurrentSubTransaction() {
    unimplemented!()
}
pub fn RollbackAndReleaseCurrentSubTransaction() {
    unimplemented!()
}
pub fn IsSubTransaction() -> bool {
    unimplemented!()
}
pub fn EstimateTransactionStateSpace() -> usize {
    unimplemented!()
}
pub fn SerializeTransactionState(_maxsize: usize, _start_address: &mut [u8]) {
    unimplemented!()
}
pub fn StartParallelWorkerTransaction(_tstatespace: &[u8]) {
    unimplemented!()
}
pub fn EndParallelWorkerTransaction() {
    unimplemented!()
}
pub fn IsTransactionBlock() -> bool {
    unimplemented!()
}
pub fn IsTransactionOrTransactionBlock() -> bool {
    unimplemented!()
}
pub fn TransactionBlockStatusCode() -> i8 {
    unimplemented!()
}
pub fn AbortOutOfAnyTransaction() {
    unimplemented!()
}
pub fn PreventInTransactionBlock(_isTopLevel: bool, _stmtType: &str) {
    unimplemented!()
}
pub fn RequireTransactionBlock(_isTopLevel: bool, _stmtType: &str) {
    unimplemented!()
}
pub fn WarnNoTransactionBlock(_isTopLevel: bool, _stmtType: &str) {
    unimplemented!()
}
pub fn IsInTransactionBlock(_isTopLevel: bool) -> bool {
    unimplemented!()
}
pub fn RegisterXactCallback(_callback: XactCallback) {
    unimplemented!()
}
pub fn UnregisterXactCallback(_callback: XactCallback) {
    unimplemented!()
}
pub fn RegisterSubXactCallback(_callback: SubXactCallback) {
    unimplemented!()
}
pub fn UnregisterSubXactCallback(_callback: SubXactCallback) {
    unimplemented!()
}

pub fn IsSubxactTopXidLogPending() -> bool {
    unimplemented!()
}
pub fn MarkSubxactTopXidLogged() {
    unimplemented!()
}

/// Returns the committed children XIDs (out-param + count folded into return).
pub fn xactGetCommittedChildren() -> Vec<TransactionId> {
    unimplemented!()
}

pub fn XactLogCommitRecord(
    _commit_time: TimestampTz,
    _subxacts: &[TransactionId],
    _rels: &[RelFileLocator],
    _droppedstats: &[xl_xact_stats_item],
    _msgs: &[SharedInvalidationMessage],
    _relcacheInval: bool,
    _xactflags: i32,
    _twophase_xid: TransactionId,
    _twophase_gid: &str,
) -> XLogRecPtr {
    unimplemented!()
}

pub fn XactLogAbortRecord(
    _abort_time: TimestampTz,
    _subxacts: &[TransactionId],
    _rels: &[RelFileLocator],
    _droppedstats: &[xl_xact_stats_item],
    _xactflags: i32,
    _twophase_xid: TransactionId,
    _twophase_gid: &str,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn xact_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}

// xactdesc.c
pub fn xact_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn xact_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}

// also in xactdesc.c (shared front/backend). Parse fills the parsed struct.
pub fn ParseCommitRecord(_info: u8, _xlrec: &xl_xact_commit) -> xl_xact_parsed_commit {
    unimplemented!()
}
pub fn ParseAbortRecord(_info: u8, _xlrec: &xl_xact_abort) -> xl_xact_parsed_abort {
    unimplemented!()
}
pub fn ParsePrepareRecord(_info: u8, _xlrec: &xl_xact_prepare) -> xl_xact_parsed_prepare {
    unimplemented!()
}

pub fn EnterParallelMode() {
    unimplemented!()
}
pub fn ExitParallelMode() {
    unimplemented!()
}
pub fn IsInParallelMode() -> bool {
    unimplemented!()
}

/*-------------------------------------------------------------------------
 *
 * snapbuild.rs
 *
 *    Infrastructure for building historic catalog snapshots based on contents
 *    of the WAL, for the purpose of decoding heapam.c style values in the
 *    WAL.
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents and we
 * do so by reading and interpreting the WAL stream. The aim is to build a
 * snapshot that behaves the same as a freshly taken MVCC snapshot would have
 * at the time the XLogRecord was generated.
 *
 * To build the snapshots we reuse the infrastructure built for Hot
 * Standby. The in-memory snapshots we build look different than HS' because
 * we have different needs. To successfully decode data from the WAL we only
 * need to access catalog tables and (sys|rel|cat)cache, not the actual user
 * tables since the data we decode is wholly contained in the WAL
 * records. Also, our snapshots need to be different in comparison to normal
 * MVCC ones because in contrast to those we cannot fully rely on the clog and
 * pg_subtrans for information about committed transactions because they might
 * commit in the future from the POV of the WAL entry we're currently
 * decoding. This definition has the advantage that we only need to prevent
 * removal of catalog rows, while normal table's rows can still be
 * removed. This is achieved by using the replication slot mechanism.
 *
 * As the percentage of transactions modifying the catalog normally is fairly
 * small in comparisons to ones only manipulating user data, we keep track of
 * the committed catalog modifying ones inside [xmin, xmax) instead of keeping
 * track of all running transactions like it's done in a normal snapshot. Note
 * that we're generally only looking at transactions that have acquired an
 * xid. That is we keep a list of transactions between snapshot->(xmin, xmax)
 * that we consider committed, everything else is considered aborted/in
 * progress. That also allows us not to care about subtransactions before they
 * have committed which means this module, in contrast to HS, doesn't have to
 * care about suboverflowed subtransactions and similar.
 *
 * One complexity of doing this is that to e.g. handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate versions of the
 * catalog in a transaction. During normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with that however is that for space
 * efficiency reasons, the cmin and cmax are not included in WAL records. We
 * cannot read the cmin/cmax from the tuple itself, either, because it is
 * reset on crash recovery. Even if we could, we could not decode combocids
 * which are only tracked in the original backend's memory. To work around
 * that, heapam writes an extra WAL record (XLOG_HEAP2_NEW_CID) every time a
 * catalog row is modified, which includes the cmin and cmax of the
 * tuple. During decoding, we insert the ctid->(cmin,cmax) mappings into the
 * reorder buffer, and use them at visibility checks instead of the cmin/cmax
 * on the tuple itself. Check the reorderbuffer.c's comment above
 * ResolveCminCmaxDuringDecoding() for details.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases.
 *
 * To replace the normal catalog snapshots with decoding ones use the
 * SetupHistoricSnapshot() and TeardownHistoricSnapshot() functions.
 *
 *
 *
 * The snapbuild machinery is starting up in several stages, as illustrated
 * by the following graph describing the SnapBuild->state transitions:
 *
 *         +-------------------------+
 *    +----|         START           |-------------+
 *    |    +-------------------------+             |
 *    |                 |                          |
 *    |                 |                          |
 *    |        running_xacts #1                    |
 *    |                 |                          |
 *    |                 |                          |
 *    |                 v                          |
 *    |    +-------------------------+             v
 *    |    |   BUILDING_SNAPSHOT     |------------>|
 *    |    +-------------------------+             |
 *    |                 |                          |
 *    |                 |                          |
 *    | running_xacts #2, xacts from #1 finished   |
 *    |                 |                          |
 *    |                 |                          |
 *    |                 v                          |
 *    |    +-------------------------+             v
 *    |    |       FULL_SNAPSHOT     |------------>|
 *    |    +-------------------------+             |
 *    |                 |                          |
 * running_xacts        |                   saved snapshot
 * with zero xacts      |              at running_xacts's lsn
 *    |                 |                          |
 *    | running_xacts with xacts from #2 finished  |
 *    |                 |                          |
 *    |                 v                          |
 *    |    +-------------------------+             |
 *    +--->|SNAPBUILD_CONSISTENT     |<------------+
 *         +-------------------------+
 *
 * Initially the machinery is in the START stage. When an xl_running_xacts
 * record is read that is sufficiently new (above the safe xmin horizon),
 * there's a state transition. If there were no running xacts when the
 * xl_running_xacts record was generated, we'll directly go into CONSISTENT
 * state, otherwise we'll switch to the BUILDING_SNAPSHOT state. Having a full
 * snapshot means that all transactions that start henceforth can be decoded
 * in their entirety, but transactions that started previously can't. In
 * FULL_SNAPSHOT we'll switch into CONSISTENT once all those previously
 * running transactions have committed or aborted.
 *
 * Only transactions that commit after CONSISTENT state has been reached will
 * be replayed, even though they might have started while still in
 * FULL_SNAPSHOT. That ensures that we'll reach a point where no previous
 * changes has been exported, but all the following ones will be. That point
 * is a convenient point to initialize replication from, which is why we
 * export a snapshot at that point, which *can* be used to read normal data.
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/replication/logical/snapbuild.c
 *    -> src/replication/logical/snapbuild.rs
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

// ---------------------------------------------------------------------------
// Imports from real homes
// ---------------------------------------------------------------------------

// access/transam - TransactionId comparators, helpers, constants.
use crate::access::transam::transam::{
    TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdPrecedes,
    TransactionIdPrecedesOrEquals,
};
use crate::access::transam::{
    TransactionIdAdvance,
    TransactionIdIsNormal, TransactionIdIsValid, InvalidTransactionId, FirstNormalTransactionId,
};
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, LSN_FORMAT_ARGS, XLogRecPtr};

// utils/snapshot - SnapshotData, Snapshot, snapshot-type constants.
use crate::utils::snapshot::{
    Snapshot, SnapshotData, SNAPSHOT_HISTORIC_MVCC, SNAPSHOT_MVCC,
};

// port/pg_crc32c - CRC-32C helpers.
use crate::port::pg_crc32c::{
    pg_crc32c, COMP_CRC32C, EQ_CRC32C, FIN_CRC32C, INIT_CRC32C,
};

// miscadmin - MyProcPid, CHECK_FOR_INTERRUPTS.
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// c.rs - CommandId constants.
use crate::c::{CommandId, FirstCommandId, InvalidCommandId};

// pg_config_manual - MAXPGPATH.
use crate::pg_config_manual::MAXPGPATH;

// c.rs - PG_BINARY.
use crate::c::PG_BINARY;

// xl_running_xacts lives in storage/standbydefs.rs.
use crate::storage::standbydefs::xl_running_xacts;

// xl_heap_new_cid lives in access/rmgrdesc/heapdesc.rs.
use crate::access::rmgrdesc::heapdesc::xl_heap_new_cid;

// XACT_XINFO_HAS_INVALS.
use crate::access::rmgrdesc::xactdesc::XACT_XINFO_HAS_INVALS;

// ilist macros for dlist iteration.
use crate::{dlist_container, dlist_foreach};
use crate::lib::ilist::{dlist_head, dlist_iter, dlist_node};

// Resource owners.
use crate::utils::resowner::resowner::{ResourceOwner, CurrentResourceOwner};

// ---------------------------------------------------------------------------
// Re-export types defined in snapbuild_internal.rs so callers can import
// from this module.
// ---------------------------------------------------------------------------
pub use crate::replication::snapbuild_internal::{
    SnapBuild, SnapBuildOnDisk, SnapBuildState, SnapBuild_catchange, SnapBuild_committed,
};

// SnapBuildState constants (from snapbuild.h enum).
pub const SNAPBUILD_START: SnapBuildState = -1;
pub const SNAPBUILD_BUILDING_SNAPSHOT: SnapBuildState = 0;
pub const SNAPBUILD_FULL_SNAPSHOT: SnapBuildState = 1;
pub const SNAPBUILD_CONSISTENT: SnapBuildState = 2;

// Serialization magic / version (snapbuild.c).
pub const SNAPBUILD_MAGIC: u32 = 0x51A1E001;
pub const SNAPBUILD_VERSION: u32 = 6;

// Serialized-snapshot directory (storage/fd.h: PG_LOGICAL_SNAPSHOTS_DIR).
pub const PG_LOGICAL_SNAPSHOTS_DIR: &[u8] = b"pg_logical/snapshots\0";

// ---------------------------------------------------------------------------
// Local stub types for unported dependencies
// ---------------------------------------------------------------------------

// ReorderBuffer is opaque until reorderbuffer.c is ported.
// (already declared as c_void in snapbuild_internal.rs; re-alias here)
pub type ReorderBuffer = core::ffi::c_void;

// ReorderBufferTXN - minimal stub (topology fields used below).
#[repr(C)]
pub struct ReorderBufferTXN {
    pub xid: TransactionId,
    // The node field for dlist linkage.
    pub node: dlist_node,
    // LSN of the first relevant record for this transaction.
    pub restart_decoding_lsn: XLogRecPtr,
    // Whether this transaction has been prepared (2PC).
    pub is_prepared: bool,
    // ... other fields omitted (TODO(pg-port): fill in when reorderbuffer.rs lands)
}

// A minimal ReorderBuffer shell that exposes the fields snapbuild.c uses.
#[repr(C)]
pub struct ReorderBufferShell {
    /// All known toplevel transactions, ordered by first LSN.
    pub toplevel_by_lsn: dlist_head,
    /// Transactions that have made catalog changes (dclist).
    pub catchange_txns: CatalogChangeDList,
    /// The LSN at which we will restart decoding if we crash.
    pub current_restart_decoding_lsn: XLogRecPtr,
}

/// Opaque placeholder for dclist_head (lib/ilist.h).
/// TODO(pg-port): replace with crate::lib::ilist::dclist_head when ported.
#[repr(C)]
pub struct CatalogChangeDList {
    _opaque: [u8; 0],
}

// SharedInvalidationMessage - opaque (storage/sinval.h union).
pub type SharedInvalidationMessage = core::ffi::c_void;

// PGPROC minimal (just xmin field used here).
#[repr(C)]
pub struct PGPROC {
    pub xmin: TransactionId,
    // ... other fields omitted
}

// LWLock / modes.
#[repr(C)]
pub struct LWLock { _opaque: u8 }
pub const LW_SHARED: c_int = 1;

// dirent / DIR opaque stubs (storage/fd.h).
#[repr(C)]
pub struct DIR { _opaque: u8 }
#[repr(C)]
pub struct dirent {
    pub d_name: [c_char; 256],
}

// PGFileType (common/file_utils.h).
pub type PGFileType = c_int;
pub const PGFILETYPE_ERROR: PGFileType = 0;
pub const PGFILETYPE_REG: PGFileType = 1;

// ERRCODE constants - local stubs.
pub const ERRCODE_DATA_CORRUPTED: c_int = 0; // TODO(pg-port): errcodes.h
pub const ERRCODE_T_R_SERIALIZATION_FAILURE: c_int = 0; // TODO(pg-port): errcodes.h

// O_* / errno stubs (local to this file; same values as macOS/Linux).
const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 0x0200;
const O_EXCL: c_int = 0x0800;
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;

// WAIT_EVENT constants (pgstat.h) - opaque u32 stubs.
const WAIT_EVENT_SNAPBUILD_READ: u32 = 0;
const WAIT_EVENT_SNAPBUILD_WRITE: u32 = 0;
const WAIT_EVENT_SNAPBUILD_SYNC: u32 = 0;

// ---------------------------------------------------------------------------
// Static module-level state (SavedResourceOwnerDuringExport, ExportInProgress)
// ---------------------------------------------------------------------------

/*
 * Starting a transaction -- which we need to do while exporting a snapshot --
 * removes knowledge about the previously used resowner, so we save it here.
 */
static mut SavedResourceOwnerDuringExport: ResourceOwner = core::ptr::null_mut();
static mut ExportInProgress: bool = false;

// ---------------------------------------------------------------------------
// External stubs - unported dependencies  TODO(pg-port)
// ---------------------------------------------------------------------------

// MyProc global (storage/proc.h / storage/lmgr/proc.c).
extern "C" { pub static mut MyProc: *mut PGPROC; }
// MyProcPid (miscadmin.h).
extern "C" { pub static mut MyProcPid: c_int; }

/// xidComparator(a, b) - qsort/bsearch comparator for TransactionId.
/// TODO(pg-port): real home is access/transam/transam.c.
unsafe extern "C" fn xidComparator(a: *const c_void, b: *const c_void) -> c_int {
    let ax = *(a as *const TransactionId);
    let bx = *(b as *const TransactionId);
    if ax < bx { -1 } else if ax > bx { 1 } else { 0 }
}

/// NormalTransactionIdPrecedes - only for normal xids.
#[inline]
unsafe fn NormalTransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    TransactionIdPrecedes(id1, id2)
}

/// NormalTransactionIdFollows - only for normal xids.
#[inline]
unsafe fn NormalTransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    TransactionIdFollows(id1, id2)
}

// ---------- ReorderBuffer stubs  TODO(pg-port) ----------

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferXidHasBaseSnapshot(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
) -> bool {
    unimplemented!() // TODO(pg-port): real ReorderBufferXidHasBaseSnapshot in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferSetBaseSnapshot(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _snap: Snapshot,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferSetBaseSnapshot in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferXidSetCatalogChanges(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferXidSetCatalogChanges in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferAddNewTupleCids(
    _reorder: *mut ReorderBuffer,
    _top_xid: TransactionId,
    _lsn: XLogRecPtr,
    _target_locator: RelFileLocator,
    _target_tid: ItemPointerData,
    _cmin: CommandId,
    _cmax: CommandId,
    _combocid: CommandId,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferAddNewTupleCids in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferAddNewCommandId(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _cid: CommandId,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferAddNewCommandId in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferGetOldestXmin(_reorder: *mut ReorderBuffer) -> TransactionId {
    unimplemented!() // TODO(pg-port): real ReorderBufferGetOldestXmin in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferGetOldestTXN(
    _reorder: *mut ReorderBuffer,
) -> *mut ReorderBufferTXN {
    unimplemented!() // TODO(pg-port): real ReorderBufferGetOldestTXN in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferSetRestartPoint(
    _reorder: *mut ReorderBuffer,
    _lsn: XLogRecPtr,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferSetRestartPoint in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferGetCatalogChangesXacts(
    _reorder: *mut ReorderBuffer,
) -> *mut TransactionId {
    unimplemented!() // TODO(pg-port): real ReorderBufferGetCatalogChangesXacts in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferXidHasCatalogChanges(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
) -> bool {
    unimplemented!() // TODO(pg-port): real ReorderBufferXidHasCatalogChanges in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferAddSnapshot(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _snap: Snapshot,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferAddSnapshot in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferGetInvalidations(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _msgs: *mut *mut SharedInvalidationMessage,
) -> u32 {
    unimplemented!() // TODO(pg-port): real ReorderBufferGetInvalidations in reorderbuffer.c
}

/// TODO(pg-port): real home reorderbuffer.c.
pub unsafe fn ReorderBufferAddDistributedInvalidations(
    _reorder: *mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _ninvalidations: u32,
    _msgs: *mut SharedInvalidationMessage,
) {
    unimplemented!() // TODO(pg-port): real ReorderBufferAddDistributedInvalidations in reorderbuffer.c
}

/// rbtxn_is_prepared - check whether a transaction is a prepared (2PC) one.
/// TODO(pg-port): real home reorderbuffer.c.
#[inline]
unsafe fn rbtxn_is_prepared(txn: *const ReorderBufferTXN) -> bool {
    (*txn).is_prepared
}

/// dclist_count - count entries in a doubly-counted list.
/// TODO(pg-port): real home lib/ilist.h / lib/ilist.c.
unsafe fn dclist_count(_head: *const CatalogChangeDList) -> Size {
    unimplemented!() // TODO(pg-port): real dclist_count in lib/ilist.c
}

// ---------- Slot / logical.c stubs  TODO(pg-port) ----------

/// TODO(pg-port): real home replication/logical.c.
pub unsafe fn LogicalIncreaseXminForSlot(_lsn: XLogRecPtr, _xmin: TransactionId) {
    unimplemented!() // TODO(pg-port): real LogicalIncreaseXminForSlot in replication/logical.c
}

/// TODO(pg-port): real home replication/logical.c.
pub unsafe fn LogicalIncreaseRestartDecodingForSlot(
    _current_lsn: XLogRecPtr,
    _restart_lsn: XLogRecPtr,
) {
    unimplemented!() // TODO(pg-port): real LogicalIncreaseRestartDecodingForSlot in replication/logical.c
}

/// TODO(pg-port): real home replication/slot.c.
pub unsafe fn ReplicationSlotsComputeLogicalRestartLSN() -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real ReplicationSlotsComputeLogicalRestartLSN in replication/slot.c
}

// ---------- ProcArray / xact stubs  TODO(pg-port) ----------

/// TODO(pg-port): real home storage/procarray.c.
pub unsafe fn GetOldestSafeDecodingTransactionId(_copy_xacts: bool) -> TransactionId {
    unimplemented!() // TODO(pg-port): real GetOldestSafeDecodingTransactionId in storage/procarray.c
}

/// TODO(pg-port): real home storage/procarray.c (extern PGDLLIMPORT).
pub unsafe fn GetMaxSnapshotXidCount() -> c_int {
    unimplemented!() // TODO(pg-port): real GetMaxSnapshotXidCount in storage/procarray.c
}

/// TODO(pg-port): real home storage/lmgr/lwlock.c.
pub unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockAcquire in storage/lmgr/lwlock.c
}

/// TODO(pg-port): real home storage/lmgr/lwlock.c.
pub unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO(pg-port): real LWLockRelease in storage/lmgr/lwlock.c
}

/// ProcArrayLock (storage/procarray.c extern).
pub static mut ProcArrayLock: *mut LWLock = core::ptr::null_mut();

/// TODO(pg-port): real home access/transam/xact.c.
pub unsafe fn IsTransactionOrTransactionBlock() -> bool {
    unimplemented!() // TODO(pg-port): real IsTransactionOrTransactionBlock in access/transam/xact.c
}

/// TODO(pg-port): real home access/transam/xact.c.
pub unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO(pg-port): real IsTransactionState in access/transam/xact.c
}

/// TODO(pg-port): real home access/transam/xact.c.
pub unsafe fn StartTransactionCommand() {
    unimplemented!() // TODO(pg-port): real StartTransactionCommand in access/transam/xact.c
}

/// TODO(pg-port): real home access/transam/xact.c.
pub unsafe fn AbortCurrentTransaction() {
    unimplemented!() // TODO(pg-port): real AbortCurrentTransaction in access/transam/xact.c
}

/// TODO(pg-port): real home access/transam/xact.c (GUC variable).
pub static mut XactIsoLevel: c_int = 0;
pub static mut XactReadOnly: bool = false;
pub const XACT_REPEATABLE_READ: c_int = 2;

/// TODO(pg-port): real home utils/snapmgr.c.
pub unsafe fn InvalidateCatalogSnapshot() {
    unimplemented!() // TODO(pg-port): real InvalidateCatalogSnapshot in utils/snapmgr.c
}

/// TODO(pg-port): real home utils/snapmgr.c.
pub unsafe fn HaveRegisteredOrActiveSnapshot() -> bool {
    unimplemented!() // TODO(pg-port): real HaveRegisteredOrActiveSnapshot in utils/snapmgr.c
}

/// TODO(pg-port): real home utils/snapmgr.c.
pub unsafe fn HistoricSnapshotActive() -> bool {
    unimplemented!() // TODO(pg-port): real HistoricSnapshotActive in utils/snapmgr.c
}

/// TODO(pg-port): real home utils/snapmgr.c.
pub unsafe fn ExportSnapshot(_snap: Snapshot) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real ExportSnapshot in utils/snapmgr.c
}

/// TODO(pg-port): real home access/transam/xact.c.
pub unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdIsCurrentTransactionId in access/transam/xact.c
}

/// TODO(pg-port): real home storage/lmgr/lmgr.c.
pub unsafe fn XactLockTableWait(
    _xid: TransactionId,
    _rel: *mut c_void,
    _ctid: *mut c_void,
    _reason: c_int,
) {
    unimplemented!() // TODO(pg-port): real XactLockTableWait in storage/lmgr/lmgr.c
}
pub const XLTW_None: c_int = 0;

/// TODO(pg-port): real home storage/standby.c.
pub unsafe fn LogStandbySnapshot() {
    unimplemented!() // TODO(pg-port): real LogStandbySnapshot in storage/standby.c
}

/// TODO(pg-port): real home access/transam/xlog.c.
pub unsafe fn RecoveryInProgress() -> bool {
    unimplemented!() // TODO(pg-port): real RecoveryInProgress in access/transam/xlog.c
}

/// TODO(pg-port): real home access/transam/xlog.c.
pub unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real GetRedoRecPtr in access/transam/xlog.c
}

// ---------- File I/O stubs  TODO(pg-port) ----------

/// TODO(pg-port): real home storage/file/fd.c.
unsafe fn OpenTransientFile(_path: *const c_char, _flags: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): real OpenTransientFile in storage/file/fd.c
}

/// TODO(pg-port): real home storage/file/fd.c.
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): real CloseTransientFile in storage/file/fd.c
}

/// TODO(pg-port): real home storage/file/fd.c (wraps fsync(2)).
unsafe fn pg_fsync(_fd: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): real pg_fsync in storage/file/fd.c
}

/// TODO(pg-port): real home common/file_utils.c.
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) {
    unimplemented!() // TODO(pg-port): real fsync_fname in common/file_utils.c
}

/// TODO(pg-port): real home storage/file/fd.c.
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!() // TODO(pg-port): real AllocateDir in storage/file/fd.c
}

/// TODO(pg-port): real home storage/file/fd.c.
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!() // TODO(pg-port): real ReadDir in storage/file/fd.c
}

/// TODO(pg-port): real home storage/file/fd.c.
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!() // TODO(pg-port): real FreeDir in storage/file/fd.c
}

/// TODO(pg-port): real home common/file_utils.c.
unsafe fn get_dirent_type(
    _path: *const c_char,
    _de: *const dirent,
    _look_through_symlinks: bool,
    _elevel: c_int,
) -> PGFileType {
    unimplemented!() // TODO(pg-port): real get_dirent_type in common/file_utils.c
}

/// pgstat_report_wait_start / pgstat_report_wait_end stubs.
/// TODO(pg-port): real homes pgstat.h / pgstat.c.
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {}
unsafe fn pgstat_report_wait_end() {}

/// MemoryContextAlloc - explicit-context palloc (palloc.h).
/// (The prelude already exposes MemoryContextAllocZero; add this variant.)
unsafe fn MemoryContextAlloc(context: MemoryContext, size: Size) -> *mut c_void {
    // palloc from a context: in the real backend this uses context->methods->alloc.
    // Stub: allocate from current context (the caller switches context first).
    palloc(size)
}

// Opaque placeholder types referenced in ReorderBufferAddNewTupleCids.
// TODO(pg-port): real structs live in storage/relfilelocator.h and
// storage/itemptr.h respectively.
#[repr(C)]
pub struct RelFileLocator { pub spcOid: u32, pub dbOid: u32, pub relNumber: u32 }
#[repr(C)]
pub struct ItemPointerData { pub ip_blkid: [u8; 4], pub ip_posid: u16 }

// errcode_for_file_access stub (errcodes.h / elog.h).
unsafe fn errcode_for_file_access() -> c_int { 0 }

// errmsg_internal / errmsg_plural / errdetail_internal / errdetail stubs.
// The caller uses them inside ereport!() so they need to return c_int.
macro_rules! errmsg_internal {
    ($($t:tt)*) => {{ let _ = format!($($t)*); 0i32 }};
}
macro_rules! errmsg_plural {
    ($s:expr, $p:expr, $n:expr, $($t:tt)*) => {{
        let _ = format!($p, $($t)*);
        0i32
    }};
}
macro_rules! errdetail {
    ($($t:tt)*) => {{ let _ = format!($($t)*); 0i32 }};
}
macro_rules! errdetail_internal {
    ($($t:tt)*) => {{ let _ = format!($($t)*); 0i32 }};
}

// snprintf / sscanf / sprintf - libc wrappers.
extern "C" {
    fn snprintf(buf: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn stat(path: *const c_char, buf: *mut StatBuf) -> c_int;
    fn rename(old: *const c_char, new: *const c_char) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
}

// Minimal stat buffer (sys/stat.h).
#[repr(C)]
struct StatBuf { _pad: [u8; 128] }

// errno access.
extern "C" { fn __error() -> *mut c_int; }
#[inline]
unsafe fn get_errno() -> c_int { *__error() }
#[inline]
unsafe fn set_errno(e: c_int) { *__error() = e; }

// ---------------------------------------------------------------------------
// SnapBuildOnDisk size helpers (offsetof equivalents for serialization).
// ---------------------------------------------------------------------------

/// Size of the constant (non-variable) part of SnapBuildOnDisk.
/// = offsetof(SnapBuildOnDisk, builder) + sizeof(SnapBuild)
/// In C: `#define SnapBuildOnDiskConstantSize offsetof(SnapBuildOnDisk, builder)`
/// We store sizeof(SnapBuildOnDisk) as the constant part (builder is the last fixed field).
#[inline]
fn snap_build_on_disk_constant_size() -> usize {
    core::mem::size_of::<SnapBuildOnDisk>()
}

/// Size of the part NOT covered by the checksum.
/// = offsetof(SnapBuildOnDisk, version)  (magic + checksum fields come first)
#[inline]
fn snap_build_on_disk_not_checksummed_size() -> usize {
    // magic: u32, checksum: pg_crc32c(u32) = 8 bytes total.
    2 * core::mem::size_of::<u32>()
}

// ===========================================================================
// Part 2 - Public API functions
// ===========================================================================

/*
 * Allocate a new snapshot builder.
 *
 * xmin_horizon is the xid >= which we can be sure no catalog rows have been
 * removed, start_lsn is the LSN >= we want to replay commits.
 */
pub unsafe fn AllocateSnapshotBuilder(
    reorder: *mut ReorderBuffer,
    xmin_horizon: TransactionId,
    start_lsn: XLogRecPtr,
    need_full_snapshot: bool,
    in_slot_creation: bool,
    two_phase_at: XLogRecPtr,
) -> *mut SnapBuild {
    let context: MemoryContext;
    let oldcontext: MemoryContext;
    let builder: *mut SnapBuild;

    /* allocate memory in own context, to have better accountability */
    context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"snapshot builder context",
        ALLOCSET_DEFAULT_SIZES
    ) as MemoryContext;
    oldcontext = MemoryContextSwitchTo(context);

    builder = palloc0(core::mem::size_of::<SnapBuild>()) as *mut SnapBuild;

    (*builder).state = SNAPBUILD_START;
    (*builder).context = context as crate::utils::mmgr::memnodes::MemoryContext;
    (*builder).reorder = reorder as *mut core::ffi::c_void;
    /* Other struct members initialized by zeroing via palloc0 above */

    (*builder).committed.xcnt = 0;
    (*builder).committed.xcnt_space = 128; /* arbitrary number */
    (*builder).committed.xip =
        palloc0((*builder).committed.xcnt_space * core::mem::size_of::<TransactionId>())
            as *mut TransactionId;
    (*builder).committed.includes_all_transactions = true;

    (*builder).catchange.xcnt = 0;
    (*builder).catchange.xip = core::ptr::null_mut();

    (*builder).initial_xmin_horizon = xmin_horizon;
    (*builder).start_decoding_at = start_lsn;
    (*builder).in_slot_creation = in_slot_creation;
    (*builder).building_full_snapshot = need_full_snapshot;
    (*builder).two_phase_at = two_phase_at;

    MemoryContextSwitchTo(oldcontext);

    builder
}

/*
 * Free a snapshot builder.
 */
pub unsafe fn FreeSnapshotBuilder(builder: *mut SnapBuild) {
    let context: MemoryContext = (*builder).context as crate::utils::palloc::MemoryContext;

    /* free snapshot explicitly, that contains some error checking */
    if !(*builder).snapshot.is_null() {
        SnapBuildSnapDecRefcount((*builder).snapshot);
        (*builder).snapshot = core::ptr::null_mut();
    }

    /* other resources are deallocated via memory context reset */
    MemoryContextDelete(context);
}

/*
 * Free an unreferenced snapshot that has previously been built by us.
 */
unsafe fn SnapBuildFreeSnapshot(snap: Snapshot) {
    /* make sure we don't get passed an external snapshot */
    Assert!((*snap).snapshot_type == SNAPSHOT_HISTORIC_MVCC);

    /* make sure nobody modified our snapshot */
    Assert!((*snap).curcid == FirstCommandId);
    Assert!(!(*snap).suboverflowed);
    Assert!(!(*snap).takenDuringRecovery);
    Assert!((*snap).regd_count == 0);

    /* slightly more likely, so it's checked even without c-asserts */
    if (*snap).copied {
        ereport!(ERROR, errmsg!("cannot free a copied snapshot"));
    }

    if (*snap).active_count != 0 {
        ereport!(ERROR, errmsg!("cannot free an active snapshot"));
    }

    pfree(snap as *mut c_void);
}

/*
 * In which state of snapshot building are we?
 */
pub unsafe fn SnapBuildCurrentState(builder: *mut SnapBuild) -> SnapBuildState {
    (*builder).state
}

/*
 * Return the LSN at which the two-phase decoding was first enabled.
 */
pub unsafe fn SnapBuildGetTwoPhaseAt(builder: *mut SnapBuild) -> XLogRecPtr {
    (*builder).two_phase_at
}

/*
 * Set the LSN at which two-phase decoding is enabled.
 */
pub unsafe fn SnapBuildSetTwoPhaseAt(builder: *mut SnapBuild, ptr: XLogRecPtr) {
    (*builder).two_phase_at = ptr;
}

/*
 * Should the contents of transaction ending at 'ptr' be decoded?
 */
pub unsafe fn SnapBuildXactNeedsSkip(builder: *mut SnapBuild, ptr: XLogRecPtr) -> bool {
    ptr < (*builder).start_decoding_at
}

/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as builder->snapshot.
 */
unsafe fn SnapBuildSnapIncRefcount(snap: Snapshot) {
    (*snap).active_count += 1;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible, so that external resources that have been handed an
 * IncRef'ed Snapshot can adjust its refcount easily.
 */
pub unsafe fn SnapBuildSnapDecRefcount(snap: Snapshot) {
    /* make sure we don't get passed an external snapshot */
    Assert!((*snap).snapshot_type == SNAPSHOT_HISTORIC_MVCC);

    /* make sure nobody modified our snapshot */
    Assert!((*snap).curcid == FirstCommandId);
    Assert!(!(*snap).suboverflowed);
    Assert!(!(*snap).takenDuringRecovery);

    Assert!((*snap).regd_count == 0);

    Assert!((*snap).active_count > 0);

    /* slightly more likely, so it's checked even without casserts */
    if (*snap).copied {
        ereport!(ERROR, errmsg!("cannot free a copied snapshot"));
    }

    (*snap).active_count -= 1;
    if (*snap).active_count == 0 {
        SnapBuildFreeSnapshot(snap);
    }
}

/*
 * Build a new snapshot, based on currently committed catalog-modifying
 * transactions.
 *
 * In-progress transactions with catalog access are *not* allowed to modify
 * these snapshots; they have to copy them and fill in appropriate ->curcid
 * and ->subxip/subxcnt values.
 */
unsafe fn SnapBuildBuildSnapshot(builder: *mut SnapBuild) -> Snapshot {
    let snapshot: Snapshot;
    let ssize: Size;

    Assert!((*builder).state >= SNAPBUILD_FULL_SNAPSHOT);

    ssize = core::mem::size_of::<SnapshotData>()
        + core::mem::size_of::<TransactionId>() * (*builder).committed.xcnt
        + core::mem::size_of::<TransactionId>() * 1; /* toplevel xid */

    snapshot = MemoryContextAllocZero((*builder).context as crate::utils::palloc::MemoryContext, ssize) as Snapshot;

    (*snapshot).snapshot_type = SNAPSHOT_HISTORIC_MVCC;

    /*
     * We misuse the original meaning of SnapshotData's xip and subxip fields
     * to make the more fitting for our needs.
     *
     * In the 'xip' array we store transactions that have to be treated as
     * committed. Since we will only ever look at tuples from transactions
     * that have modified the catalog it's more efficient to store those few
     * that exist between xmin and xmax (frequently there are none).
     *
     * Snapshots that are used in transactions that have modified the catalog
     * also use the 'subxip' array to store their toplevel xid and all the
     * subtransaction xids so we can recognize when we need to treat rows as
     * visible that are not in xip but still need to be visible. Subxip only
     * gets filled when the transaction is copied into the context of a
     * catalog modifying transaction since we otherwise share a snapshot
     * between transactions. As long as a txn hasn't modified the catalog it
     * doesn't need to treat any uncommitted rows as visible, so there is no
     * need for those xids.
     *
     * Both arrays are qsort'ed so that we can use bsearch() on them.
     */
    Assert!(TransactionIdIsNormal((*builder).xmin));
    Assert!(TransactionIdIsNormal((*builder).xmax));

    (*snapshot).xmin = (*builder).xmin;
    (*snapshot).xmax = (*builder).xmax;

    /* store all transactions to be treated as committed by this snapshot */
    (*snapshot).xip = (snapshot as *mut u8)
        .add(core::mem::size_of::<SnapshotData>()) as *mut TransactionId;
    (*snapshot).xcnt = (*builder).committed.xcnt as u32;
    core::ptr::copy_nonoverlapping(
        (*builder).committed.xip,
        (*snapshot).xip,
        (*builder).committed.xcnt,
    );

    /* sort so we can bsearch() */
    libc_qsort(
        (*snapshot).xip as *mut c_void,
        (*snapshot).xcnt as usize,
        core::mem::size_of::<TransactionId>(),
        xidComparator,
    );

    /*
     * Initially, subxip is empty, i.e. it's a snapshot to be used by
     * transactions that don't modify the catalog. Will be filled by
     * ReorderBufferCopySnap() if necessary.
     */
    (*snapshot).subxcnt = 0;
    (*snapshot).subxip = core::ptr::null_mut();

    (*snapshot).suboverflowed = false;
    (*snapshot).takenDuringRecovery = false;
    (*snapshot).copied = false;
    (*snapshot).curcid = FirstCommandId;
    (*snapshot).active_count = 0;
    (*snapshot).regd_count = 0;
    (*snapshot).snapXactCompletionCount = 0;

    snapshot
}

// libc qsort binding.
extern "C" {
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *mut c_void;
}

/// libc qsort wrapper (Rust function pointers are not extern "C" by default).
#[inline]
unsafe fn libc_qsort(
    base: *mut c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) {
    qsort(base, nmemb, size, compar);
}

/*
 * Build the initial slot snapshot and convert it to a normal snapshot that
 * is understood by HeapTupleSatisfiesMVCC.
 *
 * The snapshot will be usable directly in current transaction or exported
 * for loading in different transaction.
 */
pub unsafe fn SnapBuildInitialSnapshot(builder: *mut SnapBuild) -> Snapshot {
    let snap: Snapshot;
    let mut xid: TransactionId;
    let safeXid: TransactionId;
    let newxip: *mut TransactionId;
    let mut newxcnt: c_int = 0;

    Assert!(XactIsoLevel == XACT_REPEATABLE_READ);
    Assert!((*builder).building_full_snapshot);

    /* don't allow older snapshots */
    InvalidateCatalogSnapshot(); /* about to overwrite MyProc->xmin */
    if HaveRegisteredOrActiveSnapshot() {
        ereport!(ERROR, errmsg!("cannot build an initial slot snapshot when snapshots exist"));
    }
    Assert!(!HistoricSnapshotActive());

    if (*builder).state != SNAPBUILD_CONSISTENT {
        ereport!(ERROR, errmsg!("cannot build an initial slot snapshot before reaching a consistent state"));
    }

    if !(*builder).committed.includes_all_transactions {
        ereport!(ERROR, errmsg!("cannot build an initial slot snapshot, not all transactions are monitored anymore"));
    }

    /* so we don't overwrite the existing value */
    if TransactionIdIsValid((*MyProc).xmin) {
        ereport!(ERROR, errmsg!("cannot build an initial slot snapshot when MyProc->xmin already is valid"));
    }

    snap = SnapBuildBuildSnapshot(builder);

    /*
     * We know that snap->xmin is alive, enforced by the logical xmin
     * mechanism. Due to that we can do this without locks, we're only
     * changing our own value.
     *
     * Building an initial snapshot is expensive and an unenforced xmin
     * horizon would have bad consequences, therefore always double-check that
     * the horizon is enforced.
     */
    LWLockAcquire(ProcArrayLock, LW_SHARED);
    safeXid = GetOldestSafeDecodingTransactionId(false);
    LWLockRelease(ProcArrayLock);

    if TransactionIdFollows(safeXid, (*snap).xmin) {
        ereport!(ERROR, errmsg!("cannot build an initial slot snapshot as oldest safe xid {} follows snapshot's xmin {}",
                safeXid, (*snap).xmin));
    }

    (*MyProc).xmin = (*snap).xmin;

    /* allocate in transaction context */
    newxip = palloc(
        core::mem::size_of::<TransactionId>() * GetMaxSnapshotXidCount() as usize
    ) as *mut TransactionId;

    /*
     * snapbuild.c builds transactions in an "inverted" manner, which means it
     * stores committed transactions in ->xip, not ones in progress. Build a
     * classical snapshot by marking all non-committed transactions as
     * in-progress. This can be expensive.
     */
    xid = (*snap).xmin;
    while NormalTransactionIdPrecedes(xid, (*snap).xmax) {
        let test: *mut c_void;

        /*
         * Check whether transaction committed using the decoding snapshot
         * meaning of ->xip.
         */
        test = bsearch(
            &xid as *const TransactionId as *const c_void,
            (*snap).xip as *const c_void,
            (*snap).xcnt as usize,
            core::mem::size_of::<TransactionId>(),
            xidComparator,
        );

        if test.is_null() {
            if newxcnt >= GetMaxSnapshotXidCount() {
                ereport!(ERROR, errmsg!("initial slot snapshot too large")) /* C also: errcode */;
            }

            *newxip.add(newxcnt as usize) = xid;
            newxcnt += 1;
        }

        TransactionIdAdvance(&mut xid);
    }

    /* adjust remaining snapshot fields as needed */
    (*snap).snapshot_type = SNAPSHOT_MVCC;
    (*snap).xcnt = newxcnt as u32;
    (*snap).xip = newxip;

    snap
}

/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 */
pub unsafe fn SnapBuildExportSnapshot(builder: *mut SnapBuild) -> *const c_char {
    let snap: Snapshot;
    let snapname: *mut c_char;

    if IsTransactionOrTransactionBlock() {
        ereport!(ERROR, errmsg!("cannot export a snapshot from within a transaction"));
    }

    if !SavedResourceOwnerDuringExport.is_null() {
        ereport!(ERROR, errmsg!("can only export one snapshot at a time"));
    }

    SavedResourceOwnerDuringExport = CurrentResourceOwner;
    ExportInProgress = true;

    StartTransactionCommand();

    /* There doesn't seem to a nice API to set these */
    XactIsoLevel = XACT_REPEATABLE_READ;
    XactReadOnly = true;

    snap = SnapBuildInitialSnapshot(builder);

    /*
     * now that we've built a plain snapshot, make it active and use the
     * normal mechanisms for exporting it
     */
    snapname = ExportSnapshot(snap);

    ereport!(LOG,
        errmsg!(
            "exported logical decoding snapshot: \"{}\" with {} transaction ID(s)",
            core::ffi::CStr::from_ptr(snapname).to_str().unwrap_or(""),
            (*snap).xcnt
        ));
    snapname as *const c_char
}

/*
 * Ensure there is a snapshot and if not build one for current transaction.
 */
pub unsafe fn SnapBuildGetOrBuildSnapshot(builder: *mut SnapBuild) -> Snapshot {
    Assert!((*builder).state == SNAPBUILD_CONSISTENT);

    /* only build a new snapshot if we don't have a prebuilt one */
    if (*builder).snapshot.is_null() {
        (*builder).snapshot = SnapBuildBuildSnapshot(builder);
        /* increase refcount for the snapshot builder */
        SnapBuildSnapIncRefcount((*builder).snapshot);
    }

    (*builder).snapshot
}

/*
 * Reset a previously SnapBuildExportSnapshot()'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource
 * owner back to its original value.
 */
pub unsafe fn SnapBuildClearExportedSnapshot() {
    let tmpResOwner: ResourceOwner;

    /* nothing exported, that is the usual case */
    if !ExportInProgress {
        return;
    }

    if !IsTransactionState() {
        ereport!(ERROR, errmsg!("clearing exported snapshot in wrong transaction state"));
    }

    /*
     * AbortCurrentTransaction() takes care of resetting the snapshot state,
     * so remember SavedResourceOwnerDuringExport.
     */
    tmpResOwner = SavedResourceOwnerDuringExport;

    /* make sure nothing could have ever happened */
    AbortCurrentTransaction();

    CurrentResourceOwner = tmpResOwner;
}

/*
 * Clear snapshot export state during transaction abort.
 */
pub unsafe fn SnapBuildResetExportedSnapshotState() {
    SavedResourceOwnerDuringExport = core::ptr::null_mut();
    ExportInProgress = false;
}

/*
 * Handle the effects of a single heap change, appropriate to the current state
 * of the snapshot builder and returns whether changes made at (xid, lsn) can
 * be decoded.
 */
pub unsafe fn SnapBuildProcessChange(
    builder: *mut SnapBuild,
    xid: TransactionId,
    lsn: XLogRecPtr,
) -> bool {
    /*
     * We can't handle data in transactions if we haven't built a snapshot
     * yet, so don't store them.
     */
    if (*builder).state < SNAPBUILD_FULL_SNAPSHOT {
        return false;
    }

    /*
     * No point in keeping track of changes in transactions that we don't have
     * enough information about to decode. This means that they started before
     * we got into the SNAPBUILD_FULL_SNAPSHOT state.
     */
    if (*builder).state < SNAPBUILD_CONSISTENT
        && TransactionIdPrecedes(xid, (*builder).next_phase_at)
    {
        return false;
    }

    /*
     * If the reorderbuffer doesn't yet have a snapshot, add one now, it will
     * be needed to decode the change we're currently processing.
     */
    if !ReorderBufferXidHasBaseSnapshot((*builder).reorder, xid) {
        /* only build a new snapshot if we don't have a prebuilt one */
        if (*builder).snapshot.is_null() {
            (*builder).snapshot = SnapBuildBuildSnapshot(builder);
            /* increase refcount for the snapshot builder */
            SnapBuildSnapIncRefcount((*builder).snapshot);
        }

        /*
         * Increase refcount for the transaction we're handing the snapshot
         * out to.
         */
        SnapBuildSnapIncRefcount((*builder).snapshot);
        ReorderBufferSetBaseSnapshot((*builder).reorder, xid, lsn, (*builder).snapshot);
    }

    true
}

/*
 * Do CommandId/combo CID handling after reading an xl_heap_new_cid record.
 * This implies that a transaction has done some form of write to system
 * catalogs.
 */
pub unsafe fn SnapBuildProcessNewCid(
    builder: *mut SnapBuild,
    xid: TransactionId,
    lsn: XLogRecPtr,
    xlrec: *mut xl_heap_new_cid,
) {
    let cid: CommandId;

    /*
     * we only log new_cid's if a catalog tuple was modified, so mark the
     * transaction as containing catalog modifications
     */
    ReorderBufferXidSetCatalogChanges((*builder).reorder, xid, lsn);

    ReorderBufferAddNewTupleCids(
        (*builder).reorder,
        (*xlrec).top_xid,
        lsn,
        core::mem::transmute((*xlrec).target_locator),
        core::mem::transmute((*xlrec).target_tid),
        (*xlrec).cmin,
        (*xlrec).cmax,
        (*xlrec).combocid,
    );

    /* figure out new command id */
    if (*xlrec).cmin != InvalidCommandId && (*xlrec).cmax != InvalidCommandId {
        cid = if (*xlrec).cmin > (*xlrec).cmax {
            (*xlrec).cmin
        } else {
            (*xlrec).cmax
        };
    } else if (*xlrec).cmax != InvalidCommandId {
        cid = (*xlrec).cmax;
    } else if (*xlrec).cmin != InvalidCommandId {
        cid = (*xlrec).cmin;
    } else {
        cid = InvalidCommandId; /* silence compiler */
        ereport!(ERROR, errmsg!("xl_heap_new_cid record without a valid CommandId"));
    }

    ReorderBufferAddNewCommandId((*builder).reorder, xid, lsn, cid + 1);
}

// ===========================================================================
// Part 3 - Snapshot distribution, commit tracking, running-xacts handling
// ===========================================================================

/*
 * Add a new Snapshot and invalidation messages to all transactions we're
 * decoding that currently are in-progress so they can see new catalog contents
 * made by the transaction that just committed. This is necessary because those
 * in-progress transactions will use the new catalog's contents from here on
 * (at the very least everything they do needs to be compatible with newer
 * catalog contents).
 */
unsafe fn SnapBuildDistributeSnapshotAndInval(
    builder: *mut SnapBuild,
    lsn: XLogRecPtr,
    xid: TransactionId,
) {
    let reorder_shell = (*builder).reorder as *mut ReorderBufferShell;

    /*
     * Iterate through all toplevel transactions. This can include
     * subtransactions which we just don't yet know to be that, but that's
     * fine, they will just get an unnecessary snapshot and invalidations
     * queued.
     */
    let head = core::ptr::addr_of_mut!((*reorder_shell).toplevel_by_lsn);
    let mut txn_i: dlist_iter = core::mem::zeroed();
    dlist_foreach!(txn_i, head, {
        let txn: *mut ReorderBufferTXN =
            dlist_container!(ReorderBufferTXN, node, txn_i.cur);

        Assert!(TransactionIdIsValid((*txn).xid));

        /*
         * If we don't have a base snapshot yet, there are no changes in this
         * transaction which in turn implies we don't yet need a snapshot at
         * all. We'll add a snapshot when the first change gets queued.
         *
         * Similarly, we don't need to add invalidations to a transaction
         * whose base snapshot is not yet set. Once a base snapshot is built,
         * it will include the xids of committed transactions that have
         * modified the catalog, thus reflecting the new catalog contents. The
         * existing catalog cache will have already been invalidated after
         * processing the invalidations in the transaction that modified
         * catalogs, ensuring that a fresh cache is constructed during
         * decoding.
         *
         * NB: This works correctly even for subtransactions because
         * ReorderBufferAssignChild() takes care to transfer the base snapshot
         * to the top-level transaction, and while iterating the changequeue
         * we'll get the change from the subtxn.
         */
        if !ReorderBufferXidHasBaseSnapshot((*builder).reorder, (*txn).xid) {
            continue;
        }

        /*
         * We don't need to add snapshot or invalidations to prepared
         * transactions as they should not see the new catalog contents.
         */
        if rbtxn_is_prepared(txn) {
            continue;
        }

        elog!(DEBUG2, "adding a new snapshot and invalidations to {} at {}/{}",
            (*txn).xid,
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1);

        /*
         * increase the snapshot's refcount for the transaction we are handing
         * it out to
         */
        SnapBuildSnapIncRefcount((*builder).snapshot);
        ReorderBufferAddSnapshot((*builder).reorder, (*txn).xid, lsn, (*builder).snapshot);

        /*
         * Add invalidation messages to the reorder buffer of in-progress
         * transactions except the current committed transaction, for which we
         * will execute invalidations at the end.
         *
         * It is required, otherwise, we will end up using the stale catcache
         * contents built by the current transaction even after its decoding,
         * which should have been invalidated due to concurrent catalog
         * changing transaction.
         *
         * Distribute only the invalidation messages generated by the current
         * committed transaction. Invalidation messages received from other
         * transactions would have already been propagated to the relevant
         * in-progress transactions. This transaction would have processed
         * those invalidations, ensuring that subsequent transactions observe
         * a consistent cache state.
         */
        if (*txn).xid != xid {
            let mut ninvalidations: u32 = 0;
            let mut msgs: *mut SharedInvalidationMessage = core::ptr::null_mut();

            ninvalidations = ReorderBufferGetInvalidations(
                (*builder).reorder,
                xid,
                &mut msgs,
            );

            if ninvalidations > 0 {
                Assert!(!msgs.is_null());

                ReorderBufferAddDistributedInvalidations(
                    (*builder).reorder,
                    (*txn).xid,
                    lsn,
                    ninvalidations,
                    msgs,
                );
            }
        }
    });
}

/*
 * Keep track of a new catalog changing transaction that has committed.
 */
unsafe fn SnapBuildAddCommittedTxn(builder: *mut SnapBuild, xid: TransactionId) {
    Assert!(TransactionIdIsValid(xid));

    if (*builder).committed.xcnt == (*builder).committed.xcnt_space {
        (*builder).committed.xcnt_space = (*builder).committed.xcnt_space * 2 + 1;

        elog!(DEBUG1, "increasing space for committed transactions to {}",
            (*builder).committed.xcnt_space as u32);

        (*builder).committed.xip = repalloc(
            (*builder).committed.xip as *mut c_void,
            (*builder).committed.xcnt_space * core::mem::size_of::<TransactionId>(),
        ) as *mut TransactionId;
    }

    /*
     * TODO: It might make sense to keep the array sorted here instead of
     * doing it every time we build a new snapshot. On the other hand this
     * gets called repeatedly when a transaction with subtransactions commits.
     */
    *(*builder).committed.xip.add((*builder).committed.xcnt) = xid;
    (*builder).committed.xcnt += 1;
}

/*
 * Remove knowledge about transactions we treat as committed or containing catalog
 * changes that are smaller than ->xmin. Those won't ever get checked via
 * the ->committed or ->catchange array, respectively. The committed xids will
 * get checked via the clog machinery.
 *
 * We can ideally remove the transaction from catchange array once it is
 * finished (committed/aborted) but that could be costly as we need to maintain
 * the xids order in the array.
 */
unsafe fn SnapBuildPurgeOlderTxn(builder: *mut SnapBuild) {
    let mut off: usize;
    let workspace: *mut TransactionId;
    let mut surviving_xids: usize = 0;

    /* not ready yet */
    if !TransactionIdIsNormal((*builder).xmin) {
        return;
    }

    /* TODO: Neater algorithm than just copying and iterating? */
    workspace = MemoryContextAlloc(
        (*builder).context as crate::utils::palloc::MemoryContext,
        (*builder).committed.xcnt * core::mem::size_of::<TransactionId>(),
    ) as *mut TransactionId;

    /* copy xids that still are interesting to workspace */
    off = 0;
    while off < (*builder).committed.xcnt {
        if NormalTransactionIdPrecedes(*(*builder).committed.xip.add(off), (*builder).xmin) {
            /* remove */
        } else {
            *workspace.add(surviving_xids) = *(*builder).committed.xip.add(off);
            surviving_xids += 1;
        }
        off += 1;
    }

    /* copy workspace back to persistent state */
    core::ptr::copy_nonoverlapping(
        workspace,
        (*builder).committed.xip,
        surviving_xids,
    );

    elog!(DEBUG3,
        "purged committed transactions from {} to {}, xmin: {}, xmax: {}",
        (*builder).committed.xcnt as u32,
        surviving_xids as u32,
        (*builder).xmin,
        (*builder).xmax);
    (*builder).committed.xcnt = surviving_xids;

    pfree(workspace as *mut c_void);

    /*
     * Purge xids in ->catchange as well. The purged array must also be sorted
     * in xidComparator order.
     */
    if (*builder).catchange.xcnt > 0 {
        /*
         * Since catchange.xip is sorted, we find the lower bound of xids that
         * are still interesting.
         */
        off = 0;
        while off < (*builder).catchange.xcnt {
            if TransactionIdFollowsOrEquals(
                *(*builder).catchange.xip.add(off),
                (*builder).xmin,
            ) {
                break;
            }
            off += 1;
        }

        surviving_xids = (*builder).catchange.xcnt - off;

        if surviving_xids > 0 {
            core::ptr::copy(
                (*builder).catchange.xip.add(off),
                (*builder).catchange.xip,
                surviving_xids,
            );
        } else {
            pfree((*builder).catchange.xip as *mut c_void);
            (*builder).catchange.xip = core::ptr::null_mut();
        }

        elog!(DEBUG3,
            "purged catalog modifying transactions from {} to {}, xmin: {}, xmax: {}",
            (*builder).catchange.xcnt as u32,
            surviving_xids as u32,
            (*builder).xmin,
            (*builder).xmax);
        (*builder).catchange.xcnt = surviving_xids;
    }
}

/*
 * Handle everything that needs to be done when a transaction commits
 */
pub unsafe fn SnapBuildCommitTxn(
    builder: *mut SnapBuild,
    lsn: XLogRecPtr,
    xid: TransactionId,
    nsubxacts: c_int,
    subxacts: *mut TransactionId,
    xinfo: u32,
) {
    let mut nxact: c_int;

    let mut needs_snapshot = false;
    let mut needs_timetravel = false;
    let mut sub_needs_timetravel = false;

    let mut xmax: TransactionId = xid;

    /*
     * Transactions preceding BUILDING_SNAPSHOT will neither be decoded, nor
     * will they be part of a snapshot.  So we don't need to record anything.
     */
    if (*builder).state == SNAPBUILD_START
        || ((*builder).state == SNAPBUILD_BUILDING_SNAPSHOT
            && TransactionIdPrecedes(xid, (*builder).next_phase_at))
    {
        /* ensure that only commits after this are getting replayed */
        if (*builder).start_decoding_at <= lsn {
            (*builder).start_decoding_at = lsn + 1;
        }
        return;
    }

    if (*builder).state < SNAPBUILD_CONSISTENT {
        /* ensure that only commits after this are getting replayed */
        if (*builder).start_decoding_at <= lsn {
            (*builder).start_decoding_at = lsn + 1;
        }

        /*
         * If building an exportable snapshot, force xid to be tracked, even
         * if the transaction didn't modify the catalog.
         */
        if (*builder).building_full_snapshot {
            needs_timetravel = true;
        }
    }

    nxact = 0;
    while nxact < nsubxacts {
        let subxid: TransactionId = *subxacts.add(nxact as usize);

        /*
         * Add subtransaction to base snapshot if catalog modifying, we don't
         * distinguish to toplevel transactions there.
         */
        if SnapBuildXidHasCatalogChanges(builder, subxid, xinfo) {
            sub_needs_timetravel = true;
            needs_snapshot = true;

            elog!(DEBUG1, "found subtransaction {}:{} with catalog changes",
                xid, subxid);

            SnapBuildAddCommittedTxn(builder, subxid);

            if NormalTransactionIdFollows(subxid, xmax) {
                xmax = subxid;
            }
        }
        /*
         * If we're forcing timetravel we also need visibility information
         * about subtransaction, so keep track of subtransaction's state, even
         * if not catalog modifying.  Don't need to distribute a snapshot in
         * that case.
         */
        else if needs_timetravel {
            SnapBuildAddCommittedTxn(builder, subxid);
            if NormalTransactionIdFollows(subxid, xmax) {
                xmax = subxid;
            }
        }

        nxact += 1;
    }

    /* if top-level modified catalog, it'll need a snapshot */
    if SnapBuildXidHasCatalogChanges(builder, xid, xinfo) {
        elog!(DEBUG2, "found top level transaction {}, with catalog changes", xid);
        needs_snapshot = true;
        needs_timetravel = true;
        SnapBuildAddCommittedTxn(builder, xid);
    } else if sub_needs_timetravel {
        /* track toplevel txn as well, subxact alone isn't meaningful */
        elog!(DEBUG2,
            "forced transaction {} to do timetravel due to one of its subtransactions",
            xid);
        needs_timetravel = true;
        SnapBuildAddCommittedTxn(builder, xid);
    } else if needs_timetravel {
        elog!(DEBUG2, "forced transaction {} to do timetravel", xid);

        SnapBuildAddCommittedTxn(builder, xid);
    }

    if !needs_timetravel {
        /* record that we cannot export a general snapshot anymore */
        (*builder).committed.includes_all_transactions = false;
    }

    Assert!(!needs_snapshot || needs_timetravel);

    /*
     * Adjust xmax of the snapshot builder, we only do that for committed,
     * catalog modifying, transactions, everything else isn't interesting for
     * us since we'll never look at the respective rows.
     */
    if needs_timetravel
        && (!TransactionIdIsValid((*builder).xmax)
            || TransactionIdFollowsOrEquals(xmax, (*builder).xmax))
    {
        (*builder).xmax = xmax;
        TransactionIdAdvance(&mut (*builder).xmax);
    }

    /* if there's any reason to build a historic snapshot, do so now */
    if needs_snapshot {
        /*
         * If we haven't built a complete snapshot yet there's no need to hand
         * it out, it wouldn't (and couldn't) be used anyway.
         */
        if (*builder).state < SNAPBUILD_FULL_SNAPSHOT {
            return;
        }

        /*
         * Decrease the snapshot builder's refcount of the old snapshot, note
         * that it still will be used if it has been handed out to the
         * reorderbuffer earlier.
         */
        if !(*builder).snapshot.is_null() {
            SnapBuildSnapDecRefcount((*builder).snapshot);
        }

        (*builder).snapshot = SnapBuildBuildSnapshot(builder);

        /* we might need to execute invalidations, add snapshot */
        if !ReorderBufferXidHasBaseSnapshot((*builder).reorder, xid) {
            SnapBuildSnapIncRefcount((*builder).snapshot);
            ReorderBufferSetBaseSnapshot((*builder).reorder, xid, lsn, (*builder).snapshot);
        }

        /* refcount of the snapshot builder for the new snapshot */
        SnapBuildSnapIncRefcount((*builder).snapshot);

        /*
         * Add a new catalog snapshot and invalidations messages to all
         * currently running transactions.
         */
        SnapBuildDistributeSnapshotAndInval(builder, lsn, xid);
    }
}

/*
 * Check the reorder buffer and the snapshot to see if the given transaction has
 * modified catalogs.
 */
#[inline]
unsafe fn SnapBuildXidHasCatalogChanges(
    builder: *mut SnapBuild,
    xid: TransactionId,
    xinfo: u32,
) -> bool {
    if ReorderBufferXidHasCatalogChanges((*builder).reorder, xid) {
        return true;
    }

    /*
     * The transactions that have changed catalogs must have invalidation
     * info.
     */
    if xinfo & XACT_XINFO_HAS_INVALS == 0 {
        return false;
    }

    /* Check the catchange XID array */
    (*builder).catchange.xcnt > 0
        && !bsearch(
            &xid as *const TransactionId as *const c_void,
            (*builder).catchange.xip as *const c_void,
            (*builder).catchange.xcnt,
            core::mem::size_of::<TransactionId>(),
            xidComparator,
        )
        .is_null()
}

/* -----------------------------------
 * Snapshot building functions dealing with xlog records
 * -----------------------------------
 */

/*
 * Process a running xacts record, and use its information to first build a
 * historic snapshot and later to release resources that aren't needed
 * anymore.
 */
pub unsafe fn SnapBuildProcessRunningXacts(
    builder: *mut SnapBuild,
    lsn: XLogRecPtr,
    running: *mut xl_running_xacts,
) {
    let txn: *mut ReorderBufferTXN;
    let xmin: TransactionId;

    /*
     * If we're not consistent yet, inspect the record to see whether it
     * allows to get closer to being consistent. If we are consistent, dump
     * our snapshot so others or we, after a restart, can use it.
     */
    if (*builder).state < SNAPBUILD_CONSISTENT {
        /* returns false if there's no point in performing cleanup just yet */
        if !SnapBuildFindSnapshot(builder, lsn, running) {
            return;
        }
    } else {
        SnapBuildSerialize(builder, lsn);
    }

    /*
     * Update range of interesting xids based on the running xacts
     * information. We don't increase ->xmax using it, because once we are in
     * a consistent state we can do that ourselves and much more efficiently
     * so, because we only need to do it for catalog transactions since we
     * only ever look at those.
     *
     * NB: We only increase xmax when a catalog modifying transaction commits
     * (see SnapBuildCommitTxn).  Because of this, xmax can be lower than
     * xmin, which looks odd but is correct and actually more efficient, since
     * we hit fast paths in heapam_visibility.c.
     */
    (*builder).xmin = (*running).oldestRunningXid;

    /* Remove transactions we don't need to keep track off anymore */
    SnapBuildPurgeOlderTxn(builder);

    /*
     * Advance the xmin limit for the current replication slot, to allow
     * vacuum to clean up the tuples this slot has been protecting.
     *
     * The reorderbuffer might have an xmin among the currently running
     * snapshots; use it if so.  If not, we need only consider the snapshots
     * we'll produce later, which can't be less than the oldest running xid in
     * the record we're reading now.
     */
    let xmin_candidate = ReorderBufferGetOldestXmin((*builder).reorder);
    let xmin = if xmin_candidate == InvalidTransactionId {
        (*running).oldestRunningXid
    } else {
        xmin_candidate
    };
    elog!(DEBUG3,
        "xmin: {}, xmax: {}, oldest running: {}, oldest xmin: {}",
        (*builder).xmin,
        (*builder).xmax,
        (*running).oldestRunningXid,
        xmin);
    LogicalIncreaseXminForSlot(lsn, xmin);

    /*
     * Also tell the slot where we can restart decoding from. We don't want to
     * do that after every commit because changing that implies an fsync of
     * the logical slot's state file, so we only do it every time we see a
     * running xacts record.
     *
     * Do so by looking for the oldest in progress transaction (determined by
     * the first LSN of any of its relevant records). Every transaction
     * remembers the last location we stored the snapshot to disk before its
     * beginning. That point is where we can restart from.
     */

    /*
     * Can't know about a serialized snapshot's location if we're not
     * consistent.
     */
    if (*builder).state < SNAPBUILD_CONSISTENT {
        return;
    }

    let txn = ReorderBufferGetOldestTXN((*builder).reorder);

    /*
     * oldest ongoing txn might have started when we didn't yet serialize
     * anything because we hadn't reached a consistent state yet.
     */
    if !txn.is_null() && (*txn).restart_decoding_lsn != InvalidXLogRecPtr {
        LogicalIncreaseRestartDecodingForSlot(lsn, (*txn).restart_decoding_lsn);
    }
    /*
     * No in-progress transaction, can reuse the last serialized snapshot if
     * we have one.
     */
    else if txn.is_null() {
        let reorder_shell = (*builder).reorder as *mut ReorderBufferShell;
        if (*reorder_shell).current_restart_decoding_lsn != InvalidXLogRecPtr
            && (*builder).last_serialized_snapshot != InvalidXLogRecPtr
        {
            LogicalIncreaseRestartDecodingForSlot(lsn, (*builder).last_serialized_snapshot);
        }
    }
}


/*
 * Build the start of a snapshot that's capable of decoding the catalog.
 *
 * Helper function for SnapBuildProcessRunningXacts() while we're not yet
 * consistent.
 *
 * Returns true if there is a point in performing internal maintenance/cleanup
 * using the xl_running_xacts record.
 */
unsafe fn SnapBuildFindSnapshot(
    builder: *mut SnapBuild,
    lsn: XLogRecPtr,
    running: *mut xl_running_xacts,
) -> bool {
    /* ---
     * Build catalog decoding snapshot incrementally using information about
     * the currently running transactions. There are several ways to do that:
     *
     * a) There were no running transactions when the xl_running_xacts record
     *    was inserted, jump to CONSISTENT immediately. We might find such a
     *    state while waiting on c)'s sub-states.
     *
     * b) This (in a previous run) or another decoding slot serialized a
     *    snapshot to disk that we can use. Can't use this method while finding
     *    the start point for decoding changes as the restart LSN would be an
     *    arbitrary LSN but we need to find the start point to extract changes
     *    where we won't see the data for partial transactions. Also, we cannot
     *    use this method when a slot needs a full snapshot for export or direct
     *    use, as that snapshot will only contain catalog modifying transactions.
     *
     * c) First incrementally build a snapshot for catalog tuples
     *    (BUILDING_SNAPSHOT), that requires all, already in-progress,
     *    transactions to finish.  Every transaction starting after that
     *    (FULL_SNAPSHOT state), has enough information to be decoded.  But
     *    for older running transactions no viable snapshot exists yet, so
     *    CONSISTENT will only be reached once all of those have finished.
     * ---
     */

    /*
     * xl_running_xacts record is older than what we can use, we might not
     * have all necessary catalog rows anymore.
     */
    if TransactionIdIsNormal((*builder).initial_xmin_horizon)
        && NormalTransactionIdPrecedes(
            (*running).oldestRunningXid,
            (*builder).initial_xmin_horizon,
        )
    {
        ereport!(DEBUG1, errmsg!(
                "skipping snapshot at {}/{} while building logical decoding snapshot, xmin horizon too low",
                LSN_FORMAT_ARGS(lsn).0,
                LSN_FORMAT_ARGS(lsn).1
            )) /* C also: errdetail_internal */;

        SnapBuildWaitSnapshot(running, (*builder).initial_xmin_horizon);

        return true;
    }

    /*
     * a) No transaction were running, we can jump to consistent.
     *
     * This is not affected by races around xl_running_xacts, because we can
     * miss transaction commits, but currently not transactions starting.
     *
     * NB: We might have already started to incrementally assemble a snapshot,
     * so we need to be careful to deal with that.
     */
    if (*running).oldestRunningXid == (*running).nextXid {
        if (*builder).start_decoding_at == InvalidXLogRecPtr
            || (*builder).start_decoding_at <= lsn
        {
            /* can decode everything after this */
            (*builder).start_decoding_at = lsn + 1;
        }

        /* As no transactions were running xmin/xmax can be trivially set. */
        (*builder).xmin = (*running).nextXid; /* < are finished */
        (*builder).xmax = (*running).nextXid; /* >= are running */

        /* so we can safely use the faster comparisons */
        Assert!(TransactionIdIsNormal((*builder).xmin));
        Assert!(TransactionIdIsNormal((*builder).xmax));

        (*builder).state = SNAPBUILD_CONSISTENT;
        (*builder).next_phase_at = InvalidTransactionId;

        ereport!(LOG, errmsg!("logical decoding found consistent point at {}/{}",
                LSN_FORMAT_ARGS(lsn).0,
                LSN_FORMAT_ARGS(lsn).1)) /* C also: errdetail */;

        return false;
    }

    /*
     * b) valid on disk state and while neither building full snapshot nor
     * creating a slot.
     */
    else if !(*builder).building_full_snapshot
        && !(*builder).in_slot_creation
        && SnapBuildRestore(builder, lsn)
    {
        /* there won't be any state to cleanup */
        return false;
    }

    /*
     * c) transition from START to BUILDING_SNAPSHOT.
     *
     * In START state, and a xl_running_xacts record with running xacts is
     * encountered.  In that case, switch to BUILDING_SNAPSHOT state, and
     * record xl_running_xacts->nextXid.  Once all running xacts have finished
     * (i.e. they're all >= nextXid), we have a complete catalog snapshot.  It
     * might look that we could use xl_running_xacts's ->xids information to
     * get there quicker, but that is problematic because transactions marked
     * as running, might already have inserted their commit record - it's
     * infeasible to change that with locking.
     */
    else if (*builder).state == SNAPBUILD_START {
        (*builder).state = SNAPBUILD_BUILDING_SNAPSHOT;
        (*builder).next_phase_at = (*running).nextXid;

        /*
         * Start with an xmin/xmax that's correct for future, when all the
         * currently running transactions have finished. We'll update both
         * while waiting for the pending transactions to finish.
         */
        (*builder).xmin = (*running).nextXid; /* < are finished */
        (*builder).xmax = (*running).nextXid; /* >= are running */

        /* so we can safely use the faster comparisons */
        Assert!(TransactionIdIsNormal((*builder).xmin));
        Assert!(TransactionIdIsNormal((*builder).xmax));

        ereport!(LOG, errmsg!("logical decoding found initial starting point at {}/{}",
                LSN_FORMAT_ARGS(lsn).0,
                LSN_FORMAT_ARGS(lsn).1)) /* C also: errdetail */;

        SnapBuildWaitSnapshot(running, (*running).nextXid);
    }

    /*
     * c) transition from BUILDING_SNAPSHOT to FULL_SNAPSHOT.
     *
     * In BUILDING_SNAPSHOT state, and this xl_running_xacts' oldestRunningXid
     * is >= than nextXid from when we switched to BUILDING_SNAPSHOT.  This
     * means all transactions starting afterwards have enough information to
     * be decoded.  Switch to FULL_SNAPSHOT.
     */
    else if (*builder).state == SNAPBUILD_BUILDING_SNAPSHOT
        && TransactionIdPrecedesOrEquals(
            (*builder).next_phase_at,
            (*running).oldestRunningXid,
        )
    {
        (*builder).state = SNAPBUILD_FULL_SNAPSHOT;
        (*builder).next_phase_at = (*running).nextXid;

        ereport!(LOG, errmsg!("logical decoding found initial consistent point at {}/{}",
                LSN_FORMAT_ARGS(lsn).0,
                LSN_FORMAT_ARGS(lsn).1)) /* C also: errdetail */;

        SnapBuildWaitSnapshot(running, (*running).nextXid);
    }

    /*
     * c) transition from FULL_SNAPSHOT to CONSISTENT.
     *
     * In FULL_SNAPSHOT state, and this xl_running_xacts' oldestRunningXid is
     * >= than nextXid from when we switched to FULL_SNAPSHOT.  This means all
     * transactions that are currently in progress have a catalog snapshot,
     * and all their changes have been collected.  Switch to CONSISTENT.
     */
    else if (*builder).state == SNAPBUILD_FULL_SNAPSHOT
        && TransactionIdPrecedesOrEquals(
            (*builder).next_phase_at,
            (*running).oldestRunningXid,
        )
    {
        (*builder).state = SNAPBUILD_CONSISTENT;
        (*builder).next_phase_at = InvalidTransactionId;

        ereport!(LOG, errmsg!("logical decoding found consistent point at {}/{}",
                LSN_FORMAT_ARGS(lsn).0,
                LSN_FORMAT_ARGS(lsn).1)) /* C also: errdetail */;
    }

    /*
     * We already started to track running xacts and need to wait for all
     * in-progress ones to finish. We fall through to the normal processing of
     * records so incremental cleanup can be performed.
     */
    true
}

/* ---
 * Iterate through xids in record, wait for all older than the cutoff to
 * finish.  Then, if possible, log a new xl_running_xacts record.
 *
 * This isn't required for the correctness of decoding, but to:
 * a) allow isolationtester to notice that we're currently waiting for
 *    something.
 * b) log a new xl_running_xacts record where it'd be helpful, without having
 *    to wait for bgwriter or checkpointer.
 * ---
 */
unsafe fn SnapBuildWaitSnapshot(running: *mut xl_running_xacts, cutoff: TransactionId) {
    let mut off: usize = 0;

    while off < (*running).xcnt as usize {
        let xid: TransactionId = *(*running).xids.as_ptr().add(off);

        /*
         * Upper layers should prevent that we ever need to wait on ourselves.
         * Check anyway, since failing to do so would either result in an
         * endless wait or an Assert() failure.
         */
        if TransactionIdIsCurrentTransactionId(xid) {
            ereport!(ERROR, errmsg!("waiting for ourselves"));
        }

        if TransactionIdFollows(xid, cutoff) {
            off += 1;
            continue;
        }

        XactLockTableWait(xid, core::ptr::null_mut(), core::ptr::null_mut(), XLTW_None);

        off += 1;
    }

    /*
     * All transactions we needed to finish finished - try to ensure there is
     * another xl_running_xacts record in a timely manner, without having to
     * wait for bgwriter or checkpointer to log one.  During recovery we can't
     * enforce that, so we'll have to wait.
     */
    if !RecoveryInProgress() {
        LogStandbySnapshot();
    }
}

// ===========================================================================
// Part 4 - Serialization / deserialization / checkpoint cleanup
// ===========================================================================

/*
 * Store/Load a snapshot from disk, depending on the snapshot builder's state.
 *
 * Supposed to be used by external (i.e. not snapbuild.c) code that just read
 * a record that's a potential location for a serialized snapshot.
 */
pub unsafe fn SnapBuildSerializationPoint(builder: *mut SnapBuild, lsn: XLogRecPtr) {
    if (*builder).state < SNAPBUILD_CONSISTENT {
        SnapBuildRestore(builder, lsn);
    } else {
        SnapBuildSerialize(builder, lsn);
    }
}

/*
 * Serialize the snapshot 'builder' at the location 'lsn' if it hasn't already
 * been done by another decoding process.
 */
unsafe fn SnapBuildSerialize(builder: *mut SnapBuild, lsn: XLogRecPtr) {
    let needed_length: Size;
    let mut ondisk: *mut SnapBuildOnDisk = core::ptr::null_mut();
    let mut catchange_xip: *mut TransactionId = core::ptr::null_mut();
    let old_ctx: MemoryContext;
    let catchange_xcnt: Size;
    let mut ondisk_c: *mut u8;
    let fd: c_int;
    let mut tmppath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let ret: c_int;
    let mut stat_buf: StatBuf = StatBuf { _pad: [0; 128] };
    let mut sz: Size;

    Assert!(lsn != InvalidXLogRecPtr);
    Assert!(
        (*builder).last_serialized_snapshot == InvalidXLogRecPtr
            || (*builder).last_serialized_snapshot <= lsn
    );

    /*
     * no point in serializing if we cannot continue to work immediately after
     * restoring the snapshot
     */
    if (*builder).state < SNAPBUILD_CONSISTENT {
        return;
    }

    /* consistent snapshots have no next phase */
    Assert!((*builder).next_phase_at == InvalidTransactionId);

    /*
     * We identify snapshots by the LSN they are valid for. We don't need to
     * include timelines in the name as each LSN maps to exactly one timeline
     * unless the user used pg_resetwal or similar. If a user did so, there's
     * no hope continuing to decode anyway.
     */
    {
        let fmt = b"%s/%X-%X.snap\0";
        sprintf(
            path.as_mut_ptr(),
            fmt.as_ptr() as *const c_char,
            PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char,
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1,
        );
    }

    /*
     * first check whether some other backend already has written the snapshot
     * for this LSN. It's perfectly fine if there's none, so we accept ENOENT
     * as a valid state. Everything else is an unexpected error.
     */
    ret = stat(path.as_ptr(), &mut stat_buf);

    if ret != 0 && get_errno() != ENOENT {
        ereport!(ERROR, errmsg!("could not stat file \"{}\": {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    } else if ret == 0 {
        /*
         * somebody else has already serialized to this point, don't overwrite
         * but remember location, so we don't need to read old data again.
         *
         * To be sure it has been synced to disk after the rename() from the
         * tempfile filename to the real filename, we just repeat the fsync.
         * That ought to be cheap because in most scenarios it should already
         * be safely on disk.
         */
        fsync_fname(path.as_ptr(), false);
        fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char, true);

        (*builder).last_serialized_snapshot = lsn;
        // goto out
        ReorderBufferSetRestartPoint((*builder).reorder, (*builder).last_serialized_snapshot);
        /* be tidy - nothing to free yet */
        return;
    }

    /*
     * there is an obvious race condition here between the time we stat(2) the
     * file and us writing the file. But we rename the file into place
     * atomically and all files created need to contain the same data anyway,
     * so this is perfectly fine, although a bit of a resource waste. Locking
     * seems like pointless complication.
     */
    elog!(DEBUG1, "serializing snapshot to {}",
        core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""));

    /* to make sure only we will write to this tempfile, include pid */
    {
        let fmt = b"%s/%X-%X.snap.%d.tmp\0";
        sprintf(
            tmppath.as_mut_ptr(),
            fmt.as_ptr() as *const c_char,
            PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char,
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1,
            MyProcPid,
        );
    }

    /*
     * Unlink temporary file if it already exists, needs to have been before a
     * crash/error since we won't enter this function twice from within a
     * single decoding slot/backend and the temporary file contains the pid of
     * the current process.
     */
    if unlink(tmppath.as_ptr()) != 0 && get_errno() != ENOENT {
        ereport!(ERROR, errmsg!("could not remove file \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    old_ctx = MemoryContextSwitchTo((*builder).context as crate::utils::palloc::MemoryContext);

    /* Get the catalog modifying transactions that are yet not committed */
    catchange_xip = ReorderBufferGetCatalogChangesXacts((*builder).reorder);
    let reorder_shell = (*builder).reorder as *mut ReorderBufferShell;
    catchange_xcnt = dclist_count(&(*reorder_shell).catchange_txns);

    needed_length = core::mem::size_of::<SnapBuildOnDisk>()
        + core::mem::size_of::<TransactionId>()
            * ((*builder).committed.xcnt + catchange_xcnt);

    ondisk_c = palloc0(needed_length) as *mut u8;
    ondisk = ondisk_c as *mut SnapBuildOnDisk;
    (*ondisk).magic = SNAPBUILD_MAGIC;
    (*ondisk).version = SNAPBUILD_VERSION;
    (*ondisk).length = needed_length as u32;
    let mut checksum = INIT_CRC32C();
    checksum = COMP_CRC32C(
        checksum,
        (ondisk as *const u8).add(snap_build_on_disk_not_checksummed_size()) as *const c_void,
        snap_build_on_disk_constant_size() - snap_build_on_disk_not_checksummed_size(),
    );
    ondisk_c = ondisk_c.add(core::mem::size_of::<SnapBuildOnDisk>());

    core::ptr::copy_nonoverlapping(
        builder as *const u8,
        &mut (*ondisk).builder as *mut SnapBuild as *mut u8,
        core::mem::size_of::<SnapBuild>(),
    );
    /* NULL-ify memory-only data */
    (*ondisk).builder.context = core::ptr::null_mut();
    (*ondisk).builder.snapshot = core::ptr::null_mut();
    (*ondisk).builder.reorder = core::ptr::null_mut();
    (*ondisk).builder.committed.xip = core::ptr::null_mut();
    (*ondisk).builder.catchange.xip = core::ptr::null_mut();
    /* update catchange only on disk data */
    (*ondisk).builder.catchange.xcnt = catchange_xcnt;

    checksum = COMP_CRC32C(
        checksum,
        &(*ondisk).builder as *const SnapBuild as *const c_void,
        core::mem::size_of::<SnapBuild>(),
    );

    /* copy committed xacts */
    if (*builder).committed.xcnt > 0 {
        sz = core::mem::size_of::<TransactionId>() * (*builder).committed.xcnt;
        core::ptr::copy_nonoverlapping((*builder).committed.xip, ondisk_c as *mut TransactionId, (*builder).committed.xcnt);
        checksum = COMP_CRC32C(checksum, ondisk_c as *const c_void, sz);
        ondisk_c = ondisk_c.add(sz);
    }

    /* copy catalog modifying xacts */
    if catchange_xcnt > 0 {
        sz = core::mem::size_of::<TransactionId>() * catchange_xcnt;
        core::ptr::copy_nonoverlapping(catchange_xip, ondisk_c as *mut TransactionId, catchange_xcnt);
        checksum = COMP_CRC32C(checksum, ondisk_c as *const c_void, sz);
        ondisk_c = ondisk_c.add(sz);
    }

    (*ondisk).checksum = FIN_CRC32C(checksum);

    /* we have valid data now, open tempfile and write it there */
    let fd = OpenTransientFile(tmppath.as_ptr(), O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
    if fd < 0 {
        ereport!(ERROR, errmsg!("could not open file \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_WRITE);
    if write(fd, ondisk as *const c_void, needed_length) != needed_length as isize {
        let save_errno = get_errno();

        CloseTransientFile(fd);

        /* if write didn't set errno, assume problem is no disk space */
        set_errno(if save_errno != 0 { save_errno } else { ENOSPC });
        ereport!(ERROR, errmsg!("could not write to file \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }
    pgstat_report_wait_end();

    /*
     * fsync the file before renaming so that even if we crash after this we
     * have either a fully valid file or nothing.
     *
     * It's safe to just ERROR on fsync() here because we'll retry the whole
     * operation including the writes.
     *
     * TODO: Do the fsync() via checkpoints/restartpoints, doing it here has
     * some noticeable overhead since it's performed synchronously during
     * decoding?
     */
    pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_SYNC);
    if pg_fsync(fd) != 0 {
        let save_errno = get_errno();

        CloseTransientFile(fd);
        set_errno(save_errno);
        ereport!(ERROR, errmsg!("could not fsync file \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, errmsg!("could not close file \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char, true);

    /*
     * We may overwrite the work from some other backend, but that's ok, our
     * snapshot is valid as well, we'll just have done some superfluous work.
     */
    if rename(tmppath.as_ptr(), path.as_ptr()) != 0 {
        ereport!(ERROR, errmsg!("could not rename file \"{}\" to \"{}\": {}",
                core::ffi::CStr::from_ptr(tmppath.as_ptr()).to_str().unwrap_or(""),
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    /* make sure we persist */
    fsync_fname(path.as_ptr(), false);
    fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char, true);

    /*
     * Now there's no way we can lose the dumped state anymore, remember this
     * as a serialization point.
     */
    (*builder).last_serialized_snapshot = lsn;

    MemoryContextSwitchTo(old_ctx);

    // out:
    ReorderBufferSetRestartPoint((*builder).reorder, (*builder).last_serialized_snapshot);
    /* be tidy */
    if !ondisk.is_null() {
        pfree(ondisk as *mut c_void);
    }
    if !catchange_xip.is_null() {
        pfree(catchange_xip as *mut c_void);
    }
}

/*
 * Restore the logical snapshot file contents to 'ondisk'.
 *
 * 'context' is the memory context where the catalog modifying/committed xid
 * will live.
 * If 'missing_ok' is true, will not throw an error if the file is not found.
 */
pub unsafe fn SnapBuildRestoreSnapshot(
    ondisk: *mut SnapBuildOnDisk,
    lsn: XLogRecPtr,
    context: MemoryContext,
    missing_ok: bool,
) -> bool {
    let fd: c_int;
    let mut checksum: pg_crc32c;
    let mut sz: Size;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    {
        let fmt = b"%s/%X-%X.snap\0";
        sprintf(
            path.as_mut_ptr(),
            fmt.as_ptr() as *const c_char,
            PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char,
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1,
        );
    }

    let fd = OpenTransientFile(path.as_ptr(), O_RDONLY | PG_BINARY);

    if fd < 0 {
        if missing_ok && get_errno() == ENOENT {
            return false;
        }

        ereport!(ERROR, errmsg!("could not open file \"{}\": {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    /* ----
     * Make sure the snapshot had been stored safely to disk, that's normally
     * cheap.
     * Note that we do not need PANIC here, nobody will be able to use the
     * slot without fsyncing, and saving it won't succeed without an fsync()
     * either...
     * ----
     */
    fsync_fname(path.as_ptr(), false);
    fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char, true);

    /* read statically sized portion of snapshot */
    SnapBuildRestoreContents(fd, ondisk as *mut c_void, snap_build_on_disk_constant_size(), path.as_ptr());

    if (*ondisk).magic != SNAPBUILD_MAGIC {
        ereport!(ERROR, errmsg!("snapbuild state file \"{}\" has wrong magic number: {} instead of {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                (*ondisk).magic,
                SNAPBUILD_MAGIC)) /* C also: errcode */;
    }

    if (*ondisk).version != SNAPBUILD_VERSION {
        ereport!(ERROR, errmsg!("snapbuild state file \"{}\" has unsupported version: {} instead of {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                (*ondisk).version,
                SNAPBUILD_VERSION)) /* C also: errcode */;
    }

    checksum = INIT_CRC32C();
    checksum = COMP_CRC32C(
        checksum,
        (ondisk as *const u8).add(snap_build_on_disk_not_checksummed_size()) as *const c_void,
        snap_build_on_disk_constant_size() - snap_build_on_disk_not_checksummed_size(),
    );

    /* read SnapBuild */
    SnapBuildRestoreContents(
        fd,
        &mut (*ondisk).builder as *mut SnapBuild as *mut c_void,
        core::mem::size_of::<SnapBuild>(),
        path.as_ptr(),
    );
    checksum = COMP_CRC32C(
        checksum,
        &(*ondisk).builder as *const SnapBuild as *const c_void,
        core::mem::size_of::<SnapBuild>(),
    );

    /* restore committed xacts information */
    if (*ondisk).builder.committed.xcnt > 0 {
        sz = core::mem::size_of::<TransactionId>() * (*ondisk).builder.committed.xcnt;
        (*ondisk).builder.committed.xip =
            MemoryContextAllocZero(context, sz) as *mut TransactionId;
        SnapBuildRestoreContents(fd, (*ondisk).builder.committed.xip as *mut c_void, sz, path.as_ptr());
        checksum = COMP_CRC32C(
            checksum,
            (*ondisk).builder.committed.xip as *const c_void,
            sz,
        );
    }

    /* restore catalog modifying xacts information */
    if (*ondisk).builder.catchange.xcnt > 0 {
        sz = core::mem::size_of::<TransactionId>() * (*ondisk).builder.catchange.xcnt;
        (*ondisk).builder.catchange.xip =
            MemoryContextAllocZero(context, sz) as *mut TransactionId;
        SnapBuildRestoreContents(fd, (*ondisk).builder.catchange.xip as *mut c_void, sz, path.as_ptr());
        checksum = COMP_CRC32C(
            checksum,
            (*ondisk).builder.catchange.xip as *const c_void,
            sz,
        );
    }

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, errmsg!("could not close file \"{}\": {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    checksum = FIN_CRC32C(checksum);

    /* verify checksum of what we've read */
    if !EQ_CRC32C(checksum, (*ondisk).checksum) {
        ereport!(ERROR, errmsg!("checksum mismatch for snapbuild state file \"{}\": is {}, should be {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                checksum,
                (*ondisk).checksum)) /* C also: errcode */;
    }

    true
}

/*
 * Restore a snapshot into 'builder' if previously one has been stored at the
 * location indicated by 'lsn'. Returns true if successful, false otherwise.
 */
unsafe fn SnapBuildRestore(builder: *mut SnapBuild, lsn: XLogRecPtr) -> bool {
    let mut ondisk: SnapBuildOnDisk = core::mem::zeroed();

    /* no point in loading a snapshot if we're already there */
    if (*builder).state == SNAPBUILD_CONSISTENT {
        return false;
    }

    /* validate and restore the snapshot to 'ondisk' */
    if !SnapBuildRestoreSnapshot(&mut ondisk, lsn, (*builder).context as crate::utils::palloc::MemoryContext, true) {
        return false;
    }

    /*
     * ok, we now have a sensible snapshot here, figure out if it has more
     * information than we have.
     */

    /*
     * We are only interested in consistent snapshots for now, comparing
     * whether one incomplete snapshot is more "advanced" seems to be
     * unnecessarily complex.
     */
    if ondisk.builder.state < SNAPBUILD_CONSISTENT {
        // goto snapshot_not_interesting
        if !ondisk.builder.committed.xip.is_null() {
            pfree(ondisk.builder.committed.xip as *mut c_void);
        }
        if !ondisk.builder.catchange.xip.is_null() {
            pfree(ondisk.builder.catchange.xip as *mut c_void);
        }
        return false;
    }

    /*
     * Don't use a snapshot that requires an xmin that we cannot guarantee to
     * be available.
     */
    if TransactionIdPrecedes(ondisk.builder.xmin, (*builder).initial_xmin_horizon) {
        // goto snapshot_not_interesting
        if !ondisk.builder.committed.xip.is_null() {
            pfree(ondisk.builder.committed.xip as *mut c_void);
        }
        if !ondisk.builder.catchange.xip.is_null() {
            pfree(ondisk.builder.catchange.xip as *mut c_void);
        }
        return false;
    }

    /*
     * Consistent snapshots have no next phase. Reset next_phase_at as it is
     * possible that an old value may remain.
     */
    Assert!(ondisk.builder.next_phase_at == InvalidTransactionId);
    (*builder).next_phase_at = InvalidTransactionId;

    /* ok, we think the snapshot is sensible, copy over everything important */
    (*builder).xmin = ondisk.builder.xmin;
    (*builder).xmax = ondisk.builder.xmax;
    (*builder).state = ondisk.builder.state;

    (*builder).committed.xcnt = ondisk.builder.committed.xcnt;
    /* We only allocated/stored xcnt, not xcnt_space xids ! */
    /* don't overwrite preallocated xip, if we don't have anything here */
    if (*builder).committed.xcnt > 0 {
        pfree((*builder).committed.xip as *mut c_void);
        (*builder).committed.xcnt_space = ondisk.builder.committed.xcnt;
        (*builder).committed.xip = ondisk.builder.committed.xip;
    }
    ondisk.builder.committed.xip = core::ptr::null_mut();

    /* set catalog modifying transactions */
    if !(*builder).catchange.xip.is_null() {
        pfree((*builder).catchange.xip as *mut c_void);
    }
    (*builder).catchange.xcnt = ondisk.builder.catchange.xcnt;
    (*builder).catchange.xip = ondisk.builder.catchange.xip;
    ondisk.builder.catchange.xip = core::ptr::null_mut();

    /* our snapshot is not interesting anymore, build a new one */
    if !(*builder).snapshot.is_null() {
        SnapBuildSnapDecRefcount((*builder).snapshot);
    }
    (*builder).snapshot = SnapBuildBuildSnapshot(builder);
    SnapBuildSnapIncRefcount((*builder).snapshot);

    ReorderBufferSetRestartPoint((*builder).reorder, lsn);

    Assert!((*builder).state == SNAPBUILD_CONSISTENT);

    ereport!(LOG, errmsg!("logical decoding found consistent point at {}/{}",
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1)) /* C also: errdetail */;
    true
}

/*
 * Read the contents of the serialized snapshot to 'dest'.
 */
unsafe fn SnapBuildRestoreContents(
    fd: c_int,
    dest: *mut c_void,
    size: Size,
    path: *const c_char,
) {
    let read_bytes: isize;

    pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_READ);
    read_bytes = read(fd, dest, size);
    pgstat_report_wait_end();
    if read_bytes != size as isize {
        let save_errno = get_errno();

        CloseTransientFile(fd);

        if read_bytes < 0 {
            set_errno(save_errno);
            ereport!(ERROR, errmsg!("could not read file \"{}\": {}",
                    core::ffi::CStr::from_ptr(path).to_str().unwrap_or(""),
                    get_errno())) /* C also: errcode_for_file_access */;
        } else {
            ereport!(ERROR, errmsg!("could not read file \"{}\": read {} of {}",
                    core::ffi::CStr::from_ptr(path).to_str().unwrap_or(""),
                    read_bytes,
                    size)) /* C also: errcode */;
        }
    }
}

/*
 * Remove all serialized snapshots that are not required anymore because no
 * slot can need them. This doesn't actually have to run during a checkpoint,
 * but it's a convenient point to schedule this.
 *
 * NB: We run this during checkpoints even if logical decoding is disabled so
 * we cleanup old slots at some point after it got disabled.
 */
pub unsafe fn CheckPointSnapBuild() {
    let mut cutoff: XLogRecPtr;
    let redo: XLogRecPtr;
    let snap_dir: *mut DIR;
    let mut snap_de: *mut dirent;
    let mut path: [c_char; MAXPGPATH + 64] = [0; MAXPGPATH + 64];

    /*
     * We start off with a minimum of the last redo pointer. No new
     * replication slot will start before that, so that's a safe upper bound
     * for removal.
     */
    redo = GetRedoRecPtr();

    /* now check for the restart ptrs from existing slots */
    cutoff = ReplicationSlotsComputeLogicalRestartLSN();

    /* don't start earlier than the restart lsn */
    if redo < cutoff {
        cutoff = redo;
    }

    snap_dir = AllocateDir(PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char);
    loop {
        snap_de = ReadDir(snap_dir, PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char);
        if snap_de.is_null() {
            break;
        }

        let mut hi: u32 = 0;
        let mut lo: u32 = 0;
        let lsn: XLogRecPtr;
        let de_type: PGFileType;

        let name_cstr = core::ffi::CStr::from_ptr((*snap_de).d_name.as_ptr());
        let name_str = name_cstr.to_str().unwrap_or("");
        if name_str == "." || name_str == ".." {
            continue;
        }

        {
            let fmt = b"%s/%s\0";
            snprintf(
                path.as_mut_ptr(),
                path.len(),
                fmt.as_ptr() as *const c_char,
                PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char,
                (*snap_de).d_name.as_ptr(),
            );
        }
        de_type = get_dirent_type(path.as_ptr(), snap_de, false, DEBUG1);

        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_REG {
            elog!(DEBUG1, "only regular files expected: {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""));
            continue;
        }

        /*
         * temporary filenames from SnapBuildSerialize() include the LSN and
         * everything but are postfixed by .$pid.tmp. We can just remove them
         * the same as other files because there can be none that are
         * currently being written that are older than cutoff.
         *
         * We just log a message if a file doesn't fit the pattern, it's
         * probably some editors lock/state file or similar...
         */
        {
            let fmt = b"%X-%X.snap\0";
            let rc = sscanf(
                (*snap_de).d_name.as_ptr(),
                fmt.as_ptr() as *const c_char,
                &mut hi as *mut u32,
                &mut lo as *mut u32,
            );
            if rc != 2 {
                ereport!(LOG, errmsg!("could not parse file name \"{}\"",
                        core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or("")));
                continue;
            }
        }

        lsn = ((hi as u64) << 32) | (lo as u64);

        /* check whether we still need it */
        if lsn < cutoff || cutoff == InvalidXLogRecPtr {
            elog!(DEBUG1, "removing snapbuild snapshot {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""));

            /*
             * It's not particularly harmful, though strange, if we can't
             * remove the file here. Don't prevent the checkpoint from
             * completing, that'd be a cure worse than the disease.
             */
            if unlink(path.as_ptr()) < 0 {
                ereport!(LOG, errmsg!("could not remove file \"{}\": {}",
                        core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                        get_errno())) /* C also: errcode_for_file_access */;
                continue;
            }
        }
    }
    FreeDir(snap_dir);
}

/*
 * Check if a logical snapshot at the specified point has been serialized.
 */
pub unsafe fn SnapBuildSnapshotExists(lsn: XLogRecPtr) -> bool {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let ret: c_int;
    let mut stat_buf: StatBuf = StatBuf { _pad: [0; 128] };

    {
        let fmt = b"%s/%X-%X.snap\0";
        sprintf(
            path.as_mut_ptr(),
            fmt.as_ptr() as *const c_char,
            PG_LOGICAL_SNAPSHOTS_DIR.as_ptr() as *const c_char,
            LSN_FORMAT_ARGS(lsn).0,
            LSN_FORMAT_ARGS(lsn).1,
        );
    }

    ret = stat(path.as_ptr(), &mut stat_buf);

    if ret != 0 && get_errno() != ENOENT {
        ereport!(ERROR, errmsg!("could not stat file \"{}\": {}",
                core::ffi::CStr::from_ptr(path.as_ptr()).to_str().unwrap_or(""),
                get_errno())) /* C also: errcode_for_file_access */;
    }

    ret == 0
}

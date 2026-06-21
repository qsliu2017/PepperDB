/* -------------------------------------------------------------------------
 *
 * decode.rs
 *      This module decodes WAL records read using xlogreader.h's APIs for the
 *      purpose of logical decoding by passing information to the
 *      reorderbuffer module (containing the actual changes) and to the
 *      snapbuild module to build a fitting catalog snapshot (to be able to
 *      properly decode the changes in the reorderbuffer).
 *
 * NOTE:
 *      This basically tries to handle all low level xlog stuff for
 *      reorderbuffer.c and snapbuild.c. There's some minor leakage where a
 *      specific record's struct is used to pass data along, but those just
 *      happen to contain the right amount of data in a convenient
 *      format. There isn't and shouldn't be much intelligence about the
 *      contents of records in here except turning them into a more usable
 *      format.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/replication/logical/decode.rs
 *
 * -------------------------------------------------------------------------
 */
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Imports from real homes in the ported tree
// ---------------------------------------------------------------------------

// access/transam/xlogreader.rs - XLogReaderState, accessor functions,
//   RepOriginId / InvalidRepOriginId, XLogRecPtr.
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockData, XLogRecGetBlockTag, XLogRecGetData,
    XLogRecGetDataLen, XLogRecGetInfo, XLogRecGetOrigin, XLogRecGetRmid,
    XLogRecGetTopXid, XLogRecGetXid, RepOriginId,
};
// access/transam/xlogdefs.rs - XLogRecPtr.
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr};

// access/transam/xlog_internal.rs - GetRmgr / RmgrData / XLogRecordBuffer (void stub).
use crate::access::transam::xlog_internal::{GetRmgr, RmgrData, XLogRecordBuffer as _XLogRecordBufferVoid};

// access/rmgrdesc/xlogdesc.rs - XLOG_* opcodes + xl_parameter_change + WAL_LEVEL_LOGICAL.
use crate::access::rmgrdesc::xlogdesc::{
    WAL_LEVEL_LOGICAL, XLOG_BACKUP_END, XLOG_CHECKPOINT_ONLINE,
    XLOG_CHECKPOINT_REDO, XLOG_CHECKPOINT_SHUTDOWN, XLOG_END_OF_RECOVERY,
    XLOG_FPI, XLOG_FPI_FOR_HINT, XLOG_FPW_CHANGE, XLOG_NEXTOID, XLOG_NOOP,
    XLOG_OVERWRITE_CONTRECORD, XLOG_PARAMETER_CHANGE, XLOG_RESTORE_POINT,
    XLOG_SWITCH, xl_parameter_change,
};

// access/rmgrdesc/xactdesc.rs - XLOG_XACT_* opcodes, parsed record types,
//   ParseCommitRecord / ParseAbortRecord / ParsePrepareRecord, XACT_XINFO_HAS_ORIGIN.
use crate::access::rmgrdesc::xactdesc::{
    ParseAbortRecord, ParseCommitRecord, ParsePrepareRecord, XACT_XINFO_HAS_ORIGIN,
    XLOG_XACT_ABORT, XLOG_XACT_ABORT_PREPARED, XLOG_XACT_ASSIGNMENT,
    XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_INVALIDATIONS,
    XLOG_XACT_OPMASK, XLOG_XACT_PREPARE, xl_xact_abort, xl_xact_commit,
    xl_xact_invals, xl_xact_parsed_abort, xl_xact_parsed_commit,
    xl_xact_parsed_prepare, xl_xact_prepare,
};

// access/rmgrdesc/heapdesc.rs - XLOG_HEAP_* / XLOG_HEAP2_* opcodes + XLH_* flags +
//   heap record structs.
use crate::access::rmgrdesc::heapdesc::{
    XLOG_HEAP2_LOCK_UPDATED, XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP2_NEW_CID,
    XLOG_HEAP2_PRUNE_ON_ACCESS, XLOG_HEAP2_PRUNE_VACUUM_CLEANUP,
    XLOG_HEAP2_PRUNE_VACUUM_SCAN, XLOG_HEAP2_REWRITE, XLOG_HEAP2_VISIBLE,
    XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE,
    XLOG_HEAP_INPLACE, XLOG_HEAP_INSERT, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK,
    XLOG_HEAP_TRUNCATE, XLOG_HEAP_UPDATE, XLH_DELETE_CONTAINS_OLD,
    XLH_DELETE_IS_SUPER, XLH_INSERT_CONTAINS_NEW_TUPLE, XLH_INSERT_IS_SPECULATIVE,
    XLH_INSERT_LAST_IN_MULTI, XLH_INSERT_ON_TOAST_RELATION,
    XLH_TRUNCATE_CASCADE, XLH_TRUNCATE_RESTART_SEQS,
    XLH_UPDATE_CONTAINS_NEW_TUPLE, XLH_UPDATE_CONTAINS_OLD, xl_heap_delete,
    xl_heap_header, xl_heap_insert, xl_heap_multi_insert, xl_heap_new_cid,
    xl_heap_truncate, xl_heap_update, xl_multi_insert_tuple,
};

// access/rmgrdesc/standbydesc.rs - XLOG_RUNNING_XACTS / XLOG_STANDBY_LOCK /
//   XLOG_INVALIDATIONS + xl_running_xacts.
use crate::access::rmgrdesc::standbydesc::{
    XLOG_INVALIDATIONS, XLOG_RUNNING_XACTS, XLOG_STANDBY_LOCK, xl_running_xacts,
};

// access/rmgrdesc/logicalmsgdesc.rs - XLOG_LOGICAL_MESSAGE + xl_logical_message.
use crate::access::rmgrdesc::logicalmsgdesc::{XLOG_LOGICAL_MESSAGE, xl_logical_message};

// access/htup_details.rs - HeapTupleData / HeapTupleHeader / SizeofHeapTupleHeader.
use crate::access::htup_details::{HeapTupleData, HeapTupleHeader, HeapTupleHeaderData, SizeofHeapTupleHeader};

// access/transam/xlogreader.rs - XLR_INFO_MASK (re-exported from there).
use crate::access::transam::xlogreader::XLR_INFO_MASK;

// c.rs / prelude - primitive types.
use crate::c::{uint8, Size, TransactionId};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::storage::relfilelocator::RelFileLocator;
// TimestampTz: use the local typedef from xactdesc (same int64 alias as everywhere else).
use crate::access::rmgrdesc::xactdesc::TimestampTz;

// replication/snapbuild_internal.rs - SnapBuild + SnapBuildState constants.
use crate::replication::snapbuild_internal::{SnapBuild, SnapBuildState};

// replication/output_plugin.rs - OutputPluginCallbacks.
use crate::replication::output_plugin::OutputPluginCallbacks;

// ---------------------------------------------------------------------------
// XLogRecordBuffer (from decode.h merged here)
//
// The C definition lives in replication/decode.h; since xlog_internal.rs
// only defines a void alias we provide the real layout here and expose it
// as the canonical definition for this crate.
// ---------------------------------------------------------------------------

/// XLogRecordBuffer: groups the original/end LSN pointers with the decoded
/// WAL record pointer for the duration of a single record's dispatch.
///
/// Mirrors `typedef struct XLogRecordBuffer` in replication/decode.h.
#[repr(C)]
pub struct XLogRecordBuffer {
    pub origptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    /// Pointer to the in-progress XLogReaderState whose current record is
    /// the one being decoded.
    pub record: *mut XLogReaderState,
}

// ---------------------------------------------------------------------------
// LogicalDecodingContext
//
// The full struct lives in replication/logical.h.  The logicalfuncs.rs stub
// only has a minimal projection; we need the complete layout here.
// Fields are laid out to match the C struct exactly (repr(C)).
// ---------------------------------------------------------------------------

/// Callback set carried by LogicalDecodingContext.  The C typedef is
/// `LogicalDecodingContext.callbacks` (OutputPluginCallbacks from
/// replication/output_plugin.h).  We use the type from output_plugin.rs.
///
/// Wrappers for individual callbacks called from this module.
// These are declared extern (stubs) because they call back into the
// output plugin loaded at runtime.  Until replication/logical.c is ported,
// they remain unimplemented stubs.
unsafe fn filter_prepare_cb_wrapper(ctx: *mut LogicalDecodingContext,
                                     xid: TransactionId,
                                     gid: *const c_char) -> bool { crate::replication::logical::logical::filter_prepare_cb_wrapper(ctx as _, xid as _, gid as _) }

unsafe fn filter_by_origin_cb_wrapper(ctx: *mut LogicalDecodingContext,
                                       origin_id: RepOriginId) -> bool { crate::replication::logical::logical::filter_by_origin_cb_wrapper(ctx as _, origin_id as _) }

/// Full LogicalDecodingContext layout (replication/logical.h).
#[repr(C)]
pub struct LogicalDecodingContext {
    /// The memory context of the decoding context itself.
    pub context: *mut c_void, // MemoryContext
    /// Output plugin's callbacks.
    pub callbacks: OutputPluginCallbacks,
    /// Options passed to the output plugin.
    pub options: *mut c_void, // OutputPluginOptions*
    /// Replication slot associated with this decoding context.
    pub slot: *mut ReplicationSlot,
    /// Snapshot builder state.
    pub snapshot_builder: *mut SnapBuild,
    /// Reorder buffer.
    pub reorder: *mut ReorderBuffer,
    /// WAL reader.
    pub reader: *mut XLogReaderState,
    /// Whether we are just fast-forwarding through the WAL without decoding.
    pub fast_forward: bool,
    /// Whether the output plugin wants to process two-phase (prepared) xacts.
    pub twophase: bool,
    /// Set true when a record would have been processed if not for fast_forward.
    pub processing_required: bool,
    /// Internal output buffer.
    pub out: *mut c_void, // StringInfo
    /// Scratch memory context, reset between top-level transactions.
    pub reorder_context: *mut c_void, // MemoryContext
}

// ---------------------------------------------------------------------------
// Opaque types not yet ported
// ---------------------------------------------------------------------------

/// ReorderBuffer: opaque until reorderbuffer.c is ported.
pub type ReorderBuffer = c_void;

/// ReplicationSlotPersistentData (minimal projection used here).
#[repr(C)]
pub struct ReplicationSlotPersistentData {
    /// OID of the database this slot is for.
    pub database: Oid,
    // other fields omitted until slot.h is ported
}

/// ReplicationSlot (minimal projection used here).
#[repr(C)]
pub struct ReplicationSlot {
    pub data: ReplicationSlotPersistentData,
}

/// ReorderBufferChange: opaque stub.
pub type ReorderBufferChange = c_void;

/// Snapshot: opaque pointer alias (real definition is in utils/snapshot.h).
// Already defined in utils/snapshot.rs; re-use via the import above.

// ---------------------------------------------------------------------------
// Heap size constants (mirrors of macros in access/heapam_xlog.h).
// Computed from the struct layouts in heapdesc.rs.
// ---------------------------------------------------------------------------

/// SizeOfHeapHeader = sizeof(xl_heap_header).
#[inline(always)]
const fn size_of_heap_header() -> Size {
    core::mem::size_of::<xl_heap_header>()
}

/// SizeOfHeapUpdate = offsetof(xl_heap_update, new_offnum) + sizeof(OffsetNumber).
/// We use the total struct size as a conservative approximation matching
/// the upstream formula.
#[inline(always)]
const fn size_of_heap_update() -> Size {
    core::mem::size_of::<xl_heap_update>()
}

/// SizeOfHeapDelete = offsetof(xl_heap_delete, flags) + sizeof(uint8).
#[inline(always)]
const fn size_of_heap_delete() -> Size {
    core::mem::size_of::<xl_heap_delete>()
}

/// SizeOfMultiInsertTuple = offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8).
#[inline(always)]
const fn size_of_multi_insert_tuple() -> Size {
    core::mem::size_of::<xl_multi_insert_tuple>()
}

// SnapBuildState integer constants (from replication/snapbuild.h).
pub const SNAPBUILD_START: SnapBuildState = -1;
pub const SNAPBUILD_BUILDING_SNAPSHOT: SnapBuildState = 0;
pub const SNAPBUILD_FULL_SNAPSHOT: SnapBuildState = 1;
pub const SNAPBUILD_CONSISTENT: SnapBuildState = 2;

// ---------------------------------------------------------------------------
// Stub functions for SnapBuild and ReorderBuffer APIs
// (real homes: replication/logical/snapbuild.c + replication/reorderbuffer.c)
// ---------------------------------------------------------------------------

unsafe fn SnapBuildCurrentState(builder: *mut SnapBuild) -> SnapBuildState { crate::replication::logical::snapbuild::SnapBuildCurrentState(builder as _) }

unsafe fn SnapBuildProcessChange(builder: *mut SnapBuild,
                                  xid: TransactionId,
                                  lsn: XLogRecPtr) -> bool { crate::replication::logical::snapbuild::SnapBuildProcessChange(builder as _, xid as _, lsn as _) }

unsafe fn SnapBuildCommitTxn(builder: *mut SnapBuild,
                               lsn: XLogRecPtr,
                               xid: TransactionId,
                               nsubxacts: i32,
                               subxacts: *mut TransactionId,
                               xinfo: u32) { crate::replication::logical::snapbuild::SnapBuildCommitTxn(builder as _, lsn as _, xid as _, nsubxacts as _, subxacts as _, xinfo as _) }

unsafe fn SnapBuildProcessRunningXacts(builder: *mut SnapBuild,
                                        lsn: XLogRecPtr,
                                        running: *mut xl_running_xacts) { crate::replication::logical::snapbuild::SnapBuildProcessRunningXacts(builder as _, lsn as _, running as _) }

unsafe fn SnapBuildProcessNewCid(builder: *mut SnapBuild,
                                  xid: TransactionId,
                                  lsn: XLogRecPtr,
                                  xlrec: *mut xl_heap_new_cid) { crate::replication::logical::snapbuild::SnapBuildProcessNewCid(builder as _, xid as _, lsn as _, xlrec as _) }

unsafe fn SnapBuildSerializationPoint(builder: *mut SnapBuild, lsn: XLogRecPtr) { crate::replication::logical::snapbuild::SnapBuildSerializationPoint(builder as _, lsn as _) }

unsafe fn SnapBuildGetOrBuildSnapshot(builder: *mut SnapBuild) -> *mut c_void /* Snapshot */ { crate::replication::logical::snapbuild::SnapBuildGetOrBuildSnapshot(builder as _) as _ }

unsafe fn SnapBuildGetTwoPhaseAt(builder: *mut SnapBuild) -> XLogRecPtr { crate::replication::logical::snapbuild::SnapBuildGetTwoPhaseAt(builder as _) }

unsafe fn SnapBuildXactNeedsSkip(builder: *mut SnapBuild, lsn: XLogRecPtr) -> bool { crate::replication::logical::snapbuild::SnapBuildXactNeedsSkip(builder as _, lsn as _) }

unsafe fn ReorderBufferProcessXid(reorder: *mut ReorderBuffer,
                                    xid: TransactionId,
                                    lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferProcessXid(reorder as _, xid as _, lsn as _) }

unsafe fn ReorderBufferAssignChild(reorder: *mut ReorderBuffer,
                                    toplevel: TransactionId,
                                    xid: TransactionId,
                                    lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferAssignChild(reorder as _, toplevel as _, xid as _, lsn as _) }

unsafe fn ReorderBufferForget(reorder: *mut ReorderBuffer,
                               xid: TransactionId,
                               lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferForget(reorder as _, xid as _, lsn as _) }

unsafe fn ReorderBufferCommitChild(reorder: *mut ReorderBuffer,
                                    xid: TransactionId,
                                    subxid: TransactionId,
                                    origptr: XLogRecPtr,
                                    endptr: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferCommitChild(reorder as _, xid as _, subxid as _, origptr as _, endptr as _) }

unsafe fn ReorderBufferCommit(reorder: *mut ReorderBuffer,
                               xid: TransactionId,
                               origptr: XLogRecPtr,
                               endptr: XLogRecPtr,
                               commit_time: TimestampTz,
                               origin_id: RepOriginId,
                               origin_lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferCommit(reorder as _, xid as _, origptr as _, endptr as _, commit_time as _, origin_id as _, origin_lsn as _) }

unsafe fn ReorderBufferFinishPrepared(reorder: *mut ReorderBuffer,
                                       xid: TransactionId,
                                       origptr: XLogRecPtr,
                                       endptr: XLogRecPtr,
                                       two_phase_at: XLogRecPtr,
                                       prepare_time: TimestampTz,
                                       origin_id: RepOriginId,
                                       origin_lsn: XLogRecPtr,
                                       gid: *const c_char,
                                       is_commit: bool) { crate::replication::logical::reorderbuffer::ReorderBufferFinishPrepared(reorder as _, xid as _, origptr as _, endptr as _, two_phase_at as _, prepare_time as _, origin_id as _, origin_lsn as _, gid as _, is_commit) }

unsafe fn ReorderBufferPrepare(reorder: *mut ReorderBuffer,
                                xid: TransactionId,
                                gid: *const c_char) { crate::replication::logical::reorderbuffer::ReorderBufferPrepare(reorder as _, xid as _, gid as _) }

unsafe fn ReorderBufferAbort(reorder: *mut ReorderBuffer,
                              xid: TransactionId,
                              lsn: XLogRecPtr,
                              abort_time: TimestampTz) { crate::replication::logical::reorderbuffer::ReorderBufferAbort(reorder as _, xid as _, lsn as _, abort_time as _) }

unsafe fn ReorderBufferAbortOld(reorder: *mut ReorderBuffer,
                                 oldest_running_xid: TransactionId) { crate::replication::logical::reorderbuffer::ReorderBufferAbortOld(reorder as _, oldest_running_xid as _) }

unsafe fn ReorderBufferSkipPrepare(reorder: *mut ReorderBuffer, xid: TransactionId) { crate::replication::logical::reorderbuffer::ReorderBufferSkipPrepare(reorder as _, xid as _) }

unsafe fn ReorderBufferRememberPrepareInfo(reorder: *mut ReorderBuffer,
                                            xid: TransactionId,
                                            origptr: XLogRecPtr,
                                            endptr: XLogRecPtr,
                                            prepare_time: TimestampTz,
                                            origin_id: RepOriginId,
                                            origin_lsn: XLogRecPtr) -> bool { crate::replication::logical::reorderbuffer::ReorderBufferRememberPrepareInfo(reorder as _, xid as _, origptr as _, endptr as _, prepare_time as _, origin_id as _, origin_lsn as _) }

unsafe fn ReorderBufferInvalidate(reorder: *mut ReorderBuffer,
                                   xid: TransactionId,
                                   lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferInvalidate(reorder as _, xid as _, lsn as _) }

unsafe fn ReorderBufferAddInvalidations(reorder: *mut ReorderBuffer,
                                         xid: TransactionId,
                                         lsn: XLogRecPtr,
                                         nmsgs: i32,
                                         msgs: *mut c_void) { crate::replication::logical::reorderbuffer::ReorderBufferAddInvalidations(reorder as _, xid as _, lsn as _, nmsgs as _, msgs as _) }

unsafe fn ReorderBufferXidSetCatalogChanges(reorder: *mut ReorderBuffer,
                                             xid: TransactionId,
                                             lsn: XLogRecPtr) { crate::replication::logical::reorderbuffer::ReorderBufferXidSetCatalogChanges(reorder as _, xid as _, lsn as _) }

unsafe fn ReorderBufferImmediateInvalidation(reorder: *mut ReorderBuffer,
                                              nmsgs: i32,
                                              msgs: *mut c_void) { crate::replication::logical::reorderbuffer::ReorderBufferImmediateInvalidation(reorder as _, nmsgs as _, msgs as _) }

unsafe fn ReorderBufferAllocChange(reorder: *mut ReorderBuffer) -> *mut ReorderBufferChange { unimplemented!() }

unsafe fn ReorderBufferAllocTupleBuf(reorder: *mut ReorderBuffer,
                                      tuple_len: Size) -> *mut HeapTupleData { crate::replication::logical::reorderbuffer::ReorderBufferAllocTupleBuf(reorder as _, tuple_len) }

unsafe fn ReorderBufferAllocRelids(reorder: *mut ReorderBuffer,
                                    nrelids: u32) -> *mut Oid { crate::replication::logical::reorderbuffer::ReorderBufferAllocRelids(reorder as _, nrelids as _) }

unsafe fn ReorderBufferQueueChange(reorder: *mut ReorderBuffer,
                                    xid: TransactionId,
                                    lsn: XLogRecPtr,
                                    change: *mut ReorderBufferChange,
                                    toast_relation: bool) { crate::replication::logical::reorderbuffer::ReorderBufferQueueChange(reorder as _, xid as _, lsn as _, change as _, toast_relation) }

unsafe fn ReorderBufferQueueMessage(reorder: *mut ReorderBuffer,
                                     xid: TransactionId,
                                     snapshot: *mut c_void,
                                     end_lsn: XLogRecPtr,
                                     transactional: bool,
                                     message: *const c_char,
                                     message_size: Size,
                                     message_body: *const c_char) { crate::replication::logical::reorderbuffer::ReorderBufferQueueMessage(reorder as _, xid as _, snapshot as _, end_lsn as _, transactional, message as _, message_size, message_body as _) }

unsafe fn UpdateDecodingStats(ctx: *mut LogicalDecodingContext) { crate::replication::logical::logical::UpdateDecodingStats(ctx as _) }

unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }

// ReorderBufferChange field accessors -- since ReorderBufferChange is an
// opaque type we keep all field access in stubs below.  When reorderbuffer.c
// is ported these helpers will be replaced by direct struct field access.

/// REORDER_BUFFER_CHANGE_INSERT and friends (replication/reorderbuffer.h).
pub const REORDER_BUFFER_CHANGE_INSERT: c_int = 0;
pub const REORDER_BUFFER_CHANGE_UPDATE: c_int = 1;
pub const REORDER_BUFFER_CHANGE_DELETE: c_int = 2;
pub const REORDER_BUFFER_CHANGE_TRUNCATE: c_int = 5;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT: c_int = 3;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM: c_int = 4;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT: c_int = 6;

/// Set change->action (stub until ReorderBufferChange is a real struct).
unsafe fn rb_change_set_action(change: *mut ReorderBufferChange, action: c_int) {
    unimplemented!() // TODO(pg-port): set change->action
}

/// Set change->origin_id.
unsafe fn rb_change_set_origin(change: *mut ReorderBufferChange, origin_id: RepOriginId) {
    unimplemented!() // TODO(pg-port): set change->origin_id
}

/// Set change->data.tp.rlocator by copy.
unsafe fn rb_change_set_rlocator(change: *mut ReorderBufferChange,
                                  rlocator: *const RelFileLocator) {
    unimplemented!() // TODO(pg-port): memcpy into change->data.tp.rlocator
}

/// Set change->data.tp.newtuple.
unsafe fn rb_change_set_newtuple(change: *mut ReorderBufferChange,
                                  tuple: *mut HeapTupleData) {
    unimplemented!() // TODO(pg-port): change->data.tp.newtuple = tuple
}

/// Set change->data.tp.oldtuple.
unsafe fn rb_change_set_oldtuple(change: *mut ReorderBufferChange,
                                  tuple: *mut HeapTupleData) {
    unimplemented!() // TODO(pg-port): change->data.tp.oldtuple = tuple
}

/// Set change->data.tp.clear_toast_afterwards.
unsafe fn rb_change_set_clear_toast(change: *mut ReorderBufferChange, v: bool) {
    unimplemented!() // TODO(pg-port): change->data.tp.clear_toast_afterwards = v
}

/// Set change->data.truncate fields.
unsafe fn rb_change_set_truncate(change: *mut ReorderBufferChange,
                                  cascade: bool,
                                  restart_seqs: bool,
                                  nrelids: u32,
                                  relids: *mut Oid) {
    unimplemented!() // TODO(pg-port): set truncate sub-fields
}

/// ItemPointerSetInvalid for HeapTupleData.t_self.
unsafe fn ItemPointerSetInvalid(ptr: *mut crate::storage::itemptr::ItemPointerData) { crate::storage::itemptr::ItemPointerSetInvalid(ptr as _) }

// ---------------------------------------------------------------------------
// Public interface (from decode.h)
// ---------------------------------------------------------------------------

/*
 * Take every XLogReadRecord()ed record and perform the actions required to
 * decode it using the output plugin already setup in the logical decoding
 * context.
 *
 * NB: Note that every record's xid needs to be processed by reorderbuffer
 * (xids contained in the content of records are not relevant for this rule).
 * That means that for records which'd otherwise not go through the
 * reorderbuffer ReorderBufferProcessXid() has to be called. We don't want to
 * call ReorderBufferProcessXid for each record type by default, because
 * e.g. empty xacts can be handled more efficiently if there's no previous
 * state for them.
 *
 * We also support the ability to fast forward thru records, skipping some
 * record types completely - see individual record types for details.
 */
pub unsafe fn LogicalDecodingProcessRecord(ctx: *mut LogicalDecodingContext,
                                            record: *mut XLogReaderState) {
    let mut buf = XLogRecordBuffer {
        origptr: (*(*ctx).reader).ReadRecPtr,
        endptr: (*(*ctx).reader).EndRecPtr,
        record,
    };

    let txid = XLogRecGetTopXid(record);

    /*
     * If the top-level xid is valid, we need to assign the subxact to the
     * top-level xact. We need to do this for all records, hence we do it
     * before the switch.
     */
    if TransactionIdIsValid(txid) {
        ReorderBufferAssignChild((*ctx).reorder,
                                 txid,
                                 XLogRecGetXid(record),
                                 buf.origptr);
    }

    let rmgr: RmgrData = GetRmgr(XLogRecGetRmid(record));

    if let Some(rm_decode) = rmgr.rm_decode {
        // rm_decode expects (*mut LogicalDecodingContext, *mut XLogRecordBuffer).
        // The C header uses the real types; our RmgrData uses the void-aliased
        // XLogRecordBuffer from xlog_internal.rs.  Transmute via raw casts.
        rm_decode(ctx as *mut _,
                  &mut buf as *mut XLogRecordBuffer as *mut _XLogRecordBufferVoid);
    } else {
        /* just deal with xid, and done */
        ReorderBufferProcessXid((*ctx).reorder, XLogRecGetXid(record), buf.origptr);
    }
}

/*
 * Handle rmgr XLOG_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn xlog_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;
    let info: uint8 = XLogRecGetInfo((*buf).record) & !XLR_INFO_MASK;

    ReorderBufferProcessXid((*ctx).reorder, XLogRecGetXid((*buf).record),
                             (*buf).origptr);

    match info {
        /* this is also used in END_OF_RECOVERY checkpoints */
        XLOG_CHECKPOINT_SHUTDOWN | XLOG_END_OF_RECOVERY => {
            SnapBuildSerializationPoint(builder, (*buf).origptr);
        }
        XLOG_CHECKPOINT_ONLINE => {
            /*
             * a RUNNING_XACTS record will have been logged near to this, we
             * can restart from there.
             */
        }
        XLOG_PARAMETER_CHANGE => {
            let xlrec: *mut xl_parameter_change =
                XLogRecGetData((*buf).record) as *mut xl_parameter_change;

            /*
             * If wal_level on the primary is reduced to less than
             * logical, we want to prevent existing logical slots from
             * being used.  Existing logical slots on the standby get
             * invalidated when this WAL record is replayed; and further,
             * slot creation fails when wal_level is not sufficient; but
             * all these operations are not synchronized, so a logical
             * slot may creep in while the wal_level is being reduced.
             * Hence this extra check.
             */
            if (*xlrec).wal_level < WAL_LEVEL_LOGICAL {
                /*
                 * This can occur only on a standby, as a primary would
                 * not allow to restart after changing wal_level < logical
                 * if there is pre-existing logical slot.
                 */
                Assert!(RecoveryInProgress());
                ereport!(
                    ERROR,
                    errmsg!(
                        "logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary"
                    )
                );
            }
        }
        XLOG_NOOP
        | XLOG_NEXTOID
        | XLOG_SWITCH
        | XLOG_BACKUP_END
        | XLOG_RESTORE_POINT
        | XLOG_FPW_CHANGE
        | XLOG_FPI_FOR_HINT
        | XLOG_FPI
        | XLOG_OVERWRITE_CONTRECORD
        | XLOG_CHECKPOINT_REDO => {}
        _ => {
            elog!(ERROR, "unexpected RM_XLOG_ID record type: {}", info);
        }
    }
}

/*
 * Handle rmgr XACT_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn xact_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;
    let reorder: *mut ReorderBuffer = (*ctx).reorder;
    let r: *mut XLogReaderState = (*buf).record;
    let info: uint8 = XLogRecGetInfo(r) & XLOG_XACT_OPMASK;

    /*
     * If the snapshot isn't yet fully built, we cannot decode anything, so
     * bail out.
     */
    if SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return;
    }

    match info {
        XLOG_XACT_COMMIT | XLOG_XACT_COMMIT_PREPARED => {
            let xlrec: *mut xl_xact_commit = XLogRecGetData(r) as *mut xl_xact_commit;
            let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
            ParseCommitRecord(XLogRecGetInfo((*buf).record), xlrec, &mut parsed);

            let xid: TransactionId;
            if !TransactionIdIsValid(parsed.twophase_xid) {
                xid = XLogRecGetXid(r);
            } else {
                xid = parsed.twophase_xid;
            }

            /*
             * We would like to process the transaction in a two-phase
             * manner iff output plugin supports two-phase commits and
             * doesn't filter the transaction at prepare time.
             */
            let two_phase: bool = if info == XLOG_XACT_COMMIT_PREPARED {
                !(FilterPrepare(ctx, xid, parsed.twophase_gid.as_ptr()))
            } else {
                false
            };

            DecodeCommit(ctx, buf, &mut parsed, xid, two_phase);
        }
        XLOG_XACT_ABORT | XLOG_XACT_ABORT_PREPARED => {
            let xlrec: *mut xl_xact_abort = XLogRecGetData(r) as *mut xl_xact_abort;
            let mut parsed: xl_xact_parsed_abort = core::mem::zeroed();
            ParseAbortRecord(XLogRecGetInfo((*buf).record), xlrec, &mut parsed);

            let xid: TransactionId;
            if !TransactionIdIsValid(parsed.twophase_xid) {
                xid = XLogRecGetXid(r);
            } else {
                xid = parsed.twophase_xid;
            }

            /*
             * We would like to process the transaction in a two-phase
             * manner iff output plugin supports two-phase commits and
             * doesn't filter the transaction at prepare time.
             */
            let two_phase: bool = if info == XLOG_XACT_ABORT_PREPARED {
                !(FilterPrepare(ctx, xid, parsed.twophase_gid.as_ptr()))
            } else {
                false
            };

            DecodeAbort(ctx, buf, &mut parsed, xid, two_phase);
        }
        XLOG_XACT_ASSIGNMENT => {
            /*
             * We assign subxact to the toplevel xact while processing each
             * record if required.  So, we don't need to do anything here. See
             * LogicalDecodingProcessRecord.
             */
        }
        XLOG_XACT_INVALIDATIONS => {
            let xid: TransactionId = XLogRecGetXid(r);
            let invals: *mut xl_xact_invals = XLogRecGetData(r) as *mut xl_xact_invals;

            /*
             * Execute the invalidations for xid-less transactions,
             * otherwise, accumulate them so that they can be processed at
             * the commit time.
             */
            if TransactionIdIsValid(xid) {
                if !(*ctx).fast_forward {
                    // msgs is a flexible-array trailing the struct; pass as void*.
                    ReorderBufferAddInvalidations(reorder, xid,
                                                  (*buf).origptr,
                                                  (*invals).nmsgs,
                                                  (*invals).msgs.as_ptr() as *mut c_void);
                }
                ReorderBufferXidSetCatalogChanges((*ctx).reorder, xid,
                                                   (*buf).origptr);
            } else if !(*ctx).fast_forward {
                ReorderBufferImmediateInvalidation((*ctx).reorder,
                                                   (*invals).nmsgs,
                                                   (*invals).msgs.as_ptr() as *mut c_void);
            }
        }
        XLOG_XACT_PREPARE => {
            let mut parsed: xl_xact_parsed_prepare = core::mem::zeroed();
            let xlrec: *mut xl_xact_prepare = XLogRecGetData(r) as *mut xl_xact_prepare;

            /* ok, parse it */
            ParsePrepareRecord(XLogRecGetInfo((*buf).record), xlrec, &mut parsed);

            /*
             * We would like to process the transaction in a two-phase
             * manner iff output plugin supports two-phase commits and
             * doesn't filter the transaction at prepare time.
             */
            if FilterPrepare(ctx, parsed.twophase_xid, parsed.twophase_gid.as_ptr()) {
                ReorderBufferProcessXid(reorder, parsed.twophase_xid,
                                        (*buf).origptr);
                return; // break
            }

            /*
             * Note that if the prepared transaction has locked [user]
             * catalog tables exclusively then decoding prepare can block
             * till the main transaction is committed because it needs to
             * lock the catalog tables.
             *
             * XXX Now, this can even lead to a deadlock if the prepare
             * transaction is waiting to get it logically replicated for
             * distributed 2PC. This can be avoided by disallowing
             * preparing transactions that have locked [user] catalog
             * tables exclusively but as of now, we ask users not to do
             * such an operation.
             */
            DecodePrepare(ctx, buf, &mut parsed);
        }
        _ => {
            elog!(ERROR, "unexpected RM_XACT_ID record type: {}", info);
        }
    }
}

/*
 * Handle rmgr STANDBY_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn standby_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;
    let r: *mut XLogReaderState = (*buf).record;
    let info: uint8 = XLogRecGetInfo(r) & !XLR_INFO_MASK;

    ReorderBufferProcessXid((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr);

    match info {
        XLOG_RUNNING_XACTS => {
            let running: *mut xl_running_xacts = XLogRecGetData(r) as *mut xl_running_xacts;

            SnapBuildProcessRunningXacts(builder, (*buf).origptr, running);

            /*
             * Abort all transactions that we keep track of, that are
             * older than the record's oldestRunningXid. This is the most
             * convenient spot for doing so since, in contrast to shutdown
             * or end-of-recovery checkpoints, we have information about
             * all running transactions which includes prepared ones,
             * while shutdown checkpoints just know that no non-prepared
             * transactions are in progress.
             */
            ReorderBufferAbortOld((*ctx).reorder, (*running).oldestRunningXid);
        }
        XLOG_STANDBY_LOCK => {}
        XLOG_INVALIDATIONS => {
            /*
             * We are processing the invalidations at the command level via
             * XLOG_XACT_INVALIDATIONS.  So we don't need to do anything here.
             */
        }
        _ => {
            elog!(ERROR, "unexpected RM_STANDBY_ID record type: {}", info);
        }
    }
}

/*
 * Handle rmgr HEAP2_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn heap2_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let info: uint8 = XLogRecGetInfo((*buf).record) & XLOG_HEAP_OPMASK;
    let xid: TransactionId = XLogRecGetXid((*buf).record);
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;

    ReorderBufferProcessXid((*ctx).reorder, xid, (*buf).origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding data changes. However, it's crucial to build the base
     * snapshot during fast-forward mode (as is done in
     * SnapBuildProcessChange()) because we require the snapshot's xmin when
     * determining the candidate catalog_xmin for the replication slot. See
     * SnapBuildProcessRunningXacts().
     */
    if SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return;
    }

    match info {
        XLOG_HEAP2_MULTI_INSERT => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeMultiInsert(ctx, buf);
            }
        }
        XLOG_HEAP2_NEW_CID => {
            if !(*ctx).fast_forward {
                let xlrec: *mut xl_heap_new_cid =
                    XLogRecGetData((*buf).record) as *mut xl_heap_new_cid;
                SnapBuildProcessNewCid(builder, xid, (*buf).origptr, xlrec);
            }
            // fall through -- no break in C
        }
        XLOG_HEAP2_REWRITE => {
            /*
             * Although these records only exist to serve the needs of logical
             * decoding, all the work happens as part of crash or archive
             * recovery, so we don't need to do anything here.
             */
        }
        /*
         * Everything else here is just low level physical stuff we're not
         * interested in.
         */
        XLOG_HEAP2_PRUNE_ON_ACCESS
        | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
        | XLOG_HEAP2_VISIBLE
        | XLOG_HEAP2_LOCK_UPDATED => {}
        _ => {
            elog!(ERROR, "unexpected RM_HEAP2_ID record type: {}", info);
        }
    }
}

/*
 * Handle rmgr HEAP_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn heap_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let info: uint8 = XLogRecGetInfo((*buf).record) & XLOG_HEAP_OPMASK;
    let xid: TransactionId = XLogRecGetXid((*buf).record);
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;

    ReorderBufferProcessXid((*ctx).reorder, xid, (*buf).origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding data changes. However, it's crucial to build the base
     * snapshot during fast-forward mode (as is done in
     * SnapBuildProcessChange()) because we require the snapshot's xmin when
     * determining the candidate catalog_xmin for the replication slot. See
     * SnapBuildProcessRunningXacts().
     */
    if SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return;
    }

    match info {
        XLOG_HEAP_INSERT => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeInsert(ctx, buf);
            }
        }
        /*
         * Treat HOT update as normal updates. There is no useful
         * information in the fact that we could make it a HOT update
         * locally and the WAL layout is compatible.
         */
        XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_UPDATE => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeUpdate(ctx, buf);
            }
        }
        XLOG_HEAP_DELETE => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeDelete(ctx, buf);
            }
        }
        XLOG_HEAP_TRUNCATE => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeTruncate(ctx, buf);
            }
        }
        XLOG_HEAP_INPLACE => {
            /*
             * Inplace updates are only ever performed on catalog tuples and
             * can, per definition, not change tuple visibility.  Since we
             * also don't decode catalog tuples, we're not interested in the
             * record's contents.
             */
        }
        XLOG_HEAP_CONFIRM => {
            if SnapBuildProcessChange(builder, xid, (*buf).origptr)
                && !(*ctx).fast_forward
            {
                DecodeSpecConfirm(ctx, buf);
            }
        }
        XLOG_HEAP_LOCK => {
            /* we don't care about row level locks for now */
        }
        _ => {
            elog!(ERROR, "unexpected RM_HEAP_ID record type: {}", info);
        }
    }
}

/*
 * Ask output plugin whether we want to skip this PREPARE and send
 * this transaction as a regular commit later.
 */
#[inline]
unsafe fn FilterPrepare(ctx: *mut LogicalDecodingContext,
                         xid: TransactionId,
                         gid: *const c_char) -> bool {
    /*
     * Skip if decoding of two-phase transactions at PREPARE time is not
     * enabled. In that case, all two-phase transactions are considered
     * filtered out and will be applied as regular transactions at COMMIT
     * PREPARED.
     */
    if !(*ctx).twophase {
        return true;
    }

    /*
     * The filter_prepare callback is optional. When not supplied, all
     * prepared transactions should go through.
     */
    if (*ctx).callbacks.filter_prepare_cb.is_none() {
        return false;
    }

    filter_prepare_cb_wrapper(ctx, xid, gid)
}

#[inline]
unsafe fn FilterByOrigin(ctx: *mut LogicalDecodingContext, origin_id: RepOriginId) -> bool {
    if (*ctx).callbacks.filter_by_origin_cb.is_none() {
        return false;
    }

    filter_by_origin_cb_wrapper(ctx, origin_id)
}

/*
 * Handle rmgr LOGICALMSG_ID records for LogicalDecodingProcessRecord().
 */
pub unsafe fn logicalmsg_decode(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;
    let r: *mut XLogReaderState = (*buf).record;
    let xid: TransactionId = XLogRecGetXid(r);
    let info: uint8 = XLogRecGetInfo(r) & !XLR_INFO_MASK;
    let origin_id: RepOriginId = XLogRecGetOrigin(r);
    let mut snapshot: *mut c_void = core::ptr::null_mut(); // Snapshot
    let message: *mut xl_logical_message;

    if info != XLOG_LOGICAL_MESSAGE {
        elog!(ERROR, "unexpected RM_LOGICALMSG_ID record type: {}", info);
    }

    ReorderBufferProcessXid((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr);

    /* If we don't have snapshot, there is no point in decoding messages */
    if SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT {
        return;
    }

    message = XLogRecGetData(r) as *mut xl_logical_message;

    if (*message).dbId != (*(*ctx).slot).data.database
        || FilterByOrigin(ctx, origin_id)
    {
        return;
    }

    if (*message).transactional
        && !SnapBuildProcessChange(builder, xid, (*buf).origptr)
    {
        return;
    } else if !(*message).transactional
        && (SnapBuildCurrentState(builder) != SNAPBUILD_CONSISTENT
            || SnapBuildXactNeedsSkip(builder, (*buf).origptr))
    {
        return;
    }

    /*
     * We also skip decoding in fast_forward mode. This check must be last
     * because we don't want to set the processing_required flag unless we
     * have a decodable message.
     */
    if (*ctx).fast_forward {
        /*
         * We need to set processing_required flag to notify the message's
         * existence to the caller. Usually, the flag is set when either the
         * COMMIT or ABORT records are decoded, but this must be turned on
         * here because the non-transactional logical message is decoded
         * without waiting for these records.
         */
        if !(*message).transactional {
            (*ctx).processing_required = true;
        }
        return;
    }

    /*
     * If this is a non-transactional change, get the snapshot we're expected
     * to use. We only get here when the snapshot is consistent, and the
     * change is not meant to be skipped.
     *
     * For transactional changes we don't need a snapshot, we'll use the
     * regular snapshot maintained by ReorderBuffer. We just leave it NULL.
     */
    if !(*message).transactional {
        snapshot = SnapBuildGetOrBuildSnapshot(builder);
    }

    ReorderBufferQueueMessage((*ctx).reorder, xid, snapshot, (*buf).endptr,
                               (*message).transactional,
                               (*message).message.as_ptr(), /* first part of message is prefix */
                               (*message).message_size,
                               (*message).message.as_ptr().add((*message).prefix_size));
}

/*
 * Consolidated commit record handling between the different form of commit
 * records.
 *
 * 'two_phase' indicates that caller wants to process the transaction in two
 * phases, first process prepare if not already done and then process
 * commit_prepared.
 */
unsafe fn DecodeCommit(ctx: *mut LogicalDecodingContext,
                        buf: *mut XLogRecordBuffer,
                        parsed: *mut xl_xact_parsed_commit,
                        xid: TransactionId,
                        two_phase: bool) {
    let mut origin_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut commit_time: TimestampTz = (*parsed).xact_time;
    let origin_id: RepOriginId = XLogRecGetOrigin((*buf).record);
    let mut i: i32;

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = (*parsed).origin_lsn;
        commit_time = (*parsed).origin_timestamp;
    }

    SnapBuildCommitTxn((*ctx).snapshot_builder, (*buf).origptr, xid,
                       (*parsed).nsubxacts, (*parsed).subxacts,
                       (*parsed).xinfo);

    /* ----
     * Check whether we are interested in this specific transaction, and tell
     * the reorderbuffer to forget the content of the (sub-)transactions
     * if not.
     *
     * We can't just use ReorderBufferAbort() here, because we need to execute
     * the transaction's invalidations.  This currently won't be needed if
     * we're just skipping over the transaction because currently we only do
     * so during startup, to get to the first transaction the client needs. As
     * we have reset the catalog caches before starting to read WAL, and we
     * haven't yet touched any catalogs, there can't be anything to invalidate.
     * But if we're "forgetting" this commit because it happened in another
     * database, the invalidations might be important, because they could be
     * for shared catalogs and we might have loaded data into the relevant
     * syscaches.
     * ---
     */
    if DecodeTXNNeedSkip(ctx, buf, (*parsed).dbId, origin_id) {
        i = 0;
        while i < (*parsed).nsubxacts {
            ReorderBufferForget((*ctx).reorder, *(*parsed).subxacts.add(i as usize),
                                (*buf).origptr);
            i += 1;
        }
        ReorderBufferForget((*ctx).reorder, xid, (*buf).origptr);
        return;
    }

    /* tell the reorderbuffer about the surviving subtransactions */
    i = 0;
    while i < (*parsed).nsubxacts {
        ReorderBufferCommitChild((*ctx).reorder, xid,
                                  *(*parsed).subxacts.add(i as usize),
                                  (*buf).origptr, (*buf).endptr);
        i += 1;
    }

    /*
     * Send the final commit record if the transaction data is already
     * decoded, otherwise, process the entire transaction.
     */
    if two_phase {
        ReorderBufferFinishPrepared((*ctx).reorder, xid, (*buf).origptr, (*buf).endptr,
                                    SnapBuildGetTwoPhaseAt((*ctx).snapshot_builder),
                                    commit_time, origin_id, origin_lsn,
                                    (*parsed).twophase_gid.as_ptr(), true);
    } else {
        ReorderBufferCommit((*ctx).reorder, xid, (*buf).origptr, (*buf).endptr,
                             commit_time, origin_id, origin_lsn);
    }

    /*
     * Update the decoding stats at transaction prepare/commit/abort.
     * Additionally we send the stats when we spill or stream the changes to
     * avoid losing them in case the decoding is interrupted. It is not clear
     * that sending more or less frequently than this would be better.
     */
    UpdateDecodingStats(ctx);
}

/*
 * Decode PREPARE record. Similar logic as in DecodeCommit.
 *
 * Note that we don't skip prepare even if have detected concurrent abort
 * because it is quite possible that we had already sent some changes before we
 * detect abort in which case we need to abort those changes in the subscriber.
 * To abort such changes, we do send the prepare and then the rollback prepared
 * which is what happened on the publisher-side as well. Now, we can invent a
 * new abort API wherein in such cases we send abort and skip sending prepared
 * and rollback prepared but then it is not that straightforward because we
 * might have streamed this transaction by that time in which case it is
 * handled when the rollback is encountered. It is not impossible to optimize
 * the concurrent abort case but it can introduce design complexity w.r.t
 * handling different cases so leaving it for now as it doesn't seem worth it.
 */
unsafe fn DecodePrepare(ctx: *mut LogicalDecodingContext,
                         buf: *mut XLogRecordBuffer,
                         parsed: *mut xl_xact_parsed_prepare) {
    let builder: *mut SnapBuild = (*ctx).snapshot_builder;
    let origin_lsn: XLogRecPtr = (*parsed).origin_lsn;
    let mut prepare_time: TimestampTz = (*parsed).xact_time;
    let origin_id: RepOriginId = XLogRecGetOrigin((*buf).record);
    let mut i: i32;
    let xid: TransactionId = (*parsed).twophase_xid;

    if (*parsed).origin_timestamp != 0 {
        prepare_time = (*parsed).origin_timestamp;
    }

    /*
     * Remember the prepare info for a txn so that it can be used later in
     * commit prepared if required. See ReorderBufferFinishPrepared.
     */
    if !ReorderBufferRememberPrepareInfo((*ctx).reorder, xid, (*buf).origptr,
                                          (*buf).endptr, prepare_time, origin_id,
                                          origin_lsn)
    {
        return;
    }

    /* We can't start streaming unless a consistent state is reached. */
    if SnapBuildCurrentState(builder) < SNAPBUILD_CONSISTENT {
        ReorderBufferSkipPrepare((*ctx).reorder, xid);
        return;
    }

    /*
     * Check whether we need to process this transaction. See
     * DecodeTXNNeedSkip for the reasons why we sometimes want to skip the
     * transaction.
     *
     * We can't call ReorderBufferForget as we did in DecodeCommit as the txn
     * hasn't yet been committed, removing this txn before a commit might
     * result in the computation of an incorrect restart_lsn. See
     * SnapBuildProcessRunningXacts. But we need to process cache
     * invalidations if there are any for the reasons mentioned in
     * DecodeCommit.
     */
    if DecodeTXNNeedSkip(ctx, buf, (*parsed).dbId, origin_id) {
        ReorderBufferSkipPrepare((*ctx).reorder, xid);
        ReorderBufferInvalidate((*ctx).reorder, xid, (*buf).origptr);
        return;
    }

    /* Tell the reorderbuffer about the surviving subtransactions. */
    i = 0;
    while i < (*parsed).nsubxacts {
        ReorderBufferCommitChild((*ctx).reorder, xid,
                                  *(*parsed).subxacts.add(i as usize),
                                  (*buf).origptr, (*buf).endptr);
        i += 1;
    }

    /* replay actions of all transaction + subtransactions in order */
    ReorderBufferPrepare((*ctx).reorder, xid, (*parsed).twophase_gid.as_ptr());

    /*
     * Update the decoding stats at transaction prepare/commit/abort.
     * Additionally we send the stats when we spill or stream the changes to
     * avoid losing them in case the decoding is interrupted. It is not clear
     * that sending more or less frequently than this would be better.
     */
    UpdateDecodingStats(ctx);
}


/*
 * Get the data from the various forms of abort records and pass it on to
 * snapbuild.c and reorderbuffer.c.
 *
 * 'two_phase' indicates to finish prepared transaction.
 */
unsafe fn DecodeAbort(ctx: *mut LogicalDecodingContext,
                       buf: *mut XLogRecordBuffer,
                       parsed: *mut xl_xact_parsed_abort,
                       xid: TransactionId,
                       two_phase: bool) {
    let mut i: i32;
    let mut origin_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut abort_time: TimestampTz = (*parsed).xact_time;
    let origin_id: RepOriginId = XLogRecGetOrigin((*buf).record);
    let skip_xact: bool;

    if (*parsed).xinfo & XACT_XINFO_HAS_ORIGIN != 0 {
        origin_lsn = (*parsed).origin_lsn;
        abort_time = (*parsed).origin_timestamp;
    }

    /*
     * Check whether we need to process this transaction. See
     * DecodeTXNNeedSkip for the reasons why we sometimes want to skip the
     * transaction.
     */
    skip_xact = DecodeTXNNeedSkip(ctx, buf, (*parsed).dbId, origin_id);

    /*
     * Send the final rollback record for a prepared transaction unless we
     * need to skip it. For non-two-phase xacts, simply forget the xact.
     */
    if two_phase && !skip_xact {
        ReorderBufferFinishPrepared((*ctx).reorder, xid, (*buf).origptr, (*buf).endptr,
                                    InvalidXLogRecPtr,
                                    abort_time, origin_id, origin_lsn,
                                    (*parsed).twophase_gid.as_ptr(), false);
    } else {
        i = 0;
        while i < (*parsed).nsubxacts {
            ReorderBufferAbort((*ctx).reorder,
                               *(*parsed).subxacts.add(i as usize),
                               (*(*buf).record).EndRecPtr,
                               abort_time);
            i += 1;
        }

        ReorderBufferAbort((*ctx).reorder, xid, (*(*buf).record).EndRecPtr,
                           abort_time);
    }

    /* update the decoding stats */
    UpdateDecodingStats(ctx);
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into tuplebufs.
 *
 * Inserts can contain the new tuple.
 */
unsafe fn DecodeInsert(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let mut datalen: Size = 0;
    let tupledata: *mut c_char;
    let tuplelen: Size;
    let r: *mut XLogReaderState = (*buf).record;
    let xlrec: *mut xl_heap_insert;
    let change: *mut ReorderBufferChange;
    let mut target_locator: RelFileLocator = core::mem::zeroed();

    xlrec = XLogRecGetData(r) as *mut xl_heap_insert;

    /*
     * Ignore insert records without new tuples (this does happen when
     * raw_heap_insert marks the TOAST record as HEAP_INSERT_NO_LOGICAL).
     */
    if (*xlrec).flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return;
    }

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &raw mut target_locator as *mut crate::access::transam::xlogreader::RelFileLocator, core::ptr::null_mut(),
                       core::ptr::null_mut());
    if target_locator.dbOid != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    change = ReorderBufferAllocChange((*ctx).reorder);
    if (*xlrec).flags & XLH_INSERT_IS_SPECULATIVE == 0 {
        rb_change_set_action(change, REORDER_BUFFER_CHANGE_INSERT);
    } else {
        rb_change_set_action(change, REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT);
    }
    rb_change_set_origin(change, XLogRecGetOrigin(r));

    rb_change_set_rlocator(change, &target_locator);

    tupledata = XLogRecGetBlockData(r, 0, &mut datalen);
    tuplelen = datalen - size_of_heap_header();

    let newtuple = ReorderBufferAllocTupleBuf((*ctx).reorder, tuplelen);
    rb_change_set_newtuple(change, newtuple);

    DecodeXLogTuple(tupledata, datalen, newtuple);

    rb_change_set_clear_toast(change, true);

    ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr,
                              change,
                              (*xlrec).flags & XLH_INSERT_ON_TOAST_RELATION != 0);
}

/*
 * Parse XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE, which have the same layout
 * in the record, from wal into proper tuplebufs.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
unsafe fn DecodeUpdate(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let r: *mut XLogReaderState = (*buf).record;
    let xlrec: *mut xl_heap_update;
    let change: *mut ReorderBufferChange;
    let data: *mut c_char;
    let mut target_locator: RelFileLocator = core::mem::zeroed();

    xlrec = XLogRecGetData(r) as *mut xl_heap_update;

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &raw mut target_locator as *mut crate::access::transam::xlogreader::RelFileLocator, core::ptr::null_mut(),
                       core::ptr::null_mut());
    if target_locator.dbOid != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    change = ReorderBufferAllocChange((*ctx).reorder);
    rb_change_set_action(change, REORDER_BUFFER_CHANGE_UPDATE);
    rb_change_set_origin(change, XLogRecGetOrigin(r));
    rb_change_set_rlocator(change, &target_locator);

    if (*xlrec).flags & XLH_UPDATE_CONTAINS_NEW_TUPLE != 0 {
        let mut datalen: Size = 0;
        let tuplelen: Size;

        let data = XLogRecGetBlockData(r, 0, &mut datalen);
        tuplelen = datalen - size_of_heap_header();

        let newtuple = ReorderBufferAllocTupleBuf((*ctx).reorder, tuplelen);
        rb_change_set_newtuple(change, newtuple);

        DecodeXLogTuple(data, datalen, newtuple);
    }

    if (*xlrec).flags & XLH_UPDATE_CONTAINS_OLD != 0 {
        let datalen: Size;
        let tuplelen: Size;

        /* caution, remaining data in record is not aligned */
        let data = (XLogRecGetData(r) as *mut c_char).add(size_of_heap_update());
        datalen = XLogRecGetDataLen(r) as Size - size_of_heap_update();
        tuplelen = datalen - size_of_heap_header();

        let oldtuple = ReorderBufferAllocTupleBuf((*ctx).reorder, tuplelen);
        rb_change_set_oldtuple(change, oldtuple);

        DecodeXLogTuple(data, datalen, oldtuple);
    }

    rb_change_set_clear_toast(change, true);

    ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr,
                              change, false);
}

/*
 * Parse XLOG_HEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
unsafe fn DecodeDelete(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let r: *mut XLogReaderState = (*buf).record;
    let xlrec: *mut xl_heap_delete;
    let change: *mut ReorderBufferChange;
    let mut target_locator: RelFileLocator = core::mem::zeroed();

    xlrec = XLogRecGetData(r) as *mut xl_heap_delete;

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &raw mut target_locator as *mut crate::access::transam::xlogreader::RelFileLocator, core::ptr::null_mut(),
                       core::ptr::null_mut());
    if target_locator.dbOid != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    change = ReorderBufferAllocChange((*ctx).reorder);

    if (*xlrec).flags & XLH_DELETE_IS_SUPER != 0 {
        rb_change_set_action(change, REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT);
    } else {
        rb_change_set_action(change, REORDER_BUFFER_CHANGE_DELETE);
    }

    rb_change_set_origin(change, XLogRecGetOrigin(r));
    rb_change_set_rlocator(change, &target_locator);

    /* old primary key stored */
    if (*xlrec).flags & XLH_DELETE_CONTAINS_OLD != 0 {
        let datalen: Size = XLogRecGetDataLen(r) as Size - size_of_heap_delete();
        let tuplelen: Size = datalen - size_of_heap_header();

        Assert!(XLogRecGetDataLen(r) as Size >
                size_of_heap_delete() + size_of_heap_header());

        let oldtuple = ReorderBufferAllocTupleBuf((*ctx).reorder, tuplelen);
        rb_change_set_oldtuple(change, oldtuple);

        DecodeXLogTuple((xlrec as *mut c_char).add(size_of_heap_delete()),
                        datalen, oldtuple);
    }

    rb_change_set_clear_toast(change, true);

    ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr,
                              change, false);
}

/*
 * Parse XLOG_HEAP_TRUNCATE from wal
 */
unsafe fn DecodeTruncate(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let r: *mut XLogReaderState = (*buf).record;
    let xlrec: *mut xl_heap_truncate;
    let change: *mut ReorderBufferChange;

    xlrec = XLogRecGetData(r) as *mut xl_heap_truncate;

    /* only interested in our database */
    if (*xlrec).dbId != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    change = ReorderBufferAllocChange((*ctx).reorder);
    rb_change_set_action(change, REORDER_BUFFER_CHANGE_TRUNCATE);
    rb_change_set_origin(change, XLogRecGetOrigin(r));

    let relids = ReorderBufferAllocRelids((*ctx).reorder, (*xlrec).nrelids);
    let cascade = (*xlrec).flags & XLH_TRUNCATE_CASCADE != 0;
    let restart_seqs = (*xlrec).flags & XLH_TRUNCATE_RESTART_SEQS != 0;
    rb_change_set_truncate(change, cascade, restart_seqs, (*xlrec).nrelids, relids);

    core::ptr::copy_nonoverlapping(
        (*xlrec).relids.as_ptr(),
        relids,
        (*xlrec).nrelids as usize,
    );

    ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r),
                              (*buf).origptr, change, false);
}

/*
 * Decode XLOG_HEAP2_MULTI_INSERT record into multiple tuplebufs.
 *
 * Currently MULTI_INSERT will always contain the full tuples.
 */
unsafe fn DecodeMultiInsert(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let r: *mut XLogReaderState = (*buf).record;
    let xlrec: *mut xl_heap_multi_insert;
    let mut i: i32;
    let data: *mut c_char;
    let tupledata: *mut c_char;
    let mut tuplelen: Size = 0;
    let mut rlocator: RelFileLocator = core::mem::zeroed();

    xlrec = XLogRecGetData(r) as *mut xl_heap_multi_insert;

    /*
     * Ignore insert records without new tuples.  This happens when a
     * multi_insert is done on a catalog or on a non-persistent relation.
     */
    if (*xlrec).flags & XLH_INSERT_CONTAINS_NEW_TUPLE == 0 {
        return;
    }

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &raw mut rlocator as *mut crate::access::transam::xlogreader::RelFileLocator, core::ptr::null_mut(),
                       core::ptr::null_mut());
    if rlocator.dbOid != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    /*
     * We know that this multi_insert isn't for a catalog, so the block should
     * always have data even if a full-page write of it is taken.
     */
    tupledata = XLogRecGetBlockData(r, 0, &mut tuplelen);
    Assert!(!tupledata.is_null());

    let mut data_ptr = tupledata;
    i = 0;
    while i < (*xlrec).ntuples as i32 {
        let change: *mut ReorderBufferChange;
        let xlhdr: *mut xl_multi_insert_tuple;
        let datalen: i32;
        let tuple: *mut HeapTupleData;
        let header: *mut HeapTupleHeaderData;

        change = ReorderBufferAllocChange((*ctx).reorder);
        rb_change_set_action(change, REORDER_BUFFER_CHANGE_INSERT);
        rb_change_set_origin(change, XLogRecGetOrigin(r));
        rb_change_set_rlocator(change, &rlocator);

        // SHORTALIGN aligns to 2-byte boundary (mirrors C SHORTALIGN macro).
        xlhdr = crate::c::SHORTALIGN(data_ptr as usize) as *mut xl_multi_insert_tuple;
        data_ptr = (xlhdr as *mut c_char).add(size_of_multi_insert_tuple());
        datalen = (*xlhdr).datalen as i32;

        let tuple_buf = ReorderBufferAllocTupleBuf((*ctx).reorder, datalen as Size);
        rb_change_set_newtuple(change, tuple_buf);

        tuple = tuple_buf;
        header = (*tuple).t_data as *mut HeapTupleHeaderData;

        /* not a disk based tuple */
        ItemPointerSetInvalid(&mut (*tuple).t_self);

        /*
         * We can only figure this out after reassembling the transactions.
         */
        (*tuple).t_tableOid = InvalidOid;

        (*tuple).t_len = datalen as u32 + SizeofHeapTupleHeader as u32;

        core::ptr::write_bytes(header as *mut u8, 0, SizeofHeapTupleHeader);

        core::ptr::copy_nonoverlapping(
            data_ptr as *const u8,
            ((*tuple).t_data as *mut u8).add(SizeofHeapTupleHeader),
            datalen as usize,
        );
        (*header).t_infomask = (*xlhdr).t_infomask;
        (*header).t_infomask2 = (*xlhdr).t_infomask2;
        (*header).t_hoff = (*xlhdr).t_hoff;

        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert()
         * call.
         */
        let clear_toast = (*xlrec).flags & XLH_INSERT_LAST_IN_MULTI != 0
            && (i + 1) == (*xlrec).ntuples as i32;
        rb_change_set_clear_toast(change, clear_toast);

        ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r),
                                  (*buf).origptr, change, false);

        /* move to the next xl_multi_insert_tuple entry */
        data_ptr = data_ptr.add(datalen as usize);

        i += 1;
    }
    Assert!(data_ptr == tupledata.add(tuplelen));
}

/*
 * Parse XLOG_HEAP_CONFIRM from wal into a confirmation change.
 *
 * This is pretty trivial, all the state essentially already setup by the
 * speculative insertion.
 */
unsafe fn DecodeSpecConfirm(ctx: *mut LogicalDecodingContext, buf: *mut XLogRecordBuffer) {
    let r: *mut XLogReaderState = (*buf).record;
    let change: *mut ReorderBufferChange;
    let mut target_locator: RelFileLocator = core::mem::zeroed();

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &raw mut target_locator as *mut crate::access::transam::xlogreader::RelFileLocator, core::ptr::null_mut(),
                       core::ptr::null_mut());
    if target_locator.dbOid != (*(*ctx).slot).data.database {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if FilterByOrigin(ctx, XLogRecGetOrigin(r)) {
        return;
    }

    change = ReorderBufferAllocChange((*ctx).reorder);
    rb_change_set_action(change, REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM);
    rb_change_set_origin(change, XLogRecGetOrigin(r));

    rb_change_set_rlocator(change, &target_locator);

    rb_change_set_clear_toast(change, true);

    ReorderBufferQueueChange((*ctx).reorder, XLogRecGetXid(r), (*buf).origptr,
                              change, false);
}


/*
 * Read a HeapTuple as WAL logged by heap_insert, heap_update and heap_delete
 * (but not by heap_multi_insert) into a tuplebuf.
 *
 * The size 'len' and the pointer 'data' in the record need to be
 * computed outside as they are record specific.
 */
unsafe fn DecodeXLogTuple(data: *mut c_char, len: Size, tuple: *mut HeapTupleData) {
    let mut xlhdr: xl_heap_header = core::mem::zeroed();
    let datalen: i32 = len as i32 - size_of_heap_header() as i32;
    let header: *mut HeapTupleHeaderData;

    Assert!(datalen >= 0);

    (*tuple).t_len = datalen as u32 + SizeofHeapTupleHeader as u32;
    header = (*tuple).t_data as *mut HeapTupleHeaderData;

    /* not a disk based tuple */
    ItemPointerSetInvalid(&mut (*tuple).t_self);

    /* we can only figure this out after reassembling the transactions */
    (*tuple).t_tableOid = InvalidOid;

    /* data is not stored aligned, copy to aligned storage */
    core::ptr::copy_nonoverlapping(
        data as *const u8,
        &mut xlhdr as *mut xl_heap_header as *mut u8,
        size_of_heap_header(),
    );

    core::ptr::write_bytes(header as *mut u8, 0, SizeofHeapTupleHeader);

    core::ptr::copy_nonoverlapping(
        data.add(size_of_heap_header()) as *const u8,
        ((*tuple).t_data as *mut u8).add(SizeofHeapTupleHeader),
        datalen as usize,
    );

    (*header).t_infomask = xlhdr.t_infomask;
    (*header).t_infomask2 = xlhdr.t_infomask2;
    (*header).t_hoff = xlhdr.t_hoff;
}

/*
 * Check whether we are interested in this specific transaction.
 *
 * There can be several reasons we might not be interested in this
 * transaction:
 * 1) We might not be interested in decoding transactions up to this
 *    LSN. This can happen because we previously decoded it and now just
 *    are restarting or if we haven't assembled a consistent snapshot yet.
 * 2) The transaction happened in another database.
 * 3) The output plugin is not interested in the origin.
 * 4) We are doing fast-forwarding
 */
unsafe fn DecodeTXNNeedSkip(ctx: *mut LogicalDecodingContext,
                              buf: *mut XLogRecordBuffer,
                              txn_dbid: Oid,
                              origin_id: RepOriginId) -> bool {
    if SnapBuildXactNeedsSkip((*ctx).snapshot_builder, (*buf).origptr)
        || (txn_dbid != InvalidOid && txn_dbid != (*(*ctx).slot).data.database)
        || FilterByOrigin(ctx, origin_id)
    {
        return true;
    }

    /*
     * We also skip decoding in fast_forward mode. In passing set the
     * processing_required flag to indicate that if it were not for
     * fast_forward mode, processing would have been required.
     */
    if (*ctx).fast_forward {
        (*ctx).processing_required = true;
        return true;
    }

    false
}

// ---------------------------------------------------------------------------
// Helper: TransactionIdIsValid (mirrors the C macro).
// The canonical definition is in crate::c / crate::access::transam.
// ---------------------------------------------------------------------------
#[inline(always)]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != crate::access::transam::InvalidTransactionId
}

/*-------------------------------------------------------------------------
 *
 * pgoutput.rs
 *     Logical Replication output plugin
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *       src/backend/replication/pgoutput/pgoutput.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

// ---------------------------------------------------------------------------
// Imports from real homes
// ---------------------------------------------------------------------------

use crate::replication::logical::logical::{
    LogicalDecodingContext,
    OutputPluginPrepareWrite,
    OutputPluginWrite,
    OutputPluginUpdateProgress,
};
use crate::replication::output_plugin::{
    OutputPluginOptions,
    OutputPluginCallbacks,
    OUTPUT_PLUGIN_BINARY_OUTPUT,
};
use crate::replication::logical::reorderbuffer::{
    ReorderBufferTXN,
    ReorderBufferChange,
    ReorderBufferChangeType,
    REORDER_BUFFER_CHANGE_INSERT,
    REORDER_BUFFER_CHANGE_UPDATE,
    REORDER_BUFFER_CHANGE_DELETE,
    rbtxn_is_subtxn,
    rbtxn_get_toptxn,
    rbtxn_is_streamed,
    TimestampTz,
};
use crate::replication::logicalproto::{
    logicalrep_write_begin,
    logicalrep_write_commit,
    logicalrep_write_begin_prepare,
    logicalrep_write_prepare,
    logicalrep_write_commit_prepared,
    logicalrep_write_rollback_prepared,
    logicalrep_write_stream_prepare,
    logicalrep_write_origin,
    logicalrep_write_insert,
    logicalrep_write_update,
    logicalrep_write_delete,
    logicalrep_write_truncate,
    logicalrep_write_message,
    logicalrep_write_rel,
    logicalrep_write_typ,
    logicalrep_write_stream_start,
    logicalrep_write_stream_stop,
    logicalrep_write_stream_commit,
    logicalrep_write_stream_abort,
    logicalrep_should_publish_column,
    LOGICALREP_PROTO_MAX_VERSION_NUM,
    LOGICALREP_PROTO_MIN_VERSION_NUM,
    LOGICALREP_PROTO_STREAM_VERSION_NUM,
    LOGICALREP_PROTO_TWOPHASE_VERSION_NUM,
    LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM,
};
use crate::replication::logical::origin::replorigin_by_oid;
use crate::catalog::pg_subscription::{
    LOGICALREP_ORIGIN_NONE,
    LOGICALREP_ORIGIN_ANY,
    LOGICALREP_STREAM_OFF,
    LOGICALREP_STREAM_ON,
    LOGICALREP_STREAM_PARALLEL,
};
use crate::catalog::catalog::FirstGenbkiObjectId;
use crate::executor::execReplication::PublicationActions;
use crate::nodes::bitmapset::{Bitmapset, bms_equal, bms_free, bms_make_singleton};
use crate::nodes::pg_list::{
    List,
    lappend, lappend_xid, list_member_oid, list_member_xid, list_free, list_free_deep,
};
use crate::nodes::makefuncs::make_orclause;
use crate::nodes::read::stringToNode;
use crate::access::common::attmap::{AttrMap, free_attrmap, build_attrmap_by_name_if_req};
use crate::access::common::tupconvert::execute_attr_map_slot;
use crate::access::common::tupdesc::{
    TupleDesc,
    TupleDescAttr,
    TupleDescCompactAttr,
    CreateTupleDescCopyConstr,
    FreeTupleDesc,
};
use crate::executor::tuptable::{
    TupleTableSlot,
    TupleTableSlotOps,
    ExecClearTuple,
    slot_getallattrs,
};
use crate::executor::execTuples::{TTSOpsHeapTuple, TTSOpsVirtual};
use crate::executor::executor::{
    CreateExecutorState,
    ExecPrepareExpr,
    ExecEvalExprSwitchContext,
    GetPerTupleExprContext,
    ResetPerTupleExprContext,
    ExecInitRangeTable,
};
use crate::nodes::execnodes::{EState, ExprState, ExprContext};
use crate::nodes::parsenodes::{RangeTblEntry, RTEKind};
use crate::utils::mmgr::mcxt::{
    CacheMemoryContext,
    MemoryContextRegisterResetCallback,
};
use crate::utils::palloc::{
    MemoryContext,
    MemoryContextCallback,
    MemoryContextCallbackFunction,
    palloc0,
    pfree,
    MemoryContextAllocZero,
};
use crate::utils::rel::Relation;
use crate::utils::cache::syscache::{
    SearchSysCache2,
    SysCacheGetAttr,
    ReleaseSysCache,
};
use crate::catalog::partition::get_partition_ancestors;
use crate::commands::defrem::{defGetBoolean, defGetString};
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::utils::palloc::pstrdup;
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::{Oid, InvalidOid};
use crate::access::transam::InvalidTransactionId;
use crate::access::transam::xlogreader::InvalidRepOriginId;
use crate::access::transam::xlogdefs::InvalidXLogRecPtr;

// ---------------------------------------------------------------------------
// Stubs -- TODO(pg-port): wire to real modules once ported
// ---------------------------------------------------------------------------

// pub/catalog stubs

/// Publication as returned by GetPublicationByName / GetRelationPublications.
/// TODO(pg-port): real type lives in catalog/pg_publication.h -> commands/publicationcmds.c
#[repr(C)]
pub struct Publication {
    pub oid: Oid,
    pub name: *mut c_char,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubactions: PublicationActions,
    /// TODO(pg-port): pubgencols_type -- catalog/pg_publication.h PublishGencolsType
    pub pubgencols_type: PublishGencolsType,
}

/// TODO(pg-port): catalog/pg_publication.h
pub type PublishGencolsType = c_char;
/// TODO(pg-port): catalog/pg_publication.h
pub const PUBLISH_GENCOLS_NONE: PublishGencolsType = b'n' as c_char;
/// TODO(pg-port): catalog/pg_publication.h
pub const PUBLISH_GENCOLS_STORED: PublishGencolsType = b's' as c_char;

// Syscache IDs -- TODO(pg-port): real values in utils/syscache.h
const PUBLICATIONOID: c_int = 0;
const PUBLICATIONNAMESPACEMAP: c_int = 0;
const PUBLICATIONRELMAP: c_int = 0;
const NAMESPACEOID: c_int = 0;

// Attribute number for pg_publication_rel.prqual
// TODO(pg-port): catalog/pg_publication_rel.h
const Anum_pg_publication_rel_prqual: c_int = 1;

// AccessShareLock -- TODO(pg-port): storage/lockdefs.h
const AccessShareLock: c_int = 1;

/// A heap tuple pointer -- TODO(pg-port): access/htup.h
pub type HeapTuple = *mut c_void;

/// TODO(pg-port): utils/rel.h macro -- RelationIsValid
#[inline]
unsafe fn RelationIsValid(rel: Relation) -> bool {
    !rel.is_null()
}
/// TODO(pg-port): utils/rel.h -- RelationGetRelid
#[inline]
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    (*rel).rd_id
}
/// TODO(pg-port): utils/rel.h -- RelationGetDescr
#[inline]
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    (*rel).rd_att
}
/// TODO(pg-port): utils/rel.h -- RelationGetRelationName
#[inline]
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    (*(*rel).rd_rel).relname.data.as_ptr()
}
/// TODO(pg-port): utils/rel.h -- RelationGetNamespace
#[inline]
unsafe fn RelationGetNamespace(rel: Relation) -> Oid {
    (*(*rel).rd_rel).relnamespace
}
/// TODO(pg-port): catalog/partition.h -- get_partition_ancestors (already imported above via catalog::partition)
/// TODO(pg-port): utils/rel.h -- rel flags
#[inline]
unsafe fn RelationIdGetRelation(_reloid: Oid) -> Relation {
    core::ptr::null_mut() // TODO(pg-port): real impl in access/common/relation.c
}
#[inline]
unsafe fn RelationClose(_rel: Relation) {
    // TODO(pg-port): real impl in access/common/relation.c
}

// Executor state helpers
#[inline]
unsafe fn MakeSingleTupleTableSlot(tupdesc: TupleDesc, ops: *const TupleTableSlotOps) -> *mut TupleTableSlot {
    core::ptr::null_mut() // TODO(pg-port): real impl in executor/execTuples.c
}
#[inline]
unsafe fn MakeTupleTableSlot(tupdesc: TupleDesc, ops: *const TupleTableSlotOps) -> *mut TupleTableSlot {
    core::ptr::null_mut() // TODO(pg-port): real impl in executor/execTuples.c
}
#[inline]
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    // TODO(pg-port): real impl in executor/execTuples.c
}
#[inline]
unsafe fn ExecStoreHeapTuple(_tuple: HeapTuple, _slot: *mut TupleTableSlot, _should_free: bool) -> *mut TupleTableSlot {
    core::ptr::null_mut() // TODO(pg-port): real impl in executor/execTuples.c
}
#[inline]
unsafe fn ExecStoreVirtualTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    core::ptr::null_mut() // TODO(pg-port): real impl in executor/execTuples.c
}

// catalog / publication helpers -- TODO(pg-port)
#[inline]
unsafe fn GetPublicationByName(_pubname: *const c_char, _missing_ok: bool) -> *mut Publication {
    core::ptr::null_mut() // TODO(pg-port): commands/publicationcmds.c
}
#[inline]
unsafe fn GetRelationPublications(_relid: Oid) -> *mut List {
    core::ptr::null_mut() // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn GetSchemaPublications(_schemaid: Oid) -> *mut List {
    core::ptr::null_mut() // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn GetTopMostAncestorInPublication(
    _puboid: Oid,
    _ancestors: *mut List,
    _level: *mut c_int,
) -> Oid {
    InvalidOid // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn is_publishable_relation(_rel: Relation) -> bool {
    false // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn check_and_fetch_column_list(
    _pub: *mut Publication,
    _relid: Oid,
    _mcxt: MemoryContext,
    _cols: *mut *mut Bitmapset,
) -> bool {
    false // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn pub_form_cols_map(_rel: Relation, _gencols_type: PublishGencolsType) -> *mut Bitmapset {
    core::ptr::null_mut() // TODO(pg-port): catalog/pg_publication.c
}
#[inline]
unsafe fn expand_generated_columns_in_expr(
    node: *mut c_void,
    _rel: Relation,
    _rt_index: c_int,
) -> *mut c_void {
    node // TODO(pg-port): rewrite/rewriteHandler.c
}

// syscache helpers -- TODO(pg-port)
#[inline]
unsafe fn SearchSysCacheExists2(_cacheid: c_int, _key1: Datum, _key2: Datum) -> bool {
    false // TODO(pg-port): utils/cache/syscache.c
}
#[inline]
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}

// defrem helpers
#[inline]
unsafe fn defGetStreamingMode(_def: *mut DefElem) -> c_char {
    LOGICALREP_STREAM_OFF // TODO(pg-port): commands/defrem.c
}

// SplitIdentifierString -- TODO(pg-port): utils/adt/varlena.c
#[inline]
unsafe fn SplitIdentifierString(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    true // TODO(pg-port)
}

// parser helpers
#[inline]
unsafe fn addRTEPermissionInfo(_perminfos: *mut *mut List, _rte: *mut RangeTblEntry) {
    // TODO(pg-port): parser/parse_relation.c
}
#[inline]
unsafe fn makeNode_RangeTblEntry() -> *mut RangeTblEntry {
    palloc0(core::mem::size_of::<RangeTblEntry>()) as *mut RangeTblEntry
}

// inval / cache callbacks -- TODO(pg-port)
#[inline]
unsafe fn CacheRegisterSyscacheCallback(_cacheid: c_int, _cb: unsafe fn(Datum, c_int, u32), _arg: Datum) {
    // TODO(pg-port): utils/cache/inval.c
}
#[inline]
unsafe fn CacheRegisterRelcacheCallback(_cb: unsafe fn(Datum, Oid), _arg: Datum) {
    // TODO(pg-port): utils/cache/inval.c
}
#[inline]
unsafe fn CacheRegisterRelSyncCallback(_cb: unsafe fn(Datum, Oid), _arg: Datum) {
    // TODO(pg-port): replication/logical/logical.c
}

// MemoryContext helpers -- TODO(pg-port)
#[inline]
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _min_context_size: usize,
    _init_block_size: usize,
    _max_block_size: usize,
) -> MemoryContext {
    core::ptr::null_mut() // TODO(pg-port): utils/mmgr/aset.c
}
#[inline]
unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    context // TODO(pg-port): utils/mmgr/mcxt.c
}
#[inline]
unsafe fn MemoryContextReset(_context: MemoryContext) {
    // TODO(pg-port): utils/mmgr/mcxt.c
}
#[inline]
unsafe fn MemoryContextDelete(_context: MemoryContext) {
    // TODO(pg-port): utils/mmgr/mcxt.c
}
#[inline]
unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    // TODO(pg-port): utils/mmgr/mcxt.c
}

// lsyscache helpers
#[inline]
unsafe fn get_rel_namespace(_relid: Oid) -> Oid {
    InvalidOid // TODO(pg-port): utils/cache/lsyscache.c
}
#[inline]
unsafe fn get_rel_relispartition(_relid: Oid) -> bool {
    false // TODO(pg-port): utils/cache/lsyscache.c
}
#[inline]
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    0 // TODO(pg-port): utils/cache/lsyscache.c
}
#[inline]
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    core::ptr::null_mut() // TODO(pg-port): utils/cache/lsyscache.c
}

// list helpers
#[inline]
unsafe fn llast_oid(list: *mut List) -> Oid {
    InvalidOid // TODO(pg-port): nodes/pg_list.h
}
#[inline]
unsafe fn list_length(list: *const List) -> c_int {
    0 // TODO(pg-port): nodes/pg_list.h
}
#[inline]
unsafe fn GetCurrentCommandId(_increment: bool) -> u32 {
    0 // TODO(pg-port): access/xact.h
}

// RELKIND_PARTITIONED_TABLE -- TODO(pg-port): access/relscan.h
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// ALLOCSET size constants -- values from utils/memutils.h
const ALLOCSET_DEFAULT_MIN: usize = 0;
const ALLOCSET_DEFAULT_INIT: usize = 8 * 1024;
const ALLOCSET_DEFAULT_MAX: usize = 8 * 1024 * 1024;
const ALLOCSET_SMALL_MIN: usize = 0;
const ALLOCSET_SMALL_INIT: usize = 1024;
const ALLOCSET_SMALL_MAX: usize = 8 * 1024 * 1024;

// PG_UINT32_MAX
const PG_UINT32_MAX: u64 = u32::MAX as u64;

// elog levels (prelude usually provides these, fall back to local consts)
use crate::elog;

// Datum
use crate::postgres::Datum;
// StringInfo
use crate::lib::stringinfo::StringInfo;
// Size
use crate::c::Size;
// DefElem
use crate::nodes::parsenodes::DefElem;
// XLogRecPtr, RepOriginId, TransactionId
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xlogreader::RepOriginId;
use crate::c::TransactionId;
// uint32
use crate::c::uint32;

// ---------------------------------------------------------------------------
// HTAB / hash helpers -- reuse stub from reorderbuffer
// ---------------------------------------------------------------------------
use crate::replication::logical::reorderbuffer::{HTAB, HASHCTL, HASH_SEQ_STATUS};

extern "C" {
    fn hash_create(
        tabname: *const c_char,
        nelem: c_long,
        info: *mut HASHCTL,
        flags: c_int,
    ) -> *mut HTAB;
    fn hash_search(
        hashp: *mut HTAB,
        keyPtr: *const c_void,
        action: c_int,
        foundPtr: *mut bool,
    ) -> *mut c_void;
    fn hash_destroy(hashp: *mut HTAB);
    fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB);
    fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void;
}

// hash flags (from utils/hsearch.h)
const HASH_ELEM: c_int = 0x0010;
const HASH_BLOBS: c_int = 0x0400;
const HASH_CONTEXT: c_int = 0x4000;
const HASH_ENTER: c_int = 1;
const HASH_FIND: c_int = 3;

// c_long
use core::ffi::c_long;

// ---------------------------------------------------------------------------
// strVal helper
// ---------------------------------------------------------------------------
#[inline]
unsafe fn strVal(val: *mut c_void) -> *mut c_char {
    // DefElem->arg is a Node; for String nodes the val is at offset 8
    // TODO(pg-port): use crate::nodes::value::strVal once ported
    (val as *mut c_char).add(8)
}

// ---------------------------------------------------------------------------
// NUM_ROWFILTER_PUBACTIONS
// ---------------------------------------------------------------------------
const NUM_ROWFILTER_PUBACTIONS: usize = 3;

// ---------------------------------------------------------------------------
// RowFilterPubAction enum
// ---------------------------------------------------------------------------
/*
 * Only 3 publication actions are used for row filtering ("insert", "update",
 * "delete"). See RelationSyncEntry.exprstate[].
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RowFilterPubAction {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

use RowFilterPubAction as PUBACTION;
const PUBACTION_INSERT: usize = PUBACTION::Insert as usize;
const PUBACTION_UPDATE: usize = PUBACTION::Update as usize;
const PUBACTION_DELETE: usize = PUBACTION::Delete as usize;

// ---------------------------------------------------------------------------
// RelationSyncEntry
// ---------------------------------------------------------------------------
/*
 * Entry in the map used to remember which relation schemas we sent.
 *
 * The schema_sent flag determines if the current schema record for the
 * relation (and for its ancestor if publish_as_relid is set) was already
 * sent to the subscriber (in which case we don't need to send it again).
 *
 * The schema cache on downstream is however updated only at commit time,
 * and with streamed transactions the commit order may be different from
 * the order the transactions are sent in. Also, the (sub) transactions
 * might get aborted so we need to send the schema for each (sub) transaction
 * so that we don't lose the schema information on abort. For handling this,
 * we maintain the list of xids (streamed_txns) for those we have already sent
 * the schema.
 *
 * For partitions, 'pubactions' considers not only the table's own
 * publications, but also those of all of its ancestors.
 */
#[repr(C)]
pub struct RelationSyncEntry {
    pub relid: Oid,             /* relation oid */

    pub replicate_valid: bool,  /* overall validity flag for entry */

    pub schema_sent: bool,

    /*
     * This will be PUBLISH_GENCOLS_STORED if the relation contains generated
     * columns and the 'publish_generated_columns' parameter is set to
     * PUBLISH_GENCOLS_STORED. Otherwise, it will be PUBLISH_GENCOLS_NONE,
     * indicating that no generated columns should be published, unless
     * explicitly specified in the column list.
     */
    pub include_gencols_type: PublishGencolsType,
    pub streamed_txns: *mut List, /* streamed toplevel transactions with this
                                   * schema */

    /* are we publishing this rel? */
    pub pubactions: PublicationActions,

    /*
     * ExprState array for row filter. Different publication actions don't
     * allow multiple expressions to always be combined into one, because
     * updates or deletes restrict the column in expression to be part of the
     * replica identity index whereas inserts do not have this restriction, so
     * there is one ExprState per publication action.
     */
    pub exprstate: [*mut ExprState; NUM_ROWFILTER_PUBACTIONS],
    pub estate: *mut EState,            /* executor state used for row filter */
    pub new_slot: *mut TupleTableSlot,  /* slot for storing new tuple */
    pub old_slot: *mut TupleTableSlot,  /* slot for storing old tuple */

    /*
     * OID of the relation to publish changes as.  For a partition, this may
     * be set to one of its ancestors whose schema will be used when
     * replicating changes, if publish_via_partition_root is set for the
     * publication.
     */
    pub publish_as_relid: Oid,

    /*
     * Map used when replicating using an ancestor's schema to convert tuples
     * from partition's type to the ancestor's; NULL if publish_as_relid is
     * same as 'relid' or if unnecessary due to partition and the ancestor
     * having identical TupleDesc.
     */
    pub attrmap: *mut AttrMap,

    /*
     * Columns included in the publication, or NULL if all columns are
     * included implicitly.  Note that the attnums in this bitmap are not
     * shifted by FirstLowInvalidHeapAttributeNumber.
     */
    pub columns: *mut Bitmapset,

    /*
     * Private context to store additional data for this entry - state for the
     * row filter expressions, column list, etc.
     */
    pub entry_cxt: MemoryContext,
}

// ---------------------------------------------------------------------------
// PGOutputTxnData
// ---------------------------------------------------------------------------
/*
 * Maintain a per-transaction level variable to track whether the transaction
 * has sent BEGIN. BEGIN is only sent when the first change in a transaction
 * is processed. This makes it possible to skip sending a pair of BEGIN/COMMIT
 * messages for empty transactions which saves network bandwidth.
 *
 * This optimization is not used for prepared transactions because if the
 * WALSender restarts after prepare of a transaction and before commit prepared
 * of the same transaction then we won't be able to figure out if we have
 * skipped sending BEGIN/PREPARE of a transaction as it was empty. This is
 * because we would have lost the in-memory txndata information that was
 * present prior to the restart. This will result in sending a spurious
 * COMMIT PREPARED without a corresponding prepared transaction at the
 * downstream which would lead to an error when it tries to process it.
 *
 * XXX We could achieve this optimization by changing protocol to send
 * additional information so that downstream can detect that the corresponding
 * prepare has not been sent. However, adding such a check for every
 * transaction in the downstream could be costly so we might want to do it
 * optionally.
 *
 * We also don't have this optimization for streamed transactions because
 * they can contain prepared transactions.
 */
#[repr(C)]
pub struct PGOutputTxnData {
    pub sent_begin_txn: bool, /* flag indicating whether BEGIN has been sent */
}

// ---------------------------------------------------------------------------
// PGOutputData
// ---------------------------------------------------------------------------
/* (from src/include/replication/pgoutput.h) */
#[repr(C)]
pub struct PGOutputData {
    pub context: MemoryContext,     /* private memory context for transient
                                     * allocations */
    pub cachectx: MemoryContext,    /* private memory context for cache data */
    pub pubctx: MemoryContext,      /* private memory context for publication data */

    pub in_streaming: bool,         /* true if we are streaming a chunk of
                                     * transaction */

    /* client-supplied info: */
    pub protocol_version: uint32,
    pub publication_names: *mut List,
    pub publications: *mut List,
    pub binary: bool,
    pub streaming: c_char,
    pub messages: bool,
    pub two_phase: bool,
    pub publish_no_origin: bool,
}

// ---------------------------------------------------------------------------
// Module-level static state
// ---------------------------------------------------------------------------

/* Map used to remember which relation schemas we sent. */
static mut RelationSyncCache: *mut HTAB = core::ptr::null_mut();

static mut publications_valid: bool = false;

// ---------------------------------------------------------------------------
// _PG_output_plugin_init -- Specify output plugin callbacks
// ---------------------------------------------------------------------------

/*
 * Specify output plugin callbacks
 */
pub unsafe fn _PG_output_plugin_init(cb: *mut OutputPluginCallbacks) {
    // Our callbacks take *mut LogicalDecodingContext (real struct) while the
    // vtable stores *mut c_void (the output_plugin.rs alias). The pointer
    // representations are identical, so transmute is safe here.
    (*cb).startup_cb = Some(std::mem::transmute(pgoutput_startup as usize));
    (*cb).begin_cb = Some(std::mem::transmute(pgoutput_begin_txn as usize));
    (*cb).change_cb = Some(std::mem::transmute(pgoutput_change as usize));
    (*cb).truncate_cb = Some(std::mem::transmute(pgoutput_truncate as usize));
    (*cb).message_cb = Some(std::mem::transmute(pgoutput_message as usize));
    (*cb).commit_cb = Some(std::mem::transmute(pgoutput_commit_txn as usize));

    (*cb).begin_prepare_cb = Some(std::mem::transmute(pgoutput_begin_prepare_txn as usize));
    (*cb).prepare_cb = Some(std::mem::transmute(pgoutput_prepare_txn as usize));
    (*cb).commit_prepared_cb = Some(std::mem::transmute(pgoutput_commit_prepared_txn as usize));
    (*cb).rollback_prepared_cb = Some(std::mem::transmute(pgoutput_rollback_prepared_txn as usize));
    (*cb).filter_by_origin_cb = Some(std::mem::transmute(pgoutput_origin_filter as usize));
    (*cb).shutdown_cb = Some(std::mem::transmute(pgoutput_shutdown as usize));

    /* transaction streaming */
    (*cb).stream_start_cb = Some(std::mem::transmute(pgoutput_stream_start as usize));
    (*cb).stream_stop_cb = Some(std::mem::transmute(pgoutput_stream_stop as usize));
    (*cb).stream_abort_cb = Some(std::mem::transmute(pgoutput_stream_abort as usize));
    (*cb).stream_commit_cb = Some(std::mem::transmute(pgoutput_stream_commit as usize));
    (*cb).stream_change_cb = Some(std::mem::transmute(pgoutput_change as usize));
    (*cb).stream_message_cb = Some(std::mem::transmute(pgoutput_message as usize));
    (*cb).stream_truncate_cb = Some(std::mem::transmute(pgoutput_truncate as usize));
    /* transaction streaming - two-phase commit */
    (*cb).stream_prepare_cb = Some(std::mem::transmute(pgoutput_stream_prepare_txn as usize));
}

// ---------------------------------------------------------------------------
// parse_output_parameters
// ---------------------------------------------------------------------------

unsafe fn parse_output_parameters(options: *mut List, data: *mut PGOutputData) {
    let mut protocol_version_given = false;
    let mut publication_names_given = false;
    let mut binary_option_given = false;
    let mut messages_option_given = false;
    let mut streaming_given = false;
    let mut two_phase_option_given = false;
    let mut origin_option_given = false;

    (*data).binary = false;
    (*data).streaming = LOGICALREP_STREAM_OFF;
    (*data).messages = false;
    (*data).two_phase = false;

    // foreach(lc, options)
    // TODO(pg-port): use real foreach! macro once pg_list iteration is wired
    let mut lc = if options.is_null() {
        core::ptr::null_mut()
    } else {
        (*(*options).elements).ptr_value
    };

    while !lc.is_null() {
        let defel = lc as *mut DefElem;

        // Assert(defel->arg == NULL || IsA(defel->arg, String));

        let defname_ptr = (*defel).defname;

        // Compare defname strings
        let defname = core::ffi::CStr::from_ptr(defname_ptr).to_bytes();

        if defname == b"proto_version" {
            if protocol_version_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            protocol_version_given = true;

            let arg_str = strVal((*defel).arg as *mut c_void);
            let mut endptr: *mut c_char = core::ptr::null_mut();

            // errno = 0;
            let parsed: u64 = libc_strtoul(arg_str, &mut endptr, 10);
            if !endptr.is_null() && *endptr != 0 {
                ereport!(ERROR,
                    errmsg!("invalid proto_version"));
            }

            if parsed > PG_UINT32_MAX {
                ereport!(ERROR,
                    errmsg!("proto_version out of range"));
            }

            (*data).protocol_version = parsed as uint32;
        } else if defname == b"publication_names" {
            if publication_names_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            publication_names_given = true;

            /*
             * Pass a copy of the DefElem->arg since SplitIdentifierString
             * modifies its input.
             */
            let arg_str = strVal((*defel).arg as *mut c_void);
            let mut namelist: *mut List = core::ptr::null_mut();
            if !SplitIdentifierString(pstrdup(arg_str), b',' as c_char, &mut namelist) {
                ereport!(ERROR, errmsg!("invalid publication_names syntax"));
            }
            (*data).publication_names = namelist;
        } else if defname == b"binary" {
            if binary_option_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            binary_option_given = true;

            (*data).binary = defGetBoolean(defel);
        } else if defname == b"messages" {
            if messages_option_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            messages_option_given = true;

            (*data).messages = defGetBoolean(defel);
        } else if defname == b"streaming" {
            if streaming_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            streaming_given = true;

            (*data).streaming = defGetStreamingMode(defel);
        } else if defname == b"two_phase" {
            if two_phase_option_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            two_phase_option_given = true;

            (*data).two_phase = defGetBoolean(defel);
        } else if defname == b"origin" {
            if origin_option_given {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
            }
            origin_option_given = true;

            let origin = defGetString(defel);
            let none_cstr = LOGICALREP_ORIGIN_NONE.as_ptr() as *const c_char;
            let any_cstr = LOGICALREP_ORIGIN_ANY.as_ptr() as *const c_char;
            if pg_strcasecmp(origin, none_cstr) == 0 {
                (*data).publish_no_origin = true;
            } else if pg_strcasecmp(origin, any_cstr) == 0 {
                (*data).publish_no_origin = false;
            } else {
                ereport!(ERROR, errmsg!("unrecognized origin value"));
            }
        } else {
            elog!(ERROR, "unrecognized pgoutput option");
        }

        // Advance to next list cell -- TODO(pg-port): use real list iteration
        lc = core::ptr::null_mut(); // sentinel: break after first (stub)
        break;
    }

    /* Check required options */
    if !protocol_version_given {
        ereport!(ERROR, errmsg!("option \"proto_version\" missing"));
    }
    if !publication_names_given {
        ereport!(ERROR, errmsg!("option \"publication_names\" missing"));
    }
}

// libc strtoul stub -- TODO(pg-port): expose via port
unsafe fn libc_strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> u64 {
    // TODO(pg-port): wire to real C strtoul
    0
}

// ---------------------------------------------------------------------------
// pgoutput_memory_context_reset
// ---------------------------------------------------------------------------

/*
 * Memory context reset callback of PGOutputData->context.
 */
unsafe extern "C" fn pgoutput_memory_context_reset(_arg: *mut c_void) {
    if !RelationSyncCache.is_null() {
        hash_destroy(RelationSyncCache);
        RelationSyncCache = core::ptr::null_mut();
    }
}

// ---------------------------------------------------------------------------
// pgoutput_startup
// ---------------------------------------------------------------------------

/*
 * Initialize this plugin
 */
unsafe extern "C" fn pgoutput_startup(
    ctx: *mut LogicalDecodingContext,
    opt: *mut OutputPluginOptions,
    is_init: bool,
) {
    let data = palloc0(core::mem::size_of::<PGOutputData>()) as *mut PGOutputData;
    static mut publication_callback_registered: bool = false;

    /* Create our memory context for private allocations. */
    (*data).context = AllocSetContextCreate(
        (*ctx).context,
        b"logical replication output context\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_MIN,
        ALLOCSET_DEFAULT_INIT,
        ALLOCSET_DEFAULT_MAX,
    );

    (*data).cachectx = AllocSetContextCreate(
        (*ctx).context,
        b"logical replication cache context\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_MIN,
        ALLOCSET_DEFAULT_INIT,
        ALLOCSET_DEFAULT_MAX,
    );

    (*data).pubctx = AllocSetContextCreate(
        (*ctx).context,
        b"logical replication publication list context\0".as_ptr() as *const c_char,
        ALLOCSET_SMALL_MIN,
        ALLOCSET_SMALL_INIT,
        ALLOCSET_SMALL_MAX,
    );

    /*
     * Ensure to cleanup RelationSyncCache even when logical decoding invoked
     * via SQL interface ends up with an error.
     */
    let mcallback = palloc0(core::mem::size_of::<MemoryContextCallback>()) as *mut MemoryContextCallback;
    (*mcallback).func = Some(pgoutput_memory_context_reset);
    MemoryContextRegisterResetCallback((*ctx).context as *mut std::ffi::c_void as crate::utils::mmgr::memnodes::MemoryContext, mcallback);

    (*ctx).output_plugin_private = data as *mut c_void;

    /* This plugin uses binary protocol. */
    (*opt).output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

    /*
     * This is replication start and not slot initialization.
     *
     * Parse and validate options passed by the client.
     */
    if !is_init {
        /* Parse the params and ERROR if we see any we don't recognize */
        parse_output_parameters((*ctx).output_plugin_options, data);

        /* Check if we support requested protocol */
        if (*data).protocol_version > LOGICALREP_PROTO_MAX_VERSION_NUM as uint32 {
            ereport!(ERROR,
                errmsg!("client sent proto_version but server only supports protocol or lower"));
        }

        if (*data).protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM as uint32 {
            ereport!(ERROR,
                errmsg!("client sent proto_version but server only supports protocol or higher"));
        }

        /*
         * Decide whether to enable streaming. It is disabled by default, in
         * which case we just update the flag in decoding context. Otherwise
         * we only allow it with sufficient version of the protocol, and when
         * the output plugin supports it.
         */
        if (*data).streaming == LOGICALREP_STREAM_OFF {
            (*ctx).streaming = false;
        } else if (*data).streaming == LOGICALREP_STREAM_ON
            && ((*data).protocol_version as c_int) < LOGICALREP_PROTO_STREAM_VERSION_NUM
        {
            ereport!(ERROR,
                errmsg!("requested proto_version does not support streaming"));
        } else if (*data).streaming == LOGICALREP_STREAM_PARALLEL
            && ((*data).protocol_version as c_int) < LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
        {
            ereport!(ERROR,
                errmsg!("requested proto_version does not support parallel streaming"));
        } else if !(*ctx).streaming {
            ereport!(ERROR,
                errmsg!("streaming requested, but not supported by output plugin"));
        }

        /*
         * Here, we just check whether the two-phase option is passed by
         * plugin and decide whether to enable it at later point of time. It
         * remains enabled if the previous start-up has done so. But we only
         * allow the option to be passed in with sufficient version of the
         * protocol, and when the output plugin supports it.
         */
        if !(*data).two_phase {
            (*ctx).twophase_opt_given = false;
        } else if ((*data).protocol_version as c_int) < LOGICALREP_PROTO_TWOPHASE_VERSION_NUM {
            ereport!(ERROR,
                errmsg!("requested proto_version does not support two-phase commit"));
        } else if !(*ctx).twophase {
            ereport!(ERROR,
                errmsg!("two-phase commit requested, but not supported by output plugin"));
        } else {
            (*ctx).twophase_opt_given = true;
        }

        /* Init publication state. */
        (*data).publications = core::ptr::null_mut(); // NIL
        publications_valid = false;

        /*
         * Register callback for pg_publication if we didn't already do that
         * during some previous call in this process.
         */
        if !publication_callback_registered {
            CacheRegisterSyscacheCallback(
                PUBLICATIONOID,
                publication_invalidation_cb,
                0 as Datum,
            );
            CacheRegisterRelSyncCallback(rel_sync_cache_relation_cb, 0 as Datum);
            publication_callback_registered = true;
        }

        /* Initialize relation schema cache. */
        init_rel_sync_cache(CacheMemoryContext as *mut std::ffi::c_void as MemoryContext);
    } else {
        /*
         * Disable the streaming and prepared transactions during the slot
         * initialization mode.
         */
        (*ctx).streaming = false;
        (*ctx).twophase = false;
    }
}

// ---------------------------------------------------------------------------
// pgoutput_begin_txn
// ---------------------------------------------------------------------------

/*
 * BEGIN callback.
 *
 * Don't send the BEGIN message here instead postpone it until the first
 * change. In logical replication, a common scenario is to replicate a set of
 * tables (instead of all tables) and transactions whose changes were on
 * the table(s) that are not published will produce empty transactions. These
 * empty transactions will send BEGIN and COMMIT messages to subscribers,
 * using bandwidth on something with little/no use for logical replication.
 */
unsafe extern "C" fn pgoutput_begin_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
) {
    let txndata = MemoryContextAllocZero(
        (*ctx).context,
        core::mem::size_of::<PGOutputTxnData>(),
    ) as *mut PGOutputTxnData;

    (*txn).output_plugin_private = txndata as *mut c_void;
}

// ---------------------------------------------------------------------------
// pgoutput_send_begin
// ---------------------------------------------------------------------------

/*
 * Send BEGIN.
 *
 * This is called while processing the first change of the transaction.
 */
unsafe fn pgoutput_send_begin(ctx: *mut LogicalDecodingContext, txn: *mut ReorderBufferTXN) {
    let send_replication_origin = (*txn).origin_id != InvalidRepOriginId;
    let txndata = (*txn).output_plugin_private as *mut PGOutputTxnData;

    // Assert(txndata);
    // Assert(!txndata->sent_begin_txn);

    OutputPluginPrepareWrite(ctx, !send_replication_origin);
    logicalrep_write_begin((*ctx).out, txn as *mut c_void);
    (*txndata).sent_begin_txn = true;

    send_repl_origin(ctx, (*txn).origin_id, (*txn).origin_lsn, send_replication_origin);

    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_commit_txn
// ---------------------------------------------------------------------------

/*
 * COMMIT callback
 */
unsafe extern "C" fn pgoutput_commit_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let txndata = (*txn).output_plugin_private as *mut PGOutputTxnData;

    // Assert(txndata);

    /*
     * We don't need to send the commit message unless some relevant change
     * from this transaction has been sent to the downstream.
     */
    let sent_begin_txn = (*txndata).sent_begin_txn;
    OutputPluginUpdateProgress(ctx, !sent_begin_txn);
    pfree(txndata as *mut c_void);
    (*txn).output_plugin_private = core::ptr::null_mut();

    if !sent_begin_txn {
        elog!(DEBUG1, "skipped replication of an empty transaction");
        return;
    }

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_commit((*ctx).out, txn as *mut c_void, commit_lsn);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_begin_prepare_txn
// ---------------------------------------------------------------------------

/*
 * BEGIN PREPARE callback
 */
unsafe extern "C" fn pgoutput_begin_prepare_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
) {
    let send_replication_origin = (*txn).origin_id != InvalidRepOriginId;

    OutputPluginPrepareWrite(ctx, !send_replication_origin);
    logicalrep_write_begin_prepare((*ctx).out, txn as *mut c_void);

    send_repl_origin(ctx, (*txn).origin_id, (*txn).origin_lsn, send_replication_origin);

    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_prepare_txn
// ---------------------------------------------------------------------------

/*
 * PREPARE callback
 */
unsafe extern "C" fn pgoutput_prepare_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    OutputPluginUpdateProgress(ctx, false);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_prepare((*ctx).out, txn as *mut c_void, prepare_lsn);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_commit_prepared_txn
// ---------------------------------------------------------------------------

/*
 * COMMIT PREPARED callback
 */
unsafe extern "C" fn pgoutput_commit_prepared_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    OutputPluginUpdateProgress(ctx, false);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_commit_prepared((*ctx).out, txn as *mut c_void, commit_lsn);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_rollback_prepared_txn
// ---------------------------------------------------------------------------

/*
 * ROLLBACK PREPARED callback
 */
unsafe extern "C" fn pgoutput_rollback_prepared_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
) {
    OutputPluginUpdateProgress(ctx, false);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_rollback_prepared((*ctx).out, txn as *mut c_void, prepare_end_lsn, prepare_time);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// maybe_send_schema
// ---------------------------------------------------------------------------

/*
 * Write the current schema of the relation and its ancestor (if any) if not
 * done yet.
 */
unsafe fn maybe_send_schema(
    ctx: *mut LogicalDecodingContext,
    change: *mut ReorderBufferChange,
    relation: crate::utils::rel::Relation,
    relentry: *mut RelationSyncEntry,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let schema_sent: bool;
    let mut xid: TransactionId = InvalidTransactionId;
    let mut topxid: TransactionId = InvalidTransactionId;

    /*
     * Remember XID of the (sub)transaction for the change. We don't care if
     * it's top-level transaction or not (we have already sent that XID in
     * start of the current streaming block).
     *
     * If we're not in a streaming block, just use InvalidTransactionId and
     * the write methods will not include it.
     */
    if (*data).in_streaming {
        xid = (*(*change).txn).xid;
    }

    if rbtxn_is_subtxn((*change).txn) {
        topxid = (*rbtxn_get_toptxn((*change).txn)).xid;
    } else {
        topxid = xid;
    }

    /*
     * Do we need to send the schema? We do track streamed transactions
     * separately, because those may be applied later (and the regular
     * transactions won't see their effects until then) and in an order that
     * we don't know at this point.
     *
     * XXX There is a scope of optimization here. Currently, we always send
     * the schema first time in a streaming transaction but we can probably
     * avoid that by checking 'relentry->schema_sent' flag. However, before
     * doing that we need to study its impact on the case where we have a mix
     * of streaming and non-streaming transactions.
     */
    if (*data).in_streaming {
        schema_sent = get_schema_sent_in_streamed_txn(relentry, topxid);
    } else {
        schema_sent = (*relentry).schema_sent;
    }

    /* Nothing to do if we already sent the schema. */
    if schema_sent {
        return;
    }

    /*
     * Send the schema.  If the changes will be published using an ancestor's
     * schema, not the relation's own, send that ancestor's schema before
     * sending relation's own (XXX - maybe sending only the former suffices?).
     */
    if (*relentry).publish_as_relid != RelationGetRelid(relation) {
        let ancestor = RelationIdGetRelation((*relentry).publish_as_relid);
        send_relation_and_attrs(ancestor, xid, ctx, relentry);
        RelationClose(ancestor);
    }

    send_relation_and_attrs(relation, xid, ctx, relentry);

    if (*data).in_streaming {
        set_schema_sent_in_streamed_txn(relentry, topxid);
    } else {
        (*relentry).schema_sent = true;
    }
}

// ---------------------------------------------------------------------------
// send_relation_and_attrs
// ---------------------------------------------------------------------------

/*
 * Sends a relation
 */
unsafe fn send_relation_and_attrs(
    relation: crate::utils::rel::Relation,
    xid: TransactionId,
    ctx: *mut LogicalDecodingContext,
    relentry: *mut RelationSyncEntry,
) {
    let desc = RelationGetDescr(relation);
    let columns = (*relentry).columns;
    let include_gencols_type = (*relentry).include_gencols_type;

    /*
     * Write out type info if needed.  We do that only for user-created types.
     * We use FirstGenbkiObjectId as the cutoff, so that we only consider
     * objects with hand-assigned OIDs to be "built in", not for instance any
     * function or type defined in the information_schema. This is important
     * because only hand-assigned OIDs can be expected to remain stable across
     * major versions.
     */
    let natts = (*desc).natts;
    for i in 0..natts {
        let att = TupleDescAttr(desc, i);

        if !logicalrep_should_publish_column(att as *mut c_void, columns, include_gencols_type as c_int) {
            continue;
        }

        if (*att).atttypid < FirstGenbkiObjectId {
            continue;
        }

        OutputPluginPrepareWrite(ctx, false);
        logicalrep_write_typ((*ctx).out, xid, (*att).atttypid);
        OutputPluginWrite(ctx, false);
    }

    OutputPluginPrepareWrite(ctx, false);
    logicalrep_write_rel((*ctx).out, xid, relation as *mut c_void, columns, include_gencols_type as c_int);
    OutputPluginWrite(ctx, false);
}

// ---------------------------------------------------------------------------
// create_estate_for_relation
// ---------------------------------------------------------------------------

/*
 * Executor state preparation for evaluation of row filter expressions for the
 * specified relation.
 */
unsafe fn create_estate_for_relation(rel: crate::utils::rel::Relation) -> *mut EState {
    let estate = CreateExecutorState();

    let rte = makeNode_RangeTblEntry();
    (*rte).rtekind = RTEKind::RTE_RELATION;
    (*rte).relid = RelationGetRelid(rel);
    (*rte).relkind = (*(*rel).rd_rel).relkind;
    (*rte).rellockmode = AccessShareLock;

    let mut perminfos: *mut List = core::ptr::null_mut();
    addRTEPermissionInfo(&mut perminfos, rte);

    let rte_list = lappend(core::ptr::null_mut(), rte as *mut c_void);
    ExecInitRangeTable(estate, rte_list, perminfos, bms_make_singleton(1));

    (*estate).es_output_cid = GetCurrentCommandId(false);

    estate
}

// ---------------------------------------------------------------------------
// pgoutput_row_filter_exec_expr
// ---------------------------------------------------------------------------

/*
 * Evaluates row filter.
 *
 * If the row filter evaluates to NULL, it is taken as false i.e. the change
 * isn't replicated.
 */
unsafe fn pgoutput_row_filter_exec_expr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
) -> bool {
    let mut isnull: bool = false;

    // Assert(state != NULL);

    let ret = ExecEvalExprSwitchContext(state, econtext, &mut isnull);

    elog!(DEBUG3, "row filter evaluates");

    if isnull {
        return false;
    }

    crate::postgres::DatumGetBool(ret)
}

// ---------------------------------------------------------------------------
// pgoutput_ensure_entry_cxt
// ---------------------------------------------------------------------------

/*
 * Make sure the per-entry memory context exists.
 */
unsafe fn pgoutput_ensure_entry_cxt(data: *mut PGOutputData, entry: *mut RelationSyncEntry) {
    /* The context may already exist, in which case bail out. */
    if !(*entry).entry_cxt.is_null() {
        return;
    }

    let relation = RelationIdGetRelation((*entry).publish_as_relid);

    (*entry).entry_cxt = AllocSetContextCreate(
        (*data).cachectx,
        b"entry private context\0".as_ptr() as *const c_char,
        ALLOCSET_SMALL_MIN,
        ALLOCSET_SMALL_INIT,
        ALLOCSET_SMALL_MAX,
    );

    MemoryContextCopyAndSetIdentifier(
        (*entry).entry_cxt,
        RelationGetRelationName(relation),
    );
}

// ---------------------------------------------------------------------------
// pgoutput_row_filter_init
// ---------------------------------------------------------------------------

/*
 * Initialize the row filter.
 */
unsafe fn pgoutput_row_filter_init(
    data: *mut PGOutputData,
    publications: *mut List,
    entry: *mut RelationSyncEntry,
) {
    let mut rfnodes: [*mut List; NUM_ROWFILTER_PUBACTIONS] = [
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        core::ptr::null_mut(),
    ]; /* One per pubaction */
    let mut no_filter: [bool; NUM_ROWFILTER_PUBACTIONS] = [false, false, false]; /* One per pubaction */
    let mut has_filter = true;
    let schemaid = get_rel_namespace((*entry).publish_as_relid);

    /*
     * Find if there are any row filters for this relation. If there are, then
     * prepare the necessary ExprState and cache it in entry->exprstate. To
     * build an expression state, we need to ensure the following:
     *
     * All the given publication-table mappings must be checked.
     *
     * Multiple publications might have multiple row filters for this
     * relation. Since row filter usage depends on the DML operation, there
     * are multiple lists (one for each operation) to which row filters will
     * be appended.
     *
     * FOR ALL TABLES and FOR TABLES IN SCHEMA implies "don't use row filter
     * expression" so it takes precedence.
     */

    // foreach(lc, publications)
    // TODO(pg-port): real list iteration via foreach!; using stub loop
    let mut pub_cell_ptr = if publications.is_null() {
        core::ptr::null_mut()
    } else {
        (*(*publications).elements).ptr_value
    };

    while !pub_cell_ptr.is_null() {
        let pub_ = pub_cell_ptr as *mut Publication;
        let rftuple: crate::access::htup_details::HeapTupleData = core::mem::zeroed();
        let rftuple_ptr: *mut crate::access::htup_details::HeapTupleData = core::ptr::null_mut();
        let mut rfdatum: Datum = 0;
        let mut pub_no_filter = true;

        /*
         * If the publication is FOR ALL TABLES, or the publication includes a
         * FOR TABLES IN SCHEMA where the table belongs to the referred
         * schema, then it is treated the same as if there are no row filters
         * (even if other publications have a row filter).
         */
        if !(*pub_).alltables
            && !SearchSysCacheExists2(
                PUBLICATIONNAMESPACEMAP,
                ObjectIdGetDatum(schemaid),
                ObjectIdGetDatum((*pub_).oid),
            )
        {
            /*
             * Check for the presence of a row filter in this publication.
             */
            let ht = SearchSysCache2(
                PUBLICATIONRELMAP,
                ObjectIdGetDatum((*entry).publish_as_relid),
                ObjectIdGetDatum((*pub_).oid),
            );

            if HeapTupleIsValid(ht as *mut c_void) {
                /* Null indicates no filter. */
                rfdatum = SysCacheGetAttr(
                    PUBLICATIONRELMAP,
                    ht,
                    Anum_pg_publication_rel_prqual as i16,
                    &mut pub_no_filter,
                );
            }
        }

        if pub_no_filter {
            if !rftuple_ptr.is_null() {
                ReleaseSysCache(rftuple_ptr);
            }

            no_filter[PUBACTION_INSERT] |= (*pub_).pubactions.pubinsert;
            no_filter[PUBACTION_UPDATE] |= (*pub_).pubactions.pubupdate;
            no_filter[PUBACTION_DELETE] |= (*pub_).pubactions.pubdelete;

            /*
             * Quick exit if all the DML actions are publicized via this
             * publication.
             */
            if no_filter[PUBACTION_INSERT]
                && no_filter[PUBACTION_UPDATE]
                && no_filter[PUBACTION_DELETE]
            {
                has_filter = false;
                break;
            }

            /* No additional work for this publication. Next one. */
            pub_cell_ptr = core::ptr::null_mut(); // TODO(pg-port): advance list cell
            continue;
        }

        /* Form the per pubaction row filter lists. */
        if (*pub_).pubactions.pubinsert && !no_filter[PUBACTION_INSERT] {
            let s = crate::utils::adt::varlena::TextDatumGetCString(rfdatum);
            rfnodes[PUBACTION_INSERT] = lappend(rfnodes[PUBACTION_INSERT], s as *mut c_void);
        }
        if (*pub_).pubactions.pubupdate && !no_filter[PUBACTION_UPDATE] {
            let s = crate::utils::adt::varlena::TextDatumGetCString(rfdatum);
            rfnodes[PUBACTION_UPDATE] = lappend(rfnodes[PUBACTION_UPDATE], s as *mut c_void);
        }
        if (*pub_).pubactions.pubdelete && !no_filter[PUBACTION_DELETE] {
            let s = crate::utils::adt::varlena::TextDatumGetCString(rfdatum);
            rfnodes[PUBACTION_DELETE] = lappend(rfnodes[PUBACTION_DELETE], s as *mut c_void);
        }

        ReleaseSysCache(rftuple_ptr);

        pub_cell_ptr = core::ptr::null_mut(); // TODO(pg-port): advance list cell
    } /* loop all subscribed publications */

    /* Clean the row filter */
    for idx in 0..NUM_ROWFILTER_PUBACTIONS {
        if no_filter[idx] {
            list_free_deep(rfnodes[idx]);
            rfnodes[idx] = core::ptr::null_mut();
        }
    }

    if has_filter {
        let relation = RelationIdGetRelation((*entry).publish_as_relid);

        pgoutput_ensure_entry_cxt(data, entry);

        /*
         * Now all the filters for all pubactions are known. Combine them when
         * their pubactions are the same.
         */
        let oldctx = MemoryContextSwitchTo((*entry).entry_cxt);
        (*entry).estate = create_estate_for_relation(relation);
        for idx in 0..NUM_ROWFILTER_PUBACTIONS {
            if rfnodes[idx].is_null() {
                continue;
            }

            let mut filters: *mut List = core::ptr::null_mut();

            // foreach(lc, rfnodes[idx])
            // TODO(pg-port): real list iteration
            let mut cell = if rfnodes[idx].is_null() {
                core::ptr::null_mut()
            } else {
                (*(*rfnodes[idx]).elements).ptr_value
            };
            while !cell.is_null() {
                let s = cell as *mut c_char;
                let node = expand_generated_columns_in_expr(
                    stringToNode(s),
                    relation,
                    1,
                );
                filters = lappend(filters, node);
                cell = core::ptr::null_mut(); // TODO(pg-port): advance list cell
            }

            /* combine the row filter and cache the ExprState */
            let rfnode = make_orclause(filters) as *mut crate::nodes::primnodes::Expr;
            (*entry).exprstate[idx] = ExecPrepareExpr(rfnode, (*entry).estate);
        } /* for each pubaction */
        MemoryContextSwitchTo(oldctx);

        RelationClose(relation);
    }
}

// ---------------------------------------------------------------------------
// check_and_init_gencol
// ---------------------------------------------------------------------------

/*
 * If the table contains a generated column, check for any conflicting
 * values of 'publish_generated_columns' parameter in the publications.
 */
unsafe fn check_and_init_gencol(
    _data: *mut PGOutputData,
    publications: *mut List,
    entry: *mut RelationSyncEntry,
) {
    let relation = RelationIdGetRelation((*entry).publish_as_relid);
    let desc = RelationGetDescr(relation);
    let mut gencolpresent = false;
    let mut first = true;

    /* Check if there is any generated column present. */
    for i in 0..(*desc).natts {
        let att = TupleDescAttr(desc, i);

        if (*att).attgenerated != 0 {
            gencolpresent = true;
            break;
        }
    }

    /* There are no generated columns to be published. */
    if !gencolpresent {
        (*entry).include_gencols_type = PUBLISH_GENCOLS_NONE;
        return;
    }

    /*
     * There may be a conflicting value for 'publish_generated_columns'
     * parameter in the publications.
     */
    // foreach_ptr(Publication, pub, publications)
    // TODO(pg-port): real list iteration
    let mut cell = if publications.is_null() {
        core::ptr::null_mut()
    } else {
        (*(*publications).elements).ptr_value
    };
    while !cell.is_null() {
        let pub_ = cell as *mut Publication;

        /*
         * The column list takes precedence over the
         * 'publish_generated_columns' parameter. Those will be checked later,
         * see pgoutput_column_list_init.
         */
        if check_and_fetch_column_list(pub_, (*entry).publish_as_relid, core::ptr::null_mut(), core::ptr::null_mut()) {
            cell = core::ptr::null_mut(); // TODO(pg-port): advance
            continue;
        }

        if first {
            (*entry).include_gencols_type = (*pub_).pubgencols_type;
            first = false;
        } else if (*entry).include_gencols_type != (*pub_).pubgencols_type {
            ereport!(ERROR,
                errmsg!("cannot use different values of publish_generated_columns for table in different publications"));
        }

        cell = core::ptr::null_mut(); // TODO(pg-port): advance
    }
}

// ---------------------------------------------------------------------------
// pgoutput_column_list_init
// ---------------------------------------------------------------------------

/*
 * Initialize the column list.
 */
unsafe fn pgoutput_column_list_init(
    data: *mut PGOutputData,
    publications: *mut List,
    entry: *mut RelationSyncEntry,
) {
    let mut first = true;
    let relation = RelationIdGetRelation((*entry).publish_as_relid);
    let mut found_pub_collist = false;
    let mut relcols: *mut Bitmapset = core::ptr::null_mut();

    pgoutput_ensure_entry_cxt(data, entry);

    /*
     * Find if there are any column lists for this relation. If there are,
     * build a bitmap using the column lists.
     *
     * Multiple publications might have multiple column lists for this
     * relation.
     *
     * Note that we don't support the case where the column list is different
     * for the same table when combining publications. See comments atop
     * fetch_table_list. But one can later change the publication so we still
     * need to check all the given publication-table mappings and report an
     * error if any publications have a different column list.
     */
    // foreach(lc, publications)
    // TODO(pg-port): real list iteration
    let mut cell = if publications.is_null() {
        core::ptr::null_mut()
    } else {
        (*(*publications).elements).ptr_value
    };
    while !cell.is_null() {
        let pub_ = cell as *mut Publication;
        let mut cols: *mut Bitmapset = core::ptr::null_mut();

        /* Retrieve the bitmap of columns for a column list publication. */
        let has_collist = check_and_fetch_column_list(
            pub_,
            (*entry).publish_as_relid,
            (*entry).entry_cxt,
            &mut cols,
        );
        found_pub_collist |= has_collist;

        /*
         * For non-column list publications -- e.g. TABLE (without a column
         * list), ALL TABLES, or ALL TABLES IN SCHEMA, we consider all columns
         * of the table (including generated columns when
         * 'publish_generated_columns' parameter is true).
         */
        if cols.is_null() {
            /*
             * Cache the table columns for the first publication with no
             * specified column list to detect publication with a different
             * column list.
             */
            if relcols.is_null() && list_length(publications) > 1 {
                let oldcxt = MemoryContextSwitchTo((*entry).entry_cxt);

                relcols = pub_form_cols_map(relation, (*entry).include_gencols_type);
                MemoryContextSwitchTo(oldcxt);
            }

            cols = relcols;
        }

        if first {
            (*entry).columns = cols;
            first = false;
        } else if !bms_equal((*entry).columns, cols) {
            ereport!(ERROR,
                errmsg!("cannot use different column lists for table in different publications"));
        }

        cell = core::ptr::null_mut(); // TODO(pg-port): advance list cell
    } /* loop all subscribed publications */

    /*
     * If no column list publications exist, columns to be published will be
     * computed later according to the 'publish_generated_columns' parameter.
     */
    if !found_pub_collist {
        (*entry).columns = core::ptr::null_mut();
    }

    RelationClose(relation);
}

// ---------------------------------------------------------------------------
// init_tuple_slot
// ---------------------------------------------------------------------------

/*
 * Initialize the slot for storing new and old tuples, and build the map that
 * will be used to convert the relation's tuples into the ancestor's format.
 */
unsafe fn init_tuple_slot(
    data: *mut PGOutputData,
    relation: crate::utils::rel::Relation,
    entry: *mut RelationSyncEntry,
) {
    let oldctx = MemoryContextSwitchTo((*data).cachectx);

    /*
     * Create tuple table slots. Create a copy of the TupleDesc as it needs to
     * live as long as the cache remains.
     */
    let oldtupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));
    let newtupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

    (*entry).old_slot = MakeSingleTupleTableSlot(oldtupdesc, &TTSOpsHeapTuple as *const TupleTableSlotOps);
    (*entry).new_slot = MakeSingleTupleTableSlot(newtupdesc, &TTSOpsHeapTuple as *const TupleTableSlotOps);

    MemoryContextSwitchTo(oldctx);

    /*
     * Cache the map that will be used to convert the relation's tuples into
     * the ancestor's format, if needed.
     */
    if (*entry).publish_as_relid != RelationGetRelid(relation) {
        let ancestor = RelationIdGetRelation((*entry).publish_as_relid);
        let indesc = RelationGetDescr(relation);
        let outdesc = RelationGetDescr(ancestor);

        /* Map must live as long as the logical decoding context. */
        let oldctx2 = MemoryContextSwitchTo((*data).cachectx);

        (*entry).attrmap = build_attrmap_by_name_if_req(indesc, outdesc, false);

        MemoryContextSwitchTo(oldctx2);
        RelationClose(ancestor);
    }
}

// ---------------------------------------------------------------------------
// pgoutput_row_filter
// ---------------------------------------------------------------------------

/*
 * Change is checked against the row filter if any.
 *
 * Returns true if the change is to be replicated, else false.
 *
 * For inserts, evaluate the row filter for new tuple.
 * For deletes, evaluate the row filter for old tuple.
 * For updates, evaluate the row filter for old and new tuple.
 *
 * For updates, if both evaluations are true, we allow sending the UPDATE and
 * if both the evaluations are false, it doesn't replicate the UPDATE. Now, if
 * only one of the tuples matches the row filter expression, we transform
 * UPDATE to DELETE or INSERT to avoid any data inconsistency based on the
 * following rules:
 *
 * Case 1: old-row (no match)    new-row (no match)  -> (drop change)
 * Case 2: old-row (no match)    new row (match)     -> INSERT
 * Case 3: old-row (match)       new-row (no match)  -> DELETE
 * Case 4: old-row (match)       new row (match)     -> UPDATE
 *
 * The new action is updated in the action parameter.
 *
 * The new slot could be updated when transforming the UPDATE into INSERT,
 * because the original new tuple might not have column values from the replica
 * identity.
 */
unsafe fn pgoutput_row_filter(
    relation: crate::utils::rel::Relation,
    old_slot: *mut TupleTableSlot,
    new_slot_ptr: *mut *mut TupleTableSlot,
    entry: *mut RelationSyncEntry,
    action: *mut ReorderBufferChangeType,
) -> bool {
    let result: bool;
    let mut tmp_new_slot: *mut TupleTableSlot = core::ptr::null_mut();
    let new_slot = *new_slot_ptr;
    let ecxt: *mut ExprContext;
    let filter_exprstate: *mut ExprState;

    /*
     * We need this map to avoid relying on ReorderBufferChangeType enums
     * having specific values.
     */
    // map_changetype_pubaction: indexed by ReorderBufferChangeType variant value
    // Insert=0 -> PUBACTION_INSERT=0, Update=1 -> PUBACTION_UPDATE=1, Delete=2 -> PUBACTION_DELETE=2
    let pubaction_idx: usize = match *action {
        REORDER_BUFFER_CHANGE_INSERT => PUBACTION_INSERT,
        REORDER_BUFFER_CHANGE_UPDATE => PUBACTION_UPDATE,
        REORDER_BUFFER_CHANGE_DELETE => PUBACTION_DELETE,
        _ => {
            // Assert(false) in C; panic is appropriate here
            unreachable!("unexpected ReorderBufferChangeType in row filter");
        }
    };

    // Assert(new_slot || old_slot);

    /* Get the corresponding row filter */
    filter_exprstate = (*entry).exprstate[pubaction_idx];

    /* Bail out if there is no row filter */
    if filter_exprstate.is_null() {
        return true;
    }

    elog!(DEBUG3, "table has row filter");

    ResetPerTupleExprContext((*entry).estate);

    ecxt = GetPerTupleExprContext((*entry).estate);

    /*
     * For the following occasions where there is only one tuple, we can
     * evaluate the row filter for that tuple and return.
     *
     * For inserts, we only have the new tuple.
     *
     * For updates, we can have only a new tuple when none of the replica
     * identity columns changed and none of those columns have external data
     * but we still need to evaluate the row filter for the new tuple as the
     * existing values of those columns might not match the filter. Also,
     * users can use constant expressions in the row filter, so we anyway need
     * to evaluate it for the new tuple.
     *
     * For deletes, we only have the old tuple.
     */
    if new_slot.is_null() || old_slot.is_null() {
        (*ecxt).ecxt_scantuple = if !new_slot.is_null() { new_slot } else { old_slot };
        result = pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);
        return result;
    }

    /*
     * Both the old and new tuples must be valid only for updates and need to
     * be checked against the row filter.
     */
    // Assert(pubaction_idx == PUBACTION_UPDATE);

    slot_getallattrs(new_slot);
    slot_getallattrs(old_slot);

    tmp_new_slot = core::ptr::null_mut();
    let desc = RelationGetDescr(relation);

    /*
     * The new tuple might not have all the replica identity columns, in which
     * case it needs to be copied over from the old tuple.
     */
    for i in 0..(*desc).natts {
        let att = TupleDescCompactAttr(desc, i);

        /*
         * if the column in the new tuple or old tuple is null, nothing to do
         */
        if *(*new_slot).tts_isnull.add(i as usize) || *(*old_slot).tts_isnull.add(i as usize) {
            continue;
        }

        /*
         * Unchanged toasted replica identity columns are only logged in the
         * old tuple. Copy this over to the new tuple. The changed (or WAL
         * Logged) toast values are always assembled in memory and set as
         * VARTAG_INDIRECT. See ReorderBufferToastReplace.
         */
        if (*att).attlen == -1
            && VARATT_IS_EXTERNAL_ONDISK(*(*new_slot).tts_values.add(i as usize))
            && !VARATT_IS_EXTERNAL_ONDISK(*(*old_slot).tts_values.add(i as usize))
        {
            if tmp_new_slot.is_null() {
                tmp_new_slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual as *const TupleTableSlotOps);
                ExecClearTuple(tmp_new_slot);

                core::ptr::copy_nonoverlapping(
                    (*new_slot).tts_values,
                    (*tmp_new_slot).tts_values,
                    (*desc).natts as usize,
                );
                core::ptr::copy_nonoverlapping(
                    (*new_slot).tts_isnull,
                    (*tmp_new_slot).tts_isnull,
                    (*desc).natts as usize,
                );
            }

            *(*tmp_new_slot).tts_values.add(i as usize) = *(*old_slot).tts_values.add(i as usize);
            *(*tmp_new_slot).tts_isnull.add(i as usize) = *(*old_slot).tts_isnull.add(i as usize);
        }
    }

    (*ecxt).ecxt_scantuple = old_slot;
    let old_matched = pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);

    if !tmp_new_slot.is_null() {
        ExecStoreVirtualTuple(tmp_new_slot);
        (*ecxt).ecxt_scantuple = tmp_new_slot;
    } else {
        (*ecxt).ecxt_scantuple = new_slot;
    }

    let new_matched = pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);

    /*
     * Case 1: if both tuples don't match the row filter, bailout. Send
     * nothing.
     */
    if !old_matched && !new_matched {
        return false;
    }

    /*
     * Case 2: if the old tuple doesn't satisfy the row filter but the new
     * tuple does, transform the UPDATE into INSERT.
     *
     * Use the newly transformed tuple that must contain the column values for
     * all the replica identity columns. This is required to ensure that the
     * while inserting the tuple in the downstream node, we have all the
     * required column values.
     */
    if !old_matched && new_matched {
        *action = REORDER_BUFFER_CHANGE_INSERT;

        if !tmp_new_slot.is_null() {
            *new_slot_ptr = tmp_new_slot;
        }
    }

    /*
     * Case 3: if the old tuple satisfies the row filter but the new tuple
     * doesn't, transform the UPDATE into DELETE.
     *
     * This transformation does not require another tuple. The Old tuple will
     * be used for DELETE.
     */
    else if old_matched && !new_matched {
        *action = REORDER_BUFFER_CHANGE_DELETE;
    }

    /*
     * Case 4: if both tuples match the row filter, transformation isn't
     * required. (*action is default UPDATE).
     */

    true
}

// ---------------------------------------------------------------------------
// VARATT_IS_EXTERNAL_ONDISK stub -- TODO(pg-port): access/varatt.h
// ---------------------------------------------------------------------------
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(_datum: Datum) -> bool {
    false // TODO(pg-port): access/varatt.h
}

// ---------------------------------------------------------------------------
// pgoutput_change
// ---------------------------------------------------------------------------

/*
 * Sends the decoded DML over wire.
 *
 * This is called both in streaming and non-streaming modes.
 */
unsafe extern "C" fn pgoutput_change(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    relation: crate::utils::rel::Relation,
    change: *mut ReorderBufferChange,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let txndata = (*txn).output_plugin_private as *mut PGOutputTxnData;
    let old: MemoryContext;
    let relentry: *mut RelationSyncEntry;
    let mut xid: TransactionId = InvalidTransactionId;
    let mut ancestor: crate::utils::rel::Relation = core::ptr::null_mut();
    let mut targetrel = relation;
    let mut action = (*change).action;
    let mut old_slot: *mut TupleTableSlot = core::ptr::null_mut();
    let mut new_slot: *mut TupleTableSlot = core::ptr::null_mut();

    if !is_publishable_relation(relation) {
        return;
    }

    /*
     * Remember the xid for the change in streaming mode. We need to send xid
     * with each change in the streaming mode so that subscriber can make
     * their association and on aborts, it can discard the corresponding
     * changes.
     */
    if (*data).in_streaming {
        xid = (*(*change).txn).xid;
    }

    relentry = get_rel_sync_entry(data, relation);

    /* First check the table filter */
    match action {
        REORDER_BUFFER_CHANGE_INSERT => {
            if !(*relentry).pubactions.pubinsert {
                return;
            }
        }
        REORDER_BUFFER_CHANGE_UPDATE => {
            if !(*relentry).pubactions.pubupdate {
                return;
            }
        }
        REORDER_BUFFER_CHANGE_DELETE => {
            if !(*relentry).pubactions.pubdelete {
                return;
            }

            /*
             * This is only possible if deletes are allowed even when replica
             * identity is not defined for a table. Since the DELETE action
             * can't be published, we simply return.
             */
            if (*change).data.tp.oldtuple.is_null() {
                elog!(DEBUG1, "didn't send DELETE change because of missing oldtuple");
                return;
            }
        }
        _ => {
            // Assert(false)
        }
    }

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo((*data).context);

    /* Switch relation if publishing via root. */
    if (*relentry).publish_as_relid != RelationGetRelid(relation) {
        // Assert(relation->rd_rel->relispartition);
        ancestor = RelationIdGetRelation((*relentry).publish_as_relid);
        targetrel = ancestor;
    }

    if !(*change).data.tp.oldtuple.is_null() {
        old_slot = (*relentry).old_slot;
        ExecStoreHeapTuple((*change).data.tp.oldtuple as *mut c_void, old_slot, false);

        /* Convert tuple if needed. */
        if !(*relentry).attrmap.is_null() {
            let slot = MakeTupleTableSlot(
                RelationGetDescr(targetrel),
                &TTSOpsVirtual as *const TupleTableSlotOps,
            );

            old_slot = execute_attr_map_slot((*relentry).attrmap, old_slot, slot);
        }
    }

    if !(*change).data.tp.newtuple.is_null() {
        new_slot = (*relentry).new_slot;
        ExecStoreHeapTuple((*change).data.tp.newtuple as *mut c_void, new_slot, false);

        /* Convert tuple if needed. */
        if !(*relentry).attrmap.is_null() {
            let slot = MakeTupleTableSlot(
                RelationGetDescr(targetrel),
                &TTSOpsVirtual as *const TupleTableSlotOps,
            );

            new_slot = execute_attr_map_slot((*relentry).attrmap, new_slot, slot);
        }
    }

    /*
     * Check row filter.
     *
     * Updates could be transformed to inserts or deletes based on the results
     * of the row filter for old and new tuple.
     */
    if !pgoutput_row_filter(targetrel, old_slot, &mut new_slot, relentry, &mut action) {
        // goto cleanup
        cleanup_pgoutput_change(ancestor, relentry, old_slot, new_slot, old, data);
        return;
    }

    /*
     * Send BEGIN if we haven't yet.
     *
     * We send the BEGIN message after ensuring that we will actually send the
     * change. This avoids sending a pair of BEGIN/COMMIT messages for empty
     * transactions.
     */
    if !txndata.is_null() && !(*txndata).sent_begin_txn {
        pgoutput_send_begin(ctx, txn);
    }

    /*
     * Schema should be sent using the original relation because it also sends
     * the ancestor's relation.
     */
    maybe_send_schema(ctx, change, relation, relentry);

    OutputPluginPrepareWrite(ctx, true);

    /* Send the data */
    match action {
        REORDER_BUFFER_CHANGE_INSERT => {
            logicalrep_write_insert(
                (*ctx).out,
                xid,
                targetrel as *mut c_void,
                new_slot as *mut c_void,
                (*data).binary,
                (*relentry).columns,
                (*relentry).include_gencols_type as c_int,
            );
        }
        REORDER_BUFFER_CHANGE_UPDATE => {
            logicalrep_write_update(
                (*ctx).out,
                xid,
                targetrel as *mut c_void,
                old_slot as *mut c_void,
                new_slot as *mut c_void,
                (*data).binary,
                (*relentry).columns,
                (*relentry).include_gencols_type as c_int,
            );
        }
        REORDER_BUFFER_CHANGE_DELETE => {
            logicalrep_write_delete(
                (*ctx).out,
                xid,
                targetrel as *mut c_void,
                old_slot as *mut c_void,
                (*data).binary,
                (*relentry).columns,
                (*relentry).include_gencols_type as c_int,
            );
        }
        _ => {
            // Assert(false)
        }
    }

    OutputPluginWrite(ctx, true);

    // cleanup:
    cleanup_pgoutput_change(ancestor, relentry, old_slot, new_slot, old, data);
}

/// Shared cleanup logic for pgoutput_change (corresponds to 'cleanup:' label in C).
#[inline]
unsafe fn cleanup_pgoutput_change(
    ancestor: crate::utils::rel::Relation,
    relentry: *const RelationSyncEntry,
    old_slot: *mut TupleTableSlot,
    new_slot: *mut TupleTableSlot,
    old: MemoryContext,
    data: *mut PGOutputData,
) {
    if RelationIsValid(ancestor) {
        RelationClose(ancestor);
    }

    /* Drop the new slots that were used to store the converted tuples. */
    if !(*relentry).attrmap.is_null() {
        if !old_slot.is_null() {
            ExecDropSingleTupleTableSlot(old_slot);
        }

        if !new_slot.is_null() {
            ExecDropSingleTupleTableSlot(new_slot);
        }
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset((*data).context);
}

// ---------------------------------------------------------------------------
// pgoutput_truncate
// ---------------------------------------------------------------------------

unsafe extern "C" fn pgoutput_truncate(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    nrelations: c_int,
    relations: *mut crate::utils::rel::Relation,
    change: *mut ReorderBufferChange,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let txndata = (*txn).output_plugin_private as *mut PGOutputTxnData;
    let old: MemoryContext;
    let mut relentry: *mut RelationSyncEntry;
    let mut nrelids: c_int;
    let relids: *mut Oid;
    let mut xid: TransactionId = InvalidTransactionId;

    /* Remember the xid for the change in streaming mode. See pgoutput_change. */
    if (*data).in_streaming {
        xid = (*(*change).txn).xid;
    }

    old = MemoryContextSwitchTo((*data).context);

    relids = palloc0(nrelations as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    nrelids = 0;

    for i in 0..nrelations {
        let relation = *relations.add(i as usize);
        let relid = RelationGetRelid(relation);

        if !is_publishable_relation(relation) {
            continue;
        }

        relentry = get_rel_sync_entry(data, relation);

        if !(*relentry).pubactions.pubtruncate {
            continue;
        }

        /*
         * Don't send partitions if the publication wants to send only the
         * root tables through it.
         */
        if (*(*relation).rd_rel).relispartition
            && (*relentry).publish_as_relid != relid
        {
            continue;
        }

        *relids.add(nrelids as usize) = relid;
        nrelids += 1;

        /* Send BEGIN if we haven't yet */
        if !txndata.is_null() && !(*txndata).sent_begin_txn {
            pgoutput_send_begin(ctx, txn);
        }

        maybe_send_schema(ctx, change, relation, relentry);
    }

    if nrelids > 0 {
        OutputPluginPrepareWrite(ctx, true);
        logicalrep_write_truncate(
            (*ctx).out,
            xid,
            nrelids,
            relids,
            (*change).data.truncate.cascade,
            (*change).data.truncate.restart_seqs,
        );
        OutputPluginWrite(ctx, true);
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset((*data).context);
}

// ---------------------------------------------------------------------------
// pgoutput_message
// ---------------------------------------------------------------------------

unsafe extern "C" fn pgoutput_message(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    sz: Size,
    message: *const c_char,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let mut xid: TransactionId = InvalidTransactionId;

    if !(*data).messages {
        return;
    }

    /*
     * Remember the xid for the message in streaming mode. See
     * pgoutput_change.
     */
    if (*data).in_streaming {
        xid = (*txn).xid;
    }

    /*
     * Output BEGIN if we haven't yet. Avoid for non-transactional messages.
     */
    if transactional {
        let txndata = (*txn).output_plugin_private as *mut PGOutputTxnData;

        /* Send BEGIN if we haven't yet */
        if !txndata.is_null() && !(*txndata).sent_begin_txn {
            pgoutput_send_begin(ctx, txn);
        }
    }

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_message((*ctx).out, xid, message_lsn, transactional, prefix, sz, message);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// pgoutput_origin_filter
// ---------------------------------------------------------------------------

/*
 * Return true if the data is associated with an origin and the user has
 * requested the changes that don't have an origin, false otherwise.
 */
unsafe extern "C" fn pgoutput_origin_filter(
    ctx: *mut LogicalDecodingContext,
    origin_id: RepOriginId,
) -> bool {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;

    if (*data).publish_no_origin && origin_id != InvalidRepOriginId {
        return true;
    }

    false
}

// ---------------------------------------------------------------------------
// pgoutput_shutdown
// ---------------------------------------------------------------------------

/*
 * Shutdown the output plugin.
 *
 * Note, we don't need to clean the data->context, data->cachectx, and
 * data->pubctx as they are child contexts of the ctx->context so they
 * will be cleaned up by logical decoding machinery.
 */
unsafe extern "C" fn pgoutput_shutdown(ctx: *mut LogicalDecodingContext) {
    pgoutput_memory_context_reset(core::ptr::null_mut());
}

// ---------------------------------------------------------------------------
// LoadPublications
// ---------------------------------------------------------------------------

/*
 * Load publications from the list of publication names.
 *
 * Here, we skip the publications that don't exist yet. This will allow us
 * to silently continue the replication in the absence of a missing publication.
 * This is required because we allow the users to create publications after they
 * have specified the required publications at the time of replication start.
 */
unsafe fn LoadPublications(pubnames: *mut List) -> *mut List {
    let mut result: *mut List = core::ptr::null_mut(); // NIL

    // foreach(lc, pubnames)
    // TODO(pg-port): real list iteration
    let mut cell = if pubnames.is_null() {
        core::ptr::null_mut()
    } else {
        (*(*pubnames).elements).ptr_value
    };
    while !cell.is_null() {
        let pubname = cell as *mut c_char;
        let pub_ = GetPublicationByName(pubname, true);

        if !pub_.is_null() {
            result = lappend(result, pub_ as *mut c_void);
        } else {
            ereport!(WARNING,
                errmsg!("skipped loading publication because it does not exist at this point in the WAL"));
        }

        cell = core::ptr::null_mut(); // TODO(pg-port): advance list cell
    }

    result
}

// ---------------------------------------------------------------------------
// publication_invalidation_cb
// ---------------------------------------------------------------------------

/*
 * Publication syscache invalidation callback.
 *
 * Called for invalidations on pg_publication.
 */
unsafe fn publication_invalidation_cb(_arg: Datum, _cacheid: c_int, _hashvalue: u32) {
    publications_valid = false;
}

// ---------------------------------------------------------------------------
// pgoutput_stream_start
// ---------------------------------------------------------------------------

/*
 * START STREAM callback
 */
unsafe extern "C" fn pgoutput_stream_start(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let mut send_replication_origin = (*txn).origin_id != InvalidRepOriginId;

    /* we can't nest streaming of transactions */
    // Assert(!data->in_streaming);

    /*
     * If we already sent the first stream for this transaction then don't
     * send the origin id in the subsequent streams.
     */
    if rbtxn_is_streamed(txn) {
        send_replication_origin = false;
    }

    OutputPluginPrepareWrite(ctx, !send_replication_origin);
    logicalrep_write_stream_start((*ctx).out, (*txn).xid, !rbtxn_is_streamed(txn));

    send_repl_origin(ctx, (*txn).origin_id, InvalidXLogRecPtr, send_replication_origin);

    OutputPluginWrite(ctx, true);

    /* we're streaming a chunk of transaction now */
    (*data).in_streaming = true;
}

// ---------------------------------------------------------------------------
// pgoutput_stream_stop
// ---------------------------------------------------------------------------

/*
 * STOP STREAM callback
 */
unsafe extern "C" fn pgoutput_stream_stop(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;

    /* we should be streaming a transaction */
    // Assert(data->in_streaming);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_stream_stop((*ctx).out);
    OutputPluginWrite(ctx, true);

    /* we've stopped streaming a transaction */
    (*data).in_streaming = false;
}

// ---------------------------------------------------------------------------
// pgoutput_stream_abort
// ---------------------------------------------------------------------------

/*
 * Notify downstream to discard the streamed transaction (along with all
 * its subtransactions, if it's a toplevel transaction).
 */
unsafe extern "C" fn pgoutput_stream_abort(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    abort_lsn: XLogRecPtr,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;
    let write_abort_info = (*data).streaming == LOGICALREP_STREAM_PARALLEL;

    /*
     * The abort should happen outside streaming block, even for streamed
     * transactions. The transaction has to be marked as streamed, though.
     */
    // Assert(!data->in_streaming);

    /* determine the toplevel transaction */
    let toptxn = rbtxn_get_toptxn(txn);

    // Assert(rbtxn_is_streamed(toptxn));

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_stream_abort(
        (*ctx).out,
        (*toptxn).xid,
        (*txn).xid,
        abort_lsn,
        (*txn).xact_time.abort_time,
        write_abort_info,
    );

    OutputPluginWrite(ctx, true);

    cleanup_rel_sync_cache((*toptxn).xid, false);
}

// ---------------------------------------------------------------------------
// pgoutput_stream_commit
// ---------------------------------------------------------------------------

/*
 * Notify downstream to apply the streamed transaction (along with all
 * its subtransactions).
 */
unsafe extern "C" fn pgoutput_stream_commit(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
) {
    let data = (*ctx).output_plugin_private as *mut PGOutputData;

    /*
     * The commit should happen outside streaming block, even for streamed
     * transactions. The transaction has to be marked as streamed, though.
     */
    // Assert(!data->in_streaming);
    // Assert(rbtxn_is_streamed(txn));

    OutputPluginUpdateProgress(ctx, false);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_stream_commit((*ctx).out, txn as *mut c_void, commit_lsn);
    OutputPluginWrite(ctx, true);

    cleanup_rel_sync_cache((*txn).xid, true);
}

// ---------------------------------------------------------------------------
// pgoutput_stream_prepare_txn
// ---------------------------------------------------------------------------

/*
 * PREPARE callback (for streaming two-phase commit).
 *
 * Notify the downstream to prepare the transaction.
 */
unsafe extern "C" fn pgoutput_stream_prepare_txn(
    ctx: *mut LogicalDecodingContext,
    txn: *mut ReorderBufferTXN,
    prepare_lsn: XLogRecPtr,
) {
    // Assert(rbtxn_is_streamed(txn));

    OutputPluginUpdateProgress(ctx, false);
    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_stream_prepare((*ctx).out, txn as *mut c_void, prepare_lsn);
    OutputPluginWrite(ctx, true);
}

// ---------------------------------------------------------------------------
// init_rel_sync_cache
// ---------------------------------------------------------------------------

/*
 * Initialize the relation schema sync cache for a decoding session.
 *
 * The hash table is destroyed at the end of a decoding session. While
 * relcache invalidations still exist and will still be invoked, they
 * will just see the null hash table global and take no action.
 */
unsafe fn init_rel_sync_cache(cachectx: MemoryContext) {
    static mut relation_callbacks_registered: bool = false;

    /* Nothing to do if hash table already exists */
    if !RelationSyncCache.is_null() {
        return;
    }

    /* Make a new hash table for the cache */
    let mut ctl: HASHCTL = core::mem::zeroed();
    ctl.keysize = core::mem::size_of::<Oid>();
    ctl.entrysize = core::mem::size_of::<RelationSyncEntry>();
    ctl.hcxt = cachectx;

    RelationSyncCache = hash_create(
        b"logical replication output relation cache\0".as_ptr() as *const c_char,
        128,
        &mut ctl,
        HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
    );

    // Assert(RelationSyncCache != NULL);

    /* No more to do if we already registered callbacks */
    if relation_callbacks_registered {
        return;
    }

    /* We must update the cache entry for a relation after a relcache flush */
    CacheRegisterRelcacheCallback(rel_sync_cache_relation_cb, 0 as Datum);

    /*
     * Flush all cache entries after a pg_namespace change, in case it was a
     * schema rename affecting a relation being replicated.
     *
     * XXX: It is not a good idea to invalidate all the relation entries in
     * RelationSyncCache on schema rename. We can optimize it to invalidate
     * only the required relations by either having a specific invalidation
     * message containing impacted relations or by having schema information
     * in each RelationSyncCache entry and using hashvalue of pg_namespace.oid
     * passed to the callback.
     */
    CacheRegisterSyscacheCallback(
        NAMESPACEOID,
        rel_sync_cache_publication_cb,
        0 as Datum,
    );

    relation_callbacks_registered = true;
}

// ---------------------------------------------------------------------------
// get_schema_sent_in_streamed_txn
// ---------------------------------------------------------------------------

/*
 * We expect relatively small number of streamed transactions.
 */
unsafe fn get_schema_sent_in_streamed_txn(
    entry: *mut RelationSyncEntry,
    xid: TransactionId,
) -> bool {
    list_member_xid((*entry).streamed_txns, xid)
}

// ---------------------------------------------------------------------------
// set_schema_sent_in_streamed_txn
// ---------------------------------------------------------------------------

/*
 * Add the xid in the rel sync entry for which we have already sent the schema
 * of the relation.
 */
unsafe fn set_schema_sent_in_streamed_txn(
    entry: *mut RelationSyncEntry,
    xid: TransactionId,
) {
    let oldctx = MemoryContextSwitchTo(CacheMemoryContext as *mut std::ffi::c_void as MemoryContext);

    (*entry).streamed_txns = lappend_xid((*entry).streamed_txns, xid);

    MemoryContextSwitchTo(oldctx);
}

// ---------------------------------------------------------------------------
// get_rel_sync_entry
// ---------------------------------------------------------------------------

/*
 * Find or create entry in the relation schema cache.
 *
 * This looks up publications that the given relation is directly or
 * indirectly part of (the latter if it's really the relation's ancestor that
 * is part of a publication) and fills up the found entry with the information
 * about which operations to publish and whether to use an ancestor's schema
 * when publishing.
 */
unsafe fn get_rel_sync_entry(
    data: *mut PGOutputData,
    relation: crate::utils::rel::Relation,
) -> *mut RelationSyncEntry {
    let mut found: bool = false;
    let relid = RelationGetRelid(relation);

    // Assert(RelationSyncCache != NULL);

    /* Find cached relation info, creating if not found */
    let entry = hash_search(
        RelationSyncCache,
        &relid as *const Oid as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut RelationSyncEntry;
    // Assert(entry != NULL);

    /* initialize entry, if it's new */
    if !found {
        (*entry).replicate_valid = false;
        (*entry).schema_sent = false;
        (*entry).include_gencols_type = PUBLISH_GENCOLS_NONE;
        (*entry).streamed_txns = core::ptr::null_mut(); // NIL
        (*entry).pubactions.pubinsert = false;
        (*entry).pubactions.pubupdate = false;
        (*entry).pubactions.pubdelete = false;
        (*entry).pubactions.pubtruncate = false;
        (*entry).new_slot = core::ptr::null_mut();
        (*entry).old_slot = core::ptr::null_mut();
        core::ptr::write_bytes((*entry).exprstate.as_mut_ptr(), 0, NUM_ROWFILTER_PUBACTIONS);
        (*entry).entry_cxt = core::ptr::null_mut();
        (*entry).publish_as_relid = InvalidOid;
        (*entry).columns = core::ptr::null_mut();
        (*entry).attrmap = core::ptr::null_mut();
    }

    /* Validate the entry */
    if !(*entry).replicate_valid {
        let schemaid = get_rel_namespace(relid);
        let pubids = GetRelationPublications(relid);

        /*
         * We don't acquire a lock on the namespace system table as we build
         * the cache entry using a historic snapshot and all the later changes
         * are absorbed while decoding WAL.
         */
        let schemapubids = GetSchemaPublications(schemaid);
        let mut publish_as_relid = relid;
        let mut publish_ancestor_level: c_int = 0;
        let am_partition = get_rel_relispartition(relid);
        let relkind = get_rel_relkind(relid);
        let mut rel_publications: *mut List = core::ptr::null_mut(); // NIL

        /* Reload publications if needed before use. */
        if !publications_valid {
            MemoryContextReset((*data).pubctx);

            let oldctx = MemoryContextSwitchTo((*data).pubctx);
            (*data).publications = LoadPublications((*data).publication_names);
            MemoryContextSwitchTo(oldctx);
            publications_valid = true;
        }

        /*
         * Reset schema_sent status as the relation definition may have
         * changed.  Also reset pubactions to empty in case rel was dropped
         * from a publication.  Also free any objects that depended on the
         * earlier definition.
         */
        (*entry).schema_sent = false;
        (*entry).include_gencols_type = PUBLISH_GENCOLS_NONE;
        list_free((*entry).streamed_txns);
        (*entry).streamed_txns = core::ptr::null_mut();
        bms_free((*entry).columns);
        (*entry).columns = core::ptr::null_mut();
        (*entry).pubactions.pubinsert = false;
        (*entry).pubactions.pubupdate = false;
        (*entry).pubactions.pubdelete = false;
        (*entry).pubactions.pubtruncate = false;

        /*
         * Tuple slots cleanups. (Will be rebuilt later if needed).
         */
        if !(*entry).old_slot.is_null() {
            let desc = (*(*entry).old_slot).tts_tupleDescriptor;

            // Assert(desc->tdrefcount == -1);

            ExecDropSingleTupleTableSlot((*entry).old_slot);

            /*
             * ExecDropSingleTupleTableSlot() would not free the TupleDesc, so
             * do it now to avoid any leaks.
             */
            FreeTupleDesc(desc);
        }
        if !(*entry).new_slot.is_null() {
            let desc = (*(*entry).new_slot).tts_tupleDescriptor;

            // Assert(desc->tdrefcount == -1);

            ExecDropSingleTupleTableSlot((*entry).new_slot);

            /*
             * ExecDropSingleTupleTableSlot() would not free the TupleDesc, so
             * do it now to avoid any leaks.
             */
            FreeTupleDesc(desc);
        }

        (*entry).old_slot = core::ptr::null_mut();
        (*entry).new_slot = core::ptr::null_mut();

        if !(*entry).attrmap.is_null() {
            free_attrmap((*entry).attrmap);
        }
        (*entry).attrmap = core::ptr::null_mut();

        /*
         * Row filter cache cleanups.
         */
        if !(*entry).entry_cxt.is_null() {
            MemoryContextDelete((*entry).entry_cxt);
        }

        (*entry).entry_cxt = core::ptr::null_mut();
        (*entry).estate = core::ptr::null_mut();
        core::ptr::write_bytes((*entry).exprstate.as_mut_ptr(), 0, NUM_ROWFILTER_PUBACTIONS);

        /*
         * Build publication cache. We can't use one provided by relcache as
         * relcache considers all publications that the given relation is in,
         * but here we only need to consider ones that the subscriber
         * requested.
         */
        // foreach(lc, data->publications)
        // TODO(pg-port): real list iteration
        let mut cell = if (*data).publications.is_null() {
            core::ptr::null_mut()
        } else {
            (*(*(*data).publications).elements).ptr_value
        };
        while !cell.is_null() {
            let pub_ = cell as *mut Publication;
            let mut publish = false;

            /*
             * Under what relid should we publish changes in this publication?
             * We'll use the top-most relid across all publications. Also
             * track the ancestor level for this publication.
             */
            let mut pub_relid = relid;
            let mut ancestor_level: c_int = 0;

            /*
             * If this is a FOR ALL TABLES publication, pick the partition
             * root and set the ancestor level accordingly.
             */
            if (*pub_).alltables {
                publish = true;
                if (*pub_).pubviaroot && am_partition {
                    let ancestors = get_partition_ancestors(relid);

                    pub_relid = llast_oid(ancestors);
                    ancestor_level = list_length(ancestors);
                }
            }

            if !publish {
                let mut ancestor_published = false;

                /*
                 * For a partition, check if any of the ancestors are
                 * published.  If so, note down the topmost ancestor that is
                 * published via this publication, which will be used as the
                 * relation via which to publish the partition's changes.
                 */
                if am_partition {
                    let mut level: c_int = 0;
                    let ancestors = get_partition_ancestors(relid);

                    let ancestor_oid = GetTopMostAncestorInPublication(
                        (*pub_).oid,
                        ancestors,
                        &mut level,
                    );

                    if ancestor_oid != InvalidOid {
                        ancestor_published = true;
                        if (*pub_).pubviaroot {
                            pub_relid = ancestor_oid;
                            ancestor_level = level;
                        }
                    }
                }

                if list_member_oid(pubids, (*pub_).oid)
                    || list_member_oid(schemapubids, (*pub_).oid)
                    || ancestor_published
                {
                    publish = true;
                }
            }

            /*
             * If the relation is to be published, determine actions to
             * publish, and list of columns, if appropriate.
             *
             * Don't publish changes for partitioned tables, because
             * publishing those of its partitions suffices, unless partition
             * changes won't be published due to pubviaroot being set.
             */
            if publish
                && (relkind != RELKIND_PARTITIONED_TABLE || (*pub_).pubviaroot)
            {
                (*entry).pubactions.pubinsert |= (*pub_).pubactions.pubinsert;
                (*entry).pubactions.pubupdate |= (*pub_).pubactions.pubupdate;
                (*entry).pubactions.pubdelete |= (*pub_).pubactions.pubdelete;
                (*entry).pubactions.pubtruncate |= (*pub_).pubactions.pubtruncate;

                /*
                 * We want to publish the changes as the top-most ancestor
                 * across all publications. So we need to check if the already
                 * calculated level is higher than the new one. If yes, we can
                 * ignore the new value (as it's a child). Otherwise the new
                 * value is an ancestor, so we keep it.
                 */
                if publish_ancestor_level > ancestor_level {
                    cell = core::ptr::null_mut(); // TODO(pg-port): continue
                    continue;
                }

                /*
                 * If we found an ancestor higher up in the tree, discard the
                 * list of publications through which we replicate it, and use
                 * the new ancestor.
                 */
                if publish_ancestor_level < ancestor_level {
                    publish_as_relid = pub_relid;
                    publish_ancestor_level = ancestor_level;

                    /* reset the publication list for this relation */
                    rel_publications = core::ptr::null_mut(); // NIL
                } else {
                    /* Same ancestor level, has to be the same OID. */
                    // Assert(publish_as_relid == pub_relid);
                }

                /* Track publications for this ancestor. */
                rel_publications = lappend(rel_publications, pub_ as *mut c_void);
            }

            cell = core::ptr::null_mut(); // TODO(pg-port): advance list cell
        }

        (*entry).publish_as_relid = publish_as_relid;

        /*
         * Initialize the tuple slot, map, and row filter. These are only used
         * when publishing inserts, updates, or deletes.
         */
        if (*entry).pubactions.pubinsert
            || (*entry).pubactions.pubupdate
            || (*entry).pubactions.pubdelete
        {
            /* Initialize the tuple slot and map */
            init_tuple_slot(data, relation, entry);

            /* Initialize the row filter */
            pgoutput_row_filter_init(data, rel_publications, entry);

            /* Check whether to publish generated columns. */
            check_and_init_gencol(data, rel_publications, entry);

            /* Initialize the column list */
            pgoutput_column_list_init(data, rel_publications, entry);
        }

        list_free(pubids);
        list_free(schemapubids);
        list_free(rel_publications);

        (*entry).replicate_valid = true;
    }

    entry
}

// ---------------------------------------------------------------------------
// cleanup_rel_sync_cache
// ---------------------------------------------------------------------------

/*
 * Cleanup list of streamed transactions and update the schema_sent flag.
 *
 * When a streamed transaction commits or aborts, we need to remove the
 * toplevel XID from the schema cache. If the transaction aborted, the
 * subscriber will simply throw away the schema records we streamed, so
 * we don't need to do anything else.
 *
 * If the transaction is committed, the subscriber will update the relation
 * cache - so tweak the schema_sent flag accordingly.
 */
unsafe fn cleanup_rel_sync_cache(xid: TransactionId, is_commit: bool) {
    let mut hash_seq: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut entry: *mut RelationSyncEntry;

    // Assert(RelationSyncCache != NULL);

    hash_seq_init(&mut hash_seq, RelationSyncCache);
    loop {
        entry = hash_seq_search(&mut hash_seq) as *mut RelationSyncEntry;
        if entry.is_null() {
            break;
        }

        /*
         * We can set the schema_sent flag for an entry that has committed xid
         * in the list as that ensures that the subscriber would have the
         * corresponding schema and we don't need to send it unless there is
         * any invalidation for that relation.
         */
        // foreach_xid(streamed_txn, entry->streamed_txns)
        // TODO(pg-port): real list iteration for xid lists
        let mut txn_cell = if (*entry).streamed_txns.is_null() {
            core::ptr::null_mut()
        } else {
            (*(*(*entry).streamed_txns).elements).ptr_value
        };
        while !txn_cell.is_null() {
            let streamed_txn = txn_cell as *mut TransactionId;
            if xid == *streamed_txn {
                if is_commit {
                    (*entry).schema_sent = true;
                }

                // entry->streamed_txns = foreach_delete_current(entry->streamed_txns, ...)
                // TODO(pg-port): use real list deletion; for now just clear
                (*entry).streamed_txns = core::ptr::null_mut();
                break;
            }
            txn_cell = core::ptr::null_mut(); // TODO(pg-port): advance
        }
    }
}

// ---------------------------------------------------------------------------
// rel_sync_cache_relation_cb
// ---------------------------------------------------------------------------

/*
 * Relcache invalidation callback
 */
unsafe fn rel_sync_cache_relation_cb(_arg: Datum, relid: Oid) {
    let mut entry: *mut RelationSyncEntry;

    /*
     * We can get here if the plugin was used in SQL interface as the
     * RelationSyncCache is destroyed when the decoding finishes, but there is
     * no way to unregister the relcache invalidation callback.
     */
    if RelationSyncCache.is_null() {
        return;
    }

    /*
     * Nobody keeps pointers to entries in this hash table around outside
     * logical decoding callback calls - but invalidation events can come in
     * *during* a callback if we do any syscache access in the callback.
     * Because of that we must mark the cache entry as invalid but not damage
     * any of its substructure here.  The next get_rel_sync_entry() call will
     * rebuild it all.
     */
    if relid != InvalidOid {
        /*
         * Getting invalidations for relations that aren't in the table is
         * entirely normal.  So we don't care if it's found or not.
         */
        entry = hash_search(
            RelationSyncCache,
            &relid as *const Oid as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut RelationSyncEntry;
        if !entry.is_null() {
            (*entry).replicate_valid = false;
        }
    } else {
        /* Whole cache must be flushed. */
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        hash_seq_init(&mut status, RelationSyncCache);
        loop {
            entry = hash_seq_search(&mut status) as *mut RelationSyncEntry;
            if entry.is_null() {
                break;
            }
            (*entry).replicate_valid = false;
        }
    }
}

// ---------------------------------------------------------------------------
// rel_sync_cache_publication_cb
// ---------------------------------------------------------------------------

/*
 * Publication relation/schema map syscache invalidation callback
 *
 * Called for invalidations on pg_namespace.
 */
unsafe fn rel_sync_cache_publication_cb(_arg: Datum, _cacheid: c_int, _hashvalue: u32) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut entry: *mut RelationSyncEntry;

    /*
     * We can get here if the plugin was used in SQL interface as the
     * RelationSyncCache is destroyed when the decoding finishes, but there is
     * no way to unregister the invalidation callbacks.
     */
    if RelationSyncCache.is_null() {
        return;
    }

    /*
     * We have no easy way to identify which cache entries this invalidation
     * event might have affected, so just mark them all invalid.
     */
    hash_seq_init(&mut status, RelationSyncCache);
    loop {
        entry = hash_seq_search(&mut status) as *mut RelationSyncEntry;
        if entry.is_null() {
            break;
        }
        (*entry).replicate_valid = false;
    }
}

// ---------------------------------------------------------------------------
// send_repl_origin
// ---------------------------------------------------------------------------

/* Send Replication origin */
unsafe fn send_repl_origin(
    ctx: *mut LogicalDecodingContext,
    origin_id: RepOriginId,
    origin_lsn: XLogRecPtr,
    send_origin: bool,
) {
    if send_origin {
        let mut origin: *mut c_char = core::ptr::null_mut();

        /*----------
         * XXX: which behaviour do we want here?
         *
         * Alternatives:
         *  - don't send origin message if origin name not found
         *    (that's what we do now)
         *  - throw error - that will break replication, not good
         *  - send some special "unknown" origin
         *----------
         */
        if replorigin_by_oid(origin_id, true, &mut origin) {
            /* Message boundary */
            OutputPluginWrite(ctx, false);
            OutputPluginPrepareWrite(ctx, true);

            logicalrep_write_origin((*ctx).out, origin, origin_lsn);
        }
    }
}

// ---------------------------------------------------------------------------
// Stubs for missing type field access -- TODO(pg-port): wire once rel.rs is complete
// ---------------------------------------------------------------------------

// TextDatumGetCString stub -- TODO(pg-port): utils/adt/varlena.h
mod varlena_stub {
    use crate::postgres::Datum;
    use core::ffi::c_char;
    #[inline]
    pub unsafe fn TextDatumGetCString(_datum: Datum) -> *mut c_char {
        core::ptr::null_mut() // TODO(pg-port)
    }
}

// Re-export for use above in pgoutput_row_filter_init
mod crate_utils_adt_varlena {
    pub use super::varlena_stub::TextDatumGetCString;
}

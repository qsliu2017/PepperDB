/*-------------------------------------------------------------------------
 *
 * spi.c
 *              Server Programming Interface
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/spi.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unused_unsafe)]

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_long, c_void};
use core::mem::{size_of, zeroed};
use core::ptr::{null, null_mut};

/* access/htup_details.h */
use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderData,
};
/* access/sysattr.h */
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
/* access/transam/xact.h */
use crate::access::transam::xact::{
    SavedTransactionCharacteristics, SaveTransactionCharacteristics,
    RestoreTransactionCharacteristics, CommitTransactionCommand,
    StartTransactionCommand, AbortCurrentTransaction, IsSubTransaction,
    GetCurrentSubTransactionId,
};
/* access/common/tupdesc.h */
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
/* nodes/pg_list.h */
use crate::nodes::pg_list::{List, ListCell, NIL, lfirst, lappend, list_length, linitial};
/* catalog/pg_type_d.h */
use crate::catalog::pg_type_d::RECORDOID;
/* nodes/nodes.h */
use crate::nodes::nodes::CmdType::CMD_UTILITY;
/* nodes/params.h */
use crate::nodes::params::{ParamListInfo, ParamListInfoData, ParamExternData, PARAM_FLAG_CONST};
/* nodes/plannodes.h (PlannedStmt) */
use crate::nodes::plannodes::PlannedStmt;
/* parser/parser.h */
use crate::parser::parser::{
    RawParseMode, RAW_PARSE_DEFAULT,
    RAW_PARSE_PLPGSQL_EXPR, RAW_PARSE_PLPGSQL_ASSIGN1,
    RAW_PARSE_PLPGSQL_ASSIGN2, RAW_PARSE_PLPGSQL_ASSIGN3,
};
/* utils/palloc.h (MemoryContext is in prelude) */
/* MemoryContextDelete / MemoryContextReset come in via crate::prelude::* (memutils.h) */
/* utils/misc/memutils.h (AllocSetContextCreate is in prelude) */
/* utils/snapshot.h */
use crate::utils::snapshot::{Snapshot, InvalidSnapshot};
/* utils/portal.h */
use crate::utils::portal::{
    Portal, PortalData, PortalIsValid, CreatePortal, CreateNewPortal,
    PortalDrop, GetPortalByName, PortalDefineQuery, PORTAL_MULTI_QUERY,
};
/* tcop/dest.h */
use crate::tcop::dest::{
    DestReceiver, CommandDest, CommandDest::DestNone, CommandDest::DestSPI,
    CreateDestReceiver, None_Receiver,
};
/* tcop/cmdtag.h */
use crate::tcop::cmdtag::{QueryCompletion, InitializeQueryCompletion};
use crate::tcop::cmdtag::CommandTag;
use crate::tcop::cmdtag::CommandTag::{CMDTAG_SELECT, CMDTAG_COPY};
/* commands/trigger.h */
use crate::commands::trigger::TriggerData;
/* commands/prepare.h (CachedPlanSource, CachedPlan) */
use crate::commands::prepare::{CachedPlanSource, CachedPlan};
/* executor/spi_priv.h */
use crate::executor::spi_priv::{
    _SPI_connection, _SPI_plan, _SPI_PLAN_MAGIC, RawParseMode as SpiRawParseMode,
    SPITupleTable as SpiPrivTupleTable,
};
/* executor/execdesc.h */
use crate::executor::execdesc::QueryDesc;
/* utils/rel.h */
use crate::utils::rel::{Relation, RelationData};
/* lib/ilist.h */
use crate::lib::ilist::{
    slist_head, slist_node, slist_mutable_iter,
    slist_init, slist_push_head, slist_delete_current,
};
/* nodes/parsenodes.h */
use crate::nodes::parsenodes::{
    FetchDirection, FETCH_FORWARD, FETCH_BACKWARD,
    CopyStmt, TransactionStmt, CreateTableAsStmt,
    CURSOR_OPT_SCROLL, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_PARALLEL_OK,
};
/* utils/misc/queryenvironment.h */
use crate::utils::misc::queryenvironment::{
    QueryEnvironment, EphemeralNamedRelation, EphemeralNamedRelationData,
    ENR_NAMED_TUPLESTORE,
};
/* access/sdir.h */
use crate::access::sdir::ForwardScanDirection;

/* macros from crate root */
use crate::{foreach, current_cell, lfirst_node, linitial_node, IsA};

/*
 * These global variables are part of the API for various SPI functions
 * (a horrible API choice, but it's too late now).  To reduce the risk of
 * interference between different SPI callers, we save and restore them
 * when entering/exiting a SPI nesting level.
 */
pub static mut SPI_processed: u64 = 0;
pub static mut SPI_tuptable: *mut SPITupleTable = null_mut();
pub static mut SPI_result: c_int = 0;

static mut _SPI_stack: *mut _SPI_connection = null_mut();
static mut _SPI_current: *mut _SPI_connection = null_mut();
static mut _SPI_stack_depth: c_int = 0; /* allocated size of _SPI_stack */
static mut _SPI_connected: c_int = -1; /* current stack index */

struct SPICallbackArg {
    query: *const c_char,
    mode: RawParseMode,
}

/* ----------------------------------------------------------------
 * Public types (from spi.h, translated here as the canonical home)
 * ---------------------------------------------------------------- */

/// SPITupleTable - result tuple set returned by SPI_execute et al.
#[repr(C)]
pub struct SPITupleTable {
    /* Public members */
    /// tuple descriptor
    pub tupdesc: TupleDesc,
    /// array of tuples
    pub vals: *mut HeapTuple,
    /// number of valid tuples
    pub numvals: u64,

    /* Private members, not intended for external callers */
    /// allocated length of vals array
    pub alloced: u64,
    /// memory context of result table
    pub tuptabcxt: MemoryContext,
    /// link for internal bookkeeping
    pub next: slist_node,
    /// subxact in which tuptable was created
    pub subid: SubTransactionId,
}

/// Optional arguments for SPI_prepare_extended
#[repr(C)]
pub struct SPIPrepareOptions {
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
    pub parseMode: RawParseMode,
    pub cursorOptions: c_int,
}

/// Optional arguments for SPI_execute[_plan]_extended
#[repr(C)]
pub struct SPIExecuteOptions {
    pub params: ParamListInfo,
    pub read_only: bool,
    pub allow_nonatomic: bool,
    pub must_return_tuples: bool,
    pub tcount: u64,
    pub dest: *mut DestReceiver,
    pub owner: ResourceOwner,
}

/// Optional arguments for SPI_cursor_parse_open
#[repr(C)]
pub struct SPIParseOpenOptions {
    pub params: ParamListInfo,
    pub cursorOptions: c_int,
    pub read_only: bool,
}

/// Plans are opaque structs for standard users of SPI
pub type SPIPlanPtr = *mut _SPI_plan;

/* SPI_ERROR_* */
pub const SPI_ERROR_CONNECT: c_int = -1;
pub const SPI_ERROR_COPY: c_int = -2;
pub const SPI_ERROR_OPUNKNOWN: c_int = -3;
pub const SPI_ERROR_UNCONNECTED: c_int = -4;
/* SPI_ERROR_CURSOR = -5 -- not used anymore */
pub const SPI_ERROR_ARGUMENT: c_int = -6;
pub const SPI_ERROR_PARAM: c_int = -7;
pub const SPI_ERROR_TRANSACTION: c_int = -8;
pub const SPI_ERROR_NOATTRIBUTE: c_int = -9;
pub const SPI_ERROR_NOOUTFUNC: c_int = -10;
pub const SPI_ERROR_TYPUNKNOWN: c_int = -11;
pub const SPI_ERROR_REL_DUPLICATE: c_int = -12;
pub const SPI_ERROR_REL_NOT_FOUND: c_int = -13;

/* SPI_OK_* */
pub const SPI_OK_CONNECT: c_int = 1;
pub const SPI_OK_FINISH: c_int = 2;
pub const SPI_OK_FETCH: c_int = 3;
pub const SPI_OK_UTILITY: c_int = 4;
pub const SPI_OK_SELECT: c_int = 5;
pub const SPI_OK_SELINTO: c_int = 6;
pub const SPI_OK_INSERT: c_int = 7;
pub const SPI_OK_DELETE: c_int = 8;
pub const SPI_OK_UPDATE: c_int = 9;
pub const SPI_OK_CURSOR: c_int = 10;
pub const SPI_OK_INSERT_RETURNING: c_int = 11;
pub const SPI_OK_DELETE_RETURNING: c_int = 12;
pub const SPI_OK_UPDATE_RETURNING: c_int = 13;
pub const SPI_OK_REWRITTEN: c_int = 14;
pub const SPI_OK_REL_REGISTER: c_int = 15;
pub const SPI_OK_REL_UNREGISTER: c_int = 16;
pub const SPI_OK_TD_REGISTER: c_int = 17;
pub const SPI_OK_MERGE: c_int = 18;
pub const SPI_OK_MERGE_RETURNING: c_int = 19;

pub const SPI_OPT_NONATOMIC: c_int = 1 << 0;

/* ---- stubs for not-yet-translated dependencies ---- */

/// TODO(pg-port): ResourceOwner lives in utils/resowner.h
pub type ResourceOwner = *mut c_void;

/// TODO(pg-port): ErrorContextCallback lives in utils/elog.h
#[repr(C)]
pub struct ErrorContextCallback {
    pub callback: unsafe fn(*mut c_void),
    pub arg: *mut c_void,
    pub previous: *mut ErrorContextCallback,
}

/// TODO(pg-port): error_context_stack lives in utils/elog.c (global)
pub static mut error_context_stack: *mut ErrorContextCallback = null_mut();

/// TODO(pg-port): ErrorData lives in utils/elog.h
pub type ErrorData = c_void;

/* ParserSetupHook lives in nodes/params.h; use the real definition so it
 * matches _SPI_plan.parserSetup and the params-based hooks. */
use crate::nodes::params::ParserSetupHook;

/// TODO(pg-port): CurTransactionContext / CurrentResourceOwner globals
pub static mut CurrentResourceOwner: ResourceOwner = null_mut();

/* RawStmt lives in nodes/parsenodes.h */
use crate::nodes::parsenodes::RawStmt;

/* ---- stub functions for not-yet-translated subsystems ---- */

unsafe fn repalloc_huge(pointer: *mut c_void, size: Size) -> *mut c_void {
    /* TODO(pg-port): real repalloc_huge lives in utils/mmgr/mcxt.c */
    repalloc(pointer, size)
}

unsafe fn heap_copytuple(_tuple: HeapTuple) -> HeapTuple {
    /* TODO(pg-port): access/common/heaptuple.c */
    unimplemented!("heap_copytuple")
}

unsafe fn heap_freetuple(_tuple: HeapTuple) {
    /* TODO(pg-port): access/common/heaptuple.c */
}

unsafe fn heap_deform_tuple(
    _tuple: HeapTuple,
    _tupleDesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    /* TODO(pg-port): access/common/heaptuple.c */
    unimplemented!("heap_deform_tuple")
}

unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    /* TODO(pg-port): access/common/heaptuple.c */
    unimplemented!("heap_form_tuple")
}

unsafe fn heap_getattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupleDesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    /* TODO(pg-port): access/htup_details.h inline */
    unimplemented!("heap_getattr")
}

unsafe fn heap_copy_tuple_as_datum(_tuple: HeapTuple, _tupdesc: TupleDesc) -> Datum {
    /* TODO(pg-port): access/common/heaptuple.c */
    unimplemented!("heap_copy_tuple_as_datum")
}

unsafe fn DatumGetHeapTupleHeader(_datum: Datum) -> HeapTupleHeader {
    /* TODO(pg-port): access/htup_details.h */
    unimplemented!("DatumGetHeapTupleHeader")
}

unsafe fn assign_record_type_typmod(_tupdesc: TupleDesc) {
    /* TODO(pg-port): utils/cache/typcache.c */
    unimplemented!("assign_record_type_typmod")
}

unsafe fn namestrcmp(_name: *const c_void, _str: *const c_char) -> c_int {
    /* TODO(pg-port): utils/adt/name.c */
    unimplemented!("namestrcmp")
}

unsafe fn SystemAttributeByName(_attname: *const c_char) -> *const c_void {
    /* TODO(pg-port): access/common/sysattr.c */
    null()
}

unsafe fn SystemAttributeDefinition(_attnum: c_int) -> *const c_void {
    /* TODO(pg-port): access/common/sysattr.c */
    null()
}

unsafe fn NameStr(_name: *const c_void) -> *const c_char {
    /* TODO(pg-port): c.h macro -> real NameData.data */
    unimplemented!("NameStr")
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    /* TODO(pg-port): utils/cache/syscache.c */
    unimplemented!("SearchSysCache1")
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    /* TODO(pg-port): utils/cache/syscache.c */
}

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    /* TODO(pg-port): access/htup.h macro */
    !tuple.is_null()
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    /* TODO(pg-port): access/htup.h macro */
    unimplemented!("GETSTRUCT")
}

unsafe fn ObjectIdGetDatum(_objectId: Oid) -> Datum {
    /* TODO(pg-port): postgres.h macro */
    _objectId as Datum
}

unsafe fn getTypeOutputInfo(
    _type_oid: Oid,
    _typOutput: *mut Oid,
    _typIsVarlena: *mut bool,
) {
    /* TODO(pg-port): utils/cache/lsyscache.c */
    unimplemented!("getTypeOutputInfo")
}

unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char {
    /* TODO(pg-port): utils/fmgr.c */
    unimplemented!("OidOutputFunctionCall")
}

unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    /* TODO(pg-port): utils/cache/lsyscache.c */
    unimplemented!("get_namespace_name")
}

unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    /* TODO(pg-port): utils/rel.h macro */
    unimplemented!("RelationGetRelationName")
}

unsafe fn RelationGetNamespace(_rel: Relation) -> Oid {
    /* TODO(pg-port): utils/rel.h macro */
    unimplemented!("RelationGetNamespace")
}

unsafe fn datumTransfer(_value: Datum, _typByVal: bool, _typLen: c_int) -> Datum {
    /* TODO(pg-port): utils/adt/datum.c */
    unimplemented!("datumTransfer")
}

unsafe fn ExecCopySlotHeapTuple(_slot: *mut c_void) -> HeapTuple {
    /* TODO(pg-port): executor/execTuples.c */
    unimplemented!("ExecCopySlotHeapTuple")
}

unsafe fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    /* TODO(pg-port): access/common/tupdesc.c */
    unimplemented!("CreateTupleDescCopy")
}

unsafe fn MemoryContextStrdup(_context: MemoryContext, _s: *const c_char) -> *mut c_char {
    /* TODO(pg-port): utils/mmgr/mcxt.c */
    unimplemented!("MemoryContextStrdup")
}

unsafe fn MemoryContextSetParent(_context: MemoryContext, _new_parent: MemoryContext) {
    /* TODO(pg-port): utils/mmgr/mcxt.c */
    unimplemented!("MemoryContextSetParent")
}

unsafe fn CacheMemoryContext() -> MemoryContext {
    /* TODO(pg-port): utils/mmgr/mcxt.c global */
    null_mut()
}
static mut _CacheMemoryContext: MemoryContext = null_mut();

unsafe fn TopTransactionContext() -> MemoryContext {
    /* TODO(pg-port): access/transam/xact.c global */
    null_mut()
}
static mut _TopTransactionContext: MemoryContext = null_mut();

unsafe fn PortalContext() -> MemoryContext {
    /* TODO(pg-port): utils/mmgr/portalmem.c global */
    null_mut()
}

unsafe fn raw_parser(_str: *const c_char, _mode: RawParseMode) -> *mut List {
    /* TODO(pg-port): parser/parser.c */
    unimplemented!("raw_parser")
}

unsafe fn pg_analyze_and_rewrite_withcb(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    /* TODO(pg-port): tcop/analyze.c */
    unimplemented!("pg_analyze_and_rewrite_withcb")
}

unsafe fn pg_analyze_and_rewrite_fixedparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *mut Oid,
    _numParams: c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    /* TODO(pg-port): tcop/analyze.c */
    unimplemented!("pg_analyze_and_rewrite_fixedparams")
}

unsafe fn CreateCachedPlan(
    _raw_parse_tree: *mut RawStmt,
    _query_string: *const c_char,
    _commandTag: CommandTag,
) -> *mut CachedPlanSource {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CreateCachedPlan")
}

unsafe fn CreateOneShotCachedPlan(
    _raw_parse_tree: *mut RawStmt,
    _query_string: *const c_char,
    _commandTag: CommandTag,
) -> *mut CachedPlanSource {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CreateOneShotCachedPlan")
}

unsafe fn CompleteCachedPlan(
    _plansource: *mut CachedPlanSource,
    _querytree_list: *mut List,
    _queryEnv: *mut QueryEnvironment,
    _param_types: *mut Oid,
    _num_params: c_int,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _cursor_options: c_int,
    _fixed_result: bool,
) {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CompleteCachedPlan")
}

unsafe fn GetCachedPlan(
    _plansource: *mut CachedPlanSource,
    _boundParams: ParamListInfo,
    _owner: ResourceOwner,
    _queryEnv: *mut QueryEnvironment,
) -> *mut CachedPlan {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("GetCachedPlan")
}

unsafe fn ReleaseCachedPlan(_cplan: *mut CachedPlan, _owner: ResourceOwner) {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("ReleaseCachedPlan")
}

unsafe fn SaveCachedPlan(_plansource: *mut CachedPlanSource) {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("SaveCachedPlan")
}

unsafe fn DropCachedPlan(_plansource: *mut CachedPlanSource) {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("DropCachedPlan")
}

unsafe fn CopyCachedPlan(_plansource: *mut CachedPlanSource) -> *mut CachedPlanSource {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CopyCachedPlan")
}

unsafe fn CachedPlanIsValid(_plansource: *mut CachedPlanSource) -> bool {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CachedPlanIsValid")
}

unsafe fn CachedPlanSetParentContext(
    _plansource: *mut CachedPlanSource,
    _newcxt: MemoryContext,
) {
    /* TODO(pg-port): utils/cache/plancache.c */
    unimplemented!("CachedPlanSetParentContext")
}

unsafe fn CreateCommandTag(_parsetree: *mut c_void) -> CommandTag {
    /* TODO(pg-port): tcop/cmdtag.c */
    CommandTag::CMDTAG_UNKNOWN
}

unsafe fn GetCommandTagName(_commandTag: CommandTag) -> *const c_char {
    /* TODO(pg-port): tcop/cmdtag.c */
    crate::tcop::cmdtag::GetCommandTagName(_commandTag)
}

/* TODO(pg-port): commands/prepare.rs stubs CachedPlanSource.commandTag as
 * `*mut CommandTag` (an opaque pointer) rather than the real `CommandTag`
 * value.  Reinterpret it here so SPI can compare/name it as in C; drop this
 * once plancache.c lands and the field becomes a plain CommandTag. */
unsafe fn plansource_commandtag(plansource: *mut CachedPlanSource) -> CommandTag {
    core::mem::transmute::<c_int, CommandTag>((*plansource).commandTag as usize as c_int)
}

unsafe fn copyObject(_from: *mut c_void) -> *mut c_void {
    /* TODO(pg-port): nodes/copyfuncs.c */
    unimplemented!("copyObject")
}

unsafe fn copyParamList(_from: ParamListInfo) -> ParamListInfo {
    /* TODO(pg-port): nodes/params.c */
    unimplemented!("copyParamList")
}

unsafe fn makeParamList(_numParams: c_int) -> ParamListInfo {
    /* TODO(pg-port): nodes/params.c */
    unimplemented!("makeParamList")
}

unsafe fn PlannedStmtRequiresSnapshot(_pstmt: *mut PlannedStmt) -> bool {
    /* TODO(pg-port): executor/execMain.c */
    unimplemented!("PlannedStmtRequiresSnapshot")
}

unsafe fn CommandIsReadOnly(_pstmt: *mut PlannedStmt) -> bool {
    /* TODO(pg-port): tcop/utility.c */
    unimplemented!("CommandIsReadOnly")
}

unsafe fn CreateCommandName(_node: *mut c_void) -> *const c_char {
    /* TODO(pg-port): tcop/cmdtag.c */
    unimplemented!("CreateCommandName")
}

unsafe fn ExecSupportsBackwardScan(_node: *mut c_void) -> bool {
    /* TODO(pg-port): executor/execAmi.c */
    unimplemented!("ExecSupportsBackwardScan")
}

unsafe fn ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) {
    /* TODO(pg-port): executor/execMain.c */
    unimplemented!("ExecutorStart")
}

unsafe fn ExecutorRun(_queryDesc: *mut QueryDesc, _direction: i32, _count: u64) {
    /* TODO(pg-port): executor/execMain.c */
    unimplemented!("ExecutorRun")
}

unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) {
    /* TODO(pg-port): executor/execMain.c */
    unimplemented!("ExecutorFinish")
}

unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) {
    /* TODO(pg-port): executor/execMain.c */
    unimplemented!("ExecutorEnd")
}

unsafe fn CreateQueryDesc(
    _plannedstmt: *mut PlannedStmt,
    _sourceText: *const c_char,
    _snapshot: Snapshot,
    _crosscheck_snapshot: Snapshot,
    _dest: *mut DestReceiver,
    _params: ParamListInfo,
    _queryEnv: *mut QueryEnvironment,
    _instrument_options: c_int,
) -> *mut QueryDesc {
    /* TODO(pg-port): executor/execdesc.c */
    unimplemented!("CreateQueryDesc")
}

unsafe fn FreeQueryDesc(_qdesc: *mut QueryDesc) {
    /* TODO(pg-port): executor/execdesc.c */
    unimplemented!("FreeQueryDesc")
}

unsafe fn ProcessUtility(
    _pstmt: *mut PlannedStmt,
    _queryString: *const c_char,
    _readOnlyTree: bool,
    _context: ProcessUtilityContext,
    _params: ParamListInfo,
    _queryEnv: *mut QueryEnvironment,
    _dest: *mut DestReceiver,
    _qc: *mut QueryCompletion,
) {
    /* TODO(pg-port): tcop/utility.c */
    unimplemented!("ProcessUtility")
}

/// TODO(pg-port): ProcessUtilityContext lives in tcop/utility.h
pub type ProcessUtilityContext = c_int;
pub const PROCESS_UTILITY_QUERY: ProcessUtilityContext = 0;
pub const PROCESS_UTILITY_QUERY_NONATOMIC: ProcessUtilityContext = 1;

unsafe fn PushActiveSnapshot(_snap: Snapshot) {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("PushActiveSnapshot")
}

unsafe fn PushCopiedSnapshot(_snap: Snapshot) {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("PushCopiedSnapshot")
}

unsafe fn PopActiveSnapshot() {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("PopActiveSnapshot")
}

unsafe fn GetActiveSnapshot() -> Snapshot {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("GetActiveSnapshot")
}

unsafe fn GetTransactionSnapshot() -> Snapshot {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("GetTransactionSnapshot")
}

unsafe fn ActiveSnapshotSet() -> bool {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("ActiveSnapshotSet")
}

unsafe fn EnsurePortalSnapshotExists() {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("EnsurePortalSnapshotExists")
}

unsafe fn UpdateActiveSnapshotCommandId() {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("UpdateActiveSnapshotCommandId")
}

unsafe fn CommandCounterIncrement() {
    /* TODO(pg-port): access/transam/xact.c */
    unimplemented!("CommandCounterIncrement")
}

unsafe fn HoldPinnedPortals() {
    /* TODO(pg-port): utils/mmgr/portalmem.c */
    unimplemented!("HoldPinnedPortals")
}

unsafe fn ForgetPortalSnapshots() {
    /* TODO(pg-port): utils/snapmgr.c */
    unimplemented!("ForgetPortalSnapshots")
}

unsafe fn PortalStart(
    _portal: Portal,
    _params: ParamListInfo,
    _eflags: c_int,
    _snapshot: Snapshot,
) {
    /* TODO(pg-port): utils/mmgr/portalmem.c */
    unimplemented!("PortalStart")
}

unsafe fn PortalRunFetch(
    _portal: Portal,
    _fdirection: FetchDirection,
    _count: c_long,
    _dest: *mut DestReceiver,
) -> u64 {
    /* TODO(pg-port): utils/mmgr/portalmem.c */
    unimplemented!("PortalRunFetch")
}

unsafe fn CopyErrorData() -> *mut ErrorData {
    /* TODO(pg-port): utils/elog.c */
    unimplemented!("CopyErrorData")
}

unsafe fn FlushErrorState() {
    /* TODO(pg-port): utils/elog.c */
    unimplemented!("FlushErrorState")
}

unsafe fn ReThrowError(_edata: *mut ErrorData) {
    /* TODO(pg-port): utils/elog.c */
    unimplemented!("ReThrowError")
}

unsafe fn geterrposition() -> c_int {
    /* TODO(pg-port): utils/elog.c */
    0
}

unsafe fn errposition(_cursorpos: c_int) -> c_int {
    /* TODO(pg-port): utils/elog.c */
    0
}

unsafe fn internalerrposition(_cursorpos: c_int) -> c_int {
    /* TODO(pg-port): utils/elog.c */
    0
}

unsafe fn internalerrquery(_query: *const c_char) -> c_int {
    /* TODO(pg-port): utils/elog.c */
    0
}

unsafe fn errcontext_msg(_fmt: *const c_char) -> c_int {
    /* TODO(pg-port): utils/elog.c -- errcontext is a macro wrapping this */
    0
}

unsafe fn create_queryEnv() -> *mut QueryEnvironment {
    /* TODO(pg-port): utils/misc/queryenvironment.c */
    unimplemented!("create_queryEnv")
}

unsafe fn get_ENR(_env: *mut QueryEnvironment, _name: *const c_char) -> EphemeralNamedRelation {
    /* TODO(pg-port): utils/misc/queryenvironment.c */
    unimplemented!("get_ENR")
}

unsafe fn register_ENR(_env: *mut QueryEnvironment, _rel: EphemeralNamedRelation) {
    /* TODO(pg-port): utils/misc/queryenvironment.c */
    unimplemented!("register_ENR")
}

unsafe fn unregister_ENR(_env: *mut QueryEnvironment, _name: *const c_char) {
    /* TODO(pg-port): utils/misc/queryenvironment.c */
    unimplemented!("unregister_ENR")
}

unsafe fn tuplestore_tuple_count(_state: *mut c_void) -> i64 {
    /* TODO(pg-port): utils/sort/tuplestore.c */
    0
}

unsafe fn TYPEOID() -> c_int { 1 } /* TODO(pg-port): utils/cache/syscache.h */

unsafe fn EXEC_FLAG_SKIP_TRIGGERS() -> c_int { 0x0004 } /* TODO(pg-port): executor/executor.h */

/* =================== interface functions =================== */

pub unsafe fn SPI_connect() -> c_int {
    SPI_connect_ext(0)
}

pub unsafe fn SPI_connect_ext(options: c_int) -> c_int {
    let mut newdepth: c_int;

    /* Enlarge stack if necessary */
    if _SPI_stack.is_null() {
        if _SPI_connected != -1 || _SPI_stack_depth != 0 {
            elog!(ERROR, "SPI stack corrupted");
        }
        newdepth = 16;
        _SPI_stack = MemoryContextAlloc(
            TopMemoryContext,
            (newdepth as usize) * size_of::<_SPI_connection>(),
        ) as *mut _SPI_connection;
        _SPI_stack_depth = newdepth;
    } else {
        if _SPI_stack_depth <= 0 || _SPI_stack_depth <= _SPI_connected {
            elog!(ERROR, "SPI stack corrupted");
        }
        if _SPI_stack_depth == _SPI_connected + 1 {
            newdepth = _SPI_stack_depth * 2;
            _SPI_stack = repalloc(
                _SPI_stack as *mut c_void,
                (newdepth as usize) * size_of::<_SPI_connection>(),
            ) as *mut _SPI_connection;
            _SPI_stack_depth = newdepth;
        }
    }

    /* Enter new stack level */
    _SPI_connected += 1;
    Assert!(_SPI_connected >= 0 && _SPI_connected < _SPI_stack_depth);

    _SPI_current = &mut *_SPI_stack.add(_SPI_connected as usize);
    (*_SPI_current).processed = 0;
    (*_SPI_current).tuptable = null_mut();
    (*_SPI_current).execSubid = InvalidSubTransactionId;
    slist_init(&mut (*_SPI_current).tuptables);
    (*_SPI_current).procCxt = null_mut(); /* in case we fail to create 'em */
    (*_SPI_current).execCxt = null_mut();
    (*_SPI_current).connectSubid = GetCurrentSubTransactionId();
    (*_SPI_current).queryEnv = null_mut();
    (*_SPI_current).atomic = if options & SPI_OPT_NONATOMIC != 0 { false } else { true };
    (*_SPI_current).internal_xact = false;
    (*_SPI_current).outer_processed = SPI_processed;
    (*_SPI_current).outer_tuptable = SPI_tuptable as *mut SpiPrivTupleTable;
    (*_SPI_current).outer_result = SPI_result;

    /*
     * Create memory contexts for this procedure
     *
     * In atomic contexts (the normal case), we use TopTransactionContext,
     * otherwise PortalContext, so that it lives across transaction
     * boundaries.
     *
     * XXX It could be better to use PortalContext as the parent context in
     * all cases, but we may not be inside a portal (consider deferred-trigger
     * execution).  Perhaps CurTransactionContext could be an option?  For now
     * it doesn't matter because we clean up explicitly in AtEOSubXact_SPI();
     * but see also AtEOXact_SPI().
     */
    (*_SPI_current).procCxt = AllocSetContextCreate!(
        if (*_SPI_current).atomic {
            _TopTransactionContext
        } else {
            PortalContext()
        },
        c"SPI Proc".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    (*_SPI_current).execCxt = AllocSetContextCreate!(
        if (*_SPI_current).atomic {
            _TopTransactionContext
        } else {
            (*_SPI_current).procCxt
        },
        c"SPI Exec".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    /* ... and switch to procedure's context */
    (*_SPI_current).savedcxt = MemoryContextSwitchTo((*_SPI_current).procCxt);

    /*
     * Reset API global variables so that current caller cannot accidentally
     * depend on state of an outer caller.
     */
    SPI_processed = 0;
    SPI_tuptable = null_mut();
    SPI_result = 0;

    SPI_OK_CONNECT
}

pub unsafe fn SPI_finish() -> c_int {
    let mut res: c_int;

    res = _SPI_begin_call(false); /* just check we're connected */
    if res < 0 {
        return res;
    }

    /* Restore memory context as it was before procedure call */
    MemoryContextSwitchTo((*_SPI_current).savedcxt);

    /* Release memory used in procedure call (including tuptables) */
    MemoryContextDelete((*_SPI_current).execCxt);
    (*_SPI_current).execCxt = null_mut();
    MemoryContextDelete((*_SPI_current).procCxt);
    (*_SPI_current).procCxt = null_mut();

    /*
     * Restore outer API variables, especially SPI_tuptable which is probably
     * pointing at a just-deleted tuptable
     */
    SPI_processed = (*_SPI_current).outer_processed;
    SPI_tuptable = (*_SPI_current).outer_tuptable as *mut SPITupleTable;
    SPI_result = (*_SPI_current).outer_result;

    /* Exit stack level */
    _SPI_connected -= 1;
    if _SPI_connected < 0 {
        _SPI_current = null_mut();
    } else {
        _SPI_current = &mut *_SPI_stack.add(_SPI_connected as usize);
    }

    SPI_OK_FINISH
}

/*
 * SPI_start_transaction is a no-op, kept for backwards compatibility.
 * SPI callers are *always* inside a transaction.
 */
pub unsafe fn SPI_start_transaction() {
}

unsafe fn _SPI_commit(chain: bool) {
    let oldcontext: MemoryContext = CurrentMemoryContext;
    let mut savetc: SavedTransactionCharacteristics = zeroed();

    /*
     * Complain if we are in a context that doesn't permit transaction
     * termination.  (Note: here and _SPI_rollback should be the only places
     * that throw ERRCODE_INVALID_TRANSACTION_TERMINATION, so that callers can
     * test for that with security that they know what happened.)
     */
    if (*_SPI_current).atomic {
        ereport!(ERROR, errmsg!("invalid transaction termination") /* C also: errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION) */);
    }

    /*
     * This restriction is required by PLs implemented on top of SPI.  They
     * use subtransactions to establish exception blocks that are supposed to
     * be rolled back together if there is an error.  Terminating the
     * top-level transaction in such a block violates that idea.  A future PL
     * implementation might have different ideas about this, in which case
     * this restriction would have to be refined or the check possibly be
     * moved out of SPI into the PLs.  Note however that the code below relies
     * on not being within a subtransaction.
     */
    if IsSubTransaction() {
        ereport!(ERROR, errmsg!("cannot commit while a subtransaction is active") /* C also: errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION) */);
    }

    if chain {
        SaveTransactionCharacteristics(&mut savetc);
    }

    /* Catch any error occurring during the COMMIT */
    /* PG_TRY / PG_CATCH -- translated as a closure-based stub */
    {
        /* Protect current SPI stack entry against deletion */
        (*_SPI_current).internal_xact = true;

        /*
         * Hold any pinned portals that any PLs might be using.  We have to do
         * this before changing transaction state, since this will run
         * user-defined code that might throw an error.
         */
        HoldPinnedPortals();

        /* Release snapshots associated with portals */
        ForgetPortalSnapshots();

        /* Do the deed */
        CommitTransactionCommand();

        /* Immediately start a new transaction */
        StartTransactionCommand();
        if chain {
            RestoreTransactionCharacteristics(&savetc);
        }

        MemoryContextSwitchTo(oldcontext);

        (*_SPI_current).internal_xact = false;
    }
    /* NOTE: PG_CATCH block omitted - TODO(pg-port): wire up setjmp/longjmp error handling */
}

pub unsafe fn SPI_commit() {
    _SPI_commit(false);
}

pub unsafe fn SPI_commit_and_chain() {
    _SPI_commit(true);
}

unsafe fn _SPI_rollback(chain: bool) {
    let oldcontext: MemoryContext = CurrentMemoryContext;
    let mut savetc: SavedTransactionCharacteristics = zeroed();

    /* see comments in _SPI_commit() */
    if (*_SPI_current).atomic {
        ereport!(ERROR, errmsg!("invalid transaction termination") /* C also: errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION) */);
    }

    /* see comments in _SPI_commit() */
    if IsSubTransaction() {
        ereport!(ERROR, errmsg!("cannot roll back while a subtransaction is active") /* C also: errcode(ERRCODE_INVALID_TRANSACTION_TERMINATION) */);
    }

    if chain {
        SaveTransactionCharacteristics(&mut savetc);
    }

    /* Catch any error occurring during the ROLLBACK */
    {
        /* Protect current SPI stack entry against deletion */
        (*_SPI_current).internal_xact = true;

        /*
         * Hold any pinned portals that any PLs might be using.  We have to do
         * this before changing transaction state, since this will run
         * user-defined code that might throw an error, and in any case
         * couldn't be run in an already-aborted transaction.
         */
        HoldPinnedPortals();

        /* Release snapshots associated with portals */
        ForgetPortalSnapshots();

        /* Do the deed */
        AbortCurrentTransaction();

        /* Immediately start a new transaction */
        StartTransactionCommand();
        if chain {
            RestoreTransactionCharacteristics(&savetc);
        }

        MemoryContextSwitchTo(oldcontext);

        (*_SPI_current).internal_xact = false;
    }
    /* NOTE: PG_CATCH block omitted - TODO(pg-port): wire up setjmp/longjmp error handling */
}

pub unsafe fn SPI_rollback() {
    _SPI_rollback(false);
}

pub unsafe fn SPI_rollback_and_chain() {
    _SPI_rollback(true);
}

/*
 * Clean up SPI state at transaction commit or abort.
 */
pub unsafe fn AtEOXact_SPI(isCommit: bool) {
    let mut found: bool = false;

    /*
     * Pop stack entries, stopping if we find one marked internal_xact (that
     * one belongs to the caller of SPI_commit or SPI_rollback).
     */
    while _SPI_connected >= 0 {
        let connection: *mut _SPI_connection = &mut *_SPI_stack.add(_SPI_connected as usize);

        if (*connection).internal_xact {
            break;
        }

        found = true;

        /*
         * We need not release the procedure's memory contexts explicitly, as
         * they'll go away automatically when their parent context does; see
         * notes in SPI_connect_ext.
         */

        /*
         * Restore outer global variables and pop the stack entry.  Unlike
         * SPI_finish(), we don't risk switching to memory contexts that might
         * be already gone.
         */
        SPI_processed = (*connection).outer_processed;
        SPI_tuptable = (*connection).outer_tuptable as *mut SPITupleTable;
        SPI_result = (*connection).outer_result;

        _SPI_connected -= 1;
        if _SPI_connected < 0 {
            _SPI_current = null_mut();
        } else {
            _SPI_current = &mut *_SPI_stack.add(_SPI_connected as usize);
        }
    }

    /* We should only find entries to pop during an ABORT. */
    if found && isCommit {
        ereport!(WARNING, errmsg!("transaction left non-empty SPI stack") /* C also: errcode(ERRCODE_WARNING); errhint!("Check for missing \"SPI_finish\" calls.") */);
    }
}

/*
 * Clean up SPI state at subtransaction commit or abort.
 *
 * During commit, there shouldn't be any unclosed entries remaining from
 * the current subtransaction; we emit a warning if any are found.
 */
pub unsafe fn AtEOSubXact_SPI(isCommit: bool, mySubid: SubTransactionId) {
    let mut found: bool = false;

    while _SPI_connected >= 0 {
        let connection: *mut _SPI_connection = &mut *_SPI_stack.add(_SPI_connected as usize);

        if (*connection).connectSubid != mySubid {
            break; /* couldn't be any underneath it either */
        }

        if (*connection).internal_xact {
            break;
        }

        found = true;

        /*
         * Release procedure memory explicitly (see note in SPI_connect)
         */
        if !(*connection).execCxt.is_null() {
            MemoryContextDelete((*connection).execCxt);
            (*connection).execCxt = null_mut();
        }
        if !(*connection).procCxt.is_null() {
            MemoryContextDelete((*connection).procCxt);
            (*connection).procCxt = null_mut();
        }

        /*
         * Restore outer global variables and pop the stack entry.  Unlike
         * SPI_finish(), we don't risk switching to memory contexts that might
         * be already gone.
         */
        SPI_processed = (*connection).outer_processed;
        SPI_tuptable = (*connection).outer_tuptable as *mut SPITupleTable;
        SPI_result = (*connection).outer_result;

        _SPI_connected -= 1;
        if _SPI_connected < 0 {
            _SPI_current = null_mut();
        } else {
            _SPI_current = &mut *_SPI_stack.add(_SPI_connected as usize);
        }
    }

    if found && isCommit {
        ereport!(WARNING, errmsg!("subtransaction left non-empty SPI stack") /* C also: errcode(ERRCODE_WARNING); errhint!("Check for missing \"SPI_finish\" calls.") */);
    }

    /*
     * If we are aborting a subtransaction and there is an open SPI context
     * surrounding the subxact, clean up to prevent memory leakage.
     */
    if !_SPI_current.is_null() && !isCommit {
        let mut siter: slist_mutable_iter = zeroed();

        /*
         * Throw away executor state if current executor operation was started
         * within current subxact (essentially, force a _SPI_end_call(true)).
         */
        if (*_SPI_current).execSubid >= mySubid {
            (*_SPI_current).execSubid = InvalidSubTransactionId;
            MemoryContextReset((*_SPI_current).execCxt);
        }

        /* throw away any tuple tables created within current subxact */
        /* TODO(pg-port): slist_foreach_modify not yet wired; skip for now */
        /*
        slist_foreach_modify!(siter, &mut (*_SPI_current).tuptables, {
            let tuptable: *mut SPITupleTable = slist_container!(SPITupleTable, next, siter.cur);
            if (*tuptable).subid >= mySubid {
                slist_delete_current(&mut siter);
                if tuptable == (*_SPI_current).tuptable as *mut SPITupleTable {
                    (*_SPI_current).tuptable = null_mut();
                }
                if tuptable == SPI_tuptable {
                    SPI_tuptable = null_mut();
                }
                MemoryContextDelete((*tuptable).tuptabcxt);
            }
        });
        */
    }
}

/*
 * Are we executing inside a procedure (that is, a nonatomic SPI context)?
 */
pub unsafe fn SPI_inside_nonatomic_context() -> bool {
    if _SPI_current.is_null() {
        return false; /* not in any SPI context at all */
    }
    /* these tests must match _SPI_commit's opinion of what's atomic: */
    if (*_SPI_current).atomic {
        return false; /* it's atomic (ie function not procedure) */
    }
    if IsSubTransaction() {
        return false; /* if within subtransaction, it's atomic */
    }
    true
}


/* Parse, plan, and execute a query string */
pub unsafe fn SPI_execute(src: *const c_char, read_only: bool, tcount: c_long) -> c_int {
    let mut plan: _SPI_plan = zeroed();
    let mut options: SPIExecuteOptions = zeroed();
    let mut res: c_int;

    if src.is_null() || tcount < 0 {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = CURSOR_OPT_PARALLEL_OK;

    _SPI_prepare_oneshot_plan(src, &mut plan);

    options.read_only = read_only;
    options.tcount = tcount as u64;

    res = _SPI_execute_plan(&mut plan, &options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

/* Obsolete version of SPI_execute */
pub unsafe fn SPI_exec(src: *const c_char, tcount: c_long) -> c_int {
    SPI_execute(src, false, tcount)
}

/* Parse, plan, and execute a query string, with extensible options */
pub unsafe fn SPI_execute_extended(
    src: *const c_char,
    options: *const SPIExecuteOptions,
) -> c_int {
    let mut res: c_int;
    let mut plan: _SPI_plan = zeroed();

    if src.is_null() || options.is_null() {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = CURSOR_OPT_PARALLEL_OK;
    if !(*options).params.is_null() {
        plan.parserSetup = (*(*options).params).parserSetup;
        plan.parserSetupArg = (*(*options).params).parserSetupArg;
    }

    _SPI_prepare_oneshot_plan(src, &mut plan);

    res = _SPI_execute_plan(&mut plan, &*options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

/* Execute a previously prepared plan */
pub unsafe fn SPI_execute_plan(
    plan: SPIPlanPtr,
    Values: *mut Datum,
    Nulls: *const c_char,
    read_only: bool,
    tcount: c_long,
) -> c_int {
    let mut options: SPIExecuteOptions = zeroed();
    let mut res: c_int;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC || tcount < 0 {
        return SPI_ERROR_ARGUMENT;
    }

    if (*plan).nargs > 0 && Values.is_null() {
        return SPI_ERROR_PARAM;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    options.params = _SPI_convert_params((*plan).nargs, (*plan).argtypes,
                                         Values, Nulls);
    options.read_only = read_only;
    options.tcount = tcount as u64;

    res = _SPI_execute_plan(plan, &options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

/* Obsolete version of SPI_execute_plan */
pub unsafe fn SPI_execp(
    plan: SPIPlanPtr,
    Values: *mut Datum,
    Nulls: *const c_char,
    tcount: c_long,
) -> c_int {
    SPI_execute_plan(plan, Values, Nulls, false, tcount)
}

/* Execute a previously prepared plan */
pub unsafe fn SPI_execute_plan_extended(
    plan: SPIPlanPtr,
    options: *const SPIExecuteOptions,
) -> c_int {
    let mut res: c_int;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC || options.is_null() {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    res = _SPI_execute_plan(plan, &*options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

/* Execute a previously prepared plan */
pub unsafe fn SPI_execute_plan_with_paramlist(
    plan: SPIPlanPtr,
    params: ParamListInfo,
    read_only: bool,
    tcount: c_long,
) -> c_int {
    let mut options: SPIExecuteOptions = zeroed();
    let mut res: c_int;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC || tcount < 0 {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    options.params = params;
    options.read_only = read_only;
    options.tcount = tcount as u64;

    res = _SPI_execute_plan(plan, &options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

/*
 * SPI_execute_snapshot -- identical to SPI_execute_plan, except that we allow
 * the caller to specify exactly which snapshots to use, which will be
 * registered here.  Also, the caller may specify that AFTER triggers should be
 * queued as part of the outer query rather than being fired immediately at the
 * end of the command.
 *
 * This is currently not documented in spi.sgml because it is only intended
 * for use by RI triggers.
 *
 * Passing snapshot == InvalidSnapshot will select the normal behavior of
 * fetching a new snapshot for each query.
 */
pub unsafe fn SPI_execute_snapshot(
    plan: SPIPlanPtr,
    Values: *mut Datum,
    Nulls: *const c_char,
    snapshot: Snapshot,
    crosscheck_snapshot: Snapshot,
    read_only: bool,
    fire_triggers: bool,
    tcount: c_long,
) -> c_int {
    let mut options: SPIExecuteOptions = zeroed();
    let mut res: c_int;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC || tcount < 0 {
        return SPI_ERROR_ARGUMENT;
    }

    if (*plan).nargs > 0 && Values.is_null() {
        return SPI_ERROR_PARAM;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    options.params = _SPI_convert_params((*plan).nargs, (*plan).argtypes,
                                         Values, Nulls);
    options.read_only = read_only;
    options.tcount = tcount as u64;

    res = _SPI_execute_plan(plan, &options,
                            snapshot, crosscheck_snapshot,
                            fire_triggers);

    _SPI_end_call(true);
    res
}

/*
 * SPI_execute_with_args -- plan and execute a query with supplied arguments
 *
 * This is functionally equivalent to SPI_prepare followed by
 * SPI_execute_plan.
 */
pub unsafe fn SPI_execute_with_args(
    src: *const c_char,
    nargs: c_int,
    argtypes: *mut Oid,
    Values: *mut Datum,
    Nulls: *const c_char,
    read_only: bool,
    tcount: c_long,
) -> c_int {
    let mut res: c_int;
    let mut plan: _SPI_plan = zeroed();
    let mut paramLI: ParamListInfo;
    let mut options: SPIExecuteOptions = zeroed();

    if src.is_null() || nargs < 0 || tcount < 0 {
        return SPI_ERROR_ARGUMENT;
    }

    if nargs > 0 && (argtypes.is_null() || Values.is_null()) {
        return SPI_ERROR_PARAM;
    }

    res = _SPI_begin_call(true);
    if res < 0 {
        return res;
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = CURSOR_OPT_PARALLEL_OK;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = None;
    plan.parserSetupArg = null_mut();

    paramLI = _SPI_convert_params(nargs, argtypes, Values, Nulls);

    _SPI_prepare_oneshot_plan(src, &mut plan);

    options.params = paramLI;
    options.read_only = read_only;
    options.tcount = tcount as u64;

    res = _SPI_execute_plan(&mut plan, &options,
                            InvalidSnapshot, InvalidSnapshot,
                            true);

    _SPI_end_call(true);
    res
}

pub unsafe fn SPI_prepare(src: *const c_char, nargs: c_int, argtypes: *mut Oid) -> SPIPlanPtr {
    SPI_prepare_cursor(src, nargs, argtypes, 0)
}

pub unsafe fn SPI_prepare_cursor(
    src: *const c_char,
    nargs: c_int,
    argtypes: *mut Oid,
    cursorOptions: c_int,
) -> SPIPlanPtr {
    let mut plan: _SPI_plan = zeroed();
    let result: SPIPlanPtr;

    if src.is_null() || nargs < 0 || (nargs > 0 && argtypes.is_null()) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    SPI_result = _SPI_begin_call(true);
    if SPI_result < 0 {
        return null_mut();
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = cursorOptions;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = None;
    plan.parserSetupArg = null_mut();

    _SPI_prepare_plan(src, &mut plan);

    /* copy plan to procedure context */
    result = _SPI_make_plan_non_temp(&mut plan);

    _SPI_end_call(true);

    result
}

pub unsafe fn SPI_prepare_extended(
    src: *const c_char,
    options: *const SPIPrepareOptions,
) -> SPIPlanPtr {
    let mut plan: _SPI_plan = zeroed();
    let result: SPIPlanPtr;

    if src.is_null() || options.is_null() {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    SPI_result = _SPI_begin_call(true);
    if SPI_result < 0 {
        return null_mut();
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = (*options).parseMode as SpiRawParseMode;
    plan.cursor_options = (*options).cursorOptions;
    plan.nargs = 0;
    plan.argtypes = null_mut();
    plan.parserSetup = (*options).parserSetup;
    plan.parserSetupArg = (*options).parserSetupArg;

    _SPI_prepare_plan(src, &mut plan);

    /* copy plan to procedure context */
    result = _SPI_make_plan_non_temp(&mut plan);

    _SPI_end_call(true);

    result
}

pub unsafe fn SPI_prepare_params(
    src: *const c_char,
    parserSetup: ParserSetupHook,
    parserSetupArg: *mut c_void,
    cursorOptions: c_int,
) -> SPIPlanPtr {
    let mut plan: _SPI_plan = zeroed();
    let result: SPIPlanPtr;

    if src.is_null() {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    SPI_result = _SPI_begin_call(true);
    if SPI_result < 0 {
        return null_mut();
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = cursorOptions;
    plan.nargs = 0;
    plan.argtypes = null_mut();
    plan.parserSetup = parserSetup;
    plan.parserSetupArg = parserSetupArg;

    _SPI_prepare_plan(src, &mut plan);

    /* copy plan to procedure context */
    result = _SPI_make_plan_non_temp(&mut plan);

    _SPI_end_call(true);

    result
}

pub unsafe fn SPI_keepplan(plan: SPIPlanPtr) -> c_int {
    let mut lc: *mut ListCell;

    if plan.is_null()
        || (*plan).magic != _SPI_PLAN_MAGIC
        || (*plan).saved
        || (*plan).oneshot
    {
        return SPI_ERROR_ARGUMENT;
    }

    /*
     * Mark it saved, reparent it under CacheMemoryContext, and mark all the
     * component CachedPlanSources as saved.  This sequence cannot fail
     * partway through, so there's no risk of long-term memory leakage.
     */
    (*plan).saved = true;
    MemoryContextSetParent((*plan).plancxt, _CacheMemoryContext);

    foreach!(lc, (*plan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;
        SaveCachedPlan(plansource);
    });

    0
}

pub unsafe fn SPI_saveplan(plan: SPIPlanPtr) -> SPIPlanPtr {
    let newplan: SPIPlanPtr;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    SPI_result = _SPI_begin_call(false); /* don't change context */
    if SPI_result < 0 {
        return null_mut();
    }

    newplan = _SPI_save_plan(plan);

    SPI_result = _SPI_end_call(false);

    newplan
}

pub unsafe fn SPI_freeplan(plan: SPIPlanPtr) -> c_int {
    let mut lc: *mut ListCell;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC {
        return SPI_ERROR_ARGUMENT;
    }

    /* Release the plancache entries */
    foreach!(lc, (*plan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;
        DropCachedPlan(plansource);
    });

    /* Now get rid of the _SPI_plan and subsidiary data in its plancxt */
    MemoryContextDelete((*plan).plancxt);

    0
}

pub unsafe fn SPI_copytuple(tuple: HeapTuple) -> HeapTuple {
    let mut oldcxt: MemoryContext;
    let ctuple: HeapTuple;

    if tuple.is_null() {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    if _SPI_current.is_null() {
        SPI_result = SPI_ERROR_UNCONNECTED;
        return null_mut();
    }

    oldcxt = MemoryContextSwitchTo((*_SPI_current).savedcxt);

    ctuple = heap_copytuple(tuple);

    MemoryContextSwitchTo(oldcxt);

    ctuple
}

pub unsafe fn SPI_returntuple(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
) -> HeapTupleHeader {
    let mut oldcxt: MemoryContext;
    let dtup: HeapTupleHeader;

    if tuple.is_null() || tupdesc.is_null() {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    if _SPI_current.is_null() {
        SPI_result = SPI_ERROR_UNCONNECTED;
        return null_mut();
    }

    /* For RECORD results, make sure a typmod has been assigned */
    if (*tupdesc).tdtypeid == RECORDOID && (*tupdesc).tdtypmod < 0 {
        assign_record_type_typmod(tupdesc);
    }

    oldcxt = MemoryContextSwitchTo((*_SPI_current).savedcxt);

    dtup = DatumGetHeapTupleHeader(heap_copy_tuple_as_datum(tuple, tupdesc));

    MemoryContextSwitchTo(oldcxt);

    dtup
}

pub unsafe fn SPI_modifytuple(
    rel: Relation,
    tuple: HeapTuple,
    natts: c_int,
    attnum: *mut c_int,
    Values: *mut Datum,
    Nulls: *const c_char,
) -> HeapTuple {
    let mut oldcxt: MemoryContext;
    let mut mtuple: HeapTuple;
    let numberOfAttributes: c_int;
    let v: *mut Datum;
    let n: *mut bool;
    let mut i: c_int;

    if rel.is_null()
        || tuple.is_null()
        || natts < 0
        || attnum.is_null()
        || Values.is_null()
    {
        SPI_result = SPI_ERROR_ARGUMENT;
        return null_mut();
    }

    if _SPI_current.is_null() {
        SPI_result = SPI_ERROR_UNCONNECTED;
        return null_mut();
    }

    oldcxt = MemoryContextSwitchTo((*_SPI_current).savedcxt);

    SPI_result = 0;

    numberOfAttributes = (*(*rel).rd_att).natts;
    v = palloc((numberOfAttributes as usize) * size_of::<Datum>()) as *mut Datum;
    n = palloc((numberOfAttributes as usize) * size_of::<bool>()) as *mut bool;

    /* fetch old values and nulls */
    heap_deform_tuple(tuple, (*rel).rd_att, v, n);

    /* replace values and nulls */
    i = 0;
    while i < natts {
        if *attnum.add(i as usize) <= 0 || *attnum.add(i as usize) > numberOfAttributes {
            break;
        }
        *v.add((*attnum.add(i as usize) - 1) as usize) = *Values.add(i as usize);
        *n.add((*attnum.add(i as usize) - 1) as usize) =
            !Nulls.is_null() && *Nulls.add(i as usize) == b'n' as c_char;
        i += 1;
    }

    if i == natts { /* no errors in *attnum */
        mtuple = heap_form_tuple((*rel).rd_att, v, n);

        /*
         * copy the identification info of the old tuple: t_ctid, t_self, and
         * OID (if any)
         */
        (*(*mtuple).t_data).t_ctid = (*(*tuple).t_data).t_ctid;
        (*mtuple).t_self = (*tuple).t_self;
        (*mtuple).t_tableOid = (*tuple).t_tableOid;
    } else {
        mtuple = null_mut();
        SPI_result = SPI_ERROR_NOATTRIBUTE;
    }

    pfree(v as *mut c_void);
    pfree(n as *mut c_void);

    MemoryContextSwitchTo(oldcxt);

    mtuple
}

pub unsafe fn SPI_fnumber(tupdesc: TupleDesc, fname: *const c_char) -> c_int {
    let mut res: c_int;
    let sysatt: *const c_void;

    res = 0;
    while res < (*tupdesc).natts {
        let attr = TupleDescAttr(tupdesc, res);

        if namestrcmp(&(*attr).attname as *const _ as *const c_void, fname) == 0
            && !(*attr).attisdropped
        {
            return res + 1;
        }
        res += 1;
    }

    sysatt = SystemAttributeByName(fname);
    if !sysatt.is_null() {
        return (*(sysatt as *const FormData_pg_attribute_stub)).attnum as c_int;
    }

    /* SPI_ERROR_NOATTRIBUTE is different from all sys column numbers */
    SPI_ERROR_NOATTRIBUTE
}

/* minimal stub for attnum field access in SystemAttributeByName result */
#[repr(C)]
struct FormData_pg_attribute_stub {
    pub attnum: i16,
}

pub unsafe fn SPI_fname(tupdesc: TupleDesc, fnumber: c_int) -> *mut c_char {
    let att: *const c_void;

    SPI_result = 0;

    if fnumber > (*tupdesc).natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as c_int
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return null_mut();
    }

    if fnumber > 0 {
        att = TupleDescAttr(tupdesc, fnumber - 1) as *const c_void;
    } else {
        att = SystemAttributeDefinition(fnumber);
    }

    pstrdup(NameStr_stub(att))
}

/* helper: pull attname field from a FormData_pg_attribute pointer (opaque) */
unsafe fn NameStr_stub(att: *const c_void) -> *const c_char {
    /* TODO(pg-port): NameStr macro expands name.data; use offset 0 here as stub */
    unimplemented!("NameStr_stub")
}

pub unsafe fn SPI_getvalue(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
    fnumber: c_int,
) -> *mut c_char {
    let val: Datum;
    let mut isnull: bool = false;
    let typoid: Oid;
    let mut foutoid: Oid = 0;
    let mut typisvarlena: bool = false;

    SPI_result = 0;

    if fnumber > (*tupdesc).natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as c_int
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return null_mut();
    }

    val = heap_getattr(tuple, fnumber, tupdesc, &mut isnull);
    if isnull {
        return null_mut();
    }

    if fnumber > 0 {
        typoid = (*TupleDescAttr(tupdesc, fnumber - 1)).atttypid;
    } else {
        typoid = (*(SystemAttributeDefinition(fnumber) as *const FormData_pg_attribute_stub_typid)).atttypid;
    }

    getTypeOutputInfo(typoid, &mut foutoid, &mut typisvarlena);

    OidOutputFunctionCall(foutoid, val)
}

#[repr(C)]
struct FormData_pg_attribute_stub_typid {
    pub atttypid: Oid,
}

pub unsafe fn SPI_getbinval(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
    fnumber: c_int,
    isnull: *mut bool,
) -> Datum {
    SPI_result = 0;

    if fnumber > (*tupdesc).natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as c_int
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        *isnull = true;
        return 0 as Datum; /* (Datum) NULL */
    }

    heap_getattr(tuple, fnumber, tupdesc, isnull)
}

pub unsafe fn SPI_gettype(tupdesc: TupleDesc, fnumber: c_int) -> *mut c_char {
    let typoid: Oid;
    let typeTuple: HeapTuple;
    let result: *mut c_char;

    SPI_result = 0;

    if fnumber > (*tupdesc).natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as c_int
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return null_mut();
    }

    if fnumber > 0 {
        typoid = (*TupleDescAttr(tupdesc, fnumber - 1)).atttypid;
    } else {
        typoid = (*(SystemAttributeDefinition(fnumber) as *const FormData_pg_attribute_stub_typid)).atttypid;
    }

    typeTuple = SearchSysCache1(TYPEOID(), ObjectIdGetDatum(typoid));

    if !HeapTupleIsValid(typeTuple) {
        SPI_result = SPI_ERROR_TYPUNKNOWN;
        return null_mut();
    }

    result = pstrdup(NameStr(GETSTRUCT(typeTuple) as *const c_void));
    ReleaseSysCache(typeTuple);
    result
}

/*
 * Get the data type OID for a column.
 *
 * There's nothing similar for typmod and typcollation.  The rare consumers
 * thereof should inspect the TupleDesc directly.
 */
pub unsafe fn SPI_gettypeid(tupdesc: TupleDesc, fnumber: c_int) -> Oid {
    SPI_result = 0;

    if fnumber > (*tupdesc).natts
        || fnumber == 0
        || fnumber <= FirstLowInvalidHeapAttributeNumber as c_int
    {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return InvalidOid;
    }

    if fnumber > 0 {
        (*TupleDescAttr(tupdesc, fnumber - 1)).atttypid
    } else {
        (*(SystemAttributeDefinition(fnumber) as *const FormData_pg_attribute_stub_typid)).atttypid
    }
}

pub unsafe fn SPI_getrelname(rel: Relation) -> *mut c_char {
    pstrdup(RelationGetRelationName(rel))
}

pub unsafe fn SPI_getnspname(rel: Relation) -> *mut c_char {
    get_namespace_name(RelationGetNamespace(rel))
}

pub unsafe fn SPI_palloc(size: Size) -> *mut c_void {
    if _SPI_current.is_null() {
        elog!(ERROR, "SPI_palloc called while not connected to SPI");
    }

    MemoryContextAlloc((*_SPI_current).savedcxt, size)
}

pub unsafe fn SPI_repalloc(pointer: *mut c_void, size: Size) -> *mut c_void {
    /* No longer need to worry which context chunk was in... */
    repalloc(pointer, size)
}

pub unsafe fn SPI_pfree(pointer: *mut c_void) {
    /* No longer need to worry which context chunk was in... */
    pfree(pointer);
}

pub unsafe fn SPI_datumTransfer(value: Datum, typByVal: bool, typLen: c_int) -> Datum {
    let mut oldcxt: MemoryContext;
    let result: Datum;

    if _SPI_current.is_null() {
        elog!(ERROR, "SPI_datumTransfer called while not connected to SPI");
    }

    oldcxt = MemoryContextSwitchTo((*_SPI_current).savedcxt);

    result = datumTransfer(value, typByVal, typLen);

    MemoryContextSwitchTo(oldcxt);

    result
}

pub unsafe fn SPI_freetuple(tuple: HeapTuple) {
    /* No longer need to worry which context tuple was in... */
    heap_freetuple(tuple);
}

pub unsafe fn SPI_freetuptable(tuptable: *mut SPITupleTable) {
    let mut found: bool = false;

    /* ignore call if NULL pointer */
    if tuptable.is_null() {
        return;
    }

    /*
     * Search only the topmost SPI context for a matching tuple table.
     */
    if !_SPI_current.is_null() {
        /* TODO(pg-port): slist_foreach_modify not yet wired; stub the search */
        /*
        let mut siter: slist_mutable_iter = zeroed();
        slist_foreach_modify!(siter, &mut (*_SPI_current).tuptables, {
            let tt: *mut SPITupleTable = slist_container!(SPITupleTable, next, siter.cur);
            if tt == tuptable {
                slist_delete_current(&mut siter);
                found = true;
                break;
            }
        });
        */
        /* TODO(pg-port): stub - assume found */
        found = true;
    }

    /*
     * Refuse the deletion if we didn't find it in the topmost SPI context.
     * This is primarily a guard against double deletion, but might prevent
     * other errors as well.  Since the worst consequence of not deleting a
     * tuptable would be a transient memory leak, this is just a WARNING.
     */
    if !found {
        elog!(WARNING, "attempt to delete invalid SPITupleTable {:p}", tuptable);
        return;
    }

    /* for safety, reset global variables that might point at tuptable */
    if !_SPI_current.is_null() && tuptable == (*_SPI_current).tuptable as *mut SPITupleTable {
        (*_SPI_current).tuptable = null_mut();
    }
    if tuptable == SPI_tuptable {
        SPI_tuptable = null_mut();
    }

    /* release all memory belonging to tuptable */
    MemoryContextDelete((*tuptable).tuptabcxt);
}


/*
 * SPI_cursor_open()
 *
 *  Open a prepared SPI plan as a portal
 */
pub unsafe fn SPI_cursor_open(
    name: *const c_char,
    plan: SPIPlanPtr,
    Values: *mut Datum,
    Nulls: *const c_char,
    read_only: bool,
) -> Portal {
    let portal: Portal;
    let paramLI: ParamListInfo;

    /* build transient ParamListInfo in caller's context */
    paramLI = _SPI_convert_params((*plan).nargs, (*plan).argtypes,
                                   Values, Nulls);

    portal = SPI_cursor_open_internal(name, plan, paramLI, read_only);

    /* done with the transient ParamListInfo */
    if !paramLI.is_null() {
        pfree(paramLI as *mut c_void);
    }

    portal
}


/*
 * SPI_cursor_open_with_args()
 *
 * Parse and plan a query and open it as a portal.
 */
pub unsafe fn SPI_cursor_open_with_args(
    name: *const c_char,
    src: *const c_char,
    nargs: c_int,
    argtypes: *mut Oid,
    Values: *mut Datum,
    Nulls: *const c_char,
    read_only: bool,
    cursorOptions: c_int,
) -> Portal {
    let result: Portal;
    let mut plan: _SPI_plan = zeroed();
    let paramLI: ParamListInfo;

    if src.is_null() || nargs < 0 {
        elog!(ERROR, "SPI_cursor_open_with_args called with invalid arguments");
    }

    if nargs > 0 && (argtypes.is_null() || Values.is_null()) {
        elog!(ERROR, "SPI_cursor_open_with_args called with missing parameters");
    }

    SPI_result = _SPI_begin_call(true);
    if SPI_result < 0 {
        elog!(ERROR, "SPI_cursor_open_with_args called while not connected");
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = cursorOptions;
    plan.nargs = nargs;
    plan.argtypes = argtypes;
    plan.parserSetup = None;
    plan.parserSetupArg = null_mut();

    /* build transient ParamListInfo in executor context */
    paramLI = _SPI_convert_params(nargs, argtypes, Values, Nulls);

    _SPI_prepare_plan(src, &mut plan);

    /* We needn't copy the plan; SPI_cursor_open_internal will do so */

    result = SPI_cursor_open_internal(name, &mut plan, paramLI, read_only);

    /* And clean up */
    _SPI_end_call(true);

    result
}


/*
 * SPI_cursor_open_with_paramlist()
 *
 *  Same as SPI_cursor_open except that parameters (if any) are passed
 *  as a ParamListInfo, which supports dynamic parameter set determination
 */
pub unsafe fn SPI_cursor_open_with_paramlist(
    name: *const c_char,
    plan: SPIPlanPtr,
    params: ParamListInfo,
    read_only: bool,
) -> Portal {
    SPI_cursor_open_internal(name, plan, params, read_only)
}

/* Parse a query and open it as a cursor */
pub unsafe fn SPI_cursor_parse_open(
    name: *const c_char,
    src: *const c_char,
    options: *const SPIParseOpenOptions,
) -> Portal {
    let result: Portal;
    let mut plan: _SPI_plan = zeroed();

    if src.is_null() || options.is_null() {
        elog!(ERROR, "SPI_cursor_parse_open called with invalid arguments");
    }

    SPI_result = _SPI_begin_call(true);
    if SPI_result < 0 {
        elog!(ERROR, "SPI_cursor_parse_open called while not connected");
    }

    plan.magic = _SPI_PLAN_MAGIC;
    plan.parse_mode = RAW_PARSE_DEFAULT as SpiRawParseMode;
    plan.cursor_options = (*options).cursorOptions;
    if !(*options).params.is_null() {
        plan.parserSetup = (*(*options).params).parserSetup;
        plan.parserSetupArg = (*(*options).params).parserSetupArg;
    }

    _SPI_prepare_plan(src, &mut plan);

    /* We needn't copy the plan; SPI_cursor_open_internal will do so */

    result = SPI_cursor_open_internal(name, &mut plan,
                                      (*options).params, (*options).read_only);

    /* And clean up */
    _SPI_end_call(true);

    result
}


/*
 * SPI_cursor_open_internal()
 *
 *  Common code for SPI_cursor_open variants
 */
unsafe fn SPI_cursor_open_internal(
    name: *const c_char,
    plan: SPIPlanPtr,
    mut paramLI: ParamListInfo,
    read_only: bool,
) -> Portal {
    let plansource: *mut CachedPlanSource;
    let mut cplan: *mut CachedPlan;
    let mut stmt_list: *mut List;
    let query_string: *mut c_char;
    let snapshot: Snapshot;
    let mut oldcontext: MemoryContext;
    let portal: Portal;
    let mut spicallbackarg: SPICallbackArg = SPICallbackArg {
        query: null(),
        mode: RAW_PARSE_DEFAULT,
    };
    let mut spierrcontext: ErrorContextCallback = zeroed();

    /*
     * Check that the plan is something the Portal code will special-case as
     * returning one tupleset.
     */
    if !SPI_is_cursor_plan(plan) {
        /* try to give a good error message */
        let cmdtag: *const c_char;

        if list_length((*plan).plancache_list) != 1 {
            ereport!(ERROR, errmsg!("cannot open multi-query plan as cursor") /* C also: errcode(ERRCODE_INVALID_CURSOR_DEFINITION) */);
        }
        plansource = linitial((*plan).plancache_list) as *mut CachedPlanSource;
        /* A SELECT that fails SPI_is_cursor_plan() must be SELECT INTO */
        if plansource_commandtag(plansource) == CMDTAG_SELECT {
            cmdtag = b"SELECT INTO\0".as_ptr() as *const c_char;
        } else {
            cmdtag = GetCommandTagName(plansource_commandtag(plansource));
        }
        ereport!(ERROR, errmsg!("cannot open {} query as cursor",
                    core::ffi::CStr::from_ptr(cmdtag).to_string_lossy()) /* C also: errcode(ERRCODE_INVALID_CURSOR_DEFINITION) */);
    }

    Assert!(list_length((*plan).plancache_list) == 1);
    let plansource: *mut CachedPlanSource =
        linitial((*plan).plancache_list) as *mut CachedPlanSource;

    /* Push the SPI stack */
    if _SPI_begin_call(true) < 0 {
        elog!(ERROR, "SPI_cursor_open called while not connected");
    }

    /* Reset SPI result (note we deliberately don't touch lastoid) */
    SPI_processed = 0;
    SPI_tuptable = null_mut();
    (*_SPI_current).processed = 0;
    (*_SPI_current).tuptable = null_mut();

    /* Create the portal */
    let portal: Portal;
    if name.is_null() || *name == 0 {
        /* Use a random nonconflicting name */
        portal = CreateNewPortal();
    } else {
        /* In this path, error if portal of same name already exists */
        portal = CreatePortal(name, false, false);
    }

    /* Copy the plan's query string into the portal */
    query_string = MemoryContextStrdup((*portal).portalContext as MemoryContext,
                                       (*plansource).query_string);

    /*
     * Setup error traceback support for ereport(), in case GetCachedPlan
     * throws an error.
     */
    spicallbackarg.query = (*plansource).query_string;
    spicallbackarg.mode = core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode);
    spierrcontext.callback = _SPI_error_callback_trampoline;
    spierrcontext.arg = &mut spicallbackarg as *mut _ as *mut c_void;
    spierrcontext.previous = error_context_stack;
    error_context_stack = &mut spierrcontext;

    /*
     * Note: for a saved plan, we mustn't have any failure occur between
     * GetCachedPlan and PortalDefineQuery; that would result in leaking our
     * plancache refcount.
     */

    /* Replan if needed, and increment plan refcount for portal */
    cplan = GetCachedPlan(plansource, paramLI, null_mut(), (*_SPI_current).queryEnv);
    stmt_list = (*cplan).stmt_list;

    if !(*plan).saved {
        /*
         * We don't want the portal to depend on an unsaved CachedPlanSource,
         * so must copy the plan into the portal's context.  An error here
         * will result in leaking our refcount on the plan, but it doesn't
         * matter because the plan is unsaved and hence transient anyway.
         */
        oldcontext = MemoryContextSwitchTo((*portal).portalContext as MemoryContext);
        stmt_list = copyObject(stmt_list as *mut c_void) as *mut List;
        MemoryContextSwitchTo(oldcontext);
        ReleaseCachedPlan(cplan, null_mut());
        cplan = null_mut(); /* portal shouldn't depend on cplan */
    }

    /*
     * Set up the portal.
     */
    PortalDefineQuery(portal,
                      null(), /* no statement name */
                      query_string,
                      plansource_commandtag(plansource),
                      stmt_list,
                      cplan as *mut crate::utils::portal::CachedPlan);

    /*
     * Set up options for portal.  Default SCROLL type is chosen the same way
     * as PerformCursorOpen does it.
     */
    (*portal).cursorOptions = (*plan).cursor_options;
    if ((*portal).cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
        if list_length(stmt_list) == 1
            && (*linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list)).commandType != CMD_UTILITY
            && (*linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list)).rowMarks.is_null()
            && ExecSupportsBackwardScan(
                (*linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list)).planTree as *mut c_void,
            )
        {
            (*portal).cursorOptions |= CURSOR_OPT_SCROLL;
        } else {
            (*portal).cursorOptions |= CURSOR_OPT_NO_SCROLL;
        }
    }

    /*
     * Disallow SCROLL with SELECT FOR UPDATE.  This is not redundant with the
     * check in transformDeclareCursorStmt because the cursor options might
     * not have come through there.
     */
    if ((*portal).cursorOptions & CURSOR_OPT_SCROLL) != 0 {
        if list_length(stmt_list) == 1
            && (*linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list)).commandType != CMD_UTILITY
            && !(*linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list)).rowMarks.is_null()
        {
            ereport!(ERROR, errmsg!("DECLARE SCROLL CURSOR ... FOR UPDATE/SHARE is not supported") /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); errdetail!("Scrollable cursors must be READ ONLY.") */);
        }
    }

    /* Make current query environment available to portal at execution time. */
    (*portal).queryEnv = (*_SPI_current).queryEnv;

    /*
     * If told to be read-only, we'd better check for read-only queries. This
     * can't be done earlier because we need to look at the finished, planned
     * queries.  (In particular, we don't want to do it between GetCachedPlan
     * and PortalDefineQuery, because throwing an error between those steps
     * would result in leaking our plancache refcount.)
     */
    if read_only {
        let mut lc: *mut ListCell;

        foreach!(lc, stmt_list, {
            let pstmt: *mut PlannedStmt = lfirst(current_cell!(lc)) as *mut PlannedStmt;

            if !CommandIsReadOnly(pstmt) {
                ereport!(ERROR, errmsg!("{} is not allowed in a non-volatile function",
                            core::ffi::CStr::from_ptr(CreateCommandName(pstmt as *mut c_void)).to_string_lossy()) /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }
        });
    }

    /* Set up the snapshot to use. */
    if read_only {
        snapshot = GetActiveSnapshot();
    } else {
        CommandCounterIncrement();
        snapshot = GetTransactionSnapshot();
    }

    /*
     * If the plan has parameters, copy them into the portal.  Note that this
     * must be done after revalidating the plan, because in dynamic parameter
     * cases the set of parameters could have changed during re-parsing.
     */
    if !paramLI.is_null() {
        oldcontext = MemoryContextSwitchTo((*portal).portalContext as MemoryContext);
        paramLI = copyParamList(paramLI);
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * Start portal execution.
     */
    PortalStart(portal, paramLI, 0, snapshot);

    Assert!((*portal).strategy != PORTAL_MULTI_QUERY);

    /* Pop the error context stack */
    error_context_stack = spierrcontext.previous;

    /* Pop the SPI stack */
    _SPI_end_call(true);

    /* Return the created portal */
    portal
}

/* trampoline so ErrorContextCallback.callback has the right fn type */
unsafe fn _SPI_error_callback_trampoline(arg: *mut c_void) {
    _SPI_error_callback(arg);
}


/*
 * SPI_cursor_find()
 *
 *  Find the portal of an existing open cursor
 */
pub unsafe fn SPI_cursor_find(name: *const c_char) -> Portal {
    GetPortalByName(name)
}


/*
 * SPI_cursor_fetch()
 *
 *  Fetch rows in a cursor
 */
pub unsafe fn SPI_cursor_fetch(portal: Portal, forward: bool, count: c_long) {
    _SPI_cursor_operation(
        portal,
        if forward { FETCH_FORWARD } else { FETCH_BACKWARD },
        count,
        CreateDestReceiver(DestSPI),
    );
    /* we know that the DestSPI receiver doesn't need a destroy call */
}


/*
 * SPI_cursor_move()
 *
 *  Move in a cursor
 */
pub unsafe fn SPI_cursor_move(portal: Portal, forward: bool, count: c_long) {
    _SPI_cursor_operation(
        portal,
        if forward { FETCH_FORWARD } else { FETCH_BACKWARD },
        count,
        None_Receiver(),
    );
}


/*
 * SPI_scroll_cursor_fetch()
 *
 *  Fetch rows in a scrollable cursor
 */
pub unsafe fn SPI_scroll_cursor_fetch(
    portal: Portal,
    direction: FetchDirection,
    count: c_long,
) {
    _SPI_cursor_operation(portal,
                          direction, count,
                          CreateDestReceiver(DestSPI));
    /* we know that the DestSPI receiver doesn't need a destroy call */
}


/*
 * SPI_scroll_cursor_move()
 *
 *  Move in a scrollable cursor
 */
pub unsafe fn SPI_scroll_cursor_move(
    portal: Portal,
    direction: FetchDirection,
    count: c_long,
) {
    _SPI_cursor_operation(portal, direction, count, None_Receiver());
}


/*
 * SPI_cursor_close()
 *
 *  Close a cursor
 */
pub unsafe fn SPI_cursor_close(portal: Portal) {
    if !PortalIsValid(portal) {
        elog!(ERROR, "invalid portal in SPI cursor operation");
    }

    PortalDrop(portal, false);
}

/*
 * Returns the Oid representing the type id for argument at argIndex. First
 * parameter is at index zero.
 */
pub unsafe fn SPI_getargtypeid(plan: SPIPlanPtr, argIndex: c_int) -> Oid {
    if plan.is_null()
        || (*plan).magic != _SPI_PLAN_MAGIC
        || argIndex < 0
        || argIndex >= (*plan).nargs
    {
        SPI_result = SPI_ERROR_ARGUMENT;
        return InvalidOid;
    }
    *(*plan).argtypes.add(argIndex as usize)
}

/*
 * Returns the number of arguments for the prepared plan.
 */
pub unsafe fn SPI_getargcount(plan: SPIPlanPtr) -> c_int {
    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC {
        SPI_result = SPI_ERROR_ARGUMENT;
        return -1;
    }
    (*plan).nargs
}

/*
 * Returns true if the plan contains exactly one command
 * and that command returns tuples to the caller (eg, SELECT or
 * INSERT ... RETURNING, but not SELECT ... INTO). In essence,
 * the result indicates if the command can be used with SPI_cursor_open
 *
 * Parameters
 *    plan: A plan previously prepared using SPI_prepare
 */
pub unsafe fn SPI_is_cursor_plan(plan: SPIPlanPtr) -> bool {
    let plansource: *mut CachedPlanSource;

    if plan.is_null() || (*plan).magic != _SPI_PLAN_MAGIC {
        SPI_result = SPI_ERROR_ARGUMENT;
        return false;
    }

    if list_length((*plan).plancache_list) != 1 {
        SPI_result = 0;
        return false; /* not exactly 1 pre-rewrite command */
    }
    let plansource: *mut CachedPlanSource =
        linitial((*plan).plancache_list) as *mut CachedPlanSource;

    /*
     * We used to force revalidation of the cached plan here, but that seems
     * unnecessary: invalidation could mean a change in the rowtype of the
     * tuples returned by a plan, but not whether it returns tuples at all.
     */
    SPI_result = 0;

    /* Does it return tuples? */
    if !(*plansource).resultDesc.is_null() {
        return true;
    }

    false
}

/*
 * SPI_plan_is_valid --- test whether a SPI plan is currently valid
 * (that is, not marked as being in need of revalidation).
 *
 * See notes for CachedPlanIsValid before using this.
 */
pub unsafe fn SPI_plan_is_valid(plan: SPIPlanPtr) -> bool {
    let mut lc: *mut ListCell;

    Assert!((*plan).magic == _SPI_PLAN_MAGIC);

    foreach!(lc, (*plan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;

        if !CachedPlanIsValid(plansource) {
            return false;
        }
    });
    true
}

/*
 * SPI_result_code_string --- convert any SPI return code to a string
 *
 * This is often useful in error messages.  Most callers will probably
 * only pass negative (error-case) codes, but for generality we recognize
 * the success codes too.
 */
pub unsafe fn SPI_result_code_string(code: c_int) -> *const c_char {
    static mut buf: [c_char; 64] = [0; 64];

    match code {
        SPI_ERROR_CONNECT => return b"SPI_ERROR_CONNECT\0".as_ptr() as *const c_char,
        SPI_ERROR_COPY => return b"SPI_ERROR_COPY\0".as_ptr() as *const c_char,
        SPI_ERROR_OPUNKNOWN => return b"SPI_ERROR_OPUNKNOWN\0".as_ptr() as *const c_char,
        SPI_ERROR_UNCONNECTED => return b"SPI_ERROR_UNCONNECTED\0".as_ptr() as *const c_char,
        SPI_ERROR_ARGUMENT => return b"SPI_ERROR_ARGUMENT\0".as_ptr() as *const c_char,
        SPI_ERROR_PARAM => return b"SPI_ERROR_PARAM\0".as_ptr() as *const c_char,
        SPI_ERROR_TRANSACTION => return b"SPI_ERROR_TRANSACTION\0".as_ptr() as *const c_char,
        SPI_ERROR_NOATTRIBUTE => return b"SPI_ERROR_NOATTRIBUTE\0".as_ptr() as *const c_char,
        SPI_ERROR_NOOUTFUNC => return b"SPI_ERROR_NOOUTFUNC\0".as_ptr() as *const c_char,
        SPI_ERROR_TYPUNKNOWN => return b"SPI_ERROR_TYPUNKNOWN\0".as_ptr() as *const c_char,
        SPI_ERROR_REL_DUPLICATE => return b"SPI_ERROR_REL_DUPLICATE\0".as_ptr() as *const c_char,
        SPI_ERROR_REL_NOT_FOUND => return b"SPI_ERROR_REL_NOT_FOUND\0".as_ptr() as *const c_char,
        SPI_OK_CONNECT => return b"SPI_OK_CONNECT\0".as_ptr() as *const c_char,
        SPI_OK_FINISH => return b"SPI_OK_FINISH\0".as_ptr() as *const c_char,
        SPI_OK_FETCH => return b"SPI_OK_FETCH\0".as_ptr() as *const c_char,
        SPI_OK_UTILITY => return b"SPI_OK_UTILITY\0".as_ptr() as *const c_char,
        SPI_OK_SELECT => return b"SPI_OK_SELECT\0".as_ptr() as *const c_char,
        SPI_OK_SELINTO => return b"SPI_OK_SELINTO\0".as_ptr() as *const c_char,
        SPI_OK_INSERT => return b"SPI_OK_INSERT\0".as_ptr() as *const c_char,
        SPI_OK_DELETE => return b"SPI_OK_DELETE\0".as_ptr() as *const c_char,
        SPI_OK_UPDATE => return b"SPI_OK_UPDATE\0".as_ptr() as *const c_char,
        SPI_OK_CURSOR => return b"SPI_OK_CURSOR\0".as_ptr() as *const c_char,
        SPI_OK_INSERT_RETURNING => return b"SPI_OK_INSERT_RETURNING\0".as_ptr() as *const c_char,
        SPI_OK_DELETE_RETURNING => return b"SPI_OK_DELETE_RETURNING\0".as_ptr() as *const c_char,
        SPI_OK_UPDATE_RETURNING => return b"SPI_OK_UPDATE_RETURNING\0".as_ptr() as *const c_char,
        SPI_OK_REWRITTEN => return b"SPI_OK_REWRITTEN\0".as_ptr() as *const c_char,
        SPI_OK_REL_REGISTER => return b"SPI_OK_REL_REGISTER\0".as_ptr() as *const c_char,
        SPI_OK_REL_UNREGISTER => return b"SPI_OK_REL_UNREGISTER\0".as_ptr() as *const c_char,
        SPI_OK_TD_REGISTER => return b"SPI_OK_TD_REGISTER\0".as_ptr() as *const c_char,
        SPI_OK_MERGE => return b"SPI_OK_MERGE\0".as_ptr() as *const c_char,
        SPI_OK_MERGE_RETURNING => return b"SPI_OK_MERGE_RETURNING\0".as_ptr() as *const c_char,
        _ => {}
    }
    /* Unrecognized code ... return something useful ... */
    /* TODO(pg-port): sprintf -- just return a static fallback for now */
    b"Unrecognized SPI code\0".as_ptr() as *const c_char
}

/*
 * SPI_plan_get_plan_sources --- get a SPI plan's underlying list of
 * CachedPlanSources.
 *
 * CAUTION: there is no check on whether the CachedPlanSources are up-to-date.
 *
 * This is exported so that PL/pgSQL can use it (this beats letting PL/pgSQL
 * look directly into the SPIPlan for itself).  It's not documented in
 * spi.sgml because we'd just as soon not have too many places using this.
 */
pub unsafe fn SPI_plan_get_plan_sources(plan: SPIPlanPtr) -> *mut List {
    Assert!((*plan).magic == _SPI_PLAN_MAGIC);
    (*plan).plancache_list
}

/*
 * SPI_plan_get_cached_plan --- get a SPI plan's generic CachedPlan,
 * if the SPI plan contains exactly one CachedPlanSource.  If not,
 * return NULL.
 *
 * The plan's refcount is incremented (and logged in CurrentResourceOwner,
 * if it's a saved plan).  Caller is responsible for doing ReleaseCachedPlan.
 *
 * This is exported so that PL/pgSQL can use it (this beats letting PL/pgSQL
 * look directly into the SPIPlan for itself).  It's not documented in
 * spi.sgml because we'd just as soon not have too many places using this.
 */
pub unsafe fn SPI_plan_get_cached_plan(plan: SPIPlanPtr) -> *mut CachedPlan {
    let plansource: *mut CachedPlanSource;
    let cplan: *mut CachedPlan;
    let mut spicallbackarg: SPICallbackArg = SPICallbackArg {
        query: null(),
        mode: RAW_PARSE_DEFAULT,
    };
    let mut spierrcontext: ErrorContextCallback = zeroed();

    Assert!((*plan).magic == _SPI_PLAN_MAGIC);

    /* Can't support one-shot plans here */
    if (*plan).oneshot {
        return null_mut();
    }

    /* Must have exactly one CachedPlanSource */
    if list_length((*plan).plancache_list) != 1 {
        return null_mut();
    }
    let plansource: *mut CachedPlanSource =
        linitial((*plan).plancache_list) as *mut CachedPlanSource;

    /* Setup error traceback support for ereport() */
    spicallbackarg.query = (*plansource).query_string;
    spicallbackarg.mode = core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode);
    spierrcontext.callback = _SPI_error_callback_trampoline;
    spierrcontext.arg = &mut spicallbackarg as *mut _ as *mut c_void;
    spierrcontext.previous = error_context_stack;
    error_context_stack = &mut spierrcontext;

    /* Get the generic plan for the query */
    let cplan: *mut CachedPlan = GetCachedPlan(
        plansource,
        null_mut(),
        if (*plan).saved { CurrentResourceOwner } else { null_mut() },
        (*_SPI_current).queryEnv,
    );
    /* TODO(pg-port): commands/prepare.rs CachedPlanSource stub lacks the gplan
     * field; restore Assert(cplan == plansource->gplan) once plancache.c lands. */
    let _ = cplan;

    /* Pop the error context stack */
    error_context_stack = spierrcontext.previous;

    cplan
}

/* =================== private functions =================== */

/*
 * spi_dest_startup
 *		Initialize to receive tuples from Executor into SPITupleTable
 *		of current SPI procedure
 */
pub unsafe fn spi_dest_startup(
    _self_: *mut DestReceiver,
    _operation: c_int,
    typeinfo: TupleDesc,
) {
    let tuptable: *mut SPITupleTable;
    let oldcxt: MemoryContext;
    let tuptabcxt: MemoryContext;

    if _SPI_current.is_null() {
        elog!(ERROR, "spi_dest_startup called while not connected to SPI");
    }

    if !(*_SPI_current).tuptable.is_null() {
        elog!(ERROR, "improper call to spi_dest_startup");
    }

    /* We create the tuple table context as a child of procCxt */

    oldcxt = _SPI_procmem(); /* switch to procedure memory context */

    tuptabcxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SPI TupTable".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    MemoryContextSwitchTo(tuptabcxt);

    (*_SPI_current).tuptable = palloc0(size_of::<SPITupleTable>()) as *mut SpiPrivTupleTable;
    tuptable = (*_SPI_current).tuptable as *mut SPITupleTable;
    (*tuptable).tuptabcxt = tuptabcxt;
    (*tuptable).subid = GetCurrentSubTransactionId();

    /*
     * The tuptable is now valid enough to be freed by AtEOSubXact_SPI, so put
     * it onto the SPI context's tuptables list.  This will ensure it's not
     * leaked even in the unlikely event the following few lines fail.
     */
    slist_push_head(&mut (*_SPI_current).tuptables, &mut (*tuptable).next);

    /* set up initial allocations */
    (*tuptable).alloced = 128;
    (*tuptable).vals = palloc((*tuptable).alloced as usize * size_of::<HeapTuple>()) as *mut HeapTuple;
    (*tuptable).numvals = 0;
    (*tuptable).tupdesc = CreateTupleDescCopy(typeinfo);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * spi_printtup
 *		store tuple retrieved by Executor into SPITupleTable
 *		of current SPI procedure
 */
pub unsafe fn spi_printtup(slot: *mut c_void, _self_: *mut DestReceiver) -> bool {
    let tuptable: *mut SPITupleTable;
    let oldcxt: MemoryContext;

    if _SPI_current.is_null() {
        elog!(ERROR, "spi_printtup called while not connected to SPI");
    }

    tuptable = (*_SPI_current).tuptable as *mut SPITupleTable;
    if tuptable.is_null() {
        elog!(ERROR, "improper call to spi_printtup");
    }

    oldcxt = MemoryContextSwitchTo((*tuptable).tuptabcxt);

    if (*tuptable).numvals >= (*tuptable).alloced {
        /* Double the size of the pointer array */
        let newalloced: u64 = (*tuptable).alloced * 2;

        (*tuptable).vals = repalloc_huge(
            (*tuptable).vals as *mut c_void,
            newalloced as usize * size_of::<HeapTuple>(),
        ) as *mut HeapTuple;
        (*tuptable).alloced = newalloced;
    }

    *(*tuptable).vals.add((*tuptable).numvals as usize) = ExecCopySlotHeapTuple(slot);
    (*tuptable).numvals += 1;

    MemoryContextSwitchTo(oldcxt);

    true
}

/*
 * _SPI_prepare_plan
 *
 * Parse and analyze a querystring.
 *
 * At entry, plan->argtypes and plan->nargs (or alternatively plan->parserSetup
 * and plan->parserSetupArg) must be valid, as must plan->parse_mode and
 * plan->cursor_options.
 *
 * Results are stored into *plan (specifically, plan->plancache_list).
 * Note that the result data is all in CurrentMemoryContext or child contexts
 * thereof; in practice this means it is in the SPI executor context, and
 * what we are creating is a "temporary" SPIPlan.  Cruft generated during
 * parsing is also left in CurrentMemoryContext.
 */
unsafe fn _SPI_prepare_plan(src: *const c_char, plan: SPIPlanPtr) {
    let raw_parsetree_list: *mut List;
    let mut plancache_list: *mut List;
    let mut spicallbackarg: SPICallbackArg = SPICallbackArg {
        query: null(),
        mode: RAW_PARSE_DEFAULT,
    };
    let mut spierrcontext: ErrorContextCallback = zeroed();

    /* Setup error traceback support for ereport() */
    spicallbackarg.query = src;
    spicallbackarg.mode = core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode);
    spierrcontext.callback = _SPI_error_callback_trampoline;
    spierrcontext.arg = &mut spicallbackarg as *mut _ as *mut c_void;
    spierrcontext.previous = error_context_stack;
    error_context_stack = &mut spierrcontext;

    /* Parse the request string into a list of raw parse trees. */
    raw_parsetree_list = raw_parser(src, core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode));

    /*
     * Do parse analysis and rule rewrite for each raw parsetree, storing the
     * results into unsaved plancache entries.
     */
    plancache_list = NIL;

    foreach!(lc, raw_parsetree_list, {
        let parsetree: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, current_cell!(lc));
        let stmt_list: *mut List;
        let plansource: *mut CachedPlanSource;

        /*
         * Create the CachedPlanSource before we do parse analysis, since it
         * needs to see the unmodified raw parse tree.
         */
        plansource = CreateCachedPlan(
            parsetree,
            src,
            CreateCommandTag((*parsetree).stmt as *mut c_void),
        );

        /*
         * Parameter datatypes are driven by parserSetup hook if provided,
         * otherwise we use the fixed parameter list.
         */
        if !(*plan).parserSetup.is_none() {
            Assert!((*plan).nargs == 0);
            stmt_list = pg_analyze_and_rewrite_withcb(
                parsetree,
                src,
                (*plan).parserSetup,
                (*plan).parserSetupArg,
                (*_SPI_current).queryEnv,
            );
        } else {
            stmt_list = pg_analyze_and_rewrite_fixedparams(
                parsetree,
                src,
                (*plan).argtypes,
                (*plan).nargs,
                (*_SPI_current).queryEnv,
            );
        }

        /* Finish filling in the CachedPlanSource */
        CompleteCachedPlan(
            plansource,
            stmt_list,
            null_mut(),
            (*plan).argtypes,
            (*plan).nargs,
            (*plan).parserSetup,
            (*plan).parserSetupArg,
            (*plan).cursor_options,
            false, /* not fixed result */
        );

        plancache_list = lappend(plancache_list, plansource as *mut c_void);
    });

    (*plan).plancache_list = plancache_list;
    (*plan).oneshot = false;

    /* Pop the error context stack */
    error_context_stack = spierrcontext.previous;
}

/*
 * _SPI_prepare_oneshot_plan
 *
 * Parse, but don't analyze, a querystring.
 *
 * This is a stripped-down version of _SPI_prepare_plan that only does the
 * initial raw parsing.  It creates "one shot" CachedPlanSources
 * that still require parse analysis before execution is possible.
 */
unsafe fn _SPI_prepare_oneshot_plan(src: *const c_char, plan: SPIPlanPtr) {
    let raw_parsetree_list: *mut List;
    let mut plancache_list: *mut List;
    let mut spicallbackarg: SPICallbackArg = SPICallbackArg {
        query: null(),
        mode: RAW_PARSE_DEFAULT,
    };
    let mut spierrcontext: ErrorContextCallback = zeroed();

    /* Setup error traceback support for ereport() */
    spicallbackarg.query = src;
    spicallbackarg.mode = core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode);
    spierrcontext.callback = _SPI_error_callback_trampoline;
    spierrcontext.arg = &mut spicallbackarg as *mut _ as *mut c_void;
    spierrcontext.previous = error_context_stack;
    error_context_stack = &mut spierrcontext;

    /* Parse the request string into a list of raw parse trees. */
    raw_parsetree_list = raw_parser(src, core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode));

    /*
     * Construct plancache entries, but don't do parse analysis yet.
     */
    plancache_list = NIL;

    foreach!(lc, raw_parsetree_list, {
        let parsetree: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, current_cell!(lc));
        let plansource: *mut CachedPlanSource;

        plansource = CreateOneShotCachedPlan(
            parsetree,
            src,
            CreateCommandTag((*parsetree).stmt as *mut c_void),
        );

        plancache_list = lappend(plancache_list, plansource as *mut c_void);
    });

    (*plan).plancache_list = plancache_list;
    (*plan).oneshot = true;

    /* Pop the error context stack */
    error_context_stack = spierrcontext.previous;
}

/*
 * _SPI_execute_plan: execute the given plan with the given options
 *
 * options contains options accessible from outside SPI:
 * params: parameter values to pass to query
 * read_only: true for read-only execution (no CommandCounterIncrement)
 * allow_nonatomic: true to allow nonatomic CALL/DO execution
 * must_return_tuples: throw error if query doesn't return tuples
 * tcount: execution tuple-count limit, or 0 for none
 * dest: DestReceiver to receive output, or NULL for normal SPI output
 * owner: ResourceOwner that will be used to hold refcount on plan;
 *		if NULL, CurrentResourceOwner is used (ignored for non-saved plan)
 *
 * Additional, only-internally-accessible options:
 * snapshot: query snapshot to use, or InvalidSnapshot for the normal
 *		behavior of taking a new snapshot for each query.
 * crosscheck_snapshot: for RI use, all others pass InvalidSnapshot
 * fire_triggers: true to fire AFTER triggers at end of query (normal case);
 *		false means any AFTER triggers are postponed to end of outer query
 */
unsafe fn _SPI_execute_plan(
    plan: SPIPlanPtr,
    options: *const SPIExecuteOptions,
    snapshot: Snapshot,
    crosscheck_snapshot: Snapshot,
    fire_triggers: bool,
) -> c_int {
    let mut my_res: c_int = 0;
    let mut my_processed: u64 = 0;
    let mut my_tuptable: *mut SPITupleTable = null_mut();
    let mut res: c_int = 0;
    let allow_nonatomic: bool;
    let mut pushed_active_snap: bool = false;
    let mut plan_owner: ResourceOwner = (*options).owner;
    let mut spicallbackarg: SPICallbackArg = SPICallbackArg {
        query: null(),
        mode: RAW_PARSE_DEFAULT,
    };
    let mut spierrcontext: ErrorContextCallback = zeroed();
    let mut cplan: *mut CachedPlan = null_mut();

    /*
     * We allow nonatomic behavior only if options->allow_nonatomic is set
     * *and* the SPI_OPT_NONATOMIC flag was given when connecting and we are
     * not inside a subtransaction.  The latter two tests match whether
     * _SPI_commit() would allow a commit; see there for more commentary.
     */
    allow_nonatomic = (*options).allow_nonatomic
        && !(*_SPI_current).atomic
        && !IsSubTransaction();

    /* Setup error traceback support for ereport() */
    spicallbackarg.query = null(); /* we'll fill this below */
    spicallbackarg.mode = core::mem::transmute::<c_int, RawParseMode>((*plan).parse_mode);
    spierrcontext.callback = _SPI_error_callback_trampoline;
    spierrcontext.arg = &mut spicallbackarg as *mut _ as *mut c_void;
    spierrcontext.previous = error_context_stack;
    error_context_stack = &mut spierrcontext;

    /*
     * We support four distinct snapshot management behaviors:
     *
     * snapshot != InvalidSnapshot, read_only = true: use exactly the given snapshot.
     *
     * snapshot != InvalidSnapshot, read_only = false: use the given snapshot,
     * modified by advancing its command ID before each querytree.
     *
     * snapshot == InvalidSnapshot, read_only = true: do nothing for queries
     * that require no snapshot.  For those that do, ensure that a Portal
     * snapshot exists; then use that, or use the entry-time ActiveSnapshot if
     * that exists and is different.
     *
     * snapshot == InvalidSnapshot, read_only = false: do nothing for queries
     * that require no snapshot.  For those that do, ensure that a Portal
     * snapshot exists; then, in atomic execution (!allow_nonatomic) take a
     * full new snapshot for each user command, and advance its command ID
     * before each querytree within the command.  In allow_nonatomic mode we
     * just use the Portal snapshot unmodified.
     *
     * In the first two cases, we can just push the snap onto the stack once
     * for the whole plan list.
     *
     * Note that snapshot != InvalidSnapshot implies an atomic execution context.
     */
    if snapshot != InvalidSnapshot {
        /* this intentionally tests the options field not the derived value */
        Assert!(!(*options).allow_nonatomic);
        if (*options).read_only {
            PushActiveSnapshot(snapshot);
            pushed_active_snap = true;
        } else {
            /* Make sure we have a private copy of the snapshot to modify */
            PushCopiedSnapshot(snapshot);
            pushed_active_snap = true;
        }
    }

    /*
     * Ensure that we have a resource owner if plan is saved, and not if it isn't.
     */
    if !(*plan).saved {
        plan_owner = null_mut();
    } else if plan_owner.is_null() {
        plan_owner = CurrentResourceOwner;
    }

    /*
     * We interpret must_return_tuples as "there must be at least one query,
     * and all of them must return tuples".  This is a bit laxer than
     * SPI_is_cursor_plan's check, but there seems no reason to enforce that
     * there be only one query.
     */
    if (*options).must_return_tuples && (*plan).plancache_list == NIL {
        ereport!(ERROR, errmsg!("empty query does not return tuples") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
    }

    'fail: {
        foreach!(lc1, (*plan).plancache_list, {
            let plansource: *mut CachedPlanSource =
                lfirst(current_cell!(lc1)) as *mut CachedPlanSource;
            let stmt_list: *mut List;

            spicallbackarg.query = (*plansource).query_string;

            /* If this is a one-shot plan, we still need to do parse analysis. */
            if (*plan).oneshot {
                /* TODO(pg-port): commands/prepare.rs CachedPlanSource stub lacks
                 * the raw_parse_tree field; restore (*plansource).raw_parse_tree
                 * once plancache.c lands. */
                let parsetree: *mut RawStmt = null_mut();
                let src: *const c_char = (*plansource).query_string;
                let querytree_list: *mut List;

                /*
                 * Parameter datatypes are driven by parserSetup hook if provided,
                 * otherwise we use the fixed parameter list.
                 */
                if parsetree.is_null() {
                    /* querytree_list = NIL; assigned below */
                    CompleteCachedPlan(
                        plansource,
                        NIL,
                        null_mut(),
                        (*plan).argtypes,
                        (*plan).nargs,
                        (*plan).parserSetup,
                        (*plan).parserSetupArg,
                        (*plan).cursor_options,
                        false,
                    );
                } else if !(*plan).parserSetup.is_none() {
                    Assert!((*plan).nargs == 0);
                    let ql = pg_analyze_and_rewrite_withcb(
                        parsetree,
                        src,
                        (*plan).parserSetup,
                        (*plan).parserSetupArg,
                        (*_SPI_current).queryEnv,
                    );
                    /* Finish filling in the CachedPlanSource */
                    CompleteCachedPlan(
                        plansource,
                        ql,
                        null_mut(),
                        (*plan).argtypes,
                        (*plan).nargs,
                        (*plan).parserSetup,
                        (*plan).parserSetupArg,
                        (*plan).cursor_options,
                        false,
                    );
                } else {
                    let ql = pg_analyze_and_rewrite_fixedparams(
                        parsetree,
                        src,
                        (*plan).argtypes,
                        (*plan).nargs,
                        (*_SPI_current).queryEnv,
                    );
                    /* Finish filling in the CachedPlanSource */
                    CompleteCachedPlan(
                        plansource,
                        ql,
                        null_mut(),
                        (*plan).argtypes,
                        (*plan).nargs,
                        (*plan).parserSetup,
                        (*plan).parserSetupArg,
                        (*plan).cursor_options,
                        false,
                    );
                }
            }

            /*
             * If asked to, complain when query does not return tuples.
             * (Replanning can't change this, so we can check it before that.
             * However, we can't check it till after parse analysis, so in the
             * case of a one-shot plan this is the earliest we could check.)
             */
            if (*options).must_return_tuples && (*plansource).resultDesc.is_null() {
                /* try to give a good error message */
                let cmdtag: *const c_char;

                /* A SELECT without resultDesc must be SELECT INTO */
                if plansource_commandtag(plansource) == CMDTAG_SELECT {
                    cmdtag = b"SELECT INTO\0".as_ptr() as *const c_char;
                } else {
                    cmdtag = GetCommandTagName(plansource_commandtag(plansource));
                }
                ereport!(ERROR, errmsg!("{} query does not return tuples",
                    core::ffi::CStr::from_ptr(cmdtag).to_string_lossy())
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
            }

            /*
             * Replan if needed, and increment plan refcount.  If it's a saved
             * plan, the refcount must be backed by the plan_owner.
             */
            cplan = GetCachedPlan(
                plansource,
                (*options).params,
                plan_owner,
                (*_SPI_current).queryEnv,
            );

            stmt_list = (*cplan).stmt_list;

            /*
             * If we weren't given a specific snapshot to use, and the statement
             * list requires a snapshot, set that up.
             */
            if snapshot == InvalidSnapshot
                && (list_length(stmt_list) > 1
                    || (list_length(stmt_list) == 1
                        && PlannedStmtRequiresSnapshot(
                            linitial_node!(PlannedStmt, T_PlannedStmt, stmt_list),
                        )))
            {
                /*
                 * First, ensure there's a Portal-level snapshot.  This back-fills
                 * the snapshot stack in case the previous operation was a COMMIT
                 * or ROLLBACK inside a procedure or DO block.
                 */
                EnsurePortalSnapshotExists();

                /*
                 * In the default non-read-only case, get a new per-statement-list
                 * snapshot, replacing any that we pushed in a previous cycle.
                 * Skip it when doing non-atomic execution.
                 */
                if !(*options).read_only && !allow_nonatomic {
                    if pushed_active_snap {
                        PopActiveSnapshot();
                    }
                    PushActiveSnapshot(GetTransactionSnapshot());
                    pushed_active_snap = true;
                }
            }

            foreach!(lc2, stmt_list, {
                let stmt: *mut PlannedStmt =
                    lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc2));
                let can_set_tag: bool = (*stmt).canSetTag;
                let dest: *mut DestReceiver;

                /*
                 * Reset output state.
                 */
                (*_SPI_current).processed = 0;
                (*_SPI_current).tuptable = null_mut();

                /* Check for unsupported cases. */
                if !(*stmt).utilityStmt.is_null() {
                    if IsA!((*stmt).utilityStmt, T_CopyStmt) {
                        let cstmt: *mut CopyStmt = (*stmt).utilityStmt as *mut CopyStmt;
                        if (*cstmt).filename.is_null() {
                            my_res = SPI_ERROR_COPY;
                            break 'fail;
                        }
                    } else if IsA!((*stmt).utilityStmt, T_TransactionStmt) {
                        my_res = SPI_ERROR_TRANSACTION;
                        break 'fail;
                    }
                }

                if (*options).read_only && !CommandIsReadOnly(stmt) {
                    ereport!(ERROR, errmsg!("{} is not allowed in a non-volatile function",
                        core::ffi::CStr::from_ptr(CreateCommandName(stmt as *mut c_void)).to_string_lossy())
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }

                /*
                 * If not read-only mode, advance the command counter before each
                 * command and update the snapshot.
                 */
                if !(*options).read_only && pushed_active_snap {
                    CommandCounterIncrement();
                    UpdateActiveSnapshotCommandId();
                }

                /*
                 * Select appropriate tuple receiver.  Output from non-canSetTag
                 * subqueries always goes to the bit bucket.
                 */
                if !can_set_tag {
                    dest = CreateDestReceiver(DestNone);
                } else if !(*options).dest.is_null() {
                    dest = (*options).dest;
                } else {
                    dest = CreateDestReceiver(DestSPI);
                }

                if (*stmt).utilityStmt.is_null() {
                    let qdesc: *mut QueryDesc;
                    let snap: Snapshot;

                    if ActiveSnapshotSet() {
                        snap = GetActiveSnapshot();
                    } else {
                        snap = InvalidSnapshot;
                    }

                    qdesc = CreateQueryDesc(
                        stmt,
                        (*plansource).query_string,
                        snap,
                        crosscheck_snapshot,
                        dest,
                        (*options).params,
                        (*_SPI_current).queryEnv,
                        0,
                    );
                    res = _SPI_pquery(
                        qdesc,
                        fire_triggers,
                        if can_set_tag { (*options).tcount } else { 0 },
                    );
                    FreeQueryDesc(qdesc);
                } else {
                    let context: ProcessUtilityContext;
                    let mut qc: QueryCompletion = zeroed();

                    /*
                     * If we're not allowing nonatomic operations, tell
                     * ProcessUtility this is an atomic execution context.
                     */
                    if allow_nonatomic {
                        context = PROCESS_UTILITY_QUERY_NONATOMIC;
                    } else {
                        context = PROCESS_UTILITY_QUERY;
                    }

                    InitializeQueryCompletion(&mut qc);
                    ProcessUtility(
                        stmt,
                        (*plansource).query_string,
                        true, /* protect plancache's node tree */
                        context,
                        (*options).params,
                        (*_SPI_current).queryEnv,
                        dest,
                        &mut qc,
                    );

                    /* Update "processed" if stmt returned tuples */
                    if !(*_SPI_current).tuptable.is_null() {
                        (*_SPI_current).processed =
                            (*((*_SPI_current).tuptable as *mut SPITupleTable)).numvals;
                    }

                    res = SPI_OK_UTILITY;

                    /*
                     * Some utility statements return a row count, even though the
                     * tuples are not returned to the caller.
                     */
                    if IsA!((*stmt).utilityStmt, T_CreateTableAsStmt) {
                        let ctastmt: *mut CreateTableAsStmt =
                            (*stmt).utilityStmt as *mut CreateTableAsStmt;

                        if qc.commandTag == CMDTAG_SELECT {
                            (*_SPI_current).processed = qc.nprocessed;
                        } else {
                            /*
                             * Must be an IF NOT EXISTS that did nothing, or a
                             * CREATE ... WITH NO DATA.
                             */
                            Assert!((*ctastmt).if_not_exists || (*(*ctastmt).into).skipData);
                            (*_SPI_current).processed = 0;
                        }

                        /*
                         * For historical reasons, if CREATE TABLE AS was spelled
                         * as SELECT INTO, return a special return code.
                         */
                        if (*ctastmt).is_select_into {
                            res = SPI_OK_SELINTO;
                        }
                    } else if IsA!((*stmt).utilityStmt, T_CopyStmt) {
                        Assert!(qc.commandTag == CMDTAG_COPY);
                        (*_SPI_current).processed = qc.nprocessed;
                    }
                }

                /*
                 * The last canSetTag query sets the status values returned to the
                 * caller.  Be careful to free any tuptables not returned, to
                 * avoid intra-transaction memory leak.
                 */
                if can_set_tag {
                    my_processed = (*_SPI_current).processed;
                    SPI_freetuptable(my_tuptable);
                    my_tuptable = (*_SPI_current).tuptable as *mut SPITupleTable;
                    my_res = res;
                } else {
                    SPI_freetuptable((*_SPI_current).tuptable as *mut SPITupleTable);
                    (*_SPI_current).tuptable = null_mut();
                }

                /*
                 * We don't issue a destroy call to the receiver.  The SPI and
                 * None receivers would ignore it anyway, while if the caller
                 * supplied a receiver, it's not our job to destroy it.
                 */

                if res < 0 {
                    my_res = res;
                    break 'fail;
                }
            }); /* foreach lc2 */

            /* Done with this plan, so release refcount */
            ReleaseCachedPlan(cplan, plan_owner);
            cplan = null_mut();

            /*
             * If not read-only mode, advance the command counter after the last
             * command.  This ensures that its effects are visible, in case it was
             * DDL that would affect the next CachedPlanSource.
             */
            if !(*options).read_only {
                CommandCounterIncrement();
            }
        }); /* foreach lc1 */
    } /* 'fail block */

    /* Pop the snapshot off the stack if we pushed one */
    if pushed_active_snap {
        PopActiveSnapshot();
    }

    /* We no longer need the cached plan refcount, if any */
    if !cplan.is_null() {
        ReleaseCachedPlan(cplan, plan_owner);
    }

    /* Pop the error context stack */
    error_context_stack = spierrcontext.previous;

    /* Save results for caller */
    SPI_processed = my_processed;
    SPI_tuptable = my_tuptable;

    /* tuptable now is caller's responsibility, not SPI's */
    (*_SPI_current).tuptable = null_mut();

    /*
     * If none of the queries had canSetTag, return SPI_OK_REWRITTEN. Prior to
     * 8.4, we used return the last query's result code, but not its auxiliary
     * results, but that's confusing.
     */
    if my_res == 0 {
        my_res = SPI_OK_REWRITTEN;
    }

    my_res
}

/*
 * Convert arrays of query parameters to form wanted by planner and executor
 */
unsafe fn _SPI_convert_params(
    nargs: c_int,
    argtypes: *mut Oid,
    Values: *mut Datum,
    Nulls: *const c_char,
) -> ParamListInfo {
    let param_li: ParamListInfo;

    if nargs > 0 {
        param_li = makeParamList(nargs);

        for i in 0..nargs {
            let prm: *mut ParamExternData = &mut (*param_li).params[i as usize];

            (*prm).value = *Values.add(i as usize);
            (*prm).isnull = !Nulls.is_null() && *Nulls.add(i as usize) == b'n' as c_char;
            (*prm).pflags = PARAM_FLAG_CONST as u16;
            (*prm).ptype = *argtypes.add(i as usize);
        }
    } else {
        param_li = null_mut();
    }
    param_li
}

unsafe fn _SPI_pquery(queryDesc: *mut QueryDesc, fire_triggers: bool, tcount: u64) -> c_int {
    use crate::nodes::nodes::CmdType;
    let operation: CmdType = (*queryDesc).operation;
    let eflags: c_int;
    let res: c_int;

    res = match operation {
        CmdType::CMD_SELECT => {
            if (*(*queryDesc).dest).mydest == DestNone {
                /* Don't return SPI_OK_SELECT if we're discarding result */
                SPI_OK_UTILITY
            } else {
                SPI_OK_SELECT
            }
        }
        CmdType::CMD_INSERT => {
            if (*(*queryDesc).plannedstmt).hasReturning {
                SPI_OK_INSERT_RETURNING
            } else {
                SPI_OK_INSERT
            }
        }
        CmdType::CMD_DELETE => {
            if (*(*queryDesc).plannedstmt).hasReturning {
                SPI_OK_DELETE_RETURNING
            } else {
                SPI_OK_DELETE
            }
        }
        CmdType::CMD_UPDATE => {
            if (*(*queryDesc).plannedstmt).hasReturning {
                SPI_OK_UPDATE_RETURNING
            } else {
                SPI_OK_UPDATE
            }
        }
        CmdType::CMD_MERGE => {
            if (*(*queryDesc).plannedstmt).hasReturning {
                SPI_OK_MERGE_RETURNING
            } else {
                SPI_OK_MERGE
            }
        }
        _ => return SPI_ERROR_OPUNKNOWN,
    };

    /* Select execution options */
    if fire_triggers {
        eflags = 0; /* default run-to-completion flags */
    } else {
        eflags = EXEC_FLAG_SKIP_TRIGGERS();
    }

    ExecutorStart(queryDesc, eflags);

    ExecutorRun(queryDesc, ForwardScanDirection as i32, tcount);

    (*_SPI_current).processed = (*(*queryDesc).estate).es_processed;

    if (res == SPI_OK_SELECT || (*(*queryDesc).plannedstmt).hasReturning)
        && (*(*queryDesc).dest).mydest == DestSPI
    {
        if _SPI_checktuples() {
            elog!(ERROR, "consistency check on SPI tuple count failed");
        }
    }

    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);
    /* FreeQueryDesc is done by the caller */

    res
}

/*
 * _SPI_error_callback
 *
 * Add context information when a query invoked via SPI fails
 */
unsafe fn _SPI_error_callback(arg: *mut c_void) {
    let carg: *mut SPICallbackArg = arg as *mut SPICallbackArg;
    let query: *const c_char = (*carg).query;
    let syntaxerrposition: c_int;

    if query.is_null() {
        /* in case arg wasn't set yet */
        return;
    }

    /*
     * If there is a syntax error position, convert to internal syntax error;
     * otherwise treat the query as an item of context stack
     */
    syntaxerrposition = geterrposition();
    if syntaxerrposition > 0 {
        errposition(0);
        internalerrposition(syntaxerrposition);
        internalerrquery(query);
    } else {
        /* Use the parse mode to decide how to describe the query */
        match (*carg).mode {
            RAW_PARSE_PLPGSQL_EXPR => {
                errcontext_msg(
                    b"PL/pgSQL expression \"%s\"\0".as_ptr() as *const c_char,
                );
                /* C also: errcontext("PL/pgSQL expression \"%s\"", query) */
            }
            RAW_PARSE_PLPGSQL_ASSIGN1
            | RAW_PARSE_PLPGSQL_ASSIGN2
            | RAW_PARSE_PLPGSQL_ASSIGN3 => {
                errcontext_msg(
                    b"PL/pgSQL assignment \"%s\"\0".as_ptr() as *const c_char,
                );
                /* C also: errcontext("PL/pgSQL assignment \"%s\"", query) */
            }
            _ => {
                errcontext_msg(
                    b"SQL statement \"%s\"\0".as_ptr() as *const c_char,
                );
                /* C also: errcontext("SQL statement \"%s\"", query) */
            }
        }
    }
}

/*
 * _SPI_cursor_operation()
 *
 *	Do a FETCH or MOVE in a cursor
 */
unsafe fn _SPI_cursor_operation(
    portal: Portal,
    direction: FetchDirection,
    count: c_long,
    dest: *mut DestReceiver,
) {
    let nfetched: u64;

    /* Check that the portal is valid */
    if !PortalIsValid(portal) {
        elog!(ERROR, "invalid portal in SPI cursor operation");
    }

    /* Push the SPI stack */
    if _SPI_begin_call(true) < 0 {
        elog!(ERROR, "SPI cursor operation called while not connected");
    }

    /* Reset the SPI result (note we deliberately don't touch lastoid) */
    SPI_processed = 0;
    SPI_tuptable = null_mut();
    (*_SPI_current).processed = 0;
    (*_SPI_current).tuptable = null_mut();

    /* Run the cursor */
    nfetched = PortalRunFetch(portal, direction, count, dest);

    /*
     * Think not to combine this store with the preceding function call. If
     * the portal contains calls to functions that use SPI, then _SPI_stack is
     * likely to move around while the portal runs.  When control returns,
     * _SPI_current will point to the correct stack entry... but the pointer
     * may be different than it was beforehand. So we must be sure to re-fetch
     * the pointer after the function call completes.
     */
    (*_SPI_current).processed = nfetched;

    if (*dest).mydest == DestSPI && _SPI_checktuples() {
        elog!(ERROR, "consistency check on SPI tuple count failed");
    }

    /* Put the result into place for access by caller */
    SPI_processed = (*_SPI_current).processed;
    SPI_tuptable = (*_SPI_current).tuptable as *mut SPITupleTable;

    /* tuptable now is caller's responsibility, not SPI's */
    (*_SPI_current).tuptable = null_mut();

    /* Pop the SPI stack */
    _SPI_end_call(true);
}

unsafe fn _SPI_execmem() -> MemoryContext {
    MemoryContextSwitchTo((*_SPI_current).execCxt)
}

unsafe fn _SPI_procmem() -> MemoryContext {
    MemoryContextSwitchTo((*_SPI_current).procCxt)
}

/*
 * _SPI_begin_call: begin a SPI operation within a connected procedure
 *
 * use_exec is true if we intend to make use of the procedure's execCxt
 * during this SPI operation.  We'll switch into that context, and arrange
 * for it to be cleaned up at _SPI_end_call or if an error occurs.
 */
unsafe fn _SPI_begin_call(use_exec: bool) -> c_int {
    if _SPI_current.is_null() {
        return SPI_ERROR_UNCONNECTED;
    }

    if use_exec {
        /* remember when the Executor operation started */
        (*_SPI_current).execSubid = GetCurrentSubTransactionId();
        /* switch to the Executor memory context */
        _SPI_execmem();
    }

    0
}

/*
 * _SPI_end_call: end a SPI operation within a connected procedure
 *
 * use_exec must be the same as in the previous _SPI_begin_call
 *
 * Note: this currently has no failure return cases, so callers don't check
 */
unsafe fn _SPI_end_call(use_exec: bool) -> c_int {
    if use_exec {
        /* switch to the procedure memory context */
        _SPI_procmem();
        /* mark Executor context no longer in use */
        (*_SPI_current).execSubid = InvalidSubTransactionId;
        /* and free Executor memory */
        MemoryContextReset((*_SPI_current).execCxt);
    }

    0
}

unsafe fn _SPI_checktuples() -> bool {
    let processed: u64 = (*_SPI_current).processed;
    let tuptable: *mut SPITupleTable = (*_SPI_current).tuptable as *mut SPITupleTable;
    let mut failed: bool = false;

    if tuptable.is_null() {
        /* spi_dest_startup was not called */
        failed = true;
    } else if processed != (*tuptable).numvals {
        failed = true;
    }

    failed
}

/*
 * Convert a "temporary" SPIPlan into an "unsaved" plan.
 *
 * The passed _SPI_plan struct is on the stack, and all its subsidiary data
 * is in or under the current SPI executor context.  Copy the plan into the
 * SPI procedure context so it will survive _SPI_end_call().  To minimize
 * data copying, this destructively modifies the input plan, by taking the
 * plancache entries away from it and reparenting them to the new SPIPlan.
 */
unsafe fn _SPI_make_plan_non_temp(plan: SPIPlanPtr) -> SPIPlanPtr {
    let newplan: SPIPlanPtr;
    let parentcxt: MemoryContext = (*_SPI_current).procCxt;
    let plancxt: MemoryContext;
    let mut oldcxt: MemoryContext;

    /* Assert the input is a temporary SPIPlan */
    Assert!((*plan).magic == _SPI_PLAN_MAGIC);
    Assert!((*plan).plancxt.is_null());
    /* One-shot plans can't be saved */
    Assert!(!(*plan).oneshot);

    /*
     * Create a memory context for the plan, underneath the procedure context.
     * We don't expect the plan to be very large.
     */
    plancxt = AllocSetContextCreate!(
        parentcxt,
        c"SPI Plan".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(plancxt);

    /* Copy the _SPI_plan struct and subsidiary data into the new context */
    newplan = palloc0(size_of::<_SPI_plan>()) as SPIPlanPtr;
    (*newplan).magic = _SPI_PLAN_MAGIC;
    (*newplan).plancxt = plancxt;
    (*newplan).parse_mode = (*plan).parse_mode;
    (*newplan).cursor_options = (*plan).cursor_options;
    (*newplan).nargs = (*plan).nargs;
    if (*plan).nargs > 0 {
        (*newplan).argtypes = palloc((*plan).nargs as usize * size_of::<Oid>()) as *mut Oid;
        core::ptr::copy_nonoverlapping(
            (*plan).argtypes,
            (*newplan).argtypes,
            (*plan).nargs as usize,
        );
    } else {
        (*newplan).argtypes = null_mut();
    }
    (*newplan).parserSetup = (*plan).parserSetup;
    (*newplan).parserSetupArg = (*plan).parserSetupArg;

    /*
     * Reparent all the CachedPlanSources into the procedure context.  In
     * theory this could fail partway through due to the pallocs, but we don't
     * care too much since both the procedure context and the executor context
     * would go away on error.
     */
    foreach!(lc, (*plan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;

        CachedPlanSetParentContext(plansource, parentcxt);

        /* Build new list, with list cells in plancxt */
        (*newplan).plancache_list = lappend((*newplan).plancache_list, plansource as *mut c_void);
    });

    MemoryContextSwitchTo(oldcxt);

    /* For safety, unlink the CachedPlanSources from the temporary plan */
    (*plan).plancache_list = NIL;

    newplan
}

/*
 * Make a "saved" copy of the given plan.
 */
unsafe fn _SPI_save_plan(plan: SPIPlanPtr) -> SPIPlanPtr {
    let newplan: SPIPlanPtr;
    let plancxt: MemoryContext;
    let mut oldcxt: MemoryContext;

    /* One-shot plans can't be saved */
    Assert!(!(*plan).oneshot);

    /*
     * Create a memory context for the plan.  We don't expect the plan to be
     * very large, so use smaller-than-default alloc parameters.  It's a
     * transient context until we finish copying everything.
     */
    plancxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SPI Plan".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(plancxt);

    /* Copy the SPI plan into its own context */
    newplan = palloc0(size_of::<_SPI_plan>()) as SPIPlanPtr;
    (*newplan).magic = _SPI_PLAN_MAGIC;
    (*newplan).plancxt = plancxt;
    (*newplan).parse_mode = (*plan).parse_mode;
    (*newplan).cursor_options = (*plan).cursor_options;
    (*newplan).nargs = (*plan).nargs;
    if (*plan).nargs > 0 {
        (*newplan).argtypes = palloc((*plan).nargs as usize * size_of::<Oid>()) as *mut Oid;
        core::ptr::copy_nonoverlapping(
            (*plan).argtypes,
            (*newplan).argtypes,
            (*plan).nargs as usize,
        );
    } else {
        (*newplan).argtypes = null_mut();
    }
    (*newplan).parserSetup = (*plan).parserSetup;
    (*newplan).parserSetupArg = (*plan).parserSetupArg;

    /* Copy all the plancache entries */
    foreach!(lc, (*plan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;
        let newsource: *mut CachedPlanSource;

        newsource = CopyCachedPlan(plansource);
        (*newplan).plancache_list = lappend((*newplan).plancache_list, newsource as *mut c_void);
    });

    MemoryContextSwitchTo(oldcxt);

    /*
     * Mark it saved, reparent it under CacheMemoryContext, and mark all the
     * component CachedPlanSources as saved.  This sequence cannot fail
     * partway through, so there's no risk of long-term memory leakage.
     */
    (*newplan).saved = true;
    MemoryContextSetParent((*newplan).plancxt, CacheMemoryContext());

    foreach!(lc, (*newplan).plancache_list, {
        let plansource: *mut CachedPlanSource = lfirst(current_cell!(lc)) as *mut CachedPlanSource;

        SaveCachedPlan(plansource);
    });

    newplan
}

/*
 * Internal lookup of ephemeral named relation by name.
 */
unsafe fn _SPI_find_ENR_by_name(name: *const c_char) -> EphemeralNamedRelation {
    /* internal static function; any error is bug in SPI itself */
    Assert!(!name.is_null());

    /* fast exit if no tuplestores have been added */
    if (*_SPI_current).queryEnv.is_null() {
        return null_mut();
    }

    get_ENR((*_SPI_current).queryEnv, name)
}

/*
 * Register an ephemeral named relation for use by the planner and executor on
 * subsequent calls using this SPI connection.
 */
pub unsafe fn SPI_register_relation(enr: EphemeralNamedRelation) -> c_int {
    let r#match: EphemeralNamedRelation;
    let mut res: c_int;

    if enr.is_null() || (*enr).md.name.is_null() {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(false); /* keep current memory context */
    if res < 0 {
        return res;
    }

    r#match = _SPI_find_ENR_by_name((*enr).md.name);
    if !r#match.is_null() {
        res = SPI_ERROR_REL_DUPLICATE;
    } else {
        if (*_SPI_current).queryEnv.is_null() {
            (*_SPI_current).queryEnv = create_queryEnv();
        }

        register_ENR((*_SPI_current).queryEnv, enr);
        res = SPI_OK_REL_REGISTER;
    }

    _SPI_end_call(false);

    res
}

/*
 * Unregister an ephemeral named relation by name.  This will probably be a
 * rarely used function, since SPI_finish will clear it automatically.
 */
pub unsafe fn SPI_unregister_relation(name: *const c_char) -> c_int {
    let r#match: EphemeralNamedRelation;
    let mut res: c_int;

    if name.is_null() {
        return SPI_ERROR_ARGUMENT;
    }

    res = _SPI_begin_call(false); /* keep current memory context */
    if res < 0 {
        return res;
    }

    r#match = _SPI_find_ENR_by_name(name);
    if !r#match.is_null() {
        unregister_ENR((*_SPI_current).queryEnv, (*r#match).md.name);
        res = SPI_OK_REL_UNREGISTER;
    } else {
        res = SPI_ERROR_REL_NOT_FOUND;
    }

    _SPI_end_call(false);

    res
}

/*
 * Register the transient relations from 'tdata' using this SPI connection.
 * This should be called by PL implementations' trigger handlers after
 * connecting, in order to make transition tables visible to any queries run
 * in this connection.
 */
pub unsafe fn SPI_register_trigger_data(tdata: *mut TriggerData) -> c_int {
    /* TODO(pg-port): Trigger struct is opaque; minimal layout stub for tgnewtable/tgoldtable */
    #[repr(C)]
    struct TriggerLayout {
        tgrelid: Oid,
        tgparentid: Oid,
        tgname: *mut c_char,
        tgfoid: Oid,
        tgtype: i16,
        tgenabled: c_char,
        tgisinternal: bool,
        tgisclone: bool,
        tgconstrrelid: Oid,
        tgconstrindid: Oid,
        tgconstraint: Oid,
        tgdeferrable: bool,
        tginitdeferred: bool,
        tgnargs: i16,
        tgnattr: i16,
        tgattr: *mut c_void,
        tgargs: *mut *mut c_char,
        tgqual: *mut c_char,
        tgoldtable: *mut c_char,
        tgnewtable: *mut c_char,
    }

    if tdata.is_null() {
        return SPI_ERROR_ARGUMENT;
    }

    if !(*tdata).tg_newtable.is_null() {
        let enr: EphemeralNamedRelation =
            palloc(size_of::<EphemeralNamedRelationData>()) as EphemeralNamedRelation;
        let rc: c_int;
        let trig: *const TriggerLayout = (*tdata).tg_trigger as *const TriggerLayout;

        (*enr).md.name = (*trig).tgnewtable;
        (*enr).md.reliddesc = (*(*tdata).tg_relation).rd_id;
        (*enr).md.tupdesc = null_mut();
        (*enr).md.enrtype = ENR_NAMED_TUPLESTORE;
        (*enr).md.enrtuples =
            tuplestore_tuple_count((*tdata).tg_newtable as *mut c_void) as f64;
        (*enr).reldata = (*tdata).tg_newtable as *mut c_void;
        rc = SPI_register_relation(enr);
        if rc != SPI_OK_REL_REGISTER {
            return rc;
        }
    }

    if !(*tdata).tg_oldtable.is_null() {
        let enr: EphemeralNamedRelation =
            palloc(size_of::<EphemeralNamedRelationData>()) as EphemeralNamedRelation;
        let rc: c_int;
        let trig: *const TriggerLayout = (*tdata).tg_trigger as *const TriggerLayout;

        (*enr).md.name = (*trig).tgoldtable;
        (*enr).md.reliddesc = (*(*tdata).tg_relation).rd_id;
        (*enr).md.tupdesc = null_mut();
        (*enr).md.enrtype = ENR_NAMED_TUPLESTORE;
        (*enr).md.enrtuples =
            tuplestore_tuple_count((*tdata).tg_oldtable as *mut c_void) as f64;
        (*enr).reldata = (*tdata).tg_oldtable as *mut c_void;
        rc = SPI_register_relation(enr);
        if rc != SPI_OK_REL_REGISTER {
            return rc;
        }
    }

    SPI_OK_TD_REGISTER
}

/*-------------------------------------------------------------------------
 *
 * functions.c
 *    Execution of SQL-language functions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/functions.c
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
#![allow(unreachable_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use crate::list_make1;
use crate::nodes::pg_list::{List, NIL, lfirst, lappend, list_length, list_nth,
    ListCell};
use crate::nodes::nodes::{Node, NodeTag, CmdType};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::primnodes::{Param, ParamKind, RangeTblRef};
use crate::nodes::parsenodes::{ColumnRef, ParamRef, A_Star, RangeTblEntry,
    RTEKind};
use crate::nodes::value::String as PgString;
use crate::nodes::parsenodes::{Query, RawStmt, CallStmt};
use crate::nodes::primnodes::TargetEntry;
use crate::nodes::plannodes::PlannedStmt;
use crate::nodes::execnodes::{
    EState, ExprContext, TupleTableSlot, JunkFilter, ReturnSetInfo,
    Tuplestorestate,
};
use crate::executor::tuptable::TTS_EMPTY;
use crate::nodes::execnodes::{ExprMultipleResult, ExprEndResult};
use crate::nodes::execnodes::{SFRM_ValuePerCall, SFRM_Materialize,
    SFRM_Materialize_Random, SFRM_Materialize_Preferred};
use crate::executor::execdesc::{QueryDesc, CreateQueryDesc, FreeQueryDesc};
use crate::executor::execExpr::ExecInitJunkFilter;
use crate::executor::execJunk::{ExecInitJunkFilterConversion, ExecFilterJunk};
use crate::executor::execUtils::ExecCleanTargetListLength;
use crate::executor::tuptable::{ExecClearTuple, ExecMaterializeSlot};
use crate::executor::execMain::{
    ExecutorStart, ExecutorRun, ExecutorFinish, ExecutorEnd,
};
use crate::executor::executor::EXEC_FLAG_SKIP_TRIGGERS;
use crate::executor::execSRF::ExecFetchSlotHeapTupleDatum;
use crate::access::htup_details::{HeapTuple, HeapTupleData};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr,
    TupleDescCompactAttr, CreateTupleDescCopy};
use crate::access::transam::xact::{
    ResourceOwner, CurrentResourceOwner,
    CommandCounterIncrement,
};
use crate::tcop::dest::{DestReceiver, CommandDest, None_Receiver,
    CreateDestReceiver};
use crate::tcop::dest::CommandDest::*;
use crate::tcop::utility::ProcessUtility;
use crate::utils::cache::funccache::{
    CachedFunction, CachedFunctionHashKey,
    cached_function_compile,
};
use crate::utils::cache::syscache::{
    SysCacheGetAttr, SysCacheGetAttrNotNull, SearchSysCache1, ReleaseSysCache,
};
use crate::utils::cache::lsyscache::{
    get_typlenbyval, get_typlen, get_typcollation, get_typtype,
    type_is_rowtype,
};
use crate::utils::fmgr::get_call_expr_argtype;
// MemoryContext lifecycle: most symbols come from the prelude; the few not
// re-exported there are pulled directly from their mcxt home.
use crate::utils::mmgr::mcxt::{
    MemoryContextStrdup, MemoryContextSetParent,
    MemoryContextRegisterResetCallback, CacheMemoryContext,
};
use crate::utils::palloc::{
    palloc, palloc0, pfree, pstrdup, repalloc,
    MemoryContextCallback,
};
use crate::nodes::params::{
    ParamListInfo, ParamListInfoData, ParamExternData,
    PARAM_FLAG_CONST, makeParamList, ParseState,
};
use crate::utils::fmgr::{
    FmgrInfo, FunctionCallInfo,
};
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::expandeddatum::MakeExpandedObjectReadOnlyInternal;
use crate::utils::builtins::{format_type_be, TextDatumGetCString};
use crate::catalog::pg_proc::{Form_pg_proc,
    PROVOLATILE_VOLATILE, PROKIND_PROCEDURE};
use crate::commands::prepare::{CachedPlanSource, CachedPlan};
use crate::tcop::tcopprot::pg_analyze_and_rewrite_withcb;
use crate::nodes::params::ParserSetupHook;
use crate::parser::parse_func::{ParseFuncOrColumn};
use crate::parser::parse_coerce::coerce_to_target_type;
use crate::nodes::primnodes::{COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST};
use crate::parser::parse_collate::assign_expr_collations;
use crate::rewrite::rewriteHandler::AcquireRewriteLocks;
use crate::tcop::tcopprot::pg_rewrite_query;
use crate::nodes::makefuncs::{
    makeConst, makeTargetEntry, makeAlias,
    makeVarFromTargetEntry, makeFromExpr,
};
use crate::nodes::value::makeString;
use crate::nodes::nodeFuncs::exprType;
use crate::executor::execExpr::ExecBuildAggTrans;
use crate::nodes::primnodes::Expr;
use crate::catalog::pg_attribute::FormData_pg_attribute;
use crate::access::attnum::AttrNumber;
use crate::pg_config_manual::FUNC_MAX_ARGS;

// catalog/pg_proc.h attribute numbers (not yet pub in catalog::pg_proc).
pub const Anum_pg_proc_proargmodes: AttrNumber = 20;
pub const Anum_pg_proc_proargnames: AttrNumber = 22;
pub const Anum_pg_proc_prosrc: AttrNumber = 29;
pub const Anum_pg_proc_prosqlbody: AttrNumber = 35;

// utils/cache/lsyscache.h: get_func_input_arg_names / IsPolymorphicType,
// and fmgr.h get_call_result_type -- not yet pub in their canonical homes.
unsafe fn get_func_input_arg_names(
    _proargnames: Datum,
    _proargmodes: Datum,
    _arg_names: *mut *mut *mut c_char,
) -> c_int {
    todo!("TODO(pg-port): get_func_input_arg_names")
}
unsafe fn IsPolymorphicType(_typid: Oid) -> bool {
    todo!("TODO(pg-port): IsPolymorphicType")
}
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    todo!("TODO(pg-port): get_call_result_type")
}

// utils/time/snapmgr.h -- no snapmgr.rs yet; minimal local stubs.
unsafe fn GetTransactionSnapshot() -> Snapshot {
    todo!("TODO(pg-port): GetTransactionSnapshot")
}
unsafe fn PushActiveSnapshot(_snapshot: Snapshot) {
    todo!("TODO(pg-port): PushActiveSnapshot")
}
unsafe fn PopActiveSnapshot() {
    todo!("TODO(pg-port): PopActiveSnapshot")
}
unsafe fn UpdateActiveSnapshotCommandId() {
    todo!("TODO(pg-port): UpdateActiveSnapshotCommandId")
}
unsafe fn ActiveSnapshotSet() -> bool {
    todo!("TODO(pg-port): ActiveSnapshotSet")
}

// utils/expandeddatum.h MakeExpandedObjectReadOnly() macro.
#[inline]
unsafe fn MakeExpandedObjectReadOnly(d: Datum, isnull: bool, typlen: c_int) -> Datum {
    if typlen == -1 && !isnull {
        MakeExpandedObjectReadOnlyInternal(d)
    } else {
        d
    }
}

// TODO(pg-port): parser/parse_node.h full ParseState with hooks
use crate::nodes::params::ParseState as PgParseState;

// TODO(pg-port): utils/plancache.h
use crate::commands::prepare::CachedPlanSource as CPS;
pub unsafe fn GetCachedPlan(
    _plansource: *mut CachedPlanSource,
    _params: ParamListInfo,
    _owner: ResourceOwner,
    _qc: *mut c_void,
) -> *mut CachedPlan {
    todo!("TODO(pg-port): GetCachedPlan")
}
pub unsafe fn ReleaseCachedPlan(_plan: *mut CachedPlan, _owner: ResourceOwner) {
    todo!("TODO(pg-port): ReleaseCachedPlan")
}
pub unsafe fn CreateCachedPlan(
    _raw_parse_tree: *mut RawStmt,
    _query_string: *const c_char,
    _commandTag: *mut c_void,
) -> *mut CachedPlanSource {
    todo!("TODO(pg-port): CreateCachedPlan")
}
pub unsafe fn CreateCachedPlanForQuery(
    _parsetree: *mut Query,
    _query_string: *const c_char,
    _commandTag: *mut c_void,
) -> *mut CachedPlanSource {
    todo!("TODO(pg-port): CreateCachedPlanForQuery")
}
pub unsafe fn CompleteCachedPlan(
    _plansource: *mut CachedPlanSource,
    _querytree_list: *mut List,
    _query_context: MemoryContext,
    _param_types: *mut Oid,
    _num_params: c_int,
    _parserSetup: Option<ParserSetupHook>,
    _parserSetupArg: *mut c_void,
    _cursor_options: c_int,
    _fixed_result: bool,
) {
    todo!("TODO(pg-port): CompleteCachedPlan")
}
pub unsafe fn SaveCachedPlan(_plansource: *mut CachedPlanSource) {
    todo!("TODO(pg-port): SaveCachedPlan")
}
pub unsafe fn DropCachedPlan(_plansource: *mut CachedPlanSource) {
    todo!("TODO(pg-port): DropCachedPlan")
}
pub unsafe fn SetPostRewriteHook(
    _plansource: *mut CachedPlanSource,
    _hook: Option<unsafe fn(*mut List, *mut c_void)>,
    _arg: *mut c_void,
) {
    todo!("TODO(pg-port): SetPostRewriteHook")
}

// TODO(pg-port): utils/snapmgr.h
pub unsafe fn GetActiveSnapshot() -> Snapshot {
    todo!("TODO(pg-port): GetActiveSnapshot")
}

// TODO(pg-port): catalog/pg_proc.h GETSTRUCT
pub unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut c_void {
    todo!("TODO(pg-port): GETSTRUCT")
}

// TODO(pg-port): nodes/makefuncs.h
pub unsafe fn copyObject<T>(_obj: *mut T) -> *mut T {
    todo!("TODO(pg-port): copyObject")
}
pub unsafe fn stringToNode(_str: *mut c_char) -> *mut Node {
    todo!("TODO(pg-port): stringToNode")
}
pub unsafe fn pg_parse_query(_query_string: *const c_char) -> *mut List {
    todo!("TODO(pg-port): pg_parse_query")
}
pub unsafe fn CreateCommandTag(_node: *mut Node) -> *mut c_void {
    todo!("TODO(pg-port): CreateCommandTag")
}
pub unsafe fn CreateCommandName(_node: *mut c_void) -> *const c_char {
    todo!("TODO(pg-port): CreateCommandName")
}
pub unsafe fn CommandIsReadOnly(_stmt: *mut PlannedStmt) -> bool {
    todo!("TODO(pg-port): CommandIsReadOnly")
}
pub unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) {
    todo!("TODO(pg-port): BlessTupleDesc")
}
pub unsafe fn slot_getattr(
    _slot: *mut TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum {
    todo!("TODO(pg-port): slot_getattr")
}
pub unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: TupleDesc,
    _ops: *const c_void,
) -> *mut TupleTableSlot {
    todo!("TODO(pg-port): MakeSingleTupleTableSlot")
}
pub static TTSOpsMinimalTuple: c_int = 0;
pub unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    todo!("TODO(pg-port): tuplestore_begin_heap")
}
pub unsafe fn tuplestore_puttupleslot(
    _state: *mut Tuplestorestate,
    _slot: *mut TupleTableSlot,
) {
    todo!("TODO(pg-port): tuplestore_puttupleslot")
}
pub unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    todo!("TODO(pg-port): tuplestore_end")
}
pub unsafe fn RegisterExprContextCallback(
    _econtext: *mut ExprContext,
    _func: Option<unsafe fn(Datum)>,
    _arg: Datum,
) {
    todo!("TODO(pg-port): RegisterExprContextCallback")
}
pub unsafe fn UnregisterExprContextCallback(
    _econtext: *mut ExprContext,
    _func: Option<unsafe fn(Datum)>,
    _arg: Datum,
) {
    todo!("TODO(pg-port): UnregisterExprContextCallback")
}
pub unsafe fn DatumGetPointer(_d: Datum) -> *mut c_void {
    _d as *mut c_void
}
pub unsafe fn PointerGetDatum(_p: *mut c_void) -> Datum {
    _p as Datum
}
pub unsafe fn geterrposition() -> c_int { 0 }
pub unsafe fn errposition(_pos: c_int) {}
pub unsafe fn internalerrposition(_pos: c_int) {}
pub unsafe fn internalerrquery(_query: *const c_char) {}
pub unsafe fn repalloc_array<T>(_ptr: *mut T, _n: usize) -> *mut T {
    todo!("TODO(pg-port): repalloc_array")
}
// TODO(pg-port): work_mem GUC
pub static work_mem: c_int = 4096;
// TODO(pg-port): InvalidSnapshot
pub const InvalidSnapshot: Snapshot = ptr::null_mut();

// CURSOR_OPT constants
pub const CURSOR_OPT_PARALLEL_OK: c_int = 0x0800;
pub const CURSOR_OPT_NO_SCROLL: c_int = 0x0004;

// error_context_stack
pub static mut error_context_stack: *mut ErrorContextCallback = ptr::null_mut();
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe fn(*mut c_void)>,
    pub arg: *mut c_void,
}

// catalog IDs (TODO: generate from syscache_ids.h)
const PROCNAMEARGSNSP: c_int = 47; // placeholder
const PROCOID: c_int = 28;        // placeholder

// list_nth_node helper (casts list_nth result)
unsafe fn list_nth_node_Query(list: *mut List, n: c_int) -> *mut Query {
    list_nth(list, n) as *mut Query
}
unsafe fn list_nth_node_RawStmt(list: *mut List, n: c_int) -> *mut RawStmt {
    list_nth(list, n) as *mut RawStmt
}

// llast helpers
unsafe fn llast_node_List(list: *mut List) -> *mut List {
    todo!("TODO(pg-port): llast_node_List")
}
unsafe fn llast(list: *mut List) -> *mut c_void {
    todo!("TODO(pg-port): llast")
}
unsafe fn lsecond(list: *mut List) -> *mut c_void {
    todo!("TODO(pg-port): lsecond")
}
unsafe fn lthird(list: *mut List) -> *mut c_void {
    todo!("TODO(pg-port): lthird")
}
unsafe fn IsA_List(n: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_List")
}
unsafe fn castNode_List(n: *mut c_void) -> *mut List {
    n as *mut List
}
unsafe fn linitial_node_List(list: *mut List) -> *mut List {
    todo!("TODO(pg-port): linitial_node_List")
}
unsafe fn linitial_node_Query(list: *mut List) -> *mut Query {
    todo!("TODO(pg-port): linitial_node_Query")
}
unsafe fn linitial(list: *mut List) -> *mut c_void {
    todo!("TODO(pg-port): linitial")
}
unsafe fn lfirst_node_List(lc: *mut ListCell) -> *mut List {
    todo!("TODO(pg-port): lfirst_node_List")
}
unsafe fn lfirst_node_Query(lc: *mut ListCell) -> *mut Query {
    todo!("TODO(pg-port): lfirst_node_Query")
}
unsafe fn lfirst_node_PlannedStmt(lc: *mut ListCell) -> *mut PlannedStmt {
    todo!("TODO(pg-port): lfirst_node_PlannedStmt")
}
unsafe fn lfirst_node_TargetEntry(lc: *mut ListCell) -> *mut TargetEntry {
    todo!("TODO(pg-port): lfirst_node_TargetEntry")
}
unsafe fn lfirst_CachedPlanSource(lc: *mut ListCell) -> *mut CachedPlanSource {
    todo!("TODO(pg-port): lfirst_CachedPlanSource")
}
unsafe fn foreach_current_index(_lc: *mut ListCell) -> usize {
    todo!("TODO(pg-port): foreach_current_index")
}

// ParseState hook fields (TODO: real ParseState from parser/parse_node.h)
#[repr(C)]
pub struct FullParseState {
    pub p_pre_columnref_hook: Option<unsafe fn(*mut FullParseState, *mut ColumnRef) -> *mut Node>,
    pub p_post_columnref_hook: Option<unsafe fn(*mut FullParseState, *mut ColumnRef, *mut Node) -> *mut Node>,
    pub p_paramref_hook: Option<unsafe fn(*mut FullParseState, *mut ParamRef) -> *mut Node>,
    pub p_ref_hook_state: *mut c_void,
    pub p_last_srf: *mut Node,
}

pub unsafe fn makeNode_Param() -> *mut Param {
    todo!("TODO(pg-port): makeNode_Param")
}
pub unsafe fn makeNode_Query() -> *mut Query {
    todo!("TODO(pg-port): makeNode_Query")
}
pub unsafe fn makeNode_RangeTblEntry() -> *mut RangeTblEntry {
    todo!("TODO(pg-port): makeNode_RangeTblEntry")
}
pub unsafe fn makeNode_RangeTblRef() -> *mut RangeTblRef {
    todo!("TODO(pg-port): makeNode_RangeTblRef")
}

// OidIsValid
#[inline]
unsafe fn OidIsValid(oid: Oid) -> bool { oid != 0 }

// InvalidOid
pub const InvalidOid: Oid = 0;
// VOIDOID
pub const VOIDOID: Oid = 2278;
// RECORDOID
pub const RECORDOID: Oid = 2249;
// INT4OID
pub const INT4OID: Oid = 23;

// type type constants
pub const TYPTYPE_BASE: c_char = 'b' as c_char;
pub const TYPTYPE_DOMAIN: c_char = 'd' as c_char;
pub const TYPTYPE_ENUM: c_char = 'e' as c_char;
pub const TYPTYPE_RANGE: c_char = 'r' as c_char;
pub const TYPTYPE_MULTIRANGE: c_char = 'm' as c_char;
pub const TYPTYPE_COMPOSITE: c_char = 'c' as c_char;

// Snapshot type alias
pub type Snapshot = *mut c_void;

/*
 * Specialized DestReceiver for collecting query output in a SQL function
 */
#[repr(C)]
pub struct DR_sqlfunction {
    pub pub_: DestReceiver,            /* publicly-known function pointers */
    pub tstore: *mut Tuplestorestate,  /* where to put result tuples, or NULL */
    pub filter: *mut JunkFilter,       /* filter to convert tuple type */
}

/*
 * We have an execution_state record for each query in a function.  Each
 * record references a plantree for its query.  If the query is currently in
 * F_EXEC_RUN state then there's a QueryDesc too.
 *
 * The "next" fields chain together all the execution_state records generated
 * from a single original parsetree.  (There will only be more than one in
 * case of rule expansion of the original parsetree.)  The chain structure is
 * quite vestigial at this point, because we allocate the records in an array
 * for ease of memory management.  But we'll get rid of it some other day.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub enum ExecStatus {
    F_EXEC_START,
    F_EXEC_RUN,
    F_EXEC_DONE,
}
use ExecStatus::*;

#[repr(C)]
pub struct execution_state {
    pub next: *mut execution_state,
    pub status: ExecStatus,
    pub setsResult: bool,   /* true if this query produces func's result */
    pub lazyEval: bool,     /* true if should fetch one row at a time */
    pub stmt: *mut PlannedStmt,  /* plan for this query */
    pub qd: *mut QueryDesc,      /* null unless status == RUN */
}

/*
 * Data associated with a SQL-language function is kept in two main
 * data structures:
 *
 * 1. SQLFunctionHashEntry is a long-lived (potentially session-lifespan)
 * struct that holds all the info we need out of the function's pg_proc row.
 * In addition it holds pointers to CachedPlanSource(s) that manage creation
 * of plans for the query(s) within the function.  A SQLFunctionHashEntry is
 * potentially shared across multiple concurrent executions of the function,
 * so it must contain no execution-specific state; but its use_count must
 * reflect the number of SQLFunctionCache structs pointing at it.
 * If the function's pg_proc row is updated, we throw away and regenerate
 * the SQLFunctionHashEntry and subsidiary data.  (Also note that if the
 * function is polymorphic or used as a trigger, there is a separate
 * SQLFunctionHashEntry for each usage, so that we need consider only one
 * set of relevant data types.)  The struct itself is in memory managed by
 * funccache.c, and its subsidiary data is kept in one of two contexts:
 *  * pcontext ("parse context") holds the raw parse trees or Query trees
 *    that we read from the pg_proc row.  These will be converted to
 *    CachedPlanSources as they are needed.  Once the last one is converted,
 *    pcontext can be freed.
 *  * hcontext ("hash context") holds everything else belonging to the
 *    SQLFunctionHashEntry.
 *
 * 2. SQLFunctionCache is subsidiary data for a single FmgrInfo struct.
 * It is pointed to by the fn_extra field of the FmgrInfo struct, and is
 * always allocated in the FmgrInfo's fn_mcxt.  It holds a reference to
 * the CachedPlan for the current query, and other execution-specific data.
 * A few subsidiary items such as the ParamListInfo object are also kept
 * directly in fn_mcxt (which is also called fcontext here).  But most
 * subsidiary data is in jfcontext or subcontext.
 */

#[repr(C)]
pub struct SQLFunctionHashEntry {
    pub cfunc: CachedFunction,   /* fields managed by funccache.c */

    pub fname: *mut c_char,      /* function name (for error msgs) */
    pub src: *mut c_char,        /* function body text (for error msgs) */

    pub pinfo: SQLFunctionParseInfoPtr,  /* data for parser callback hooks */
    pub argtyplen: *mut int16,           /* lengths of the input argument types */

    pub rettype: Oid,           /* actual return type */
    pub typlen: int16,          /* length of the return type */
    pub typbyval: bool,         /* true if return type is pass by value */
    pub returnsSet: bool,       /* true if returning multiple rows */
    pub returnsTuple: bool,     /* true if returning whole tuple result */
    pub readonly_func: bool,    /* true to run in "read only" mode */
    pub prokind: c_char,        /* prokind from pg_proc row */

    pub rettupdesc: TupleDesc,  /* result tuple descriptor */

    pub source_list: *mut List,  /* RawStmts or Queries read from pg_proc */
    pub num_queries: c_int,      /* original length of source_list */
    pub raw_source: bool,        /* true if source_list contains RawStmts */

    pub plansource_list: *mut List,  /* CachedPlanSources for fn's queries */

    pub pcontext: MemoryContext,  /* memory context holding source_list */
    pub hcontext: MemoryContext,  /* memory context holding all else */
}

#[repr(C)]
pub struct SQLFunctionCache {
    pub func: *mut SQLFunctionHashEntry,  /* associated SQLFunctionHashEntry */

    pub active: bool,           /* are we executing this cache entry? */
    pub lazyEvalOK: bool,       /* true if lazyEval is safe */
    pub shutdown_reg: bool,     /* true if registered shutdown callback */
    pub lazyEval: bool,         /* true if using lazyEval for result query */
    pub randomAccess: bool,     /* true if tstore needs random access */
    pub ownSubcontext: bool,    /* is subcontext really a separate context? */

    pub paramLI: ParamListInfo,  /* Param list representing current args */

    pub tstore: *mut Tuplestorestate,  /* where we accumulate result for a SRF */
    pub tscontext: MemoryContext,      /* memory context that tstore should be in */

    pub junkFilter: *mut JunkFilter,  /* will be NULL if function returns VOID */
    pub jf_generation: c_int,        /* tracks whether junkFilter is up-to-date */

    /*
     * While executing a particular query within the function, cplan is the
     * CachedPlan we've obtained for that query, and eslist is a chain of
     * execution_state records for the individual plans within the CachedPlan.
     * If eslist is not NULL at entry to fmgr_sql, then we are resuming
     * execution of a lazyEval-mode set-returning function.
     *
     * next_query_index is the 0-based index of the next CachedPlanSource to
     * get a CachedPlan from.
     */
    pub cplan: *mut CachedPlan,      /* Plan for current query, if any */
    pub cowner: ResourceOwner,       /* CachedPlan is registered with this owner */
    pub next_query_index: c_int,     /* index of next CachedPlanSource to run */

    pub eslist: *mut execution_state,   /* chain of execution_state records */
    pub esarray: *mut execution_state,  /* storage for eslist */
    pub esarray_len: c_int,             /* allocated length of esarray[] */

    /* if positive, this is the 1-based index of the query we're processing */
    pub error_query_index: c_int,

    pub fcontext: MemoryContext,    /* memory context holding this struct and all
                                     * subsidiary data */
    pub jfcontext: MemoryContext,   /* subsidiary memory context holding
                                     * junkFilter, result slot, and related data */
    pub subcontext: MemoryContext,  /* subsidiary memory context for sub-executor */

    /* Callback to release our use-count on the SQLFunctionHashEntry */
    pub mcb: MemoryContextCallback,
}

pub type SQLFunctionCachePtr = *mut SQLFunctionCache;

/*
 * Data structure needed by the parser callback hooks to resolve parameter
 * references during parsing of a SQL function's body.
 */
#[repr(C)]
pub struct SQLFunctionParseInfo {
    pub fname: *mut c_char,   /* function's name */
    pub nargs: c_int,         /* number of input arguments */
    pub argtypes: *mut Oid,   /* resolved types of input arguments */
    pub argnames: *mut *mut c_char, /* names of input arguments; NULL if none */
    /* Note that argnames[i] can be NULL, if some args are unnamed */
    pub collation: Oid,       /* function's input collation, if known */
}

pub type SQLFunctionParseInfoPtr = *mut SQLFunctionParseInfo;


/*
 * Prepare the SQLFunctionParseInfo struct for parsing a SQL function body
 *
 * This includes resolving actual types of polymorphic arguments.
 *
 * call_expr can be passed as NULL, but then we will fail if there are any
 * polymorphic arguments.
 */
pub unsafe fn prepare_sql_fn_parse_info(
    procedureTuple: HeapTuple,
    call_expr: *mut Node,
    inputCollation: Oid,
) -> SQLFunctionParseInfoPtr {
    let pinfo: SQLFunctionParseInfoPtr;
    let procedureStruct = GETSTRUCT(procedureTuple) as *mut c_void as *mut FormData_pg_proc;
    let nargs: c_int;

    pinfo = palloc0(std::mem::size_of::<SQLFunctionParseInfo>()) as SQLFunctionParseInfoPtr;

    /* Function's name (only) can be used to qualify argument names */
    (*pinfo).fname = pstrdup(NameStr_pg_proc(procedureStruct));

    /* Save the function's input collation */
    (*pinfo).collation = inputCollation;

    /*
     * Copy input argument types from the pg_proc entry, then resolve any
     * polymorphic types.
     */
    (*pinfo).nargs = (*procedureStruct).pronargs as c_int;
    nargs = (*pinfo).nargs;
    if nargs > 0 {
        let argOidVect: *mut Oid;
        let mut argnum: c_int;

        argOidVect = palloc((nargs as usize) * std::mem::size_of::<Oid>()) as *mut Oid;
        ptr::copy_nonoverlapping(
            (*procedureStruct).proargtypes.values.as_ptr(),
            argOidVect,
            nargs as usize,
        );

        argnum = 0;
        while argnum < nargs {
            let mut argtype = *argOidVect.add(argnum as usize);

            if IsPolymorphicType(argtype) {
                argtype = get_call_expr_argtype(call_expr, argnum);
                if argtype == InvalidOid {
                    ereport!(ERROR, errmsg!("could not determine actual type of argument declared {}",
                               CStr::from_ptr(format_type_be(*argOidVect.add(argnum as usize))).to_string_lossy()) /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */);
                }
                *argOidVect.add(argnum as usize) = argtype;
            }
            argnum += 1;
        }

        (*pinfo).argtypes = argOidVect;
    }

    /*
     * Collect names of arguments, too, if any
     */
    if nargs > 0 {
        let mut proargnames: Datum;
        let mut proargmodes: Datum;
        let n_arg_names: c_int;
        let mut isNull: bool = false;

        proargnames = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
                                      Anum_pg_proc_proargnames,
                                      &mut isNull);
        if isNull {
            proargnames = PointerGetDatum(ptr::null_mut()); /* just to be sure */
        }

        proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
                                      Anum_pg_proc_proargmodes,
                                      &mut isNull);
        if isNull {
            proargmodes = PointerGetDatum(ptr::null_mut()); /* just to be sure */
        }

        n_arg_names = get_func_input_arg_names(proargnames, proargmodes,
                                                &mut (*pinfo).argnames);

        /* Paranoia: ignore the result if too few array entries */
        if n_arg_names < nargs {
            (*pinfo).argnames = ptr::null_mut();
        }
    } else {
        (*pinfo).argnames = ptr::null_mut();
    }

    pinfo
}

/*
 * Parser setup hook for parsing a SQL function body.
 */
pub unsafe fn sql_fn_parser_setup(
    pstate: *mut FullParseState,
    pinfo: SQLFunctionParseInfoPtr,
) {
    (*pstate).p_pre_columnref_hook = None;
    (*pstate).p_post_columnref_hook = Some(sql_fn_post_column_ref);
    (*pstate).p_paramref_hook = Some(sql_fn_param_ref);
    /* no need to use p_coerce_param_hook */
    (*pstate).p_ref_hook_state = pinfo as *mut c_void;
}

/*
 * sql_fn_post_column_ref		parser callback for ColumnRefs
 */
unsafe fn sql_fn_post_column_ref(
    pstate: *mut FullParseState,
    cref: *mut ColumnRef,
    var: *mut Node,
) -> *mut Node {
    let pinfo = (*pstate).p_ref_hook_state as SQLFunctionParseInfoPtr;
    let mut nnames: c_int;
    let field1: *mut Node;
    let mut subfield: *mut Node = ptr::null_mut();
    let name1: *const c_char;
    let mut name2: *const c_char = ptr::null();
    let mut param: *mut Node;

    /*
     * Never override a table-column reference.  This corresponds to
     * considering the parameter names to appear in a scope outside the
     * individual SQL commands, which is what we want.
     */
    if !var.is_null() {
        return ptr::null_mut();
    }

    /*----------
     * The allowed syntaxes are:
     *
     * A         A = parameter name
     * A.B       A = function name, B = parameter name
     *           OR: A = record-typed parameter name, B = field name
     *           (the first possibility takes precedence)
     * A.B.C     A = function name, B = record-typed parameter name,
     *           C = field name
     * A.*       Whole-row reference to composite parameter A.
     * A.B.*     Same, with A = function name, B = parameter name
     *
     * Here, it's sufficient to ignore the "*" in the last two cases --- the
     * main parser will take care of expanding the whole-row reference.
     *----------
     */
    nnames = list_length((*cref).fields);

    if nnames > 3 {
        return ptr::null_mut();
    }

    if IsA_AStar(llast((*cref).fields) as *mut Node) {
        nnames -= 1;
    }

    field1 = linitial((*cref).fields) as *mut Node;
    name1 = strVal(field1);
    if nnames > 1 {
        subfield = lsecond((*cref).fields) as *mut Node;
        name2 = strVal(subfield);
    }

    if nnames == 3 {
        /*
         * Three-part name: if the first part doesn't match the function name,
         * we can fail immediately. Otherwise, look up the second part, and
         * take the third part to be a field reference.
         */
        if strcmp(name1, (*pinfo).fname) != 0 {
            return ptr::null_mut();
        }

        param = sql_fn_resolve_param_name(pinfo, name2, (*cref).location);

        subfield = lthird((*cref).fields) as *mut Node;
        Assert!(!subfield.is_null()); /* Assert(IsA(subfield, String)) */
    } else if nnames == 2 && strcmp(name1, (*pinfo).fname) == 0 {
        /*
         * Two-part name with first part matching function name: first see if
         * second part matches any parameter name.
         */
        param = sql_fn_resolve_param_name(pinfo, name2, (*cref).location);

        if !param.is_null() {
            /* Yes, so this is a parameter reference, no subfield */
            subfield = ptr::null_mut();
        } else {
            /* No, so try to match as parameter name and subfield */
            param = sql_fn_resolve_param_name(pinfo, name1, (*cref).location);
        }
    } else {
        /* Single name, or parameter name followed by subfield */
        param = sql_fn_resolve_param_name(pinfo, name1, (*cref).location);
    }

    if param.is_null() {
        return ptr::null_mut(); /* No match */
    }

    if !subfield.is_null() {
        /*
         * Must be a reference to a field of a composite parameter; otherwise
         * ParseFuncOrColumn will return NULL, and we'll fail back at the
         * caller.
         */
        param = ParseFuncOrColumn(
            pstate as *mut crate::parser::parse_node::ParseState,
            list_make1!(subfield as *mut c_void),
            list_make1!(param as *mut c_void),
            (*pstate).p_last_srf,
            ptr::null_mut(),
            false,
            (*cref).location,
        );
    }

    param
}

/*
 * sql_fn_param_ref		parser callback for ParamRefs ($n symbols)
 */
unsafe fn sql_fn_param_ref(
    pstate: *mut FullParseState,
    pref: *mut ParamRef,
) -> *mut Node {
    let pinfo = (*pstate).p_ref_hook_state as SQLFunctionParseInfoPtr;
    let paramno = (*pref).number;

    /* Check parameter number is valid */
    if paramno <= 0 || paramno > (*pinfo).nargs {
        return ptr::null_mut(); /* unknown parameter number */
    }

    sql_fn_make_param(pinfo, paramno, (*pref).location)
}

/*
 * sql_fn_make_param		construct a Param node for the given paramno
 */
unsafe fn sql_fn_make_param(
    pinfo: SQLFunctionParseInfoPtr,
    paramno: c_int,
    location: c_int,
) -> *mut Node {
    let param: *mut Param;

    param = makeNode_Param();
    (*param).paramkind = ParamKind::PARAM_EXTERN;
    (*param).paramid = paramno;
    (*param).paramtype = *(*pinfo).argtypes.add((paramno - 1) as usize);
    (*param).paramtypmod = -1;
    (*param).paramcollid = get_typcollation((*param).paramtype);
    (*param).location = location;

    /*
     * If we have a function input collation, allow it to override the
     * type-derived collation for parameter symbols.  (XXX perhaps this should
     * not happen if the type collation is not default?)
     */
    if OidIsValid((*pinfo).collation) && OidIsValid((*param).paramcollid) {
        (*param).paramcollid = (*pinfo).collation;
    }

    param as *mut Node
}

/*
 * Search for a function parameter of the given name; if there is one,
 * construct and return a Param node for it.  If not, return NULL.
 * Helper function for sql_fn_post_column_ref.
 */
unsafe fn sql_fn_resolve_param_name(
    pinfo: SQLFunctionParseInfoPtr,
    paramname: *const c_char,
    location: c_int,
) -> *mut Node {
    let mut i: c_int;

    if (*pinfo).argnames.is_null() {
        return ptr::null_mut();
    }

    i = 0;
    while i < (*pinfo).nargs {
        let argname = *(*pinfo).argnames.add(i as usize);
        if !argname.is_null() && strcmp(argname, paramname) == 0 {
            return sql_fn_make_param(pinfo, i + 1, location);
        }
        i += 1;
    }

    ptr::null_mut()
}

// Stubs for C stdlib / pg utilities used below
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    libc::strcmp(a, b)
}

// TODO(pg-port): nodes/value.h strVal
unsafe fn strVal(node: *mut Node) -> *const c_char {
    todo!("TODO(pg-port): strVal")
}
// TODO(pg-port): IsA check for A_Star
unsafe fn IsA_AStar(node: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_AStar")
}

// TODO(pg-port): catalog/pg_proc.h FormData_pg_proc
#[repr(C)]
pub struct FormData_pg_proc {
    pub proname: NameData,
    pub pronargs: int16,
    pub proargtypes: OidVector,
    pub proretset: bool,
    pub provolatile: c_char,
    pub prokind: c_char,
}
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}
#[repr(C)]
pub struct OidVector {
    pub values: [Oid; FUNC_MAX_ARGS as usize],
}
unsafe fn NameStr_pg_proc(proc_: *mut FormData_pg_proc) -> *const c_char {
    (*proc_).proname.data.as_ptr()
}

// TODO(pg-port): utils/errcodes.h
pub const ERRCODE_DATATYPE_MISMATCH: c_int = 0;
pub const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
pub const ERRCODE_INVALID_FUNCTION_DEFINITION: c_int = 0;

// TODO(pg-port): CachedPlan.generation field
unsafe fn cplan_generation(cplan: *mut CachedPlan) -> c_int { 0 }

// TODO(pg-port): ForwardScanDirection, ScanDirection
pub const ForwardScanDirection: c_int = 1;
// TODO(pg-port): PROCESS_UTILITY_QUERY
pub const PROCESS_UTILITY_QUERY: c_int = 0;
// TODO(pg-port): CopyStmt, TransactionStmt node tags
unsafe fn IsA_CopyStmt(node: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_CopyStmt")
}
unsafe fn IsA_TransactionStmt(node: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_TransactionStmt")
}
unsafe fn IsA_CallStmt(node: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_CallStmt")
}
unsafe fn IsA_ReturnSetInfo(node: *mut Node) -> bool {
    todo!("TODO(pg-port): IsA_ReturnSetInfo")
}

/*
 * Initialize the SQLFunctionCache for a SQL function
 */
unsafe fn init_sql_fcache(fcinfo: FunctionCallInfo, lazyEvalOK: bool) -> SQLFunctionCachePtr {
    let finfo: *mut FmgrInfo = (*fcinfo).flinfo;
    let func: *mut SQLFunctionHashEntry;
    let mut fcache: SQLFunctionCachePtr;

    /*
     * If this is the first execution for this FmgrInfo, set up a cache struct
     * (initially containing null pointers).  The cache must live as long as
     * the FmgrInfo, so it goes in fn_mcxt.  Also set up a memory context
     * callback that will be invoked when fn_mcxt is deleted.
     */
    fcache = (*finfo).fn_extra as SQLFunctionCachePtr;
    if fcache.is_null() {
        fcache = MemoryContextAllocZero((*finfo).fn_mcxt,
                                        std::mem::size_of::<SQLFunctionCache>())
                 as SQLFunctionCachePtr;
        (*fcache).fcontext = (*finfo).fn_mcxt;
        (*fcache).mcb.func = Some(RemoveSQLFunctionCache_cb);
        (*fcache).mcb.arg = fcache as *mut c_void;
        MemoryContextRegisterResetCallback((*finfo).fn_mcxt, &mut (*fcache).mcb);
        (*finfo).fn_extra = fcache as *mut c_void;
    }

    /*
     * If the SQLFunctionCache is marked as active, we must have errored out
     * of a prior execution.  Reset state.
     */
    if (*fcache).active {
        /*
         * In general, this stanza should clear all the same fields that
         * ShutdownSQLFunction would.  Note we must clear fcache->cplan
         * without doing ReleaseCachedPlan, because error cleanup from the
         * prior execution would have taken care of releasing that plan.
         * Likewise, if tstore is still set then it is pointing at garbage.
         */
        (*fcache).cplan = ptr::null_mut();
        (*fcache).eslist = ptr::null_mut();
        (*fcache).tstore = ptr::null_mut();
        (*fcache).shutdown_reg = false;
        (*fcache).active = false;
    }

    /*
     * If we are resuming execution of a set-returning function, just keep
     * using the same cache.  We do not ask funccache.c to re-validate the
     * SQLFunctionHashEntry: we want to run to completion using the function's
     * initial definition.
     */
    if !(*fcache).eslist.is_null() {
        Assert!(!(*fcache).func.is_null());
        return fcache;
    }

    /*
     * Look up, or re-validate, the long-lived hash entry.  Make the hash key
     * depend on the result of get_call_result_type() when that's composite,
     * so that we can safely assume that we'll build a new hash entry if the
     * composite rowtype changes.
     */
    func = cached_function_compile(fcinfo as *mut crate::utils::cache::funccache::FunctionCallInfoBaseData,
                                    (*fcache).func as *mut CachedFunction,
                                    sql_compile_callback,
                                    Some(sql_delete_callback),
                                    std::mem::size_of::<SQLFunctionHashEntry>(),
                                    true,
                                    false)
           as *mut SQLFunctionHashEntry;

    /*
     * Install the hash pointer in the SQLFunctionCache, and increment its use
     * count to reflect that.  If cached_function_compile gave us back a
     * different hash entry than we were using before, we must decrement that
     * one's use count.
     */
    if func != (*fcache).func {
        if !(*fcache).func.is_null() {
            Assert!((*(*fcache).func).cfunc.use_count > 0);
            (*(*fcache).func).cfunc.use_count -= 1;
        }
        (*fcache).func = func;
        (*func).cfunc.use_count += 1;
        /* Assume we need to rebuild the junkFilter */
        (*fcache).junkFilter = ptr::null_mut();
    }

    /*
     * We're beginning a new execution of the function, so convert params to
     * appropriate format.
     */
    postquel_sub_params(fcache, fcinfo);

    /* Also reset lazyEval state for the new execution. */
    (*fcache).lazyEvalOK = lazyEvalOK;
    (*fcache).lazyEval = false;

    /* Also reset data about where we are in the function. */
    (*fcache).eslist = ptr::null_mut();
    (*fcache).next_query_index = 0;
    (*fcache).error_query_index = 0;

    fcache
}

/*
 * Set up the per-query execution_state records for the next query within
 * the SQL function.
 *
 * Returns true if successful, false if there are no more queries.
 */
unsafe fn init_execution_state(fcache: SQLFunctionCachePtr) -> bool {
    let plansource: *mut CachedPlanSource;
    let mut preves: *mut execution_state = ptr::null_mut();
    let mut lasttages: *mut execution_state = ptr::null_mut();
    let nstmts: c_int;
    let mut lc: *mut ListCell;

    /*
     * Clean up after previous query, if there was one.
     */
    if !(*fcache).cplan.is_null() {
        ReleaseCachedPlan((*fcache).cplan, (*fcache).cowner);
        (*fcache).cplan = ptr::null_mut();
    }
    (*fcache).eslist = ptr::null_mut();

    /*
     * Get the next CachedPlanSource, or stop if there are no more.  We might
     * need to create the next CachedPlanSource; if so, advance
     * error_query_index first, so that errors detected in prepare_next_query
     * are blamed on the right statement.
     */
    if (*fcache).next_query_index >= list_length((*(*fcache).func).plansource_list) {
        if (*fcache).next_query_index >= (*(*fcache).func).num_queries {
            return false;
        }
        (*fcache).error_query_index += 1;
        prepare_next_query((*fcache).func);
    } else {
        (*fcache).error_query_index += 1;
    }

    plansource = list_nth((*(*fcache).func).plansource_list,
                          (*fcache).next_query_index) as *mut CachedPlanSource;
    (*fcache).next_query_index += 1;

    /*
     * Generate plans for the query or queries within this CachedPlanSource.
     * Register the CachedPlan with the current resource owner.
     */
    (*fcache).cowner = CurrentResourceOwner;
    (*fcache).cplan = GetCachedPlan(plansource,
                                     (*fcache).paramLI,
                                     (*fcache).cowner,
                                     ptr::null_mut());

    /*
     * If necessary, make esarray[] bigger to hold the needed state.
     */
    nstmts = list_length((*(*fcache).cplan).stmt_list);
    if nstmts > (*fcache).esarray_len {
        if (*fcache).esarray.is_null() {
            (*fcache).esarray = MemoryContextAlloc(
                (*fcache).fcontext,
                (std::mem::size_of::<execution_state>() as c_int * nstmts) as usize)
                as *mut execution_state;
        } else {
            (*fcache).esarray = repalloc_array((*fcache).esarray, nstmts as usize);
        }
        (*fcache).esarray_len = nstmts;
    }

    /*
     * Build execution_state list to match the number of contained plans.
     */
    {
        let stmt_list = (*(*fcache).cplan).stmt_list;
        let mut idx: c_int = 0;
        while idx < list_length(stmt_list) {
            let cell = (*stmt_list).elements.add(idx as usize);
            let stmt = (*cell).ptr_value as *mut PlannedStmt;
            let newes: *mut execution_state;

            /*
             * Precheck all commands for validity in a function.  This should
             * generally match the restrictions spi.c applies.
             */
            if (*stmt).commandType == CMD_UTILITY {
                if IsA_CopyStmt((*stmt).utilityStmt) {
                    ereport!(ERROR, errmsg!("cannot COPY to/from client in an SQL function")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }

                if IsA_TransactionStmt((*stmt).utilityStmt) {
                    ereport!(ERROR, errmsg!("{} is not allowed in an SQL function",
                               CStr::from_ptr(CreateCommandName((*stmt).utilityStmt as *mut c_void))
                                   .to_string_lossy())
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }
            }

            if (*(*fcache).func).readonly_func && !CommandIsReadOnly(stmt) {
                ereport!(ERROR, errmsg!("{} is not allowed in a non-volatile function",
                           CStr::from_ptr(CreateCommandName(stmt as *mut c_void))
                               .to_string_lossy())
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }

            /* OK, build the execution_state for this query */
            newes = (*fcache).esarray.add(idx as usize);
            if !preves.is_null() {
                (*preves).next = newes;
            } else {
                (*fcache).eslist = newes;
            }

            (*newes).next = ptr::null_mut();
            (*newes).status = F_EXEC_START;
            (*newes).setsResult = false; /* might change below */
            (*newes).lazyEval = false;   /* might change below */
            (*newes).stmt = stmt;
            (*newes).qd = ptr::null_mut();

            if (*stmt).canSetTag {
                lasttages = newes;
            }

            preves = newes;
            idx += 1;
        }
    }

    /*
     * If this isn't the last CachedPlanSource, we're done here.  Otherwise,
     * we need to prepare information about how to return the results.
     */
    if (*fcache).next_query_index < (*(*fcache).func).num_queries {
        return true;
    }

    /*
     * Construct a JunkFilter we can use to coerce the returned rowtype to the
     * desired form, unless the result type is VOID.
     */
    if (*(*fcache).func).rettype != VOIDOID &&
       ((*fcache).junkFilter.is_null() ||
        (*fcache).jf_generation != cplan_generation((*fcache).cplan))
    {
        let slot: *mut TupleTableSlot;
        let resulttlist: *mut List;
        let oldcontext: MemoryContext;

        /* Create or reset the jfcontext */
        if (*fcache).jfcontext.is_null() {
            (*fcache).jfcontext = AllocSetContextCreate!((*fcache).fcontext,
                                                         b"SQL function junkfilter\0".as_ptr() as *const c_char,
                                                         ALLOCSET_SMALL_SIZES);
        } else {
            MemoryContextReset((*fcache).jfcontext);
        }
        oldcontext = MemoryContextSwitchTo((*fcache).jfcontext);

        slot = MakeSingleTupleTableSlot(ptr::null_mut(), &TTSOpsMinimalTuple as *const c_int as *const c_void);

        /*
         * Re-fetch the (possibly modified) output tlist of the final statement.
         */
        resulttlist = get_sql_fn_result_tlist(plansource_query_list(plansource));

        /*
         * If the result is composite, *and* we are returning the whole tuple
         * result, we need to insert nulls for any dropped columns.
         */
        if !(*(*fcache).func).rettupdesc.is_null() && (*(*fcache).func).returnsTuple {
            (*fcache).junkFilter = ExecInitJunkFilterConversion(resulttlist,
                                                                 (*(*fcache).func).rettupdesc,
                                                                 slot);
        } else {
            (*fcache).junkFilter = ExecInitJunkFilter(resulttlist, slot);
        }

        /*
         * The resulttlist tree belongs to the plancache and might disappear
         * underneath us due to plancache invalidation.
         */
        (*(*fcache).junkFilter).jf_targetList = NIL;

        /* Make sure output rowtype is properly blessed */
        if (*(*fcache).func).returnsTuple {
            BlessTupleDesc((*(*(*fcache).junkFilter).jf_resultSlot).tts_tupleDescriptor);
        }

        /* Mark the JunkFilter as up-to-date */
        (*fcache).jf_generation = cplan_generation((*fcache).cplan);

        MemoryContextSwitchTo(oldcontext);
    }

    if (*(*fcache).func).returnsSet &&
       !(*(*fcache).func).returnsTuple &&
       type_is_rowtype((*(*fcache).func).rettype)
    {
        /*
         * Returning rowtype as if it were scalar --- materialize won't work.
         */
        (*fcache).lazyEvalOK = true;
    }

    /*
     * Mark the last canSetTag query as delivering the function result; then,
     * if it is a plain SELECT, mark it for lazy evaluation.
     */
    if !lasttages.is_null() && !(*fcache).junkFilter.is_null() {
        (*lasttages).setsResult = true;
        if (*fcache).lazyEvalOK &&
           (*(*lasttages).stmt).commandType == CMD_SELECT &&
           !(*(*lasttages).stmt).hasModifyingCTE
        {
            (*fcache).lazyEval = true;
            (*lasttages).lazyEval = true;
        }
    }

    true
}

/*
 * Convert the SQL function's next query from source form (RawStmt or Query)
 * into a CachedPlanSource.  If it's the last query, also determine whether
 * the function returnsTuple.
 */
unsafe fn prepare_next_query(func: *mut SQLFunctionHashEntry) {
    let qindex: c_int;
    let islast: bool;
    let plansource: *mut CachedPlanSource;
    let queryTree_list: *mut List;
    let oldcontext: MemoryContext;

    /* Which query should we process? */
    qindex = list_length((*func).plansource_list);
    Assert!(qindex < (*func).num_queries); /* else caller error */
    islast = qindex + 1 >= (*func).num_queries;

    /*
     * Parse and/or rewrite the query, creating a CachedPlanSource that holds
     * a copy of the original parsetree.
     */
    if !(*func).raw_source {
        /* Source queries are already parse-analyzed */
        let parsetree: *mut Query = list_nth_node_Query((*func).source_list, qindex);
        let parsetree = copyObject(parsetree);
        plansource = CreateCachedPlanForQuery(parsetree,
                                               (*func).src,
                                               CreateCommandTag(parsetree as *mut Node));
        AcquireRewriteLocks(parsetree, true, false);
        queryTree_list = pg_rewrite_query(parsetree);
    } else {
        /* Source queries are raw parsetrees */
        let parsetree: *mut RawStmt = list_nth_node_RawStmt((*func).source_list, qindex);
        let parsetree = copyObject(parsetree);
        plansource = CreateCachedPlan(parsetree,
                                       (*func).src,
                                       CreateCommandTag((*parsetree).stmt));
        queryTree_list = pg_analyze_and_rewrite_withcb(parsetree,
                                                        (*func).src,
                                                        Some(sql_fn_parser_setup_cb),
                                                        (*func).pinfo as *mut c_void,
                                                        ptr::null_mut());
    }

    /*
     * Check that there are no statements we don't want to allow.
     */
    check_sql_fn_statement(queryTree_list);

    /*
     * If this is the last query, check that the function returns the type it
     * claims to.
     */
    if islast {
        (*func).returnsTuple = check_sql_stmt_retval(queryTree_list,
                                                      (*func).rettype,
                                                      (*func).rettupdesc,
                                                      (*func).prokind,
                                                      false);
    }

    /*
     * Now that check_sql_stmt_retval has done its thing, we can complete plan
     * cache entry creation.
     */
    CompleteCachedPlan(plansource,
                       queryTree_list,
                       ptr::null_mut(),
                       ptr::null_mut(),
                       0,
                       Some(Some(sql_fn_parser_setup_cb as unsafe fn(*mut ParseState, *mut c_void))),
                       (*func).pinfo as *mut c_void,
                       CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_NO_SCROLL,
                       false);

    /*
     * Install post-rewrite hook.  Its arg is the hash entry if this is the
     * last statement, else NULL.
     */
    SetPostRewriteHook(plansource,
                       Some(sql_postrewrite_callback_hook),
                       if islast { func as *mut c_void } else { ptr::null_mut() });

    /*
     * While the CachedPlanSources can take care of themselves, our List
     * pointing to them had better be in the hcontext.
     */
    oldcontext = MemoryContextSwitchTo((*func).hcontext);
    (*func).plansource_list = lappend((*func).plansource_list,
                                       plansource as *mut c_void);
    MemoryContextSwitchTo(oldcontext);

    /*
     * As soon as we've linked the CachedPlanSource into the list, mark it as
     * "saved".
     */
    SaveCachedPlan(plansource);

    /*
     * Finally, if this was the last statement, we can flush the pcontext with
     * the original query trees; they're all safely copied into
     * CachedPlanSources now.
     */
    if islast {
        (*func).source_list = NIL; /* avoid dangling pointer */
        MemoryContextDelete((*func).pcontext);
        (*func).pcontext = ptr::null_mut();
    }
}

// Shim: pg_analyze_and_rewrite_withcb takes ParserSetupHook; wrap sql_fn_parser_setup
unsafe fn sql_fn_parser_setup_cb(pstate: *mut ParseState, arg: *mut c_void) {
    sql_fn_parser_setup(pstate as *mut FullParseState, arg as SQLFunctionParseInfoPtr);
}
// Shim: sql_postrewrite_callback wrapped for hook signature
unsafe fn sql_postrewrite_callback_hook(querytree_list: *mut List, arg: *mut c_void) {
    sql_postrewrite_callback(querytree_list, arg);
}
// Shim: RemoveSQLFunctionCache wrapped for MemoryContextCallback
unsafe extern "C" fn RemoveSQLFunctionCache_cb(arg: *mut c_void) {
    RemoveSQLFunctionCache(arg);
}

/*
 * Fill a new SQLFunctionHashEntry.
 *
 * The passed-in "cfunc" struct is expected to be zeroes, except
 * for the CachedFunction fields, which we don't touch here.
 */
unsafe extern "C" fn sql_compile_callback(
    fcinfo: *mut crate::utils::cache::funccache::FunctionCallInfoBaseData,
    procedureTuple: *mut crate::utils::cache::funccache::HeapTupleData,
    hashkey: *const CachedFunctionHashKey,
    cfunc: *mut CachedFunction,
    forValidator: bool,
) {
    // funccache.rs carries its own placeholder fcinfo/HeapTuple stub types;
    // re-cast to the canonical fmgr/htup_details pointers used below.
    let fcinfo: FunctionCallInfo = fcinfo as FunctionCallInfo;
    let procedureTuple: HeapTuple = procedureTuple as HeapTuple;
    let func = cfunc as *mut SQLFunctionHashEntry;
    let procedureStruct = GETSTRUCT(procedureTuple) as *mut FormData_pg_proc;
    let mut comperrcontext = ErrorContextCallback {
        previous: ptr::null_mut(),
        callback: None,
        arg: ptr::null_mut(),
    };
    let hcontext: MemoryContext;
    let pcontext: MemoryContext;
    let oldcontext: MemoryContext = CurrentMemoryContext;
    let mut rettype: Oid = 0;
    let mut rettupdesc: TupleDesc = ptr::null_mut();
    let tmp: Datum;
    let mut isNull: bool = false;
    let source_list: *mut List;

    /*
     * Setup error traceback support for ereport() during compile.
     */
    comperrcontext.callback = Some(sql_compile_error_callback_cb);
    comperrcontext.arg = func as *mut c_void;
    comperrcontext.previous = error_context_stack;
    error_context_stack = &mut comperrcontext;

    /*
     * Create the hash entry's memory context.  For now it's a child of the
     * caller's context, so that it will go away if we fail partway through.
     */
    hcontext = AllocSetContextCreate!(CurrentMemoryContext,
                                      b"SQL function\0".as_ptr() as *const c_char,
                                      ALLOCSET_SMALL_SIZES);

    /*
     * Create the not-as-long-lived pcontext.  We make this a child of
     * hcontext so that it doesn't require separate deletion.
     */
    pcontext = AllocSetContextCreate!(hcontext,
                                      b"SQL function parse trees\0".as_ptr() as *const c_char,
                                      ALLOCSET_SMALL_SIZES);
    (*func).pcontext = pcontext;

    /*
     * copy function name immediately for use by error reporting callback, and
     * for use as memory context identifier
     */
    (*func).fname = MemoryContextStrdup(hcontext, NameStr_pg_proc(procedureStruct));
    MemoryContextSetIdentifier(hcontext, (*func).fname);

    /*
     * Resolve any polymorphism, obtaining the actual result type, and the
     * corresponding tupdesc if it's a rowtype.
     */
    get_call_result_type(fcinfo, &mut rettype, &mut rettupdesc);

    (*func).rettype = rettype;
    if !rettupdesc.is_null() {
        MemoryContextSwitchTo(hcontext);
        (*func).rettupdesc = CreateTupleDescCopy(rettupdesc);
        MemoryContextSwitchTo(oldcontext);
    }

    /* Fetch the typlen and byval info for the result type */
    get_typlenbyval(rettype, &mut (*func).typlen, &mut (*func).typbyval);

    /* Remember whether we're returning setof something */
    (*func).returnsSet = (*procedureStruct).proretset;

    /* Remember if function is STABLE/IMMUTABLE */
    (*func).readonly_func =
        (*procedureStruct).provolatile != PROVOLATILE_VOLATILE;

    /* Remember routine kind */
    (*func).prokind = (*procedureStruct).prokind;

    /*
     * We need the actual argument types to pass to the parser.
     */
    MemoryContextSwitchTo(hcontext);
    (*func).pinfo = prepare_sql_fn_parse_info(procedureTuple,
                                               (*(*fcinfo).flinfo).fn_expr,
                                               PG_GET_COLLATION());
    MemoryContextSwitchTo(oldcontext);

    /*
     * Now that we have the resolved argument types, collect their typlens for
     * use in postquel_sub_params.
     */
    (*func).argtyplen = MemoryContextAlloc(hcontext,
        ((*(*func).pinfo).nargs as usize) * std::mem::size_of::<int16>())
        as *mut int16;
    {
        let mut i = 0;
        while i < (*(*func).pinfo).nargs {
            *(*func).argtyplen.add(i as usize) =
                get_typlen(*(*(*func).pinfo).argtypes.add(i as usize));
            i += 1;
        }
    }

    /*
     * And of course we need the function body text.
     */
    tmp = SysCacheGetAttrNotNull(PROCOID, procedureTuple, Anum_pg_proc_prosrc);
    (*func).src = MemoryContextStrdup(hcontext,
                                       TextDatumGetCString(tmp));

    /* If we have prosqlbody, pay attention to that not prosrc. */
    {
        let tmp2 = SysCacheGetAttr(PROCOID,
                                    procedureTuple,
                                    Anum_pg_proc_prosqlbody,
                                    &mut isNull);
        if !isNull {
            /* Source queries are already parse-analyzed */
            let n = stringToNode(TextDatumGetCString(tmp2));
            if IsA_List(n) {
                source_list = linitial_node_List(castNode_List(n as *mut c_void));
            } else {
                source_list = list_make1!(n as *mut c_void);
            }
            (*func).raw_source = false;
        } else {
            /* Source queries are raw parsetrees */
            source_list = pg_parse_query((*func).src);
            (*func).raw_source = true;
        }
    }

    /*
     * Note: we must save the number of queries so that we'll still remember
     * how many there are after we discard source_list.
     */
    (*func).num_queries = list_length(source_list);

    /*
     * Edge case: empty function body is OK only if it returns VOID.
     */
    if (*func).num_queries == 0 && rettype != VOIDOID {
        ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                   CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.") */ */);
    }

    /* Save the source trees in pcontext for now. */
    MemoryContextSwitchTo(pcontext);
    (*func).source_list = copyObject(source_list);
    MemoryContextSwitchTo(oldcontext);

    /*
     * We now have a fully valid hash entry, so reparent hcontext under
     * CacheMemoryContext.
     */
    MemoryContextSetParent(hcontext, CacheMemoryContext);
    (*func).hcontext = hcontext;

    error_context_stack = comperrcontext.previous;
}

// Shim wrapper for sql_compile_error_callback (callback type takes *mut c_void)
unsafe fn sql_compile_error_callback_cb(arg: *mut c_void) {
    sql_compile_error_callback(arg);
}
// Shim wrapper for sql_exec_error_callback
unsafe fn sql_exec_error_callback_cb(arg: *mut c_void) {
    sql_exec_error_callback(arg);
}

// TODO(pg-port): PG_GET_COLLATION() macro
unsafe fn PG_GET_COLLATION() -> Oid { 0 }

/*
 * Deletion callback used by funccache.c.
 */
unsafe extern "C" fn sql_delete_callback(cfunc: *mut CachedFunction) {
    let func = cfunc as *mut SQLFunctionHashEntry;

    /* Release the CachedPlanSources */
    {
        let plist = (*func).plansource_list;
        let mut i: c_int = 0;
        while i < list_length(plist) {
            let plansource = (*(*plist).elements.add(i as usize)).ptr_value as *mut CachedPlanSource;
            DropCachedPlan(plansource);
            i += 1;
        }
    }
    (*func).plansource_list = NIL;

    /*
     * If we have an hcontext, free it, thereby getting rid of all subsidiary
     * data.
     */
    if !(*func).hcontext.is_null() {
        MemoryContextDelete((*func).hcontext);
    }
    (*func).hcontext = ptr::null_mut();
}

/*
 * Post-rewrite callback used by plancache.c.
 */
unsafe fn sql_postrewrite_callback(querytree_list: *mut List, arg: *mut c_void) {
    /*
     * Check that there are no statements we don't want to allow.
     */
    check_sql_fn_statement(querytree_list);

    /*
     * If this is the last query, we must re-do what check_sql_stmt_retval did
     * to its targetlist.
     */
    if !arg.is_null() {
        let func = arg as *mut SQLFunctionHashEntry;
        let mut returnsTuple: bool;

        returnsTuple = check_sql_stmt_retval(querytree_list,
                                              (*func).rettype,
                                              (*func).rettupdesc,
                                              (*func).prokind,
                                              false);
        if returnsTuple != (*func).returnsTuple {
            ereport!(ERROR, errmsg!("cached plan must not change result type")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
    }
}

/* Start up execution of one execution_state node */
unsafe fn postquel_start(es: *mut execution_state, fcache: SQLFunctionCachePtr) {
    let dest: *mut DestReceiver;
    let oldcontext: MemoryContext = CurrentMemoryContext;

    Assert!((*es).qd.is_null());

    /* Caller should have ensured a suitable snapshot is active */
    Assert!(ActiveSnapshotSet());

    /*
     * In lazyEval mode for a SRF, we must run the sub-executor in a child of
     * fcontext, so that it can survive across multiple calls to fmgr_sql.
     */
    if (*es).lazyEval && (*(*fcache).func).returnsSet {
        (*fcache).subcontext = AllocSetContextCreate!((*fcache).fcontext,
                                                      b"SQL function execution\0".as_ptr() as *const c_char,
                                                      ALLOCSET_DEFAULT_SIZES);
        (*fcache).ownSubcontext = true;
    } else if (*(*es).stmt).commandType == CMD_UTILITY {
        /*
         * If this is a utility statement, it won't make its own sub-context,
         * so it seems advisable to make one that we can free on completion.
         */
        (*fcache).subcontext = AllocSetContextCreate!(CurrentMemoryContext,
                                                      b"SQL function execution\0".as_ptr() as *const c_char,
                                                      ALLOCSET_DEFAULT_SIZES);
        (*fcache).ownSubcontext = true;
    } else {
        (*fcache).subcontext = CurrentMemoryContext;
        (*fcache).ownSubcontext = false;
    }

    /*
     * Build a tuplestore if needed, that is if it's a set-returning function
     * and we're producing the function result without using lazyEval mode.
     */
    if (*es).setsResult {
        Assert!((*fcache).tstore.is_null());
        if (*(*fcache).func).returnsSet && !(*es).lazyEval {
            MemoryContextSwitchTo((*fcache).tscontext);
            (*fcache).tstore = tuplestore_begin_heap((*fcache).randomAccess,
                                                      false, work_mem);
        }
    }

    /* Switch into the selected subcontext (might be a no-op) */
    MemoryContextSwitchTo((*fcache).subcontext);

    /*
     * If this query produces the function result, collect its output using
     * our custom DestReceiver; else discard any output.
     */
    if (*es).setsResult {
        let myState: *mut DR_sqlfunction;

        dest = CreateDestReceiver(DestSQLFunction);
        /* pass down the needed info to the dest receiver routines */
        myState = dest as *mut DR_sqlfunction;
        Assert!((*myState).pub_.mydest == DestSQLFunction);
        (*myState).tstore = (*fcache).tstore; /* might be NULL */
        (*myState).filter = (*fcache).junkFilter;

        /* Make very sure the junkfilter's result slot is empty */
        ExecClearTuple((*(*fcache).junkFilter).jf_resultSlot);
    } else {
        dest = None_Receiver();
    }

    (*es).qd = CreateQueryDesc((*es).stmt,
                                (*(*fcache).func).src,
                                GetActiveSnapshot() as crate::nodes::execnodes::Snapshot,
                                InvalidSnapshot as crate::nodes::execnodes::Snapshot,
                                dest,
                                (*fcache).paramLI,
                                if !(*es).qd.is_null() { (*(*es).qd).queryEnv } else { ptr::null_mut() },
                                0);

    /* Utility commands don't need Executor. */
    if (*(*es).qd).operation != CMD_UTILITY {
        /*
         * In lazyEval mode, do not let the executor set up an AfterTrigger
         * context.
         */
        let eflags: c_int;

        if (*es).lazyEval {
            eflags = EXEC_FLAG_SKIP_TRIGGERS;
        } else {
            eflags = 0; /* default run-to-completion flags */
        }
        ExecutorStart((*es).qd, eflags);
    }

    (*es).status = F_EXEC_RUN;

    MemoryContextSwitchTo(oldcontext);
}

/* Run one execution_state; either to completion or to first result row */
/* Returns true if we ran to completion */
unsafe fn postquel_getnext(es: *mut execution_state, fcache: SQLFunctionCachePtr) -> bool {
    let result: bool;
    let oldcontext: MemoryContext;

    /* Run the sub-executor in subcontext */
    oldcontext = MemoryContextSwitchTo((*fcache).subcontext);

    if (*(*es).qd).operation == CMD_UTILITY {
        ProcessUtility((*(*es).qd).plannedstmt,
                       (*(*fcache).func).src,
                       true, /* protect function cache's parsetree */
                       PROCESS_UTILITY_QUERY,
                       (*(*es).qd).params,
                       (*(*es).qd).queryEnv,
                       (*(*es).qd).dest,
                       ptr::null_mut());
        result = true; /* never stops early */
    } else {
        /* Run regular commands to completion unless lazyEval */
        let count: u64 = if (*es).lazyEval { 1 } else { 0 };

        ExecutorRun((*es).qd, ForwardScanDirection, count);

        /*
         * If we requested run to completion OR there was no tuple returned,
         * command must be complete.
         */
        result = count == 0 || (*(*(*es).qd).estate).es_processed == 0;
    }

    MemoryContextSwitchTo(oldcontext);

    result
}

/* Shut down execution of one execution_state node */
unsafe fn postquel_end(es: *mut execution_state, fcache: SQLFunctionCachePtr) {
    let oldcontext: MemoryContext;

    /* Run the sub-executor in subcontext */
    oldcontext = MemoryContextSwitchTo((*fcache).subcontext);

    /* mark status done to ensure we don't do ExecutorEnd twice */
    (*es).status = F_EXEC_DONE;

    /* Utility commands don't need Executor. */
    if (*(*es).qd).operation != CMD_UTILITY {
        ExecutorFinish((*es).qd);
        ExecutorEnd((*es).qd);
    }

    let destroy_fn = (*(*(*es).qd).dest).rDestroy;
    if let Some(f) = destroy_fn {
        f((*(*es).qd).dest);
    }

    FreeQueryDesc((*es).qd);
    (*es).qd = ptr::null_mut();

    MemoryContextSwitchTo(oldcontext);

    /* Delete the subcontext, if it's actually a separate context */
    if (*fcache).ownSubcontext {
        MemoryContextDelete((*fcache).subcontext);
    }
    (*fcache).subcontext = ptr::null_mut();
}

/* Build ParamListInfo array representing current arguments */
unsafe fn postquel_sub_params(fcache: SQLFunctionCachePtr, fcinfo: FunctionCallInfo) {
    let nargs = (*fcinfo).nargs;

    if nargs > 0 {
        let paramLI: ParamListInfo;
        let argtypes: *mut Oid = (*(*(*fcache).func).pinfo).argtypes;
        let argtyplen: *mut int16 = (*(*fcache).func).argtyplen;

        if (*fcache).paramLI.is_null() {
            /* First time through: build a persistent ParamListInfo struct */
            let oldcontext: MemoryContext;

            oldcontext = MemoryContextSwitchTo((*fcache).fcontext);
            paramLI = makeParamList(nargs as c_int);
            (*fcache).paramLI = paramLI;
            MemoryContextSwitchTo(oldcontext);
        } else {
            paramLI = (*fcache).paramLI;
            Assert!((*paramLI).numParams == nargs as c_int);
        }

        let mut i = 0;
        while i < nargs {
            let prm: *mut ParamExternData = (*paramLI).params.as_mut_ptr().add(i as usize);

            /*
             * If an incoming parameter value is a R/W expanded datum, we
             * force it to R/O.
             */
            (*prm).isnull = (*(*fcinfo).args.as_ptr().add(i as usize)).isnull;
            (*prm).value = MakeExpandedObjectReadOnly((*(*fcinfo).args.as_ptr().add(i as usize)).value,
                                                       (*prm).isnull,
                                                       *argtyplen.add(i as usize) as c_int);
            /* Allow the value to be substituted into custom plans */
            (*prm).pflags = PARAM_FLAG_CONST as u16;
            (*prm).ptype = *argtypes.add(i as usize);
            i += 1;
        }
    } else {
        (*fcache).paramLI = ptr::null_mut();
    }
}

/*
 * Extract the SQL function's value from a single result row.
 */
unsafe fn postquel_get_single_result(
    slot: *mut TupleTableSlot,
    fcinfo: FunctionCallInfo,
    fcache: SQLFunctionCachePtr,
) -> Datum {
    let value: Datum;

    /*
     * Set up to return the function value.  For pass-by-reference datatypes,
     * be sure to copy the result into the current context.
     */
    if (*(*fcache).func).returnsTuple {
        /* We must return the whole tuple as a Datum. */
        (*fcinfo).isnull = false;
        value = ExecFetchSlotHeapTupleDatum(slot);
    } else {
        /*
         * Returning a scalar, which we have to extract from the first column
         * of the SELECT result.
         */
        value = slot_getattr(slot, 1, &mut (*fcinfo).isnull);

        if !(*fcinfo).isnull {
            let _ = datumCopy(value, (*(*fcache).func).typbyval, (*(*fcache).func).typlen as c_int);
        }
    }

    /* Clear the slot for next time */
    ExecClearTuple(slot);

    value
}

/*
 * fmgr_sql: function call manager for SQL functions
 */
pub unsafe fn fmgr_sql(fcinfo: FunctionCallInfo) -> Datum {
    let mut fcache: SQLFunctionCachePtr;
    let mut sqlerrcontext = ErrorContextCallback {
        previous: ptr::null_mut(),
        callback: None,
        arg: ptr::null_mut(),
    };
    let tscontext: MemoryContext;
    let randomAccess: bool;
    let lazyEvalOK: bool;
    let mut pushed_snapshot: bool;
    let mut es: *mut execution_state;
    let mut slot: *mut TupleTableSlot;
    let mut result: Datum;

    /* Check call context */
    if (*(*fcinfo).flinfo).fn_retset {
        let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;

        /*
         * For simplicity, we require callers to support both set eval modes.
         */
        if rsi.is_null() || !IsA_ReturnSetInfo(rsi as *mut Node) ||
           ((*rsi).allowedModes & SFRM_ValuePerCall as c_int) == 0 ||
           ((*rsi).allowedModes & SFRM_Materialize as c_int) == 0
        {
            ereport!(ERROR, errmsg!("set-valued function called in context that cannot accept a set")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
        randomAccess = ((*rsi).allowedModes & SFRM_Materialize_Random as c_int) != 0;
        lazyEvalOK = ((*rsi).allowedModes & SFRM_Materialize_Preferred as c_int) == 0;
        /* tuplestore, if used, must have query lifespan */
        tscontext = (*(*rsi).econtext).ecxt_per_query_memory;
    } else {
        randomAccess = false;
        lazyEvalOK = true;
        /* we won't need a tuplestore */
        tscontext = ptr::null_mut();
    }

    /*
     * Initialize fcache if starting a fresh execution.
     */
    fcache = init_sql_fcache(fcinfo, lazyEvalOK);

    /* Mark fcache as active */
    (*fcache).active = true;

    /* Remember info that we might need later to construct tuplestore */
    (*fcache).tscontext = tscontext;
    (*fcache).randomAccess = randomAccess;

    /*
     * Now we can set up error traceback support for ereport()
     */
    sqlerrcontext.callback = Some(sql_exec_error_callback_cb);
    sqlerrcontext.arg = fcache as *mut c_void;
    sqlerrcontext.previous = error_context_stack;
    error_context_stack = &mut sqlerrcontext;

    /*
     * Find first unfinished execution_state.  If none, advance to the next
     * query in function.
     */
    'find_es: loop {
        es = (*fcache).eslist;
        while !es.is_null() && (*es).status == F_EXEC_DONE {
            es = (*es).next;
        }
        if !es.is_null() {
            break 'find_es;
        }
        if !init_execution_state(fcache) {
            break 'find_es;
        }
    }

    /*
     * Execute each command in the function one after another until we either
     * run out of commands or get a result row from a lazily-evaluated SELECT.
     */
    pushed_snapshot = false;
    while !es.is_null() {
        let completed: bool;

        if (*es).status == F_EXEC_START {
            /*
             * If not read-only, be sure to advance the command counter for
             * each command.
             */
            if !(*(*fcache).func).readonly_func {
                CommandCounterIncrement();
                if !pushed_snapshot {
                    PushActiveSnapshot(GetTransactionSnapshot());
                    pushed_snapshot = true;
                } else {
                    UpdateActiveSnapshotCommandId();
                }
            }

            postquel_start(es, fcache);
        } else if !(*(*fcache).func).readonly_func && !pushed_snapshot {
            /* Re-establish active snapshot when re-entering function */
            PushActiveSnapshot((*(*es).qd).snapshot as Snapshot);
            pushed_snapshot = true;
        }

        completed = postquel_getnext(es, fcache);

        /*
         * If we ran the command to completion, we can shut it down now.
         */
        if completed || !(*(*fcache).func).returnsSet {
            postquel_end(es, fcache);
        }

        /*
         * Break from loop if we didn't shut down (implying we got a
         * lazily-evaluated row).
         */
        if (*es).status != F_EXEC_DONE {
            break;
        }

        /*
         * Advance to next execution_state, and perhaps next query.
         */
        es = (*es).next;
        while es.is_null() {
            /*
             * Flush the current snapshot so that we will take a new one for
             * the new query list.
             */
            if pushed_snapshot {
                PopActiveSnapshot();
                pushed_snapshot = false;
            }

            if !init_execution_state(fcache) {
                break; /* end of function */
            }

            es = (*fcache).eslist;
        }
    }

    /*
     * The result slot or tuplestore now contains whatever row(s) we are
     * supposed to return.
     */
    if (*(*fcache).func).returnsSet {
        let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;

        if !es.is_null() {
            /*
             * If we stopped short of being done, we must have a lazy-eval row.
             */
            Assert!((*es).lazyEval);
            /* The junkfilter's result slot contains the query result tuple */
            Assert!(!(*fcache).junkFilter.is_null());
            slot = (*(*fcache).junkFilter).jf_resultSlot;
            Assert!(!TTS_EMPTY(slot));
            /* Extract the result as a datum, and copy out from the slot */
            result = postquel_get_single_result(slot, fcinfo, fcache);

            /*
             * Let caller know we're not finished.
             */
            (*rsi).isDone = ExprMultipleResult;

            /*
             * Ensure we will get shut down cleanly if the exprcontext is not
             * run to completion.
             */
            if !(*fcache).shutdown_reg {
                RegisterExprContextCallback((*rsi).econtext,
                                             Some(ShutdownSQLFunction_cb),
                                             PointerGetDatum(fcache as *mut c_void));
                (*fcache).shutdown_reg = true;
            }
        } else if (*fcache).lazyEval {
            /*
             * We are done with a lazy evaluation.  Let caller know we're finished.
             */
            (*rsi).isDone = ExprEndResult;

            (*fcinfo).isnull = true;
            result = 0 as Datum;

            /* Deregister shutdown callback, if we made one */
            if (*fcache).shutdown_reg {
                UnregisterExprContextCallback((*rsi).econtext,
                                               Some(ShutdownSQLFunction_cb),
                                               PointerGetDatum(fcache as *mut c_void));
                (*fcache).shutdown_reg = false;
            }
        } else {
            /*
             * We are done with a non-lazy evaluation.  Return whatever is in
             * the tuplestore.
             */
            Assert!(!(*fcache).tstore.is_null() || (*(*fcache).func).rettype == VOIDOID);
            (*rsi).returnMode = SFRM_Materialize;
            (*rsi).setResult = (*fcache).tstore;
            (*fcache).tstore = ptr::null_mut();
            /* must copy desc because execSRF.c will free it */
            if !(*fcache).junkFilter.is_null() {
                (*rsi).setDesc = CreateTupleDescCopy(
                    (*(*(*fcache).junkFilter).jf_resultSlot).tts_tupleDescriptor);
            }

            (*fcinfo).isnull = true;
            result = 0 as Datum;

            /* Deregister shutdown callback, if we made one */
            if (*fcache).shutdown_reg {
                UnregisterExprContextCallback((*rsi).econtext,
                                               Some(ShutdownSQLFunction_cb),
                                               PointerGetDatum(fcache as *mut c_void));
                (*fcache).shutdown_reg = false;
            }
        }
    } else {
        /*
         * Non-set function.  If we got a row, return it; else return NULL.
         */
        if !(*fcache).junkFilter.is_null() {
            /* The junkfilter's result slot contains the query result tuple */
            slot = (*(*fcache).junkFilter).jf_resultSlot;
            if !TTS_EMPTY(slot) {
                result = postquel_get_single_result(slot, fcinfo, fcache);
            } else {
                (*fcinfo).isnull = true;
                result = 0 as Datum;
            }
        } else {
            /* Should only get here for VOID functions and procedures */
            Assert!((*(*fcache).func).rettype == VOIDOID);
            (*fcinfo).isnull = true;
            result = 0 as Datum;
        }
    }

    /* Pop snapshot if we have pushed one */
    if pushed_snapshot {
        PopActiveSnapshot();
    }

    /*
     * If we've gone through every command in the function, we are done.
     */
    if es.is_null() {
        (*fcache).eslist = ptr::null_mut();
    }

    /* Mark fcache as inactive */
    (*fcache).active = false;

    error_context_stack = sqlerrcontext.previous;

    result
}

// Shim: ShutdownSQLFunction wrapped for ExprContextCallback signature
unsafe fn ShutdownSQLFunction_cb(arg: Datum) {
    ShutdownSQLFunction(arg);
}

/*
 * error context callback to let us supply a traceback during compile
 */
unsafe fn sql_compile_error_callback(arg: *mut c_void) {
    let func = arg as *mut SQLFunctionHashEntry;
    let syntaxerrposition: c_int;

    /*
     * We can do nothing useful if sql_compile_callback() didn't get as far as
     * copying the function name
     */
    if (*func).fname.is_null() {
        return;
    }

    /*
     * If there is a syntax error position, convert to internal syntax error
     */
    syntaxerrposition = geterrposition();
    if syntaxerrposition > 0 && !(*func).src.is_null() {
        errposition(0);
        internalerrposition(syntaxerrposition);
        internalerrquery((*func).src);
    }

    /*
     * sql_compile_callback() doesn't do any per-query processing, so just
     * report the context as "during startup".
     */
    errcontext_sql_fn((*func).fname, -1);
}

/*
 * error context callback to let us supply a call-stack traceback at runtime
 */
unsafe fn sql_exec_error_callback(arg: *mut c_void) {
    let fcache = arg as SQLFunctionCachePtr;
    let syntaxerrposition: c_int;

    /*
     * If there is a syntax error position, convert to internal syntax error
     */
    syntaxerrposition = geterrposition();
    if syntaxerrposition > 0 && !(*(*fcache).func).src.is_null() {
        errposition(0);
        internalerrposition(syntaxerrposition);
        internalerrquery((*(*fcache).func).src);
    }

    /*
     * If we failed while executing an identifiable query within the function,
     * report that.  Otherwise say it was "during startup".
     */
    if (*fcache).error_query_index > 0 {
        errcontext_sql_fn_stmt((*(*fcache).func).fname, (*fcache).error_query_index);
    } else {
        errcontext_sql_fn((*(*fcache).func).fname, -1);
    }
}

// TODO(pg-port): errcontext macro - C uses errcontext() which appends to error context
unsafe fn errcontext_sql_fn(fname: *mut c_char, _stmt: c_int) {
    // no-op stub; real impl calls errcontext()
    let _ = CStr::from_ptr(fname).to_string_lossy();
}
unsafe fn errcontext_sql_fn_stmt(fname: *mut c_char, stmt: c_int) {
    let _ = (CStr::from_ptr(fname).to_string_lossy(), stmt);
}

/*
 * ExprContext callback function
 *
 * We register this in the active ExprContext while a set-returning SQL
 * function is running, in case the function needs to be shut down before it
 * has been run to completion.
 */
unsafe fn ShutdownSQLFunction(arg: Datum) {
    let fcache = DatumGetPointer(arg) as SQLFunctionCachePtr;
    let mut es: *mut execution_state;

    es = (*fcache).eslist;
    while !es.is_null() {
        /* Shut down anything still running */
        if (*es).status == F_EXEC_RUN {
            /* Re-establish active snapshot for any called functions */
            if !(*(*fcache).func).readonly_func {
                PushActiveSnapshot((*(*es).qd).snapshot as Snapshot);
            }

            postquel_end(es, fcache);

            if !(*(*fcache).func).readonly_func {
                PopActiveSnapshot();
            }
        }
        es = (*es).next;
    }
    (*fcache).eslist = ptr::null_mut();

    /* Release tuplestore if we have one */
    if !(*fcache).tstore.is_null() {
        tuplestore_end((*fcache).tstore);
    }
    (*fcache).tstore = ptr::null_mut();

    /* Release CachedPlan if we have one */
    if !(*fcache).cplan.is_null() {
        ReleaseCachedPlan((*fcache).cplan, (*fcache).cowner);
    }
    (*fcache).cplan = ptr::null_mut();

    /* execUtils will deregister the callback... */
    (*fcache).shutdown_reg = false;
}

/*
 * MemoryContext callback function
 *
 * We register this in the memory context that contains a SQLFunctionCache
 * struct.  When the memory context is reset or deleted, we release the
 * reference count (if any) that the cache holds on the long-lived hash entry.
 */
unsafe fn RemoveSQLFunctionCache(arg: *mut c_void) {
    let fcache = arg as *mut SQLFunctionCache;

    /* Release reference count on SQLFunctionHashEntry */
    if !(*fcache).func.is_null() {
        Assert!((*(*fcache).func).cfunc.use_count > 0);
        (*(*fcache).func).cfunc.use_count -= 1;
        /* This should be unnecessary, but let's just be sure: */
        (*fcache).func = ptr::null_mut();
    }
}

/*
 * check_sql_fn_statements
 *
 * Check statements in an SQL function.  Error out if there is anything that
 * is not acceptable.
 */
pub unsafe fn check_sql_fn_statements(queryTreeLists: *mut List) {
    /* We are given a list of sublists of Queries */
    let mut i: c_int = 0;
    while i < list_length(queryTreeLists) {
        let sublist = (*(*queryTreeLists).elements.add(i as usize)).ptr_value as *mut List;
        check_sql_fn_statement(sublist);
        i += 1;
    }
}

/*
 * As above, for a single sublist of Queries.
 */
unsafe fn check_sql_fn_statement(queryTreeList: *mut List) {
    let mut i: c_int = 0;
    while i < list_length(queryTreeList) {
        let query = (*(*queryTreeList).elements.add(i as usize)).ptr_value as *mut Query;

        /*
         * Disallow calling procedures with output arguments.  The current
         * implementation would just throw the output values away, unless the
         * statement is the last one.
         */
        if (*query).commandType == CMD_UTILITY &&
           IsA_CallStmt((*query).utilityStmt)
        {
            let stmt = (*query).utilityStmt as *mut CallStmt;

            if !(*stmt).outargs.is_null() && (*(*stmt).outargs).length != 0 {
                ereport!(ERROR, errmsg!("calling procedures with output arguments is not supported in SQL functions")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }
        }

        i += 1;
    }
}

/*
 * check_sql_fn_retval()
 *      Check return value of a list of lists of sql parse trees.
 *
 * Returns true if the sql function returns the entire tuple result of its
 * final statement, or false if it returns just the first column result.
 */
pub unsafe fn check_sql_fn_retval(
    queryTreeLists: *mut List,
    rettype: Oid,
    rettupdesc: TupleDesc,
    prokind: c_char,
    insertDroppedCols: bool,
) -> bool {
    let queryTreeList: *mut List;

    /*
     * We consider only the last sublist of Query nodes.
     */
    if !queryTreeLists.is_null() && (*queryTreeLists).length != 0 {
        queryTreeList = llast_node_List(queryTreeLists);
    } else {
        queryTreeList = NIL;
    }

    check_sql_stmt_retval(queryTreeList, rettype, rettupdesc, prokind, insertDroppedCols)
}

/*
 * As for check_sql_fn_retval, but we are given just the last query's
 * rewritten-queries list.
 */
unsafe fn check_sql_stmt_retval(
    queryTreeList: *mut List,
    rettype: Oid,
    rettupdesc: TupleDesc,
    prokind: c_char,
    insertDroppedCols: bool,
) -> bool {
    let mut is_tuple_result: bool = false;
    let mut parse: *mut Query = ptr::null_mut();
    let mut parse_cell: *mut ListCell = ptr::null_mut();
    let tlist: *mut List;
    let tlistlen: c_int;
    let tlist_is_modifiable: bool;
    let fn_typtype: c_char;
    let mut upper_tlist: *mut List = NIL;
    let mut upper_tlist_nontrivial: bool = false;

    /*
     * If it's declared to return VOID, we don't care what's in the function.
     */
    if rettype == VOIDOID {
        return false;
    }

    /*
     * Find the last canSetTag query in the list of Query nodes.
     */
    {
        let mut i: c_int = 0;
        while i < list_length(queryTreeList) {
            let cell = (*queryTreeList).elements.add(i as usize);
            let q = (*cell).ptr_value as *mut Query;
            if (*q).canSetTag {
                parse = q;
                parse_cell = cell;
            }
            i += 1;
        }
    }

    /*
     * Determine tlist from parse.
     */
    let (tlist_ptr, tlist_is_mod) = 'get_tlist: {
        if !parse.is_null() && (*parse).commandType == CMD_SELECT {
            break 'get_tlist ((*parse).targetList, (*parse).setOperations.is_null());
        } else if !parse.is_null() &&
            ((*parse).commandType == CMD_INSERT ||
             (*parse).commandType == CMD_UPDATE ||
             (*parse).commandType == CMD_DELETE ||
             (*parse).commandType == CMD_MERGE) &&
            !(*parse).returningList.is_null()
        {
            break 'get_tlist ((*parse).returningList, true);
        } else {
            /* Last statement is a utility command, or it rewrote to nothing */
            ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                       CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.") */ */);
            return false; /* keep compiler quiet */
        }
    };
    let tlist = tlist_ptr;
    let tlist_is_modifiable = tlist_is_mod;

    /*
     * Count the non-junk entries in the result targetlist.
     */
    let tlistlen = ExecCleanTargetListLength(tlist);

    fn_typtype = get_typtype(rettype);

    'tlist_coercion_finished: {
        if fn_typtype == TYPTYPE_BASE ||
           fn_typtype == TYPTYPE_DOMAIN ||
           fn_typtype == TYPTYPE_ENUM ||
           fn_typtype == TYPTYPE_RANGE ||
           fn_typtype == TYPTYPE_MULTIRANGE
        {
            /*
             * For scalar-type returns, the target list must have exactly one
             * non-junk entry, and its type must be coercible to rettype.
             */
            let tle: *mut TargetEntry;

            if tlistlen != 1 {
                ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                           CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Final statement must return exactly one column.") */ */);
            }

            /* We assume here that non-junk TLEs must come first in tlists */
            tle = linitial(tlist) as *mut TargetEntry;
            Assert!(!(*tle).resjunk);

            if !coerce_fn_result_column(tle, rettype, -1,
                                         tlist_is_modifiable,
                                         &mut upper_tlist,
                                         &mut upper_tlist_nontrivial)
            {
                ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                           CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Actual return type is ...") */ */);
            }
        } else if fn_typtype == TYPTYPE_COMPOSITE || rettype == RECORDOID {
            /*
             * Returns a rowtype.
             */
            let tupnatts: c_int;
            let mut tuplogcols: c_int;
            let mut colindex: c_int;

            /*
             * If the target list has one non-junk entry, and that expression has
             * or can be coerced to the declared return type, take it as the result.
             */
            if tlistlen == 1 && prokind != PROKIND_PROCEDURE {
                let tle = linitial(tlist) as *mut TargetEntry;
                Assert!(!(*tle).resjunk);
                if coerce_fn_result_column(tle, rettype, -1,
                                            tlist_is_modifiable,
                                            &mut upper_tlist,
                                            &mut upper_tlist_nontrivial)
                {
                    /* Note that we're NOT setting is_tuple_result */
                    break 'tlist_coercion_finished;
                }
            }

            /*
             * If the caller didn't provide an expected tupdesc, we can't do any
             * further checking.  Assume we're returning the whole tuple.
             */
            if rettupdesc.is_null() {
                return true;
            }

            /*
             * Verify that the targetlist matches the return tuple type.
             */
            tupnatts = (*rettupdesc).natts;
            tuplogcols = 0;
            colindex = 0;

            {
                let mut ti: c_int = 0;
                while ti < list_length(tlist) {
                    let tle = (*(*tlist).elements.add(ti as usize)).ptr_value as *mut TargetEntry;
                    let mut attr: *mut FormData_pg_attribute;

                    /* resjunk columns can simply be ignored */
                    if (*tle).resjunk {
                        ti += 1;
                        continue;
                    }

                    loop {
                        colindex += 1;
                        if colindex > tupnatts {
                            ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                                       CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Final statement returns too many columns.") */ */);
                        }
                        attr = TupleDescAttr(rettupdesc, colindex - 1);
                        if (*attr).attisdropped && insertDroppedCols {
                            let null_expr: *mut Expr;

                            /* The type of the null we insert isn't important */
                            null_expr = makeConst(INT4OID, -1, InvalidOid,
                                                   std::mem::size_of::<i32>() as i32,
                                                   0 as Datum,
                                                   true,  /* isnull */
                                                   true /* byval */) as *mut Expr;
                            upper_tlist = lappend(upper_tlist,
                                                   makeTargetEntry(null_expr,
                                                                   (list_length(upper_tlist) + 1) as i16,
                                                                   ptr::null_mut(),
                                                                   false) as *mut c_void);
                            upper_tlist_nontrivial = true;
                        }
                        if !(*attr).attisdropped {
                            break;
                        }
                    }
                    tuplogcols += 1;

                    if !coerce_fn_result_column(tle,
                                                 (*attr).atttypid, (*attr).atttypmod,
                                                 tlist_is_modifiable,
                                                 &mut upper_tlist,
                                                 &mut upper_tlist_nontrivial)
                    {
                        ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                                   CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Final statement returns wrong type at column ...") */ */);
                    }

                    ti += 1;
                }
            }

            /* remaining columns in rettupdesc had better all be dropped */
            colindex += 1;
            while colindex <= tupnatts {
                if !(*TupleDescCompactAttr(rettupdesc, colindex - 1)).attisdropped {
                    ereport!(ERROR, errmsg!("return type mismatch in function declared to return {}",
                               CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                        /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) /* C also: errdetail("Final statement returns too few columns.") */ */);
                }
                if insertDroppedCols {
                    let null_expr: *mut Expr;

                    /* The type of the null we insert isn't important */
                    null_expr = makeConst(INT4OID, -1, InvalidOid,
                                           std::mem::size_of::<i32>() as i32,
                                           0 as Datum,
                                           true,  /* isnull */
                                           true /* byval */) as *mut Expr;
                    upper_tlist = lappend(upper_tlist,
                                           makeTargetEntry(null_expr,
                                                           (list_length(upper_tlist) + 1) as i16,
                                                           ptr::null_mut(),
                                                           false) as *mut c_void);
                    upper_tlist_nontrivial = true;
                }
                colindex += 1;
            }

            /* Report that we are returning entire tuple result */
            is_tuple_result = true;
        } else {
            ereport!(ERROR, errmsg!("return type {} is not supported for SQL functions",
                       CStr::from_ptr(format_type_be(rettype)).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */);
        }
    } // 'tlist_coercion_finished

    /*
     * If necessary, modify the final Query by injecting an extra Query level
     * that just performs a projection.
     */
    if upper_tlist_nontrivial {
        let newquery: *mut Query;
        let mut colnames: *mut List = NIL;
        let rte: *mut RangeTblEntry;
        let rtr: *mut RangeTblRef;

        Assert!((*parse).commandType == CMD_SELECT);

        /* Most of the upper Query struct can be left as zeroes/nulls */
        newquery = makeNode_Query();
        (*newquery).commandType = CMD_SELECT;
        (*newquery).querySource = (*parse).querySource;
        (*newquery).canSetTag = true;
        (*newquery).targetList = upper_tlist;

        /* We need a moderately realistic colnames list for the subquery RTE */
        {
            let ptlist = (*parse).targetList;
            let mut ci: c_int = 0;
            while ci < list_length(ptlist) {
                let tle = (*(*ptlist).elements.add(ci as usize)).ptr_value as *mut TargetEntry;
                if (*tle).resjunk {
                    ci += 1;
                    continue;
                }
                let name = if (*tle).resname.is_null() {
                    b"\0".as_ptr() as *mut c_char
                } else {
                    (*tle).resname
                };
                colnames = lappend(colnames, makeString(name) as *mut c_void);
                ci += 1;
            }
        }

        /* Build a suitable RTE for the subquery */
        rte = makeNode_RangeTblEntry();
        (*rte).rtekind = RTEKind::RTE_SUBQUERY;
        (*rte).subquery = parse;
        (*rte).eref = makeAlias(b"*SELECT*\0".as_ptr() as *const c_char, colnames);
        (*rte).alias = (*rte).eref;
        (*rte).lateral = false;
        (*rte).inh = false;
        (*rte).inFromCl = true;
        (*newquery).rtable = list_make1!(rte as *mut c_void);

        rtr = makeNode_RangeTblRef();
        (*rtr).rtindex = 1;
        (*newquery).jointree = makeFromExpr(list_make1!(rtr as *mut c_void),
                                             ptr::null_mut());

        /*
         * Make sure the new query is marked as having row security if the
         * original one does.
         */
        (*newquery).hasRowSecurity = (*parse).hasRowSecurity;

        /* Replace original query in the correct element of the query list */
        (*parse_cell).ptr_value = newquery as *mut c_void;
    }

    is_tuple_result
}

/*
 * Process one function result column for check_sql_fn_retval
 */
unsafe fn coerce_fn_result_column(
    src_tle: *mut TargetEntry,
    res_type: Oid,
    res_typmod: i32,
    tlist_is_modifiable: bool,
    upper_tlist: *mut *mut List,
    upper_tlist_nontrivial: *mut bool,
) -> bool {
    let new_tle: *mut TargetEntry;
    let new_tle_expr: *mut Expr;
    let cast_result: *mut Node;

    /*
     * If the TLE has a sortgroupref marking, don't change it.
     * Otherwise, it's safe to modify in-place unless the query as a whole
     * has issues with that.
     */
    if tlist_is_modifiable && (*src_tle).ressortgroupref == 0 {
        /* OK to modify src_tle in place, if necessary */
        cast_result = coerce_to_target_type(ptr::null_mut(),
                                              (*src_tle).expr as *mut Node,
                                              exprType((*src_tle).expr as *mut Node),
                                              res_type, res_typmod,
                                              COERCION_ASSIGNMENT,
                                              COERCE_IMPLICIT_CAST,
                                              -1);
        if cast_result.is_null() {
            return false;
        }
        assign_expr_collations(ptr::null_mut(), cast_result);
        (*src_tle).expr = cast_result as *mut Expr;
        /* Make a Var referencing the possibly-modified TLE */
        new_tle_expr = makeVarFromTargetEntry(1, src_tle) as *mut Expr;
    } else {
        /* Any casting must happen in the upper tlist */
        let var = makeVarFromTargetEntry(1, src_tle);

        cast_result = coerce_to_target_type(ptr::null_mut(),
                                              var as *mut Node,
                                              (*var).vartype,
                                              res_type, res_typmod,
                                              COERCION_ASSIGNMENT,
                                              COERCE_IMPLICIT_CAST,
                                              -1);
        if cast_result.is_null() {
            return false;
        }
        assign_expr_collations(ptr::null_mut(), cast_result);
        /* Did the coercion actually do anything? */
        if cast_result != var as *mut Node {
            *upper_tlist_nontrivial = true;
        }
        new_tle_expr = cast_result as *mut Expr;
    }
    new_tle = makeTargetEntry(new_tle_expr,
                               (list_length(*upper_tlist) + 1) as i16,
                               (*src_tle).resname, false);
    *upper_tlist = lappend(*upper_tlist, new_tle as *mut c_void);
    true
}

/*
 * Extract the targetlist of the last canSetTag query in the given list
 * of parsed-and-rewritten Queries.  Returns NIL if there is none.
 */
unsafe fn get_sql_fn_result_tlist(queryTreeList: *mut List) -> *mut List {
    let mut parse: *mut Query = ptr::null_mut();

    {
        let mut gi: c_int = 0;
        while gi < list_length(queryTreeList) {
            let q = (*(*queryTreeList).elements.add(gi as usize)).ptr_value as *mut Query;
            if (*q).canSetTag {
                parse = q;
            }
            gi += 1;
        }
    }

    if !parse.is_null() && (*parse).commandType == CMD_SELECT {
        return (*parse).targetList;
    } else if !parse.is_null() &&
        ((*parse).commandType == CMD_INSERT ||
         (*parse).commandType == CMD_UPDATE ||
         (*parse).commandType == CMD_DELETE ||
         (*parse).commandType == CMD_MERGE) &&
        !(*parse).returningList.is_null()
    {
        return (*parse).returningList;
    } else {
        return NIL;
    }
}

/*
 * CreateSQLFunctionDestReceiver -- create a suitable DestReceiver object
 */
pub unsafe fn CreateSQLFunctionDestReceiver() -> *mut DestReceiver {
    let self_ = palloc0(std::mem::size_of::<DR_sqlfunction>()) as *mut DR_sqlfunction;

    (*self_).pub_.receiveSlot = Some(sqlfunction_receive);
    (*self_).pub_.rStartup = Some(sqlfunction_startup);
    (*self_).pub_.rShutdown = Some(sqlfunction_shutdown);
    (*self_).pub_.rDestroy = Some(sqlfunction_destroy);
    (*self_).pub_.mydest = DestSQLFunction;

    /* private fields will be set by postquel_start */

    self_ as *mut DestReceiver
}

/*
 * sqlfunction_startup --- executor startup
 */
unsafe fn sqlfunction_startup(self_: *mut DestReceiver, operation: c_int, typeinfo: TupleDesc) {
    /* no-op */
}

/*
 * sqlfunction_receive --- receive one tuple
 */
unsafe fn sqlfunction_receive(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let myState = self_ as *mut DR_sqlfunction;

    if !(*myState).tstore.is_null() {
        /* We are collecting all of a set result into the tuplestore */

        /* Filter tuple as needed */
        let slot = ExecFilterJunk((*myState).filter, slot);

        /* Store the filtered tuple into the tuplestore */
        tuplestore_puttupleslot((*myState).tstore, slot);
    } else {
        /*
         * We only want the first tuple, which we'll save in the junkfilter's
         * result slot.  Ignore any additional tuples passed.
         */
        if TTS_EMPTY((*(*myState).filter).jf_resultSlot) {
            /* Filter tuple as needed */
            let slot = ExecFilterJunk((*myState).filter, slot);
            Assert!(slot == (*(*myState).filter).jf_resultSlot);

            /* Materialize the slot so it preserves pass-by-ref values */
            ExecMaterializeSlot(slot);
        }
    }

    true
}

/*
 * sqlfunction_shutdown --- executor end
 */
unsafe fn sqlfunction_shutdown(self_: *mut DestReceiver) {
    /* no-op */
}

/*
 * sqlfunction_destroy --- release DestReceiver object
 */
unsafe fn sqlfunction_destroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut c_void);
}

// TODO(pg-port): CachedPlanSource.query_list field (plancache.h)
unsafe fn plansource_query_list(_plansource: *mut CachedPlanSource) -> *mut List {
    todo!("TODO(pg-port): plansource_query_list")
}

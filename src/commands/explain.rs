//! explain.rs -- Explain query execution plans
//!
//! 1:1 translation of postgres/src/backend/commands/explain.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/explain.c
//!
//! -----------------------------------------------------------------------
//!
//! STUB list (all marked TODO(pg-port)):
//!   - QueryRewrite / QueryRewrite-related rewriteHandler stubs
//!   - ExplainBeginOutput / ExplainEndOutput / ExplainSeparatePlans (explain_format.rs)
//!   - begin_tup_output_tupdesc / do_text_output_multiline / do_text_output_oneline / end_tup_output
//!   - ExplainResultDesc (TupleDesc plumbing)
//!   - pg_plan_query / CreateQueryDesc / ExecutorStart / ExecutorRun / ExecutorFinish / ExecutorEnd / FreeQueryDesc
//!   - ExplainExecuteQuery (prepare.rs)
//!   - CreateTableAsRelExists / CreateIntoRelDestReceiver / GetIntoRelEFlags (createas.rs)
//!   - CreateExplainSerializeDestReceiver / GetSerializationMetrics (libpq/serialize)
//!   - get_explain_guc_options / GetConfigOptionByName (utils/guc_tables)
//!   - select_rtable_names_for_explain / deparse_context_for_plan_tree / set_deparse_context_plan /
//!     deparse_expression / get_window_frame_options_for_explain (utils/ruleutils -- big TODO family)
//!   - planstate_tree_walker (nodes/nodeFuncs.rs -- already ported)
//!   - ExplainOpenGroup / ExplainCloseGroup / ExplainOpenSetAsideGroup / ExplainSaveGroup /
//!     ExplainRestoreGroup / ExplainIndentText / ExplainDummyGroup / ExplainPropertyText /
//!     ExplainPropertyBool / ExplainPropertyInteger / ExplainPropertyUInteger / ExplainPropertyFloat /
//!     ExplainPropertyList / ExplainPropertyListNested (commands/explain_format.rs -- already ported)
//!   - tuplestore_get_stats / tuplesort_get_stats / tuplesort_method_name / tuplesort_space_type_name
//!   - InstrEndLoop / InstrJitAgg (executor/instrument.rs -- already ported)
//!   - bms_add_member / bms_add_members / bms_is_member (nodes/bitmapset)
//!   - rt_fetch / list_nth / lcons / lappend / list_delete_first / linitial / linitial_int / lfirst_int /
//!     lfirst_oid / lfirst_node! / list_make1 / list_length / lnext / NIL (pg_list -- already ported)
//!   - make_andclause / make_orclause / make_ands_explicit / get_tle_by_resno / castNode (nodes utils)
//!   - quote_identifier / get_rel_name / get_namespace_name_or_temp / get_rel_namespace /
//!     get_func_name / get_func_namespace / get_constraint_name / get_opname / get_collation_name /
//!     get_typcollation / lookup_type_cache / exprType / get_equality_op_for_ordering_op (utils/lsyscache)
//!   - JumbleQuery / IsQueryIdEnabled / compute_query_id / COMPUTE_QUERY_ID_REGRESS
//!   - AllocSetContextCreate / MemoryContextSwitchTo / MemoryContextMemConsumed (utils/mmgr)
//!   - pgBufferUsage / BufferUsageAccumDiff (executor/instrument.rs -- stub global)
//!   - PushCopiedSnapshot / PopActiveSnapshot / GetActiveSnapshot / UpdateActiveSnapshotCommandId
//!   - CommandCounterIncrement
//!   - track_io_timing (GUC)
//!   - ScanDirectionIsBackward macro
//!   - foreach_current_index (list macro)
//!   - BuildParamLogString
//!   - trigger instrumentation (report_triggers -> ri_TrigDesc / ri_TrigInstrument)
//!   - JIT instrumentation (ExplainPrintJIT -> jit.rs types)
//!   - INSTR_TIME_* macros (portability::instr_time)
//!   - post_parse_analyze_hook

use crate::prelude::*;

use crate::commands::explain_format::*;
use crate::commands::explain_state::*;
use crate::executor::execdesc::QueryDesc;
use crate::executor::instrument::{
    BufferUsage, Instrumentation, WalUsage, WorkerInstrumentation,
    INSTRUMENT_BUFFERS, INSTRUMENT_ROWS, INSTRUMENT_TIMER, INSTRUMENT_WAL,
};
use crate::jit::jit::{JitInstrumentation, SharedJitInstrumentation};
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoSpaces, appendStringInfoString, initStringInfo,
    resetStringInfo, StringInfo, StringInfoData,
};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{
    AggState, AppendState, BitmapAndState, BitmapHeapScanState, BitmapIndexScanState,
    BitmapOrState, CteScanState, CustomScanState, EState, ForeignScanState, GatherMergeState,
    GatherState, GroupState, HashState, IndexOnlyScanState, IndexScanState, IncrementalSortState,
    MaterialState, MemoizeState, MergeAppendState, ModifyTableState, PlanState, RecursiveUnionState,
    ResultRelInfo, SharedJitInstrumentation as ExecSharedJitInstrumentation, SortState,
    SubPlanState, SubqueryScanState, TableFuncScanState, WindowAggState,
    innerPlanState, outerPlanState,
};
use crate::nodes::pg_list::{
    lappend, lcons, lfirst, linitial, list_delete_first, list_length,
    list_nth, List,
};
use crate::lfirst_node;
use crate::nodes::plannodes::{
    Agg, Append, BitmapHeapScan, BitmapIndexScan, CustomScan, ForeignScan, FunctionScan,
    Gather, GatherMerge, Group, Hash, IndexOnlyScan, IndexScan, IncrementalSort, Join,
    LockRows, Material, MergeAppend, MergeJoin, Memoize, ModifyTable, NestLoop, Plan,
    PlannedStmt, RecursiveUnion, Result as PgResult, Scan, SetOp, Sort, SubqueryScan,
    TableFuncScan, TidRangeScan, TidScan, Unique, WindowAgg,
};
use crate::nodes::parsenodes::{
    CreateTableAsStmt, DeclareCursorStmt, DefElem, ExplainStmt,
    RangeTblEntry, RTEKind::*,
};
use crate::nodes::primnodes::{IntoClause, TableFunc};
use crate::nodes::nodes::{
    Node, NodeTag,
    CmdType::*, JoinType::*, AggStrategy::*, AggSplit::*,
    SetOpCmd::*, SetOpStrategy::*, OnConflictAction::*,
    DO_AGGSPLIT_COMBINE, DO_AGGSPLIT_SKIPFINAL,
};
use crate::{appendStringInfo, current_cell, foreach};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ADD, INSTR_TIME_GET_DOUBLE, INSTR_TIME_GET_MILLISEC,
    INSTR_TIME_IS_ZERO, INSTR_TIME_SET_CURRENT, INSTR_TIME_SET_ZERO, INSTR_TIME_SUBTRACT,
};

// ---------------------------------------------------------------------------
// Opaque stubs for not-yet-ported types
// ---------------------------------------------------------------------------

/// STUB: parser/parse_node.h ParseState
pub type ParseStateExplain = c_void;

pub use crate::nodes::execnodes::{Snapshot, SnapshotData};

/// STUB: tcop/dest.h DestReceiver
pub type DestReceiver = c_void;

/// STUB: tcop/dest.h TupOutputState
pub type TupOutputState = c_void;

/// STUB: nodes/primnodes.h RangeTblFunction
pub type RangeTblFunction = c_void;

/// STUB: utils/guc_tables.h config_generic
pub type config_generic = c_void;

/// STUB: utils/typcache.h TypeCacheEntry
pub type TypeCacheEntry = c_void;

/// STUB: utils/typecache.h -- TYPECACHE flag bits
pub const TYPECACHE_LT_OPR: c_uint = 1 << 0;
pub const TYPECACHE_GT_OPR: c_uint = 1 << 1;

pub use crate::nodes::parsenodes::TableSampleClause;

/// STUB: utils/tuplesort.h Tuplesortstate
pub type Tuplesortstate = c_void;

/// STUB: utils/tuplestore.h Tuplestorestate
pub type Tuplestorestate = c_void;

/// nodes/execnodes.h IncrementalSortGroupInfo -- local mirror (matches execnodes layout)
#[repr(C)]
pub struct IncrementalSortGroupInfo {
    pub groupCount: int64,
    pub maxDiskSpaceUsed: int64,
    pub totalDiskSpaceUsed: int64,
    pub maxMemorySpaceUsed: int64,
    pub totalMemorySpaceUsed: int64,
    pub sortMethods: bits32,
}

/// nodes/execnodes.h IncrementalSortInfo -- local mirror
#[repr(C)]
pub struct IncrementalSortInfo {
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

/// nodes/execnodes.h SharedIncrementalSortInfo -- local mirror
#[repr(C)]
pub struct SharedIncrementalSortInfo {
    pub num_workers: c_int,
    // sinfo: [IncrementalSortInfo; FLEXIBLE_ARRAY_MEMBER]
}

/// STUB: nodes/parsenodes.h ExecuteStmt
pub type ExecuteStmt = c_void;

/// STUB: nodes/parsenodes.h NotifyStmt
pub type NotifyStmt = c_void;

/// STUB: catalog/pg_type.h OIDs
pub const TEXTOID: Oid = 25;
pub const XMLOID: Oid = 142;
pub const JSONOID: Oid = 114;

/// STUB: utils/tupleformat.h Index
pub type Index = c_uint;

/// STUB: nodes/parsenodes.h JumbleState
pub type JumbleState = c_void;

/// STUB: nodes/primnodes.h ParamListInfo
pub type ParamListInfo = c_void;

/// STUB: utils/palloc.h MemoryContextCounters
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MemoryContextCounters {
    pub totalspace: usize,
    pub freespace: usize,
    pub nblocks: usize,
}

/// STUB: utils/mmgr.h MemoryContext
pub type MemoryContext = *mut c_void;

/// STUB: nodes/parsenodes.h Query
pub type Query = c_void;

/// STUB: nodes/primnodes.h TargetEntry
pub type TargetEntry = c_void;

/// STUB: nodes/primnodes.h SubPlan
pub type SubPlan = c_void;

/// STUB: catalog/objectaddress.h ObjectType
pub type ObjectType = c_int;

/// STUB: nodes/parsenodes.h TupleDesc
pub type TupleDesc = *mut c_void;

/// STUB: access/sdir.h ScanDirection
pub type ScanDirection = c_int;
pub const ForwardScanDirection: ScanDirection = 1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const BackwardScanDirection: ScanDirection = -1;

/// STUB: jit/jit.h PGJIT_* flags
pub const PGJIT_PERFORM: c_int = 1 << 0;
pub const PGJIT_OPT3: c_int   = 1 << 1;
pub const PGJIT_INLINE: c_int = 1 << 2;
pub const PGJIT_EXPR: c_int   = 1 << 3;
pub const PGJIT_DEFORM: c_int = 1 << 4;

/// STUB: executor flags
pub const EXEC_FLAG_EXPLAIN_ONLY: c_int = 1 << 1;
pub const EXEC_FLAG_EXPLAIN_GENERIC: c_int = 1 << 2;

/// STUB: nodes/primnodes.h -- cursor options
pub const CURSOR_OPT_PARALLEL_OK: c_int = 0x0800;


/// STUB: TableFuncType
pub const TFT_XMLTABLE: c_int  = 1;
pub const TFT_JSON_TABLE: c_int = 2;

/// STUB: FRAMEOPTION_NONDEFAULT
pub const FRAMEOPTION_NONDEFAULT: c_int = 0x00001;

/// STUB: TuplesortMethod
pub type TuplesortMethod = c_int;
pub const SORT_TYPE_STILL_IN_PROGRESS: TuplesortMethod = 0;
pub const NUM_TUPLESORTMETHODS: c_int = 6;

/// STUB: TuplesortSpaceType
pub type TuplesortSpaceType = c_int;
pub const SORT_SPACE_TYPE_MEMORY: TuplesortSpaceType = 0;
pub const SORT_SPACE_TYPE_DISK: TuplesortSpaceType = 1;

/// STUB: nodes/parsenodes.h FuncExpr
pub type FuncExpr = c_void;
pub const T_FuncExpr: NodeTag = NodeTag::T_FuncExpr;

/// STUB: nodes/execnodes.h BitmapHeapScanInstrumentation fields
pub type BitmapHeapScanInstrSharedInfo = c_void;

/// STUB: nodes/execnodes.h MemoizeInstrumentation
#[repr(C)]
pub struct MemoizeInstrStub {
    pub cache_hits: uint64,
    pub cache_misses: uint64,
    pub cache_evictions: uint64,
    pub cache_overflows: uint64,
    pub mem_peak: int64,
}

/// STUB: nodes/execnodes.h SharedMemoizeInfo
#[repr(C)]
pub struct SharedMemoizeInfo {
    pub num_workers: c_int,
    // sinstrument: [MemoizeInstrumentation; FLEXIBLE_ARRAY_MEMBER]
}

/// STUB: nodes/execnodes.h AggregateInstrumentation
#[repr(C)]
pub struct AggregateInstrStub {
    pub hash_mem_peak: int64,
    pub hash_disk_used: uint64,
    pub hash_batches_used: c_int,
}

/// STUB: nodes/execnodes.h SharedAggInfo
#[repr(C)]
pub struct SharedAggInfo {
    pub num_workers: c_int,
    // sinstrument: [AggregateInstrumentation; FLEXIBLE_ARRAY_MEMBER]
}

/// STUB: FdwRoutine -- foreign data wrapper function table
#[repr(C)]
pub struct FdwRoutine {
    pub ExplainForeignScan: Option<unsafe fn(*mut ForeignScanState, *mut ExplainState)>,
    pub ExplainDirectModify: Option<unsafe fn(*mut ForeignScanState, *mut ExplainState)>,
    pub ExplainForeignModify: Option<
        unsafe fn(*mut ModifyTableState, *mut ResultRelInfo, *mut List, c_int, *mut ExplainState),
    >,
}

/// STUB: nodes/parsenodes.h QueryEnvironment
pub type QueryEnvironment = c_void;

/// STUB: SerializeMetrics
#[repr(C)]
pub struct SerializeMetrics {
    pub timeSpent: instr_time,
    pub bytesSent: uint64,
    pub bufferUsage: BufferUsage,
}

// ---------------------------------------------------------------------------
// Macros translated locally
// ---------------------------------------------------------------------------

/// BYTES_TO_KILOBYTES(b)  ((b + 1023) / 1024)
macro_rules! BYTES_TO_KILOBYTES {
    ($b:expr) => {
        ($b + 1023) / 1024
    };
}

/* DO_AGGSPLIT_SKIPFINAL / DO_AGGSPLIT_COMBINE: use real fns from nodes::nodes */

// ---------------------------------------------------------------------------
// Hook type definitions (mirroring commands/explain.h)
// ---------------------------------------------------------------------------

/// Hook for plugins to get control in ExplainOneQuery()
pub type ExplainOneQuery_hook_type = Option<
    unsafe fn(
        query: *mut Query,
        cursorOptions: c_int,
        into: *mut IntoClause,
        es: *mut ExplainState,
        queryString: *const c_char,
        params: *mut ParamListInfo,
        queryEnv: *mut QueryEnvironment,
    ),
>;

/// Hook for plugins to get control in explain_get_index_name()
pub type explain_get_index_name_hook_type = Option<unsafe fn(indexId: Oid) -> *const c_char>;

/// Per-plan hook for plugins to print additional info
pub type explain_per_plan_hook_type = Option<
    unsafe fn(
        plannedstmt: *mut PlannedStmt,
        into: *mut IntoClause,
        es: *mut ExplainState,
        queryString: *const c_char,
        params: *mut ParamListInfo,
        queryEnv: *mut QueryEnvironment,
    ),
>;

/// Per-node hook for plugins to print additional info
pub type explain_per_node_hook_type = Option<
    unsafe fn(
        planstate: *mut PlanState,
        ancestors: *mut List,
        relationship: *const c_char,
        plan_name: *const c_char,
        es: *mut ExplainState,
    ),
>;

// ---------------------------------------------------------------------------
// File-scope hook globals
// ---------------------------------------------------------------------------

/// Hook for plugins to get control in ExplainOneQuery()
pub static mut ExplainOneQuery_hook: ExplainOneQuery_hook_type = None;

/// Hook for plugins to get control in explain_get_index_name()
pub static mut explain_get_index_name_hook: explain_get_index_name_hook_type = None;

/// Per-plan hook for plugins to print additional info
pub static mut explain_per_plan_hook: explain_per_plan_hook_type = None;

/// Per-node hook for plugins to print additional info
pub static mut explain_per_node_hook: explain_per_node_hook_type = None;

// ---------------------------------------------------------------------------
// External C functions referenced but not yet ported
// ---------------------------------------------------------------------------

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn psprintf(fmt: *const c_char, ...) -> *mut c_char;
}

// ---------------------------------------------------------------------------
// Stub functions for not-yet-ported subsystems
// ---------------------------------------------------------------------------

// TODO(pg-port): rewrite/rewriteHandler.c -- QueryRewrite
unsafe fn QueryRewrite(_query: *mut Query) -> *mut List {
    null_mut()
}

// TODO(pg-port): parser/analyze.c -- post_parse_analyze_hook global
static mut post_parse_analyze_hook: Option<
    unsafe fn(*mut ParseStateExplain, *mut Query, *mut JumbleState),
> = None;

// TODO(pg-port): utils/queryid.c
unsafe fn IsQueryIdEnabled() -> bool {
    false
}

// TODO(pg-port): utils/queryid.c
unsafe fn JumbleQuery(_query: *mut Query) -> *mut JumbleState {
    null_mut()
}

// TODO(pg-port): parser/analyze.h
static mut compute_query_id: c_int = 0;
const COMPUTE_QUERY_ID_REGRESS: c_int = 2;

// TODO(pg-port): tcop/dest.c
static mut None_Receiver: *mut DestReceiver = null_mut();

// TODO(pg-port): commands/createas.c
unsafe fn CreateTableAsRelExists(_ctas: *mut CreateTableAsStmt) -> bool {
    false
}

// TODO(pg-port): commands/createas.c
unsafe fn CreateIntoRelDestReceiver(_into: *mut IntoClause) -> *mut DestReceiver {
    null_mut()
}

// TODO(pg-port): commands/createas.c
unsafe fn GetIntoRelEFlags(_into: *mut IntoClause) -> c_int {
    0
}

// TODO(pg-port): libpq/serialize.c
unsafe fn CreateExplainSerializeDestReceiver(_es: *mut ExplainState) -> *mut DestReceiver {
    null_mut()
}

// TODO(pg-port): libpq/serialize.c
unsafe fn GetSerializationMetrics(_dest: *mut DestReceiver) -> SerializeMetrics {
    SerializeMetrics {
        timeSpent: instr_time::default(),
        bytesSent: 0,
        bufferUsage: BufferUsage::default(),
    }
}

// TODO(pg-port): executor/execMain.c
unsafe fn ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) {}

// TODO(pg-port): executor/execMain.c
unsafe fn ExecutorRun(_queryDesc: *mut QueryDesc, _dir: ScanDirection, _count: u64) {}

// TODO(pg-port): executor/execMain.c
unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) {}

// TODO(pg-port): executor/execMain.c
unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) {}

// TODO(pg-port): planner.c
unsafe fn pg_plan_query(
    _query: *mut Query,
    _queryString: *const c_char,
    _cursorOptions: c_int,
    _params: *mut ParamListInfo,
) -> *mut PlannedStmt {
    null_mut()
}

// TODO(pg-port): commands/prepare.c
unsafe fn ExplainExecuteQuery(
    _stmt: *mut ExecuteStmt,
    _into: *mut IntoClause,
    _es: *mut ExplainState,
    _pstate: *mut ParseStateExplain,
    _params: *mut ParamListInfo,
) {}

// TODO(pg-port): utils/snapmgr.c
unsafe fn PushCopiedSnapshot(_snapshot: Snapshot) {}
unsafe fn PopActiveSnapshot() {}
unsafe fn GetActiveSnapshot() -> Snapshot { null_mut() }
unsafe fn UpdateActiveSnapshotCommandId() {}
static mut InvalidSnapshot: Snapshot = null_mut();

// TODO(pg-port): access/xact.c
unsafe fn CommandCounterIncrement() {}

// TODO(pg-port): utils/guc_tables.c
unsafe fn get_explain_guc_options(_num: *mut c_int) -> *mut *mut config_generic {
    null_mut()
}

// TODO(pg-port): utils/guc.c
unsafe fn GetConfigOptionByName(
    _name: *const c_char,
    _varname: *mut *const c_char,
    _missing_ok: bool,
) -> *mut c_char {
    null_mut()
}

// TODO(pg-port): utils/ruleutils.c -- select_rtable_names_for_explain
unsafe fn select_rtable_names_for_explain(
    _rtable: *mut List,
    _rels_used: *mut Bitmapset,
) -> *mut List {
    null_mut()
}

// TODO(pg-port): utils/ruleutils.c -- deparse_context_for_plan_tree
unsafe fn deparse_context_for_plan_tree(
    _pstmt: *mut PlannedStmt,
    _rtable_names: *mut List,
) -> *mut List {
    null_mut()
}

// TODO(pg-port): utils/ruleutils.c -- set_deparse_context_plan
unsafe fn set_deparse_context_plan(
    _deparse_cxt: *mut List,
    _plan: *mut Plan,
    _ancestors: *mut List,
) -> *mut List {
    null_mut()
}

// TODO(pg-port): utils/ruleutils.c -- deparse_expression
unsafe fn deparse_expression(
    _node: *mut Node,
    _context: *mut List,
    _useprefix: bool,
    _showimplicit: bool,
) -> *mut c_char {
    null_mut()
}

// TODO(pg-port): utils/ruleutils.c -- get_window_frame_options_for_explain
unsafe fn get_window_frame_options_for_explain(
    _frameOptions: c_int,
    _startOffset: *mut Node,
    _endOffset: *mut Node,
    _context: *mut List,
    _useprefix: bool,
) -> *mut c_char {
    null_mut()
}

// TODO(pg-port): nodes/nodeFuncs.c (already ported in nodeFuncs.rs; re-declare here)
extern "C" {}
unsafe fn planstate_tree_walker(
    _planstate: *mut PlanState,
    _walker: unsafe fn(*mut PlanState, *mut *mut Bitmapset) -> bool,
    _context: *mut *mut Bitmapset,
) -> bool {
    false
}

// TODO(pg-port): nodes/bitmapset.c
unsafe fn bms_add_member(bms: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    let _ = x;
    bms
}
unsafe fn bms_add_members(bms: *mut Bitmapset, _other: *mut Bitmapset) -> *mut Bitmapset {
    bms
}
unsafe fn bms_is_member(_x: c_int, _bms: *mut Bitmapset) -> bool {
    false
}

// TODO(pg-port): utils/lsyscache.c
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_namespace_name_or_temp(_nsid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_rel_namespace(_relid: Oid) -> Oid { 0 }
unsafe fn get_func_name(_funcid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_func_namespace(_funcid: Oid) -> Oid { 0 }
unsafe fn get_constraint_name(_conoid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_opname(_opid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_collation_name(_colloid: Oid) -> *mut c_char { null_mut() }
unsafe fn get_typcollation(_typid: Oid) -> Oid { 0 }
unsafe fn exprType(_node: *mut Node) -> Oid { 0 }
unsafe fn lookup_type_cache(_typid: Oid, _flags: c_uint) -> *mut TypeCacheEntry { null_mut() }
unsafe fn get_equality_op_for_ordering_op(_opid: Oid, _reverse: *mut bool) -> Oid { 0 }
unsafe fn OidIsValid(oid: Oid) -> bool { oid != 0 }
unsafe fn quote_identifier(ident: *const c_char) -> *const c_char { ident }
unsafe fn get_tle_by_resno(_tlist: *mut List, _resno: i16) -> *mut TargetEntry { null_mut() }
unsafe fn rt_fetch(_rti: Index, _rtable: *mut List) -> *mut RangeTblEntry { null_mut() }
unsafe fn make_andclause(_clauses: *mut List) -> *mut Node { null_mut() }
unsafe fn make_orclause(_clauses: *mut List) -> *mut Node { null_mut() }
unsafe fn make_ands_explicit(_clauses: *mut List) -> *mut Node { null_mut() }
unsafe fn lfirst_int(_lc: *mut crate::nodes::pg_list::ListCell) -> c_int { 0 }
unsafe fn lfirst_oid(_lc: *mut crate::nodes::pg_list::ListCell) -> Oid { 0 }
unsafe fn linitial_int(_list: *mut List) -> c_int { 0 }
unsafe fn list_make1(_datum: *mut c_void) -> *mut List { null_mut() }
unsafe fn lnext(_list: *mut List, _lc: *mut crate::nodes::pg_list::ListCell) -> *mut crate::nodes::pg_list::ListCell { null_mut() }
unsafe fn unconstify(_: *const c_char, p: *mut c_char) -> *mut c_char { p }
unsafe fn BuildParamLogString(_params: *mut ParamListInfo, _qcontext: *mut c_void, _maxlen: c_int) -> *mut c_char { null_mut() }

// TODO(pg-port): utils/mmgr.h
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _arg0: usize, _arg1: usize, _arg2: usize,
) -> MemoryContext {
    null_mut()
}
unsafe fn MemoryContextSwitchTo(_ctx: MemoryContext) -> MemoryContext { null_mut() }
unsafe fn MemoryContextMemConsumed(_ctx: MemoryContext, _counters: *mut MemoryContextCounters) {}
static mut CurrentMemoryContext: MemoryContext = null_mut();
static mut TopMemoryContext: MemoryContext = null_mut();
const ALLOCSET_DEFAULT_SIZES: (usize, usize, usize) = (0, 0, 0);

// TODO(pg-port): executor/instrument.c
unsafe fn InstrEndLoop(_instr: *mut Instrumentation) {}
// TODO(pg-port): jit/jit.c
unsafe fn InstrJitAgg(
    _dst: *mut JitInstrumentation,
    _src: *mut JitInstrumentation,
) {}

// TODO(pg-port): utils/snapmgr.c -- pgBufferUsage
static mut pgBufferUsage: BufferUsage = BufferUsage {
    shared_blks_hit: 0, shared_blks_read: 0,
    shared_blks_dirtied: 0, shared_blks_written: 0,
    local_blks_hit: 0, local_blks_read: 0,
    local_blks_dirtied: 0, local_blks_written: 0,
    temp_blks_read: 0, temp_blks_written: 0,
    shared_blk_read_time: instr_time { ticks: 0 },
    shared_blk_write_time: instr_time { ticks: 0 },
    local_blk_read_time: instr_time { ticks: 0 },
    local_blk_write_time: instr_time { ticks: 0 },
    temp_blk_read_time: instr_time { ticks: 0 },
    temp_blk_write_time: instr_time { ticks: 0 },
};

// TODO(pg-port): executor/instrument.c
unsafe fn BufferUsageAccumDiff(
    _dst: *mut BufferUsage,
    _add: *const BufferUsage,
    _sub: *const BufferUsage,
) {}

// TODO(pg-port): guc -- track_io_timing
static mut track_io_timing: bool = false;

// TODO(pg-port): catalog/pg_type.h
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc { null_mut() }
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc, _attnum: i16, _attname: *const c_char,
    _oid: Oid, _typmod: i32, _attdim: c_int,
) {}
unsafe fn ExplainResultDesc_inner(_stmt: *mut ExplainStmt) -> TupleDesc { null_mut() }

// TODO(pg-port): tuplesort / tuplestore stats
unsafe fn tuplesort_get_stats(_state: *mut Tuplesortstate, _stats: *mut c_void) {}
unsafe fn tuplesort_method_name(_m: TuplesortMethod) -> *const c_char { c"???".as_ptr() }
unsafe fn tuplesort_space_type_name(_t: TuplesortSpaceType) -> *const c_char { c"???".as_ptr() }
unsafe fn tuplestore_get_stats(
    _ts: *mut Tuplestorestate,
    _storetype: *mut *mut c_char,
    _spaceused: *mut int64,
) {}

// TODO(pg-port): tcop/tcopprot.h
unsafe fn begin_tup_output_tupdesc(
    _dest: *mut DestReceiver,
    _tupdesc: TupleDesc,
    _ops: *const c_void,
) -> *mut TupOutputState {
    null_mut()
}
unsafe fn do_text_output_multiline(_tstate: *mut TupOutputState, _txt: *mut c_char) {}
unsafe fn do_text_output_oneline(_tstate: *mut TupOutputState, _txt: *mut c_char) {}
unsafe fn end_tup_output(_tstate: *mut TupOutputState) {}
static TTSOpsVirtual: [u8; 0] = [];

// TODO(pg-port): TriggerDesc accessor stubs (execnodes::TriggerDesc is opaque)
// Real layout: utils/reltrigger.h
#[repr(C)]
struct TriggerDescLayout {
    triggers: *mut TriggerStubLayout,
    numtriggers: c_int,
    // boolean flags follow; irrelevant here
}
#[repr(C)]
struct TriggerStubLayout {
    tgoid: Oid,
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
    tgattr: *mut i16,
    tgargs: *mut *mut c_char,
    tgqual: *mut c_char,
    tgoldtable: *mut c_char,
    tgnewtable: *mut c_char,
}
unsafe fn trigdesc_numtriggers(td: *mut crate::nodes::execnodes::TriggerDesc) -> c_int {
    (*(td as *mut TriggerDescLayout)).numtriggers
}
unsafe fn trigdesc_trigger(td: *mut crate::nodes::execnodes::TriggerDesc, n: c_int) -> *mut TriggerStubLayout {
    (*(td as *mut TriggerDescLayout)).triggers.offset(n as isize)
}

// TODO(pg-port): RelationGetRelationName accessor stub (access/utils/rel.h)
unsafe fn RelationGetRelationName(rel: crate::nodes::execnodes::Relation) -> *mut c_char {
    crate::utils::rel::RelationGetRelationName(rel)
}

// TODO(pg-port): pfree wrapper
unsafe fn pfree(ptr: *mut c_void) {
    let _ = ptr;
}

// TODO(pg-port): IndexScanInstrumentation.nsearches accessor (access/genam.h is opaque here)
#[repr(C)]
struct IndexScanInstrShim {
    nsearches: uint64,
}
#[repr(C)]
struct SharedIndexScanInstrShim {
    num_workers: c_int,
    // winstrument: [IndexScanInstrShim; FLEXIBLE_ARRAY_MEMBER]
}
unsafe fn iss_instr_nsearches(instr: *mut crate::nodes::execnodes::IndexScanInstrumentation) -> uint64 {
    (*(instr as *mut IndexScanInstrShim)).nsearches
}
unsafe fn shared_iss_num_workers(si: *mut crate::nodes::execnodes::SharedIndexScanInstrumentation) -> c_int {
    (*(si as *mut SharedIndexScanInstrShim)).num_workers
}
unsafe fn shared_iss_winstrument(si: *mut crate::nodes::execnodes::SharedIndexScanInstrumentation, i: c_int) -> *mut IndexScanInstrShim {
    let base = (si as *mut u8).add(
        core::mem::size_of::<SharedIndexScanInstrShim>()
            + i as usize * core::mem::size_of::<IndexScanInstrShim>()
    );
    base as *mut IndexScanInstrShim
}

// TODO(pg-port): TableSampleClause accessor stubs (explain.rs has it as c_void)
// Real layout: nodes/parsenodes.h
#[repr(C)]
struct TableSampleClauseLayout {
    r#type: crate::nodes::nodes::NodeTag,
    tsmhandler: Oid,
    args: *mut crate::nodes::pg_list::List,
    repeatable: *mut crate::nodes::nodes::Node,
}
unsafe fn tsc_tsmhandler(tsc: *mut TableSampleClause) -> Oid {
    (*(tsc as *mut TableSampleClauseLayout)).tsmhandler
}
unsafe fn tsc_args(tsc: *mut TableSampleClause) -> *mut crate::nodes::pg_list::List {
    (*(tsc as *mut TableSampleClauseLayout)).args
}
unsafe fn tsc_repeatable(tsc: *mut TableSampleClause) -> *mut crate::nodes::nodes::Node {
    (*(tsc as *mut TableSampleClauseLayout)).repeatable
}

// helper: render a NUL-terminated C string as &str for format! sites
unsafe fn cstr_s<'a>(s: *const c_char) -> &'a str {
    if s.is_null() { return ""; }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}

// helper: check whether a node tag is IsA(plan, T_*)
unsafe fn nodeTag_plan(plan: *mut Plan) -> NodeTag {
    (*(plan as *mut Node)).r#type
}
unsafe fn IsA_plan(plan: *mut Plan, tag: NodeTag) -> bool {
    nodeTag_plan(plan) == tag
}
unsafe fn IsA_ps(ps: *mut PlanState, tag: NodeTag) -> bool {
    (*(ps as *mut Node)).r#type == tag
}
unsafe fn nodeTag(plan: *mut Plan) -> NodeTag {
    nodeTag_plan(plan)
}

// ---------------------------------------------------------------------------
// ExplainQuery
// ---------------------------------------------------------------------------

/*
 * ExplainQuery -
 *   execute an EXPLAIN command
 */
pub unsafe fn ExplainQuery(
    pstate: *mut ParseStateExplain,
    stmt: *mut ExplainStmt,
    params: *mut ParamListInfo,
    dest: *mut DestReceiver,
) {
    let es: *mut ExplainState = NewExplainState();
    let tstate: *mut TupOutputState;
    let mut jstate: *mut JumbleState = null_mut();
    let query: *mut Query;
    let rewritten: *mut List;

    /* Configure the ExplainState based on the provided options */
    ParseExplainOptionList(es, (*stmt).options, pstate);

    /* Extract the query and, if enabled, jumble it */
    query = (*stmt).query as *mut Query;
    if IsQueryIdEnabled() {
        jstate = JumbleQuery(query);
    }

    if let Some(hook) = post_parse_analyze_hook {
        hook(pstate, query, jstate);
    }

    /*
     * Parse analysis was done already, but we still have to run the rule
     * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
     * came straight from the parser, or suitable locks were acquired by
     * plancache.c.
     */
    rewritten = QueryRewrite((*stmt).query as *mut Query);

    /* emit opening boilerplate */
    ExplainBeginOutput(es);

    if rewritten.is_null() {
        /*
         * In the case of an INSTEAD NOTHING, tell at least that.  But in
         * non-text format, the output is delimited, so this isn't necessary.
         */
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoString((*es).str, c"Query rewrites to nothing\n".as_ptr());
        }
    } else {
        /* Explain every plan */
        foreach!(l, rewritten, {
            ExplainOneQuery(
                lfirst(current_cell!(l)) as *mut Query,
                CURSOR_OPT_PARALLEL_OK,
                null_mut(),
                es,
                pstate,
                params,
            );

            /* Separate plans with an appropriate separator */
            if !lnext(rewritten, current_cell!(l)).is_null() {
                ExplainSeparatePlans(es);
            }
        });
    }

    /* emit closing boilerplate */
    ExplainEndOutput(es);
    Assert!((*es).indent == 0);

    /* output tuples */
    tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc_inner(stmt), &TTSOpsVirtual as *const _ as *const c_void);
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        do_text_output_multiline(tstate, (*(*es).str).data);
    } else {
        do_text_output_oneline(tstate, (*(*es).str).data);
    }
    end_tup_output(tstate);

    pfree((*(*es).str).data as *mut c_void);
}

/*
 * ExplainResultDesc -
 *   construct the result tupledesc for an EXPLAIN
 */
pub unsafe fn ExplainResultDesc(stmt: *mut ExplainStmt) -> TupleDesc {
    let tupdesc: TupleDesc;
    let mut result_type: Oid = TEXTOID;

    /* Check for XML format option */
    foreach!(lc, (*stmt).options, {
        let opt = lfirst(current_cell!(lc)) as *mut DefElem;

        if strcmp((*opt).defname, c"format".as_ptr()) == 0 {
            let p = crate::commands::define::defGetString(opt);

            if strcmp(p, c"xml".as_ptr()) == 0 {
                result_type = XMLOID;
            } else if strcmp(p, c"json".as_ptr()) == 0 {
                result_type = JSONOID;
            } else {
                result_type = TEXTOID;
            }
            /* don't "break", as ExplainQuery will use the last value */
        }
    });

    /* Need a tuple descriptor representing a single TEXT or XML column */
    tupdesc = CreateTemplateTupleDesc(1);
    TupleDescInitEntry(tupdesc, 1i16, c"QUERY PLAN".as_ptr(), result_type, -1, 0);
    tupdesc
}

/*
 * ExplainOneQuery -
 *   print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
unsafe fn ExplainOneQuery(
    query: *mut Query,
    cursorOptions: c_int,
    into: *mut IntoClause,
    es: *mut ExplainState,
    pstate: *mut ParseStateExplain,
    params: *mut ParamListInfo,
) {
    /* planner will not cope with utility statements */
    // TODO(pg-port): query->commandType check -- using stub CMD_UTILITY
    // if (*query).commandType == CMD_UTILITY { ... }

    /* if an advisor plugin is present, let it manage things */
    if let Some(hook) = ExplainOneQuery_hook {
        // TODO(pg-port): pstate->p_sourcetext / p_queryEnv fields not yet accessible
        hook(
            query, cursorOptions, into, es,
            null(), null_mut(), null_mut(),
        );
    } else {
        standard_ExplainOneQuery(
            query, cursorOptions, into, es,
            null(), params, null_mut(),
        );
    }
}

/*
 * standard_ExplainOneQuery -
 *   print out the execution plan for one Query, without calling a hook.
 */
pub unsafe fn standard_ExplainOneQuery(
    query: *mut Query,
    cursorOptions: c_int,
    into: *mut IntoClause,
    es: *mut ExplainState,
    queryString: *const c_char,
    params: *mut ParamListInfo,
    queryEnv: *mut QueryEnvironment,
) {
    let plan: *mut PlannedStmt;
    let mut planstart: instr_time = core::mem::zeroed();
    let mut planduration: instr_time = core::mem::zeroed();
    let mut bufusage_start: BufferUsage = core::mem::zeroed();
    let mut bufusage: BufferUsage = core::mem::zeroed();
    let mut mem_counters: MemoryContextCounters = core::mem::zeroed();
    let mut planner_ctx: MemoryContext = null_mut();
    let mut saved_ctx: MemoryContext = null_mut();

    if (*es).memory {
        /*
         * Create a new memory context to measure planner's memory consumption
         * accurately.  Note that if the planner were to be modified to use a
         * different memory context type, here we would be changing that to
         * AllocSet, which might be undesirable.  However, we don't have a way
         * to create a context of the same type as another, so we pray and
         * hope that this is OK.
         */
        planner_ctx = AllocSetContextCreate(
            CurrentMemoryContext,
            c"explain analyze planner context".as_ptr(),
            ALLOCSET_DEFAULT_SIZES.0,
            ALLOCSET_DEFAULT_SIZES.1,
            ALLOCSET_DEFAULT_SIZES.2,
        );
        saved_ctx = MemoryContextSwitchTo(planner_ctx);
    }

    if (*es).buffers {
        bufusage_start = pgBufferUsage;
    }
    INSTR_TIME_SET_CURRENT(&mut planstart);

    /* plan the query */
    plan = pg_plan_query(query, queryString, cursorOptions, params);

    INSTR_TIME_SET_CURRENT(&mut planduration);
    INSTR_TIME_SUBTRACT(&mut planduration, planstart);

    if (*es).memory {
        MemoryContextSwitchTo(saved_ctx);
        MemoryContextMemConsumed(planner_ctx, &mut mem_counters);
    }

    /* calc differences of buffer counters. */
    if (*es).buffers {
        memset(&mut bufusage as *mut BufferUsage as *mut c_void, 0, core::mem::size_of::<BufferUsage>());
        BufferUsageAccumDiff(&mut bufusage, &pgBufferUsage, &bufusage_start);
    }

    /* run it (if needed) and produce output */
    ExplainOnePlan(
        plan, into, es, queryString, params, queryEnv,
        &planduration,
        if (*es).buffers { &bufusage } else { null() },
        if (*es).memory { &mem_counters } else { null() },
    );
}

/*
 * ExplainOneUtility -
 *   print out the execution plan for one utility statement
 */
pub unsafe fn ExplainOneUtility(
    utilityStmt: *mut Node,
    into: *mut IntoClause,
    es: *mut ExplainState,
    pstate: *mut ParseStateExplain,
    params: *mut ParamListInfo,
) {
    if utilityStmt.is_null() {
        return;
    }

    if (*utilityStmt).r#type == NodeTag::T_CreateTableAsStmt {
        /*
         * We have to rewrite the contained SELECT and then pass it back to
         * ExplainOneQuery.  Copy to be safe in the EXPLAIN EXECUTE case.
         */
        let ctas = utilityStmt as *mut CreateTableAsStmt;
        let ctas_query: *mut Query;
        let rewritten: *mut List;
        let mut jstate: *mut JumbleState = null_mut();

        /*
         * Check if the relation exists or not.  This is done at this stage to
         * avoid query planning or execution.
         */
        if CreateTableAsRelExists(ctas) {
            // TODO(pg-port): ctas->objtype comparison needs ObjectType port
            ExplainDummyGroup(c"CREATE TABLE AS".as_ptr(), null(), es);
            return;
        }

        ctas_query = copyObject((*ctas).query as *mut c_void) as *mut Query;
        if IsQueryIdEnabled() {
            jstate = JumbleQuery(ctas_query);
        }
        if let Some(hook) = post_parse_analyze_hook {
            hook(pstate, ctas_query, jstate);
        }
        rewritten = QueryRewrite(ctas_query);
        Assert!(list_length(rewritten) == 1);
        ExplainOneQuery(
            linitial(rewritten) as *mut Query,
            CURSOR_OPT_PARALLEL_OK,
            (*ctas).into,
            es,
            pstate,
            params,
        );
    } else if (*utilityStmt).r#type == NodeTag::T_DeclareCursorStmt {
        /*
         * Likewise for DECLARE CURSOR.
         *
         * Notice that if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll
         * actually run the query.  This is different from pre-8.3 behavior
         * but seems more useful than not running the query.  No cursor will
         * be created, however.
         */
        let dcs = utilityStmt as *mut DeclareCursorStmt;
        let dcs_query: *mut Query;
        let rewritten: *mut List;
        let mut jstate: *mut JumbleState = null_mut();

        dcs_query = copyObject((*dcs).query as *mut c_void) as *mut Query;
        if IsQueryIdEnabled() {
            jstate = JumbleQuery(dcs_query);
        }
        if let Some(hook) = post_parse_analyze_hook {
            hook(pstate, dcs_query, jstate);
        }

        rewritten = QueryRewrite(dcs_query);
        Assert!(list_length(rewritten) == 1);
        ExplainOneQuery(
            linitial(rewritten) as *mut Query,
            (*dcs).options,
            null_mut(),
            es,
            pstate,
            params,
        );
    } else if (*utilityStmt).r#type == NodeTag::T_ExecuteStmt {
        ExplainExecuteQuery(utilityStmt as *mut ExecuteStmt, into, es, pstate, params);
    } else if (*utilityStmt).r#type == NodeTag::T_NotifyStmt {
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoString((*es).str, c"NOTIFY\n".as_ptr());
        } else {
            ExplainDummyGroup(c"Notify".as_ptr(), null(), es);
        }
    } else {
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoString(
                (*es).str,
                c"Utility statements have no plan structure\n".as_ptr(),
            );
        } else {
            ExplainDummyGroup(c"Utility Statement".as_ptr(), null(), es);
        }
    }
}

// TODO(pg-port): copyObject is in nodes/copyfuncs.c
unsafe fn copyObject(obj: *mut c_void) -> *mut c_void { obj }

// ---------------------------------------------------------------------------
// ExplainOnePlan
// ---------------------------------------------------------------------------

/*
 * ExplainOnePlan -
 *     given a planned query, execute it if needed, and then print
 *     EXPLAIN output
 */
pub unsafe fn ExplainOnePlan(
    plannedstmt: *mut PlannedStmt,
    into: *mut IntoClause,
    es: *mut ExplainState,
    queryString: *const c_char,
    params: *mut ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    planduration: *const instr_time,
    bufusage: *const BufferUsage,
    mem_counters: *const MemoryContextCounters,
) {
    let dest: *mut DestReceiver;
    let queryDesc: *mut QueryDesc;
    let mut starttime: instr_time = core::mem::zeroed();
    let mut totaltime: f64 = 0.0;
    let eflags: c_int;
    let mut instrument_option: c_int = 0;
    let mut serializeMetrics: SerializeMetrics = core::mem::zeroed();

    // Assert(plannedstmt->commandType != CMD_UTILITY);

    if (*es).analyze && (*es).timing {
        instrument_option |= INSTRUMENT_TIMER;
    } else if (*es).analyze {
        instrument_option |= INSTRUMENT_ROWS;
    }

    if (*es).buffers {
        instrument_option |= INSTRUMENT_BUFFERS;
    }
    if (*es).wal {
        instrument_option |= INSTRUMENT_WAL;
    }

    /*
     * We always collect timing for the entire statement, even when node-level
     * timing is off, so we don't look at es->timing here.  (We could skip
     * this if !es->summary, but it's hardly worth the complication.)
     */
    INSTR_TIME_SET_CURRENT(&mut starttime);

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.
     */
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    /*
     * We discard the output if we have no use for it.  If we're explaining
     * CREATE TABLE AS, we'd better use the appropriate tuple receiver, while
     * the SERIALIZE option requires its own tuple receiver.  (If you specify
     * SERIALIZE while explaining CREATE TABLE AS, you'll see zeroes for the
     * results, which is appropriate since no data would have gone to the
     * client.)
     */
    if !into.is_null() {
        dest = CreateIntoRelDestReceiver(into);
    } else if (*es).serialize != EXPLAIN_SERIALIZE_NONE {
        dest = CreateExplainSerializeDestReceiver(es);
    } else {
        dest = None_Receiver;
    }

    /* Create a QueryDesc for the query */
    queryDesc = crate::executor::execdesc::CreateQueryDesc(
        plannedstmt,
        queryString,
        GetActiveSnapshot(),
        InvalidSnapshot,
        dest as *mut crate::tcop::dest::DestReceiver,
        params as *mut crate::nodes::params::ParamListInfoData,
        queryEnv as *mut crate::utils::misc::queryenvironment::QueryEnvironment,
        instrument_option,
    );

    /* Select execution options */
    if (*es).analyze {
        eflags = 0; /* default run-to-completion flags */
    } else {
        let mut ef = EXEC_FLAG_EXPLAIN_ONLY;
        if (*es).generic {
            ef |= EXEC_FLAG_EXPLAIN_GENERIC;
        }
        if !into.is_null() {
            ef |= GetIntoRelEFlags(into);
        }
        /* call ExecutorStart to prepare the plan for execution */
        ExecutorStart(queryDesc, ef);

        /* grab serialization metrics before we destroy the DestReceiver */
        if (*es).serialize != EXPLAIN_SERIALIZE_NONE {
            serializeMetrics = GetSerializationMetrics(dest);
        }

        /* call the DestReceiver's destroy method even during explain */
        // dest->rDestroy(dest); -- TODO(pg-port): DestReceiver vtable

        ExplainOpenGroup(c"Query".as_ptr(), null(), true, es);

        /* Create textual dump of plan tree */
        ExplainPrintPlan(es, queryDesc);

        /* Show buffer and/or memory usage in planning */
        if peek_buffer_usage(es, bufusage) || !mem_counters.is_null() {
            ExplainOpenGroup(c"Planning".as_ptr(), c"Planning".as_ptr(), true, es);

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfoString((*es).str, c"Planning:\n".as_ptr());
                (*es).indent += 1;
            }

            if !bufusage.is_null() {
                show_buffer_usage(es, bufusage);
            }

            if !mem_counters.is_null() {
                show_memory_counters(es, mem_counters);
            }

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                (*es).indent -= 1;
            }

            ExplainCloseGroup(c"Planning".as_ptr(), c"Planning".as_ptr(), true, es);
        }

        if (*es).summary && !planduration.is_null() {
            let plantime = INSTR_TIME_GET_DOUBLE(*planduration);
            ExplainPropertyFloat(c"Planning Time".as_ptr(), c"ms".as_ptr(), 1000.0 * plantime, 3, es);
        }

        /* Print info about runtime of triggers */
        // (es->analyze is false here, so ExplainPrintTriggers skipped)

        if (*es).costs {
            ExplainPrintJITSummary(es, queryDesc);
        }

        if (*es).serialize != EXPLAIN_SERIALIZE_NONE {
            ExplainPrintSerialize(es, &serializeMetrics);
        }

        if let Some(hook) = explain_per_plan_hook {
            hook(plannedstmt, into, es, queryString, params, queryEnv);
        }

        INSTR_TIME_SET_CURRENT(&mut starttime);
        ExecutorEnd(queryDesc);
        crate::executor::execdesc::FreeQueryDesc(queryDesc);
        PopActiveSnapshot();

        totaltime += elapsed_time(&mut starttime);

        // (es->analyze is false: no execution time reporting)

        ExplainCloseGroup(c"Query".as_ptr(), null(), true, es);
        return;
    }

    // analyze == true path
    {
        let mut ef = 0i32;
        if (*es).generic {
            ef |= EXEC_FLAG_EXPLAIN_GENERIC;
        }
        if !into.is_null() {
            ef |= GetIntoRelEFlags(into);
        }
        ExecutorStart(queryDesc, ef);

        /* Execute the plan for statistics if asked for */
        {
            let dir: ScanDirection;

            /* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
            if !into.is_null() && (*into).skipData {
                dir = NoMovementScanDirection;
            } else {
                dir = ForwardScanDirection;
            }

            /* run the plan */
            ExecutorRun(queryDesc, dir, 0);

            /* run cleanup too */
            ExecutorFinish(queryDesc);

            /* We can't run ExecutorEnd 'till we're done printing the stats... */
            totaltime += elapsed_time(&mut starttime);
        }

        /* grab serialization metrics before we destroy the DestReceiver */
        if (*es).serialize != EXPLAIN_SERIALIZE_NONE {
            serializeMetrics = GetSerializationMetrics(dest);
        }

        /* call the DestReceiver's destroy method even during explain */
        // dest->rDestroy(dest); -- TODO(pg-port)

        ExplainOpenGroup(c"Query".as_ptr(), null(), true, es);

        /* Create textual dump of plan tree */
        ExplainPrintPlan(es, queryDesc);

        /* Show buffer and/or memory usage in planning */
        if peek_buffer_usage(es, bufusage) || !mem_counters.is_null() {
            ExplainOpenGroup(c"Planning".as_ptr(), c"Planning".as_ptr(), true, es);

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfoString((*es).str, c"Planning:\n".as_ptr());
                (*es).indent += 1;
            }

            if !bufusage.is_null() {
                show_buffer_usage(es, bufusage);
            }

            if !mem_counters.is_null() {
                show_memory_counters(es, mem_counters);
            }

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                (*es).indent -= 1;
            }

            ExplainCloseGroup(c"Planning".as_ptr(), c"Planning".as_ptr(), true, es);
        }

        if (*es).summary && !planduration.is_null() {
            let plantime = INSTR_TIME_GET_DOUBLE(*planduration);
            ExplainPropertyFloat(c"Planning Time".as_ptr(), c"ms".as_ptr(), 1000.0 * plantime, 3, es);
        }

        /* Print info about runtime of triggers */
        if (*es).analyze {
            ExplainPrintTriggers(es, queryDesc);
        }

        /*
         * Print info about JITing.  Tied to es->costs because we don't want to
         * display this in regression tests, as it'd cause output differences
         * depending on build options.
         */
        if (*es).costs {
            ExplainPrintJITSummary(es, queryDesc);
        }

        /* Print info about serialization of output */
        if (*es).serialize != EXPLAIN_SERIALIZE_NONE {
            ExplainPrintSerialize(es, &serializeMetrics);
        }

        /* Allow plugins to print additional information */
        if let Some(hook) = explain_per_plan_hook {
            hook(plannedstmt, into, es, queryString, params, queryEnv);
        }

        /*
         * Close down the query and free resources.  Include time for this in the
         * total execution time (although it should be pretty minimal).
         */
        INSTR_TIME_SET_CURRENT(&mut starttime);

        ExecutorEnd(queryDesc);
        crate::executor::execdesc::FreeQueryDesc(queryDesc);
        PopActiveSnapshot();

        /* We need a CCI just in case query expanded to multiple plans */
        if (*es).analyze {
            CommandCounterIncrement();
        }

        totaltime += elapsed_time(&mut starttime);

        /*
         * We only report execution time if we actually ran the query (that is,
         * the user specified ANALYZE), and if summary reporting is enabled.
         */
        if (*es).summary && (*es).analyze {
            ExplainPropertyFloat(
                c"Execution Time".as_ptr(),
                c"ms".as_ptr(),
                1000.0 * totaltime,
                3,
                es,
            );
        }

        ExplainCloseGroup(c"Query".as_ptr(), null(), true, es);
    }
}

/*
 * ExplainPrintSettings -
 *    Print summary of modified settings affecting query planning.
 */
unsafe fn ExplainPrintSettings(es: *mut ExplainState) {
    let mut num: c_int = 0;
    let gucs: *mut *mut config_generic;

    /* bail out if information about settings not requested */
    if !(*es).settings {
        return;
    }

    /* request an array of relevant settings */
    gucs = get_explain_guc_options(&mut num);

    if (*es).format != EXPLAIN_FORMAT_TEXT {
        ExplainOpenGroup(c"Settings".as_ptr(), c"Settings".as_ptr(), true, es);

        for _i in 0..num {
            /* TODO(pg-port): config_generic.name not yet accessible (opaque type); skip */
        }

        ExplainCloseGroup(c"Settings".as_ptr(), c"Settings".as_ptr(), true, es);
    } else {
        let mut str: StringInfoData = core::mem::zeroed();

        /* In TEXT mode, print nothing if there are no options */
        if num <= 0 {
            return;
        }

        initStringInfo(&mut str);

        for _i in 0..num {
            /* TODO(pg-port): config_generic.name field not accessible (opaque type) */
        }

        ExplainPropertyText(c"Settings".as_ptr(), str.data, es);
    }
}

/*
 * ExplainPrintPlan -
 *   convert a QueryDesc's plan tree to text and append it to es->str
 */
pub unsafe fn ExplainPrintPlan(es: *mut ExplainState, queryDesc: *mut QueryDesc) {
    let mut rels_used: *mut Bitmapset = null_mut();
    let ps: *mut PlanState;

    /* Set up ExplainState fields associated with this plan tree */
    Assert!(!(*queryDesc).plannedstmt.is_null());
    (*es).pstmt = (*queryDesc).plannedstmt;
    (*es).rtable = (*(*queryDesc).plannedstmt).rtable;
    ExplainPreScanNode((*queryDesc).planstate, &mut rels_used);
    (*es).rtable_names = select_rtable_names_for_explain((*es).rtable, rels_used);
    (*es).deparse_cxt = deparse_context_for_plan_tree(
        (*queryDesc).plannedstmt,
        (*es).rtable_names,
    );
    (*es).printed_subplans = null_mut();
    (*es).rtable_size = list_length((*es).rtable);
    foreach!(lc, (*es).rtable, {
        let rte = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_GROUP {
            (*es).rtable_size -= 1;
            break;
        }
    });

    /*
     * Sometimes we mark a Gather node as "invisible", which means that it's
     * not to be displayed in EXPLAIN output.  The purpose of this is to allow
     * running regression tests with debug_parallel_query=regress to get the
     * same results as running the same tests with debug_parallel_query=off.
     */
    let mut ps = (*queryDesc).planstate;
    if IsA_ps(ps, NodeTag::T_GatherState)
        && (*((*ps).plan as *mut Gather)).invisible
    {
        ps = outerPlanState(ps);
        (*es).hide_workers = true;
    }
    ExplainNode(ps, null_mut(), null(), null(), es);

    /*
     * If requested, include information about GUC parameters with values that
     * don't match the built-in defaults.
     */
    ExplainPrintSettings(es);

    /*
     * COMPUTE_QUERY_ID_REGRESS means COMPUTE_QUERY_ID_AUTO, but we don't show
     * the queryid in any of the EXPLAIN plans to keep stable the results
     * generated by regression test suites.
     */
    if (*es).verbose
        && (*(*queryDesc).plannedstmt).queryId != 0
        && compute_query_id != COMPUTE_QUERY_ID_REGRESS
    {
        ExplainPropertyInteger(
            c"Query Identifier".as_ptr(),
            null(),
            (*(*queryDesc).plannedstmt).queryId as int64,
            es,
        );
    }
}

/*
 * ExplainPrintTriggers -
 *   convert a QueryDesc's trigger statistics to text and append it to
 *   es->str
 */
pub unsafe fn ExplainPrintTriggers(es: *mut ExplainState, queryDesc: *mut QueryDesc) {
    let rInfo: *mut ResultRelInfo;
    let show_relname: bool;
    let resultrels: *mut List;
    let routerels: *mut List;
    let targrels: *mut List;

    resultrels = (*(*queryDesc).estate).es_opened_result_relations;
    routerels = (*(*queryDesc).estate).es_tuple_routing_result_relations;
    targrels = (*(*queryDesc).estate).es_trig_target_relations;

    ExplainOpenGroup(c"Triggers".as_ptr(), c"Triggers".as_ptr(), false, es);

    show_relname = list_length(resultrels) > 1
        || !routerels.is_null()
        || !targrels.is_null();

    foreach!(l, resultrels, {
        let rInfo = lfirst(current_cell!(l)) as *mut ResultRelInfo;
        report_triggers(rInfo, show_relname, es);
    });

    foreach!(l, routerels, {
        let rInfo = lfirst(current_cell!(l)) as *mut ResultRelInfo;
        report_triggers(rInfo, show_relname, es);
    });

    foreach!(l, targrels, {
        let rInfo = lfirst(current_cell!(l)) as *mut ResultRelInfo;
        report_triggers(rInfo, show_relname, es);
    });

    ExplainCloseGroup(c"Triggers".as_ptr(), c"Triggers".as_ptr(), false, es);
}

/*
 * ExplainPrintJITSummary -
 *    Print summarized JIT instrumentation from leader and workers
 */
pub unsafe fn ExplainPrintJITSummary(es: *mut ExplainState, queryDesc: *mut QueryDesc) {
    let mut ji: JitInstrumentation = core::mem::zeroed();

    if !((*(*queryDesc).estate).es_jit_flags & PGJIT_PERFORM != 0) {
        return;
    }

    /*
     * Work with a copy instead of modifying the leader state, since this
     * function may be called twice
     */
    if !(*(*queryDesc).estate).es_jit.is_null() {
        InstrJitAgg(&mut ji, &mut (*(*(*queryDesc).estate).es_jit).instr as *mut _ as *mut JitInstrumentation);
    }

    /* If this process has done JIT in parallel workers, merge stats */
    if !(*(*queryDesc).estate).es_jit_worker_instr.is_null() {
        InstrJitAgg(&mut ji, (*(*queryDesc).estate).es_jit_worker_instr as *mut _ as *mut JitInstrumentation);
    }

    ExplainPrintJIT(es, (*(*queryDesc).estate).es_jit_flags, &mut ji);
}

/*
 * ExplainPrintJIT -
 *   Append information about JITing to es->str.
 */
unsafe fn ExplainPrintJIT(
    es: *mut ExplainState,
    jit_flags: c_int,
    ji: *mut JitInstrumentation,
) {
    let mut total_time: instr_time = core::mem::zeroed();

    /* don't print information if no JITing happened */
    if ji.is_null() || (*ji).created_functions == 0 {
        return;
    }

    /* calculate total time */
    INSTR_TIME_SET_ZERO(&mut total_time);
    /* don't add deform_counter, it's included in generation_counter */
    INSTR_TIME_ADD(&mut total_time, (*ji).generation_counter);
    INSTR_TIME_ADD(&mut total_time, (*ji).inlining_counter);
    INSTR_TIME_ADD(&mut total_time, (*ji).optimization_counter);
    INSTR_TIME_ADD(&mut total_time, (*ji).emission_counter);

    ExplainOpenGroup(c"JIT".as_ptr(), c"JIT".as_ptr(), true, es);

    /* for higher density, open code the text output format */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        appendStringInfoString((*es).str, c"JIT:\n".as_ptr());
        (*es).indent += 1;

        ExplainPropertyInteger(c"Functions".as_ptr(), null(), (*ji).created_functions as int64, es);

        ExplainIndentText(es);
        appendStringInfo!(
            (*es).str,
            "Options: {} {}, {} {}, {} {}, {} {}\n",
            "Inlining", if jit_flags & PGJIT_INLINE != 0 { "true" } else { "false" },
            "Optimization", if jit_flags & PGJIT_OPT3 != 0 { "true" } else { "false" },
            "Expressions", if jit_flags & PGJIT_EXPR != 0 { "true" } else { "false" },
            "Deforming", if jit_flags & PGJIT_DEFORM != 0 { "true" } else { "false" }
        );

        if (*es).analyze && (*es).timing {
            ExplainIndentText(es);
            appendStringInfo!(
                (*es).str,
                "Timing: {} {:.3} ms ({} {:.3} ms), {} {:.3} ms, {} {:.3} ms, {} {:.3} ms, {} {:.3} ms\n",
                "Generation", 1000.0 * INSTR_TIME_GET_DOUBLE((*ji).generation_counter),
                "Deform", 1000.0 * INSTR_TIME_GET_DOUBLE((*ji).deform_counter),
                "Inlining", 1000.0 * INSTR_TIME_GET_DOUBLE((*ji).inlining_counter),
                "Optimization", 1000.0 * INSTR_TIME_GET_DOUBLE((*ji).optimization_counter),
                "Emission", 1000.0 * INSTR_TIME_GET_DOUBLE((*ji).emission_counter),
                "Total", 1000.0 * INSTR_TIME_GET_DOUBLE(total_time)
            );
        }

        (*es).indent -= 1;
    } else {
        ExplainPropertyInteger(c"Functions".as_ptr(), null(), (*ji).created_functions as int64, es);

        ExplainOpenGroup(c"Options".as_ptr(), c"Options".as_ptr(), true, es);
        ExplainPropertyBool(c"Inlining".as_ptr(), jit_flags & PGJIT_INLINE != 0, es);
        ExplainPropertyBool(c"Optimization".as_ptr(), jit_flags & PGJIT_OPT3 != 0, es);
        ExplainPropertyBool(c"Expressions".as_ptr(), jit_flags & PGJIT_EXPR != 0, es);
        ExplainPropertyBool(c"Deforming".as_ptr(), jit_flags & PGJIT_DEFORM != 0, es);
        ExplainCloseGroup(c"Options".as_ptr(), c"Options".as_ptr(), true, es);

        if (*es).analyze && (*es).timing {
            ExplainOpenGroup(c"Timing".as_ptr(), c"Timing".as_ptr(), true, es);

            ExplainOpenGroup(c"Generation".as_ptr(), c"Generation".as_ptr(), true, es);
            ExplainPropertyFloat(
                c"Deform".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*ji).deform_counter),
                3, es,
            );
            ExplainPropertyFloat(
                c"Total".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*ji).generation_counter),
                3, es,
            );
            ExplainCloseGroup(c"Generation".as_ptr(), c"Generation".as_ptr(), true, es);

            ExplainPropertyFloat(
                c"Inlining".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*ji).inlining_counter),
                3, es,
            );
            ExplainPropertyFloat(
                c"Optimization".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*ji).optimization_counter),
                3, es,
            );
            ExplainPropertyFloat(
                c"Emission".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*ji).emission_counter),
                3, es,
            );
            ExplainPropertyFloat(
                c"Total".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE(total_time),
                3, es,
            );

            ExplainCloseGroup(c"Timing".as_ptr(), c"Timing".as_ptr(), true, es);
        }
    }

    ExplainCloseGroup(c"JIT".as_ptr(), c"JIT".as_ptr(), true, es);
}

/*
 * ExplainPrintSerialize -
 *   Append information about query output volume to es->str.
 */
unsafe fn ExplainPrintSerialize(es: *mut ExplainState, metrics: *const SerializeMetrics) {
    let format: *const c_char;

    /* We shouldn't get called for EXPLAIN_SERIALIZE_NONE */
    if (*es).serialize == EXPLAIN_SERIALIZE_TEXT {
        format = c"text".as_ptr();
    } else {
        // Assert(es->serialize == EXPLAIN_SERIALIZE_BINARY);
        format = c"binary".as_ptr();
    }

    ExplainOpenGroup(c"Serialization".as_ptr(), c"Serialization".as_ptr(), true, es);

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        if (*es).timing {
            appendStringInfo!(
                (*es).str,
                "Serialization: time={:.3} ms  output={}kB  format={}\n",
                1000.0 * INSTR_TIME_GET_DOUBLE((*metrics).timeSpent),
                BYTES_TO_KILOBYTES!((*metrics).bytesSent),
                cstr_s(format)
            );
        } else {
            appendStringInfo!(
                (*es).str,
                "Serialization: output={}kB  format={}\n",
                BYTES_TO_KILOBYTES!((*metrics).bytesSent),
                cstr_s(format)
            );
        }

        if (*es).buffers && peek_buffer_usage(es, &(*metrics).bufferUsage) {
            (*es).indent += 1;
            show_buffer_usage(es, &(*metrics).bufferUsage);
            (*es).indent -= 1;
        }
    } else {
        if (*es).timing {
            ExplainPropertyFloat(
                c"Time".as_ptr(), c"ms".as_ptr(),
                1000.0 * INSTR_TIME_GET_DOUBLE((*metrics).timeSpent),
                3, es,
            );
        }
        ExplainPropertyUInteger(
            c"Output Volume".as_ptr(),
            c"kB".as_ptr(),
            BYTES_TO_KILOBYTES!((*metrics).bytesSent),
            es,
        );
        ExplainPropertyText(c"Format".as_ptr(), format, es);
        if (*es).buffers {
            show_buffer_usage(es, &(*metrics).bufferUsage);
        }
    }

    ExplainCloseGroup(c"Serialization".as_ptr(), c"Serialization".as_ptr(), true, es);
}

/*
 * ExplainQueryText -
 *   add a "Query Text" node that contains the actual text of the query
 */
pub unsafe fn ExplainQueryText(es: *mut ExplainState, queryDesc: *mut QueryDesc) {
    if !(*queryDesc).sourceText.is_null() {
        ExplainPropertyText(c"Query Text".as_ptr(), (*queryDesc).sourceText, es);
    }
}

/*
 * ExplainQueryParameters -
 *   add a "Query Parameters" node that describes the parameters of the query
 */
pub unsafe fn ExplainQueryParameters(
    es: *mut ExplainState,
    params: *mut ParamListInfo,
    maxlen: c_int,
) {
    let str: *mut c_char;

    /* This check is consistent with errdetail_params() */
    if params.is_null() || maxlen == 0 {
        return;
    }

    str = BuildParamLogString(params, null_mut(), maxlen);
    if !str.is_null() && *str != 0 {
        ExplainPropertyText(c"Query Parameters".as_ptr(), str, es);
    }
}

/*
 * report_triggers -
 *     report execution stats for a single relation's triggers
 */
unsafe fn report_triggers(rInfo: *mut ResultRelInfo, show_relname: bool, es: *mut ExplainState) {
    if (*rInfo).ri_TrigDesc.is_null() || (*rInfo).ri_TrigInstrument.is_null() {
        return;
    }
    let nt_total = trigdesc_numtriggers((*rInfo).ri_TrigDesc);
    for nt in 0..nt_total {
        let trig = trigdesc_trigger((*rInfo).ri_TrigDesc, nt);
        let instr: *mut Instrumentation = (*rInfo).ri_TrigInstrument.offset(nt as isize);
        let relname: *mut c_char;
        let mut conname: *mut c_char = null_mut();

        /* Must clean up instrumentation state */
        InstrEndLoop(instr);

        /*
         * We ignore triggers that were never invoked; they likely aren't
         * relevant to the current query type.
         */
        if (*instr).ntuples == 0.0 {
            continue;
        }

        ExplainOpenGroup(c"Trigger".as_ptr(), null(), true, es);

        relname = RelationGetRelationName((*rInfo).ri_RelationDesc);
        if OidIsValid((*trig).tgconstraint) {
            conname = get_constraint_name((*trig).tgconstraint);
        }

        /*
         * In text format, we avoid printing both the trigger name and the
         * constraint name unless VERBOSE is specified.  In non-text formats
         * we just print everything.
         */
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            if (*es).verbose || conname.is_null() {
                appendStringInfo!((*es).str, "Trigger {}", cstr_s((*trig).tgname));
            } else {
                appendStringInfoString((*es).str, c"Trigger".as_ptr());
            }
            if !conname.is_null() {
                appendStringInfo!((*es).str, " for constraint {}", cstr_s(conname));
            }
            if show_relname {
                appendStringInfo!((*es).str, " on {}", cstr_s(relname));
            }
            if (*es).timing {
                appendStringInfo!(
                    (*es).str,
                    ": time={:.3} calls={:.0}\n",
                    1000.0 * (*instr).total,
                    (*instr).ntuples
                );
            } else {
                appendStringInfo!((*es).str, ": calls={:.0}\n", (*instr).ntuples);
            }
        } else {
            ExplainPropertyText(c"Trigger Name".as_ptr(), (*trig).tgname, es);
            if !conname.is_null() {
                ExplainPropertyText(c"Constraint Name".as_ptr(), conname, es);
            }
            ExplainPropertyText(c"Relation".as_ptr(), relname, es);
            if (*es).timing {
                ExplainPropertyFloat(
                    c"Time".as_ptr(), c"ms".as_ptr(),
                    1000.0 * (*instr).total, 3, es,
                );
            }
            ExplainPropertyFloat(c"Calls".as_ptr(), null(), (*instr).ntuples, 0, es);
        }

        if !conname.is_null() {
            pfree(conname as *mut c_void);
        }

        ExplainCloseGroup(c"Trigger".as_ptr(), null(), true, es);
    }
}

/* Compute elapsed time in seconds since given timestamp */
unsafe fn elapsed_time(starttime: *mut instr_time) -> f64 {
    let mut endtime: instr_time = core::mem::zeroed();

    INSTR_TIME_SET_CURRENT(&mut endtime);
    INSTR_TIME_SUBTRACT(&mut endtime, *starttime);
    INSTR_TIME_GET_DOUBLE(endtime)
}

// ---------------------------------------------------------------------------
// ExplainPreScanNode / plan_is_disabled
// ---------------------------------------------------------------------------

/*
 * ExplainPreScanNode -
 *   Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 */
unsafe fn ExplainPreScanNode(
    planstate: *mut PlanState,
    rels_used: *mut *mut Bitmapset,
) -> bool {
    let plan = (*planstate).plan;

    match nodeTag(plan) {
        NodeTag::T_SeqScan
        | NodeTag::T_SampleScan
        | NodeTag::T_IndexScan
        | NodeTag::T_IndexOnlyScan
        | NodeTag::T_BitmapHeapScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_SubqueryScan
        | NodeTag::T_FunctionScan
        | NodeTag::T_TableFuncScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_NamedTuplestoreScan
        | NodeTag::T_WorkTableScan => {
            *rels_used =
                bms_add_member(*rels_used, (*(plan as *mut Scan)).scanrelid as c_int);
        }
        NodeTag::T_ForeignScan => {
            *rels_used = bms_add_members(
                *rels_used,
                (*(plan as *mut ForeignScan)).fs_base_relids,
            );
        }
        NodeTag::T_CustomScan => {
            *rels_used = bms_add_members(
                *rels_used,
                (*(plan as *mut CustomScan)).custom_relids,
            );
        }
        NodeTag::T_ModifyTable => {
            *rels_used = bms_add_member(
                *rels_used,
                (*(plan as *mut ModifyTable)).nominalRelation as c_int,
            );
            if (*(plan as *mut ModifyTable)).exclRelRTI != 0 {
                *rels_used = bms_add_member(
                    *rels_used,
                    (*(plan as *mut ModifyTable)).exclRelRTI as c_int,
                );
            }
            /* Ensure Vars used in RETURNING will have refnames */
            if !(*plan).targetlist.is_null() {
                *rels_used = bms_add_member(
                    *rels_used,
                    linitial_int((*(plan as *mut ModifyTable)).resultRelations),
                );
            }
        }
        NodeTag::T_Append => {
            *rels_used = bms_add_members(
                *rels_used,
                (*(plan as *mut Append)).apprelids,
            );
        }
        NodeTag::T_MergeAppend => {
            *rels_used = bms_add_members(
                *rels_used,
                (*(plan as *mut MergeAppend)).apprelids,
            );
        }
        _ => {}
    }

    planstate_tree_walker(planstate, ExplainPreScanNode, rels_used)
}

/*
 * plan_is_disabled
 *     Checks if the given plan node type was disabled during query planning.
 *     This is evident by the disabled_nodes field being higher than the sum of
 *     the disabled_nodes field from the plan's children.
 */

// Helper macros for Plan children
macro_rules! outerPlan {
    ($node:expr) => {
        (*($node as *mut Plan)).lefttree
    };
}
macro_rules! innerPlan {
    ($node:expr) => {
        (*($node as *mut Plan)).righttree
    };
}

unsafe fn plan_is_disabled(plan: *mut Plan) -> bool {
    let child_disabled_nodes: c_int;

    /* The node is certainly not disabled if this is zero */
    if (*plan).disabled_nodes == 0 {
        return false;
    }

    let mut cdn: c_int = 0;

    /*
     * Handle special nodes first.  Children of BitmapOrs and BitmapAnds can't
     * be disabled, so no need to handle those specifically.
     */
    if IsA_plan(plan, NodeTag::T_Append) {
        let aplan = plan as *mut Append;

        /*
         * Sum the Append childrens' disabled_nodes.  This purposefully
         * includes any run-time pruned children.  Ignoring those could give
         * us the incorrect number of disabled nodes.
         */
        foreach!(lc, (*aplan).appendplans, {
            let subplan = lfirst(current_cell!(lc)) as *mut Plan;
            cdn += (*subplan).disabled_nodes;
        });
    } else if IsA_plan(plan, NodeTag::T_MergeAppend) {
        let maplan = plan as *mut MergeAppend;

        foreach!(lc, (*maplan).mergeplans, {
            let subplan = lfirst(current_cell!(lc)) as *mut Plan;
            cdn += (*subplan).disabled_nodes;
        });
    } else if IsA_plan(plan, NodeTag::T_SubqueryScan) {
        cdn += (*(*(plan as *mut SubqueryScan)).subplan).disabled_nodes;
    } else if IsA_plan(plan, NodeTag::T_CustomScan) {
        let cplan = plan as *mut CustomScan;

        foreach!(lc, (*cplan).custom_plans, {
            let subplan = lfirst(current_cell!(lc)) as *mut Plan;
            cdn += (*subplan).disabled_nodes;
        });
    } else {
        /*
         * Else, sum up disabled_nodes from the plan's inner and outer side.
         */
        let outer = outerPlan!(plan);
        let inner = innerPlan!(plan);
        if !outer.is_null() {
            cdn += (*outer).disabled_nodes;
        }
        if !inner.is_null() {
            cdn += (*inner).disabled_nodes;
        }
    }

    /*
     * It's disabled if the plan's disabled_nodes is higher than the sum of
     * its child's plan disabled_nodes.
     */
    (*plan).disabled_nodes > cdn
}

// ---------------------------------------------------------------------------
// ExplainNode - the main plan-tree printer
// ---------------------------------------------------------------------------

/*
 * ExplainNode -
 *   Appends a description of a plan tree to es->str
 */
unsafe fn ExplainNode(
    planstate: *mut PlanState,
    mut ancestors: *mut List,
    relationship: *const c_char,
    plan_name: *const c_char,
    es: *mut ExplainState,
) {
    let plan = (*planstate).plan;
    let pname: *const c_char;          /* node type name for text output */
    let sname: *const c_char;          /* node type name for non-text output */
    let mut strategy: *const c_char = null();
    let mut partialmode: *const c_char = null();
    let mut operation: *const c_char = null();
    let mut custom_name: *const c_char = null();
    let save_workers_state = (*es).workers_state;
    let save_indent = (*es).indent;
    let haschildren: bool;
    let isdisabled: bool;

    /*
     * Prepare per-worker output buffers, if needed.  We'll append the data in
     * these to the main output string further down.
     */
    if !(*planstate).worker_instrument.is_null()
        && (*es).analyze
        && !(*es).hide_workers
    {
        (*es).workers_state =
            ExplainCreateWorkersState((*(*planstate).worker_instrument).num_workers);
    } else {
        (*es).workers_state = null_mut();
    }

    /* Identify plan node type, and print generic details */
    let pname_buf: *mut c_char; /* heap-alloc'd only for some cases */
    let pname_sname: (*const c_char, *const c_char);
    pname_sname = match nodeTag(plan) {
        NodeTag::T_Result => (c"Result".as_ptr(), c"Result".as_ptr()),
        NodeTag::T_ProjectSet => (c"ProjectSet".as_ptr(), c"ProjectSet".as_ptr()),
        NodeTag::T_ModifyTable => {
            let sn = c"ModifyTable".as_ptr();
            let op_tag = (*(plan as *mut ModifyTable)).operation;
            let (pn, op) = if op_tag == CMD_INSERT {
                (c"Insert".as_ptr(), c"Insert".as_ptr())
            } else if op_tag == CMD_UPDATE {
                (c"Update".as_ptr(), c"Update".as_ptr())
            } else if op_tag == CMD_DELETE {
                (c"Delete".as_ptr(), c"Delete".as_ptr())
            } else if op_tag == CMD_MERGE {
                (c"Merge".as_ptr(), c"Merge".as_ptr())
            } else {
                (c"???".as_ptr(), null())
            };
            operation = op;
            (pn, sn)
        }
        NodeTag::T_Append      => (c"Append".as_ptr(), c"Append".as_ptr()),
        NodeTag::T_MergeAppend => (c"Merge Append".as_ptr(), c"Merge Append".as_ptr()),
        NodeTag::T_RecursiveUnion => (c"Recursive Union".as_ptr(), c"Recursive Union".as_ptr()),
        NodeTag::T_BitmapAnd   => (c"BitmapAnd".as_ptr(), c"BitmapAnd".as_ptr()),
        NodeTag::T_BitmapOr    => (c"BitmapOr".as_ptr(), c"BitmapOr".as_ptr()),
        NodeTag::T_NestLoop    => (c"Nested Loop".as_ptr(), c"Nested Loop".as_ptr()),
        NodeTag::T_MergeJoin   => (c"Merge".as_ptr(), c"Merge Join".as_ptr()),
        NodeTag::T_HashJoin    => (c"Hash".as_ptr(), c"Hash Join".as_ptr()),
        NodeTag::T_SeqScan     => (c"Seq Scan".as_ptr(), c"Seq Scan".as_ptr()),
        NodeTag::T_SampleScan  => (c"Sample Scan".as_ptr(), c"Sample Scan".as_ptr()),
        NodeTag::T_Gather      => (c"Gather".as_ptr(), c"Gather".as_ptr()),
        NodeTag::T_GatherMerge => (c"Gather Merge".as_ptr(), c"Gather Merge".as_ptr()),
        NodeTag::T_IndexScan   => (c"Index Scan".as_ptr(), c"Index Scan".as_ptr()),
        NodeTag::T_IndexOnlyScan => (c"Index Only Scan".as_ptr(), c"Index Only Scan".as_ptr()),
        NodeTag::T_BitmapIndexScan => (c"Bitmap Index Scan".as_ptr(), c"Bitmap Index Scan".as_ptr()),
        NodeTag::T_BitmapHeapScan  => (c"Bitmap Heap Scan".as_ptr(), c"Bitmap Heap Scan".as_ptr()),
        NodeTag::T_TidScan      => (c"Tid Scan".as_ptr(), c"Tid Scan".as_ptr()),
        NodeTag::T_TidRangeScan => (c"Tid Range Scan".as_ptr(), c"Tid Range Scan".as_ptr()),
        NodeTag::T_SubqueryScan => (c"Subquery Scan".as_ptr(), c"Subquery Scan".as_ptr()),
        NodeTag::T_FunctionScan => (c"Function Scan".as_ptr(), c"Function Scan".as_ptr()),
        NodeTag::T_TableFuncScan => (c"Table Function Scan".as_ptr(), c"Table Function Scan".as_ptr()),
        NodeTag::T_ValuesScan   => (c"Values Scan".as_ptr(), c"Values Scan".as_ptr()),
        NodeTag::T_CteScan      => (c"CTE Scan".as_ptr(), c"CTE Scan".as_ptr()),
        NodeTag::T_NamedTuplestoreScan => (c"Named Tuplestore Scan".as_ptr(), c"Named Tuplestore Scan".as_ptr()),
        NodeTag::T_WorkTableScan => (c"WorkTable Scan".as_ptr(), c"WorkTable Scan".as_ptr()),
        NodeTag::T_ForeignScan => {
            let sn = c"Foreign Scan".as_ptr();
            let op_tag = (*(plan as *mut ForeignScan)).operation;
            let (pn, op): (*const c_char, *const c_char) = if op_tag == CMD_SELECT {
                (c"Foreign Scan".as_ptr(), c"Select".as_ptr())
            } else if op_tag == CMD_INSERT {
                (c"Foreign Insert".as_ptr(), c"Insert".as_ptr())
            } else if op_tag == CMD_UPDATE {
                (c"Foreign Update".as_ptr(), c"Update".as_ptr())
            } else if op_tag == CMD_DELETE {
                (c"Foreign Delete".as_ptr(), c"Delete".as_ptr())
            } else {
                (c"???".as_ptr(), null())
            };
            operation = op;
            (pn, sn)
        }
        NodeTag::T_CustomScan => {
            let sn = c"Custom Scan".as_ptr();
            custom_name = (*(*(plan as *mut CustomScan)).methods).CustomName;
            let pn: *const c_char = if !custom_name.is_null() {
                psprintf(c"Custom Scan (%s)".as_ptr(), custom_name)
            } else {
                sn
            };
            (pn, sn)
        }
        NodeTag::T_Material    => (c"Materialize".as_ptr(), c"Materialize".as_ptr()),
        NodeTag::T_Memoize     => (c"Memoize".as_ptr(), c"Memoize".as_ptr()),
        NodeTag::T_Sort        => (c"Sort".as_ptr(), c"Sort".as_ptr()),
        NodeTag::T_IncrementalSort => (c"Incremental Sort".as_ptr(), c"Incremental Sort".as_ptr()),
        NodeTag::T_Group       => (c"Group".as_ptr(), c"Group".as_ptr()),
        NodeTag::T_Agg => {
            let agg = plan as *mut Agg;
            let sn = c"Aggregate".as_ptr();
            strategy = match (*agg).aggstrategy {
                AGG_PLAIN  => c"Plain".as_ptr(),
                AGG_SORTED => c"Sorted".as_ptr(),
                AGG_HASHED => c"Hashed".as_ptr(),
                AGG_MIXED  => c"Mixed".as_ptr(),
            };
            let base_pname: *const c_char = match (*agg).aggstrategy {
                AGG_PLAIN  => c"Aggregate".as_ptr(),
                AGG_SORTED => c"GroupAggregate".as_ptr(),
                AGG_HASHED => c"HashAggregate".as_ptr(),
                AGG_MIXED  => c"MixedAggregate".as_ptr(),
            };
            if DO_AGGSPLIT_SKIPFINAL((*agg).aggsplit) {
                partialmode = c"Partial".as_ptr();
                let pn = psprintf(c"%s %s".as_ptr(), partialmode, base_pname);
                (pn, sn)
            } else if DO_AGGSPLIT_COMBINE((*agg).aggsplit) {
                partialmode = c"Finalize".as_ptr();
                let pn = psprintf(c"%s %s".as_ptr(), partialmode, base_pname);
                (pn, sn)
            } else {
                partialmode = c"Simple".as_ptr();
                (base_pname, sn)
            }
        }
        NodeTag::T_WindowAgg  => (c"WindowAgg".as_ptr(), c"WindowAgg".as_ptr()),
        NodeTag::T_Unique     => (c"Unique".as_ptr(), c"Unique".as_ptr()),
        NodeTag::T_SetOp => {
            let sn = c"SetOp".as_ptr();
            let st = (*(plan as *mut SetOp)).strategy;
            strategy = if st == SETOP_SORTED { c"Sorted".as_ptr() }
                       else if st == SETOP_HASHED { c"Hashed".as_ptr() }
                       else { c"???".as_ptr() };
            let pn: *const c_char = if st == SETOP_SORTED { c"SetOp".as_ptr() }
                       else if st == SETOP_HASHED { c"HashSetOp".as_ptr() }
                       else { c"SetOp ???".as_ptr() };
            (pn, sn)
        }
        NodeTag::T_LockRows   => (c"LockRows".as_ptr(), c"LockRows".as_ptr()),
        NodeTag::T_Limit      => (c"Limit".as_ptr(), c"Limit".as_ptr()),
        NodeTag::T_Hash       => (c"Hash".as_ptr(), c"Hash".as_ptr()),
        _                     => (c"???".as_ptr(), c"???".as_ptr()),
    };
    let pname = pname_sname.0;
    let sname = pname_sname.1;

    ExplainOpenGroup(
        c"Plan".as_ptr(),
        if relationship.is_null() { c"Plan".as_ptr() } else { null() },
        true,
        es,
    );

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        if !plan_name.is_null() {
            ExplainIndentText(es);
            appendStringInfo!((*es).str, "{}\n", cstr_s(plan_name));
            (*es).indent += 1;
        }
        if (*es).indent > 0 {
            ExplainIndentText(es);
            appendStringInfoString((*es).str, c"->  ".as_ptr());
            (*es).indent += 2;
        }
        if (*plan).parallel_aware {
            appendStringInfoString((*es).str, c"Parallel ".as_ptr());
        }
        if (*plan).async_capable {
            appendStringInfoString((*es).str, c"Async ".as_ptr());
        }
        appendStringInfoString((*es).str, pname);
        (*es).indent += 1;
    } else {
        ExplainPropertyText(c"Node Type".as_ptr(), sname, es);
        if !strategy.is_null() {
            ExplainPropertyText(c"Strategy".as_ptr(), strategy, es);
        }
        if !partialmode.is_null() {
            ExplainPropertyText(c"Partial Mode".as_ptr(), partialmode, es);
        }
        if !operation.is_null() {
            ExplainPropertyText(c"Operation".as_ptr(), operation, es);
        }
        if !relationship.is_null() {
            ExplainPropertyText(c"Parent Relationship".as_ptr(), relationship, es);
        }
        if !plan_name.is_null() {
            ExplainPropertyText(c"Subplan Name".as_ptr(), plan_name, es);
        }
        if !custom_name.is_null() {
            ExplainPropertyText(c"Custom Plan Provider".as_ptr(), custom_name, es);
        }
        ExplainPropertyBool(c"Parallel Aware".as_ptr(), (*plan).parallel_aware, es);
        ExplainPropertyBool(c"Async Capable".as_ptr(), (*plan).async_capable, es);
    }

    /* Second pass: node-type-specific target info */
    match nodeTag(plan) {
        NodeTag::T_SeqScan
        | NodeTag::T_SampleScan
        | NodeTag::T_BitmapHeapScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_SubqueryScan
        | NodeTag::T_FunctionScan
        | NodeTag::T_TableFuncScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_WorkTableScan => {
            ExplainScanTarget(plan as *mut Scan, es);
        }
        NodeTag::T_ForeignScan | NodeTag::T_CustomScan => {
            if (*(plan as *mut Scan)).scanrelid > 0 {
                ExplainScanTarget(plan as *mut Scan, es);
            }
        }
        NodeTag::T_IndexScan => {
            let indexscan = plan as *mut IndexScan;
            ExplainIndexScanDetails((*indexscan).indexid, (*indexscan).indexorderdir, es);
            ExplainScanTarget(indexscan as *mut Scan, es);
        }
        NodeTag::T_IndexOnlyScan => {
            let indexonlyscan = plan as *mut IndexOnlyScan;
            ExplainIndexScanDetails(
                (*indexonlyscan).indexid,
                (*indexonlyscan).indexorderdir,
                es,
            );
            ExplainScanTarget(indexonlyscan as *mut Scan, es);
        }
        NodeTag::T_BitmapIndexScan => {
            let bitmapindexscan = plan as *mut BitmapIndexScan;
            let indexname = explain_get_index_name((*bitmapindexscan).indexid);

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                appendStringInfo!((*es).str, " on {}", cstr_s(quote_identifier(indexname)));
            } else {
                ExplainPropertyText(c"Index Name".as_ptr(), indexname, es);
            }
        }
        NodeTag::T_ModifyTable => {
            ExplainModifyTarget(plan as *mut ModifyTable, es);
        }
        NodeTag::T_NestLoop | NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            let jointype_tag = (*(plan as *mut Join)).jointype;
            let jointype: *const c_char = if jointype_tag == JOIN_INNER {
                c"Inner".as_ptr()
            } else if jointype_tag == JOIN_LEFT {
                c"Left".as_ptr()
            } else if jointype_tag == JOIN_FULL {
                c"Full".as_ptr()
            } else if jointype_tag == JOIN_RIGHT {
                c"Right".as_ptr()
            } else if jointype_tag == JOIN_SEMI {
                c"Semi".as_ptr()
            } else if jointype_tag == JOIN_ANTI {
                c"Anti".as_ptr()
            } else if jointype_tag == JOIN_RIGHT_SEMI {
                c"Right Semi".as_ptr()
            } else if jointype_tag == JOIN_RIGHT_ANTI {
                c"Right Anti".as_ptr()
            } else {
                c"???".as_ptr()
            };

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                /*
                 * For historical reasons, the join type is interpolated
                 * into the node type name...
                 */
                if jointype_tag != JOIN_INNER {
                    appendStringInfo!((*es).str, " {} Join", cstr_s(jointype));
                } else if !IsA_plan(plan, NodeTag::T_NestLoop) {
                    appendStringInfoString((*es).str, c" Join".as_ptr());
                }
            } else {
                ExplainPropertyText(c"Join Type".as_ptr(), jointype, es);
            }
        }
        NodeTag::T_SetOp => {
            let setopcmd = (*(plan as *mut SetOp)).cmd;
            let setopcmd_str: *const c_char = if setopcmd == SETOPCMD_INTERSECT {
                c"Intersect".as_ptr()
            } else if setopcmd == SETOPCMD_INTERSECT_ALL {
                c"Intersect All".as_ptr()
            } else if setopcmd == SETOPCMD_EXCEPT {
                c"Except".as_ptr()
            } else if setopcmd == SETOPCMD_EXCEPT_ALL {
                c"Except All".as_ptr()
            } else {
                c"???".as_ptr()
            };
            if (*es).format == EXPLAIN_FORMAT_TEXT {
                appendStringInfo!((*es).str, " {}", cstr_s(setopcmd_str));
            } else {
                ExplainPropertyText(c"Command".as_ptr(), setopcmd_str, es);
            }
        }
        _ => {}
    }

    /* cost/rows/width */
    if (*es).costs {
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfo!(
                (*es).str,
                "  (cost={:.2}..{:.2} rows={:.0} width={})",
                (*plan).startup_cost,
                (*plan).total_cost,
                (*plan).plan_rows,
                (*plan).plan_width
            );
        } else {
            ExplainPropertyFloat(c"Startup Cost".as_ptr(), null(), (*plan).startup_cost, 2, es);
            ExplainPropertyFloat(c"Total Cost".as_ptr(), null(), (*plan).total_cost, 2, es);
            ExplainPropertyFloat(c"Plan Rows".as_ptr(), null(), (*plan).plan_rows, 0, es);
            ExplainPropertyInteger(c"Plan Width".as_ptr(), null(), (*plan).plan_width as int64, es);
        }
    }

    /*
     * We have to forcibly clean up the instrumentation state because we
     * haven't done ExecutorEnd yet.  This is pretty grotty ...
     */
    if !(*planstate).instrument.is_null() {
        InstrEndLoop((*planstate).instrument);
    }

    if (*es).analyze
        && !(*planstate).instrument.is_null()
        && (*(*planstate).instrument).nloops > 0.0
    {
        let nloops = (*(*planstate).instrument).nloops;
        let startup_ms = 1000.0 * (*(*planstate).instrument).startup / nloops;
        let total_ms = 1000.0 * (*(*planstate).instrument).total / nloops;
        let rows = (*(*planstate).instrument).ntuples / nloops;

        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoString((*es).str, c" (actual ".as_ptr());

            if (*es).timing {
                appendStringInfo!((*es).str, "time={:.3}..{:.3} ", startup_ms, total_ms);
            }

            appendStringInfo!((*es).str, "rows={:.2} loops={:.0})", rows, nloops);
        } else {
            if (*es).timing {
                ExplainPropertyFloat(c"Actual Startup Time".as_ptr(), c"ms".as_ptr(), startup_ms, 3, es);
                ExplainPropertyFloat(c"Actual Total Time".as_ptr(), c"ms".as_ptr(), total_ms, 3, es);
            }
            ExplainPropertyFloat(c"Actual Rows".as_ptr(), null(), rows, 2, es);
            ExplainPropertyFloat(c"Actual Loops".as_ptr(), null(), nloops, 0, es);
        }
    } else if (*es).analyze {
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoString((*es).str, c" (never executed)".as_ptr());
        } else {
            if (*es).timing {
                ExplainPropertyFloat(c"Actual Startup Time".as_ptr(), c"ms".as_ptr(), 0.0, 3, es);
                ExplainPropertyFloat(c"Actual Total Time".as_ptr(), c"ms".as_ptr(), 0.0, 3, es);
            }
            ExplainPropertyFloat(c"Actual Rows".as_ptr(), null(), 0.0, 0, es);
            ExplainPropertyFloat(c"Actual Loops".as_ptr(), null(), 0.0, 0, es);
        }
    }

    /* in text format, first line ends here */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        appendStringInfoChar((*es).str, b'\n' as c_char);
    }

    isdisabled = plan_is_disabled(plan);
    if (*es).format != EXPLAIN_FORMAT_TEXT || isdisabled {
        ExplainPropertyBool(c"Disabled".as_ptr(), isdisabled, es);
    }

    /* prepare per-worker general execution details */
    if !(*es).workers_state.is_null() && (*es).verbose {
        let w = (*planstate).worker_instrument;

        for n in 0..(*w).num_workers {
            let instrument = (*w).instrument.as_mut_ptr().add(n as usize) as *mut Instrumentation;
            let nloops = (*instrument).nloops;
            let startup_ms: f64;
            let total_ms: f64;
            let rows: f64;

            if nloops <= 0.0 {
                continue;
            }
            startup_ms = 1000.0 * (*instrument).startup / nloops;
            total_ms = 1000.0 * (*instrument).total / nloops;
            rows = (*instrument).ntuples / nloops;

            ExplainOpenWorker(n, es);

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfoString((*es).str, c"actual ".as_ptr());
                if (*es).timing {
                    appendStringInfo!((*es).str, "time={:.3}..{:.3} ", startup_ms, total_ms);
                }
                appendStringInfo!((*es).str, "rows={:.2} loops={:.0}\n", rows, nloops);
            } else {
                if (*es).timing {
                    ExplainPropertyFloat(c"Actual Startup Time".as_ptr(), c"ms".as_ptr(), startup_ms, 3, es);
                    ExplainPropertyFloat(c"Actual Total Time".as_ptr(), c"ms".as_ptr(), total_ms, 3, es);
                }
                ExplainPropertyFloat(c"Actual Rows".as_ptr(), null(), rows, 2, es);
                ExplainPropertyFloat(c"Actual Loops".as_ptr(), null(), nloops, 0, es);
            }

            ExplainCloseWorker(n, es);
        }
    }

    /* target list */
    if (*es).verbose {
        show_plan_tlist(planstate, ancestors, es);
    }

    /* unique join */
    match nodeTag(plan) {
        NodeTag::T_NestLoop | NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            /* try not to be too chatty about this in text mode */
            if (*es).format != EXPLAIN_FORMAT_TEXT
                || ((*es).verbose && (*(plan as *mut Join)).inner_unique)
            {
                ExplainPropertyBool(
                    c"Inner Unique".as_ptr(),
                    (*(plan as *mut Join)).inner_unique,
                    es,
                );
            }
        }
        _ => {}
    }

    /* quals, sort keys, etc */
    match nodeTag(plan) {
        NodeTag::T_IndexScan => {
            show_scan_qual(
                (*(plan as *mut IndexScan)).indexqualorig,
                c"Index Cond".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut IndexScan)).indexqualorig.is_null() {
                show_instrumentation_count(c"Rows Removed by Index Recheck".as_ptr(), 2, planstate, es);
            }
            show_scan_qual(
                (*(plan as *mut IndexScan)).indexorderbyorig,
                c"Order By".as_ptr(), planstate, ancestors, es,
            );
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            show_indexsearches_info(planstate, es);
        }
        NodeTag::T_IndexOnlyScan => {
            show_scan_qual(
                (*(plan as *mut IndexOnlyScan)).indexqual,
                c"Index Cond".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut IndexOnlyScan)).recheckqual.is_null() {
                show_instrumentation_count(c"Rows Removed by Index Recheck".as_ptr(), 2, planstate, es);
            }
            show_scan_qual(
                (*(plan as *mut IndexOnlyScan)).indexorderby,
                c"Order By".as_ptr(), planstate, ancestors, es,
            );
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            if (*es).analyze && !(*planstate).instrument.is_null() {
                ExplainPropertyFloat(
                    c"Heap Fetches".as_ptr(), null(),
                    (*(*planstate).instrument).ntuples2, 0, es,
                );
            }
            show_indexsearches_info(planstate, es);
        }
        NodeTag::T_BitmapIndexScan => {
            show_scan_qual(
                (*(plan as *mut BitmapIndexScan)).indexqualorig,
                c"Index Cond".as_ptr(), planstate, ancestors, es,
            );
            show_indexsearches_info(planstate, es);
        }
        NodeTag::T_BitmapHeapScan => {
            show_scan_qual(
                (*(plan as *mut BitmapHeapScan)).bitmapqualorig,
                c"Recheck Cond".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut BitmapHeapScan)).bitmapqualorig.is_null() {
                show_instrumentation_count(c"Rows Removed by Index Recheck".as_ptr(), 2, planstate, es);
            }
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            show_tidbitmap_info(planstate as *mut BitmapHeapScanState, es);
        }
        NodeTag::T_SampleScan => {
            show_tablesample(
                (*(plan as *mut crate::nodes::plannodes::SampleScan)).tablesample,
                planstate, ancestors, es,
            );
            /* fall through to SeqScan filter */
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_SeqScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_NamedTuplestoreScan
        | NodeTag::T_WorkTableScan
        | NodeTag::T_SubqueryScan => {
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            if IsA_plan(plan, NodeTag::T_CteScan) {
                show_ctescan_info(planstate as *mut CteScanState, es);
            }
        }
        NodeTag::T_Gather => {
            let gather = plan as *mut Gather;
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            ExplainPropertyInteger(c"Workers Planned".as_ptr(), null(), (*gather).num_workers as int64, es);

            if (*es).analyze {
                let nworkers = (*(planstate as *mut GatherState)).nworkers_launched;
                ExplainPropertyInteger(c"Workers Launched".as_ptr(), null(), nworkers as int64, es);
            }

            if (*gather).single_copy || (*es).format != EXPLAIN_FORMAT_TEXT {
                ExplainPropertyBool(c"Single Copy".as_ptr(), (*gather).single_copy, es);
            }
        }
        NodeTag::T_GatherMerge => {
            let gm = plan as *mut GatherMerge;
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            ExplainPropertyInteger(c"Workers Planned".as_ptr(), null(), (*gm).num_workers as int64, es);

            if (*es).analyze {
                let nworkers = (*(planstate as *mut GatherMergeState)).nworkers_launched;
                ExplainPropertyInteger(c"Workers Launched".as_ptr(), null(), nworkers as int64, es);
            }
        }
        NodeTag::T_FunctionScan => {
            if (*es).verbose {
                let mut fexprs: *mut List = null_mut();

                foreach!(lc, (*(plan as *mut FunctionScan)).functions, {
                    let rtfunc = lfirst(current_cell!(lc)) as *mut RangeTblFunction;
                    // TODO(pg-port): rtfunc->funcexpr -- RangeTblFunction not yet accessible
                    fexprs = lappend(fexprs, null_mut());
                });
                /* We rely on show_expression to insert commas as needed */
                show_expression(
                    fexprs as *mut Node,
                    c"Function Call".as_ptr(), planstate, ancestors,
                    (*es).verbose, es,
                );
            }
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_TableFuncScan => {
            if (*es).verbose {
                let tablefunc = (*(plan as *mut TableFuncScan)).tablefunc;
                show_expression(
                    tablefunc as *mut Node,
                    c"Table Function Call".as_ptr(), planstate, ancestors,
                    (*es).verbose, es,
                );
            }
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            show_table_func_scan_info(planstate as *mut TableFuncScanState, es);
        }
        NodeTag::T_TidScan => {
            /*
             * The tidquals list has OR semantics, so be sure to show it
             * as an OR condition.
             */
            let mut tidquals = (*(plan as *mut TidScan)).tidquals;
            if list_length(tidquals) > 1 {
                tidquals = list_make1(make_orclause(tidquals) as *mut c_void);
            }
            show_scan_qual(tidquals, c"TID Cond".as_ptr(), planstate, ancestors, es);
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_TidRangeScan => {
            /*
             * The tidrangequals list has AND semantics, so be sure to
             * show it as an AND condition.
             */
            let mut tidquals = (*(plan as *mut TidRangeScan)).tidrangequals;
            if list_length(tidquals) > 1 {
                tidquals = list_make1(make_andclause(tidquals) as *mut c_void);
            }
            show_scan_qual(tidquals, c"TID Cond".as_ptr(), planstate, ancestors, es);
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_ForeignScan => {
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            show_foreignscan_info(planstate as *mut ForeignScanState, es);
        }
        NodeTag::T_CustomScan => {
            let css = planstate as *mut CustomScanState;
            show_scan_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            if let Some(f) = (*(*css).methods).ExplainCustomScan {
                f(css, ancestors, es as *mut crate::nodes::extensible::ExplainState);
            }
        }
        NodeTag::T_NestLoop => {
            show_upper_qual(
                (*(plan as *mut NestLoop)).join.joinqual,
                c"Join Filter".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut NestLoop)).join.joinqual.is_null() {
                show_instrumentation_count(c"Rows Removed by Join Filter".as_ptr(), 1, planstate, es);
            }
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 2, planstate, es);
            }
        }
        NodeTag::T_MergeJoin => {
            show_upper_qual(
                (*(plan as *mut MergeJoin)).mergeclauses,
                c"Merge Cond".as_ptr(), planstate, ancestors, es,
            );
            show_upper_qual(
                (*(plan as *mut MergeJoin)).join.joinqual,
                c"Join Filter".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut MergeJoin)).join.joinqual.is_null() {
                show_instrumentation_count(c"Rows Removed by Join Filter".as_ptr(), 1, planstate, es);
            }
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 2, planstate, es);
            }
        }
        NodeTag::T_HashJoin => {
            show_upper_qual(
                (*(plan as *mut crate::nodes::plannodes::HashJoin)).hashclauses,
                c"Hash Cond".as_ptr(), planstate, ancestors, es,
            );
            show_upper_qual(
                (*(plan as *mut crate::nodes::plannodes::HashJoin)).join.joinqual,
                c"Join Filter".as_ptr(), planstate, ancestors, es,
            );
            if !(*(plan as *mut crate::nodes::plannodes::HashJoin)).join.joinqual.is_null() {
                show_instrumentation_count(c"Rows Removed by Join Filter".as_ptr(), 1, planstate, es);
            }
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 2, planstate, es);
            }
        }
        NodeTag::T_Agg => {
            show_agg_keys(planstate as *mut AggState, ancestors, es);
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            show_hashagg_info(planstate as *mut AggState, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_WindowAgg => {
            show_window_def(planstate as *mut WindowAggState, ancestors, es);
            show_upper_qual(
                (*(plan as *mut WindowAgg)).runConditionOrig,
                c"Run Condition".as_ptr(), planstate, ancestors, es,
            );
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
            show_windowagg_info(planstate as *mut WindowAggState, es);
        }
        NodeTag::T_Group => {
            show_group_keys(planstate as *mut GroupState, ancestors, es);
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_Sort => {
            show_sort_keys(planstate as *mut SortState, ancestors, es);
            show_sort_info(planstate as *mut SortState, es);
        }
        NodeTag::T_IncrementalSort => {
            show_incremental_sort_keys(planstate as *mut IncrementalSortState, ancestors, es);
            show_incremental_sort_info(planstate as *mut IncrementalSortState, es);
        }
        NodeTag::T_MergeAppend => {
            show_merge_append_keys(planstate as *mut MergeAppendState, ancestors, es);
        }
        NodeTag::T_Result => {
            show_upper_qual(
                (*(plan as *mut PgResult)).resconstantqual as *mut List,
                c"One-Time Filter".as_ptr(), planstate, ancestors, es,
            );
            show_upper_qual((*plan).qual, c"Filter".as_ptr(), planstate, ancestors, es);
            if !(*plan).qual.is_null() {
                show_instrumentation_count(c"Rows Removed by Filter".as_ptr(), 1, planstate, es);
            }
        }
        NodeTag::T_ModifyTable => {
            show_modifytable_info(planstate as *mut ModifyTableState, ancestors, es);
        }
        NodeTag::T_Hash => {
            show_hash_info(planstate as *mut HashState, es);
        }
        NodeTag::T_Material => {
            show_material_info(planstate as *mut MaterialState, es);
        }
        NodeTag::T_Memoize => {
            show_memoize_info(planstate as *mut MemoizeState, ancestors, es);
        }
        NodeTag::T_RecursiveUnion => {
            show_recursive_union_info(planstate as *mut RecursiveUnionState, es);
        }
        _ => {}
    }

    /*
     * Prepare per-worker JIT instrumentation.  As with the overall JIT
     * summary, this is printed only if printing costs is enabled.
     */
    if !(*es).workers_state.is_null() && (*es).costs && (*es).verbose {
        let w = (*planstate).worker_jit_instrument;

        if !w.is_null() {
            for n in 0..(*w).num_workers {
                ExplainOpenWorker(n, es);
                ExplainPrintJIT(
                    es,
                    (*(*planstate).state).es_jit_flags,
                    (*w).jit_instr.as_mut_ptr().add(n as usize) as *mut JitInstrumentation,
                );
                ExplainCloseWorker(n, es);
            }
        }
    }

    /* Show buffer/WAL usage */
    if (*es).buffers && !(*planstate).instrument.is_null() {
        show_buffer_usage(es, &(*(*planstate).instrument).bufusage);
    }
    if (*es).wal && !(*planstate).instrument.is_null() {
        show_wal_usage(es, &(*(*planstate).instrument).walusage);
    }

    /* Prepare per-worker buffer/WAL usage */
    if !(*es).workers_state.is_null() && ((*es).buffers || (*es).wal) && (*es).verbose {
        let w = (*planstate).worker_instrument;

        for n in 0..(*w).num_workers {
            let instrument = (*w).instrument.as_mut_ptr().add(n as usize) as *mut Instrumentation;
            let nloops = (*instrument).nloops;

            if nloops <= 0.0 {
                continue;
            }

            ExplainOpenWorker(n, es);
            if (*es).buffers {
                show_buffer_usage(es, &(*instrument).bufusage);
            }
            if (*es).wal {
                show_wal_usage(es, &(*instrument).walusage);
            }
            ExplainCloseWorker(n, es);
        }
    }

    /* Show per-worker details for this plan node, then pop that stack */
    if !(*es).workers_state.is_null() {
        ExplainFlushWorkersState(es);
    }
    (*es).workers_state = save_workers_state;

    /* Allow plugins to print additional information */
    if let Some(hook) = explain_per_node_hook {
        hook(planstate, ancestors, relationship, plan_name, es);
    }

    /*
     * If partition pruning was done during executor initialization, the
     * number of child plans we'll display below will be less than the number
     * of subplans that was specified in the plan.
     */
    match nodeTag(plan) {
        NodeTag::T_Append => {
            ExplainMissingMembers(
                (*(planstate as *mut AppendState)).as_nplans,
                list_length((*(plan as *mut Append)).appendplans),
                es,
            );
        }
        NodeTag::T_MergeAppend => {
            ExplainMissingMembers(
                (*(planstate as *mut MergeAppendState)).ms_nplans,
                list_length((*(plan as *mut MergeAppend)).mergeplans),
                es,
            );
        }
        _ => {}
    }

    /* Get ready to display the child plans */
    haschildren = !(*planstate).initPlan.is_null()
        || !outerPlanState(planstate).is_null()
        || !innerPlanState(planstate).is_null()
        || IsA_plan(plan, NodeTag::T_Append)
        || IsA_plan(plan, NodeTag::T_MergeAppend)
        || IsA_plan(plan, NodeTag::T_BitmapAnd)
        || IsA_plan(plan, NodeTag::T_BitmapOr)
        || IsA_plan(plan, NodeTag::T_SubqueryScan)
        || (IsA_ps(planstate, NodeTag::T_CustomScanState)
            && !(*(planstate as *mut CustomScanState)).custom_ps.is_null())
        || !(*planstate).subPlan.is_null();

    if haschildren {
        ExplainOpenGroup(c"Plans".as_ptr(), c"Plans".as_ptr(), false, es);
        /* Pass current Plan as head of ancestors list for children */
        ancestors = lcons(plan as *mut c_void, ancestors);
    }

    /* initPlan-s */
    if !(*planstate).initPlan.is_null() {
        ExplainSubPlans((*planstate).initPlan, ancestors, c"InitPlan".as_ptr(), es);
    }

    /* lefttree */
    if !outerPlanState(planstate).is_null() {
        ExplainNode(outerPlanState(planstate), ancestors, c"Outer".as_ptr(), null(), es);
    }

    /* righttree */
    if !innerPlanState(planstate).is_null() {
        ExplainNode(innerPlanState(planstate), ancestors, c"Inner".as_ptr(), null(), es);
    }

    /* special child plans */
    match nodeTag(plan) {
        NodeTag::T_Append => {
            ExplainMemberNodes(
                (*(planstate as *mut AppendState)).appendplans,
                (*(planstate as *mut AppendState)).as_nplans,
                ancestors, es,
            );
        }
        NodeTag::T_MergeAppend => {
            ExplainMemberNodes(
                (*(planstate as *mut MergeAppendState)).mergeplans,
                (*(planstate as *mut MergeAppendState)).ms_nplans,
                ancestors, es,
            );
        }
        NodeTag::T_BitmapAnd => {
            ExplainMemberNodes(
                (*(planstate as *mut BitmapAndState)).bitmapplans,
                (*(planstate as *mut BitmapAndState)).nplans,
                ancestors, es,
            );
        }
        NodeTag::T_BitmapOr => {
            ExplainMemberNodes(
                (*(planstate as *mut BitmapOrState)).bitmapplans,
                (*(planstate as *mut BitmapOrState)).nplans,
                ancestors, es,
            );
        }
        NodeTag::T_SubqueryScan => {
            ExplainNode(
                (*(planstate as *mut SubqueryScanState)).subplan,
                ancestors, c"Subquery".as_ptr(), null(), es,
            );
        }
        NodeTag::T_CustomScan => {
            ExplainCustomChildren(planstate as *mut CustomScanState, ancestors, es);
        }
        _ => {}
    }

    /* subPlan-s */
    if !(*planstate).subPlan.is_null() {
        ExplainSubPlans((*planstate).subPlan, ancestors, c"SubPlan".as_ptr(), es);
    }

    /* end of child plans */
    if haschildren {
        ancestors = list_delete_first(ancestors);
        ExplainCloseGroup(c"Plans".as_ptr(), c"Plans".as_ptr(), false, es);
    }

    /* in text format, undo whatever indentation we added */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        (*es).indent = save_indent;
    }

    ExplainCloseGroup(
        c"Plan".as_ptr(),
        if relationship.is_null() { c"Plan".as_ptr() } else { null() },
        true,
        es,
    );
}

// ---------------------------------------------------------------------------
// show_plan_tlist / show_expression / show_qual / show_scan_qual / show_upper_qual
// ---------------------------------------------------------------------------

/*
 * Show the targetlist of a plan node
 */
unsafe fn show_plan_tlist(planstate: *mut PlanState, ancestors: *mut List, es: *mut ExplainState) {
    let plan = (*planstate).plan;
    let context: *mut List;
    let mut result: *mut List = null_mut();
    let useprefix: bool;

    /* No work if empty tlist (this occurs eg in bitmap indexscans) */
    if (*plan).targetlist.is_null() {
        return;
    }
    /* The tlist of an Append isn't real helpful, so suppress it */
    if IsA_plan(plan, NodeTag::T_Append) { return; }
    /* Likewise for MergeAppend and RecursiveUnion */
    if IsA_plan(plan, NodeTag::T_MergeAppend) { return; }
    if IsA_plan(plan, NodeTag::T_RecursiveUnion) { return; }

    /*
     * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
     */
    if IsA_plan(plan, NodeTag::T_ForeignScan)
        && (*(plan as *mut ForeignScan)).operation != CMD_SELECT
    {
        return;
    }

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, plan, ancestors);
    useprefix = (*es).rtable_size > 1;

    /* Deparse each result column (we now include resjunk ones) */
    foreach!(lc, (*plan).targetlist, {
        let tle = lfirst(current_cell!(lc)) as *mut TargetEntry;
        // TODO(pg-port): deparse_expression over tle->expr
        result = lappend(result, null_mut());
    });

    /* Print results */
    ExplainPropertyList(c"Output".as_ptr(), result, es);
}

/*
 * Show a generic expression
 */
unsafe fn show_expression(
    node: *mut Node,
    qlabel: *const c_char,
    planstate: *mut PlanState,
    ancestors: *mut List,
    useprefix: bool,
    es: *mut ExplainState,
) {
    let context: *mut List;
    let exprstr: *mut c_char;

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, (*planstate).plan, ancestors);

    /* Deparse the expression */
    exprstr = deparse_expression(node, context, useprefix, false);

    /* And add to es->str */
    ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
unsafe fn show_qual(
    qual: *mut List,
    qlabel: *const c_char,
    planstate: *mut PlanState,
    ancestors: *mut List,
    useprefix: bool,
    es: *mut ExplainState,
) {
    let node: *mut Node;

    /* No work if empty qual */
    if qual.is_null() {
        return;
    }

    /* Convert AND list to explicit AND */
    node = make_ands_explicit(qual);

    /* And show it */
    show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
unsafe fn show_scan_qual(
    qual: *mut List,
    qlabel: *const c_char,
    planstate: *mut PlanState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let useprefix = IsA_plan((*planstate).plan, NodeTag::T_SubqueryScan) || (*es).verbose;
    show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
unsafe fn show_upper_qual(
    qual: *mut List,
    qlabel: *const c_char,
    planstate: *mut PlanState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let useprefix = (*es).rtable_size > 1 || (*es).verbose;
    show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

// ---------------------------------------------------------------------------
// Sort/group key display
// ---------------------------------------------------------------------------

/*
 * Show the sort keys for a Sort node.
 */
unsafe fn show_sort_keys(sortstate: *mut SortState, ancestors: *mut List, es: *mut ExplainState) {
    let plan = (*sortstate).ss.ps.plan as *mut Sort;

    show_sort_group_keys(
        sortstate as *mut PlanState,
        c"Sort Key".as_ptr(),
        (*plan).numCols,
        0,
        (*plan).sortColIdx,
        (*plan).sortOperators,
        (*plan).collations,
        (*plan).nullsFirst,
        ancestors,
        es,
    );
}

/*
 * Show the sort keys for an IncrementalSort node.
 */
unsafe fn show_incremental_sort_keys(
    incrsortstate: *mut IncrementalSortState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = (*incrsortstate).ss.ps.plan as *mut IncrementalSort;

    show_sort_group_keys(
        incrsortstate as *mut PlanState,
        c"Sort Key".as_ptr(),
        (*plan).sort.numCols,
        (*plan).nPresortedCols,
        (*plan).sort.sortColIdx,
        (*plan).sort.sortOperators,
        (*plan).sort.collations,
        (*plan).sort.nullsFirst,
        ancestors,
        es,
    );
}

/*
 * Likewise, for a MergeAppend node.
 */
unsafe fn show_merge_append_keys(
    mstate: *mut MergeAppendState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = (*mstate).ps.plan as *mut MergeAppend;

    show_sort_group_keys(
        mstate as *mut PlanState,
        c"Sort Key".as_ptr(),
        (*plan).numCols,
        0,
        (*plan).sortColIdx,
        (*plan).sortOperators,
        (*plan).collations,
        (*plan).nullsFirst,
        ancestors,
        es,
    );
}

/*
 * Show the grouping keys for an Agg node.
 */
unsafe fn show_agg_keys(astate: *mut AggState, mut ancestors: *mut List, es: *mut ExplainState) {
    let plan = (*astate).ss.ps.plan as *mut Agg;

    if (*plan).numCols > 0 || !(*plan).groupingSets.is_null() {
        /* The key columns refer to the tlist of the child plan */
        ancestors = lcons(plan as *mut c_void, ancestors);

        if !(*plan).groupingSets.is_null() {
            show_grouping_sets(outerPlanState(astate as *mut PlanState), plan, ancestors, es);
        } else {
            show_sort_group_keys(
                outerPlanState(astate as *mut PlanState),
                c"Group Key".as_ptr(),
                (*plan).numCols,
                0,
                (*plan).grpColIdx,
                null_mut(),
                null_mut(),
                null_mut(),
                ancestors,
                es,
            );
        }

        ancestors = list_delete_first(ancestors);
    }
}

unsafe fn show_grouping_sets(
    planstate: *mut PlanState,
    agg: *mut Agg,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let context: *mut List;
    let useprefix: bool;

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, (*planstate).plan, ancestors);
    useprefix = (*es).rtable_size > 1 || (*es).verbose;

    ExplainOpenGroup(c"Grouping Sets".as_ptr(), c"Grouping Sets".as_ptr(), false, es);

    show_grouping_set_keys(planstate, agg, null_mut(), context, useprefix, ancestors, es);

    foreach!(lc, (*agg).chain, {
        let aggnode = lfirst(current_cell!(lc)) as *mut Agg;
        let sortnode = (*aggnode).plan.lefttree as *mut Sort;
        // TODO(pg-port): lefttree pointer access -- may need Plan->lefttree field
        show_grouping_set_keys(planstate, aggnode, null_mut(), context, useprefix, ancestors, es);
    });

    ExplainCloseGroup(c"Grouping Sets".as_ptr(), c"Grouping Sets".as_ptr(), false, es);
}

unsafe fn show_grouping_set_keys(
    planstate: *mut PlanState,
    aggnode: *mut Agg,
    sortnode: *mut Sort,
    context: *mut List,
    useprefix: bool,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = (*planstate).plan;
    let mut exprstr: *mut c_char;
    let gsets = (*aggnode).groupingSets;
    let keycols = (*aggnode).grpColIdx;
    let keyname: *const c_char;
    let keysetname: *const c_char;

    if (*aggnode).aggstrategy == AGG_HASHED || (*aggnode).aggstrategy == AGG_MIXED {
        keyname    = c"Hash Key".as_ptr();
        keysetname = c"Hash Keys".as_ptr();
    } else {
        keyname    = c"Group Key".as_ptr();
        keysetname = c"Group Keys".as_ptr();
    }

    ExplainOpenGroup(c"Grouping Set".as_ptr(), null(), true, es);

    if !sortnode.is_null() {
        show_sort_group_keys(
            planstate,
            c"Sort Key".as_ptr(),
            (*sortnode).numCols,
            0,
            (*sortnode).sortColIdx,
            (*sortnode).sortOperators,
            (*sortnode).collations,
            (*sortnode).nullsFirst,
            ancestors,
            es,
        );
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            (*es).indent += 1;
        }
    }

    ExplainOpenGroup(keysetname, keysetname, false, es);

    foreach!(lc, gsets, {
        let mut result: *mut List = null_mut();
        let gset = lfirst(current_cell!(lc)) as *mut List;

        foreach!(lc2, gset, {
            let i = lfirst_int(current_cell!(lc2)) as usize;
            let keyresno = *keycols.add(i);
            let target = get_tle_by_resno((*plan).targetlist, keyresno);

            if target.is_null() {
                ereport!(ERROR, errmsg!("no tlist entry for key {}", keyresno));
            }
            /* Deparse the expression, showing any top-level cast */
            /* TODO(pg-port): tle->expr -- TargetEntry not yet accessible */
            exprstr = deparse_expression(null_mut(), context, useprefix, true);
            result = lappend(result, exprstr as *mut c_void);
        });

        if result.is_null() && (*es).format == EXPLAIN_FORMAT_TEXT {
            ExplainPropertyText(keyname, c"()".as_ptr(), es);
        } else {
            ExplainPropertyListNested(keyname, result, es);
        }
    });

    ExplainCloseGroup(keysetname, keysetname, false, es);

    if !sortnode.is_null() && (*es).format == EXPLAIN_FORMAT_TEXT {
        (*es).indent -= 1;
    }

    ExplainCloseGroup(c"Grouping Set".as_ptr(), null(), true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
unsafe fn show_group_keys(gstate: *mut GroupState, mut ancestors: *mut List, es: *mut ExplainState) {
    let plan = (*gstate).ss.ps.plan as *mut Group;

    /* The key columns refer to the tlist of the child plan */
    ancestors = lcons(plan as *mut c_void, ancestors);
    show_sort_group_keys(
        outerPlanState(gstate as *mut PlanState),
        c"Group Key".as_ptr(),
        (*plan).numCols,
        0,
        (*plan).grpColIdx,
        null_mut(),
        null_mut(),
        null_mut(),
        ancestors,
        es,
    );
    ancestors = list_delete_first(ancestors);
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
unsafe fn show_sort_group_keys(
    planstate: *mut PlanState,
    qlabel: *const c_char,
    nkeys: c_int,
    nPresortedKeys: c_int,
    keycols: *mut i16,
    sortOperators: *mut Oid,
    collations: *mut Oid,
    nullsFirst: *mut bool,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = (*planstate).plan;
    let context: *mut List;
    let mut result: *mut List = null_mut();
    let mut resultPresorted: *mut List = null_mut();
    let mut sortkeybuf: StringInfoData = core::mem::zeroed();
    let useprefix: bool;

    if nkeys <= 0 {
        return;
    }

    initStringInfo(&mut sortkeybuf);

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, plan, ancestors);
    useprefix = (*es).rtable_size > 1 || (*es).verbose;

    for keyno in 0..nkeys {
        /* find key expression in tlist */
        let keyresno = *keycols.add(keyno as usize);
        let target = get_tle_by_resno((*plan).targetlist, keyresno);
        let exprstr: *mut c_char;

        if target.is_null() {
            ereport!(ERROR, errmsg!("no tlist entry for key {}", keyresno));
        }
        /* Deparse the expression, showing any top-level cast */
        /* TODO(pg-port): tle->expr -- TargetEntry not yet accessible */
        exprstr = deparse_expression(null_mut(), context, useprefix, true);
        resetStringInfo(&mut sortkeybuf);
        appendStringInfoString(&mut sortkeybuf, exprstr);
        /* Append sort order information, if relevant */
        if !sortOperators.is_null() {
            show_sortorder_options(
                &mut sortkeybuf,
                null_mut(),
                *sortOperators.add(keyno as usize),
                *collations.add(keyno as usize),
                *nullsFirst.add(keyno as usize),
            );
        }
        /* Emit one property-list item per sort key */
        result = lappend(result, pstrdup(sortkeybuf.data) as *mut c_void);
        if keyno < nPresortedKeys {
            resultPresorted = lappend(resultPresorted, exprstr as *mut c_void);
        }
    }

    ExplainPropertyList(qlabel, result, es);
    if nPresortedKeys > 0 {
        ExplainPropertyList(c"Presorted Key".as_ptr(), resultPresorted, es);
    }
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
unsafe fn show_sortorder_options(
    buf: *mut StringInfoData,
    sortexpr: *mut Node,
    sortOperator: Oid,
    collation: Oid,
    nullsFirst: bool,
) {
    let sortcoltype = exprType(sortexpr);
    let mut reverse = false;
    let typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

    /*
     * Print COLLATE if it's not default for the column's type.
     */
    if OidIsValid(collation) && collation != get_typcollation(sortcoltype) {
        let collname = get_collation_name(collation);

        if collname.is_null() {
            ereport!(ERROR, errmsg!("cache lookup failed for collation {}", collation));
        }
        appendStringInfo!(buf, " COLLATE {}", cstr_s(quote_identifier(collname)));
    }

    /* Print direction if not ASC, or USING if non-default sort operator */
    if !typentry.is_null() && sortOperator == (*(typentry as *mut TypeCacheEntryStub)).gt_opr {
        appendStringInfoString(buf, c" DESC".as_ptr());
        reverse = true;
    } else if typentry.is_null()
        || sortOperator != (*(typentry as *mut TypeCacheEntryStub)).lt_opr
    {
        let opname = get_opname(sortOperator);
        if opname.is_null() {
            ereport!(ERROR, errmsg!("cache lookup failed for operator {}", sortOperator));
        }
        appendStringInfo!(buf, " USING {}", cstr_s(opname));
        /* Determine whether operator would be considered ASC or DESC */
        get_equality_op_for_ordering_op(sortOperator, &mut reverse);
    }

    /* Add NULLS FIRST/LAST only if it wouldn't be default */
    if nullsFirst && !reverse {
        appendStringInfoString(buf, c" NULLS FIRST".as_ptr());
    } else if !nullsFirst && reverse {
        appendStringInfoString(buf, c" NULLS LAST".as_ptr());
    }
}

// Stub for TypeCacheEntry with just the fields we need
#[repr(C)]
struct TypeCacheEntryStub {
    lt_opr: Oid,
    gt_opr: Oid,
}

// ---------------------------------------------------------------------------
// WindowAgg display
// ---------------------------------------------------------------------------

/*
 * Show the window definition for a WindowAgg node.
 */
unsafe fn show_window_def(
    planstate: *mut WindowAggState,
    mut ancestors: *mut List,
    es: *mut ExplainState,
) {
    let wagg = (*planstate).ss.ps.plan as *mut WindowAgg;
    let mut wbuf: StringInfoData = core::mem::zeroed();
    let mut needspace = false;

    initStringInfo(&mut wbuf);
    appendStringInfo!(&mut wbuf, "{} AS (", cstr_s(quote_identifier((*wagg).winname)));

    /* The key columns refer to the tlist of the child plan */
    ancestors = lcons(wagg as *mut c_void, ancestors);
    if (*wagg).partNumCols > 0 {
        appendStringInfoString(&mut wbuf, c"PARTITION BY ".as_ptr());
        show_window_keys(
            &mut wbuf,
            outerPlanState(planstate as *mut PlanState),
            (*wagg).partNumCols,
            (*wagg).partColIdx,
            ancestors,
            es,
        );
        needspace = true;
    }
    if (*wagg).ordNumCols > 0 {
        if needspace {
            appendStringInfoChar(&mut wbuf, b' ' as c_char);
        }
        appendStringInfoString(&mut wbuf, c"ORDER BY ".as_ptr());
        show_window_keys(
            &mut wbuf,
            outerPlanState(planstate as *mut PlanState),
            (*wagg).ordNumCols,
            (*wagg).ordColIdx,
            ancestors,
            es,
        );
        needspace = true;
    }
    ancestors = list_delete_first(ancestors);

    if (*wagg).frameOptions & FRAMEOPTION_NONDEFAULT != 0 {
        let context: *mut List;
        let useprefix: bool;
        let framestr: *mut c_char;

        /* Set up deparsing context for possible frame expressions */
        context = set_deparse_context_plan(
            (*es).deparse_cxt,
            (*(planstate as *mut PlanState)).plan,
            ancestors,
        );
        useprefix = (*es).rtable_size > 1 || (*es).verbose;
        framestr = get_window_frame_options_for_explain(
            (*wagg).frameOptions,
            (*wagg).startOffset,
            (*wagg).endOffset,
            context,
            useprefix,
        );
        if needspace {
            appendStringInfoChar(&mut wbuf, b' ' as c_char);
        }
        appendStringInfoString(&mut wbuf, framestr);
        pfree(framestr as *mut c_void);
    }
    appendStringInfoChar(&mut wbuf, b')' as c_char);
    ExplainPropertyText(c"Window".as_ptr(), wbuf.data, es);
    pfree(wbuf.data as *mut c_void);
}

/*
 * Append the keys of a window's PARTITION BY or ORDER BY clause to buf.
 */
unsafe fn show_window_keys(
    buf: *mut StringInfoData,
    planstate: *mut PlanState,
    nkeys: c_int,
    keycols: *mut i16,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = (*planstate).plan;
    let context: *mut List;
    let useprefix: bool;

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, plan, ancestors);
    useprefix = (*es).rtable_size > 1 || (*es).verbose;

    for keyno in 0..nkeys {
        /* find key expression in tlist */
        let keyresno = *keycols.add(keyno as usize);
        let target = get_tle_by_resno((*plan).targetlist, keyresno);
        let exprstr: *mut c_char;

        if target.is_null() {
            ereport!(ERROR, errmsg!("no tlist entry for key {}", keyresno));
        }
        /* Deparse the expression, showing any top-level cast */
        /* TODO(pg-port): tle->expr -- TargetEntry not yet accessible */
        exprstr = deparse_expression(null_mut(), context, useprefix, true);
        if keyno > 0 {
            appendStringInfoString(buf, c", ".as_ptr());
        }
        appendStringInfoString(buf, exprstr);
        pfree(exprstr as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// show_storage_info, show_tablesample, show_sort_info, show_incremental_sort_info
// show_hash_info, show_material_info, show_windowagg_info, show_ctescan_info
// show_table_func_scan_info, show_recursive_union_info, show_memoize_info
// show_hashagg_info, show_indexsearches_info, show_tidbitmap_info
// show_instrumentation_count, show_foreignscan_info
// ---------------------------------------------------------------------------

/*
 * Show information on storage method and maximum memory/disk space used.
 */
unsafe fn show_storage_info(maxStorageType: *mut c_char, maxSpaceUsed: int64, es: *mut ExplainState) {
    let maxSpaceUsedKB: int64 = BYTES_TO_KILOBYTES!(maxSpaceUsed);

    if (*es).format != EXPLAIN_FORMAT_TEXT {
        ExplainPropertyText(c"Storage".as_ptr(), maxStorageType, es);
        ExplainPropertyInteger(c"Maximum Storage".as_ptr(), c"kB".as_ptr(), maxSpaceUsedKB, es);
    } else {
        ExplainIndentText(es);
        appendStringInfo!(
            (*es).str,
            "Storage: {}  Maximum Storage: {}kB\n",
            cstr_s(maxStorageType),
            maxSpaceUsedKB
        );
    }
}

/*
 * Show TABLESAMPLE properties
 */
unsafe fn show_tablesample(
    tsc: *mut TableSampleClause,
    planstate: *mut PlanState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let context: *mut List;
    let useprefix: bool;
    let method_name: *mut c_char;
    let mut params: *mut List = null_mut();
    let repeatable: *mut c_char;

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, (*planstate).plan, ancestors);
    useprefix = (*es).rtable_size > 1;

    /* Get the tablesample method name */
    method_name = get_func_name(tsc_tsmhandler(tsc));

    /* Deparse parameter expressions */
    foreach!(lc, tsc_args(tsc), {
        let arg = lfirst(current_cell!(lc)) as *mut Node;
        params = lappend(params, deparse_expression(arg, context, useprefix, false) as *mut c_void);
    });

    if !tsc_repeatable(tsc).is_null() {
        repeatable = deparse_expression(tsc_repeatable(tsc), context, useprefix, false);
    } else {
        repeatable = null_mut();
    }

    /* Print results */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        let mut first = true;

        ExplainIndentText(es);
        appendStringInfo!((*es).str, "Sampling: {} (", cstr_s(method_name));
        foreach!(lc, params, {
            if !first {
                appendStringInfoString((*es).str, c", ".as_ptr());
            }
            appendStringInfoString((*es).str, lfirst(current_cell!(lc)) as *const c_char);
            first = false;
        });
        appendStringInfoChar((*es).str, b')' as c_char);
        if !repeatable.is_null() {
            appendStringInfo!((*es).str, " REPEATABLE ({})", cstr_s(repeatable));
        }
        appendStringInfoChar((*es).str, b'\n' as c_char);
    } else {
        ExplainPropertyText(c"Sampling Method".as_ptr(), method_name, es);
        ExplainPropertyList(c"Sampling Parameters".as_ptr(), params, es);
        if !repeatable.is_null() {
            ExplainPropertyText(c"Repeatable Seed".as_ptr(), repeatable, es);
        }
    }
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
unsafe fn show_sort_info(sortstate: *mut SortState, es: *mut ExplainState) {
    if !(*es).analyze {
        return;
    }

    if (*sortstate).sort_Done && !(*sortstate).tuplesortstate.is_null() {
        let state = (*sortstate).tuplesortstate as *mut Tuplesortstate;
        let _stats: core::mem::MaybeUninit<[u8; 32]> = core::mem::MaybeUninit::uninit();
        // TODO(pg-port): tuplesort_get_stats / tuplesort_method_name / tuplesort_space_type_name
        // Stub: skip
    }

    // TODO(pg-port): shared_info for parallel sort workers
}

/*
 * This function is used for both a non-parallel node and each worker in a
 * parallel incremental sort node.
 */
unsafe fn show_incremental_sort_group_info(
    groupInfo: *mut IncrementalSortGroupInfo,
    groupLabel: *const c_char,
    indent: bool,
    es: *mut ExplainState,
) {
    let mut methodNames: *mut List = null_mut();

    /* Generate a list of sort methods used across all groups. */
    for bit in 0..NUM_TUPLESORTMETHODS {
        let sortMethod: TuplesortMethod = 1 << bit;
        if (*groupInfo).sortMethods & sortMethod as bits32 != 0 {
            let methodName = tuplesort_method_name(sortMethod);
            methodNames = lappend(methodNames, methodName as *mut c_char as *mut c_void);
        }
    }

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        if indent {
            appendStringInfoSpaces((*es).str, (*es).indent * 2);
        }
        appendStringInfo!(
            (*es).str,
            "{} Groups: {}  Sort Method",
            cstr_s(groupLabel),
            (*groupInfo).groupCount
        );
        /* plural/singular based on methodNames size */
        if list_length(methodNames) > 1 {
            appendStringInfoString((*es).str, c"s: ".as_ptr());
        } else {
            appendStringInfoString((*es).str, c": ".as_ptr());
        }
        let mut idx: c_int = 0;
        let total = list_length(methodNames);
        foreach!(methodCell, methodNames, {
            appendStringInfoString((*es).str, lfirst(current_cell!(methodCell)) as *const c_char);
            if idx < total - 1 {
                appendStringInfoString((*es).str, c", ".as_ptr());
            }
            idx += 1;
        });

        if (*groupInfo).maxMemorySpaceUsed > 0 {
            let avgSpace = (*groupInfo).totalMemorySpaceUsed / (*groupInfo).groupCount;
            let spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
            appendStringInfo!(
                (*es).str,
                "  Average {}: {}kB  Peak {}: {}kB",
                cstr_s(spaceTypeName), avgSpace,
                cstr_s(spaceTypeName), (*groupInfo).maxMemorySpaceUsed
            );
        }

        if (*groupInfo).maxDiskSpaceUsed > 0 {
            let avgSpace = (*groupInfo).totalDiskSpaceUsed / (*groupInfo).groupCount;
            let spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
            appendStringInfo!(
                (*es).str,
                "  Average {}: {}kB  Peak {}: {}kB",
                cstr_s(spaceTypeName), avgSpace,
                cstr_s(spaceTypeName), (*groupInfo).maxDiskSpaceUsed
            );
        }
    } else {
        let mut groupName: StringInfoData = core::mem::zeroed();
        initStringInfo(&mut groupName);
        appendStringInfo!(&mut groupName as *mut StringInfoData, "{} Groups", cstr_s(groupLabel));

        ExplainOpenGroup(c"Incremental Sort Groups".as_ptr(), groupName.data, true, es);
        ExplainPropertyInteger(c"Group Count".as_ptr(), null(), (*groupInfo).groupCount, es);

        ExplainPropertyList(c"Sort Methods Used".as_ptr(), methodNames, es);

        if (*groupInfo).maxMemorySpaceUsed > 0 {
            let avgSpace = (*groupInfo).totalMemorySpaceUsed / (*groupInfo).groupCount;
            let spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
            let mut memoryName: StringInfoData = core::mem::zeroed();
            initStringInfo(&mut memoryName);
            appendStringInfo!(&mut memoryName as *mut StringInfoData, "Sort Space {}", cstr_s(spaceTypeName));
            ExplainOpenGroup(c"Sort Space".as_ptr(), memoryName.data, true, es);
            ExplainPropertyInteger(c"Average Sort Space Used".as_ptr(), c"kB".as_ptr(), avgSpace, es);
            ExplainPropertyInteger(c"Peak Sort Space Used".as_ptr(), c"kB".as_ptr(), (*groupInfo).maxMemorySpaceUsed, es);
            ExplainCloseGroup(c"Sort Space".as_ptr(), memoryName.data, true, es);
        }
        if (*groupInfo).maxDiskSpaceUsed > 0 {
            let avgSpace = (*groupInfo).totalDiskSpaceUsed / (*groupInfo).groupCount;
            let spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
            let mut diskName: StringInfoData = core::mem::zeroed();
            initStringInfo(&mut diskName);
            appendStringInfo!(&mut diskName as *mut StringInfoData, "Sort Space {}", cstr_s(spaceTypeName));
            ExplainOpenGroup(c"Sort Space".as_ptr(), diskName.data, true, es);
            ExplainPropertyInteger(c"Average Sort Space Used".as_ptr(), c"kB".as_ptr(), avgSpace, es);
            ExplainPropertyInteger(c"Peak Sort Space Used".as_ptr(), c"kB".as_ptr(), (*groupInfo).maxDiskSpaceUsed, es);
            ExplainCloseGroup(c"Sort Space".as_ptr(), diskName.data, true, es);
        }

        ExplainCloseGroup(c"Incremental Sort Groups".as_ptr(), groupName.data, true, es);
    }
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for an incremental sort node
 */
unsafe fn show_incremental_sort_info(
    incrsortstate: *mut IncrementalSortState,
    es: *mut ExplainState,
) {
    // Cast incsort_info to our local layout-compatible mirror.
    let fullsortGroupInfo = &mut (*incrsortstate).incsort_info.fullsortGroupInfo
        as *mut _ as *mut IncrementalSortGroupInfo;

    if !(*es).analyze {
        return;
    }

    /*
     * Since we never have any prefix groups unless we've first sorted a full
     * groups and transitioned modes (copying the tuples into a prefix group),
     * we don't need to do anything if there were 0 full groups.
     */
    if (*fullsortGroupInfo).groupCount > 0 {
        show_incremental_sort_group_info(fullsortGroupInfo, c"Full-sort".as_ptr(), true, es);
        let prefixsortGroupInfo = &mut (*incrsortstate).incsort_info.prefixsortGroupInfo
            as *mut _ as *mut IncrementalSortGroupInfo;
        if (*prefixsortGroupInfo).groupCount > 0 {
            if (*es).format == EXPLAIN_FORMAT_TEXT {
                appendStringInfoChar((*es).str, b'\n' as c_char);
            }
            show_incremental_sort_group_info(prefixsortGroupInfo, c"Pre-sorted".as_ptr(), true, es);
        }
        if (*es).format == EXPLAIN_FORMAT_TEXT {
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }
    }

    if !(*incrsortstate).shared_info.is_null() {
        let shared_info = (*incrsortstate).shared_info as *mut SharedIncrementalSortInfo;

        for n in 0..(*shared_info).num_workers {
            // sinfo[] is a flexible array at the end of SharedIncrementalSortInfo
            let incsort_info = (shared_info as *mut u8)
                .add(core::mem::size_of::<SharedIncrementalSortInfo>())
                .add(n as usize * core::mem::size_of::<IncrementalSortInfo>())
                as *mut IncrementalSortInfo;
            let fullsortGroupInfo = &mut (*incsort_info).fullsortGroupInfo as *mut IncrementalSortGroupInfo;

            /*
             * If a worker hasn't processed any sort groups at all, then
             * exclude it from output since it either didn't launch or didn't
             * contribute anything meaningful.
             */
            if (*fullsortGroupInfo).groupCount == 0 {
                continue;
            }

            if !(*es).workers_state.is_null() {
                ExplainOpenWorker(n, es);
            }

            let indent_first_line = (*es).workers_state.is_null() || (*es).verbose;
            show_incremental_sort_group_info(fullsortGroupInfo, c"Full-sort".as_ptr(), indent_first_line, es);
            let prefixsortGroupInfo = &mut (*incsort_info).prefixsortGroupInfo as *mut IncrementalSortGroupInfo;
            if (*prefixsortGroupInfo).groupCount > 0 {
                if (*es).format == EXPLAIN_FORMAT_TEXT {
                    appendStringInfoChar((*es).str, b'\n' as c_char);
                }
                show_incremental_sort_group_info(prefixsortGroupInfo, c"Pre-sorted".as_ptr(), true, es);
            }
            if (*es).format == EXPLAIN_FORMAT_TEXT {
                appendStringInfoChar((*es).str, b'\n' as c_char);
            }

            if !(*es).workers_state.is_null() {
                ExplainCloseWorker(n, es);
            }
        }
    }
}

/*
 * Show information on hash buckets/batches.
 */
unsafe fn show_hash_info(hashstate: *mut HashState, es: *mut ExplainState) {
    use crate::nodes::execnodes::HashInstrumentation;

    let mut hinstrument: HashInstrumentation = core::mem::zeroed();

    /*
     * Collect stats from the local process, even when it's a parallel query.
     * In a parallel query, the leader process may or may not have run the
     * hash join, and even if it did it may not have built a hash table due to
     * timing (if it started late it might have seen no tuples in the outer
     * relation and skipped building the hash table).  Therefore we have to be
     * prepared to get instrumentation data from all participants.
     */
    if !(*hashstate).hinstrument.is_null() {
        memcpy(
            &mut hinstrument as *mut HashInstrumentation as *mut c_void,
            (*hashstate).hinstrument as *const c_void,
            core::mem::size_of::<HashInstrumentation>(),
        );
    }

    /*
     * Merge results from workers.  In the parallel-oblivious case, the
     * results from all participants should be identical, except where
     * participants didn't run the join at all so have no data.  In the
     * parallel-aware case, we need to consider all the results.  Each worker
     * may have seen a different subset of batches and we want to report the
     * highest memory usage across all batches.  We take the maxima of other
     * values too, for the same reasons as in ExecHashAccumInstrumentation.
     */
    if !(*hashstate).shared_info.is_null() {
        let shared_info = (*hashstate).shared_info;
        for i in 0..(*shared_info).num_workers {
            // hinstrument[] is flexible array at end of SharedHashInfo
            let worker_hi = (shared_info as *mut u8)
                .add(core::mem::size_of::<crate::nodes::execnodes::SharedHashInfo>())
                .add(i as usize * core::mem::size_of::<HashInstrumentation>())
                as *mut HashInstrumentation;

            if (*worker_hi).nbuckets > hinstrument.nbuckets {
                hinstrument.nbuckets = (*worker_hi).nbuckets;
            }
            if (*worker_hi).nbuckets_original > hinstrument.nbuckets_original {
                hinstrument.nbuckets_original = (*worker_hi).nbuckets_original;
            }
            if (*worker_hi).nbatch > hinstrument.nbatch {
                hinstrument.nbatch = (*worker_hi).nbatch;
            }
            if (*worker_hi).nbatch_original > hinstrument.nbatch_original {
                hinstrument.nbatch_original = (*worker_hi).nbatch_original;
            }
            if (*worker_hi).space_peak > hinstrument.space_peak {
                hinstrument.space_peak = (*worker_hi).space_peak;
            }
        }
    }

    if hinstrument.nbatch > 0 {
        let spacePeakKb: uint64 = BYTES_TO_KILOBYTES!(hinstrument.space_peak as uint64);

        if (*es).format != EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger(c"Hash Buckets".as_ptr(), null(), hinstrument.nbuckets as int64, es);
            ExplainPropertyInteger(c"Original Hash Buckets".as_ptr(), null(), hinstrument.nbuckets_original as int64, es);
            ExplainPropertyInteger(c"Hash Batches".as_ptr(), null(), hinstrument.nbatch as int64, es);
            ExplainPropertyInteger(c"Original Hash Batches".as_ptr(), null(), hinstrument.nbatch_original as int64, es);
            ExplainPropertyUInteger(c"Peak Memory Usage".as_ptr(), c"kB".as_ptr(), spacePeakKb, es);
        } else if hinstrument.nbatch_original != hinstrument.nbatch
            || hinstrument.nbuckets_original != hinstrument.nbuckets
        {
            ExplainIndentText(es);
            appendStringInfo!(
                (*es).str,
                "Buckets: {} (originally {})  Batches: {} (originally {})  Memory Usage: {}kB\n",
                hinstrument.nbuckets,
                hinstrument.nbuckets_original,
                hinstrument.nbatch,
                hinstrument.nbatch_original,
                spacePeakKb
            );
        } else {
            ExplainIndentText(es);
            appendStringInfo!(
                (*es).str,
                "Buckets: {}  Batches: {}  Memory Usage: {}kB\n",
                hinstrument.nbuckets,
                hinstrument.nbatch,
                spacePeakKb
            );
        }
    }
}

/*
 * Show information on material node, storage method and maximum memory/disk
 * space used.
 */
unsafe fn show_material_info(mstate: *mut MaterialState, es: *mut ExplainState) {
    let mut maxStorageType: *mut c_char = null_mut();
    let mut maxSpaceUsed: int64 = 0;

    let tupstore = (*mstate).tuplestorestate as *mut Tuplestorestate;

    /*
     * Nothing to show if ANALYZE option wasn't used or if execution didn't
     * get as far as creating the tuplestore.
     */
    if !(*es).analyze || tupstore.is_null() {
        return;
    }

    tuplestore_get_stats(tupstore, &mut maxStorageType, &mut maxSpaceUsed);
    show_storage_info(maxStorageType, maxSpaceUsed, es);
}

/*
 * Show information on WindowAgg node, storage method and maximum memory/disk
 * space used.
 */
unsafe fn show_windowagg_info(winstate: *mut WindowAggState, es: *mut ExplainState) {
    let mut maxStorageType: *mut c_char = null_mut();
    let mut maxSpaceUsed: int64 = 0;

    let tupstore = (*winstate).buffer as *mut Tuplestorestate;

    if !(*es).analyze || tupstore.is_null() {
        return;
    }

    tuplestore_get_stats(tupstore, &mut maxStorageType, &mut maxSpaceUsed);
    show_storage_info(maxStorageType, maxSpaceUsed, es);
}

/*
 * Show information on CTE Scan node, storage method and maximum memory/disk
 * space used.
 */
unsafe fn show_ctescan_info(ctescanstate: *mut CteScanState, es: *mut ExplainState) {
    let mut maxStorageType: *mut c_char = null_mut();
    let mut maxSpaceUsed: int64 = 0;

    // TODO(pg-port): CteScanState.leader->cte_table not yet accessible
    let tupstore: *mut Tuplestorestate = null_mut();

    if !(*es).analyze || tupstore.is_null() {
        return;
    }

    tuplestore_get_stats(tupstore, &mut maxStorageType, &mut maxSpaceUsed);
    show_storage_info(maxStorageType, maxSpaceUsed, es);
}

/*
 * Show information on Table Function Scan node, storage method and maximum
 * memory/disk space used.
 */
unsafe fn show_table_func_scan_info(tscanstate: *mut TableFuncScanState, es: *mut ExplainState) {
    let mut maxStorageType: *mut c_char = null_mut();
    let mut maxSpaceUsed: int64 = 0;

    // TODO(pg-port): TableFuncScanState.tupstore not yet accessible
    let tupstore: *mut Tuplestorestate = null_mut();

    if !(*es).analyze || tupstore.is_null() {
        return;
    }

    tuplestore_get_stats(tupstore, &mut maxStorageType, &mut maxSpaceUsed);
    show_storage_info(maxStorageType, maxSpaceUsed, es);
}

/*
 * Show information on Recursive Union node, storage method and maximum
 * memory/disk space used.
 */
unsafe fn show_recursive_union_info(rstate: *mut RecursiveUnionState, es: *mut ExplainState) {
    let mut maxStorageType: *mut c_char = null_mut();
    let mut tempStorageType: *mut c_char = null_mut();
    let mut maxSpaceUsed: int64 = 0;
    let mut tempSpaceUsed: int64 = 0;

    if !(*es).analyze {
        return;
    }

    /*
     * Recursive union node uses two tuplestores.  We employ the storage type
     * from one of them which consumed more memory/disk than the other.  The
     * storage size is sum of the two.
     */
    tuplestore_get_stats((*rstate).working_table as *mut Tuplestorestate, &mut tempStorageType, &mut tempSpaceUsed);
    tuplestore_get_stats((*rstate).intermediate_table as *mut Tuplestorestate, &mut maxStorageType, &mut maxSpaceUsed);

    if tempSpaceUsed > maxSpaceUsed {
        maxStorageType = tempStorageType;
    }

    maxSpaceUsed += tempSpaceUsed;
    show_storage_info(maxStorageType, maxSpaceUsed, es);
}

/*
 * Show information on memoize hits/misses/evictions and memory usage.
 */
unsafe fn show_memoize_info(
    mstate: *mut MemoizeState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let plan = mstate as *mut PlanState;
    let context: *mut List;
    let mut keystr: StringInfoData = core::mem::zeroed();
    let separator = c"".as_ptr() as *const c_char;
    let useprefix: bool;
    let memPeakKb: int64;

    initStringInfo(&mut keystr);

    useprefix = (*es).rtable_size > 1 || (*es).verbose;

    /* Set up deparsing context */
    context = set_deparse_context_plan((*es).deparse_cxt, (*plan).plan, ancestors);

    // TODO(pg-port): ((Memoize *)plan)->param_exprs not yet accessible
    ExplainPropertyText(c"Cache Key".as_ptr(), keystr.data, es);
    ExplainPropertyText(
        c"Cache Mode".as_ptr(),
        if (*mstate).binary_mode { c"binary".as_ptr() } else { c"logical".as_ptr() },
        es,
    );

    pfree(keystr.data as *mut c_void);

    if !(*es).analyze {
        return;
    }

    // TODO(pg-port): mstate->stats.cache_misses / mem_peak / mem_used not yet accessible
}

/*
 * Show information on hash aggregate memory usage and batches.
 */
unsafe fn show_hashagg_info(aggstate: *mut AggState, es: *mut ExplainState) {
    use crate::nodes::execnodes::{AggregateInstrumentation, SharedAggInfo as ExecSharedAggInfo};

    let agg = (*aggstate).ss.ps.plan as *mut Agg;
    let memPeakKb: int64 = BYTES_TO_KILOBYTES!((*aggstate).hash_mem_peak as uint64) as int64;

    if (*agg).aggstrategy != AGG_HASHED && (*agg).aggstrategy != AGG_MIXED {
        return;
    }

    if (*es).format != EXPLAIN_FORMAT_TEXT {
        if (*es).costs {
            ExplainPropertyInteger(
                c"Planned Partitions".as_ptr(), null(),
                (*aggstate).hash_planned_partitions as int64, es,
            );
        }

        /*
         * During parallel query the leader may have not helped out.  We
         * detect this by checking how much memory it used.  If we find it
         * didn't do any work then we don't show its properties.
         */
        if (*es).analyze && (*aggstate).hash_mem_peak > 0 {
            ExplainPropertyInteger(c"HashAgg Batches".as_ptr(), null(), (*aggstate).hash_batches_used as int64, es);
            ExplainPropertyInteger(c"Peak Memory Usage".as_ptr(), c"kB".as_ptr(), memPeakKb, es);
            ExplainPropertyInteger(c"Disk Usage".as_ptr(), c"kB".as_ptr(), (*aggstate).hash_disk_used as int64, es);
        }
    } else {
        let mut gotone = false;

        if (*es).costs && (*aggstate).hash_planned_partitions > 0 {
            ExplainIndentText(es);
            appendStringInfo!((*es).str, "Planned Partitions: {}", (*aggstate).hash_planned_partitions);
            gotone = true;
        }

        /*
         * During parallel query the leader may have not helped out.  We
         * detect this by checking how much memory it used.  If we find it
         * didn't do any work then we don't show its properties.
         */
        if (*es).analyze && (*aggstate).hash_mem_peak > 0 {
            if !gotone {
                ExplainIndentText(es);
            } else {
                appendStringInfoSpaces((*es).str, 2);
            }

            appendStringInfo!(
                (*es).str,
                "Batches: {}  Memory Usage: {}kB",
                (*aggstate).hash_batches_used, memPeakKb
            );
            gotone = true;

            /* Only display disk usage if we spilled to disk */
            if (*aggstate).hash_batches_used > 1 {
                appendStringInfo!((*es).str, "  Disk Usage: {}kB", (*aggstate).hash_disk_used);
            }
        }

        if gotone {
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }
    }

    /* Display stats for each parallel worker */
    if (*es).analyze && !(*aggstate).shared_info.is_null() {
        let shared_info = (*aggstate).shared_info as *mut ExecSharedAggInfo;
        for n in 0..(*shared_info).num_workers {
            let sinstrument = (shared_info as *mut u8)
                .add(core::mem::size_of::<ExecSharedAggInfo>())
                .add(n as usize * core::mem::size_of::<AggregateInstrumentation>())
                as *mut AggregateInstrumentation;
            /* Skip workers that didn't do anything */
            if (*sinstrument).hash_mem_peak == 0 {
                continue;
            }
            let hash_disk_used = (*sinstrument).hash_disk_used;
            let hash_batches_used = (*sinstrument).hash_batches_used;
            let memPeakKb: int64 = BYTES_TO_KILOBYTES!((*sinstrument).hash_mem_peak as uint64) as int64;

            if !(*es).workers_state.is_null() {
                ExplainOpenWorker(n, es);
            }

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfo!(
                    (*es).str,
                    "Batches: {}  Memory Usage: {}kB",
                    hash_batches_used, memPeakKb
                );
                /* Only display disk usage if we spilled to disk */
                if hash_batches_used > 1 {
                    appendStringInfo!((*es).str, "  Disk Usage: {}kB", hash_disk_used);
                }
                appendStringInfoChar((*es).str, b'\n' as c_char);
            } else {
                ExplainPropertyInteger(c"HashAgg Batches".as_ptr(), null(), hash_batches_used as int64, es);
                ExplainPropertyInteger(c"Peak Memory Usage".as_ptr(), c"kB".as_ptr(), memPeakKb, es);
                ExplainPropertyInteger(c"Disk Usage".as_ptr(), c"kB".as_ptr(), hash_disk_used as int64, es);
            }

            if !(*es).workers_state.is_null() {
                ExplainCloseWorker(n, es);
            }
        }
    }
}

/*
 * Show the total number of index searches for a
 * IndexScan/IndexOnlyScan/BitmapIndexScan node
 */
unsafe fn show_indexsearches_info(planstate: *mut PlanState, es: *mut ExplainState) {
    let plan = (*planstate).plan;
    let mut shared_info_ptr: *mut crate::nodes::execnodes::SharedIndexScanInstrumentation = null_mut();
    let mut nsearches: uint64 = 0;

    if !(*es).analyze {
        return;
    }

    /* Initialize counters with stats from the local process first */
    match nodeTag(plan) {
        NodeTag::T_IndexScan => {
            let indexstate = planstate as *mut IndexScanState;
            nsearches = iss_instr_nsearches(&mut (*indexstate).iss_Instrument as *mut _);
            shared_info_ptr = (*indexstate).iss_SharedInfo;
        }
        NodeTag::T_IndexOnlyScan => {
            let indexstate = planstate as *mut IndexOnlyScanState;
            nsearches = iss_instr_nsearches(&mut (*indexstate).ioss_Instrument as *mut _);
            shared_info_ptr = (*indexstate).ioss_SharedInfo;
        }
        NodeTag::T_BitmapIndexScan => {
            let indexstate = planstate as *mut BitmapIndexScanState;
            nsearches = iss_instr_nsearches(&mut (*indexstate).biss_Instrument as *mut _);
            shared_info_ptr = (*indexstate).biss_SharedInfo;
        }
        _ => {}
    }

    /* Next get the sum of the counters set within each and every process */
    if !shared_info_ptr.is_null() {
        let num_workers = shared_iss_num_workers(shared_info_ptr);
        for i in 0..num_workers {
            let winstrument = shared_iss_winstrument(shared_info_ptr, i);
            nsearches += (*winstrument).nsearches;
        }
    }

    ExplainPropertyUInteger(c"Index Searches".as_ptr(), null(), nsearches, es);
}

/*
 * Show exact/lossy pages for a BitmapHeapScan node
 */
unsafe fn show_tidbitmap_info(planstate: *mut BitmapHeapScanState, es: *mut ExplainState) {
    use crate::nodes::execnodes::{BitmapHeapScanInstrumentation, SharedBitmapHeapInstrumentation};

    if !(*es).analyze {
        return;
    }

    if (*es).format != EXPLAIN_FORMAT_TEXT {
        ExplainPropertyUInteger(c"Exact Heap Blocks".as_ptr(), null(), (*planstate).stats.exact_pages, es);
        ExplainPropertyUInteger(c"Lossy Heap Blocks".as_ptr(), null(), (*planstate).stats.lossy_pages, es);
    } else {
        if (*planstate).stats.exact_pages > 0 || (*planstate).stats.lossy_pages > 0 {
            ExplainIndentText(es);
            appendStringInfoString((*es).str, c"Heap Blocks:".as_ptr());
            if (*planstate).stats.exact_pages > 0 {
                appendStringInfo!((*es).str, " exact={}", (*planstate).stats.exact_pages);
            }
            if (*planstate).stats.lossy_pages > 0 {
                appendStringInfo!((*es).str, " lossy={}", (*planstate).stats.lossy_pages);
            }
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }
    }

    /* Display stats for each parallel worker */
    if !(*planstate).pstate.is_null() && !(*planstate).sinstrument.is_null() {
        let sinstrument = (*planstate).sinstrument;
        for n in 0..(*sinstrument).num_workers {
            let si = (sinstrument as *mut u8)
                .add(core::mem::size_of::<SharedBitmapHeapInstrumentation>())
                .add(n as usize * core::mem::size_of::<BitmapHeapScanInstrumentation>())
                as *mut BitmapHeapScanInstrumentation;

            if (*si).exact_pages == 0 && (*si).lossy_pages == 0 {
                continue;
            }

            if !(*es).workers_state.is_null() {
                ExplainOpenWorker(n, es);
            }

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfoString((*es).str, c"Heap Blocks:".as_ptr());
                if (*si).exact_pages > 0 {
                    appendStringInfo!((*es).str, " exact={}", (*si).exact_pages);
                }
                if (*si).lossy_pages > 0 {
                    appendStringInfo!((*es).str, " lossy={}", (*si).lossy_pages);
                }
                appendStringInfoChar((*es).str, b'\n' as c_char);
            } else {
                ExplainPropertyUInteger(c"Exact Heap Blocks".as_ptr(), null(), (*si).exact_pages, es);
                ExplainPropertyUInteger(c"Lossy Heap Blocks".as_ptr(), null(), (*si).lossy_pages, es);
            }

            if !(*es).workers_state.is_null() {
                ExplainCloseWorker(n, es);
            }
        }
    }
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
unsafe fn show_instrumentation_count(
    qlabel: *const c_char,
    which: c_int,
    planstate: *mut PlanState,
    es: *mut ExplainState,
) {
    let nfiltered: f64;
    let nloops: f64;

    if !(*es).analyze || (*planstate).instrument.is_null() {
        return;
    }

    if which == 2 {
        nfiltered = (*(*planstate).instrument).nfiltered2;
    } else {
        nfiltered = (*(*planstate).instrument).nfiltered1;
    }
    nloops = (*(*planstate).instrument).nloops;

    /* In text mode, suppress zero counts; they're not interesting enough */
    if nfiltered > 0.0 || (*es).format != EXPLAIN_FORMAT_TEXT {
        if nloops > 0.0 {
            ExplainPropertyFloat(qlabel, null(), nfiltered / nloops, 0, es);
        } else {
            ExplainPropertyFloat(qlabel, null(), 0.0, 0, es);
        }
    }
}

/*
 * Show extra information for a ForeignScan node.
 */
unsafe fn show_foreignscan_info(fsstate: *mut ForeignScanState, es: *mut ExplainState) {
    let fdwroutine = (*fsstate).fdwroutine;

    /* Let the FDW emit whatever fields it wants */
    if (*((*fsstate).ss.ps.plan as *mut ForeignScan)).operation != CMD_SELECT {
        if let Some(f) = (*fdwroutine).ExplainDirectModify {
            f(fsstate, es as *mut c_void);
        }
    } else {
        if let Some(f) = (*fdwroutine).ExplainForeignScan {
            f(fsstate, es as *mut c_void);
        }
    }
}

// ---------------------------------------------------------------------------
// explain_get_index_name
// ---------------------------------------------------------------------------

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 */
unsafe fn explain_get_index_name(indexId: Oid) -> *const c_char {
    let result: *const c_char;

    if let Some(hook) = explain_get_index_name_hook {
        result = hook(indexId);
    } else {
        result = null();
    }
    if result.is_null() {
        /* default behavior: look it up in the catalogs */
        let r = get_rel_name(indexId);
        if r.is_null() {
            ereport!(ERROR, errmsg!("cache lookup failed for index {}", indexId));
        }
        return r;
    }
    result
}

// ---------------------------------------------------------------------------
// Buffer / WAL / memory usage display
// ---------------------------------------------------------------------------

/*
 * Return whether show_buffer_usage would have anything to print, if given
 * the same 'usage' data.  Note that when the format is anything other than
 * text, we print even if the counters are all zeroes.
 */
unsafe fn peek_buffer_usage(es: *mut ExplainState, usage: *const BufferUsage) -> bool {
    if usage.is_null() {
        return false;
    }

    if (*es).format != EXPLAIN_FORMAT_TEXT {
        return true;
    }

    let has_shared = (*usage).shared_blks_hit > 0
        || (*usage).shared_blks_read > 0
        || (*usage).shared_blks_dirtied > 0
        || (*usage).shared_blks_written > 0;
    let has_local = (*usage).local_blks_hit > 0
        || (*usage).local_blks_read > 0
        || (*usage).local_blks_dirtied > 0
        || (*usage).local_blks_written > 0;
    let has_temp = (*usage).temp_blks_read > 0 || (*usage).temp_blks_written > 0;
    let has_shared_timing = !INSTR_TIME_IS_ZERO((*usage).shared_blk_read_time)
        || !INSTR_TIME_IS_ZERO((*usage).shared_blk_write_time);
    let has_local_timing = !INSTR_TIME_IS_ZERO((*usage).local_blk_read_time)
        || !INSTR_TIME_IS_ZERO((*usage).local_blk_write_time);
    let has_temp_timing = !INSTR_TIME_IS_ZERO((*usage).temp_blk_read_time)
        || !INSTR_TIME_IS_ZERO((*usage).temp_blk_write_time);

    has_shared || has_local || has_temp || has_shared_timing || has_local_timing || has_temp_timing
}

/*
 * Show buffer usage details.  This better be sync with peek_buffer_usage.
 */
unsafe fn show_buffer_usage(es: *mut ExplainState, usage: *const BufferUsage) {
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        let has_shared = (*usage).shared_blks_hit > 0
            || (*usage).shared_blks_read > 0
            || (*usage).shared_blks_dirtied > 0
            || (*usage).shared_blks_written > 0;
        let has_local = (*usage).local_blks_hit > 0
            || (*usage).local_blks_read > 0
            || (*usage).local_blks_dirtied > 0
            || (*usage).local_blks_written > 0;
        let has_temp = (*usage).temp_blks_read > 0 || (*usage).temp_blks_written > 0;
        let has_shared_timing = !INSTR_TIME_IS_ZERO((*usage).shared_blk_read_time)
            || !INSTR_TIME_IS_ZERO((*usage).shared_blk_write_time);
        let has_local_timing = !INSTR_TIME_IS_ZERO((*usage).local_blk_read_time)
            || !INSTR_TIME_IS_ZERO((*usage).local_blk_write_time);
        let has_temp_timing = !INSTR_TIME_IS_ZERO((*usage).temp_blk_read_time)
            || !INSTR_TIME_IS_ZERO((*usage).temp_blk_write_time);

        /* Show only positive counter values. */
        if has_shared || has_local || has_temp {
            ExplainIndentText(es);
            appendStringInfoString((*es).str, c"Buffers:".as_ptr());

            if has_shared {
                appendStringInfoString((*es).str, c" shared".as_ptr());
                if (*usage).shared_blks_hit > 0 {
                    appendStringInfo!((*es).str, " hit={}", (*usage).shared_blks_hit);
                }
                if (*usage).shared_blks_read > 0 {
                    appendStringInfo!((*es).str, " read={}", (*usage).shared_blks_read);
                }
                if (*usage).shared_blks_dirtied > 0 {
                    appendStringInfo!((*es).str, " dirtied={}", (*usage).shared_blks_dirtied);
                }
                if (*usage).shared_blks_written > 0 {
                    appendStringInfo!((*es).str, " written={}", (*usage).shared_blks_written);
                }
                if has_local || has_temp {
                    appendStringInfoChar((*es).str, b',' as c_char);
                }
            }
            if has_local {
                appendStringInfoString((*es).str, c" local".as_ptr());
                if (*usage).local_blks_hit > 0 {
                    appendStringInfo!((*es).str, " hit={}", (*usage).local_blks_hit);
                }
                if (*usage).local_blks_read > 0 {
                    appendStringInfo!((*es).str, " read={}", (*usage).local_blks_read);
                }
                if (*usage).local_blks_dirtied > 0 {
                    appendStringInfo!((*es).str, " dirtied={}", (*usage).local_blks_dirtied);
                }
                if (*usage).local_blks_written > 0 {
                    appendStringInfo!((*es).str, " written={}", (*usage).local_blks_written);
                }
                if has_temp {
                    appendStringInfoChar((*es).str, b',' as c_char);
                }
            }
            if has_temp {
                appendStringInfoString((*es).str, c" temp".as_ptr());
                if (*usage).temp_blks_read > 0 {
                    appendStringInfo!((*es).str, " read={}", (*usage).temp_blks_read);
                }
                if (*usage).temp_blks_written > 0 {
                    appendStringInfo!((*es).str, " written={}", (*usage).temp_blks_written);
                }
            }
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }

        /* As above, show only positive counter values. */
        if has_shared_timing || has_local_timing || has_temp_timing {
            ExplainIndentText(es);
            appendStringInfoString((*es).str, c"I/O Timings:".as_ptr());

            if has_shared_timing {
                appendStringInfoString((*es).str, c" shared".as_ptr());
                if !INSTR_TIME_IS_ZERO((*usage).shared_blk_read_time) {
                    appendStringInfo!((*es).str, " read={:.3}", INSTR_TIME_GET_MILLISEC((*usage).shared_blk_read_time));
                }
                if !INSTR_TIME_IS_ZERO((*usage).shared_blk_write_time) {
                    appendStringInfo!((*es).str, " write={:.3}", INSTR_TIME_GET_MILLISEC((*usage).shared_blk_write_time));
                }
                if has_local_timing || has_temp_timing {
                    appendStringInfoChar((*es).str, b',' as c_char);
                }
            }
            if has_local_timing {
                appendStringInfoString((*es).str, c" local".as_ptr());
                if !INSTR_TIME_IS_ZERO((*usage).local_blk_read_time) {
                    appendStringInfo!((*es).str, " read={:.3}", INSTR_TIME_GET_MILLISEC((*usage).local_blk_read_time));
                }
                if !INSTR_TIME_IS_ZERO((*usage).local_blk_write_time) {
                    appendStringInfo!((*es).str, " write={:.3}", INSTR_TIME_GET_MILLISEC((*usage).local_blk_write_time));
                }
                if has_temp_timing {
                    appendStringInfoChar((*es).str, b',' as c_char);
                }
            }
            if has_temp_timing {
                appendStringInfoString((*es).str, c" temp".as_ptr());
                if !INSTR_TIME_IS_ZERO((*usage).temp_blk_read_time) {
                    appendStringInfo!((*es).str, " read={:.3}", INSTR_TIME_GET_MILLISEC((*usage).temp_blk_read_time));
                }
                if !INSTR_TIME_IS_ZERO((*usage).temp_blk_write_time) {
                    appendStringInfo!((*es).str, " write={:.3}", INSTR_TIME_GET_MILLISEC((*usage).temp_blk_write_time));
                }
            }
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }
    } else {
        ExplainPropertyInteger(c"Shared Hit Blocks".as_ptr(), null(), (*usage).shared_blks_hit, es);
        ExplainPropertyInteger(c"Shared Read Blocks".as_ptr(), null(), (*usage).shared_blks_read, es);
        ExplainPropertyInteger(c"Shared Dirtied Blocks".as_ptr(), null(), (*usage).shared_blks_dirtied, es);
        ExplainPropertyInteger(c"Shared Written Blocks".as_ptr(), null(), (*usage).shared_blks_written, es);
        ExplainPropertyInteger(c"Local Hit Blocks".as_ptr(), null(), (*usage).local_blks_hit, es);
        ExplainPropertyInteger(c"Local Read Blocks".as_ptr(), null(), (*usage).local_blks_read, es);
        ExplainPropertyInteger(c"Local Dirtied Blocks".as_ptr(), null(), (*usage).local_blks_dirtied, es);
        ExplainPropertyInteger(c"Local Written Blocks".as_ptr(), null(), (*usage).local_blks_written, es);
        ExplainPropertyInteger(c"Temp Read Blocks".as_ptr(), null(), (*usage).temp_blks_read, es);
        ExplainPropertyInteger(c"Temp Written Blocks".as_ptr(), null(), (*usage).temp_blks_written, es);
        if track_io_timing {
            ExplainPropertyFloat(c"Shared I/O Read Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).shared_blk_read_time), 3, es);
            ExplainPropertyFloat(c"Shared I/O Write Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).shared_blk_write_time), 3, es);
            ExplainPropertyFloat(c"Local I/O Read Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).local_blk_read_time), 3, es);
            ExplainPropertyFloat(c"Local I/O Write Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).local_blk_write_time), 3, es);
            ExplainPropertyFloat(c"Temp I/O Read Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).temp_blk_read_time), 3, es);
            ExplainPropertyFloat(c"Temp I/O Write Time".as_ptr(), c"ms".as_ptr(),
                INSTR_TIME_GET_MILLISEC((*usage).temp_blk_write_time), 3, es);
        }
    }
}

/*
 * Show WAL usage details.
 */
unsafe fn show_wal_usage(es: *mut ExplainState, usage: *const WalUsage) {
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        /* Show only positive counter values. */
        if (*usage).wal_records > 0
            || (*usage).wal_fpi > 0
            || (*usage).wal_bytes > 0
            || (*usage).wal_buffers_full > 0
        {
            ExplainIndentText(es);
            appendStringInfoString((*es).str, c"WAL:".as_ptr());

            if (*usage).wal_records > 0 {
                appendStringInfo!((*es).str, " records={}", (*usage).wal_records);
            }
            if (*usage).wal_fpi > 0 {
                appendStringInfo!((*es).str, " fpi={}", (*usage).wal_fpi);
            }
            if (*usage).wal_bytes > 0 {
                appendStringInfo!((*es).str, " bytes={}", (*usage).wal_bytes);
            }
            if (*usage).wal_buffers_full > 0 {
                appendStringInfo!((*es).str, " buffers full={}", (*usage).wal_buffers_full);
            }
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }
    } else {
        ExplainPropertyInteger(c"WAL Records".as_ptr(), null(), (*usage).wal_records, es);
        ExplainPropertyInteger(c"WAL FPI".as_ptr(), null(), (*usage).wal_fpi, es);
        ExplainPropertyUInteger(c"WAL Bytes".as_ptr(), null(), (*usage).wal_bytes, es);
        ExplainPropertyInteger(c"WAL Buffers Full".as_ptr(), null(), (*usage).wal_buffers_full, es);
    }
}

/*
 * Show memory usage details.
 */
unsafe fn show_memory_counters(
    es: *mut ExplainState,
    mem_counters: *const MemoryContextCounters,
) {
    let memUsedkB: int64 =
        BYTES_TO_KILOBYTES!(((*mem_counters).totalspace - (*mem_counters).freespace) as int64);
    let memAllocatedkB: int64 =
        BYTES_TO_KILOBYTES!((*mem_counters).totalspace as int64);

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        appendStringInfo!(
            (*es).str,
            "Memory: used={}kB  allocated={}kB",
            memUsedkB,
            memAllocatedkB
        );
        appendStringInfoChar((*es).str, b'\n' as c_char);
    } else {
        ExplainPropertyInteger(c"Memory Used".as_ptr(), c"kB".as_ptr(), memUsedkB, es);
        ExplainPropertyInteger(c"Memory Allocated".as_ptr(), c"kB".as_ptr(), memAllocatedkB, es);
    }
}

// ---------------------------------------------------------------------------
// ExplainIndexScanDetails, ExplainScanTarget, ExplainModifyTarget,
// ExplainTargetRel, show_modifytable_info, ExplainMemberNodes,
// ExplainMissingMembers, ExplainSubPlans, ExplainCustomChildren,
// ExplainCreateWorkersState, ExplainOpenWorker, ExplainCloseWorker,
// ExplainFlushWorkersState
// ---------------------------------------------------------------------------

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
unsafe fn ExplainIndexScanDetails(
    indexid: Oid,
    indexorderdir: ScanDirection,
    es: *mut ExplainState,
) {
    let indexname = explain_get_index_name(indexid);

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        if indexorderdir == BackwardScanDirection {
            appendStringInfoString((*es).str, c" Backward".as_ptr());
        }
        appendStringInfo!((*es).str, " using {}", cstr_s(quote_identifier(indexname)));
    } else {
        let scandir: *const c_char = if indexorderdir == BackwardScanDirection {
            c"Backward".as_ptr()
        } else if indexorderdir == ForwardScanDirection {
            c"Forward".as_ptr()
        } else {
            c"???".as_ptr()
        };
        ExplainPropertyText(c"Scan Direction".as_ptr(), scandir, es);
        ExplainPropertyText(c"Index Name".as_ptr(), indexname, es);
    }
}

/*
 * Show the target of a Scan node
 */
unsafe fn ExplainScanTarget(plan: *mut Scan, es: *mut ExplainState) {
    ExplainTargetRel(plan as *mut Plan, (*plan).scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).
 */
unsafe fn ExplainModifyTarget(plan: *mut ModifyTable, es: *mut ExplainState) {
    ExplainTargetRel(plan as *mut Plan, (*plan).nominalRelation, es);
}

/*
 * Show the target relation of a scan or modify node
 */
unsafe fn ExplainTargetRel(plan: *mut Plan, rti: Index, es: *mut ExplainState) {
    let mut objectname: *mut c_char = null_mut();
    let mut namespace: *mut c_char = null_mut();
    let mut objecttag: *const c_char = null();
    let rte: *mut RangeTblEntry;
    let refname: *mut c_char;

    rte = rt_fetch(rti, (*es).rtable);
    refname = list_nth((*es).rtable_names, (rti as c_int) - 1) as *mut c_char;
    if refname.is_null() {
        // TODO(pg-port): rte->eref->aliasname -- RangeTblEntry.eref not yet accessible
    }

    match nodeTag(plan) {
        NodeTag::T_SeqScan
        | NodeTag::T_SampleScan
        | NodeTag::T_IndexScan
        | NodeTag::T_IndexOnlyScan
        | NodeTag::T_BitmapHeapScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_ForeignScan
        | NodeTag::T_CustomScan
        | NodeTag::T_ModifyTable => {
            /* Assert it's on a real relation */
            // Assert(rte->rtekind == RTE_RELATION);
            objectname = get_rel_name((*rte).relid);
            if (*es).verbose {
                namespace = get_namespace_name_or_temp(get_rel_namespace((*rte).relid));
            }
            objecttag = c"Relation Name".as_ptr();
        }
        NodeTag::T_FunctionScan => {
            let fscan = plan as *mut FunctionScan;

            /* Assert it's on a RangeFunction */
            // Assert(rte->rtekind == RTE_FUNCTION);

            /*
             * If the expression is still a function call of a single
             * function, we can get the real name of the function.
             */
            if list_length((*fscan).functions) == 1 {
                // TODO(pg-port): RangeTblFunction.funcexpr / FuncExpr.funcid not yet accessible
                objectname = get_func_name(0);
            }
            objecttag = c"Function Name".as_ptr();
        }
        NodeTag::T_TableFuncScan => {
            let tablefunc = (*(plan as *mut TableFuncScan)).tablefunc as *mut TableFunc;

            // TODO(pg-port): TableFunc.functype not yet accessible
            objectname = c"xmltable".as_ptr() as *mut c_char; // placeholder
            objecttag = c"Table Function Name".as_ptr();
        }
        NodeTag::T_ValuesScan => {
            // Assert(rte->rtekind == RTE_VALUES);
        }
        NodeTag::T_CteScan => {
            /* Assert it's on a non-self-reference CTE */
            // Assert(rte->rtekind == RTE_CTE);
            // TODO(pg-port): rte->ctename not yet accessible
            objecttag = c"CTE Name".as_ptr();
        }
        NodeTag::T_NamedTuplestoreScan => {
            // Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
            // TODO(pg-port): rte->enrname not yet accessible
            objecttag = c"Tuplestore Name".as_ptr();
        }
        NodeTag::T_WorkTableScan => {
            /* Assert it's on a self-reference CTE */
            // Assert(rte->rtekind == RTE_CTE);
            // TODO(pg-port): rte->ctename not yet accessible
            objecttag = c"CTE Name".as_ptr();
        }
        _ => {}
    }

    if (*es).format == EXPLAIN_FORMAT_TEXT {
        appendStringInfoString((*es).str, c" on".as_ptr());
        if !namespace.is_null() {
            appendStringInfo!(
                (*es).str,
                " {}.{}",
                cstr_s(quote_identifier(namespace)),
                cstr_s(quote_identifier(objectname))
            );
        } else if !objectname.is_null() {
            appendStringInfo!((*es).str, " {}", cstr_s(quote_identifier(objectname)));
        }
        if objectname.is_null()
            || (refname.is_null() || strcmp(refname, objectname) != 0)
        {
            if !refname.is_null() {
                appendStringInfo!((*es).str, " {}", cstr_s(quote_identifier(refname)));
            }
        }
    } else {
        if !objecttag.is_null() && !objectname.is_null() {
            ExplainPropertyText(objecttag, objectname, es);
        }
        if !namespace.is_null() {
            ExplainPropertyText(c"Schema".as_ptr(), namespace, es);
        }
        if !refname.is_null() {
            ExplainPropertyText(c"Alias".as_ptr(), refname, es);
        }
    }
}

/*
 * Show extra information for a ModifyTable node
 */
unsafe fn show_modifytable_info(
    mtstate: *mut ModifyTableState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let node = (*mtstate).ps.plan as *mut ModifyTable;
    let operation: *const c_char;
    let foperation: *const c_char;
    let labeltargets: bool;
    let mut idxNames: *mut List = null_mut();

    let (op, fop): (*const c_char, *const c_char) = if (*node).operation == CMD_INSERT {
        (c"Insert".as_ptr(), c"Foreign Insert".as_ptr())
    } else if (*node).operation == CMD_UPDATE {
        (c"Update".as_ptr(), c"Foreign Update".as_ptr())
    } else if (*node).operation == CMD_DELETE {
        (c"Delete".as_ptr(), c"Foreign Delete".as_ptr())
    } else if (*node).operation == CMD_MERGE {
        (c"Merge".as_ptr(), c"Foreign Merge".as_ptr())
    } else {
        (c"???".as_ptr(), c"Foreign ???".as_ptr())
    };
    operation = op;
    foperation = fop;

    /*
     * Should we explicitly label target relations?
     *
     * If there's only one target relation, do not list it if it's the
     * relation named in the query, or if it has been pruned.
     */
    labeltargets = (*mtstate).mt_nrels > 1
        || ((*mtstate).mt_nrels == 1
            && (*(*mtstate).resultRelInfo).ri_RangeTableIndex != (*node).nominalRelation
            && bms_is_member(
                (*(*mtstate).resultRelInfo).ri_RangeTableIndex as c_int,
                (*(*mtstate).ps.state).es_unpruned_relids,
            ));

    if labeltargets {
        ExplainOpenGroup(c"Target Tables".as_ptr(), c"Target Tables".as_ptr(), false, es);
    }

    for j in 0..(*mtstate).mt_nrels {
        let resultRelInfo = (*mtstate).resultRelInfo.add(j as usize);
        let fdwroutine = (*resultRelInfo).ri_FdwRoutine;

        if labeltargets {
            /* Open a group for this target */
            ExplainOpenGroup(c"Target Table".as_ptr(), null(), true, es);

            /*
             * In text mode, decorate each target with operation type, so that
             * ExplainTargetRel's output of " on foo" will read nicely.
             */
            if (*es).format == EXPLAIN_FORMAT_TEXT {
                ExplainIndentText(es);
                appendStringInfoString(
                    (*es).str,
                    if fdwroutine.is_null() { operation } else { foperation },
                );
            }

            /* Identify target */
            ExplainTargetRel(
                node as *mut Plan,
                (*resultRelInfo).ri_RangeTableIndex,
                es,
            );

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                appendStringInfoChar((*es).str, b'\n' as c_char);
                (*es).indent += 1;
            }
        }

        /* Give FDW a chance if needed */
        if !(*resultRelInfo).ri_usesFdwDirectModify
            && !fdwroutine.is_null()
        {
            if let Some(f) = (*fdwroutine).ExplainForeignModify {
                let fdw_private = list_nth((*node).fdwPrivLists, j) as *mut List;
                f(mtstate, resultRelInfo, fdw_private, j, es as *mut c_void);
            }
        }

        if labeltargets {
            /* Undo the indentation we added in text format */
            if (*es).format == EXPLAIN_FORMAT_TEXT {
                (*es).indent -= 1;
            }

            /* Close the group */
            ExplainCloseGroup(c"Target Table".as_ptr(), null(), true, es);
        }
    }

    /* Gather names of ON CONFLICT arbiter indexes */
    foreach!(lst, (*node).arbiterIndexes, {
        let indexname = get_rel_name(lfirst_oid(current_cell!(lst)));
        idxNames = lappend(idxNames, indexname as *mut c_void);
    });

    if (*node).onConflictAction != ONCONFLICT_NONE {
        ExplainPropertyText(
            c"Conflict Resolution".as_ptr(),
            if (*node).onConflictAction == ONCONFLICT_NOTHING {
                c"NOTHING".as_ptr()
            } else {
                c"UPDATE".as_ptr()
            },
            es,
        );

        /*
         * Don't display arbiter indexes at all when DO NOTHING variant
         * implicitly ignores all conflicts
         */
        if !idxNames.is_null() {
            ExplainPropertyList(c"Conflict Arbiter Indexes".as_ptr(), idxNames, es);
        }

        /* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
        if !(*node).onConflictWhere.is_null() {
            show_upper_qual(
                (*node).onConflictWhere as *mut List,
                c"Conflict Filter".as_ptr(),
                &mut (*mtstate).ps,
                ancestors,
                es,
            );
            show_instrumentation_count(c"Rows Removed by Conflict Filter".as_ptr(), 1, &mut (*mtstate).ps, es);
        }

        /* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
        if (*es).analyze && !(*mtstate).ps.instrument.is_null() {
            let total: f64;
            let insert_path: f64;
            let other_path: f64;

            InstrEndLoop((*outerPlanState(&mut (*mtstate).ps)).instrument);

            /* count the number of source rows */
            total = (*(*outerPlanState(&mut (*mtstate).ps)).instrument).ntuples;
            other_path = (*(*mtstate).ps.instrument).ntuples2;
            insert_path = total - other_path;

            ExplainPropertyFloat(c"Tuples Inserted".as_ptr(), null(), insert_path, 0, es);
            ExplainPropertyFloat(c"Conflicting Tuples".as_ptr(), null(), other_path, 0, es);
        }
    } else if (*node).operation == CMD_MERGE {
        /* EXPLAIN ANALYZE display of tuples processed */
        if (*es).analyze && !(*mtstate).ps.instrument.is_null() {
            let total: f64;
            let insert_path: f64;
            let update_path: f64;
            let delete_path: f64;
            let skipped_path: f64;

            InstrEndLoop((*outerPlanState(&mut (*mtstate).ps)).instrument);

            /* count the number of source rows */
            total = (*(*outerPlanState(&mut (*mtstate).ps)).instrument).ntuples;
            insert_path = (*mtstate).mt_merge_inserted as f64;
            update_path = (*mtstate).mt_merge_updated as f64;
            delete_path = (*mtstate).mt_merge_deleted as f64;
            skipped_path = total - insert_path - update_path - delete_path;
            // Assert(skipped_path >= 0);

            if (*es).format == EXPLAIN_FORMAT_TEXT {
                if total > 0.0 {
                    ExplainIndentText(es);
                    appendStringInfoString((*es).str, c"Tuples:".as_ptr());
                    if insert_path > 0.0 {
                        appendStringInfo!((*es).str, " inserted={:.0}", insert_path);
                    }
                    if update_path > 0.0 {
                        appendStringInfo!((*es).str, " updated={:.0}", update_path);
                    }
                    if delete_path > 0.0 {
                        appendStringInfo!((*es).str, " deleted={:.0}", delete_path);
                    }
                    if skipped_path > 0.0 {
                        appendStringInfo!((*es).str, " skipped={:.0}", skipped_path);
                    }
                    appendStringInfoChar((*es).str, b'\n' as c_char);
                }
            } else {
                ExplainPropertyFloat(c"Tuples Inserted".as_ptr(), null(), insert_path, 0, es);
                ExplainPropertyFloat(c"Tuples Updated".as_ptr(), null(), update_path, 0, es);
                ExplainPropertyFloat(c"Tuples Deleted".as_ptr(), null(), delete_path, 0, es);
                ExplainPropertyFloat(c"Tuples Skipped".as_ptr(), null(), skipped_path, 0, es);
            }
        }
    }

    if labeltargets {
        ExplainCloseGroup(c"Target Tables".as_ptr(), c"Target Tables".as_ptr(), false, es);
    }
}

/*
 * Explain the constituent plans of an Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 */
unsafe fn ExplainMemberNodes(
    planstates: *mut *mut PlanState,
    nplans: c_int,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    for j in 0..nplans {
        ExplainNode(*planstates.add(j as usize), ancestors, c"Member".as_ptr(), null(), es);
    }
}

/*
 * Report about any pruned subnodes of an Append or MergeAppend node.
 *
 * nplans indicates the number of live subplans.
 * nchildren indicates the original number of subnodes in the Plan;
 * some of these may have been pruned by the run-time pruning code.
 */
unsafe fn ExplainMissingMembers(nplans: c_int, nchildren: c_int, es: *mut ExplainState) {
    if nplans < nchildren || (*es).format != EXPLAIN_FORMAT_TEXT {
        ExplainPropertyInteger(
            c"Subplans Removed".as_ptr(),
            null(),
            (nchildren - nplans) as int64,
            es,
        );
    }
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlans.
 */
unsafe fn ExplainSubPlans(
    plans: *mut List,
    mut ancestors: *mut List,
    relationship: *const c_char,
    es: *mut ExplainState,
) {
    foreach!(lst, plans, {
        let sps = lfirst(current_cell!(lst)) as *mut SubPlanState;
        let sp = (*sps).subplan as *mut SubPlan;

        /*
         * There can be multiple SubPlan nodes referencing the same physical
         * subplan (same plan_id, which is its index in PlannedStmt.subplans).
         * We should print a subplan only once, so track which ones we already
         * printed.
         */
        // TODO(pg-port): SubPlan.plan_id not yet accessible
        // if bms_is_member((*sp).plan_id, (*es).printed_subplans) { continue; }
        // (*es).printed_subplans = bms_add_member((*es).printed_subplans, (*sp).plan_id);

        /*
         * Treat the SubPlan node as an ancestor of the plan node(s) within
         * it, so that ruleutils.c can find the referents of subplan
         * parameters.
         */
        ancestors = lcons(sp as *mut c_void, ancestors);

        ExplainNode((*sps).planstate, ancestors, relationship, null(), es);

        ancestors = list_delete_first(ancestors);
    });
}

/*
 * Explain a list of children of a CustomScan.
 */
unsafe fn ExplainCustomChildren(
    css: *mut CustomScanState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    let label: *const c_char = if list_length((*css).custom_ps) != 1 {
        c"children".as_ptr()
    } else {
        c"child".as_ptr()
    };

    foreach!(cell, (*css).custom_ps, {
        ExplainNode(lfirst(current_cell!(cell)) as *mut PlanState, ancestors, label, null(), es);
    });
}

/*
 * Create a per-plan-node workspace for collecting per-worker data.
 */
unsafe fn ExplainCreateWorkersState(num_workers: c_int) -> *mut ExplainWorkersState {
    let wstate = palloc(core::mem::size_of::<ExplainWorkersState>()) as *mut ExplainWorkersState;
    (*wstate).num_workers = num_workers;
    (*wstate).worker_inited = palloc0(num_workers as usize * core::mem::size_of::<bool>()) as *mut bool;
    (*wstate).worker_str =
        palloc0(num_workers as usize * core::mem::size_of::<StringInfoData>()) as *mut StringInfoData;
    (*wstate).worker_state_save = palloc(num_workers as usize * core::mem::size_of::<c_int>()) as *mut c_int;
    wstate
}

/*
 * Begin or resume output into the set-aside group for worker N.
 */
unsafe fn ExplainOpenWorker(n: c_int, es: *mut ExplainState) {
    let wstate = (*es).workers_state;

    Assert!(!wstate.is_null());
    Assert!(n >= 0 && n < (*wstate).num_workers);

    /* Save prior output buffer pointer */
    (*wstate).prev_str = (*es).str;

    if !*(*wstate).worker_inited.add(n as usize) {
        /* First time through, so create the buffer for this worker */
        initStringInfo(&mut *(*wstate).worker_str.add(n as usize));
        (*es).str = (*wstate).worker_str.add(n as usize);

        /*
         * Push suitable initial formatting state for this worker's field
         * group.  We allow one extra logical nesting level, since this group
         * will eventually be wrapped in an outer "Workers" group.
         */
        ExplainOpenSetAsideGroup(c"Worker".as_ptr(), null(), true, 2, es);

        /*
         * In non-TEXT formats we always emit a "Worker Number" field, even if
         * there's no other data for this worker.
         */
        if (*es).format != EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger(c"Worker Number".as_ptr(), null(), n as int64, es);
        }

        *(*wstate).worker_inited.add(n as usize) = true;
    } else {
        /* Resuming output for a worker we've already emitted some data for */
        (*es).str = (*wstate).worker_str.add(n as usize);

        /* Restore formatting state saved by last ExplainCloseWorker() */
        ExplainRestoreGroup(es, 2, (*wstate).worker_state_save.add(n as usize));
    }

    /*
     * In TEXT format, prefix the first output line for this worker with
     * "Worker N:".  Then, any additional lines should be indented one more
     * stop than the "Worker N" line is.
     */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        if (*(*es).str).len == 0 {
            ExplainIndentText(es);
            appendStringInfo!((*es).str, "Worker {}:  ", n);
        }

        (*es).indent += 1;
    }
}

/*
 * End output for worker N --- must pair with previous ExplainOpenWorker call
 */
unsafe fn ExplainCloseWorker(n: c_int, es: *mut ExplainState) {
    let wstate = (*es).workers_state;

    Assert!(!wstate.is_null());
    Assert!(n >= 0 && n < (*wstate).num_workers);
    Assert!(*(*wstate).worker_inited.add(n as usize));

    /*
     * Save formatting state in case we do another ExplainOpenWorker(), then
     * pop the formatting stack.
     */
    ExplainSaveGroup(es, 2, (*wstate).worker_state_save.add(n as usize));

    /*
     * In TEXT format, if we didn't actually produce any output line(s) then
     * truncate off the partial line emitted by ExplainOpenWorker.
     */
    if (*es).format == EXPLAIN_FORMAT_TEXT {
        while (*(*es).str).len > 0
            && *(*(*es).str).data.add((*(*es).str).len as usize - 1) != b'\n' as c_char
        {
            (*(*es).str).len -= 1;
            *(*(*es).str).data.add((*(*es).str).len as usize) = 0;
        }

        (*es).indent -= 1;
    }

    /* Restore prior output buffer pointer */
    (*es).str = (*wstate).prev_str;
}

/*
 * Print per-worker info for current node, then free the ExplainWorkersState.
 */
unsafe fn ExplainFlushWorkersState(es: *mut ExplainState) {
    let wstate = (*es).workers_state;

    ExplainOpenGroup(c"Workers".as_ptr(), c"Workers".as_ptr(), false, es);
    for i in 0..(*wstate).num_workers {
        if *(*wstate).worker_inited.add(i as usize) {
            /* This must match previous ExplainOpenSetAsideGroup call */
            ExplainOpenGroup(c"Worker".as_ptr(), null(), true, es);
            appendStringInfoString((*es).str, (*(*wstate).worker_str.add(i as usize)).data);
            ExplainCloseGroup(c"Worker".as_ptr(), null(), true, es);

            pfree((*(*wstate).worker_str.add(i as usize)).data as *mut c_void);
        }
    }
    ExplainCloseGroup(c"Workers".as_ptr(), c"Workers".as_ptr(), false, es);

    pfree((*wstate).worker_inited as *mut c_void);
    pfree((*wstate).worker_str as *mut c_void);
    pfree((*wstate).worker_state_save as *mut c_void);
    pfree(wstate as *mut c_void);
}

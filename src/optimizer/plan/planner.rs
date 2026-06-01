//! planner.rs
//!   The query optimizer external interface.
//!
//! Translated 1:1 from postgres/src/backend/optimizer/plan/planner.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/planner.c

#![allow(unused_variables)]
#![allow(unreachable_code)]
#![allow(unreachable_patterns)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_assignments)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;
use crate::nodes::primnodes::{TableFunc, MergeAction, OnConflictExpr};
use crate::nodes::lockoptions::LockClauseStrength;
use crate::nodes::plannodes::{RowMarkType, ROW_MARK_REFERENCE, ROW_MARK_KEYSHARE, ROW_MARK_SHARE, ROW_MARK_NOKEYEXCLUSIVE, ROW_MARK_EXCLUSIVE, ROW_MARK_COPY};
use crate::optimizer::paths::{PathKeysComparison, PATHKEYS_EQUAL, PATHKEYS_BETTER1, PATHKEYS_BETTER2, PATHKEYS_DIFFERENT};
use crate::nodes::lockoptions::{LCS_NONE, LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE};
use crate::nodes::nodes::LIMIT_OPTION_COUNT;
use crate::nodes::lockoptions::LockWaitBlock;
use crate::nodes::pathnodes::RELOPT_OTHER_UPPER_REL;
// TODO(pg-port): real WindowFuncLists lives in optimizer/clauses.h
#[repr(C)] pub struct WindowFuncLists {
    pub numWindowFuncs: c_int,
    pub maxWinRef: crate::c::Index,
    pub windowFuncs: *mut *mut List,
}

use crate::nodes::nodes::LimitOption;

use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    foreach, forboth, current_cell, makeNode, IsA, castNode, lfirst_node,
    Assert, elog, list_make1, list_make1_int,
};

use std::ptr;
use std::ffi::{c_int, c_char, c_void};

use crate::postgres_ext::Oid;
use crate::nodes::nodes::{
    Node, NodeTag, nodeTag, Cost,
    CMD_SELECT, CMD_UPDATE, CMD_DELETE, CMD_MERGE, CMD_UTILITY,
    AggStrategy, AGG_PLAIN, AGG_SORTED, AGG_HASHED, AGG_MIXED,
    AggSplit, AGGSPLIT_SIMPLE, AGGSPLIT_INITIAL_SERIAL, AGGSPLIT_FINAL_DESERIAL,
    JoinType,
};
use crate::nodes::pg_list::{
    List, ListCell, NIL,
    lfirst, lfirst_int, lfirst_oid,
    list_length, list_head, list_nth, lnext, lcons,
    lappend, lappend_int, lappend_oid, list_concat, list_copy, list_copy_head,
    list_make1_impl, list_free, list_member, list_member_ptr, list_member_int,
    list_difference_int, list_delete_int, list_concat_unique, list_concat_unique_ptr,
    linitial, linitial_int,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RTEPermissionInfo, TableSampleClause,
    RTE_RELATION, RTE_SUBQUERY, RTE_JOIN, RTE_RESULT, RTE_GROUP,
    RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES,
    WithCheckOption, RowMarkClause, SortGroupClause, WindowClause,
};
use crate::nodes::primnodes::{
    Var, Const, Aggref, WindowFunc, TargetEntry, Expr,
    FromExpr, JoinExpr, RangeTblRef, RelabelType,
    WindowFuncRunCondition,
};
use crate::nodes::pathnodes::{
    PlannerInfo, PlannerGlobal, RelOptInfo, Path, PathTarget, PathKey,
    IndexOptInfo, IndexPath, AggInfo, AggClauseCosts, QualCost,
    RollupData, GroupingSetData, GroupByOrdering, JoinDomain,
    GroupPathExtraData, FinalPathExtraData,
    PlannerParamItem,
    UpperRelationKind,
    GROUPING_CAN_USE_SORT, GROUPING_CAN_USE_HASH, GROUPING_CAN_PARTIAL_AGG,
    PartitionwiseAggregateType,
    PARTITIONWISE_AGGREGATE_NONE, PARTITIONWISE_AGGREGATE_FULL,
    PARTITIONWISE_AGGREGATE_PARTIAL,
};
use crate::nodes::plannodes::{
    Plan, Gather, PlannedStmt, PlanRowMark,
};
use crate::nodes::bitmapset::{
    Bitmapset,
    bms_add_member, bms_del_member, bms_del_members, bms_is_member, bms_is_subset,
    bms_is_empty, bms_equal, bms_make_singleton, bms_next_member, bms_num_members,
    bms_overlap_list, bms_free, bms_difference, bms_membership,
    BMS_MULTIPLE,
};

// ---------------------------------------------------------------------------
//   UpperRelationKind constants (nodes/pathnodes.h)
// ---------------------------------------------------------------------------
use crate::nodes::pathnodes::UpperRelationKind::{
    UPPERREL_GROUP_AGG, UPPERREL_PARTIAL_GROUP_AGG, UPPERREL_WINDOW,
    UPPERREL_PARTIAL_DISTINCT, UPPERREL_DISTINCT, UPPERREL_ORDERED,
    UPPERREL_FINAL,
};

// ---------------------------------------------------------------------------
//   GUC parameters
// ---------------------------------------------------------------------------

pub const DEFAULT_CURSOR_TUPLE_FRACTION: f64 = 0.1;

pub static mut cursor_tuple_fraction: f64 = DEFAULT_CURSOR_TUPLE_FRACTION;
pub static mut debug_parallel_query: c_int = DEBUG_PARALLEL_OFF;
pub static mut parallel_leader_participation: bool = true;
pub static mut enable_distinct_reordering: bool = true;

/// Hook for plugins to get control in planner()
pub static mut planner_hook: planner_hook_type = None;

/// Hook for plugins to get control when grouping_planner() plans upper rels
pub static mut create_upper_paths_hook: create_upper_paths_hook_type = None;

pub type planner_hook_type = Option<
    unsafe fn(
        parse: *mut Query,
        query_string: *const c_char,
        cursorOptions: c_int,
        boundParams: ParamListInfo,
    ) -> *mut PlannedStmt,
>;

pub type create_upper_paths_hook_type = Option<
    unsafe fn(
        root: *mut PlannerInfo,
        stage: UpperRelationKind,
        input_rel: *mut RelOptInfo,
        output_rel: *mut RelOptInfo,
        extra: *mut c_void,
    ),
>;

/* Expression kind codes for preprocess_expression */
const EXPRKIND_QUAL: c_int = 0;
const EXPRKIND_TARGET: c_int = 1;
const EXPRKIND_RTFUNC: c_int = 2;
const EXPRKIND_RTFUNC_LATERAL: c_int = 3;
const EXPRKIND_VALUES: c_int = 4;
const EXPRKIND_VALUES_LATERAL: c_int = 5;
const EXPRKIND_LIMIT: c_int = 6;
const EXPRKIND_APPINFO: c_int = 7;
const EXPRKIND_PHV: c_int = 8;
const EXPRKIND_TABLESAMPLE: c_int = 9;
const EXPRKIND_ARBITER_ELEM: c_int = 10;
const EXPRKIND_TABLEFUNC: c_int = 11;
const EXPRKIND_TABLEFUNC_LATERAL: c_int = 12;
const EXPRKIND_GROUPEXPR: c_int = 13;

/// Data specific to grouping sets
#[repr(C)]
pub struct grouping_sets_data {
    pub rollups: *mut List,
    pub hash_sets_idx: *mut List,
    pub dNumHashGroups: f64,
    pub any_hashable: bool,
    pub unsortable_refs: *mut Bitmapset,
    pub unhashable_refs: *mut Bitmapset,
    pub unsortable_sets: *mut List,
    pub tleref_to_colnum_map: *mut c_int,
}

/// Temporary structure for use during WindowClause reordering in order to be
/// able to sort WindowClauses on partitioning/ordering prefix.
#[repr(C)]
pub struct WindowClauseSortData {
    pub wc: *mut WindowClause,
    /// A List of unique ordering/partitioning clauses per Window
    pub uniqueOrder: *mut List,
}

/// Passthrough data for standard_qp_callback
#[repr(C)]
pub struct standard_qp_extra {
    /// active windows, if any
    pub activeWindows: *mut List,
    /// grouping sets data, if any
    pub gset_data: *mut grouping_sets_data,
    /// parent set operation or NULL if not a subquery belonging to a set operation
    pub setop: *mut SetOperationStmt,
}

// ===========================================================================
// Stubs / aliases for not-yet-ported types and leaf callees.
// TODO(pg-port): replace with real translations as the relevant files land.
// ===========================================================================

pub type ParamListInfo = *mut c_void; // TODO(pg-port): real ParamListInfo in nodes/params.h
pub use crate::nodes::pathnodes::FdwRoutine; // unified with the real RelOptInfo.fdwroutine type
pub type Index = c_uint;
pub type BlockNumber = u32;
pub type Relation = *mut c_void; // TODO(pg-port): real Relation in utils/rel.h
pub type Size = usize;
pub type Datum = usize;
pub type int64 = i64;
pub type Cardinality = f64;

use std::ffi::c_uint;

// TODO(pg-port): real SetOperationStmt lives in nodes/parsenodes.rs (deferred).
#[repr(C)]
pub struct SetOperationStmt {
    pub r#type: NodeTag,
    pub groupClauses: *mut List,
    pub colTypes: *mut List,
}

// TODO(pg-port): real WindowFuncRunCondition fields elsewhere; using primnodes import above.

// --- cursor / parallel option flags (nodes/parsenodes.h) ---
const CURSOR_OPT_PARALLEL_OK: c_int = 0x0020;
const CURSOR_OPT_FAST_PLAN: c_int = 0x0010;
const CURSOR_OPT_SCROLL: c_int = 0x0002;

// --- debug_parallel_query values (optimizer/optimizer.h) ---
pub const DEBUG_PARALLEL_OFF: c_int = 0;
pub const DEBUG_PARALLEL_ON: c_int = 1;
pub const DEBUG_PARALLEL_REGRESS: c_int = 2;

// --- proparallel values (catalog/pg_proc.h) ---
const PROPARALLEL_UNSAFE: c_char = b'u' as c_char;
const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char;
const PROPARALLEL_SAFE: c_char = b's' as c_char;

// --- relkind values (catalog/pg_class.h) ---
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_VIEW: c_char = b'v' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;

// --- persistence values ---
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

// --- lock modes (storage/lockdefs.h) ---
const AccessShareLock: c_int = 1;
const NoLock: c_int = 0;

// --- object types for acl (nodes/parsenodes.h) ---
const OBJECT_VIEW: c_int = 0;
const ACLCHECK_NO_PRIV: c_int = 0;

// --- scan direction (access/sdir.h) ---
const ForwardScanDirection: c_int = 1;

// --- type Oids (catalog/pg_type_d.h) ---
const INT8OID: Oid = 20;
const BOOLOID: Oid = 16;
const BYTEAOID: Oid = 17;
const INTERNALOID: Oid = 2281;
const InvalidOid: Oid = 0;

// --- JIT flags (jit/jit.h) ---
const PGJIT_NONE: c_int = 0;
const PGJIT_PERFORM: c_int = 1 << 0;
const PGJIT_OPT3: c_int = 1 << 1;
const PGJIT_INLINE: c_int = 1 << 2;
const PGJIT_EXPR: c_int = 1 << 3;
const PGJIT_DEFORM: c_int = 1 << 4;

// --- pull_var_clause flags (optimizer/optimizer.h) ---
const PVC_RECURSE_AGGREGATES: c_int = 0x0002;
const PVC_INCLUDE_AGGREGATES: c_int = 0x0001;
const PVC_RECURSE_WINDOWFUNCS: c_int = 0x0008;
const PVC_INCLUDE_WINDOWFUNCS: c_int = 0x0004;
const PVC_INCLUDE_PLACEHOLDERS: c_int = 0x0020;

// --- error codes ---
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

// --- aggkind helpers (nodes/primnodes.h) ---
unsafe fn AGGKIND_IS_ORDERED_SET(aggkind: c_char) -> bool {
    aggkind != b'n' as c_char
}

unsafe fn DO_AGGSPLIT_SKIPFINAL(aggsplit: AggSplit) -> bool {
    // (aggsplit & AGGSPLITOP_SKIPFINAL) != 0
    todo_aggsplit_skipfinal(aggsplit)
}
unsafe fn DO_AGGSPLIT_SERIALIZE(aggsplit: AggSplit) -> bool {
    todo_aggsplit_serialize(aggsplit)
}
// TODO(pg-port): real AGGSPLIT bit-test macros live in nodes/nodes.rs.
unsafe fn todo_aggsplit_skipfinal(_aggsplit: AggSplit) -> bool { false }
unsafe fn todo_aggsplit_serialize(_aggsplit: AggSplit) -> bool { false }

const FLOAT8PASSBYVAL: bool = true;

// --- misc global flags (miscadmin.h / optimizer/cost.h) ---
extern "C" {
    pub static IsUnderPostmaster: bool;
}
pub static mut max_parallel_workers_per_gather: c_int = 2;
pub static mut max_parallel_maintenance_workers: c_int = 2;
pub static mut parallel_setup_cost: Cost = 1000.0;
pub static mut parallel_tuple_cost: Cost = 0.1;
pub static mut cpu_operator_cost: Cost = 0.0025;
pub static mut maintenance_work_mem: c_int = 65536;
pub static mut jit_enabled: bool = false;
pub static mut jit_above_cost: f64 = 100000.0;
pub static mut jit_optimize_above_cost: f64 = 500000.0;
pub static mut jit_inline_above_cost: f64 = 500000.0;
pub static mut jit_expressions: bool = true;
pub static mut jit_tuple_deforming: bool = true;
pub static mut enable_indexscan: bool = true;
pub static mut enable_hashagg: bool = true;
pub static mut enable_incremental_sort: bool = true;
pub static mut enable_partitionwise_aggregate: bool = false;
pub static mut enable_presorted_aggregate: bool = true;

// --- T_ tags for support nodes ---
const T_SupportRequestOptimizeWindowClause: NodeTag = NodeTag::T_Invalid;
const T_PlannerGlobal: NodeTag = NodeTag::T_PlannerGlobal;
const T_PlannerInfo: NodeTag = NodeTag::T_PlannerInfo;

// TODO(pg-port): real SupportRequestOptimizeWindowClause in nodes/supportnodes.h
#[repr(C)]
pub struct SupportRequestOptimizeWindowClause {
    pub r#type: NodeTag,
    pub window_clause: *mut WindowClause,
    pub window_func: *mut WindowFunc,
    pub frameOptions: c_int,
}

// ---- leaf-callee stubs (genuinely-unported; replace as files land) ----

unsafe fn IsParallelWorker() -> bool { false } // TODO(pg-port): access/parallel.h
unsafe fn max_parallel_hazard(_parse: *mut Query) -> c_char { PROPARALLEL_UNSAFE } // TODO(pg-port): optimizer/clauses.c
unsafe fn pgstat_report_plan_id(_id: u64, _force: bool) {} // TODO(pg-port): utils/backend_status.c

unsafe fn fetch_upper_rel(_root: *mut PlannerInfo, _kind: UpperRelationKind, _relids: *mut Bitmapset) -> *mut RelOptInfo { unimplemented!() } // TODO(pg-port): optimizer/util/relnode.c
unsafe fn get_cheapest_fractional_path_local(_rel: *mut RelOptInfo, _tf: f64) -> *mut Path { unimplemented!() }
unsafe fn create_plan(_root: *mut PlannerInfo, _best_path: *mut Path) -> *mut Plan { unimplemented!() } // TODO(pg-port): optimizer/plan/createplan.c
unsafe fn ExecSupportsBackwardScan(_plan: *mut Plan) -> bool { unimplemented!() } // TODO(pg-port): executor/execAmi.c
unsafe fn materialize_finished_plan(_plan: *mut Plan) -> *mut Plan { unimplemented!() }
unsafe fn SS_compute_initplan_cost(_initplan: *mut List, _cost: *mut Cost, _unsafe_initplans: *mut bool) {} // TODO(pg-port): optimizer/plan/subselect.c
unsafe fn SS_finalize_plan(_root: *mut PlannerInfo, _plan: *mut Plan) {}
unsafe fn SS_process_ctes(_root: *mut PlannerInfo) {}
unsafe fn SS_identify_outer_params(_root: *mut PlannerInfo) {}
unsafe fn SS_charge_for_initplans(_root: *mut PlannerInfo, _rel: *mut RelOptInfo) {}
unsafe fn SS_process_sublinks(_root: *mut PlannerInfo, expr: *mut Node, _isQual: bool) -> *mut Node { expr }
unsafe fn SS_replace_correlation_vars(_root: *mut PlannerInfo, expr: *mut Node) -> *mut Node { expr }
unsafe fn assign_special_exec_param(_root: *mut PlannerInfo) -> c_int { -1 }
unsafe fn set_plan_references(_root: *mut PlannerInfo, plan: *mut Plan) -> *mut Plan { plan } // TODO(pg-port): optimizer/plan/setrefs.rs
unsafe fn DestroyPartitionDirectory(_dir: *mut c_void) {} // TODO(pg-port): partitioning/partdesc.c

unsafe fn transform_MERGE_to_join(_parse: *mut Query) {} // TODO(pg-port): parser/parse_merge.c
unsafe fn replace_empty_jointree(_parse: *mut Query) {} // TODO(pg-port): optimizer/prep/prepjointree.c
unsafe fn pull_up_sublinks(_root: *mut PlannerInfo) {}
unsafe fn preprocess_function_rtes(_root: *mut PlannerInfo) {}
unsafe fn expand_virtual_generated_columns(root: *mut PlannerInfo) -> *mut Query { (*root).parse }
unsafe fn pull_up_subqueries(_root: *mut PlannerInfo) {}
unsafe fn flatten_simple_union_all(_root: *mut PlannerInfo) {}
unsafe fn has_subclass(_relid: Oid) -> bool { false } // TODO(pg-port): catalog/pg_inherits.c
unsafe fn getRTEPermissionInfo(_perminfos: *mut List, _rte: *mut RangeTblEntry) -> *mut RTEPermissionInfo { ptr::null_mut() }
unsafe fn ExecCheckOneRelPerms(_perminfo: *mut RTEPermissionInfo) -> bool { true }
unsafe fn aclcheck_error(_code: c_int, _objtype: c_int, _name: *const c_char) {}
unsafe fn get_rel_name(_relid: Oid) -> *const c_char { ptr::null() }
unsafe fn flatten_join_alias_vars(_root: *mut PlannerInfo, _query: *mut Query, expr: *mut Node) -> *mut Node { expr }
unsafe fn flatten_group_exprs(_root: *mut PlannerInfo, _query: *mut Query, expr: *mut Node) -> *mut Node { expr }
unsafe fn expression_returns_set(_node: *mut Node) -> bool { false }
unsafe fn expand_grouping_sets(gsets: *mut List, _gd: bool, _limit: c_int) -> *mut List { gsets }
unsafe fn contain_agg_clause(_node: *mut Node) -> bool { false }
unsafe fn contain_volatile_functions(_node: *mut Node) -> bool { false }
unsafe fn contain_subplans(_node: *mut Node) -> bool { false }
unsafe fn pull_varnos(_root: *mut PlannerInfo, _node: *mut Node) -> *mut Bitmapset { ptr::null_mut() }
unsafe fn reduce_outer_joins(_root: *mut PlannerInfo) {}
unsafe fn remove_useless_result_rtes(_root: *mut PlannerInfo) {}
unsafe fn eval_const_expressions(_root: *mut PlannerInfo, expr: *mut Node) -> *mut Node { expr }
unsafe fn canonicalize_qual(expr: *mut Expr, _is_check: bool) -> *mut Expr { expr }
unsafe fn convert_saop_to_hashed_saop(_node: *mut Node) {}
unsafe fn make_ands_implicit(expr: *mut Expr) -> *mut List { expr as *mut List }
unsafe fn fix_opfuncids(_node: *mut Node) {}
unsafe fn extract_query_dependencies_walker(_node: *mut Node, _root: *mut PlannerInfo) -> bool { false }

unsafe fn plan_set_operations(_root: *mut PlannerInfo) -> *mut RelOptInfo { unimplemented!() } // TODO(pg-port): optimizer/prep/prepunion.c
unsafe fn is_parallel_safe(_root: *mut PlannerInfo, _node: *mut Node) -> bool { false } // TODO(pg-port): optimizer/util/clauses.c
unsafe fn make_pathkeys_for_sortclauses(_root: *mut PlannerInfo, _sortclauses: *mut List, _tlist: *mut List) -> *mut List { NIL } // TODO(pg-port): optimizer/path/pathkeys.c
unsafe fn make_pathkeys_for_sortclauses_extended(
    _root: *mut PlannerInfo, _sortclauses: *mut *mut List, _tlist: *mut List,
    _remove_redundant: bool, _remove_group_rtindex: bool,
    sortable: *mut bool, _set_ec_sortref: bool,
) -> *mut List { if !sortable.is_null() { *sortable = true; } NIL }
unsafe fn query_planner(_root: *mut PlannerInfo, _qp_callback: query_pathkeys_callback, _qp_extra: *mut c_void) -> *mut RelOptInfo { unimplemented!() } // TODO(pg-port): optimizer/plan/planmain.rs
unsafe fn preprocess_targetlist(_root: *mut PlannerInfo) {} // TODO(pg-port): optimizer/prep/preptlist.c
unsafe fn preprocess_aggrefs(_root: *mut PlannerInfo, _clause: *mut Node) {}
unsafe fn preprocess_minmax_aggregates(_root: *mut PlannerInfo) {} // TODO(pg-port): optimizer/plan/planagg.rs
unsafe fn find_window_functions(_clause: *mut Node, _maxWinRef: c_int) -> *mut WindowFuncLists { unimplemented!() }
unsafe fn create_pathtarget(_root: *mut PlannerInfo, _tlist: *mut List) -> *mut PathTarget { unimplemented!() }
unsafe fn create_empty_pathtarget() -> *mut PathTarget { unimplemented!() }
unsafe fn copy_pathtarget(_t: *mut PathTarget) -> *mut PathTarget { unimplemented!() }
unsafe fn set_pathtarget_cost_width(_root: *mut PlannerInfo, t: *mut PathTarget) -> *mut PathTarget { t }
unsafe fn get_pathtarget_sortgroupref(_t: *mut PathTarget, _i: c_int) -> Index { 0 }
unsafe fn add_column_to_pathtarget(_t: *mut PathTarget, _expr: *mut Expr, _sgref: Index) {}
unsafe fn add_new_columns_to_pathtarget(_t: *mut PathTarget, _exprs: *mut List) {}
unsafe fn clamp_width_est(t: int64) -> c_int { t as c_int }
unsafe fn split_pathtarget_at_srfs(_root: *mut PlannerInfo, _t: *mut PathTarget, _input: *mut PathTarget, _targets: *mut *mut List, _contain: *mut *mut List) {}
unsafe fn split_pathtarget_at_srfs_grouping(_root: *mut PlannerInfo, _t: *mut PathTarget, _input: *mut PathTarget, _targets: *mut *mut List, _contain: *mut *mut List) {}

unsafe fn estimate_expression_value(_root: *mut PlannerInfo, node: *mut Node) -> *mut Node { node }
unsafe fn estimate_num_groups(_root: *mut PlannerInfo, _groupExprs: *mut List, _rows: f64, _pgset: *mut *mut List, _hentry: *mut c_void) -> f64 { 1.0 }
unsafe fn estimate_hashagg_tablesize(_root: *mut PlannerInfo, _path: *mut Path, _agg_costs: *const AggClauseCosts, _dNumGroups: f64) -> f64 { 0.0 }
unsafe fn get_hash_memory_limit() -> Size { 0 }
unsafe fn get_agg_clause_costs(_root: *mut PlannerInfo, _aggsplit: AggSplit, _costs: *mut AggClauseCosts) {}
unsafe fn get_sortgrouplist_exprs(_clause: *mut List, _tlist: *mut List) -> *mut List { NIL }
unsafe fn get_sortgroupref_clause(_ref: Index, _clauses: *mut List) -> *mut SortGroupClause { ptr::null_mut() }
unsafe fn get_sortgroupref_clause_noerr(_ref: Index, _clauses: *mut List) -> *mut SortGroupClause { ptr::null_mut() }
unsafe fn grouping_is_sortable(_clause: *mut List) -> bool { true }
unsafe fn grouping_is_hashable(_clause: *mut List) -> bool { true }
unsafe fn get_relids_in_jointree(_jtnode: *mut Node, _include_joins: bool, _include_inner_sides: bool) -> *mut Bitmapset { ptr::null_mut() }
unsafe fn CheckSelectLocking(_parse: *mut Query, _strength: LockClauseStrength) {}
unsafe fn GetFdwRoutineByRelId(_relid: Oid) -> *mut FdwRoutine { ptr::null_mut() }

unsafe fn pull_var_clause(_node: *mut Node, _flags: c_int) -> *mut List { NIL }
unsafe fn remove_nulling_relids(node: *mut Node, _removable: *mut Bitmapset, _except: *mut Bitmapset) -> *mut Node { node }

unsafe fn set_cheapest(_rel: *mut RelOptInfo) {}
unsafe fn add_path(_rel: *mut RelOptInfo, _path: *mut Path) {}
unsafe fn add_partial_path(_rel: *mut RelOptInfo, _path: *mut Path) {}
unsafe fn generate_useful_gather_paths(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _override_rows: bool) {}
unsafe fn compute_gather_rows(_path: *mut Path) -> f64 { 0.0 }

unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo { ptr::null_mut() }
unsafe fn adjust_inherited_attnums_multilevel(_root: *mut PlannerInfo, colnos: *mut List, _childrelid: c_int, _toprelid: c_int) -> *mut List { colnos }
unsafe fn adjust_appendrel_attrs_multilevel(_root: *mut PlannerInfo, node: *mut Node, _child: *mut RelOptInfo, _top: *mut RelOptInfo) -> *mut Node { node }
unsafe fn adjust_appendrel_attrs(_root: *mut PlannerInfo, node: *mut Node, _nappinfos: c_int, _appinfos: *mut *mut c_void) -> *mut Node { node }
unsafe fn find_appinfos_by_relids(_root: *mut PlannerInfo, _relids: *mut Bitmapset, nappinfos: *mut c_int) -> *mut *mut c_void { *nappinfos = 0; ptr::null_mut() }
unsafe fn add_paths_to_append_rel(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _live_children: *mut List) {}

// path constructors (optimizer/util/pathnode.c)
unsafe fn create_lockrows_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _rowMarks: *mut List, _epqParam: c_int) -> *mut Path { unimplemented!() }
unsafe fn create_limit_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _limitOffset: *mut Node, _limitCount: *mut Node, _limitOption: LimitOption, _offset_est: int64, _count_est: int64) -> *mut Path { unimplemented!() }
unsafe fn create_modifytable_path(
    _root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path,
    _operation: c_int, _canSetTag: bool, _nominalRelation: Index, _rootRelation: Index,
    _partColsUpdated: bool, _resultRelations: *mut List, _updateColnosLists: *mut List,
    _withCheckOptionLists: *mut List, _returningLists: *mut List, _rowMarks: *mut List,
    _onconflict: *mut OnConflictExpr, _mergeActionLists: *mut List, _mergeJoinConditions: *mut List,
    _epqParam: c_int,
) -> *mut Path { unimplemented!() }
unsafe fn create_group_result_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _target: *mut PathTarget, _havingqual: *mut List) -> *mut Path { unimplemented!() }
unsafe fn create_append_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpaths: *mut List, _partial_subpaths: *mut List, _pathkeys: *mut List, _required_outer: *mut Bitmapset, _parallel_workers: c_int, _parallel_aware: bool, _rows: f64) -> *mut Path { unimplemented!() }
unsafe fn create_groupingsets_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _having_qual: *mut List, _aggstrategy: AggStrategy, _rollups: *mut List, _agg_costs: *const AggClauseCosts) -> *mut Path { unimplemented!() }
unsafe fn create_sort_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _pathkeys: *mut List, _limit_tuples: f64) -> *mut Path { unimplemented!() }
unsafe fn create_incremental_sort_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _pathkeys: *mut List, _presorted_keys: c_int, _limit_tuples: f64) -> *mut Path { unimplemented!() }
unsafe fn create_windowagg_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _target: *mut PathTarget, _windowFuncs: *mut List, _runCondition: *mut List, _winclause: *mut WindowClause, _qual: *mut List, _topwindow: bool) -> *mut Path { unimplemented!() }
unsafe fn create_upper_unique_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _numCols: c_int, _numGroups: f64) -> *mut Path { unimplemented!() }
unsafe fn create_agg_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _target: *mut PathTarget, _aggstrategy: AggStrategy, _aggsplit: AggSplit, _groupClause: *mut List, _qual: *mut List, _aggcosts: *const AggClauseCosts, _numGroups: f64) -> *mut Path { unimplemented!() }
unsafe fn create_group_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _groupClause: *mut List, _qual: *mut List, _numGroups: f64) -> *mut Path { unimplemented!() }
unsafe fn create_gather_merge_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _target: *mut PathTarget, _pathkeys: *mut List, _required_outer: *mut Bitmapset, _rows: *mut f64) -> *mut Path { unimplemented!() }
unsafe fn create_set_projection_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _target: *mut PathTarget) -> *mut Path { unimplemented!() }
unsafe fn create_projection_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _subpath: *mut Path, _target: *mut PathTarget) -> *mut Path { unimplemented!() }
unsafe fn apply_projection_to_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, subpath: *mut Path, _target: *mut PathTarget) -> *mut Path { subpath }
unsafe fn create_seqscan_path(_root: *mut PlannerInfo, _rel: *mut RelOptInfo, _required_outer: *mut Bitmapset, _parallel_workers: c_int) -> *mut Path { unimplemented!() }
unsafe fn create_index_path(_root: *mut PlannerInfo, _index: *mut IndexOptInfo, _indexclauses: *mut List, _indexorderbys: *mut List, _indexorderbycols: *mut List, _pathkeys: *mut List, _indexscandir: c_int, _indexonly: bool, _required_outer: *mut Bitmapset, _loop_count: f64, _partial_path: bool) -> *mut IndexPath { unimplemented!() }

// pathkey comparison helpers (optimizer/path/pathkeys.c)
unsafe fn pathkeys_contained_in(_keys1: *mut List, _keys2: *mut List) -> bool { false }
unsafe fn pathkeys_count_contained_in(_keys1: *mut List, _keys2: *mut List, n_common: *mut c_int) -> bool { *n_common = 0; false }
unsafe fn compare_pathkeys(_keys1: *mut List, _keys2: *mut List) -> PathKeysComparison { PATHKEYS_DIFFERENT }
unsafe fn append_pathkeys(target: *mut List, _source: *mut List) -> *mut List { target }
unsafe fn get_useful_group_keys_orderings(_root: *mut PlannerInfo, _path: *mut Path) -> *mut List { NIL }
unsafe fn compare_fractional_path_costs(_path1: *mut Path, _path2: *mut Path, _fraction: f64) -> c_int { 0 }

// catalog/cost helpers
unsafe fn get_func_support(_funcid: Oid) -> Oid { InvalidOid }
unsafe fn get_typavgwidth(_typid: Oid, _typmod: i32) -> c_int { 0 }
unsafe fn cost_qual_eval(cost: *mut QualCost, _quals: *mut List, _root: *mut PlannerInfo) { /* fills out-param */ ptr::write_bytes(cost, 0, 1); }
unsafe fn cost_qual_eval_node(cost: *mut QualCost, _node: *mut Node, _root: *mut PlannerInfo) { ptr::write_bytes(cost, 0, 1); }
unsafe fn cost_sort(_path: *mut Path, _root: *mut PlannerInfo, _pathkeys: *mut List, _disabled_nodes: c_int, _input_cost: Cost, _tuples: f64, _width: c_int, _comparison_cost: Cost, _sort_mem: c_int, _limit_tuples: f64) {}
unsafe fn get_relation_data_width(_relid: Oid, _attr_widths: *mut i32) -> c_int { 0 }
unsafe fn estimate_rel_size(_rel: Relation, _attr_widths: *mut i32, _pages: *mut BlockNumber, _tuples: *mut f64, _allvisfrac: *mut f64) {}
unsafe fn compute_parallel_worker(_rel: *mut RelOptInfo, _heap_pages: f64, _index_pages: f64, _max_workers: c_int) -> c_int { 0 }
unsafe fn setup_simple_rel_arrays(_root: *mut PlannerInfo) {}
unsafe fn build_simple_rel(_root: *mut PlannerInfo, _relid: c_int, _parent: *mut RelOptInfo) -> *mut RelOptInfo { ptr::null_mut() }

// catalog open/close + relation helpers
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation { ptr::null_mut() }
unsafe fn table_close(_rel: Relation, _lockmode: c_int) {}
unsafe fn index_open(_relid: Oid, _lockmode: c_int) -> Relation { ptr::null_mut() }
unsafe fn index_close(_rel: Relation, _lockmode: c_int) {}
unsafe fn RelationGetIndexExpressions(_index: Relation) -> *mut List { NIL }
unsafe fn RelationGetIndexPredicate(_index: Relation) -> *mut List { NIL }
unsafe fn rt_fetch(rti: Index, rtable: *mut List) -> *mut RangeTblEntry {
    list_nth(rtable, (rti - 1) as c_int) as *mut RangeTblEntry
}
unsafe fn addRTEPermissionInfo(_rteperminfos: *mut *mut List, _rte: *mut RangeTblEntry) -> *mut RTEPermissionInfo { ptr::null_mut() }
unsafe fn list_cell_number(_list: *mut List, _cell: *mut ListCell) -> c_int { 0 }

// makefuncs / nodeFuncs
unsafe fn makeConst(_consttype: Oid, _consttypmod: i32, _constcollid: Oid, _constlen: c_int, _constvalue: Datum, _constisnull: bool, _constbyval: bool) -> *mut Const { unimplemented!() }
unsafe fn make_opclause(_opno: Oid, _opresulttype: Oid, _opretset: bool, _leftop: *mut Expr, _rightop: *mut Expr, _opcollid: Oid, _inputcollid: Oid) -> *mut Expr { unimplemented!() }
unsafe fn exprType(_node: *mut Node) -> Oid { InvalidOid }
unsafe fn exprCollation(_node: *mut Node) -> Oid { InvalidOid }
unsafe fn assignSortGroupRef(_tle: *mut TargetEntry, _tlist: *mut List) -> Index { 0 }
unsafe fn LCS_asString(_strength: LockClauseStrength) -> *const c_char { ptr::null() }

// knapsack / bipartite (lib/knapsack.c, lib/bipartite_match.c)
#[repr(C)]
pub struct BipartiteMatchState {
    pub pair_uv: *mut c_int,
    pub pair_vu: *mut c_int,
}
unsafe fn BipartiteMatch(_u: c_int, _v: c_int, _adjacency: *mut *mut i16) -> *mut BipartiteMatchState { unimplemented!() }
unsafe fn BipartiteMatchFree(_state: *mut BipartiteMatchState) {}
unsafe fn DiscreteKnapsack(_max_weight: c_int, _num_items: c_int, _item_weights: *mut c_int, _item_values: *mut f64) -> *mut Bitmapset { ptr::null_mut() }

// Datum helpers
unsafe fn DatumGetInt64(d: Datum) -> int64 { d as int64 }
unsafe fn Int64GetDatum(v: int64) -> Datum { v as Datum }
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void { d as *mut c_void }
unsafe fn PointerGetDatum<T>(p: *const T) -> Datum { p as Datum }
unsafe fn OidFunctionCall1(_funcid: Oid, _arg: Datum) -> Datum { 0 }
unsafe fn OidIsValid(o: Oid) -> bool { o != InvalidOid }

// list helpers not (yet) re-exported from pg_list
unsafe fn for_each_cell_iter(list: *mut List, start: *mut ListCell) -> Vec<*mut ListCell> {
    let mut v = Vec::new();
    let mut c = start;
    while !c.is_null() {
        v.push(c);
        c = lnext(list, c);
    }
    v
}

unsafe fn check_stack_depth() {}
unsafe fn IS_OTHER_REL(_rel: *mut RelOptInfo) -> bool { false }
unsafe fn IS_PARTITIONED_REL(_rel: *mut RelOptInfo) -> bool { false }
unsafe fn IS_DUMMY_REL(_rel: *mut RelOptInfo) -> bool { false }
unsafe fn IS_OUTER_JOIN(_jointype: JoinType) -> bool { false }

// snprintf / string helpers
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char { ptr::null_mut() }

// RELOPT kinds

/// query_pathkeys callback type: `void (*)(PlannerInfo *root, void *extra)`.
pub type query_pathkeys_callback =
    unsafe fn(root: *mut PlannerInfo, extra: *mut c_void);

// Min/Max helpers
#[inline]
fn Max_f64(a: f64, b: f64) -> f64 { if a > b { a } else { b } }
#[inline]
fn Min_f64(a: f64, b: f64) -> f64 { if a < b { a } else { b } }
#[inline]
fn Max_i64(a: i64, b: i64) -> i64 { if a > b { a } else { b } }
#[inline]
fn Min_i64(a: i64, b: i64) -> i64 { if a < b { a } else { b } }

// ***************************************************************************
//
//     Query optimizer entry point
//
// To support loadable plugins that monitor or modify planner behavior,
// we provide a hook variable that lets a plugin get control before and
// after the standard planning process.  The plugin would normally call
// standard_planner().
//
// Note to plugin authors: standard_planner() scribbles on its Query input,
// so you'd better copy that data structure if you want to plan more than once.
//
// ***************************************************************************
pub unsafe fn planner(
    parse: *mut Query,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    let result: *mut PlannedStmt;

    if let Some(hook) = planner_hook {
        result = hook(parse, query_string, cursorOptions, boundParams);
    } else {
        result = standard_planner(parse, query_string, cursorOptions, boundParams);
    }

    pgstat_report_plan_id((*result).planId as u64, false);

    result
}

pub unsafe fn standard_planner(
    parse: *mut Query,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    let result: *mut PlannedStmt;
    let glob: *mut PlannerGlobal;
    let mut tuple_fraction: f64;
    let root: *mut PlannerInfo;
    let final_rel: *mut RelOptInfo;
    let best_path: *mut Path;
    let mut top_plan: *mut Plan;
    let mut lp: *mut ListCell;
    let mut lr: *mut ListCell;

    /*
     * Set up global state for this planner invocation.  This data is needed
     * across all levels of sub-Query that might exist in the given command,
     * so we keep it in a separate struct that's linked to by each per-Query
     * PlannerInfo.
     */
    glob = makeNode!(PlannerGlobal, T_PlannerGlobal);

    (*glob).boundParams = boundParams;
    (*glob).subplans = NIL;
    (*glob).subpaths = NIL;
    (*glob).subroots = NIL;
    (*glob).rewindPlanIDs = ptr::null_mut();
    (*glob).finalrtable = NIL;
    (*glob).allRelids = ptr::null_mut();
    (*glob).prunableRelids = ptr::null_mut();
    (*glob).finalrteperminfos = NIL;
    (*glob).finalrowmarks = NIL;
    (*glob).resultRelations = NIL;
    (*glob).appendRelations = NIL;
    (*glob).partPruneInfos = NIL;
    (*glob).relationOids = NIL;
    (*glob).invalItems = NIL;
    (*glob).paramExecTypes = NIL;
    (*glob).lastPHId = 0;
    (*glob).lastRowMarkId = 0;
    (*glob).lastPlanNodeId = 0;
    (*glob).transientPlan = false;
    (*glob).dependsOnRole = false;
    (*glob).partition_directory = ptr::null_mut();

    /*
     * Assess whether it's feasible to use parallel mode for this query. We
     * can't do this in a standalone backend, or if the command will try to
     * modify any data, or if this is a cursor operation, or if GUCs are set
     * to values that don't permit parallelism, or if parallel-unsafe
     * functions are present in the query tree.
     */
    if (cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0
        && IsUnderPostmaster
        && (*parse).commandType == CMD_SELECT
        && !(*parse).hasModifyingCTE
        && max_parallel_workers_per_gather > 0
        && !IsParallelWorker()
    {
        /* all the cheap tests pass, so scan the query tree */
        (*glob).maxParallelHazard = max_parallel_hazard(parse);
        (*glob).parallelModeOK = (*glob).maxParallelHazard != PROPARALLEL_UNSAFE;
    } else {
        /* skip the query tree scan, just assume it's unsafe */
        (*glob).maxParallelHazard = PROPARALLEL_UNSAFE;
        (*glob).parallelModeOK = false;
    }

    /*
     * glob->parallelModeNeeded is normally set to false here and changed to
     * true during plan creation if a Gather or Gather Merge plan is actually
     * created (cf. create_gather_plan, create_gather_merge_plan).
     */
    (*glob).parallelModeNeeded =
        (*glob).parallelModeOK && (debug_parallel_query != DEBUG_PARALLEL_OFF);

    /* Determine what fraction of the plan is likely to be scanned */
    if (cursorOptions & CURSOR_OPT_FAST_PLAN) != 0 {
        /*
         * We have no real idea how many tuples the user will ultimately FETCH
         * from a cursor, but it is often the case that he doesn't want 'em
         * all, or would prefer a fast-start plan anyway so that he can
         * process some of the tuples sooner.  Use a GUC parameter to decide
         * what fraction to optimize for.
         */
        tuple_fraction = cursor_tuple_fraction;

        /*
         * We document cursor_tuple_fraction as simply being a fraction, which
         * means the edge cases 0 and 1 have to be treated specially here.  We
         * convert 1 to 0 ("all the tuples") and 0 to a very small fraction.
         */
        if tuple_fraction >= 1.0 {
            tuple_fraction = 0.0;
        } else if tuple_fraction <= 0.0 {
            tuple_fraction = 1e-10;
        }
    } else {
        /* Default assumption is we need all the tuples */
        tuple_fraction = 0.0;
    }

    /* primary planning entry point (may recurse for subqueries) */
    root = subquery_planner(glob, parse, ptr::null_mut(), false, tuple_fraction, ptr::null_mut());

    /* Select best Path and turn it into a Plan */
    final_rel = fetch_upper_rel(root, UPPERREL_FINAL, ptr::null_mut());
    best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

    top_plan = create_plan(root, best_path);

    /*
     * If creating a plan for a scrollable cursor, make sure it can run
     * backwards on demand.  Add a Material node at the top at need.
     */
    if (cursorOptions & CURSOR_OPT_SCROLL) != 0 {
        if !ExecSupportsBackwardScan(top_plan) {
            top_plan = materialize_finished_plan(top_plan);
        }
    }

    /*
     * Optionally add a Gather node for testing purposes, provided this is
     * actually a safe thing to do.
     */
    if debug_parallel_query != DEBUG_PARALLEL_OFF
        && (*top_plan).parallel_safe
        && ((*top_plan).initPlan == NIL || debug_parallel_query != DEBUG_PARALLEL_REGRESS)
    {
        let gather: *mut Gather = makeNode!(Gather, T_Gather);
        let mut initplan_cost: Cost = 0.0;
        let mut unsafe_initplans: bool = false;

        (*gather).plan.targetlist = (*top_plan).targetlist;
        (*gather).plan.qual = NIL;
        (*gather).plan.lefttree = top_plan;
        (*gather).plan.righttree = ptr::null_mut();
        (*gather).num_workers = 1;
        (*gather).single_copy = true;
        (*gather).invisible = debug_parallel_query == DEBUG_PARALLEL_REGRESS;

        /* Transfer any initPlans to the new top node */
        (*gather).plan.initPlan = (*top_plan).initPlan;
        (*top_plan).initPlan = NIL;

        /*
         * Since this Gather has no parallel-aware descendants to signal to,
         * we don't need a rescan Param.
         */
        (*gather).rescan_param = -1;

        /*
         * Ideally we'd use cost_gather here, but setting up dummy path data
         * to satisfy it doesn't seem much cleaner than knowing what it does.
         */
        (*gather).plan.startup_cost = (*top_plan).startup_cost + parallel_setup_cost;
        (*gather).plan.total_cost = (*top_plan).total_cost
            + parallel_setup_cost
            + parallel_tuple_cost * (*top_plan).plan_rows;
        (*gather).plan.plan_rows = (*top_plan).plan_rows;
        (*gather).plan.plan_width = (*top_plan).plan_width;
        (*gather).plan.parallel_aware = false;
        (*gather).plan.parallel_safe = false;

        /*
         * Delete the initplans' cost from top_plan.  We needn't add it to the
         * Gather node, since the above coding already included it there.
         */
        SS_compute_initplan_cost((*gather).plan.initPlan, &raw mut initplan_cost, &raw mut unsafe_initplans);
        (*top_plan).startup_cost -= initplan_cost;
        (*top_plan).total_cost -= initplan_cost;

        /* use parallel mode for parallel plans. */
        (*(*root).glob).parallelModeNeeded = true;

        top_plan = &raw mut (*gather).plan;
    }

    /*
     * If any Params were generated, run through the plan tree and compute
     * each plan node's extParam/allParam sets.
     */
    if (*glob).paramExecTypes != NIL {
        Assert!(list_length((*glob).subplans) == list_length((*glob).subroots));
        forboth!(lp, (*glob).subplans, lr, (*glob).subroots, {
            let subplan: *mut Plan = lfirst(lp) as *mut Plan;
            let subroot: *mut PlannerInfo = lfirst_node!(PlannerInfo, T_PlannerInfo, lr);

            SS_finalize_plan(subroot, subplan);
        });
        SS_finalize_plan(root, top_plan);
    }

    /* final cleanup of the plan */
    Assert!((*glob).finalrtable == NIL);
    Assert!((*glob).finalrteperminfos == NIL);
    Assert!((*glob).finalrowmarks == NIL);
    Assert!((*glob).resultRelations == NIL);
    Assert!((*glob).appendRelations == NIL);
    top_plan = set_plan_references(root, top_plan);
    /* ... and the subplans (both regular subplans and initplans) */
    Assert!(list_length((*glob).subplans) == list_length((*glob).subroots));
    forboth!(lp, (*glob).subplans, lr, (*glob).subroots, {
        let subplan: *mut Plan = lfirst(lp) as *mut Plan;
        let subroot: *mut PlannerInfo = lfirst_node!(PlannerInfo, T_PlannerInfo, lr);

        *(&raw mut (*lp).ptr_value as *mut *mut Plan) = set_plan_references(subroot, subplan);
    });

    /* build the PlannedStmt result */
    result = makeNode!(PlannedStmt, T_PlannedStmt);

    (*result).commandType = (*parse).commandType;
    (*result).queryId = (*parse).queryId;
    (*result).hasReturning = (*parse).returningList != NIL;
    (*result).hasModifyingCTE = (*parse).hasModifyingCTE;
    (*result).canSetTag = (*parse).canSetTag;
    (*result).transientPlan = (*glob).transientPlan;
    (*result).dependsOnRole = (*glob).dependsOnRole;
    (*result).parallelModeNeeded = (*glob).parallelModeNeeded;
    (*result).planTree = top_plan;
    (*result).partPruneInfos = (*glob).partPruneInfos;
    (*result).rtable = (*glob).finalrtable;
    (*result).unprunableRelids = bms_difference((*glob).allRelids, (*glob).prunableRelids);
    (*result).permInfos = (*glob).finalrteperminfos;
    (*result).resultRelations = (*glob).resultRelations;
    (*result).appendRelations = (*glob).appendRelations;
    (*result).subplans = (*glob).subplans;
    (*result).rewindPlanIDs = (*glob).rewindPlanIDs;
    (*result).rowMarks = (*glob).finalrowmarks;
    (*result).relationOids = (*glob).relationOids;
    (*result).invalItems = (*glob).invalItems;
    (*result).paramExecTypes = (*glob).paramExecTypes;
    /* utilityStmt should be null, but we might as well copy it */
    (*result).utilityStmt = (*parse).utilityStmt;
    (*result).stmt_location = (*parse).stmt_location;
    (*result).stmt_len = (*parse).stmt_len;

    (*result).jitFlags = PGJIT_NONE;
    if jit_enabled && jit_above_cost >= 0.0 && (*top_plan).total_cost > jit_above_cost {
        (*result).jitFlags |= PGJIT_PERFORM;

        /*
         * Decide how much effort should be put into generating better code.
         */
        if jit_optimize_above_cost >= 0.0 && (*top_plan).total_cost > jit_optimize_above_cost {
            (*result).jitFlags |= PGJIT_OPT3;
        }
        if jit_inline_above_cost >= 0.0 && (*top_plan).total_cost > jit_inline_above_cost {
            (*result).jitFlags |= PGJIT_INLINE;
        }

        /*
         * Decide which operations should be JITed.
         */
        if jit_expressions {
            (*result).jitFlags |= PGJIT_EXPR;
        }
        if jit_tuple_deforming {
            (*result).jitFlags |= PGJIT_DEFORM;
        }
    }

    if !(*glob).partition_directory.is_null() {
        DestroyPartitionDirectory((*glob).partition_directory);
    }

    result
}

/*--------------------
 * subquery_planner
 *	  Invokes the planner on a subquery.  We recurse to here for each
 *	  sub-SELECT found in the query tree.
 *--------------------
 */
pub unsafe fn subquery_planner(
    glob: *mut PlannerGlobal,
    mut parse: *mut Query,
    parent_root: *mut PlannerInfo,
    hasRecursion: bool,
    tuple_fraction: f64,
    setops: *mut SetOperationStmt,
) -> *mut PlannerInfo {
    let root: *mut PlannerInfo;
    let mut newWithCheckOptions: *mut List;
    let mut newHaving: *mut List;
    let mut hasOuterJoins: bool;
    let mut hasResultRTEs: bool;
    let final_rel: *mut RelOptInfo;
    let mut l: *mut ListCell;

    /* Create a PlannerInfo data structure for this subquery */
    root = makeNode!(PlannerInfo, T_PlannerInfo);
    (*root).parse = parse;
    (*root).glob = glob;
    (*root).query_level = if !parent_root.is_null() { (*parent_root).query_level + 1 } else { 1 };
    (*root).parent_root = parent_root;
    (*root).plan_params = NIL;
    (*root).outer_params = ptr::null_mut();
    (*root).planner_cxt = CurrentMemoryContext as *mut c_void;
    (*root).init_plans = NIL;
    (*root).cte_plan_ids = NIL;
    (*root).multiexpr_params = NIL;
    (*root).join_domains = NIL;
    (*root).eq_classes = NIL;
    (*root).ec_merging_done = false;
    (*root).last_rinfo_serial = 0;
    (*root).all_result_relids = if (*parse).resultRelation != 0 {
        bms_make_singleton((*parse).resultRelation as c_int)
    } else {
        ptr::null_mut()
    };
    (*root).leaf_result_relids = ptr::null_mut(); /* we'll find out leaf-ness later */
    (*root).append_rel_list = NIL;
    (*root).row_identity_vars = NIL;
    (*root).rowMarks = NIL;
    ptr::write_bytes((*root).upper_rels.as_mut_ptr(), 0, (*root).upper_rels.len());
    ptr::write_bytes((*root).upper_targets.as_mut_ptr(), 0, (*root).upper_targets.len());
    (*root).processed_groupClause = NIL;
    (*root).processed_distinctClause = NIL;
    (*root).processed_tlist = NIL;
    (*root).update_colnos = NIL;
    (*root).grouping_map = ptr::null_mut();
    (*root).minmax_aggs = NIL;
    (*root).qual_security_level = 0;
    (*root).hasPseudoConstantQuals = false;
    (*root).hasAlternativeSubPlans = false;
    (*root).placeholdersFrozen = false;
    (*root).hasRecursion = hasRecursion;
    if hasRecursion {
        (*root).wt_param_id = assign_special_exec_param(root);
    } else {
        (*root).wt_param_id = -1;
    }
    (*root).non_recursive_path = ptr::null_mut();
    (*root).partColsUpdated = false;

    /*
     * Create the top-level join domain.  This won't have valid contents until
     * deconstruct_jointree fills it in, but the node needs to exist before
     * that so we can build EquivalenceClasses referencing it.
     */
    (*root).join_domains = list_make1!(makeNode!(JoinDomain, T_JoinDomain));

    /*
     * If there is a WITH list, process each WITH query and either convert it
     * to RTE_SUBQUERY RTE(s) or build an initplan SubPlan structure for it.
     */
    if !(*parse).cteList.is_null() {
        SS_process_ctes(root);
    }

    /*
     * If it's a MERGE command, transform the joinlist as appropriate.
     */
    transform_MERGE_to_join(parse);

    /*
     * If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE, so
     * that we don't need so many special cases to deal with that situation.
     */
    replace_empty_jointree(parse);

    /*
     * Look for ANY and EXISTS SubLinks in WHERE and JOIN/ON clauses, and try
     * to transform them into joins.
     */
    if (*parse).hasSubLinks {
        pull_up_sublinks(root);
    }

    /*
     * Scan the rangetable for function RTEs, do const-simplification on them,
     * and then inline them if possible.
     */
    preprocess_function_rtes(root);

    /*
     * Scan the rangetable for relations with virtual generated columns, and
     * replace all Var nodes in the query that reference these columns with
     * the generation expressions.
     */
    parse = expand_virtual_generated_columns(root);
    (*root).parse = parse;

    /*
     * Check to see if any subqueries in the jointree can be merged into this
     * query.
     */
    pull_up_subqueries(root);

    /*
     * If this is a simple UNION ALL query, flatten it into an appendrel.
     */
    if !(*parse).setOperations.is_null() {
        flatten_simple_union_all(root);
    }

    /*
     * Survey the rangetable to see what kinds of entries are present.
     */
    (*root).hasJoinRTEs = false;
    (*root).hasLateralRTEs = false;
    (*root).group_rtindex = 0;
    hasOuterJoins = false;
    hasResultRTEs = false;
    foreach!(l, (*parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));

        match (*rte).rtekind {
            RTE_RELATION => {
                if (*rte).inh {
                    /*
                     * Check to see if the relation actually has any children;
                     * if not, clear the inh flag so we can treat it as a
                     * plain base relation.
                     */
                    (*rte).inh = has_subclass((*rte).relid);
                }
            }
            RTE_JOIN => {
                (*root).hasJoinRTEs = true;
                if IS_OUTER_JOIN((*rte).jointype) {
                    hasOuterJoins = true;
                }
            }
            RTE_RESULT => {
                hasResultRTEs = true;
            }
            RTE_GROUP => {
                Assert!((*parse).hasGroupRTE);
                (*root).group_rtindex = (list_cell_number((*parse).rtable, current_cell!(l)) + 1) as c_int;
            }
            _ => {
                /* No work here for other RTE types */
            }
        }

        if (*rte).lateral {
            (*root).hasLateralRTEs = true;
        }

        /*
         * We can also determine the maximum security level required for any
         * securityQuals now.
         */
        if !(*rte).securityQuals.is_null() {
            (*root).qual_security_level =
                std::cmp::max((*root).qual_security_level, list_length((*rte).securityQuals) as Index);
        }
    });

    /*
     * If we have now verified that the query target relation is
     * non-inheriting, mark it as a leaf target.
     */
    if (*parse).resultRelation != 0 {
        let rte: *mut RangeTblEntry = rt_fetch((*parse).resultRelation as u32, (*parse).rtable);

        if !(*rte).inh {
            (*root).leaf_result_relids = bms_make_singleton((*parse).resultRelation as c_int);
        }
    }

    /*
     * Check access permissions for any view relations mentioned in the query.
     */
    foreach!(l, (*parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));

        if (*rte).perminfoindex != 0 && (*rte).relkind == RELKIND_VIEW {
            let perminfo: *mut RTEPermissionInfo;
            let result: bool;

            perminfo = getRTEPermissionInfo((*parse).rteperminfos, rte);
            result = ExecCheckOneRelPerms(perminfo);
            if !result {
                aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name((*perminfo).relid));
            }
        }
    });

    /*
     * Preprocess RowMark information.
     */
    preprocess_rowmarks(root);

    /*
     * Set hasHavingQual to remember if HAVING clause is present.
     */
    (*root).hasHavingQual = !(*parse).havingQual.is_null();

    /*
     * Do expression preprocessing on targetlist and quals, as well as other
     * random expressions in the querytree.
     */
    (*parse).targetList =
        preprocess_expression(root, (*parse).targetList as *mut Node, EXPRKIND_TARGET) as *mut List;

    newWithCheckOptions = NIL;
    foreach!(l, (*parse).withCheckOptions, {
        let wco: *mut WithCheckOption = lfirst_node!(WithCheckOption, T_WithCheckOption, current_cell!(l));

        (*wco).qual = preprocess_expression(root, (*wco).qual, EXPRKIND_QUAL);
        if !(*wco).qual.is_null() {
            newWithCheckOptions = lappend(newWithCheckOptions, wco as *mut c_void);
        }
    });
    (*parse).withCheckOptions = newWithCheckOptions;

    (*parse).returningList =
        preprocess_expression(root, (*parse).returningList as *mut Node, EXPRKIND_TARGET) as *mut List;

    preprocess_qual_conditions(root, (*parse).jointree as *mut Node);

    (*parse).havingQual = preprocess_expression(root, (*parse).havingQual, EXPRKIND_QUAL);

    foreach!(l, (*parse).windowClause, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(l));

        /* partitionClause/orderClause are sort/group expressions */
        (*wc).startOffset = preprocess_expression(root, (*wc).startOffset, EXPRKIND_LIMIT);
        (*wc).endOffset = preprocess_expression(root, (*wc).endOffset, EXPRKIND_LIMIT);
    });

    (*parse).limitOffset = preprocess_expression(root, (*parse).limitOffset, EXPRKIND_LIMIT);
    (*parse).limitCount = preprocess_expression(root, (*parse).limitCount, EXPRKIND_LIMIT);

    if !(*parse).onConflict.is_null() {
        (*(*parse).onConflict).arbiterElems = preprocess_expression(
            root,
            (*(*parse).onConflict).arbiterElems as *mut Node,
            EXPRKIND_ARBITER_ELEM,
        ) as *mut List;
        (*(*parse).onConflict).arbiterWhere =
            preprocess_expression(root, (*(*parse).onConflict).arbiterWhere, EXPRKIND_QUAL);
        (*(*parse).onConflict).onConflictSet = preprocess_expression(
            root,
            (*(*parse).onConflict).onConflictSet as *mut Node,
            EXPRKIND_TARGET,
        ) as *mut List;
        (*(*parse).onConflict).onConflictWhere =
            preprocess_expression(root, (*(*parse).onConflict).onConflictWhere, EXPRKIND_QUAL);
        /* exclRelTlist contains only Vars, so no preprocessing needed */
    }

    foreach!(l, (*parse).mergeActionList, {
        let action: *mut MergeAction = lfirst(current_cell!(l)) as *mut MergeAction;

        (*action).targetList =
            preprocess_expression(root, (*action).targetList as *mut Node, EXPRKIND_TARGET) as *mut List;
        (*action).qual = preprocess_expression(root, (*action).qual as *mut Node, EXPRKIND_QUAL);
    });

    (*parse).mergeJoinCondition =
        preprocess_expression(root, (*parse).mergeJoinCondition, EXPRKIND_QUAL);

    (*root).append_rel_list =
        preprocess_expression(root, (*root).append_rel_list as *mut Node, EXPRKIND_APPINFO) as *mut List;

    /* Also need to preprocess expressions within RTEs */
    foreach!(l, (*parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));
        let kind: c_int;
        let mut lcsq: *mut ListCell;

        if (*rte).rtekind == RTE_RELATION {
            if !(*rte).tablesample.is_null() {
                (*rte).tablesample = preprocess_expression(
                    root,
                    (*rte).tablesample as *mut Node,
                    EXPRKIND_TABLESAMPLE,
                ) as *mut TableSampleClause;
            }
        } else if (*rte).rtekind == RTE_SUBQUERY {
            /*
             * We don't want to do all preprocessing yet on the subquery's
             * expressions; but if it contains any join aliases of our level,
             * those have to get expanded now.
             */
            if (*rte).lateral && (*root).hasJoinRTEs {
                (*rte).subquery = flatten_join_alias_vars(
                    root,
                    (*root).parse,
                    (*rte).subquery as *mut Node,
                ) as *mut Query;
            }
        } else if (*rte).rtekind == RTE_FUNCTION {
            /* Preprocess the function expression(s) fully */
            kind = if (*rte).lateral { EXPRKIND_RTFUNC_LATERAL } else { EXPRKIND_RTFUNC };
            (*rte).functions =
                preprocess_expression(root, (*rte).functions as *mut Node, kind) as *mut List;
        } else if (*rte).rtekind == RTE_TABLEFUNC {
            /* Preprocess the function expression(s) fully */
            kind = if (*rte).lateral { EXPRKIND_TABLEFUNC_LATERAL } else { EXPRKIND_TABLEFUNC };
            (*rte).tablefunc =
                preprocess_expression(root, (*rte).tablefunc as *mut Node, kind) as *mut TableFunc;
        } else if (*rte).rtekind == RTE_VALUES {
            /* Preprocess the values lists fully */
            kind = if (*rte).lateral { EXPRKIND_VALUES_LATERAL } else { EXPRKIND_VALUES };
            (*rte).values_lists =
                preprocess_expression(root, (*rte).values_lists as *mut Node, kind) as *mut List;
        } else if (*rte).rtekind == RTE_GROUP {
            /* Preprocess the groupexprs list fully */
            (*rte).groupexprs =
                preprocess_expression(root, (*rte).groupexprs as *mut Node, EXPRKIND_GROUPEXPR) as *mut List;
        }

        /*
         * Process each element of the securityQuals list as if it were a
         * separate qual expression.
         */
        foreach!(lcsq, (*rte).securityQuals, {
            *(&raw mut (*current_cell!(lcsq)).ptr_value as *mut *mut Node) =
                preprocess_expression(root, lfirst(current_cell!(lcsq)) as *mut Node, EXPRKIND_QUAL);
        });
    });

    /*
     * Now that we are done preprocessing expressions, get rid of the
     * joinaliasvars lists.
     */
    if (*root).hasJoinRTEs {
        foreach!(l, (*parse).rtable, {
            let rte: *mut RangeTblEntry = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));

            (*rte).joinaliasvars = NIL;
        });
    }

    /*
     * Replace any Vars in the subquery's targetlist and havingQual that
     * reference GROUP outputs with the underlying grouping expressions.
     */
    if (*parse).hasGroupRTE {
        (*parse).targetList =
            flatten_group_exprs(root, (*root).parse, (*parse).targetList as *mut Node) as *mut List;
        (*parse).havingQual = flatten_group_exprs(root, (*root).parse, (*parse).havingQual);
    }

    /* Constant-folding might have removed all set-returning functions */
    if (*parse).hasTargetSRFs {
        (*parse).hasTargetSRFs = expression_returns_set((*parse).targetList as *mut Node);
    }

    /*
     * If we have grouping sets, expand the groupingSets tree of this query to
     * a flat list of grouping sets.
     */
    if !(*parse).groupingSets.is_null() {
        (*parse).groupingSets =
            expand_grouping_sets((*parse).groupingSets, (*parse).groupDistinct, -1);
    }

    /*
     * In some cases we may want to transfer a HAVING clause into WHERE.
     */
    newHaving = NIL;
    foreach!(l, (*parse).havingQual as *mut List, {
        let havingclause: *mut Node = lfirst(current_cell!(l)) as *mut Node;

        if contain_agg_clause(havingclause)
            || contain_volatile_functions(havingclause)
            || contain_subplans(havingclause)
            || (!(*parse).groupClause.is_null()
                && !(*parse).groupingSets.is_null()
                && bms_is_member((*root).group_rtindex as c_int, pull_varnos(root, havingclause)))
        {
            /* keep it in HAVING */
            newHaving = lappend(newHaving, havingclause as *mut c_void);
        } else if !(*parse).groupClause.is_null()
            && ((*parse).groupingSets == NIL
                || (linitial((*parse).groupingSets) as *mut List) != NIL)
        {
            /* There is GROUP BY, but no empty grouping set */
            let whereclause: *mut Node;

            /* Preprocess the HAVING clause fully */
            whereclause = preprocess_expression(root, havingclause, EXPRKIND_QUAL);
            /* ... and move it to WHERE */
            (*(*parse).jointree).quals = list_concat(
                (*(*parse).jointree).quals as *mut List,
                whereclause as *mut List,
            ) as *mut Node;
        } else {
            /* There is an empty grouping set (perhaps implicitly) */
            let whereclause: *mut Node;

            /* Preprocess the HAVING clause fully */
            whereclause = preprocess_expression(root, copyObject_node(havingclause), EXPRKIND_QUAL);
            /* ... and put a copy in WHERE */
            (*(*parse).jointree).quals = list_concat(
                (*(*parse).jointree).quals as *mut List,
                whereclause as *mut List,
            ) as *mut Node;
            /* ... and also keep it in HAVING */
            newHaving = lappend(newHaving, havingclause as *mut c_void);
        }
    });
    (*parse).havingQual = newHaving as *mut Node;

    /*
     * If we have any outer joins, try to reduce them to plain inner joins.
     */
    if hasOuterJoins {
        reduce_outer_joins(root);
    }

    /*
     * If we have any RTE_RESULT relations, see if they can be deleted from
     * the jointree.
     */
    if hasResultRTEs || hasOuterJoins {
        remove_useless_result_rtes(root);
    }

    /*
     * Do the main planning.
     */
    grouping_planner(root, tuple_fraction, setops);

    /*
     * Capture the set of outer-level param IDs we have access to.
     */
    SS_identify_outer_params(root);

    /*
     * If any initPlans were created in this query level, adjust the surviving
     * Paths' costs and parallel-safety flags to account for them.
     */
    final_rel = fetch_upper_rel(root, UPPERREL_FINAL, ptr::null_mut());
    SS_charge_for_initplans(root, final_rel);

    /*
     * Make sure we've identified the cheapest Path for the final rel.
     */
    set_cheapest(final_rel);

    root
}

/*
 * preprocess_expression
 *		Do subquery_planner's preprocessing work for an expression,
 *		which can be a targetlist, a WHERE clause (including JOIN/ON
 *		conditions), a HAVING clause, or a few other things.
 */
unsafe fn preprocess_expression(root: *mut PlannerInfo, mut expr: *mut Node, kind: c_int) -> *mut Node {
    /*
     * Fall out quickly if expression is empty.
     */
    if expr.is_null() {
        return ptr::null_mut();
    }

    /*
     * If the query has any join RTEs, replace join alias variables with
     * base-relation variables.
     */
    if (*root).hasJoinRTEs
        && !(kind == EXPRKIND_RTFUNC
            || kind == EXPRKIND_VALUES
            || kind == EXPRKIND_TABLESAMPLE
            || kind == EXPRKIND_TABLEFUNC)
    {
        expr = flatten_join_alias_vars(root, (*root).parse, expr);
    }

    /*
     * Simplify constant expressions.
     */
    if kind != EXPRKIND_RTFUNC {
        expr = eval_const_expressions(root, expr);
    }

    /*
     * If it's a qual or havingQual, canonicalize it.
     */
    if kind == EXPRKIND_QUAL {
        expr = canonicalize_qual(expr as *mut Expr, false) as *mut Node;
    }

    /*
     * Check for ANY ScalarArrayOpExpr with Const arrays and set the
     * hashfuncid of any that might execute more quickly by using hash lookups.
     */
    if kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET {
        convert_saop_to_hashed_saop(expr);
    }

    /* Expand SubLinks to SubPlans */
    if (*(*root).parse).hasSubLinks {
        expr = SS_process_sublinks(root, expr, kind == EXPRKIND_QUAL);
    }

    /* Replace uplevel vars with Param nodes (this IS possible in VALUES) */
    if (*root).query_level > 1 {
        expr = SS_replace_correlation_vars(root, expr);
    }

    /*
     * If it's a qual or havingQual, convert it to implicit-AND format.
     */
    if kind == EXPRKIND_QUAL {
        expr = make_ands_implicit(expr as *mut Expr) as *mut Node;
    }

    expr
}

/*
 * preprocess_qual_conditions
 *		Recursively scan the query's jointree and do subquery_planner's
 *		preprocessing work on each qual condition found therein.
 */
unsafe fn preprocess_qual_conditions(root: *mut PlannerInfo, jtnode: *mut Node) {
    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        /* nothing to do here */
    } else if IsA!(jtnode, T_FromExpr) {
        let f: *mut FromExpr = jtnode as *mut FromExpr;
        let mut l: *mut ListCell;

        foreach!(l, (*f).fromlist, {
            preprocess_qual_conditions(root, lfirst(current_cell!(l)) as *mut Node);
        });

        (*f).quals = preprocess_expression(root, (*f).quals, EXPRKIND_QUAL);
    } else if IsA!(jtnode, T_JoinExpr) {
        let j: *mut JoinExpr = jtnode as *mut JoinExpr;

        preprocess_qual_conditions(root, (*j).larg);
        preprocess_qual_conditions(root, (*j).rarg);

        (*j).quals = preprocess_expression(root, (*j).quals, EXPRKIND_QUAL);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as c_int);
    }
}

/*
 * preprocess_phv_expression
 *	  Do preprocessing on a PlaceHolderVar expression that's been pulled up.
 */
pub unsafe fn preprocess_phv_expression(root: *mut PlannerInfo, expr: *mut Expr) -> *mut Expr {
    preprocess_expression(root, expr as *mut Node, EXPRKIND_PHV) as *mut Expr
}

// TODO(pg-port): real copyObject lives in nodes/copyfuncs.rs (deferred). Shallow stub.
unsafe fn copyObject_node(node: *mut Node) -> *mut Node {
    node
}
unsafe fn copyObject_list(list: *mut List) -> *mut List {
    list
}

/*--------------------
 * grouping_planner
 *	  Perform planning steps related to grouping, aggregation, etc.
 *--------------------
 */
unsafe fn grouping_planner(
    root: *mut PlannerInfo,
    mut tuple_fraction: f64,
    setops: *mut SetOperationStmt,
) {
    let parse: *mut Query = (*root).parse;
    let mut offset_est: int64 = 0;
    let mut count_est: int64 = 0;
    let mut limit_tuples: f64 = -1.0;
    let mut have_postponed_srfs: bool = false;
    let mut final_target: *mut PathTarget;
    let mut final_targets: *mut List = NIL;
    let mut final_targets_contain_srfs: *mut List = NIL;
    let final_target_parallel_safe: bool;
    let mut current_rel: *mut RelOptInfo;
    let final_rel: *mut RelOptInfo;
    let mut extra: FinalPathExtraData = std::mem::zeroed();
    let mut lc: *mut ListCell;

    /* Tweak caller-supplied tuple_fraction if have LIMIT/OFFSET */
    if !(*parse).limitCount.is_null() || !(*parse).limitOffset.is_null() {
        tuple_fraction = preprocess_limit(root, tuple_fraction, &raw mut offset_est, &raw mut count_est);

        /*
         * If we have a known LIMIT, and don't have an unknown OFFSET, we can
         * estimate the effects of using a bounded sort.
         */
        if count_est > 0 && offset_est >= 0 {
            limit_tuples = count_est as f64 + offset_est as f64;
        }
    }

    /* Make tuple_fraction accessible to lower-level routines */
    (*root).tuple_fraction = tuple_fraction;

    if !(*parse).setOperations.is_null() {
        /*
         * Construct Paths for set operations.
         */
        current_rel = plan_set_operations(root);

        /*
         * Use the processed_tlist returned by plan_set_operations.
         */
        Assert!((*parse).commandType == CMD_SELECT);

        /* for safety, copy processed_tlist instead of modifying in-place */
        (*root).processed_tlist =
            postprocess_setop_tlist(copyObject_list((*root).processed_tlist), (*parse).targetList);

        /* Also extract the PathTarget form of the setop result tlist */
        final_target = (*(*current_rel).cheapest_total_path).pathtarget;

        /* And check whether it's parallel safe */
        final_target_parallel_safe = is_parallel_safe(root, (*final_target).exprs as *mut Node);

        /* The setop result tlist couldn't contain any SRFs */
        Assert!(!(*parse).hasTargetSRFs);
        final_targets = NIL;
        final_targets_contain_srfs = NIL;

        /*
         * Can't handle FOR [KEY] UPDATE/SHARE here.
         */
        if !(*parse).rowMarks.is_null() {
            ereport!(ERROR, errmsg!("{} is not allowed with UNION/INTERSECT/EXCEPT",
                std::ffi::CStr::from_ptr(LCS_asString(
                    (*lfirst_node!(RowMarkClause, T_RowMarkClause, list_head((*parse).rowMarks))).strength)).to_string_lossy()));
            unreachable!();
        }

        /*
         * Calculate pathkeys that represent result ordering requirements
         */
        Assert!((*parse).distinctClause == NIL);
        (*root).sort_pathkeys =
            make_pathkeys_for_sortclauses(root, (*parse).sortClause, (*root).processed_tlist);
    } else {
        /* No set operations, do regular planning */
        let mut sort_input_target: *mut PathTarget;
        let mut sort_input_targets: *mut List = NIL;
        let mut sort_input_targets_contain_srfs: *mut List = NIL;
        let sort_input_target_parallel_safe: bool;
        let mut grouping_target: *mut PathTarget;
        let mut grouping_targets: *mut List = NIL;
        let mut grouping_targets_contain_srfs: *mut List = NIL;
        let grouping_target_parallel_safe: bool;
        let mut scanjoin_target: *mut PathTarget;
        let mut scanjoin_targets: *mut List = core::ptr::null_mut();
        let mut scanjoin_targets_contain_srfs: *mut List = core::ptr::null_mut();
        let scanjoin_target_parallel_safe: bool;
        let scanjoin_target_same_exprs: bool;
        let have_grouping: bool;
        let mut wflists: *mut WindowFuncLists = ptr::null_mut();
        let mut activeWindows: *mut List = NIL;
        let mut gset_data: *mut grouping_sets_data = ptr::null_mut();
        let mut qp_extra: standard_qp_extra = std::mem::zeroed();

        /* A recursive query should always have setOperations */
        Assert!(!(*root).hasRecursion);

        /* Preprocess grouping sets and GROUP BY clause, if any */
        if !(*parse).groupingSets.is_null() {
            gset_data = preprocess_grouping_sets(root);
        } else if !(*parse).groupClause.is_null() {
            /* Preprocess regular GROUP BY clause, if any */
            (*root).processed_groupClause = preprocess_groupclause(root, NIL);
        }

        /*
         * Preprocess targetlist.
         */
        preprocess_targetlist(root);

        /*
         * Mark all the aggregates with resolved aggtranstypes.
         */
        if (*parse).hasAggs {
            preprocess_aggrefs(root, (*root).processed_tlist as *mut Node);
            preprocess_aggrefs(root, (*parse).havingQual);
        }

        /*
         * Locate any window functions in the tlist.
         */
        if (*parse).hasWindowFuncs {
            wflists = find_window_functions(
                (*root).processed_tlist as *mut Node,
                list_length((*parse).windowClause),
            );
            if (*wflists).numWindowFuncs > 0 {
                /*
                 * See if any modifications can be made to each WindowClause.
                 */
                optimize_window_clauses(root, wflists);

                /* Extract the list of windows actually in use. */
                activeWindows = select_active_windows(root, wflists);

                /* Make sure they all have names, for EXPLAIN's use. */
                name_active_windows(activeWindows);
            } else {
                (*parse).hasWindowFuncs = false;
            }
        }

        /*
         * Preprocess MIN/MAX aggregates, if any.
         */
        if (*parse).hasAggs {
            preprocess_minmax_aggregates(root);
        }

        /*
         * Figure out whether there's a hard limit on the number of rows.
         */
        if !(*parse).groupClause.is_null()
            || !(*parse).groupingSets.is_null()
            || !(*parse).distinctClause.is_null()
            || (*parse).hasAggs
            || (*parse).hasWindowFuncs
            || (*parse).hasTargetSRFs
            || (*root).hasHavingQual
        {
            (*root).limit_tuples = -1.0;
        } else {
            (*root).limit_tuples = limit_tuples;
        }

        /* Set up data needed by standard_qp_callback */
        qp_extra.activeWindows = activeWindows;
        qp_extra.gset_data = gset_data;

        /*
         * If we're a subquery for a set operation, store the SetOperationStmt
         * in qp_extra.
         */
        qp_extra.setop = setops;

        /*
         * Generate the best unsorted and presorted paths for the scan/join
         * portion of this Query.
         */
        current_rel = query_planner(root, standard_qp_callback, &raw mut qp_extra as *mut c_void);

        /*
         * Convert the query's result tlist into PathTarget format.
         */
        final_target = create_pathtarget(root, (*root).processed_tlist);
        final_target_parallel_safe = is_parallel_safe(root, (*final_target).exprs as *mut Node);

        /*
         * If ORDER BY was given, consider whether we should use a post-sort
         * projection.
         */
        if !(*parse).sortClause.is_null() {
            sort_input_target = make_sort_input_target(root, final_target, &raw mut have_postponed_srfs);
            sort_input_target_parallel_safe =
                is_parallel_safe(root, (*sort_input_target).exprs as *mut Node);
        } else {
            sort_input_target = final_target;
            sort_input_target_parallel_safe = final_target_parallel_safe;
        }

        /*
         * If we have window functions to deal with, the output from any
         * grouping step needs to be what the window functions want.
         */
        if !activeWindows.is_null() {
            grouping_target = make_window_input_target(root, final_target, activeWindows);
            grouping_target_parallel_safe =
                is_parallel_safe(root, (*grouping_target).exprs as *mut Node);
        } else {
            grouping_target = sort_input_target;
            grouping_target_parallel_safe = sort_input_target_parallel_safe;
        }

        /*
         * If we have grouping or aggregation to do, the topmost scan/join
         * plan node must emit what the grouping step wants.
         */
        have_grouping = !(*parse).groupClause.is_null()
            || !(*parse).groupingSets.is_null()
            || (*parse).hasAggs
            || (*root).hasHavingQual;
        if have_grouping {
            scanjoin_target = make_group_input_target(root, final_target);
            scanjoin_target_parallel_safe =
                is_parallel_safe(root, (*scanjoin_target).exprs as *mut Node);
        } else {
            scanjoin_target = grouping_target;
            scanjoin_target_parallel_safe = grouping_target_parallel_safe;
        }

        /*
         * If there are any SRFs in the targetlist, we must separate each of
         * these PathTargets into SRF-computing and SRF-free targets.
         */
        if (*parse).hasTargetSRFs {
            /* final_target doesn't recompute any SRFs in sort_input_target */
            split_pathtarget_at_srfs(
                root,
                final_target,
                sort_input_target,
                &raw mut final_targets,
                &raw mut final_targets_contain_srfs,
            );
            final_target = linitial_node_pathtarget(final_targets);
            Assert!(linitial_int(final_targets_contain_srfs) == 0);
            /* likewise for sort_input_target vs. grouping_target */
            split_pathtarget_at_srfs(
                root,
                sort_input_target,
                grouping_target,
                &raw mut sort_input_targets,
                &raw mut sort_input_targets_contain_srfs,
            );
            sort_input_target = linitial_node_pathtarget(sort_input_targets);
            Assert!(linitial_int(sort_input_targets_contain_srfs) == 0);
            /* likewise for grouping_target vs. scanjoin_target */
            split_pathtarget_at_srfs_grouping(
                root,
                grouping_target,
                scanjoin_target,
                &raw mut grouping_targets,
                &raw mut grouping_targets_contain_srfs,
            );
            grouping_target = linitial_node_pathtarget(grouping_targets);
            Assert!(linitial_int(grouping_targets_contain_srfs) == 0);
            /* scanjoin_target will not have any SRFs precomputed for it */
            split_pathtarget_at_srfs(
                root,
                scanjoin_target,
                ptr::null_mut(),
                &raw mut scanjoin_targets,
                &raw mut scanjoin_targets_contain_srfs,
            );
            scanjoin_target = linitial_node_pathtarget(scanjoin_targets);
            Assert!(linitial_int(scanjoin_targets_contain_srfs) == 0);
        } else {
            /* initialize lists; for most of these, dummy values are OK */
            final_targets = NIL;
            final_targets_contain_srfs = NIL;
            sort_input_targets = NIL;
            sort_input_targets_contain_srfs = NIL;
            grouping_targets = NIL;
            grouping_targets_contain_srfs = NIL;
            scanjoin_targets = list_make1!(scanjoin_target);
            scanjoin_targets_contain_srfs = NIL;
        }

        /* Apply scan/join target. */
        scanjoin_target_same_exprs = list_length(scanjoin_targets) == 1
            && equal((*scanjoin_target).exprs as *mut c_void, (*(*current_rel).reltarget).exprs as *mut c_void);
        apply_scanjoin_target_to_paths(
            root,
            current_rel,
            scanjoin_targets,
            scanjoin_targets_contain_srfs,
            scanjoin_target_parallel_safe,
            scanjoin_target_same_exprs,
        );

        /*
         * Save the various upper-rel PathTargets into root->upper_targets[].
         */
        (*root).upper_targets[UPPERREL_FINAL as usize] = final_target;
        (*root).upper_targets[UPPERREL_ORDERED as usize] = final_target;
        (*root).upper_targets[UPPERREL_DISTINCT as usize] = sort_input_target;
        (*root).upper_targets[UPPERREL_PARTIAL_DISTINCT as usize] = sort_input_target;
        (*root).upper_targets[UPPERREL_WINDOW as usize] = sort_input_target;
        (*root).upper_targets[UPPERREL_GROUP_AGG as usize] = grouping_target;

        /*
         * If we have grouping and/or aggregation, consider ways to implement that.
         */
        if have_grouping {
            current_rel = create_grouping_paths(
                root,
                current_rel,
                grouping_target,
                grouping_target_parallel_safe,
                gset_data,
            );
            /* Fix things up if grouping_target contains SRFs */
            if (*parse).hasTargetSRFs {
                adjust_paths_for_srfs(root, current_rel, grouping_targets, grouping_targets_contain_srfs);
            }
        }

        /*
         * If we have window functions, consider ways to implement those.
         */
        if !activeWindows.is_null() {
            current_rel = create_window_paths(
                root,
                current_rel,
                grouping_target,
                sort_input_target,
                sort_input_target_parallel_safe,
                wflists,
                activeWindows,
            );
            /* Fix things up if sort_input_target contains SRFs */
            if (*parse).hasTargetSRFs {
                adjust_paths_for_srfs(root, current_rel, sort_input_targets, sort_input_targets_contain_srfs);
            }
        }

        /*
         * If there is a DISTINCT clause, consider ways to implement that.
         */
        if !(*parse).distinctClause.is_null() {
            current_rel = create_distinct_paths(root, current_rel, sort_input_target);
        }
    } /* end of if (setOperations) */

    /*
     * If ORDER BY was given, consider ways to implement that.
     */
    if !(*parse).sortClause.is_null() {
        current_rel = create_ordered_paths(
            root,
            current_rel,
            final_target,
            final_target_parallel_safe,
            if have_postponed_srfs { -1.0 } else { limit_tuples },
        );
        /* Fix things up if final_target contains SRFs */
        if (*parse).hasTargetSRFs {
            adjust_paths_for_srfs(root, current_rel, final_targets, final_targets_contain_srfs);
        }
    }

    /*
     * Now we are prepared to build the final-output upperrel.
     */
    final_rel = fetch_upper_rel(root, UPPERREL_FINAL, ptr::null_mut());

    /*
     * If the input rel is marked consider_parallel and there's nothing that's
     * not parallel-safe in the LIMIT clause, then the final_rel can be marked
     * consider_parallel as well.
     */
    if (*current_rel).consider_parallel
        && is_parallel_safe(root, (*parse).limitOffset)
        && is_parallel_safe(root, (*parse).limitCount)
    {
        (*final_rel).consider_parallel = true;
    }

    /*
     * If the current_rel belongs to a single FDW, so does the final_rel.
     */
    (*final_rel).serverid = (*current_rel).serverid;
    (*final_rel).userid = (*current_rel).userid;
    (*final_rel).useridiscurrent = (*current_rel).useridiscurrent;
    (*final_rel).fdwroutine = (*current_rel).fdwroutine;

    /*
     * Generate paths for the final_rel.
     */
    foreach!(lc, (*current_rel).pathlist, {
        let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;

        /*
         * If there is a FOR [KEY] UPDATE/SHARE clause, add the LockRows node.
         */
        if !(*parse).rowMarks.is_null() {
            path = create_lockrows_path(
                root,
                final_rel,
                path,
                (*root).rowMarks,
                assign_special_exec_param(root),
            );
        }

        /*
         * If there is a LIMIT/OFFSET clause, add the LIMIT node.
         */
        if limit_needed(parse) {
            path = create_limit_path(
                root,
                final_rel,
                path,
                (*parse).limitOffset,
                (*parse).limitCount,
                (*parse).limitOption,
                offset_est,
                count_est,
            );
        }

        /*
         * If this is an INSERT/UPDATE/DELETE/MERGE, add the ModifyTable node.
         */
        if (*parse).commandType != CMD_SELECT {
            let rootRelation: Index;
            let mut resultRelations: *mut List = NIL;
            let mut updateColnosLists: *mut List = NIL;
            let mut withCheckOptionLists: *mut List = NIL;
            let mut returningLists: *mut List = NIL;
            let mut mergeActionLists: *mut List = NIL;
            let mut mergeJoinConditions: *mut List = NIL;
            let rowMarks: *mut List;

            if bms_membership((*root).all_result_relids) == BMS_MULTIPLE {
                /* Inherited UPDATE/DELETE/MERGE */
                let top_result_rel: *mut RelOptInfo = find_base_rel(root, (*parse).resultRelation as c_int);
                let mut resultRelation: c_int = -1;

                /* Pass the root result rel forward to the executor. */
                rootRelation = (*parse).resultRelation as u32;

                /* Add only leaf children to ModifyTable. */
                loop {
                    resultRelation = bms_next_member((*root).leaf_result_relids, resultRelation);
                    if resultRelation < 0 {
                        break;
                    }
                    let this_result_rel: *mut RelOptInfo = find_base_rel(root, resultRelation);

                    if IS_DUMMY_REL(this_result_rel) {
                        continue;
                    }

                    /* Build per-target-rel lists needed by ModifyTable */
                    resultRelations = lappend_int(resultRelations, resultRelation);
                    if (*parse).commandType == CMD_UPDATE {
                        let mut update_colnos: *mut List = (*root).update_colnos;

                        if this_result_rel != top_result_rel {
                            update_colnos = adjust_inherited_attnums_multilevel(
                                root,
                                update_colnos,
                                (*this_result_rel).relid as c_int,
                                (*top_result_rel).relid as c_int,
                            );
                        }
                        updateColnosLists = lappend(updateColnosLists, update_colnos as *mut c_void);
                    }
                    if !(*parse).withCheckOptions.is_null() {
                        let mut withCheckOptions: *mut List = (*parse).withCheckOptions;

                        if this_result_rel != top_result_rel {
                            withCheckOptions = adjust_appendrel_attrs_multilevel(
                                root,
                                withCheckOptions as *mut Node,
                                this_result_rel,
                                top_result_rel,
                            ) as *mut List;
                        }
                        withCheckOptionLists = lappend(withCheckOptionLists, withCheckOptions as *mut c_void);
                    }
                    if !(*parse).returningList.is_null() {
                        let mut returningList: *mut List = (*parse).returningList;

                        if this_result_rel != top_result_rel {
                            returningList = adjust_appendrel_attrs_multilevel(
                                root,
                                returningList as *mut Node,
                                this_result_rel,
                                top_result_rel,
                            ) as *mut List;
                        }
                        returningLists = lappend(returningLists, returningList as *mut c_void);
                    }
                    if !(*parse).mergeActionList.is_null() {
                        let mut ll: *mut ListCell;
                        let mut mergeActionList: *mut List = NIL;

                        foreach!(ll, (*parse).mergeActionList, {
                            let action: *mut MergeAction = lfirst(current_cell!(ll)) as *mut MergeAction;
                            let leaf_action: *mut MergeAction = copyObject_node(action as *mut Node) as *mut MergeAction;

                            (*leaf_action).qual = adjust_appendrel_attrs_multilevel(
                                root,
                                (*action).qual as *mut Node,
                                this_result_rel,
                                top_result_rel,
                            );
                            (*leaf_action).targetList = adjust_appendrel_attrs_multilevel(
                                root,
                                (*action).targetList as *mut Node,
                                this_result_rel,
                                top_result_rel,
                            ) as *mut List;
                            if (*leaf_action).commandType == CMD_UPDATE {
                                (*leaf_action).updateColnos = adjust_inherited_attnums_multilevel(
                                    root,
                                    (*action).updateColnos,
                                    (*this_result_rel).relid as c_int,
                                    (*top_result_rel).relid as c_int,
                                );
                            }
                            mergeActionList = lappend(mergeActionList, leaf_action as *mut c_void);
                        });

                        mergeActionLists = lappend(mergeActionLists, mergeActionList as *mut c_void);
                    }
                    if (*parse).commandType == CMD_MERGE {
                        let mut mergeJoinCondition: *mut Node = (*parse).mergeJoinCondition;

                        if this_result_rel != top_result_rel {
                            mergeJoinCondition = adjust_appendrel_attrs_multilevel(
                                root,
                                mergeJoinCondition,
                                this_result_rel,
                                top_result_rel,
                            );
                        }
                        mergeJoinConditions = lappend(mergeJoinConditions, mergeJoinCondition as *mut c_void);
                    }
                }

                if resultRelations == NIL {
                    /*
                     * We managed to exclude every child rel, so generate a
                     * dummy one-relation plan.
                     */
                    resultRelations = list_make1_int!((*parse).resultRelation as c_int);
                    if (*parse).commandType == CMD_UPDATE {
                        updateColnosLists = list_make1!((*root).update_colnos);
                    }
                    if !(*parse).withCheckOptions.is_null() {
                        withCheckOptionLists = list_make1!((*parse).withCheckOptions);
                    }
                    if !(*parse).returningList.is_null() {
                        returningLists = list_make1!((*parse).returningList);
                    }
                    if !(*parse).mergeActionList.is_null() {
                        mergeActionLists = list_make1!((*parse).mergeActionList);
                    }
                    if (*parse).commandType == CMD_MERGE {
                        mergeJoinConditions = list_make1!((*parse).mergeJoinCondition);
                    }
                }
            } else {
                /* Single-relation INSERT/UPDATE/DELETE/MERGE. */
                rootRelation = 0; /* there's no separate root rel */
                resultRelations = list_make1_int!((*parse).resultRelation as c_int);
                if (*parse).commandType == CMD_UPDATE {
                    updateColnosLists = list_make1!((*root).update_colnos);
                }
                if !(*parse).withCheckOptions.is_null() {
                    withCheckOptionLists = list_make1!((*parse).withCheckOptions);
                }
                if !(*parse).returningList.is_null() {
                    returningLists = list_make1!((*parse).returningList);
                }
                if !(*parse).mergeActionList.is_null() {
                    mergeActionLists = list_make1!((*parse).mergeActionList);
                }
                if (*parse).commandType == CMD_MERGE {
                    mergeJoinConditions = list_make1!((*parse).mergeJoinCondition);
                }
            }

            /*
             * If there was a FOR [KEY] UPDATE/SHARE clause, the LockRows node
             * will have dealt with fetching non-locked marked rows.
             */
            if !(*parse).rowMarks.is_null() {
                rowMarks = NIL;
            } else {
                rowMarks = (*root).rowMarks;
            }

            path = create_modifytable_path(
                root,
                final_rel,
                path,
                (*parse).commandType as c_int,
                (*parse).canSetTag,
                (*parse).resultRelation as u32,
                rootRelation,
                (*root).partColsUpdated,
                resultRelations,
                updateColnosLists,
                withCheckOptionLists,
                returningLists,
                rowMarks,
                (*parse).onConflict,
                mergeActionLists,
                mergeJoinConditions,
                assign_special_exec_param(root),
            );
        }

        /* And shove it into final_rel */
        add_path(final_rel, path);
    });

    /*
     * Generate partial paths for final_rel, too, if outer query levels might
     * be able to make use of them.
     */
    if (*final_rel).consider_parallel && (*root).query_level > 1 && !limit_needed(parse) {
        Assert!((*parse).rowMarks.is_null() && (*parse).commandType == CMD_SELECT);
        foreach!(lc, (*current_rel).partial_pathlist, {
            let partial_path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;

            add_partial_path(final_rel, partial_path);
        });
    }

    extra.limit_needed = limit_needed(parse);
    extra.limit_tuples = limit_tuples;
    extra.count_est = count_est;
    extra.offset_est = offset_est;

    /*
     * If there is an FDW that's responsible for all baserels of the query,
     * let it consider adding ForeignPaths.
     */
    if !(*final_rel).fdwroutine.is_null() && fdw_has_GetForeignUpperPaths((*final_rel).fdwroutine) {
        fdw_GetForeignUpperPaths(
            (*final_rel).fdwroutine,
            root,
            UPPERREL_FINAL,
            current_rel,
            final_rel,
            &raw mut extra as *mut c_void,
        );
    }

    /* Let extensions possibly add some more paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_FINAL, current_rel, final_rel, &raw mut extra as *mut c_void);
    }

    /* Note: currently, we leave it to callers to do set_cheapest() */
}

// helpers for upper-rel target linitial
unsafe fn linitial_node_pathtarget(list: *mut List) -> *mut PathTarget {
    linitial(list) as *mut PathTarget
}

// TODO(pg-port): real equal() lives in nodes/equalfuncs.rs (deferred).
unsafe fn equal(_a: *mut c_void, _b: *mut c_void) -> bool { false }

// FDW routine helpers (foreign/fdwapi.h). TODO(pg-port): real FdwRoutine struct.
unsafe fn fdw_has_GetForeignUpperPaths(_fdwroutine: *mut FdwRoutine) -> bool { false }
unsafe fn fdw_GetForeignUpperPaths(
    _fdwroutine: *mut FdwRoutine,
    _root: *mut PlannerInfo,
    _stage: UpperRelationKind,
    _input_rel: *mut RelOptInfo,
    _output_rel: *mut RelOptInfo,
    _extra: *mut c_void,
) {}

/*
 * Do preprocessing for groupingSets clause and related data.
 */
unsafe fn preprocess_grouping_sets(root: *mut PlannerInfo) -> *mut grouping_sets_data {
    let parse: *mut Query = (*root).parse;
    let sets: *mut List;
    let mut maxref: c_int = 0;
    let mut lc_set: *mut ListCell;
    let gd: *mut grouping_sets_data = palloc0(std::mem::size_of::<grouping_sets_data>()) as *mut grouping_sets_data;

    /*
     * We don't currently make any attempt to optimize the groupClause when
     * there are grouping sets, so just duplicate it in processed_groupClause.
     */
    (*root).processed_groupClause = (*parse).groupClause;

    /* Detect unhashable and unsortable grouping expressions */
    (*gd).any_hashable = false;
    (*gd).unhashable_refs = ptr::null_mut();
    (*gd).unsortable_refs = ptr::null_mut();
    (*gd).unsortable_sets = NIL;
    (*gd).rollups = NIL;
    (*gd).hash_sets_idx = NIL;
    (*gd).dNumHashGroups = 0.0;

    if !(*parse).groupClause.is_null() {
        let mut lc: *mut ListCell;

        foreach!(lc, (*parse).groupClause, {
            let gc: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(lc));
            let r#ref: Index = (*gc).tleSortGroupRef;

            if r#ref as c_int > maxref {
                maxref = r#ref as c_int;
            }

            if !(*gc).hashable {
                (*gd).unhashable_refs = bms_add_member((*gd).unhashable_refs, r#ref as c_int);
            }

            if !OidIsValid((*gc).sortop) {
                (*gd).unsortable_refs = bms_add_member((*gd).unsortable_refs, r#ref as c_int);
            }
        });
    }

    /* Allocate workspace array for remapping */
    (*gd).tleref_to_colnum_map =
        palloc(((maxref + 1) as usize) * std::mem::size_of::<c_int>()) as *mut c_int;

    /*
     * If we have any unsortable sets, we must extract them before trying to
     * prepare rollups.
     */
    if !bms_is_empty((*gd).unsortable_refs) {
        let mut sortable_sets: *mut List = NIL;
        let mut lc: *mut ListCell;

        foreach!(lc, (*parse).groupingSets, {
            let gset: *mut List = lfirst(current_cell!(lc)) as *mut List;

            if bms_overlap_list((*gd).unsortable_refs, gset) {
                let gs: *mut GroupingSetData = makeNode!(GroupingSetData, T_GroupingSetData);

                (*gs).set = gset;
                (*gd).unsortable_sets = lappend((*gd).unsortable_sets, gs as *mut c_void);

                /*
                 * We must enforce here that an unsortable set is hashable.
                 */
                if bms_overlap_list((*gd).unhashable_refs, gset) {
                    ereport!(ERROR, errmsg!("could not implement GROUP BY"));
                    unreachable!();
                }
            } else {
                sortable_sets = lappend(sortable_sets, gset as *mut c_void);
            }
        });

        if !sortable_sets.is_null() {
            sets = extract_rollup_sets(sortable_sets);
        } else {
            sets = NIL;
        }
    } else {
        sets = extract_rollup_sets((*parse).groupingSets);
    }

    foreach!(lc_set, sets, {
        let mut current_sets: *mut List = lfirst(current_cell!(lc_set)) as *mut List;
        let rollup: *mut RollupData = makeNode!(RollupData, T_RollupData);
        let gs: *mut GroupingSetData;

        /*
         * Reorder the current list of grouping sets into correct prefix order.
         */
        current_sets = reorder_grouping_sets(
            current_sets,
            if list_length(sets) == 1 { (*parse).sortClause } else { NIL },
        );

        /*
         * Get the initial (and therefore largest) grouping set.
         */
        gs = lfirst_node!(GroupingSetData, T_GroupingSetData, list_head(current_sets));

        /*
         * Order the groupClause appropriately.
         */
        if !(*gs).set.is_null() {
            (*rollup).groupClause = preprocess_groupclause(root, (*gs).set);
        } else {
            (*rollup).groupClause = NIL;
        }

        /*
         * Is it hashable?
         */
        if !(*gs).set.is_null() && !bms_overlap_list((*gd).unhashable_refs, (*gs).set) {
            (*rollup).hashable = true;
            (*gd).any_hashable = true;
        }

        /*
         * Remap the entries in the grouping sets from sortgrouprefs to plain
         * indices into the groupClause.
         */
        (*rollup).gsets = remap_to_groupclause_idx(
            (*rollup).groupClause,
            current_sets,
            (*gd).tleref_to_colnum_map,
        );
        (*rollup).gsets_data = current_sets;

        (*gd).rollups = lappend((*gd).rollups, rollup as *mut c_void);
    });

    if !(*gd).unsortable_sets.is_null() {
        /*
         * Construct hash_sets_idx based on the entire original groupclause.
         */
        (*gd).hash_sets_idx = remap_to_groupclause_idx(
            (*parse).groupClause,
            (*gd).unsortable_sets,
            (*gd).tleref_to_colnum_map,
        );
        (*gd).any_hashable = true;
    }

    gd
}

/*
 * Given a groupclause and a list of GroupingSetData, return equivalent sets
 * (without annotation) mapped to indexes into the given groupclause.
 */
unsafe fn remap_to_groupclause_idx(
    groupClause: *mut List,
    gsets: *mut List,
    tleref_to_colnum_map: *mut c_int,
) -> *mut List {
    let mut r#ref: c_int = 0;
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;

    foreach!(lc, groupClause, {
        let gc: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(lc));

        *tleref_to_colnum_map.add((*gc).tleSortGroupRef as usize) = r#ref;
        r#ref += 1;
    });

    foreach!(lc, gsets, {
        let mut set: *mut List = NIL;
        let mut lc2: *mut ListCell;
        let gs: *mut GroupingSetData = lfirst_node!(GroupingSetData, T_GroupingSetData, current_cell!(lc));

        foreach!(lc2, (*gs).set, {
            set = lappend_int(set, *tleref_to_colnum_map.add(lfirst_int(current_cell!(lc2)) as usize));
        });

        result = lappend(result, set as *mut c_void);
    });

    result
}

/*
 * preprocess_rowmarks - set up PlanRowMarks if needed
 */
unsafe fn preprocess_rowmarks(root: *mut PlannerInfo) {
    let parse: *mut Query = (*root).parse;
    let mut rels: *mut Bitmapset;
    let mut prowmarks: *mut List;
    let mut l: *mut ListCell;
    let mut i: c_int;

    if !(*parse).rowMarks.is_null() {
        /*
         * We've got trouble if FOR [KEY] UPDATE/SHARE appears inside grouping.
         */
        CheckSelectLocking(
            parse,
            (*lfirst_node!(RowMarkClause, T_RowMarkClause, list_head((*parse).rowMarks))).strength,
        );
    } else {
        /*
         * We only need rowmarks for UPDATE, DELETE, MERGE, or FOR [KEY]
         * UPDATE/SHARE.
         */
        if (*parse).commandType != CMD_UPDATE
            && (*parse).commandType != CMD_DELETE
            && (*parse).commandType != CMD_MERGE
        {
            return;
        }
    }

    /*
     * We need to have rowmarks for all base relations except the target.
     */
    rels = get_relids_in_jointree((*parse).jointree as *mut Node, false, false);
    if (*parse).resultRelation != 0 {
        rels = bms_del_member(rels, (*parse).resultRelation as c_int);
    }

    /*
     * Convert RowMarkClauses to PlanRowMark representation.
     */
    prowmarks = NIL;
    foreach!(l, (*parse).rowMarks, {
        let rc: *mut RowMarkClause = lfirst_node!(RowMarkClause, T_RowMarkClause, current_cell!(l));
        let rte: *mut RangeTblEntry = rt_fetch((*rc).rti, (*parse).rtable);
        let newrc: *mut PlanRowMark;

        Assert!((*rc).rti != (*parse).resultRelation as u32);

        /*
         * Ignore RowMarkClauses for subqueries.
         */
        if (*rte).rtekind != RTE_RELATION {
            continue;
        }

        rels = bms_del_member(rels, (*rc).rti as c_int);

        newrc = makeNode!(PlanRowMark, T_PlanRowMark);
        (*newrc).rti = (*rc).rti;
        (*newrc).prti = (*rc).rti;
        (*(*root).glob).lastRowMarkId += 1;
        (*newrc).rowmarkId = (*(*root).glob).lastRowMarkId;
        (*newrc).markType = select_rowmark_type(rte, (*rc).strength);
        (*newrc).allMarkTypes = 1 << ((*newrc).markType as c_int);
        (*newrc).strength = (*rc).strength;
        (*newrc).waitPolicy = (*rc).waitPolicy;
        (*newrc).isParent = false;

        prowmarks = lappend(prowmarks, newrc as *mut c_void);
    });

    /*
     * Now, add rowmarks for any non-target, non-locked base relations.
     */
    i = 0;
    foreach!(l, (*parse).rtable, {
        let rte: *mut RangeTblEntry = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(l));
        let newrc: *mut PlanRowMark;

        i += 1;
        if !bms_is_member(i, rels) {
            continue;
        }

        newrc = makeNode!(PlanRowMark, T_PlanRowMark);
        (*newrc).rti = i as Index;
        (*newrc).prti = i as Index;
        (*(*root).glob).lastRowMarkId += 1;
        (*newrc).rowmarkId = (*(*root).glob).lastRowMarkId;
        (*newrc).markType = select_rowmark_type(rte, LCS_NONE);
        (*newrc).allMarkTypes = 1 << ((*newrc).markType as c_int);
        (*newrc).strength = LCS_NONE;
        (*newrc).waitPolicy = LockWaitBlock; /* doesn't matter */
        (*newrc).isParent = false;

        prowmarks = lappend(prowmarks, newrc as *mut c_void);
    });

    (*root).rowMarks = prowmarks;
}

/*
 * Select RowMarkType to use for a given table
 */
pub unsafe fn select_rowmark_type(rte: *mut RangeTblEntry, strength: LockClauseStrength) -> RowMarkType {
    if (*rte).rtekind != RTE_RELATION {
        /* If it's not a table at all, use ROW_MARK_COPY */
        ROW_MARK_COPY
    } else if (*rte).relkind == RELKIND_FOREIGN_TABLE {
        /* Let the FDW select the rowmark type, if it wants to */
        let fdwroutine: *mut FdwRoutine = GetFdwRoutineByRelId((*rte).relid);

        if fdw_has_GetForeignRowMarkType(fdwroutine) {
            fdw_GetForeignRowMarkType(fdwroutine, rte, strength)
        } else {
            /* Otherwise, use ROW_MARK_COPY by default */
            ROW_MARK_COPY
        }
    } else {
        /* Regular table, apply the appropriate lock type */
        match strength {
            LCS_NONE => {
                /*
                 * We don't need a tuple lock, only the ability to re-fetch the row.
                 */
                ROW_MARK_REFERENCE
            }
            LCS_FORKEYSHARE => ROW_MARK_KEYSHARE,
            LCS_FORSHARE => ROW_MARK_SHARE,
            LCS_FORNOKEYUPDATE => ROW_MARK_NOKEYEXCLUSIVE,
            LCS_FORUPDATE => ROW_MARK_EXCLUSIVE,
            _ => {
                elog!(ERROR, "unrecognized LockClauseStrength {}", strength as c_int);
                ROW_MARK_EXCLUSIVE /* keep compiler quiet */
            }
        }
    }
}

unsafe fn fdw_has_GetForeignRowMarkType(_fdwroutine: *mut FdwRoutine) -> bool { false }
unsafe fn fdw_GetForeignRowMarkType(_fdwroutine: *mut FdwRoutine, _rte: *mut RangeTblEntry, _strength: LockClauseStrength) -> RowMarkType { ROW_MARK_COPY }

/*
 * preprocess_limit - do pre-estimation for LIMIT and/or OFFSET clauses
 */
unsafe fn preprocess_limit(
    root: *mut PlannerInfo,
    mut tuple_fraction: f64,
    offset_est: *mut int64,
    count_est: *mut int64,
) -> f64 {
    let parse: *mut Query = (*root).parse;
    let est: *mut Node;
    let mut limit_fraction: f64;

    /* Should not be called unless LIMIT or OFFSET */
    Assert!(!(*parse).limitCount.is_null() || !(*parse).limitOffset.is_null());

    /*
     * Try to obtain the clause values.
     */
    if !(*parse).limitCount.is_null() {
        est = estimate_expression_value(root, (*parse).limitCount);
        if !est.is_null() && IsA!(est, T_Const) {
            if (*(est as *mut Const)).constisnull {
                /* NULL indicates LIMIT ALL, ie, no limit */
                *count_est = 0; /* treat as not present */
            } else {
                *count_est = DatumGetInt64((*(est as *mut Const)).constvalue);
                if *count_est <= 0 {
                    *count_est = 1; /* force to at least 1 */
                }
            }
        } else {
            *count_est = -1; /* can't estimate */
        }
    } else {
        *count_est = 0; /* not present */
    }

    if !(*parse).limitOffset.is_null() {
        let est2 = estimate_expression_value(root, (*parse).limitOffset);
        if !est2.is_null() && IsA!(est2, T_Const) {
            if (*(est2 as *mut Const)).constisnull {
                /* Treat NULL as no offset; the executor will too */
                *offset_est = 0; /* treat as not present */
            } else {
                *offset_est = DatumGetInt64((*(est2 as *mut Const)).constvalue);
                if *offset_est < 0 {
                    *offset_est = 0; /* treat as not present */
                }
            }
        } else {
            *offset_est = -1; /* can't estimate */
        }
    } else {
        *offset_est = 0; /* not present */
    }

    if *count_est != 0 {
        /*
         * A LIMIT clause limits the absolute number of tuples returned.
         */
        if *count_est < 0 || *offset_est < 0 {
            /* LIMIT or OFFSET is an expression ... punt ... */
            limit_fraction = 0.10;
        } else {
            /* LIMIT (plus OFFSET, if any) is max number of tuples needed */
            limit_fraction = *count_est as f64 + *offset_est as f64;
        }

        /*
         * If we have absolute limits from both caller and LIMIT, use the
         * smaller value; likewise if they are both fractional.
         */
        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                /* both absolute */
                tuple_fraction = Min_f64(tuple_fraction, limit_fraction);
            } else {
                /* caller absolute, limit fractional; use caller's value */
            }
        } else if tuple_fraction > 0.0 {
            if limit_fraction >= 1.0 {
                /* caller fractional, limit absolute; use limit */
                tuple_fraction = limit_fraction;
            } else {
                /* both fractional */
                tuple_fraction = Min_f64(tuple_fraction, limit_fraction);
            }
        } else {
            /* no info from caller, just use limit */
            tuple_fraction = limit_fraction;
        }
    } else if *offset_est != 0 && tuple_fraction > 0.0 {
        /*
         * We have an OFFSET but no LIMIT.
         */
        if *offset_est < 0 {
            limit_fraction = 0.10;
        } else {
            limit_fraction = *offset_est as f64;
        }

        /*
         * If we have absolute counts from both caller and OFFSET, add them.
         */
        if tuple_fraction >= 1.0 {
            if limit_fraction >= 1.0 {
                /* both absolute, so add them together */
                tuple_fraction += limit_fraction;
            } else {
                /* caller absolute, limit fractional; use limit */
                tuple_fraction = limit_fraction;
            }
        } else {
            if limit_fraction >= 1.0 {
                /* caller fractional, limit absolute; use caller's value */
            } else {
                /* both fractional, so add them together */
                tuple_fraction += limit_fraction;
                if tuple_fraction >= 1.0 {
                    tuple_fraction = 0.0; /* assume fetch all */
                }
            }
        }
    }

    tuple_fraction
}

/*
 * limit_needed - do we actually need a Limit plan node?
 */
pub unsafe fn limit_needed(parse: *mut Query) -> bool {
    let mut node: *mut Node;

    node = (*parse).limitCount;
    if !node.is_null() {
        if IsA!(node, T_Const) {
            /* NULL indicates LIMIT ALL, ie, no limit */
            if !(*(node as *mut Const)).constisnull {
                return true; /* LIMIT with a constant value */
            }
        } else {
            return true; /* non-constant LIMIT */
        }
    }

    node = (*parse).limitOffset;
    if !node.is_null() {
        if IsA!(node, T_Const) {
            /* Treat NULL as no offset; the executor would too */
            if !(*(node as *mut Const)).constisnull {
                let offset: int64 = DatumGetInt64((*(node as *mut Const)).constvalue);

                if offset != 0 {
                    return true; /* OFFSET with a nonzero value */
                }
            }
        } else {
            return true; /* non-constant OFFSET */
        }
    }

    false /* don't need a Limit plan node */
}

/*
 * preprocess_groupclause - do preparatory work on GROUP BY clause
 */
unsafe fn preprocess_groupclause(root: *mut PlannerInfo, force: *mut List) -> *mut List {
    let parse: *mut Query = (*root).parse;
    let mut new_groupclause: *mut List = NIL;
    let mut sl: *mut ListCell;
    let mut gl: *mut ListCell;

    /* For grouping sets, we need to force the ordering */
    if !force.is_null() {
        foreach!(sl, force, {
            let r#ref: Index = lfirst_int(current_cell!(sl)) as Index;
            let cl: *mut SortGroupClause = get_sortgroupref_clause(r#ref, (*parse).groupClause);

            new_groupclause = lappend(new_groupclause, cl as *mut c_void);
        });

        return new_groupclause;
    }

    /* If no ORDER BY, nothing useful to do here */
    if (*parse).sortClause == NIL {
        return list_copy((*parse).groupClause);
    }

    /*
     * Scan the ORDER BY clause and construct a list of matching GROUP BY items.
     */
    let mut gl_was_null = false;
    foreach!(sl, (*parse).sortClause, {
        let sc: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(sl));
        let mut matched = false;

        gl = list_head((*parse).groupClause);
        while !gl.is_null() {
            let gc: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, gl);

            if equal(gc as *mut c_void, sc as *mut c_void) {
                new_groupclause = lappend(new_groupclause, gc as *mut c_void);
                matched = true;
                break;
            }
            gl = lnext((*parse).groupClause, gl);
        }
        if gl.is_null() {
            gl_was_null = true;
            break; /* no match, so stop scanning */
        }
    });

    /* If no match at all, no point in reordering GROUP BY */
    if new_groupclause == NIL {
        return list_copy((*parse).groupClause);
    }

    /*
     * Add any remaining GROUP BY items to the new list.
     */
    foreach!(gl, (*parse).groupClause, {
        let gc: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(gl));

        if list_member_ptr(new_groupclause, gc as *mut c_void) {
            continue; /* it matched an ORDER BY item */
        }
        if !OidIsValid((*gc).sortop) {
            /* give up, GROUP BY can't be sorted */
            return list_copy((*parse).groupClause);
        }
        new_groupclause = lappend(new_groupclause, gc as *mut c_void);
    });

    /* Success --- install the rearranged GROUP BY list */
    Assert!(list_length((*parse).groupClause) == list_length(new_groupclause));
    new_groupclause
}

/*
 * Extract lists of grouping sets that can be implemented using a single
 * rollup-type aggregate pass each. Returns a list of lists of grouping sets.
 */
unsafe fn extract_rollup_sets(groupingSets: *mut List) -> *mut List {
    let num_sets_raw: c_int = list_length(groupingSets);
    let mut num_empty: c_int = 0;
    let mut num_sets: c_int; /* distinct sets */
    let mut num_chains: c_int = 0;
    let mut result: *mut List = NIL;
    let results: *mut *mut List;
    let orig_sets: *mut *mut List;
    let set_masks: *mut *mut Bitmapset;
    let chains: *mut c_int;
    let adjacency: *mut *mut i16;
    let adjacency_buf: *mut i16;
    let state: *mut BipartiteMatchState;
    let mut i: c_int;
    let mut j: c_int;
    let mut j_size: c_int;
    let mut lc1: *mut ListCell = list_head(groupingSets);

    /*
     * Start by stripping out empty sets.
     */
    while !lc1.is_null() && (lfirst(lc1) as *mut List) == NIL {
        num_empty += 1;
        lc1 = lnext(groupingSets, lc1);
    }

    /* bail out now if it turns out that all we had were empty sets. */
    if lc1.is_null() {
        return list_make1!(groupingSets);
    }

    orig_sets =
        palloc0(((num_sets_raw + 1) as usize) * std::mem::size_of::<*mut List>()) as *mut *mut List;
    set_masks =
        palloc0(((num_sets_raw + 1) as usize) * std::mem::size_of::<*mut Bitmapset>()) as *mut *mut Bitmapset;
    adjacency =
        palloc0(((num_sets_raw + 1) as usize) * std::mem::size_of::<*mut i16>()) as *mut *mut i16;
    adjacency_buf = palloc(((num_sets_raw + 1) as usize) * std::mem::size_of::<i16>()) as *mut i16;

    j_size = 0;
    j = 0;
    i = 1;

    for lc in for_each_cell_iter(groupingSets, lc1) {
        let candidate: *mut List = lfirst(lc) as *mut List;
        let mut candidate_set: *mut Bitmapset = ptr::null_mut();
        let mut lc2: *mut ListCell;
        let mut dup_of: c_int = 0;

        foreach!(lc2, candidate, {
            candidate_set = bms_add_member(candidate_set, lfirst_int(current_cell!(lc2)));
        });

        /* we can only be a dup if we're the same length as a previous set */
        if j_size == list_length(candidate) {
            let mut k: c_int = j;
            while k < i {
                if bms_equal(*set_masks.add(k as usize), candidate_set) {
                    dup_of = k;
                    break;
                }
                k += 1;
            }
        } else if j_size < list_length(candidate) {
            j_size = list_length(candidate);
            j = i;
        }

        if dup_of > 0 {
            *orig_sets.add(dup_of as usize) = lappend(*orig_sets.add(dup_of as usize), candidate as *mut c_void);
            bms_free(candidate_set);
        } else {
            let mut k: c_int;
            let mut n_adj: c_int = 0;

            *orig_sets.add(i as usize) = list_make1!(candidate);
            *set_masks.add(i as usize) = candidate_set;

            /* fill in adjacency list; no need to compare equal-size sets */
            k = j - 1;
            while k > 0 {
                if bms_is_subset(*set_masks.add(k as usize), candidate_set) {
                    n_adj += 1;
                    *adjacency_buf.add(n_adj as usize) = k as i16;
                }
                k -= 1;
            }

            if n_adj > 0 {
                *adjacency_buf.add(0) = n_adj as i16;
                *adjacency.add(i as usize) =
                    palloc(((n_adj + 1) as usize) * std::mem::size_of::<i16>()) as *mut i16;
                ptr::copy_nonoverlapping(
                    adjacency_buf,
                    *adjacency.add(i as usize),
                    (n_adj + 1) as usize,
                );
            } else {
                *adjacency.add(i as usize) = ptr::null_mut();
            }

            i += 1;
        }
    }

    num_sets = i - 1;

    /*
     * Apply the graph matching algorithm to do the work.
     */
    state = BipartiteMatch(num_sets, num_sets, adjacency);

    /*
     * Now, the state->pair* fields have the info we need to assign sets to chains.
     */
    chains = palloc0(((num_sets + 1) as usize) * std::mem::size_of::<c_int>()) as *mut c_int;

    i = 1;
    while i <= num_sets {
        let u: c_int = *(*state).pair_vu.add(i as usize);
        let v: c_int = *(*state).pair_uv.add(i as usize);

        if u > 0 && u < i {
            *chains.add(i as usize) = *chains.add(u as usize);
        } else if v > 0 && v < i {
            *chains.add(i as usize) = *chains.add(v as usize);
        } else {
            num_chains += 1;
            *chains.add(i as usize) = num_chains;
        }
        i += 1;
    }

    /* build result lists. */
    results = palloc0(((num_chains + 1) as usize) * std::mem::size_of::<*mut List>()) as *mut *mut List;

    i = 1;
    while i <= num_sets {
        let c: c_int = *chains.add(i as usize);

        Assert!(c > 0);

        *results.add(c as usize) = list_concat(*results.add(c as usize), *orig_sets.add(i as usize));
        i += 1;
    }

    /* push any empty sets back on the first list. */
    while num_empty > 0 {
        num_empty -= 1;
        *results.add(1) = lcons(NIL as *mut c_void, *results.add(1));
    }

    /* make result list */
    i = 1;
    while i <= num_chains {
        result = lappend(result, *results.add(i as usize) as *mut c_void);
        i += 1;
    }

    /*
     * Free all the things.
     */
    BipartiteMatchFree(state);
    pfree(results as *mut c_void);
    pfree(chains as *mut c_void);
    i = 1;
    while i <= num_sets {
        if !(*adjacency.add(i as usize)).is_null() {
            pfree(*adjacency.add(i as usize) as *mut c_void);
        }
        i += 1;
    }
    pfree(adjacency as *mut c_void);
    pfree(adjacency_buf as *mut c_void);
    pfree(orig_sets as *mut c_void);
    i = 1;
    while i <= num_sets {
        bms_free(*set_masks.add(i as usize));
        i += 1;
    }
    pfree(set_masks as *mut c_void);

    result
}

/*
 * Reorder the elements of a list of grouping sets such that they have correct
 * prefix relationships. Also inserts the GroupingSetData annotations.
 */
unsafe fn reorder_grouping_sets(groupingSets: *mut List, mut sortclause: *mut List) -> *mut List {
    let mut lc: *mut ListCell;
    let mut previous: *mut List = NIL;
    let mut result: *mut List = NIL;

    foreach!(lc, groupingSets, {
        let candidate: *mut List = lfirst(current_cell!(lc)) as *mut List;
        let mut new_elems: *mut List = list_difference_int(candidate, previous);
        let gs: *mut GroupingSetData = makeNode!(GroupingSetData, T_GroupingSetData);

        while list_length(sortclause) > list_length(previous) && new_elems != NIL {
            let sc: *mut SortGroupClause = list_nth(sortclause, list_length(previous)) as *mut SortGroupClause;
            let r#ref: c_int = (*sc).tleSortGroupRef as c_int;

            if list_member_int(new_elems, r#ref) {
                previous = lappend_int(previous, r#ref);
                new_elems = list_delete_int(new_elems, r#ref);
            } else {
                /* diverged from the sortclause; give up on it */
                sortclause = NIL;
                break;
            }
        }

        previous = list_concat(previous, new_elems);

        (*gs).set = list_copy(previous);
        result = lcons(gs as *mut c_void, result);
    });

    list_free(previous);

    result
}

/*
 * has_volatile_pathkey
 */
unsafe fn has_volatile_pathkey(keys: *mut List) -> bool {
    let mut lc: *mut ListCell;

    foreach!(lc, keys, {
        let pathkey: *mut PathKey = lfirst_node!(PathKey, T_PathKey, current_cell!(lc));

        if (*(*pathkey).pk_eclass).ec_has_volatile {
            return true;
        }
    });

    false
}

/*
 * adjust_group_pathkeys_for_groupagg
 */
unsafe fn adjust_group_pathkeys_for_groupagg(root: *mut PlannerInfo) {
    let grouppathkeys: *mut List = (*root).group_pathkeys;
    let mut bestpathkeys: *mut List;
    let mut bestaggs: *mut Bitmapset;
    let mut unprocessed_aggs: *mut Bitmapset;
    let mut lc: *mut ListCell;
    let mut i: c_int;

    /* Shouldn't be here if there are grouping sets */
    Assert!((*(*root).parse).groupingSets == NIL);
    /* Shouldn't be here unless there are some ordered aggregates */
    Assert!((*root).numOrderedAggs > 0);

    /* Do nothing if disabled */
    if !enable_presorted_aggregate {
        return;
    }

    /*
     * Make a first pass over all AggInfos to collect a Bitmapset.
     */
    unprocessed_aggs = ptr::null_mut();
    foreach!(lc, (*root).agginfos, {
        let agginfo: *mut AggInfo = lfirst_node!(AggInfo, T_AggInfo, current_cell!(lc));
        let aggref: *mut Aggref = linitial_node_aggref((*agginfo).aggrefs);

        if AGGKIND_IS_ORDERED_SET((*aggref).aggkind) {
            continue;
        }

        /* Skip unless there's a DISTINCT or ORDER BY clause */
        if (*aggref).aggdistinct == NIL && (*aggref).aggorder == NIL {
            continue;
        }

        /* Additional safety checks are needed if there's a FILTER clause */
        if !(*aggref).aggfilter.is_null() {
            let mut lc2: *mut ListCell;
            let mut allow_presort: bool = true;

            foreach!(lc2, (*aggref).args, {
                let tle: *mut TargetEntry = lfirst(current_cell!(lc2)) as *mut TargetEntry;
                let mut expr: *mut Expr = (*tle).expr;

                while IsA!(expr, T_RelabelType) {
                    expr = (*(castNode!(RelabelType, T_RelabelType, expr))).arg;
                }

                /* Common case, Vars and Consts are ok */
                if IsA!(expr, T_Var) || IsA!(expr, T_Const) {
                    continue;
                }

                /* Unsupported.  Don't try to presort for this Aggref */
                allow_presort = false;
                break;
            });

            /* Skip unsupported Aggrefs */
            if !allow_presort {
                continue;
            }
        }

        unprocessed_aggs = bms_add_member(unprocessed_aggs, foreach_current_index_lc(current_cell!(lc), (*root).agginfos));
    });

    /*
     * Now process all the unprocessed_aggs to find the best set of pathkeys.
     */
    bestpathkeys = NIL;
    bestaggs = ptr::null_mut();
    while bms_num_members(unprocessed_aggs) > bms_num_members(bestaggs) {
        let mut aggindexes: *mut Bitmapset = ptr::null_mut();
        let mut currpathkeys: *mut List = NIL;

        i = -1;
        loop {
            i = bms_next_member(unprocessed_aggs, i);
            if i < 0 {
                break;
            }
            let agginfo: *mut AggInfo = list_nth_node_aggref_info((*root).agginfos, i);
            let aggref: *mut Aggref = linitial_node_aggref((*agginfo).aggrefs);
            let sortlist: *mut List;
            let mut pathkeys: *mut List;

            if (*aggref).aggdistinct != NIL {
                sortlist = (*aggref).aggdistinct;
            } else {
                sortlist = (*aggref).aggorder;
            }

            pathkeys = make_pathkeys_for_sortclauses(root, sortlist, (*aggref).args);

            /*
             * Ignore Aggrefs which have volatile functions in their ORDER BY
             * or DISTINCT clause.
             */
            if has_volatile_pathkey(pathkeys) {
                unprocessed_aggs = bms_del_member(unprocessed_aggs, i);
                continue;
            }

            /*
             * When not set yet, take the pathkeys from the first unprocessed aggregate.
             */
            if currpathkeys == NIL {
                currpathkeys = pathkeys;

                /* include the GROUP BY pathkeys, if they exist */
                if grouppathkeys != NIL {
                    currpathkeys = append_pathkeys(list_copy(grouppathkeys), currpathkeys);
                }

                /* record that we found pathkeys for this aggregate */
                aggindexes = bms_add_member(aggindexes, i);
            } else {
                /* now look for a stronger set of matching pathkeys */

                /* include the GROUP BY pathkeys, if they exist */
                if grouppathkeys != NIL {
                    pathkeys = append_pathkeys(list_copy(grouppathkeys), pathkeys);
                }

                /* are 'pathkeys' compatible or better than 'currpathkeys'? */
                match compare_pathkeys(currpathkeys, pathkeys) {
                    PATHKEYS_BETTER2 => {
                        /* 'pathkeys' are stronger, use these ones instead */
                        currpathkeys = pathkeys;
                        /* FALLTHROUGH */
                        aggindexes = bms_add_member(aggindexes, i);
                    }
                    PATHKEYS_BETTER1 => {
                        /* 'pathkeys' are less strict */
                        /* FALLTHROUGH */
                        aggindexes = bms_add_member(aggindexes, i);
                    }
                    PATHKEYS_EQUAL => {
                        /* mark this aggregate as covered by 'currpathkeys' */
                        aggindexes = bms_add_member(aggindexes, i);
                    }
                    PATHKEYS_DIFFERENT => {}
                    _ => {}
                }
            }
        }

        /* remove the aggregates that we've just processed */
        unprocessed_aggs = bms_del_members(unprocessed_aggs, aggindexes);

        /*
         * If this pass included more aggregates than the previous best then
         * use these ones as the best set.
         */
        if bms_num_members(aggindexes) > bms_num_members(bestaggs) {
            bestaggs = aggindexes;
            bestpathkeys = currpathkeys;
        }
    }

    /*
     * If we found any ordered aggregates, update root->group_pathkeys.
     */
    if bestpathkeys != NIL {
        (*root).group_pathkeys = bestpathkeys;
    }

    /*
     * Now that we've found the best set of aggregates we can set the
     * presorted flag.
     */
    i = -1;
    loop {
        i = bms_next_member(bestaggs, i);
        if i < 0 {
            break;
        }
        let agginfo: *mut AggInfo = list_nth_node_aggref_info((*root).agginfos, i);

        foreach!(lc, (*agginfo).aggrefs, {
            let aggref: *mut Aggref = lfirst_node!(Aggref, T_Aggref, current_cell!(lc));

            (*aggref).aggpresorted = true;
        });
    }
}

unsafe fn linitial_node_aggref(list: *mut List) -> *mut Aggref {
    linitial(list) as *mut Aggref
}
unsafe fn list_nth_node_aggref_info(list: *mut List, n: c_int) -> *mut AggInfo {
    list_nth(list, n) as *mut AggInfo
}
unsafe fn foreach_current_index_lc(cell: *mut ListCell, list: *mut List) -> c_int {
    // index of cell within list
    let mut idx = 0;
    let mut c = list_head(list);
    while !c.is_null() {
        if c == cell {
            return idx;
        }
        idx += 1;
        c = lnext(list, c);
    }
    idx
}

/*
 * Compute query_pathkeys and other pathkeys during plan generation
 */
unsafe fn standard_qp_callback(root: *mut PlannerInfo, extra: *mut c_void) {
    let parse: *mut Query = (*root).parse;
    let qp_extra: *mut standard_qp_extra = extra as *mut standard_qp_extra;
    let tlist: *mut List = (*root).processed_tlist;
    let activeWindows: *mut List = (*qp_extra).activeWindows;

    /*
     * Calculate pathkeys that represent grouping/ordering and/or ordered
     * aggregate requirements.
     */
    if !(*qp_extra).gset_data.is_null() {
        /*
         * With grouping sets, just use the first RollupData's groupClause.
         */
        let rollups: *mut List = (*(*qp_extra).gset_data).rollups;
        let mut groupClause: *mut List = if !rollups.is_null() {
            (*lfirst_node!(RollupData, T_RollupData, list_head(rollups))).groupClause
        } else {
            NIL
        };

        if grouping_is_sortable(groupClause) {
            let mut sortable: bool = false;

            (*root).group_pathkeys = make_pathkeys_for_sortclauses_extended(
                root,
                &raw mut groupClause,
                tlist,
                false,
                (*parse).hasGroupRTE,
                &raw mut sortable,
                false,
            );
            Assert!(sortable);
            (*root).num_groupby_pathkeys = list_length((*root).group_pathkeys);
        } else {
            (*root).group_pathkeys = NIL;
            (*root).num_groupby_pathkeys = 0;
        }
    } else if !(*parse).groupClause.is_null() || (*root).numOrderedAggs > 0 {
        /*
         * With a plain GROUP BY list, we can remove redundant grouping items.
         */
        let mut sortable: bool = false;

        (*root).group_pathkeys = make_pathkeys_for_sortclauses_extended(
            root,
            &raw mut (*root).processed_groupClause,
            tlist,
            true,
            false,
            &raw mut sortable,
            true,
        );
        if !sortable {
            /* Can't sort; no point in considering aggregate ordering either */
            (*root).group_pathkeys = NIL;
            (*root).num_groupby_pathkeys = 0;
        } else {
            (*root).num_groupby_pathkeys = list_length((*root).group_pathkeys);
            /* If we have ordered aggs, consider adding onto group_pathkeys */
            if (*root).numOrderedAggs > 0 {
                adjust_group_pathkeys_for_groupagg(root);
            }
        }
    } else {
        (*root).group_pathkeys = NIL;
        (*root).num_groupby_pathkeys = 0;
    }

    /* We consider only the first (bottom) window in pathkeys logic */
    if activeWindows != NIL {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, list_head(activeWindows));

        (*root).window_pathkeys = make_pathkeys_for_window(root, wc, tlist);
    } else {
        (*root).window_pathkeys = NIL;
    }

    /*
     * As with GROUP BY, we can discard any redundant DISTINCT items.
     */
    if !(*parse).distinctClause.is_null() {
        let mut sortable: bool = false;

        /* Make a copy since pathkey processing can modify the list */
        (*root).processed_distinctClause = list_copy((*parse).distinctClause);
        (*root).distinct_pathkeys = make_pathkeys_for_sortclauses_extended(
            root,
            &raw mut (*root).processed_distinctClause,
            tlist,
            true,
            false,
            &raw mut sortable,
            false,
        );
        if !sortable {
            (*root).distinct_pathkeys = NIL;
        }
    } else {
        (*root).distinct_pathkeys = NIL;
    }

    (*root).sort_pathkeys = make_pathkeys_for_sortclauses(root, (*parse).sortClause, tlist);

    /* setting setop_pathkeys might be useful to the union planner */
    if !(*qp_extra).setop.is_null() {
        let mut groupClauses: *mut List;
        let mut sortable: bool = false;

        groupClauses = generate_setop_child_grouplist((*qp_extra).setop, tlist);

        (*root).setop_pathkeys = make_pathkeys_for_sortclauses_extended(
            root,
            &raw mut groupClauses,
            tlist,
            false,
            false,
            &raw mut sortable,
            false,
        );
        if !sortable {
            (*root).setop_pathkeys = NIL;
        }
    } else {
        (*root).setop_pathkeys = NIL;
    }

    /*
     * Figure out whether we want a sorted result from query_planner.
     */
    if !(*root).group_pathkeys.is_null() {
        (*root).query_pathkeys = (*root).group_pathkeys;
    } else if !(*root).window_pathkeys.is_null() {
        (*root).query_pathkeys = (*root).window_pathkeys;
    } else if list_length((*root).distinct_pathkeys) > list_length((*root).sort_pathkeys) {
        (*root).query_pathkeys = (*root).distinct_pathkeys;
    } else if !(*root).sort_pathkeys.is_null() {
        (*root).query_pathkeys = (*root).sort_pathkeys;
    } else if (*root).setop_pathkeys != NIL {
        (*root).query_pathkeys = (*root).setop_pathkeys;
    } else {
        (*root).query_pathkeys = NIL;
    }
}

/*
 * Estimate number of groups produced by grouping clauses (1 if not grouping)
 */
unsafe fn get_number_of_groups(
    root: *mut PlannerInfo,
    path_rows: f64,
    gd: *mut grouping_sets_data,
    target_list: *mut List,
) -> f64 {
    let parse: *mut Query = (*root).parse;
    let mut dNumGroups: f64;

    if !(*parse).groupClause.is_null() {
        let mut groupExprs: *mut List;

        if !(*parse).groupingSets.is_null() {
            /* Add up the estimates for each grouping set */
            let mut lc: *mut ListCell;

            Assert!(!gd.is_null()); /* keep Coverity happy */

            dNumGroups = 0.0;

            foreach!(lc, (*gd).rollups, {
                let rollup: *mut RollupData = lfirst_node!(RollupData, T_RollupData, current_cell!(lc));
                let mut lc2: *mut ListCell;
                let mut lc3: *mut ListCell;

                groupExprs = get_sortgrouplist_exprs((*rollup).groupClause, target_list);

                (*rollup).numGroups = 0.0;

                forboth!(lc2, (*rollup).gsets, lc3, (*rollup).gsets_data, {
                    let mut gset: *mut List = lfirst(lc2) as *mut List;
                    let gs: *mut GroupingSetData = lfirst_node!(GroupingSetData, T_GroupingSetData, lc3);
                    let numGroups: f64 = estimate_num_groups(
                        root,
                        groupExprs,
                        path_rows,
                        &raw mut gset,
                        ptr::null_mut(),
                    );

                    (*gs).numGroups = numGroups;
                    (*rollup).numGroups += numGroups;
                });

                dNumGroups += (*rollup).numGroups;
            });

            if !(*gd).hash_sets_idx.is_null() {
                let mut lc2: *mut ListCell;

                (*gd).dNumHashGroups = 0.0;

                groupExprs = get_sortgrouplist_exprs((*parse).groupClause, target_list);

                forboth!(lc, (*gd).hash_sets_idx, lc2, (*gd).unsortable_sets, {
                    let mut gset: *mut List = lfirst(lc) as *mut List;
                    let gs: *mut GroupingSetData = lfirst_node!(GroupingSetData, T_GroupingSetData, lc2);
                    let numGroups: f64 = estimate_num_groups(
                        root,
                        groupExprs,
                        path_rows,
                        &raw mut gset,
                        ptr::null_mut(),
                    );

                    (*gs).numGroups = numGroups;
                    (*gd).dNumHashGroups += numGroups;
                });

                dNumGroups += (*gd).dNumHashGroups;
            }
        } else {
            /* Plain GROUP BY -- estimate based on optimized groupClause */
            groupExprs = get_sortgrouplist_exprs((*root).processed_groupClause, target_list);

            dNumGroups = estimate_num_groups(root, groupExprs, path_rows, ptr::null_mut(), ptr::null_mut());
        }
    } else if !(*parse).groupingSets.is_null() {
        /* Empty grouping sets ... one result row for each one */
        dNumGroups = list_length((*parse).groupingSets) as f64;
    } else if (*parse).hasAggs || (*root).hasHavingQual {
        /* Plain aggregation, one result row */
        dNumGroups = 1.0;
    } else {
        /* Not grouping */
        dNumGroups = 1.0;
    }

    dNumGroups
}

/*
 * create_grouping_paths
 */
unsafe fn create_grouping_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    target: *mut PathTarget,
    target_parallel_safe: bool,
    gd: *mut grouping_sets_data,
) -> *mut RelOptInfo {
    let parse: *mut Query = (*root).parse;
    let grouped_rel: *mut RelOptInfo;
    let mut partially_grouped_rel: *mut RelOptInfo = ptr::null_mut();
    let mut agg_costs: AggClauseCosts = std::mem::zeroed();

    ptr::write_bytes(&raw mut agg_costs, 0, 1);
    get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &raw mut agg_costs);

    /*
     * Create grouping relation to hold fully aggregated grouping and/or
     * aggregation paths.
     */
    grouped_rel = make_grouping_rel(root, input_rel, target, target_parallel_safe, (*parse).havingQual);

    /*
     * Create either paths for a degenerate grouping or paths for ordinary grouping.
     */
    if is_degenerate_grouping(root) {
        create_degenerate_grouping_paths(root, input_rel, grouped_rel);
    } else {
        let mut flags: c_int = 0;
        let mut extra: GroupPathExtraData = std::mem::zeroed();

        /*
         * Determine whether it's possible to perform sort-based implementations.
         */
        if (!gd.is_null() && (*gd).rollups != NIL)
            || grouping_is_sortable((*root).processed_groupClause)
        {
            flags |= GROUPING_CAN_USE_SORT;
        }

        /*
         * Determine whether we should consider hash-based implementations.
         */
        if !(*parse).groupClause.is_null()
            && (*root).numOrderedAggs == 0
            && (if !gd.is_null() { (*gd).any_hashable } else { grouping_is_hashable((*root).processed_groupClause) })
        {
            flags |= GROUPING_CAN_USE_HASH;
        }

        /*
         * Determine whether partial aggregation is possible.
         */
        if can_partial_agg(root) {
            flags |= GROUPING_CAN_PARTIAL_AGG;
        }

        extra.flags = flags;
        extra.target_parallel_safe = target_parallel_safe;
        extra.havingQual = (*parse).havingQual;
        extra.targetList = (*parse).targetList;
        extra.partial_costs_set = false;

        /*
         * Determine whether partitionwise aggregation is in theory possible.
         */
        if enable_partitionwise_aggregate && (*parse).groupingSets.is_null() {
            extra.patype = PARTITIONWISE_AGGREGATE_FULL;
        } else {
            extra.patype = PARTITIONWISE_AGGREGATE_NONE;
        }

        create_ordinary_grouping_paths(
            root,
            input_rel,
            grouped_rel,
            &raw const agg_costs,
            gd,
            &raw mut extra,
            &raw mut partially_grouped_rel,
        );
    }

    set_cheapest(grouped_rel);
    grouped_rel
}

/*
 * make_grouping_rel
 */
unsafe fn make_grouping_rel(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    target: *mut PathTarget,
    target_parallel_safe: bool,
    havingQual: *mut Node,
) -> *mut RelOptInfo {
    let grouped_rel: *mut RelOptInfo;

    if IS_OTHER_REL(input_rel) {
        grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, (*input_rel).relids);
        (*grouped_rel).reloptkind = RELOPT_OTHER_UPPER_REL;
    } else {
        /*
         * By tradition, the relids set for the main grouping relation is NULL.
         */
        grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, ptr::null_mut());
    }

    /* Set target. */
    (*grouped_rel).reltarget = target;

    /*
     * If the input relation is not parallel-safe, then the grouped relation
     * can't be parallel-safe, either.
     */
    if (*input_rel).consider_parallel
        && target_parallel_safe
        && is_parallel_safe(root, havingQual)
    {
        (*grouped_rel).consider_parallel = true;
    }

    /*
     * If the input rel belongs to a single FDW, so does the grouped rel.
     */
    (*grouped_rel).serverid = (*input_rel).serverid;
    (*grouped_rel).userid = (*input_rel).userid;
    (*grouped_rel).useridiscurrent = (*input_rel).useridiscurrent;
    (*grouped_rel).fdwroutine = (*input_rel).fdwroutine;

    grouped_rel
}

/*
 * is_degenerate_grouping
 */
unsafe fn is_degenerate_grouping(root: *mut PlannerInfo) -> bool {
    let parse: *mut Query = (*root).parse;

    ((*root).hasHavingQual || !(*parse).groupingSets.is_null())
        && !(*parse).hasAggs
        && (*parse).groupClause == NIL
}

/*
 * create_degenerate_grouping_paths
 */
unsafe fn create_degenerate_grouping_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    grouped_rel: *mut RelOptInfo,
) {
    let parse: *mut Query = (*root).parse;
    let mut nrows: c_int;
    let mut path: *mut Path;

    nrows = list_length((*parse).groupingSets);
    if nrows > 1 {
        /*
         * Doesn't seem worthwhile writing code to cons up a generate_series
         * or a values scan to emit multiple rows. Instead just make N clones.
         */
        let mut paths: *mut List = NIL;

        loop {
            nrows -= 1;
            if nrows < 0 {
                break;
            }
            path = create_group_result_path(
                root,
                grouped_rel,
                (*grouped_rel).reltarget,
                (*parse).havingQual as *mut List,
            );
            paths = lappend(paths, path as *mut c_void);
        }
        path = create_append_path(
            root,
            grouped_rel,
            paths,
            NIL,
            NIL,
            ptr::null_mut(),
            0,
            false,
            -1.0,
        );
    } else {
        /* No grouping sets, or just one, so one output row */
        path = create_group_result_path(
            root,
            grouped_rel,
            (*grouped_rel).reltarget,
            (*parse).havingQual as *mut List,
        );
    }

    add_path(grouped_rel, path);
}

/*
 * create_ordinary_grouping_paths
 */
unsafe fn create_ordinary_grouping_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    grouped_rel: *mut RelOptInfo,
    agg_costs: *const AggClauseCosts,
    gd: *mut grouping_sets_data,
    extra: *mut GroupPathExtraData,
    partially_grouped_rel_p: *mut *mut RelOptInfo,
) {
    let cheapest_path: *mut Path = (*input_rel).cheapest_total_path;
    let mut partially_grouped_rel: *mut RelOptInfo = ptr::null_mut();
    let dNumGroups: f64;
    let mut patype: PartitionwiseAggregateType = PARTITIONWISE_AGGREGATE_NONE;

    /*
     * If this is the topmost grouping relation or if the parent relation is
     * doing some form of partitionwise aggregation, then we may be able to do
     * it at this level also.
     */
    if (*extra).patype != PARTITIONWISE_AGGREGATE_NONE && IS_PARTITIONED_REL(input_rel) {
        if (*extra).patype == PARTITIONWISE_AGGREGATE_FULL
            && group_by_has_partkey(input_rel, (*extra).targetList, (*(*root).parse).groupClause)
        {
            patype = PARTITIONWISE_AGGREGATE_FULL;
        } else if ((*extra).flags & GROUPING_CAN_PARTIAL_AGG) != 0 {
            patype = PARTITIONWISE_AGGREGATE_PARTIAL;
        } else {
            patype = PARTITIONWISE_AGGREGATE_NONE;
        }
    }

    /*
     * Before generating paths for grouped_rel, we first generate any possible
     * partially grouped paths.
     */
    if ((*extra).flags & GROUPING_CAN_PARTIAL_AGG) != 0 {
        let force_rel_creation: bool;

        force_rel_creation = patype == PARTITIONWISE_AGGREGATE_PARTIAL;

        partially_grouped_rel =
            create_partial_grouping_paths(root, grouped_rel, input_rel, gd, extra, force_rel_creation);
    }

    /* Set out parameter. */
    *partially_grouped_rel_p = partially_grouped_rel;

    /* Apply partitionwise aggregation technique, if possible. */
    if patype != PARTITIONWISE_AGGREGATE_NONE {
        create_partitionwise_grouping_paths(
            root,
            input_rel,
            grouped_rel,
            partially_grouped_rel,
            agg_costs,
            gd,
            patype,
            extra,
        );
    }

    /* If we are doing partial aggregation only, return. */
    if (*extra).patype == PARTITIONWISE_AGGREGATE_PARTIAL {
        Assert!(!partially_grouped_rel.is_null());

        if !(*partially_grouped_rel).pathlist.is_null() {
            set_cheapest(partially_grouped_rel);
        }

        return;
    }

    /* Gather any partially grouped partial paths. */
    if !partially_grouped_rel.is_null() && !(*partially_grouped_rel).partial_pathlist.is_null() {
        gather_grouping_paths(root, partially_grouped_rel);
        set_cheapest(partially_grouped_rel);
    }

    /*
     * Estimate number of groups.
     */
    dNumGroups = get_number_of_groups(root, (*cheapest_path).rows, gd, (*extra).targetList);

    /* Build final grouping paths */
    add_paths_to_grouping_rel(
        root,
        input_rel,
        grouped_rel,
        partially_grouped_rel,
        agg_costs,
        gd,
        dNumGroups,
        extra,
    );

    /* Give a helpful error if we failed to find any implementation */
    if (*grouped_rel).pathlist == NIL {
        ereport!(ERROR, errmsg!("could not implement GROUP BY"));
        unreachable!();
    }

    /*
     * If there is an FDW that's responsible for all baserels of the query.
     */
    if !(*grouped_rel).fdwroutine.is_null() && fdw_has_GetForeignUpperPaths((*grouped_rel).fdwroutine) {
        fdw_GetForeignUpperPaths(
            (*grouped_rel).fdwroutine,
            root,
            UPPERREL_GROUP_AGG,
            input_rel,
            grouped_rel,
            extra as *mut c_void,
        );
    }

    /* Let extensions possibly add some more paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_GROUP_AGG, input_rel, grouped_rel, extra as *mut c_void);
    }
}

/*
 * For a given input path, consider the possible ways of doing grouping sets on it.
 */
unsafe fn consider_groupingsets_paths(
    root: *mut PlannerInfo,
    grouped_rel: *mut RelOptInfo,
    path: *mut Path,
    is_sorted: bool,
    can_hash: bool,
    gd: *mut grouping_sets_data,
    agg_costs: *const AggClauseCosts,
    dNumGroups: f64,
) {
    let parse: *mut Query = (*root).parse;
    let hash_mem_limit: Size = get_hash_memory_limit();

    /*
     * If we're not being offered sorted input, then only consider plans that
     * can be done entirely by hashing.
     */
    if !is_sorted {
        let mut new_rollups: *mut List = NIL;
        let mut unhashed_rollup: *mut RollupData = ptr::null_mut();
        let mut sets_data: *mut List;
        let mut empty_sets_data: *mut List = NIL;
        let mut empty_sets: *mut List = NIL;
        let mut lc: *mut ListCell;
        let mut l_start: *mut ListCell = list_head((*gd).rollups);
        let mut strat: AggStrategy = AGG_HASHED;
        let hashsize: f64;
        let mut exclude_groups: f64 = 0.0;

        Assert!(can_hash);

        /*
         * If the input is coincidentally sorted usefully, save hashtable space.
         */
        if !l_start.is_null() && pathkeys_contained_in((*root).group_pathkeys, (*path).pathkeys) {
            unhashed_rollup = lfirst_node!(RollupData, T_RollupData, l_start);
            exclude_groups = (*unhashed_rollup).numGroups;
            l_start = lnext((*gd).rollups, l_start);
        }

        hashsize = estimate_hashagg_tablesize(root, path, agg_costs, dNumGroups - exclude_groups);

        /*
         * gd->rollups is empty if we have only unsortable columns to work with.
         */
        if hashsize > hash_mem_limit as f64 && !(*gd).rollups.is_null() {
            return; /* nope, won't fit */
        }

        /*
         * We need to burst the existing rollups list into individual grouping sets.
         */
        sets_data = list_copy((*gd).unsortable_sets);

        for lc_c in for_each_cell_iter((*gd).rollups, l_start) {
            let rollup: *mut RollupData = lfirst_node!(RollupData, T_RollupData, lc_c);

            if !(*rollup).hashable {
                return;
            }

            sets_data = list_concat(sets_data, (*rollup).gsets_data);
        }
        foreach!(lc, sets_data, {
            let gs: *mut GroupingSetData = lfirst_node!(GroupingSetData, T_GroupingSetData, current_cell!(lc));
            let gset: *mut List = (*gs).set;
            let rollup: *mut RollupData;

            if gset == NIL {
                /* Empty grouping sets can't be hashed. */
                empty_sets_data = lappend(empty_sets_data, gs as *mut c_void);
                empty_sets = lappend(empty_sets, NIL as *mut c_void);
            } else {
                rollup = makeNode!(RollupData, T_RollupData);

                (*rollup).groupClause = preprocess_groupclause(root, gset);
                (*rollup).gsets_data = list_make1!(gs);
                (*rollup).gsets = remap_to_groupclause_idx(
                    (*rollup).groupClause,
                    (*rollup).gsets_data,
                    (*gd).tleref_to_colnum_map,
                );
                (*rollup).numGroups = (*gs).numGroups;
                (*rollup).hashable = true;
                (*rollup).is_hashed = true;
                new_rollups = lappend(new_rollups, rollup as *mut c_void);
            }
        });

        /*
         * If we didn't find anything nonempty to hash, then bail.
         */
        if new_rollups == NIL {
            return;
        }

        /*
         * If there were empty grouping sets they should have been in the first rollup.
         */
        Assert!(unhashed_rollup.is_null() || empty_sets.is_null());

        if !unhashed_rollup.is_null() {
            new_rollups = lappend(new_rollups, unhashed_rollup as *mut c_void);
            strat = AGG_MIXED;
        } else if !empty_sets.is_null() {
            let rollup: *mut RollupData = makeNode!(RollupData, T_RollupData);

            (*rollup).groupClause = NIL;
            (*rollup).gsets_data = empty_sets_data;
            (*rollup).gsets = empty_sets;
            (*rollup).numGroups = list_length(empty_sets) as f64;
            (*rollup).hashable = false;
            (*rollup).is_hashed = false;
            new_rollups = lappend(new_rollups, rollup as *mut c_void);
            strat = AGG_MIXED;
        }

        add_path(
            grouped_rel,
            create_groupingsets_path(
                root,
                grouped_rel,
                path,
                (*parse).havingQual as *mut List,
                strat,
                new_rollups,
                agg_costs,
            ),
        );
        return;
    }

    /*
     * If we have sorted input but nothing we can do with it, bail.
     */
    if (*gd).rollups == NIL {
        return;
    }

    /*
     * Given sorted input, we try and make two paths.
     */
    if can_hash && (*gd).any_hashable {
        let mut rollups: *mut List = NIL;
        let mut hash_sets: *mut List = list_copy((*gd).unsortable_sets);
        let mut availspace: f64 = hash_mem_limit as f64;
        let mut lc: *mut ListCell;

        /*
         * Account first for space needed for groups we can't sort at all.
         */
        availspace -= estimate_hashagg_tablesize(root, path, agg_costs, (*gd).dNumHashGroups);

        if availspace > 0.0 && list_length((*gd).rollups) > 1 {
            let scale: f64;
            let num_rollups: c_int = list_length((*gd).rollups);
            let k_capacity: c_int;
            let k_weights: *mut c_int = palloc((num_rollups as usize) * std::mem::size_of::<c_int>()) as *mut c_int;
            let mut hash_items: *mut Bitmapset = ptr::null_mut();
            let mut ii: c_int;

            scale = Max_f64(availspace / (20.0 * num_rollups as f64), 1.0);
            k_capacity = (availspace / scale).floor() as c_int;

            /*
             * We leave the first rollup out of consideration.
             */
            ii = 0;
            for lc_c in for_each_cell_iter((*gd).rollups, lnext((*gd).rollups, list_head((*gd).rollups))) {
                let rollup: *mut RollupData = lfirst_node!(RollupData, T_RollupData, lc_c);

                if (*rollup).hashable {
                    let sz: f64 = estimate_hashagg_tablesize(root, path, agg_costs, (*rollup).numGroups);

                    *k_weights.add(ii as usize) =
                        Min_f64((sz / scale).floor(), k_capacity as f64 + 1.0) as c_int;
                    ii += 1;
                }
            }

            /*
             * Apply knapsack algorithm.
             */
            if ii > 0 {
                hash_items = DiscreteKnapsack(k_capacity, ii, k_weights, ptr::null_mut());
            }

            if !bms_is_empty(hash_items) {
                rollups = list_make1!(linitial((*gd).rollups));

                ii = 0;
                for lc_c in for_each_cell_iter((*gd).rollups, lnext((*gd).rollups, list_head((*gd).rollups))) {
                    let rollup: *mut RollupData = lfirst_node!(RollupData, T_RollupData, lc_c);

                    if (*rollup).hashable {
                        if bms_is_member(ii, hash_items) {
                            hash_sets = list_concat(hash_sets, (*rollup).gsets_data);
                        } else {
                            rollups = lappend(rollups, rollup as *mut c_void);
                        }
                        ii += 1;
                    } else {
                        rollups = lappend(rollups, rollup as *mut c_void);
                    }
                }
            }
        }

        if rollups.is_null() && !hash_sets.is_null() {
            rollups = list_copy((*gd).rollups);
        }

        foreach!(lc, hash_sets, {
            let gs: *mut GroupingSetData = lfirst_node!(GroupingSetData, T_GroupingSetData, current_cell!(lc));
            let rollup: *mut RollupData = makeNode!(RollupData, T_RollupData);

            Assert!((*gs).set != NIL);

            (*rollup).groupClause = preprocess_groupclause(root, (*gs).set);
            (*rollup).gsets_data = list_make1!(gs);
            (*rollup).gsets = remap_to_groupclause_idx(
                (*rollup).groupClause,
                (*rollup).gsets_data,
                (*gd).tleref_to_colnum_map,
            );
            (*rollup).numGroups = (*gs).numGroups;
            (*rollup).hashable = true;
            (*rollup).is_hashed = true;
            rollups = lcons(rollup as *mut c_void, rollups);
        });

        if !rollups.is_null() {
            add_path(
                grouped_rel,
                create_groupingsets_path(
                    root,
                    grouped_rel,
                    path,
                    (*parse).havingQual as *mut List,
                    AGG_MIXED,
                    rollups,
                    agg_costs,
                ),
            );
        }
    }

    /*
     * Now try the simple sorted case.
     */
    if (*gd).unsortable_sets.is_null() {
        add_path(
            grouped_rel,
            create_groupingsets_path(
                root,
                grouped_rel,
                path,
                (*parse).havingQual as *mut List,
                AGG_SORTED,
                (*gd).rollups,
                agg_costs,
            ),
        );
    }
}

/*
 * create_window_paths
 */
unsafe fn create_window_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    input_target: *mut PathTarget,
    output_target: *mut PathTarget,
    output_target_parallel_safe: bool,
    wflists: *mut WindowFuncLists,
    activeWindows: *mut List,
) -> *mut RelOptInfo {
    let window_rel: *mut RelOptInfo;
    let mut lc: *mut ListCell;

    /* For now, do all work in the (WINDOW, NULL) upperrel */
    window_rel = fetch_upper_rel(root, UPPERREL_WINDOW, ptr::null_mut());

    /*
     * If the input relation is not parallel-safe, then the window relation
     * can't be parallel-safe, either.
     */
    if (*input_rel).consider_parallel
        && output_target_parallel_safe
        && is_parallel_safe(root, activeWindows as *mut Node)
    {
        (*window_rel).consider_parallel = true;
    }

    /*
     * If the input rel belongs to a single FDW, so does the window rel.
     */
    (*window_rel).serverid = (*input_rel).serverid;
    (*window_rel).userid = (*input_rel).userid;
    (*window_rel).useridiscurrent = (*input_rel).useridiscurrent;
    (*window_rel).fdwroutine = (*input_rel).fdwroutine;

    /*
     * Consider computing window functions.
     */
    foreach!(lc, (*input_rel).pathlist, {
        let path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let mut presorted_keys: c_int = 0;

        if path == (*input_rel).cheapest_total_path
            || pathkeys_count_contained_in((*root).window_pathkeys, (*path).pathkeys, &raw mut presorted_keys)
            || presorted_keys > 0
        {
            create_one_window_path(
                root,
                window_rel,
                path,
                input_target,
                output_target,
                wflists,
                activeWindows,
            );
        }
    });

    /*
     * If there is an FDW.
     */
    if !(*window_rel).fdwroutine.is_null() && fdw_has_GetForeignUpperPaths((*window_rel).fdwroutine) {
        fdw_GetForeignUpperPaths(
            (*window_rel).fdwroutine,
            root,
            UPPERREL_WINDOW,
            input_rel,
            window_rel,
            ptr::null_mut(),
        );
    }

    /* Let extensions possibly add some more paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_WINDOW, input_rel, window_rel, ptr::null_mut());
    }

    /* Now choose the best path(s) */
    set_cheapest(window_rel);

    window_rel
}

/*
 * Stack window-function implementation steps atop the given Path.
 */
unsafe fn create_one_window_path(
    root: *mut PlannerInfo,
    window_rel: *mut RelOptInfo,
    mut path: *mut Path,
    input_target: *mut PathTarget,
    output_target: *mut PathTarget,
    wflists: *mut WindowFuncLists,
    activeWindows: *mut List,
) {
    let mut window_target: *mut PathTarget;
    let mut l: *mut ListCell;
    let mut topqual: *mut List = NIL;

    /*
     * Since each window clause could require a different sort order, we stack
     * up a WindowAgg node for each clause.
     */
    window_target = input_target;

    foreach!(l, activeWindows, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(l));
        let window_pathkeys: *mut List;
        let mut runcondition: *mut List = NIL;
        let mut presorted_keys: c_int = 0;
        let is_sorted: bool;
        let topwindow: bool;
        let mut lc2: *mut ListCell;

        window_pathkeys = make_pathkeys_for_window(root, wc, (*root).processed_tlist);

        is_sorted = pathkeys_count_contained_in(window_pathkeys, (*path).pathkeys, &raw mut presorted_keys);

        /* Sort if necessary */
        if !is_sorted {
            if presorted_keys == 0 || !enable_incremental_sort {
                path = create_sort_path(root, window_rel, path, window_pathkeys, -1.0);
            } else {
                path = create_incremental_sort_path(root, window_rel, path, window_pathkeys, presorted_keys, -1.0);
            }
        }

        if !lnext(activeWindows, current_cell!(l)).is_null() {
            /*
             * Add the current WindowFuncs to the output target for this
             * intermediate WindowAggPath.
             */
            let mut tuple_width: int64 = (*window_target).width as int64;

            window_target = copy_pathtarget(window_target);
            foreach!(lc2, *(*wflists).windowFuncs.add((*wc).winref as usize), {
                let wfunc: *mut WindowFunc = lfirst_node!(WindowFunc, T_WindowFunc, current_cell!(lc2));

                add_column_to_pathtarget(window_target, wfunc as *mut Expr, 0);
                tuple_width += get_typavgwidth((*wfunc).wintype, -1) as int64;
            });
            (*window_target).width = clamp_width_est(tuple_width);
        } else {
            /* Install the goal target in the topmost WindowAgg */
            window_target = output_target;
        }

        /* mark the final item in the list as the top-level window */
        topwindow = foreach_current_index_lc(current_cell!(l), activeWindows) == list_length(activeWindows) - 1;

        /*
         * Collect the WindowFuncRunConditions from each WindowFunc.
         */
        foreach!(lc2, *(*wflists).windowFuncs.add((*wc).winref as usize), {
            let mut lc3: *mut ListCell;
            let wfunc: *mut WindowFunc = lfirst_node!(WindowFunc, T_WindowFunc, current_cell!(lc2));

            foreach!(lc3, (*wfunc).runCondition, {
                let wfuncrc: *mut WindowFuncRunCondition = lfirst_node!(WindowFuncRunCondition, T_WindowFuncRunCondition, current_cell!(lc3));
                let opexpr: *mut Expr;
                let leftop: *mut Expr;
                let rightop: *mut Expr;

                if (*wfuncrc).wfunc_left {
                    leftop = copyObject_node(wfunc as *mut Node) as *mut Expr;
                    rightop = copyObject_node((*wfuncrc).arg as *mut Node) as *mut Expr;
                } else {
                    leftop = copyObject_node((*wfuncrc).arg as *mut Node) as *mut Expr;
                    rightop = copyObject_node(wfunc as *mut Node) as *mut Expr;
                }

                opexpr = make_opclause(
                    (*wfuncrc).opno,
                    BOOLOID,
                    false,
                    leftop,
                    rightop,
                    InvalidOid,
                    (*wfuncrc).inputcollid,
                );

                runcondition = lappend(runcondition, opexpr as *mut c_void);

                if !topwindow {
                    topqual = lappend(topqual, opexpr as *mut c_void);
                }
            });
        });

        path = create_windowagg_path(
            root,
            window_rel,
            path,
            window_target,
            *(*wflists).windowFuncs.add((*wc).winref as usize),
            runcondition,
            wc,
            if topwindow { topqual } else { NIL },
            topwindow,
        );
    });

    add_path(window_rel, path);
}

/*
 * create_distinct_paths
 */
unsafe fn create_distinct_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    target: *mut PathTarget,
) -> *mut RelOptInfo {
    let distinct_rel: *mut RelOptInfo;

    /* For now, do all work in the (DISTINCT, NULL) upperrel */
    distinct_rel = fetch_upper_rel(root, UPPERREL_DISTINCT, ptr::null_mut());

    /*
     * We don't compute anything at this level.
     */
    (*distinct_rel).consider_parallel = (*input_rel).consider_parallel;

    /*
     * If the input rel belongs to a single FDW, so does the distinct_rel.
     */
    (*distinct_rel).serverid = (*input_rel).serverid;
    (*distinct_rel).userid = (*input_rel).userid;
    (*distinct_rel).useridiscurrent = (*input_rel).useridiscurrent;
    (*distinct_rel).fdwroutine = (*input_rel).fdwroutine;

    /* build distinct paths based on input_rel's pathlist */
    create_final_distinct_paths(root, input_rel, distinct_rel);

    /* now build distinct paths based on input_rel's partial_pathlist */
    create_partial_distinct_paths(root, input_rel, distinct_rel, target);

    /* Give a helpful error if we failed to create any paths */
    if (*distinct_rel).pathlist == NIL {
        ereport!(ERROR, errmsg!("could not implement DISTINCT"));
        unreachable!();
    }

    /*
     * If there is an FDW.
     */
    if !(*distinct_rel).fdwroutine.is_null() && fdw_has_GetForeignUpperPaths((*distinct_rel).fdwroutine) {
        fdw_GetForeignUpperPaths(
            (*distinct_rel).fdwroutine,
            root,
            UPPERREL_DISTINCT,
            input_rel,
            distinct_rel,
            ptr::null_mut(),
        );
    }

    /* Let extensions possibly add some more paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_DISTINCT, input_rel, distinct_rel, ptr::null_mut());
    }

    /* Now choose the best path(s) */
    set_cheapest(distinct_rel);

    distinct_rel
}

/*
 * create_partial_distinct_paths
 */
unsafe fn create_partial_distinct_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    final_distinct_rel: *mut RelOptInfo,
    target: *mut PathTarget,
) {
    let partial_distinct_rel: *mut RelOptInfo;
    let parse: *mut Query;
    let distinctExprs: *mut List;
    let numDistinctRows: f64;
    let cheapest_partial_path: *mut Path;
    let mut lc: *mut ListCell;

    /* nothing to do when there are no partial paths in the input rel */
    if !(*input_rel).consider_parallel || (*input_rel).partial_pathlist == NIL {
        return;
    }

    parse = (*root).parse;

    /* can't do parallel DISTINCT ON */
    if (*parse).hasDistinctOn {
        return;
    }

    partial_distinct_rel = fetch_upper_rel(root, UPPERREL_PARTIAL_DISTINCT, ptr::null_mut());
    (*partial_distinct_rel).reltarget = target;
    (*partial_distinct_rel).consider_parallel = (*input_rel).consider_parallel;

    /*
     * If input_rel belongs to a single FDW.
     */
    (*partial_distinct_rel).serverid = (*input_rel).serverid;
    (*partial_distinct_rel).userid = (*input_rel).userid;
    (*partial_distinct_rel).useridiscurrent = (*input_rel).useridiscurrent;
    (*partial_distinct_rel).fdwroutine = (*input_rel).fdwroutine;

    cheapest_partial_path = linitial((*input_rel).partial_pathlist) as *mut Path;

    distinctExprs = get_sortgrouplist_exprs((*root).processed_distinctClause, (*parse).targetList);

    /* estimate how many distinct rows we'll get from each worker */
    numDistinctRows = estimate_num_groups(
        root,
        distinctExprs,
        (*cheapest_partial_path).rows,
        ptr::null_mut(),
        ptr::null_mut(),
    );

    /*
     * Try sorting the cheapest path and put unique paths atop of those.
     */
    if grouping_is_sortable((*root).processed_distinctClause) {
        foreach!(lc, (*input_rel).partial_pathlist, {
            let input_path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let mut sorted_path: *mut Path;
            let useful_pathkeys_list: *mut List;

            useful_pathkeys_list = get_useful_pathkeys_for_distinct(
                root,
                (*root).distinct_pathkeys,
                (*input_path).pathkeys,
            );
            Assert!(list_length(useful_pathkeys_list) > 0);

            let mut lcp: *mut ListCell;
            foreach!(lcp, useful_pathkeys_list, {
                let useful_pathkeys: *mut List = lfirst(current_cell!(lcp)) as *mut List;

                sorted_path = make_ordered_path(
                    root,
                    partial_distinct_rel,
                    input_path,
                    cheapest_partial_path,
                    useful_pathkeys,
                    -1.0,
                );

                if sorted_path.is_null() {
                    continue;
                }

                /*
                 * An empty distinct_pathkeys means all tuples have the same value.
                 */
                if (*root).distinct_pathkeys == NIL {
                    let limitCount: *mut Node;

                    limitCount = makeConst(
                        INT8OID,
                        -1,
                        InvalidOid,
                        std::mem::size_of::<int64>() as c_int,
                        Int64GetDatum(1),
                        false,
                        FLOAT8PASSBYVAL,
                    ) as *mut Node;

                    add_partial_path(
                        partial_distinct_rel,
                        create_limit_path(
                            root,
                            partial_distinct_rel,
                            sorted_path,
                            ptr::null_mut(),
                            limitCount,
                            LIMIT_OPTION_COUNT,
                            0,
                            1,
                        ),
                    );
                } else {
                    add_partial_path(
                        partial_distinct_rel,
                        create_upper_unique_path(
                            root,
                            partial_distinct_rel,
                            sorted_path,
                            list_length((*root).distinct_pathkeys),
                            numDistinctRows,
                        ),
                    );
                }
            });
        });
    }

    /*
     * Now try hash aggregate paths, if enabled and hashing is possible.
     */
    if enable_hashagg && grouping_is_hashable((*root).processed_distinctClause) {
        add_partial_path(
            partial_distinct_rel,
            create_agg_path(
                root,
                partial_distinct_rel,
                cheapest_partial_path,
                (*cheapest_partial_path).pathtarget,
                AGG_HASHED,
                AGGSPLIT_SIMPLE,
                (*root).processed_distinctClause,
                NIL,
                ptr::null(),
                numDistinctRows,
            ),
        );
    }

    /*
     * If there is an FDW.
     */
    if !(*partial_distinct_rel).fdwroutine.is_null()
        && fdw_has_GetForeignUpperPaths((*partial_distinct_rel).fdwroutine)
    {
        fdw_GetForeignUpperPaths(
            (*partial_distinct_rel).fdwroutine,
            root,
            UPPERREL_PARTIAL_DISTINCT,
            input_rel,
            partial_distinct_rel,
            ptr::null_mut(),
        );
    }

    /* Let extensions possibly add some more partial paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_PARTIAL_DISTINCT, input_rel, partial_distinct_rel, ptr::null_mut());
    }

    if (*partial_distinct_rel).partial_pathlist != NIL {
        generate_useful_gather_paths(root, partial_distinct_rel, true);
        set_cheapest(partial_distinct_rel);

        /*
         * Finally, create paths to distinctify the final result.
         */
        create_final_distinct_paths(root, partial_distinct_rel, final_distinct_rel);
    }
}

/*
 * create_final_distinct_paths
 */
unsafe fn create_final_distinct_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    distinct_rel: *mut RelOptInfo,
) -> *mut RelOptInfo {
    let parse: *mut Query = (*root).parse;
    let cheapest_input_path: *mut Path = (*input_rel).cheapest_total_path;
    let numDistinctRows: f64;
    let allow_hash: bool;

    /* Estimate number of distinct rows there will be */
    if !(*parse).groupClause.is_null()
        || !(*parse).groupingSets.is_null()
        || (*parse).hasAggs
        || (*root).hasHavingQual
    {
        /*
         * If there was grouping or aggregation, use the number of input rows.
         */
        numDistinctRows = (*cheapest_input_path).rows;
    } else {
        /*
         * Otherwise, the UNIQUE filter has effects comparable to GROUP BY.
         */
        let distinctExprs: *mut List;

        distinctExprs = get_sortgrouplist_exprs((*root).processed_distinctClause, (*parse).targetList);
        numDistinctRows = estimate_num_groups(
            root,
            distinctExprs,
            (*cheapest_input_path).rows,
            ptr::null_mut(),
            ptr::null_mut(),
        );
    }

    /*
     * Consider sort-based implementations of DISTINCT, if possible.
     */
    if grouping_is_sortable((*root).processed_distinctClause) {
        let needed_pathkeys: *mut List;
        let mut lc: *mut ListCell;
        let limittuples: f64 = if (*root).distinct_pathkeys == NIL { 1.0 } else { -1.0 };

        if (*parse).hasDistinctOn
            && list_length((*root).distinct_pathkeys) < list_length((*root).sort_pathkeys)
        {
            needed_pathkeys = (*root).sort_pathkeys;
        } else {
            needed_pathkeys = (*root).distinct_pathkeys;
        }

        foreach!(lc, (*input_rel).pathlist, {
            let input_path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let mut sorted_path: *mut Path;
            let useful_pathkeys_list: *mut List;

            useful_pathkeys_list =
                get_useful_pathkeys_for_distinct(root, needed_pathkeys, (*input_path).pathkeys);
            Assert!(list_length(useful_pathkeys_list) > 0);

            let mut lcp: *mut ListCell;
            foreach!(lcp, useful_pathkeys_list, {
                let useful_pathkeys: *mut List = lfirst(current_cell!(lcp)) as *mut List;

                sorted_path = make_ordered_path(
                    root,
                    distinct_rel,
                    input_path,
                    cheapest_input_path,
                    useful_pathkeys,
                    limittuples,
                );

                if sorted_path.is_null() {
                    continue;
                }

                if (*root).distinct_pathkeys == NIL {
                    let limitCount: *mut Node;

                    limitCount = makeConst(
                        INT8OID,
                        -1,
                        InvalidOid,
                        std::mem::size_of::<int64>() as c_int,
                        Int64GetDatum(1),
                        false,
                        FLOAT8PASSBYVAL,
                    ) as *mut Node;

                    add_path(
                        distinct_rel,
                        create_limit_path(
                            root,
                            distinct_rel,
                            sorted_path,
                            ptr::null_mut(),
                            limitCount,
                            LIMIT_OPTION_COUNT,
                            0,
                            1,
                        ),
                    );
                } else {
                    add_path(
                        distinct_rel,
                        create_upper_unique_path(
                            root,
                            distinct_rel,
                            sorted_path,
                            list_length((*root).distinct_pathkeys),
                            numDistinctRows,
                        ),
                    );
                }
            });
        });
    }

    /*
     * Consider hash-based implementations of DISTINCT, if possible.
     */
    if (*distinct_rel).pathlist == NIL {
        allow_hash = true; /* we have no alternatives */
    } else if (*parse).hasDistinctOn || !enable_hashagg {
        allow_hash = false; /* policy-based decision not to hash */
    } else {
        allow_hash = true; /* default */
    }

    if allow_hash && grouping_is_hashable((*root).processed_distinctClause) {
        /* Generate hashed aggregate path --- no sort needed */
        add_path(
            distinct_rel,
            create_agg_path(
                root,
                distinct_rel,
                cheapest_input_path,
                (*cheapest_input_path).pathtarget,
                AGG_HASHED,
                AGGSPLIT_SIMPLE,
                (*root).processed_distinctClause,
                NIL,
                ptr::null(),
                numDistinctRows,
            ),
        );
    }

    distinct_rel
}

/*
 * get_useful_pathkeys_for_distinct
 */
unsafe fn get_useful_pathkeys_for_distinct(
    root: *mut PlannerInfo,
    needed_pathkeys: *mut List,
    path_pathkeys: *mut List,
) -> *mut List {
    let mut useful_pathkeys_list: *mut List = NIL;
    let mut useful_pathkeys: *mut List = NIL;

    /* always include the given 'needed_pathkeys' */
    useful_pathkeys_list = lappend(useful_pathkeys_list, needed_pathkeys as *mut c_void);

    if !enable_distinct_reordering {
        return useful_pathkeys_list;
    }

    /*
     * Scan the given 'path_pathkeys' and construct a list of PathKey nodes.
     */
    let mut lc: *mut ListCell;
    foreach!(lc, path_pathkeys, {
        let pathkey: *mut PathKey = lfirst_node!(PathKey, T_PathKey, current_cell!(lc));

        if !list_member_ptr(needed_pathkeys, pathkey as *mut c_void) {
            break;
        }
        if (*(*root).parse).hasDistinctOn
            && !list_member_ptr((*root).distinct_pathkeys, pathkey as *mut c_void)
        {
            break;
        }

        useful_pathkeys = lappend(useful_pathkeys, pathkey as *mut c_void);
    });

    /* If no match at all, no point in reordering needed_pathkeys */
    if useful_pathkeys == NIL {
        return useful_pathkeys_list;
    }

    /*
     * If not full match, the resulting pathkey list is not useful without
     * incremental sort.
     */
    if list_length(useful_pathkeys) < list_length(needed_pathkeys) && !enable_incremental_sort {
        return useful_pathkeys_list;
    }

    /* Append the remaining PathKey nodes in needed_pathkeys */
    useful_pathkeys = list_concat_unique_ptr(useful_pathkeys, needed_pathkeys);

    /*
     * If the resulting pathkey list is the same as the 'needed_pathkeys', just drop it.
     */
    if compare_pathkeys(needed_pathkeys, useful_pathkeys) == PATHKEYS_EQUAL {
        return useful_pathkeys_list;
    }

    useful_pathkeys_list = lappend(useful_pathkeys_list, useful_pathkeys as *mut c_void);

    useful_pathkeys_list
}

/*
 * create_ordered_paths
 */
unsafe fn create_ordered_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    target: *mut PathTarget,
    target_parallel_safe: bool,
    limit_tuples: f64,
) -> *mut RelOptInfo {
    let cheapest_input_path: *mut Path = (*input_rel).cheapest_total_path;
    let ordered_rel: *mut RelOptInfo;
    let mut lc: *mut ListCell;

    /* For now, do all work in the (ORDERED, NULL) upperrel */
    ordered_rel = fetch_upper_rel(root, UPPERREL_ORDERED, ptr::null_mut());

    /*
     * If the input relation is not parallel-safe.
     */
    if (*input_rel).consider_parallel && target_parallel_safe {
        (*ordered_rel).consider_parallel = true;
    }

    /*
     * If the input rel belongs to a single FDW.
     */
    (*ordered_rel).serverid = (*input_rel).serverid;
    (*ordered_rel).userid = (*input_rel).userid;
    (*ordered_rel).useridiscurrent = (*input_rel).useridiscurrent;
    (*ordered_rel).fdwroutine = (*input_rel).fdwroutine;

    foreach!(lc, (*input_rel).pathlist, {
        let input_path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let mut sorted_path: *mut Path;
        let is_sorted: bool;
        let mut presorted_keys: c_int = 0;

        is_sorted = pathkeys_count_contained_in((*root).sort_pathkeys, (*input_path).pathkeys, &raw mut presorted_keys);

        if is_sorted {
            sorted_path = input_path;
        } else {
            /*
             * Try at least sorting the cheapest path.
             */
            if input_path != cheapest_input_path
                && (presorted_keys == 0 || !enable_incremental_sort)
            {
                continue;
            }

            if presorted_keys == 0 || !enable_incremental_sort {
                sorted_path = create_sort_path(root, ordered_rel, input_path, (*root).sort_pathkeys, limit_tuples);
            } else {
                sorted_path = create_incremental_sort_path(root, ordered_rel, input_path, (*root).sort_pathkeys, presorted_keys, limit_tuples);
            }
        }

        /*
         * If the pathtarget of the result path has different expressions, a
         * projection step is needed.
         */
        if !equal((*(*sorted_path).pathtarget).exprs as *mut c_void, (*target).exprs as *mut c_void) {
            sorted_path = apply_projection_to_path(root, ordered_rel, sorted_path, target);
        }

        add_path(ordered_rel, sorted_path);
    });

    /*
     * Consider sorting the cheapest partial path and using Gather Merge.
     */
    if (*ordered_rel).consider_parallel
        && (*root).sort_pathkeys != NIL
        && (*input_rel).partial_pathlist != NIL
    {
        let cheapest_partial_path: *mut Path;

        cheapest_partial_path = linitial((*input_rel).partial_pathlist) as *mut Path;

        foreach!(lc, (*input_rel).partial_pathlist, {
            let input_path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let mut sorted_path: *mut Path;
            let is_sorted: bool;
            let mut presorted_keys: c_int = 0;
            let mut total_groups: f64;

            is_sorted = pathkeys_count_contained_in((*root).sort_pathkeys, (*input_path).pathkeys, &raw mut presorted_keys);

            if is_sorted {
                continue;
            }

            if input_path != cheapest_partial_path
                && (presorted_keys == 0 || !enable_incremental_sort)
            {
                continue;
            }

            if presorted_keys == 0 || !enable_incremental_sort {
                sorted_path = create_sort_path(root, ordered_rel, input_path, (*root).sort_pathkeys, limit_tuples);
            } else {
                sorted_path = create_incremental_sort_path(root, ordered_rel, input_path, (*root).sort_pathkeys, presorted_keys, limit_tuples);
            }
            total_groups = compute_gather_rows(sorted_path);
            sorted_path = create_gather_merge_path(
                root,
                ordered_rel,
                sorted_path,
                (*sorted_path).pathtarget,
                (*root).sort_pathkeys,
                ptr::null_mut(),
                &raw mut total_groups,
            );

            /*
             * If the pathtarget of the result path has different expressions.
             */
            if !equal((*(*sorted_path).pathtarget).exprs as *mut c_void, (*target).exprs as *mut c_void) {
                sorted_path = apply_projection_to_path(root, ordered_rel, sorted_path, target);
            }

            add_path(ordered_rel, sorted_path);
        });
    }

    /*
     * If there is an FDW.
     */
    if !(*ordered_rel).fdwroutine.is_null() && fdw_has_GetForeignUpperPaths((*ordered_rel).fdwroutine) {
        fdw_GetForeignUpperPaths(
            (*ordered_rel).fdwroutine,
            root,
            UPPERREL_ORDERED,
            input_rel,
            ordered_rel,
            ptr::null_mut(),
        );
    }

    /* Let extensions possibly add some more paths */
    if let Some(hook) = create_upper_paths_hook {
        hook(root, UPPERREL_ORDERED, input_rel, ordered_rel, ptr::null_mut());
    }

    /*
     * No need to bother with set_cheapest here.
     */
    Assert!((*ordered_rel).pathlist != NIL);

    ordered_rel
}

/*
 * make_group_input_target
 */
unsafe fn make_group_input_target(root: *mut PlannerInfo, final_target: *mut PathTarget) -> *mut PathTarget {
    let parse: *mut Query = (*root).parse;
    let input_target: *mut PathTarget;
    let mut non_group_cols: *mut List;
    let mut non_group_vars: *mut List;
    let mut i: c_int;
    let mut lc: *mut ListCell;

    /*
     * We must build a target containing all grouping columns, plus any other
     * Vars mentioned in the query's targetlist and HAVING qual.
     */
    input_target = create_empty_pathtarget();
    non_group_cols = NIL;

    i = 0;
    foreach!(lc, (*final_target).exprs, {
        let mut expr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
        let sgref: Index = get_pathtarget_sortgroupref(final_target, i);

        if sgref != 0
            && !(*root).processed_groupClause.is_null()
            && !get_sortgroupref_clause_noerr(sgref, (*root).processed_groupClause).is_null()
        {
            /*
             * It's a grouping column, so add it to the input target as-is.
             */
            if (*parse).hasGroupRTE && (*parse).groupingSets != NIL {
                Assert!((*root).group_rtindex > 0);
                expr = remove_nulling_relids(
                    expr as *mut Node,
                    bms_make_singleton((*root).group_rtindex as c_int),
                    ptr::null_mut(),
                ) as *mut Expr;
            }
            add_column_to_pathtarget(input_target, expr, sgref);
        } else {
            /*
             * Non-grouping column.
             */
            non_group_cols = lappend(non_group_cols, expr as *mut c_void);
        }

        i += 1;
    });

    /*
     * If there's a HAVING clause, we'll need the Vars it uses, too.
     */
    if !(*parse).havingQual.is_null() {
        non_group_cols = lappend(non_group_cols, (*parse).havingQual as *mut c_void);
    }

    /*
     * Pull out all the Vars mentioned in non-group cols.
     */
    non_group_vars = pull_var_clause(
        non_group_cols as *mut Node,
        PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );
    if (*parse).hasGroupRTE && (*parse).groupingSets != NIL {
        Assert!((*root).group_rtindex > 0);
        non_group_vars = remove_nulling_relids(
            non_group_vars as *mut Node,
            bms_make_singleton((*root).group_rtindex as c_int),
            ptr::null_mut(),
        ) as *mut List;
    }
    add_new_columns_to_pathtarget(input_target, non_group_vars);

    /* clean up cruft */
    list_free(non_group_vars);
    list_free(non_group_cols);

    /* XXX this causes some redundant cost calculation ... */
    set_pathtarget_cost_width(root, input_target)
}

/*
 * make_partial_grouping_target
 */
unsafe fn make_partial_grouping_target(
    root: *mut PlannerInfo,
    grouping_target: *mut PathTarget,
    havingQual: *mut Node,
) -> *mut PathTarget {
    let partial_target: *mut PathTarget;
    let mut non_group_cols: *mut List;
    let non_group_exprs: *mut List;
    let mut i: c_int;
    let mut lc: *mut ListCell;

    partial_target = create_empty_pathtarget();
    non_group_cols = NIL;

    i = 0;
    foreach!(lc, (*grouping_target).exprs, {
        let expr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
        let sgref: Index = get_pathtarget_sortgroupref(grouping_target, i);

        if sgref != 0
            && !(*root).processed_groupClause.is_null()
            && !get_sortgroupref_clause_noerr(sgref, (*root).processed_groupClause).is_null()
        {
            /*
             * It's a grouping column, so add it to the partial_target as-is.
             */
            add_column_to_pathtarget(partial_target, expr, sgref);
        } else {
            /*
             * Non-grouping column.
             */
            non_group_cols = lappend(non_group_cols, expr as *mut c_void);
        }

        i += 1;
    });

    /*
     * If there's a HAVING clause.
     */
    if !havingQual.is_null() {
        non_group_cols = lappend(non_group_cols, havingQual as *mut c_void);
    }

    /*
     * Pull out all the Vars, PlaceHolderVars, and Aggrefs.
     */
    non_group_exprs = pull_var_clause(
        non_group_cols as *mut Node,
        PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );

    add_new_columns_to_pathtarget(partial_target, non_group_exprs);

    /*
     * Adjust Aggrefs to put them in partial mode.
     */
    foreach!(lc, (*partial_target).exprs, {
        let aggref: *mut Aggref = lfirst(current_cell!(lc)) as *mut Aggref;

        if IsA!(aggref, T_Aggref) {
            let newaggref: *mut Aggref;

            newaggref = makeNode!(Aggref, T_Aggref);
            ptr::copy_nonoverlapping(aggref, newaggref, 1);

            /* For now, assume serialization is required */
            mark_partial_aggref(newaggref, AGGSPLIT_INITIAL_SERIAL);

            *(&raw mut (*current_cell!(lc)).ptr_value as *mut *mut Aggref) = newaggref;
        }
    });

    /* clean up cruft */
    list_free(non_group_exprs);
    list_free(non_group_cols);

    /* XXX this causes some redundant cost calculation ... */
    set_pathtarget_cost_width(root, partial_target)
}

/*
 * mark_partial_aggref
 */
pub unsafe fn mark_partial_aggref(agg: *mut Aggref, aggsplit: AggSplit) {
    /* aggtranstype should be computed by this point */
    Assert!(OidIsValid((*agg).aggtranstype));
    /* ... but aggsplit should still be as the parser left it */
    Assert!((*agg).aggsplit == AGGSPLIT_SIMPLE);

    /* Mark the Aggref with the intended partial-aggregation mode */
    (*agg).aggsplit = aggsplit;

    /*
     * Adjust result type if needed.
     */
    if DO_AGGSPLIT_SKIPFINAL(aggsplit) {
        if (*agg).aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(aggsplit) {
            (*agg).aggtype = BYTEAOID;
        } else {
            (*agg).aggtype = (*agg).aggtranstype;
        }
    }
}

/*
 * postprocess_setop_tlist
 */
unsafe fn postprocess_setop_tlist(new_tlist: *mut List, orig_tlist: *mut List) -> *mut List {
    let mut l: *mut ListCell;
    let mut orig_tlist_item: *mut ListCell = list_head(orig_tlist);

    foreach!(l, new_tlist, {
        let new_tle: *mut TargetEntry = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(l));
        let orig_tle: *mut TargetEntry;

        /* ignore resjunk columns in setop result */
        if (*new_tle).resjunk {
            continue;
        }

        Assert!(!orig_tlist_item.is_null());
        orig_tle = lfirst_node!(TargetEntry, T_TargetEntry, orig_tlist_item);
        orig_tlist_item = lnext(orig_tlist, orig_tlist_item);
        if (*orig_tle).resjunk {
            /* should not happen */
            elog!(ERROR, "resjunk output columns are not implemented");
        }
        Assert!((*new_tle).resno == (*orig_tle).resno);
        (*new_tle).ressortgroupref = (*orig_tle).ressortgroupref;
    });
    if !orig_tlist_item.is_null() {
        elog!(ERROR, "resjunk output columns are not implemented");
    }
    new_tlist
}

/*
 * optimize_window_clauses
 */
unsafe fn optimize_window_clauses(root: *mut PlannerInfo, wflists: *mut WindowFuncLists) {
    let windowClause: *mut List = (*(*root).parse).windowClause;
    let mut lc: *mut ListCell;

    foreach!(lc, windowClause, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));
        let mut lc2: *mut ListCell;
        let mut optimizedFrameOptions: c_int = 0;
        let mut lc2_was_null = true;

        Assert!((*wc).winref <= (*wflists).maxWinRef);

        /* skip any WindowClauses that have no WindowFuncs */
        if *(*wflists).windowFuncs.add((*wc).winref as usize) == NIL {
            continue;
        }

        let mut broke = false;
        foreach!(lc2, *(*wflists).windowFuncs.add((*wc).winref as usize), {
            let mut req: SupportRequestOptimizeWindowClause = std::mem::zeroed();
            let res: *mut SupportRequestOptimizeWindowClause;
            let wfunc: *mut WindowFunc = lfirst_node!(WindowFunc, T_WindowFunc, current_cell!(lc2));
            let prosupport: Oid;

            prosupport = get_func_support((*wfunc).winfnoid);

            /* Check if there's a support function for 'wfunc' */
            if !OidIsValid(prosupport) {
                broke = true;
                break; /* can't optimize this WindowClause */
            }

            req.r#type = T_SupportRequestOptimizeWindowClause;
            req.window_clause = wc;
            req.window_func = wfunc;
            req.frameOptions = (*wc).frameOptions;

            /* call the support function */
            res = DatumGetPointer(OidFunctionCall1(prosupport, PointerGetDatum(&raw const req)))
                as *mut SupportRequestOptimizeWindowClause;

            if res.is_null() {
                broke = true;
                break;
            }

            /*
             * Save these frameOptions for the first WindowFunc.
             */
            if foreach_current_index_lc(current_cell!(lc2), *(*wflists).windowFuncs.add((*wc).winref as usize)) == 0 {
                optimizedFrameOptions = (*res).frameOptions;
            } else if optimizedFrameOptions != (*res).frameOptions {
                broke = true;
                break; /* skip to the next WindowClause, if any */
            }
        });
        lc2_was_null = !broke;

        /* adjust the frameOptions if all WindowFunc's agree that it's ok */
        if lc2_was_null && (*wc).frameOptions != optimizedFrameOptions {
            let mut lc3: *mut ListCell;

            /* apply the new frame options */
            (*wc).frameOptions = optimizedFrameOptions;

            /*
             * Check to see if changing the frameOptions has caused a duplicate.
             */
            if list_length(windowClause) == 1 {
                continue;
            }

            foreach!(lc3, windowClause, {
                let existing_wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc3));

                /* skip over the WindowClause we're currently editing */
                if existing_wc == wc {
                    continue;
                }

                if equal((*wc).partitionClause as *mut c_void, (*existing_wc).partitionClause as *mut c_void)
                    && equal((*wc).orderClause as *mut c_void, (*existing_wc).orderClause as *mut c_void)
                    && (*wc).frameOptions == (*existing_wc).frameOptions
                    && equal((*wc).startOffset as *mut c_void, (*existing_wc).startOffset as *mut c_void)
                    && equal((*wc).endOffset as *mut c_void, (*existing_wc).endOffset as *mut c_void)
                {
                    let mut lc4: *mut ListCell;

                    foreach!(lc4, *(*wflists).windowFuncs.add((*wc).winref as usize), {
                        let wfunc: *mut WindowFunc = lfirst_node!(WindowFunc, T_WindowFunc, current_cell!(lc4));

                        (*wfunc).winref = (*existing_wc).winref;
                    });

                    /* move list items */
                    *(*wflists).windowFuncs.add((*existing_wc).winref as usize) = list_concat(
                        *(*wflists).windowFuncs.add((*existing_wc).winref as usize),
                        *(*wflists).windowFuncs.add((*wc).winref as usize),
                    );
                    *(*wflists).windowFuncs.add((*wc).winref as usize) = NIL;

                    break;
                }
            });
        }
    });

    /*
     * XXX remove any duplicate WindowFuncs from each WindowClause.
     */
    foreach!(lc, windowClause, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));
        let mut lc2: *mut ListCell;
        let list: *mut List = *(*wflists).windowFuncs.add((*wc).winref as usize);
        let mut newlist: *mut List = NIL;

        if list == NIL {
            continue;
        }

        foreach!(lc2, list, {
            if !list_member(newlist, lfirst(current_cell!(lc2))) {
                newlist = lappend(newlist, lfirst(current_cell!(lc2)));
            } else {
                (*wflists).numWindowFuncs -= 1;
            }
        });
        list_free(list);

        *(*wflists).windowFuncs.add((*wc).winref as usize) = newlist;
    });
}

/*
 * select_active_windows
 */
unsafe fn select_active_windows(root: *mut PlannerInfo, wflists: *mut WindowFuncLists) -> *mut List {
    let windowClause: *mut List = (*(*root).parse).windowClause;
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;
    let mut nActive: c_int = 0;
    let actives: *mut WindowClauseSortData = palloc(
        std::mem::size_of::<WindowClauseSortData>() * (list_length(windowClause) as usize),
    ) as *mut WindowClauseSortData;

    /* First, construct an array of the active windows */
    foreach!(lc, windowClause, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));

        /* It's only active if wflists shows some related WindowFuncs */
        Assert!((*wc).winref <= (*wflists).maxWinRef);
        if *(*wflists).windowFuncs.add((*wc).winref as usize) == NIL {
            continue;
        }

        (*actives.add(nActive as usize)).wc = wc; /* original clause */

        (*actives.add(nActive as usize)).uniqueOrder =
            list_concat_unique(list_copy((*wc).partitionClause), (*wc).orderClause);
        nActive += 1;
    });

    /*
     * Sort active windows by their partitioning/ordering clauses.
     */
    qsort_window(actives, nActive);

    /* build ordered list of the original WindowClause nodes */
    let mut i: c_int = 0;
    while i < nActive {
        result = lappend(result, (*actives.add(i as usize)).wc as *mut c_void);
        i += 1;
    }

    pfree(actives as *mut c_void);

    result
}

unsafe fn qsort_window(actives: *mut WindowClauseSortData, n: c_int) {
    if n <= 1 {
        return;
    }
    let slice = std::slice::from_raw_parts_mut(actives, n as usize);
    slice.sort_by(|a, b| {
        match common_prefix_cmp(a as *const _ as *const c_void, b as *const _ as *const c_void) {
            x if x < 0 => std::cmp::Ordering::Less,
            x if x > 0 => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    });
}

/*
 * name_active_windows
 */
unsafe fn name_active_windows(activeWindows: *mut List) {
    let mut next_n: c_int = 1;
    let mut lc: *mut ListCell;

    foreach!(lc, activeWindows, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));

        /* Nothing to do if it has a name already. */
        if !(*wc).name.is_null() {
            continue;
        }

        /* Select a name not currently present in the list. */
        loop {
            let mut lc2: *mut ListCell;
            let newname = format!("w{}", next_n);
            next_n += 1;
            let cnewname = std::ffi::CString::new(newname.clone()).unwrap();
            let mut matched = false;

            foreach!(lc2, activeWindows, {
                let wc2: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc2));

                if !(*wc2).name.is_null()
                    && std::ffi::CStr::from_ptr((*wc2).name).to_string_lossy() == newname
                {
                    matched = true;
                    break; /* matched */
                }
            });
            if !matched {
                (*wc).name = pstrdup(cnewname.as_ptr());
                break; /* reached the end with no match */
            }
        }
    });
}

/*
 * common_prefix_cmp
 *	  QSort comparison function for WindowClauseSortData
 */
unsafe fn common_prefix_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let wcsa: *const WindowClauseSortData = a as *const WindowClauseSortData;
    let wcsb: *const WindowClauseSortData = b as *const WindowClauseSortData;
    let mut item_a: *mut ListCell;
    let mut item_b: *mut ListCell;

    forboth!(item_a, (*wcsa).uniqueOrder, item_b, (*wcsb).uniqueOrder, {
        let sca: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, item_a);
        let scb: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, item_b);

        if (*sca).tleSortGroupRef > (*scb).tleSortGroupRef {
            return -1;
        } else if (*sca).tleSortGroupRef < (*scb).tleSortGroupRef {
            return 1;
        } else if (*sca).sortop > (*scb).sortop {
            return -1;
        } else if (*sca).sortop < (*scb).sortop {
            return 1;
        } else if (*sca).nulls_first && !(*scb).nulls_first {
            return -1;
        } else if !(*sca).nulls_first && (*scb).nulls_first {
            return 1;
        }
        /* no need to compare eqop, since it is fully determined by sortop */
    });

    if list_length((*wcsa).uniqueOrder) > list_length((*wcsb).uniqueOrder) {
        return -1;
    } else if list_length((*wcsa).uniqueOrder) < list_length((*wcsb).uniqueOrder) {
        return 1;
    }

    0
}

/*
 * make_window_input_target
 */
unsafe fn make_window_input_target(
    root: *mut PlannerInfo,
    final_target: *mut PathTarget,
    activeWindows: *mut List,
) -> *mut PathTarget {
    let input_target: *mut PathTarget;
    let mut sgrefs: *mut Bitmapset;
    let mut flattenable_cols: *mut List;
    let flattenable_vars: *mut List;
    let mut i: c_int;
    let mut lc: *mut ListCell;

    Assert!((*(*root).parse).hasWindowFuncs);

    /*
     * Collect the sortgroupref numbers of window PARTITION/ORDER BY clauses.
     */
    sgrefs = ptr::null_mut();
    foreach!(lc, activeWindows, {
        let wc: *mut WindowClause = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));
        let mut lc2: *mut ListCell;

        foreach!(lc2, (*wc).partitionClause, {
            let sortcl: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(lc2));

            sgrefs = bms_add_member(sgrefs, (*sortcl).tleSortGroupRef as c_int);
        });
        foreach!(lc2, (*wc).orderClause, {
            let sortcl: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(lc2));

            sgrefs = bms_add_member(sgrefs, (*sortcl).tleSortGroupRef as c_int);
        });
    });

    /* Add in sortgroupref numbers of GROUP BY clauses, too */
    foreach!(lc, (*root).processed_groupClause, {
        let grpcl: *mut SortGroupClause = lfirst_node!(SortGroupClause, T_SortGroupClause, current_cell!(lc));

        sgrefs = bms_add_member(sgrefs, (*grpcl).tleSortGroupRef as c_int);
    });

    /*
     * Construct a target containing all the non-flattenable targetlist items.
     */
    input_target = create_empty_pathtarget();
    flattenable_cols = NIL;

    i = 0;
    foreach!(lc, (*final_target).exprs, {
        let expr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
        let sgref: Index = get_pathtarget_sortgroupref(final_target, i);

        /*
         * Don't want to deconstruct window clauses or GROUP BY items.
         */
        if sgref != 0 && bms_is_member(sgref as c_int, sgrefs) {
            add_column_to_pathtarget(input_target, expr, sgref);
        } else {
            flattenable_cols = lappend(flattenable_cols, expr as *mut c_void);
        }

        i += 1;
    });

    /*
     * Pull out all the Vars and Aggrefs mentioned in flattenable columns.
     */
    flattenable_vars = pull_var_clause(
        flattenable_cols as *mut Node,
        PVC_INCLUDE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );
    add_new_columns_to_pathtarget(input_target, flattenable_vars);

    /* clean up cruft */
    list_free(flattenable_vars);
    list_free(flattenable_cols);

    /* XXX this causes some redundant cost calculation ... */
    set_pathtarget_cost_width(root, input_target)
}

/*
 * make_pathkeys_for_window
 */
unsafe fn make_pathkeys_for_window(
    root: *mut PlannerInfo,
    wc: *mut WindowClause,
    tlist: *mut List,
) -> *mut List {
    let mut window_pathkeys: *mut List = NIL;

    /* Throw error if can't sort */
    if !grouping_is_sortable((*wc).partitionClause) {
        ereport!(ERROR, errmsg!("could not implement window PARTITION BY"));
        unreachable!();
    }
    if !grouping_is_sortable((*wc).orderClause) {
        ereport!(ERROR, errmsg!("could not implement window ORDER BY"));
        unreachable!();
    }

    /*
     * First fetch the pathkeys for the PARTITION BY clause.
     */
    if (*wc).partitionClause != NIL {
        let mut sortable: bool = false;

        window_pathkeys = make_pathkeys_for_sortclauses_extended(
            root,
            &raw mut (*wc).partitionClause,
            tlist,
            true,
            false,
            &raw mut sortable,
            false,
        );

        Assert!(sortable);
    }

    /*
     * Fetch ORDER BY pathkeys.
     */
    if (*wc).orderClause != NIL {
        let orderby_pathkeys: *mut List;

        orderby_pathkeys = make_pathkeys_for_sortclauses(root, (*wc).orderClause, tlist);

        /* Okay, make the combined pathkeys */
        if window_pathkeys != NIL {
            window_pathkeys = append_pathkeys(window_pathkeys, orderby_pathkeys);
        } else {
            window_pathkeys = orderby_pathkeys;
        }
    }

    window_pathkeys
}

/*
 * make_sort_input_target
 */
unsafe fn make_sort_input_target(
    root: *mut PlannerInfo,
    final_target: *mut PathTarget,
    have_postponed_srfs: *mut bool,
) -> *mut PathTarget {
    let parse: *mut Query = (*root).parse;
    let input_target: *mut PathTarget;
    let ncols: c_int;
    let col_is_srf: *mut bool;
    let postpone_col: *mut bool;
    let mut have_srf: bool;
    let mut have_volatile: bool;
    let mut have_expensive: bool;
    let mut have_srf_sortcols: bool;
    let postpone_srfs: bool;
    let mut postponable_cols: *mut List;
    let postponable_vars: *mut List;
    let mut i: c_int;
    let mut lc: *mut ListCell;

    /* Shouldn't get here unless query has ORDER BY */
    Assert!(!(*parse).sortClause.is_null());

    *have_postponed_srfs = false; /* default result */

    /* Inspect tlist and collect per-column information */
    ncols = list_length((*final_target).exprs);
    col_is_srf = palloc0((ncols as usize) * std::mem::size_of::<bool>()) as *mut bool;
    postpone_col = palloc0((ncols as usize) * std::mem::size_of::<bool>()) as *mut bool;
    have_srf = false;
    have_volatile = false;
    have_expensive = false;
    have_srf_sortcols = false;

    i = 0;
    foreach!(lc, (*final_target).exprs, {
        let expr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;

        /*
         * If the column has a sortgroupref, assume it has to be evaluated
         * before sorting.
         */
        if get_pathtarget_sortgroupref(final_target, i) == 0 {
            if (*parse).hasTargetSRFs && expression_returns_set(expr as *mut Node) {
                /* We'll decide below whether these are postponable */
                *col_is_srf.add(i as usize) = true;
                have_srf = true;
            } else if contain_volatile_functions(expr as *mut Node) {
                /* Unconditionally postpone */
                *postpone_col.add(i as usize) = true;
                have_volatile = true;
            } else {
                let mut cost: QualCost = std::mem::zeroed();

                cost_qual_eval_node(&raw mut cost, expr as *mut Node, root);

                /*
                 * We arbitrarily define "expensive" as "more than 10X
                 * cpu_operator_cost".
                 */
                if cost.per_tuple > 10.0 * cpu_operator_cost {
                    *postpone_col.add(i as usize) = true;
                    have_expensive = true;
                }
            }
        } else {
            /* For sortgroupref cols, just check if any contain SRFs */
            if !have_srf_sortcols
                && (*parse).hasTargetSRFs
                && expression_returns_set(expr as *mut Node)
            {
                have_srf_sortcols = true;
            }
        }

        i += 1;
    });

    /*
     * We can postpone SRFs if we have some but none are in sortgroupref cols.
     */
    postpone_srfs = have_srf && !have_srf_sortcols;

    /*
     * If we don't need a post-sort projection, just return final_target.
     */
    if !(postpone_srfs
        || have_volatile
        || (have_expensive && (!(*parse).limitCount.is_null() || (*root).tuple_fraction > 0.0)))
    {
        return final_target;
    }

    /*
     * Report whether the post-sort projection will contain SRFs.
     */
    *have_postponed_srfs = postpone_srfs;

    /*
     * Construct the sort-input target.
     */
    input_target = create_empty_pathtarget();
    postponable_cols = NIL;

    i = 0;
    foreach!(lc, (*final_target).exprs, {
        let expr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;

        if *postpone_col.add(i as usize) || (postpone_srfs && *col_is_srf.add(i as usize)) {
            postponable_cols = lappend(postponable_cols, expr as *mut c_void);
        } else {
            add_column_to_pathtarget(input_target, expr, get_pathtarget_sortgroupref(final_target, i));
        }

        i += 1;
    });

    /*
     * Pull out all the Vars, Aggrefs, and WindowFuncs mentioned in postponable columns.
     */
    postponable_vars = pull_var_clause(
        postponable_cols as *mut Node,
        PVC_INCLUDE_AGGREGATES | PVC_INCLUDE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );
    add_new_columns_to_pathtarget(input_target, postponable_vars);

    /* clean up cruft */
    list_free(postponable_vars);
    list_free(postponable_cols);

    /* XXX this represents even more redundant cost calculation ... */
    set_pathtarget_cost_width(root, input_target)
}

/*
 * get_cheapest_fractional_path
 */
pub unsafe fn get_cheapest_fractional_path(rel: *mut RelOptInfo, mut tuple_fraction: f64) -> *mut Path {
    let mut best_path: *mut Path = (*rel).cheapest_total_path;
    let mut l: *mut ListCell;

    /* If all tuples will be retrieved, just return the cheapest-total path */
    if tuple_fraction <= 0.0 {
        return best_path;
    }

    /* Convert absolute # of tuples to a fraction; no need to clamp to 0..1 */
    if tuple_fraction >= 1.0 && (*best_path).rows > 0.0 {
        tuple_fraction /= (*best_path).rows;
    }

    foreach!(l, (*rel).pathlist, {
        let path: *mut Path = lfirst(current_cell!(l)) as *mut Path;

        if !(*path).param_info.is_null() {
            continue;
        }

        if path == (*rel).cheapest_total_path
            || compare_fractional_path_costs(best_path, path, tuple_fraction) <= 0
        {
            continue;
        }

        best_path = path;
    });

    best_path
}

/*
 * adjust_paths_for_srfs
 */
unsafe fn adjust_paths_for_srfs(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    targets: *mut List,
    targets_contain_srfs: *mut List,
) {
    let mut lc: *mut ListCell;

    Assert!(list_length(targets) == list_length(targets_contain_srfs));
    Assert!(linitial_int(targets_contain_srfs) == 0);

    /* If no SRFs appear at this plan level, nothing to do */
    if list_length(targets) == 1 {
        return;
    }

    /*
     * Stack SRF-evaluation nodes atop each path for the rel.
     */
    foreach!(lc, (*rel).pathlist, {
        let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let mut newpath: *mut Path = subpath;
        let mut lc1: *mut ListCell;
        let mut lc2: *mut ListCell;

        Assert!((*subpath).param_info.is_null());
        forboth!(lc1, targets, lc2, targets_contain_srfs, {
            let thistarget: *mut PathTarget = lfirst_node!(PathTarget, T_PathTarget, lc1);
            let contains_srfs: bool = lfirst_int(lc2) != 0;

            /* If this level doesn't contain SRFs, do regular projection */
            if contains_srfs {
                newpath = create_set_projection_path(root, rel, newpath, thistarget);
            } else {
                newpath = apply_projection_to_path(root, rel, newpath, thistarget);
            }
        });
        *(&raw mut (*current_cell!(lc)).ptr_value as *mut *mut Path) = newpath;
        if subpath == (*rel).cheapest_startup_path {
            (*rel).cheapest_startup_path = newpath;
        }
        if subpath == (*rel).cheapest_total_path {
            (*rel).cheapest_total_path = newpath;
        }
    });

    /* Likewise for partial paths, if any */
    foreach!(lc, (*rel).partial_pathlist, {
        let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let mut newpath: *mut Path = subpath;
        let mut lc1: *mut ListCell;
        let mut lc2: *mut ListCell;

        Assert!((*subpath).param_info.is_null());
        forboth!(lc1, targets, lc2, targets_contain_srfs, {
            let thistarget: *mut PathTarget = lfirst_node!(PathTarget, T_PathTarget, lc1);
            let contains_srfs: bool = lfirst_int(lc2) != 0;

            /* If this level doesn't contain SRFs, do regular projection */
            if contains_srfs {
                newpath = create_set_projection_path(root, rel, newpath, thistarget);
            } else {
                /* avoid apply_projection_to_path, in case of multiple refs */
                newpath = create_projection_path(root, rel, newpath, thistarget);
            }
        });
        *(&raw mut (*current_cell!(lc)).ptr_value as *mut *mut Path) = newpath;
    });
}

/*
 * expression_planner
 */
pub unsafe fn expression_planner(expr: *mut Expr) -> *mut Expr {
    let result: *mut Node;

    /*
     * Convert named-argument function calls, insert default arguments and
     * simplify constant subexprs
     */
    result = eval_const_expressions(ptr::null_mut(), expr as *mut Node);

    /* Fill in opfuncid values if missing */
    fix_opfuncids(result);

    result as *mut Expr
}

/*
 * expression_planner_with_deps
 */
pub unsafe fn expression_planner_with_deps(
    expr: *mut Expr,
    relationOids: *mut *mut List,
    invalItems: *mut *mut List,
) -> *mut Expr {
    let result: *mut Node;
    let mut glob: PlannerGlobal = std::mem::zeroed();
    let mut root: PlannerInfo = std::mem::zeroed();

    /* Make up dummy planner state so we can use setrefs machinery */
    ptr::write_bytes(&raw mut glob, 0, 1);
    glob.r#type = T_PlannerGlobal;
    glob.relationOids = NIL;
    glob.invalItems = NIL;

    ptr::write_bytes(&raw mut root, 0, 1);
    root.r#type = T_PlannerInfo;
    root.glob = &raw mut glob;

    /*
     * Convert named-argument function calls, insert default arguments and
     * simplify constant subexprs.
     */
    result = eval_const_expressions(&raw mut root, expr as *mut Node);

    /* Fill in opfuncid values if missing */
    fix_opfuncids(result);

    /*
     * Now walk the finished expression to find anything else we ought to
     * record as an expression dependency.
     */
    extract_query_dependencies_walker(result, &raw mut root);

    *relationOids = glob.relationOids;
    *invalItems = glob.invalItems;

    result as *mut Expr
}

/*
 * plan_cluster_use_sort
 */
pub unsafe fn plan_cluster_use_sort(tableOid: Oid, indexOid: Oid) -> bool {
    let root: *mut PlannerInfo;
    let query: *mut Query;
    let glob: *mut PlannerGlobal;
    let rte: *mut RangeTblEntry;
    let rel: *mut RelOptInfo;
    let mut indexInfo: *mut IndexOptInfo;
    let mut indexExprCost: QualCost = std::mem::zeroed();
    let comparisonCost: Cost;
    let seqScanPath: *mut Path;
    let mut seqScanAndSortPath: Path = std::mem::zeroed();
    let indexScanPath: *mut IndexPath;
    let mut lc: *mut ListCell;

    /* We can short-circuit the cost comparison if indexscans are disabled */
    if !enable_indexscan {
        return true; /* use sort */
    }

    /* Set up mostly-dummy planner state */
    query = makeNode!(Query, T_Query);
    (*query).commandType = CMD_SELECT;

    glob = makeNode!(PlannerGlobal, T_PlannerGlobal);

    root = makeNode!(PlannerInfo, T_PlannerInfo);
    (*root).parse = query;
    (*root).glob = glob;
    (*root).query_level = 1;
    (*root).planner_cxt = CurrentMemoryContext as *mut c_void;
    (*root).wt_param_id = -1;
    (*root).join_domains = list_make1!(makeNode!(JoinDomain, T_JoinDomain));

    /* Build a minimal RTE for the rel */
    rte = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*rte).rtekind = RTE_RELATION;
    (*rte).relid = tableOid;
    (*rte).relkind = RELKIND_RELATION; /* Don't be too picky. */
    (*rte).rellockmode = AccessShareLock;
    (*rte).lateral = false;
    (*rte).inh = false;
    (*rte).inFromCl = true;
    (*query).rtable = list_make1!(rte);
    addRTEPermissionInfo(&raw mut (*query).rteperminfos, rte);

    /* Set up RTE/RelOptInfo arrays */
    setup_simple_rel_arrays(root);

    /* Build RelOptInfo */
    rel = build_simple_rel(root, 1, ptr::null_mut());

    /* Locate IndexOptInfo for the target index */
    indexInfo = ptr::null_mut();
    let mut found = false;
    foreach!(lc, (*rel).indexlist, {
        indexInfo = lfirst_node!(IndexOptInfo, T_IndexOptInfo, current_cell!(lc));
        if (*indexInfo).indexoid == indexOid {
            found = true;
            break;
        }
    });

    /*
     * It's possible that get_relation_info did not generate an IndexOptInfo.
     */
    if !found {
        /* not in the list? */
        return true; /* use sort */
    }

    /*
     * Rather than doing all the pushups, just do a quick hack for rows and width.
     */
    (*rel).rows = (*rel).tuples;
    (*(*rel).reltarget).width = get_relation_data_width(tableOid, ptr::null_mut());

    (*root).total_table_pages = (*rel).pages as f64;

    /*
     * Determine eval cost of the index expressions, if any.
     */
    cost_qual_eval(&raw mut indexExprCost, (*indexInfo).indexprs, root);
    comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);

    /* Estimate the cost of seq scan + sort */
    seqScanPath = create_seqscan_path(root, rel, ptr::null_mut(), 0);
    cost_sort(
        &raw mut seqScanAndSortPath,
        root,
        NIL,
        (*seqScanPath).disabled_nodes,
        (*seqScanPath).total_cost,
        (*rel).tuples,
        (*(*rel).reltarget).width,
        comparisonCost,
        maintenance_work_mem,
        -1.0,
    );

    /* Estimate the cost of index scan */
    indexScanPath = create_index_path(
        root,
        indexInfo,
        NIL,
        NIL,
        NIL,
        NIL,
        ForwardScanDirection,
        false,
        ptr::null_mut(),
        1.0,
        false,
    );

    seqScanAndSortPath.total_cost < (*indexScanPath).path.total_cost
}

/*
 * plan_create_index_workers
 */
pub unsafe fn plan_create_index_workers(tableOid: Oid, indexOid: Oid) -> c_int {
    let root: *mut PlannerInfo;
    let query: *mut Query;
    let glob: *mut PlannerGlobal;
    let rte: *mut RangeTblEntry;
    let heap: Relation;
    let index: Relation;
    let rel: *mut RelOptInfo;
    let mut parallel_workers: c_int;
    let mut heap_blocks: BlockNumber = 0;
    let mut reltuples: f64 = 0.0;
    let mut allvisfrac: f64 = 0.0;

    /*
     * We don't allow performing parallel operation in standalone backend or
     * when parallelism is disabled.
     */
    if !IsUnderPostmaster || max_parallel_maintenance_workers == 0 {
        return 0;
    }

    /* Set up largely-dummy planner state */
    query = makeNode!(Query, T_Query);
    (*query).commandType = CMD_SELECT;

    glob = makeNode!(PlannerGlobal, T_PlannerGlobal);

    root = makeNode!(PlannerInfo, T_PlannerInfo);
    (*root).parse = query;
    (*root).glob = glob;
    (*root).query_level = 1;
    (*root).planner_cxt = CurrentMemoryContext as *mut c_void;
    (*root).wt_param_id = -1;
    (*root).join_domains = list_make1!(makeNode!(JoinDomain, T_JoinDomain));

    /*
     * Build a minimal RTE.
     */
    rte = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*rte).rtekind = RTE_RELATION;
    (*rte).relid = tableOid;
    (*rte).relkind = RELKIND_RELATION; /* Don't be too picky. */
    (*rte).rellockmode = AccessShareLock;
    (*rte).lateral = false;
    (*rte).inh = true;
    (*rte).inFromCl = true;
    (*query).rtable = list_make1!(rte);
    addRTEPermissionInfo(&raw mut (*query).rteperminfos, rte);

    /* Set up RTE/RelOptInfo arrays */
    setup_simple_rel_arrays(root);

    /* Build RelOptInfo */
    rel = build_simple_rel(root, 1, ptr::null_mut());

    /* Rels are assumed already locked by the caller */
    heap = table_open(tableOid, NoLock);
    index = index_open(indexOid, NoLock);

    /*
     * Determine if it's safe to proceed.
     */
    if relation_is_temp(heap)
        || !is_parallel_safe(root, RelationGetIndexExpressions(index) as *mut Node)
        || !is_parallel_safe(root, RelationGetIndexPredicate(index) as *mut Node)
    {
        parallel_workers = 0;
    } else if (*rel).rel_parallel_workers != -1 {
        /*
         * If parallel_workers storage parameter is set for the table.
         */
        parallel_workers = std::cmp::min((*rel).rel_parallel_workers, max_parallel_maintenance_workers);
    } else {
        /*
         * Estimate heap relation size ourselves.
         */
        estimate_rel_size(
            heap,
            ptr::null_mut(),
            &raw mut heap_blocks,
            &raw mut reltuples,
            &raw mut allvisfrac,
        );

        /*
         * Determine number of workers to scan the heap relation.
         */
        parallel_workers = compute_parallel_worker(rel, heap_blocks as f64, -1.0, max_parallel_maintenance_workers);

        /*
         * Cap workers based on available maintenance_work_mem as needed.
         */
        while parallel_workers > 0 && maintenance_work_mem / (parallel_workers + 1) < 32 * 1024 {
            parallel_workers -= 1;
        }
    }

    index_close(index, NoLock);
    table_close(heap, NoLock);

    parallel_workers
}

unsafe fn relation_is_temp(_rel: Relation) -> bool { false } // TODO(pg-port): rd_rel->relpersistence == RELPERSISTENCE_TEMP

/*
 * add_paths_to_grouping_rel
 */
unsafe fn add_paths_to_grouping_rel(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    grouped_rel: *mut RelOptInfo,
    partially_grouped_rel: *mut RelOptInfo,
    agg_costs: *const AggClauseCosts,
    gd: *mut grouping_sets_data,
    dNumGroups: f64,
    extra: *mut GroupPathExtraData,
) {
    let parse: *mut Query = (*root).parse;
    let cheapest_path: *mut Path = (*input_rel).cheapest_total_path;
    let mut lc: *mut ListCell;
    let can_hash: bool = ((*extra).flags & GROUPING_CAN_USE_HASH) != 0;
    let can_sort: bool = ((*extra).flags & GROUPING_CAN_USE_SORT) != 0;
    let havingQual: *mut List = (*extra).havingQual as *mut List;
    let agg_final_costs: *mut AggClauseCosts = &raw mut (*extra).agg_final_costs;

    if can_sort {
        /*
         * Use any available suitably-sorted path as input.
         */
        foreach!(lc, (*input_rel).pathlist, {
            let mut lc2: *mut ListCell;
            let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let path_save: *mut Path = path;
            let pathkey_orderings: *mut List;

            /* generate alternative group orderings that might be useful */
            pathkey_orderings = get_useful_group_keys_orderings(root, path);

            Assert!(list_length(pathkey_orderings) > 0);

            foreach!(lc2, pathkey_orderings, {
                let info: *mut GroupByOrdering = lfirst(current_cell!(lc2)) as *mut GroupByOrdering;

                /* restore the path (we replace it in the loop) */
                path = path_save;

                path = make_ordered_path(root, grouped_rel, path, cheapest_path, (*info).pathkeys, -1.0);
                if path.is_null() {
                    continue;
                }

                /* Now decide what to stick atop it */
                if !(*parse).groupingSets.is_null() {
                    consider_groupingsets_paths(root, grouped_rel, path, true, can_hash, gd, agg_costs, dNumGroups);
                } else if (*parse).hasAggs {
                    /*
                     * We have aggregation, possibly with plain GROUP BY.
                     */
                    add_path(
                        grouped_rel,
                        create_agg_path(
                            root,
                            grouped_rel,
                            path,
                            (*grouped_rel).reltarget,
                            if !(*parse).groupClause.is_null() { AGG_SORTED } else { AGG_PLAIN },
                            AGGSPLIT_SIMPLE,
                            (*info).clauses,
                            havingQual,
                            agg_costs,
                            dNumGroups,
                        ),
                    );
                } else if !(*parse).groupClause.is_null() {
                    /*
                     * We have GROUP BY without aggregation or grouping sets.
                     */
                    add_path(
                        grouped_rel,
                        create_group_path(root, grouped_rel, path, (*info).clauses, havingQual, dNumGroups),
                    );
                } else {
                    /* Other cases should have been handled above */
                    Assert!(false);
                }
            });
        });

        /*
         * Instead of operating directly on the input relation, we can
         * consider finalizing a partially aggregated path.
         */
        if !partially_grouped_rel.is_null() {
            foreach!(lc, (*partially_grouped_rel).pathlist, {
                let mut lc2: *mut ListCell;
                let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
                let path_save: *mut Path = path;
                let pathkey_orderings: *mut List;

                pathkey_orderings = get_useful_group_keys_orderings(root, path);

                Assert!(list_length(pathkey_orderings) > 0);

                foreach!(lc2, pathkey_orderings, {
                    let info: *mut GroupByOrdering = lfirst(current_cell!(lc2)) as *mut GroupByOrdering;

                    path = path_save;

                    path = make_ordered_path(
                        root,
                        grouped_rel,
                        path,
                        (*partially_grouped_rel).cheapest_total_path,
                        (*info).pathkeys,
                        -1.0,
                    );

                    if path.is_null() {
                        continue;
                    }

                    if (*parse).hasAggs {
                        add_path(
                            grouped_rel,
                            create_agg_path(
                                root,
                                grouped_rel,
                                path,
                                (*grouped_rel).reltarget,
                                if !(*parse).groupClause.is_null() { AGG_SORTED } else { AGG_PLAIN },
                                AGGSPLIT_FINAL_DESERIAL,
                                (*info).clauses,
                                havingQual,
                                agg_final_costs,
                                dNumGroups,
                            ),
                        );
                    } else {
                        add_path(
                            grouped_rel,
                            create_group_path(root, grouped_rel, path, (*info).clauses, havingQual, dNumGroups),
                        );
                    }
                });
            });
        }
    }

    if can_hash {
        if !(*parse).groupingSets.is_null() {
            /*
             * Try for a hash-only groupingsets path over unsorted input.
             */
            consider_groupingsets_paths(root, grouped_rel, cheapest_path, false, true, gd, agg_costs, dNumGroups);
        } else {
            /*
             * Generate a HashAgg Path.
             */
            add_path(
                grouped_rel,
                create_agg_path(
                    root,
                    grouped_rel,
                    cheapest_path,
                    (*grouped_rel).reltarget,
                    AGG_HASHED,
                    AGGSPLIT_SIMPLE,
                    (*root).processed_groupClause,
                    havingQual,
                    agg_costs,
                    dNumGroups,
                ),
            );
        }

        /*
         * Generate a Finalize HashAgg Path atop of the cheapest partially
         * grouped path, assuming there is one.
         */
        if !partially_grouped_rel.is_null() && !(*partially_grouped_rel).pathlist.is_null() {
            let path: *mut Path = (*partially_grouped_rel).cheapest_total_path;

            add_path(
                grouped_rel,
                create_agg_path(
                    root,
                    grouped_rel,
                    path,
                    (*grouped_rel).reltarget,
                    AGG_HASHED,
                    AGGSPLIT_FINAL_DESERIAL,
                    (*root).processed_groupClause,
                    havingQual,
                    agg_final_costs,
                    dNumGroups,
                ),
            );
        }
    }

    /*
     * When partitionwise aggregate is used.
     */
    if (*grouped_rel).partial_pathlist != NIL {
        gather_grouping_paths(root, grouped_rel);
    }
}

/*
 * create_partial_grouping_paths
 */
unsafe fn create_partial_grouping_paths(
    root: *mut PlannerInfo,
    grouped_rel: *mut RelOptInfo,
    input_rel: *mut RelOptInfo,
    gd: *mut grouping_sets_data,
    extra: *mut GroupPathExtraData,
    force_rel_creation: bool,
) -> *mut RelOptInfo {
    let parse: *mut Query = (*root).parse;
    let partially_grouped_rel: *mut RelOptInfo;
    let agg_partial_costs: *mut AggClauseCosts = &raw mut (*extra).agg_partial_costs;
    let agg_final_costs: *mut AggClauseCosts = &raw mut (*extra).agg_final_costs;
    let mut cheapest_partial_path: *mut Path = ptr::null_mut();
    let mut cheapest_total_path: *mut Path = ptr::null_mut();
    let mut dNumPartialGroups: f64 = 0.0;
    let mut dNumPartialPartialGroups: f64 = 0.0;
    let mut lc: *mut ListCell;
    let can_hash: bool = ((*extra).flags & GROUPING_CAN_USE_HASH) != 0;
    let can_sort: bool = ((*extra).flags & GROUPING_CAN_USE_SORT) != 0;

    /*
     * Consider whether we should generate partially aggregated non-partial paths.
     */
    if (*input_rel).pathlist != NIL && (*extra).patype == PARTITIONWISE_AGGREGATE_PARTIAL {
        cheapest_total_path = (*input_rel).cheapest_total_path;
    }

    /*
     * If parallelism is possible for grouped_rel.
     */
    if (*grouped_rel).consider_parallel && (*input_rel).partial_pathlist != NIL {
        cheapest_partial_path = linitial((*input_rel).partial_pathlist) as *mut Path;
    }

    /*
     * If we can't partially aggregate, don't bother creating the new RelOptInfo.
     */
    if cheapest_total_path.is_null() && cheapest_partial_path.is_null() && !force_rel_creation {
        return ptr::null_mut();
    }

    /*
     * Build a new upper relation.
     */
    partially_grouped_rel = fetch_upper_rel(root, UPPERREL_PARTIAL_GROUP_AGG, (*grouped_rel).relids);
    (*partially_grouped_rel).consider_parallel = (*grouped_rel).consider_parallel;
    (*partially_grouped_rel).reloptkind = (*grouped_rel).reloptkind;
    (*partially_grouped_rel).serverid = (*grouped_rel).serverid;
    (*partially_grouped_rel).userid = (*grouped_rel).userid;
    (*partially_grouped_rel).useridiscurrent = (*grouped_rel).useridiscurrent;
    (*partially_grouped_rel).fdwroutine = (*grouped_rel).fdwroutine;

    /*
     * Build target list for partial aggregate paths.
     */
    (*partially_grouped_rel).reltarget =
        make_partial_grouping_target(root, (*grouped_rel).reltarget, (*extra).havingQual);

    if !(*extra).partial_costs_set {
        /*
         * Collect statistics about aggregates for estimating costs.
         */
        ptr::write_bytes(agg_partial_costs, 0, 1);
        ptr::write_bytes(agg_final_costs, 0, 1);
        if (*parse).hasAggs {
            /* partial phase */
            get_agg_clause_costs(root, AGGSPLIT_INITIAL_SERIAL, agg_partial_costs);

            /* final phase */
            get_agg_clause_costs(root, AGGSPLIT_FINAL_DESERIAL, agg_final_costs);
        }

        (*extra).partial_costs_set = true;
    }

    /* Estimate number of partial groups. */
    if !cheapest_total_path.is_null() {
        dNumPartialGroups = get_number_of_groups(root, (*cheapest_total_path).rows, gd, (*extra).targetList);
    }
    if !cheapest_partial_path.is_null() {
        dNumPartialPartialGroups =
            get_number_of_groups(root, (*cheapest_partial_path).rows, gd, (*extra).targetList);
    }

    if can_sort && !cheapest_total_path.is_null() {
        /* This should have been checked previously */
        Assert!((*parse).hasAggs || !(*parse).groupClause.is_null());

        /*
         * Use any available suitably-sorted path as input.
         */
        foreach!(lc, (*input_rel).pathlist, {
            let mut lc2: *mut ListCell;
            let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let path_save: *mut Path = path;
            let pathkey_orderings: *mut List;

            pathkey_orderings = get_useful_group_keys_orderings(root, path);

            Assert!(list_length(pathkey_orderings) > 0);

            foreach!(lc2, pathkey_orderings, {
                let info: *mut GroupByOrdering = lfirst(current_cell!(lc2)) as *mut GroupByOrdering;

                path = path_save;

                path = make_ordered_path(root, partially_grouped_rel, path, cheapest_total_path, (*info).pathkeys, -1.0);

                if path.is_null() {
                    continue;
                }

                if (*parse).hasAggs {
                    add_path(
                        partially_grouped_rel,
                        create_agg_path(
                            root,
                            partially_grouped_rel,
                            path,
                            (*partially_grouped_rel).reltarget,
                            if !(*parse).groupClause.is_null() { AGG_SORTED } else { AGG_PLAIN },
                            AGGSPLIT_INITIAL_SERIAL,
                            (*info).clauses,
                            NIL,
                            agg_partial_costs,
                            dNumPartialGroups,
                        ),
                    );
                } else {
                    add_path(
                        partially_grouped_rel,
                        create_group_path(root, partially_grouped_rel, path, (*info).clauses, NIL, dNumPartialGroups),
                    );
                }
            });
        });
    }

    if can_sort && !cheapest_partial_path.is_null() {
        /* Similar to above logic, but for partial paths. */
        foreach!(lc, (*input_rel).partial_pathlist, {
            let mut lc2: *mut ListCell;
            let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
            let path_save: *mut Path = path;
            let pathkey_orderings: *mut List;

            pathkey_orderings = get_useful_group_keys_orderings(root, path);

            Assert!(list_length(pathkey_orderings) > 0);

            foreach!(lc2, pathkey_orderings, {
                let info: *mut GroupByOrdering = lfirst(current_cell!(lc2)) as *mut GroupByOrdering;

                path = path_save;

                path = make_ordered_path(root, partially_grouped_rel, path, cheapest_partial_path, (*info).pathkeys, -1.0);

                if path.is_null() {
                    continue;
                }

                if (*parse).hasAggs {
                    add_partial_path(
                        partially_grouped_rel,
                        create_agg_path(
                            root,
                            partially_grouped_rel,
                            path,
                            (*partially_grouped_rel).reltarget,
                            if !(*parse).groupClause.is_null() { AGG_SORTED } else { AGG_PLAIN },
                            AGGSPLIT_INITIAL_SERIAL,
                            (*info).clauses,
                            NIL,
                            agg_partial_costs,
                            dNumPartialPartialGroups,
                        ),
                    );
                } else {
                    add_partial_path(
                        partially_grouped_rel,
                        create_group_path(root, partially_grouped_rel, path, (*info).clauses, NIL, dNumPartialPartialGroups),
                    );
                }
            });
        });
    }

    /*
     * Add a partially-grouped HashAgg Path where possible.
     */
    if can_hash && !cheapest_total_path.is_null() {
        /* Checked above */
        Assert!((*parse).hasAggs || !(*parse).groupClause.is_null());

        add_path(
            partially_grouped_rel,
            create_agg_path(
                root,
                partially_grouped_rel,
                cheapest_total_path,
                (*partially_grouped_rel).reltarget,
                AGG_HASHED,
                AGGSPLIT_INITIAL_SERIAL,
                (*root).processed_groupClause,
                NIL,
                agg_partial_costs,
                dNumPartialGroups,
            ),
        );
    }

    /*
     * Now add a partially-grouped HashAgg partial Path where possible.
     */
    if can_hash && !cheapest_partial_path.is_null() {
        add_partial_path(
            partially_grouped_rel,
            create_agg_path(
                root,
                partially_grouped_rel,
                cheapest_partial_path,
                (*partially_grouped_rel).reltarget,
                AGG_HASHED,
                AGGSPLIT_INITIAL_SERIAL,
                (*root).processed_groupClause,
                NIL,
                agg_partial_costs,
                dNumPartialPartialGroups,
            ),
        );
    }

    /*
     * If there is an FDW.
     */
    if !(*partially_grouped_rel).fdwroutine.is_null()
        && fdw_has_GetForeignUpperPaths((*partially_grouped_rel).fdwroutine)
    {
        fdw_GetForeignUpperPaths(
            (*partially_grouped_rel).fdwroutine,
            root,
            UPPERREL_PARTIAL_GROUP_AGG,
            input_rel,
            partially_grouped_rel,
            extra as *mut c_void,
        );
    }

    partially_grouped_rel
}

/*
 * make_ordered_path
 */
unsafe fn make_ordered_path(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    mut path: *mut Path,
    cheapest_path: *mut Path,
    pathkeys: *mut List,
    limit_tuples: f64,
) -> *mut Path {
    let is_sorted: bool;
    let mut presorted_keys: c_int = 0;

    is_sorted = pathkeys_count_contained_in(pathkeys, (*path).pathkeys, &raw mut presorted_keys);

    if !is_sorted {
        /*
         * Try at least sorting the cheapest path.
         */
        if path != cheapest_path && (presorted_keys == 0 || !enable_incremental_sort) {
            return ptr::null_mut();
        }

        if presorted_keys == 0 || !enable_incremental_sort {
            path = create_sort_path(root, rel, path, pathkeys, limit_tuples);
        } else {
            path = create_incremental_sort_path(root, rel, path, pathkeys, presorted_keys, limit_tuples);
        }
    }

    path
}

/*
 * Generate Gather and Gather Merge paths for a grouping relation.
 */
unsafe fn gather_grouping_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    let mut lc: *mut ListCell;
    let cheapest_partial_path: *mut Path;
    let groupby_pathkeys: *mut List;

    /*
     * Trim off any pathkeys added for ORDER BY / DISTINCT aggregates.
     */
    if list_length((*root).group_pathkeys) > (*root).num_groupby_pathkeys {
        groupby_pathkeys = list_copy_head((*root).group_pathkeys, (*root).num_groupby_pathkeys);
    } else {
        groupby_pathkeys = (*root).group_pathkeys;
    }

    /* Try Gather for unordered paths and Gather Merge for ordered ones. */
    generate_useful_gather_paths(root, rel, true);

    cheapest_partial_path = linitial((*rel).partial_pathlist) as *mut Path;

    /* XXX Shouldn't this also consider the group-key-reordering? */
    foreach!(lc, (*rel).partial_pathlist, {
        let mut path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let is_sorted: bool;
        let mut presorted_keys: c_int = 0;
        let mut total_groups: f64;

        is_sorted = pathkeys_count_contained_in(groupby_pathkeys, (*path).pathkeys, &raw mut presorted_keys);

        if is_sorted {
            continue;
        }

        if path != cheapest_partial_path && (presorted_keys == 0 || !enable_incremental_sort) {
            continue;
        }

        if presorted_keys == 0 || !enable_incremental_sort {
            path = create_sort_path(root, rel, path, groupby_pathkeys, -1.0);
        } else {
            path = create_incremental_sort_path(root, rel, path, groupby_pathkeys, presorted_keys, -1.0);
        }
        total_groups = compute_gather_rows(path);
        path = create_gather_merge_path(
            root,
            rel,
            path,
            (*rel).reltarget,
            groupby_pathkeys,
            ptr::null_mut(),
            &raw mut total_groups,
        );

        add_path(rel, path);
    });
}

/*
 * can_partial_agg
 */
unsafe fn can_partial_agg(root: *mut PlannerInfo) -> bool {
    let parse: *mut Query = (*root).parse;

    if !(*parse).hasAggs && (*parse).groupClause == NIL {
        /*
         * We don't know how to do parallel aggregation unless we have either
         * some aggregates or a grouping clause.
         */
        false
    } else if !(*parse).groupingSets.is_null() {
        /* We don't know how to do grouping sets in parallel. */
        false
    } else if (*root).hasNonPartialAggs || (*root).hasNonSerialAggs {
        /* Insufficient support for partial mode. */
        false
    } else {
        /* Everything looks good. */
        true
    }
}

/*
 * apply_scanjoin_target_to_paths
 */
unsafe fn apply_scanjoin_target_to_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    scanjoin_targets: *mut List,
    scanjoin_targets_contain_srfs: *mut List,
    scanjoin_target_parallel_safe: bool,
    tlist_same_exprs: bool,
) {
    let rel_is_partitioned: bool = IS_PARTITIONED_REL(rel);
    let scanjoin_target: *mut PathTarget;
    let mut lc: *mut ListCell;

    /* This recurses, so be paranoid. */
    check_stack_depth();

    /*
     * If the rel is partitioned, we want to drop its existing paths.
     */
    if rel_is_partitioned {
        (*rel).pathlist = NIL;
    }

    /*
     * If the scan/join target is not parallel-safe, partial paths cannot
     * generate it.
     */
    if !scanjoin_target_parallel_safe {
        generate_useful_gather_paths(root, rel, false);

        /* Can't use parallel query above this level. */
        (*rel).partial_pathlist = NIL;
        (*rel).consider_parallel = false;
    }

    /* Finish dropping old paths for a partitioned rel, per comment above */
    if rel_is_partitioned {
        (*rel).partial_pathlist = NIL;
    }

    /* Extract SRF-free scan/join target. */
    scanjoin_target = lfirst_node!(PathTarget, T_PathTarget, list_head(scanjoin_targets));

    /*
     * Apply the SRF-free scan/join target to each existing path.
     */
    foreach!(lc, (*rel).pathlist, {
        let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;

        /* Shouldn't have any parameterized paths anymore */
        Assert!((*subpath).param_info.is_null());

        if tlist_same_exprs {
            (*(*subpath).pathtarget).sortgrouprefs = (*scanjoin_target).sortgrouprefs;
        } else {
            let newpath: *mut Path;

            newpath = create_projection_path(root, rel, subpath, scanjoin_target);
            *(&raw mut (*current_cell!(lc)).ptr_value as *mut *mut Path) = newpath;
        }
    });

    /* Likewise adjust the targets for any partial paths. */
    foreach!(lc, (*rel).partial_pathlist, {
        let subpath: *mut Path = lfirst(current_cell!(lc)) as *mut Path;

        /* Shouldn't have any parameterized paths anymore */
        Assert!((*subpath).param_info.is_null());

        if tlist_same_exprs {
            (*(*subpath).pathtarget).sortgrouprefs = (*scanjoin_target).sortgrouprefs;
        } else {
            let newpath: *mut Path;

            newpath = create_projection_path(root, rel, subpath, scanjoin_target);
            *(&raw mut (*current_cell!(lc)).ptr_value as *mut *mut Path) = newpath;
        }
    });

    /*
     * Now, if final scan/join target contains SRFs, insert ProjectSetPath(s).
     */
    if (*(*root).parse).hasTargetSRFs {
        adjust_paths_for_srfs(root, rel, scanjoin_targets, scanjoin_targets_contain_srfs);
    }

    /*
     * Update the rel's target to be the final (with SRFs) scan/join target.
     */
    (*rel).reltarget = llast_node_pathtarget(scanjoin_targets);

    /*
     * If the relation is partitioned, recursively apply the scan/join target.
     */
    if rel_is_partitioned {
        let mut live_children: *mut List = NIL;
        let mut i: c_int;

        /* Adjust each partition. */
        i = -1;
        loop {
            i = bms_next_member((*rel).live_parts, i);
            if i < 0 {
                break;
            }
            let child_rel: *mut RelOptInfo = *(*rel).part_rels.add(i as usize);
            let mut appinfos: *mut *mut c_void;
            let mut nappinfos: c_int = 0;
            let mut child_scanjoin_targets: *mut List = NIL;

            Assert!(!child_rel.is_null());

            /* Dummy children can be ignored. */
            if IS_DUMMY_REL(child_rel) {
                continue;
            }

            /* Translate scan/join targets for this child. */
            appinfos = find_appinfos_by_relids(root, (*child_rel).relids, &raw mut nappinfos);
            foreach!(lc, scanjoin_targets, {
                let mut target: *mut PathTarget = lfirst_node!(PathTarget, T_PathTarget, current_cell!(lc));

                target = copy_pathtarget(target);
                (*target).exprs = adjust_appendrel_attrs(
                    root,
                    (*target).exprs as *mut Node,
                    nappinfos,
                    appinfos,
                ) as *mut List;
                child_scanjoin_targets = lappend(child_scanjoin_targets, target as *mut c_void);
            });
            pfree(appinfos as *mut c_void);

            /* Recursion does the real work. */
            apply_scanjoin_target_to_paths(
                root,
                child_rel,
                child_scanjoin_targets,
                scanjoin_targets_contain_srfs,
                scanjoin_target_parallel_safe,
                tlist_same_exprs,
            );

            /* Save non-dummy children for Append paths. */
            if !IS_DUMMY_REL(child_rel) {
                live_children = lappend(live_children, child_rel as *mut c_void);
            }
        }

        /* Build new paths for this relation by appending child paths. */
        add_paths_to_append_rel(root, rel, live_children);
    }

    /*
     * Consider generating Gather or Gather Merge paths.
     */
    if (*rel).consider_parallel && !IS_OTHER_REL(rel) {
        generate_useful_gather_paths(root, rel, false);
    }

    /*
     * Reassess which paths are the cheapest.
     */
    set_cheapest(rel);
}

unsafe fn llast_node_pathtarget(list: *mut List) -> *mut PathTarget {
    list_nth(list, list_length(list) - 1) as *mut PathTarget
}

/*
 * create_partitionwise_grouping_paths
 */
unsafe fn create_partitionwise_grouping_paths(
    root: *mut PlannerInfo,
    input_rel: *mut RelOptInfo,
    grouped_rel: *mut RelOptInfo,
    partially_grouped_rel: *mut RelOptInfo,
    agg_costs: *const AggClauseCosts,
    gd: *mut grouping_sets_data,
    patype: PartitionwiseAggregateType,
    extra: *mut GroupPathExtraData,
) {
    let mut grouped_live_children: *mut List = NIL;
    let mut partially_grouped_live_children: *mut List = NIL;
    let target: *mut PathTarget = (*grouped_rel).reltarget;
    let mut partial_grouping_valid: bool = true;
    let mut i: c_int;

    Assert!(patype != PARTITIONWISE_AGGREGATE_NONE);
    Assert!(patype != PARTITIONWISE_AGGREGATE_PARTIAL || !partially_grouped_rel.is_null());

    /* Add paths for partitionwise aggregation/grouping. */
    i = -1;
    loop {
        i = bms_next_member((*input_rel).live_parts, i);
        if i < 0 {
            break;
        }
        let child_input_rel: *mut RelOptInfo = *(*input_rel).part_rels.add(i as usize);
        let child_target: *mut PathTarget;
        let appinfos: *mut *mut c_void;
        let mut nappinfos: c_int = 0;
        let mut child_extra: GroupPathExtraData = std::mem::zeroed();
        let child_grouped_rel: *mut RelOptInfo;
        let mut child_partially_grouped_rel: *mut RelOptInfo = ptr::null_mut();

        Assert!(!child_input_rel.is_null());

        /* Dummy children can be ignored. */
        if IS_DUMMY_REL(child_input_rel) {
            continue;
        }

        child_target = copy_pathtarget(target);

        /*
         * Copy the given "extra" structure as is and then override.
         */
        ptr::copy_nonoverlapping(extra, &raw mut child_extra, 1);

        appinfos = find_appinfos_by_relids(root, (*child_input_rel).relids, &raw mut nappinfos);

        (*child_target).exprs =
            adjust_appendrel_attrs(root, (*target).exprs as *mut Node, nappinfos, appinfos) as *mut List;

        /* Translate havingQual and targetList. */
        child_extra.havingQual = adjust_appendrel_attrs(root, (*extra).havingQual, nappinfos, appinfos);
        child_extra.targetList =
            adjust_appendrel_attrs(root, (*extra).targetList as *mut Node, nappinfos, appinfos) as *mut List;

        /*
         * extra->patype was the value computed for our parent rel.
         */
        child_extra.patype = patype;

        /*
         * Create grouping relation to hold fully aggregated grouping for the child.
         */
        child_grouped_rel = make_grouping_rel(
            root,
            child_input_rel,
            child_target,
            (*extra).target_parallel_safe,
            child_extra.havingQual,
        );

        /* Create grouping paths for this child relation. */
        create_ordinary_grouping_paths(
            root,
            child_input_rel,
            child_grouped_rel,
            agg_costs,
            gd,
            &raw mut child_extra,
            &raw mut child_partially_grouped_rel,
        );

        if !child_partially_grouped_rel.is_null() {
            partially_grouped_live_children =
                lappend(partially_grouped_live_children, child_partially_grouped_rel as *mut c_void);
        } else {
            partial_grouping_valid = false;
        }

        if patype == PARTITIONWISE_AGGREGATE_FULL {
            set_cheapest(child_grouped_rel);
            grouped_live_children = lappend(grouped_live_children, child_grouped_rel as *mut c_void);
        }

        pfree(appinfos as *mut c_void);
    }

    /*
     * Try to create append paths for partially grouped children.
     */
    if !partially_grouped_rel.is_null() && partial_grouping_valid {
        Assert!(partially_grouped_live_children != NIL);

        add_paths_to_append_rel(root, partially_grouped_rel, partially_grouped_live_children);

        /*
         * We need call set_cheapest.
         */
        if !(*partially_grouped_rel).pathlist.is_null() {
            set_cheapest(partially_grouped_rel);
        }
    }

    /* If possible, create append paths for fully grouped children. */
    if patype == PARTITIONWISE_AGGREGATE_FULL {
        Assert!(grouped_live_children != NIL);

        add_paths_to_append_rel(root, grouped_rel, grouped_live_children);
    }
}

/*
 * group_by_has_partkey
 */
unsafe fn group_by_has_partkey(
    input_rel: *mut RelOptInfo,
    targetList: *mut List,
    groupClause: *mut List,
) -> bool {
    let groupexprs: *mut List = get_sortgrouplist_exprs(groupClause, targetList);
    let mut cnt: c_int;
    let partnatts: c_int;

    /* Input relation should be partitioned. */
    Assert!(!(*input_rel).part_scheme.is_null());

    /* Rule out early, if there are no partition keys present. */
    if (*input_rel).partexprs.is_null() {
        return false;
    }

    partnatts = (*(*input_rel).part_scheme).partnatts as c_int;

    cnt = 0;
    while cnt < partnatts {
        let partexprs: *mut List = *(*input_rel).partexprs.add(cnt as usize);
        let mut lc: *mut ListCell;
        let mut found: bool = false;

        foreach!(lc, partexprs, {
            let mut lg: *mut ListCell;
            let partexpr: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
            let partcoll: Oid = *(*(*input_rel).part_scheme).partcollation.add(cnt as usize);

            foreach!(lg, groupexprs, {
                let mut groupexpr: *mut Expr = lfirst(current_cell!(lg)) as *mut Expr;
                let groupcoll: Oid = exprCollation(groupexpr as *mut Node);

                /*
                 * Note: we can assume there is at most one RelabelType node.
                 */
                if IsA!(groupexpr, T_RelabelType) {
                    groupexpr = (*(groupexpr as *mut RelabelType)).arg;
                }

                if equal(groupexpr as *mut c_void, partexpr as *mut c_void) {
                    /*
                     * Reject a match if the grouping collation does not match.
                     */
                    if OidIsValid(partcoll) && OidIsValid(groupcoll) && partcoll != groupcoll {
                        return false;
                    }

                    found = true;
                    break;
                }
            });

            if found {
                break;
            }
        });

        /*
         * If none of the partition key expressions match, return false.
         */
        if !found {
            return false;
        }
        cnt += 1;
    }

    true
}

/*
 * generate_setop_child_grouplist
 */
unsafe fn generate_setop_child_grouplist(op: *mut SetOperationStmt, targetlist: *mut List) -> *mut List {
    let grouplist: *mut List = copyObject_list((*op).groupClauses);
    let mut lg: *mut ListCell;
    let mut lt: *mut ListCell;
    let mut ct: *mut ListCell;

    lg = list_head(grouplist);
    ct = list_head((*op).colTypes);
    foreach!(lt, targetlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lt)) as *mut TargetEntry;
        let sgc: *mut SortGroupClause;
        let coltype: Oid;

        /* resjunk columns could have sortgrouprefs.  Leave these alone */
        if (*tle).resjunk {
            continue;
        }

        /*
         * We expect every non-resjunk target to have a SortGroupClause and colTypes.
         */
        Assert!(!lg.is_null());
        Assert!(!ct.is_null());
        sgc = lfirst(lg) as *mut SortGroupClause;
        coltype = lfirst_oid(ct);

        /* reject if target type isn't the same as the setop target type */
        if coltype != exprType((*tle).expr as *mut Node) {
            return NIL;
        }

        lg = lnext(grouplist, lg);
        ct = lnext((*op).colTypes, ct);

        /* assign a tleSortGroupRef, or reuse the existing one */
        (*sgc).tleSortGroupRef = assignSortGroupRef(tle, targetlist);
    });

    Assert!(lg.is_null());
    Assert!(ct.is_null());

    grouplist
}

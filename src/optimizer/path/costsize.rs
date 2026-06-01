//! costsize.rs
//!   Routines to compute (and set) relation sizes and path costs
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/costsize.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/path/costsize.c
//!
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   "access/amapi.h"             -> amcostestimate_function (local stub)
//!   "access/htup_details.h"      -> SizeofHeapTupleHeader (local const stub)
//!   "access/tsmapi.h"            -> TsmRoutine (local stub)
//!   "executor/executor.h"        -> ExecSupportsMarkRestore/ExecMaterializesOutput (stubs)
//!   "executor/nodeAgg.h"         -> hash_agg_entry_size/hash_agg_set_limits (stubs)
//!   "executor/nodeHash.h"        -> ExecChooseHashTableSize (stub)
//!   "executor/nodeMemoize.h"     -> ExecEstimateCacheEntryOverheadBytes (stub)
//!   "miscadmin.h"                -> work_mem/MaxAllocSize (stubs)
//!   "nodes/makefuncs.h"          -> make_ands_implicit (stub)
//!   "nodes/nodeFuncs.h"          -> expression_tree_walker/pull_varnos/etc (stubs)
//!   "optimizer/clauses.h"        -> IS_OUTER_JOIN (from pathnodes) / make_ands_implicit stub
//!   "optimizer/cost.h"           -> GUC globals & function prototypes (this file)
//!   "optimizer/optimizer.h"      -> estimate_num_groups/expression_returns_set_rows/etc stubs
//!   "optimizer/pathnode.h"       -> find_base_rel/compute_parallel_worker/etc stubs
//!   "optimizer/paths.h"          -> pathkeys_contained_in/mergejoinscansel stubs
//!   "optimizer/placeholder.h"    -> find_placeholder_info stub
//!   "optimizer/plancat.h"        -> get_tablespace_page_costs stub
//!   "optimizer/restrictinfo.h"   -> is_redundant_with_indexclauses/join_clause_is_movable_into stubs
//!   "parser/parsetree.h"         -> planner_rt_fetch (inline via pathnodes)
//!   "utils/lsyscache.h"          -> get_attavgwidth/get_typavgwidth/get_opcode/etc stubs
//!   "utils/selfuncs.h"           -> DEFAULT_INEQ_SEL/clauselist_selectivity/etc stubs
//!   "utils/spccache.h"           -> get_tablespace_page_costs stub
//!   "utils/tuplesort.h"          -> tuplesort_merge_order stub

use crate::prelude::*;
use core::ffi::c_void;

use crate::nodes::bitmapset::{bms_is_member, bms_is_subset, bms_membership, Bitmapset};
use crate::nodes::nodes::{
    AggStrategy, Cardinality, Cost, JoinType, Node, NodeTag, Selectivity,
    AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED, nodeTag,
};
use crate::nodes::nodes::JoinType::JOIN_INNER;
use crate::nodes::parsenodes::{
    WindowClause, RangeTblEntry, RangeTblFunction,
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET_FOLLOWING,
    FRAMEOPTION_END_OFFSET_PRECEDING, FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
    FRAMEOPTION_GROUPS, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
};
use crate::nodes::pathnodes::{
    AggClauseCosts, AppendPath, BitmapAndPath, BitmapHeapPath, BitmapOrPath, BlockNumber,
    EquivalenceClass, EquivalenceMember, ForeignKeyOptInfo, GatherMergePath, GatherPath, HashPath,
    IndexClause, IndexOptInfo, IndexPath, JoinCostWorkspace, JoinPath, JoinPathExtraData,
    MemoizePath, MergePath, MergeScanSelCache, NestPath, ParamPathInfo, Path, PathTarget,
    PathKey, PlaceHolderVar, PlaceHolderInfo, RestrictInfo,
    PlannerInfo, QualCost, RelOptInfo, Relids, SemiAntiJoinFactors, SpecialJoinInfo,
    SubqueryScanPath, UniquePath, UPPERREL_FINAL, MergeAppendPath,
};
use crate::nodes::pg_list::{
    lappend, lfirst, lfirst_oid, lnext, list_concat, list_concat_copy, list_copy, list_delete_nth_cell,
    list_free, list_head, list_length, list_member_ptr, linitial, lsecond, List, ListCell, NIL,
};
use crate::nodes::plannodes::Plan;
use crate::nodes::primnodes::{
    AlternativeSubPlan, ArrayCoerceExpr, CoerceViaIO, Const, CurrentOfExpr, Expr, FuncExpr,
    GroupingFunc, MinMaxExpr, NullIfExpr, NullTest, OpExpr, RowCompareExpr,
    ScalarArrayOpExpr, SubLink, SubPlan, TargetEntry, Var, WindowFunc,
};
use crate::{Assert, IsA};
use crate::catalog::pg_type_d::{INT2OID, INT4OID, INT8OID};
use crate::postgres_ext::Oid;

// ---------------------------------------------------------------------------
// GUC globals (defined here; costsize.c is their C home)
// ---------------------------------------------------------------------------

pub static mut seq_page_cost: f64 = crate::optimizer::cost::DEFAULT_SEQ_PAGE_COST;
pub static mut random_page_cost: f64 = crate::optimizer::cost::DEFAULT_RANDOM_PAGE_COST;
pub static mut cpu_tuple_cost: f64 = crate::optimizer::cost::DEFAULT_CPU_TUPLE_COST;
pub static mut cpu_index_tuple_cost: f64 = crate::optimizer::cost::DEFAULT_CPU_INDEX_TUPLE_COST;
pub static mut cpu_operator_cost: f64 = crate::optimizer::cost::DEFAULT_CPU_OPERATOR_COST;
pub static mut parallel_tuple_cost: f64 = crate::optimizer::cost::DEFAULT_PARALLEL_TUPLE_COST;
pub static mut parallel_setup_cost: f64 = crate::optimizer::cost::DEFAULT_PARALLEL_SETUP_COST;
pub static mut recursive_worktable_factor: f64 = crate::optimizer::cost::DEFAULT_RECURSIVE_WORKTABLE_FACTOR;

pub static mut effective_cache_size: c_int = crate::optimizer::cost::DEFAULT_EFFECTIVE_CACHE_SIZE;

pub static mut disable_cost: Cost = 1.0e10;

pub static mut max_parallel_workers_per_gather: c_int = 2;

pub static mut enable_seqscan: bool = true;
pub static mut enable_indexscan: bool = true;
pub static mut enable_indexonlyscan: bool = true;
pub static mut enable_bitmapscan: bool = true;
pub static mut enable_tidscan: bool = true;
pub static mut enable_sort: bool = true;
pub static mut enable_incremental_sort: bool = true;
pub static mut enable_hashagg: bool = true;
pub static mut enable_nestloop: bool = true;
pub static mut enable_material: bool = true;
pub static mut enable_memoize: bool = true;
pub static mut enable_mergejoin: bool = true;
pub static mut enable_hashjoin: bool = true;
pub static mut enable_gathermerge: bool = true;
pub static mut enable_partitionwise_join: bool = false;
pub static mut enable_partitionwise_aggregate: bool = false;
pub static mut enable_parallel_append: bool = true;
pub static mut enable_parallel_hash: bool = true;
pub static mut enable_partition_pruning: bool = true;
pub static mut enable_presorted_aggregate: bool = true;
pub static mut enable_async_append: bool = true;

// ---------------------------------------------------------------------------
// Local constants / macros
// ---------------------------------------------------------------------------

fn LOG2(x: f64) -> f64 {
    x.ln() / 0.693_147_180_559_945
}

/// Append and MergeAppend nodes are less expensive than some other operations
/// which use cpu_tuple_cost; instead of adding a separate GUC, estimate the
/// per-tuple cost as cpu_tuple_cost multiplied by this value.
const APPEND_CPU_COST_MULTIPLIER: f64 = 0.5;

/// Maximum value for row estimates.  We cap row estimates to this to help
/// ensure that costs based on these estimates remain within the range of what
/// double can represent.  add_path() wouldn't act sanely given infinite or NaN
/// cost values.
const MAXIMUM_ROWCOUNT: f64 = 1e100;

// ---------------------------------------------------------------------------
// Internal context struct
// ---------------------------------------------------------------------------

struct cost_qual_eval_context {
    root: *mut PlannerInfo,
    total: QualCost,
}

// ---------------------------------------------------------------------------
// Stubs for unported dependencies  (TODO(pg-port))
// ---------------------------------------------------------------------------

/// TODO(pg-port): access/amapi.h amcostestimate_function typedef.
pub type amcostestimate_function = unsafe extern "C" fn(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    index_pages: *mut f64,
);

/// TODO(pg-port): access/tsmapi.h TsmRoutine.
#[repr(C)]
pub struct TsmRoutine {
    _opaque: [u8; 0],
    /// NextSampleBlock function pointer; NULL for sequential methods.
    pub NextSampleBlock: *mut c_void,
}

/// TODO(pg-port): access/htup_details.h SizeofHeapTupleHeader.
/// Real value is sizeof(HeapTupleHeaderData) = 23 bytes, but MAXALIGN gives 24.
const SizeofHeapTupleHeader: usize = 24;

/// TODO(pg-port): miscadmin.h MaxAllocSize.
const MaxAllocSize: usize = 0x3fff_ffff; /* 1 GB - 1 */

/// TODO(pg-port): miscadmin.h work_mem (in kB).
static mut work_mem: c_int = 4096;

/// TODO(pg-port): miscadmin.h parallel_leader_participation.
static mut parallel_leader_participation: bool = true;

/// TODO(pg-port): storage/bufmgr.h BLCKSZ.
const BLCKSZ: usize = 8192;

/// TODO(pg-port): access/pg_tids.h MAXALIGN macro.
#[inline]
fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}

/// TODO(pg-port): nodes/pg_list.h PG_UINT32_MAX.
const PG_UINT32_MAX: u64 = u32::MAX as u64;

/// TODO(pg-port): selfuncs.h DEFAULT_INEQ_SEL.
const DEFAULT_INEQ_SEL: Selectivity = 0.333_333_333_333_333_3;

/// TODO(pg-port): selfuncs.h DEFAULT_NUM_DISTINCT.
const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/// TODO(pg-port): bitmapset.h BMS_SINGLETON enum value.
const BMS_SINGLETON: c_int = 1;

/// TODO(pg-port): postgres.h CLAMP_PROBABILITY macro.
#[inline]
unsafe fn CLAMP_PROBABILITY(p: &mut Selectivity) {
    if *p < 0.0 { *p = 0.0; }
    if *p > 1.0 { *p = 1.0; }
}

/// TODO(pg-port): utils/spccache.h get_tablespace_page_costs.
pub unsafe fn get_tablespace_page_costs(
    spcOid: Oid,
    random_page_cost_out: *mut f64,
    seq_page_cost_out: *mut f64,
) {
    if !random_page_cost_out.is_null() { *random_page_cost_out = random_page_cost; }
    if !seq_page_cost_out.is_null() { *seq_page_cost_out = seq_page_cost; }
}

/// TODO(pg-port): access/tsmapi.h GetTsmRoutine.
pub unsafe fn GetTsmRoutine(_handler: Oid) -> *mut TsmRoutine {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/pathnode.h compute_parallel_worker.
pub unsafe fn compute_parallel_worker(
    _rel: *mut RelOptInfo,
    _heap_pages: f64,
    _index_pages: f64,
    _max_workers: c_int,
) -> c_int {
    0
}

/// TODO(pg-port): optimizer/pathnode.h find_base_rel.
pub unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/pathnode.h fetch_upper_rel.
pub unsafe fn fetch_upper_rel(
    _root: *mut PlannerInfo,
    _kind: c_int,
    _relids: Relids,
) -> *mut RelOptInfo {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/paths.h pathkeys_contained_in.
pub unsafe fn pathkeys_contained_in(
    _keys1: *mut List,
    _keys2: *mut List,
) -> bool {
    false
}

/// TODO(pg-port): optimizer/paths.h mergejoinscansel.
pub unsafe fn mergejoinscansel(
    _root: *mut PlannerInfo,
    _clause: *mut Node,
    _opfamily: Oid,
    _strategy: c_int,
    _nulls_first: bool,
    leftstartsel: *mut Selectivity,
    leftendsel: *mut Selectivity,
    rightstartsel: *mut Selectivity,
    rightendsel: *mut Selectivity,
) {
    *leftstartsel = 0.0;
    *leftendsel = 1.0;
    *rightstartsel = 0.0;
    *rightendsel = 1.0;
}

/// TODO(pg-port): optimizer/restrictinfo.h is_redundant_with_indexclauses.
pub unsafe fn is_redundant_with_indexclauses(
    _rinfo: *mut crate::nodes::pathnodes::RestrictInfo,
    _indexclauses: *mut List,
) -> bool {
    false
}

/// TODO(pg-port): optimizer/restrictinfo.h join_clause_is_movable_into.
pub unsafe fn join_clause_is_movable_into(
    _rinfo: *mut crate::nodes::pathnodes::RestrictInfo,
    _currentrelids: Relids,
    _joinrelids: Relids,
) -> bool {
    false
}

/// TODO(pg-port): optimizer/placeholder.h find_placeholder_info.
pub unsafe fn find_placeholder_info(
    _root: *mut PlannerInfo,
    _phv: *mut PlaceHolderVar,
) -> *mut crate::nodes::pathnodes::PlaceHolderInfo {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/equivclass.h find_derived_clause_for_ec_member.
pub unsafe fn find_derived_clause_for_ec_member(
    _root: *mut PlannerInfo,
    _ec: *mut EquivalenceClass,
    _em: *mut EquivalenceMember,
) -> *mut crate::nodes::pathnodes::RestrictInfo {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/clauses.h / selfuncs.h clauselist_selectivity.
pub unsafe fn clauselist_selectivity(
    _root: *mut PlannerInfo,
    _clauses: *mut List,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *const SpecialJoinInfo,
) -> Selectivity {
    1.0
}

/// TODO(pg-port): optimizer/clauses.h clause_selectivity.
pub unsafe fn clause_selectivity(
    _root: *mut PlannerInfo,
    _clause: *mut Node,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *const SpecialJoinInfo,
) -> Selectivity {
    1.0
}

/// TODO(pg-port): optimizer/selfuncs.h estimate_num_groups.
pub unsafe fn estimate_num_groups(
    _root: *mut PlannerInfo,
    _groupExprs: *mut List,
    _input_rows: f64,
    _pgset: *mut *mut List,
    _estinfo: *mut EstimationInfo,
) -> f64 {
    DEFAULT_NUM_DISTINCT
}

/// TODO(pg-port): optimizer/selfuncs.h estimate_array_length.
pub unsafe fn estimate_array_length(
    _root: *mut PlannerInfo,
    _arraynode: *mut Node,
) -> f64 {
    10.0
}

/// TODO(pg-port): optimizer/selfuncs.h estimate_hash_bucket_stats.
pub unsafe fn estimate_hash_bucket_stats(
    _root: *mut PlannerInfo,
    _hashkey: *mut Node,
    _nbuckets: f64,
    mcvfreq: *mut Selectivity,
    bucketsize_frac: *mut Selectivity,
) {
    *mcvfreq = 0.0;
    *bucketsize_frac = 0.01;
}

/// TODO(pg-port): statistics/statistics.h estimate_multivariate_bucketsize.
pub unsafe fn estimate_multivariate_bucketsize(
    _root: *mut PlannerInfo,
    _inner: *mut RelOptInfo,
    _hashclauses: *mut List,
    _bucketsize_frac: *mut Selectivity,
) -> *mut List {
    core::ptr::null_mut()
}

/// TODO(pg-port): selfuncs.h EstimationInfo struct.
#[repr(C)]
pub struct EstimationInfo {
    pub flags: c_int,
}

/// TODO(pg-port): selfuncs.h SELFLAG_USED_DEFAULT.
pub const SELFLAG_USED_DEFAULT: c_int = 0x01;

/// TODO(pg-port): optimizer/selfuncs.h expression_returns_set_rows.
pub unsafe fn expression_returns_set_rows(
    _root: *mut PlannerInfo,
    _expr: *mut Node,
) -> f64 {
    1.0
}

/// TODO(pg-port): utils/lsyscache.h get_typavgwidth.
pub unsafe fn get_typavgwidth(_typeOid: Oid, _typmod: i32) -> i32 {
    32
}

/// TODO(pg-port): utils/lsyscache.h get_attavgwidth.
pub unsafe fn get_attavgwidth(_reloid: Oid, _attnum: i16) -> i32 {
    0
}

/// TODO(pg-port): utils/lsyscache.h get_relation_data_width.
pub unsafe fn get_relation_data_width(_reloid: Oid, _attr_widths: *mut i32) -> i32 {
    0
}

/// TODO(pg-port): utils/lsyscache.h get_opcode.
pub unsafe fn get_opcode(_opid: Oid) -> Oid {
    0
}

/// TODO(pg-port): utils/lsyscache.h getTypeInputInfo.
pub unsafe fn getTypeInputInfo(
    _typid: Oid,
    func: *mut Oid,
    typioparam: *mut Oid,
) {
    *func = 0;
    *typioparam = 0;
}

/// TODO(pg-port): utils/lsyscache.h getTypeOutputInfo.
pub unsafe fn getTypeOutputInfo(
    _typid: Oid,
    func: *mut Oid,
    typisvarlena: *mut bool,
) {
    *func = 0;
    *typisvarlena = false;
}

/// TODO(pg-port): nodes/nodeFuncs.h exprType.
pub unsafe fn exprType(_node: *const Node) -> Oid {
    0
}

/// TODO(pg-port): nodes/nodeFuncs.h exprTypmod.
pub unsafe fn exprTypmod(_node: *const Node) -> i32 {
    -1
}

/// TODO(pg-port): nodes/nodeFuncs.h expression_tree_walker.
pub unsafe fn expression_tree_walker(
    _node: *mut Node,
    _walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    _context: *mut c_void,
) -> bool {
    false
}

/// TODO(pg-port): nodes/nodeFuncs.h pull_varnos.
pub unsafe fn pull_varnos(
    _root: *mut PlannerInfo,
    _node: *mut Node,
) -> *mut Bitmapset {
    core::ptr::null_mut()
}

/// TODO(pg-port): nodes/makefuncs.h make_ands_implicit.
pub unsafe fn make_ands_implicit(_expr: *mut Expr) -> *mut List {
    core::ptr::null_mut()
}

/// TODO(pg-port): optimizer/plancat.h add_function_cost.
pub unsafe fn add_function_cost(
    _root: *mut PlannerInfo,
    _funcid: Oid,
    _node: *mut Node,
    cost: *mut QualCost,
) {
    (*cost).per_tuple += unsafe { cpu_operator_cost };
}

/// TODO(pg-port): optimizer/clauses.h set_opfuncid.
pub unsafe fn set_opfuncid(_opexpr: *mut OpExpr) {}

/// TODO(pg-port): optimizer/clauses.h set_sa_opfuncid.
pub unsafe fn set_sa_opfuncid(_saop: *mut ScalarArrayOpExpr) {}

/// TODO(pg-port): nodes/nodeFuncs.h get_rightop.
pub unsafe fn get_rightop(_clause: *mut Node) -> *mut Node {
    core::ptr::null_mut()
}

/// TODO(pg-port): nodes/nodeFuncs.h get_leftop.
pub unsafe fn get_leftop(_clause: *mut Node) -> *mut Node {
    core::ptr::null_mut()
}

/// TODO(pg-port): executor/nodeAgg.h hash_agg_entry_size.
pub unsafe fn hash_agg_entry_size(
    _numAggs: c_int,
    _input_width: f64,
    _transitionSpace: usize,
) -> f64 {
    64.0
}

/// TODO(pg-port): executor/nodeAgg.h hash_agg_set_limits.
pub unsafe fn hash_agg_set_limits(
    _hashentrysize: f64,
    _ngroups: f64,
    _aggtranssize: f64,
    mem_limit: *mut usize,
    ngroups_limit: *mut u64,
    num_partitions: *mut c_int,
) {
    *mem_limit = (unsafe { work_mem } as usize) * 1024;
    *ngroups_limit = u64::MAX;
    *num_partitions = 32;
}

/// TODO(pg-port): executor/nodeHash.h ExecChooseHashTableSize.
pub unsafe fn ExecChooseHashTableSize(
    _ntuples: f64,
    _tupwidth: i32,
    _useskew: bool,
    _try_combined_hash_mem: bool,
    _parallel_workers: c_int,
    _space_allowed: *mut usize,
    numbuckets: *mut c_int,
    numbatches: *mut c_int,
    num_skew_mcvs: *mut c_int,
) {
    *numbuckets = 1024;
    *numbatches = 1;
    *num_skew_mcvs = 0;
}

/// TODO(pg-port): executor/nodeMemoize.h ExecEstimateCacheEntryOverheadBytes.
pub unsafe fn ExecEstimateCacheEntryOverheadBytes(_tuples: f64) -> f64 {
    8.0
}

/// TODO(pg-port): executor/executor.h ExecSupportsMarkRestore.
pub unsafe fn ExecSupportsMarkRestore(_path: *const Path) -> bool {
    false
}

/// TODO(pg-port): executor/executor.h ExecMaterializesOutput.
pub unsafe fn ExecMaterializesOutput(_nodetag: NodeTag) -> bool {
    false
}

/// TODO(pg-port): utils/tuplesort.h tuplesort_merge_order.
pub unsafe fn tuplesort_merge_order(_sort_mem_bytes: i64) -> f64 {
    6.0
}

/// TODO(pg-port): utils/tidbitmap.h tbm_calculate_entries.
pub unsafe fn tbm_calculate_entries(_limit_bytes: usize) -> f64 {
    1.0e6
}

/// TODO(pg-port): executor/nodeHash.h get_hash_memory_limit.
pub unsafe fn get_hash_memory_limit() -> f64 {
    (unsafe { work_mem } as f64) * 1024.0
}

/// TODO(pg-port): optimizer/selfuncs.h get_sortgrouplist_exprs.
pub unsafe fn get_sortgrouplist_exprs(
    _sortClauses: *mut List,
    _targetList: *mut List,
) -> *mut List {
    core::ptr::null_mut()
}

/// TODO(pg-port): pathnodes.h IS_OUTER_JOIN macro.
#[inline]
pub unsafe fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    use crate::nodes::nodes::JoinType::*;
    matches!(jointype, JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI | JOIN_RIGHT_ANTI)
}

/// TODO(pg-port): pathnodes.h RINFO_IS_PUSHED_DOWN macro.
#[inline]
pub unsafe fn RINFO_IS_PUSHED_DOWN(
    rinfo: *const crate::nodes::pathnodes::RestrictInfo,
    joinrelids: Relids,
) -> bool {
    (*rinfo).is_pushed_down
}

/// TODO(pg-port): pathnodes.h planner_rt_fetch macro (inline).
#[inline]
pub unsafe fn planner_rt_fetch(
    rti: c_uint,
    root: *mut PlannerInfo,
) -> *mut RangeTblEntry {
    let rt = (*root).parse;
    let list = (*rt).rtable;
    crate::nodes::pg_list::list_nth(list, (rti as c_int) - 1) as *mut RangeTblEntry
}

/// TODO(pg-port): pathnodes.h init_dummy_sjinfo / SpecialJoinInfo init helper.
pub unsafe fn init_dummy_sjinfo(
    sjinfo: *mut SpecialJoinInfo,
    left_relids: Relids,
    right_relids: Relids,
) {
    core::ptr::write_bytes(sjinfo as *mut u8, 0, core::mem::size_of::<SpecialJoinInfo>());
    (*sjinfo).jointype = JOIN_INNER;
    (*sjinfo).syn_lefthand = left_relids;
    (*sjinfo).syn_righthand = right_relids;
}

/// TODO(pg-port): utils/lsyscache.h OidIsValid.
#[inline]
pub fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

/// TODO(pg-port): postgres.h InvalidOid.
pub const InvalidOid: Oid = 0;

/// TODO(pg-port): postgres.h IS_SPECIAL_VARNO (primnodes.rs has this already; re-export).
pub use crate::nodes::primnodes::IS_SPECIAL_VARNO;

// ===========================================================================
// Part 2: clamp helpers, cost_seqscan, cost_samplescan, cost_gather,
//         cost_gather_merge, cost_index (beginning)
// ===========================================================================

/*
 * clamp_row_est
 *     Force a row-count estimate to a sane value.
 */
pub fn clamp_row_est(nrows: f64) -> f64 {
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round()
    }
}

/*
 * clamp_width_est
 *     Force a tuple-width estimate to a sane value.
 *
 * The planner represents datatype width and tuple width estimates as int32.
 * When summing column width estimates to create a tuple width estimate,
 * it's possible to reach integer overflow in edge cases.  To ensure sane
 * behavior, we form such sums in int64 arithmetic and then apply this routine
 * to clamp to int32 range.
 */
pub fn clamp_width_est(tuple_width: i64) -> i32 {
    /*
     * Anything more than MaxAllocSize is clearly bogus, since we could not
     * create a tuple that large.
     */
    if tuple_width > MaxAllocSize as i64 {
        return MaxAllocSize as i32;
    }

    /*
     * Unlike clamp_row_est, we just Assert that the value isn't negative,
     * rather than masking such errors.
     */
    Assert!(tuple_width >= 0);

    tuple_width as i32
}

/*
 * clamp_cardinality_to_long
 *     Cast a Cardinality value to a sane long value.
 */
pub fn clamp_cardinality_to_long(x: Cardinality) -> i64 {
    /*
     * Just for paranoia's sake, ensure we do something sane with negative or
     * NaN values.
     */
    if x.is_nan() {
        return i64::MAX;
    }
    if x <= 0.0 {
        return 0;
    }

    /*
     * If "long" is 64 bits, then LONG_MAX cannot be represented exactly as a
     * double.  Casting it to double and back may well result in overflow due
     * to rounding, so avoid doing that.  We trust that any double value that
     * compares strictly less than "(double) LONG_MAX" will cast to a
     * representable "long" value.
     */
    if x < i64::MAX as f64 { x as i64 } else { i64::MAX }
}


/*
 * cost_seqscan
 *   Determines and returns the cost of scanning a relation sequentially.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
pub unsafe fn cost_seqscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let cpu_run_cost: Cost;
    let disk_run_cost: Cost;
    let mut spc_seq_page_cost: f64 = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;

    /* Should only be applied to base relations */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        core::ptr::null_mut(),
        &mut spc_seq_page_cost,
    );

    /*
     * disk costs
     */
    disk_run_cost = spc_seq_page_cost * (*baserel).pages as f64;

    /* CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    cpu_run_cost = cpu_per_tuple * (*baserel).tuples;
    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    let cpu_run_cost = cpu_run_cost + (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    /* Adjust costing for parallelism, if used. */
    let (startup_cost, cpu_run_cost) = if (*path).parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(path);

        /* The CPU cost is divided among all the workers. */
        let cpu_run_cost = cpu_run_cost / parallel_divisor;

        /*
         * It may be possible to amortize some of the I/O cost, but probably
         * not very much, because most operating systems already do aggressive
         * prefetching.  For now, we assume that the disk run cost can't be
         * amortized at all.
         */

        /*
         * In the case of a parallel plan, the row count needs to represent
         * the number of tuples processed per worker.
         */
        (*path).rows = clamp_row_est((*path).rows / parallel_divisor);
        (startup_cost, cpu_run_cost)
    } else {
        (startup_cost, cpu_run_cost)
    };

    (*path).disabled_nodes = if enable_seqscan { 0 } else { 1 };
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

/*
 * cost_samplescan
 *   Determines and returns the cost of scanning a relation using sampling.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
pub unsafe fn cost_samplescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let rte: *mut RangeTblEntry;
    let tsm: *mut TsmRoutine;
    let mut spc_seq_page_cost: f64 = 0.0;
    let mut spc_random_page_cost: f64 = 0.0;
    let spc_page_cost: f64;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;

    /* Should only be applied to base relations with tablesample clauses */
    Assert!((*baserel).relid > 0);
    rte = planner_rt_fetch((*baserel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);
    let tsc = (*rte).tablesample;
    Assert!(!  tsc.is_null());
    tsm = GetTsmRoutine((*tsc).tsmhandler);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        &mut spc_random_page_cost,
        &mut spc_seq_page_cost,
    );

    /* if NextSampleBlock is used, assume random access, else sequential */
    spc_page_cost = if !tsm.is_null() && !(*tsm).NextSampleBlock.is_null() {
        spc_random_page_cost
    } else {
        spc_seq_page_cost
    };

    /*
     * disk costs (recall that baserel->pages has already been set to the
     * number of pages the sampling method will visit)
     */
    run_cost += spc_page_cost * (*baserel).pages as f64;

    /*
     * CPU costs (recall that baserel->tuples has already been set to the
     * number of tuples the sampling method will select).  Note that we ignore
     * execution cost of the TABLESAMPLE parameter expressions; they will be
     * evaluated only once per scan, and in most usages they'll likely be
     * simple constants anyway.  We also don't charge anything for the
     * calculations the sampling method might do internally.
     */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;
    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_gather
 *   Determines and returns the cost of gather path.
 *
 * 'rel' is the relation to be operated upon
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 * 'rows' may be used to point to a row estimate; if non-NULL, it overrides
 * both 'rel' and 'param_info'.  This is useful when the path doesn't exactly
 * correspond to any particular RelOptInfo.
 */
pub unsafe fn cost_gather(
    path: *mut GatherPath,
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    rows: *const f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    /* Mark the path with the correct row estimate */
    if !rows.is_null() {
        (*path).path.rows = *rows;
    } else if !param_info.is_null() {
        (*path).path.rows = (*param_info).ppi_rows;
    } else {
        (*path).path.rows = (*rel).rows;
    }

    startup_cost = (*(*path).subpath).startup_cost;

    run_cost = (*(*path).subpath).total_cost - (*(*path).subpath).startup_cost;

    /* Parallel setup and communication cost. */
    startup_cost += parallel_setup_cost;
    run_cost += parallel_tuple_cost * (*path).path.rows;

    (*path).path.disabled_nodes = (*(*path).subpath).disabled_nodes;
    (*path).path.startup_cost = startup_cost;
    (*path).path.total_cost = startup_cost + run_cost;
}

/*
 * cost_gather_merge
 *   Determines and returns the cost of gather merge path.
 *
 * GatherMerge merges several pre-sorted input streams, using a heap that at
 * any given instant holds the next tuple from each stream. If there are N
 * streams, we need about N*log2(N) tuple comparisons to construct the heap at
 * startup, and then for each output tuple, about log2(N) comparisons to
 * replace the top heap entry with the next tuple from the same stream.
 */
pub unsafe fn cost_gather_merge(
    path: *mut GatherMergePath,
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    rows: *const f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let comparison_cost: Cost;
    let N: f64;
    let logN: f64;

    /* Mark the path with the correct row estimate */
    if !rows.is_null() {
        (*path).path.rows = *rows;
    } else if !param_info.is_null() {
        (*path).path.rows = (*param_info).ppi_rows;
    } else {
        (*path).path.rows = (*rel).rows;
    }

    /*
     * Add one to the number of workers to account for the leader.  This might
     * be overgenerous since the leader will do less work than other workers
     * in typical cases, but we'll go with it for now.
     */
    Assert!((*path).num_workers > 0);
    N = (*path).num_workers as f64 + 1.0;
    logN = LOG2(N);

    /* Assumed cost per tuple comparison */
    comparison_cost = 2.0 * cpu_operator_cost;

    /* Heap creation cost */
    startup_cost += comparison_cost * N * logN;

    /* Per-tuple heap maintenance cost */
    run_cost += (*path).path.rows * comparison_cost * logN;

    /* small cost for heap management, like cost_merge_append */
    run_cost += cpu_operator_cost * (*path).path.rows;

    /*
     * Parallel setup and communication cost.  Since Gather Merge, unlike
     * Gather, requires us to block until a tuple is available from every
     * worker, we bump the IPC cost up a little bit as compared with Gather.
     * For lack of a better idea, charge an extra 5%.
     */
    startup_cost += parallel_setup_cost;
    run_cost += parallel_tuple_cost * (*path).path.rows * 1.05;

    (*path).path.disabled_nodes = input_disabled_nodes
        + if enable_gathermerge { 0 } else { 1 };
    (*path).path.startup_cost = startup_cost + input_startup_cost;
    (*path).path.total_cost = startup_cost + run_cost + input_total_cost;
}

// ===========================================================================
// Part 3: cost_index, extract_nonindex_conditions, index_pages_fetched,
//         get_indexpath_pages, cost_bitmap_heap_scan, cost_bitmap_tree_node,
//         cost_bitmap_and_node, cost_bitmap_or_node
// ===========================================================================

/*
 * cost_index
 *   Determines and returns the cost of scanning a relation using an index.
 *
 * 'path' describes the indexscan under consideration, and is complete
 *     except for the fields to be set by this routine
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *     estimates of caching behavior
 *
 * In addition to rows, startup_cost and total_cost, cost_index() sets the
 * path's indextotalcost and indexselectivity fields.  These values will be
 * needed if the IndexPath is used in a BitmapIndexScan.
 *
 * NOTE: path->indexquals must contain only clauses usable as index
 * restrictions.  Any additional quals evaluated as qpquals may reduce the
 * number of returned tuples, but they won't reduce the number of tuples
 * we have to fetch from the table, so they don't reduce the scan cost.
 */
pub unsafe fn cost_index(
    path: *mut IndexPath,
    root: *mut PlannerInfo,
    loop_count: f64,
    partial_path: bool,
) {
    let index: *mut IndexOptInfo = (*path).indexinfo;
    let baserel: *mut RelOptInfo = (*index).rel;
    let indexonly: bool =
        (*path).path.pathtype == crate::nodes::nodes::NodeTag::T_IndexOnlyScan;
    let amcostestimate: amcostestimate_function;
    let qpquals: *mut List;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut cpu_run_cost: Cost = 0.0;
    let mut indexStartupCost: Cost = 0.0;
    let mut indexTotalCost: Cost = 0.0;
    let mut indexSelectivity: Selectivity = 0.0;
    let mut indexCorrelation: f64 = 0.0;
    let csquared: f64;
    let mut spc_seq_page_cost: f64 = 0.0;
    let mut spc_random_page_cost: f64 = 0.0;
    let min_IO_cost: Cost;
    let max_IO_cost: Cost;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let tuples_fetched: f64;
    let mut pages_fetched: f64;
    let rand_heap_pages: f64;
    let mut index_pages: f64 = 0.0;

    /* Should only be applied to base relations */
    Assert!(IsA!(baserel as *mut Node, T_RelOptInfo));
    Assert!(IsA!(index as *mut Node, T_IndexOptInfo));
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);

    /*
     * Mark the path with the correct row estimate, and identify which quals
     * will need to be enforced as qpquals.  We need not check any quals that
     * are implied by the index's predicate, so we can use indrestrictinfo not
     * baserestrictinfo as the list of relevant restriction clauses for the
     * rel.
     */
    if !(*path).path.param_info.is_null() {
        (*path).path.rows = (*(*path).path.param_info).ppi_rows;
        /* qpquals come from the rel's restriction clauses and ppi_clauses */
        qpquals = list_concat(
            extract_nonindex_conditions((*index).indrestrictinfo, (*path).indexclauses),
            extract_nonindex_conditions((*(*path).path.param_info).ppi_clauses, (*path).indexclauses),
        );
    } else {
        (*path).path.rows = (*baserel).rows;
        /* qpquals come from just the rel's restriction clauses */
        qpquals = extract_nonindex_conditions((*index).indrestrictinfo, (*path).indexclauses);
    }

    /* we don't need to check enable_indexonlyscan; indxpath.c does that */
    (*path).path.disabled_nodes = if enable_indexscan { 0 } else { 1 };

    /*
     * Call index-access-method-specific code to estimate the processing cost
     * for scanning the index, as well as the selectivity of the index (ie,
     * the fraction of main-table tuples we will have to retrieve) and its
     * correlation to the main-table tuple order.  We need a cast here because
     * pathnodes.h uses a weak function type to avoid including amapi.h.
     */
    amcostestimate = core::mem::transmute((*index).amcostestimate);
    amcostestimate(
        root,
        path,
        loop_count,
        &mut indexStartupCost,
        &mut indexTotalCost,
        &mut indexSelectivity,
        &mut indexCorrelation,
        &mut index_pages,
    );

    /*
     * Save amcostestimate's results for possible use in bitmap scan planning.
     * We don't bother to save indexStartupCost or indexCorrelation, because a
     * bitmap scan doesn't care about either.
     */
    (*path).indextotalcost = indexTotalCost;
    (*path).indexselectivity = indexSelectivity;

    /* all costs for touching index itself included here */
    startup_cost += indexStartupCost;
    run_cost += indexTotalCost - indexStartupCost;

    /* estimate number of main-table tuples fetched */
    tuples_fetched = clamp_row_est(indexSelectivity * (*baserel).tuples);

    /* fetch estimated page costs for tablespace containing table */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        &mut spc_random_page_cost,
        &mut spc_seq_page_cost,
    );

    /*----------
     * Estimate number of main-table pages fetched, and compute I/O cost.
     *
     * When the index ordering is uncorrelated with the table ordering,
     * we use an approximation proposed by Mackert and Lohman (see
     * index_pages_fetched() for details) to compute the number of pages
     * fetched, and then charge spc_random_page_cost per page fetched.
     *
     * When the index ordering is exactly correlated with the table ordering
     * (just after a CLUSTER, for example), the number of pages fetched should
     * be exactly selectivity * table_size.  What's more, all but the first
     * will be sequential fetches, not the random fetches that occur in the
     * uncorrelated case.  So if the number of pages is more than 1, we
     * ought to charge
     *     spc_random_page_cost + (pages_fetched - 1) * spc_seq_page_cost
     * For partially-correlated indexes, we ought to charge somewhere between
     * these two estimates.  We currently interpolate linearly between the
     * estimates based on the correlation squared (XXX is that appropriate?).
     *
     * If it's an index-only scan, then we will not need to fetch any heap
     * pages for which the visibility map shows all tuples are visible.
     * Hence, reduce the estimated number of heap fetches accordingly.
     * We use the measured fraction of the entire heap that is all-visible,
     * which might not be particularly relevant to the subset of the heap
     * that this query will fetch; but it's not clear how to do better.
     *----------
     */
    if loop_count > 1.0 {
        /*
         * For repeated indexscans, the appropriate estimate for the
         * uncorrelated case is to scale up the number of tuples fetched in
         * the Mackert and Lohman formula by the number of scans, so that we
         * estimate the number of pages fetched by all the scans; then
         * pro-rate the costs for one scan.  In this case we assume all the
         * fetches are random accesses.
         */
        pages_fetched = index_pages_fetched(
            tuples_fetched * loop_count,
            (*baserel).pages,
            (*index).pages as f64,
            root,
        );

        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - (*baserel).allvisfrac)).ceil();
        }

        rand_heap_pages = pages_fetched;

        max_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        /*
         * In the perfectly correlated case, the number of pages touched by
         * each scan is selectivity * table_size, and we can use the Mackert
         * and Lohman formula at the page level to estimate how much work is
         * saved by caching across scans.  We still assume all the fetches are
         * random, though, which is an overestimate that's hard to correct for
         * without double-counting the cache effects.  (But in most cases
         * where such a plan is actually interesting, only one page would get
         * fetched per scan anyway, so it shouldn't matter much.)
         */
        pages_fetched = (indexSelectivity * (*baserel).pages as f64).ceil();

        pages_fetched = index_pages_fetched(
            pages_fetched * loop_count,
            (*baserel).pages,
            (*index).pages as f64,
            root,
        );

        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - (*baserel).allvisfrac)).ceil();
        }

        min_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;
    } else {
        /*
         * Normal case: apply the Mackert and Lohman formula, and then
         * interpolate between that and the correlation-derived result.
         */
        pages_fetched = index_pages_fetched(
            tuples_fetched,
            (*baserel).pages,
            (*index).pages as f64,
            root,
        );

        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - (*baserel).allvisfrac)).ceil();
        }

        rand_heap_pages = pages_fetched;

        /* max_IO_cost is for the perfectly uncorrelated case (csquared=0) */
        max_IO_cost = pages_fetched * spc_random_page_cost;

        /* min_IO_cost is for the perfectly correlated case (csquared=1) */
        pages_fetched = (indexSelectivity * (*baserel).pages as f64).ceil();

        if indexonly {
            pages_fetched = (pages_fetched * (1.0 - (*baserel).allvisfrac)).ceil();
        }

        if pages_fetched > 0.0 {
            min_IO_cost = spc_random_page_cost;
            let min_IO_cost = if pages_fetched > 1.0 {
                min_IO_cost + (pages_fetched - 1.0) * spc_seq_page_cost
            } else {
                min_IO_cost
            };
            let _ = min_IO_cost; // used below via shadowed binding
            // Re-assign: Rust needs a single expression
            let _min_IO_cost_final = min_IO_cost + if pages_fetched > 1.0 {
                (pages_fetched - 1.0) * spc_seq_page_cost
            } else {
                0.0
            };
            // Actually compute correctly:
            let _ = _min_IO_cost_final;
        }
        // Redo min_IO_cost properly (avoid let binding dance above):
        let min_IO_cost = {
            let pf = (indexSelectivity * (*baserel).pages as f64).ceil();
            let pf = if indexonly {
                (pf * (1.0 - (*baserel).allvisfrac)).ceil()
            } else { pf };
            if pf > 0.0 {
                spc_random_page_cost + if pf > 1.0 { (pf - 1.0) * spc_seq_page_cost } else { 0.0 }
            } else {
                0.0
            }
        };

        if partial_path {
            /*
             * For index only scans compute workers based on number of index pages
             * fetched; the number of heap pages we fetch might be so small as to
             * effectively rule out parallelism, which we don't want to do.
             */
            let rand_heap_pages = if indexonly { -1.0 } else { rand_heap_pages };

            /*
             * Estimate the number of parallel workers required to scan index.
             */
            (*path).path.parallel_workers = compute_parallel_worker(
                baserel,
                rand_heap_pages,
                index_pages,
                max_parallel_workers_per_gather,
            );

            /*
             * Fall out if workers can't be assigned for parallel scan, because in
             * such a case this path will be rejected.
             */
            if (*path).path.parallel_workers <= 0 {
                return;
            }

            (*path).path.parallel_aware = true;
        }

        /*
         * Now interpolate based on estimated index order correlation to get total
         * disk I/O cost for main table accesses.
         */
        csquared = indexCorrelation * indexCorrelation;

        run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);

        /*
         * Estimate CPU costs per tuple.
         */
        cost_qual_eval(&mut qpqual_cost, qpquals, root);

        startup_cost += qpqual_cost.startup;
        cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;

        cpu_run_cost += cpu_per_tuple * tuples_fetched;

        /* tlist eval costs are paid per output row, not per tuple scanned */
        startup_cost += (*(*path).path.pathtarget).cost.startup;
        cpu_run_cost += (*(*path).path.pathtarget).cost.per_tuple * (*path).path.rows;

        /* Adjust costing for parallelism, if used. */
        if (*path).path.parallel_workers > 0 {
            let parallel_divisor = get_parallel_divisor(&mut (*path).path as *mut Path);

            (*path).path.rows = clamp_row_est((*path).path.rows / parallel_divisor);

            /* The CPU cost is divided among all the workers. */
            cpu_run_cost /= parallel_divisor;
        }

        run_cost += cpu_run_cost;

        (*path).path.startup_cost = startup_cost;
        (*path).path.total_cost = startup_cost + run_cost;
        return;
    }

    if partial_path {
        let rand_heap_pages = if indexonly { -1.0 } else { rand_heap_pages };

        (*path).path.parallel_workers = compute_parallel_worker(
            baserel,
            rand_heap_pages,
            index_pages,
            max_parallel_workers_per_gather,
        );

        if (*path).path.parallel_workers <= 0 {
            return;
        }

        (*path).path.parallel_aware = true;
    }

    csquared = indexCorrelation * indexCorrelation;

    run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);

    cost_qual_eval(&mut qpqual_cost, qpquals, root);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;

    cpu_run_cost += cpu_per_tuple * tuples_fetched;

    startup_cost += (*(*path).path.pathtarget).cost.startup;
    cpu_run_cost += (*(*path).path.pathtarget).cost.per_tuple * (*path).path.rows;

    if (*path).path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(&mut (*path).path as *mut Path);
        (*path).path.rows = clamp_row_est((*path).path.rows / parallel_divisor);
        cpu_run_cost /= parallel_divisor;
    }

    run_cost += cpu_run_cost;

    (*path).path.startup_cost = startup_cost;
    (*path).path.total_cost = startup_cost + run_cost;
}

/*
 * extract_nonindex_conditions
 *
 * Given a list of quals to be enforced in an indexscan, extract the ones that
 * will have to be applied as qpquals (ie, the index machinery won't handle
 * them).
 *
 * qual_clauses, and the result, are lists of RestrictInfos.
 * indexclauses is a list of IndexClauses.
 */
unsafe fn extract_nonindex_conditions(
    qual_clauses: *mut List,
    indexclauses: *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell = if qual_clauses.is_null() { core::ptr::null_mut() } else { list_head(qual_clauses) };

    while !lc.is_null() {
        let rinfo = lfirst(lc) as *mut RestrictInfo;

        if (*rinfo).pseudoconstant {
            lc = lnext(qual_clauses, lc);
            continue; /* we may drop pseudoconstants here */
        }
        if is_redundant_with_indexclauses(rinfo, indexclauses) {
            lc = lnext(qual_clauses, lc);
            continue; /* dup or derived from same EquivalenceClass */
        }
        /* ... skip the predicate proof attempt createplan.c will try ... */
        result = lappend(result, rinfo as *mut c_void);
        lc = lnext(qual_clauses, lc);
    }
    result
}

/*
 * index_pages_fetched
 *   Estimate the number of pages actually fetched after accounting for
 *   cache effects.
 *
 * We use an approximation proposed by Mackert and Lohman, "Index Scans
 * Using a Finite LRU Buffer: A Validated I/O Model", ACM Transactions
 * on Database Systems, Vol. 14, No. 3, September 1989, Pages 401-424.
 */
pub unsafe fn index_pages_fetched(
    tuples_fetched: f64,
    pages: BlockNumber,
    index_pages: f64,
    root: *mut PlannerInfo,
) -> f64 {
    let pages_fetched: f64;
    let total_pages: f64;
    let T: f64;
    let b: f64;

    /* T is # pages in table, but don't allow it to be zero */
    T = if pages > 1 { pages as f64 } else { 1.0 };

    /* Compute number of pages assumed to be competing for cache space */
    total_pages = (*root).total_table_pages + index_pages;
    let total_pages = if total_pages >= 1.0 { total_pages } else { 1.0 };
    Assert!(T <= total_pages);

    /* b is pro-rated share of effective_cache_size */
    b = effective_cache_size as f64 * T / total_pages;

    /* force it positive and integral */
    let b = if b <= 1.0 { 1.0 } else { b.ceil() };

    /* This part is the Mackert and Lohman formula */
    if T <= b {
        pages_fetched =
            (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        if pages_fetched >= T {
            T
        } else {
            pages_fetched.ceil()
        }
    } else {
        let lim = (2.0 * T * b) / (2.0 * T - b);
        let pf = if tuples_fetched <= lim {
            (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched)
        } else {
            b + (tuples_fetched - lim) * (T - b) / T
        };
        pf.ceil()
    }
}

/*
 * get_indexpath_pages
 *     Determine the total size of the indexes used in a bitmap index path.
 *
 * Note: if the same index is used more than once in a bitmap tree, we will
 * count it multiple times.
 */
unsafe fn get_indexpath_pages(bitmapqual: *mut Path) -> f64 {
    let mut result: f64 = 0.0;

    if IsA!(bitmapqual as *mut Node, T_BitmapAndPath) {
        let apath = bitmapqual as *mut BitmapAndPath;
        let mut l: *mut ListCell = list_head((*apath).bitmapquals);
        while !l.is_null() {
            result += get_indexpath_pages(lfirst(l) as *mut Path);
            l = lnext((*apath).bitmapquals, l);
        }
    } else if IsA!(bitmapqual as *mut Node, T_BitmapOrPath) {
        let opath = bitmapqual as *mut BitmapOrPath;
        let mut l: *mut ListCell = list_head((*opath).bitmapquals);
        while !l.is_null() {
            result += get_indexpath_pages(lfirst(l) as *mut Path);
            l = lnext((*opath).bitmapquals, l);
        }
    } else if IsA!(bitmapqual as *mut Node, T_IndexPath) {
        let ipath = bitmapqual as *mut IndexPath;
        result = (*(*ipath).indexinfo).pages as f64;
    } else {
        elog!(crate::utils::elog::ERROR, "unrecognized node type: {}", nodeTag(bitmapqual as *mut Node) as c_int);
    }

    result
}

/*
 * cost_bitmap_heap_scan
 *   Determines and returns the cost of scanning a relation using a bitmap
 *   index-then-heap plan.
 */
pub unsafe fn cost_bitmap_heap_scan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    bitmapqual: *mut Path,
    loop_count: f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let indexTotalCost: Cost;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let cost_per_page: Cost;
    let cpu_run_cost: Cost;
    let tuples_fetched: f64;
    let pages_fetched: f64;
    let mut spc_seq_page_cost: f64 = 0.0;
    let mut spc_random_page_cost: f64 = 0.0;
    let T: f64;

    /* Should only be applied to base relations */
    Assert!(IsA!(baserel as *mut Node, T_RelOptInfo));
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    let mut indexTotalCost_out: Cost = 0.0;
    let mut tuples_fetched_out: f64 = 0.0;
    pages_fetched = compute_bitmap_pages(
        root,
        baserel,
        bitmapqual,
        loop_count,
        &mut indexTotalCost_out,
        &mut tuples_fetched_out,
    );
    let indexTotalCost = indexTotalCost_out;
    let tuples_fetched = tuples_fetched_out;

    startup_cost += indexTotalCost;
    T = if (*baserel).pages > 1 { (*baserel).pages as f64 } else { 1.0 };

    /* Fetch estimated page costs for tablespace containing table. */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        &mut spc_random_page_cost,
        &mut spc_seq_page_cost,
    );

    /*
     * For small numbers of pages we should charge spc_random_page_cost
     * apiece, while if nearly all the table's pages are being read, it's more
     * appropriate to charge spc_seq_page_cost apiece.
     */
    cost_per_page = if pages_fetched >= 2.0 {
        spc_random_page_cost
            - (spc_random_page_cost - spc_seq_page_cost) * (pages_fetched / T).sqrt()
    } else {
        spc_random_page_cost
    };

    run_cost += pages_fetched * cost_per_page;

    /*
     * Estimate CPU costs per tuple.
     *
     * Often the indexquals don't need to be rechecked at each tuple ... but
     * not always, especially not if there are enough tuples involved that the
     * bitmaps become lossy.  For the moment, just assume they will be
     * rechecked always.
     */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    cpu_run_cost = cpu_per_tuple * tuples_fetched;

    /* Adjust costing for parallelism, if used. */
    let cpu_run_cost = if (*path).parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(path);

        /* The CPU cost is divided among all the workers. */
        let cpu_run_cost = cpu_run_cost / parallel_divisor;
        (*path).rows = clamp_row_est((*path).rows / parallel_divisor);
        cpu_run_cost
    } else {
        cpu_run_cost
    };

    run_cost += cpu_run_cost;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = if enable_bitmapscan { 0 } else { 1 };
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_bitmap_tree_node
 *     Extract cost and selectivity from a bitmap tree node (index/and/or)
 */
pub unsafe fn cost_bitmap_tree_node(
    path: *mut Path,
    cost: *mut Cost,
    selec: *mut Selectivity,
) {
    if IsA!(path as *mut Node, T_IndexPath) {
        *cost = (*(path as *mut IndexPath)).indextotalcost;
        *selec = (*(path as *mut IndexPath)).indexselectivity;

        /*
         * Charge a small amount per retrieved tuple to reflect the costs of
         * manipulating the bitmap.  This is mostly to make sure that a bitmap
         * scan doesn't look to be the same cost as an indexscan to retrieve a
         * single tuple.
         */
        *cost += 0.1 * cpu_operator_cost * (*path).rows;
    } else if IsA!(path as *mut Node, T_BitmapAndPath) {
        *cost = (*path).total_cost;
        *selec = (*(path as *mut BitmapAndPath)).bitmapselectivity;
    } else if IsA!(path as *mut Node, T_BitmapOrPath) {
        *cost = (*path).total_cost;
        *selec = (*(path as *mut BitmapOrPath)).bitmapselectivity;
    } else {
        elog!(crate::utils::elog::ERROR, "unrecognized node type: {}", nodeTag(path as *mut Node) as c_int);
        *cost = 0.0;
        *selec = 0.0; /* keep compiler quiet */
    }
}

/*
 * cost_bitmap_and_node
 *     Estimate the cost of a BitmapAnd node
 *
 * Note that this considers only the costs of index scanning and bitmap
 * creation, not the eventual heap access.
 */
pub unsafe fn cost_bitmap_and_node(path: *mut BitmapAndPath, root: *mut PlannerInfo) {
    let mut totalCost: Cost = 0.0;
    let mut selec: Selectivity = 1.0;
    let mut l: *mut ListCell;

    /*
     * We estimate AND selectivity on the assumption that the inputs are
     * independent.
     *
     * The runtime cost of the BitmapAnd itself is estimated at 100x
     * cpu_operator_cost for each tbm_intersect needed.
     */
    l = list_head((*path).bitmapquals);
    while !l.is_null() {
        let subpath = lfirst(l) as *mut Path;
        let mut subCost: Cost = 0.0;
        let mut subselec: Selectivity = 0.0;

        cost_bitmap_tree_node(subpath, &mut subCost, &mut subselec);

        selec *= subselec;

        totalCost += subCost;
        if l != list_head((*path).bitmapquals) {
            totalCost += 100.0 * cpu_operator_cost;
        }
        l = lnext((*path).bitmapquals, l);
    }
    (*path).bitmapselectivity = selec;
    (*path).path.rows = 0.0; /* per above, not used */
    (*path).path.disabled_nodes = 0;
    (*path).path.startup_cost = totalCost;
    (*path).path.total_cost = totalCost;
}

/*
 * cost_bitmap_or_node
 *     Estimate the cost of a BitmapOr node
 *
 * See comments for cost_bitmap_and_node.
 */
pub unsafe fn cost_bitmap_or_node(path: *mut BitmapOrPath, root: *mut PlannerInfo) {
    let mut totalCost: Cost = 0.0;
    let mut selec: Selectivity = 0.0;
    let mut l: *mut ListCell;

    /*
     * We estimate OR selectivity on the assumption that the inputs are
     * non-overlapping, since that's often the case in "x IN (list)" type
     * situations.
     *
     * The runtime cost of the BitmapOr itself is estimated at 100x
     * cpu_operator_cost for each tbm_union needed.
     */
    l = list_head((*path).bitmapquals);
    while !l.is_null() {
        let subpath = lfirst(l) as *mut Path;
        let mut subCost: Cost = 0.0;
        let mut subselec: Selectivity = 0.0;

        cost_bitmap_tree_node(subpath, &mut subCost, &mut subselec);

        selec += subselec;

        totalCost += subCost;
        if l != list_head((*path).bitmapquals) && !IsA!(subpath as *mut Node, T_IndexPath) {
            totalCost += 100.0 * cpu_operator_cost;
        }
        l = lnext((*path).bitmapquals, l);
    }
    (*path).bitmapselectivity = if selec < 1.0 { selec } else { 1.0 };
    (*path).path.rows = 0.0; /* per above, not used */
    (*path).path.startup_cost = totalCost;
    (*path).path.total_cost = totalCost;
}

// ===========================================================================
// Part 4: cost_tidscan, cost_tidrangescan, cost_subqueryscan, cost_functionscan,
//         cost_tablefuncscan, cost_valuesscan, cost_ctescan,
//         cost_namedtuplestorescan, cost_resultscan, cost_recursive_union
// ===========================================================================

/*
 * cost_tidscan
 *   Determines and returns the cost of scanning a relation using TIDs.
 *
 * 'baserel' is the relation to be scanned
 * 'tidquals' is the list of TID-checkable quals
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
pub unsafe fn cost_tidscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    tidquals: *mut List,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let mut tid_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut ntuples: f64;
    let mut l: *mut ListCell;
    let mut spc_random_page_cost: f64 = 0.0;

    /* Should only be applied to base relations */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);
    Assert!(!  tidquals.is_null());

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* Count how many tuples we expect to retrieve */
    ntuples = 0.0;
    l = list_head(tidquals);
    while !l.is_null() {
        let rinfo = lfirst(l) as *mut RestrictInfo;
        let qual = (*rinfo).clause as *mut Node;

        /*
         * We must use a TID scan for CurrentOfExpr; in any other case, we
         * should be generating a TID scan only if enable_tidscan=true.
         */
        Assert!(enable_tidscan || IsA!(qual, T_CurrentOfExpr));
        Assert!(list_length(tidquals) == 1 || !IsA!(qual, T_CurrentOfExpr));

        if IsA!(qual, T_ScalarArrayOpExpr) {
            /* Each element of the array yields 1 tuple */
            let saop = qual as *mut ScalarArrayOpExpr;
            let arraynode = lsecond((*saop).args) as *mut Node;
            ntuples += estimate_array_length(root, arraynode);
        } else if IsA!(qual, T_CurrentOfExpr) {
            /* CURRENT OF yields 1 tuple */
            ntuples += 1.0;
        } else {
            /* It's just CTID = something, count 1 tuple */
            ntuples += 1.0;
        }
        l = lnext(tidquals, l);
    }

    /*
     * The TID qual expressions will be computed once, any other baserestrict
     * quals once per retrieved tuple.
     */
    cost_qual_eval(&mut tid_qual_cost, tidquals, root);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        &mut spc_random_page_cost,
        core::ptr::null_mut(),
    );

    /* disk costs --- assume each tuple on a different page */
    run_cost += spc_random_page_cost * ntuples;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    /* XXX currently we assume TID quals are a subset of qpquals */
    startup_cost += qpqual_cost.startup + tid_qual_cost.per_tuple;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    /*
     * There are assertions above verifying that we only reach this function
     * either when enable_tidscan=true or when the TID scan is the only legal
     * path, so it's safe to set disabled_nodes to zero here.
     */
    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_tidrangescan
 *   Determines and sets the costs of scanning a relation using a range of
 *   TIDs for 'path'
 *
 * 'baserel' is the relation to be scanned
 * 'tidrangequals' is the list of TID-checkable range quals
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
pub unsafe fn cost_tidrangescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    tidrangequals: *mut List,
    param_info: *mut ParamPathInfo,
) {
    let selectivity: Selectivity;
    let pages: f64;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let mut tid_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let ntuples: f64;
    let nseqpages: f64;
    let mut spc_random_page_cost: f64 = 0.0;
    let mut spc_seq_page_cost: f64 = 0.0;

    /* Should only be applied to base relations */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* Count how many tuples and pages we expect to scan */
    selectivity = clauselist_selectivity(root, tidrangequals, (*baserel).relid as c_int,
                                         JOIN_INNER, core::ptr::null());
    pages = (selectivity * (*baserel).pages as f64).ceil();

    let pages = if pages <= 0.0 { 1.0 } else { pages };

    /*
     * The first page in a range requires a random seek, but each subsequent
     * page is just a normal sequential page read.
     */
    ntuples = selectivity * (*baserel).tuples;
    nseqpages = pages - 1.0;

    /*
     * The TID qual expressions will be computed once, any other baserestrict
     * quals once per retrieved tuple.
     */
    cost_qual_eval(&mut tid_qual_cost, tidrangequals, root);

    /* fetch estimated page cost for tablespace containing table */
    get_tablespace_page_costs(
        (*baserel).reltablespace,
        &mut spc_random_page_cost,
        &mut spc_seq_page_cost,
    );

    /* disk costs; 1 random page and the remainder as seq pages */
    run_cost += spc_random_page_cost + spc_seq_page_cost * nseqpages;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    /*
     * XXX currently we assume TID quals are a subset of qpquals at this
     * point; they will be removed (if possible) when we create the plan.
     */
    startup_cost += qpqual_cost.startup + tid_qual_cost.per_tuple;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    /* we should not generate this path type when enable_tidscan=false */
    Assert!(enable_tidscan);
    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_subqueryscan
 *   Determines and returns the cost of scanning a subquery RTE.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 * 'trivial_pathtarget' is true if the pathtarget is believed to be trivial.
 */
pub unsafe fn cost_subqueryscan(
    path: *mut SubqueryScanPath,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    trivial_pathtarget: bool,
) {
    let mut startup_cost: Cost;
    let mut run_cost: Cost;
    let qpquals: *mut List;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;

    /* Should only be applied to base relations that are subqueries */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_SUBQUERY);

    /*
     * We compute the rowcount estimate as the subplan's estimate times the
     * selectivity of relevant restriction clauses.
     */
    if !param_info.is_null() {
        qpquals = list_concat_copy((*param_info).ppi_clauses, (*baserel).baserestrictinfo);
    } else {
        qpquals = (*baserel).baserestrictinfo;
    }

    (*path).path.rows = clamp_row_est(
        (*(*path).subpath).rows
            * clauselist_selectivity(root, qpquals, 0, JOIN_INNER, core::ptr::null()),
    );

    /*
     * Cost of path is cost of evaluating the subplan, plus cost of evaluating
     * any restriction clauses and tlist that will be attached to the
     * SubqueryScan node, plus cpu_tuple_cost to account for selection and
     * projection overhead.
     */
    (*path).path.disabled_nodes = (*(*path).subpath).disabled_nodes;
    (*path).path.startup_cost = (*(*path).subpath).startup_cost;
    (*path).path.total_cost = (*(*path).subpath).total_cost;

    /*
     * However, if there are no relevant restriction clauses and the
     * pathtarget is trivial, then we expect that setrefs.c will optimize away
     * the SubqueryScan plan node altogether.
     */
    if qpquals.is_null() && trivial_pathtarget {
        return;
    }

    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost = qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost = cpu_per_tuple * (*(*path).subpath).rows;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).path.pathtarget).cost.startup;
    run_cost += (*(*path).path.pathtarget).cost.per_tuple * (*path).path.rows;

    (*path).path.startup_cost += startup_cost;
    (*path).path.total_cost += startup_cost + run_cost;
}

/*
 * cost_functionscan
 *   Determines and returns the cost of scanning a function RTE.
 *
 * 'baserel' is the relation to be scanned
 * 'param_info' is the ParamPathInfo if this is a parameterized path, else NULL
 */
pub unsafe fn cost_functionscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let rte: *mut RangeTblEntry;
    let mut exprcost = QualCost { startup: 0.0, per_tuple: 0.0 };

    /* Should only be applied to base relations that are functions */
    Assert!((*baserel).relid > 0);
    rte = planner_rt_fetch((*baserel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_FUNCTION);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /*
     * Estimate costs of executing the function expression(s).
     *
     * Currently, nodeFunctionscan.c always executes the functions to
     * completion before returning any rows, and caches the results in a
     * tuplestore.  So the function eval cost is all startup cost.
     */
    cost_qual_eval_node(&mut exprcost, (*rte).functions as *mut Node, root);

    startup_cost += exprcost.startup + exprcost.per_tuple;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_tablefuncscan
 *   Determines and returns the cost of scanning a table function.
 */
pub unsafe fn cost_tablefuncscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;
    let rte: *mut RangeTblEntry;
    let mut exprcost = QualCost { startup: 0.0, per_tuple: 0.0 };

    /* Should only be applied to base relations that are functions */
    Assert!((*baserel).relid > 0);
    rte = planner_rt_fetch((*baserel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_TABLEFUNC);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /*
     * Estimate costs of executing the table func expression(s).
     */
    cost_qual_eval_node(&mut exprcost, (*rte).tablefunc as *mut Node, root);

    startup_cost += exprcost.startup + exprcost.per_tuple;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_valuesscan
 *   Determines and returns the cost of scanning a VALUES RTE.
 */
pub unsafe fn cost_valuesscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut cpu_per_tuple: Cost;

    /* Should only be applied to base relations that are values lists */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_VALUES);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /*
     * For now, estimate list evaluation cost at one operator eval per list
     * (probably pretty bogus, but is it worth being smarter?)
     */
    cpu_per_tuple = cpu_operator_cost;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_ctescan
 *   Determines and returns the cost of scanning a CTE RTE.
 *
 * Note: this is used for both self-reference and regular CTEs.
 */
pub unsafe fn cost_ctescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut cpu_per_tuple: Cost;

    /* Should only be applied to base relations that are CTEs */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_CTE);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* Charge one CPU tuple cost per row for tuplestore manipulation */
    cpu_per_tuple = cpu_tuple_cost;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).pathtarget).cost.startup;
    run_cost += (*(*path).pathtarget).cost.per_tuple * (*path).rows;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_namedtuplestorescan
 *   Determines and returns the cost of scanning a named tuplestore.
 */
pub unsafe fn cost_namedtuplestorescan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut cpu_per_tuple: Cost;

    /* Should only be applied to base relations that are Tuplestores */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_NAMEDTUPLESTORE);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* Charge one CPU tuple cost per row for tuplestore manipulation */
    cpu_per_tuple = cpu_tuple_cost;

    /* Add scanning CPU costs */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_resultscan
 *   Determines and returns the cost of scanning an RTE_RESULT relation.
 */
pub unsafe fn cost_resultscan(
    path: *mut Path,
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut qpqual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let cpu_per_tuple: Cost;

    /* Should only be applied to RTE_RESULT base relations */
    Assert!((*baserel).relid > 0);
    Assert!((*baserel).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RESULT);

    /* Mark the path with the correct row estimate */
    if !param_info.is_null() {
        (*path).rows = (*param_info).ppi_rows;
    } else {
        (*path).rows = (*baserel).rows;
    }

    /* We charge qual cost plus cpu_tuple_cost */
    get_restriction_qual_cost(root, baserel, param_info, &mut qpqual_cost);

    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * (*baserel).tuples;

    (*path).disabled_nodes = 0;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_recursive_union
 *   Determines and returns the cost of performing a recursive union,
 *   and also the estimated output size.
 *
 * We are given Paths for the nonrecursive and recursive terms.
 */
pub unsafe fn cost_recursive_union(runion: *mut Path, nrterm: *mut Path, rterm: *mut Path) {
    let startup_cost: Cost;
    let total_cost: Cost;
    let total_rows: f64;

    /* We probably have decent estimates for the non-recursive term */
    startup_cost = (*nrterm).startup_cost;
    total_cost = (*nrterm).total_cost;
    total_rows = (*nrterm).rows;

    /*
     * We arbitrarily assume that about 10 recursive iterations will be
     * needed, and that we've managed to get a good fix on the cost and output
     * size of each one of them.
     */
    let total_cost = total_cost + 10.0 * (*rterm).total_cost;
    let total_rows = total_rows + 10.0 * (*rterm).rows;

    /*
     * Also charge cpu_tuple_cost per row to account for the costs of
     * manipulating the tuplestores.
     */
    let total_cost = total_cost + cpu_tuple_cost * total_rows;

    (*runion).disabled_nodes = (*nrterm).disabled_nodes + (*rterm).disabled_nodes;
    (*runion).startup_cost = startup_cost;
    (*runion).total_cost = total_cost;
    (*runion).rows = total_rows;
    (*(*runion).pathtarget).width = if (*(*nrterm).pathtarget).width > (*(*rterm).pathtarget).width {
        (*(*nrterm).pathtarget).width
    } else {
        (*(*rterm).pathtarget).width
    };
}

// ===========================================================================
// Part 5: cost_tuplesort (static), cost_incremental_sort, cost_sort,
//         append_nonpartial_cost (static), cost_append, cost_merge_append,
//         cost_material, cost_memoize_rescan (static)
// ===========================================================================

/*
 * cost_tuplesort
 *   Determines and returns the cost of sorting a relation using tuplesort,
 *   not including the cost of reading the input data.
 */
unsafe fn cost_tuplesort(
    startup_cost: *mut Cost,
    run_cost: *mut Cost,
    mut tuples: f64,
    width: c_int,
    mut comparison_cost: Cost,
    sort_mem: c_int,
    limit_tuples: f64,
) {
    let input_bytes = relation_byte_size(tuples, width);
    let output_bytes: f64;
    let output_tuples: f64;
    let sort_mem_bytes: i64 = sort_mem as i64 * 1024;

    /*
     * We want to be sure the cost of a sort is never estimated as zero, even
     * if passed-in tuple count is zero.  Besides, mustn't do log(0)...
     */
    if tuples < 2.0 {
        tuples = 2.0;
    }

    /* Include the default cost-per-comparison */
    comparison_cost += 2.0 * cpu_operator_cost;

    /* Do we have a useful LIMIT? */
    if limit_tuples > 0.0 && limit_tuples < tuples {
        output_tuples = limit_tuples;
        output_bytes = relation_byte_size(output_tuples, width);
    } else {
        output_tuples = tuples;
        output_bytes = input_bytes;
    }

    if output_bytes > sort_mem_bytes as f64 {
        /*
         * We'll have to use a disk-based sort of all the tuples
         */
        let npages = (input_bytes / BLCKSZ as f64).ceil();
        let nruns = input_bytes / sort_mem_bytes as f64;
        let mergeorder = tuplesort_merge_order(sort_mem_bytes);
        let log_runs: f64;
        let npageaccesses: f64;

        /*
         * CPU costs
         *
         * Assume about N log2 N comparisons
         */
        *startup_cost = comparison_cost * tuples * LOG2(tuples);

        /* Disk costs */

        /* Compute logM(r) as log(r) / log(M) */
        log_runs = if nruns > mergeorder {
            (nruns.ln() / mergeorder.ln()).ceil()
        } else {
            1.0
        };
        npageaccesses = 2.0 * npages * log_runs;
        /* Assume 3/4ths of accesses are sequential, 1/4th are not */
        *startup_cost += npageaccesses
            * (seq_page_cost * 0.75 + random_page_cost * 0.25);
    } else if tuples > 2.0 * output_tuples || input_bytes > sort_mem_bytes as f64 {
        /*
         * We'll use a bounded heap-sort keeping just K tuples in memory, for
         * a total number of tuple comparisons of N log2 K; but the constant
         * factor is a bit higher than for quicksort.  Tweak it so that the
         * cost curve is continuous at the crossover point.
         */
        *startup_cost = comparison_cost * tuples * LOG2(2.0 * output_tuples);
    } else {
        /* We'll use plain quicksort on all the input tuples */
        *startup_cost = comparison_cost * tuples * LOG2(tuples);
    }

    /*
     * Also charge a small amount (arbitrarily set equal to operator cost) per
     * extracted tuple.  We don't charge cpu_tuple_cost because a Sort node
     * doesn't do qual-checking or projection, so it has less overhead than
     * most plan nodes.
     */
    *run_cost = cpu_operator_cost * tuples;
}

/*
 * cost_incremental_sort
 *   Determines and returns the cost of sorting a relation incrementally.
 */
pub unsafe fn cost_incremental_sort(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    presorted_keys: c_int,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    mut input_tuples: f64,
    width: c_int,
    comparison_cost: Cost,
    sort_mem: c_int,
    limit_tuples: f64,
) {
    let startup_cost: Cost;
    let run_cost: Cost;
    let input_run_cost = input_total_cost - input_startup_cost;
    let group_tuples: f64;
    let input_groups: f64;
    let mut group_startup_cost: Cost = 0.0;
    let mut group_run_cost: Cost = 0.0;
    let group_input_run_cost: Cost;
    let mut presortedExprs: *mut List = NIL;
    let mut l: *mut ListCell;
    let mut unknown_varno: bool = false;

    Assert!(presorted_keys > 0 && presorted_keys < list_length(pathkeys));

    /*
     * We want to be sure the cost of a sort is never estimated as zero.
     */
    if input_tuples < 2.0 {
        input_tuples = 2.0;
    }

    /* Default estimate of number of groups, capped to one group per row. */
    let mut input_groups: f64 = if input_tuples < DEFAULT_NUM_DISTINCT {
        input_tuples
    } else {
        DEFAULT_NUM_DISTINCT
    };

    /*
     * Extract presorted keys as list of expressions.
     *
     * We need to be careful about Vars containing "varno 0".
     */
    l = list_head(pathkeys);
    let mut key_idx: c_int = 0;
    while !l.is_null() {
        let key = lfirst(l) as *mut PathKey;
        let member = linitial((*(*key).pk_eclass).ec_members) as *mut EquivalenceMember;

        /*
         * Check if the expression contains Var with "varno 0".
         */
        if bms_is_member(0, pull_varnos(root, (*member).em_expr as *mut Node)) {
            unknown_varno = true;
            break;
        }

        /* expression not containing any Vars with "varno 0" */
        presortedExprs = lappend(presortedExprs, (*member).em_expr as *mut c_void);

        key_idx += 1;
        if key_idx >= presorted_keys {
            break;
        }
        l = lnext(pathkeys, l);
    }

    /* Estimate the number of groups with equal presorted keys. */
    if !unknown_varno {
        input_groups = estimate_num_groups(
            root,
            presortedExprs,
            input_tuples,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );
    }

    group_tuples = input_tuples / input_groups;
    group_input_run_cost = input_run_cost / input_groups;

    /*
     * Estimate the average cost of sorting of one group where presorted keys
     * are equal.
     */
    cost_tuplesort(
        &mut group_startup_cost,
        &mut group_run_cost,
        group_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );

    /*
     * Startup cost of incremental sort is the startup cost of its first group
     * plus the cost of its input.
     */
    startup_cost = group_startup_cost + input_startup_cost + group_input_run_cost;

    /*
     * After we started producing tuples from the first group, the cost of
     * producing all the tuples is given by the cost to finish processing this
     * group, plus the total cost to process the remaining groups, plus the
     * remaining cost of input.
     */
    run_cost = group_run_cost
        + (group_run_cost + group_startup_cost) * (input_groups - 1.0)
        + group_input_run_cost * (input_groups - 1.0);

    /*
     * Incremental sort adds some overhead by itself. Firstly, it has to
     * detect the sort groups.
     */
    let run_cost = run_cost + (cpu_tuple_cost + comparison_cost) * input_tuples;

    /*
     * Additionally, we charge double cpu_tuple_cost for each input group to
     * account for the tuplesort_reset that's performed after each group.
     */
    let run_cost = run_cost + 2.0 * cpu_tuple_cost * input_groups;

    (*path).rows = input_tuples;

    /* should not generate these paths when enable_incremental_sort=false */
    Assert!(enable_incremental_sort);
    (*path).disabled_nodes = input_disabled_nodes;

    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_sort
 *   Determines and returns the cost of sorting a relation, including
 *   the cost of reading the input data.
 *
 * NOTE: some callers currently pass NIL for pathkeys because they
 * can't conveniently supply the sort keys.
 */
pub unsafe fn cost_sort(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    input_disabled_nodes: c_int,
    input_cost: Cost,
    tuples: f64,
    width: c_int,
    comparison_cost: Cost,
    sort_mem: c_int,
    limit_tuples: f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    cost_tuplesort(
        &mut startup_cost,
        &mut run_cost,
        tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );

    startup_cost += input_cost;

    (*path).rows = tuples;
    (*path).disabled_nodes = input_disabled_nodes + if enable_sort { 0 } else { 1 };
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * append_nonpartial_cost
 *   Estimate the cost of the non-partial paths in a Parallel Append.
 *   The non-partial paths are assumed to be the first "numpaths" paths
 *   from the subpaths list, and to be in order of decreasing cost.
 */
unsafe fn append_nonpartial_cost(
    subpaths: *mut List,
    numpaths: c_int,
    parallel_workers: c_int,
) -> Cost {
    let costarr: *mut Cost;
    let arrlen: c_int;
    let mut l: *mut ListCell;
    let mut cell: *mut ListCell;
    let mut path_index: c_int;
    let mut min_index: c_int;
    let mut max_index: c_int;

    if numpaths == 0 {
        return 0.0;
    }

    /*
     * Array length is number of workers or number of relevant paths,
     * whichever is less.
     */
    arrlen = if parallel_workers < numpaths { parallel_workers } else { numpaths };
    costarr = crate::utils::palloc::palloc(
        core::mem::size_of::<Cost>() * arrlen as usize
    ) as *mut Cost;

    /* The first few paths will each be claimed by a different worker. */
    path_index = 0;
    cell = list_head(subpaths);
    while !cell.is_null() {
        let subpath = lfirst(cell) as *mut Path;

        if path_index == arrlen {
            break;
        }
        *costarr.add(path_index as usize) = (*subpath).total_cost;
        path_index += 1;
        cell = lnext(subpaths, cell);
    }

    /*
     * Since subpaths are sorted by decreasing cost, the last one will have
     * the minimum cost.
     */
    min_index = arrlen - 1;

    /*
     * For each of the remaining subpaths, add its cost to the array element
     * with minimum cost.
     */
    let mut l = cell;
    while !l.is_null() {
        let subpath = lfirst(l) as *mut Path;

        /* Consider only the non-partial paths */
        if path_index == numpaths {
            break;
        }
        path_index += 1;

        *costarr.add(min_index as usize) += (*subpath).total_cost;

        /* Update the new min cost array index */
        min_index = 0;
        for i in 0..arrlen as usize {
            if *costarr.add(i) < *costarr.add(min_index as usize) {
                min_index = i as c_int;
            }
        }
        l = lnext(subpaths, l);
    }

    /* Return the highest cost from the array */
    max_index = 0;
    for i in 0..arrlen as usize {
        if *costarr.add(i) > *costarr.add(max_index as usize) {
            max_index = i as c_int;
        }
    }

    *costarr.add(max_index as usize)
}

/*
 * cost_append
 *   Determines and returns the cost of an Append node.
 */
pub unsafe fn cost_append(apath: *mut AppendPath) {
    let mut l: *mut ListCell;

    (*apath).path.disabled_nodes = 0;
    (*apath).path.startup_cost = 0.0;
    (*apath).path.total_cost = 0.0;
    (*apath).path.rows = 0.0;

    if (*apath).subpaths.is_null() {
        return;
    }

    if !(*apath).path.parallel_aware {
        let pathkeys = (*apath).path.pathkeys;

        if pathkeys.is_null() {
            let firstsubpath = linitial((*apath).subpaths) as *mut Path;

            /*
             * For an unordered, non-parallel-aware Append we take the startup
             * cost as the startup cost of the first subpath.
             */
            (*apath).path.startup_cost = (*firstsubpath).startup_cost;

            /*
             * Compute rows, number of disabled nodes, and total cost as sums
             * of underlying subplan values.
             */
            l = list_head((*apath).subpaths);
            while !l.is_null() {
                let subpath = lfirst(l) as *mut Path;

                (*apath).path.rows += (*subpath).rows;
                (*apath).path.disabled_nodes += (*subpath).disabled_nodes;
                (*apath).path.total_cost += (*subpath).total_cost;
                l = lnext((*apath).subpaths, l);
            }
        } else {
            /*
             * For an ordered, non-parallel-aware Append we take the startup
             * cost as the sum of the subpath startup costs.
             */
            l = list_head((*apath).subpaths);
            while !l.is_null() {
                let subpath_orig = lfirst(l) as *mut Path;
                let mut sort_path = core::mem::zeroed::<Path>(); /* dummy for result of cost_sort */
                let subpath: *mut Path;

                if !pathkeys_contained_in(pathkeys, (*subpath_orig).pathkeys) {
                    /*
                     * We'll need to insert a Sort node, so include costs for
                     * that.
                     */
                    cost_sort(
                        &mut sort_path as *mut Path,
                        core::ptr::null_mut(), /* doesn't currently need root */
                        pathkeys,
                        (*subpath_orig).disabled_nodes,
                        (*subpath_orig).total_cost,
                        (*subpath_orig).rows,
                        (*(*subpath_orig).pathtarget).width,
                        0.0,
                        work_mem,
                        (*apath).limit_tuples,
                    );
                    subpath = &mut sort_path as *mut Path;
                } else {
                    subpath = subpath_orig;
                }

                (*apath).path.rows += (*subpath).rows;
                (*apath).path.disabled_nodes += (*subpath).disabled_nodes;
                (*apath).path.startup_cost += (*subpath).startup_cost;
                (*apath).path.total_cost += (*subpath).total_cost;
                l = lnext((*apath).subpaths, l);
            }
        }
    } else {
        /* parallel-aware */
        let mut i: c_int = 0;
        let parallel_divisor = get_parallel_divisor(&mut (*apath).path as *mut Path);

        /* Parallel-aware Append never produces ordered output. */
        Assert!((*apath).path.pathkeys.is_null());

        /* Calculate startup cost. */
        l = list_head((*apath).subpaths);
        while !l.is_null() {
            let subpath = lfirst(l) as *mut Path;

            /*
             * Append will start returning tuples when the child node having
             * lowest startup cost is done setting up.
             */
            if i == 0 {
                (*apath).path.startup_cost = (*subpath).startup_cost;
            } else if i < (*apath).path.parallel_workers {
                if (*subpath).startup_cost < (*apath).path.startup_cost {
                    (*apath).path.startup_cost = (*subpath).startup_cost;
                }
            }

            /*
             * Apply parallel divisor to subpaths.
             */
            if i < (*apath).first_partial_path {
                (*apath).path.rows += (*subpath).rows / parallel_divisor;
            } else {
                let subpath_parallel_divisor = get_parallel_divisor(subpath);
                (*apath).path.rows +=
                    (*subpath).rows * (subpath_parallel_divisor / parallel_divisor);
                (*apath).path.total_cost += (*subpath).total_cost;
            }

            (*apath).path.disabled_nodes += (*subpath).disabled_nodes;
            (*apath).path.rows = clamp_row_est((*apath).path.rows);

            i += 1;
            l = lnext((*apath).subpaths, l);
        }

        /* Add cost for non-partial subpaths. */
        (*apath).path.total_cost += append_nonpartial_cost(
            (*apath).subpaths,
            (*apath).first_partial_path,
            (*apath).path.parallel_workers,
        );
    }

    /*
     * Although Append does not do any selection or projection, it's not free;
     * add a small per-tuple overhead.
     */
    (*apath).path.total_cost +=
        cpu_tuple_cost * APPEND_CPU_COST_MULTIPLIER * (*apath).path.rows;
}

/*
 * cost_merge_append
 *   Determines and returns the cost of a MergeAppend node.
 *
 * MergeAppend merges several pre-sorted input streams, using a heap that
 * at any given instant holds the next tuple from each stream.
 */
pub unsafe fn cost_merge_append(
    path: *mut Path,
    root: *mut PlannerInfo,
    pathkeys: *mut List,
    n_streams: c_int,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let comparison_cost: Cost;
    let N: f64;
    let logN: f64;

    /*
     * Avoid log(0)...
     */
    N = if n_streams < 2 { 2.0 } else { n_streams as f64 };
    logN = LOG2(N);

    /* Assumed cost per tuple comparison */
    comparison_cost = 2.0 * cpu_operator_cost;

    /* Heap creation cost */
    startup_cost += comparison_cost * N * logN;

    /* Per-tuple heap maintenance cost */
    run_cost += tuples * comparison_cost * logN;

    /*
     * Although MergeAppend does not do any selection or projection, it's not
     * free; add a small per-tuple overhead.
     */
    run_cost += cpu_tuple_cost * APPEND_CPU_COST_MULTIPLIER * tuples;

    (*path).disabled_nodes = input_disabled_nodes;
    (*path).startup_cost = startup_cost + input_startup_cost;
    (*path).total_cost = startup_cost + run_cost + input_total_cost;
}

/*
 * cost_material
 *   Determines and returns the cost of materializing a relation, including
 *   the cost of reading the input data.
 */
pub unsafe fn cost_material(
    path: *mut Path,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
    width: c_int,
) {
    let startup_cost = input_startup_cost;
    let mut run_cost = input_total_cost - input_startup_cost;
    let nbytes = relation_byte_size(tuples, width);
    let work_mem_bytes = work_mem as f64 * 1024.0;

    (*path).rows = tuples;

    /*
     * Whether spilling or not, charge 2x cpu_operator_cost per tuple to
     * reflect bookkeeping overhead.
     */
    run_cost += 2.0 * cpu_operator_cost * tuples;

    /*
     * If we will spill to disk, charge at the rate of seq_page_cost per page.
     */
    if nbytes > work_mem_bytes {
        let npages = (nbytes / BLCKSZ as f64).ceil();
        run_cost += seq_page_cost * npages;
    }

    (*path).disabled_nodes = input_disabled_nodes + if enable_material { 0 } else { 1 };
    (*path).startup_cost = startup_cost;
    (*path).total_cost = startup_cost + run_cost;
}

/*
 * cost_memoize_rescan
 *   Determines the estimated cost of rescanning a Memoize node.
 */
unsafe fn cost_memoize_rescan(
    root: *mut PlannerInfo,
    mpath: *mut MemoizePath,
    rescan_startup_cost: *mut Cost,
    rescan_total_cost: *mut Cost,
) {
    let mut estinfo = EstimationInfo { flags: 0 };
    let mut lc: *mut ListCell;
    let input_startup_cost: Cost = (*(*mpath).subpath).startup_cost;
    let input_total_cost: Cost = (*(*mpath).subpath).total_cost;
    let tuples: f64 = (*(*mpath).subpath).rows;
    let calls: f64 = (*mpath).calls;
    let width: c_int = (*(*(*mpath).subpath).pathtarget).width;

    let hash_mem_bytes: f64;
    let mut est_entry_bytes: f64;
    let est_cache_entries: f64;
    let ndistinct: f64;
    let evict_ratio: f64;
    let hit_ratio: f64;
    let mut startup_cost: Cost;
    let mut total_cost: Cost;

    /* available cache space */
    hash_mem_bytes = get_hash_memory_limit();

    /*
     * Set the number of bytes each cache entry should consume in the cache.
     */
    est_entry_bytes = relation_byte_size(tuples, width)
        + ExecEstimateCacheEntryOverheadBytes(tuples);

    /* include the estimated width for the cache keys */
    lc = if !(*mpath).param_exprs.is_null() { list_head((*mpath).param_exprs) } else { core::ptr::null_mut() };
    while !lc.is_null() {
        est_entry_bytes += get_expr_width(root, lfirst(lc) as *const Node) as f64;
        lc = lnext((*mpath).param_exprs, lc);
    }

    /* estimate on the upper limit of cache entries we can hold at once */
    est_cache_entries = (hash_mem_bytes / est_entry_bytes).floor();

    /* estimate on the distinct number of parameter values */
    ndistinct = estimate_num_groups(
        root,
        (*mpath).param_exprs,
        calls,
        core::ptr::null_mut(),
        &mut estinfo as *mut EstimationInfo,
    );

    /*
     * When the estimation fell back on using a default value, assume that
     * every call will have unique parameters.
     */
    let ndistinct = if (estinfo.flags & SELFLAG_USED_DEFAULT) != 0 {
        calls
    } else {
        ndistinct
    };

    /*
     * Set the path's est_entries.
     */
    let min_nd_ece = if ndistinct < est_cache_entries { ndistinct } else { est_cache_entries };
    (*mpath).est_entries = if min_nd_ece < PG_UINT32_MAX as f64 { min_nd_ece as u32 } else { u32::MAX };

    /*
     * Estimate how often we'll incur the cost of cache eviction.
     */
    evict_ratio = 1.0
        - (if est_cache_entries < ndistinct { est_cache_entries } else { ndistinct }) / ndistinct;

    /*
     * Estimate cache hit ratio.
     */
    hit_ratio = ((calls - ndistinct) / calls)
        * (est_cache_entries / if ndistinct > est_cache_entries { ndistinct } else { est_cache_entries });

    Assert!(hit_ratio >= 0.0 && hit_ratio <= 1.0);

    /*
     * Set the total_cost accounting for the expected cache hit ratio.
     */
    total_cost = input_total_cost * (1.0 - hit_ratio) + cpu_operator_cost;

    /* Now adjust the total cost to account for cache evictions */

    /* Charge a cpu_tuple_cost for evicting the actual cache entry */
    total_cost += cpu_tuple_cost * evict_ratio;

    /*
     * Charge a 10th of cpu_operator_cost to evict every tuple in that entry.
     */
    total_cost += cpu_operator_cost / 10.0 * evict_ratio * tuples;

    /*
     * Now adjust for storing things in the cache.
     */
    total_cost += cpu_tuple_cost + cpu_operator_cost * tuples;

    /*
     * Getting the first row must be also be proportioned according to the
     * expected cache hit ratio.
     */
    startup_cost = input_startup_cost * (1.0 - hit_ratio);

    /*
     * Additionally we charge a cpu_tuple_cost to account for cache lookups.
     */
    startup_cost += cpu_tuple_cost;

    *rescan_startup_cost = startup_cost;
    *rescan_total_cost = total_cost;
}

// ===========================================================================
// Part 6: cost_agg, get_windowclause_startup_tuples (static), cost_windowagg,
//         cost_group, initial_cost_nestloop, final_cost_nestloop
// ===========================================================================

/*
 * cost_agg
 *     Determines and returns the cost of performing an Agg plan node,
 *     including the cost of its input.
 *
 * aggcosts can be NULL when there are no actual aggregate functions.
 */
pub unsafe fn cost_agg(
    path: *mut Path,
    root: *mut PlannerInfo,
    aggstrategy: AggStrategy,
    aggcosts: *const AggClauseCosts,
    numGroupCols: c_int,
    numGroups: f64,
    quals: *mut List,
    mut disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    input_width: f64,
) {
    let output_tuples: f64;
    let startup_cost: Cost;
    let total_cost: Cost;
    let dummy_aggcosts = AggClauseCosts {
        transCost: QualCost { startup: 0.0, per_tuple: 0.0 },
        finalCost: QualCost { startup: 0.0, per_tuple: 0.0 },
        transitionSpace: 0,
    };

    /* Use all-zero per-aggregate costs if NULL is passed */
    let aggcosts: *const AggClauseCosts = if aggcosts.is_null() {
        Assert!(aggstrategy == AGG_HASHED);
        &dummy_aggcosts as *const AggClauseCosts
    } else {
        aggcosts
    };

    /*
     * The transCost.per_tuple component of aggcosts should be charged once
     * per input tuple, corresponding to the costs of evaluating the aggregate
     * transfns and their input expressions. The finalCost.per_tuple component
     * is charged once per output tuple.
     */
    let (startup_cost, total_cost, output_tuples) = if aggstrategy == AGG_PLAIN {
        let mut sc = input_total_cost;
        sc += (*aggcosts).transCost.startup;
        sc += (*aggcosts).transCost.per_tuple * input_tuples;
        sc += (*aggcosts).finalCost.startup;
        sc += (*aggcosts).finalCost.per_tuple;
        /* we aren't grouping */
        let tc = sc + cpu_tuple_cost;
        (sc, tc, 1.0_f64)
    } else if aggstrategy == AGG_SORTED || aggstrategy == AGG_MIXED {
        /* Here we are able to deliver output on-the-fly */
        let sc = input_startup_cost;
        let mut tc = input_total_cost;
        if aggstrategy == AGG_MIXED && !enable_hashagg {
            disabled_nodes += 1;
        }
        /* calcs phrased this way to match HASHED case, see note above */
        tc += (*aggcosts).transCost.startup;
        tc += (*aggcosts).transCost.per_tuple * input_tuples;
        tc += (cpu_operator_cost * numGroupCols as f64) * input_tuples;
        tc += (*aggcosts).finalCost.startup;
        tc += (*aggcosts).finalCost.per_tuple * numGroups;
        tc += cpu_tuple_cost * numGroups;
        (sc, tc, numGroups)
    } else {
        /* must be AGG_HASHED */
        let mut sc = input_total_cost;
        if !enable_hashagg {
            disabled_nodes += 1;
        }
        sc += (*aggcosts).transCost.startup;
        sc += (*aggcosts).transCost.per_tuple * input_tuples;
        /* cost of computing hash value */
        sc += (cpu_operator_cost * numGroupCols as f64) * input_tuples;
        sc += (*aggcosts).finalCost.startup;

        let mut tc = sc;
        tc += (*aggcosts).finalCost.per_tuple * numGroups;
        /* cost of retrieving from hash table */
        tc += cpu_tuple_cost * numGroups;
        (sc, tc, numGroups)
    };

    let mut startup_cost = startup_cost;
    let mut total_cost = total_cost;
    let mut output_tuples = output_tuples;

    /*
     * Add the disk costs of hash aggregation that spills to disk.
     */
    if aggstrategy == AGG_HASHED || aggstrategy == AGG_MIXED {
        let pages: f64;
        let mut pages_written: f64 = 0.0;
        let mut pages_read: f64 = 0.0;
        let spill_cost: f64;
        let hashentrysize: f64;
        let nbatches: f64;
        let mut mem_limit: usize = 0;
        let mut ngroups_limit: u64 = 0;
        let mut num_partitions: c_int = 0;
        let depth: f64;

        /*
         * Estimate number of batches based on the computed limits.
         */
        hashentrysize = hash_agg_entry_size(
            list_length((*root).aggtransinfos),
            input_width,
            (*aggcosts).transitionSpace,
        );
        hash_agg_set_limits(
            hashentrysize,
            numGroups,
            0.0,
            &mut mem_limit,
            &mut ngroups_limit,
            &mut num_partitions,
        );

        let nbatches = {
            let a = (numGroups * hashentrysize) / mem_limit as f64;
            let b = numGroups / ngroups_limit as f64;
            (if a > b { a } else { b }).ceil().max(1.0)
        };
        let num_partitions = if num_partitions > 2 { num_partitions } else { 2 };

        /*
         * The number of partitions can change at different levels of
         * recursion; but for the purposes of this calculation assume it stays
         * constant.
         */
        depth = (nbatches.ln() / (num_partitions as f64).ln()).ceil();

        /*
         * Estimate number of pages read and written.
         */
        pages = relation_byte_size(input_tuples, input_width as c_int) / BLCKSZ as f64;
        pages_written = pages * depth;
        pages_read = pages * depth;

        /*
         * HashAgg has somewhat worse IO behavior than Sort.
         */
        pages_read *= 2.0;
        pages_written *= 2.0;

        startup_cost += pages_written * random_page_cost;
        total_cost += pages_written * random_page_cost;
        total_cost += pages_read * seq_page_cost;

        /* account for CPU cost of spilling a tuple and reading it back */
        spill_cost = depth * input_tuples * 2.0 * cpu_tuple_cost;
        startup_cost += spill_cost;
        total_cost += spill_cost;
    }

    /*
     * If there are quals (HAVING quals), account for their cost and
     * selectivity.
     */
    if !quals.is_null() {
        let mut qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };

        cost_qual_eval(&mut qual_cost, quals, root);
        startup_cost += qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        output_tuples = clamp_row_est(
            output_tuples
                * clauselist_selectivity(root, quals, 0, JOIN_INNER, core::ptr::null()),
        );
    }

    (*path).rows = output_tuples;
    (*path).disabled_nodes = disabled_nodes;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = total_cost;
}

/*
 * get_windowclause_startup_tuples
 *     Estimate how many tuples we'll need to fetch from a WindowAgg's
 *     subnode before we can output the first WindowAgg tuple.
 */
unsafe fn get_windowclause_startup_tuples(
    root: *mut PlannerInfo,
    wc: *mut WindowClause,
    input_tuples: f64,
) -> f64 {
    let frameOptions: c_int = (*wc).frameOptions;
    let partition_tuples: f64;
    let return_tuples: f64;
    let peer_tuples: f64;

    /*
     * First, figure out how many partitions there are likely to be and set
     * partition_tuples according to that estimate.
     */
    partition_tuples = if !(*wc).partitionClause.is_null() {
        let partexprs = get_sortgrouplist_exprs(
            (*wc).partitionClause,
            (*(*root).parse).targetList,
        );

        let num_partitions = estimate_num_groups(
            root,
            partexprs,
            input_tuples,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );
        list_free(partexprs);

        input_tuples / num_partitions
    } else {
        /* all tuples belong to the same partition */
        input_tuples
    };

    /* estimate the number of tuples in each peer group */
    peer_tuples = if !(*wc).orderClause.is_null() {
        let orderexprs = get_sortgrouplist_exprs(
            (*wc).orderClause,
            (*(*root).parse).targetList,
        );

        /* estimate out how many peer groups there are in the partition */
        let num_groups = estimate_num_groups(
            root,
            orderexprs,
            partition_tuples,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );
        list_free(orderexprs);
        partition_tuples / num_groups
    } else {
        /* no ORDER BY so only 1 tuple belongs in each peer group */
        1.0
    };

    return_tuples = if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) != 0 {
        /* include all partition rows */
        partition_tuples
    } else if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* just count the current row */
            1.0
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /*
             * When in RANGE/GROUPS mode, it's more complex.
             */
            if (*wc).orderClause.is_null() {
                partition_tuples
            } else {
                peer_tuples
            }
        } else {
            Assert!(false);
            1.0
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
        /*
         * BETWEEN ... AND N PRECEDING will only need to read the WindowAgg's
         * subnode after N ROWS/RANGES/GROUPS.
         */
        1.0
    } else if (frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) != 0 {
        let endOffset = (*wc).endOffset as *mut Const;
        let end_offset_value: f64;

        /* try and figure out the value specified in the endOffset. */
        end_offset_value = if IsA!(endOffset as *mut Node, T_Const) {
            if (*endOffset).constisnull {
                1.0
            } else {
                match (*endOffset).consttype {
                    INT2OID => {
                        crate::postgres::DatumGetInt16((*endOffset).constvalue) as f64
                    }
                    INT4OID => {
                        crate::postgres::DatumGetInt32((*endOffset).constvalue) as f64
                    }
                    INT8OID => {
                        crate::postgres::DatumGetInt64((*endOffset).constvalue) as f64
                    }
                    _ => partition_tuples / peer_tuples * DEFAULT_INEQ_SEL,
                }
            }
        } else {
            /*
             * When the end bound is not a Const, we'll just need to guess.
             */
            partition_tuples / peer_tuples * DEFAULT_INEQ_SEL
        };

        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* include the N FOLLOWING and the current row */
            end_offset_value + 1.0
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /* include N FOLLOWING ranges/group and the initial range/group */
            peer_tuples * (end_offset_value + 1.0)
        } else {
            Assert!(false);
            1.0
        }
    } else {
        Assert!(false);
        1.0
    };

    let return_tuples = if !(*wc).partitionClause.is_null() || !(*wc).orderClause.is_null() {
        /*
         * Cap the return value to the estimated partition tuples and account
         * for the extra tuple WindowAgg will need to read.
         */
        if return_tuples + 1.0 < partition_tuples {
            return_tuples + 1.0
        } else {
            partition_tuples
        }
    } else {
        /*
         * Cap the return value so it's never higher than the expected tuples
         * in the partition.
         */
        if return_tuples < partition_tuples { return_tuples } else { partition_tuples }
    };

    /*
     * We needn't worry about any EXCLUDE options as those only exclude rows
     * from being aggregated, not from being read from the WindowAgg's
     * subnode.
     */

    clamp_row_est(return_tuples)
}

/*
 * cost_windowagg
 *     Determines and returns the cost of performing a WindowAgg plan node,
 *     including the cost of its input.
 *
 * Input is assumed already properly sorted.
 */
pub unsafe fn cost_windowagg(
    path: *mut Path,
    root: *mut PlannerInfo,
    windowFuncs: *mut List,
    winclause: *mut WindowClause,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    let mut startup_cost: Cost;
    let mut total_cost: Cost;
    let startup_tuples: f64;
    let numPartCols: c_int;
    let numOrderCols: c_int;
    let mut lc: *mut ListCell;

    numPartCols = list_length((*winclause).partitionClause);
    numOrderCols = list_length((*winclause).orderClause);

    startup_cost = input_startup_cost;
    total_cost = input_total_cost;

    /*
     * Window functions are assumed to cost their stated execution cost, plus
     * the cost of evaluating their input expressions, per tuple.
     */
    lc = if !windowFuncs.is_null() { list_head(windowFuncs) } else { core::ptr::null_mut() };
    while !lc.is_null() {
        let wfunc = lfirst(lc) as *mut WindowFunc;
        let wfunccost: Cost;
        let mut argcosts = QualCost { startup: 0.0, per_tuple: 0.0 };

        add_function_cost(root, (*wfunc).winfnoid, wfunc as *mut Node, &mut argcosts);
        startup_cost += argcosts.startup;
        let wfunccost = argcosts.per_tuple;

        /* also add the input expressions' cost to per-input-row costs */
        let mut argcosts2 = QualCost { startup: 0.0, per_tuple: 0.0 };
        cost_qual_eval_node(&mut argcosts2, (*wfunc).args as *mut Node, root);
        startup_cost += argcosts2.startup;
        let wfunccost = wfunccost + argcosts2.per_tuple;

        /*
         * Add the filter's cost to per-input-row costs.
         */
        let mut argcosts3 = QualCost { startup: 0.0, per_tuple: 0.0 };
        cost_qual_eval_node(&mut argcosts3, (*wfunc).aggfilter as *mut Node, root);
        startup_cost += argcosts3.startup;
        let wfunccost = wfunccost + argcosts3.per_tuple;

        total_cost += wfunccost * input_tuples;
        lc = lnext(windowFuncs, lc);
    }

    /*
     * We also charge cpu_operator_cost per grouping column per tuple for
     * grouping comparisons, plus cpu_tuple_cost per tuple for general
     * overhead.
     */
    total_cost += cpu_operator_cost * (numPartCols + numOrderCols) as f64 * input_tuples;
    total_cost += cpu_tuple_cost * input_tuples;

    (*path).rows = input_tuples;
    (*path).disabled_nodes = input_disabled_nodes;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = total_cost;

    /*
     * Also, take into account how many tuples we need to read from the
     * subnode in order to produce the first tuple from the WindowAgg.
     */
    startup_tuples =
        get_windowclause_startup_tuples(root, winclause, input_tuples);

    if startup_tuples > 1.0 {
        (*path).startup_cost +=
            (total_cost - startup_cost) / input_tuples * (startup_tuples - 1.0);
    }
}

/*
 * cost_group
 *     Determines and returns the cost of performing a Group plan node,
 *     including the cost of its input.
 *
 * Note: caller must ensure that input costs are for appropriately-sorted
 * input.
 */
pub unsafe fn cost_group(
    path: *mut Path,
    root: *mut PlannerInfo,
    numGroupCols: c_int,
    numGroups: f64,
    quals: *mut List,
    input_disabled_nodes: c_int,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
) {
    let mut output_tuples = numGroups;
    let startup_cost = input_startup_cost;
    let mut total_cost = input_total_cost;

    /*
     * Charge one cpu_operator_cost per comparison per input tuple.
     */
    total_cost += cpu_operator_cost * input_tuples * numGroupCols as f64;

    /*
     * If there are quals (HAVING quals), account for their cost and
     * selectivity.
     */
    if !quals.is_null() {
        let mut qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };

        cost_qual_eval(&mut qual_cost, quals, root);
        let startup_cost = startup_cost + qual_cost.startup;
        total_cost += qual_cost.startup + output_tuples * qual_cost.per_tuple;

        output_tuples = clamp_row_est(
            output_tuples
                * clauselist_selectivity(root, quals, 0, JOIN_INNER, core::ptr::null()),
        );
    }

    (*path).rows = output_tuples;
    (*path).disabled_nodes = input_disabled_nodes;
    (*path).startup_cost = startup_cost;
    (*path).total_cost = total_cost;
}

/*
 * initial_cost_nestloop
 *   Preliminary estimate of the cost of a nestloop join path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.
 */
pub unsafe fn initial_cost_nestloop(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    outer_path: *mut Path,
    inner_path: *mut Path,
    extra: *mut JoinPathExtraData,
) {
    let mut disabled_nodes: c_int;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let outer_path_rows = (*outer_path).rows;
    let inner_rescan_start_cost: Cost;
    let inner_rescan_total_cost: Cost;
    let inner_run_cost: Cost;
    let inner_rescan_run_cost: Cost;

    /* Count up disabled nodes. */
    disabled_nodes = if enable_nestloop { 0 } else { 1 };
    disabled_nodes += (*inner_path).disabled_nodes;
    disabled_nodes += (*outer_path).disabled_nodes;

    /* estimate costs to rescan the inner relation */
    let mut rescan_start: Cost = 0.0;
    let mut rescan_total: Cost = 0.0;
    cost_rescan(root, inner_path, &mut rescan_start, &mut rescan_total);
    let inner_rescan_start_cost = rescan_start;
    let inner_rescan_total_cost = rescan_total;

    /* cost of source data */

    /*
     * NOTE: clearly, we must pay both outer and inner paths' startup_cost
     * before we can start returning tuples.
     */
    startup_cost += (*outer_path).startup_cost + (*inner_path).startup_cost;
    run_cost += (*outer_path).total_cost - (*outer_path).startup_cost;
    if outer_path_rows > 1.0 {
        run_cost += (outer_path_rows - 1.0) * inner_rescan_start_cost;
    }

    inner_run_cost = (*inner_path).total_cost - (*inner_path).startup_cost;
    inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost;

    use crate::nodes::nodes::JoinType::*;
    if jointype == JOIN_SEMI
        || jointype == JOIN_ANTI
        || (*extra).inner_unique
    {
        /*
         * With a SEMI or ANTI join, or if the innerrel is known unique, the
         * executor will stop after the first match.
         */

        /* Save private data for final_cost_nestloop */
        (*workspace).inner_run_cost = inner_run_cost;
        (*workspace).inner_rescan_run_cost = inner_rescan_run_cost;
    } else {
        /* Normal case; we'll scan whole input rel for each outer row */
        run_cost += inner_run_cost;
        if outer_path_rows > 1.0 {
            run_cost += (outer_path_rows - 1.0) * inner_rescan_run_cost;
        }
    }

    /* CPU costs left for later */

    /* Public result fields */
    (*workspace).disabled_nodes = disabled_nodes;
    (*workspace).startup_cost = startup_cost;
    (*workspace).total_cost = startup_cost + run_cost;
    /* Save private data for final_cost_nestloop */
    (*workspace).run_cost = run_cost;
}

/*
 * final_cost_nestloop
 *   Final estimate of the cost and result size of a nestloop join path.
 *
 * 'path' is already filled in except for the rows and cost fields
 * 'workspace' is the result from initial_cost_nestloop
 * 'extra' contains miscellaneous information about the join
 */
pub unsafe fn final_cost_nestloop(
    root: *mut PlannerInfo,
    path: *mut NestPath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    let outer_path: *mut Path = (*path).jpath.outerjoinpath;
    let inner_path: *mut Path = (*path).jpath.innerjoinpath;
    let mut outer_path_rows = (*outer_path).rows;
    let mut inner_path_rows = (*inner_path).rows;
    let startup_cost = (*workspace).startup_cost;
    let mut run_cost = (*workspace).run_cost;
    let cpu_per_tuple: Cost;
    let mut restrict_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let ntuples: f64;

    /* Set the number of disabled nodes. */
    (*path).jpath.path.disabled_nodes = (*workspace).disabled_nodes;

    /* Protect some assumptions below that rowcounts aren't zero */
    if outer_path_rows <= 0.0 { outer_path_rows = 1.0; }
    if inner_path_rows <= 0.0 { inner_path_rows = 1.0; }
    /* Mark the path with the correct row estimate */
    if !(*path).jpath.path.param_info.is_null() {
        (*path).jpath.path.rows = (*(*path).jpath.path.param_info).ppi_rows;
    } else {
        (*path).jpath.path.rows = (*(*path).jpath.path.parent).rows;
    }

    /* For partial paths, scale row estimate. */
    if (*path).jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(&mut (*path).jpath.path as *mut Path);
        (*path).jpath.path.rows =
            clamp_row_est((*path).jpath.path.rows / parallel_divisor);
    }

    /* cost of inner-relation source data (we already dealt with outer rel) */
    use crate::nodes::nodes::JoinType::*;
    let ntuples = if (*path).jpath.jointype == JOIN_SEMI
        || (*path).jpath.jointype == JOIN_ANTI
        || (*extra).inner_unique
    {
        /*
         * With a SEMI or ANTI join, or if the innerrel is known unique, the
         * executor will stop after the first match.
         */
        let inner_run_cost = (*workspace).inner_run_cost;
        let inner_rescan_run_cost = (*workspace).inner_rescan_run_cost;
        let outer_matched_rows: f64 =
            (outer_path_rows * (*extra).semifactors.outer_match_frac).round();
        let mut outer_unmatched_rows = outer_path_rows - outer_matched_rows;
        let inner_scan_frac: Selectivity = 2.0 / ((*extra).semifactors.match_count + 1.0);

        /*
         * Compute number of tuples processed (not number emitted!).
         */
        let mut ntuples = outer_matched_rows * inner_path_rows * inner_scan_frac;

        /*
         * Now we need to estimate the actual costs of scanning the inner
         * relation.
         */
        if has_indexed_join_quals(path) {
            /*
             * Successfully-matched outer rows will only require scanning
             * inner_scan_frac of the inner relation.
             */
            run_cost += inner_run_cost * inner_scan_frac;
            if outer_matched_rows > 1.0 {
                run_cost += (outer_matched_rows - 1.0)
                    * inner_rescan_run_cost
                    * inner_scan_frac;
            }

            /*
             * Add the cost of inner-scan executions for unmatched outer rows.
             */
            run_cost +=
                outer_unmatched_rows * inner_rescan_run_cost / inner_path_rows;

            /*
             * We won't be evaluating any quals at all for unmatched rows, so
             * don't add them to ntuples.
             */
        } else {
            /* First, count all unmatched join tuples as being processed */
            ntuples += outer_unmatched_rows * inner_path_rows;

            /* Now add the forced full scan, and decrement appropriate count */
            run_cost += inner_run_cost;
            if outer_unmatched_rows >= 1.0 {
                outer_unmatched_rows -= 1.0;
            } else {
                let outer_matched_rows = outer_matched_rows - 1.0;
            }

            /* Add inner run cost for additional outer tuples having matches */
            if outer_matched_rows > 0.0 {
                run_cost +=
                    outer_matched_rows * inner_rescan_run_cost * inner_scan_frac;
            }

            /* Add inner run cost for additional unmatched outer tuples */
            if outer_unmatched_rows > 0.0 {
                run_cost += outer_unmatched_rows * inner_rescan_run_cost;
            }
        }
        ntuples
    } else {
        /* Normal-case source costs were included in preliminary estimate */

        /* Compute number of tuples processed (not number emitted!) */
        outer_path_rows * inner_path_rows
    };

    /* CPU costs */
    cost_qual_eval(&mut restrict_qual_cost, (*path).jpath.joinrestrictinfo, root);
    let startup_cost = startup_cost + restrict_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + restrict_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    let startup_cost = startup_cost + (*(*path).jpath.path.pathtarget).cost.startup;
    run_cost += (*(*path).jpath.path.pathtarget).cost.per_tuple * (*path).jpath.path.rows;

    (*path).jpath.path.startup_cost = startup_cost;
    (*path).jpath.path.total_cost = startup_cost + run_cost;
}

// ===========================================================================
// Part 7: initial_cost_mergejoin, final_cost_mergejoin, cached_scansel,
//         initial_cost_hashjoin, final_cost_hashjoin
// ===========================================================================

/*
 * initial_cost_mergejoin
 *   Preliminary estimate of the cost of a mergejoin path.
 */
pub unsafe fn initial_cost_mergejoin(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    mergeclauses: *mut List,
    outer_path: *mut Path,
    inner_path: *mut Path,
    outersortkeys: *mut List,
    innersortkeys: *mut List,
    outer_presorted_keys: c_int,
    extra: *mut JoinPathExtraData,
) {
    let mut disabled_nodes: c_int;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut outer_path_rows = (*outer_path).rows;
    let mut inner_path_rows = (*inner_path).rows;
    let inner_run_cost: Cost;
    let (outer_rows, inner_rows, outer_skip_rows, inner_skip_rows): (f64, f64, f64, f64);
    let (mut outerstartsel, mut outerendsel, mut innerstartsel, mut innerendsel): (Selectivity, Selectivity, Selectivity, Selectivity);
    let mut sort_path: Path = core::mem::zeroed();

    /* Protect some assumptions below that rowcounts aren't zero */
    if outer_path_rows <= 0.0 { outer_path_rows = 1.0; }
    if inner_path_rows <= 0.0 { inner_path_rows = 1.0; }

    if !mergeclauses.is_null() && jointype != JOIN_INNER.into() {
        // use JOIN_FULL check
        let is_full = {
            use crate::nodes::nodes::JoinType::*;
            jointype == JOIN_FULL
        };
        if !is_full {
            let firstclause: *mut RestrictInfo =
                linitial(mergeclauses) as *mut RestrictInfo;
            let opathkeys: *mut List = if !outersortkeys.is_null() {
                outersortkeys
            } else {
                (*outer_path).pathkeys
            };
            let ipathkeys: *mut List = if !innersortkeys.is_null() {
                innersortkeys
            } else {
                (*inner_path).pathkeys
            };
            Assert!(!  opathkeys.is_null());
            Assert!(!  ipathkeys.is_null());
            let opathkey: *mut PathKey = linitial(opathkeys) as *mut PathKey;
            let ipathkey: *mut PathKey = linitial(ipathkeys) as *mut PathKey;
            /* debugging check */
            if (*opathkey).pk_opfamily != (*ipathkey).pk_opfamily
                || (*(*opathkey).pk_eclass).ec_collation
                    != (*(*ipathkey).pk_eclass).ec_collation
                || (*opathkey).pk_cmptype != (*ipathkey).pk_cmptype
                || (*opathkey).pk_nulls_first != (*ipathkey).pk_nulls_first
            {
                elog!(crate::utils::elog::ERROR,
                    "left and right pathkeys do not match in mergejoin");
            }
            /* Get the selectivity with caching */
            let cache: *mut MergeScanSelCache =
                cached_scansel(root, firstclause, opathkey);

            if bms_is_subset(
                (*firstclause).left_relids,
                (*(*outer_path).parent).relids,
            ) {
                /* left side of clause is outer */
                outerstartsel = (*cache).leftstartsel;
                outerendsel = (*cache).leftendsel;
                innerstartsel = (*cache).rightstartsel;
                innerendsel = (*cache).rightendsel;
            } else {
                /* left side of clause is inner */
                outerstartsel = (*cache).rightstartsel;
                outerendsel = (*cache).rightendsel;
                innerstartsel = (*cache).leftstartsel;
                innerendsel = (*cache).leftendsel;
            }
            use crate::nodes::nodes::JoinType::*;
            if jointype == JOIN_LEFT || jointype == JOIN_ANTI {
                outerstartsel = 0.0;
                outerendsel = 1.0;
            } else if jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI {
                innerstartsel = 0.0;
                innerendsel = 1.0;
            }
        } else {
            outerstartsel = 0.0;
            innerstartsel = 0.0;
            outerendsel = 1.0;
            innerendsel = 1.0;
        }
    } else {
        /* cope with clauseless or full mergejoin */
        outerstartsel = 0.0;
        innerstartsel = 0.0;
        outerendsel = 1.0;
        innerendsel = 1.0;
    }

    /*
     * Convert selectivities to row counts.
     */
    outer_skip_rows = (outer_path_rows * outerstartsel).round();
    inner_skip_rows = (inner_path_rows * innerstartsel).round();
    outer_rows = clamp_row_est(outer_path_rows * outerendsel);
    inner_rows = clamp_row_est(inner_path_rows * innerendsel);

    Assert!(outer_skip_rows <= outer_rows);
    Assert!(inner_skip_rows <= inner_rows);

    /* Readjust scan selectivities */
    outerstartsel = outer_skip_rows / outer_path_rows;
    innerstartsel = inner_skip_rows / inner_path_rows;
    outerendsel = outer_rows / outer_path_rows;
    innerendsel = inner_rows / inner_path_rows;

    Assert!(outerstartsel <= outerendsel);
    Assert!(innerstartsel <= innerendsel);

    disabled_nodes = if enable_mergejoin { 0 } else { 1 };

    /* cost of source data */

    if !outersortkeys.is_null() {
        /* do we need to sort outer? */
        Assert!(!  pathkeys_contained_in(outersortkeys, (*outer_path).pathkeys));

        if enable_incremental_sort && outer_presorted_keys > 0 {
            cost_incremental_sort(
                &mut sort_path as *mut Path,
                root,
                outersortkeys,
                outer_presorted_keys,
                (*outer_path).disabled_nodes,
                (*outer_path).startup_cost,
                (*outer_path).total_cost,
                outer_path_rows,
                (*(*outer_path).pathtarget).width,
                0.0,
                work_mem,
                -1.0,
            );
        } else {
            cost_sort(
                &mut sort_path as *mut Path,
                root,
                outersortkeys,
                (*outer_path).disabled_nodes,
                (*outer_path).total_cost,
                outer_path_rows,
                (*(*outer_path).pathtarget).width,
                0.0,
                work_mem,
                -1.0,
            );
        }
        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost +=
            (sort_path.total_cost - sort_path.startup_cost) * outerstartsel;
        run_cost +=
            (sort_path.total_cost - sort_path.startup_cost) * (outerendsel - outerstartsel);
    } else {
        disabled_nodes += (*outer_path).disabled_nodes;
        startup_cost += (*outer_path).startup_cost;
        startup_cost +=
            ((*outer_path).total_cost - (*outer_path).startup_cost) * outerstartsel;
        run_cost += ((*outer_path).total_cost - (*outer_path).startup_cost)
            * (outerendsel - outerstartsel);
    }

    if !innersortkeys.is_null() {
        /* do we need to sort inner? */
        Assert!(!  pathkeys_contained_in(innersortkeys, (*inner_path).pathkeys));
        /* We do not consider incremental sort for inner path */
        cost_sort(
            &mut sort_path as *mut Path,
            root,
            innersortkeys,
            (*inner_path).disabled_nodes,
            (*inner_path).total_cost,
            inner_path_rows,
            (*(*inner_path).pathtarget).width,
            0.0,
            work_mem,
            -1.0,
        );
        disabled_nodes += sort_path.disabled_nodes;
        startup_cost += sort_path.startup_cost;
        startup_cost +=
            (sort_path.total_cost - sort_path.startup_cost) * innerstartsel;
        inner_run_cost =
            (sort_path.total_cost - sort_path.startup_cost) * (innerendsel - innerstartsel);
    } else {
        disabled_nodes += (*inner_path).disabled_nodes;
        startup_cost += (*inner_path).startup_cost;
        startup_cost +=
            ((*inner_path).total_cost - (*inner_path).startup_cost) * innerstartsel;
        inner_run_cost = ((*inner_path).total_cost - (*inner_path).startup_cost)
            * (innerendsel - innerstartsel);
    }

    /* CPU costs left for later */

    /* Public result fields */
    (*workspace).disabled_nodes = disabled_nodes;
    (*workspace).startup_cost = startup_cost;
    (*workspace).total_cost = startup_cost + run_cost + inner_run_cost;
    /* Save private data for final_cost_mergejoin */
    (*workspace).run_cost = run_cost;
    (*workspace).inner_run_cost = inner_run_cost;
    (*workspace).outer_rows = outer_rows;
    (*workspace).inner_rows = inner_rows;
    (*workspace).outer_skip_rows = outer_skip_rows;
    (*workspace).inner_skip_rows = inner_skip_rows;
}

/*
 * final_cost_mergejoin
 *   Final estimate of the cost and result size of a mergejoin path.
 */
pub unsafe fn final_cost_mergejoin(
    root: *mut PlannerInfo,
    path: *mut MergePath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    let outer_path: *mut Path = (*path).jpath.outerjoinpath;
    let inner_path: *mut Path = (*path).jpath.innerjoinpath;
    let mut inner_path_rows = (*inner_path).rows;
    let mergeclauses: *mut List = (*path).path_mergeclauses;
    let innersortkeys: *mut List = (*path).innersortkeys;
    let startup_cost = (*workspace).startup_cost;
    let mut run_cost = (*workspace).run_cost;
    let inner_run_cost = (*workspace).inner_run_cost;
    let outer_rows = (*workspace).outer_rows;
    let inner_rows = (*workspace).inner_rows;
    let outer_skip_rows = (*workspace).outer_skip_rows;
    let inner_skip_rows = (*workspace).inner_skip_rows;
    let cpu_per_tuple: Cost;
    let bare_inner_cost: Cost;
    let mat_inner_cost: Cost;
    let mut merge_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut qp_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mergejointuples: f64;
    let mut rescannedtuples: f64;
    let rescanratio: f64;

    /* Set the number of disabled nodes. */
    (*path).jpath.path.disabled_nodes = (*workspace).disabled_nodes;

    /* Protect some assumptions below that rowcounts aren't zero */
    if inner_path_rows <= 0.0 { inner_path_rows = 1.0; }

    /* Mark the path with the correct row estimate */
    if !(*path).jpath.path.param_info.is_null() {
        (*path).jpath.path.rows = (*(*path).jpath.path.param_info).ppi_rows;
    } else {
        (*path).jpath.path.rows = (*(*path).jpath.path.parent).rows;
    }

    /* For partial paths, scale row estimate. */
    if (*path).jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(&mut (*path).jpath.path as *mut Path);
        (*path).jpath.path.rows =
            clamp_row_est((*path).jpath.path.rows / parallel_divisor);
    }

    /* Compute cost of the mergequals and qpquals separately. */
    cost_qual_eval(&mut merge_qual_cost, mergeclauses, root);
    cost_qual_eval(&mut qp_qual_cost, (*path).jpath.joinrestrictinfo, root);
    qp_qual_cost.startup -= merge_qual_cost.startup;
    qp_qual_cost.per_tuple -= merge_qual_cost.per_tuple;

    /*
     * With a SEMI or ANTI join, or if the innerrel is known unique, the
     * executor will stop scanning for matches after the first match.
     */
    use crate::nodes::nodes::JoinType::*;
    if ((*path).jpath.jointype == JOIN_SEMI
        || (*path).jpath.jointype == JOIN_ANTI
        || (*extra).inner_unique)
        && (list_length((*path).jpath.joinrestrictinfo)
            == list_length((*path).path_mergeclauses))
    {
        (*path).skip_mark_restore = true;
    } else {
        (*path).skip_mark_restore = false;
    }

    mergejointuples = approx_tuple_count(root, &mut (*path).jpath as *mut JoinPath, mergeclauses);

    if IsA!(outer_path as *mut Node, T_UniquePath) || (*path).skip_mark_restore {
        rescannedtuples = 0.0;
    } else {
        rescannedtuples = mergejointuples - inner_path_rows;
        if rescannedtuples < 0.0 {
            rescannedtuples = 0.0;
        }
    }

    rescanratio = 1.0 + (rescannedtuples / inner_rows);

    bare_inner_cost = inner_run_cost * rescanratio;
    mat_inner_cost =
        inner_run_cost + cpu_operator_cost * inner_rows * rescanratio;

    /* Decide whether we want to materialize the inner input */
    if (*path).skip_mark_restore {
        (*path).materialize_inner = false;
    } else if enable_material && mat_inner_cost < bare_inner_cost {
        (*path).materialize_inner = true;
    } else if innersortkeys.is_null()
        && !ExecSupportsMarkRestore(inner_path)
    {
        (*path).materialize_inner = true;
    } else if enable_material
        && !innersortkeys.is_null()
        && relation_byte_size(inner_path_rows, (*(*inner_path).pathtarget).width)
            > work_mem as f64 * 1024.0
    {
        (*path).materialize_inner = true;
    } else {
        (*path).materialize_inner = false;
    }

    /* Charge the right incremental cost for the chosen case */
    if (*path).materialize_inner {
        run_cost += mat_inner_cost;
    } else {
        run_cost += bare_inner_cost;
    }

    /* CPU costs */
    let mut startup_cost = startup_cost;
    startup_cost += merge_qual_cost.startup;
    startup_cost +=
        merge_qual_cost.per_tuple * (outer_skip_rows + inner_skip_rows * rescanratio);
    run_cost += merge_qual_cost.per_tuple
        * ((outer_rows - outer_skip_rows) + (inner_rows - inner_skip_rows) * rescanratio);

    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * mergejointuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).jpath.path.pathtarget).cost.startup;
    run_cost += (*(*path).jpath.path.pathtarget).cost.per_tuple
        * (*path).jpath.path.rows;

    (*path).jpath.path.startup_cost = startup_cost;
    (*path).jpath.path.total_cost = startup_cost + run_cost;
}

/*
 * run mergejoinscansel() with caching
 */
unsafe fn cached_scansel(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    pathkey: *mut PathKey,
) -> *mut MergeScanSelCache {
    let mut lc: *mut ListCell;

    /* Do we have this result already? */
    lc = if !(*rinfo).scansel_cache.is_null() {
        list_head((*rinfo).scansel_cache)
    } else {
        core::ptr::null_mut()
    };
    while !lc.is_null() {
        let cache = lfirst(lc) as *mut MergeScanSelCache;
        if (*cache).opfamily == (*pathkey).pk_opfamily
            && (*cache).collation == (*(*pathkey).pk_eclass).ec_collation
            && (*cache).cmptype == (*pathkey).pk_cmptype
            && (*cache).nulls_first == (*pathkey).pk_nulls_first
        {
            return cache;
        }
        lc = lnext((*rinfo).scansel_cache, lc);
    }

    /* Nope, do the computation */
    let mut leftstartsel: Selectivity = 0.0;
    let mut leftendsel: Selectivity = 0.0;
    let mut rightstartsel: Selectivity = 0.0;
    let mut rightendsel: Selectivity = 0.0;

    mergejoinscansel(
        root,
        (*rinfo).clause as *mut Node,
        (*pathkey).pk_opfamily,
        (*pathkey).pk_cmptype,
        (*pathkey).pk_nulls_first,
        &mut leftstartsel,
        &mut leftendsel,
        &mut rightstartsel,
        &mut rightendsel,
    );

    /* Cache the result in suitably long-lived workspace */
    let oldcontext = MemoryContextSwitchTo((*root).planner_cxt as crate::utils::palloc::MemoryContext);

    let cache = crate::utils::palloc::palloc(core::mem::size_of::<MergeScanSelCache>())
        as *mut MergeScanSelCache;
    (*cache).opfamily = (*pathkey).pk_opfamily;
    (*cache).collation = (*(*pathkey).pk_eclass).ec_collation;
    (*cache).cmptype = (*pathkey).pk_cmptype;
    (*cache).nulls_first = (*pathkey).pk_nulls_first;
    (*cache).leftstartsel = leftstartsel;
    (*cache).leftendsel = leftendsel;
    (*cache).rightstartsel = rightstartsel;
    (*cache).rightendsel = rightendsel;

    (*rinfo).scansel_cache = lappend((*rinfo).scansel_cache, cache as *mut c_void);

    MemoryContextSwitchTo(oldcontext);

    cache
}

/*
 * initial_cost_hashjoin
 *   Preliminary estimate of the cost of a hashjoin path.
 */
pub unsafe fn initial_cost_hashjoin(
    root: *mut PlannerInfo,
    workspace: *mut JoinCostWorkspace,
    jointype: JoinType,
    hashclauses: *mut List,
    outer_path: *mut Path,
    inner_path: *mut Path,
    extra: *mut JoinPathExtraData,
    parallel_hash: bool,
) {
    let mut disabled_nodes: c_int;
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let outer_path_rows = (*outer_path).rows;
    let inner_path_rows = (*inner_path).rows;
    let mut inner_path_rows_total = inner_path_rows;
    let num_hashclauses = list_length(hashclauses);
    let mut numbuckets: c_int = 0;
    let mut numbatches: c_int = 0;
    let mut num_skew_mcvs: c_int = 0;
    let mut space_allowed: usize = 0; /* unused */

    /* Count up disabled nodes. */
    disabled_nodes = if enable_hashjoin { 0 } else { 1 };
    disabled_nodes += (*inner_path).disabled_nodes;
    disabled_nodes += (*outer_path).disabled_nodes;

    /* cost of source data */
    startup_cost += (*outer_path).startup_cost;
    run_cost += (*outer_path).total_cost - (*outer_path).startup_cost;
    startup_cost += (*inner_path).total_cost;

    /*
     * Cost of computing hash function: must do it once per input tuple.
     */
    startup_cost +=
        (cpu_operator_cost * num_hashclauses as f64 + cpu_tuple_cost) * inner_path_rows;
    run_cost += cpu_operator_cost * num_hashclauses as f64 * outer_path_rows;

    if parallel_hash {
        inner_path_rows_total *= get_parallel_divisor(inner_path);
    }

    ExecChooseHashTableSize(
        inner_path_rows_total,
        (*(*inner_path).pathtarget).width,
        true,  /* useskew */
        parallel_hash, /* try_combined_hash_mem */
        (*outer_path).parallel_workers,
        &mut space_allowed,
        &mut numbuckets,
        &mut numbatches,
        &mut num_skew_mcvs,
    );

    if numbatches > 1 {
        let outerpages = page_size(outer_path_rows, (*(*outer_path).pathtarget).width);
        let innerpages = page_size(inner_path_rows, (*(*inner_path).pathtarget).width);

        startup_cost += seq_page_cost * innerpages;
        run_cost += seq_page_cost * (innerpages + 2.0 * outerpages);
    }

    /* CPU costs left for later */

    /* Public result fields */
    (*workspace).disabled_nodes = disabled_nodes;
    (*workspace).startup_cost = startup_cost;
    (*workspace).total_cost = startup_cost + run_cost;
    /* Save private data for final_cost_hashjoin */
    (*workspace).run_cost = run_cost;
    (*workspace).numbuckets = numbuckets;
    (*workspace).numbatches = numbatches;
    (*workspace).inner_rows_total = inner_path_rows_total;
}

/*
 * final_cost_hashjoin
 *   Final estimate of the cost and result size of a hashjoin path.
 */
pub unsafe fn final_cost_hashjoin(
    root: *mut PlannerInfo,
    path: *mut HashPath,
    workspace: *mut JoinCostWorkspace,
    extra: *mut JoinPathExtraData,
) {
    let outer_path: *mut Path = (*path).jpath.outerjoinpath;
    let inner_path: *mut Path = (*path).jpath.innerjoinpath;
    let outer_path_rows = (*outer_path).rows;
    let inner_path_rows = (*inner_path).rows;
    let inner_path_rows_total = (*workspace).inner_rows_total;
    let hashclauses: *mut List = (*path).path_hashclauses;
    let mut startup_cost = (*workspace).startup_cost;
    let mut run_cost = (*workspace).run_cost;
    let numbuckets = (*workspace).numbuckets;
    let numbatches = (*workspace).numbatches;
    let cpu_per_tuple: Cost;
    let mut hash_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let mut qp_qual_cost = QualCost { startup: 0.0, per_tuple: 0.0 };
    let hashjointuples: f64;
    let virtualbuckets: f64;
    let mut innerbucketsize: Selectivity;
    let mut innermcvfreq: Selectivity;
    let mut hcl: *mut ListCell;

    /* Set the number of disabled nodes. */
    (*path).jpath.path.disabled_nodes = (*workspace).disabled_nodes;

    /* Mark the path with the correct row estimate */
    if !(*path).jpath.path.param_info.is_null() {
        (*path).jpath.path.rows = (*(*path).jpath.path.param_info).ppi_rows;
    } else {
        (*path).jpath.path.rows = (*(*path).jpath.path.parent).rows;
    }

    /* For partial paths, scale row estimate. */
    if (*path).jpath.path.parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(&mut (*path).jpath.path as *mut Path);
        (*path).jpath.path.rows =
            clamp_row_est((*path).jpath.path.rows / parallel_divisor);
    }

    /* mark the path with estimated # of batches */
    (*path).num_batches = numbatches;
    /* store the total number of tuples */
    (*path).inner_rows_total = inner_path_rows_total;

    virtualbuckets = numbuckets as f64 * numbatches as f64;

    /* Determine bucketsize fraction and MCV frequency for the inner relation. */
    if IsA!(inner_path as *mut Node, T_UniquePath) {
        innerbucketsize = 1.0 / virtualbuckets;
        innermcvfreq = 0.0;
    } else {
        innerbucketsize = 1.0;
        innermcvfreq = 1.0;

        /* Try to estimate bucket size using extended statistics. */
        let otherclauses: *mut List = estimate_multivariate_bucketsize(
            root,
            (*inner_path).parent,
            hashclauses,
            &mut innerbucketsize,
        );

        /* Pass through the remaining clauses */
        hcl = if !otherclauses.is_null() {
            list_head(otherclauses)
        } else {
            core::ptr::null_mut()
        };
        while !hcl.is_null() {
            let restrictinfo = lfirst(hcl) as *mut RestrictInfo;
            let thisbucketsize: Selectivity;
            let thismcvfreq: Selectivity;

            if bms_is_subset((*restrictinfo).right_relids, (*(*inner_path).parent).relids) {
                /* righthand side is inner */
                thisbucketsize = (*restrictinfo).right_bucketsize;
                if thisbucketsize < 0.0 {
                    /* not cached yet */
                    estimate_hash_bucket_stats(
                        root,
                        get_rightop((*restrictinfo).clause as *mut Node),
                        virtualbuckets,
                        &mut (*restrictinfo).right_mcvfreq,
                        &mut (*restrictinfo).right_bucketsize,
                    );
                }
                let thisbucketsize = (*restrictinfo).right_bucketsize;
                let thismcvfreq = (*restrictinfo).right_mcvfreq;
                if innerbucketsize > thisbucketsize { innerbucketsize = thisbucketsize; }
                if innermcvfreq > thismcvfreq { innermcvfreq = thismcvfreq; }
            } else {
                Assert!(bms_is_subset((*restrictinfo).left_relids, (*(*inner_path).parent).relids));
                /* lefthand side is inner */
                let thisbucketsize = (*restrictinfo).left_bucketsize;
                if thisbucketsize < 0.0 {
                    /* not cached yet */
                    estimate_hash_bucket_stats(
                        root,
                        get_leftop((*restrictinfo).clause as *mut Node),
                        virtualbuckets,
                        &mut (*restrictinfo).left_mcvfreq,
                        &mut (*restrictinfo).left_bucketsize,
                    );
                }
                let thisbucketsize = (*restrictinfo).left_bucketsize;
                let thismcvfreq = (*restrictinfo).left_mcvfreq;
                if innerbucketsize > thisbucketsize { innerbucketsize = thisbucketsize; }
                if innermcvfreq > thismcvfreq { innermcvfreq = thismcvfreq; }
            }
            hcl = lnext(otherclauses, hcl);
        }
    }

    /*
     * If the bucket holding the inner MCV would exceed hash_mem, apply disable_cost.
     */
    if relation_byte_size(
        clamp_row_est(inner_path_rows * innermcvfreq),
        (*(*inner_path).pathtarget).width,
    ) > get_hash_memory_limit() as f64
    {
        startup_cost += disable_cost;
    }

    /* Compute cost of the hashquals and qpquals separately. */
    cost_qual_eval(&mut hash_qual_cost, hashclauses, root);
    cost_qual_eval(&mut qp_qual_cost, (*path).jpath.joinrestrictinfo, root);
    qp_qual_cost.startup -= hash_qual_cost.startup;
    qp_qual_cost.per_tuple -= hash_qual_cost.per_tuple;

    /* CPU costs */
    use crate::nodes::nodes::JoinType::*;
    let hashjointuples = if (*path).jpath.jointype == JOIN_SEMI
        || (*path).jpath.jointype == JOIN_ANTI
        || (*extra).inner_unique
    {
        let outer_matched_rows =
            (outer_path_rows * (*extra).semifactors.outer_match_frac).round();
        let inner_scan_frac = 2.0 / ((*extra).semifactors.match_count + 1.0);

        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_matched_rows
            * clamp_row_est(inner_path_rows * innerbucketsize * inner_scan_frac)
            * 0.5;

        run_cost += hash_qual_cost.per_tuple
            * (outer_path_rows - outer_matched_rows)
            * clamp_row_est(inner_path_rows / virtualbuckets)
            * 0.05;

        if (*path).jpath.jointype == JOIN_ANTI {
            outer_path_rows - outer_matched_rows
        } else {
            outer_matched_rows
        }
    } else {
        startup_cost += hash_qual_cost.startup;
        run_cost += hash_qual_cost.per_tuple
            * outer_path_rows
            * clamp_row_est(inner_path_rows * innerbucketsize)
            * 0.5;

        approx_tuple_count(root, &mut (*path).jpath as *mut JoinPath, hashclauses)
    };

    startup_cost += qp_qual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost + qp_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * hashjointuples;

    /* tlist eval costs are paid per output row, not per tuple scanned */
    startup_cost += (*(*path).jpath.path.pathtarget).cost.startup;
    run_cost += (*(*path).jpath.path.pathtarget).cost.per_tuple
        * (*path).jpath.path.rows;

    (*path).jpath.path.startup_cost = startup_cost;
    (*path).jpath.path.total_cost = startup_cost + run_cost;
}

// ===========================================================================
// Part 8: cost_subplan, cost_rescan (static), cost_qual_eval,
//         cost_qual_eval_node, cost_qual_eval_walker (static),
//         get_restriction_qual_cost (static), compute_semi_anti_join_factors,
//         has_indexed_join_quals (static), approx_tuple_count (static),
//         set_baserel_size_estimates, get_parameterized_baserel_size,
//         set_joinrel_size_estimates, get_parameterized_joinrel_size,
//         calc_joinrel_size_estimate (static),
//         get_foreign_key_join_selectivity (static),
//         set_subquery_size_estimates, set_function_size_estimates,
//         set_tablefunc_size_estimates, set_values_size_estimates,
//         set_cte_size_estimates, set_namedtuplestore_size_estimates,
//         set_result_size_estimates, set_foreign_size_estimates,
//         set_rel_width (static), set_pathtarget_cost_width,
//         get_expr_width (static), relation_byte_size (static),
//         page_size (static), get_parallel_divisor (static),
//         compute_bitmap_pages, compute_gather_rows
// ===========================================================================

/*
 * cost_subplan
 *   Figure the costs for a SubPlan (or initplan).
 */
pub unsafe fn cost_subplan(
    root: *mut PlannerInfo,
    subplan: *mut SubPlan,
    plan: *mut Plan,
) {
    let mut sp_cost = QualCost { startup: 0.0, per_tuple: 0.0 };

    cost_qual_eval(
        &mut sp_cost,
        make_ands_implicit((*subplan).testexpr as *mut Expr),
        core::ptr::null_mut(),
    );

    if (*subplan).useHashTable {
        sp_cost.startup += (*plan).total_cost + cpu_operator_cost * (*plan).plan_rows;
    } else {
        let plan_run_cost = (*plan).total_cost - (*plan).startup_cost;

        if (*subplan).subLinkType == crate::nodes::primnodes::SubLinkType::EXISTS_SUBLINK {
            /* we only need to fetch 1 tuple; clamp to avoid zero divide */
            sp_cost.per_tuple +=
                plan_run_cost / clamp_row_est((*plan).plan_rows);
        } else if (*subplan).subLinkType == crate::nodes::primnodes::SubLinkType::ALL_SUBLINK
            || (*subplan).subLinkType == crate::nodes::primnodes::SubLinkType::ANY_SUBLINK
        {
            /* assume we need 50% of the tuples */
            sp_cost.per_tuple += 0.50 * plan_run_cost;
            sp_cost.per_tuple += 0.50 * (*plan).plan_rows * cpu_operator_cost;
        } else {
            /* assume we need all tuples */
            sp_cost.per_tuple += plan_run_cost;
        }

        if (*subplan).parParam.is_null()
            && ExecMaterializesOutput(nodeTag(plan as *mut Node))
        {
            sp_cost.startup += (*plan).startup_cost;
        } else {
            sp_cost.per_tuple += (*plan).startup_cost;
        }
    }

    (*subplan).startup_cost = sp_cost.startup;
    (*subplan).per_call_cost = sp_cost.per_tuple;
}


/*
 * cost_rescan
 *   Given a finished Path, estimate the costs of rescanning it after
 *   having done so the first time.
 */
unsafe fn cost_rescan(
    root: *mut PlannerInfo,
    path: *mut Path,
    rescan_startup_cost: *mut Cost,
    rescan_total_cost: *mut Cost,
) {
    use crate::nodes::nodes::NodeTag::*;
    match (*path).pathtype {
        T_FunctionScan => {
            /*
             * nodeFunctionscan.c always executes the function to completion
             * before returning any rows, and caches the results in a tuplestore.
             */
            *rescan_startup_cost = 0.0;
            *rescan_total_cost = (*path).total_cost - (*path).startup_cost;
        }
        T_HashJoin => {
            if (*(path as *mut HashPath)).num_batches == 1 {
                /* Startup cost is exactly the cost of hash table building */
                *rescan_startup_cost = 0.0;
                *rescan_total_cost = (*path).total_cost - (*path).startup_cost;
            } else {
                *rescan_startup_cost = (*path).startup_cost;
                *rescan_total_cost = (*path).total_cost;
            }
        }
        T_CteScan | T_WorkTableScan => {
            let run_cost = cpu_tuple_cost * (*path).rows;
            let nbytes =
                relation_byte_size((*path).rows, (*(*path).pathtarget).width);
            let work_mem_bytes = work_mem as f64 * 1024.0;
            let run_cost = if nbytes > work_mem_bytes {
                let npages = (nbytes / BLCKSZ as f64).ceil();
                run_cost + seq_page_cost * npages
            } else {
                run_cost
            };
            *rescan_startup_cost = 0.0;
            *rescan_total_cost = run_cost;
        }
        T_Material | T_Sort => {
            let run_cost = cpu_operator_cost * (*path).rows;
            let nbytes =
                relation_byte_size((*path).rows, (*(*path).pathtarget).width);
            let work_mem_bytes = work_mem as f64 * 1024.0;
            let run_cost = if nbytes > work_mem_bytes {
                let npages = (nbytes / BLCKSZ as f64).ceil();
                run_cost + seq_page_cost * npages
            } else {
                run_cost
            };
            *rescan_startup_cost = 0.0;
            *rescan_total_cost = run_cost;
        }
        T_Memoize => {
            /* All the hard work is done by cost_memoize_rescan */
            cost_memoize_rescan(
                root,
                path as *mut MemoizePath,
                rescan_startup_cost,
                rescan_total_cost,
            );
        }
        _ => {
            *rescan_startup_cost = (*path).startup_cost;
            *rescan_total_cost = (*path).total_cost;
        }
    }
}


/*
 * cost_qual_eval
 *   Estimate the CPU costs of evaluating a WHERE clause.
 */
pub unsafe fn cost_qual_eval(
    cost: *mut QualCost,
    quals: *mut List,
    root: *mut PlannerInfo,
) {
    let mut context = cost_qual_eval_context {
        root,
        total: QualCost { startup: 0.0, per_tuple: 0.0 },
    };

    /* We don't charge any cost for the implicit ANDing at top level ... */
    let mut l: *mut ListCell = if !quals.is_null() { list_head(quals) } else { core::ptr::null_mut() };
    while !l.is_null() {
        let qual = lfirst(l) as *mut Node;
        cost_qual_eval_walker(qual, &mut context);
        l = lnext(quals, l);
    }

    *cost = context.total;
}

/*
 * cost_qual_eval_node
 *   As above, for a single RestrictInfo or expression.
 */
pub unsafe fn cost_qual_eval_node(
    cost: *mut QualCost,
    qual: *mut Node,
    root: *mut PlannerInfo,
) {
    let mut context = cost_qual_eval_context {
        root,
        total: QualCost { startup: 0.0, per_tuple: 0.0 },
    };

    cost_qual_eval_walker(qual, &mut context);

    *cost = context.total;
}

unsafe fn cost_qual_eval_walker(
    node: *mut Node,
    context: *mut cost_qual_eval_context,
) -> bool {
    if node.is_null() {
        return false;
    }

    /*
     * RestrictInfo nodes contain an eval_cost field reserved for this
     * routine's use.
     */
    if IsA!(node, T_RestrictInfo) {
        let rinfo = node as *mut RestrictInfo;

        if (*rinfo).eval_cost.startup < 0.0 {
            let mut locContext = cost_qual_eval_context {
                root: (*context).root,
                total: QualCost { startup: 0.0, per_tuple: 0.0 },
            };

            if !(*rinfo).orclause.is_null() {
                cost_qual_eval_walker((*rinfo).orclause as *mut Node, &mut locContext);
            } else {
                cost_qual_eval_walker((*rinfo).clause as *mut Node, &mut locContext);
            }

            if (*rinfo).pseudoconstant {
                locContext.total.startup += locContext.total.per_tuple;
                locContext.total.per_tuple = 0.0;
            }
            (*rinfo).eval_cost = locContext.total;
        }
        (*context).total.startup += (*rinfo).eval_cost.startup;
        (*context).total.per_tuple += (*rinfo).eval_cost.per_tuple;
        /* do NOT recurse into children */
        return false;
    }

    if IsA!(node, T_FuncExpr) {
        add_function_cost(
            (*context).root,
            (*(node as *mut FuncExpr)).funcid,
            node,
            &mut (*context).total,
        );
    } else if IsA!(node, T_OpExpr)
        || IsA!(node, T_DistinctExpr)
        || IsA!(node, T_NullIfExpr)
    {
        /* rely on struct equivalence to treat these all alike */
        set_opfuncid(node as *mut OpExpr);
        add_function_cost(
            (*context).root,
            (*(node as *mut OpExpr)).opfuncid,
            node,
            &mut (*context).total,
        );
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        let saop = node as *mut ScalarArrayOpExpr;
        let arraynode = lsecond((*saop).args) as *mut Node;
        let mut sacosts = QualCost { startup: 0.0, per_tuple: 0.0 };
        let mut hcosts = QualCost { startup: 0.0, per_tuple: 0.0 };
        let estarraylen =
            estimate_array_length((*context).root, arraynode);

        set_sa_opfuncid(saop);
        add_function_cost((*context).root, (*saop).opfuncid, core::ptr::null_mut(), &mut sacosts);

        if OidIsValid((*saop).hashfuncid) {
            /* Handle costs for hashed ScalarArrayOpExpr */
            add_function_cost((*context).root, (*saop).hashfuncid, core::ptr::null_mut(), &mut hcosts);
            (*context).total.startup += sacosts.startup + hcosts.startup;
            /* Estimate the cost of building the hashtable. */
            (*context).total.startup += estarraylen * hcosts.per_tuple;
            /* Charge for hashtable lookups. */
            (*context).total.per_tuple += hcosts.per_tuple + sacosts.per_tuple;
        } else {
            (*context).total.startup += sacosts.startup;
            (*context).total.per_tuple +=
                sacosts.per_tuple * estarraylen * 0.5;
        }
    } else if IsA!(node, T_Aggref) || IsA!(node, T_WindowFunc) {
        /*
         * Aggref and WindowFunc nodes are treated like Vars, ie, zero
         * execution cost in the current model.
         */
        return false; /* don't recurse into children */
    } else if IsA!(node, T_GroupingFunc) {
        /* Treat this as having cost 1 */
        (*context).total.per_tuple += cpu_operator_cost;
        return false; /* don't recurse into children */
    } else if IsA!(node, T_CoerceViaIO) {
        let iocoerce = node as *mut CoerceViaIO;
        let mut iofunc: Oid = 0;
        let mut typioparam: Oid = 0;
        let mut typisvarlena: bool = false;

        /* check the result type's input function */
        getTypeInputInfo((*iocoerce).resulttype, &mut iofunc, &mut typioparam);
        add_function_cost((*context).root, iofunc, core::ptr::null_mut(), &mut (*context).total);
        /* check the input type's output function */
        getTypeOutputInfo(
            exprType((*iocoerce).arg as *mut Node),
            &mut iofunc,
            &mut typisvarlena,
        );
        add_function_cost((*context).root, iofunc, core::ptr::null_mut(), &mut (*context).total);
    } else if IsA!(node, T_ArrayCoerceExpr) {
        let acoerce = node as *mut ArrayCoerceExpr;
        let mut perelemcost = QualCost { startup: 0.0, per_tuple: 0.0 };

        cost_qual_eval_node(&mut perelemcost, (*acoerce).elemexpr as *mut Node, (*context).root);
        (*context).total.startup += perelemcost.startup;
        if perelemcost.per_tuple > 0.0 {
            (*context).total.per_tuple += perelemcost.per_tuple
                * estimate_array_length((*context).root, (*acoerce).arg as *mut Node);
        }
    } else if IsA!(node, T_RowCompareExpr) {
        /* Conservatively assume we will check all the columns */
        let rcexpr = node as *mut RowCompareExpr;
        let mut lc: *mut ListCell =
            if !(*rcexpr).opnos.is_null() { list_head((*rcexpr).opnos) } else { core::ptr::null_mut() };
        while !lc.is_null() {
            let opid = lfirst_oid(lc);
            add_function_cost(
                (*context).root,
                get_opcode(opid),
                core::ptr::null_mut(),
                &mut (*context).total,
            );
            lc = lnext((*rcexpr).opnos, lc);
        }
    } else if IsA!(node, T_MinMaxExpr)
        || IsA!(node, T_SQLValueFunction)
        || IsA!(node, T_XmlExpr)
        || IsA!(node, T_CoerceToDomain)
        || IsA!(node, T_NextValueExpr)
        || IsA!(node, T_JsonExpr)
    {
        /* Treat all these as having cost 1 */
        (*context).total.per_tuple += cpu_operator_cost;
    } else if IsA!(node, T_SubLink) {
        /* This routine should not be applied to un-planned expressions */
        elog!(crate::utils::elog::ERROR, "cannot handle unplanned sub-select");
    } else if IsA!(node, T_SubPlan) {
        let subplan = node as *mut SubPlan;
        (*context).total.startup += (*subplan).startup_cost;
        (*context).total.per_tuple += (*subplan).per_call_cost;
        return false;
    } else if IsA!(node, T_AlternativeSubPlan) {
        let asplan = node as *mut AlternativeSubPlan;
        return cost_qual_eval_walker(
            linitial((*asplan).subplans) as *mut Node,
            context,
        );
    } else if IsA!(node, T_PlaceHolderVar) {
        /*
         * A PlaceHolderVar should be given cost zero when considering general
         * expression evaluation costs.
         */
        return false;
    }

    /* recurse into children */
    expression_tree_walker(node, core::mem::transmute::<unsafe fn(*mut Node, *mut cost_qual_eval_context) -> bool, unsafe fn(*mut Node, *mut c_void) -> bool>(cost_qual_eval_walker), context as *mut c_void)
}

/*
 * get_restriction_qual_cost
 *   Compute evaluation costs of a baserel's restriction quals, plus any
 *   movable join quals that have been pushed down to the scan.
 */
unsafe fn get_restriction_qual_cost(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    param_info: *mut ParamPathInfo,
    qpqual_cost: *mut QualCost,
) {
    if !param_info.is_null() {
        /* Include costs of pushed-down clauses */
        cost_qual_eval(qpqual_cost, (*param_info).ppi_clauses, root);

        (*qpqual_cost).startup += (*baserel).baserestrictcost.startup;
        (*qpqual_cost).per_tuple += (*baserel).baserestrictcost.per_tuple;
    } else {
        *qpqual_cost = (*baserel).baserestrictcost;
    }
}


/*
 * compute_semi_anti_join_factors
 *   Estimate how much of the inner input a SEMI, ANTI, or inner_unique join
 *   can be expected to scan.
 */
pub unsafe fn compute_semi_anti_join_factors(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
    semifactors: *mut SemiAntiJoinFactors,
) {
    let jselec: Selectivity;
    let nselec: Selectivity;
    let avgmatch: Selectivity;
    let mut norm_sjinfo: SpecialJoinInfo = core::mem::zeroed();
    let joinquals: *mut List;
    let mut l: *mut ListCell;

    if IS_OUTER_JOIN(jointype) {
        let mut jq: *mut List = NIL;
        l = if !restrictlist.is_null() { list_head(restrictlist) } else { core::ptr::null_mut() };
        while !l.is_null() {
            let rinfo = lfirst(l) as *mut RestrictInfo;
            if !RINFO_IS_PUSHED_DOWN(rinfo, (*joinrel).relids) {
                jq = lappend(jq, rinfo as *mut c_void);
            }
            l = lnext(restrictlist, l);
        }
        joinquals = jq;
    } else {
        joinquals = restrictlist;
    }

    use crate::nodes::nodes::JoinType::*;
    jselec = clauselist_selectivity(
        root,
        joinquals,
        0,
        if jointype == JOIN_ANTI { JOIN_ANTI } else { JOIN_SEMI },
        sjinfo,
    );

    init_dummy_sjinfo(&mut norm_sjinfo, (*outerrel).relids, (*innerrel).relids);

    nselec = clauselist_selectivity(
        root,
        joinquals,
        0,
        JOIN_INNER,
        &mut norm_sjinfo as *mut SpecialJoinInfo,
    );

    /* Avoid leaking a lot of ListCells */
    if IS_OUTER_JOIN(jointype) {
        list_free(joinquals);
    }

    let avgmatch = if jselec > 0.0 {
        let a = nselec * (*innerrel).rows / jselec;
        if a > 1.0 { a } else { 1.0 }
    } else {
        1.0
    };

    (*semifactors).outer_match_frac = jselec;
    (*semifactors).match_count = avgmatch;
}

/*
 * has_indexed_join_quals
 *   Check whether all the joinquals of a nestloop join are used as
 *   inner index quals.
 */
unsafe fn has_indexed_join_quals(path: *mut NestPath) -> bool {
    let joinpath: *mut JoinPath = &mut (*path).jpath as *mut JoinPath;
    let joinrelids: Relids = (*(*joinpath).path.parent).relids;
    let innerpath: *mut Path = (*joinpath).innerjoinpath;
    let indexclauses: *mut List;
    let mut found_one = false;
    let mut lc: *mut ListCell;

    /* If join still has quals to evaluate, it's not fast */
    if (*joinpath).joinrestrictinfo != NIL {
        return false;
    }
    /* Nor if the inner path isn't parameterized at all */
    if (*innerpath).param_info.is_null() {
        return false;
    }

    /* Find the indexclauses list for the inner scan */
    use crate::nodes::nodes::NodeTag::*;
    indexclauses = match (*innerpath).pathtype {
        T_IndexScan | T_IndexOnlyScan => {
            (*(innerpath as *mut IndexPath)).indexclauses
        }
        T_BitmapHeapScan => {
            let bmqual: *mut Path = (*(innerpath as *mut BitmapHeapPath)).bitmapqual;
            if IsA!(bmqual as *mut Node, T_IndexPath) {
                (*(bmqual as *mut IndexPath)).indexclauses
            } else {
                return false;
            }
        }
        _ => {
            return false;
        }
    };

    let ppi_clauses = (*(*innerpath).param_info).ppi_clauses;
    lc = if !ppi_clauses.is_null() {
        list_head(ppi_clauses)
    } else {
        core::ptr::null_mut()
    };
    while !lc.is_null() {
        let rinfo = lfirst(lc) as *mut RestrictInfo;
        if join_clause_is_movable_into(
            rinfo,
            (*(*innerpath).parent).relids,
            joinrelids,
        ) {
            if !is_redundant_with_indexclauses(rinfo, indexclauses) {
                return false;
            }
            found_one = true;
        }
        lc = lnext(ppi_clauses, lc);
    }
    found_one
}


/*
 * approx_tuple_count
 *   Quick-and-dirty estimation of the number of join rows passing
 *   a set of qual conditions.
 */
unsafe fn approx_tuple_count(
    root: *mut PlannerInfo,
    path: *mut JoinPath,
    quals: *mut List,
) -> f64 {
    let outer_tuples = (*(*path).outerjoinpath).rows;
    let inner_tuples = (*(*path).innerjoinpath).rows;
    let mut sjinfo: SpecialJoinInfo = core::mem::zeroed();
    let mut selec: Selectivity = 1.0;
    let mut l: *mut ListCell;

    /* Make up a SpecialJoinInfo for JOIN_INNER semantics. */
    init_dummy_sjinfo(
        &mut sjinfo,
        (*(*(*path).outerjoinpath).parent).relids,
        (*(*(*path).innerjoinpath).parent).relids,
    );

    /* Get the approximate selectivity */
    l = if !quals.is_null() { list_head(quals) } else { core::ptr::null_mut() };
    while !l.is_null() {
        let qual = lfirst(l) as *mut Node;
        /* Note that clause_selectivity will be able to cache its result */
        selec *= clause_selectivity(root, qual, 0, JOIN_INNER, &mut sjinfo as *mut SpecialJoinInfo);
        l = lnext(quals, l);
    }

    /* Apply it to the input relation sizes */
    clamp_row_est(selec * outer_tuples * inner_tuples)
}


/*
 * set_baserel_size_estimates
 *   Set the size estimates for the given base relation.
 */
pub unsafe fn set_baserel_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    /* Should only be applied to base relations */
    Assert!((*rel).relid > 0);

    let nrows = (*rel).tuples
        * clauselist_selectivity(
            root,
            (*rel).baserestrictinfo,
            0,
            JOIN_INNER,
            core::ptr::null_mut(),
        );

    (*rel).rows = clamp_row_est(nrows);

    cost_qual_eval(&mut (*rel).baserestrictcost, (*rel).baserestrictinfo, root);

    set_rel_width(root, rel);
}

/*
 * get_parameterized_baserel_size
 *   Make a size estimate for a parameterized scan of a base relation.
 */
pub unsafe fn get_parameterized_baserel_size(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    param_clauses: *mut List,
) -> f64 {
    let allclauses = list_concat_copy(param_clauses, (*rel).baserestrictinfo);
    let mut nrows = (*rel).tuples
        * clauselist_selectivity(
            root,
            allclauses,
            (*rel).relid as c_int, /* do not use 0! */
            JOIN_INNER,
            core::ptr::null_mut(),
        );
    nrows = clamp_row_est(nrows);
    /* For safety, make sure result is not more than the base estimate */
    if nrows > (*rel).rows {
        nrows = (*rel).rows;
    }
    nrows
}

/*
 * set_joinrel_size_estimates
 *   Set the size estimates for the given join relation.
 */
pub unsafe fn set_joinrel_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
) {
    (*rel).rows = calc_joinrel_size_estimate(
        root,
        rel,
        outer_rel,
        inner_rel,
        (*outer_rel).rows,
        (*inner_rel).rows,
        sjinfo,
        restrictlist,
    );
}

/*
 * get_parameterized_joinrel_size
 *   Make a size estimate for a parameterized scan of a join relation.
 */
pub unsafe fn get_parameterized_joinrel_size(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    sjinfo: *mut SpecialJoinInfo,
    restrict_clauses: *mut List,
) -> f64 {
    let mut nrows = calc_joinrel_size_estimate(
        root,
        rel,
        (*outer_path).parent,
        (*inner_path).parent,
        (*outer_path).rows,
        (*inner_path).rows,
        sjinfo,
        restrict_clauses,
    );
    /* For safety, make sure result is not more than the base estimate */
    if nrows > (*rel).rows {
        nrows = (*rel).rows;
    }
    nrows
}

/*
 * calc_joinrel_size_estimate
 *   Workhorse for set_joinrel_size_estimates and get_parameterized_joinrel_size.
 */
unsafe fn calc_joinrel_size_estimate(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    outer_rows: f64,
    inner_rows: f64,
    sjinfo: *mut SpecialJoinInfo,
    mut restrictlist: *mut List,
) -> f64 {
    let jointype: JoinType = (*sjinfo).jointype;
    let fkselec: Selectivity;
    let jselec: Selectivity;
    let pselec: Selectivity;
    let nrows: f64;

    fkselec = get_foreign_key_join_selectivity(
        root,
        (*outer_rel).relids,
        (*inner_rel).relids,
        sjinfo,
        &mut restrictlist,
    );

    use crate::nodes::nodes::JoinType::*;
    if IS_OUTER_JOIN(jointype) {
        let mut joinquals: *mut List = NIL;
        let mut pushedquals: *mut List = NIL;
        let mut l = if !restrictlist.is_null() { list_head(restrictlist) } else { core::ptr::null_mut() };

        while !l.is_null() {
            let rinfo = lfirst(l) as *mut RestrictInfo;
            if RINFO_IS_PUSHED_DOWN(rinfo, (*joinrel).relids) {
                pushedquals = lappend(pushedquals, rinfo as *mut c_void);
            } else {
                joinquals = lappend(joinquals, rinfo as *mut c_void);
            }
            l = lnext(restrictlist, l);
        }

        jselec = clauselist_selectivity(root, joinquals, 0, jointype, sjinfo);
        pselec = clauselist_selectivity(root, pushedquals, 0, jointype, sjinfo);

        list_free(joinquals);
        list_free(pushedquals);

        let nrows = match jointype {
            JOIN_LEFT => {
                let mut n = outer_rows * inner_rows * fkselec * jselec;
                if n < outer_rows { n = outer_rows; }
                n * pselec
            }
            JOIN_FULL => {
                let mut n = outer_rows * inner_rows * fkselec * jselec;
                if n < outer_rows { n = outer_rows; }
                if n < inner_rows { n = inner_rows; }
                n * pselec
            }
            JOIN_ANTI => {
                outer_rows * (1.0 - fkselec * jselec) * pselec
            }
            _ => {
                elog!(crate::utils::elog::ERROR, "unrecognized join type: {}", jointype as c_int);
                0.0
            }
        };
        return clamp_row_est(nrows);
    } else {
        jselec = clauselist_selectivity(root, restrictlist, 0, jointype, sjinfo);
        let pselec = 0.0f64; /* not used for inner join */

        let nrows = match jointype {
            JOIN_INNER => outer_rows * inner_rows * fkselec * jselec,
            JOIN_SEMI => outer_rows * fkselec * jselec,
            _ => {
                elog!(crate::utils::elog::ERROR, "unrecognized join type: {}", jointype as c_int);
                0.0
            }
        };
        return clamp_row_est(nrows);
    }
}

/*
 * get_foreign_key_join_selectivity
 *   Estimate join selectivity for foreign-key-related clauses.
 */
unsafe fn get_foreign_key_join_selectivity(
    root: *mut PlannerInfo,
    outer_relids: Relids,
    inner_relids: Relids,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut *mut List,
) -> Selectivity {
    let mut fkselec: Selectivity = 1.0;
    let jointype: JoinType = (*sjinfo).jointype;
    let mut worklist: *mut List = *restrictlist;
    let mut lc: *mut ListCell;

    use crate::nodes::nodes::JoinType::*;

    let fkey_list = (*root).fkey_list;
    lc = if !fkey_list.is_null() { list_head(fkey_list) } else { core::ptr::null_mut() };
    while !lc.is_null() {
        let fkinfo = lfirst(lc) as *mut ForeignKeyOptInfo;
        let ref_is_outer: bool;
        let mut removedlist: *mut List = NIL;

        if bms_is_member((*fkinfo).con_relid as c_int, outer_relids)
            && bms_is_member((*fkinfo).ref_relid as c_int, inner_relids)
        {
            ref_is_outer = false;
        } else if bms_is_member((*fkinfo).ref_relid as c_int, outer_relids)
            && bms_is_member((*fkinfo).con_relid as c_int, inner_relids)
        {
            ref_is_outer = true;
        } else {
            lc = lnext(fkey_list, lc);
            continue;
        }

        if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
            && (ref_is_outer || bms_membership(inner_relids) != crate::nodes::bitmapset::BMS_SINGLETON)
        {
            lc = lnext(fkey_list, lc);
            continue;
        }

        if worklist == *restrictlist {
            worklist = list_copy(worklist);
        }

        let mut cell = if !worklist.is_null() { list_head(worklist) } else { core::ptr::null_mut() };
        while !cell.is_null() {
            let rinfo = lfirst(cell) as *mut RestrictInfo;
            let mut remove_it = false;

            for i in 0..(*fkinfo).nkeys as usize {
                if !(*rinfo).parent_ec.is_null() {
                    if (*fkinfo).eclass[i] == (*rinfo).parent_ec {
                        remove_it = true;
                        break;
                    }
                } else {
                    if list_member_ptr((*fkinfo).rinfos[i], rinfo as *mut c_void) {
                        remove_it = true;
                        break;
                    }
                }
            }
            if remove_it {
                let cell_idx = crate::nodes::pg_list::list_cell_number(worklist, cell);
                worklist = list_delete_nth_cell(worklist, cell_idx);
                removedlist = lappend(removedlist, rinfo as *mut c_void);
                cell = if !worklist.is_null() { list_head(worklist) } else { core::ptr::null_mut() };
                continue;
            }
            cell = lnext(worklist, cell);
        }

        if removedlist.is_null()
            || list_length(removedlist)
                != ((*fkinfo).nmatched_ec - (*fkinfo).nconst_ec + (*fkinfo).nmatched_ri) as c_int
        {
            worklist = list_concat(worklist, removedlist);
            lc = lnext(fkey_list, lc);
            continue;
        }

        if jointype == JOIN_SEMI || jointype == JOIN_ANTI {
            let ref_rel = find_base_rel(root, (*fkinfo).ref_relid as c_int);
            let ref_tuples = if (*ref_rel).tuples > 1.0 { (*ref_rel).tuples } else { 1.0 };
            fkselec *= (*ref_rel).rows / ref_tuples;
        } else {
            let ref_rel = find_base_rel(root, (*fkinfo).ref_relid as c_int);
            let ref_tuples = if (*ref_rel).tuples > 1.0 { (*ref_rel).tuples } else { 1.0 };
            fkselec *= 1.0 / ref_tuples;
        }

        if (*fkinfo).nconst_ec > 0 {
            for i in 0..(*fkinfo).nkeys as usize {
                let ec = (*fkinfo).eclass[i];
                if !ec.is_null() && (*ec).ec_has_const {
                    let em = (*fkinfo).fk_eclass_member[i];
                    let rinfo = find_derived_clause_for_ec_member(root, ec, em);
                    if !rinfo.is_null() {
                        let s0 = clause_selectivity(
                            root,
                            rinfo as *mut Node,
                            0,
                            jointype,
                            sjinfo,
                        );
                        if s0 > 0.0 {
                            fkselec /= s0;
                        }
                    }
                }
            }
        }

        lc = lnext(fkey_list, lc);
    }

    *restrictlist = worklist;
    CLAMP_PROBABILITY(&mut fkselec);
    fkselec
}

/*
 * set_subquery_size_estimates
 *   Set the size estimates for a base relation that is a subquery.
 */
pub unsafe fn set_subquery_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    let subroot: *mut PlannerInfo = (*rel).subroot;
    let mut lc: *mut ListCell;

    Assert!((*rel).relid > 0);
    Assert!(
        (*planner_rt_fetch((*rel).relid, root)).rtekind
            == crate::nodes::parsenodes::RTEKind::RTE_SUBQUERY,
    );

    let sub_final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL as c_int, core::ptr::null_mut());
    (*rel).tuples = (*(*sub_final_rel).cheapest_total_path).rows;

    let target_list = (*(*subroot).parse).targetList;
    lc = if !target_list.is_null() {
        list_head(target_list)
    } else {
        core::ptr::null_mut()
    };
    while !lc.is_null() {
        let te = lfirst(lc) as *mut TargetEntry;
        let texpr = (*te).expr as *mut Node;
        let mut item_width: i32 = 0;

        /* junk columns aren't visible to upper query */
        if (*te).resjunk {
            lc = lnext(target_list, lc);
            continue;
        }

        if (*te).resno < (*rel).min_attr as i16 || (*te).resno > (*rel).max_attr as i16 {
            lc = lnext(target_list, lc);
            continue;
        }

        if IsA!(texpr, T_Var) && (*(*subroot).parse).setOperations.is_null() {
            let var = texpr as *mut Var;
            let subrel = find_base_rel(subroot, (*var).varno as c_int);
            item_width =
                *(*subrel).attr_widths.add(((*var).varattno as i32 - (*subrel).min_attr as i32) as usize);
        }
        *(*rel).attr_widths
            .add(((*te).resno as i32 - (*rel).min_attr as i32) as usize) = item_width;
        lc = lnext(target_list, lc);
    }

    /* Now estimate number of output rows, etc */
    set_baserel_size_estimates(root, rel);
}

/*
 * set_function_size_estimates
 *   Set the size estimates for a base relation that is a function call.
 */
pub unsafe fn set_function_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);
    let rte = planner_rt_fetch((*rel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_FUNCTION);

    (*rel).tuples = 0.0;
    let functions = (*rte).functions;
    let mut lc: *mut ListCell = if !functions.is_null() { list_head(functions) } else { core::ptr::null_mut() };
    while !lc.is_null() {
        let rtfunc = lfirst(lc) as *mut RangeTblFunction;
        let ntup = expression_returns_set_rows(root, (*rtfunc).funcexpr as *mut Node);
        if ntup > (*rel).tuples {
            (*rel).tuples = ntup;
        }
        lc = lnext(functions, lc);
    }

    set_baserel_size_estimates(root, rel);
}

/*
 * set_tablefunc_size_estimates
 *   Set the size estimates for a base relation that is a table function.
 */
pub unsafe fn set_tablefunc_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);
    Assert!(
        (*planner_rt_fetch((*rel).relid, root)).rtekind
            == crate::nodes::parsenodes::RTEKind::RTE_TABLEFUNC,
    );

    (*rel).tuples = 100.0;

    set_baserel_size_estimates(root, rel);
}

/*
 * set_values_size_estimates
 *   Set the size estimates for a base relation that is a values list.
 */
pub unsafe fn set_values_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);
    let rte = planner_rt_fetch((*rel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_VALUES);

    (*rel).tuples = list_length((*rte).values_lists) as f64;

    set_baserel_size_estimates(root, rel);
}

/*
 * set_cte_size_estimates
 *   Set the size estimates for a base relation that is a CTE reference.
 */
pub unsafe fn set_cte_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    cte_rows: f64,
) {
    Assert!((*rel).relid > 0);
    let rte = planner_rt_fetch((*rel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_CTE);

    if (*rte).self_reference {
        (*rel).tuples = clamp_row_est(recursive_worktable_factor * cte_rows);
    } else {
        (*rel).tuples = cte_rows;
    }

    set_baserel_size_estimates(root, rel);
}

/*
 * set_namedtuplestore_size_estimates
 *   Set the size estimates for a base relation that is a tuplestore reference.
 */
pub unsafe fn set_namedtuplestore_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);
    let rte = planner_rt_fetch((*rel).relid, root);
    Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_NAMEDTUPLESTORE);

    (*rel).tuples = (*rte).enrtuples;
    if (*rel).tuples < 0.0 {
        (*rel).tuples = 1000.0;
    }

    set_baserel_size_estimates(root, rel);
}

/*
 * set_result_size_estimates
 *   Set the size estimates for an RTE_RESULT base relation.
 */
pub unsafe fn set_result_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);
    Assert!(
        (*planner_rt_fetch((*rel).relid, root)).rtekind
            == crate::nodes::parsenodes::RTEKind::RTE_RESULT,
    );

    /* RTE_RESULT always generates a single row, natively */
    (*rel).tuples = 1.0;

    set_baserel_size_estimates(root, rel);
}

/*
 * set_foreign_size_estimates
 *   Set the size estimates for a base relation that is a foreign table.
 */
pub unsafe fn set_foreign_size_estimates(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    Assert!((*rel).relid > 0);

    (*rel).rows = 1000.0; /* entirely bogus default estimate */

    cost_qual_eval(&mut (*rel).baserestrictcost, (*rel).baserestrictinfo, root);

    set_rel_width(root, rel);
}


/*
 * set_rel_width
 *   Set the estimated output width of a base relation.
 */
unsafe fn set_rel_width(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) {
    let reloid: Oid = (*planner_rt_fetch((*rel).relid, root)).relid;
    let mut tuple_width: i64 = 0;
    let mut have_wholerow_var = false;
    let mut lc: *mut ListCell;

    /* Vars are assumed to have cost zero, but other exprs do not */
    (*(*rel).reltarget).cost.startup = 0.0;
    (*(*rel).reltarget).cost.per_tuple = 0.0;

    let reltarget_exprs = (*(*rel).reltarget).exprs;
    lc = if !reltarget_exprs.is_null() {
        list_head(reltarget_exprs)
    } else {
        core::ptr::null_mut()
    };
    while !lc.is_null() {
        let node = lfirst(lc) as *mut Node;

        if IsA!(node, T_Var) && (*(node as *mut Var)).varno == (*rel).relid as c_int {
            let var = node as *mut Var;
            let ndx: usize =
                ((*var).varattno as i32 - (*rel).min_attr as i32) as usize;

            if (*var).varattno == 0 {
                have_wholerow_var = true;
                lc = lnext(reltarget_exprs, lc);
                continue;
            }

            if *(*rel).attr_widths.add(ndx) > 0 {
                tuple_width += *(*rel).attr_widths.add(ndx) as i64;
                lc = lnext(reltarget_exprs, lc);
                continue;
            }

            /* Try to get column width from statistics */
            if reloid != InvalidOid && (*var).varattno > 0 {
                let item_width = get_attavgwidth(reloid, (*var).varattno);
                if item_width > 0 {
                    *(*rel).attr_widths.add(ndx) = item_width;
                    tuple_width += item_width as i64;
                    lc = lnext(reltarget_exprs, lc);
                    continue;
                }
            }

            /* Not a plain relation or no statistics */
            let item_width = get_typavgwidth((*var).vartype, (*var).vartypmod);
            Assert!(item_width > 0);
            *(*rel).attr_widths.add(ndx) = item_width;
            tuple_width += item_width as i64;
        } else if IsA!(node, T_PlaceHolderVar) {
            let phv = node as *mut PlaceHolderVar;
            let phinfo = find_placeholder_info(root, phv);
            let mut cost = QualCost { startup: 0.0, per_tuple: 0.0 };

            tuple_width += (*phinfo).ph_width as i64;
            cost_qual_eval_node(&mut cost, (*phv).phexpr as *mut Node, root);
            (*(*rel).reltarget).cost.startup += cost.startup;
            (*(*rel).reltarget).cost.per_tuple += cost.per_tuple;
        } else {
            let item_width = get_typavgwidth(exprType(node), exprTypmod(node));
            Assert!(item_width > 0);
            tuple_width += item_width as i64;
            let mut cost = QualCost { startup: 0.0, per_tuple: 0.0 };
            cost_qual_eval_node(&mut cost, node, root);
            (*(*rel).reltarget).cost.startup += cost.startup;
            (*(*rel).reltarget).cost.per_tuple += cost.per_tuple;
        }
        lc = lnext(reltarget_exprs, lc);
    }

    if have_wholerow_var {
        let mut wholerow_width: i64 = MAXALIGN(SizeofHeapTupleHeader as usize) as i64;

        if reloid != InvalidOid {
            /* Real relation, so estimate true tuple width */
            wholerow_width += get_relation_data_width(
                reloid,
                ((*rel).attr_widths as *mut i32).offset(-((*rel).min_attr as isize)),
            ) as i64;
        } else {
            /* Do what we can with info for a phony rel */
            let mut i: i16 = 1;
            while i <= (*rel).max_attr {
                wholerow_width +=
                    *(*rel).attr_widths.add((i as i32 - (*rel).min_attr as i32) as usize) as i64;
                i += 1;
            }
        }

        *(*rel).attr_widths
            .add((0i32 - (*rel).min_attr as i32) as usize) =
            clamp_width_est(wholerow_width);

        tuple_width += wholerow_width;
    }

    (*(*rel).reltarget).width = clamp_width_est(tuple_width);
}

/*
 * set_pathtarget_cost_width
 *   Set the estimated eval cost and output width of a PathTarget tlist.
 */
pub unsafe fn set_pathtarget_cost_width(
    root: *mut PlannerInfo,
    target: *mut PathTarget,
) -> *mut PathTarget {
    let mut tuple_width: i64 = 0;
    let mut lc: *mut ListCell;

    /* Vars are assumed to have cost zero, but other exprs do not */
    (*target).cost.startup = 0.0;
    (*target).cost.per_tuple = 0.0;

    let target_exprs = (*target).exprs;
    lc = if !target_exprs.is_null() { list_head(target_exprs) } else { core::ptr::null_mut() };
    while !lc.is_null() {
        let node = lfirst(lc) as *mut Node;

        tuple_width += get_expr_width(root, node as *const Node) as i64;

        /* For non-Vars, account for evaluation cost */
        if !IsA!(node, T_Var) {
            let mut cost = QualCost { startup: 0.0, per_tuple: 0.0 };
            cost_qual_eval_node(&mut cost, node, root);
            (*target).cost.startup += cost.startup;
            (*target).cost.per_tuple += cost.per_tuple;
        }
        lc = lnext(target_exprs, lc);
    }

    (*target).width = clamp_width_est(tuple_width);

    target
}

/*
 * get_expr_width
 *   Estimate the width of the given expr.
 */
unsafe fn get_expr_width(root: *mut PlannerInfo, expr: *const Node) -> i32 {
    if IsA!(expr as *mut Node, T_Var) {
        let var = expr as *const Var;

        /* We should not see any upper-level Vars here */
        Assert!((*var).varlevelsup == 0);

        /* Try to get data from RelOptInfo cache */
        if !IS_SPECIAL_VARNO((*var).varno)
            && ((*var).varno as c_int) < (*root).simple_rel_array_size
        {
            let rel = *(*root).simple_rel_array.offset((*var).varno as isize);
            if !rel.is_null()
                && (*var).varattno >= (*rel).min_attr as i16
                && (*var).varattno <= (*rel).max_attr as i16
            {
                let ndx =
                    ((*var).varattno as i32 - (*rel).min_attr as i32) as usize;
                if *(*rel).attr_widths.add(ndx) > 0 {
                    return *(*rel).attr_widths.add(ndx);
                }
            }
        }

        /* No cached data available */
        let width = get_typavgwidth((*var).vartype, (*var).vartypmod);
        Assert!(width > 0);
        return width;
    }

    let width = get_typavgwidth(
        exprType(expr as *mut Node),
        exprTypmod(expr as *mut Node),
    );
    Assert!(width > 0);
    width
}

/*
 * relation_byte_size
 *   Estimate the storage space in bytes for a given number of tuples
 *   of a given width (size in bytes).
 */
unsafe fn relation_byte_size(tuples: f64, width: c_int) -> f64 {
    tuples
        * (MAXALIGN(width as usize) as f64
            + MAXALIGN(SizeofHeapTupleHeader as usize) as f64)
}

/*
 * page_size
 *   Returns an estimate of the number of pages covered by a given
 *   number of tuples of a given width (size in bytes).
 */
unsafe fn page_size(tuples: f64, width: c_int) -> f64 {
    (relation_byte_size(tuples, width) / BLCKSZ as f64).ceil()
}

/*
 * Estimate the fraction of the work that each worker will do given the
 * number of workers budgeted for the path.
 */
unsafe fn get_parallel_divisor(path: *mut Path) -> f64 {
    let mut parallel_divisor = (*path).parallel_workers as f64;

    if parallel_leader_participation {
        let leader_contribution = 1.0 - 0.3 * (*path).parallel_workers as f64;
        if leader_contribution > 0.0 {
            parallel_divisor += leader_contribution;
        }
    }

    parallel_divisor
}

/*
 * compute_bitmap_pages
 *   Estimate number of pages fetched from heap in a bitmap heap scan.
 */
pub unsafe fn compute_bitmap_pages(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    bitmapqual: *mut Path,
    loop_count: f64,
    cost_p: *mut Cost,
    tuples_p: *mut f64,
) -> f64 {
    let mut indexTotalCost: Cost = 0.0;
    let mut indexSelectivity: Selectivity = 0.0;
    let T: f64;
    let mut pages_fetched: f64;
    let mut tuples_fetched: f64;
    let heap_pages: f64;
    let maxentries: f64;

    /* Fetch total cost of obtaining the bitmap and its total selectivity. */
    cost_bitmap_tree_node(bitmapqual, &mut indexTotalCost, &mut indexSelectivity);

    /* Estimate number of main-table pages fetched. */
    tuples_fetched = clamp_row_est(indexSelectivity * (*baserel).tuples);

    T = if (*baserel).pages > 1 { (*baserel).pages as f64 } else { 1.0 };

    pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);

    heap_pages = if pages_fetched < (*baserel).pages as f64 {
        pages_fetched
    } else {
        (*baserel).pages as f64
    };
    maxentries = tbm_calculate_entries(work_mem as usize * 1024);

    if loop_count > 1.0 {
        pages_fetched = index_pages_fetched(
            tuples_fetched * loop_count,
            (*baserel).pages,
            get_indexpath_pages(bitmapqual),
            root,
        );
        pages_fetched /= loop_count;
    }

    if pages_fetched >= T {
        pages_fetched = T;
    } else {
        pages_fetched = pages_fetched.ceil();
    }

    if maxentries < heap_pages {
        let lossy_pages = if heap_pages - maxentries / 2.0 > 0.0 {
            heap_pages - maxentries / 2.0
        } else {
            0.0
        };
        let exact_pages = heap_pages - lossy_pages;

        if lossy_pages > 0.0 {
            tuples_fetched = clamp_row_est(
                indexSelectivity * (exact_pages / heap_pages) * (*baserel).tuples
                    + (lossy_pages / heap_pages) * (*baserel).tuples,
            );
        }
    }

    if !cost_p.is_null() {
        *cost_p = indexTotalCost;
    }
    if !tuples_p.is_null() {
        *tuples_p = tuples_fetched;
    }

    pages_fetched
}

/*
 * compute_gather_rows
 *   Estimate number of rows for gather (merge) nodes.
 */
pub unsafe fn compute_gather_rows(path: *mut Path) -> f64 {
    Assert!((*path).parallel_workers > 0);
    clamp_row_est((*path).rows * get_parallel_divisor(path))
}

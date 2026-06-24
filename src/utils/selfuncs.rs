//! Translated from PostgreSQL src/include/utils/selfuncs.h
//! Selectivity functions for standard operators, and assorted infrastructure for
//! selectivity and cost estimation.

use bitflags::bitflags;

use crate::access::cmptype::CompareType;
use crate::access::htup::HeapTuple;
use crate::access::attnum::AttrNumber;
use crate::c::Index;
use crate::fmgr::FmgrInfo;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pathnodes::{
    AggClauseCosts, IndexOptInfo, IndexPath, Path, PlannerInfo, RelOptInfo, SpecialJoinInfo,
};
use crate::nodes::primnodes::{
    BoolTestType, NullTestType, RowCompareExpr, ScalarArrayOpExpr,
};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// Cost is f64 (nodes::nodes); GenericCosts uses it.
use crate::nodes::nodes::Cost;

// Default selectivity estimates. See header note: chosen to favor indexscans.

/// Default selectivity estimate for equalities such as "A = b".
pub const DEFAULT_EQ_SEL: f64 = 0.005;

/// Default selectivity estimate for inequalities such as "A < b".
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;

/// Default selectivity estimate for range inequalities "A > b AND A < c".
pub const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

/// Default selectivity estimate for multirange inequalities "A > b AND A < c".
pub const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

/// Default selectivity estimate for pattern-match operators such as LIKE.
pub const DEFAULT_MATCH_SEL: f64 = 0.005;

/// Default selectivity estimate for other matching operators.
pub const DEFAULT_MATCHING_SEL: f64 = 0.010;

/// Default number of distinct values in a table.
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/// Default selectivity estimate for boolean and null test nodes.
pub const DEFAULT_UNK_SEL: f64 = 0.005;
pub const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

/// Clamp a computed probability estimate to the valid [0, 1] range.
pub fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

bitflags! {
    /// Flags some selectivity estimators pass back to describe assumptions made.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SelFlag: u32 {
        /// Estimation fell back on one of the DEFAULTs.
        const USED_DEFAULT = 1 << 0;
    }
}

pub struct EstimationInfo {
    /// Flags marking special properties of the estimation.
    pub flags: SelFlag,
}

/// Return data from examine_variable and friends.
pub struct VariableStatData {
    /// The Var or expression tree.
    pub var: *mut Node, // TODO(ptr)
    /// Relation, or None if not identifiable.
    pub rel: Option<*mut RelOptInfo>, // TODO(ptr)
    /// pg_statistic tuple, or None if none. Must be freed when caller is done.
    pub stats_tuple: Option<HeapTuple>,
    /// How to free stats_tuple. The C `void (*)(HeapTuple)` -> closure.
    pub freefunc: Option<Box<dyn FnMut(HeapTuple)>>,
    /// Exposed type of expression.
    pub vartype: Oid,
    /// Actual type (after stripping relabel).
    pub atttype: Oid,
    /// Actual typmod (after stripping relabel).
    pub atttypmod: i32,
    /// Matches unique index, DISTINCT or GROUP-BY clause.
    pub isunique: bool,
    /// True if user has SELECT privilege on all rows from the table or column.
    pub acl_ok: bool,
}

// ReleaseVariableStats(vardata) -> drop the stats tuple via freefunc; in Rust this
// is the freefunc closure invoked at end of use (or Drop). No macro needed.

/// Intermediate and final values returned by genericcostestimate.
pub struct GenericCosts {
    /// Index-related startup cost.
    pub index_startup_cost: Cost,
    /// Total index-related scan cost.
    pub index_total_cost: Cost,
    /// Selectivity of index.
    pub index_selectivity: Selectivity,
    /// Order correlation of index.
    pub index_correlation: f64,

    /// Number of leaf pages visited.
    pub num_index_pages: f64,
    /// Number of leaf tuples visited.
    pub num_index_tuples: f64,
    /// Relevant random_page_cost value.
    pub spc_random_page_cost: f64,
    /// # indexscans from ScalarArrayOpExprs.
    pub num_sa_scans: f64,
}

// Hooks for plugins to get control when we ask for stats. C used PGDLLIMPORT fn
// pointers; in single-process Rust these stay fn-pointer types (set at startup).
pub type GetRelationStatsHook = fn(
    root: &mut PlannerInfo,
    rte: &RangeTblEntry,
    attnum: AttrNumber,
    vardata: &mut VariableStatData,
) -> bool;
pub type GetIndexStatsHook = fn(
    root: &mut PlannerInfo,
    index_oid: Oid,
    indexattnum: AttrNumber,
    vardata: &mut VariableStatData,
) -> bool;

pub static mut get_relation_stats_hook: Option<GetRelationStatsHook> = None;
pub static mut get_index_stats_hook: Option<GetIndexStatsHook> = None;

// Functions in selfuncs.c

pub fn examine_variable(
    _root: &mut PlannerInfo,
    _node: &Node,
    _varRelid: i32,
    _vardata: &mut VariableStatData,
) {
    unimplemented!()
}

pub fn all_rows_selectable(
    _root: &mut PlannerInfo,
    _varno: Index,
    _varattnos: &Bitmapset,
) -> bool {
    unimplemented!()
}

pub fn statistic_proc_security_check(
    _vardata: &VariableStatData,
    _func_oid: Oid,
) -> bool {
    unimplemented!()
}

/// On success returns (vardata, other, varonleft); None if not a usable
/// restriction clause. Folds the bool return and out-params into the result.
pub fn get_restriction_variable(
    _root: &mut PlannerInfo,
    _args: &[*mut Node],
    _varRelid: i32,
) -> Option<(VariableStatData, *mut Node, bool)> {
    unimplemented!()
}

/// Returns (vardata1, vardata2, join_is_reversed).
pub fn get_join_variables(
    _root: &mut PlannerInfo,
    _args: &[*mut Node],
    _sjinfo: &SpecialJoinInfo,
) -> (VariableStatData, VariableStatData, bool) {
    unimplemented!()
}

/// Returns (numdistinct, isdefault).
pub fn get_variable_numdistinct(_vardata: &VariableStatData) -> (f64, bool) {
    unimplemented!()
}

/// Returns (selectivity, sumcommon).
pub fn mcv_selectivity(
    _vardata: &VariableStatData,
    _opproc: &FmgrInfo,
    _collation: Oid,
    _constval: Datum,
    _varonleft: bool,
) -> (f64, f64) {
    unimplemented!()
}

/// Returns (selectivity, hist_size).
pub fn histogram_selectivity(
    _vardata: &VariableStatData,
    _opproc: &FmgrInfo,
    _collation: Oid,
    _constval: Datum,
    _varonleft: bool,
    _min_hist_size: i32,
    _n_skip: i32,
) -> (f64, i32) {
    unimplemented!()
}

pub fn generic_restriction_selectivity(
    _root: &mut PlannerInfo,
    _oproid: Oid,
    _collation: Oid,
    _args: &[*mut Node],
    _varRelid: i32,
    _default_selectivity: f64,
) -> f64 {
    unimplemented!()
}

pub fn ineq_histogram_selectivity(
    _root: &mut PlannerInfo,
    _vardata: &VariableStatData,
    _opoid: Oid,
    _opproc: &FmgrInfo,
    _isgt: bool,
    _iseq: bool,
    _collation: Oid,
    _constval: Datum,
    _consttype: Oid,
) -> f64 {
    unimplemented!()
}

pub fn var_eq_const(
    _vardata: &VariableStatData,
    _oproid: Oid,
    _collation: Oid,
    _constval: Datum,
    _constisnull: bool,
    _varonleft: bool,
    _negate: bool,
) -> f64 {
    unimplemented!()
}

pub fn var_eq_non_const(
    _vardata: &VariableStatData,
    _oproid: Oid,
    _collation: Oid,
    _other: &Node,
    _varonleft: bool,
    _negate: bool,
) -> f64 {
    unimplemented!()
}

pub fn boolvarsel(_root: &mut PlannerInfo, _arg: &Node, _varRelid: i32) -> Selectivity {
    unimplemented!()
}

pub fn booltestsel(
    _root: &mut PlannerInfo,
    _booltesttype: BoolTestType,
    _arg: &Node,
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub fn nulltestsel(
    _root: &mut PlannerInfo,
    _nulltesttype: NullTestType,
    _arg: &Node,
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub fn scalararraysel(
    _root: &mut PlannerInfo,
    _clause: &ScalarArrayOpExpr,
    _is_join_clause: bool,
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub fn estimate_array_length(_root: &mut PlannerInfo, _arrayexpr: &Node) -> f64 {
    unimplemented!()
}

pub fn rowcomparesel(
    _root: &mut PlannerInfo,
    _clause: &RowCompareExpr,
    _varRelid: i32,
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

/// Returns (leftstart, leftend, rightstart, rightend) merge-join scan selectivity.
pub fn mergejoinscansel(
    _root: &mut PlannerInfo,
    _clause: &Node,
    _opfamily: Oid,
    _cmptype: CompareType,
    _nulls_first: bool,
) -> (Selectivity, Selectivity, Selectivity, Selectivity) {
    unimplemented!()
}

/// Returns (num_groups, estinfo). C filled the EstimationInfo out-param.
pub fn estimate_num_groups(
    _root: &mut PlannerInfo,
    _groupExprs: &[*mut Node],
    _input_rows: f64,
    _pgset: Option<&mut Vec<i32>>,
) -> (f64, EstimationInfo) {
    unimplemented!()
}

/// Returns (bucketsizes list, innerbucketsize).
pub fn estimate_multivariate_bucketsize(
    _root: &mut PlannerInfo,
    _inner: &RelOptInfo,
    _hashclauses: &[*mut Node],
) -> (Vec<*mut Node>, Selectivity) {
    unimplemented!()
}

/// Returns (mcv_freq, bucketsize_frac).
pub fn estimate_hash_bucket_stats(
    _root: &mut PlannerInfo,
    _hashkey: &Node,
    _nbuckets: f64,
) -> (Selectivity, Selectivity) {
    unimplemented!()
}

pub fn estimate_hashagg_tablesize(
    _root: &mut PlannerInfo,
    _path: &Path,
    _agg_costs: &AggClauseCosts,
    _dNumGroups: f64,
) -> f64 {
    unimplemented!()
}

pub fn get_quals_from_indexclauses(_indexclauses: &[*mut Node]) -> Vec<*mut Node> {
    unimplemented!()
}

pub fn index_other_operands_eval_cost(
    _root: &mut PlannerInfo,
    _indexquals: &[*mut Node],
) -> Cost {
    unimplemented!()
}

pub fn add_predicate_to_index_quals(
    _index: &IndexOptInfo,
    _indexQuals: &[*mut Node],
) -> Vec<*mut Node> {
    unimplemented!()
}

pub fn genericcostestimate(
    _root: &mut PlannerInfo,
    _path: &IndexPath,
    _loop_count: f64,
    _costs: &mut GenericCosts,
) {
    unimplemented!()
}

// Functions in array_selfuncs.c

pub fn scalararraysel_containment(
    _root: &mut PlannerInfo,
    _leftop: &Node,
    _rightop: &Node,
    _elemtype: Oid,
    _isEquality: bool,
    _useOr: bool,
    _varRelid: i32,
) -> Selectivity {
    unimplemented!()
}

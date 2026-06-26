//! Translated from PostgreSQL src/include/optimizer/optimizer.h
//! External API for the Postgres planner.

#![allow(clippy::boxed_local, reason = "1:1 PG port: Box<Node>/Box<Path> mirrors PG pointer-passed nodes")]

use bitflags::bitflags;

use crate::access::htup::HeapTupleData;
use crate::c::Index;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{Cardinality, JoinType, Node, Selectivity};
use crate::nodes::params::ParamListInfoData;
use crate::nodes::parsenodes::{Query, SortGroupClause};
use crate::nodes::pathnodes::{IndexOptInfo, PlannerInfo, SpecialJoinInfo};
use crate::nodes::plannodes::PlannedStmt;
use crate::nodes::primnodes::{ScalarArrayOpExpr, TargetEntry};
use crate::postgres_ext::Oid;

type PlannerInfoRef<'a> = &'a mut PlannerInfo;
type SpecialJoinInfoRef<'a> = &'a SpecialJoinInfo;
type IndexOptInfoRef<'a> = &'a IndexOptInfo;

// in path/clausesel.c:

pub fn clause_selectivity(
    _root: PlannerInfoRef,
    _clause: Option<Box<Node>>,
    _var_relid: i32,
    _jointype: JoinType,
    _sjinfo: Option<SpecialJoinInfoRef>,
) -> Selectivity {
    unimplemented!()
}

pub fn clause_selectivity_ext(
    _root: PlannerInfoRef,
    _clause: Option<Box<Node>>,
    _var_relid: i32,
    _jointype: JoinType,
    _sjinfo: Option<SpecialJoinInfoRef>,
    _use_extended_stats: bool,
) -> Selectivity {
    unimplemented!()
}

pub fn clauselist_selectivity(
    _root: PlannerInfoRef,
    _clauses: Vec<Box<Node>>,
    _var_relid: i32,
    _jointype: JoinType,
    _sjinfo: Option<SpecialJoinInfoRef>,
) -> Selectivity {
    unimplemented!()
}

pub fn clauselist_selectivity_ext(
    _root: PlannerInfoRef,
    _clauses: Vec<Box<Node>>,
    _var_relid: i32,
    _jointype: JoinType,
    _sjinfo: Option<SpecialJoinInfoRef>,
    _use_extended_stats: bool,
) -> Selectivity {
    unimplemented!()
}

// in path/costsize.c: widely used cost parameters.
// TODO(global): migrate these GUCs to session/planner config.
pub static mut SEQ_PAGE_COST: f64 = 0.0;
pub static mut RANDOM_PAGE_COST: f64 = 0.0;
pub static mut CPU_TUPLE_COST: f64 = 0.0;
pub static mut CPU_INDEX_TUPLE_COST: f64 = 0.0;
pub static mut CPU_OPERATOR_COST: f64 = 0.0;
pub static mut PARALLEL_TUPLE_COST: f64 = 0.0;
pub static mut PARALLEL_SETUP_COST: f64 = 0.0;
pub static mut RECURSIVE_WORKTABLE_FACTOR: f64 = 0.0;
pub static mut EFFECTIVE_CACHE_SIZE: i32 = 0;

pub fn clamp_row_est(_nrows: f64) -> f64 {
    unimplemented!()
}

pub fn clamp_width_est(_tuple_width: i64) -> i32 {
    unimplemented!()
}

pub fn clamp_cardinality_to_long(_x: Cardinality) -> i64 {
    unimplemented!()
}

// in path/indxpath.c:

pub fn is_pseudo_constant_for_index(
    _root: PlannerInfoRef,
    _expr: Option<Box<Node>>,
    _index: IndexOptInfoRef,
) -> bool {
    unimplemented!()
}

// in plan/planner.c:

/// possible values for debug_parallel_query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugParallelMode {
    Off,
    On,
    Regress,
}

// GUC parameters. TODO(global): migrate to session config.
pub static mut DEBUG_PARALLEL_QUERY: i32 = 0;
pub static mut PARALLEL_LEADER_PARTICIPATION: bool = false;
pub static mut ENABLE_DISTINCT_REORDERING: bool = false;

pub fn planner(
    _parse: Box<Query>,
    _query_string: &str,
    _cursor_options: i32,
    _bound_params: Option<Box<ParamListInfoData>>,
) -> Box<PlannedStmt> {
    unimplemented!()
}

pub fn expression_planner(_expr: Box<Node>) -> Box<Node> {
    unimplemented!()
}

/// C out-params `List **relationOids, List **invalItems` -> returned tuple.
pub fn expression_planner_with_deps(
    _expr: Box<Node>,
) -> (Box<Node>, Vec<Oid>, Vec<Box<Node>>) {
    unimplemented!()
}

pub fn plan_cluster_use_sort(_table_oid: Oid, _index_oid: Oid) -> bool {
    unimplemented!()
}

pub fn plan_create_index_workers(_table_oid: Oid, _index_oid: Oid) -> i32 {
    unimplemented!()
}

// in plan/setrefs.c:

/// C out-params `relationOids, invalItems, hasRowSecurity` -> returned tuple.
pub fn extract_query_dependencies(_query: Option<Box<Node>>) -> (Vec<Oid>, Vec<Box<Node>>, bool) {
    unimplemented!()
}

// in prep/prepqual.c:

pub fn negate_clause(_node: Option<Box<Node>>) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn canonicalize_qual(_qual: Option<Box<Node>>, _is_check: bool) -> Option<Box<Node>> {
    unimplemented!()
}

// in util/clauses.c:

pub fn contain_mutable_functions(_clause: Option<Box<Node>>) -> bool {
    unimplemented!()
}

pub fn contain_mutable_functions_after_planning(_expr: Box<Node>) -> bool {
    unimplemented!()
}

pub fn contain_volatile_functions(_clause: Option<Box<Node>>) -> bool {
    unimplemented!()
}

pub fn contain_volatile_functions_after_planning(_expr: Box<Node>) -> bool {
    unimplemented!()
}

pub fn contain_volatile_functions_not_nextval(_clause: Option<Box<Node>>) -> bool {
    unimplemented!()
}

pub fn eval_const_expressions(
    _root: Option<PlannerInfoRef>,
    _node: Option<Box<Node>>,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn convert_saop_to_hashed_saop(_node: Option<Box<Node>>) {
    unimplemented!()
}

pub fn estimate_expression_value(
    _root: PlannerInfoRef,
    _node: Option<Box<Node>>,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn evaluate_expr(
    _expr: Box<Node>,
    _result_type: Oid,
    _result_typmod: i32,
    _result_collation: Oid,
) -> Box<Node> {
    unimplemented!()
}

pub fn expand_function_arguments(
    _args: Vec<Box<Node>>,
    _include_out_arguments: bool,
    _result_type: Oid,
    _func_tuple: &HeapTupleData,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn make_saop_expr(
    _oper: Oid,
    _leftexpr: Option<Box<Node>>,
    _coltype: Oid,
    _arraycollid: Oid,
    _inputcollid: Oid,
    _exprs: Vec<Box<Node>>,
    _have_non_const: bool,
) -> Box<ScalarArrayOpExpr> {
    unimplemented!()
}

// in util/predtest.c:

pub fn predicate_implied_by(
    _predicate_list: Vec<Box<Node>>,
    _clause_list: Vec<Box<Node>>,
    _weak: bool,
) -> bool {
    unimplemented!()
}

pub fn predicate_refuted_by(
    _predicate_list: Vec<Box<Node>>,
    _clause_list: Vec<Box<Node>>,
    _weak: bool,
) -> bool {
    unimplemented!()
}

// in util/tlist.c:

pub fn count_nonjunk_tlist_entries(_tlist: &[TargetEntry]) -> i32 {
    unimplemented!()
}

pub fn get_sortgroupref_tle(_sortref: Index, _target_list: &[TargetEntry]) -> Option<&TargetEntry> {
    unimplemented!()
}

pub fn get_sortgroupclause_tle<'a>(
    _sg_clause: &SortGroupClause,
    _target_list: &'a [TargetEntry],
) -> Option<&'a TargetEntry> {
    unimplemented!()
}

pub fn get_sortgroupclause_expr(
    _sg_clause: &SortGroupClause,
    _target_list: &[TargetEntry],
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn get_sortgrouplist_exprs(
    _sg_clauses: &[SortGroupClause],
    _target_list: &[TargetEntry],
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn get_sortgroupref_clause(_sortref: Index, _clauses: &[SortGroupClause]) -> Box<SortGroupClause> {
    unimplemented!()
}

pub fn get_sortgroupref_clause_noerr(
    _sortref: Index,
    _clauses: &[SortGroupClause],
) -> Option<Box<SortGroupClause>> {
    unimplemented!()
}

// in util/var.c:

bitflags! {
    /// Bits that can be OR'd into the flags argument of pull_var_clause().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PullVarClauseFlags: i32 {
        /// include Aggrefs in output list
        const INCLUDE_AGGREGATES = 0x0001;
        /// recurse into Aggref arguments
        const RECURSE_AGGREGATES = 0x0002;
        /// include WindowFuncs in output list
        const INCLUDE_WINDOWFUNCS = 0x0004;
        /// recurse into WindowFunc arguments
        const RECURSE_WINDOWFUNCS = 0x0008;
        /// include PlaceHolderVars in output list
        const INCLUDE_PLACEHOLDERS = 0x0010;
        /// recurse into PlaceHolderVar arguments
        const RECURSE_PLACEHOLDERS = 0x0020;
        /// include ConvertRowtypeExprs in output list
        const INCLUDE_CONVERTROWTYPES = 0x0040;
    }
}

pub fn pull_varnos(_root: PlannerInfoRef, _node: Option<Box<Node>>) -> Bitmapset {
    unimplemented!()
}

pub fn pull_varnos_of_level(
    _root: PlannerInfoRef,
    _node: Option<Box<Node>>,
    _levelsup: i32,
) -> Bitmapset {
    unimplemented!()
}

/// C out-param `Bitmapset **varattnos` -> returned value.
pub fn pull_varattnos(_node: Option<Box<Node>>, _varno: Index) -> Bitmapset {
    unimplemented!()
}

pub fn pull_vars_of_level(_node: Option<Box<Node>>, _levelsup: i32) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn contain_var_clause(_node: Option<Box<Node>>) -> bool {
    unimplemented!()
}

pub fn contain_vars_of_level(_node: Option<Box<Node>>, _levelsup: i32) -> bool {
    unimplemented!()
}

pub fn contain_vars_returning_old_or_new(_node: Option<Box<Node>>) -> bool {
    unimplemented!()
}

pub fn locate_var_of_level(_node: Option<Box<Node>>, _levelsup: i32) -> i32 {
    unimplemented!()
}

pub fn pull_var_clause(_node: Option<Box<Node>>, _flags: PullVarClauseFlags) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn flatten_join_alias_vars(
    _root: PlannerInfoRef,
    _query: &Query,
    _node: Option<Box<Node>>,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn flatten_group_exprs(
    _root: PlannerInfoRef,
    _query: &Query,
    _node: Option<Box<Node>>,
) -> Option<Box<Node>> {
    unimplemented!()
}
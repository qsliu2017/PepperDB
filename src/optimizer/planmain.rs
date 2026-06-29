//! Translated from PostgreSQL src/include/optimizer/planmain.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::access::attnum::AttrNumber;
use crate::nodes::nodes::{AggSplit, AggStrategy, JoinType, LimitOption};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Relids, RestrictInfo, SpecialJoinInfo,
};
use crate::nodes::plannodes::{Agg, ForeignScan, Limit, Plan, Sort, SubqueryScan};
use crate::nodes::primnodes::{Aggref, Expr, Index, Param, TargetEntry};
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;

/* GUC parameters */
pub const DEFAULT_CURSOR_TUPLE_FRACTION: f64 = 0.1;
pub static mut CURSOR_TUPLE_FRACTION: f64 = DEFAULT_CURSOR_TUPLE_FRACTION;
pub static mut ENABLE_SELF_JOIN_ELIMINATION: bool = false;

/// query_planner callback to compute query_pathkeys.
pub type QueryPathkeysCallback = fn(root: &mut PlannerInfo);

/* prototypes for plan/planmain.c */
/// PG `query_planner`. See `crate::backend::optimizer::plan::planmain`.
pub use crate::backend::optimizer::plan::planmain::query_planner;

/* prototypes for plan/planagg.c */
pub use crate::backend::optimizer::plan::planagg::preprocess_minmax_aggregates;

/* prototypes for plan/createplan.c */
/// PG `create_plan`. See `crate::backend::optimizer::plan::planmain`. Returns the
/// polymorphic top plan node (`Plan *` in C -> `Node`).
pub use crate::backend::optimizer::plan::planmain::create_plan;

pub fn make_foreignscan(
    qptlist: Vec<TargetEntry>,
    qpqual: Vec<Expr>,
    scanrelid: Index,
    fdw_exprs: Vec<Expr>,
    fdw_private: Vec<Node>,
    fdw_scan_tlist: Vec<TargetEntry>,
    fdw_recheck_quals: Vec<Expr>,
    outer_plan: Option<Box<Plan>>,
) -> ForeignScan {
    unimplemented!()
}

pub fn change_plan_targetlist(
    subplan: Box<Plan>,
    tlist: Vec<TargetEntry>,
    tlist_parallel_safe: bool,
) -> Plan {
    unimplemented!()
}

pub fn materialize_finished_plan(subplan: Box<Plan>) -> Plan {
    unimplemented!()
}

pub fn is_projection_capable_path(path: &crate::nodes::pathnodes::Path) -> bool {
    unimplemented!()
}

pub fn is_projection_capable_plan(plan: &Plan) -> bool {
    unimplemented!()
}

/* External use of these functions is deprecated: */
pub fn make_sort_from_sortclauses(sortcls: Vec<crate::nodes::parsenodes::SortGroupClause>, lefttree: Box<Plan>) -> Sort {
    unimplemented!()
}

pub fn make_agg(
    tlist: Vec<TargetEntry>,
    qual: Vec<Expr>,
    aggstrategy: AggStrategy,
    aggsplit: AggSplit,
    num_group_cols: i32,
    grp_col_idx: &[AttrNumber],
    grp_operators: &[Oid],
    grp_collations: &[Oid],
    grouping_sets: Vec<Node>,
    chain: Vec<Plan>,
    d_num_groups: f64,
    transition_space: usize,
    lefttree: Box<Plan>,
) -> Agg {
    unimplemented!()
}

pub fn make_limit(
    lefttree: Box<Plan>,
    limit_offset: Option<Node>,
    limit_count: Option<Node>,
    limit_option: LimitOption,
    uniq_num_cols: i32,
    uniq_col_idx: &[AttrNumber],
    uniq_operators: &[Oid],
    uniq_collations: &[Oid],
) -> Limit {
    unimplemented!()
}

/* prototypes for plan/initsplan.c */
pub static mut FROM_COLLAPSE_LIMIT: i32 = 0;
pub static mut JOIN_COLLAPSE_LIMIT: i32 = 0;

pub use crate::backend::optimizer::plan::initsplan::{
    add_base_rels_to_query, add_other_rels_to_query, add_vars_to_attr_needed,
    add_vars_to_targetlist, build_base_rel_tlists,
};

pub fn remove_useless_groupby_columns(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn find_lateral_references(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn rebuild_lateral_attr_needed(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn create_lateral_join_info(root: &mut PlannerInfo) {
    unimplemented!()
}

pub use crate::backend::optimizer::plan::initsplan::{
    deconstruct_jointree, distribute_restrictinfo_to_rels, restriction_is_always_false,
    restriction_is_always_true,
};

pub use crate::backend::optimizer::plan::initsplan::process_implied_equality;

pub use crate::backend::optimizer::plan::initsplan::build_implied_join_equality;

pub fn rebuild_joinclause_attr_needed(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn match_foreign_keys_to_quals(root: &mut PlannerInfo) {
    unimplemented!()
}

/* prototypes for plan/analyzejoins.c */
pub use crate::backend::optimizer::plan::analyzejoins::{
    innerrel_is_unique, innerrel_is_unique_ext, query_is_distinct_for, query_supports_distinctness,
    reduce_unique_semijoins, remove_useless_joins, remove_useless_self_joins,
};

/* prototypes for plan/setrefs.c */
/// PG `set_plan_references`. See `crate::backend::optimizer::plan::setrefs`.
/// `plan` is the polymorphic top plan node (`Plan *` in C -> `Node`).
pub use crate::backend::optimizer::plan::setrefs::set_plan_references;

pub fn trivial_subqueryscan(plan: &SubqueryScan) -> bool {
    unimplemented!()
}

pub fn find_minmax_agg_replacement_param(root: &mut PlannerInfo, aggref: &Aggref) -> Option<Param> {
    unimplemented!()
}

pub fn record_plan_function_dependency(root: &mut PlannerInfo, funcid: Oid) {
    unimplemented!()
}

pub fn record_plan_type_dependency(root: &mut PlannerInfo, typid: Oid) {
    unimplemented!()
}

pub fn extract_query_dependencies_walker(node: &Node, context: &mut PlannerInfo) -> bool {
    unimplemented!()
}

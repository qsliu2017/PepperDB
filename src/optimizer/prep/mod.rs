//! Planner preprocessing (postgres/src/backend/optimizer/prep).
//!
//! So far: boolean-qual canonicalization (`prepqual`).

pub mod prepqual;

// optimizer/prep.h - prototypes for files in optimizer/prep/

use std::ffi::c_int;

use crate::c::Index;
use crate::nodes::nodes::{AggSplit, Node};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{AggClauseCosts, PlannerInfo, RelOptInfo, Relids};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::PlanRowMark;

/*
 * prototypes for prepjointree.c
 */
pub unsafe fn transform_MERGE_to_join(parse: *mut Query) {
    unimplemented!()
}

pub unsafe fn replace_empty_jointree(parse: *mut Query) {
    unimplemented!()
}

pub unsafe fn pull_up_sublinks(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn preprocess_function_rtes(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn expand_virtual_generated_columns(root: *mut PlannerInfo) -> *mut Query {
    unimplemented!()
}

pub unsafe fn pull_up_subqueries(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn flatten_simple_union_all(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn reduce_outer_joins(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn remove_useless_result_rtes(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn get_relids_in_jointree(
    jtnode: *mut Node,
    include_outer_joins: bool,
    include_inner_joins: bool,
) -> Relids {
    unimplemented!()
}

pub unsafe fn get_relids_for_join(query: *mut Query, joinrelid: c_int) -> Relids {
    unimplemented!()
}

/*
 * prototypes for preptlist.c
 */
pub unsafe fn preprocess_targetlist(root: *mut PlannerInfo) {
    unimplemented!()
}

pub unsafe fn extract_update_targetlist_colnos(tlist: *mut List) -> *mut List {
    unimplemented!()
}

pub unsafe fn get_plan_rowmark(rowmarks: *mut List, rtindex: Index) -> *mut PlanRowMark {
    unimplemented!()
}

/*
 * prototypes for prepagg.c
 */
pub unsafe fn get_agg_clause_costs(
    root: *mut PlannerInfo,
    aggsplit: AggSplit,
    costs: *mut AggClauseCosts,
) {
    unimplemented!()
}

pub unsafe fn preprocess_aggrefs(root: *mut PlannerInfo, clause: *mut Node) {
    unimplemented!()
}

/*
 * prototypes for prepunion.c
 */
pub unsafe fn plan_set_operations(root: *mut PlannerInfo) -> *mut RelOptInfo {
    unimplemented!()
}
pub mod prepagg;
pub mod prepjointree;
pub mod prepunion;

//! Translated from PostgreSQL src/include/optimizer/prep.h

use crate::nodes::nodes::{AggSplit, Node};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{AggClauseCosts, PlannerInfo, RelOptInfo, Relids};
use crate::nodes::plannodes::PlanRowMark;
use crate::nodes::primnodes::{Index, TargetEntry};

/* prototypes for prepjointree.c */
pub fn transform_merge_to_join(parse: &mut Query) {
    unimplemented!()
}

pub use crate::backend::optimizer::prep::prepjointree::replace_empty_jointree;

pub fn pull_up_sublinks(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn preprocess_function_rtes(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn expand_virtual_generated_columns(root: &mut PlannerInfo) -> Query {
    unimplemented!()
}

pub use crate::backend::optimizer::prep::prepjointree::{flatten_simple_union_all, pull_up_subqueries};

pub fn reduce_outer_joins(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn remove_useless_result_rtes(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn get_relids_in_jointree(
    jtnode: &Node,
    include_outer_joins: bool,
    include_inner_joins: bool,
) -> Relids {
    unimplemented!()
}

pub fn get_relids_for_join(query: &Query, joinrelid: i32) -> Relids {
    unimplemented!()
}

/* prototypes for preptlist.c */
/// PG `preprocess_targetlist`. See `crate::backend::optimizer::prep::preptlist`.
pub use crate::backend::optimizer::prep::preptlist::preprocess_targetlist;

pub fn extract_update_targetlist_colnos(tlist: &[TargetEntry]) -> Vec<i32> {
    unimplemented!()
}

pub fn get_plan_rowmark(rowmarks: &[PlanRowMark], rtindex: Index) -> Option<PlanRowMark> {
    unimplemented!()
}

/* prototypes for prepagg.c */
pub fn get_agg_clause_costs(root: &mut PlannerInfo, aggsplit: AggSplit, costs: &mut AggClauseCosts) {
    unimplemented!()
}

pub use crate::backend::optimizer::prep::prepagg::preprocess_aggrefs;

/* prototypes for prepunion.c */
pub fn plan_set_operations(root: &mut PlannerInfo) -> RelOptInfo {
    unimplemented!()
}

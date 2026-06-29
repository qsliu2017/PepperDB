//! Translated from PostgreSQL src/include/optimizer/plancat.h

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{CmdType, JoinType, Selectivity};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pathnodes::{
    PlannerInfo, QualCost, RelOptInfo, SpecialJoinInfo,
};
use crate::nodes::primnodes::{Expr, Index};
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;

/// Hook for plugins to get control in get_relation_info().
pub type GetRelationInfoHookType =
    fn(root: &mut PlannerInfo, relation_object_id: Oid, inhparent: bool, rel: &mut RelOptInfo);
pub static mut GET_RELATION_INFO_HOOK: Option<GetRelationInfoHookType> = None;

/// Out-params of estimate_rel_size folded into a struct (function-mapping 5.3).
pub struct RelSizeEstimate {
    pub attr_widths: Vec<i32>,
    pub pages: BlockNumber,
    pub tuples: f64,
    pub allvisfrac: f64,
}

pub fn get_relation_info(
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    inhparent: bool,
    rel: &mut RelOptInfo,
) {
    unimplemented!()
}

pub fn infer_arbiter_indexes(root: &mut PlannerInfo) -> Vec<Oid> {
    unimplemented!()
}

pub fn estimate_rel_size(rel: &crate::utils::rel::RelationData) -> RelSizeEstimate {
    unimplemented!()
}

/// Returns the data width; `attr_widths` out-array folded into the tuple.
pub fn get_rel_data_width(rel: &crate::utils::rel::RelationData) -> (i32, Vec<i32>) {
    unimplemented!()
}

/// Returns the data width; `attr_widths` out-array folded into the tuple.
pub fn get_relation_data_width(relid: Oid) -> (i32, Vec<i32>) {
    unimplemented!()
}

pub fn relation_excluded_by_constraints(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    rte: &RangeTblEntry,
) -> bool {
    unimplemented!()
}

pub fn build_physical_tlist(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
) -> Vec<crate::nodes::primnodes::TargetEntry> {
    unimplemented!()
}

pub fn has_unique_index(rel: &RelOptInfo, attno: crate::access::attnum::AttrNumber) -> bool {
    unimplemented!()
}

pub fn restriction_selectivity(
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    var_relid: i32,
) -> Selectivity {
    unimplemented!()
}

pub fn join_selectivity(
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub fn function_selectivity(
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    is_join: bool,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    unimplemented!()
}

pub fn add_function_cost(root: &mut PlannerInfo, funcid: Oid, node: Option<&Node>, cost: &mut QualCost) {
    unimplemented!()
}

pub fn get_function_rows(root: &mut PlannerInfo, funcid: Oid, node: Option<&Node>) -> f64 {
    unimplemented!()
}

pub fn has_row_triggers(root: &mut PlannerInfo, rti: Index, event: CmdType) -> bool {
    unimplemented!()
}

pub fn has_transition_tables(root: &mut PlannerInfo, rti: Index, event: CmdType) -> bool {
    unimplemented!()
}

pub fn has_stored_generated_columns(root: &mut PlannerInfo, rti: Index) -> bool {
    unimplemented!()
}

pub fn get_dependent_generated_columns(
    root: &mut PlannerInfo,
    rti: Index,
    target_cols: &Bitmapset,
) -> Bitmapset {
    unimplemented!()
}

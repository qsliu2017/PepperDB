//! Translated from PostgreSQL src/include/optimizer/placeholder.h

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    PlaceHolderInfo, PlaceHolderVar, PlannerInfo, RelOptInfo, Relids, SpecialJoinInfo,
};
use crate::nodes::primnodes::Expr;

pub fn make_placeholder_expr(root: &mut PlannerInfo, expr: Box<Expr>, phrels: Relids) -> PlaceHolderVar {
    unimplemented!()
}

pub fn find_placeholder_info(root: &mut PlannerInfo, phv: &PlaceHolderVar) -> PlaceHolderInfo {
    unimplemented!()
}

pub fn find_placeholders_in_jointree(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn fix_placeholder_input_needed_levels(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn rebuild_placeholder_attr_needed(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn add_placeholders_to_base_rels(root: &mut PlannerInfo) {
    unimplemented!()
}

pub fn add_placeholders_to_joinrel(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
) {
    unimplemented!()
}

pub fn contain_placeholder_references_to(root: &mut PlannerInfo, clause: &Node, relid: i32) -> bool {
    unimplemented!()
}

pub fn get_placeholder_nulling_relids(root: &mut PlannerInfo, phinfo: &PlaceHolderInfo) -> Relids {
    unimplemented!()
}

pub fn strip_noop_phvs(node: Box<Node>) -> Box<Node> {
    unimplemented!()
}

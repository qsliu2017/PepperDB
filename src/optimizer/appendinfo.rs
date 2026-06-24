//! Translated from PostgreSQL src/include/optimizer/appendinfo.h
//! Routines for mapping expressions between append rel parent(s) and children

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{AppendRelInfo, PlannerInfo, RelOptInfo, Relids};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::primnodes::Var;
use crate::utils::relcache::Relation;

pub fn make_append_rel_info(
    parentrel: Relation,
    childrel: Relation,
    parent_rt_index: usize,
    child_rt_index: usize,
) -> Box<AppendRelInfo> {
    unimplemented!()
}

pub fn adjust_appendrel_attrs(
    root: &mut PlannerInfo,
    node: Option<Box<Node>>,
    appinfos: &[&AppendRelInfo],
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn adjust_appendrel_attrs_multilevel(
    root: &mut PlannerInfo,
    node: Option<Box<Node>>,
    childrel: &RelOptInfo,
    parentrel: &RelOptInfo,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn adjust_child_relids(relids: Relids, appinfos: &[&AppendRelInfo]) -> Relids {
    unimplemented!()
}

pub fn adjust_child_relids_multilevel(
    root: &mut PlannerInfo,
    relids: Relids,
    childrel: &RelOptInfo,
    parentrel: &RelOptInfo,
) -> Relids {
    unimplemented!()
}

pub fn adjust_inherited_attnums(attnums: Vec<i32>, context: &AppendRelInfo) -> Vec<i32> {
    unimplemented!()
}

pub fn adjust_inherited_attnums_multilevel(
    root: &mut PlannerInfo,
    attnums: Vec<i32>,
    child_relid: usize,
    top_parent_relid: usize,
) -> Vec<i32> {
    unimplemented!()
}

/// C: out-params `processed_tlist`, `update_colnos` -> returned tuple.
pub fn get_translated_update_targetlist(
    root: &mut PlannerInfo,
    relid: usize,
) -> (Vec<Box<Node>>, Vec<i32>) {
    unimplemented!()
}

/// C: out-param `nappinfos` -> the returned Vec's length.
pub fn find_appinfos_by_relids<'a>(
    root: &'a PlannerInfo,
    relids: Relids,
) -> Vec<&'a AppendRelInfo> {
    unimplemented!()
}

pub fn add_row_identity_var(
    root: &mut PlannerInfo,
    orig_var: &Var,
    rtindex: usize,
    rowid_name: &str,
) {
    unimplemented!()
}

pub fn add_row_identity_columns(
    root: &mut PlannerInfo,
    rtindex: usize,
    target_rte: &mut RangeTblEntry,
    target_relation: Relation,
) {
    unimplemented!()
}

pub fn distribute_row_identity_vars(root: &mut PlannerInfo) {
    unimplemented!()
}

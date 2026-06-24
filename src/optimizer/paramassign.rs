//! Translated from PostgreSQL src/include/optimizer/paramassign.h
//! Functions for assigning PARAM_EXEC slots during planning.

use crate::nodes::pathnodes::{PlaceHolderVar, PlannerInfo, Relids};
use crate::nodes::primnodes::{
    Aggref, GroupingFunc, MergeSupportFunc, Param, ReturningExpr, Var,
};
use crate::postgres_ext::Oid;

pub fn replace_outer_var(root: &mut PlannerInfo, var: &Var) -> Box<Param> {
    unimplemented!()
}

pub fn replace_outer_placeholdervar(root: &mut PlannerInfo, phv: &PlaceHolderVar) -> Box<Param> {
    unimplemented!()
}

pub fn replace_outer_agg(root: &mut PlannerInfo, agg: &Aggref) -> Box<Param> {
    unimplemented!()
}

pub fn replace_outer_grouping(root: &mut PlannerInfo, grp: &GroupingFunc) -> Box<Param> {
    unimplemented!()
}

pub fn replace_outer_merge_support(root: &mut PlannerInfo, msf: &MergeSupportFunc) -> Box<Param> {
    unimplemented!()
}

pub fn replace_outer_returning(root: &mut PlannerInfo, rexpr: &ReturningExpr) -> Box<Param> {
    unimplemented!()
}

pub fn replace_nestloop_param_var(root: &mut PlannerInfo, var: &Var) -> Box<Param> {
    unimplemented!()
}

pub fn replace_nestloop_param_placeholdervar(
    root: &mut PlannerInfo,
    phv: &PlaceHolderVar,
) -> Box<Param> {
    unimplemented!()
}

pub fn process_subquery_nestloop_params(root: &mut PlannerInfo, subplan_params: &[Box<crate::nodes::nodes::Node>]) {
    unimplemented!()
}

pub fn identify_current_nestloop_params(
    root: &mut PlannerInfo,
    leftrelids: Relids,
    outerrelids: Relids,
) -> Vec<Box<crate::nodes::nodes::Node>> {
    unimplemented!()
}

pub fn generate_new_exec_param(
    root: &mut PlannerInfo,
    paramtype: Oid,
    paramtypmod: i32,
    paramcollation: Oid,
) -> Box<Param> {
    unimplemented!()
}

pub fn assign_special_exec_param(root: &mut PlannerInfo) -> i32 {
    unimplemented!()
}

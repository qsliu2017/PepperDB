//! Translated from PostgreSQL src/include/optimizer/joininfo.h
//! prototypes for joininfo.c.

#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids, RestrictInfo};

pub fn have_relevant_joinclause(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    unimplemented!()
}

pub fn add_join_clause_to_rels(
    root: &mut PlannerInfo,
    restrictinfo: &RestrictInfo,
    join_relids: Relids,
) {
    unimplemented!()
}

pub fn remove_join_clause_from_rels(
    root: &mut PlannerInfo,
    restrictinfo: &RestrictInfo,
    join_relids: Relids,
) {
    unimplemented!()
}

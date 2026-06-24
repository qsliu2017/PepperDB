//! Translated from PostgreSQL src/include/optimizer/joininfo.h
//! prototypes for joininfo.c.

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

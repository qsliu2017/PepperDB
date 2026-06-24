//! Translated from PostgreSQL src/include/optimizer/inherit.h
//! prototypes for inherit.c.

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pathnodes::{AppendRelInfo, PlannerInfo, RelOptInfo};

pub fn expand_inherited_rtentry(
    root: &mut PlannerInfo,
    rel: &mut RelOptInfo,
    rte: &RangeTblEntry,
    rti: usize,
) {
    unimplemented!()
}

pub fn get_rel_all_updated_cols(root: &mut PlannerInfo, rel: &RelOptInfo) -> Bitmapset {
    unimplemented!()
}

pub fn apply_child_basequals(
    root: &mut PlannerInfo,
    parentrel: &RelOptInfo,
    childrel: &mut RelOptInfo,
    child_rte: &RangeTblEntry,
    appinfo: &AppendRelInfo,
) -> bool {
    unimplemented!()
}

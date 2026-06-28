//! Translated from PostgreSQL src/include/optimizer/tlist.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::access::attnum::AttrNumber;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::SortGroupClause;
use crate::nodes::pathnodes::{PathTarget, PlannerInfo};
use crate::nodes::primnodes::{Expr, Index, TargetEntry};
use crate::postgres_ext::Oid;

/// PG `tlist_member`. See `crate::backend::optimizer::util::tlist`.
pub use crate::backend::optimizer::util::tlist::tlist_member;

pub fn add_to_flat_tlist(tlist: Vec<TargetEntry>, exprs: &[Expr]) -> Vec<TargetEntry> {
    unimplemented!()
}

pub fn get_tlist_exprs(tlist: &[TargetEntry], include_junk: bool) -> Vec<Expr> {
    unimplemented!()
}

pub fn tlist_same_exprs(tlist1: &[TargetEntry], tlist2: &[TargetEntry]) -> bool {
    unimplemented!()
}

pub fn tlist_same_datatypes(tlist: &[TargetEntry], col_types: &[Oid], junk_ok: bool) -> bool {
    unimplemented!()
}

pub fn tlist_same_collations(tlist: &[TargetEntry], col_collations: &[Oid], junk_ok: bool) -> bool {
    unimplemented!()
}

/// PG `apply_tlist_labeling`. See `crate::backend::optimizer::util::tlist`.
pub use crate::backend::optimizer::util::tlist::apply_tlist_labeling;

pub fn extract_grouping_ops(group_clause: &[SortGroupClause]) -> Vec<Oid> {
    unimplemented!()
}

pub fn extract_grouping_collations(group_clause: &[SortGroupClause], tlist: &[TargetEntry]) -> Vec<Oid> {
    unimplemented!()
}

pub fn extract_grouping_cols(group_clause: &[SortGroupClause], tlist: &[TargetEntry]) -> Vec<AttrNumber> {
    unimplemented!()
}

pub fn grouping_is_sortable(group_clause: &[SortGroupClause]) -> bool {
    unimplemented!()
}

pub fn grouping_is_hashable(group_clause: &[SortGroupClause]) -> bool {
    unimplemented!()
}

/// PG `make_pathtarget_from_tlist`. See `crate::backend::optimizer::util::tlist`.
pub use crate::backend::optimizer::util::tlist::make_pathtarget_from_tlist;

/// PG `make_tlist_from_pathtarget`. See `crate::backend::optimizer::util::tlist`.
pub use crate::backend::optimizer::util::tlist::make_tlist_from_pathtarget;

pub fn copy_pathtarget(src: &PathTarget) -> PathTarget {
    unimplemented!()
}

pub fn create_empty_pathtarget() -> PathTarget {
    unimplemented!()
}

pub fn add_column_to_pathtarget(target: &mut PathTarget, expr: Box<Expr>, sortgroupref: Index) {
    unimplemented!()
}

pub fn add_new_column_to_pathtarget(target: &mut PathTarget, expr: Box<Expr>) {
    unimplemented!()
}

pub fn add_new_columns_to_pathtarget(target: &mut PathTarget, exprs: &[Expr]) {
    unimplemented!()
}

pub fn apply_pathtarget_labeling_to_tlist(tlist: &mut [TargetEntry], target: &PathTarget) {
    unimplemented!()
}

/// out-params `targets`/`targets_contain_srfs` folded into the returned tuple.
pub fn split_pathtarget_at_srfs(
    root: &mut PlannerInfo,
    target: &PathTarget,
    input_target: &PathTarget,
) -> (Vec<PathTarget>, Vec<bool>) {
    unimplemented!()
}

/// out-params `targets`/`targets_contain_srfs` folded into the returned tuple.
pub fn split_pathtarget_at_srfs_grouping(
    root: &mut PlannerInfo,
    target: &PathTarget,
    input_target: &PathTarget,
) -> (Vec<PathTarget>, Vec<bool>) {
    unimplemented!()
}

/// Convenience macro: a PathTarget with valid cost/width fields.
pub fn create_pathtarget(root: &mut PlannerInfo, tlist: &[TargetEntry]) -> PathTarget {
    unimplemented!()
}

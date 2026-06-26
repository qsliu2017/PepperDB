//! Translated from PostgreSQL src/include/optimizer/restrictinfo.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Box<Node>/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::fn_params_excessive_bools, reason = "1:1 PG port: bool flags mirror PG C signature")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::bitmapset::{bms_is_subset, Bitmapset};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids, RestrictInfo};
use crate::nodes::primnodes::{Expr, Index};
use crate::postgres_ext::Oid;

pub fn make_plain_restrictinfo(
    root: &mut PlannerInfo,
    clause: Box<Expr>,
    orclause: Box<Expr>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Relids,
    incompatible_relids: Relids,
    outer_relids: Relids,
) -> RestrictInfo {
    unimplemented!()
}

pub fn make_restrictinfo(
    root: &mut PlannerInfo,
    clause: Box<Expr>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Option<Relids>,
    incompatible_relids: Option<Relids>,
    outer_relids: Option<Relids>,
) -> RestrictInfo {
    unimplemented!()
}

/// Convenience for the common case of a valid-everywhere qual.
pub fn make_simple_restrictinfo(root: &mut PlannerInfo, clause: Box<Expr>) -> RestrictInfo {
    make_restrictinfo(
        root, clause, true, false, false, false, 0, None, None, None,
    )
}

pub fn commute_restrictinfo(rinfo: &RestrictInfo, comm_op: Oid) -> RestrictInfo {
    unimplemented!()
}

pub fn restriction_is_or_clause(restrictinfo: &RestrictInfo) -> bool {
    unimplemented!()
}

pub fn restriction_is_securely_promotable(restrictinfo: &RestrictInfo, rel: &RelOptInfo) -> bool {
    unimplemented!()
}

pub fn get_actual_clauses(restrictinfo_list: &[RestrictInfo]) -> Vec<Expr> {
    unimplemented!()
}

pub fn extract_actual_clauses(restrictinfo_list: &[RestrictInfo], pseudoconstant: bool) -> Vec<Expr> {
    unimplemented!()
}

/// out-params `joinquals`/`otherquals` folded into the returned tuple.
pub fn extract_actual_join_clauses(
    restrictinfo_list: &[RestrictInfo],
    joinrelids: Relids,
) -> (Vec<Expr>, Vec<Expr>) {
    unimplemented!()
}

pub fn join_clause_is_movable_to(rinfo: &RestrictInfo, baserel: &RelOptInfo) -> bool {
    unimplemented!()
}

pub fn join_clause_is_movable_into(
    rinfo: &RestrictInfo,
    currentrelids: Relids,
    current_and_outer: Relids,
) -> bool {
    unimplemented!()
}

/// Determine whether a join clause is of the right form to use in this join.
///
/// We already know that the clause is a binary opclause referencing only the
/// rels in the current join.  The point here is to check whether it has the
/// form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
/// rather than mixing outer and inner vars on either side.  If it matches,
/// we set the transient flag outer_is_left to identify which side is which.
pub fn clause_sides_match_join(
    rinfo: &mut RestrictInfo,
    outerrelids: &Bitmapset,
    innerrelids: &Bitmapset,
) -> bool {
    let left = rinfo.left_relids.as_ref();
    let right = rinfo.right_relids.as_ref();
    let is_sub = |r: Option<&Relids>, b: &Bitmapset| r.is_none_or(|x| bms_is_subset(x, b));
    if is_sub(left, outerrelids) && is_sub(right, innerrelids) {
        rinfo.outer_is_left = true; // lefthand side is outer
        true
    } else if is_sub(left, innerrelids) && is_sub(right, outerrelids) {
        rinfo.outer_is_left = false; // righthand side is outer
        true
    } else {
        false // no good for these input relations
    }
}

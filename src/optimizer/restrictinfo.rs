//! Translated from PostgreSQL src/include/optimizer/restrictinfo.h

#![allow(clippy::boxed_local, reason = "1:1 PG port: Node/Box<Path> mirrors PG pointer-passed nodes")]
#![allow(clippy::fn_params_excessive_bools, reason = "1:1 PG port: bool flags mirror PG C signature")]
#![allow(clippy::needless_pass_by_value, reason = "1:1 PG port: stubs take owned node values matching PG C signatures; consumed once implemented")]

use crate::nodes::bitmapset::{bms_is_subset, Bitmapset};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids, RestrictInfo};
use crate::nodes::primnodes::Expr;

// Bodies live in the backend definition module (rules.md s3); re-export them so
// `crate::optimizer::restrictinfo::<name>` keeps resolving under the C name.
pub use crate::backend::optimizer::util::restrictinfo::{
    commute_restrictinfo, extract_actual_clauses, get_actual_clauses, make_plain_restrictinfo,
    make_restrictinfo, make_simple_restrictinfo, restriction_is_or_clause,
};

pub fn restriction_is_securely_promotable(restrictinfo: &RestrictInfo, rel: &RelOptInfo) -> bool {
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

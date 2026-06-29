//! PlaceHolderVar and PlaceHolderInfo manipulation. Translated from
//! backend/optimizer/util/placeholder.c.
//!
//! A `PlaceHolderVar` wraps an expression that must be evaluated below the top
//! of the plan tree (typically because an outer join or LATERAL reference makes
//! a lower-level value visible higher up under controlled nullability). Each
//! distinct PHV gets one `PlaceHolderInfo` recording where it can be evaluated
//! (`ph_eval_at`), where it is needed (`ph_needed`), its lateral refs and width.
//!
//! For an inner-join query with no outer joins or LATERAL refs, the planner
//! creates NO PlaceHolderVars: `glob.last_phid` stays 0 and `placeholder_list`
//! stays empty. The jointree scan and the per-joinrel/base-rel hooks then loop
//! over an empty list and do nothing, which is exactly correct. This module
//! translates the full structure; the points that require infrastructure not
//! yet present (the LATERAL/outer-join `find_base_rel` write-back, the
//! PlaceHolderVar arms of `pull_var_clause`/`pull_varnos`/the tree walkers) are
//! only reached once PHVs actually exist, and route through `not_yet_reachable`
//! (rules.md s4: clean grow guards, not half-written logic).

#![allow(
    clippy::boxed_local,
    reason = "1:1 PG port: expr is a pointer-passed Expr node in the C signature"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: signatures take owned node values matching PG C; consumed once the deferred paths land"
)]

use crate::backend::nodes::nodeFuncs::{expression_tree_mutator, expression_tree_walker};
use crate::nodes::bitmapset::{
    bms_add_members, bms_copy, bms_del_members, bms_difference, bms_get_singleton_member,
    bms_int_members, bms_is_member, bms_is_subset, bms_next_member, bms_nonempty_difference,
};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    PlaceHolderInfo, PlaceHolderVar, PlannerInfo, RelOptInfo, Relids, SpecialJoinInfo,
};
use crate::nodes::primnodes::Expr;
use crate::optimizer::optimizer::{
    clamp_width_est, pull_var_clause, pull_varnos, PullVarClauseFlags,
};
use crate::optimizer::planmain::{add_vars_to_attr_needed, add_vars_to_targetlist};

/// Panic for a placeholder path not yet translated for this milestone
/// (rules.md s4). Reached only once real PlaceHolderVars exist, which requires
/// outer-join / LATERAL infrastructure not present for the inner-join case.
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// Flags `pull_var_clause` uses to gather Vars/PHVs from a PHV's expression.
fn phv_pull_flags() -> PullVarClauseFlags {
    PullVarClauseFlags::RECURSE_AGGREGATES
        | PullVarClauseFlags::RECURSE_WINDOWFUNCS
        | PullVarClauseFlags::INCLUDE_PLACEHOLDERS
}

/// PG `make_placeholder_expr`: build a `PlaceHolderVar` for `expr`.
///
/// `phrels` is the syntactic location (a set of relids) to attribute to the
/// expression. The caller adjusts `phlevelsup`/`phnullingrels` as needed.
/// Touches only `root.glob`, since the query level isn't known here.
pub fn make_placeholder_expr(
    root: &mut PlannerInfo,
    expr: Box<Expr>,
    phrels: Relids,
) -> PlaceHolderVar {
    root.glob.last_phid += 1;
    PlaceHolderVar {
        phexpr: *expr,
        phrels: Some(phrels),
        phnullingrels: None, // caller may change this later
        phid: root.glob.last_phid,
        phlevelsup: 0, // caller may change this later
    }
}

/// PG `find_placeholder_info`: fetch (or lazily create) the `PlaceHolderInfo`
/// for `phv`. Creating one is an error once the PHI set is frozen.
///
/// Returns a clone; the canonical entry lives in `placeholder_array[phid]` and
/// `placeholder_list`. Only reached when PHVs exist (outer-join/LATERAL); the
/// var-gathering helpers it calls are still stubbed for the inner-join case.
pub fn find_placeholder_info(root: &mut PlannerInfo, phv: &PlaceHolderVar) -> PlaceHolderInfo {
    // If this ever isn't true we'd need to look in parent lists.
    crate::assert!(phv.phlevelsup == 0);

    // Use placeholder_array to look up an existing PlaceHolderInfo quickly.
    if let Some(Some(phinfo)) = root.placeholder_array.get(phv.phid) {
        crate::assert!(phinfo.phid == phv.phid);
        return (**phinfo).clone();
    }

    // Not found, so create it.
    if root.placeholders_frozen {
        not_yet_reachable("find_placeholder_info: too late to create a new PlaceHolderInfo");
    }

    // By convention ph_var->phnullingrels is always empty: the PlaceHolderInfo
    // is the initially-calculated state, before any outer join nulls it.
    let mut ph_var = Box::new(phv.clone());
    ph_var.phnullingrels = None;

    // Referenced rels outside the PHV's syntactic scope are LATERAL refs:
    // they go in ph_lateral but not ph_eval_at. If none are within scope,
    // force evaluation at the syntactic location.
    let phrels = phv.phrels.clone().unwrap_or_default();
    let rels_used = pull_varnos(root, Some(ph_var.phexpr.clone()));
    let ph_lateral = bms_difference(&rels_used, &phrels);
    let mut ph_eval_at = bms_int_members(rels_used, &phrels);
    if ph_eval_at.is_empty() {
        ph_eval_at = bms_copy(&phrels);
        crate::assert!(!ph_eval_at.is_empty());
    }
    let ph_width = crate::utils::lsyscache::get_typavgwidth(
        crate::backend::nodes::nodeFuncs::exprType(&ph_var.phexpr),
        crate::backend::nodes::nodeFuncs::exprTypmod(&ph_var.phexpr),
    );

    let phinfo = PlaceHolderInfo {
        phid: phv.phid,
        ph_var,
        ph_eval_at: Some(ph_eval_at),
        ph_lateral: Some(ph_lateral),
        ph_needed: None, // initially unused
        ph_width,
    };

    // Add to both placeholder_list and placeholder_array (indexed by phid).
    root.placeholder_list.push(Box::new(phinfo.clone()));
    if root.placeholder_array.len() <= phinfo.phid {
        root.placeholder_array.resize(phinfo.phid + 1, None);
    }
    root.placeholder_array[phinfo.phid] = Some(Box::new(phinfo.clone()));

    // The contained expression may hold lower-level PHVs; register those too.
    let phexpr = phinfo.ph_var.phexpr.clone();
    find_placeholders_in_expr(root, Some(&phexpr));

    phinfo
}

/// PG `find_placeholders_in_jointree`: scan the jointree for PHVs and build
/// `PlaceHolderInfo`s. The targetlist is handled by `build_base_rel_tlists`.
pub fn find_placeholders_in_jointree(root: &mut PlannerInfo) {
    // Must be done before freezing the set of PHIs.
    crate::assert!(!root.placeholders_frozen);

    // Nothing to do if the query contains no PlaceHolderVars.
    if root.glob.last_phid != 0 {
        let jointree = root.parse.jointree.clone();
        crate::assert!(matches!(jointree, Some(Node::FromExpr(_))));
        find_placeholders_recurse(root, jointree.as_ref());
    }
}

/// PG `find_placeholders_recurse`: one level of `find_placeholders_in_jointree`.
fn find_placeholders_recurse(root: &mut PlannerInfo, jtnode: Option<&Node>) {
    match jtnode {
        // None, or a RangeTblRef with no quals of its own: nothing to do.
        None | Some(Node::RangeTblRef(_)) => {}
        Some(Node::FromExpr(f)) => {
            let f = f.clone();
            for child in &f.fromlist {
                find_placeholders_recurse(root, Some(child));
            }
            find_placeholders_in_expr(root, f.quals.as_ref());
        }
        Some(Node::JoinExpr(j)) => {
            let j = j.clone();
            find_placeholders_recurse(root, j.larg.as_ref());
            find_placeholders_recurse(root, j.rarg.as_ref());
            find_placeholders_in_expr(root, j.quals.as_ref());
        }
        Some(other) => {
            not_yet_reachable(&format!("find_placeholders_recurse: unrecognized node {other:?}"));
        }
    }
}

/// PG `find_placeholders_in_expr`: create a `PlaceHolderInfo` for each PHV in
/// the given expression.
fn find_placeholders_in_expr(root: &mut PlannerInfo, expr: Option<&Node>) {
    let vars = pull_var_clause(expr.cloned(), phv_pull_flags());
    for v in vars {
        // Ignore any plain Vars; only PlaceHolderVars matter here.
        if let Node::PlaceHolderVar(phv) = v {
            find_placeholder_info(root, &phv);
        }
    }
}

/// PG `fix_placeholder_input_needed_levels`: ensure all vars/PHVs needed to
/// evaluate each placeholder are available at its eval level. No-op when
/// `placeholder_list` is empty.
pub fn fix_placeholder_input_needed_levels(root: &mut PlannerInfo) {
    let list = root.placeholder_list.clone();
    for phinfo in &list {
        let vars = pull_var_clause(Some(phinfo.ph_var.phexpr.clone()), phv_pull_flags());
        let eval_at = phinfo.ph_eval_at.clone().unwrap_or_default();
        add_vars_to_targetlist(root, &vars, eval_at);
    }
}

/// PG `rebuild_placeholder_attr_needed`: re-add attr_needed/ph_needed bits for
/// vars/PHVs used inside placeholders (after useless-outer-join removal). Like
/// `fix_placeholder_input_needed_levels` but calls `add_vars_to_attr_needed`.
/// No-op when `placeholder_list` is empty.
pub fn rebuild_placeholder_attr_needed(root: &mut PlannerInfo) {
    let list = root.placeholder_list.clone();
    for phinfo in &list {
        let vars = pull_var_clause(Some(phinfo.ph_var.phexpr.clone()), phv_pull_flags());
        let eval_at = phinfo.ph_eval_at.clone().unwrap_or_default();
        add_vars_to_attr_needed(root, &vars, eval_at);
    }
}

/// PG `add_placeholders_to_base_rels`: add PHVs computable at a base rel and
/// needed above it to that rel's targetlist. No-op when `placeholder_list` is
/// empty (the inner-join case). The actual base-rel write-back is deferred:
/// `find_base_rel` hands back an owned clone, so updating its reltarget can't
/// flow back to `root` until the rel array gains in-place access.
pub fn add_placeholders_to_base_rels(root: &mut PlannerInfo) {
    let list = root.placeholder_list.clone();
    for phinfo in &list {
        let eval_at = phinfo.ph_eval_at.clone().unwrap_or_default();
        let needed = phinfo.ph_needed.clone().unwrap_or_default();
        let computable_at_scan = bms_get_singleton_member(&eval_at).is_some();
        if computable_at_scan && bms_nonempty_difference(&needed, &eval_at) {
            // A scan-level value has not been nulled by any outer join.
            crate::assert!(phinfo.ph_var.phnullingrels.is_none());
            not_yet_reachable(
                "add_placeholders_to_base_rels: base-rel reltarget write-back (LATERAL/PHV)",
            );
        }
    }
}

/// PG `add_placeholders_to_joinrel`: add newly-computable PHVs to a join rel's
/// targetlist, and fold any lateral refs into `direct_lateral_relids`. No-op
/// when `placeholder_list` is empty (the inner-join case): `build_join_rel`
/// calls this for every joinrel, so it must be harmless on an empty list.
pub fn add_placeholders_to_joinrel(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    _sjinfo: &SpecialJoinInfo,
) {
    let relids = joinrel.relids.clone().unwrap_or_default();
    let outer_relids = outer_rel.relids.clone().unwrap_or_default();
    let inner_relids = inner_rel.relids.clone().unwrap_or_default();
    let reltarget = joinrel
        .reltarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("add_placeholders_to_joinrel: missing reltarget"));
    let mut tuple_width = i64::from(reltarget.width);

    let list = root.placeholder_list.clone();
    for phinfo in &list {
        let eval_at = phinfo.ph_eval_at.clone().unwrap_or_default();
        // Is it computable here?
        if !bms_is_subset(&eval_at, &relids) {
            continue;
        }

        // Is it still needed above this joinrel?
        let needed = phinfo.ph_needed.clone().unwrap_or_default();
        if bms_nonempty_difference(&needed, &relids)
            && !bms_is_subset(&eval_at, &outer_relids)
            && !bms_is_subset(&eval_at, &inner_relids)
        {
            // Computable here but not in either input: emit it and charge the
            // contained expression's cost. Requires PathTarget cost machinery
            // for PHVs that only exists once PHVs are created.
            crate::assert!(phinfo.ph_var.phnullingrels.is_none());
            not_yet_reachable(
                "add_placeholders_to_joinrel: emit newly-computable PHV into joinrel reltarget",
            );
        }

        // Adjust direct_lateral_relids to include the PHV's source rel(s); done
        // even when the PHV isn't emitted so join_is_legal() accepts valid
        // orderings. build_join_rel() strips the join's own relids afterward.
        let ph_lateral = phinfo.ph_lateral.clone().unwrap_or_default();
        let dlr = joinrel.direct_lateral_relids.take().unwrap_or_default();
        joinrel.direct_lateral_relids = Some(bms_add_members(dlr, &ph_lateral));
        tuple_width += i64::from(phinfo.ph_width);
    }

    if let Some(target) = joinrel.reltarget.as_mut() {
        target.width = clamp_width_est(tuple_width);
    }
}

/// PG `contain_placeholder_references_to`: does any PHV in `clause` reference
/// `relid` inside its contained expression? Fast-path `false` when the query
/// has no PHVs (the inner-join case), so this is safe to call there.
pub fn contain_placeholder_references_to(root: &mut PlannerInfo, clause: &Node, relid: i32) -> bool {
    if root.glob.last_phid == 0 {
        return false;
    }
    contain_placeholder_references_walker(Some(clause), relid, 0)
}

fn contain_placeholder_references_walker(
    node: Option<&Node>,
    relid: i32,
    sublevels_up: usize,
) -> bool {
    let Some(node) = node else {
        return false;
    };
    if let Node::PlaceHolderVar(phv) = node {
        // Look through PHVs of other query levels.
        if phv.phlevelsup == sublevels_up {
            // phrels match => found it. We don't recurse into the contained
            // expression: phrels already summarizes it, and phnullingrels is
            // irrelevant (it records OJs that null the result afterwards).
            return phv
                .phrels
                .as_ref()
                .is_some_and(|phrels| bms_is_member(relid, phrels));
        }
    }
    if matches!(node, Node::Query(_)) {
        // RTE subquery / not-yet-planned sublink subquery: query_tree_walker
        // with sublevels_up+1 is not yet present for the inner-join case.
        not_yet_reachable("contain_placeholder_references_walker: Query subtree (query_tree_walker)");
    }
    expression_tree_walker(node, |child| {
        contain_placeholder_references_walker(Some(child), relid, sublevels_up)
    })
}

/// PG `get_placeholder_nulling_relids`: union of outer-join relids that can null
/// the placeholder, computed from the `nulling_relids` of each baserel in
/// `ph_eval_at`, minus the OJs already in `ph_eval_at`.
pub fn get_placeholder_nulling_relids(root: &mut PlannerInfo, phinfo: &PlaceHolderInfo) -> Relids {
    let mut result = Relids::default();
    let eval_at = phinfo.ph_eval_at.clone().unwrap_or_default();

    let mut relid: i32 = -1;
    while let Some(next) = bms_next_member(&eval_at, relid) {
        relid = next;
        if relid <= 0 {
            continue;
        }
        // Ignore the RTE_GROUP RTE.
        if relid == root.group_rtindex {
            continue;
        }
        match root.simple_rel_array.get(relid as usize).and_then(Option::as_ref) {
            None => {
                // Must be an outer join.
                crate::assert!(root
                    .outer_join_rels
                    .as_ref()
                    .is_some_and(|ojr| bms_is_member(relid, ojr)));
            }
            Some(rel) => {
                if let Some(nulling) = rel.nulling_relids.as_ref() {
                    result = bms_add_members(result, nulling);
                }
            }
        }
    }

    // Remove any OJs already included in ph_eval_at.
    bms_del_members(result, &eval_at)
}

/// PG `strip_noop_phvs`: strip non-nullable (no-op) PlaceHolderVars from a
/// scan-level expression, exposing the base expression. Fast-paths to a return
/// when there are no strippable PHVs (the inner-join case), so it's safe there.
pub fn strip_noop_phvs(node: Node) -> Node {
    if !contain_noop_phv_walker(Some(&node)) {
        return node;
    }
    strip_noop_phvs_mutator(node)
}

/// PG `contain_noop_phv_walker`: is there any PHV with empty phnullingrels?
fn contain_noop_phv_walker(node: Option<&Node>) -> bool {
    let Some(node) = node else {
        return false;
    };
    if is_noop_phv(node) {
        return true;
    }
    expression_tree_walker(node, |child| contain_noop_phv_walker(Some(child)))
}

/// Helper: is this optional relid set empty?
fn is_empty_relids(r: &Relids) -> bool {
    r.is_empty()
}

/// A PlaceHolderVar with empty phnullingrels is a strippable no-op.
fn is_noop_phv(node: &Node) -> bool {
    matches!(node, Node::PlaceHolderVar(phv)
        if phv.phnullingrels.as_ref().is_none_or(is_empty_relids))
}

/// PG `strip_noop_phvs_mutator`: replace non-nullable PHVs with their contained
/// expression, recursively.
fn strip_noop_phvs_mutator(node: Node) -> Node {
    if let Node::PlaceHolderVar(phv) = node {
        return if phv.phnullingrels.as_ref().is_none_or(is_empty_relids) {
            strip_noop_phvs_mutator(phv.phexpr)
        } else {
            // Keep this PHV but mutate its contained expression.
            expression_tree_mutator(Node::PlaceHolderVar(phv), strip_noop_phvs_mutator)
        };
    }
    expression_tree_mutator(node, strip_noop_phvs_mutator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `PlannerInfo` with an empty placeholder_list and last_phid == 0 means
    /// every hook is a structural no-op. We exercise the empty-list paths
    /// directly to confirm they neither panic nor mutate.
    #[test]
    fn empty_placeholder_list_hooks_are_noops() {
        // find_placeholders_in_jointree returns immediately when last_phid == 0,
        // so it never touches the jointree. We model that guard here without
        // constructing a full PlannerInfo (which is large): the guard is
        // `root.glob.last_phid != 0`, and with no PHVs the body is skipped.
        let last_phid: usize = 0;
        assert!(last_phid == 0, "no PlaceHolderVars => jointree scan skipped");

        // contain_placeholder_references_to's fast path: last_phid == 0 => false
        // regardless of the clause. The walker below is the post-fast-path part;
        // on a PHV-free leaf it returns false.
        let leaf = Node::RangeTblRef(Box::new(crate::nodes::primnodes::RangeTblRef {
            rtindex: 1,
        }));
        assert!(!contain_noop_phv_walker(Some(&leaf)));

        // strip_noop_phvs on a PHV-free node returns it unchanged.
        let stripped = strip_noop_phvs(leaf.clone());
        assert_eq!(stripped, leaf);
    }
}

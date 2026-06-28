//! Post-processing of the executor's plan tree: flatten the rangetable and
//! adjust Var references to match. Translated from
//! backend/optimizer/plan/setrefs.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::planmain` (the C declaration lives in
//! planmain.h) under the C name.
//!
//! Disposition: `grow`. M1's live path is `set_plan_references` over a childless
//! `Result` plan: there is no rangetable to flatten (the FROM-less SELECT has an
//! empty rtable), and `set_plan_refs`' `T_Result` arm over a const targetlist is
//! identity plus the plan_node_id assignment (no Vars to offset, no ROWID_VARs,
//! resconstantqual is NULL). The per-node-tag `set_plan_refs` switch and the
//! rangetable-flattening / rowmark / appendrel / subplan handling are grow guards
//! (rules.md s4) and grow per milestone.

#![allow(
    clippy::boxed_local,
    clippy::unnecessary_box_returns,
    reason = "1:1 PG port: set_plan_refs takes/returns the polymorphic plan node by pointer (Plan* -> Box<Node>)"
)]

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::plannodes::Result;

/// Panic for a setrefs path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `set_plan_references`: prepare the plan tree for execution by flattening
/// the rangetable into `glob->finalrtable` (offsetting RT indexes) and fixing up
/// Var references throughout the plan.
///
/// `plan` is the polymorphic top plan node. M1's only plan node is a `Result`.
pub fn set_plan_references(root: &mut PlannerInfo, plan: Box<Node>) -> Box<Node> {
    let rtoffset = root.glob.finalrtable.len();

    // add_rtes_to_flat_rtable: append this query's RTEs to the flat rangetable.
    // M1's rangetable is empty (a table-less SELECT), so nothing is added and the
    // final rtable stays empty. The RTE-flattening (and the rowmark / appendrel /
    // AlternativeSubPlan workspace handling) grows with the rangetable machinery.
    if !root.parse.rtable.is_empty() {
        not_yet_reachable("set_plan_references: rangetable flattening");
    }
    if !root.row_marks.is_empty() || !root.append_rel_list.is_empty() {
        not_yet_reachable("set_plan_references: rowmarks / appendrels");
    }
    crate::assert!(rtoffset == 0);

    set_plan_refs(root, plan, rtoffset)
}

/// PG `set_plan_refs`: per-node-tag fixup of a single plan node and its subtree.
/// M1 lives the `T_Result` arm; the rest grow per milestone.
fn set_plan_refs(root: &mut PlannerInfo, plan: Box<Node>, rtoffset: usize) -> Box<Node> {
    match *plan {
        Node::Result(r) => Box::new(Node::Result(Box::new(set_result_refs(root, *r, rtoffset)))),
        other => not_yet_reachable(&format!("set_plan_refs: {other:?}")),
    }
}

fn set_result_refs(root: &mut PlannerInfo, mut plan: Result, _rtoffset: usize) -> Result {
    // Assign this node a unique ID.
    plan.plan.plan_node_id = root.glob.last_plan_node_id;
    root.glob.last_plan_node_id += 1;

    if plan.plan.lefttree.is_some() {
        // A Result with a subplan is an upper node (set_upper_references); M1's
        // Result is childless.
        not_yet_reachable("set_plan_refs: Result with subplan");
    }

    // Childless Result: fix_scan_list over the tlist. For a const tlist there are
    // no Vars (and no ROWID_VARs) to rewrite, so this is identity. resconstantqual
    // is NULL on the const path.
    fix_scan_tlist_identity(&plan.plan.targetlist);
    crate::assert!(plan.plan.qual.is_empty());
    crate::assert!(plan.resconstantqual.is_none());

    plan
}

/// `fix_scan_list` over a const targetlist is identity: assert there are no Vars
/// (which would need RT-index offsetting / Param replacement that grows later).
fn fix_scan_tlist_identity(tlist: &[Box<Node>]) {
    for entry in tlist {
        let Node::TargetEntry(te) = &**entry else {
            not_yet_reachable("set_plan_refs: tlist entry is not a TargetEntry");
        };
        match te.expr.as_deref() {
            Some(Node::Const(_)) | None => {}
            Some(_) => not_yet_reachable("set_plan_refs: non-Const expr in scan tlist"),
        }
    }
}

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

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::plannodes::{Result, SeqScan};

/// Panic for a setrefs path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `set_plan_references`: prepare the plan tree for execution by flattening
/// the rangetable into `glob->finalrtable` (offsetting RT indexes) and fixing up
/// Var references throughout the plan.
///
/// `plan` is the polymorphic top plan node. M1's plan is a `Result` (empty
/// rangetable); M2 adds a `SeqScan` over a single base-rel rangetable. With one
/// query the rtoffset is 0, so the flattened indexes equal the originals.
pub fn set_plan_references(root: &mut PlannerInfo, plan: Node) -> Node {
    let rtoffset = root.glob.finalrtable.len();
    crate::assert!(rtoffset == 0);

    if !root.row_marks.is_empty() || !root.append_rel_list.is_empty() {
        not_yet_reachable("set_plan_references: rowmarks / appendrels");
    }

    // add_rtes_to_flat_rtable: append this query's RTEs (and their perminfos) to
    // the flat rangetable. With a single query the indexes are unchanged. The
    // per-RTE field scrubbing PG does (dropping subquery/joinaliasvars detail) is
    // not needed for the M2 RTE_RELATION entry.
    add_rtes_to_flat_rtable(root);

    set_plan_refs(root, plan, rtoffset)
}

/// PG `add_rtes_to_flat_rtable` (M2 subset): copy the query's RTEs and perminfos
/// into the global flat rangetable. Single-query, so a straight append.
fn add_rtes_to_flat_rtable(root: &mut PlannerInfo) {
    let rtable = root.parse.rtable.clone();
    let perminfos = root.parse.rteperminfos.clone();
    root.glob.finalrtable.extend(rtable);
    root.glob.finalrteperminfos.extend(perminfos);
}

/// PG `set_plan_refs`: per-node-tag fixup of a single plan node and its subtree.
/// M1 lives the `T_Result` arm; M2 adds `T_SeqScan`; the rest grow per milestone.
fn set_plan_refs(root: &mut PlannerInfo, plan: Node, rtoffset: usize) -> Node {
    match plan {
        Node::Result(r) => Node::Result(Box::new(set_result_refs(root, *r, rtoffset))),
        Node::SeqScan(s) => Node::SeqScan(Box::new(set_seqscan_refs(root, *s, rtoffset))),
        Node::ModifyTable(m) => Node::ModifyTable(Box::new(set_modifytable_refs(root, *m, rtoffset))),
        other => not_yet_reachable(&format!("set_plan_refs: {other:?}")),
    }
}

/// PG `set_plan_refs` T_ModifyTable arm (M2 subset): offset the result-relation RT
/// indexes, assign the plan node id, and recurse into the source subplan. RETURNING
/// tlist fixup / WCO / ON CONFLICT / per-target lists grow at their milestones.
fn set_modifytable_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::ModifyTable,
    rtoffset: usize,
) -> crate::nodes::plannodes::ModifyTable {
    plan.plan.plan_node_id = root.glob.last_plan_node_id;
    root.glob.last_plan_node_id += 1;

    let off = rtoffset as crate::nodes::primnodes::Index;
    plan.nominal_relation += off;
    if plan.root_relation != 0 {
        plan.root_relation += off;
    }
    plan.result_relations = plan
        .result_relations
        .iter()
        .map(|&rti| rti + rtoffset as i32)
        .collect();

    // Recurse into the source subplan.
    if let Some(sub) = plan.plan.lefttree.take() {
        plan.plan.lefttree = Some(set_plan_refs(root, sub, rtoffset));
    }
    plan
}

/// PG `set_plan_refs` T_SeqScan arm: offset the scanrelid and fix the scan's
/// targetlist/qual Var references. With rtoffset 0 (single query) the scanrelid
/// and base-rel Var varnos are unchanged; this assigns the plan node id and
/// asserts the tlist is well-formed (Vars over the scan rel, or Consts).
fn set_seqscan_refs(root: &mut PlannerInfo, mut plan: SeqScan, rtoffset: usize) -> SeqScan {
    plan.scan.scanrelid += rtoffset as crate::nodes::primnodes::Index;
    plan.scan.plan.plan_node_id = root.glob.last_plan_node_id;
    root.glob.last_plan_node_id += 1;

    // fix_scan_list over the tlist + qual: with rtoffset 0, a base-rel Var keeps
    // its varno/varattno and every other expr (Const/OpExpr/FuncExpr/BoolExpr) is
    // unchanged; the only fixups (ROWID_VAR / Param / upper-var offsetting) are not
    // present on the M3 scan plan.
    fix_scan_tlist_identity(&plan.scan.plan.targetlist, rtoffset);
    fix_scan_qual_identity(&plan.scan.plan.qual, rtoffset);
    plan
}

fn set_result_refs(root: &mut PlannerInfo, mut plan: Result, rtoffset: usize) -> Result {
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
    fix_scan_tlist_identity(&plan.plan.targetlist, rtoffset);
    crate::assert!(plan.plan.qual.is_empty());
    crate::assert!(plan.resconstantqual.is_none());

    plan
}

/// `fix_scan_list` over a scan/result targetlist. With `rtoffset == 0` the fixup
/// is identity: a base-rel `Var` keeps its varno/varattno and every other expr
/// folds to itself. A non-zero offset needs the general `fix_scan_expr` walk
/// (ROWID_VAR / Param / upper-var offsetting) that grows with multi-query plans.
fn fix_scan_tlist_identity(tlist: &[Node], rtoffset: usize) {
    if rtoffset != 0 {
        not_yet_reachable("set_plan_refs: non-zero rtoffset Var fixup");
    }
    for entry in tlist {
        let Node::TargetEntry(te) = entry else {
            not_yet_reachable("set_plan_refs: tlist entry is not a TargetEntry");
        };
        fix_scan_expr_identity(te.expr.as_ref());
    }
}

/// `fix_scan_list` over a scan node's qual (an implicit-AND list of clauses).
/// Identity at `rtoffset == 0`, like the tlist fixup.
fn fix_scan_qual_identity(qual: &[Node], rtoffset: usize) {
    if rtoffset != 0 {
        not_yet_reachable("set_plan_refs: non-zero rtoffset qual fixup");
    }
    for clause in qual {
        fix_scan_expr_identity(Some(clause));
    }
}

/// `fix_scan_expr` (identity at rtoffset 0): validate that an expression contains
/// only the node kinds reachable on the M3 scan plan (Var/Const/OpExpr/FuncExpr/
/// BoolExpr); recurse to assert no surprising node deeper in. Var offsetting,
/// PlaceHolderVar/Param/ROWID_VAR rewriting grow with multi-query plans.
fn fix_scan_expr_identity(expr: Option<&Node>) {
    let Some(expr) = expr else { return };
    match expr {
        Node::Const(_) | Node::Var(_) => {}
        Node::OpExpr(op) => op.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        Node::FuncExpr(f) => f.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        Node::BoolExpr(b) => b.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        other => not_yet_reachable(&format!("set_plan_refs: unexpected scan expr {other:?}")),
    }
}

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
use crate::nodes::plannodes::{Agg, Limit, Result, SeqScan, Sort, Unique};
use crate::nodes::primnodes::OUTER_VAR;

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
        Node::Agg(a) => Node::Agg(Box::new(set_agg_refs(root, *a, rtoffset))),
        Node::Sort(s) => Node::Sort(Box::new(set_sort_refs(root, *s, rtoffset))),
        Node::Unique(u) => Node::Unique(Box::new(set_unique_refs(root, *u, rtoffset))),
        Node::Limit(l) => Node::Limit(Box::new(set_limit_refs(root, *l, rtoffset))),
        other => not_yet_reachable(&format!("set_plan_refs: {other:?}")),
    }
}

/// PG `set_plan_refs` T_Agg arm + `set_upper_references`: recurse into the child,
/// then rewrite the Agg's tlist Vars to reference the child (subplan) output by
/// position (OUTER_VAR), and assign each Aggref its sequential `aggno`. (M5: no
/// partial-agg combine, no HAVING qual, no grouping sets.)
fn set_agg_refs(root: &mut PlannerInfo, mut plan: Agg, rtoffset: usize) -> Agg {
    crate::assert!(plan.chain.is_empty() && plan.grouping_sets.is_empty());
    let child = plan
        .plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_plan_refs: Agg without child"));
    let child = set_plan_refs(root, child, rtoffset);
    let child_tlist = plan_tlist(&child).to_vec();

    plan.plan.plan_node_id = next_plan_node_id(root);
    fix_upper_tlist(&mut plan.plan.targetlist, &child_tlist);
    crate::assert!(plan.plan.qual.is_empty(), "set_plan_refs: Agg HAVING qual not yet reachable");
    assign_agg_nos(&mut plan.plan.targetlist);
    plan.plan.lefttree = Some(child);
    plan
}

/// PG `set_plan_refs` T_Sort arm: recurse into the child, assign the node id; the
/// Sort's tlist is its child's (a passthrough), so its Var refs are already
/// child-relative (no rewrite needed). The sortColIdx are child output positions.
fn set_sort_refs(root: &mut PlannerInfo, mut plan: Sort, rtoffset: usize) -> Sort {
    let child = plan
        .plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_plan_refs: Sort without child"));
    let child = set_plan_refs(root, child, rtoffset);
    let child_tlist = plan_tlist(&child).to_vec();
    plan.plan.plan_node_id = next_plan_node_id(root);
    // A Sort projects nothing; its tlist mirrors the child output. Rewrite any Vars
    // to OUTER_VAR positions for faithfulness (identity when already positional).
    fix_upper_tlist(&mut plan.plan.targetlist, &child_tlist);
    plan.plan.lefttree = Some(child);
    plan
}

/// PG `set_plan_refs` T_Unique arm: like Sort -- recurse, assign id, passthrough tlist.
fn set_unique_refs(root: &mut PlannerInfo, mut plan: Unique, rtoffset: usize) -> Unique {
    let child = plan
        .plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_plan_refs: Unique without child"));
    let child = set_plan_refs(root, child, rtoffset);
    let child_tlist = plan_tlist(&child).to_vec();
    plan.plan.plan_node_id = next_plan_node_id(root);
    fix_upper_tlist(&mut plan.plan.targetlist, &child_tlist);
    plan.plan.lefttree = Some(child);
    plan
}

/// PG `set_plan_refs` T_Limit arm: recurse, assign id; passthrough tlist. The
/// OFFSET/COUNT exprs are Consts (no Vars to fix).
fn set_limit_refs(root: &mut PlannerInfo, mut plan: Limit, rtoffset: usize) -> Limit {
    let child = plan
        .plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_plan_refs: Limit without child"));
    let child = set_plan_refs(root, child, rtoffset);
    let child_tlist = plan_tlist(&child).to_vec();
    plan.plan.plan_node_id = next_plan_node_id(root);
    fix_upper_tlist(&mut plan.plan.targetlist, &child_tlist);
    plan.plan.lefttree = Some(child);
    plan
}

/// Allocate the next plan node id (PG's `glob->last_plan_node_id`).
fn next_plan_node_id(root: &mut PlannerInfo) -> i32 {
    let id = root.glob.last_plan_node_id;
    root.glob.last_plan_node_id += 1;
    id
}

/// The output targetlist of a plan node (`Plan.targetlist`).
fn plan_tlist(plan: &Node) -> &[Node] {
    match plan {
        Node::Result(r) => &r.plan.targetlist,
        Node::SeqScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        other => not_yet_reachable(&format!("set_plan_refs: child tlist of {other:?}")),
    }
}

/// PG `set_upper_references` core: rewrite every `Var` in an upper node's tlist to
/// reference the subplan's output by position (varno OUTER_VAR, varattno = the child
/// output column holding that Var). The executor reads the rewritten `varattno`
/// directly into the child tuple. Aggref-argument Vars are rewritten too.
fn fix_upper_tlist(tlist: &mut [Node], child_tlist: &[Node]) {
    for entry in tlist.iter_mut() {
        let Node::TargetEntry(te) = entry else { continue };
        if let Some(expr) = te.expr.take() {
            te.expr = Some(fix_upper_expr(expr, child_tlist));
        }
    }
}

/// Rewrite the Vars in `expr` to OUTER_VAR positions over the child output. The
/// M5-reachable upper expressions are bare Vars (grouping/sort columns), Aggrefs
/// (whose argument Vars are rewritten), and Consts.
fn fix_upper_expr(expr: Node, child_tlist: &[Node]) -> Node {
    match expr {
        Node::Var(mut v) => {
            let pos = child_position_of_var(child_tlist, v.varno, v.varattno).unwrap_or_else(|| {
                not_yet_reachable("set_upper_references: Var not found in subplan output")
            });
            v.varno = OUTER_VAR;
            v.varattno = pos;
            Node::Var(v)
        }
        Node::Aggref(mut agg) => {
            // Rewrite the aggregate's argument Vars (TargetEntry-wrapped) to point at
            // the child output. The Aggref node itself stays in the Agg tlist.
            agg.args = agg
                .args
                .into_iter()
                .map(|a| match a {
                    Node::TargetEntry(mut te) => {
                        if let Some(inner) = te.expr.take() {
                            te.expr = Some(fix_upper_expr(inner, child_tlist));
                        }
                        Node::TargetEntry(te)
                    }
                    other => fix_upper_expr(other, child_tlist),
                })
                .collect();
            Node::Aggref(agg)
        }
        other => other,
    }
}

/// The child output column position (1-based) holding the base-rel Var
/// (`varno`,`varattno`), or None.
fn child_position_of_var(
    child_tlist: &[Node],
    varno: i32,
    varattno: crate::access::attnum::AttrNumber,
) -> Option<crate::access::attnum::AttrNumber> {
    for n in child_tlist {
        let Node::TargetEntry(te) = n else { continue };
        match te.expr.as_ref() {
            // The child entry is itself a base-rel Var: match on varno/varattno.
            Some(Node::Var(cv)) if cv.varno == varno && cv.varattno == varattno => {
                return Some(te.resno);
            }
            // The child entry is an OUTER_VAR (already-rewritten lower upper node):
            // match on the rewritten attno.
            Some(Node::Var(cv)) if cv.varno == OUTER_VAR && cv.varattno == varattno => {
                return Some(te.resno);
            }
            _ => {}
        }
    }
    None
}

/// PG `set_upper_references` Aggref `aggno` assignment: number the Aggrefs in the
/// Agg's tlist 0..n in resno order (nodeAgg resolves them positionally).
fn assign_agg_nos(tlist: &mut [Node]) {
    let mut aggno = 0;
    for n in tlist.iter_mut() {
        let Node::TargetEntry(te) = n else { continue };
        if let Some(Node::Aggref(agg)) = te.expr.as_mut() {
            agg.aggno = aggno;
            agg.aggtransno = aggno;
            aggno += 1;
        }
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
        Node::Const(_) | Node::Var(_) | Node::CaseTestExpr(_) => {}
        Node::OpExpr(op) | Node::NullIfExpr(op) => {
            op.args.iter().for_each(|a| fix_scan_expr_identity(Some(a)));
        }
        Node::FuncExpr(f) => f.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        Node::BoolExpr(b) => b.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        // M4 (step 23): casts + conditional expressions -- recurse into children.
        Node::RelabelType(r) => fix_scan_expr_identity(r.arg.as_ref()),
        Node::CoerceViaIO(c) => fix_scan_expr_identity(c.arg.as_ref()),
        Node::CaseExpr(c) => {
            fix_scan_expr_identity(c.arg.as_ref());
            for arm in &c.args {
                fix_scan_expr_identity(Some(arm));
            }
            fix_scan_expr_identity(c.defresult.as_ref());
        }
        Node::CaseWhen(w) => {
            fix_scan_expr_identity(w.expr.as_ref());
            fix_scan_expr_identity(w.result.as_ref());
        }
        Node::CoalesceExpr(c) => c.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        Node::MinMaxExpr(m) => m.args.iter().for_each(|a| fix_scan_expr_identity(Some(a))),
        other => not_yet_reachable(&format!("set_plan_refs: unexpected scan expr {other:?}")),
    }
}

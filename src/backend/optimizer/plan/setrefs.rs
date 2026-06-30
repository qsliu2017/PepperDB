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
use crate::nodes::plannodes::{Agg, HashJoin, Limit, MergeJoin, NestLoop, Result, SeqScan, Sort, Unique};
use crate::nodes::primnodes::{INNER_VAR, OUTER_VAR};

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

    if !root.append_rel_list.is_empty() {
        not_yet_reachable("set_plan_references: appendrels");
    }
    // Row marks (FOR UPDATE) offset their RT index by rtoffset (0 with one query).
    // The finalrowmarks were already published by preprocess_rowmarks; with rtoffset
    // 0 they need no adjustment.

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
        Node::IndexScan(s) => Node::IndexScan(Box::new(set_indexscan_refs(root, *s, rtoffset))),
        Node::BitmapHeapScan(s) => {
            Node::BitmapHeapScan(Box::new(set_bitmap_heapscan_refs(root, *s, rtoffset)))
        }
        Node::BitmapIndexScan(s) => {
            Node::BitmapIndexScan(Box::new(set_bitmap_indexscan_refs(root, *s, rtoffset)))
        }
        Node::ModifyTable(m) => Node::ModifyTable(Box::new(set_modifytable_refs(root, *m, rtoffset))),
        Node::LockRows(l) => Node::LockRows(Box::new(set_lockrows_refs(root, *l, rtoffset))),
        Node::NestLoop(n) => Node::NestLoop(Box::new(set_nestloop_refs(root, *n, rtoffset))),
        Node::MergeJoin(m) => Node::MergeJoin(Box::new(set_mergejoin_refs(root, *m, rtoffset))),
        Node::HashJoin(h) => Node::HashJoin(Box::new(set_hashjoin_refs(root, *h, rtoffset))),
        Node::Hash(h) => Node::Hash(Box::new(set_hash_refs(root, *h, rtoffset))),
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
        Node::IndexScan(s) => &s.scan.plan.targetlist,
        Node::IndexOnlyScan(s) => &s.scan.plan.targetlist,
        Node::BitmapHeapScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        Node::NestLoop(n) => &n.join.plan.targetlist,
        Node::MergeJoin(m) => &m.join.plan.targetlist,
        Node::HashJoin(h) => &h.join.plan.targetlist,
        Node::Hash(h) => &h.plan.targetlist,
        other => not_yet_reachable(&format!("set_plan_refs: child tlist of {other:?}")),
    }
}

/// PG `set_plan_refs` T_NestLoop arm + `set_join_references`: recurse into the
/// outer/inner subplans, then rewrite the join's targetlist + joinqual Vars to
/// reference the outer (OUTER_VAR) / inner (INNER_VAR) child output by position. M7
/// has no nestloop params.
fn set_nestloop_refs(root: &mut PlannerInfo, mut plan: NestLoop, rtoffset: usize) -> NestLoop {
    plan.join.plan.plan_node_id = next_plan_node_id(root);
    // PG order: index the children's (still base-Var) output tlists and fix the
    // join's own exprs FIRST, THEN recurse into the children (which rewrites their
    // tlists to OUTER_VAR/INNER_VAR positions). Doing it the other way breaks a
    // join-over-join: the lower join's tlist would already be rewritten when the
    // upper join searches it for a base Var.
    let (outer, inner) = take_join_children(&mut plan.join.plan);
    let outer_tlist = plan_tlist(&outer).to_vec();
    let inner_tlist = plan_tlist(&inner).to_vec();

    fix_join_exprs(&mut plan.join.joinqual, &outer_tlist, &inner_tlist);
    fix_join_exprs(&mut plan.join.plan.targetlist, &outer_tlist, &inner_tlist);
    fix_join_qual(&mut plan.join.plan.qual, &outer_tlist, &inner_tlist);
    crate::assert!(plan.nest_params.is_empty(), "set_join_references: nestloop params not yet reachable");

    plan.join.plan.lefttree = Some(set_plan_refs(root, outer, rtoffset));
    plan.join.plan.righttree = Some(set_plan_refs(root, inner, rtoffset));
    plan
}

/// PG `set_plan_refs` T_MergeJoin arm: like NestLoop, plus the mergeclauses' Vars
/// are rewritten against the outer/inner child outputs.
fn set_mergejoin_refs(root: &mut PlannerInfo, mut plan: MergeJoin, rtoffset: usize) -> MergeJoin {
    plan.join.plan.plan_node_id = next_plan_node_id(root);
    let (outer, inner) = take_join_children(&mut plan.join.plan);
    let outer_tlist = plan_tlist(&outer).to_vec();
    let inner_tlist = plan_tlist(&inner).to_vec();

    fix_join_exprs(&mut plan.join.joinqual, &outer_tlist, &inner_tlist);
    fix_join_exprs(&mut plan.mergeclauses, &outer_tlist, &inner_tlist);
    fix_join_exprs(&mut plan.join.plan.targetlist, &outer_tlist, &inner_tlist);
    fix_join_qual(&mut plan.join.plan.qual, &outer_tlist, &inner_tlist);

    plan.join.plan.lefttree = Some(set_plan_refs(root, outer, rtoffset));
    plan.join.plan.righttree = Some(set_plan_refs(root, inner, rtoffset));
    plan
}

/// PG `set_plan_refs` T_HashJoin arm: like NestLoop, plus the hashclauses' Vars and
/// the (outer-only) hashkeys are rewritten. The HashJoin's righttree is a Hash node;
/// the Hash node's own hashkeys reference the inner child and are rewritten to
/// INNER_VAR against the Hash's input tlist.
fn set_hashjoin_refs(root: &mut PlannerInfo, mut plan: HashJoin, rtoffset: usize) -> HashJoin {
    plan.join.plan.plan_node_id = next_plan_node_id(root);
    let (outer, inner) = take_join_children(&mut plan.join.plan);
    let outer_tlist = plan_tlist(&outer).to_vec();
    // The inner child here is the Hash node; the hashclauses reference the Hash's
    // output (which mirrors its still-base-Var input tlist before recursion).
    let inner_tlist = plan_tlist(&inner).to_vec();

    fix_join_exprs(&mut plan.join.joinqual, &outer_tlist, &inner_tlist);
    fix_join_exprs(&mut plan.hashclauses, &outer_tlist, &inner_tlist);
    // The HashJoin's hashkeys (outer-side probe keys) reference only the outer child.
    fix_outer_exprs(&mut plan.hashkeys, &outer_tlist);
    fix_join_exprs(&mut plan.join.plan.targetlist, &outer_tlist, &inner_tlist);
    fix_join_qual(&mut plan.join.plan.qual, &outer_tlist, &inner_tlist);

    plan.join.plan.lefttree = Some(set_plan_refs(root, outer, rtoffset));
    plan.join.plan.righttree = Some(set_plan_refs(root, inner, rtoffset));
    plan
}

/// PG `set_plan_refs` T_Hash arm: recurse into the inner child, assign the node id,
/// then rewrite the Hash's own hashkeys to INNER_VAR positions over the child output
/// (the Hash sits below the HashJoin's inner side).
fn set_hash_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::Hash,
    rtoffset: usize,
) -> crate::nodes::plannodes::Hash {
    let child = plan
        .plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_plan_refs: Hash without child"));
    let child = set_plan_refs(root, child, rtoffset);
    let child_tlist = plan_tlist(&child).to_vec();
    plan.plan.plan_node_id = next_plan_node_id(root);
    // The Hash's tlist is a passthrough of its child; rewrite its hashkeys to
    // OUTER_VAR positions over the child (the Hash has a single input -> OUTER_VAR).
    fix_outer_exprs(&mut plan.hashkeys, &child_tlist);
    fix_upper_tlist(&mut plan.plan.targetlist, &child_tlist);
    plan.plan.lefttree = Some(child);
    plan
}

/// Take a join's outer (lefttree) + inner (righttree) subplans out of the node,
/// WITHOUT recursing. PG's `set_join_references` indexes the children's output
/// tlists (still in base-Var form) and fixes the join's own exprs before the
/// children are recursed (which would rewrite those tlists to OUTER/INNER). The
/// caller recurses into each returned child after fixing the parent.
fn take_join_children(plan: &mut crate::nodes::plannodes::Plan) -> (Node, Node) {
    let outer = plan
        .lefttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_join_references: join without outer subplan"));
    let inner = plan
        .righttree
        .take()
        .unwrap_or_else(|| not_yet_reachable("set_join_references: join without inner subplan"));
    (outer, inner)
}

/// PG `fix_join_expr` over a clause list: rewrite each clause's Vars against the
/// outer (OUTER_VAR) then inner (INNER_VAR) child indexed tlists, replacing the list
/// in place.
fn fix_join_exprs(clauses: &mut Vec<Node>, outer_tlist: &[Node], inner_tlist: &[Node]) {
    *clauses = std::mem::take(clauses)
        .into_iter()
        .map(|c| fix_join_expr(c, outer_tlist, inner_tlist))
        .collect();
}

/// PG `fix_join_expr` over a join node's qpqual (implicit-AND list). M7 inner joins
/// carry no qpqual (every clause is a joinqual), so this is empty.
fn fix_join_qual(qual: &mut Vec<Node>, outer_tlist: &[Node], inner_tlist: &[Node]) {
    fix_join_exprs(qual, outer_tlist, inner_tlist);
}

/// PG `fix_upper_expr` over an outer-only expression list (the HashJoin/Hash
/// hashkeys reference only their single child): rewrite Vars to OUTER_VAR positions.
fn fix_outer_exprs(exprs: &mut Vec<Node>, child_tlist: &[Node]) {
    *exprs = std::mem::take(exprs)
        .into_iter()
        .map(|e| fix_upper_expr(e, child_tlist))
        .collect();
}

/// PG `fix_join_expr_mutator`: rewrite the Vars in a join expression so each Var
/// references the outer or inner child's output by position. A Var found in the
/// outer child tlist becomes `(OUTER_VAR, outer_resno)`; one found in the inner
/// child tlist becomes `(INNER_VAR, inner_resno)`. The M7-reachable join expressions
/// are Vars and binary OpExprs over Vars/Consts.
fn fix_join_expr(expr: Node, outer_tlist: &[Node], inner_tlist: &[Node]) -> Node {
    match expr {
        Node::Var(var) => {
            if let Some(newvar) = search_indexed_tlist_for_var(&var, outer_tlist, OUTER_VAR) {
                return Node::Var(Box::new(newvar));
            }
            if let Some(newvar) = search_indexed_tlist_for_var(&var, inner_tlist, INNER_VAR) {
                return Node::Var(Box::new(newvar));
            }
            not_yet_reachable("fix_join_expr: Var not found in either subplan output");
        }
        Node::OpExpr(mut op) => {
            op.args = op
                .args
                .into_iter()
                .map(|a| fix_join_expr(a, outer_tlist, inner_tlist))
                .collect();
            Node::OpExpr(op)
        }
        Node::BoolExpr(mut b) => {
            b.args = b
                .args
                .into_iter()
                .map(|a| fix_join_expr(a, outer_tlist, inner_tlist))
                .collect();
            Node::BoolExpr(b)
        }
        Node::FuncExpr(mut f) => {
            f.args = f
                .args
                .into_iter()
                .map(|a| fix_join_expr(a, outer_tlist, inner_tlist))
                .collect();
            Node::FuncExpr(f)
        }
        Node::TargetEntry(mut te) => {
            if let Some(inner) = te.expr.take() {
                te.expr = Some(fix_join_expr(inner, outer_tlist, inner_tlist));
            }
            Node::TargetEntry(te)
        }
        other => other,
    }
}

/// PG `search_indexed_tlist_for_var`: find the base-rel Var (`varno`,`varattno`) in
/// `child_tlist` and return a copy with `varno = newvarno` (OUTER_VAR or INNER_VAR)
/// and `varattno = the child output column position` (the matching TargetEntry's
/// resno). Returns None if the Var is not produced by this child.
fn search_indexed_tlist_for_var(
    var: &crate::nodes::primnodes::Var,
    child_tlist: &[Node],
    newvarno: i32,
) -> Option<crate::nodes::primnodes::Var> {
    for n in child_tlist {
        let Node::TargetEntry(te) = n else { continue };
        if let Some(Node::Var(cv)) = te.expr.as_ref()
            && cv.varno == var.varno
            && cv.varattno == var.varattno
        {
            let mut newvar = var.clone();
            newvar.varno = newvarno;
            newvar.varattno = te.resno;
            return Some(newvar);
        }
    }
    None
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
    // RETURNING list Vars (scan Vars over the result relation) keep their identity
    // with rtoffset 0; they read the subplan slot at exec time. The targetlist mirror
    // is fixed by the same identity pass.
    fix_scan_tlist_identity(&plan.plan.targetlist, rtoffset);
    plan
}

/// PG `set_plan_refs` T_LockRows arm: recurse into the child and offset the row marks'
/// RT indices (no-op with rtoffset 0). The LockRows projects its child unchanged.
fn set_lockrows_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::LockRows,
    rtoffset: usize,
) -> crate::nodes::plannodes::LockRows {
    plan.plan.plan_node_id = root.glob.last_plan_node_id;
    root.glob.last_plan_node_id += 1;
    let off = rtoffset as crate::nodes::primnodes::Index;
    for m in &mut plan.row_marks {
        if let crate::nodes::nodes::Node::PlanRowMark(rm) = m {
            rm.rti += off;
            rm.prti += off;
        }
    }
    if let Some(sub) = plan.plan.lefttree.take() {
        plan.plan.lefttree = Some(set_plan_refs(root, sub, rtoffset));
    }
    fix_scan_tlist_identity(&plan.plan.targetlist, rtoffset);
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

/// PG `set_plan_refs` T_IndexScan arm + `fix_indexqual_references`: offset the
/// scanrelid, assign the node id, and fix the scan tlist/qual Var references. The
/// `indexqual` (already in INDEX_VAR form from createplan) and `indexqualorig` (heap
/// Vars) are identity-validated at rtoffset 0; createplan already did the
/// INDEX_VAR rewrite, so setrefs only assigns the node id and offsets the scanrelid.
fn set_indexscan_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::IndexScan,
    rtoffset: usize,
) -> crate::nodes::plannodes::IndexScan {
    plan.scan.scanrelid += rtoffset as crate::nodes::primnodes::Index;
    plan.scan.plan.plan_node_id = next_plan_node_id(root);

    fix_scan_tlist_identity(&plan.scan.plan.targetlist, rtoffset);
    fix_scan_qual_identity(&plan.scan.plan.qual, rtoffset);
    // The indexqualorig is the heap-Var recheck clause (identity at rtoffset 0); the
    // indexqual is already INDEX_VAR-rewritten. Validate both contain only expected
    // node kinds (Var/Const/OpExpr).
    for clause in &plan.indexqualorig {
        fix_scan_expr_identity(Some(clause));
    }
    plan
}

/// PG `set_plan_refs` T_BitmapHeapScan arm: offset the scanrelid, assign the node id,
/// fix the scan tlist/qual, recurse into the BitmapIndexScan child, and validate the
/// `bitmapqualorig` recheck clause.
fn set_bitmap_heapscan_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::BitmapHeapScan,
    rtoffset: usize,
) -> crate::nodes::plannodes::BitmapHeapScan {
    plan.scan.scanrelid += rtoffset as crate::nodes::primnodes::Index;
    plan.scan.plan.plan_node_id = next_plan_node_id(root);

    fix_scan_tlist_identity(&plan.scan.plan.targetlist, rtoffset);
    fix_scan_qual_identity(&plan.scan.plan.qual, rtoffset);
    for clause in &plan.bitmapqualorig {
        fix_scan_expr_identity(Some(clause));
    }

    // Recurse into the bitmap producer (BitmapIndexScan) child.
    if let Some(child) = plan.scan.plan.lefttree.take() {
        plan.scan.plan.lefttree = Some(set_plan_refs(root, child, rtoffset));
    }
    plan
}

/// PG `set_plan_refs` T_BitmapIndexScan arm: offset the scanrelid, assign the node
/// id. The indexqual is already INDEX_VAR-rewritten; the recheck `indexqualorig` is
/// the heap-Var clause (identity at rtoffset 0).
fn set_bitmap_indexscan_refs(
    root: &mut PlannerInfo,
    mut plan: crate::nodes::plannodes::BitmapIndexScan,
    rtoffset: usize,
) -> crate::nodes::plannodes::BitmapIndexScan {
    plan.scan.scanrelid += rtoffset as crate::nodes::primnodes::Index;
    plan.scan.plan.plan_node_id = next_plan_node_id(root);
    for clause in &plan.indexqualorig {
        fix_scan_expr_identity(Some(clause));
    }
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

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::nodes::makefuncs::makeTargetEntry;
    use crate::nodes::nodes::JoinType;
    use crate::nodes::plannodes::{Join, NestLoop, Plan, Scan, SeqScan};
    use crate::nodes::primnodes::{Var, VarReturningType};
    use crate::postgres_ext::{InvalidOid, Oid};

    const INT4: Oid = Oid(23);

    fn var(varno: i32, varattno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: INT4,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as crate::nodes::primnodes::Index,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    fn tle(expr: Node, resno: i16) -> Node {
        Node::TargetEntry(Box::new(makeTargetEntry(Some(expr), resno, None, false)))
    }

    fn empty_plan(tlist: Vec<Node>) -> Plan {
        Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: tlist,
            qual: Vec::new(),
            lefttree: None,
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        }
    }

    fn seqscan(relid: i32, attno: i16) -> Node {
        Node::SeqScan(Box::new(SeqScan {
            scan: Scan {
                plan: empty_plan(vec![tle(var(relid, attno), 1)]),
                scanrelid: relid as crate::nodes::primnodes::Index,
            },
        }))
    }

    fn test_root() -> PlannerInfo {
        crate::backend::optimizer::plan::initsplan::tests::test_planner_info()
    }

    /// A NestLoop whose tlist is (rel1.col1, rel2.col1) and whose joinqual is the
    /// `rel1.col1 = rel2.col1` clause, over two SeqScan children. After set_plan_refs
    /// the tlist/qual Vars must reference OUTER_VAR (outer child) / INNER_VAR (inner).
    #[test]
    fn set_join_references_rewrites_outer_inner_var() {
        use crate::nodes::primnodes::OpExpr;
        let joinqual = Node::OpExpr(Box::new(OpExpr {
            opno: Oid(96),
            opfuncid: InvalidOid,
            opresulttype: Oid(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var(1, 1), var(2, 1)],
            location: -1,
        }));
        let mut plan = empty_plan(vec![tle(var(1, 1), 1), tle(var(2, 1), 2)]);
        plan.lefttree = Some(seqscan(1, 1));
        plan.righttree = Some(seqscan(2, 1));
        let nl = NestLoop {
            join: Join { plan, jointype: JoinType::INNER, inner_unique: false, joinqual: vec![joinqual] },
            nest_params: Vec::new(),
        };

        let mut root = test_root();
        let Node::NestLoop(out) = set_plan_refs(&mut root, Node::NestLoop(Box::new(nl)), 0) else {
            panic!("not a NestLoop");
        };

        // tlist[0] = rel1.col1 -> OUTER_VAR position 1; tlist[1] = rel2.col1 -> INNER_VAR position 1.
        let tlist_var = |i: usize| -> Var {
            let Node::TargetEntry(te) = &out.join.plan.targetlist[i] else { panic!() };
            let Some(Node::Var(v)) = te.expr.as_ref() else { panic!() };
            (**v).clone()
        };
        let v0 = tlist_var(0);
        assert_eq!(v0.varno, OUTER_VAR);
        assert_eq!(v0.varattno, 1);
        let v1 = tlist_var(1);
        assert_eq!(v1.varno, INNER_VAR);
        assert_eq!(v1.varattno, 1);

        // The joinqual's two operands are rewritten the same way: lhs OUTER, rhs INNER.
        let Node::OpExpr(op) = &out.join.joinqual[0] else { panic!("joinqual not an OpExpr") };
        let Node::Var(l) = &op.args[0] else { panic!() };
        let Node::Var(r) = &op.args[1] else { panic!() };
        assert_eq!(l.varno, OUTER_VAR);
        assert_eq!(r.varno, INNER_VAR);

        // The children are still SeqScans with assigned node ids.
        assert!(matches!(out.join.plan.lefttree.as_ref(), Some(Node::SeqScan(_))));
        assert!(matches!(out.join.plan.righttree.as_ref(), Some(Node::SeqScan(_))));
    }
}

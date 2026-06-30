//! Querytree manipulation subroutines for the query rewriter. Translated from
//! backend/rewrite/rewriteManip.c (disposition: full leaf, M11-reachable subset).
//!
//! These are the mechanical Var-renumbering walkers that subquery pullup / view
//! expansion need. They recurse over the M11-reachable node shapes (Var, Const,
//! Param, OpExpr/DistinctExpr/NullIfExpr, BoolExpr, FuncExpr, RelabelType,
//! CoerceViaIO, TargetEntry, FromExpr, JoinExpr, RangeTblRef, and Query via its
//! rtable/jointree/tlist/quals). Unreachable node tags route to a clear grow guard
//! (rules.md s4); none of these helpers is half-written for the shapes it claims.
//!
//! `sublevels_up` tracks query nesting depth: a node is rewritten only when its
//! own level (Var.varlevelsup) equals the running depth, exactly as PG's walkers
//! gate on `context->sublevels_up`.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::nodes::primnodes::Var;

#[cold]
fn not_yet_reachable(what: &str, node: &Node) -> ! {
    unimplemented!("rewriteManip {what}: node {node:?} not yet translated for this milestone");
}

/// Is a Var's `varlevelsup` (usize) equal to a signed nesting depth?
fn level_eq(levelsup: crate::nodes::primnodes::Index, sublevels_up: i32) -> bool {
    i64::try_from(levelsup).unwrap_or(i64::MAX) == i64::from(sublevels_up)
}

/// Add a signed delta to an `Index` (usize) rangetable index / levelsup field,
/// saturating at 0 (a faithful renumber never drives the field negative).
fn add_index(value: crate::nodes::primnodes::Index, delta: i32) -> crate::nodes::primnodes::Index {
    let v = i64::try_from(value).unwrap_or(i64::MAX) + i64::from(delta);
    usize::try_from(v.max(0)).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// OffsetVarNodes: add `offset` to every Var.varno / RangeTblRef.rtindex /
// JoinExpr.rtindex at the matching query level.
// ---------------------------------------------------------------------------

/// PG `OffsetVarNodes`: add `offset` to the rangetable indexes of all Vars (and
/// RangeTblRef/JoinExpr rtindexes) in `node` whose level == `sublevels_up`.
pub fn offset_var_nodes(node: &mut Node, offset: i32, sublevels_up: i32) {
    // PG: if starting at a Query, go straight to the in-query walker so
    // sublevels_up is not incremented prematurely. At level 0 also fix the
    // Query's own rangetable indexes (resultRelation etc.; M11 views are plain
    // SELECTs with resultRelation 0, so only that field would matter).
    if let Node::Query(q) = node {
        if sublevels_up == 0 && q.resultRelation != 0 {
            q.resultRelation += offset;
        }
        offset_var_nodes_in_query(q, offset, sublevels_up);
    } else {
        offset_var_nodes_walk(node, offset, sublevels_up);
    }
}

fn offset_var_nodes_walk(node: &mut Node, offset: i32, sublevels_up: i32) {
    match node {
        Node::Var(v) => {
            if level_eq(v.varlevelsup, sublevels_up) {
                v.varno += offset;
                if v.varnosyn > 0 {
                    v.varnosyn = add_index(v.varnosyn, offset);
                }
            }
        }
        Node::Const(_) | Node::Param(_) | Node::SetToDefault(_) | Node::CaseTestExpr(_)
        | Node::SQLValueFunction(_) | Node::CoerceToDomainValue(_) => {}
        Node::RangeTblRef(rtr) => {
            if sublevels_up == 0 {
                rtr.rtindex += offset;
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &mut o.args {
                offset_var_nodes_walk(a, offset, sublevels_up);
            }
        }
        Node::BoolExpr(b) => {
            for a in &mut b.args {
                offset_var_nodes_walk(a, offset, sublevels_up);
            }
        }
        Node::FuncExpr(f) => {
            for a in &mut f.args {
                offset_var_nodes_walk(a, offset, sublevels_up);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_mut() {
                offset_var_nodes_walk(a, offset, sublevels_up);
            }
        }
        Node::TargetEntry(t) => {
            if let Some(e) = t.expr.as_mut() {
                offset_var_nodes_walk(e, offset, sublevels_up);
            }
        }
        Node::FromExpr(f) => {
            for c in &mut f.fromlist {
                offset_var_nodes_walk(c, offset, sublevels_up);
            }
            if let Some(q) = f.quals.as_mut() {
                offset_var_nodes_walk(q, offset, sublevels_up);
            }
        }
        Node::JoinExpr(j) => {
            if sublevels_up == 0 && j.rtindex != 0 {
                j.rtindex += offset;
            }
            if let Some(l) = j.larg.as_mut() {
                offset_var_nodes_walk(l, offset, sublevels_up);
            }
            if let Some(r) = j.rarg.as_mut() {
                offset_var_nodes_walk(r, offset, sublevels_up);
            }
            if let Some(q) = j.quals.as_mut() {
                offset_var_nodes_walk(q, offset, sublevels_up);
            }
        }
        Node::Query(_) => offset_var_nodes_query(node, offset, sublevels_up + 1),
        other => not_yet_reachable("OffsetVarNodes", other),
    }
}

/// Recurse into a Query node's rangetable subqueries, jointree, tlist and quals.
fn offset_var_nodes_query(node: &mut Node, offset: i32, sublevels_up: i32) {
    let Node::Query(q) = node else { unreachable!("offset_var_nodes_query on non-Query") };
    offset_var_nodes_in_query(q, offset, sublevels_up);
}

fn offset_var_nodes_in_query(q: &mut Query, offset: i32, sublevels_up: i32) {
    for te in &mut q.targetList {
        offset_var_nodes_walk(te, offset, sublevels_up);
    }
    if let Some(jt) = q.jointree.as_mut() {
        offset_var_nodes_walk(jt, offset, sublevels_up);
    }
    for rte in &mut q.rtable {
        if let Node::RangeTblEntry(rte) = rte
            && let Some(sub) = rte.subquery.as_mut()
        {
            // A subquery RTE's own contents are one level deeper.
            offset_var_nodes_in_query(sub, offset, sublevels_up + 1);
        }
    }
}

// ---------------------------------------------------------------------------
// IncrementVarSublevelsUp: bump Var.varlevelsup by `delta` for Vars whose level
// >= `min_sublevels_up`.
// ---------------------------------------------------------------------------

/// PG `IncrementVarSublevelsUp`: add `delta` to `varlevelsup` of every Var (and
/// the levelsup field of Aggref/PlaceHolderVar/CTE RTEs) whose levelsup is at
/// least `min_sublevels_up`.
pub fn increment_var_sublevels_up(node: &mut Node, delta: i32, min_sublevels_up: i32) {
    increment_var_sublevels_up_walk(node, delta, min_sublevels_up);
}

fn increment_var_sublevels_up_walk(node: &mut Node, delta: i32, min: i32) {
    match node {
        Node::Var(v) => {
            if i64::try_from(v.varlevelsup).unwrap_or(i64::MAX) >= i64::from(min) {
                v.varlevelsup = add_index(v.varlevelsup, delta);
            }
        }
        Node::Const(_) | Node::Param(_) | Node::SetToDefault(_) | Node::CaseTestExpr(_)
        | Node::SQLValueFunction(_) | Node::CoerceToDomainValue(_) | Node::RangeTblRef(_) => {}
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &mut o.args {
                increment_var_sublevels_up_walk(a, delta, min);
            }
        }
        Node::BoolExpr(b) => {
            for a in &mut b.args {
                increment_var_sublevels_up_walk(a, delta, min);
            }
        }
        Node::FuncExpr(f) => {
            for a in &mut f.args {
                increment_var_sublevels_up_walk(a, delta, min);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_mut() {
                increment_var_sublevels_up_walk(a, delta, min);
            }
        }
        Node::TargetEntry(t) => {
            if let Some(e) = t.expr.as_mut() {
                increment_var_sublevels_up_walk(e, delta, min);
            }
        }
        Node::FromExpr(f) => {
            for c in &mut f.fromlist {
                increment_var_sublevels_up_walk(c, delta, min);
            }
            if let Some(q) = f.quals.as_mut() {
                increment_var_sublevels_up_walk(q, delta, min);
            }
        }
        Node::JoinExpr(j) => {
            if let Some(l) = j.larg.as_mut() {
                increment_var_sublevels_up_walk(l, delta, min);
            }
            if let Some(r) = j.rarg.as_mut() {
                increment_var_sublevels_up_walk(r, delta, min);
            }
            if let Some(q) = j.quals.as_mut() {
                increment_var_sublevels_up_walk(q, delta, min);
            }
        }
        Node::Query(q) => increment_var_sublevels_in_query(q, delta, min + 1),
        other => not_yet_reachable("IncrementVarSublevelsUp", other),
    }
}

fn increment_var_sublevels_in_query(q: &mut Query, delta: i32, min: i32) {
    for te in &mut q.targetList {
        increment_var_sublevels_up_walk(te, delta, min);
    }
    if let Some(jt) = q.jointree.as_mut() {
        increment_var_sublevels_up_walk(jt, delta, min);
    }
    for rte in &mut q.rtable {
        if let Node::RangeTblEntry(rte) = rte
            && let Some(sub) = rte.subquery.as_mut()
        {
            increment_var_sublevels_in_query(sub, delta, min + 1);
        }
    }
}

// ---------------------------------------------------------------------------
// AddQual: AND a new qualification onto a query's jointree quals.
// ---------------------------------------------------------------------------

/// PG `AddQual`: add `qual` to the WHERE clause of `parsetree` (ANDed with the
/// existing jointree quals). A NULL `qual` is a no-op.
pub fn add_qual(parsetree: &mut Query, qual: Option<Node>) {
    let Some(qual) = qual else { return };
    let Some(Node::FromExpr(f)) = parsetree.jointree.as_mut() else {
        // PG always has a FromExpr at the jointree top after analysis.
        unreachable!("AddQual: jointree top is not a FromExpr");
    };
    f.quals = Some(match f.quals.take() {
        None => qual,
        Some(existing) => make_and_qual(existing, qual),
    });
}

/// AND two qualification clauses (PG `make_and_qual` -> a 2-arg AND BoolExpr).
fn make_and_qual(a: Node, b: Node) -> Node {
    use crate::nodes::primnodes::{BoolExpr, BoolExprType};
    Node::BoolExpr(Box::new(BoolExpr {
        boolop: BoolExprType::AND_EXPR,
        args: vec![a, b],
        location: -1,
    }))
}

// ---------------------------------------------------------------------------
// replace_vars_from_targetlist: replace every Var referencing `target_varno`
// (at level `sublevels_up`) with the matching targetlist entry's expression.
// This is the pullup_replace_vars core for the inner-join / no-PHV case.
// ---------------------------------------------------------------------------

/// Replace, in `node`, every Var with `varno == target_varno` at level
/// `sublevels_up` with a copy of the targetlist expression at that varattno
/// (PG `ReplaceVarsFromTargetList` / `pullup_replace_vars`, the no-PHV path). The
/// replacement expression's levelsup is bumped by the replaced Var's varlevelsup.
pub fn replace_vars_from_targetlist(
    node: Node,
    target_varno: i32,
    sublevels_up: i32,
    targetlist: &[Node],
) -> Node {
    replace_walk(node, target_varno, sublevels_up, targetlist)
}

fn replace_walk(node: Node, target_varno: i32, sublevels_up: i32, tlist: &[Node]) -> Node {
    match node {
        Node::Var(v) => {
            if v.varno == target_varno && level_eq(v.varlevelsup, sublevels_up) {
                let mut repl = lookup_tle_expr(tlist, v.varattno);
                if v.varlevelsup > 0 {
                    let up = i32::try_from(v.varlevelsup).unwrap_or(i32::MAX);
                    increment_var_sublevels_up(&mut repl, up, 0);
                }
                repl
            } else {
                Node::Var(v)
            }
        }
        n @ (Node::Const(_) | Node::Param(_) | Node::SetToDefault(_) | Node::CaseTestExpr(_)
        | Node::SQLValueFunction(_) | Node::CoerceToDomainValue(_) | Node::RangeTblRef(_)) => n,
        Node::OpExpr(mut o) => {
            o.args = replace_list(o.args, target_varno, sublevels_up, tlist);
            Node::OpExpr(o)
        }
        Node::DistinctExpr(mut o) => {
            o.args = replace_list(o.args, target_varno, sublevels_up, tlist);
            Node::DistinctExpr(o)
        }
        Node::NullIfExpr(mut o) => {
            o.args = replace_list(o.args, target_varno, sublevels_up, tlist);
            Node::NullIfExpr(o)
        }
        Node::BoolExpr(mut b) => {
            b.args = replace_list(b.args, target_varno, sublevels_up, tlist);
            Node::BoolExpr(b)
        }
        Node::FuncExpr(mut f) => {
            f.args = replace_list(f.args, target_varno, sublevels_up, tlist);
            Node::FuncExpr(f)
        }
        Node::RelabelType(mut r) => {
            r.arg = r.arg.map(|a| replace_walk(a, target_varno, sublevels_up, tlist));
            Node::RelabelType(r)
        }
        Node::TargetEntry(mut t) => {
            t.expr = t.expr.map(|e| replace_walk(e, target_varno, sublevels_up, tlist));
            Node::TargetEntry(t)
        }
        Node::FromExpr(mut f) => {
            f.fromlist = replace_list(f.fromlist, target_varno, sublevels_up, tlist);
            f.quals = f.quals.map(|q| replace_walk(q, target_varno, sublevels_up, tlist));
            Node::FromExpr(f)
        }
        Node::JoinExpr(mut j) => {
            j.larg = j.larg.map(|l| replace_walk(l, target_varno, sublevels_up, tlist));
            j.rarg = j.rarg.map(|r| replace_walk(r, target_varno, sublevels_up, tlist));
            j.quals = j.quals.map(|q| replace_walk(q, target_varno, sublevels_up, tlist));
            Node::JoinExpr(j)
        }
        other => not_yet_reachable("replace_vars_from_targetlist", &other),
    }
}

fn replace_list(list: Vec<Node>, target_varno: i32, sublevels_up: i32, tlist: &[Node]) -> Vec<Node> {
    list.into_iter().map(|n| replace_walk(n, target_varno, sublevels_up, tlist)).collect()
}

/// PG `get_tle_by_resno` + `copyObject(tle->expr)`: the targetlist entry whose
/// resno equals `attno`, cloning its expression. The whole-row reference
/// (`attno == 0`, `SELECT view.*`) is staged.
fn lookup_tle_expr(tlist: &[Node], attno: i16) -> Node {
    if attno == 0 {
        unimplemented!("replace_vars_from_targetlist: whole-row Var (SELECT view.*) staged");
    }
    for te in tlist {
        if let Node::TargetEntry(t) = te
            && t.resno == attno
            && !t.resjunk
        {
            return t
                .expr
                .clone()
                .unwrap_or_else(|| unreachable!("a matched non-junk TargetEntry has an expr"));
        }
    }
    unreachable!("replace_vars_from_targetlist: no targetlist entry for varattno {attno}");
}

/// PG `rangeTableEntry_used`: is rangetable entry `rt_index` (at `sublevels_up`)
/// referenced by any Var/RangeTblRef in `node`? Used by fireRIRrules to skip
/// already-expanded entries.
pub fn range_table_entry_used(node: &Node, rt_index: i32, sublevels_up: i32) -> bool {
    let mut found = false;
    rte_used_walk(node, rt_index, sublevels_up, &mut found);
    found
}

fn rte_used_walk(node: &Node, rt_index: i32, sublevels_up: i32, found: &mut bool) {
    if *found {
        return;
    }
    match node {
        Node::Var(v) => {
            if v.varno == rt_index && level_eq(v.varlevelsup, sublevels_up) {
                *found = true;
            }
        }
        Node::RangeTblRef(rtr) => {
            if sublevels_up == 0 && rtr.rtindex == rt_index {
                *found = true;
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &o.args {
                rte_used_walk(a, rt_index, sublevels_up, found);
            }
        }
        Node::BoolExpr(b) => {
            for a in &b.args {
                rte_used_walk(a, rt_index, sublevels_up, found);
            }
        }
        Node::FuncExpr(f) => {
            for a in &f.args {
                rte_used_walk(a, rt_index, sublevels_up, found);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = &r.arg {
                rte_used_walk(a, rt_index, sublevels_up, found);
            }
        }
        Node::TargetEntry(t) => {
            if let Some(e) = &t.expr {
                rte_used_walk(e, rt_index, sublevels_up, found);
            }
        }
        Node::FromExpr(f) => {
            for c in &f.fromlist {
                rte_used_walk(c, rt_index, sublevels_up, found);
            }
            if let Some(q) = &f.quals {
                rte_used_walk(q, rt_index, sublevels_up, found);
            }
        }
        Node::JoinExpr(j) => {
            if let Some(l) = &j.larg {
                rte_used_walk(l, rt_index, sublevels_up, found);
            }
            if let Some(r) = &j.rarg {
                rte_used_walk(r, rt_index, sublevels_up, found);
            }
            if let Some(q) = &j.quals {
                rte_used_walk(q, rt_index, sublevels_up, found);
            }
        }
        // Constants and leaves never reference an RTE.
        _ => {}
    }
}

/// PG `IsA(node, Var)` guard for callers that need a typed Var.
#[must_use]
pub fn as_var(node: &Node) -> Option<&Var> {
    match node {
        Node::Var(v) => Some(v),
        _ => None,
    }
}

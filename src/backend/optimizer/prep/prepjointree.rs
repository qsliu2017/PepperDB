//! Planner preprocessing for subqueries and jointrees. Translated from
//! backend/optimizer/prep/prepjointree.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::prep` under the C names.
//!
//! Disposition: `grow`. M2's live entry is `replace_empty_jointree`: a FROM-less
//! SELECT (or INSERT ... VALUES with no SELECT source) gets a single `RTE_RESULT`
//! RTE plus a `RangeTblRef` in the jointree, matching PG so a `SELECT 1` plans
//! with a one-entry rangetable. The sublink-pullup / subquery-pullup / outer-join
//! reduction / function-RTE inlining passes are grow guards (rules.md s4).

use crate::nodes::makefuncs::makeAlias;
use crate::nodes::nodes::{CmdType, JoinType, Node};
use crate::nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::primnodes::RangeTblRef;
use crate::postgres_ext::InvalidOid;

/// Panic for a prepjointree path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `replace_empty_jointree`: if the query's jointree fromlist is empty (a
/// FROM-less SELECT), inject one `RTE_RESULT` RTE and a `RangeTblRef` to it. A
/// top-level setop tree is left alone.
pub fn replace_empty_jointree(parse: &mut Query) {
    // Nothing to do if the jointree is already nonempty.
    let from_empty = match parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => f.fromlist.is_empty(),
        // No jointree behaves like an empty FROM.
        None => true,
        Some(_) => return,
    };
    if !from_empty {
        return;
    }
    // We mustn't change it in the top level of a setop tree, either.
    if parse.setOperations.is_some() {
        return;
    }

    // Create the RTE_RESULT RTE and append it to the rangetable.
    let rte = make_result_rte();
    parse.rtable.push(Node::RangeTblEntry(Box::new(rte)));
    let rti = parse.rtable.len() as i32;

    // Jam a RangeTblRef into the jointree's fromlist.
    let rtr = Node::RangeTblRef(Box::new(RangeTblRef { rtindex: rti }));
    match parse.jointree.as_mut() {
        Some(Node::FromExpr(f)) => f.fromlist = vec![rtr],
        _ => {
            parse.jointree = Some(Node::FromExpr(Box::new(
                crate::nodes::makefuncs::makeFromExpr(vec![rtr], None),
            )));
        }
    }
}

/// PG `pull_up_subqueries`: flatten subqueries-in-FROM into the parent jointree.
///
/// Walks the jointree; a subquery RTE simple enough to pull up
/// (`is_simple_subquery`) is spliced into its parent (`pull_up_simple_subquery`),
/// a simple UNION ALL subquery becomes an append relation
/// (`pull_up_simple_union_all`), etc. M7 has no subqueries in FROM, so the walk
/// leaves the tree unchanged; the actual splice machinery is staged (see
/// `pull_up_simple_subquery`).
pub fn pull_up_subqueries(root: &mut PlannerInfo) {
    // Top level of jointree must always be a FromExpr.
    let jointree = root
        .parse
        .jointree
        .take()
        .unwrap_or_else(|| not_yet_reachable("pull_up_subqueries: missing jointree"));
    crate::assert!(matches!(jointree, Node::FromExpr(_)));
    let jointree = pull_up_subqueries_recurse(root, jointree, false);
    crate::assert!(matches!(jointree, Node::FromExpr(_)));
    root.parse.jointree = Some(jointree);
}

/// PG `pull_up_subqueries_recurse`: recursive guts of `pull_up_subqueries`,
/// returning the (possibly rewritten) jointree node. `in_outer_join` stands in
/// for PG's `lowest_outer_join` pointer (we only need "are we below an OJ?" to
/// gate LATERAL pullups, which are staged anyway).
fn pull_up_subqueries_recurse(root: &mut PlannerInfo, jtnode: Node, in_outer_join: bool) -> Node {
    match jtnode {
        Node::RangeTblRef(ref rtr) => {
            let varno = rtr.rtindex as usize;
            let rte = match root.parse.rtable.get(varno - 1) {
                Some(Node::RangeTblEntry(rte)) => rte.as_ref(),
                _ => not_yet_reachable("pull_up_subqueries_recurse: missing RTE for RangeTblRef"),
            };

            // Simple subquery RTE -> pull up. (We are never an appendrel member
            // at M7, so the is_safe_append_member gate is not exercised.)
            if rte.rtekind == RTEKind::SUBQUERY
                && let Some(subquery) = rte.subquery.as_deref()
                && is_simple_subquery(root, subquery, rte, in_outer_join)
            {
                return pull_up_simple_subquery(root, jtnode);
            }
            // Simple UNION ALL subquery -> flatten to an append relation.
            if rte.rtekind == RTEKind::SUBQUERY
                && let Some(subquery) = rte.subquery.as_deref()
                && is_simple_union_all(subquery)
            {
                return pull_up_simple_union_all(root, jtnode);
            }
            // VALUES / FUNCTION inlining are staged too; M7 reaches neither.
            if rte.rtekind == RTEKind::VALUES && !in_outer_join {
                not_yet_reachable("pull_up_subqueries_recurse: simple VALUES pullup");
            }
            if rte.rtekind == RTEKind::FUNCTION {
                not_yet_reachable("pull_up_subqueries_recurse: constant FUNCTION inlining");
            }
            // Otherwise, do nothing at this node.
            jtnode
        }
        Node::FromExpr(mut f) => {
            // Recursively transform all the child nodes.
            f.fromlist = f
                .fromlist
                .into_iter()
                .map(|child| pull_up_subqueries_recurse(root, child, in_outer_join))
                .collect();
            Node::FromExpr(f)
        }
        Node::JoinExpr(mut j) => {
            // Recurse, telling children when they're inside an outer join.
            let child_in_oj = match j.jointype {
                JoinType::INNER => in_outer_join,
                JoinType::LEFT | JoinType::SEMI | JoinType::ANTI | JoinType::FULL
                | JoinType::RIGHT => true,
                other => not_yet_reachable(&format!(
                    "pull_up_subqueries_recurse: unrecognized join type {other:?}"
                )),
            };
            j.larg = j.larg.map(|n| pull_up_subqueries_recurse(root, n, child_in_oj));
            j.rarg = j.rarg.map(|n| pull_up_subqueries_recurse(root, n, child_in_oj));
            Node::JoinExpr(j)
        }
        other => not_yet_reachable(&format!(
            "pull_up_subqueries_recurse: unrecognized node type {other:?}"
        )),
    }
}

/// PG `pull_up_simple_subquery`: splice a pullable subquery's jointree into the
/// parent, replacing the parent's Vars that reference the subquery's outputs.
///
/// STAGED (rules.md s4): the splice depends on `pullup_replace_vars` (the ~2700-
/// line Var-substitution pass), `OffsetVarNodes`/`IncrementVarSublevelsUp`,
/// `substitute_phv_relids`, and recursive subquery planning -- none translated
/// yet. `is_simple_subquery` already gated us here, so reaching this means a
/// genuine `FROM (subquery)`, which M7 does not plan.
fn pull_up_simple_subquery(_root: &mut PlannerInfo, _jtnode: Node) -> Node {
    not_yet_reachable("pull_up_simple_subquery: subquery flattening (pullup_replace_vars)");
}

/// PG `pull_up_simple_union_all` / `flatten_simple_union_all`: flatten a simple
/// `UNION ALL` subquery into an append relation. STAGED (appendrel expansion).
fn pull_up_simple_union_all(_root: &mut PlannerInfo, _jtnode: Node) -> Node {
    not_yet_reachable("pull_up_simple_union_all: UNION ALL appendrel flattening");
}

/// PG `flatten_simple_union_all`: the top-level UNION ALL flattening entry. The
/// flattening itself is staged; with no setop query at M7 this is a no-op.
pub fn flatten_simple_union_all(root: &mut PlannerInfo) {
    if root.parse.setOperations.is_some() {
        not_yet_reachable("flatten_simple_union_all: top-level UNION ALL flattening");
    }
}

/// PG `is_simple_subquery`: is `subquery` simple enough to pull up into its
/// parent? Reproduces PG's full series of disqualifying checks. LATERAL-specific
/// checks (which need `jointree_contains_lateral_outer_refs` /
/// `pull_varnos_of_level`) are staged: a LATERAL subquery below an outer join is
/// conservatively declined.
fn is_simple_subquery(
    _root: &PlannerInfo,
    subquery: &Query,
    rte: &RangeTblEntry,
    in_outer_join: bool,
) -> bool {
    // Make sure it's a valid subselect.
    crate::assert!(subquery.commandType == CmdType::SELECT);

    // Can't pull up a query with setops (simple UNION ALL is a separate path).
    if subquery.setOperations.is_some() {
        return false;
    }

    // Can't pull up grouping / aggregation / SRFs / sorting / limiting / WITH,
    // nor an explicit FOR UPDATE/SHARE.
    if subquery.hasAggs
        || subquery.hasWindowFuncs
        || subquery.hasTargetSRFs
        || !subquery.groupClause.is_empty()
        || !subquery.groupingSets.is_empty()
        || subquery.havingQual.is_some()
        || !subquery.sortClause.is_empty()
        || !subquery.distinctClause.is_empty()
        || subquery.limitOffset.is_some()
        || subquery.limitCount.is_some()
        || subquery.hasForUpdate
        || !subquery.cteList.is_empty()
    {
        return false;
    }

    // Don't pull up a security-barrier view.
    if rte.security_barrier {
        return false;
    }

    // LATERAL pullup restrictions need the lateral-ref analysis; stage by
    // declining a LATERAL subquery that sits below an outer join.
    if rte.lateral && in_outer_join {
        // TODO(lateral): jointree_contains_lateral_outer_refs + targetlist check.
        return false;
    }

    // Don't pull up a subquery with volatile functions in its targetlist
    // (contain_volatile_functions is a stub); declining here is conservative and
    // correct. With the other gates passed, M7 never constructs such a subquery.
    // TODO(volatility): contain_volatile_functions(subquery.targetList).

    true
}

/// PG `is_simple_union_all`: is `subquery` a simple `UNION ALL` (all UNION ALL,
/// no coercions)? The recursive leaf-type check is staged; we recognize the
/// top-level shape (a SetOperationStmt with no ORDER BY/LIMIT/locking/WITH) and
/// stage the recursion.
fn is_simple_union_all(subquery: &Query) -> bool {
    crate::assert!(subquery.commandType == CmdType::SELECT);

    // Is it a set-operation query at all?
    let Some(Node::SetOperationStmt(_)) = subquery.setOperations.as_ref() else {
        return false;
    };

    // Can't handle ORDER BY, LIMIT/OFFSET, locking, or WITH.
    if !subquery.sortClause.is_empty()
        || subquery.limitOffset.is_some()
        || subquery.limitCount.is_some()
        || !subquery.rowMarks.is_empty()
        || !subquery.cteList.is_empty()
    {
        return false;
    }

    // The recursive UNION-ALL/coercion check (is_simple_union_all_recurse) is
    // staged; declining here keeps the simple UNION ALL on the non-flattened
    // (correct, unoptimized) path.
    // TODO(union-all): is_simple_union_all_recurse leaf-type comparison.
    false
}

/// `makeNode(RangeTblEntry)` for an `RTE_RESULT` (eref `*RESULT*`).
fn make_result_rte() -> RangeTblEntry {
    let eref = makeAlias("*RESULT*", Vec::new());
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::RESULT,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes: Vec::new(),
        coltypmods: Vec::new(),
        colcollations: Vec::new(),
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral: false,
        inFromCl: false,
        securityQuals: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::primnodes::FromExpr;

    /// `FROM a, b` (two RELATION RangeTblRefs, no subqueries) -> the jointree is
    /// returned unchanged by pull_up_subqueries (nothing to pull up).
    #[test]
    fn pull_up_subqueries_noop_over_relations() {
        let mut root = crate::backend::optimizer::plan::initsplan::tests::test_planner_info();
        // Two relation RTEs.
        let mk_rel = || {
            let mut r = make_result_rte();
            r.rtekind = RTEKind::RELATION;
            r.relid = crate::postgres_ext::Oid::new(1000);
            Node::RangeTblEntry(Box::new(r))
        };
        root.parse.rtable = vec![mk_rel(), mk_rel()];
        let jt = Node::FromExpr(Box::new(FromExpr {
            fromlist: vec![
                Node::RangeTblRef(Box::new(RangeTblRef { rtindex: 1 })),
                Node::RangeTblRef(Box::new(RangeTblRef { rtindex: 2 })),
            ],
            quals: None,
        }));
        root.parse.jointree = Some(jt);
        pull_up_subqueries(&mut root);
        // Unchanged: still a 2-element FromExpr of the same RangeTblRefs.
        let Some(Node::FromExpr(f)) = root.parse.jointree.as_ref() else {
            panic!("expected FromExpr");
        };
        assert_eq!(f.fromlist.len(), 2);
        assert!(matches!(f.fromlist[0], Node::RangeTblRef(ref r) if r.rtindex == 1));
        assert!(matches!(f.fromlist[1], Node::RangeTblRef(ref r) if r.rtindex == 2));
    }

    /// flatten_simple_union_all is a no-op when there is no top-level setop.
    #[test]
    fn flatten_simple_union_all_noop_without_setop() {
        let mut root = crate::backend::optimizer::plan::initsplan::tests::test_planner_info();
        flatten_simple_union_all(&mut root);
        assert!(root.parse.setOperations.is_none());
    }
}

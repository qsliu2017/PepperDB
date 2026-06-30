//! Handle CTEs (common table expressions) in the parser. Translated from
//! backend/parser/parse_cte.c (disposition: grow -- the M12 milestone subset:
//! non-recursive WITH and the canonical `WITH RECURSIVE` shape
//! `non-recursive-term UNION [ALL] recursive-term`). PG's dependency graph /
//! topological sort over mutually-referencing CTEs, the well-formed-recursion
//! checker, and the SEARCH / CYCLE clauses are deferred to later milestones.
//!
//! Each CTE body is analyzed in a child `ParseState` that sees the WITH namespace
//! (so `FROM cte_name` resolves to an `RTE_CTE`). A non-recursive CTE's output
//! column names/types are derived from its analyzed query's target list
//! (`analyze_cte_target_list`). A recursive CTE is analyzed term-by-term: the
//! non-recursive (left) term sets the CTE's column info FIRST, so the recursive
//! (right) term's self-reference resolves to a CTE RTE with those columns; the two
//! terms then become the leaves of the CTE Query's `SetOperationStmt`.

use std::sync::Arc;

use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::backend::parser::analyze::{finish_set_operation_stmt_pub, transform_stmt_async_pub};
use crate::catalog::genbki::DEFAULT_COLLATION_OID;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{
    CommonTableExpr, Query, SetOperation, SetOperationStmt, WithClause,
};
use crate::nodes::value::makeString;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;

const UNKNOWNOID: Oid = Oid::new(705);
const TEXTOID: Oid = Oid::new(25);

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `transformWithClause`: transform the WITH list into analyzed CTE Querys, set
/// up the CTE namespace, and return the transformed CTE list (to put on the output
/// Query's `cteList`). Each CTE is added to `pstate.p_ctenamespace` before analysis
/// (per spec all WITH items are visible to all others, but the M12 subset analyzes
/// them in declaration order without the dependency sort).
pub async fn transform_with_clause(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    with_clause: &WithClause,
) -> Vec<Node> {
    crate::assert!(pstate.p_ctenamespace.is_empty());
    crate::assert!(pstate.p_future_ctes.is_empty());

    // Duplicate-name check + tentative non-recursive marking.
    let mut ctes: Vec<CommonTableExpr> = Vec::new();
    for n in &with_clause.ctes {
        let Node::CommonTableExpr(cte) = n else {
            not_yet_reachable("transformWithClause: WITH item is not a CommonTableExpr");
        };
        let mut cte = (**cte).clone();
        let name = cte.ctename.clone().unwrap_or_default();
        if ctes.iter().any(|c| c.ctename.as_deref() == Some(name.as_str())) {
            duplicate_cte_error(&name);
        }
        cte.cterecursive = false;
        cte.cterefcount = 0;
        if !matches!(cte.ctequery, Some(Node::SelectStmt(_))) {
            not_yet_reachable("transformWithClause: data-modifying WITH query");
        }
        ctes.push(cte);
    }

    if with_clause.recursive {
        // Per spec all WITH items are visible to all; M12 analyzes them in order.
        // Each recursive CTE is self-marked before analysis so its body's
        // self-reference resolves.
        for cte in &mut ctes {
            cte.cterecursive = true;
        }
        // Stage the namespace (all items visible) before analysis.
        pstate.p_ctenamespace.clone_from(&ctes);
        #[allow(
            clippy::needless_range_loop,
            reason = "the loop both analyzes CTE i (mutating pstate) and reads back pstate.p_ctenamespace[i] into ctes[i]; the parallel index is the clean form"
        )]
        for i in 0..ctes.len() {
            analyze_cte(shared, pstate, i).await;
            // Reflect the analyzed CTE back into the working list.
            ctes[i].clone_from(&pstate.p_ctenamespace[i]);
        }
    } else {
        // Non-recursive: analyze each CTE, adding it to the namespace as we go so a
        // later CTE can reference an earlier one.
        pstate.p_future_ctes.clone_from(&ctes);
        for cte in &mut ctes {
            pstate.p_future_ctes.remove(0);
            analyze_cte_nonrecursive(shared, pstate, cte).await;
            pstate.p_ctenamespace.push(cte.clone());
        }
    }

    // Carry back the analyzed CTEs (with their final refcounts).
    pstate.p_ctenamespace.clone_from(&ctes);
    ctes.into_iter().map(|c| Node::CommonTableExpr(Box::new(c))).collect()
}

/// Analyze one non-recursive CTE: transform its SELECT body in a child ParseState
/// (seeing the WITH namespace), then derive its output column info.
async fn analyze_cte_nonrecursive(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    cte: &mut CommonTableExpr,
) {
    let Some(Node::SelectStmt(stmt)) = cte.ctequery.clone() else {
        not_yet_reachable("analyzeCTE: non-recursive CTE body is not a SELECT");
    };
    let mut child = make_cte_child_pstate(pstate, cte);
    let query = Box::pin(transform_stmt_async_pub(shared, &mut child, &Node::SelectStmt(stmt))).await;
    propagate_child_flags(pstate, &child);
    if query.commandType != CmdType::SELECT {
        not_yet_reachable("analyzeCTE: non-SELECT CTE body");
    }
    let mut query = query;
    query.canSetTag = false;
    analyze_cte_target_list(cte, &query.targetList, false);
    cte.ctequery = Some(Node::Query(query));
}

/// Analyze the i-th recursive CTE (already in `pstate.p_ctenamespace[i]`). The body
/// must be the canonical `non-recursive UNION [ALL] recursive` shape: the
/// non-recursive term is transformed first (setting the CTE's column types), then
/// the recursive term (its self-reference resolves to the CTE RTE), and the two are
/// assembled into the CTE Query's SetOperationStmt.
async fn analyze_cte(shared: &Arc<SharedState>, pstate: &mut ParseState, idx: usize) {
    let cte = pstate.p_ctenamespace[idx].clone();
    let Some(Node::SelectStmt(stmt)) = cte.ctequery.clone() else {
        not_yet_reachable("analyzeCTE: recursive CTE body is not a SELECT");
    };

    if stmt.op == SetOperation::NONE {
        // Not actually recursive (no UNION): analyze as a plain SELECT.
        let mut cte = cte;
        Box::pin(analyze_cte_nonrecursive(shared, pstate, &mut cte)).await;
        cte.cterecursive = false;
        pstate.p_ctenamespace[idx] = cte;
        return;
    }
    if stmt.op != SetOperation::UNION {
        not_yet_reachable("analyzeCTE: recursive CTE must use UNION (not INTERSECT/EXCEPT)");
    }
    let larg = stmt.larg.clone().unwrap_or_else(|| not_yet_reachable("recursive CTE: missing non-recursive term"));
    let rarg = stmt.rarg.clone().unwrap_or_else(|| not_yet_reachable("recursive CTE: missing recursive term"));
    if larg.op != SetOperation::NONE || rarg.op != SetOperation::NONE {
        not_yet_reachable("analyzeCTE: recursive term is itself a set operation");
    }

    // 1) Non-recursive term first, in a child pstate that sees the WITH namespace.
    let mut child = make_cte_child_pstate(pstate, &cte);
    let nr_query = Box::pin(transform_stmt_async_pub(shared, &mut child, &Node::SelectStmt(larg.clone()))).await;
    propagate_child_flags(pstate, &child);

    // 2) Determine the CTE's output column info from the non-recursive term, so the
    //    recursive term's self-reference RTE gets the right columns (forcing unknown
    //    columns to text, the recursive rule).
    let mut cte = cte;
    analyze_cte_target_list(&mut cte, &nr_query.targetList, true);
    pstate.p_ctenamespace[idx] = cte.clone();

    // 3) Recursive term: a fresh child pstate seeing the WITH namespace (now with the
    //    CTE columns set), so `FROM cte` resolves to a self-reference CTE RTE.
    let mut rchild = make_cte_child_pstate(pstate, &cte);
    let r_query = Box::pin(transform_stmt_async_pub(shared, &mut rchild, &Node::SelectStmt(rarg.clone()))).await;
    propagate_child_flags(pstate, &rchild);

    // 4) Assemble the CTE Query's SetOperationStmt over the two leaf Querys (the
    //    port's set-op representation: embedded Node::Query leaves).
    let col_types: Vec<Oid> = cte.ctecoltypes.clone();
    let sostmt = SetOperationStmt {
        op: SetOperation::UNION,
        all: stmt.all,
        larg: Some(Node::Query(nr_query.clone())),
        rarg: Some(Node::Query(r_query)),
        colTypes: col_types.clone(),
        colTypmods: vec![-1; col_types.len()],
        colCollations: vec![InvalidOid; col_types.len()],
        groupClauses: Vec::new(),
    };

    let mut cte_query = make_setop_query(pstate, sostmt, &nr_query, &col_types);
    cte_query.canSetTag = false;
    cte.ctequery = Some(Node::Query(cte_query));
    pstate.p_ctenamespace[idx] = cte;
}

/// Build the CTE's wrapping set-op Query (mirrors analyze.rs
/// `finish_set_operation_stmt` for the recursive-CTE case): the top target list is
/// the non-recursive term's column names with the reconciled types (Var varno 0).
fn make_setop_query(
    pstate: &mut ParseState,
    sostmt: SetOperationStmt,
    nr_query: &Query,
    col_types: &[Oid],
) -> Box<Query> {
    finish_set_operation_stmt_pub(pstate, sostmt, nr_query, col_types)
}

/// PG `analyzeCTETargetList`: derive the CTE's output column names/types/typmods/
/// collations from the (non-junk) target entries. For a recursive CTE, force any
/// unknown-type column to text (resolved before the recursive term is analyzed).
fn analyze_cte_target_list(cte: &mut CommonTableExpr, tlist: &[Node], recursive: bool) {
    crate::assert!(cte.ctecolnames.is_empty());
    cte.ctecolnames = cte.aliascolnames.clone();
    cte.ctecoltypes.clear();
    cte.ctecoltypmods.clear();
    cte.ctecolcollations.clear();
    let numaliases = cte.aliascolnames.len();
    let mut varattno = 0usize;
    for n in tlist {
        let Node::TargetEntry(te) = n else { continue };
        if te.resjunk {
            continue;
        }
        varattno += 1;
        if varattno > numaliases {
            let attrname = te.resname.clone().unwrap_or_default();
            cte.ctecolnames.push(Node::String_(makeString(attrname)));
        }
        let expr = te.expr.as_ref().unwrap_or_else(|| not_yet_reachable("analyzeCTETargetList: null expr"));
        let mut coltype = exprType(expr);
        let mut coltypmod = exprTypmod(expr);
        let mut colcoll = exprCollation(expr);
        if recursive && coltype == UNKNOWNOID {
            coltype = TEXTOID;
            coltypmod = -1;
            if !colcoll.is_valid() {
                colcoll = DEFAULT_COLLATION_OID;
            }
        }
        cte.ctecoltypes.push(coltype);
        cte.ctecoltypmods.push(coltypmod);
        cte.ctecolcollations.push(colcoll);
    }
    if varattno < numaliases {
        not_yet_reachable("analyzeCTETargetList: more aliases than columns");
    }
}

/// Build a child `ParseState` for analyzing a CTE body: it sees the enclosing WITH
/// namespace and records `p_parent_cte` (so a self-reference inside a recursive term
/// resolves). The CTE list is shared by clone (the refcount bumps inside the body
/// don't need to flow back for the M12 subset -- they affect only sharing, which the
/// executor handles per-scan).
fn make_cte_child_pstate(parent: &ParseState, cte: &CommonTableExpr) -> Box<ParseState> {
    let mut child = crate::backend::parser::parse_node::make_child_parsestate(parent);
    child.p_parent_cte = Some(Box::new(cte.clone()));
    child
}

/// Merge a CTE body child ParseState's discovered query-property flags back up.
fn propagate_child_flags(pstate: &mut ParseState, child: &ParseState) {
    pstate.p_has_aggs |= child.p_has_aggs;
    pstate.p_has_window_funcs |= child.p_has_window_funcs;
    pstate.p_has_sub_links |= child.p_has_sub_links;
    pstate.p_has_modifying_cte |= child.p_has_modifying_cte;
}

#[cold]
fn duplicate_cte_error(name: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!("WITH query name \"{name}\" specified more than once"));
    });
    unreachable!("ereport(ERROR) diverges");
}

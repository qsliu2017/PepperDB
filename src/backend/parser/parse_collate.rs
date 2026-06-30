//! Assign collation information to expressions in a completed parse tree.
//! Translated from backend/parser/parse_collate.c.
//!
//! Non-type-centric free functions (`assign_query_collations`,
//! `assign_expr_collations`, ...); bodies here as snake_case `pub fn`s, re-exported
//! from `crate::parser::parse_collate` under the C names.
//!
//! Disposition: `grow`. The collation-assignment tree walker is large (one arm per
//! collation-bearing node). For M1 the only expression reached is an int4 `Const`,
//! which is uncollatable, so the whole walk is a faithful no-op: each
//! subexpression yields `InvalidOid` / `COLLATE_NONE` and nothing is rewritten.
//! `assign_query_collations` walks the Query's M1-reachable expression fields
//! directly (`query_tree_walker` is not translated yet); it grows to the full
//! field set as that walker lands. `assign_collations_walker` is scaffolded: the
//! uncollatable-leaf arm is live, every collation-bearing arm routes to a single
//! not-yet-reachable staging arm (rules.md s4).

use crate::nodes::nodeFuncs::exprCollation;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::{InvalidOid, Oid};

/// PG `CollateStrength`: how forcibly a collation propagates upward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "grow scaffold: only None is reached for M1 (uncollatable consts); \
              Implicit/Conflict/Explicit are set by collation-bearing arms added later"
)]
enum CollateStrength {
    /// no collation has a strength yet
    None,
    /// collation derived from a child node
    Implicit,
    /// conflicting collations from children
    Conflict,
    /// collation set by a COLLATE clause
    Explicit,
}

/// PG `assign_collations_context`: state threaded down the collation walk.
#[allow(
    dead_code,
    reason = "grow scaffold: the M1 no-op walk writes these but reads them only in \
              the collation-bearing arms added later"
)]
struct AssignCollationsContext {
    collation: Oid,
    strength: CollateStrength,
    location: i32,
}

/// Panic for a collation-bearing node tag not yet translated for this milestone.
#[cold]
fn not_yet_reachable(node: &Node) -> ! {
    unimplemented!("assign_collations_walker: node tag not yet reachable for this milestone: {node:?}");
}

/// PG `assign_query_collations`: assign collations throughout a `Query`'s
/// expressions.
///
/// PG uses `query_tree_walker` (ignoring the range table and CTE subqueries) to
/// reach each top-level expression. That walker is not translated yet, so we walk
/// the M1-reachable expression fields directly: the target list, and the join
/// tree's qualification. Both are no-ops for uncollatable expressions.
pub fn assign_query_collations(pstate: &mut ParseState, query: &mut Query) {
    for tle in &mut query.targetList {
        if let Node::TargetEntry(te) = tle
            && let Some(expr) = te.expr.as_mut() {
                assign_expr_collations(pstate, expr);
            }
    }
    if let Some(jointree) = query.jointree.as_mut()
        && let Node::FromExpr(f) = jointree
            && let Some(quals) = f.quals.as_mut() {
                assign_expr_collations(pstate, quals);
            }
}

/// PG `assign_list_collations`: assign collations to each expression in a list.
pub fn assign_list_collations(pstate: &mut ParseState, exprs: &mut [Node]) {
    for node in exprs {
        assign_expr_collations(pstate, node);
    }
}

/// PG `assign_expr_collations`: assign collations throughout one expression tree.
pub fn assign_expr_collations(pstate: &mut ParseState, expr: &mut Node) {
    let mut context = AssignCollationsContext {
        collation: InvalidOid,
        strength: CollateStrength::None,
        location: -1,
    };
    assign_collations_walker(pstate, expr, &mut context);
}

/// PG `assign_collations_walker`: the recursive collation-assignment walker
/// (file-local in parse_collate.c, so private here). Returns `false` (PG's walker
/// abort convention) when done; the result is ignored by all callers.
///
/// Grow dispatcher: the uncollatable-leaf arm (`Const`, etc.) is the only one
/// reached for M1 and is a no-op. Collation-bearing / recursive arms grow later.
fn assign_collations_walker(
    pstate: &mut ParseState,
    node: &mut Node,
    context: &mut AssignCollationsContext,
) -> bool {
    match node {
        // A leaf expression (a `Const` or a `Var`): no children to recurse into.
        // Its result-type collation (if any) propagates to its parent with implicit
        // strength; an uncollatable leaf (int/bool, collation InvalidOid) is a
        // no-op. PG handles a Var/Const this way in the walker's leaf default.
        Node::Const(_) | Node::Var(_) | Node::Param(_) => {
            let collation = exprCollation(node);
            if collation == InvalidOid {
                context.collation = InvalidOid;
                context.strength = CollateStrength::None;
            } else {
                context.collation = collation;
                context.strength = CollateStrength::Implicit;
            }
            context.location = -1;
            false
        }
        // OpExpr / FuncExpr / BoolExpr: recurse into the argument list, then set the
        // node's own result collation. For the M3 int4/bool operators and support
        // functions every input and result is uncollatable, so each child reports
        // CollateStrength::None and the node's collation is InvalidOid (the general
        // collation-merge over collatable inputs grows with text/varchar). The walk
        // must still descend so nested OpExprs are visited.
        Node::OpExpr(_) | Node::FuncExpr(_) | Node::BoolExpr(_) | Node::NullIfExpr(_) => {
            walk_args(pstate, node);
            let collation = exprCollation(node);
            // exprSetCollation is a no-op when the result type is uncollatable, but
            // is faithful for the general case; M3 collation is always InvalidOid.
            crate::nodes::nodeFuncs::exprSetCollation(node, collation);
            context.collation = InvalidOid;
            context.strength = CollateStrength::None;
            context.location = -1;
            false
        }
        // M4 (step 23): casts + conditional expressions. The M4-reachable result
        // types (numeric/int/float/date) are all uncollatable, so the walk descends
        // into the children (so nested OpExprs are visited) and sets the node's
        // collation to InvalidOid. The general collation merge over collatable inputs
        // grows with text/varchar.
        Node::RelabelType(r) => {
            if let Some(arg) = r.arg.as_mut() {
                assign_expr_collations(pstate, arg);
            }
            no_collation(context);
            false
        }
        Node::CoerceViaIO(c) => {
            if let Some(arg) = c.arg.as_mut() {
                assign_expr_collations(pstate, arg);
            }
            no_collation(context);
            false
        }
        Node::CaseExpr(c) => {
            if let Some(arg) = c.arg.as_mut() {
                assign_expr_collations(pstate, arg);
            }
            for arm in &mut c.args {
                assign_expr_collations(pstate, arm);
            }
            if let Some(def) = c.defresult.as_mut() {
                assign_expr_collations(pstate, def);
            }
            no_collation(context);
            false
        }
        Node::CaseWhen(w) => {
            if let Some(cond) = w.expr.as_mut() {
                assign_expr_collations(pstate, cond);
            }
            if let Some(res) = w.result.as_mut() {
                assign_expr_collations(pstate, res);
            }
            no_collation(context);
            false
        }
        Node::CoalesceExpr(c) => {
            for arg in &mut c.args {
                assign_expr_collations(pstate, arg);
            }
            no_collation(context);
            false
        }
        Node::MinMaxExpr(m) => {
            for arg in &mut m.args {
                assign_expr_collations(pstate, arg);
            }
            no_collation(context);
            false
        }
        // CaseTestExpr is an uncollatable placeholder (M4 reaches it only in the
        // staged simple-CASE form, but the leaf walk is a no-op).
        Node::CaseTestExpr(_) => {
            no_collation(context);
            false
        }
        // M5 (step 26): an Aggref. Descend into its argument expressions (the
        // TargetEntry-wrapped inputs) so nested collatable expressions are visited;
        // the M5 aggregates (count/sum/min/max over int) produce uncollatable
        // results, so the node's own collation is InvalidOid. The aggregate's input
        // collation (inputcollid) selection over collatable inputs grows with text.
        Node::Aggref(agg) => {
            for arg in &mut agg.args {
                if let Node::TargetEntry(te) = arg
                    && let Some(expr) = te.expr.as_mut()
                {
                    assign_expr_collations(pstate, expr);
                } else {
                    assign_expr_collations(pstate, arg);
                }
            }
            no_collation(context);
            false
        }
        // M12 (step 42): a WindowFunc. Descend into its (plain) argument expressions;
        // the reachable window functions produce uncollatable results.
        Node::WindowFunc(w) => {
            for arg in &mut w.args {
                assign_expr_collations(pstate, arg);
            }
            no_collation(context);
            false
        }
        // M12 (step 44): a SubLink. Descend into the (already-transformed) testexpr
        // only; the sub-Query was collated when analyzed and is not re-walked.
        Node::SubLink(sl) => assign_sublink_collations(pstate, sl, context),
        // CollateExpr / ... (collation-bearing) grow per milestone.
        other => not_yet_reachable(other),
    }
}

/// PG `assign_collations_walker` SubLink arm (M12, step 44): assign collations to the
/// SubLink's testexpr (the ANY/ALL left-hand expression + combining op). The
/// sub-Query in `subselect` is NOT re-walked (PG's query_tree_walker skips
/// sub-selects). A SubLink's own result is boolean or the EXPR column type -- handled
/// as uncollatable here.
fn assign_sublink_collations(
    pstate: &mut ParseState,
    sl: &mut crate::nodes::primnodes::SubLink,
    context: &mut AssignCollationsContext,
) -> bool {
    if let Some(testexpr) = sl.testexpr.as_mut() {
        assign_expr_collations(pstate, testexpr);
    }
    no_collation(context);
    false
}

/// Set the walker context to "no collation" (an uncollatable result). The M4 node
/// types all produce uncollatable (numeric/int/date) results.
fn no_collation(context: &mut AssignCollationsContext) {
    context.collation = InvalidOid;
    context.strength = CollateStrength::None;
    context.location = -1;
}

/// Walk each argument of an OpExpr / FuncExpr / BoolExpr (the recursive descent of
/// the collation walker over a node's `args` list). Each child gets its own context
/// (PG threads a fresh per-child context, then merges; the M3 uncollatable case
/// needs no merge).
fn walk_args(pstate: &mut ParseState, node: &mut Node) {
    let args: Option<&mut Vec<Node>> = match node {
        Node::OpExpr(op) => Some(&mut op.args),
        Node::FuncExpr(f) => Some(&mut f.args),
        Node::BoolExpr(b) => Some(&mut b.args),
        _ => None,
    };
    if let Some(args) = args {
        for arg in args {
            assign_expr_collations(pstate, arg);
        }
    }
}

/// PG `select_common_collation`: choose the common collation of a set of
/// expressions. Not reached for M1 (no multi-input collatable expression yet);
/// grows with the operator/function transform paths.
pub fn select_common_collation(
    _pstate: &mut ParseState,
    _exprs: &[Node],
    _none_ok: bool,
) -> Oid {
    unimplemented!("select_common_collation: collatable multi-input expressions deferred");
}

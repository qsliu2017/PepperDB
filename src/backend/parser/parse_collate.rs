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
        if let Node::TargetEntry(te) = &mut **tle
            && let Some(expr) = te.expr.as_deref_mut() {
                assign_expr_collations(pstate, expr);
            }
    }
    if let Some(jointree) = query.jointree.as_deref_mut()
        && let Node::FromExpr(f) = jointree
            && let Some(quals) = f.quals.as_deref_mut() {
                assign_expr_collations(pstate, quals);
            }
}

/// PG `assign_list_collations`: assign collations to each expression in a list.
pub fn assign_list_collations(pstate: &mut ParseState, exprs: &mut [Box<Node>]) {
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
#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "grow scaffold: the M1 leaf arm only reads `node`, but the collation-\
              bearing arms added later call exprSetCollation through this &mut"
)]
fn assign_collations_walker(
    _pstate: &mut ParseState,
    node: &mut Node,
    context: &mut AssignCollationsContext,
) -> bool {
    match node {
        // An uncollatable leaf: its result type carries no collation, so it does
        // not influence its parent. PG's general default recurses first (no
        // children here) then leaves collation = InvalidOid / COLLATE_NONE.
        Node::Const(_) => {
            let collation = exprCollation(node);
            // int4 (and every M1 const) is uncollatable: nothing to propagate.
            crate::assert!(collation == InvalidOid);
            context.collation = InvalidOid;
            context.strength = CollateStrength::None;
            context.location = -1;
            false
        }
        // CollateExpr / FuncExpr / OpExpr / CaseExpr / ... (collation-bearing or
        // recursive) grow per milestone.
        other => not_yet_reachable(other),
    }
}

/// PG `select_common_collation`: choose the common collation of a set of
/// expressions. Not reached for M1 (no multi-input collatable expression yet);
/// grows with the operator/function transform paths.
pub fn select_common_collation(
    _pstate: &mut ParseState,
    _exprs: &[Box<Node>],
    _none_ok: bool,
) -> Oid {
    unimplemented!("select_common_collation: collatable multi-input expressions deferred");
}

//! Handle target lists. Translated from backend/parser/parse_target.c.
//!
//! Non-type-centric free functions (`transformTargetList`, `transformTargetEntry`,
//! `FigureColname`, ...); bodies here as snake_case `pub fn`s with the C symbol in
//! the doc comment, re-exported from `crate::parser::parse_target` under the C
//! names.
//!
//! Disposition: `grow`. For M1 the SELECT target-list path is translated end to
//! end: `transformTargetList` iterates the `ResTarget`s, `transformTargetEntry`
//! transforms each value expr and defaults its column name via `FigureColname`
//! (a bare constant has no name -> "?column?"). The `something.*` star-expansion
//! branch reaches `ExpandColumnRefStar` (range-table machinery, not translated)
//! and stages there; `FigureColnameInternal` is a grow dispatcher whose
//! self-contained arms are live and whose subquery/JSON arms route to a single
//! not-yet-reachable staging arm (rules.md s4).

use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{A_Expr_Kind, ResTarget};
use crate::nodes::primnodes::TargetEntry;
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_node::{ParseExprKind, ParseState};

/// PG `transformTargetList`: turn a list of raw `ResTarget`s into a list of
/// transformed `TargetEntry`s.
///
/// Each list element is a `ResTarget` node. The `something.*` expansion (handled
/// before the plain-expression path in PG) needs range-table machinery not
/// present for M1; a star target reaches `transformTargetEntry`'s deferred path.
pub fn transformTargetList(
    pstate: &mut ParseState,
    targetlist: Vec<Box<Node>>,
    expr_kind: ParseExprKind,
) -> Vec<Box<Node>> {
    // Shouldn't have any leftover multiassign items at start.
    crate::assert!(pstate.p_multiassign_exprs.is_empty());

    // PG expands "something.*" in SELECT and RETURNING (but not UPDATE) before the
    // plain-expression path; that expansion needs range-table machinery not
    // present for M1, so only the plain path below is wired here.
    let mut p_target: Vec<Box<Node>> = Vec::with_capacity(targetlist.len());
    for o_target in targetlist {
        let Node::ResTarget(res) = *o_target else {
            crate::elog!(
                crate::utils::elog::ERROR,
                "transformTargetList expected a ResTarget".to_string()
            );
            unreachable!("elog(ERROR) diverges");
        };
        let ResTarget { name, val, .. } = *res;

        // Star-expansion (res->val is a ColumnRef/A_Indirection ending in A_Star)
        // is handled by ExpandColumnRefStar / ExpandIndirectionStar in PG; those
        // need RTE expansion not translated for M1. The plain-expression path
        // below handles a single transformed expression.
        let tle = transformTargetEntry(pstate, val, None, expr_kind, name, false);
        p_target.push(Box::new(Node::TargetEntry(tle)));
    }

    // Multiassign resjunk items only arise in an UPDATE tlist (not M1); none to
    // attach here.
    p_target
}

/// PG `transformTargetEntry`: transform one target-list value into a
/// `TargetEntry`, defaulting the column name when none was given.
///
/// PG computes the column name from the *raw* node after transforming the expr;
/// since `FigureColname` only inspects the raw node's shape (independent of the
/// transform), we read the name before consuming the node into `transformExpr`.
pub fn transformTargetEntry(
    pstate: &mut ParseState,
    node: Option<Box<Node>>,
    expr: Option<Box<Node>>,
    expr_kind: ParseExprKind,
    colname: Option<String>,
    resjunk: bool,
) -> Box<TargetEntry> {
    // Generate the default column name from the raw node before it is consumed.
    // (EXPR_KIND_UPDATE_SOURCE SetToDefault passthrough is not reachable for M1.)
    let colname = match colname {
        Some(c) => Some(c),
        None if !resjunk => node.as_deref().map(FigureColname),
        None => None,
    };

    // Transform the node if the caller didn't do it already.
    let expr = expr.or_else(|| transformExpr(pstate, node, expr_kind));

    let resno = pstate.p_next_resno as crate::access::attnum::AttrNumber;
    pstate.p_next_resno += 1;
    Box::new(makeTargetEntry(expr, resno, colname, resjunk))
}

/// PG `FigureColname`: pick a column name for a target without an explicit AS.
/// Returns "?column?" when nothing can be guessed (e.g. a bare constant).
pub fn FigureColname(node: &Node) -> String {
    let mut name: Option<&str> = None;
    figure_colname_internal(node, &mut name);
    name.unwrap_or("?column?").to_string()
}

/// PG `FigureColnameInternal`: walk a raw expression for a name, returning a
/// "strength" (0 = nothing, 1 = weak/typecast, 2 = strong). File-local in
/// parse_target.c, so private here.
///
/// Grow dispatcher. The arms that name a node from a value node's string
/// (`T_ColumnRef`, `T_FuncCall`, `T_TypeCast`, `T_A_Indirection`) need value
/// nodes to be `Node` enum variants; those variants are not defined yet (see
/// `crate::nodes::value`), so those arms grow when value nodes land (M2+). The
/// arms with constant/recursive names are live now. A bare `A_Const` (and any
/// other unnamed leaf) falls through to strength 0 -> "?column?".
fn figure_colname_internal<'a>(node: &'a Node, name: &mut Option<&'a str>) -> i32 {
    match node {
        Node::A_Expr(a) => {
            if a.kind == A_Expr_Kind::NULLIF {
                // make nullif() act like a regular function
                *name = Some("nullif");
                return 2;
            }
            0
        }
        Node::CollateClause(c) => {
            c.arg.as_deref().map_or(0, |arg| figure_colname_internal(arg, name))
        }
        Node::GroupingFunc(_) => {
            *name = Some("grouping");
            2
        }
        Node::MergeSupportFunc(_) => {
            *name = Some("merge_action");
            2
        }
        // A_Const and every other unnamed leaf fall through to strength 0
        // ("?column?"). ColumnRef / FuncCall / TypeCast / A_Indirection (value-
        // node names) and SubLink / CaseExpr / JsonExpr (subquery/JSON names)
        // grow in later milestones.
        _ => 0,
    }
}

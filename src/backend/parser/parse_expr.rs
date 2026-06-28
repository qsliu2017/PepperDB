//! Handle the parse analysis of expressions. Translated from
//! backend/parser/parse_expr.c.
//!
//! Non-type-centric free functions (`transformExpr`, `ParseExprKindName`); bodies
//! here as snake_case `pub fn`s with the C symbol in the doc comment, re-exported
//! from `crate::parser::parse_expr` under the C names.
//!
//! Disposition: `grow`. `transform_expr_recurse` is parse_expr.c's central
//! `switch (nodeTag)` dispatcher. It is scaffolded so each later milestone fills
//! one arm (ColumnRef, A_Expr, FuncCall, ...) without restructuring; for M1 only
//! the `A_Const` arm (-> `make_const`) and the trivially-reachable NULL arm are
//! live. Every other tag routes through a single clearly-marked
//! `not_yet_reachable` staging arm (rules.md s4); none is half-written.

use crate::nodes::nodes::Node;
use crate::parser::parse_node::{ParseExprKind, ParseState};

/// Panic for an expression node tag whose transform arm reaches a subsystem not
/// yet translated for this milestone (rules.md s4). Distinct from PG's
/// `elog(ERROR, "unrecognized node type")` for a genuinely bad tag.
#[cold]
fn not_yet_reachable(node: &Node) -> ! {
    unimplemented!("transformExprRecurse: node tag not yet reachable for this milestone: {node:?}");
}

/// PG `transformExpr`: analyze and transform an expression, saving/restoring the
/// `p_expr_kind` identity around the recursion so context-specific error messages
/// are correct. A NULL input transforms to NULL.
pub fn transformExpr(
    pstate: &mut ParseState,
    expr: Option<Box<Node>>,
    expr_kind: ParseExprKind,
) -> Option<Box<Node>> {
    crate::assert!(expr_kind != ParseExprKind::None);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = expr_kind;

    let result = transform_expr_recurse(pstate, expr);

    pstate.p_expr_kind = sv_expr_kind;
    result
}

/// PG `transformExprRecurse`: the per-nodetag transform dispatcher (file-local in
/// parse_expr.c, so private here). Grows one arm per milestone.
fn transform_expr_recurse(pstate: &mut ParseState, expr: Option<Box<Node>>) -> Option<Box<Node>> {
    // Need do nothing for an empty subexpression.
    let expr = expr?;

    // PG guards recursion depth with check_stack_depth(); the recursive descent
    // here is bounded by the same call graph and grows with it.
    match *expr {
        Node::A_Const(aconst) => {
            Some(Box::new(Node::Const(crate::parser::parse_node::make_const(pstate, &aconst))))
        }
        // ColumnRef / ParamRef / A_Expr / FuncCall / TypeCast / ... arms are
        // filled by later milestones; for M1 they route here cleanly.
        other => not_yet_reachable(&other),
    }
}

/// PG `ParseExprKindName`: the user-facing name of a `ParseExprKind`, for error
/// messages ("... is not allowed in WHERE", etc).
#[allow(
    clippy::match_same_arms,
    reason = "1:1 with PG's per-ParseExprKind switch; distinct kinds share a label \
              (WHERE/COPY WHERE, VALUES, RETURNING) - merging arms loses the mapping"
)]
pub fn ParseExprKindName(expr_kind: ParseExprKind) -> &'static str {
    use ParseExprKind as K;
    match expr_kind {
        K::None => "invalid expression context",
        K::Other => "extension expression",
        K::JoinOn => "JOIN/ON",
        K::JoinUsing => "JOIN/USING",
        K::FromSubselect => "sub-SELECT in FROM",
        K::FromFunction => "function in FROM",
        K::Where => "WHERE",
        K::Policy => "POLICY",
        K::Having => "HAVING",
        K::Filter => "FILTER",
        K::WindowPartition => "window PARTITION BY",
        K::WindowOrder => "window ORDER BY",
        K::WindowFrameRange => "window RANGE",
        K::WindowFrameRows => "window ROWS",
        K::WindowFrameGroups => "window GROUPS",
        K::SelectTarget => "SELECT",
        K::InsertTarget => "INSERT",
        K::UpdateSource | K::UpdateTarget => "UPDATE",
        K::MergeWhen => "MERGE WHEN",
        K::GroupBy => "GROUP BY",
        K::OrderBy => "ORDER BY",
        K::DistinctOn => "DISTINCT ON",
        K::Limit => "LIMIT",
        K::Offset => "OFFSET",
        K::Returning | K::MergeReturning => "RETURNING",
        K::Values | K::ValuesSingle => "VALUES",
        K::CheckConstraint | K::DomainCheck => "CHECK",
        K::ColumnDefault | K::FunctionDefault => "DEFAULT",
        K::IndexExpression => "index expression",
        K::IndexPredicate => "index predicate",
        K::StatsExpression => "statistics expression",
        K::AlterColTransform => "USING",
        K::ExecuteParameter => "EXECUTE",
        K::TriggerWhen => "WHEN",
        K::PartitionBound => "partition bound",
        K::PartitionExpression => "PARTITION BY",
        K::CallArgument => "CALL",
        K::CopyWhere => "WHERE",
        K::GeneratedColumn => "GENERATED AS",
        K::CycleMark => "CYCLE",
    }
}

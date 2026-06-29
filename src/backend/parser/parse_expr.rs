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
use crate::nodes::parsenodes::{ColumnRef, ColumnRefField};
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
    expr: Option<Node>,
    expr_kind: ParseExprKind,
) -> Option<Node> {
    crate::assert!(expr_kind != ParseExprKind::None);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = expr_kind;

    let result = transform_expr_recurse(pstate, expr);

    pstate.p_expr_kind = sv_expr_kind;
    result
}

/// PG `transformExprRecurse`: the per-nodetag transform dispatcher (file-local in
/// parse_expr.c, so private here). Grows one arm per milestone.
fn transform_expr_recurse(pstate: &mut ParseState, expr: Option<Node>) -> Option<Node> {
    // Need do nothing for an empty subexpression.
    let expr = expr?;

    // PG guards recursion depth with check_stack_depth(); the recursive descent
    // here is bounded by the same call graph and grows with it.
    match expr {
        Node::A_Const(aconst) => {
            Some(Node::Const(crate::parser::parse_node::make_const(pstate, &aconst)))
        }
        Node::ColumnRef(cref) => Some(transform_column_ref(pstate, &cref)),
        Node::A_Expr(aexpr) => Some(transform_a_expr(pstate, *aexpr)),
        Node::BoolExpr(bexpr) => Some(transform_bool_expr(pstate, *bexpr)),
        Node::FuncCall(fc) => Some(transform_func_call(pstate, &fc)),
        // ParamRef / TypeCast / CaseExpr / ... arms are filled by later milestones;
        // for now they route here cleanly.
        other => not_yet_reachable(&other),
    }
}

/// PG `transformAExprOp` (the AEXPR_OP arm of transformAExpr): transform both
/// operands and resolve the operator into an `OpExpr` via `make_op`. M3 reaches the
/// plain binary operator; the row-comparison / "expr op (subselect)" / scalar-array
/// special cases grow at their milestones.
fn transform_a_expr(pstate: &mut ParseState, a: crate::nodes::parsenodes::A_Expr) -> Node {
    use crate::nodes::parsenodes::A_Expr_Kind;
    if a.kind != A_Expr_Kind::OP {
        unimplemented!("transformAExpr: non-OP A_Expr kind not yet reachable for this milestone");
    }
    let last_srf = pstate.p_last_srf.clone();
    let lexpr = transform_expr_recurse(pstate, a.lexpr);
    let rexpr = transform_expr_recurse(pstate, a.rexpr);
    crate::parser::parse_oper::make_op(pstate, &a.name, lexpr, rexpr, last_srf.as_ref(), a.location)
}

/// PG `transformBoolExpr`: transform each argument to bool and build the BoolExpr.
/// Each argument is coerced to boolean (`coerce_to_boolean`); for M3 the AND/OR/NOT
/// operands are already boolean expressions (comparisons), so the coercion is the
/// identity. The non-boolean-argument coercion grows with `coerce_to_boolean`.
fn transform_bool_expr(pstate: &mut ParseState, b: crate::nodes::primnodes::BoolExpr) -> Node {
    let args = b
        .args
        .into_iter()
        .map(|arg| {
            transform_expr_recurse(pstate, Some(arg))
                .unwrap_or_else(|| not_yet_reachable_msg("transformBoolExpr: NULL argument"))
        })
        .collect();
    Node::BoolExpr(Box::new(crate::nodes::primnodes::BoolExpr {
        boolop: b.boolop,
        args,
        location: b.location,
    }))
}

/// PG `transformFuncCall` -> `ParseFuncOrColumn`: transform the argument list and
/// resolve the function (or column projection) into a FuncExpr.
fn transform_func_call(pstate: &mut ParseState, fc: &crate::nodes::parsenodes::FuncCall) -> Node {
    let args = fc
        .args
        .iter()
        .cloned()
        .map(|arg| {
            transform_expr_recurse(pstate, Some(arg))
                .unwrap_or_else(|| not_yet_reachable_msg("transformFuncCall: NULL argument"))
        })
        .collect();
    crate::parser::parse_func::ParseFuncOrColumn(pstate, &fc.funcname, args, fc, fc.location)
}

#[cold]
fn not_yet_reachable_msg(msg: &str) -> ! {
    unimplemented!("{msg}");
}

/// PG `transformColumnRef`: resolve a `ColumnRef` to a `Var` (or whole-row ref).
/// M2 covers an unqualified column (`a`) and a table-qualified column (`t.a`); the
/// 3-part (schema-qualified) form, whole-row `t.*`, the pre/post columnref hooks,
/// and the backwards-compatible bare-relation-name path grow at their milestones.
fn transform_column_ref(pstate: &mut ParseState, cref: &ColumnRef) -> Node {
    use crate::backend::parser::parse_relation::{col_name_to_var, scan_ns_item_for_column};

    match cref.fields.as_slice() {
        [ColumnRefField::String(field1)] => {
            let colname = &field1.sval;
            col_name_to_var(pstate, colname, false, cref.location)
                .unwrap_or_else(|| undefined_column(colname))
        }
        [ColumnRefField::String(field1), ColumnRefField::String(field2)] => {
            let relname = &field1.sval;
            let colname = &field2.sval;
            let idx = refname_namespace_item(pstate, relname)
                .unwrap_or_else(|| missing_from_entry(relname));
            scan_ns_item_for_column(&pstate.p_namespace[idx], 0, colname, cref.location)
                .unwrap_or_else(|| undefined_column(colname))
        }
        [.., ColumnRefField::Star(_)] => {
            unimplemented!("transformColumnRef: whole-row (table.*) reference not yet translated for this milestone");
        }
        _ => unimplemented!("transformColumnRef: 3+-part / schema-qualified column reference not yet translated for this milestone"),
    }
}

/// PG `refnameNamespaceItem` (M2 subset): find the index of a namespace item whose
/// refname (eref aliasname) equals `relname`. Only the current level is searched
/// (no parent-level / schema-qualified lookup yet).
fn refname_namespace_item(pstate: &ParseState, relname: &str) -> Option<usize> {
    pstate
        .p_namespace
        .iter()
        .position(|ns| ns.rel_visible && ns.names.aliasname.as_deref() == Some(relname))
}

#[cold]
fn undefined_column(colname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("column \"{colname}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn missing_from_entry(relname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE).errmsg(format!(
            "missing FROM-clause entry for table \"{relname}\""
        ));
    });
    unreachable!("ereport(ERROR) diverges");
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

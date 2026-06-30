//! Aggregate-call and aggregate-check handling for the parser. Translated from
//! backend/parser/parse_agg.c.
//!
//! Non-type-centric free functions (`transformAggregateCall`,
//! `parseCheckAggregates`); bodies here as snake_case `pub fn`s, re-exported under
//! the C names from the header `crate::parser::parse_agg`.
//!
//! Disposition: `grow`. M5 (step 26) reaches the regular-aggregate path: a
//! `count(*)` / `count(expr)` / `sum/min/max(expr)` call -- already resolved to its
//! `aggfnoid`/`aggtype` by `func_get_detail` -- has its argument list wrapped in
//! `TargetEntry`s (the Aggref `args` tlist), its `aggargtypes` filled, and
//! `pstate.p_has_aggs` set; `parseCheckAggregates` then verifies the targetlist /
//! HAVING reference only grouped columns or aggregates. ORDER BY / DISTINCT inside
//! the aggregate, ordered-set/hypothetical aggregates, FILTER, grouping sets, and
//! the levelsup/window machinery are clean grow guards (rules.md s4).

use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::nodes::primnodes::Aggref;
use crate::parser::parse_node::ParseState;

/// Panic for an aggregate path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `transformAggregateCall`: finish building an `Aggref` whose `aggfnoid` /
/// `aggtype` / `aggstar` are already set (by `ParseFuncOrColumn`'s aggregate arm).
/// Wraps the transformed argument expressions in a `TargetEntry` list (PG's
/// `agg->args`), records the input argument types in `aggargtypes`, and marks the
/// parse state as containing aggregates.
///
/// M5 reaches a normal aggregate over a (possibly empty for `count(*)`) positional
/// argument list. DISTINCT and ORDER BY inside the aggregate, ordered-set /
/// hypothetical-set kinds, and the agg-level (`agglevelsup`) / nesting checks grow
/// at their milestones.
#[allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: transformAggregateCall takes aggorder by value (List *aggorder); consumed once the ORDER-BY-in-aggregate path lands"
)]
pub fn transformAggregateCall(
    pstate: &mut ParseState,
    agg: &mut Aggref,
    args: Vec<Node>,
    aggorder: Vec<Node>,
    agg_distinct: bool,
) {
    if agg_distinct {
        not_yet_reachable("transformAggregateCall: DISTINCT aggregate");
    }
    if !aggorder.is_empty() {
        not_yet_reachable("transformAggregateCall: ORDER BY in aggregate");
    }

    // Build the arg type list and wrap each argument in a TargetEntry. count(*) has
    // no arguments (aggstar); a normal aggregate has one positional arg in M5.
    let mut argtypes: Vec<crate::postgres_ext::Oid> = Vec::with_capacity(args.len());
    let mut tlist: Vec<Node> = Vec::with_capacity(args.len());
    for (i, arg) in args.into_iter().enumerate() {
        argtypes.push(exprType(&arg));
        let attno = (i + 1) as crate::access::attnum::AttrNumber;
        tlist.push(Node::TargetEntry(Box::new(makeTargetEntry(Some(arg), attno, None, false))));
    }

    agg.aggargtypes = argtypes;
    agg.args = tlist;
    agg.aggorder = Vec::new();
    agg.aggdistinct = Vec::new();
    // aggkind 'n' (normal) was set by the caller; aggtranstype is resolved by the
    // planner (resolve_aggregate_transtype) once the input types are final.

    // The aggregate's level is the current query level (no outer-level refs in M5);
    // check_agglevels_and_constraints (placement / nesting / FILTER) grows later.
    pstate.p_has_aggs = true;
}

/// PG `transformWindowFuncCall`: link a `WindowFunc` to its `WindowClause` by
/// recording its `WindowDef` in `pstate.p_windowdefs` and setting `winref` to the
/// 1-based position of that def. A reference (`OVER name`, only `refname` set)
/// resolves against a named WINDOW definition; an inline `OVER (...)` def is matched
/// against the existing inline defs (deduplicated when identical) or appended.
///
/// M12 (step 42): the no-nested-window / placement (SELECT/ORDER BY only) checks are
/// staged behind the milestone tlists (the window call already arrives only from the
/// SELECT target list); they grow with the general expression-kind walker.
pub fn transformWindowFuncCall(
    pstate: &mut ParseState,
    wfunc: &mut crate::nodes::primnodes::WindowFunc,
    windef: &crate::nodes::parsenodes::WindowDef,
) {
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::WindowDef;

    if let Some(refname) = windef.refname.as_ref() {
        // `OVER name`: reference a named WINDOW definition. Find it by name.
        let pos = pstate.p_windowdefs.iter().position(|n| {
            matches!(n, Node::WindowDef(d) if d.name.as_deref() == Some(refname.as_str()))
        });
        let Some(pos) = pos else {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("window \"{refname}\" does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        };
        wfunc.winref = (pos + 1) as crate::c::Index;
    } else {
        // Inline `OVER (...)`: reuse an identical anonymous def if one exists, else
        // append. Identity is the partition/order/frame triple (PG compares the
        // WindowDef fields; the milestone defs carry no name/refname).
        let matched = pstate.p_windowdefs.iter().position(|n| {
            matches!(n, Node::WindowDef(d)
                if d.name.is_none()
                    && d.partitionClause == windef.partitionClause
                    && d.orderClause == windef.orderClause
                    && d.frameOptions == windef.frameOptions
                    && d.startOffset == windef.startOffset
                    && d.endOffset == windef.endOffset)
        });
        let pos = matched.unwrap_or_else(|| {
            pstate.p_windowdefs.push(Node::WindowDef(Box::new(WindowDef {
                name: None,
                refname: None,
                partitionClause: windef.partitionClause.clone(),
                orderClause: windef.orderClause.clone(),
                frameOptions: windef.frameOptions,
                startOffset: windef.startOffset.clone(),
                endOffset: windef.endOffset.clone(),
                location: windef.location,
            })));
            pstate.p_windowdefs.len() - 1
        });
        wfunc.winref = (pos + 1) as crate::c::Index;
    }

    pstate.p_has_window_funcs = true;
}

/// PG `parseCheckAggregates`: after the SELECT is otherwise transformed, verify the
/// query is a valid aggregate/grouped query -- every targetlist (and HAVING)
/// expression must be either an aggregate input or built only from the GROUP BY
/// columns. M5 enforces the core rule for plain GROUP BY columns and aggregates;
/// grouping sets, the RTE_GROUP rewrite, and the ungrouped-Var error detail grow at
/// their milestones.
pub fn parseCheckAggregates(pstate: &mut ParseState, qry: &mut Query) {
    crate::assert!(
        pstate.p_has_aggs || !qry.groupClause.is_empty() || qry.havingQual.is_some()
    );

    if !qry.groupingSets.is_empty() {
        not_yet_reachable("parseCheckAggregates: grouping sets");
    }
    if qry.havingQual.is_some() {
        not_yet_reachable("parseCheckAggregates: HAVING qual");
    }

    // The set of group-by target expressions (by sortgroupref -> TargetEntry expr).
    let grouped: Vec<Node> = qry
        .groupClause
        .iter()
        .filter_map(|gc| {
            let Node::SortGroupClause(sgc) = gc else { return None };
            get_sortgroupclause_expr(sgc.tleSortGroupRef, &qry.targetList)
        })
        .collect();

    // Every non-aggregate targetlist expression must be derivable from the grouped
    // expressions (M5: it must BE one of them, or be a constant/aggregate). The full
    // check_ungrouped_columns recursion (rejecting ungrouped Vars deep in an expr)
    // grows with richer grouped expressions; the milestone tlist is flat.
    for te in &qry.targetList {
        let Node::TargetEntry(te) = te else { continue };
        let Some(expr) = te.expr.as_ref() else { continue };
        check_grouped_expr(expr, &grouped);
    }
}

/// Verify a targetlist expression is grouping-legal: an aggregate, a constant, or a
/// member of the grouped expression set. A bare `Var` not in a group clause is the
/// classic "column must appear in GROUP BY or be used in an aggregate" error.
fn check_grouped_expr(expr: &Node, grouped: &[Node]) {
    match expr {
        // Aggregates consume their (ungrouped) inputs legally; constants are always
        // legal.
        Node::Aggref(_) | Node::GroupingFunc(_) | Node::Const(_) => {}
        // Otherwise the expression itself must be a grouped expression.
        other => {
            if grouped.iter().any(|g| g == other) {
                return;
            }
            // A non-grouped Var (or expression over one) without an enclosing
            // aggregate is the ungrouped-column error.
            if contains_var(other) {
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_GROUPING_ERROR).errmsg(
                        "column must appear in the GROUP BY clause or be used in an aggregate function"
                            .to_owned(),
                    );
                });
                unreachable!("ereport(ERROR) diverges");
            }
        }
    }
}

/// Whether an expression tree contains a `Var` (an ungrouped column reference). A
/// shallow walk over the M5-reachable expression kinds.
fn contains_var(expr: &Node) -> bool {
    match expr {
        Node::Var(_) => true,
        Node::OpExpr(op) | Node::NullIfExpr(op) => op.args.iter().any(contains_var),
        Node::FuncExpr(f) => f.args.iter().any(contains_var),
        Node::BoolExpr(b) => b.args.iter().any(contains_var),
        Node::RelabelType(r) => r.arg.as_ref().is_some_and(contains_var),
        Node::CoerceViaIO(c) => c.arg.as_ref().is_some_and(contains_var),
        // Aggref inputs are legal; everything else carries no ungrouped Var.
        _ => false,
    }
}

/// PG `get_sortgroupclause_tle`'s expr accessor: the targetlist entry expression
/// matching the given `ressortgroupref`.
fn get_sortgroupclause_expr(sortgroupref: crate::c::Index, tlist: &[Node]) -> Option<Node> {
    tlist.iter().find_map(|n| {
        let Node::TargetEntry(te) = n else { return None };
        if te.ressortgroupref == sortgroupref {
            te.expr.clone()
        } else {
            None
        }
    })
}

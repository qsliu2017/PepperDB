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

    // The set of group-by target expressions (by sortgroupref -> TargetEntry expr).
    let grouped: Vec<Node> = qry
        .groupClause
        .iter()
        .filter_map(|gc| {
            let Node::SortGroupClause(sgc) = gc else { return None };
            get_sortgroupclause_expr(sgc.tleSortGroupRef, &qry.targetList)
        })
        .collect();

    // Every non-aggregate targetlist / HAVING expression must be built only from
    // grouped expressions, aggregates, and constants (PG check_ungrouped_columns:
    // a subexpression equal to a grouping expression is legal as a whole; a Var
    // not under an aggregate and not grouped is the classic error).
    for te in &qry.targetList {
        let Node::TargetEntry(te) = te else { continue };
        let Some(expr) = te.expr.as_ref() else { continue };
        check_ungrouped_columns(expr, &grouped, qry);
    }
    if let Some(having) = qry.havingQual.clone() {
        check_ungrouped_columns(&having, &grouped, qry);
    }
}

/// PG `check_ungrouped_columns_walker` (regular-Var subset): an expression equal to
/// a grouped expression is legal (don't descend); an Aggref aggregates its inputs
/// (legal); a bare current-level `Var` is the ungrouped-column error; otherwise
/// recurse into the reachable containers.
#[allow(
    clippy::match_same_arms,
    reason = "1:1 with PG check_ungrouped_columns_walker: the aggregate arm is a \
              deliberate stop-descent, distinct in intent from the leaf default"
)]
fn check_ungrouped_columns(expr: &Node, grouped: &[Node], qry: &Query) {
    if grouped.iter().any(|g| g == expr) {
        return;
    }
    match expr {
        // Aggregates consume their (ungrouped) inputs legally at this level.
        Node::Aggref(_) | Node::GroupingFunc(_) => {}
        Node::Var(v) if v.varlevelsup == 0 => ungrouped_var_error(v, qry),
        Node::OpExpr(op) | Node::NullIfExpr(op) | Node::DistinctExpr(op) => {
            for a in &op.args {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::FuncExpr(f) => {
            for a in &f.args {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::BoolExpr(b) => {
            for a in &b.args {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_ref() {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::CoerceViaIO(c) => {
            if let Some(a) = c.arg.as_ref() {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::BooleanTest(b) => {
            if let Some(a) = b.arg.as_ref() {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::CoalesceExpr(c) => {
            for a in &c.args {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::MinMaxExpr(m) => {
            for a in &m.args {
                check_ungrouped_columns(a, grouped, qry);
            }
        }
        Node::CaseExpr(c) => {
            if let Some(a) = c.arg.as_ref() {
                check_ungrouped_columns(a, grouped, qry);
            }
            for w in &c.args {
                check_ungrouped_columns(w, grouped, qry);
            }
            if let Some(d) = c.defresult.as_ref() {
                check_ungrouped_columns(d, grouped, qry);
            }
        }
        Node::CaseWhen(w) => {
            if let Some(e) = w.expr.as_ref() {
                check_ungrouped_columns(e, grouped, qry);
            }
            if let Some(r) = w.result.as_ref() {
                check_ungrouped_columns(r, grouped, qry);
            }
        }
        // Consts / Params / already-planned SubPlans carry no ungrouped Var.
        _ => {}
    }
}

/// The `column "rel.col" must appear in the GROUP BY clause or be used in an
/// aggregate function` error, with the Var's parse position (PG
/// check_ungrouped_columns_walker's regular-Var ereport).
#[cold]
fn ungrouped_var_error(v: &crate::nodes::primnodes::Var, qry: &Query) -> ! {
    // rte->eref->aliasname + get_rte_attribute_name(rte, varattno).
    let (relname, attname) = rte_col_names(qry, v.varno, v.varattno);
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_GROUPING_ERROR).errmsg(format!(
            "column \"{relname}.{attname}\" must appear in the GROUP BY clause or be used in an aggregate function"
        ));
        if v.location >= 0 {
            e.errposition(v.location + 1); // parser_errposition: 1-based
        }
    });
    unreachable!("ereport(ERROR) diverges");
}

/// The RTE alias name + column name for a (varno, varattno) pair.
fn rte_col_names(qry: &Query, varno: i32, varattno: i16) -> (String, String) {
    let rte = qry.rtable.get((varno - 1) as usize);
    let Some(Node::RangeTblEntry(rte)) = rte else {
        return ("?".to_owned(), "?".to_owned());
    };
    let relname = rte
        .eref
        .as_ref()
        .and_then(|a| a.aliasname.clone())
        .unwrap_or_else(|| "?".to_owned());
    let attname = rte
        .eref
        .as_ref()
        .and_then(|a| a.colnames.get((varattno - 1) as usize).cloned())
        .map_or_else(|| "?".to_owned(), |s| s.sval);
    (relname, attname)
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

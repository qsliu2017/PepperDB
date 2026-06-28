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

use crate::access::attnum::AttrNumber;
use crate::nodes::makefuncs::{makeTargetEntry, makeVar};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{AclMode, A_Expr_Kind, ColumnRef, ColumnRefField, RTEKind, ResTarget};
use crate::nodes::primnodes::{TargetEntry, VarReturningType};
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_node::{ParseExprKind, ParseNamespaceItem, ParseState};

/// PG `transformTargetList`: turn a list of raw `ResTarget`s into a list of
/// transformed `TargetEntry`s.
///
/// Each list element is a `ResTarget` node. The `something.*` expansion (handled
/// before the plain-expression path in PG) needs range-table machinery not
/// present for M1; a star target reaches `transformTargetEntry`'s deferred path.
pub fn transformTargetList(
    pstate: &mut ParseState,
    targetlist: Vec<Node>,
    expr_kind: ParseExprKind,
) -> Vec<Node> {
    // Shouldn't have any leftover multiassign items at start.
    crate::assert!(pstate.p_multiassign_exprs.is_empty());

    // PG expands "something.*" in SELECT and RETURNING (but not UPDATE) before the
    // plain-expression path.
    let expand_star = expr_kind != ParseExprKind::UpdateSource;
    let mut p_target: Vec<Node> = Vec::with_capacity(targetlist.len());
    for o_target in targetlist {
        let Node::ResTarget(res) = o_target else {
            crate::elog!(
                crate::utils::elog::ERROR,
                "transformTargetList expected a ResTarget".to_string()
            );
            unreachable!("elog(ERROR) diverges");
        };
        let ResTarget { name, val, .. } = *res;

        // Check for "something.*": the star appears as the last field of a
        // ColumnRef. (A_Indirection ending in A_Star grows with indirection.)
        if expand_star
            && let Some(Node::ColumnRef(cref)) = val.as_ref()
            && matches!(cref.fields.last(), Some(ColumnRefField::Star(_)))
        {
            p_target.extend(expand_column_ref_star(pstate, cref));
            continue;
        }

        let tle = transformTargetEntry(pstate, val, None, expr_kind, name, false);
        p_target.push(Node::TargetEntry(tle));
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
    node: Option<Node>,
    expr: Option<Node>,
    expr_kind: ParseExprKind,
    colname: Option<String>,
    resjunk: bool,
) -> Box<TargetEntry> {
    // Generate the default column name from the raw node before it is consumed.
    // (EXPR_KIND_UPDATE_SOURCE SetToDefault passthrough is not reachable for M1.)
    let colname = match colname {
        Some(c) => Some(c),
        None if !resjunk => node.as_ref().map(FigureColname),
        None => None,
    };

    // Transform the node if the caller didn't do it already.
    let expr = expr.or_else(|| transformExpr(pstate, node, expr_kind));

    let resno = pstate.p_next_resno as crate::access::attnum::AttrNumber;
    pstate.p_next_resno += 1;
    Box::new(makeTargetEntry(expr, resno, colname, resjunk))
}

/// PG `ExpandColumnRefStar`: expand a `something.*` target. M2 covers the bare `*`
/// form (expand all tables in the namespace); the `relation.*` form grows with the
/// whole-row / hook machinery.
fn expand_column_ref_star(pstate: &mut ParseState, cref: &ColumnRef) -> Vec<Node> {
    if cref.fields.len() == 1 {
        // Bare '*': expand all tables.
        expand_all_tables(pstate, cref.location)
    } else {
        unimplemented!("ExpandColumnRefStar: relation.* expansion not yet translated for this milestone")
    }
}

/// PG `ExpandAllTables`: expand a bare `*` into the columns of every col-visible
/// namespace item. Errors if there is no table to expand (`SELECT *` with no FROM).
fn expand_all_tables(pstate: &mut ParseState, location: i32) -> Vec<Node> {
    let mut target: Vec<Node> = Vec::new();
    let mut found_table = false;
    // Index loop so the per-nsitem borrow ends before bumping p_next_resno.
    for idx in 0..pstate.p_namespace.len() {
        if !pstate.p_namespace[idx].cols_visible {
            continue;
        }
        crate::assert!(!pstate.p_namespace[idx].lateral_only);
        found_table = true;
        let mut tes = expand_ns_item_attrs(pstate, idx, location);
        target.append(&mut tes);
    }
    if !found_table {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
                .errmsg("SELECT * with no tables specified is not valid".to_string());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    target
}

/// PG `expandNSItemAttrs`: build a `TargetEntry` per live column of the nsitem,
/// each over a `Var` (from `expandNSItemVars`), assigning sequential resnos. Marks
/// the relation's perminfo as needing SELECT. (`markVarForSelectPriv` per-column
/// detail grows with column-level privileges.)
fn expand_ns_item_attrs(pstate: &mut ParseState, ns_idx: usize, location: i32) -> Vec<Node> {
    let (vars, names) = expand_ns_item_vars(&pstate.p_namespace[ns_idx], 0, location);

    // Require read access to the table (redundant with per-column for non-empty
    // rels, but needed for a zero-column table).
    if pstate.p_namespace[ns_idx].rte.rtekind == RTEKind::RELATION {
        let perminfo_index = pstate.p_namespace[ns_idx].rte.perminfoindex;
        pstate.p_rteperminfos[perminfo_index - 1].requiredPerms |= AclMode::SELECT;
    }

    vars.into_iter()
        .zip(names)
        .map(|(var, label)| {
            let resno = pstate.p_next_resno as AttrNumber;
            pstate.p_next_resno += 1;
            Node::TargetEntry(Box::new(makeTargetEntry(Some(var), resno, Some(label), false)))
        })
        .collect()
}

/// PG `expandNSItemVars`: build a `Var` for each live (non-dropped) column of the
/// nsitem, returning the Vars and their colnames in parallel.
fn expand_ns_item_vars(
    nsitem: &ParseNamespaceItem,
    sublevels_up: crate::c::Index,
    location: i32,
) -> (Vec<Node>, Vec<String>) {
    let mut vars = Vec::new();
    let mut names = Vec::new();
    for (colindex, colnameval) in nsitem.names.colnames.iter().enumerate() {
        let colname = &colnameval.sval;
        let nscol = &nsitem.nscolumns[colindex];
        if nscol.dontexpand {
            continue;
        }
        if colname.is_empty() {
            // dropped column, ignore
            crate::assert!(nscol.varno == 0);
            continue;
        }
        crate::assert!(nscol.varno > 0);
        let mut var = makeVar(
            nscol.varno as i32,
            nscol.varattno,
            nscol.vartype,
            nscol.vartypmod,
            nscol.varcollid,
            sublevels_up,
        );
        var.varreturningtype = VarReturningType::DEFAULT;
        var.varnosyn = nscol.varnosyn;
        var.varattnosyn = nscol.varattnosyn;
        var.location = location;
        vars.push(Node::Var(Box::new(var)));
        names.push(colname.clone());
    }
    (vars, names)
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
            c.arg.as_ref().map_or(0, |arg| figure_colname_internal(arg, name))
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

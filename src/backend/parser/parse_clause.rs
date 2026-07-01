//! Handle clauses of a SELECT/INSERT (FROM, WHERE, ...). Translated from
//! backend/parser/parse_clause.c.
//!
//! Non-type-centric free functions (`transformFromClause`,
//! `transformFromClauseItem`, `setTargetTable`, ...); bodies here as snake_case
//! `pub fn`s with the C symbol in the doc comment, re-exported from
//! `crate::parser::parse_clause` under the C names.
//!
//! Disposition: `grow`. M2's live path is a FROM clause of plain table refs and
//! an INSERT target table: `transformFromClause` -> `transformFromClauseItem`
//! opens each RangeVar (a relcache lookup; ASYNC because the open is a lock-wait
//! leaf, rules.md s5) and builds its RTE via `addRangeTableEntryForRelation`,
//! adding a RangeTblRef to the joinlist; `setTargetTable` opens the INSERT target.
//! JOIN syntax, subquery/function FROM items, aliases, WHERE/LIMIT/GROUP/sort
//! clauses, and the namespace-conflict / LATERAL bookkeeping are grow guards
//! (rules.md s4).


use std::sync::Arc;

use crate::backend::parser::parse_relation::{
    add_ns_item_to_query, add_range_table_entry_for_relation,
};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::AclMode;
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::{ParseNamespaceItem, ParseState};
use crate::shared_state::SharedState;
use crate::storage::lockdefs::LockMode;

/// Panic for a parse_clause path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `transformFromClause`: transform each FROM item, adding its RTE to the
/// rangetable and a RangeTblRef to the joinlist, and exposing it in the namespace.
/// M2 covers plain table references (no JOIN/subquery/function items, no LATERAL).
pub async fn transform_from_clause(shared: &Arc<SharedState>, pstate: &mut ParseState, frm_list: Vec<Node>) {
    for n in frm_list {
        let (rtr, nsitem) = transform_from_clause_item(shared, pstate, n).await;
        // checkNameSpaceConflicts / setNamespaceLateralState grow with multi-item
        // FROM + LATERAL; a single plain ref has no conflict to check.
        pstate.p_joinlist.push(rtr);
        add_ns_item_to_query(pstate, nsitem, false, true, true);
    }
}

/// PG `transformFromClauseItem` (RangeVar arm): a plain table reference becomes an
/// `RTE_RELATION` plus a `RangeTblRef`. Returns the RangeTblRef node and the
/// nsitem (the caller adds them to the joinlist/namespace).
async fn transform_from_clause_item(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    n: Node,
) -> (Node, ParseNamespaceItem) {
    // gram.y `transformFromClauseItem` (RangeFunction arm): a function call in FROM.
    if let Node::RangeFunction(r) = &n {
        let _ = shared;
        let nsitem = transform_range_function(pstate, r);
        let rtr = Node::RangeTblRef(Box::new(crate::nodes::primnodes::RangeTblRef {
            rtindex: nsitem.rtindex,
        }));
        return (rtr, nsitem);
    }

    let Node::RangeVar(rv) = n else {
        not_yet_reachable("transformFromClauseItem: non-RangeVar FROM item (join/subquery)");
    };
    // getRTEForSpecialRelationTypes: an unqualified RangeVar matching a visible CTE
    // name becomes an RTE_CTE (the WITH-query reference), not a table open.
    let nsitem = if let Some((cte, levelsup)) = scan_namespace_for_cte(pstate, &rv) {
        let item = crate::backend::parser::parse_relation::add_range_table_entry_for_cte(
            pstate, &cte, levelsup, &rv, true,
        );
        // Bump the CTE's refcount on a non-self reference (the body uses it).
        if matches!(cte.ctequery, Some(Node::Query(_))) {
            bump_cte_refcount(pstate, cte.ctename.as_deref());
        }
        item
    } else {
        transform_table_entry(shared, pstate, &rv).await
    };
    let rtr = Node::RangeTblRef(Box::new(crate::nodes::primnodes::RangeTblRef {
        rtindex: nsitem.rtindex,
    }));
    (rtr, nsitem)
}

/// PG `transformRangeFunction`: transform a function-in-FROM item into an
/// `RTE_FUNCTION` nsitem. Each raw function expression is transformed with
/// `EXPR_KIND_FROM_FUNCTION` (like a SELECT output expr), its display name is
/// captured via `FigureColname`, collations are assigned, and the RTE is built by
/// `addRangeTableEntryForFunction`. M8 reaches the single-function, no-coldeflist,
/// no-ORDINALITY, non-LATERAL form (the SRF/record functions the type tests use);
/// UNNEST expansion, ROWS FROM(), coldeflists, and LATERAL cross-refs grow later.
fn transform_range_function(
    pstate: &mut ParseState,
    r: &crate::nodes::parsenodes::RangeFunction,
) -> ParseNamespaceItem {
    use crate::backend::parser::parse_collate::assign_list_collations;
    use crate::backend::parser::parse_target::FigureColname;
    use crate::parser::parse_expr::transformExpr;
    use crate::parser::parse_node::ParseExprKind;

    // We make lateral_only names of this level visible (SQL-spec UNNEST convenience).
    crate::assert!(!pstate.p_lateral_active);
    pstate.p_lateral_active = true;

    let mut funcexprs: Vec<Node> = Vec::new();
    let mut funcnames: Vec<String> = Vec::new();
    let coldeflists: Vec<Vec<Node>> = Vec::new();

    for fexpr in &r.functions {
        // ROWS FROM / UNNEST multi-arg expansion and per-function coldeflists are
        // grow guards; the milestone item is a single plain function call.
        let last_srf = pstate.p_last_srf.clone();
        let newfexpr = transformExpr(pstate, Some(fexpr.clone()), ParseExprKind::FromFunction)
            .unwrap_or_else(|| not_yet_reachable("transformRangeFunction: NULL function expression"));

        // nodeFunctionscan.c requires SRFs to be at the top level of the FROM item.
        if pstate.p_last_srf != last_srf && pstate.p_last_srf.as_ref() != Some(&newfexpr) {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("set-returning functions must appear at top level of FROM".to_string());
            });
            unreachable!("ereport(ERROR) diverges");
        }

        funcnames.push(FigureColname(fexpr));
        funcexprs.push(newfexpr);
    }

    pstate.p_lateral_active = false;

    if !r.coldeflist.is_empty() {
        not_yet_reachable("transformRangeFunction: top-level column definition list (ROWS FROM/UNNEST)");
    }

    // Assign collations so the RTE exposes correct collation info for its Vars.
    assign_list_collations(pstate, &mut funcexprs);

    // Milestone functions are never LATERAL (no cross-references).
    let is_lateral = r.lateral;
    if is_lateral {
        not_yet_reachable("transformRangeFunction: LATERAL function item");
    }

    crate::backend::parser::parse_relation::add_range_table_entry_for_function(
        pstate,
        &funcnames,
        funcexprs,
        &coldeflists,
        r,
        is_lateral,
        true,
    )
}

/// PG `scanNameSpaceForCTE`: find an unqualified RangeVar's name among the
/// referenceable CTEs (`p_ctenamespace`, plus parent levels), returning the CTE and
/// its `ctelevelsup`. A schema-qualified RangeVar never matches a CTE.
fn scan_namespace_for_cte(
    pstate: &ParseState,
    rv: &RangeVar,
) -> Option<(crate::nodes::parsenodes::CommonTableExpr, crate::c::Index)> {
    if rv.schemaname.is_some() {
        return None;
    }
    let refname = rv.relname.as_deref()?;
    let mut levelsup = 0;
    let mut ps: Option<&ParseState> = Some(pstate);
    while let Some(p) = ps {
        for cte in &p.p_ctenamespace {
            if cte.ctename.as_deref() == Some(refname) {
                return Some((cte.clone(), levelsup));
            }
        }
        levelsup += 1;
        ps = p.parent_parse_state.as_deref();
    }
    None
}

/// Bump the `cterefcount` of the named CTE in the (innermost matching) namespace.
fn bump_cte_refcount(pstate: &mut ParseState, ctename: Option<&str>) {
    let Some(name) = ctename else { return };
    if let Some(cte) = pstate
        .p_ctenamespace
        .iter_mut()
        .find(|c| c.ctename.as_deref() == Some(name))
    {
        cte.cterefcount += 1;
    }
}

/// PG `transformTableEntry`: open the relation (AccessShareLock) and build its
/// RTE. The open is the async lock/relcache step; the RTE build is sync.
async fn transform_table_entry(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    rv: &RangeVar,
) -> ParseNamespaceItem {
    let rel = open_table_for_parse(shared, rv).await;
    // SAFETY: live open relation with a built descriptor.
    let nsitem = add_range_table_entry_for_relation(
        pstate,
        &rel,
        LockMode::AccessShareLock as i32,
        rv.alias.as_deref(),
        rv.inh,
        true,
    );
    // table_close(rel, NoLock) keeps the lock to end of xact; the relcache refcount
    // drop is RAII / deferred (M2 holds the entry for the rest of planning).
    nsitem
}

/// PG `setTargetTable`: open the INSERT/UPDATE/DELETE target relation, add it to
/// the rangetable (but NOT the joinlist or namespace), and record it as the
/// pstate's target. Returns the target's RT index. M2 supports the plain INSERT
/// target (RowExclusiveLock, no inheritance expansion).
pub async fn set_target_table(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    relation: &RangeVar,
    inh: bool,
    also_source: bool,
    required_perms: AclMode,
) -> i32 {
    let rel = open_table_for_parse(shared, relation).await;
    let nsitem = add_range_table_entry_for_relation(
        pstate,
        &rel,
        LockMode::RowExclusiveLock as i32,
        None,
        inh,
        false,
    );
    let rtindex = nsitem.rtindex;

    // Stamp the required INSERT/UPDATE perms on the target's perminfo.
    let perminfo_index = nsitem.rte.perminfoindex;
    pstate.p_rteperminfos[(perminfo_index - 1) as usize].requiredPerms = required_perms;

    pstate.p_target_relation = Some(rel);
    pstate.p_target_nsitem = Some(Box::new(nsitem.clone()));

    // PG: UPDATE/DELETE/MERGE also make the target a source relation -- add the
    // RTE's RangeTblRef to the join list and expose it in the namespace so SET / qual
    // / RETURNING expressions can reference its columns. INSERT (also_source=false)
    // keeps the target out of the namespace (its tlist is built from VALUES).
    if also_source {
        add_ns_item_to_query(pstate, nsitem, true, true, true);
    }
    rtindex
}

/// Resolve a RangeVar to an open relcache `Relation` for parse analysis. M2 does
/// not take the heavyweight lock through `relation_open` (the sync
/// `RangeVarGetRelid` stub is not wired); it resolves the OID via the async
/// catalog scan and ensures the relcache entry is built. The AccessShareLock the
/// faithful path would take is approximated by the relcache build (the M2 tests
/// run single-statement, no concurrent DDL).
async fn open_table_for_parse(shared: &Arc<SharedState>, rv: &RangeVar) -> Arc<crate::utils::rel::RelationData> {
    use crate::backend::catalog::namespace::range_var_get_relid;
    use crate::backend::utils::cache::relcache::{relation_build_desc, relation_id_get_relation};

    let oid = range_var_get_relid(shared, rv.schemaname.as_deref(), rv.relname.as_deref().unwrap_or("")).await;
    let Some(oid) = oid else {
        relation_does_not_exist(rv.relname.as_deref().unwrap_or(""));
    };

    if let Some(rel) = relation_id_get_relation(oid) {
        return rel;
    }
    relation_build_desc(shared, oid)
        .await
        .unwrap_or_else(|| relation_does_not_exist(rv.relname.as_deref().unwrap_or("")))
}

#[cold]
fn relation_does_not_exist(relname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation \"{relname}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `transformWhereClause`: transform a WHERE/HAVING-style qualifier expression
/// and coerce it to boolean. A NULL clause yields no qual. `construct_name` names
/// the clause for the type-mismatch error (e.g. "WHERE").
#[must_use]
pub fn transform_where_clause(
    pstate: &mut ParseState,
    clause: Option<Node>,
    expr_kind: crate::parser::parse_node::ParseExprKind,
    construct_name: &str,
) -> Option<Node> {
    let clause = clause?;
    let qual = crate::parser::parse_expr::transformExpr(pstate, Some(clause), expr_kind)?;
    Some(crate::parser::parse_coerce::coerce_to_boolean(pstate, qual, construct_name))
}

// ===========================================================================
//  ORDER BY / GROUP BY / DISTINCT / LIMIT  (M5, step 26)
//
//  These build the SortGroupClause lists on the Query. Each GROUP BY / ORDER BY /
//  DISTINCT item resolves to a TargetEntry (reusing an existing one when its
//  expression matches, else appending a resjunk entry), gets a sort/group ref, and
//  yields a SortGroupClause carrying the equality (eqop) and ordering (sortop)
//  operators resolved over the column type. Grouping sets, DISTINCT ON, ORDER BY
//  USING, and the SQL99 column-alias rules are clean grow guards (rules.md s4).
// ===========================================================================

use crate::nodes::nodeFuncs::exprType;
use crate::nodes::parsenodes::{SortByDir, SortByNulls, SortBy, SortGroupClause};

/// PG `transformGroupClause`: turn the raw GROUP BY list into a `SortGroupClause`
/// list, resolving each item against (and possibly extending) the targetlist. M5
/// reaches plain expression group items; grouping sets / ROLLUP / CUBE grow.
#[allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: transformGroupClause takes sort_clause by value (List *); read-only here, threaded for the GROUP BY/ORDER BY ref-sharing"
)]
pub fn transform_group_clause(
    pstate: &mut ParseState,
    grouplist: Vec<Node>,
    grouping_sets: &mut Vec<Node>,
    targetlist: &mut Vec<Node>,
    sort_clause: Vec<Node>,
    expr_kind: crate::parser::parse_node::ParseExprKind,
    use_sql99: bool,
) -> Vec<Node> {
    let _ = (grouping_sets, use_sql99);
    let mut result: Vec<Node> = Vec::new();
    for gexpr in grouplist {
        if matches!(gexpr, Node::GroupingSet(_)) {
            not_yet_reachable("transformGroupClause: grouping sets");
        }
        let tle_resno = find_target_list_entry(pstate, gexpr, targetlist, expr_kind);
        // addTargetToGroupList: build the SortGroupClause (eqop + optional sortop).
        result = add_target_to_group_list(tle_resno, result, targetlist, &sort_clause);
    }
    result
}

/// PG `transformSortClause`: turn the raw ORDER BY list into a `SortGroupClause`
/// list. Each `SortBy` resolves to a targetlist entry, then `addTargetToSortList`
/// builds the clause (sortop + nulls_first from ASC/DESC/NULLS).
pub fn transform_sort_clause(
    pstate: &mut ParseState,
    orderlist: Vec<Node>,
    targetlist: &mut Vec<Node>,
    expr_kind: crate::parser::parse_node::ParseExprKind,
    use_sql99: bool,
) -> Vec<Node> {
    let _ = use_sql99;
    let mut sortlist: Vec<Node> = Vec::new();
    for item in orderlist {
        let Node::SortBy(sortby) = item else {
            not_yet_reachable("transformSortClause: ORDER BY item is not a SortBy");
        };
        let node = sortby
            .node
            .clone()
            .unwrap_or_else(|| not_yet_reachable("transformSortClause: empty SortBy"));
        let resno = find_target_list_entry(pstate, node, targetlist, expr_kind);
        sortlist = add_target_to_sort_list(resno, sortlist, targetlist, &sortby);
    }
    sortlist
}

/// PG `transformWindowDefinitions`: turn the parse state's collected `WindowDef`s
/// (the explicit `WINDOW name AS (...)` list plus the inline `OVER (...)`
/// definitions, both gathered into `pstate.p_windowdefs` by `transformWindowFuncCall`)
/// into the `Query.windowClause` list of `WindowClause` nodes, each carrying its
/// transformed `partitionClause` / `orderClause` (as `SortGroupClause` lists) and
/// the `frameOptions` / start/end offsets. The `winref` matches the value
/// `transformWindowFuncCall` stamped on each `WindowFunc` (1-based position).
///
/// M12 (step 42): window inheritance (`OVER (base ...)`) and the RANGE/GROUPS
/// in_range support-function resolution are clean grow guards; the milestone frames
/// are ROWS and the default RANGE UNBOUNDED PRECEDING .. CURRENT ROW.
pub fn transformWindowDefinitions(
    pstate: &mut ParseState,
    windowdefs: &[Node],
    targetlist: &mut Vec<Node>,
) -> Vec<Node> {
    use crate::nodes::parsenodes::{FrameOptions, WindowClause, WindowDef};

    let mut result: Vec<Node> = Vec::new();
    for (i, wd) in windowdefs.iter().enumerate() {
        let Node::WindowDef(windef): &Node = wd else {
            not_yet_reachable("transformWindowDefinitions: non-WindowDef in window list");
        };
        let windef: &WindowDef = windef;

        // PARTITION BY -> SortGroupClause list (group semantics: eqop + sortop).
        let partition_clause = transform_group_clause(
            pstate,
            windef.partitionClause.clone(),
            &mut Vec::new(),
            targetlist,
            Vec::new(),
            crate::parser::parse_node::ParseExprKind::WindowPartition,
            true,
        );

        // ORDER BY -> SortGroupClause list (full ordering: sortop + nulls_first).
        let order_clause = transform_sort_clause(
            pstate,
            windef.orderClause.clone(),
            targetlist,
            crate::parser::parse_node::ParseExprKind::WindowOrder,
            true,
        );

        let frame_options = windef.frameOptions;
        // RANGE/GROUPS modes with an ORDER BY column need an in_range function and a
        // single ordering column; the milestone reaches ROWS frames and the default
        // RANGE UNBOUNDED PRECEDING .. CURRENT ROW (which needs no in_range probe).
        let start_offset = transformFrameOffset(pstate, frame_options, true, windef.startOffset.clone());
        let end_offset = transformFrameOffset(pstate, frame_options, false, windef.endOffset.clone());

        let exotic =
            FrameOptions::from_bits_truncate(frame_options).intersects(FrameOptions::EXCLUSION);
        if exotic {
            not_yet_reachable("transformWindowDefinitions: frame EXCLUDE clause");
        }

        result.push(Node::WindowClause(Box::new(WindowClause {
            name: windef.name.clone(),
            refname: windef.refname.clone(),
            partitionClause: partition_clause,
            orderClause: order_clause,
            frameOptions: frame_options,
            startOffset: start_offset,
            endOffset: end_offset,
            startInRangeFunc: crate::postgres_ext::InvalidOid,
            endInRangeFunc: crate::postgres_ext::InvalidOid,
            inRangeColl: crate::postgres_ext::InvalidOid,
            inRangeAsc: true,
            inRangeNullsFirst: false,
            winref: (i + 1) as crate::c::Index,
            copiedOrder: false,
        })));
    }
    result
}

/// PG `transformFrameOffset`: transform a ROWS/RANGE/GROUPS frame OFFSET expression
/// (`n PRECEDING` / `n FOLLOWING`) and coerce it to the offset's expected type. For
/// ROWS and GROUPS the offset is an int8 row/group count. RANGE-mode offsets (which
/// require the column-type in_range support function) are staged.
pub fn transformFrameOffset(
    pstate: &mut ParseState,
    frame_options: i32,
    is_start: bool,
    clause: Option<Node>,
) -> Option<Node> {
    use crate::catalog::genbki::{INT2OID, INT4OID, INT8OID};
    use crate::nodes::parsenodes::FrameOptions;

    let opts = FrameOptions::from_bits_truncate(frame_options);
    let offset_bit = if is_start {
        FrameOptions::START_OFFSET
    } else {
        FrameOptions::END_OFFSET
    };
    if !opts.intersects(offset_bit) {
        return None; // not an OFFSET bound -> no offset expression
    }

    if opts.contains(FrameOptions::RANGE) {
        not_yet_reachable("transformFrameOffset: RANGE-mode OFFSET (needs in_range)");
    }

    let node = clause.unwrap_or_else(|| {
        not_yet_reachable("transformFrameOffset: OFFSET bound without an offset expression")
    });
    let expr = crate::parser::parse_expr::transformExpr(
        pstate,
        Some(node),
        crate::parser::parse_node::ParseExprKind::WindowFrameRange,
    )
    .unwrap_or_else(|| not_yet_reachable("transformFrameOffset: NULL offset expression"));

    // ROWS / GROUPS: the offset is a bigint row/group count. An integer literal
    // widens exactly to int8 (the same fold transform_limit_clause uses).
    let expr_type = exprType(&expr);
    if expr_type == INT8OID {
        return Some(expr);
    }
    if (expr_type == INT4OID || expr_type == INT2OID)
        && let Node::Const(c) = &expr
        && !c.constisnull
    {
        let v = i64::from(crate::postgres::DatumGetInt32(c.constvalue));
        return Some(Node::Const(Box::new(crate::nodes::primnodes::Const {
            consttype: INT8OID,
            consttypmod: -1,
            constcollid: crate::postgres_ext::InvalidOid,
            constlen: 8,
            constvalue: crate::postgres::Int64GetDatum(v),
            constisnull: false,
            constbyval: true,
            location: c.location,
        })));
    }
    not_yet_reachable("transformFrameOffset: non-literal ROWS/GROUPS offset");
}

/// PG `transformDistinctClause` (plain DISTINCT): build a `SortGroupClause` per
/// non-junk targetlist column (DISTINCT over the whole select list), reusing the
/// ORDER BY clauses' refs where they coincide. DISTINCT ON grows separately.
#[allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: transformDistinctClause takes sort_clause by value (List *); read here to align DISTINCT with ORDER BY"
)]
#[allow(
    clippy::ptr_arg,
    reason = "header signature (List **targetlist in/out); entries are mutated in place (ressortgroupref), so &mut is required"
)]
pub fn transform_distinct_clause(
    pstate: &mut ParseState,
    targetlist: &mut Vec<Node>,
    sort_clause: Vec<Node>,
    is_agg: bool,
) -> Vec<Node> {
    let _ = (pstate, is_agg);
    let mut result: Vec<Node> = Vec::new();

    // PG first emits the SortGroupClauses already in the ORDER BY (so DISTINCT and
    // ORDER BY agree on the leading columns), then adds one for every other
    // non-junk tlist column. M5's milestone DISTINCT has no ORDER BY interplay
    // beyond a prefix; reuse the sort clauses verbatim, then extend.
    result.extend_from_slice(&sort_clause);

    // Snapshot the current resnos to iterate without holding a borrow on targetlist.
    let resnos: Vec<crate::access::attnum::AttrNumber> = targetlist
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(te) if !te.resjunk => Some(te.resno),
            _ => None,
        })
        .collect();
    for resno in resnos {
        if sort_list_has_resno(&result, targetlist, resno) {
            continue;
        }
        result = add_target_to_group_list(resno, result, targetlist, &[]);
    }
    result
}

/// PG `transformLimitClause`: transform a LIMIT/OFFSET expression and coerce it to
/// int8. A NULL clause (LIMIT ALL / no OFFSET) yields None.
pub fn transform_limit_clause(
    pstate: &mut ParseState,
    clause: Option<Node>,
    expr_kind: crate::parser::parse_node::ParseExprKind,
    construct_name: &str,
    limit_option: crate::nodes::nodes::LimitOption,
) -> Option<Node> {
    use crate::catalog::genbki::INT8OID;
    use crate::nodes::primnodes::CoercionForm;
    use crate::parser::parse_coerce::coerce_to_target_type;

    use crate::catalog::genbki::{INT2OID, INT4OID};

    let _ = limit_option;
    let clause = clause?;
    let qual = crate::parser::parse_expr::transformExpr(pstate, Some(clause), expr_kind)?;
    let qual_type = exprType(&qual);
    if qual_type == INT8OID {
        return Some(qual);
    }

    // An integer-literal LIMIT/OFFSET (the common form) constant-folds to an int8
    // Const directly: the int2/int4 -> int8 widening is exact and avoids depending
    // on the int8 cast-catalog rows (not in the M4 cast seed). PG reaches the same
    // result via coerce_to_specific_type + const-folding.
    if (qual_type == INT4OID || qual_type == INT2OID)
        && let Node::Const(c) = &qual
        && !c.constisnull
    {
        let v = i64::from(crate::postgres::DatumGetInt32(c.constvalue));
        return Some(Node::Const(Box::new(crate::nodes::primnodes::Const {
            consttype: INT8OID,
            consttypmod: -1,
            constcollid: crate::postgres_ext::InvalidOid,
            constlen: 8,
            constvalue: crate::postgres::Int64GetDatum(v),
            constisnull: false,
            constbyval: true,
            location: c.location,
        })));
    }

    // Otherwise attempt the catalog cast (int8-typed columns / expressions).
    let coerced = coerce_to_target_type(
        pstate,
        Some(qual),
        qual_type,
        INT8OID,
        -1,
        crate::nodes::primnodes::CoercionContext::ASSIGNMENT,
        CoercionForm::IMPLICIT_CAST,
        -1,
    )
    .unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("argument of {construct_name} must be type bigint"));
        });
        unreachable!("ereport(ERROR) diverges");
    });
    Some(coerced)
}

/// PG `findTargetlistEntry` (SQL92 subset): resolve a GROUP BY / ORDER BY raw
/// expression to a targetlist entry's resno. An integer literal `N` references the
/// Nth output column (SQL92 ordinal); otherwise the expression is transformed and
/// matched against the existing tlist (reusing an equal entry) or appended as a new
/// resjunk entry. The SQL99 name-only rule and ambiguity checks grow later.
fn find_target_list_entry(
    pstate: &mut ParseState,
    node: Node,
    targetlist: &mut Vec<Node>,
    expr_kind: crate::parser::parse_node::ParseExprKind,
) -> crate::access::attnum::AttrNumber {
    // SQL92 ordinal: a bare positive integer A_Const selects the Nth tlist column.
    if let Node::A_Const(c) = &node
        && let crate::nodes::parsenodes::ValUnion::Integer(iv) = &c.val
    {
        let target = iv.ival;
        let non_junk: Vec<crate::access::attnum::AttrNumber> = targetlist
            .iter()
            .filter_map(|n| match n {
                Node::TargetEntry(te) if !te.resjunk => Some(te.resno),
                _ => None,
            })
            .collect();
        if target < 1 || (target as usize) > non_junk.len() {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_INVALID_COLUMN_REFERENCE)
                    .errmsg(format!("ORDER/GROUP BY position {target} is not in select list"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
        return non_junk[(target - 1) as usize];
    }

    // Transform the expression, then match it against the existing tlist.
    let expr = crate::parser::parse_expr::transformExpr(pstate, Some(node), expr_kind)
        .unwrap_or_else(|| not_yet_reachable("findTargetlistEntry: NULL expression"));
    for n in targetlist.iter() {
        if let Node::TargetEntry(te) = n
            && !te.resjunk
            && te.expr.as_ref() == Some(&expr)
        {
            return te.resno;
        }
    }

    // Not found: append a new resjunk target entry holding the expression.
    let resno = pstate.p_next_resno as crate::access::attnum::AttrNumber;
    pstate.p_next_resno += 1;
    let tle = crate::nodes::makefuncs::makeTargetEntry(Some(expr), resno, None, true);
    targetlist.push(Node::TargetEntry(Box::new(tle)));
    resno
}

/// PG `addTargetToGroupList`: assign a sort/group ref to the targetlist entry at
/// `resno` and append a `SortGroupClause` carrying its equality + (best-effort)
/// ordering operators. The collation handling and the hash/sort op-family lookups
/// are reduced to the int/text exact-name resolution for M5.
fn add_target_to_group_list(
    resno: crate::access::attnum::AttrNumber,
    mut grouplist: Vec<Node>,
    targetlist: &mut [Node],
    sort_clause: &[Node],
) -> Vec<Node> {
    // Reuse an existing ORDER BY clause's ref if it already covers this column (so
    // GROUP BY and ORDER BY agree); else assign a fresh ref.
    let restype = tlist_entry_type(targetlist, resno);
    let sortgroupref = assign_sort_group_ref(targetlist, resno);

    // If a sort clause already references this column, reuse its operators.
    if let Some(existing) = sort_clause.iter().find_map(|n| match n {
        Node::SortGroupClause(sgc) if sgc.tleSortGroupRef == sortgroupref => Some(sgc.clone()),
        _ => None,
    }) {
        grouplist.push(Node::SortGroupClause(Box::new(*existing)));
        return grouplist;
    }

    let (eqop, sortop) = get_sort_group_operators(restype);
    grouplist.push(Node::SortGroupClause(Box::new(SortGroupClause {
        tleSortGroupRef: sortgroupref,
        eqop,
        sortop,
        reverse_sort: false,
        nulls_first: false,
        hashable: false,
    })));
    grouplist
}

/// PG `addTargetToSortList`: assign a sort/group ref to the entry at `resno` and
/// append a `SortGroupClause` carrying the ordering operator (from ASC/DESC) and
/// the nulls-first flag (from NULLS FIRST/LAST, defaulting to DESC's reverse rule).
fn add_target_to_sort_list(
    resno: crate::access::attnum::AttrNumber,
    mut sortlist: Vec<Node>,
    targetlist: &mut [Node],
    sortby: &SortBy,
) -> Vec<Node> {
    let restype = tlist_entry_type(targetlist, resno);
    let reverse = matches!(sortby.sortby_dir, SortByDir::DESC);
    let (eqop, lt_op, gt_op) = get_ordering_operators(restype);
    let sortop = if reverse { gt_op } else { lt_op };

    let nulls_first = match sortby.sortby_nulls {
        SortByNulls::FIRST => true,
        SortByNulls::LAST => false,
        // SQL default: NULLS LAST for ASC, NULLS FIRST for DESC.
        SortByNulls::DEFAULT => reverse,
    };

    let sortgroupref = assign_sort_group_ref(targetlist, resno);
    sortlist.push(Node::SortGroupClause(Box::new(SortGroupClause {
        tleSortGroupRef: sortgroupref,
        eqop,
        sortop,
        reverse_sort: reverse,
        nulls_first,
        hashable: false,
    })));
    sortlist
}

/// PG `assignSortGroupRef`: if the targetlist entry at `resno` already has a
/// sort/group ref, return it; otherwise assign the next unused ref (max + 1) and
/// stamp it on the entry.
fn assign_sort_group_ref(
    targetlist: &mut [Node],
    resno: crate::access::attnum::AttrNumber,
) -> crate::c::Index {
    let mut maxref: crate::c::Index = 0;
    for n in targetlist.iter() {
        if let Node::TargetEntry(te) = n {
            maxref = maxref.max(te.ressortgroupref);
        }
    }
    for n in targetlist.iter_mut() {
        if let Node::TargetEntry(te) = n
            && te.resno == resno
        {
            if te.ressortgroupref != 0 {
                return te.ressortgroupref;
            }
            te.ressortgroupref = maxref + 1;
            return te.ressortgroupref;
        }
    }
    unreachable!("assignSortGroupRef: resno {resno} not in targetlist")
}

/// The type OID of the targetlist entry at `resno`.
fn tlist_entry_type(targetlist: &[Node], resno: crate::access::attnum::AttrNumber) -> crate::postgres_ext::Oid {
    for n in targetlist {
        if let Node::TargetEntry(te) = n
            && te.resno == resno
        {
            return te.expr.as_ref().map_or(crate::postgres_ext::InvalidOid, exprType);
        }
    }
    unreachable!("tlist_entry_type: resno {resno} not in targetlist")
}

/// Whether a SortGroupClause list already references the column at `resno` (matched
/// via the entry's sortgroupref).
fn sort_list_has_resno(
    sortlist: &[Node],
    targetlist: &[Node],
    resno: crate::access::attnum::AttrNumber,
) -> bool {
    let sgr = targetlist.iter().find_map(|n| match n {
        Node::TargetEntry(te) if te.resno == resno => Some(te.ressortgroupref),
        _ => None,
    });
    let Some(sgr) = sgr else { return false };
    if sgr == 0 {
        return false;
    }
    sortlist.iter().any(|n| matches!(n, Node::SortGroupClause(sgc) if sgc.tleSortGroupRef == sgr))
}

/// PG `get_sort_group_operators` (M5 subset, grouping use): the (eqop, sortop) for a
/// type, resolved by exact operator name in pg_catalog. Returns the default `<`
/// ordering operator (GROUP BY does not need a specific direction).
fn get_sort_group_operators(typ: crate::postgres_ext::Oid) -> (crate::postgres_ext::Oid, crate::postgres_ext::Oid) {
    use crate::backend::parser::parse_oper::opername_get_oprid;
    let eqop = opername_get_oprid("=", typ, typ);
    let sortop = opername_get_oprid("<", typ, typ);
    (eqop, sortop)
}

/// PG `get_sort_group_operators` (M5 subset, ordering use): the (eqop, `<`, `>`)
/// operators for a type, resolved by exact operator name in pg_catalog.
fn get_ordering_operators(
    typ: crate::postgres_ext::Oid,
) -> (crate::postgres_ext::Oid, crate::postgres_ext::Oid, crate::postgres_ext::Oid) {
    use crate::backend::parser::parse_oper::opername_get_oprid;
    let eqop = opername_get_oprid("=", typ, typ);
    let lt_op = opername_get_oprid("<", typ, typ);
    let gt_op = opername_get_oprid(">", typ, typ);
    (eqop, lt_op, gt_op)
}

//! Transform the raw parse tree into a query tree. Translated from
//! backend/parser/analyze.c.
//!
//! This is the parse-analysis hub: `parse_analyze_*` set up a `ParseState` and
//! call `transformTopLevelStmt` -> `transformStmt` -> the per-statement transform
//! (`transformSelectStmt`, ...), turning a `RawStmt` into a `Query`. Non-type-
//! centric free functions; bodies here as snake_case `pub fn`s, re-exported from
//! `crate::parser::analyze` under the C names.
//!
//! Disposition: `grow`. `transformStmt` is the statement-tag dispatcher and
//! `transformSelectStmt` the SELECT clause handler; both are scaffolded so each
//! later milestone fills one arm/clause (FROM, WHERE, GROUP BY, sort, limit, set
//! ops, INSERT/UPDATE/DELETE, ...) without restructuring. For M1 the live path is
//! a simple constant SELECT: target list only, empty range table, empty-FROM join
//! tree. Every not-yet-reachable statement tag / clause routes through a single
//! clearly-marked staging arm (rules.md s4); none is half-written.


use std::sync::Arc;

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{
    AclMode, InsertStmt, Query, QuerySource, RawStmt, SelectStmt, SetOperation,
};
use crate::nodes::primnodes::OverridingKind;
use crate::parser::parse_collate::assign_query_collations;
use crate::parser::parse_node::{make_parsestate, ParseExprKind, ParseState};
use crate::parser::parse_target::transformTargetList;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Panic for a statement / SELECT clause not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `makeNode(Query)`: a zero-initialized `Query` (palloc0 semantics). The
/// first enum variant is the zero discriminant, matching C's all-bytes-zero.
fn make_query() -> Box<Query> {
    Box::new(Query {
        commandType: CmdType::UNKNOWN,
        querySource: QuerySource::ORIGINAL,
        queryId: 0,
        canSetTag: false,
        utilityStmt: None,
        resultRelation: 0,
        hasAggs: false,
        hasWindowFuncs: false,
        hasTargetSRFs: false,
        hasSubLinks: false,
        hasDistinctOn: false,
        hasRecursive: false,
        hasModifyingCTE: false,
        hasForUpdate: false,
        hasRowSecurity: false,
        hasGroupRTE: false,
        isReturn: false,
        cteList: Vec::new(),
        rtable: Vec::new(),
        rteperminfos: Vec::new(),
        jointree: None,
        mergeActionList: Vec::new(),
        mergeTargetRelation: 0,
        mergeJoinCondition: None,
        targetList: Vec::new(),
        r#override: OverridingKind::NOT_SET,
        onConflict: None,
        returningOldAlias: None,
        returningNewAlias: None,
        returningList: Vec::new(),
        groupClause: Vec::new(),
        groupDistinct: false,
        groupingSets: Vec::new(),
        havingQual: None,
        windowClause: Vec::new(),
        distinctClause: Vec::new(),
        sortClause: Vec::new(),
        limitOffset: None,
        limitCount: None,
        limitOption: crate::nodes::nodes::LimitOption::COUNT,
        rowMarks: Vec::new(),
        setOperations: None,
        constraintDeps: Vec::new(),
        withCheckOptions: Vec::new(),
        stmt_location: 0,
        stmt_len: 0,
    })
}

/// PG `parse_analyze_fixedparams`: analyze a raw parse tree, producing a `Query`.
/// (`paramTypes` handling is deferred; M1 has no parameters.)
pub fn parse_analyze_fixedparams(
    parse_tree: &RawStmt,
    source_text: &str,
    _param_types: &[Oid],
    num_params: i32,
    _query_env: Option<&mut crate::utils::queryenvironment::QueryEnvironment>,
) -> Box<Query> {
    let mut pstate = make_parsestate(None);
    pstate.p_sourcetext = Some(source_text.to_string());

    if num_params > 0 {
        not_yet_reachable("parse_analyze_fixedparams: external parameter setup");
    }

    let query = transformTopLevelStmt(&mut pstate, parse_tree);

    // Query jumbling (JumbleQuery, gated by IsQueryIdEnabled), the
    // post_parse_analyze_hook, and pgstat_report_query_id are observability hooks
    // reaching not-yet-translated subsystems; deferred (they do not affect the
    // produced Query). free_parsestate is RAII (Drop of pstate).
    crate::parser::parse_node::free_parsestate(&mut pstate);

    query
}

/// PG `transformTopLevelStmt`: top-level entry; SELECT INTO is allowed here.
pub fn transformTopLevelStmt(pstate: &mut ParseState, parse_tree: &RawStmt) -> Box<Query> {
    // PG calls transformOptionalSelectInto to rewrite a top-level SELECT ... INTO
    // into CREATE TABLE AS. M1 has no INTO clause; the leftmost-SelectStmt drill
    // and CTAS rewrite grow with SELECT INTO support.
    let stmt = parse_tree.stmt.as_ref().unwrap_or_else(|| {
        not_yet_reachable("transformTopLevelStmt: empty RawStmt");
    });
    let mut result = transformStmt(pstate, stmt);
    result.stmt_location = parse_tree.stmt_location;
    result.stmt_len = parse_tree.stmt_len;
    result
}

/// PG `transformStmt`: dispatch on the raw statement's node tag, producing a
/// `Query`. Grows one statement arm per milestone.
pub fn transformStmt(pstate: &mut ParseState, parse_tree: &Node) -> Box<Query> {
    let mut result = match parse_tree {
        Node::SelectStmt(n) => {
            if !n.valuesLists.is_empty() {
                not_yet_reachable("transformStmt: VALUES clause");
            } else if n.op == SetOperation::NONE {
                transformSelectStmt(pstate, n)
            } else {
                not_yet_reachable("transformStmt: set-operation SELECT");
            }
        }
        // InsertStmt / DeleteStmt / UpdateStmt / MergeStmt and the special-case
        // transforms (DECLARE CURSOR / EXPLAIN / CREATE TABLE AS / CALL) grow in
        // later milestones. Everything else - the utility statements (CreateStmt,
        // ...) - needs no transformation: return the original parse tree with a
        // CMD_UTILITY Query node plastered on top (PG transformStmt default arm).
        other => {
            let mut q = make_query();
            q.commandType = CmdType::UTILITY;
            q.utilityStmt = Some(other.clone());
            q
        }
    };

    // Mark as original query until we learn differently.
    result.querySource = QuerySource::ORIGINAL;
    result.canSetTag = true;
    result
}

/// PG `transformSelectStmt`: build a `Query` for a simple (non-set-op, non-VALUES)
/// SELECT. Grows one clause per milestone.
///
/// M1 handles the target list only. FROM, WHERE, HAVING, GROUP BY, sort,
/// DISTINCT, LIMIT, window, locking, and CTE clauses are not reachable yet; each
/// is wired to a not-yet-reachable guard so a query that uses one fails cleanly
/// rather than silently dropping the clause.
fn transformSelectStmt(pstate: &mut ParseState, stmt: &SelectStmt) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::SELECT;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformSelectStmt: WITH clause");
    }
    if stmt.intoClause.is_some() {
        not_yet_reachable("transformSelectStmt: SELECT ... INTO");
    }
    if !stmt.fromClause.is_empty() {
        not_yet_reachable("transformSelectStmt: FROM clause");
    }

    // Transform the target list (the only clause live for M1). The raw target
    // list is cloned out of the borrowed stmt to hand owned nodes to the
    // transform, matching PG passing the list by pointer.
    qry.targetList =
        transformTargetList(pstate, stmt.targetList.clone(), ParseExprKind::SelectTarget);

    // markTargetListOrigins is a no-op for table-less targets (no Vars); it grows
    // with the range-table machinery.

    if stmt.whereClause.is_some() {
        not_yet_reachable("transformSelectStmt: WHERE clause");
    }
    if stmt.havingClause.is_some() {
        not_yet_reachable("transformSelectStmt: HAVING clause");
    }
    if !stmt.sortClause.is_empty() {
        not_yet_reachable("transformSelectStmt: ORDER BY clause");
    }
    if !stmt.groupClause.is_empty() {
        not_yet_reachable("transformSelectStmt: GROUP BY clause");
    }
    if !stmt.distinctClause.is_empty() {
        not_yet_reachable("transformSelectStmt: DISTINCT clause");
    }
    if stmt.limitOffset.is_some() || stmt.limitCount.is_some() {
        not_yet_reachable("transformSelectStmt: LIMIT/OFFSET clause");
    }
    if !stmt.windowClause.is_empty() {
        not_yet_reachable("transformSelectStmt: WINDOW clause");
    }
    if !stmt.lockingClause.is_empty() {
        not_yet_reachable("transformSelectStmt: locking clause");
    }

    // Resolve any still-unresolved output columns as type text. For M1 every
    // target is already a resolved type (int4 const), so this is a no-op;
    // resolveTargetListUnknowns grows with UNKNOWN-typed string literals.

    qry.rtable = std::mem::take(&mut pstate.p_rtable)
        .into_iter()
        .map(|rte| Node::RangeTblEntry(Box::new(rte)))
        .collect();
    qry.rteperminfos = std::mem::take(&mut pstate.p_rteperminfos)
        .into_iter()
        .map(|pi| Node::RTEPermissionInfo(Box::new(pi)))
        .collect();
    // jointree = makeFromExpr(p_joinlist, qual). For M1 the join list and qual are
    // both empty (table-less SELECT, no WHERE).
    let joinlist = std::mem::take(&mut pstate.p_joinlist);
    qry.jointree =
        Some(Node::FromExpr(Box::new(crate::nodes::makefuncs::makeFromExpr(joinlist, None))));

    qry.hasSubLinks = pstate.p_has_sub_links;
    qry.hasWindowFuncs = pstate.p_has_window_funcs;
    qry.hasTargetSRFs = pstate.p_has_target_srfs;
    qry.hasAggs = pstate.p_has_aggs;

    assign_query_collations(pstate, &mut qry);

    // parseCheckAggregates only runs when there are aggregates / GROUP BY / HAVING;
    // none for M1.

    qry
}

// ===========================================================================
//  Async parse-analysis path (statements that open relations)
//
//  PG opens relations during parse analysis (parserOpenTable -> table_openrv),
//  which is a lock-wait leaf and therefore ASYNC in this port (rules.md s5). The
//  sync `parse_analyze_fixedparams` above keeps handling the table-less
//  `SELECT const` path that exec_simple_query drives synchronously; statements
//  that touch a relation (INSERT, SELECT ... FROM) go through this async entry,
//  exercised by the analyze/plan tests over initdb'd catalogs. The two paths
//  share the helpers that don't open relations (target list, collations).
// ===========================================================================

/// Async sibling of `parse_analyze_fixedparams`: analyze a raw parse tree that may
/// reference relations.
pub async fn parse_analyze_fixedparams_async(
    shared: &Arc<SharedState>,
    parse_tree: &RawStmt,
    source_text: &str,
    _param_types: &[Oid],
    num_params: i32,
) -> Box<Query> {
    let mut pstate = make_parsestate(None);
    pstate.p_sourcetext = Some(source_text.to_string());

    if num_params > 0 {
        not_yet_reachable("parse_analyze_fixedparams_async: external parameter setup");
    }

    let stmt = parse_tree.stmt.as_ref().unwrap_or_else(|| {
        not_yet_reachable("transformTopLevelStmt: empty RawStmt");
    });
    let mut query = transform_stmt_async(shared, &mut pstate, stmt).await;
    query.stmt_location = parse_tree.stmt_location;
    query.stmt_len = parse_tree.stmt_len;

    crate::parser::parse_node::free_parsestate(&mut pstate);
    query
}

/// PG `transformStmt` (async arms): dispatch the statement tag, opening relations
/// where needed. SELECT routes through `transform_select_stmt_async` (which handles
/// FROM); INSERT through `transform_insert_stmt`. A table-less constant SELECT
/// still works (its FROM clause is empty).
async fn transform_stmt_async(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    parse_tree: &Node,
) -> Box<Query> {
    let mut result = match parse_tree {
        Node::SelectStmt(n) => {
            if !n.valuesLists.is_empty() {
                not_yet_reachable("transformStmt: bare VALUES statement");
            } else if n.op == SetOperation::NONE {
                transform_select_stmt_async(shared, pstate, n).await
            } else {
                not_yet_reachable("transformStmt: set-operation SELECT");
            }
        }
        Node::InsertStmt(n) => transform_insert_stmt(shared, pstate, n).await,
        other => {
            let mut q = make_query();
            q.commandType = CmdType::UTILITY;
            q.utilityStmt = Some(other.clone());
            q
        }
    };
    result.querySource = QuerySource::ORIGINAL;
    result.canSetTag = true;
    result
}

/// PG `transformSelectStmt` with FROM-clause handling. Processes the FROM clause
/// (building the rangetable) before the target list, so `*` and column references
/// resolve against the namespace. The remaining clauses (WHERE/GROUP/sort/...)
/// stay grow-guarded as in the sync path.
async fn transform_select_stmt_async(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &SelectStmt,
) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::SELECT;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformSelectStmt: WITH clause");
    }
    if stmt.intoClause.is_some() {
        not_yet_reachable("transformSelectStmt: SELECT ... INTO");
    }

    // Process the FROM clause (builds the rangetable + namespace).
    crate::backend::parser::parse_clause::transform_from_clause(
        shared,
        pstate,
        stmt.fromClause.clone(),
    )
    .await;

    // Transform the target list (now that the namespace is populated, `*` and
    // column refs resolve).
    qry.targetList =
        transformTargetList(pstate, stmt.targetList.clone(), ParseExprKind::SelectTarget);

    reject_unsupported_select_clauses(stmt);

    finish_query(pstate, &mut qry);
    qry
}

/// PG `transformInsertStmt` (M2 subset): `INSERT INTO t [(cols)] VALUES (row)` and
/// the general `INSERT ... SELECT`. The single-row VALUES path computes the row
/// directly as the query targetlist (PG: "works just like a SELECT without FROM");
/// multi-row VALUES, DEFAULT VALUES, ON CONFLICT, and RETURNING grow at their
/// milestones.
async fn transform_insert_stmt(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &InsertStmt,
) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::INSERT;
    pstate.p_is_insert = true;
    qry.r#override = stmt.r#override;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformInsertStmt: WITH clause");
    }
    if stmt.onConflictClause.is_some() {
        not_yet_reachable("transformInsertStmt: ON CONFLICT clause");
    }
    if stmt.returningClause.is_some() {
        not_yet_reachable("transformInsertStmt: RETURNING clause");
    }

    let select_stmt = stmt.selectStmt.as_ref();
    let is_general_select = is_general_select(select_stmt);

    // Open the target table (RowExclusiveLock) and set it as the result relation.
    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("transformInsertStmt: missing target relation"));
    qry.resultRelation = crate::backend::parser::parse_clause::set_target_table(
        shared,
        pstate,
        relation,
        false,
        false,
        AclMode::INSERT,
    )
    .await;

    // Validate stmt->cols, or build the default ordered column list.
    let (icolumns, attrnos) = check_insert_targets(pstate, &stmt.cols);

    // Determine the INSERT variant and compute the row expression list.
    let expr_list: Vec<Node> = if select_stmt.is_none() {
        not_yet_reachable("transformInsertStmt: INSERT ... DEFAULT VALUES");
    } else if is_general_select {
        not_yet_reachable("transformInsertStmt: INSERT ... SELECT (general select source)");
    } else {
        // VALUES source.
        let Some(Node::SelectStmt(sel)) = select_stmt else {
            not_yet_reachable("transformInsertStmt: non-SelectStmt VALUES source");
        };
        if sel.valuesLists.len() != 1 {
            not_yet_reachable("transformInsertStmt: multi-row VALUES (VALUES RTE)");
        }
        let Node::RowExpr(row) = &sel.valuesLists[0] else {
            not_yet_reachable("transformInsertStmt: VALUES row is not a RowExpr carrier");
        };
        transform_expression_list(pstate, &row.args, ParseExprKind::ValuesSingle)
    };

    // Generate the query targetlist: each expr keyed to its target attno.
    crate::assert!(expr_list.len() <= icolumns.len());
    let perminfo_index = pstate
        .p_target_nsitem
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("transformInsertStmt: no target nsitem"))
        .rte
        .perminfoindex;
    qry.targetList = expr_list
        .into_iter()
        .zip(icolumns.iter().zip(attrnos.iter()))
        .map(|(expr, (col, &attno))| {
            let name = col_name(col);
            mark_inserted_col(pstate, perminfo_index, attno);
            Node::TargetEntry(Box::new(crate::nodes::makefuncs::makeTargetEntry(
                Some(expr),
                attno,
                name,
                false,
            )))
        })
        .collect();

    finish_query(pstate, &mut qry);
    qry
}

/// PG's `isGeneralSelect` test: a VALUES source with sort/limit/locking/with is
/// treated as a general SELECT. M2 only constructs a bare VALUES SelectStmt, so a
/// non-VALUES SelectStmt source is the only general-select case.
fn is_general_select(select_stmt: Option<&Node>) -> bool {
    let Some(Node::SelectStmt(sel)) = select_stmt else {
        return false;
    };
    sel.valuesLists.is_empty()
        || !sel.sortClause.is_empty()
        || sel.limitOffset.is_some()
        || sel.limitCount.is_some()
        || !sel.lockingClause.is_empty()
        || sel.withClause.is_some()
}

/// PG `checkInsertTargets` (M2 subset): with no explicit column list, build the
/// default target list = all the relation's live columns in order. An explicit
/// column list maps each named ResTarget to its attno. Returns (icolumns, attnos).
fn check_insert_targets(
    pstate: &ParseState,
    cols: &[Node],
) -> (Vec<Node>, Vec<crate::access::attnum::AttrNumber>) {
    let rel = pstate.p_target_relation.as_ref()
        .unwrap_or_else(|| unreachable!("target relation set by set_target_table"));
    let tupdesc = rel.rd_att.as_ref()
        .unwrap_or_else(|| unreachable!("target relation has a descriptor"));

    if cols.is_empty() {
        // Default: every non-dropped column, in attno order.
        let mut icolumns = Vec::new();
        let mut attnos = Vec::new();
        for i in 0..tupdesc.natts as usize {
            let attr = tupdesc.attr(i);
            if attr.attisdropped {
                continue;
            }
            let attno = (i + 1) as crate::access::attnum::AttrNumber;
            let name = attr_name(attr);
            let rt = crate::nodes::parsenodes::ResTarget {
                name: Some(name),
                indirection: Vec::new(),
                val: None,
                location: -1,
            };
            icolumns.push(Node::ResTarget(Box::new(rt)));
            attnos.push(attno);
        }
        (icolumns, attnos)
    } else {
        not_yet_reachable("checkInsertTargets: explicit INSERT column list");
    }
}

/// PG `transformExpressionList` (EXPR_KIND_VALUES_SINGLE subset): transform each
/// expression in a VALUES row. `*` expansion / SetToDefault grow later.
fn transform_expression_list(
    pstate: &mut ParseState,
    exprs: &[Node],
    expr_kind: ParseExprKind,
) -> Vec<Node> {
    exprs
        .iter()
        .map(|e| {
            crate::parser::parse_expr::transformExpr(pstate, Some(e.clone()), expr_kind)
                .unwrap_or_else(|| not_yet_reachable("transformExpressionList: NULL expression"))
        })
        .collect()
}

/// Tail shared by the async transforms: flatten rtable/perminfos, build the
/// jointree, copy the pstate flags, and assign collations.
fn finish_query(pstate: &mut ParseState, qry: &mut Query) {
    qry.rtable = std::mem::take(&mut pstate.p_rtable)
        .into_iter()
        .map(|rte| Node::RangeTblEntry(Box::new(rte)))
        .collect();
    qry.rteperminfos = std::mem::take(&mut pstate.p_rteperminfos)
        .into_iter()
        .map(|pi| Node::RTEPermissionInfo(Box::new(pi)))
        .collect();
    let joinlist = std::mem::take(&mut pstate.p_joinlist);
    qry.jointree =
        Some(Node::FromExpr(Box::new(crate::nodes::makefuncs::makeFromExpr(joinlist, None))));

    qry.hasSubLinks = pstate.p_has_sub_links;
    qry.hasWindowFuncs = pstate.p_has_window_funcs;
    qry.hasTargetSRFs = pstate.p_has_target_srfs;
    qry.hasAggs = pstate.p_has_aggs;

    assign_query_collations(pstate, qry);
}

/// The not-yet-reachable clause guards shared by the SELECT transforms.
fn reject_unsupported_select_clauses(stmt: &SelectStmt) {
    if stmt.whereClause.is_some() {
        not_yet_reachable("transformSelectStmt: WHERE clause");
    }
    if stmt.havingClause.is_some() {
        not_yet_reachable("transformSelectStmt: HAVING clause");
    }
    if !stmt.sortClause.is_empty() {
        not_yet_reachable("transformSelectStmt: ORDER BY clause");
    }
    if !stmt.groupClause.is_empty() {
        not_yet_reachable("transformSelectStmt: GROUP BY clause");
    }
    if !stmt.distinctClause.is_empty() {
        not_yet_reachable("transformSelectStmt: DISTINCT clause");
    }
    if stmt.limitOffset.is_some() || stmt.limitCount.is_some() {
        not_yet_reachable("transformSelectStmt: LIMIT/OFFSET clause");
    }
    if !stmt.windowClause.is_empty() {
        not_yet_reachable("transformSelectStmt: WINDOW clause");
    }
    if !stmt.lockingClause.is_empty() {
        not_yet_reachable("transformSelectStmt: locking clause");
    }
}

/// Read a ResTarget's column name (for the INSERT tlist resname).
fn col_name(col: &Node) -> Option<String> {
    match col {
        Node::ResTarget(rt) => rt.name.clone(),
        _ => None,
    }
}

/// PG marks `attno - FirstLowInvalidHeapAttributeNumber` in the target perminfo's
/// `insertedCols` here (`bms_add_member`). The bitmapset machinery is a hollow stub
/// for M2 (bitmapset.rs not translated) and `insertedCols` is permission-check
/// bookkeeping not exercised by the analyze+plan path; the column accounting grows
/// when the Bitmapset body + executor permission checks land.
fn mark_inserted_col(
    _pstate: &mut ParseState,
    _perminfo_index: crate::c::Index,
    _attno: crate::access::attnum::AttrNumber,
) {
}

/// Read a `FormData_pg_attribute`'s `attname` as an owned String.
fn attr_name(attr: &crate::catalog::pg_attribute::FormData_pg_attribute) -> String {
    let bytes = crate::c::NameStr(&attr.attname);
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::nodes::Node;
    use crate::parser::parser::RawParseMode;
    use crate::postgres::DatumGetInt32;

    /// Raw-parse `s` and return its single RawStmt.
    fn raw(s: &str) -> RawStmt {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1, "expected exactly one statement");
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        *rs
    }

    fn analyze(s: &str) -> Box<Query> {
        parse_analyze_fixedparams(&raw(s), s, &[], 0, None)
    }

    /// Pull the i-th TargetEntry out of a Query.
    fn tle(q: &Query, i: usize) -> &crate::nodes::primnodes::TargetEntry {
        let Node::TargetEntry(te) = &q.targetList[i] else { panic!("not a TargetEntry") };
        te
    }

    /// Pull the Const out of a TargetEntry's expr.
    fn const_of(q: &Query, i: usize) -> &crate::nodes::primnodes::Const {
        let Node::Const(c) = tle(q, i).expr.as_ref().unwrap() else { panic!("not a Const") };
        c
    }

    #[test]
    fn select_one_builds_query() {
        let q = analyze("SELECT 1");
        assert_eq!(q.commandType, CmdType::SELECT);
        assert_eq!(q.querySource, QuerySource::ORIGINAL);
        assert!(q.canSetTag);
        assert_eq!(q.targetList.len(), 1);

        let te = tle(&q, 0);
        assert_eq!(te.resno, 1);
        assert_eq!(te.resname.as_deref(), Some("?column?"));
        assert!(!te.resjunk);

        let c = const_of(&q, 0);
        assert_eq!(c.consttype, INT4OID);
        assert_eq!(c.constlen, 4);
        assert!(c.constbyval);
        assert!(!c.constisnull);
        assert_eq!(DatumGetInt32(c.constvalue), 1);

        // rtable empty; jointree is an empty-from FromExpr.
        assert!(q.rtable.is_empty());
        let Node::FromExpr(f) = q.jointree.as_ref().unwrap() else { panic!("not FromExpr") };
        assert!(f.fromlist.is_empty());
        assert!(f.quals.is_none());
    }

    #[test]
    fn select_42() {
        let q = analyze("SELECT 42");
        assert_eq!(DatumGetInt32(const_of(&q, 0).constvalue), 42);
        assert_eq!(tle(&q, 0).resname.as_deref(), Some("?column?"));
    }

    #[test]
    fn select_two_constants_get_sequential_resnos() {
        let q = analyze("SELECT 1, 2");
        assert_eq!(q.targetList.len(), 2);
        assert_eq!(tle(&q, 0).resno, 1);
        assert_eq!(tle(&q, 1).resno, 2);
        assert_eq!(DatumGetInt32(const_of(&q, 0).constvalue), 1);
        assert_eq!(DatumGetInt32(const_of(&q, 1).constvalue), 2);
    }

    #[test]
    fn select_with_alias_uses_explicit_name() {
        let q = analyze("SELECT 1 AS x");
        assert_eq!(tle(&q, 0).resname.as_deref(), Some("x"));
    }

    #[test]
    fn make_const_on_integer_value_builds_int4_const() {
        use crate::nodes::parsenodes::{A_Const, ValUnion};
        use crate::nodes::value::makeInteger;
        let mut pstate = make_parsestate(None);
        let aconst = A_Const { val: ValUnion::Integer(makeInteger(7)), isnull: false, location: -1 };
        let c = crate::parser::parse_node::make_const(&mut pstate, &aconst);
        assert_eq!(c.consttype, INT4OID);
        assert_eq!(c.constlen, 4);
        assert!(c.constbyval);
        assert!(!c.constisnull);
        assert_eq!(DatumGetInt32(c.constvalue), 7);
    }

    #[test]
    fn transform_expr_unsupported_node_routes_cleanly() {
        // A ColumnRef is not reachable in M1; transformExpr must route it to the
        // not-yet-reachable staging arm (an unimplemented! panic), NOT a spurious
        // index/unwrap panic. The M1 constant path never reaches this arm.
        use crate::nodes::parsenodes::ColumnRef;
        let mut pstate = make_parsestate(None);
        let cref = Node::ColumnRef(Box::new(ColumnRef { fields: Vec::new(), location: -1 }));
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::parser::parse_expr::transformExpr(
                &mut pstate,
                Some(cref),
                ParseExprKind::SelectTarget,
            )
        }));
        std::panic::set_hook(prev);
        assert!(res.is_err(), "an unsupported node must route to the staging arm");
    }
}

// ===========================================================================
//  M2 relation-based parse-analysis + planning tests (over initdb'd catalogs).
//  These drive INSERT and SELECT ... FROM through the async analyze path and the
//  sync planner, against a created user table `t(a int)`.
// ===========================================================================
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod relation_tests {
    use std::sync::Arc;

    use crate::nodes::nodes::{CmdType, Node};
    use crate::parser::parser::RawParseMode;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-analyze-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
        use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
        use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(DB_OID);
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

        let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(Box::pin(f(shared))))));
        let body = Box::pin(catcache_scope(body));
        let body = Box::pin(with_insertion(body));
        let body = Box::pin(combocid_scope(body));
        let body = Box::pin(snapmgr_scope(body));
        let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(owner, body),
        )
        .await
    }

    async fn init_db(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
        use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};

        StartTransactionCommand(shared).await;
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
        crate::backend::bootstrap::bootstrap::bootstrap_catalogs(shared).await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
        refresh_active_snapshot(shared);
    }

    fn refresh_active_snapshot(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::GetCurrentCommandId;
        use crate::backend::utils::time::snapmgr::{
            GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
        };
        PopActiveSnapshot();
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
    }

    /// Create a user table `t(a int)` and return its OID.
    async fn create_table_t(shared: &Arc<SharedState>) -> Oid {
        use crate::access::tupdesc::TupleDescData;
        use crate::backend::catalog::heap::heap_create_with_catalog;
        use crate::catalog::genbki::INT4OID;
        use crate::catalog::pg_class::{RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
        use crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;

        let mut td = TupleDescData::create_template(1);
        td.init_builtin_entry(1, "a", INT4OID, -1, 0);
        let tupdesc = Arc::new(td);

        let relid = heap_create_with_catalog(
            shared,
            "t",
            PG_PUBLIC_NAMESPACE,
            crate::common::relpath::DEFAULTTABLESPACE_OID,
            Oid(0),
            Oid(0),
            Oid(10),
            Oid(2),
            tupdesc,
            RELKIND_RELATION,
            RELPERSISTENCE_PERMANENT,
            false,
        )
        .await;
        crate::backend::access::transam::xact::CommandCounterIncrement();
        refresh_active_snapshot(shared);
        relid
    }

    /// Raw-parse `s` and return its single RawStmt.
    fn raw(s: &str) -> crate::nodes::parsenodes::RawStmt {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1, "expected exactly one statement");
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        *rs
    }

    async fn analyze(shared: &Arc<SharedState>, s: &str) -> Box<crate::nodes::parsenodes::Query> {
        super::parse_analyze_fixedparams_async(shared, &raw(s), s, &[], 0).await
    }

    /// Plan an analyzed query. SELECT goes through the real query_rewrite (a
    /// pass-through for a plain base-rel RTE); INSERT skips the rewriter, whose
    /// target-list IUD rewriting (rewriteTargetListIU / I-U-D rule firing) is the
    /// M11 rewrite milestone, not part of this step's analyze+plan deliverable
    /// (transformInsertStmt already produced the final attno-keyed targetlist).
    fn plan(
        shared: &Arc<SharedState>,
        q: crate::nodes::parsenodes::Query,
    ) -> crate::nodes::plannodes::PlannedStmt {
        let _ = shared;
        let mut parse = if q.commandType == CmdType::INSERT {
            q
        } else {
            let mut rewritten = crate::backend::rewrite::rewriteHandler::query_rewrite(q);
            assert_eq!(rewritten.len(), 1);
            rewritten.remove(0)
        };
        crate::backend::optimizer::plan::planner::standard_planner(&mut parse, "", 0, None)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_values_analyzes_to_cmd_insert() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            let relid = create_table_t(&shared).await;

            let q = analyze(&shared, "INSERT INTO t VALUES (1)").await;
            assert_eq!(q.commandType, CmdType::INSERT);
            // resultRelation points at t's RTE (the target table's RT index).
            assert!(q.resultRelation > 0);
            let Node::RangeTblEntry(rte) = &q.rtable[(q.resultRelation - 1) as usize] else {
                panic!("result relation RTE missing");
            };
            assert_eq!(rte.relid, relid, "result relation is t");
            assert_eq!(rte.rtekind, crate::nodes::parsenodes::RTEKind::RELATION);

            // One target entry: a Const 1 keyed to attno 1.
            assert_eq!(q.targetList.len(), 1);
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            assert_eq!(te.resno, 1);
            let Node::Const(c) = te.expr.as_ref().unwrap() else { panic!("VALUES expr not a Const") };
            assert_eq!(crate::postgres::DatumGetInt32(c.constvalue), 1);

            // It plans to a ModifyTable(Insert) over a Result source (the VALUES
            // single row planned as a FROM-less Result).
            let stmt = plan(&shared, *q);
            assert_eq!(stmt.command_type, CmdType::INSERT);
            assert_eq!(stmt.result_relations, vec![q_result_relation(&stmt)]);
            let Node::ModifyTable(m) = &stmt.plan_tree else { panic!("plan is not a ModifyTable") };
            assert_eq!(m.operation, CmdType::INSERT);
            assert!(matches!(m.plan.lefttree.as_ref(), Some(Node::Result(_))), "source is a Result");
        }))
        .await;
    }

    /// The single result relation RT index recorded on the planned statement.
    fn q_result_relation(stmt: &crate::nodes::plannodes::PlannedStmt) -> i32 {
        stmt.result_relations[0]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn select_star_from_t_plans_to_seqscan() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            let relid = create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT * FROM t").await;
            assert_eq!(q.commandType, CmdType::SELECT);
            // The rangetable has the RTE_RELATION for t.
            assert_eq!(q.rtable.len(), 1);
            let Node::RangeTblEntry(rte) = &q.rtable[0] else { panic!("not an RTE") };
            assert_eq!(rte.relid, relid);
            assert_eq!(rte.rtekind, crate::nodes::parsenodes::RTEKind::RELATION);
            // The `*` expanded to one column (a) -> a Var.
            assert_eq!(q.targetList.len(), 1);
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::Var(v) = te.expr.as_ref().unwrap() else { panic!("`*` did not expand to a Var") };
            assert_eq!(v.varattno, 1);
            assert_eq!(te.resname.as_deref(), Some("a"));

            let stmt = plan(&shared, *q);
            // The plan is a SeqScan over t.
            let Node::SeqScan(scan) = &stmt.plan_tree else { panic!("plan is not a SeqScan") };
            assert!(scan.scan.scanrelid > 0, "scanrelid is t's RT index");
            assert_eq!(scan.scan.plan.targetlist.len(), 1);
            let Node::TargetEntry(pte) = &scan.scan.plan.targetlist[0] else { panic!() };
            let Node::Var(pv) = pte.expr.as_ref().unwrap() else { panic!("scan tlist not a Var") };
            assert_eq!(pv.varattno, 1);
            // The planned rangetable carries the RTE_RELATION for t.
            assert_eq!(stmt.rtable.len(), 1);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn select_a_from_t_resolves_var_attno_1() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a FROM t").await;
            assert_eq!(q.targetList.len(), 1);
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::Var(v) = te.expr.as_ref().unwrap() else { panic!("`a` did not resolve to a Var") };
            assert_eq!(v.varattno, 1, "column a resolves to attno 1");
            assert_eq!(v.varno, 1, "the only FROM rel is RT index 1");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn select_one_plans_with_one_rte_result() {
        // replace_empty_jointree: a FROM-less SELECT now plans with one RTE_RESULT
        // in the rangetable, and still yields a Result plan.
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let q = analyze(&shared, "SELECT 1").await;
            // The analyzed Query rangetable is still empty (RTE_RESULT is injected
            // in the planner, not parse analysis).
            assert!(q.rtable.is_empty());

            let stmt = plan(&shared, *q);
            assert_eq!(stmt.rtable.len(), 1, "FROM-less SELECT gains one RTE_RESULT");
            let Node::RangeTblEntry(rte) = &stmt.rtable[0] else { panic!("not an RTE") };
            assert_eq!(rte.rtekind, crate::nodes::parsenodes::RTEKind::RESULT);
            assert!(matches!(&stmt.plan_tree, Node::Result(_)), "still a Result plan");
        }))
        .await;
    }
}

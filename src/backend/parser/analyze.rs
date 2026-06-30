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
    AclMode, DeleteStmt, InsertStmt, MergeStmt, Query, QuerySource, RawStmt, SelectStmt,
    SetOperation, UpdateStmt,
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
        Node::UpdateStmt(n) => transform_update_stmt(shared, pstate, n).await,
        Node::DeleteStmt(n) => transform_delete_stmt(shared, pstate, n).await,
        Node::MergeStmt(n) => transform_merge_stmt(shared, pstate, n).await,
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

    // M7 (step 32): an INNER `joined_table` (`a JOIN b ON q` / `a CROSS JOIN b`) is
    // equivalent to a comma cross-join with the ON quals ANDed into WHERE. Flatten
    // the FROM list to its base RangeVars and gather the join quals, so the proven
    // comma-join + WHERE pipeline plans it (the planner's join search produces the
    // cost-chosen nestloop/hash/merge). OUTER joins and JOIN ... USING stay grow
    // guards inside `flatten_inner_joins` (USING needs a real join RTE).
    let mut from_list = Vec::new();
    let mut join_quals: Vec<Node> = Vec::new();
    for item in stmt.fromClause.clone() {
        flatten_inner_joins(item, &mut from_list, &mut join_quals);
    }
    let where_clause = combine_quals(stmt.whereClause.clone(), join_quals);

    // Process the FROM clause (builds the rangetable + namespace).
    crate::backend::parser::parse_clause::transform_from_clause(shared, pstate, from_list).await;

    // The target-list / WHERE expression transform is synchronous and resolves
    // operators/functions through the (hit-only) syscache. Over the wire each
    // backend has a cold per-task catcache, so async-warm the operator/function
    // caches the statement references before the sync transform runs.
    warm_expr_caches(shared, pstate, &stmt.targetList, where_clause.as_ref()).await;

    // M5 (step 26): warm the aggregate-resolution caches (PROCNAMEARGSNSP for the
    // aggregate names, AGGFNOID for the resolved aggregate) and the grouping/sort
    // comparison operators (`=`/`<`/`>` over the column types) so the sync GROUP BY
    // / ORDER BY / aggregate transforms resolve over the wire.
    warm_grouping_caches(shared, pstate, stmt).await;

    // Transform the target list (now that the namespace is populated, `*` and
    // column refs resolve). Aggregate calls in the target list resolve to Aggref
    // nodes here (transformAggregateCall, which sets pstate.p_has_aggs).
    qry.targetList =
        transformTargetList(pstate, stmt.targetList.clone(), ParseExprKind::SelectTarget);

    // Transform the WHERE clause (coerced to boolean) into the jointree qual. This
    // is the WHERE plus any flattened INNER-join ON/USING quals (see above).
    let qual = crate::backend::parser::parse_clause::transform_where_clause(
        pstate,
        where_clause,
        ParseExprKind::Where,
        "WHERE",
    );

    // GROUP BY -> Query.groupClause; ORDER BY -> Query.sortClause; DISTINCT ->
    // Query.distinctClause; LIMIT/OFFSET -> Query.limit{Count,Offset}. PG's clause
    // ordering: GROUP BY (which may extend the tlist) before ORDER BY/DISTINCT,
    // which reference the (now-final) tlist; LIMIT/OFFSET last. (M5, step 26.)
    transform_select_clauses(pstate, stmt, &mut qry);

    reject_unsupported_select_clauses(stmt);

    finish_query(pstate, &mut qry, qual);

    // FOR UPDATE/SHARE locking clause -> rowMarks (M8, step 34). Must run after the
    // rangetable is final (finish_query flattens it onto the Query).
    if !stmt.lockingClause.is_empty() {
        transform_locking_clause(&mut qry, &stmt.lockingClause);
    }

    // parseCheckAggregates runs only when the query is an aggregate/grouped query.
    if qry.hasAggs || !qry.groupClause.is_empty() || qry.havingQual.is_some() {
        crate::parser::parse_agg::parseCheckAggregates(pstate, &mut qry);
    }

    qry
}

/// Flatten an INNER `joined_table` FROM item into its base RangeVars + the
/// equivalent ON/USING quals (which the caller ANDs into WHERE). A plain RangeVar
/// passes through. M7 handles INNER/CROSS joins; OUTER joins are a grow guard (they
/// need real join RTEs + NULL-extension, not a flattened cross join).
fn flatten_inner_joins(item: Node, from_list: &mut Vec<Node>, quals: &mut Vec<Node>) {
    match item {
        Node::JoinExpr(j) => {
            if j.jointype != crate::nodes::nodes::JoinType::INNER {
                not_yet_reachable("transformFromClause: OUTER join (needs a join RTE)");
            }
            // USING needs a real RTE_JOIN with merged columns; flattening to a
            // cross-join silently duplicates the USING columns and breaks
            // unqualified refs, so fail loudly until the join RTE exists.
            if !j.usingClause.is_empty() {
                not_yet_reachable("transformFromClause: JOIN ... USING (needs a join RTE); use JOIN ... ON");
            }
            let larg = j.larg.unwrap_or_else(|| not_yet_reachable("JoinExpr without larg"));
            let rarg = j.rarg.unwrap_or_else(|| not_yet_reachable("JoinExpr without rarg"));
            flatten_inner_joins(larg, from_list, quals);
            flatten_inner_joins(rarg, from_list, quals);
            if let Some(q) = j.quals {
                quals.push(q);
            }
        }
        other => from_list.push(other),
    }
}

/// AND the WHERE clause with the flattened join quals (an implicit-AND of the ON /
/// USING conditions). Returns the combined clause, or `None` if both are empty.
fn combine_quals(where_clause: Option<Node>, join_quals: Vec<Node>) -> Option<Node> {
    let mut clauses: Vec<Node> = join_quals;
    if let Some(w) = where_clause {
        clauses.insert(0, w);
    }
    let mut it = clauses.into_iter();
    let first = it.next()?;
    Some(it.fold(first, crate::backend::parser::parser::make_and_expr))
}

/// Transform the GROUP BY / ORDER BY / DISTINCT / LIMIT / OFFSET clauses of a
/// SELECT into their `Query` fields (M5, step 26). Factored out of
/// `transform_select_stmt_async` because `finish_query` consumes pstate's tlist
/// staging; these run before that.
fn transform_select_clauses(
    pstate: &mut ParseState,
    stmt: &SelectStmt,
    qry: &mut Query,
) {
    use crate::backend::parser::parse_clause::{
        transform_distinct_clause, transform_group_clause, transform_limit_clause,
        transform_sort_clause,
    };

    // GROUP BY first: it can append resjunk tlist entries that ORDER BY/DISTINCT
    // then see. The ORDER BY clauses are passed in so GROUP BY can reuse their refs.
    if !stmt.groupClause.is_empty() {
        let presort = transform_sort_clause(
            pstate,
            stmt.sortClause.clone(),
            &mut qry.targetList,
            ParseExprKind::OrderBy,
            true,
        );
        qry.groupClause = transform_group_clause(
            pstate,
            stmt.groupClause.clone(),
            &mut qry.groupingSets,
            &mut qry.targetList,
            presort.clone(),
            ParseExprKind::GroupBy,
            true,
        );
        qry.sortClause = presort;
    } else if !stmt.sortClause.is_empty() {
        qry.sortClause = transform_sort_clause(
            pstate,
            stmt.sortClause.clone(),
            &mut qry.targetList,
            ParseExprKind::OrderBy,
            false,
        );
    }

    // DISTINCT (over the whole select list), aligned with ORDER BY's leading refs.
    if !stmt.distinctClause.is_empty() {
        qry.distinctClause =
            transform_distinct_clause(pstate, &mut qry.targetList, qry.sortClause.clone(), false);
    }

    // LIMIT / OFFSET (coerced to int8).
    qry.limitOffset = transform_limit_clause(
        pstate,
        stmt.limitOffset.clone(),
        ParseExprKind::Offset,
        "OFFSET",
        crate::nodes::nodes::LimitOption::COUNT,
    );
    qry.limitCount = transform_limit_clause(
        pstate,
        stmt.limitCount.clone(),
        ParseExprKind::Limit,
        "LIMIT",
        stmt.limitOption,
    );
    qry.limitOption = stmt.limitOption;

    // hasAggs is read from pstate (set by transformAggregateCall during the tlist
    // transform); finish_query copies it onto qry.
}

/// Async-warm the operator/function syscaches the SELECT's expressions reference,
/// so the synchronous expression transform (`make_op`/`make_fn` -> hit-only
/// `search_sys_cache`) resolves them in a cold per-backend catcache (the wire
/// path). This is the operator/function analog of how `transform_from_clause`
/// warms the relation caches: there is no async expression transform, so the
/// caches are warmed up-front here.
///
/// The candidate operand types are the column types visible in the namespace plus
/// `int4`/`bool` (the integer-literal and boolean-result types reachable at M3);
/// every operator/function name found in the target list + WHERE clause is warmed
/// against that small type set (`OPERNAMENSP` -> `OPEROID` -> `PROCOID`, and
/// `PROCNAMEARGSNSP` -> `PROCOID` for function calls). Over-warming a few unused
/// M5 (step 26): async-warm the aggregate-resolution + grouping/ordering operator
/// caches the SYNC transform needs over the wire. There is no async aggregate /
/// clause transform, so the caches must be hit-warm before `transformTargetList` /
/// `transformGroupClause` / `transformSortClause` run:
///   - PROCNAMEARGSNSP for every aggregate name in the target list, over the empty
///     arg list (`count(*)`) and each candidate single-arg type; then AGGFNOID for
///     the resolved aggregate (read off the warmed pg_proc row).
///   - OPERNAMENSP for `=` / `<` / `>` over the candidate column types (the eqop /
///     sortop the GROUP BY / ORDER BY clause resolution looks up).
///
/// Over-warming unused combinations is harmless (a negative cache entry).
#[allow(
    clippy::cast_ptr_alignment,
    reason = "GETSTRUCT returns the MAXALIGN'd tuple body, aligned for Form_pg_proc"
)]
async fn warm_grouping_caches(shared: &Arc<SharedState>, pstate: &ParseState, stmt: &SelectStmt) {
    use crate::backend::catalog::heap::name_data;
    use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
    use crate::catalog::genbki::INT4OID;
    use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
    use crate::catalog::pg_proc::FormData_pg_proc;
    use crate::postgres::{NameGetDatum, ObjectIdGetDatum, PointerGetDatum};
    use crate::utils::syscache::SysCacheIdentifier as Sc;

    // Candidate operand/argument types: the namespace column types + int4.
    let mut types: Vec<Oid> = vec![INT4OID];
    for nsitem in &pstate.p_namespace {
        for col in &nsitem.nscolumns {
            if col.vartype != Oid(0) && !types.contains(&col.vartype) {
                types.push(col.vartype);
            }
        }
    }

    // 1) Aggregate names in the target list (FuncCall nodes). Warm PROCNAMEARGSNSP
    //    over the empty arg list (count(*)) and each single candidate-type arg, then
    //    AGGFNOID for whatever pg_proc row resolves (an aggregate prokind 'a').
    let mut agg_names: Vec<String> = Vec::new();
    for n in &stmt.targetList {
        if let Node::ResTarget(rt) = n {
            collect_func_names(rt.val.as_ref(), &mut agg_names);
        }
    }
    for name in &agg_names {
        let nd = name_data(name);
        // Try the zero-arg form (count(*)) and each single-arg type.
        let mut arglists: Vec<Vec<Oid>> = vec![Vec::new()];
        for &t in &types {
            arglists.push(vec![t]);
        }
        for argtypes in &arglists {
            let argvec = crate::utils::builtins::buildoidvector(argtypes);
            let keys = [
                NameGetDatum(&nd),
                PointerGetDatum(argvec.cast::<u8>()),
                ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
            ];
            let Some(tup) = search_sys_cache_populate(shared, Sc::PROCNAMEARGSNSP, &keys).await
            else {
                continue;
            };
            let aggfnoid = {
                // SAFETY: a held PROCNAMEARGSNSP hit -> a pg_proc row.
                let form = unsafe {
                    &*crate::access::htup_details::GETSTRUCT(&*tup).cast::<FormData_pg_proc>()
                };
                (form.prokind == crate::catalog::pg_proc::PROKIND_AGGREGATE).then_some(form.oid)
            };
            release_sys_cache(tup);
            if let Some(oid) = aggfnoid
                && let Some(t) =
                    search_sys_cache_populate(shared, Sc::AGGFNOID, &[ObjectIdGetDatum(oid)]).await
            {
                release_sys_cache(t);
            }
        }
    }

    // 2) The grouping / ordering comparison operators (`=`/`<`/`>`) over each
    //    candidate type, but only when the query actually groups/sorts/distincts.
    if stmt.groupClause.is_empty()
        && stmt.sortClause.is_empty()
        && stmt.distinctClause.is_empty()
    {
        return;
    }
    for opname in ["=", "<", ">"] {
        let nd = name_data(opname);
        for &t in &types {
            let keys = [
                NameGetDatum(&nd),
                ObjectIdGetDatum(t),
                ObjectIdGetDatum(t),
                ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
            ];
            if let Some(tup) = search_sys_cache_populate(shared, Sc::OPERNAMENSP, &keys).await {
                release_sys_cache(tup);
            }
        }
    }
}

/// Collect the (last-component) function names of every `FuncCall` in a raw
/// expression tree, for aggregate-cache pre-warming.
fn collect_func_names(node: Option<&Node>, names: &mut Vec<String>) {
    let Some(node) = node else { return };
    match node {
        Node::FuncCall(fc) => {
            if let Some(Node::String_(s)) = fc.funcname.last()
                && !names.contains(&s.sval)
            {
                names.push(s.sval.clone());
            }
            for arg in &fc.args {
                collect_func_names(Some(arg), names);
            }
        }
        Node::A_Expr(a) => {
            collect_func_names(a.lexpr.as_ref(), names);
            collect_func_names(a.rexpr.as_ref(), names);
        }
        Node::TypeCast(tc) => collect_func_names(tc.arg.as_ref(), names),
        _ => {}
    }
}

/// type combinations is harmless (a negative cache entry).
#[allow(
    clippy::cast_ptr_alignment,
    reason = "GETSTRUCT returns the MAXALIGN'd tuple body, aligned for Form_pg_operator"
)]
async fn warm_expr_caches(
    shared: &Arc<SharedState>,
    pstate: &ParseState,
    target_list: &[Node],
    where_clause: Option<&Node>,
) {
    use crate::backend::catalog::heap::name_data;
    use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
    use crate::catalog::genbki::{BOOLOID, INT4OID};
    use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
    use crate::catalog::pg_operator::FormData_pg_operator;
    use crate::postgres::{NameGetDatum, ObjectIdGetDatum};
    use crate::utils::syscache::SysCacheIdentifier;

    // Candidate operand types: the namespace column types + int4 + bool.
    let mut types: Vec<Oid> = vec![INT4OID, BOOLOID];
    for nsitem in &pstate.p_namespace {
        for col in &nsitem.nscolumns {
            if col.vartype != Oid(0) && !types.contains(&col.vartype) {
                types.push(col.vartype);
            }
        }
    }

    // Collect operator names + function names from the target list + WHERE.
    let mut op_names: Vec<String> = Vec::new();
    let mut fn_names: Vec<String> = Vec::new();
    for n in target_list {
        if let Node::ResTarget(rt) = n {
            collect_expr_names(rt.val.as_ref(), &mut op_names, &mut fn_names);
        }
    }
    collect_expr_names(where_clause, &mut op_names, &mut fn_names);

    // Warm each operator over the candidate type cross-product.
    for opname in &op_names {
        let nd = name_data(opname);
        for &lt in &types {
            for &rt in &types {
                let keys = [
                    NameGetDatum(&nd),
                    ObjectIdGetDatum(lt),
                    ObjectIdGetDatum(rt),
                    ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
                ];
                let Some(tup) =
                    search_sys_cache_populate(shared, SysCacheIdentifier::OPERNAMENSP, &keys).await
                else {
                    continue;
                };
                // Also warm OPEROID + the operator function's PROCOID.
                let (oid, oprcode) = {
                    // SAFETY: a held OPERNAMENSP hit -> a pg_operator row.
                    let form = unsafe {
                        &*crate::access::htup_details::GETSTRUCT(&*tup)
                            .cast::<FormData_pg_operator>()
                    };
                    (form.oid, form.oprcode)
                };
                release_sys_cache(tup);
                if let Some(t) = search_sys_cache_populate(
                    shared,
                    SysCacheIdentifier::OPEROID,
                    &[ObjectIdGetDatum(oid)],
                )
                .await
                {
                    release_sys_cache(t);
                }
                if let Some(t) = search_sys_cache_populate(
                    shared,
                    SysCacheIdentifier::PROCOID,
                    &[ObjectIdGetDatum(oprcode)],
                )
                .await
                {
                    release_sys_cache(t);
                }
            }
        }
    }

    // Function-call warming (PROCNAMEARGSNSP) is staged with the function-call
    // wire milestone; the M3 wire path reaches operators (warmed above). The
    // collected `fn_names` are recorded so that pass can grow here.
    let _ = &fn_names;

    // M4 (step 23): warm the cast-resolution caches the sync transform needs over
    // the wire -- the type-name lookups (TYPENAMENSP), the type metadata (TYPEOID),
    // and the cast catalog (CASTSOURCETARGET). The transform resolves casts in sync
    // context, so these must be hit-warm. Warm:
    //  - TYPENAMENSP for every type name referenced in a CAST/typed-literal, plus
    //    the M4 base-type names (so `::numeric`/`::float8`/`::text` resolve),
    //  - TYPEOID for the M4 base types (typinput/output/category reads),
    //  - CASTSOURCETARGET for the candidate-source x M4-target type cross product.
    warm_cast_caches(shared, target_list, where_clause, &types).await;
}

/// Async-warm the M4 cast-resolution syscaches (TYPENAMENSP / TYPEOID /
/// CASTSOURCETARGET) so the SYNC cast transform hits them over the wire. See
/// `warm_expr_caches`. The M4 base-type set is the numeric tower + date/time + the
/// existing namespace types in `candidate_types`.
async fn warm_cast_caches(
    shared: &Arc<SharedState>,
    target_list: &[Node],
    where_clause: Option<&Node>,
    candidate_types: &[Oid],
) {
    use crate::backend::catalog::heap::name_data;
    use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
    use crate::catalog::genbki::{
        BOOLOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NUMERICOID, TEXTOID,
        TIMESTAMPOID,
    };
    use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
    use crate::postgres::{NameGetDatum, ObjectIdGetDatum};
    use crate::utils::syscache::SysCacheIdentifier as Sc;

    // The M4 base types (oid + the canonical pg_catalog name to resolve by name).
    let base_types: &[(Oid, &str)] = &[
        (INT2OID, "int2"), (INT4OID, "int4"), (INT8OID, "int8"),
        (FLOAT4OID, "float4"), (FLOAT8OID, "float8"), (NUMERICOID, "numeric"),
        (DATEOID, "date"), (TIMESTAMPOID, "timestamp"), (TEXTOID, "text"), (BOOLOID, "bool"),
    ];

    // TYPEOID + TYPENAMENSP for each base type (name and oid resolution).
    for &(oid, name) in base_types {
        if let Some(t) = search_sys_cache_populate(shared, Sc::TYPEOID, &[ObjectIdGetDatum(oid)]).await {
            release_sys_cache(t);
        }
        let nd = name_data(name);
        let keys = [NameGetDatum(&nd), ObjectIdGetDatum(PG_CATALOG_NAMESPACE)];
        if let Some(t) = search_sys_cache_populate(shared, Sc::TYPENAMENSP, &keys).await {
            release_sys_cache(t);
        }
    }

    // Also warm TYPENAMENSP for any explicit type name in the query's casts/literals
    // (covers user-spelled names; the base set above covers the keyword spellings).
    let mut type_names: Vec<String> = Vec::new();
    for n in target_list {
        if let Node::ResTarget(rt) = n {
            collect_type_names(rt.val.as_ref(), &mut type_names);
        }
    }
    collect_type_names(where_clause, &mut type_names);
    for tn in &type_names {
        let nd = name_data(tn);
        let keys = [NameGetDatum(&nd), ObjectIdGetDatum(PG_CATALOG_NAMESPACE)];
        if let Some(t) = search_sys_cache_populate(shared, Sc::TYPENAMENSP, &keys).await {
            release_sys_cache(t);
        }
    }

    // CASTSOURCETARGET for (candidate source) x (every base target). Over-warming
    // unused pairs is harmless (a negative cache entry).
    let mut sources: Vec<Oid> = candidate_types.to_vec();
    for &(oid, _) in base_types {
        if !sources.contains(&oid) {
            sources.push(oid);
        }
    }
    for &src in &sources {
        for &(tgt, _) in base_types {
            let keys = [ObjectIdGetDatum(src), ObjectIdGetDatum(tgt)];
            if let Some(t) = search_sys_cache_populate(shared, Sc::CASTSOURCETARGET, &keys).await {
                release_sys_cache(t);
            }
        }
    }
}

/// Collect the (last-component) type names referenced in a raw expression's
/// TypeCasts (and the typed-literal TypeCasts), for cast-cache pre-warming.
fn collect_type_names(node: Option<&Node>, names: &mut Vec<String>) {
    let Some(node) = node else { return };
    match node {
        Node::TypeCast(tc) => {
            if let Some(tn) = &tc.typeName
                && let Some(last) = tn.names.last()
                && !names.contains(&last.sval)
            {
                names.push(last.sval.clone());
            }
            collect_type_names(tc.arg.as_ref(), names);
        }
        Node::A_Expr(a) => {
            collect_type_names(a.lexpr.as_ref(), names);
            collect_type_names(a.rexpr.as_ref(), names);
        }
        Node::BoolExpr(b) => {
            for arg in &b.args {
                collect_type_names(Some(arg), names);
            }
        }
        Node::FuncCall(fc) => {
            for arg in &fc.args {
                collect_type_names(Some(arg), names);
            }
        }
        Node::CaseExpr(c) => {
            collect_type_names(c.arg.as_ref(), names);
            for arm in &c.args {
                if let Node::CaseWhen(w) = arm {
                    collect_type_names(w.expr.as_ref(), names);
                    collect_type_names(w.result.as_ref(), names);
                }
            }
            collect_type_names(c.defresult.as_ref(), names);
        }
        Node::CoalesceExpr(c) => {
            for arg in &c.args {
                collect_type_names(Some(arg), names);
            }
        }
        Node::MinMaxExpr(m) => {
            for arg in &m.args {
                collect_type_names(Some(arg), names);
            }
        }
        _ => {}
    }
}

/// Recursively collect operator names (from `A_Expr`) and function names (from
/// `FuncCall`) out of a raw expression tree, for cache pre-warming.
fn collect_expr_names(node: Option<&Node>, ops: &mut Vec<String>, funcs: &mut Vec<String>) {
    let Some(node) = node else { return };
    match node {
        Node::A_Expr(a) => {
            if let Some(Node::String_(s)) = a.name.first()
                && !ops.contains(&s.sval)
            {
                ops.push(s.sval.clone());
            }
            collect_expr_names(a.lexpr.as_ref(), ops, funcs);
            collect_expr_names(a.rexpr.as_ref(), ops, funcs);
        }
        Node::BoolExpr(b) => {
            for arg in &b.args {
                collect_expr_names(Some(arg), ops, funcs);
            }
        }
        Node::FuncCall(fc) => {
            if let Some(Node::String_(s)) = fc.funcname.last()
                && !funcs.contains(&s.sval)
            {
                funcs.push(s.sval.clone());
            }
            for arg in &fc.args {
                collect_expr_names(Some(arg), ops, funcs);
            }
        }
        // M4 (step 23): recurse into casts + conditional expressions so the
        // operators inside (e.g. `a > 0` in a CASE WHEN, or NULLIF's "=") are warmed.
        Node::TypeCast(tc) => collect_expr_names(tc.arg.as_ref(), ops, funcs),
        Node::CaseExpr(c) => {
            collect_expr_names(c.arg.as_ref(), ops, funcs);
            for arm in &c.args {
                if let Node::CaseWhen(w) = arm {
                    collect_expr_names(w.expr.as_ref(), ops, funcs);
                    collect_expr_names(w.result.as_ref(), ops, funcs);
                }
            }
            collect_expr_names(c.defresult.as_ref(), ops, funcs);
        }
        Node::CoalesceExpr(c) => {
            for arg in &c.args {
                collect_expr_names(Some(arg), ops, funcs);
            }
        }
        Node::MinMaxExpr(m) => {
            for arg in &m.args {
                collect_expr_names(Some(arg), ops, funcs);
            }
        }
        _ => {}
    }
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

    // INSERT has no WHERE qual on its own jointree (the source SELECT, if any, was
    // transformed separately).
    finish_query(pstate, &mut qry, None);
    qry
}

/// PG `transformUpdateStmt`: build a `Query` for `UPDATE t SET ... [FROM ...]
/// [WHERE ...] [RETURNING ...]`. The target relation is opened as both result rel
/// and source (its columns are referenceable in SET/WHERE/RETURNING). The SET list
/// is resolved to attno-keyed TargetEntries (transformUpdateTargetList); the FROM
/// list / WHERE qual / RETURNING list are transformed against the namespace.
async fn transform_update_stmt(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &UpdateStmt,
) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::UPDATE;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformUpdateStmt: WITH clause");
    }

    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("transformUpdateStmt: missing target relation"));
    // setTargetTable(inh=true, alsoSource=true): the target is also a source rel.
    qry.resultRelation = crate::backend::parser::parse_clause::set_target_table(
        shared, pstate, relation, true, true, AclMode::UPDATE,
    )
    .await;

    // Additional FROM relations (UPDATE ... FROM other) extend the namespace.
    crate::backend::parser::parse_clause::transform_from_clause(
        shared,
        pstate,
        stmt.fromClause.clone(),
    )
    .await;

    // Warm the caches the SET / WHERE / RETURNING expressions reference, then run
    // the (sync) expression transforms.
    let set_targets = stmt.targetList.clone();
    warm_expr_caches(shared, pstate, &set_targets, stmt.whereClause.as_ref()).await;

    // SET targetlist -> attno-keyed TargetEntries (transformUpdateTargetList).
    qry.targetList = transform_update_target_list(pstate, &set_targets);

    // WHERE qual.
    let qual = crate::backend::parser::parse_clause::transform_where_clause(
        pstate,
        stmt.whereClause.clone(),
        ParseExprKind::Where,
        "WHERE",
    );

    // RETURNING.
    transform_returning(pstate, &mut qry, stmt.returningClause.as_deref());

    finish_query(pstate, &mut qry, qual);
    qry
}

/// PG `transformDeleteStmt`: build a `Query` for `DELETE FROM t [USING ...]
/// [WHERE ...] [RETURNING ...]`.
async fn transform_delete_stmt(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &DeleteStmt,
) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::DELETE;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformDeleteStmt: WITH clause");
    }

    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("transformDeleteStmt: missing target relation"));
    qry.resultRelation = crate::backend::parser::parse_clause::set_target_table(
        shared, pstate, relation, true, true, AclMode::DELETE,
    )
    .await;

    // USING relations extend the namespace (DELETE ... USING other).
    crate::backend::parser::parse_clause::transform_from_clause(
        shared,
        pstate,
        stmt.usingClause.clone(),
    )
    .await;

    warm_expr_caches(shared, pstate, &[], stmt.whereClause.as_ref()).await;

    let qual = crate::backend::parser::parse_clause::transform_where_clause(
        pstate,
        stmt.whereClause.clone(),
        ParseExprKind::Where,
        "WHERE",
    );

    transform_returning(pstate, &mut qry, stmt.returningClause.as_deref());

    finish_query(pstate, &mut qry, qual);
    qry
}

/// PG `transformMergeStmt` (M8 basic form): parse+analyze `MERGE INTO t USING src
/// ON cond WHEN ...`. The full executor path is staged (planner emits a
/// not_yet_reachable ModifyTable for MERGE this milestone); the transform builds the
/// Query (target + source join, the merge action list) so MERGE parses and analyzes.
async fn transform_merge_stmt(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    stmt: &MergeStmt,
) -> Box<Query> {
    let mut qry = make_query();
    qry.commandType = CmdType::MERGE;

    if stmt.withClause.is_some() {
        not_yet_reachable("transformMergeStmt: WITH clause");
    }

    let relation = stmt
        .relation
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("transformMergeStmt: missing target relation"));
    qry.resultRelation = crate::backend::parser::parse_clause::set_target_table(
        shared, pstate, relation, true, true, AclMode::INSERT,
    )
    .await;
    qry.mergeTargetRelation = qry.resultRelation;

    // The source relation (USING table_ref) joins the target.
    let source = stmt
        .sourceRelation
        .clone()
        .unwrap_or_else(|| not_yet_reachable("transformMergeStmt: missing source relation"));
    crate::backend::parser::parse_clause::transform_from_clause(shared, pstate, vec![source]).await;

    // The ON join condition.
    qry.mergeJoinCondition = crate::backend::parser::parse_clause::transform_where_clause(
        pstate,
        stmt.joinCondition.clone(),
        ParseExprKind::JoinOn,
        "MERGE ON",
    );

    // Transform the WHEN clauses into MergeActions. The action exprs resolve against
    // the joined namespace (target + source).
    qry.mergeActionList = transform_merge_when_clauses(pstate, &stmt.mergeWhenClauses);

    transform_returning(pstate, &mut qry, stmt.returningClause.as_deref());

    // The join qual is folded into the jointree (the source RTE is in the joinlist).
    finish_query(pstate, &mut qry, None);
    qry
}

/// PG `transformUpdateTargetList`: resolve each SET ResTarget to a TargetEntry keyed
/// to the target column's attno. The assigned expression is transformed
/// (UpdateSource kind) and coerced to the column type. `resname` is the column name;
/// `resjunk` false. The result is attno-ordered by preptlist::expand_targetlist.
fn transform_update_target_list(pstate: &mut ParseState, origtlist: &[Node]) -> Vec<Node> {
    use crate::nodes::makefuncs::makeTargetEntry;
    use crate::nodes::primnodes::{CoercionContext, CoercionForm};

    let rel = pstate
        .p_target_relation
        .as_ref()
        .unwrap_or_else(|| unreachable!("UPDATE target relation set by set_target_table"))
        .clone();
    let tupdesc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| unreachable!("UPDATE target relation has a descriptor"));

    origtlist
        .iter()
        .map(|node| {
            let Node::ResTarget(rt) = node else {
                not_yet_reachable("transformUpdateTargetList: SET item is not a ResTarget");
            };
            let colname = rt
                .name
                .clone()
                .unwrap_or_else(|| not_yet_reachable("transformUpdateTargetList: SET column has no name"));
            // attribute_to_attnum: find the named column's attno + type.
            let (attno, atttypid, atttypmod) = lookup_column(tupdesc, &colname);

            // Transform the assigned expression (UPDATE_SOURCE kind).
            let raw = rt
                .val
                .clone()
                .unwrap_or_else(|| not_yet_reachable("transformUpdateTargetList: SET value is SetToDefault"));
            let expr = crate::parser::parse_expr::transformExpr(
                pstate,
                Some(raw),
                ParseExprKind::UpdateSource,
            )
            .unwrap_or_else(|| not_yet_reachable("transformUpdateTargetList: NULL SET expression"));

            // Coerce the value to the column type (assignment context).
            let exprtype = crate::backend::nodes::nodeFuncs::exprType(&expr);
            let coerced = crate::backend::parser::parse_coerce::coerce_to_target_type(
                pstate,
                Some(expr),
                exprtype,
                atttypid,
                atttypmod,
                CoercionContext::ASSIGNMENT,
                CoercionForm::IMPLICIT_CAST,
                -1,
            )
            .unwrap_or_else(|| not_yet_reachable("transformUpdateTargetList: cannot coerce SET value to column type"));

            Node::TargetEntry(Box::new(makeTargetEntry(Some(coerced), attno, Some(colname), false)))
        })
        .collect()
}

/// PG `transformReturningList`: transform the RETURNING target list (a SELECT-like
/// projection over the modified rows). Sets `qry.returningList` and marks the query.
/// `RETURNING *` expands to all the target relation's columns.
fn transform_returning(
    pstate: &mut ParseState,
    qry: &mut Query,
    returning: Option<&crate::nodes::parsenodes::ReturningClause>,
) {
    let Some(returning) = returning else { return };
    if !returning.options.is_empty() {
        not_yet_reachable("transformReturningList: RETURNING WITH (OLD/NEW alias)");
    }
    let save_next_resno = pstate.p_next_resno;
    pstate.p_next_resno = 1;
    qry.returningList =
        transformTargetList(pstate, returning.exprs.clone(), ParseExprKind::Returning);
    pstate.p_next_resno = save_next_resno;
}

/// PG `transformMergeStmt`'s action-clause loop (M8 subset): turn each
/// `MergeWhenClause` into a `MergeAction`. The UPDATE SET list and INSERT VALUES list
/// are transformed against the joined namespace. The extra `WHEN MATCHED AND <qual>`
/// condition and RETURNING-from-MERGE are staged.
fn transform_merge_when_clauses(pstate: &mut ParseState, clauses: &[Node]) -> Vec<Node> {
    clauses
        .iter()
        .map(|node| {
            let Node::MergeWhenClause(wc) = node else {
                not_yet_reachable("transformMergeStmt: WHEN item is not a MergeWhenClause");
            };
            if wc.condition.is_some() {
                not_yet_reachable("transformMergeStmt: WHEN ... AND <condition>");
            }
            let target_list = match wc.commandType {
                CmdType::UPDATE => transform_update_target_list(pstate, &wc.targetList),
                CmdType::INSERT => {
                    not_yet_reachable("transformMergeStmt: WHEN NOT MATCHED THEN INSERT action transform")
                }
                CmdType::DELETE | CmdType::NOTHING => Vec::new(),
                other => not_yet_reachable(&format!("transformMergeStmt: WHEN action {other:?}")),
            };
            Node::MergeAction(Box::new(crate::nodes::primnodes::MergeAction {
                matchKind: wc.matchKind,
                commandType: wc.commandType,
                r#override: wc.r#override,
                qual: None,
                targetList: target_list,
                updateColnos: Vec::new(),
            }))
        })
        .collect()
}

/// Find a column by name in a tuple descriptor, returning its 1-based attno, type
/// OID, and typmod (PG `attnameAttNum` + the pg_attribute lookup).
fn lookup_column(
    tupdesc: &crate::access::tupdesc::TupleDescData,
    colname: &str,
) -> (crate::access::attnum::AttrNumber, Oid, i32) {
    for i in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(i);
        if attr.attisdropped {
            continue;
        }
        if attr_name(attr) == colname {
            return ((i + 1) as crate::access::attnum::AttrNumber, attr.atttypid, attr.atttypmod);
        }
    }
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("column \"{colname}\" of relation does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
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
fn finish_query(pstate: &mut ParseState, qry: &mut Query, qual: Option<Node>) {
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
        Some(Node::FromExpr(Box::new(crate::nodes::makefuncs::makeFromExpr(joinlist, qual))));

    qry.hasSubLinks = pstate.p_has_sub_links;
    qry.hasWindowFuncs = pstate.p_has_window_funcs;
    qry.hasTargetSRFs = pstate.p_has_target_srfs;
    qry.hasAggs = pstate.p_has_aggs;

    assign_query_collations(pstate, qry);
}

/// The not-yet-reachable clause guards shared by the SELECT transforms. WHERE
/// (transform_where_clause), GROUP BY / ORDER BY / DISTINCT / LIMIT / OFFSET
/// (transform_select_clauses) are handled as of M5; HAVING / WINDOW / locking grow.
fn reject_unsupported_select_clauses(stmt: &SelectStmt) {
    if stmt.havingClause.is_some() {
        not_yet_reachable("transformSelectStmt: HAVING clause");
    }
    if !stmt.windowClause.is_empty() {
        not_yet_reachable("transformSelectStmt: WINDOW clause");
    }
    // The locking clause (FOR UPDATE/SHARE) is handled by transform_locking_clause
    // after finish_query (M8, step 34).
}

/// PG `transformLockingClause`: turn each `FOR UPDATE/SHARE [OF ...]` clause into
/// `Query.rowMarks` (a `RowMarkClause` per locked relation RTE). With no `OF` list
/// the lock applies to every plain-relation RTE in the rangetable. `NOWAIT` / `SKIP
/// LOCKED` carry through the wait policy. Sets `hasForUpdate`.
fn transform_locking_clause(qry: &mut Query, locking: &[Node]) {
    use crate::nodes::lockoptions::LockClauseStrength;
    use crate::nodes::parsenodes::{RTEKind, RowMarkClause};

    for clause_node in locking {
        let Node::LockingClause(lc) = clause_node else {
            not_yet_reachable("transformLockingClause: not a LockingClause");
        };
        if !lc.lockedRels.is_empty() {
            not_yet_reachable("transformLockingClause: FOR UPDATE OF <rel> list");
        }
        // applyLockingClause to every plain-relation RTE (no OF list).
        for (i, rte_node) in qry.rtable.iter().enumerate() {
            let Node::RangeTblEntry(rte) = rte_node else { continue };
            if rte.rtekind != RTEKind::RELATION {
                continue;
            }
            let rti = (i + 1) as crate::nodes::primnodes::Index;
            // applyLockingClause: add (or strengthen) the rowmark for this RTI.
            if let Some(existing) = qry.rowMarks.iter_mut().find_map(|n| match n {
                Node::RowMarkClause(rmc) if rmc.rti == rti => Some(rmc),
                _ => None,
            }) {
                if (lc.strength as i32) > (existing.strength as i32) {
                    existing.strength = lc.strength;
                }
                if (lc.waitPolicy as i32) > (existing.waitPolicy as i32) {
                    existing.waitPolicy = lc.waitPolicy;
                }
            } else {
                qry.rowMarks.push(Node::RowMarkClause(Box::new(RowMarkClause {
                    rti,
                    strength: lc.strength,
                    waitPolicy: lc.waitPolicy,
                    pushedDown: false,
                })));
            }
        }
        // PG sets hasForUpdate for FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE alike.
        qry.hasForUpdate = true;
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

    #[test]
    fn join_using_fails_loudly_not_flatten() {
        // `JOIN ... USING` cannot be flattened to a cross-join (it would duplicate
        // the USING columns); it must route to the not-yet-reachable staging arm
        // rather than silently produce wrong output.
        use crate::backend::nodes::makefuncs::make_range_var;
        use crate::backend::parser::parser::make_join_expr;
        use crate::nodes::nodes::JoinType;
        let mk_rel =
            |name: &str| Node::RangeVar(Box::new(make_range_var(None, Some(name.to_string()), -1)));
        let join = make_join_expr(
            JoinType::INNER,
            mk_rel("a"),
            mk_rel("b"),
            None,
            vec!["x".to_string()],
        );
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut from_list = Vec::new();
            let mut quals = Vec::new();
            flatten_inner_joins(join, &mut from_list, &mut quals);
        }));
        std::panic::set_hook(prev);
        assert!(res.is_err(), "JOIN ... USING must fail loudly, not flatten");
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

    // ---- M3: operator / function resolution -------------------------------

    /// `SELECT a + 1 FROM t` -> the targetlist expr is an OpExpr resolving `+` over
    /// (int4, int4) to operator 551, function int4pl (177), result int4.
    #[tokio::test(flavor = "multi_thread")]
    async fn select_a_plus_one_resolves_to_int4pl_opexpr() {
        use crate::catalog::genbki::INT4OID;
        use crate::utils::fmgroids::F_INT4PL;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a + 1 FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::OpExpr(op) = te.expr.as_ref().unwrap() else { panic!("a + 1 not an OpExpr") };
            assert_eq!(op.opno, Oid(551), "the int4 + operator");
            assert_eq!(op.opfuncid, F_INT4PL, "opfuncid is int4pl");
            assert_eq!(op.opresulttype, INT4OID, "result type int4");
            assert!(!op.opretset);
            assert_eq!(op.args.len(), 2);
            assert!(matches!(op.args[0], Node::Var(_)), "lhs is the Var a");
            assert!(matches!(op.args[1], Node::Const(_)), "rhs is the Const 1");
        }))
        .await;
    }

    // ---------------------------------------------------------------------
    // M4 (step 23): casts + conditional expressions.
    // ---------------------------------------------------------------------

    /// `SELECT a::numeric FROM t` -> a FuncExpr calling int4_numeric (the via-func
    /// cast from pg_cast), result type numeric.
    #[tokio::test(flavor = "multi_thread")]
    async fn cast_int4_to_numeric_resolves_to_funcexpr() {
        use crate::catalog::genbki::NUMERICOID;
        // The int4->numeric cast function is `int4_numeric` (proname `numeric`, arg
        // int4) -- OID 1740, whose fmgroid is F_NUMERIC_INT4 (Gen_fmgrtab names by
        // proname_argtypes, not prosrc).
        use crate::utils::fmgroids::F_NUMERIC_INT4;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a::numeric FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::FuncExpr(f) = te.expr.as_ref().unwrap() else { panic!("cast not a FuncExpr") };
            assert_eq!(f.funcid, F_NUMERIC_INT4, "cast func is int4->numeric (OID 1740)");
            assert_eq!(f.funcresulttype, NUMERICOID, "result type numeric");
            assert_eq!(f.args.len(), 1);
        }))
        .await;
    }

    /// `SELECT CAST(a AS float8) FROM t` -> a FuncExpr calling i4tod, result float8.
    #[tokio::test(flavor = "multi_thread")]
    async fn cast_int4_to_float8_resolves_to_funcexpr() {
        use crate::catalog::genbki::FLOAT8OID;
        use crate::utils::fmgroids::F_FLOAT8_INT4;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT CAST(a AS float8) FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!() };
            let Node::FuncExpr(f) = te.expr.as_ref().unwrap() else { panic!("not a FuncExpr") };
            assert_eq!(f.funcid, F_FLOAT8_INT4, "cast func is int4->float8 (i4tod, OID 316)");
            assert_eq!(f.funcresulttype, FLOAT8OID);
        }))
        .await;
    }

    /// `SELECT a::text FROM t` -> a CoerceViaIO node (int4 -> text has no pg_cast
    /// row; find_coercion_pathway returns COERCEVIAIO for the string-category target).
    #[tokio::test(flavor = "multi_thread")]
    async fn cast_int4_to_text_resolves_to_coerce_via_io() {
        use crate::catalog::genbki::TEXTOID;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a::text FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!() };
            let Node::CoerceViaIO(c) = te.expr.as_ref().unwrap() else { panic!("text cast not a CoerceViaIO") };
            assert_eq!(c.resulttype, TEXTOID);
        }))
        .await;
    }

    /// `SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM t` -> a CaseExpr (searched
    /// form, casetype int4) with one WHEN arm and an ELSE.
    #[tokio::test(flavor = "multi_thread")]
    async fn case_when_resolves_to_caseexpr() {
        use crate::catalog::genbki::INT4OID;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT CASE WHEN a > 0 THEN 1 ELSE 0 END FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!() };
            let Node::CaseExpr(c) = te.expr.as_ref().unwrap() else { panic!("not a CaseExpr") };
            assert!(c.arg.is_none(), "searched CASE");
            assert_eq!(c.casetype, INT4OID);
            assert_eq!(c.args.len(), 1);
            assert!(c.defresult.is_some());
        }))
        .await;
    }

    /// `SELECT COALESCE(a, 0) FROM t` -> a CoalesceExpr; `NULLIF(a, 0)` -> a
    /// NullIfExpr; `GREATEST(a,0)`/`LEAST(a,0)` -> MinMaxExprs.
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesce_nullif_minmax_resolve() {
        use crate::catalog::genbki::INT4OID;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let target = |q: &crate::nodes::parsenodes::Query| -> Node {
                let Node::TargetEntry(te) = &q.targetList[0] else { panic!("no target") };
                te.expr.clone().unwrap()
            };

            let coalesce_q = analyze(&shared, "SELECT COALESCE(a, 0) FROM t").await;
            let Node::CoalesceExpr(coalesce) = target(&coalesce_q) else { panic!("not a CoalesceExpr") };
            assert_eq!(coalesce.coalescetype, INT4OID);
            assert_eq!(coalesce.args.len(), 2);

            let nullif_q = analyze(&shared, "SELECT NULLIF(a, 0) FROM t").await;
            let Node::NullIfExpr(nullif) = target(&nullif_q) else { panic!("not a NullIfExpr") };
            assert_eq!(nullif.opresulttype, INT4OID, "NULLIF result type is arg0's type");

            let greatest_q = analyze(&shared, "SELECT GREATEST(a, 0) FROM t").await;
            let Node::MinMaxExpr(greatest) = target(&greatest_q) else { panic!("not a MinMaxExpr") };
            assert!(matches!(greatest.op, crate::nodes::primnodes::MinMaxOp::GREATEST));
            assert_eq!(greatest.minmaxtype, INT4OID);

            let least_q = analyze(&shared, "SELECT LEAST(a, 0) FROM t").await;
            let Node::MinMaxExpr(least) = target(&least_q) else { panic!("not a MinMaxExpr") };
            assert!(matches!(least.op, crate::nodes::primnodes::MinMaxOp::LEAST));
        }))
        .await;
    }

    /// Unit: `find_coercion_pathway(int4 -> numeric)` is a via-function cast, and a
    /// string literal coerces to date (via the date typinput).
    #[tokio::test(flavor = "multi_thread")]
    async fn find_coercion_pathway_int4_numeric_is_func() {
        use crate::catalog::genbki::{INT4OID, NUMERICOID};
        use crate::nodes::primnodes::CoercionContext;
        use crate::parser::parse_coerce::{find_coercion_pathway, CoercionPathType};
        use crate::utils::fmgroids::F_NUMERIC_INT4;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            // Warm the CASTSOURCETARGET entry (the sync find_coercion_pathway is hit-only).
            warm_one_cast(&shared, INT4OID, NUMERICOID).await;

            let mut funcid = crate::postgres_ext::InvalidOid;
            let path = find_coercion_pathway(NUMERICOID, INT4OID, CoercionContext::IMPLICIT, &mut funcid);
            assert_eq!(path, CoercionPathType::Func, "int4->numeric is a via-func cast");
            assert_eq!(funcid, F_NUMERIC_INT4);
        }))
        .await;
    }

    /// Warm a single CASTSOURCETARGET entry (test helper).
    async fn warm_one_cast(shared: &Arc<SharedState>, src: Oid, tgt: Oid) {
        use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
        use crate::postgres::ObjectIdGetDatum;
        use crate::utils::syscache::SysCacheIdentifier;
        let keys = [ObjectIdGetDatum(src), ObjectIdGetDatum(tgt)];
        if let Some(t) =
            search_sys_cache_populate(shared, SysCacheIdentifier::CASTSOURCETARGET, &keys).await
        {
            release_sys_cache(t);
        }
    }

    /// `SELECT a FROM t WHERE a > 0` -> the qual is an OpExpr for int4gt with
    /// result type bool.
    #[tokio::test(flavor = "multi_thread")]
    async fn where_a_gt_zero_resolves_to_int4gt_opexpr() {
        use crate::catalog::genbki::BOOLOID;
        use crate::utils::fmgroids::F_INT4GT;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a FROM t WHERE a > 0").await;
            let Node::FromExpr(jt) = q.jointree.as_ref().expect("jointree") else { panic!("not FromExpr") };
            let Some(Node::OpExpr(op)) = jt.quals.as_ref() else { panic!("WHERE qual is not an OpExpr") };
            assert_eq!(op.opno, Oid(521), "the int4 > operator");
            assert_eq!(op.opfuncid, F_INT4GT, "opfuncid is int4gt");
            assert_eq!(op.opresulttype, BOOLOID, "comparison result type is bool");
        }))
        .await;
    }

    /// M3 qual planning: `SELECT a + 1 FROM t WHERE a > 0` plans to a SeqScan whose
    /// `plan.qual` carries the int4gt clause (from the WHERE via RestrictInfo) and
    /// whose targetlist carries the int4pl OpExpr (the projection).
    #[tokio::test(flavor = "multi_thread")]
    async fn where_qual_attaches_to_seqscan_plan() {
        use crate::nodes::plannodes::PlannedStmt;
        use crate::utils::fmgroids::{F_INT4GT, F_INT4PL};
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a + 1 FROM t WHERE a > 0").await;
            let stmt: PlannedStmt = plan(&shared, *q);
            let Node::SeqScan(seqscan) = &stmt.plan_tree else {
                panic!("top plan is not a SeqScan");
            };
            // The WHERE qual landed on the SeqScan as an int4gt OpExpr.
            assert_eq!(seqscan.scan.plan.qual.len(), 1, "one qual clause");
            let Node::OpExpr(qop) = &seqscan.scan.plan.qual[0] else { panic!("qual not an OpExpr") };
            assert_eq!(qop.opfuncid, F_INT4GT, "qual is int4gt");
            // The projection a+1 is an int4pl OpExpr in the targetlist.
            let Node::TargetEntry(te) = &seqscan.scan.plan.targetlist[0] else { panic!("not a TargetEntry") };
            let Node::OpExpr(top) = te.expr.as_ref().unwrap() else { panic!("tlist expr not an OpExpr") };
            assert_eq!(top.opfuncid, F_INT4PL, "projection is int4pl");
        }))
        .await;
    }

    /// `a AND b` / `NOT a` parse-analyze to BoolExpr nodes (booleans flow through
    /// unchanged for boolean operands).
    #[tokio::test(flavor = "multi_thread")]
    async fn bool_ops_resolve_to_boolexpr() {
        use crate::nodes::primnodes::BoolExprType;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            // `a > 0 AND a < 5` -> AND BoolExpr of two comparison OpExprs.
            let q = analyze(&shared, "SELECT a FROM t WHERE a > 0 AND a < 5").await;
            let Node::FromExpr(jt) = q.jointree.as_ref().expect("jointree") else { panic!("not FromExpr") };
            let Some(Node::BoolExpr(b)) = jt.quals.as_ref() else { panic!("WHERE qual is not a BoolExpr") };
            assert!(matches!(b.boolop, BoolExprType::AND_EXPR));
            assert_eq!(b.args.len(), 2);
            assert!(b.args.iter().all(|a| matches!(a, Node::OpExpr(_))));

            // `NOT (a > 0)` -> NOT BoolExpr.
            let q2 = analyze(&shared, "SELECT a FROM t WHERE NOT a > 0").await;
            let Node::FromExpr(jt2) = q2.jointree.as_ref().expect("jointree") else { panic!("not FromExpr") };
            let Some(Node::BoolExpr(n)) = jt2.quals.as_ref() else { panic!("WHERE qual is not a BoolExpr") };
            assert!(matches!(n.boolop, BoolExprType::NOT_EXPR));
            assert_eq!(n.args.len(), 1);
        }))
        .await;
    }

    /// SearchSysCache(OPERNAMENSP, '+', int4, int4) returns int4pl's operator row.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "GETSTRUCT returns the MAXALIGN'd tuple body, aligned for Form_pg_operator"
    )]
    async fn searchsyscache_opernamensp_resolves_plus() {
        use crate::access::htup_details::GETSTRUCT;
        use crate::backend::catalog::heap::name_data;
        use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache_populate};
        use crate::catalog::genbki::INT4OID;
        use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
        use crate::catalog::pg_operator::FormData_pg_operator;
        use crate::postgres::{NameGetDatum, ObjectIdGetDatum};
        use crate::utils::fmgroids::F_INT4PL;
        use crate::utils::syscache::SysCacheIdentifier;

        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let nd = name_data("+");
            let keys = [
                NameGetDatum(&nd),
                ObjectIdGetDatum(INT4OID),
                ObjectIdGetDatum(INT4OID),
                ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
            ];
            let tup = search_sys_cache_populate(&shared, SysCacheIdentifier::OPERNAMENSP, &keys).await;
            let tup = tup.expect("OPERNAMENSP('+',int4,int4,pg_catalog) must resolve");
            // SAFETY: a held OPERNAMENSP hit -> a pg_operator row.
            let form = unsafe { &*GETSTRUCT(&*tup).cast::<FormData_pg_operator>() };
            assert_eq!(form.oid, Oid(551), "+(int4,int4) is operator 551");
            assert_eq!(form.oprcode, F_INT4PL, "oprcode is int4pl");
            assert_eq!(form.oprresult, INT4OID);
            release_sys_cache(tup);
        }))
        .await;
    }

    /// A function call `int4pl(a, 1)` resolves via PROCNAMEARGSNSP to a FuncExpr.
    #[tokio::test(flavor = "multi_thread")]
    async fn func_call_resolves_to_funcexpr() {
        use crate::catalog::genbki::INT4OID;
        use crate::utils::fmgroids::F_INT4PL;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT int4pl(a, 1) FROM t").await;
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::FuncExpr(f) = te.expr.as_ref().unwrap() else { panic!("not a FuncExpr") };
            assert_eq!(f.funcid, F_INT4PL, "resolved to int4pl");
            assert_eq!(f.funcresulttype, INT4OID);
            assert_eq!(f.args.len(), 2);
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

    // ---- M5 (step 26): grouping / aggregation / ordering / distinct / limit -----

    /// `SELECT count(*) FROM t` -> hasAggs, one Aggref (count, OID 2803, int8) in the
    /// targetlist; plans to a plain Agg over the SeqScan.
    #[tokio::test(flavor = "multi_thread")]
    async fn count_star_analyzes_and_plans_to_plain_agg() {
        use crate::nodes::nodes::AggStrategy;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT count(*) FROM t").await;
            assert!(q.hasAggs, "count(*) sets hasAggs");
            assert!(q.groupClause.is_empty(), "no GROUP BY");
            let Node::TargetEntry(te) = &q.targetList[0] else { panic!("not a TargetEntry") };
            let Node::Aggref(agg) = te.expr.as_ref().unwrap() else { panic!("not an Aggref") };
            assert_eq!(agg.aggfnoid, Oid(2803), "count() aggregate OID");
            assert_eq!(agg.aggtype, Oid(20), "count returns int8");
            assert!(agg.aggstar, "count(*) sets aggstar");

            let stmt = plan(&shared, *q);
            let Node::Agg(a) = &stmt.plan_tree else { panic!("plan is not an Agg") };
            assert!(matches!(a.aggstrategy, AggStrategy::PLAIN), "no GROUP BY -> AGG_PLAIN");
            assert_eq!(a.num_cols, 0, "no grouping columns");
            assert!(matches!(a.plan.lefttree.as_ref(), Some(Node::SeqScan(_))), "Agg over SeqScan");
        }))
        .await;
    }

    /// `SELECT a, count(*) FROM t GROUP BY a` -> one SortGroupClause (eqop int4eq 96),
    /// the targetlist Var a + Aggref count; plans to AGG_SORTED over a Sort over the
    /// SeqScan, with grpColIdx pointing at the group key.
    #[tokio::test(flavor = "multi_thread")]
    async fn group_by_analyzes_and_plans_to_sorted_agg() {
        use crate::nodes::nodes::AggStrategy;
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a, count(*) FROM t GROUP BY a").await;
            assert!(q.hasAggs);
            assert_eq!(q.groupClause.len(), 1, "one GROUP BY column");
            let Node::SortGroupClause(sgc) = &q.groupClause[0] else { panic!("not a SortGroupClause") };
            assert_eq!(sgc.eqop, Oid(96), "int4 equality operator (int4eq)");
            assert_ne!(sgc.tleSortGroupRef, 0, "the group column has a sortgroupref");

            let stmt = plan(&shared, *q);
            let Node::Agg(a) = &stmt.plan_tree else { panic!("plan is not an Agg") };
            assert!(matches!(a.aggstrategy, AggStrategy::SORTED), "GROUP BY -> AGG_SORTED");
            assert_eq!(a.num_cols, 1);
            assert_eq!(a.grp_col_idx.len(), 1, "one grouping column index");
            assert_eq!(a.grp_operators[0], Oid(96));
            // The Agg's child is a Sort on the group key, over the SeqScan.
            let Some(Node::Sort(s)) = a.plan.lefttree.as_ref() else { panic!("Agg child is not a Sort") };
            assert_eq!(s.num_cols, 1, "Sort on the single group key");
            assert!(matches!(s.plan.lefttree.as_ref(), Some(Node::SeqScan(_))), "Sort over SeqScan");
        }))
        .await;
    }

    /// `SELECT a FROM t ORDER BY a DESC` -> one ORDER BY SortGroupClause with the `>`
    /// sortop (int4gt 521) and reverse_sort; plans to a Sort over the SeqScan.
    #[tokio::test(flavor = "multi_thread")]
    async fn order_by_desc_analyzes_and_plans_to_sort() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT a FROM t ORDER BY a DESC").await;
            assert_eq!(q.sortClause.len(), 1);
            let Node::SortGroupClause(sgc) = &q.sortClause[0] else { panic!("not a SortGroupClause") };
            assert!(sgc.reverse_sort, "DESC sets reverse_sort");
            assert_eq!(sgc.sortop, Oid(521), "int4 `>` operator for DESC");

            let stmt = plan(&shared, *q);
            let Node::Sort(s) = &stmt.plan_tree else { panic!("plan is not a Sort") };
            assert_eq!(s.num_cols, 1);
            assert_eq!(s.sort_operators[0], Oid(521));
            assert!(s.nulls_first[0], "DESC default is NULLS FIRST");
        }))
        .await;
    }

    /// `SELECT DISTINCT a FROM t` -> distinctClause set; plans to a Unique over a
    /// Sort over the SeqScan. `SELECT a FROM t ORDER BY a LIMIT 2` -> a Limit (int8
    /// count Const) over a Sort over the SeqScan.
    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_and_limit_plan_shapes() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            create_table_t(&shared).await;

            let q = analyze(&shared, "SELECT DISTINCT a FROM t").await;
            assert_eq!(q.distinctClause.len(), 1, "one DISTINCT column");
            let stmt = plan(&shared, *q);
            let Node::Unique(u) = &stmt.plan_tree else { panic!("plan is not a Unique") };
            assert_eq!(u.num_cols, 1);
            assert!(matches!(u.plan.lefttree.as_ref(), Some(Node::Sort(_))), "Unique over Sort");

            let q = analyze(&shared, "SELECT a FROM t ORDER BY a LIMIT 2").await;
            assert!(q.limitCount.is_some(), "LIMIT count set");
            let Some(Node::Const(c)) = q.limitCount.as_ref() else { panic!("LIMIT not a Const") };
            assert_eq!(c.consttype, Oid(20), "LIMIT count coerced to int8");
            assert_eq!(crate::postgres::DatumGetInt64(c.constvalue), 2);
            let stmt = plan(&shared, *q);
            let Node::Limit(l) = &stmt.plan_tree else { panic!("plan is not a Limit") };
            assert!(matches!(l.plan.lefttree.as_ref(), Some(Node::Sort(_))), "Limit over Sort");
        }))
        .await;
    }
}

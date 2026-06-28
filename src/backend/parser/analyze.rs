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

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{Query, QuerySource, RawStmt, SelectStmt, SetOperation};
use crate::nodes::primnodes::OverridingKind;
use crate::parser::parse_collate::assign_query_collations;
use crate::parser::parse_node::{make_parsestate, ParseExprKind, ParseState};
use crate::parser::parse_target::transformTargetList;
use crate::postgres_ext::Oid;

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
        // InsertStmt / DeleteStmt / UpdateStmt / MergeStmt / utility statements
        // grow in later milestones.
        other => not_yet_reachable(&format!("transformStmt: {other:?}")),
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

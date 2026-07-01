//! Primary module of the query rewriter. Translated from
//! backend/rewrite/rewriteHandler.c.
//!
//! `QueryRewrite` is the entry point: it runs the non-SELECT rule pass
//! (`RewriteQuery`), then the RIR (ON SELECT / view) pass (`fireRIRrules`) over
//! each resulting query, then the canSetTag bookkeeping. Non-type-centric free
//! functions; bodies here as snake_case `pub fn`s, re-exported from
//! `crate::rewrite::rewriteHandler` under the C names.
//!
//! Disposition: `grow`. The top-level shape of `QueryRewrite` and the
//! `RewriteQuery` dispatcher are reproduced faithfully, but the deep
//! rule/view/RLS bodies are scaffolded: for a table-less SELECT with no rules,
//! no RTEs, and no CTEs, `RewriteQuery` returns the single input query unchanged
//! and `fireRIRrules` is a pass-through. Every not-yet-translated arm
//! (INSERT/UPDATE/DELETE target-list rewriting, INSERT/UPDATE/DELETE rule firing,
//! WITH data-modifying recursion, view auto-update, RLS, sublink/RTE RIR
//! expansion) routes through a single clearly-marked staging guard (rules.md s4);
//! none is half-written. Later milestones (M11 views/rules/RI) ADD arms rather
//! than restructure.

use crate::nodes::nodes::CmdType;
use crate::nodes::parsenodes::{Query, QuerySource};

/// Panic for a rewrite path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `RewriteQuery`: rewrite the query and apply the rules again on the
/// rewritten queries; returns 0..n queries.
///
/// `rewrite_events` is the open-rule-action stack used to detect infinite rule
/// recursion; `orig_rt_length`/`num_ctes_processed` track product-query state.
/// For M1 every argument beyond `parsetree` is its identity value (`0`/empty).
///
/// M1 live path: a table-less `CMD_SELECT` with no CTEs. The WITH
/// data-modifying recursion, the INSERT/UPDATE/DELETE target-list rewriting and
/// rule firing, view auto-update, and product-query recursion are all scaffolded
/// guards reached only by non-M1 queries. SELECT (and UTILITY) are not rewritten
/// by this pass: the single input query falls straight through to the result
/// list.
fn rewrite_query(
    parsetree: Query,
    _rewrite_events: &mut Vec<RewriteEvent>,
    _orig_rt_length: i32,
    _num_ctes_processed: i32,
) -> Vec<Query> {
    let event = parsetree.commandType;

    // PG first recursively rewrites data-modifying statements in WITH clauses. The
    // M12 (step 43) CTEs are plain SELECT bodies (no data-modifying WITH, no views
    // inside CTE bodies for this milestone), so the WITH list passes through the
    // rewrite untouched; the data-modifying-WITH recursion grows later.
    if parsetree.hasModifyingCTE {
        not_yet_reachable("RewriteQuery: data-modifying WITH clause");
    }

    // INSERT/UPDATE/DELETE/MERGE: PG also fires I/U/D rules and handles view
    // auto-update here. Those (and the data-modifying-WITH recursion) grow with the
    // rules/views milestone (M11). The INSERT DEFAULT expansion (PG's
    // rewriteTargetListIns / build_column_default) is done in the planner's
    // `preprocess_targetlist` (expand_targetlist INSERT path) -- it needs the
    // result-relation descriptor, which the planner has -- so a plain data-modifying
    // statement is not run through this pass (postgres.rs routes it straight to the
    // planner). A rule-bearing relation would route here once rules land.
    if event != CmdType::SELECT && event != CmdType::UTILITY {
        not_yet_reachable("RewriteQuery: INSERT/UPDATE/DELETE/MERGE rule firing (M11)");
    }

    // SELECT and UTILITY are not rewritten by this pass. With no INSTEAD rule
    // and no product query, the (unmodified) original query is the sole result.
    // (PG distinguishes INSERT lcons vs append ordering, irrelevant for SELECT.)
    vec![parsetree]
}

/// PG `fireRIRrules`: apply ON SELECT (RIR) rules and expand views, recursing
/// through subquery RTEs. A RELATION RTE that is a view (has an ON SELECT
/// `_RETURN` rule in the registry) is replaced by a SUBQUERY RTE holding the
/// view's query (`apply_retrieve_rule`); a SUBQUERY RTE is recursed into. Plain
/// tables and the planner-injected RTE_RESULT have no rule and pass through.
///
/// `active_rirs` is the OID stack used to detect infinite recursion in
/// self-referencing views. RLS policies and sublink RIR recursion are staged.
fn fire_rir_rules(mut parsetree: Query, active_rirs: &mut Vec<crate::postgres_ext::Oid>) -> Query {
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::RTEKind;

    // The rtable grows as views expand into subquery RTEs; walk by index and
    // re-read the length each iteration (PG's manual rt_index loop).
    let mut rt_index = 0usize;
    while rt_index < parsetree.rtable.len() {
        // Snapshot the RTE kind / relid for this index without holding a borrow.
        let (rtekind, relid) = match &parsetree.rtable[rt_index] {
            Node::RangeTblEntry(rte) => (rte.rtekind, rte.relid),
            _ => not_yet_reachable("fireRIRrules: rangetable entry is not an RTE"),
        };

        match rtekind {
            // A subquery RTE: recurse into its query (e.g. an already-expanded view
            // referencing another view).
            RTEKind::SUBQUERY => {
                if let Node::RangeTblEntry(rte) = &mut parsetree.rtable[rt_index]
                    && let Some(sub) = rte.subquery.take()
                {
                    let expanded = fire_rir_rules(*sub, active_rirs);
                    if let Node::RangeTblEntry(rte) = &mut parsetree.rtable[rt_index] {
                        rte.subquery = Some(Box::new(expanded));
                    }
                }
            }
            // A relation RTE: if it is a view (has an ON SELECT rule), expand it.
            RTEKind::RELATION => {
                if let Some(view_query) = on_select_rule_action(relid) {
                    // Recursion guard: a view referencing itself.
                    if active_rirs.contains(&relid) {
                        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                            e.errcode(crate::utils::errcodes::ERRCODE_INVALID_OBJECT_DEFINITION)
                                .errmsg("infinite recursion detected in rules for relation".to_owned());
                        });
                        unreachable!("ereport(ERROR) diverges");
                    }
                    active_rirs.push(relid);
                    parsetree = apply_retrieve_rule(parsetree, view_query, rt_index, active_rirs);
                    active_rirs.pop();
                }
            }
            // RESULT: planner placeholder, nothing to expand. CTE: the body is held
            // in cteList (recursed below), not in this RTE; nothing to expand here.
            // VALUES: an in-line row list with no ON SELECT rule; nothing to expand.
            // FUNCTION: a function-in-FROM; PG recurses RIR into its funcexprs (for
            // views referenced in function arguments), but the step-08 SRFs take only
            // literal/scalar args -- no RTE to expand -- so it is a pass-through.
            RTEKind::RESULT | RTEKind::CTE | RTEKind::VALUES | RTEKind::FUNCTION => {}
            _ => not_yet_reachable("fireRIRrules: range-table / view / RLS expansion"),
        }
        rt_index += 1;
    }

    // Recurse into the WITH-list CTE bodies (each an analyzed Query), so a CTE that
    // scans a view expands. M12 CTE bodies are plain SELECTs (pass-through).
    if !parsetree.cteList.is_empty() {
        parsetree.cteList = std::mem::take(&mut parsetree.cteList)
            .into_iter()
            .map(|n| match n {
                Node::CommonTableExpr(mut cte) => {
                    if let Some(Node::Query(q)) = cte.ctequery.take() {
                        cte.ctequery = Some(Node::Query(Box::new(fire_rir_rules(*q, active_rirs))));
                    }
                    Node::CommonTableExpr(cte)
                }
                other => other,
            })
            .collect();
    }

    if parsetree.hasSubLinks {
        // PG `fireRIRonSubLink`: recurse RIR-rule application into every SubLink's
        // sub-Query (so views referenced inside a sub-select are expanded too). Walk
        // the target list + jointree quals (where SubLinks live after analysis).
        let mut tlist = std::mem::take(&mut parsetree.targetList);
        for n in &mut tlist {
            fire_rir_on_sublinks_node(n, active_rirs);
        }
        parsetree.targetList = tlist;
        let jointree = parsetree.jointree.take();
        if let Some(Node::FromExpr(mut f)) = jointree {
            if let Some(q) = f.quals.as_mut() {
                fire_rir_on_sublinks_node(q, active_rirs);
            }
            parsetree.jointree = Some(Node::FromExpr(f));
        } else {
            parsetree.jointree = jointree;
        }
    }
    parsetree
}

/// PG `fireRIRonSubLink` (the recursion part): descend an expression, and for each
/// SubLink, apply RIR rules to its sub-Query. Other node kinds recurse into their
/// children. Base-table sub-selects are unaffected (no view -> no rule).
fn fire_rir_on_sublinks_node(
    node: &mut crate::nodes::nodes::Node,
    active_rirs: &mut Vec<crate::postgres_ext::Oid>,
) {
    use crate::nodes::nodes::Node;
    match node {
        Node::SubLink(sl) => {
            if let Some(lhs) = sl.testexpr.as_mut() {
                fire_rir_on_sublinks_node(lhs, active_rirs);
            }
            if let Some(Node::Query(q)) = sl.subselect.take() {
                sl.subselect = Some(Node::Query(Box::new(fire_rir_rules(*q, active_rirs))));
            }
        }
        Node::TargetEntry(t) => {
            if let Some(e) = t.expr.as_mut() {
                fire_rir_on_sublinks_node(e, active_rirs);
            }
        }
        Node::BoolExpr(b) => {
            for a in &mut b.args {
                fire_rir_on_sublinks_node(a, active_rirs);
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &mut o.args {
                fire_rir_on_sublinks_node(a, active_rirs);
            }
        }
        Node::FuncExpr(f) => {
            for a in &mut f.args {
                fire_rir_on_sublinks_node(a, active_rirs);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_mut() {
                fire_rir_on_sublinks_node(a, active_rirs);
            }
        }
        _ => {}
    }
}

/// The view query stored as relation `relid`'s ON SELECT `_RETURN` rule action, or
/// `None` if `relid` is not a view. Reads the process-wide rule registry.
fn on_select_rule_action(relid: crate::postgres_ext::Oid) -> Option<Query> {
    let registry = crate::backend::rewrite::rule_registry::RuleRegistry::get()?;
    let rules = registry.rules_for(relid)?;
    rules
        .rules
        .iter()
        .find(|r| r.event == CmdType::SELECT && r.is_instead)
        .and_then(|r| r.actions.first().cloned())
}

/// PG `ApplyRetrieveRule` (the plain `SELECT FROM view` path): splice the view's
/// query into the host query's RTE at `rt_index`, turning that RTE into a
/// SUBQUERY RTE. The view's query is first expanded itself (recursive
/// `fire_rir_rules`) so a view-on-a-view inlines fully. The result-relation
/// (view as DML target), FOR UPDATE markup, RLS, and security-barrier handling
/// are staged (rules.md s4).
fn apply_retrieve_rule(
    mut parsetree: Query,
    view_query: Query,
    rt_index: usize,
    active_rirs: &mut Vec<crate::postgres_ext::Oid>,
) -> Query {
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::RTEKind;

    // The view is the SELECT's result relation only for DML on a view (staged).
    if parsetree.resultRelation == (rt_index as i32 + 1) {
        not_yet_reachable("ApplyRetrieveRule: view as INSERT/UPDATE/DELETE target");
    }

    // Deep-copy the rule action (the registry's tree must not be scribbled on) and
    // expand views referenced inside it. `active_rirs` still holds this view's OID,
    // so a self-reference is caught.
    let rule_action = fire_rir_rules(view_query, active_rirs);

    // Splice: turn the relation RTE into a subquery RTE holding the view's query.
    // relid/relkind/perminfoindex are deliberately left set (PG keeps them so the
    // view itself can be locked + permission-checked), but the planner's M11 path
    // reads only rtekind/subquery, so they are inert here.
    let Node::RangeTblEntry(rte) = &mut parsetree.rtable[rt_index] else {
        not_yet_reachable("ApplyRetrieveRule: target RTE is not an RTE");
    };
    rte.rtekind = RTEKind::SUBQUERY;
    rte.subquery = Some(Box::new(rule_action));
    rte.inh = false; // never set on a subquery RTE
    rte.tablesample = None;

    parsetree
}

/// PG `QueryRewrite`: primary entry point to the query rewriter. Rewrites one
/// top-level original query, possibly returning 0 or many queries.
///
/// The parsetree must have come straight from the parser or been scanned by
/// `AcquireRewriteLocks` for suitable locks.
pub fn query_rewrite(parsetree: Query) -> Vec<Query> {
    let input_query_id = parsetree.queryId;
    let orig_cmd_type = parsetree.commandType;

    // This function is only applied to top-level original queries.
    crate::assert!(parsetree.querySource == QuerySource::ORIGINAL);
    crate::assert!(parsetree.canSetTag);

    // Step 1: apply all non-SELECT rules, possibly getting 0 or many queries.
    let mut rewrite_events: Vec<RewriteEvent> = Vec::new();
    let querylist = rewrite_query(parsetree, &mut rewrite_events, 0, 0);

    // Step 2: apply all the RIR rules on each query; also a handy place to mark
    // each query with the original queryId.
    let mut results: Vec<Query> = Vec::with_capacity(querylist.len());
    for query in querylist {
        let mut active_rirs: Vec<crate::postgres_ext::Oid> = Vec::new();
        let mut query = fire_rir_rules(query, &mut active_rirs);
        query.queryId = input_query_id;
        results.push(query);
    }

    // Step 3: determine which, if any, of the resulting queries sets the command
    // tag, and update canSetTag accordingly. If the original query is still in
    // the list it sets the tag; otherwise the last INSTEAD query of the same kind
    // as the original is allowed to. (Either can leave no query setting the tag.)
    let mut found_original_query = false;
    let mut last_instead: Option<usize> = None;

    for (i, query) in results.iter().enumerate() {
        if query.querySource == QuerySource::ORIGINAL {
            crate::assert!(query.canSetTag);
            crate::assert!(!found_original_query);
            found_original_query = true;
            if !cfg!(debug_assertions) {
                break;
            }
        } else {
            crate::assert!(!query.canSetTag);
            if query.commandType == orig_cmd_type
                && (query.querySource == QuerySource::INSTEAD_RULE
                    || query.querySource == QuerySource::QUAL_INSTEAD_RULE)
            {
                last_instead = Some(i);
            }
        }
    }

    if !found_original_query
        && let Some(i) = last_instead
    {
        results[i].canSetTag = true;
    }

    results
}

/// PG `rewrite_event`: an open query-rewrite action, used to detect infinite
/// recursion in `RewriteQuery`. Unused on the M1 SELECT path (no rules fire);
/// the field shape is carried so rule firing can push/pop events as it grows.
#[allow(dead_code, reason = "grow: populated by INSERT/UPDATE/DELETE rule firing (M11)")]
struct RewriteEvent {
    /// OID of the relation having rules.
    relation: crate::postgres_ext::Oid,
    /// Type of rule being fired.
    event: CmdType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::Node;
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    /// Raw-parse + analyze `s` into its single top-level Query.
    fn analyze(s: &str) -> Query {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1, "expected exactly one statement");
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs: RawStmt = *rs;
        *crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, s, &[], 0, None)
    }

    #[test]
    fn rewrite_select_one_is_identity() {
        let q = analyze("SELECT 1");
        let expected = q.clone();

        let out = query_rewrite(q);

        assert_eq!(out.len(), 1, "table-less SELECT rewrites to one query");
        let got = &out[0];
        assert_eq!(got.commandType, CmdType::SELECT);
        assert_eq!(got.querySource, QuerySource::ORIGINAL);
        assert!(got.canSetTag, "the original query still sets the tag");
        assert_eq!(got.targetList.len(), 1);
        // queryId is stamped from the input (0 here) -- unchanged.
        assert_eq!(got.queryId, expected.queryId);
        // The query is returned unchanged.
        assert_eq!(*got, expected);
    }

    #[test]
    fn rewrite_preserves_single_target_entry() {
        let q = analyze("SELECT 42");
        let out = query_rewrite(q);
        assert_eq!(out.len(), 1);
        let Node::TargetEntry(te) = &out[0].targetList[0] else { panic!("not a TargetEntry") };
        assert_eq!(te.resno, 1);
        assert!(!te.resjunk);
    }

    #[test]
    fn two_statements_each_rewrite_to_themselves() {
        // QueryRewrite takes ONE Query and returns a list; SELECT 1 and SELECT 2
        // analyzed separately each rewrite to a one-element list equal to input.
        let q1 = analyze("SELECT 1");
        let q2 = analyze("SELECT 2");
        let e1 = q1.clone();
        let e2 = q2.clone();

        let out1 = query_rewrite(q1);
        let out2 = query_rewrite(q2);

        assert_eq!(out1.len(), 1);
        assert_eq!(out2.len(), 1);
        assert_eq!(out1[0], e1);
        assert_eq!(out2[0], e2);
        // The two rewrites are independent and distinct.
        assert_ne!(out1[0], out2[0]);
    }
}

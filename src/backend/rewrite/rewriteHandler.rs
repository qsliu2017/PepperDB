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
    parsetree: Box<Query>,
    _rewrite_events: &mut Vec<RewriteEvent>,
    _orig_rt_length: i32,
    _num_ctes_processed: i32,
) -> Vec<Query> {
    let event = parsetree.commandType;

    // PG first recursively rewrites data-modifying statements in WITH clauses.
    // M1 has no CTEs; a non-empty cteList means a WITH query, not yet reachable.
    if !parsetree.cteList.is_empty() {
        not_yet_reachable("RewriteQuery: WITH clause");
    }

    // INSERT/UPDATE/DELETE/MERGE: adjust the targetlist, fire I/U/D rules, handle
    // view auto-update, and recurse into product queries. None of this is
    // reachable for an M1 SELECT; the whole block grows in later milestones.
    if event != CmdType::SELECT && event != CmdType::UTILITY {
        not_yet_reachable("RewriteQuery: INSERT/UPDATE/DELETE/MERGE rewriting");
    }

    // SELECT and UTILITY are not rewritten by this pass. With no INSTEAD rule
    // and no product query, the (unmodified) original query is the sole result.
    // (PG distinguishes INSERT lcons vs append ordering, irrelevant for SELECT.)
    vec![*parsetree]
}

/// PG `fireRIRrules`: apply ON SELECT (RIR) rules and expand views/RLS, recursing
/// through sublinks and subquery RTEs.
///
/// M1 live path: a table-less SELECT has no RTEs to expand, no view to inline,
/// no RLS policies, and no sublinks, so this is a pass-through returning the
/// input query unchanged. The per-RTE view/RLS expansion and sublink recursion
/// grow with the range-table and rule machinery.
fn fire_rir_rules(parsetree: Query, _active_rirs: &mut Vec<crate::postgres_ext::Oid>) -> Query {
    // PG walks parsetree->rtable expanding view/subquery RTEs and applying RLS,
    // then recurses into sublinks. An empty range table with no sublinks has
    // nothing to do.
    if !parsetree.rtable.is_empty() {
        not_yet_reachable("fireRIRrules: range-table / view / RLS expansion");
    }
    if parsetree.hasSubLinks {
        not_yet_reachable("fireRIRrules: sublink RIR recursion");
    }
    parsetree
}

/// PG `QueryRewrite`: primary entry point to the query rewriter. Rewrites one
/// top-level original query, possibly returning 0 or many queries.
///
/// The parsetree must have come straight from the parser or been scanned by
/// `AcquireRewriteLocks` for suitable locks.
pub fn query_rewrite(parsetree: Box<Query>) -> Vec<Query> {
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
    fn analyze(s: &str) -> Box<Query> {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1, "expected exactly one statement");
        let Node::RawStmt(rs) = *list.remove(0) else { panic!("not a RawStmt") };
        let rs: RawStmt = *rs;
        crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, s, &[], 0, None)
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
        assert_eq!(*got, *expected);
    }

    #[test]
    fn rewrite_preserves_single_target_entry() {
        let q = analyze("SELECT 42");
        let out = query_rewrite(q);
        assert_eq!(out.len(), 1);
        let Node::TargetEntry(te) = &*out[0].targetList[0] else { panic!("not a TargetEntry") };
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
        assert_eq!(out1[0], *e1);
        assert_eq!(out2[0], *e2);
        // The two rewrites are independent and distinct.
        assert_ne!(out1[0], out2[0]);
    }
}

//! MIN()/MAX() aggregate optimization. Translated from
//! backend/optimizer/plan/planagg.c.
//!
//! `preprocess_minmax_aggregates` recognizes queries of the form
//! `SELECT MIN(x), MAX(y) FROM t` and rewrites each aggregate into an index
//! scan that reads just the first/last row, instead of scanning the whole table.
//! It is a pure OPTIMIZATION: declining to apply it leaves the normal aggregate
//! plan, which is correct (just slower).
//!
//! Translated FULLY at the structural level: the unoptimizable-case guards are
//! reproduced 1:1 (no aggregates, GROUP BY / windowing / grouping sets, CTEs,
//! more than one table). When none of those guards fires, PG would scan the
//! aggregate list and try to build an index path for each MIN/MAX.
//!
//! STAGED (rules.md s4): `can_minmax_aggs` (classify the Aggrefs as MIN/MAX and
//! find their sort operators) and `build_minmax_path` (construct the index
//! scan + the MinMaxAggPath) reach the selfuncs / index-path substrate that M7
//! does not exercise. We treat the optimization as "declined" past the guards,
//! so the normal grouping path is used. This matches PG's own behaviour when
//! `can_minmax_aggs` returns false or no indexable path is found.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::RTEKind;
use crate::nodes::pathnodes::PlannerInfo;

/// PG `preprocess_minmax_aggregates`: try to replace MIN/MAX aggregates with
/// index scans. Reproduces the unoptimizable-case guards; the index-path build
/// is staged, so we decline past the guards (normal aggregate plan is used).
pub fn preprocess_minmax_aggregates(root: &mut PlannerInfo) {
    let parse = &root.parse;

    // minmax_aggs list should be empty at this point.
    crate::assert!(root.minmax_aggs.is_empty());

    // Nothing to do if the query has no aggregates.
    if !parse.hasAggs {
        return;
    }

    crate::assert!(parse.setOperations.is_none()); // shouldn't get here if a setop
    crate::assert!(parse.rowMarks.is_empty()); // nor if FOR UPDATE

    // Reject GROUP BY / windowing / grouping sets: grouping looks at all rows
    // anyway, so there's no point optimizing MIN/MAX.
    if !parse.groupClause.is_empty() || parse.groupingSets.len() > 1 || parse.hasWindowFuncs {
        return;
    }

    // Reject queries with CTEs: there's no way to build an index scan on one.
    if !parse.cteList.is_empty() {
        return;
    }

    // Restrict to exactly one table; the single table could be buried in several
    // FromExpr levels due to subqueries.
    let mut jtnode = parse.jointree.as_ref();
    while let Some(Node::FromExpr(f)) = jtnode {
        if f.fromlist.len() != 1 {
            return;
        }
        jtnode = f.fromlist.first();
    }
    let Some(Node::RangeTblRef(rtr)) = jtnode else {
        return;
    };
    let rte = match root.simple_rte_array.get(rtr.rtindex as usize) {
        Some(Some(rte)) => rte,
        // Before query_planner builds simple_rte_array we can still consult the
        // parse rangetable; fall back to it.
        _ => match parse.rtable.get((rtr.rtindex - 1) as usize) {
            Some(Node::RangeTblEntry(rte)) => rte,
            _ => return,
        },
    };
    // ordinary relation, ok; flattened UNION ALL subquery, ok; else decline.
    let single_table_ok =
        matches!(rte.rtekind, RTEKind::RELATION) || (rte.rtekind == RTEKind::SUBQUERY && rte.inh);
    if single_table_ok {
        // Past the guards PG would: can_minmax_aggs(root, &aggs_list); then for
        // each build_minmax_path(...) or give up. Both reach the
        // index-path/selfuncs substrate. Decline (leave the normal aggregate
        // plan) until that lands.
        // TODO(planagg): can_minmax_aggs + build_minmax_path + MinMaxAggPath.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declines_when_no_aggs() {
        let mut root = crate::backend::optimizer::plan::initsplan::tests::test_planner_info();
        root.parse.hasAggs = false;
        // Must not panic and must not populate minmax_aggs.
        preprocess_minmax_aggregates(&mut root);
        assert!(root.minmax_aggs.is_empty());
    }

    #[test]
    fn declines_with_group_by() {
        let mut root = crate::backend::optimizer::plan::initsplan::tests::test_planner_info();
        root.parse.hasAggs = true;
        // A non-empty groupClause makes PG decline before reaching staged code.
        root.parse.groupClause =
            vec![Node::RangeTblRef(Box::new(crate::nodes::primnodes::RangeTblRef { rtindex: 1 }))];
        preprocess_minmax_aggregates(&mut root);
        assert!(root.minmax_aggs.is_empty());
    }
}

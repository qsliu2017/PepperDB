//! Aggregate preprocessing. Translated from backend/optimizer/prep/prepagg.c.
//!
//! `preprocess_aggrefs` walks an expression tree, and for each `Aggref` assigns
//! its `aggno` (a slot in `root.agginfos`) and `aggtransno` (a slot in
//! `root.aggtransinfos`), de-duplicating identical aggregate calls so they share
//! one transition-state computation. PG runs this before `query_planner`; the
//! numbering it produces is later consumed by setrefs/createplan.
//!
//! STAGED (rules.md s4): `preprocess_aggref` -- the per-Aggref body -- fetches
//! the aggregate's transition/final/combine/serial functions and transition
//! type from `pg_aggregate` (`SearchSysCache1(AGGFNOID)`, `get_aggregate_argtypes`,
//! `resolve_aggregate_transtype`, `get_typlenbyval`), none of which the syscache
//! exposes yet. So the dedup/numbering here is staged. M5 currently assigns
//! `aggno == aggtransno` positionally in `set_upper_references` (setrefs.rs),
//! which is correct for the single-table single-agg M5 queries; that positional
//! approach stays live until the catalog lookups land here.
//!
//! Translated: the public entry `preprocess_aggrefs` and its tree walker
//! (find every Aggref). When an Aggref is actually present, processing routes to
//! `not_yet_reachable` rather than silently mis-numbering.

use crate::nodes::nodeFuncs::expression_tree_walker;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::PlannerInfo;

/// Panic for a prepagg path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `preprocess_aggrefs`: assign `aggno`/`aggtransno` to every Aggref in
/// `clause`, populating `root.agginfos` / `root.aggtransinfos` with dedup. The
/// per-Aggref body is staged (pg_aggregate syscache); see the module doc.
pub fn preprocess_aggrefs(root: &mut PlannerInfo, clause: &Node) {
    preprocess_aggrefs_walker(clause, root);
}

/// PG `preprocess_aggrefs_walker`: when the node is an Aggref, process it (don't
/// recurse into its args); otherwise recurse via `expression_tree_walker`.
#[allow(
    clippy::only_used_in_recursion,
    reason = "staged: root feeds preprocess_aggref (the pg_aggregate lookup) once it lands"
)]
fn preprocess_aggrefs_walker(node: &Node, root: &mut PlannerInfo) -> bool {
    if let Node::Aggref(aggref) = node {
        // PG calls preprocess_aggref(aggref, root) here, which reads pg_aggregate.
        // Staged: route to not_yet_reachable so we don't mis-number. M5's
        // positional setrefs numbering covers the queries we currently plan.
        crate::assert!(aggref.agglevelsup == 0);
        not_yet_reachable("preprocess_aggref: pg_aggregate transition-state lookup");
    }
    expression_tree_walker(node, |child| preprocess_aggrefs_walker(child, root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::primnodes::{Var, VarReturningType};
    use crate::postgres_ext::{InvalidOid, Oid};

    fn var() -> Node {
        Node::Var(Box::new(Var {
            varno: 1,
            varattno: 1,
            vartype: Oid::new(23),
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: 1,
            varattnosyn: 1,
            location: -1,
        }))
    }

    /// With no Aggref present, the walker visits everything and touches no root
    /// state. (A clause containing an Aggref would route to the staged path.)
    #[test]
    fn preprocess_aggrefs_no_aggref_is_noop() {
        let mut root = crate::backend::optimizer::plan::initsplan::tests::test_planner_info();
        preprocess_aggrefs(&mut root, &var());
        assert!(root.agginfos.is_empty());
        assert!(root.aggtransinfos.is_empty());
    }
}

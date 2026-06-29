//! Join-removal analysis. Translated from backend/optimizer/plan/analyzejoins.c.
//!
//! These are OPTIMIZATIONS run after `deconstruct_jointree`: `remove_useless_joins`
//! drops an inner/left join to a provably-unique, otherwise-unused side, and
//! `reduce_unique_semijoins` turns a SEMI join whose RHS is unique into a plain
//! inner join. Both are correctness-neutral: declining to remove anything yields
//! a correct (just unoptimized) plan.
//!
//! STAGED (rules.md s4): the uniqueness-proof substrate -- `join_is_removable`,
//! `rel_supports_distinctness`, `query_is_distinct_for`, `innerrel_is_unique`,
//! and the `remove_rel_from_query` surgery -- depends on unique-index proofs
//! (`relation_has_unique_index_for`, selfuncs) and the join-tree rewrite that
//! M7 does not exercise. We translate the public structure as a near-no-op:
//! `remove_useless_joins` / `remove_useless_self_joins` scan but return the
//! joinlist unchanged, and `reduce_unique_semijoins` leaves `join_info_list`
//! untouched. The uniqueness predicates route to `not_yet_reachable`.
//!
//! The no-op is faithful to PG's own no-op path: when no join is removable,
//! `remove_useless_joins` only reads `join_info_list` (via `join_is_removable`)
//! and returns `joinlist`; it mutates `root` only on the removal path.

use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::Query;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, RestrictInfo, Relids};
use crate::postgres_ext::Oid;

/// Panic for an analyzejoins path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `remove_useless_joins`: drop left/inner joins to a unique, unused side.
///
/// Near-no-op: PG iterates `join_info_list`, calling `join_is_removable` for
/// each, and only mutates `root`/`joinlist` on the removal path. The
/// removal-proof substrate is staged, so we always take the "not removable"
/// branch and return `joinlist` unchanged. No `root` fields are written.
pub fn remove_useless_joins(root: &mut PlannerInfo, joinlist: Vec<Node>) -> Vec<Node> {
    let _ = root;
    // TODO(join-removal): iterate join_info_list, call join_is_removable, and on
    // success remove_leftjoinrel_from_query + drop the sjinfo, restarting.
    joinlist
}

/// PG `reduce_unique_semijoins`: convert a SEMI join with a unique RHS into a
/// plain inner join. Near-no-op: the `rel_supports_distinctness` /
/// `innerrel_is_unique` proof is staged, so we leave `join_info_list` untouched.
pub fn reduce_unique_semijoins(root: &mut PlannerInfo) {
    let _ = root;
    // TODO(join-removal): for each JOIN_SEMI sjinfo with a single-baserel RHS
    // that is provably unique for the join clauses, rewrite it to JOIN_INNER.
}

/// PG `remove_useless_self_joins`: drop a self-join between two scans of the same
/// relation when the join condition forces row identity. Near-no-op (staged).
pub fn remove_useless_self_joins(root: &mut PlannerInfo, joinlist: Vec<Node>) -> Vec<Node> {
    let _ = root;
    // TODO(join-removal): self-join elimination (split_selfjoin_quals + surgery).
    joinlist
}

/// PG `query_supports_distinctness`: can `query_is_distinct_for` ever prove this
/// subquery distinct? Staged with the rest of the uniqueness substrate.
pub fn query_supports_distinctness(_query: &Query) -> bool {
    not_yet_reachable("query_supports_distinctness: subquery distinctness proof");
}

/// PG `query_is_distinct_for`: is the subquery's output distinct on `colnos`
/// under the equality operators `opids`? Staged.
pub fn query_is_distinct_for(_query: &Query, _colnos: &[Node], _opids: &[Oid]) -> bool {
    not_yet_reachable("query_is_distinct_for: subquery distinctness proof");
}

/// PG `innerrel_is_unique`: is `innerrel` provably unique for the given outer
/// rels under the join clauses? Staged (unique-index proof substrate).
pub fn innerrel_is_unique(
    _root: &mut PlannerInfo,
    _joinrelids: Relids,
    _outerrelids: Relids,
    _innerrel: &RelOptInfo,
    _jointype: JoinType,
    _restrictlist: &[RestrictInfo],
    _force_cache: bool,
) -> bool {
    not_yet_reachable("innerrel_is_unique: relation uniqueness proof");
}

/// PG `innerrel_is_unique_ext`: like `innerrel_is_unique`, but also returns the
/// extra baserestrictinfo clauses used in the proof (C out-param `extra_clauses`).
/// Staged.
pub fn innerrel_is_unique_ext(
    _root: &mut PlannerInfo,
    _joinrelids: Relids,
    _outerrelids: Relids,
    _innerrel: &RelOptInfo,
    _jointype: JoinType,
    _restrictlist: &[RestrictInfo],
    _force_cache: bool,
) -> (bool, Vec<RestrictInfo>) {
    not_yet_reachable("innerrel_is_unique_ext: relation uniqueness proof");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::primnodes::RangeTblRef;

    /// A trivial PlannerInfo good enough for the no-op functions, which only
    /// read (no fields) on the no-op path.
    fn test_root() -> PlannerInfo {
        crate::backend::optimizer::plan::initsplan::tests::test_planner_info()
    }

    fn rtr(i: i32) -> Node {
        Node::RangeTblRef(Box::new(RangeTblRef { rtindex: i }))
    }

    #[test]
    fn remove_useless_joins_is_noop() {
        let mut root = test_root();
        let joinlist = vec![rtr(1), rtr(2)];
        let out = remove_useless_joins(&mut root, joinlist.clone());
        assert_eq!(out, joinlist, "no-op must return the joinlist unchanged");
        // join_info_list untouched (was empty, still empty).
        assert!(root.join_info_list.is_empty());
    }

    #[test]
    fn reduce_unique_semijoins_is_noop() {
        let mut root = test_root();
        let before = root.join_info_list.len();
        reduce_unique_semijoins(&mut root);
        assert_eq!(root.join_info_list.len(), before);
    }

    #[test]
    fn remove_useless_self_joins_is_noop() {
        let mut root = test_root();
        let joinlist = vec![rtr(1)];
        let out = remove_useless_self_joins(&mut root, joinlist.clone());
        assert_eq!(out, joinlist);
    }
}

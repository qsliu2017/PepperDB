//! Utilities for matching and building path keys. Translated from
//! backend/optimizer/path/pathkeys.c.
//!
//! A PathKey describes one sort key in a path's output ordering: an
//! EquivalenceClass (the value being ordered), the index opfamily defining the
//! ordering operator, the sort direction (`cmptype`, COMPARE_LT for ASC /
//! COMPARE_GT for DESC), and a NULLS-FIRST flag. A list of PathKeys describes a
//! complete sort order. Pathkeys drive merge joins and let the planner satisfy
//! an ORDER BY without an explicit sort. See src/backend/optimizer/README.
//!
//! Representation note (differs from PG's pointer identity):
//! PG keeps every "canonical" PathKey once in `root->canon_pathkeys` and lets
//! paths share that single pointer, so two pathkeys are "the same" iff their
//! pointers are equal -- and `pathkey_is_redundant`, `compare_pathkeys`,
//! `find_mergeclauses_for_outer_pathkeys`, etc. all rely on raw pointer
//! comparison of both the PathKey and its `pk_eclass`. Here `PathKey` carries a
//! *cloned* `Box<EquivalenceClass>` snapshot (the committed pathnodes
//! representation), so there is no pointer to compare. Instead we compare by
//! VALUE: `PathKey` derives `PartialEq`, which compares the boxed EC by value
//! plus opfamily/cmptype/nulls_first. Because `make_canonical_pathkey` dedups
//! every PathKey it hands out against `root.canon_pathkeys`, value equality of
//! two canonical PathKeys coincides exactly with PG's pointer equality, and EC
//! identity reduces to EC value equality (same members/relids/opfamilies/...).
//! Every "same EC" / "same pathkey" test below is therefore a `==` on the value.
//!
//! Scope (rules.md s4, INNER-JOIN + ORDER BY path complete): fully translated
//! are make_canonical_pathkey, append_pathkeys, pathkey_is_redundant,
//! make_pathkey_from_sortinfo/sortop, compare_pathkeys, pathkeys_contained_in,
//! pathkeys_count_contained_in, build_index_pathkeys, build_expression_pathkey,
//! build_join_pathkeys, make_pathkeys_for_sortclauses(+_extended), the
//! merge-clause reasoning (initialize/update_mergeclause_eclasses,
//! find_mergeclauses_for_outer_pathkeys, select_outer_pathkeys_for_merge,
//! make_inner_pathkeys_for_merge, trim_mergeclauses_for_inner_pathkeys), and the
//! usefulness checks (truncate_useless_pathkeys, has_useful_pathkeys,
//! get_cheapest_*_path_for_pathkeys). Staged (stub-call, note inline): the
//! partitioning helpers (build_partition_pathkeys), subquery conversion
//! (convert_subquery_pathkeys), and GROUP BY reordering
//! (get_useful_group_keys_orderings). The syscache lookups they reach
//! (get_opfamily_member_for_cmptype, get_ordering_op_properties, op_input_types,
//! ...) are themselves stubs today; pathkey *construction* works regardless.

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: some helpers take owned node values matching PG C signatures"
)]

use crate::access::cmptype::CompareType;
use crate::access::sdir::{scan_direction_is_backward, ScanDirection};
use crate::backend::nodes::nodeFuncs::{exprCollation, get_leftop, get_rightop};
use crate::nodes::bitmapset::{bms_is_subset, bms_overlap};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::SortGroupClause;
use crate::nodes::pathnodes::{
    CostSelector, EquivalenceClass, IndexOptInfo, Path, PathKey, PlannerInfo, RelOptInfo, Relids,
    RestrictInfo,
};
use crate::nodes::primnodes::{Expr, Index, TargetEntry};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::elog::OrElog;
use crate::utils::lsyscache::{
    get_mergejoin_opfamilies, get_opfamily_member_for_cmptype, get_ordering_op_properties,
    op_input_types,
};

/// PG `enable_group_by_reordering` GUC.
pub static mut ENABLE_GROUP_BY_REORDERING: bool = true;

/****************************************************************************
 *		PATHKEY CONSTRUCTION AND REDUNDANCY TESTING
 ****************************************************************************/

/// PG `make_canonical_pathkey`: find a pre-existing matching pathkey in the
/// query's "canonical" pathkey list, creating one if absent. Returns a clone of
/// the canonical entry (PG returns the shared pointer; see the representation
/// note -- here value equality stands in for pointer identity).
///
/// Must not be used until EC merging is complete (`root.ec_merging_done`).
#[must_use]
pub fn make_canonical_pathkey(
    root: &mut PlannerInfo,
    eclass: &EquivalenceClass,
    opfamily: Oid,
    cmptype: CompareType,
    nulls_first: bool,
) -> PathKey {
    if !root.ec_merging_done {
        crate::elog!(crate::utils::elog::ERROR, "too soon to build canonical pathkeys");
    }

    // The passed eclass might be non-canonical, so chase up to the top.
    let mut eclass = eclass.clone();
    while let Some(merged) = eclass.merged.clone() {
        eclass = *merged;
    }

    for pk in &root.canon_pathkeys {
        if *pk.eclass == eclass
            && pk.opfamily == opfamily
            && pk.cmptype == cmptype
            && pk.nulls_first == nulls_first
        {
            return (**pk).clone();
        }
    }

    let pk = PathKey {
        eclass: Box::new(eclass),
        opfamily,
        cmptype,
        nulls_first,
    };
    root.canon_pathkeys.push(Box::new(pk.clone()));
    pk
}

/// PG `append_pathkeys`: append all non-redundant PathKeys in `source` onto
/// `target`, returning the updated list.
#[must_use]
pub fn append_pathkeys(mut target: Vec<PathKey>, source: &[PathKey]) -> Vec<PathKey> {
    for pk in source {
        if !pathkey_is_redundant(pk, &target) {
            target.push(pk.clone());
        }
    }
    target
}

/// PG `pathkey_is_redundant`: is a pathkey redundant with one already in the
/// list? Redundant if its EC contains a constant (EC_MUST_BE_REDUNDANT) or if
/// the same EC already appears in the list. Both the pathkey and the list
/// members must be canonical for this to be correct.
fn pathkey_is_redundant(new_pathkey: &PathKey, pathkeys: &[PathKey]) -> bool {
    // EC_MUST_BE_REDUNDANT(eclass) == eclass->ec_has_const.
    if new_pathkey.eclass.has_const {
        return true;
    }

    pathkeys.iter().any(|old| *old.eclass == *new_pathkey.eclass)
}

/// PG `make_pathkey_from_sortinfo`: given an expression and sort-order info,
/// create a canonical PathKey (possibly redundant). Returns None if `create_it`
/// is false and the sort key isn't already in some EquivalenceClass.
#[allow(clippy::too_many_arguments, reason = "1:1 PG port of make_pathkey_from_sortinfo")]
fn make_pathkey_from_sortinfo(
    root: &mut PlannerInfo,
    expr: &Expr,
    opfamily: Oid,
    opcintype: Oid,
    collation: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> Option<PathKey> {
    let cmptype = if reverse_sort {
        CompareType::Gt
    } else {
        CompareType::Lt
    };

    // EquivalenceClasses contain opfamily lists based on mergejoinable equality
    // operators, so look up the opfamily's equality operator and its membership.
    let equality_op =
        get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, CompareType::Eq);
    let Some(equality_op) = equality_op.filter(|op| *op != InvalidOid) else {
        crate::elog!(
            crate::utils::elog::ERROR,
            "missing equality operator in opfamily"
        );
        unreachable!("elog(ERROR) diverges");
    };
    let opfamilies = get_mergejoin_opfamilies(equality_op);
    if opfamilies.is_empty() {
        crate::elog!(
            crate::utils::elog::ERROR,
            "could not find opfamilies for equality operator"
        );
    }

    // Find or (optionally) create a matching EquivalenceClass.
    let eclass =
        get_eclass_for_sort_expr(root, expr, &opfamilies, opcintype, collation, sortref, rel, create_it)?;

    Some(make_canonical_pathkey(root, &eclass, opfamily, cmptype, nulls_first))
}

/// PG `make_pathkey_from_sortop`: like make_pathkey_from_sortinfo, but work from
/// a sort operator (looked up in pg_amop).
fn make_pathkey_from_sortop(
    root: &mut PlannerInfo,
    expr: &Expr,
    ordering_op: Oid,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: Index,
    create_it: bool,
) -> Option<PathKey> {
    let Some((opfamily, opcintype, _cmptype)) = get_ordering_op_properties(ordering_op) else {
        crate::elog!(
            crate::utils::elog::ERROR,
            "operator is not a valid ordering operator"
        );
        unreachable!("elog(ERROR) diverges");
    };

    // SortGroupClause doesn't carry collation, so consult the expr.
    let collation = exprCollation(expr);

    make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        collation,
        reverse_sort,
        nulls_first,
        sortref,
        Relids::default(),
        create_it,
    )
}

/****************************************************************************
 *		PATHKEY COMPARISONS
 ****************************************************************************/

/// PG `compare_pathkeys`: compare two pathkey lists for equality / containment.
/// BETTER1 means keys1 is the longer (superset); BETTER2 means keys2 is longer.
#[must_use]
pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison {
    for (pathkey1, pathkey2) in keys1.iter().zip(keys2.iter()) {
        if pathkey1 != pathkey2 {
            return PathKeysComparison::Different; // no need to keep looking
        }
    }

    // If we reached the end of only one list, the other is longer (not a subset).
    match keys1.len().cmp(&keys2.len()) {
        std::cmp::Ordering::Greater => PathKeysComparison::Better1, // key1 is longer
        std::cmp::Ordering::Less => PathKeysComparison::Better2,    // key2 is longer
        std::cmp::Ordering::Equal => PathKeysComparison::Equal,
    }
}

/// PG `pathkeys_contained_in`: true iff keys2 are at least as well sorted as
/// keys1 (keys1 is a prefix of, or equal to, keys2).
#[must_use]
pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    matches!(
        compare_pathkeys(keys1, keys2),
        PathKeysComparison::Equal | PathKeysComparison::Better2
    )
}

/// PG `pathkeys_count_contained_in`: same as pathkeys_contained_in, but also
/// returns the length of the longest common prefix. Out-param `n_common` folded
/// into the returned tuple (bool, n_common).
#[must_use]
pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, i32) {
    if keys1.is_empty() {
        return (true, 0);
    }
    if keys2.is_empty() {
        return (false, 0);
    }

    let mut n = 0;
    for (pathkey1, pathkey2) in keys1.iter().zip(keys2.iter()) {
        if pathkey1 != pathkey2 {
            return (false, n);
        }
        n += 1;
    }

    // We've processed the whole shorter list; keys1 is contained iff it ended.
    (keys1.len() <= keys2.len(), n)
}

/// PG `get_cheapest_path_for_pathkeys`: cheapest path satisfying the pathkeys
/// and parameterization (parallel-safe if required), else None.
#[must_use]
pub fn get_cheapest_path_for_pathkeys(
    paths: &[Path],
    pathkeys: &[PathKey],
    required_outer: Relids,
    cost_criterion: CostSelector,
    require_parallel_safe: bool,
) -> Option<Path> {
    let mut matched_path: Option<&Path> = None;

    for path in paths {
        if require_parallel_safe && !path.parallel_safe {
            continue;
        }

        // Cost comparison is cheaper than pathkey comparison, so do it first.
        if matched_path.is_some_and(|m| compare_path_costs(m, path, cost_criterion) <= 0) {
            continue;
        }

        if pathkeys_contained_in(pathkeys, &deref_pathkeys(&path.pathkeys))
            && bms_is_subset(&path_req_outer(path), &required_outer)
        {
            matched_path = Some(path);
        }
    }
    matched_path.cloned()
}

/// PG `get_cheapest_fractional_path_for_pathkeys`: cheapest path for retrieving
/// `fraction` of the tuples that satisfies the pathkeys and parameterization.
#[must_use]
pub fn get_cheapest_fractional_path_for_pathkeys(
    paths: &[Path],
    pathkeys: &[PathKey],
    required_outer: Relids,
    fraction: f64,
) -> Option<Path> {
    let mut matched_path: Option<&Path> = None;

    for path in paths {
        if matched_path.is_some_and(|m| compare_fractional_path_costs(m, path, fraction) <= 0) {
            continue;
        }

        if pathkeys_contained_in(pathkeys, &deref_pathkeys(&path.pathkeys))
            && bms_is_subset(&path_req_outer(path), &required_outer)
        {
            matched_path = Some(path);
        }
    }
    matched_path.cloned()
}

/// PG `get_cheapest_parallel_safe_total_inner`: first unparameterized
/// parallel-safe path (the list is total-cost ordered, so first wins).
#[must_use]
pub fn get_cheapest_parallel_safe_total_inner(paths: &[Path]) -> Option<Path> {
    paths
        .iter()
        .find(|p| p.parallel_safe && path_req_outer(p).is_empty())
        .cloned()
}

/// PG `get_useful_group_keys_orderings`: which orderings of GROUP BY keys are
/// interesting. STAGED -- GROUP BY reordering with incremental sort is not in
/// the INNER-JOIN + ORDER BY path; the reachable reorder helper
/// (group_keys_reorder_by_pathkeys) and the `GroupByOrdering` plumbing are
/// deferred. Returns an empty list for now.
#[must_use]
pub fn get_useful_group_keys_orderings(_root: &mut PlannerInfo, _path: &Path) -> Vec<Node> {
    Vec::new()
}

/****************************************************************************
 *		NEW PATHKEY FORMATION
 ****************************************************************************/

/// PG `build_index_pathkeys`: pathkeys describing the ordering induced by an
/// index scan. An unordered index induces no ordering (returns empty). For a
/// backward scan, sort direction and nulls-first are inverted per column. The
/// result is canonical (redundant pathkeys removed) and may be shorter than the
/// key-column count; caller should also call truncate_useless_pathkeys.
#[must_use]
pub fn build_index_pathkeys(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    scandir: ScanDirection,
) -> Vec<PathKey> {
    let mut retval: Vec<PathKey> = Vec::new();

    if index.sortopfamily.is_empty() {
        return retval; // non-orderable index
    }

    let backward = scan_direction_is_backward(scandir);
    let index_relids = index
        .rel
        .as_ref()
        .and_then(|r| r.relids.clone())
        .unwrap_or_default();

    for (i, indextle) in index.indextlist.iter().enumerate() {
        // INCLUDE (non-key) columns are stored unordered.
        if i >= index.nkeycolumns as usize {
            break;
        }

        // The tlist item is a TargetEntry; its expr is the index key.
        let Node::TargetEntry(tle) = indextle else {
            break;
        };
        let Some(indexkey) = tle.expr.as_ref() else {
            break;
        };

        let (reverse_sort, nulls_first) = if backward {
            (!index.reverse_sort[i], !index.nulls_first[i])
        } else {
            (index.reverse_sort[i], index.nulls_first[i])
        };

        let cpathkey = make_pathkey_from_sortinfo(
            root,
            indexkey,
            index.sortopfamily[i],
            index.opcintype[i],
            index.indexcollations[i],
            reverse_sort,
            nulls_first,
            0,
            index_relids.clone(),
            false,
        );

        if let Some(cpathkey) = cpathkey {
            if !pathkey_is_redundant(&cpathkey, &retval) {
                retval.push(cpathkey);
            }
        } else {
            // Boolean index keys can be redundant without an EC; otherwise the
            // sort key is uninteresting and any lower columns are too.
            if !indexcol_is_bool_constant_for_query(root, index, i) {
                break;
            }
        }
    }

    retval
}

/// PG `build_partition_pathkeys`: pathkeys induced by the partitions of
/// `partrel`. STAGED -- partitionwise ordering is not part of the INNER-JOIN +
/// ORDER BY path (it reaches the partition-scheme machinery and
/// partkey_is_bool_constant_for_query). Returns (empty, partialkeys=true) to
/// signal "no usable partition ordering". Out-param `partialkeys` folded into
/// the returned tuple.
#[must_use]
pub fn build_partition_pathkeys(
    _root: &mut PlannerInfo,
    _partrel: &RelOptInfo,
    _scandir: ScanDirection,
) -> (Vec<PathKey>, bool) {
    (Vec::new(), true)
}

/// PG `build_expression_pathkey`: pathkeys for an ordering by a single
/// expression using the given sort operator. Empty if `create_it` is false and
/// the expression isn't already in some EquivalenceClass.
#[must_use]
pub fn build_expression_pathkey(
    root: &mut PlannerInfo,
    expr: &Expr,
    opno: Oid,
    rel: Relids,
    create_it: bool,
) -> Vec<PathKey> {
    let Some((opfamily, opcintype, cmptype)) = get_ordering_op_properties(opno) else {
        crate::elog!(
            crate::utils::elog::ERROR,
            "operator is not a valid ordering operator"
        );
        unreachable!("elog(ERROR) diverges");
    };

    let reverse = cmptype == CompareType::Gt;
    let cpathkey = make_pathkey_from_sortinfo(
        root,
        expr,
        opfamily,
        opcintype,
        exprCollation(expr),
        reverse,
        reverse,
        0,
        rel,
        create_it,
    );

    cpathkey.into_iter().collect()
}

/// PG `convert_subquery_pathkeys`: translate a subquery's output pathkeys into
/// outer-query terms. STAGED -- subquery scans are not in the INNER-JOIN +
/// ORDER BY path; this reaches find_var_for_subquery_tle and the outer/inner EC
/// matching scoring. Returns an empty list (no usable subquery ordering).
#[must_use]
pub fn convert_subquery_pathkeys(
    _root: &mut PlannerInfo,
    _rel: &RelOptInfo,
    _subquery_pathkeys: &[PathKey],
    _subquery_tlist: &[TargetEntry],
) -> Vec<PathKey> {
    Vec::new()
}

/// PG `build_join_pathkeys`: pathkeys for a join relation built by mergejoin or
/// nestloop. Normally the outer path's keys carry through; FULL/RIGHT/RIGHT_ANTI
/// joins yield no order (null lefthand rows inserted at random points). We
/// truncate keys uninteresting to higher joins.
#[must_use]
pub fn build_join_pathkeys(
    root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    jointype: JoinType,
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    crate::assert!(jointype != JoinType::RIGHT_SEMI);

    if matches!(
        jointype,
        JoinType::FULL | JoinType::RIGHT | JoinType::RIGHT_ANTI
    ) {
        return Vec::new();
    }

    // Pathkeys are canonical by construction; just truncate to what's useful.
    truncate_useless_pathkeys(root, joinrel, outer_pathkeys)
}

/****************************************************************************
 *		PATHKEYS AND SORT CLAUSES
 ****************************************************************************/

/// PG `make_pathkeys_for_sortclauses`: pathkeys representing the sort order of a
/// list of SortGroupClauses. The result is always canonical. Caller error if
/// not all clauses are sortable.
#[must_use]
pub fn make_pathkeys_for_sortclauses(
    root: &mut PlannerInfo,
    sortclauses: &[SortGroupClause],
    tlist: &[TargetEntry],
) -> Vec<PathKey> {
    let mut sortclauses = sortclauses.to_vec();
    let (result, sortable) =
        make_pathkeys_for_sortclauses_extended(root, &mut sortclauses, tlist, false, false, false);
    crate::assert!(sortable);
    result
}

/// PG `make_pathkeys_for_sortclauses_extended`: see make_pathkeys_for_sortclauses.
/// If `remove_redundant`, sort clauses giving rise to redundant pathkeys are
/// removed from `sortclauses` (pass-by-reference). If `remove_group_rtindex`,
/// strip the grouping step's RT index from sort expressions first. `set_ec_sortref`
/// copies a clause's sortref into its EC when not yet set. In-out `sortclauses`
/// and out `sortable` folded into the returned tuple (Vec<PathKey>, sortable).
#[must_use]
pub fn make_pathkeys_for_sortclauses_extended(
    root: &mut PlannerInfo,
    sortclauses: &mut Vec<SortGroupClause>,
    tlist: &[TargetEntry],
    remove_redundant: bool,
    remove_group_rtindex: bool,
    set_ec_sortref: bool,
) -> (Vec<PathKey>, bool) {
    // remove_group_rtindex would strip the GROUP RTE from each sortkey via
    // remove_nulling_relids; that path (grouping over a GROUP RTE) is staged and
    // the helper is a stub, so we use the sortkey as-is.
    let _ = remove_group_rtindex;

    let mut pathkeys: Vec<PathKey> = Vec::new();
    let mut sortable = true;
    let mut redundant_indexes: Vec<usize> = Vec::new();

    for (idx, sortcl) in sortclauses.iter().enumerate() {
        let Some(sortkey) = get_sortgroupclause_expr(sortcl, tlist) else {
            crate::elog!(crate::utils::elog::ERROR, "ORDER BY clause not in targetlist");
            unreachable!("elog(ERROR) diverges");
        };
        if sortcl.sortop == InvalidOid {
            sortable = false;
            continue;
        }

        let Some(mut pathkey) = make_pathkey_from_sortop(
            root,
            &sortkey,
            sortcl.sortop,
            sortcl.reverse_sort,
            sortcl.nulls_first,
            sortcl.tleSortGroupRef,
            true,
        ) else {
            // create_it=true always yields a PathKey; defensive only.
            sortable = false;
            continue;
        };

        if pathkey.eclass.sortref == 0 && set_ec_sortref {
            pathkey.eclass.sortref = sortcl.tleSortGroupRef;
        }

        // Canonical form eliminates redundant ordering keys.
        if pathkey_is_redundant(&pathkey, &pathkeys) {
            redundant_indexes.push(idx);
        } else {
            pathkeys.push(pathkey);
        }
    }

    if remove_redundant && !redundant_indexes.is_empty() {
        let mut idx = 0;
        sortclauses.retain(|_| {
            let keep = !redundant_indexes.contains(&idx);
            idx += 1;
            keep
        });
    }

    (pathkeys, sortable)
}

/****************************************************************************
 *		PATHKEYS AND MERGECLAUSES
 ****************************************************************************/

/// PG `initialize_mergeclause_eclasses`: set the left_ec/right_ec links on a
/// mergeclause RestrictInfo from its mergeopfamilies, creating ECs if needed.
/// Called before EC merging completes, so update_mergeclause_eclasses must be
/// called before the links are used.
pub fn initialize_mergeclause_eclasses(root: &mut PlannerInfo, restrictinfo: &mut RestrictInfo) {
    crate::assert!(!restrictinfo.mergeopfamilies.is_empty());
    crate::assert!(restrictinfo.left_ec.is_none());
    crate::assert!(restrictinfo.right_ec.is_none());

    let clause = restrictinfo.clause.clone();
    let opno = match &clause {
        Node::OpExpr(e) => e.opno,
        _ => InvalidOid,
    };
    let inputcollid = match &clause {
        Node::OpExpr(e) => e.inputcollid,
        _ => InvalidOid,
    };
    let (lefttype, righttype) = op_input_types(opno);

    let opfamilies = restrictinfo.mergeopfamilies.clone();
    let leftop = get_leftop(&clause).cloned();
    let rightop = get_rightop(&clause).cloned();

    if let Some(leftop) = leftop {
        restrictinfo.left_ec = get_eclass_for_sort_expr(
            root,
            &leftop,
            &opfamilies,
            lefttype,
            inputcollid,
            0,
            Relids::default(),
            true,
        )
        .map(Box::new);
    }
    if let Some(rightop) = rightop {
        restrictinfo.right_ec = get_eclass_for_sort_expr(
            root,
            &rightop,
            &opfamilies,
            righttype,
            inputcollid,
            0,
            Relids::default(),
            true,
        )
        .map(Box::new);
    }
}

/// PG `update_mergeclause_eclasses`: chase the cached EC links up to their
/// canonical merged parents.
pub fn update_mergeclause_eclasses(_root: &mut PlannerInfo, restrictinfo: &mut RestrictInfo) {
    crate::assert!(!restrictinfo.mergeopfamilies.is_empty());
    crate::assert!(restrictinfo.left_ec.is_some());
    crate::assert!(restrictinfo.right_ec.is_some());

    while let Some(merged) = restrictinfo.left_ec.as_ref().and_then(|ec| ec.merged.clone()) {
        restrictinfo.left_ec = Some(merged);
    }
    while let Some(merged) = restrictinfo.right_ec.as_ref().and_then(|ec| ec.merged.clone()) {
        restrictinfo.right_ec = Some(merged);
    }
}

/// PG `find_mergeclauses_for_outer_pathkeys`: find a maximal list of
/// mergeclauses usable with the given outer-rel ordering, ordered to match the
/// pathkeys. The restrictinfos must be marked (via `outer_is_left`). Empty if no
/// merge can be done.
#[must_use]
pub fn find_mergeclauses_for_outer_pathkeys(
    root: &mut PlannerInfo,
    pathkeys: &[PathKey],
    restrictinfos: &[RestrictInfo],
) -> Vec<RestrictInfo> {
    // Make sure we have eclasses cached in the clauses.
    let mut rinfos = restrictinfos.to_vec();
    for rinfo in &mut rinfos {
        update_mergeclause_eclasses(root, rinfo);
    }

    let mut mergeclauses: Vec<RestrictInfo> = Vec::new();

    for pathkey in pathkeys {
        let pathkey_ec = &pathkey.eclass;
        let mut matched: Vec<RestrictInfo> = Vec::new();

        // A mergejoin clause matches a pathkey if it has the same (outer) EC.
        // Take all matches (multiple are possible in outer-join scenarios).
        for rinfo in &rinfos {
            let clause_ec = if rinfo.outer_is_left {
                rinfo.left_ec.as_ref()
            } else {
                rinfo.right_ec.as_ref()
            };
            if clause_ec.is_some_and(|ec| **ec == **pathkey_ec) {
                matched.push(rinfo.clone());
            }
        }

        // No mergeclause for this position => any further keys are useless.
        if matched.is_empty() {
            break;
        }
        mergeclauses.extend(matched);
    }

    mergeclauses
}

/// PG `select_outer_pathkeys_for_merge`: build a possible outer-relation sort
/// ordering usable with the given mergeclauses. Prefers matching the requested
/// query_pathkeys (to avoid a final sort / enable incremental sort), then lists
/// "more popular" ECs (most unmatched EC peers) first.
#[must_use]
pub fn select_outer_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    joinrel: &RelOptInfo,
) -> Vec<PathKey> {
    let n_clauses = mergeclauses.len();
    if n_clauses == 0 {
        return Vec::new();
    }

    let joinrel_relids = joinrel.relids.clone().unwrap_or_default();

    // Arrays of the distinct ECs used by the mergeclauses and their scores.
    let mut ecs: Vec<EquivalenceClass> = Vec::with_capacity(n_clauses);
    let mut scores: Vec<i32> = Vec::with_capacity(n_clauses);

    let mut rinfos = mergeclauses.to_vec();
    for rinfo in &mut rinfos {
        update_mergeclause_eclasses(root, rinfo);

        let oeclass = if rinfo.outer_is_left {
            rinfo.left_ec.clone()
        } else {
            rinfo.right_ec.clone()
        };
        let Some(oeclass) = oeclass else { continue };
        let oeclass = *oeclass;

        // Reject duplicate ECs.
        if ecs.contains(&oeclass) {
            continue;
        }

        // Score = # of EC members that are future join partners.
        let mut score = 0;
        for em in &oeclass.members {
            crate::assert!(!em.is_child);
            let overlaps = em
                .relids
                .as_ref()
                .is_some_and(|r| bms_overlap(r, &joinrel_relids));
            if !em.is_const && !overlaps {
                score += 1;
            }
        }

        ecs.push(oeclass);
        scores.push(score);
    }

    let necs = ecs.len();
    let mut pathkeys: Vec<PathKey> = Vec::new();

    // Do we have all the ECs in query_pathkeys? If so we can emit an order
    // useful for final output. A query_pathkeys prefix covering the whole join
    // condition is also useful (enables incremental sort upstream).
    if !root.query_pathkeys.is_empty() {
        let query_pathkeys = root.query_pathkeys.clone();
        let mut matches = 0;
        let mut all = true;
        for query_pathkey in &query_pathkeys {
            let query_ec = &query_pathkey.eclass;
            if ecs.iter().any(|ec| *ec == **query_ec) {
                matches += 1;
            } else {
                all = false;
                break;
            }
        }

        if all {
            // Copy query_pathkeys as starting point; mark their ECs as emitted.
            pathkeys = query_pathkeys.iter().map(|pk| (**pk).clone()).collect();
            for query_pathkey in &query_pathkeys {
                let query_ec = &query_pathkey.eclass;
                if let Some(j) = ecs.iter().position(|ec| *ec == **query_ec) {
                    scores[j] = -1;
                }
            }
        } else if matches == n_clauses as i32 {
            // Matched all join clauses (but not all query_pathkeys): use prefix.
            return query_pathkeys
                .iter()
                .take(matches as usize)
                .map(|pk| (**pk).clone())
                .collect();
        }
    }

    // Add remaining ECs in popularity (score) order with default sort ordering.
    loop {
        // Pick the highest-scoring remaining EC (ties resolve to the first, as
        // PG's linear scan does -- so fold keeping the first strict maximum).
        let best = scores.iter().enumerate().take(necs).fold(
            None::<(usize, i32)>,
            |acc, (j, &s)| match acc {
                Some((_, best)) if s > best => Some((j, s)),
                Some(_) => acc,
                None => Some((j, s)),
            },
        );
        let Some((best_j, best_score)) = best else {
            break;
        };
        if best_score < 0 {
            break;
        }
        scores[best_j] = -1;
        let ec = ecs[best_j].clone();
        let opfamily = ec.opfamilies.first().copied().unwrap_or(InvalidOid);
        let pathkey = make_canonical_pathkey(root, &ec, opfamily, CompareType::Lt, false);
        crate::assert!(!pathkey_is_redundant(&pathkey, &pathkeys));
        pathkeys.push(pathkey);
    }

    pathkeys
}

/// PG `make_inner_pathkeys_for_merge`: the explicit sort order to apply to an
/// inner path for the given mergeclauses. `outer_pathkeys` are the known
/// canonical outer-side pathkeys. The restrictinfos must be marked (via
/// `outer_is_left`).
#[must_use]
pub fn make_inner_pathkeys_for_merge(
    root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    outer_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let mut pathkeys: Vec<PathKey> = Vec::new();
    let mut lastoeclass: Option<EquivalenceClass> = None;
    let mut opathkey: Option<PathKey> = None;
    let mut lop = 0usize;

    let mut rinfos = mergeclauses.to_vec();
    for rinfo in &mut rinfos {
        update_mergeclause_eclasses(root, rinfo);

        let (oeclass, ieclass) = if rinfo.outer_is_left {
            (rinfo.left_ec.clone(), rinfo.right_ec.clone())
        } else {
            (rinfo.right_ec.clone(), rinfo.left_ec.clone())
        };
        let Some(oeclass) = oeclass.map(|b| *b) else { continue };
        let Some(ieclass) = ieclass.map(|b| *b) else { continue };

        // Outer eclass should match the current or next outer pathkey.
        if lastoeclass.as_ref() != Some(&oeclass) {
            if lop >= outer_pathkeys.len() {
                crate::elog!(crate::utils::elog::ERROR, "too few pathkeys for mergeclauses");
            }
            let opk = outer_pathkeys[lop].clone();
            lop += 1;
            lastoeclass = Some((*opk.eclass).clone());
            if lastoeclass.as_ref() != Some(&oeclass) {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "outer pathkeys do not match mergeclause"
                );
            }
            opathkey = Some(opk);
        }

        // opathkey is always set on the first iteration (lastoeclass starts None
        // -> the advance branch runs) and stays set thereafter.
        let opk = opathkey
            .clone()
            .unwrap_or_error_with(|| "mergeclause without an outer pathkey".to_owned());

        // Same EC on both sides => outer pathkey is canonical for inner too.
        let pathkey = if ieclass == oeclass {
            opk
        } else {
            make_canonical_pathkey(root, &ieclass, opk.opfamily, opk.cmptype, opk.nulls_first)
        };

        if !pathkey_is_redundant(&pathkey, &pathkeys) {
            pathkeys.push(pathkey);
        }
    }

    pathkeys
}

/// PG `trim_mergeclauses_for_inner_pathkeys`: trim a mergeclause list to those
/// working with a specified inner-rel ordering (a prefix of the given list).
/// Needed because make_inner_pathkeys_for_merge's result isn't necessarily in
/// mergeclause order. The mergeclauses must be marked (via `outer_is_left`).
#[must_use]
pub fn trim_mergeclauses_for_inner_pathkeys(
    _root: &mut PlannerInfo,
    mergeclauses: &[RestrictInfo],
    pathkeys: &[PathKey],
) -> Vec<RestrictInfo> {
    if pathkeys.is_empty() {
        return Vec::new();
    }

    let mut new_mergeclauses: Vec<RestrictInfo> = Vec::new();
    let mut lip = 1usize; // next pathkey to consider
    let mut pathkey_ec = (*pathkeys[0].eclass).clone();
    let mut matched_pathkey = false;

    for rinfo in mergeclauses {
        // (update_mergeclause_eclasses already done by caller.)
        let clause_ec = if rinfo.outer_is_left {
            rinfo.right_ec.as_ref()
        } else {
            rinfo.left_ec.as_ref()
        };
        let Some(clause_ec) = clause_ec.map(|b| (**b).clone()) else {
            break;
        };

        // If no match, try to advance to the next inner pathkey.
        if clause_ec != pathkey_ec {
            if !matched_pathkey || lip >= pathkeys.len() {
                break;
            }
            pathkey_ec = (*pathkeys[lip].eclass).clone();
            lip += 1;
            // PG resets matched_pathkey=false here; in this control flow the
            // following test always re-sets it (match -> true) or breaks, so the
            // reset is never observed. Omitted to avoid a dead-store.
        }

        if clause_ec == pathkey_ec {
            new_mergeclauses.push(rinfo.clone());
            matched_pathkey = true;
        } else {
            break;
        }
    }

    new_mergeclauses
}

/****************************************************************************
 *		PATHKEY USEFULNESS CHECKS
 ****************************************************************************/

/// PG `pathkeys_useful_for_merging`: count pathkeys potentially useful for
/// mergejoins above `rel` -- those matching the merge ordering of either side
/// of any joinclause for the rel. Stops at the first "wrong" sort direction or
/// the first non-matching key.
fn pathkeys_useful_for_merging(root: &mut PlannerInfo, rel: &RelOptInfo, pathkeys: &[PathKey]) -> i32 {
    let mut useful = 0;

    for pathkey in pathkeys {
        if !right_merge_direction(root, pathkey) {
            break;
        }

        // PG first checks the EC for not-yet-joined members
        // (eclass_useful_for_merging); failing that, it searches the rel's
        // joininfo list for a mergejoinable clause with a matching side EC.
        let matched = if rel.has_eclass_joins && eclass_useful_for_merging(root, &pathkey.eclass, rel)
        {
            true
        } else {
            let mut joininfo = rel.joininfo.clone();
            joininfo.iter_mut().any(|restrictinfo| {
                if restrictinfo.mergeopfamilies.is_empty() {
                    return false;
                }
                update_mergeclause_eclasses(root, restrictinfo);
                let target = &*pathkey.eclass;
                restrictinfo.left_ec.as_deref() == Some(target)
                    || restrictinfo.right_ec.as_deref() == Some(target)
            })
        };

        if matched {
            useful += 1;
        } else {
            break;
        }
    }

    useful
}

/// PG `right_merge_direction`: does the pathkey embody the preferred sort
/// direction for merging its column? Prefer the direction matching an ORDER BY
/// key if present; else prefer ASC.
fn right_merge_direction(root: &PlannerInfo, pathkey: &PathKey) -> bool {
    for query_pathkey in &root.query_pathkeys {
        if *pathkey.eclass == *query_pathkey.eclass && pathkey.opfamily == query_pathkey.opfamily {
            return pathkey.cmptype == query_pathkey.cmptype;
        }
    }
    pathkey.cmptype == CompareType::Lt
}

/// PG `pathkeys_useful_for_ordering`: count leading pathkeys shared with the
/// requested query output ordering (a prefix is useful via incremental sort).
fn pathkeys_useful_for_ordering(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    let query_pathkeys = deref_pathkeys(&root.query_pathkeys);
    let (_, n_common) = pathkeys_count_contained_in(&query_pathkeys, pathkeys);
    n_common
}

/// PG `pathkeys_useful_for_grouping`: count the leading pathkeys with a matching
/// group key.
fn pathkeys_useful_for_grouping(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    if root.group_pathkeys.is_empty() {
        return 0;
    }
    let mut n = 0;
    for pathkey in pathkeys {
        if !root.group_pathkeys.iter().any(|gp| **gp == *pathkey) {
            break;
        }
        n += 1;
    }
    n
}

/// PG `pathkeys_useful_for_distinct`: count the leading pathkeys shared with the
/// DISTINCT pathkeys.
fn pathkeys_useful_for_distinct(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    if root.distinct_pathkeys.is_empty() {
        return 0;
    }
    let mut n = 0;
    for pathkey in pathkeys {
        if !root.distinct_pathkeys.iter().any(|dp| **dp == *pathkey) {
            break;
        }
        n += 1;
    }
    n
}

/// PG `pathkeys_useful_for_setop`: count leading pathkeys shared with the setop
/// pathkeys.
fn pathkeys_useful_for_setop(root: &PlannerInfo, pathkeys: &[PathKey]) -> i32 {
    let setop_pathkeys = deref_pathkeys(&root.setop_pathkeys);
    let (_, n_common) = pathkeys_count_contained_in(&setop_pathkeys, pathkeys);
    n_common
}

/// PG `truncate_useless_pathkeys`: shorten a pathkey list to just the useful
/// prefix (max usefulness across merging/ordering/grouping/distinct/setop).
#[must_use]
pub fn truncate_useless_pathkeys(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let mut nuseful = pathkeys_useful_for_merging(root, rel, pathkeys);
    nuseful = nuseful.max(pathkeys_useful_for_ordering(root, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_grouping(root, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_distinct(root, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_setop(root, pathkeys));

    if nuseful == 0 {
        Vec::new()
    } else if nuseful as usize >= pathkeys.len() {
        pathkeys.to_vec()
    } else {
        pathkeys[..nuseful as usize].to_vec()
    }
}

/// PG `has_useful_pathkeys`: cheap test for whether `rel` could have any useful
/// pathkeys. OK to over-report "true"; must never under-report.
#[must_use]
pub fn has_useful_pathkeys(root: &mut PlannerInfo, rel: &RelOptInfo) -> bool {
    if !rel.joininfo.is_empty() || rel.has_eclass_joins {
        return true; // might use pathkeys for merging
    }
    if !root.group_pathkeys.is_empty() {
        return true; // grouping
    }
    if !root.query_pathkeys.is_empty() {
        return true; // ordering
    }
    false
}

/****************************************************************************
 *		HELPERS
 ****************************************************************************/

/// PG `PathKeysComparison` (declared in paths.h). Result of compare_pathkeys.
pub use crate::optimizer::paths::PathKeysComparison;

/// PG `PATH_REQ_OUTER`: the rels a path is parameterized by (empty if not).
fn path_req_outer(path: &Path) -> Relids {
    path.param_info
        .as_ref()
        .and_then(|pi| pi.req_outer.clone())
        .unwrap_or_default()
}

/// Borrow a `Vec<Box<PathKey>>` (root/path storage) as a `Vec<PathKey>` for the
/// value-comparison APIs. The clone is cheap relative to pathkey comparison and
/// keeps the public signatures on `&[PathKey]` per the header stubs.
fn deref_pathkeys(pks: &[Box<PathKey>]) -> Vec<PathKey> {
    pks.iter().map(|b| (**b).clone()).collect()
}

// equivclass.c helpers (translated concurrently; re-exported from paths.rs).
use crate::optimizer::paths::{eclass_useful_for_merging, get_eclass_for_sort_expr};
// pathnode.c cost comparisons.
use crate::optimizer::pathnode::{compare_fractional_path_costs, compare_path_costs};

/// PG `get_sortgroupclause_expr`: the tlist expression a SortGroupClause refs.
/// `get_sortgroupclause_expr` in optimizer.rs is a stub today, so we do the
/// simple lookup inline (find the TargetEntry whose ressortgroupref matches).
fn get_sortgroupclause_expr(sgc: &SortGroupClause, tlist: &[TargetEntry]) -> Option<Node> {
    tlist
        .iter()
        .find(|tle| tle.ressortgroupref != 0 && tle.ressortgroupref == sgc.tleSortGroupRef)
        .and_then(|tle| tle.expr.clone())
}

/// PG `indexcol_is_bool_constant_for_query` (indxpath.c). Staged: boolean index
/// columns implicitly constrained by the query are out of the INNER-JOIN + ORDER
/// BY scope; returning false means build_index_pathkeys stops at the first index
/// column lacking an EC, which is the conservative (correct-but-shorter) result.
fn indexcol_is_bool_constant_for_query(
    _root: &PlannerInfo,
    _index: &IndexOptInfo,
    _indexcol: usize,
) -> bool {
    false
}

#[cfg(test)]
mod tests;

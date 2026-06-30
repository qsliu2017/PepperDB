//! joininfo list manipulation routines. Translated from
//! backend/optimizer/util/joininfo.c.
//!
//! Three free functions that maintain each base rel's `joininfo` list (the
//! RestrictInfos for join clauses that mention the rel) and answer whether a
//! join clause relates two rels.
//!
//! Representation note (the `find_base_rel` owned-clone convention, see
//! relnode.rs): `simple_rel_array` holds `Option<Box<RelOptInfo>>`, and
//! `find_base_rel_ignore_join` returns an owned *clone* of the parked rel, so
//! mutating it would not flow back. `add_/remove_join_clause_from_rels` must
//! therefore append to / remove from `root.simple_rel_array[relid]` in place.
//! We index `simple_rel_array` directly (entry `None` = relid is not a base
//! rel, i.e. an outer-join relid, which PG's `find_base_rel_ignore_join` reports
//! by returning NULL -- here it would panic, so we never call it from these two
//! mutators).

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: join_relids is an owned Relids matching the C signature"
)]

use crate::nodes::bitmapset::{bms_next_member, bms_overlap};
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo, Relids, RestrictInfo};

/// Panic for a joininfo path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `have_relevant_joinclause`: detect whether some join clause involves both
/// `rel1` and `rel2`.
///
/// The clause need not be evaluable with only these two rels (e.g.
/// `a.x = (b.y + c.z)` is a relevant reason to join b and c). We scan the
/// shorter of the two joininfo lists for a RestrictInfo whose `required_relids`
/// overlaps the other rel's relids, then fall back to the EquivalenceClass data
/// (which can hold join relationships not emitted into the joininfo lists).
pub fn have_relevant_joinclause(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    // Scan whichever joininfo list is shorter against the other rel's relids.
    let (joininfo, other_relids) = if rel1.joininfo.len() <= rel2.joininfo.len() {
        (&rel1.joininfo, &rel2.relids)
    } else {
        (&rel2.joininfo, &rel1.relids)
    };

    let mut result = other_relids.as_ref().is_some_and(|other_relids| {
        joininfo.iter().any(|rinfo| {
            rinfo
                .required_relids
                .as_ref()
                .is_some_and(|req| bms_overlap(other_relids, req))
        })
    });

    // EquivalenceClass joins may relate the rels without a joininfo entry.
    if !result && rel1.has_eclass_joins && rel2.has_eclass_joins {
        result = crate::optimizer::paths::have_relevant_eclass_joinclause(root, rel1, rel2);
    }

    result
}

/// PG `add_join_clause_to_rels`: add `restrictinfo` to the joininfo list of each
/// base rel in `join_relids`.
///
/// `join_relids` is the set of relations participating in the join clause (some
/// may be outer-join relids, which are not base rels and are skipped). The same
/// RestrictInfo value is appended to every list it belongs to.
///
/// Write-back: `find_base_rel_ignore_join` returns an owned clone, so we mutate
/// `root.simple_rel_array[relid]` directly. A `None` slot means the relid is not
/// a base rel (an outer-join relid) and is skipped -- PG's
/// `find_base_rel_ignore_join` returns NULL for that case.
pub fn add_join_clause_to_rels(
    root: &mut PlannerInfo,
    restrictinfo: &RestrictInfo,
    join_relids: Relids,
) {
    // TODO(step 31): call restriction_is_always_true/false (restrictinfo.c,
    // selfuncs-adjacent). For M7 an ordinary join clause `a.x = b.y` is neither
    // provably always-true (no early return) nor always-false (no constant-FALSE
    // substitution), so both are treated as false here.
    if restriction_is_always_true(root, restrictinfo) {
        return;
    }
    if restriction_is_always_false(root, restrictinfo) {
        // Substitute the qual with constant-FALSE, preserving rinfo_serial /
        // last_rinfo_serial. Not reached for M7 join clauses.
        not_yet_reachable("add_join_clause_to_rels: always-false constant-FALSE substitution");
    }

    let mut cur_relid = -1;
    while let Some(relid) = bms_next_member(&join_relids, cur_relid) {
        cur_relid = relid;
        // Only baserels get the clause; a None slot is an outer-join relid.
        if let Some(rel) = root.simple_rel_array[relid as usize].as_mut() {
            rel.joininfo.push(Box::new(restrictinfo.clone()));
        }
    }
}

/// PG `remove_join_clause_from_rels`: delete `restrictinfo` from every joininfo
/// list it is in, reversing `add_join_clause_to_rels`. Used when a relation
/// turns out not to need joining.
///
/// PG compares by pointer (the same node is shared across lists) and asserts the
/// clause is present. Here RestrictInfos are cloned per list, so we match by
/// `rinfo_serial` (unique within the PlannerInfo context). Same write-back rule
/// as `add_join_clause_to_rels`.
pub fn remove_join_clause_from_rels(
    root: &mut PlannerInfo,
    restrictinfo: &RestrictInfo,
    join_relids: Relids,
) {
    let serial = restrictinfo.rinfo_serial;
    let mut cur_relid = -1;
    while let Some(relid) = bms_next_member(&join_relids, cur_relid) {
        cur_relid = relid;
        // We would only have added the clause to baserels.
        if let Some(rel) = root.simple_rel_array[relid as usize].as_mut() {
            crate::assert!(rel.joininfo.iter().any(|ri| ri.rinfo_serial == serial));
            rel.joininfo.retain(|ri| ri.rinfo_serial != serial);
        }
    }
}

/// PG `restriction_is_always_true` (planmain.c / restrictinfo.c). Deferred to
/// step 31; an ordinary M7 join clause is never provably always-true.
fn restriction_is_always_true(_root: &mut PlannerInfo, _restrictinfo: &RestrictInfo) -> bool {
    false
}

/// PG `restriction_is_always_false`. Deferred to step 31; an ordinary M7 join
/// clause is never provably always-false.
fn restriction_is_always_false(_root: &mut PlannerInfo, _restrictinfo: &RestrictInfo) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::{bms_make_singleton, bms_union};
    use crate::nodes::nodes::Node;
    use crate::nodes::pathnodes::{QualCost, RelOptKind, VolatileFunctionStatus};
    use crate::postgres_ext::InvalidOid;

    /// A base RelOptInfo for `relid` with the given joininfo list.
    fn base_rel(relid: i32) -> RelOptInfo {
        let mut rel = make_node_reloptinfo();
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel
    }

    /// A RestrictInfo whose `required_relids` is the given set; only the fields
    /// joininfo cares about are meaningful (clause is a dummy Const).
    fn join_rinfo(required: Relids, serial: i32) -> RestrictInfo {
        RestrictInfo {
            clause: Node::Const(Box::new(crate::nodes::primnodes::Const {
                consttype: crate::postgres_ext::Oid::new(16),
                consttypmod: -1,
                constcollid: InvalidOid,
                constlen: 1,
                constvalue: crate::postgres::Datum(0),
                constisnull: false,
                constbyval: true,
                location: -1,
            })),
            is_pushed_down: false,
            can_join: true,
            pseudoconstant: false,
            has_clone: false,
            is_clone: false,
            leakproof: false,
            has_volatile: VolatileFunctionStatus::UNKNOWN,
            security_level: 0,
            num_base_rels: 2,
            clause_relids: Some(required.clone()),
            required_relids: Some(required),
            incompatible_relids: None,
            outer_relids: None,
            left_relids: None,
            right_relids: None,
            orclause: None,
            rinfo_serial: serial,
            parent_ec: None,
            eval_cost: QualCost { startup: -1.0, per_tuple: -1.0 },
            norm_selec: -1.0,
            outer_selec: -1.0,
            mergeopfamilies: Vec::new(),
            left_ec: None,
            right_ec: None,
            left_em: None,
            right_em: None,
            scansel_cache: Vec::new(),
            outer_is_left: false,
            hashjoinoperator: InvalidOid,
            left_bucketsize: -1.0,
            right_bucketsize: -1.0,
            left_mcvfreq: -1.0,
            right_mcvfreq: -1.0,
            left_hasheqoperator: InvalidOid,
            right_hasheqoperator: InvalidOid,
        }
    }

    fn make_node_reloptinfo() -> RelOptInfo {
        RelOptInfo {
            reloptkind: RelOptKind::BASEREL,
            relids: None,
            rows: 0.0,
            consider_startup: false,
            consider_param_startup: false,
            consider_parallel: false,
            reltarget: None,
            pathlist: Vec::new(),
            ppilist: Vec::new(),
            partial_pathlist: Vec::new(),
            cheapest_startup_path: None,
            cheapest_total_path: None,
            cheapest_unique_path: None,
            cheapest_parameterized_paths: Vec::new(),
            direct_lateral_relids: None,
            lateral_relids: None,
            relid: 0,
            reltablespace: InvalidOid,
            rtekind: crate::nodes::parsenodes::RTEKind::RELATION,
            min_attr: 0,
            max_attr: 0,
            attr_needed: Vec::new(),
            attr_widths: Vec::new(),
            notnullattnums: None,
            nulling_relids: None,
            lateral_vars: Vec::new(),
            lateral_referencers: None,
            indexlist: Vec::new(),
            statlist: Vec::new(),
            pages: 0,
            tuples: 0.0,
            allvisfrac: 0.0,
            eclass_indexes: None,
            subroot: None,
            subplan_params: Vec::new(),
            rel_parallel_workers: -1,
            amflags: crate::nodes::pathnodes::AmFlags::empty(),
            serverid: InvalidOid,
            userid: InvalidOid,
            useridiscurrent: false,
            unique_for_rels: Vec::new(),
            non_unique_for_rels: Vec::new(),
            baserestrictinfo: Vec::new(),
            baserestrictcost: QualCost { startup: 0.0, per_tuple: 0.0 },
            baserestrict_min_security: usize::MAX,
            joininfo: Vec::new(),
            has_eclass_joins: false,
            consider_partitionwise_join: false,
            parent: None,
            top_parent: None,
            top_parent_relids: None,
            part_scheme: None,
            nparts: -1,
            partbounds_merged: false,
            partition_qual: Vec::new(),
            part_rels: Vec::new(),
            live_parts: None,
            all_partrels: None,
            partexprs: Vec::new(),
            nullable_partexprs: Vec::new(),
        }
    }

    /// PlannerInfo with two base rels parked at simple_rel_array[1] and [2].
    /// Only simple_rel_array (and the eclass-join fallback inputs) are read by
    /// the joininfo routines; every other field is left in its zero/empty state.
    fn planner_with_two_base_rels() -> PlannerInfo {
        let mut root = test_planner_info();
        root.simple_rel_array =
            vec![None, Some(Box::new(base_rel(1))), Some(Box::new(base_rel(2)))];
        root.simple_rte_array = vec![None, None, None];
        root
    }

    /// have_relevant_joinclause over two rels whose relids are {1} and {2},
    /// using only the joininfo overlap path (no eclass joins). The shorter list
    /// is scanned; a RestrictInfo required by {1,2} overlaps the other rel.
    #[test]
    fn have_relevant_joinclause_via_joininfo() {
        let mut root = planner_with_two_base_rels();
        let join_relids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        let mut rel1 = base_rel(1);
        let mut rel2 = base_rel(2);
        // The clause lives on both rels' joininfo (as add_join_clause_to_rels
        // maintains it), so the shorter list -- whichever -- finds the overlap.
        rel1.joininfo.push(Box::new(join_rinfo(join_relids.clone(), 7)));
        rel2.joininfo.push(Box::new(join_rinfo(join_relids, 7)));

        assert!(have_relevant_joinclause(&mut root, &rel1, &rel2));

        // Without the clause, no eclass joins -> not relevant.
        let rel1_empty = base_rel(1);
        assert!(!have_relevant_joinclause(&mut root, &rel1_empty, &rel2));
    }

    /// add_join_clause_to_rels appends to both baserels; remove_ pulls it back.
    #[test]
    fn add_then_remove_join_clause() {
        let mut root = planner_with_two_base_rels();
        let join_relids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        let rinfo = join_rinfo(join_relids.clone(), 11);

        add_join_clause_to_rels(&mut root, &rinfo, join_relids.clone());
        assert_eq!(root.simple_rel_array[1].as_ref().unwrap().joininfo.len(), 1);
        assert_eq!(root.simple_rel_array[2].as_ref().unwrap().joininfo.len(), 1);

        // have_relevant_joinclause now sees the clause on the parked rels.
        let rel1 = root.simple_rel_array[1].clone().unwrap();
        let rel2 = root.simple_rel_array[2].clone().unwrap();
        assert!(have_relevant_joinclause(&mut root, &rel1, &rel2));

        remove_join_clause_from_rels(&mut root, &rinfo, join_relids);
        assert!(root.simple_rel_array[1].as_ref().unwrap().joininfo.is_empty());
        assert!(root.simple_rel_array[2].as_ref().unwrap().joininfo.is_empty());
    }

    /// A bare PlannerInfo (every list/option empty); the joininfo routines read
    /// only simple_rel_array plus the two rels passed by reference.
    #[allow(
        clippy::too_many_lines,
        reason = "PlannerInfo/Query/PlannerGlobal are large palloc0 structs; the test constructor must fill every field"
    )]
    fn test_planner_info() -> PlannerInfo {
        use crate::nodes::nodes::{CmdType, LimitOption};
        use crate::nodes::parsenodes::{Query, QuerySource};
        use crate::nodes::pathnodes::PlannerGlobal;
        use crate::nodes::primnodes::OverridingKind;

        let parse = Query {
            commandType: CmdType::SELECT,
            querySource: QuerySource::ORIGINAL,
            queryId: 0,
            canSetTag: true,
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
            limitOption: LimitOption::COUNT,
            rowMarks: Vec::new(),
            setOperations: None,
            constraintDeps: Vec::new(),
            withCheckOptions: Vec::new(),
            stmt_location: -1,
            stmt_len: 0,
        };
        let glob = PlannerGlobal {
            subplans: Vec::new(),
            subpaths: Vec::new(),
            subroots: Vec::new(),
            rewind_plan_ids: None,
            finalrtable: Vec::new(),
            all_relids: None,
            prunable_relids: None,
            finalrteperminfos: Vec::new(),
            finalrowmarks: Vec::new(),
            result_relations: Vec::new(),
            append_relations: Vec::new(),
            part_prune_infos: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            last_phid: 0,
            last_row_mark_id: 0,
            last_plan_node_id: 0,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_ok: false,
            parallel_mode_needed: false,
            max_parallel_hazard: 0,
        };
        PlannerInfo {
            parse: Box::new(parse),
            glob: Box::new(glob),
            query_level: 1,
            parent_root: None,
            plan_params: Vec::new(),
            outer_params: None,
            simple_rel_array: Vec::new(),
            simple_rte_array: Vec::new(),
            append_rel_array: Vec::new(),
            all_baserels: None,
            outer_join_rels: None,
            all_query_rels: None,
            join_rel_list: Vec::new(),
            join_rel_level: Vec::new(),
            join_cur_level: 0,
            init_plans: Vec::new(),
            cte_plan_ids: Vec::new(),
            multiexpr_params: Vec::new(),
            join_domains: Vec::new(),
            eq_classes: Vec::new(),
            ec_merging_done: false,
            canon_pathkeys: Vec::new(),
            left_join_clauses: Vec::new(),
            right_join_clauses: Vec::new(),
            full_join_clauses: Vec::new(),
            join_info_list: Vec::new(),
            last_rinfo_serial: 0,
            all_result_relids: None,
            leaf_result_relids: None,
            append_rel_list: Vec::new(),
            row_identity_vars: Vec::new(),
            row_marks: Vec::new(),
            placeholder_list: Vec::new(),
            placeholder_array: Vec::new(),
            fkey_list: Vec::new(),
            query_pathkeys: Vec::new(),
            group_pathkeys: Vec::new(),
            num_groupby_pathkeys: 0,
            window_pathkeys: Vec::new(),
            distinct_pathkeys: Vec::new(),
            sort_pathkeys: Vec::new(),
            setop_pathkeys: Vec::new(),
            part_schemes: Vec::new(),
            initial_rels: Vec::new(),
            upper_rels: std::array::from_fn(|_| Vec::new()),
            upper_targets: std::array::from_fn(|_| None),
            processed_group_clause: Vec::new(),
            processed_distinct_clause: Vec::new(),
            processed_tlist: Vec::new(),
            scan_input_tlist: Vec::new(),
            update_colnos: Vec::new(),
            grouping_map: Vec::new(),
            minmax_aggs: Vec::new(),
            total_table_pages: 0.0,
            tuple_fraction: 0.0,
            limit_tuples: 0.0,
            qual_security_level: 0,
            has_join_rtes: false,
            has_lateral_rtes: false,
            has_having_qual: false,
            has_pseudo_constant_quals: false,
            has_alternative_subplans: false,
            placeholders_frozen: false,
            has_recursion: false,
            group_rtindex: 0,
            agginfos: Vec::new(),
            aggtransinfos: Vec::new(),
            num_ordered_aggs: 0,
            has_non_partial_aggs: false,
            has_non_serial_aggs: false,
            wt_param_id: -1,
            non_recursive_path: None,
            cur_outer_rels: None,
            cur_outer_params: Vec::new(),
            is_alt_subplan: Vec::new(),
            is_used_subplan: Vec::new(),
            part_cols_updated: false,
            part_prune_infos: Vec::new(),
        }
    }
}

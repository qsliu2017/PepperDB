#![allow(
    clippy::too_many_lines,
    reason = "the planner() helper is a flat PlannerInfo struct literal"
)]
#![allow(
    clippy::redundant_clone,
    reason = "test bodies clone PathKeys for readability across multiple assertions"
)]

use super::*;
use crate::nodes::nodes::{CmdType, LimitOption};
use crate::nodes::parsenodes::{Query, QuerySource};
use crate::nodes::pathnodes::{EquivalenceClass, JoinDomain, PlannerGlobal, PlannerInfo};
use crate::nodes::primnodes::OverridingKind;

const INT4OID: Oid = Oid(23);
const BTREE_INT4_OPF: Oid = Oid(1976);

/// Build a fresh empty EquivalenceClass with a single distinguishing relid set.
/// `key` makes the EC value-distinct (so two ECs with different keys compare
/// unequal), mimicking distinct real ECs without the full member machinery.
fn new_ec(key: i32) -> EquivalenceClass {
    EquivalenceClass {
        opfamilies: vec![BTREE_INT4_OPF],
        collation: InvalidOid,
        childmembers_size: 0,
        members: Vec::new(),
        childmembers: Vec::new(),
        sources: Vec::new(),
        derives_list: Vec::new(),
        relids: Some(crate::nodes::bitmapset::bms_make_singleton(key)),
        has_const: false,
        has_volatile: false,
        broken: false,
        sortref: key as usize,
        min_security: 0,
        max_security: 0,
        merged: None,
    }
}

fn planner() -> PlannerInfo {
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
        join_domains: vec![Box::new(JoinDomain { jd_relids: None })],
        eq_classes: Vec::new(),
        ec_merging_done: true,
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

#[test]
fn make_canonical_pathkey_dedups() {
    let mut root = planner();
    let ec = new_ec(1);

    let pk1 = make_canonical_pathkey(&mut root, &ec, BTREE_INT4_OPF, CompareType::Lt, false);
    let pk2 = make_canonical_pathkey(&mut root, &ec, BTREE_INT4_OPF, CompareType::Lt, false);

    // Second call must find the existing entry: no growth, equal returns.
    assert_eq!(root.canon_pathkeys.len(), 1);
    assert_eq!(pk1, pk2);
    assert_eq!(pk1.opfamily, BTREE_INT4_OPF);
    assert_eq!(pk1.cmptype, CompareType::Lt);
    assert!(!pk1.nulls_first);

    // A different direction is a distinct canonical pathkey.
    let pk3 = make_canonical_pathkey(&mut root, &ec, BTREE_INT4_OPF, CompareType::Gt, false);
    assert_eq!(root.canon_pathkeys.len(), 2);
    assert_ne!(pk1, pk3);
}

#[test]
fn pathkeys_contained_in_prefix() {
    let mut root = planner();
    let k1 = make_canonical_pathkey(&mut root, &new_ec(1), BTREE_INT4_OPF, CompareType::Lt, false);
    let k2 = make_canonical_pathkey(&mut root, &new_ec(2), BTREE_INT4_OPF, CompareType::Lt, false);

    let longer = vec![k1.clone(), k2];
    let shorter = vec![k1];

    // A path sorted by (a, b) satisfies a required ORDER BY a.
    assert!(pathkeys_contained_in(&shorter, &longer));
    // But (a) does not satisfy a required ORDER BY (a, b).
    assert!(!pathkeys_contained_in(&longer, &shorter));
}

#[test]
fn compare_pathkeys_semantics() {
    let mut root = planner();
    let k1 = make_canonical_pathkey(&mut root, &new_ec(1), BTREE_INT4_OPF, CompareType::Lt, false);
    let k2 = make_canonical_pathkey(&mut root, &new_ec(2), BTREE_INT4_OPF, CompareType::Lt, false);

    let one = vec![k1.clone()];
    let both = vec![k1.clone(), k2.clone()];

    // keys2 is the longer superset -> BETTER2.
    assert!(matches!(
        compare_pathkeys(&one, &both),
        PathKeysComparison::Better2
    ));
    // keys1 is the longer superset -> BETTER1.
    assert!(matches!(
        compare_pathkeys(&both, &one),
        PathKeysComparison::Better1
    ));
    // Identical -> EQUAL.
    assert!(matches!(
        compare_pathkeys(&both, &both),
        PathKeysComparison::Equal
    ));
    // Diverging first key -> DIFFERENT.
    let other = vec![k2];
    assert!(matches!(
        compare_pathkeys(&one, &other),
        PathKeysComparison::Different
    ));
}

#[test]
fn pathkeys_count_contained_in_common_prefix() {
    let mut root = planner();
    let k1 = make_canonical_pathkey(&mut root, &new_ec(1), BTREE_INT4_OPF, CompareType::Lt, false);
    let k2 = make_canonical_pathkey(&mut root, &new_ec(2), BTREE_INT4_OPF, CompareType::Lt, false);
    let k3 = make_canonical_pathkey(&mut root, &new_ec(3), BTREE_INT4_OPF, CompareType::Lt, false);

    let a = vec![k1.clone(), k2.clone()];
    let b = vec![k1.clone(), k2.clone(), k3];
    // a is a prefix of b: contained, 2 common.
    assert_eq!(pathkeys_count_contained_in(&a, &b), (true, 2));

    // Diverge at position 1.
    let c = vec![k1, k2];
    let d = vec![c[0].clone()];
    assert_eq!(pathkeys_count_contained_in(&c, &d), (false, 1));
}

#[test]
fn append_pathkeys_skips_redundant() {
    let mut root = planner();
    let k1 = make_canonical_pathkey(&mut root, &new_ec(1), BTREE_INT4_OPF, CompareType::Lt, false);
    let k2 = make_canonical_pathkey(&mut root, &new_ec(2), BTREE_INT4_OPF, CompareType::Lt, false);
    // Same EC as k1 but different direction -> redundant (same EC).
    let k1_desc =
        make_canonical_pathkey(&mut root, &new_ec(1), BTREE_INT4_OPF, CompareType::Gt, false);

    let target = vec![k1.clone()];
    let out = append_pathkeys(target, &[k1_desc, k2.clone()]);
    // k1_desc is redundant (same EC as k1); only k2 is appended.
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], k1);
    assert_eq!(out[1], k2);
}

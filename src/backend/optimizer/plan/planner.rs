//! The query optimizer external interface. Translated from
//! backend/optimizer/plan/planner.c.
//!
//! `standard_planner` is the keystone entry point: it sets up the per-invocation
//! `PlannerGlobal`, calls `subquery_planner` (which sets up the per-Query
//! `PlannerInfo` and runs `grouping_planner`), turns the cheapest Path into a
//! Plan via `create_plan`, finalizes Params (`SS_finalize_plan`) and var refs
//! (`set_plan_references`), and assembles the `PlannedStmt`. Non-type-centric
//! free functions; bodies here as snake_case `pub fn`s, re-exported from
//! `crate::optimizer::planner` / `crate::optimizer::optimizer` under the C names.
//!
//! Disposition: `grow`. M1's live path is the table-less constant SELECT:
//! standard_planner -> subquery_planner -> grouping_planner -> query_planner ->
//! create_group_result_path -> create_plan (create_group_result_plan) ->
//! set_plan_references -> PlannedStmt with a single `Result` plan node. The
//! parallel-mode assessment, cursor tuple-fraction tweaks, the WITH/MERGE/
//! empty-jointree/sublink/subquery-pullup/setop/expression-preprocessing/
//! rowmark/HAVING-pullup machinery of subquery_planner, and the
//! grouping/window/distinct/sort/limit/setop arms of grouping_planner are all
//! scaffolded as grow guards (rules.md s4); none is half-written. PlannerInfo /
//! PlannerGlobal are per-plan owned structs threaded by `&mut` (rules.md s8),
//! NOT task_local/shared.

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: standard_planner/subquery_planner mirror PG by-value pointer params (boundParams, parent_root); consumed once they are threaded into glob/root as planning grows"
)]

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::params::ParamListInfoData;
use crate::nodes::parsenodes::{Query, SetOperationStmt};
use crate::nodes::pathnodes::{PlannerGlobal, PlannerInfo, UpperRelationKind};
use crate::nodes::plannodes::PlannedStmt;
use crate::backend::optimizer::plan::planmain::{create_plan, query_planner};
use crate::backend::optimizer::plan::setrefs::set_plan_references;
use crate::backend::optimizer::plan::subselect::ss_finalize_plan;
use crate::backend::optimizer::prep::preptlist::preprocess_targetlist;
use crate::backend::optimizer::util::clauses::eval_const_expressions;

const PGJIT_NONE: i32 = 0;

/// `char` parallel-hazard code: PROPARALLEL_UNSAFE (`'u'`), stored as the
/// `PlannerGlobal.max_parallel_hazard` u8.
const PROPARALLEL_UNSAFE: u8 = b'u';

// Cursor option bits (PG `CURSOR_OPT_*` in nodes/parsenodes.h). M1 supports none.
const CURSOR_OPT_SCROLL: i32 = 0x0002;
const CURSOR_OPT_FAST_PLAN: i32 = 0x0008;
const CURSOR_OPT_PARALLEL_OK: i32 = 0x0010;

/// Panic for a planner path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `makeNode(PlannerGlobal)`: a zero-initialized `PlannerGlobal` (palloc0
/// semantics). boundParams/partition_directory are opaque planner inputs dropped
/// from the skeleton (pathnodes.rs), so they take no field here.
fn make_planner_global() -> Box<PlannerGlobal> {
    Box::new(PlannerGlobal {
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
    })
}

/// PG `standard_planner`: turn a `Query` into a `PlannedStmt`. The planner_hook,
/// if set, would wrap this; M1 always uses the standard path.
pub fn standard_planner(
    parse: &mut Query,
    _query_string: &str,
    cursor_options: i32,
    bound_params: Option<Box<ParamListInfoData>>,
) -> PlannedStmt {
    // boundParams are not modeled yet (no external Params on the M1 path).
    if bound_params.is_some() {
        not_yet_reachable("standard_planner: bound parameters");
    }

    // Set up global state for this planner invocation.
    let mut glob = make_planner_global();

    // Assess parallel-mode feasibility. M1 runs no parallel plans; PG's cheap
    // tests all fail in a standalone backend, so just assume unsafe. The full
    // max_parallel_hazard scan grows when parallel plans are considered.
    glob.max_parallel_hazard = PROPARALLEL_UNSAFE;
    glob.parallel_mode_ok = false;
    glob.parallel_mode_needed = false;

    // Determine what fraction of the plan is likely to be scanned. CURSOR_OPT_*
    // handling (fast/scroll cursors) grows with cursor support; default is "all
    // tuples".
    if cursor_options & (CURSOR_OPT_FAST_PLAN | CURSOR_OPT_SCROLL | CURSOR_OPT_PARALLEL_OK) != 0 {
        not_yet_reachable("standard_planner: cursor options");
    }
    let tuple_fraction = 0.0;

    // Primary planning entry point (may recurse for subqueries).
    let mut root = subquery_planner(&mut glob, parse, None, false, tuple_fraction, None);

    // Select best Path and turn it into a Plan.
    let final_rel = fetch_final_rel(&root);
    let best_path = final_rel
        .cheapest_total_path
        .clone()
        .unwrap_or_else(|| not_yet_reachable("standard_planner: no cheapest path"));

    let mut top_plan = create_plan(&mut root, &best_path);

    // M5 (step 26): build the upper (grouping/aggregation/distinct/sort/limit) plan
    // on top of the scan/join plan. PG builds these as Paths in grouping_planner and
    // turns them into plan nodes in create_plan; with the port's flat Path this is
    // assembled directly here from the query's clauses (the ModifyTable precedent
    // below). SELECT only -- an INSERT source has no grouping stage.
    if parse.commandType == CmdType::SELECT {
        top_plan = crate::backend::optimizer::plan::createplan::build_upper_plan(&root, top_plan);
    }

    // FOR UPDATE/SHARE: wrap the scan plan in a LockRows node that locks the
    // selected rows (M8, step 34). PG builds a LockRowsPath in grouping_planner; the
    // port assembles it here from root.row_marks (set by preprocess_rowmarks). The
    // ModifyTable below (if any) sits ABOVE the LockRows.
    if !root.row_marks.is_empty() && parse.commandType == CmdType::SELECT {
        top_plan = make_lockrows_plan(&root, top_plan);
    }

    // For a data-modifying statement, wrap the source plan in a ModifyTable. PG
    // builds a ModifyTablePath in grouping_planner; the port wraps the source plan
    // here for the single, non-inherited target (the ModifyTablePath/bitmapset path
    // for inherited targets grows later). Records the result relation on glob.
    if matches!(
        parse.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
        top_plan = make_modifytable_plan(&mut root, parse, top_plan);
    }

    // Scrollable-cursor materialization and the debug_parallel_query Gather
    // injection are gated out above (no cursor/parallel options in M1).

    // If any Params were generated, compute extParam/allParam sets. None on the
    // M1 const path; SS_finalize_plan is otherwise a no-op recurse.
    if !glob.param_exec_types.is_empty() {
        ss_finalize_plan(&mut root, &mut top_plan);
    }

    // Final cleanup of the plan: flatten the rangetable and fix var refs.
    let top_plan = set_plan_references(&mut root, top_plan);

    // Build the PlannedStmt result.
    let glob = &root.glob;
    crate::assert!(glob.append_relations.is_empty());
    PlannedStmt {
        command_type: parse.commandType,
        query_id: parse.queryId,
        plan_id: 0,
        has_returning: !parse.returningList.is_empty(),
        has_modifying_cte: parse.hasModifyingCTE,
        can_set_tag: parse.canSetTag,
        transient_plan: glob.transient_plan,
        depends_on_role: glob.depends_on_role,
        parallel_mode_needed: glob.parallel_mode_needed,
        jit_flags: PGJIT_NONE,
        plan_tree: top_plan,
        part_prune_infos: glob.part_prune_infos.clone(),
        rtable: glob.finalrtable.clone(),
        // bms_difference(allRelids, prunableRelids); both None on the M1 path.
        unprunable_relids: None,
        perm_infos: glob.finalrteperminfos.clone(),
        result_relations: glob.result_relations.clone(),
        // glob.append_relations is empty on the M1 path (no inheritance/partitioning);
        // its Vec<Box<AppendRelInfo>> -> Vec<Node> conversion grows with that.
        append_relations: Vec::new(),
        subplans: glob.subplans.clone(),
        rewind_plan_ids: glob.rewind_plan_ids.clone(),
        row_marks: glob.finalrowmarks.clone(),
        relation_oids: glob.relation_oids.clone(),
        inval_items: glob.inval_items.clone(),
        param_exec_types: glob.param_exec_types.clone(),
        utility_stmt: parse.utilityStmt.clone(),
        stmt_location: parse.stmt_location,
        stmt_len: parse.stmt_len,
    }
}

/// PG `create_modifytable_plan`: wrap `subplan` (the source rows) in a `ModifyTable`
/// plan targeting the query's single result relation. Records the result relation in
/// `glob.result_relations`. M8 (step 34) adds the UPDATE/DELETE/MERGE operations, the
/// RETURNING projection lists, and the row marks; inherited targets, WCO, ON
/// CONFLICT, and the FDW fields grow at their milestones.
fn make_modifytable_plan(
    root: &mut PlannerInfo,
    parse: &Query,
    subplan: Node,
) -> Node {
    let result_relation = parse.resultRelation;
    crate::assert!(result_relation > 0);
    root.glob.result_relations = vec![result_relation];

    // The plan's own tlist is the RETURNING projection (empty otherwise). PG keeps
    // `returningLists` as a list-of-lists (one per result relation); with a single
    // target the port stores the RETURNING TargetEntries directly in the plan's
    // targetlist and mirrors a per-rel copy in `returning_lists` for the executor's
    // per-result-rel setup. M8's RETURNING Vars are resolved against the subplan slot.
    let plan_tlist = parse.returningList.clone();
    let returning_lists = parse.returningList.clone();

    // The merge action list (the single target's actions), carried for MERGE.
    let merge_action_lists = parse.mergeActionList.clone();

    let modify = crate::nodes::plannodes::ModifyTable {
        plan: crate::nodes::plannodes::Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: plan_tlist,
            qual: Vec::new(),
            lefttree: Some(subplan),
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        },
        operation: parse.commandType,
        can_set_tag: parse.canSetTag,
        nominal_relation: result_relation as crate::nodes::primnodes::Index,
        root_relation: 0,
        part_cols_updated: false,
        result_relations: vec![result_relation],
        update_colnos_lists: Vec::new(),
        with_check_option_lists: Vec::new(),
        returning_old_alias: parse.returningOldAlias.clone(),
        returning_new_alias: parse.returningNewAlias.clone(),
        returning_lists,
        fdw_priv_lists: Vec::new(),
        fdw_direct_modify_plans: None,
        // The row marks (FOR UPDATE rows being modified) are owned by the ModifyTable
        // in PG when there is no separate LockRows; M8's UPDATE/DELETE locks the row
        // via the heap update/delete tuple-lock itself, so no row marks here.
        row_marks: Vec::new(),
        epq_param: -1,
        on_conflict_action: crate::nodes::nodes::OnConflictAction::NONE,
        arbiter_indexes: Vec::new(),
        on_conflict_set: Vec::new(),
        on_conflict_cols: Vec::new(),
        on_conflict_where: None,
        excl_rel_rti: 0,
        excl_rel_tlist: Vec::new(),
        merge_action_lists,
        merge_join_conditions: Vec::new(),
    };
    Node::ModifyTable(Box::new(modify))
}

/// PG `create_lockrows_plan`: wrap `subplan` in a `LockRows` node carrying the
/// query's row marks (a `PlanRowMark` per locked relation). The executor's
/// ExecLockRows locks each row from the subplan per its mark (M8, step 34).
fn make_lockrows_plan(root: &PlannerInfo, subplan: Node) -> Node {
    let row_marks = root.row_marks.clone();
    let lockrows = crate::nodes::plannodes::LockRows {
        plan: crate::nodes::plannodes::Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            // LockRows projects its child's tlist unchanged.
            targetlist: top_plan_tlist_clone(&subplan),
            qual: Vec::new(),
            lefttree: Some(subplan),
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        },
        row_marks,
        epq_param: -1,
    };
    Node::LockRows(Box::new(lockrows))
}

/// The targetlist of the child plan node (LockRows projects its child unchanged).
fn top_plan_tlist_clone(node: &Node) -> Vec<Node> {
    match node {
        Node::SeqScan(s) => s.scan.plan.targetlist.clone(),
        Node::IndexScan(s) => s.scan.plan.targetlist.clone(),
        Node::IndexOnlyScan(s) => s.scan.plan.targetlist.clone(),
        Node::BitmapHeapScan(s) => s.scan.plan.targetlist.clone(),
        Node::Result(r) => r.plan.targetlist.clone(),
        Node::Sort(s) => s.plan.targetlist.clone(),
        Node::Limit(l) => l.plan.targetlist.clone(),
        Node::Agg(a) => a.plan.targetlist.clone(),
        Node::NestLoop(n) => n.join.plan.targetlist.clone(),
        Node::HashJoin(h) => h.join.plan.targetlist.clone(),
        Node::MergeJoin(m) => m.join.plan.targetlist.clone(),
        _ => Vec::new(),
    }
}

/// PG `preprocess_rowmarks`: convert the query's `RowMarkClause`s (FOR UPDATE/SHARE)
/// into `PlanRowMark`s on `root.row_marks`, and mirror them into
/// `glob.finalrowmarks` (the PlannedStmt's row marks). The non-target/non-locked
/// REFERENCE rowmarks PG also adds (for EPQ rechecks of other rels) are not needed in
/// the single-rel M8 case. MERGE / inheritance rowmark handling grows later.
fn preprocess_rowmarks(root: &mut PlannerInfo) {
    use crate::nodes::nodes::Node;
    use crate::nodes::plannodes::PlanRowMark;

    if root.parse.rowMarks.is_empty() {
        return;
    }
    // PG bails to "no marks" for MERGE / a query with no real locked rels; M8 only
    // reaches FOR UPDATE on a plain SELECT.
    if root.parse.commandType != CmdType::SELECT {
        not_yet_reachable("preprocess_rowmarks: FOR UPDATE on a non-SELECT");
    }

    let mut prowmarks: Vec<PlanRowMark> = Vec::new();
    for rc_node in &root.parse.rowMarks {
        let Node::RowMarkClause(rc) = rc_node else {
            not_yet_reachable("preprocess_rowmarks: not a RowMarkClause");
        };
        let rte = &root.parse.rtable[rc.rti - 1];
        let mark_type = select_rowmark_type(rte, rc.strength);
        root.glob.last_row_mark_id += 1;
        prowmarks.push(PlanRowMark {
            rti: rc.rti,
            prti: rc.rti,
            rowmark_id: root.glob.last_row_mark_id,
            mark_type,
            all_mark_types: 1 << (mark_type as i32),
            strength: rc.strength,
            wait_policy: rc.waitPolicy,
            is_parent: false,
        });
    }

    let mark_nodes: Vec<Node> = prowmarks
        .into_iter()
        .map(|m| Node::PlanRowMark(Box::new(m)))
        .collect();
    root.glob.finalrowmarks.clone_from(&mark_nodes);
    root.row_marks = mark_nodes;
}

/// PG `select_rowmark_type`: pick the `RowMarkType` for an RTE under a lock strength.
/// A plain relation under FOR UPDATE / NO KEY UPDATE marks ROW_MARK_EXCLUSIVE /
/// NOKEYEXCLUSIVE; FOR SHARE / KEY SHARE marks SHARE / KEYSHARE. Non-relations use
/// ROW_MARK_COPY (not reachable in M8). Foreign tables grow later.
fn select_rowmark_type(
    rte: &crate::nodes::nodes::Node,
    strength: crate::nodes::lockoptions::LockClauseStrength,
) -> crate::nodes::plannodes::RowMarkType {
    use crate::nodes::lockoptions::LockClauseStrength as S;
    use crate::nodes::plannodes::RowMarkType;
    let crate::nodes::nodes::Node::RangeTblEntry(rte) = rte else {
        return RowMarkType::COPY;
    };
    if rte.rtekind != crate::nodes::parsenodes::RTEKind::RELATION {
        return RowMarkType::COPY;
    }
    match strength {
        S::FORUPDATE => RowMarkType::EXCLUSIVE,
        S::FORNOKEYUPDATE => RowMarkType::NOKEYEXCLUSIVE,
        S::FORSHARE => RowMarkType::SHARE,
        S::FORKEYSHARE => RowMarkType::KEYSHARE,
        S::NONE => RowMarkType::REFERENCE,
    }
}

/// PG `subquery_planner`: the per-Query planning driver. Sets up the
/// `PlannerInfo` ("root"), runs the simplification/pullup/preprocessing passes,
/// then `grouping_planner`, and returns the root with the final rel's cheapest
/// path selected.
pub fn subquery_planner(
    glob: &mut PlannerGlobal,
    parse: &mut Query,
    parent_root: Option<&mut PlannerInfo>,
    has_recursion: bool,
    tuple_fraction: f64,
    setops: Option<&SetOperationStmt>,
) -> PlannerInfo {
    if has_recursion {
        not_yet_reachable("subquery_planner: recursive query");
    }
    if setops.is_some() {
        not_yet_reachable("subquery_planner: set operations");
    }
    let query_level = parent_root.as_ref().map_or(1, |p| p.query_level + 1);
    if parent_root.is_some() {
        not_yet_reachable("subquery_planner: sub-Query recursion");
    }

    // Create a PlannerInfo data structure for this subquery.
    let mut root = make_planner_info(glob, parse, query_level);

    // The WITH list (SS_process_ctes), MERGE jointree transform, ANY/EXISTS
    // sublink pullup, function-RTE inlining, generated-column expansion, subquery
    // pullup, and UNION ALL flattening all precede the rangetable survey. None
    // applies to the M2 single-rel / const SELECT / INSERT ... VALUES paths.
    if !root.parse.cteList.is_empty() {
        not_yet_reachable("subquery_planner: WITH clause");
    }
    if root.parse.setOperations.is_some() {
        not_yet_reachable("subquery_planner: set operations");
    }
    if root.parse.hasSubLinks {
        not_yet_reachable("subquery_planner: sublinks");
    }

    // If the FROM clause is empty, replace it with a dummy RTE_RESULT so that the
    // jointree is never empty (PG: replace_empty_jointree in prepjointree). This
    // gives a FROM-less `SELECT 1` a one-entry rangetable (an RTE_RESULT), matching
    // PG, and is the source of the M2 RTE_RESULT.
    crate::backend::optimizer::prep::prepjointree::replace_empty_jointree(&mut root.parse);

    // Survey the rangetable. M2 supports an RTE_RELATION (a base rel) and an
    // RTE_RESULT (the empty-FROM placeholder). JOIN/SUBQUERY/FUNCTION/VALUES/GROUP
    // RTEs, lateral refs, and security quals grow with their milestones.
    //
    // PG `expand_inherited_tables` (folded here): a RELATION RTE's `inh` flag starts
    // true (the RangeVar default "descend into children"); prep clears it when the
    // relation `!relhassubclass`. Inheritance/partitioning is unsupported this
    // milestone, so no table has subclasses -> clear every RELATION's `inh`. This
    // keeps the multi-rel join path's `get_relation_info(rte.inh)` off the
    // inheritance-parent grow guard (the single-rel path already passes `false`).
    for rte in &mut root.parse.rtable {
        let Node::RangeTblEntry(rte) = rte else {
            not_yet_reachable("subquery_planner: rangetable entry is not an RTE");
        };
        match rte.rtekind {
            crate::nodes::parsenodes::RTEKind::RELATION => rte.inh = false,
            crate::nodes::parsenodes::RTEKind::RESULT => {}
            other => not_yet_reachable(&format!("subquery_planner: RTE kind {other:?}")),
        }
    }

    // preprocess_rowmarks: convert the query's RowMarkClauses (FOR UPDATE/SHARE) into
    // PlanRowMarks on root.row_marks (M8, step 34).
    preprocess_rowmarks(&mut root);

    root.has_having_qual = root.parse.havingQual.is_some();
    if root.has_having_qual {
        not_yet_reachable("subquery_planner: HAVING clause");
    }

    // Expression preprocessing on the targetlist (eval_const_expressions etc).
    // M1's targets are already-resolved Const nodes that fold to themselves, so
    // PG's preprocess_expression over the tlist is an identity here. The general
    // preprocess_expression (and the WCO/returning/qual/window/limit/onConflict/
    // merge passes) grows when non-trivial expressions appear.
    if root.parse.hasTargetSRFs {
        not_yet_reachable("subquery_planner: set-returning functions in tlist");
    }
    // LIMIT/OFFSET expressions are int8 Consts (transformLimitClause const-folds the
    // literal form), so preprocess_expression over them is an identity; they need no
    // guard. M8's RETURNING list is simple Vars/consts (preprocess_expression is an
    // identity over them), so it needs no guard either. WCO / onConflict / window
    // expression preprocessing grows with those features. MERGE action preprocessing
    // is staged (MERGE execution is staged this milestone).
    if !root.parse.withCheckOptions.is_empty()
        || root.parse.onConflict.is_some()
        || !root.parse.windowClause.is_empty()
    {
        not_yet_reachable("subquery_planner: expression preprocessing");
    }

    // reduce_outer_joins / remove_useless_result_rtes need an actual rangetable;
    // none in M1.

    // Do the main planning.
    grouping_planner(&mut root, tuple_fraction, setops);

    // SS_identify_outer_params / SS_charge_for_initplans are no-ops with no
    // outer params and no initplans. Make sure the cheapest path of the final
    // rel is identified (set inside grouping_planner via set_cheapest already).

    root
}

/// PG `makeNode(PlannerInfo)` + the subquery_planner field initialization that
/// matters for the M1 path. The many DP-join-search / EC / placeholder / agg
/// workspaces are zero-initialized (palloc0 semantics) and grow as their phases
/// do.
fn make_planner_info(glob: &PlannerGlobal, parse: &Query, query_level: usize) -> PlannerInfo {
    PlannerInfo {
        parse: Box::new(parse.clone()),
        // The glob is shared across sub-Querys in C; the M1 path has a single
        // Query, so a clone of the (so-far-empty) glob is equivalent. The
        // assembled finalrtable etc. are read back from root.glob at the end.
        glob: Box::new(glob.clone()),
        query_level,
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

/// PG `grouping_planner`: the main planning driver below subquery_planner. It
/// builds the scan/join paths (`query_planner`), applies the final target, and
/// produces the final-output upper rel.
fn grouping_planner(root: &mut PlannerInfo, tuple_fraction: f64, setops: Option<&SetOperationStmt>) {
    let parse = &root.parse;

    root.tuple_fraction = tuple_fraction;

    if parse.setOperations.is_some() || setops.is_some() {
        not_yet_reachable("grouping_planner: set operations");
    }

    // No set operations: regular planning.
    crate::assert!(!root.has_recursion);

    if !root.parse.groupingSets.is_empty() {
        not_yet_reachable("grouping_planner: grouping sets");
    }
    if root.parse.hasWindowFuncs || !root.parse.windowClause.is_empty() {
        not_yet_reachable("grouping_planner: window functions");
    }

    // Preprocess targetlist into root->processed_tlist (the FINAL tlist; carries the
    // Aggrefs and the grouping/sort-keyed ressortgrouprefs).
    preprocess_targetlist(root);

    // M5 (step 26): the query has an upper (grouping/aggregation/sort/distinct/limit)
    // stage when it groups, aggregates, distincts, sorts, or limits. When it does,
    // the base scan must compute the *group/agg-input* tlist (the flattened Vars the
    // grouping/aggregation reads), not the final Aggref-bearing tlist; compute it and
    // stash it in `scan_input_tlist` so query_planner builds the scan rel from it.
    let needs_upper = root.parse.hasAggs
        || !root.parse.groupClause.is_empty()
        || !root.parse.distinctClause.is_empty()
        || !root.parse.sortClause.is_empty()
        || root.parse.limitCount.is_some()
        || root.parse.limitOffset.is_some();
    if needs_upper {
        root.scan_input_tlist = make_scan_input_tlist(root);
    }

    root.limit_tuples = -1.0;

    // Generate the best paths for the scan/join portion of the query (the
    // FROM/WHERE processing). For a table-less SELECT this builds the dummy
    // result rel with its one Result path.
    let current_rel = query_planner(root, standard_qp_callback);

    // The scan/join (reltarget) is the scan-input target when grouping, else the
    // final target. Stash it in the upper-target slots (read by callers that build
    // upper rels; the M5 upper plan is built in standard_planner from the clauses).
    let scan_target = current_rel
        .reltarget
        .clone()
        .unwrap_or_else(|| not_yet_reachable("grouping_planner: missing reltarget"));
    for slot in &mut root.upper_targets {
        *slot = Some(scan_target.clone());
    }

    // The LockRows (FOR UPDATE) and ModifyTable (INSERT/UPDATE/DELETE/MERGE) plan
    // nodes are assembled in standard_planner from the scan/join plan (the port's
    // flat-Path precedent), so grouping_planner just records the scan/join rel here.
    root.upper_rels[UpperRelationKind::FINAL as usize] = vec![Box::new(current_rel)];
}

/// Build the scan/join (group/agg-input) targetlist: the flattened set of base-rel
/// `Var`s the grouping/aggregation/sort reads, in stable order. For the M5 milestone
/// shape this is the distinct Vars appearing in the final tlist's grouping columns,
/// aggregate arguments, and ORDER BY expressions (PG's `make_group_input_target` /
/// `make_sort_input_target`, collapsed to the Var-pulling core). Resnos are assigned
/// 1..n and the ressortgrouprefs are carried so the upper nodes can find group keys.
fn make_scan_input_tlist(root: &PlannerInfo) -> Vec<Node> {
    let mut exprs: Vec<Node> = Vec::new();
    let mut sortgrouprefs: Vec<crate::c::Index> = Vec::new();
    // Pull every Var out of the final tlist (the aggregate inputs are Vars inside
    // Aggrefs; the grouping/sort columns are Vars directly). Deduplicate by Var
    // identity (varno/varattno), keeping the first occurrence's sortgroupref.
    for n in &root.processed_tlist {
        let Node::TargetEntry(te) = n else { continue };
        let Some(expr) = te.expr.as_ref() else { continue };
        pull_vars_into(expr, te.ressortgroupref, &mut exprs, &mut sortgrouprefs);
    }

    exprs
        .into_iter()
        .zip(sortgrouprefs)
        .enumerate()
        .map(|(i, (expr, sgr))| {
            let mut tle = crate::nodes::makefuncs::makeTargetEntry(
                Some(expr),
                (i + 1) as crate::access::attnum::AttrNumber,
                None,
                false,
            );
            tle.ressortgroupref = sgr;
            Node::TargetEntry(Box::new(tle))
        })
        .collect()
}

/// Pull the base-rel `Var`s out of a final-tlist expression into the scan-input
/// list (deduplicating on varno/varattno). A bare grouping/sort Var carries its
/// `sortgroupref`; Aggref-argument Vars carry 0. The M5-reachable expression kinds
/// (Var, Aggref over a Var arg) are handled; richer exprs grow with the general
/// pull_var_clause.
fn pull_vars_into(
    expr: &Node,
    sortgroupref: crate::c::Index,
    exprs: &mut Vec<Node>,
    refs: &mut Vec<crate::c::Index>,
) {
    match expr {
        Node::Var(v) => {
            // Deduplicate on (varno, varattno); keep the first sortgroupref seen.
            for (i, e) in exprs.iter().enumerate() {
                if let Node::Var(ev) = e
                    && ev.varno == v.varno
                    && ev.varattno == v.varattno
                {
                    if refs[i] == 0 {
                        refs[i] = sortgroupref;
                    }
                    return;
                }
            }
            exprs.push(expr.clone());
            refs.push(sortgroupref);
        }
        Node::Aggref(agg) => {
            for arg in &agg.args {
                // Aggref args are TargetEntry-wrapped (transformAggregateCall).
                let inner = match arg {
                    Node::TargetEntry(te) => te.expr.as_ref(),
                    other => Some(other),
                };
                if let Some(inner) = inner {
                    pull_vars_into(inner, 0, exprs, refs);
                }
            }
        }
        // M5 milestone tlists are flat (a Var or an Aggref per column). Constants
        // carry no Var; richer projection expressions grow with pull_var_clause.
        _ => {}
    }
}

/// PG `standard_qp_callback`: compute the query's sort/group/distinct/setop
/// pathkeys for query_planner. The M5 sorted-aggregation plan is built directly in
/// standard_planner (not via pathkey-driven path selection), so the pathkey lists
/// stay empty here; they grow when cost-based ordered-path selection lands.
fn standard_qp_callback(_root: &mut PlannerInfo) {}

/// Fetch the final-output upper rel (`fetch_upper_rel(root, UPPERREL_FINAL)`).
/// query_planner/grouping_planner store it directly in `upper_rels[FINAL]` for
/// the M1 path; the keyed fetch_upper_rel (which creates-or-finds by relids)
/// grows with multi-rel upper processing.
fn fetch_final_rel(root: &PlannerInfo) -> &crate::nodes::pathnodes::RelOptInfo {
    let final_slot = &root.upper_rels[UpperRelationKind::FINAL as usize];
    crate::assert!(final_slot.len() == 1);
    &final_slot[0]
}

/// PG `expression_planner`: run the planner's standalone-expression
/// transformations (const-simplification, default-arg insertion, opfuncid fixup)
/// on a bare expression. M1 needs only the const-fold (a Const folds to itself);
/// fix_opfuncids over a const tree is a no-op. (Defined in planner.c in PG;
/// declared in optimizer.h.)
pub fn expression_planner(expr: Node) -> Node {
    let result = eval_const_expressions(None, Some(expr));
    // fix_opfuncids fills in missing opfuncid values; no OpExprs on the M1 path.
    result.unwrap_or_else(|| not_yet_reachable("expression_planner: NULL expression"))
}

/// PG `limit_needed`: does the query have an effective LIMIT/OFFSET? M1 has
/// none. Declared in planner.h.
pub fn limit_needed(parse: &Query) -> bool {
    if parse.limitCount.is_some() || parse.limitOffset.is_some() {
        not_yet_reachable("limit_needed: LIMIT/OFFSET");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;
    use crate::postgres::DatumGetInt32;

    /// Raw-parse + analyze + rewrite `s` into a single top-level Query, ready for
    /// the planner. Mirrors how PostgresMain feeds the planner.
    fn plan(s: &str) -> PlannedStmt {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1, "expected exactly one statement");
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs: RawStmt = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, s, &[], 0, None);
        let mut rewritten = crate::backend::rewrite::rewriteHandler::query_rewrite(*q);
        assert_eq!(rewritten.len(), 1, "table-less SELECT rewrites to one query");
        let mut parse = rewritten.remove(0);
        standard_planner(&mut parse, s, 0, None)
    }

    /// Pull the Result plan node out of a PlannedStmt.
    fn result_of(stmt: &PlannedStmt) -> &crate::nodes::plannodes::Result {
        let Node::Result(r) = &stmt.plan_tree else { panic!("planTree is not a Result") };
        r
    }

    /// Pull the i-th TargetEntry of the Result's plan targetlist.
    fn tle(r: &crate::nodes::plannodes::Result, i: usize) -> &crate::nodes::primnodes::TargetEntry {
        let Node::TargetEntry(te) = &r.plan.targetlist[i] else { panic!("not a TargetEntry") };
        te
    }

    /// Pull the Const out of a TargetEntry's expr.
    fn const_of(r: &crate::nodes::plannodes::Result, i: usize) -> &crate::nodes::primnodes::Const {
        let Node::Const(c) = tle(r, i).expr.as_ref().unwrap() else { panic!("not a Const") };
        c
    }

    #[test]
    fn select_one_plans_to_result() {
        let stmt = plan("SELECT 1");
        assert_eq!(stmt.command_type, CmdType::SELECT);
        assert!(stmt.can_set_tag);
        // replace_empty_jointree injects one RTE_RESULT for a FROM-less SELECT, so
        // the planned rangetable has exactly that one entry (matching PG 18.4).
        assert_eq!(stmt.rtable.len(), 1, "FROM-less SELECT gets one RTE_RESULT");
        let Node::RangeTblEntry(rte) = &stmt.rtable[0] else { panic!("not an RTE") };
        assert_eq!(
            rte.rtekind,
            crate::nodes::parsenodes::RTEKind::RESULT,
            "the injected RTE is an RTE_RESULT"
        );
        assert!(!stmt.has_returning);

        let r = result_of(&stmt);
        assert!(r.resconstantqual.is_none(), "no one-time qual for a plain const SELECT");
        assert!(r.plan.lefttree.is_none(), "the Result is childless");
        assert!((r.plan.plan_rows - 1.0).abs() < f64::EPSILON, "Result emits one row");
        assert_eq!(r.plan.targetlist.len(), 1);

        let te = tle(r, 0);
        assert_eq!(te.resno, 1);
        assert_eq!(te.resname.as_deref(), Some("?column?"), "resname carried from the query tlist");

        let c = const_of(r, 0);
        assert_eq!(c.consttype, INT4OID);
        assert_eq!(DatumGetInt32(c.constvalue), 1);
    }

    #[test]
    fn select_42_plans_to_result() {
        let stmt = plan("SELECT 42");
        let r = result_of(&stmt);
        assert_eq!(r.plan.targetlist.len(), 1);
        assert_eq!(DatumGetInt32(const_of(r, 0).constvalue), 42);
    }

    #[test]
    fn select_two_constants_two_target_entries() {
        let stmt = plan("SELECT 1, 2");
        let r = result_of(&stmt);
        assert_eq!(r.plan.targetlist.len(), 2);
        assert_eq!(tle(r, 0).resno, 1);
        assert_eq!(tle(r, 1).resno, 2);
        assert_eq!(DatumGetInt32(const_of(r, 0).constvalue), 1);
        assert_eq!(DatumGetInt32(const_of(r, 1).constvalue), 2);
    }

    #[test]
    fn select_with_alias_keeps_name_on_plan_tlist() {
        let stmt = plan("SELECT 1 AS x");
        let r = result_of(&stmt);
        assert_eq!(tle(r, 0).resname.as_deref(), Some("x"));
    }
}

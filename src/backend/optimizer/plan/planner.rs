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

    // Scrollable-cursor materialization and the debug_parallel_query Gather
    // injection are gated out above (no cursor/parallel options in M1).

    // If any Params were generated, compute extParam/allParam sets. None on the
    // M1 const path; SS_finalize_plan is otherwise a no-op recurse.
    if !glob.param_exec_types.is_empty() {
        ss_finalize_plan(&mut root, &mut top_plan);
    }

    // Final cleanup of the plan: flatten the rangetable and fix var refs.
    crate::assert!(glob.finalrtable.is_empty());
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

    // The WITH list (SS_process_ctes), MERGE jointree transform, empty-jointree
    // RTE_RESULT injection, ANY/EXISTS sublink pullup, function-RTE inlining,
    // generated-column expansion, subquery pullup, and UNION ALL flattening all
    // precede the rangetable survey. None applies to a table-less constant SELECT.
    if !root.parse.cteList.is_empty() {
        not_yet_reachable("subquery_planner: WITH clause");
    }
    if root.parse.setOperations.is_some() {
        not_yet_reachable("subquery_planner: set operations");
    }
    if root.parse.hasSubLinks {
        not_yet_reachable("subquery_planner: sublinks");
    }

    // Survey the rangetable. M1's const SELECT has an empty rangetable, so there
    // is nothing to survey (no JOIN/RESULT/GROUP RTEs, no lateral, no security
    // quals). The per-RTE-kind survey and expression preprocessing grow with the
    // rangetable machinery.
    if !root.parse.rtable.is_empty() {
        not_yet_reachable("subquery_planner: rangetable survey / preprocessing");
    }
    if root.parse.resultRelation != 0 {
        not_yet_reachable("subquery_planner: result relation");
    }

    // preprocess_rowmarks: none in M1.

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
    if !root.parse.returningList.is_empty()
        || !root.parse.withCheckOptions.is_empty()
        || root.parse.onConflict.is_some()
        || !root.parse.mergeActionList.is_empty()
        || !root.parse.windowClause.is_empty()
        || root.parse.limitOffset.is_some()
        || root.parse.limitCount.is_some()
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

    if parse.limitCount.is_some() || parse.limitOffset.is_some() {
        not_yet_reachable("grouping_planner: LIMIT/OFFSET");
    }
    root.tuple_fraction = tuple_fraction;

    if parse.setOperations.is_some() || setops.is_some() {
        not_yet_reachable("grouping_planner: set operations");
    }

    // No set operations: regular planning.
    crate::assert!(!root.has_recursion);

    if !root.parse.groupingSets.is_empty() || !root.parse.groupClause.is_empty() {
        not_yet_reachable("grouping_planner: GROUP BY / grouping sets");
    }

    // Preprocess targetlist into root->processed_tlist.
    preprocess_targetlist(root);

    if root.parse.hasAggs {
        not_yet_reachable("grouping_planner: aggregates");
    }
    if root.parse.hasWindowFuncs {
        not_yet_reachable("grouping_planner: window functions");
    }

    root.limit_tuples = -1.0;

    // Generate the best paths for the scan/join portion of the query (the
    // FROM/WHERE processing). For a table-less SELECT this builds the dummy
    // result rel with its one Result path. qp_callback computes query pathkeys;
    // M1 has none.
    let current_rel = query_planner(root, standard_qp_callback);

    // Convert the result tlist into PathTarget form and stash the upper targets.
    // M1: jams the processed tlist into the result rel's reltarget directly and
    // skips apply_scanjoin_target_to_paths; PG applies the scan/join target to
    // paths in grouping_planner.
    // TODO(M2): route through apply_scanjoin_target_to_paths when the real
    // scan/join target machinery lands.
    if root.parse.sortClause.is_empty()
        && root.parse.distinctClause.is_empty()
        && root.parse.windowClause.is_empty()
    {
        // final_target == scanjoin_target == the rel's reltarget (the const tlist).
        let final_target = current_rel
            .reltarget
            .clone()
            .unwrap_or_else(|| not_yet_reachable("grouping_planner: missing reltarget"));
        for slot in &mut root.upper_targets {
            *slot = Some(final_target.clone());
        }
    } else {
        not_yet_reachable("grouping_planner: ORDER BY / DISTINCT / WINDOW targets");
    }

    // The final-output upper rel IS the scan/join rel for the const path (no
    // grouping/window/distinct/sort/limit upper rels added). PG copies the
    // pathlist into UPPERREL_FINAL with LockRows/Limit/ModifyTable steps as
    // needed; M1 needs none, so the scan/join rel is the final rel directly.
    if !root.parse.rowMarks.is_empty() || root.parse.commandType != CmdType::SELECT {
        not_yet_reachable("grouping_planner: LockRows / ModifyTable");
    }
    root.upper_rels[UpperRelationKind::FINAL as usize] = vec![Box::new(current_rel)];
}

/// PG `standard_qp_callback`: compute the query's sort/group/distinct/setop
/// pathkeys for query_planner. M1 has no ordering clauses, so all pathkey lists
/// stay empty; this is a no-op. Grows with ORDER BY / GROUP BY / DISTINCT.
fn standard_qp_callback(root: &mut PlannerInfo) {
    if !root.parse.sortClause.is_empty()
        || !root.parse.groupClause.is_empty()
        || !root.parse.distinctClause.is_empty()
    {
        not_yet_reachable("standard_qp_callback: pathkeys for ordering clauses");
    }
}

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
        assert!(stmt.rtable.is_empty(), "table-less SELECT has an empty rangetable");
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

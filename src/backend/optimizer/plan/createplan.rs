//! Routines to create the desired plan for processing a query. Translated from
//! backend/optimizer/plan/createplan.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s. The
//! public entry `create_plan` lives in planmain.rs (re-exported via
//! `crate::optimizer::planmain`); the recursion driver and the Result builder
//! live here.
//!
//! Disposition: `grow`. M1's live path is the Result plan for a FROM-less
//! SELECT: `create_plan_recurse` dispatches the path's `pathtype` and the
//! `T_Result` arm builds a childless `Result` from the path's pathtarget. The
//! scan/join/append/agg/sort/limit/... arms of the nodeTag switch are grow guards
//! (rules.md s4) and grow per milestone.

use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodes::{AggSplit, AggStrategy, Node};
use crate::nodes::pathnodes::{Path, PathType, PlannerInfo};
use crate::nodes::plannodes::{Agg, Limit, Plan, Result, Scan, SeqScan, Sort, Unique};

/// Panic for a createplan path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `create_plan_recurse`: recursively build a Plan from a Path. Dispatches on
/// the Path's pathtype (the NodeTag of the plan it builds). M1/M2 live the
/// `T_Result` and `T_SeqScan` arms; the rest grow per milestone. Returns the
/// polymorphic plan node.
pub fn create_plan_recurse(root: &mut PlannerInfo, best_path: &Path) -> Node {
    match best_path.pathtype {
        PathType::Result => {
            // PG distinguishes ProjectionPath / MinMaxAggPath / GroupResultPath /
            // simple RTE_RESULT scan here. For M1 the only Result path is the
            // group-result path of a FROM-less SELECT.
            Node::Result(Box::new(create_group_result_plan(root, best_path)))
        }
        PathType::SeqScan => Node::SeqScan(Box::new(create_seqscan_plan(root, best_path))),
        other => not_yet_reachable(&format!("create_plan_recurse: {other:?}")),
    }
}

/// PG `create_seqscan_plan`: build a `SeqScan` plan from a seqscan Path. The plan's
/// targetlist is built from the path's pathtarget (`build_path_tlist`); its qual is
/// the base rel's restriction clauses (the WHERE), stripped of their RestrictInfo
/// wrappers by `extract_actual_clauses`. The scanrelid is the base rel's RT index.
fn create_seqscan_plan(root: &mut PlannerInfo, best_path: &Path) -> SeqScan {
    let parent = best_path
        .parent
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_seqscan_plan: missing parent rel"));
    let scan_relid = parent.relid;
    crate::assert!(scan_relid > 0);

    if best_path.param_info.is_some() {
        not_yet_reachable("create_seqscan_plan: parameterized scan (nestloop params)");
    }

    // scan_clauses = rel->baserestrictinfo. Sort/qpqual reordering and
    // index-implied-clause removal grow later; M3 takes the per-tuple clauses.
    let scan_clauses: Vec<crate::nodes::pathnodes::RestrictInfo> =
        parent.baserestrictinfo.iter().map(|ri| (**ri).clone()).collect();
    let qual = crate::backend::optimizer::util::restrictinfo::extract_actual_clauses(
        &scan_clauses,
        false,
    );

    let tlist = build_path_tlist(root, best_path);
    let mut plan = make_seqscan(tlist, qual, scan_relid);
    copy_generic_path_info(&mut plan.scan.plan, best_path);
    plan
}

/// PG `make_seqscan`: construct a `SeqScan` plan node.
fn make_seqscan(tlist: Vec<Node>, qual: Vec<Node>, scanrelid: crate::nodes::primnodes::Index) -> SeqScan {
    SeqScan {
        scan: Scan {
            plan: empty_plan(tlist, qual),
            scanrelid,
        },
    }
}

/// A zero-default `Plan` (makeNode(Plan) semantics) carrying the given tlist+qual.
fn empty_plan(tlist: Vec<Node>, qual: Vec<Node>) -> Plan {
    Plan {
        disabled_nodes: 0,
        startup_cost: 0.0,
        total_cost: 0.0,
        plan_rows: 0.0,
        plan_width: 0,
        parallel_aware: false,
        parallel_safe: false,
        async_capable: false,
        plan_node_id: 0,
        targetlist: tlist,
        qual,
        lefttree: None,
        righttree: None,
        init_plan: Vec::new(),
        ext_param: None,
        all_param: None,
    }
}

/// PG `create_group_result_plan`: build a Result plan for a GroupResultPath. The
/// plan's targetlist comes from the path's pathtarget (`build_path_tlist`); the
/// quals become the one-time `resconstantqual`. M1 has no quals.
fn create_group_result_plan(root: &mut PlannerInfo, best_path: &Path) -> Result {
    let tlist = build_path_tlist(root, best_path);

    // best_path->quals are the GroupResultPath's bare clauses; M1 has none. The
    // skeleton stores the embedded Path in the rel pathlist (planmain), so the
    // quals (always empty on the const path) are not carried here.
    let quals: Option<Node> = None;

    let mut plan = make_result(tlist, quals, None);
    copy_generic_path_info(&mut plan.plan, best_path);
    plan
}

/// PG `build_path_tlist`: build a targetlist from a path's pathtarget, assigning
/// resnos 1..n. Parameterized-path lateral-ref replacement is not reachable on
/// the M1 path (no param_info).
fn build_path_tlist(_root: &mut PlannerInfo, path: &Path) -> Vec<Node> {
    if path.param_info.is_some() {
        not_yet_reachable("build_path_tlist: parameterized path lateral refs");
    }
    let pathtarget = path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("build_path_tlist: missing pathtarget"));
    let has_sortgrouprefs = !pathtarget.sortgrouprefs.is_empty();

    pathtarget
        .exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let mut tle = makeTargetEntry(Some(expr.clone()), (i + 1) as i16, None, false);
            if has_sortgrouprefs {
                tle.ressortgroupref = pathtarget.sortgrouprefs[i];
            }
            Node::TargetEntry(Box::new(tle))
        })
        .collect()
}

/// PG `make_result`: construct a Result plan node with the given tlist and
/// one-time qual (`resconstantqual`), over an optional subplan.
fn make_result(
    tlist: Vec<Node>,
    resconstantqual: Option<Node>,
    subplan: Option<Node>,
) -> Result {
    Result {
        plan: Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: tlist,
            qual: Vec::new(),
            lefttree: subplan,
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        },
        resconstantqual,
    }
}

// ===========================================================================
//  Upper plan construction (M5, step 26): Agg / Sort / Group / Unique / Limit.
//
//  PG builds these as Paths in grouping_planner and turns them into plan nodes via
//  create_agg_plan / create_sort_plan / ... ; with the port's flat Path they are
//  assembled directly here from the query's clauses over the already-built scan/join
//  plan. The child plan's output tlist is the scan-input tlist (the group/agg-input
//  Vars). The grouping/aggregation/sort/distinct/limit stages are layered bottom-up:
//    scan -> [Sort(group keys) ->] Agg(SORTED|PLAIN) -> [Sort(ORDER BY) ->]
//            [Unique(DISTINCT) ->] [Limit].
// ===========================================================================

/// Assemble the upper (grouping/aggregation/distinct/sort/limit) plan over the
/// scan/join `subplan`. Reads the query's clauses + the final `processed_tlist`.
/// Returns the topmost plan node (or `subplan` unchanged when no upper stage).
pub fn build_upper_plan(root: &PlannerInfo, subplan: Node) -> Node {
    let parse = &root.parse;
    let has_grouping = parse.hasAggs || !parse.groupClause.is_empty();
    let has_distinct = !parse.distinctClause.is_empty();
    let has_sort = !parse.sortClause.is_empty();
    let has_limit = parse.limitCount.is_some() || parse.limitOffset.is_some();

    if !has_grouping && !has_distinct && !has_sort && !has_limit {
        return subplan;
    }

    // The child (scan) output tlist: the scan-input tlist (resnos 1..n over the
    // group/agg-input Vars). Upper nodes reference columns by these positions.
    let mut plan = subplan;

    // 1) Grouping / aggregation.
    if has_grouping {
        plan = create_agg_plan(root, plan);
    }

    // 2) ORDER BY (a Sort over the current plan's output).
    if has_sort {
        let keys = sort_keys_from_clause(&root.parse.sortClause, current_tlist(&plan));
        plan = Node::Sort(Box::new(make_sort(plan, keys)));
    }

    // 3) DISTINCT (a Unique over a sorted input; the milestone DISTINCT input is
    //    sorted by a Sort on the distinct columns).
    if has_distinct {
        plan = create_unique_plan(root, plan);
    }

    // 4) LIMIT / OFFSET.
    if has_limit {
        plan = Node::Limit(Box::new(make_limit(
            plan,
            root.parse.limitOffset.clone(),
            root.parse.limitCount.clone(),
            root.parse.limitOption,
        )));
    }

    plan
}

/// PG `create_agg_plan` + `make_agg` (M5 subset): build an `Agg` over `subplan`. The
/// strategy is AGG_PLAIN for whole-table aggregation (no GROUP BY) and AGG_SORTED
/// otherwise, with a `Sort` on the grouping columns inserted below. The Agg's tlist
/// is the final `processed_tlist` (Vars + Aggrefs); `grpColIdx`/`grpOperators` come
/// from the group clause resolved against the child's output columns.
fn create_agg_plan(root: &PlannerInfo, subplan: Node) -> Node {
    let group_clause = root.parse.groupClause.clone();
    let final_tlist = root.processed_tlist.clone();

    let (strategy, child) = if group_clause.is_empty() {
        (AggStrategy::PLAIN, subplan)
    } else {
        // AGG_SORTED: sort the child on the grouping columns first.
        let keys = sort_keys_from_clause(&group_clause, current_tlist(&subplan));
        let sort = Node::Sort(Box::new(make_sort(subplan, keys)));
        (AggStrategy::SORTED, sort)
    };

    // grpColIdx = the child output positions of the grouping columns; grpOperators =
    // the SortGroupClause eqops; grpCollations = InvalidOid (no collation tracking in
    // M5's int/text grouping).
    let child_tlist = current_tlist(&child);
    let mut grp_col_idx = Vec::new();
    let mut grp_operators = Vec::new();
    let mut grp_collations = Vec::new();
    for gc in &group_clause {
        let Node::SortGroupClause(sgc) = gc else { continue };
        let colpos = child_col_for_sortgroupref(child_tlist, sgc.tleSortGroupRef);
        grp_col_idx.push(colpos);
        grp_operators.push(sgc.eqop);
        grp_collations.push(crate::postgres_ext::InvalidOid);
    }

    let num_cols = i32::try_from(grp_col_idx.len()).unwrap_or(0);
    let agg = Agg {
        plan: Plan {
            lefttree: Some(child),
            ..empty_plan(final_tlist, Vec::new())
        },
        aggstrategy: strategy,
        aggsplit: AggSplit::SIMPLE,
        num_cols,
        grp_col_idx,
        grp_operators,
        grp_collations,
        num_groups: 0,
        transition_space: 0,
        agg_params: None,
        grouping_sets: Vec::new(),
        chain: Vec::new(),
    };
    Node::Agg(Box::new(agg))
}

/// PG `create_distinct_paths` + `create_upper_unique_plan` (M5 subset): a `Unique`
/// over a `Sort` on the distinct columns. The distinct columns are every column of
/// the (already-final) tlist; the Sort orders the input so adjacent duplicates are
/// detected by Unique.
fn create_unique_plan(root: &PlannerInfo, subplan: Node) -> Node {
    let distinct_clause = root.parse.distinctClause.clone();

    // Sort the input on the distinct columns (their sortop), then Unique on the eqop.
    let sort_keys = sort_keys_from_clause(&distinct_clause, current_tlist(&subplan));
    let sorted = Node::Sort(Box::new(make_sort(subplan, sort_keys)));

    let sorted_tlist = current_tlist(&sorted);
    let mut uniq_col_idx = Vec::new();
    let mut uniq_operators = Vec::new();
    let mut uniq_collations = Vec::new();
    for dc in &distinct_clause {
        let Node::SortGroupClause(sgc) = dc else { continue };
        uniq_col_idx.push(child_col_for_sortgroupref(sorted_tlist, sgc.tleSortGroupRef));
        uniq_operators.push(sgc.eqop);
        uniq_collations.push(crate::postgres_ext::InvalidOid);
    }

    let num_cols = i32::try_from(uniq_col_idx.len()).unwrap_or(0);
    // The Unique's tlist is its child's tlist (a passthrough).
    let tlist = sorted_tlist.to_vec();
    let unique = Unique {
        plan: Plan { lefttree: Some(sorted), ..empty_plan(tlist, Vec::new()) },
        num_cols,
        uniq_col_idx,
        uniq_operators,
        uniq_collations,
    };
    Node::Unique(Box::new(unique))
}

/// PG `make_sort_from_sortclauses` (M5 subset): the per-key (col-index, sortop,
/// nulls_first) extracted from a SortGroupClause list (ORDER BY, or the implicit
/// ordering of a GROUP BY / DISTINCT clause), resolved against the child output
/// tlist by sortgroupref.
fn sort_keys_from_clause(sortcls: &[Node], child_tlist: &[Node]) -> Vec<SortKey> {
    sortcls
        .iter()
        .filter_map(|n| {
            let Node::SortGroupClause(sgc) = n else { return None };
            Some(SortKey {
                col: child_col_for_sortgroupref(child_tlist, sgc.tleSortGroupRef),
                sortop: sgc.sortop,
                nulls_first: sgc.nulls_first,
            })
        })
        .collect()
}

/// One resolved sort key: the child output column position (1-based), its ordering
/// operator, and the NULLS FIRST flag.
struct SortKey {
    col: crate::access::attnum::AttrNumber,
    sortop: crate::postgres_ext::Oid,
    nulls_first: bool,
}

/// PG `make_sort`: a `Sort` node over `subplan` with the given keys. The Sort's
/// tlist is its child's (a Sort never projects).
fn make_sort(subplan: Node, keys: Vec<SortKey>) -> Sort {
    let tlist = current_tlist(&subplan).to_vec();
    let num_cols = i32::try_from(keys.len()).unwrap_or(0);
    let mut sort_col_idx = Vec::with_capacity(keys.len());
    let mut sort_operators = Vec::with_capacity(keys.len());
    let mut collations = Vec::with_capacity(keys.len());
    let mut nulls_first = Vec::with_capacity(keys.len());
    for k in keys {
        sort_col_idx.push(k.col);
        sort_operators.push(k.sortop);
        collations.push(crate::postgres_ext::InvalidOid);
        nulls_first.push(k.nulls_first);
    }
    Sort {
        plan: Plan { lefttree: Some(subplan), ..empty_plan(tlist, Vec::new()) },
        num_cols,
        sort_col_idx,
        sort_operators,
        collations,
        nulls_first,
    }
}

/// PG `make_limit` (M5 subset): a `Limit` over `subplan` with the (already int8)
/// OFFSET/COUNT expressions. The Limit's tlist is its child's (a passthrough).
fn make_limit(
    subplan: Node,
    limit_offset: Option<Node>,
    limit_count: Option<Node>,
    limit_option: crate::nodes::nodes::LimitOption,
) -> Limit {
    let tlist = current_tlist(&subplan).to_vec();
    Limit {
        plan: Plan { lefttree: Some(subplan), ..empty_plan(tlist, Vec::new()) },
        limit_offset,
        limit_count,
        limit_option,
        uniq_num_cols: 0,
        uniq_col_idx: Vec::new(),
        uniq_operators: Vec::new(),
        uniq_collations: Vec::new(),
    }
}

/// The output targetlist of the given plan node (`Plan.targetlist`).
fn current_tlist(plan: &Node) -> &[Node] {
    match plan {
        Node::Result(r) => &r.plan.targetlist,
        Node::SeqScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        _ => not_yet_reachable("build_upper_plan: unexpected child plan node"),
    }
}

/// The child output column position (1-based) of the entry carrying `sortgroupref`.
fn child_col_for_sortgroupref(
    child_tlist: &[Node],
    sortgroupref: crate::c::Index,
) -> crate::access::attnum::AttrNumber {
    for n in child_tlist {
        if let Node::TargetEntry(te) = n
            && te.ressortgroupref == sortgroupref
        {
            return te.resno;
        }
    }
    not_yet_reachable("build_upper_plan: group/sort key not in child output");
}

/// PG `copy_generic_path_info`: copy the Path's cost/row/width/parallel info onto
/// the Plan node.
fn copy_generic_path_info(dest: &mut Plan, src: &Path) {
    dest.disabled_nodes = src.disabled_nodes;
    dest.startup_cost = src.startup_cost;
    dest.total_cost = src.total_cost;
    dest.plan_rows = src.rows;
    dest.plan_width = src
        .pathtarget
        .as_ref()
        .map_or(0, |t| t.width);
    dest.parallel_aware = src.parallel_aware;
    dest.parallel_safe = src.parallel_safe;
}

//! Planning of sub-selects (SubLinks / SubPlans / initplans). Translated from
//! backend/optimizer/plan/subselect.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::subselect` under the C names.
//!
//! Disposition: `grow`. M1 has no sublinks/subplans/initplans, so the SS_*
//! touch-points that standard_planner / subquery_planner call are near no-ops
//! over the plan tree. `ss_finalize_plan` (extParam/allParam computation) is only
//! invoked when Params exist, which never happens on the const path; it is a
//! pass-through here. The CTE/sublink-to-join conversions, correlation-var
//! replacement, and initplan machinery are grow guards (rules.md s4).

use crate::backend::optimizer::plan::planner::{plan_subquery, PlannedSubquery};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{CommonTableExpr, Query, RTEKind, SetOperationStmt};
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::plannodes::{CteScan, Plan, RecursiveUnion, Scan, WorkTableScan};
use crate::nodes::primnodes::{Index, Param, ParamKind, SubLink, SubLinkType, SubPlan, OUTER_VAR};
use crate::postgres_ext::{InvalidOid, Oid};

/// Panic for a subselect path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// A sub-select nested inside another sub-select is not yet supported (its inner
/// SubPlan would be discarded -> silently-wrong rows); raise a catchable error.
#[cold]
fn nested_sublink_error() -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("nested sub-selects are not yet supported".to_owned());
    });
    unreachable!("ereport(ERROR) diverges");
}

/// A planned CTE: its name + the finished subplan that produces its rows + the
/// subplan's flattened rangetable + its column types, plus (for a recursive CTE)
/// the working-table param id. The host body's CteScan/WorkTableScan references
/// are resolved to these.
pub struct CtePlan {
    pub name: String,
    pub recursive: bool,
    pub wt_param: i32,
    pub plan: Node,
    pub rtable: Vec<Node>,
    pub perminfos: Vec<Node>,
    pub col_types: Vec<Oid>,
}

/// The result of planning a WITH query: the host body plan + the combined
/// rangetable (host rtable ++ each CTE subplan's rtable, offset).
pub struct WithPlan {
    pub plan: Node,
    pub rtable: Vec<Node>,
    pub perminfos: Vec<Node>,
}

/// Monotonic source of working-table param ids for recursive CTEs (PG
/// `glob->lastPlanNodeId`-adjacent; a simple per-process counter suffices here
/// because the id only needs to be unique within one plan's EState worktables).
static WT_PARAM_COUNTER: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

fn next_wt_param() -> i32 {
    WT_PARAM_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// PG `SS_process_ctes`: plan each CTE in the host query's `cteList` into a finished
/// subplan. A non-recursive CTE plans straight through (`plan_subquery`); a recursive
/// CTE plans its non-recursive + recursive terms and wraps them in a RecursiveUnion
/// (the recursive term's self-reference is a WorkTableScan reading the shared working
/// table by `wt_param`).
pub fn ss_process_ctes(cte_list: &[Node]) -> Vec<CtePlan> {
    cte_list
        .iter()
        .map(|n| {
            let Node::CommonTableExpr(cte) = n else {
                not_yet_reachable("SS_process_ctes: cteList entry is not a CommonTableExpr");
            };
            plan_one_cte(cte)
        })
        .collect()
}

/// Plan one CTE into a `CtePlan`.
fn plan_one_cte(cte: &CommonTableExpr) -> CtePlan {
    let name = cte.ctename.clone().unwrap_or_default();
    let Some(Node::Query(ctequery)) = cte.ctequery.as_ref() else {
        not_yet_reachable("SS_process_ctes: CTE body is not an analyzed Query");
    };

    if cte.cterecursive {
        plan_recursive_cte(&name, ctequery)
    } else if let Some(Node::SetOperationStmt(sostmt)) = ctequery.setOperations.as_ref() {
        // A non-recursive CTE whose body is a set operation (e.g. a `WITH RECURSIVE`
        // CTE that turned out not to self-reference): plan it through the same set-op
        // machinery the top-level UNION/INTERSECT/EXCEPT path uses.
        let result = crate::backend::optimizer::prep::prepunion::plan_set_operations(
            sostmt,
            &ctequery.targetList,
        );
        CtePlan {
            name,
            recursive: false,
            wt_param: -1,
            plan: result.plan,
            rtable: result.rtable,
            perminfos: result.perminfos,
            col_types: result.col_types,
        }
    } else {
        let mut body = (**ctequery).clone();
        let PlannedSubquery { plan, rtable, perminfos } = plan_subquery(&mut body, -1);
        let col_types = plan_output_types(&plan);
        CtePlan { name, recursive: false, wt_param: -1, plan, rtable, perminfos, col_types }
    }
}

/// Plan a recursive CTE: the body is a `SELECT ... UNION [ALL] SELECT ...` Query whose
/// `setOperations` is a SetOperationStmt over the non-recursive term (larg) + the
/// recursive term (rarg, embedded `Node::Query` leaves). The recursive term's
/// `FROM cte` is a self-reference CTE RTE -- rewritten to a WorkTableScan reading the
/// shared working table. The two terms become the RecursiveUnion's children.
fn plan_recursive_cte(name: &str, ctequery: &Query) -> CtePlan {
    let Some(Node::SetOperationStmt(sostmt)) = ctequery.setOperations.as_ref() else {
        not_yet_reachable("plan_recursive_cte: recursive CTE body has no SetOperationStmt");
    };
    let SetOperationStmt { all, larg, rarg, colTypes, .. } = &**sostmt;
    let Some(Node::Query(nr_query)) = larg.as_ref() else {
        not_yet_reachable("plan_recursive_cte: non-recursive term is not a Query");
    };
    let Some(Node::Query(r_query)) = rarg.as_ref() else {
        not_yet_reachable("plan_recursive_cte: recursive term is not a Query");
    };

    let wt_param = next_wt_param();

    // Non-recursive term: a plain subplan.
    let mut nr = (**nr_query).clone();
    let nr_planned = plan_subquery(&mut nr, -1);

    // Recursive term: a WorkTableScan reading the shared working table (wt_param), with
    // the term's projection (e.g. `n+1`) + filter (e.g. `n<5`). The recursive term's
    // own rangetable (the self-reference CTE RTE for the working table) lands after the
    // non-recursive term's, so the WorkTableScan's scanrelid (and its tlist/qual Vars)
    // are offset past it.
    let col_types = colTypes.clone();
    let offset = i32::try_from(nr_planned.rtable.len()).unwrap_or(0);
    let (rec_plan, rec_rtable) = build_recursive_term(r_query, wt_param, offset);

    let mut rtable = nr_planned.rtable;
    let mut rec_rtable = rec_rtable;
    rtable.append(&mut rec_rtable);
    let perminfos = nr_planned.perminfos;
    let _ = name;
    let ncols = col_types.len();
    // UNION (not ALL) deduplicates; the RecursiveUnion dedups on all columns when
    // num_cols > 0.
    let num_cols = if *all { 0 } else { i32::try_from(ncols).unwrap_or(0) };

    let tlist = passthrough_tlist(&col_types);
    let ru = RecursiveUnion {
        plan: Plan {
            lefttree: Some(nr_planned.plan),
            righttree: Some(rec_plan),
            ..blank_plan(tlist)
        },
        wt_param,
        num_cols,
        dup_col_idx: (1..=ncols as i16).collect(),
        dup_operators: col_types.iter().map(|&t| eq_op(t)).collect(),
        dup_collations: vec![InvalidOid; ncols],
        num_groups: 0,
    };

    CtePlan {
        name: name.to_string(),
        recursive: true,
        wt_param,
        plan: Node::RecursiveUnion(Box::new(ru)),
        rtable,
        perminfos,
        col_types,
    }
}

/// Build the recursive term's plan: a `WorkTableScan` that reads the shared working
/// table (`wt_param`), projecting the term's target list (e.g. `n+1`) and filtering
/// by its WHERE qual (e.g. `n<5`). The term is the canonical `SELECT <exprs> FROM cte
/// WHERE <qual>` over the single self-reference CTE RTE (the working table). Its
/// scanrelid + Vars are offset by `offset` so they land in the combined rangetable
/// after the non-recursive term's. Returns the WorkTableScan node + the recursive
/// term's rangetable (the self-reference CTE RTE).
fn build_recursive_term(r_query: &Query, wt_param: i32, offset: i32) -> (Node, Vec<Node>) {
    // The recursive term must be a single self-reference CTE scan.
    let from = host_fromlist(r_query);
    if from.len() != 1 {
        not_yet_reachable("plan_recursive_cte: recursive term references more than one relation");
    }
    let Node::RangeTblRef(rtr) = &from[0] else {
        not_yet_reachable("plan_recursive_cte: recursive term FROM item is not a RangeTblRef");
    };
    let rti = rtr.rtindex;
    let Node::RangeTblEntry(rte) = &r_query.rtable[(rti - 1) as usize] else {
        not_yet_reachable("plan_recursive_cte: recursive term RTE is not an RTE");
    };
    if rte.rtekind != RTEKind::CTE || !rte.self_reference {
        not_yet_reachable("plan_recursive_cte: recursive term is not a self-reference CTE scan");
    }

    // The WorkTableScan's tlist = the term's target list (the projection), qual = the
    // term's WHERE. Both reference the CTE RTE's columns (varno = rti); offset the
    // scanrelid + Vars into the combined rangetable.
    let mut tlist = r_query.targetList.clone();
    let mut qual = host_qual(r_query);
    let off = offset;
    offset_exprs(&mut tlist, off);
    offset_exprs(&mut qual, off);

    let scanrelid = usize::try_from(rti + off).unwrap_or(0);
    let wts = WorkTableScan {
        scan: Scan {
            plan: Plan { qual, ..blank_plan(tlist) },
            scanrelid,
        },
        wt_param,
    };

    // The recursive term's rangetable (the self-reference CTE RTE for the worktable).
    let rec_rtable = r_query.rtable.clone();

    (Node::WorkTableScan(Box::new(wts)), rec_rtable)
}

/// Add `offset` to the varno of every base-rel Var in an expression list (the
/// recursive term's tlist/qual Vars reference the self-reference CTE RTE).
fn offset_exprs(exprs: &mut [Node], offset: i32) {
    for e in exprs.iter_mut() {
        offset_expr(e, offset);
    }
}

fn offset_expr(expr: &mut Node, offset: i32) {
    match expr {
        Node::Var(v) => {
            if v.varlevelsup == 0 && v.varno > 0 {
                v.varno += offset;
                if v.varnosyn > 0 {
                    v.varnosyn += offset as Index;
                }
            }
        }
        Node::TargetEntry(t) => {
            if let Some(e) = t.expr.as_mut() {
                offset_expr(e, offset);
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &mut o.args {
                offset_expr(a, offset);
            }
        }
        Node::BoolExpr(b) => {
            for a in &mut b.args {
                offset_expr(a, offset);
            }
        }
        Node::FuncExpr(f) => {
            for a in &mut f.args {
                offset_expr(a, offset);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_mut() {
                offset_expr(a, offset);
            }
        }
        _ => {}
    }
}

/// The host/recursive-term query's WHERE qual as an implicit-AND list.
fn host_qual(parse: &Query) -> Vec<Node> {
    match parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => crate::backend::nodes::makefuncs::make_ands_implicit(f.quals.clone()),
        _ => Vec::new(),
    }
}

/// PG `plan_set_operations` host body for a WITH query: plan each CTE
/// (`ss_process_ctes`), then plan the host body, resolving each CTE-RTE scan to a
/// CteScan over the matching CTE subplan. The combined rangetable is the host rtable
/// (the CTE RTEs) ++ each CTE subplan's rtable (offset). The milestone host body is a
/// passthrough `SELECT <cols> FROM cte` (optionally referenced more than once); the
/// CteScan materializes + serves the CTE rows.
pub fn plan_with_query(parse: &mut Query) -> WithPlan {
    let planned = ss_process_ctes(&parse.cteList);

    // The host body references the CTEs (each a CteScan); the combined rangetable is
    // built incrementally as each CTE-scan leaf is emitted (its own host CTE RTE + the
    // CTE subplan's rtable). `next_off` tracks the running combined rtable length so
    // each leaf's plan + RTEs land at the right offset.
    let mut ctx = HostCtx {
        planned: &planned,
        rtable: Vec::new(),
        perminfos: Vec::new(),
    };

    let host_plan = if let Some(Node::SetOperationStmt(so)) = parse.setOperations.as_ref() {
        // A set-op over CTE scans: recurse the tree, each leaf a `SELECT * FROM cte`.
        build_setop_host(&mut ctx, so, &parse.targetList)
    } else {
        build_cte_scan_host(&mut ctx, parse)
    };

    parse.rtable = ctx.rtable.clone();
    parse.rteperminfos = ctx.perminfos.clone();
    WithPlan { plan: host_plan, rtable: ctx.rtable, perminfos: ctx.perminfos }
}

/// Mutable context threaded while building the host body: the planned CTEs + the
/// combined rangetable/perminfos accumulated as CteScan leaves are emitted.
struct HostCtx<'a> {
    planned: &'a [CtePlan],
    rtable: Vec<Node>,
    perminfos: Vec<Node>,
}

/// Build the host body for a plain `SELECT <cols> FROM cte`: one CteScan over the
/// referenced CTE.
fn build_cte_scan_host(ctx: &mut HostCtx<'_>, parse: &Query) -> Node {
    let fromlist = host_fromlist(parse);
    if fromlist.len() != 1 {
        not_yet_reachable("plan_with_query: host body references more than one relation");
    }
    let Node::RangeTblRef(rtr) = &fromlist[0] else {
        not_yet_reachable("plan_with_query: host FROM item is not a RangeTblRef");
    };
    let Node::RangeTblEntry(rte) = &parse.rtable[(rtr.rtindex - 1) as usize] else {
        not_yet_reachable("plan_with_query: host RTE is not an RTE");
    };
    emit_cte_scan(ctx, rte)
}

/// Build a set-op host body whose leaves are CTE scans (each `SELECT * FROM cte`).
fn build_setop_host(
    ctx: &mut HostCtx<'_>,
    so: &SetOperationStmt,
    top_tlist: &[Node],
) -> Node {
    let plan = combine_setop_over_ctes(ctx, so);
    // Re-label the combined node's tlist with the result column names.
    let mut plan = plan;
    relabel_tlist(&mut plan, top_tlist);
    plan
}

/// Recurse a set-op tree whose leaves are CTE-scan Querys, combining via the same
/// Append / Append+Unique / SetOp shapes prepunion uses.
fn combine_setop_over_ctes(ctx: &mut HostCtx<'_>, so: &SetOperationStmt) -> Node {
    let left = setop_arm_over_ctes(ctx, so.larg.as_ref());
    let right = setop_arm_over_ctes(ctx, so.rarg.as_ref());
    crate::backend::optimizer::prep::prepunion::combine_setop_branches(
        so.op, so.all, left, right, &so.colTypes,
    )
}

/// Plan one arm of a CTE-scan set-op tree: a leaf `SELECT * FROM cte` -> a CteScan;
/// a nested set-op -> recurse.
fn setop_arm_over_ctes(ctx: &mut HostCtx<'_>, arm: Option<&Node>) -> Node {
    match arm {
        Some(Node::Query(q)) => {
            let fromlist = host_fromlist(q);
            if fromlist.len() != 1 {
                not_yet_reachable("plan_with_query: set-op CTE leaf references more than one relation");
            }
            let Node::RangeTblRef(rtr) = &fromlist[0] else {
                not_yet_reachable("plan_with_query: set-op CTE leaf FROM item is not a RangeTblRef");
            };
            let Node::RangeTblEntry(rte) = &q.rtable[(rtr.rtindex - 1) as usize] else {
                not_yet_reachable("plan_with_query: set-op CTE leaf RTE is not an RTE");
            };
            emit_cte_scan(ctx, rte)
        }
        Some(Node::SetOperationStmt(so)) => combine_setop_over_ctes(ctx, so),
        _ => not_yet_reachable("plan_with_query: set-op arm is not a Query / SetOperationStmt"),
    }
}

/// Emit a CteScan for a CTE RTE: allocate its host RTE slot in the combined
/// rangetable, append the CTE subplan's rtable (offset), and build the CteScan over
/// the (offset) subplan with the right scanrelid.
fn emit_cte_scan(ctx: &mut HostCtx<'_>, rte: &crate::nodes::parsenodes::RangeTblEntry) -> Node {
    if rte.rtekind != RTEKind::CTE {
        not_yet_reachable("plan_with_query: host body FROM item is not a CTE reference");
    }
    let ctename = rte.ctename.clone().unwrap_or_default();
    let cp = ctx
        .planned
        .iter()
        .find(|c| c.name == ctename)
        .unwrap_or_else(|| not_yet_reachable("plan_with_query: host references an unknown CTE"));

    // This CteScan's host RTE slot is the next combined rtable index.
    let scanrelid = (ctx.rtable.len() + 1) as Index;
    ctx.rtable.push(Node::RangeTblEntry(Box::new(rte.clone())));

    // The CTE subplan's rtable lands after this RTE slot; offset the subplan into it.
    let sub_offset = i32::try_from(ctx.rtable.len()).unwrap_or(0);
    let mut sub_plan = cp.plan.clone();
    crate::backend::optimizer::prep::prepunion::offset_plan_rt_indices_pub(&mut sub_plan, sub_offset);
    ctx.rtable.extend(cp.rtable.clone());
    ctx.perminfos.extend(cp.perminfos.clone());

    let tlist = passthrough_tlist(&cp.col_types);
    let scan = CteScan {
        scan: Scan {
            plan: Plan { lefttree: Some(sub_plan), ..blank_plan(tlist) },
            scanrelid,
        },
        cte_plan_id: 0,
        cte_param: cp.wt_param,
    };
    Node::CteScan(Box::new(scan))
}

/// Re-label a combined node's top targetlist with the result column names from the
/// host Query's tlist.
fn relabel_tlist(plan: &mut Node, top_tlist: &[Node]) {
    let names: Vec<Option<String>> = top_tlist
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(t) if !t.resjunk => Some(t.resname.clone()),
            _ => None,
        })
        .collect();
    let tlist = match plan {
        Node::Append(a) => &mut a.plan.targetlist,
        Node::SetOp(s) => &mut s.plan.targetlist,
        Node::Unique(u) => &mut u.plan.targetlist,
        Node::CteScan(c) => &mut c.scan.plan.targetlist,
        _ => return,
    };
    for (te, name) in tlist.iter_mut().zip(names) {
        if let Node::TargetEntry(t) = te {
            t.resname = name;
        }
    }
}

/// The host query's jointree fromlist.
fn host_fromlist(parse: &Query) -> Vec<Node> {
    match parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => f.fromlist.clone(),
        _ => Vec::new(),
    }
}

/// A passthrough targetlist (an OUTER_VAR Var per column) for a node that forwards
/// its child's tuples unchanged.
fn passthrough_tlist(col_types: &[Oid]) -> Vec<Node> {
    col_types
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let var = crate::backend::nodes::makefuncs::make_var(
                OUTER_VAR,
                (i + 1) as i16,
                t,
                -1,
                InvalidOid,
                0,
            );
            Node::TargetEntry(Box::new(crate::backend::nodes::makefuncs::make_target_entry(
                Some(Node::Var(Box::new(var))),
                (i + 1) as i16,
                None,
                false,
            )))
        })
        .collect()
}

/// The per-column output types of a finished plan, read from its plan targetlist.
fn plan_output_types(plan: &Node) -> Vec<Oid> {
    plan_tlist(plan)
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(t) if !t.resjunk => {
                Some(t.expr.as_ref().map_or(InvalidOid, crate::backend::nodes::nodeFuncs::exprType))
            }
            _ => None,
        })
        .collect()
}

fn plan_tlist(plan: &Node) -> &[Node] {
    match plan {
        Node::Result(r) => &r.plan.targetlist,
        Node::SeqScan(s) => &s.scan.plan.targetlist,
        Node::IndexScan(s) => &s.scan.plan.targetlist,
        Node::IndexOnlyScan(s) => &s.scan.plan.targetlist,
        Node::BitmapHeapScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        Node::Append(a) => &a.plan.targetlist,
        Node::SetOp(s) => &s.plan.targetlist,
        Node::CteScan(c) => &c.scan.plan.targetlist,
        Node::RecursiveUnion(r) => &r.plan.targetlist,
        Node::NestLoop(n) => &n.join.plan.targetlist,
        Node::MergeJoin(m) => &m.join.plan.targetlist,
        Node::HashJoin(h) => &h.join.plan.targetlist,
        other => not_yet_reachable(&format!("plan_output_types: {other:?}")),
    }
}

/// A blank Plan carrying `tlist`.
fn blank_plan(tlist: Vec<Node>) -> Plan {
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
        qual: Vec::new(),
        lefttree: None,
        righttree: None,
        init_plan: Vec::new(),
        ext_param: None,
        all_param: None,
    }
}

fn eq_op(typ: Oid) -> Oid {
    crate::backend::parser::parse_oper::opername_get_oprid("=", typ, typ)
}

/// PG `SS_finalize_plan`: compute the extParam/allParam sets for every node of a
/// finished plan tree. The port's single-level SubPlan execution does not consult
/// extParam/allParam (each SubPlan run-state carries its own parParam/setParam), so
/// this is a no-op that leaves the per-node Param sets unset. (When deeper
/// initplan-attachment / multi-level finalize is needed, this grows.)
pub fn ss_finalize_plan(_root: &mut PlannerInfo, _plan: &mut Node) {}

// ===========================================================================
//  SubLink -> SubPlan conversion (M12, step 44). PG: subselect.c
//  SS_process_sublinks / make_subplan / build_subplan / SS_replace_correlation_vars
//  / convert_testexpr. The port plans each sub-select as a self-contained subplan
//  (its own rangetable, offset into glob->finalrtable) registered in glob->subplans,
//  and replaces each SubLink in the outer qual/tlist with a PARAM_EXEC Param whose
//  slot the SubPlan fills at run time.
// ===========================================================================

/// PG `SS_process_sublinks`: walk an expression, converting every SubLink it
/// contains into a SubPlan (registered on `root.glob`) and returning the rewritten
/// expression (the SubLink replaced by the SubPlan's output Param, or for a
/// correlated SubPlan, a `Node::SubPlan` whose execution yields the boolean/scalar).
pub fn ss_process_sublinks(root: &mut PlannerInfo, node: Node, _is_qual: bool) -> Node {
    process_sublinks_mutator(root, node)
}

fn process_sublinks_mutator(root: &mut PlannerInfo, node: Node) -> Node {
    match node {
        Node::SubLink(sl) => make_subplan(root, *sl),
        Node::BoolExpr(mut b) => {
            b.args = b.args.into_iter().map(|a| process_sublinks_mutator(root, a)).collect();
            Node::BoolExpr(b)
        }
        Node::OpExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| process_sublinks_mutator(root, a)).collect();
            Node::OpExpr(o)
        }
        Node::NullIfExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| process_sublinks_mutator(root, a)).collect();
            Node::NullIfExpr(o)
        }
        Node::FuncExpr(mut f) => {
            f.args = f.args.into_iter().map(|a| process_sublinks_mutator(root, a)).collect();
            Node::FuncExpr(f)
        }
        Node::DistinctExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| process_sublinks_mutator(root, a)).collect();
            Node::DistinctExpr(o)
        }
        Node::RelabelType(mut r) => {
            r.arg = r.arg.map(|a| process_sublinks_mutator(root, a));
            Node::RelabelType(r)
        }
        Node::TargetEntry(mut t) => {
            t.expr = t.expr.map(|e| process_sublinks_mutator(root, e));
            Node::TargetEntry(t)
        }
        other => other,
    }
}

/// PG `make_subplan` + `build_subplan` (the port's focused subset): plan the
/// SubLink's analyzed sub-Query into a self-contained subplan, classify it as an
/// uncorrelated InitPlan or a correlated SubPlan, allocate its output/correlation
/// PARAM_EXEC slots, build the SubPlan node, register it on `root.glob`, and return
/// the replacement expression for the outer tree.
fn make_subplan(root: &mut PlannerInfo, sublink: SubLink) -> Node {
    let SubLink { subLinkType, testexpr, subselect, .. } = sublink;
    let Some(Node::Query(subquery)) = subselect else {
        not_yet_reachable("make_subplan: sub-select was not analyzed into a Query");
    };
    let mut subquery = *subquery;

    // A sub-select that itself contains a SubLink would plan its own subplans onto a
    // throwaway inner PlannerGlobal (inside plan_subquery), which is discarded here --
    // the inner SubPlan's PARAM_EXEC output would stay NULL at run time (silently wrong
    // rows). Reject loudly until nested-subplan attachment lands (a later milestone).
    if subquery.hasSubLinks {
        nested_sublink_error();
    }

    // Replace correlation Vars (varlevelsup > 0) in the sub-Query with PARAM_EXEC
    // Params, collecting (paramid, outerVar) pairs into plan_params. After this the
    // sub-Query references only its own (level-0) rangetable.
    let mut plan_params: Vec<(i32, Node)> = Vec::new();
    replace_correlation_vars_query(root, &mut subquery, &mut plan_params);

    // Plan the (now self-contained) sub-Query. Its rtable is 1..k local; offset its
    // scan RT indices into the OUTER query's rangetable (parse.rtable) and append its
    // rtable there. set_plan_references later flattens parse.rtable (outer + subplan
    // RTEs) into glob.finalrtable with rtoffset 0; the subplan plan trees are offset
    // here so their scan nodes index those final positions directly.
    let PlannedSubquery { mut plan, rtable, perminfos } = plan_subquery(&mut subquery, -1);
    let rt_offset = i32::try_from(root.parse.rtable.len()).unwrap_or(0);
    crate::backend::optimizer::prep::prepunion::offset_plan_rt_indices_pub(&mut plan, rt_offset);
    root.parse.rtable.extend(rtable);
    root.parse.rteperminfos.extend(perminfos);

    // The first non-junk output column type (PG get_first_col_type).
    let (first_col_type, first_col_typmod, first_col_collation) = first_col_type(&plan);

    // Register the subplan; its plan_id is its 1-based index in glob.subplans.
    root.glob.subplans.push(plan);
    let plan_id = i32::try_from(root.glob.subplans.len()).unwrap_or(0);

    let par_param: Vec<i32> = plan_params.iter().map(|(id, _)| *id).collect();
    let args: Vec<Node> = plan_params.into_iter().map(|(_, v)| v).collect();
    let is_correlated = !par_param.is_empty();

    let mut splan = SubPlan {
        subLinkType,
        testexpr: None,
        paramIds: Vec::new(),
        plan_id,
        plan_name: Some(format!(
            "{} {}",
            if is_correlated { "SubPlan" } else { "InitPlan" },
            plan_id
        )),
        firstColType: first_col_type,
        firstColTypmod: first_col_typmod,
        firstColCollation: first_col_collation,
        useHashTable: false,
        unknownEqFalse: false,
        parallel_safe: false,
        setParam: Vec::new(),
        parParam: par_param,
        args,
        startup_cost: 0.0,
        per_call_cost: 0.0,
    };

    match subLinkType {
        SubLinkType::EXISTS_SUBLINK if !is_correlated => {
            // Uncorrelated EXISTS -> InitPlan: a single boolean output Param.
            let prm = generate_new_exec_param(root, BOOLOID());
            splan.setParam = vec![prm.paramid];
            register_init_plan(root, splan);
            Node::Param(Box::new(prm))
        }
        SubLinkType::EXPR_SUBLINK if !is_correlated => {
            // Uncorrelated scalar -> InitPlan: one output Param of the column's type.
            let prm = generate_new_exec_param(root, first_col_type);
            splan.setParam = vec![prm.paramid];
            register_init_plan(root, splan);
            Node::Param(Box::new(prm))
        }
        SubLinkType::EXISTS_SUBLINK | SubLinkType::EXPR_SUBLINK => {
            // Correlated EXISTS/EXPR -> SubPlan: a per-outer-row boolean/scalar output
            // Param the SubPlan run-state fills. Mark setParam (its single output).
            let out_type =
                if subLinkType == SubLinkType::EXISTS_SUBLINK { BOOLOID() } else { first_col_type };
            let prm = generate_new_exec_param(root, out_type);
            splan.setParam = vec![prm.paramid];
            root.glob.subplan_nodes.push(splan);
            Node::Param(Box::new(prm))
        }
        SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => {
            // ANY/ALL: build the testexpr with PARAM_SUBLINK placeholders replaced by
            // fresh PARAM_EXEC params (one per subquery output column = paramIds). The
            // SubPlan combines the per-row testexpr results (3-valued OR/AND) into a
            // single boolean output Param.
            let testexpr = testexpr
                .unwrap_or_else(|| not_yet_reachable("make_subplan: ANY/ALL without testexpr"));
            let (testexpr, param_ids) = convert_testexpr(root, testexpr, &plan_output_types(get_subplan(root, plan_id)));
            splan.testexpr = Some(testexpr);
            splan.paramIds = param_ids;
            // The combined boolean result goes in a fresh output Param.
            let prm = generate_new_exec_param(root, BOOLOID());
            splan.setParam = vec![prm.paramid];
            root.glob.subplan_nodes.push(splan);
            Node::Param(Box::new(prm))
        }
        other => not_yet_reachable(&format!("make_subplan: subLinkType {other:?}")),
    }
}

/// Register a SubPlan as an InitPlan (uncorrelated, runs once). Stored on
/// `root.glob` alongside the correlated SubPlans (the executor distinguishes them by
/// `parParam` being empty).
fn register_init_plan(root: &mut PlannerInfo, splan: SubPlan) {
    root.glob.subplan_nodes.push(splan);
}

fn get_subplan(root: &PlannerInfo, plan_id: i32) -> &Node {
    &root.glob.subplans[(plan_id - 1) as usize]
}

/// The first non-junk output column's (type, typmod, collation) (PG get_first_col_type).
fn first_col_type(plan: &Node) -> (Oid, i32, Oid) {
    for n in plan_tlist(plan) {
        if let Node::TargetEntry(t) = n {
            if t.resjunk {
                continue;
            }
            if let Some(e) = t.expr.as_ref() {
                use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
                return (exprType(e), exprTypmod(e), exprCollation(e));
            }
        }
    }
    (InvalidOid, -1, InvalidOid)
}

#[allow(non_snake_case)]
fn BOOLOID() -> Oid {
    crate::catalog::genbki::BOOLOID
}

/// PG `generate_new_exec_param`: allocate a fresh PARAM_EXEC slot of `paramtype`,
/// returning a PARAM_EXEC Param referencing it. The slot index is the current length
/// of glob.param_exec_types (and that vector is extended).
fn generate_new_exec_param(root: &mut PlannerInfo, paramtype: Oid) -> Param {
    let paramid = i32::try_from(root.glob.param_exec_types.len()).unwrap_or(0);
    root.glob.param_exec_types.push(paramtype);
    Param {
        paramkind: ParamKind::EXEC,
        paramid,
        paramtype,
        paramtypmod: -1,
        paramcollid: InvalidOid,
        location: -1,
    }
}

/// PG `convert_testexpr`: replace the PARAM_SUBLINK placeholders (one per subquery
/// output column, paramid = column resno) in `testexpr` with fresh PARAM_EXEC params
/// (the SubPlan loads the subquery's current-row column values into these before
/// evaluating the testexpr). Returns the rewritten testexpr + the new param ids.
fn convert_testexpr(root: &mut PlannerInfo, testexpr: Node, col_types: &[Oid]) -> (Node, Vec<i32>) {
    // Allocate one PARAM_EXEC per output column (resno 1..n).
    let params: Vec<Param> = col_types
        .iter()
        .map(|&t| generate_new_exec_param(root, t))
        .collect();
    let param_ids = params.iter().map(|p| p.paramid).collect();
    (convert_testexpr_mutator(testexpr, &params), param_ids)
}

fn convert_testexpr_mutator(node: Node, params: &[Param]) -> Node {
    match node {
        Node::Param(p) if p.paramkind == ParamKind::SUBLINK => {
            // paramid is the subquery column resno (1-based).
            let idx = (p.paramid - 1) as usize;
            let np = params
                .get(idx)
                .unwrap_or_else(|| not_yet_reachable("convert_testexpr: PARAM_SUBLINK id out of range"));
            Node::Param(Box::new(np.clone()))
        }
        Node::OpExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| convert_testexpr_mutator(a, params)).collect();
            Node::OpExpr(o)
        }
        Node::BoolExpr(mut b) => {
            b.args = b.args.into_iter().map(|a| convert_testexpr_mutator(a, params)).collect();
            Node::BoolExpr(b)
        }
        Node::FuncExpr(mut f) => {
            f.args = f.args.into_iter().map(|a| convert_testexpr_mutator(a, params)).collect();
            Node::FuncExpr(f)
        }
        Node::RelabelType(mut r) => {
            r.arg = r.arg.map(|a| convert_testexpr_mutator(a, params));
            Node::RelabelType(r)
        }
        // A nested SubLink in the testexpr is left as-is (its own PARAM_SUBLINKs).
        other => other,
    }
}

/// PG `SS_replace_correlation_vars` (over a sub-Query): replace every Var with
/// `varlevelsup > 0` (a reference to an outer query level) with a PARAM_EXEC Param,
/// collecting (paramid, level-0 outer Var) into `plan_params`. The outer Var (with
/// varlevelsup decremented to 0) becomes the SubPlan's arg, evaluated in the parent.
fn replace_correlation_vars_query(
    root: &mut PlannerInfo,
    query: &mut Query,
    plan_params: &mut Vec<(i32, Node)>,
) {
    // Target list.
    let tlist = std::mem::take(&mut query.targetList);
    query.targetList = tlist
        .into_iter()
        .map(|n| replace_correlation_vars(root, n, plan_params))
        .collect();
    // WHERE qual (jointree).
    if let Some(Node::FromExpr(mut f)) = query.jointree.take() {
        f.quals = f.quals.map(|q| replace_correlation_vars(root, q, plan_params));
        query.jointree = Some(Node::FromExpr(f));
    }

    // PG replaces correlation Vars in havingQual / group / sort exprs too. Those
    // clauses are not yet rewritten here, so a correlation Var in HAVING/GROUP BY/
    // ORDER BY of the sub-select would be left un-Param'd: parParam stays empty and
    // the subplan is misclassified as a run-once InitPlan (stale for every outer row,
    // silently wrong). Reject loudly until those clauses are rewritten too.
    let mut has_uplevel = false;
    if let Some(h) = query.havingQual.as_ref() {
        contains_uplevel_var(h, &mut has_uplevel);
    }
    for n in query.groupClause.iter().chain(query.sortClause.iter()) {
        contains_uplevel_var(n, &mut has_uplevel);
    }
    if has_uplevel {
        correlated_having_error();
    }
}

/// Set `found` if `node` contains a Var with `varlevelsup > 0` (a correlation
/// reference to an outer query level).
fn contains_uplevel_var(node: &Node, found: &mut bool) {
    if *found {
        return;
    }
    match node {
        Node::Var(v) if v.varlevelsup > 0 => *found = true,
        Node::TargetEntry(t) => {
            if let Some(e) = t.expr.as_ref() {
                contains_uplevel_var(e, found);
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &o.args {
                contains_uplevel_var(a, found);
            }
        }
        Node::BoolExpr(b) => {
            for a in &b.args {
                contains_uplevel_var(a, found);
            }
        }
        Node::FuncExpr(f) => {
            for a in &f.args {
                contains_uplevel_var(a, found);
            }
        }
        Node::Aggref(a) => {
            for n in &a.args {
                contains_uplevel_var(n, found);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_ref() {
                contains_uplevel_var(a, found);
            }
        }
        _ => {}
    }
}

/// A correlation reference in the HAVING/GROUP BY/ORDER BY of a sub-select is not
/// yet supported (it would be misclassified as an uncorrelated InitPlan -> stale
/// result); raise a catchable error.
#[cold]
fn correlated_having_error() -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED).errmsg(
            "correlated reference in HAVING/GROUP BY/ORDER BY of a sub-select is not yet supported"
                .to_owned(),
        );
    });
    unreachable!("ereport(ERROR) diverges");
}

fn replace_correlation_vars(
    root: &mut PlannerInfo,
    node: Node,
    plan_params: &mut Vec<(i32, Node)>,
) -> Node {
    match node {
        Node::Var(v) if v.varlevelsup > 0 => {
            // Decrement the level so the arg is evaluated in the parent (level 0).
            let mut outer = (*v).clone();
            outer.varlevelsup -= 1;
            let paramid = assign_param_for_var(root, &outer, plan_params);
            Node::Param(Box::new(Param {
                paramkind: ParamKind::EXEC,
                paramid,
                paramtype: v.vartype,
                paramtypmod: v.vartypmod,
                paramcollid: v.varcollid,
                location: v.location,
            }))
        }
        Node::Var(v) => Node::Var(v),
        Node::TargetEntry(mut t) => {
            t.expr = t.expr.map(|e| replace_correlation_vars(root, e, plan_params));
            Node::TargetEntry(t)
        }
        Node::OpExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| replace_correlation_vars(root, a, plan_params)).collect();
            Node::OpExpr(o)
        }
        Node::NullIfExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| replace_correlation_vars(root, a, plan_params)).collect();
            Node::NullIfExpr(o)
        }
        Node::BoolExpr(mut b) => {
            b.args = b.args.into_iter().map(|a| replace_correlation_vars(root, a, plan_params)).collect();
            Node::BoolExpr(b)
        }
        Node::FuncExpr(mut f) => {
            f.args = f.args.into_iter().map(|a| replace_correlation_vars(root, a, plan_params)).collect();
            Node::FuncExpr(f)
        }
        Node::DistinctExpr(mut o) => {
            o.args = o.args.into_iter().map(|a| replace_correlation_vars(root, a, plan_params)).collect();
            Node::DistinctExpr(o)
        }
        Node::RelabelType(mut r) => {
            r.arg = r.arg.map(|a| replace_correlation_vars(root, a, plan_params));
            Node::RelabelType(r)
        }
        Node::Aggref(mut a) => {
            a.args = a.args.into_iter().map(|n| replace_correlation_vars(root, n, plan_params)).collect();
            Node::Aggref(a)
        }
        Node::FromExpr(mut f) => {
            f.fromlist = f.fromlist.into_iter().map(|n| replace_correlation_vars(root, n, plan_params)).collect();
            f.quals = f.quals.map(|q| replace_correlation_vars(root, q, plan_params));
            Node::FromExpr(f)
        }
        other => other,
    }
}

/// PG `assign_param_for_var`: find or create the PARAM_EXEC slot for an outer Var,
/// returning its paramid. Dedups on (varno, varattno, vartype) so the same outer
/// column shared by multiple correlation references reuses one slot/arg.
fn assign_param_for_var(
    root: &mut PlannerInfo,
    var: &crate::nodes::primnodes::Var,
    plan_params: &mut Vec<(i32, Node)>,
) -> i32 {
    for (id, item) in plan_params.iter() {
        if let Node::Var(v) = item
            && v.varno == var.varno
            && v.varattno == var.varattno
            && v.vartype == var.vartype
        {
            return *id;
        }
    }
    let paramid = i32::try_from(root.glob.param_exec_types.len()).unwrap_or(0);
    root.glob.param_exec_types.push(var.vartype);
    plan_params.push((paramid, Node::Var(Box::new(var.clone()))));
    paramid
}

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
use crate::nodes::primnodes::{Index, OUTER_VAR};
use crate::postgres_ext::{InvalidOid, Oid};

/// Panic for a subselect path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
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
/// finished plan tree. Called by standard_planner only when Params were
/// generated; M1 generates none, so this is reached only if a Param appears
/// (then it grows). The pass-through over a const Result computes empty
/// param sets. `plan` is the polymorphic top plan node.
pub fn ss_finalize_plan(_root: &mut PlannerInfo, plan: &mut Node) {
    // finalize_plan recurses the tree accumulating Param IDs. A childless const
    // Result references no Params; its ext_param/all_param are empty.
    let Node::Result(result) = plan else {
        not_yet_reachable("SS_finalize_plan: non-Result top plan");
    };
    if result.plan.lefttree.is_some() {
        not_yet_reachable("SS_finalize_plan: subplan recursion");
    }
    if !result.plan.init_plan.is_empty() {
        not_yet_reachable("SS_finalize_plan: initplans");
    }
    result.plan.ext_param = None;
    result.plan.all_param = None;
}

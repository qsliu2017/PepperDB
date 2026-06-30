//! Plan a set operation (UNION/INTERSECT/EXCEPT [ALL]). Translated from the
//! `plan_set_operations` family in backend/optimizer/prep/prepunion.c
//! (disposition: grow -- the M12 two-input subset; PG's appendrel-of-N-branches,
//! cost-based sorted-vs-hashed SetOp choice, and the colTypes/colCollations Var
//! re-targeting are collapsed to the executor's per-side-counting SetOp and a
//! straightforward Append).
//!
//! The port's analyze layer (analyze.rs `finish_set_operation_stmt`) builds the
//! `SetOperationStmt` tree with embedded `Node::Query` leaves (NOT RangeTblRef into
//! the top Query's rtable, which stays empty), so this planner recurses that tree:
//!   - a leaf `Query` is planned independently (`plan_subquery`) into a finished
//!     plan + its own flattened rangetable;
//!   - the branch rangetables are concatenated into one combined rangetable, each
//!     branch's plan offset by the running length (so a branch's `scanrelid` and
//!     base Vars land at the combined RT index -- PG's OffsetVarNodes, applied to
//!     the finished plan tree's scan nodes here);
//!   - the branches are combined: UNION ALL -> `Append`; UNION -> `Append` then a
//!     `Sort`+`Unique` dedup over all output columns; INTERSECT/EXCEPT [ALL] -> a
//!     `SetOp` over the two branch plans (the executor counts per side).
//!
//! Precedence is honored by the tree shape the analyze layer already nested
//! (`a UNION b INTERSECT c` parses as `a UNION (b INTERSECT c)`).

use crate::backend::nodes::makefuncs::make_var;
use crate::backend::optimizer::plan::planner::{plan_subquery, PlannedSubquery};
use crate::backend::parser::parse_oper::opername_get_oprid;
use crate::nodes::nodes::{Node, SetOpCmd, SetOpStrategy};
use crate::nodes::parsenodes::{Query, SetOperation, SetOperationStmt};
use crate::nodes::plannodes::{Append, Plan, SetOp, Sort, Unique};
use crate::nodes::primnodes::{Index, OUTER_VAR};
use crate::postgres_ext::{InvalidOid, Oid};

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// The result of planning a set-op (sub)tree: the combined plan plus the
/// concatenated branch rangetables (and their perminfos) gathered so far.
pub struct SetOpResult {
    pub plan: Node,
    pub rtable: Vec<Node>,
    pub perminfos: Vec<Node>,
    /// the output column types (per `SetOperationStmt.colTypes`), for tlist/dedup.
    pub col_types: Vec<Oid>,
}

/// PG `plan_set_operations`: turn the top `SetOperationStmt` of a Query into a
/// combined plan, building the combined rangetable. `top_tlist` is the top Query's
/// target list (Var(varno=0) per output column with the result column names); it
/// supplies the result column names for the combined node's targetlist.
pub fn plan_set_operations(sostmt: &SetOperationStmt, top_tlist: &[Node]) -> SetOpResult {
    let mut result = combine_set_op(sostmt);
    // Re-label the combined node's top targetlist with the result column names from
    // the top Query's tlist (the leftmost branch's names).
    relabel_top_tlist(&mut result.plan, top_tlist);
    result
}

/// Combine one `SetOperationStmt` node (an internal set-op) into a plan over its two
/// recursively-planned arms.
fn combine_set_op(so: &SetOperationStmt) -> SetOpResult {
    match so.op {
        SetOperation::UNION => generate_union_plan(so),
        SetOperation::INTERSECT | SetOperation::EXCEPT => generate_nonunion_plan(so),
        SetOperation::NONE => not_yet_reachable("combine_set_op: SETOP_NONE node"),
    }
}

/// PG `recurse_set_operations`: plan one arm of the set-op tree. A leaf
/// (`Node::Query`) is planned independently; an internal `SetOperationStmt` plans
/// both arms and combines them.
fn recurse_set_operations(node: &Node) -> SetOpResult {
    match node {
        Node::Query(q) => plan_leaf_query(q),
        Node::SetOperationStmt(so) => combine_set_op(so),
        other => not_yet_reachable(&format!("recurse_set_operations: {other:?}")),
    }
}

/// Plan a leaf branch Query into a finished plan + its own rangetable. The
/// branch's output column types come from its plan tlist.
fn plan_leaf_query(q: &Query) -> SetOpResult {
    let mut leaf = q.clone();
    let PlannedSubquery { plan, rtable, perminfos } = plan_subquery(&mut leaf, -1);
    let col_types = branch_col_types(&plan);
    SetOpResult { plan, rtable, perminfos, col_types }
}

/// PG `generate_union_paths`: combine the two arms under UNION [ALL]. The arms are
/// stacked under an `Append`; for UNION (not ALL) a `Sort`+`Unique` over all output
/// columns deduplicates the concatenation.
fn generate_union_plan(so: &SetOperationStmt) -> SetOpResult {
    let larg = so.larg.as_ref().unwrap_or_else(|| not_yet_reachable("UNION: missing left arm"));
    let rarg = so.rarg.as_ref().unwrap_or_else(|| not_yet_reachable("UNION: missing right arm"));

    let left = recurse_set_operations(larg);
    let right = recurse_set_operations(rarg);

    // Concatenate the branch rangetables, offsetting the right branch's plan by the
    // left branch's rangetable length so its scan RT indices land in the combined
    // table.
    let (left_plan, right_plan, rtable, perminfos) = splice_branches(left, right);

    let col_types = so.colTypes.clone();
    let tlist = setop_tlist(&col_types, None);

    let append = Node::Append(Box::new(Append {
        plan: empty_plan(tlist),
        apprelids: None,
        appendplans: vec![left_plan, right_plan],
        nasyncplans: 0,
        first_partial_plan: 0,
        part_prune_index: -1,
    }));

    let plan = if so.all {
        append
    } else {
        dedup_plan(append, &col_types)
    };

    SetOpResult { plan, rtable, perminfos, col_types }
}

/// PG `generate_nonunion_paths`: combine the two arms under INTERSECT/EXCEPT [ALL].
/// The executor's `SetOp` takes the two branch plans as its left/right children and
/// counts per side, so no flag-tagging or sorted-merge is built here.
fn generate_nonunion_plan(so: &SetOperationStmt) -> SetOpResult {
    let larg = so.larg.as_ref().unwrap_or_else(|| not_yet_reachable("set-op: missing left arm"));
    let rarg = so.rarg.as_ref().unwrap_or_else(|| not_yet_reachable("set-op: missing right arm"));

    let left = recurse_set_operations(larg);
    let right = recurse_set_operations(rarg);
    let (left_plan, right_plan, rtable, perminfos) = splice_branches(left, right);

    let col_types = so.colTypes.clone();
    let cmd = match (so.op, so.all) {
        (SetOperation::INTERSECT, false) => SetOpCmd::INTERSECT,
        (SetOperation::INTERSECT, true) => SetOpCmd::INTERSECT_ALL,
        (SetOperation::EXCEPT, false) => SetOpCmd::EXCEPT,
        (SetOperation::EXCEPT, true) => SetOpCmd::EXCEPT_ALL,
        _ => not_yet_reachable("generate_nonunion_plan: not INTERSECT/EXCEPT"),
    };

    let ncols = col_types.len();
    let cmp_operators: Vec<Oid> = col_types.iter().map(|&t| eq_op(t)).collect();
    let setop = SetOp {
        plan: Plan {
            lefttree: Some(left_plan),
            righttree: Some(right_plan),
            ..empty_plan(setop_tlist(&col_types, None))
        },
        cmd,
        strategy: SetOpStrategy::HASHED,
        num_cols: i32::try_from(ncols).unwrap_or(0),
        cmp_col_idx: (1..=ncols as i16).collect(),
        cmp_operators,
        cmp_collations: vec![InvalidOid; ncols],
        cmp_nulls_first: vec![false; ncols],
        num_groups: 0,
    };
    SetOpResult { plan: Node::SetOp(Box::new(setop)), rtable, perminfos, col_types }
}

/// Concatenate the two branches' rangetables, offsetting the RIGHT branch's plan
/// (its scan nodes) by the LEFT branch's rangetable length so the right branch's RT
/// indices land in the combined table. Returns (left_plan, right_plan, rtable,
/// perminfos).
fn splice_branches(
    mut left: SetOpResult,
    mut right: SetOpResult,
) -> (Node, Node, Vec<Node>, Vec<Node>) {
    let offset = i32::try_from(left.rtable.len()).unwrap_or(0);
    offset_plan_rt_indices(&mut right.plan, offset);

    let mut rtable = std::mem::take(&mut left.rtable);
    rtable.append(&mut right.rtable);
    let mut perminfos = std::mem::take(&mut left.perminfos);
    perminfos.append(&mut right.perminfos);

    (left.plan, right.plan, rtable, perminfos)
}

/// Public re-export of the Plan-tree RT-index offsetter (for the CTE planner in
/// subselect.rs, which offsets each CTE subplan into the combined rangetable).
pub fn offset_plan_rt_indices_pub(plan: &mut Node, offset: i32) {
    offset_plan_rt_indices(plan, offset);
}

/// Combine two already-built branch plans under a set operation, given the output
/// column types. UNION ALL -> Append; UNION -> Append + Sort/Unique dedup;
/// INTERSECT/EXCEPT [ALL] -> SetOp. Used by the CTE host planner (subselect.rs),
/// which manages the combined rangetable itself (the branch plans are already at
/// their final RT indices), so no rangetable splicing happens here.
pub fn combine_setop_branches(
    op: SetOperation,
    all: bool,
    left_plan: Node,
    right_plan: Node,
    col_types: &[Oid],
) -> Node {
    match op {
        SetOperation::UNION => {
            let append = Node::Append(Box::new(Append {
                plan: empty_plan(setop_tlist(col_types, None)),
                apprelids: None,
                appendplans: vec![left_plan, right_plan],
                nasyncplans: 0,
                first_partial_plan: 0,
                part_prune_index: -1,
            }));
            if all {
                append
            } else {
                dedup_plan(append, col_types)
            }
        }
        SetOperation::INTERSECT | SetOperation::EXCEPT => {
            let ncols = col_types.len();
            let cmd = match (op, all) {
                (SetOperation::INTERSECT, false) => SetOpCmd::INTERSECT,
                (SetOperation::INTERSECT, true) => SetOpCmd::INTERSECT_ALL,
                (SetOperation::EXCEPT, false) => SetOpCmd::EXCEPT,
                (SetOperation::EXCEPT, true) => SetOpCmd::EXCEPT_ALL,
                _ => not_yet_reachable("combine_setop_branches: not INTERSECT/EXCEPT"),
            };
            Node::SetOp(Box::new(SetOp {
                plan: Plan {
                    lefttree: Some(left_plan),
                    righttree: Some(right_plan),
                    ..empty_plan(setop_tlist(col_types, None))
                },
                cmd,
                strategy: SetOpStrategy::HASHED,
                num_cols: i32::try_from(ncols).unwrap_or(0),
                cmp_col_idx: (1..=ncols as i16).collect(),
                cmp_operators: col_types.iter().map(|&t| eq_op(t)).collect(),
                cmp_collations: vec![InvalidOid; ncols],
                cmp_nulls_first: vec![false; ncols],
                num_groups: 0,
            }))
        }
        SetOperation::NONE => not_yet_reachable("combine_setop_branches: SETOP_NONE"),
    }
}

/// Add `offset` to every scan node's `scanrelid` (and base-rel Var varno) in a
/// finished plan tree -- the OffsetVarNodes equivalent applied to a Plan tree. The
/// scan executor opens its relation via `scanrelid` indexing the combined
/// rangetable; the tlist Vars are renumbered too for faithfulness.
fn offset_plan_rt_indices(plan: &mut Node, offset: i32) {
    if offset == 0 {
        return;
    }
    let off = offset as Index;
    match plan {
        Node::SeqScan(s) => {
            s.scan.scanrelid += off;
            offset_exprs_varno(&mut s.scan.plan.targetlist, offset);
            offset_exprs_varno(&mut s.scan.plan.qual, offset);
        }
        Node::IndexScan(s) => {
            s.scan.scanrelid += off;
            offset_exprs_varno(&mut s.scan.plan.targetlist, offset);
            offset_exprs_varno(&mut s.scan.plan.qual, offset);
        }
        Node::IndexOnlyScan(s) => {
            s.scan.scanrelid += off;
            offset_exprs_varno(&mut s.scan.plan.targetlist, offset);
            offset_exprs_varno(&mut s.scan.plan.qual, offset);
        }
        Node::BitmapHeapScan(s) => {
            s.scan.scanrelid += off;
            offset_exprs_varno(&mut s.scan.plan.targetlist, offset);
            offset_exprs_varno(&mut s.scan.plan.qual, offset);
            if let Some(c) = s.scan.plan.lefttree.as_mut() {
                offset_plan_rt_indices(c, offset);
            }
        }
        Node::BitmapIndexScan(s) => {
            s.scan.scanrelid += off;
        }
        Node::Result(r) => {
            if let Some(c) = r.plan.lefttree.as_mut() {
                offset_plan_rt_indices(c, offset);
            }
        }
        Node::Sort(s) => offset_child(&mut s.plan, offset),
        Node::Unique(u) => offset_child(&mut u.plan, offset),
        Node::Limit(l) => offset_child(&mut l.plan, offset),
        Node::Agg(a) => offset_child(&mut a.plan, offset),
        Node::WindowAgg(w) => offset_child(&mut w.plan, offset),
        Node::Material(m) => offset_child(&mut m.plan, offset),
        Node::Group(g) => offset_child(&mut g.plan, offset),
        Node::NestLoop(n) => offset_join(&mut n.join.plan, offset),
        Node::MergeJoin(m) => offset_join(&mut m.join.plan, offset),
        Node::HashJoin(h) => offset_join(&mut h.join.plan, offset),
        Node::Hash(h) => offset_child(&mut h.plan, offset),
        Node::Append(a) => {
            for c in &mut a.appendplans {
                offset_plan_rt_indices(c, offset);
            }
        }
        Node::SetOp(s) => offset_join(&mut s.plan, offset),
        Node::RecursiveUnion(r) => offset_join(&mut r.plan, offset),
        Node::CteScan(c) => {
            c.scan.scanrelid += off;
            offset_exprs_varno(&mut c.scan.plan.targetlist, offset);
            if let Some(sub) = c.scan.plan.lefttree.as_mut() {
                offset_plan_rt_indices(sub, offset);
            }
        }
        Node::WorkTableScan(w) => {
            w.scan.scanrelid += off;
            offset_exprs_varno(&mut w.scan.plan.targetlist, offset);
            offset_exprs_varno(&mut w.scan.plan.qual, offset);
        }
        other => not_yet_reachable(&format!("offset_plan_rt_indices: {other:?}")),
    }
}

/// Offset a single-child upper node's child plan.
fn offset_child(plan: &mut Plan, offset: i32) {
    if let Some(c) = plan.lefttree.as_mut() {
        offset_plan_rt_indices(c, offset);
    }
}

/// Offset both children of a join/two-input plan node.
fn offset_join(plan: &mut Plan, offset: i32) {
    if let Some(c) = plan.lefttree.as_mut() {
        offset_plan_rt_indices(c, offset);
    }
    if let Some(c) = plan.righttree.as_mut() {
        offset_plan_rt_indices(c, offset);
    }
}

/// Add `offset` to the varno of every base-rel `Var` (varlevelsup 0, real varno)
/// in an expression list. OUTER_VAR/INNER_VAR (already-rewritten upper Vars) are
/// left untouched -- they are positional, not RT-index, references.
fn offset_exprs_varno(exprs: &mut [Node], offset: i32) {
    for e in exprs.iter_mut() {
        offset_expr_varno(e, offset);
    }
}

fn offset_expr_varno(expr: &mut Node, offset: i32) {
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
                offset_expr_varno(e, offset);
            }
        }
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            for a in &mut o.args {
                offset_expr_varno(a, offset);
            }
        }
        Node::BoolExpr(b) => {
            for a in &mut b.args {
                offset_expr_varno(a, offset);
            }
        }
        Node::FuncExpr(f) => {
            for a in &mut f.args {
                offset_expr_varno(a, offset);
            }
        }
        Node::RelabelType(r) => {
            if let Some(a) = r.arg.as_mut() {
                offset_expr_varno(a, offset);
            }
        }
        _ => {}
    }
}

/// Stack a `Sort`+`Unique` over `subplan` to deduplicate the UNION concatenation on
/// all output columns. The Sort orders by every column (col 1..n) and Unique
/// removes adjacent duplicates with the per-column equality operators.
fn dedup_plan(subplan: Node, col_types: &[Oid]) -> Node {
    let ncols = col_types.len();
    let tlist = setop_tlist(col_types, None);

    let sort = Node::Sort(Box::new(Sort {
        plan: Plan { lefttree: Some(subplan), ..empty_plan(tlist.clone()) },
        num_cols: i32::try_from(ncols).unwrap_or(0),
        sort_col_idx: (1..=ncols as i16).collect(),
        sort_operators: col_types.iter().map(|&t| lt_op(t)).collect(),
        collations: vec![InvalidOid; ncols],
        nulls_first: vec![false; ncols],
    }));

    Node::Unique(Box::new(Unique {
        plan: Plan { lefttree: Some(sort), ..empty_plan(tlist) },
        num_cols: i32::try_from(ncols).unwrap_or(0),
        uniq_col_idx: (1..=ncols as i16).collect(),
        uniq_operators: col_types.iter().map(|&t| eq_op(t)).collect(),
        uniq_collations: vec![InvalidOid; ncols],
    }))
}

/// The combined node's output targetlist: an OUTER_VAR `Var` per output column
/// (resno 1..n, type colTypes[i]). The executor copies child tuples without
/// projecting, so these Vars are positional; `names` (when given) supplies the
/// result column names for the wire RowDescription.
fn setop_tlist(col_types: &[Oid], names: Option<&[Option<String>]>) -> Vec<Node> {
    col_types
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let var = make_var(OUTER_VAR, (i + 1) as i16, t, -1, InvalidOid, 0);
            let resname = names.and_then(|n| n.get(i).cloned().flatten());
            Node::TargetEntry(Box::new(crate::backend::nodes::makefuncs::make_target_entry(
                Some(Node::Var(Box::new(var))),
                (i + 1) as i16,
                resname,
                false,
            )))
        })
        .collect()
}

/// Re-label the combined plan node's top targetlist with the result column names
/// from the top Query's tlist (the leftmost branch's names).
fn relabel_top_tlist(plan: &mut Node, top_tlist: &[Node]) {
    let names: Vec<Option<String>> = top_tlist
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(t) if !t.resjunk => Some(t.resname.clone()),
            _ => None,
        })
        .collect();
    let tlist = top_plan_tlist_mut(plan);
    for (te, name) in tlist.iter_mut().zip(names) {
        if let Node::TargetEntry(t) = te {
            t.resname = name;
        }
    }
}

/// Borrow the top plan node's targetlist (for the set-op combining nodes).
fn top_plan_tlist_mut(plan: &mut Node) -> &mut Vec<Node> {
    match plan {
        Node::Append(a) => &mut a.plan.targetlist,
        Node::SetOp(s) => &mut s.plan.targetlist,
        Node::Unique(u) => &mut u.plan.targetlist,
        Node::Sort(s) => &mut s.plan.targetlist,
        Node::Result(r) => &mut r.plan.targetlist,
        Node::SeqScan(s) => &mut s.scan.plan.targetlist,
        other => not_yet_reachable(&format!("relabel_top_tlist: {other:?}")),
    }
}

/// The per-column output types of a finished branch plan, read from its plan
/// targetlist (a Var/TargetEntry per output column).
fn branch_col_types(plan: &Node) -> Vec<Oid> {
    plan_tlist(plan)
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(t) if !t.resjunk => {
                Some(t.expr.as_ref().map_or(InvalidOid, crate::nodes::nodeFuncs::exprType))
            }
            _ => None,
        })
        .collect()
}

/// The output targetlist of a finished plan node.
fn plan_tlist(plan: &Node) -> &[Node] {
    match plan {
        Node::Result(r) => &r.plan.targetlist,
        Node::SeqScan(s) => &s.scan.plan.targetlist,
        Node::IndexScan(s) => &s.scan.plan.targetlist,
        Node::IndexOnlyScan(s) => &s.scan.plan.targetlist,
        Node::BitmapHeapScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::WindowAgg(w) => &w.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        Node::Append(a) => &a.plan.targetlist,
        Node::SetOp(s) => &s.plan.targetlist,
        Node::NestLoop(n) => &n.join.plan.targetlist,
        Node::MergeJoin(m) => &m.join.plan.targetlist,
        Node::HashJoin(h) => &h.join.plan.targetlist,
        other => not_yet_reachable(&format!("plan_tlist: {other:?}")),
    }
}

/// A blank `Plan` carrying `tlist` (the common-field skeleton, palloc0 semantics).
fn empty_plan(tlist: Vec<Node>) -> Plan {
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

/// The default `=` operator OID for a type (for SetOp/Unique equality).
fn eq_op(typ: Oid) -> Oid {
    opername_get_oprid("=", typ, typ)
}

/// The default `<` operator OID for a type (for the dedup Sort ordering).
fn lt_op(typ: Oid) -> Oid {
    opername_get_oprid("<", typ, typ)
}

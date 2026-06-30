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
        PathType::IndexScan => Node::IndexScan(Box::new(create_indexscan_plan(root, best_path))),
        PathType::BitmapHeapScan => {
            Node::BitmapHeapScan(Box::new(create_bitmap_scan_plan(root, best_path)))
        }
        PathType::NestLoop | PathType::MergeJoin | PathType::HashJoin => {
            create_join_plan(root, best_path)
        }
        other => not_yet_reachable(&format!("create_plan_recurse: {other:?}")),
    }
}

/// PG `create_join_plan`: dispatch a join Path to its concrete join-plan builder.
/// The gating-quals (pseudoconstant) wrapping is staged (M7 inner joins carry no
/// pseudoconstant join clauses).
fn create_join_plan(root: &mut PlannerInfo, best_path: &Path) -> Node {
    match best_path.pathtype {
        PathType::NestLoop => Node::NestLoop(Box::new(create_nestloop_plan(root, best_path))),
        PathType::MergeJoin => Node::MergeJoin(Box::new(create_mergejoin_plan(root, best_path))),
        PathType::HashJoin => Node::HashJoin(Box::new(create_hashjoin_plan(root, best_path))),
        other => not_yet_reachable(&format!("create_join_plan: {other:?}")),
    }
}

/// The JoinPathDetail carried by a join Path (NestLoop/MergeJoin/HashJoin). It holds
/// the jointype, the outer/inner subpaths, and the join restriction clauses.
fn join_detail(best_path: &Path) -> &crate::nodes::pathnodes::JoinPathDetail {
    best_path
        .join_detail
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_join_plan: path carries no join detail"))
}

/// The actual (un-RestrictInfo'd) join clauses of an inner-join path. PG splits the
/// joinrestrictinfo into `joinqual` (the ON/USING conditions) and `otherqual` (the
/// WHERE filter); for an INNER join the split is degenerate -- every clause is a
/// joinqual and there is no otherqual (`extract_actual_clauses(.., false)`).
fn inner_join_clauses(
    jointype: crate::nodes::nodes::JoinType,
    joinrestrictinfo: &[Box<crate::nodes::pathnodes::RestrictInfo>],
) -> Vec<Node> {
    use crate::nodes::nodes::JoinType;
    if jointype != JoinType::INNER {
        not_yet_reachable("create_join_plan: outer-join joinqual/otherqual split");
    }
    let rinfos: Vec<crate::nodes::pathnodes::RestrictInfo> =
        joinrestrictinfo.iter().map(|r| (**r).clone()).collect();
    crate::backend::optimizer::util::restrictinfo::extract_actual_clauses(&rinfos, false)
}

/// PG `make_nestloop` via `create_nestloop_plan`: build a `NestLoop` from a nestloop
/// Path. The outer subplan is the path's outer subpath; the inner subplan is the
/// inner subpath. For an INNER join the join clauses all become `joinqual` and there
/// is no per-tuple `qual`. M7 has no nestloop params (`required_outer` empty).
fn create_nestloop_plan(
    root: &mut PlannerInfo,
    best_path: &Path,
) -> crate::nodes::plannodes::NestLoop {
    use crate::nodes::plannodes::{Join, NestLoop};
    let d = join_detail(best_path);

    let tlist = build_path_tlist(root, best_path);
    let outer_plan = create_plan_recurse(root, &d.outerjoinpath);
    let inner_plan = create_plan_recurse(root, &d.innerjoinpath);

    let joinclauses = inner_join_clauses(d.jointype, &d.joinrestrictinfo);

    let mut node = NestLoop {
        join: Join {
            plan: Plan {
                lefttree: Some(outer_plan),
                righttree: Some(inner_plan),
                ..empty_plan(tlist, Vec::new())
            },
            jointype: d.jointype,
            inner_unique: d.inner_unique,
            joinqual: joinclauses,
        },
        nest_params: Vec::new(),
    };
    copy_generic_path_info(&mut node.join.plan, best_path);
    node
}

/// PG `create_mergejoin_plan`: build a `MergeJoin`. The mergeclauses are extracted
/// from the path's merge detail and switched so the outer Var is on the left
/// (`get_switched_clauses`); the non-merge join clauses become `joinqual`. When an
/// input is not already sorted as required (the path's outer/inner sortkeys are
/// non-empty) a `Sort` plan is inserted on that side. The merge family/collation/
/// reversal/nulls-first arrays are derived per mergeclause.
fn create_mergejoin_plan(
    root: &mut PlannerInfo,
    best_path: &Path,
) -> crate::nodes::plannodes::MergeJoin {
    use crate::nodes::plannodes::{Join, MergeJoin};
    let d = join_detail(best_path);
    let merge = d
        .merge
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_mergejoin_plan: path carries no merge detail"));

    let tlist = build_path_tlist(root, best_path);
    let mut outer_plan = create_plan_recurse(root, &d.outerjoinpath);
    let mut inner_plan = create_plan_recurse(root, &d.innerjoinpath);

    // The outer Var of each merge clause goes on the left (executor convention).
    let outer_relids = d
        .outerjoinpath
        .parent
        .as_ref()
        .and_then(|p| p.relids.clone())
        .unwrap_or_default();
    let (mergeclauses, switched) = get_switched_clauses(&merge.path_mergeclauses, &outer_relids);

    // The join clauses minus the merge clauses become the joinqual.
    let all_join = inner_join_clauses(d.jointype, &d.joinrestrictinfo);
    let joinclauses: Vec<Node> = all_join
        .into_iter()
        .filter(|c| !mergeclauses.contains(c))
        .collect();

    // Sort each side that is not already ordered as the merge requires.
    if !merge.outersortkeys.is_empty() {
        outer_plan = sort_plan_for_merge(&mergeclauses, &switched, &outer_plan, true);
    }
    if !merge.innersortkeys.is_empty() {
        inner_plan = sort_plan_for_merge(&mergeclauses, &switched, &inner_plan, false);
    }

    // Per-clause merge family/collation/reversal/nulls-first.
    let n = mergeclauses.len();
    let merge_families = mergeclauses.iter().map(merge_family_of).collect();
    let merge_collations = vec![crate::postgres_ext::InvalidOid; n];
    let merge_reversals = vec![false; n]; // ASC merge (COMPARE_LT) for the M7 paths.
    let merge_nulls_first = vec![false; n];

    let mut node = MergeJoin {
        join: Join {
            plan: Plan {
                lefttree: Some(outer_plan),
                righttree: Some(inner_plan),
                ..empty_plan(tlist, Vec::new())
            },
            jointype: d.jointype,
            inner_unique: d.inner_unique,
            joinqual: joinclauses,
        },
        skip_mark_restore: merge.skip_mark_restore,
        mergeclauses,
        merge_families,
        merge_collations,
        merge_reversals,
        merge_nulls_first,
    };
    copy_generic_path_info(&mut node.join.plan, best_path);
    node
}

/// PG `create_hashjoin_plan`: build a `HashJoin` over a `Hash` node on the inner
/// side. The hashclauses are switched so the outer Var is on the left; the
/// `outer_hashkeys`/`inner_hashkeys` are the left/right operands of each switched
/// hashclause. The Hash node wraps the inner subplan and carries the inner hashkeys.
fn create_hashjoin_plan(
    root: &mut PlannerInfo,
    best_path: &Path,
) -> crate::nodes::plannodes::HashJoin {
    use crate::nodes::plannodes::{Hash, HashJoin, Join};
    let d = join_detail(best_path);
    let hash = d
        .hash
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_hashjoin_plan: path carries no hash detail"));

    let tlist = build_path_tlist(root, best_path);
    let outer_plan = create_plan_recurse(root, &d.outerjoinpath);
    let inner_plan = create_plan_recurse(root, &d.innerjoinpath);

    let outer_relids = d
        .outerjoinpath
        .parent
        .as_ref()
        .and_then(|p| p.relids.clone())
        .unwrap_or_default();
    let (hashclauses, _switched) = get_switched_clauses(&hash.path_hashclauses, &outer_relids);

    let all_join = inner_join_clauses(d.jointype, &d.joinrestrictinfo);
    let joinclauses: Vec<Node> = all_join
        .into_iter()
        .filter(|c| !hashclauses.contains(c))
        .collect();

    // Split each switched hashclause into its outer (left) and inner (right) key.
    let mut hashoperators = Vec::with_capacity(hashclauses.len());
    let mut hashcollations = Vec::with_capacity(hashclauses.len());
    let mut outer_hashkeys = Vec::with_capacity(hashclauses.len());
    let mut inner_hashkeys = Vec::with_capacity(hashclauses.len());
    for c in &hashclauses {
        let Node::OpExpr(op) = c else {
            not_yet_reachable("create_hashjoin_plan: non-OpExpr hash clause");
        };
        hashoperators.push(op.opno);
        hashcollations.push(op.inputcollid);
        outer_hashkeys.push(op.args[0].clone());
        inner_hashkeys.push(op.args[1].clone());
    }

    // The Hash node on the inner side: its tlist is the inner plan's, it carries the
    // inner hashkeys, and it has no skew table (M7 has no skew optimization).
    let hash_tlist = current_tlist(&inner_plan).to_vec();
    let hash_node = Hash {
        plan: Plan {
            lefttree: Some(inner_plan),
            ..empty_plan(hash_tlist, Vec::new())
        },
        hashkeys: inner_hashkeys,
        skew_table: crate::postgres_ext::InvalidOid,
        skew_column: 0,
        skew_inherit: false,
        rows_total: 0.0,
    };

    let mut node = HashJoin {
        join: Join {
            plan: Plan {
                lefttree: Some(outer_plan),
                righttree: Some(Node::Hash(Box::new(hash_node))),
                ..empty_plan(tlist, Vec::new())
            },
            jointype: d.jointype,
            inner_unique: d.inner_unique,
            joinqual: joinclauses,
        },
        hashclauses,
        hashoperators,
        hashcollations,
        hashkeys: outer_hashkeys,
    };
    copy_generic_path_info(&mut node.join.plan, best_path);
    node
}

/// PG `get_switched_clauses`: produce the plain (un-RestrictInfo'd) merge/hash
/// clauses with the outer-relation Var always on the left of the operator. A clause
/// whose right side is the outer rel is commuted (its args swapped); the matching
/// `outer_is_left` flag is returned per clause. Returns (switched clauses, the
/// per-clause outer_is_left flags). M7 builtin "=" operators are their own
/// commutator, so commuting only swaps the args.
fn get_switched_clauses(
    clauses: &[Box<crate::nodes::pathnodes::RestrictInfo>],
    outer_relids: &crate::nodes::pathnodes::Relids,
) -> (Vec<Node>, Vec<bool>) {
    use crate::nodes::bitmapset::bms_is_subset;
    let mut out = Vec::with_capacity(clauses.len());
    let mut flags = Vec::with_capacity(clauses.len());
    for rinfo in clauses {
        let Node::OpExpr(op) = &rinfo.clause else {
            not_yet_reachable("get_switched_clauses: non-OpExpr merge/hash clause");
        };
        let right_is_outer = rinfo
            .right_relids
            .as_ref()
            .is_some_and(|r| bms_is_subset(r, outer_relids));
        if right_is_outer {
            // Commute: swap the operands so the outer Var lands on the left.
            let mut temp = op.clone();
            temp.opfuncid = crate::postgres_ext::InvalidOid;
            temp.args.swap(0, 1);
            out.push(Node::OpExpr(temp));
            flags.push(false);
        } else {
            out.push(Node::OpExpr(op.clone()));
            flags.push(true);
        }
    }
    (out, flags)
}

/// Build a `Sort` plan ordering `child` on the merge keys taken from the side
/// (`outer_side` true = left operand, false = right operand) of each merge clause.
/// The sort column is the child output position of that key Var; the sort operator
/// is the btree "<" of the merge clause's equality operator.
fn sort_plan_for_merge(
    mergeclauses: &[Node],
    _switched: &[bool],
    child: &Node,
    outer_side: bool,
) -> Node {
    let child_tlist = current_tlist(child);
    let arg_index = usize::from(!outer_side); // 0 = outer (left), 1 = inner (right)
    let keys: Vec<SortKey> = mergeclauses
        .iter()
        .filter_map(|c| {
            let Node::OpExpr(op) = c else { return None };
            let Node::Var(var) = &op.args[arg_index] else { return None };
            let col = scan_col_for_var(child_tlist, var)?;
            Some(SortKey { col, sortop: btree_lt_of_eq(op.opno), nulls_first: false })
        })
        .collect();
    Node::Sort(Box::new(make_sort(child.clone(), keys)))
}

/// The btree-family OID a merge clause's equality operator belongs to (used as the
/// MergeJoin `mergeFamilies` entry). M7 seed types map to the integer/text/oid/bool
/// btree opfamilies.
fn merge_family_of(clause: &Node) -> crate::postgres_ext::Oid {
    let Node::OpExpr(op) = clause else {
        return crate::postgres_ext::InvalidOid;
    };
    let fams = crate::backend::utils::cache::lsyscache::get_mergejoin_opfamilies(op.opno);
    fams.first().copied().unwrap_or(crate::postgres_ext::InvalidOid)
}

/// Map an equality operator OID to its same-type btree "<" operator OID (the
/// merge/sort ordering operator). M7 seed types: int2/int4/int8/text/oid/bool.
fn btree_lt_of_eq(eq_opno: crate::postgres_ext::Oid) -> crate::postgres_ext::Oid {
    use crate::postgres_ext::Oid;
    // pg_operator.dat same-type "<": int2(95), int4(97), int8(412), bool(58),
    // text(664), oid(609). Maps from the "=" OID.
    let lt = match eq_opno.get() {
        94 => 95,   // int2
        96 => 97,   // int4
        410 => 412, // int8
        91 => 58,   // bool
        98 => 664,  // text
        607 => 609, // oid
        _ => return crate::postgres_ext::InvalidOid,
    };
    Oid::new(lt)
}

/// The child output column position (1-based) of a base-rel Var, by matching
/// varno/varattno against the child tlist's TargetEntry Var exprs.
fn scan_col_for_var(
    child_tlist: &[Node],
    var: &crate::nodes::primnodes::Var,
) -> Option<crate::access::attnum::AttrNumber> {
    for n in child_tlist {
        let Node::TargetEntry(te) = n else { continue };
        if let Some(Node::Var(cv)) = te.expr.as_ref()
            && cv.varno == var.varno
            && cv.varattno == var.varattno
        {
            return Some(te.resno);
        }
    }
    None
}


/// PG `create_indexscan_plan` (M6 plain IndexScan form): build an `IndexScan` from an
/// index Path. The plan's `indexqual` is the matched clauses with the index-column
/// Var rewritten to `INDEX_VAR` (`fix_indexqual_clause`); `indexqualorig` keeps the
/// original heap-Var clauses (for the recheck). The scan's filter `qual` is the
/// base restriction clauses not handled by the index (so they are not applied
/// twice). The targetlist comes from the path's pathtarget.
fn create_indexscan_plan(root: &mut PlannerInfo, best_path: &Path) -> crate::nodes::plannodes::IndexScan {
    use crate::nodes::plannodes::IndexScan;

    let parent = best_path
        .parent
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_indexscan_plan: missing parent rel"));
    let scan_relid = parent.relid;
    crate::assert!(scan_relid > 0);

    let detail = best_path
        .index_detail
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_indexscan_plan: path carries no index detail"));

    // The original (heap-Var) index clauses, and the index-Var-rewritten quals.
    let indexqualorig = index_clause_quals(&detail.indexclauses);
    let indexqual: Vec<Node> = indexqualorig
        .iter()
        .map(|c| fix_indexqual_clause(c, &detail.indexinfo))
        .collect();

    // The scan filter is the base restriction clauses minus those the index checks
    // (M6 indexquals are exact, not lossy, so the index-handled clauses are dropped).
    let scan_clauses: Vec<crate::nodes::pathnodes::RestrictInfo> =
        parent.baserestrictinfo.iter().map(|ri| (**ri).clone()).collect();
    let all_clauses = crate::backend::optimizer::util::restrictinfo::extract_actual_clauses(
        &scan_clauses,
        false,
    );
    let qual: Vec<Node> = all_clauses
        .into_iter()
        .filter(|c| !indexqualorig.contains(c))
        .collect();

    let tlist = build_path_tlist(root, best_path);

    let mut plan = IndexScan {
        scan: Scan { plan: empty_plan(tlist, qual), scanrelid: scan_relid },
        indexid: detail.indexinfo.indexoid,
        indexqual,
        indexqualorig,
        indexorderby: Vec::new(),
        indexorderbyorig: Vec::new(),
        indexorderbyops: Vec::new(),
        indexorderdir: detail.indexscandir,
    };
    copy_generic_path_info(&mut plan.scan.plan, best_path);
    plan
}

/// PG `create_bitmap_scan_plan` (M6 form): build a `BitmapHeapScan` over a
/// `BitmapIndexScan` child. The heap node carries `bitmapqualorig` (the original
/// heap-Var clauses, for the lossy-page recheck); its lefttree is the bitmap index
/// scan producing the TID bitmap.
fn create_bitmap_scan_plan(
    root: &mut PlannerInfo,
    best_path: &Path,
) -> crate::nodes::plannodes::BitmapHeapScan {
    use crate::nodes::plannodes::{BitmapHeapScan, BitmapIndexScan};

    let parent = best_path
        .parent
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_bitmap_scan_plan: missing parent rel"));
    let scan_relid = parent.relid;
    crate::assert!(scan_relid > 0);

    let detail = best_path
        .index_detail
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("create_bitmap_scan_plan: path carries no index detail"));

    let indexqualorig = index_clause_quals(&detail.indexclauses);
    let indexqual: Vec<Node> = indexqualorig
        .iter()
        .map(|c| fix_indexqual_clause(c, &detail.indexinfo))
        .collect();

    // The BitmapIndexScan child: produces the TID bitmap from the index quals.
    let bitmap_index = BitmapIndexScan {
        scan: Scan { plan: empty_plan(Vec::new(), Vec::new()), scanrelid: scan_relid },
        indexid: detail.indexinfo.indexoid,
        isshared: false,
        indexqual,
        indexqualorig: indexqualorig.clone(),
    };

    // The heap scan: recheck the original clauses on lossy pages; filter the
    // remaining base restriction clauses.
    let scan_clauses: Vec<crate::nodes::pathnodes::RestrictInfo> =
        parent.baserestrictinfo.iter().map(|ri| (**ri).clone()).collect();
    let all_clauses = crate::backend::optimizer::util::restrictinfo::extract_actual_clauses(
        &scan_clauses,
        false,
    );
    let qual: Vec<Node> = all_clauses
        .into_iter()
        .filter(|c| !indexqualorig.contains(c))
        .collect();

    let tlist = build_path_tlist(root, best_path);

    let mut plan = BitmapHeapScan {
        scan: Scan { plan: empty_plan(tlist, qual), scanrelid: scan_relid },
        bitmapqualorig: indexqualorig,
    };
    // The bitmap producer is the BitmapHeapScan's lefttree (execProcnode inits it via
    // s.scan.plan.lefttree and drives it through MultiExecProcNode).
    plan.scan.plan.lefttree = Some(Node::BitmapIndexScan(Box::new(bitmap_index)));
    copy_generic_path_info(&mut plan.scan.plan, best_path);
    plan
}

/// The original (heap-Var) clauses of a set of `IndexClause`s.
fn index_clause_quals(indexclauses: &[Box<crate::nodes::pathnodes::IndexClause>]) -> Vec<Node> {
    indexclauses
        .iter()
        .flat_map(|ic| ic.indexquals.iter().map(|ri| ri.clause.clone()))
        .collect()
}

/// PG `fix_indexqual_clause` / `fix_indexqual_operand`: rewrite the index-column Var
/// in an `indexcol op const` clause from its heap (varno, heap-attno) form to the
/// `INDEX_VAR` form the index AM expects -- `varno = INDEX_VAR`, `varattno = the
/// 1-based index column position`. M6 handles a binary OpExpr with the indexed Var
/// on the left and a Const on the right.
fn fix_indexqual_clause(clause: &Node, index: &crate::nodes::pathnodes::IndexOptInfo) -> Node {
    use crate::nodes::primnodes::INDEX_VAR;
    let Node::OpExpr(op) = clause else {
        not_yet_reachable("fix_indexqual_clause: non-OpExpr index clause");
    };
    let mut op = op.clone();
    let Some(Node::Var(var)) = op.args.first() else {
        not_yet_reachable("fix_indexqual_clause: index clause has no Var operand");
    };
    // Find which index key column this heap attno is, so the index Var's attno is the
    // 1-based index column position.
    let heap_attno = i32::from(var.varattno);
    let indexcol = index
        .indexkeys
        .iter()
        .position(|&k| k == heap_attno)
        .unwrap_or_else(|| not_yet_reachable("fix_indexqual_clause: Var not an index column"));
    let mut newvar = var.clone();
    newvar.varno = INDEX_VAR;
    newvar.varattno = (indexcol + 1) as i16;
    newvar.varnosyn = INDEX_VAR as crate::c::Index;
    newvar.varattnosyn = (indexcol + 1) as i16;
    op.args[0] = Node::Var(newvar);
    Node::OpExpr(op)
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
        Node::IndexScan(s) => &s.scan.plan.targetlist,
        Node::IndexOnlyScan(s) => &s.scan.plan.targetlist,
        Node::BitmapHeapScan(s) => &s.scan.plan.targetlist,
        Node::Agg(a) => &a.plan.targetlist,
        Node::Sort(s) => &s.plan.targetlist,
        Node::Unique(u) => &u.plan.targetlist,
        Node::Limit(l) => &l.plan.targetlist,
        Node::NestLoop(n) => &n.join.plan.targetlist,
        Node::MergeJoin(m) => &m.join.plan.targetlist,
        Node::HashJoin(h) => &h.join.plan.targetlist,
        Node::Hash(h) => &h.plan.targetlist,
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

#[cfg(test)]
mod join_tests {
    use super::*;
    use crate::nodes::bitmapset::{bms_make_singleton, bms_union};
    use crate::nodes::nodes::JoinType;
    use crate::nodes::pathnodes::{
        HashPathDetail, JoinPathDetail, MergePathDetail, PathTarget, QualCost, RelOptInfo,
        RelOptKind, RestrictInfo, VolatileFunctionStatus,
    };
    use crate::nodes::primnodes::{OpExpr, Var, VarReturningType};
    use crate::postgres_ext::{InvalidOid, Oid};
    use crate::backend::optimizer::util::relnode::make_node_reloptinfo;

    const INT4: Oid = Oid::new(23);
    const INT4_EQ: Oid = Oid::new(96);

    fn var(varno: i32, varattno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: INT4,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as crate::nodes::primnodes::Index,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    fn pathtarget(exprs: Vec<Node>) -> PathTarget {
        PathTarget {
            exprs,
            sortgrouprefs: Vec::new(),
            cost: QualCost { startup: 0.0, per_tuple: 0.0 },
            width: 4,
            has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
        }
    }

    /// A bare SeqScan base-rel `Path` over relid, producing column 1 of that rel.
    fn scan_path(relid: i32) -> Path {
        let mut rel = make_node_reloptinfo(RelOptKind::BASEREL);
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel.rtekind = crate::nodes::parsenodes::RTEKind::RELATION;
        rel.reltarget = Some(Box::new(pathtarget(vec![var(relid, 1)])));
        let pathtarget = rel.reltarget.clone();
        Path {
            pathtype: PathType::SeqScan,
            parent: Some(Box::new(rel)),
            pathtarget,
            param_info: None,
            parallel_aware: false,
            parallel_safe: false,
            parallel_workers: 0,
            rows: 10.0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 1.0,
            pathkeys: Vec::new(),
            index_detail: None,
            join_detail: None,
        }
    }

    /// The `rel1.col1 = rel2.col1` equality RestrictInfo (as a merge/hash clause).
    fn eq_clause() -> Box<RestrictInfo> {
        let clause = Node::OpExpr(Box::new(OpExpr {
            opno: INT4_EQ,
            opfuncid: InvalidOid,
            opresulttype: Oid::new(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var(1, 1), var(2, 1)],
            location: -1,
        }));
        let mut ri = blank_ri(clause);
        ri.left_relids = Some(bms_make_singleton(1));
        ri.right_relids = Some(bms_make_singleton(2));
        ri.can_join = true;
        Box::new(ri)
    }

    fn blank_ri(clause: Node) -> RestrictInfo {
        RestrictInfo {
            clause,
            is_pushed_down: false,
            can_join: false,
            pseudoconstant: false,
            has_clone: false,
            is_clone: false,
            leakproof: false,
            has_volatile: VolatileFunctionStatus::NOVOLATILE,
            security_level: 0,
            num_base_rels: 2,
            clause_relids: None,
            required_relids: None,
            incompatible_relids: None,
            outer_relids: None,
            left_relids: None,
            right_relids: None,
            orclause: None,
            rinfo_serial: 1,
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

    /// The {1,2} joinrel whose output is (rel1.col1, rel2.col1).
    fn joinrel() -> RelOptInfo {
        let mut rel = make_node_reloptinfo(RelOptKind::JOINREL);
        rel.relids = Some(bms_union(&bms_make_singleton(1), &bms_make_singleton(2)));
        rel.reltarget = Some(Box::new(pathtarget(vec![var(1, 1), var(2, 1)])));
        rel
    }

    /// A join `Path` of `pathtype` over the two scan subpaths, carrying the equality
    /// join clause and (for merge/hash) the merge/hash detail.
    fn join_path(pathtype: PathType) -> Path {
        let jr = joinrel();
        let merge = (pathtype == PathType::MergeJoin).then(|| MergePathDetail {
            path_mergeclauses: vec![eq_clause()],
            outersortkeys: vec![Box::new(dummy_pathkey())],
            innersortkeys: vec![Box::new(dummy_pathkey())],
            outer_presorted_keys: 0,
            skip_mark_restore: false,
            materialize_inner: false,
        });
        let hash = (pathtype == PathType::HashJoin).then(|| HashPathDetail {
            path_hashclauses: vec![eq_clause()],
            num_batches: 1,
            inner_rows_total: 0.0,
        });
        let pathtarget = jr.reltarget.clone();
        Path {
            pathtype,
            parent: Some(Box::new(jr)),
            pathtarget,
            param_info: None,
            parallel_aware: false,
            parallel_safe: false,
            parallel_workers: 0,
            rows: 1.0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 2.0,
            pathkeys: Vec::new(),
            index_detail: None,
            join_detail: Some(Box::new(JoinPathDetail {
                jointype: JoinType::INNER,
                inner_unique: false,
                outerjoinpath: Box::new(scan_path(1)),
                innerjoinpath: Box::new(scan_path(2)),
                joinrestrictinfo: vec![eq_clause()],
                merge,
                hash,
            })),
        }
    }

    fn dummy_pathkey() -> crate::nodes::pathnodes::PathKey {
        crate::nodes::pathnodes::PathKey {
            eclass: Box::new(empty_eclass()),
            opfamily: Oid::new(1976),
            cmptype: crate::access::cmptype::CompareType::Lt,
            nulls_first: false,
        }
    }

    fn empty_eclass() -> crate::nodes::pathnodes::EquivalenceClass {
        crate::nodes::pathnodes::EquivalenceClass {
            opfamilies: Vec::new(),
            collation: InvalidOid,
            childmembers_size: 0,
            members: Vec::new(),
            childmembers: Vec::new(),
            sources: Vec::new(),
            derives_list: Vec::new(),
            relids: None,
            has_const: false,
            has_volatile: false,
            broken: false,
            sortref: 0,
            min_security: 0,
            max_security: 0,
            merged: None,
        }
    }

    fn root() -> PlannerInfo {
        crate::backend::optimizer::plan::initsplan::tests::test_planner_info()
    }

    #[test]
    fn create_nestloop_plan_builds_two_seqscan_children() {
        let mut root = root();
        let Node::NestLoop(nl) = create_plan_recurse(&mut root, &join_path(PathType::NestLoop))
        else {
            panic!("not a NestLoop");
        };
        // The join's targetlist is the joinrel target (two columns).
        assert_eq!(nl.join.plan.targetlist.len(), 2);
        assert_eq!(nl.join.jointype, JoinType::INNER);
        // The joinqual is the one equality clause.
        assert_eq!(nl.join.joinqual.len(), 1);
        // Outer + inner subplans are SeqScans over rel1 / rel2.
        assert!(matches!(nl.join.plan.lefttree.as_ref(), Some(Node::SeqScan(_))));
        assert!(matches!(nl.join.plan.righttree.as_ref(), Some(Node::SeqScan(_))));
        assert!(nl.nest_params.is_empty());
    }

    #[test]
    fn create_hashjoin_plan_builds_hash_node_on_inner() {
        let mut root = root();
        let Node::HashJoin(hj) = create_plan_recurse(&mut root, &join_path(PathType::HashJoin))
        else {
            panic!("not a HashJoin");
        };
        assert_eq!(hj.hashclauses.len(), 1);
        assert_eq!(hj.hashoperators, vec![INT4_EQ]);
        assert_eq!(hj.hashkeys.len(), 1, "outer hash keys");
        // Outer is a SeqScan; inner is a Hash node over a SeqScan.
        assert!(matches!(hj.join.plan.lefttree.as_ref(), Some(Node::SeqScan(_))));
        let Some(Node::Hash(hash)) = hj.join.plan.righttree.as_ref() else {
            panic!("inner is not a Hash node");
        };
        assert_eq!(hash.hashkeys.len(), 1, "inner hash keys");
        assert!(matches!(hash.plan.lefttree.as_ref(), Some(Node::SeqScan(_))));
    }

    #[test]
    fn create_mergejoin_plan_builds_mergeclauses_and_sorts() {
        let mut root = root();
        let Node::MergeJoin(mj) = create_plan_recurse(&mut root, &join_path(PathType::MergeJoin))
        else {
            panic!("not a MergeJoin");
        };
        assert_eq!(mj.mergeclauses.len(), 1);
        assert_eq!(mj.merge_families.len(), 1);
        // Both sides sorted (outersortkeys/innersortkeys non-empty -> Sort nodes).
        assert!(matches!(mj.join.plan.lefttree.as_ref(), Some(Node::Sort(_))));
        assert!(matches!(mj.join.plan.righttree.as_ref(), Some(Node::Sort(_))));
    }

    /// A base scan RelOptInfo for `relid` with one seqscan path + cheapest set.
    fn scan_rel_with_seqscan(root: &mut PlannerInfo, relid: i32, tuples: f64) -> RelOptInfo {
        use crate::backend::optimizer::util::pathnode::{add_path, create_seqscan_path, set_cheapest};
        let mut rel = make_node_reloptinfo(RelOptKind::BASEREL);
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel.rtekind = crate::nodes::parsenodes::RTEKind::RELATION;
        rel.rows = tuples;
        rel.tuples = tuples;
        rel.pages = 1;
        rel.min_attr = 1;
        rel.max_attr = 1;
        rel.attr_needed = vec![Some(bms_make_singleton(0))];
        rel.attr_widths = vec![4];
        rel.reltarget = Some(Box::new(pathtarget(vec![var(relid, 1)])));
        let p = create_seqscan_path(root, &rel, 0);
        add_path(&mut rel, p);
        set_cheapest(&mut rel);
        rel
    }

    /// The `rel1.col1 = rel2.col1` join clause with merge/hash fields set (as
    /// initsplan would) and ECs initialized (as distribute_qual_to_rels would).
    fn join_clause(root: &mut PlannerInfo) -> RestrictInfo {
        let mut ri = *eq_clause();
        ri.clause_relids = Some(bms_union(&bms_make_singleton(1), &bms_make_singleton(2)));
        ri.required_relids = ri.clause_relids.clone();
        ri.mergeopfamilies = crate::backend::utils::cache::lsyscache::get_mergejoin_opfamilies(INT4_EQ);
        ri.hashjoinoperator = INT4_EQ;
        crate::backend::optimizer::path::pathkeys::initialize_mergeclause_eclasses(root, &mut ri);
        ri
    }

    /// query_planner over two base rels + `a.x = b.y`: the join search builds a
    /// joinrel; its cheapest path turns into a join plan (NestLoop/Hash/MergeJoin)
    /// over the two SeqScans.
    #[test]
    fn two_rel_join_search_plans_to_join_over_two_seqscans() {
        let mut root = root();
        root.ec_merging_done = true;
        root.all_query_rels = Some(bms_union(&bms_make_singleton(1), &bms_make_singleton(2)));

        let mut rel1 = scan_rel_with_seqscan(&mut root, 1, 10.0);
        let mut rel2 = scan_rel_with_seqscan(&mut root, 2, 20.0);
        let ri = join_clause(&mut root);
        rel1.joininfo.push(Box::new(ri.clone()));
        rel2.joininfo.push(Box::new(ri));
        root.simple_rel_array = vec![None, Some(Box::new(rel1.clone())), Some(Box::new(rel2.clone()))];
        root.simple_rte_array = vec![None, None, None];

        // The join-rel target is the two scanned columns (so build_path_tlist works).
        let mut joinrel = crate::backend::optimizer::path::joinrels::make_join_rel(&mut root, &rel1, &rel2);
        joinrel.reltarget = Some(Box::new(pathtarget(vec![var(1, 1), var(2, 1)])));
        // Re-stamp the joinrel target onto the cheapest path's pathtarget.
        let mut best = (**joinrel.cheapest_total_path.as_ref().expect("cheapest path")).clone();
        best.pathtarget = joinrel.reltarget.clone();

        let plan = create_plan_recurse(&mut root, &best);

        // The plan is one of the three join nodes over two SeqScan-derived children.
        let (left, right) = match &plan {
            Node::NestLoop(n) => (n.join.plan.lefttree.as_ref(), n.join.plan.righttree.as_ref()),
            Node::HashJoin(h) => (h.join.plan.lefttree.as_ref(), h.join.plan.righttree.as_ref()),
            Node::MergeJoin(m) => (m.join.plan.lefttree.as_ref(), m.join.plan.righttree.as_ref()),
            other => panic!("expected a join plan, got {other:?}"),
        };
        // Outer child is a SeqScan; inner child is a SeqScan (nestloop/merge) or a
        // Hash over a SeqScan (hashjoin) or a Sort over a SeqScan (mergejoin).
        assert!(matches!(left, Some(Node::SeqScan(_) | Node::Sort(_))));
        assert!(matches!(
            right,
            Some(Node::SeqScan(_) | Node::Hash(_) | Node::Sort(_))
        ));
    }
}

//! createplan.rs
//!   Routines to create the desired plan for processing a query.
//!
//! Translated 1:1 from postgres/src/backend/optimizer/plan/createplan.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/createplan.c

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_assignments)]

use crate::prelude::*;
use crate::miscadmin::check_stack_depth;
use crate::list_make1;
type Relids = *mut Bitmapset;
unsafe fn errcode(_c: c_int) -> c_int { 0 }
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int { 0 }

use crate::{
    foreach, forboth, current_cell, IsA, makeNode, castNode, lfirst_node,
    Assert, elog, ereport, errmsg,
};

use std::ffi::{c_char, c_int, c_void, CStr};
use std::os::raw::c_long;

use crate::c::{int16, uint16, int32, uint32, int64, uint64, Index, Size};
use crate::access::attnum::AttrNumber;
use crate::postgres_ext::Oid;

use crate::utils::fmgr::FunctionCallInfo;

use crate::nodes::nodes::{
    Node, NodeTag, nodeTag, Cost,
    JoinType, JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_RIGHT, JOIN_SEMI, JOIN_ANTI,
    CmdType, CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, CMD_MERGE, CMD_UTILITY,
    AggStrategy, AGG_PLAIN, AGG_SORTED, AGG_HASHED, AGG_MIXED,
    AggSplit, AGGSPLIT_SIMPLE,
    SetOpCmd, SetOpStrategy, SETOP_SORTED, SETOP_HASHED,
    OnConflictAction, ONCONFLICT_NONE,
    LimitOption, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
};
use crate::nodes::pg_list::{
    List, ListCell, NIL, lfirst, lfirst_int, lfirst_oid, lsecond,
    linitial, linitial_int, list_head, list_length, list_nth_int, lnext,
    lappend, lappend_int, lappend_oid, list_concat, list_concat_copy,
    list_concat_unique, list_copy, list_copy_head,
    list_member, list_member_ptr, list_difference, list_difference_ptr,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_is_member, bms_is_empty, bms_is_subset,
    bms_add_member, bms_union, bms_free, bms_difference, bms_nonempty_difference,
    bms_make_singleton, bms_next_member,
};

use crate::nodes::primnodes::{
    Var, Const, Expr, TargetEntry, OpExpr, RelabelType, RowCompareExpr,
    ScalarArrayOpExpr, NullTest, TableFunc, OnConflictExpr, FuncExpr,
    INDEX_VAR, IS_SPECIAL_VARNO,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RTEPermissionInfo, SortGroupClause, WindowClause,
    CommonTableExpr, TableSampleClause,
    RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES,
    RTE_CTE, RTE_NAMEDTUPLESTORE, RTE_RESULT,
};
use crate::nodes::plannodes::{
    Plan, Scan, SeqScan, SampleScan, IndexScan, IndexOnlyScan, BitmapIndexScan,
    BitmapHeapScan, TidScan, TidRangeScan, SubqueryScan, FunctionScan,
    TableFuncScan, ValuesScan, CteScan, NamedTuplestoreScan, WorkTableScan,
    ForeignScan, CustomScan, Join, NestLoop, MergeJoin, HashJoin, NestLoopParam,
    Gather, GatherMerge, Hash, Memoize, Material, Sort, IncrementalSort, Unique,
    SetOp, LockRows, Limit, Agg, Group, WindowAgg, Result, ProjectSet,
    ModifyTable, Append, MergeAppend, RecursiveUnion, BitmapAnd, BitmapOr,
    SUBQUERY_SCAN_UNKNOWN,
};
use crate::nodes::pathnodes::{
    PlannerInfo, PlannerGlobal, RelOptInfo, IndexOptInfo, ParamPathInfo,
    Path, IndexPath, BitmapHeapPath, BitmapAndPath, BitmapOrPath, TidPath,
    TidRangePath, SubqueryScanPath, ForeignPath, CustomPath, AppendPath,
    MergeAppendPath, GroupResultPath, MaterialPath, MemoizePath, UniquePath,
    GatherPath, GatherMergePath, ProjectionPath, ProjectSetPath, SortPath,
    IncrementalSortPath, GroupPath, UpperUniquePath, AggPath, GroupingSetsPath,
    MinMaxAggPath, WindowAggPath, SetOpPath, RecursiveUnionPath, LockRowsPath,
    ModifyTablePath, LimitPath, JoinPath, NestPath, MergePath, HashPath,
    PathTarget, PathKey, EquivalenceClass, EquivalenceMember, RollupData,
    MinMaxAggInfo, PlaceHolderVar, PlaceHolderInfo, RestrictInfo, IndexClause,
    UNIQUE_PATH_NOOP, UNIQUE_PATH_HASH, UNIQUE_PATH_SORT,
    RELOPT_BASEREL, RELOPT_UPPER_REL, RELOPT_JOINREL, RELOPT_OTHER_JOINREL,
    RELOPT_OTHER_MEMBER_REL, RELOPT_OTHER_UPPER_REL,
};

// ---------------------------------------------------------------------------
// Flag bits that can appear in the flags argument of create_plan_recurse().
// These can be OR-ed together.
//
// CP_EXACT_TLIST specifies that the generated plan node must return exactly
// the tlist specified by the path's pathtarget (this overrides both
// CP_SMALL_TLIST and CP_LABEL_TLIST, if those are set).  Otherwise, the
// plan node is allowed to return just the Vars and PlaceHolderVars needed
// to evaluate the pathtarget.
//
// CP_SMALL_TLIST specifies that a narrower tlist is preferred.  This is
// passed down by parent nodes such as Sort and Hash, which will have to
// store the returned tuples.
//
// CP_LABEL_TLIST specifies that the plan node must return columns matching
// any sortgrouprefs specified in its pathtarget, with appropriate
// ressortgroupref labels.  This is passed down by parent nodes such as Sort
// and Group, which need these values to be available in their inputs.
//
// CP_IGNORE_TLIST specifies that the caller plans to replace the targetlist,
// and therefore it doesn't matter a bit what target list gets generated.
// ---------------------------------------------------------------------------
const CP_EXACT_TLIST: c_int = 0x0001; /* Plan must return specified tlist */
const CP_SMALL_TLIST: c_int = 0x0002; /* Prefer narrower tlists */
const CP_LABEL_TLIST: c_int = 0x0004; /* tlist must contain sortgrouprefs */
const CP_IGNORE_TLIST: c_int = 0x0008; /* caller will replace tlist */

// ===========================================================================
// External GUC variables and constants (referenced from this module).
// TODO(pg-port): wire these up to their real homes once ported.
// ===========================================================================
extern "C" {
    static enable_async_append: bool;
    static enable_partition_pruning: bool;
    static enable_incremental_sort: bool;
    static enable_sort: bool;
    static work_mem: c_int;
    static cpu_operator_cost: f64;
    static restrict_nonsystem_relation_kind: c_int;
}
const RESTRICT_RELKIND_FOREIGN_TABLE: c_int = 0x02;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const FirstNormalObjectId: Oid = 16384;
const FirstLowInvalidHeapAttributeNumber: c_int = -8;
const InvalidOid: Oid = 0;
const InvalidAttrNumber: AttrNumber = 0;
const CUSTOMPATH_SUPPORT_PROJECTION: c_int = 0x0002;

// ---------------------------------------------------------------------------
// ScanDirection (access/sdir.h)
// ---------------------------------------------------------------------------
use crate::access::sdir::{ScanDirection, ForwardScanDirection, BackwardScanDirection};

// ---------------------------------------------------------------------------
// CompareType / strategy constants (access/cmptype.h)
// ---------------------------------------------------------------------------
const COMPARE_EQ: c_int = 3;
const COMPARE_GT: c_int = 5;

// ===========================================================================
// Stubs for not-yet-ported callees.  TODO(pg-port): replace with real ports.
// ===========================================================================

/// `copyObject()` (nodes/copyfuncs.c): deep copy of a node tree.  Not yet
/// ported; this is a shallow-copy stub like in sibling optimizer files.
/// TODO(pg-port): replace with the real recursive copyObject once copyfuncs.c
/// is translated.
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    let p = palloc(core::mem::size_of::<T>()) as *mut T;
    core::ptr::copy_nonoverlapping(node, p, 1);
    p
}

// --- nodeFuncs.h ---
use crate::nodes::nodeFuncs::{exprType, exprCollation};

// --- nodes/makefuncs.h ---
use crate::nodes::makefuncs::{makeVar, makeTargetEntry, make_ands_explicit};
// TODO(pg-port): real makeBoolConst lives in nodes/makefuncs.c
unsafe fn makeBoolConst(value: bool, isnull: bool) -> *mut Node {
    unimplemented!()
}

// --- optimizer/clauses.h ---
// TODO(pg-port): real make_orclause lives in optimizer/util/clauses.c
unsafe fn make_orclause(orclauses: *mut List) -> *mut Expr { unimplemented!() }
// TODO(pg-port): real contain_mutable_functions lives in optimizer/util/clauses.c
unsafe fn contain_mutable_functions(clause: *mut Node) -> bool { unimplemented!() }
// TODO(pg-port): real is_parallel_safe lives in optimizer/util/clauses.c
unsafe fn is_parallel_safe(root: *mut PlannerInfo, node: *mut Node) -> bool { unimplemented!() }
// TODO(pg-port): real is_opclause lives in nodes/nodeFuncs.h
unsafe fn is_opclause(clause: *const c_void) -> bool { unimplemented!() }
// TODO(pg-port): real CommuteOpExpr lives in optimizer/util/clauses.c
unsafe fn CommuteOpExpr(clause: *mut OpExpr) { unimplemented!() }

// --- optimizer/cost.h ---
use crate::optimizer::cost::{cost_qual_eval_node};
// TODO(pg-port): real cost_sort lives in optimizer/path/costsize.c
unsafe fn cost_sort(
    path: *mut Path, root: *mut PlannerInfo, pathkeys: *mut List,
    disabled_nodes: c_int, input_cost: Cost, tuples: f64, width: c_int,
    comparison_cost: Cost, sort_mem: c_int, limit_tuples: f64,
) { unimplemented!() }
// TODO(pg-port): real cost_incremental_sort lives in optimizer/path/costsize.c
unsafe fn cost_incremental_sort(
    path: *mut Path, root: *mut PlannerInfo, pathkeys: *mut List,
    presorted_keys: c_int, disabled_nodes: c_int,
    input_startup_cost: Cost, input_total_cost: Cost,
    input_tuples: f64, width: c_int, comparison_cost: Cost,
    sort_mem: c_int, limit_tuples: f64,
) { unimplemented!() }
// TODO(pg-port): real cost_material lives in optimizer/path/costsize.c
unsafe fn cost_material(
    path: *mut Path, disabled_nodes: c_int, input_startup_cost: Cost,
    input_total_cost: Cost, tuples: f64, width: c_int,
) { unimplemented!() }

// --- optimizer/optimizer.h ---
// TODO(pg-port): real clamp_row_est lives in optimizer/path/costsize.c
unsafe fn clamp_row_est(nrows: f64) -> f64 { unimplemented!() }
// TODO(pg-port): real clamp_cardinality_to_long lives in optimizer/path/costsize.c
unsafe fn clamp_cardinality_to_long(nrows: f64) -> c_long { unimplemented!() }

// --- QualCost (nodes/pathnodes.h) ---
#[repr(C)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

// --- optimizer/paramassign.h ---
// TODO(pg-port): real symbols live in optimizer/util/paramassign.rs
unsafe fn replace_nestloop_param_var(root: *mut PlannerInfo, var: *mut Var) -> *mut crate::nodes::primnodes::Param { unimplemented!() }
unsafe fn replace_nestloop_param_placeholdervar(root: *mut PlannerInfo, phv: *mut PlaceHolderVar) -> *mut crate::nodes::primnodes::Param { unimplemented!() }
unsafe fn process_subquery_nestloop_params(root: *mut PlannerInfo, subplan_params: *mut List) { unimplemented!() }
unsafe fn identify_current_nestloop_params(root: *mut PlannerInfo, leftrelids: Relids, outerrelids: Relids) -> *mut List { unimplemented!() }
unsafe fn assign_special_exec_param(root: *mut PlannerInfo) -> c_int { unimplemented!() }

// --- optimizer/pathnode.h ---
// TODO(pg-port): real reparameterize_path_by_child lives in optimizer/util/pathnode.c
unsafe fn reparameterize_path_by_child(root: *mut PlannerInfo, path: *mut Path, child_rel: *mut RelOptInfo) -> *mut Path { unimplemented!() }

// --- optimizer/paths.h ---
// TODO(pg-port): real pathkeys_contained_in lives in optimizer/path/pathkeys.c
unsafe fn pathkeys_contained_in(keys1: *mut List, keys2: *mut List) -> bool { unimplemented!() }
// TODO(pg-port): real find_ec_member_matching_expr lives in optimizer/path/equivclass.c
unsafe fn find_ec_member_matching_expr(ec: *mut EquivalenceClass, expr: *mut Expr, relids: Relids) -> *mut EquivalenceMember { unimplemented!() }
// TODO(pg-port): real find_computable_ec_member lives in optimizer/path/equivclass.c
unsafe fn find_computable_ec_member(root: *mut PlannerInfo, ec: *mut EquivalenceClass, exprs: *mut List, relids: Relids, require_parallel_safe: bool) -> *mut EquivalenceMember { unimplemented!() }

// --- optimizer/placeholder.h ---
// TODO(pg-port): real find_placeholder_info lives in optimizer/util/placeholder.c
unsafe fn find_placeholder_info(root: *mut PlannerInfo, phv: *mut PlaceHolderVar) -> *mut PlaceHolderInfo { unimplemented!() }

// --- optimizer/plancat.h ---
// TODO(pg-port): real build_physical_tlist lives in optimizer/util/plancat.c
unsafe fn build_physical_tlist(root: *mut PlannerInfo, rel: *mut RelOptInfo) -> *mut List { unimplemented!() }
// TODO(pg-port): real infer_arbiter_indexes lives in optimizer/util/plancat.c
unsafe fn infer_arbiter_indexes(root: *mut PlannerInfo) -> *mut List { unimplemented!() }
// TODO(pg-port): real has_row_triggers lives in optimizer/util/plancat.c
unsafe fn has_row_triggers(root: *mut PlannerInfo, rti: Index, event: CmdType) -> bool { unimplemented!() }
// TODO(pg-port): real has_stored_generated_columns lives in optimizer/util/plancat.c
unsafe fn has_stored_generated_columns(root: *mut PlannerInfo, rti: Index) -> bool { unimplemented!() }

// --- optimizer/prep.h ---
// TODO(pg-port): real make_partition_pruneinfo lives in partitioning/partprune.c
unsafe fn make_partition_pruneinfo(root: *mut PlannerInfo, parentrel: *mut RelOptInfo, subpaths: *mut List, prunequal: *mut List) -> c_int { unimplemented!() }

// --- optimizer/restrictinfo.h ---
use crate::optimizer::util::restrictinfo::{extract_actual_clauses, extract_actual_join_clauses, get_actual_clauses};
// TODO(pg-port): real is_redundant_with_indexclauses lives in optimizer/util/restrictinfo.c
unsafe fn is_redundant_with_indexclauses(rinfo: *mut RestrictInfo, indexclauses: *mut List) -> bool { unimplemented!() }
// TODO(pg-port): real is_redundant_derived_clause lives in optimizer/util/restrictinfo.c
unsafe fn is_redundant_derived_clause(rinfo: *mut RestrictInfo, clauselist: *mut List) -> bool { unimplemented!() }

// --- optimizer/subselect.h ---
// TODO(pg-port): real symbols live in optimizer/plan/subselect.c
unsafe fn SS_attach_initplans(root: *mut PlannerInfo, plan: *mut Plan) { unimplemented!() }
unsafe fn SS_make_initplan_from_plan(root: *mut PlannerInfo, subroot: *mut PlannerInfo, plan: *mut Plan, prm: *mut crate::nodes::primnodes::Param) { unimplemented!() }
unsafe fn SS_compute_initplan_cost(init_plans: *mut List, initplan_cost_p: *mut Cost, unsafe_initplans_p: *mut bool) { unimplemented!() }
// TODO(pg-port): real pull_paramids lives in optimizer/plan/subselect.c
unsafe fn pull_paramids(expr: *mut Expr) -> *mut Bitmapset { unimplemented!() }

// --- optimizer/tlist.h ---
use crate::optimizer::util::tlist::{
    apply_tlist_labeling, tlist_member, apply_pathtarget_labeling_to_tlist,
    make_tlist_from_pathtarget,
};
// TODO(pg-port): real symbols live in optimizer/util/tlist.c
unsafe fn tlist_same_exprs(tlist1: *mut List, tlist2: *mut List) -> bool { unimplemented!() }
unsafe fn get_sortgroupclause_tle(sgClause: *mut SortGroupClause, targetList: *mut List) -> *mut TargetEntry { unimplemented!() }
unsafe fn get_sortgroupref_tle(sortref: Index, targetList: *mut List) -> *mut TargetEntry { unimplemented!() }
unsafe fn get_tle_by_resno(tlist: *mut List, resno: AttrNumber) -> *mut TargetEntry { unimplemented!() }
unsafe fn extract_grouping_cols(groupClause: *mut List, tlist: *mut List) -> *mut AttrNumber { unimplemented!() }
unsafe fn extract_grouping_ops(groupClause: *mut List) -> *mut Oid { unimplemented!() }
unsafe fn extract_grouping_collations(groupClause: *mut List, tlist: *mut List) -> *mut Oid { unimplemented!() }
unsafe fn extract_update_targetlist_colnos(tlist: *mut List) -> *mut List { unimplemented!() }

// --- parser/parse_clause.h ---
// TODO(pg-port): real assignSortGroupRef lives in parser/parse_clause.c
unsafe fn assignSortGroupRef(tle: *mut TargetEntry, tlist: *mut List) -> Index { unimplemented!() }

// --- nodes/nodeFuncs.h ---
// TODO(pg-port): real expression_tree_mutator lives in nodes/nodeFuncs.c
unsafe fn expression_tree_mutator(
    node: *mut Node,
    mutator: unsafe fn(*mut Node, *mut PlannerInfo) -> *mut Node,
    context: *mut PlannerInfo,
) -> *mut Node { unimplemented!() }

// --- optimizer/var.h ---
// TODO(pg-port): real pull_varattnos lives in optimizer/util/var.c
unsafe fn pull_varattnos(node: *mut Node, varno: Index, varattnos: *mut *mut Bitmapset) { unimplemented!() }

// --- partitioning / placeholder ---
// TODO(pg-port): real strip_phvs_in_index_operand lives in optimizer/util/placeholder.c
unsafe fn strip_phvs_in_index_operand(node: *mut Node) -> *mut Node { unimplemented!() }

// --- utils/lsyscache.h ---
// TODO(pg-port): real symbols live in utils/cache/lsyscache.c
unsafe fn get_compatible_hash_operators(opno: Oid, lhs_opno: *mut Oid, rhs_opno: *mut Oid) -> bool { unimplemented!() }
unsafe fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> Oid { unimplemented!() }
unsafe fn get_equality_op_for_ordering_op(opno: Oid, reverse: *mut bool) -> Oid { unimplemented!() }
unsafe fn get_opfamily_member_for_cmptype(opfamily: Oid, lefttype: Oid, righttype: Oid, cmptype: c_int) -> Oid { unimplemented!() }
unsafe fn get_rel_name(relid: Oid) -> *mut c_char { unimplemented!() }

// --- foreign/fdwapi.h ---
// TODO(pg-port): real GetFdwRoutineByRelId lives in foreign/foreign.c
unsafe fn GetFdwRoutineByRelId(relid: Oid) -> *mut c_void { unimplemented!() }

// --- rewrite/rewriteHandler.h / rewriteManip.h ---
// TODO(pg-port): real symbols live in rewrite/rewriteManip.c
unsafe fn contain_vars_returning_old_or_new(node: *mut Node) -> bool { unimplemented!() }
unsafe fn has_transition_tables(root: *mut PlannerInfo, rti: Index, event: CmdType) -> bool { unimplemented!() }

// --- optimizer/subselect.h (subquery scan trivial check) ---
// TODO(pg-port): real trivial_subqueryscan lives in optimizer/plan/setrefs.c
unsafe fn trivial_subqueryscan(plan: *mut SubqueryScan) -> bool { unimplemented!() }

// --- predicate prover ---
// TODO(pg-port): real predicate_implied_by lives in optimizer/util/predtest.c
unsafe fn predicate_implied_by(predicate_list: *mut List, clause_list: *mut List, weak: bool) -> bool { unimplemented!() }

// ---------------------------------------------------------------------------
// Helper macros translated from C macros in pathnodes.h / parsetree.h
// ---------------------------------------------------------------------------

// IS_JOIN_REL: rel->reloptkind == RELOPT_JOINREL || RELOPT_OTHER_JOINREL
unsafe fn IS_JOIN_REL(rel: *mut RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_JOINREL || (*rel).reloptkind == RELOPT_OTHER_JOINREL
}

// IS_OTHER_REL: rel->reloptkind == RELOPT_OTHER_MEMBER_REL || RELOPT_OTHER_JOINREL || RELOPT_OTHER_UPPER_REL
unsafe fn IS_OTHER_REL(rel: *mut RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_OTHER_MEMBER_REL
        || (*rel).reloptkind == RELOPT_OTHER_JOINREL
        || (*rel).reloptkind == RELOPT_OTHER_UPPER_REL
}

// IS_OUTER_JOIN(jointype)
unsafe fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    (1 << (jointype as u32)) & ((1 << (JOIN_LEFT as u32)) | (1 << (JOIN_FULL as u32))
        | (1 << (JOIN_RIGHT as u32)) | (1 << (JOIN_ANTI as u32))
        | (1 << (8u32 /* JOIN_RIGHT_ANTI */))) != 0
}

// IS_DUMMY_APPEND(path)
unsafe fn IS_DUMMY_APPEND(path: *mut Path) -> bool {
    IsA!(path, T_AppendPath) && (*(path as *mut AppendPath)).subpaths.is_null()
}

// PATH_REQ_OUTER(path): path->param_info ? path->param_info->ppi_req_outer : NULL
unsafe fn PATH_REQ_OUTER(path: *mut Path) -> Relids {
    if !(*path).param_info.is_null() {
        (*(*path).param_info).ppi_req_outer
    } else {
        core::ptr::null_mut()
    }
}

// planner_rt_fetch(rti, root)
unsafe fn planner_rt_fetch(rti: Index, root: *mut PlannerInfo) -> *mut RangeTblEntry {
    *(*root).simple_rte_array.add(rti as usize)
}

// ===========================================================================
// create_plan
//	  Creates the access plan for a query by recursively processing the
//	  desired tree of pathnodes, starting at the node 'best_path'.  For
//	  every pathnode found, we create a corresponding plan node containing
//	  appropriate id, target list, and qualification information.
//
//	  The tlists and quals in the plan tree are still in planner format,
//	  ie, Vars still correspond to the parser's numbering.  This will be
//	  fixed later by setrefs.c.
//
//	  best_path is the best access path
//
//	  Returns a Plan tree.
// ===========================================================================
pub unsafe fn create_plan(root: *mut PlannerInfo, best_path: *mut Path) -> *mut Plan {
    let plan: *mut Plan;

    /* plan_params should not be in use in current query level */
    Assert!((*root).plan_params == NIL);

    /* Initialize this module's workspace in PlannerInfo */
    (*root).curOuterRels = core::ptr::null_mut();
    (*root).curOuterParams = NIL;

    /* Recursively process the path tree, demanding the correct tlist result */
    let plan = create_plan_recurse(root, best_path, CP_EXACT_TLIST);

    /*
     * Make sure the topmost plan node's targetlist exposes the original
     * column names and other decorative info.  Targetlists generated within
     * the planner don't bother with that stuff, but we must have it on the
     * top-level tlist seen at execution time.  However, ModifyTable plan
     * nodes don't have a tlist matching the querytree targetlist.
     */
    if !IsA!(plan, T_ModifyTable) {
        apply_tlist_labeling((*plan).targetlist, (*root).processed_tlist);
    }

    /*
     * Attach any initPlans created in this query level to the topmost plan
     * node.
     */
    SS_attach_initplans(root, plan);

    /* Check we successfully assigned all NestLoopParams to plan nodes */
    if (*root).curOuterParams != NIL {
        elog!(ERROR, "failed to assign all NestLoopParams to plan nodes");
    }

    /*
     * Reset plan_params to ensure param IDs used for nestloop params are not
     * re-used later
     */
    (*root).plan_params = NIL;

    plan
}

// create_plan_recurse
//	  Recursive guts of create_plan().
unsafe fn create_plan_recurse(root: *mut PlannerInfo, best_path: *mut Path, flags: c_int) -> *mut Plan {
    let plan: *mut Plan;

    /* Guard against stack overflow due to overly complex plans */
    check_stack_depth();

    match (*best_path).pathtype {
        NodeTag::T_SeqScan
        | NodeTag::T_SampleScan
        | NodeTag::T_IndexScan
        | NodeTag::T_IndexOnlyScan
        | NodeTag::T_BitmapHeapScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_SubqueryScan
        | NodeTag::T_FunctionScan
        | NodeTag::T_TableFuncScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_WorkTableScan
        | NodeTag::T_NamedTuplestoreScan
        | NodeTag::T_ForeignScan
        | NodeTag::T_CustomScan => {
            plan = create_scan_plan(root, best_path, flags);
        }
        NodeTag::T_HashJoin | NodeTag::T_MergeJoin | NodeTag::T_NestLoop => {
            plan = create_join_plan(root, best_path as *mut JoinPath);
        }
        NodeTag::T_Append => {
            plan = create_append_plan(root, best_path as *mut AppendPath, flags);
        }
        NodeTag::T_MergeAppend => {
            plan = create_merge_append_plan(root, best_path as *mut MergeAppendPath, flags);
        }
        NodeTag::T_Result => {
            if IsA!(best_path, T_ProjectionPath) {
                plan = create_projection_plan(root, best_path as *mut ProjectionPath, flags);
            } else if IsA!(best_path, T_MinMaxAggPath) {
                plan = create_minmaxagg_plan(root, best_path as *mut MinMaxAggPath) as *mut Plan;
            } else if IsA!(best_path, T_GroupResultPath) {
                plan = create_group_result_plan(root, best_path as *mut GroupResultPath) as *mut Plan;
            } else {
                /* Simple RTE_RESULT base relation */
                Assert!(IsA!(best_path, T_Path));
                plan = create_scan_plan(root, best_path, flags);
            }
        }
        NodeTag::T_ProjectSet => {
            plan = create_project_set_plan(root, best_path as *mut ProjectSetPath) as *mut Plan;
        }
        NodeTag::T_Material => {
            plan = create_material_plan(root, best_path as *mut MaterialPath, flags) as *mut Plan;
        }
        NodeTag::T_Memoize => {
            plan = create_memoize_plan(root, best_path as *mut MemoizePath, flags) as *mut Plan;
        }
        NodeTag::T_Unique => {
            if IsA!(best_path, T_UpperUniquePath) {
                plan = create_upper_unique_plan(root, best_path as *mut UpperUniquePath, flags) as *mut Plan;
            } else {
                Assert!(IsA!(best_path, T_UniquePath));
                plan = create_unique_plan(root, best_path as *mut UniquePath, flags);
            }
        }
        NodeTag::T_Gather => {
            plan = create_gather_plan(root, best_path as *mut GatherPath) as *mut Plan;
        }
        NodeTag::T_Sort => {
            plan = create_sort_plan(root, best_path as *mut SortPath, flags) as *mut Plan;
        }
        NodeTag::T_IncrementalSort => {
            plan = create_incrementalsort_plan(root, best_path as *mut IncrementalSortPath, flags) as *mut Plan;
        }
        NodeTag::T_Group => {
            plan = create_group_plan(root, best_path as *mut GroupPath) as *mut Plan;
        }
        NodeTag::T_Agg => {
            if IsA!(best_path, T_GroupingSetsPath) {
                plan = create_groupingsets_plan(root, best_path as *mut GroupingSetsPath);
            } else {
                Assert!(IsA!(best_path, T_AggPath));
                plan = create_agg_plan(root, best_path as *mut AggPath) as *mut Plan;
            }
        }
        NodeTag::T_WindowAgg => {
            plan = create_windowagg_plan(root, best_path as *mut WindowAggPath) as *mut Plan;
        }
        NodeTag::T_SetOp => {
            plan = create_setop_plan(root, best_path as *mut SetOpPath, flags) as *mut Plan;
        }
        NodeTag::T_RecursiveUnion => {
            plan = create_recursiveunion_plan(root, best_path as *mut RecursiveUnionPath) as *mut Plan;
        }
        NodeTag::T_LockRows => {
            plan = create_lockrows_plan(root, best_path as *mut LockRowsPath, flags) as *mut Plan;
        }
        NodeTag::T_ModifyTable => {
            plan = create_modifytable_plan(root, best_path as *mut ModifyTablePath) as *mut Plan;
        }
        NodeTag::T_Limit => {
            plan = create_limit_plan(root, best_path as *mut LimitPath, flags) as *mut Plan;
        }
        NodeTag::T_GatherMerge => {
            plan = create_gather_merge_plan(root, best_path as *mut GatherMergePath) as *mut Plan;
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", (*best_path).pathtype as c_int);
            unreachable!();
        }
    }

    plan
}

// create_scan_plan
//	 Create a scan plan for the parent relation of 'best_path'.
unsafe fn create_scan_plan(root: *mut PlannerInfo, best_path: *mut Path, mut flags: c_int) -> *mut Plan {
    let rel = (*best_path).parent;
    let mut scan_clauses: *mut List;
    let gating_clauses: *mut List;
    let tlist: *mut List;
    let plan: *mut Plan;

    /*
     * Extract the relevant restriction clauses from the parent relation.
     */
    match (*best_path).pathtype {
        NodeTag::T_IndexScan | NodeTag::T_IndexOnlyScan => {
            scan_clauses = (*(*castNode!(IndexPath, T_IndexPath, best_path)).indexinfo).indrestrictinfo;
        }
        _ => {
            scan_clauses = (*rel).baserestrictinfo;
        }
    }

    /*
     * If this is a parameterized scan, we also need to enforce all the join
     * clauses available from the outer relation(s).
     */
    if !(*best_path).param_info.is_null() {
        scan_clauses = list_concat_copy(scan_clauses, (*(*best_path).param_info).ppi_clauses);
    }

    /*
     * Detect whether we have any pseudoconstant quals to deal with.
     */
    if IS_JOIN_REL(rel) {
        let join_clauses: *mut List;

        Assert!((*best_path).pathtype == NodeTag::T_ForeignScan
            || (*best_path).pathtype == NodeTag::T_CustomScan);
        if (*best_path).pathtype == NodeTag::T_ForeignScan {
            join_clauses = (*(best_path as *mut ForeignPath)).fdw_restrictinfo;
        } else {
            join_clauses = (*(best_path as *mut CustomPath)).custom_restrictinfo;
        }

        gating_clauses = get_gating_quals(root, join_clauses);
    } else {
        gating_clauses = get_gating_quals(root, scan_clauses);
    }
    if !gating_clauses.is_null() {
        flags = 0;
    }

    /*
     * For table scans, rather than using the relation targetlist, we prefer
     * to generate a tlist containing all Vars in order.
     */
    if flags == CP_IGNORE_TLIST {
        tlist = core::ptr::null_mut();
    } else if use_physical_tlist(root, best_path, flags) {
        if (*best_path).pathtype == NodeTag::T_IndexOnlyScan {
            /* For index-only scan, the preferred tlist is the index's */
            tlist = copyObject((*(*(best_path as *mut IndexPath)).indexinfo).indextlist);

            if flags & CP_LABEL_TLIST != 0 {
                apply_pathtarget_labeling_to_tlist(tlist, (*best_path).pathtarget);
            }
        } else {
            let mut t = build_physical_tlist(root, rel);
            if t == NIL {
                /* Failed because of dropped cols, so use regular method */
                t = build_path_tlist(root, best_path);
            } else {
                /* As above, transfer sortgroupref data to replacement tlist */
                if flags & CP_LABEL_TLIST != 0 {
                    apply_pathtarget_labeling_to_tlist(t, (*best_path).pathtarget);
                }
            }
            tlist = t;
        }
    } else {
        tlist = build_path_tlist(root, best_path);
    }

    match (*best_path).pathtype {
        NodeTag::T_SeqScan => {
            plan = create_seqscan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_SampleScan => {
            plan = create_samplescan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_IndexScan => {
            plan = create_indexscan_plan(root, best_path as *mut IndexPath, tlist, scan_clauses, false) as *mut Plan;
        }
        NodeTag::T_IndexOnlyScan => {
            plan = create_indexscan_plan(root, best_path as *mut IndexPath, tlist, scan_clauses, true) as *mut Plan;
        }
        NodeTag::T_BitmapHeapScan => {
            plan = create_bitmap_scan_plan(root, best_path as *mut BitmapHeapPath, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_TidScan => {
            plan = create_tidscan_plan(root, best_path as *mut TidPath, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_TidRangeScan => {
            plan = create_tidrangescan_plan(root, best_path as *mut TidRangePath, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_SubqueryScan => {
            plan = create_subqueryscan_plan(root, best_path as *mut SubqueryScanPath, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_FunctionScan => {
            plan = create_functionscan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_TableFuncScan => {
            plan = create_tablefuncscan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_ValuesScan => {
            plan = create_valuesscan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_CteScan => {
            plan = create_ctescan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_NamedTuplestoreScan => {
            plan = create_namedtuplestorescan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_Result => {
            plan = create_resultscan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_WorkTableScan => {
            plan = create_worktablescan_plan(root, best_path, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_ForeignScan => {
            plan = create_foreignscan_plan(root, best_path as *mut ForeignPath, tlist, scan_clauses) as *mut Plan;
        }
        NodeTag::T_CustomScan => {
            plan = create_customscan_plan(root, best_path as *mut CustomPath, tlist, scan_clauses) as *mut Plan;
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", (*best_path).pathtype as c_int);
            unreachable!();
        }
    }

    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time quals.
     */
    if !gating_clauses.is_null() {
        return create_gating_plan(root, best_path, plan, gating_clauses);
    }

    plan
}

// Build a target list (ie, a list of TargetEntry) for the Path's output.
unsafe fn build_path_tlist(root: *mut PlannerInfo, path: *mut Path) -> *mut List {
    let mut tlist: *mut List = NIL;
    let sortgrouprefs = (*(*path).pathtarget).sortgrouprefs;
    let mut resno: c_int = 1;
    let mut v: *mut ListCell;

    foreach!(v, (*(*path).pathtarget).exprs, {
        let mut node = lfirst(current_cell!(v)) as *mut Node;
        let tle: *mut TargetEntry;

        if !(*path).param_info.is_null() {
            node = replace_nestloop_params(root, node);
        }

        tle = makeTargetEntry(node as *mut Expr, resno as AttrNumber, core::ptr::null_mut(), false);
        if !sortgrouprefs.is_null() {
            (*tle).ressortgroupref = *sortgrouprefs.add((resno - 1) as usize);
        }

        tlist = lappend(tlist, tle as *mut c_void);
        resno += 1;
    });
    tlist
}

// use_physical_tlist
unsafe fn use_physical_tlist(root: *mut PlannerInfo, path: *mut Path, flags: c_int) -> bool {
    let rel = (*path).parent;
    let mut i: c_int;
    let mut lc: *mut ListCell;

    if flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0 {
        return false;
    }

    if (*rel).rtekind != RTE_RELATION
        && (*rel).rtekind != RTE_SUBQUERY
        && (*rel).rtekind != RTE_FUNCTION
        && (*rel).rtekind != RTE_TABLEFUNC
        && (*rel).rtekind != RTE_VALUES
        && (*rel).rtekind != RTE_CTE
    {
        return false;
    }

    if (*rel).reloptkind != RELOPT_BASEREL {
        return false;
    }

    if IsA!(path, T_CustomPath) {
        return false;
    }

    if IsA!(path, T_BitmapHeapPath) && (*(*path).pathtarget).exprs == NIL {
        return false;
    }

    i = (*rel).min_attr as i32;
    while i <= 0 {
        if !bms_is_empty(*(*rel).attr_needed.add((i - (*rel).min_attr as i32) as usize)) {
            return false;
        }
        i += 1;
    }

    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(current_cell!(lc)) as *mut PlaceHolderInfo;

        if bms_nonempty_difference((*phinfo).ph_needed, (*rel).relids)
            && bms_is_subset((*phinfo).ph_eval_at, (*rel).relids)
        {
            return false;
        }
    });

    if (*path).pathtype == NodeTag::T_IndexOnlyScan {
        let indexinfo = (*(path as *mut IndexPath)).indexinfo;

        i = 0;
        while i < (*indexinfo).ncolumns {
            if !*(*indexinfo).canreturn.add(i as usize) {
                return false;
            }
            i += 1;
        }
    }

    if (flags & CP_LABEL_TLIST != 0) && !(*(*path).pathtarget).sortgrouprefs.is_null() {
        let mut sortgroupatts: *mut Bitmapset = core::ptr::null_mut();

        i = 0;
        foreach!(lc, (*(*path).pathtarget).exprs, {
            let expr = lfirst(current_cell!(lc)) as *mut Expr;

            if *(*(*path).pathtarget).sortgrouprefs.add(i as usize) != 0 {
                if !expr.is_null() && IsA!(expr, T_Var) {
                    let mut attno = (*(expr as *mut Var)).varattno as c_int;

                    attno -= FirstLowInvalidHeapAttributeNumber;
                    if bms_is_member(attno, sortgroupatts) {
                        return false;
                    }
                    sortgroupatts = bms_add_member(sortgroupatts, attno);
                } else {
                    return false;
                }
            }
            i += 1;
        });
    }

    true
}

// get_gating_quals
//	  See if there are pseudoconstant quals in a node's quals list
unsafe fn get_gating_quals(root: *mut PlannerInfo, mut quals: *mut List) -> *mut List {
    /* No need to look if we know there are no pseudoconstants */
    if !(*root).hasPseudoConstantQuals {
        return NIL;
    }

    /* Sort into desirable execution order while still in RestrictInfo form */
    quals = order_qual_clauses(root, quals);

    /* Pull out any pseudoconstant quals from the RestrictInfo list */
    extract_actual_clauses(quals, true)
}

// create_gating_plan
//	  Deal with pseudoconstant qual clauses
unsafe fn create_gating_plan(
    root: *mut PlannerInfo,
    path: *mut Path,
    plan: *mut Plan,
    gating_quals: *mut List,
) -> *mut Plan {
    let gplan: *mut Plan;
    let mut splan: *mut Plan;

    Assert!(!gating_quals.is_null());

    splan = plan;
    if IsA!(plan, T_Result) {
        let rplan = plan as *mut Result;

        if (*rplan).plan.lefttree.is_null() && (*rplan).resconstantqual.is_null() {
            splan = core::ptr::null_mut();
        }
    }

    gplan = make_result(build_path_tlist(root, path), gating_quals as *mut Node, splan) as *mut Plan;

    copy_plan_costsize(gplan, plan);

    /* Gating quals could be unsafe, so better use the Path's safety flag */
    (*gplan).parallel_safe = (*path).parallel_safe;

    gplan
}

// create_join_plan
//	  Create a join plan for 'best_path' and (recursively) plans for its
//	  inner and outer paths.
unsafe fn create_join_plan(root: *mut PlannerInfo, best_path: *mut JoinPath) -> *mut Plan {
    let mut plan: *mut Plan;
    let gating_clauses: *mut List;

    match (*best_path).path.pathtype {
        NodeTag::T_MergeJoin => {
            plan = create_mergejoin_plan(root, best_path as *mut MergePath) as *mut Plan;
        }
        NodeTag::T_HashJoin => {
            plan = create_hashjoin_plan(root, best_path as *mut HashPath) as *mut Plan;
        }
        NodeTag::T_NestLoop => {
            plan = create_nestloop_plan(root, best_path as *mut NestPath) as *mut Plan;
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", (*best_path).path.pathtype as c_int);
            unreachable!();
        }
    }

    gating_clauses = get_gating_quals(root, (*best_path).joinrestrictinfo);
    if !gating_clauses.is_null() {
        plan = create_gating_plan(root, best_path as *mut Path, plan, gating_clauses);
    }

    plan
}

// mark_async_capable_plan
unsafe fn mark_async_capable_plan(plan: *mut Plan, path: *mut Path) -> bool {
    match nodeTag(path as *mut Node) {
        NodeTag::T_SubqueryScanPath => {
            let scan_plan = plan as *mut SubqueryScan;

            if IsA!(plan, T_Result) {
                return false;
            }

            if trivial_subqueryscan(scan_plan)
                && mark_async_capable_plan(
                    (*scan_plan).subplan,
                    (*(path as *mut SubqueryScanPath)).subpath,
                )
            {
                // break;
            } else {
                return false;
            }
        }
        NodeTag::T_ForeignPath => {
            let fdwroutine = (*(*path).parent).fdwroutine;

            if IsA!(plan, T_Result) {
                return false;
            }

            Assert!(!fdwroutine.is_null());
            /* TODO(pg-port): IsForeignPathAsyncCapable callback dispatch */
            return false;
        }
        NodeTag::T_ProjectionPath => {
            if IsA!(plan, T_Result) {
                return false;
            }

            if mark_async_capable_plan(plan, (*(path as *mut ProjectionPath)).subpath) {
                return true;
            }
            return false;
        }
        _ => return false,
    }

    (*plan).async_capable = true;

    true
}

// create_append_plan
//	  Create an Append plan for 'best_path' and (recursively) plans
//	  for its subpaths.
unsafe fn create_append_plan(root: *mut PlannerInfo, best_path: *mut AppendPath, flags: c_int) -> *mut Plan {
    let plan: *mut Append;
    let mut tlist = build_path_tlist(root, &raw mut (*best_path).path);
    let orig_tlist_length = list_length(tlist);
    let mut tlist_was_changed = false;
    let pathkeys = (*best_path).path.pathkeys;
    let mut subplans: *mut List = NIL;
    let mut subpaths: *mut ListCell;
    let mut nasyncplans: c_int = 0;
    let rel = (*best_path).path.parent;
    let mut nodenumsortkeys: c_int = 0;
    let mut nodeSortColIdx: *mut AttrNumber = core::ptr::null_mut();
    let mut nodeSortOperators: *mut Oid = core::ptr::null_mut();
    let mut nodeCollations: *mut Oid = core::ptr::null_mut();
    let mut nodeNullsFirst: *mut bool = core::ptr::null_mut();
    let mut consider_async = false;

    /*
     * The subpaths list could be empty.  In that case generate a dummy plan
     * that returns no rows.
     */
    if (*best_path).subpaths == NIL {
        /* Generate a Result plan with constant-FALSE gating qual */
        let dplan: *mut Plan;

        dplan = make_result(
            tlist,
            list_make1!(makeBoolConst(false, false) as *mut c_void) as *mut Node,
            core::ptr::null_mut(),
        ) as *mut Plan;

        copy_generic_path_info(dplan, best_path as *mut Path);

        return dplan;
    }

    plan = makeNode!(Append, T_Append);
    (*plan).plan.targetlist = tlist;
    (*plan).plan.qual = NIL;
    (*plan).plan.lefttree = core::ptr::null_mut();
    (*plan).plan.righttree = core::ptr::null_mut();
    (*plan).apprelids = (*rel).relids;

    if pathkeys != NIL {
        prepare_sort_from_pathkeys(
            plan as *mut Plan,
            pathkeys,
            (*(*best_path).path.parent).relids,
            core::ptr::null(),
            true,
            &raw mut nodenumsortkeys,
            &raw mut nodeSortColIdx,
            &raw mut nodeSortOperators,
            &raw mut nodeCollations,
            &raw mut nodeNullsFirst,
        );
        tlist_was_changed = orig_tlist_length != list_length((*plan).plan.targetlist);
    }

    /* If appropriate, consider async append */
    consider_async = enable_async_append
        && pathkeys == NIL
        && !(*best_path).path.parallel_safe
        && list_length((*best_path).subpaths) > 1;

    /* Build the plan for each child */
    foreach!(subpaths, (*best_path).subpaths, {
        let subpath = lfirst(current_cell!(subpaths)) as *mut Path;
        let mut subplan: *mut Plan;

        /* Must insist that all children return the same tlist */
        subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

        if pathkeys != NIL {
            let mut numsortkeys: c_int = 0;
            let mut sortColIdx: *mut AttrNumber = core::ptr::null_mut();
            let mut sortOperators: *mut Oid = core::ptr::null_mut();
            let mut collations: *mut Oid = core::ptr::null_mut();
            let mut nullsFirst: *mut bool = core::ptr::null_mut();

            subplan = prepare_sort_from_pathkeys(
                subplan,
                pathkeys,
                (*(*subpath).parent).relids,
                nodeSortColIdx,
                false,
                &raw mut numsortkeys,
                &raw mut sortColIdx,
                &raw mut sortOperators,
                &raw mut collations,
                &raw mut nullsFirst,
            );

            Assert!(numsortkeys == nodenumsortkeys);
            if libc_memcmp(
                sortColIdx as *const c_void,
                nodeSortColIdx as *const c_void,
                numsortkeys as usize * core::mem::size_of::<AttrNumber>(),
            ) != 0
            {
                elog!(ERROR, "Append child's targetlist doesn't match Append");
            }
            Assert!(libc_memcmp(sortOperators as *const c_void, nodeSortOperators as *const c_void, numsortkeys as usize * core::mem::size_of::<Oid>()) == 0);
            Assert!(libc_memcmp(collations as *const c_void, nodeCollations as *const c_void, numsortkeys as usize * core::mem::size_of::<Oid>()) == 0);
            Assert!(libc_memcmp(nullsFirst as *const c_void, nodeNullsFirst as *const c_void, numsortkeys as usize * core::mem::size_of::<bool>()) == 0);

            /* Now, insert a Sort node if subplan isn't sufficiently ordered */
            if !pathkeys_contained_in(pathkeys, (*subpath).pathkeys) {
                let sort = make_sort(subplan, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst);

                label_sort_with_costsize(root, sort, (*best_path).limit_tuples);
                subplan = sort as *mut Plan;
            }
        }

        /* If needed, check to see if subplan can be executed asynchronously */
        if consider_async && mark_async_capable_plan(subplan, subpath) {
            Assert!((*subplan).async_capable);
            nasyncplans += 1;
        }

        subplans = lappend(subplans, subplan as *mut c_void);
    });

    /* Set below if we find quals that we can use to run-time prune */
    (*plan).part_prune_index = -1;

    if enable_partition_pruning {
        let mut prunequal: *mut List;

        prunequal = extract_actual_clauses((*rel).baserestrictinfo, false);

        if !(*best_path).path.param_info.is_null() {
            let mut prmquals = (*(*best_path).path.param_info).ppi_clauses;

            prmquals = extract_actual_clauses(prmquals, false);
            prmquals = replace_nestloop_params(root, prmquals as *mut Node) as *mut List;

            prunequal = list_concat(prunequal, prmquals);
        }

        if prunequal != NIL {
            (*plan).part_prune_index = make_partition_pruneinfo(root, rel, (*best_path).subpaths, prunequal);
        }
    }

    (*plan).appendplans = subplans;
    (*plan).nasyncplans = nasyncplans;
    (*plan).first_partial_plan = (*best_path).first_partial_path;

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    if tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0) {
        tlist = list_copy_head((*plan).plan.targetlist, orig_tlist_length);
        return inject_projection_plan(plan as *mut Plan, tlist, (*plan).plan.parallel_safe);
    } else {
        return plan as *mut Plan;
    }
}

// create_merge_append_plan
unsafe fn create_merge_append_plan(root: *mut PlannerInfo, best_path: *mut MergeAppendPath, flags: c_int) -> *mut Plan {
    let node = makeNode!(MergeAppend, T_MergeAppend);
    let plan = &raw mut (*node).plan;
    let mut tlist = build_path_tlist(root, &raw mut (*best_path).path);
    let orig_tlist_length = list_length(tlist);
    let tlist_was_changed: bool;
    let pathkeys = (*best_path).path.pathkeys;
    let mut subplans: *mut List = NIL;
    let mut subpaths: *mut ListCell;
    let rel = (*best_path).path.parent;

    copy_generic_path_info(plan, best_path as *mut Path);
    (*plan).targetlist = tlist;
    (*plan).qual = NIL;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).apprelids = (*rel).relids;

    prepare_sort_from_pathkeys(
        plan,
        pathkeys,
        (*(*best_path).path.parent).relids,
        core::ptr::null(),
        true,
        &raw mut (*node).numCols,
        &raw mut (*node).sortColIdx,
        &raw mut (*node).sortOperators,
        &raw mut (*node).collations,
        &raw mut (*node).nullsFirst,
    );
    tlist_was_changed = orig_tlist_length != list_length((*plan).targetlist);

    foreach!(subpaths, (*best_path).subpaths, {
        let subpath = lfirst(current_cell!(subpaths)) as *mut Path;
        let mut subplan: *mut Plan;
        let mut numsortkeys: c_int = 0;
        let mut sortColIdx: *mut AttrNumber = core::ptr::null_mut();
        let mut sortOperators: *mut Oid = core::ptr::null_mut();
        let mut collations: *mut Oid = core::ptr::null_mut();
        let mut nullsFirst: *mut bool = core::ptr::null_mut();

        /* Must insist that all children return the same tlist */
        subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

        subplan = prepare_sort_from_pathkeys(
            subplan,
            pathkeys,
            (*(*subpath).parent).relids,
            (*node).sortColIdx,
            false,
            &raw mut numsortkeys,
            &raw mut sortColIdx,
            &raw mut sortOperators,
            &raw mut collations,
            &raw mut nullsFirst,
        );

        Assert!(numsortkeys == (*node).numCols);
        if libc_memcmp(sortColIdx as *const c_void, (*node).sortColIdx as *const c_void, numsortkeys as usize * core::mem::size_of::<AttrNumber>()) != 0 {
            elog!(ERROR, "MergeAppend child's targetlist doesn't match MergeAppend");
        }
        Assert!(libc_memcmp(sortOperators as *const c_void, (*node).sortOperators as *const c_void, numsortkeys as usize * core::mem::size_of::<Oid>()) == 0);
        Assert!(libc_memcmp(collations as *const c_void, (*node).collations as *const c_void, numsortkeys as usize * core::mem::size_of::<Oid>()) == 0);
        Assert!(libc_memcmp(nullsFirst as *const c_void, (*node).nullsFirst as *const c_void, numsortkeys as usize * core::mem::size_of::<bool>()) == 0);

        /* Now, insert a Sort node if subplan isn't sufficiently ordered */
        if !pathkeys_contained_in(pathkeys, (*subpath).pathkeys) {
            let sort = make_sort(subplan, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst);

            label_sort_with_costsize(root, sort, (*best_path).limit_tuples);
            subplan = sort as *mut Plan;
        }

        subplans = lappend(subplans, subplan as *mut c_void);
    });

    /* Set below if we find quals that we can use to run-time prune */
    (*node).part_prune_index = -1;

    if enable_partition_pruning {
        let prunequal: *mut List;

        prunequal = extract_actual_clauses((*rel).baserestrictinfo, false);

        /* We don't currently generate any parameterized MergeAppend paths */
        Assert!((*best_path).path.param_info.is_null());

        if prunequal != NIL {
            (*node).part_prune_index = make_partition_pruneinfo(root, rel, (*best_path).subpaths, prunequal);
        }
    }

    (*node).mergeplans = subplans;

    if tlist_was_changed && (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST) != 0) {
        tlist = list_copy_head((*plan).targetlist, orig_tlist_length);
        return inject_projection_plan(plan, tlist, (*plan).parallel_safe);
    } else {
        return plan;
    }
}

// create_group_result_plan
unsafe fn create_group_result_plan(root: *mut PlannerInfo, best_path: *mut GroupResultPath) -> *mut Result {
    let plan: *mut Result;
    let tlist: *mut List;
    let quals: *mut List;

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    /* best_path->quals is just bare clauses */
    quals = order_qual_clauses(root, (*best_path).quals);

    plan = make_result(tlist, quals as *mut Node, core::ptr::null_mut());

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_project_set_plan
unsafe fn create_project_set_plan(root: *mut PlannerInfo, best_path: *mut ProjectSetPath) -> *mut ProjectSet {
    let plan: *mut ProjectSet;
    let subplan: *mut Plan;
    let tlist: *mut List;

    /* Since we intend to project, we don't need to constrain child tlist */
    subplan = create_plan_recurse(root, (*best_path).subpath, 0);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    plan = make_project_set(tlist, subplan);

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_material_plan
unsafe fn create_material_plan(root: *mut PlannerInfo, best_path: *mut MaterialPath, flags: c_int) -> *mut Material {
    let plan: *mut Material;
    let subplan: *mut Plan;

    subplan = create_plan_recurse(root, (*best_path).subpath, flags | CP_SMALL_TLIST);

    plan = make_material(subplan);

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_memoize_plan
unsafe fn create_memoize_plan(root: *mut PlannerInfo, best_path: *mut MemoizePath, flags: c_int) -> *mut Memoize {
    let plan: *mut Memoize;
    let keyparamids: *mut Bitmapset;
    let subplan: *mut Plan;
    let operators: *mut Oid;
    let collations: *mut Oid;
    let mut param_exprs: *mut List = NIL;
    let mut lc: *mut ListCell;
    let mut lc2: *mut ListCell;
    let nkeys: c_int;
    let mut i: c_int;

    subplan = create_plan_recurse(root, (*best_path).subpath, flags | CP_SMALL_TLIST);

    param_exprs = replace_nestloop_params(root, (*best_path).param_exprs as *mut Node) as *mut List;

    nkeys = list_length(param_exprs);
    Assert!(nkeys > 0);
    operators = palloc(nkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    collations = palloc(nkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;

    i = 0;
    forboth!(lc, param_exprs, lc2, (*best_path).hash_operators, {
        let param_expr = lfirst(lc) as *mut Expr;
        let opno = lfirst_oid(lc2);

        *operators.add(i as usize) = opno;
        *collations.add(i as usize) = exprCollation(param_expr as *mut Node);
        i += 1;
    });

    keyparamids = pull_paramids(param_exprs as *mut Expr);

    plan = make_memoize(
        subplan, operators, collations, param_exprs,
        (*best_path).singlerow, (*best_path).binary_mode,
        (*best_path).est_entries, keyparamids,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_unique_plan
unsafe fn create_unique_plan(root: *mut PlannerInfo, best_path: *mut UniquePath, flags: c_int) -> *mut Plan {
    let plan: *mut Plan;
    let mut subplan: *mut Plan;
    let in_operators: *mut List;
    let uniq_exprs: *mut List;
    let mut newtlist: *mut List;
    let mut nextresno: c_int;
    let mut newitems: bool;
    let numGroupCols: c_int;
    let groupColIdx: *mut AttrNumber;
    let groupCollations: *mut Oid;
    let mut groupColPos: c_int;
    let mut l: *mut ListCell;

    /* Unique doesn't project, so tlist requirements pass through */
    subplan = create_plan_recurse(root, (*best_path).subpath, flags);

    /* Done if we don't need to do any actual unique-ifying */
    if (*best_path).umethod == UNIQUE_PATH_NOOP {
        return subplan;
    }

    in_operators = (*best_path).in_operators;
    uniq_exprs = (*best_path).uniq_exprs;

    /* initialize modified subplan tlist as just the "required" vars */
    newtlist = build_path_tlist(root, &raw mut (*best_path).path);
    nextresno = list_length(newtlist) + 1;
    newitems = false;

    foreach!(l, uniq_exprs, {
        let uniqexpr = lfirst(current_cell!(l)) as *mut Expr;
        let mut tle: *mut TargetEntry;

        tle = tlist_member(uniqexpr, newtlist);
        if tle.is_null() {
            tle = makeTargetEntry(uniqexpr, nextresno as AttrNumber, core::ptr::null_mut(), false);
            newtlist = lappend(newtlist, tle as *mut c_void);
            nextresno += 1;
            newitems = true;
        }
    });

    /* Use change_plan_targetlist in case we need to insert a Result node */
    if newitems || (*best_path).umethod == UNIQUE_PATH_SORT {
        subplan = change_plan_targetlist(subplan, newtlist, (*best_path).path.parallel_safe);
    }

    newtlist = (*subplan).targetlist;
    numGroupCols = list_length(uniq_exprs);
    groupColIdx = palloc(numGroupCols as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    groupCollations = palloc(numGroupCols as usize * core::mem::size_of::<Oid>()) as *mut Oid;

    groupColPos = 0;
    foreach!(l, uniq_exprs, {
        let uniqexpr = lfirst(current_cell!(l)) as *mut Expr;
        let tle: *mut TargetEntry;

        tle = tlist_member(uniqexpr, newtlist);
        if tle.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "failed to find unique expression in subplan tlist");
        }
        *groupColIdx.add(groupColPos as usize) = (*tle).resno;
        *groupCollations.add(groupColPos as usize) = exprCollation((*tle).expr as *mut Node);
        groupColPos += 1;
    });

    if (*best_path).umethod == UNIQUE_PATH_HASH {
        let groupOperators: *mut Oid;

        groupOperators = palloc(numGroupCols as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        groupColPos = 0;
        foreach!(l, in_operators, {
            let in_oper = lfirst_oid(current_cell!(l));
            let mut eq_oper: Oid = 0;

            if !get_compatible_hash_operators(in_oper, core::ptr::null_mut(), &raw mut eq_oper) {
                elog!(ERROR, "could not find compatible hash operator for operator {}", in_oper);
            }
            *groupOperators.add(groupColPos as usize) = eq_oper;
            groupColPos += 1;
        });

        plan = make_agg(
            build_path_tlist(root, &raw mut (*best_path).path),
            NIL,
            AGG_HASHED,
            AGGSPLIT_SIMPLE,
            numGroupCols,
            groupColIdx,
            groupOperators,
            groupCollations,
            NIL,
            NIL,
            (*best_path).path.rows,
            0,
            subplan,
        ) as *mut Plan;
    } else {
        let mut sortList: *mut List = NIL;
        let sort: *mut Sort;

        /* Create an ORDER BY list to sort the input compatibly */
        groupColPos = 0;
        foreach!(l, in_operators, {
            let in_oper = lfirst_oid(current_cell!(l));
            let sortop: Oid;
            let eqop: Oid;
            let tle: *mut TargetEntry;
            let sortcl: *mut SortGroupClause;

            sortop = get_ordering_op_for_equality_op(in_oper, false);
            if !OidIsValid(sortop) {
                /* shouldn't happen */
                elog!(ERROR, "could not find ordering operator for equality operator {}", in_oper);
            }

            eqop = get_equality_op_for_ordering_op(sortop, core::ptr::null_mut());
            if !OidIsValid(eqop) {
                /* shouldn't happen */
                elog!(ERROR, "could not find equality operator for ordering operator {}", sortop);
            }

            tle = get_tle_by_resno((*subplan).targetlist, *groupColIdx.add(groupColPos as usize));
            Assert!(!tle.is_null());

            sortcl = makeNode!(SortGroupClause, T_SortGroupClause);
            (*sortcl).tleSortGroupRef = assignSortGroupRef(tle, (*subplan).targetlist);
            (*sortcl).eqop = eqop;
            (*sortcl).sortop = sortop;
            (*sortcl).reverse_sort = false;
            (*sortcl).nulls_first = false;
            (*sortcl).hashable = false; /* no need to make this accurate */
            sortList = lappend(sortList, sortcl as *mut c_void);
            groupColPos += 1;
        });
        sort = make_sort_from_sortclauses(sortList, subplan);
        label_sort_with_costsize(root, sort, -1.0);
        plan = make_unique_from_sortclauses(sort as *mut Plan, sortList) as *mut Plan;
    }

    /* Copy cost data from Path to Plan */
    copy_generic_path_info(plan, &raw mut (*best_path).path);

    plan
}

// create_gather_plan
unsafe fn create_gather_plan(root: *mut PlannerInfo, best_path: *mut GatherPath) -> *mut Gather {
    let gather_plan: *mut Gather;
    let subplan: *mut Plan;
    let tlist: *mut List;

    subplan = create_plan_recurse(root, (*best_path).subpath, CP_EXACT_TLIST);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    gather_plan = make_gather(
        tlist,
        NIL,
        (*best_path).num_workers,
        assign_special_exec_param(root),
        (*best_path).single_copy,
        subplan,
    );

    copy_generic_path_info(&raw mut (*gather_plan).plan, &raw mut (*best_path).path);

    /* use parallel mode for parallel plans. */
    (*(*root).glob).parallelModeNeeded = true;

    gather_plan
}

// create_gather_merge_plan
unsafe fn create_gather_merge_plan(root: *mut PlannerInfo, best_path: *mut GatherMergePath) -> *mut GatherMerge {
    let gm_plan: *mut GatherMerge;
    let mut subplan: *mut Plan;
    let pathkeys = (*best_path).path.pathkeys;
    let tlist = build_path_tlist(root, &raw mut (*best_path).path);

    /* As with Gather, project away columns in the workers. */
    subplan = create_plan_recurse(root, (*best_path).subpath, CP_EXACT_TLIST);

    /* Create a shell for a GatherMerge plan. */
    gm_plan = makeNode!(GatherMerge, T_GatherMerge);
    (*gm_plan).plan.targetlist = tlist;
    (*gm_plan).num_workers = (*best_path).num_workers;
    copy_generic_path_info(&raw mut (*gm_plan).plan, &raw mut (*best_path).path);

    /* Assign the rescan Param. */
    (*gm_plan).rescan_param = assign_special_exec_param(root);

    /* Gather Merge is pointless with no pathkeys; use Gather instead. */
    Assert!(pathkeys != NIL);

    /* Compute sort column info, and adjust subplan's tlist as needed */
    subplan = prepare_sort_from_pathkeys(
        subplan,
        pathkeys,
        (*(*(*best_path).subpath).parent).relids,
        (*gm_plan).sortColIdx,
        false,
        &raw mut (*gm_plan).numCols,
        &raw mut (*gm_plan).sortColIdx,
        &raw mut (*gm_plan).sortOperators,
        &raw mut (*gm_plan).collations,
        &raw mut (*gm_plan).nullsFirst,
    );

    Assert!(pathkeys_contained_in(pathkeys, (*(*best_path).subpath).pathkeys));

    /* Now insert the subplan under GatherMerge. */
    (*gm_plan).plan.lefttree = subplan;

    /* use parallel mode for parallel plans. */
    (*(*root).glob).parallelModeNeeded = true;

    gm_plan
}

// create_projection_plan
unsafe fn create_projection_plan(root: *mut PlannerInfo, best_path: *mut ProjectionPath, flags: c_int) -> *mut Plan {
    let plan: *mut Plan;
    let subplan: *mut Plan;
    let tlist: *mut List;
    let mut needs_result_node = false;

    if use_physical_tlist(root, &raw mut (*best_path).path, flags) {
        subplan = create_plan_recurse(root, (*best_path).subpath, 0);
        tlist = (*subplan).targetlist;
        if flags & CP_LABEL_TLIST != 0 {
            apply_pathtarget_labeling_to_tlist(tlist, (*best_path).path.pathtarget);
        }
    } else if is_projection_capable_path((*best_path).subpath) {
        subplan = create_plan_recurse(root, (*best_path).subpath, CP_IGNORE_TLIST);
        Assert!(is_projection_capable_plan(subplan));
        tlist = build_path_tlist(root, &raw mut (*best_path).path);
    } else {
        subplan = create_plan_recurse(root, (*best_path).subpath, 0);
        tlist = build_path_tlist(root, &raw mut (*best_path).path);
        needs_result_node = !tlist_same_exprs(tlist, (*subplan).targetlist);
    }

    if !needs_result_node {
        /* Don't need a separate Result, just assign tlist to subplan */
        plan = subplan;
        (*plan).targetlist = tlist;

        /* Label plan with the estimated costs we actually used */
        (*plan).startup_cost = (*best_path).path.startup_cost;
        (*plan).total_cost = (*best_path).path.total_cost;
        (*plan).plan_rows = (*best_path).path.rows;
        (*plan).plan_width = (*(*best_path).path.pathtarget).width;
        (*plan).parallel_safe = (*best_path).path.parallel_safe;
        /* ... but don't change subplan's parallel_aware flag */
    } else {
        /* We need a Result node */
        plan = make_result(tlist, core::ptr::null_mut(), subplan) as *mut Plan;

        copy_generic_path_info(plan, best_path as *mut Path);
    }

    plan
}

// inject_projection_plan
//	  Insert a Result node to do a projection step.
unsafe fn inject_projection_plan(subplan: *mut Plan, tlist: *mut List, parallel_safe: bool) -> *mut Plan {
    let plan: *mut Plan;

    plan = make_result(tlist, core::ptr::null_mut(), subplan) as *mut Plan;

    copy_plan_costsize(plan, subplan);
    (*plan).parallel_safe = parallel_safe;

    plan
}

// change_plan_targetlist
//	  Externally available wrapper for inject_projection_plan.
pub unsafe fn change_plan_targetlist(mut subplan: *mut Plan, tlist: *mut List, tlist_parallel_safe: bool) -> *mut Plan {
    if !is_projection_capable_plan(subplan) && !tlist_same_exprs(tlist, (*subplan).targetlist) {
        subplan = inject_projection_plan(subplan, tlist, (*subplan).parallel_safe && tlist_parallel_safe);
    } else {
        /* Else we can just replace the plan node's tlist */
        (*subplan).targetlist = tlist;
        (*subplan).parallel_safe &= tlist_parallel_safe;
    }
    subplan
}

// create_sort_plan
unsafe fn create_sort_plan(root: *mut PlannerInfo, best_path: *mut SortPath, flags: c_int) -> *mut Sort {
    let plan: *mut Sort;
    let subplan: *mut Plan;

    subplan = create_plan_recurse(root, (*best_path).subpath, flags | CP_SMALL_TLIST);

    plan = make_sort_from_pathkeys(
        subplan,
        (*best_path).path.pathkeys,
        if IS_OTHER_REL((*(*best_path).subpath).parent) {
            (*(*best_path).path.parent).relids
        } else {
            core::ptr::null_mut()
        },
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_incrementalsort_plan
unsafe fn create_incrementalsort_plan(root: *mut PlannerInfo, best_path: *mut IncrementalSortPath, flags: c_int) -> *mut IncrementalSort {
    let plan: *mut IncrementalSort;
    let subplan: *mut Plan;

    /* See comments in create_sort_plan() above */
    subplan = create_plan_recurse(root, (*best_path).spath.subpath, flags | CP_SMALL_TLIST);
    plan = make_incrementalsort_from_pathkeys(
        subplan,
        (*best_path).spath.path.pathkeys,
        if IS_OTHER_REL((*(*best_path).spath.subpath).parent) {
            (*(*best_path).spath.path.parent).relids
        } else {
            core::ptr::null_mut()
        },
        (*best_path).nPresortedCols,
    );

    copy_generic_path_info(&raw mut (*plan).sort.plan, best_path as *mut Path);

    plan
}

// create_group_plan
unsafe fn create_group_plan(root: *mut PlannerInfo, best_path: *mut GroupPath) -> *mut Group {
    let plan: *mut Group;
    let subplan: *mut Plan;
    let tlist: *mut List;
    let quals: *mut List;

    subplan = create_plan_recurse(root, (*best_path).subpath, CP_LABEL_TLIST);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    quals = order_qual_clauses(root, (*best_path).qual);

    plan = make_group(
        tlist,
        quals,
        list_length((*best_path).groupClause),
        extract_grouping_cols((*best_path).groupClause, (*subplan).targetlist),
        extract_grouping_ops((*best_path).groupClause),
        extract_grouping_collations((*best_path).groupClause, (*subplan).targetlist),
        subplan,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_upper_unique_plan
unsafe fn create_upper_unique_plan(root: *mut PlannerInfo, best_path: *mut UpperUniquePath, flags: c_int) -> *mut Unique {
    let plan: *mut Unique;
    let subplan: *mut Plan;

    subplan = create_plan_recurse(root, (*best_path).subpath, flags | CP_LABEL_TLIST);

    plan = make_unique_from_pathkeys(subplan, (*best_path).path.pathkeys, (*best_path).numkeys);

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_agg_plan
unsafe fn create_agg_plan(root: *mut PlannerInfo, best_path: *mut AggPath) -> *mut Agg {
    let plan: *mut Agg;
    let subplan: *mut Plan;
    let tlist: *mut List;
    let quals: *mut List;

    subplan = create_plan_recurse(root, (*best_path).subpath, CP_LABEL_TLIST);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    quals = order_qual_clauses(root, (*best_path).qual);

    plan = make_agg(
        tlist, quals,
        (*best_path).aggstrategy,
        (*best_path).aggsplit,
        list_length((*best_path).groupClause),
        extract_grouping_cols((*best_path).groupClause, (*subplan).targetlist),
        extract_grouping_ops((*best_path).groupClause),
        extract_grouping_collations((*best_path).groupClause, (*subplan).targetlist),
        NIL,
        NIL,
        (*best_path).numGroups,
        (*best_path).transitionSpace as usize,
        subplan,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// remap_groupColIdx
unsafe fn remap_groupColIdx(root: *mut PlannerInfo, groupClause: *mut List) -> *mut AttrNumber {
    let grouping_map = (*root).grouping_map;
    let new_grpColIdx: *mut AttrNumber;
    let mut lc: *mut ListCell;
    let mut i: c_int;

    Assert!(!grouping_map.is_null());

    new_grpColIdx = palloc0(core::mem::size_of::<AttrNumber>() * list_length(groupClause) as usize) as *mut AttrNumber;

    i = 0;
    foreach!(lc, groupClause, {
        let clause = lfirst(current_cell!(lc)) as *mut SortGroupClause;

        *new_grpColIdx.add(i as usize) = *grouping_map.add((*clause).tleSortGroupRef as usize);
        i += 1;
    });

    new_grpColIdx
}

// create_groupingsets_plan
unsafe fn create_groupingsets_plan(root: *mut PlannerInfo, best_path: *mut GroupingSetsPath) -> *mut Plan {
    let plan: *mut Agg;
    let subplan: *mut Plan;
    let rollups = (*best_path).rollups;
    let grouping_map: *mut AttrNumber;
    let mut maxref: c_int;
    let mut chain: *mut List;
    let mut lc: *mut ListCell;

    /* Shouldn't get here without grouping sets */
    Assert!(!(*(*root).parse).groupingSets.is_null());
    Assert!(rollups != NIL);

    subplan = create_plan_recurse(root, (*best_path).subpath, CP_LABEL_TLIST);

    maxref = 0;
    foreach!(lc, (*root).processed_groupClause, {
        let gc = lfirst(current_cell!(lc)) as *mut SortGroupClause;

        if (*gc).tleSortGroupRef as c_int > maxref {
            maxref = (*gc).tleSortGroupRef as c_int;
        }
    });

    grouping_map = palloc0((maxref + 1) as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;

    /* Now look up the column numbers in the child's tlist */
    foreach!(lc, (*root).processed_groupClause, {
        let gc = lfirst(current_cell!(lc)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(gc, (*subplan).targetlist);

        *grouping_map.add((*gc).tleSortGroupRef as usize) = (*tle).resno;
    });

    Assert!((*root).grouping_map.is_null());
    (*root).grouping_map = grouping_map;

    chain = NIL;
    if list_length(rollups) > 1 {
        let mut is_first_sort = (*(linitial(rollups) as *mut RollupData)).is_hashed;

        // for_each_from(lc, rollups, 1)
        lc = lnext(rollups, list_head(rollups));
        while !lc.is_null() {
            let rollup = lfirst(lc) as *mut RollupData;
            let new_grpColIdx: *mut AttrNumber;
            let mut sort_plan: *mut Plan = core::ptr::null_mut();
            let agg_plan: *mut Plan;
            let strat: AggStrategy;

            new_grpColIdx = remap_groupColIdx(root, (*rollup).groupClause);

            if !(*rollup).is_hashed && !is_first_sort {
                sort_plan = make_sort_from_groupcols((*rollup).groupClause, new_grpColIdx, subplan) as *mut Plan;
            }

            if !(*rollup).is_hashed {
                is_first_sort = false;
            }

            if (*rollup).is_hashed {
                strat = AGG_HASHED;
            } else if linitial((*rollup).gsets) == (NIL as *mut c_void) {
                strat = AGG_PLAIN;
            } else {
                strat = AGG_SORTED;
            }

            agg_plan = make_agg(
                NIL,
                NIL,
                strat,
                AGGSPLIT_SIMPLE,
                list_length(linitial((*rollup).gsets) as *mut List),
                new_grpColIdx,
                extract_grouping_ops((*rollup).groupClause),
                extract_grouping_collations((*rollup).groupClause, (*subplan).targetlist),
                (*rollup).gsets,
                NIL,
                (*rollup).numGroups,
                (*best_path).transitionSpace as usize,
                sort_plan,
            ) as *mut Plan;

            if !sort_plan.is_null() {
                (*sort_plan).targetlist = NIL;
                (*sort_plan).lefttree = core::ptr::null_mut();
            }

            chain = lappend(chain, agg_plan as *mut c_void);

            lc = lnext(rollups, lc);
        }
    }

    /* Now make the real Agg node */
    {
        let rollup = linitial(rollups) as *mut RollupData;
        let top_grpColIdx: *mut AttrNumber;
        let numGroupCols: c_int;

        top_grpColIdx = remap_groupColIdx(root, (*rollup).groupClause);

        numGroupCols = list_length(linitial((*rollup).gsets) as *mut List);

        plan = make_agg(
            build_path_tlist(root, &raw mut (*best_path).path),
            (*best_path).qual,
            (*best_path).aggstrategy,
            AGGSPLIT_SIMPLE,
            numGroupCols,
            top_grpColIdx,
            extract_grouping_ops((*rollup).groupClause),
            extract_grouping_collations((*rollup).groupClause, (*subplan).targetlist),
            (*rollup).gsets,
            chain,
            (*rollup).numGroups,
            (*best_path).transitionSpace as usize,
            subplan,
        );

        /* Copy cost data from Path to Plan */
        copy_generic_path_info(&raw mut (*plan).plan, &raw mut (*best_path).path);
    }

    plan as *mut Plan
}

// create_minmaxagg_plan
unsafe fn create_minmaxagg_plan(root: *mut PlannerInfo, best_path: *mut MinMaxAggPath) -> *mut Result {
    let plan: *mut Result;
    let tlist: *mut List;
    let mut lc: *mut ListCell;

    /* Prepare an InitPlan for each aggregate's subquery. */
    foreach!(lc, (*best_path).mmaggregates, {
        let mminfo = lfirst(current_cell!(lc)) as *mut MinMaxAggInfo;
        let subroot = (*mminfo).subroot;
        let subparse = (*subroot).parse;
        let mut subplan: *mut Plan;

        subplan = create_plan(subroot, (*mminfo).path);

        subplan = make_limit(
            subplan,
            (*subparse).limitOffset,
            (*subparse).limitCount,
            (*subparse).limitOption,
            0, core::ptr::null_mut(), core::ptr::null_mut(), core::ptr::null_mut(),
        ) as *mut Plan;

        /* Must apply correct cost/width data to Limit node */
        (*subplan).disabled_nodes = (*(*mminfo).path).disabled_nodes;
        (*subplan).startup_cost = (*(*mminfo).path).startup_cost;
        (*subplan).total_cost = (*mminfo).pathcost;
        (*subplan).plan_rows = 1.0;
        (*subplan).plan_width = (*(*(*mminfo).path).pathtarget).width;
        (*subplan).parallel_aware = false;
        (*subplan).parallel_safe = (*(*mminfo).path).parallel_safe;

        /* Convert the plan into an InitPlan in the outer query. */
        SS_make_initplan_from_plan(root, subroot, subplan, (*mminfo).param);
    });

    /* Generate the output plan --- basically just a Result */
    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    plan = make_result(tlist, (*best_path).quals as *mut Node, core::ptr::null_mut());

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    Assert!((*root).minmax_aggs == NIL);
    (*root).minmax_aggs = (*best_path).mmaggregates;

    plan
}

// create_windowagg_plan
unsafe fn create_windowagg_plan(root: *mut PlannerInfo, best_path: *mut WindowAggPath) -> *mut WindowAgg {
    let plan: *mut WindowAgg;
    let wc = (*best_path).winclause;
    let numPart = list_length((*wc).partitionClause);
    let numOrder = list_length((*wc).orderClause);
    let subplan: *mut Plan;
    let tlist: *mut List;
    let mut partNumCols: c_int;
    let partColIdx: *mut AttrNumber;
    let partOperators: *mut Oid;
    let partCollations: *mut Oid;
    let mut ordNumCols: c_int;
    let ordColIdx: *mut AttrNumber;
    let ordOperators: *mut Oid;
    let ordCollations: *mut Oid;
    let mut lc: *mut ListCell;

    subplan = create_plan_recurse(root, (*best_path).subpath, CP_LABEL_TLIST | CP_SMALL_TLIST);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    partColIdx = palloc(core::mem::size_of::<AttrNumber>() * numPart as usize) as *mut AttrNumber;
    partOperators = palloc(core::mem::size_of::<Oid>() * numPart as usize) as *mut Oid;
    partCollations = palloc(core::mem::size_of::<Oid>() * numPart as usize) as *mut Oid;

    partNumCols = 0;
    foreach!(lc, (*wc).partitionClause, {
        let sgc = lfirst(current_cell!(lc)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sgc, (*subplan).targetlist);

        Assert!(OidIsValid((*sgc).eqop));
        *partColIdx.add(partNumCols as usize) = (*tle).resno;
        *partOperators.add(partNumCols as usize) = (*sgc).eqop;
        *partCollations.add(partNumCols as usize) = exprCollation((*tle).expr as *mut Node);
        partNumCols += 1;
    });

    ordColIdx = palloc(core::mem::size_of::<AttrNumber>() * numOrder as usize) as *mut AttrNumber;
    ordOperators = palloc(core::mem::size_of::<Oid>() * numOrder as usize) as *mut Oid;
    ordCollations = palloc(core::mem::size_of::<Oid>() * numOrder as usize) as *mut Oid;

    ordNumCols = 0;
    foreach!(lc, (*wc).orderClause, {
        let sgc = lfirst(current_cell!(lc)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sgc, (*subplan).targetlist);

        Assert!(OidIsValid((*sgc).eqop));
        *ordColIdx.add(ordNumCols as usize) = (*tle).resno;
        *ordOperators.add(ordNumCols as usize) = (*sgc).eqop;
        *ordCollations.add(ordNumCols as usize) = exprCollation((*tle).expr as *mut Node);
        ordNumCols += 1;
    });

    /* And finally we can make the WindowAgg node */
    plan = make_windowagg(
        tlist,
        wc,
        partNumCols, partColIdx, partOperators, partCollations,
        ordNumCols, ordColIdx, ordOperators, ordCollations,
        (*best_path).runCondition,
        (*best_path).qual,
        (*best_path).topwindow,
        subplan,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_setop_plan
unsafe fn create_setop_plan(root: *mut PlannerInfo, best_path: *mut SetOpPath, flags: c_int) -> *mut SetOp {
    let plan: *mut SetOp;
    let tlist = build_path_tlist(root, &raw mut (*best_path).path);
    let leftplan: *mut Plan;
    let rightplan: *mut Plan;
    let numGroups: c_long;

    leftplan = create_plan_recurse(root, (*best_path).leftpath, flags | CP_LABEL_TLIST);
    rightplan = create_plan_recurse(root, (*best_path).rightpath, flags | CP_LABEL_TLIST);

    /* Convert numGroups to long int --- but 'ware overflow! */
    numGroups = clamp_cardinality_to_long((*best_path).numGroups);

    plan = make_setop(
        (*best_path).cmd,
        (*best_path).strategy,
        tlist,
        leftplan,
        rightplan,
        (*best_path).groupList,
        numGroups,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_recursiveunion_plan
unsafe fn create_recursiveunion_plan(root: *mut PlannerInfo, best_path: *mut RecursiveUnionPath) -> *mut RecursiveUnion {
    let plan: *mut RecursiveUnion;
    let leftplan: *mut Plan;
    let rightplan: *mut Plan;
    let tlist: *mut List;
    let numGroups: c_long;

    /* Need both children to produce same tlist, so force it */
    leftplan = create_plan_recurse(root, (*best_path).leftpath, CP_EXACT_TLIST);
    rightplan = create_plan_recurse(root, (*best_path).rightpath, CP_EXACT_TLIST);

    tlist = build_path_tlist(root, &raw mut (*best_path).path);

    /* Convert numGroups to long int --- but 'ware overflow! */
    numGroups = clamp_cardinality_to_long((*best_path).numGroups);

    plan = make_recursive_union(
        tlist,
        leftplan,
        rightplan,
        (*best_path).wtParam,
        (*best_path).distinctList,
        numGroups,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_lockrows_plan
unsafe fn create_lockrows_plan(root: *mut PlannerInfo, best_path: *mut LockRowsPath, flags: c_int) -> *mut LockRows {
    let plan: *mut LockRows;
    let subplan: *mut Plan;

    /* LockRows doesn't project, so tlist requirements pass through */
    subplan = create_plan_recurse(root, (*best_path).subpath, flags);

    plan = make_lockrows(subplan, (*best_path).rowMarks, (*best_path).epqParam);

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// create_modifytable_plan
unsafe fn create_modifytable_plan(root: *mut PlannerInfo, best_path: *mut ModifyTablePath) -> *mut ModifyTable {
    let plan: *mut ModifyTable;
    let subpath = (*best_path).subpath;
    let subplan: *mut Plan;

    /* Subplan must produce exactly the specified tlist */
    subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

    /* Transfer resname/resjunk labeling, too, to keep executor happy */
    apply_tlist_labeling((*subplan).targetlist, (*root).processed_tlist);

    plan = make_modifytable(
        root,
        subplan,
        (*best_path).operation,
        (*best_path).canSetTag,
        (*best_path).nominalRelation,
        (*best_path).rootRelation,
        (*best_path).partColsUpdated,
        (*best_path).resultRelations,
        (*best_path).updateColnosLists,
        (*best_path).withCheckOptionLists,
        (*best_path).returningLists,
        (*best_path).rowMarks,
        (*best_path).onconflict,
        (*best_path).mergeActionLists,
        (*best_path).mergeJoinConditions,
        (*best_path).epqParam,
    );

    copy_generic_path_info(&raw mut (*plan).plan, &raw mut (*best_path).path);

    plan
}

// create_limit_plan
unsafe fn create_limit_plan(root: *mut PlannerInfo, best_path: *mut LimitPath, flags: c_int) -> *mut Limit {
    let plan: *mut Limit;
    let subplan: *mut Plan;
    let mut numUniqkeys: c_int = 0;
    let mut uniqColIdx: *mut AttrNumber = core::ptr::null_mut();
    let mut uniqOperators: *mut Oid = core::ptr::null_mut();
    let mut uniqCollations: *mut Oid = core::ptr::null_mut();

    /* Limit doesn't project, so tlist requirements pass through */
    subplan = create_plan_recurse(root, (*best_path).subpath, flags);

    /* Extract information necessary for comparing rows for WITH TIES. */
    if (*best_path).limitOption == LIMIT_OPTION_WITH_TIES {
        let parse = (*root).parse;
        let mut l: *mut ListCell;

        numUniqkeys = list_length((*parse).sortClause);
        uniqColIdx = palloc(numUniqkeys as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
        uniqOperators = palloc(numUniqkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        uniqCollations = palloc(numUniqkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;

        numUniqkeys = 0;
        foreach!(l, (*parse).sortClause, {
            let sortcl = lfirst(current_cell!(l)) as *mut SortGroupClause;
            let tle = get_sortgroupclause_tle(sortcl, (*parse).targetList);

            *uniqColIdx.add(numUniqkeys as usize) = (*tle).resno;
            *uniqOperators.add(numUniqkeys as usize) = (*sortcl).eqop;
            *uniqCollations.add(numUniqkeys as usize) = exprCollation((*tle).expr as *mut Node);
            numUniqkeys += 1;
        });
    }

    plan = make_limit(
        subplan,
        (*best_path).limitOffset,
        (*best_path).limitCount,
        (*best_path).limitOption,
        numUniqkeys, uniqColIdx, uniqOperators, uniqCollations,
    );

    copy_generic_path_info(&raw mut (*plan).plan, best_path as *mut Path);

    plan
}

// *****************************************************************************
//	BASE-RELATION SCAN METHODS
// *****************************************************************************

// create_seqscan_plan
unsafe fn create_seqscan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut SeqScan {
    let scan_plan: *mut SeqScan;
    let scan_relid = (*(*best_path).parent).relid;

    /* it should be a base rel... */
    Assert!(scan_relid > 0);
    Assert!((*(*best_path).parent).rtekind == RTE_RELATION);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_seqscan(tlist, scan_clauses, scan_relid);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_samplescan_plan
unsafe fn create_samplescan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut SampleScan {
    let scan_plan: *mut SampleScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut tsc: *mut TableSampleClause;

    /* it should be a base rel with a tablesample clause... */
    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_RELATION);
    tsc = (*rte).tablesample;
    Assert!(!tsc.is_null());

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
        tsc = replace_nestloop_params(root, tsc as *mut Node) as *mut TableSampleClause;
    }

    scan_plan = make_samplescan(tlist, scan_clauses, scan_relid, tsc);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_indexscan_plan
unsafe fn create_indexscan_plan(root: *mut PlannerInfo, best_path: *mut IndexPath, tlist: *mut List, scan_clauses: *mut List, indexonly: bool) -> *mut Scan {
    let scan_plan: *mut Scan;
    let indexclauses = (*best_path).indexclauses;
    let mut indexorderbys = (*best_path).indexorderbys;
    let baserelid = (*(*best_path).path.parent).relid;
    let indexinfo = (*best_path).indexinfo;
    let indexoid = (*indexinfo).indexoid;
    let mut qpqual: *mut List;
    let mut stripped_indexquals: *mut List = core::ptr::null_mut();
    let mut fixed_indexquals: *mut List = core::ptr::null_mut();
    let fixed_indexorderbys: *mut List;
    let mut indexorderbyops: *mut List = NIL;
    let mut l: *mut ListCell;

    /* it should be a base rel... */
    Assert!(baserelid > 0);
    Assert!((*(*best_path).path.parent).rtekind == RTE_RELATION);
    /* check the scan direction is valid */
    Assert!((*best_path).indexscandir == ForwardScanDirection || (*best_path).indexscandir == BackwardScanDirection);

    fix_indexqual_references(root, best_path, &raw mut stripped_indexquals, &raw mut fixed_indexquals);

    /* Likewise fix up index attr references in the ORDER BY expressions. */
    fixed_indexorderbys = fix_indexorderby_references(root, best_path);

    qpqual = NIL;
    foreach!(l, scan_clauses, {
        let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

        if (*rinfo).pseudoconstant {
            continue; /* we may drop pseudoconstants here */
        }
        if is_redundant_with_indexclauses(rinfo, indexclauses) {
            continue; /* dup or derived from same EquivalenceClass */
        }
        if !contain_mutable_functions((*rinfo).clause as *mut Node)
            && predicate_implied_by(list_make1!((*rinfo).clause as *mut c_void), stripped_indexquals, false)
        {
            continue; /* provably implied by indexquals */
        }
        qpqual = lappend(qpqual, rinfo as *mut c_void);
    });

    /* Sort clauses into best execution order */
    qpqual = order_qual_clauses(root, qpqual);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    qpqual = extract_actual_clauses(qpqual, false);

    if !(*best_path).path.param_info.is_null() {
        stripped_indexquals = replace_nestloop_params(root, stripped_indexquals as *mut Node) as *mut List;
        qpqual = replace_nestloop_params(root, qpqual as *mut Node) as *mut List;
        indexorderbys = replace_nestloop_params(root, indexorderbys as *mut Node) as *mut List;
    }

    if !indexorderbys.is_null() {
        let mut pathkeyCell: *mut ListCell;
        let mut exprCell: *mut ListCell;

        Assert!(list_length((*best_path).path.pathkeys) == list_length(indexorderbys));
        forboth!(pathkeyCell, (*best_path).path.pathkeys, exprCell, indexorderbys, {
            let pathkey = lfirst(pathkeyCell) as *mut PathKey;
            let expr = lfirst(exprCell) as *mut Node;
            let exprtype = exprType(expr);
            let sortop: Oid;

            /* Get sort operator from opfamily */
            sortop = get_opfamily_member_for_cmptype((*pathkey).pk_opfamily, exprtype, exprtype, (*pathkey).pk_cmptype);
            if !OidIsValid(sortop) {
                elog!(ERROR, "missing operator {}({},{}) in opfamily {}", (*pathkey).pk_cmptype, exprtype, exprtype, (*pathkey).pk_opfamily);
            }
            indexorderbyops = lappend_oid(indexorderbyops, sortop);
        });
    }

    if indexonly {
        let mut i: c_int = 0;

        foreach!(l, (*indexinfo).indextlist, {
            let indextle = lfirst(current_cell!(l)) as *mut TargetEntry;

            (*indextle).resjunk = !*(*indexinfo).canreturn.add(i as usize);
            i += 1;
        });
    }

    /* Finally ready to build the plan node */
    if indexonly {
        scan_plan = make_indexonlyscan(
            tlist, qpqual, baserelid, indexoid,
            fixed_indexquals, stripped_indexquals,
            fixed_indexorderbys, (*indexinfo).indextlist,
            (*best_path).indexscandir,
        ) as *mut Scan;
    } else {
        scan_plan = make_indexscan(
            tlist, qpqual, baserelid, indexoid,
            fixed_indexquals, stripped_indexquals,
            fixed_indexorderbys, indexorderbys, indexorderbyops,
            (*best_path).indexscandir,
        ) as *mut Scan;
    }

    copy_generic_path_info(&raw mut (*scan_plan).plan, &raw mut (*best_path).path);

    scan_plan
}

// create_bitmap_scan_plan
unsafe fn create_bitmap_scan_plan(root: *mut PlannerInfo, best_path: *mut BitmapHeapPath, tlist: *mut List, scan_clauses: *mut List) -> *mut BitmapHeapScan {
    let baserelid = (*(*best_path).path.parent).relid;
    let bitmapqualplan: *mut Plan;
    let mut bitmapqualorig: *mut List = core::ptr::null_mut();
    let mut indexquals: *mut List = core::ptr::null_mut();
    let mut indexECs: *mut List = core::ptr::null_mut();
    let mut qpqual: *mut List;
    let mut l: *mut ListCell;
    let scan_plan: *mut BitmapHeapScan;

    /* it should be a base rel... */
    Assert!(baserelid > 0);
    Assert!((*(*best_path).path.parent).rtekind == RTE_RELATION);

    /* Process the bitmapqual tree into a Plan tree and qual lists */
    bitmapqualplan = create_bitmap_subplan(root, (*best_path).bitmapqual, &raw mut bitmapqualorig, &raw mut indexquals, &raw mut indexECs);

    if (*best_path).path.parallel_aware {
        bitmap_subplan_mark_shared(bitmapqualplan);
    }

    qpqual = NIL;
    foreach!(l, scan_clauses, {
        let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));
        let clause = (*rinfo).clause as *mut Node;

        if (*rinfo).pseudoconstant {
            continue; /* we may drop pseudoconstants here */
        }
        if list_member(indexquals, clause as *mut c_void) {
            continue; /* simple duplicate */
        }
        if !(*rinfo).parent_ec.is_null() && list_member_ptr(indexECs, (*rinfo).parent_ec as *mut c_void) {
            continue; /* derived from same EquivalenceClass */
        }
        if !contain_mutable_functions(clause) && predicate_implied_by(list_make1!(clause as *mut c_void), indexquals, false) {
            continue; /* provably implied by indexquals */
        }
        qpqual = lappend(qpqual, rinfo as *mut c_void);
    });

    /* Sort clauses into best execution order */
    qpqual = order_qual_clauses(root, qpqual);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    qpqual = extract_actual_clauses(qpqual, false);

    bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

    if !(*best_path).path.param_info.is_null() {
        qpqual = replace_nestloop_params(root, qpqual as *mut Node) as *mut List;
        bitmapqualorig = replace_nestloop_params(root, bitmapqualorig as *mut Node) as *mut List;
    }

    /* Finally ready to build the plan node */
    scan_plan = make_bitmap_heapscan(tlist, qpqual, bitmapqualplan, bitmapqualorig, baserelid);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, &raw mut (*best_path).path);

    scan_plan
}

// create_bitmap_subplan
unsafe fn create_bitmap_subplan(
    root: *mut PlannerInfo,
    bitmapqual: *mut Path,
    qual: *mut *mut List,
    indexqual: *mut *mut List,
    indexECs: *mut *mut List,
) -> *mut Plan {
    let plan: *mut Plan;

    if IsA!(bitmapqual, T_BitmapAndPath) {
        let apath = bitmapqual as *mut BitmapAndPath;
        let mut subplans: *mut List = NIL;
        let mut subquals: *mut List = NIL;
        let mut subindexquals: *mut List = NIL;
        let mut subindexECs: *mut List = NIL;
        let mut l: *mut ListCell;

        foreach!(l, (*apath).bitmapquals, {
            let subplan: *mut Plan;
            let mut subqual: *mut List = core::ptr::null_mut();
            let mut subindexqual: *mut List = core::ptr::null_mut();
            let mut subindexEC: *mut List = core::ptr::null_mut();

            subplan = create_bitmap_subplan(root, lfirst(current_cell!(l)) as *mut Path, &raw mut subqual, &raw mut subindexqual, &raw mut subindexEC);
            subplans = lappend(subplans, subplan as *mut c_void);
            subquals = list_concat_unique(subquals, subqual);
            subindexquals = list_concat_unique(subindexquals, subindexqual);
            /* Duplicates in indexECs aren't worth getting rid of */
            subindexECs = list_concat(subindexECs, subindexEC);
        });
        plan = make_bitmap_and(subplans) as *mut Plan;
        (*plan).startup_cost = (*apath).path.startup_cost;
        (*plan).total_cost = (*apath).path.total_cost;
        (*plan).plan_rows = clamp_row_est((*apath).bitmapselectivity * (*(*apath).path.parent).tuples);
        (*plan).plan_width = 0; /* meaningless */
        (*plan).parallel_aware = false;
        (*plan).parallel_safe = (*apath).path.parallel_safe;
        *qual = subquals;
        *indexqual = subindexquals;
        *indexECs = subindexECs;
    } else if IsA!(bitmapqual, T_BitmapOrPath) {
        let opath = bitmapqual as *mut BitmapOrPath;
        let mut subplans: *mut List = NIL;
        let mut subquals: *mut List = NIL;
        let mut subindexquals: *mut List = NIL;
        let mut const_true_subqual = false;
        let mut const_true_subindexqual = false;
        let mut l: *mut ListCell;

        foreach!(l, (*opath).bitmapquals, {
            let subplan: *mut Plan;
            let mut subqual: *mut List = core::ptr::null_mut();
            let mut subindexqual: *mut List = core::ptr::null_mut();
            let mut subindexEC: *mut List = core::ptr::null_mut();

            subplan = create_bitmap_subplan(root, lfirst(current_cell!(l)) as *mut Path, &raw mut subqual, &raw mut subindexqual, &raw mut subindexEC);
            subplans = lappend(subplans, subplan as *mut c_void);
            if subqual == NIL {
                const_true_subqual = true;
            } else if !const_true_subqual {
                subquals = lappend(subquals, make_ands_explicit(subqual) as *mut c_void);
            }
            if subindexqual == NIL {
                const_true_subindexqual = true;
            } else if !const_true_subindexqual {
                subindexquals = lappend(subindexquals, make_ands_explicit(subindexqual) as *mut c_void);
            }
        });

        if list_length(subplans) == 1 {
            plan = linitial(subplans) as *mut Plan;
        } else {
            plan = make_bitmap_or(subplans) as *mut Plan;
            (*plan).startup_cost = (*opath).path.startup_cost;
            (*plan).total_cost = (*opath).path.total_cost;
            (*plan).plan_rows = clamp_row_est((*opath).bitmapselectivity * (*(*opath).path.parent).tuples);
            (*plan).plan_width = 0; /* meaningless */
            (*plan).parallel_aware = false;
            (*plan).parallel_safe = (*opath).path.parallel_safe;
        }

        if const_true_subqual {
            *qual = NIL;
        } else if list_length(subquals) <= 1 {
            *qual = subquals;
        } else {
            *qual = list_make1!(make_orclause(subquals) as *mut c_void);
        }
        if const_true_subindexqual {
            *indexqual = NIL;
        } else if list_length(subindexquals) <= 1 {
            *indexqual = subindexquals;
        } else {
            *indexqual = list_make1!(make_orclause(subindexquals) as *mut c_void);
        }
        *indexECs = NIL;
    } else if IsA!(bitmapqual, T_IndexPath) {
        let ipath = bitmapqual as *mut IndexPath;
        let iscan: *mut IndexScan;
        let mut subquals: *mut List;
        let mut subindexquals: *mut List;
        let mut subindexECs: *mut List;
        let mut l: *mut ListCell;

        /* Use the regular indexscan plan build machinery... */
        iscan = castNode!(IndexScan, T_IndexScan, create_indexscan_plan(root, ipath, NIL, NIL, false));
        /* then convert to a bitmap indexscan */
        plan = make_bitmap_indexscan((*iscan).scan.scanrelid, (*iscan).indexid, (*iscan).indexqual, (*iscan).indexqualorig) as *mut Plan;
        /* and set its cost/width fields appropriately */
        (*plan).startup_cost = 0.0;
        (*plan).total_cost = (*ipath).indextotalcost;
        (*plan).plan_rows = clamp_row_est((*ipath).indexselectivity * (*(*ipath).path.parent).tuples);
        (*plan).plan_width = 0; /* meaningless */
        (*plan).parallel_aware = false;
        (*plan).parallel_safe = (*ipath).path.parallel_safe;
        /* Extract original index clauses, actual index quals, relevant ECs */
        subquals = NIL;
        subindexquals = NIL;
        subindexECs = NIL;
        foreach!(l, (*ipath).indexclauses, {
            let iclause = lfirst(current_cell!(l)) as *mut IndexClause;
            let rinfo = (*iclause).rinfo;

            Assert!(!(*rinfo).pseudoconstant);
            subquals = lappend(subquals, (*rinfo).clause as *mut c_void);
            subindexquals = list_concat(subindexquals, get_actual_clauses((*iclause).indexquals));
            if !(*rinfo).parent_ec.is_null() {
                subindexECs = lappend(subindexECs, (*rinfo).parent_ec as *mut c_void);
            }
        });
        /* We can add any index predicate conditions, too */
        foreach!(l, (*(*ipath).indexinfo).indpred, {
            let pred = lfirst(current_cell!(l)) as *mut Expr;

            if !predicate_implied_by(list_make1!(pred as *mut c_void), subquals, false) {
                subquals = lappend(subquals, pred as *mut c_void);
                subindexquals = lappend(subindexquals, pred as *mut c_void);
            }
        });
        *qual = subquals;
        *indexqual = subindexquals;
        *indexECs = subindexECs;
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(bitmapqual as *mut Node) as c_int);
        unreachable!();
    }

    plan
}

// create_tidscan_plan
unsafe fn create_tidscan_plan(root: *mut PlannerInfo, best_path: *mut TidPath, tlist: *mut List, mut scan_clauses: *mut List) -> *mut TidScan {
    let scan_plan: *mut TidScan;
    let scan_relid = (*(*best_path).path.parent).relid;
    let mut tidquals = (*best_path).tidquals;

    /* it should be a base rel... */
    Assert!(scan_relid > 0);
    Assert!((*(*best_path).path.parent).rtekind == RTE_RELATION);

    if list_length(tidquals) == 1 {
        let mut qpqual: *mut List = NIL;
        let mut l: *mut ListCell;

        foreach!(l, scan_clauses, {
            let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

            if (*rinfo).pseudoconstant {
                continue; /* we may drop pseudoconstants here */
            }
            if list_member_ptr(tidquals, rinfo as *mut c_void) {
                continue; /* simple duplicate */
            }
            if is_redundant_derived_clause(rinfo, tidquals) {
                continue; /* derived from same EquivalenceClass */
            }
            qpqual = lappend(qpqual, rinfo as *mut c_void);
        });
        scan_clauses = qpqual;
    }

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo lists to bare expressions; ignore pseudoconstants */
    tidquals = extract_actual_clauses(tidquals, false);
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    if list_length(tidquals) > 1 {
        scan_clauses = list_difference(scan_clauses, list_make1!(make_orclause(tidquals) as *mut c_void));
    }

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).path.param_info.is_null() {
        tidquals = replace_nestloop_params(root, tidquals as *mut Node) as *mut List;
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_tidscan(tlist, scan_clauses, scan_relid, tidquals);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, &raw mut (*best_path).path);

    scan_plan
}

// create_tidrangescan_plan
unsafe fn create_tidrangescan_plan(root: *mut PlannerInfo, best_path: *mut TidRangePath, tlist: *mut List, mut scan_clauses: *mut List) -> *mut TidRangeScan {
    let scan_plan: *mut TidRangeScan;
    let scan_relid = (*(*best_path).path.parent).relid;
    let mut tidrangequals = (*best_path).tidrangequals;

    /* it should be a base rel... */
    Assert!(scan_relid > 0);
    Assert!((*(*best_path).path.parent).rtekind == RTE_RELATION);

    {
        let mut qpqual: *mut List = NIL;
        let mut l: *mut ListCell;

        foreach!(l, scan_clauses, {
            let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(l));

            if (*rinfo).pseudoconstant {
                continue; /* we may drop pseudoconstants here */
            }
            if list_member_ptr(tidrangequals, rinfo as *mut c_void) {
                continue; /* simple duplicate */
            }
            qpqual = lappend(qpqual, rinfo as *mut c_void);
        });
        scan_clauses = qpqual;
    }

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo lists to bare expressions; ignore pseudoconstants */
    tidrangequals = extract_actual_clauses(tidrangequals, false);
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).path.param_info.is_null() {
        tidrangequals = replace_nestloop_params(root, tidrangequals as *mut Node) as *mut List;
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_tidrangescan(tlist, scan_clauses, scan_relid, tidrangequals);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, &raw mut (*best_path).path);

    scan_plan
}

// create_subqueryscan_plan
unsafe fn create_subqueryscan_plan(root: *mut PlannerInfo, best_path: *mut SubqueryScanPath, tlist: *mut List, mut scan_clauses: *mut List) -> *mut SubqueryScan {
    let scan_plan: *mut SubqueryScan;
    let rel = (*best_path).path.parent;
    let scan_relid = (*rel).relid;
    let subplan: *mut Plan;

    /* it should be a subquery base rel... */
    Assert!(scan_relid > 0);
    Assert!((*rel).rtekind == RTE_SUBQUERY);

    subplan = create_plan((*rel).subroot, (*best_path).subpath);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    if !(*best_path).path.param_info.is_null() {
        process_subquery_nestloop_params(root, (*rel).subplan_params);
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_subqueryscan(tlist, scan_clauses, scan_relid, subplan);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, &raw mut (*best_path).path);

    scan_plan
}

// create_functionscan_plan
unsafe fn create_functionscan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut FunctionScan {
    let scan_plan: *mut FunctionScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut functions: *mut List;

    /* it should be a function base rel... */
    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_FUNCTION);
    functions = (*rte).functions;

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
        /* The function expressions could contain nestloop params, too */
        functions = replace_nestloop_params(root, functions as *mut Node) as *mut List;
    }

    scan_plan = make_functionscan(tlist, scan_clauses, scan_relid, functions, (*rte).funcordinality);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_tablefuncscan_plan
unsafe fn create_tablefuncscan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut TableFuncScan {
    let scan_plan: *mut TableFuncScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut tablefunc: *mut TableFunc;

    /* it should be a function base rel... */
    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_TABLEFUNC);
    tablefunc = (*rte).tablefunc;

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
        /* The function expressions could contain nestloop params, too */
        tablefunc = replace_nestloop_params(root, tablefunc as *mut Node) as *mut TableFunc;
    }

    scan_plan = make_tablefuncscan(tlist, scan_clauses, scan_relid, tablefunc);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_valuesscan_plan
unsafe fn create_valuesscan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut ValuesScan {
    let scan_plan: *mut ValuesScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut values_lists: *mut List;

    /* it should be a values base rel... */
    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_VALUES);
    values_lists = (*rte).values_lists;

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
        /* The values lists could contain nestloop params, too */
        values_lists = replace_nestloop_params(root, values_lists as *mut Node) as *mut List;
    }

    scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid, values_lists);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_ctescan_plan
unsafe fn create_ctescan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut CteScan {
    let scan_plan: *mut CteScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut ctesplan: *mut crate::nodes::primnodes::SubPlan = core::ptr::null_mut();
    let plan_id: c_int;
    let cte_param_id: c_int;
    let mut cteroot: *mut PlannerInfo;
    let mut levelsup: Index;
    let mut ndx: c_int;
    let mut lc: *mut ListCell;

    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_CTE);
    Assert!(!(*rte).self_reference);

    /* Find the referenced CTE, and locate the SubPlan previously made for it. */
    levelsup = (*rte).ctelevelsup;
    cteroot = root;
    while levelsup > 0 {
        levelsup -= 1;
        cteroot = (*cteroot).parent_root;
        if cteroot.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "bad levelsup for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
        }
    }

    ndx = 0;
    lc = list_head((*(*cteroot).parse).cteList);
    while !lc.is_null() {
        let cte = lfirst(lc) as *mut CommonTableExpr;

        if libc_strcmp((*cte).ctename, (*rte).ctename) == 0 {
            break;
        }
        ndx += 1;
        lc = lnext((*(*cteroot).parse).cteList, lc);
    }
    if lc.is_null() {
        /* shouldn't happen */
        elog!(ERROR, "could not find CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }
    if ndx >= list_length((*cteroot).cte_plan_ids) {
        elog!(ERROR, "could not find plan for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }
    plan_id = list_nth_int((*cteroot).cte_plan_ids, ndx);
    if plan_id <= 0 {
        elog!(ERROR, "no plan was made for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }
    lc = list_head((*cteroot).init_plans);
    while !lc.is_null() {
        ctesplan = lfirst(lc) as *mut crate::nodes::primnodes::SubPlan;
        if (*ctesplan).plan_id == plan_id {
            break;
        }
        lc = lnext((*cteroot).init_plans, lc);
    }
    if lc.is_null() {
        /* shouldn't happen */
        elog!(ERROR, "could not find plan for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }

    cte_param_id = linitial_int((*ctesplan).setParam);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_ctescan(tlist, scan_clauses, scan_relid, plan_id, cte_param_id);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_namedtuplestorescan_plan
unsafe fn create_namedtuplestorescan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut NamedTuplestoreScan {
    let scan_plan: *mut NamedTuplestoreScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;

    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_NAMEDTUPLESTORE);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_namedtuplestorescan(tlist, scan_clauses, scan_relid, (*rte).enrname);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_resultscan_plan
unsafe fn create_resultscan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut Result {
    let scan_plan: *mut Result;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;

    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_RESULT);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_result(tlist, scan_clauses as *mut Node, core::ptr::null_mut());

    copy_generic_path_info(&raw mut (*scan_plan).plan, best_path);

    scan_plan
}

// create_worktablescan_plan
unsafe fn create_worktablescan_plan(root: *mut PlannerInfo, best_path: *mut Path, tlist: *mut List, mut scan_clauses: *mut List) -> *mut WorkTableScan {
    let scan_plan: *mut WorkTableScan;
    let scan_relid = (*(*best_path).parent).relid;
    let rte: *mut RangeTblEntry;
    let mut levelsup: Index;
    let mut cteroot: *mut PlannerInfo;

    Assert!(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert!((*rte).rtekind == RTE_CTE);
    Assert!((*rte).self_reference);

    levelsup = (*rte).ctelevelsup;
    if levelsup == 0 {
        /* shouldn't happen */
        elog!(ERROR, "bad levelsup for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }
    levelsup -= 1;
    cteroot = root;
    while levelsup > 0 {
        levelsup -= 1;
        cteroot = (*cteroot).parent_root;
        if cteroot.is_null() {
            /* shouldn't happen */
            elog!(ERROR, "bad levelsup for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
        }
    }
    if (*cteroot).wt_param_id < 0 {
        /* shouldn't happen */
        elog!(ERROR, "could not find param ID for CTE \"{}\"", CStr::from_ptr((*rte).ctename).to_string_lossy());
    }

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).param_info.is_null() {
        scan_clauses = replace_nestloop_params(root, scan_clauses as *mut Node) as *mut List;
    }

    scan_plan = make_worktablescan(tlist, scan_clauses, scan_relid, (*cteroot).wt_param_id);

    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, best_path);

    scan_plan
}

// create_foreignscan_plan
unsafe fn create_foreignscan_plan(root: *mut PlannerInfo, best_path: *mut ForeignPath, tlist: *mut List, mut scan_clauses: *mut List) -> *mut ForeignScan {
    let scan_plan: *mut ForeignScan;
    let rel = (*best_path).path.parent;
    let scan_relid = (*rel).relid;
    let mut rel_oid: Oid = InvalidOid;
    let mut outer_plan: *mut Plan = core::ptr::null_mut();

    Assert!(!(*rel).fdwroutine.is_null());

    /* transform the child path if any */
    if !(*best_path).fdw_outerpath.is_null() {
        outer_plan = create_plan_recurse(root, (*best_path).fdw_outerpath, CP_EXACT_TLIST);
    }

    if scan_relid > 0 {
        let rte: *mut RangeTblEntry;

        Assert!((*rel).rtekind == RTE_RELATION);
        rte = planner_rt_fetch(scan_relid, root);
        Assert!((*rte).rtekind == RTE_RELATION);
        rel_oid = (*rte).relid;
    }

    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* TODO(pg-port): GetForeignPlan FDW callback dispatch */
    scan_plan = fdw_GetForeignPlan((*rel).fdwroutine as *mut c_void, root, rel, rel_oid, best_path, tlist, scan_clauses, outer_plan);

    /* Copy cost data from Path to Plan; no need to make FDW do this */
    copy_generic_path_info(&raw mut (*scan_plan).scan.plan, &raw mut (*best_path).path);

    /* Copy user OID to access as; likewise no need to make FDW do this */
    (*scan_plan).checkAsUser = (*rel).userid;

    /* Copy foreign server OID; likewise, no need to make FDW do this */
    (*scan_plan).fs_server = (*rel).serverid;

    if (*rel).reloptkind == RELOPT_UPPER_REL {
        (*scan_plan).fs_relids = (*root).all_query_rels;
    } else {
        (*scan_plan).fs_relids = (*(*best_path).path.parent).relids;
    }

    (*scan_plan).fs_base_relids = bms_difference((*scan_plan).fs_relids, (*root).outer_join_rels);

    if (*rel).useridiscurrent {
        (*(*root).glob).dependsOnRole = true;
    }

    if !(*best_path).path.param_info.is_null() {
        (*scan_plan).scan.plan.qual = replace_nestloop_params(root, (*scan_plan).scan.plan.qual as *mut Node) as *mut List;
        (*scan_plan).fdw_exprs = replace_nestloop_params(root, (*scan_plan).fdw_exprs as *mut Node) as *mut List;
        (*scan_plan).fdw_recheck_quals = replace_nestloop_params(root, (*scan_plan).fdw_recheck_quals as *mut Node) as *mut List;
    }

    (*scan_plan).fsSystemCol = false;
    if scan_relid > 0 {
        let mut attrs_used: *mut Bitmapset = core::ptr::null_mut();
        let mut lc: *mut ListCell;
        let mut i: c_int;

        pull_varattnos((*(*rel).reltarget).exprs as *mut Node, scan_relid, &raw mut attrs_used);

        /* Add all the attributes used by restriction clauses. */
        foreach!(lc, (*rel).baserestrictinfo, {
            let rinfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;

            pull_varattnos((*rinfo).clause as *mut Node, scan_relid, &raw mut attrs_used);
        });

        /* Now, are any system columns requested from rel? */
        i = FirstLowInvalidHeapAttributeNumber + 1;
        while i < 0 {
            if bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used) {
                (*scan_plan).fsSystemCol = true;
                break;
            }
            i += 1;
        }

        bms_free(attrs_used);
    }

    scan_plan
}

// create_customscan_plan
unsafe fn create_customscan_plan(root: *mut PlannerInfo, best_path: *mut CustomPath, tlist: *mut List, mut scan_clauses: *mut List) -> *mut CustomScan {
    let cplan: *mut CustomScan;
    let rel = (*best_path).path.parent;
    let mut custom_plans: *mut List = NIL;
    let mut lc: *mut ListCell;

    /* Recursively transform child paths. */
    foreach!(lc, (*best_path).custom_paths, {
        let plan = create_plan_recurse(root, lfirst(current_cell!(lc)) as *mut Path, CP_EXACT_TLIST);

        custom_plans = lappend(custom_plans, plan as *mut c_void);
    });

    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* TODO(pg-port): PlanCustomPath custom-scan provider callback dispatch */
    cplan = castNode!(CustomScan, T_CustomScan, custom_PlanCustomPath((*best_path).methods as *const c_void, root, rel, best_path, tlist, scan_clauses, custom_plans));

    copy_generic_path_info(&raw mut (*cplan).scan.plan, &raw mut (*best_path).path);

    /* Likewise, copy the relids that are represented by this custom scan */
    (*cplan).custom_relids = (*(*best_path).path.parent).relids;

    if !(*best_path).path.param_info.is_null() {
        (*cplan).scan.plan.qual = replace_nestloop_params(root, (*cplan).scan.plan.qual as *mut Node) as *mut List;
        (*cplan).custom_exprs = replace_nestloop_params(root, (*cplan).custom_exprs as *mut Node) as *mut List;
    }

    cplan
}

// *****************************************************************************
//	JOIN METHODS
// *****************************************************************************

unsafe fn create_nestloop_plan(root: *mut PlannerInfo, best_path: *mut NestPath) -> *mut NestLoop {
    let join_plan: *mut NestLoop;
    let mut outer_plan: *mut Plan;
    let inner_plan: *mut Plan;
    let outerrelids: Relids;
    let tlist = build_path_tlist(root, &raw mut (*best_path).jpath.path);
    let mut joinrestrictclauses = (*best_path).jpath.joinrestrictinfo;
    let mut joinclauses: *mut List = core::ptr::null_mut();
    let mut otherclauses: *mut List = core::ptr::null_mut();
    let nestParams: *mut List;
    let mut outer_tlist: *mut List;
    let mut outer_parallel_safe: bool;
    let saveOuterRels = (*root).curOuterRels;
    let mut lc: *mut ListCell;

    (*best_path).jpath.innerjoinpath = reparameterize_path_by_child(root, (*best_path).jpath.innerjoinpath, (*(*best_path).jpath.outerjoinpath).parent);

    Assert!(!(*best_path).jpath.innerjoinpath.is_null());

    /* NestLoop can project, so no need to be picky about child tlists */
    outer_plan = create_plan_recurse(root, (*best_path).jpath.outerjoinpath, 0);

    /* For a nestloop, include outer relids in curOuterRels for inner side */
    outerrelids = (*(*best_path).jpath.outerjoinpath).parent.as_ref().map(|p| p.relids).unwrap_or(core::ptr::null_mut());
    let outerrelids = (*(*best_path).jpath.outerjoinpath).parent;
    let outerrelids = (*outerrelids).relids;
    (*root).curOuterRels = bms_union((*root).curOuterRels, outerrelids);

    inner_plan = create_plan_recurse(root, (*best_path).jpath.innerjoinpath, 0);

    /* Restore curOuterRels */
    bms_free((*root).curOuterRels);
    (*root).curOuterRels = saveOuterRels;

    /* Sort join qual clauses into best execution order */
    joinrestrictclauses = order_qual_clauses(root, joinrestrictclauses);

    if IS_OUTER_JOIN((*best_path).jpath.jointype) {
        extract_actual_join_clauses(joinrestrictclauses, (*(*best_path).jpath.path.parent).relids, &raw mut joinclauses, &raw mut otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinrestrictclauses, false);
        otherclauses = NIL;
    }

    /* Replace any outer-relation variables with nestloop params */
    if !(*best_path).jpath.path.param_info.is_null() {
        joinclauses = replace_nestloop_params(root, joinclauses as *mut Node) as *mut List;
        otherclauses = replace_nestloop_params(root, otherclauses as *mut Node) as *mut List;
    }

    nestParams = identify_current_nestloop_params(root, outerrelids, PATH_REQ_OUTER(best_path as *mut Path));

    outer_tlist = (*outer_plan).targetlist;
    outer_parallel_safe = (*outer_plan).parallel_safe;
    foreach!(lc, nestParams, {
        let nlp = lfirst(current_cell!(lc)) as *mut NestLoopParam;
        let phv: *mut PlaceHolderVar;
        let tle: *mut TargetEntry;

        if IsA!((*nlp).paramval, T_Var) {
            continue; /* nothing to do for simple Vars */
        }
        /* Otherwise it must be a PHV */
        phv = castNode!(PlaceHolderVar, T_PlaceHolderVar, (*nlp).paramval);

        if !tlist_member(phv as *mut Expr, outer_tlist).is_null() {
            continue; /* already available */
        }

        (*phv).phexpr = replace_nestloop_params(root, (*phv).phexpr as *mut Node) as *mut Expr;

        /* Make a shallow copy of outer_tlist, if we didn't already */
        if outer_tlist == (*outer_plan).targetlist {
            outer_tlist = list_copy(outer_tlist);
        }
        /* ... and add the needed expression */
        tle = makeTargetEntry(copyObject(phv) as *mut Expr, (list_length(outer_tlist) + 1) as AttrNumber, core::ptr::null_mut(), true);
        outer_tlist = lappend(outer_tlist, tle as *mut c_void);
        /* ... and track whether tlist is (still) parallel-safe */
        if outer_parallel_safe {
            outer_parallel_safe = is_parallel_safe(root, phv as *mut Node);
        }
    });
    if outer_tlist != (*outer_plan).targetlist {
        outer_plan = change_plan_targetlist(outer_plan, outer_tlist, outer_parallel_safe);
    }

    /* And finally, we can build the join plan node */
    join_plan = make_nestloop(tlist, joinclauses, otherclauses, nestParams, outer_plan, inner_plan, (*best_path).jpath.jointype, (*best_path).jpath.inner_unique);

    copy_generic_path_info(&raw mut (*join_plan).join.plan, &raw mut (*best_path).jpath.path);

    join_plan
}

unsafe fn create_mergejoin_plan(root: *mut PlannerInfo, best_path: *mut MergePath) -> *mut MergeJoin {
    let join_plan: *mut MergeJoin;
    let mut outer_plan: *mut Plan;
    let mut inner_plan: *mut Plan;
    let tlist = build_path_tlist(root, &raw mut (*best_path).jpath.path);
    let mut joinclauses: *mut List = core::ptr::null_mut();
    let mut otherclauses: *mut List = core::ptr::null_mut();
    let mut mergeclauses: *mut List;
    let outerpathkeys: *mut List;
    let innerpathkeys: *mut List;
    let nClauses: c_int;
    let mergefamilies: *mut Oid;
    let mergecollations: *mut Oid;
    let mergereversals: *mut bool;
    let mergenullsfirst: *mut bool;
    let mut opathkey: *mut PathKey;
    let mut opeclass: *mut EquivalenceClass;
    let mut i: c_int;
    let mut lc: *mut ListCell;
    let mut lop: *mut ListCell;
    let mut lip: *mut ListCell;
    let outer_path = (*best_path).jpath.outerjoinpath;
    let inner_path = (*best_path).jpath.innerjoinpath;

    outer_plan = create_plan_recurse(root, (*best_path).jpath.outerjoinpath, if (*best_path).outersortkeys != NIL { CP_SMALL_TLIST } else { 0 });
    inner_plan = create_plan_recurse(root, (*best_path).jpath.innerjoinpath, if (*best_path).innersortkeys != NIL { CP_SMALL_TLIST } else { 0 });

    /* Sort join qual clauses into best execution order */
    /* NB: do NOT reorder the mergeclauses */
    joinclauses = order_qual_clauses(root, (*best_path).jpath.joinrestrictinfo);

    if IS_OUTER_JOIN((*best_path).jpath.jointype) {
        extract_actual_join_clauses(joinclauses, (*(*best_path).jpath.path.parent).relids, &raw mut joinclauses, &raw mut otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinclauses, false);
        otherclauses = NIL;
    }

    mergeclauses = get_actual_clauses((*best_path).path_mergeclauses);
    joinclauses = list_difference(joinclauses, mergeclauses);

    if !(*best_path).jpath.path.param_info.is_null() {
        joinclauses = replace_nestloop_params(root, joinclauses as *mut Node) as *mut List;
        otherclauses = replace_nestloop_params(root, otherclauses as *mut Node) as *mut List;
    }

    mergeclauses = get_switched_clauses((*best_path).path_mergeclauses, (*(*best_path).jpath.outerjoinpath).parent.cast::<RelOptInfo>().as_ref().map(|_| core::ptr::null_mut()).unwrap_or((*(*(*best_path).jpath.outerjoinpath).parent).relids));

    /* Create explicit sort nodes for the outer and inner paths if necessary. */
    if !(*best_path).outersortkeys.is_null() {
        let outer_relids = (*(*outer_path).parent).relids;
        let sort_plan: *mut Plan;

        Assert!(!pathkeys_contained_in((*best_path).outersortkeys, (*outer_path).pathkeys));

        if enable_incremental_sort && (*best_path).outer_presorted_keys > 0 {
            sort_plan = make_incrementalsort_from_pathkeys(outer_plan, (*best_path).outersortkeys, outer_relids, (*best_path).outer_presorted_keys) as *mut Plan;

            label_incrementalsort_with_costsize(root, sort_plan as *mut IncrementalSort, (*best_path).outersortkeys, -1.0);
        } else {
            sort_plan = make_sort_from_pathkeys(outer_plan, (*best_path).outersortkeys, outer_relids) as *mut Plan;

            label_sort_with_costsize(root, sort_plan as *mut Sort, -1.0);
        }

        outer_plan = sort_plan;
        outerpathkeys = (*best_path).outersortkeys;
    } else {
        outerpathkeys = (*(*best_path).jpath.outerjoinpath).pathkeys;
    }

    if !(*best_path).innersortkeys.is_null() {
        let inner_relids = (*(*inner_path).parent).relids;
        let sort: *mut Sort;

        Assert!(!pathkeys_contained_in((*best_path).innersortkeys, (*inner_path).pathkeys));

        sort = make_sort_from_pathkeys(inner_plan, (*best_path).innersortkeys, inner_relids);

        label_sort_with_costsize(root, sort, -1.0);
        inner_plan = sort as *mut Plan;
        innerpathkeys = (*best_path).innersortkeys;
    } else {
        innerpathkeys = (*(*best_path).jpath.innerjoinpath).pathkeys;
    }

    if (*best_path).materialize_inner {
        let matplan = make_material(inner_plan) as *mut Plan;

        copy_plan_costsize(matplan, inner_plan);
        (*matplan).total_cost += cpu_operator_cost * (*matplan).plan_rows;

        inner_plan = matplan;
    }

    nClauses = list_length(mergeclauses);
    Assert!(nClauses == list_length((*best_path).path_mergeclauses));
    mergefamilies = palloc(nClauses as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    mergecollations = palloc(nClauses as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    mergereversals = palloc(nClauses as usize * core::mem::size_of::<bool>()) as *mut bool;
    mergenullsfirst = palloc(nClauses as usize * core::mem::size_of::<bool>()) as *mut bool;

    opathkey = core::ptr::null_mut();
    opeclass = core::ptr::null_mut();
    lop = list_head(outerpathkeys);
    lip = list_head(innerpathkeys);
    i = 0;
    foreach!(lc, (*best_path).path_mergeclauses, {
        let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(lc));
        let oeclass: *mut EquivalenceClass;
        let ieclass: *mut EquivalenceClass;
        let mut ipathkey: *mut PathKey = core::ptr::null_mut();
        let mut ipeclass: *mut EquivalenceClass = core::ptr::null_mut();
        let mut first_inner_match = false;

        /* fetch outer/inner eclass from mergeclause */
        if (*rinfo).outer_is_left {
            oeclass = (*rinfo).left_ec;
            ieclass = (*rinfo).right_ec;
        } else {
            oeclass = (*rinfo).right_ec;
            ieclass = (*rinfo).left_ec;
        }
        Assert!(!oeclass.is_null());
        Assert!(!ieclass.is_null());

        if oeclass != opeclass {
            /* doesn't match the current opathkey, so must match the next */
            if lop.is_null() {
                elog!(ERROR, "outer pathkeys do not match mergeclauses");
            }
            opathkey = lfirst(lop) as *mut PathKey;
            opeclass = (*opathkey).pk_eclass;
            lop = lnext(outerpathkeys, lop);
            if oeclass != opeclass {
                elog!(ERROR, "outer pathkeys do not match mergeclauses");
            }
        }

        if !lip.is_null() {
            ipathkey = lfirst(lip) as *mut PathKey;
            ipeclass = (*ipathkey).pk_eclass;
            if ieclass == ipeclass {
                /* successful first match to this inner pathkey */
                lip = lnext(innerpathkeys, lip);
                first_inner_match = true;
            }
        }
        if !first_inner_match {
            /* redundant clause ... must match something before lip */
            let mut l2: *mut ListCell;

            l2 = list_head(innerpathkeys);
            while !l2.is_null() {
                if l2 == lip {
                    break;
                }
                ipathkey = lfirst(l2) as *mut PathKey;
                ipeclass = (*ipathkey).pk_eclass;
                if ieclass == ipeclass {
                    break;
                }
                l2 = lnext(innerpathkeys, l2);
            }
            if ieclass != ipeclass {
                elog!(ERROR, "inner pathkeys do not match mergeclauses");
            }
        }

        if (*opathkey).pk_opfamily != (*ipathkey).pk_opfamily
            || (*(*opathkey).pk_eclass).ec_collation != (*(*ipathkey).pk_eclass).ec_collation
        {
            elog!(ERROR, "left and right pathkeys do not match in mergejoin");
        }
        if first_inner_match
            && ((*opathkey).pk_cmptype != (*ipathkey).pk_cmptype || (*opathkey).pk_nulls_first != (*ipathkey).pk_nulls_first)
        {
            elog!(ERROR, "left and right pathkeys do not match in mergejoin");
        }

        /* OK, save info for executor */
        *mergefamilies.add(i as usize) = (*opathkey).pk_opfamily;
        *mergecollations.add(i as usize) = (*(*opathkey).pk_eclass).ec_collation;
        *mergereversals.add(i as usize) = if (*opathkey).pk_cmptype == COMPARE_GT { true } else { false };
        *mergenullsfirst.add(i as usize) = (*opathkey).pk_nulls_first;
        i += 1;
    });

    join_plan = make_mergejoin(tlist, joinclauses, otherclauses, mergeclauses, mergefamilies, mergecollations, mergereversals, mergenullsfirst, outer_plan, inner_plan, (*best_path).jpath.jointype, (*best_path).jpath.inner_unique, (*best_path).skip_mark_restore);

    /* Costs of sort and material steps are included in path cost already */
    copy_generic_path_info(&raw mut (*join_plan).join.plan, &raw mut (*best_path).jpath.path);

    join_plan
}

unsafe fn create_hashjoin_plan(root: *mut PlannerInfo, best_path: *mut HashPath) -> *mut HashJoin {
    let join_plan: *mut HashJoin;
    let hash_plan: *mut Hash;
    let outer_plan: *mut Plan;
    let inner_plan: *mut Plan;
    let tlist = build_path_tlist(root, &raw mut (*best_path).jpath.path);
    let mut joinclauses: *mut List = core::ptr::null_mut();
    let mut otherclauses: *mut List = core::ptr::null_mut();
    let mut hashclauses: *mut List;
    let mut hashoperators: *mut List = NIL;
    let mut hashcollations: *mut List = NIL;
    let mut inner_hashkeys: *mut List = NIL;
    let mut outer_hashkeys: *mut List = NIL;
    let mut skewTable: Oid = InvalidOid;
    let mut skewColumn: AttrNumber = InvalidAttrNumber;
    let mut skewInherit = false;
    let mut lc: *mut ListCell;

    outer_plan = create_plan_recurse(root, (*best_path).jpath.outerjoinpath, if (*best_path).num_batches > 1 { CP_SMALL_TLIST } else { 0 });
    inner_plan = create_plan_recurse(root, (*best_path).jpath.innerjoinpath, CP_SMALL_TLIST);

    /* Sort join qual clauses into best execution order */
    joinclauses = order_qual_clauses(root, (*best_path).jpath.joinrestrictinfo);
    /* There's no point in sorting the hash clauses ... */

    if IS_OUTER_JOIN((*best_path).jpath.jointype) {
        extract_actual_join_clauses(joinclauses, (*(*best_path).jpath.path.parent).relids, &raw mut joinclauses, &raw mut otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinclauses, false);
        otherclauses = NIL;
    }

    hashclauses = get_actual_clauses((*best_path).path_hashclauses);
    joinclauses = list_difference(joinclauses, hashclauses);

    if !(*best_path).jpath.path.param_info.is_null() {
        joinclauses = replace_nestloop_params(root, joinclauses as *mut Node) as *mut List;
        otherclauses = replace_nestloop_params(root, otherclauses as *mut Node) as *mut List;
    }

    hashclauses = get_switched_clauses((*best_path).path_hashclauses, (*(*(*best_path).jpath.outerjoinpath).parent).relids);

    if list_length(hashclauses) == 1 {
        let clause = linitial(hashclauses) as *mut OpExpr;
        let mut node: *mut Node;

        Assert!(is_opclause(clause as *const c_void));
        node = linitial((*clause).args) as *mut Node;
        if IsA!(node, T_RelabelType) {
            node = (*(node as *mut RelabelType)).arg as *mut Node;
        }
        if IsA!(node, T_Var) {
            let var = node as *mut Var;
            let rte: *mut RangeTblEntry;

            rte = *(*root).simple_rte_array.add((*var).varno as usize);
            if (*rte).rtekind == RTE_RELATION {
                skewTable = (*rte).relid;
                skewColumn = (*var).varattno;
                skewInherit = (*rte).inh;
            }
        }
    }

    foreach!(lc, hashclauses, {
        let hclause = lfirst_node!(OpExpr, T_OpExpr, current_cell!(lc));

        hashoperators = lappend_oid(hashoperators, (*hclause).opno);
        hashcollations = lappend_oid(hashcollations, (*hclause).inputcollid);
        outer_hashkeys = lappend(outer_hashkeys, linitial((*hclause).args));
        inner_hashkeys = lappend(inner_hashkeys, lsecond((*hclause).args));
    });

    hash_plan = make_hash(inner_plan, inner_hashkeys, skewTable, skewColumn, skewInherit);

    copy_plan_costsize(&raw mut (*hash_plan).plan, inner_plan);
    (*hash_plan).plan.startup_cost = (*hash_plan).plan.total_cost;

    if (*best_path).jpath.path.parallel_aware {
        (*hash_plan).plan.parallel_aware = true;
        (*hash_plan).rows_total = (*best_path).inner_rows_total;
    }

    join_plan = make_hashjoin(tlist, joinclauses, otherclauses, hashclauses, hashoperators, hashcollations, outer_hashkeys, outer_plan, hash_plan as *mut Plan, (*best_path).jpath.jointype, (*best_path).jpath.inner_unique);

    copy_generic_path_info(&raw mut (*join_plan).join.plan, &raw mut (*best_path).jpath.path);

    join_plan
}

// *****************************************************************************
//	SUPPORTING ROUTINES
// *****************************************************************************

// replace_nestloop_params
unsafe fn replace_nestloop_params(root: *mut PlannerInfo, expr: *mut Node) -> *mut Node {
    /* No setup needed for tree walk, so away we go */
    replace_nestloop_params_mutator(expr, root)
}

unsafe fn replace_nestloop_params_mutator(node: *mut Node, root: *mut PlannerInfo) -> *mut Node {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(node, T_Var) {
        let var = node as *mut Var;

        /* Upper-level Vars should be long gone at this point */
        Assert!((*var).varlevelsup == 0);
        /* If not to be replaced, we can just return the Var unmodified */
        if IS_SPECIAL_VARNO((*var).varno) || !bms_is_member((*var).varno as c_int, (*root).curOuterRels) {
            return node;
        }
        /* Replace the Var with a nestloop Param */
        return replace_nestloop_param_var(root, var) as *mut Node;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;

        /* Upper-level PlaceHolderVars should be long gone at this point */
        Assert!((*phv).phlevelsup == 0);

        if !bms_is_subset((*find_placeholder_info(root, phv)).ph_eval_at, (*root).curOuterRels) {
            let newphv = makeNode!(PlaceHolderVar, T_PlaceHolderVar);

            core::ptr::copy_nonoverlapping(phv, newphv, 1);
            (*newphv).phexpr = replace_nestloop_params_mutator((*phv).phexpr as *mut Node, root) as *mut Expr;
            return newphv as *mut Node;
        }
        /* Replace the PlaceHolderVar with a nestloop Param */
        return replace_nestloop_param_placeholdervar(root, phv) as *mut Node;
    }
    expression_tree_mutator(node, replace_nestloop_params_mutator, root)
}

// fix_indexqual_references
unsafe fn fix_indexqual_references(root: *mut PlannerInfo, index_path: *mut IndexPath, stripped_indexquals_p: *mut *mut List, fixed_indexquals_p: *mut *mut List) {
    let index = (*index_path).indexinfo;
    let mut stripped_indexquals: *mut List;
    let mut fixed_indexquals: *mut List;
    let mut lc: *mut ListCell;

    stripped_indexquals = NIL;
    fixed_indexquals = NIL;

    foreach!(lc, (*index_path).indexclauses, {
        let iclause = lfirst_node!(IndexClause, T_IndexClause, current_cell!(lc));
        let indexcol = (*iclause).indexcol;
        let mut lc2: *mut ListCell;

        foreach!(lc2, (*iclause).indexquals, {
            let rinfo = lfirst_node!(RestrictInfo, T_RestrictInfo, current_cell!(lc2));
            let mut clause = (*rinfo).clause as *mut Node;

            stripped_indexquals = lappend(stripped_indexquals, clause as *mut c_void);
            clause = fix_indexqual_clause(root, index, indexcol as c_int, clause, (*iclause).indexcols);
            fixed_indexquals = lappend(fixed_indexquals, clause as *mut c_void);
        });
    });

    *stripped_indexquals_p = stripped_indexquals;
    *fixed_indexquals_p = fixed_indexquals;
}

// fix_indexorderby_references
unsafe fn fix_indexorderby_references(root: *mut PlannerInfo, index_path: *mut IndexPath) -> *mut List {
    let index = (*index_path).indexinfo;
    let mut fixed_indexorderbys: *mut List;
    let mut lcc: *mut ListCell;
    let mut lci: *mut ListCell;

    fixed_indexorderbys = NIL;

    forboth!(lcc, (*index_path).indexorderbys, lci, (*index_path).indexorderbycols, {
        let mut clause = lfirst(lcc) as *mut Node;
        let indexcol = lfirst_int(lci);

        clause = fix_indexqual_clause(root, index, indexcol as c_int, clause, NIL);
        fixed_indexorderbys = lappend(fixed_indexorderbys, clause as *mut c_void);
    });

    fixed_indexorderbys
}

// fix_indexqual_clause
unsafe fn fix_indexqual_clause(root: *mut PlannerInfo, index: *mut IndexOptInfo, indexcol: c_int, mut clause: *mut Node, indexcolnos: *mut List) -> *mut Node {
    clause = replace_nestloop_params(root, clause);

    if IsA!(clause, T_OpExpr) {
        let op = clause as *mut OpExpr;

        /* Replace the indexkey expression with an index Var. */
        let head = list_head((*op).args);
        (*head).ptr_value = fix_indexqual_operand(linitial((*op).args) as *mut Node, index, indexcol) as *mut c_void;
    } else if IsA!(clause, T_RowCompareExpr) {
        let rc = clause as *mut RowCompareExpr;
        let mut lca: *mut ListCell;
        let mut lcai: *mut ListCell;

        /* Replace the indexkey expressions with index Vars. */
        Assert!(list_length((*rc).largs) == list_length(indexcolnos));
        forboth!(lca, (*rc).largs, lcai, indexcolnos, {
            (*lca).ptr_value = fix_indexqual_operand(lfirst(lca) as *mut Node, index, lfirst_int(lcai)) as *mut c_void;
        });
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        let saop = clause as *mut ScalarArrayOpExpr;

        /* Replace the indexkey expression with an index Var. */
        let head = list_head((*saop).args);
        (*head).ptr_value = fix_indexqual_operand(linitial((*saop).args) as *mut Node, index, indexcol) as *mut c_void;
    } else if IsA!(clause, T_NullTest) {
        let nt = clause as *mut NullTest;

        /* Replace the indexkey expression with an index Var. */
        (*nt).arg = fix_indexqual_operand((*nt).arg as *mut Node, index, indexcol) as *mut Expr;
    } else {
        elog!(ERROR, "unsupported indexqual type: {}", nodeTag(clause) as c_int);
    }

    clause
}

// fix_indexqual_operand
unsafe fn fix_indexqual_operand(mut node: *mut Node, index: *mut IndexOptInfo, indexcol: c_int) -> *mut Node {
    let result: *mut Var;
    let mut pos: c_int;
    let mut indexpr_item: *mut ListCell;

    Assert!(indexcol >= 0 && indexcol < (*index).ncolumns);

    /* Remove any PlaceHolderVar wrapping of the indexkey */
    node = strip_phvs_in_index_operand(node);

    /* Remove any binary-compatible relabeling of the indexkey */
    while IsA!(node, T_RelabelType) {
        node = (*(node as *mut RelabelType)).arg as *mut Node;
    }

    if *(*index).indexkeys.add(indexcol as usize) != 0 {
        /* It's a simple index column */
        if IsA!(node, T_Var)
            && (*(node as *mut Var)).varno == (*(*index).rel).relid as c_int
            && (*(node as *mut Var)).varattno as c_int == *(*index).indexkeys.add(indexcol as usize)
        {
            let result = copyObject(node) as *mut Var;
            (*result).varno = INDEX_VAR;
            (*result).varattno = (indexcol + 1) as AttrNumber;
            return result as *mut Node;
        } else {
            elog!(ERROR, "index key does not match expected index column");
        }
    }

    /* It's an index expression, so find and cross-check the expression */
    indexpr_item = list_head((*index).indexprs);
    pos = 0;
    while pos < (*index).ncolumns {
        if *(*index).indexkeys.add(pos as usize) == 0 {
            if indexpr_item.is_null() {
                elog!(ERROR, "too few entries in indexprs list");
            }
            if pos == indexcol {
                let mut indexkey: *mut Node;

                indexkey = lfirst(indexpr_item) as *mut Node;
                if !indexkey.is_null() && IsA!(indexkey, T_RelabelType) {
                    indexkey = (*(indexkey as *mut RelabelType)).arg as *mut Node;
                }
                if equal(node as *const c_void, indexkey as *const c_void) {
                    let result = makeVar(INDEX_VAR, (indexcol + 1) as AttrNumber, exprType(lfirst(indexpr_item) as *mut Node), -1, exprCollation(lfirst(indexpr_item) as *mut Node), 0);
                    return result as *mut Node;
                } else {
                    elog!(ERROR, "index key does not match expected index column");
                }
            }
            indexpr_item = lnext((*index).indexprs, indexpr_item);
        }
        pos += 1;
    }

    /* Oops... */
    elog!(ERROR, "index key does not match expected index column");
    unreachable!();
}

// get_switched_clauses
unsafe fn get_switched_clauses(clauses: *mut List, outerrelids: Relids) -> *mut List {
    let mut t_list: *mut List = NIL;
    let mut l: *mut ListCell;

    foreach!(l, clauses, {
        let restrictinfo = lfirst(current_cell!(l)) as *mut RestrictInfo;
        let clause = (*restrictinfo).clause as *mut OpExpr;

        Assert!(is_opclause(clause as *const c_void));
        if bms_is_subset((*restrictinfo).right_relids, outerrelids) {
            let temp = makeNode!(OpExpr, T_OpExpr);

            (*temp).opno = (*clause).opno;
            (*temp).opfuncid = InvalidOid;
            (*temp).opresulttype = (*clause).opresulttype;
            (*temp).opretset = (*clause).opretset;
            (*temp).opcollid = (*clause).opcollid;
            (*temp).inputcollid = (*clause).inputcollid;
            (*temp).args = list_copy((*clause).args);
            (*temp).location = (*clause).location;
            /* Commute it --- note this modifies the temp node in-place. */
            CommuteOpExpr(temp);
            t_list = lappend(t_list, temp as *mut c_void);
            (*restrictinfo).outer_is_left = false;
        } else {
            Assert!(bms_is_subset((*restrictinfo).left_relids, outerrelids));
            t_list = lappend(t_list, clause as *mut c_void);
            (*restrictinfo).outer_is_left = true;
        }
    });
    t_list
}

// order_qual_clauses
unsafe fn order_qual_clauses(root: *mut PlannerInfo, clauses: *mut List) -> *mut List {
    #[derive(Clone, Copy)]
    struct QualItem {
        clause: *mut Node,
        cost: Cost,
        security_level: Index,
    }
    let nitems = list_length(clauses);
    let items: *mut QualItem;
    let mut lc: *mut ListCell;
    let mut i: c_int;
    let mut result: *mut List;

    /* No need to work hard for 0 or 1 clause */
    if nitems <= 1 {
        return clauses;
    }

    items = palloc(nitems as usize * core::mem::size_of::<QualItem>()) as *mut QualItem;
    i = 0;
    foreach!(lc, clauses, {
        let clause = lfirst(current_cell!(lc)) as *mut Node;
        let mut qcost = QualCost { startup: 0.0, per_tuple: 0.0 };

        cost_qual_eval_node(&raw mut qcost as *mut crate::nodes::pathnodes::QualCost, clause, root);
        (*items.add(i as usize)).clause = clause;
        (*items.add(i as usize)).cost = qcost.per_tuple;
        if IsA!(clause, T_RestrictInfo) {
            let rinfo = clause as *mut RestrictInfo;

            if (*rinfo).leakproof && (*items.add(i as usize)).cost < 10.0 * cpu_operator_cost {
                (*items.add(i as usize)).security_level = 0;
            } else {
                (*items.add(i as usize)).security_level = (*rinfo).security_level;
            }
        } else {
            (*items.add(i as usize)).security_level = 0;
        }
        i += 1;
    });

    /* Insertion sort. */
    i = 1;
    while i < nitems {
        let newitem = *items.add(i as usize);
        let mut j: c_int;

        j = i;
        while j > 0 {
            let olditem = items.add((j - 1) as usize);

            if newitem.security_level > (*olditem).security_level
                || (newitem.security_level == (*olditem).security_level && newitem.cost >= (*olditem).cost)
            {
                break;
            }
            *items.add(j as usize) = *olditem;
            j -= 1;
        }
        *items.add(j as usize) = newitem;
        i += 1;
    }

    /* Convert back to a list */
    result = NIL;
    i = 0;
    while i < nitems {
        result = lappend(result, (*items.add(i as usize)).clause as *mut c_void);
        i += 1;
    }

    result
}

// copy_generic_path_info
unsafe fn copy_generic_path_info(dest: *mut Plan, src: *mut Path) {
    (*dest).disabled_nodes = (*src).disabled_nodes;
    (*dest).startup_cost = (*src).startup_cost;
    (*dest).total_cost = (*src).total_cost;
    (*dest).plan_rows = (*src).rows;
    (*dest).plan_width = (*(*src).pathtarget).width;
    (*dest).parallel_aware = (*src).parallel_aware;
    (*dest).parallel_safe = (*src).parallel_safe;
}

// copy_plan_costsize
unsafe fn copy_plan_costsize(dest: *mut Plan, src: *mut Plan) {
    (*dest).disabled_nodes = (*src).disabled_nodes;
    (*dest).startup_cost = (*src).startup_cost;
    (*dest).total_cost = (*src).total_cost;
    (*dest).plan_rows = (*src).plan_rows;
    (*dest).plan_width = (*src).plan_width;
    /* Assume the inserted node is not parallel-aware. */
    (*dest).parallel_aware = false;
    /* Assume the inserted node is parallel-safe, if child plan is. */
    (*dest).parallel_safe = (*src).parallel_safe;
}

// label_sort_with_costsize
unsafe fn label_sort_with_costsize(root: *mut PlannerInfo, plan: *mut Sort, limit_tuples: f64) {
    let lefttree = (*plan).plan.lefttree;
    let mut sort_path: Path = core::mem::zeroed(); /* dummy for result of cost_sort */

    Assert!(IsA!(plan, T_Sort));

    cost_sort(&raw mut sort_path, root, NIL, (*plan).plan.disabled_nodes, (*lefttree).total_cost, (*lefttree).plan_rows, (*lefttree).plan_width, 0.0, work_mem, limit_tuples);
    (*plan).plan.startup_cost = sort_path.startup_cost;
    (*plan).plan.total_cost = sort_path.total_cost;
    (*plan).plan.plan_rows = (*lefttree).plan_rows;
    (*plan).plan.plan_width = (*lefttree).plan_width;
    (*plan).plan.parallel_aware = false;
    (*plan).plan.parallel_safe = (*lefttree).parallel_safe;
}

// label_incrementalsort_with_costsize
unsafe fn label_incrementalsort_with_costsize(root: *mut PlannerInfo, plan: *mut IncrementalSort, pathkeys: *mut List, limit_tuples: f64) {
    let lefttree = (*plan).sort.plan.lefttree;
    let mut sort_path: Path = core::mem::zeroed(); /* dummy for result of cost_incremental_sort */

    Assert!(IsA!(plan, T_IncrementalSort));

    cost_incremental_sort(&raw mut sort_path, root, pathkeys, (*plan).nPresortedCols, (*plan).sort.plan.disabled_nodes, (*lefttree).startup_cost, (*lefttree).total_cost, (*lefttree).plan_rows, (*lefttree).plan_width, 0.0, work_mem, limit_tuples);
    (*plan).sort.plan.startup_cost = sort_path.startup_cost;
    (*plan).sort.plan.total_cost = sort_path.total_cost;
    (*plan).sort.plan.plan_rows = (*lefttree).plan_rows;
    (*plan).sort.plan.plan_width = (*lefttree).plan_width;
    (*plan).sort.plan.parallel_aware = false;
    (*plan).sort.plan.parallel_safe = (*lefttree).parallel_safe;
}

// bitmap_subplan_mark_shared
unsafe fn bitmap_subplan_mark_shared(plan: *mut Plan) {
    if IsA!(plan, T_BitmapAnd) {
        bitmap_subplan_mark_shared(linitial((*(plan as *mut BitmapAnd)).bitmapplans) as *mut Plan);
    } else if IsA!(plan, T_BitmapOr) {
        (*(plan as *mut BitmapOr)).isshared = true;
        bitmap_subplan_mark_shared(linitial((*(plan as *mut BitmapOr)).bitmapplans) as *mut Plan);
    } else if IsA!(plan, T_BitmapIndexScan) {
        (*(plan as *mut BitmapIndexScan)).isshared = true;
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(plan) as c_int);
    }
}

// *****************************************************************************
//	PLAN NODE BUILDING ROUTINES
// *****************************************************************************

unsafe fn make_seqscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index) -> *mut SeqScan {
    let node = makeNode!(SeqScan, T_SeqScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;

    node
}

unsafe fn make_samplescan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, tsc: *mut TableSampleClause) -> *mut SampleScan {
    let node = makeNode!(SampleScan, T_SampleScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).tablesample = tsc;

    node
}

unsafe fn make_indexscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, indexid: Oid, indexqual: *mut List, indexqualorig: *mut List, indexorderby: *mut List, indexorderbyorig: *mut List, indexorderbyops: *mut List, indexscandir: ScanDirection) -> *mut IndexScan {
    let node = makeNode!(IndexScan, T_IndexScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).indexid = indexid;
    (*node).indexqual = indexqual;
    (*node).indexqualorig = indexqualorig;
    (*node).indexorderby = indexorderby;
    (*node).indexorderbyorig = indexorderbyorig;
    (*node).indexorderbyops = indexorderbyops;
    (*node).indexorderdir = indexscandir;

    node
}

unsafe fn make_indexonlyscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, indexid: Oid, indexqual: *mut List, recheckqual: *mut List, indexorderby: *mut List, indextlist: *mut List, indexscandir: ScanDirection) -> *mut IndexOnlyScan {
    let node = makeNode!(IndexOnlyScan, T_IndexOnlyScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).indexid = indexid;
    (*node).indexqual = indexqual;
    (*node).recheckqual = recheckqual;
    (*node).indexorderby = indexorderby;
    (*node).indextlist = indextlist;
    (*node).indexorderdir = indexscandir;

    node
}

unsafe fn make_bitmap_indexscan(scanrelid: Index, indexid: Oid, indexqual: *mut List, indexqualorig: *mut List) -> *mut BitmapIndexScan {
    let node = makeNode!(BitmapIndexScan, T_BitmapIndexScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = NIL; /* not used */
    (*plan).qual = NIL; /* not used */
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).indexid = indexid;
    (*node).indexqual = indexqual;
    (*node).indexqualorig = indexqualorig;

    node
}

unsafe fn make_bitmap_heapscan(qptlist: *mut List, qpqual: *mut List, lefttree: *mut Plan, bitmapqualorig: *mut List, scanrelid: Index) -> *mut BitmapHeapScan {
    let node = makeNode!(BitmapHeapScan, T_BitmapHeapScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).bitmapqualorig = bitmapqualorig;

    node
}

unsafe fn make_tidscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, tidquals: *mut List) -> *mut TidScan {
    let node = makeNode!(TidScan, T_TidScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).tidquals = tidquals;

    node
}

unsafe fn make_tidrangescan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, tidrangequals: *mut List) -> *mut TidRangeScan {
    let node = makeNode!(TidRangeScan, T_TidRangeScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).tidrangequals = tidrangequals;

    node
}

unsafe fn make_subqueryscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, subplan: *mut Plan) -> *mut SubqueryScan {
    let node = makeNode!(SubqueryScan, T_SubqueryScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).subplan = subplan;
    (*node).scanstatus = SUBQUERY_SCAN_UNKNOWN;

    node
}

unsafe fn make_functionscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, functions: *mut List, funcordinality: bool) -> *mut FunctionScan {
    let node = makeNode!(FunctionScan, T_FunctionScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).functions = functions;
    (*node).funcordinality = funcordinality;

    node
}

unsafe fn make_tablefuncscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, tablefunc: *mut TableFunc) -> *mut TableFuncScan {
    let node = makeNode!(TableFuncScan, T_TableFuncScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).tablefunc = tablefunc;

    node
}

unsafe fn make_valuesscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, values_lists: *mut List) -> *mut ValuesScan {
    let node = makeNode!(ValuesScan, T_ValuesScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).values_lists = values_lists;

    node
}

unsafe fn make_ctescan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, ctePlanId: c_int, cteParam: c_int) -> *mut CteScan {
    let node = makeNode!(CteScan, T_CteScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).ctePlanId = ctePlanId;
    (*node).cteParam = cteParam;

    node
}

unsafe fn make_namedtuplestorescan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, enrname: *mut c_char) -> *mut NamedTuplestoreScan {
    let node = makeNode!(NamedTuplestoreScan, T_NamedTuplestoreScan);
    let plan = &raw mut (*node).scan.plan;

    /* cost should be inserted by caller */
    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).enrname = enrname;

    node
}

unsafe fn make_worktablescan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, wtParam: c_int) -> *mut WorkTableScan {
    let node = makeNode!(WorkTableScan, T_WorkTableScan);
    let plan = &raw mut (*node).scan.plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;
    (*node).wtParam = wtParam;

    node
}

pub unsafe fn make_foreignscan(qptlist: *mut List, qpqual: *mut List, scanrelid: Index, fdw_exprs: *mut List, fdw_private: *mut List, fdw_scan_tlist: *mut List, fdw_recheck_quals: *mut List, outer_plan: *mut Plan) -> *mut ForeignScan {
    let node = makeNode!(ForeignScan, T_ForeignScan);
    let plan = &raw mut (*node).scan.plan;

    /* cost will be filled in by create_foreignscan_plan */
    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = outer_plan;
    (*plan).righttree = core::ptr::null_mut();
    (*node).scan.scanrelid = scanrelid;

    /* these may be overridden by the FDW's PlanDirectModify callback. */
    (*node).operation = CMD_SELECT;
    (*node).resultRelation = 0;

    /* checkAsUser, fs_server will be filled in by create_foreignscan_plan */
    (*node).checkAsUser = InvalidOid;
    (*node).fs_server = InvalidOid;
    (*node).fdw_exprs = fdw_exprs;
    (*node).fdw_private = fdw_private;
    (*node).fdw_scan_tlist = fdw_scan_tlist;
    (*node).fdw_recheck_quals = fdw_recheck_quals;
    /* fs_relids, fs_base_relids will be filled by create_foreignscan_plan */
    (*node).fs_relids = core::ptr::null_mut();
    (*node).fs_base_relids = core::ptr::null_mut();
    /* fsSystemCol will be filled in by create_foreignscan_plan */
    (*node).fsSystemCol = false;

    node
}

unsafe fn make_recursive_union(tlist: *mut List, lefttree: *mut Plan, righttree: *mut Plan, wtParam: c_int, distinctList: *mut List, numGroups: c_long) -> *mut RecursiveUnion {
    let node = makeNode!(RecursiveUnion, T_RecursiveUnion);
    let plan = &raw mut (*node).plan;
    let numCols = list_length(distinctList);

    (*plan).targetlist = tlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = righttree;
    (*node).wtParam = wtParam;

    (*node).numCols = numCols;
    if numCols > 0 {
        let mut keyno: c_int = 0;
        let dupColIdx: *mut AttrNumber;
        let dupOperators: *mut Oid;
        let dupCollations: *mut Oid;
        let mut slitem: *mut ListCell;

        dupColIdx = palloc(core::mem::size_of::<AttrNumber>() * numCols as usize) as *mut AttrNumber;
        dupOperators = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;
        dupCollations = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;

        foreach!(slitem, distinctList, {
            let sortcl = lfirst(current_cell!(slitem)) as *mut SortGroupClause;
            let tle = get_sortgroupclause_tle(sortcl, (*plan).targetlist);

            *dupColIdx.add(keyno as usize) = (*tle).resno;
            *dupOperators.add(keyno as usize) = (*sortcl).eqop;
            *dupCollations.add(keyno as usize) = exprCollation((*tle).expr as *mut Node);
            Assert!(OidIsValid(*dupOperators.add(keyno as usize)));
            keyno += 1;
        });
        (*node).dupColIdx = dupColIdx;
        (*node).dupOperators = dupOperators;
        (*node).dupCollations = dupCollations;
    }
    (*node).numGroups = numGroups;

    node
}

unsafe fn make_bitmap_and(bitmapplans: *mut List) -> *mut BitmapAnd {
    let node = makeNode!(BitmapAnd, T_BitmapAnd);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = NIL;
    (*plan).qual = NIL;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).bitmapplans = bitmapplans;

    node
}

unsafe fn make_bitmap_or(bitmapplans: *mut List) -> *mut BitmapOr {
    let node = makeNode!(BitmapOr, T_BitmapOr);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = NIL;
    (*plan).qual = NIL;
    (*plan).lefttree = core::ptr::null_mut();
    (*plan).righttree = core::ptr::null_mut();
    (*node).bitmapplans = bitmapplans;

    node
}

unsafe fn make_nestloop(tlist: *mut List, joinclauses: *mut List, otherclauses: *mut List, nestParams: *mut List, lefttree: *mut Plan, righttree: *mut Plan, jointype: JoinType, inner_unique: bool) -> *mut NestLoop {
    let node = makeNode!(NestLoop, T_NestLoop);
    let plan = &raw mut (*node).join.plan;

    (*plan).targetlist = tlist;
    (*plan).qual = otherclauses;
    (*plan).lefttree = lefttree;
    (*plan).righttree = righttree;
    (*node).join.jointype = jointype;
    (*node).join.inner_unique = inner_unique;
    (*node).join.joinqual = joinclauses;
    (*node).nestParams = nestParams;

    node
}

unsafe fn make_hashjoin(tlist: *mut List, joinclauses: *mut List, otherclauses: *mut List, hashclauses: *mut List, hashoperators: *mut List, hashcollations: *mut List, hashkeys: *mut List, lefttree: *mut Plan, righttree: *mut Plan, jointype: JoinType, inner_unique: bool) -> *mut HashJoin {
    let node = makeNode!(HashJoin, T_HashJoin);
    let plan = &raw mut (*node).join.plan;

    (*plan).targetlist = tlist;
    (*plan).qual = otherclauses;
    (*plan).lefttree = lefttree;
    (*plan).righttree = righttree;
    (*node).hashclauses = hashclauses;
    (*node).hashoperators = hashoperators;
    (*node).hashcollations = hashcollations;
    (*node).hashkeys = hashkeys;
    (*node).join.jointype = jointype;
    (*node).join.inner_unique = inner_unique;
    (*node).join.joinqual = joinclauses;

    node
}

unsafe fn make_hash(lefttree: *mut Plan, hashkeys: *mut List, skewTable: Oid, skewColumn: AttrNumber, skewInherit: bool) -> *mut Hash {
    let node = makeNode!(Hash, T_Hash);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    (*node).hashkeys = hashkeys;
    (*node).skewTable = skewTable;
    (*node).skewColumn = skewColumn;
    (*node).skewInherit = skewInherit;

    node
}

unsafe fn make_mergejoin(tlist: *mut List, joinclauses: *mut List, otherclauses: *mut List, mergeclauses: *mut List, mergefamilies: *mut Oid, mergecollations: *mut Oid, mergereversals: *mut bool, mergenullsfirst: *mut bool, lefttree: *mut Plan, righttree: *mut Plan, jointype: JoinType, inner_unique: bool, skip_mark_restore: bool) -> *mut MergeJoin {
    let node = makeNode!(MergeJoin, T_MergeJoin);
    let plan = &raw mut (*node).join.plan;

    (*plan).targetlist = tlist;
    (*plan).qual = otherclauses;
    (*plan).lefttree = lefttree;
    (*plan).righttree = righttree;
    (*node).skip_mark_restore = skip_mark_restore;
    (*node).mergeclauses = mergeclauses;
    (*node).mergeFamilies = mergefamilies;
    (*node).mergeCollations = mergecollations;
    (*node).mergeReversals = mergereversals;
    (*node).mergeNullsFirst = mergenullsfirst;
    (*node).join.jointype = jointype;
    (*node).join.inner_unique = inner_unique;
    (*node).join.joinqual = joinclauses;

    node
}

// make_sort --- basic routine to build a Sort plan node
unsafe fn make_sort(lefttree: *mut Plan, numCols: c_int, sortColIdx: *mut AttrNumber, sortOperators: *mut Oid, collations: *mut Oid, nullsFirst: *mut bool) -> *mut Sort {
    let node: *mut Sort;
    let plan: *mut Plan;

    node = makeNode!(Sort, T_Sort);

    plan = &raw mut (*node).plan;
    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).disabled_nodes = (*lefttree).disabled_nodes + if enable_sort == false { 1 } else { 0 };
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();
    (*node).numCols = numCols;
    (*node).sortColIdx = sortColIdx;
    (*node).sortOperators = sortOperators;
    (*node).collations = collations;
    (*node).nullsFirst = nullsFirst;

    node
}

// make_incrementalsort --- basic routine to build an IncrementalSort plan node
unsafe fn make_incrementalsort(lefttree: *mut Plan, numCols: c_int, nPresortedCols: c_int, sortColIdx: *mut AttrNumber, sortOperators: *mut Oid, collations: *mut Oid, nullsFirst: *mut bool) -> *mut IncrementalSort {
    let node: *mut IncrementalSort;
    let plan: *mut Plan;

    node = makeNode!(IncrementalSort, T_IncrementalSort);

    plan = &raw mut (*node).sort.plan;
    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();
    (*node).nPresortedCols = nPresortedCols;
    (*node).sort.numCols = numCols;
    (*node).sort.sortColIdx = sortColIdx;
    (*node).sort.sortOperators = sortOperators;
    (*node).sort.collations = collations;
    (*node).sort.nullsFirst = nullsFirst;

    node
}

// prepare_sort_from_pathkeys
unsafe fn prepare_sort_from_pathkeys(mut lefttree: *mut Plan, pathkeys: *mut List, relids: Relids, reqColIdx: *const AttrNumber, mut adjust_tlist_in_place: bool, p_numsortkeys: *mut c_int, p_sortColIdx: *mut *mut AttrNumber, p_sortOperators: *mut *mut Oid, p_collations: *mut *mut Oid, p_nullsFirst: *mut *mut bool) -> *mut Plan {
    let mut tlist = (*lefttree).targetlist;
    let mut i: *mut ListCell;
    let mut numsortkeys: c_int;
    let sortColIdx: *mut AttrNumber;
    let sortOperators: *mut Oid;
    let collations: *mut Oid;
    let nullsFirst: *mut bool;

    numsortkeys = list_length(pathkeys);
    sortColIdx = palloc(numsortkeys as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    sortOperators = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    collations = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    nullsFirst = palloc(numsortkeys as usize * core::mem::size_of::<bool>()) as *mut bool;

    numsortkeys = 0;

    foreach!(i, pathkeys, {
        let pathkey = lfirst(current_cell!(i)) as *mut PathKey;
        let ec = (*pathkey).pk_eclass;
        let mut em: *mut EquivalenceMember;
        let mut tle: *mut TargetEntry = core::ptr::null_mut();
        let mut pk_datatype: Oid = InvalidOid;
        let sortop: Oid;
        let mut j: *mut ListCell;

        if (*ec).ec_has_volatile {
            if (*ec).ec_sortref == 0 {
                /* can't happen */
                elog!(ERROR, "volatile EquivalenceClass has no sortref");
            }
            tle = get_sortgroupref_tle((*ec).ec_sortref, tlist);
            Assert!(!tle.is_null());
            Assert!(list_length((*ec).ec_members) == 1);
            pk_datatype = (*(linitial((*ec).ec_members) as *mut EquivalenceMember)).em_datatype;
        } else if !reqColIdx.is_null() {
            tle = get_tle_by_resno(tlist, *reqColIdx.add(numsortkeys as usize));
            if !tle.is_null() {
                em = find_ec_member_matching_expr(ec, (*tle).expr, relids);
                if !em.is_null() {
                    /* found expr at right place in tlist */
                    pk_datatype = (*em).em_datatype;
                } else {
                    tle = core::ptr::null_mut();
                }
            }
        } else {
            foreach!(j, tlist, {
                tle = lfirst(current_cell!(j)) as *mut TargetEntry;
                em = find_ec_member_matching_expr(ec, (*tle).expr, relids);
                if !em.is_null() {
                    /* found expr already in tlist */
                    pk_datatype = (*em).em_datatype;
                    break;
                }
                tle = core::ptr::null_mut();
            });
        }

        if tle.is_null() {
            /* No matching tlist item; look for a computable expression. */
            em = find_computable_ec_member(core::ptr::null_mut(), ec, tlist, relids, false);
            if em.is_null() {
                elog!(ERROR, "could not find pathkey item to sort");
            }
            pk_datatype = (*em).em_datatype;

            /* Do we need to insert a Result node? */
            if !adjust_tlist_in_place && !is_projection_capable_plan(lefttree) {
                /* copy needed so we don't modify input's tlist below */
                tlist = copyObject(tlist);
                lefttree = inject_projection_plan(lefttree, tlist, (*lefttree).parallel_safe);
            }

            /* Don't bother testing is_projection_capable_plan again */
            adjust_tlist_in_place = true;

            /* Add resjunk entry to input's tlist */
            tle = makeTargetEntry(copyObject((*em).em_expr), (list_length(tlist) + 1) as AttrNumber, core::ptr::null_mut(), true);
            tlist = lappend(tlist, tle as *mut c_void);
            (*lefttree).targetlist = tlist; /* just in case NIL before */
        }

        sortop = get_opfamily_member_for_cmptype((*pathkey).pk_opfamily, pk_datatype, pk_datatype, (*pathkey).pk_cmptype);
        if !OidIsValid(sortop) {
            /* should not happen */
            elog!(ERROR, "missing operator {}({},{}) in opfamily {}", (*pathkey).pk_cmptype, pk_datatype, pk_datatype, (*pathkey).pk_opfamily);
        }

        /* Add the column to the sort arrays */
        *sortColIdx.add(numsortkeys as usize) = (*tle).resno;
        *sortOperators.add(numsortkeys as usize) = sortop;
        *collations.add(numsortkeys as usize) = (*ec).ec_collation;
        *nullsFirst.add(numsortkeys as usize) = (*pathkey).pk_nulls_first;
        numsortkeys += 1;
    });

    /* Return results */
    *p_numsortkeys = numsortkeys;
    *p_sortColIdx = sortColIdx;
    *p_sortOperators = sortOperators;
    *p_collations = collations;
    *p_nullsFirst = nullsFirst;

    lefttree
}

// make_sort_from_pathkeys
unsafe fn make_sort_from_pathkeys(mut lefttree: *mut Plan, pathkeys: *mut List, relids: Relids) -> *mut Sort {
    let mut numsortkeys: c_int = 0;
    let mut sortColIdx: *mut AttrNumber = core::ptr::null_mut();
    let mut sortOperators: *mut Oid = core::ptr::null_mut();
    let mut collations: *mut Oid = core::ptr::null_mut();
    let mut nullsFirst: *mut bool = core::ptr::null_mut();

    /* Compute sort column info, and adjust lefttree as needed */
    lefttree = prepare_sort_from_pathkeys(lefttree, pathkeys, relids, core::ptr::null(), false, &raw mut numsortkeys, &raw mut sortColIdx, &raw mut sortOperators, &raw mut collations, &raw mut nullsFirst);

    /* Now build the Sort node */
    make_sort(lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst)
}

// make_incrementalsort_from_pathkeys
unsafe fn make_incrementalsort_from_pathkeys(mut lefttree: *mut Plan, pathkeys: *mut List, relids: Relids, nPresortedCols: c_int) -> *mut IncrementalSort {
    let mut numsortkeys: c_int = 0;
    let mut sortColIdx: *mut AttrNumber = core::ptr::null_mut();
    let mut sortOperators: *mut Oid = core::ptr::null_mut();
    let mut collations: *mut Oid = core::ptr::null_mut();
    let mut nullsFirst: *mut bool = core::ptr::null_mut();

    /* Compute sort column info, and adjust lefttree as needed */
    lefttree = prepare_sort_from_pathkeys(lefttree, pathkeys, relids, core::ptr::null(), false, &raw mut numsortkeys, &raw mut sortColIdx, &raw mut sortOperators, &raw mut collations, &raw mut nullsFirst);

    /* Now build the Sort node */
    make_incrementalsort(lefttree, numsortkeys, nPresortedCols, sortColIdx, sortOperators, collations, nullsFirst)
}

// make_sort_from_sortclauses
pub unsafe fn make_sort_from_sortclauses(sortcls: *mut List, lefttree: *mut Plan) -> *mut Sort {
    let sub_tlist = (*lefttree).targetlist;
    let mut l: *mut ListCell;
    let mut numsortkeys: c_int;
    let sortColIdx: *mut AttrNumber;
    let sortOperators: *mut Oid;
    let collations: *mut Oid;
    let nullsFirst: *mut bool;

    /* Convert list-ish representation to arrays wanted by executor */
    numsortkeys = list_length(sortcls);
    sortColIdx = palloc(numsortkeys as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    sortOperators = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    collations = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    nullsFirst = palloc(numsortkeys as usize * core::mem::size_of::<bool>()) as *mut bool;

    numsortkeys = 0;
    foreach!(l, sortcls, {
        let sortcl = lfirst(current_cell!(l)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sortcl, sub_tlist);

        *sortColIdx.add(numsortkeys as usize) = (*tle).resno;
        *sortOperators.add(numsortkeys as usize) = (*sortcl).sortop;
        *collations.add(numsortkeys as usize) = exprCollation((*tle).expr as *mut Node);
        *nullsFirst.add(numsortkeys as usize) = (*sortcl).nulls_first;
        numsortkeys += 1;
    });

    make_sort(lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst)
}

// make_sort_from_groupcols
unsafe fn make_sort_from_groupcols(groupcls: *mut List, grpColIdx: *mut AttrNumber, lefttree: *mut Plan) -> *mut Sort {
    let sub_tlist = (*lefttree).targetlist;
    let mut l: *mut ListCell;
    let mut numsortkeys: c_int;
    let sortColIdx: *mut AttrNumber;
    let sortOperators: *mut Oid;
    let collations: *mut Oid;
    let nullsFirst: *mut bool;

    /* Convert list-ish representation to arrays wanted by executor */
    numsortkeys = list_length(groupcls);
    sortColIdx = palloc(numsortkeys as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    sortOperators = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    collations = palloc(numsortkeys as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    nullsFirst = palloc(numsortkeys as usize * core::mem::size_of::<bool>()) as *mut bool;

    numsortkeys = 0;
    foreach!(l, groupcls, {
        let grpcl = lfirst(current_cell!(l)) as *mut SortGroupClause;
        let tle = get_tle_by_resno(sub_tlist, *grpColIdx.add(numsortkeys as usize));

        if tle.is_null() {
            elog!(ERROR, "could not retrieve tle for sort-from-groupcols");
        }

        *sortColIdx.add(numsortkeys as usize) = (*tle).resno;
        *sortOperators.add(numsortkeys as usize) = (*grpcl).sortop;
        *collations.add(numsortkeys as usize) = exprCollation((*tle).expr as *mut Node);
        *nullsFirst.add(numsortkeys as usize) = (*grpcl).nulls_first;
        numsortkeys += 1;
    });

    make_sort(lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst)
}

unsafe fn make_material(lefttree: *mut Plan) -> *mut Material {
    let node = makeNode!(Material, T_Material);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    node
}

// materialize_finished_plan: stick a Material node atop a completed plan
pub unsafe fn materialize_finished_plan(subplan: *mut Plan) -> *mut Plan {
    let matplan: *mut Plan;
    let mut matpath: Path = core::mem::zeroed(); /* dummy for result of cost_material */
    let mut initplan_cost: Cost = 0.0;
    let mut unsafe_initplans: bool = false;

    matplan = make_material(subplan) as *mut Plan;

    (*matplan).initPlan = (*subplan).initPlan;
    (*subplan).initPlan = NIL;

    /* Move the initplans' cost delta, as well */
    SS_compute_initplan_cost((*matplan).initPlan, &raw mut initplan_cost, &raw mut unsafe_initplans);
    (*subplan).startup_cost -= initplan_cost;
    (*subplan).total_cost -= initplan_cost;

    /* Set cost data */
    cost_material(&raw mut matpath, (*subplan).disabled_nodes, (*subplan).startup_cost, (*subplan).total_cost, (*subplan).plan_rows, (*subplan).plan_width);
    (*matplan).disabled_nodes = (*subplan).disabled_nodes;
    (*matplan).startup_cost = matpath.startup_cost + initplan_cost;
    (*matplan).total_cost = matpath.total_cost + initplan_cost;
    (*matplan).plan_rows = (*subplan).plan_rows;
    (*matplan).plan_width = (*subplan).plan_width;
    (*matplan).parallel_aware = false;
    (*matplan).parallel_safe = (*subplan).parallel_safe;

    matplan
}

unsafe fn make_memoize(lefttree: *mut Plan, hashoperators: *mut Oid, collations: *mut Oid, param_exprs: *mut List, singlerow: bool, binary_mode: bool, est_entries: uint32, keyparamids: *mut Bitmapset) -> *mut Memoize {
    let node = makeNode!(Memoize, T_Memoize);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    (*node).numKeys = list_length(param_exprs);
    (*node).hashOperators = hashoperators;
    (*node).collations = collations;
    (*node).param_exprs = param_exprs;
    (*node).singlerow = singlerow;
    (*node).binary_mode = binary_mode;
    (*node).est_entries = est_entries;
    (*node).keyparamids = keyparamids;

    node
}

pub unsafe fn make_agg(tlist: *mut List, qual: *mut List, aggstrategy: AggStrategy, aggsplit: AggSplit, numGroupCols: c_int, grpColIdx: *mut AttrNumber, grpOperators: *mut Oid, grpCollations: *mut Oid, groupingSets: *mut List, chain: *mut List, dNumGroups: f64, transitionSpace: Size, lefttree: *mut Plan) -> *mut Agg {
    let node = makeNode!(Agg, T_Agg);
    let plan = &raw mut (*node).plan;
    let numGroups: c_long;

    /* Reduce to long, but 'ware overflow! */
    numGroups = clamp_cardinality_to_long(dNumGroups);

    (*node).aggstrategy = aggstrategy;
    (*node).aggsplit = aggsplit;
    (*node).numCols = numGroupCols;
    (*node).grpColIdx = grpColIdx;
    (*node).grpOperators = grpOperators;
    (*node).grpCollations = grpCollations;
    (*node).numGroups = numGroups;
    (*node).transitionSpace = transitionSpace as u64;
    (*node).aggParams = core::ptr::null_mut(); /* SS_finalize_plan() will fill this */
    (*node).groupingSets = groupingSets;
    (*node).chain = chain;

    (*plan).qual = qual;
    (*plan).targetlist = tlist;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    node
}

unsafe fn make_windowagg(tlist: *mut List, wc: *mut WindowClause, partNumCols: c_int, partColIdx: *mut AttrNumber, partOperators: *mut Oid, partCollations: *mut Oid, ordNumCols: c_int, ordColIdx: *mut AttrNumber, ordOperators: *mut Oid, ordCollations: *mut Oid, runCondition: *mut List, qual: *mut List, topWindow: bool, lefttree: *mut Plan) -> *mut WindowAgg {
    let node = makeNode!(WindowAgg, T_WindowAgg);
    let plan = &raw mut (*node).plan;

    (*node).winname = (*wc).name;
    (*node).winref = (*wc).winref;
    (*node).partNumCols = partNumCols;
    (*node).partColIdx = partColIdx;
    (*node).partOperators = partOperators;
    (*node).partCollations = partCollations;
    (*node).ordNumCols = ordNumCols;
    (*node).ordColIdx = ordColIdx;
    (*node).ordOperators = ordOperators;
    (*node).ordCollations = ordCollations;
    (*node).frameOptions = (*wc).frameOptions;
    (*node).startOffset = (*wc).startOffset;
    (*node).endOffset = (*wc).endOffset;
    (*node).runCondition = runCondition;
    /* a duplicate of the above for EXPLAIN */
    (*node).runConditionOrig = runCondition;
    (*node).startInRangeFunc = (*wc).startInRangeFunc;
    (*node).endInRangeFunc = (*wc).endInRangeFunc;
    (*node).inRangeColl = (*wc).inRangeColl;
    (*node).inRangeAsc = (*wc).inRangeAsc;
    (*node).inRangeNullsFirst = (*wc).inRangeNullsFirst;
    (*node).topWindow = topWindow;

    (*plan).targetlist = tlist;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();
    (*plan).qual = qual;

    node
}

unsafe fn make_group(tlist: *mut List, qual: *mut List, numGroupCols: c_int, grpColIdx: *mut AttrNumber, grpOperators: *mut Oid, grpCollations: *mut Oid, lefttree: *mut Plan) -> *mut Group {
    let node = makeNode!(Group, T_Group);
    let plan = &raw mut (*node).plan;

    (*node).numCols = numGroupCols;
    (*node).grpColIdx = grpColIdx;
    (*node).grpOperators = grpOperators;
    (*node).grpCollations = grpCollations;

    (*plan).qual = qual;
    (*plan).targetlist = tlist;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    node
}

// make_unique_from_sortclauses
unsafe fn make_unique_from_sortclauses(lefttree: *mut Plan, distinctList: *mut List) -> *mut Unique {
    let node = makeNode!(Unique, T_Unique);
    let plan = &raw mut (*node).plan;
    let numCols = list_length(distinctList);
    let mut keyno: c_int = 0;
    let uniqColIdx: *mut AttrNumber;
    let uniqOperators: *mut Oid;
    let uniqCollations: *mut Oid;
    let mut slitem: *mut ListCell;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    Assert!(numCols > 0);
    uniqColIdx = palloc(core::mem::size_of::<AttrNumber>() * numCols as usize) as *mut AttrNumber;
    uniqOperators = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;
    uniqCollations = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;

    foreach!(slitem, distinctList, {
        let sortcl = lfirst(current_cell!(slitem)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sortcl, (*plan).targetlist);

        *uniqColIdx.add(keyno as usize) = (*tle).resno;
        *uniqOperators.add(keyno as usize) = (*sortcl).eqop;
        *uniqCollations.add(keyno as usize) = exprCollation((*tle).expr as *mut Node);
        Assert!(OidIsValid(*uniqOperators.add(keyno as usize)));
        keyno += 1;
    });

    (*node).numCols = numCols;
    (*node).uniqColIdx = uniqColIdx;
    (*node).uniqOperators = uniqOperators;
    (*node).uniqCollations = uniqCollations;

    node
}

// make_unique_from_pathkeys
unsafe fn make_unique_from_pathkeys(lefttree: *mut Plan, pathkeys: *mut List, numCols: c_int) -> *mut Unique {
    let node = makeNode!(Unique, T_Unique);
    let plan = &raw mut (*node).plan;
    let mut keyno: c_int = 0;
    let uniqColIdx: *mut AttrNumber;
    let uniqOperators: *mut Oid;
    let uniqCollations: *mut Oid;
    let mut lc: *mut ListCell;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    Assert!(numCols >= 0 && numCols <= list_length(pathkeys));
    uniqColIdx = palloc(core::mem::size_of::<AttrNumber>() * numCols as usize) as *mut AttrNumber;
    uniqOperators = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;
    uniqCollations = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;

    foreach!(lc, pathkeys, {
        let pathkey = lfirst(current_cell!(lc)) as *mut PathKey;
        let ec = (*pathkey).pk_eclass;
        let mut em: *mut EquivalenceMember;
        let mut tle: *mut TargetEntry = core::ptr::null_mut();
        let mut pk_datatype: Oid = InvalidOid;
        let eqop: Oid;
        let mut j: *mut ListCell;

        /* Ignore pathkeys beyond the specified number of columns */
        if keyno >= numCols {
            break;
        }

        if (*ec).ec_has_volatile {
            if (*ec).ec_sortref == 0 {
                /* can't happen */
                elog!(ERROR, "volatile EquivalenceClass has no sortref");
            }
            tle = get_sortgroupref_tle((*ec).ec_sortref, (*plan).targetlist);
            Assert!(!tle.is_null());
            Assert!(list_length((*ec).ec_members) == 1);
            pk_datatype = (*(linitial((*ec).ec_members) as *mut EquivalenceMember)).em_datatype;
        } else {
            foreach!(j, (*plan).targetlist, {
                tle = lfirst(current_cell!(j)) as *mut TargetEntry;
                em = find_ec_member_matching_expr(ec, (*tle).expr, core::ptr::null_mut());
                if !em.is_null() {
                    /* found expr already in tlist */
                    pk_datatype = (*em).em_datatype;
                    break;
                }
                tle = core::ptr::null_mut();
            });
        }

        if tle.is_null() {
            elog!(ERROR, "could not find pathkey item to sort");
        }

        eqop = get_opfamily_member_for_cmptype((*pathkey).pk_opfamily, pk_datatype, pk_datatype, COMPARE_EQ);
        if !OidIsValid(eqop) {
            /* should not happen */
            elog!(ERROR, "missing operator {}({},{}) in opfamily {}", COMPARE_EQ, pk_datatype, pk_datatype, (*pathkey).pk_opfamily);
        }

        *uniqColIdx.add(keyno as usize) = (*tle).resno;
        *uniqOperators.add(keyno as usize) = eqop;
        *uniqCollations.add(keyno as usize) = (*ec).ec_collation;

        keyno += 1;
    });

    (*node).numCols = numCols;
    (*node).uniqColIdx = uniqColIdx;
    (*node).uniqOperators = uniqOperators;
    (*node).uniqCollations = uniqCollations;

    node
}

unsafe fn make_gather(qptlist: *mut List, qpqual: *mut List, nworkers: c_int, rescan_param: c_int, single_copy: bool, subplan: *mut Plan) -> *mut Gather {
    let node = makeNode!(Gather, T_Gather);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = qptlist;
    (*plan).qual = qpqual;
    (*plan).lefttree = subplan;
    (*plan).righttree = core::ptr::null_mut();
    (*node).num_workers = nworkers;
    (*node).rescan_param = rescan_param;
    (*node).single_copy = single_copy;
    (*node).invisible = false;
    (*node).initParam = core::ptr::null_mut();

    node
}

// make_setop
unsafe fn make_setop(cmd: SetOpCmd, strategy: SetOpStrategy, tlist: *mut List, lefttree: *mut Plan, righttree: *mut Plan, groupList: *mut List, numGroups: c_long) -> *mut SetOp {
    let node = makeNode!(SetOp, T_SetOp);
    let plan = &raw mut (*node).plan;
    let numCols = list_length(groupList);
    let mut keyno: c_int = 0;
    let cmpColIdx: *mut AttrNumber;
    let cmpOperators: *mut Oid;
    let cmpCollations: *mut Oid;
    let cmpNullsFirst: *mut bool;
    let mut slitem: *mut ListCell;

    (*plan).targetlist = tlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = righttree;

    cmpColIdx = palloc(core::mem::size_of::<AttrNumber>() * numCols as usize) as *mut AttrNumber;
    cmpOperators = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;
    cmpCollations = palloc(core::mem::size_of::<Oid>() * numCols as usize) as *mut Oid;
    cmpNullsFirst = palloc(core::mem::size_of::<bool>() * numCols as usize) as *mut bool;

    foreach!(slitem, groupList, {
        let sortcl = lfirst(current_cell!(slitem)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sortcl, (*plan).targetlist);

        *cmpColIdx.add(keyno as usize) = (*tle).resno;
        if strategy == SETOP_HASHED {
            *cmpOperators.add(keyno as usize) = (*sortcl).eqop;
        } else {
            *cmpOperators.add(keyno as usize) = (*sortcl).sortop;
        }
        Assert!(OidIsValid(*cmpOperators.add(keyno as usize)));
        *cmpCollations.add(keyno as usize) = exprCollation((*tle).expr as *mut Node);
        *cmpNullsFirst.add(keyno as usize) = (*sortcl).nulls_first;
        keyno += 1;
    });

    (*node).cmd = cmd;
    (*node).strategy = strategy;
    (*node).numCols = numCols;
    (*node).cmpColIdx = cmpColIdx;
    (*node).cmpOperators = cmpOperators;
    (*node).cmpCollations = cmpCollations;
    (*node).cmpNullsFirst = cmpNullsFirst;
    (*node).numGroups = numGroups;

    node
}

// make_lockrows
unsafe fn make_lockrows(lefttree: *mut Plan, rowMarks: *mut List, epqParam: c_int) -> *mut LockRows {
    let node = makeNode!(LockRows, T_LockRows);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    (*node).rowMarks = rowMarks;
    (*node).epqParam = epqParam;

    node
}

// make_limit
pub unsafe fn make_limit(lefttree: *mut Plan, limitOffset: *mut Node, limitCount: *mut Node, limitOption: LimitOption, uniqNumCols: c_int, uniqColIdx: *mut AttrNumber, uniqOperators: *mut Oid, uniqCollations: *mut Oid) -> *mut Limit {
    let node = makeNode!(Limit, T_Limit);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = (*lefttree).targetlist;
    (*plan).qual = NIL;
    (*plan).lefttree = lefttree;
    (*plan).righttree = core::ptr::null_mut();

    (*node).limitOffset = limitOffset;
    (*node).limitCount = limitCount;
    (*node).limitOption = limitOption;
    (*node).uniqNumCols = uniqNumCols;
    (*node).uniqColIdx = uniqColIdx;
    (*node).uniqOperators = uniqOperators;
    (*node).uniqCollations = uniqCollations;

    node
}

// make_result
unsafe fn make_result(tlist: *mut List, resconstantqual: *mut Node, subplan: *mut Plan) -> *mut Result {
    let node = makeNode!(Result, T_Result);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = tlist;
    (*plan).qual = NIL;
    (*plan).lefttree = subplan;
    (*plan).righttree = core::ptr::null_mut();
    (*node).resconstantqual = resconstantqual;

    node
}

// make_project_set
unsafe fn make_project_set(tlist: *mut List, subplan: *mut Plan) -> *mut ProjectSet {
    let node = makeNode!(ProjectSet, T_ProjectSet);
    let plan = &raw mut (*node).plan;

    (*plan).targetlist = tlist;
    (*plan).qual = NIL;
    (*plan).lefttree = subplan;
    (*plan).righttree = core::ptr::null_mut();

    node
}

// make_modifytable
unsafe fn make_modifytable(root: *mut PlannerInfo, subplan: *mut Plan, operation: CmdType, canSetTag: bool, nominalRelation: Index, rootRelation: Index, partColsUpdated: bool, resultRelations: *mut List, updateColnosLists: *mut List, withCheckOptionLists: *mut List, returningLists: *mut List, rowMarks: *mut List, onconflict: *mut OnConflictExpr, mergeActionLists: *mut List, mergeJoinConditions: *mut List, epqParam: c_int) -> *mut ModifyTable {
    let node = makeNode!(ModifyTable, T_ModifyTable);
    let mut returning_old_or_new = false;
    let mut returning_old_or_new_valid = false;
    let mut transition_tables = false;
    let mut transition_tables_valid = false;
    let mut fdw_private_list: *mut List;
    let mut direct_modify_plans: *mut Bitmapset;
    let mut lc: *mut ListCell;
    let mut i: c_int;

    Assert!(operation == CMD_MERGE
        || (if operation == CMD_UPDATE {
            list_length(resultRelations) == list_length(updateColnosLists)
        } else {
            updateColnosLists == NIL
        }));
    Assert!(withCheckOptionLists == NIL || list_length(resultRelations) == list_length(withCheckOptionLists));
    Assert!(returningLists == NIL || list_length(resultRelations) == list_length(returningLists));

    (*node).plan.lefttree = subplan;
    (*node).plan.righttree = core::ptr::null_mut();
    (*node).plan.qual = NIL;
    /* setrefs.c will fill in the targetlist, if needed */
    (*node).plan.targetlist = NIL;

    (*node).operation = operation;
    (*node).canSetTag = canSetTag;
    (*node).nominalRelation = nominalRelation;
    (*node).rootRelation = rootRelation;
    (*node).partColsUpdated = partColsUpdated;
    (*node).resultRelations = resultRelations;
    if onconflict.is_null() {
        (*node).onConflictAction = ONCONFLICT_NONE;
        (*node).onConflictSet = NIL;
        (*node).onConflictCols = NIL;
        (*node).onConflictWhere = core::ptr::null_mut();
        (*node).arbiterIndexes = NIL;
        (*node).exclRelRTI = 0;
        (*node).exclRelTlist = NIL;
    } else {
        (*node).onConflictAction = (*onconflict).action;

        (*node).onConflictSet = (*onconflict).onConflictSet;
        (*node).onConflictCols = extract_update_targetlist_colnos((*node).onConflictSet);
        (*node).onConflictWhere = (*onconflict).onConflictWhere;

        (*node).arbiterIndexes = infer_arbiter_indexes(root);

        (*node).exclRelRTI = (*onconflict).exclRelIndex as u32;
        (*node).exclRelTlist = (*onconflict).exclRelTlist;
    }
    (*node).updateColnosLists = updateColnosLists;
    (*node).withCheckOptionLists = withCheckOptionLists;
    (*node).returningOldAlias = (*(*root).parse).returningOldAlias;
    (*node).returningNewAlias = (*(*root).parse).returningNewAlias;
    (*node).returningLists = returningLists;
    (*node).rowMarks = rowMarks;
    (*node).mergeActionLists = mergeActionLists;
    (*node).mergeJoinConditions = mergeJoinConditions;
    (*node).epqParam = epqParam;

    fdw_private_list = NIL;
    direct_modify_plans = core::ptr::null_mut();
    i = 0;
    foreach!(lc, resultRelations, {
        let rti = lfirst_int(current_cell!(lc)) as Index;
        let mut fdwroutine: *mut c_void;
        let fdw_private: *mut List;
        let mut direct_modify: bool;

        if (rti as c_int) < (*root).simple_rel_array_size && !(*(*root).simple_rel_array.add(rti as usize)).is_null() {
            let resultRel = *(*root).simple_rel_array.add(rti as usize);

            fdwroutine = (*resultRel).fdwroutine as *mut c_void;
        } else {
            let rte = planner_rt_fetch(rti, root);

            if (*rte).rtekind == RTE_RELATION && (*rte).relkind == RELKIND_FOREIGN_TABLE {
                /* Check if the access to foreign tables is restricted */
                if (restrict_nonsystem_relation_kind & RESTRICT_RELKIND_FOREIGN_TABLE) != 0 {
                    /* there must not be built-in foreign tables */
                    Assert!((*rte).relid >= FirstNormalObjectId);
                    ereport!(ERROR, errmsg!("access to non-system foreign table is restricted"));
                }

                fdwroutine = GetFdwRoutineByRelId((*rte).relid);
            } else {
                fdwroutine = core::ptr::null_mut();
            }
        }

        if operation == CMD_MERGE && !fdwroutine.is_null() {
            let rte = planner_rt_fetch(rti, root);

            ereport!(ERROR, errmsg!("cannot execute MERGE on relation \"{}\"", CStr::from_ptr(get_rel_name((*rte).relid)).to_string_lossy()));
        }

        direct_modify = false;
        if !fdwroutine.is_null()
            && fdw_has_PlanDirectModify(fdwroutine)
            && withCheckOptionLists == NIL
            && !has_row_triggers(root, rti, operation)
            && !has_stored_generated_columns(root, rti)
        {
            if !returning_old_or_new_valid {
                returning_old_or_new = contain_vars_returning_old_or_new((*(*root).parse).returningList as *mut Node);
                returning_old_or_new_valid = true;
            }
            if !returning_old_or_new {
                if !transition_tables_valid {
                    transition_tables = has_transition_tables(root, nominalRelation, operation);
                    transition_tables_valid = true;
                }
                if !transition_tables {
                    direct_modify = fdw_PlanDirectModify(fdwroutine, root, node, rti, i);
                }
            }
        }
        if direct_modify {
            direct_modify_plans = bms_add_member(direct_modify_plans, i);
        }

        if !direct_modify && !fdwroutine.is_null() && fdw_has_PlanForeignModify(fdwroutine) {
            fdw_private = fdw_PlanForeignModify(fdwroutine, root, node, rti, i);
        } else {
            fdw_private = NIL;
        }
        fdw_private_list = lappend(fdw_private_list, fdw_private as *mut c_void);
        i += 1;
    });
    (*node).fdwPrivLists = fdw_private_list;
    (*node).fdwDirectModifyPlans = direct_modify_plans;

    node
}

// is_projection_capable_path
pub unsafe fn is_projection_capable_path(path: *mut Path) -> bool {
    /* Most plan types can project, so just list the ones that can't */
    match (*path).pathtype {
        NodeTag::T_Hash
        | NodeTag::T_Material
        | NodeTag::T_Memoize
        | NodeTag::T_Sort
        | NodeTag::T_IncrementalSort
        | NodeTag::T_Unique
        | NodeTag::T_SetOp
        | NodeTag::T_LockRows
        | NodeTag::T_Limit
        | NodeTag::T_ModifyTable
        | NodeTag::T_MergeAppend
        | NodeTag::T_RecursiveUnion => false,
        NodeTag::T_CustomScan => {
            if (*castNode!(CustomPath, T_CustomPath, path)).flags & (CUSTOMPATH_SUPPORT_PROJECTION as u32) != 0 {
                return true;
            }
            false
        }
        NodeTag::T_Append => IS_DUMMY_APPEND(path),
        NodeTag::T_ProjectSet => false,
        _ => true,
    }
}

// is_projection_capable_plan
pub unsafe fn is_projection_capable_plan(plan: *mut Plan) -> bool {
    /* Most plan types can project, so just list the ones that can't */
    match nodeTag(plan as *mut Node) {
        NodeTag::T_Hash
        | NodeTag::T_Material
        | NodeTag::T_Memoize
        | NodeTag::T_Sort
        | NodeTag::T_Unique
        | NodeTag::T_SetOp
        | NodeTag::T_LockRows
        | NodeTag::T_Limit
        | NodeTag::T_ModifyTable
        | NodeTag::T_Append
        | NodeTag::T_MergeAppend
        | NodeTag::T_RecursiveUnion => false,
        NodeTag::T_CustomScan => {
            if (*(plan as *mut CustomScan)).flags & (CUSTOMPATH_SUPPORT_PROJECTION as u32) != 0 {
                return true;
            }
            false
        }
        NodeTag::T_ProjectSet => false,
        _ => true,
    }
}

// ===========================================================================
// Local C-library / FDW callback dispatch stubs.
// TODO(pg-port): replace with real bindings once those modules are ported.
// ===========================================================================
extern "C" {
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}
unsafe fn libc_memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int { memcmp(s1, s2, n) }
unsafe fn libc_strcmp(s1: *const c_char, s2: *const c_char) -> c_int { strcmp(s1, s2) }

// TODO(pg-port): real equal lives in nodes/equalfuncs.rs
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool { crate::nodes::equalfuncs::equal(a, b) }

// TODO(pg-port): FdwRoutine callback dispatch (foreign/fdwapi.h).  These wrap
// the function-pointer fields of FdwRoutine, which is not yet ported.
unsafe fn fdw_GetForeignPlan(fdwroutine: *mut c_void, root: *mut PlannerInfo, rel: *mut RelOptInfo, rel_oid: Oid, best_path: *mut ForeignPath, tlist: *mut List, scan_clauses: *mut List, outer_plan: *mut Plan) -> *mut ForeignScan { unimplemented!() }
unsafe fn fdw_has_PlanDirectModify(fdwroutine: *mut c_void) -> bool { unimplemented!() }
unsafe fn fdw_has_PlanForeignModify(fdwroutine: *mut c_void) -> bool { unimplemented!() }
unsafe fn fdw_PlanDirectModify(fdwroutine: *mut c_void, root: *mut PlannerInfo, node: *mut ModifyTable, rti: Index, subplan_index: c_int) -> bool { unimplemented!() }
unsafe fn fdw_PlanForeignModify(fdwroutine: *mut c_void, root: *mut PlannerInfo, node: *mut ModifyTable, rti: Index, subplan_index: c_int) -> *mut List { unimplemented!() }

// TODO(pg-port): CustomPathMethods callback dispatch (nodes/extensible.h).
unsafe fn custom_PlanCustomPath(methods: *const c_void, root: *mut PlannerInfo, rel: *mut RelOptInfo, best_path: *mut CustomPath, tlist: *mut List, scan_clauses: *mut List, custom_plans: *mut List) -> *mut Plan { unimplemented!() }

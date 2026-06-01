/*-------------------------------------------------------------------------
 *
 * partprune.c
 *		Support for partition pruning during query planning and execution
 *
 * This module implements partition pruning using the information contained in
 * a table's partition descriptor, query clauses, and run-time parameters.
 *
 * During planning, clauses that can be matched to the table's partition key
 * are turned into a set of "pruning steps", which are then executed to
 * identify a set of partitions (as indexes in the RelOptInfo->part_rels
 * array) that satisfy the constraints in the step.  Partitions not in the set
 * are said to have been pruned.
 *
 * A base pruning step may involve expressions whose values are only known
 * during execution, such as Params, in which case pruning cannot occur
 * entirely during planning.  In that case, such steps are included alongside
 * the plan, so that they can be used by the executor for further pruning.
 *
 * There are two kinds of pruning steps.  A "base" pruning step represents
 * tests on partition key column(s), typically comparisons to expressions.
 * A "combine" pruning step represents a Boolean connector (AND/OR), and
 * combines the outputs of some previous steps using the appropriate
 * combination method.
 *
 * See gen_partprune_steps_internal() for more details on step generation.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/partitioning/partprune.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use crate::prelude::*;
use std::ffi::{c_char, c_int, c_void};
use std::mem::size_of;
use std::ptr;

use crate::nodes::pg_list::{
    List, ListCell, NIL, lfirst, lfirst_int, lfirst_oid, lappend, lappend_int, lappend_oid,
    list_length, list_free, list_head, lnext, llast, linitial,
    list_nth, list_copy, list_concat,
};
use crate::{list_make1, list_make2};
use crate::nodes::bitmapset::{
    Bitmapset, bms_add_member, bms_add_members, bms_copy, bms_equal, bms_free,
    bms_is_empty, bms_is_member, bms_next_member, bms_num_members, bms_add_range,
    bms_del_member, bms_del_members, bms_int_members, bms_join, bms_make_singleton,
};
use crate::nodes::pathnodes::{
    RelOptInfo, PlannerInfo, AppendRelInfo, PartitionScheme, PartitionSchemeData,
    IS_PARTITIONED_REL,
};
use crate::nodes::primnodes::{
    Expr, Const, Var, NullTest, NullTestType, NullTestType::*, BoolExpr, BoolExprType,
    ScalarArrayOpExpr, ArrayExpr, FuncExpr, CoercionForm, RelabelType,
    OpExpr, BooleanTest, BoolTestType, BoolTestType::*, Param, ParamKind, ParamKind::*,
};
use crate::nodes::nodes::{Node, NodeTag, NodeTag::*};
use crate::nodes::plannodes::{
    PartitionPruneInfo, PartitionedRelPruneInfo, PartitionPruneStep,
    PartitionPruneStepOp, PartitionPruneStepCombine,
    PartitionPruneCombineOp, PartitionPruneCombineOp::*,
};
use crate::utils::rel::{Relation, RelationGetRelid};
use crate::utils::fmgr::{FmgrInfo, fmgr_info_copy, fmgr_info_cxt};
use crate::utils::palloc::{palloc, palloc0, pfree};
use crate::utils::mmgr::mcxt::CurrentMemoryContext;
use crate::access::stratnum::{
    StrategyNumber, InvalidStrategy,
    BTLessStrategyNumber, BTLessEqualStrategyNumber, BTEqualStrategyNumber,
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTMaxStrategyNumber,
    HTEqualStrategyNumber, HTMaxStrategyNumber,
};
use crate::nodes::parsenodes::{
    PartitionStrategy, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST,
    PARTITION_STRATEGY_RANGE,
};
use crate::pg_config_manual::PARTITION_MAX_KEYS;
use crate::postgres_ext::InvalidOid;
use crate::catalog::pg_type_d::BOOLOID;
use crate::nodes::pathnodes::PartitionBoundInfoData;
use crate::partitioning::partbounds::{
    PartitionBoundInfo,
    partition_bound_has_default, partition_bound_accepts_nulls,
    partition_list_bsearch, partition_range_datum_bsearch,
    partition_rbound_datum_cmp, compute_partition_hash_value,
};
use crate::executor::execPartition::{PartitionPruneContext, PruneCxtStateIdx};
use crate::{makeNode, castNode, IsA, foreach, current_cell};

/* TODO(pg-port): find_base_rel - optimizer/util/relnode.c */
unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
    unimplemented!("TODO(pg-port): find_base_rel - optimizer/util/relnode.c")
}

/* TODO(pg-port): find_appinfos_by_relids - optimizer/util/inherit.c */
unsafe fn find_appinfos_by_relids(
    root: *mut PlannerInfo,
    relids: *mut Bitmapset,
    nappinfos: *mut c_int,
) -> *mut *mut AppendRelInfo {
    unimplemented!("TODO(pg-port): find_appinfos_by_relids - optimizer/util/inherit.c")
}

/* TODO(pg-port): adjust_appendrel_attrs - optimizer/util/inherit.c */
unsafe fn adjust_appendrel_attrs(
    root: *mut PlannerInfo,
    node: *mut Node,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) -> *mut Node {
    unimplemented!("TODO(pg-port): adjust_appendrel_attrs - optimizer/util/inherit.c")
}

/* TODO(pg-port): adjust_appendrel_attrs_multilevel - optimizer/util/inherit.c */
unsafe fn adjust_appendrel_attrs_multilevel(
    root: *mut PlannerInfo,
    node: *mut Node,
    child_rel: *mut RelOptInfo,
    top_parent_rel: *mut RelOptInfo,
) -> *mut Node {
    unimplemented!("TODO(pg-port): adjust_appendrel_attrs_multilevel - optimizer/util/inherit.c")
}

/* TODO(pg-port): planner_rt_fetch macro - optimizer/optimizer.h */
unsafe fn planner_rt_fetch(
    rti: c_int,
    root: *mut PlannerInfo,
) -> *mut crate::nodes::parsenodes::RangeTblEntry {
    unimplemented!("TODO(pg-port): planner_rt_fetch - optimizer/optimizer.h")
}

/* TODO(pg-port): predicate_refuted_by - optimizer/util/predtest.c */
unsafe fn predicate_refuted_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    unimplemented!("TODO(pg-port): predicate_refuted_by - optimizer/util/predtest.c")
}

/* TODO(pg-port): contain_var_clause - optimizer/util/var.c */
unsafe fn contain_var_clause(node: *mut Node) -> bool {
    unimplemented!("TODO(pg-port): contain_var_clause - optimizer/util/var.c")
}

/* TODO(pg-port): contain_volatile_functions - optimizer/util/clauses.c */
unsafe fn contain_volatile_functions(node: *mut Node) -> bool {
    unimplemented!("TODO(pg-port): contain_volatile_functions - optimizer/util/clauses.c")
}

/* TODO(pg-port): expression_tree_walker - nodes/nodeFuncs.c */
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: unsafe fn(*mut Node, *mut *mut Bitmapset) -> bool,
    context: *mut *mut Bitmapset,
) -> bool {
    unimplemented!("TODO(pg-port): expression_tree_walker - nodes/nodeFuncs.c")
}

/* TODO(pg-port): op_in_opfamily - utils/cache/lsyscache.c */
unsafe fn op_in_opfamily(opno: Oid, partopfamily: Oid) -> bool {
    unimplemented!("TODO(pg-port): op_in_opfamily - utils/cache/lsyscache.c")
}

/* TODO(pg-port): op_strict - utils/cache/lsyscache.c */
unsafe fn op_strict(opno: Oid) -> bool {
    unimplemented!("TODO(pg-port): op_strict - utils/cache/lsyscache.c")
}

/* TODO(pg-port): op_volatile - utils/cache/lsyscache.c */
unsafe fn op_volatile(opno: Oid) -> c_char {
    unimplemented!("TODO(pg-port): op_volatile - utils/cache/lsyscache.c")
}

/* TODO(pg-port): get_op_opfamily_properties - utils/cache/lsyscache.c */
unsafe fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    ordering_op: bool,
    op_strategy: *mut StrategyNumber,
    op_lefttype: *mut Oid,
    op_righttype: *mut Oid,
) {
    unimplemented!("TODO(pg-port): get_op_opfamily_properties - utils/cache/lsyscache.c")
}

/* TODO(pg-port): get_opfamily_proc - utils/cache/lsyscache.c */
unsafe fn get_opfamily_proc(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    procnum: c_int,
) -> Oid {
    unimplemented!("TODO(pg-port): get_opfamily_proc - utils/cache/lsyscache.c")
}

/* TODO(pg-port): get_negator - utils/cache/lsyscache.c */
unsafe fn get_negator(opno: Oid) -> Oid {
    unimplemented!("TODO(pg-port): get_negator - utils/cache/lsyscache.c")
}

/* TODO(pg-port): get_commutator - utils/cache/lsyscache.c */
unsafe fn get_commutator(opno: Oid) -> Oid {
    unimplemented!("TODO(pg-port): get_commutator - utils/cache/lsyscache.c")
}

/* TODO(pg-port): get_leftop / get_rightop - nodes/nodeFuncs.c */
unsafe fn get_leftop(clause: *const Expr) -> *mut Node {
    unimplemented!("TODO(pg-port): get_leftop - nodes/nodeFuncs.c")
}
unsafe fn get_rightop(clause: *const Expr) -> *mut Node {
    unimplemented!("TODO(pg-port): get_rightop - nodes/nodeFuncs.c")
}

/* TODO(pg-port): make_opclause - nodes/makefuncs.c */
unsafe fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: *mut Expr,
    rightop: *mut Expr,
    opcollid: Oid,
    inputcollid: Oid,
) -> *mut Expr {
    unimplemented!("TODO(pg-port): make_opclause - nodes/makefuncs.c")
}

/* TODO(pg-port): makeBoolExpr - nodes/makefuncs.c */
unsafe fn makeBoolExpr(boolop: BoolExprType, args: *mut List, location: c_int) -> *mut Expr {
    unimplemented!("TODO(pg-port): makeBoolExpr - nodes/makefuncs.c")
}

/* TODO(pg-port): makeBoolConst - nodes/makefuncs.c */
unsafe fn makeBoolConst(value: bool, isnull: bool) -> *mut Node {
    unimplemented!("TODO(pg-port): makeBoolConst - nodes/makefuncs.c")
}

/* TODO(pg-port): makeConst - nodes/makefuncs.c */
unsafe fn makeConst(
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i16,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> *mut Const {
    unimplemented!("TODO(pg-port): makeConst - nodes/makefuncs.c")
}

/* TODO(pg-port): copyObject - nodes/copyfuncs.c */
unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    unimplemented!("TODO(pg-port): copyObject - nodes/copyfuncs.c")
}

/* TODO(pg-port): equal - nodes/equalfuncs.c */
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    unimplemented!("TODO(pg-port): equal - nodes/equalfuncs.c")
}

/* TODO(pg-port): negate_clause - optimizer/util/clauses.c */
unsafe fn negate_clause(node: *mut Node) -> *mut Node {
    unimplemented!("TODO(pg-port): negate_clause - optimizer/util/clauses.c")
}

/* TODO(pg-port): is_orclause / is_andclause / is_notclause - nodes/nodeFuncs.c */
unsafe fn is_orclause(clause: *const Expr) -> bool {
    unimplemented!("TODO(pg-port): is_orclause - nodes/nodeFuncs.c")
}
unsafe fn is_andclause(clause: *const Expr) -> bool {
    unimplemented!("TODO(pg-port): is_andclause - nodes/nodeFuncs.c")
}
unsafe fn is_notclause(clause: *const Expr) -> bool {
    unimplemented!("TODO(pg-port): is_notclause - nodes/nodeFuncs.c")
}
unsafe fn get_notclausearg(clause: *const Expr) -> *mut Expr {
    unimplemented!("TODO(pg-port): get_notclausearg - nodes/nodeFuncs.c")
}

/* TODO(pg-port): list_concat_copy - nodes/list.c */
unsafe fn list_concat_copy(list1: *mut List, list2: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): list_concat_copy - nodes/list.c")
}

/* TODO(pg-port): list_make1_oid - nodes/list.c */
unsafe fn list_make1_oid(x: Oid) -> *mut List {
    unimplemented!("TODO(pg-port): list_make1_oid - nodes/list.c")
}

/* TODO(pg-port): lsecond - nodes/list.c */
unsafe fn lsecond(list: *mut List) -> *mut c_void {
    unimplemented!("TODO(pg-port): lsecond - nodes/list.c")
}

/* TODO(pg-port): for_each_cell macro emulation - see below */
/* Emulated as a Rust for loop using lnext manually */

/* TODO(pg-port): IsBuiltinBooleanOpfamily - utils/cache/lsyscache.c */
unsafe fn IsBuiltinBooleanOpfamily(opfamily: Oid) -> bool {
    unimplemented!("TODO(pg-port): IsBuiltinBooleanOpfamily - utils/cache/lsyscache.c")
}

/* TODO(pg-port): enable_partition_pruning GUC - optimizer/cost.c */
static mut enable_partition_pruning: bool = true;

/* TODO(pg-port): DatumGetBool - postgres.h */
#[inline]
unsafe fn DatumGetBool(datum: Datum) -> bool {
    datum != 0
}

/* TODO(pg-port): check_stack_depth - port/misc.c */
unsafe fn check_stack_depth() {
    /* TODO(pg-port): real check in miscadmin.c */
}

/* TODO(pg-port): ExecEvalExprSwitchContext - executor/execExpr.c */
unsafe fn ExecEvalExprSwitchContext(
    state: *mut crate::nodes::execnodes::ExprState,
    econtext: *mut crate::nodes::execnodes::ExprContext,
    isNull: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): ExecEvalExprSwitchContext - executor/execExpr.c")
}

/* TODO(pg-port): ARR_ELEMTYPE - utils/array.h */
unsafe fn ARR_ELEMTYPE(arr: *mut ArrayType) -> Oid {
    unimplemented!("TODO(pg-port): ARR_ELEMTYPE - utils/array.h")
}

/* TODO(pg-port): DatumGetArrayTypeP - utils/array.h */
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    unimplemented!("TODO(pg-port): DatumGetArrayTypeP - utils/array.h")
}

/* TODO(pg-port): get_typlenbyvalalign - utils/cache/lsyscache.c */
unsafe fn get_typlenbyvalalign(
    typid: Oid,
    typlen: *mut i16,
    typbyval: *mut bool,
    typalign: *mut c_char,
) {
    unimplemented!("TODO(pg-port): get_typlenbyvalalign - utils/cache/lsyscache.c")
}

/* TODO(pg-port): deconstruct_array - utils/adt/arrayfuncs.c */
unsafe fn deconstruct_array(
    array: *mut ArrayType,
    elmtype: Oid,
    elmlen: i16,
    elmbyval: bool,
    elmalign: c_char,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    unimplemented!("TODO(pg-port): deconstruct_array - utils/adt/arrayfuncs.c")
}

/* Opaque ArrayType placeholder */
pub struct ArrayType;

/* TODO(pg-port): BTORDER_PROC constant - access/nbtree.h */
const BTORDER_PROC: c_int = 1;
/* TODO(pg-port): HASHEXTENDED_PROC constant - access/hash.h */
const HASHEXTENDED_PROC: c_int = 2;
/* TODO(pg-port): PROVOLATILE_IMMUTABLE constant - catalog/pg_proc.h */
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;
/* TODO(pg-port): BooleanEqualOperator - catalog/pg_operator.h */
const BooleanEqualOperator: Oid = 91;

/* PartCollMatchesExprColl macro */
macro_rules! PartCollMatchesExprColl {
    ($partcoll:expr, $exprcoll:expr) => {
        ($partcoll) == InvalidOid || ($partcoll) == ($exprcoll)
    };
}

/*
 * Information about a clause matched with a partition key.
 */
struct PartClauseInfo {
    keyno: c_int,        /* Partition key number (0 to partnatts - 1) */
    opno: Oid,           /* operator used to compare partkey to expr */
    op_is_ne: bool,      /* is clause's original operator <> ? */
    expr: *mut Expr,     /* expr the partition key is compared to */
    cmpfn: Oid,          /* Oid of function to compare 'expr' to the
                          * partition key */
    op_strategy: StrategyNumber, /* btree strategy identifying the operator */
}

/*
 * PartClauseMatchStatus
 *		Describes the result of match_clause_to_partition_key()
 */
#[derive(PartialEq, Eq)]
enum PartClauseMatchStatus {
    PARTCLAUSE_NOMATCH,
    PARTCLAUSE_MATCH_CLAUSE,
    PARTCLAUSE_MATCH_NULLNESS,
    PARTCLAUSE_MATCH_STEPS,
    PARTCLAUSE_MATCH_CONTRADICT,
    PARTCLAUSE_UNSUPPORTED,
}
use PartClauseMatchStatus::*;

/*
 * PartClauseTarget
 *		Identifies which qual clauses we can use for generating pruning steps
 */
#[derive(PartialEq, Eq, Copy, Clone)]
enum PartClauseTarget {
    PARTTARGET_PLANNER, /* want to prune during planning */
    PARTTARGET_INITIAL, /* want to prune during executor startup */
    PARTTARGET_EXEC,    /* want to prune during each plan node scan */
}
use PartClauseTarget::*;

/*
 * GeneratePruningStepsContext
 *		Information about the current state of generation of "pruning steps"
 *		for a given set of clauses
 *
 * gen_partprune_steps() initializes and returns an instance of this struct.
 *
 * Note that has_mutable_op, has_mutable_arg, and has_exec_param are set if
 * we found any potentially-useful-for-pruning clause having those properties,
 * whether or not we actually used the clause in the steps list.  This
 * definition allows us to skip the PARTTARGET_EXEC pass in some cases.
 */
struct GeneratePruningStepsContext {
    /* Copies of input arguments for gen_partprune_steps: */
    rel: *mut RelOptInfo,       /* the partitioned relation */
    target: PartClauseTarget,   /* use-case we're generating steps for */
    /* Result data: */
    steps: *mut List,           /* list of PartitionPruneSteps */
    has_mutable_op: bool,       /* clauses include any stable operators */
    has_mutable_arg: bool,      /* clauses include any mutable comparison
                                 * values, *other than* exec params */
    has_exec_param: bool,       /* clauses include any PARAM_EXEC params */
    contradictory: bool,        /* clauses were proven self-contradictory */
    /* Working state: */
    next_step_id: c_int,
}

/* The result of performing one PartitionPruneStep */
struct PruneStepResult {
    /*
     * The offsets of bounds (in a table's boundinfo) whose partition is
     * selected by the pruning step.
     */
    bound_offsets: *mut Bitmapset,

    scan_default: bool, /* Scan the default partition? */
    scan_null: bool,    /* Scan the partition for NULL values? */
}

/* forward declarations - omitted; Rust needs none */

/*
 * make_partition_pruneinfo
 *		Checks if the given set of quals can be used to build pruning steps
 *		that the executor can use to prune away unneeded partitions.  If
 *		suitable quals are found then a PartitionPruneInfo is built and tagged
 *		onto the PlannerInfo's partPruneInfos list.
 *
 * The return value is the 0-based index of the item added to the
 * partPruneInfos list or -1 if nothing was added.
 *
 * 'parentrel' is the RelOptInfo for an appendrel, and 'subpaths' is the list
 * of scan paths for its child rels.
 * 'prunequal' is a list of potential pruning quals (i.e., restriction
 * clauses that are applicable to the appendrel).
 */
pub unsafe fn make_partition_pruneinfo(
    root: *mut PlannerInfo,
    parentrel: *mut RelOptInfo,
    subpaths: *mut List,
    prunequal: *mut List,
) -> c_int {
    let mut pruneinfo: *mut PartitionPruneInfo;
    let mut allmatchedsubplans: *mut Bitmapset = ptr::null_mut();
    let mut allpartrelids: *mut List;
    let mut prunerelinfos: *mut List;
    let mut relid_subplan_map: *mut c_int;
    let mut lc: *mut ListCell;
    let mut i: c_int;

    /*
     * Scan the subpaths to see which ones are scans of partition child
     * relations, and identify their parent partitioned rels.  (Note: we must
     * restrict the parent partitioned rels to be parentrel or children of
     * parentrel, otherwise we couldn't translate prunequal to match.)
     *
     * Also construct a temporary array to map from partition-child-relation
     * relid to the index in 'subpaths' of the scan plan for that partition.
     * (Use of "subplan" rather than "subpath" is a bit of a misnomer, but
     * we'll let it stand.)  For convenience, we use 1-based indexes here, so
     * that zero can represent an un-filled array entry.
     */
    allpartrelids = NIL;
    relid_subplan_map = palloc0(
        (size_of::<c_int>() * (*root).simple_rel_array_size as usize) as Size
    ) as *mut c_int;

    i = 1;
    foreach!(lc, subpaths, {
        let path = lfirst(current_cell!(lc)) as *mut crate::nodes::pathnodes::Path;
        let pathrel: *mut RelOptInfo = (*path).parent;

        /* We don't consider partitioned joins here */
        if (*pathrel).reloptkind == crate::nodes::pathnodes::RelOptKind::RELOPT_OTHER_MEMBER_REL {
            let mut prel: *mut RelOptInfo = pathrel;
            let mut partrelids: *mut Bitmapset = ptr::null_mut();

            /*
             * Traverse up to the pathrel's topmost partitioned parent,
             * collecting parent relids as we go; but stop if we reach
             * parentrel.  (Normally, a pathrel's topmost partitioned parent
             * is either parentrel or a UNION ALL appendrel child of
             * parentrel.  But when handling partitionwise joins of
             * multi-level partitioning trees, we can see an append path whose
             * parentrel is an intermediate partitioned table.)
             */
            loop {
                let appinfo: *mut AppendRelInfo;

                Assert!((*prel).relid < (*root).simple_rel_array_size as Index);
                appinfo = *(*root).append_rel_array.add((*prel).relid as usize);
                prel = find_base_rel(root, (*appinfo).parent_relid as c_int);
                if !IS_PARTITIONED_REL(prel) {
                    break; /* reached a non-partitioned parent */
                }
                /* accept this level as an interesting parent */
                partrelids = bms_add_member(partrelids, (*prel).relid as c_int);
                if prel == parentrel {
                    break; /* don't traverse above parentrel */
                }
                if (*prel).reloptkind != crate::nodes::pathnodes::RelOptKind::RELOPT_OTHER_MEMBER_REL {
                    break;
                }
            }

            if !partrelids.is_null() {
                /*
                 * Found some relevant parent partitions, which may or may not
                 * overlap with partition trees we already found.  Add new
                 * information to the allpartrelids list.
                 */
                allpartrelids = add_part_relids(allpartrelids, partrelids);
                /* Also record the subplan in relid_subplan_map[] */
                /* No duplicates please */
                Assert!(*relid_subplan_map.add((*pathrel).relid as usize) == 0);
                *relid_subplan_map.add((*pathrel).relid as usize) = i;
            }
        }
        i += 1;
    });

    /*
     * We now build a PartitionedRelPruneInfo for each topmost partitioned rel
     * (omitting any that turn out not to have useful pruning quals).
     */
    prunerelinfos = NIL;
    foreach!(lc, allpartrelids, {
        let partrelids: *mut Bitmapset = lfirst(current_cell!(lc)) as *mut Bitmapset;
        let mut pinfolist: *mut List;
        let mut matchedsubplans: *mut Bitmapset = ptr::null_mut();

        pinfolist = make_partitionedrel_pruneinfo(
            root,
            parentrel,
            prunequal,
            partrelids,
            relid_subplan_map,
            &mut matchedsubplans,
        );

        /* When pruning is possible, record the matched subplans */
        if pinfolist != NIL {
            prunerelinfos = lappend(prunerelinfos, pinfolist as *mut c_void);
            allmatchedsubplans = bms_join(matchedsubplans, allmatchedsubplans);
        }
    });

    pfree(relid_subplan_map as *mut c_void);

    /*
     * If none of the partition hierarchies had any useful run-time pruning
     * quals, then we can just not bother with run-time pruning.
     */
    if prunerelinfos == NIL {
        return -1;
    }

    /* Else build the result data structure */
    pruneinfo = makeNode!(PartitionPruneInfo, T_PartitionPruneInfo);
    (*pruneinfo).relids = bms_copy((*parentrel).relids);
    (*pruneinfo).prune_infos = prunerelinfos;

    /*
     * Some subplans may not belong to any of the identified partitioned rels.
     * This can happen for UNION ALL queries which include a non-partitioned
     * table, or when some of the hierarchies aren't run-time prunable.  Build
     * a bitmapset of the indexes of all such subplans, so that the executor
     * can identify which subplans should never be pruned.
     */
    if bms_num_members(allmatchedsubplans) < list_length(subpaths) {
        let mut other_subplans: *mut Bitmapset;

        /* Create the complement of allmatchedsubplans */
        other_subplans = bms_add_range(ptr::null_mut(), 0, list_length(subpaths) - 1);
        other_subplans = bms_del_members(other_subplans, allmatchedsubplans);

        (*pruneinfo).other_subplans = other_subplans;
    } else {
        (*pruneinfo).other_subplans = ptr::null_mut();
    }

    (*root).partPruneInfos = lappend((*root).partPruneInfos, pruneinfo as *mut c_void);

    list_length((*root).partPruneInfos) - 1
}

/*
 * add_part_relids
 *		Add new info to a list of Bitmapsets of partitioned relids.
 *
 * Within 'allpartrelids', there is one Bitmapset for each topmost parent
 * partitioned rel.  Each Bitmapset contains the RT indexes of the topmost
 * parent as well as its relevant non-leaf child partitions.  Since (by
 * construction of the rangetable list) parent partitions must have lower
 * RT indexes than their children, we can distinguish the topmost parent
 * as being the lowest set bit in the Bitmapset.
 *
 * 'partrelids' contains the RT indexes of a parent partitioned rel, and
 * possibly some non-leaf children, that are newly identified as parents of
 * some subpath rel passed to make_partition_pruneinfo().  These are added
 * to an appropriate member of 'allpartrelids'.
 *
 * Note that the list contains only RT indexes of partitioned tables that
 * are parents of some scan-level relation appearing in the 'subpaths' that
 * make_partition_pruneinfo() is dealing with.  Also, "topmost" parents are
 * not allowed to be higher than the 'parentrel' associated with the append
 * path.  In this way, we avoid expending cycles on partitioned rels that
 * can't contribute useful pruning information for the problem at hand.
 * (It is possible for 'parentrel' to be a child partitioned table, and it
 * is also possible for scan-level relations to be child partitioned tables
 * rather than leaf partitions.  Hence we must construct this relation set
 * with reference to the particular append path we're dealing with, rather
 * than looking at the full partitioning structure represented in the
 * RelOptInfos.)
 */
unsafe fn add_part_relids(
    allpartrelids: *mut List,
    partrelids: *mut Bitmapset,
) -> *mut List {
    let mut targetpart: Index;
    let mut lc: *mut ListCell;

    /* We can easily get the lowest set bit this way: */
    targetpart = bms_next_member(partrelids, -1) as Index;
    Assert!(targetpart > 0);

    /* Look for a matching topmost parent */
    foreach!(lc, allpartrelids, {
        let mut currpartrelids: *mut Bitmapset = lfirst(current_cell!(lc)) as *mut Bitmapset;
        let currtarget: Index = bms_next_member(currpartrelids, -1) as Index;

        if targetpart == currtarget {
            /* Found a match, so add any new RT indexes to this hierarchy */
            currpartrelids = bms_add_members(currpartrelids, partrelids);
            // lfirst(lc) = currpartrelids -- update the cell's data pointer
            (*current_cell!(lc)).ptr_value = currpartrelids as *mut c_void;
            return allpartrelids;
        }
    });
    /* No match, so add the new partition hierarchy to the list */
    lappend(allpartrelids, partrelids as *mut c_void)
}

/*
 * make_partitionedrel_pruneinfo
 *		Build a List of PartitionedRelPruneInfos, one for each interesting
 *		partitioned rel in a partitioning hierarchy.  These can be used in the
 *		executor to allow additional partition pruning to take place.
 *
 * parentrel: rel associated with the appendpath being considered
 * prunequal: potential pruning quals, represented for parentrel
 * partrelids: Set of RT indexes identifying relevant partitioned tables
 *   within a single partitioning hierarchy
 * relid_subplan_map[]: maps child relation relids to subplan indexes
 * matchedsubplans: on success, receives the set of subplan indexes which
 *   were matched to this partition hierarchy
 *
 * If we cannot find any useful run-time pruning steps, return NIL.
 * However, on success, each rel identified in partrelids will have
 * an element in the result list, even if some of them are useless.
 */
unsafe fn make_partitionedrel_pruneinfo(
    root: *mut PlannerInfo,
    parentrel: *mut RelOptInfo,
    mut prunequal: *mut List,
    partrelids: *mut Bitmapset,
    relid_subplan_map: *mut c_int,
    matchedsubplans: *mut *mut Bitmapset,
) -> *mut List {
    let mut targetpart: *mut RelOptInfo = ptr::null_mut();
    let mut pinfolist: *mut List = NIL;
    let mut doruntimeprune: bool = false;
    let mut relid_subpart_map: *mut c_int;
    let mut subplansfound: *mut Bitmapset = ptr::null_mut();
    let mut lc: *mut ListCell;
    let mut rti: c_int;
    let mut i: c_int;

    /*
     * Examine each partitioned rel, constructing a temporary array to map
     * from planner relids to index of the partitioned rel, and building a
     * PartitionedRelPruneInfo for each partitioned rel.
     *
     * In this phase we discover whether runtime pruning is needed at all; if
     * not, we can avoid doing further work.
     */
    relid_subpart_map = palloc0(
        (size_of::<c_int>() * (*root).simple_rel_array_size as usize) as Size
    ) as *mut c_int;

    i = 1;
    rti = -1;
    while { rti = bms_next_member(partrelids, rti); rti > 0 } {
        let subpart: *mut RelOptInfo = find_base_rel(root, rti);
        let pinfo: *mut PartitionedRelPruneInfo;
        let mut partprunequal: *mut List;
        let initial_pruning_steps: *mut List;
        let exec_pruning_steps: *mut List;
        let execparamids: *mut Bitmapset;
        let mut context: GeneratePruningStepsContext = std::mem::zeroed();

        /*
         * Fill the mapping array.
         *
         * relid_subpart_map maps relid of a non-leaf partition to the index
         * in the returned PartitionedRelPruneInfo list of the info for that
         * partition.  We use 1-based indexes here, so that zero can represent
         * an un-filled array entry.
         */
        Assert!(rti < (*root).simple_rel_array_size);
        *relid_subpart_map.add(rti as usize) = i;
        i += 1;

        /*
         * Translate pruning qual, if necessary, for this partition.
         *
         * The first item in the list is the target partitioned relation.
         */
        if targetpart.is_null() {
            targetpart = subpart;

            /*
             * The prunequal is presented to us as a qual for 'parentrel'.
             * Frequently this rel is the same as targetpart, so we can skip
             * an adjust_appendrel_attrs step.  But it might not be, and then
             * we have to translate.  We update the prunequal parameter here,
             * because in later iterations of the loop for child partitions,
             * we want to translate from parent to child variables.
             */
            if !bms_equal((*parentrel).relids, (*subpart).relids) {
                let mut nappinfos: c_int = 0;
                let appinfos: *mut *mut AppendRelInfo = find_appinfos_by_relids(
                    root,
                    (*subpart).relids,
                    &mut nappinfos,
                );

                prunequal = adjust_appendrel_attrs(
                    root,
                    prunequal as *mut Node,
                    nappinfos,
                    appinfos,
                ) as *mut List;

                pfree(appinfos as *mut c_void);
            }

            partprunequal = prunequal;
        } else {
            /*
             * For sub-partitioned tables the columns may not be in the same
             * order as the parent, so we must translate the prunequal to make
             * it compatible with this relation.
             */
            partprunequal = adjust_appendrel_attrs_multilevel(
                root,
                prunequal as *mut Node,
                subpart,
                targetpart,
            ) as *mut List;
        }

        /*
         * Convert pruning qual to pruning steps.  We may need to do this
         * twice, once to obtain executor startup pruning steps, and once for
         * executor per-scan pruning steps.  This first pass creates startup
         * pruning steps and detects whether there's any possibly-useful quals
         * that would require per-scan pruning.
         */
        gen_partprune_steps(subpart, partprunequal, PARTTARGET_INITIAL, &mut context);

        if context.contradictory {
            /*
             * This shouldn't happen as the planner should have detected this
             * earlier. However, we do use additional quals from parameterized
             * paths here. These do only compare Params to the partition key,
             * so this shouldn't cause the discovery of any new qual
             * contradictions that were not previously discovered as the Param
             * values are unknown during planning.  Anyway, we'd better do
             * something sane here, so let's just disable run-time pruning.
             */
            return NIL;
        }

        /*
         * If no mutable operators or expressions appear in usable pruning
         * clauses, then there's no point in running startup pruning, because
         * plan-time pruning should have pruned everything prunable.
         */
        let initial_pruning_steps: *mut List = if context.has_mutable_op || context.has_mutable_arg {
            context.steps
        } else {
            NIL
        };

        /*
         * If no exec Params appear in potentially-usable pruning clauses,
         * then there's no point in even thinking about per-scan pruning.
         */
        let (exec_pruning_steps, execparamids): (*mut List, *mut Bitmapset) =
        if context.has_exec_param {
            /* ... OK, we'd better think about it */
            gen_partprune_steps(subpart, partprunequal, PARTTARGET_EXEC, &mut context);

            if context.contradictory {
                /* As above, skip run-time pruning if anything fishy happens */
                return NIL;
            }

            let eps = context.steps;

            /*
             * Detect which exec Params actually got used; the fact that some
             * were in available clauses doesn't mean we actually used them.
             * Skip per-scan pruning if there are none.
             */
            let eid = get_partkey_exec_paramids(eps);

            if bms_is_empty(eid) {
                (NIL, ptr::null_mut())
            } else {
                (eps, eid)
            }
        } else {
            /* No exec Params anywhere, so forget about scan-time pruning */
            (NIL, ptr::null_mut())
        };

        if !initial_pruning_steps.is_null() && initial_pruning_steps != NIL
            || !exec_pruning_steps.is_null() && exec_pruning_steps != NIL
        {
            doruntimeprune = true;
        }

        /* Begin constructing the PartitionedRelPruneInfo for this rel */
        let pinfo: *mut PartitionedRelPruneInfo =
            makeNode!(PartitionedRelPruneInfo, T_PartitionedRelPruneInfo);
        (*pinfo).rtindex = rti as Index;
        (*pinfo).initial_pruning_steps = initial_pruning_steps;
        (*pinfo).exec_pruning_steps = exec_pruning_steps;
        (*pinfo).execparamids = execparamids;
        /* Remaining fields will be filled in the next loop */

        pinfolist = lappend(pinfolist, pinfo as *mut c_void);
    }

    if !doruntimeprune {
        /* No run-time pruning required. */
        pfree(relid_subpart_map as *mut c_void);
        return NIL;
    }

    /*
     * Run-time pruning will be required, so initialize other information.
     * That includes two maps -- one needed to convert partition indexes of
     * leaf partitions to the indexes of their subplans in the subplan list,
     * another needed to convert partition indexes of sub-partitioned
     * partitions to the indexes of their PartitionedRelPruneInfo in the
     * PartitionedRelPruneInfo list.
     */
    foreach!(lc, pinfolist, {
        let pinfo: *mut PartitionedRelPruneInfo = lfirst(current_cell!(lc)) as *mut PartitionedRelPruneInfo;
        let subpart: *mut RelOptInfo = find_base_rel(root, (*pinfo).rtindex as c_int);
        let mut present_parts: *mut Bitmapset;
        let nparts: c_int = (*subpart).nparts;
        let subplan_map: *mut c_int;
        let subpart_map: *mut c_int;
        let relid_map: *mut Oid;
        let leafpart_rti_map: *mut c_int;

        /*
         * Construct the subplan and subpart maps for this partitioning level.
         * Here we convert to zero-based indexes, with -1 for empty entries.
         * Also construct a Bitmapset of all partitions that are present (that
         * is, not pruned already).
         */
        subplan_map = palloc((nparts as usize * size_of::<c_int>()) as Size) as *mut c_int;
        std::ptr::write_bytes(subplan_map, 0xff, nparts as usize); /* memset -1 */
        subpart_map = palloc((nparts as usize * size_of::<c_int>()) as Size) as *mut c_int;
        std::ptr::write_bytes(subpart_map, 0xff, nparts as usize); /* memset -1 */
        relid_map = palloc0((nparts as usize * size_of::<Oid>()) as Size) as *mut Oid;
        leafpart_rti_map = palloc0((nparts as usize * size_of::<c_int>()) as Size) as *mut c_int;
        present_parts = ptr::null_mut();

        let mut ii: c_int = -1;
        while { ii = bms_next_member((*subpart).live_parts, ii); ii >= 0 } {
            let partrel: *mut RelOptInfo = *(*subpart).part_rels.add(ii as usize);
            let subplanidx: c_int;
            let subpartidx: c_int;

            Assert!(!partrel.is_null());

            *subplan_map.add(ii as usize) = *relid_subplan_map.add((*partrel).relid as usize) - 1;
            subplanidx = *subplan_map.add(ii as usize);
            *subpart_map.add(ii as usize) = *relid_subpart_map.add((*partrel).relid as usize) - 1;
            subpartidx = *subpart_map.add(ii as usize);
            *relid_map.add(ii as usize) = (*planner_rt_fetch((*partrel).relid as c_int, root)).relid;

            /*
             * Track the RT indexes of "leaf" partitions so they can be
             * included in the PlannerGlobal.prunableRelids set, indicating
             * relations that may be pruned during executor startup.
             *
             * Only leaf partitions with a valid subplan that are prunable
             * using initial pruning are added to prunableRelids. So
             * partitions without a subplan due to constraint exclusion will
             * remain in PlannedStmt.unprunableRelids.
             */
            if subplanidx >= 0 {
                present_parts = bms_add_member(present_parts, ii);

                /*
                 * Non-leaf partitions may appear here when they use an
                 * unflattened Append or MergeAppend. These should not be
                 * included in prunableRelids.
                 */
                if (*partrel).nparts == -1 {
                    *leafpart_rti_map.add(ii as usize) = (*partrel).relid as c_int;
                }

                /* Record finding this subplan  */
                subplansfound = bms_add_member(subplansfound, subplanidx);
            } else if subpartidx >= 0 {
                present_parts = bms_add_member(present_parts, ii);
            }
        }

        /*
         * Ensure there were no stray PartitionedRelPruneInfo generated for
         * partitioned tables that we have no sub-paths or
         * sub-PartitionedRelPruneInfo for.
         */
        Assert!(!bms_is_empty(present_parts));

        /* Record the maps and other information. */
        (*pinfo).present_parts = present_parts;
        (*pinfo).nparts = nparts;
        (*pinfo).subplan_map = subplan_map;
        (*pinfo).subpart_map = subpart_map;
        (*pinfo).relid_map = relid_map;
        (*pinfo).leafpart_rti_map = leafpart_rti_map;
    });

    pfree(relid_subpart_map as *mut c_void);

    *matchedsubplans = subplansfound;

    pinfolist
}

/*
 * gen_partprune_steps
 *		Process 'clauses' (typically a rel's baserestrictinfo list of clauses)
 *		and create a list of "partition pruning steps".
 *
 * 'target' tells whether to generate pruning steps for planning (use
 * immutable clauses only), or for executor startup (use any allowable
 * clause except ones containing PARAM_EXEC Params), or for executor
 * per-scan pruning (use any allowable clause).
 *
 * 'context' is an output argument that receives the steps list as well as
 * some subsidiary flags; see the GeneratePruningStepsContext typedef.
 */
unsafe fn gen_partprune_steps(
    rel: *mut RelOptInfo,
    mut clauses: *mut List,
    target: PartClauseTarget,
    context: *mut GeneratePruningStepsContext,
) {
    /* Initialize all output values to zero/false/NULL */
    std::ptr::write_bytes(context, 0, 1);
    (*context).rel = rel;
    (*context).target = target;

    /*
     * If this partitioned table is in turn a partition, and it shares any
     * partition keys with its parent, then it's possible that the hierarchy
     * allows the parent a narrower range of values than some of its
     * partitions (particularly the default one).  This is normally not
     * useful, but it can be to prune the default partition.
     */
    if partition_bound_has_default((*rel).boundinfo) && (*rel).partition_qual != NIL {
        /* Make a copy to avoid modifying the passed-in List */
        clauses = list_concat_copy(clauses, (*rel).partition_qual);
    }

    /* Down into the rabbit-hole. */
    gen_partprune_steps_internal(context, clauses);
}

/*
 * prune_append_rel_partitions
 *		Process rel's baserestrictinfo and make use of quals which can be
 *		evaluated during query planning in order to determine the minimum set
 *		of partitions which must be scanned to satisfy these quals.  Returns
 *		the matching partitions in the form of a Bitmapset containing the
 *		partitions' indexes in the rel's part_rels array.
 *
 * Callers must ensure that 'rel' is a partitioned table.
 */
pub unsafe fn prune_append_rel_partitions(rel: *mut RelOptInfo) -> *mut Bitmapset {
    let clauses: *mut List = (*rel).baserestrictinfo;
    let pruning_steps: *mut List;
    let mut gcontext: GeneratePruningStepsContext = std::mem::zeroed();
    let mut context: PartitionPruneContext = std::mem::zeroed();

    Assert!(!(*rel).part_scheme.is_null());

    /* If there are no partitions, return the empty set */
    if (*rel).nparts == 0 {
        return ptr::null_mut();
    }

    /*
     * If pruning is disabled or if there are no clauses to prune with, return
     * all partitions.
     */
    if !enable_partition_pruning || clauses == NIL {
        return bms_add_range(ptr::null_mut(), 0, (*rel).nparts - 1);
    }

    /*
     * Process clauses to extract pruning steps that are usable at plan time.
     * If the clauses are found to be contradictory, we can return the empty
     * set.
     */
    gen_partprune_steps(rel, clauses, PARTTARGET_PLANNER, &mut gcontext);
    if gcontext.contradictory {
        return ptr::null_mut();
    }
    let pruning_steps: *mut List = gcontext.steps;

    /* If there's nothing usable, return all partitions */
    if pruning_steps == NIL {
        return bms_add_range(ptr::null_mut(), 0, (*rel).nparts - 1);
    }

    /* Set up PartitionPruneContext */
    context.strategy = (*(*rel).part_scheme).strategy as c_char;
    context.partnatts = (*(*rel).part_scheme).partnatts as c_int;
    context.nparts = (*rel).nparts;
    context.boundinfo = (*rel).boundinfo;
    context.partcollation = (*(*rel).part_scheme).partcollation;
    context.partsupfunc = (*(*rel).part_scheme).partsupfunc;
    context.stepcmpfuncs = palloc0(
        (size_of::<FmgrInfo>()
            * context.partnatts as usize
            * list_length(pruning_steps) as usize) as Size,
    ) as *mut FmgrInfo;
    context.ppccontext = CurrentMemoryContext;

    /* These are not valid when being called from the planner */
    context.planstate = ptr::null_mut();
    context.exprcontext = ptr::null_mut();
    context.exprstates = ptr::null_mut();

    /* Actual pruning happens here. */
    get_matching_partitions(&mut context, pruning_steps)
}

/*
 * get_matching_partitions
 *		Determine partitions that survive partition pruning
 *
 * Note: context->exprcontext must be valid when the pruning_steps were
 * generated with a target other than PARTTARGET_PLANNER.
 *
 * Returns a Bitmapset of the RelOptInfo->part_rels indexes of the surviving
 * partitions.
 */
pub unsafe fn get_matching_partitions(
    context: *mut PartitionPruneContext,
    pruning_steps: *mut List,
) -> *mut Bitmapset {
    let mut result: *mut Bitmapset;
    let num_steps: c_int = list_length(pruning_steps);
    let mut i: c_int;
    let results: *mut *mut PruneStepResult;
    let final_result: *mut PruneStepResult;
    let mut lc: *mut ListCell;
    let mut scan_default: bool;

    /* If there are no pruning steps then all partitions match. */
    if num_steps == 0 {
        Assert!((*context).nparts > 0);
        return bms_add_range(ptr::null_mut(), 0, (*context).nparts - 1);
    }

    /*
     * Allocate space for individual pruning steps to store its result.  Each
     * slot will hold a PruneStepResult after performing a given pruning step.
     * Later steps may use the result of one or more earlier steps.  The
     * result of applying all pruning steps is the value contained in the slot
     * of the last pruning step.
     */
    results = palloc0(
        (num_steps as usize * size_of::<*mut PruneStepResult>()) as Size
    ) as *mut *mut PruneStepResult;
    foreach!(lc, pruning_steps, {
        let step: *mut PartitionPruneStep = lfirst(current_cell!(lc)) as *mut PartitionPruneStep;

        match crate::nodes::nodes::nodeTag(step as *const Node) {
            T_PartitionPruneStepOp => {
                *results.add((*step).step_id as usize) =
                    perform_pruning_base_step(context, step as *mut PartitionPruneStepOp);
            }
            T_PartitionPruneStepCombine => {
                *results.add((*step).step_id as usize) =
                    perform_pruning_combine_step(
                        context,
                        step as *mut PartitionPruneStepCombine,
                        results,
                    );
            }
            _ => {
                elog!(ERROR, "invalid pruning step type: {}", crate::nodes::nodes::nodeTag(step as *const Node) as i32);
            }
        }
    });

    /*
     * At this point we know the offsets of all the datums whose corresponding
     * partitions need to be in the result, including special null-accepting
     * and default partitions.  Collect the actual partition indexes now.
     */
    final_result = *results.add(num_steps as usize - 1);
    Assert!(!final_result.is_null());
    i = -1;
    result = ptr::null_mut();
    scan_default = (*final_result).scan_default;
    while { i = bms_next_member((*final_result).bound_offsets, i); i >= 0 } {
        let partindex: c_int;

        Assert!(i < (*(*context).boundinfo).nindexes);
        partindex = *(*(*context).boundinfo).indexes.add(i as usize);

        if partindex < 0 {
            /*
             * In range partitioning cases, if a partition index is -1 it
             * means that the bound at the offset is the upper bound for a
             * range not covered by any partition (other than a possible
             * default partition).  In hash partitioning, the same means no
             * partition has been defined for the corresponding remainder
             * value.
             *
             * In either case, the value is still part of the queried range of
             * values, so mark to scan the default partition if one exists.
             */
            scan_default |= partition_bound_has_default((*context).boundinfo);
            continue;
        }

        result = bms_add_member(result, partindex);
    }

    /* Add the null and/or default partition if needed and present. */
    if (*final_result).scan_null {
        Assert!((*context).strategy as u8 == PARTITION_STRATEGY_LIST as u8);
        Assert!(partition_bound_accepts_nulls((*context).boundinfo));
        result = bms_add_member(result, (*(*context).boundinfo).null_index);
    }
    if scan_default {
        Assert!(
            (*context).strategy as u8 == PARTITION_STRATEGY_LIST as u8
                || (*context).strategy as u8 == PARTITION_STRATEGY_RANGE as u8
        );
        Assert!(partition_bound_has_default((*context).boundinfo));
        result = bms_add_member(result, (*(*context).boundinfo).default_index);
    }

    result
}

/*
 * gen_partprune_steps_internal
 *		Processes 'clauses' to generate a List of partition pruning steps.  We
 *		return NIL when no steps were generated.
 *
 * These partition pruning steps come in 2 forms; operator steps and combine
 * steps.
 *
 * Operator steps (PartitionPruneStepOp) contain details of clauses that we
 * determined that we can use for partition pruning.  These contain details of
 * the expression which is being compared to the partition key and the
 * comparison function.
 *
 * Combine steps (PartitionPruneStepCombine) instruct the partition pruning
 * code how it should produce a single set of partitions from multiple input
 * operator and other combine steps.  A PARTPRUNE_COMBINE_INTERSECT type
 * combine step will merge its input steps to produce a result which only
 * contains the partitions which are present in all of the input operator
 * steps.  A PARTPRUNE_COMBINE_UNION combine step will produce a result that
 * has all of the partitions from each of the input operator steps.
 *
 * For BoolExpr clauses, each argument is processed recursively. Steps
 * generated from processing an OR BoolExpr will be combined using
 * PARTPRUNE_COMBINE_UNION.  AND BoolExprs get combined using
 * PARTPRUNE_COMBINE_INTERSECT.
 *
 * Otherwise, the list of clauses we receive we assume to be mutually ANDed.
 * We generate all of the pruning steps we can based on these clauses and then
 * at the end, if we have more than 1 step, we combine each step with a
 * PARTPRUNE_COMBINE_INTERSECT combine step.  Single steps are returned as-is.
 *
 * If we find clauses that are mutually contradictory, or contradictory with
 * the partitioning constraint, or a pseudoconstant clause that contains
 * false, we set context->contradictory to true and return NIL (that is, no
 * pruning steps).  Caller should consider all partitions as pruned in that
 * case.
 */
unsafe fn gen_partprune_steps_internal(
    context: *mut GeneratePruningStepsContext,
    clauses: *mut List,
) -> *mut List {
    let part_scheme: PartitionScheme = (*(*context).rel).part_scheme;
    let mut keyclauses: [*mut List; PARTITION_MAX_KEYS] = [NIL; PARTITION_MAX_KEYS];
    let mut nullkeys: *mut Bitmapset = ptr::null_mut();
    let mut notnullkeys: *mut Bitmapset = ptr::null_mut();
    let mut generate_opsteps: bool = false;
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;

    /*
     * If this partitioned relation has a default partition and is itself a
     * partition (as evidenced by partition_qual being not NIL), we first
     * check if the clauses contradict the partition constraint.  If they do,
     * there's no need to generate any steps as it'd already be proven that no
     * partitions need to be scanned.
     *
     * This is a measure of last resort only to be used because the default
     * partition cannot be pruned using the steps generated from clauses that
     * contradict the parent's partition constraint; regular pruning, which is
     * cheaper, is sufficient when no default partition exists.
     */
    if partition_bound_has_default((*(*context).rel).boundinfo)
        && predicate_refuted_by((*(*context).rel).partition_qual, clauses, false)
    {
        (*context).contradictory = true;
        return NIL;
    }

    /* keyclauses already zeroed above */
    foreach!(lc, clauses, {
        let mut clause: *mut Expr = lfirst(current_cell!(lc)) as *mut Expr;
        let mut i: c_int;

        /* Look through RestrictInfo, if any */
        if IsA!(clause, T_RestrictInfo) {
            clause = (*(clause as *mut crate::nodes::pathnodes::RestrictInfo)).clause;
        }

        /* Constant-false-or-null is contradictory */
        if IsA!(clause, T_Const) {
            let c = clause as *mut Const;
            if (*c).constisnull || !DatumGetBool((*c).constvalue) {
                (*context).contradictory = true;
                return NIL;
            }
        }

        /* Get the BoolExpr's out of the way. */
        if IsA!(clause, T_BoolExpr) {
            /*
             * Generate steps for arguments.
             *
             * While steps generated for the arguments themselves will be
             * added to context->steps during recursion and will be evaluated
             * independently, collect their step IDs to be stored in the
             * combine step we'll be creating.
             */
            if is_orclause(clause) {
                let mut arg_stepids: *mut List = NIL;
                let mut all_args_contradictory: bool = true;
                let mut lc1: *mut ListCell;

                /*
                 * We can share the outer context area with the recursive
                 * call, but contradictory had better not be true yet.
                 */
                Assert!(!(*context).contradictory);

                /*
                 * Get pruning step for each arg.  If we get contradictory for
                 * all args, it means the OR expression is false as a whole.
                 */
                foreach!(lc1, (*(clause as *mut BoolExpr)).args, {
                    let arg: *mut Expr = lfirst(current_cell!(lc1)) as *mut Expr;
                    let arg_contradictory: bool;
                    let argsteps: *mut List;

                    let argsteps = gen_partprune_steps_internal(context, list_make1!(arg as *mut c_void));
                    arg_contradictory = (*context).contradictory;
                    /* Keep context->contradictory clear till we're done */
                    (*context).contradictory = false;

                    if arg_contradictory {
                        /* Just ignore self-contradictory arguments. */
                        continue;
                    } else {
                        all_args_contradictory = false;
                    }

                    if argsteps != NIL {
                        /*
                         * gen_partprune_steps_internal() always adds a single
                         * combine step when it generates multiple steps, so
                         * here we can just pay attention to the last one in
                         * the list.  If it just generated one, then the last
                         * one in the list is still the one we want.
                         */
                        let last: *mut PartitionPruneStep = llast(argsteps) as *mut PartitionPruneStep;

                        arg_stepids = lappend_int(arg_stepids, (*last).step_id);
                    } else {
                        let orstep: *mut PartitionPruneStep;

                        /*
                         * The arg didn't contain a clause matching this
                         * partition key.  We cannot prune using such an arg.
                         * To indicate that to the pruning code, we must
                         * construct a dummy PartitionPruneStepCombine whose
                         * source_stepids is set to an empty List.
                         */
                        orstep = gen_prune_step_combine(context, NIL, PARTPRUNE_COMBINE_UNION);
                        arg_stepids = lappend_int(arg_stepids, (*orstep).step_id);
                    }
                });

                /* If all the OR arms are contradictory, we can stop */
                if all_args_contradictory {
                    (*context).contradictory = true;
                    return NIL;
                }

                if arg_stepids != NIL {
                    let step: *mut PartitionPruneStep;

                    step = gen_prune_step_combine(context, arg_stepids, PARTPRUNE_COMBINE_UNION);
                    result = lappend(result, step as *mut c_void);
                }
                continue;
            } else if is_andclause(clause) {
                let args: *mut List = (*(clause as *mut BoolExpr)).args;
                let argsteps: *mut List;

                /*
                 * args may itself contain clauses of arbitrary type, so just
                 * recurse and later combine the component partitions sets
                 * using a combine step.
                 */
                let argsteps = gen_partprune_steps_internal(context, args);

                /* If any AND arm is contradictory, we can stop immediately */
                if (*context).contradictory {
                    return NIL;
                }

                /*
                 * gen_partprune_steps_internal() always adds a single combine
                 * step when it generates multiple steps, so here we can just
                 * pay attention to the last one in the list.  If it just
                 * generated one, then the last one in the list is still the
                 * one we want.
                 */
                if argsteps != NIL {
                    result = lappend(result, llast(argsteps));
                }

                continue;
            }

            /*
             * Fall-through for a NOT clause, which if it's a Boolean clause,
             * will be handled in match_clause_to_partition_key(). We
             * currently don't perform any pruning for more complex NOT
             * clauses.
             */
        }

        /*
         * See if we can match this clause to any of the partition keys.
         */
        i = 0;
        while i < (*part_scheme).partnatts as c_int {
            let partkey: *mut Expr = linitial(*(*(*context).rel).partexprs.add(i as usize)) as *mut Expr;
            let mut clause_is_not_null: bool = false;
            let mut pc: *mut PartClauseInfo = ptr::null_mut();
            let mut clause_steps: *mut List = NIL;

            match match_clause_to_partition_key(
                context,
                clause,
                partkey,
                i,
                &mut clause_is_not_null,
                &mut pc,
                &mut clause_steps,
            ) {
                PARTCLAUSE_MATCH_CLAUSE => {
                    Assert!(!pc.is_null());

                    /*
                     * Since we only allow strict operators, check for any
                     * contradicting IS NULL.
                     */
                    if bms_is_member(i, nullkeys) {
                        (*context).contradictory = true;
                        return NIL;
                    }
                    generate_opsteps = true;
                    *keyclauses.as_mut_ptr().add(i as usize) =
                        lappend(*keyclauses.as_ptr().add(i as usize), pc as *mut c_void);
                    break;
                }
                PARTCLAUSE_MATCH_NULLNESS => {
                    if !clause_is_not_null {
                        /*
                         * check for conflicting IS NOT NULL as well as
                         * contradicting strict clauses
                         */
                        if bms_is_member(i, notnullkeys)
                            || *keyclauses.as_ptr().add(i as usize) != NIL
                        {
                            (*context).contradictory = true;
                            return NIL;
                        }
                        nullkeys = bms_add_member(nullkeys, i);
                    } else {
                        /* check for conflicting IS NULL */
                        if bms_is_member(i, nullkeys) {
                            (*context).contradictory = true;
                            return NIL;
                        }
                        notnullkeys = bms_add_member(notnullkeys, i);
                    }
                    break;
                }
                PARTCLAUSE_MATCH_STEPS => {
                    Assert!(clause_steps != NIL);
                    result = list_concat(result, clause_steps);
                    break;
                }
                PARTCLAUSE_MATCH_CONTRADICT => {
                    /* We've nothing more to do if a contradiction was found. */
                    (*context).contradictory = true;
                    return NIL;
                }
                PARTCLAUSE_NOMATCH => {
                    /*
                     * Clause didn't match this key, but it might match the
                     * next one.
                     */
                    i += 1;
                    continue;
                }
                PARTCLAUSE_UNSUPPORTED => {
                    /* This clause cannot be used for pruning. */
                    break;
                }
            }

            /* done; go check the next clause. */
            break;
        }
    });

    /*-----------
     * Now generate some (more) pruning steps.  We have three strategies:
     *
     * 1) Generate pruning steps based on IS NULL clauses:
     *   a) For list partitioning, null partition keys can only be found in
     *      the designated null-accepting partition, so if there are IS NULL
     *      clauses containing partition keys we should generate a pruning
     *      step that gets rid of all partitions but that one.  We can
     *      disregard any OpExpr we may have found.
     *   b) For range partitioning, only the default partition can contain
     *      NULL values, so the same rationale applies.
     *   c) For hash partitioning, we only apply this strategy if we have
     *      IS NULL clauses for all the keys.  Strategy 2 below will take
     *      care of the case where some keys have OpExprs and others have
     *      IS NULL clauses.
     *
     * 2) If not, generate steps based on OpExprs we have (if any).
     *
     * 3) If this doesn't work either, we may be able to generate steps to
     *    prune just the null-accepting partition (if one exists), if we have
     *    IS NOT NULL clauses for all partition keys.
     */
    if !bms_is_empty(nullkeys)
        && ((*part_scheme).strategy as u8 == PARTITION_STRATEGY_LIST as u8
            || (*part_scheme).strategy as u8 == PARTITION_STRATEGY_RANGE as u8
            || ((*part_scheme).strategy as u8 == PARTITION_STRATEGY_HASH as u8
                && bms_num_members(nullkeys) == (*part_scheme).partnatts as c_int))
    {
        let step: *mut PartitionPruneStep;

        /* Strategy 1 */
        step = gen_prune_step_op(context, InvalidStrategy, false, NIL, NIL, nullkeys);
        result = lappend(result, step as *mut c_void);
    } else if generate_opsteps {
        let opsteps: *mut List;

        /* Strategy 2 */
        let opsteps = gen_prune_steps_from_opexps(context, keyclauses.as_mut_ptr(), nullkeys);
        result = list_concat(result, opsteps);
    } else if bms_num_members(notnullkeys) == (*part_scheme).partnatts as c_int {
        let step: *mut PartitionPruneStep;

        /* Strategy 3 */
        step = gen_prune_step_op(context, InvalidStrategy, false, NIL, NIL, ptr::null_mut());
        result = lappend(result, step as *mut c_void);
    }

    /*
     * Finally, if there are multiple steps, since the 'clauses' are mutually
     * ANDed, add an INTERSECT step to combine the partition sets resulting
     * from them and append it to the result list.
     */
    if list_length(result) > 1 {
        let mut step_ids: *mut List = NIL;
        let final_step: *mut PartitionPruneStep;

        foreach!(lc, result, {
            let step: *mut PartitionPruneStep = lfirst(current_cell!(lc)) as *mut PartitionPruneStep;

            step_ids = lappend_int(step_ids, (*step).step_id);
        });

        let final_step = gen_prune_step_combine(context, step_ids, PARTPRUNE_COMBINE_INTERSECT);
        result = lappend(result, final_step as *mut c_void);
    }

    result
}

/*
 * gen_prune_step_op
 *		Generate a pruning step for a specific operator
 *
 * The step is assigned a unique step identifier and added to context's 'steps'
 * list.
 */
unsafe fn gen_prune_step_op(
    context: *mut GeneratePruningStepsContext,
    opstrategy: StrategyNumber,
    op_is_ne: bool,
    exprs: *mut List,
    cmpfns: *mut List,
    nullkeys: *mut Bitmapset,
) -> *mut PartitionPruneStep {
    let opstep: *mut PartitionPruneStepOp =
        makeNode!(PartitionPruneStepOp, T_PartitionPruneStepOp);

    (*opstep).step.step_id = (*context).next_step_id;
    (*context).next_step_id += 1;

    /*
     * For clauses that contain an <> operator, set opstrategy to
     * InvalidStrategy to signal get_matching_list_bounds to do the right
     * thing.
     */
    (*opstep).opstrategy = if op_is_ne { InvalidStrategy } else { opstrategy };
    Assert!(list_length(exprs) == list_length(cmpfns));
    (*opstep).exprs = exprs;
    (*opstep).cmpfns = cmpfns;
    (*opstep).nullkeys = nullkeys;

    (*context).steps = lappend((*context).steps, opstep as *mut c_void);

    opstep as *mut PartitionPruneStep
}

/*
 * gen_prune_step_combine
 *		Generate a pruning step for a combination of several other steps
 *
 * The step is assigned a unique step identifier and added to context's
 * 'steps' list.
 */
unsafe fn gen_prune_step_combine(
    context: *mut GeneratePruningStepsContext,
    source_stepids: *mut List,
    combineOp: PartitionPruneCombineOp,
) -> *mut PartitionPruneStep {
    let cstep: *mut PartitionPruneStepCombine =
        makeNode!(PartitionPruneStepCombine, T_PartitionPruneStepCombine);

    (*cstep).step.step_id = (*context).next_step_id;
    (*context).next_step_id += 1;
    (*cstep).combineOp = combineOp;
    (*cstep).source_stepids = source_stepids;

    (*context).steps = lappend((*context).steps, cstep as *mut c_void);

    cstep as *mut PartitionPruneStep
}

/*
 * gen_prune_steps_from_opexps
 *		Generate and return a list of PartitionPruneStepOp that are based on
 *		OpExpr and BooleanTest clauses that have been matched to the partition
 *		key.
 *
 * 'keyclauses' is an array of List pointers, indexed by the partition key's
 * index.  Each List element in the array can contain clauses that match to
 * the corresponding partition key column.  Partition key columns without any
 * matched clauses will have an empty List.
 *
 * Some partitioning strategies allow pruning to still occur when we only have
 * clauses for a prefix of the partition key columns, for example, RANGE
 * partitioning.  Other strategies, such as HASH partitioning, require clauses
 * for all partition key columns.
 *
 * When we return multiple pruning steps here, it's up to the caller to add a
 * relevant "combine" step to combine the returned steps.  This is not done
 * here as callers may wish to include additional pruning steps before
 * combining them all.
 */
unsafe fn gen_prune_steps_from_opexps(
    context: *mut GeneratePruningStepsContext,
    keyclauses: *mut *mut List,
    nullkeys: *mut Bitmapset,
) -> *mut List {
    let part_scheme: PartitionScheme = (*(*context).rel).part_scheme;
    let mut opsteps: *mut List = NIL;
    let mut btree_clauses: [*mut List; BTMaxStrategyNumber as usize + 1] = [NIL; BTMaxStrategyNumber as usize + 1];
    let mut hash_clauses: [*mut List; HTMaxStrategyNumber as usize + 1] = [NIL; HTMaxStrategyNumber as usize + 1];
    let mut i: c_int;
    let mut lc: *mut ListCell;

    i = 0;
    while i < (*part_scheme).partnatts as c_int {
        let clauselist: *mut List = *keyclauses.add(i as usize);
        let mut consider_next_key: bool = true;

        /*
         * For range partitioning, if we have no clauses for the current key,
         * we can't consider any later keys either, so we can stop here.
         */
        if (*part_scheme).strategy as u8 == PARTITION_STRATEGY_RANGE as u8
            && clauselist == NIL
        {
            break;
        }

        /*
         * For hash partitioning, if a column doesn't have the necessary
         * equality clause, there should be an IS NULL clause, otherwise
         * pruning is not possible.
         */
        if (*part_scheme).strategy as u8 == PARTITION_STRATEGY_HASH as u8
            && clauselist == NIL
            && !bms_is_member(i, nullkeys)
        {
            return NIL;
        }

        foreach!(lc, clauselist, {
            let pc: *mut PartClauseInfo = lfirst(current_cell!(lc)) as *mut PartClauseInfo;
            let mut lefttype: Oid = 0;
            let mut righttype: Oid = 0;

            /* Look up the operator's btree/hash strategy number. */
            if (*pc).op_strategy == InvalidStrategy {
                get_op_opfamily_properties(
                    (*pc).opno,
                    *(*part_scheme).partopfamily.add(i as usize),
                    false,
                    &mut (*pc).op_strategy,
                    &mut lefttype,
                    &mut righttype,
                );
            }

            match (*part_scheme).strategy as u8 {
                s if s == PARTITION_STRATEGY_LIST as u8
                    || s == PARTITION_STRATEGY_RANGE as u8 =>
                {
                    let idx = (*pc).op_strategy as usize;
                    btree_clauses[idx] = lappend(btree_clauses[idx], pc as *mut c_void);

                    /*
                     * We can't consider subsequent partition keys if the
                     * clause for the current key contains a non-inclusive
                     * operator.
                     */
                    if (*pc).op_strategy == BTLessStrategyNumber
                        || (*pc).op_strategy == BTGreaterStrategyNumber
                    {
                        consider_next_key = false;
                    }
                }
                s if s == PARTITION_STRATEGY_HASH as u8 => {
                    if (*pc).op_strategy != HTEqualStrategyNumber {
                        elog!(ERROR, "invalid clause for hash partitioning");
                    }
                    let idx = (*pc).op_strategy as usize;
                    hash_clauses[idx] = lappend(hash_clauses[idx], pc as *mut c_void);
                }
                _ => {
                    elog!(ERROR, "invalid partition strategy: {}", (*part_scheme).strategy as c_int);
                }
            }
        });

        /*
         * If we've decided that clauses for subsequent partition keys
         * wouldn't be useful for pruning, don't search any further.
         */
        if !consider_next_key {
            break;
        }

        i += 1;
    }

    /*
     * Now, we have divided clauses according to their operator strategies.
     * Check for each strategy if we can generate pruning step(s) by
     * collecting a list of expressions whose values will constitute a vector
     * that can be used as a lookup key by a partition bound searching
     * function.
     */
    match (*part_scheme).strategy as u8 {
        s if s == PARTITION_STRATEGY_LIST as u8 || s == PARTITION_STRATEGY_RANGE as u8 => {
            let eq_clauses: *mut List = btree_clauses[BTEqualStrategyNumber as usize];
            let le_clauses: *mut List = btree_clauses[BTLessEqualStrategyNumber as usize];
            let ge_clauses: *mut List = btree_clauses[BTGreaterEqualStrategyNumber as usize];
            let mut strat: StrategyNumber;

            /*
             * For each clause under consideration for a given strategy,
             * we collect expressions from clauses for earlier keys, whose
             * operator strategy is inclusive, into a list called
             * 'prefix'. By appending the clause's own expression to the
             * 'prefix', we'll generate one step using the so generated
             * vector and assign the current strategy to it.  Actually,
             * 'prefix' might contain multiple clauses for the same key,
             * in which case, we must generate steps for various
             * combinations of expressions of different keys, which
             * get_steps_using_prefix takes care of for us.
             */
            strat = 1;
            while strat <= BTMaxStrategyNumber {
                let mut lc2: *mut ListCell;
                foreach!(lc2, btree_clauses[strat as usize], {
                    let pc: *mut PartClauseInfo = lfirst(current_cell!(lc2)) as *mut PartClauseInfo;
                    let mut eq_start: *mut ListCell = list_head(eq_clauses);
                    let mut le_start: *mut ListCell = list_head(le_clauses);
                    let mut ge_start: *mut ListCell = list_head(ge_clauses);
                    let mut lc1: *mut ListCell;
                    let mut prefix: *mut List = NIL;
                    let pc_steps: *mut List;
                    let mut prefix_valid: bool = true;
                    let mut pk_has_clauses: bool;
                    let mut keyno: c_int;

                    /*
                     * If this is a clause for the first partition key,
                     * there are no preceding expressions; generate a
                     * pruning step without a prefix.
                     *
                     * Note that we pass NULL for step_nullkeys, because
                     * we don't search list/range partition bounds where
                     * some keys are NULL.
                     */
                    if (*pc).keyno == 0 {
                        Assert!((*pc).op_strategy == strat);
                        let pc_steps = get_steps_using_prefix(
                            context,
                            strat,
                            (*pc).op_is_ne,
                            (*pc).expr,
                            (*pc).cmpfn,
                            ptr::null_mut(),
                            NIL,
                        );
                        opsteps = list_concat(opsteps, pc_steps);
                        continue;
                    }

                    /*
                     * We arrange clauses into prefix in ascending order
                     * of their partition key numbers.
                     */
                    keyno = 0;
                    'keyloop: while keyno < (*pc).keyno {
                        pk_has_clauses = false;

                        /*
                         * Expressions from = clauses can always be in the
                         * prefix, provided they're from an earlier key.
                         */
                        lc1 = eq_start;
                        while !lc1.is_null() {
                            let eqpc: *mut PartClauseInfo = lfirst(lc1) as *mut PartClauseInfo;

                            if (*eqpc).keyno == keyno {
                                prefix = lappend(prefix, eqpc as *mut c_void);
                                pk_has_clauses = true;
                            } else {
                                Assert!((*eqpc).keyno > keyno);
                                break;
                            }
                            lc1 = lnext(eq_clauses, lc1);
                        }
                        eq_start = lc1;

                        /*
                         * If we're generating steps for </<= strategy, we
                         * can add other <= clauses to the prefix,
                         * provided they're from an earlier key.
                         */
                        if strat == BTLessStrategyNumber || strat == BTLessEqualStrategyNumber {
                            lc1 = le_start;
                            while !lc1.is_null() {
                                let lepc: *mut PartClauseInfo = lfirst(lc1) as *mut PartClauseInfo;

                                if (*lepc).keyno == keyno {
                                    prefix = lappend(prefix, lepc as *mut c_void);
                                    pk_has_clauses = true;
                                } else {
                                    Assert!((*lepc).keyno > keyno);
                                    break;
                                }
                                lc1 = lnext(le_clauses, lc1);
                            }
                            le_start = lc1;
                        }

                        /*
                         * If we're generating steps for >/>= strategy, we
                         * can add other >= clauses to the prefix,
                         * provided they're from an earlier key.
                         */
                        if strat == BTGreaterStrategyNumber || strat == BTGreaterEqualStrategyNumber {
                            lc1 = ge_start;
                            while !lc1.is_null() {
                                let gepc: *mut PartClauseInfo = lfirst(lc1) as *mut PartClauseInfo;

                                if (*gepc).keyno == keyno {
                                    prefix = lappend(prefix, gepc as *mut c_void);
                                    pk_has_clauses = true;
                                } else {
                                    Assert!((*gepc).keyno > keyno);
                                    break;
                                }
                                lc1 = lnext(ge_clauses, lc1);
                            }
                            ge_start = lc1;
                        }

                        /*
                         * If this key has no clauses, prefix is not valid
                         * anymore.
                         */
                        if !pk_has_clauses {
                            prefix_valid = false;
                            break 'keyloop;
                        }

                        keyno += 1;
                    }

                    /*
                     * If prefix_valid, generate PartitionPruneStepOps.
                     * Otherwise, we would not find clauses for a valid
                     * subset of the partition keys anymore for the
                     * strategy; give up on generating partition pruning
                     * steps further for the strategy.
                     *
                     * As mentioned above, if 'prefix' contains multiple
                     * expressions for the same key, the following will
                     * generate multiple steps, one for each combination
                     * of the expressions for different keys.
                     *
                     * Note that we pass NULL for step_nullkeys, because
                     * we don't search list/range partition bounds where
                     * some keys are NULL.
                     */
                    if prefix_valid {
                        Assert!((*pc).op_strategy == strat);
                        let pc_steps = get_steps_using_prefix(
                            context,
                            strat,
                            (*pc).op_is_ne,
                            (*pc).expr,
                            (*pc).cmpfn,
                            ptr::null_mut(),
                            prefix,
                        );
                        opsteps = list_concat(opsteps, pc_steps);
                    } else {
                        break;
                    }
                });
                strat += 1;
            }
        }
        s if s == PARTITION_STRATEGY_HASH as u8 => {
            let eq_clauses: *mut List = hash_clauses[HTEqualStrategyNumber as usize];

            /* For hash partitioning, we have just the = strategy. */
            if eq_clauses != NIL {
                let mut pc: *mut PartClauseInfo;
                let pc_steps: *mut List;
                let mut prefix: *mut List = NIL;
                let last_keyno: c_int;
                let mut lc1: *mut ListCell;

                /*
                 * Locate the clause for the greatest column.  This may
                 * not belong to the last partition key, but it is the
                 * clause belonging to the last partition key we found a
                 * clause for above.
                 */
                pc = llast(eq_clauses) as *mut PartClauseInfo;

                /*
                 * There might be multiple clauses which matched to that
                 * partition key; find the first such clause.  While at
                 * it, add all the clauses before that one to 'prefix'.
                 */
                last_keyno = (*pc).keyno;
                lc = list_head(eq_clauses);
                while !lc.is_null() {
                    pc = lfirst(lc) as *mut PartClauseInfo;
                    if (*pc).keyno == last_keyno {
                        break;
                    }
                    prefix = lappend(prefix, pc as *mut c_void);
                    lc = lnext(eq_clauses, lc);
                }

                /*
                 * For each clause for the "last" column, after appending
                 * the clause's own expression to the 'prefix', we'll
                 * generate one step using the so generated vector and
                 * assign = as its strategy.  Actually, 'prefix' might
                 * contain multiple clauses for the same key, in which
                 * case, we must generate steps for various combinations
                 * of expressions of different keys, which
                 * get_steps_using_prefix will take care of for us.
                 */
                lc1 = lc;
                while !lc1.is_null() {
                    pc = lfirst(lc1) as *mut PartClauseInfo;

                    /*
                     * Note that we pass nullkeys for step_nullkeys,
                     * because we need to tell hash partition bound search
                     * function which of the keys we found IS NULL clauses
                     * for.
                     */
                    Assert!((*pc).op_strategy == HTEqualStrategyNumber);
                    let pc_steps = get_steps_using_prefix(
                        context,
                        HTEqualStrategyNumber,
                        false,
                        (*pc).expr,
                        (*pc).cmpfn,
                        nullkeys,
                        prefix,
                    );
                    opsteps = list_concat(opsteps, pc_steps);
                    lc1 = lnext(eq_clauses, lc1);
                }
            }
        }
        _ => {
            elog!(ERROR, "invalid partition strategy: {}", (*part_scheme).strategy as c_int);
        }
    }

    opsteps
}

/*
 * If the partition key has a collation, then the clause must have the same
 * input collation.  If the partition key is non-collatable, we assume the
 * collation doesn't matter, because while collation wasn't considered when
 * performing partitioning, the clause still may have a collation assigned
 * due to the other input being of a collatable type.
 *
 * See also IndexCollMatchesExprColl.
 */
/* PartCollMatchesExprColl! macro defined near top of file */

/*
 * match_clause_to_partition_key
 *		Attempt to match the given 'clause' with the specified partition key.
 *
 * Return value is:
 * * PARTCLAUSE_NOMATCH if the clause doesn't match this partition key (but
 *   caller should keep trying, because it might match a subsequent key).
 *   Output arguments: none set.
 *
 * * PARTCLAUSE_MATCH_CLAUSE if there is a match.
 *   Output arguments: *pc is set to a PartClauseInfo constructed for the
 *   matched clause.
 *
 * * PARTCLAUSE_MATCH_NULLNESS if there is a match, and the matched clause was
 *   either a "a IS NULL" or "a IS NOT NULL" clause.
 *   Output arguments: *clause_is_not_null is set to false in the former case
 *   true otherwise.
 *
 * * PARTCLAUSE_MATCH_STEPS if there is a match.
 *   Output arguments: *clause_steps is set to the list of recursively
 *   generated steps for the clause.
 *
 * * PARTCLAUSE_MATCH_CONTRADICT if the clause is self-contradictory, ie
 *   it provably returns FALSE or NULL.
 *   Output arguments: none set.
 *
 * * PARTCLAUSE_UNSUPPORTED if the clause doesn't match this partition key
 *   and couldn't possibly match any other one either, due to its form or
 *   properties (such as containing a volatile function).
 *   Output arguments: none set.
 */
unsafe fn match_clause_to_partition_key(
    context: *mut GeneratePruningStepsContext,
    clause: *mut Expr,
    partkey: *mut Expr,
    partkeyidx: c_int,
    clause_is_not_null: *mut bool,
    pc: *mut *mut PartClauseInfo,
    clause_steps: *mut *mut List,
) -> PartClauseMatchStatus {
    let mut boolmatchstatus: PartClauseMatchStatus;
    let part_scheme: PartitionScheme = (*(*context).rel).part_scheme;
    let partopfamily: Oid = *(*part_scheme).partopfamily.add(partkeyidx as usize);
    let partcoll: Oid = *(*part_scheme).partcollation.add(partkeyidx as usize);
    let mut expr: *mut Expr = ptr::null_mut();
    let mut notclause: bool = false;

    /*
     * Recognize specially shaped clauses that match a Boolean partition key.
     */
    boolmatchstatus = match_boolean_partition_clause(partopfamily, clause, partkey, &mut expr, &mut notclause);

    if boolmatchstatus == PARTCLAUSE_MATCH_CLAUSE {
        let partclause: *mut PartClauseInfo;

        /*
         * For bool tests in the form of partkey IS NOT true and IS NOT false,
         * we invert these clauses.  Effectively, "partkey IS NOT true"
         * becomes "partkey IS false OR partkey IS NULL".  We do this by
         * building an OR BoolExpr and forming a clause just like that and
         * punt it off to gen_partprune_steps_internal() to generate pruning
         * steps.
         */
        if notclause {
            let mut new_clauses: *mut List;
            let mut or_clause: *mut List;
            let new_booltest: *mut BooleanTest = copyObject(clause as *mut BooleanTest);
            let nulltest: *mut NullTest;

            /* We expect 'notclause' to only be set to true for BooleanTests */
            Assert!(IsA!(clause, T_BooleanTest));

            /* reverse the bool test */
            if (*new_booltest).booltesttype == IS_NOT_TRUE {
                (*new_booltest).booltesttype = IS_FALSE;
            } else if (*new_booltest).booltesttype == IS_NOT_FALSE {
                (*new_booltest).booltesttype = IS_TRUE;
            } else {
                /*
                 * We only expect match_boolean_partition_clause to return
                 * PARTCLAUSE_MATCH_CLAUSE for IS_NOT_TRUE and IS_NOT_FALSE.
                 */
                Assert!(false);
            }

            nulltest = makeNode!(NullTest, T_NullTest);
            (*nulltest).arg = copyObject(partkey) as *mut Expr;
            (*nulltest).nulltesttype = IS_NULL;
            (*nulltest).argisrow = false;
            (*nulltest).location = -1;

            new_clauses = list_make2!(new_booltest as *mut c_void, nulltest as *mut c_void);
            or_clause = list_make1!(makeBoolExpr(BoolExprType::OR_EXPR, new_clauses, -1) as *mut c_void);

            /* Finally, generate steps */
            *clause_steps = gen_partprune_steps_internal(context, or_clause);

            if (*context).contradictory {
                return PARTCLAUSE_MATCH_CONTRADICT; /* shouldn't happen */
            } else if *clause_steps == NIL {
                return PARTCLAUSE_UNSUPPORTED; /* step generation failed */
            }
            return PARTCLAUSE_MATCH_STEPS;
        }

        partclause = palloc(size_of::<PartClauseInfo>() as Size) as *mut PartClauseInfo;
        (*partclause).keyno = partkeyidx;
        /* Do pruning with the Boolean equality operator. */
        (*partclause).opno = BooleanEqualOperator;
        (*partclause).op_is_ne = false;
        (*partclause).expr = expr;
        /* We know that expr is of Boolean type. */
        (*partclause).cmpfn = (*(*part_scheme).partsupfunc.add(partkeyidx as usize)).fn_oid;
        (*partclause).op_strategy = InvalidStrategy;

        *pc = partclause;

        return PARTCLAUSE_MATCH_CLAUSE;
    } else if boolmatchstatus == PARTCLAUSE_MATCH_NULLNESS {
        /*
         * Handle IS UNKNOWN and IS NOT UNKNOWN.  These just logically
         * translate to IS NULL and IS NOT NULL.
         */
        *clause_is_not_null = notclause;
        return PARTCLAUSE_MATCH_NULLNESS;
    } else if IsA!(clause, T_OpExpr)
        && list_length((*(clause as *mut OpExpr)).args) == 2
    {
        let opclause: *mut OpExpr = clause as *mut OpExpr;
        let mut leftop: *mut Expr;
        let mut rightop: *mut Expr;
        let mut opno: Oid;
        let mut op_lefttype: Oid = 0;
        let mut op_righttype: Oid = 0;
        let mut negator: Oid = InvalidOid;
        let mut cmpfn: Oid;
        let mut op_strategy: StrategyNumber = 0;
        let mut is_opne_listp: bool = false;
        let partclause: *mut PartClauseInfo;

        leftop = get_leftop(clause as *const Expr) as *mut Expr;
        if IsA!(leftop, T_RelabelType) {
            leftop = (*(leftop as *mut RelabelType)).arg;
        }
        rightop = get_rightop(clause as *const Expr) as *mut Expr;
        if IsA!(rightop, T_RelabelType) {
            rightop = (*(rightop as *mut RelabelType)).arg;
        }
        opno = (*opclause).opno;

        /* check if the clause matches this partition key */
        if equal(leftop as *const c_void, partkey as *const c_void) {
            expr = rightop;
        } else if equal(rightop as *const c_void, partkey as *const c_void) {
            /*
             * It's only useful if we can commute the operator to put the
             * partkey on the left.  If we can't, the clause can be deemed
             * UNSUPPORTED.  Even if its leftop matches some later partkey, we
             * now know it has Vars on the right, so it's no use.
             */
            opno = get_commutator(opno);
            if !OidIsValid(opno) {
                return PARTCLAUSE_UNSUPPORTED;
            }
            expr = leftop;
        } else {
            /* clause does not match this partition key, but perhaps next. */
            return PARTCLAUSE_NOMATCH;
        }

        /*
         * Partition key match also requires collation match.  There may be
         * multiple partkeys with the same expression but different
         * collations, so failure is NOMATCH.
         */
        if !PartCollMatchesExprColl!(partcoll, (*opclause).inputcollid) {
            return PARTCLAUSE_NOMATCH;
        }

        /*
         * See if the operator is relevant to the partitioning opfamily.
         *
         * Normally we only care about operators that are listed as being part
         * of the partitioning operator family.  But there is one exception:
         * the not-equals operators are not listed in any operator family
         * whatsoever, but their negators (equality) are.  We can use one of
         * those if we find it, but only for list partitioning.
         *
         * Note: we report NOMATCH on failure if the negator isn't the
         * equality operator for the partkey's opfamily as other partkeys may
         * have the same expression but different opfamily.  That's unlikely,
         * but not much more so than duplicate expressions with different
         * collations.
         */
        if op_in_opfamily(opno, partopfamily) {
            get_op_opfamily_properties(opno, partopfamily, false, &mut op_strategy, &mut op_lefttype, &mut op_righttype);
        } else {
            /* not supported for anything apart from LIST partitioned tables */
            if (*part_scheme).strategy as u8 != PARTITION_STRATEGY_LIST as u8 {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /* See if the negator is equality */
            negator = get_negator(opno);
            if OidIsValid(negator) && op_in_opfamily(negator, partopfamily) {
                get_op_opfamily_properties(negator, partopfamily, false, &mut op_strategy, &mut op_lefttype, &mut op_righttype);
                if op_strategy == BTEqualStrategyNumber {
                    is_opne_listp = true; /* bingo */
                }
            }

            /* Nope, it's not <> either. */
            if !is_opne_listp {
                return PARTCLAUSE_NOMATCH;
            }
        }

        /*
         * Only allow strict operators.  This will guarantee nulls are
         * filtered.  (This test is likely useless, since btree and hash
         * comparison operators are generally strict.)
         */
        if !op_strict(opno) {
            return PARTCLAUSE_UNSUPPORTED;
        }

        /*
         * OK, we have a match to the partition key and a suitable operator.
         * Examine the other argument to see if it's usable for pruning.
         *
         * In most of these cases, we can return UNSUPPORTED because the same
         * failure would occur no matter which partkey it's matched to.  (In
         * particular, now that we've successfully matched one side of the
         * opclause to a partkey, there is no chance that matching the other
         * side to another partkey will produce a usable result, since that'd
         * mean there are Vars on both sides.)
         *
         * Also, if we reject an argument for a target-dependent reason, set
         * appropriate fields of *context to report that.  We postpone these
         * tests until after matching the partkey and the operator, so as to
         * reduce the odds of setting the context fields for clauses that do
         * not end up contributing to pruning steps.
         *
         * First, check for non-Const argument.  (We assume that any immutable
         * subexpression will have been folded to a Const already.)
         */
        if !IsA!(expr, T_Const) {
            let paramids: *mut Bitmapset;

            /*
             * When pruning in the planner, we only support pruning using
             * comparisons to constants.  We cannot prune on the basis of
             * anything that's not immutable.  (Note that has_mutable_arg and
             * has_exec_param do not get set for this target value.)
             */
            if (*context).target == PARTTARGET_PLANNER {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * We can never prune using an expression that contains Vars.
             */
            if contain_var_clause(expr as *mut Node) {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * And we must reject anything containing a volatile function.
             * Stable functions are OK though.
             */
            if contain_volatile_functions(expr as *mut Node) {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * See if there are any exec Params.  If so, we can only use this
             * expression during per-scan pruning.
             */
            let paramids = pull_exec_paramids(expr);
            if !bms_is_empty(paramids) {
                (*context).has_exec_param = true;
                if (*context).target != PARTTARGET_EXEC {
                    return PARTCLAUSE_UNSUPPORTED;
                }
            } else {
                /* It's potentially usable, but mutable */
                (*context).has_mutable_arg = true;
            }
        }

        /*
         * Check whether the comparison operator itself is immutable.  (We
         * assume anything that's in a btree or hash opclass is at least
         * stable, but we need to check for immutability.)
         */
        if op_volatile(opno) != PROVOLATILE_IMMUTABLE {
            (*context).has_mutable_op = true;

            /*
             * When pruning in the planner, we cannot prune with mutable
             * operators.
             */
            if (*context).target == PARTTARGET_PLANNER {
                return PARTCLAUSE_UNSUPPORTED;
            }
        }

        /*
         * Now find the procedure to use, based on the types.  If the clause's
         * other argument is of the same type as the partitioning opclass's
         * declared input type, we can use the procedure cached in
         * PartitionKey.  If not, search for a cross-type one in the same
         * opfamily; if one doesn't exist, report no match.
         */
        if op_righttype == *(*part_scheme).partopcintype.add(partkeyidx as usize) {
            cmpfn = (*(*part_scheme).partsupfunc.add(partkeyidx as usize)).fn_oid;
        } else {
            match (*part_scheme).strategy as u8 {
                /*
                 * For range and list partitioning, we need the ordering
                 * procedure with lefttype being the partition key's type,
                 * and righttype the clause's operator's right type.
                 */
                s if s == PARTITION_STRATEGY_LIST as u8 || s == PARTITION_STRATEGY_RANGE as u8 => {
                    cmpfn = get_opfamily_proc(
                        *(*part_scheme).partopfamily.add(partkeyidx as usize),
                        *(*part_scheme).partopcintype.add(partkeyidx as usize),
                        op_righttype,
                        BTORDER_PROC,
                    );
                }
                /*
                 * For hash partitioning, we need the hashing procedure
                 * for the clause's type.
                 */
                s if s == PARTITION_STRATEGY_HASH as u8 => {
                    cmpfn = get_opfamily_proc(
                        *(*part_scheme).partopfamily.add(partkeyidx as usize),
                        op_righttype,
                        op_righttype,
                        HASHEXTENDED_PROC,
                    );
                }
                _ => {
                    elog!(ERROR, "invalid partition strategy: {}", (*part_scheme).strategy as c_int);
                    cmpfn = InvalidOid; /* keep compiler quiet */
                }
            }

            if !OidIsValid(cmpfn) {
                return PARTCLAUSE_NOMATCH;
            }
        }

        /*
         * Build the clause, passing the negator if applicable.
         */
        let partclause = palloc(size_of::<PartClauseInfo>() as Size) as *mut PartClauseInfo;
        (*partclause).keyno = partkeyidx;
        if is_opne_listp {
            Assert!(OidIsValid(negator));
            (*partclause).opno = negator;
            (*partclause).op_is_ne = true;
            (*partclause).op_strategy = InvalidStrategy;
        } else {
            (*partclause).opno = opno;
            (*partclause).op_is_ne = false;
            (*partclause).op_strategy = op_strategy;
        }
        (*partclause).expr = expr;
        (*partclause).cmpfn = cmpfn;

        *pc = partclause;

        return PARTCLAUSE_MATCH_CLAUSE;
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        let saop: *mut ScalarArrayOpExpr = clause as *mut ScalarArrayOpExpr;
        let saop_op: Oid = (*saop).opno;
        let saop_coll: Oid = (*saop).inputcollid;
        let mut leftop: *mut Expr = linitial((*saop).args) as *mut Expr;
        let rightop: *mut Expr = lsecond((*saop).args) as *mut Expr;
        let mut elem_exprs: *mut List;
        let mut elem_clauses: *mut List;
        let mut lc1: *mut ListCell;

        if IsA!(leftop, T_RelabelType) {
            leftop = (*(leftop as *mut RelabelType)).arg;
        }

        /* check if the LHS matches this partition key */
        if !equal(leftop as *const c_void, partkey as *const c_void)
            || !PartCollMatchesExprColl!(partcoll, (*saop).inputcollid)
        {
            return PARTCLAUSE_NOMATCH;
        }

        /*
         * See if the operator is relevant to the partitioning opfamily.
         *
         * In case of NOT IN (..), we get a '<>', which we handle if list
         * partitioning is in use and we're able to confirm that it's negator
         * is a btree equality operator belonging to the partitioning operator
         * family.  As above, report NOMATCH for non-matching operator.
         */
        if !op_in_opfamily(saop_op, partopfamily) {
            let negator2: Oid;

            if (*part_scheme).strategy as u8 != PARTITION_STRATEGY_LIST as u8 {
                return PARTCLAUSE_NOMATCH;
            }

            negator2 = get_negator(saop_op);
            if OidIsValid(negator2) && op_in_opfamily(negator2, partopfamily) {
                let mut strategy: StrategyNumber = 0;
                let mut lefttype2: Oid = 0;
                let mut righttype2: Oid = 0;

                get_op_opfamily_properties(negator2, partopfamily, false, &mut strategy, &mut lefttype2, &mut righttype2);
                if strategy != BTEqualStrategyNumber {
                    return PARTCLAUSE_NOMATCH;
                }
            } else {
                return PARTCLAUSE_NOMATCH; /* no useful negator */
            }
        }

        /*
         * Only allow strict operators.  This will guarantee nulls are
         * filtered.  (This test is likely useless, since btree and hash
         * comparison operators are generally strict.)
         */
        if !op_strict(saop_op) {
            return PARTCLAUSE_UNSUPPORTED;
        }

        /*
         * OK, we have a match to the partition key and a suitable operator.
         * Examine the array argument to see if it's usable for pruning.  This
         * is identical to the logic for a plain OpExpr.
         */
        if !IsA!(rightop, T_Const) {
            let paramids: *mut Bitmapset;

            /*
             * When pruning in the planner, we only support pruning using
             * comparisons to constants.  We cannot prune on the basis of
             * anything that's not immutable.  (Note that has_mutable_arg and
             * has_exec_param do not get set for this target value.)
             */
            if (*context).target == PARTTARGET_PLANNER {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * We can never prune using an expression that contains Vars.
             */
            if contain_var_clause(rightop as *mut Node) {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * And we must reject anything containing a volatile function.
             * Stable functions are OK though.
             */
            if contain_volatile_functions(rightop as *mut Node) {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * See if there are any exec Params.  If so, we can only use this
             * expression during per-scan pruning.
             */
            let paramids = pull_exec_paramids(rightop);
            if !bms_is_empty(paramids) {
                (*context).has_exec_param = true;
                if (*context).target != PARTTARGET_EXEC {
                    return PARTCLAUSE_UNSUPPORTED;
                }
            } else {
                /* It's potentially usable, but mutable */
                (*context).has_mutable_arg = true;
            }
        }

        /*
         * Check whether the comparison operator itself is immutable.  (We
         * assume anything that's in a btree or hash opclass is at least
         * stable, but we need to check for immutability.)
         */
        if op_volatile(saop_op) != PROVOLATILE_IMMUTABLE {
            (*context).has_mutable_op = true;

            /*
             * When pruning in the planner, we cannot prune with mutable
             * operators.
             */
            if (*context).target == PARTTARGET_PLANNER {
                return PARTCLAUSE_UNSUPPORTED;
            }
        }

        /*
         * Examine the contents of the array argument.
         */
        elem_exprs = NIL;
        if IsA!(rightop, T_Const) {
            /*
             * For a constant array, convert the elements to a list of Const
             * nodes, one for each array element (excepting nulls).
             */
            let arr: *mut Const = rightop as *mut Const;
            let arrval: *mut ArrayType;
            let mut elemlen: i16 = 0;
            let mut elembyval: bool = false;
            let mut elemalign: c_char = 0;
            let mut elem_values: *mut Datum = ptr::null_mut();
            let mut elem_nulls: *mut bool = ptr::null_mut();
            let mut num_elems: c_int = 0;
            let mut ii: c_int;

            /* If the array itself is null, the saop returns null */
            if (*arr).constisnull {
                return PARTCLAUSE_MATCH_CONTRADICT;
            }

            arrval = DatumGetArrayTypeP((*arr).constvalue);
            get_typlenbyvalalign(ARR_ELEMTYPE(arrval), &mut elemlen, &mut elembyval, &mut elemalign);
            deconstruct_array(
                arrval,
                ARR_ELEMTYPE(arrval),
                elemlen,
                elembyval,
                elemalign,
                &mut elem_values,
                &mut elem_nulls,
                &mut num_elems,
            );
            ii = 0;
            while ii < num_elems {
                let elem_expr: *mut Const;

                /*
                 * A null array element must lead to a null comparison result,
                 * since saop_op is known strict.  We can ignore it in the
                 * useOr case, but otherwise it implies self-contradiction.
                 */
                if *elem_nulls.add(ii as usize) {
                    if (*saop).useOr {
                        ii += 1;
                        continue;
                    }
                    return PARTCLAUSE_MATCH_CONTRADICT;
                }

                let elem_expr = makeConst(
                    ARR_ELEMTYPE(arrval),
                    -1,
                    (*arr).constcollid,
                    elemlen,
                    *elem_values.add(ii as usize),
                    false,
                    elembyval,
                );
                elem_exprs = lappend(elem_exprs, elem_expr as *mut c_void);
                ii += 1;
            }
        } else if IsA!(rightop, T_ArrayExpr) {
            let arrexpr: *mut ArrayExpr = castNode!(ArrayExpr, T_ArrayExpr, rightop as *mut Node);

            /*
             * For a nested ArrayExpr, we don't know how to get the actual
             * scalar values out into a flat list, so we give up doing
             * anything with this ScalarArrayOpExpr.
             */
            if (*arrexpr).multidims {
                return PARTCLAUSE_UNSUPPORTED;
            }

            /*
             * Otherwise, we can just use the list of element values.
             */
            elem_exprs = (*arrexpr).elements;
        } else {
            /* Give up on any other clause types. */
            return PARTCLAUSE_UNSUPPORTED;
        }

        /*
         * Now generate a list of clauses, one for each array element, of the
         * form leftop saop_op elem_expr
         */
        elem_clauses = NIL;
        foreach!(lc1, elem_exprs, {
            let elem_clause: *mut Expr;

            let elem_clause = make_opclause(saop_op, BOOLOID, false, leftop, lfirst(current_cell!(lc1)) as *mut Expr, InvalidOid, saop_coll);
            elem_clauses = lappend(elem_clauses, elem_clause as *mut c_void);
        });

        /*
         * If we have an ANY clause and multiple elements, now turn the list
         * of clauses into an OR expression.
         */
        if (*saop).useOr && list_length(elem_clauses) > 1 {
            elem_clauses = list_make1!(makeBoolExpr(BoolExprType::OR_EXPR, elem_clauses, -1) as *mut c_void);
        }

        /* Finally, generate steps */
        *clause_steps = gen_partprune_steps_internal(context, elem_clauses);
        if (*context).contradictory {
            return PARTCLAUSE_MATCH_CONTRADICT;
        } else if *clause_steps == NIL {
            return PARTCLAUSE_UNSUPPORTED; /* step generation failed */
        }
        return PARTCLAUSE_MATCH_STEPS;
    } else if IsA!(clause, T_NullTest) {
        let nulltest: *mut NullTest = clause as *mut NullTest;
        let mut arg: *mut Expr = (*nulltest).arg;

        if IsA!(arg, T_RelabelType) {
            arg = (*(arg as *mut RelabelType)).arg;
        }

        /* Does arg match with this partition key column? */
        if !equal(arg as *const c_void, partkey as *const c_void) {
            return PARTCLAUSE_NOMATCH;
        }

        *clause_is_not_null = (*nulltest).nulltesttype == IS_NOT_NULL;

        return PARTCLAUSE_MATCH_NULLNESS;
    }

    /*
     * If we get here then the return value depends on the result of the
     * match_boolean_partition_clause call above.  If the call returned
     * PARTCLAUSE_UNSUPPORTED then we're either not dealing with a bool qual
     * or the bool qual is not suitable for pruning.  Since the qual didn't
     * match up to any of the other qual types supported here, then trying to
     * match it against any other partition key is a waste of time, so just
     * return PARTCLAUSE_UNSUPPORTED.  If the qual just couldn't be matched to
     * this partition key, then it may match another, so return
     * PARTCLAUSE_NOMATCH.  The only other value that
     * match_boolean_partition_clause can return is PARTCLAUSE_MATCH_CLAUSE,
     * and since that value was already dealt with above, then we can just
     * return boolmatchstatus.
     */
    boolmatchstatus
}

/*
 * get_steps_using_prefix
 *		Generate a list of PartitionPruneStepOps based on the given input.
 *
 * 'step_lastexpr' and 'step_lastcmpfn' are the Expr and comparison function
 * belonging to the final partition key that we have a clause for.  'prefix'
 * is a list of PartClauseInfos for partition key numbers prior to the given
 * 'step_lastexpr' and 'step_lastcmpfn'.  'prefix' may contain multiple
 * PartClauseInfos belonging to a single partition key.  We will generate a
 * PartitionPruneStepOp for each combination of the given PartClauseInfos
 * using, at most, one PartClauseInfo per partition key.
 *
 * For LIST and RANGE partitioned tables, callers must ensure that
 * step_nullkeys is NULL, and that prefix contains at least one clause for
 * each of the partition keys prior to the key that 'step_lastexpr' and
 * 'step_lastcmpfn' belong to.
 *
 * For HASH partitioned tables, callers must ensure that 'prefix' contains at
 * least one clause for each of the partition keys apart from the final key
 * (the expr and comparison function for the final key are in 'step_lastexpr'
 * and 'step_lastcmpfn').  A bit set in step_nullkeys can substitute clauses
 * in the 'prefix' list for any given key.  If a bit is set in 'step_nullkeys'
 * for a given key, then there must be no PartClauseInfo for that key in the
 * 'prefix' list.
 *
 * For each of the above cases, callers must ensure that PartClauseInfos in
 * 'prefix' are sorted in ascending order of keyno.
 */
unsafe fn get_steps_using_prefix(
    context: *mut GeneratePruningStepsContext,
    step_opstrategy: StrategyNumber,
    step_op_is_ne: bool,
    step_lastexpr: *mut Expr,
    step_lastcmpfn: Oid,
    step_nullkeys: *mut Bitmapset,
    prefix: *mut List,
) -> *mut List {
    /* step_nullkeys must be empty for RANGE and LIST partitioned tables */
    Assert!(
        step_nullkeys.is_null()
            || (*(*(*context).rel).part_scheme).strategy as u8 == PARTITION_STRATEGY_HASH as u8
    );

    /*
     * No recursive processing is required when 'prefix' is an empty list.
     * This occurs when there is only 1 partition key column.
     */
    if prefix == NIL {
        let step: *mut PartitionPruneStep;

        let step = gen_prune_step_op(
            context,
            step_opstrategy,
            step_op_is_ne,
            list_make1!(step_lastexpr as *mut c_void),
            list_make1_oid(step_lastcmpfn),
            step_nullkeys,
        );
        return list_make1!(step as *mut c_void);
    }

    /* Recurse to generate steps for every combination of clauses. */
    get_steps_using_prefix_recurse(
        context,
        step_opstrategy,
        step_op_is_ne,
        step_lastexpr,
        step_lastcmpfn,
        step_nullkeys,
        prefix,
        list_head(prefix),
        NIL,
        NIL,
    )
}

/*
 * get_steps_using_prefix_recurse
 *		Generate and return a list of PartitionPruneStepOps using the 'prefix'
 *		list of PartClauseInfos starting at the 'start' cell.
 *
 * When 'prefix' contains multiple PartClauseInfos for a single partition key
 * we create a PartitionPruneStepOp for each combination of duplicated
 * PartClauseInfos.  The returned list will contain a PartitionPruneStepOp
 * for each unique combination of input PartClauseInfos containing at most one
 * PartClauseInfo per partition key.
 *
 * 'prefix' is the input list of PartClauseInfos sorted by keyno.
 * 'start' marks the cell that searching the 'prefix' list should start from.
 * 'step_exprs' and 'step_cmpfns' each contains the expressions and cmpfns
 * we've generated so far from the clauses for the previous part keys.
 */
unsafe fn get_steps_using_prefix_recurse(
    context: *mut GeneratePruningStepsContext,
    step_opstrategy: StrategyNumber,
    step_op_is_ne: bool,
    step_lastexpr: *mut Expr,
    step_lastcmpfn: Oid,
    step_nullkeys: *mut Bitmapset,
    prefix: *mut List,
    start: *mut ListCell,
    step_exprs: *mut List,
    step_cmpfns: *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;
    let cur_keyno: c_int;
    let final_keyno: c_int;

    /* Actually, recursion would be limited by PARTITION_MAX_KEYS. */
    check_stack_depth();

    Assert!(!start.is_null());
    cur_keyno = (*(lfirst(start) as *mut PartClauseInfo)).keyno;
    final_keyno = (*(llast(prefix) as *mut PartClauseInfo)).keyno;

    /* Check if we need to recurse. */
    if cur_keyno < final_keyno {
        let mut pc: *mut PartClauseInfo;
        let mut next_start: *mut ListCell;

        /*
         * Find the first PartClauseInfo belonging to the next partition key,
         * the next recursive call must start iteration of the prefix list
         * from that point.
         */
        lc = start;
        while !lc.is_null() {
            pc = lfirst(lc) as *mut PartClauseInfo;

            if (*pc).keyno > cur_keyno {
                break;
            }
            lc = lnext(prefix, lc);
        }

        /* record where to start iterating in the next recursive call */
        next_start = lc;

        /*
         * For each PartClauseInfo with keyno set to cur_keyno, add its expr
         * and cmpfn to step_exprs and step_cmpfns, respectively, and recurse
         * using 'next_start' as the starting point in the 'prefix' list.
         */
        lc = start;
        while !lc.is_null() {
            let moresteps: *mut List;
            let mut step_exprs1: *mut List;
            let mut step_cmpfns1: *mut List;

            pc = lfirst(lc) as *mut PartClauseInfo;
            if (*pc).keyno == cur_keyno {
                /* Leave the original step_exprs unmodified. */
                step_exprs1 = list_copy(step_exprs);
                step_exprs1 = lappend(step_exprs1, (*pc).expr as *mut c_void);

                /* Leave the original step_cmpfns unmodified. */
                step_cmpfns1 = list_copy(step_cmpfns);
                step_cmpfns1 = lappend_oid(step_cmpfns1, (*pc).cmpfn);
            } else {
                /* check the 'prefix' list is sorted correctly */
                Assert!((*pc).keyno > cur_keyno);
                break;
            }

            let moresteps = get_steps_using_prefix_recurse(
                context,
                step_opstrategy,
                step_op_is_ne,
                step_lastexpr,
                step_lastcmpfn,
                step_nullkeys,
                prefix,
                next_start,
                step_exprs1,
                step_cmpfns1,
            );
            result = list_concat(result, moresteps);

            list_free(step_exprs1);
            list_free(step_cmpfns1);

            lc = lnext(prefix, lc);
        }
    } else {
        /*
         * End the current recursion cycle and start generating steps, one for
         * each clause with cur_keyno, which is all clauses from here onward
         * till the end of the list.  Note that for hash partitioning,
         * step_nullkeys is allowed to be non-empty, in which case step_exprs
         * would only contain expressions for the partition keys that are not
         * specified in step_nullkeys.
         */
        Assert!(
            list_length(step_exprs) == cur_keyno || !bms_is_empty(step_nullkeys)
        );

        /*
         * Note also that for hash partitioning, each partition key should
         * have either equality clauses or an IS NULL clause, so if a
         * partition key doesn't have an expression, it would be specified in
         * step_nullkeys.
         */
        Assert!(
            (*(*(*context).rel).part_scheme).strategy as u8 != PARTITION_STRATEGY_HASH as u8
                || list_length(step_exprs) + 2 + bms_num_members(step_nullkeys)
                    == (*(*(*context).rel).part_scheme).partnatts as c_int
        );
        lc = start;
        while !lc.is_null() {
            let pc: *mut PartClauseInfo = lfirst(lc) as *mut PartClauseInfo;
            let step: *mut PartitionPruneStep;
            let mut step_exprs1: *mut List;
            let mut step_cmpfns1: *mut List;

            Assert!((*pc).keyno == cur_keyno);

            /* Leave the original step_exprs unmodified. */
            step_exprs1 = list_copy(step_exprs);
            step_exprs1 = lappend(step_exprs1, (*pc).expr as *mut c_void);
            step_exprs1 = lappend(step_exprs1, step_lastexpr as *mut c_void);

            /* Leave the original step_cmpfns unmodified. */
            step_cmpfns1 = list_copy(step_cmpfns);
            step_cmpfns1 = lappend_oid(step_cmpfns1, (*pc).cmpfn);
            step_cmpfns1 = lappend_oid(step_cmpfns1, step_lastcmpfn);

            let step = gen_prune_step_op(
                context,
                step_opstrategy,
                step_op_is_ne,
                step_exprs1,
                step_cmpfns1,
                step_nullkeys,
            );
            result = lappend(result, step as *mut c_void);

            lc = lnext(prefix, lc);
        }
    }

    result
}

/*
 * get_matching_hash_bounds
 *		Determine offset of the hash bound matching the specified values,
 *		considering that all the non-null values come from clauses containing
 *		a compatible hash equality operator and any keys that are null come
 *		from an IS NULL clause.
 *
 * Generally this function will return a single matching bound offset,
 * although if a partition has not been setup for a given modulus then we may
 * return no matches.  If the number of clauses found don't cover the entire
 * partition key, then we'll need to return all offsets.
 *
 * 'opstrategy' if non-zero must be HTEqualStrategyNumber.
 *
 * 'values' contains Datums indexed by the partition key to use for pruning.
 *
 * 'nvalues', the number of Datums in the 'values' array.
 *
 * 'partsupfunc' contains partition hashing functions that can produce correct
 * hash for the type of the values contained in 'values'.
 *
 * 'nullkeys' is the set of partition keys that are null.
 */
unsafe fn get_matching_hash_bounds(
    context: *mut PartitionPruneContext,
    opstrategy: StrategyNumber,
    values: *mut Datum,
    nvalues: c_int,
    partsupfunc: *mut FmgrInfo,
    nullkeys: *mut Bitmapset,
) -> *mut PruneStepResult {
    let result: *mut PruneStepResult =
        palloc0(size_of::<PruneStepResult>() as Size) as *mut PruneStepResult;
    let boundinfo: PartitionBoundInfo = (*context).boundinfo;
    let partindices: *mut c_int = (*boundinfo).indexes;
    let partnatts: c_int = (*context).partnatts;
    let mut isnull: [bool; PARTITION_MAX_KEYS] = [false; PARTITION_MAX_KEYS];
    let mut i: c_int;
    let rowHash: u64;
    let greatest_modulus: c_int;
    let partcollation: *mut Oid = (*context).partcollation;

    Assert!((*context).strategy as u8 == PARTITION_STRATEGY_HASH as u8);

    /*
     * For hash partitioning we can only perform pruning based on equality
     * clauses to the partition key or IS NULL clauses.  We also can only
     * prune if we got values for all keys.
     */
    if nvalues + bms_num_members(nullkeys) == partnatts {
        /*
         * If there are any values, they must have come from clauses
         * containing an equality operator compatible with hash partitioning.
         */
        Assert!(opstrategy == HTEqualStrategyNumber || nvalues == 0);

        i = 0;
        while i < partnatts {
            isnull[i as usize] = bms_is_member(i, nullkeys);
            i += 1;
        }

        let rowHash = compute_partition_hash_value(
            partnatts,
            partsupfunc,
            partcollation,
            values,
            isnull.as_mut_ptr(),
        );

        greatest_modulus = (*boundinfo).nindexes;
        if *partindices.add((rowHash % greatest_modulus as u64) as usize) >= 0 {
            (*result).bound_offsets =
                bms_make_singleton((rowHash % greatest_modulus as u64) as c_int);
        }
    } else {
        /* Report all valid offsets into the boundinfo->indexes array. */
        (*result).bound_offsets =
            bms_add_range(ptr::null_mut(), 0, (*boundinfo).nindexes - 1);
    }

    /*
     * There is neither a special hash null partition or the default hash
     * partition.
     */
    (*result).scan_null = false;
    (*result).scan_default = false;

    result
}

/*
 * get_matching_list_bounds
 *		Determine the offsets of list bounds matching the specified value,
 *		according to the semantics of the given operator strategy
 *
 * scan_default will be set in the returned struct, if the default partition
 * needs to be scanned, provided one exists at all.  scan_null will be set if
 * the special null-accepting partition needs to be scanned.
 *
 * 'opstrategy' if non-zero must be a btree strategy number.
 *
 * 'value' contains the value to use for pruning.
 *
 * 'nvalues', if non-zero, should be exactly 1, because of list partitioning.
 *
 * 'partsupfunc' contains the list partitioning comparison function to be used
 * to perform partition_list_bsearch
 *
 * 'nullkeys' is the set of partition keys that are null.
 */
unsafe fn get_matching_list_bounds(
    context: *mut PartitionPruneContext,
    opstrategy: StrategyNumber,
    value: Datum,
    nvalues: c_int,
    partsupfunc: *mut FmgrInfo,
    nullkeys: *mut Bitmapset,
) -> *mut PruneStepResult {
    let result: *mut PruneStepResult =
        palloc0(size_of::<PruneStepResult>() as Size) as *mut PruneStepResult;
    let boundinfo: PartitionBoundInfo = (*context).boundinfo;
    let mut off: c_int;
    let mut minoff: c_int;
    let mut maxoff: c_int;
    let mut is_equal: bool = false;
    let mut inclusive: bool = false;
    let partcollation: *mut Oid = (*context).partcollation;

    Assert!((*context).strategy as u8 == PARTITION_STRATEGY_LIST as u8);
    Assert!((*context).partnatts == 1);

    (*result).scan_null = false;
    (*result).scan_default = false;

    if !bms_is_empty(nullkeys) {
        /*
         * Nulls may exist in only one partition - the partition whose
         * accepted set of values includes null or the default partition if
         * the former doesn't exist.
         */
        if partition_bound_accepts_nulls(boundinfo) {
            (*result).scan_null = true;
        } else {
            (*result).scan_default = partition_bound_has_default(boundinfo);
        }
        return result;
    }

    /*
     * If there are no datums to compare keys with, but there are partitions,
     * just return the default partition if one exists.
     */
    if (*boundinfo).ndatums == 0 {
        (*result).scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    minoff = 0;
    maxoff = (*boundinfo).ndatums - 1;

    /*
     * If there are no values to compare with the datums in boundinfo, it
     * means the caller asked for partitions for all non-null datums.  Add
     * indexes of *all* partitions, including the default if any.
     */
    if nvalues == 0 {
        Assert!((*boundinfo).ndatums > 0);
        (*result).bound_offsets = bms_add_range(ptr::null_mut(), 0, (*boundinfo).ndatums - 1);
        (*result).scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    /* Special case handling of values coming from a <> operator clause. */
    if opstrategy == InvalidStrategy {
        /*
         * First match to all bounds.  We'll remove any matching datums below.
         */
        Assert!((*boundinfo).ndatums > 0);
        (*result).bound_offsets = bms_add_range(ptr::null_mut(), 0, (*boundinfo).ndatums - 1);

        off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
        if off >= 0 && is_equal {

            /* We have a match. Remove from the result. */
            Assert!(*(*boundinfo).indexes.add(off as usize) >= 0);
            (*result).bound_offsets = bms_del_member((*result).bound_offsets, off);
        }

        /* Always include the default partition if any. */
        (*result).scan_default = partition_bound_has_default(boundinfo);

        return result;
    }

    /*
     * With range queries, always include the default list partition, because
     * list partitions divide the key space in a discontinuous manner, not all
     * values in the given range will have a partition assigned.  This may not
     * technically be true for some data types (e.g. integer types), however,
     * we currently lack any sort of infrastructure to provide us with proofs
     * that would allow us to do anything smarter here.
     */
    if opstrategy != BTEqualStrategyNumber {
        (*result).scan_default = partition_bound_has_default(boundinfo);
    }

    match opstrategy {
        s if s == BTEqualStrategyNumber => {
            off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
            if off >= 0 && is_equal {
                Assert!(*(*boundinfo).indexes.add(off as usize) >= 0);
                (*result).bound_offsets = bms_make_singleton(off);
            } else {
                (*result).scan_default = partition_bound_has_default(boundinfo);
            }
            return result;
        }
        s if s == BTGreaterEqualStrategyNumber => {
            inclusive = true;
            /* fall through to BTGreaterStrategyNumber logic */
            off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
            if off >= 0 {
                /* We don't want the matched datum to be in the result. */
                if !is_equal || !inclusive {
                    off += 1;
                }
            } else {
                /*
                 * This case means all partition bounds are greater, which in
                 * turn means that all partitions satisfy this key.
                 */
                off = 0;
            }

            /*
             * off is greater than the numbers of datums we have partitions
             * for.  The only possible partition that could contain a match is
             * the default partition, but we must've set context->scan_default
             * above anyway if one exists.
             */
            if off > (*boundinfo).ndatums - 1 {
                return result;
            }

            minoff = off;
        }
        s if s == BTGreaterStrategyNumber => {
            off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
            if off >= 0 {
                if !is_equal || !inclusive {
                    off += 1;
                }
            } else {
                off = 0;
            }
            if off > (*boundinfo).ndatums - 1 {
                return result;
            }
            minoff = off;
        }
        s if s == BTLessEqualStrategyNumber => {
            inclusive = true;
            /* fall through to BTLessStrategyNumber logic */
            off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
            if off >= 0 && is_equal && !inclusive {
                off -= 1;
            }

            /*
             * off is smaller than the datums of all non-default partitions.
             * The only possible partition that could contain a match is the
             * default partition, but we must've set context->scan_default
             * above anyway if one exists.
             */
            if off < 0 {
                return result;
            }

            maxoff = off;
        }
        s if s == BTLessStrategyNumber => {
            off = partition_list_bsearch(partsupfunc, partcollation, boundinfo, value, &mut is_equal);
            if off >= 0 && is_equal && !inclusive {
                off -= 1;
            }
            if off < 0 {
                return result;
            }
            maxoff = off;
        }
        _ => {
            elog!(ERROR, "invalid strategy number {}", opstrategy);
        }
    }

    Assert!(minoff >= 0 && maxoff >= 0);
    (*result).bound_offsets = bms_add_range(ptr::null_mut(), minoff, maxoff);
    result
}


/*
 * get_matching_range_bounds
 *		Determine the offsets of range bounds matching the specified values,
 *		according to the semantics of the given operator strategy
 *
 * Each datum whose offset is in result is to be treated as the upper bound of
 * the partition that will contain the desired values.
 *
 * scan_default is set in the returned struct if a default partition exists
 * and we're absolutely certain that it needs to be scanned.  We do *not* set
 * it just because values match portions of the key space uncovered by
 * partitions other than default (space which we normally assume to belong to
 * the default partition): the final set of bounds obtained after combining
 * multiple pruning steps might exclude it, so we infer its inclusion
 * elsewhere.
 *
 * 'opstrategy' must be a btree strategy number.
 *
 * 'values' contains Datums indexed by the partition key to use for pruning.
 *
 * 'nvalues', number of Datums in 'values' array. Must be <= context->partnatts.
 *
 * 'partsupfunc' contains the range partitioning comparison functions to be
 * used to perform partition_range_datum_bsearch or partition_rbound_datum_cmp
 * using.
 *
 * 'nullkeys' is the set of partition keys that are null.
 */
unsafe fn get_matching_range_bounds(
    context: *mut PartitionPruneContext,
    opstrategy: StrategyNumber,
    values: *mut Datum,
    nvalues: c_int,
    partsupfunc: *mut FmgrInfo,
    nullkeys: *mut Bitmapset,
) -> *mut PruneStepResult {
    let result: *mut PruneStepResult =
        palloc0(size_of::<PruneStepResult>() as Size) as *mut PruneStepResult;
    let boundinfo: PartitionBoundInfo = (*context).boundinfo;
    let partcollation: *mut Oid = (*context).partcollation;
    let partnatts: c_int = (*context).partnatts;
    let partindices: *mut c_int = (*boundinfo).indexes;
    let mut off: c_int;
    let mut minoff: c_int;
    let mut maxoff: c_int;
    let mut is_equal: bool = false;
    let mut inclusive: bool = false;

    Assert!((*context).strategy as u8 == PARTITION_STRATEGY_RANGE as u8);
    Assert!(nvalues <= partnatts);

    (*result).scan_null = false;
    (*result).scan_default = false;

    /*
     * If there are no datums to compare keys with, or if we got an IS NULL
     * clause just return the default partition, if it exists.
     */
    if (*boundinfo).ndatums == 0 || !bms_is_empty(nullkeys) {
        (*result).scan_default = partition_bound_has_default(boundinfo);
        return result;
    }

    minoff = 0;
    maxoff = (*boundinfo).ndatums;

    /*
     * If there are no values to compare with the datums in boundinfo, it
     * means the caller asked for partitions for all non-null datums.  Add
     * indexes of *all* partitions, including the default partition if one
     * exists.
     */
    if nvalues == 0 {
        /* ignore key space not covered by any partitions */
        if *partindices.add(minoff as usize) < 0 {
            minoff += 1;
        }
        if *partindices.add(maxoff as usize) < 0 {
            maxoff -= 1;
        }

        (*result).scan_default = partition_bound_has_default(boundinfo);
        Assert!(*partindices.add(minoff as usize) >= 0 && *partindices.add(maxoff as usize) >= 0);
        (*result).bound_offsets = bms_add_range(ptr::null_mut(), minoff, maxoff);

        return result;
    }

    /*
     * If the query does not constrain all key columns, we'll need to scan the
     * default partition, if any.
     */
    if nvalues < partnatts {
        (*result).scan_default = partition_bound_has_default(boundinfo);
    }

    match opstrategy {
        s if s == BTEqualStrategyNumber => {
            /* Look for the smallest bound that is = lookup value. */
            off = partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo, nvalues, values, &mut is_equal);

            if off >= 0 && is_equal {
                if nvalues == partnatts {
                    /* There can only be zero or one matching partition. */
                    (*result).bound_offsets = bms_make_singleton(off + 1);
                    return result;
                } else {
                    let saved_off: c_int = off;

                    /*
                     * Since the lookup value contains only a prefix of keys,
                     * we must find other bounds that may also match the
                     * prefix.  partition_range_datum_bsearch() returns the
                     * offset of one of them, find others by checking adjacent
                     * bounds.
                     */

                    /*
                     * First find greatest bound that's smaller than the
                     * lookup value.
                     */
                    while off >= 1 {
                        let cmpval: i32;

                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add((off - 1) as usize),
                            *(*boundinfo).kind.add((off - 1) as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }
                        off -= 1;
                    }

                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );

                    /*
                     * We can treat 'off' as the offset of the smallest bound
                     * to be included in the result, if we know it is the
                     * upper bound of the partition in which the lookup value
                     * could possibly exist.  One case it couldn't is if the
                     * bound, or precisely the matched portion of its prefix,
                     * is not inclusive.
                     */
                    if *(*(*boundinfo).kind.add(off as usize)).add(nvalues as usize)
                        == crate::nodes::parsenodes::PartitionRangeDatumKind::PARTITION_RANGE_DATUM_MINVALUE
                    {
                        off += 1;
                    }

                    minoff = off;

                    /*
                     * Now find smallest bound that's greater than the lookup
                     * value.
                     */
                    off = saved_off;
                    while off < (*boundinfo).ndatums - 1 {
                        let cmpval: i32;

                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add((off + 1) as usize),
                            *(*boundinfo).kind.add((off + 1) as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }
                        off += 1;
                    }

                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );

                    /*
                     * off + 1, then would be the offset of the greatest bound
                     * to be included in the result.
                     */
                    maxoff = off + 1;
                }

                Assert!(minoff >= 0 && maxoff >= 0);
                (*result).bound_offsets = bms_add_range(ptr::null_mut(), minoff, maxoff);
            } else {
                /*
                 * The lookup value falls in the range between some bounds in
                 * boundinfo.  'off' would be the offset of the greatest bound
                 * that is <= lookup value, so add off + 1 to the result
                 * instead as the offset of the upper bound of the only
                 * partition that may contain the lookup value.  If 'off' is
                 * -1 indicating that all bounds are greater, then we simply
                 * end up adding the first bound's offset, that is, 0.
                 */
                (*result).bound_offsets = bms_make_singleton(off + 1);
            }

            return result;
        }
        s if s == BTGreaterEqualStrategyNumber => {
            inclusive = true;
            /* fall through */

            /*
             * Look for the smallest bound that is > or >= lookup value and
             * set minoff to its offset.
             */
            off = partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo, nvalues, values, &mut is_equal);
            if off < 0 {
                /*
                 * All bounds are greater than the lookup value, so include
                 * all of them in the result.
                 */
                minoff = 0;
            } else {
                if is_equal && nvalues < partnatts {
                    /*
                     * Since the lookup value contains only a prefix of keys,
                     * we must find other bounds that may also match the
                     * prefix.  partition_range_datum_bsearch() returns the
                     * offset of one of them, find others by checking adjacent
                     * bounds.
                     *
                     * Based on whether the lookup values are inclusive or
                     * not, we must either include the indexes of all such
                     * bounds in the result (that is, set minoff to the index
                     * of smallest such bound) or find the smallest one that's
                     * greater than the lookup values and set minoff to that.
                     */
                    while off >= 1 && off < (*boundinfo).ndatums - 1 {
                        let cmpval: i32;
                        let nextoff: c_int;

                        nextoff = if inclusive { off - 1 } else { off + 1 };
                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(nextoff as usize),
                            *(*boundinfo).kind.add(nextoff as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }

                        off = nextoff;
                    }

                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );

                    minoff = if inclusive { off } else { off + 1 };
                } else {

                    /*
                     * lookup value falls in the range between some bounds in
                     * boundinfo.  off would be the offset of the greatest
                     * bound that is <= lookup value, so add off + 1 to the
                     * result instead as the offset of the upper bound of the
                     * smallest partition that may contain the lookup value.
                     */
                    minoff = off + 1;
                }
            }
        }
        s if s == BTGreaterStrategyNumber => {
            /*
             * Look for the smallest bound that is > or >= lookup value and
             * set minoff to its offset.
             */
            off = partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo, nvalues, values, &mut is_equal);
            if off < 0 {
                minoff = 0;
            } else {
                if is_equal && nvalues < partnatts {
                    while off >= 1 && off < (*boundinfo).ndatums - 1 {
                        let cmpval: i32;
                        let nextoff: c_int;

                        nextoff = if inclusive { off - 1 } else { off + 1 };
                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(nextoff as usize),
                            *(*boundinfo).kind.add(nextoff as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }
                        off = nextoff;
                    }
                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );
                    minoff = if inclusive { off } else { off + 1 };
                } else {
                    minoff = off + 1;
                }
            }
        }
        s if s == BTLessEqualStrategyNumber => {
            inclusive = true;
            /* fall through */

            /*
             * Look for the greatest bound that is < or <= lookup value and
             * set maxoff to its offset.
             */
            off = partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo, nvalues, values, &mut is_equal);
            if off >= 0 {
                /*
                 * See the comment above.
                 */
                if is_equal && nvalues < partnatts {
                    while off >= 1 && off < (*boundinfo).ndatums - 1 {
                        let cmpval: i32;
                        let nextoff: c_int;

                        nextoff = if inclusive { off + 1 } else { off - 1 };
                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(nextoff as usize),
                            *(*boundinfo).kind.add(nextoff as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }

                        off = nextoff;
                    }

                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );

                    maxoff = if inclusive { off + 1 } else { off };
                }

                /*
                 * The lookup value falls in the range between some bounds in
                 * boundinfo.  'off' would be the offset of the greatest bound
                 * that is <= lookup value, so add off + 1 to the result
                 * instead as the offset of the upper bound of the greatest
                 * partition that may contain lookup value.  If the lookup
                 * value had exactly matched the bound, but it isn't
                 * inclusive, no need add the adjacent partition.
                 */
                else if !is_equal || inclusive {
                    maxoff = off + 1;
                } else {
                    maxoff = off;
                }
            } else {
                /*
                 * 'off' is -1 indicating that all bounds are greater, so just
                 * set the first bound's offset as maxoff.
                 */
                maxoff = off + 1;
            }
        }
        s if s == BTLessStrategyNumber => {
            /*
             * Look for the greatest bound that is < or <= lookup value and
             * set maxoff to its offset.
             */
            off = partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo, nvalues, values, &mut is_equal);
            if off >= 0 {
                if is_equal && nvalues < partnatts {
                    while off >= 1 && off < (*boundinfo).ndatums - 1 {
                        let cmpval: i32;
                        let nextoff: c_int;

                        nextoff = if inclusive { off + 1 } else { off - 1 };
                        cmpval = partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(nextoff as usize),
                            *(*boundinfo).kind.add(nextoff as usize),
                            values,
                            nvalues,
                        );
                        if cmpval != 0 {
                            break;
                        }
                        off = nextoff;
                    }
                    Assert!(
                        0 == partition_rbound_datum_cmp(
                            partsupfunc,
                            partcollation,
                            *(*boundinfo).datums.add(off as usize),
                            *(*boundinfo).kind.add(off as usize),
                            values,
                            nvalues,
                        )
                    );
                    maxoff = if inclusive { off + 1 } else { off };
                } else if !is_equal || inclusive {
                    maxoff = off + 1;
                } else {
                    maxoff = off;
                }
            } else {
                maxoff = off + 1;
            }
        }
        _ => {
            elog!(ERROR, "invalid strategy number {}", opstrategy);
        }
    }

    Assert!(minoff >= 0 && minoff <= (*boundinfo).ndatums);
    Assert!(maxoff >= 0 && maxoff <= (*boundinfo).ndatums);

    /*
     * If the smallest partition to return has MINVALUE (negative infinity) as
     * its lower bound, increment it to point to the next finite bound
     * (supposedly its upper bound), so that we don't inadvertently end up
     * scanning the default partition.
     */
    if minoff < (*boundinfo).ndatums && *partindices.add(minoff as usize) < 0 {
        let lastkey: c_int = nvalues - 1;

        if *(*(*boundinfo).kind.add(minoff as usize)).add(lastkey as usize)
            == crate::nodes::parsenodes::PartitionRangeDatumKind::PARTITION_RANGE_DATUM_MINVALUE
        {
            minoff += 1;
            Assert!(*(*boundinfo).indexes.add(minoff as usize) >= 0);
        }
    }

    /*
     * If the previous greatest partition has MAXVALUE (positive infinity) as
     * its upper bound (something only possible to do with multi-column range
     * partitioning), we scan switch to it as the greatest partition to
     * return.  Again, so that we don't inadvertently end up scanning the
     * default partition.
     */
    if maxoff >= 1 && *partindices.add(maxoff as usize) < 0 {
        let lastkey: c_int = nvalues - 1;

        if *(*(*boundinfo).kind.add((maxoff - 1) as usize)).add(lastkey as usize)
            == crate::nodes::parsenodes::PartitionRangeDatumKind::PARTITION_RANGE_DATUM_MAXVALUE
        {
            maxoff -= 1;
            Assert!(*(*boundinfo).indexes.add(maxoff as usize) >= 0);
        }
    }

    Assert!(minoff >= 0 && maxoff >= 0);
    if minoff <= maxoff {
        (*result).bound_offsets = bms_add_range(ptr::null_mut(), minoff, maxoff);
    }

    result
}

/*
 * pull_exec_paramids
 *		Returns a Bitmapset containing the paramids of all Params with
 *		paramkind = PARAM_EXEC in 'expr'.
 */
unsafe fn pull_exec_paramids(expr: *mut Expr) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = ptr::null_mut();

    pull_exec_paramids_walker(expr as *mut Node, &mut result);

    result
}

unsafe fn pull_exec_paramids_walker(node: *mut Node, context: *mut *mut Bitmapset) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        let param: *mut Param = node as *mut Param;

        if (*param).paramkind == PARAM_EXEC {
            *context = bms_add_member(*context, (*param).paramid);
        }
        return false;
    }
    expression_tree_walker(node, pull_exec_paramids_walker, context)
}

/*
 * get_partkey_exec_paramids
 *		Loop through given pruning steps and find out which exec Params
 *		are used.
 *
 * Returns a Bitmapset of Param IDs.
 */
unsafe fn get_partkey_exec_paramids(steps: *mut List) -> *mut Bitmapset {
    let mut execparamids: *mut Bitmapset = ptr::null_mut();
    let mut lc: *mut ListCell;

    foreach!(lc, steps, {
        let step: *mut PartitionPruneStepOp = lfirst(current_cell!(lc)) as *mut PartitionPruneStepOp;
        let mut lc2: *mut ListCell;

        if !IsA!(step, T_PartitionPruneStepOp) {
            continue;
        }

        foreach!(lc2, (*step).exprs, {
            let expr: *mut Expr = lfirst(current_cell!(lc2)) as *mut Expr;

            /* We can be quick for plain Consts */
            if !IsA!(expr, T_Const) {
                execparamids = bms_join(execparamids, pull_exec_paramids(expr));
            }
        });
    });

    execparamids
}

/*
 * perform_pruning_base_step
 *		Determines the indexes of datums that satisfy conditions specified in
 *		'opstep'.
 *
 * Result also contains whether special null-accepting and/or default
 * partition need to be scanned.
 */
unsafe fn perform_pruning_base_step(
    context: *mut PartitionPruneContext,
    opstep: *mut PartitionPruneStepOp,
) -> *mut PruneStepResult {
    let mut lc1: *mut ListCell;
    let mut lc2: *mut ListCell;
    let mut keyno: c_int;
    let mut nvalues: c_int;
    let mut values: [Datum; PARTITION_MAX_KEYS] = [0; PARTITION_MAX_KEYS];
    let partsupfunc: *mut FmgrInfo;
    let mut stateidx: c_int;

    /*
     * There better be the same number of expressions and compare functions.
     */
    Assert!(list_length((*opstep).exprs) == list_length((*opstep).cmpfns));

    nvalues = 0;
    lc1 = list_head((*opstep).exprs);
    lc2 = list_head((*opstep).cmpfns);

    /*
     * Generate the partition lookup key that will be used by one of the
     * get_matching_*_bounds functions called below.
     */
    keyno = 0;
    while keyno < (*context).partnatts {
        /*
         * For hash partitioning, it is possible that values of some keys are
         * not provided in operator clauses, but instead the planner found
         * that they appeared in a IS NULL clause.
         */
        if bms_is_member(keyno, (*opstep).nullkeys) {
            keyno += 1;
            continue;
        }

        /*
         * For range partitioning, we must only perform pruning with values
         * for either all partition keys or a prefix thereof.
         */
        if keyno > nvalues && (*context).strategy as u8 == PARTITION_STRATEGY_RANGE as u8 {
            break;
        }

        if !lc1.is_null() {
            let expr: *mut Expr;
            let mut datum: Datum = 0;
            let mut isnull: bool = false;
            let cmpfn: Oid;

            expr = lfirst(lc1) as *mut Expr;
            stateidx = PruneCxtStateIdx((*context).partnatts, (*opstep).step.step_id, keyno);
            partkey_datum_from_expr(context, expr, stateidx, &mut datum, &mut isnull);

            /*
             * Since we only allow strict operators in pruning steps, any
             * null-valued comparison value must cause the comparison to fail,
             * so that no partitions could match.
             */
            if isnull {
                let result: *mut PruneStepResult =
                    palloc(size_of::<PruneStepResult>() as Size) as *mut PruneStepResult;
                (*result).bound_offsets = ptr::null_mut();
                (*result).scan_default = false;
                (*result).scan_null = false;

                return result;
            }

            /* Set up the stepcmpfuncs entry, unless we already did */
            cmpfn = lfirst_oid(lc2);
            Assert!(OidIsValid(cmpfn));
            if cmpfn != (*(*context).stepcmpfuncs.add(stateidx as usize)).fn_oid {
                /*
                 * If the needed support function is the same one cached in
                 * the relation's partition key, copy the cached FmgrInfo.
                 * Otherwise (i.e., when we have a cross-type comparison), an
                 * actual lookup is required.
                 */
                if cmpfn == (*(*context).partsupfunc.add(keyno as usize)).fn_oid {
                    fmgr_info_copy(
                        (*context).stepcmpfuncs.add(stateidx as usize),
                        (*context).partsupfunc.add(keyno as usize),
                        (*context).ppccontext,
                    );
                } else {
                    fmgr_info_cxt(
                        cmpfn,
                        (*context).stepcmpfuncs.add(stateidx as usize),
                        (*context).ppccontext,
                    );
                }
            }

            values[keyno as usize] = datum;
            nvalues += 1;

            lc1 = lnext((*opstep).exprs, lc1);
            lc2 = lnext((*opstep).cmpfns, lc2);
        }

        keyno += 1;
    }

    /*
     * Point partsupfunc to the entry for the 0th key of this step; the
     * additional support functions, if any, follow consecutively.
     */
    stateidx = PruneCxtStateIdx((*context).partnatts, (*opstep).step.step_id, 0);
    partsupfunc = (*context).stepcmpfuncs.add(stateidx as usize);

    match (*context).strategy as u8 {
        s if s == PARTITION_STRATEGY_HASH as u8 => {
            get_matching_hash_bounds(
                context,
                (*opstep).opstrategy,
                values.as_mut_ptr(),
                nvalues,
                partsupfunc,
                (*opstep).nullkeys,
            )
        }
        s if s == PARTITION_STRATEGY_LIST as u8 => {
            get_matching_list_bounds(
                context,
                (*opstep).opstrategy,
                values[0],
                nvalues,
                &mut *partsupfunc,
                (*opstep).nullkeys,
            )
        }
        s if s == PARTITION_STRATEGY_RANGE as u8 => {
            get_matching_range_bounds(
                context,
                (*opstep).opstrategy,
                values.as_mut_ptr(),
                nvalues,
                partsupfunc,
                (*opstep).nullkeys,
            )
        }
        _ => {
            elog!(ERROR, "unexpected partition strategy: {}", (*context).strategy as c_int);
            ptr::null_mut()
        }
    }
}

/*
 * perform_pruning_combine_step
 *		Determines the indexes of datums obtained by combining those given
 *		by the steps identified by cstep->source_stepids using the specified
 *		combination method
 *
 * Since cstep may refer to the result of earlier steps, we also receive
 * step_results here.
 */
unsafe fn perform_pruning_combine_step(
    context: *mut PartitionPruneContext,
    cstep: *mut PartitionPruneStepCombine,
    step_results: *mut *mut PruneStepResult,
) -> *mut PruneStepResult {
    let result: *mut PruneStepResult =
        palloc0(size_of::<PruneStepResult>() as Size) as *mut PruneStepResult;
    let mut firststep: bool;
    let mut lc1: *mut ListCell;

    /*
     * A combine step without any source steps is an indication to not perform
     * any partition pruning.  Return all datum indexes in that case.
     */
    if (*cstep).source_stepids == NIL {
        let boundinfo: PartitionBoundInfo = (*context).boundinfo;

        (*result).bound_offsets = bms_add_range(ptr::null_mut(), 0, (*boundinfo).nindexes - 1);
        (*result).scan_default = partition_bound_has_default(boundinfo);
        (*result).scan_null = partition_bound_accepts_nulls(boundinfo);
        return result;
    }

    match (*cstep).combineOp {
        PARTPRUNE_COMBINE_UNION => {
            foreach!(lc1, (*cstep).source_stepids, {
                let step_id: c_int = lfirst_int(current_cell!(lc1));
                let step_result: *mut PruneStepResult;

                /*
                 * step_results[step_id] must contain a valid result, which is
                 * confirmed by the fact that cstep's step_id is greater than
                 * step_id and the fact that results of the individual steps
                 * are evaluated in sequence of their step_ids.
                 */
                if step_id >= (*cstep).step.step_id {
                    elog!(ERROR, "invalid pruning combine step argument");
                }
                let step_result = *step_results.add(step_id as usize);
                Assert!(!step_result.is_null());

                /* Record any additional datum indexes from this step */
                (*result).bound_offsets = bms_add_members(
                    (*result).bound_offsets,
                    (*step_result).bound_offsets,
                );

                /* Update whether to scan null and default partitions. */
                if !(*result).scan_null {
                    (*result).scan_null = (*step_result).scan_null;
                }
                if !(*result).scan_default {
                    (*result).scan_default = (*step_result).scan_default;
                }
            });
        }
        PARTPRUNE_COMBINE_INTERSECT => {
            firststep = true;
            foreach!(lc1, (*cstep).source_stepids, {
                let step_id: c_int = lfirst_int(current_cell!(lc1));
                let step_result: *mut PruneStepResult;

                if step_id >= (*cstep).step.step_id {
                    elog!(ERROR, "invalid pruning combine step argument");
                }
                let step_result = *step_results.add(step_id as usize);
                Assert!(!step_result.is_null());

                if firststep {
                    /* Copy step's result the first time. */
                    (*result).bound_offsets = bms_copy((*step_result).bound_offsets);
                    (*result).scan_null = (*step_result).scan_null;
                    (*result).scan_default = (*step_result).scan_default;
                    firststep = false;
                } else {
                    /* Record datum indexes common to both steps */
                    (*result).bound_offsets = bms_int_members(
                        (*result).bound_offsets,
                        (*step_result).bound_offsets,
                    );

                    /* Update whether to scan null and default partitions. */
                    if (*result).scan_null {
                        (*result).scan_null = (*step_result).scan_null;
                    }
                    if (*result).scan_default {
                        (*result).scan_default = (*step_result).scan_default;
                    }
                }
            });
        }
    }

    result
}

/*
 * match_boolean_partition_clause
 *
 * If we're able to match the clause to the partition key as specially-shaped
 * boolean clause, set *outconst to a Const containing a true, false or NULL
 * value, set *notclause according to if the clause was in the "not" form,
 * i.e. "IS NOT TRUE", "IS NOT FALSE" or "IS NOT UNKNOWN" and return
 * PARTCLAUSE_MATCH_CLAUSE for "IS [NOT] (TRUE|FALSE)" clauses and
 * PARTCLAUSE_MATCH_NULLNESS for "IS [NOT] UNKNOWN" clauses.  Otherwise,
 * return PARTCLAUSE_UNSUPPORTED if the clause cannot be used for partition
 * pruning, and PARTCLAUSE_NOMATCH for supported clauses that do not match this
 * 'partkey'.
 */
unsafe fn match_boolean_partition_clause(
    partopfamily: Oid,
    clause: *mut Expr,
    partkey: *mut Expr,
    outconst: *mut *mut Expr,
    notclause: *mut bool,
) -> PartClauseMatchStatus {
    let mut leftop: *mut Expr;

    *outconst = ptr::null_mut();
    *notclause = false;

    /*
     * Partitioning currently can only use built-in AMs, so checking for
     * built-in boolean opfamilies is good enough.
     */
    if !IsBuiltinBooleanOpfamily(partopfamily) {
        return PARTCLAUSE_UNSUPPORTED;
    }

    if IsA!(clause, T_BooleanTest) {
        let btest: *mut BooleanTest = clause as *mut BooleanTest;

        leftop = (*btest).arg;
        if IsA!(leftop, T_RelabelType) {
            leftop = (*(leftop as *mut RelabelType)).arg;
        }

        if equal(leftop as *const c_void, partkey as *const c_void) {
            match (*btest).booltesttype {
                IS_NOT_TRUE => {
                    *notclause = true;
                    /* fall through */
                    *outconst = makeBoolConst(true, false) as *mut Expr;
                    return PARTCLAUSE_MATCH_CLAUSE;
                }
                IS_TRUE => {
                    *outconst = makeBoolConst(true, false) as *mut Expr;
                    return PARTCLAUSE_MATCH_CLAUSE;
                }
                IS_NOT_FALSE => {
                    *notclause = true;
                    /* fall through */
                    *outconst = makeBoolConst(false, false) as *mut Expr;
                    return PARTCLAUSE_MATCH_CLAUSE;
                }
                IS_FALSE => {
                    *outconst = makeBoolConst(false, false) as *mut Expr;
                    return PARTCLAUSE_MATCH_CLAUSE;
                }
                IS_NOT_UNKNOWN => {
                    *notclause = true;
                    /* fall through */
                    return PARTCLAUSE_MATCH_NULLNESS;
                }
                IS_UNKNOWN => {
                    return PARTCLAUSE_MATCH_NULLNESS;
                }
                _ => {
                    return PARTCLAUSE_UNSUPPORTED;
                }
            }
        }
        /* does not match partition key */
        return PARTCLAUSE_NOMATCH;
    } else {
        let is_not_clause2: bool = is_notclause(clause);

        leftop = if is_not_clause2 {
            get_notclausearg(clause)
        } else {
            clause
        };

        if IsA!(leftop, T_RelabelType) {
            leftop = (*(leftop as *mut RelabelType)).arg;
        }

        /* Compare to the partition key, and make up a clause ... */
        if equal(leftop as *const c_void, partkey as *const c_void) {
            *outconst = makeBoolConst(!is_not_clause2, false) as *mut Expr;
        } else if equal(negate_clause(leftop as *mut Node) as *const c_void, partkey as *const c_void) {
            *outconst = makeBoolConst(is_not_clause2, false) as *mut Expr;
        } else {
            return PARTCLAUSE_NOMATCH;
        }

        return PARTCLAUSE_MATCH_CLAUSE;
    }
}

/*
 * partkey_datum_from_expr
 *		Evaluate expression for potential partition pruning
 *
 * Evaluate 'expr'; set *value and *isnull to the resulting Datum and nullflag.
 *
 * If expr isn't a Const, its ExprState is in stateidx of the context
 * exprstate array.
 *
 * Note that the evaluated result may be in the per-tuple memory context of
 * context->exprcontext, and we may have leaked other memory there too.
 * This memory must be recovered by resetting that ExprContext after
 * we're done with the pruning operation (see execPartition.c).
 */
unsafe fn partkey_datum_from_expr(
    context: *mut PartitionPruneContext,
    expr: *mut Expr,
    stateidx: c_int,
    value: *mut Datum,
    isnull: *mut bool,
) {
    if IsA!(expr, T_Const) {
        /* We can always determine the value of a constant */
        let con: *mut Const = expr as *mut Const;

        *value = (*con).constvalue;
        *isnull = (*con).constisnull;
    } else {
        let exprstate: *mut crate::nodes::execnodes::ExprState;
        let ectx: *mut crate::nodes::execnodes::ExprContext;

        /*
         * We should never see a non-Const in a step unless the caller has
         * passed a valid ExprContext.
         */
        Assert!(!(*context).exprcontext.is_null());

        exprstate = *(*context).exprstates.add(stateidx as usize);
        ectx = (*context).exprcontext;
        *value = ExecEvalExprSwitchContext(exprstate, ectx, isnull);
    }
}

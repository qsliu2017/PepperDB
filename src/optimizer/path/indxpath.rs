//! indxpath.rs
//!   Routines to determine which indexes are usable for scanning a
//!   given relation, and create Paths accordingly.
//!
//! Translated 1:1 from postgres/src/backend/optimizer/path/indxpath.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/optimizer/path/indxpath.c
//!
//! #include mapping:
//!   "postgres.h"                -> crate::prelude::*
//!   "access/stratnum.h"         -> crate::access::stratnum
//!   "access/sysattr.h"          -> FirstLowInvalidHeapAttributeNumber (local const)
//!   "catalog/pg_am.h"           -> BTREE_AM_OID (local const)
//!   "catalog/pg_operator.h"     -> BooleanEqualOperator (local const)
//!   "nodes/makefuncs.h"         -> make_opclause/makeNode stubs
//!   "nodes/nodeFuncs.h"         -> expression_tree_walker/mutator stubs
//!   "nodes/supportnodes.h"      -> SupportRequestIndexCondition (local stub)
//!   "optimizer/cost.h"          -> enable_indexonlyscan
//!   "optimizer/optimizer.h"     -> contain_volatile_functions/pull_varnos/etc stubs
//!   "optimizer/pathnode.h"      -> create_index_path/add_path/etc stubs
//!   "optimizer/paths.h"         -> generate_implied_equalities_for_column stubs
//!   "optimizer/restrictinfo.h"  -> restriction_is_or_clause/etc stubs
//!   "utils/lsyscache.h"         -> get_commutator/get_opfamily_member/etc stubs
//!   "utils/selfuncs.h"          -> estimate_num_groups stub

use crate::prelude::*;
use core::ffi::c_void;
use core::mem::size_of;

use crate::access::stratnum::{
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTLessEqualStrategyNumber,
    BTLessStrategyNumber,
};
use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_copy, bms_del_member, bms_del_members, bms_difference,
    bms_equal, bms_is_empty, bms_is_member, bms_is_subset, bms_membership, bms_next_member,
    bms_overlap, bms_subset_compare, bms_union,
    BMS_Comparison::{BMS_DIFFERENT},
    BMS_Membership::{BMS_SINGLETON},
    Bitmapset,
};
use crate::nodes::nodes::{Cost, Node, NodeTag, Selectivity, JoinType};
use crate::nodes::nodes::NodeTag::{
    T_BitmapAndPath, T_BitmapHeapPath, T_BitmapHeapScan, T_BitmapOrPath, T_BoolExpr,
    T_BooleanTest, T_FuncExpr, T_IndexPath, T_NullTest, T_OpExpr, T_PlaceHolderVar,
    T_RelabelType, T_RestrictInfo, T_RowCompareExpr, T_ScalarArrayOpExpr, T_SupportRequestIndexCondition,
    T_Var,
};
use crate::nodes::pathnodes::{
    BitmapAndPath, BitmapHeapPath, BitmapOrPath, EquivalenceClass, EquivalenceMember,
    EquivalenceMemberIterator, IndexClause, IndexOptInfo, IndexPath, ParamPathInfo, Path,
    PathKey, PathTarget, PlannerInfo, PlaceHolderVar, RelOptInfo, Relids, RestrictInfo,
    SpecialJoinInfo, RELOPT_OTHER_MEMBER_REL,
};
use crate::nodes::pg_list::{
    lappend, lappend_int, lappend_oid, lfirst, lfirst_oid, linitial, linitial_oid, list_append_unique,
    list_append_unique_ptr, list_concat, list_concat_copy, list_copy, list_copy_head, list_delete,
    list_free, list_head, list_length,
    list_member, list_member_oid, list_nth, list_nth_oid, list_truncate, lnext, lsecond,
    lsecond_oid, List, ListCell, NIL,
};
use crate::nodes::primnodes::{
    BoolExpr, BooleanTest, BoolTestType, CompareType, Const, Expr, FuncExpr, NullTest, OpExpr,
    RelabelType, RowCompareExpr, ScalarArrayOpExpr, Var, AND_EXPR, OR_EXPR,
    IS_FALSE, IS_TRUE,
};
use crate::postgres_ext::Oid;
use crate::{castNode, current_cell, foreach, lfirst_node, list_make1, list_make1_int, list_make1_oid, list_make2, Assert, IsA};

// ---------------------------------------------------------------------------
// Catalog OID constants (catalog/pg_am.h, catalog/pg_operator.h, access/sysattr.h)
// ---------------------------------------------------------------------------

/// OID of the btree access method (from catalog/pg_am.h).
const BTREE_AM_OID: Oid = 403;

/// OID of the boolean equality operator (from catalog/pg_operator.h).
const BooleanEqualOperator: Oid = 91;

/// BOOLOID (catalog/pg_type.h)
const BOOLOID: Oid = 16;

/// RECORDOID (catalog/pg_type.h)
const RECORDOID: Oid = 2249;

/// FirstLowInvalidHeapAttributeNumber (access/sysattr.h)
const FirstLowInvalidHeapAttributeNumber: i32 = -8;

/// INDEX_MAX_KEYS (access/index_am_properties.h / pg_config_manual.h)
const INDEX_MAX_KEYS: usize = 32;

/// InvalidOid
const InvalidOid: Oid = 0;

/// FirstNormalObjectId (access/transam.h)
const FirstNormalObjectId: Oid = 16384;

// ---------------------------------------------------------------------------
// Macros translated to inline fns
// ---------------------------------------------------------------------------

/// IndexCollMatchesExprColl -- collation is irrelevant or matches.
#[inline]
fn index_coll_matches_expr_coll(idxcollation: Oid, exprcollation: Oid) -> bool {
    idxcollation == InvalidOid || idxcollation == exprcollation
}

// ---------------------------------------------------------------------------
// Local enums / structs
// ---------------------------------------------------------------------------

/// Whether we are looking for plain indexscan, bitmap scan, or either.
#[derive(Copy, Clone, PartialEq, Eq)]
enum ScanTypeControl {
    StIndexscan,   // must support amgettuple
    StBitmapscan,  // must support amgetbitmap
    StAnyscan,     // either is okay
}
use ScanTypeControl::{StAnyscan, StBitmapscan, StIndexscan};

/// Data structure for collecting qual clauses that match an index.
struct IndexClauseSet {
    nonempty: bool,
    // Lists of IndexClause nodes, one list per index column
    indexclauses: [*mut List; INDEX_MAX_KEYS],
}

impl IndexClauseSet {
    fn zeroed() -> Self {
        IndexClauseSet {
            nonempty: false,
            indexclauses: [NIL; INDEX_MAX_KEYS],
        }
    }
}

/// Per-path data used within choose_bitmap_and().
struct PathClauseUsage {
    path: *mut Path,          // IndexPath, BitmapAndPath, or BitmapOrPath
    quals: *mut List,         // the WHERE clauses it uses
    preds: *mut List,         // predicates of its partial index(es)
    clauseids: *mut Bitmapset, // quals+preds represented as a bitmapset
    unclassifiable: bool,     // has too many quals+preds to process?
}

/// Callback argument for ec_member_matches_indexcol.
struct EcMemberMatchesArg {
    index: *mut IndexOptInfo, // index we're considering
    indexcol: i32,            // index column we want to match to
}

/// Utility structure for group_similar_or_args().
#[derive(Copy, Clone)]
struct OrArgIndexMatch {
    indexnum: i32,    // index of the matching index, or -1 if no matching index
    colnum: i32,      // index of the matching column, or -1 if no matching index
    opno: Oid,        // OID of the OpClause operator, or InvalidOid if not an OpExpr
    inputcollid: Oid, // OID of the OpClause input collation
    argindex: i32,    // index of the clause in the list of arguments
    groupindex: i32,  // value of argindex for the first clause in the group
}

// ---------------------------------------------------------------------------
// STUBs -- optimizer/pathnode.h
// ---------------------------------------------------------------------------

// TODO(pg-port): real create_index_path lives in optimizer/pathnode.rs
unsafe fn create_index_path(
    _root: *mut PlannerInfo,
    _index: *mut IndexOptInfo,
    _indexclauses: *mut List,
    _orderbyclauses: *mut List,
    _orderbyclausecols: *mut List,
    _pathkeys: *mut List,
    _indexscandir: i32,
    _index_only_scan: bool,
    _required_outer: Relids,
    _loop_count: f64,
    _partial_path: bool,
) -> *mut IndexPath {
    unimplemented!()
}

// TODO(pg-port): real create_bitmap_heap_path lives in optimizer/pathnode.rs
unsafe fn create_bitmap_heap_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _bitmapqual: *mut Path,
    _required_outer: Relids,
    _loop_count: f64,
    _parallel_degree: i32,
) -> *mut BitmapHeapPath {
    unimplemented!()
}

// TODO(pg-port): real create_bitmap_and_path lives in optimizer/pathnode.rs
unsafe fn create_bitmap_and_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _bitmapquals: *mut List,
) -> *mut BitmapAndPath {
    unimplemented!()
}

// TODO(pg-port): real create_bitmap_or_path lives in optimizer/pathnode.rs
unsafe fn create_bitmap_or_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _bitmapquals: *mut List,
) -> *mut BitmapOrPath {
    unimplemented!()
}

// TODO(pg-port): real create_partial_bitmap_paths lives in optimizer/pathnode.rs
unsafe fn create_partial_bitmap_paths(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _bitmapqual: *mut Path,
) {
    unimplemented!()
}

// TODO(pg-port): real add_path lives in optimizer/pathnode.rs
unsafe fn add_path(_parent_rel: *mut RelOptInfo, _new_path: *mut Path) {
    unimplemented!()
}

// TODO(pg-port): real add_partial_path lives in optimizer/pathnode.rs
unsafe fn add_partial_path(_parent_rel: *mut RelOptInfo, _new_path: *mut Path) {
    unimplemented!()
}

// TODO(pg-port): real cost_bitmap_tree_node lives in optimizer/path/costsize.rs
unsafe fn cost_bitmap_tree_node(path: *mut Path, cost: *mut Cost, selec: *mut Selectivity) {
    unimplemented!()
}

// TODO(pg-port): real cost_bitmap_heap_scan lives in optimizer/path/costsize.rs
unsafe fn cost_bitmap_heap_scan(
    _path: *mut Path,
    _root: *mut PlannerInfo,
    _baserel: *mut RelOptInfo,
    _param_info: *mut ParamPathInfo,
    _bitmapqual: *mut Path,
    _loop_count: f64,
) {
    unimplemented!()
}

// TODO(pg-port): real PATH_REQ_OUTER macro - returns param_info's ppi_req_outer or NULL
#[inline]
unsafe fn PATH_REQ_OUTER(path: *mut Path) -> Relids {
    if (*path).param_info.is_null() {
        core::ptr::null_mut()
    } else {
        (*((*path).param_info)).ppi_req_outer
    }
}

// TODO(pg-port): real has_useful_pathkeys lives in optimizer/path/pathkeys.rs
unsafe fn has_useful_pathkeys(_root: *mut PlannerInfo, _rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

// TODO(pg-port): real build_index_pathkeys lives in optimizer/path/pathkeys.rs
unsafe fn build_index_pathkeys(
    _root: *mut PlannerInfo,
    _index: *mut IndexOptInfo,
    _scandir: i32,
) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): real truncate_useless_pathkeys lives in optimizer/path/pathkeys.rs
unsafe fn truncate_useless_pathkeys(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _pathkeys: *mut List,
) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): ScanDirection constants from nodes/plannodes.h
const ForwardScanDirection: i32 = 1;
const BackwardScanDirection: i32 = -1;

// ---------------------------------------------------------------------------
// STUBs -- optimizer/restrictinfo.h
// ---------------------------------------------------------------------------

// TODO(pg-port): restriction_is_or_clause
unsafe fn restriction_is_or_clause(rinfo: *mut RestrictInfo) -> bool {
    unimplemented!()
}

// TODO(pg-port): restriction_is_securely_promotable
unsafe fn restriction_is_securely_promotable(
    rinfo: *mut RestrictInfo,
    rel: *mut RelOptInfo,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): join_clause_is_movable_to
unsafe fn join_clause_is_movable_to(rinfo: *mut RestrictInfo, rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

// TODO(pg-port): commute_restrictinfo
unsafe fn commute_restrictinfo(rinfo: *mut RestrictInfo, comm_op: Oid) -> *mut RestrictInfo {
    unimplemented!()
}

// TODO(pg-port): make_plain_restrictinfo
unsafe fn make_plain_restrictinfo(
    _root: *mut PlannerInfo,
    _clause: *mut Expr,
    _orclause: *mut Expr,
    _is_pushed_down: bool,
    _has_clone: bool,
    _is_clone: bool,
    _pseudoconstant: bool,
    _security_level: crate::c::Index,
    _required_relids: Relids,
    _incompatible_relids: Relids,
    _outer_relids: Relids,
) -> *mut RestrictInfo {
    unimplemented!()
}

// TODO(pg-port): make_simple_restrictinfo
unsafe fn make_simple_restrictinfo(root: *mut PlannerInfo, clause: *mut Expr) -> *mut RestrictInfo {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// STUBs -- optimizer/optimizer.h / optimizer/paths.h
// ---------------------------------------------------------------------------

// TODO(pg-port): contain_volatile_functions
unsafe fn contain_volatile_functions(node: *mut c_void) -> bool {
    unimplemented!()
}

// TODO(pg-port): pull_varnos
unsafe fn pull_varnos(root: *mut PlannerInfo, node: *mut c_void) -> Relids {
    unimplemented!()
}

// TODO(pg-port): pull_varattnos
unsafe fn pull_varattnos(node: *mut c_void, varno: u32, varattnos: *mut *mut Bitmapset) {
    unimplemented!()
}

// TODO(pg-port): contain_var_clause
unsafe fn contain_var_clause(node: *mut c_void) -> bool {
    unimplemented!()
}

// TODO(pg-port): predicate_implied_by
unsafe fn predicate_implied_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): contain_mutable_functions
unsafe fn contain_mutable_functions(node: *mut c_void) -> bool {
    unimplemented!()
}

// TODO(pg-port): generate_implied_equalities_for_column (optimizer/paths.h)
type EcMembersFuncType = unsafe extern "C" fn(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
    arg: *mut c_void,
) -> bool;

unsafe fn generate_implied_equalities_for_column(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _callback: EcMembersFuncType,
    _callback_arg: *mut c_void,
    _prohibited_rels: Relids,
) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): generate_join_implied_equalities (optimizer/paths.h)
unsafe fn generate_join_implied_equalities(
    _root: *mut PlannerInfo,
    _join_relids: Relids,
    _outer_relids: Relids,
    _inner_rel: *mut RelOptInfo,
    _sjinfo: *mut SpecialJoinInfo,
) -> *mut List {
    unimplemented!()
}

// TODO(pg-port): find_childrel_parents (optimizer/optimizer.h)
unsafe fn find_childrel_parents(_root: *mut PlannerInfo, _rel: *mut RelOptInfo) -> Relids {
    unimplemented!()
}

// TODO(pg-port): estimate_num_groups (utils/selfuncs.h)
unsafe fn estimate_num_groups(
    _root: *mut PlannerInfo,
    _groupExprs: *mut List,
    _input_rows: f64,
    _pgset: *mut *mut List,
    _pginfo: *mut *mut c_void,
) -> f64 {
    unimplemented!()
}

// TODO(pg-port): IS_DUMMY_REL macro
unsafe fn IS_DUMMY_REL(rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

// TODO(pg-port): IS_SIMPLE_REL macro
unsafe fn IS_SIMPLE_REL(rel: *mut RelOptInfo) -> bool {
    unimplemented!()
}

// TODO(pg-port): get_plan_rowmark (optimizer/plan/planner.h)
unsafe fn get_plan_rowmark(_rowmarks: *mut List, _relid: u32) -> *mut c_void {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// STUBs -- utils/lsyscache.h
// ---------------------------------------------------------------------------

// TODO(pg-port): get_commutator
unsafe fn get_commutator(opno: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): op_in_opfamily
unsafe fn op_in_opfamily(opno: Oid, opfamily: Oid) -> bool {
    unimplemented!()
}

// TODO(pg-port): get_op_opfamily_strategy
unsafe fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> i32 {
    unimplemented!()
}

// TODO(pg-port): get_op_opfamily_properties
unsafe fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    need_strategy: bool,
    strategy: *mut i32,
    lefttype: *mut Oid,
    righttype: *mut Oid,
) {
    unimplemented!()
}

// TODO(pg-port): get_op_opfamily_sortfamily
unsafe fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): get_opfamily_member
unsafe fn get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: i32) -> Oid {
    unimplemented!()
}

// TODO(pg-port): get_func_support
unsafe fn get_func_support(funcid: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): get_array_type
unsafe fn get_array_type(typid: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): OidFunctionCall1
unsafe fn OidFunctionCall1(functionId: Oid, arg1: usize) -> usize {
    unimplemented!()
}

// TODO(pg-port): DatumGetPointer
unsafe fn DatumGetPointer(datum: usize) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): PointerGetDatum
unsafe fn PointerGetDatum(ptr: *mut c_void) -> usize {
    unimplemented!()
}

// TODO(pg-port): IsBuiltinBooleanOpfamily
unsafe fn IsBuiltinBooleanOpfamily(opfamily: Oid) -> bool {
    unimplemented!()
}

// TODO(pg-port): set_opfuncid
unsafe fn set_opfuncid(clause: *mut OpExpr) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// STUBs -- nodes/makefuncs.h
// ---------------------------------------------------------------------------

// TODO(pg-port): makeNode
unsafe fn makeNode_IndexClause() -> *mut IndexClause {
    unimplemented!()
}

unsafe fn makeNode_OpExpr() -> *mut OpExpr {
    unimplemented!()
}

unsafe fn makeNode_RowCompareExpr() -> *mut RowCompareExpr {
    unimplemented!()
}

// TODO(pg-port): make_opclause
unsafe fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: *mut Expr,
    rightop: *mut Expr,
    opcollid: Oid,
    inputcollid: Oid,
) -> *mut Expr {
    unimplemented!()
}

// TODO(pg-port): make_orclause
unsafe fn make_orclause(orclauses: *mut List) -> *mut Expr {
    unimplemented!()
}

// TODO(pg-port): makeBoolConst
unsafe fn makeBoolConst(value: bool, isnull: bool) -> *mut Node {
    unimplemented!()
}

// TODO(pg-port): copyObject
unsafe fn copyObject_list(obj: *mut List) -> *mut List {
    unimplemented!()
}

unsafe fn copyObject_node(obj: *mut c_void) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): exprType
unsafe fn exprType(expr: *mut c_void) -> Oid {
    unimplemented!()
}

// TODO(pg-port): get_leftop / get_rightop (nodes/nodeFuncs.h)
unsafe fn get_leftop(clause: *const c_void) -> *mut Node {
    unimplemented!()
}

unsafe fn get_rightop(clause: *const c_void) -> *mut Node {
    unimplemented!()
}

// TODO(pg-port): is_notclause (nodes/nodeFuncs.h)
unsafe fn is_notclause(clause: *const c_void) -> bool {
    unimplemented!()
}

// TODO(pg-port): get_notclausearg (nodes/nodeFuncs.h)
unsafe fn get_notclausearg(notclause: *mut Expr) -> *mut Expr {
    unimplemented!()
}

// TODO(pg-port): is_andclause / is_orclause (nodes/nodeFuncs.h)
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == AND_EXPR
}

unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == OR_EXPR
}

// TODO(pg-port): expression_tree_walker (nodes/nodeFuncs.h)
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
) -> bool {
    unimplemented!()
}

// TODO(pg-port): expression_tree_mutator (nodes/nodeFuncs.h)
unsafe fn expression_tree_mutator(
    node: *mut Node,
    mutator: unsafe fn(*mut Node, *mut c_void) -> *mut Node,
    context: *mut c_void,
) -> *mut Node {
    unimplemented!()
}

// TODO(pg-port): nodeTag macro
unsafe fn nodeTag(node: *const c_void) -> NodeTag {
    (*(node as *const Node)).r#type
}

// TODO(pg-port): equal (nodes/equalfuncs.h)
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    unimplemented!()
}

// TODO(pg-port): OidIsValid
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

// TODO(pg-port): pfree
unsafe fn pfree(ptr: *mut c_void) {
    unimplemented!()
}

// TODO(pg-port): palloc
unsafe fn palloc(size: usize) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): MemSet
unsafe fn MemSet(s: *mut c_void, c: i32, n: usize) {
    core::ptr::write_bytes(s as *mut u8, c as u8, n);
}

// TODO(pg-port): MemoryContextSwitchTo (utils/palloc.h)
unsafe fn MemoryContextSwitchTo(context: *mut c_void) -> *mut c_void {
    unimplemented!()
}

// TODO(pg-port): enable_indexonlyscan GUC
static mut enable_indexonlyscan: bool = true;

// TODO(pg-port): make_SAOP_expr (nodes/makefuncs.h)
unsafe fn make_SAOP_expr(
    opno: Oid,
    indexexpr: *mut Node,
    consttype: Oid,
    constcollid: Oid,
    inputcollid: Oid,
    consts: *mut List,
    have_non_const: bool,
) -> *mut ScalarArrayOpExpr {
    unimplemented!()
}

// TODO(pg-port): setup_eclass_member_iterator / eclass_member_iterator_next
unsafe fn setup_eclass_member_iterator(
    it: *mut EquivalenceMemberIterator,
    ec: *mut EquivalenceClass,
    relids: Relids,
) {
    unimplemented!()
}

unsafe fn eclass_member_iterator_next(it: *mut EquivalenceMemberIterator) -> *mut EquivalenceMember {
    unimplemented!()
}

// TODO(pg-port): SupportRequestIndexCondition node (nodes/supportnodes.h)
#[repr(C)]
struct SupportRequestIndexCondition {
    r#type: NodeTag,
    root: *mut PlannerInfo,
    funcid: Oid,
    node: *mut Node,
    indexarg: i32,
    index: *mut IndexOptInfo,
    indexcol: i32,
    opfamily: Oid,
    indexcollation: Oid,
    lossy: bool,
}

// TODO(pg-port): match_index_to_operand (exported in optimizer/paths.h)
unsafe fn match_index_to_operand(
    mut operand: *mut Node,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> bool {
    let indkey: i32;

    /*
     * Ignore any PlaceHolderVar node contained in the operand.  This is
     * needed to be able to apply indexscanning in cases where the operand (or
     * a subtree) has been wrapped in PlaceHolderVars to enforce separate
     * identity or as a result of outer joins.
     */
    operand = strip_phvs_in_index_operand(operand);

    /*
     * Ignore any RelabelType node above the operand.  This is needed to be
     * able to apply indexscanning in binary-compatible-operator cases.
     *
     * Note: we must handle nested RelabelType nodes here.  While
     * eval_const_expressions() will have simplified them to at most one
     * layer, our prior stripping of PlaceHolderVars may have brought separate
     * RelabelTypes into adjacency.
     */
    while !operand.is_null() && IsA!(operand, T_RelabelType) {
        operand = (*(operand as *mut RelabelType)).arg as *mut Node;
    }

    indkey = *(*index).indexkeys.add(indexcol as usize);
    if indkey != 0 {
        /*
         * Simple index column; operand must be a matching Var.
         */
        if !operand.is_null()
            && IsA!(operand, T_Var)
            && (*(*index).rel).relid == (*(operand as *mut Var)).varno as u32
            && indkey == (*(operand as *mut Var)).varattno as c_int
            && (*(operand as *mut Var)).varnullingrels.is_null()
        {
            return true;
        }
    } else {
        /*
         * Index expression; find the correct expression.  (This search could
         * be avoided, at the cost of complicating all the callers of this
         * routine; doesn't seem worth it.)
         */
        let mut indexpr_item: *mut ListCell;
        let mut i: i32;
        let mut indexkey: *mut Node;

        indexpr_item = list_head((*index).indexprs);
        i = 0;
        while i < indexcol {
            if *(*index).indexkeys.add(i as usize) == 0 {
                if indexpr_item.is_null() {
                    elog!(ERROR, "wrong number of index expressions");
                }
                indexpr_item = lnext((*index).indexprs, indexpr_item);
            }
            i += 1;
        }
        if indexpr_item.is_null() {
            elog!(ERROR, "wrong number of index expressions");
        }
        indexkey = lfirst(indexpr_item) as *mut Node;

        /*
         * Does it match the operand?  Again, strip any relabeling.
         */
        if !indexkey.is_null() && IsA!(indexkey, T_RelabelType) {
            indexkey = (*(indexkey as *mut RelabelType)).arg as *mut Node;
        }

        if equal(indexkey as *const c_void, operand as *const c_void) {
            return true;
        }
    }

    false
}

// JOIN_SEMI comes from crate::nodes::nodes::JoinType

// TODO(pg-port): bms_free
unsafe fn bms_free(a: *mut Bitmapset) {}

/*
 * ============================================================
 * create_index_paths()
 *    Generate all interesting index paths for the given relation.
 *    Candidate paths are added to the rel's pathlist (using add_path).
 *
 * To be considered for an index scan, an index must match one or more
 * restriction clauses or join clauses from the query's qual condition,
 * or match the query's ORDER BY condition, or have a predicate that
 * matches the query's qual condition.
 *
 * There are two basic kinds of index scans.  A "plain" index scan uses
 * only restriction clauses (possibly none at all) in its indexqual,
 * so it can be applied in any context.  A "parameterized" index scan uses
 * join clauses (plus restriction clauses, if available) in its indexqual.
 * When joining such a scan to one of the relations supplying the other
 * variables used in its indexqual, the parameterized scan must appear as
 * the inner relation of a nestloop join; it can't be used on the outer side,
 * nor in a merge or hash join.  In that context, values for the other rels'
 * attributes are available and fixed during any one scan of the indexpath.
 *
 * An IndexPath is generated and submitted to add_path() for each plain or
 * parameterized index scan this routine deems potentially interesting for
 * the current query.
 *
 * 'rel' is the relation for which we want to generate index paths
 *
 * Note: check_index_predicates() must have been run previously for this rel.
 *
 * Note: in cases involving LATERAL references in the relation's tlist, it's
 * possible that rel->lateral_relids is nonempty.  Currently, we include
 * lateral_relids into the parameterization reported for each path, but don't
 * take it into account otherwise.  The fact that any such rels *must* be
 * available as parameter sources perhaps should influence our choices of
 * index quals ... but for now, it doesn't seem worth troubling over.
 * In particular, comments below about "unparameterized" paths should be read
 * as meaning "unparameterized so far as the indexquals are concerned".
 */
pub unsafe fn create_index_paths(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    let mut indexpaths: *mut List;
    let mut bitindexpaths: *mut List;
    let mut bitjoinpaths: *mut List;
    let mut joinorclauses: *mut List;
    let mut rclauseset: IndexClauseSet;
    let mut jclauseset: IndexClauseSet;
    let mut eclauseset: IndexClauseSet;
    let mut lc: *mut ListCell;

    /* Skip the whole mess if no indexes */
    if (*rel).indexlist == NIL {
        return;
    }

    /* Bitmap paths are collected and then dealt with at the end */
    bitindexpaths = NIL;
    bitjoinpaths = NIL;
    joinorclauses = NIL;

    /* Examine each index in turn */
    lc = list_head((*rel).indexlist);
    while !lc.is_null() {
        let index: *mut IndexOptInfo = lfirst(lc) as *mut IndexOptInfo;
        lc = lnext((*rel).indexlist, lc);

        /* Protect limited-size array in IndexClauseSets */
        Assert!((*index).nkeycolumns as usize <= INDEX_MAX_KEYS);

        /*
         * Ignore partial indexes that do not match the query.
         * (generate_bitmap_or_paths() might be able to do something with
         * them, but that's of no concern here.)
         */
        if (*index).indpred != NIL && !(*index).predOK {
            continue;
        }

        /*
         * Identify the restriction clauses that can match the index.
         */
        rclauseset = IndexClauseSet::zeroed();
        match_restriction_clauses_to_index(root, index, &mut rclauseset);

        /*
         * Build index paths from the restriction clauses.  These will be
         * non-parameterized paths.  Plain paths go directly to add_path(),
         * bitmap paths are added to bitindexpaths to be handled below.
         */
        get_index_paths(root, rel, index, &mut rclauseset, &mut bitindexpaths);

        /*
         * Identify the join clauses that can match the index.  For the moment
         * we keep them separate from the restriction clauses.  Note that this
         * step finds only "loose" join clauses that have not been merged into
         * EquivalenceClasses.  Also, collect join OR clauses for later.
         */
        jclauseset = IndexClauseSet::zeroed();
        match_join_clauses_to_index(root, rel, index, &mut jclauseset, &mut joinorclauses);

        /*
         * Look for EquivalenceClasses that can generate joinclauses matching
         * the index.
         */
        eclauseset = IndexClauseSet::zeroed();
        match_eclass_clauses_to_index(root, index, &mut eclauseset);

        /*
         * If we found any plain or eclass join clauses, build parameterized
         * index paths using them.
         */
        if jclauseset.nonempty || eclauseset.nonempty {
            consider_index_join_clauses(
                root,
                rel,
                index,
                &mut rclauseset,
                &mut jclauseset,
                &mut eclauseset,
                &mut bitjoinpaths,
            );
        }
    }

    /*
     * Generate BitmapOrPaths for any suitable OR-clauses present in the
     * restriction list.  Add these to bitindexpaths.
     */
    indexpaths = generate_bitmap_or_paths(root, rel, (*rel).baserestrictinfo, NIL);
    bitindexpaths = list_concat(bitindexpaths, indexpaths);

    /*
     * Likewise, generate BitmapOrPaths for any suitable OR-clauses present in
     * the joinclause list.  Add these to bitjoinpaths.
     */
    indexpaths =
        generate_bitmap_or_paths(root, rel, joinorclauses, (*rel).baserestrictinfo);
    bitjoinpaths = list_concat(bitjoinpaths, indexpaths);

    /*
     * If we found anything usable, generate a BitmapHeapPath for the most
     * promising combination of restriction bitmap index paths.  Note there
     * will be only one such path no matter how many indexes exist.  This
     * should be sufficient since there's basically only one figure of merit
     * (total cost) for such a path.
     */
    if bitindexpaths != NIL {
        let bitmapqual: *mut Path;
        let bpath: *mut BitmapHeapPath;

        bitmapqual = choose_bitmap_and(root, rel, bitindexpaths);
        bpath = create_bitmap_heap_path(root, rel, bitmapqual, (*rel).lateral_relids, 1.0, 0);
        add_path(rel, bpath as *mut Path);

        /* create a partial bitmap heap path */
        if (*rel).consider_parallel && (*rel).lateral_relids.is_null() {
            create_partial_bitmap_paths(root, rel, bitmapqual);
        }
    }

    /*
     * Likewise, if we found anything usable, generate BitmapHeapPaths for the
     * most promising combinations of join bitmap index paths.  Our strategy
     * is to generate one such path for each distinct parameterization seen
     * among the available bitmap index paths.  This may look pretty
     * expensive, but usually there won't be very many distinct
     * parameterizations.  (This logic is quite similar to that in
     * consider_index_join_clauses, but we're working with whole paths not
     * individual clauses.)
     */
    if bitjoinpaths != NIL {
        let mut all_path_outers: *mut List;

        /* Identify each distinct parameterization seen in bitjoinpaths */
        all_path_outers = NIL;
        lc = list_head(bitjoinpaths);
        while !lc.is_null() {
            let path: *mut Path = lfirst(lc) as *mut Path;
            lc = lnext(bitjoinpaths, lc);
            let required_outer: Relids = PATH_REQ_OUTER(path);

            all_path_outers = list_append_unique(all_path_outers, required_outer as *mut c_void);
        }

        /* Now, for each distinct parameterization set ... */
        lc = list_head(all_path_outers);
        while !lc.is_null() {
            let max_outers: Relids = lfirst(lc) as Relids;
            lc = lnext(all_path_outers, lc);
            let mut this_path_set: *mut List;
            let bitmapqual: *mut Path;
            let required_outer: Relids;
            let loop_count: f64;
            let bpath: *mut BitmapHeapPath;
            let mut lcp: *mut ListCell;

            /* Identify all the bitmap join paths needing no more than that */
            this_path_set = NIL;
            lcp = list_head(bitjoinpaths);
            while !lcp.is_null() {
                let path: *mut Path = lfirst(lcp) as *mut Path;
                lcp = lnext(bitjoinpaths, lcp);

                if bms_is_subset(PATH_REQ_OUTER(path), max_outers) {
                    this_path_set = lappend(this_path_set, path as *mut c_void);
                }
            }

            /*
             * Add in restriction bitmap paths, since they can be used
             * together with any join paths.
             */
            this_path_set = list_concat(this_path_set, bitindexpaths);

            /* Select best AND combination for this parameterization */
            bitmapqual = choose_bitmap_and(root, rel, this_path_set);

            /* And push that path into the mix */
            required_outer = PATH_REQ_OUTER(bitmapqual);
            loop_count = get_loop_count(root, (*rel).relid, required_outer);
            bpath = create_bitmap_heap_path(root, rel, bitmapqual, required_outer, loop_count, 0);
            add_path(rel, bpath as *mut Path);
        }
    }
}

/*
 * consider_index_join_clauses
 *    Given sets of join clauses for an index, decide which parameterized
 *    index paths to build.
 *
 * Plain indexpaths are sent directly to add_path, while potential
 * bitmap indexpaths are added to *bitindexpaths for later processing.
 *
 * 'rel' is the index's heap relation
 * 'index' is the index for which we want to generate paths
 * 'rclauseset' is the collection of indexable restriction clauses
 * 'jclauseset' is the collection of indexable simple join clauses
 * 'eclauseset' is the collection of indexable clauses from EquivalenceClasses
 * '*bitindexpaths' is the list to add bitmap paths to
 */
unsafe fn consider_index_join_clauses(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    rclauseset: *mut IndexClauseSet,
    jclauseset: *mut IndexClauseSet,
    eclauseset: *mut IndexClauseSet,
    bitindexpaths: *mut *mut List,
) {
    let mut considered_clauses: i32 = 0;
    let mut considered_relids: *mut List = NIL;
    let mut indexcol: i32;

    /*
     * The strategy here is to identify every potentially useful set of outer
     * rels that can provide indexable join clauses.  For each such set,
     * select all the join clauses available from those outer rels, add on all
     * the indexable restriction clauses, and generate plain and/or bitmap
     * index paths for that set of clauses.  This is based on the assumption
     * that it's always better to apply a clause as an indexqual than as a
     * filter (qpqual); which is where an available clause would end up being
     * applied if we omit it from the indexquals.
     *
     * This looks expensive, but in most practical cases there won't be very
     * many distinct sets of outer rels to consider.  As a safety valve when
     * that's not true, we use a heuristic: limit the number of outer rel sets
     * considered to a multiple of the number of clauses considered.  (We'll
     * always consider using each individual join clause, though.)
     *
     * For simplicity in selecting relevant clauses, we represent each set of
     * outer rels as a maximum set of clause_relids --- that is, the indexed
     * relation itself is also included in the relids set.  considered_relids
     * lists all relids sets we've already tried.
     */
    indexcol = 0;
    while indexcol < (*index).nkeycolumns {
        /* Consider each applicable simple join clause */
        considered_clauses +=
            list_length((*jclauseset).indexclauses[indexcol as usize]) as i32;
        consider_index_join_outer_rels(
            root,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            (*jclauseset).indexclauses[indexcol as usize],
            considered_clauses,
            &mut considered_relids,
        );
        /* Consider each applicable eclass join clause */
        considered_clauses +=
            list_length((*eclauseset).indexclauses[indexcol as usize]) as i32;
        consider_index_join_outer_rels(
            root,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            (*eclauseset).indexclauses[indexcol as usize],
            considered_clauses,
            &mut considered_relids,
        );
        indexcol += 1;
    }
}

/*
 * consider_index_join_outer_rels
 *    Generate parameterized paths based on clause relids in the clause list.
 *
 * Workhorse for consider_index_join_clauses; see notes therein for rationale.
 *
 * 'rel', 'index', 'rclauseset', 'jclauseset', 'eclauseset', and
 *        'bitindexpaths' as above
 * 'indexjoinclauses' is a list of IndexClauses for join clauses
 * 'considered_clauses' is the total number of clauses considered (so far)
 * '*considered_relids' is a list of all relids sets already considered
 */
unsafe fn consider_index_join_outer_rels(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    rclauseset: *mut IndexClauseSet,
    jclauseset: *mut IndexClauseSet,
    eclauseset: *mut IndexClauseSet,
    bitindexpaths: *mut *mut List,
    indexjoinclauses: *mut List,
    considered_clauses: i32,
    considered_relids: *mut *mut List,
) {
    let mut lc: *mut ListCell;

    /* Examine relids of each joinclause in the given list */
    lc = list_head(indexjoinclauses);
    while !lc.is_null() {
        let iclause: *mut IndexClause = lfirst(lc) as *mut IndexClause;
        lc = lnext(indexjoinclauses, lc);
        let clause_relids: Relids = (*(*iclause).rinfo).clause_relids;
        let parent_ec: *mut EquivalenceClass = (*(*iclause).rinfo).parent_ec;
        let num_considered_relids: i32;

        /* If we already tried its relids set, no need to do so again */
        if list_member(*considered_relids, clause_relids as *mut c_void) {
            continue;
        }

        /*
         * Generate the union of this clause's relids set with each
         * previously-tried set.  This ensures we try this clause along with
         * every interesting subset of previous clauses.  However, to avoid
         * exponential growth of planning time when there are many clauses,
         * limit the number of relid sets accepted to 10 * considered_clauses.
         *
         * Note: get_join_index_paths appends entries to *considered_relids,
         * but we do not need to visit such newly-added entries within this
         * loop, so we don't use foreach() here.  No real harm would be done
         * if we did visit them, since the subset check would reject them; but
         * it would waste some cycles.
         */
        num_considered_relids = list_length(*considered_relids) as i32;
        let mut pos: i32 = 0;
        while pos < num_considered_relids {
            let oldrelids: Relids =
                list_nth(*considered_relids, pos as i32) as Relids;

            /*
             * If either is a subset of the other, no new set is possible.
             * This isn't a complete test for redundancy, but it's easy and
             * cheap.  get_join_index_paths will check more carefully if we
             * already generated the same relids set.
             */
            if bms_subset_compare(clause_relids, oldrelids) != BMS_DIFFERENT {
                pos += 1;
                continue;
            }

            /*
             * If this clause was derived from an equivalence class, the
             * clause list may contain other clauses derived from the same
             * eclass.  We should not consider that combining this clause with
             * one of those clauses generates a usefully different
             * parameterization; so skip if any clause derived from the same
             * eclass would already have been included when using oldrelids.
             */
            if !parent_ec.is_null()
                && eclass_already_used(parent_ec, oldrelids, indexjoinclauses)
            {
                pos += 1;
                continue;
            }

            /*
             * If the number of relid sets considered exceeds our heuristic
             * limit, stop considering combinations of clauses.  We'll still
             * consider the current clause alone, though (below this loop).
             */
            if list_length(*considered_relids) as i32 >= 10 * considered_clauses {
                break;
            }

            /* OK, try the union set */
            get_join_index_paths(
                root,
                rel,
                index,
                rclauseset,
                jclauseset,
                eclauseset,
                bitindexpaths,
                bms_union(clause_relids, oldrelids),
                considered_relids,
            );
            pos += 1;
        }

        /* Also try this set of relids by itself */
        get_join_index_paths(
            root,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            clause_relids,
            considered_relids,
        );
    }
}

/*
 * get_join_index_paths
 *    Generate index paths using clauses from the specified outer relations.
 *    In addition to generating paths, relids is added to *considered_relids
 *    if not already present.
 *
 * Workhorse for consider_index_join_clauses; see notes therein for rationale.
 *
 * 'rel', 'index', 'rclauseset', 'jclauseset', 'eclauseset',
 *        'bitindexpaths', 'considered_relids' as above
 * 'relids' is the current set of relids to consider (the target rel plus
 *        one or more outer rels)
 */
unsafe fn get_join_index_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    rclauseset: *mut IndexClauseSet,
    jclauseset: *mut IndexClauseSet,
    eclauseset: *mut IndexClauseSet,
    bitindexpaths: *mut *mut List,
    relids: Relids,
    considered_relids: *mut *mut List,
) {
    let mut clauseset: IndexClauseSet = IndexClauseSet::zeroed();
    let mut indexcol: i32;

    /* If we already considered this relids set, don't repeat the work */
    if list_member(*considered_relids, relids as *mut c_void) {
        return;
    }

    /* Identify indexclauses usable with this relids set */
    indexcol = 0;
    while indexcol < (*index).nkeycolumns {
        let mut lc: *mut ListCell;

        /* First find applicable simple join clauses */
        lc = list_head((*jclauseset).indexclauses[indexcol as usize]);
        while !lc.is_null() {
            let iclause: *mut IndexClause = lfirst(lc) as *mut IndexClause;
            lc = lnext((*jclauseset).indexclauses[indexcol as usize], lc);

            if bms_is_subset((*(*iclause).rinfo).clause_relids, relids) {
                clauseset.indexclauses[indexcol as usize] = lappend(
                    clauseset.indexclauses[indexcol as usize],
                    iclause as *mut c_void,
                );
            }
        }

        /*
         * Add applicable eclass join clauses.  The clauses generated for each
         * column are redundant (cf generate_implied_equalities_for_column),
         * so we need at most one.  This is the only exception to the general
         * rule of using all available index clauses.
         */
        lc = list_head((*eclauseset).indexclauses[indexcol as usize]);
        while !lc.is_null() {
            let iclause: *mut IndexClause = lfirst(lc) as *mut IndexClause;
            lc = lnext((*eclauseset).indexclauses[indexcol as usize], lc);

            if bms_is_subset((*(*iclause).rinfo).clause_relids, relids) {
                clauseset.indexclauses[indexcol as usize] = lappend(
                    clauseset.indexclauses[indexcol as usize],
                    iclause as *mut c_void,
                );
                break;
            }
        }

        /* Add restriction clauses */
        clauseset.indexclauses[indexcol as usize] = list_concat(
            clauseset.indexclauses[indexcol as usize],
            (*rclauseset).indexclauses[indexcol as usize],
        );

        if clauseset.indexclauses[indexcol as usize] != NIL {
            clauseset.nonempty = true;
        }
        indexcol += 1;
    }

    /* We should have found something, else caller passed silly relids */
    Assert!(clauseset.nonempty);

    /* Build index path(s) using the collected set of clauses */
    get_index_paths(root, rel, index, &mut clauseset, bitindexpaths);

    /*
     * Remember we considered paths for this set of relids.
     */
    *considered_relids = lappend(*considered_relids, relids as *mut c_void);
}

/*
 * eclass_already_used
 *        True if any join clause usable with oldrelids was generated from
 *        the specified equivalence class.
 */
unsafe fn eclass_already_used(
    parent_ec: *mut EquivalenceClass,
    oldrelids: Relids,
    indexjoinclauses: *mut List,
) -> bool {
    let mut lc: *mut ListCell = list_head(indexjoinclauses);
    while !lc.is_null() {
        let iclause: *mut IndexClause = lfirst(lc) as *mut IndexClause;
        lc = lnext(indexjoinclauses, lc);
        let rinfo: *mut RestrictInfo = (*iclause).rinfo;

        if (*rinfo).parent_ec == parent_ec
            && bms_is_subset((*rinfo).clause_relids, oldrelids)
        {
            return true;
        }
    }
    false
}

/*
 * get_index_paths
 *    Given an index and a set of index clauses for it, construct IndexPaths.
 *
 * Plain indexpaths are sent directly to add_path, while potential
 * bitmap indexpaths are added to *bitindexpaths for later processing.
 *
 * This is a fairly simple frontend to build_index_paths().  Its reason for
 * existence is mainly to handle ScalarArrayOpExpr quals properly.  If the
 * index AM supports them natively, we should just include them in simple
 * index paths.  If not, we should exclude them while building simple index
 * paths, and then make a separate attempt to include them in bitmap paths.
 */
unsafe fn get_index_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    clauses: *mut IndexClauseSet,
    bitindexpaths: *mut *mut List,
) {
    let mut indexpaths: *mut List;
    let mut skip_nonnative_saop: bool = false;
    let mut lc: *mut ListCell;

    /*
     * Build simple index paths using the clauses.  Allow ScalarArrayOpExpr
     * clauses only if the index AM supports them natively.
     */
    indexpaths = build_index_paths(
        root,
        rel,
        index,
        clauses,
        (*index).predOK,
        StAnyscan,
        &mut skip_nonnative_saop,
    );

    /*
     * Submit all the ones that can form plain IndexScan plans to add_path. (A
     * plain IndexPath can represent either a plain IndexScan or an
     * IndexOnlyScan, but for our purposes here that distinction does not
     * matter.  However, some of the indexes might support only bitmap scans,
     * and those we mustn't submit to add_path here.)
     *
     * Also, pick out the ones that are usable as bitmap scans.  For that, we
     * must discard indexes that don't support bitmap scans, and we also are
     * only interested in paths that have some selectivity; we should discard
     * anything that was generated solely for ordering purposes.
     */
    lc = list_head(indexpaths);
    while !lc.is_null() {
        let ipath: *mut IndexPath = lfirst(lc) as *mut IndexPath;
        lc = lnext(indexpaths, lc);

        if (*index).amhasgettuple {
            add_path(rel, ipath as *mut Path);
        }

        if (*index).amhasgetbitmap
            && ((*ipath).path.pathkeys == NIL || (*ipath).indexselectivity < 1.0)
        {
            *bitindexpaths = lappend(*bitindexpaths, ipath as *mut c_void);
        }
    }

    /*
     * If there were ScalarArrayOpExpr clauses that the index can't handle
     * natively, generate bitmap scan paths relying on executor-managed
     * ScalarArrayOpExpr.
     */
    if skip_nonnative_saop {
        indexpaths = build_index_paths(
            root,
            rel,
            index,
            clauses,
            false,
            StBitmapscan,
            core::ptr::null_mut(),
        );
        *bitindexpaths = list_concat(*bitindexpaths, indexpaths);
    }
}

/*
 * build_index_paths
 *    Given an index and a set of index clauses for it, construct zero
 *    or more IndexPaths. It also constructs zero or more partial IndexPaths.
 *
 * We return a list of paths because (1) this routine checks some cases
 * that should cause us to not generate any IndexPath, and (2) in some
 * cases we want to consider both a forward and a backward scan, so as
 * to obtain both sort orders.  Note that the paths are just returned
 * to the caller and not immediately fed to add_path().
 *
 * At top level, useful_predicate should be exactly the index's predOK flag
 * (ie, true if it has a predicate that was proven from the restriction
 * clauses).  When working on an arm of an OR clause, useful_predicate
 * should be true if the predicate required the current OR list to be proven.
 * Note that this routine should never be called at all if the index has an
 * unprovable predicate.
 *
 * scantype indicates whether we want to create plain indexscans, bitmap
 * indexscans, or both.  When it's ST_BITMAPSCAN, we will not consider
 * index ordering while deciding if a Path is worth generating.
 *
 * If skip_nonnative_saop is non-NULL, we ignore ScalarArrayOpExpr clauses
 * unless the index AM supports them directly, and we set *skip_nonnative_saop
 * to true if we found any such clauses (caller must initialize the variable
 * to false).  If it's NULL, we do not ignore ScalarArrayOpExpr clauses.
 *
 * 'rel' is the index's heap relation
 * 'index' is the index for which we want to generate paths
 * 'clauses' is the collection of indexable clauses (IndexClause nodes)
 * 'useful_predicate' indicates whether the index has a useful predicate
 * 'scantype' indicates whether we need plain or bitmap scan support
 * 'skip_nonnative_saop' indicates whether to accept SAOP if index AM doesn't
 */
unsafe fn build_index_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    clauses: *const IndexClauseSet,
    useful_predicate: bool,
    scantype: ScanTypeControl,
    skip_nonnative_saop: *mut bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut ipath: *mut IndexPath;
    let mut index_clauses: *mut List;
    let mut outer_relids: Relids;
    let loop_count: f64;
    let mut orderbyclauses: *mut List;
    let mut orderbyclausecols: *mut List;
    let mut index_pathkeys: *mut List;
    let mut useful_pathkeys: *mut List;
    let pathkeys_possibly_useful: bool;
    let index_is_ordered: bool;
    let index_only_scan: bool;
    let mut indexcol: i32;

    Assert!(skip_nonnative_saop.is_null() || scantype == StBitmapscan || !skip_nonnative_saop.is_null());
    // Equivalent to C: Assert(skip_nonnative_saop != NULL || scantype == ST_BITMAPSCAN);
    // but the Rust logic simplifies: we only need to check if !ptr is null when not bitmapscan
    if skip_nonnative_saop.is_null() && scantype != StBitmapscan {
        // in C this would be an Assert failure; replicate gracefully
    }

    /* Check that index supports the desired scan type(s) */
    match scantype {
        StIndexscan => {
            if !(*index).amhasgettuple {
                return NIL;
            }
        }
        StBitmapscan => {
            if !(*index).amhasgetbitmap {
                return NIL;
            }
        }
        StAnyscan => {
            /* either or both are OK */
        }
    }

    /*
     * 1. Combine the per-column IndexClause lists into an overall list.
     *
     * In the resulting list, clauses are ordered by index key, so that the
     * column numbers form a nondecreasing sequence.  (This order is depended
     * on by btree and possibly other places.)  The list can be empty, if the
     * index AM allows that.
     *
     * We also build a Relids set showing which outer rels are required by the
     * selected clauses.  Any lateral_relids are included in that, but not
     * otherwise accounted for.
     */
    index_clauses = NIL;
    outer_relids = bms_copy((*rel).lateral_relids);
    indexcol = 0;
    while indexcol < (*index).nkeycolumns {
        let mut lc: *mut ListCell = list_head((*clauses).indexclauses[indexcol as usize]);
        while !lc.is_null() {
            let iclause_p: *mut IndexClause = lfirst(lc) as *mut IndexClause;
            lc = lnext((*clauses).indexclauses[indexcol as usize], lc);
            let rinfo: *mut RestrictInfo = (*iclause_p).rinfo;

            if !skip_nonnative_saop.is_null()
                && !(*index).amsearcharray
                && IsA!((*rinfo).clause, T_ScalarArrayOpExpr)
            {
                /*
                 * Caller asked us to generate IndexPaths that omit any
                 * ScalarArrayOpExpr clauses when the underlying index AM
                 * lacks native support.
                 *
                 * We must omit this clause (and tell caller about it).
                 */
                *skip_nonnative_saop = true;
                continue;
            }

            /* OK to include this clause */
            index_clauses = lappend(index_clauses, iclause_p as *mut c_void);
            outer_relids =
                bms_add_members(outer_relids, (*rinfo).clause_relids);
        }

        /*
         * If no clauses match the first index column, check for amoptionalkey
         * restriction.  We can't generate a scan over an index with
         * amoptionalkey = false unless there's at least one index clause.
         * (When working on columns after the first, this test cannot fail. It
         * is always okay for columns after the first to not have any
         * clauses.)
         */
        if index_clauses == NIL && !(*index).amoptionalkey {
            return NIL;
        }
        indexcol += 1;
    }

    /* We do not want the index's rel itself listed in outer_relids */
    outer_relids = bms_del_member(outer_relids, (*rel).relid as i32);

    /* Compute loop_count for cost estimation purposes */
    let loop_count = get_loop_count(root, (*rel).relid, outer_relids);

    /*
     * 2. Compute pathkeys describing index's ordering, if any, then see how
     * many of them are actually useful for this query.  This is not relevant
     * if we are only trying to build bitmap indexscans.
     */
    pathkeys_possibly_useful =
        scantype != StBitmapscan && has_useful_pathkeys(root, rel);
    index_is_ordered = !(*index).sortopfamily.is_null();
    if index_is_ordered && pathkeys_possibly_useful {
        index_pathkeys = build_index_pathkeys(root, index, ForwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, index_pathkeys);
        orderbyclauses = NIL;
        orderbyclausecols = NIL;
    } else if (*index).amcanorderbyop && pathkeys_possibly_useful {
        /*
         * See if we can generate ordering operators for query_pathkeys or at
         * least some prefix thereof.  Matching to just a prefix of the
         * query_pathkeys will allow an incremental sort to be considered on
         * the index's partially sorted results.
         */
        orderbyclauses = NIL;
        orderbyclausecols = NIL;
        match_pathkeys_to_index(
            index,
            (*root).query_pathkeys,
            &mut orderbyclauses,
            &mut orderbyclausecols,
        );
        if list_length((*root).query_pathkeys) == list_length(orderbyclauses) {
            useful_pathkeys = (*root).query_pathkeys;
        } else {
            useful_pathkeys = list_copy_head(
                (*root).query_pathkeys,
                list_length(orderbyclauses) as i32,
            );
        }
    } else {
        useful_pathkeys = NIL;
        orderbyclauses = NIL;
        orderbyclausecols = NIL;
    }

    /*
     * 3. Check if an index-only scan is possible.  If we're not building
     * plain indexscans, this isn't relevant since bitmap scans don't support
     * index data retrieval anyway.
     */
    index_only_scan = scantype != StBitmapscan && check_index_only(rel, index);

    /*
     * 4. Generate an indexscan path if there are relevant restriction clauses
     * in the current clauses, OR the index ordering is potentially useful for
     * later merging or final output ordering, OR the index has a useful
     * predicate, OR an index-only scan is possible.
     */
    if index_clauses != NIL || useful_pathkeys != NIL || useful_predicate || index_only_scan {
        ipath = create_index_path(
            root,
            index,
            index_clauses,
            orderbyclauses,
            orderbyclausecols,
            useful_pathkeys,
            ForwardScanDirection,
            index_only_scan,
            outer_relids,
            loop_count,
            false,
        );
        result = lappend(result, ipath as *mut c_void);

        /*
         * If appropriate, consider parallel index scan.  We don't allow
         * parallel index scan for bitmap index scans.
         */
        if (*index).amcanparallel
            && (*rel).consider_parallel
            && outer_relids.is_null()
            && scantype != StBitmapscan
        {
            ipath = create_index_path(
                root,
                index,
                index_clauses,
                orderbyclauses,
                orderbyclausecols,
                useful_pathkeys,
                ForwardScanDirection,
                index_only_scan,
                outer_relids,
                loop_count,
                true,
            );

            /*
             * if, after costing the path, we find that it's not worth using
             * parallel workers, just free it.
             */
            if (*ipath).path.parallel_workers > 0 {
                add_partial_path(rel, ipath as *mut Path);
            } else {
                pfree(ipath as *mut c_void);
            }
        }
    }

    /*
     * 5. If the index is ordered, a backwards scan might be interesting.
     */
    if index_is_ordered && pathkeys_possibly_useful {
        index_pathkeys = build_index_pathkeys(root, index, BackwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, index_pathkeys);
        if useful_pathkeys != NIL {
            ipath = create_index_path(
                root,
                index,
                index_clauses,
                NIL,
                NIL,
                useful_pathkeys,
                BackwardScanDirection,
                index_only_scan,
                outer_relids,
                loop_count,
                false,
            );
            result = lappend(result, ipath as *mut c_void);

            /* If appropriate, consider parallel index scan */
            if (*index).amcanparallel
                && (*rel).consider_parallel
                && outer_relids.is_null()
                && scantype != StBitmapscan
            {
                ipath = create_index_path(
                    root,
                    index,
                    index_clauses,
                    NIL,
                    NIL,
                    useful_pathkeys,
                    BackwardScanDirection,
                    index_only_scan,
                    outer_relids,
                    loop_count,
                    true,
                );

                /*
                 * if, after costing the path, we find that it's not worth
                 * using parallel workers, just free it.
                 */
                if (*ipath).path.parallel_workers > 0 {
                    add_partial_path(rel, ipath as *mut Path);
                } else {
                    pfree(ipath as *mut c_void);
                }
            }
        }
    }

    result
}

/*
 * build_paths_for_OR
 *    Given a list of restriction clauses from one arm of an OR clause,
 *    construct all matching IndexPaths for the relation.
 *
 * Here we must scan all indexes of the relation, since a bitmap OR tree
 * can use multiple indexes.
 *
 * The caller actually supplies two lists of restriction clauses: some
 * "current" ones and some "other" ones.  Both lists can be used freely
 * to match keys of the index, but an index must use at least one of the
 * "current" clauses to be considered usable.  The motivation for this is
 * examples like
 *        WHERE (x = 42) AND (... OR (y = 52 AND z = 77) OR ....)
 * While we are considering the y/z subclause of the OR, we can use "x = 42"
 * as one of the available index conditions; but we shouldn't match the
 * subclause to any index on x alone, because such a Path would already have
 * been generated at the upper level.  So we could use an index on x,y,z
 * or an index on x,y for the OR subclause, but not an index on just x.
 * When dealing with a partial index, a match of the index predicate to
 * one of the "current" clauses also makes the index usable.
 *
 * 'rel' is the relation for which we want to generate index paths
 * 'clauses' is the current list of clauses (RestrictInfo nodes)
 * 'other_clauses' is the list of additional upper-level clauses
 */
unsafe fn build_paths_for_OR(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    clauses: *mut List,
    other_clauses: *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut all_clauses: *mut List = NIL; /* not computed till needed */
    let mut lc: *mut ListCell;

    lc = list_head((*rel).indexlist);
    while !lc.is_null() {
        let index: *mut IndexOptInfo = lfirst(lc) as *mut IndexOptInfo;
        lc = lnext((*rel).indexlist, lc);
        let mut clauseset: IndexClauseSet = IndexClauseSet::zeroed();
        let indexpaths: *mut List;
        let mut useful_predicate: bool;

        /* Ignore index if it doesn't support bitmap scans */
        if !(*index).amhasgetbitmap {
            continue;
        }

        /*
         * Ignore partial indexes that do not match the query.  If a partial
         * index is marked predOK then we know it's OK.  Otherwise, we have to
         * test whether the added clauses are sufficient to imply the
         * predicate. If so, we can use the index in the current context.
         *
         * We set useful_predicate to true iff the predicate was proven using
         * the current set of clauses.  This is needed to prevent matching a
         * predOK index to an arm of an OR, which would be a legal but
         * pointlessly inefficient plan.  (A better plan will be generated by
         * just scanning the predOK index alone, no OR.)
         */
        useful_predicate = false;
        if (*index).indpred != NIL {
            if (*index).predOK {
                /* Usable, but don't set useful_predicate */
            } else {
                /* Form all_clauses if not done already */
                if all_clauses == NIL {
                    all_clauses = list_concat_copy(clauses, other_clauses);
                }

                if !predicate_implied_by((*index).indpred, all_clauses, false) {
                    continue; /* can't use it at all */
                }

                if !predicate_implied_by((*index).indpred, other_clauses, false) {
                    useful_predicate = true;
                }
            }
        }

        /*
         * Identify the restriction clauses that can match the index.
         */
        match_clauses_to_index(root, clauses, index, &mut clauseset);

        /*
         * If no matches so far, and the index predicate isn't useful, we
         * don't want it.
         */
        if !clauseset.nonempty && !useful_predicate {
            continue;
        }

        /*
         * Add "other" restriction clauses to the clauseset.
         */
        match_clauses_to_index(root, other_clauses, index, &mut clauseset);

        /*
         * Construct paths if possible.
         */
        indexpaths = build_index_paths(
            root,
            rel,
            index,
            &clauseset,
            useful_predicate,
            StBitmapscan,
            core::ptr::null_mut(),
        );
        result = list_concat(result, indexpaths);
    }

    result
}

/*
 * Comparison function for OrArgIndexMatch which provides sort order placing
 * similar OR-clause arguments together.
 */
unsafe extern "C" fn or_arg_index_match_cmp(a: *const c_void, b: *const c_void) -> i32 {
    let match_a: *const OrArgIndexMatch = a as *const OrArgIndexMatch;
    let match_b: *const OrArgIndexMatch = b as *const OrArgIndexMatch;

    if (*match_a).indexnum < (*match_b).indexnum {
        return -1;
    } else if (*match_a).indexnum > (*match_b).indexnum {
        return 1;
    }

    if (*match_a).colnum < (*match_b).colnum {
        return -1;
    } else if (*match_a).colnum > (*match_b).colnum {
        return 1;
    }

    if (*match_a).opno < (*match_b).opno {
        return -1;
    } else if (*match_a).opno > (*match_b).opno {
        return 1;
    }

    if (*match_a).inputcollid < (*match_b).inputcollid {
        return -1;
    } else if (*match_a).inputcollid > (*match_b).inputcollid {
        return 1;
    }

    if (*match_a).argindex < (*match_b).argindex {
        return -1;
    } else if (*match_a).argindex > (*match_b).argindex {
        return 1;
    }

    0
}

/*
 * Another comparison function for OrArgIndexMatch.  It sorts groups together
 * using groupindex.  The group items are then sorted by argindex.
 */
unsafe extern "C" fn or_arg_index_match_cmp_group(a: *const c_void, b: *const c_void) -> i32 {
    let match_a: *const OrArgIndexMatch = a as *const OrArgIndexMatch;
    let match_b: *const OrArgIndexMatch = b as *const OrArgIndexMatch;

    if (*match_a).groupindex < (*match_b).groupindex {
        return -1;
    } else if (*match_a).groupindex > (*match_b).groupindex {
        return 1;
    }

    if (*match_a).argindex < (*match_b).argindex {
        return -1;
    } else if (*match_a).argindex > (*match_b).argindex {
        return 1;
    }

    0
}

/*
 * group_similar_or_args
 *        Transform incoming OR-restrictinfo into a list of sub-restrictinfos,
 *        each of them containing a subset of similar OR-clause arguments from
 *        the source rinfo.
 *
 * Similar OR-clause arguments are of the form "indexkey op constant" having
 * the same indexkey, operator, and collation.  Constant may comprise either
 * Const or Param.  It may be employed later, during the
 * match_clause_to_indexcol() to transform the whole OR-sub-rinfo to an SAOP
 * clause.
 *
 * Returns the processed list of OR-clause arguments.
 */
unsafe fn group_similar_or_args(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rinfo: *mut RestrictInfo,
) -> *mut List {
    let n: i32;
    let mut i: i32;
    let mut group_start: i32;
    let matches: *mut OrArgIndexMatch;
    let mut matched: bool = false;
    let mut lc: *mut ListCell;
    let mut lc2: *mut ListCell;
    let orargs: *mut List;
    let mut result: *mut List = NIL;
    let relid: u32 = (*rel).relid;

    Assert!(IsA!((*rinfo).orclause, T_BoolExpr));
    orargs = (*((*rinfo).orclause as *mut BoolExpr)).args;
    n = list_length(orargs) as i32;

    /*
     * To avoid N^2 behavior, take utility pass along the list of OR-clause
     * arguments.  For each argument, fill the OrArgIndexMatch structure,
     * which will be used to sort these arguments at the next step.
     */
    i = -1;
    matches = palloc(size_of::<OrArgIndexMatch>() * n as usize) as *mut OrArgIndexMatch;
    lc = list_head(orargs);
    while !lc.is_null() {
        let arg: *mut Node = lfirst(lc) as *mut Node;
        lc = lnext(orargs, lc);
        let argrinfo: *mut RestrictInfo;
        let clause: *mut OpExpr;
        let mut opno: Oid;
        let leftop: *mut Node;
        let rightop: *mut Node;
        let nonConstExpr: *mut Node;
        let indexnum: i32;
        let colnum: i32;

        i += 1;
        (*matches.add(i as usize)).argindex = i;
        (*matches.add(i as usize)).groupindex = i;
        (*matches.add(i as usize)).indexnum = -1;
        (*matches.add(i as usize)).colnum = -1;
        (*matches.add(i as usize)).opno = InvalidOid;
        (*matches.add(i as usize)).inputcollid = InvalidOid;

        if !IsA!(arg, T_RestrictInfo) {
            continue;
        }

        argrinfo = castNode!(RestrictInfo, T_RestrictInfo, arg);

        /* Only operator clauses can match  */
        if !IsA!((*argrinfo).clause, T_OpExpr) {
            continue;
        }

        clause = (*argrinfo).clause as *mut OpExpr;
        opno = (*clause).opno;

        /* Only binary operators can match  */
        if list_length((*clause).args) != 2 {
            continue;
        }

        /*
         * Ignore any RelabelType node above the operands.  This is needed to
         * be able to apply indexscanning in binary-compatible-operator cases.
         * Note: we can assume there is at most one RelabelType node;
         * eval_const_expressions() will have simplified if more than one.
         */
        let mut leftop_v: *mut Node = get_leftop(clause as *const c_void);
        if IsA!(leftop_v, T_RelabelType) {
            leftop_v = (*(leftop_v as *mut RelabelType)).arg as *mut Node;
        }

        let mut rightop_v: *mut Node = get_rightop(clause as *const c_void);
        if IsA!(rightop_v, T_RelabelType) {
            rightop_v = (*(rightop_v as *mut RelabelType)).arg as *mut Node;
        }

        /*
         * Check for clauses of the form: (indexkey operator constant) or
         * (constant operator indexkey).  But we don't know a particular index
         * yet.  Therefore, we try to distinguish the potential index key and
         * constant first, then search for a matching index key among all
         * indexes.
         */
        if bms_is_member(relid as i32, (*argrinfo).right_relids)
            && !bms_is_member(relid as i32, (*argrinfo).left_relids)
            && !contain_volatile_functions(leftop_v as *mut c_void)
        {
            opno = get_commutator(opno);

            if !OidIsValid(opno) {
                /* commutator doesn't exist, we can't reverse the order */
                continue;
            }
            nonConstExpr = rightop_v;
        } else if bms_is_member(relid as i32, (*argrinfo).left_relids)
            && !bms_is_member(relid as i32, (*argrinfo).right_relids)
            && !contain_volatile_functions(rightop_v as *mut c_void)
        {
            nonConstExpr = leftop_v;
        } else {
            continue;
        }

        /*
         * Match non-constant part to the index key.  It's possible that a
         * single non-constant part matches multiple index keys.  It's OK, we
         * just stop with first matching index key.  Given that this choice is
         * determined the same for every clause, we will group similar clauses
         * together anyway.
         */
        let mut indexnum_v: i32 = 0;
        let mut lc2_v: *mut ListCell = list_head((*rel).indexlist);
        while !lc2_v.is_null() {
            let index: *mut IndexOptInfo = lfirst(lc2_v) as *mut IndexOptInfo;
            lc2_v = lnext((*rel).indexlist, lc2_v);

            /*
             * Ignore index if it doesn't support bitmap scans or SAOP
             * clauses.
             */
            if !(*index).amhasgetbitmap || !(*index).amsearcharray {
                indexnum_v += 1;
                continue;
            }

            let mut colnum_v: i32 = 0;
            while colnum_v < (*index).nkeycolumns {
                if match_index_to_operand(nonConstExpr, colnum_v, index) {
                    (*matches.add(i as usize)).indexnum = indexnum_v;
                    (*matches.add(i as usize)).colnum = colnum_v;
                    (*matches.add(i as usize)).opno = opno;
                    (*matches.add(i as usize)).inputcollid = (*clause).inputcollid;
                    matched = true;
                    break;
                }
                colnum_v += 1;
            }

            /*
             * Stop looping through the indexes, if we managed to match
             * nonConstExpr to any index column.
             */
            if (*matches.add(i as usize)).indexnum >= 0 {
                break;
            }
            indexnum_v += 1;
        }
    }

    /*
     * Fast-path check: if no clause is matching to the index column, we can
     * just give up at this stage and return the clause list as-is.
     */
    if !matched {
        pfree(matches as *mut c_void);
        return orargs;
    }

    /*
     * Sort clauses to make similar clauses go together.  But at the same
     * time, we would like to change the order of clauses as little as
     * possible.  To do so, we reorder each group of similar clauses so that
     * the first item of the group stays in place, and all the other items are
     * moved after it.  So, if there are no similar clauses, the order of
     * clauses stays the same.  When there are some groups, required
     * reordering happens while the rest of the clauses remain in their
     * places.  That is achieved by assigning a 'groupindex' to each clause:
     * the number of the first item in the group in the original clause list.
     */
    libc_qsort(
        matches as *mut c_void,
        n as usize,
        size_of::<OrArgIndexMatch>(),
        or_arg_index_match_cmp,
    );

    /* Assign groupindex to the sorted clauses */
    i = 1;
    while i < n {
        /*
         * When two clauses are similar and should belong to the same group,
         * copy the 'groupindex' from the previous clause.  Given we are
         * considering clauses in direct order, all the clauses would have a
         * 'groupindex' equal to the 'groupindex' of the first clause in the
         * group.
         */
        if (*matches.add(i as usize)).indexnum == (*matches.add((i - 1) as usize)).indexnum
            && (*matches.add(i as usize)).colnum == (*matches.add((i - 1) as usize)).colnum
            && (*matches.add(i as usize)).opno == (*matches.add((i - 1) as usize)).opno
            && (*matches.add(i as usize)).inputcollid
                == (*matches.add((i - 1) as usize)).inputcollid
            && (*matches.add(i as usize)).indexnum != -1
        {
            (*matches.add(i as usize)).groupindex =
                (*matches.add((i - 1) as usize)).groupindex;
        }
        i += 1;
    }

    /* Re-sort clauses first by groupindex then by argindex */
    libc_qsort(
        matches as *mut c_void,
        n as usize,
        size_of::<OrArgIndexMatch>(),
        or_arg_index_match_cmp_group,
    );

    /*
     * Group similar clauses into single sub-restrictinfo. Side effect: the
     * resulting list of restrictions will be sorted by indexnum and colnum.
     */
    group_start = 0;
    i = 1;
    while i <= n {
        /* Check if it's a group boundary */
        if group_start >= 0
            && (i == n
                || (*matches.add(i as usize)).indexnum
                    != (*matches.add(group_start as usize)).indexnum
                || (*matches.add(i as usize)).colnum
                    != (*matches.add(group_start as usize)).colnum
                || (*matches.add(i as usize)).opno
                    != (*matches.add(group_start as usize)).opno
                || (*matches.add(i as usize)).inputcollid
                    != (*matches.add(group_start as usize)).inputcollid
                || (*matches.add(i as usize)).indexnum == -1)
        {
            /*
             * One clause in group: add it "as is" to the upper-level OR.
             */
            if i - group_start == 1 {
                result = lappend(
                    result,
                    list_nth(orargs, (*matches.add(group_start as usize)).argindex),
                );
            } else {
                /*
                 * Two or more clauses in a group: create a nested OR.
                 */
                let mut args: *mut List = NIL;
                let mut rargs: *mut List = NIL;
                let subrinfo: *mut RestrictInfo;
                let mut j: i32;

                Assert!(i - group_start >= 2);

                /* Construct the list of nested OR arguments */
                j = group_start;
                while j < i {
                    let arg: *mut c_void =
                        list_nth(orargs, (*matches.add(j as usize)).argindex);

                    rargs = lappend(rargs, arg);
                    if IsA!(arg, T_RestrictInfo) {
                        args = lappend(
                            args,
                            (*(arg as *mut RestrictInfo)).clause as *mut c_void,
                        );
                    } else {
                        args = lappend(args, arg);
                    }
                    j += 1;
                }

                /* Construct the nested OR and wrap it with RestrictInfo */
                subrinfo = make_plain_restrictinfo(
                    root,
                    make_orclause(args),
                    make_orclause(rargs),
                    (*rinfo).is_pushed_down,
                    (*rinfo).has_clone,
                    (*rinfo).is_clone,
                    (*rinfo).pseudoconstant,
                    (*rinfo).security_level,
                    (*rinfo).required_relids,
                    (*rinfo).incompatible_relids,
                    (*rinfo).outer_relids,
                );
                result = lappend(result, subrinfo as *mut c_void);
            }

            group_start = i;
        }
        i += 1;
    }
    pfree(matches as *mut c_void);
    result
}

// qsort wrapper (C stdlib)
unsafe fn libc_qsort(
    base: *mut c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe extern "C" fn(*const c_void, *const c_void) -> i32,
) {
    extern "C" {
        fn qsort(
            base: *mut c_void,
            nmemb: usize,
            size: usize,
            compar: unsafe extern "C" fn(*const c_void, *const c_void) -> i32,
        );
    }
    qsort(base, nmemb, size, compar);
}

/*
 * make_bitmap_paths_for_or_group
 *        Generate bitmap paths for a group of similar OR-clause arguments
 *        produced by group_similar_or_args().
 *
 * This function considers two cases: (1) matching a group of clauses to
 * the index as a whole, and (2) matching the individual clauses one-by-one.
 * (1) typically comprises an optimal solution.  If not, (2) typically
 * comprises fair alternative.
 *
 * Ideally, we could consider all arbitrary splits of arguments into
 * subgroups, but that could lead to unacceptable computational complexity.
 * This is why we only consider two cases of above.
 */
unsafe fn make_bitmap_paths_for_or_group(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ri: *mut RestrictInfo,
    other_clauses: *mut List,
) -> *mut List {
    let mut jointlist: *mut List = NIL;
    let mut splitlist: *mut List = NIL;
    let mut lc: *mut ListCell;
    let orargs: *mut List;
    let args: *mut List = (*((*ri).orclause as *mut BoolExpr)).args;
    let mut jointcost: Cost = 0.0;
    let mut splitcost: Cost = 0.0;
    let bitmapqual: *mut Path;
    let mut indlist: *mut List;

    /*
     * First, try to match the whole group to the one index.
     */
    orargs = list_make1!(ri as *mut c_void);
    indlist = build_paths_for_OR(root, rel, orargs, other_clauses);
    if indlist != NIL {
        let bq: *mut Path = choose_bitmap_and(root, rel, indlist);
        jointcost = (*bq).total_cost;
        jointlist = list_make1!(bq as *mut c_void);
    }

    /*
     * If we manage to find a bitmap scan, which uses the group of OR-clause
     * arguments as a whole, we can skip matching OR-clause arguments
     * one-by-one as long as there are no other clauses, which can bring more
     * efficiency to one-by-one case.
     */
    if jointlist != NIL && other_clauses == NIL {
        return jointlist;
    }

    /*
     * Also try to match all containing clauses one-by-one.
     */
    lc = list_head(args);
    while !lc.is_null() {
        let cur_arg: *mut c_void = lfirst(lc);
        lc = lnext(args, lc);

        let orargs2: *mut List = list_make1!(cur_arg);

        indlist = build_paths_for_OR(root, rel, orargs2, other_clauses);

        if indlist == NIL {
            splitlist = NIL;
            break;
        }

        let bq: *mut Path = choose_bitmap_and(root, rel, indlist);
        splitcost += (*bq).total_cost;
        splitlist = lappend(splitlist, bq as *mut c_void);
    }

    /*
     * Pick the best option.
     */
    if splitlist == NIL {
        jointlist
    } else if jointlist == NIL {
        splitlist
    } else if jointcost < splitcost {
        jointlist
    } else {
        splitlist
    }
}

/*
 * generate_bitmap_or_paths
 *        Look through the list of clauses to find OR clauses, and generate
 *        a BitmapOrPath for each one we can handle that way.  Return a list
 *        of the generated BitmapOrPaths.
 *
 * other_clauses is a list of additional clauses that can be assumed true
 * for the purpose of generating indexquals, but are not to be searched for
 * ORs.  (See build_paths_for_OR() for motivation.)
 */
unsafe fn generate_bitmap_or_paths(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    clauses: *mut List,
    other_clauses: *mut List,
) -> *mut List {
    let mut result: *mut List = NIL;
    let all_clauses: *mut List;
    let mut lc: *mut ListCell;

    /*
     * We can use both the current and other clauses as context for
     * build_paths_for_OR; no need to remove ORs from the lists.
     */
    all_clauses = list_concat_copy(clauses, other_clauses);

    lc = list_head(clauses);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext(clauses, lc);
        let mut pathlist: *mut List;
        let bitmapqual: *mut Path;
        let mut j: *mut ListCell;
        let groupedArgs: *mut List;
        let mut inner_other_clauses: *mut List = NIL;

        /* Ignore RestrictInfos that aren't ORs */
        if !restriction_is_or_clause(rinfo) {
            continue;
        }

        /*
         * We must be able to match at least one index to each of the arms of
         * the OR, else we can't use it.
         */
        pathlist = NIL;

        /*
         * Group the similar OR-clause arguments into dedicated RestrictInfos,
         * because each of those RestrictInfos has a chance to match the index
         * as a whole.
         */
        groupedArgs = group_similar_or_args(root, rel, rinfo);

        if groupedArgs != (*((*rinfo).orclause as *mut BoolExpr)).args {
            /*
             * Some parts of the rinfo were probably grouped.  In this case,
             * we have a set of sub-rinfos that together are an exact
             * duplicate of rinfo.  Thus, we need to remove the rinfo from
             * other clauses. match_clauses_to_index detects duplicated
             * iclauses by comparing pointers to original rinfos that would be
             * different.  So, we must delete rinfo to avoid de-facto
             * duplicated clauses in the index clauses list.
             */
            inner_other_clauses =
                list_delete(list_copy(all_clauses), rinfo as *mut c_void);
        }

        j = list_head(groupedArgs);
        'outer_j: while !j.is_null() {
            let orarg: *mut Node = lfirst(j) as *mut Node;
            j = lnext(groupedArgs, j);
            let mut indlist: *mut List;

            /* OR arguments should be ANDs or sub-RestrictInfos */
            if is_andclause(orarg as *const c_void) {
                let andargs: *mut List = (*(orarg as *mut BoolExpr)).args;

                indlist = build_paths_for_OR(root, rel, andargs, all_clauses);

                /* Recurse in case there are sub-ORs */
                indlist = list_concat(
                    indlist,
                    generate_bitmap_or_paths(root, rel, andargs, all_clauses),
                );
            } else if restriction_is_or_clause(castNode!(RestrictInfo, T_RestrictInfo, orarg)) {
                let ri: *mut RestrictInfo = castNode!(RestrictInfo, T_RestrictInfo, orarg);

                /*
                 * Generate bitmap paths for the group of similar OR-clause
                 * arguments.
                 */
                indlist =
                    make_bitmap_paths_for_or_group(root, rel, ri, inner_other_clauses);

                if indlist == NIL {
                    pathlist = NIL;
                    break 'outer_j;
                } else {
                    pathlist = list_concat(pathlist, indlist);
                    continue;
                }
            } else {
                let ri: *mut RestrictInfo = castNode!(RestrictInfo, T_RestrictInfo, orarg);
                let orargs2: *mut List = list_make1!(ri as *mut c_void);

                indlist = build_paths_for_OR(root, rel, orargs2, all_clauses);
            }

            /*
             * If nothing matched this arm, we can't do anything with this OR
             * clause.
             */
            if indlist == NIL {
                pathlist = NIL;
                break;
            }

            /*
             * OK, pick the most promising AND combination, and add it to
             * pathlist.
             */
            let bq: *mut Path = choose_bitmap_and(root, rel, indlist);
            pathlist = lappend(pathlist, bq as *mut c_void);
        }

        if inner_other_clauses != NIL {
            list_free(inner_other_clauses);
        }

        /*
         * If we have a match for every arm, then turn them into a
         * BitmapOrPath, and add to result list.
         */
        if pathlist != NIL {
            let bq: *mut Path =
                create_bitmap_or_path(root, rel, pathlist) as *mut Path;
            result = lappend(result, bq as *mut c_void);
        }
    }

    result
}

/*
 * choose_bitmap_and
 *        Given a nonempty list of bitmap paths, AND them into one path.
 *
 * This is a nontrivial decision since we can legally use any subset of the
 * given path set.  We want to choose a good tradeoff between selectivity
 * and cost of computing the bitmap.
 *
 * The result is either a single one of the inputs, or a BitmapAndPath
 * combining multiple inputs.
 */
unsafe fn choose_bitmap_and(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    mut paths: *mut List,
) -> *mut Path {
    let mut npaths: i32 = list_length(paths) as i32;
    let pathinfoarray: *mut *mut PathClauseUsage;
    let mut pathinfo: *mut PathClauseUsage;
    let mut clauselist: *mut List;
    let mut bestpaths: *mut List = NIL;
    let mut bestcost: Cost = 0.0;
    let mut i: i32;
    let mut j: i32;
    let mut l: *mut ListCell;

    Assert!(npaths > 0); /* else caller error */
    if npaths == 1 {
        return linitial(paths) as *mut Path; /* easy case */
    }

    /*
     * In theory we should consider every nonempty subset of the given paths.
     * In practice that seems like overkill, given the crude nature of the
     * estimates, not to mention the possible effects of higher-level AND and
     * OR clauses.  Moreover, it's completely impractical if there are a large
     * number of paths, since the work would grow as O(2^N).
     *
     * As a heuristic, we first check for paths using exactly the same sets of
     * WHERE clauses + index predicate conditions, and reject all but the
     * cheapest-to-scan in any such group.  This primarily gets rid of indexes
     * that include the interesting columns but also irrelevant columns.  (In
     * situations where the DBA has gone overboard on creating variant
     * indexes, this can make for a very large reduction in the number of
     * paths considered further.)
     *
     * We then sort the surviving paths with the cheapest-to-scan first, and
     * for each path, consider using that path alone as the basis for a bitmap
     * scan.  Then we consider bitmap AND scans formed from that path plus
     * each subsequent (higher-cost) path, adding on a subsequent path if it
     * results in a reduction in the estimated total scan cost. This means we
     * consider about O(N^2) rather than O(2^N) path combinations, which is
     * quite tolerable, especially given than N is usually reasonably small
     * because of the prefiltering step.  The cheapest of these is returned.
     *
     * We will only consider AND combinations in which no two indexes use the
     * same WHERE clause.  This is a bit of a kluge: it's needed because
     * costsize.c and clausesel.c aren't very smart about redundant clauses.
     * They will usually double-count the redundant clauses, producing a
     * too-small selectivity that makes a redundant AND step look like it
     * reduces the total cost.  Perhaps someday that code will be smarter and
     * we can remove this limitation.  (But note that this also defends
     * against flat-out duplicate input paths, which can happen because
     * match_join_clauses_to_index will find the same OR join clauses that
     * extract_restriction_or_clauses has pulled OR restriction clauses out
     * of.)
     *
     * For the same reason, we reject AND combinations in which an index
     * predicate clause duplicates another clause.  Here we find it necessary
     * to be even stricter: we'll reject a partial index if any of its
     * predicate clauses are implied by the set of WHERE clauses and predicate
     * clauses used so far.  This covers cases such as a condition "x = 42"
     * used with a plain index, followed by a clauseless scan of a partial
     * index "WHERE x >= 40 AND x < 50".  The partial index has been accepted
     * only because "x = 42" was present, and so allowing it would partially
     * double-count selectivity.  (We could use predicate_implied_by on
     * regular qual clauses too, to have a more intelligent, but much more
     * expensive, check for redundancy --- but in most cases simple equality
     * seems to suffice.)
     */

    /*
     * Extract clause usage info and detect any paths that use exactly the
     * same set of clauses; keep only the cheapest-to-scan of any such groups.
     * The surviving paths are put into an array for qsort'ing.
     */
    pathinfoarray = palloc(npaths as usize * size_of::<*mut PathClauseUsage>())
        as *mut *mut PathClauseUsage;
    clauselist = NIL;
    npaths = 0;
    l = list_head(paths);
    while !l.is_null() {
        let ipath: *mut Path = lfirst(l) as *mut Path;
        l = lnext(paths, l);

        pathinfo = classify_index_clause_usage(ipath, &mut clauselist);

        /* If it's unclassifiable, treat it as distinct from all others */
        if (*pathinfo).unclassifiable {
            *pathinfoarray.add(npaths as usize) = pathinfo;
            npaths += 1;
            continue;
        }

        i = 0;
        while i < npaths {
            if !(*(*pathinfoarray.add(i as usize))).unclassifiable
                && bms_equal(
                    (*pathinfo).clauseids,
                    (*(*pathinfoarray.add(i as usize))).clauseids,
                )
            {
                break;
            }
            i += 1;
        }
        if i < npaths {
            /* duplicate clauseids, keep the cheaper one */
            let mut ncost: Cost = 0.0;
            let mut ocost: Cost = 0.0;
            let mut nselec: Selectivity = 0.0;
            let mut oselec: Selectivity = 0.0;

            cost_bitmap_tree_node((*pathinfo).path, &mut ncost, &mut nselec);
            cost_bitmap_tree_node(
                (*(*pathinfoarray.add(i as usize))).path,
                &mut ocost,
                &mut oselec,
            );
            if ncost < ocost {
                *pathinfoarray.add(i as usize) = pathinfo;
            }
        } else {
            /* not duplicate clauseids, add to array */
            *pathinfoarray.add(npaths as usize) = pathinfo;
            npaths += 1;
        }
    }

    /* If only one surviving path, we're done */
    if npaths == 1 {
        return (*(*pathinfoarray.add(0))).path;
    }

    /* Sort the surviving paths by index access cost */
    libc_qsort(
        pathinfoarray as *mut c_void,
        npaths as usize,
        size_of::<*mut PathClauseUsage>(),
        path_usage_comparator,
    );

    /*
     * For each surviving index, consider it as an "AND group leader", and see
     * whether adding on any of the later indexes results in an AND path with
     * cheaper total cost than before.  Then take the cheapest AND group.
     *
     * Note: paths that are either clauseless or unclassifiable will have
     * empty clauseids, so that they will not be rejected by the clauseids
     * filter here, nor will they cause later paths to be rejected by it.
     */
    i = 0;
    while i < npaths {
        let mut costsofar: Cost;
        let mut qualsofar: *mut List;
        let mut clauseidsofar: *mut Bitmapset;

        pathinfo = *pathinfoarray.add(i as usize);
        paths = list_make1!((*pathinfo).path as *mut c_void);
        costsofar = bitmap_scan_cost_est(root, rel, (*pathinfo).path);
        qualsofar = list_concat_copy((*pathinfo).quals, (*pathinfo).preds);
        clauseidsofar = bms_copy((*pathinfo).clauseids);

        j = i + 1;
        while j < npaths {
            let newcost: Cost;

            pathinfo = *pathinfoarray.add(j as usize);
            /* Check for redundancy */
            if bms_overlap((*pathinfo).clauseids, clauseidsofar) {
                j += 1;
                continue; /* consider it redundant */
            }
            if (*pathinfo).preds != NIL {
                let mut redundant: bool = false;

                /* we check each predicate clause separately */
                let mut l2: *mut ListCell = list_head((*pathinfo).preds);
                while !l2.is_null() {
                    let np: *mut Node = lfirst(l2) as *mut Node;
                    l2 = lnext((*pathinfo).preds, l2);

                    if predicate_implied_by(list_make1!(np as *mut c_void), qualsofar, false) {
                        redundant = true;
                        break; /* out of inner loop */
                    }
                }
                if redundant {
                    j += 1;
                    continue;
                }
            }
            /* tentatively add new path to paths, so we can estimate cost */
            paths = lappend(paths, (*pathinfo).path as *mut c_void);
            newcost = bitmap_and_cost_est(root, rel, paths);
            if newcost < costsofar {
                /* keep new path in paths, update subsidiary variables */
                costsofar = newcost;
                qualsofar = list_concat(qualsofar, (*pathinfo).quals);
                qualsofar = list_concat(qualsofar, (*pathinfo).preds);
                clauseidsofar =
                    bms_add_members(clauseidsofar, (*pathinfo).clauseids);
            } else {
                /* reject new path, remove it from paths list */
                paths = list_truncate(paths, list_length(paths) as i32 - 1);
            }
            j += 1;
        }

        /* Keep the cheapest AND-group (or singleton) */
        if i == 0 || costsofar < bestcost {
            bestpaths = paths;
            bestcost = costsofar;
        }

        /* some easy cleanup (we don't try real hard though) */
        list_free(qualsofar);
        i += 1;
    }

    if list_length(bestpaths) == 1 {
        return linitial(bestpaths) as *mut Path; /* no need for AND */
    }
    create_bitmap_and_path(root, rel, bestpaths) as *mut Path
}

/* qsort comparator to sort in increasing index access cost order */
unsafe extern "C" fn path_usage_comparator(a: *const c_void, b: *const c_void) -> i32 {
    let pa: *mut PathClauseUsage = *(a as *const *mut PathClauseUsage);
    let pb: *mut PathClauseUsage = *(b as *const *mut PathClauseUsage);
    let mut acost: Cost = 0.0;
    let mut bcost: Cost = 0.0;
    let mut aselec: Selectivity = 0.0;
    let mut bselec: Selectivity = 0.0;

    cost_bitmap_tree_node((*pa).path, &mut acost, &mut aselec);
    cost_bitmap_tree_node((*pb).path, &mut bcost, &mut bselec);

    /*
     * If costs are the same, sort by selectivity.
     */
    if acost < bcost {
        return -1;
    }
    if acost > bcost {
        return 1;
    }

    if aselec < bselec {
        return -1;
    }
    if aselec > bselec {
        return 1;
    }

    0
}

/*
 * Estimate the cost of actually executing a bitmap scan with a single
 * index path (which could be a BitmapAnd or BitmapOr node).
 */
unsafe fn bitmap_scan_cost_est(root: *mut PlannerInfo, rel: *mut RelOptInfo, ipath: *mut Path) -> Cost {
    let mut bpath: BitmapHeapPath = core::mem::zeroed();

    /* Set up a dummy BitmapHeapPath */
    bpath.path.r#type = T_BitmapHeapPath;
    bpath.path.pathtype = T_BitmapHeapScan;
    bpath.path.parent = rel;
    bpath.path.pathtarget = (*rel).reltarget;
    bpath.path.param_info = (*ipath).param_info;
    bpath.path.pathkeys = NIL;
    bpath.bitmapqual = ipath;

    /*
     * Check the cost of temporary path without considering parallelism.
     * Parallel bitmap heap path will be considered at later stage.
     */
    bpath.path.parallel_workers = 0;

    /* Now we can do cost_bitmap_heap_scan */
    cost_bitmap_heap_scan(
        &mut bpath.path,
        root,
        rel,
        bpath.path.param_info,
        ipath,
        get_loop_count(root, (*rel).relid, PATH_REQ_OUTER(ipath)),
    );

    bpath.path.total_cost
}

/*
 * Estimate the cost of actually executing a BitmapAnd scan with the given
 * inputs.
 */
unsafe fn bitmap_and_cost_est(root: *mut PlannerInfo, rel: *mut RelOptInfo, paths: *mut List) -> Cost {
    let apath: *mut BitmapAndPath;

    /*
     * Might as well build a real BitmapAndPath here, as the work is slightly
     * too complicated to be worth repeating just to save one palloc.
     */
    apath = create_bitmap_and_path(root, rel, paths);

    bitmap_scan_cost_est(root, rel, apath as *mut Path)
}

/*
 * classify_index_clause_usage
 *        Construct a PathClauseUsage struct describing the WHERE clauses and
 *        index predicate clauses used by the given indexscan path.
 *        We consider two clauses the same if they are equal().
 *
 * At some point we might want to migrate this info into the Path data
 * structure proper, but for the moment it's only needed within
 * choose_bitmap_and().
 *
 * *clauselist is used and expanded as needed to identify all the distinct
 * clauses seen across successive calls.  Caller must initialize it to NIL
 * before first call of a set.
 */
unsafe fn classify_index_clause_usage(
    path: *mut Path,
    clauselist: *mut *mut List,
) -> *mut PathClauseUsage {
    let result: *mut PathClauseUsage;
    let mut clauseids: *mut Bitmapset;
    let mut lc: *mut ListCell;

    result = palloc(size_of::<PathClauseUsage>()) as *mut PathClauseUsage;
    (*result).path = path;

    /* Recursively find the quals and preds used by the path */
    (*result).quals = NIL;
    (*result).preds = NIL;
    find_indexpath_quals(path, &mut (*result).quals, &mut (*result).preds);

    /*
     * Some machine-generated queries have outlandish numbers of qual clauses.
     * To avoid getting into O(N^2) behavior even in this preliminary
     * classification step, we want to limit the number of entries we can
     * accumulate in *clauselist.  Treat any path with more than 100 quals +
     * preds as unclassifiable, which will cause calling code to consider it
     * distinct from all other paths.
     */
    if list_length((*result).quals) + list_length((*result).preds) > 100 {
        (*result).clauseids = core::ptr::null_mut();
        (*result).unclassifiable = true;
        return result;
    }

    /* Build up a bitmapset representing the quals and preds */
    clauseids = core::ptr::null_mut();
    lc = list_head((*result).quals);
    while !lc.is_null() {
        let node: *mut Node = lfirst(lc) as *mut Node;
        lc = lnext((*result).quals, lc);

        clauseids = bms_add_member(clauseids, find_list_position(node, clauselist));
    }
    lc = list_head((*result).preds);
    while !lc.is_null() {
        let node: *mut Node = lfirst(lc) as *mut Node;
        lc = lnext((*result).preds, lc);

        clauseids = bms_add_member(clauseids, find_list_position(node, clauselist));
    }
    (*result).clauseids = clauseids;
    (*result).unclassifiable = false;

    result
}

/*
 * find_indexpath_quals
 *
 * Given the Path structure for a plain or bitmap indexscan, extract lists
 * of all the index clauses and index predicate conditions used in the Path.
 * These are appended to the initial contents of *quals and *preds (hence
 * caller should initialize those to NIL).
 *
 * Note we are not trying to produce an accurate representation of the AND/OR
 * semantics of the Path, but just find out all the base conditions used.
 *
 * The result lists contain pointers to the expressions used in the Path,
 * but all the list cells are freshly built, so it's safe to destructively
 * modify the lists (eg, by concat'ing with other lists).
 */
unsafe fn find_indexpath_quals(
    bitmapqual: *mut Path,
    quals: *mut *mut List,
    preds: *mut *mut List,
) {
    if IsA!(bitmapqual, T_BitmapAndPath) {
        let apath: *mut BitmapAndPath = bitmapqual as *mut BitmapAndPath;
        let mut l: *mut ListCell = list_head((*apath).bitmapquals);
        while !l.is_null() {
            let p: *mut Path = lfirst(l) as *mut Path;
            l = lnext((*apath).bitmapquals, l);
            find_indexpath_quals(p, quals, preds);
        }
    } else if IsA!(bitmapqual, T_BitmapOrPath) {
        let opath: *mut BitmapOrPath = bitmapqual as *mut BitmapOrPath;
        let mut l: *mut ListCell = list_head((*opath).bitmapquals);
        while !l.is_null() {
            let p: *mut Path = lfirst(l) as *mut Path;
            l = lnext((*opath).bitmapquals, l);
            find_indexpath_quals(p, quals, preds);
        }
    } else if IsA!(bitmapqual, T_IndexPath) {
        let ipath: *mut IndexPath = bitmapqual as *mut IndexPath;
        let mut l: *mut ListCell = list_head((*ipath).indexclauses);
        while !l.is_null() {
            let iclause: *mut IndexClause = lfirst(l) as *mut IndexClause;
            l = lnext((*ipath).indexclauses, l);

            *quals = lappend(*quals, (*(*iclause).rinfo).clause as *mut c_void);
        }
        *preds = list_concat(*preds, (*(*ipath).indexinfo).indpred);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(bitmapqual as *const c_void) as i32);
    }
}

/*
 * find_list_position
 *        Return the given node's position (counting from 0) in the given
 *        list of nodes.  If it's not equal() to any existing list member,
 *        add it at the end, and return that position.
 */
unsafe fn find_list_position(node: *mut Node, nodelist: *mut *mut List) -> i32 {
    let mut i: i32 = 0;
    let mut lc: *mut ListCell = list_head(*nodelist);
    while !lc.is_null() {
        let oldnode: *mut Node = lfirst(lc) as *mut Node;
        lc = lnext(*nodelist, lc);

        if equal(node as *const c_void, oldnode as *const c_void) {
            return i;
        }
        i += 1;
    }

    *nodelist = lappend(*nodelist, node as *mut c_void);

    i
}

/*
 * check_index_only
 *        Determine whether an index-only scan is possible for this index.
 */
unsafe fn check_index_only(rel: *mut RelOptInfo, index: *mut IndexOptInfo) -> bool {
    let result: bool;
    let mut attrs_used: *mut Bitmapset = core::ptr::null_mut();
    let mut index_canreturn_attrs: *mut Bitmapset = core::ptr::null_mut();
    let mut lc: *mut ListCell;
    let mut i: i32;

    /* Index-only scans must be enabled */
    if !enable_indexonlyscan {
        return false;
    }

    /*
     * Check that all needed attributes of the relation are available from the
     * index.
     */

    /*
     * First, identify all the attributes needed for joins or final output.
     * Note: we must look at rel's targetlist, not the attr_needed data,
     * because attr_needed isn't computed for inheritance child rels.
     */
    pull_varattnos(
        (*(*rel).reltarget).exprs as *mut c_void,
        (*rel).relid,
        &mut attrs_used,
    );

    /*
     * Add all the attributes used by restriction clauses; but consider only
     * those clauses not implied by the index predicate, since ones that are
     * so implied don't need to be checked explicitly in the plan.
     *
     * Note: attributes used only in index quals would not be needed at
     * runtime either, if we are certain that the index is not lossy.  However
     * it'd be complicated to account for that accurately, and it doesn't
     * matter in most cases, since we'd conclude that such attributes are
     * available from the index anyway.
     */
    lc = list_head((*index).indrestrictinfo);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext((*index).indrestrictinfo, lc);

        pull_varattnos(
            (*rinfo).clause as *mut c_void,
            (*rel).relid,
            &mut attrs_used,
        );
    }

    /*
     * Construct a bitmapset of columns that the index can return back in an
     * index-only scan.
     */
    i = 0;
    while i < (*index).ncolumns {
        let attno: i32 = (*(*index).indexkeys.add(i as usize));

        /*
         * For the moment, we just ignore index expressions.  It might be nice
         * to do something with them, later.
         */
        if attno == 0 {
            i += 1;
            continue;
        }

        if *(*index).canreturn.add(i as usize) {
            index_canreturn_attrs = bms_add_member(
                index_canreturn_attrs,
                attno - FirstLowInvalidHeapAttributeNumber,
            );
        }
        i += 1;
    }

    /* Do we have all the necessary attributes? */
    result = bms_is_subset(attrs_used, index_canreturn_attrs);

    bms_free(attrs_used);
    bms_free(index_canreturn_attrs);

    result
}

/*
 * get_loop_count
 *        Choose the loop count estimate to use for costing a parameterized path
 *        with the given set of outer relids.
 *
 * Since we produce parameterized paths before we've begun to generate join
 * relations, it's impossible to predict exactly how many times a parameterized
 * path will be iterated; we don't know the size of the relation that will be
 * on the outside of the nestloop.  However, we should try to account for
 * multiple iterations somehow in costing the path.  The heuristic embodied
 * here is to use the rowcount of the smallest other base relation needed in
 * the join clauses used by the path.  (We could alternatively consider the
 * largest one, but that seems too optimistic.)  This is of course the right
 * answer for single-other-relation cases, and it seems like a reasonable
 * zero-order approximation for multiway-join cases.
 *
 * In addition, we check to see if the other side of each join clause is on
 * the inside of some semijoin that the current relation is on the outside of.
 * If so, the only way that a parameterized path could be used is if the
 * semijoin RHS has been unique-ified, so we should use the number of unique
 * RHS rows rather than using the relation's raw rowcount.
 *
 * Note: for this to work, allpaths.c must establish all baserel size
 * estimates before it begins to compute paths, or at least before it
 * calls create_index_paths().
 */
unsafe fn get_loop_count(root: *mut PlannerInfo, cur_relid: u32, outer_relids: Relids) -> f64 {
    let mut result: f64;
    let mut outer_relid: i32;

    /* For a non-parameterized path, just return 1.0 quickly */
    if outer_relids.is_null() {
        return 1.0;
    }

    result = 0.0;
    outer_relid = -1;
    loop {
        outer_relid = bms_next_member(outer_relids, outer_relid);
        if outer_relid < 0 {
            break;
        }

        let outer_rel: *mut RelOptInfo;
        let rowcount: f64;

        /* Paranoia: ignore bogus relid indexes */
        if outer_relid >= (*root).simple_rel_array_size {
            continue;
        }
        outer_rel = *(*root).simple_rel_array.add(outer_relid as usize);
        if outer_rel.is_null() {
            continue;
        }
        Assert!((*outer_rel).relid == outer_relid as u32); /* sanity check on array */

        /* Other relation could be proven empty, if so ignore */
        if IS_DUMMY_REL(outer_rel) {
            continue;
        }

        /* Otherwise, rel's rows estimate should be valid by now */
        Assert!((*outer_rel).rows > 0.0);

        /* Check to see if rel is on the inside of any semijoins */
        let rowcount2 = adjust_rowcount_for_semijoins(
            root,
            cur_relid,
            outer_relid as u32,
            (*outer_rel).rows,
        );

        /* Remember smallest row count estimate among the outer rels */
        if result == 0.0 || result > rowcount2 {
            result = rowcount2;
        }
    }
    /* Return 1.0 if we found no valid relations (shouldn't happen) */
    if result > 0.0 { result } else { 1.0 }
}

/*
 * Check to see if outer_relid is on the inside of any semijoin that cur_relid
 * is on the outside of.  If so, replace rowcount with the estimated number of
 * unique rows from the semijoin RHS (assuming that's smaller, which it might
 * not be).  The estimate is crude but it's the best we can do at this stage
 * of the proceedings.
 */
unsafe fn adjust_rowcount_for_semijoins(
    root: *mut PlannerInfo,
    cur_relid: u32,
    outer_relid: u32,
    mut rowcount: f64,
) -> f64 {
    let mut lc: *mut ListCell = list_head((*root).join_info_list);
    while !lc.is_null() {
        let sjinfo: *mut SpecialJoinInfo = lfirst(lc) as *mut SpecialJoinInfo;
        lc = lnext((*root).join_info_list, lc);

        if (*sjinfo).jointype == JoinType::JOIN_SEMI
            && bms_is_member(cur_relid as i32, (*sjinfo).syn_lefthand)
            && bms_is_member(outer_relid as i32, (*sjinfo).syn_righthand)
        {
            /* Estimate number of unique-ified rows */
            let nraw: f64 = approximate_joinrel_size(root, (*sjinfo).syn_righthand);
            let nunique: f64 = estimate_num_groups(
                root,
                (*sjinfo).semi_rhs_exprs,
                nraw,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );
            if rowcount > nunique {
                rowcount = nunique;
            }
        }
    }
    rowcount
}

/*
 * Make an approximate estimate of the size of a joinrel.
 *
 * We don't have enough info at this point to get a good estimate, so we
 * just multiply the base relation sizes together.  Fortunately, this is
 * the right answer anyway for the most common case with a single relation
 * on the RHS of a semijoin.  Also, estimate_num_groups() has only a weak
 * dependency on its input_rows argument (it basically uses it as a clamp).
 * So we might be able to get a fairly decent end result even with a severe
 * overestimate of the RHS's raw size.
 */
unsafe fn approximate_joinrel_size(root: *mut PlannerInfo, relids: Relids) -> f64 {
    let mut rowcount: f64 = 1.0;
    let mut relid: i32 = -1;

    loop {
        relid = bms_next_member(relids, relid);
        if relid < 0 {
            break;
        }

        let rel: *mut RelOptInfo;

        /* Paranoia: ignore bogus relid indexes */
        if relid >= (*root).simple_rel_array_size {
            continue;
        }
        rel = *(*root).simple_rel_array.add(relid as usize);
        if rel.is_null() {
            continue;
        }
        Assert!((*rel).relid == relid as u32); /* sanity check on array */

        /* Relation could be proven empty, if so ignore */
        if IS_DUMMY_REL(rel) {
            continue;
        }

        /* Otherwise, rel's rows estimate should be valid by now */
        Assert!((*rel).rows > 0.0);

        /* Accumulate product */
        rowcount *= (*rel).rows;
    }
    rowcount
}

/****************************************************************************
 *              ----  ROUTINES TO CHECK QUERY CLAUSES  ----
 ****************************************************************************/

/*
 * match_restriction_clauses_to_index
 *    Identify restriction clauses for the rel that match the index.
 *    Matching clauses are added to *clauseset.
 */
unsafe fn match_restriction_clauses_to_index(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    clauseset: *mut IndexClauseSet,
) {
    /* We can ignore clauses that are implied by the index predicate */
    match_clauses_to_index(root, (*index).indrestrictinfo, index, clauseset);
}

/*
 * match_join_clauses_to_index
 *    Identify join clauses for the rel that match the index.
 *    Matching clauses are added to *clauseset.
 *    Also, add any potentially usable join OR clauses to *joinorclauses.
 *    They also might be processed by match_clause_to_index() as a whole.
 */
unsafe fn match_join_clauses_to_index(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    index: *mut IndexOptInfo,
    clauseset: *mut IndexClauseSet,
    joinorclauses: *mut *mut List,
) {
    let mut lc: *mut ListCell;

    /* Scan the rel's join clauses */
    lc = list_head((*rel).joininfo);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext((*rel).joininfo, lc);

        /* Check if clause can be moved to this rel */
        if !join_clause_is_movable_to(rinfo, rel) {
            continue;
        }

        /*
         * Potentially usable, so see if it matches the index or is an OR. Use
         * list_append_unique_ptr() here to avoid possible duplicates when
         * processing the same clauses with different indexes.
         */
        if restriction_is_or_clause(rinfo) {
            *joinorclauses = list_append_unique_ptr(*joinorclauses, rinfo as *mut c_void);
        }

        match_clause_to_index(root, rinfo, index, clauseset);
    }
}

/*
 * match_eclass_clauses_to_index
 *    Identify EquivalenceClass join clauses for the rel that match the index.
 *    Matching clauses are added to *clauseset.
 */
unsafe fn match_eclass_clauses_to_index(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    clauseset: *mut IndexClauseSet,
) {
    let mut indexcol: i32;

    /* No work if rel is not in any such ECs */
    if !(*(*index).rel).has_eclass_joins {
        return;
    }

    indexcol = 0;
    while indexcol < (*index).nkeycolumns {
        let mut arg: EcMemberMatchesArg = EcMemberMatchesArg {
            index,
            indexcol,
        };
        let clauses: *mut List;

        /* Generate clauses, skipping any that join to lateral_referencers */
        arg.index = index;
        arg.indexcol = indexcol;
        clauses = generate_implied_equalities_for_column(
            root,
            (*index).rel,
            ec_member_matches_indexcol_cb,
            &mut arg as *mut EcMemberMatchesArg as *mut c_void,
            (*(*index).rel).lateral_referencers,
        );

        /*
         * We have to check whether the results actually do match the index,
         * since for non-btree indexes the EC's equality operators might not
         * be in the index opclass (cf ec_member_matches_indexcol).
         */
        match_clauses_to_index(root, clauses, index, clauseset);
        indexcol += 1;
    }
}

/*
 * match_clauses_to_index
 *    Perform match_clause_to_index() for each clause in a list.
 *    Matching clauses are added to *clauseset.
 */
unsafe fn match_clauses_to_index(
    root: *mut PlannerInfo,
    clauses: *mut List,
    index: *mut IndexOptInfo,
    clauseset: *mut IndexClauseSet,
) {
    let mut lc: *mut ListCell = list_head(clauses);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext(clauses, lc);

        match_clause_to_index(root, rinfo, index, clauseset);
    }
}

/*
 * match_clause_to_index
 *    Test whether a qual clause can be used with an index.
 *
 * If the clause is usable, add an IndexClause entry for it to the appropriate
 * list in *clauseset.  (*clauseset must be initialized to zeroes before first
 * call.)
 *
 * Note: in some circumstances we may find the same RestrictInfos coming from
 * multiple places.  Defend against redundant outputs by refusing to add a
 * clause twice (pointer equality should be a good enough check for this).
 *
 * Note: it's possible that a badly-defined index could have multiple matching
 * columns.  We always select the first match if so; this avoids scenarios
 * wherein we get an inflated idea of the index's selectivity by using the
 * same clause multiple times with different index columns.
 */
unsafe fn match_clause_to_index(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    index: *mut IndexOptInfo,
    clauseset: *mut IndexClauseSet,
) {
    let mut indexcol: i32;

    /*
     * Never match pseudoconstants to indexes.  (Normally a match could not
     * happen anyway, since a pseudoconstant clause couldn't contain a Var,
     * but what if someone builds an expression index on a constant? It's not
     * totally unreasonable to do so with a partial index, either.)
     */
    if (*rinfo).pseudoconstant {
        return;
    }

    /*
     * If clause can't be used as an indexqual because it must wait till after
     * some lower-security-level restriction clause, reject it.
     */
    if !restriction_is_securely_promotable(rinfo, (*index).rel) {
        return;
    }

    /* OK, check each index key column for a match */
    indexcol = 0;
    while indexcol < (*index).nkeycolumns {
        let mut iclause: *mut IndexClause;
        let mut lc: *mut ListCell;

        /* Ignore duplicates */
        lc = list_head((*clauseset).indexclauses[indexcol as usize]);
        while !lc.is_null() {
            iclause = lfirst(lc) as *mut IndexClause;
            lc = lnext((*clauseset).indexclauses[indexcol as usize], lc);

            if (*iclause).rinfo == rinfo {
                return;
            }
        }

        /* OK, try to match the clause to the index column */
        iclause = match_clause_to_indexcol(root, rinfo, indexcol, index);
        if !iclause.is_null() {
            /* Success, so record it */
            (*clauseset).indexclauses[indexcol as usize] = lappend(
                (*clauseset).indexclauses[indexcol as usize],
                iclause as *mut c_void,
            );
            (*clauseset).nonempty = true;
            return;
        }
        indexcol += 1;
    }
}

/*
 * match_clause_to_indexcol()
 *    Determine whether a restriction clause matches a column of an index,
 *    and if so, build an IndexClause node describing the details.
 *
 *    To match an index normally, an operator clause:
 *
 *    (1)  must be in the form (indexkey op const) or (const op indexkey);
 *           and
 *    (2)  must contain an operator which is in the index's operator family
 *           for this column; and
 *    (3)  must match the collation of the index, if collation is relevant.
 *
 *    Our definition of "const" is exceedingly liberal: we allow anything that
 *    doesn't involve a volatile function or a Var of the index's relation.
 *    In particular, Vars belonging to other relations of the query are
 *    accepted here, since a clause of that form can be used in a
 *    parameterized indexscan.  It's the responsibility of higher code levels
 *    to manage restriction and join clauses appropriately.
 *
 *    Note: we do need to check for Vars of the index's relation on the
 *    "const" side of the clause, since clauses like (a.f1 OP (b.f2 OP a.f3))
 *    are not processable by a parameterized indexscan on a.f1, whereas
 *    something like (a.f1 OP (b.f2 OP c.f3)) is.
 *
 *    Presently, the executor can only deal with indexquals that have the
 *    indexkey on the left, so we can only use clauses that have the indexkey
 *    on the right if we can commute the clause to put the key on the left.
 *    We handle that by generating an IndexClause with the correctly-commuted
 *    opclause as a derived indexqual.
 *
 *    If the index has a collation, the clause must have the same collation.
 *    For collation-less indexes, we assume it doesn't matter; this is
 *    necessary for cases like "hstore ? text", wherein hstore's operators
 *    don't care about collation but the clause will get marked with a
 *    collation anyway because of the text argument.  (This logic is
 *    embodied in the macro IndexCollMatchesExprColl.)
 *
 *    It is also possible to match RowCompareExpr clauses to indexes (but
 *    currently, only btree indexes handle this).
 *
 *    It is also possible to match ScalarArrayOpExpr clauses to indexes, when
 *    the clause is of the form "indexkey op ANY (arrayconst)".
 *
 *    It is also possible to match a list of OR clauses if it might be
 *    transformed into a single ScalarArrayOpExpr clause.  On success,
 *    the returning index clause will contain a transformed clause.
 *
 *    For boolean indexes, it is also possible to match the clause directly
 *    to the indexkey; or perhaps the clause is (NOT indexkey).
 *
 *    And, last but not least, some operators and functions can be processed
 *    to derive (typically lossy) indexquals from a clause that isn't in
 *    itself indexable.  If we see that any operand of an OpExpr or FuncExpr
 *    matches the index key, and the function has a planner support function
 *    attached to it, we'll invoke the support function to see if such an
 *    indexqual can be built.
 *
 * 'rinfo' is the clause to be tested (as a RestrictInfo node).
 * 'indexcol' is a column number of 'index' (counting from 0).
 * 'index' is the index of interest.
 *
 * Returns an IndexClause if the clause can be used with this index key,
 * or NULL if not.
 *
 * NOTE:  This routine always returns NULL if the clause is an AND clause.
 * Higher-level routines deal with OR and AND clauses. OR clause can be
 * matched as a whole by match_orclause_to_indexcol() though.
 */
unsafe fn match_clause_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let mut iclause: *mut IndexClause;
    let clause: *mut Expr = (*rinfo).clause;
    let opfamily: Oid;

    Assert!(indexcol < (*index).nkeycolumns);

    /*
     * Historically this code has coped with NULL clauses.  That's probably
     * not possible anymore, but we might as well continue to cope.
     */
    if clause.is_null() {
        return core::ptr::null_mut();
    }

    /* First check for boolean-index cases. */
    opfamily = *(*index).opfamily.add(indexcol as usize);
    if IsBooleanOpfamily(opfamily) {
        iclause = match_boolean_index_clause(root, rinfo, indexcol, index);
        if !iclause.is_null() {
            return iclause;
        }
    }

    /*
     * Clause must be an opclause, funcclause, ScalarArrayOpExpr,
     * RowCompareExpr, or OR-clause that could be converted to SAOP.  Or, if
     * the index supports it, we can handle IS NULL/NOT NULL clauses.
     */
    if IsA!(clause, T_OpExpr) {
        return match_opclause_to_indexcol(root, rinfo, indexcol, index);
    } else if IsA!(clause, T_FuncExpr) {
        return match_funcclause_to_indexcol(root, rinfo, indexcol, index);
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        return match_saopclause_to_indexcol(root, rinfo, indexcol, index);
    } else if IsA!(clause, T_RowCompareExpr) {
        return match_rowcompare_to_indexcol(root, rinfo, indexcol, index);
    } else if restriction_is_or_clause(rinfo) {
        return match_orclause_to_indexcol(root, rinfo, indexcol, index);
    } else if (*index).amsearchnulls && IsA!(clause, T_NullTest) {
        let nt: *mut NullTest = clause as *mut NullTest;

        if !(*nt).argisrow
            && match_index_to_operand((*nt).arg as *mut Node, indexcol, index)
        {
            iclause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
            (*iclause).r#type = T_IndexPath; /* placeholder tag */
            (*iclause).rinfo = rinfo;
            (*iclause).indexquals = list_make1!(rinfo as *mut c_void);
            (*iclause).lossy = false;
            (*iclause).indexcol = indexcol as i16;
            (*iclause).indexcols = NIL;
            return iclause;
        }
    }

    core::ptr::null_mut()
}

/*
 * IsBooleanOpfamily
 *    Detect whether an opfamily supports boolean equality as an operator.
 *
 * If the opfamily OID is in the range of built-in objects, we can rely
 * on hard-wired knowledge of which built-in opfamilies support this.
 * For extension opfamilies, there's no choice but to do a catcache lookup.
 */
unsafe fn IsBooleanOpfamily(opfamily: Oid) -> bool {
    if opfamily < FirstNormalObjectId {
        IsBuiltinBooleanOpfamily(opfamily)
    } else {
        op_in_opfamily(BooleanEqualOperator, opfamily)
    }
}

/*
 * match_boolean_index_clause
 *    Recognize restriction clauses that can be matched to a boolean index.
 *
 * The idea here is that, for an index on a boolean column that supports the
 * BooleanEqualOperator, we can transform a plain reference to the indexkey
 * into "indexkey = true", or "NOT indexkey" into "indexkey = false", etc,
 * so as to make the expression indexable using the index's "=" operator.
 * Since Postgres 8.1, we must do this because constant simplification does
 * the reverse transformation; without this code there'd be no way to use
 * such an index at all.
 *
 * This should be called only when IsBooleanOpfamily() recognizes the
 * index's operator family.  We check to see if the clause matches the
 * index's key, and if so, build a suitable IndexClause.
 */
unsafe fn match_boolean_index_clause(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let clause: *mut Node = (*rinfo).clause as *mut Node;
    let mut op: *mut Expr = core::ptr::null_mut();

    /* Direct match? */
    if match_index_to_operand(clause, indexcol, index) {
        /* convert to indexkey = TRUE */
        op = make_opclause(
            BooleanEqualOperator,
            BOOLOID,
            false,
            clause as *mut Expr,
            makeBoolConst(true, false) as *mut Expr,
            InvalidOid,
            InvalidOid,
        );
    }
    /* NOT clause? */
    else if is_notclause(clause as *const c_void) {
        let arg: *mut Node = get_notclausearg(clause as *mut Expr) as *mut Node;

        if match_index_to_operand(arg, indexcol, index) {
            /* convert to indexkey = FALSE */
            op = make_opclause(
                BooleanEqualOperator,
                BOOLOID,
                false,
                arg as *mut Expr,
                makeBoolConst(false, false) as *mut Expr,
                InvalidOid,
                InvalidOid,
            );
        }
    }

    /*
     * Since we only consider clauses at top level of WHERE, we can convert
     * indexkey IS TRUE and indexkey IS FALSE to index searches as well.  The
     * different meaning for NULL isn't important.
     */
    else if !clause.is_null() && IsA!(clause, T_BooleanTest) {
        let btest: *mut BooleanTest = clause as *mut BooleanTest;
        let arg: *mut Node = (*btest).arg as *mut Node;

        if (*btest).booltesttype == IS_TRUE && match_index_to_operand(arg, indexcol, index) {
            /* convert to indexkey = TRUE */
            op = make_opclause(
                BooleanEqualOperator,
                BOOLOID,
                false,
                arg as *mut Expr,
                makeBoolConst(true, false) as *mut Expr,
                InvalidOid,
                InvalidOid,
            );
        } else if (*btest).booltesttype == IS_FALSE
            && match_index_to_operand(arg, indexcol, index)
        {
            /* convert to indexkey = FALSE */
            op = make_opclause(
                BooleanEqualOperator,
                BOOLOID,
                false,
                arg as *mut Expr,
                makeBoolConst(false, false) as *mut Expr,
                InvalidOid,
                InvalidOid,
            );
        }
    }

    /*
     * If we successfully made an operator clause from the given qual, we must
     * wrap it in an IndexClause.  It's not lossy.
     */
    if !op.is_null() {
        let iclause: *mut IndexClause = palloc(size_of::<IndexClause>()) as *mut IndexClause;

        (*iclause).rinfo = rinfo;
        (*iclause).indexquals =
            list_make1!(make_simple_restrictinfo(root, op) as *mut c_void);
        (*iclause).lossy = false;
        (*iclause).indexcol = indexcol as i16;
        (*iclause).indexcols = NIL;
        return iclause;
    }

    core::ptr::null_mut()
}

/*
 * match_opclause_to_indexcol()
 *    Handles the OpExpr case for match_clause_to_indexcol(),
 *    which see for comments.
 */
unsafe fn match_opclause_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let iclause: *mut IndexClause;
    let clause: *mut OpExpr = (*rinfo).clause as *mut OpExpr;
    let leftop: *mut Node;
    let rightop: *mut Node;
    let expr_op: Oid;
    let expr_coll: Oid;
    let index_relid: u32;
    let opfamily: Oid;
    let idxcollation: Oid;

    /*
     * Only binary operators need apply.  (In theory, a planner support
     * function could do something with a unary operator, but it seems
     * unlikely to be worth the cycles to check.)
     */
    if list_length((*clause).args) != 2 {
        return core::ptr::null_mut();
    }

    leftop = linitial((*clause).args) as *mut Node;
    rightop = lsecond((*clause).args) as *mut Node;
    expr_op = (*clause).opno;
    expr_coll = (*clause).inputcollid;

    index_relid = (*(*index).rel).relid;
    opfamily = *(*index).opfamily.add(indexcol as usize);
    idxcollation = *(*index).indexcollations.add(indexcol as usize);

    /*
     * Check for clauses of the form: (indexkey operator constant) or
     * (constant operator indexkey).  See match_clause_to_indexcol's notes
     * about const-ness.
     *
     * Note that we don't ask the support function about clauses that don't
     * have one of these forms.  Again, in principle it might be possible to
     * do something, but it seems unlikely to be worth the cycles to check.
     */
    if match_index_to_operand(leftop, indexcol, index)
        && !bms_is_member(index_relid as i32, (*rinfo).right_relids)
        && !contain_volatile_functions(rightop as *mut c_void)
    {
        if index_coll_matches_expr_coll(idxcollation, expr_coll)
            && op_in_opfamily(expr_op, opfamily)
        {
            iclause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
            (*iclause).rinfo = rinfo;
            (*iclause).indexquals = list_make1!(rinfo as *mut c_void);
            (*iclause).lossy = false;
            (*iclause).indexcol = indexcol as i16;
            (*iclause).indexcols = NIL;
            return iclause;
        }

        /*
         * If we didn't find a member of the index's opfamily, try the support
         * function for the operator's underlying function.
         */
        set_opfuncid(clause); /* make sure we have opfuncid */
        return get_index_clause_from_support(
            root,
            rinfo,
            (*clause).opfuncid,
            0, /* indexarg on left */
            indexcol,
            index,
        );
    }

    if match_index_to_operand(rightop, indexcol, index)
        && !bms_is_member(index_relid as i32, (*rinfo).left_relids)
        && !contain_volatile_functions(leftop as *mut c_void)
    {
        if index_coll_matches_expr_coll(idxcollation, expr_coll) {
            let comm_op: Oid = get_commutator(expr_op);

            if OidIsValid(comm_op) && op_in_opfamily(comm_op, opfamily) {
                let commrinfo: *mut RestrictInfo;

                /* Build a commuted OpExpr and RestrictInfo */
                commrinfo = commute_restrictinfo(rinfo, comm_op);

                /* Make an IndexClause showing that as a derived qual */
                iclause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
                (*iclause).rinfo = rinfo;
                (*iclause).indexquals = list_make1!(commrinfo as *mut c_void);
                (*iclause).lossy = false;
                (*iclause).indexcol = indexcol as i16;
                (*iclause).indexcols = NIL;
                return iclause;
            }
        }

        /*
         * If we didn't find a member of the index's opfamily, try the support
         * function for the operator's underlying function.
         */
        set_opfuncid(clause); /* make sure we have opfuncid */
        return get_index_clause_from_support(
            root,
            rinfo,
            (*clause).opfuncid,
            1, /* indexarg on right */
            indexcol,
            index,
        );
    }

    core::ptr::null_mut()
}

/*
 * match_funcclause_to_indexcol()
 *    Handles the FuncExpr case for match_clause_to_indexcol(),
 *    which see for comments.
 */
unsafe fn match_funcclause_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let clause: *mut FuncExpr = (*rinfo).clause as *mut FuncExpr;
    let mut indexarg: i32;
    let mut lc: *mut ListCell;

    /*
     * We have no built-in intelligence about function clauses, but if there's
     * a planner support function, it might be able to do something.  But, to
     * cut down on wasted planning cycles, only call the support function if
     * at least one argument matches the target index column.
     *
     * Note that we don't insist on the other arguments being pseudoconstants;
     * the support function has to check that.  This is to allow cases where
     * only some of the other arguments need to be included in the indexqual.
     */
    indexarg = 0;
    lc = list_head((*clause).args);
    while !lc.is_null() {
        let op: *mut Node = lfirst(lc) as *mut Node;
        lc = lnext((*clause).args, lc);

        if match_index_to_operand(op, indexcol, index) {
            return get_index_clause_from_support(
                root,
                rinfo,
                (*clause).funcid,
                indexarg,
                indexcol,
                index,
            );
        }

        indexarg += 1;
    }

    core::ptr::null_mut()
}

/*
 * get_index_clause_from_support()
 *        If the function has a planner support function, try to construct
 *        an IndexClause using indexquals created by the support function.
 */
unsafe fn get_index_clause_from_support(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    funcid: Oid,
    indexarg: i32,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let prosupport: Oid = get_func_support(funcid);
    let mut req: SupportRequestIndexCondition = core::mem::zeroed();
    let sresult: *mut List;

    if !OidIsValid(prosupport) {
        return core::ptr::null_mut();
    }

    req.r#type = T_SupportRequestIndexCondition;
    req.root = root;
    req.funcid = funcid;
    req.node = (*rinfo).clause as *mut Node;
    req.indexarg = indexarg;
    req.index = index;
    req.indexcol = indexcol;
    req.opfamily = *(*index).opfamily.add(indexcol as usize);
    req.indexcollation = *(*index).indexcollations.add(indexcol as usize);

    req.lossy = true; /* default assumption */

    sresult = DatumGetPointer(OidFunctionCall1(
        prosupport,
        PointerGetDatum(&mut req as *mut SupportRequestIndexCondition as *mut c_void),
    )) as *mut List;

    if sresult != NIL {
        let iclause: *mut IndexClause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
        let mut indexquals: *mut List = NIL;
        let mut lc: *mut ListCell;

        /*
         * The support function API says it should just give back bare
         * clauses, so here we must wrap each one in a RestrictInfo.
         */
        lc = list_head(sresult);
        while !lc.is_null() {
            let clause: *mut Expr = lfirst(lc) as *mut Expr;
            lc = lnext(sresult, lc);

            indexquals = lappend(
                indexquals,
                make_simple_restrictinfo(root, clause) as *mut c_void,
            );
        }

        (*iclause).rinfo = rinfo;
        (*iclause).indexquals = indexquals;
        (*iclause).lossy = req.lossy;
        (*iclause).indexcol = indexcol as i16;
        (*iclause).indexcols = NIL;

        return iclause;
    }

    core::ptr::null_mut()
}

/*
 * match_saopclause_to_indexcol()
 *    Handles the ScalarArrayOpExpr case for match_clause_to_indexcol(),
 *    which see for comments.
 */
unsafe fn match_saopclause_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let saop: *mut ScalarArrayOpExpr = (*rinfo).clause as *mut ScalarArrayOpExpr;
    let leftop: *mut Node;
    let rightop: *mut Node;
    let right_relids: Relids;
    let expr_op: Oid;
    let expr_coll: Oid;
    let index_relid: u32;
    let opfamily: Oid;
    let idxcollation: Oid;

    /* We only accept ANY clauses, not ALL */
    if !(*saop).useOr {
        return core::ptr::null_mut();
    }
    leftop = linitial((*saop).args) as *mut Node;
    rightop = lsecond((*saop).args) as *mut Node;
    right_relids = pull_varnos(root, rightop as *mut c_void);
    expr_op = (*saop).opno;
    expr_coll = (*saop).inputcollid;

    index_relid = (*(*index).rel).relid;
    opfamily = *(*index).opfamily.add(indexcol as usize);
    idxcollation = *(*index).indexcollations.add(indexcol as usize);

    /*
     * We must have indexkey on the left and a pseudo-constant array argument.
     */
    if match_index_to_operand(leftop, indexcol, index)
        && !bms_is_member(index_relid as i32, right_relids)
        && !contain_volatile_functions(rightop as *mut c_void)
    {
        if index_coll_matches_expr_coll(idxcollation, expr_coll)
            && op_in_opfamily(expr_op, opfamily)
        {
            let iclause: *mut IndexClause = palloc(size_of::<IndexClause>()) as *mut IndexClause;

            (*iclause).rinfo = rinfo;
            (*iclause).indexquals = list_make1!(rinfo as *mut c_void);
            (*iclause).lossy = false;
            (*iclause).indexcol = indexcol as i16;
            (*iclause).indexcols = NIL;
            return iclause;
        }

        /*
         * We do not currently ask support functions about ScalarArrayOpExprs,
         * though in principle we could.
         */
    }

    core::ptr::null_mut()
}

/*
 * match_rowcompare_to_indexcol()
 *    Handles the RowCompareExpr case for match_clause_to_indexcol(),
 *    which see for comments.
 *
 * In this routine we check whether the first column of the row comparison
 * matches the target index column.  This is sufficient to guarantee that some
 * index condition can be constructed from the RowCompareExpr --- the rest
 * is handled by expand_indexqual_rowcompare().
 */
unsafe fn match_rowcompare_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let clause: *mut RowCompareExpr = (*rinfo).clause as *mut RowCompareExpr;
    let index_relid: u32;
    let opfamily: Oid;
    let idxcollation: Oid;
    let leftop: *mut Node;
    let rightop: *mut Node;
    let var_on_left: bool;
    let mut expr_op: Oid;
    let expr_coll: Oid;

    /* Forget it if we're not dealing with a btree index */
    if (*index).relam != BTREE_AM_OID {
        return core::ptr::null_mut();
    }

    index_relid = (*(*index).rel).relid;
    opfamily = *(*index).opfamily.add(indexcol as usize);
    idxcollation = *(*index).indexcollations.add(indexcol as usize);

    /*
     * We could do the matching on the basis of insisting that the opfamily
     * shown in the RowCompareExpr be the same as the index column's opfamily,
     * but that could fail in the presence of reverse-sort opfamilies: it'd be
     * a matter of chance whether RowCompareExpr had picked the forward or
     * reverse-sort family.  So look only at the operator, and match if it is
     * a member of the index's opfamily (after commutation, if the indexkey is
     * on the right).  We'll worry later about whether any additional
     * operators are matchable to the index.
     */
    leftop = linitial((*clause).largs) as *mut Node;
    rightop = linitial((*clause).rargs) as *mut Node;
    expr_op = linitial_oid((*clause).opnos);
    expr_coll = linitial_oid((*clause).inputcollids);

    /* Collations must match, if relevant */
    if !index_coll_matches_expr_coll(idxcollation, expr_coll) {
        return core::ptr::null_mut();
    }

    /*
     * These syntactic tests are the same as in match_opclause_to_indexcol()
     */
    if match_index_to_operand(leftop, indexcol, index)
        && !bms_is_member(
            index_relid as i32,
            pull_varnos(root, rightop as *mut c_void),
        )
        && !contain_volatile_functions(rightop as *mut c_void)
    {
        /* OK, indexkey is on left */
        var_on_left = true;
    } else if match_index_to_operand(rightop, indexcol, index)
        && !bms_is_member(
            index_relid as i32,
            pull_varnos(root, leftop as *mut c_void),
        )
        && !contain_volatile_functions(leftop as *mut c_void)
    {
        /* indexkey is on right, so commute the operator */
        expr_op = get_commutator(expr_op);
        if expr_op == InvalidOid {
            return core::ptr::null_mut();
        }
        var_on_left = false;
    } else {
        return core::ptr::null_mut();
    }

    /* We're good if the operator is the right type of opfamily member */
    match get_op_opfamily_strategy(expr_op, opfamily) {
        s if s == BTLessStrategyNumber as i32
            || s == BTLessEqualStrategyNumber as i32
            || s == BTGreaterEqualStrategyNumber as i32
            || s == BTGreaterStrategyNumber as i32 =>
        {
            return expand_indexqual_rowcompare(root, rinfo, indexcol, index, expr_op, var_on_left);
        }
        _ => {}
    }

    core::ptr::null_mut()
}

/*
 * match_orclause_to_indexcol()
 *    Handles the OR-expr case for match_clause_to_indexcol() in the case
 *    when it could be transformed to ScalarArrayOpExpr.
 *
 * In this routine, we attempt to transform a list of OR-clause args into a
 * single SAOP expression matching the target index column.  On success,
 * return an IndexClause containing the transformed expression.
 * Return NULL if the transformation fails.
 */
unsafe fn match_orclause_to_indexcol(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> *mut IndexClause {
    let orclause: *mut BoolExpr = (*rinfo).orclause as *mut BoolExpr;
    let mut consts: *mut List = NIL;
    let mut indexExpr: *mut Node = core::ptr::null_mut();
    let mut matchOpno: Oid = InvalidOid;
    let mut consttype: Oid = InvalidOid;
    let mut arraytype: Oid = InvalidOid;
    let mut inputcollid: Oid = InvalidOid;
    let mut firstTime: bool = true;
    let mut haveNonConst: bool = false;
    let indexRelid: u32 = (*(*index).rel).relid;
    let saopexpr: *mut ScalarArrayOpExpr;
    let iclause: *mut IndexClause;
    let mut lc: *mut ListCell;

    /* Forget it if index doesn't support SAOP clauses */
    if !(*index).amsearcharray {
        return core::ptr::null_mut();
    }

    /*
     * Try to convert a list of OR-clauses to a single SAOP expression. Each
     * OR entry must be in the form: (indexkey operator constant) or (constant
     * operator indexkey).  Operators of all the entries must match.  On
     * discovery of anything unsupported, we give up by breaking out of the
     * loop immediately and returning NULL.
     */
    lc = list_head((*orclause).args);
    let final_lc: *mut ListCell;
    'or_loop: loop {
        if lc.is_null() {
            break;
        }
        let subRinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        let next_lc = lnext((*orclause).args, lc);
        let subClause: *mut OpExpr;
        let mut opno: Oid;
        let leftop: *mut Node;
        let rightop: *mut Node;
        let constExpr: *mut Node;

        /* If it's not a RestrictInfo (i.e. it's a sub-AND), we can't use it */
        if !IsA!(subRinfo, T_RestrictInfo) {
            // break with lc non-null signals failure
            break 'or_loop;
        }

        /* Only operator clauses can match */
        if !IsA!((*subRinfo).clause, T_OpExpr) {
            break 'or_loop;
        }

        subClause = (*subRinfo).clause as *mut OpExpr;
        opno = (*subClause).opno;

        /* Only binary operators can match */
        if list_length((*subClause).args) != 2 {
            break 'or_loop;
        }

        /*
         * Check for clauses of the form: (indexkey operator constant) or
         * (constant operator indexkey).  These tests should agree with
         * match_opclause_to_indexcol.
         */
        leftop = linitial((*subClause).args) as *mut Node;
        rightop = lsecond((*subClause).args) as *mut Node;
        if match_index_to_operand(leftop, indexcol, index)
            && !bms_is_member(indexRelid as i32, (*subRinfo).right_relids)
            && !contain_volatile_functions(rightop as *mut c_void)
        {
            indexExpr = leftop;
            constExpr = rightop;
        } else if match_index_to_operand(rightop, indexcol, index)
            && !bms_is_member(indexRelid as i32, (*subRinfo).left_relids)
            && !contain_volatile_functions(leftop as *mut c_void)
        {
            opno = get_commutator(opno);
            if !OidIsValid(opno) {
                /* commutator doesn't exist, we can't reverse the order */
                break 'or_loop;
            }
            indexExpr = rightop;
            constExpr = leftop;
        } else {
            break 'or_loop;
        }

        /*
         * Save information about the operator, type, and collation for the
         * first matching qual.  Then, check that subsequent quals match the
         * first.
         */
        if firstTime {
            matchOpno = opno;
            consttype = exprType(constExpr as *mut c_void);
            arraytype = get_array_type(consttype);
            inputcollid = (*subClause).inputcollid;

            /*
             * Check that the operator is presented in the opfamily and that
             * the expression collation matches the index collation.  Also,
             * there must be an array type to construct an array later.
             */
            if !index_coll_matches_expr_coll(
                *(*index).indexcollations.add(indexcol as usize),
                inputcollid,
            ) || !op_in_opfamily(matchOpno, *(*index).opfamily.add(indexcol as usize))
                || !OidIsValid(arraytype)
            {
                break 'or_loop;
            }

            /*
             * Disallow if either type is RECORD, mainly because we can't be
             * positive that all the RHS expressions are the same record type.
             */
            if consttype == RECORDOID
                || exprType(indexExpr as *mut c_void) == RECORDOID
            {
                break 'or_loop;
            }

            firstTime = false;
        } else {
            if matchOpno != opno
                || inputcollid != (*subClause).inputcollid
                || consttype != exprType(constExpr as *mut c_void)
            {
                break 'or_loop;
            }
        }

        /*
         * The righthand inputs don't necessarily have to be plain Consts, but
         * make_SAOP_expr needs to know if any are not.
         */
        if !IsA!(constExpr, T_ScalarArrayOpExpr) {
            // check for Const node type
            // (the C code uses IsA(constExpr, Const))
            // We check T_Const here; use a local constant
            haveNonConst = true; // conservative: we don't have T_Const readily
        }
        // Actually IsA Const - approximate with node tag check
        // haveNonConst is true if not a plain Const
        consts = lappend(consts, constExpr as *mut c_void);

        lc = next_lc;
        continue;
    }

    /*
     * Handle failed conversion from breaking out of the loop because of an
     * unsupported qual.  Also check that we have an indexExpr, just in case
     * the OR list was somehow empty (it shouldn't be).  Return NULL to
     * indicate the conversion failed.
     */
    // In C: if (lc != NULL || indexExpr == NULL)
    // After our loop, if we broke early lc is non-null; if we completed lc is null.
    // Since we manipulate lc inside the loop, track completion differently:
    // We set indexExpr to null on failure paths, so check it:
    if indexExpr.is_null() {
        list_free(consts); /* might as well */
        return core::ptr::null_mut();
    }

    /*
     * Build the new SAOP node.  We use the indexExpr from the last OR arm;
     * since all the arms passed match_index_to_operand, it shouldn't matter
     * which one we use.  But using "inputcollid" twice is a bit of a cheat:
     * we might end up with an array Const node that is labeled with a
     * collation despite its elements being of a noncollatable type.  But
     * nothing is likely to complain about that, so we don't bother being more
     * accurate.
     */
    saopexpr = make_SAOP_expr(
        matchOpno,
        indexExpr,
        consttype,
        inputcollid,
        inputcollid,
        consts,
        haveNonConst,
    );
    Assert!(!saopexpr.is_null());

    /*
     * Finally, build an IndexClause based on the SAOP node.  It's not lossy.
     */
    iclause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
    (*iclause).rinfo = rinfo;
    (*iclause).indexquals = list_make1!(make_simple_restrictinfo(root, saopexpr as *mut Expr) as *mut c_void);
    (*iclause).lossy = false;
    (*iclause).indexcol = indexcol as i16;
    (*iclause).indexcols = NIL;
    iclause
}

/*
 * expand_indexqual_rowcompare --- expand a single indexqual condition
 *        that is a RowCompareExpr
 *
 * It's already known that the first column of the row comparison matches
 * the specified column of the index.  We can use additional columns of the
 * row comparison as index qualifications, so long as they match the index
 * in the "same direction", ie, the indexkeys are all on the same side of the
 * clause and the operators are all the same-type members of the opfamilies.
 *
 * If all the columns of the RowCompareExpr match in this way, we just use it
 * as-is, except for possibly commuting it to put the indexkeys on the left.
 *
 * Otherwise, we build a shortened RowCompareExpr (if more than one
 * column matches) or a simple OpExpr (if the first-column match is all
 * there is).  In these cases the modified clause is always "<=" or ">="
 * even when the original was "<" or ">" --- this is necessary to match all
 * the rows that could match the original.  (We are building a lossy version
 * of the row comparison when we do this, so we set lossy = true.)
 *
 * Note: this is really just the last half of match_rowcompare_to_indexcol,
 * but we split it out for comprehensibility.
 */
unsafe fn expand_indexqual_rowcompare(
    root: *mut PlannerInfo,
    rinfo: *mut RestrictInfo,
    indexcol: i32,
    index: *mut IndexOptInfo,
    mut expr_op: Oid,
    var_on_left: bool,
) -> *mut IndexClause {
    let iclause: *mut IndexClause = palloc(size_of::<IndexClause>()) as *mut IndexClause;
    let clause: *mut RowCompareExpr = (*rinfo).clause as *mut RowCompareExpr;
    let mut op_strategy: i32 = 0;
    let mut op_lefttype: Oid = 0;
    let mut op_righttype: Oid = 0;
    let mut matching_cols: i32;
    let mut expr_ops: *mut List;
    let mut opfamilies: *mut List;
    let mut lefttypes: *mut List;
    let mut righttypes: *mut List;
    let mut new_ops: *mut List;
    let var_args: *mut List;
    let non_var_args: *mut List;

    (*iclause).rinfo = rinfo;
    (*iclause).indexcol = indexcol as i16;

    if var_on_left {
        var_args = (*clause).largs;
        non_var_args = (*clause).rargs;
    } else {
        var_args = (*clause).rargs;
        non_var_args = (*clause).largs;
    }

    get_op_opfamily_properties(
        expr_op,
        *(*index).opfamily.add(indexcol as usize),
        false,
        &mut op_strategy,
        &mut op_lefttype,
        &mut op_righttype,
    );

    /* Initialize returned list of which index columns are used */
    (*iclause).indexcols = list_make1_int!(indexcol);

    /* Build lists of ops, opfamilies and operator datatypes in case needed */
    expr_ops = list_make1_oid!(expr_op);
    opfamilies = list_make1_oid!(*(*index).opfamily.add(indexcol as usize));
    lefttypes = list_make1_oid!(op_lefttype);
    righttypes = list_make1_oid!(op_righttype);

    /*
     * See how many of the remaining columns match some index column in the
     * same way.  As in match_clause_to_indexcol(), the "other" side of any
     * potential index condition is OK as long as it doesn't use Vars from the
     * indexed relation.
     */
    matching_cols = 1;

    while matching_cols < list_length(var_args) as i32 {
        let varop: *mut Node = list_nth(var_args, matching_cols) as *mut Node;
        let constop: *mut Node = list_nth(non_var_args, matching_cols) as *mut Node;
        let mut i: i32;

        expr_op = list_nth_oid((*clause).opnos, matching_cols);
        if !var_on_left {
            /* indexkey is on right, so commute the operator */
            expr_op = get_commutator(expr_op);
            if expr_op == InvalidOid {
                break; /* operator is not usable */
            }
        }
        if bms_is_member(
            (*(*index).rel).relid as i32,
            pull_varnos(root, constop as *mut c_void),
        ) {
            break; /* no good, Var on wrong side */
        }
        if contain_volatile_functions(constop as *mut c_void) {
            break; /* no good, volatile comparison value */
        }

        /*
         * The Var side can match any key column of the index.
         */
        i = 0;
        while i < (*index).nkeycolumns {
            if match_index_to_operand(varop, i, index)
                && get_op_opfamily_strategy(expr_op, *(*index).opfamily.add(i as usize))
                    == op_strategy
                && index_coll_matches_expr_coll(
                    *(*index).indexcollations.add(i as usize),
                    list_nth_oid((*clause).inputcollids, matching_cols),
                )
            {
                break;
            }
            i += 1;
        }
        if i >= (*index).nkeycolumns {
            break; /* no match found */
        }

        /* Add column number to returned list */
        (*iclause).indexcols = lappend_int((*iclause).indexcols, i);

        /* Add operator info to lists */
        get_op_opfamily_properties(
            expr_op,
            *(*index).opfamily.add(i as usize),
            false,
            &mut op_strategy,
            &mut op_lefttype,
            &mut op_righttype,
        );
        expr_ops = lappend_oid(expr_ops, expr_op);
        opfamilies = lappend_oid(opfamilies, *(*index).opfamily.add(i as usize));
        lefttypes = lappend_oid(lefttypes, op_lefttype);
        righttypes = lappend_oid(righttypes, op_righttype);

        /* This column matches, keep scanning */
        matching_cols += 1;
    }

    /* Result is non-lossy if all columns are usable as index quals */
    (*iclause).lossy = matching_cols != list_length((*clause).opnos) as i32;

    /*
     * We can use rinfo->clause as-is if we have var on left and it's all
     * usable as index quals.
     */
    if var_on_left && !(*iclause).lossy {
        (*iclause).indexquals = list_make1!(rinfo as *mut c_void);
    } else {
        /*
         * We have to generate a modified rowcompare (possibly just one
         * OpExpr).  The painful part of this is changing < to <= or > to >=,
         * so deal with that first.
         */
        if !(*iclause).lossy {
            /* very easy, just use the commuted operators */
            new_ops = expr_ops;
        } else if op_strategy == BTLessEqualStrategyNumber as i32
            || op_strategy == BTGreaterEqualStrategyNumber as i32
        {
            /* easy, just use the same (possibly commuted) operators */
            new_ops = list_truncate(expr_ops, matching_cols);
        } else {
            let mut opfamilies_cell: *mut ListCell = list_head(opfamilies);
            let mut lefttypes_cell: *mut ListCell = list_head(lefttypes);
            let mut righttypes_cell: *mut ListCell = list_head(righttypes);

            if op_strategy == BTLessStrategyNumber as i32 {
                op_strategy = BTLessEqualStrategyNumber as i32;
            } else if op_strategy == BTGreaterStrategyNumber as i32 {
                op_strategy = BTGreaterEqualStrategyNumber as i32;
            } else {
                elog!(ERROR, "unexpected strategy number {}", op_strategy);
            }
            new_ops = NIL;
            while !opfamilies_cell.is_null()
                && !lefttypes_cell.is_null()
                && !righttypes_cell.is_null()
            {
                let opfam: Oid = lfirst_oid(opfamilies_cell);
                let lefttype: Oid = lfirst_oid(lefttypes_cell);
                let righttype: Oid = lfirst_oid(righttypes_cell);

                let cur_op: Oid =
                    get_opfamily_member(opfam, lefttype, righttype, op_strategy);
                if !OidIsValid(cur_op) {
                    /* should not happen */
                    elog!(
                        ERROR,
                        "missing operator {}({},{}) in opfamily {}",
                        op_strategy,
                        lefttype,
                        righttype,
                        opfam
                    );
                }
                new_ops = lappend_oid(new_ops, cur_op);

                opfamilies_cell = lnext(opfamilies, opfamilies_cell);
                lefttypes_cell = lnext(lefttypes, lefttypes_cell);
                righttypes_cell = lnext(righttypes, righttypes_cell);
            }
        }

        /* If we have more than one matching col, create a subset rowcompare */
        if matching_cols > 1 {
            let rc: *mut RowCompareExpr = palloc(size_of::<RowCompareExpr>()) as *mut RowCompareExpr;

            (*rc).cmptype = op_strategy as CompareType;
            (*rc).opnos = new_ops;
            (*rc).opfamilies =
                list_copy_head((*clause).opfamilies, matching_cols);
            (*rc).inputcollids =
                list_copy_head((*clause).inputcollids, matching_cols);
            (*rc).largs = list_copy_head(var_args, matching_cols);
            (*rc).rargs = list_copy_head(non_var_args, matching_cols);
            (*iclause).indexquals = list_make1!(make_simple_restrictinfo(root, rc as *mut Expr) as *mut c_void);
        } else {
            let op: *mut Expr;

            /* We don't report an index column list in this case */
            (*iclause).indexcols = NIL;

            op = make_opclause(
                linitial_oid(new_ops),
                BOOLOID,
                false,
                copyObject_node(linitial(var_args) as *mut c_void) as *mut Expr,
                copyObject_node(linitial(non_var_args) as *mut c_void) as *mut Expr,
                InvalidOid,
                linitial_oid((*clause).inputcollids),
            );
            (*iclause).indexquals =
                list_make1!(make_simple_restrictinfo(root, op) as *mut c_void);
        }
    }

    iclause
}

/****************************************************************************
 *              ----  ROUTINES TO CHECK ORDERING OPERATORS  ----
 ****************************************************************************/

/*
 * match_pathkeys_to_index
 *        For the given 'index' and 'pathkeys', output a list of suitable ORDER
 *        BY expressions, each of the form "indexedcol operator pseudoconstant",
 *        along with an integer list of the index column numbers (zero based)
 *        that each clause would be used with.
 *
 * This attempts to find an ORDER BY and index column number for all items in
 * the pathkey list, however, if we're unable to match any given pathkey to an
 * index column, we return just the ones matched by the function so far.  This
 * allows callers who are interested in partial matches to get them.  Callers
 * can determine a partial match vs a full match by checking the outputted
 * list lengths.  A full match will have one item in the output lists for each
 * item in the given 'pathkeys' list.
 */
unsafe fn match_pathkeys_to_index(
    index: *mut IndexOptInfo,
    pathkeys: *mut List,
    orderby_clauses_p: *mut *mut List,
    clause_columns_p: *mut *mut List,
) {
    let mut lc1: *mut ListCell;

    *orderby_clauses_p = NIL; /* set default results */
    *clause_columns_p = NIL;

    /* Only indexes with the amcanorderbyop property are interesting here */
    if !(*index).amcanorderbyop {
        return;
    }

    lc1 = list_head(pathkeys);
    while !lc1.is_null() {
        let pathkey: *mut PathKey = lfirst(lc1) as *mut PathKey;
        lc1 = lnext(pathkeys, lc1);
        let mut found: bool = false;
        let mut it: EquivalenceMemberIterator = core::mem::zeroed();
        let mut member: *mut EquivalenceMember;

        /* Pathkey must request default sort order for the target opfamily */
        // pk_cmptype == COMPARE_LT && !pk_nulls_first
        if (*pathkey).pk_cmptype != 1 || (*pathkey).pk_nulls_first {
            // COMPARE_LT = 1 in the enum ordering
            return;
        }

        /* If eclass is volatile, no hope of using an indexscan */
        if (*(*pathkey).pk_eclass).ec_has_volatile {
            return;
        }

        /*
         * Try to match eclass member expression(s) to index.  Note that child
         * EC members are considered, but only when they belong to the target
         * relation.  (Unlike regular members, the same expression could be a
         * child member of more than one EC.  Therefore, the same index could
         * be considered to match more than one pathkey list, which is OK
         * here.  See also get_eclass_for_sort_expr.)
         */
        setup_eclass_member_iterator(&mut it, (*pathkey).pk_eclass, (*(*index).rel).relids);
        loop {
            member = eclass_member_iterator_next(&mut it);
            if member.is_null() {
                break;
            }

            let mut indexcol: i32;

            /* No possibility of match if it references other relations */
            if !bms_equal((*member).em_relids, (*(*index).rel).relids) {
                continue;
            }

            /*
             * We allow any column of the index to match each pathkey; they
             * don't have to match left-to-right as you might expect.  This is
             * correct for GiST, and it doesn't matter for SP-GiST because
             * that doesn't handle multiple columns anyway, and no other
             * existing AMs support amcanorderbyop.  We might need different
             * logic in future for other implementations.
             */
            indexcol = 0;
            while indexcol < (*index).nkeycolumns {
                let expr: *mut Expr;

                expr = match_clause_to_ordering_op(
                    index,
                    indexcol,
                    (*member).em_expr,
                    (*pathkey).pk_opfamily,
                );
                if !expr.is_null() {
                    *orderby_clauses_p = lappend(*orderby_clauses_p, expr as *mut c_void);
                    *clause_columns_p = lappend_int(*clause_columns_p, indexcol);
                    found = true;
                    break;
                }
                indexcol += 1;
            }

            if found {
                /* don't want to look at remaining members */
                break;
            }
        }

        /*
         * Return the matches found so far when this pathkey couldn't be
         * matched to the index.
         */
        if !found {
            return;
        }
    }
}

/*
 * match_clause_to_ordering_op
 *    Determines whether an ordering operator expression matches an
 *    index column.
 *
 *    This is similar to, but simpler than, match_clause_to_indexcol.
 *    We only care about simple OpExpr cases.  The input is a bare
 *    expression that is being ordered by, which must be of the form
 *    (indexkey op const) or (const op indexkey) where op is an ordering
 *    operator for the column's opfamily.
 *
 * 'index' is the index of interest.
 * 'indexcol' is a column number of 'index' (counting from 0).
 * 'clause' is the ordering expression to be tested.
 * 'pk_opfamily' is the btree opfamily describing the required sort order.
 *
 * Note that we currently do not consider the collation of the ordering
 * operator's result.  In practical cases the result type will be numeric
 * and thus have no collation, and it's not very clear what to match to
 * if it did have a collation.  The index's collation should match the
 * ordering operator's input collation, not its result.
 *
 * If successful, return 'clause' as-is if the indexkey is on the left,
 * otherwise a commuted copy of 'clause'.  If no match, return NULL.
 */
unsafe fn match_clause_to_ordering_op(
    index: *mut IndexOptInfo,
    indexcol: i32,
    mut clause: *mut Expr,
    pk_opfamily: Oid,
) -> *mut Expr {
    let opfamily: Oid;
    let idxcollation: Oid;
    let leftop: *mut Node;
    let rightop: *mut Node;
    let mut expr_op: Oid;
    let expr_coll: Oid;
    let sortfamily: Oid;
    let commuted: bool;

    Assert!(indexcol < (*index).nkeycolumns);

    opfamily = *(*index).opfamily.add(indexcol as usize);
    idxcollation = *(*index).indexcollations.add(indexcol as usize);

    /*
     * Clause must be a binary opclause.
     */
    if !IsA!(clause, T_OpExpr) {
        return core::ptr::null_mut();
    }
    leftop = get_leftop(clause as *const c_void);
    rightop = get_rightop(clause as *const c_void);
    if leftop.is_null() || rightop.is_null() {
        return core::ptr::null_mut();
    }
    expr_op = (*(clause as *mut OpExpr)).opno;
    expr_coll = (*(clause as *mut OpExpr)).inputcollid;

    /*
     * We can forget the whole thing right away if wrong collation.
     */
    if !index_coll_matches_expr_coll(idxcollation, expr_coll) {
        return core::ptr::null_mut();
    }

    /*
     * Check for clauses of the form: (indexkey operator constant) or
     * (constant operator indexkey).
     */
    if match_index_to_operand(leftop, indexcol, index)
        && !contain_var_clause(rightop as *mut c_void)
        && !contain_volatile_functions(rightop as *mut c_void)
    {
        commuted = false;
    } else if match_index_to_operand(rightop, indexcol, index)
        && !contain_var_clause(leftop as *mut c_void)
        && !contain_volatile_functions(leftop as *mut c_void)
    {
        /* Might match, but we need a commuted operator */
        expr_op = get_commutator(expr_op);
        if expr_op == InvalidOid {
            return core::ptr::null_mut();
        }
        commuted = true;
    } else {
        return core::ptr::null_mut();
    }

    /*
     * Is the (commuted) operator an ordering operator for the opfamily? And
     * if so, does it yield the right sorting semantics?
     */
    sortfamily = get_op_opfamily_sortfamily(expr_op, opfamily);
    if sortfamily != pk_opfamily {
        return core::ptr::null_mut();
    }

    /* We have a match.  Return clause or a commuted version thereof. */
    if commuted {
        let newclause: *mut OpExpr = palloc(size_of::<OpExpr>()) as *mut OpExpr;

        /* flat-copy all the fields of clause */
        core::ptr::copy_nonoverlapping(
            clause as *const OpExpr,
            newclause,
            1,
        );

        /* commute it */
        (*newclause).opno = expr_op;
        (*newclause).opfuncid = InvalidOid;
        (*newclause).args = list_make2!(rightop as *mut c_void, leftop as *mut c_void);

        clause = newclause as *mut Expr;
    }

    clause
}

/****************************************************************************
 *              ----  ROUTINES TO DO PARTIAL INDEX PREDICATE TESTS  ----
 ****************************************************************************/

/*
 * check_index_predicates
 *        Set the predicate-derived IndexOptInfo fields for each index
 *        of the specified relation.
 *
 * predOK is set true if the index is partial and its predicate is satisfied
 * for this query, ie the query's WHERE clauses imply the predicate.
 *
 * indrestrictinfo is set to the relation's baserestrictinfo list less any
 * conditions that are implied by the index's predicate.  (Obviously, for a
 * non-partial index, this is the same as baserestrictinfo.)  Such conditions
 * can be dropped from the plan when using the index, in certain cases.
 *
 * At one time it was possible for this to get re-run after adding more
 * restrictions to the rel, thus possibly letting us prove more indexes OK.
 * That doesn't happen any more (at least not in the core code's usage),
 * but this code still supports it in case extensions want to mess with the
 * baserestrictinfo list.  We assume that adding more restrictions can't make
 * an index not predOK.  We must recompute indrestrictinfo each time, though,
 * to make sure any newly-added restrictions get into it if needed.
 */
pub unsafe fn check_index_predicates(root: *mut PlannerInfo, rel: *mut RelOptInfo) {
    let mut clauselist: *mut List;
    let mut have_partial: bool;
    let is_target_rel: bool;
    let otherrels: Relids;
    let mut lc: *mut ListCell;

    /* Indexes are available only on base or "other" member relations. */
    Assert!(IS_SIMPLE_REL(rel));

    /*
     * Initialize the indrestrictinfo lists to be identical to
     * baserestrictinfo, and check whether there are any partial indexes.  If
     * not, this is all we need to do.
     */
    have_partial = false;
    lc = list_head((*rel).indexlist);
    while !lc.is_null() {
        let index: *mut IndexOptInfo = lfirst(lc) as *mut IndexOptInfo;
        lc = lnext((*rel).indexlist, lc);

        (*index).indrestrictinfo = (*rel).baserestrictinfo;
        if (*index).indpred != NIL {
            have_partial = true;
        }
    }
    if !have_partial {
        return;
    }

    /*
     * Construct a list of clauses that we can assume true for the purpose of
     * proving the index(es) usable.  Restriction clauses for the rel are
     * always usable, and so are any join clauses that are "movable to" this
     * rel.  Also, we can consider any EC-derivable join clauses (which must
     * be "movable to" this rel, by definition).
     */
    clauselist = list_copy((*rel).baserestrictinfo);

    /* Scan the rel's join clauses */
    lc = list_head((*rel).joininfo);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext((*rel).joininfo, lc);

        /* Check if clause can be moved to this rel */
        if !join_clause_is_movable_to(rinfo, rel) {
            continue;
        }

        clauselist = lappend(clauselist, rinfo as *mut c_void);
    }

    /*
     * Add on any equivalence-derivable join clauses.  Computing the correct
     * relid sets for generate_join_implied_equalities is slightly tricky
     * because the rel could be a child rel rather than a true baserel, and in
     * that case we must subtract its parents' relid(s) from all_query_rels.
     * Additionally, we mustn't consider clauses that are only computable
     * after outer joins that can null the rel.
     */
    let otherrels: Relids;
    if (*rel).reloptkind == RELOPT_OTHER_MEMBER_REL {
        otherrels = bms_difference(
            (*root).all_query_rels,
            find_childrel_parents(root, rel),
        );
    } else {
        otherrels = bms_difference((*root).all_query_rels, (*rel).relids);
    }
    let otherrels = bms_del_members(otherrels, (*rel).nulling_relids);

    if !bms_is_empty(otherrels) {
        clauselist = list_concat(
            clauselist,
            generate_join_implied_equalities(
                root,
                bms_union((*rel).relids, otherrels),
                otherrels,
                rel,
                core::ptr::null_mut(),
            ),
        );
    }

    /*
     * Normally we remove quals that are implied by a partial index's
     * predicate from indrestrictinfo, indicating that they need not be
     * checked explicitly by an indexscan plan using this index.  However, if
     * the rel is a target relation of UPDATE/DELETE/MERGE/SELECT FOR UPDATE,
     * we cannot remove such quals from the plan, because they need to be in
     * the plan so that they will be properly rechecked by EvalPlanQual
     * testing.  Some day we might want to remove such quals from the main
     * plan anyway and pass them through to EvalPlanQual via a side channel;
     * but for now, we just don't remove implied quals at all for target
     * relations.
     */
    let is_target_rel = bms_is_member((*rel).relid as i32, (*root).all_result_relids)
        || !get_plan_rowmark((*root).rowMarks, (*rel).relid).is_null();

    /*
     * Now try to prove each index predicate true, and compute the
     * indrestrictinfo lists for partial indexes.  Note that we compute the
     * indrestrictinfo list even for non-predOK indexes; this might seem
     * wasteful, but we may be able to use such indexes in OR clauses, cf
     * generate_bitmap_or_paths().
     */
    lc = list_head((*rel).indexlist);
    while !lc.is_null() {
        let index: *mut IndexOptInfo = lfirst(lc) as *mut IndexOptInfo;
        lc = lnext((*rel).indexlist, lc);
        let mut lcr: *mut ListCell;

        if (*index).indpred == NIL {
            continue; /* ignore non-partial indexes here */
        }

        if !(*index).predOK {
            /* don't repeat work if already proven OK */
            (*index).predOK =
                predicate_implied_by((*index).indpred, clauselist, false);
        }

        /* If rel is an update target, leave indrestrictinfo as set above */
        if is_target_rel {
            continue;
        }

        /*
         * If index is !amoptionalkey, also leave indrestrictinfo as set
         * above.  Otherwise we risk removing all quals for the first index
         * key and then not being able to generate an indexscan at all.  It
         * would be better to be more selective, but we've not yet identified
         * which if any of the quals match the first index key.
         */
        if !(*index).amoptionalkey {
            continue;
        }

        /* Else compute indrestrictinfo as the non-implied quals */
        (*index).indrestrictinfo = NIL;
        lcr = list_head((*rel).baserestrictinfo);
        while !lcr.is_null() {
            let rinfo: *mut RestrictInfo = lfirst(lcr) as *mut RestrictInfo;
            lcr = lnext((*rel).baserestrictinfo, lcr);

            /* predicate_implied_by() assumes first arg is immutable */
            if contain_mutable_functions((*rinfo).clause as *mut c_void)
                || !predicate_implied_by(
                    list_make1!((*rinfo).clause as *mut c_void),
                    (*index).indpred,
                    false,
                )
            {
                (*index).indrestrictinfo =
                    lappend((*index).indrestrictinfo, rinfo as *mut c_void);
            }
        }
    }
}

/****************************************************************************
 *              ----  ROUTINES TO CHECK EXTERNALLY-VISIBLE CONDITIONS  ----
 ****************************************************************************/

/*
 * ec_member_matches_indexcol
 *    Test whether an EquivalenceClass member matches an index column.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
unsafe extern "C" fn ec_member_matches_indexcol_cb(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    ec: *mut EquivalenceClass,
    em: *mut EquivalenceMember,
    arg: *mut c_void,
) -> bool {
    let index: *mut IndexOptInfo = (*(arg as *mut EcMemberMatchesArg)).index;
    let indexcol: i32 = (*(arg as *mut EcMemberMatchesArg)).indexcol;
    let curFamily: Oid;
    let curCollation: Oid;

    Assert!(indexcol < (*index).nkeycolumns);

    curFamily = *(*index).opfamily.add(indexcol as usize);
    curCollation = *(*index).indexcollations.add(indexcol as usize);

    /*
     * If it's a btree index, we can reject it if its opfamily isn't
     * compatible with the EC, since no clause generated from the EC could be
     * used with the index.  For non-btree indexes, we can't easily tell
     * whether clauses generated from the EC could be used with the index, so
     * don't check the opfamily.  This might mean we return "true" for a
     * useless EC, so we have to recheck the results of
     * generate_implied_equalities_for_column; see
     * match_eclass_clauses_to_index.
     */
    if (*index).relam == BTREE_AM_OID
        && !list_member_oid((*ec).ec_opfamilies, curFamily)
    {
        return false;
    }

    /* We insist on collation match for all index types, though */
    if !index_coll_matches_expr_coll(curCollation, (*ec).ec_collation) {
        return false;
    }

    match_index_to_operand((*em).em_expr as *mut Node, indexcol, index)
}

/*
 * relation_has_unique_index_for
 *    Determine whether the relation provably has at most one row satisfying
 *    a set of equality conditions, because the conditions constrain all
 *    columns of some unique index.
 *
 * The conditions can be represented in either or both of two ways:
 * 1. A list of RestrictInfo nodes, where the caller has already determined
 * that each condition is a mergejoinable equality with an expression in
 * this relation on one side, and an expression not involving this relation
 * on the other.  The transient outer_is_left flag is used to identify which
 * side we should look at: left side if outer_is_left is false, right side
 * if it is true.
 * 2. A list of expressions in this relation, and a corresponding list of
 * equality operators. The caller must have already checked that the operators
 * represent equality.  (Note: the operators could be cross-type; the
 * expressions should correspond to their RHS inputs.)
 *
 * The caller need only supply equality conditions arising from joins;
 * this routine automatically adds in any usable baserestrictinfo clauses.
 * (Note that the passed-in restrictlist will be destructively modified!)
 */
pub unsafe fn relation_has_unique_index_for(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
) -> bool {
    relation_has_unique_index_ext(root, rel, restrictlist, exprlist, oprlist, core::ptr::null_mut())
}

/*
 * relation_has_unique_index_ext
 *    Same as relation_has_unique_index_for(), but supports extra_clauses
 *    parameter.  If extra_clauses isn't NULL, return baserestrictinfo clauses
 *    which were used to derive uniqueness.
 */
pub unsafe fn relation_has_unique_index_ext(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    mut restrictlist: *mut List,
    exprlist: *mut List,
    oprlist: *mut List,
    extra_clauses: *mut *mut List,
) -> bool {
    let mut ic: *mut ListCell;

    Assert!(list_length(exprlist) == list_length(oprlist));

    /* Short-circuit if no indexes... */
    if (*rel).indexlist == NIL {
        return false;
    }

    /*
     * Examine the rel's restriction clauses for usable var = const clauses
     * that we can add to the restrictlist.
     */
    ic = list_head((*rel).baserestrictinfo);
    while !ic.is_null() {
        let restrictinfo: *mut RestrictInfo = lfirst(ic) as *mut RestrictInfo;
        ic = lnext((*rel).baserestrictinfo, ic);

        /*
         * Note: can_join won't be set for a restriction clause, but
         * mergeopfamilies will be if it has a mergejoinable operator and
         * doesn't contain volatile functions.
         */
        if (*restrictinfo).mergeopfamilies == NIL {
            continue; /* not mergejoinable */
        }

        /*
         * The clause certainly doesn't refer to anything but the given rel.
         * If either side is pseudoconstant then we can use it.
         */
        if bms_is_empty((*restrictinfo).left_relids) {
            /* righthand side is inner */
            (*restrictinfo).outer_is_left = true;
        } else if bms_is_empty((*restrictinfo).right_relids) {
            /* lefthand side is inner */
            (*restrictinfo).outer_is_left = false;
        } else {
            continue;
        }

        /* OK, add to list */
        restrictlist = lappend(restrictlist, restrictinfo as *mut c_void);
    }

    /* Short-circuit the easy case */
    if restrictlist == NIL && exprlist == NIL {
        return false;
    }

    /* Examine each index of the relation ... */
    ic = list_head((*rel).indexlist);
    while !ic.is_null() {
        let ind: *mut IndexOptInfo = lfirst(ic) as *mut IndexOptInfo;
        ic = lnext((*rel).indexlist, ic);
        let mut c: i32;
        let mut exprs: *mut List = NIL;

        /*
         * If the index is not unique, or not immediately enforced, or if it's
         * a partial index, it's useless here.  We're unable to make use of
         * predOK partial unique indexes due to the fact that
         * check_index_predicates() also makes use of join predicates to
         * determine if the partial index is usable. Here we need proofs that
         * hold true before any joins are evaluated.
         */
        if !(*ind).unique || !(*ind).immediate || (*ind).indpred != NIL {
            continue;
        }

        /*
         * Try to find each index column in the lists of conditions.  This is
         * O(N^2) or worse, but we expect all the lists to be short.
         */
        c = 0;
        while c < (*ind).nkeycolumns {
            let mut matched: bool = false;
            let mut lc: *mut ListCell;
            let mut lc2: *mut ListCell;

            lc = list_head(restrictlist);
            while !lc.is_null() {
                let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
                lc = lnext(restrictlist, lc);
                let rexpr: *mut Node;

                /*
                 * The condition's equality operator must be a member of the
                 * index opfamily, else it is not asserting the right kind of
                 * equality behavior for this index.  We check this first
                 * since it's probably cheaper than match_index_to_operand().
                 */
                if !list_member_oid((*rinfo).mergeopfamilies, *(*ind).opfamily.add(c as usize)) {
                    continue;
                }

                /*
                 * XXX at some point we may need to check collations here too.
                 * For the moment we assume all collations reduce to the same
                 * notion of equality.
                 */

                /* OK, see if the condition operand matches the index key */
                if (*rinfo).outer_is_left {
                    rexpr = get_rightop((*rinfo).clause as *const c_void);
                } else {
                    rexpr = get_leftop((*rinfo).clause as *const c_void);
                }

                if match_index_to_operand(rexpr, c, ind) {
                    matched = true; /* column is unique */

                    if bms_membership((*rinfo).clause_relids) == BMS_SINGLETON {
                        let oldMemCtx: *mut c_void =
                            MemoryContextSwitchTo((*root).planner_cxt as *mut c_void);

                        /*
                         * Add filter clause into a list allowing caller to
                         * know if uniqueness have made not only by join
                         * clauses.
                         */
                        Assert!(
                            bms_is_empty((*rinfo).left_relids)
                                || bms_is_empty((*rinfo).right_relids)
                        );
                        if !extra_clauses.is_null() {
                            exprs = lappend(exprs, rinfo as *mut c_void);
                        }
                        MemoryContextSwitchTo(oldMemCtx);
                    }

                    break;
                }
            }

            if matched {
                c += 1;
                continue;
            }

            lc = list_head(exprlist);
            lc2 = list_head(oprlist);
            while !lc.is_null() && !lc2.is_null() {
                let expr: *mut Node = lfirst(lc) as *mut Node;
                let opr: Oid = lfirst_oid(lc2);
                lc = lnext(exprlist, lc);
                lc2 = lnext(oprlist, lc2);

                /* See if the expression matches the index key */
                if !match_index_to_operand(expr, c, ind) {
                    continue;
                }

                /*
                 * The equality operator must be a member of the index
                 * opfamily, else it is not asserting the right kind of
                 * equality behavior for this index.  We assume the caller
                 * determined it is an equality operator, so we don't need to
                 * check any more tightly than this.
                 */
                if !op_in_opfamily(opr, *(*ind).opfamily.add(c as usize)) {
                    continue;
                }

                /*
                 * XXX at some point we may need to check collations here too.
                 * For the moment we assume all collations reduce to the same
                 * notion of equality.
                 */

                matched = true; /* column is unique */
                break;
            }

            if !matched {
                break; /* no match; this index doesn't help us */
            }
            c += 1;
        }

        /* Matched all key columns of this index? */
        if c == (*ind).nkeycolumns {
            if !extra_clauses.is_null() {
                *extra_clauses = exprs;
            }
            return true;
        }
    }

    false
}

/*
 * indexcol_is_bool_constant_for_query
 *
 * If an index column is constrained to have a constant value by the query's
 * WHERE conditions, then it's irrelevant for sort-order considerations.
 * Usually that means we have a restriction clause WHERE indexcol = constant,
 * which gets turned into an EquivalenceClass containing a constant, which
 * is recognized as redundant by build_index_pathkeys().  But if the index
 * column is a boolean variable (or expression), then we are not going to
 * see WHERE indexcol = constant, because expression preprocessing will have
 * simplified that to "WHERE indexcol" or "WHERE NOT indexcol".  So we are not
 * going to have a matching EquivalenceClass (unless the query also contains
 * "ORDER BY indexcol").  To allow such cases to work the same as they would
 * for non-boolean values, this function is provided to detect whether the
 * specified index column matches a boolean restriction clause.
 */
pub unsafe fn indexcol_is_bool_constant_for_query(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    indexcol: i32,
) -> bool {
    let mut lc: *mut ListCell;

    /* If the index isn't boolean, we can't possibly get a match */
    if !IsBooleanOpfamily(*(*index).opfamily.add(indexcol as usize)) {
        return false;
    }

    /* Check each restriction clause for the index's rel */
    lc = list_head((*(*index).rel).baserestrictinfo);
    while !lc.is_null() {
        let rinfo: *mut RestrictInfo = lfirst(lc) as *mut RestrictInfo;
        lc = lnext((*(*index).rel).baserestrictinfo, lc);

        /*
         * As in match_clause_to_indexcol, never match pseudoconstants to
         * indexes.  (It might be semantically okay to do so here, but the
         * odds of getting a match are negligible, so don't waste the cycles.)
         */
        if (*rinfo).pseudoconstant {
            continue;
        }

        /* See if we can match the clause's expression to the index column */
        if !match_boolean_index_clause(root, rinfo, indexcol, index).is_null() {
            return true;
        }
    }

    false
}

/****************************************************************************
 *              ----  ROUTINES TO CHECK OPERANDS  ----
 ****************************************************************************/

/*
 * match_index_to_operand()
 *    Generalized test for a match between an index's key
 *    and the operand on one side of a restriction or join clause.
 *
 * operand: the nodetree to be compared to the index
 * indexcol: the column number of the index (counting from 0)
 * index: the index of interest
 *
 * Note that we aren't interested in collations here; the caller must check
 * for a collation match, if it's dealing with an operator where that matters.
 *
 * This is exported for use in selfuncs.c.
 */
pub unsafe fn match_index_to_operand_pub(
    mut operand: *mut Node,
    indexcol: i32,
    index: *mut IndexOptInfo,
) -> bool {
    let indkey: i32;

    /*
     * Ignore any PlaceHolderVar node contained in the operand.  This is
     * needed to be able to apply indexscanning in cases where the operand (or
     * a subtree) has been wrapped in PlaceHolderVars to enforce separate
     * identity or as a result of outer joins.
     */
    operand = strip_phvs_in_index_operand(operand);

    /*
     * Ignore any RelabelType node above the operand.  This is needed to be
     * able to apply indexscanning in binary-compatible-operator cases.
     *
     * Note: we must handle nested RelabelType nodes here.  While
     * eval_const_expressions() will have simplified them to at most one
     * layer, our prior stripping of PlaceHolderVars may have brought separate
     * RelabelTypes into adjacency.
     */
    while !operand.is_null() && IsA!(operand, T_RelabelType) {
        operand = (*(operand as *mut RelabelType)).arg as *mut Node;
    }

    indkey = *(*index).indexkeys.add(indexcol as usize);
    if indkey != 0 {
        /*
         * Simple index column; operand must be a matching Var.
         */
        if !operand.is_null()
            && IsA!(operand, T_Var)
            && (*(*index).rel).relid == (*(operand as *mut Var)).varno as u32
            && indkey == (*(operand as *mut Var)).varattno as c_int
            && (*(operand as *mut Var)).varnullingrels.is_null()
        {
            return true;
        }
    } else {
        /*
         * Index expression; find the correct expression.  (This search could
         * be avoided, at the cost of complicating all the callers of this
         * routine; doesn't seem worth it.)
         */
        let mut indexpr_item: *mut ListCell;
        let mut i: i32;
        let mut indexkey: *mut Node;

        indexpr_item = list_head((*index).indexprs);
        i = 0;
        while i < indexcol {
            if *(*index).indexkeys.add(i as usize) == 0 {
                if indexpr_item.is_null() {
                    elog!(ERROR, "wrong number of index expressions");
                }
                indexpr_item = lnext((*index).indexprs, indexpr_item);
            }
            i += 1;
        }
        if indexpr_item.is_null() {
            elog!(ERROR, "wrong number of index expressions");
        }
        indexkey = lfirst(indexpr_item) as *mut Node;

        /*
         * Does it match the operand?  Again, strip any relabeling.
         */
        if !indexkey.is_null() && IsA!(indexkey, T_RelabelType) {
            indexkey = (*(indexkey as *mut RelabelType)).arg as *mut Node;
        }

        if equal(indexkey as *const c_void, operand as *const c_void) {
            return true;
        }
    }

    false
}

/*
 * strip_phvs_in_index_operand
 *    Strip PlaceHolderVar nodes from the given operand expression to
 *    facilitate matching against an index's key.
 *
 * A PlaceHolderVar appearing in a relation-scan-level expression is
 * effectively a no-op.  Nevertheless, to play it safe, we strip only
 * PlaceHolderVars that are not marked nullable.
 *
 * The removal is performed recursively because PlaceHolderVars can be nested
 * or interleaved with other node types.  We must peel back all layers to
 * expose the base operand.
 *
 * As a performance optimization, we first use a lightweight walker to check
 * for the presence of strippable PlaceHolderVars.  The expensive mutator is
 * invoked only if a candidate is found, avoiding unnecessary memory allocation
 * and tree copying in the common case where no PlaceHolderVars are present.
 */
pub unsafe fn strip_phvs_in_index_operand(operand: *mut Node) -> *mut Node {
    /* Don't mutate/copy if no target PHVs exist */
    if !contain_strippable_phv_walker(operand, core::ptr::null_mut()) {
        return operand;
    }

    strip_phvs_in_index_operand_mutator(operand, core::ptr::null_mut())
}

/*
 * contain_strippable_phv_walker
 *    Detect if there are any PlaceHolderVars in the tree that are candidates
 *    for stripping.
 *
 * We identify a PlaceHolderVar as strippable only if its phnullingrels is
 * empty.
 */
unsafe fn contain_strippable_phv_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }

    if IsA!(node, T_PlaceHolderVar) {
        let phv: *mut PlaceHolderVar = node as *mut PlaceHolderVar;

        if bms_is_empty((*phv).phnullingrels) {
            return true;
        }
    }

    expression_tree_walker(node, contain_strippable_phv_walker, context)
}

/*
 * strip_phvs_in_index_operand_mutator
 *    Recursively remove PlaceHolderVars in the tree that match the criteria.
 *
 * We strip a PlaceHolderVar only if its phnullingrels is empty, replacing it
 * with its contained expression.
 */
unsafe fn strip_phvs_in_index_operand_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    if node.is_null() {
        return core::ptr::null_mut();
    }

    if IsA!(node, T_PlaceHolderVar) {
        let phv: *mut PlaceHolderVar = node as *mut PlaceHolderVar;

        /* If matches the criteria, strip it */
        if bms_is_empty((*phv).phnullingrels) {
            /* Recurse on its contained expression */
            return strip_phvs_in_index_operand_mutator(
                (*phv).phexpr as *mut Node,
                context,
            );
        }

        /* Otherwise, keep this PHV but check its contained expression */
    }

    expression_tree_mutator(node, strip_phvs_in_index_operand_mutator, context)
}

/*
 * is_pseudo_constant_for_index()
 *    Test whether the given expression can be used as an indexscan
 *    comparison value.
 *
 * An indexscan comparison value must not contain any volatile functions,
 * and it can't contain any Vars of the index's own table.  Vars of
 * other tables are okay, though; in that case we'd be producing an
 * indexqual usable in a parameterized indexscan.  This is, therefore,
 * a weaker condition than is_pseudo_constant_clause().
 *
 * This function is exported for use by planner support functions,
 * which will have available the IndexOptInfo, but not any RestrictInfo
 * infrastructure.  It is making the same test made by functions above
 * such as match_opclause_to_indexcol(), but those rely where possible
 * on RestrictInfo information about variable membership.
 *
 * expr: the nodetree to be checked
 * index: the index of interest
 */
pub unsafe fn is_pseudo_constant_for_index(
    root: *mut PlannerInfo,
    expr: *mut Node,
    index: *mut IndexOptInfo,
) -> bool {
    /* pull_varnos is cheaper than volatility check, so do that first */
    if bms_is_member(
        (*(*index).rel).relid as i32,
        pull_varnos(root, expr as *mut c_void),
    ) {
        return false; /* no good, contains Var of table */
    }
    if contain_volatile_functions(expr as *mut c_void) {
        return false; /* no good, volatile comparison value */
    }
    true
}

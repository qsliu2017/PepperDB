//! initsplan.rs
//!   Target list, group by, qualification, joininfo initialization routines
//!
//! Translated 1:1 from postgres/src/backend/optimizer/plan/initsplan.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/initsplan.c

#![allow(unused_variables)]
#![allow(unreachable_code)]
#![allow(unreachable_patterns)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_assignments)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;
use crate::{
    foreach, IsA, makeNode, lfirst_node, Assert, elog, ereport,
    list_make1, list_make2, linitial_node};

use std::ptr;
use std::ffi::{c_int, c_char};

use crate::nodes::nodes::{
    Node, NodeTag, nodeTag,
    JoinType, JOIN_INNER, JOIN_LEFT, JOIN_ANTI, JOIN_SEMI, JOIN_FULL,
};
use crate::nodes::pg_list::{
    List, ListCell, NIL,
    list_length, list_concat, list_free, list_free_deep,
    lappend, lappend_oid, linitial, llast,
    lfirst, lfirst_int, lfirst_oid,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_make_singleton, bms_add_member, bms_del_member,
    bms_add_members, bms_del_members, bms_int_members,
    bms_union, bms_copy, bms_intersect, bms_difference,
    bms_is_empty, bms_is_member, bms_is_subset, bms_overlap,
    bms_equal, bms_next_member, bms_get_singleton_member,
    bms_membership, BMS_SINGLETON, BMS_MULTIPLE,
    bms_subset_compare, BMS_SUBSET1,
};
use crate::nodes::primnodes::{
    Var, Expr, OpExpr, NullTest, BoolExpr, RelabelType,
    TargetEntry, RangeTblRef, FromExpr, JoinExpr,
    INNER_VAR, OUTER_VAR,
    IS_NULL, IS_NOT_NULL,
};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, PlaceHolderVar, PlaceHolderInfo,
    AppendRelInfo, SpecialJoinInfo, JoinDomain, OuterJoinClauseInfo,
    ForeignKeyOptInfo, RestrictInfo, IndexOptInfo,
    Relids,
    RELOPT_BASEREL,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RowMarkClause, SortGroupClause,
    RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES,
};
use crate::c::Index;
use crate::access::attnum::AttrNumber;
use crate::postgres_ext::Oid;
use crate::postgres::Datum;

// ---------------------------------------------------------------------------
// GUC parameters (set by GUC, initialized to defaults matching PostgreSQL).
// ---------------------------------------------------------------------------

/// from_collapse_limit GUC
pub static mut from_collapse_limit: c_int = 8;

/// join_collapse_limit GUC
pub static mut join_collapse_limit: c_int = 8;

// ---------------------------------------------------------------------------
// Local struct: JoinTreeItem
//
// deconstruct_jointree requires multiple passes over the join tree, because we
// need to finish computing JoinDomains before we start distributing quals.
// As long as we have to do that, other information such as the relevant
// qualscopes might as well be computed in the first pass too.
//
// deconstruct_recurse recursively examines the join tree and builds a List
// (in depth-first traversal order) of JoinTreeItem structs, which are then
// processed iteratively by deconstruct_distribute.  If there are outer
// joins, non-degenerate outer join clauses are processed in a third pass
// deconstruct_distribute_oj_quals.
//
// The JoinTreeItem structs themselves can be freed at the end of
// deconstruct_jointree, but do not modify or free their substructure,
// as the relid sets may also be pointed to by RestrictInfo and
// SpecialJoinInfo nodes.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct JoinTreeItem {
    // Fields filled during deconstruct_recurse:
    /// jointree node to examine
    pub jtnode: *mut Node,
    /// join domain for its ON/WHERE clauses
    pub jdomain: *mut JoinDomain,
    /// JoinTreeItem for this node's parent, or NULL if it's the top
    pub jti_parent: *mut JoinTreeItem,
    /// base+OJ Relids syntactically included in this jointree node
    pub qualscope: Relids,
    /// base+OJ Relids syntactically included in inner joins appearing at or
    /// below this jointree node
    pub inner_join_rels: Relids,
    /// if join node, Relids of the left side
    pub left_rels: Relids,
    /// if join node, Relids of the right side
    pub right_rels: Relids,
    /// if outer join, Relids of the non-nullable side
    pub nonnullable_rels: Relids,
    // Fields filled during deconstruct_distribute:
    /// if outer join, its SpecialJoinInfo
    pub sjinfo: *mut SpecialJoinInfo,
    /// outer join quals not yet distributed
    pub oj_joinclauses: *mut List,
    /// quals postponed from children due to lateral references
    pub lateral_clauses: *mut List,
}

// ---------------------------------------------------------------------------
// Stubs for unported dependencies.  TODO(pg-port): replace with real ports.
// ---------------------------------------------------------------------------

/// copyObject() (nodes/copyfuncs.c): deep copy of a node tree.
/// TODO(pg-port): replace with real recursive copyObject once copyfuncs.c is translated.
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    if node.is_null() {
        return ptr::null_mut();
    }
    let p = palloc(core::mem::size_of::<T>()) as *mut T;
    ptr::copy_nonoverlapping(node, p, 1);
    p
}

/// palloc0_object: allocate zero-initialized memory for a single object.
/// Mirrors the palloc0_object() macro from palloc.h.
unsafe fn palloc0_object_JoinTreeItem() -> *mut JoinTreeItem {
    palloc0(core::mem::size_of::<JoinTreeItem>()) as *mut JoinTreeItem
}

/// build_simple_rel() (optimizer/util/relnode.c)
/// TODO(pg-port): real symbol lives in optimizer/util/relnode.rs
unsafe fn build_simple_rel(
    _root: *mut PlannerInfo,
    _varno: c_int,
    _parent: *mut RelOptInfo,
) -> *mut RelOptInfo {
    // TODO(pg-port): real build_simple_rel in optimizer/util/relnode.c
    unimplemented!()
}

/// expand_inherited_rtentry() (optimizer/prep/prepunion.c or inherit.c)
/// TODO(pg-port): real symbol lives in optimizer/inherit.rs
unsafe fn expand_inherited_rtentry(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _rte: *mut RangeTblEntry,
    _rti: Index,
) {
    // TODO(pg-port): real expand_inherited_rtentry in optimizer/inherit.c
    unimplemented!()
}

/// pull_var_clause() (optimizer/util/var.c)
/// TODO(pg-port): real symbol lives in optimizer/util/var.rs
pub const PVC_RECURSE_AGGREGATES: c_int = 0x001;
pub const PVC_RECURSE_WINDOWFUNCS: c_int = 0x002;
pub const PVC_INCLUDE_PLACEHOLDERS: c_int = 0x004;

unsafe fn pull_var_clause(_node: *mut Node, _flags: c_int) -> *mut List {
    // TODO(pg-port): real pull_var_clause in optimizer/util/var.c
    unimplemented!()
}

/// pull_vars_of_level() (optimizer/util/var.c)
/// TODO(pg-port): real symbol lives in optimizer/util/var.rs
unsafe fn pull_vars_of_level(_node: *mut Node, _levelsup: c_int) -> *mut List {
    // TODO(pg-port): real pull_vars_of_level in optimizer/util/var.c
    unimplemented!()
}

/// find_base_rel() (optimizer/util/relnode.c)
/// TODO(pg-port): real symbol lives in optimizer/util/relnode.rs
unsafe fn find_base_rel(_root: *mut PlannerInfo, _relid: c_int) -> *mut RelOptInfo {
    // TODO(pg-port): real find_base_rel in optimizer/util/relnode.c
    unimplemented!()
}

/// find_base_rel_ignore_join() (optimizer/util/relnode.c)
/// TODO(pg-port): real symbol lives in optimizer/util/relnode.rs
unsafe fn find_base_rel_ignore_join(
    _root: *mut PlannerInfo,
    _relid: c_int,
) -> *mut RelOptInfo {
    // TODO(pg-port): real find_base_rel_ignore_join in optimizer/util/relnode.c
    unimplemented!()
}

/// find_placeholder_info() (optimizer/util/placeholder.c)
/// TODO(pg-port): real symbol lives in optimizer/util/placeholder.rs
unsafe fn find_placeholder_info(
    _root: *mut PlannerInfo,
    _phv: *mut PlaceHolderVar,
) -> *mut PlaceHolderInfo {
    // TODO(pg-port): real find_placeholder_info in optimizer/util/placeholder.c
    unimplemented!()
}

/// get_sortgroupclause_tle() (optimizer/util/tlist.c)
/// TODO(pg-port): real symbol lives in optimizer/util/tlist.rs
unsafe fn get_sortgroupclause_tle(
    _sgc: *mut SortGroupClause,
    _targetlist: *mut List,
) -> *mut TargetEntry {
    // TODO(pg-port): real get_sortgroupclause_tle in optimizer/util/tlist.c
    unimplemented!()
}

/// IncrementVarSublevelsUp() (nodes/nodeFuncs.c)
/// TODO(pg-port): real symbol lives in nodes/nodeFuncs.rs
unsafe fn IncrementVarSublevelsUp(
    _node: *mut Node,
    _delta_sublevels_up: c_int,
    _min_sublevels_up: c_int,
) {
    // TODO(pg-port): real IncrementVarSublevelsUp in nodes/nodeFuncs.c
    unimplemented!()
}

/// preprocess_phv_expression() (optimizer/plan/planner.c)
/// TODO(pg-port): real symbol lives in optimizer/plan/planner.rs
unsafe fn preprocess_phv_expression(
    _root: *mut PlannerInfo,
    _expr: *mut Expr,
) -> *mut Expr {
    // TODO(pg-port): real preprocess_phv_expression in optimizer/plan/planner.c
    unimplemented!()
}

/// pull_varnos() (optimizer/util/var.c)
/// TODO(pg-port): real symbol lives in optimizer/util/var.rs
unsafe fn pull_varnos(_root: *mut PlannerInfo, _node: *mut Node) -> Relids {
    // TODO(pg-port): real pull_varnos in optimizer/util/var.c
    unimplemented!()
}

/// find_nonnullable_rels() (optimizer/util/clauses.c)
/// TODO(pg-port): real symbol lives in optimizer/util/clauses.rs
unsafe fn find_nonnullable_rels(_clause: *mut Node) -> Relids {
    // TODO(pg-port): real find_nonnullable_rels in optimizer/util/clauses.c
    unimplemented!()
}

/// contain_placeholder_references_to() (optimizer/util/placeholder.c)
/// TODO(pg-port): real symbol lives in optimizer/util/placeholder.rs
unsafe fn contain_placeholder_references_to(
    _root: *mut PlannerInfo,
    _clause: *mut Node,
    _ojrelid: Index,
) -> bool {
    // TODO(pg-port): real contain_placeholder_references_to in optimizer/util/placeholder.c
    unimplemented!()
}

/// contain_volatile_functions() (optimizer/util/clauses.c)
/// TODO(pg-port): real symbol lives in optimizer/util/clauses.rs
unsafe fn contain_volatile_functions(_clause: *mut Node) -> bool {
    // TODO(pg-port): real contain_volatile_functions in optimizer/util/clauses.c
    unimplemented!()
}

/// make_restrictinfo() (optimizer/util/restrictinfo.c)
/// TODO(pg-port): real symbol lives in optimizer/util/restrictinfo.rs
unsafe fn make_restrictinfo(
    _root: *mut PlannerInfo,
    _clause: *mut Expr,
    _is_pushed_down: bool,
    _has_clone: bool,
    _is_clone: bool,
    _pseudoconstant: bool,
    _security_level: Index,
    _required_relids: Relids,
    _incompatible_relids: Relids,
    _outer_relids: Relids,
) -> *mut RestrictInfo {
    // TODO(pg-port): real make_restrictinfo in optimizer/util/restrictinfo.c
    unimplemented!()
}

/// process_equivalence() (optimizer/path/equivclass.c)
/// TODO(pg-port): real symbol lives in optimizer/path/equivclass.rs
unsafe fn process_equivalence(
    _root: *mut PlannerInfo,
    _restrictinfo_p: *mut *mut RestrictInfo,
    _jdomain: *mut JoinDomain,
) -> bool {
    // TODO(pg-port): real process_equivalence in optimizer/path/equivclass.c
    unimplemented!()
}

/// initialize_mergeclause_eclasses() (optimizer/path/equivclass.c)
/// TODO(pg-port): real symbol lives in optimizer/path/equivclass.rs
unsafe fn initialize_mergeclause_eclasses(
    _root: *mut PlannerInfo,
    _restrictinfo: *mut RestrictInfo,
) {
    // TODO(pg-port): real initialize_mergeclause_eclasses in optimizer/path/equivclass.c
    unimplemented!()
}

/// distribute_restrictinfo_to_rels() -- forward declared; defined below.
/// (also called from equivclass.c so it's pub)

/// add_join_clause_to_rels() (optimizer/util/joininfo.c)
/// TODO(pg-port): real symbol lives in optimizer/util/joininfo.rs
unsafe fn add_join_clause_to_rels(
    _root: *mut PlannerInfo,
    _restrictinfo: *mut RestrictInfo,
    _join_relids: Relids,
) {
    // TODO(pg-port): real add_join_clause_to_rels in optimizer/util/joininfo.c
    unimplemented!()
}

/// restriction_is_or_clause() (optimizer/util/restrictinfo.c)
/// TODO(pg-port): real symbol lives in optimizer/util/restrictinfo.rs
unsafe fn restriction_is_or_clause(_rinfo: *mut RestrictInfo) -> bool {
    // TODO(pg-port): real restriction_is_or_clause in optimizer/util/restrictinfo.c
    unimplemented!()
}

/// is_orclause() (nodes/makefuncs.c or clauses.c)
/// TODO(pg-port): real symbol lives in nodes/makefuncs.rs or optimizer/util/clauses.rs
unsafe fn is_orclause(_clause: *mut Node) -> bool {
    // TODO(pg-port): real is_orclause
    unimplemented!()
}

/// is_opclause() (nodes/nodeFuncs.c)
/// TODO(pg-port): real symbol lives in nodes/nodeFuncs.rs
unsafe fn is_opclause(_clause: *mut Expr) -> bool {
    // TODO(pg-port): real is_opclause in nodes/nodeFuncs.c
    unimplemented!()
}

/// get_leftop() / get_rightop() (nodes/nodeFuncs.c)
/// TODO(pg-port): real symbols live in nodes/nodeFuncs.rs
unsafe fn get_leftop(_expr: *mut Expr) -> *mut Node {
    // TODO(pg-port): real get_leftop in nodes/nodeFuncs.c
    unimplemented!()
}
unsafe fn get_rightop(_expr: *mut Expr) -> *mut Node {
    // TODO(pg-port): real get_rightop in nodes/nodeFuncs.c
    unimplemented!()
}

/// op_mergejoinable() / op_hashjoinable() (utils/cache/lsyscache.c)
/// TODO(pg-port): real symbols live in utils/cache/lsyscache.rs
unsafe fn op_mergejoinable(_opno: Oid, _inputtype: Oid) -> bool {
    // TODO(pg-port): real op_mergejoinable in utils/cache/lsyscache.c
    unimplemented!()
}
unsafe fn op_hashjoinable(_opno: Oid, _inputtype: Oid) -> bool {
    // TODO(pg-port): real op_hashjoinable in utils/cache/lsyscache.c
    unimplemented!()
}

/// get_mergejoin_opfamilies() (utils/cache/lsyscache.c)
/// TODO(pg-port): real symbol lives in utils/cache/lsyscache.rs
unsafe fn get_mergejoin_opfamilies(_opno: Oid) -> *mut List {
    // TODO(pg-port): real get_mergejoin_opfamilies in utils/cache/lsyscache.c
    unimplemented!()
}

/// get_commutator() (utils/cache/lsyscache.c)
/// TODO(pg-port): real symbol lives in utils/cache/lsyscache.rs
unsafe fn get_commutator(_opno: Oid) -> Oid {
    // TODO(pg-port): real get_commutator in utils/cache/lsyscache.c
    unimplemented!()
}

/// OidIsValid macro equivalent
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

/// InvalidOid
const InvalidOid: Oid = 0;

/// BOOLOID
const BOOLOID: Oid = 16;

/// exprType() (nodes/nodeFuncs.c)
/// TODO(pg-port): real symbol lives in nodes/nodeFuncs.rs
unsafe fn exprType(_expr: *const Node) -> Oid {
    // TODO(pg-port): real exprType in nodes/nodeFuncs.c
    unimplemented!()
}

/// make_opclause() (nodes/makefuncs.c)
/// TODO(pg-port): real symbol lives in nodes/makefuncs.rs
unsafe fn make_opclause(
    _opno: Oid,
    _opresulttype: Oid,
    _opretset: bool,
    _leftop: *mut Expr,
    _rightop: *mut Expr,
    _opcollid: Oid,
    _inputcollid: Oid,
) -> *mut Expr {
    // TODO(pg-port): real make_opclause in nodes/makefuncs.c
    unimplemented!()
}

/// makeBoolConst() (nodes/makefuncs.c)
/// TODO(pg-port): real symbol lives in nodes/makefuncs.rs
unsafe fn makeBoolConst(_value: bool, _isnull: bool) -> *mut Node {
    // TODO(pg-port): real makeBoolConst in nodes/makefuncs.c
    unimplemented!()
}

/// eval_const_expressions() (optimizer/util/clauses.c)
/// TODO(pg-port): real symbol lives in optimizer/util/clauses.rs
unsafe fn eval_const_expressions(
    _root: *mut PlannerInfo,
    _node: *mut Node,
) -> *mut Node {
    // TODO(pg-port): real eval_const_expressions in optimizer/util/clauses.c
    unimplemented!()
}

/// DatumGetBool
#[inline]
unsafe fn DatumGetBool(d: Datum) -> bool {
    d != 0
}

/// find_forced_null_var() (optimizer/util/clauses.c or similar)
/// TODO(pg-port): real symbol lives in optimizer/util/clauses.rs
unsafe fn find_forced_null_var(_clause: *mut Node) -> *mut Var {
    // TODO(pg-port): real find_forced_null_var in optimizer/util/clauses.c
    unimplemented!()
}

/// add_nulling_relids() (rewrite/rewriteManip.c)
/// TODO(pg-port): real symbol lives in rewrite/rewriteManip.rs
unsafe fn add_nulling_relids(
    _node: *mut Node,
    _target_rels: Relids,
    _added_relids: Relids,
) -> *mut Node {
    // TODO(pg-port): real add_nulling_relids in rewrite/rewriteManip.c
    unimplemented!()
}

/// remove_nulling_relids() (rewrite/rewriteManip.c)
/// TODO(pg-port): real symbol lives in rewrite/rewriteManip.rs
unsafe fn remove_nulling_relids(
    _node: *mut Node,
    _removable_relids: Relids,
    _except_relids: Relids,
) -> *mut Node {
    // TODO(pg-port): real remove_nulling_relids in rewrite/rewriteManip.c
    unimplemented!()
}

/// LCS_asString() (nodes/lockoptions.c or similar)
/// TODO(pg-port): real symbol lives in nodes/lockoptions.rs
unsafe fn LCS_asString(_strength: c_int) -> *const c_char {
    // TODO(pg-port): real LCS_asString
    unimplemented!()
}

/// errcode() etc. -- assumed available from prelude
/// ereport macro uses these; stubs so the file compiles independently.

/// errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0; // TODO(pg-port): real value

/// errmsg stub -- real one is in prelude
unsafe fn errmsg_stub(_fmt: *const c_char) -> c_int { 0 }

/// enable_hashagg GUC
/// TODO(pg-port): real GUC lives in executor/nodeAgg.c / GUC table
unsafe fn enable_hashagg_get() -> bool {
    true // default
}

/// match_eclasses_to_foreign_key_col() (optimizer/path/equivclass.c)
/// TODO(pg-port): real symbol lives in optimizer/path/equivclass.rs
#[repr(C)]
pub struct EquivalenceClass {
    pub _opaque: u8,
}

unsafe fn match_eclasses_to_foreign_key_col(
    _root: *mut PlannerInfo,
    _fkinfo: *mut ForeignKeyOptInfo,
    _colno: c_int,
) -> *mut EquivalenceClass {
    // TODO(pg-port): real match_eclasses_to_foreign_key_col in optimizer/path/equivclass.c
    unimplemented!()
}

/// lookup_type_cache() (utils/cache/typcache.c)
/// TODO(pg-port): real symbol lives in utils/cache/typcache.rs
const TYPECACHE_HASH_PROC: c_int = 0x0004;
const TYPECACHE_EQ_OPR: c_int = 0x0010;

#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub hash_proc: Oid,
    pub eq_opr: Oid,
    // ... other fields omitted
}

unsafe fn lookup_type_cache(_typeid: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    // TODO(pg-port): real lookup_type_cache in utils/cache/typcache.c
    unimplemented!()
}

/// restriction_is_always_true / restriction_is_always_false -- forward decl
/// (both are pub and defined later in this file)

/// FirstLowInvalidHeapAttributeNumber (catalog/pg_attribute.h)
const FirstLowInvalidHeapAttributeNumber: c_int = -8;

/// RELKIND_PARTITIONED_TABLE (catalog/pg_class.h)
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;

/// PG_INT32_MAX
const PG_INT32_MAX: i32 = i32::MAX;

/*****************************************************************************
 *
 *   JOIN TREES
 *
 *****************************************************************************/

/*
 * add_base_rels_to_query
 *
 *   Scan the query's jointree and create baserel RelOptInfos for all
 *   the base relations (e.g., table, subquery, and function RTEs)
 *   appearing in the jointree.
 *
 * The initial invocation must pass root->parse->jointree as the value of
 * jtnode.  Internally, the function recurses through the jointree.
 *
 * At the end of this process, there should be one baserel RelOptInfo for
 * every non-join RTE that is used in the query.  Some of the baserels
 * may be appendrel parents, which will require additional "otherrel"
 * RelOptInfos for their member rels, but those are added later.
 */
pub unsafe fn add_base_rels_to_query(root: *mut PlannerInfo, jtnode: *mut Node) {
    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut crate::nodes::primnodes::RangeTblRef)).rtindex;
        let _ = build_simple_rel(root, varno as c_int, ptr::null_mut());
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut crate::nodes::primnodes::FromExpr;
        let mut l: *mut ListCell = ptr::null_mut();
        foreach!(l, (*f).fromlist, {
            add_base_rels_to_query(root, lfirst(crate::current_cell!(l)) as *mut Node);
        });
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut crate::nodes::primnodes::JoinExpr;
        add_base_rels_to_query(root, (*j).larg);
        add_base_rels_to_query(root, (*j).rarg);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as c_int);
    }
}

/*
 * add_other_rels_to_query
 *   create "otherrel" RelOptInfos for the children of appendrel baserels
 *
 * At the end of this process, there should be RelOptInfos for all relations
 * that will be scanned by the query.
 */
pub unsafe fn add_other_rels_to_query(root: *mut PlannerInfo) {
    let mut rti: c_int = 1;
    while rti < (*root).simple_rel_array_size {
        let rel = *(*root).simple_rel_array.add(rti as usize);
        let rte = *(*root).simple_rte_array.add(rti as usize);

        // there may be empty slots corresponding to non-baserel RTEs
        if rel.is_null() {
            rti += 1;
            continue;
        }

        // Ignore any "otherrels" that were already added.
        if (*rel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        // If it's marked as inheritable, look for children.
        if (*rte).inh {
            expand_inherited_rtentry(root, rel, rte, rti as Index);
        }
        rti += 1;
    }
}


/*****************************************************************************
 *
 *   TARGET LISTS
 *
 *****************************************************************************/

/*
 * build_base_rel_tlists
 *   Add targetlist entries for each var needed in the query's final tlist
 *   (and HAVING clause, if any) to the appropriate base relations.
 *
 * We mark such vars as needed by "relation 0" to ensure that they will
 * propagate up through all join plan steps.
 */
pub unsafe fn build_base_rel_tlists(root: *mut PlannerInfo, final_tlist: *mut List) {
    let tlist_vars = pull_var_clause(
        final_tlist as *mut Node,
        PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
    );

    if !tlist_vars.is_null() {
        add_vars_to_targetlist(root, tlist_vars, bms_make_singleton(0));
        list_free(tlist_vars);
    }

    /*
     * If there's a HAVING clause, we'll need the Vars it uses, too.  Note
     * that HAVING can contain Aggrefs but not WindowFuncs.
     */
    if !(*(*root).parse).havingQual.is_null() {
        let having_vars = pull_var_clause(
            (*(*root).parse).havingQual,
            PVC_RECURSE_AGGREGATES | PVC_INCLUDE_PLACEHOLDERS,
        );

        if !having_vars.is_null() {
            add_vars_to_targetlist(root, having_vars, bms_make_singleton(0));
            list_free(having_vars);
        }
    }
}

/*
 * add_vars_to_targetlist
 *   For each variable appearing in the list, add it to the owning
 *   relation's targetlist if not already present, and mark the variable
 *   as being needed for the indicated join (or for final output if
 *   where_needed includes "relation 0").
 *
 *   The list may also contain PlaceHolderVars.  These don't necessarily
 *   have a single owning relation; we keep their attr_needed info in
 *   root->placeholder_list instead.  Find or create the associated
 *   PlaceHolderInfo entry, and update its ph_needed.
 *
 *   See also add_vars_to_attr_needed.
 */
pub unsafe fn add_vars_to_targetlist(
    root: *mut PlannerInfo,
    vars: *mut List,
    where_needed: Relids,
) {
    let mut temp: *mut ListCell = ptr::null_mut();

    Assert!(!bms_is_empty(where_needed));

    foreach!(temp, vars, {
        let node = lfirst(crate::current_cell!(temp)) as *mut Node;

        if IsA!(node, T_Var) {
            let var = node as *mut Var;
            let rel = find_base_rel(root, (*var).varno as c_int);
            let mut attno = (*var).varattno as c_int;

            if bms_is_subset(where_needed, (*rel).relids) {
                continue;
            }
            Assert!(attno >= (*rel).min_attr as c_int && attno <= (*rel).max_attr as c_int);
            attno -= (*rel).min_attr as c_int;
            if (*(*rel).reltarget).exprs.is_null()
                || (*rel).attr_needed.add(attno as usize).read().is_null()
            {
                /*
                 * Variable not yet requested, so add to rel's targetlist.
                 *
                 * The value available at the rel's scan level has not been
                 * nulled by any outer join, so drop its varnullingrels.
                 * (We'll put those back as we climb up the join tree.)
                 */
                let var_copy = copyObject(var as *const Var);
                (*var_copy).varnullingrels = ptr::null_mut();
                (*(*rel).reltarget).exprs =
                    lappend((*(*rel).reltarget).exprs, var_copy as *mut _);
                // reltarget cost and width will be computed later
            }
            let slot = (*rel).attr_needed.add(attno as usize);
            *slot = bms_add_members(*slot, where_needed);
        } else if IsA!(node, T_PlaceHolderVar) {
            let phv = node as *mut PlaceHolderVar;
            let phinfo = find_placeholder_info(root, phv);
            (*phinfo).ph_needed = bms_add_members((*phinfo).ph_needed, where_needed);
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as c_int);
        }
    });
}

/*
 * add_vars_to_attr_needed
 *   This does a subset of what add_vars_to_targetlist does: it just
 *   updates attr_needed for Vars and ph_needed for PlaceHolderVars.
 *   We assume the Vars are already in their relations' targetlists.
 *
 *   This is used to rebuild attr_needed/ph_needed sets after removal
 *   of a useless outer join.  The removed join clause might have been
 *   the only upper-level use of some other relation's Var, in which
 *   case we can reduce that Var's attr_needed and thereby possibly
 *   open the door to further join removals.  But we can't tell that
 *   without tedious reconstruction of the attr_needed data.
 *
 *   Note that if a Var's attr_needed is successfully reduced to empty,
 *   it will still be in the relation's targetlist even though we do
 *   not really need the scan plan node to emit it.  The extra plan
 *   inefficiency seems tiny enough to not be worth spending planner
 *   cycles to get rid of it.
 */
pub unsafe fn add_vars_to_attr_needed(
    root: *mut PlannerInfo,
    vars: *mut List,
    where_needed: Relids,
) {
    let mut temp: *mut ListCell = ptr::null_mut();

    Assert!(!bms_is_empty(where_needed));

    foreach!(temp, vars, {
        let node = lfirst(crate::current_cell!(temp)) as *mut Node;

        if IsA!(node, T_Var) {
            let var = node as *mut Var;
            let rel = find_base_rel(root, (*var).varno as c_int);
            let mut attno = (*var).varattno as c_int;

            if bms_is_subset(where_needed, (*rel).relids) {
                continue;
            }
            Assert!(attno >= (*rel).min_attr as c_int && attno <= (*rel).max_attr as c_int);
            attno -= (*rel).min_attr as c_int;
            let slot = (*rel).attr_needed.add(attno as usize);
            *slot = bms_add_members(*slot, where_needed);
        } else if IsA!(node, T_PlaceHolderVar) {
            let phv = node as *mut PlaceHolderVar;
            let phinfo = find_placeholder_info(root, phv);
            (*phinfo).ph_needed = bms_add_members((*phinfo).ph_needed, where_needed);
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as c_int);
        }
    });
}

/*****************************************************************************
 *
 *   GROUP BY
 *
 *****************************************************************************/

/*
 * remove_useless_groupby_columns
 *       Remove any columns in the GROUP BY clause that are redundant due to
 *       being functionally dependent on other GROUP BY columns.
 *
 * Since some other DBMSes do not allow references to ungrouped columns, it's
 * not unusual to find all columns listed in GROUP BY even though listing the
 * primary-key columns, or columns of a unique constraint would be sufficient.
 * Deleting such excess columns avoids redundant sorting or hashing work, so
 * it's worth doing.
 *
 * Relcache invalidations will ensure that cached plans become invalidated
 * when the underlying supporting indexes are dropped or if a column's NOT
 * NULL attribute is removed.
 */
pub unsafe fn remove_useless_groupby_columns(root: *mut PlannerInfo) {
    let parse = (*root).parse;
    let mut groupbyattnos: *mut *mut Bitmapset;
    let mut surplusvars: *mut *mut Bitmapset;
    let mut tryremove = false;
    let mut lc: *mut ListCell = ptr::null_mut();
    let mut relid: c_int;

    // No chance to do anything if there are less than two GROUP BY items
    if list_length((*root).processed_groupClause) < 2 {
        return;
    }

    // Don't fiddle with the GROUP BY clause if the query has grouping sets
    if !(*parse).groupingSets.is_null() {
        return;
    }

    /*
     * Scan the GROUP BY clause to find GROUP BY items that are simple Vars.
     * Fill groupbyattnos[k] with a bitmapset of the column attnos of RTE k
     * that are GROUP BY items.
     */
    let rtable_len = list_length((*parse).rtable) as usize;
    groupbyattnos = palloc0(
        core::mem::size_of::<*mut Bitmapset>() * (rtable_len + 1),
    ) as *mut *mut Bitmapset;

    foreach!(lc, (*root).processed_groupClause, {
        let sgc = lfirst(crate::current_cell!(lc)) as *mut SortGroupClause;
        let tle = get_sortgroupclause_tle(sgc, (*parse).targetList);
        let var = (*tle).expr as *mut Var;

        /*
         * Ignore non-Vars and Vars from other query levels.
         *
         * XXX in principle, stable expressions containing Vars could also be
         * removed, if all the Vars are functionally dependent on other GROUP
         * BY items.  But it's not clear that such cases occur often enough to
         * be worth troubling over.
         */
        if !IsA!(var as *mut Node, T_Var) || (*var).varlevelsup > 0 {
            continue;
        }

        // OK, remember we have this Var
        relid = (*var).varno as c_int;
        Assert!(relid <= list_length((*parse).rtable) as c_int);

        /*
         * If this isn't the first column for this relation then we now have
         * multiple columns.  That means there might be some that can be
         * removed.
         */
        let slot = groupbyattnos.add(relid as usize);
        tryremove |= !bms_is_empty(*slot);
        *slot = bms_add_member(
            *slot,
            (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber,
        );
    });

    /*
     * No Vars or didn't find multiple Vars for any relation in the GROUP BY?
     * If so, nothing can be removed, so don't waste more effort trying.
     */
    if !tryremove {
        return;
    }

    /*
     * Consider each relation and see if it is possible to remove some of its
     * Vars from GROUP BY.  For simplicity and speed, we do the actual removal
     * in a separate pass.  Here, we just fill surplusvars[k] with a bitmapset
     * of the column attnos of RTE k that are removable GROUP BY items.
     */
    surplusvars = ptr::null_mut(); // don't allocate array unless required
    relid = 0;
    foreach!(lc, (*parse).rtable, {
        let rte = lfirst(crate::current_cell!(lc)) as *mut RangeTblEntry;
        let rel: *mut RelOptInfo;
        let mut relattnos: *mut Bitmapset;
        let mut best_keycolumns: *mut Bitmapset = ptr::null_mut();
        let mut best_nkeycolumns: i32 = PG_INT32_MAX;

        relid += 1;

        // Only plain relations could have primary-key constraints
        if (*rte).rtekind != RTE_RELATION {
            continue;
        }

        /*
         * We must skip inheritance parent tables as some of the child rels
         * may cause duplicate rows.  This cannot happen with partitioned
         * tables, however.
         */
        if (*rte).inh && (*rte).relkind != RELKIND_PARTITIONED_TABLE {
            continue;
        }

        // Nothing to do unless this rel has multiple Vars in GROUP BY
        relattnos = *groupbyattnos.add(relid as usize);
        if bms_membership(relattnos) != BMS_MULTIPLE {
            continue;
        }

        rel = *(*root).simple_rel_array.add(relid as usize);

        /*
         * Now check each index for this relation to see if there are any with
         * columns which are a proper subset of the grouping columns for this
         * relation.
         */
        let mut lc2: *mut ListCell = ptr::null_mut();
        foreach!(lc2, (*rel).indexlist, {
            let index = lfirst(crate::current_cell!(lc2)) as *mut IndexOptInfo;
            let mut ind_attnos: *mut Bitmapset = ptr::null_mut();
            let mut nulls_check_ok = true;

            /*
             * Skip any non-unique and deferrable indexes.  Predicate indexes
             * have not been checked yet, so we must skip those too as the
             * predOK check that's done later might fail.
             */
            if !(*index).unique || !(*index).immediate || !(*index).indpred.is_null() {
                continue;
            }

            // For simplicity, we currently don't support expression indexes
            if !(*index).indexprs.is_null() {
                continue;
            }

            let mut i = 0;
            while i < (*index).nkeycolumns {
                /*
                 * We must insist that the index columns are all defined NOT
                 * NULL otherwise duplicate NULLs could exist.  However, we
                 * can relax this check when the index is defined with NULLS
                 * NOT DISTINCT as there can only be 1 NULL row, therefore
                 * functional dependency on the unique columns is maintained,
                 * despite the NULL.
                 */
                if !(*index).nullsnotdistinct
                    && !bms_is_member(
                        *(*index).indexkeys.add(i as usize),
                        (*rel).notnullattnums,
                    )
                {
                    nulls_check_ok = false;
                    break;
                }

                ind_attnos = bms_add_member(
                    ind_attnos,
                    *(*index).indexkeys.add(i as usize)
                        - FirstLowInvalidHeapAttributeNumber,
                );
                i += 1;
            }

            if !nulls_check_ok {
                continue;
            }

            /*
             * Skip any indexes where the indexed columns aren't a proper
             * subset of the GROUP BY.
             */
            if bms_subset_compare(ind_attnos, relattnos) != BMS_SUBSET1 {
                continue;
            }

            /*
             * Record the attribute numbers from the index with the fewest
             * columns.  This allows the largest number of columns to be
             * removed from the GROUP BY clause.  In the future, we may wish
             * to consider using the narrowest set of columns and looking at
             * pg_statistic.stawidth as it might be better to use an index
             * with, say two INT4s, rather than, say, one long varlena column.
             */
            if (*index).nkeycolumns < best_nkeycolumns {
                best_keycolumns = ind_attnos;
                best_nkeycolumns = (*index).nkeycolumns;
            }
        });

        // Did we find a suitable index?
        if !bms_is_empty(best_keycolumns) {
            /*
             * To easily remember whether we've found anything to do, we don't
             * allocate the surplusvars[] array until we find something.
             */
            if surplusvars.is_null() {
                surplusvars = palloc0(
                    core::mem::size_of::<*mut Bitmapset>() * (rtable_len + 1),
                ) as *mut *mut Bitmapset;
            }

            // Remember the attnos of the removable columns
            *surplusvars.add(relid as usize) =
                bms_difference(relattnos, best_keycolumns);
        }
    });

    /*
     * If we found any surplus Vars, build a new GROUP BY clause without them.
     * (Note: this may leave some TLEs with unreferenced ressortgroupref
     * markings, but that's harmless.)
     */
    if !surplusvars.is_null() {
        let mut new_groupby: *mut List = NIL;

        foreach!(lc, (*root).processed_groupClause, {
            let sgc = lfirst(crate::current_cell!(lc)) as *mut SortGroupClause;
            let tle = get_sortgroupclause_tle(sgc, (*parse).targetList);
            let var = (*tle).expr as *mut Var;

            /*
             * New list must include non-Vars, outer Vars, and anything not
             * marked as surplus.
             */
            if !IsA!(var as *mut Node, T_Var)
                || (*var).varlevelsup > 0
                || !bms_is_member(
                    (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber,
                    *surplusvars.add((*var).varno as usize),
                )
            {
                new_groupby = lappend(new_groupby, sgc as *mut _);
            }
        });

        (*root).processed_groupClause = new_groupby;
    }
}

/*****************************************************************************
 *
 *   LATERAL REFERENCES
 *
 *****************************************************************************/

/*
 * find_lateral_references
 *   For each LATERAL subquery, extract all its references to Vars and
 *   PlaceHolderVars of the current query level, and make sure those values
 *   will be available for evaluation of the subquery.
 *
 * While later planning steps ensure that the Var/PHV source rels are on the
 * outside of nestloops relative to the LATERAL subquery, we also need to
 * ensure that the Vars/PHVs propagate up to the nestloop join level; this
 * means setting suitable where_needed values for them.
 *
 * Note that this only deals with lateral references in unflattened LATERAL
 * subqueries.  When we flatten a LATERAL subquery, its lateral references
 * become plain Vars in the parent query, but they may have to be wrapped in
 * PlaceHolderVars if they need to be forced NULL by outer joins that don't
 * also null the LATERAL subquery.  That's all handled elsewhere.
 *
 * This has to run before deconstruct_jointree, since it might result in
 * creation of PlaceHolderInfos.
 */
pub unsafe fn find_lateral_references(root: *mut PlannerInfo) {
    let mut rti: Index;

    // We need do nothing if the query contains no LATERAL RTEs
    if !(*root).hasLateralRTEs {
        return;
    }

    /*
     * Examine all baserels (the rel array has been set up by now).
     */
    rti = 1;
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);

        // there may be empty slots corresponding to non-baserel RTEs
        if brel.is_null() {
            rti += 1;
            continue;
        }

        Assert!((*brel).relid == rti); // sanity check on array

        /*
         * This bit is less obvious than it might look.  We ignore appendrel
         * otherrels and consider only their parent baserels.  In a case where
         * a LATERAL-containing UNION ALL subquery was pulled up, it is the
         * otherrel that is actually going to be in the plan.  However, we
         * want to mark all its lateral references as needed by the parent,
         * because it is the parent's relid that will be used for join
         * planning purposes.  And the parent's RTE will contain all the
         * lateral references we need to know, since the pulled-up member is
         * nothing but a copy of parts of the original RTE's subquery.  We
         * could visit the parent's children instead and transform their
         * references back to the parent's relid, but it would be much more
         * complicated for no real gain.  (Important here is that the child
         * members have not yet received any processing beyond being pulled
         * up.)  Similarly, in appendrels created by inheritance expansion,
         * it's sufficient to look at the parent relation.
         */

        // ignore RTEs that are "other rels"
        if (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        extract_lateral_references(root, brel, rti);
        rti += 1;
    }
}

unsafe fn extract_lateral_references(
    root: *mut PlannerInfo,
    brel: *mut RelOptInfo,
    rtindex: Index,
) {
    let rte = *(*root).simple_rte_array.add(rtindex as usize);
    let mut vars: *mut List;
    let mut newvars: *mut List;
    let where_needed: Relids;
    let mut lc: *mut ListCell = ptr::null_mut();

    // No cross-references are possible if it's not LATERAL
    if !(*rte).lateral {
        return;
    }

    // Fetch the appropriate variables
    if (*rte).rtekind == RTE_RELATION {
        vars = pull_vars_of_level((*rte).tablesample as *mut Node, 0);
    } else if (*rte).rtekind == RTE_SUBQUERY {
        vars = pull_vars_of_level((*rte).subquery as *mut Node, 1);
    } else if (*rte).rtekind == RTE_FUNCTION {
        vars = pull_vars_of_level((*rte).functions as *mut Node, 0);
    } else if (*rte).rtekind == RTE_TABLEFUNC {
        vars = pull_vars_of_level((*rte).tablefunc as *mut Node, 0);
    } else if (*rte).rtekind == RTE_VALUES {
        vars = pull_vars_of_level((*rte).values_lists as *mut Node, 0);
    } else {
        Assert!(false);
        return; // keep compiler quiet
    }

    if vars.is_null() {
        return; // nothing to do
    }

    // Copy each Var (or PlaceHolderVar) and adjust it to match our level
    newvars = NIL;
    foreach!(lc, vars, {
        let mut node = lfirst(crate::current_cell!(lc)) as *mut Node;

        node = copyObject(node as *const Node);
        if IsA!(node, T_Var) {
            let var = node as *mut Var;
            // Adjustment is easy since it's just one node
            (*var).varlevelsup = 0;
        } else if IsA!(node, T_PlaceHolderVar) {
            let phv = node as *mut PlaceHolderVar;
            let levelsup = (*phv).phlevelsup as c_int;

            // Have to work harder to adjust the contained expression too
            if levelsup != 0 {
                IncrementVarSublevelsUp(node, -levelsup, 0);
            }

            /*
             * If we pulled the PHV out of a subquery RTE, its expression
             * needs to be preprocessed.  subquery_planner() already did this
             * for level-zero PHVs in function and values RTEs, though.
             */
            if levelsup > 0 {
                (*phv).phexpr = preprocess_phv_expression(root, (*phv).phexpr);
            }
        } else {
            Assert!(false);
        }
        newvars = lappend(newvars, node as *mut _);
    });

    list_free(vars);

    /*
     * We mark the Vars as being "needed" at the LATERAL RTE.  This is a bit
     * of a cheat: a more formal approach would be to mark each one as needed
     * at the join of the LATERAL RTE with its source RTE.  But it will work,
     * and it's much less tedious than computing a separate where_needed for
     * each Var.
     */
    where_needed = bms_make_singleton(rtindex as c_int);

    /*
     * Push Vars into their source relations' targetlists, and PHVs into
     * root->placeholder_list.
     */
    add_vars_to_targetlist(root, newvars, where_needed);

    /*
     * Remember the lateral references for rebuild_lateral_attr_needed and
     * create_lateral_join_info.
     */
    (*brel).lateral_vars = newvars;
}

/*
 * rebuild_lateral_attr_needed
 *   Put back attr_needed bits for Vars/PHVs needed for lateral references.
 *
 * This is used to rebuild attr_needed/ph_needed sets after removal of a
 * useless outer join.  It should match what find_lateral_references did,
 * except that we call add_vars_to_attr_needed not add_vars_to_targetlist.
 */
pub unsafe fn rebuild_lateral_attr_needed(root: *mut PlannerInfo) {
    let mut rti: Index;

    // We need do nothing if the query contains no LATERAL RTEs
    if !(*root).hasLateralRTEs {
        return;
    }

    // Examine the same baserels that find_lateral_references did
    rti = 1;
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);
        let where_needed: Relids;

        if brel.is_null() {
            rti += 1;
            continue;
        }
        if (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        /*
         * We don't need to repeat all of extract_lateral_references, since it
         * kindly saved the extracted Vars/PHVs in lateral_vars.
         */
        if (*brel).lateral_vars.is_null() {
            rti += 1;
            continue;
        }

        where_needed = bms_make_singleton(rti as c_int);

        add_vars_to_attr_needed(root, (*brel).lateral_vars, where_needed);
        rti += 1;
    }
}

/*
 * create_lateral_join_info
 *   Fill in the per-base-relation direct_lateral_relids, lateral_relids
 *   and lateral_referencers sets.
 */
pub unsafe fn create_lateral_join_info(root: *mut PlannerInfo) {
    let mut found_laterals = false;
    let mut rti: Index;
    let mut lc: *mut ListCell = ptr::null_mut();

    // We need do nothing if the query contains no LATERAL RTEs
    if !(*root).hasLateralRTEs {
        return;
    }

    // We'll need to have the ph_eval_at values for PlaceHolderVars
    Assert!((*root).placeholdersFrozen);

    /*
     * Examine all baserels (the rel array has been set up by now).
     */
    rti = 1;
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);
        let mut lateral_relids: Relids = ptr::null_mut();

        // there may be empty slots corresponding to non-baserel RTEs
        if brel.is_null() {
            rti += 1;
            continue;
        }

        Assert!((*brel).relid == rti); // sanity check on array

        // ignore RTEs that are "other rels"
        if (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        // consider each laterally-referenced Var or PHV
        foreach!(lc, (*brel).lateral_vars, {
            let node = lfirst(crate::current_cell!(lc)) as *mut Node;

            if IsA!(node, T_Var) {
                let var = node as *mut Var;
                found_laterals = true;
                lateral_relids = bms_add_member(lateral_relids, (*var).varno as c_int);
            } else if IsA!(node, T_PlaceHolderVar) {
                let phv = node as *mut PlaceHolderVar;
                let phinfo = find_placeholder_info(root, phv);
                found_laterals = true;
                lateral_relids = bms_add_members(lateral_relids, (*phinfo).ph_eval_at);
            } else {
                Assert!(false);
            }
        });

        // We now have all the simple lateral refs from this rel
        (*brel).direct_lateral_relids = lateral_relids;
        (*brel).lateral_relids = bms_copy(lateral_relids);
        rti += 1;
    }

    /*
     * Now check for lateral references within PlaceHolderVars, and mark their
     * eval_at rels as having lateral references to the source rels.
     *
     * For a PHV that is due to be evaluated at a baserel, mark its source(s)
     * as direct lateral dependencies of the baserel (adding onto the ones
     * recorded above).  If it's due to be evaluated at a join, mark its
     * source(s) as indirect lateral dependencies of each baserel in the join,
     * ie put them into lateral_relids but not direct_lateral_relids.  This is
     * appropriate because we can't put any such baserel on the outside of a
     * join to one of the PHV's lateral dependencies, but on the other hand we
     * also can't yet join it directly to the dependency.
     */
    foreach!(lc, (*root).placeholder_list, {
        let phinfo = lfirst(crate::current_cell!(lc)) as *mut PlaceHolderInfo;
        let eval_at = (*phinfo).ph_eval_at;
        let lateral_refs: Relids;
        let mut varno: c_int = 0;

        if (*phinfo).ph_lateral.is_null() {
            continue; // PHV is uninteresting if no lateral refs
        }

        found_laterals = true;

        /*
         * Include only baserels not outer joins in the evaluation sites'
         * lateral relids.  This avoids problems when outer join order gets
         * rearranged, and it should still ensure that the lateral values are
         * available when needed.
         */
        lateral_refs = bms_intersect((*phinfo).ph_lateral, (*root).all_baserels);
        Assert!(!bms_is_empty(lateral_refs));

        if bms_get_singleton_member(eval_at, &mut varno) {
            // Evaluation site is a baserel
            let brel = find_base_rel(root, varno);
            (*brel).direct_lateral_relids =
                bms_add_members((*brel).direct_lateral_relids, lateral_refs);
            (*brel).lateral_relids =
                bms_add_members((*brel).lateral_relids, lateral_refs);
        } else {
            // Evaluation site is a join
            varno = -1;
            loop {
                varno = bms_next_member(eval_at, varno);
                if varno < 0 {
                    break;
                }
                let brel = find_base_rel_ignore_join(root, varno);
                if brel.is_null() {
                    continue; // ignore outer joins in eval_at
                }
                (*brel).lateral_relids =
                    bms_add_members((*brel).lateral_relids, lateral_refs);
            }
        }
    });

    /*
     * If we found no actual lateral references, we're done; but reset the
     * hasLateralRTEs flag to avoid useless work later.
     */
    if !found_laterals {
        (*root).hasLateralRTEs = false;
        return;
    }

    /*
     * Calculate the transitive closure of the lateral_relids sets, so that
     * they describe both direct and indirect lateral references.  If relation
     * X references Y laterally, and Y references Z laterally, then we will
     * have to scan X on the inside of a nestloop with Z, so for all intents
     * and purposes X is laterally dependent on Z too.
     *
     * This code is essentially Warshall's algorithm for transitive closure.
     * The outer loop considers each baserel, and propagates its lateral
     * dependencies to those baserels that have a lateral dependency on it.
     */
    rti = 1;
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);
        let outer_lateral_relids: Relids;
        let mut rti2: Index;

        if brel.is_null() || (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        // need not consider baserel further if it has no lateral refs
        outer_lateral_relids = (*brel).lateral_relids;
        if outer_lateral_relids.is_null() {
            rti += 1;
            continue;
        }

        // else scan all baserels
        rti2 = 1;
        while (rti2 as c_int) < (*root).simple_rel_array_size {
            let brel2 = *(*root).simple_rel_array.add(rti2 as usize);

            if brel2.is_null() || (*brel2).reloptkind != RELOPT_BASEREL {
                rti2 += 1;
                continue;
            }

            // if brel2 has lateral ref to brel, propagate brel's refs
            if bms_is_member(rti as c_int, (*brel2).lateral_relids) {
                (*brel2).lateral_relids =
                    bms_add_members((*brel2).lateral_relids, outer_lateral_relids);
            }
            rti2 += 1;
        }
        rti += 1;
    }

    /*
     * Now that we've identified all lateral references, mark each baserel
     * with the set of relids of rels that reference it laterally (possibly
     * indirectly) --- that is, the inverse mapping of lateral_relids.
     */
    rti = 1;
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);
        let lateral_relids: Relids;
        let mut rti2: c_int;

        if brel.is_null() || (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        // Nothing to do at rels with no lateral refs
        lateral_relids = (*brel).lateral_relids;
        if bms_is_empty(lateral_relids) {
            rti += 1;
            continue;
        }

        // No rel should have a lateral dependency on itself
        Assert!(!bms_is_member(rti as c_int, lateral_relids));

        // Mark this rel's referencees
        rti2 = -1;
        loop {
            rti2 = bms_next_member(lateral_relids, rti2);
            if rti2 < 0 {
                break;
            }
            let brel2 = *(*root).simple_rel_array.add(rti2 as usize);

            if brel2.is_null() {
                continue; // must be an OJ
            }

            Assert!((*brel2).reloptkind == RELOPT_BASEREL);
            (*brel2).lateral_referencers =
                bms_add_member((*brel2).lateral_referencers, rti as c_int);
        }
        rti += 1;
    }
}


/*****************************************************************************
 *
 *   JOIN TREE PROCESSING
 *
 *****************************************************************************/

/*
 * deconstruct_jointree
 *   Recursively scan the query's join tree for WHERE and JOIN/ON qual
 *   clauses, and add these to the appropriate restrictinfo and joininfo
 *   lists belonging to base RelOptInfos.  Also, add SpecialJoinInfo nodes
 *   to root->join_info_list for any outer joins appearing in the query tree.
 *   Return a "joinlist" data structure showing the join order decisions
 *   that need to be made by make_one_rel().
 *
 * The "joinlist" result is a list of items that are either RangeTblRef
 * jointree nodes or sub-joinlists.  All the items at the same level of
 * joinlist must be joined in an order to be determined by make_one_rel()
 * (note that legal orders may be constrained by SpecialJoinInfo nodes).
 * A sub-joinlist represents a subproblem to be planned separately. Currently
 * sub-joinlists arise only from FULL OUTER JOIN or when collapsing of
 * subproblems is stopped by join_collapse_limit or from_collapse_limit.
 */
pub unsafe fn deconstruct_jointree(root: *mut PlannerInfo) -> *mut List {
    let result: *mut List;
    let top_jdomain: *mut JoinDomain;
    let mut item_list: *mut List = NIL;
    let mut lc: *mut ListCell = ptr::null_mut();

    /*
     * After this point, no more PlaceHolderInfos may be made, because
     * make_outerjoininfo requires all active placeholders to be present in
     * root->placeholder_list while we crawl up the join tree.
     */
    (*root).placeholdersFrozen = true;

    // Fetch the already-created top-level join domain for the query
    top_jdomain = linitial_node!(JoinDomain, T_JoinDomain, (*root).join_domains);
    (*top_jdomain).jd_relids = ptr::null_mut(); // filled during deconstruct_recurse

    // Start recursion at top of jointree
    Assert!(
        !(*root).parse.is_null()
            && !(*(*root).parse).jointree.is_null()
            && IsA!((*(*root).parse).jointree as *mut Node, T_FromExpr)
    );

    // These are filled as we scan the jointree
    (*root).all_baserels = ptr::null_mut();
    (*root).outer_join_rels = ptr::null_mut();

    // Perform the initial scan of the jointree
    result = deconstruct_recurse(
        root,
        (*(*root).parse).jointree as *mut Node,
        top_jdomain,
        ptr::null_mut(),
        &mut item_list,
    );

    // Now we can form the value of all_query_rels, too
    (*root).all_query_rels =
        bms_union((*root).all_baserels, (*root).outer_join_rels);

    // ... which should match what we computed for the top join domain
    Assert!(bms_equal((*root).all_query_rels, (*top_jdomain).jd_relids));

    // Now scan all the jointree nodes again, and distribute quals
    foreach!(lc, item_list, {
        let jtitem = lfirst(crate::current_cell!(lc)) as *mut JoinTreeItem;
        deconstruct_distribute(root, jtitem);
    });

    /*
     * If there were any special joins then we may have some postponed LEFT
     * JOIN clauses to deal with.
     */
    if !(*root).join_info_list.is_null() {
        foreach!(lc, item_list, {
            let jtitem = lfirst(crate::current_cell!(lc)) as *mut JoinTreeItem;
            if !(*jtitem).oj_joinclauses.is_null() {
                deconstruct_distribute_oj_quals(root, item_list, jtitem);
            }
        });
    }

    // Don't need the JoinTreeItems any more
    list_free_deep(item_list);

    result
}

/*
 * deconstruct_recurse
 *   One recursion level of deconstruct_jointree's initial jointree scan.
 *
 * jtnode is the jointree node to examine, and parent_domain is the
 * enclosing join domain.  (We must add all base+OJ relids appearing
 * here or below to parent_domain.)  parent_jtitem is the JoinTreeItem
 * for the parent jointree node, or NULL at the top of the recursion.
 *
 * item_list is an in/out parameter: we add a JoinTreeItem struct to
 * that list for each jointree node, in depth-first traversal order.
 * (Hence, after each call, the last list item corresponds to its jtnode.)
 *
 * Return value is the appropriate joinlist for this jointree node.
 */
unsafe fn deconstruct_recurse(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    parent_domain: *mut JoinDomain,
    parent_jtitem: *mut JoinTreeItem,
    item_list: *mut *mut List,
) -> *mut List {
    let joinlist: *mut List;
    let jtitem: *mut JoinTreeItem;

    Assert!(!jtnode.is_null());

    // Make the new JoinTreeItem, but don't add it to item_list yet
    jtitem = palloc0_object_JoinTreeItem();
    (*jtitem).jtnode = jtnode;
    (*jtitem).jti_parent = parent_jtitem;

    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut crate::nodes::primnodes::RangeTblRef)).rtindex as c_int;

        // Fill all_baserels as we encounter baserel jointree nodes
        (*root).all_baserels = bms_add_member((*root).all_baserels, varno);
        // This node belongs to parent_domain
        (*jtitem).jdomain = parent_domain;
        (*parent_domain).jd_relids =
            bms_add_member((*parent_domain).jd_relids, varno);
        // qualscope is just the one RTE
        (*jtitem).qualscope = bms_make_singleton(varno);
        // A single baserel does not create an inner join
        (*jtitem).inner_join_rels = ptr::null_mut();
        joinlist = list_make1!(jtnode as *mut _);
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut crate::nodes::primnodes::FromExpr;
        let mut remaining: c_int;
        let mut l: *mut ListCell = ptr::null_mut();

        // This node belongs to parent_domain, as do its children
        (*jtitem).jdomain = parent_domain;

        /*
         * Recurse to handle child nodes, and compute output joinlist.  We
         * collapse subproblems into a single joinlist whenever the resulting
         * joinlist wouldn't exceed from_collapse_limit members.  Also, always
         * collapse one-element subproblems, since that won't lengthen the
         * joinlist anyway.
         */
        (*jtitem).qualscope = ptr::null_mut();
        (*jtitem).inner_join_rels = ptr::null_mut();
        let mut jl: *mut List = NIL;
        remaining = list_length((*f).fromlist) as c_int;
        foreach!(l, (*f).fromlist, {
            let sub_item: *mut JoinTreeItem;
            let sub_joinlist: *mut List;
            let sub_members: c_int;

            sub_joinlist = deconstruct_recurse(
                root,
                lfirst(crate::current_cell!(l)) as *mut Node,
                parent_domain,
                jtitem,
                item_list,
            );
            sub_item = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
            (*jtitem).qualscope =
                bms_add_members((*jtitem).qualscope, (*sub_item).qualscope);
            (*jtitem).inner_join_rels = (*sub_item).inner_join_rels;
            sub_members = list_length(sub_joinlist) as c_int;
            remaining -= 1;
            if sub_members <= 1
                || list_length(jl) as c_int + sub_members + remaining
                    <= from_collapse_limit
            {
                jl = list_concat(jl, sub_joinlist);
            } else {
                jl = lappend(jl, sub_joinlist as *mut _);
            }
        });

        /*
         * A FROM with more than one list element is an inner join subsuming
         * all below it, so we should report inner_join_rels = qualscope. If
         * there was exactly one element, we should (and already did) report
         * whatever its inner_join_rels were.  If there were no elements (is
         * that still possible?) the initialization before the loop fixed it.
         */
        if list_length((*f).fromlist) > 1 {
            (*jtitem).inner_join_rels = (*jtitem).qualscope;
        }
        joinlist = jl;
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut crate::nodes::primnodes::JoinExpr;
        let child_domain: *mut JoinDomain;
        let fj_domain: *mut JoinDomain;
        let left_item: *mut JoinTreeItem;
        let right_item: *mut JoinTreeItem;
        let leftjoinlist: *mut List;
        let rightjoinlist: *mut List;

        match (*j).jointype {
            JOIN_INNER => {
                // This node belongs to parent_domain, as do its children
                (*jtitem).jdomain = parent_domain;
                // Recurse
                let ljl = deconstruct_recurse(root, (*j).larg, parent_domain, jtitem, item_list);
                let li = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                let rjl = deconstruct_recurse(root, (*j).rarg, parent_domain, jtitem, item_list);
                let ri = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                // Compute qualscope etc
                (*jtitem).qualscope = bms_union((*li).qualscope, (*ri).qualscope);
                (*jtitem).inner_join_rels = (*jtitem).qualscope;
                (*jtitem).left_rels = (*li).qualscope;
                (*jtitem).right_rels = (*ri).qualscope;
                // Inner join adds no restrictions for quals
                (*jtitem).nonnullable_rels = ptr::null_mut();

                // compute joinlist below
                let ljl2 = ljl;
                let rjl2 = rjl;
                joinlist = compute_join_joinlist(j, ljl2, rjl2);
            }
            JOIN_LEFT | JOIN_ANTI => {
                // Make new join domain for my quals and the RHS
                let cd = makeNode!(JoinDomain, T_JoinDomain) as *mut JoinDomain;
                (*cd).jd_relids = ptr::null_mut(); // filled by recursion
                (*root).join_domains = lappend((*root).join_domains, cd as *mut _);
                (*jtitem).jdomain = cd;
                // Recurse
                let ljl = deconstruct_recurse(root, (*j).larg, parent_domain, jtitem, item_list);
                let li = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                let rjl = deconstruct_recurse(root, (*j).rarg, cd, jtitem, item_list);
                let ri = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                // Compute join domain contents, qualscope etc
                (*parent_domain).jd_relids =
                    bms_add_members((*parent_domain).jd_relids, (*cd).jd_relids);
                (*jtitem).qualscope =
                    bms_union((*li).qualscope, (*ri).qualscope);
                // caution: ANTI join derived from SEMI will lack rtindex
                if (*j).rtindex != 0 {
                    (*parent_domain).jd_relids =
                        bms_add_member((*parent_domain).jd_relids, (*j).rtindex as c_int);
                    (*jtitem).qualscope =
                        bms_add_member((*jtitem).qualscope, (*j).rtindex as c_int);
                    (*root).outer_join_rels =
                        bms_add_member((*root).outer_join_rels, (*j).rtindex as c_int);
                    mark_rels_nulled_by_join(root, (*j).rtindex as Index, (*ri).qualscope);
                }
                (*jtitem).inner_join_rels =
                    bms_union((*li).inner_join_rels, (*ri).inner_join_rels);
                (*jtitem).left_rels = (*li).qualscope;
                (*jtitem).right_rels = (*ri).qualscope;
                (*jtitem).nonnullable_rels = (*li).qualscope;

                let ljl2 = ljl;
                let rjl2 = rjl;
                joinlist = compute_join_joinlist(j, ljl2, rjl2);
            }
            JOIN_SEMI => {
                // This node belongs to parent_domain, as do its children
                (*jtitem).jdomain = parent_domain;
                // Recurse
                let ljl = deconstruct_recurse(root, (*j).larg, parent_domain, jtitem, item_list);
                let li = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                let rjl = deconstruct_recurse(root, (*j).rarg, parent_domain, jtitem, item_list);
                let ri = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                // Compute qualscope etc
                (*jtitem).qualscope =
                    bms_union((*li).qualscope, (*ri).qualscope);
                // SEMI join never has rtindex, so don't add to anything
                Assert!((*j).rtindex == 0);
                (*jtitem).inner_join_rels =
                    bms_union((*li).inner_join_rels, (*ri).inner_join_rels);
                (*jtitem).left_rels = (*li).qualscope;
                (*jtitem).right_rels = (*ri).qualscope;
                // Semi join adds no restrictions for quals
                (*jtitem).nonnullable_rels = ptr::null_mut();

                let ljl2 = ljl;
                let rjl2 = rjl;
                joinlist = compute_join_joinlist(j, ljl2, rjl2);
            }
            JOIN_FULL => {
                // The FULL JOIN's quals need their very own domain
                let fjd = makeNode!(JoinDomain, T_JoinDomain) as *mut JoinDomain;
                (*root).join_domains = lappend((*root).join_domains, fjd as *mut _);
                (*jtitem).jdomain = fjd;
                // Recurse, giving each side its own join domain
                let lcd = makeNode!(JoinDomain, T_JoinDomain) as *mut JoinDomain;
                (*lcd).jd_relids = ptr::null_mut(); // filled by recursion
                (*root).join_domains = lappend((*root).join_domains, lcd as *mut _);
                let ljl = deconstruct_recurse(root, (*j).larg, lcd, jtitem, item_list);
                let li = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                (*fjd).jd_relids = bms_copy((*lcd).jd_relids);
                let rcd = makeNode!(JoinDomain, T_JoinDomain) as *mut JoinDomain;
                (*rcd).jd_relids = ptr::null_mut(); // filled by recursion
                (*root).join_domains = lappend((*root).join_domains, rcd as *mut _);
                let rjl = deconstruct_recurse(root, (*j).rarg, rcd, jtitem, item_list);
                let ri = lfirst(llast(*item_list) as *const ListCell) as *mut JoinTreeItem;
                // Compute qualscope etc
                (*fjd).jd_relids =
                    bms_add_members((*fjd).jd_relids, (*rcd).jd_relids);
                (*parent_domain).jd_relids =
                    bms_add_members((*parent_domain).jd_relids, (*fjd).jd_relids);
                (*jtitem).qualscope =
                    bms_union((*li).qualscope, (*ri).qualscope);
                Assert!((*j).rtindex != 0);
                (*parent_domain).jd_relids =
                    bms_add_member((*parent_domain).jd_relids, (*j).rtindex as c_int);
                (*jtitem).qualscope =
                    bms_add_member((*jtitem).qualscope, (*j).rtindex as c_int);
                (*root).outer_join_rels =
                    bms_add_member((*root).outer_join_rels, (*j).rtindex as c_int);
                mark_rels_nulled_by_join(root, (*j).rtindex as Index, (*li).qualscope);
                mark_rels_nulled_by_join(root, (*j).rtindex as Index, (*ri).qualscope);
                (*jtitem).inner_join_rels =
                    bms_union((*li).inner_join_rels, (*ri).inner_join_rels);
                (*jtitem).left_rels = (*li).qualscope;
                (*jtitem).right_rels = (*ri).qualscope;
                // each side is both outer and inner
                (*jtitem).nonnullable_rels = (*jtitem).qualscope;

                // force the join order exactly at this node
                joinlist = list_make1!(list_make2!(ljl as *mut _, rjl as *mut _) as *mut _);
            }
            _ => {
                // JOIN_RIGHT was eliminated during reduce_outer_joins()
                elog!(ERROR, "unrecognized join type: {}", (*j).jointype as c_int);
                joinlist = NIL; // keep compiler quiet
            }
        }
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as c_int);
        joinlist = NIL; // keep compiler quiet
    }

    // Finally, we can add the new JoinTreeItem to item_list
    *item_list = lappend(*item_list, jtitem as *mut _);

    joinlist
}

/// Helper: compute the output joinlist for a non-FULL join.
/// Mirrors the logic in deconstruct_recurse after each JOIN_INNER/LEFT/SEMI branch.
unsafe fn compute_join_joinlist(
    j: *mut crate::nodes::primnodes::JoinExpr,
    leftjoinlist: *mut List,
    rightjoinlist: *mut List,
) -> *mut List {
    if ((*j).jointype as c_int) == JOIN_FULL as c_int {
        // force the join order exactly at this node
        return list_make1!(list_make2!(leftjoinlist as *mut _, rightjoinlist as *mut _) as *mut _);
    }
    if list_length(leftjoinlist) as c_int + list_length(rightjoinlist) as c_int
        <= join_collapse_limit
    {
        // OK to combine subproblems
        list_concat(leftjoinlist, rightjoinlist)
    } else {
        // can't combine, but needn't force join order above here
        let leftpart: *mut Node;
        let rightpart: *mut Node;

        // avoid creating useless 1-element sublists
        if list_length(leftjoinlist) == 1 {
            leftpart = linitial(leftjoinlist) as *mut Node;
        } else {
            leftpart = leftjoinlist as *mut Node;
        }
        if list_length(rightjoinlist) == 1 {
            rightpart = linitial(rightjoinlist) as *mut Node;
        } else {
            rightpart = rightjoinlist as *mut Node;
        }
        list_make2!(leftpart as *mut _, rightpart as *mut _)
    }
}

/*
 * deconstruct_distribute
 *   Process one jointree node in phase 2 of deconstruct_jointree processing.
 *
 * Distribute quals of the node to appropriate restriction and join lists.
 * In addition, entries will be added to root->join_info_list for outer joins.
 */
unsafe fn deconstruct_distribute(root: *mut PlannerInfo, jtitem: *mut JoinTreeItem) {
    let jtnode = (*jtitem).jtnode;

    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut crate::nodes::primnodes::RangeTblRef)).rtindex as c_int;

        // Deal with any securityQuals attached to the RTE
        if (*root).qual_security_level > 0 {
            process_security_barrier_quals(root, varno, jtitem);
        }
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut crate::nodes::primnodes::FromExpr;

        /*
         * Process any lateral-referencing quals that were postponed to this
         * level by children.
         */
        distribute_quals_to_rels(
            root,
            (*jtitem).lateral_clauses,
            jtitem,
            ptr::null_mut(),
            (*root).qual_security_level,
            (*jtitem).qualscope,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            true,
            false,
            false,
            ptr::null_mut(),
        );

        /*
         * Now process the top-level quals.
         */
        distribute_quals_to_rels(
            root,
            (*f).quals as *mut List,
            jtitem,
            ptr::null_mut(),
            (*root).qual_security_level,
            (*jtitem).qualscope,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            true,
            false,
            false,
            ptr::null_mut(),
        );
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut crate::nodes::primnodes::JoinExpr;
        let ojscope: Relids;
        let my_quals: *mut List;
        let sjinfo: *mut SpecialJoinInfo;
        let postponed_oj_qual_list: *mut *mut List;

        /*
         * Include lateral-referencing quals postponed from children in
         * my_quals, so that they'll be handled properly in
         * make_outerjoininfo.  (This is destructive to
         * jtitem->lateral_clauses, but we won't use that again.)
         */
        my_quals = list_concat((*jtitem).lateral_clauses, (*j).quals as *mut List);

        /*
         * For an OJ, form the SpecialJoinInfo now, so that we can pass it to
         * distribute_qual_to_rels.  We must compute its ojscope too.
         *
         * Semijoins are a bit of a hybrid: we build a SpecialJoinInfo, but we
         * want ojscope = NULL for distribute_qual_to_rels.
         */
        if (*j).jointype != JOIN_INNER {
            sjinfo = make_outerjoininfo(
                root,
                (*jtitem).left_rels,
                (*jtitem).right_rels,
                (*jtitem).inner_join_rels,
                (*j).jointype,
                (*j).rtindex as Index,
                my_quals,
            );
            (*jtitem).sjinfo = sjinfo;
            if (*j).jointype == JOIN_SEMI {
                ojscope = ptr::null_mut();
            } else {
                ojscope = bms_union((*sjinfo).min_lefthand, (*sjinfo).min_righthand);
            }
        } else {
            sjinfo = ptr::null_mut();
            ojscope = ptr::null_mut();
        }

        /*
         * If it's a left join with a join clause that is strict for the LHS,
         * then we need to postpone handling of any non-degenerate join
         * clauses, in case the join is able to commute with another left join
         * per identity 3.  (Degenerate clauses need not be postponed, since
         * they will drop down below this join anyway.)
         */
        let mut oj_qual_list_storage: *mut List = ptr::null_mut();
        if (*j).jointype == JOIN_LEFT && !sjinfo.is_null() && (*sjinfo).lhs_strict {
            postponed_oj_qual_list = &mut (*jtitem).oj_joinclauses;

            /*
             * Add back any commutable lower OJ relids that were removed from
             * min_lefthand or min_righthand, else the ojscope cross-check in
             * distribute_qual_to_rels will complain.  Since we are postponing
             * processing of non-degenerate clauses, this addition doesn't
             * affect anything except that cross-check.  Real clause
             * positioning decisions will be made later, when we revisit the
             * postponed clauses.
             */
            let ojscope2 = bms_add_members(ojscope, (*sjinfo).commute_below_l);
            let ojscope3 = bms_add_members(ojscope2, (*sjinfo).commute_below_r);
            // Process the JOIN's qual clauses
            distribute_quals_to_rels(
                root,
                my_quals,
                jtitem,
                sjinfo,
                (*root).qual_security_level,
                (*jtitem).qualscope,
                ojscope3,
                (*jtitem).nonnullable_rels,
                ptr::null_mut(), // incompatible_relids
                true,            // allow_equivalence
                false,
                false,           // not clones
                postponed_oj_qual_list,
            );
        } else {
            postponed_oj_qual_list = ptr::null_mut();
            // Process the JOIN's qual clauses
            distribute_quals_to_rels(
                root,
                my_quals,
                jtitem,
                sjinfo,
                (*root).qual_security_level,
                (*jtitem).qualscope,
                ojscope,
                (*jtitem).nonnullable_rels,
                ptr::null_mut(), // incompatible_relids
                true,            // allow_equivalence
                false,
                false,           // not clones
                postponed_oj_qual_list,
            );
        }

        // And add the SpecialJoinInfo to join_info_list
        if !sjinfo.is_null() {
            (*root).join_info_list = lappend((*root).join_info_list, sjinfo as *mut _);
        }
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as c_int);
    }
}

/*
 * process_security_barrier_quals
 *   Transfer security-barrier quals into relation's baserestrictinfo list.
 *
 * The rewriter put any relevant security-barrier conditions into the RTE's
 * securityQuals field, but it's now time to copy them into the rel's
 * baserestrictinfo.
 *
 * In inheritance cases, we only consider quals attached to the parent rel
 * here; they will be valid for all children too, so it's okay to consider
 * them for purposes like equivalence class creation.  Quals attached to
 * individual child rels will be dealt with during path creation.
 */
unsafe fn process_security_barrier_quals(
    root: *mut PlannerInfo,
    rti: c_int,
    jtitem: *mut JoinTreeItem,
) {
    let rte = *(*root).simple_rte_array.add(rti as usize);
    let mut security_level: Index = 0;
    let mut lc: *mut ListCell = ptr::null_mut();

    /*
     * Each element of the securityQuals list has been preprocessed into an
     * implicitly-ANDed list of clauses.  All the clauses in a given sublist
     * should get the same security level, but successive sublists get higher
     * levels.
     */
    foreach!(lc, (*rte).securityQuals, {
        let qualset = lfirst(crate::current_cell!(lc)) as *mut List;

        /*
         * We cheat to the extent of passing ojscope = qualscope rather than
         * its more logical value of NULL.  The only effect this has is to
         * force a Var-free qual to be evaluated at the rel rather than being
         * pushed up to top of tree, which we don't want.
         */
        distribute_quals_to_rels(
            root,
            qualset,
            jtitem,
            ptr::null_mut(),
            security_level,
            (*jtitem).qualscope,
            (*jtitem).qualscope,
            ptr::null_mut(),
            ptr::null_mut(),
            true,
            false,
            false, // not clones
            ptr::null_mut(),
        );
        security_level += 1;
    });

    // Assert that qual_security_level is higher than anything we just used
    Assert!(security_level <= (*root).qual_security_level);
}

/*
 * mark_rels_nulled_by_join
 *   Fill RelOptInfo.nulling_relids of baserels nulled by this outer join
 *
 * Inputs:
 *   ojrelid: RT index of the join RTE (must not be 0)
 *   lower_rels: the base+OJ Relids syntactically below nullable side of join
 */
unsafe fn mark_rels_nulled_by_join(
    root: *mut PlannerInfo,
    ojrelid: Index,
    lower_rels: Relids,
) {
    let mut relid: c_int = -1;

    loop {
        relid = bms_next_member(lower_rels, relid);
        if relid <= 0 {
            break;
        }
        let rel = *(*root).simple_rel_array.add(relid as usize);

        // ignore the RTE_GROUP RTE
        if relid == (*root).group_rtindex as c_int {
            continue;
        }

        if rel.is_null() {
            // must be an outer join
            Assert!(bms_is_member(relid, (*root).outer_join_rels));
            continue;
        }
        (*rel).nulling_relids = bms_add_member((*rel).nulling_relids, ojrelid as c_int);
    }
}

/*
 * make_outerjoininfo
 *   Build a SpecialJoinInfo for the current outer join
 *
 * Inputs:
 *   left_rels: the base+OJ Relids syntactically on outer side of join
 *   right_rels: the base+OJ Relids syntactically on inner side of join
 *   inner_join_rels: base+OJ Relids participating in inner joins below this one
 *   jointype: what it says (must always be LEFT, FULL, SEMI, or ANTI)
 *   ojrelid: RT index of the join RTE (0 for SEMI, which isn't in the RT list)
 *   clause: the outer join's join condition (in implicit-AND format)
 *
 * The node should eventually be appended to root->join_info_list, but we
 * do not do that here.
 *
 * Note: we assume that this function is invoked bottom-up, so that
 * root->join_info_list already contains entries for all outer joins that are
 * syntactically below this one.
 */
unsafe fn make_outerjoininfo(
    root: *mut PlannerInfo,
    left_rels: Relids,
    right_rels: Relids,
    inner_join_rels: Relids,
    jointype: JoinType,
    ojrelid: Index,
    clause: *mut List,
) -> *mut SpecialJoinInfo {
    let sjinfo = makeNode!(SpecialJoinInfo, T_SpecialJoinInfo) as *mut SpecialJoinInfo;
    let clause_relids: Relids;
    let strict_relids: Relids;
    let mut min_lefthand: Relids;
    let mut min_righthand: Relids;
    let mut commute_below_l: Relids = ptr::null_mut();
    let mut commute_below_r: Relids = ptr::null_mut();
    let mut l: *mut ListCell = ptr::null_mut();

    /*
     * We should not see RIGHT JOIN here because left/right were switched
     * earlier
     */
    Assert!(jointype != JOIN_INNER);
    // Note: JOIN_RIGHT check would go here but it was eliminated earlier

    /*
     * Presently the executor cannot support FOR [KEY] UPDATE/SHARE marking of
     * rels appearing on the nullable side of an outer join. (It's somewhat
     * unclear what that would mean, anyway: what should we mark when a result
     * row is generated from no element of the nullable relation?)	So,
     * complain if any nullable rel is FOR [KEY] UPDATE/SHARE.
     *
     * You might be wondering why this test isn't made far upstream in the
     * parser.  It's because the parser hasn't got enough info --- consider
     * FOR UPDATE applied to a view.  Only after rewriting and flattening do
     * we know whether the view contains an outer join.
     *
     * We use the original RowMarkClause list here; the PlanRowMark list would
     * list everything.
     */
    foreach!(l, (*(*root).parse).rowMarks, {
        let rc = lfirst(crate::current_cell!(l)) as *mut RowMarkClause;

        if bms_is_member((*rc).rti as c_int, right_rels)
            || (jointype == JOIN_FULL && bms_is_member((*rc).rti as c_int, left_rels))
        {
            ereport!(
                ERROR,
                // translator: %s is a SQL row locking clause such as FOR UPDATE
                errmsg!(
                    "{} cannot be applied to the nullable side of an outer join",
                    ::std::ffi::CStr::from_ptr(LCS_asString((*rc).strength as c_int))
                        .to_string_lossy()
                )
            );
        }
    });

    (*sjinfo).syn_lefthand = left_rels;
    (*sjinfo).syn_righthand = right_rels;
    (*sjinfo).jointype = jointype;
    (*sjinfo).ojrelid = ojrelid;
    // these fields may get added to later:
    (*sjinfo).commute_above_l = ptr::null_mut();
    (*sjinfo).commute_above_r = ptr::null_mut();
    (*sjinfo).commute_below_l = ptr::null_mut();
    (*sjinfo).commute_below_r = ptr::null_mut();

    compute_semijoin_info(root, sjinfo, clause);

    // If it's a full join, no need to be very smart
    if jointype == JOIN_FULL {
        (*sjinfo).min_lefthand = bms_copy(left_rels);
        (*sjinfo).min_righthand = bms_copy(right_rels);
        (*sjinfo).lhs_strict = false; // don't care about this
        return sjinfo;
    }

    /*
     * Retrieve all relids mentioned within the join clause.
     */
    clause_relids = pull_varnos(root, clause as *mut Node);

    /*
     * For which relids is the clause strict, ie, it cannot succeed if the
     * rel's columns are all NULL?
     */
    strict_relids = find_nonnullable_rels(clause as *mut Node);

    // Remember whether the clause is strict for any LHS relations
    (*sjinfo).lhs_strict = bms_overlap(strict_relids, left_rels);

    /*
     * Required LHS always includes the LHS rels mentioned in the clause. We
     * may have to add more rels based on lower outer joins; see below.
     */
    min_lefthand = bms_intersect(clause_relids, left_rels);

    /*
     * Similarly for required RHS.  But here, we must also include any lower
     * inner joins, to ensure we don't try to commute with any of them.
     */
    min_righthand = bms_int_members(
        bms_union(clause_relids, inner_join_rels),
        right_rels,
    );

    /*
     * Now check previous outer joins for ordering restrictions.
     *
     * commute_below_l and commute_below_r accumulate the relids of lower
     * outer joins that we think this one can commute with.  These decisions
     * are just tentative within this loop, since we might find an
     * intermediate outer join that prevents commutation.  Surviving relids
     * will get merged into the SpecialJoinInfo structs afterwards.
     */
    commute_below_l = ptr::null_mut();
    commute_below_r = ptr::null_mut();
    foreach!(l, (*root).join_info_list, {
        let otherinfo = lfirst(crate::current_cell!(l)) as *mut SpecialJoinInfo;
        let have_unsafe_phvs: bool;

        /*
         * A full join is an optimization barrier: we can't associate into or
         * out of it.  Hence, if it overlaps either LHS or RHS of the current
         * rel, expand that side's min relset to cover the whole full join.
         */
        if (*otherinfo).jointype == JOIN_FULL {
            Assert!((*otherinfo).ojrelid != 0);
            if bms_overlap(left_rels, (*otherinfo).syn_lefthand)
                || bms_overlap(left_rels, (*otherinfo).syn_righthand)
            {
                min_lefthand = bms_add_members(min_lefthand, (*otherinfo).syn_lefthand);
                min_lefthand = bms_add_members(min_lefthand, (*otherinfo).syn_righthand);
                min_lefthand = bms_add_member(min_lefthand, (*otherinfo).ojrelid as c_int);
            }
            if bms_overlap(right_rels, (*otherinfo).syn_lefthand)
                || bms_overlap(right_rels, (*otherinfo).syn_righthand)
            {
                min_righthand = bms_add_members(min_righthand, (*otherinfo).syn_lefthand);
                min_righthand = bms_add_members(min_righthand, (*otherinfo).syn_righthand);
                min_righthand = bms_add_member(min_righthand, (*otherinfo).ojrelid as c_int);
            }
            // Needn't do anything else with the full join
            continue;
        }

        /*
         * If our join condition contains any PlaceHolderVars that need to be
         * evaluated above the lower OJ, then we can't commute with it.
         */
        if (*otherinfo).ojrelid != 0 {
            have_unsafe_phvs = contain_placeholder_references_to(
                root,
                clause as *mut Node,
                (*otherinfo).ojrelid,
            );
        } else {
            have_unsafe_phvs = false;
        }

        /*
         * For a lower OJ in our LHS, if our join condition uses the lower
         * join's RHS and is not strict for that rel, we must preserve the
         * ordering of the two OJs, so add lower OJ's full syntactic relset to
         * min_lefthand.  (We must use its full syntactic relset, not just its
         * min_lefthand + min_righthand.  This is because there might be other
         * OJs below this one that this one can commute with, but we cannot
         * commute with them if we don't with this one.)  Also, if we have
         * unsafe PHVs or the current join is a semijoin or antijoin, we must
         * preserve ordering regardless of strictness.
         *
         * Note: I believe we have to insist on being strict for at least one
         * rel in the lower OJ's min_righthand, not its whole syn_righthand.
         *
         * When we don't need to preserve ordering, check to see if outer join
         * identity 3 applies, and if so, remove the lower OJ's ojrelid from
         * our min_lefthand so that commutation is allowed.
         */
        if bms_overlap(left_rels, (*otherinfo).syn_righthand) {
            if bms_overlap(clause_relids, (*otherinfo).syn_righthand)
                && (have_unsafe_phvs
                    || jointype == JOIN_SEMI
                    || jointype == JOIN_ANTI
                    || !bms_overlap(strict_relids, (*otherinfo).min_righthand))
            {
                // Preserve ordering
                min_lefthand = bms_add_members(min_lefthand, (*otherinfo).syn_lefthand);
                min_lefthand = bms_add_members(min_lefthand, (*otherinfo).syn_righthand);
                if (*otherinfo).ojrelid != 0 {
                    min_lefthand =
                        bms_add_member(min_lefthand, (*otherinfo).ojrelid as c_int);
                }
            } else if jointype == JOIN_LEFT
                && (*otherinfo).jointype == JOIN_LEFT
                && bms_overlap(strict_relids, (*otherinfo).min_righthand)
                && !bms_overlap(clause_relids, (*otherinfo).syn_lefthand)
            {
                // Identity 3 applies, so remove the ordering restriction
                min_lefthand =
                    bms_del_member(min_lefthand, (*otherinfo).ojrelid as c_int);
                // Record the (still tentative) commutability relationship
                commute_below_l =
                    bms_add_member(commute_below_l, (*otherinfo).ojrelid as c_int);
            }
        }

        /*
         * For a lower OJ in our RHS, if our join condition does not use the
         * lower join's RHS and the lower OJ's join condition is strict, we
         * can interchange the ordering of the two OJs; otherwise we must add
         * the lower OJ's full syntactic relset to min_righthand.
         *
         * Also, if our join condition does not use the lower join's LHS
         * either, force the ordering to be preserved.  Otherwise we can end
         * up with SpecialJoinInfos with identical min_righthands, which can
         * confuse join_is_legal (see discussion in backend/optimizer/README).
         *
         * Also, we must preserve ordering anyway if we have unsafe PHVs, or
         * if either this join or the lower OJ is a semijoin or antijoin.
         *
         * When we don't need to preserve ordering, check to see if outer join
         * identity 3 applies, and if so, remove the lower OJ's ojrelid from
         * our min_righthand so that commutation is allowed.
         */
        if bms_overlap(right_rels, (*otherinfo).syn_righthand) {
            if bms_overlap(clause_relids, (*otherinfo).syn_righthand)
                || !bms_overlap(clause_relids, (*otherinfo).min_lefthand)
                || have_unsafe_phvs
                || jointype == JOIN_SEMI
                || jointype == JOIN_ANTI
                || (*otherinfo).jointype == JOIN_SEMI
                || (*otherinfo).jointype == JOIN_ANTI
                || !(*otherinfo).lhs_strict
            {
                // Preserve ordering
                min_righthand =
                    bms_add_members(min_righthand, (*otherinfo).syn_lefthand);
                min_righthand =
                    bms_add_members(min_righthand, (*otherinfo).syn_righthand);
                if (*otherinfo).ojrelid != 0 {
                    min_righthand =
                        bms_add_member(min_righthand, (*otherinfo).ojrelid as c_int);
                }
            } else if jointype == JOIN_LEFT
                && (*otherinfo).jointype == JOIN_LEFT
                && (*otherinfo).lhs_strict
            {
                // Identity 3 applies, so remove the ordering restriction
                min_righthand =
                    bms_del_member(min_righthand, (*otherinfo).ojrelid as c_int);
                // Record the (still tentative) commutability relationship
                commute_below_r =
                    bms_add_member(commute_below_r, (*otherinfo).ojrelid as c_int);
            }
        }
    });

    /*
     * Examine PlaceHolderVars.  If a PHV is supposed to be evaluated within
     * this join's nullable side, then ensure that min_righthand contains the
     * full eval_at set of the PHV.  This ensures that the PHV actually can be
     * evaluated within the RHS.  Note that this works only because we should
     * already have determined the final eval_at level for any PHV
     * syntactically within this join.
     */
    foreach!(l, (*root).placeholder_list, {
        let phinfo = lfirst(crate::current_cell!(l)) as *mut PlaceHolderInfo;
        let ph_syn_level = (*(*phinfo).ph_var).phrels;

        // Ignore placeholder if it didn't syntactically come from RHS
        if !bms_is_subset(ph_syn_level, right_rels) {
            continue;
        }

        // Else, prevent join from being formed before we eval the PHV
        min_righthand = bms_add_members(min_righthand, (*phinfo).ph_eval_at);
    });

    /*
     * If we found nothing to put in min_lefthand, punt and make it the full
     * LHS, to avoid having an empty min_lefthand which will confuse later
     * processing. (We don't try to be smart about such cases, just correct.)
     * Likewise for min_righthand.
     */
    if bms_is_empty(min_lefthand) {
        min_lefthand = bms_copy(left_rels);
    }
    if bms_is_empty(min_righthand) {
        min_righthand = bms_copy(right_rels);
    }

    // Now they'd better be nonempty
    Assert!(!bms_is_empty(min_lefthand));
    Assert!(!bms_is_empty(min_righthand));
    // Shouldn't overlap either
    Assert!(!bms_overlap(min_lefthand, min_righthand));

    (*sjinfo).min_lefthand = min_lefthand;
    (*sjinfo).min_righthand = min_righthand;

    /*
     * Now that we've identified the correct min_lefthand and min_righthand,
     * any commute_below_l or commute_below_r relids that have not gotten
     * added back into those sets (due to intervening outer joins) are indeed
     * commutable with this one.
     *
     * First, delete any subsequently-added-back relids (this is easier than
     * maintaining commute_below_l/r precisely through all the above).
     */
    commute_below_l = bms_del_members(commute_below_l, min_lefthand);
    commute_below_r = bms_del_members(commute_below_r, min_righthand);

    // Anything left?
    if !commute_below_l.is_null() || !commute_below_r.is_null() {
        // Yup, so we must update the derived data in the SpecialJoinInfos
        (*sjinfo).commute_below_l = commute_below_l;
        (*sjinfo).commute_below_r = commute_below_r;
        foreach!(l, (*root).join_info_list, {
            let otherinfo = lfirst(crate::current_cell!(l)) as *mut SpecialJoinInfo;

            if bms_is_member((*otherinfo).ojrelid as c_int, commute_below_l) {
                (*otherinfo).commute_above_l =
                    bms_add_member((*otherinfo).commute_above_l, ojrelid as c_int);
            } else if bms_is_member((*otherinfo).ojrelid as c_int, commute_below_r) {
                (*otherinfo).commute_above_r =
                    bms_add_member((*otherinfo).commute_above_r, ojrelid as c_int);
            }
        });
    }

    sjinfo
}

/*
 * compute_semijoin_info
 *   Fill semijoin-related fields of a new SpecialJoinInfo
 *
 * Note: this relies on only the jointype and syn_righthand fields of the
 * SpecialJoinInfo; the rest may not be set yet.
 */
unsafe fn compute_semijoin_info(
    root: *mut PlannerInfo,
    sjinfo: *mut SpecialJoinInfo,
    clause: *mut List,
) {
    let mut semi_operators: *mut List = NIL;
    let mut semi_rhs_exprs: *mut List = NIL;
    let mut all_btree: bool;
    let mut all_hash: bool;
    let mut lc: *mut ListCell = ptr::null_mut();

    // Initialize semijoin-related fields in case we can't unique-ify
    (*sjinfo).semi_can_btree = false;
    (*sjinfo).semi_can_hash = false;
    (*sjinfo).semi_operators = NIL;
    (*sjinfo).semi_rhs_exprs = NIL;

    // Nothing more to do if it's not a semijoin
    if (*sjinfo).jointype != JOIN_SEMI {
        return;
    }

    /*
     * Look to see whether the semijoin's join quals consist of AND'ed
     * equality operators, with (only) RHS variables on only one side of each
     * one.  If so, we can figure out how to enforce uniqueness for the RHS.
     *
     * Note that the input clause list is the list of quals that are
     * *syntactically* associated with the semijoin, which in practice means
     * the synthesized comparison list for an IN or the WHERE of an EXISTS.
     * Particularly in the latter case, it might contain clauses that aren't
     * *semantically* associated with the join, but refer to just one side or
     * the other.  We can ignore such clauses here, as they will just drop
     * down to be processed within one side or the other.  (It is okay to
     * consider only the syntactically-associated clauses here because for a
     * semijoin, no higher-level quals could refer to the RHS, and so there
     * can be no other quals that are semantically associated with this join.
     * We do things this way because it is useful to have the set of potential
     * unique-ification expressions before we can extract the list of quals
     * that are actually semantically associated with the particular join.)
     *
     * Note that the semi_operators list consists of the joinqual operators
     * themselves (but commuted if needed to put the RHS value on the right).
     * These could be cross-type operators, in which case the operator
     * actually needed for uniqueness is a related single-type operator. We
     * assume here that that operator will be available from the btree or hash
     * opclass when the time comes ... if not, create_unique_plan() will fail.
     */
    all_btree = true;
    all_hash = enable_hashagg_get(); // don't consider hash if not enabled
    foreach!(lc, clause, {
        let op = lfirst(crate::current_cell!(lc)) as *mut OpExpr;
        let mut opno: Oid;
        let left_expr: *mut Node;
        let right_expr: *mut Node;
        let left_varnos: Relids;
        let right_varnos: Relids;
        let all_varnos: Relids;
        let opinputtype: Oid;

        // Is it a binary opclause?
        if !IsA!(op as *mut Node, T_OpExpr) || list_length((*op).args) != 2 {
            // No, but does it reference both sides?
            let av = pull_varnos(root, op as *mut Node);
            if !bms_overlap(av, (*sjinfo).syn_righthand)
                || bms_is_subset(av, (*sjinfo).syn_righthand)
            {
                /*
                 * Clause refers to only one rel, so ignore it --- unless it
                 * contains volatile functions, in which case we'd better
                 * punt.
                 */
                if contain_volatile_functions(op as *mut Node) {
                    return;
                }
                continue;
            }
            // Non-operator clause referencing both sides, must punt
            return;
        }

        // Extract data from binary opclause
        opno = (*op).opno;
        left_expr = linitial((*op).args) as *mut Node;
        right_expr = crate::nodes::pg_list::lsecond((*op).args) as *mut Node;
        left_varnos = pull_varnos(root, left_expr);
        right_varnos = pull_varnos(root, right_expr);
        all_varnos = bms_union(left_varnos, right_varnos);
        opinputtype = exprType(left_expr as *const Node);

        // Does it reference both sides?
        if !bms_overlap(all_varnos, (*sjinfo).syn_righthand)
            || bms_is_subset(all_varnos, (*sjinfo).syn_righthand)
        {
            /*
             * Clause refers to only one rel, so ignore it --- unless it
             * contains volatile functions, in which case we'd better punt.
             */
            if contain_volatile_functions(op as *mut Node) {
                return;
            }
            continue;
        }

        // check rel membership of arguments
        let mut right_expr2 = right_expr;
        if !bms_is_empty(right_varnos)
            && bms_is_subset(right_varnos, (*sjinfo).syn_righthand)
            && !bms_overlap(left_varnos, (*sjinfo).syn_righthand)
        {
            // typical case, right_expr is RHS variable
        } else if !bms_is_empty(left_varnos)
            && bms_is_subset(left_varnos, (*sjinfo).syn_righthand)
            && !bms_overlap(right_varnos, (*sjinfo).syn_righthand)
        {
            // flipped case, left_expr is RHS variable
            opno = get_commutator(opno);
            if !OidIsValid(opno) {
                return;
            }
            right_expr2 = left_expr;
        } else {
            // mixed membership of args, punt
            return;
        }

        // all operators must be btree equality or hash equality
        if all_btree {
            // oprcanmerge is considered a hint...
            if !op_mergejoinable(opno, opinputtype)
                || (get_mergejoin_opfamilies(opno)).is_null()
            {
                all_btree = false;
            }
        }
        if all_hash {
            // ... but oprcanhash had better be correct
            if !op_hashjoinable(opno, opinputtype) {
                all_hash = false;
            }
        }
        if !all_btree && !all_hash {
            return;
        }

        // so far so good, keep building lists
        semi_operators = lappend_oid(semi_operators, opno);
        semi_rhs_exprs = lappend(
            semi_rhs_exprs,
            copyObject(right_expr2 as *const Node) as *mut _,
        );
    });

    // Punt if we didn't find at least one column to unique-ify
    if semi_rhs_exprs.is_null() {
        return;
    }

    /*
     * The expressions we'd need to unique-ify mustn't be volatile.
     */
    if contain_volatile_functions(semi_rhs_exprs as *mut Node) {
        return;
    }

    /*
     * If we get here, we can unique-ify the semijoin's RHS using at least one
     * of sorting and hashing.  Save the information about how to do that.
     */
    (*sjinfo).semi_can_btree = all_btree;
    (*sjinfo).semi_can_hash = all_hash;
    (*sjinfo).semi_operators = semi_operators;
    (*sjinfo).semi_rhs_exprs = semi_rhs_exprs;
}

/*
 * deconstruct_distribute_oj_quals
 *   Adjust LEFT JOIN quals to be suitable for commuted-left-join cases,
 *   then push them into the joinqual lists and EquivalenceClass structures.
 *
 * This runs immediately after we've completed the deconstruct_distribute scan.
 * jtitems contains all the JoinTreeItems (in depth-first order), and jtitem
 * is one that has postponed oj_joinclauses to deal with.
 */
unsafe fn deconstruct_distribute_oj_quals(
    root: *mut PlannerInfo,
    jtitems: *mut List,
    jtitem: *mut JoinTreeItem,
) {
    let sjinfo = (*jtitem).sjinfo;
    let qualscope: Relids;
    let ojscope: Relids;
    let nonnullable_rels: Relids;

    // Recompute syntactic and semantic scopes of this left join
    let qs = bms_union((*sjinfo).syn_lefthand, (*sjinfo).syn_righthand);
    let qualscope = bms_add_member(qs, (*sjinfo).ojrelid as c_int);
    let ojscope = bms_union((*sjinfo).min_lefthand, (*sjinfo).min_righthand);
    let nonnullable_rels = (*sjinfo).syn_lefthand;

    /*
     * If this join can commute with any other ones per outer-join identity 3,
     * and it is the one providing the join clause with flexible semantics,
     * then we have to generate variants of the join clause with different
     * nullingrels labeling.  Otherwise, just push out the postponed clause
     * as-is.
     */
    Assert!((*sjinfo).lhs_strict); // else we shouldn't be here
    if !(*sjinfo).commute_above_r.is_null() || !(*sjinfo).commute_below_l.is_null() {
        let joins_above: Relids;
        let joins_below: Relids;
        let mut incompatible_joins: Relids;
        let mut joins_so_far: Relids = ptr::null_mut();
        let mut quals: *mut List;
        let save_last_rinfo_serial: c_int;
        let mut lc: *mut ListCell = ptr::null_mut();

        // Identify the outer joins this one commutes with
        joins_above = (*sjinfo).commute_above_r;
        joins_below = (*sjinfo).commute_below_l;

        /*
         * Generate qual variants with different sets of nullingrels bits.
         *
         * We only need bit-sets that correspond to the successively less
         * deeply syntactically-nested subsets of this join and its
         * commutators.  That's true first because obviously only those forms
         * of the Vars and PHVs could appear elsewhere in the query, and
         * second because the outer join identities do not provide a way to
         * re-order such joins in a way that would require different marking.
         * (That is, while the current join may commute with several others,
         * none of those others can commute with each other.)  To visit the
         * interesting joins in syntactic nesting order, we rely on the
         * jtitems list to be ordered that way.
         *
         * We first strip out all the nullingrels bits corresponding to
         * commuting joins below this one, and then successively put them back
         * as we crawl up the join stack.
         */
        quals = (*jtitem).oj_joinclauses;
        if !bms_is_empty(joins_below) {
            quals = remove_nulling_relids(quals as *mut Node, joins_below, ptr::null_mut())
                as *mut List;
        }

        /*
         * We'll need to mark the lower versions of the quals as not safe to
         * apply above not-yet-processed joins of the stack.  This prevents
         * possibly applying a cloned qual at the wrong join level.
         */
        incompatible_joins = bms_union(joins_below, joins_above);
        incompatible_joins =
            bms_add_member(incompatible_joins, (*sjinfo).ojrelid as c_int);

        /*
         * Each time we produce RestrictInfo(s) from these quals, reset the
         * last_rinfo_serial counter, so that the RestrictInfos for the "same"
         * qual condition get identical serial numbers.  (This relies on the
         * fact that we're not changing the qual list in any way that'd affect
         * the number of RestrictInfos built from it.) This'll allow us to
         * detect duplicative qual usage later.
         */
        save_last_rinfo_serial = (*root).last_rinfo_serial;

        joins_so_far = ptr::null_mut();
        foreach!(lc, jtitems, {
            let otherjtitem = lfirst(crate::current_cell!(lc)) as *mut JoinTreeItem;
            let othersj = (*otherjtitem).sjinfo;
            let mut below_sjinfo = false;
            let mut above_sjinfo = false;
            let this_qualscope: Relids;
            let this_ojscope: Relids;
            let allow_equivalence: bool;
            let has_clone: bool;
            let is_clone: bool;

            if othersj.is_null() {
                continue; // not an outer-join item, ignore
            }

            if bms_is_member((*othersj).ojrelid as c_int, joins_below) {
                // othersj commutes with sjinfo from below left
                below_sjinfo = true;
            } else if othersj == sjinfo {
                // found our join in syntactic order
                Assert!(bms_equal(joins_so_far, joins_below));
            } else if bms_is_member((*othersj).ojrelid as c_int, joins_above) {
                // othersj commutes with sjinfo from above
                above_sjinfo = true;
            } else {
                // othersj is not relevant, ignore
                continue;
            }

            // Reset serial counter for this version of the quals
            (*root).last_rinfo_serial = save_last_rinfo_serial;

            /*
             * When we are looking at joins above sjinfo, we are envisioning
             * pushing sjinfo to above othersj, so add othersj's nulling bit
             * before distributing the quals.  We should add it to Vars coming
             * from the current join's LHS: we want to transform the second
             * form of OJ identity 3 to the first form, in which Vars of
             * relation B will appear nulled by the syntactically-upper OJ
             * within the Pbc clause, but those of relation C will not.  (In
             * the notation used by optimizer/README, we're converting a qual
             * of the form Pbc to Pb*c.)  Of course, we must also remove that
             * bit from the incompatible_joins value, else we'll make a qual
             * that can't be placed anywhere.
             */
            if above_sjinfo {
                quals = add_nulling_relids(
                    quals as *mut Node,
                    (*sjinfo).syn_lefthand,
                    bms_make_singleton((*othersj).ojrelid as c_int),
                ) as *mut List;
                incompatible_joins =
                    bms_del_member(incompatible_joins, (*othersj).ojrelid as c_int);
            }

            // Compute qualscope and ojscope for this join level
            let mut tqs = bms_union(qualscope, joins_so_far);
            let mut tojs = bms_union(ojscope, joins_so_far);
            if above_sjinfo {
                // othersj is not yet in joins_so_far, but we need it
                tqs = bms_add_member(tqs, (*othersj).ojrelid as c_int);
                tojs = bms_add_member(tojs, (*othersj).ojrelid as c_int);
                // sjinfo is in joins_so_far, and we don't want it
                tojs = bms_del_member(tojs, (*sjinfo).ojrelid as c_int);
            }

            /*
             * We generate EquivalenceClasses only from the first form of the
             * quals, with the fewest nullingrels bits set.  An EC made from
             * this version of the quals can be useful below the outer-join
             * nest, whereas versions with some nullingrels bits set would not
             * be.  We cannot generate ECs from more than one version, or
             * we'll make nonsensical conclusions that Vars with nullingrels
             * bits set are equal to their versions without.  Fortunately,
             * such ECs wouldn't be very useful anyway, because they'd equate
             * values not observable outside the join nest.  (See
             * optimizer/README.)
             *
             * The first form of the quals is also the only one marked as
             * has_clone rather than is_clone.
             */
            allow_equivalence = joins_so_far.is_null();
            has_clone = allow_equivalence;
            is_clone = !has_clone;

            distribute_quals_to_rels(
                root,
                quals,
                otherjtitem,
                sjinfo,
                (*root).qual_security_level,
                tqs,
                tojs,
                nonnullable_rels,
                bms_copy(incompatible_joins),
                allow_equivalence,
                has_clone,
                is_clone,
                ptr::null_mut(), // no more postponement
            );

            /*
             * Adjust qual nulling bits for next level up, if needed.  We
             * don't want to put sjinfo's own bit in at all, and if we're
             * above sjinfo then we did it already.  Here, we should mark all
             * Vars coming from the lower join's RHS.  (Again, we are
             * converting a qual of the form Pbc to Pb*c, but now we are
             * putting back bits that were there in the parser output and were
             * temporarily stripped above.)  Update incompatible_joins too.
             */
            if below_sjinfo {
                quals = add_nulling_relids(
                    quals as *mut Node,
                    (*othersj).syn_righthand,
                    bms_make_singleton((*othersj).ojrelid as c_int),
                ) as *mut List;
                incompatible_joins =
                    bms_del_member(incompatible_joins, (*othersj).ojrelid as c_int);
            }

            // ... and track joins processed so far
            joins_so_far = bms_add_member(joins_so_far, (*othersj).ojrelid as c_int);
        });
    } else {
        // No commutation possible, just process the postponed clauses
        distribute_quals_to_rels(
            root,
            (*jtitem).oj_joinclauses,
            jtitem,
            sjinfo,
            (*root).qual_security_level,
            qualscope,
            ojscope,
            nonnullable_rels,
            ptr::null_mut(), // incompatible_relids
            true,            // allow_equivalence
            false,
            false, // not clones
            ptr::null_mut(), // no more postponement
        );
    }
}


/*****************************************************************************
 *
 *   QUALIFICATIONS
 *
 *****************************************************************************/

/*
 * distribute_quals_to_rels
 *   Convenience routine to apply distribute_qual_to_rels to each element
 *   of an AND'ed list of clauses.
 */
unsafe fn distribute_quals_to_rels(
    root: *mut PlannerInfo,
    clauses: *mut List,
    jtitem: *mut JoinTreeItem,
    sjinfo: *mut SpecialJoinInfo,
    security_level: Index,
    qualscope: Relids,
    ojscope: Relids,
    outerjoin_nonnullable: Relids,
    incompatible_relids: Relids,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postponed_oj_qual_list: *mut *mut List,
) {
    let mut lc: *mut ListCell = ptr::null_mut();

    foreach!(lc, clauses, {
        let clause = lfirst(crate::current_cell!(lc)) as *mut Node;

        distribute_qual_to_rels(
            root,
            clause,
            jtitem,
            sjinfo,
            security_level,
            qualscope,
            ojscope,
            outerjoin_nonnullable,
            incompatible_relids,
            allow_equivalence,
            has_clone,
            is_clone,
            postponed_oj_qual_list,
        );
    });
}

/*
 * distribute_qual_to_rels
 *   Add clause information to either the baserestrictinfo or joininfo list
 *   (depending on whether the clause is a join) of each base relation
 *   mentioned in the clause.  A RestrictInfo node is created and added to
 *   the appropriate list for each rel.  Alternatively, if the clause uses a
 *   mergejoinable operator, enter its left- and right-side expressions into
 *   the query's EquivalenceClasses.
 *
 * In some cases, quals will be added to parent jtitems' lateral_clauses
 * or to postponed_oj_qual_list instead of being processed right away.
 * These will be dealt with in later calls of deconstruct_distribute.
 *
 * 'clause': the qual clause to be distributed
 * 'jtitem': the JoinTreeItem for the containing jointree node
 * 'sjinfo': join's SpecialJoinInfo (NULL for an inner join or WHERE clause)
 * 'security_level': security_level to assign to the qual
 * 'qualscope': set of base+OJ rels the qual's syntactic scope covers
 * 'ojscope': NULL if not an outer-join qual, else the minimum set of base+OJ
 *       rels needed to form this join
 * 'outerjoin_nonnullable': NULL if not an outer-join qual, else the set of
 *       base+OJ rels appearing on the outer (nonnullable) side of the join
 *       (for FULL JOIN this includes both sides of the join, and must in fact
 *       equal qualscope)
 * 'incompatible_relids': the set of outer-join relid(s) that must not be
 *       computed below this qual.  We only bother to compute this for
 *       "clone" quals, otherwise it can be left NULL.
 * 'allow_equivalence': true if it's okay to convert clause into an
 *       EquivalenceClass
 * 'has_clone': has_clone property to assign to the qual
 * 'is_clone': is_clone property to assign to the qual
 * 'postponed_oj_qual_list': if not NULL, non-degenerate outer join clauses
 *       should be added to this list instead of being processed (list entries
 *       are just the bare clauses)
 *
 * 'qualscope' identifies what level of JOIN the qual came from syntactically.
 * 'ojscope' is needed if we decide to force the qual up to the outer-join
 * level, which will be ojscope not necessarily qualscope.
 *
 * At the time this is called, root->join_info_list must contain entries for
 * at least those special joins that are syntactically below this qual.
 * (We now need that only for detection of redundant IS NULL quals.)
 */
unsafe fn distribute_qual_to_rels(
    root: *mut PlannerInfo,
    clause: *mut Node,
    jtitem: *mut JoinTreeItem,
    sjinfo: *mut SpecialJoinInfo,
    security_level: Index,
    qualscope: Relids,
    ojscope: Relids,
    outerjoin_nonnullable: Relids,
    incompatible_relids: Relids,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postponed_oj_qual_list: *mut *mut List,
) {
    let mut relids: Relids;
    let is_pushed_down: bool;
    let mut pseudoconstant = false;
    let maybe_equivalence: bool;
    let maybe_outer_join: bool;
    let restrictinfo: *mut RestrictInfo;

    /*
     * Retrieve all relids mentioned within the clause.
     */
    relids = pull_varnos(root, clause);

    /*
     * In ordinary SQL, a WHERE or JOIN/ON clause can't reference any rels
     * that aren't within its syntactic scope; however, if we pulled up a
     * LATERAL subquery then we might find such references in quals that have
     * been pulled up.  We need to treat such quals as belonging to the join
     * level that includes every rel they reference.  Although we could make
     * pull_up_subqueries() place such quals correctly to begin with, it's
     * easier to handle it here.  When we find a clause that contains Vars
     * outside its syntactic scope, locate the nearest parent join level that
     * includes all the required rels and add the clause to that level's
     * lateral_clauses list.  We'll process it when we reach that join level.
     */
    if !bms_is_subset(relids, qualscope) {
        let mut pitem = (*jtitem).jti_parent;

        Assert!((*root).hasLateralRTEs); // shouldn't happen otherwise
        Assert!(sjinfo.is_null()); // mustn't postpone past outer join
        'search: loop {
            if pitem.is_null() {
                break 'search;
            }
            if bms_is_subset(relids, (*pitem).qualscope) {
                (*pitem).lateral_clauses = lappend((*pitem).lateral_clauses, clause as *mut _);
                return;
            }

            /*
             * We should not be postponing any quals past an outer join.  If
             * this Assert fires, pull_up_subqueries() messed up.
             */
            Assert!((*pitem).sjinfo.is_null());
            pitem = (*pitem).jti_parent;
        }
        elog!(ERROR, "failed to postpone qual containing lateral reference");
    }

    /*
     * If it's an outer-join clause, also check that relids is a subset of
     * ojscope.  (This should not fail if the syntactic scope check passed.)
     */
    if !ojscope.is_null() && !bms_is_subset(relids, ojscope) {
        elog!(ERROR, "JOIN qualification cannot refer to other relations");
    }

    /*
     * If the clause is variable-free, our normal heuristic for pushing it
     * down to just the mentioned rels doesn't work, because there are none.
     *
     * If the clause is an outer-join clause, we must force it to the OJ's
     * semantic level to preserve semantics.
     *
     * Otherwise, when the clause contains volatile functions, we force it to
     * be evaluated at its original syntactic level.  This preserves the
     * expected semantics.
     *
     * When the clause contains no volatile functions either, it is actually a
     * pseudoconstant clause that will not change value during any one
     * execution of the plan, and hence can be used as a one-time qual in a
     * gating Result plan node.  We put such a clause into the regular
     * RestrictInfo lists for the moment, but eventually createplan.c will
     * pull it out and make a gating Result node immediately above whatever
     * plan node the pseudoconstant clause is assigned to.  It's usually best
     * to put a gating node as high in the plan tree as possible.
     */
    if bms_is_empty(relids) {
        if !ojscope.is_null() {
            // clause is attached to outer join, eval it there
            relids = bms_copy(ojscope);
            // mustn't use as gating qual, so don't mark pseudoconstant
        } else if contain_volatile_functions(clause) {
            // eval at original syntactic level
            relids = bms_copy(qualscope);
            // again, can't mark pseudoconstant
        } else {
            /*
             * If we are in the top-level join domain, we can push the qual to
             * the top of the plan tree.  Otherwise, be conservative and eval
             * it at original syntactic level.  (Ideally we'd push it to the
             * top of the current join domain in all cases, but that causes
             * problems if we later rearrange outer-join evaluation order.
             * Pseudoconstant quals below the top level are a pretty odd case,
             * so it's not clear that it's worth working hard on.)
             */
            let top_jdomain = linitial((*root).join_domains) as *mut JoinDomain;
            if (*jtitem).jdomain == top_jdomain {
                relids = bms_copy((*(*jtitem).jdomain).jd_relids);
            } else {
                relids = bms_copy(qualscope);
            }
            // mark as gating qual
            pseudoconstant = true;
            // tell createplan.c to check for gating quals
            (*root).hasPseudoConstantQuals = true;
        }
    }

    /*----------
     * Check to see if clause application must be delayed by outer-join
     * considerations.
     *
     * A word about is_pushed_down: we mark the qual as "pushed down" if
     * it is (potentially) applicable at a level different from its original
     * syntactic level.  This flag is used to distinguish OUTER JOIN ON quals
     * from other quals pushed down to the same joinrel.  The rules are:
     *       WHERE quals and INNER JOIN quals: is_pushed_down = true.
     *       Non-degenerate OUTER JOIN quals: is_pushed_down = false.
     *       Degenerate OUTER JOIN quals: is_pushed_down = true.
     * A "degenerate" OUTER JOIN qual is one that doesn't mention the
     * non-nullable side, and hence can be pushed down into the nullable side
     * without changing the join result.  It is correct to treat it as a
     * regular filter condition at the level where it is evaluated.
     *
     * Note: it is not immediately obvious that a simple boolean is enough
     * for this: if for some reason we were to attach a degenerate qual to
     * its original join level, it would need to be treated as an outer join
     * qual there.  However, this cannot happen, because all the rels the
     * clause mentions must be in the outer join's min_righthand, therefore
     * the join it needs must be formed before the outer join; and we always
     * attach quals to the lowest level where they can be evaluated.  But
     * if we were ever to re-introduce a mechanism for delaying evaluation
     * of "expensive" quals, this area would need work.
     *
     * Note: generally, use of is_pushed_down has to go through the macro
     * RINFO_IS_PUSHED_DOWN, because that flag alone is not always sufficient
     * to tell whether a clause must be treated as pushed-down in context.
     * This seems like another reason why it should perhaps be rethought.
     *----------
     */
    if bms_overlap(relids, outerjoin_nonnullable) {
        /*
         * The qual is attached to an outer join and mentions (some of the)
         * rels on the nonnullable side, so it's not degenerate.  If the
         * caller wants to postpone handling such clauses, just add it to
         * postponed_oj_qual_list and return.  (The work we've done up to here
         * will have to be redone later, but there's not much of it.)
         */
        if !postponed_oj_qual_list.is_null() {
            *postponed_oj_qual_list = lappend(*postponed_oj_qual_list, clause as *mut _);
            return;
        }

        /*
         * We can't use such a clause to deduce equivalence (the left and
         * right sides might be unequal above the join because one of them has
         * gone to NULL) ... but we might be able to use it for more limited
         * deductions, if it is mergejoinable.  So consider adding it to the
         * lists of set-aside outer-join clauses.
         */
        let is_pushed_down2 = false;
        let maybe_equivalence2 = false;
        let maybe_outer_join2 = true;

        /*
         * Now force the qual to be evaluated exactly at the level of joining
         * corresponding to the outer join.  We cannot let it get pushed down
         * into the nonnullable side, since then we'd produce no output rows,
         * rather than the intended single null-extended row, for any
         * nonnullable-side rows failing the qual.
         */
        Assert!(!ojscope.is_null());
        relids = ojscope;
        Assert!(!pseudoconstant);

        let restrictinfo2 = make_restrictinfo(
            root,
            clause as *mut Expr,
            is_pushed_down2,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            relids,
            incompatible_relids,
            outerjoin_nonnullable,
        );

        // add vars to targetlists if join clause
        if bms_membership(relids) == BMS_MULTIPLE {
            let vars = pull_var_clause(
                clause,
                PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
            );
            let where_needed: Relids;
            if is_clone {
                where_needed = bms_intersect(relids, (*root).all_baserels);
            } else {
                where_needed = relids;
            }
            add_vars_to_targetlist(root, vars, where_needed);
            list_free(vars);
        }

        check_mergejoinable(restrictinfo2);

        if !(*restrictinfo2).mergeopfamilies.is_null() {
            if maybe_equivalence2 {
                if process_equivalence(root, &mut (restrictinfo2 as *mut _) as *mut *mut _, (*jtitem).jdomain) {
                    return;
                }
                if !(*restrictinfo2).mergeopfamilies.is_null() {
                    initialize_mergeclause_eclasses(root, restrictinfo2);
                }
            } else if maybe_outer_join2 && (*restrictinfo2).can_join {
                initialize_mergeclause_eclasses(root, restrictinfo2);
                Assert!(!sjinfo.is_null());
                if bms_is_subset((*restrictinfo2).left_relids, outerjoin_nonnullable)
                    && !bms_overlap((*restrictinfo2).right_relids, outerjoin_nonnullable)
                {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).left_join_clauses =
                        lappend((*root).left_join_clauses, ojcinfo as *mut _);
                    return;
                }
                if bms_is_subset((*restrictinfo2).right_relids, outerjoin_nonnullable)
                    && !bms_overlap((*restrictinfo2).left_relids, outerjoin_nonnullable)
                {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).right_join_clauses =
                        lappend((*root).right_join_clauses, ojcinfo as *mut _);
                    return;
                }
                if (*sjinfo).jointype == JOIN_FULL {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).full_join_clauses =
                        lappend((*root).full_join_clauses, ojcinfo as *mut _);
                    return;
                }
                // nope, so fall through to distribute_restrictinfo_to_rels
            } else {
                initialize_mergeclause_eclasses(root, restrictinfo2);
            }
        }

        distribute_restrictinfo_to_rels(root, restrictinfo2);
    } else {
        /*
         * Normal qual clause or degenerate outer-join clause.  Either way, we
         * can mark it as pushed-down.
         */
        let is_pushed_down2 = true;

        /*
         * It's possible that this is an IS NULL clause that's redundant with
         * a lower antijoin; if so we can just discard it.  We need not test
         * in any of the other cases, because this will only be possible for
         * pushed-down clauses.
         */
        if check_redundant_nullability_qual(root, clause) {
            return;
        }

        // Feed qual to the equivalence machinery, if allowed by caller
        let maybe_equivalence2 = allow_equivalence;

        /*
         * Since it doesn't mention the LHS, it's certainly not useful as a
         * set-aside OJ clause, even if it's in an OJ.
         */
        let maybe_outer_join2 = false;

        /*
         * Build the RestrictInfo node itself.
         */
        let restrictinfo2 = make_restrictinfo(
            root,
            clause as *mut Expr,
            is_pushed_down2,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            relids,
            incompatible_relids,
            outerjoin_nonnullable,
        );

        /*
         * If it's a join clause, add vars used in the clause to targetlists of
         * their relations, so that they will be emitted by the plan nodes that
         * scan those relations (else they won't be available at the join node!).
         */
        if bms_membership(relids) == BMS_MULTIPLE {
            let vars = pull_var_clause(
                clause,
                PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
            );
            let where_needed: Relids;
            if is_clone {
                where_needed = bms_intersect(relids, (*root).all_baserels);
            } else {
                where_needed = relids;
            }
            add_vars_to_targetlist(root, vars, where_needed);
            list_free(vars);
        }

        /*
         * We check "mergejoinability" of every clause, not only join clauses,
         * because we want to know about equivalences between vars of the same
         * relation, or between vars and consts.
         */
        check_mergejoinable(restrictinfo2);

        if !(*restrictinfo2).mergeopfamilies.is_null() {
            if maybe_equivalence2 {
                if process_equivalence(root, &mut (restrictinfo2 as *mut _) as *mut *mut _, (*jtitem).jdomain) {
                    return;
                }
                if !(*restrictinfo2).mergeopfamilies.is_null() {
                    initialize_mergeclause_eclasses(root, restrictinfo2);
                }
            } else if maybe_outer_join2 && (*restrictinfo2).can_join {
                initialize_mergeclause_eclasses(root, restrictinfo2);
                Assert!(!sjinfo.is_null());
                if bms_is_subset((*restrictinfo2).left_relids, outerjoin_nonnullable)
                    && !bms_overlap((*restrictinfo2).right_relids, outerjoin_nonnullable)
                {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).left_join_clauses =
                        lappend((*root).left_join_clauses, ojcinfo as *mut _);
                    return;
                }
                if bms_is_subset((*restrictinfo2).right_relids, outerjoin_nonnullable)
                    && !bms_overlap((*restrictinfo2).left_relids, outerjoin_nonnullable)
                {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).right_join_clauses =
                        lappend((*root).right_join_clauses, ojcinfo as *mut _);
                    return;
                }
                if (*sjinfo).jointype == JOIN_FULL {
                    let ojcinfo = makeNode!(OuterJoinClauseInfo, T_OuterJoinClauseInfo) as *mut OuterJoinClauseInfo;
                    (*ojcinfo).rinfo = restrictinfo2;
                    (*ojcinfo).sjinfo = sjinfo;
                    (*root).full_join_clauses =
                        lappend((*root).full_join_clauses, ojcinfo as *mut _);
                    return;
                }
            } else {
                // we still need to set up left_ec/right_ec
                initialize_mergeclause_eclasses(root, restrictinfo2);
            }
        }

        // No EC special case applies, so push it into the clause lists
        distribute_restrictinfo_to_rels(root, restrictinfo2);
    }
}

/*
 * check_redundant_nullability_qual
 *   Check to see if the qual is an IS NULL qual that is redundant with
 *   a lower JOIN_ANTI join.
 *
 * We want to suppress redundant IS NULL quals, not so much to save cycles
 * as to avoid generating bogus selectivity estimates for them.  So if
 * redundancy is detected here, distribute_qual_to_rels() just throws away
 * the qual.
 */
unsafe fn check_redundant_nullability_qual(root: *mut PlannerInfo, clause: *mut Node) -> bool {
    let forced_null_var: *mut Var;
    let mut lc: *mut ListCell = ptr::null_mut();

    // Check for IS NULL, and identify the Var forced to NULL
    forced_null_var = find_forced_null_var(clause);
    if forced_null_var.is_null() {
        return false;
    }

    /*
     * If the Var comes from the nullable side of a lower antijoin, the IS
     * NULL condition is necessarily true.  If it's not nulled by anything,
     * there is no point in searching the join_info_list.  Otherwise, we need
     * to find out whether the nulling rel is an antijoin.
     */
    if (*forced_null_var).varnullingrels.is_null() {
        return false;
    }

    foreach!(lc, (*root).join_info_list, {
        let sjinfo = lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;

        /*
         * This test will not succeed if sjinfo->ojrelid is zero, which is
         * possible for an antijoin that was converted from a semijoin; but in
         * such a case the Var couldn't have come from its nullable side.
         */
        if (*sjinfo).jointype == JOIN_ANTI
            && (*sjinfo).ojrelid != 0
            && bms_is_member((*sjinfo).ojrelid as c_int, (*forced_null_var).varnullingrels)
        {
            return true;
        }
    });

    false
}

/*
 * add_base_clause_to_rel
 *       Add 'restrictinfo' as a baserestrictinfo to the base relation denoted
 *       by 'relid'.  We offer some simple prechecks to try to determine if the
 *       qual is always true, in which case we ignore it rather than add it.
 *       If we detect the qual is always false, we replace it with
 *       constant-FALSE.
 */
unsafe fn add_base_clause_to_rel(
    root: *mut PlannerInfo,
    relid: c_int,
    mut restrictinfo: *mut RestrictInfo,
) {
    let rel = find_base_rel(root, relid);
    let rte = *(*root).simple_rte_array.add(relid as usize);

    Assert!(bms_membership((*restrictinfo).required_relids) == BMS_SINGLETON);

    /*
     * For inheritance parent tables, we must always record the RestrictInfo
     * in baserestrictinfo as is.  If we were to transform or skip adding it,
     * then the original wouldn't be available in apply_child_basequals. Since
     * there are two RangeTblEntries for inheritance parents, one with
     * inh==true and the other with inh==false, we're still able to apply this
     * optimization to the inh==false one.  The inh==true one is what
     * apply_child_basequals() sees, whereas the inh==false one is what's used
     * for the scan node in the final plan.
     *
     * We make an exception to this for partitioned tables.  For these, we
     * always apply the constant-TRUE and constant-FALSE transformations.  A
     * qual which is either of these for a partitioned table must also be that
     * for all of its child partitions.
     */
    if !(*rte).inh || (*rte).relkind == RELKIND_PARTITIONED_TABLE {
        // Don't add the clause if it is always true
        if restriction_is_always_true(root, restrictinfo) {
            return;
        }

        /*
         * Substitute the origin qual with constant-FALSE if it is provably
         * always false.
         *
         * Note that we need to keep the same rinfo_serial, since it is in
         * practice the same condition.  We also need to reset the
         * last_rinfo_serial counter, which is essential to ensure that the
         * RestrictInfos for the "same" qual condition get identical serial
         * numbers (see deconstruct_distribute_oj_quals).
         */
        if restriction_is_always_false(root, restrictinfo) {
            let save_rinfo_serial = (*restrictinfo).rinfo_serial;
            let save_last_rinfo_serial = (*root).last_rinfo_serial;

            restrictinfo = make_restrictinfo(
                root,
                makeBoolConst(false, false) as *mut Expr,
                (*restrictinfo).is_pushed_down,
                (*restrictinfo).has_clone,
                (*restrictinfo).is_clone,
                (*restrictinfo).pseudoconstant,
                0, // security_level
                (*restrictinfo).required_relids,
                (*restrictinfo).incompatible_relids,
                (*restrictinfo).outer_relids,
            );
            (*restrictinfo).rinfo_serial = save_rinfo_serial;
            (*root).last_rinfo_serial = save_last_rinfo_serial;
        }
    }

    // Add clause to rel's restriction list
    (*rel).baserestrictinfo = lappend((*rel).baserestrictinfo, restrictinfo as *mut _);

    // Update security level info
    if (*restrictinfo).security_level < (*rel).baserestrict_min_security {
        (*rel).baserestrict_min_security = (*restrictinfo).security_level;
    }
}

/*
 * expr_is_nonnullable
 *   Check to see if the Expr cannot be NULL
 *
 * If the Expr is a simple Var that is defined NOT NULL and meanwhile is not
 * nulled by any outer joins, then we can know that it cannot be NULL.
 */
unsafe fn expr_is_nonnullable(root: *mut PlannerInfo, expr: *mut Expr) -> bool {
    let rel: *mut RelOptInfo;
    let var: *mut Var;

    // For now only check simple Vars
    if !IsA!(expr as *mut Node, T_Var) {
        return false;
    }

    var = expr as *mut Var;

    // could the Var be nulled by any outer joins?
    if !bms_is_empty((*var).varnullingrels) {
        return false;
    }

    // system columns cannot be NULL
    if (*var).varattno < 0 {
        return true;
    }

    // is the column defined NOT NULL?
    rel = find_base_rel(root, (*var).varno as c_int);
    if (*var).varattno > 0
        && bms_is_member((*var).varattno as c_int, (*rel).notnullattnums)
    {
        return true;
    }

    false
}

/*
 * restriction_is_always_true
 *   Check to see if the RestrictInfo is always true.
 *
 * Currently we only check for NullTest quals and OR clauses that include
 * NullTest quals.  We may extend it in the future.
 */
pub unsafe fn restriction_is_always_true(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    /*
     * For a clone clause, we don't have a reliable way to determine if the
     * input expression of a NullTest is non-nullable: nullingrel bits in
     * clone clauses may not reflect reality, so we dare not draw conclusions
     * from clones about whether Vars are guaranteed not-null.
     */
    if (*restrictinfo).has_clone || (*restrictinfo).is_clone {
        return false;
    }

    // Check for NullTest qual
    if IsA!((*restrictinfo).clause as *mut Node, T_NullTest) {
        let nulltest = (*restrictinfo).clause as *mut NullTest;

        // is this NullTest an IS_NOT_NULL qual?
        if (*nulltest).nulltesttype != IS_NOT_NULL {
            return false;
        }

        /*
         * Empty rows can appear NULL in some contexts and NOT NULL in others,
         * so avoid this optimization for row expressions.
         */
        if (*nulltest).argisrow {
            return false;
        }

        return expr_is_nonnullable(root, (*nulltest).arg);
    }

    // If it's an OR, check its sub-clauses
    if restriction_is_or_clause(restrictinfo) {
        let mut lc: *mut ListCell = ptr::null_mut();

        Assert!(is_orclause((*restrictinfo).orclause as *mut Node));

        /*
         * if any of the given OR branches is provably always true then the
         * entire condition is true.
         */
        let bexpr = (*restrictinfo).orclause as *mut BoolExpr;
        foreach!(lc, (*bexpr).args, {
            let orarg = lfirst(crate::current_cell!(lc)) as *mut Node;

            if !IsA!(orarg, T_RestrictInfo) {
                continue;
            }

            if restriction_is_always_true(root, orarg as *mut RestrictInfo) {
                return true;
            }
        });
    }

    false
}

/*
 * restriction_is_always_false
 *   Check to see if the RestrictInfo is always false.
 *
 * Currently we only check for NullTest quals and OR clauses that include
 * NullTest quals.  We may extend it in the future.
 */
pub unsafe fn restriction_is_always_false(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    /*
     * For a clone clause, we don't have a reliable way to determine if the
     * input expression of a NullTest is non-nullable: nullingrel bits in
     * clone clauses may not reflect reality, so we dare not draw conclusions
     * from clones about whether Vars are guaranteed not-null.
     */
    if (*restrictinfo).has_clone || (*restrictinfo).is_clone {
        return false;
    }

    // Check for NullTest qual
    if IsA!((*restrictinfo).clause as *mut Node, T_NullTest) {
        let nulltest = (*restrictinfo).clause as *mut NullTest;

        // is this NullTest an IS_NULL qual?
        if (*nulltest).nulltesttype != IS_NULL {
            return false;
        }

        /*
         * Empty rows can appear NULL in some contexts and NOT NULL in others,
         * so avoid this optimization for row expressions.
         */
        if (*nulltest).argisrow {
            return false;
        }

        return expr_is_nonnullable(root, (*nulltest).arg);
    }

    // If it's an OR, check its sub-clauses
    if restriction_is_or_clause(restrictinfo) {
        let mut lc: *mut ListCell = ptr::null_mut();

        Assert!(is_orclause((*restrictinfo).orclause as *mut Node));

        /*
         * Currently, when processing OR expressions, we only return true when
         * all of the OR branches are always false.  This could perhaps be
         * expanded to remove OR branches that are provably false.  This may
         * be a useful thing to do as it could result in the OR being left
         * with a single arg.  That's useful as it would allow the OR
         * condition to be replaced with its single argument which may allow
         * use of an index for faster filtering on the remaining condition.
         */
        let bexpr = (*restrictinfo).orclause as *mut BoolExpr;
        foreach!(lc, (*bexpr).args, {
            let orarg = lfirst(crate::current_cell!(lc)) as *mut Node;

            if !IsA!(orarg, T_RestrictInfo)
                || !restriction_is_always_false(root, orarg as *mut RestrictInfo)
            {
                return false;
            }
        });
        return true;
    }

    false
}

/*
 * distribute_restrictinfo_to_rels
 *   Push a completed RestrictInfo into the proper restriction or join
 *   clause list(s).
 *
 * This is the last step of distribute_qual_to_rels() for ordinary qual
 * clauses.  Clauses that are interesting for equivalence-class processing
 * are diverted to the EC machinery, but may ultimately get fed back here.
 */
pub unsafe fn distribute_restrictinfo_to_rels(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) {
    let relids = (*restrictinfo).required_relids;

    if !bms_is_empty(relids) {
        let mut relid: c_int = 0;

        if bms_get_singleton_member(relids, &mut relid) {
            /*
             * There is only one relation participating in the clause, so it
             * is a restriction clause for that relation.
             */
            add_base_clause_to_rel(root, relid, restrictinfo);
        } else {
            /*
             * The clause is a join clause, since there is more than one rel
             * in its relid set.
             */

            /*
             * Check for hashjoinable operators.  (We don't bother setting the
             * hashjoin info except in true join clauses.)
             */
            check_hashjoinable(restrictinfo);

            /*
             * Likewise, check if the clause is suitable to be used with a
             * Memoize node to cache inner tuples during a parameterized
             * nested loop.
             */
            check_memoizable(restrictinfo);

            /*
             * Add clause to the join lists of all the relevant relations.
             */
            add_join_clause_to_rels(root, restrictinfo, relids);
        }
    } else {
        /*
         * clause references no rels, and therefore we have no place to attach
         * it.  Shouldn't get here if callers are working properly.
         */
        elog!(ERROR, "cannot cope with variable-free clause");
    }
}

/*
 * process_implied_equality
 *   Create a restrictinfo item that says "item1 op item2", and push it
 *   into the appropriate lists.  (In practice opno is always a btree
 *   equality operator.)
 *
 * "qualscope" is the nominal syntactic level to impute to the restrictinfo.
 * This must contain at least all the rels used in the expressions, but it
 * is used only to set the qual application level when both exprs are
 * variable-free.  (Hence, it should usually match the join domain in which
 * the clause applies.)  Otherwise the qual is applied at the lowest join
 * level that provides all its variables.
 *
 * "security_level" is the security level to assign to the new restrictinfo.
 *
 * "both_const" indicates whether both items are known pseudo-constant;
 * in this case it is worth applying eval_const_expressions() in case we
 * can produce constant TRUE or constant FALSE.  (Otherwise it's not,
 * because the expressions went through eval_const_expressions already.)
 *
 * Returns the generated RestrictInfo, if any.  The result will be NULL
 * if both_const is true and we successfully reduced the clause to
 * constant TRUE.
 *
 * Note: this function will copy item1 and item2, but it is caller's
 * responsibility to make sure that the Relids parameters are fresh copies
 * not shared with other uses.
 *
 * Note: we do not do initialize_mergeclause_eclasses() here.  It is
 * caller's responsibility that left_ec/right_ec be set as necessary.
 */
pub unsafe fn process_implied_equality(
    root: *mut PlannerInfo,
    opno: Oid,
    collation: Oid,
    item1: *mut Expr,
    item2: *mut Expr,
    qualscope: Relids,
    security_level: Index,
    both_const: bool,
) -> *mut RestrictInfo {
    let restrictinfo: *mut RestrictInfo;
    let mut clause: *mut Node;
    let mut relids: Relids;
    let mut pseudoconstant = false;

    /*
     * Build the new clause.  Copy to ensure it shares no substructure with
     * original (this is necessary in case there are subselects in there...)
     */
    clause = make_opclause(
        opno,
        BOOLOID, // opresulttype
        false,   // opretset
        copyObject(item1 as *const Expr),
        copyObject(item2 as *const Expr),
        InvalidOid,
        collation,
    ) as *mut Node;

    // If both constant, try to reduce to a boolean constant.
    if both_const {
        clause = eval_const_expressions(root, clause);

        // If we produced const TRUE, just drop the clause
        if !clause.is_null() && IsA!(clause, T_Const) {
            let cclause = clause as *mut crate::nodes::primnodes::Const;
            Assert!((*cclause).consttype == BOOLOID);
            if !(*cclause).constisnull && DatumGetBool((*cclause).constvalue) {
                return ptr::null_mut();
            }
        }
    }

    /*
     * The rest of this is a very cut-down version of distribute_qual_to_rels.
     * We can skip most of the work therein, but there are a couple of special
     * cases we still have to handle.
     *
     * Retrieve all relids mentioned within the possibly-simplified clause.
     */
    relids = pull_varnos(root, clause);
    Assert!(bms_is_subset(relids, qualscope));

    /*
     * If the clause is variable-free, our normal heuristic for pushing it
     * down to just the mentioned rels doesn't work, because there are none.
     * Apply it as a gating qual at the appropriate level (see comments for
     * get_join_domain_min_rels).
     */
    if bms_is_empty(relids) {
        // eval at join domain's safe level
        relids = get_join_domain_min_rels(root, qualscope);
        // mark as gating qual
        pseudoconstant = true;
        // tell createplan.c to check for gating quals
        (*root).hasPseudoConstantQuals = true;
    }

    /*
     * Build the RestrictInfo node itself.
     */
    let restrictinfo = make_restrictinfo(
        root,
        clause as *mut Expr,
        true,  // is_pushed_down
        false, // !has_clone
        false, // !is_clone
        pseudoconstant,
        security_level,
        relids,
        ptr::null_mut(), // incompatible_relids
        ptr::null_mut(), // outer_relids
    );

    /*
     * If it's a join clause, add vars used in the clause to targetlists of
     * their relations, so that they will be emitted by the plan nodes that
     * scan those relations (else they won't be available at the join node!).
     *
     * Typically, we'd have already done this when the component expressions
     * were first seen by distribute_qual_to_rels; but it is possible that
     * some of the Vars could have missed having that done because they only
     * appeared in single-relation clauses originally.  So do it here for
     * safety.
     *
     * See also rebuild_joinclause_attr_needed, which has to partially repeat
     * this work after removal of an outer join.  (Since we will put this
     * clause into the joininfo lists, that function needn't do any extra work
     * to find it.)
     */
    if bms_membership(relids) == BMS_MULTIPLE {
        let vars = pull_var_clause(
            clause,
            PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
        );
        add_vars_to_targetlist(root, vars, relids);
        list_free(vars);
    }

    /*
     * Check mergejoinability.  This will usually succeed, since the op came
     * from an EquivalenceClass; but we could have reduced the original clause
     * to a constant.
     */
    check_mergejoinable(restrictinfo);

    /*
     * Note we don't do initialize_mergeclause_eclasses(); the caller can
     * handle that much more cheaply than we can.  It's okay to call
     * distribute_restrictinfo_to_rels() before that happens.
     */

    /*
     * Push the new clause into all the appropriate restrictinfo lists.
     */
    distribute_restrictinfo_to_rels(root, restrictinfo);

    restrictinfo
}

/*
 * build_implied_join_equality --- build a RestrictInfo for a derived equality
 *
 * This overlaps the functionality of process_implied_equality(), but we
 * must not push the RestrictInfo into the joininfo tree.
 *
 * Note: this function will copy item1 and item2, but it is caller's
 * responsibility to make sure that the Relids parameters are fresh copies
 * not shared with other uses.
 *
 * Note: we do not do initialize_mergeclause_eclasses() here.  It is
 * caller's responsibility that left_ec/right_ec be set as necessary.
 */
pub unsafe fn build_implied_join_equality(
    root: *mut PlannerInfo,
    opno: Oid,
    collation: Oid,
    item1: *mut Expr,
    item2: *mut Expr,
    qualscope: Relids,
    security_level: Index,
) -> *mut RestrictInfo {
    let clause: *mut Expr;

    /*
     * Build the new clause.  Copy to ensure it shares no substructure with
     * original (this is necessary in case there are subselects in there...)
     */
    clause = make_opclause(
        opno,
        BOOLOID, // opresulttype
        false,   // opretset
        copyObject(item1 as *const Expr),
        copyObject(item2 as *const Expr),
        InvalidOid,
        collation,
    );

    /*
     * Build the RestrictInfo node itself.
     */
    let restrictinfo = make_restrictinfo(
        root,
        clause,
        true,  // is_pushed_down
        false, // !has_clone
        false, // !is_clone
        false, // pseudoconstant
        security_level, // security_level
        qualscope,      // required_relids
        ptr::null_mut(), // incompatible_relids
        ptr::null_mut(), // outer_relids
    );

    // Set mergejoinability/hashjoinability flags
    check_mergejoinable(restrictinfo);
    check_hashjoinable(restrictinfo);
    check_memoizable(restrictinfo);

    restrictinfo
}

/*
 * get_join_domain_min_rels
 *   Identify the appropriate join level for derived quals belonging
 *   to the join domain with the given relids.
 *
 * When we derive a pseudoconstant (Var-free) clause from an EquivalenceClass,
 * we'd ideally apply the clause at the top level of the EC's join domain.
 * However, if there are any outer joins inside that domain that get commuted
 * with joins outside it, that leads to not finding a correct place to apply
 * the clause.  Instead, remove any lower outer joins from the relid set,
 * and apply the clause to just the remaining rels.  This still results in a
 * correct answer, since if the clause produces FALSE then the LHS of these
 * joins will be empty leading to an empty join result.
 *
 * However, there's no need to remove outer joins if this is the top-level
 * join domain of the query, since then there's nothing else to commute with.
 *
 * Note: it's tempting to use this in distribute_qual_to_rels where it's
 * dealing with pseudoconstant quals; but we can't because the necessary
 * SpecialJoinInfos aren't all formed at that point.
 *
 * The result is always freshly palloc'd; we do not modify domain_relids.
 */
unsafe fn get_join_domain_min_rels(root: *mut PlannerInfo, domain_relids: Relids) -> Relids {
    let mut result = bms_copy(domain_relids);
    let mut lc: *mut ListCell = ptr::null_mut();

    // Top-level join domain?
    if bms_equal(result, (*root).all_query_rels) {
        return result;
    }

    // Nope, look for lower outer joins that could potentially commute out
    foreach!(lc, (*root).join_info_list, {
        let sjinfo = lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;

        if (*sjinfo).jointype == JOIN_LEFT
            && bms_is_member((*sjinfo).ojrelid as c_int, result)
        {
            result = bms_del_member(result, (*sjinfo).ojrelid as c_int);
            result = bms_del_members(result, (*sjinfo).syn_righthand);
        }
    });

    result
}


/*
 * rebuild_joinclause_attr_needed
 *   Put back attr_needed bits for Vars/PHVs needed for join clauses.
 *
 * This is used to rebuild attr_needed/ph_needed sets after removal of a
 * useless outer join.  It should match what distribute_qual_to_rels did,
 * except that we call add_vars_to_attr_needed not add_vars_to_targetlist.
 */
pub unsafe fn rebuild_joinclause_attr_needed(root: *mut PlannerInfo) {
    /*
     * We must examine all join clauses, but there's no value in processing
     * any join clause more than once.  So it's slightly annoying that we have
     * to find them via the per-base-relation joininfo lists.  Avoid duplicate
     * processing by tracking the rinfo_serial numbers of join clauses we've
     * already seen.  (This doesn't work for is_clone clauses, so we must
     * waste effort on them.)
     */
    let mut seen_serials: *mut Bitmapset = ptr::null_mut();
    let mut rti: Index = 1;

    // Scan all baserels for join clauses
    while (rti as c_int) < (*root).simple_rel_array_size {
        let brel = *(*root).simple_rel_array.add(rti as usize);
        let mut lc: *mut ListCell = ptr::null_mut();

        if brel.is_null() {
            rti += 1;
            continue;
        }
        if (*brel).reloptkind != RELOPT_BASEREL {
            rti += 1;
            continue;
        }

        foreach!(lc, (*brel).joininfo, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
            let relids = (*rinfo).required_relids;

            if !(*rinfo).is_clone { // else serial number is not unique
                if bms_is_member((*rinfo).rinfo_serial, seen_serials) {
                    continue; // saw it already
                }
                seen_serials = bms_add_member(seen_serials, (*rinfo).rinfo_serial);
            }

            if bms_membership(relids) == BMS_MULTIPLE {
                let vars = pull_var_clause(
                    (*rinfo).clause as *mut Node,
                    PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS,
                );
                let where_needed: Relids;

                if (*rinfo).is_clone {
                    where_needed = bms_intersect(relids, (*root).all_baserels);
                } else {
                    where_needed = relids;
                }
                add_vars_to_attr_needed(root, vars, where_needed);
                list_free(vars);
            }
        });
        rti += 1;
    }
}


/*
 * match_foreign_keys_to_quals
 *       Match foreign-key constraints to equivalence classes and join quals
 *
 * The idea here is to see which query join conditions match equality
 * constraints of a foreign-key relationship.  For such join conditions,
 * we can use the FK semantics to make selectivity estimates that are more
 * reliable than estimating from statistics, especially for multiple-column
 * FKs, where the normal assumption of independent conditions tends to fail.
 *
 * In this function we annotate the ForeignKeyOptInfos in root->fkey_list
 * with info about which eclasses and join qual clauses they match, and
 * discard any ForeignKeyOptInfos that are irrelevant for the query.
 */
pub unsafe fn match_foreign_keys_to_quals(root: *mut PlannerInfo) {
    let mut newlist: *mut List = NIL;
    let mut lc: *mut ListCell = ptr::null_mut();

    foreach!(lc, (*root).fkey_list, {
        let fkinfo = lfirst(crate::current_cell!(lc)) as *mut ForeignKeyOptInfo;
        let con_rel: *mut RelOptInfo;
        let ref_rel: *mut RelOptInfo;
        let mut colno: c_int;

        /*
         * Either relid might identify a rel that is in the query's rtable but
         * isn't referenced by the jointree, or has been removed by join
         * removal, so that it won't have a RelOptInfo.  Hence don't use
         * find_base_rel() here.  We can ignore such FKs.
         */
        if (*fkinfo).con_relid >= (*root).simple_rel_array_size as Index
            || (*fkinfo).ref_relid >= (*root).simple_rel_array_size as Index
        {
            continue; // just paranoia
        }
        let cr = *(*root).simple_rel_array.add((*fkinfo).con_relid as usize);
        if cr.is_null() {
            continue;
        }
        let rr = *(*root).simple_rel_array.add((*fkinfo).ref_relid as usize);
        if rr.is_null() {
            continue;
        }

        /*
         * Ignore FK unless both rels are baserels.  This gets rid of FKs that
         * link to inheritance child rels (otherrels).
         */
        if (*cr).reloptkind != RELOPT_BASEREL || (*rr).reloptkind != RELOPT_BASEREL {
            continue;
        }

        /*
         * Scan the columns and try to match them to eclasses and quals.
         *
         * Note: for simple inner joins, any match should be in an eclass.
         * "Loose" quals that syntactically match an FK equality must have
         * been rejected for EC status because they are outer-join quals or
         * similar.  We can still consider them to match the FK.
         */
        colno = 0;
        while colno < (*fkinfo).nkeys {
            let ec: *mut EquivalenceClass;
            let con_attno: AttrNumber;
            let ref_attno: AttrNumber;
            let mut fpeqop: Oid = InvalidOid;
            let mut lc2: *mut ListCell = ptr::null_mut();

            ec = match_eclasses_to_foreign_key_col(root, fkinfo, colno);
            // Don't bother looking for loose quals if we got an EC match
            if !ec.is_null() {
                (*fkinfo).nmatched_ec += 1;
                if (*ec)._opaque != 0 { // ec_has_const placeholder check
                    (*fkinfo).nconst_ec += 1;
                }
                colno += 1;
                continue;
            }

            /*
             * Scan joininfo list for relevant clauses.  Either rel's joininfo
             * list would do equally well; we use con_rel's.
             */
            con_attno = *(*fkinfo).conkey.as_ptr().add(colno as usize);
            ref_attno = *(*fkinfo).confkey.as_ptr().add(colno as usize);
            fpeqop = InvalidOid; // we'll look this up only if needed

            foreach!(lc2, (*cr).joininfo, {
                let rinfo = lfirst(crate::current_cell!(lc2)) as *mut RestrictInfo;
                let clause = (*rinfo).clause as *mut OpExpr;
                let mut leftvar: *mut Var;
                let mut rightvar: *mut Var;

                // Only binary OpExprs are useful for consideration
                if !IsA!(clause as *mut Node, T_OpExpr)
                    || list_length((*clause).args) != 2
                {
                    continue;
                }
                leftvar = get_leftop((*rinfo).clause as *mut Expr) as *mut Var;
                rightvar = get_rightop((*rinfo).clause as *mut Expr) as *mut Var;

                // Operands must be Vars, possibly with RelabelType
                while !leftvar.is_null() && IsA!(leftvar as *mut Node, T_RelabelType) {
                    leftvar = (*(leftvar as *mut RelabelType)).arg as *mut Var;
                }
                if leftvar.is_null() || !IsA!(leftvar as *mut Node, T_Var) {
                    continue;
                }
                while !rightvar.is_null() && IsA!(rightvar as *mut Node, T_RelabelType) {
                    rightvar = (*(rightvar as *mut RelabelType)).arg as *mut Var;
                }
                if rightvar.is_null() || !IsA!(rightvar as *mut Node, T_Var) {
                    continue;
                }

                // Now try to match the vars to the current foreign key cols
                if (*fkinfo).ref_relid == (*leftvar).varno as Index
                    && ref_attno == (*leftvar).varattno
                    && (*fkinfo).con_relid == (*rightvar).varno as Index
                    && con_attno == (*rightvar).varattno
                {
                    // Vars match, but is it the right operator?
                    if (*clause).opno == *(*fkinfo).conpfeqop.as_ptr().add(colno as usize) {
                        *(*fkinfo).rinfos.as_mut_ptr().add(colno as usize) = lappend(
                            *(*fkinfo).rinfos.as_ptr().add(colno as usize),
                            rinfo as *mut _,
                        );
                        (*fkinfo).nmatched_ri += 1;
                    }
                } else if (*fkinfo).ref_relid == (*rightvar).varno as Index
                    && ref_attno == (*rightvar).varattno
                    && (*fkinfo).con_relid == (*leftvar).varno as Index
                    && con_attno == (*leftvar).varattno
                {
                    /*
                     * Reverse match, must check commutator operator.  Look it
                     * up if we didn't already.  (In the worst case we might
                     * do multiple lookups here, but that would require an FK
                     * equality operator without commutator, which is
                     * unlikely.)
                     */
                    if !OidIsValid(fpeqop) {
                        fpeqop = get_commutator(*(*fkinfo).conpfeqop.as_ptr().add(colno as usize));
                    }
                    if (*clause).opno == fpeqop {
                        *(*fkinfo).rinfos.as_mut_ptr().add(colno as usize) = lappend(
                            *(*fkinfo).rinfos.as_ptr().add(colno as usize),
                            rinfo as *mut _,
                        );
                        (*fkinfo).nmatched_ri += 1;
                    }
                }
            });

            // If we found any matching loose quals, count col as matched
            if !(*(*fkinfo).rinfos.as_ptr().add(colno as usize)).is_null() {
                (*fkinfo).nmatched_rcols += 1;
            }
            colno += 1;
        }

        /*
         * Currently, we drop multicolumn FKs that aren't fully matched to the
         * query.  Later we might figure out how to derive some sort of
         * estimate from them, in which case this test should be weakened to
         * "if ((fkinfo->nmatched_ec + fkinfo->nmatched_rcols) > 0)".
         */
        if ((*fkinfo).nmatched_ec + (*fkinfo).nmatched_rcols) == (*fkinfo).nkeys {
            newlist = lappend(newlist, fkinfo as *mut _);
        }
    });

    // Replace fkey_list, thereby discarding any useless entries
    (*root).fkey_list = newlist;
}


/*****************************************************************************
 *
 *   CHECKS FOR MERGEJOINABLE AND HASHJOINABLE CLAUSES
 *
 *****************************************************************************/

/*
 * check_mergejoinable
 *   If the restrictinfo's clause is mergejoinable, set the mergejoin
 *   info fields in the restrictinfo.
 *
 *   Currently, we support mergejoin for binary opclauses where
 *   the operator is a mergejoinable operator.  The arguments can be
 *   anything --- as long as there are no volatile functions in them.
 */
unsafe fn check_mergejoinable(restrictinfo: *mut RestrictInfo) {
    let clause = (*restrictinfo).clause;
    let opno: Oid;
    let leftarg: *mut Node;

    if (*restrictinfo).pseudoconstant {
        return;
    }
    if !is_opclause(clause) {
        return;
    }
    if list_length((*(clause as *mut OpExpr)).args) != 2 {
        return;
    }

    opno = (*(clause as *mut OpExpr)).opno;
    leftarg = linitial((*(clause as *mut OpExpr)).args) as *mut Node;

    if op_mergejoinable(opno, exprType(leftarg as *const Node))
        && !contain_volatile_functions(restrictinfo as *mut Node)
    {
        (*restrictinfo).mergeopfamilies = get_mergejoin_opfamilies(opno);
    }

    /*
     * Note: op_mergejoinable is just a hint; if we fail to find the operator
     * in any btree opfamilies, mergeopfamilies remains NIL and so the clause
     * is not treated as mergejoinable.
     */
}

/*
 * check_hashjoinable
 *   If the restrictinfo's clause is hashjoinable, set the hashjoin
 *   info fields in the restrictinfo.
 *
 *   Currently, we support hashjoin for binary opclauses where
 *   the operator is a hashjoinable operator.  The arguments can be
 *   anything --- as long as there are no volatile functions in them.
 */
unsafe fn check_hashjoinable(restrictinfo: *mut RestrictInfo) {
    let clause = (*restrictinfo).clause;
    let opno: Oid;
    let leftarg: *mut Node;

    if (*restrictinfo).pseudoconstant {
        return;
    }
    if !is_opclause(clause) {
        return;
    }
    if list_length((*(clause as *mut OpExpr)).args) != 2 {
        return;
    }

    opno = (*(clause as *mut OpExpr)).opno;
    leftarg = linitial((*(clause as *mut OpExpr)).args) as *mut Node;

    if op_hashjoinable(opno, exprType(leftarg as *const Node))
        && !contain_volatile_functions(restrictinfo as *mut Node)
    {
        (*restrictinfo).hashjoinoperator = opno;
    }
}

/*
 * check_memoizable
 *   If the restrictinfo's clause is suitable to be used for a Memoize node,
 *   set the left_hasheqoperator and right_hasheqoperator to the hash equality
 *   operator that will be needed during caching.
 */
unsafe fn check_memoizable(restrictinfo: *mut RestrictInfo) {
    let typentry: *mut TypeCacheEntry;
    let clause = (*restrictinfo).clause;
    let lefttype: Oid;
    let righttype: Oid;

    if (*restrictinfo).pseudoconstant {
        return;
    }
    if !is_opclause(clause) {
        return;
    }
    if list_length((*(clause as *mut OpExpr)).args) != 2 {
        return;
    }

    lefttype = exprType(
        linitial((*(clause as *mut OpExpr)).args) as *const Node,
    );

    let te = lookup_type_cache(lefttype, TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

    if OidIsValid((*te).hash_proc) && OidIsValid((*te).eq_opr) {
        (*restrictinfo).left_hasheqoperator = (*te).eq_opr;
    }

    righttype = exprType(
        crate::nodes::pg_list::lsecond((*(clause as *mut OpExpr)).args) as *const Node,
    );

    /*
     * Lookup the right type, unless it's the same as the left type, in which
     * case typentry is already pointing to the required TypeCacheEntry.
     */
    let te2: *mut TypeCacheEntry;
    if lefttype != righttype {
        te2 = lookup_type_cache(righttype, TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);
    } else {
        te2 = te;
    }

    if OidIsValid((*te2).hash_proc) && OidIsValid((*te2).eq_opr) {
        (*restrictinfo).right_hasheqoperator = (*te2).eq_opr;
    }
}

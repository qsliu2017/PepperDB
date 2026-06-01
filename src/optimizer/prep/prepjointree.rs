/*-------------------------------------------------------------------------
 *
 * prepjointree.c
 *    Planner preprocessing for subqueries and join tree manipulation.
 *
 * NOTE: the intended sequence for invoking these operations is
 *        replace_empty_jointree
 *        pull_up_sublinks
 *        preprocess_function_rtes
 *        expand_virtual_generated_columns
 *        pull_up_subqueries
 *        flatten_simple_union_all
 *        do expression preprocessing (including flattening JOIN alias vars)
 *        reduce_outer_joins
 *        remove_useless_result_rtes
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/optimizer/prep/prepjointree.c
 *
 *-------------------------------------------------------------------------
 */

//! Translation of postgres/src/backend/optimizer/prep/prepjointree.c
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/table.h"              -> table_open/table_close (TODO stubs)
//!   "catalog/pg_type.h"           -> type OIDs (TODO stubs)
//!   "funcapi.h"                   -> get_expr_result_type (TODO stub)
//!   "miscadmin.h"                 -> check_stack_depth (TODO stub)
//!   "nodes/makefuncs.h"           -> makeNode, makeAlias, makeFromExpr, etc.
//!   "nodes/multibitmapset.h"      -> mbms_add_members, mbms_overlap_sets (TODO stubs)
//!   "nodes/nodeFuncs.h"           -> expression_tree_walker, query_tree_walker, etc.
//!   "optimizer/clauses.h"         -> contain_volatile_functions, etc. (TODO stubs)
//!   "optimizer/optimizer.h"       -> public fn signatures
//!   "optimizer/placeholder.h"     -> make_placeholder_expr (TODO stub)
//!   "optimizer/prep.h"            -> public fn signatures
//!   "optimizer/subselect.h"       -> convert_ANY_sublink_to_join, etc. (TODO stubs)
//!   "optimizer/tlist.h"           -> tlist_same_datatypes (TODO stub)
//!   "parser/parse_relation.h"     -> makeWholeRowVar, rt_fetch (TODO stubs)
//!   "parser/parsetree.h"          -> rt_fetch macro
//!   "rewrite/rewriteHandler.h"    -> inline_set_returning_function (TODO stub)
//!   "rewrite/rewriteManip.h"      -> OffsetVarNodes, etc. (TODO stubs)
//!   "utils/rel.h"                 -> RelationGetDescr (TODO stub)
//!
//! TODO(pg-port): all extern stubs below require porting of their respective
//! source files before this module can run.

use crate::prelude::*;
use core::ffi::c_void;
use core::mem;

use crate::nodes::pg_list::{
    lappend, lfirst, linitial, list_concat, list_length, List, NIL,
};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{
    T_Const, T_FromExpr, T_JoinExpr, T_PlaceHolderVar, T_Query, T_RangeTblRef, T_SetOperationStmt,
    T_SpecialJoinInfo, T_SubLink,
};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RangeTblFunction,
    SetOperationStmt, TableSampleClause,
};
use crate::nodes::pathnodes::{AppendRelInfo, PlannerInfo, Relids};
use crate::nodes::nodes::JoinType;
use crate::nodes::primnodes::{
    Expr, FromExpr, JoinExpr, MergeAction, NullTest, RangeTblRef, SubLink, TableFunc,
    TargetEntry, Var, IS_NOT_NULL,
};
use crate::nodes::pathnodes::PlaceHolderVar;
use crate::nodes::nodes::nodeTag;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::nodes::primnodes::AttrNumber;
use crate::c::Index;
use crate::access::attnum::InvalidAttrNumber;
use crate::{foreach, list_make1, makeNode, Assert, IsA};

// ---------------------------------------------------------------------------
// Local types
// ---------------------------------------------------------------------------

/// For each leaf RTE, nullingrels[rti] is the set of relids of outer joins
/// that potentially null that RTE.
struct NullingrelInfo {
    nullingrels: *mut Relids,
    /// Length of range table (maximum index in nullingrels[]).
    /// Used only for assertion checks.
    rtlength: i32,
}

/// Options for wrapping an expression for identification purposes.
#[allow(non_camel_case_types)]
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplaceWrapOption {
    REPLACE_WRAP_NONE = 0,    /* no expressions need to be wrapped */
    REPLACE_WRAP_ALL = 1,     /* all expressions need to be wrapped */
    REPLACE_WRAP_VARFREE = 2, /* variable-free expressions need to be wrapped */
}
use ReplaceWrapOption::*;

struct PullupReplaceVarsContext {
    root: *mut PlannerInfo,
    targetlist: *mut List,       /* tlist of subquery being pulled up */
    target_rte: *mut RangeTblEntry, /* RTE of subquery */
    result_relation: i32,        /* the index of the result relation in the rewritten query */
    relids: Relids,              /* relids within subquery, as numbered after pullup */
    nullinfo: *mut NullingrelInfo, /* per-RTE nullingrel info */
    outer_hasSubLinks: *mut bool, /* -> outer query's hasSubLinks */
    varno: i32,                  /* varno of subquery */
    wrap_option: ReplaceWrapOption,
    rv_cache: *mut *mut Node,    /* cache for results with PHVs */
}

struct ReduceOuterJoinsPass1State {
    relids: Relids,
    contains_outer: bool,
    sub_states: *mut List,
}

struct ReduceOuterJoinsPass2State {
    inner_reduced: Relids,
    partial_reduced: *mut List,
}

struct ReduceOuterJoinsPartialState {
    full_join_rti: i32,
    unreduced_side: Relids,
}

// Walker context structs (defined local to the walker sections).
struct FindDependentPhvsContext {
    relids: Relids,
    sublevels_up: i32,
}

struct SubstitutePhvRelidsContext {
    varno: i32,
    sublevels_up: i32,
    subrelids: Relids,
}

// ---------------------------------------------------------------------------
// TODO(pg-port) stubs -- replace when the upstream modules are ported
// ---------------------------------------------------------------------------

extern "C" {
    // miscadmin.h
    fn check_stack_depth();
    fn CHECK_FOR_INTERRUPTS();

    // nodes/makefuncs.h
    fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut crate::nodes::primnodes::Alias;
    fn makeFromExpr(fromlist: *mut List, quals: *mut Node) -> *mut FromExpr;
    fn makeTargetEntry(
        expr: *mut Expr,
        resno: AttrNumber,
        resname: *mut c_char,
        resjunk: bool,
    ) -> *mut TargetEntry;
    fn makeVar(
        varno: Index,
        varattno: AttrNumber,
        vartype: Oid,
        vartypmod: i32,
        varcollid: Oid,
        varlevelsup: Index,
    ) -> *mut Var;
    fn makeVarFromTargetEntry(varno: Index, tle: *mut TargetEntry) -> *mut Var;
    fn makeWholeRowVar(
        rte: *mut RangeTblEntry,
        varno: Index,
        varlevelsup: Index,
        sublink_ok: bool,
    ) -> *mut Var;
    fn make_and_qual(qual1: *mut Node, qual2: *mut Node) -> *mut Node;
    fn makeFromExprFromRTE(fromlist: *mut List, quals: *mut Node) -> *mut FromExpr; // alias

    // parser/parsetree.h
    fn rt_fetch(rti: i32, rtable: *mut List) -> *mut RangeTblEntry;

    // parser/parse_relation.h
    fn CombineRangeTables(
        dst_rtable: *mut *mut List,
        dst_perminfos: *mut *mut List,
        new_rtable: *mut List,
        new_perminfos: *mut List,
    );

    // rewrite/rewriteManip.h
    fn OffsetVarNodes(node: *mut Node, offset: i32, sublevels_up: i32);
    fn IncrementVarSublevelsUp(node: *mut Node, delta_sublevels_up: i32, min_sublevels_up: i32);
    fn IncrementVarSublevelsUp_rtable(rtable: *mut List, delta: i32, min: i32);
    fn ChangeVarNodes(node: *mut Node, rt_old: i32, rt_new: i32, sublevels_up: i32);
    fn add_nulling_relids(node: *mut Node, removable_rels: Relids, nulling_rels: Relids) -> *mut Node;
    fn remove_nulling_relids(
        node: *mut Node,
        removable_rels: Relids,
        except_relids: Relids,
    ) -> *mut Node;
    fn flatten_join_alias_vars(
        root: *mut PlannerInfo,
        query: *mut Query,
        node: *mut Node,
    ) -> *mut Node;

    // optimizer/subselect.h
    fn convert_VALUES_to_ANY(
        root: *mut PlannerInfo,
        testexpr: *mut Node,
        subselect: *mut Query,
    ) -> *mut crate::nodes::primnodes::ScalarArrayOpExpr;
    fn convert_ANY_sublink_to_join(
        root: *mut PlannerInfo,
        sublink: *mut SubLink,
        available_rels: Relids,
    ) -> *mut JoinExpr;
    fn convert_EXISTS_sublink_to_join(
        root: *mut PlannerInfo,
        sublink: *mut SubLink,
        under_not: bool,
        available_rels: Relids,
    ) -> *mut JoinExpr;

    // optimizer/clauses.h
    fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node;
    fn contain_volatile_functions(node: *mut Node) -> bool;
    fn contain_nonstrict_functions(node: *mut Node) -> bool;
    fn contain_vars_of_level(node: *mut Node, levelsup: i32) -> bool;
    fn expression_returns_set(node: *mut Node) -> bool;
    fn find_nonnullable_rels(clause: *mut Node) -> Relids;
    fn find_nonnullable_vars(clause: *mut Node) -> *mut List;
    fn find_forced_null_vars(clause: *mut Node) -> *mut List;

    // optimizer/placeholder.h
    fn make_placeholder_expr(
        root: *mut PlannerInfo,
        expr: *mut Expr,
        phrels: Relids,
    ) -> *mut PlaceHolderVar;

    // optimizer/tlist.h
    fn tlist_same_datatypes(tlist: *mut List, colTypes: *mut List, junkOK: bool) -> bool;

    // optimizer/prep.h (other prep fns, used via pull_up_subqueries deps)
    fn replace_rte_variables(
        node: *mut Node,
        target_varno: i32,
        sublevels_up: i32,
        callback: Option<
            unsafe extern "C" fn(*mut Var, *mut ReplaceRteVariablesContext) -> *mut Node,
        >,
        callback_arg: *mut c_void,
        outer_hasSubLinks: *mut bool,
    ) -> *mut Node;
    fn ReplaceVarFromTargetList(
        var: *mut Var,
        target_rte: *mut RangeTblEntry,
        targetlist: *mut List,
        result_relation: i32,
        nomatch_option: i32,
        levelsup: i32,
    ) -> *mut Node;

    // rewrite/rewriteHandler.h
    fn inline_set_returning_function(
        root: *mut PlannerInfo,
        rte: *mut RangeTblEntry,
    ) -> *mut Query;

    // access/table.h
    fn table_open(relid: Oid, lockmode: i32) -> *mut Relation;
    fn table_close(relation: *mut Relation, lockmode: i32);

    // utils/rel.h (RelationGetDescr is a macro -> function stub)
    fn RelationGetDescr(relation: *mut Relation) -> *mut TupleDesc;

    // catalog/pg_type.h helpers  (also used inline via build_generation_expression)
    fn build_generation_expression(rel: *mut Relation, attno: i32) -> *mut Node;

    // funcapi.h
    fn get_expr_result_type(
        expr: *mut Node,
        resultTypeId: *mut Oid,
        resultTupleDesc: *mut *mut TupleDesc,
    ) -> TypeFuncClass;

    // nodeFuncs.h walkers
    fn expression_tree_walker(
        node: *mut Node,
        walker: Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> bool>,
        context: *mut c_void,
    ) -> bool;
    fn query_tree_walker(
        query: *mut Query,
        walker: Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> bool>,
        context: *mut c_void,
        flags: i32,
    ) -> bool;
    fn query_or_expression_tree_walker(
        node: *mut Node,
        walker: Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> bool>,
        context: *mut c_void,
        flags: i32,
    ) -> bool;
    fn range_table_entry_walker(
        rte: *mut RangeTblEntry,
        walker: Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> bool>,
        context: *mut c_void,
        flags: i32,
    ) -> bool;

    // bitmapset.h
    fn bms_make_singleton(x: i32) -> Relids;
    fn bms_add_member(a: Relids, x: i32) -> Relids;
    fn bms_del_member(a: Relids, x: i32) -> Relids;
    fn bms_add_members(a: Relids, b: Relids) -> Relids;
    fn bms_del_members(a: Relids, b: Relids) -> Relids;
    fn bms_union(a: Relids, b: Relids) -> Relids;
    fn bms_join(a: Relids, b: Relids) -> Relids;
    fn bms_copy(a: Relids) -> Relids;
    fn bms_intersect(a: Relids, b: Relids) -> Relids;
    fn bms_overlap(a: Relids, b: Relids) -> bool;
    fn bms_is_empty(a: Relids) -> bool;
    fn bms_is_member(x: i32, a: Relids) -> bool;
    fn bms_is_subset(a: Relids, b: Relids) -> bool;
    fn bms_equal(a: Relids, b: Relids) -> bool;
    fn bms_free(a: Relids);
    fn bms_singleton_member(a: Relids) -> i32;
    fn bms_next_member(a: Relids, prevbit: i32) -> i32;

    // multibitmapset.h
    fn mbms_add_members(a: *mut List, b: *mut List) -> *mut List;
    fn mbms_overlap_sets(a: *mut List, b: *mut List) -> Relids;

    // palloc
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;

    // optimizer/optimizer.h -- pull_varnos family
    fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> Relids;
    fn pull_varnos_of_level(root: *mut PlannerInfo, node: *mut Node, levelsup: i32) -> Relids;

    // copyObject
    fn copyObject(obj: *mut c_void) -> *mut c_void;
}

// Opaque C types referenced but not fully translated yet.
#[repr(C)]
pub struct Relation {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct TupleDesc {
    pub natts: i32,
    pub constr: *mut TupleConstr,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct TupleConstr {
    pub has_generated_virtual: bool,
    _opaque: [u8; 0],
}
// pg_attribute form
pub type Form_pg_attribute = *mut PgAttribute;
#[repr(C)]
pub struct PgAttribute {
    pub attgenerated: i8,
    pub atttypid: Oid,
    pub atttypmod: i32,
    pub attcollation: Oid,
    _opaque: [u8; 0],
}

const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

// funcapi.h
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TypeFuncClass {
    TYPEFUNC_SCALAR = 1,
    TYPEFUNC_COMPOSITE = 2,
    TYPEFUNC_RECORD = 3,
    TYPEFUNC_OTHER = 4,
}
use TypeFuncClass::*;

// ReplaceRteVariablesContext is opaque (used via callback)
#[repr(C)]
pub struct ReplaceRteVariablesContext {
    pub callback_arg: *mut c_void,
    _opaque: [u8; 0],
}

// REPLACEVARS_REPORT_ERROR constant
const REPLACEVARS_REPORT_ERROR: i32 = 0;

// NoLock
const NoLock: i32 = 0;

// TupleDescAttr
#[inline]
unsafe fn TupleDescAttr(tupdesc: *mut TupleDesc, i: i32) -> Form_pg_attribute {
    // In C: TupleDescAttr(tupdesc, i) = &(tupdesc)->attrs[i]
    // We model this as a raw pointer offset.
    // The actual layout of TupleDesc in pg has attrs[] as a flexible array
    // right after the fixed fields. We use a stub that calls a C helper.
    tuple_desc_attr_stub(tupdesc, i)
}

extern "C" {
    fn tuple_desc_attr_stub(tupdesc: *mut TupleDesc, i: i32) -> Form_pg_attribute;
    fn castNode_SetOperationStmt(node: *mut Node) -> *mut SetOperationStmt;
    fn linitial_node_RangeTblFunction(list: *mut List) -> *mut RangeTblFunction;
    fn makeWholeRowVar_simple(rte: *mut RangeTblEntry, varno: i32, varlevelsup: i32, sublink_ok: bool) -> *mut Var;
}

// IS_OUTER_JOIN macro
#[inline]
unsafe fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    use crate::nodes::nodes::JoinType::*;
    matches!(jointype, JOIN_LEFT | JOIN_FULL | JOIN_RIGHT | JOIN_ANTI)
}

// is_notclause / get_notclausearg -- nodeFuncs.h inline helpers
#[inline]
unsafe fn is_notclause(node: *mut Node) -> bool {
    if node.is_null() {
        return false;
    }
    if !IsA!(node, T_BoolExpr) {
        return false;
    }
    let boolexpr = node as *mut crate::nodes::primnodes::BoolExpr;
    (*boolexpr).boolop == crate::nodes::primnodes::NOT_EXPR
}

#[inline]
unsafe fn get_notclausearg(notclause: *mut Expr) -> *mut Expr {
    crate::nodes::pg_list::linitial((*( notclause as *mut crate::nodes::primnodes::BoolExpr)).args) as *mut Expr
}

#[inline]
unsafe fn is_andclause(node: *mut Node) -> bool {
    if node.is_null() {
        return false;
    }
    if !IsA!(node, T_BoolExpr) {
        return false;
    }
    let boolexpr = node as *mut crate::nodes::primnodes::BoolExpr;
    (*boolexpr).boolop == crate::nodes::primnodes::AND_EXPR
}

// make_andclause -- from makefuncs
extern "C" {
    fn make_andclause(clauses: *mut List) -> *mut crate::nodes::primnodes::BoolExpr;
}

// elog / ereport helpers (already in prelude via macros)
extern "C" {
    fn elog_error(fmt: *const c_char, ...) -> !;
}
macro_rules! elog {
    (ERROR, $fmt:literal $(, $arg:expr)*) => {
        unsafe {
            let s = concat!($fmt, "\0");
            elog_error(s.as_ptr() as *const c_char $(, $arg)*)
        }
    };
}

// ===========================================================================
// Part 2: transform_MERGE_to_join, replace_empty_jointree, pull_up_sublinks,
//         pull_up_sublinks_jointree_recurse, pull_up_sublinks_qual_recurse
// ===========================================================================

/*
 * transform_MERGE_to_join
 *        Replace a MERGE's jointree to also include the target relation.
 */
pub unsafe extern "C" fn transform_MERGE_to_join(parse: *mut Query) {
    use crate::nodes::nodes::JoinType::*;
    use crate::nodes::nodes::CmdType::*;
    use crate::nodes::primnodes::MergeMatchKind::*;

    if (*parse).commandType != CMD_MERGE {
        return;
    }

    /* XXX probably bogus */
    let vars: *mut List = NIL;

    /*
     * Work out what kind of join is required.  If there any WHEN NOT MATCHED
     * BY SOURCE/TARGET actions, an outer join is required so that we process
     * all unmatched tuples from the source and/or target relations.
     * Otherwise, we can use an inner join.
     */
    let mut have_action_matched = false;
    let mut have_action_not_matched_by_source = false;
    let mut have_action_not_matched_by_target = false;

    let mut lc = crate::nodes::pg_list::list_head((*parse).mergeActionList);
    while !lc.is_null() {
        let action = crate::nodes::pg_list::lfirst(lc) as *mut MergeAction;
        if (*action).commandType != CMD_NOTHING {
            match (*action).matchKind {
                MERGE_WHEN_MATCHED => have_action_matched = true,
                MERGE_WHEN_NOT_MATCHED_BY_SOURCE => have_action_not_matched_by_source = true,
                MERGE_WHEN_NOT_MATCHED_BY_TARGET => have_action_not_matched_by_target = true,
            }
        }
        lc = crate::nodes::pg_list::lnext((*parse).mergeActionList, lc);
    }

    let jointype: JoinType;
    if have_action_not_matched_by_source && have_action_not_matched_by_target {
        jointype = JOIN_FULL;
    } else if have_action_not_matched_by_source {
        jointype = JOIN_LEFT;
    } else if have_action_not_matched_by_target {
        jointype = JOIN_RIGHT;
    } else {
        jointype = JOIN_INNER;
    }

    /* Manufacture a join RTE to use. */
    let joinrte: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*joinrte).rtekind = crate::nodes::parsenodes::RTEKind::RTE_JOIN;
    (*joinrte).jointype = jointype;
    (*joinrte).joinmergedcols = 0;
    (*joinrte).joinaliasvars = vars;
    (*joinrte).joinleftcols = NIL;   /* MERGE does not allow JOIN USING */
    (*joinrte).joinrightcols = NIL;  /* ditto */
    (*joinrte).join_using_alias = core::ptr::null_mut();

    (*joinrte).alias = core::ptr::null_mut();
    (*joinrte).eref = makeAlias(b"*MERGE*\0".as_ptr() as *const c_char, NIL);
    (*joinrte).lateral = false;
    (*joinrte).inh = false;
    (*joinrte).inFromCl = true;

    /*
     * Add completed RTE to pstate's range table list, so that we know its
     * index.
     */
    (*parse).rtable = lappend((*parse).rtable, joinrte as *mut c_void);
    let joinrti = list_length((*parse).rtable);

    /*
     * Create a JOIN between the target and the source relation.
     *
     * Here the target is identified by parse->mergeTargetRelation.  For a
     * regular table, this will equal parse->resultRelation, but for a
     * trigger-updatable view, it will be the expanded view subquery that we
     * need to pull data from.
     *
     * The source relation is in parse->jointree->fromlist, but any quals in
     * parse->jointree->quals are restrictions on the target relation (if the
     * target relation is an auto-updatable view).
     */
    /* target rel, with any quals */
    let rtr: *mut RangeTblRef = makeNode!(RangeTblRef, T_RangeTblRef);
    (*rtr).rtindex = (*parse).mergeTargetRelation;
    let target: *mut FromExpr =
        makeFromExpr(list_make1!(rtr as *mut c_void), (*(*parse).jointree).quals);

    /* source rel (expect exactly one -- see transformMergeStmt()) */
    Assert!(list_length((*(*parse).jointree).fromlist) == 1);
    let source: *mut Node = linitial((*(*parse).jointree).fromlist) as *mut Node;

    /*
     * index of source rel (expect either a RangeTblRef or a JoinExpr -- see
     * transformFromClauseItem()).
     */
    let sourcerti: i32;
    if IsA!(source, T_RangeTblRef) {
        sourcerti = (*(source as *mut RangeTblRef)).rtindex;
    } else if IsA!(source, T_JoinExpr) {
        sourcerti = (*(source as *mut JoinExpr)).rtindex;
    } else {
        panic!("unrecognized source node type: {}", nodeTag(source) as i32);
    }

    /* Join the source and target */
    let joinexpr: *mut JoinExpr = makeNode!(JoinExpr, T_JoinExpr);
    (*joinexpr).jointype = jointype;
    (*joinexpr).isNatural = false;
    (*joinexpr).larg = target as *mut Node;
    (*joinexpr).rarg = source;
    (*joinexpr).usingClause = NIL;
    (*joinexpr).join_using_alias = core::ptr::null_mut();
    (*joinexpr).quals = (*parse).mergeJoinCondition;
    (*joinexpr).alias = core::ptr::null_mut();
    (*joinexpr).rtindex = joinrti;

    /* Make the new join be the sole entry in the query's jointree */
    (*(*parse).jointree).fromlist = list_make1!(joinexpr as *mut c_void);
    (*(*parse).jointree).quals = core::ptr::null_mut();

    /*
     * If necessary, mark parse->targetlist entries that refer to the target
     * as nullable by the join.  Normally the targetlist will be empty for a
     * MERGE, but if the target is a trigger-updatable view, it will contain a
     * whole-row Var referring to the expanded view query.
     */
    if !(*parse).targetList.is_null()
        && (jointype == JOIN_RIGHT || jointype == JOIN_FULL)
    {
        (*parse).targetList = add_nulling_relids(
            (*parse).targetList as *mut Node,
            bms_make_singleton((*parse).mergeTargetRelation),
            bms_make_singleton(joinrti),
        ) as *mut List;
    }

    /*
     * If the source relation is on the outer side of the join, mark any
     * source relation Vars in the join condition, actions, and RETURNING list
     * as nullable by the join.
     */
    if jointype == JOIN_LEFT || jointype == JOIN_FULL {
        (*parse).mergeJoinCondition = add_nulling_relids(
            (*parse).mergeJoinCondition,
            bms_make_singleton(sourcerti),
            bms_make_singleton(joinrti),
        );

        let mut lc = crate::nodes::pg_list::list_head((*parse).mergeActionList);
        while !lc.is_null() {
            let action = crate::nodes::pg_list::lfirst(lc) as *mut MergeAction;
            (*action).qual = add_nulling_relids(
                (*action).qual,
                bms_make_singleton(sourcerti),
                bms_make_singleton(joinrti),
            );
            (*action).targetList = add_nulling_relids(
                (*action).targetList as *mut Node,
                bms_make_singleton(sourcerti),
                bms_make_singleton(joinrti),
            ) as *mut List;
            lc = crate::nodes::pg_list::lnext((*parse).mergeActionList, lc);
        }

        (*parse).returningList = add_nulling_relids(
            (*parse).returningList as *mut Node,
            bms_make_singleton(sourcerti),
            bms_make_singleton(joinrti),
        ) as *mut List;
    }

    /*
     * If there are any WHEN NOT MATCHED BY SOURCE actions, the executor will
     * use the join condition to distinguish between MATCHED and NOT MATCHED
     * BY SOURCE cases.  Otherwise, it's no longer needed, and we set it to
     * NULL.
     */
    if have_action_not_matched_by_source {
        /* source wholerow Var (nullable by the new join) */
        let var: *mut Var = makeWholeRowVar_simple(
            rt_fetch(sourcerti, (*parse).rtable),
            sourcerti,
            0,
            false,
        );
        (*var).varnullingrels = bms_make_singleton(joinrti);

        /* "src IS NOT NULL" check */
        let ntest: *mut NullTest = makeNode!(NullTest, T_NullTest);
        (*ntest).arg = var as *mut Expr;
        (*ntest).nulltesttype = IS_NOT_NULL;
        (*ntest).argisrow = false;
        (*ntest).location = -1;

        /* combine it with the original join condition */
        (*parse).mergeJoinCondition =
            make_and_qual(ntest as *mut Node, (*parse).mergeJoinCondition);
    } else {
        (*parse).mergeJoinCondition = core::ptr::null_mut(); /* join condition not needed */
    }
}

/*
 * replace_empty_jointree
 *        If the Query's jointree is empty, replace it with a dummy RTE_RESULT
 *        relation.
 */
pub unsafe extern "C" fn replace_empty_jointree(parse: *mut Query) {
    use crate::nodes::parsenodes::RTEKind::*;

    /* Nothing to do if jointree is already nonempty */
    if !(*(*parse).jointree).fromlist.is_null() {
        return;
    }

    /* We mustn't change it in the top level of a setop tree, either */
    if !(*parse).setOperations.is_null() {
        return;
    }

    /* Create suitable RTE */
    let rte: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*rte).rtekind = RTE_RESULT;
    (*rte).eref = makeAlias(b"*RESULT*\0".as_ptr() as *const c_char, NIL);

    /* Add it to rangetable */
    (*parse).rtable = lappend((*parse).rtable, rte as *mut c_void);
    let rti = list_length((*parse).rtable);

    /* And jam a reference into the jointree */
    let rtr: *mut RangeTblRef = makeNode!(RangeTblRef, T_RangeTblRef);
    (*rtr).rtindex = rti;
    (*(*parse).jointree).fromlist = list_make1!(rtr as *mut c_void);
}

/*
 * pull_up_sublinks
 *        Attempt to pull up ANY and EXISTS SubLinks to be treated as
 *        semijoins or anti-semijoins.
 */
pub unsafe extern "C" fn pull_up_sublinks(root: *mut PlannerInfo) {
    let mut relids: Relids = core::ptr::null_mut();

    /* Begin recursion through the jointree */
    let jtnode: *mut Node = pull_up_sublinks_jointree_recurse(
        root,
        (*(*root).parse).jointree as *mut Node,
        &mut relids,
    );

    /*
     * root->parse->jointree must always be a FromExpr, so insert a dummy one
     * if we got a bare RangeTblRef or JoinExpr out of the recursion.
     */
    if IsA!(jtnode, T_FromExpr) {
        (*(*root).parse).jointree = jtnode as *mut FromExpr;
    } else {
        (*(*root).parse).jointree = makeFromExpr(list_make1!(jtnode as *mut c_void), core::ptr::null_mut());
    }
}

/*
 * Recurse through jointree nodes for pull_up_sublinks()
 *
 * In addition to returning the possibly-modified jointree node, we return
 * a relids set of the contained rels into *relids.
 */
unsafe fn pull_up_sublinks_jointree_recurse(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    relids: *mut Relids,
) -> *mut Node {
    /* Since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if jtnode.is_null() {
        *relids = core::ptr::null_mut();
    } else if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        *relids = bms_make_singleton(varno);
        /* jtnode is returned unmodified */
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let mut newfromlist: *mut List = NIL;
        let mut frelids: Relids = core::ptr::null_mut();
        let mut childrelids: Relids = core::ptr::null_mut();

        /* First, recurse to process children and collect their relids */
        let mut lc = crate::nodes::pg_list::list_head((*f).fromlist);
        while !lc.is_null() {
            let newchild = pull_up_sublinks_jointree_recurse(
                root,
                crate::nodes::pg_list::lfirst(lc) as *mut Node,
                &mut childrelids,
            );
            newfromlist = lappend(newfromlist, newchild as *mut c_void);
            frelids = bms_join(frelids, childrelids);
            lc = crate::nodes::pg_list::lnext((*f).fromlist, lc);
        }
        /* Build the replacement FromExpr; no quals yet */
        let newf: *mut FromExpr = makeFromExpr(newfromlist, core::ptr::null_mut());
        /* Set up a link representing the rebuilt jointree */
        let mut jtlink: *mut Node = newf as *mut Node;
        /* Now process qual --- all children are available for use */
        (*newf).quals = pull_up_sublinks_qual_recurse(
            root,
            (*f).quals,
            &mut jtlink,
            frelids,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );

        /*
         * Note that the result will be either newf, or a stack of JoinExprs
         * with newf at the base.  We rely on subsequent optimization steps to
         * flatten this and rearrange the joins as needed.
         *
         * Although we could include the pulled-up subqueries in the returned
         * relids, there's no need since upper quals couldn't refer to their
         * outputs anyway.
         */
        *relids = frelids;
        return jtlink;
    } else if IsA!(jtnode, T_JoinExpr) {
        use crate::nodes::nodes::JoinType::*;

        /*
         * Make a modifiable copy of join node, but don't bother copying its
         * subnodes (yet).
         */
        let j: *mut JoinExpr = palloc(mem::size_of::<JoinExpr>()) as *mut JoinExpr;
        core::ptr::copy_nonoverlapping(jtnode as *const JoinExpr, j, 1);
        let mut jtlink: *mut Node = j as *mut Node;

        let mut leftrelids: Relids = core::ptr::null_mut();
        let mut rightrelids: Relids = core::ptr::null_mut();

        /* Recurse to process children and collect their relids */
        (*j).larg = pull_up_sublinks_jointree_recurse(root, (*j).larg, &mut leftrelids);
        (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut rightrelids);

        /*
         * Now process qual, showing appropriate child relids as available,
         * and attach any pulled-up jointree items at the right place.
         *
         * We don't expect to see any pre-existing JOIN_SEMI, JOIN_ANTI,
         * JOIN_RIGHT_SEMI, or JOIN_RIGHT_ANTI jointypes here.
         */
        match (*j).jointype {
            JOIN_INNER => {
                (*j).quals = pull_up_sublinks_qual_recurse(
                    root,
                    (*j).quals,
                    &mut jtlink,
                    bms_union(leftrelids, rightrelids),
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );
            }
            JOIN_LEFT => {
                (*j).quals = pull_up_sublinks_qual_recurse(
                    root,
                    (*j).quals,
                    &mut (*j).rarg,
                    rightrelids,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );
            }
            JOIN_FULL => {
                /* can't do anything with full-join quals */
            }
            JOIN_RIGHT => {
                (*j).quals = pull_up_sublinks_qual_recurse(
                    root,
                    (*j).quals,
                    &mut (*j).larg,
                    leftrelids,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );
            }
            _ => {
                panic!("unrecognized join type: {}", (*j).jointype as i32);
            }
        }

        /*
         * Although we could include the pulled-up subqueries in the returned
         * relids, there's no need since upper quals couldn't refer to their
         * outputs anyway.  But we *do* need to include the join's own rtindex
         * because we haven't yet collapsed join alias variables, so upper
         * levels would mistakenly think they couldn't use references to this
         * join.
         */
        *relids = bms_join(leftrelids, rightrelids);
        if (*j).rtindex != 0 {
            *relids = bms_add_member(*relids, (*j).rtindex);
        }
        return jtlink;
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    jtnode
}

/*
 * Recurse through top-level qual nodes for pull_up_sublinks()
 *
 * jtlink1 points to the link in the jointree where any new JoinExprs should
 * be inserted if they reference available_rels1.
 *
 * Returns the replacement qual node, or NULL if the qual should be removed.
 */
unsafe fn pull_up_sublinks_qual_recurse(
    root: *mut PlannerInfo,
    node: *mut Node,
    jtlink1: *mut *mut Node,
    available_rels1: Relids,
    jtlink2: *mut *mut Node,
    available_rels2: Relids,
) -> *mut Node {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(node, T_SubLink) {
        let sublink = node as *mut SubLink;
        let mut child_rels: Relids = core::ptr::null_mut();

        /* Is it a convertible ANY or EXISTS clause? */
        use crate::nodes::primnodes::SubLinkType::*;
        if (*sublink).subLinkType == ANY_SUBLINK {
            let saop = convert_VALUES_to_ANY(
                root,
                (*sublink).testexpr,
                (*sublink).subselect as *mut Query,
            );
            if !saop.is_null() {
                /*
                 * The VALUES sequence was simplified.  Nothing more to do
                 * here.
                 */
                return saop as *mut Node;
            }

            let mut j = convert_ANY_sublink_to_join(root, sublink, available_rels1);
            if !j.is_null() {
                /* Yes; insert the new join node into the join tree */
                (*j).larg = *jtlink1;
                *jtlink1 = j as *mut Node;
                /* Recursively process pulled-up jointree nodes */
                (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                /*
                 * Now recursively process the pulled-up quals.  Any inserted
                 * joins can get stacked onto either j->larg or j->rarg.
                 */
                (*j).quals = pull_up_sublinks_qual_recurse(
                    root, (*j).quals, &mut (*j).larg, available_rels1,
                    &mut (*j).rarg, child_rels,
                );
                /* Return NULL representing constant TRUE */
                return core::ptr::null_mut();
            }
            if !jtlink2.is_null() {
                j = convert_ANY_sublink_to_join(root, sublink, available_rels2);
                if !j.is_null() {
                    (*j).larg = *jtlink2;
                    *jtlink2 = j as *mut Node;
                    (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                    (*j).quals = pull_up_sublinks_qual_recurse(
                        root, (*j).quals, &mut (*j).larg, available_rels2,
                        &mut (*j).rarg, child_rels,
                    );
                    return core::ptr::null_mut();
                }
            }
        } else if (*sublink).subLinkType == EXISTS_SUBLINK {
            let mut j = convert_EXISTS_sublink_to_join(root, sublink, false, available_rels1);
            if !j.is_null() {
                (*j).larg = *jtlink1;
                *jtlink1 = j as *mut Node;
                (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                (*j).quals = pull_up_sublinks_qual_recurse(
                    root, (*j).quals, &mut (*j).larg, available_rels1,
                    &mut (*j).rarg, child_rels,
                );
                return core::ptr::null_mut();
            }
            if !jtlink2.is_null() {
                j = convert_EXISTS_sublink_to_join(root, sublink, false, available_rels2);
                if !j.is_null() {
                    (*j).larg = *jtlink2;
                    *jtlink2 = j as *mut Node;
                    (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                    (*j).quals = pull_up_sublinks_qual_recurse(
                        root, (*j).quals, &mut (*j).larg, available_rels2,
                        &mut (*j).rarg, child_rels,
                    );
                    return core::ptr::null_mut();
                }
            }
        }
        /* Else return it unmodified */
        return node;
    }
    if is_notclause(node) {
        /* If the immediate argument of NOT is EXISTS, try to convert */
        let sublink_candidate = get_notclausearg(node as *mut Expr) as *mut Node;
        let mut child_rels: Relids = core::ptr::null_mut();

        if !sublink_candidate.is_null() && IsA!(sublink_candidate, T_SubLink) {
            let sublink = sublink_candidate as *mut SubLink;
            use crate::nodes::primnodes::SubLinkType::*;
            if (*sublink).subLinkType == EXISTS_SUBLINK {
                let mut j = convert_EXISTS_sublink_to_join(root, sublink, true, available_rels1);
                if !j.is_null() {
                    (*j).larg = *jtlink1;
                    *jtlink1 = j as *mut Node;
                    (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                    /*
                     * Now recursively process the pulled-up quals.  Because
                     * we are underneath a NOT, we can't pull up sublinks that
                     * reference the left-hand stuff, but it's still okay to
                     * pull up sublinks referencing j->rarg.
                     */
                    (*j).quals = pull_up_sublinks_qual_recurse(
                        root, (*j).quals, &mut (*j).rarg, child_rels,
                        core::ptr::null_mut(), core::ptr::null_mut(),
                    );
                    return core::ptr::null_mut();
                }
                if !jtlink2.is_null() {
                    j = convert_EXISTS_sublink_to_join(root, sublink, true, available_rels2);
                    if !j.is_null() {
                        (*j).larg = *jtlink2;
                        *jtlink2 = j as *mut Node;
                        (*j).rarg = pull_up_sublinks_jointree_recurse(root, (*j).rarg, &mut child_rels);
                        (*j).quals = pull_up_sublinks_qual_recurse(
                            root, (*j).quals, &mut (*j).rarg, child_rels,
                            core::ptr::null_mut(), core::ptr::null_mut(),
                        );
                        return core::ptr::null_mut();
                    }
                }
            }
        }
        /* Else return it unmodified */
        return node;
    }
    if is_andclause(node) {
        /* Recurse into AND clause */
        let mut newclauses: *mut List = NIL;
        let boolexpr_args = (*(node as *mut crate::nodes::primnodes::BoolExpr)).args;
        let mut lc = crate::nodes::pg_list::list_head(boolexpr_args);
        while !lc.is_null() {
            let oldclause = crate::nodes::pg_list::lfirst(lc) as *mut Node;
            let newclause = pull_up_sublinks_qual_recurse(
                root, oldclause, jtlink1, available_rels1, jtlink2, available_rels2,
            );
            if !newclause.is_null() {
                newclauses = lappend(newclauses, newclause as *mut c_void);
            }
            lc = crate::nodes::pg_list::lnext(boolexpr_args, lc);
        }
        /* We might have got back fewer clauses than we started with */
        if newclauses.is_null() {
            return core::ptr::null_mut();
        } else if list_length(newclauses) == 1 {
            return linitial(newclauses) as *mut Node;
        } else {
            return make_andclause(newclauses) as *mut Node;
        }
    }
    /* Stop if not an AND */
    node
}

// ===========================================================================
// Part 3: preprocess_function_rtes, expand_virtual_generated_columns,
//         pull_up_subqueries, pull_up_subqueries_recurse,
//         pull_up_simple_subquery
// ===========================================================================

/*
 * preprocess_function_rtes
 *        Constant-simplify any FUNCTION RTEs in the FROM clause, and then
 *        attempt to "inline" any that are set-returning functions.
 */
pub unsafe extern "C" fn preprocess_function_rtes(root: *mut PlannerInfo) {
    use crate::nodes::parsenodes::RTEKind::*;

    let root_rtable = (*(*root).parse).rtable;
    let mut lc = crate::nodes::pg_list::list_head(root_rtable);
    while !lc.is_null() {
        let rte = crate::nodes::pg_list::lfirst(lc) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_FUNCTION {
            /* Apply const-simplification */
            (*rte).functions = eval_const_expressions(root, (*rte).functions as *mut Node) as *mut List;

            /* Check safety of expansion, and expand if possible */
            let funcquery = inline_set_returning_function(root, rte);
            if !funcquery.is_null() {
                /* Successful expansion, convert the RTE to a subquery */
                (*rte).rtekind = RTE_SUBQUERY;
                (*rte).subquery = funcquery;
                (*rte).security_barrier = false;

                /*
                 * Clear fields that should not be set in a subquery RTE.
                 * However, we leave rte->functions filled in for the moment,
                 * in case makeWholeRowVar needs to consult it.  We'll clear
                 * it in setrefs.c (see add_rte_to_flat_rtable) so that this
                 * abuse of the data structure doesn't escape the planner.
                 */
                (*rte).funcordinality = false;
            }
        }
        lc = crate::nodes::pg_list::lnext(root_rtable, lc);
    }
}

/*
 * expand_virtual_generated_columns
 *        Expand all virtual generated column references in a query.
 *
 * This scans the rangetable for relations with virtual generated columns, and
 * replaces all Var nodes in the query that reference these columns with the
 * generation expressions.  Note that we do not descend into subqueries; that
 * is taken care of when the subqueries are planned.
 *
 * Returns a modified copy of the query tree, if any relations with virtual
 * generated columns are present.
 */
pub unsafe extern "C" fn expand_virtual_generated_columns(root: *mut PlannerInfo) -> *mut Query {
    use crate::nodes::parsenodes::RTEKind::*;

    let parse = (*root).parse;
    let mut rt_index: i32 = 0;
    let mut parse_out: *mut Query = parse;

    let parse_rtable = (*parse).rtable;
    let mut lc = crate::nodes::pg_list::list_head(parse_rtable);
    while !lc.is_null() {
        let rte = crate::nodes::pg_list::lfirst(lc) as *mut RangeTblEntry;
        rt_index += 1;

        /*
         * Only normal relations can have virtual generated columns.
         */
        if (*rte).rtekind != RTE_RELATION {
            lc = crate::nodes::pg_list::lnext(parse_rtable, lc);
            continue;
        }

        let rel = table_open((*rte).relid, NoLock);
        let tupdesc = RelationGetDescr(rel);

        if !(*tupdesc).constr.is_null() && (*(*tupdesc).constr).has_generated_virtual {
            let mut tlist: *mut List = NIL;
            let mut rvcontext = PullupReplaceVarsContext {
                root: core::ptr::null_mut(),
                targetlist: core::ptr::null_mut(),
                target_rte: core::ptr::null_mut(),
                result_relation: 0,
                relids: core::ptr::null_mut(),
                nullinfo: core::ptr::null_mut(),
                outer_hasSubLinks: core::ptr::null_mut(),
                varno: 0,
                wrap_option: REPLACE_WRAP_NONE,
                rv_cache: core::ptr::null_mut(),
            };

            let mut i: i32 = 0;
            while i < (*tupdesc).natts {
                let attr = TupleDescAttr(tupdesc, i);
                let tle: *mut TargetEntry;

                if (*attr).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                    let defexpr = build_generation_expression(rel, i + 1);
                    ChangeVarNodes(defexpr, 1, rt_index, 0);

                    tle = makeTargetEntry(
                        defexpr as *mut Expr,
                        (i + 1) as AttrNumber,
                        core::ptr::null_mut(),
                        false,
                    );
                    tlist = lappend(tlist, tle as *mut c_void);
                } else {
                    let var = makeVar(
                        rt_index as Index,
                        (i + 1) as AttrNumber,
                        (*attr).atttypid,
                        (*attr).atttypmod,
                        (*attr).attcollation,
                        0,
                    );
                    tle = makeTargetEntry(
                        var as *mut Expr,
                        (i + 1) as AttrNumber,
                        core::ptr::null_mut(),
                        false,
                    );
                    tlist = lappend(tlist, tle as *mut c_void);
                }
                i += 1;
            }

            Assert!(list_length(tlist) > 0);
            Assert!(!(*rte).lateral);

            /*
             * The relation's targetlist items are now in the appropriate form
             * to insert into the query, except that we may need to wrap them
             * in PlaceHolderVars.  Set up required context data for
             * pullup_replace_vars.
             */
            rvcontext.root = root;
            rvcontext.targetlist = tlist;
            rvcontext.target_rte = rte;
            rvcontext.result_relation = (*parse_out).resultRelation;
            /* won't need these values */
            rvcontext.relids = core::ptr::null_mut();
            rvcontext.nullinfo = core::ptr::null_mut();
            /* pass NULL for outer_hasSubLinks */
            rvcontext.outer_hasSubLinks = core::ptr::null_mut();
            rvcontext.varno = rt_index;
            /* this flag will be set below, if needed */
            rvcontext.wrap_option = REPLACE_WRAP_NONE;
            /* initialize cache array with indexes 0 .. length(tlist) */
            rvcontext.rv_cache = palloc0(
                ((list_length(tlist) + 1) as usize) * mem::size_of::<*mut Node>(),
            ) as *mut *mut Node;

            /*
             * If the query uses grouping sets, we need a PlaceHolderVar for
             * each expression of the relation's targetlist items.
             */
            if !(*parse_out).groupingSets.is_null() {
                rvcontext.wrap_option = REPLACE_WRAP_ALL;
            }

            /*
             * Apply pullup variable replacement throughout the query tree.
             */
            parse_out = pullup_replace_vars(parse_out as *mut Node, &mut rvcontext) as *mut Query;
        }

        table_close(rel, NoLock);
        lc = crate::nodes::pg_list::lnext(parse_rtable, lc);
    }

    parse_out
}

/*
 * pull_up_subqueries
 *        Look for subqueries in the rangetable that can be pulled up into
 *        the parent query.
 */
pub unsafe extern "C" fn pull_up_subqueries(root: *mut PlannerInfo) {
    /* Top level of jointree must always be a FromExpr */
    Assert!(IsA!((*(*root).parse).jointree as *mut Node, T_FromExpr));
    /* Recursion starts with no containing join nor appendrel */
    (*(*root).parse).jointree = pull_up_subqueries_recurse(
        root,
        (*(*root).parse).jointree as *mut Node,
        core::ptr::null_mut(),
        core::ptr::null_mut(),
    ) as *mut FromExpr;
    /* We should still have a FromExpr */
    Assert!(IsA!((*(*root).parse).jointree as *mut Node, T_FromExpr));
}

/*
 * pull_up_subqueries_recurse
 *        Recursive guts of pull_up_subqueries.
 */
unsafe fn pull_up_subqueries_recurse(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    lowest_outer_join: *mut JoinExpr,
    containing_appendrel: *mut AppendRelInfo,
) -> *mut Node {
    use crate::nodes::parsenodes::RTEKind::*;
    use crate::nodes::nodes::JoinType::*;

    /* Since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();
    /* Also, since it's a bit expensive, let's check for query cancel. */
    CHECK_FOR_INTERRUPTS();

    Assert!(!jtnode.is_null());
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        let rte = rt_fetch(varno, (*(*root).parse).rtable);

        /*
         * Is this a subquery RTE, and if so, is the subquery simple enough to
         * pull up?
         *
         * If we are looking at an append-relation member, we can't pull it up
         * unless is_safe_append_member says so.
         */
        if (*rte).rtekind == RTE_SUBQUERY
            && is_simple_subquery(root, (*rte).subquery, rte, lowest_outer_join)
            && (containing_appendrel.is_null() || is_safe_append_member((*rte).subquery))
        {
            return pull_up_simple_subquery(
                root, jtnode, rte, lowest_outer_join, containing_appendrel,
            );
        }

        /*
         * Alternatively, is it a simple UNION ALL subquery?
         */
        if (*rte).rtekind == RTE_SUBQUERY && is_simple_union_all((*rte).subquery) {
            return pull_up_simple_union_all(root, jtnode, rte);
        }

        /*
         * Or perhaps it's a simple VALUES RTE?
         *
         * We don't allow VALUES pullup below an outer join nor into an
         * appendrel (such cases are impossible anyway at the moment).
         */
        if (*rte).rtekind == RTE_VALUES
            && lowest_outer_join.is_null()
            && containing_appendrel.is_null()
            && is_simple_values(root, rte)
        {
            return pull_up_simple_values(root, jtnode, rte);
        }

        /*
         * Or perhaps it's a FUNCTION RTE that we could inline?
         */
        if (*rte).rtekind == RTE_FUNCTION {
            return pull_up_constant_function(root, jtnode, rte, containing_appendrel);
        }

        /* Otherwise, do nothing at this node. */
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;

        Assert!(containing_appendrel.is_null());
        /* Recursively transform all the child nodes */
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            let child = crate::nodes::pg_list::lfirst(lc) as *mut Node;
            let new_child = pull_up_subqueries_recurse(
                root, child, lowest_outer_join, core::ptr::null_mut(),
            );
            (*lc).ptr_value = new_child as *mut c_void;
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;

        Assert!(containing_appendrel.is_null());
        /* Recurse, being careful to tell myself when inside outer join */
        match (*j).jointype {
            JOIN_INNER => {
                (*j).larg = pull_up_subqueries_recurse(
                    root, (*j).larg, lowest_outer_join, core::ptr::null_mut(),
                );
                (*j).rarg = pull_up_subqueries_recurse(
                    root, (*j).rarg, lowest_outer_join, core::ptr::null_mut(),
                );
            }
            JOIN_LEFT | JOIN_SEMI | JOIN_ANTI | JOIN_FULL | JOIN_RIGHT => {
                (*j).larg = pull_up_subqueries_recurse(
                    root, (*j).larg, j, core::ptr::null_mut(),
                );
                (*j).rarg = pull_up_subqueries_recurse(
                    root, (*j).rarg, j, core::ptr::null_mut(),
                );
            }
            _ => {
                panic!("unrecognized join type: {}", (*j).jointype as i32);
            }
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    jtnode
}

/*
 * pull_up_simple_subquery
 *        Attempt to pull up a single simple subquery.
 *
 * jtnode is a RangeTblRef that has been tentatively identified as a simple
 * subquery by pull_up_subqueries.
 */
unsafe fn pull_up_simple_subquery(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    rte: *mut RangeTblEntry,
    lowest_outer_join: *mut JoinExpr,
    containing_appendrel: *mut AppendRelInfo,
) -> *mut Node {
    use crate::nodes::parsenodes::RTEKind::*;

    let parse = (*root).parse;
    let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
    let mut subquery: *mut Query;
    let subroot: *mut PlannerInfo;
    let rtoffset: i32;
    let mut rvcontext: PullupReplaceVarsContext;

    /*
     * Make a modifiable copy of the subquery to hack on, so that the RTE will
     * be left unchanged in case we decide below that we can't pull it up
     * after all.
     */
    subquery = copyObject((*rte).subquery as *mut c_void) as *mut Query;

    /*
     * Create a PlannerInfo data structure for this subquery.
     *
     * NOTE: the next few steps should match the first processing in
     * subquery_planner().  Can we refactor to avoid code duplication, or
     * would that just make things uglier?
     */
    subroot = makeNode!(PlannerInfo, T_PlannerInfo);
    (*subroot).parse = subquery;
    (*subroot).glob = (*root).glob;
    (*subroot).query_level = (*root).query_level;
    (*subroot).parent_root = (*root).parent_root;
    (*subroot).plan_params = NIL;
    (*subroot).outer_params = core::ptr::null_mut();
    (*subroot).planner_cxt = CurrentMemoryContext as *mut c_void;
    (*subroot).init_plans = NIL;
    (*subroot).cte_plan_ids = NIL;
    (*subroot).multiexpr_params = NIL;
    (*subroot).join_domains = NIL;
    (*subroot).eq_classes = NIL;
    (*subroot).ec_merging_done = false;
    (*subroot).last_rinfo_serial = 0;
    (*subroot).all_result_relids = core::ptr::null_mut();
    (*subroot).leaf_result_relids = core::ptr::null_mut();
    (*subroot).append_rel_list = NIL;
    (*subroot).row_identity_vars = NIL;
    (*subroot).rowMarks = NIL;
    core::ptr::write_bytes((*subroot).upper_rels.as_mut_ptr(), 0, (*subroot).upper_rels.len());
    core::ptr::write_bytes((*subroot).upper_targets.as_mut_ptr(), 0, (*subroot).upper_targets.len());
    (*subroot).processed_groupClause = NIL;
    (*subroot).processed_distinctClause = NIL;
    (*subroot).processed_tlist = NIL;
    (*subroot).update_colnos = NIL;
    (*subroot).grouping_map = core::ptr::null_mut();
    (*subroot).minmax_aggs = NIL;
    (*subroot).qual_security_level = 0;
    (*subroot).placeholdersFrozen = false;
    (*subroot).hasRecursion = false;
    (*subroot).wt_param_id = -1;
    (*subroot).non_recursive_path = core::ptr::null_mut();
    /* We don't currently need a top JoinDomain for the subroot */

    /* No CTEs to worry about */
    Assert!((*subquery).cteList.is_null());

    /*
     * If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE, so
     * that we don't need so many special cases to deal with that situation.
     */
    replace_empty_jointree(subquery);

    /*
     * Pull up any SubLinks within the subquery's quals, so that we don't
     * leave unoptimized SubLinks behind.
     */
    if (*subquery).hasSubLinks {
        pull_up_sublinks(subroot);
    }

    /*
     * Similarly, preprocess its function RTEs to inline any set-returning
     * functions in its rangetable.
     */
    preprocess_function_rtes(subroot);

    /*
     * Scan the rangetable for relations with virtual generated columns, and
     * replace all Var nodes in the query that reference these columns with
     * the generation expressions.
     */
    subquery = expand_virtual_generated_columns(subroot);
    (*subroot).parse = subquery;

    /*
     * Recursively pull up the subquery's subqueries, so that
     * pull_up_subqueries' processing is complete for its jointree and
     * rangetable.
     *
     * Note: it's okay that the subquery's recursion starts with NULL for
     * containing-join info, even if we are within an outer join in the upper
     * query; the lower query starts with a clean slate for outer-join
     * semantics.  Likewise, we needn't pass down appendrel state.
     */
    pull_up_subqueries(subroot);

    /*
     * Now we must recheck whether the subquery is still simple enough to pull
     * up.  If not, abandon processing it.
     *
     * We don't really need to recheck all the conditions involved, but it's
     * easier just to keep this "if" looking the same as the one in
     * pull_up_subqueries_recurse.
     */
    if is_simple_subquery(root, subquery, rte, lowest_outer_join)
        && (containing_appendrel.is_null() || is_safe_append_member(subquery))
    {
        /* good to go */
    } else {
        /*
         * Give up, return unmodified RangeTblRef.
         *
         * Note: The work we just did will be redone when the subquery gets
         * planned on its own.  Perhaps we could avoid that by storing the
         * modified subquery back into the rangetable, but I'm not gonna risk
         * it now.
         */
        return jtnode;
    }

    /*
     * We must flatten any join alias Vars in the subquery's targetlist,
     * because pulling up the subquery's subqueries might have changed their
     * expansions into arbitrary expressions, which could affect
     * pullup_replace_vars' decisions about whether PlaceHolderVar wrappers
     * are needed for tlist entries.  (Likely it'd be better to do
     * flatten_join_alias_vars on the whole query tree at some earlier stage,
     * maybe even in the rewriter; but for now let's just fix this case here.)
     */
    (*subquery).targetList = flatten_join_alias_vars(
        subroot,
        (*subroot).parse,
        (*subquery).targetList as *mut Node,
    ) as *mut List;

    /*
     * Adjust level-0 varnos in subquery so that we can append its rangetable
     * to upper query's.  We have to fix the subquery's append_rel_list as
     * well.
     */
    rtoffset = list_length((*parse).rtable);
    OffsetVarNodes(subquery as *mut Node, rtoffset, 0);
    OffsetVarNodes((*subroot).append_rel_list as *mut Node, rtoffset, 0);

    /*
     * Upper-level vars in subquery are now one level closer to their parent
     * than before.
     */
    IncrementVarSublevelsUp(subquery as *mut Node, -1, 1);
    IncrementVarSublevelsUp((*subroot).append_rel_list as *mut Node, -1, 1);

    /*
     * The subquery's targetlist items are now in the appropriate form to
     * insert into the top query, except that we may need to wrap them in
     * PlaceHolderVars.  Set up required context data for pullup_replace_vars.
     * (Note that we should include the subquery's inner joins in relids,
     * since it may include join alias vars referencing them.)
     */
    rvcontext = PullupReplaceVarsContext {
        root,
        targetlist: (*subquery).targetList,
        target_rte: rte,
        result_relation: 0,
        relids: if (*rte).lateral {
            get_relids_in_jointree((*subquery).jointree as *mut Node, true, true)
        } else {
            core::ptr::null_mut() /* won't need this value */
        },
        nullinfo: if (*rte).lateral {
            get_nullingrels(parse)
        } else {
            core::ptr::null_mut() /* won't need this value */
        },
        outer_hasSubLinks: &mut (*parse).hasSubLinks,
        varno,
        /* this flag will be set below, if needed */
        wrap_option: REPLACE_WRAP_NONE,
        /* initialize cache array with indexes 0 .. length(tlist) */
        rv_cache: palloc0(
            ((list_length((*subquery).targetList) + 1) as usize) * mem::size_of::<*mut Node>(),
        ) as *mut *mut Node,
    };

    /*
     * If the parent query uses grouping sets, we need a PlaceHolderVar for
     * each expression of the subquery's targetlist items.  This ensures that
     * expressions retain their separate identity so that they will match
     * grouping set columns when appropriate.  (It'd be sufficient to wrap
     * values used in grouping set columns, and do so only in non-aggregated
     * portions of the tlist and havingQual, but that would require a lot of
     * infrastructure that pullup_replace_vars hasn't currently got.)
     */
    if !(*parse).groupingSets.is_null() {
        rvcontext.wrap_option = REPLACE_WRAP_ALL;
    }

    /*
     * Replace all of the top query's references to the subquery's outputs
     * with copies of the adjusted subtlist items, being careful not to
     * replace any of the jointree structure.
     */
    perform_pullup_replace_vars(root, &mut rvcontext, containing_appendrel);

    /*
     * If the subquery had a LATERAL marker, propagate that to any of its
     * child RTEs that could possibly now contain lateral cross-references.
     * The children might or might not contain any actual lateral
     * cross-references, but we have to mark the pulled-up child RTEs so that
     * later planner stages will check for such.
     */
    if (*rte).lateral {
        let subquery_rtable = (*subquery).rtable;
        let mut lc = crate::nodes::pg_list::list_head(subquery_rtable);
        while !lc.is_null() {
            let child_rte = crate::nodes::pg_list::lfirst(lc) as *mut RangeTblEntry;

            match (*child_rte).rtekind {
                RTE_RELATION => {
                    if !(*child_rte).tablesample.is_null() {
                        (*child_rte).lateral = true;
                    }
                }
                RTE_SUBQUERY | RTE_FUNCTION | RTE_VALUES | RTE_TABLEFUNC => {
                    (*child_rte).lateral = true;
                }
                RTE_JOIN | RTE_CTE | RTE_NAMEDTUPLESTORE | RTE_RESULT | RTE_GROUP => {
                    /* these can't contain any lateral references */
                }
            }
            lc = crate::nodes::pg_list::lnext(subquery_rtable, lc);
        }
    }

    /*
     * Now append the adjusted rtable entries and their perminfos to upper
     * query. (We hold off until after fixing the upper rtable entries; no
     * point in running that code on the subquery ones too.)
     */
    CombineRangeTables(
        &mut (*parse).rtable,
        &mut (*parse).rteperminfos,
        (*subquery).rtable,
        (*subquery).rteperminfos,
    );

    /*
     * Pull up any FOR UPDATE/SHARE markers, too.  (OffsetVarNodes already
     * adjusted the marker rtindexes, so just concat the lists.)
     */
    (*parse).rowMarks = list_concat((*parse).rowMarks, (*subquery).rowMarks);

    /*
     * We also have to fix the relid sets of any PlaceHolderVar nodes in the
     * parent query.  (This could perhaps be done by pullup_replace_vars(),
     * but it seems cleaner to use two passes.)  Note in particular that any
     * PlaceHolderVar nodes just created by pullup_replace_vars() will be
     * adjusted, so having created them with the subquery's varno is correct.
     *
     * Likewise, relids appearing in AppendRelInfo nodes have to be fixed. We
     * already checked that this won't require introducing multiple subrelids
     * into the single-slot AppendRelInfo structs.
     */
    if (*(*root).glob).lastPHId != 0 || !(*root).append_rel_list.is_null() {
        let subrelids: Relids;

        subrelids = get_relids_in_jointree((*subquery).jointree as *mut Node, true, false);
        if (*(*root).glob).lastPHId != 0 {
            substitute_phv_relids(parse as *mut Node, varno, subrelids);
        }
        fix_append_rel_relids(root, varno, subrelids);
    }

    /*
     * And now add subquery's AppendRelInfos to our list.
     */
    (*root).append_rel_list =
        list_concat((*root).append_rel_list, (*subroot).append_rel_list);

    /*
     * We don't have to do the equivalent bookkeeping for outer-join info,
     * because that hasn't been set up yet.  placeholder_list likewise.
     */
    Assert!((*root).join_info_list.is_null());
    Assert!((*subroot).join_info_list.is_null());
    Assert!((*root).placeholder_list.is_null());
    Assert!((*subroot).placeholder_list.is_null());

    /*
     * We no longer need the RTE's copy of the subquery's query tree.  Getting
     * rid of it saves nothing in particular so far as this level of query is
     * concerned; but if this query level is in turn pulled up into a parent,
     * we'd waste cycles copying the now-unused query tree.
     */
    (*rte).subquery = core::ptr::null_mut();

    /*
     * Miscellaneous housekeeping.
     *
     * Although replace_rte_variables() faithfully updated parse->hasSubLinks
     * if it copied any SubLinks out of the subquery's targetlist, we still
     * could have SubLinks added to the query in the expressions of FUNCTION
     * and VALUES RTEs copied up from the subquery.  So it's necessary to copy
     * subquery->hasSubLinks anyway.  Perhaps this can be improved someday.
     */
    (*parse).hasSubLinks |= (*subquery).hasSubLinks;

    /* If subquery had any RLS conditions, now main query does too */
    (*parse).hasRowSecurity |= (*subquery).hasRowSecurity;

    /*
     * subquery won't be pulled up if it hasAggs, hasWindowFuncs, or
     * hasTargetSRFs, so no work needed on those flags
     */

    /*
     * Return the adjusted subquery jointree to replace the RangeTblRef entry
     * in parent's jointree; or, if the FromExpr is degenerate, just return
     * its single member.
     */
    Assert!(IsA!((*subquery).jointree as *mut Node, T_FromExpr));
    Assert!(!(*(*subquery).jointree).fromlist.is_null());
    if (*(*subquery).jointree).quals.is_null()
        && list_length((*(*subquery).jointree).fromlist) == 1
    {
        return linitial((*(*subquery).jointree).fromlist) as *mut Node;
    }

    (*subquery).jointree as *mut Node
}

// ===========================================================================
// Part 4: pull_up_simple_union_all, pull_up_union_leaf_queries,
//         make_setop_translation_list, is_simple_subquery,
//         pull_up_simple_values, is_simple_values,
//         pull_up_constant_function, is_simple_union_all,
//         is_simple_union_all_recurse, is_safe_append_member,
//         jointree_contains_lateral_outer_refs,
//         perform_pullup_replace_vars, replace_vars_in_jointree,
//         pullup_replace_vars, pullup_replace_vars_callback,
//         pullup_replace_vars_subquery
// ===========================================================================

/*
 * pull_up_simple_union_all
 *        Pull up a single simple UNION ALL subquery.
 */
// TODO(pg-port): depends on flatten_simple_union_all internals and
// pull_up_union_leaf_queries.  Stub returns jtnode unchanged.
unsafe fn pull_up_simple_union_all(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    rte: *mut RangeTblEntry,
) -> *mut Node {
    use crate::nodes::parsenodes::RTEKind::*;

    let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
    let subquery = (*rte).subquery;
    let rtoffset = list_length((*(*root).parse).rtable);

    /*
     * Make a modifiable copy of the subquery's rtable, so we can adjust
     * upper-level Vars in it.
     */
    let rtable = copyObject((*subquery).rtable as *mut c_void) as *mut List;

    /*
     * Upper-level vars in subquery are now one level closer to their parent
     * than before.
     */
    IncrementVarSublevelsUp_rtable(rtable, -1, 1);

    /*
     * If the UNION ALL subquery had a LATERAL marker, propagate that to all
     * its children.
     */
    if (*rte).lateral {
        let mut lc = crate::nodes::pg_list::list_head(rtable);
        while !lc.is_null() {
            let child_rte = crate::nodes::pg_list::lfirst(lc) as *mut RangeTblEntry;
            Assert!((*child_rte).rtekind == RTE_SUBQUERY);
            (*child_rte).lateral = true;
            lc = crate::nodes::pg_list::lnext(rtable, lc);
        }
    }

    /*
     * Append child RTEs (and their perminfos) to parent rtable.
     */
    CombineRangeTables(
        &mut (*(*root).parse).rtable,
        &mut (*(*root).parse).rteperminfos,
        rtable,
        (*subquery).rteperminfos,
    );

    /*
     * Recursively scan the subquery's setOperations tree and add
     * AppendRelInfo nodes for leaf subqueries to the parent's
     * append_rel_list.
     */
    Assert!(!(*subquery).setOperations.is_null());
    pull_up_union_leaf_queries(
        (*subquery).setOperations,
        root,
        varno,
        subquery,
        rtoffset,
    );

    /*
     * Mark the parent as an append relation.
     */
    (*rte).inh = true;

    jtnode
}

/*
 * pull_up_union_leaf_queries -- recursive guts of pull_up_simple_union_all
 */
unsafe fn pull_up_union_leaf_queries(
    setop: *mut Node,
    root: *mut PlannerInfo,
    parent_rtindex: i32,
    setop_query: *mut Query,
    child_rtoffset: i32,
) {
    if IsA!(setop, T_RangeTblRef) {
        let rtr = setop as *mut RangeTblRef;
        let child_rtindex = child_rtoffset + (*rtr).rtindex;

        /*
         * Build a suitable AppendRelInfo, and attach to parent's list.
         */
        let appinfo: *mut AppendRelInfo = makeNode!(AppendRelInfo, T_AppendRelInfo);
        (*appinfo).parent_relid = parent_rtindex as u32;
        (*appinfo).child_relid = child_rtindex as u32;
        (*appinfo).parent_reltype = InvalidOid;
        (*appinfo).child_reltype = InvalidOid;
        make_setop_translation_list(setop_query, child_rtindex, appinfo);
        (*appinfo).parent_reloid = InvalidOid;
        (*root).append_rel_list = lappend((*root).append_rel_list, appinfo as *mut c_void);

        /*
         * Recursively apply pull_up_subqueries to the new child RTE.
         */
        let new_rtr: *mut RangeTblRef = makeNode!(RangeTblRef, T_RangeTblRef);
        (*new_rtr).rtindex = child_rtindex;
        let _ = pull_up_subqueries_recurse(
            root,
            new_rtr as *mut Node,
            core::ptr::null_mut(),
            appinfo,
        );
    } else if IsA!(setop, T_SetOperationStmt) {
        let op = setop as *mut SetOperationStmt;

        /* Recurse to reach leaf queries */
        pull_up_union_leaf_queries((*op).larg, root, parent_rtindex, setop_query, child_rtoffset);
        pull_up_union_leaf_queries((*op).rarg, root, parent_rtindex, setop_query, child_rtoffset);
    } else {
        panic!("unrecognized node type: {}", nodeTag(setop) as i32);
    }
}

/*
 * make_setop_translation_list
 *    Build the list of translations from parent Vars to child Vars for
 *    a UNION ALL member.
 */
unsafe fn make_setop_translation_list(
    query: *mut Query,
    newvarno: i32,
    appinfo: *mut AppendRelInfo,
) {
    let mut vars: *mut List = NIL;
    let tlist_len = list_length((*query).targetList);

    /* Initialize reverse-translation array with all entries zero */
    /* (entries for resjunk columns will stay that way) */
    (*appinfo).num_child_cols = tlist_len;
    let pcolnos: *mut AttrNumber = palloc0(
        (tlist_len as usize) * mem::size_of::<AttrNumber>(),
    ) as *mut AttrNumber;
    (*appinfo).parent_colnos = pcolnos;

    let query_targetlist = (*query).targetList;
    let mut lc = crate::nodes::pg_list::list_head(query_targetlist);
    while !lc.is_null() {
        let tle = crate::nodes::pg_list::lfirst(lc) as *mut TargetEntry;
        if !(*tle).resjunk {
            vars = lappend(vars, makeVarFromTargetEntry(newvarno as Index, tle) as *mut c_void);
            *pcolnos.add(((*tle).resno - 1) as usize) = (*tle).resno;
        }
        lc = crate::nodes::pg_list::lnext(query_targetlist, lc);
    }

    (*appinfo).translated_vars = vars;
}

/*
 * is_simple_subquery
 *    Check a subquery in the range table to see if it's simple enough
 *    to pull up into the parent query.
 */
unsafe fn is_simple_subquery(
    root: *mut PlannerInfo,
    subquery: *mut Query,
    rte: *mut RangeTblEntry,
    lowest_outer_join: *mut JoinExpr,
) -> bool {
    use crate::nodes::nodes::CmdType::*;

    /*
     * Let's just make sure it's a valid subselect ...
     */
    if !IsA!(subquery as *mut Node, T_Query) || (*subquery).commandType != CMD_SELECT {
        panic!("subquery is bogus");
    }

    /*
     * Can't currently pull up a query with setops (unless it's simple UNION
     * ALL, which is handled by a different code path).
     */
    if !(*subquery).setOperations.is_null() {
        return false;
    }

    /*
     * Can't pull up a subquery involving grouping, aggregation, SRFs,
     * sorting, limiting, or WITH.
     */
    if (*subquery).hasAggs
        || (*subquery).hasWindowFuncs
        || (*subquery).hasTargetSRFs
        || !(*subquery).groupClause.is_null()
        || !(*subquery).groupingSets.is_null()
        || !(*subquery).havingQual.is_null()
        || !(*subquery).sortClause.is_null()
        || !(*subquery).distinctClause.is_null()
        || !(*subquery).limitOffset.is_null()
        || !(*subquery).limitCount.is_null()
        || (*subquery).hasForUpdate
        || !(*subquery).cteList.is_null()
    {
        return false;
    }

    /*
     * Don't pull up if the RTE represents a security-barrier view.
     */
    if (*rte).security_barrier {
        return false;
    }

    /*
     * If the subquery is LATERAL, check for pullup restrictions from that.
     */
    if (*rte).lateral {
        let restricted: bool;
        let safe_upper_varnos: Relids;

        if !lowest_outer_join.is_null() {
            restricted = true;
            safe_upper_varnos = get_relids_in_jointree(
                lowest_outer_join as *mut Node,
                true,
                true,
            );
        } else {
            restricted = false;
            safe_upper_varnos = core::ptr::null_mut(); /* doesn't matter */
        }

        if jointree_contains_lateral_outer_refs(
            root,
            (*subquery).jointree as *mut Node,
            restricted,
            safe_upper_varnos,
        ) {
            return false;
        }

        /*
         * If there's an outer join above the LATERAL subquery, also disallow
         * pullup if the subquery's targetlist has any references to rels
         * outside the outer join.
         */
        if !lowest_outer_join.is_null() {
            let lvarnos = pull_varnos_of_level(root, (*subquery).targetList as *mut Node, 1);
            if !bms_is_subset(lvarnos, safe_upper_varnos) {
                return false;
            }
        }
    }

    /*
     * Don't pull up a subquery that has any volatile functions in its
     * targetlist.
     */
    if contain_volatile_functions((*subquery).targetList as *mut Node) {
        return false;
    }

    true
}

/*
 * pull_up_simple_values
 *        Pull up a single simple VALUES RTE.
 */
unsafe fn pull_up_simple_values(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    rte: *mut RangeTblEntry,
) -> *mut Node {
    use crate::nodes::parsenodes::RTEKind::*;

    let parse = (*root).parse;
    let varno = (*(jtnode as *mut RangeTblRef)).rtindex;

    Assert!((*rte).rtekind == RTE_VALUES);
    Assert!(list_length((*rte).values_lists) == 1);

    /*
     * Need a modifiable copy of the VALUES list to hack on.
     */
    let values_list = copyObject(
        linitial((*rte).values_lists) as *mut c_void,
    ) as *mut List;

    /*
     * The VALUES RTE can't contain any Vars of level zero.
     */
    Assert!(!contain_vars_of_level(values_list as *mut Node, 0));

    /*
     * Set up required context data for pullup_replace_vars.
     * Make the VALUES list look like a subquery targetlist.
     */
    let mut tlist: *mut List = NIL;
    let mut attrno: AttrNumber = 1;
    let mut lc = crate::nodes::pg_list::list_head(values_list);
    while !lc.is_null() {
        tlist = lappend(
            tlist,
            makeTargetEntry(
                crate::nodes::pg_list::lfirst(lc) as *mut Expr,
                attrno,
                core::ptr::null_mut(),
                false,
            ) as *mut c_void,
        );
        attrno += 1;
        lc = crate::nodes::pg_list::lnext(values_list, lc);
    }

    let mut rvcontext = PullupReplaceVarsContext {
        root,
        targetlist: tlist,
        target_rte: rte,
        result_relation: 0,
        relids: core::ptr::null_mut(), /* can't be any lateral references here */
        nullinfo: core::ptr::null_mut(),
        outer_hasSubLinks: &mut (*parse).hasSubLinks,
        varno,
        wrap_option: REPLACE_WRAP_NONE,
        rv_cache: palloc0(
            ((list_length(tlist) + 1) as usize) * mem::size_of::<*mut Node>(),
        ) as *mut *mut Node,
    };

    /*
     * Replace all of the top query's references to the RTE's outputs.
     */
    perform_pullup_replace_vars(root, &mut rvcontext, core::ptr::null_mut());

    /*
     * There should be no appendrels to fix, nor any outer joins and hence no
     * PlaceHolderVars.
     */
    Assert!((*root).append_rel_list.is_null());
    Assert!((*root).join_info_list.is_null());
    Assert!((*root).placeholder_list.is_null());

    /*
     * Replace the VALUES RTE with a RESULT RTE.
     */
    Assert!(list_length((*parse).rtable) == 1);

    /* Create suitable RTE */
    let new_rte: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*new_rte).rtekind = RTE_RESULT;
    (*new_rte).eref = makeAlias(b"*RESULT*\0".as_ptr() as *const c_char, NIL);

    /* Replace rangetable */
    (*parse).rtable = list_make1!(new_rte as *mut c_void);

    /* We could manufacture a new RangeTblRef, but the one we have is fine */
    Assert!(varno == 1);

    jtnode
}

/*
 * is_simple_values
 *    Check a VALUES RTE in the range table to see if it's simple enough
 *    to pull up into the parent query.
 */
unsafe fn is_simple_values(root: *mut PlannerInfo, rte: *mut RangeTblEntry) -> bool {
    use crate::nodes::parsenodes::RTEKind::*;
    Assert!((*rte).rtekind == RTE_VALUES);

    /*
     * There must be exactly one VALUES list.
     */
    if list_length((*rte).values_lists) != 1 {
        return false;
    }

    /*
     * Don't pull up a VALUES that contains any set-returning or volatile
     * functions.
     */
    if expression_returns_set((*rte).values_lists as *mut Node)
        || contain_volatile_functions((*rte).values_lists as *mut Node)
    {
        return false;
    }

    /*
     * Do not pull up a VALUES that's not the only RTE in its parent query.
     */
    if list_length((*(*root).parse).rtable) != 1
        || rte != linitial((*(*root).parse).rtable) as *mut RangeTblEntry
    {
        return false;
    }

    true
}

/*
 * pull_up_constant_function
 *        Pull up an RTE_FUNCTION expression that was simplified to a constant.
 */
unsafe fn pull_up_constant_function(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    rte: *mut RangeTblEntry,
    containing_appendrel: *mut AppendRelInfo,
) -> *mut Node {
    use crate::nodes::parsenodes::RTEKind::*;

    let parse = (*root).parse;

    /* Fail if the RTE has ORDINALITY - we don't implement that here. */
    if (*rte).funcordinality {
        return jtnode;
    }

    /* Fail if RTE isn't a single, simple Const expr */
    if list_length((*rte).functions) != 1 {
        return jtnode;
    }
    let rtf = linitial_node_RangeTblFunction((*rte).functions);
    if !IsA!((*rtf).funcexpr, T_Const) {
        return jtnode;
    }

    /*
     * If the function's result is not a scalar, we punt.
     */
    if (*rtf).funccolcount != 1 {
        return jtnode; /* definitely composite */
    }

    /* If it has a coldeflist, it certainly returns RECORD */
    if !(*rtf).funccolnames.is_null() {
        return jtnode; /* must be a one-column RECORD type */
    }

    let mut funcrettype: Oid = InvalidOid;
    let mut tupdesc: *mut TupleDesc = core::ptr::null_mut();
    let functypclass = get_expr_result_type((*rtf).funcexpr, &mut funcrettype, &mut tupdesc);
    if !matches!(functypclass, TYPEFUNC_SCALAR) {
        return jtnode; /* must be a one-column composite type */
    }

    /* Create context for applying pullup_replace_vars */
    let target_entry = makeTargetEntry(
        (*rtf).funcexpr as *mut Expr,
        1,                           /* resno */
        core::ptr::null_mut(),       /* resname */
        false,                       /* resjunk */
    );
    let mut rvcontext = PullupReplaceVarsContext {
        root,
        targetlist: list_make1!(target_entry as *mut c_void),
        target_rte: rte,
        result_relation: 0,
        /*
         * Since this function was reduced to a Const, it doesn't contain any
         * lateral references, even if it's marked as LATERAL.
         */
        relids: core::ptr::null_mut(),
        nullinfo: core::ptr::null_mut(),
        outer_hasSubLinks: &mut (*parse).hasSubLinks,
        varno: (*(jtnode as *mut RangeTblRef)).rtindex,
        /* this flag will be set below, if needed */
        wrap_option: REPLACE_WRAP_NONE,
        rv_cache: palloc0(
            ((list_length(core::ptr::null_mut::<List>()) + 1 + 1) as usize)
                * mem::size_of::<*mut Node>(),
        ) as *mut *mut Node,
    };
    /* Re-init cache with correct size */
    rvcontext.rv_cache = palloc0(
        ((list_length(rvcontext.targetlist) + 1) as usize) * mem::size_of::<*mut Node>(),
    ) as *mut *mut Node;

    /*
     * If the parent query uses grouping sets, we need a PlaceHolderVar.
     */
    if !(*parse).groupingSets.is_null() {
        rvcontext.wrap_option = REPLACE_WRAP_ALL;
    }

    /*
     * Replace all of the top query's references to the RTE's output.
     */
    perform_pullup_replace_vars(root, &mut rvcontext, containing_appendrel);

    /*
     * Convert the RTE to be RTE_RESULT type.
     */
    (*rte).rtekind = RTE_RESULT;
    (*rte).functions = NIL;
    (*rte).lateral = false;

    jtnode
}

/*
 * is_simple_union_all
 *    Check a subquery to see if it's a simple UNION ALL.
 */
unsafe fn is_simple_union_all(subquery: *mut Query) -> bool {
    use crate::nodes::nodes::CmdType::*;

    /* Let's just make sure it's a valid subselect ... */
    if !IsA!(subquery as *mut Node, T_Query) || (*subquery).commandType != CMD_SELECT {
        panic!("subquery is bogus");
    }

    /* Is it a set-operation query at all? */
    if !IsA!((*subquery).setOperations, T_SetOperationStmt) {
        return false;
    }
    let topop = (*subquery).setOperations as *mut SetOperationStmt;
    if topop.is_null() {
        return false;
    }

    /* Can't handle ORDER BY, LIMIT/OFFSET, locking, or WITH */
    if !(*subquery).sortClause.is_null()
        || !(*subquery).limitOffset.is_null()
        || !(*subquery).limitCount.is_null()
        || !(*subquery).rowMarks.is_null()
        || !(*subquery).cteList.is_null()
    {
        return false;
    }

    /* Recursively check the tree of set operations */
    is_simple_union_all_recurse((*subquery).setOperations, subquery, (*topop).colTypes)
}

unsafe fn is_simple_union_all_recurse(
    setop: *mut Node,
    setop_query: *mut Query,
    col_types: *mut List,
) -> bool {
    use crate::nodes::parsenodes::SetOperation::*;

    /* Since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if IsA!(setop, T_RangeTblRef) {
        let rtr = setop as *mut RangeTblRef;
        let rte = rt_fetch((*rtr).rtindex, (*setop_query).rtable);
        let sub = (*rte).subquery;
        Assert!(!sub.is_null());
        /* Leaf nodes are OK if they match the toplevel column types */
        /* We don't have to compare typmods or collations here */
        return tlist_same_datatypes((*sub).targetList, col_types, true);
    } else if IsA!(setop, T_SetOperationStmt) {
        let op = setop as *mut SetOperationStmt;
        /* Must be UNION ALL */
        if (*op).op != SETOP_UNION || !(*op).all {
            return false;
        }
        /* Recurse to check inputs */
        return is_simple_union_all_recurse((*op).larg, setop_query, col_types)
            && is_simple_union_all_recurse((*op).rarg, setop_query, col_types);
    } else {
        panic!("unrecognized node type: {}", nodeTag(setop) as i32);
    }
}

/*
 * is_safe_append_member
 *    Check a subquery that is a leaf of a UNION ALL appendrel to see if it's
 *    safe to pull up.
 */
unsafe fn is_safe_append_member(subquery: *mut Query) -> bool {
    let mut jtnode = (*subquery).jointree;
    Assert!(IsA!(jtnode as *mut Node, T_FromExpr));
    /* Check the completely-empty case */
    if (*jtnode).fromlist.is_null() && (*jtnode).quals.is_null() {
        return true;
    }
    /* Check the more general case */
    'outer: loop {
        if !IsA!(jtnode as *mut Node, T_FromExpr) {
            break 'outer;
        }
        if !(*jtnode).quals.is_null() {
            return false;
        }
        if list_length((*jtnode).fromlist) != 1 {
            return false;
        }
        let inner = linitial((*jtnode).fromlist) as *mut Node;
        if !IsA!(inner, T_FromExpr) {
            if !IsA!(inner, T_RangeTblRef) {
                return false;
            }
            return true;
        }
        jtnode = inner as *mut FromExpr;
    }
    false
}

/*
 * jointree_contains_lateral_outer_refs
 *        Check for disallowed lateral references in a jointree's quals
 */
unsafe fn jointree_contains_lateral_outer_refs(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    mut restricted: bool,
    mut safe_upper_varnos: Relids,
) -> bool {
    if jtnode.is_null() {
        return false;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        return false;
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;

        /* First, recurse to check child joins */
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            if jointree_contains_lateral_outer_refs(
                root,
                crate::nodes::pg_list::lfirst(lc) as *mut Node,
                restricted,
                safe_upper_varnos,
            ) {
                return true;
            }
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }

        /* Then check the top-level quals */
        if restricted
            && !bms_is_subset(
                pull_varnos_of_level(root, (*f).quals, 1),
                safe_upper_varnos,
            )
        {
            return true;
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        use crate::nodes::nodes::JoinType::*;

        let j = jtnode as *mut JoinExpr;

        /*
         * If this is an outer join, we mustn't allow any upper lateral
         * references in or below it.
         */
        if (*j).jointype != JOIN_INNER {
            restricted = true;
            safe_upper_varnos = core::ptr::null_mut();
        }

        /* Check the child joins */
        if jointree_contains_lateral_outer_refs(root, (*j).larg, restricted, safe_upper_varnos) {
            return true;
        }
        if jointree_contains_lateral_outer_refs(root, (*j).rarg, restricted, safe_upper_varnos) {
            return true;
        }

        /* Check the JOIN's qual clauses */
        if restricted
            && !bms_is_subset(
                pull_varnos_of_level(root, (*j).quals, 1),
                safe_upper_varnos,
            )
        {
            return true;
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    false
}

/*
 * Perform pullup_replace_vars everyplace it's needed in the query tree.
 */
unsafe fn perform_pullup_replace_vars(
    root: *mut PlannerInfo,
    rvcontext: *mut PullupReplaceVarsContext,
    containing_appendrel: *mut AppendRelInfo,
) {
    let parse = (*root).parse;

    /*
     * If we are considering an appendrel child subquery, then the only part
     * of the upper query that could reference the child yet is the
     * translated_vars list of the associated AppendRelInfo.
     */
    if !containing_appendrel.is_null() {
        let save_wrap = (*rvcontext).wrap_option;
        (*rvcontext).wrap_option = REPLACE_WRAP_NONE;
        (*containing_appendrel).translated_vars = pullup_replace_vars(
            (*containing_appendrel).translated_vars as *mut Node,
            rvcontext,
        ) as *mut List;
        (*rvcontext).wrap_option = save_wrap;
        return;
    }

    /*
     * Replace all of the top query's references to the subquery's outputs
     * with copies of the adjusted subtlist items.
     */
    (*parse).targetList = pullup_replace_vars((*parse).targetList as *mut Node, rvcontext)
        as *mut List;
    (*parse).returningList = pullup_replace_vars((*parse).returningList as *mut Node, rvcontext)
        as *mut List;

    if !(*parse).onConflict.is_null() {
        (*(*parse).onConflict).onConflictSet = pullup_replace_vars(
            (*(*parse).onConflict).onConflictSet as *mut Node,
            rvcontext,
        ) as *mut List;
        (*(*parse).onConflict).onConflictWhere = pullup_replace_vars(
            (*(*parse).onConflict).onConflictWhere,
            rvcontext,
        );
        /*
         * We assume ON CONFLICT's arbiterElems, arbiterWhere, exclRelTlist
         * can't contain any references to a subquery.
         */
    }
    if !(*parse).mergeActionList.is_null() {
        let merge_action_list = (*parse).mergeActionList;
        let mut lc = crate::nodes::pg_list::list_head(merge_action_list);
        while !lc.is_null() {
            let action = crate::nodes::pg_list::lfirst(lc) as *mut MergeAction;
            (*action).qual = pullup_replace_vars((*action).qual, rvcontext);
            (*action).targetList = pullup_replace_vars(
                (*action).targetList as *mut Node,
                rvcontext,
            ) as *mut List;
            lc = crate::nodes::pg_list::lnext(merge_action_list, lc);
        }
    }
    (*parse).mergeJoinCondition =
        pullup_replace_vars((*parse).mergeJoinCondition, rvcontext);
    replace_vars_in_jointree((*parse).jointree as *mut Node, rvcontext);
    Assert!((*parse).setOperations.is_null());
    (*parse).havingQual = pullup_replace_vars((*parse).havingQual, rvcontext);

    /*
     * Replace references in the translated_vars lists of appendrels.
     */
    let append_rel_list = (*root).append_rel_list;
    let mut lc = crate::nodes::pg_list::list_head(append_rel_list);
    while !lc.is_null() {
        let appinfo = crate::nodes::pg_list::lfirst(lc) as *mut AppendRelInfo;
        (*appinfo).translated_vars = pullup_replace_vars(
            (*appinfo).translated_vars as *mut Node,
            rvcontext,
        ) as *mut List;
        lc = crate::nodes::pg_list::lnext(append_rel_list, lc);
    }

    /*
     * Replace references in the joinaliasvars lists of join RTEs and the
     * groupexprs list of group RTE.
     */
    let parse_rtable2 = (*parse).rtable;
    let mut lc = crate::nodes::pg_list::list_head(parse_rtable2);
    while !lc.is_null() {
        use crate::nodes::parsenodes::RTEKind::*;
        let otherrte = crate::nodes::pg_list::lfirst(lc) as *mut RangeTblEntry;
        if (*otherrte).rtekind == RTE_JOIN {
            (*otherrte).joinaliasvars = pullup_replace_vars(
                (*otherrte).joinaliasvars as *mut Node,
                rvcontext,
            ) as *mut List;
        } else if (*otherrte).rtekind == RTE_GROUP {
            (*otherrte).groupexprs = pullup_replace_vars(
                (*otherrte).groupexprs as *mut Node,
                rvcontext,
            ) as *mut List;
        }
        lc = crate::nodes::pg_list::lnext(parse_rtable2, lc);
    }
}

/*
 * Helper routine for perform_pullup_replace_vars: do pullup_replace_vars on
 * every expression in the jointree.
 */
unsafe fn replace_vars_in_jointree(jtnode: *mut Node, context: *mut PullupReplaceVarsContext) {
    use crate::nodes::parsenodes::RTEKind::*;
    use crate::nodes::nodes::JoinType::*;

    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        /*
         * If the RangeTblRef refers to a LATERAL subquery (that isn't the
         * same subquery we're pulling up), it might contain references to the
         * target subquery, which we must replace.
         */
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        if varno != (*context).varno {
            let rte = rt_fetch(varno, (*(*context).root).parse.cast::<Query>().as_ref().unwrap().rtable);
            Assert!(rte != (*context).target_rte);
            if (*rte).lateral {
                match (*rte).rtekind {
                    RTE_RELATION => {
                        /* shouldn't be marked LATERAL unless tablesample */
                        Assert!(!(*rte).tablesample.is_null());
                        (*rte).tablesample = pullup_replace_vars(
                            (*rte).tablesample as *mut Node,
                            context,
                        ) as *mut TableSampleClause;
                    }
                    RTE_SUBQUERY => {
                        (*rte).subquery =
                            pullup_replace_vars_subquery((*rte).subquery, context);
                    }
                    RTE_FUNCTION => {
                        (*rte).functions = pullup_replace_vars(
                            (*rte).functions as *mut Node,
                            context,
                        ) as *mut List;
                    }
                    RTE_TABLEFUNC => {
                        (*rte).tablefunc = pullup_replace_vars(
                            (*rte).tablefunc as *mut Node,
                            context,
                        ) as *mut TableFunc;
                    }
                    RTE_VALUES => {
                        (*rte).values_lists = pullup_replace_vars(
                            (*rte).values_lists as *mut Node,
                            context,
                        ) as *mut List;
                    }
                    RTE_JOIN | RTE_CTA | RTE_NAMEDTUPLESTORE | RTE_RESULT | RTE_GROUP => {
                        /* these shouldn't be marked LATERAL */
                        Assert!(false);
                    }
                }
            }
        }
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            replace_vars_in_jointree(crate::nodes::pg_list::lfirst(lc) as *mut Node, context);
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
        (*f).quals = pullup_replace_vars((*f).quals, context);
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        let save_wrap = (*context).wrap_option;

        replace_vars_in_jointree((*j).larg, context);
        replace_vars_in_jointree((*j).rarg, context);

        /*
         * Use PHVs within the join quals of a full join for variable-free
         * expressions.
         */
        if (*j).jointype == JOIN_FULL {
            (*context).wrap_option = REPLACE_WRAP_VARFREE;
        }

        (*j).quals = pullup_replace_vars((*j).quals, context);
        (*context).wrap_option = save_wrap;
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
}

/*
 * Apply pullup variable replacement throughout an expression tree.
 */
unsafe fn pullup_replace_vars(
    expr: *mut Node,
    context: *mut PullupReplaceVarsContext,
) -> *mut Node {
    replace_rte_variables(
        expr,
        (*context).varno,
        0,
        Some(pullup_replace_vars_callback_trampoline),
        context as *mut c_void,
        (*context).outer_hasSubLinks,
    )
}

/*
 * C-ABI trampoline for pullup_replace_vars_callback.
 * replace_rte_variables calls our callback via a C function pointer.
 */
unsafe extern "C" fn pullup_replace_vars_callback_trampoline(
    var: *mut Var,
    context: *mut ReplaceRteVariablesContext,
) -> *mut Node {
    let rcon = (*context).callback_arg as *mut PullupReplaceVarsContext;
    pullup_replace_vars_callback(var, rcon)
}

unsafe fn pullup_replace_vars_callback(
    var: *mut Var,
    rcon: *mut PullupReplaceVarsContext,
) -> *mut Node {
    let varattno = (*var).varattno;
    let mut newnode: *mut Node;

    /* System columns are not replaced. */
    if varattno < InvalidAttrNumber {
        return copyObject(var as *mut c_void) as *mut Node;
    }

    /*
     * We need a PlaceHolderVar if the Var-to-be-replaced has nonempty
     * varnullingrels (unless we find below that the replacement expression is
     * a Var or PlaceHolderVar that we can just add the nullingrels to).  We
     * also need one if the caller has instructed us that certain expression
     * replacements need to be wrapped for identification purposes.
     */
    let need_phv = !(*var).varnullingrels.is_null()
        || ((*rcon).wrap_option != REPLACE_WRAP_NONE);

    /*
     * If PlaceHolderVars are needed, we cache the modified expressions.
     */
    if need_phv
        && varattno >= InvalidAttrNumber
        && varattno <= list_length((*rcon).targetlist) as AttrNumber
        && !(*(*rcon).rv_cache.add(varattno as usize)).is_null()
    {
        /* Just copy the entry and fall through to adjust phlevelsup etc */
        newnode = copyObject(*(*rcon).rv_cache.add(varattno as usize) as *mut c_void) as *mut Node;
    } else {
        /*
         * Generate the replacement expression.
         */
        newnode = ReplaceVarFromTargetList(
            var,
            (*rcon).target_rte,
            (*rcon).targetlist,
            (*rcon).result_relation,
            REPLACEVARS_REPORT_ERROR,
            0,
        );

        /* Insert PlaceHolderVar if needed */
        if need_phv {
            let wrap: bool;

            if (*rcon).wrap_option == REPLACE_WRAP_ALL {
                /* Caller told us to wrap all expressions in a PlaceHolderVar */
                wrap = true;
            } else if varattno == InvalidAttrNumber {
                /*
                 * Insert PlaceHolderVar for whole-tuple reference.
                 */
                wrap = true;
            } else if !newnode.is_null() && IsA!(newnode, T_Var)
                && (*(newnode as *mut Var)).varlevelsup == 0
            {
                /*
                 * Simple Vars always escape being wrapped, unless they are
                 * lateral references to something outside the subquery being
                 * pulled up and the referenced rel is not under the same
                 * lowest nulling outer join.
                 */
                let mut w = false;
                if (*(*rcon).target_rte).lateral
                    && !bms_is_member((*(newnode as *mut Var)).varno, (*rcon).relids)
                {
                    let nullinfo = (*rcon).nullinfo;
                    let lvarno = (*(newnode as *mut Var)).varno;
                    Assert!(lvarno > 0 && (lvarno as i32) <= (*nullinfo).rtlength);
                    if !bms_is_subset(
                        *(*nullinfo).nullingrels.add((*rcon).varno as usize),
                        *(*nullinfo).nullingrels.add(lvarno as usize),
                    ) {
                        w = true;
                    }
                }
                wrap = w;
            } else if !newnode.is_null() && IsA!(newnode, T_PlaceHolderVar)
                && (*(newnode as *mut PlaceHolderVar)).phlevelsup == 0
            {
                /* The same rules apply for a PlaceHolderVar */
                let mut w = false;
                if (*(*rcon).target_rte).lateral
                    && !bms_is_subset(
                        (*(newnode as *mut PlaceHolderVar)).phrels,
                        (*rcon).relids,
                    )
                {
                    let nullinfo = (*rcon).nullinfo;
                    let lvarnos = (*(newnode as *mut PlaceHolderVar)).phrels;
                    let mut lvarno: i32 = -1;
                    'lv: loop {
                        lvarno = bms_next_member(lvarnos, lvarno);
                        if lvarno < 0 { break 'lv; }
                        Assert!(lvarno > 0 && lvarno <= (*nullinfo).rtlength);
                        if !bms_is_subset(
                            *(*nullinfo).nullingrels.add((*rcon).varno as usize),
                            *(*nullinfo).nullingrels.add(lvarno as usize),
                        ) {
                            w = true;
                            break 'lv;
                        }
                    }
                }
                wrap = w;
            } else {
                /*
                 * Else: check whether the node contains nullable Vars/PHVs
                 * and is strict enough that a PHV wrapper is unnecessary.
                 */
                let mut contain_nullable_vars = false;

                if !(*(*rcon).target_rte).lateral {
                    if contain_vars_of_level(newnode, 0) {
                        contain_nullable_vars = true;
                    }
                } else {
                    let all_varnos = pull_varnos((*rcon).root, newnode);
                    if bms_overlap(all_varnos, (*rcon).relids) {
                        contain_nullable_vars = true;
                    } else {
                        let nullinfo = (*rcon).nullinfo;
                        let mut varno: i32 = -1;
                        'vn: loop {
                            varno = bms_next_member(all_varnos, varno);
                            if varno < 0 { break 'vn; }
                            Assert!(varno > 0 && varno <= (*nullinfo).rtlength);
                            if bms_is_subset(
                                *(*nullinfo).nullingrels.add((*rcon).varno as usize),
                                *(*nullinfo).nullingrels.add(varno as usize),
                            ) {
                                contain_nullable_vars = true;
                                break 'vn;
                            }
                        }
                    }
                }

                wrap = !(contain_nullable_vars && !contain_nonstrict_functions(newnode));
            }

            if wrap {
                newnode = make_placeholder_expr(
                    (*rcon).root,
                    newnode as *mut Expr,
                    bms_make_singleton((*rcon).varno),
                ) as *mut Node;

                /*
                 * Cache it if possible.
                 */
                if varattno >= InvalidAttrNumber
                    && varattno <= list_length((*rcon).targetlist) as AttrNumber
                {
                    *(*rcon).rv_cache.add(varattno as usize) =
                        copyObject(newnode as *mut c_void) as *mut Node;
                }
            }
        }
    }

    /* Propagate any varnullingrels into the replacement expression */
    if !(*var).varnullingrels.is_null() {
        if IsA!(newnode, T_Var) {
            let newvar = newnode as *mut Var;
            Assert!((*newvar).varlevelsup == 0);
            (*newvar).varnullingrels = bms_add_members(
                (*newvar).varnullingrels,
                (*var).varnullingrels,
            );
        } else if IsA!(newnode, T_PlaceHolderVar) {
            let newphv = newnode as *mut PlaceHolderVar;
            Assert!((*newphv).phlevelsup == 0);
            (*newphv).phnullingrels = bms_add_members(
                (*newphv).phnullingrels,
                (*var).varnullingrels,
            );
        } else {
            /*
             * There should be Vars/PHVs within the expression that we can
             * modify.
             */
            if (*(*rcon).target_rte).lateral {
                let nullinfo = (*rcon).nullinfo;
                let lvarnos = pull_varnos((*rcon).root, newnode);
                let lvarnos = bms_del_members(lvarnos, (*rcon).relids);
                let mut lvarno: i32 = -1;
                loop {
                    lvarno = bms_next_member(lvarnos, lvarno);
                    if lvarno < 0 { break; }
                    Assert!(lvarno > 0 && lvarno <= (*nullinfo).rtlength);
                    let lnullingrels = bms_intersect(
                        (*var).varnullingrels,
                        *(*nullinfo).nullingrels.add(lvarno as usize),
                    );
                    if !bms_is_empty(lnullingrels) {
                        newnode = add_nulling_relids(
                            newnode,
                            bms_make_singleton(lvarno),
                            lnullingrels,
                        );
                    }
                }
            }

            /* Finally, deal with Vars/PHVs of the subquery itself */
            newnode = add_nulling_relids(newnode, (*rcon).relids, (*var).varnullingrels);
            /* Assert we did put the varnullingrels into the expression */
            Assert!(bms_is_subset(
                (*var).varnullingrels,
                pull_varnos((*rcon).root, newnode),
            ));
        }
    }

    /* Must adjust varlevelsup if replaced Var is within a subquery */
    if (*var).varlevelsup > 0 {
        IncrementVarSublevelsUp(newnode, (*var).varlevelsup as i32, 0);
    }

    newnode
}

/*
 * Apply pullup variable replacement to a subquery.
 *
 * This needs to be different from pullup_replace_vars() because
 * replace_rte_variables will think that it shouldn't increment sublevels_up
 * before entering the Query; so we need to call it with sublevels_up == 1.
 */
unsafe fn pullup_replace_vars_subquery(
    query: *mut Query,
    context: *mut PullupReplaceVarsContext,
) -> *mut Query {
    Assert!(IsA!(query as *mut Node, T_Query));
    replace_rte_variables(
        query as *mut Node,
        (*context).varno,
        1,
        Some(pullup_replace_vars_callback_trampoline),
        context as *mut c_void,
        core::ptr::null_mut(),
    ) as *mut Query
}

// ---------------------------------------------------------------------------
// flatten_simple_union_all
// ---------------------------------------------------------------------------

/*
 * flatten_simple_union_all
 *        Try to optimize top-level UNION ALL structure into an appendrel
 */
// TODO(pg-port): depends on flatten_simple_union_all internals and
// pull_up_union_leaf_queries. Partial implementation.
pub unsafe extern "C" fn flatten_simple_union_all(root: *mut PlannerInfo) {
    let parse = (*root).parse;

    /* Shouldn't be called unless query has setops */
    Assert!(IsA!((*parse).setOperations, T_SetOperationStmt));
    let topop = (*parse).setOperations as *mut SetOperationStmt;
    Assert!(!topop.is_null());

    /* Can't optimize away a recursive UNION */
    if (*root).hasRecursion {
        return;
    }

    /*
     * Recursively check the tree of set operations.  If not all UNION ALL
     * with identical column types, punt.
     */
    if !is_simple_union_all_recurse((*parse).setOperations, parse, (*topop).colTypes) {
        return;
    }

    /*
     * Locate the leftmost leaf query in the setops tree.
     */
    let mut leftmostjtnode: *mut Node = (*topop).larg;
    while !leftmostjtnode.is_null() && IsA!(leftmostjtnode, T_SetOperationStmt) {
        leftmostjtnode = (*(leftmostjtnode as *mut SetOperationStmt)).larg;
    }
    Assert!(!leftmostjtnode.is_null() && IsA!(leftmostjtnode, T_RangeTblRef));
    let leftmost_rti = (*(leftmostjtnode as *mut RangeTblRef)).rtindex;
    let leftmost_rte = rt_fetch(leftmost_rti, (*parse).rtable);
    use crate::nodes::parsenodes::RTEKind::*;
    Assert!((*leftmost_rte).rtekind == RTE_SUBQUERY);

    /*
     * Make a copy of the leftmost RTE and add it to the rtable.
     */
    let child_rte = copyObject(leftmost_rte as *mut c_void) as *mut RangeTblEntry;
    (*parse).rtable = lappend((*parse).rtable, child_rte as *mut c_void);
    let child_rti = list_length((*parse).rtable);

    /* Modify the setops tree to reference the child copy */
    (*(leftmostjtnode as *mut RangeTblRef)).rtindex = child_rti;

    /* Modify the formerly-leftmost RTE to mark it as an appendrel parent */
    (*leftmost_rte).inh = true;

    /*
     * Form a RangeTblRef for the appendrel, and insert it into FROM.
     */
    let rtr: *mut RangeTblRef = makeNode!(RangeTblRef, T_RangeTblRef);
    (*rtr).rtindex = leftmost_rti;
    Assert!((*(*parse).jointree).fromlist.is_null());
    (*(*parse).jointree).fromlist = list_make1!(rtr as *mut c_void);

    /*
     * Now pretend the query has no setops.
     */
    (*parse).setOperations = core::ptr::null_mut();

    /*
     * Build AppendRelInfo information, and apply pull_up_subqueries to the
     * leaf queries of the UNION ALL.
     */
    pull_up_union_leaf_queries(topop as *mut Node, root, leftmost_rti, parse, 0);
}

// ===========================================================================
// Part 5: reduce_outer_joins, reduce_outer_joins_pass1,
//         reduce_outer_joins_pass2, report_reduced_full_join,
//         remove_useless_result_rtes, remove_useless_results_recurse,
//         get_result_relid, remove_result_refs
// ===========================================================================

/*
 * reduce_outer_joins
 *        Attempt to reduce outer joins to plain inner joins.
 */
pub unsafe extern "C" fn reduce_outer_joins(root: *mut PlannerInfo) {
    let state1 = reduce_outer_joins_pass1((*(*root).parse).jointree as *mut Node);

    /* planner.c shouldn't have called me if no outer joins */
    if state1.is_null() || !(*state1).contains_outer {
        panic!("so where are the outer joins?");
    }

    let mut state2 = ReduceOuterJoinsPass2State {
        inner_reduced: core::ptr::null_mut(),
        partial_reduced: NIL,
    };

    reduce_outer_joins_pass2(
        (*(*root).parse).jointree as *mut Node,
        state1,
        &mut state2,
        root,
        core::ptr::null_mut(),
        NIL,
    );

    /*
     * If we successfully reduced the strength of any outer joins, we must
     * remove references to those joins as nulling rels.
     */
    if !bms_is_empty(state2.inner_reduced) {
        (*root).parse = remove_nulling_relids(
            (*root).parse as *mut Node,
            state2.inner_reduced,
            core::ptr::null_mut(),
        ) as *mut Query;
        /* There could be references in the append_rel_list, too */
        (*root).append_rel_list = remove_nulling_relids(
            (*root).append_rel_list as *mut Node,
            state2.inner_reduced,
            core::ptr::null_mut(),
        ) as *mut List;
    }

    /*
     * Partially-reduced full joins have to be done one at a time.
     */
    let partial_reduced = state2.partial_reduced;
    let mut lc = crate::nodes::pg_list::list_head(partial_reduced);
    while !lc.is_null() {
        let statep = crate::nodes::pg_list::lfirst(lc) as *mut ReduceOuterJoinsPartialState;
        let full_join_relids = bms_make_singleton((*statep).full_join_rti);

        (*root).parse = remove_nulling_relids(
            (*root).parse as *mut Node,
            full_join_relids,
            (*statep).unreduced_side,
        ) as *mut Query;
        (*root).append_rel_list = remove_nulling_relids(
            (*root).append_rel_list as *mut Node,
            full_join_relids,
            (*statep).unreduced_side,
        ) as *mut List;
        lc = crate::nodes::pg_list::lnext(partial_reduced, lc);
    }
}

/*
 * reduce_outer_joins_pass1 - phase 1 data collection
 */
unsafe fn reduce_outer_joins_pass1(jtnode: *mut Node) -> *mut ReduceOuterJoinsPass1State {
    let result = palloc(mem::size_of::<ReduceOuterJoinsPass1State>())
        as *mut ReduceOuterJoinsPass1State;
    (*result).relids = core::ptr::null_mut();
    (*result).contains_outer = false;
    (*result).sub_states = NIL;

    if jtnode.is_null() {
        return result;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        (*result).relids = bms_make_singleton(varno);
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            let sub_state = reduce_outer_joins_pass1(crate::nodes::pg_list::lfirst(lc) as *mut Node);
            (*result).relids = bms_add_members((*result).relids, (*sub_state).relids);
            (*result).contains_outer |= (*sub_state).contains_outer;
            (*result).sub_states = lappend((*result).sub_states, sub_state as *mut c_void);
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;

        /* join's own RT index is not wanted in result->relids */
        if IS_OUTER_JOIN((*j).jointype) {
            (*result).contains_outer = true;
        }

        let sub_state = reduce_outer_joins_pass1((*j).larg);
        (*result).relids = bms_add_members((*result).relids, (*sub_state).relids);
        (*result).contains_outer |= (*sub_state).contains_outer;
        (*result).sub_states = lappend((*result).sub_states, sub_state as *mut c_void);

        let sub_state = reduce_outer_joins_pass1((*j).rarg);
        (*result).relids = bms_add_members((*result).relids, (*sub_state).relids);
        (*result).contains_outer |= (*sub_state).contains_outer;
        (*result).sub_states = lappend((*result).sub_states, sub_state as *mut c_void);
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    result
}

/*
 * reduce_outer_joins_pass2 - phase 2 processing
 */
unsafe fn reduce_outer_joins_pass2(
    jtnode: *mut Node,
    state1: *mut ReduceOuterJoinsPass1State,
    state2: *mut ReduceOuterJoinsPass2State,
    root: *mut PlannerInfo,
    nonnullable_rels: Relids,
    forced_null_vars: *mut List,
) {
    use crate::nodes::nodes::JoinType::*;

    /*
     * pass 2 should never descend as far as an empty subnode or base rel.
     */
    if jtnode.is_null() {
        panic!("reached empty jointree");
    }
    if IsA!(jtnode, T_RangeTblRef) {
        panic!("reached base rel");
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;

        /* Scan quals to see if we can add any constraints */
        let mut pass_nonnullable_rels = find_nonnullable_rels((*f).quals);
        pass_nonnullable_rels = bms_add_members(pass_nonnullable_rels, nonnullable_rels);
        let mut pass_forced_null_vars = find_forced_null_vars((*f).quals);
        pass_forced_null_vars = mbms_add_members(pass_forced_null_vars, forced_null_vars);

        /* And recurse --- but only into interesting subtrees */
        Assert!(list_length((*f).fromlist) == list_length((*state1).sub_states));
        let f_fromlist = (*f).fromlist;
        let state1_sub_states = (*state1).sub_states;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        let mut ls = crate::nodes::pg_list::list_head(state1_sub_states);
        while !lc.is_null() {
            let sub_state = crate::nodes::pg_list::lfirst(ls) as *mut ReduceOuterJoinsPass1State;
            if (*sub_state).contains_outer {
                reduce_outer_joins_pass2(
                    crate::nodes::pg_list::lfirst(lc) as *mut Node,
                    sub_state,
                    state2,
                    root,
                    pass_nonnullable_rels,
                    pass_forced_null_vars,
                );
            }
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
            ls = crate::nodes::pg_list::lnext(state1_sub_states, ls);
        }
        bms_free(pass_nonnullable_rels);
        /* can't so easily clean up var lists, unfortunately */
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        let rtindex = (*j).rtindex;
        let mut jointype = (*j).jointype;
        let mut left_state = linitial((*state1).sub_states) as *mut ReduceOuterJoinsPass1State;
        let mut right_state = {
            let second = crate::nodes::pg_list::list_nth((*state1).sub_states, 1);
            second as *mut ReduceOuterJoinsPass1State
        };

        /* Can we simplify this join? */
        match jointype {
            JOIN_INNER => {}
            JOIN_LEFT => {
                if bms_overlap(nonnullable_rels, (*right_state).relids) {
                    jointype = JOIN_INNER;
                }
            }
            JOIN_RIGHT => {
                if bms_overlap(nonnullable_rels, (*left_state).relids) {
                    jointype = JOIN_INNER;
                }
            }
            JOIN_FULL => {
                if bms_overlap(nonnullable_rels, (*left_state).relids) {
                    if bms_overlap(nonnullable_rels, (*right_state).relids) {
                        jointype = JOIN_INNER;
                    } else {
                        jointype = JOIN_LEFT;
                        /* Also report partial reduction in state2 */
                        report_reduced_full_join(state2, rtindex, (*right_state).relids);
                    }
                } else if bms_overlap(nonnullable_rels, (*right_state).relids) {
                    jointype = JOIN_RIGHT;
                    /* Also report partial reduction in state2 */
                    report_reduced_full_join(state2, rtindex, (*left_state).relids);
                }
            }
            JOIN_SEMI | JOIN_ANTI => {
                /*
                 * These could only have been introduced by pull_up_sublinks,
                 * so there's no way that upper quals could refer to their
                 * righthand sides, and no point in checking.
                 */
            }
            _ => {
                panic!("unrecognized join type: {}", jointype as i32);
            }
        }

        /*
         * Convert JOIN_RIGHT to JOIN_LEFT.
         */
        if jointype == JOIN_RIGHT {
            let tmparg = (*j).larg;
            (*j).larg = (*j).rarg;
            (*j).rarg = tmparg;
            jointype = JOIN_LEFT;
            right_state = left_state;
            left_state = crate::nodes::pg_list::list_nth((*state1).sub_states, 1)
                as *mut ReduceOuterJoinsPass1State;
        }

        /*
         * See if we can reduce JOIN_LEFT to JOIN_ANTI.
         */
        if jointype == JOIN_LEFT {
            let nonnullable_vars = find_nonnullable_vars((*j).quals);
            let overlap = mbms_overlap_sets(nonnullable_vars, forced_null_vars);
            if bms_overlap(overlap, (*right_state).relids) {
                jointype = JOIN_ANTI;
            }
        }

        /*
         * Apply the jointype change, if any, to both jointree node and RTE.
         */
        if rtindex != 0 && jointype != (*j).jointype {
            let rte = rt_fetch(rtindex, (*(*root).parse).rtable);
            use crate::nodes::parsenodes::RTEKind::*;
            Assert!((*rte).rtekind == RTE_JOIN);
            Assert!((*rte).jointype == (*j).jointype);
            (*rte).jointype = jointype;
            if jointype == JOIN_INNER {
                (*state2).inner_reduced = bms_add_member((*state2).inner_reduced, rtindex);
            }
        }
        (*j).jointype = jointype;

        /* Only recurse if there's more to do below here */
        if (*left_state).contains_outer || (*right_state).contains_outer {
            let local_nonnullable_rels: Relids;
            let local_forced_null_vars: *mut List;
            let pass_nonnullable_rels: Relids;
            let pass_forced_null_vars: *mut List;

            if jointype != JOIN_FULL {
                local_nonnullable_rels = find_nonnullable_rels((*j).quals);
                local_forced_null_vars = find_forced_null_vars((*j).quals);
                if jointype == JOIN_INNER || jointype == JOIN_SEMI {
                    /* OK to merge upper and local constraints */
                    let lnr = bms_add_members(local_nonnullable_rels, nonnullable_rels);
                    let lfnv = mbms_add_members(local_forced_null_vars, forced_null_vars);
                    // Use merged versions
                    let pnr = lnr;
                    let pfnv = lfnv;
                    if (*left_state).contains_outer {
                        reduce_outer_joins_pass2(
                            (*j).larg, left_state, state2, root, pnr, pfnv,
                        );
                    }
                    if (*right_state).contains_outer {
                        reduce_outer_joins_pass2(
                            (*j).rarg, right_state, state2, root, pnr, pfnv,
                        );
                    }
                    bms_free(pnr);
                    return;
                }
                // LEFT or ANTI: pass local to nullable side, upper to non-nullable side
                if (*left_state).contains_outer {
                    /* can't pass local constraints to non-nullable side */
                    reduce_outer_joins_pass2(
                        (*j).larg, left_state, state2, root,
                        nonnullable_rels, forced_null_vars,
                    );
                }
                if (*right_state).contains_outer {
                    reduce_outer_joins_pass2(
                        (*j).rarg, right_state, state2, root,
                        local_nonnullable_rels, local_forced_null_vars,
                    );
                }
                bms_free(local_nonnullable_rels);
            } else {
                /* no use in calculating these for JOIN_FULL */
                if (*left_state).contains_outer {
                    reduce_outer_joins_pass2(
                        (*j).larg, left_state, state2, root,
                        core::ptr::null_mut(), NIL,
                    );
                }
                if (*right_state).contains_outer {
                    reduce_outer_joins_pass2(
                        (*j).rarg, right_state, state2, root,
                        core::ptr::null_mut(), NIL,
                    );
                }
            }
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
}

/* Helper for reduce_outer_joins_pass2 */
unsafe fn report_reduced_full_join(
    state2: *mut ReduceOuterJoinsPass2State,
    rtindex: i32,
    relids: Relids,
) {
    let statep = palloc(mem::size_of::<ReduceOuterJoinsPartialState>())
        as *mut ReduceOuterJoinsPartialState;
    (*statep).full_join_rti = rtindex;
    (*statep).unreduced_side = relids;
    (*state2).partial_reduced = lappend((*state2).partial_reduced, statep as *mut c_void);
}

/*
 * remove_useless_result_rtes
 *        Attempt to remove RTE_RESULT RTEs from the join tree.
 */
pub unsafe extern "C" fn remove_useless_result_rtes(root: *mut PlannerInfo) {
    let mut dropped_outer_joins: Relids = core::ptr::null_mut();

    /* Top level of jointree must always be a FromExpr */
    Assert!(IsA!((*(*root).parse).jointree as *mut Node, T_FromExpr));
    /* Recurse ... */
    (*(*root).parse).jointree = remove_useless_results_recurse(
        root,
        (*(*root).parse).jointree as *mut Node,
        core::ptr::null_mut(),
        &mut dropped_outer_joins,
    ) as *mut FromExpr;
    /* We should still have a FromExpr */
    Assert!(IsA!((*(*root).parse).jointree as *mut Node, T_FromExpr));

    /*
     * If we removed any outer-join nodes from the jointree, run around and
     * remove references to those joins as nulling rels.
     */
    if !bms_is_empty(dropped_outer_joins) {
        (*root).parse = remove_nulling_relids(
            (*root).parse as *mut Node,
            dropped_outer_joins,
            core::ptr::null_mut(),
        ) as *mut Query;
        (*root).append_rel_list = remove_nulling_relids(
            (*root).append_rel_list as *mut Node,
            dropped_outer_joins,
            core::ptr::null_mut(),
        ) as *mut List;
    }

    /*
     * Remove any PlanRowMark referencing an RTE_RESULT RTE.
     */
    let root_rowmarks = (*root).rowMarks;
    let mut lc = crate::nodes::pg_list::list_head(root_rowmarks);
    while !lc.is_null() {
        use crate::nodes::parsenodes::RTEKind::*;
        let rc = crate::nodes::pg_list::lfirst(lc) as *mut crate::nodes::plannodes::PlanRowMark;
        let next = crate::nodes::pg_list::lnext(root_rowmarks, lc);
        if (*rt_fetch((*rc).rti as i32, (*(*root).parse).rtable)).rtekind == RTE_RESULT {
            (*root).rowMarks = crate::nodes::pg_list::list_delete_cell((*root).rowMarks, lc);
        }
        lc = next;
    }
}

/*
 * remove_useless_results_recurse
 *        Recursive guts of remove_useless_result_rtes.
 */
unsafe fn remove_useless_results_recurse(
    root: *mut PlannerInfo,
    jtnode: *mut Node,
    parent_quals: *mut *mut Node,
    dropped_outer_joins: *mut Relids,
) -> *mut Node {
    use crate::nodes::nodes::JoinType::*;

    Assert!(!jtnode.is_null());
    if IsA!(jtnode, T_RangeTblRef) {
        /* Can't immediately do anything with a RangeTblRef */
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let mut result_relids: Relids = core::ptr::null_mut();

        /*
         * We can drop RTE_RESULT rels from the fromlist so long as at least
         * one child remains.
         */
        let mut lc = crate::nodes::pg_list::list_head((*f).fromlist);
        while !lc.is_null() {
            let child = crate::nodes::pg_list::lfirst(lc) as *mut Node;
            let next = crate::nodes::pg_list::lnext((*f).fromlist, lc);

            /* Recursively transform child, allowing it to push up quals ... */
            let child = remove_useless_results_recurse(
                root, child, &mut (*f).quals, dropped_outer_joins,
            );
            /* ... and stick it back into the tree */
            (*lc).ptr_value = child as *mut c_void;

            /*
             * If it's an RTE_RESULT with at least one sibling, and no sibling
             * references dependent PHVs, we can drop it.
             */
            let varno = get_result_relid(root, child);
            if list_length((*f).fromlist) > 1
                && varno != 0
                && !find_dependent_phvs_in_jointree(root, jtnode, varno)
            {
                (*f).fromlist = crate::nodes::pg_list::list_delete_cell((*f).fromlist, lc);
                result_relids = bms_add_member(result_relids, varno);
            }
            lc = next;
        }

        /*
         * Clean up if we dropped any RTE_RESULT RTEs.
         */
        if !result_relids.is_null() {
            let mut varno: i32 = -1;
            loop {
                varno = bms_next_member(result_relids, varno);
                if varno < 0 { break; }
                remove_result_refs(root, varno, jtnode);
            }
        }

        /*
         * If the FromExpr now has only one child, see if we can elide it.
         */
        if list_length((*f).fromlist) == 1
            && !core::ptr::eq(f, (*(*root).parse).jointree)
            && ((*f).quals.is_null() || !parent_quals.is_null())
        {
            /*
             * Merge any quals up to parent.
             */
            if !(*f).quals.is_null() {
                *parent_quals = list_concat(
                    (*f).quals as *mut List,
                    *parent_quals as *mut List,
                ) as *mut Node;
            }
            return linitial((*f).fromlist) as *mut Node;
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;

        /*
         * First, recurse.
         */
        let larg_parent_quals: *mut *mut Node = if (*j).jointype == JOIN_INNER {
            &mut (*j).quals
        } else if (*j).jointype == JOIN_LEFT {
            parent_quals
        } else {
            core::ptr::null_mut()
        };
        (*j).larg = remove_useless_results_recurse(
            root, (*j).larg, larg_parent_quals, dropped_outer_joins,
        );

        let rarg_parent_quals: *mut *mut Node = if (*j).jointype == JOIN_INNER
            || (*j).jointype == JOIN_LEFT
        {
            &mut (*j).quals
        } else {
            core::ptr::null_mut()
        };
        (*j).rarg = remove_useless_results_recurse(
            root, (*j).rarg, rarg_parent_quals, dropped_outer_joins,
        );

        /* Apply join-type-specific optimization rules */
        match (*j).jointype {
            JOIN_INNER => {
                let varno = get_result_relid(root, (*j).larg);
                if varno != 0 && !find_dependent_phvs_in_jointree(root, (*j).rarg, varno) {
                    remove_result_refs(root, varno, (*j).rarg);
                    if !(*j).quals.is_null() && parent_quals.is_null() {
                        return makeFromExpr(list_make1!((*j).rarg as *mut c_void), (*j).quals)
                            as *mut Node;
                    } else {
                        /* Merge any quals up to parent */
                        if !(*j).quals.is_null() {
                            *parent_quals = list_concat(
                                (*j).quals as *mut List,
                                *parent_quals as *mut List,
                            ) as *mut Node;
                        }
                        return (*j).rarg;
                    }
                }
                let varno = get_result_relid(root, (*j).rarg);
                if varno != 0 {
                    remove_result_refs(root, varno, (*j).larg);
                    if !(*j).quals.is_null() && parent_quals.is_null() {
                        return makeFromExpr(list_make1!((*j).larg as *mut c_void), (*j).quals)
                            as *mut Node;
                    } else {
                        if !(*j).quals.is_null() {
                            *parent_quals = list_concat(
                                (*j).quals as *mut List,
                                *parent_quals as *mut List,
                            ) as *mut Node;
                        }
                        return (*j).larg;
                    }
                }
            }
            JOIN_LEFT => {
                let varno = get_result_relid(root, (*j).rarg);
                if varno != 0 && ((*j).quals.is_null() || !find_dependent_phvs(root, varno)) {
                    remove_result_refs(root, varno, (*j).larg);
                    *dropped_outer_joins = bms_add_member(*dropped_outer_joins, (*j).rtindex);
                    return (*j).larg;
                }
            }
            JOIN_SEMI => {
                let varno = get_result_relid(root, (*j).rarg);
                if varno != 0 {
                    Assert!((*j).rtindex == 0);
                    remove_result_refs(root, varno, (*j).larg);
                    if !(*j).quals.is_null() && parent_quals.is_null() {
                        return makeFromExpr(list_make1!((*j).larg as *mut c_void), (*j).quals)
                            as *mut Node;
                    } else {
                        if !(*j).quals.is_null() {
                            *parent_quals = list_concat(
                                (*j).quals as *mut List,
                                *parent_quals as *mut List,
                            ) as *mut Node;
                        }
                        return (*j).larg;
                    }
                }
            }
            JOIN_FULL | JOIN_ANTI => {
                /* We have no special smarts for these cases */
            }
            _ => {
                /* Note: JOIN_RIGHT should be gone at this point */
                panic!("unrecognized join type: {}", (*j).jointype as i32);
            }
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    jtnode
}

/*
 * get_result_relid
 *        If jtnode is a RangeTblRef for an RTE_RESULT RTE, return its relid;
 *        otherwise return 0.
 */
unsafe fn get_result_relid(root: *mut PlannerInfo, jtnode: *mut Node) -> i32 {
    use crate::nodes::parsenodes::RTEKind::*;

    if !IsA!(jtnode, T_RangeTblRef) {
        return 0;
    }
    let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
    if (*rt_fetch(varno, (*(*root).parse).rtable)).rtekind != RTE_RESULT {
        return 0;
    }
    varno
}

/*
 * remove_result_refs
 *        Helper routine for dropping an unneeded RTE_RESULT RTE.
 */
unsafe fn remove_result_refs(root: *mut PlannerInfo, varno: i32, newjtloc: *mut Node) {
    /* Fix up PlaceHolderVars as needed */
    /* If there are no PHVs anywhere, we can skip this bit */
    if (*(*root).glob).lastPHId != 0 {
        let subrelids = get_relids_in_jointree(newjtloc, true, false);
        Assert!(!bms_is_empty(subrelids));
        substitute_phv_relids((*root).parse as *mut Node, varno, subrelids);
        fix_append_rel_relids(root, varno, subrelids);
    }

    /*
     * We also need to remove any PlanRowMark referencing the RTE, but we
     * postpone that work until we return to remove_useless_result_rtes.
     */
}

// ===========================================================================
// Part 6: find_dependent_phvs walker, find_dependent_phvs,
//         find_dependent_phvs_in_jointree, substitute_phv_relids walker,
//         substitute_phv_relids, fix_append_rel_relids,
//         get_relids_in_jointree, get_relids_for_join,
//         find_jointree_node_for_rel, get_nullingrels, get_nullingrels_recurse
// ===========================================================================

/*
 * find_dependent_phvs - are there any PlaceHolderVars whose relids are
 * exactly the given varno?
 */
unsafe extern "C" fn find_dependent_phvs_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    let ctx = context as *mut FindDependentPhvsContext;
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;
        if (*phv).phlevelsup == (*ctx).sublevels_up as u32
            && bms_equal((*ctx).relids, (*phv).phrels)
        {
            return true;
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        (*ctx).sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(find_dependent_phvs_walker),
            context,
            0,
        );
        (*ctx).sublevels_up -= 1;
        return result;
    }
    /* Shouldn't need to handle most planner auxiliary nodes here */
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    expression_tree_walker(node, Some(find_dependent_phvs_walker), context)
}

unsafe fn find_dependent_phvs(root: *mut PlannerInfo, varno: i32) -> bool {
    /* If there are no PHVs anywhere, we needn't work hard */
    if (*(*root).glob).lastPHId == 0 {
        return false;
    }

    let mut context = FindDependentPhvsContext {
        relids: bms_make_singleton(varno),
        sublevels_up: 0,
    };

    if query_tree_walker(
        (*root).parse,
        Some(find_dependent_phvs_walker),
        &mut context as *mut FindDependentPhvsContext as *mut c_void,
        0,
    ) {
        return true;
    }
    /* The append_rel_list could be populated already, so check it too */
    if expression_tree_walker(
        (*root).append_rel_list as *mut Node,
        Some(find_dependent_phvs_walker),
        &mut context as *mut FindDependentPhvsContext as *mut c_void,
    ) {
        return true;
    }
    false
}

unsafe fn find_dependent_phvs_in_jointree(
    root: *mut PlannerInfo,
    node: *mut Node,
    varno: i32,
) -> bool {
    /* If there are no PHVs anywhere, we needn't work hard */
    if (*(*root).glob).lastPHId == 0 {
        return false;
    }

    let mut context = FindDependentPhvsContext {
        relids: bms_make_singleton(varno),
        sublevels_up: 0,
    };

    /*
     * See if the jointree fragment itself contains references (in join quals)
     */
    if find_dependent_phvs_walker(
        node,
        &mut context as *mut FindDependentPhvsContext as *mut c_void,
    ) {
        return true;
    }

    /*
     * Otherwise, identify the set of referenced RTEs and tediously check
     * each RTE that is marked LATERAL.
     */
    let subrelids = get_relids_in_jointree(node, false, false);
    let mut relid: i32 = -1;
    loop {
        relid = bms_next_member(subrelids, relid);
        if relid < 0 { break; }
        let rte = rt_fetch(relid, (*(*root).parse).rtable);
        if (*rte).lateral
            && range_table_entry_walker(
                rte,
                Some(find_dependent_phvs_walker),
                &mut context as *mut FindDependentPhvsContext as *mut c_void,
                0,
            )
        {
            return true;
        }
    }

    false
}

/*
 * substitute_phv_relids - adjust PlaceHolderVar relid sets after pulling up
 * a subquery or removing an RTE_RESULT jointree item.
 *
 * NOTE: although this has the form of a walker, we cheat and modify the
 * nodes in-place.
 */
unsafe extern "C" fn substitute_phv_relids_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    let ctx = context as *mut SubstitutePhvRelidsContext;
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_PlaceHolderVar) {
        let phv = node as *mut PlaceHolderVar;
        if (*phv).phlevelsup == (*ctx).sublevels_up as u32
            && bms_is_member((*ctx).varno, (*phv).phrels)
        {
            (*phv).phrels = bms_union((*phv).phrels, (*ctx).subrelids);
            (*phv).phrels = bms_del_member((*phv).phrels, (*ctx).varno);
            /* Assert we haven't broken the PHV */
            Assert!(!bms_is_empty((*phv).phrels));
        }
        /* fall through to examine children */
    }
    if IsA!(node, T_Query) {
        /* Recurse into subselects */
        (*ctx).sublevels_up += 1;
        let result = query_tree_walker(
            node as *mut Query,
            Some(substitute_phv_relids_walker),
            context,
            0,
        );
        (*ctx).sublevels_up -= 1;
        return result;
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    Assert!(!IsA!(node, T_SpecialJoinInfo));
    Assert!(!IsA!(node, T_AppendRelInfo));
    Assert!(!IsA!(node, T_PlaceHolderInfo));
    Assert!(!IsA!(node, T_MinMaxAggInfo));

    expression_tree_walker(node, Some(substitute_phv_relids_walker), context)
}

unsafe fn substitute_phv_relids(node: *mut Node, varno: i32, subrelids: Relids) {
    let mut context = SubstitutePhvRelidsContext {
        varno,
        sublevels_up: 0,
        subrelids,
    };

    /*
     * Must be prepared to start with a Query or a bare expression tree.
     */
    query_or_expression_tree_walker(
        node,
        Some(substitute_phv_relids_walker),
        &mut context as *mut SubstitutePhvRelidsContext as *mut c_void,
        0,
    );
}

/*
 * fix_append_rel_relids: update RT-index fields of AppendRelInfo nodes
 */
unsafe fn fix_append_rel_relids(root: *mut PlannerInfo, varno: i32, subrelids: Relids) {
    let mut subvarno: i32 = -1;

    let root_append_rel_list = (*root).append_rel_list;
    let mut lc = crate::nodes::pg_list::list_head(root_append_rel_list);
    while !lc.is_null() {
        let appinfo = crate::nodes::pg_list::lfirst(lc) as *mut AppendRelInfo;

        /* The parent_relid shouldn't ever be a pullup target */
        Assert!((*appinfo).parent_relid != varno as u32);

        if (*appinfo).child_relid == varno as u32 {
            if subvarno < 0 {
                subvarno = bms_singleton_member(subrelids);
            }
            (*appinfo).child_relid = subvarno as u32;
        }

        /* Also fix up any PHVs in its translated vars */
        if (*(*root).glob).lastPHId != 0 {
            substitute_phv_relids(
                (*appinfo).translated_vars as *mut Node,
                varno,
                subrelids,
            );
        }
        lc = crate::nodes::pg_list::lnext(root_append_rel_list, lc);
    }
}

/*
 * get_relids_in_jointree: get set of RT indexes present in a jointree
 *
 * Base-relation relids are always included in the result.
 * If include_outer_joins is true, outer-join RT indexes are included.
 * If include_inner_joins is true, inner-join RT indexes are included.
 */
pub unsafe extern "C" fn get_relids_in_jointree(
    jtnode: *mut Node,
    include_outer_joins: bool,
    include_inner_joins: bool,
) -> Relids {
    use crate::nodes::nodes::JoinType::*;

    let mut result: Relids = core::ptr::null_mut();

    if jtnode.is_null() {
        return result;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        result = bms_make_singleton(varno);
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            result = bms_join(
                result,
                get_relids_in_jointree(
                    crate::nodes::pg_list::lfirst(lc) as *mut Node,
                    include_outer_joins,
                    include_inner_joins,
                ),
            );
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        result = get_relids_in_jointree((*j).larg, include_outer_joins, include_inner_joins);
        result = bms_join(
            result,
            get_relids_in_jointree((*j).rarg, include_outer_joins, include_inner_joins),
        );
        if (*j).rtindex != 0 {
            if (*j).jointype == JOIN_INNER {
                if include_inner_joins {
                    result = bms_add_member(result, (*j).rtindex);
                }
            } else {
                if include_outer_joins {
                    result = bms_add_member(result, (*j).rtindex);
                }
            }
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    result
}

/*
 * get_relids_for_join: get set of base+OJ RT indexes making up a join
 */
pub unsafe extern "C" fn get_relids_for_join(query: *mut Query, joinrelid: i32) -> Relids {
    let jtnode = find_jointree_node_for_rel((*query).jointree as *mut Node, joinrelid);
    if jtnode.is_null() {
        panic!("could not find join node {}", joinrelid);
    }
    get_relids_in_jointree(jtnode, true, false)
}

/*
 * find_jointree_node_for_rel: locate jointree node for a base or join RT index
 *
 * Returns NULL if not found
 */
unsafe fn find_jointree_node_for_rel(jtnode: *mut Node, relid: i32) -> *mut Node {
    if jtnode.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        if relid == varno {
            return jtnode;
        }
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            let found = find_jointree_node_for_rel(
                crate::nodes::pg_list::lfirst(lc) as *mut Node,
                relid,
            );
            if !found.is_null() {
                return found;
            }
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        if relid == (*j).rtindex {
            return jtnode;
        }
        let found = find_jointree_node_for_rel((*j).larg, relid);
        if !found.is_null() {
            return found;
        }
        let found = find_jointree_node_for_rel((*j).rarg, relid);
        if !found.is_null() {
            return found;
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
    core::ptr::null_mut()
}

/*
 * get_nullingrels: collect info about which outer joins null which relations
 */
unsafe fn get_nullingrels(parse: *mut Query) -> *mut NullingrelInfo {
    let result = palloc(mem::size_of::<NullingrelInfo>()) as *mut NullingrelInfo;
    (*result).rtlength = list_length((*parse).rtable);
    (*result).nullingrels = palloc0(
        (((*result).rtlength + 1) as usize) * mem::size_of::<Relids>(),
    ) as *mut Relids;
    get_nullingrels_recurse((*parse).jointree as *mut Node, core::ptr::null_mut(), result);
    result
}

/*
 * Recursive guts of get_nullingrels().
 *
 * Note: at any recursion level, the passed-down upper_nullingrels must be
 * treated as a constant, but it can be stored directly into *info
 * if we're at leaf level.
 */
unsafe fn get_nullingrels_recurse(
    jtnode: *mut Node,
    upper_nullingrels: Relids,
    info: *mut NullingrelInfo,
) {
    use crate::nodes::nodes::JoinType::*;

    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
        Assert!(varno > 0 && varno <= (*info).rtlength);
        *(*info).nullingrels.add(varno as usize) = upper_nullingrels;
    } else if IsA!(jtnode, T_FromExpr) {
        let f = jtnode as *mut FromExpr;
        let f_fromlist = (*f).fromlist;
        let mut lc = crate::nodes::pg_list::list_head(f_fromlist);
        while !lc.is_null() {
            get_nullingrels_recurse(
                crate::nodes::pg_list::lfirst(lc) as *mut Node,
                upper_nullingrels,
                info,
            );
            lc = crate::nodes::pg_list::lnext(f_fromlist, lc);
        }
    } else if IsA!(jtnode, T_JoinExpr) {
        let j = jtnode as *mut JoinExpr;
        let local_nullingrels: Relids;

        match (*j).jointype {
            JOIN_INNER => {
                get_nullingrels_recurse((*j).larg, upper_nullingrels, info);
                get_nullingrels_recurse((*j).rarg, upper_nullingrels, info);
            }
            JOIN_LEFT | JOIN_SEMI | JOIN_ANTI => {
                local_nullingrels =
                    bms_add_member(bms_copy(upper_nullingrels), (*j).rtindex);
                get_nullingrels_recurse((*j).larg, upper_nullingrels, info);
                get_nullingrels_recurse((*j).rarg, local_nullingrels, info);
            }
            JOIN_FULL => {
                local_nullingrels =
                    bms_add_member(bms_copy(upper_nullingrels), (*j).rtindex);
                get_nullingrels_recurse((*j).larg, local_nullingrels, info);
                get_nullingrels_recurse((*j).rarg, local_nullingrels, info);
            }
            JOIN_RIGHT => {
                local_nullingrels =
                    bms_add_member(bms_copy(upper_nullingrels), (*j).rtindex);
                get_nullingrels_recurse((*j).larg, local_nullingrels, info);
                get_nullingrels_recurse((*j).rarg, upper_nullingrels, info);
            }
            _ => {
                panic!("unrecognized join type: {}", (*j).jointype as i32);
            }
        }
    } else {
        panic!("unrecognized node type: {}", nodeTag(jtnode) as i32);
    }
}

// ---------------------------------------------------------------------------
// Missing C-type aliases not yet in parsenodes.rs - forward-compat stubs
// ---------------------------------------------------------------------------

// These are referenced in the walker assertions; if the actual types
// are not yet in crate::nodes::parsenodes we add opaque forward decls.
#[allow(non_camel_case_types)]
extern "C" {
    // PlaceHolderInfo, MinMaxAggInfo, AppendRelInfo - used in walker assertions only
}

// RTEKind::RTE_CTA is an alias used in replace_vars_in_jointree above.
// The actual name in parsenodes.rs may differ; guard with a type alias.
use crate::nodes::parsenodes::RTEKind::RTE_CTE as RTE_CTA;

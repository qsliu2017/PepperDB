//! src/backend/rewrite/rewriteHandler.c
//!   Primary module of query rewriter.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!   Some of the terms used in this file are of historic nature: "retrieve"
//!   was the PostQUEL keyword for what today is SELECT. "RIR" stands for
//!   "Retrieve-Instead-Retrieve", that is an ON SELECT DO INSTEAD SELECT rule
//!   (which has to be unconditional and where only one rule can exist on each
//!   relation).

use crate::prelude::*;

use crate::{foreach, current_cell, castNode, IsA, lfirst_node, linitial_node, makeNode, foreach_delete_current};

use crate::nodes::pg_list::{
    lfirst, list_length, NIL, List,
    lappend, lappend_oid, lcons, list_concat, list_concat_copy,
    list_delete_last, list_member_oid, linitial,
};
use crate::nodes::nodes::Node;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

type Relation = *mut crate::utils::rel::RelationData;
type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;
type Query = crate::nodes::parsenodes::Query;
type RangeTblEntry = crate::nodes::parsenodes::RangeTblEntry;
type RTEPermissionInfo = crate::nodes::parsenodes::RTEPermissionInfo;
type TargetEntry = crate::nodes::primnodes::TargetEntry;
type CommonTableExpr = crate::nodes::parsenodes::CommonTableExpr;
type RangeTblRef = crate::nodes::primnodes::RangeTblRef;
type RuleLock = crate::rewrite::prs2lock::RuleLock;
type RewriteRule = crate::rewrite::prs2lock::RewriteRule;
type CmdType = crate::nodes::nodes::CmdType;
type OverridingKind = crate::nodes::primnodes::OverridingKind;
type LockClauseStrength = crate::nodes::lockoptions::LockClauseStrength;
type LockWaitPolicy = crate::nodes::lockoptions::LockWaitPolicy;
type Form_pg_attribute = *mut crate::catalog::pg_attribute::FormData_pg_attribute;
type Bitmapset = crate::nodes::bitmapset::Bitmapset;
type ItemPointerData = crate::storage::itemptr::ItemPointerData;
use crate::access::attnum::AttrNumber;
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

// Lock modes
use crate::storage::lockdefs::{AccessShareLock, RowShareLock, RowExclusiveLock, NoLock};

// CmdType variants
use crate::nodes::nodes::CmdType::{
    CMD_SELECT, CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE,
    CMD_UTILITY, CMD_NOTHING,
};

// RTEKind variants
use crate::nodes::parsenodes::RTEKind::{
    RTE_RELATION, RTE_JOIN, RTE_SUBQUERY, RTE_FUNCTION, RTE_TABLEFUNC, RTE_VALUES,
};

// relkind chars
use crate::catalog::pg_class::{
    RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_VIEW,
    RELKIND_PARTITIONED_TABLE, RELKIND_FOREIGN_TABLE, RELKIND_COMPOSITE_TYPE,
};

// rewriteDefine constants
use crate::rewrite::rewriteDefine::{
    RULE_FIRES_ON_ORIGIN, RULE_FIRES_ON_REPLICA, RULE_DISABLED,
};

// REPLACEVARS_ constants
use crate::rewrite::rewriteManip::{
    REPLACEVARS_CHANGE_VARNO, REPLACEVARS_SUBSTITUTE_NULL, REPLACEVARS_REPORT_ERROR,
};

// identity / generation constants
use crate::catalog::pg_attribute::{
    ATTRIBUTE_IDENTITY_ALWAYS, ATTRIBUTE_IDENTITY_BY_DEFAULT, ATTRIBUTE_GENERATED_VIRTUAL,
};

// PRS2 varnos
use crate::nodes::primnodes::{PRS2_OLD_VARNO, PRS2_NEW_VARNO};

// query_tree_walker flag
const QTW_IGNORE_RC_SUBQUERIES: c_int = 0x02;

// QuerySource variants
use crate::nodes::parsenodes::QuerySource::{
    QSRC_ORIGINAL, QSRC_INSTEAD_RULE, QSRC_QUAL_INSTEAD_RULE, QSRC_NON_INSTEAD_RULE,
};

// WCO kind
use crate::nodes::parsenodes::WCOKind::WCO_VIEW_CHECK;

// ONCONFLICT action
use crate::nodes::nodes::OnConflictAction::ONCONFLICT_UPDATE;

// ACL constant
const ACL_SELECT_FOR_UPDATE: u64 = 1 << 5;

// GUC/session
const SESSION_REPLICATION_ROLE_REPLICA: c_int = 1; // TODO: commands/variable.h
static mut SessionReplicationRole: c_int = 0;      // TODO: commands/variable.c
static mut restrict_nonsystem_relation_kind: u32 = 0; // TODO: miscadmin
const RESTRICT_RELKIND_VIEW: u32 = 1;
const FirstNormalObjectId: Oid = 16384;
const InvalidAttrNumber: AttrNumber = 0;
const InvalidOid: Oid = 0;
const ALL_EVENTS: c_int =
    (1 << CMD_INSERT as c_int) | (1 << CMD_UPDATE as c_int) | (1 << CMD_DELETE as c_int);

// ---------------------------------------------------------------------------
// Local structs
// ---------------------------------------------------------------------------

/* We use a list of these to detect recursion in RewriteQuery */
struct rewrite_event {
    relation: Oid,  /* OID of relation having rules */
    event: CmdType, /* type of rule being fired */
}

struct acquireLocksOnSubLinks_context {
    for_execute: bool, /* AcquireRewriteLocks' forExecute param */
}

struct fireRIRonSubLink_context {
    activeRIRs: *mut List,
    hasRowSecurity: bool,
}

// ---------------------------------------------------------------------------
// Stubs for unported dependencies (TODO: pg-port)
// ---------------------------------------------------------------------------

unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::table::table::table_open(relationId, lockmode as _) as _
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {
    crate::access::table::table::table_close(relation as _, lockmode as _)
}
unsafe fn relation_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::common::relation::relation_open(relationId, lockmode as _) as _
}
unsafe fn relation_close(relation: Relation, lockmode: c_int) {
    crate::access::common::relation::relation_close(relation as _, lockmode as _)
}
unsafe fn try_relation_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::common::relation::try_relation_open(relationId, lockmode as _) as _
}
unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    (*relation).rd_id
}
unsafe fn RelationGetRelationName(relation: Relation) -> *mut c_char {
    NameStr(&mut (*(*relation).rd_rel).relname)
}
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    (*relation).rd_att as *mut _
}
unsafe fn RelationGetNumberOfAttributes(relation: Relation) -> c_int {
    (*(*relation).rd_att).natts
}
unsafe fn RelationIsSecurityView(relation: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationHasCheckOption(relation: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationHasCascadedCheckOption(relation: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationHasSecurityInvoker(relation: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn NameStr(name: *mut crate::c::NameData) -> *mut c_char {
    (*name).data.as_mut_ptr()
}
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc, i)
}
unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    crate::nodes::copyfuncs::copyObjectImpl(obj as *const _ as _) as *mut T
}
unsafe fn equal(a: *const Node, b: *const Node) -> bool {
    crate::nodes::equalfuncs::equal(a as _, b as _)
}
unsafe fn exprType(expr: *const Node) -> Oid {
    crate::nodes::nodeFuncs::exprType(expr as _)
}
unsafe fn exprTypmod(expr: *const Node) -> i32 {
    crate::nodes::nodeFuncs::exprTypmod(expr as _) as _
}
unsafe fn exprCollation(expr: *const Node) -> Oid {
    crate::nodes::nodeFuncs::exprCollation(expr as _)
}
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
) -> bool {
    crate::nodes::nodeFuncs::expression_tree_walker(node as _, Some(walker), context)
}
unsafe fn query_tree_walker(
    query: *mut Query,
    walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    crate::nodes::nodeFuncs::query_tree_walker(query as _, Some(walker), context, flags)
}
unsafe fn nodeTag(node: *const Node) -> u32 {
    crate::nodes::nodes::nodeTag(node) as _
}
unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    crate::utils::palloc::pstrdup(s)
}
unsafe fn palloc0(size: usize) -> *mut c_void {
    crate::utils::palloc::palloc0(size as _)
}
unsafe fn pfree(ptr: *mut c_void) {
    crate::utils::palloc::pfree(ptr)
}
unsafe fn palloc(size: usize) -> *mut c_void {
    crate::utils::palloc::palloc(size as _)
}
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(type_oid)
}
unsafe fn check_stack_depth() {
    crate::miscadmin::check_stack_depth()
}
unsafe fn get_rte_attribute_is_dropped(rte: *mut RangeTblEntry, attno: AttrNumber) -> bool {
    crate::parser::parsetree::get_rte_attribute_is_dropped(rte as _, attno)
}
unsafe fn rt_fetch(rt_index: c_int, rtable: *mut List) -> *mut RangeTblEntry {
    crate::parser::parsetree::rt_fetch(rt_index as _, rtable as _) as _
}
unsafe fn get_parse_rowmark(
    qry: *mut Query,
    rt_index: c_int,
) -> *mut crate::nodes::parsenodes::RowMarkClause {
    crate::parser::parse_relation::get_parse_rowmark(qry as _, rt_index as _) as _
}
unsafe fn applyLockingClause(
    qry: *mut Query,
    rte_index: c_int,
    strength: LockClauseStrength,
    waitPolicy: LockWaitPolicy,
    pushedDown: bool,
) {
    crate::parser::analyze::applyLockingClause(qry as _, rte_index as _, strength as _, waitPolicy as _, pushedDown)
}
unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::getRTEPermissionInfo(rteperminfos as _, rte as _) as _
}
unsafe fn addRTEPermissionInfo(
    rteperminfos: *mut *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::addRTEPermissionInfo(rteperminfos as _, rte as _) as _
}
unsafe fn addRangeTableEntryForRelation(
    pstate: *mut c_void,
    rel: Relation,
    lockmode: c_int,
    alias: *mut crate::nodes::primnodes::Alias,
    inh: bool,
    lateral: bool,
) -> *mut crate::parser::parse_node::ParseNamespaceItem {
    crate::parser::parse_relation::addRangeTableEntryForRelation(pstate as _, rel as _, lockmode as _, alias as _, inh, lateral) as _
}
unsafe fn make_parsestate(parent: *mut c_void) -> *mut c_void {
    crate::parser::parse_node::make_parsestate(parent as _) as _
}
unsafe fn makeAlias(
    aliasname: *const c_char,
    colnames: *mut List,
) -> *mut crate::nodes::primnodes::Alias {
    crate::nodes::makefuncs::makeAlias(aliasname, colnames as _) as _
}
unsafe fn makeTargetEntry(
    expr: *mut crate::nodes::primnodes::Expr,
    resno: AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    crate::nodes::makefuncs::makeTargetEntry(expr as _, resno, resname, resjunk) as _
}
unsafe fn makeWholeRowVar(
    rte: *mut RangeTblEntry,
    varno: c_int,
    varlevelsup: c_int,
    sublevels_up: bool,
) -> *mut crate::nodes::primnodes::Var {
    crate::nodes::makefuncs::makeWholeRowVar(rte as _, varno as _, varlevelsup as _, sublevels_up) as _
}
unsafe fn makeString(str: *mut c_char) -> *mut Node {
    crate::nodes::value::makeString(str) as _
}
unsafe fn makeNullConst(consttype: Oid, consttypmod: i32, constcollid: Oid) -> *mut Node {
    crate::nodes::makefuncs::makeNullConst(consttype, consttypmod as _, constcollid) as _
}
unsafe fn flatCopyTargetEntry(src_tle: *mut TargetEntry) -> *mut TargetEntry {
    crate::nodes::makefuncs::flatCopyTargetEntry(src_tle as _) as _
}
unsafe fn get_tle_by_resno(tlist: *mut List, resno: AttrNumber) -> *mut TargetEntry {
    crate::parser::parse_relation::get_tle_by_resno(tlist as _, resno) as _
}
unsafe fn ExecCleanTargetListLength(targetList: *mut List) -> c_int {
    crate::executor::execUtils::ExecCleanTargetListLength(targetList as _)
}
unsafe fn BuildOnConflictExcludedTargetlist(
    relation: Relation,
    exclRelIndex: c_int,
) -> *mut List {
    crate::parser::analyze::BuildOnConflictExcludedTargetlist(relation as _, exclRelIndex as _) as _
}
unsafe fn rangeTableEntry_used(node: *const Node, rt_index: c_int, sublevels_up: c_int) -> bool {
    crate::rewrite::rewriteManip::rangeTableEntry_used(node as _, rt_index, sublevels_up)
}
unsafe fn OffsetVarNodes(node: *mut Node, offset: c_int, sublevels_up: c_int) {
    crate::rewrite::rewriteManip::OffsetVarNodes(node as _, offset, sublevels_up)
}
unsafe fn ChangeVarNodes(
    node: *mut Node,
    rt_index: c_int,
    new_index: c_int,
    sublevels_up: c_int,
) {
    crate::rewrite::rewriteManip::ChangeVarNodes(node as _, rt_index, new_index, sublevels_up)
}
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::util::var::contain_vars_of_level(node as _, levelsup)
}
unsafe fn AddQual(qry: *mut Query, qual: *mut Node) {
    crate::rewrite::rewriteManip::AddQual(qry as _, qual as _)
}
unsafe fn AddInvertedQual(qry: *mut Query, qual: *mut Node) {
    crate::rewrite::rewriteManip::AddInvertedQual(qry as _, qual as _)
}
unsafe fn ReplaceVarsFromTargetList(
    node: *mut Node,
    result_relation: c_int,
    sublevels_up: c_int,
    target_rte: *mut RangeTblEntry,
    targetList: *mut List,
    new_rt_index: c_int,
    nomatch_option: c_int,
    nomatch_varno: c_int,
    outer_hasSubLinks: *mut bool,
) -> *mut Node {
    crate::rewrite::rewriteManip::ReplaceVarsFromTargetList(
        node as _,
        result_relation,
        sublevels_up,
        target_rte as _,
        targetList as _,
        new_rt_index,
        nomatch_option as _,
        nomatch_varno,
        outer_hasSubLinks,
    ) as _
}
unsafe fn getInsertSelectQuery(
    parsetree: *mut Query,
    subquery_ptr: *mut *mut *mut Query,
) -> *mut Query {
    crate::rewrite::rewriteManip::getInsertSelectQuery(parsetree as _, subquery_ptr as _) as _
}
unsafe fn CombineRangeTables(
    dst_rtable: *mut *mut List,
    dst_perminfos: *mut *mut List,
    src_rtable: *mut List,
    src_perminfos: *mut List,
) {
    crate::rewrite::rewriteManip::CombineRangeTables(dst_rtable as _, dst_perminfos as _, src_rtable as _, src_perminfos as _)
}
unsafe fn checkExprHasSubLink(node: *mut Node) -> bool {
    crate::rewrite::rewriteManip::checkExprHasSubLink(node as _)
}
unsafe fn strip_implicit_coercions(node: *mut Node) -> *mut Node {
    crate::nodes::nodeFuncs::strip_implicit_coercions(node as _) as _
}
unsafe fn get_row_security_policies(
    root: *mut Query,
    rte: *mut RangeTblEntry,
    rt_index: c_int,
    securityQuals: *mut *mut List,
    withCheckOptions: *mut *mut List,
    hasRowSecurity: *mut bool,
    hasSubLinks: *mut bool,
) {
    crate::rewrite::rowsecurity::get_row_security_policies(
        root as _,
        rte as _,
        rt_index as _,
        securityQuals as _,
        withCheckOptions as _,
        hasRowSecurity,
        hasSubLinks,
    )
}
unsafe fn rewriteSearchAndCycle(cte: *mut CommonTableExpr) -> *mut CommonTableExpr {
    crate::rewrite::rewriteSearchCycle::rewriteSearchAndCycle(cte as _) as _
}
unsafe fn coerce_to_target_type(
    pstate: *mut c_void,
    expr: *mut Node,
    exprtype: Oid,
    atttype: Oid,
    atttypmod: i32,
    ccontext: c_int,
    cformat: c_int,
    location: c_int,
) -> *mut Node {
    let ccontext_enum = if ccontext == COERCION_ASSIGNMENT {
        crate::nodes::primnodes::CoercionContext::COERCION_ASSIGNMENT
    } else {
        crate::nodes::primnodes::CoercionContext::COERCION_IMPLICIT
    };
    let cformat_enum = if cformat == COERCE_IMPLICIT_CAST {
        crate::nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST
    } else {
        crate::nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL
    };
    crate::parser::parse_coerce::coerce_to_target_type(
        pstate as _,
        expr as _,
        exprtype,
        atttype,
        atttypmod as _,
        ccontext_enum,
        cformat_enum,
        location,
    ) as _
}
unsafe fn coerce_null_to_domain(
    atttype: Oid,
    atttypmod: i32,
    attcollation: Oid,
    attlen: i16,
    attbyval: bool,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_null_to_domain(atttype, atttypmod as _, attcollation, attlen as _, attbyval) as _
}
unsafe fn get_typdefault(typid: Oid) -> *mut Node {
    crate::utils::cache::lsyscache::get_typdefault(typid) as _
}
unsafe fn getIdentitySequence(rel: Relation, attrno: c_int, missing_ok: bool) -> Oid {
    crate::catalog::pg_depend::getIdentitySequence(rel as _, attrno as _, missing_ok)
}
unsafe fn TupleDescGetDefault(tupdesc: TupleDesc, attrno: c_int) -> *mut Node {
    crate::access::common::tupdesc::TupleDescGetDefault(tupdesc as _, attrno as _) as _
}
unsafe fn bms_add_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_member(a as _, x) as _
}
unsafe fn bms_del_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_del_member(a as _, x) as _
}
unsafe fn bms_is_member(x: c_int, a: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_member(x, a as _)
}
unsafe fn bms_is_empty(a: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_empty(a as _)
}
unsafe fn bms_union(a: *const Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_union(a as _, b as _) as _
}
unsafe fn bms_int_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_int_members(a as _, b as _) as _
}
unsafe fn bms_next_member(a: *const Bitmapset, prevbit: c_int) -> c_int {
    crate::nodes::bitmapset::bms_next_member(a as _, prevbit)
}
unsafe fn GetFdwRoutineForRelation(
    rel: Relation,
    asDMLonly: bool,
) -> *mut crate::foreign::fdwapi::FdwRoutine {
    crate::foreign::foreign::GetFdwRoutineForRelation(rel as _, asDMLonly) as _
}

// gettext_noop: returns the string literal as *const c_char unchanged at runtime
macro_rules! gettext_noop {
    ($s:literal) => { concat!($s, "\0").as_ptr() as *const c_char }
}

// cstr: render *const c_char for elog!/ereport! format strings
unsafe fn cstr(s: *const c_char) -> &'static str {
    if s.is_null() { return "(null)"; }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("(invalid)")
}

// COERCION / COERCE constants (parser/parse_coerce.h)
const COERCION_ASSIGNMENT: c_int = 2;
const COERCE_IMPLICIT_CAST: c_int = 2;

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ===========================================================================
// PART 2: AcquireRewriteLocks, acquireLocksOnSubLinks, rewriteRuleAction,
//         adjustJoinTreeList
// ===========================================================================

/*
 * AcquireRewriteLocks -
 *   Acquire suitable locks on all the relations mentioned in the Query.
 *   These locks will ensure that the relation schemas don't change under us
 *   while we are rewriting, planning, and executing the query.
 *
 * Caution: this may modify the querytree, therefore caller should usually
 * have done a copyObject() to make a writable copy of the querytree in the
 * current memory context.
 *
 * forExecute indicates that the query is about to be executed.  If so,
 * we'll acquire the lock modes specified in the RTE rellockmode fields.
 * If forExecute is false, AccessShareLock is acquired on all relations.
 *
 * forUpdatePushedDown indicates that a pushed-down FOR [KEY] UPDATE/SHARE
 * applies to the current subquery, requiring all rels to be opened with at
 * least RowShareLock.
 */
pub unsafe fn AcquireRewriteLocks(
    parsetree: *mut Query,
    forExecute: bool,
    forUpdatePushedDown: bool,
) {
    let mut rt_index: c_int;
    let mut context = acquireLocksOnSubLinks_context { for_execute: forExecute };

    /*
     * First, process RTEs of the current query level.
     */
    rt_index = 0;
    foreach!(l, (*parsetree).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(l)) as *mut RangeTblEntry;
        let lockmode: c_int;

        rt_index += 1;
        match (*rte).rtekind {
            RTE_RELATION => {
                /*
                 * Grab the appropriate lock type for the relation, and do not
                 * release it until end of transaction.  This protects the
                 * rewriter, planner, and executor against schema changes
                 * mid-query.
                 *
                 * If forExecute is false, ignore rellockmode and just use
                 * AccessShareLock.
                 */
                if !forExecute {
                    lockmode = AccessShareLock;
                } else if forUpdatePushedDown {
                    /* Upgrade RTE's lock mode to reflect pushed-down lock */
                    if (*rte).rellockmode == AccessShareLock {
                        (*rte).rellockmode = RowShareLock;
                    }
                    lockmode = (*rte).rellockmode;
                } else {
                    lockmode = (*rte).rellockmode;
                }

                let rel: Relation = table_open((*rte).relid, lockmode);

                /*
                 * While we have the relation open, update the RTE's relkind,
                 * just in case it changed since this rule was made.
                 */
                (*rte).relkind = (*(*rel).rd_rel).relkind;

                table_close(rel, NoLock);
            }
            RTE_JOIN => {
                /*
                 * Scan the join's alias var list to see if any columns have
                 * been dropped, and if so replace those Vars with null
                 * pointers.
                 *
                 * Since a join has only two inputs, we can expect to see
                 * multiple references to the same input RTE; optimize away
                 * multiple fetches.
                 */
                let mut newaliasvars: *mut List = NIL;
                let mut curinputvarno: c_int = 0;
                let mut curinputrte: *mut RangeTblEntry = core::ptr::null_mut();

                foreach!(ll, (*rte).joinaliasvars, {
                    let aliasitem: *mut crate::nodes::primnodes::Var =
                        lfirst(current_cell!(ll)) as *mut crate::nodes::primnodes::Var;
                    let mut aliasvar: *mut crate::nodes::primnodes::Var = aliasitem;

                    /* Look through any implicit coercion */
                    aliasvar = strip_implicit_coercions(aliasvar as *mut Node)
                        as *mut crate::nodes::primnodes::Var;

                    /*
                     * If the list item isn't a simple Var, then it must
                     * represent a merged column, ie a USING column, and so it
                     * couldn't possibly be dropped, since it's referenced in
                     * the join clause.  (Conceivably it could also be a null
                     * pointer already?  But that's OK too.)
                     */
                    if !aliasvar.is_null() && IsA!(aliasvar as *mut Node, T_Var) {
                        /*
                         * The elements of an alias list have to refer to
                         * earlier RTEs of the same rtable, because that's the
                         * order the planner builds things in.  So we already
                         * processed the referenced RTE, and so it's safe to
                         * use get_rte_attribute_is_dropped on it. (This might
                         * not hold after rewriting or planning, but it's OK
                         * to assume here.)
                         */
                        assert!((*aliasvar).varlevelsup == 0);
                        if (*aliasvar).varno != curinputvarno {
                            curinputvarno = (*aliasvar).varno;
                            if curinputvarno >= rt_index {
                                elog!(ERROR, "unexpected varno {} in JOIN RTE {}",
                                    curinputvarno, rt_index);
                            }
                            curinputrte = rt_fetch(curinputvarno as c_int, (*parsetree).rtable);
                        }
                        if get_rte_attribute_is_dropped(curinputrte, (*aliasvar).varattno) {
                            /* Replace the join alias item with a NULL */
                            newaliasvars = lappend(newaliasvars, core::ptr::null_mut::<c_void>());
                            continue; // skip the lappend below
                        }
                    }
                    newaliasvars = lappend(newaliasvars, aliasitem as *mut c_void);
                });
                (*rte).joinaliasvars = newaliasvars;
            }
            RTE_SUBQUERY => {
                /*
                 * The subquery RTE itself is all right, but we have to
                 * recurse to process the represented subquery.
                 */
                AcquireRewriteLocks(
                    (*rte).subquery,
                    forExecute,
                    forUpdatePushedDown
                        || !get_parse_rowmark(parsetree, rt_index).is_null(),
                );
            }
            _ => {
                /* ignore other types of RTEs */
            }
        }
    });

    /* Recurse into subqueries in WITH */
    foreach!(l, (*parsetree).cteList, {
        let cte: *mut CommonTableExpr = lfirst(current_cell!(l)) as *mut CommonTableExpr;
        AcquireRewriteLocks((*cte).ctequery as *mut Query, forExecute, false);
    });

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (*parsetree).hasSubLinks {
        query_tree_walker(
            parsetree,
            acquireLocksOnSubLinks,
            &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
            QTW_IGNORE_RC_SUBQUERIES,
        );
    }
}

/*
 * Walker to find sublink subqueries for AcquireRewriteLocks
 */
unsafe fn acquireLocksOnSubLinks(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let context = context as *mut acquireLocksOnSubLinks_context;
    if IsA!(node, T_SubLink) {
        let sub: *mut crate::nodes::primnodes::SubLink =
            node as *mut crate::nodes::primnodes::SubLink;

        /* Do what we came for */
        AcquireRewriteLocks(
            (*sub).subselect as *mut Query,
            (*context).for_execute,
            false,
        );
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because AcquireRewriteLocks already
     * processed subselects of subselects for us.
     */
    expression_tree_walker(node, acquireLocksOnSubLinks, context as *mut c_void)
}


/*
 * rewriteRuleAction -
 *   Rewrite the rule action with appropriate qualifiers (taken from
 *   the triggering query).
 *
 * Input arguments:
 *   parsetree - original query
 *   rule_action - one action (query) of a rule
 *   rule_qual - WHERE condition of rule, or NULL if unconditional
 *   rt_index - RT index of result relation in original query
 *   event - type of rule event
 * Output arguments:
 *   *returning_flag - set true if we rewrite RETURNING clause in rule_action
 *                   (must be initialized to false)
 * Return value:
 *   rewritten form of rule_action
 */
unsafe fn rewriteRuleAction(
    parsetree: *mut Query,
    mut rule_action: *mut Query,
    mut rule_qual: *mut Node,
    rt_index: c_int,
    event: CmdType,
    returning_flag: *mut bool,
) -> *mut Query {
    let current_varno: c_int;
    let new_varno: c_int;
    let rt_length: c_int;
    let mut sub_action: *mut Query;
    let mut sub_action_ptr: *mut *mut Query = core::ptr::null_mut();
    let mut context = acquireLocksOnSubLinks_context { for_execute: true };

    /*
     * Make modifiable copies of rule action and qual (what we're passed are
     * the stored versions in the relcache; don't touch 'em!).
     */
    rule_action = copyObject(rule_action);
    rule_qual = copyObject(rule_qual);

    /*
     * Acquire necessary locks and fix any deleted JOIN RTE entries.
     */
    AcquireRewriteLocks(rule_action, true, false);
    let _ = acquireLocksOnSubLinks(
        rule_qual,
        &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
    );

    current_varno = rt_index;
    rt_length = list_length((*parsetree).rtable);
    new_varno = PRS2_NEW_VARNO as c_int + rt_length;

    /*
     * Adjust rule action and qual to offset its varnos, so that we can merge
     * its rtable with the main parsetree's rtable.
     *
     * If the rule action is an INSERT...SELECT, the OLD/NEW rtable entries
     * will be in the SELECT part, and we have to modify that rather than the
     * top-level INSERT (kluge!).
     */
    sub_action = getInsertSelectQuery(rule_action, &mut sub_action_ptr);

    OffsetVarNodes(sub_action as *mut Node, rt_length, 0);
    OffsetVarNodes(rule_qual, rt_length, 0);
    /* but references to OLD should point at original rt_index */
    ChangeVarNodes(
        sub_action as *mut Node,
        PRS2_OLD_VARNO as c_int + rt_length,
        rt_index,
        0,
    );
    ChangeVarNodes(rule_qual, PRS2_OLD_VARNO as c_int + rt_length, rt_index, 0);

    /*
     * Mark any subquery RTEs in the rule action as LATERAL if they contain
     * Vars referring to the current query level (references to NEW/OLD).
     */
    foreach!(lc, (*sub_action).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

        if (*rte).rtekind == RTE_SUBQUERY
            && !(*rte).lateral
            && contain_vars_of_level((*rte).subquery as *mut Node, 1)
        {
            (*rte).lateral = true;
        }
    });

    /*
     * Generate expanded rtable consisting of main parsetree's rtable plus
     * rule action's rtable; this becomes the complete rtable for the rule
     * action.  Also merge RTEPermissionInfo lists.
     *
     * NOTE: because planner will destructively alter rtable and rteperminfos,
     * we must ensure that rule action's lists are separate and shares no
     * substructure with the main query's lists.  Hence do a deep copy here
     * for both.
     */
    {
        let rtable_tail: *mut List = (*sub_action).rtable;
        let perminfos_tail: *mut List = (*sub_action).rteperminfos;

        /*
         * RewriteQuery relies on the fact that RT entries from the original
         * query appear at the start of the expanded rtable, so we put the
         * action's original table at the end of the list.
         */
        (*sub_action).rtable = copyObject((*parsetree).rtable);
        (*sub_action).rteperminfos = copyObject((*parsetree).rteperminfos);
        CombineRangeTables(
            &mut (*sub_action).rtable,
            &mut (*sub_action).rteperminfos,
            rtable_tail,
            perminfos_tail,
        );
    }

    /*
     * There could have been some SubLinks in parsetree's rtable, in which
     * case we'd better mark the sub_action correctly.
     */
    if (*parsetree).hasSubLinks && !(*sub_action).hasSubLinks {
        'outer: {
            foreach!(lc, (*parsetree).rtable, {
                let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

                match (*rte).rtekind {
                    RTE_RELATION => {
                        (*sub_action).hasSubLinks =
                            checkExprHasSubLink((*rte).tablesample as *mut Node);
                    }
                    RTE_FUNCTION => {
                        (*sub_action).hasSubLinks =
                            checkExprHasSubLink((*rte).functions as *mut Node);
                    }
                    RTE_TABLEFUNC => {
                        (*sub_action).hasSubLinks =
                            checkExprHasSubLink((*rte).tablefunc as *mut Node);
                    }
                    RTE_VALUES => {
                        (*sub_action).hasSubLinks =
                            checkExprHasSubLink((*rte).values_lists as *mut Node);
                    }
                    _ => {
                        /* other RTE types don't contain bare expressions */
                    }
                }
                (*sub_action).hasSubLinks |=
                    checkExprHasSubLink((*rte).securityQuals as *mut Node);
                if (*sub_action).hasSubLinks {
                    break 'outer; /* no need to keep scanning rtable */
                }
            });
        }
    }

    /*
     * Also, we might have absorbed some RTEs with RLS conditions into the
     * sub_action.  Mark it as hasRowSecurity if so.
     */
    (*sub_action).hasRowSecurity |= (*parsetree).hasRowSecurity;

    /*
     * Each rule action's jointree should be the main parsetree's jointree
     * plus that rule's jointree, but usually *without* the original rtindex
     * that we're replacing (if present, which it won't be for INSERT).
     */
    if (*sub_action).commandType != CMD_UTILITY {
        assert!(!(*sub_action).jointree.is_null());
        let keeporig: bool = (!rangeTableEntry_used(
            (*sub_action).jointree as *const Node,
            rt_index,
            0,
        )) && (rangeTableEntry_used(rule_qual, rt_index, 0)
            || rangeTableEntry_used(
                (*(*parsetree).jointree).quals,
                rt_index,
                0,
            ));
        let newjointree: *mut List = adjustJoinTreeList(parsetree, !keeporig, rt_index);
        if newjointree != NIL {
            /*
             * If sub_action is a setop, manipulating its jointree will do no
             * good at all, because the jointree is dummy.
             */
            if !(*sub_action).setOperations.is_null() {
                ereport!(ERROR, errmsg!("conditional UNION/INTERSECT/EXCEPT statements are not implemented") /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }

            (*(*sub_action).jointree).fromlist = list_concat(
                newjointree,
                (*(*sub_action).jointree).fromlist,
            );

            /*
             * There could have been some SubLinks in newjointree, in which
             * case we'd better mark the sub_action correctly.
             */
            if (*parsetree).hasSubLinks && !(*sub_action).hasSubLinks {
                (*sub_action).hasSubLinks =
                    checkExprHasSubLink(newjointree as *mut Node);
            }
        }
    }

    /*
     * If the original query has any CTEs, copy them into the rule action.
     * But we don't need them for a utility action.
     */
    if (*parsetree).cteList != NIL && (*sub_action).commandType != CMD_UTILITY {
        /*
         * Annoying implementation restriction: because CTEs are identified by
         * name within a cteList, we can't merge a CTE from the original query
         * if it has the same name as any CTE in the rule action.
         */
        foreach!(lc, (*parsetree).cteList, {
            let cte: *mut CommonTableExpr =
                lfirst(current_cell!(lc)) as *mut CommonTableExpr;

            foreach!(lc2, (*sub_action).cteList, {
                let cte2: *mut CommonTableExpr =
                    lfirst(current_cell!(lc2)) as *mut CommonTableExpr;

                if strcmp((*cte).ctename, (*cte2).ctename) == 0 {
                    ereport!(ERROR, errmsg!("WITH query name \"{}\" appears in both a rule action and the query being rewritten",
                        cstr((*cte).ctename))
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }
            });
        });

        /*
         * OK, it's safe to combine the CTE lists.
         */
        (*sub_action).cteList = list_concat(
            (*sub_action).cteList,
            copyObject((*parsetree).cteList),
        );
        /* ... and don't forget about the associated flags */
        (*sub_action).hasRecursive |= (*parsetree).hasRecursive;
        (*sub_action).hasModifyingCTE |= (*parsetree).hasModifyingCTE;

        /*
         * If rule_action is different from sub_action (i.e., the rule action
         * is an INSERT...SELECT), then we might have just added some
         * data-modifying CTEs that are not at the top query level.  This is
         * disallowed by the parser.
         */
        if (*sub_action).hasModifyingCTE && rule_action != sub_action {
            ereport!(ERROR, errmsg!("INSERT ... SELECT rule actions are not supported for queries having data-modifying statements in WITH")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
    }

    /*
     * Event Qualification forces copying of parsetree and splitting into two
     * queries one w/rule_qual, one w/NOT rule_qual. Also add user query qual
     * onto rule action.
     */
    AddQual(sub_action, rule_qual);
    AddQual(sub_action, (*(*parsetree).jointree).quals);

    /*
     * Rewrite new.attribute with right hand side of target-list entry for
     * appropriate field name in insert/update.
     *
     * KLUGE ALERT: since ReplaceVarsFromTargetList returns a mutated copy, we
     * can't just apply it to sub_action; we have to remember to update the
     * sublink inside rule_action, too.
     */
    if (event == CMD_INSERT || event == CMD_UPDATE)
        && (*sub_action).commandType != CMD_UTILITY
    {
        sub_action = ReplaceVarsFromTargetList(
            sub_action as *mut Node,
            new_varno,
            0,
            rt_fetch(new_varno, (*sub_action).rtable),
            (*parsetree).targetList,
            (*sub_action).resultRelation,
            if event == CMD_UPDATE {
                REPLACEVARS_CHANGE_VARNO
            } else {
                REPLACEVARS_SUBSTITUTE_NULL
            },
            current_varno,
            core::ptr::null_mut(),
        ) as *mut Query;
        if !sub_action_ptr.is_null() {
            *sub_action_ptr = sub_action;
        } else {
            rule_action = sub_action;
        }
    }

    /*
     * If rule_action has a RETURNING clause, then either throw it away if the
     * triggering query has no RETURNING clause, or rewrite it to emit what
     * the triggering query's RETURNING clause asks for.
     */
    if (*parsetree).returningList.is_null() {
        (*rule_action).returningList = NIL;
    } else if !(*rule_action).returningList.is_null() {
        if *returning_flag {
            ereport!(ERROR, errmsg!("cannot have RETURNING lists in multiple rules")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
        *returning_flag = true;
        (*rule_action).returningList = ReplaceVarsFromTargetList(
            (*parsetree).returningList as *mut Node,
            (*parsetree).resultRelation,
            0,
            rt_fetch((*parsetree).resultRelation, (*parsetree).rtable),
            (*rule_action).returningList,
            (*rule_action).resultRelation,
            REPLACEVARS_REPORT_ERROR,
            0,
            &mut (*rule_action).hasSubLinks,
        ) as *mut List;

        /* use triggering query's aliases for OLD and NEW in RETURNING list */
        (*rule_action).returningOldAlias = (*parsetree).returningOldAlias;
        (*rule_action).returningNewAlias = (*parsetree).returningNewAlias;

        /*
         * There could have been some SubLinks in parsetree's returningList,
         * in which case we'd better mark the rule_action correctly.
         */
        if (*parsetree).hasSubLinks && !(*rule_action).hasSubLinks {
            (*rule_action).hasSubLinks =
                checkExprHasSubLink((*rule_action).returningList as *mut Node);
        }
    }

    rule_action
}


/*
 * Copy the query's jointree list, and optionally attempt to remove any
 * occurrence of the given rt_index as a top-level join item.
 * Returns modified jointree list --- this is a separate copy sharing no
 * nodes with the original.
 */
unsafe fn adjustJoinTreeList(
    parsetree: *mut Query,
    removert: bool,
    rt_index: c_int,
) -> *mut List {
    let mut newjointree: *mut List = copyObject((*(*parsetree).jointree).fromlist);

    if removert {
        foreach!(l, newjointree, {
            let rtr: *mut RangeTblRef = lfirst(current_cell!(l)) as *mut RangeTblRef;

            if IsA!(rtr as *mut Node, T_RangeTblRef) && (*rtr).rtindex == rt_index {
                newjointree = foreach_delete_current!(newjointree, l);
                break;
            }
        });
    }
    newjointree
}

// ===========================================================================
// PART 3: rewriteTargetListIU, process_matched_tle, get_assignment_input,
//         build_column_default, searchForDefault, findDefaultOnlyColumns,
//         rewriteValuesRTE, rewriteValuesRTEToNulls
// ===========================================================================

/*
 * rewriteTargetListIU - rewrite INSERT/UPDATE targetlist into standard form
 *
 * This has the following responsibilities:
 *
 * 1. For an INSERT, add tlist entries to compute default values for any
 *    attributes that have defaults and are not assigned to in the given tlist.
 *    Also, for both INSERT and UPDATE, replace explicit DEFAULT specifications
 *    with column default expressions.
 *
 * 2. Merge multiple entries for the same target attribute, or declare error
 *    if we can't.  Multiple entries are only allowed for INSERT/UPDATE of
 *    portions of an array or record field.
 *
 * 3. Sort the tlist into standard order: non-junk fields in order by resno,
 *    then junk fields.
 */
unsafe fn rewriteTargetListIU(
    targetList: *mut List,
    commandType: CmdType,
    override_: OverridingKind,
    target_relation: Relation,
    values_rte: *mut RangeTblEntry,
    values_rte_index: c_int,
    unused_values_attrnos: *mut *mut Bitmapset,
) -> *mut List {
    let mut new_tlist: *mut List = NIL;
    let mut junk_tlist: *mut List = NIL;
    let att_tup: Form_pg_attribute;
    let mut attrno: c_int;
    let mut next_junk_attrno: c_int;
    let numattrs: c_int;
    let mut default_only_cols: *mut Bitmapset = core::ptr::null_mut();

    /*
     * We process the normal (non-junk) attributes by scanning the input tlist
     * once and transferring TLEs into an array, then scanning the array to
     * build an output tlist.
     */
    numattrs = RelationGetNumberOfAttributes(target_relation);
    let new_tles: *mut *mut TargetEntry =
        palloc0(numattrs as usize * core::mem::size_of::<*mut TargetEntry>())
            as *mut *mut TargetEntry;
    next_junk_attrno = numattrs + 1;

    foreach!(temp, targetList, {
        let old_tle: *mut TargetEntry = lfirst(current_cell!(temp)) as *mut TargetEntry;

        if !(*old_tle).resjunk {
            /* Normal attr: stash it into new_tles[] */
            attrno = (*old_tle).resno as c_int;
            if attrno < 1 || attrno > numattrs {
                elog!(ERROR, "bogus resno {} in targetlist", attrno);
            }
            let att_tup = TupleDescAttr((*target_relation).rd_att as *mut _, attrno - 1);

            /* We can (and must) ignore deleted attributes */
            if (*att_tup).attisdropped {
                continue;
            }

            /* Merge with any prior assignment to same attribute */
            let prior = *new_tles.add((attrno - 1) as usize);
            *new_tles.add((attrno - 1) as usize) = process_matched_tle(
                old_tle,
                prior,
                NameStr(&mut (*att_tup).attname) as *const c_char,
            );
        } else {
            /*
             * Copy all resjunk tlist entries to junk_tlist, and assign them
             * resnos above the last real resno.
             */
            let mut entry = old_tle;
            /* Get the resno right, but don't copy unnecessarily */
            if (*old_tle).resno as c_int != next_junk_attrno {
                entry = flatCopyTargetEntry(old_tle);
                (*entry).resno = next_junk_attrno as AttrNumber;
            }
            junk_tlist = lappend(junk_tlist, entry as *mut c_void);
            next_junk_attrno += 1;
        }
    });

    attrno = 1;
    while attrno <= numattrs {
        let mut new_tle: *mut TargetEntry = *new_tles.add((attrno - 1) as usize);
        let mut apply_default: bool;

        let att_tup = TupleDescAttr((*target_relation).rd_att as *mut _, attrno - 1);

        /* We can (and must) ignore deleted attributes */
        if (*att_tup).attisdropped {
            attrno += 1;
            continue;
        }

        /*
         * Handle the two cases where we need to insert a default expression:
         * it's an INSERT and there's no tlist entry for the column, or the
         * tlist entry is a DEFAULT placeholder node.
         */
        apply_default = (new_tle.is_null() && commandType == CMD_INSERT)
            || (!new_tle.is_null()
                && !(*new_tle).expr.is_null()
                && IsA!((*new_tle).expr as *mut Node, T_SetToDefault));

        if commandType == CMD_INSERT {
            let mut values_attrno: c_int = 0;

            /* Source attribute number for values that come from a VALUES RTE */
            if !values_rte.is_null()
                && !new_tle.is_null()
                && IsA!((*new_tle).expr as *mut Node, T_Var)
            {
                let var: *mut crate::nodes::primnodes::Var =
                    (*new_tle).expr as *mut crate::nodes::primnodes::Var;
                if (*var).varno == values_rte_index {
                    values_attrno = (*var).varattno as c_int;
                }
            }

            /*
             * Can only insert DEFAULT into GENERATED ALWAYS identity columns,
             * unless either OVERRIDING USER VALUE or OVERRIDING SYSTEM VALUE
             * is specified.
             */
            if (*att_tup).attidentity == ATTRIBUTE_IDENTITY_ALWAYS as c_char && !apply_default {
                if override_ == OverridingKind::OVERRIDING_USER_VALUE {
                    apply_default = true;
                } else if override_ != OverridingKind::OVERRIDING_SYSTEM_VALUE {
                    /*
                     * If this column's values come from a VALUES RTE, test
                     * whether it contains only SetToDefault items.
                     */
                    if values_attrno != 0 {
                        if default_only_cols.is_null() {
                            default_only_cols = findDefaultOnlyColumns(values_rte);
                        }
                        if bms_is_member(values_attrno, default_only_cols) {
                            apply_default = true;
                        }
                    }

                    if !apply_default {
                        ereport!(ERROR, errmsg!("cannot insert a non-DEFAULT value into column \"{}\"",
                            cstr(NameStr(&mut (*att_tup).attname) as *const c_char))
                            /* C also: errcode(ERRCODE_GENERATED_ALWAYS),
                               errdetail("Column \"%s\" is an identity column defined as GENERATED ALWAYS."),
                               errhint("Use OVERRIDING SYSTEM VALUE to override.") */);
                    }
                }
            }

            /*
             * Although inserting into a GENERATED BY DEFAULT identity column
             * is allowed, apply the default if OVERRIDING USER VALUE is
             * specified.
             */
            if (*att_tup).attidentity == ATTRIBUTE_IDENTITY_BY_DEFAULT as c_char
                && override_ == OverridingKind::OVERRIDING_USER_VALUE
            {
                apply_default = true;
            }

            /*
             * Can only insert DEFAULT into generated columns, regardless of
             * any OVERRIDING clauses.
             */
            if (*att_tup).attgenerated != 0 && !apply_default {
                if values_attrno != 0 {
                    if default_only_cols.is_null() {
                        default_only_cols = findDefaultOnlyColumns(values_rte);
                    }
                    if bms_is_member(values_attrno, default_only_cols) {
                        apply_default = true;
                    }
                }

                if !apply_default {
                    ereport!(ERROR, errmsg!("cannot insert a non-DEFAULT value into column \"{}\"",
                        cstr(NameStr(&mut (*att_tup).attname) as *const c_char))
                        /* C also: errcode(ERRCODE_GENERATED_ALWAYS),
                           errdetail("Column \"%s\" is a generated column.") */);
                }
            }

            /*
             * For an INSERT from a VALUES RTE, return the attribute numbers
             * of any VALUES columns that will no longer be used.
             */
            if values_attrno != 0 && apply_default && !unused_values_attrnos.is_null() {
                *unused_values_attrnos =
                    bms_add_member(*unused_values_attrnos, values_attrno);
            }
        }

        /*
         * Updates to identity and generated columns follow the same rules,
         * except that UPDATE doesn't admit OVERRIDING clauses.
         */
        if commandType == CMD_UPDATE {
            if (*att_tup).attidentity == ATTRIBUTE_IDENTITY_ALWAYS as c_char
                && !new_tle.is_null()
                && !apply_default
            {
                ereport!(ERROR, errmsg!("column \"{}\" can only be updated to DEFAULT",
                    cstr(NameStr(&mut (*att_tup).attname) as *const c_char))
                    /* C also: errcode(ERRCODE_GENERATED_ALWAYS),
                       errdetail("Column \"%s\" is an identity column defined as GENERATED ALWAYS.") */);
            }

            if (*att_tup).attgenerated != 0 && !new_tle.is_null() && !apply_default {
                ereport!(ERROR, errmsg!("column \"{}\" can only be updated to DEFAULT",
                    cstr(NameStr(&mut (*att_tup).attname) as *const c_char))
                    /* C also: errcode(ERRCODE_GENERATED_ALWAYS),
                       errdetail("Column \"%s\" is a generated column.") */);
            }
        }

        if (*att_tup).attgenerated != 0 {
            /*
             * virtual generated column stores a null value; stored generated
             * column will be fixed in executor
             */
            new_tle = core::ptr::null_mut();
        } else if apply_default {
            let mut new_expr: *mut Node = build_column_default_fn(target_relation, attrno);

            /*
             * If there is no default (ie, default is effectively NULL), we
             * can omit the tlist entry in the INSERT case, since the planner
             * can insert a NULL for itself.  But in the UPDATE case we've got
             * to explicitly set the column to NULL.
             */
            if new_expr.is_null() {
                if commandType == CMD_INSERT {
                    new_tle = core::ptr::null_mut();
                } else {
                    new_expr = coerce_null_to_domain(
                        (*att_tup).atttypid,
                        (*att_tup).atttypmod,
                        (*att_tup).attcollation,
                        (*att_tup).attlen,
                        (*att_tup).attbyval,
                    );
                }
            }

            if !new_expr.is_null() {
                new_tle = makeTargetEntry(
                    new_expr as *mut crate::nodes::primnodes::Expr,
                    attrno as AttrNumber,
                    pstrdup(NameStr(&mut (*att_tup).attname) as *const c_char),
                    false,
                );
            }
        }

        if !new_tle.is_null() {
            new_tlist = lappend(new_tlist, new_tle as *mut c_void);
        }

        attrno += 1;
    }

    pfree(new_tles as *mut c_void);

    list_concat(new_tlist, junk_tlist)
}


/*
 * Convert a matched TLE from the original tlist into a correct new TLE.
 *
 * This routine detects and handles multiple assignments to the same target
 * attribute.  (The attribute name is needed only for error messages.)
 */
unsafe fn process_matched_tle(
    src_tle: *mut TargetEntry,
    prior_tle: *mut TargetEntry,
    attrName: *const c_char,
) -> *mut TargetEntry {
    let result: *mut TargetEntry;
    let mut coerce_expr: *mut crate::nodes::primnodes::CoerceToDomain =
        core::ptr::null_mut();
    let mut src_expr: *mut Node;
    let mut prior_expr: *mut Node;
    let src_input: *mut Node;
    let prior_input: *mut Node;
    let mut priorbottom: *mut Node;
    let newexpr: *mut Node;

    if prior_tle.is_null() {
        /*
         * Normal case where this is the first assignment to the attribute.
         */
        return src_tle;
    }

    /*----------
     * Multiple assignments to same attribute.  Allow only if all are
     * FieldStore or SubscriptingRef assignment operations.  This is a bit
     * tricky because what we may actually be looking at is a nest of
     * such nodes; consider
     *   UPDATE tab SET col.fld1.subfld1 = x, col.fld2.subfld2 = y
     * The two expressions produced by the parser will look like
     *   FieldStore(col, fld1, FieldStore(placeholder, subfld1, x))
     *   FieldStore(col, fld2, FieldStore(placeholder, subfld2, y))
     * However, we can ignore the substructure and just consider the top
     * FieldStore or SubscriptingRef from each assignment.
     *----------
     */
    src_expr = (*src_tle).expr as *mut Node;
    prior_expr = (*prior_tle).expr as *mut Node;

    if !src_expr.is_null()
        && IsA!(src_expr, T_CoerceToDomain)
        && !prior_expr.is_null()
        && IsA!(prior_expr, T_CoerceToDomain)
        && (*(src_expr as *mut crate::nodes::primnodes::CoerceToDomain)).resulttype
            == (*(prior_expr as *mut crate::nodes::primnodes::CoerceToDomain)).resulttype
    {
        /* we assume without checking that resulttypmod/resultcollid match */
        coerce_expr = src_expr as *mut crate::nodes::primnodes::CoerceToDomain;
        src_expr = (*(src_expr as *mut crate::nodes::primnodes::CoerceToDomain)).arg
            as *mut Node;
        prior_expr = (*(prior_expr as *mut crate::nodes::primnodes::CoerceToDomain)).arg
            as *mut Node;
    }

    src_input = get_assignment_input(src_expr);
    prior_input = get_assignment_input(prior_expr);
    if src_input.is_null()
        || prior_input.is_null()
        || exprType(src_expr) != exprType(prior_expr)
    {
        ereport!(ERROR, errmsg!("multiple assignments to same column \"{}\"",
            cstr(attrName)) /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
    }

    /*
     * Prior TLE could be a nest of assignments if we do this more than once.
     */
    priorbottom = prior_input;
    loop {
        let newbottom = get_assignment_input(priorbottom);
        if newbottom.is_null() {
            break; /* found the original Var reference */
        }
        priorbottom = newbottom;
    }
    if !equal(priorbottom, src_input) {
        ereport!(ERROR, errmsg!("multiple assignments to same column \"{}\"",
            cstr(attrName)) /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
    }

    /*
     * Looks OK to nest 'em.
     */
    let final_expr: *mut Node;
    if IsA!(src_expr, T_FieldStore) {
        let fstore: *mut crate::nodes::primnodes::FieldStore =
            makeNode!(crate::nodes::primnodes::FieldStore, T_FieldStore) as *mut crate::nodes::primnodes::FieldStore;

        if IsA!(prior_expr, T_FieldStore) {
            /* combine the two */
            memcpy(
                fstore as *mut c_void,
                prior_expr as *const c_void,
                core::mem::size_of::<crate::nodes::primnodes::FieldStore>(),
            );
            (*fstore).newvals = list_concat_copy(
                (*(prior_expr as *mut crate::nodes::primnodes::FieldStore)).newvals,
                (*(src_expr as *mut crate::nodes::primnodes::FieldStore)).newvals,
            );
            (*fstore).fieldnums = list_concat_copy(
                (*(prior_expr as *mut crate::nodes::primnodes::FieldStore)).fieldnums,
                (*(src_expr as *mut crate::nodes::primnodes::FieldStore)).fieldnums,
            );
        } else {
            /* general case, just nest 'em */
            memcpy(
                fstore as *mut c_void,
                src_expr as *const c_void,
                core::mem::size_of::<crate::nodes::primnodes::FieldStore>(),
            );
            (*fstore).arg = prior_expr as *mut crate::nodes::primnodes::Expr;
        }
        final_expr = fstore as *mut Node;
    } else if IsA!(src_expr, T_SubscriptingRef) {
        let sbsref: *mut crate::nodes::primnodes::SubscriptingRef =
            makeNode!(crate::nodes::primnodes::SubscriptingRef, T_SubscriptingRef)
                as *mut crate::nodes::primnodes::SubscriptingRef;

        memcpy(
            sbsref as *mut c_void,
            src_expr as *const c_void,
            core::mem::size_of::<crate::nodes::primnodes::SubscriptingRef>(),
        );
        (*sbsref).refexpr = prior_expr as *mut crate::nodes::primnodes::Expr;
        final_expr = sbsref as *mut Node;
    } else {
        elog!(ERROR, "cannot happen");
        final_expr = core::ptr::null_mut();
    }

    let newexpr = if !coerce_expr.is_null() {
        /* put back the CoerceToDomain */
        let newcoerce: *mut crate::nodes::primnodes::CoerceToDomain =
            makeNode!(crate::nodes::primnodes::CoerceToDomain, T_CoerceToDomain)
                as *mut crate::nodes::primnodes::CoerceToDomain;
        memcpy(
            newcoerce as *mut c_void,
            coerce_expr as *const c_void,
            core::mem::size_of::<crate::nodes::primnodes::CoerceToDomain>(),
        );
        (*newcoerce).arg = final_expr as *mut crate::nodes::primnodes::Expr;
        newcoerce as *mut Node
    } else {
        final_expr
    };

    result = flatCopyTargetEntry(src_tle);
    (*result).expr = newexpr as *mut crate::nodes::primnodes::Expr;
    result
}

/*
 * If node is an assignment node, return its input; else return NULL
 */
unsafe fn get_assignment_input(node: *mut Node) -> *mut Node {
    if node.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(node, T_FieldStore) {
        let fstore: *mut crate::nodes::primnodes::FieldStore =
            node as *mut crate::nodes::primnodes::FieldStore;
        return (*fstore).arg as *mut Node;
    } else if IsA!(node, T_SubscriptingRef) {
        let sbsref: *mut crate::nodes::primnodes::SubscriptingRef =
            node as *mut crate::nodes::primnodes::SubscriptingRef;
        if (*sbsref).refassgnexpr.is_null() {
            return core::ptr::null_mut();
        }
        return (*sbsref).refexpr as *mut Node;
    }
    core::ptr::null_mut()
}


/*
 * Make an expression tree for the default value for a column.
 *
 * If there is no default, return a NULL instead.
 */
#[no_mangle]
pub unsafe fn build_column_default(rel: Relation, attrno: c_int) -> *mut Node {
    build_column_default_fn(rel, attrno)
}

unsafe fn build_column_default_fn(rel: Relation, attrno: c_int) -> *mut Node {
    let rd_att: TupleDesc = (*rel).rd_att as *mut _;
    let att_tup: Form_pg_attribute = TupleDescAttr(rd_att, attrno - 1);
    let atttype: Oid = (*att_tup).atttypid;
    let atttypmod: i32 = (*att_tup).atttypmod;
    let mut expr: *mut Node = core::ptr::null_mut();
    let exprtype_val: Oid;

    if (*att_tup).attidentity != 0 {
        let nve: *mut crate::nodes::primnodes::NextValueExpr =
            makeNode!(crate::nodes::primnodes::NextValueExpr, T_NextValueExpr)
                as *mut crate::nodes::primnodes::NextValueExpr;

        (*nve).seqid = getIdentitySequence(rel, attrno, false);
        (*nve).typeId = (*att_tup).atttypid;

        return nve as *mut Node;
    }

    /*
     * If relation has a default for this column, fetch that expression.
     */
    if (*att_tup).atthasdef {
        expr = TupleDescGetDefault(rd_att, attrno);
        if expr.is_null() {
            elog!(
                ERROR,
                "default expression not found for attribute {} of relation \"{}\"",
                attrno,
                cstr(RelationGetRelationName(rel) as *const c_char)
            );
        }
    }

    /*
     * No per-column default, so look for a default for the type itself.  But
     * not for generated columns.
     */
    if expr.is_null() && (*att_tup).attgenerated == 0 {
        expr = get_typdefault(atttype);
    }

    if expr.is_null() {
        return core::ptr::null_mut(); /* No default anywhere */
    }

    /*
     * Make sure the value is coerced to the target column type; this will
     * generally be true already, but there seem to be some corner cases
     * involving domain defaults where it might not be true.
     */
    exprtype_val = exprType(expr);

    expr = coerce_to_target_type(
        core::ptr::null_mut(), /* no UNKNOWN params here */
        expr,
        exprtype_val,
        atttype,
        atttypmod,
        COERCION_ASSIGNMENT,
        COERCE_IMPLICIT_CAST,
        -1,
    );
    if expr.is_null() {
        ereport!(ERROR, errmsg!("column \"{}\" is of type {} but default expression is of type {}",
            cstr(NameStr(&mut (*att_tup).attname) as *const c_char),
            cstr(format_type_be(atttype)),
            cstr(format_type_be(exprtype_val)))
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
               errhint("You will need to rewrite or cast the expression.") */);
    }

    expr
}


/* Does VALUES RTE contain any SetToDefault items? */
unsafe fn searchForDefault(rte: *mut RangeTblEntry) -> bool {
    foreach!(lc, (*rte).values_lists, {
        let sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;

        foreach!(lc2, sublist, {
            let col: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;

            if IsA!(col, T_SetToDefault) {
                return true;
            }
        });
    });
    false
}


/*
 * Search a VALUES RTE for columns that contain only SetToDefault items,
 * returning a Bitmapset containing the attribute numbers of any such columns.
 */
unsafe fn findDefaultOnlyColumns(rte: *mut RangeTblEntry) -> *mut Bitmapset {
    let mut default_only_cols: *mut Bitmapset = core::ptr::null_mut();

    foreach!(lc, (*rte).values_lists, {
        let sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;
        let mut i: c_int;

        if default_only_cols.is_null() {
            /* Populate the initial result bitmap from the first row */
            i = 0;
            foreach!(lc2, sublist, {
                let col: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;

                i += 1;
                if IsA!(col, T_SetToDefault) {
                    default_only_cols = bms_add_member(default_only_cols, i);
                }
            });
        } else {
            /* Update the result bitmap from this next row */
            i = 0;
            foreach!(lc2, sublist, {
                let col: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;

                i += 1;
                if !IsA!(col, T_SetToDefault) {
                    default_only_cols = bms_del_member(default_only_cols, i);
                }
            });
        }

        /*
         * If no column in the rows read so far contains only DEFAULT items,
         * we are done.
         */
        if bms_is_empty(default_only_cols) {
            break;
        }
    });

    default_only_cols
}


/*
 * When processing INSERT ... VALUES with a VALUES RTE (ie, multiple VALUES
 * lists), we have to replace any DEFAULT items in the VALUES lists with
 * the appropriate default expressions.
 *
 * Returns true if all DEFAULT items were replaced, and false if some were
 * left untouched.
 */
unsafe fn rewriteValuesRTE(
    parsetree: *mut Query,
    rte: *mut RangeTblEntry,
    rti: c_int,
    target_relation: Relation,
    unused_cols: *mut Bitmapset,
) -> bool {
    let mut newValues: *mut List;
    let mut isAutoUpdatableView: bool;
    let mut allReplaced: bool;
    let numattrs: c_int;
    let attrnos: *mut c_int;

    /* Steps below are not sensible for non-INSERT queries */
    assert!((*parsetree).commandType == CMD_INSERT);
    assert!((*rte).rtekind == RTE_VALUES);

    /*
     * Rebuilding all the lists is a pretty expensive proposition in a big
     * VALUES list, and it's a waste of time if there aren't any DEFAULT
     * placeholders.  So first scan to see if there are any.
     */
    if !searchForDefault(rte) {
        return true; /* nothing to do */
    }

    /*
     * Scan the targetlist for entries referring to the VALUES RTE, and note
     * the target attributes.
     */
    numattrs = list_length(linitial((*rte).values_lists) as *mut List);
    attrnos = palloc0(numattrs as usize * core::mem::size_of::<c_int>()) as *mut c_int;

    foreach!(lc, (*parsetree).targetList, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        if IsA!((*tle).expr as *mut Node, T_Var) {
            let var: *mut crate::nodes::primnodes::Var =
                (*tle).expr as *mut crate::nodes::primnodes::Var;

            if (*var).varno == rti {
                let attrno = (*var).varattno as c_int;
                assert!(attrno >= 1 && attrno <= numattrs);
                *attrnos.add((attrno - 1) as usize) = (*tle).resno as c_int;
            }
        }
    });

    /*
     * Check if the target relation is an auto-updatable view.
     */
    isAutoUpdatableView = false;
    if (*(*target_relation).rd_rel).relkind == RELKIND_VIEW
        && !view_has_instead_trigger(target_relation, CMD_INSERT, NIL)
    {
        let mut hasUpdate: bool = false;
        let mut found: bool = false;
        let locks: *mut List = matchLocks_impl(
            CMD_INSERT,
            target_relation,
            (*parsetree).resultRelation,
            parsetree,
            &mut hasUpdate,
        );

        foreach!(l, locks, {
            let rule_lock: *mut RewriteRule = lfirst(current_cell!(l)) as *mut RewriteRule;

            if (*rule_lock).isInstead && (*rule_lock).qual.is_null() {
                found = true;
                break;
            }
        });

        /*
         * If we didn't find an unconditional DO INSTEAD rule, assume that the
         * view is auto-updatable.
         */
        if !found {
            isAutoUpdatableView = true;
        }
    }

    newValues = NIL;
    allReplaced = true;
    foreach!(lc, (*rte).values_lists, {
        let sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;
        let mut newList: *mut List = NIL;
        let mut i: c_int = 0;

        assert!(list_length(sublist) == numattrs);

        foreach!(lc2, sublist, {
            let col: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;
            let attrno: c_int = *attrnos.add(i as usize);
            i += 1;

            if IsA!(col, T_SetToDefault) {
                let att_tup: Form_pg_attribute;
                let mut new_expr: *mut Node;

                /*
                 * If this column isn't used, just replace the DEFAULT with
                 * NULL (attrno will be 0 in this case).
                 */
                if bms_is_member(i, unused_cols) {
                    let def: *mut crate::nodes::primnodes::SetToDefault =
                        col as *mut crate::nodes::primnodes::SetToDefault;

                    newList = lappend(
                        newList,
                        makeNullConst((*def).typeId, (*def).typeMod, (*def).collation) as *mut c_void,
                    );
                    continue;
                }

                if attrno == 0 {
                    elog!(ERROR, "cannot set value in column {} to DEFAULT", i);
                }
                assert!(
                    attrno > 0
                        && attrno <= (*(*target_relation).rd_att).natts as c_int
                );
                att_tup = TupleDescAttr((*target_relation).rd_att as *mut _, attrno - 1);

                if !(*att_tup).attisdropped {
                    new_expr = build_column_default_fn(target_relation, attrno);
                } else {
                    new_expr = core::ptr::null_mut(); /* force a NULL if dropped */
                }

                /*
                 * If there is no default (ie, default is effectively NULL),
                 * we've got to explicitly set the column to NULL, unless the
                 * target relation is an auto-updatable view.
                 */
                if new_expr.is_null() {
                    if isAutoUpdatableView {
                        /* Leave the value untouched */
                        newList = lappend(newList, col as *mut c_void);
                        allReplaced = false;
                        continue;
                    }

                    new_expr = coerce_null_to_domain(
                        (*att_tup).atttypid,
                        (*att_tup).atttypmod,
                        (*att_tup).attcollation,
                        (*att_tup).attlen,
                        (*att_tup).attbyval,
                    );
                }
                newList = lappend(newList, new_expr as *mut c_void);
            } else {
                newList = lappend(newList, col as *mut c_void);
            }
        });
        newValues = lappend(newValues, newList as *mut c_void);
    });
    (*rte).values_lists = newValues;

    pfree(attrnos as *mut c_void);

    allReplaced
}

/*
 * Mop up any remaining DEFAULT items in the given VALUES RTE by
 * replacing them with NULL constants.
 *
 * This is used for the product queries generated by DO ALSO rules attached to
 * an auto-updatable view.
 */
unsafe fn rewriteValuesRTEToNulls(parsetree: *mut Query, rte: *mut RangeTblEntry) {
    let mut newValues: *mut List = NIL;

    foreach!(lc, (*rte).values_lists, {
        let sublist: *mut List = lfirst(current_cell!(lc)) as *mut List;
        let mut newList: *mut List = NIL;

        foreach!(lc2, sublist, {
            let col: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;

            if IsA!(col, T_SetToDefault) {
                let def: *mut crate::nodes::primnodes::SetToDefault =
                    col as *mut crate::nodes::primnodes::SetToDefault;

                newList = lappend(
                    newList,
                    makeNullConst((*def).typeId, (*def).typeMod, (*def).collation) as *mut c_void,
                );
            } else {
                newList = lappend(newList, col as *mut c_void);
            }
        });
        newValues = lappend(newValues, newList as *mut c_void);
    });
    (*rte).values_lists = newValues;
}

// ===========================================================================
// PART 4: matchLocks, ApplyRetrieveRule, markQueryForLocking,
//         fireRIRonSubLink, fireRIRrules, CopyAndAddInvertedQual,
//         fireRules, get_view_query, view_has_instead_trigger
// ===========================================================================

/*
 * matchLocks -
 *   match a relation's list of locks and returns the matching rules
 */
unsafe fn matchLocks_impl(
    event: CmdType,
    relation: Relation,
    varno: c_int,
    parsetree: *mut Query,
    hasUpdate: *mut bool,
) -> *mut List {
    let rulelocks: *mut RuleLock = (*relation).rd_rules as *mut RuleLock;
    let mut matching_locks: *mut List = NIL;
    let nlocks: c_int;
    let mut i: c_int;

    if rulelocks.is_null() {
        return NIL;
    }

    if (*parsetree).commandType != CMD_SELECT {
        if (*parsetree).resultRelation != varno {
            return NIL;
        }
    }

    nlocks = (*rulelocks).numLocks;

    i = 0;
    while i < nlocks {
        let oneLock: *mut RewriteRule = *(*rulelocks).rules.add(i as usize);

        if (*oneLock).event == CMD_UPDATE {
            *hasUpdate = true;
        }

        /*
         * Suppress ON INSERT/UPDATE/DELETE rules that are disabled or
         * configured to not fire during the current session's replication
         * role. ON SELECT rules will always be applied in order to keep views
         * working even in LOCAL or REPLICA role.
         */
        if (*oneLock).event != CMD_SELECT {
            if SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA {
                if (*oneLock).enabled == RULE_FIRES_ON_ORIGIN
                    || (*oneLock).enabled == RULE_DISABLED
                {
                    i += 1;
                    continue;
                }
            } else {
                /* ORIGIN or LOCAL ROLE */
                if (*oneLock).enabled == RULE_FIRES_ON_REPLICA
                    || (*oneLock).enabled == RULE_DISABLED
                {
                    i += 1;
                    continue;
                }
            }

            /* Non-SELECT rules are not supported for MERGE */
            if (*parsetree).commandType == CMD_MERGE {
                ereport!(ERROR, errmsg!("cannot execute MERGE on relation \"{}\"",
                    cstr(RelationGetRelationName(relation) as *const c_char))
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errdetail("MERGE is not supported for relations with rules.") */);
            }
        }

        if (*oneLock).event == event {
            if (*parsetree).commandType != CMD_SELECT
                || rangeTableEntry_used(parsetree as *const Node, varno, 0)
            {
                matching_locks = lappend(matching_locks, oneLock as *mut c_void);
            }
        }

        i += 1;
    }

    matching_locks
}


unsafe fn matchLocks(
    event: CmdType,
    relation: Relation,
    varno: c_int,
    parsetree: *mut Query,
    hasUpdate: *mut bool,
) -> *mut List {
    matchLocks_impl(event, relation, varno, parsetree, hasUpdate)
}

/*
 * ApplyRetrieveRule - expand an ON SELECT rule
 */
unsafe fn ApplyRetrieveRule(
    parsetree: *mut Query,
    rule: *mut RewriteRule,
    rt_index: c_int,
    relation: Relation,
    activeRIRs: *mut List,
) -> *mut Query {
    let mut rule_action: *mut Query;
    let mut rte: *mut RangeTblEntry;
    let rc: *mut crate::nodes::parsenodes::RowMarkClause;
    let numCols: c_int;

    if list_length((*rule).actions) != 1 {
        elog!(ERROR, "expected just one rule action");
    }
    if !(*rule).qual.is_null() {
        elog!(ERROR, "cannot handle qualified ON SELECT rule");
    }

    /* Check if the expansion of non-system views are restricted */
    if (restrict_nonsystem_relation_kind & RESTRICT_RELKIND_VIEW) != 0
        && RelationGetRelid(relation) >= FirstNormalObjectId
    {
        ereport!(ERROR, errmsg!("access to non-system view \"{}\" is restricted",
            cstr(RelationGetRelationName(relation) as *const c_char))
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
    }

    if rt_index == (*parsetree).resultRelation {
        /*
         * We have a view as the result relation of the query, and it wasn't
         * rewritten by any rule.
         *
         * For INSERT, we needn't do anything.  The unmodified RTE will serve
         * fine as the result relation.
         *
         * For UPDATE/DELETE/MERGE, we need to expand the view so as to have
         * source data for the operation.
         */
        if (*parsetree).commandType == CMD_INSERT {
            return parsetree;
        } else if (*parsetree).commandType == CMD_UPDATE
            || (*parsetree).commandType == CMD_DELETE
            || (*parsetree).commandType == CMD_MERGE
        {
            let newrte: *mut RangeTblEntry;
            let var: *mut crate::nodes::primnodes::Var;
            let tle: *mut TargetEntry;

            rte = rt_fetch(rt_index, (*parsetree).rtable);
            newrte = copyObject(rte);
            (*parsetree).rtable = lappend((*parsetree).rtable, newrte as *mut c_void);
            (*parsetree).resultRelation = list_length((*parsetree).rtable);
            /* parsetree->mergeTargetRelation unchanged (use expanded view) */

            /*
             * For the most part, Vars referencing the view should remain as
             * they are, meaning that they implicitly represent OLD values.
             * But in the RETURNING list if any, we want such Vars to
             * represent NEW values.
             *
             * Since ChangeVarNodes scribbles on the tree in-place, copy the
             * RETURNING list first for safety.
             */
            (*parsetree).returningList = copyObject((*parsetree).returningList);
            ChangeVarNodes(
                (*parsetree).returningList as *mut Node,
                rt_index,
                (*parsetree).resultRelation,
                0,
            );

            /*
             * To allow the executor to compute the original view row to pass
             * to the INSTEAD OF trigger, we add a resjunk whole-row Var
             * referencing the original RTE.
             */
            var = makeWholeRowVar(rte, rt_index, 0, false);
            tle = makeTargetEntry(
                var as *mut crate::nodes::primnodes::Expr,
                (list_length((*parsetree).targetList) + 1) as AttrNumber,
                pstrdup(c"wholerow".as_ptr()),
                true,
            );

            (*parsetree).targetList = lappend((*parsetree).targetList, tle as *mut c_void);

            /* Now, continue with expanding the original view RTE */
        } else {
            elog!(
                ERROR,
                "unrecognized commandType: {}",
                (*parsetree).commandType as c_int
            );
        }
    }

    /*
     * Check if there's a FOR [KEY] UPDATE/SHARE clause applying to this view.
     */
    rc = get_parse_rowmark(parsetree, rt_index);

    /*
     * Make a modifiable copy of the view query, and acquire needed locks on
     * the relations it mentions.  Force at least RowShareLock for all such
     * rels if there's a FOR [KEY] UPDATE/SHARE clause affecting this view.
     */
    rule_action = copyObject(linitial((*rule).actions) as *mut Query);

    AcquireRewriteLocks(rule_action, true, !rc.is_null());

    /*
     * If FOR [KEY] UPDATE/SHARE of view, mark all the contained tables as
     * implicit FOR [KEY] UPDATE/SHARE, the same as the parser would have done
     * if the view's subquery had been written out explicitly.
     */
    if !rc.is_null() {
        markQueryForLocking(
            rule_action,
            (*rule_action).jointree as *mut Node,
            (*rc).strength,
            (*rc).waitPolicy,
            true,
        );
    }

    /*
     * Recursively expand any view references inside the view.
     */
    rule_action = fireRIRrules_impl(rule_action, activeRIRs);

    /*
     * Make sure the query is marked as having row security if the view query
     * does.
     */
    (*parsetree).hasRowSecurity |= (*rule_action).hasRowSecurity;

    /*
     * Now, plug the view query in as a subselect, converting the relation's
     * original RTE to a subquery RTE.
     */
    rte = rt_fetch(rt_index, (*parsetree).rtable);

    (*rte).rtekind = RTE_SUBQUERY;
    (*rte).subquery = rule_action;
    (*rte).security_barrier = RelationIsSecurityView(relation);

    /*
     * Clear fields that should not be set in a subquery RTE.  Note that we
     * leave the relid, relkind, rellockmode, and perminfoindex fields set.
     */
    (*rte).tablesample = core::ptr::null_mut();
    (*rte).inh = false; /* must not be set for a subquery */

    /*
     * Since we allow CREATE OR REPLACE VIEW to add columns to a view, the
     * rule_action might emit more columns than we expected when the current
     * query was parsed.  Patch things up if necessary.
     */
    numCols = ExecCleanTargetListLength((*rule_action).targetList);
    while list_length((*(*rte).eref).colnames) < numCols {
        (*(*rte).eref).colnames = lappend(
            (*(*rte).eref).colnames,
            makeString(pstrdup(c"?column?".as_ptr())) as *mut c_void,
        );
    }

    parsetree
}


/*
 * Recursively mark all relations used by a view as FOR [KEY] UPDATE/SHARE.
 */
unsafe fn markQueryForLocking(
    qry: *mut Query,
    jtnode: *mut Node,
    strength: LockClauseStrength,
    waitPolicy: LockWaitPolicy,
    pushedDown: bool,
) {
    if jtnode.is_null() {
        return;
    }
    if IsA!(jtnode, T_RangeTblRef) {
        let rti: c_int = (*(jtnode as *mut RangeTblRef)).rtindex;
        let rte: *mut RangeTblEntry = rt_fetch(rti, (*qry).rtable);

        if (*rte).rtekind == RTE_RELATION {
            let perminfo: *mut RTEPermissionInfo;

            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);

            perminfo = getRTEPermissionInfo((*qry).rteperminfos, rte);
            (*perminfo).requiredPerms |= ACL_SELECT_FOR_UPDATE;
        } else if (*rte).rtekind == RTE_SUBQUERY {
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);
            /* FOR UPDATE/SHARE of subquery is propagated to subquery's rels */
            markQueryForLocking(
                (*rte).subquery,
                (*(*rte).subquery).jointree as *mut Node,
                strength,
                waitPolicy,
                true,
            );
        }
        /* other RTE types are unaffected by FOR UPDATE */
    } else if IsA!(jtnode, T_FromExpr) {
        let f: *mut crate::nodes::primnodes::FromExpr =
            jtnode as *mut crate::nodes::primnodes::FromExpr;

        foreach!(l, (*f).fromlist, {
            markQueryForLocking(
                qry,
                lfirst(current_cell!(l)) as *mut Node,
                strength,
                waitPolicy,
                pushedDown,
            );
        });
    } else if IsA!(jtnode, T_JoinExpr) {
        let j: *mut crate::nodes::primnodes::JoinExpr =
            jtnode as *mut crate::nodes::primnodes::JoinExpr;

        markQueryForLocking(qry, (*j).larg as *mut Node, strength, waitPolicy, pushedDown);
        markQueryForLocking(qry, (*j).rarg as *mut Node, strength, waitPolicy, pushedDown);
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode));
    }
}


/*
 * fireRIRonSubLink -
 *   Apply fireRIRrules() to each SubLink (subselect in expression) found
 *   in the given tree.
 *
 * NOTE: although this has the form of a walker, we cheat and modify the
 * SubLink nodes in-place.
 */
unsafe fn fireRIRonSubLink(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let context = context as *mut fireRIRonSubLink_context;
    if IsA!(node, T_SubLink) {
        let sub: *mut crate::nodes::primnodes::SubLink =
            node as *mut crate::nodes::primnodes::SubLink;

        /* Do what we came for */
        (*sub).subselect =
            fireRIRrules_impl((*sub).subselect as *mut Query, (*context).activeRIRs)
                as *mut Node;

        /*
         * Remember if any of the sublinks have row security.
         */
        (*context).hasRowSecurity |= (*((*sub).subselect as *mut Query)).hasRowSecurity;

        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because fireRIRrules already processed
     * subselects of subselects for us.
     */
    expression_tree_walker(node, fireRIRonSubLink, context as *mut c_void)
}


/*
 * fireRIRrules -
 *   Apply all RIR rules on each rangetable entry in the given query
 *
 * activeRIRs is a list of the OIDs of views we're already processing RIR
 * rules for, used to detect/reject recursion.
 */
unsafe fn fireRIRrules_impl(
    mut parsetree: *mut Query,
    mut activeRIRs: *mut List,
) -> *mut Query {
    let origResultRelation: c_int = (*parsetree).resultRelation;
    let mut rt_index: c_int;

    /*
     * Expand SEARCH and CYCLE clauses in CTEs.
     */
    foreach!(lc, (*parsetree).cteList, {
        let mut cte: *mut CommonTableExpr =
            lfirst(current_cell!(lc)) as *mut CommonTableExpr;

        if !(*cte).search_clause.is_null() || !(*cte).cycle_clause.is_null() {
            cte = rewriteSearchAndCycle(cte);
            // Update the list cell in-place (lfirst(lc) = cte in C)
            *(current_cell!(lc) as *mut *mut CommonTableExpr) = cte;
        }
    });

    /*
     * don't try to convert this into a foreach loop, because rtable list can
     * get changed each time through...
     */
    rt_index = 0;
    while rt_index < list_length((*parsetree).rtable) {
        let rte: *mut RangeTblEntry;
        let rel: Relation;
        let mut locks: *mut List;
        let rules: *mut RuleLock;
        let mut rule: *mut RewriteRule;
        let mut i: c_int;

        rt_index += 1;

        rte = rt_fetch(rt_index, (*parsetree).rtable);

        /*
         * A subquery RTE can't have associated rules, so there's nothing to
         * do to this level of the query, but we must recurse into the
         * subquery to expand any rule references in it.
         */
        if (*rte).rtekind == RTE_SUBQUERY {
            (*rte).subquery = fireRIRrules_impl((*rte).subquery, activeRIRs);

            /*
             * While we are here, make sure the query is marked as having row
             * security if any of its subqueries do.
             */
            (*parsetree).hasRowSecurity |= (*(*rte).subquery).hasRowSecurity;

            continue;
        }

        /*
         * Joins and other non-relation RTEs can be ignored completely.
         */
        if (*rte).rtekind != RTE_RELATION {
            continue;
        }

        /*
         * Always ignore RIR rules for materialized views referenced in
         * queries.
         */
        if (*rte).relkind == RELKIND_MATVIEW {
            continue;
        }

        /*
         * In INSERT ... ON CONFLICT, ignore the EXCLUDED pseudo-relation.
         */
        if !(*parsetree).onConflict.is_null()
            && rt_index == (*(*parsetree).onConflict).exclRelIndex
        {
            continue;
        }

        /*
         * If the table is not referenced in the query, then we ignore it.
         */
        if rt_index != (*parsetree).resultRelation
            && !rangeTableEntry_used(parsetree as *const Node, rt_index, 0)
        {
            continue;
        }

        /*
         * Also, if this is a new result relation introduced by
         * ApplyRetrieveRule, we don't want to do anything more with it.
         */
        if rt_index == (*parsetree).resultRelation && rt_index != origResultRelation {
            continue;
        }

        /*
         * We can use NoLock here since either the parser or
         * AcquireRewriteLocks should have locked the rel already.
         */
        rel = table_open((*rte).relid, NoLock);

        /*
         * Collect the RIR rules that we must apply
         */
        rules = (*rel).rd_rules as *mut RuleLock;
        if !rules.is_null() {
            locks = NIL;
            i = 0;
            while i < (*rules).numLocks {
                rule = *(*rules).rules.add(i as usize);
                if (*rule).event != CMD_SELECT {
                    i += 1;
                    continue;
                }
                locks = lappend(locks, rule as *mut c_void);
                i += 1;
            }

            /*
             * If we found any, apply them --- but first check for recursion!
             */
            if locks != NIL {
                if list_member_oid(activeRIRs, RelationGetRelid(rel)) {
                    ereport!(ERROR, errmsg!("infinite recursion detected in rules for relation \"{}\"",
                        cstr(RelationGetRelationName(rel) as *const c_char))
                        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */);
                }
                activeRIRs = lappend_oid(activeRIRs, RelationGetRelid(rel));

                foreach!(l, locks, {
                    rule = lfirst(current_cell!(l)) as *mut RewriteRule;

                    parsetree = ApplyRetrieveRule(
                        parsetree,
                        rule,
                        rt_index,
                        rel,
                        activeRIRs,
                    );
                });

                activeRIRs = list_delete_last(activeRIRs);
            }
        }

        table_close(rel, NoLock);
    }

    /* Recurse into subqueries in WITH */
    foreach!(lc, (*parsetree).cteList, {
        let cte: *mut CommonTableExpr = lfirst(current_cell!(lc)) as *mut CommonTableExpr;

        (*cte).ctequery = fireRIRrules_impl((*cte).ctequery as *mut Query, activeRIRs)
            as *mut Node;

        /*
         * While we are here, make sure the query is marked as having row
         * security if any of its CTEs do.
         */
        (*parsetree).hasRowSecurity |= (*((*cte).ctequery as *mut Query)).hasRowSecurity;
    });

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (*parsetree).hasSubLinks {
        let mut fire_context = fireRIRonSubLink_context {
            activeRIRs,
            hasRowSecurity: false,
        };

        query_tree_walker(
            parsetree,
            fireRIRonSubLink,
            &mut fire_context as *mut fireRIRonSubLink_context as *mut c_void,
            QTW_IGNORE_RC_SUBQUERIES,
        );

        /*
         * Make sure the query is marked as having row security if any of its
         * sublinks do.
         */
        (*parsetree).hasRowSecurity |= fire_context.hasRowSecurity;
    }

    /*
     * Apply any row-level security policies.  We do this last because it
     * requires special recursion detection if the new quals have sublink
     * subqueries.
     */
    rt_index = 0;
    foreach!(lc, (*parsetree).rtable, {
        let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;
        let rel: Relation;
        let mut securityQuals: *mut List = NIL;
        let mut withCheckOptions: *mut List = NIL;
        let mut hasRowSecurity: bool = false;
        let mut hasSubLinks: bool = false;

        rt_index += 1;

        /* Only normal relations can have RLS policies */
        if (*rte).rtekind != RTE_RELATION
            || ((*rte).relkind != RELKIND_RELATION
                && (*rte).relkind != RELKIND_PARTITIONED_TABLE)
        {
            continue;
        }

        rel = table_open((*rte).relid, NoLock);

        /*
         * Fetch any new security quals that must be applied to this RTE.
         */
        get_row_security_policies(
            parsetree,
            rte,
            rt_index,
            &mut securityQuals,
            &mut withCheckOptions,
            &mut hasRowSecurity,
            &mut hasSubLinks,
        );

        if securityQuals != NIL || withCheckOptions != NIL {
            if hasSubLinks {
                let mut context =
                    acquireLocksOnSubLinks_context { for_execute: true };
                let mut fire_context = fireRIRonSubLink_context {
                    activeRIRs,
                    hasRowSecurity: false,
                };

                /*
                 * Recursively process the new quals, checking for infinite
                 * recursion.
                 */
                if list_member_oid(activeRIRs, RelationGetRelid(rel)) {
                    ereport!(ERROR, errmsg!("infinite recursion detected in policy for relation \"{}\"",
                        cstr(RelationGetRelationName(rel) as *const c_char))
                        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */);
                }

                activeRIRs = lappend_oid(activeRIRs, RelationGetRelid(rel));

                /*
                 * get_row_security_policies just passed back securityQuals
                 * and/or withCheckOptions with SubLinks; make sure we lock
                 * any relations which are referenced.
                 */
                let _ = acquireLocksOnSubLinks(
                    securityQuals as *mut Node,
                    &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
                );
                let _ = acquireLocksOnSubLinks(
                    withCheckOptions as *mut Node,
                    &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
                );

                /*
                 * Now that we have the locks on anything added by
                 * get_row_security_policies, fire any RIR rules for them.
                 */
                expression_tree_walker(
                    securityQuals as *mut Node,
                    fireRIRonSubLink,
                    &mut fire_context as *mut fireRIRonSubLink_context as *mut c_void,
                );

                expression_tree_walker(
                    withCheckOptions as *mut Node,
                    fireRIRonSubLink,
                    &mut fire_context as *mut fireRIRonSubLink_context as *mut c_void,
                );

                /*
                 * We can ignore the value of fire_context.hasRowSecurity
                 * since we only reach this code in cases where hasRowSecurity
                 * is already true.
                 */
                assert!(hasRowSecurity);

                activeRIRs = list_delete_last(activeRIRs);
            }

            /*
             * Add the new security barrier quals to the start of the RTE's
             * list so that they get applied before any existing barrier quals.
             */
            (*rte).securityQuals = list_concat(securityQuals, (*rte).securityQuals);

            (*parsetree).withCheckOptions =
                list_concat(withCheckOptions, (*parsetree).withCheckOptions);
        }

        /*
         * Make sure the query is marked correctly if row-level security
         * applies, or if the new quals had sublinks.
         */
        if hasRowSecurity {
            (*parsetree).hasRowSecurity = true;
        }
        if hasSubLinks {
            (*parsetree).hasSubLinks = true;
        }

        table_close(rel, NoLock);
    });

    parsetree
}


unsafe fn fireRIRrules(parsetree: *mut Query, activeRIRs: *mut List) -> *mut Query {
    fireRIRrules_impl(parsetree, activeRIRs)
}

/*
 * Modify the given query by adding 'AND rule_qual IS NOT TRUE' to its
 * qualification.  This is used to generate suitable "else clauses" for
 * conditional INSTEAD rules.
 */
unsafe fn CopyAndAddInvertedQual(
    parsetree: *mut Query,
    rule_qual: *mut Node,
    rt_index: c_int,
    event: CmdType,
) -> *mut Query {
    /* Don't scribble on the passed qual (it's in the relcache!) */
    let mut new_qual: *mut Node = copyObject(rule_qual);
    let mut context = acquireLocksOnSubLinks_context { for_execute: true };

    /*
     * In case there are subqueries in the qual, acquire necessary locks and
     * fix any deleted JOIN RTE entries.
     */
    let _ = acquireLocksOnSubLinks(
        new_qual,
        &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
    );

    /* Fix references to OLD */
    ChangeVarNodes(new_qual, PRS2_OLD_VARNO as c_int, rt_index, 0);
    /* Fix references to NEW */
    if event == CMD_INSERT || event == CMD_UPDATE {
        new_qual = ReplaceVarsFromTargetList(
            new_qual,
            PRS2_NEW_VARNO as c_int,
            0,
            rt_fetch(rt_index, (*parsetree).rtable),
            (*parsetree).targetList,
            (*parsetree).resultRelation,
            if event == CMD_UPDATE {
                REPLACEVARS_CHANGE_VARNO
            } else {
                REPLACEVARS_SUBSTITUTE_NULL
            },
            rt_index,
            &mut (*parsetree).hasSubLinks,
        );
    }
    /* And attach the fixed qual */
    AddInvertedQual(parsetree, new_qual);

    parsetree
}


/*
 * fireRules -
 *   Iterate through rule locks applying rules.
 *
 * Input arguments:
 *   parsetree - original query
 *   rt_index - RT index of result relation in original query
 *   event - type of rule event
 *   locks - list of rules to fire
 * Output arguments:
 *   *instead_flag - set true if any unqualified INSTEAD rule is found
 *   *returning_flag - set true if we rewrite RETURNING clause in any rule
 *   *qual_product - filled with modified original query if any qualified
 *                   INSTEAD rule is found
 * Return value:
 *   list of rule actions adjusted for use with this query
 */
unsafe fn fireRules(
    parsetree: *mut Query,
    rt_index: c_int,
    event: CmdType,
    locks: *mut List,
    instead_flag: *mut bool,
    returning_flag: *mut bool,
    qual_product: *mut *mut Query,
) -> *mut List {
    let mut results: *mut List = NIL;

    foreach!(l, locks, {
        let rule_lock: *mut RewriteRule = lfirst(current_cell!(l)) as *mut RewriteRule;
        let event_qual: *mut Node = (*rule_lock).qual;
        let actions: *mut List = (*rule_lock).actions;
        let qsrc: crate::nodes::parsenodes::QuerySource;

        /* Determine correct QuerySource value for actions */
        if (*rule_lock).isInstead {
            if !event_qual.is_null() {
                qsrc = QSRC_QUAL_INSTEAD_RULE;
            } else {
                qsrc = QSRC_INSTEAD_RULE;
                *instead_flag = true; /* report unqualified INSTEAD */
            }
        } else {
            qsrc = QSRC_NON_INSTEAD_RULE;
        }

        if qsrc == QSRC_QUAL_INSTEAD_RULE {
            /*
             * If there are INSTEAD rules with qualifications, the original
             * query is still performed.  We save this in *qual_product so
             * RewriteQuery() can add it to the query list after we mangled it
             * up enough.
             *
             * If we have already found an unqualified INSTEAD rule, then
             * *qual_product won't be used, so don't bother building it.
             */
            if !*instead_flag {
                if (*qual_product).is_null() {
                    *qual_product = copyObject(parsetree);
                }
                *qual_product = CopyAndAddInvertedQual(*qual_product, event_qual, rt_index, event);
            }
        }

        /* Now process the rule's actions and add them to the result list */
        foreach!(r, actions, {
            let mut rule_action: *mut Query = lfirst(current_cell!(r)) as *mut Query;

            if (*rule_action).commandType == CMD_NOTHING {
                continue;
            }

            rule_action = rewriteRuleAction(
                parsetree,
                rule_action,
                event_qual,
                rt_index,
                event,
                returning_flag,
            );

            (*rule_action).querySource = qsrc;
            (*rule_action).canSetTag = false; /* might change later */

            results = lappend(results, rule_action as *mut c_void);
        });
    });

    results
}


/*
 * get_view_query - get the Query from a view's _RETURN rule.
 *
 * Caller should have verified that the relation is a view, and therefore
 * we should find an ON SELECT action.
 */
pub unsafe fn get_view_query(view: Relation) -> *mut Query {
    get_view_query_impl(view)
}

unsafe fn get_view_query_impl(view: Relation) -> *mut Query {
    assert!((*(*view).rd_rel).relkind == RELKIND_VIEW);

    let mut i: c_int = 0;
    while i < (*((*view).rd_rules as *mut RuleLock)).numLocks {
        let rule: *mut RewriteRule =
            *(*((*view).rd_rules as *mut RuleLock)).rules.add(i as usize);

        if (*rule).event == CMD_SELECT {
            /* A _RETURN rule should have only one action */
            if list_length((*rule).actions) != 1 {
                elog!(ERROR, "invalid _RETURN rule action specification");
            }

            return linitial((*rule).actions) as *mut Query;
        }
        i += 1;
    }

    elog!(ERROR, "failed to find _RETURN rule for view");
    core::ptr::null_mut() /* keep compiler quiet */
}


/*
 * view_has_instead_trigger - does view have an INSTEAD OF trigger for event?
 *
 * For MERGE, this will return true if there is an INSTEAD OF trigger for
 * every action in mergeActionList, and false if there are any actions that
 * lack an INSTEAD OF trigger.
 */
pub unsafe fn view_has_instead_trigger(
    view: Relation,
    event: CmdType,
    mergeActionList: *mut List,
) -> bool {
    view_has_instead_trigger_impl(view, event, mergeActionList)
}

unsafe fn view_has_instead_trigger_impl(
    view: Relation,
    event: CmdType,
    mergeActionList: *mut List,
) -> bool {
    let trigDesc: *mut crate::utils::reltrigger::TriggerDesc = (*view).trigdesc as *mut crate::utils::reltrigger::TriggerDesc;

    match event {
        CMD_INSERT => {
            if !trigDesc.is_null() && (*trigDesc).trig_insert_instead_row {
                return true;
            }
        }
        CMD_UPDATE => {
            if !trigDesc.is_null() && (*trigDesc).trig_update_instead_row {
                return true;
            }
        }
        CMD_DELETE => {
            if !trigDesc.is_null() && (*trigDesc).trig_delete_instead_row {
                return true;
            }
        }
        CMD_MERGE => {
            foreach!(lc, mergeActionList, {
                let action: *mut crate::nodes::primnodes::MergeAction =
                    lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;

                match (*action).commandType {
                    CMD_INSERT => {
                        if trigDesc.is_null() || !(*trigDesc).trig_insert_instead_row {
                            return false;
                        }
                    }
                    CMD_UPDATE => {
                        if trigDesc.is_null() || !(*trigDesc).trig_update_instead_row {
                            return false;
                        }
                    }
                    CMD_DELETE => {
                        if trigDesc.is_null() || !(*trigDesc).trig_delete_instead_row {
                            return false;
                        }
                    }
                    CMD_NOTHING => {
                        /* No trigger required */
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized commandType: {}",
                            (*action).commandType as c_int
                        );
                    }
                }
            });
            return true; /* no actions without an INSTEAD OF trigger */
        }
        _ => {
            elog!(ERROR, "unrecognized CmdType: {}", event as c_int);
        }
    }
    false
}

// ===========================================================================
// PART 5: view_col_is_auto_updatable, view_query_is_auto_updatable,
//         view_cols_are_auto_updatable, relation_is_updatable,
//         adjust_view_column_set, error_view_not_updatable, rewriteTargetView
// ===========================================================================

/*
 * view_col_is_auto_updatable - test whether the specified column of a view
 * is auto-updatable. Returns NULL (if the column can be updated) or a message
 * string giving the reason that it cannot be.
 *
 * The returned string has not been translated; if it is shown as an error
 * message, the caller should apply _() to translate it.
 */
unsafe fn view_col_is_auto_updatable(
    rtr: *mut RangeTblRef,
    tle: *mut TargetEntry,
) -> *const c_char {
    view_col_is_auto_updatable_impl(rtr, tle)
}

unsafe fn view_col_is_auto_updatable_impl(
    rtr: *mut RangeTblRef,
    tle: *mut TargetEntry,
) -> *const c_char {
    let var: *mut crate::nodes::primnodes::Var =
        (*tle).expr as *mut crate::nodes::primnodes::Var;

    /*
     * For now, the only updatable columns we support are those that are Vars
     * referring to user columns of the underlying base relation.
     */
    if (*tle).resjunk {
        return gettext_noop!("Junk view columns are not updatable.");
    }

    if !IsA!(var as *mut Node, T_Var)
        || (*var).varno != (*rtr).rtindex
        || (*var).varlevelsup != 0
    {
        return gettext_noop!(
            "View columns that are not columns of their base relation are not updatable."
        );
    }

    if (*var).varattno < 0 {
        return gettext_noop!(
            "View columns that refer to system columns are not updatable."
        );
    }

    if (*var).varattno == 0 {
        return gettext_noop!(
            "View columns that return whole-row references are not updatable."
        );
    }

    core::ptr::null() /* the view column is updatable */
}


/*
 * view_query_is_auto_updatable - test whether the specified view definition
 * represents an auto-updatable view. Returns NULL (if the view can be updated)
 * or a message string giving the reason that it cannot be.
 *
 * The returned string has not been translated.
 *
 * If check_cols is true, the view is required to have at least one updatable
 * column (necessary for INSERT/UPDATE).
 */
pub unsafe fn view_query_is_auto_updatable(
    viewquery: *mut Query,
    check_cols: bool,
) -> *const c_char {
    view_query_is_auto_updatable_impl(viewquery, check_cols)
}

unsafe fn view_query_is_auto_updatable_impl(
    viewquery: *mut Query,
    check_cols: bool,
) -> *const c_char {
    let rtr: *mut RangeTblRef;
    let base_rte: *mut RangeTblEntry;

    /*----------
     * Check if the view is simply updatable.  According to SQL-92 this means:
     *   - No DISTINCT clause.
     *   - Each TLE is a column reference, and each column appears at most once.
     *   - FROM contains exactly one base relation.
     *   - No GROUP BY or HAVING clauses.
     *   - No set operations (UNION, INTERSECT or EXCEPT).
     *   - No sub-queries in the WHERE clause that reference the target table.
     *----------
     */
    if (*viewquery).distinctClause != NIL {
        return gettext_noop!(
            "Views containing DISTINCT are not automatically updatable."
        );
    }

    if (*viewquery).groupClause != NIL || !(*viewquery).groupingSets.is_null() {
        return gettext_noop!(
            "Views containing GROUP BY are not automatically updatable."
        );
    }

    if !(*viewquery).havingQual.is_null() {
        return gettext_noop!(
            "Views containing HAVING are not automatically updatable."
        );
    }

    if !(*viewquery).setOperations.is_null() {
        return gettext_noop!(
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable."
        );
    }

    if (*viewquery).cteList != NIL {
        return gettext_noop!(
            "Views containing WITH are not automatically updatable."
        );
    }

    if !(*viewquery).limitOffset.is_null() || !(*viewquery).limitCount.is_null() {
        return gettext_noop!(
            "Views containing LIMIT or OFFSET are not automatically updatable."
        );
    }

    /*
     * We must not allow window functions or set returning functions in the
     * targetlist. Otherwise we might end up inserting them into the quals of
     * the main query.
     */
    if (*viewquery).hasAggs {
        return gettext_noop!(
            "Views that return aggregate functions are not automatically updatable."
        );
    }

    if (*viewquery).hasWindowFuncs {
        return gettext_noop!(
            "Views that return window functions are not automatically updatable."
        );
    }

    if (*viewquery).hasTargetSRFs {
        return gettext_noop!(
            "Views that return set-returning functions are not automatically updatable."
        );
    }

    /*
     * The view query should select from a single base relation, which must be
     * a table or another view.
     */
    if list_length((*(*viewquery).jointree).fromlist) != 1 {
        return gettext_noop!(
            "Views that do not select from a single table or view are not automatically updatable."
        );
    }

    rtr = linitial((*(*viewquery).jointree).fromlist) as *mut RangeTblRef;
    if !IsA!(rtr as *mut Node, T_RangeTblRef) {
        return gettext_noop!(
            "Views that do not select from a single table or view are not automatically updatable."
        );
    }

    base_rte = rt_fetch((*rtr).rtindex, (*viewquery).rtable);
    if (*base_rte).rtekind != RTE_RELATION
        || ((*base_rte).relkind != RELKIND_RELATION
            && (*base_rte).relkind != RELKIND_FOREIGN_TABLE
            && (*base_rte).relkind != RELKIND_VIEW
            && (*base_rte).relkind != RELKIND_PARTITIONED_TABLE)
    {
        return gettext_noop!(
            "Views that do not select from a single table or view are not automatically updatable."
        );
    }

    if !(*base_rte).tablesample.is_null() {
        return gettext_noop!(
            "Views containing TABLESAMPLE are not automatically updatable."
        );
    }

    /*
     * Check that the view has at least one updatable column. This is required
     * for INSERT/UPDATE but not for DELETE.
     */
    if check_cols {
        let mut found: bool = false;

        foreach!(cell, (*viewquery).targetList, {
            let tle: *mut TargetEntry = lfirst(current_cell!(cell)) as *mut TargetEntry;

            if view_col_is_auto_updatable_impl(rtr, tle).is_null() {
                found = true;
                break;
            }
        });

        if !found {
            return gettext_noop!(
                "Views that have no updatable columns are not automatically updatable."
            );
        }
    }

    core::ptr::null() /* the view is updatable */
}


/*
 * view_cols_are_auto_updatable - test whether all of the required columns of
 * an auto-updatable view are actually updatable. Returns NULL (if all the
 * required columns can be updated) or a message string giving the reason that
 * they cannot be.
 *
 * The returned string has not been translated.
 */
unsafe fn view_cols_are_auto_updatable(
    viewquery: *mut Query,
    required_cols: *mut Bitmapset,
    updatable_cols: *mut *mut Bitmapset,
    non_updatable_col: *mut *mut c_char,
) -> *const c_char {
    view_cols_are_auto_updatable_impl(viewquery, required_cols, updatable_cols, non_updatable_col)
}

unsafe fn view_cols_are_auto_updatable_impl(
    viewquery: *mut Query,
    required_cols: *mut Bitmapset,
    updatable_cols: *mut *mut Bitmapset,
    non_updatable_col: *mut *mut c_char,
) -> *const c_char {
    let rtr: *mut RangeTblRef;
    let mut col: AttrNumber;

    /*
     * The caller should have verified that this view is auto-updatable and so
     * there should be a single base relation.
     */
    assert!(list_length((*(*viewquery).jointree).fromlist) == 1);
    rtr = linitial((*(*viewquery).jointree).fromlist) as *mut RangeTblRef;

    /* Initialize the optional return values */
    if !updatable_cols.is_null() {
        *updatable_cols = core::ptr::null_mut();
    }
    if !non_updatable_col.is_null() {
        *non_updatable_col = core::ptr::null_mut();
    }

    /* Test each view column for updatability */
    col = (-FirstLowInvalidHeapAttributeNumber) as AttrNumber;
    foreach!(cell, (*viewquery).targetList, {
        let tle: *mut TargetEntry = lfirst(current_cell!(cell)) as *mut TargetEntry;
        let col_update_detail: *const c_char;

        col += 1;
        col_update_detail = view_col_is_auto_updatable_impl(rtr, tle);

        if col_update_detail.is_null() {
            /* The column is updatable */
            if !updatable_cols.is_null() {
                *updatable_cols = bms_add_member(*updatable_cols, col as c_int);
            }
        } else if bms_is_member(col as c_int, required_cols) {
            /* The required column is not updatable */
            if !non_updatable_col.is_null() {
                *non_updatable_col = (*tle).resname;
            }
            return col_update_detail;
        }
    });

    core::ptr::null() /* all the required view columns are updatable */
}


/*
 * relation_is_updatable - determine which update events the specified
 * relation supports.
 *
 * Returns a bitmask of rule event numbers indicating which of the INSERT,
 * UPDATE and DELETE operations are supported.
 */
pub unsafe fn relation_is_updatable(
    reloid: Oid,
    outer_reloids: *mut List,
    include_triggers: bool,
    include_cols: *mut Bitmapset,
) -> c_int {
    let mut events: c_int = 0;
    let rel: Relation;
    let rulelocks: *mut RuleLock;

    /* Since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    rel = try_relation_open(reloid, AccessShareLock);

    /*
     * If the relation doesn't exist, return zero rather than throwing an
     * error.  This is helpful since scanning an information_schema view under
     * MVCC rules can result in referencing rels that have actually been
     * deleted already.
     */
    if rel.is_null() {
        return 0;
    }

    /* If we detect a recursive view, report that it is not updatable */
    if list_member_oid(outer_reloids, RelationGetRelid(rel)) {
        relation_close(rel, AccessShareLock);
        return 0;
    }

    /* If the relation is a table, it is always updatable */
    if (*(*rel).rd_rel).relkind == RELKIND_RELATION
        || (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
    {
        relation_close(rel, AccessShareLock);
        return ALL_EVENTS;
    }

    /* Look for unconditional DO INSTEAD rules, and note supported events */
    rulelocks = (*rel).rd_rules as *mut RuleLock;
    if !rulelocks.is_null() {
        let mut i: c_int = 0;
        while i < (*rulelocks).numLocks {
            if (*(*(*rulelocks).rules.add(i as usize))).isInstead
                && (*(*(*rulelocks).rules.add(i as usize))).qual.is_null()
            {
                events |= (1 << (*(*(*rulelocks).rules.add(i as usize))).event as c_int)
                    & ALL_EVENTS;
            }
            i += 1;
        }

        /* If we have rules for all events, we're done */
        if events == ALL_EVENTS {
            relation_close(rel, AccessShareLock);
            return events;
        }
    }

    /* Similarly look for INSTEAD OF triggers, if they are to be included */
    if include_triggers {
        let trigDesc: *mut crate::utils::reltrigger::TriggerDesc = (*rel).trigdesc as *mut crate::utils::reltrigger::TriggerDesc;

        if !trigDesc.is_null() {
            if (*trigDesc).trig_insert_instead_row {
                events |= 1 << CMD_INSERT as c_int;
            }
            if (*trigDesc).trig_update_instead_row {
                events |= 1 << CMD_UPDATE as c_int;
            }
            if (*trigDesc).trig_delete_instead_row {
                events |= 1 << CMD_DELETE as c_int;
            }

            /* If we have triggers for all events, we're done */
            if events == ALL_EVENTS {
                relation_close(rel, AccessShareLock);
                return events;
            }
        }
    }

    /* If this is a foreign table, check which update events it supports */
    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
        let fdwroutine: *mut crate::foreign::fdwapi::FdwRoutine =
            GetFdwRoutineForRelation(rel, false);

        if !(*fdwroutine).IsForeignRelUpdatable.is_none() {
            events |= ((*fdwroutine).IsForeignRelUpdatable.unwrap())(rel as *mut c_void);
        } else {
            /* Assume presence of executor functions is sufficient */
            if (*fdwroutine).ExecForeignInsert.is_some() {
                events |= 1 << CMD_INSERT as c_int;
            }
            if (*fdwroutine).ExecForeignUpdate.is_some() {
                events |= 1 << CMD_UPDATE as c_int;
            }
            if (*fdwroutine).ExecForeignDelete.is_some() {
                events |= 1 << CMD_DELETE as c_int;
            }
        }

        relation_close(rel, AccessShareLock);
        return events;
    }

    /* Check if this is an automatically updatable view */
    if (*(*rel).rd_rel).relkind == RELKIND_VIEW {
        let viewquery: *mut Query = get_view_query_impl(rel);

        if view_query_is_auto_updatable_impl(viewquery, false).is_null() {
            let mut updatable_cols: *mut Bitmapset = core::ptr::null_mut();
            let mut auto_events: c_int;
            let rtr: *mut RangeTblRef;
            let base_rte: *mut RangeTblEntry;
            let baseoid: Oid;

            /*
             * Determine which of the view's columns are updatable.
             */
            view_cols_are_auto_updatable_impl(
                viewquery,
                core::ptr::null_mut(),
                &mut updatable_cols,
                core::ptr::null_mut(),
            );

            if !include_cols.is_null() {
                updatable_cols = bms_int_members(updatable_cols, include_cols);
            }

            if bms_is_empty(updatable_cols) {
                auto_events = 1 << CMD_DELETE as c_int; /* May support DELETE */
            } else {
                auto_events = ALL_EVENTS; /* May support all events */
            }

            /*
             * The base relation must also support these update commands.
             */
            rtr = linitial((*(*viewquery).jointree).fromlist) as *mut RangeTblRef;
            base_rte = rt_fetch((*rtr).rtindex, (*viewquery).rtable);
            assert!((*base_rte).rtekind == RTE_RELATION);

            if (*base_rte).relkind != RELKIND_RELATION
                && (*base_rte).relkind != RELKIND_PARTITIONED_TABLE
            {
                baseoid = (*base_rte).relid;
                let outer_reloids = lappend_oid(outer_reloids, RelationGetRelid(rel));
                let include_cols = adjust_view_column_set_impl(
                    updatable_cols,
                    (*viewquery).targetList,
                );
                auto_events &= relation_is_updatable(
                    baseoid,
                    outer_reloids,
                    include_triggers,
                    include_cols,
                );
                let _ = list_delete_last(outer_reloids);
            }
            events |= auto_events;
        }
    }

    /* If we reach here, the relation may support some update commands */
    relation_close(rel, AccessShareLock);
    events
}


/*
 * adjust_view_column_set - map a set of column numbers according to targetlist
 *
 * This is used with simply-updatable views to map column-permissions sets for
 * the view columns onto the matching columns in the underlying base relation.
 */
unsafe fn adjust_view_column_set(
    cols: *mut Bitmapset,
    targetlist: *mut List,
) -> *mut Bitmapset {
    adjust_view_column_set_impl(cols, targetlist)
}

unsafe fn adjust_view_column_set_impl(
    cols: *mut Bitmapset,
    targetlist: *mut List,
) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = core::ptr::null_mut();
    let mut col: c_int = -1;

    loop {
        col = bms_next_member(cols, col);
        if col < 0 {
            break;
        }
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        let attno: AttrNumber = (col + FirstLowInvalidHeapAttributeNumber as c_int) as AttrNumber;

        if attno == InvalidAttrNumber {
            /*
             * There's a whole-row reference to the view.  For permissions
             * purposes, treat it as a reference to each column available from
             * the view.
             */
            foreach!(lc, targetlist, {
                let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
                let var: *mut crate::nodes::primnodes::Var;

                if (*tle).resjunk {
                    continue;
                }
                var = castNode!(crate::nodes::primnodes::Var, T_Var, (*tle).expr as *mut c_void);
                result = bms_add_member(
                    result,
                    (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                );
            });
        } else {
            /*
             * Views do not have system columns, so we do not expect to see
             * any other system attnos here.
             */
            let tle: *mut TargetEntry = get_tle_by_resno(targetlist, attno);

            if !tle.is_null()
                && !(*tle).resjunk
                && IsA!((*tle).expr as *mut Node, T_Var)
            {
                let var: *mut crate::nodes::primnodes::Var =
                    (*tle).expr as *mut crate::nodes::primnodes::Var;

                result = bms_add_member(
                    result,
                    (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                );
            } else {
                elog!(ERROR, "attribute number {} not found in view targetlist", attno);
            }
        }
    }

    result
}


/*
 * error_view_not_updatable -
 *   Report an error due to an attempt to update a non-updatable view.
 */
pub unsafe fn error_view_not_updatable(
    view: Relation,
    command: CmdType,
    mergeActionList: *mut List,
    detail: *const c_char,
) {
    error_view_not_updatable_impl(view, command, mergeActionList, detail)
}

unsafe fn error_view_not_updatable_impl(
    view: Relation,
    command: CmdType,
    mergeActionList: *mut List,
    detail: *const c_char,
) {
    let trigDesc: *mut crate::utils::reltrigger::TriggerDesc = (*view).trigdesc as *mut crate::utils::reltrigger::TriggerDesc;

    match command {
        CMD_INSERT => {
            ereport!(ERROR, errmsg!("cannot insert into view \"{}\"",
                cstr(RelationGetRelationName(view) as *const c_char))
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   detail ? errdetail_internal("%s", _(detail)) : 0,
                   errhint("To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.") */);
        }
        CMD_UPDATE => {
            ereport!(ERROR, errmsg!("cannot update view \"{}\"",
                cstr(RelationGetRelationName(view) as *const c_char))
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   detail hint */);
        }
        CMD_DELETE => {
            ereport!(ERROR, errmsg!("cannot delete from view \"{}\"",
                cstr(RelationGetRelationName(view) as *const c_char))
                /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   detail hint */);
        }
        CMD_MERGE => {
            /*
             * Note that the error hints here differ from above, since MERGE
             * doesn't support rules.
             */
            foreach!(lc, mergeActionList, {
                let action: *mut crate::nodes::primnodes::MergeAction =
                    lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;

                match (*action).commandType {
                    CMD_INSERT => {
                        if trigDesc.is_null() || !(*trigDesc).trig_insert_instead_row {
                            ereport!(ERROR, errmsg!("cannot insert into view \"{}\"",
                                cstr(RelationGetRelationName(view) as *const c_char))
                                /* C also: errcode, detail, errhint INSTEAD OF INSERT trigger */);
                        }
                    }
                    CMD_UPDATE => {
                        if trigDesc.is_null() || !(*trigDesc).trig_update_instead_row {
                            ereport!(ERROR, errmsg!("cannot update view \"{}\"",
                                cstr(RelationGetRelationName(view) as *const c_char))
                                /* C also: errcode, detail, errhint INSTEAD OF UPDATE trigger */);
                        }
                    }
                    CMD_DELETE => {
                        if trigDesc.is_null() || !(*trigDesc).trig_delete_instead_row {
                            ereport!(ERROR, errmsg!("cannot delete from view \"{}\"",
                                cstr(RelationGetRelationName(view) as *const c_char))
                                /* C also: errcode, detail, errhint INSTEAD OF DELETE trigger */);
                        }
                    }
                    CMD_NOTHING => {}
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized commandType: {}",
                            (*action).commandType as c_int
                        );
                    }
                }
            });
        }
        _ => {
            elog!(ERROR, "unrecognized CmdType: {}", command as c_int);
        }
    }
}


/*
 * rewriteTargetView -
 *   Attempt to rewrite a query where the target relation is a view, so that
 *   the view's base relation becomes the target relation.
 */
unsafe fn rewriteTargetView(parsetree: *mut Query, view: Relation) -> *mut Query {
    let viewquery: *mut Query;
    let insert_or_update: bool;
    let auto_update_detail: *const c_char;
    let rtr: *mut RangeTblRef;
    let base_rt_index: c_int;
    let new_rt_index: c_int;
    let base_rte: *mut RangeTblEntry;
    let view_rte: *mut RangeTblEntry;
    let mut new_rte: *mut RangeTblEntry;
    let base_perminfo: *mut RTEPermissionInfo;
    let view_perminfo: *mut RTEPermissionInfo;
    let new_perminfo: *mut RTEPermissionInfo;
    let base_rel: Relation;
    let view_targetlist: *mut List;

    /*
     * Get the Query from the view's ON SELECT rule.  We're going to munge the
     * Query to change the view's base relation into the target relation.
     */
    viewquery = copyObject(get_view_query_impl(view));

    /* Locate RTE and perminfo describing the view in the outer query */
    view_rte = rt_fetch((*parsetree).resultRelation, (*parsetree).rtable);
    view_perminfo = getRTEPermissionInfo((*parsetree).rteperminfos, view_rte);

    /*
     * Are we doing INSERT/UPDATE, or MERGE containing INSERT/UPDATE?
     */
    let mut insert_or_update =
        (*parsetree).commandType == CMD_INSERT
            || (*parsetree).commandType == CMD_UPDATE;

    if (*parsetree).commandType == CMD_MERGE {
        foreach!(lc, (*parsetree).mergeActionList, {
            let action: *mut crate::nodes::primnodes::MergeAction =
                lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;
            if (*action).commandType == CMD_INSERT || (*action).commandType == CMD_UPDATE {
                insert_or_update = true;
                break;
            }
        });
    }

    /* Check if the expansion of non-system views are restricted */
    if (restrict_nonsystem_relation_kind & RESTRICT_RELKIND_VIEW) != 0
        && RelationGetRelid(view) >= FirstNormalObjectId
    {
        ereport!(ERROR, errmsg!("access to non-system view \"{}\" is restricted",
            cstr(RelationGetRelationName(view) as *const c_char))
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
    }

    /*
     * The view must be updatable, else fail.
     */
    auto_update_detail =
        view_query_is_auto_updatable_impl(viewquery, insert_or_update);

    if !auto_update_detail.is_null() {
        error_view_not_updatable_impl(
            view,
            (*parsetree).commandType,
            (*parsetree).mergeActionList,
            auto_update_detail,
        );
    }

    /*
     * For INSERT/UPDATE (or MERGE containing INSERT/UPDATE) the modified
     * columns must all be updatable.
     */
    if insert_or_update {
        let mut modified_cols: *mut Bitmapset = bms_union(
            (*view_perminfo).insertedCols,
            (*view_perminfo).updatedCols,
        );
        let mut non_updatable_col: *mut c_char = core::ptr::null_mut();

        foreach!(lc, (*parsetree).targetList, {
            let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

            if !(*tle).resjunk {
                modified_cols = bms_add_member(
                    modified_cols,
                    (*tle).resno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                );
            }
        });

        if !(*parsetree).onConflict.is_null() {
            foreach!(lc, (*(*parsetree).onConflict).onConflictSet, {
                let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

                if !(*tle).resjunk {
                    modified_cols = bms_add_member(
                        modified_cols,
                        (*tle).resno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                    );
                }
            });
        }

        foreach!(lc, (*parsetree).mergeActionList, {
            let action: *mut crate::nodes::primnodes::MergeAction =
                lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;

            if (*action).commandType == CMD_INSERT || (*action).commandType == CMD_UPDATE {
                foreach!(lc2, (*action).targetList, {
                    let tle: *mut TargetEntry =
                        lfirst(current_cell!(lc2)) as *mut TargetEntry;

                    if !(*tle).resjunk {
                        modified_cols = bms_add_member(
                            modified_cols,
                            (*tle).resno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                        );
                    }
                });
            }
        });

        let auto_update_detail = view_cols_are_auto_updatable_impl(
            viewquery,
            modified_cols,
            core::ptr::null_mut(),
            &mut non_updatable_col,
        );
        if !auto_update_detail.is_null() {
            /*
             * This is a different error, caused by an attempt to update a
             * non-updatable column in an otherwise updatable view.
             */
            match (*parsetree).commandType {
                CMD_INSERT => {
                    ereport!(ERROR, errmsg!("cannot insert into column \"{}\" of view \"{}\"",
                        cstr(non_updatable_col),
                        cstr(RelationGetRelationName(view) as *const c_char))
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail_internal("%s", _(auto_update_detail)) */);
                }
                CMD_UPDATE => {
                    ereport!(ERROR, errmsg!("cannot update column \"{}\" of view \"{}\"",
                        cstr(non_updatable_col),
                        cstr(RelationGetRelationName(view) as *const c_char))
                        /* C also: errcode, errdetail */);
                }
                CMD_MERGE => {
                    ereport!(ERROR, errmsg!("cannot merge into column \"{}\" of view \"{}\"",
                        cstr(non_updatable_col),
                        cstr(RelationGetRelationName(view) as *const c_char))
                        /* C also: errcode, errdetail */);
                }
                _ => {
                    elog!(
                        ERROR,
                        "unrecognized CmdType: {}",
                        (*parsetree).commandType as c_int
                    );
                }
            }
        }
    }

    /*
     * For MERGE, there must not be any INSTEAD OF triggers on an otherwise
     * updatable view.
     */
    if (*parsetree).commandType == CMD_MERGE {
        foreach!(lc, (*parsetree).mergeActionList, {
            let action: *mut crate::nodes::primnodes::MergeAction =
                lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;

            if (*action).commandType != CMD_NOTHING
                && view_has_instead_trigger_impl(view, (*action).commandType, NIL)
            {
                ereport!(ERROR, errmsg!("cannot merge into view \"{}\"",
                    cstr(RelationGetRelationName(view) as *const c_char))
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errdetail("MERGE is not supported for views with INSTEAD OF triggers for some actions but not all."),
                       errhint(...) */);
            }
        });
    }

    /*
     * If we get here, view_query_is_auto_updatable() has verified that the
     * view contains a single base relation.
     */
    assert!(list_length((*(*viewquery).jointree).fromlist) == 1);
    rtr = linitial((*(*viewquery).jointree).fromlist) as *mut RangeTblRef;

    base_rt_index = (*rtr).rtindex;
    base_rte = rt_fetch(base_rt_index, (*viewquery).rtable);
    assert!((*base_rte).rtekind == RTE_RELATION);
    base_perminfo = getRTEPermissionInfo((*viewquery).rteperminfos, base_rte);

    /*
     * Up to now, the base relation hasn't been touched at all in our query.
     * We need to acquire lock on it before we try to do anything with it.
     */
    base_rel = table_open((*base_rte).relid, RowExclusiveLock);

    /*
     * While we have the relation open, update the RTE's relkind.
     */
    (*base_rte).relkind = (*(*base_rel).rd_rel).relkind;

    /*
     * If the view query contains any sublink subqueries then we need to also
     * acquire locks on any relations they refer to.
     */
    if (*viewquery).hasSubLinks {
        let mut context = acquireLocksOnSubLinks_context { for_execute: true };
        query_tree_walker(
            viewquery,
            acquireLocksOnSubLinks,
            &mut context as *mut acquireLocksOnSubLinks_context as *mut c_void,
            QTW_IGNORE_RC_SUBQUERIES,
        );
    }

    /*
     * Create a new target RTE describing the base relation, and add it to the
     * outer query's rangetable.
     *
     * Be sure to set rellockmode to the correct thing for the target table.
     * Since we copied the whole viewquery above, we can just scribble on
     * base_rte instead of copying it.
     */
    new_rte = base_rte;
    (*new_rte).rellockmode = RowExclusiveLock;

    (*parsetree).rtable = lappend((*parsetree).rtable, new_rte as *mut c_void);
    let new_rt_index = list_length((*parsetree).rtable);

    /*
     * INSERTs never inherit.
     */
    if (*parsetree).commandType == CMD_INSERT {
        (*new_rte).inh = false;
    }

    /*
     * Adjust the view's targetlist Vars to reference the new target RTE.
     */
    view_targetlist = (*viewquery).targetList;

    ChangeVarNodes(
        view_targetlist as *mut Node,
        base_rt_index,
        new_rt_index,
        0,
    );

    /*
     * If the view has "security_invoker" set, mark the new target relation
     * for the permissions checks that we want to enforce against the query
     * caller. Otherwise we want to enforce them against the view owner.
     */
    (*new_rte).perminfoindex = 0;
    new_perminfo = addRTEPermissionInfo(&mut (*parsetree).rteperminfos, new_rte);
    if RelationHasSecurityInvoker(view) {
        (*new_perminfo).checkAsUser = InvalidOid;
    } else {
        (*new_perminfo).checkAsUser = (*(*view).rd_rel).relowner;
    }
    (*new_perminfo).requiredPerms = (*view_perminfo).requiredPerms;

    /*
     * Now for the per-column permissions bits.
     */
    assert!(bms_is_empty((*new_perminfo).insertedCols) && bms_is_empty((*new_perminfo).updatedCols));

    (*new_perminfo).selectedCols = (*base_perminfo).selectedCols;

    (*new_perminfo).insertedCols =
        adjust_view_column_set_impl((*view_perminfo).insertedCols, view_targetlist);

    (*new_perminfo).updatedCols =
        adjust_view_column_set_impl((*view_perminfo).updatedCols, view_targetlist);

    /*
     * Move any security barrier quals from the view RTE onto the new target
     * RTE.
     */
    (*new_rte).securityQuals = (*view_rte).securityQuals;
    (*view_rte).securityQuals = NIL;

    /*
     * Now update all Vars in the outer query that reference the view to
     * reference the appropriate column of the base relation instead.
     */
    let parsetree = ReplaceVarsFromTargetList(
        parsetree as *mut Node,
        (*parsetree).resultRelation,
        0,
        view_rte,
        view_targetlist,
        new_rt_index,
        REPLACEVARS_REPORT_ERROR,
        0,
        core::ptr::null_mut(),
    ) as *mut Query;

    /*
     * Update all other RTI references in the query that point to the view
     * to point to the new base relation instead.
     */
    ChangeVarNodes(
        parsetree as *mut Node,
        (*parsetree).resultRelation,
        new_rt_index,
        0,
    );
    assert!((*parsetree).resultRelation == new_rt_index);

    /*
     * For INSERT/UPDATE we must also update resnos in the targetlist to refer
     * to columns of the base relation.  Similarly for MERGE INSERT/UPDATE
     * actions.
     */
    if (*parsetree).commandType != CMD_DELETE {
        foreach!(lc, (*parsetree).targetList, {
            let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
            let view_tle: *mut TargetEntry;

            if (*tle).resjunk {
                continue;
            }

            view_tle = get_tle_by_resno(view_targetlist, (*tle).resno);
            if !view_tle.is_null()
                && !(*view_tle).resjunk
                && IsA!((*view_tle).expr as *mut Node, T_Var)
            {
                (*tle).resno =
                    (*((*view_tle).expr as *mut crate::nodes::primnodes::Var)).varattno;
            } else {
                elog!(ERROR, "attribute number {} not found in view targetlist", (*tle).resno);
            }
        });

        foreach!(lc, (*parsetree).mergeActionList, {
            let action: *mut crate::nodes::primnodes::MergeAction =
                lfirst(current_cell!(lc)) as *mut crate::nodes::primnodes::MergeAction;

            if (*action).commandType == CMD_INSERT || (*action).commandType == CMD_UPDATE {
                foreach!(lc2, (*action).targetList, {
                    let tle: *mut TargetEntry =
                        lfirst(current_cell!(lc2)) as *mut TargetEntry;
                    let view_tle: *mut TargetEntry;

                    if (*tle).resjunk {
                        continue;
                    }

                    view_tle = get_tle_by_resno(view_targetlist, (*tle).resno);
                    if !view_tle.is_null()
                        && !(*view_tle).resjunk
                        && IsA!((*view_tle).expr as *mut Node, T_Var)
                    {
                        (*tle).resno = (*((*view_tle).expr
                            as *mut crate::nodes::primnodes::Var))
                            .varattno;
                    } else {
                        elog!(
                            ERROR,
                            "attribute number {} not found in view targetlist",
                            (*tle).resno
                        );
                    }
                });
            }
        });
    }

    /*
     * For INSERT .. ON CONFLICT .. DO UPDATE, we must also update assorted
     * stuff in the onConflict data structure.
     */
    if !(*parsetree).onConflict.is_null()
        && (*(*parsetree).onConflict).action == ONCONFLICT_UPDATE
    {
        let old_exclRelIndex: c_int;
        let new_exclRelIndex: c_int;
        let new_exclNSItem: *mut crate::parser::parse_node::ParseNamespaceItem;
        let new_exclRte: *mut RangeTblEntry;
        let tmp_tlist: *mut List;

        /*
         * Like the INSERT/UPDATE code above, update the resnos in the
         * auxiliary UPDATE targetlist to refer to columns of the base relation.
         */
        foreach!(lc, (*(*parsetree).onConflict).onConflictSet, {
            let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
            let view_tle: *mut TargetEntry;

            if (*tle).resjunk {
                continue;
            }

            view_tle = get_tle_by_resno(view_targetlist, (*tle).resno);
            if !view_tle.is_null()
                && !(*view_tle).resjunk
                && IsA!((*view_tle).expr as *mut Node, T_Var)
            {
                (*tle).resno =
                    (*((*view_tle).expr as *mut crate::nodes::primnodes::Var)).varattno;
            } else {
                elog!(ERROR, "attribute number {} not found in view targetlist", (*tle).resno);
            }
        });

        /*
         * Also, create a new RTE for the EXCLUDED pseudo-relation.
         */
        old_exclRelIndex = (*(*parsetree).onConflict).exclRelIndex;

        new_exclNSItem = addRangeTableEntryForRelation(
            make_parsestate(core::ptr::null_mut()),
            base_rel,
            RowExclusiveLock,
            makeAlias(c"excluded".as_ptr(), NIL),
            false,
            false,
        );
        new_exclRte = (*new_exclNSItem).p_rte as *mut RangeTblEntry;
        (*new_exclRte).relkind = RELKIND_COMPOSITE_TYPE;
        /* Ignore the RTEPermissionInfo that would've been added. */
        (*new_exclRte).perminfoindex = 0;

        (*parsetree).rtable = lappend((*parsetree).rtable, new_exclRte as *mut c_void);
        let new_exclRelIndex_val = list_length((*parsetree).rtable);
        (*(*parsetree).onConflict).exclRelIndex = new_exclRelIndex_val;

        /*
         * Replace the targetlist for the EXCLUDED pseudo-relation.
         */
        (*(*parsetree).onConflict).exclRelTlist =
            BuildOnConflictExcludedTargetlist(base_rel, new_exclRelIndex_val);

        /*
         * Update all Vars in the ON CONFLICT clause that refer to the old
         * EXCLUDED pseudo-relation.
         */
        tmp_tlist = copyObject(view_targetlist);

        ChangeVarNodes(
            tmp_tlist as *mut Node,
            new_rt_index,
            new_exclRelIndex_val,
            0,
        );

        (*parsetree).onConflict = ReplaceVarsFromTargetList(
            (*parsetree).onConflict as *mut Node,
            old_exclRelIndex as c_int,
            0,
            view_rte,
            tmp_tlist,
            new_rt_index,
            REPLACEVARS_REPORT_ERROR,
            0,
            &mut (*parsetree).hasSubLinks,
        ) as *mut crate::nodes::primnodes::OnConflictExpr;
    }

    /*
     * For UPDATE/DELETE/MERGE, pull up any WHERE quals from the view.
     */
    if (*parsetree).commandType != CMD_INSERT
        && !(*(*viewquery).jointree).quals.is_null()
    {
        let mut viewqual: *mut Node =
            (*(*viewquery).jointree).quals as *mut Node;

        /*
         * Even though we copied viewquery already at the top of this
         * function, we must duplicate the viewqual again here.
         */
        viewqual = copyObject(viewqual);

        ChangeVarNodes(viewqual, base_rt_index, new_rt_index, 0);

        if RelationIsSecurityView(view) {
            /*
             * The view's quals go in front of existing barrier quals.
             *
             * Note: the parsetree has been mutated, so the new_rte pointer is
             * stale and needs to be re-computed.
             */
            new_rte = rt_fetch(new_rt_index, (*parsetree).rtable);
            (*new_rte).securityQuals = lcons(viewqual as *mut c_void, (*new_rte).securityQuals);

            /*
             * Do not set parsetree->hasRowSecurity, because these aren't RLS
             * conditions.
             */

            /*
             * Make sure that the query is marked correctly if the added qual
             * has sublinks.
             */
            if !(*parsetree).hasSubLinks {
                (*parsetree).hasSubLinks = checkExprHasSubLink(viewqual);
            }
        } else {
            AddQual(parsetree, viewqual);
        }
    }

    /*
     * For INSERT/UPDATE (or MERGE containing INSERT/UPDATE), if the view has
     * the WITH CHECK OPTION, or any parent view specified WITH CASCADED CHECK
     * OPTION, add the quals from the view to the query's withCheckOptions list.
     */
    if insert_or_update {
        let has_wco: bool = RelationHasCheckOption(view);
        let mut cascaded: bool = RelationHasCascadedCheckOption(view);

        /*
         * If the parent view has a cascaded check option, treat this view as
         * if it also had a cascaded check option.
         */
        if (*parsetree).withCheckOptions != NIL {
            let parent_wco: *mut crate::nodes::parsenodes::WithCheckOption =
                linitial((*parsetree).withCheckOptions)
                    as *mut crate::nodes::parsenodes::WithCheckOption;

            if (*parent_wco).cascaded {
                let _ = has_wco; // has_wco = true
                cascaded = true;
            }
        }

        /*
         * Add the new WithCheckOption to the start of the list.
         */
        if (has_wco || cascaded) && (cascaded || !(*(*viewquery).jointree).quals.is_null()) {
            let wco: *mut crate::nodes::parsenodes::WithCheckOption =
                makeNode!(crate::nodes::parsenodes::WithCheckOption, T_WithCheckOption)
                    as *mut crate::nodes::parsenodes::WithCheckOption;

            (*wco).kind = WCO_VIEW_CHECK;
            (*wco).relname = pstrdup(RelationGetRelationName(view) as *const c_char);
            (*wco).polname = core::ptr::null_mut();
            (*wco).qual = core::ptr::null_mut();
            (*wco).cascaded = cascaded;

            (*parsetree).withCheckOptions = lcons(wco as *mut c_void, (*parsetree).withCheckOptions);

            if !(*(*viewquery).jointree).quals.is_null() {
                (*wco).qual = (*(*viewquery).jointree).quals as *mut Node;
                ChangeVarNodes((*wco).qual, base_rt_index, new_rt_index, 0);

                /*
                 * For INSERT, make sure that the query is marked correctly if
                 * the added qual has sublinks.
                 */
                if !(*parsetree).hasSubLinks
                    && (*parsetree).commandType == CMD_INSERT
                {
                    (*parsetree).hasSubLinks = checkExprHasSubLink((*wco).qual);
                }
            }
        }
    }

    table_close(base_rel, NoLock);

    parsetree
}

// ===========================================================================
// PART 6: RewriteQuery, expand_generated_columns_internal,
//         expand_generated_columns_in_expr, build_generation_expression,
//         QueryRewrite
// ===========================================================================

unsafe fn RewriteQuery(
    parsetree: *mut Query,
    rewrite_events: *mut List,
    orig_rt_length: c_int,
    num_ctes_processed: c_int,
) -> *mut List {
    let event: CmdType = (*parsetree).commandType;
    let mut instead: bool = false;
    let mut returning: bool = false;
    let mut updatableview: bool = false;
    let mut qual_product: *mut Query = core::ptr::null_mut();
    let mut rewritten: *mut List = NIL;

    /*
     * First, recursively process any insert/update/delete/merge statements in
     * WITH clauses.
     */
    let cte_count = list_length((*parsetree).cteList);
    let mut i: c_int = 0;
    foreach!(lc1, (*parsetree).cteList, {
        if i >= cte_count - num_ctes_processed {
            break;
        }
        i += 1;

        let cte: *mut CommonTableExpr = lfirst(current_cell!(lc1)) as *mut CommonTableExpr;
        let ctequery: *mut Query = castNode!(Query, T_Query, (*cte).ctequery as *mut c_void);

        if (*ctequery).commandType == CMD_SELECT {
            continue;
        }

        let newstuff: *mut List = RewriteQuery(ctequery, rewrite_events, 0, 0);

        if list_length(newstuff) == 1 {
            let ctequery2: *mut Query =
                linitial_node!(Query, T_Query, newstuff) as *mut Query;
            if !((*ctequery2).commandType == CMD_SELECT
                || (*ctequery2).commandType == CMD_UPDATE
                || (*ctequery2).commandType == CMD_INSERT
                || (*ctequery2).commandType == CMD_DELETE
                || (*ctequery2).commandType == CMD_MERGE)
            {
                ereport!(ERROR, errmsg!("DO INSTEAD NOTIFY rules are not supported for data-modifying statements in WITH")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }
            assert!(!(*ctequery2).canSetTag);
            (*cte).ctequery = ctequery2 as *mut Node;
        } else if newstuff == NIL {
            ereport!(ERROR, errmsg!("DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        } else {
            foreach!(lc2, newstuff, {
                let q: *mut Query = lfirst(current_cell!(lc2)) as *mut Query;

                if (*q).querySource == QSRC_QUAL_INSTEAD_RULE {
                    ereport!(ERROR, errmsg!("conditional DO INSTEAD rules are not supported for data-modifying statements in WITH")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }
                if (*q).querySource == QSRC_NON_INSTEAD_RULE {
                    ereport!(ERROR, errmsg!("DO ALSO rules are not supported for data-modifying statements in WITH")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }
            });

            ereport!(ERROR, errmsg!("multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
    });
    let num_ctes_processed = list_length((*parsetree).cteList);

    /*
     * If the statement is an insert, update, delete, or merge, adjust its
     * targetlist as needed, and then fire INSERT/UPDATE/DELETE rules on it.
     */
    if event != CMD_SELECT && event != CMD_UTILITY {
        let result_relation: c_int = (*parsetree).resultRelation;
        assert!(result_relation != 0);
        let rt_entry: *mut RangeTblEntry = rt_fetch(result_relation, (*parsetree).rtable);
        assert!((*rt_entry).rtekind == RTE_RELATION);

        let rt_entry_relation: Relation = table_open((*rt_entry).relid, NoLock);

        let mut values_rte_index: c_int = 0;
        let mut defaults_remaining: bool = false;
        let mut hasUpdate: bool = false;

        if event == CMD_INSERT {
            let mut values_rte: *mut RangeTblEntry = core::ptr::null_mut();

            foreach!(lc2, (*(*parsetree).jointree).fromlist, {
                let rtr: *mut RangeTblRef = lfirst(current_cell!(lc2)) as *mut RangeTblRef;

                if IsA!(rtr as *mut Node, T_RangeTblRef) && (*rtr).rtindex > orig_rt_length {
                    let rte: *mut RangeTblEntry =
                        rt_fetch((*rtr).rtindex, (*parsetree).rtable);

                    if (*rte).rtekind == RTE_VALUES {
                        if !values_rte.is_null() {
                            elog!(ERROR, "more than one VALUES RTE found");
                        }
                        values_rte = rte;
                        values_rte_index = (*rtr).rtindex;
                    }
                }
            });

            if !values_rte.is_null() {
                let mut unused_values_attrnos: *mut Bitmapset = core::ptr::null_mut();

                (*parsetree).targetList = rewriteTargetListIU(
                    (*parsetree).targetList,
                    (*parsetree).commandType,
                    (*parsetree).r#override,
                    rt_entry_relation,
                    values_rte,
                    values_rte_index,
                    &mut unused_values_attrnos,
                );
                if !rewriteValuesRTE(
                    parsetree,
                    values_rte,
                    values_rte_index,
                    rt_entry_relation,
                    unused_values_attrnos,
                ) {
                    defaults_remaining = true;
                }
            } else {
                (*parsetree).targetList = rewriteTargetListIU(
                    (*parsetree).targetList,
                    (*parsetree).commandType,
                    (*parsetree).r#override,
                    rt_entry_relation,
                    core::ptr::null_mut(),
                    0,
                    core::ptr::null_mut(),
                );
            }

            if !(*parsetree).onConflict.is_null()
                && (*(*parsetree).onConflict).action == ONCONFLICT_UPDATE
            {
                (*(*parsetree).onConflict).onConflictSet = rewriteTargetListIU(
                    (*(*parsetree).onConflict).onConflictSet,
                    CMD_UPDATE,
                    (*parsetree).r#override,
                    rt_entry_relation,
                    core::ptr::null_mut(),
                    0,
                    core::ptr::null_mut(),
                );
            }
        } else if event == CMD_UPDATE {
            assert!((*parsetree).r#override == crate::nodes::primnodes::OverridingKind::OVERRIDING_NOT_SET);
            (*parsetree).targetList = rewriteTargetListIU(
                (*parsetree).targetList,
                (*parsetree).commandType,
                (*parsetree).r#override,
                rt_entry_relation,
                core::ptr::null_mut(),
                0,
                core::ptr::null_mut(),
            );
        } else if event == CMD_MERGE {
            assert!((*parsetree).r#override == crate::nodes::primnodes::OverridingKind::OVERRIDING_NOT_SET);

            foreach!(lc1, (*parsetree).mergeActionList, {
                let action: *mut crate::nodes::primnodes::MergeAction =
                    lfirst(current_cell!(lc1)) as *mut crate::nodes::primnodes::MergeAction;

                match (*action).commandType {
                    CMD_NOTHING | CMD_DELETE => { /* Nothing to do */ }
                    CMD_UPDATE | CMD_INSERT => {
                        (*action).targetList = rewriteTargetListIU(
                            (*action).targetList,
                            (*action).commandType,
                            (*action).r#override,
                            rt_entry_relation,
                            core::ptr::null_mut(),
                            0,
                            core::ptr::null_mut(),
                        );
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized commandType: {}",
                            (*action).commandType as c_int
                        );
                    }
                }
            });
        } else if event == CMD_DELETE {
            /* Nothing to do here */
        } else {
            elog!(ERROR, "unrecognized commandType: {}", event as c_int);
        }

        let locks: *mut List =
            matchLocks(event, rt_entry_relation, result_relation, parsetree, &mut hasUpdate);

        let product_orig_rt_length: c_int = list_length((*parsetree).rtable);
        let product_queries: *mut List = fireRules(
            parsetree,
            result_relation,
            event,
            locks,
            &mut instead,
            &mut returning,
            &mut qual_product,
        );

        /*
         * If we have a VALUES RTE with remaining DEFAULT items, and product
         * queries exist, finalize the VALUES RTE for each product query.
         */
        if defaults_remaining && product_queries != NIL {
            foreach!(n, product_queries, {
                let mut pt: *mut Query = lfirst(current_cell!(n)) as *mut Query;

                if (*pt).commandType == CMD_INSERT
                    && !(*pt).jointree.is_null()
                    && IsA!((*pt).jointree as *mut Node, T_FromExpr)
                    && list_length((*(*pt).jointree).fromlist) == 1
                {
                    let jtnode: *mut Node =
                        linitial((*(*pt).jointree).fromlist) as *mut Node;

                    if IsA!(jtnode, T_RangeTblRef) {
                        let rtindex: c_int = (*(jtnode as *mut RangeTblRef)).rtindex;
                        let src_rte: *mut RangeTblEntry =
                            rt_fetch(rtindex, (*pt).rtable);

                        if (*src_rte).rtekind == RTE_SUBQUERY
                            && !(*src_rte).subquery.is_null()
                            && IsA!((*src_rte).subquery as *mut Node, T_Query)
                            && (*(*src_rte).subquery).commandType == CMD_SELECT
                        {
                            pt = (*src_rte).subquery;
                        }
                    }
                }

                let values_rte2: *mut RangeTblEntry = rt_fetch(values_rte_index, (*pt).rtable);
                if (*values_rte2).rtekind != RTE_VALUES {
                    elog!(ERROR, "failed to find VALUES RTE in product query");
                }

                rewriteValuesRTEToNulls(pt, values_rte2);
            });
        }

        /*
         * If no unqualified INSTEAD rule, and target is a view without INSTEAD
         * OF triggers, try auto-update.
         */
        if !instead
            && (*(*rt_entry_relation).rd_rel).relkind == RELKIND_VIEW
            && !view_has_instead_trigger_impl(
                rt_entry_relation,
                event,
                (*parsetree).mergeActionList,
            )
        {
            if !qual_product.is_null() {
                error_view_not_updatable_impl(
                    rt_entry_relation,
                    (*parsetree).commandType,
                    (*parsetree).mergeActionList,
                    gettext_noop!(
                        "Views with conditional DO INSTEAD rules are not automatically updatable."
                    ),
                );
            }

            let parsetree2 = rewriteTargetView(parsetree, rt_entry_relation);
            // parsetree is now re-assigned; update the outer mutable binding
            let product_queries2 = if (*parsetree2).commandType == CMD_INSERT {
                lcons(parsetree2 as *mut c_void, product_queries)
            } else {
                lappend(product_queries, parsetree2 as *mut c_void)
            };

            instead = true;
            returning = true;
            updatableview = true;

            /* Recurse on product queries including the rewritten view query */
            if product_queries2 != NIL {
                let rev: *mut rewrite_event =
                    palloc(core::mem::size_of::<rewrite_event>()) as *mut rewrite_event;
                (*rev).relation = RelationGetRelid(rt_entry_relation);
                (*rev).event = event;
                let rewrite_events2 = lappend(rewrite_events, rev as *mut c_void);

                foreach!(n, product_queries2, {
                    let pt: *mut Query = lfirst(current_cell!(n)) as *mut Query;
                    let newstuff: *mut List = RewriteQuery(
                        pt,
                        rewrite_events2,
                        if pt == parsetree2 {
                            orig_rt_length
                        } else {
                            product_orig_rt_length
                        },
                        num_ctes_processed,
                    );
                    rewritten = list_concat(rewritten, newstuff);
                });

                let _ = list_delete_last(rewrite_events2);
            }

            if (instead || !qual_product.is_null())
                && !(*parsetree2).returningList.is_null()
                && !returning
            {
                match event {
                    CMD_INSERT => {
                        ereport!(ERROR, errmsg!("cannot perform INSERT RETURNING on relation \"{}\"",
                            cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                            /* C also: errcode, errhint */);
                    }
                    CMD_UPDATE => {
                        ereport!(ERROR, errmsg!("cannot perform UPDATE RETURNING on relation \"{}\"",
                            cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                            /* C also: errcode, errhint */);
                    }
                    CMD_DELETE => {
                        ereport!(ERROR, errmsg!("cannot perform DELETE RETURNING on relation \"{}\"",
                            cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                            /* C also: errcode, errhint */);
                    }
                    _ => {
                        elog!(ERROR, "unrecognized commandType: {}", event as c_int);
                    }
                }
            }

            if !(*parsetree2).onConflict.is_null()
                && (product_queries2 != NIL || hasUpdate)
                && !updatableview
            {
                ereport!(ERROR, errmsg!("INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }

            table_close(rt_entry_relation, NoLock);

            /* finish the "not instead" path for the rewritten query */
            if !instead {
                if (*parsetree2).commandType == CMD_INSERT {
                    if !qual_product.is_null() {
                        rewritten = lcons(qual_product as *mut c_void, rewritten);
                    } else {
                        rewritten = lcons(parsetree2 as *mut c_void, rewritten);
                    }
                } else {
                    if !qual_product.is_null() {
                        rewritten = lappend(rewritten, qual_product as *mut c_void);
                    } else {
                        rewritten = lappend(rewritten, parsetree2 as *mut c_void);
                    }
                }
            }

            if (*parsetree2).cteList != NIL {
                let mut qcount: c_int = 0;
                foreach!(lc1, rewritten, {
                    let q: *mut Query = lfirst(current_cell!(lc1)) as *mut Query;
                    if (*q).commandType != CMD_UTILITY {
                        qcount += 1;
                    }
                });
                if qcount > 1 {
                    ereport!(ERROR, errmsg!("WITH cannot be used in a query that is rewritten by rules into multiple queries")
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                }
            }

            return rewritten;
        }

        /* Normal path (non-view or view with instead trigger) */
        if product_queries != NIL {
            let rev: *mut rewrite_event =
                palloc(core::mem::size_of::<rewrite_event>()) as *mut rewrite_event;
            (*rev).relation = RelationGetRelid(rt_entry_relation);
            (*rev).event = event;

            foreach!(n, rewrite_events, {
                let rev2: *mut rewrite_event = lfirst(current_cell!(n)) as *mut rewrite_event;
                if (*rev2).relation == (*rev).relation && (*rev2).event == event {
                    ereport!(ERROR, errmsg!("infinite recursion detected in rules for relation \"{}\"",
                        cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */);
                }
            });

            let rewrite_events2 = lappend(rewrite_events, rev as *mut c_void);

            foreach!(n, product_queries, {
                let pt: *mut Query = lfirst(current_cell!(n)) as *mut Query;
                let newstuff: *mut List = RewriteQuery(
                    pt,
                    rewrite_events2,
                    if pt == parsetree {
                        orig_rt_length
                    } else {
                        product_orig_rt_length
                    },
                    num_ctes_processed,
                );
                rewritten = list_concat(rewritten, newstuff);
            });

            let _ = list_delete_last(rewrite_events2);
        }

        if (instead || !qual_product.is_null())
            && !(*parsetree).returningList.is_null()
            && !returning
        {
            match event {
                CMD_INSERT => {
                    ereport!(ERROR, errmsg!("cannot perform INSERT RETURNING on relation \"{}\"",
                        cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                        /* C also: errcode, errhint */);
                }
                CMD_UPDATE => {
                    ereport!(ERROR, errmsg!("cannot perform UPDATE RETURNING on relation \"{}\"",
                        cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                        /* C also: errcode, errhint */);
                }
                CMD_DELETE => {
                    ereport!(ERROR, errmsg!("cannot perform DELETE RETURNING on relation \"{}\"",
                        cstr(RelationGetRelationName(rt_entry_relation) as *const c_char))
                        /* C also: errcode, errhint */);
                }
                _ => {
                    elog!(ERROR, "unrecognized commandType: {}", event as c_int);
                }
            }
        }

        if !(*parsetree).onConflict.is_null()
            && (product_queries != NIL || hasUpdate)
            && !updatableview
        {
            ereport!(ERROR, errmsg!("INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }

        table_close(rt_entry_relation, NoLock);
    }

    /*
     * For INSERTs, the original query is done first; for UPDATE/DELETE, last.
     */
    if !instead {
        if (*parsetree).commandType == CMD_INSERT {
            if !qual_product.is_null() {
                rewritten = lcons(qual_product as *mut c_void, rewritten);
            } else {
                rewritten = lcons(parsetree as *mut c_void, rewritten);
            }
        } else {
            if !qual_product.is_null() {
                rewritten = lappend(rewritten, qual_product as *mut c_void);
            } else {
                rewritten = lappend(rewritten, parsetree as *mut c_void);
            }
        }
    }

    /*
     * If the original query has a CTE list, and we have multiple result
     * queries, that's an error.
     */
    if (*parsetree).cteList != NIL {
        let mut qcount: c_int = 0;
        foreach!(lc1, rewritten, {
            let q: *mut Query = lfirst(current_cell!(lc1)) as *mut Query;
            if (*q).commandType != CMD_UTILITY {
                qcount += 1;
            }
        });
        if qcount > 1 {
            ereport!(ERROR, errmsg!("WITH cannot be used in a query that is rewritten by rules into multiple queries")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        }
    }

    rewritten
}


/*
 * expand_generated_columns_internal - expand virtual generated columns
 * replacing Vars at rt_index with their generation expressions.
 */
unsafe fn expand_generated_columns_internal(
    node: *mut Node,
    rel: Relation,
    rt_index: c_int,
    rte: *mut RangeTblEntry,
    result_relation: c_int,
) -> *mut Node {
    let tupdesc: TupleDesc = RelationGetDescr(rel);

    if !(*tupdesc).constr.is_null()
        && (*(*tupdesc).constr).has_generated_virtual
    {
        let mut tlist: *mut List = NIL;

        let mut i: c_int = 0;
        while i < (*tupdesc).natts {
            let attr: Form_pg_attribute =
                TupleDescAttr(tupdesc, i);

            if (*attr).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                let defexpr: *mut Node =
                    build_generation_expression_impl(rel, i + 1);
                ChangeVarNodes(defexpr, 1, rt_index, 0);

                let te: *mut TargetEntry = makeTargetEntry(
                    defexpr as *mut crate::nodes::primnodes::Expr,
                    (i + 1) as AttrNumber,
                    core::ptr::null_mut(),
                    false,
                );
                tlist = lappend(tlist, te as *mut c_void);
            }
            i += 1;
        }

        assert!(list_length(tlist) > 0);

        return ReplaceVarsFromTargetList(
            node,
            rt_index,
            0,
            rte,
            tlist,
            result_relation,
            REPLACEVARS_CHANGE_VARNO,
            rt_index,
            core::ptr::null_mut(),
        );
    }

    node
}


/*
 * expand_generated_columns_in_expr - expand virtual generated columns in an
 * expression that is not part of a query (e.g., a default expression or index
 * predicate). The rt_index is usually 1.
 */
#[no_mangle]
pub unsafe fn expand_generated_columns_in_expr(
    node: *mut Node,
    rel: Relation,
    rt_index: c_int,
) -> *mut Node {
    let tupdesc: TupleDesc = RelationGetDescr(rel);

    if !(*tupdesc).constr.is_null()
        && (*(*tupdesc).constr).has_generated_virtual
    {
        let rte: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry)
            as *mut RangeTblEntry;

        /* eref needs to be set; the actual name doesn't matter */
        (*rte).eref = makeAlias(RelationGetRelationName(rel) as *const c_char, NIL);
        (*rte).rtekind = RTE_RELATION;
        (*rte).relid = RelationGetRelid(rel);

        return expand_generated_columns_internal(node, rel, rt_index, rte, 0);
    }

    node
}


/*
 * build_generation_expression - build the generation expression for a virtual
 * generated column. Errors out if no generation expression is found.
 */
pub unsafe fn build_generation_expression(rel: Relation, attrno: c_int) -> *mut Node {
    build_generation_expression_impl(rel, attrno)
}

unsafe fn build_generation_expression_impl(rel: Relation, attrno: c_int) -> *mut Node {
    let rd_att: TupleDesc = RelationGetDescr(rel);
    let att_tup: Form_pg_attribute =
        TupleDescAttr(rd_att, attrno - 1);

    assert!(!(*rd_att).constr.is_null() && (*(*rd_att).constr).has_generated_virtual);
    assert!((*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL);

    let mut defexpr: *mut Node = build_column_default(rel, attrno);
    if defexpr.is_null() {
        elog!(
            ERROR,
            "no generation expression found for column number {} of table \"{}\"",
            attrno,
            cstr(RelationGetRelationName(rel) as *const c_char)
        );
    }

    let attcollid: Oid = (*att_tup).attcollation;
    if attcollid != InvalidOid && attcollid != exprCollation(defexpr) {
        let ce: *mut crate::nodes::primnodes::CollateExpr =
            makeNode!(crate::nodes::primnodes::CollateExpr, T_CollateExpr)
                as *mut crate::nodes::primnodes::CollateExpr;

        (*ce).arg = defexpr as *mut crate::nodes::primnodes::Expr;
        (*ce).collOid = attcollid;
        (*ce).location = -1;

        defexpr = ce as *mut Node;
    }

    defexpr
}


/*
 * QueryRewrite -
 *   Primary entry point to the query rewriter. Rewrite one query via the
 *   query rewrite system, possibly returning 0 or many queries.
 */
pub unsafe fn QueryRewrite(parsetree: *mut Query) -> *mut List {
    let input_query_id: i64 = (*parsetree).queryId;
    let querylist: *mut List;
    let mut results: *mut List = NIL;
    let origCmdType: CmdType;
    let mut foundOriginalQuery: bool = false;
    let mut lastInstead: *mut Query = core::ptr::null_mut();

    assert!((*parsetree).querySource == QSRC_ORIGINAL);
    assert!((*parsetree).canSetTag);

    /*
     * Step 1: Apply all non-SELECT rules, possibly getting 0 or many queries.
     */
    querylist = RewriteQuery(parsetree, NIL, 0, 0);

    /*
     * Step 2: Apply all RIR rules on each query. Also stamp each query with
     * the original queryId.
     */
    foreach!(l, querylist, {
        let mut query: *mut Query = lfirst(current_cell!(l)) as *mut Query;

        query = fireRIRrules(query, NIL);
        (*query).queryId = input_query_id;

        results = lappend(results, query as *mut c_void);
    });

    /*
     * Step 3: Determine which result query sets the command-result tag.
     *
     * If the original query is still in the list, it sets the tag.
     * Otherwise, the last INSTEAD query of the same kind is allowed to set it.
     */
    origCmdType = (*parsetree).commandType;

    foreach!(l, results, {
        let query: *mut Query = lfirst(current_cell!(l)) as *mut Query;

        if (*query).querySource == QSRC_ORIGINAL {
            assert!((*query).canSetTag);
            assert!(!foundOriginalQuery);
            foundOriginalQuery = true;
            break; /* no need to look further if not asserting */
        } else {
            assert!(!(*query).canSetTag);
            if (*query).commandType == origCmdType
                && ((*query).querySource == QSRC_INSTEAD_RULE
                    || (*query).querySource == QSRC_QUAL_INSTEAD_RULE)
            {
                lastInstead = query;
            }
        }
    });

    if !foundOriginalQuery && !lastInstead.is_null() {
        (*lastInstead).canSetTag = true;
    }

    results
}

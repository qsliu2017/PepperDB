//! plancache.rs
//!   Plan cache management.
//!
//! Translated 1:1 from postgres/src/backend/utils/cache/plancache.c
//!
//! The plan cache manager has two principal responsibilities: deciding when
//! to use a generic plan versus a custom (parameter-value-specific) plan,
//! and tracking whether cached plans need to be invalidated because of schema
//! changes in the objects they depend on.
//!
//! The logic for choosing generic or custom plans is in choose_custom_plan,
//! which see for comments.
//!
//! Cache invalidation is driven off sinval events.  Any CachedPlanSource
//! that matches the event is marked invalid, as is its generic CachedPlan
//! if it has one.  When (and if) the next demand for a cached plan occurs,
//! parse analysis and/or rewrite is repeated to build a new valid query tree,
//! and then planning is performed as normal.  We also force re-analysis and
//! re-planning if the active search_path is different from the previous time
//! or, if RLS is involved, if the user changes or the RLS environment changes.
//!
//! Note that if the sinval was a result of user DDL actions, parse analysis
//! could throw an error, for example if a column referenced by the query is
//! no longer present.  Another possibility is for the query's output tupdesc
//! to change (for instance "SELECT *" might expand differently than before).
//! The creator of a cached plan can specify whether it is allowable for the
//! query to change output tupdesc on replan --- if so, it's up to the
//! caller to notice changes and cope with them.
//!
//! Currently, we track exactly the dependencies of plans on relations,
//! user-defined functions, and domains.  On relcache invalidation events or
//! pg_proc or pg_type syscache invalidation events, we invalidate just those
//! plans that depend on the particular object being modified.  (Note: this
//! scheme assumes that any table modification that requires replanning will
//! generate a relcache inval event.)  We also watch for inval events on
//! certain other system catalogs, such as pg_namespace; but for them, our
//! response is just to invalidate all plans.  We expect updates on those
//! catalogs to be infrequent enough that more-detailed tracking is not worth
//! the effort.
//!
//! In addition to full-fledged query plans, we provide a facility for
//! detecting invalidations of simple scalar expressions.  This is fairly
//! bare-bones; it's the caller's responsibility to build a new expression
//! if the old one gets invalidated.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/cache/plancache.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;
use crate::{foreach, current_cell, IsA, castNode, lfirst_node, linitial_node, dlist_foreach, dlist_container};

use std::ffi::{c_char, c_int, c_void};
use libc::memcpy;

use crate::c::{uint32, int64, TransactionId};
use crate::postgres_ext::{Oid, InvalidOid};

use crate::lib::ilist::{dlist_head, dlist_node, dlist_iter, dlist_init, dlist_push_tail, dlist_delete};
use crate::nodes::nodes::{Node, CmdType, CMD_UTILITY};
use crate::nodes::pg_list::{List, NIL, list_length, list_member_oid, lfirst};
use crate::nodes::params::{ParamListInfo, ParserSetupHook};
use crate::nodes::parsenodes::{
    Query, RawStmt, RangeTblEntry, RTEKind, RTE_RELATION, RTE_SUBQUERY, CommonTableExpr,
    CURSOR_OPT_GENERIC_PLAN, CURSOR_OPT_CUSTOM_PLAN,
};
use crate::nodes::primnodes::SubLink;
use crate::nodes::plannodes::{PlannedStmt, PlanInvalItem};
use crate::nodes::nodeFuncs::{query_tree_walker, expression_tree_walker, QTW_IGNORE_RC_SUBQUERIES};

use crate::access::common::tupdesc::{
    TupleDesc, CreateTupleDescCopy, FreeTupleDesc, equalRowTypes,
};
use crate::access::transam::{
    InvalidTransactionId, TransactionIdIsValid, TransactionIdIsNormal, TransactionIdEquals,
};

use crate::tcop::cmdtag::CommandTag;
use crate::catalog::namespace::SearchPathMatcher;
use crate::utils::misc::queryenvironment::QueryEnvironment;
use crate::utils::misc::rls::row_security;
use crate::utils::memutils::ALLOCSET_SMALL_SIZES;
use crate::miscadmin::GetUserId;
use crate::utils::resowner::resowner::{
    ResourceOwner, ResourceOwnerDesc, RESOURCE_RELEASE_AFTER_LOCKS, RELEASE_PRIO_PLANCACHE_REFS,
    ResourceOwnerRemember, ResourceOwnerForget, ResourceOwnerEnlarge, ResourceOwnerReleaseAllOfKind,
};

// ---------------------------------------------------------------------------
// plancache.h  --  src/include/utils/plancache.h
//
// Plan cache definitions.  See plancache.c for comments.
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
// ---------------------------------------------------------------------------

/* possible values for plan_cache_mode */
pub const PLAN_CACHE_MODE_AUTO: c_int = 0;
pub const PLAN_CACHE_MODE_FORCE_GENERIC_PLAN: c_int = 1;
pub const PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN: c_int = 2;

/* Optional callback to editorialize on rewritten parse trees */
pub type PostRewriteHook = Option<unsafe fn(querytree_list: *mut List, arg: *mut c_void)>;

pub const CACHEDPLANSOURCE_MAGIC: c_int = 195726186;
pub const CACHEDPLAN_MAGIC: c_int = 953717834;
pub const CACHEDEXPR_MAGIC: c_int = 838275847;

/*
 * CachedPlanSource (which might better have been called CachedQuery)
 * represents a SQL query that we expect to use multiple times.  See the
 * extensive comment in plancache.h for full details.
 */
#[repr(C)]
pub struct CachedPlanSource {
    pub magic: c_int,                       /* should equal CACHEDPLANSOURCE_MAGIC */
    pub raw_parse_tree: *mut RawStmt,       /* output of raw_parser(), or NULL */
    pub analyzed_parse_tree: *mut Query,    /* analyzed parse tree, or NULL */
    pub query_string: *const c_char,        /* source text of query */
    pub commandTag: CommandTag,             /* command tag for query */
    pub param_types: *mut Oid,              /* array of parameter type OIDs, or NULL */
    pub num_params: c_int,                  /* length of param_types array */
    pub parserSetup: ParserSetupHook,       /* alternative parameter spec method */
    pub parserSetupArg: *mut c_void,
    pub postRewrite: PostRewriteHook,       /* see SetPostRewriteHook */
    pub postRewriteArg: *mut c_void,
    pub cursor_options: c_int,              /* cursor options used for planning */
    pub fixed_result: bool,                 /* disallow change in result tupdesc? */
    pub resultDesc: TupleDesc,              /* result type; NULL = doesn't return tuples */
    pub context: MemoryContext,             /* memory context holding all above */
    /* These fields describe the current analyzed-and-rewritten query tree: */
    pub query_list: *mut List,              /* list of Query nodes, or NIL if not valid */
    pub relationOids: *mut List,            /* OIDs of relations the queries depend on */
    pub invalItems: *mut List,              /* other dependencies, as PlanInvalItems */
    pub search_path: *mut SearchPathMatcher, /* search_path used for parsing and planning */
    pub query_context: MemoryContext,       /* context holding the above, or NULL */
    pub rewriteRoleId: Oid,                 /* Role ID we did rewriting for */
    pub rewriteRowSecurity: bool,           /* row_security used during rewrite */
    pub dependsOnRLS: bool,                 /* is rewritten query specific to the above? */
    /* If we have a generic plan, this is a reference-counted link to it: */
    pub gplan: *mut CachedPlan,             /* generic plan, or NULL if not valid */
    /* Some state flags: */
    pub is_oneshot: bool,                   /* is it a "oneshot" plan? */
    pub is_complete: bool,                  /* has CompleteCachedPlan been done? */
    pub is_saved: bool,                     /* has CachedPlanSource been "saved"? */
    pub is_valid: bool,                     /* is the query_list currently valid? */
    pub generation: c_int,                  /* increments each time we create a plan */
    /* If CachedPlanSource has been saved, it is a member of a global list */
    pub node: dlist_node,                   /* list link, if is_saved */
    /* State kept to help decide whether to use custom or generic plans: */
    pub generic_cost: f64,                  /* cost of generic plan, or -1 if not known */
    pub total_custom_cost: f64,             /* total cost of custom plans so far */
    pub num_custom_plans: int64,            /* # of custom plans included in total */
    pub num_generic_plans: int64,           /* # of generic plans */
}

/*
 * CachedPlan represents an execution plan derived from a CachedPlanSource.
 */
#[repr(C)]
pub struct CachedPlan {
    pub magic: c_int,               /* should equal CACHEDPLAN_MAGIC */
    pub stmt_list: *mut List,       /* list of PlannedStmts */
    pub is_oneshot: bool,           /* is it a "oneshot" plan? */
    pub is_saved: bool,             /* is CachedPlan in a long-lived context? */
    pub is_valid: bool,             /* is the stmt_list currently valid? */
    pub planRoleId: Oid,            /* Role ID the plan was created for */
    pub dependsOnRole: bool,        /* is plan specific to that role? */
    pub saved_xmin: TransactionId,  /* if valid, replan when TransactionXmin changes */
    pub generation: c_int,          /* parent's generation number for this plan */
    pub refcount: c_int,            /* count of live references to this struct */
    pub context: MemoryContext,     /* context containing this CachedPlan */
}

/*
 * CachedExpression is a low-overhead mechanism for caching the planned form
 * of standalone scalar expressions.
 */
#[repr(C)]
pub struct CachedExpression {
    pub magic: c_int,               /* should equal CACHEDEXPR_MAGIC */
    pub expr: *mut Node,            /* planned form of expression */
    pub is_valid: bool,             /* is the expression still valid? */
    /* remaining fields should be treated as private to plancache.c: */
    pub relationOids: *mut List,    /* OIDs of relations the expr depends on */
    pub invalItems: *mut List,      /* other dependencies, as PlanInvalItems */
    pub context: MemoryContext,     /* context containing this CachedExpression */
    pub node: dlist_node,           /* link in global list of CachedExpressions */
}

// ---------------------------------------------------------------------------
// Dependency stubs: these live in other .c files that have not been ported.
// ---------------------------------------------------------------------------

/* TransactionXmin lives in utils/time/snapmgr.c, not ported yet. */
static mut TransactionXmin: TransactionId = InvalidTransactionId;

/* cpu_operator_cost lives in optimizer/path/costsize.c. */
static mut cpu_operator_cost: f64 = 0.0025;

/* CacheMemoryContext lives in utils/cache/catcache.c (inval.c group). */
static mut CacheMemoryContext: MemoryContext = null_mut();

extern "C" {
    /* inval.c */
    fn CacheRegisterRelcacheCallback(func: RelcacheCallbackFunction, arg: Datum);
    fn CacheRegisterSyscacheCallback(cacheid: c_int, func: SyscacheCallbackFunction, arg: Datum);
}

pub type RelcacheCallbackFunction = unsafe fn(arg: Datum, relid: Oid);
pub type SyscacheCallbackFunction = unsafe fn(arg: Datum, cacheid: c_int, hashvalue: uint32);

/* Syscache IDs (catalog/pg_*; from syscache.h). */
const PROCOID: c_int = 47;
const TYPEOID: c_int = 82;
const NAMESPACEOID: c_int = 38;
const OPEROID: c_int = 40;
const AMOPOPID: c_int = 3;
const FOREIGNSERVEROID: c_int = 32;
const FOREIGNDATAWRAPPEROID: c_int = 30;

/* memutils.h: ALLOCSET_START_SMALL_SIZES handled by AllocSetContextCreate macro. */

unsafe fn MemoryContextSetParent(_context: MemoryContext, _new_parent: MemoryContext) {
    // TODO(pg-port): real MemoryContextSetParent lives in utils/mmgr/mcxt.c
    unimplemented!()
}

unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    // TODO(pg-port): real MemoryContextCopyAndSetIdentifier lives in utils/mmgr/mcxt.c
    unimplemented!()
}

unsafe fn MemoryContextGetParent(_context: MemoryContext) -> MemoryContext {
    // TODO(pg-port): real MemoryContextGetParent lives in utils/mmgr/mcxt.c
    unimplemented!()
}

unsafe fn copyObjectImpl(_node: *const c_void) -> *mut c_void {
    // TODO(pg-port): real copyObject lives in nodes/copyfuncs.c
    unimplemented!()
}

/* copyObject() macro wrapper used throughout; preserves C type via cast at call. */
unsafe fn copyObject<T>(node: *mut T) -> *mut T {
    copyObjectImpl(node as *const c_void) as *mut T
}

unsafe fn stmt_requires_parse_analysis(_parse_tree: *mut RawStmt) -> bool {
    // TODO(pg-port): real stmt_requires_parse_analysis lives in parser/analyze.c
    unimplemented!()
}

unsafe fn analyze_requires_snapshot(_parse_tree: *mut RawStmt) -> bool {
    // TODO(pg-port): real analyze_requires_snapshot lives in parser/analyze.c
    unimplemented!()
}

unsafe fn query_requires_rewrite_plan(_query: *mut Query) -> bool {
    // TODO(pg-port): real query_requires_rewrite_plan lives in parser/analyze.c
    unimplemented!()
}

unsafe fn extract_query_dependencies(
    _query: *mut Node,
    _relationOids: *mut *mut List,
    _invalItems: *mut *mut List,
    _dependsOnRLS: *mut bool,
) {
    // TODO(pg-port): real extract_query_dependencies lives in optimizer/plan/setrefs.c
    unimplemented!()
}

unsafe fn GetSearchPathMatcher(_context: MemoryContext) -> *mut SearchPathMatcher {
    // TODO(pg-port): real GetSearchPathMatcher lives in catalog/namespace.c
    unimplemented!()
}

unsafe fn SearchPathMatchesCurrentEnvironment(_path: *mut SearchPathMatcher) -> bool {
    // TODO(pg-port): real SearchPathMatchesCurrentEnvironment lives in catalog/namespace.c
    unimplemented!()
}

unsafe fn CopySearchPathMatcher(_path: *mut SearchPathMatcher) -> *mut SearchPathMatcher {
    // TODO(pg-port): real CopySearchPathMatcher lives in catalog/namespace.c
    unimplemented!()
}

unsafe fn ActiveSnapshotSet() -> bool {
    // TODO(pg-port): real ActiveSnapshotSet lives in utils/time/snapmgr.c
    unimplemented!()
}

unsafe fn PushActiveSnapshot(_snap: *mut c_void) {
    // TODO(pg-port): real PushActiveSnapshot lives in utils/time/snapmgr.c
    unimplemented!()
}

unsafe fn PopActiveSnapshot() {
    // TODO(pg-port): real PopActiveSnapshot lives in utils/time/snapmgr.c
    unimplemented!()
}

unsafe fn GetTransactionSnapshot() -> *mut c_void {
    // TODO(pg-port): real GetTransactionSnapshot lives in utils/time/snapmgr.c
    unimplemented!()
}

unsafe fn pg_analyze_and_rewrite_withcb(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    // TODO(pg-port): real pg_analyze_and_rewrite_withcb lives in tcop/postgres.c
    unimplemented!()
}

unsafe fn pg_analyze_and_rewrite_fixedparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _param_types: *const Oid,
    _num_params: c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    // TODO(pg-port): real pg_analyze_and_rewrite_fixedparams lives in tcop/postgres.c
    unimplemented!()
}

unsafe fn AcquireRewriteLocks(_parsetree: *mut Query, _forExecute: bool, _forUpdatePushedDown: bool) {
    // TODO(pg-port): real AcquireRewriteLocks lives in rewrite/rewriteHandler.c
    unimplemented!()
}

unsafe fn pg_rewrite_query(_query: *mut Query) -> *mut List {
    // TODO(pg-port): real pg_rewrite_query lives in tcop/postgres.c
    unimplemented!()
}

unsafe fn pg_plan_queries(
    _querytrees: *mut List,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut List {
    // TODO(pg-port): real pg_plan_queries lives in tcop/postgres.c
    unimplemented!()
}

unsafe fn LockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): real LockRelationOid lives in storage/lmgr/lmgr.c
    unimplemented!()
}

unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): real UnlockRelationOid lives in storage/lmgr/lmgr.c
    unimplemented!()
}

unsafe fn UtilityContainsQuery(_parsetree: *mut Node) -> *mut Query {
    // TODO(pg-port): real UtilityContainsQuery lives in tcop/utility.c
    unimplemented!()
}

unsafe fn UtilityTupleDescriptor(_parsetree: *mut Node) -> TupleDesc {
    // TODO(pg-port): real UtilityTupleDescriptor lives in tcop/utility.c
    unimplemented!()
}

unsafe fn FetchStatementTargetList(_stmt: *mut Node) -> *mut List {
    // TODO(pg-port): real FetchStatementTargetList lives in tcop/pquery.c
    unimplemented!()
}

unsafe fn ChoosePortalStrategy(_stmts: *mut List) -> PortalStrategy {
    // TODO(pg-port): real ChoosePortalStrategy lives in tcop/pquery.c
    unimplemented!()
}

unsafe fn ExecCleanTypeFromTL(_targetList: *mut List) -> TupleDesc {
    // TODO(pg-port): real ExecCleanTypeFromTL lives in executor/execTuples.c
    unimplemented!()
}

unsafe fn expression_planner_with_deps(
    _expr: *mut c_void,
    _relationOids: *mut *mut List,
    _invalItems: *mut *mut List,
) -> *mut c_void {
    // TODO(pg-port): real expression_planner_with_deps lives in optimizer/plan/planner.c
    unimplemented!()
}

/* PortalStrategy values (tcop/pquery.h). */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PortalStrategy {
    PORTAL_ONE_SELECT,
    PORTAL_ONE_RETURNING,
    PORTAL_ONE_MOD_WITH,
    PORTAL_UTIL_SELECT,
    PORTAL_MULTI_QUERY,
}
pub use PortalStrategy::*;

// ---------------------------------------------------------------------------
// Module statics
// ---------------------------------------------------------------------------

/*
 * This is the head of the backend's list of "saved" CachedPlanSources (i.e.,
 * those that are in long-lived storage and are examined for sinval events).
 * We use a dlist instead of separate List cells so that we can guarantee
 * to save a CachedPlanSource without error.
 */
static mut saved_plan_list: dlist_head = dlist_head { head: dlist_node { prev: null_mut(), next: null_mut() } };

/*
 * This is the head of the backend's list of CachedExpressions.
 */
static mut cached_expression_list: dlist_head = dlist_head { head: dlist_node { prev: null_mut(), next: null_mut() } };

/* ResourceOwner callbacks to track plancache references */

static planref_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    name: b"plancache reference\0".as_ptr() as *const c_char,
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_PLANCACHE_REFS,
    ReleaseResource: ResOwnerReleaseCachedPlan,
    DebugPrint: None, /* the default message is fine */
};

/* Convenience wrappers over ResourceOwnerRemember/Forget */
#[inline]
unsafe fn ResourceOwnerRememberPlanCacheRef(owner: ResourceOwner, plan: *mut CachedPlan) {
    ResourceOwnerRemember(owner, PointerGetDatum(plan as *const c_void), &planref_resowner_desc);
}
#[inline]
unsafe fn ResourceOwnerForgetPlanCacheRef(owner: ResourceOwner, plan: *mut CachedPlan) {
    ResourceOwnerForget(owner, PointerGetDatum(plan as *const c_void), &planref_resowner_desc);
}

/* GUC parameter */
pub static mut plan_cache_mode: c_int = PLAN_CACHE_MODE_AUTO;

/*
 * InitPlanCache: initialize module during InitPostgres.
 *
 * All we need to do is hook into inval.c's callback lists.
 */
pub unsafe fn InitPlanCache() {
    // The C source statically initializes the two dlist heads via
    // DLIST_STATIC_INIT; here we initialize them once during module init.
    dlist_init(&raw mut saved_plan_list);
    dlist_init(&raw mut cached_expression_list);

    CacheRegisterRelcacheCallback(PlanCacheRelCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(PROCOID, PlanCacheObjectCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(TYPEOID, PlanCacheObjectCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(NAMESPACEOID, PlanCacheSysCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(OPEROID, PlanCacheSysCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(AMOPOPID, PlanCacheSysCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(FOREIGNSERVEROID, PlanCacheSysCallback, 0 as Datum);
    CacheRegisterSyscacheCallback(FOREIGNDATAWRAPPEROID, PlanCacheSysCallback, 0 as Datum);
}

/*
 * CreateCachedPlan: initially create a plan cache entry for a raw parse tree.
 *
 * See the comment in plancache.c for the full contract.
 *
 * raw_parse_tree: output of raw_parser(), or NULL if empty query
 * query_string: original query text
 * commandTag: command tag for query, or UNKNOWN if empty query
 */
pub unsafe fn CreateCachedPlan(
    raw_parse_tree: *mut RawStmt,
    query_string: *const c_char,
    commandTag: CommandTag,
) -> *mut CachedPlanSource {
    let plansource: *mut CachedPlanSource;
    let source_context: MemoryContext;
    let oldcxt: MemoryContext;

    Assert!(!query_string.is_null()); /* required as of 8.4 */

    /*
     * Make a dedicated memory context for the CachedPlanSource and its
     * permanent subsidiary data.  It's probably not going to be large, but
     * just in case, allow it to grow large.  Initially it's a child of the
     * caller's context (which we assume to be transient), so that it will be
     * cleaned up on error.
     */
    source_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"CachedPlanSource".as_ptr(),
        ALLOCSET_START_SMALL_SIZES
    );

    /*
     * Create and fill the CachedPlanSource struct within the new context.
     * Most fields are just left empty for the moment.
     */
    oldcxt = MemoryContextSwitchTo(source_context);

    plansource = palloc0(core::mem::size_of::<CachedPlanSource>()) as *mut CachedPlanSource;
    (*plansource).magic = CACHEDPLANSOURCE_MAGIC;
    (*plansource).raw_parse_tree = copyObject(raw_parse_tree);
    (*plansource).analyzed_parse_tree = null_mut();
    (*plansource).query_string = pstrdup(query_string);
    MemoryContextSetIdentifier(source_context, (*plansource).query_string);
    (*plansource).commandTag = commandTag;
    (*plansource).param_types = null_mut();
    (*plansource).num_params = 0;
    (*plansource).parserSetup = None;
    (*plansource).parserSetupArg = null_mut();
    (*plansource).postRewrite = None;
    (*plansource).postRewriteArg = null_mut();
    (*plansource).cursor_options = 0;
    (*plansource).fixed_result = false;
    (*plansource).resultDesc = null_mut();
    (*plansource).context = source_context;
    (*plansource).query_list = NIL;
    (*plansource).relationOids = NIL;
    (*plansource).invalItems = NIL;
    (*plansource).search_path = null_mut();
    (*plansource).query_context = null_mut();
    (*plansource).rewriteRoleId = InvalidOid;
    (*plansource).rewriteRowSecurity = false;
    (*plansource).dependsOnRLS = false;
    (*plansource).gplan = null_mut();
    (*plansource).is_oneshot = false;
    (*plansource).is_complete = false;
    (*plansource).is_saved = false;
    (*plansource).is_valid = false;
    (*plansource).generation = 0;
    (*plansource).generic_cost = -1.0;
    (*plansource).total_custom_cost = 0.0;
    (*plansource).num_generic_plans = 0;
    (*plansource).num_custom_plans = 0;

    MemoryContextSwitchTo(oldcxt);

    plansource
}

/*
 * CreateCachedPlanForQuery: initially create a plan cache entry for a Query.
 *
 * This is used in the same way as CreateCachedPlan, except that the source
 * query has already been through parse analysis, and the plancache will never
 * try to re-do that step.
 */
pub unsafe fn CreateCachedPlanForQuery(
    analyzed_parse_tree: *mut Query,
    query_string: *const c_char,
    commandTag: CommandTag,
) -> *mut CachedPlanSource {
    let plansource: *mut CachedPlanSource;
    let oldcxt: MemoryContext;

    /* Rather than duplicating CreateCachedPlan, just do this: */
    plansource = CreateCachedPlan(null_mut(), query_string, commandTag);
    oldcxt = MemoryContextSwitchTo((*plansource).context);
    (*plansource).analyzed_parse_tree = copyObject(analyzed_parse_tree);
    MemoryContextSwitchTo(oldcxt);

    plansource
}

/*
 * CreateOneShotCachedPlan: initially create a one-shot plan cache entry.
 *
 * This variant of CreateCachedPlan creates a plan cache entry that is meant
 * to be used only once.  No data copying occurs.
 */
pub unsafe fn CreateOneShotCachedPlan(
    raw_parse_tree: *mut RawStmt,
    query_string: *const c_char,
    commandTag: CommandTag,
) -> *mut CachedPlanSource {
    let plansource: *mut CachedPlanSource;

    Assert!(!query_string.is_null()); /* required as of 8.4 */

    /*
     * Create and fill the CachedPlanSource struct within the caller's memory
     * context.  Most fields are just left empty for the moment.
     */
    plansource = palloc0(core::mem::size_of::<CachedPlanSource>()) as *mut CachedPlanSource;
    (*plansource).magic = CACHEDPLANSOURCE_MAGIC;
    (*plansource).raw_parse_tree = raw_parse_tree;
    (*plansource).analyzed_parse_tree = null_mut();
    (*plansource).query_string = query_string;
    (*plansource).commandTag = commandTag;
    (*plansource).param_types = null_mut();
    (*plansource).num_params = 0;
    (*plansource).parserSetup = None;
    (*plansource).parserSetupArg = null_mut();
    (*plansource).postRewrite = None;
    (*plansource).postRewriteArg = null_mut();
    (*plansource).cursor_options = 0;
    (*plansource).fixed_result = false;
    (*plansource).resultDesc = null_mut();
    (*plansource).context = CurrentMemoryContext;
    (*plansource).query_list = NIL;
    (*plansource).relationOids = NIL;
    (*plansource).invalItems = NIL;
    (*plansource).search_path = null_mut();
    (*plansource).query_context = null_mut();
    (*plansource).rewriteRoleId = InvalidOid;
    (*plansource).rewriteRowSecurity = false;
    (*plansource).dependsOnRLS = false;
    (*plansource).gplan = null_mut();
    (*plansource).is_oneshot = true;
    (*plansource).is_complete = false;
    (*plansource).is_saved = false;
    (*plansource).is_valid = false;
    (*plansource).generation = 0;
    (*plansource).generic_cost = -1.0;
    (*plansource).total_custom_cost = 0.0;
    (*plansource).num_generic_plans = 0;
    (*plansource).num_custom_plans = 0;

    plansource
}

/*
 * CompleteCachedPlan: second step of creating a plan cache entry.
 *
 * See the comment in plancache.c for the full contract.
 */
pub unsafe fn CompleteCachedPlan(
    plansource: *mut CachedPlanSource,
    mut querytree_list: *mut List,
    mut querytree_context: MemoryContext,
    param_types: *mut Oid,
    num_params: c_int,
    parserSetup: ParserSetupHook,
    parserSetupArg: *mut c_void,
    cursor_options: c_int,
    fixed_result: bool,
) {
    let source_context: MemoryContext = (*plansource).context;
    let oldcxt: MemoryContext = CurrentMemoryContext;

    /* Assert caller is doing things in a sane order */
    Assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    Assert!(!(*plansource).is_complete);

    /*
     * If caller supplied a querytree_context, reparent it underneath the
     * CachedPlanSource's context; otherwise, create a suitable context and
     * copy the querytree_list into it.  But no data copying should be done
     * for one-shot plans; for those, assume the passed querytree_list is
     * sufficiently long-lived.
     */
    if (*plansource).is_oneshot {
        querytree_context = CurrentMemoryContext;
    } else if !querytree_context.is_null() {
        MemoryContextSetParent(querytree_context, source_context);
        MemoryContextSwitchTo(querytree_context);
    } else {
        /* Again, it's a good bet the querytree_context can be small */
        querytree_context = AllocSetContextCreate!(
            source_context,
            c"CachedPlanQuery".as_ptr(),
            ALLOCSET_START_SMALL_SIZES
        );
        MemoryContextSwitchTo(querytree_context);
        querytree_list = copyObject(querytree_list);
    }

    (*plansource).query_context = querytree_context;
    (*plansource).query_list = querytree_list;

    if !(*plansource).is_oneshot && StmtPlanRequiresRevalidation(plansource) {
        /*
         * Use the planner machinery to extract dependencies.  Data is saved
         * in query_context.  (We assume that not a lot of extra cruft is
         * created by this call.)  We can skip this for one-shot plans, and
         * plans not needing revalidation have no such dependencies anyway.
         */
        extract_query_dependencies(
            querytree_list as *mut Node,
            &raw mut (*plansource).relationOids,
            &raw mut (*plansource).invalItems,
            &raw mut (*plansource).dependsOnRLS,
        );

        /* Update RLS info as well. */
        (*plansource).rewriteRoleId = GetUserId();
        (*plansource).rewriteRowSecurity = row_security;

        /*
         * Also save the current search_path in the query_context.  (This
         * should not generate much extra cruft either, since almost certainly
         * the path is already valid.)	Again, we don't really need this for
         * one-shot plans; and we *must* skip this for transaction control
         * commands, because this could result in catalog accesses.
         */
        (*plansource).search_path = GetSearchPathMatcher(querytree_context);
    }

    /*
     * Save the final parameter types (or other parameter specification data)
     * into the source_context, as well as our other parameters.  Also save
     * the result tuple descriptor.
     */
    MemoryContextSwitchTo(source_context);

    if num_params > 0 {
        (*plansource).param_types =
            palloc(num_params as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        memcpy(
            (*plansource).param_types as *mut c_void,
            param_types as *const c_void,
            num_params as usize * core::mem::size_of::<Oid>(),
        );
    } else {
        (*plansource).param_types = null_mut();
    }
    (*plansource).num_params = num_params;
    (*plansource).parserSetup = parserSetup;
    (*plansource).parserSetupArg = parserSetupArg;
    (*plansource).cursor_options = cursor_options;
    (*plansource).fixed_result = fixed_result;
    (*plansource).resultDesc = PlanCacheComputeResultDesc(querytree_list);

    MemoryContextSwitchTo(oldcxt);

    (*plansource).is_complete = true;
    (*plansource).is_valid = true;
}

/*
 * SetPostRewriteHook: set a hook to modify post-rewrite query trees
 */
pub unsafe fn SetPostRewriteHook(
    plansource: *mut CachedPlanSource,
    postRewrite: PostRewriteHook,
    postRewriteArg: *mut c_void,
) {
    Assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    (*plansource).postRewrite = postRewrite;
    (*plansource).postRewriteArg = postRewriteArg;
}

/*
 * SaveCachedPlan: save a cached plan permanently
 */
pub unsafe fn SaveCachedPlan(plansource: *mut CachedPlanSource) {
    /* Assert caller is doing things in a sane order */
    Assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    Assert!((*plansource).is_complete);
    Assert!(!(*plansource).is_saved);

    /* This seems worth a real test, though */
    if (*plansource).is_oneshot {
        elog!(ERROR, "cannot save one-shot cached plan");
    }

    /*
     * In typical use, this function would be called before generating any
     * plans from the CachedPlanSource.  If there is a generic plan, moving it
     * into CacheMemoryContext would be pretty risky since it's unclear
     * whether the caller has taken suitable care with making references
     * long-lived.  Best thing to do seems to be to discard the plan.
     */
    ReleaseGenericPlan(plansource);

    /*
     * Reparent the source memory context under CacheMemoryContext so that it
     * will live indefinitely.  The query_context follows along since it's
     * already a child of the other one.
     */
    MemoryContextSetParent((*plansource).context, CacheMemoryContext);

    /*
     * Add the entry to the global list of cached plans.
     */
    dlist_push_tail(&raw mut saved_plan_list, &raw mut (*plansource).node);

    (*plansource).is_saved = true;
}

/*
 * DropCachedPlan: destroy a cached plan.
 */
pub unsafe fn DropCachedPlan(plansource: *mut CachedPlanSource) {
    Assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);

    /* If it's been saved, remove it from the list */
    if (*plansource).is_saved {
        dlist_delete(&raw mut (*plansource).node);
        (*plansource).is_saved = false;
    }

    /* Decrement generic CachedPlan's refcount and drop if no longer needed */
    ReleaseGenericPlan(plansource);

    /* Mark it no longer valid */
    (*plansource).magic = 0;

    /*
     * Remove the CachedPlanSource and all subsidiary data (including the
     * query_context if any).  But if it's a one-shot we can't free anything.
     */
    if !(*plansource).is_oneshot {
        MemoryContextDelete((*plansource).context);
    }
}

/*
 * ReleaseGenericPlan: release a CachedPlanSource's generic plan, if any.
 */
unsafe fn ReleaseGenericPlan(plansource: *mut CachedPlanSource) {
    /* Be paranoid about the possibility that ReleaseCachedPlan fails */
    if !(*plansource).gplan.is_null() {
        let plan: *mut CachedPlan = (*plansource).gplan;

        Assert!((*plan).magic == CACHEDPLAN_MAGIC);
        (*plansource).gplan = null_mut();
        ReleaseCachedPlan(plan, null_mut());
    }
}

/*
 * We must skip "overhead" operations that involve database access when the
 * cached plan's subject statement is a transaction control command or one
 * that requires a snapshot not to be set yet (such as SET or LOCK).  More
 * generally, statements that do not require parse analysis/rewrite/plan
 * activity never need to be revalidated, so we can treat them all like that.
 * For the convenience of postgres.c, treat empty statements that way too.
 */
unsafe fn StmtPlanRequiresRevalidation(plansource: *mut CachedPlanSource) -> bool {
    if !(*plansource).raw_parse_tree.is_null() {
        return stmt_requires_parse_analysis((*plansource).raw_parse_tree);
    } else if !(*plansource).analyzed_parse_tree.is_null() {
        return query_requires_rewrite_plan((*plansource).analyzed_parse_tree);
    }
    /* empty query never needs revalidation */
    false
}

/*
 * Determine if creating a plan for this CachedPlanSource requires a snapshot.
 * In fact this function matches StmtPlanRequiresRevalidation(), but we want
 * to preserve the distinction between stmt_requires_parse_analysis() and
 * analyze_requires_snapshot().
 */
unsafe fn BuildingPlanRequiresSnapshot(plansource: *mut CachedPlanSource) -> bool {
    if !(*plansource).raw_parse_tree.is_null() {
        return analyze_requires_snapshot((*plansource).raw_parse_tree);
    } else if !(*plansource).analyzed_parse_tree.is_null() {
        return query_requires_rewrite_plan((*plansource).analyzed_parse_tree);
    }
    /* empty query never needs a snapshot */
    false
}

/*
 * RevalidateCachedQuery: ensure validity of analyzed-and-rewritten query tree.
 *
 * See the comment in plancache.c for the full contract.
 */
unsafe fn RevalidateCachedQuery(
    plansource: *mut CachedPlanSource,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    let mut snapshot_set: bool;
    let tlist: *mut List; /* transient query-tree list */
    let qlist: *mut List; /* permanent query-tree list */
    let mut resultDesc: TupleDesc;
    let querytree_context: MemoryContext;
    let mut oldcxt: MemoryContext;

    /*
     * For one-shot plans, we do not support revalidation checking; it's
     * assumed the query is parsed, planned, and executed in one transaction,
     * so that no lock re-acquisition is necessary.  Also, if the statement
     * type can't require revalidation, we needn't do anything (and we mustn't
     * risk catalog accesses when handling, eg, transaction control commands).
     */
    if (*plansource).is_oneshot || !StmtPlanRequiresRevalidation(plansource) {
        Assert!((*plansource).is_valid);
        return NIL;
    }

    /*
     * If the query is currently valid, we should have a saved search_path ---
     * check to see if that matches the current environment.  If not, we want
     * to force replan.  (We could almost ignore this consideration when
     * working from an analyzed parse tree; but there are scenarios where
     * planning can have search_path-dependent results, for example if it
     * inlines an old-style SQL function.)
     */
    if (*plansource).is_valid {
        Assert!(!(*plansource).search_path.is_null());
        if !SearchPathMatchesCurrentEnvironment((*plansource).search_path) {
            /* Invalidate the querytree and generic plan */
            (*plansource).is_valid = false;
            if !(*plansource).gplan.is_null() {
                (*(*plansource).gplan).is_valid = false;
            }
        }
    }

    /*
     * If the query rewrite phase had a possible RLS dependency, we must redo
     * it if either the role or the row_security setting has changed.
     */
    if (*plansource).is_valid
        && (*plansource).dependsOnRLS
        && ((*plansource).rewriteRoleId != GetUserId()
            || (*plansource).rewriteRowSecurity != row_security)
    {
        (*plansource).is_valid = false;
    }

    /*
     * If the query is currently valid, acquire locks on the referenced
     * objects; then check again.  We need to do it this way to cover the race
     * condition that an invalidation message arrives before we get the locks.
     */
    if (*plansource).is_valid {
        AcquirePlannerLocks((*plansource).query_list, true);

        /*
         * By now, if any invalidation has happened, the inval callback
         * functions will have marked the query invalid.
         */
        if (*plansource).is_valid {
            /* Successfully revalidated and locked the query. */
            return NIL;
        }

        /* Oops, the race case happened.  Release useless locks. */
        AcquirePlannerLocks((*plansource).query_list, false);
    }

    /*
     * Discard the no-longer-useful rewritten query tree.  (Note: we don't
     * want to do this any earlier, else we'd not have been able to release
     * locks correctly in the race condition case.)
     */
    (*plansource).is_valid = false;
    (*plansource).query_list = NIL;
    (*plansource).relationOids = NIL;
    (*plansource).invalItems = NIL;
    (*plansource).search_path = null_mut();

    /*
     * Free the query_context.  We don't really expect MemoryContextDelete to
     * fail, but just in case, make sure the CachedPlanSource is left in a
     * reasonably sane state.  (The generic plan won't get unlinked yet, but
     * that's acceptable.)
     */
    if !(*plansource).query_context.is_null() {
        let qcxt: MemoryContext = (*plansource).query_context;

        (*plansource).query_context = null_mut();
        MemoryContextDelete(qcxt);
    }

    /* Drop the generic plan reference if any */
    ReleaseGenericPlan(plansource);

    /*
     * Now re-do parse analysis and rewrite.  This not incidentally acquires
     * the locks we need to do planning safely.
     */
    Assert!((*plansource).is_complete);

    /*
     * If a snapshot is already set (the normal case), we can just use that
     * for parsing/planning.  But if it isn't, install one.  Note: no point in
     * checking whether parse analysis requires a snapshot; utility commands
     * don't have invalidatable plans, so we'd not get here for such a
     * command.
     */
    snapshot_set = false;
    if !ActiveSnapshotSet() {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * Run parse analysis (if needed) and rule rewriting.
     */
    if !(*plansource).raw_parse_tree.is_null() {
        /* Source is raw parse tree */
        let rawtree: *mut RawStmt;

        /*
         * The parser tends to scribble on its input, so we must copy the raw
         * parse tree to prevent corruption of the cache.
         */
        rawtree = copyObject((*plansource).raw_parse_tree);
        if (*plansource).parserSetup.is_some() {
            tlist = pg_analyze_and_rewrite_withcb(
                rawtree,
                (*plansource).query_string,
                (*plansource).parserSetup,
                (*plansource).parserSetupArg,
                queryEnv,
            );
        } else {
            tlist = pg_analyze_and_rewrite_fixedparams(
                rawtree,
                (*plansource).query_string,
                (*plansource).param_types,
                (*plansource).num_params,
                queryEnv,
            );
        }
    } else if !(*plansource).analyzed_parse_tree.is_null() {
        /* Source is pre-analyzed query, so we only need to rewrite */
        let analyzed_tree: *mut Query;

        /* The rewriter scribbles on its input, too, so copy */
        analyzed_tree = copyObject((*plansource).analyzed_parse_tree);
        /* Acquire locks needed before rewriting ... */
        AcquireRewriteLocks(analyzed_tree, true, false);
        /* ... and do it */
        tlist = pg_rewrite_query(analyzed_tree);
    } else {
        /* Empty query, nothing to do */
        tlist = NIL;
    }

    /* Apply post-rewrite callback if there is one */
    if let Some(postRewrite) = (*plansource).postRewrite {
        postRewrite(tlist, (*plansource).postRewriteArg);
    }

    /* Release snapshot if we got one */
    if snapshot_set {
        PopActiveSnapshot();
    }

    /*
     * Check or update the result tupdesc.
     *
     * We assume the parameter types didn't change from the first time, so no
     * need to update that.
     */
    resultDesc = PlanCacheComputeResultDesc(tlist);
    if resultDesc.is_null() && (*plansource).resultDesc.is_null() {
        /* OK, doesn't return tuples */
    } else if resultDesc.is_null()
        || (*plansource).resultDesc.is_null()
        || !equalRowTypes(resultDesc, (*plansource).resultDesc)
    {
        /* can we give a better error message? */
        if (*plansource).fixed_result {
            ereport!(
                ERROR,
                errmsg!("cached plan must not change result type")
            );
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        }
        oldcxt = MemoryContextSwitchTo((*plansource).context);
        if !resultDesc.is_null() {
            resultDesc = CreateTupleDescCopy(resultDesc);
        }
        if !(*plansource).resultDesc.is_null() {
            FreeTupleDesc((*plansource).resultDesc);
        }
        (*plansource).resultDesc = resultDesc;
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * Allocate new query_context and copy the completed querytree into it.
     * It's transient until we complete the copying and dependency extraction.
     */
    querytree_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"CachedPlanQuery".as_ptr(),
        ALLOCSET_START_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(querytree_context);

    qlist = copyObject(tlist);

    /*
     * Use the planner machinery to extract dependencies.  Data is saved in
     * query_context.  (We assume that not a lot of extra cruft is created by
     * this call.)
     */
    extract_query_dependencies(
        qlist as *mut Node,
        &raw mut (*plansource).relationOids,
        &raw mut (*plansource).invalItems,
        &raw mut (*plansource).dependsOnRLS,
    );

    /* Update RLS info as well. */
    (*plansource).rewriteRoleId = GetUserId();
    (*plansource).rewriteRowSecurity = row_security;

    /*
     * Also save the current search_path in the query_context.  (This should
     * not generate much extra cruft either, since almost certainly the path
     * is already valid.)
     */
    (*plansource).search_path = GetSearchPathMatcher(querytree_context);

    MemoryContextSwitchTo(oldcxt);

    /* Now reparent the finished query_context and save the links */
    MemoryContextSetParent(querytree_context, (*plansource).context);

    (*plansource).query_context = querytree_context;
    (*plansource).query_list = qlist;

    /*
     * Note: we do not reset generic_cost or total_custom_cost, although we
     * could choose to do so.  If the DDL or statistics change that prompted
     * the invalidation meant a significant change in the cost estimates, it
     * would be better to reset those variables and start fresh; but often it
     * doesn't, and we're better retaining our hard-won knowledge about the
     * relative costs.
     */

    (*plansource).is_valid = true;

    /* Return transient copy of querytrees for possible use in planning */
    tlist
}

/*
 * CheckCachedPlan: see if the CachedPlanSource's generic plan is valid.
 *
 * Caller must have already called RevalidateCachedQuery to verify that the
 * querytree is up to date.
 *
 * On a "true" return, we have acquired the locks needed to run the plan.
 * (We must do this for the "true" result to be race-condition-free.)
 */
unsafe fn CheckCachedPlan(plansource: *mut CachedPlanSource) -> bool {
    let plan: *mut CachedPlan = (*plansource).gplan;

    /* Assert that caller checked the querytree */
    Assert!((*plansource).is_valid);

    /* If there's no generic plan, just say "false" */
    if plan.is_null() {
        return false;
    }

    Assert!((*plan).magic == CACHEDPLAN_MAGIC);
    /* Generic plans are never one-shot */
    Assert!(!(*plan).is_oneshot);

    /*
     * If plan isn't valid for current role, we can't use it.
     */
    if (*plan).is_valid && (*plan).dependsOnRole && (*plan).planRoleId != GetUserId() {
        (*plan).is_valid = false;
    }

    /*
     * If it appears valid, acquire locks and recheck; this is much the same
     * logic as in RevalidateCachedQuery, but for a plan.
     */
    if (*plan).is_valid {
        /*
         * Plan must have positive refcount because it is referenced by
         * plansource; so no need to fear it disappears under us here.
         */
        Assert!((*plan).refcount > 0);

        AcquireExecutorLocks((*plan).stmt_list, true);

        /*
         * If plan was transient, check to see if TransactionXmin has
         * advanced, and if so invalidate it.
         */
        if (*plan).is_valid
            && TransactionIdIsValid((*plan).saved_xmin)
            && !TransactionIdEquals((*plan).saved_xmin, TransactionXmin)
        {
            (*plan).is_valid = false;
        }

        /*
         * By now, if any invalidation has happened, the inval callback
         * functions will have marked the plan invalid.
         */
        if (*plan).is_valid {
            /* Successfully revalidated and locked the query. */
            return true;
        }

        /* Oops, the race case happened.  Release useless locks. */
        AcquireExecutorLocks((*plan).stmt_list, false);
    }

    /*
     * Plan has been invalidated, so unlink it from the parent and release it.
     */
    ReleaseGenericPlan(plansource);

    false
}

/*
 * BuildCachedPlan: construct a new CachedPlan from a CachedPlanSource.
 *
 * See the comment in plancache.c for the full contract.
 */
unsafe fn BuildCachedPlan(
    plansource: *mut CachedPlanSource,
    mut qlist: *mut List,
    boundParams: ParamListInfo,
    queryEnv: *mut QueryEnvironment,
) -> *mut CachedPlan {
    let plan: *mut CachedPlan;
    let mut plist: *mut List;
    let mut snapshot_set: bool;
    let mut is_transient: bool;
    let plan_context: MemoryContext;
    let oldcxt: MemoryContext = CurrentMemoryContext;
    let lc: *mut crate::nodes::pg_list::ListCell;

    /*
     * Normally the querytree should be valid already, but if it's not,
     * rebuild it.
     *
     * NOTE: GetCachedPlan should have called RevalidateCachedQuery first, so
     * we ought to be holding sufficient locks to prevent any invalidation.
     * However, if we're building a custom plan after having built and
     * rejected a generic plan, it's possible to reach here with is_valid
     * false due to an invalidation while making the generic plan.  In theory
     * the invalidation must be a false positive, perhaps a consequence of an
     * sinval reset event or the debug_discard_caches code.  But for safety,
     * let's treat it as real and redo the RevalidateCachedQuery call.
     */
    if !(*plansource).is_valid {
        qlist = RevalidateCachedQuery(plansource, queryEnv);
    }

    /*
     * If we don't already have a copy of the querytree list that can be
     * scribbled on by the planner, make one.  For a one-shot plan, we assume
     * it's okay to scribble on the original query_list.
     */
    if qlist == NIL {
        if !(*plansource).is_oneshot {
            qlist = copyObject((*plansource).query_list);
        } else {
            qlist = (*plansource).query_list;
        }
    }

    /*
     * If a snapshot is already set (the normal case), we can just use that
     * for planning.  But if it isn't, and we need one, install one.
     */
    snapshot_set = false;
    if !ActiveSnapshotSet() && BuildingPlanRequiresSnapshot(plansource) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * Generate the plan.
     */
    plist = pg_plan_queries(
        qlist,
        (*plansource).query_string,
        (*plansource).cursor_options,
        boundParams,
    );

    /* Release snapshot if we got one */
    if snapshot_set {
        PopActiveSnapshot();
    }

    /*
     * Normally we make a dedicated memory context for the CachedPlan and its
     * subsidiary data.  (It's probably not going to be large, but just in
     * case, allow it to grow large.  It's transient for the moment.)  But for
     * a one-shot plan, we just leave it in the caller's memory context.
     */
    if !(*plansource).is_oneshot {
        plan_context = AllocSetContextCreate!(
            CurrentMemoryContext,
            c"CachedPlan".as_ptr(),
            ALLOCSET_START_SMALL_SIZES
        );
        MemoryContextCopyAndSetIdentifier(plan_context, (*plansource).query_string);

        /*
         * Copy plan into the new context.
         */
        MemoryContextSwitchTo(plan_context);

        plist = copyObject(plist);
    } else {
        plan_context = CurrentMemoryContext;
    }

    /*
     * Create and fill the CachedPlan struct within the new context.
     */
    plan = palloc(core::mem::size_of::<CachedPlan>()) as *mut CachedPlan;
    (*plan).magic = CACHEDPLAN_MAGIC;
    (*plan).stmt_list = plist;

    /*
     * CachedPlan is dependent on role either if RLS affected the rewrite
     * phase or if a role dependency was injected during planning.  And it's
     * transient if any plan is marked so.
     */
    (*plan).planRoleId = GetUserId();
    (*plan).dependsOnRole = (*plansource).dependsOnRLS;
    is_transient = false;
    let _ = lc;
    foreach!(lc, plist, {
        let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc));

        if (*plannedstmt).commandType == CMD_UTILITY {
            continue; /* Ignore utility statements */
        }

        if (*plannedstmt).transientPlan {
            is_transient = true;
        }
        if (*plannedstmt).dependsOnRole {
            (*plan).dependsOnRole = true;
        }
    });
    if is_transient {
        Assert!(TransactionIdIsNormal(TransactionXmin));
        (*plan).saved_xmin = TransactionXmin;
    } else {
        (*plan).saved_xmin = InvalidTransactionId;
    }
    (*plan).refcount = 0;
    (*plan).context = plan_context;
    (*plan).is_oneshot = (*plansource).is_oneshot;
    (*plan).is_saved = false;
    (*plan).is_valid = true;

    /* assign generation number to new plan */
    (*plansource).generation += 1;
    (*plan).generation = (*plansource).generation;

    MemoryContextSwitchTo(oldcxt);

    plan
}

/*
 * choose_custom_plan: choose whether to use custom or generic plan
 *
 * This defines the policy followed by GetCachedPlan.
 */
unsafe fn choose_custom_plan(plansource: *mut CachedPlanSource, boundParams: ParamListInfo) -> bool {
    let avg_custom_cost: f64;

    /* One-shot plans will always be considered custom */
    if (*plansource).is_oneshot {
        return true;
    }

    /* Otherwise, never any point in a custom plan if there's no parameters */
    if boundParams.is_null() {
        return false;
    }
    /* ... nor when planning would be a no-op */
    if !StmtPlanRequiresRevalidation(plansource) {
        return false;
    }

    /* Let settings force the decision */
    if plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN {
        return false;
    }
    if plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN {
        return true;
    }

    /* See if caller wants to force the decision */
    if (*plansource).cursor_options & CURSOR_OPT_GENERIC_PLAN != 0 {
        return false;
    }
    if (*plansource).cursor_options & CURSOR_OPT_CUSTOM_PLAN != 0 {
        return true;
    }

    /* Generate custom plans until we have done at least 5 (arbitrary) */
    if (*plansource).num_custom_plans < 5 {
        return true;
    }

    avg_custom_cost = (*plansource).total_custom_cost / (*plansource).num_custom_plans as f64;

    /*
     * Prefer generic plan if it's less expensive than the average custom
     * plan.  (Because we include a charge for cost of planning in the
     * custom-plan costs, this means the generic plan only has to be less
     * expensive than the execution cost plus replan cost of the custom
     * plans.)
     *
     * Note that if generic_cost is -1 (indicating we've not yet determined
     * the generic plan cost), we'll always prefer generic at this point.
     */
    if (*plansource).generic_cost < avg_custom_cost {
        return false;
    }

    true
}

/*
 * cached_plan_cost: calculate estimated cost of a plan
 *
 * If include_planner is true, also include the estimated cost of constructing
 * the plan.  (We must factor that into the cost of using a custom plan, but
 * we don't count it for a generic plan.)
 */
unsafe fn cached_plan_cost(plan: *mut CachedPlan, include_planner: bool) -> f64 {
    let mut result: f64 = 0.0;
    let lc: *mut crate::nodes::pg_list::ListCell;

    let _ = lc;
    foreach!(lc, (*plan).stmt_list, {
        let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc));

        if (*plannedstmt).commandType == CMD_UTILITY {
            continue; /* Ignore utility statements */
        }

        result += (*(*plannedstmt).planTree).total_cost;

        if include_planner {
            /*
             * Currently we use a very crude estimate of planning effort based
             * on the number of relations in the finished plan's rangetable.
             * Join planning effort actually scales much worse than linearly
             * in the number of relations --- but only until the join collapse
             * limits kick in.  Also, while inheritance child relations surely
             * add to planning effort, they don't make the join situation
             * worse.  So the actual shape of the planning cost curve versus
             * number of relations isn't all that obvious.  It will take
             * considerable work to arrive at a less crude estimate, and for
             * now it's not clear that's worth doing.
             *
             * The other big difficulty here is that we don't have any very
             * good model of how planning cost compares to execution costs.
             * The current multiplier of 1000 * cpu_operator_cost is probably
             * on the low side, but we'll try this for awhile before making a
             * more aggressive correction.
             *
             * If we ever do write a more complicated estimator, it should
             * probably live in src/backend/optimizer/ not here.
             */
            let nrelations: c_int = list_length((*plannedstmt).rtable);

            result += 1000.0 * cpu_operator_cost * (nrelations + 1) as f64;
        }
    });

    result
}

/*
 * GetCachedPlan: get a cached plan from a CachedPlanSource.
 *
 * This function hides the logic that decides whether to use a generic
 * plan or a custom plan for the given parameters: the caller does not know
 * which it will get.
 *
 * On return, the plan is valid and we have sufficient locks to begin
 * execution.
 *
 * On return, the refcount of the plan has been incremented; a later
 * ReleaseCachedPlan() call is expected.  If "owner" is not NULL then
 * the refcount has been reported to that ResourceOwner (note that this
 * is only supported for "saved" CachedPlanSources).
 *
 * Note: if any replanning activity is required, the caller's memory context
 * is used for that work.
 */
pub unsafe fn GetCachedPlan(
    plansource: *mut CachedPlanSource,
    boundParams: ParamListInfo,
    owner: ResourceOwner,
    queryEnv: *mut QueryEnvironment,
) -> *mut CachedPlan {
    let mut plan: *mut CachedPlan = null_mut();
    let mut qlist: *mut List;
    let mut customplan: bool;

    /* Assert caller is doing things in a sane order */
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    debug_assert!((*plansource).is_complete);
    /* This seems worth a real test, though */
    if !owner.is_null() && !(*plansource).is_saved {
        elog!(ERROR, "cannot apply ResourceOwner to non-saved cached plan");
    }

    /* Make sure the querytree list is valid and we have parse-time locks */
    qlist = RevalidateCachedQuery(plansource, queryEnv);

    /* Decide whether to use a custom plan */
    customplan = choose_custom_plan(plansource, boundParams);

    if !customplan {
        if CheckCachedPlan(plansource) {
            /* We want a generic plan, and we already have a valid one */
            plan = (*plansource).gplan;
            debug_assert!((*plan).magic == CACHEDPLAN_MAGIC);
        } else {
            /* Build a new generic plan */
            plan = BuildCachedPlan(plansource, qlist, null_mut(), queryEnv);
            /* Just make real sure plansource->gplan is clear */
            ReleaseGenericPlan(plansource);
            /* Link the new generic plan into the plansource */
            (*plansource).gplan = plan;
            (*plan).refcount += 1;
            /* Immediately reparent into appropriate context */
            if (*plansource).is_saved {
                /* saved plans all live under CacheMemoryContext */
                MemoryContextSetParent((*plan).context, CacheMemoryContext);
                (*plan).is_saved = true;
            } else {
                /* otherwise, it should be a sibling of the plansource */
                MemoryContextSetParent(
                    (*plan).context,
                    MemoryContextGetParent((*plansource).context),
                );
            }
            /* Update generic_cost whenever we make a new generic plan */
            (*plansource).generic_cost = cached_plan_cost(plan, false);

            /*
             * If, based on the now-known value of generic_cost, we'd not have
             * chosen to use a generic plan, then forget it and make a custom
             * plan.  This is a bit of a wart but is necessary to avoid a
             * glitch in behavior when the custom plans are consistently big
             * winners; at some point we'll experiment with a generic plan and
             * find it's a loser, but we don't want to actually execute that
             * plan.
             */
            customplan = choose_custom_plan(plansource, boundParams);

            /*
             * If we choose to plan again, we need to re-copy the query_list,
             * since the planner probably scribbled on it.  We can force
             * BuildCachedPlan to do that by passing NIL.
             */
            qlist = NIL;
        }
    }

    if customplan {
        /* Build a custom plan */
        plan = BuildCachedPlan(plansource, qlist, boundParams, queryEnv);
        /* Accumulate total costs of custom plans */
        (*plansource).total_custom_cost += cached_plan_cost(plan, true);

        (*plansource).num_custom_plans += 1;
    } else {
        (*plansource).num_generic_plans += 1;
    }

    debug_assert!(!plan.is_null());

    /* Flag the plan as in use by caller */
    if !owner.is_null() {
        ResourceOwnerEnlarge(owner);
    }
    (*plan).refcount += 1;
    if !owner.is_null() {
        ResourceOwnerRememberPlanCacheRef(owner, plan);
    }

    /*
     * Saved plans should be under CacheMemoryContext so they will not go away
     * until their reference count goes to zero.  In the generic-plan cases we
     * already took care of that, but for a custom plan, do it as soon as we
     * have created a reference-counted link.
     */
    if customplan && (*plansource).is_saved {
        MemoryContextSetParent((*plan).context, CacheMemoryContext);
        (*plan).is_saved = true;
    }

    plan
}

/*
 * ReleaseCachedPlan: release active use of a cached plan.
 *
 * This decrements the reference count, and frees the plan if the count
 * has thereby gone to zero.  If "owner" is not NULL, it is assumed that
 * the reference count is managed by that ResourceOwner.
 *
 * Note: owner == NULL is used for releasing references that are in
 * persistent data structures, such as the parent CachedPlanSource or a
 * Portal.  Transient references should be protected by a resource owner.
 */
#[no_mangle]
pub unsafe fn ReleaseCachedPlan(plan: *mut CachedPlan, owner: ResourceOwner) {
    debug_assert!((*plan).magic == CACHEDPLAN_MAGIC);
    if !owner.is_null() {
        debug_assert!((*plan).is_saved);
        ResourceOwnerForgetPlanCacheRef(owner, plan);
    }
    debug_assert!((*plan).refcount > 0);
    (*plan).refcount -= 1;
    if (*plan).refcount == 0 {
        /* Mark it no longer valid */
        (*plan).magic = 0;

        /* One-shot plans do not own their context, so we can't free them */
        if !(*plan).is_oneshot {
            MemoryContextDelete((*plan).context);
        }
    }
}

/*
 * CachedPlanAllowsSimpleValidityCheck: can we use CachedPlanIsSimplyValid?
 *
 * This function, together with CachedPlanIsSimplyValid, provides a fast path
 * for revalidating "simple" generic plans.  The core requirement to be simple
 * is that the plan must not require taking any locks, which translates to
 * not touching any tables; this happens to match up well with an important
 * use-case in PL/pgSQL.  This function tests whether that's true, along
 * with checking some other corner cases that we'd rather not bother with
 * handling in the fast path.  (Note that it's still possible for such a plan
 * to be invalidated, for example due to a change in a function that was
 * inlined into the plan.)
 *
 * If the plan is simply valid, and "owner" is not NULL, record a refcount on
 * the plan in that resowner before returning.  It is caller's responsibility
 * to be sure that a refcount is held on any plan that's being actively used.
 *
 * This must only be called on known-valid generic plans (eg, ones just
 * returned by GetCachedPlan).  If it returns true, the caller may re-use
 * the cached plan as long as CachedPlanIsSimplyValid returns true; that
 * check is much cheaper than the full revalidation done by GetCachedPlan.
 * Nonetheless, no required checks are omitted.
 */
#[no_mangle]
pub unsafe fn CachedPlanAllowsSimpleValidityCheck(
    plansource: *mut CachedPlanSource,
    plan: *mut CachedPlan,
    owner: ResourceOwner,
) -> bool {
    let lc: *mut crate::nodes::pg_list::ListCell;

    /*
     * Sanity-check that the caller gave us a validated generic plan.  Notice
     * that we *don't* assert plansource->is_valid as you might expect; that's
     * because it's possible that that's already false when GetCachedPlan
     * returns, e.g. because ResetPlanCache happened partway through.  We
     * should accept the plan as long as plan->is_valid is true, and expect to
     * replan after the next CachedPlanIsSimplyValid call.
     */
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    debug_assert!((*plan).magic == CACHEDPLAN_MAGIC);
    debug_assert!((*plan).is_valid);
    debug_assert!(plan == (*plansource).gplan);
    debug_assert!(!(*plansource).search_path.is_null());
    debug_assert!(SearchPathMatchesCurrentEnvironment((*plansource).search_path));

    /* We don't support oneshot plans here. */
    if (*plansource).is_oneshot {
        return false;
    }
    debug_assert!(!(*plan).is_oneshot);

    /*
     * If the plan is dependent on RLS considerations, or it's transient,
     * reject.  These things probably can't ever happen for table-free
     * queries, but for safety's sake let's check.
     */
    if (*plansource).dependsOnRLS {
        return false;
    }
    if (*plan).dependsOnRole {
        return false;
    }
    if TransactionIdIsValid((*plan).saved_xmin) {
        return false;
    }

    /*
     * Reject if AcquirePlannerLocks would have anything to do.  This is
     * simplistic, but there's no need to inquire any more carefully; indeed,
     * for current callers it shouldn't even be possible to hit any of these
     * checks.
     */
    let _ = lc;
    foreach!(lc, (*plansource).query_list, {
        let query = lfirst_node!(Query, T_Query, current_cell!(lc));

        if (*query).commandType == CMD_UTILITY {
            return false;
        }
        if !(*query).rtable.is_null() || !(*query).cteList.is_null() || (*query).hasSubLinks {
            return false;
        }
    });

    /*
     * Reject if AcquireExecutorLocks would have anything to do.  This is
     * probably unnecessary given the previous check, but let's be safe.
     */
    foreach!(lc, (*plan).stmt_list, {
        let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc));
        let lc2: *mut crate::nodes::pg_list::ListCell;

        if (*plannedstmt).commandType == CMD_UTILITY {
            return false;
        }

        /*
         * We have to grovel through the rtable because it's likely to contain
         * an RTE_RESULT relation, rather than being totally empty.
         */
        let _ = lc2;
        foreach!(lc2, (*plannedstmt).rtable, {
            let rte = lfirst(current_cell!(lc2)) as *mut RangeTblEntry;

            if (*rte).rtekind == RTE_RELATION {
                return false;
            }
        });
    });

    /*
     * Okay, it's simple.  Note that what we've primarily established here is
     * that no locks need be taken before checking the plan's is_valid flag.
     */

    /* Bump refcount if requested. */
    if !owner.is_null() {
        ResourceOwnerEnlarge(owner);
        (*plan).refcount += 1;
        ResourceOwnerRememberPlanCacheRef(owner, plan);
    }

    true
}

/*
 * CachedPlanIsSimplyValid: quick check for plan still being valid
 *
 * This function must not be used unless CachedPlanAllowsSimpleValidityCheck
 * previously said it was OK.
 *
 * If the plan is valid, and "owner" is not NULL, record a refcount on
 * the plan in that resowner before returning.  It is caller's responsibility
 * to be sure that a refcount is held on any plan that's being actively used.
 *
 * The code here is unconditionally safe as long as the only use of this
 * CachedPlanSource is in connection with the particular CachedPlan pointer
 * that's passed in.  If the plansource were being used for other purposes,
 * it's possible that its generic plan could be invalidated and regenerated
 * while the current caller wasn't looking, and then there could be a chance
 * collision of address between this caller's now-stale plan pointer and the
 * actual address of the new generic plan.  For current uses, that scenario
 * can't happen; but with a plansource shared across multiple uses, it'd be
 * advisable to also save plan->generation and verify that that still matches.
 */
#[no_mangle]
pub unsafe fn CachedPlanIsSimplyValid(
    plansource: *mut CachedPlanSource,
    plan: *mut CachedPlan,
    owner: ResourceOwner,
) -> bool {
    /*
     * Careful here: since the caller doesn't necessarily hold a refcount on
     * the plan to start with, it's possible that "plan" is a dangling
     * pointer.  Don't dereference it until we've verified that it still
     * matches the plansource's gplan (which is either valid or NULL).
     */
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);

    /*
     * Has cache invalidation fired on this plan?  We can check this right
     * away since there are no locks that we'd need to acquire first.  Note
     * that here we *do* check plansource->is_valid, so as to force plan
     * rebuild if that's become false.
     */
    if !(*plansource).is_valid
        || plan.is_null()
        || plan != (*plansource).gplan
        || !(*plan).is_valid
    {
        return false;
    }

    debug_assert!((*plan).magic == CACHEDPLAN_MAGIC);

    /* Is the search_path still the same as when we made it? */
    debug_assert!(!(*plansource).search_path.is_null());
    if !SearchPathMatchesCurrentEnvironment((*plansource).search_path) {
        return false;
    }

    /* It's still good.  Bump refcount if requested. */
    if !owner.is_null() {
        ResourceOwnerEnlarge(owner);
        (*plan).refcount += 1;
        ResourceOwnerRememberPlanCacheRef(owner, plan);
    }

    true
}

/*
 * CachedPlanSetParentContext: move a CachedPlanSource to a new memory context
 *
 * This can only be applied to unsaved plans; once saved, a plan always
 * lives underneath CacheMemoryContext.
 */
pub unsafe fn CachedPlanSetParentContext(
    plansource: *mut CachedPlanSource,
    newcontext: MemoryContext,
) {
    /* Assert caller is doing things in a sane order */
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    debug_assert!((*plansource).is_complete);

    /* These seem worth real tests, though */
    if (*plansource).is_saved {
        elog!(ERROR, "cannot move a saved cached plan to another context");
    }
    if (*plansource).is_oneshot {
        elog!(ERROR, "cannot move a one-shot cached plan to another context");
    }

    /* OK, let the caller keep the plan where he wishes */
    MemoryContextSetParent((*plansource).context, newcontext);

    /*
     * The query_context needs no special handling, since it's a child of
     * plansource->context.  But if there's a generic plan, it should be
     * maintained as a sibling of plansource->context.
     */
    if !(*plansource).gplan.is_null() {
        debug_assert!((*(*plansource).gplan).magic == CACHEDPLAN_MAGIC);
        MemoryContextSetParent((*(*plansource).gplan).context, newcontext);
    }
}

/*
 * CopyCachedPlan: make a copy of a CachedPlanSource
 *
 * This is a convenience routine that does the equivalent of
 * CreateCachedPlan + CompleteCachedPlan, using the data stored in the
 * input CachedPlanSource.  The result is therefore "unsaved" (regardless
 * of the state of the source), and we don't copy any generic plan either.
 * The result will be currently valid, or not, the same as the source.
 */
pub unsafe fn CopyCachedPlan(plansource: *mut CachedPlanSource) -> *mut CachedPlanSource {
    let newsource: *mut CachedPlanSource;
    let source_context: MemoryContext;
    let querytree_context: MemoryContext;
    let oldcxt: MemoryContext;

    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    debug_assert!((*plansource).is_complete);

    /*
     * One-shot plans can't be copied, because we haven't taken care that
     * parsing/planning didn't scribble on the raw parse tree or querytrees.
     */
    if (*plansource).is_oneshot {
        elog!(ERROR, "cannot copy a one-shot cached plan");
    }

    source_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"CachedPlanSource".as_ptr(),
        ALLOCSET_START_SMALL_SIZES
    );

    oldcxt = MemoryContextSwitchTo(source_context);

    newsource = palloc0(core::mem::size_of::<CachedPlanSource>()) as *mut CachedPlanSource;
    (*newsource).magic = CACHEDPLANSOURCE_MAGIC;
    (*newsource).raw_parse_tree = copyObject((*plansource).raw_parse_tree);
    (*newsource).analyzed_parse_tree = copyObject((*plansource).analyzed_parse_tree);
    (*newsource).query_string = pstrdup((*plansource).query_string);
    MemoryContextSetIdentifier(source_context, (*newsource).query_string);
    (*newsource).commandTag = (*plansource).commandTag;
    if (*plansource).num_params > 0 {
        (*newsource).param_types =
            palloc((*plansource).num_params as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        memcpy(
            (*newsource).param_types as *mut c_void,
            (*plansource).param_types as *const c_void,
            (*plansource).num_params as usize * core::mem::size_of::<Oid>(),
        );
    } else {
        (*newsource).param_types = null_mut();
    }
    (*newsource).num_params = (*plansource).num_params;
    (*newsource).parserSetup = (*plansource).parserSetup;
    (*newsource).parserSetupArg = (*plansource).parserSetupArg;
    (*newsource).postRewrite = (*plansource).postRewrite;
    (*newsource).postRewriteArg = (*plansource).postRewriteArg;
    (*newsource).cursor_options = (*plansource).cursor_options;
    (*newsource).fixed_result = (*plansource).fixed_result;
    if !(*plansource).resultDesc.is_null() {
        (*newsource).resultDesc = CreateTupleDescCopy((*plansource).resultDesc);
    } else {
        (*newsource).resultDesc = null_mut();
    }
    (*newsource).context = source_context;

    querytree_context = AllocSetContextCreate!(
        source_context,
        c"CachedPlanQuery".as_ptr(),
        ALLOCSET_START_SMALL_SIZES
    );
    MemoryContextSwitchTo(querytree_context);
    (*newsource).query_list = copyObject((*plansource).query_list);
    (*newsource).relationOids = copyObject((*plansource).relationOids);
    (*newsource).invalItems = copyObject((*plansource).invalItems);
    if !(*plansource).search_path.is_null() {
        (*newsource).search_path = CopySearchPathMatcher((*plansource).search_path);
    }
    (*newsource).query_context = querytree_context;
    (*newsource).rewriteRoleId = (*plansource).rewriteRoleId;
    (*newsource).rewriteRowSecurity = (*plansource).rewriteRowSecurity;
    (*newsource).dependsOnRLS = (*plansource).dependsOnRLS;

    (*newsource).gplan = null_mut();

    (*newsource).is_oneshot = false;
    (*newsource).is_complete = true;
    (*newsource).is_saved = false;
    (*newsource).is_valid = (*plansource).is_valid;
    (*newsource).generation = (*plansource).generation;

    /* We may as well copy any acquired cost knowledge */
    (*newsource).generic_cost = (*plansource).generic_cost;
    (*newsource).total_custom_cost = (*plansource).total_custom_cost;
    (*newsource).num_generic_plans = (*plansource).num_generic_plans;
    (*newsource).num_custom_plans = (*plansource).num_custom_plans;

    MemoryContextSwitchTo(oldcxt);

    newsource
}

/*
 * CachedPlanIsValid: test whether the rewritten querytree within a
 * CachedPlanSource is currently valid (that is, not marked as being in need
 * of revalidation).
 *
 * This result is only trustworthy (ie, free from race conditions) if
 * the caller has acquired locks on all the relations used in the plan.
 */
pub unsafe fn CachedPlanIsValid(plansource: *mut CachedPlanSource) -> bool {
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    (*plansource).is_valid
}

/*
 * CachedPlanGetTargetList: return tlist, if any, describing plan's output
 *
 * The result is guaranteed up-to-date.  However, it is local storage
 * within the cached plan, and may disappear next time the plan is updated.
 */
pub unsafe fn CachedPlanGetTargetList(
    plansource: *mut CachedPlanSource,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    let pstmt: *mut Query;

    /* Assert caller is doing things in a sane order */
    debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);
    debug_assert!((*plansource).is_complete);

    /*
     * No work needed if statement doesn't return tuples (we assume this
     * feature cannot be changed by an invalidation)
     */
    if (*plansource).resultDesc.is_null() {
        return NIL;
    }

    /* Make sure the querytree list is valid and we have parse-time locks */
    RevalidateCachedQuery(plansource, queryEnv);

    /* Get the primary statement and find out what it returns */
    pstmt = QueryListGetPrimaryStmt((*plansource).query_list);

    FetchStatementTargetList(pstmt as *mut Node)
}

/*
 * GetCachedExpression: construct a CachedExpression for an expression.
 *
 * This performs the same transformations on the expression as
 * expression_planner(), ie, convert an expression as emitted by parse
 * analysis to be ready to pass to the executor.
 *
 * The result is stashed in a private, long-lived memory context.
 * (Note that this might leak a good deal of memory in the caller's
 * context before that.)  The passed-in expr tree is not modified.
 */
#[no_mangle]
pub unsafe fn GetCachedExpression(mut expr: *mut Node) -> *mut CachedExpression {
    let cexpr: *mut CachedExpression;
    let mut relationOids: *mut List = null_mut();
    let mut invalItems: *mut List = null_mut();
    let cexpr_context: MemoryContext;
    let oldcxt: MemoryContext;

    /*
     * Pass the expression through the planner, and collect dependencies.
     * Everything built here is leaked in the caller's context; that's
     * intentional to minimize the size of the permanent data structure.
     */
    expr = expression_planner_with_deps(
        expr as *mut c_void,
        &raw mut relationOids,
        &raw mut invalItems,
    ) as *mut Node;

    /*
     * Make a private memory context, and copy what we need into that.  To
     * avoid leaking a long-lived context if we fail while copying data, we
     * initially make the context under the caller's context.
     */
    cexpr_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"CachedExpression".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );

    oldcxt = MemoryContextSwitchTo(cexpr_context);

    cexpr = palloc(core::mem::size_of::<CachedExpression>()) as *mut CachedExpression;
    (*cexpr).magic = CACHEDEXPR_MAGIC;
    (*cexpr).expr = copyObject(expr);
    (*cexpr).is_valid = true;
    (*cexpr).relationOids = copyObject(relationOids);
    (*cexpr).invalItems = copyObject(invalItems);
    (*cexpr).context = cexpr_context;

    MemoryContextSwitchTo(oldcxt);

    /*
     * Reparent the expr's memory context under CacheMemoryContext so that it
     * will live indefinitely.
     */
    MemoryContextSetParent(cexpr_context, CacheMemoryContext);

    /*
     * Add the entry to the global list of cached expressions.
     */
    dlist_push_tail(&raw mut cached_expression_list, &raw mut (*cexpr).node);

    cexpr
}

/*
 * FreeCachedExpression
 *		Delete a CachedExpression.
 */
#[no_mangle]
pub unsafe fn FreeCachedExpression(cexpr: *mut CachedExpression) {
    /* Sanity check */
    debug_assert!((*cexpr).magic == CACHEDEXPR_MAGIC);
    /* Unlink from global list */
    dlist_delete(&raw mut (*cexpr).node);
    /* Free all storage associated with CachedExpression */
    MemoryContextDelete((*cexpr).context);
}

/*
 * QueryListGetPrimaryStmt
 *		Get the "primary" stmt within a list, ie, the one marked canSetTag.
 *
 * Returns NULL if no such stmt.  If multiple queries within the list are
 * marked canSetTag, returns the first one.  Neither of these cases should
 * occur in present usages of this function.
 */
unsafe fn QueryListGetPrimaryStmt(stmts: *mut List) -> *mut Query {
    let lc: *mut crate::nodes::pg_list::ListCell;

    let _ = lc;
    foreach!(lc, stmts, {
        let stmt = lfirst_node!(Query, T_Query, current_cell!(lc));

        if (*stmt).canSetTag {
            return stmt;
        }
    });
    null_mut()
}

/*
 * AcquireExecutorLocks: acquire locks needed for execution of a cached plan;
 * or release them if acquire is false.
 */
unsafe fn AcquireExecutorLocks(stmt_list: *mut List, acquire: bool) {
    let lc1: *mut crate::nodes::pg_list::ListCell;

    let _ = lc1;
    foreach!(lc1, stmt_list, {
        let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc1));
        let lc2: *mut crate::nodes::pg_list::ListCell;

        if (*plannedstmt).commandType == CMD_UTILITY {
            /*
             * Ignore utility statements, except those (such as EXPLAIN) that
             * contain a parsed-but-not-planned query.  Note: it's okay to use
             * ScanQueryForLocks, even though the query hasn't been through
             * rule rewriting, because rewriting doesn't change the query
             * representation.
             */
            let query = UtilityContainsQuery((*plannedstmt).utilityStmt);

            if !query.is_null() {
                ScanQueryForLocks(query, acquire);
            }
            continue;
        }

        let _ = lc2;
        foreach!(lc2, (*plannedstmt).rtable, {
            let rte = lfirst(current_cell!(lc2)) as *mut RangeTblEntry;

            if !((*rte).rtekind == RTE_RELATION
                || ((*rte).rtekind == RTE_SUBQUERY && OidIsValid((*rte).relid)))
            {
                continue;
            }

            /*
             * Acquire the appropriate type of lock on each relation OID. Note
             * that we don't actually try to open the rel, and hence will not
             * fail if it's been dropped entirely --- we'll just transiently
             * acquire a non-conflicting lock.
             */
            if acquire {
                LockRelationOid((*rte).relid, (*rte).rellockmode);
            } else {
                UnlockRelationOid((*rte).relid, (*rte).rellockmode);
            }
        });
    });
}

/*
 * AcquirePlannerLocks: acquire locks needed for planning of a querytree list;
 * or release them if acquire is false.
 *
 * Note that we don't actually try to open the relations, and hence will not
 * fail if one has been dropped entirely --- we'll just transiently acquire
 * a non-conflicting lock.
 */
unsafe fn AcquirePlannerLocks(stmt_list: *mut List, acquire: bool) {
    let lc: *mut crate::nodes::pg_list::ListCell;

    let _ = lc;
    foreach!(lc, stmt_list, {
        let mut query = lfirst_node!(Query, T_Query, current_cell!(lc));

        if (*query).commandType == CMD_UTILITY {
            /* Ignore utility statements, unless they contain a Query */
            query = UtilityContainsQuery((*query).utilityStmt);
            if !query.is_null() {
                ScanQueryForLocks(query, acquire);
            }
            continue;
        }

        ScanQueryForLocks(query, acquire);
    });
}

/*
 * ScanQueryForLocks: recursively scan one Query for AcquirePlannerLocks.
 */
unsafe fn ScanQueryForLocks(parsetree: *mut Query, acquire: bool) {
    let lc: *mut crate::nodes::pg_list::ListCell;

    /* Shouldn't get called on utility commands */
    debug_assert!((*parsetree).commandType != CMD_UTILITY);

    /*
     * First, process RTEs of the current query level.
     */
    let _ = lc;
    foreach!(lc, (*parsetree).rtable, {
        let rte = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

        match (*rte).rtekind {
            RTEKind::RTE_RELATION => {
                /* Acquire or release the appropriate type of lock */
                if acquire {
                    LockRelationOid((*rte).relid, (*rte).rellockmode);
                } else {
                    UnlockRelationOid((*rte).relid, (*rte).rellockmode);
                }
            }

            RTEKind::RTE_SUBQUERY => {
                /* If this was a view, must lock/unlock the view */
                if OidIsValid((*rte).relid) {
                    if acquire {
                        LockRelationOid((*rte).relid, (*rte).rellockmode);
                    } else {
                        UnlockRelationOid((*rte).relid, (*rte).rellockmode);
                    }
                }
                /* Recurse into subquery-in-FROM */
                ScanQueryForLocks((*rte).subquery, acquire);
            }

            _ => {
                /* ignore other types of RTEs */
            }
        }
    });

    /* Recurse into subquery-in-WITH */
    foreach!(lc, (*parsetree).cteList, {
        let cte = lfirst_node!(CommonTableExpr, T_CommonTableExpr, current_cell!(lc));

        ScanQueryForLocks(castNode!(Query, T_Query, (*cte).ctequery), acquire);
    });

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (*parsetree).hasSubLinks {
        let mut acquire = acquire;
        query_tree_walker(
            parsetree,
            Some(ScanQueryWalker),
            &raw mut acquire as *mut c_void,
            QTW_IGNORE_RC_SUBQUERIES,
        );
    }
}

/*
 * Walker to find sublink subqueries for ScanQueryForLocks
 */
unsafe fn ScanQueryWalker(node: *mut Node, acquire: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_SubLink) {
        let sub = node as *mut SubLink;

        /* Do what we came for */
        ScanQueryForLocks(castNode!(Query, T_Query, (*sub).subselect), *(acquire as *mut bool));
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because ScanQueryForLocks already
     * processed subselects of subselects for us.
     */
    expression_tree_walker(node, Some(ScanQueryWalker), acquire)
}

/*
 * PlanCacheComputeResultDesc: given a list of analyzed-and-rewritten Queries,
 * determine the result tupledesc it will produce.  Returns NULL if the
 * execution will not return tuples.
 *
 * Note: the result is created or copied into current memory context.
 */
unsafe fn PlanCacheComputeResultDesc(stmt_list: *mut List) -> TupleDesc {
    let query: *mut Query;

    match ChoosePortalStrategy(stmt_list) {
        PortalStrategy::PORTAL_ONE_SELECT | PortalStrategy::PORTAL_ONE_MOD_WITH => {
            query = linitial_node!(Query, T_Query, stmt_list);
            return ExecCleanTypeFromTL((*query).targetList);
        }

        PortalStrategy::PORTAL_ONE_RETURNING => {
            query = QueryListGetPrimaryStmt(stmt_list);
            debug_assert!(!(*query).returningList.is_null());
            return ExecCleanTypeFromTL((*query).returningList);
        }

        PortalStrategy::PORTAL_UTIL_SELECT => {
            query = linitial_node!(Query, T_Query, stmt_list);
            debug_assert!(!(*query).utilityStmt.is_null());
            return UtilityTupleDescriptor((*query).utilityStmt);
        }

        PortalStrategy::PORTAL_MULTI_QUERY => {
            /* will not return tuples */
        }
    }
    null_mut()
}

/*
 * PlanCacheRelCallback
 *		Relcache inval callback function
 *
 * Invalidate all plans mentioning the given rel, or all plans mentioning
 * any rel at all if relid == InvalidOid.
 */
unsafe fn PlanCacheRelCallback(_arg: Datum, relid: Oid) {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &raw mut saved_plan_list, {
        let plansource = dlist_container!(CachedPlanSource, node, (iter).cur);

        debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if !(*plansource).is_valid {
            continue;
        }

        /* Never invalidate if parse/plan would be a no-op anyway */
        if !StmtPlanRequiresRevalidation(plansource) {
            continue;
        }

        /*
         * Check the dependency list for the rewritten querytree.
         */
        if if relid == InvalidOid {
            (*plansource).relationOids != NIL
        } else {
            list_member_oid((*plansource).relationOids, relid)
        } {
            /* Invalidate the querytree and generic plan */
            (*plansource).is_valid = false;
            if !(*plansource).gplan.is_null() {
                (*(*plansource).gplan).is_valid = false;
            }
        }

        /*
         * The generic plan, if any, could have more dependencies than the
         * querytree does, so we have to check it too.
         */
        if !(*plansource).gplan.is_null() && (*(*plansource).gplan).is_valid {
            let lc: *mut crate::nodes::pg_list::ListCell;

            let _ = lc;
            foreach!(lc, (*(*plansource).gplan).stmt_list, {
                let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc));

                if (*plannedstmt).commandType == CMD_UTILITY {
                    continue; /* Ignore utility statements */
                }
                if if relid == InvalidOid {
                    (*plannedstmt).relationOids != NIL
                } else {
                    list_member_oid((*plannedstmt).relationOids, relid)
                } {
                    /* Invalidate the generic plan only */
                    (*(*plansource).gplan).is_valid = false;
                    break; /* out of stmt_list scan */
                }
            });
        }
    });

    /* Likewise check cached expressions */
    dlist_foreach!(iter, &raw mut cached_expression_list, {
        let cexpr = dlist_container!(CachedExpression, node, (iter).cur);

        debug_assert!((*cexpr).magic == CACHEDEXPR_MAGIC);

        /* No work if it's already invalidated */
        if !(*cexpr).is_valid {
            continue;
        }

        if if relid == InvalidOid {
            (*cexpr).relationOids != NIL
        } else {
            list_member_oid((*cexpr).relationOids, relid)
        } {
            (*cexpr).is_valid = false;
        }
    });
}

/*
 * PlanCacheObjectCallback
 *		Syscache inval callback function for PROCOID and TYPEOID caches
 *
 * Invalidate all plans mentioning the object with the specified hash value,
 * or all plans mentioning any member of this cache if hashvalue == 0.
 */
unsafe fn PlanCacheObjectCallback(_arg: Datum, cacheid: c_int, hashvalue: uint32) {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &raw mut saved_plan_list, {
        let plansource = dlist_container!(CachedPlanSource, node, (iter).cur);
        let lc: *mut crate::nodes::pg_list::ListCell;

        debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if !(*plansource).is_valid {
            continue;
        }

        /* Never invalidate if parse/plan would be a no-op anyway */
        if !StmtPlanRequiresRevalidation(plansource) {
            continue;
        }

        /*
         * Check the dependency list for the rewritten querytree.
         */
        let _ = lc;
        foreach!(lc, (*plansource).invalItems, {
            let item = lfirst(current_cell!(lc)) as *mut PlanInvalItem;

            if (*item).cacheId != cacheid {
                continue;
            }
            if hashvalue == 0 || (*item).hashValue == hashvalue {
                /* Invalidate the querytree and generic plan */
                (*plansource).is_valid = false;
                if !(*plansource).gplan.is_null() {
                    (*(*plansource).gplan).is_valid = false;
                }
                break;
            }
        });

        /*
         * The generic plan, if any, could have more dependencies than the
         * querytree does, so we have to check it too.
         */
        if !(*plansource).gplan.is_null() && (*(*plansource).gplan).is_valid {
            foreach!(lc, (*(*plansource).gplan).stmt_list, {
                let plannedstmt = lfirst_node!(PlannedStmt, T_PlannedStmt, current_cell!(lc));
                let lc3: *mut crate::nodes::pg_list::ListCell;

                if (*plannedstmt).commandType == CMD_UTILITY {
                    continue; /* Ignore utility statements */
                }
                let _ = lc3;
                foreach!(lc3, (*plannedstmt).invalItems, {
                    let item = lfirst(current_cell!(lc3)) as *mut PlanInvalItem;

                    if (*item).cacheId != cacheid {
                        continue;
                    }
                    if hashvalue == 0 || (*item).hashValue == hashvalue {
                        /* Invalidate the generic plan only */
                        (*(*plansource).gplan).is_valid = false;
                        break; /* out of invalItems scan */
                    }
                });
                if !(*(*plansource).gplan).is_valid {
                    break; /* out of stmt_list scan */
                }
            });
        }
    });

    /* Likewise check cached expressions */
    dlist_foreach!(iter, &raw mut cached_expression_list, {
        let cexpr = dlist_container!(CachedExpression, node, (iter).cur);
        let lc: *mut crate::nodes::pg_list::ListCell;

        debug_assert!((*cexpr).magic == CACHEDEXPR_MAGIC);

        /* No work if it's already invalidated */
        if !(*cexpr).is_valid {
            continue;
        }

        let _ = lc;
        foreach!(lc, (*cexpr).invalItems, {
            let item = lfirst(current_cell!(lc)) as *mut PlanInvalItem;

            if (*item).cacheId != cacheid {
                continue;
            }
            if hashvalue == 0 || (*item).hashValue == hashvalue {
                (*cexpr).is_valid = false;
                break;
            }
        });
    });
}

/*
 * PlanCacheSysCallback
 *		Syscache inval callback function for other caches
 *
 * Just invalidate everything...
 */
unsafe fn PlanCacheSysCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    ResetPlanCache();
}

/*
 * ResetPlanCache: invalidate all cached plans.
 */
pub unsafe fn ResetPlanCache() {
    let mut iter: dlist_iter = dlist_iter {
        cur: null_mut(),
        end: null_mut(),
    };

    dlist_foreach!(iter, &raw mut saved_plan_list, {
        let plansource = dlist_container!(CachedPlanSource, node, (iter).cur);

        debug_assert!((*plansource).magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if !(*plansource).is_valid {
            continue;
        }

        /*
         * We *must not* mark transaction control statements as invalid,
         * particularly not ROLLBACK, because they may need to be executed in
         * aborted transactions when we can't revalidate them (cf bug #5269).
         * In general there's no point in invalidating statements for which a
         * new parse analysis/rewrite/plan cycle would certainly give the same
         * results.
         */
        if !StmtPlanRequiresRevalidation(plansource) {
            continue;
        }

        (*plansource).is_valid = false;
        if !(*plansource).gplan.is_null() {
            (*(*plansource).gplan).is_valid = false;
        }
    });

    /* Likewise invalidate cached expressions */
    dlist_foreach!(iter, &raw mut cached_expression_list, {
        let cexpr = dlist_container!(CachedExpression, node, (iter).cur);

        debug_assert!((*cexpr).magic == CACHEDEXPR_MAGIC);

        (*cexpr).is_valid = false;
    });
}

/*
 * Release all CachedPlans remembered by 'owner'
 */
#[no_mangle]
pub unsafe fn ReleaseAllPlanCacheRefsInOwner(owner: ResourceOwner) {
    ResourceOwnerReleaseAllOfKind(owner, &planref_resowner_desc);
}

/* ResourceOwner callbacks */

unsafe fn ResOwnerReleaseCachedPlan(res: Datum) {
    ReleaseCachedPlan(DatumGetPointer(res) as *mut CachedPlan, null_mut());
}

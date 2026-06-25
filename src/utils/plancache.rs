//! Translated from PostgreSQL src/include/utils/plancache.h
//! Plan cache definitions. In-memory.

use crate::access::tupdesc::TupleDesc;
use crate::c::TransactionId;
use crate::catalog::namespace::SearchPathMatcher;
use crate::lib::ilist::dlist_node;
use crate::nodes::nodes::Node;
use crate::nodes::params::{ParamListInfo, ParserSetupHook};
use crate::nodes::parsenodes::Query;
use crate::nodes::plannodes::PlannedStmt;
use crate::postgres_ext::Oid;
use crate::tcop::cmdtaglist::CommandTag;
use crate::utils::palloc::MemoryContext;
use crate::utils::queryenvironment::QueryEnvironment;
use crate::utils::resowner::ResourceOwner;

/// possible values for plan_cache_mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheMode {
    PLAN_CACHE_MODE_AUTO,
    PLAN_CACHE_MODE_FORCE_GENERIC_PLAN,
    PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN,
}

/// GUC parameter (process global). TODO(global).
pub static mut plan_cache_mode: i32 = 0;

/// Optional callback to editorialize on rewritten parse trees. `void *arg`
/// becomes a captured closure.
pub type PostRewriteHook = fn(querytree_list: &mut [Query]);

pub const CACHEDPLANSOURCE_MAGIC: i32 = 195726186;
pub const CACHEDPLAN_MAGIC: i32 = 953717834;
pub const CACHEDEXPR_MAGIC: i32 = 838275847;

/// A cached SQL query: source text, source parse tree, analyzed-and-rewritten
/// query tree, and adjunct data. Only one of raw/analyzed parse tree is set.
pub struct CachedPlanSource {
    /// should equal CACHEDPLANSOURCE_MAGIC
    pub magic: i32,
    /// output of raw_parser(), or None. TODO(ptr).
    pub raw_parse_tree: Option<*mut Node>,
    /// analyzed parse tree, or None. TODO(ptr).
    pub analyzed_parse_tree: Option<*mut Query>,
    /// source text of query
    pub query_string: String,
    /// command tag for query
    pub commandTag: CommandTag,
    /// parameter type OIDs, or empty
    pub param_types: Vec<Oid>,
    /// length of param_types array
    pub num_params: i32,
    /// alternative parameter spec method
    pub parserSetup: Option<ParserSetupHook>,
    /// opaque parser-setup arg. TODO(ptr): captured closure.
    pub parserSetupArg: *mut core::ffi::c_void,
    /// see SetPostRewriteHook
    pub postRewrite: Option<PostRewriteHook>,
    pub postRewriteArg: *mut core::ffi::c_void,
    /// cursor options used for planning
    pub cursor_options: i32,
    /// disallow change in result tupdesc?
    pub fixed_result: bool,
    /// result type; null = doesn't return tuples
    pub resultDesc: TupleDesc,
    /// memory context holding all above
    pub context: MemoryContext,

    // Current analyzed-and-rewritten query tree:
    /// list of Query nodes, or empty if not valid
    pub query_list: Vec<Query>,
    /// OIDs of relations the queries depend on
    pub relationOids: Vec<Oid>,
    /// other dependencies, as PlanInvalItems
    pub invalItems: Vec<Node>,
    /// search_path used for parsing and planning. TODO(ptr).
    pub search_path: Option<*mut SearchPathMatcher>,
    /// context holding the above, or None
    pub query_context: MemoryContext,
    /// Role ID we did rewriting for
    pub rewriteRoleId: Oid,
    /// row_security used during rewrite
    pub rewriteRowSecurity: bool,
    /// is rewritten query specific to the above?
    pub dependsOnRLS: bool,

    /// generic plan, or None. TODO(ptr): reference-counted link.
    pub gplan: Option<*mut CachedPlan>,

    // State flags:
    pub is_oneshot: bool,
    pub is_complete: bool,
    pub is_saved: bool,
    pub is_valid: bool,
    /// increments each time we create a plan
    pub generation: i32,
    /// global-list link, if is_saved
    pub node: dlist_node,

    // Custom-vs-generic decision state:
    /// cost of generic plan, or -1 if not known
    pub generic_cost: f64,
    pub total_custom_cost: f64,
    pub num_custom_plans: i64,
    pub num_generic_plans: i64,
}

/// An execution plan derived from a CachedPlanSource. Reference-counted.
pub struct CachedPlan {
    /// should equal CACHEDPLAN_MAGIC
    pub magic: i32,
    /// list of PlannedStmts
    pub stmt_list: Vec<PlannedStmt>,
    pub is_oneshot: bool,
    pub is_saved: bool,
    pub is_valid: bool,
    /// Role ID the plan was created for
    pub planRoleId: Oid,
    pub dependsOnRole: bool,
    /// replan when TransactionXmin changes from this value
    pub saved_xmin: TransactionId,
    /// parent's generation number for this plan
    pub generation: i32,
    /// count of live references
    pub refcount: i32,
    /// context containing this CachedPlan
    pub context: MemoryContext,
}

/// Cached planned form of a standalone scalar expression.
pub struct CachedExpression {
    /// should equal CACHEDEXPR_MAGIC
    pub magic: i32,
    /// planned form of expression
    pub expr: Box<Node>,
    pub is_valid: bool,
    // private to plancache.c:
    pub relationOids: Vec<Oid>,
    pub invalItems: Vec<Node>,
    pub context: MemoryContext,
    pub node: dlist_node,
}

pub fn InitPlanCache() {
    unimplemented!()
}
pub fn ResetPlanCache() {
    unimplemented!()
}

pub fn ReleaseAllPlanCacheRefsInOwner(_owner: ResourceOwner) {
    unimplemented!()
}

pub fn CreateCachedPlan(
    _raw_parse_tree: &mut Node,
    _query_string: &str,
    _command_tag: CommandTag,
) -> *mut CachedPlanSource {
    unimplemented!() // TODO(ptr)
}
pub fn CreateCachedPlanForQuery(
    _analyzed_parse_tree: &mut Query,
    _query_string: &str,
    _command_tag: CommandTag,
) -> *mut CachedPlanSource {
    unimplemented!() // TODO(ptr)
}
pub fn CreateOneShotCachedPlan(
    _raw_parse_tree: &mut Node,
    _query_string: &str,
    _command_tag: CommandTag,
) -> *mut CachedPlanSource {
    unimplemented!() // TODO(ptr)
}
pub fn CompleteCachedPlan(
    _plansource: &mut CachedPlanSource,
    _querytree_list: &[Query],
    _querytree_context: MemoryContext,
    _param_types: &[Oid],
    _num_params: i32,
    _parser_setup: Option<ParserSetupHook>,
    _parser_setup_arg: *mut core::ffi::c_void,
    _cursor_options: i32,
    _fixed_result: bool,
) {
    unimplemented!()
}
pub fn SetPostRewriteHook(
    _plansource: &mut CachedPlanSource,
    _post_rewrite: PostRewriteHook,
    _post_rewrite_arg: *mut core::ffi::c_void,
) {
    unimplemented!()
}

pub fn SaveCachedPlan(_plansource: &mut CachedPlanSource) {
    unimplemented!()
}
pub fn DropCachedPlan(_plansource: &mut CachedPlanSource) {
    unimplemented!()
}

pub fn CachedPlanSetParentContext(_plansource: &mut CachedPlanSource, _newcontext: MemoryContext) {
    unimplemented!()
}

pub fn CopyCachedPlan(_plansource: &mut CachedPlanSource) -> *mut CachedPlanSource {
    unimplemented!() // TODO(ptr)
}

pub fn CachedPlanIsValid(_plansource: &mut CachedPlanSource) -> bool {
    unimplemented!()
}

pub fn CachedPlanGetTargetList(
    _plansource: &mut CachedPlanSource,
    _query_env: Option<&mut QueryEnvironment>,
) -> Vec<Node> {
    unimplemented!()
}

pub fn GetCachedPlan(
    _plansource: &mut CachedPlanSource,
    _bound_params: Option<ParamListInfo>,
    _owner: ResourceOwner,
    _query_env: Option<&mut QueryEnvironment>,
) -> *mut CachedPlan {
    unimplemented!() // TODO(ptr)
}
pub fn ReleaseCachedPlan(_plan: &mut CachedPlan, _owner: ResourceOwner) {
    unimplemented!()
}

pub fn CachedPlanAllowsSimpleValidityCheck(
    _plansource: &mut CachedPlanSource,
    _plan: &mut CachedPlan,
    _owner: ResourceOwner,
) -> bool {
    unimplemented!()
}
pub fn CachedPlanIsSimplyValid(
    _plansource: &mut CachedPlanSource,
    _plan: &mut CachedPlan,
    _owner: ResourceOwner,
) -> bool {
    unimplemented!()
}

pub fn GetCachedExpression(_expr: &mut Node) -> *mut CachedExpression {
    unimplemented!() // TODO(ptr)
}
pub fn FreeCachedExpression(_cexpr: &mut CachedExpression) {
    unimplemented!()
}

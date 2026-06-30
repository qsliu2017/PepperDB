//! Translated from PostgreSQL src/include/utils/plancache.h
//! Plan cache definitions. In-memory.
//!
//! Ownership model (rules.md s10 - pointers become ownership, not raw refcounts):
//!  - A `CachedPlanSource` is OWNED by whoever created it. A named prepared
//!    statement's source lives in the per-backend prepared-statement table
//!    (prepare.c, phase 2); a one-shot / cursor source is owned by the portal.
//!    Callers thread it by `&`/`&mut`.
//!  - A `CachedPlan` is reference-counted in C (`refcount`). Here it is shared via
//!    `Arc<CachedPlan>`: `GetCachedPlan` hands out an `Arc` clone (the caller's
//!    "reference"), the generic plan is cached as an `Arc` inside the source, and
//!    `ReleaseCachedPlan` drops the caller's `Arc`. The last drop frees it - the
//!    Arc strong count IS the C refcount.
//!  - STAGED (later milestones): plan-invalidation callbacks + relcache/sinval
//!    driven `RevalidateCachedQuery` (M9 revalidation is a minimal "still valid"),
//!    the generic-vs-custom cost tuning, MemoryContext-based plan storage, the
//!    saved-plan global list, and the result TupleDesc (Describe path = phase 2).

use std::sync::Arc;

use crate::c::TransactionId;
use crate::nodes::parsenodes::{Query, RawStmt};
use crate::nodes::params::{ParamListInfoData, ParserSetupHook};
use crate::nodes::plannodes::PlannedStmt;
use crate::postgres_ext::Oid;
use crate::tcop::cmdtaglist::CommandTag;
use crate::utils::queryenvironment::QueryEnvironment;

/// possible values for plan_cache_mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheMode {
    AUTO,
    FORCE_GENERIC_PLAN,
    FORCE_CUSTOM_PLAN,
}

/// GUC parameter (process global). TODO(global).
pub static mut plan_cache_mode: i32 = 0;

/// Optional callback to editorialize on rewritten parse trees. `void *arg`
/// becomes a captured closure (staged - unused for M9).
pub type PostRewriteHook = fn(querytree_list: &mut [Query]);

pub const CACHEDPLANSOURCE_MAGIC: i32 = 195_726_186;
pub const CACHEDPLAN_MAGIC: i32 = 953_717_834;
pub const CACHEDEXPR_MAGIC: i32 = 838_275_847;

/// A cached SQL query: source text, source parse tree, analyzed-and-rewritten
/// query tree, and adjunct data. Exactly one of `raw_parse_tree` /
/// `analyzed_parse_tree` is set (the other is `None`).
pub struct CachedPlanSource {
    /// should equal CACHEDPLANSOURCE_MAGIC
    pub magic: i32,
    /// output of raw_parser(), or None (owned).
    pub raw_parse_tree: Option<Box<RawStmt>>,
    /// pre-analyzed query, or None (owned) - the CreateCachedPlanForQuery path.
    pub analyzed_parse_tree: Option<Box<Query>>,
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
    /// see SetPostRewriteHook (staged)
    pub postRewrite: Option<PostRewriteHook>,
    /// cursor options used for planning
    pub cursor_options: i32,
    /// disallow change in result tupdesc?
    pub fixed_result: bool,

    // Current analyzed-and-rewritten query tree:
    /// list of Query nodes, or empty if not valid (owned)
    pub query_list: Vec<Query>,
    /// OIDs of relations the queries depend on
    pub relationOids: Vec<Oid>,
    /// Role ID we did rewriting for
    pub rewriteRoleId: Oid,
    /// row_security used during rewrite
    pub rewriteRowSecurity: bool,
    /// is rewritten query specific to the above?
    pub dependsOnRLS: bool,

    /// generic plan, or None (shared via Arc - the C reference-counted link)
    pub gplan: Option<Arc<CachedPlan>>,

    // State flags:
    pub is_oneshot: bool,
    pub is_complete: bool,
    pub is_saved: bool,
    pub is_valid: bool,
    /// increments each time we create a plan
    pub generation: i32,

    // Custom-vs-generic decision state:
    /// cost of generic plan, or -1 if not known
    pub generic_cost: f64,
    pub total_custom_cost: f64,
    pub num_custom_plans: i64,
    pub num_generic_plans: i64,
}

/// An execution plan derived from a CachedPlanSource. Shared via `Arc`; the Arc
/// strong count is the C `refcount`.
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
}

// --- function re-exports: bodies live in src/backend/utils/cache/plancache.rs ---
pub use crate::backend::utils::cache::plancache::{
    CompleteCachedPlan, CreateCachedPlan, CreateCachedPlanForQuery, CreateOneShotCachedPlan,
    GetCachedPlan, ReleaseCachedPlan, RevalidateCachedQuery,
};

pub fn InitPlanCache() {
    unimplemented!()
}
pub fn ResetPlanCache() {
    unimplemented!()
}

pub fn SaveCachedPlan(_plansource: &mut CachedPlanSource) {
    unimplemented!()
}
pub fn DropCachedPlan(_plansource: &mut CachedPlanSource) {
    unimplemented!()
}

pub fn CachedPlanIsValid(_plansource: &mut CachedPlanSource) -> bool {
    unimplemented!()
}

pub fn CachedPlanGetTargetList(
    _plansource: &mut CachedPlanSource,
    _query_env: Option<&mut QueryEnvironment>,
) -> Vec<crate::nodes::nodes::Node> {
    unimplemented!()
}

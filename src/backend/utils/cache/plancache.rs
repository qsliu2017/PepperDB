//! Plan cache: the `CachedPlanSource` / `CachedPlan` machinery that prepared
//! statements, cursors, and SPI share. Translated from
//! backend/utils/cache/plancache.c (disposition: full leaf, M9-reachable subset).
//!
//! Lifecycle (PG): `CreateCachedPlan` (store the raw parse tree + tag) ->
//! `CompleteCachedPlan` (attach the analyzed/rewritten query list + param spec)
//! -> `GetCachedPlan` (revalidate, choose generic vs custom, plan, hand back a
//! reference) -> `ReleaseCachedPlan` (drop the reference).
//!
//! Ownership (rules.md s10): the source is owned by its holder (the prepared-stmt
//! table or a portal); a `CachedPlan` is shared via `Arc` and the Arc strong count
//! IS the C `refcount` - see the header doc.
//!
//! STAGED for later milestones (kept structurally honest, not silently dropped):
//!  - `RevalidateCachedQuery` is a minimal "still valid" (DDL/relcache/sinval-
//!    driven invalidation + re-analysis lands with plan invalidation);
//!  - the generic-vs-custom cost comparison is the simple heuristic "custom when
//!    bound params are present" (PG's `choose_custom_plan` cost tuning later);
//!  - MemoryContext-based plan storage and the saved-plan global list.

use std::sync::Arc;

use crate::nodes::params::ParamListInfoData;
use crate::nodes::parsenodes::{Query, RawStmt};
use crate::nodes::plannodes::PlannedStmt;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::tcop::cmdtaglist::CommandTag;
use crate::utils::plancache::{
    CachedPlan, CachedPlanSource, CACHEDPLANSOURCE_MAGIC, CACHEDPLAN_MAGIC,
};
use crate::utils::queryenvironment::QueryEnvironment;

/// PG `CreateCachedPlan`: create a `CachedPlanSource` from a raw parse tree. Most
/// fields are left empty until `CompleteCachedPlan`.
#[must_use]
pub fn CreateCachedPlan(
    raw_parse_tree: RawStmt,
    query_string: &str,
    command_tag: CommandTag,
) -> Box<CachedPlanSource> {
    new_plansource(Some(Box::new(raw_parse_tree)), None, query_string, command_tag, false)
}

/// PG `CreateCachedPlanForQuery`: like CreateCachedPlan but from an already
/// parse-analyzed Query (the no-raw-parse-tree path).
#[must_use]
pub fn CreateCachedPlanForQuery(
    analyzed_parse_tree: Query,
    query_string: &str,
    command_tag: CommandTag,
) -> Box<CachedPlanSource> {
    new_plansource(None, Some(Box::new(analyzed_parse_tree)), query_string, command_tag, false)
}

/// PG `CreateOneShotCachedPlan`: a source that will be planned and executed only
/// once, so no querytree/plan copying is needed.
#[must_use]
pub fn CreateOneShotCachedPlan(
    raw_parse_tree: RawStmt,
    query_string: &str,
    command_tag: CommandTag,
) -> Box<CachedPlanSource> {
    new_plansource(Some(Box::new(raw_parse_tree)), None, query_string, command_tag, true)
}

fn new_plansource(
    raw_parse_tree: Option<Box<RawStmt>>,
    analyzed_parse_tree: Option<Box<Query>>,
    query_string: &str,
    command_tag: CommandTag,
    is_oneshot: bool,
) -> Box<CachedPlanSource> {
    Box::new(CachedPlanSource {
        magic: CACHEDPLANSOURCE_MAGIC,
        raw_parse_tree,
        analyzed_parse_tree,
        query_string: query_string.to_owned(),
        commandTag: command_tag,
        param_types: Vec::new(),
        num_params: 0,
        parserSetup: None,
        postRewrite: None,
        cursor_options: 0,
        fixed_result: false,
        query_list: Vec::new(),
        relationOids: Vec::new(),
        rewriteRoleId: InvalidOid,
        rewriteRowSecurity: false,
        dependsOnRLS: false,
        gplan: None,
        is_oneshot,
        is_complete: false,
        is_saved: false,
        is_valid: false,
        generation: 0,
        generic_cost: -1.0,
        total_custom_cost: 0.0,
        num_custom_plans: 0,
        num_generic_plans: 0,
    })
}

/// PG `CompleteCachedPlan`: attach the analyzed-and-rewritten query list plus the
/// final parameter specification to a source created by CreateCachedPlan. After
/// this the source is complete and valid.
///
/// The C dependency extraction (`extract_query_dependencies` -> relationOids/
/// invalItems/search_path) feeds invalidation, which is staged for M9; the query
/// list is stored directly (no MemoryContext reparenting - ownership is explicit).
#[allow(
    clippy::too_many_arguments,
    reason = "1:1 with PG CompleteCachedPlan's parameter list (querytree + param spec + cursor/result flags)"
)]
pub fn CompleteCachedPlan(
    plansource: &mut CachedPlanSource,
    querytree_list: Vec<Query>,
    param_types: &[Oid],
    num_params: i32,
    parser_setup: Option<crate::nodes::params::ParserSetupHook>,
    cursor_options: i32,
    fixed_result: bool,
) {
    crate::assert!(plansource.magic == CACHEDPLANSOURCE_MAGIC);
    crate::assert!(!plansource.is_complete);

    plansource.query_list = querytree_list;
    // extract_query_dependencies + GetSearchPathMatcher (invalidation deps) staged.
    plansource.param_types = param_types.to_vec();
    plansource.num_params = num_params;
    plansource.parserSetup = parser_setup;
    plansource.cursor_options = cursor_options;
    plansource.fixed_result = fixed_result;
    // resultDesc (PlanCacheComputeResultDesc) computed by the Describe path (phase 2).

    plansource.is_complete = true;
    plansource.is_valid = true;
}

/// PG `RevalidateCachedQuery`: ensure the source's query list is up to date and
/// (in full PG) reacquire parse-time locks, re-analyzing if an invalidation made
/// the query stale. M9 has no DDL/relcache invalidation, so a complete source is
/// always still valid; this returns the current query list. The full
/// inval-driven re-analysis path is staged here cleanly.
pub fn RevalidateCachedQuery<'a>(
    plansource: &'a mut CachedPlanSource,
    _query_env: Option<&mut QueryEnvironment>,
) -> &'a [Query] {
    crate::assert!(plansource.magic == CACHEDPLANSOURCE_MAGIC);
    if !plansource.is_valid {
        // The DDL-invalidation -> re-parse-analyze -> re-rewrite rebuild lands with
        // plan invalidation; nothing invalidates a complete source in M9.
        unimplemented!("RevalidateCachedQuery: invalidation-driven re-analysis not yet reachable");
    }
    &plansource.query_list
}

/// PG `GetCachedPlan`: get a plan from a CachedPlanSource, building one if needed.
/// Returns a reference-counted `CachedPlan` (an `Arc` clone).
///
/// For M9 the generic-vs-custom choice is the simple heuristic PG also starts
/// from: use a custom (param-specialized) plan when bound parameters are present,
/// otherwise a generic plan that is cached on the source for reuse. The cost
/// comparison that later overrides this heuristic is staged.
#[must_use]
pub fn GetCachedPlan(
    plansource: &mut CachedPlanSource,
    bound_params: Option<&ParamListInfoData>,
    _query_env: Option<&mut QueryEnvironment>,
) -> Arc<CachedPlan> {
    crate::assert!(plansource.magic == CACHEDPLANSOURCE_MAGIC);
    crate::assert!(plansource.is_complete);

    // Make sure the querytree list is valid (and, in full PG, parse-time locks held).
    RevalidateCachedQuery(plansource, None);

    let customplan = choose_custom_plan(plansource, bound_params);

    if !customplan {
        // Generic plan: reuse the cached one if valid, else build and cache it.
        if let Some(gplan) = &plansource.gplan
            && gplan.is_valid
        {
            plansource.num_generic_plans += 1;
            return Arc::clone(gplan);
        }
        let plan = Arc::new(build_cached_plan(plansource, None));
        plansource.gplan = Some(Arc::clone(&plan));
        plansource.generic_cost = 0.0;
        plansource.num_generic_plans += 1;
        return plan;
    }

    // Custom plan: planned with the bound params, never cached on the source.
    let plan = Arc::new(build_cached_plan(plansource, bound_params));
    plansource.total_custom_cost += 0.0;
    plansource.num_custom_plans += 1;
    plan
}

/// PG `BuildCachedPlan`: run the planner over the (revalidated) query list to
/// produce the `PlannedStmt`s, wrapped in a `CachedPlan`.
fn build_cached_plan(
    plansource: &mut CachedPlanSource,
    bound_params: Option<&ParamListInfoData>,
) -> CachedPlan {
    use crate::backend::optimizer::plan::planner::standard_planner;

    // boundParams threading into the planner is staged (the planner does not yet
    // specialize on external Param values; param VALUES flow through ExprContext at
    // execute time). The query list itself is fully planned.
    let _ = bound_params;
    let stmt_list: Vec<PlannedStmt> = plansource
        .query_list
        .iter()
        .map(|q| {
            // A utility Query is not planned; it is wrapped trivially. M9's plan
            // cache reaches plannable queries (SELECT), so plan each one.
            let mut parse = q.clone();
            standard_planner(&mut parse, &plansource.query_string, plansource.cursor_options, None)
        })
        .collect();

    plansource.generation += 1;

    CachedPlan {
        magic: CACHEDPLAN_MAGIC,
        stmt_list,
        is_oneshot: plansource.is_oneshot,
        is_saved: plansource.is_saved,
        is_valid: true,
        planRoleId: InvalidOid,
        dependsOnRole: false,
        saved_xmin: crate::c::TransactionId::default(),
        generation: plansource.generation,
    }
}

/// PG `choose_custom_plan`: decide generic vs custom. M9 heuristic: a custom plan
/// when bound parameters are present (otherwise the plan can't differ from a
/// generic one), else generic. The cost-history tuning is staged.
fn choose_custom_plan(plansource: &CachedPlanSource, bound_params: Option<&ParamListInfoData>) -> bool {
    // One-shot plans are never cached as generic; treat as custom.
    if plansource.is_oneshot {
        return true;
    }
    bound_params.is_some_and(|p| p.num_params > 0)
}

/// PG `PlanCacheComputeResultDesc`: the result `TupleDesc` a complete source will
/// produce (for the extended-protocol Describe Statement, and the prepared-stmt
/// result desc). `None` if the execution returns no tuples (a DML statement
/// without RETURNING, or a utility statement). M9 reaches a SELECT (the
/// targetlist types) and a RETURNING DML (the returning-list types); the bare-
/// utility / no-tuple cases return `None`.
#[must_use]
pub fn plan_cache_compute_result_desc(
    plansource: &CachedPlanSource,
) -> Option<crate::access::tupdesc::TupleDesc> {
    use crate::backend::executor::execTuples::exec_clean_type_from_tl;
    use crate::nodes::nodes::CmdType;

    let query = plansource.query_list.first()?;
    match query.commandType {
        CmdType::SELECT => Some(exec_clean_type_from_tl(&query.targetList)),
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE => {
            if query.returningList.is_empty() {
                None
            } else {
                Some(exec_clean_type_from_tl(&query.returningList))
            }
        }
        // A utility query (UTIL_SELECT) result desc grows with the SHOW/EXPLAIN
        // tuple descriptors; M9 Describe over those is not reached.
        _ => None,
    }
}

/// PG `ReleaseCachedPlan`: drop a caller's reference to a `CachedPlan`. With Arc
/// the strong count is the refcount, so taking the value by move and dropping it
/// decrements; the last drop frees it.
pub fn ReleaseCachedPlan(plan: Arc<CachedPlan>) {
    crate::assert!(plan.magic == CACHEDPLAN_MAGIC);
    drop(plan);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::Node;
    use crate::parser::parser::RawParseMode;

    /// Raw-parse + analyze + rewrite `s` into a single Query for the plan cache,
    /// plus the RawStmt (mirrors how PostgresMain feeds CreateCachedPlan).
    fn raw_and_query(s: &str) -> (RawStmt, Query) {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs: RawStmt = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, s, &[], 0, None);
        let mut rewritten = crate::backend::rewrite::rewriteHandler::query_rewrite(*q);
        assert_eq!(rewritten.len(), 1);
        (rs, rewritten.remove(0))
    }

    #[test]
    fn create_complete_get_release_select1() {
        let (rs, q) = raw_and_query("SELECT 1");
        let mut src = CreateCachedPlan(rs, "SELECT 1", CommandTag::Select);
        assert_eq!(src.magic, CACHEDPLANSOURCE_MAGIC);
        assert!(!src.is_complete);

        CompleteCachedPlan(&mut src, vec![q], &[], 0, None, 0, true);
        assert!(src.is_complete);
        assert!(src.is_valid);
        assert_eq!(src.query_list.len(), 1);

        // No bound params -> generic plan; it is cached on the source for reuse.
        let plan = GetCachedPlan(&mut src, None, None);
        assert_eq!(plan.magic, CACHEDPLAN_MAGIC);
        assert_eq!(plan.stmt_list.len(), 1);
        assert!(src.gplan.is_some());
        assert_eq!(src.num_generic_plans, 1);

        // A second Get reuses the cached generic plan (same Arc).
        let plan2 = GetCachedPlan(&mut src, None, None);
        assert!(Arc::ptr_eq(&plan, &plan2));
        assert_eq!(src.num_generic_plans, 2);

        ReleaseCachedPlan(plan);
        ReleaseCachedPlan(plan2);
        // The source still holds the generic plan after the callers released theirs.
        assert!(src.gplan.is_some());
    }

    #[test]
    fn oneshot_is_custom() {
        let (rs, q) = raw_and_query("SELECT 1");
        let mut src = CreateOneShotCachedPlan(rs, "SELECT 1", CommandTag::Select);
        CompleteCachedPlan(&mut src, vec![q], &[], 0, None, 0, true);
        let plan = GetCachedPlan(&mut src, None, None);
        assert_eq!(plan.stmt_list.len(), 1);
        // A one-shot plan is treated as custom -> not cached on the source.
        assert!(src.gplan.is_none());
        assert_eq!(src.num_custom_plans, 1);
        ReleaseCachedPlan(plan);
    }
}

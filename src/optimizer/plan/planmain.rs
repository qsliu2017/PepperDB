//! optimizer/plan/planmain.c - routines to plan a single basic join query.

use crate::prelude::*;

use crate::IsA;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, Path, PathTarget,
};
use crate::nodes::parsenodes::{Query, RangeTblEntry, RTE_RESULT};
use crate::nodes::primnodes::RangeTblRef;
use crate::nodes::pg_list::{List, list_length, linitial, NIL};

use crate::optimizer::optimizer::{debug_parallel_query, DEBUG_PARALLEL_OFF};

use crate::optimizer::paths::{
    make_one_rel, reconsider_outer_join_clauses, generate_base_implied_equalities,
};
use crate::optimizer::util::placeholder::{
    find_placeholders_in_jointree, fix_placeholder_input_needed_levels,
    add_placeholders_to_base_rels,
};
use crate::optimizer::util::orclauses::extract_restriction_or_clauses;
use crate::optimizer::util::appendinfo::distribute_row_identity_vars;

/// query_pathkeys callback type: `void (*)(PlannerInfo *root, void *extra)`.
pub type query_pathkeys_callback =
    unsafe extern "C" fn(root: *mut PlannerInfo, extra: *mut c_void);

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees.  TODO: replace with real translations.
// ---------------------------------------------------------------------------

unsafe fn setup_simple_rel_arrays(_root: *mut PlannerInfo) { crate::optimizer::util::relnode::setup_simple_rel_arrays(_root) }

unsafe fn build_simple_rel(
    _root: *mut PlannerInfo,
    _relid: c_int,
    _parent: *mut RelOptInfo,
) -> *mut RelOptInfo { crate::optimizer::util::relnode::build_simple_rel(_root, _relid, _parent) }

unsafe fn is_parallel_safe(_root: *mut PlannerInfo, _node: *mut Node) -> bool { crate::optimizer::util::clauses::is_parallel_safe(_root, _node) }

unsafe fn create_group_result_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _target: *mut PathTarget,
    _havingqual: *mut List,
) -> *mut Path { crate::optimizer::util::pathnode::create_group_result_path(_root, _rel, _target, _havingqual) as *mut Path }

unsafe fn add_path(_parent_rel: *mut RelOptInfo, _new_path: *mut Path) { crate::optimizer::util::pathnode::add_path(_parent_rel, _new_path) }

unsafe fn set_cheapest(_parent_rel: *mut RelOptInfo) { crate::optimizer::util::pathnode::set_cheapest(_parent_rel) }

unsafe fn add_base_rels_to_query(_root: *mut PlannerInfo, _jtnode: *mut Node) { crate::optimizer::plan::initsplan::add_base_rels_to_query(_root, _jtnode) }

unsafe fn remove_useless_groupby_columns(_root: *mut PlannerInfo) { crate::optimizer::plan::initsplan::remove_useless_groupby_columns(_root) }

unsafe fn build_base_rel_tlists(_root: *mut PlannerInfo, _final_tlist: *mut List) { crate::optimizer::plan::initsplan::build_base_rel_tlists(_root, _final_tlist) }

unsafe fn find_lateral_references(_root: *mut PlannerInfo) { crate::optimizer::plan::initsplan::find_lateral_references(_root) }

unsafe fn deconstruct_jointree(_root: *mut PlannerInfo) -> *mut List { crate::optimizer::plan::initsplan::deconstruct_jointree(_root) }

unsafe fn remove_useless_joins(_root: *mut PlannerInfo, _joinlist: *mut List) -> *mut List { crate::optimizer::plan::analyzejoins::remove_useless_joins(_root, _joinlist) }

unsafe fn reduce_unique_semijoins(_root: *mut PlannerInfo) { crate::optimizer::plan::analyzejoins::reduce_unique_semijoins(_root) }

unsafe fn remove_useless_self_joins(
    _root: *mut PlannerInfo,
    _joinlist: *mut List,
) -> *mut List { crate::optimizer::plan::analyzejoins::remove_useless_self_joins(_root, _joinlist) }

unsafe fn create_lateral_join_info(_root: *mut PlannerInfo) { crate::optimizer::plan::initsplan::create_lateral_join_info(_root) }

unsafe fn match_foreign_keys_to_quals(_root: *mut PlannerInfo) { crate::optimizer::plan::initsplan::match_foreign_keys_to_quals(_root) }

unsafe fn add_other_rels_to_query(_root: *mut PlannerInfo) { crate::optimizer::plan::initsplan::add_other_rels_to_query(_root) }

// ---------------------------------------------------------------------------
// query_planner
//   Generate a path (that is, a simplified plan) for a basic query, which may
//   involve joins but not any fancier features.
//
// Since query_planner does not handle the toplevel processing (grouping,
// sorting, etc) it cannot select the best path by itself.  Instead, it returns
// the RelOptInfo for the top level of joining, and the caller (grouping_planner)
// can choose among the surviving paths for the rel.
// ---------------------------------------------------------------------------
pub unsafe fn query_planner(
    root: *mut PlannerInfo,
    qp_callback: query_pathkeys_callback,
    qp_extra: *mut c_void,
) -> *mut RelOptInfo {
    let parse: *mut Query = (*root).parse;
    let joinlist: *mut List;
    let final_rel: *mut RelOptInfo;

    /*
     * Init planner lists to empty.
     *
     * NOTE: append_rel_list was set up by subquery_planner, so do not touch
     * here.
     */
    (*root).join_rel_list = NIL;
    (*root).join_rel_hash = null_mut();
    (*root).join_rel_level = null_mut();
    (*root).join_cur_level = 0;
    (*root).canon_pathkeys = NIL;
    (*root).left_join_clauses = NIL;
    (*root).right_join_clauses = NIL;
    (*root).full_join_clauses = NIL;
    (*root).join_info_list = NIL;
    (*root).placeholder_list = NIL;
    (*root).placeholder_array = null_mut();
    (*root).placeholder_array_size = 0;
    (*root).fkey_list = NIL;
    (*root).initial_rels = NIL;

    /*
     * Set up arrays for accessing base relations and AppendRelInfos.
     */
    setup_simple_rel_arrays(root);

    /*
     * In the trivial case where the jointree is a single RTE_RESULT relation,
     * bypass all the rest of this function and just make a RelOptInfo and its
     * one access path.  This is worth optimizing because it applies for common
     * cases like "SELECT expression" and "INSERT ... VALUES()".
     */
    Assert!((*(*parse).jointree).fromlist != NIL);
    if list_length((*(*parse).jointree).fromlist) == 1 {
        let jtnode: *mut Node = linitial((*(*parse).jointree).fromlist) as *mut Node;

        if IsA!(jtnode, T_RangeTblRef) {
            let varno: c_int = (*(jtnode as *mut RangeTblRef)).rtindex;
            let rte: *mut RangeTblEntry = *(*root).simple_rte_array.add(varno as usize);

            Assert!(!rte.is_null());
            if (*rte).rtekind == RTE_RESULT {
                /* Make the RelOptInfo for it directly */
                let final_rel = build_simple_rel(root, varno, null_mut());

                /*
                 * If query allows parallelism in general, check whether the
                 * quals are parallel-restricted.  (We need not check
                 * final_rel->reltarget because it's empty at this point.
                 * Anything parallel-restricted in the query tlist will be
                 * dealt with later.)  We should always do this in a subquery,
                 * since it might be useful to use the subquery in parallel
                 * paths in the parent level.  At top level this is normally not
                 * worth the cycles, because a Result-only plan would never be
                 * interesting to parallelize.  However, if debug_parallel_query
                 * is on, then we want to execute the Result in a parallel
                 * worker if possible, so we must check.
                 */
                if (*(*root).glob).parallelModeOK
                    && ((*root).query_level > 1
                        || debug_parallel_query != DEBUG_PARALLEL_OFF)
                {
                    (*final_rel).consider_parallel =
                        is_parallel_safe(root, (*(*parse).jointree).quals);
                }

                /*
                 * The only path for it is a trivial Result path.  We cheat a
                 * bit here by using a GroupResultPath, because that way we can
                 * just jam the quals into it without preprocessing them.  (But,
                 * if you hold your head at the right angle, a FROM-less SELECT
                 * is a kind of degenerate-grouping case, so it's not that much
                 * of a cheat.)
                 */
                add_path(
                    final_rel,
                    create_group_result_path(
                        root,
                        final_rel,
                        (*final_rel).reltarget,
                        (*(*parse).jointree).quals as *mut List,
                    ),
                );

                /* Select cheapest path (pretty easy in this case...) */
                set_cheapest(final_rel);

                /*
                 * We don't need to run generate_base_implied_equalities, but we
                 * do need to pretend that EC merging is complete.
                 */
                (*root).ec_merging_done = true;

                /*
                 * We still are required to call qp_callback, in case it's
                 * something like "SELECT 2+2 ORDER BY 1".
                 */
                qp_callback(root, qp_extra);

                return final_rel;
            }
        }
    }

    /*
     * Construct RelOptInfo nodes for all base relations used in the query.
     * Appendrel member relations ("other rels") will be added later.
     *
     * Note: the reason we find the baserels by searching the jointree, rather
     * than scanning the rangetable, is that the rangetable may contain RTEs for
     * rels not actively part of the query, for example views.  We don't want to
     * make RelOptInfos for them.
     */
    add_base_rels_to_query(root, (*parse).jointree as *mut Node);

    /* Remove any redundant GROUP BY columns */
    remove_useless_groupby_columns(root);

    /*
     * Examine the targetlist and join tree, adding entries to baserel
     * targetlists for all referenced Vars, and generating PlaceHolderInfo
     * entries for all referenced PlaceHolderVars.  Restrict and join clauses
     * are added to appropriate lists belonging to the mentioned relations. We
     * also build EquivalenceClasses for provably equivalent expressions. The
     * SpecialJoinInfo list is also built to hold information about join order
     * restrictions.  Finally, we form a target joinlist for make_one_rel() to
     * work from.
     */
    build_base_rel_tlists(root, (*root).processed_tlist);

    find_placeholders_in_jointree(root);

    find_lateral_references(root);

    joinlist = deconstruct_jointree(root);

    /*
     * Reconsider any postponed outer-join quals now that we have built up
     * equivalence classes.  (This could result in further additions or
     * mergings of classes.)
     */
    reconsider_outer_join_clauses(root);

    /*
     * If we formed any equivalence classes, generate additional restriction
     * clauses as appropriate.  (Implied join clauses are formed on-the-fly
     * later.)
     */
    generate_base_implied_equalities(root);

    /*
     * We have completed merging equivalence sets, so it's now possible to
     * generate pathkeys in canonical form; so compute query_pathkeys and other
     * pathkeys fields in PlannerInfo.
     */
    qp_callback(root, qp_extra);

    /*
     * Examine any "placeholder" expressions generated during subquery pullup.
     * Make sure that the Vars they need are marked as needed at the relevant
     * join level.  This must be done before join removal because it might cause
     * Vars or placeholders to be needed above a join when they weren't so
     * marked before.
     */
    fix_placeholder_input_needed_levels(root);

    /*
     * Remove any useless outer joins.  Ideally this would be done during
     * jointree preprocessing, but the necessary information isn't available
     * until we've built baserel data structures and classified qual clauses.
     */
    let joinlist = remove_useless_joins(root, joinlist);

    /*
     * Also, reduce any semijoins with unique inner rels to plain inner joins.
     * Likewise, this can't be done until now for lack of needed info.
     */
    reduce_unique_semijoins(root);

    /*
     * Remove self joins on a unique column.
     */
    let joinlist = remove_useless_self_joins(root, joinlist);

    /*
     * Now distribute "placeholders" to base rels as needed.  This has to be
     * done after join removal because removal could change whether a
     * placeholder is evaluable at a base rel.
     */
    add_placeholders_to_base_rels(root);

    /*
     * Construct the lateral reference sets now that we have finalized
     * PlaceHolderVar eval levels.
     */
    create_lateral_join_info(root);

    /*
     * Match foreign keys to equivalence classes and join quals.  This must be
     * done after finalizing equivalence classes, and it's useful to wait till
     * after join removal so that we can skip processing foreign keys involving
     * removed relations.
     */
    match_foreign_keys_to_quals(root);

    /*
     * Look for join OR clauses that we can extract single-relation restriction
     * OR clauses from.
     */
    extract_restriction_or_clauses(root);

    /*
     * Now expand appendrels by adding "otherrels" for their children.  We delay
     * this to the end so that we have as much information as possible available
     * for each baserel, including all restriction clauses.  That let us prune
     * away partitions that don't satisfy a restriction clause. Also note that
     * some information such as lateral_relids is propagated from baserels to
     * otherrels here, so we must have computed it already.
     */
    add_other_rels_to_query(root);

    /*
     * Distribute any UPDATE/DELETE/MERGE row identity variables to the target
     * relations.  This can't be done till we've finished expansion of
     * appendrels.
     */
    distribute_row_identity_vars(root);

    /*
     * Ready to do the primary planning.
     */
    final_rel = make_one_rel(root, joinlist);

    /* Check that we got at least one usable path */
    if final_rel.is_null()
        || (*final_rel).cheapest_total_path.is_null()
        || !(*(*final_rel).cheapest_total_path).param_info.is_null()
    {
        elog!(ERROR, "failed to construct the join relation");
    }

    final_rel
}

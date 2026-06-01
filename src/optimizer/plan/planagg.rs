//! src/backend/optimizer/plan/planagg.c
//!
//! planagg.c
//!   Special planning for aggregate queries.
//!
//! This module tries to replace MIN/MAX aggregate functions by subqueries
//! of the form
//!     (SELECT col FROM tab
//!      WHERE col IS NOT NULL AND existing-quals
//!      ORDER BY col ASC/DESC
//!      LIMIT 1)
//! Given a suitable index on tab.col, this can be much faster than the
//! generic scan-all-the-rows aggregation plan.  We can handle multiple
//! MIN/MAX aggregates by generating multiple subqueries, and their
//! orderings can be different.  However, if the query contains any
//! non-optimizable aggregates, there's no point since we'll have to
//! scan all the rows anyway.
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!   src/backend/optimizer/plan/planagg.c

use crate::prelude::*;
use crate::{IsA, makeNode, foreach, current_cell, castNode, lfirst_node, linitial_node};
use crate::catalog::pg_type_d::INT8OID;

use std::ptr;

use crate::postgres_ext::Oid;
use crate::access::attnum::AttrNumber;

// ----- Local stub types (real definitions live elsewhere) -----

#[allow(non_camel_case_types)]
pub type PlannerInfo = crate::nodes::pathnodes::PlannerInfo;
#[allow(non_camel_case_types)]
pub type Query = crate::nodes::parsenodes::Query;
#[allow(non_camel_case_types)]
pub type FromExpr = crate::nodes::primnodes::FromExpr;
#[allow(non_camel_case_types)]
pub type RangeTblRef = crate::nodes::primnodes::RangeTblRef;
#[allow(non_camel_case_types)]
pub type RangeTblEntry = crate::nodes::parsenodes::RangeTblEntry;
#[allow(non_camel_case_types)]
pub type List = crate::nodes::pg_list::List;
#[allow(non_camel_case_types)]
pub type ListCell = crate::nodes::pg_list::ListCell;
#[allow(non_camel_case_types)]
pub type RelOptInfo = crate::nodes::pathnodes::RelOptInfo;
#[allow(non_camel_case_types)]
pub type MinMaxAggInfo = crate::nodes::pathnodes::MinMaxAggInfo;
#[allow(non_camel_case_types)]
pub type AggInfo = crate::nodes::pathnodes::AggInfo;
#[allow(non_camel_case_types)]
pub type Aggref = crate::nodes::primnodes::Aggref;
#[allow(non_camel_case_types)]
pub type TargetEntry = crate::nodes::primnodes::TargetEntry;
#[allow(non_camel_case_types)]
pub type NullTest = crate::nodes::primnodes::NullTest;
#[allow(non_camel_case_types)]
pub type SortGroupClause = crate::nodes::parsenodes::SortGroupClause;
#[allow(non_camel_case_types)]
pub type Path = crate::nodes::pathnodes::Path;
#[allow(non_camel_case_types)]
pub type Node = crate::nodes::nodes::Node;
#[allow(non_camel_case_types)]
pub type Cost = std::os::raw::c_double;
#[allow(non_camel_case_types)]
pub type HeapTuple = crate::access::htup_details::HeapTuple;
#[allow(non_camel_case_types)]
pub type Form_pg_aggregate = *mut crate::catalog::pg_aggregate::FormData_pg_aggregate;

/*
 * preprocess_minmax_aggregates - preprocess MIN/MAX aggregates
 *
 * Check to see whether the query contains MIN/MAX aggregate functions that
 * might be optimizable via indexscans.  If it does, and all the aggregates
 * are potentially optimizable, then create a MinMaxAggPath and add it to
 * the (UPPERREL_GROUP_AGG, NULL) upperrel.
 *
 * This should be called by grouping_planner() just before it's ready to call
 * query_planner(), because we generate indexscan paths by cloning the
 * planner's state and invoking query_planner() on a modified version of
 * the query parsetree.  Thus, all preprocessing needed before query_planner()
 * must already be done.  This relies on the list of aggregates in
 * root->agginfos, so preprocess_aggrefs() must have been called already, too.
 */
#[allow(unreachable_code)]
pub unsafe fn preprocess_minmax_aggregates(root: *mut PlannerInfo) {
    let parse: *mut Query = (*root).parse;
    let mut jtnode: *mut FromExpr;
    let rtr: *mut RangeTblRef;
    let rte: *mut RangeTblEntry;
    let mut aggs_list: *mut List;
    let grouped_rel: *mut RelOptInfo;

    /* minmax_aggs list should be empty at this point */
    Assert!((*root).minmax_aggs == NIL());

    /* Nothing to do if query has no aggregates */
    if !(*parse).hasAggs {
        return;
    }

    Assert!(!(*parse).setOperations.is_null()); /* shouldn't get here if a setop */
    Assert!((*parse).rowMarks == NIL()); /* nor if FOR UPDATE */

    /*
     * Reject unoptimizable cases.
     *
     * We don't handle GROUP BY or windowing, because our current
     * implementations of grouping require looking at all the rows anyway, and
     * so there's not much point in optimizing MIN/MAX.
     */
    if !(*parse).groupClause.is_null()
        || list_length((*parse).groupingSets) > 1
        || (*parse).hasWindowFuncs
    {
        return;
    }

    /*
     * Reject if query contains any CTEs; there's no way to build an indexscan
     * on one so we couldn't succeed here.  (If the CTEs are unreferenced,
     * that's not true, but it doesn't seem worth expending cycles to check.)
     */
    if !(*parse).cteList.is_null() {
        return;
    }

    /*
     * We also restrict the query to reference exactly one table, since join
     * conditions can't be handled reasonably.  (We could perhaps handle a
     * query containing cartesian-product joins, but it hardly seems worth the
     * trouble.)  However, the single table could be buried in several levels
     * of FromExpr due to subqueries.  Note the "single" table could be an
     * inheritance parent, too, including the case of a UNION ALL subquery
     * that's been flattened to an appendrel.
     */
    jtnode = (*parse).jointree;
    while IsA!(jtnode, T_FromExpr) {
        if list_length((*jtnode).fromlist) != 1 {
            return;
        }
        jtnode = linitial((*jtnode).fromlist) as *mut FromExpr;
    }
    if !IsA!(jtnode, T_RangeTblRef) {
        return;
    }
    rtr = jtnode as *mut RangeTblRef;
    rte = planner_rt_fetch((*rtr).rtindex, root);
    if (*rte).rtekind == RTE_RELATION {
        /* ordinary relation, ok */
    } else if (*rte).rtekind == RTE_SUBQUERY && (*rte).inh {
        /* flattened UNION ALL subquery, ok */
    } else {
        return;
    }

    /*
     * Examine all the aggregates and verify all are MIN/MAX aggregates.  Stop
     * as soon as we find one that isn't.
     */
    aggs_list = NIL();
    if !can_minmax_aggs(root, &mut aggs_list) {
        return;
    }

    /*
     * OK, there is at least the possibility of performing the optimization.
     * Build an access path for each aggregate.  If any of the aggregates
     * prove to be non-indexable, give up; there is no point in optimizing
     * just some of them.
     */
    foreach!(lc, aggs_list, {
        let mminfo: *mut MinMaxAggInfo = lfirst(current_cell!(lc)) as *mut MinMaxAggInfo;
        let eqop: Oid;
        let mut reverse: bool = false;

        /*
         * We'll need the equality operator that goes with the aggregate's
         * ordering operator.
         */
        eqop = get_equality_op_for_ordering_op((*mminfo).aggsortop, &mut reverse);
        if !OidIsValid(eqop) {
            /* shouldn't happen */
            elog!(
                ERROR,
                "could not find equality operator for ordering operator {}",
                (*mminfo).aggsortop
            );
        }

        /*
         * We can use either an ordering that gives NULLS FIRST or one that
         * gives NULLS LAST; furthermore there's unlikely to be much
         * performance difference between them, so it doesn't seem worth
         * costing out both ways if we get a hit on the first one.  NULLS
         * FIRST is more likely to be available if the operator is a
         * reverse-sort operator, so try that first if reverse.
         */
        if build_minmax_path(root, mminfo, eqop, (*mminfo).aggsortop, reverse, reverse) {
            continue;
        }
        if build_minmax_path(root, mminfo, eqop, (*mminfo).aggsortop, reverse, !reverse) {
            continue;
        }

        /* No indexable path for this aggregate, so fail */
        return;
    });

    /*
     * OK, we can do the query this way.  Prepare to create a MinMaxAggPath
     * node.
     *
     * First, create an output Param node for each agg.  (If we end up not
     * using the MinMaxAggPath, we'll waste a PARAM_EXEC slot for each agg,
     * which is not worth worrying about.  We can't wait till create_plan time
     * to decide whether to make the Param, unfortunately.)
     */
    foreach!(lc, aggs_list, {
        let mminfo: *mut MinMaxAggInfo = lfirst(current_cell!(lc)) as *mut MinMaxAggInfo;

        (*mminfo).param = SS_make_initplan_output_param(
            root,
            exprType((*mminfo).target as *mut Node),
            -1,
            exprCollation((*mminfo).target as *mut Node),
        );
    });

    /*
     * Create a MinMaxAggPath node with the appropriate estimated costs and
     * other needed data, and add it to the UPPERREL_GROUP_AGG upperrel, where
     * it will compete against the standard aggregate implementation.  (It
     * will likely always win, but we need not assume that here.)
     *
     * Note: grouping_planner won't have created this upperrel yet, but it's
     * fine for us to create it first.  We will not have inserted the correct
     * consider_parallel value in it, but MinMaxAggPath paths are currently
     * never parallel-safe anyway, so that doesn't matter.  Likewise, it
     * doesn't matter that we haven't filled FDW-related fields in the rel.
     * Also, because there are no rowmarks, we know that the processed_tlist
     * doesn't need to change anymore, so making the pathtarget now is safe.
     */
    grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, ptr::null_mut());
    add_path(
        grouped_rel,
        create_minmaxagg_path(
            root,
            grouped_rel,
            create_pathtarget(root, (*root).processed_tlist),
            aggs_list,
            (*parse).havingQual as *mut List,
        ) as *mut Path,
    );
}

/*
 * can_minmax_aggs
 *		Examine all the aggregates in the query, and check if they are
 *		all MIN/MAX aggregates.  If so, build a list of MinMaxAggInfo
 *		nodes for them.
 *
 * Returns false if a non-MIN/MAX aggregate is found, true otherwise.
 */
unsafe fn can_minmax_aggs(root: *mut PlannerInfo, context: *mut *mut List) -> bool {
    /*
     * This function used to have to scan the query for itself, but now we can
     * just thumb through the AggInfo list made by preprocess_aggrefs.
     */
    foreach!(lc, (*root).agginfos, {
        let agginfo: *mut AggInfo = lfirst_node!(AggInfo, T_AggInfo, current_cell!(lc));
        let aggref: *mut Aggref = linitial_node!(Aggref, T_Aggref, (*agginfo).aggrefs);
        let aggsortop: Oid;
        let curTarget: *mut TargetEntry;
        let mminfo: *mut MinMaxAggInfo;

        Assert!((*aggref).agglevelsup == 0);
        if list_length((*aggref).args) != 1 {
            return false; /* it couldn't be MIN/MAX */
        }

        /*
         * ORDER BY is usually irrelevant for MIN/MAX, but it can change the
         * outcome if the aggsortop's operator class recognizes non-identical
         * values as equal.  For example, 4.0 and 4.00 are equal according to
         * numeric_ops, yet distinguishable.  If MIN() receives more than one
         * value equal to 4.0 and no value less than 4.0, it is unspecified
         * which of those equal values MIN() returns.  An ORDER BY expression
         * that differs for each of those equal values of the argument
         * expression makes the result predictable once again.  This is a
         * niche requirement, and we do not implement it with subquery paths.
         * In any case, this test lets us reject ordered-set aggregates
         * quickly.
         */
        if (*aggref).aggorder != NIL() {
            return false;
        }
        /* note: we do not care if DISTINCT is mentioned ... */

        /*
         * We might implement the optimization when a FILTER clause is present
         * by adding the filter to the quals of the generated subquery.  For
         * now, just punt.
         */
        if !(*aggref).aggfilter.is_null() {
            return false;
        }

        aggsortop = fetch_agg_sort_op((*aggref).aggfnoid);
        if !OidIsValid(aggsortop) {
            return false; /* not a MIN/MAX aggregate */
        }

        curTarget = linitial((*aggref).args) as *mut TargetEntry;

        if contain_mutable_functions((*curTarget).expr as *mut Node) {
            return false; /* not potentially indexable */
        }

        if type_is_rowtype(exprType((*curTarget).expr as *mut Node)) {
            return false; /* IS NOT NULL would have weird semantics */
        }

        mminfo = makeNode!(MinMaxAggInfo, T_MinMaxAggInfo);
        (*mminfo).aggfnoid = (*aggref).aggfnoid;
        (*mminfo).aggsortop = aggsortop;
        (*mminfo).target = (*curTarget).expr;
        (*mminfo).subroot = ptr::null_mut(); /* don't compute path yet */
        (*mminfo).path = ptr::null_mut();
        (*mminfo).pathcost = 0.0;
        (*mminfo).param = ptr::null_mut();

        *context = lappend(*context, mminfo as *mut std::ffi::c_void);
    });
    true
}

/*
 * build_minmax_path
 *		Given a MIN/MAX aggregate, try to build an indexscan Path it can be
 *		optimized with.
 *
 * If successful, stash the best path in *mminfo and return true.
 * Otherwise, return false.
 */
unsafe fn build_minmax_path(
    root: *mut PlannerInfo,
    mminfo: *mut MinMaxAggInfo,
    eqop: Oid,
    sortop: Oid,
    reverse_sort: bool,
    nulls_first: bool,
) -> bool {
    let subroot: *mut PlannerInfo;
    let parse: *mut Query;
    let tle: *mut TargetEntry;
    let tlist: *mut List;
    let ntest: *mut NullTest;
    let sortcl: *mut SortGroupClause;
    let final_rel: *mut RelOptInfo;
    let mut sorted_path: *mut Path;
    let path_cost: Cost;
    let path_fraction: std::os::raw::c_double;

    /*
     * We are going to construct what is effectively a sub-SELECT query, so
     * clone the current query level's state and adjust it to make it look
     * like a subquery.  Any outer references will now be one level higher
     * than before.  (This means that when we are done, there will be no Vars
     * of level 1, which is why the subquery can become an initplan.)
     */
    subroot = palloc(std::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
    ptr::copy_nonoverlapping(root, subroot, 1);
    (*subroot).query_level += 1;
    (*subroot).parent_root = root;
    /* reset subplan-related stuff */
    (*subroot).plan_params = NIL();
    (*subroot).outer_params = ptr::null_mut();
    (*subroot).init_plans = NIL();
    (*subroot).agginfos = NIL();
    (*subroot).aggtransinfos = NIL();

    parse = copyObject(root_parse(root) as *mut Node) as *mut Query;
    (*subroot).parse = parse;
    IncrementVarSublevelsUp(parse as *mut Node, 1, 1);

    /* append_rel_list might contain outer Vars? */
    (*subroot).append_rel_list = copyObject((*root).append_rel_list as *mut Node) as *mut List;
    IncrementVarSublevelsUp((*subroot).append_rel_list as *mut Node, 1, 1);
    /* There shouldn't be any OJ info to translate, as yet */
    Assert!((*subroot).join_info_list == NIL());
    /* and we haven't made equivalence classes, either */
    Assert!((*subroot).eq_classes == NIL());
    /* and we haven't created PlaceHolderInfos, either */
    Assert!((*subroot).placeholder_list == NIL());

    /*----------
     * Generate modified query of the form
     *		(SELECT col FROM tab
     *		 WHERE col IS NOT NULL AND existing-quals
     *		 ORDER BY col ASC/DESC
     *		 LIMIT 1)
     *----------
     */
    /* single tlist entry that is the aggregate target */
    tle = makeTargetEntry(
        copyObject((*mminfo).target as *mut Node) as *mut crate::nodes::primnodes::Expr,
        1 as AttrNumber,
        pstrdup(c"agg_target".as_ptr()),
        false,
    );
    tlist = list_make1(tle as *mut std::ffi::c_void);
    (*parse).targetList = tlist;
    (*subroot).processed_tlist = tlist;

    /* No HAVING, no DISTINCT, no aggregates anymore */
    (*parse).havingQual = ptr::null_mut();
    (*subroot).hasHavingQual = false;
    (*parse).distinctClause = NIL();
    (*parse).hasDistinctOn = false;
    (*parse).hasAggs = false;

    /* Build "target IS NOT NULL" expression */
    ntest = makeNode!(NullTest, T_NullTest);
    (*ntest).nulltesttype = IS_NOT_NULL;
    (*ntest).arg = copyObject((*mminfo).target as *mut Node) as *mut crate::nodes::primnodes::Expr;
    /* we checked it wasn't a rowtype in can_minmax_aggs */
    (*ntest).argisrow = false;
    (*ntest).location = -1;

    /* User might have had that in WHERE already */
    if !list_member((*(*parse).jointree).quals as *mut List, ntest as *mut std::ffi::c_void) {
        (*(*parse).jointree).quals =
            lcons(ntest as *mut std::ffi::c_void, (*(*parse).jointree).quals as *mut List)
                as *mut Node;
    }

    /* Build suitable ORDER BY clause */
    sortcl = makeNode!(SortGroupClause, T_SortGroupClause);
    (*sortcl).tleSortGroupRef = assignSortGroupRef(tle, (*subroot).processed_tlist);
    (*sortcl).eqop = eqop;
    (*sortcl).sortop = sortop;
    (*sortcl).reverse_sort = reverse_sort;
    (*sortcl).nulls_first = nulls_first;
    (*sortcl).hashable = false; /* no need to make this accurate */
    (*parse).sortClause = list_make1(sortcl as *mut std::ffi::c_void);

    /* set up expressions for LIMIT 1 */
    (*parse).limitOffset = ptr::null_mut();
    (*parse).limitCount = makeConst(
        INT8OID,
        -1,
        InvalidOid,
        std::mem::size_of::<i64>() as std::os::raw::c_int,
        Int64GetDatum(1),
        false,
        FLOAT8PASSBYVAL,
    ) as *mut Node;

    /*
     * Generate the best paths for this query, telling query_planner that we
     * have LIMIT 1.
     */
    (*subroot).tuple_fraction = 1.0;
    (*subroot).limit_tuples = 1.0;

    final_rel = query_planner(subroot, Some(minmax_qp_callback), ptr::null_mut());

    /*
     * Since we didn't go through subquery_planner() to handle the subquery,
     * we have to do some of the same cleanup it would do, in particular cope
     * with params and initplans used within this subquery.  (This won't
     * matter if we end up not using the subplan.)
     */
    SS_identify_outer_params(subroot);
    SS_charge_for_initplans(subroot, final_rel);

    /*
     * Get the best presorted path, that being the one that's cheapest for
     * fetching just one row.  If there's no such path, fail.
     */
    if (*final_rel).rows > 1.0 {
        path_fraction = 1.0 / (*final_rel).rows;
    } else {
        path_fraction = 1.0;
    }

    sorted_path = get_cheapest_fractional_path_for_pathkeys(
        (*final_rel).pathlist,
        (*subroot).query_pathkeys,
        ptr::null_mut(),
        path_fraction,
    );
    if sorted_path.is_null() {
        return false;
    }

    /*
     * The path might not return exactly what we want, so fix that.  (We
     * assume that this won't change any conclusions about which was the
     * cheapest path.)
     */
    sorted_path = apply_projection_to_path(
        subroot,
        final_rel,
        sorted_path,
        create_pathtarget(subroot, (*subroot).processed_tlist),
    );

    /*
     * Determine cost to get just the first row of the presorted path.
     *
     * Note: cost calculation here should match
     * compare_fractional_path_costs().
     */
    path_cost = (*sorted_path).startup_cost
        + path_fraction * ((*sorted_path).total_cost - (*sorted_path).startup_cost);

    /* Save state for further processing */
    (*mminfo).subroot = subroot;
    (*mminfo).path = sorted_path;
    (*mminfo).pathcost = path_cost;

    true
}

/*
 * Compute query_pathkeys and other pathkeys during query_planner()
 */
unsafe extern "C" fn minmax_qp_callback(root: *mut PlannerInfo, _extra: *mut std::ffi::c_void) {
    (*root).group_pathkeys = NIL();
    (*root).window_pathkeys = NIL();
    (*root).distinct_pathkeys = NIL();

    (*root).sort_pathkeys = make_pathkeys_for_sortclauses(
        root,
        (*(*root).parse).sortClause,
        (*(*root).parse).targetList,
    );

    (*root).query_pathkeys = (*root).sort_pathkeys;
}

/*
 * Get the OID of the sort operator, if any, associated with an aggregate.
 * Returns InvalidOid if there is no such operator.
 */
unsafe fn fetch_agg_sort_op(aggfnoid: Oid) -> Oid {
    let aggTuple: HeapTuple;
    let aggform: Form_pg_aggregate;
    let aggsortop: Oid;

    /* fetch aggregate entry from pg_aggregate */
    aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
    if !HeapTupleIsValid(aggTuple) {
        return InvalidOid;
    }
    aggform = GETSTRUCT(aggTuple) as Form_pg_aggregate;
    aggsortop = (*aggform).aggsortop;
    ReleaseSysCache(aggTuple);

    aggsortop
}

// ----- Local stubs for unported helpers -----

#[inline]
unsafe fn root_parse(root: *mut PlannerInfo) -> *mut Query {
    (*root).parse
}

unsafe fn list_length(_l: *mut List) -> std::os::raw::c_int {
    unimplemented!() // TODO: src/backend/nodes/list.c
}
unsafe fn linitial(_l: *mut List) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: src/include/nodes/pg_list.h
}
unsafe fn lappend(_l: *mut List, _d: *mut std::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: src/backend/nodes/list.c
}
unsafe fn lcons(_d: *mut std::ffi::c_void, _l: *mut List) -> *mut List {
    unimplemented!() // TODO: src/backend/nodes/list.c
}
unsafe fn list_make1(_d: *mut std::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: src/include/nodes/pg_list.h
}
unsafe fn list_member(_l: *mut List, _d: *mut std::ffi::c_void) -> bool {
    unimplemented!() // TODO: src/backend/nodes/list.c
}
unsafe fn lfirst(_cell: *mut ListCell) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: src/include/nodes/pg_list.h
}
unsafe fn NIL() -> *mut List {
    ptr::null_mut()
}
unsafe fn OidIsValid(_oid: Oid) -> bool {
    unimplemented!() // TODO: src/include/c.h
}
unsafe fn planner_rt_fetch(_rti: std::os::raw::c_int, _root: *mut PlannerInfo) -> *mut RangeTblEntry {
    unimplemented!() // TODO: src/include/parser/parsetree.h
}
unsafe fn get_equality_op_for_ordering_op(_opno: Oid, _reverse: *mut bool) -> Oid {
    unimplemented!() // TODO: src/backend/utils/cache/lsyscache.c
}
unsafe fn SS_make_initplan_output_param(
    _root: *mut PlannerInfo,
    _resulttype: Oid,
    _resulttypmod: std::os::raw::c_int,
    _resultcollation: Oid,
) -> *mut crate::nodes::primnodes::Param {
    unimplemented!() // TODO: src/backend/optimizer/plan/subselect.c
}
unsafe fn exprType(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: src/backend/nodes/nodeFuncs.c
}
unsafe fn exprCollation(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: src/backend/nodes/nodeFuncs.c
}
unsafe fn fetch_upper_rel(
    _root: *mut PlannerInfo,
    _kind: crate::nodes::pathnodes::UpperRelationKind,
    _relids: *mut std::ffi::c_void,
) -> *mut RelOptInfo {
    unimplemented!() // TODO: src/backend/optimizer/util/relnode.c
}
unsafe fn add_path(_parent_rel: *mut RelOptInfo, _new_path: *mut Path) {
    unimplemented!() // TODO: src/backend/optimizer/util/pathnode.c
}
unsafe fn create_minmaxagg_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _target: *mut crate::nodes::pathnodes::PathTarget,
    _mmaggregates: *mut List,
    _quals: *mut List,
) -> *mut crate::nodes::pathnodes::MinMaxAggPath {
    unimplemented!() // TODO: src/backend/optimizer/util/pathnode.c
}
unsafe fn create_pathtarget(
    _root: *mut PlannerInfo,
    _tlist: *mut List,
) -> *mut crate::nodes::pathnodes::PathTarget {
    unimplemented!() // TODO: src/backend/optimizer/util/tlist.c
}
unsafe fn linitial_node_AggInfo() {}
unsafe fn contain_mutable_functions(_clause: *mut Node) -> bool {
    unimplemented!() // TODO: src/backend/optimizer/util/clauses.c
}
unsafe fn type_is_rowtype(_typid: Oid) -> bool {
    unimplemented!() // TODO: src/backend/utils/cache/lsyscache.c
}
unsafe fn copyObject(_from: *mut Node) -> *mut Node {
    unimplemented!() // TODO: src/backend/nodes/copyfuncs.c
}
unsafe fn IncrementVarSublevelsUp(
    _node: *mut Node,
    _delta_sublevels_up: std::os::raw::c_int,
    _min_sublevels_up: std::os::raw::c_int,
) {
    unimplemented!() // TODO: src/backend/rewrite/rewriteManip.c
}
unsafe fn makeTargetEntry(
    _expr: *mut crate::nodes::primnodes::Expr,
    _resno: AttrNumber,
    _resname: *mut std::ffi::c_char,
    _resjunk: bool,
) -> *mut TargetEntry {
    unimplemented!() // TODO: src/backend/nodes/makefuncs.c
}
unsafe fn assignSortGroupRef(_tle: *mut TargetEntry, _tlist: *mut List) -> crate::c::Index {
    unimplemented!() // TODO: src/backend/parser/parse_clause.c
}
unsafe fn makeConst(
    _consttype: Oid,
    _consttypmod: std::os::raw::c_int,
    _constcollid: Oid,
    _constlen: std::os::raw::c_int,
    _constvalue: Datum,
    _constisnull: bool,
    _constbyval: bool,
) -> *mut crate::nodes::primnodes::Const {
    unimplemented!() // TODO: src/backend/nodes/makefuncs.c
}
unsafe fn query_planner(
    _root: *mut PlannerInfo,
    _qp_callback: Option<unsafe extern "C" fn(*mut PlannerInfo, *mut std::ffi::c_void)>,
    _qp_extra: *mut std::ffi::c_void,
) -> *mut RelOptInfo {
    unimplemented!() // TODO: src/backend/optimizer/plan/planmain.c
}
unsafe fn SS_identify_outer_params(_root: *mut PlannerInfo) {
    unimplemented!() // TODO: src/backend/optimizer/plan/subselect.c
}
unsafe fn SS_charge_for_initplans(_root: *mut PlannerInfo, _final_rel: *mut RelOptInfo) {
    unimplemented!() // TODO: src/backend/optimizer/plan/subselect.c
}
unsafe fn get_cheapest_fractional_path_for_pathkeys(
    _paths: *mut List,
    _pathkeys: *mut List,
    _required_outer: *mut std::ffi::c_void,
    _fraction: std::os::raw::c_double,
) -> *mut Path {
    unimplemented!() // TODO: src/backend/optimizer/util/pathkeys.c
}
unsafe fn apply_projection_to_path(
    _root: *mut PlannerInfo,
    _rel: *mut RelOptInfo,
    _path: *mut Path,
    _target: *mut crate::nodes::pathnodes::PathTarget,
) -> *mut Path {
    unimplemented!() // TODO: src/backend/optimizer/util/pathnode.c
}
unsafe fn make_pathkeys_for_sortclauses(
    _root: *mut PlannerInfo,
    _sortclauses: *mut List,
    _tlist: *mut List,
) -> *mut List {
    unimplemented!() // TODO: src/backend/optimizer/path/pathkeys.c
}
unsafe fn SearchSysCache1(_cacheId: std::os::raw::c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(_tuple: HeapTuple) -> bool {
    unimplemented!() // TODO: src/include/access/htup.h
}
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: src/include/access/htup_details.h
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

// Constants used above (stubbed)
#[allow(non_upper_case_globals)]
const RTE_RELATION: crate::nodes::parsenodes::RTEKind = crate::nodes::parsenodes::RTEKind::RTE_RELATION;
#[allow(non_upper_case_globals)]
const RTE_SUBQUERY: crate::nodes::parsenodes::RTEKind = crate::nodes::parsenodes::RTEKind::RTE_SUBQUERY;
#[allow(non_upper_case_globals)]
const UPPERREL_GROUP_AGG: crate::nodes::pathnodes::UpperRelationKind =
    crate::nodes::pathnodes::UpperRelationKind::UPPERREL_GROUP_AGG;
#[allow(non_upper_case_globals)]
const IS_NOT_NULL: crate::nodes::primnodes::NullTestType =
    crate::nodes::primnodes::NullTestType::IS_NOT_NULL;
#[allow(non_upper_case_globals)]
const AGGFNOID: std::os::raw::c_int = 0; // TODO: src/include/utils/syscache.h

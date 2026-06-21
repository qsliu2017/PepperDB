/*-------------------------------------------------------------------------
 *
 * relnode.c / relnode.rs
 *	  Relation-node lookup/construction routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/relnode.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(
    non_snake_case,
    non_upper_case_globals,
    unused_variables,
    dead_code,
    clippy::missing_safety_doc
)]

use crate::prelude::*;

use core::ffi::{c_int, c_void};
use core::mem::size_of;

use crate::nodes::bitmapset::{
    Bitmapset,
    bms_add_member, bms_add_members, bms_copy, bms_del_members, bms_equal, bms_int_members,
    bms_intersect, bms_is_empty, bms_is_member, bms_is_subset, bms_join, bms_make_singleton,
    bms_nonempty_difference, bms_num_members, bms_overlap, bms_union,
};
use crate::nodes::nodes::{Node, NodeTag, JoinType, JoinType::*};
use crate::nodes::parsenodes::{RangeTblEntry, RTEKind::*, PartitionStrategy, PARTITION_STRATEGY_HASH};
use crate::nodes::pathnodes::{
    AppendRelInfo, ParamPathInfo, Path, PlaceHolderInfo, PlaceHolderVar, PlannerInfo,
    RelOptInfo, Relids, RestrictInfo, RowIdentityVarInfo, SpecialJoinInfo,
    UpperRelationKind,
    RelOptKind::*,
    IS_OTHER_REL, PATH_REQ_OUTER,
};
use crate::optimizer::path::costsize::RINFO_IS_PUSHED_DOWN;
use crate::nodes::pg_list::{
    lappend, lappend_oid, lfirst, lsecond, linitial, linitial_oid, list_concat, list_concat_copy,
    list_copy, list_head, list_length, list_make2_impl, list_member_oid, list_nth,
    list_append_unique_ptr, List, NIL,
};
use crate::nodes::primnodes::{
    CoalesceExpr, Expr, OpExpr, RelabelType, Var, ROWID_VAR,
};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHCTL, HASH_COMPARE, HASH_CONTEXT, HASH_ELEM,
    HASH_FUNCTION, HTAB, HASHACTION::{HASH_ENTER, HASH_FIND},
};
use crate::utils::palloc::{palloc0, CurrentMemoryContext};
use crate::nodes::bitmapset::bitmap_hash;
use crate::nodes::bitmapset::bitmap_match;

use crate::c::{OidIsValid, Index, Size};
use crate::nodes::nodes::IS_OUTER_JOIN;
use crate::pg_config_manual::PARTITION_MAX_KEYS;
use crate::access::stratnum::HTEqualStrategyNumber;

use crate::{Assert, IsA, castNode, foreach, current_cell, makeNode, lfirst_node};

/* ----------------------------------------------------------------
 * Imports: other optimizer modules
 * ---------------------------------------------------------------- */

/* optimizer/util/placeholder.h */
use crate::optimizer::util::placeholder::{
    add_placeholders_to_joinrel, find_placeholder_info,
};

/* optimizer/util/tlist.h -- create_empty_pathtarget, copy_pathtarget */
use crate::optimizer::util::tlist::create_empty_pathtarget;

/* optimizer/util/restrictinfo.h -- join_clause_is_movable_into */
use crate::optimizer::util::restrictinfo::join_clause_is_movable_into;

/* optimizer/paths.h -- generate_join_implied_equalities,
 * generate_join_implied_equalities_for_ecs, has_relevant_eclass_joinclause,
 * exprs_known_equal, has_useful_pathkeys, add_child_join_rel_equivalences,
 * mark_dummy_rel */
use crate::optimizer::paths::{
    generate_join_implied_equalities, generate_join_implied_equalities_for_ecs,
    has_relevant_eclass_joinclause, has_useful_pathkeys, add_child_join_rel_equivalences,
    exprs_known_equal, mark_dummy_rel,
};

/* optimizer/util/inherit.h -- apply_child_basequals */
use crate::optimizer::util::inherit::apply_child_basequals;

/* optimizer/util/appendinfo.h -- adjust_appendrel_attrs, adjust_child_relids */
use crate::optimizer::util::appendinfo::{
    adjust_appendrel_attrs, adjust_child_relids,
};

/* optimizer/util/clauses.h -- is_parallel_safe */
use crate::optimizer::util::clauses::is_parallel_safe;

/* optimizer/cost.h -- set_joinrel_size_estimates, get_parameterized_baserel_size,
 * get_parameterized_joinrel_size, enable_partitionwise_join */
use crate::optimizer::cost::{
    set_joinrel_size_estimates, get_parameterized_baserel_size,
    get_parameterized_joinrel_size, enable_partitionwise_join,
};

/* nodes/nodeFuncs.h -- exprCollation, exprType */
use crate::nodes::nodeFuncs::{exprCollation, exprType};
/* nodes/nodes.h -- nodeTag */
use crate::nodes::nodes::nodeTag;

/* rewrite/rewriteManip.h -- remove_nulling_relids */
use crate::rewrite::rewriteManip::remove_nulling_relids;

/* ----------------------------------------------------------------
 * Local types
 * ---------------------------------------------------------------- */

/* JoinHashEntry -- keyed by join_relids Relids, value is RelOptInfo * */
#[repr(C)]
struct JoinHashEntry {
    join_relids: Relids, /* hash key --- MUST BE FIRST */
    join_rel: *mut RelOptInfo,
}

/* ----------------------------------------------------------------
 * Local stubs (genuinely unported dependencies)
 * ---------------------------------------------------------------- */

/* utils/lsyscache.h: op_strict, op_in_opfamily, get_opfamily_member,
 * get_mergejoin_opfamilies */

/// TODO(pg-port): utils/cache/lsyscache.c op_strict
unsafe fn op_strict(opno: Oid) -> bool {
    crate::utils::cache::lsyscache::op_strict(opno)
}

/// TODO(pg-port): utils/cache/lsyscache.c op_in_opfamily
unsafe fn op_in_opfamily(opno: Oid, opfamily: Oid) -> bool {
    crate::utils::cache::lsyscache::op_in_opfamily(opno, opfamily)
}

/// TODO(pg-port): utils/cache/lsyscache.c get_opfamily_member
unsafe fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> Oid {
    crate::utils::cache::lsyscache::get_opfamily_member(opfamily, lefttype, righttype, strategy)
}

/// TODO(pg-port): utils/cache/lsyscache.c get_mergejoin_opfamilies
unsafe fn get_mergejoin_opfamilies(opno: Oid) -> *mut List {
    unimplemented!()
}

/* optimizer/plancat.h -- get_relation_info */

/// TODO(pg-port): optimizer/plancat.c get_relation_info
unsafe fn get_relation_info(
    root: *mut PlannerInfo,
    relationObjectId: Oid,
    inhparent: bool,
    rel: *mut RelOptInfo,
) {
    crate::optimizer::util::plancat::get_relation_info(root, relationObjectId, inhparent, rel)
}

/* rewrite/rowsecurity.h / parser -- getRTEPermissionInfo */

/// TODO(pg-port): rewrite/rowsecurity.c getRTEPermissionInfo
unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::getRTEPermissionInfo(rteperminfos, rte)
}

/* nodes/nodeFuncs.h -- copyObject */

/// TODO(pg-port): nodes/copyfuncs.c copyObject
unsafe fn copyObject<T>(obj: *const T) -> *mut T {
    unimplemented!("TODO(pg-port): copyfuncs copyObject")
}

/* nodes/nodeFuncs.h -- equal */

/// TODO(pg-port): nodes/equalfuncs.c equal
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    crate::nodes::equalfuncs::equal(a, b)
}

/* miscadmin.h -- GetUserId */
use crate::miscadmin::GetUserId;

/* utils/palloc.h -- repalloc0 variants (typed helpers) */

/// palloc0_array(T, n): zeroed allocation of n * size_of::<T>() bytes
unsafe fn palloc0_array_bytes(elem_size: usize, n: usize) -> *mut c_void {
    palloc0(elem_size * n)
}

/// repalloc0_array for RelOptInfo*: grow array from old_size to new_size, zero new slots
unsafe fn repalloc0_array_reloptinfo(
    arr: *mut *mut RelOptInfo,
    old_n: usize,
    new_n: usize,
) -> *mut *mut RelOptInfo {
    let ptr = crate::utils::palloc::repalloc(
        arr as *mut c_void,
        new_n * size_of::<*mut RelOptInfo>(),
    ) as *mut *mut RelOptInfo;
    core::ptr::write_bytes(ptr.add(old_n), 0, (new_n - old_n) * size_of::<*mut RelOptInfo>());
    ptr
}

unsafe fn repalloc0_array_rangetbl(
    arr: *mut *mut RangeTblEntry,
    old_n: usize,
    new_n: usize,
) -> *mut *mut RangeTblEntry {
    let ptr = crate::utils::palloc::repalloc(
        arr as *mut c_void,
        new_n * size_of::<*mut RangeTblEntry>(),
    ) as *mut *mut RangeTblEntry;
    core::ptr::write_bytes(ptr.add(old_n), 0, (new_n - old_n) * size_of::<*mut RangeTblEntry>());
    ptr
}

unsafe fn repalloc0_array_appendrel(
    arr: *mut *mut AppendRelInfo,
    old_n: usize,
    new_n: usize,
) -> *mut *mut AppendRelInfo {
    let ptr = crate::utils::palloc::repalloc(
        arr as *mut c_void,
        new_n * size_of::<*mut AppendRelInfo>(),
    ) as *mut *mut AppendRelInfo;
    core::ptr::write_bytes(ptr.add(old_n), 0, (new_n - old_n) * size_of::<*mut AppendRelInfo>());
    ptr
}

pub use crate::nodes::parsenodes::RTEPermissionInfo;

/* clamp_width_est helper -- from optimizer/plan/planner.c */
/// Clamp a tuple-width estimate to fit in a c_int.
#[inline]
unsafe fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > i32::MAX as i64 {
        i32::MAX
    } else if tuple_width < 0 {
        0
    } else {
        tuple_width as i32
    }
}

/* ================================================================
 * setup_simple_rel_arrays
 * ================================================================ */

/*
 * setup_simple_rel_arrays
 *	  Prepare the arrays we use for quickly accessing base relations
 *	  and AppendRelInfos.
 */
pub unsafe fn setup_simple_rel_arrays(root: *mut PlannerInfo) {
    let size: c_int;
    let mut rti: Index;
    let lc: *mut crate::nodes::pg_list::ListCell;

    /* Arrays are accessed using RT indexes (1..N) */
    size = list_length((*(*root).parse).rtable) + 1;
    (*root).simple_rel_array_size = size;

    /*
     * simple_rel_array is initialized to all NULLs, since no RelOptInfos
     * exist yet.  It'll be filled by later calls to build_simple_rel().
     */
    (*root).simple_rel_array = palloc0(
        (size as usize) * size_of::<*mut RelOptInfo>()
    ) as *mut *mut RelOptInfo;

    /* simple_rte_array is an array equivalent of the rtable list */
    (*root).simple_rte_array = palloc0(
        (size as usize) * size_of::<*mut RangeTblEntry>()
    ) as *mut *mut RangeTblEntry;

    rti = 1;
    foreach!(lc2, (*(*root).parse).rtable, {
        let rte = lfirst(crate::current_cell!(lc2)) as *mut RangeTblEntry;
        *(*root).simple_rte_array.add(rti as usize) = rte;
        rti += 1;
    });

    /* append_rel_array is not needed if there are no AppendRelInfos */
    if (*root).append_rel_list.is_null() {
        (*root).append_rel_array = core::ptr::null_mut();
        return;
    }

    (*root).append_rel_array = palloc0(
        (size as usize) * size_of::<*mut AppendRelInfo>()
    ) as *mut *mut AppendRelInfo;

    /*
     * append_rel_array is filled with any already-existing AppendRelInfos,
     * which currently could only come from UNION ALL flattening.  We might
     * add more later during inheritance expansion, but it's the
     * responsibility of the expansion code to update the array properly.
     */
    foreach!(lc3, (*root).append_rel_list, {
        let appinfo = lfirst_node!(AppendRelInfo, T_AppendRelInfo, crate::current_cell!(lc3));
        let child_relid = (*appinfo).child_relid as usize;

        /* Sanity check */
        Assert!(child_relid < size as usize);

        if !(*(*root).append_rel_array.add(child_relid)).is_null() {
            elog!(ERROR, "child relation already exists");
        }

        *(*root).append_rel_array.add(child_relid) = appinfo;
    });
}

/*
 * expand_planner_arrays
 *		Expand the PlannerInfo's per-RTE arrays by add_size members
 *		and initialize the newly added entries to NULLs
 *
 * Note: this causes the append_rel_array to become allocated even if
 * it was not before.  This is okay for current uses, because we only call
 * this when adding child relations, which always have AppendRelInfos.
 */
pub unsafe fn expand_planner_arrays(root: *mut PlannerInfo, add_size: c_int) {
    let new_size: c_int;

    Assert!(add_size > 0);

    new_size = (*root).simple_rel_array_size + add_size;

    (*root).simple_rel_array = repalloc0_array_reloptinfo(
        (*root).simple_rel_array,
        (*root).simple_rel_array_size as usize,
        new_size as usize,
    );

    (*root).simple_rte_array = repalloc0_array_rangetbl(
        (*root).simple_rte_array,
        (*root).simple_rel_array_size as usize,
        new_size as usize,
    );

    if !(*root).append_rel_array.is_null() {
        (*root).append_rel_array = repalloc0_array_appendrel(
            (*root).append_rel_array,
            (*root).simple_rel_array_size as usize,
            new_size as usize,
        );
    } else {
        (*root).append_rel_array = palloc0(
            (new_size as usize) * size_of::<*mut AppendRelInfo>()
        ) as *mut *mut AppendRelInfo;
    }

    (*root).simple_rel_array_size = new_size;
}

/*
 * build_simple_rel
 *	  Construct a new RelOptInfo for a base relation or 'other' relation.
 */
pub unsafe fn build_simple_rel(
    root: *mut PlannerInfo,
    relid: c_int,
    parent: *mut RelOptInfo,
) -> *mut RelOptInfo {
    let rel: *mut RelOptInfo;
    let rte: *mut RangeTblEntry;

    /* Rel should not exist already */
    Assert!(relid > 0 && relid < (*root).simple_rel_array_size);
    if !(*(*root).simple_rel_array.add(relid as usize)).is_null() {
        elog!(ERROR, "rel {} already exists", relid);
    }

    /* Fetch RTE for relation */
    rte = *(*root).simple_rte_array.add(relid as usize);
    Assert!(!rte.is_null());

    rel = makeNode!(RelOptInfo, T_RelOptInfo);
    (*rel).reloptkind = if !parent.is_null() { RELOPT_OTHER_MEMBER_REL } else { RELOPT_BASEREL };
    (*rel).relids = bms_make_singleton(relid);
    (*rel).rows = 0.0;
    /* cheap startup cost is interesting iff not all tuples to be retrieved */
    (*rel).consider_startup = (*root).tuple_fraction > 0.0;
    (*rel).consider_param_startup = false; /* might get changed later */
    (*rel).consider_parallel = false; /* might get changed later */
    (*rel).reltarget = create_empty_pathtarget();
    (*rel).pathlist = NIL;
    (*rel).ppilist = NIL;
    (*rel).partial_pathlist = NIL;
    (*rel).cheapest_startup_path = core::ptr::null_mut();
    (*rel).cheapest_total_path = core::ptr::null_mut();
    (*rel).cheapest_unique_path = core::ptr::null_mut();
    (*rel).cheapest_parameterized_paths = NIL;
    (*rel).relid = relid as Index;
    (*rel).rtekind = (*rte).rtekind;
    /* min_attr, max_attr, attr_needed, attr_widths are set below */
    (*rel).notnullattnums = core::ptr::null_mut();
    (*rel).lateral_vars = NIL;
    (*rel).indexlist = NIL;
    (*rel).statlist = NIL;
    (*rel).pages = 0;
    (*rel).tuples = 0.0;
    (*rel).allvisfrac = 0.0;
    (*rel).eclass_indexes = core::ptr::null_mut();
    (*rel).subroot = core::ptr::null_mut();
    (*rel).subplan_params = NIL;
    (*rel).rel_parallel_workers = -1; /* set up in get_relation_info */
    (*rel).amflags = 0;
    (*rel).serverid = InvalidOid;
    if (*rte).rtekind == RTE_RELATION {
        Assert!(
            parent.is_null()
                || (*parent).rtekind == RTE_RELATION
                || (*parent).rtekind == RTE_SUBQUERY
        );

        /*
         * For any RELATION rte, we need a userid with which to check
         * permission access. Baserels simply use their own
         * RTEPermissionInfo's checkAsUser.
         *
         * For otherrels normally there's no RTEPermissionInfo, so we use the
         * parent's, which normally has one. The exceptional case is that the
         * parent is a subquery, in which case the otherrel will have its own.
         */
        if (*rel).reloptkind == RELOPT_BASEREL
            || ((*rel).reloptkind == RELOPT_OTHER_MEMBER_REL
                && (*parent).rtekind == RTE_SUBQUERY)
        {
            let perminfo = getRTEPermissionInfo((*(*root).parse).rteperminfos, rte);
            (*rel).userid = (*perminfo).checkAsUser;
        } else {
            (*rel).userid = (*parent).userid;
        }
    } else {
        (*rel).userid = InvalidOid;
    }
    (*rel).useridiscurrent = false;
    (*rel).fdwroutine = core::ptr::null_mut();
    (*rel).fdw_private = core::ptr::null_mut();
    (*rel).unique_for_rels = NIL;
    (*rel).non_unique_for_rels = NIL;
    (*rel).baserestrictinfo = NIL;
    (*rel).baserestrictcost.startup = 0.0;
    (*rel).baserestrictcost.per_tuple = 0.0;
    (*rel).baserestrict_min_security = u32::MAX;
    (*rel).joininfo = NIL;
    (*rel).has_eclass_joins = false;
    (*rel).consider_partitionwise_join = false; /* might get changed later */
    (*rel).part_scheme = core::ptr::null_mut();
    (*rel).nparts = -1;
    (*rel).boundinfo = core::ptr::null_mut();
    (*rel).partbounds_merged = false;
    (*rel).partition_qual = NIL;
    (*rel).part_rels = core::ptr::null_mut();
    (*rel).live_parts = core::ptr::null_mut();
    (*rel).all_partrels = core::ptr::null_mut();
    (*rel).partexprs = core::ptr::null_mut();
    (*rel).nullable_partexprs = core::ptr::null_mut();

    /*
     * Pass assorted information down the inheritance hierarchy.
     */
    if !parent.is_null() {
        /* We keep back-links to immediate parent and topmost parent. */
        (*rel).parent = parent;
        (*rel).top_parent = if !(*parent).top_parent.is_null() {
            (*parent).top_parent
        } else {
            parent
        };
        (*rel).top_parent_relids = (*(*rel).top_parent).relids;

        /*
         * A child rel is below the same outer joins as its parent.  (We
         * presume this info was already calculated for the parent.)
         */
        (*rel).nulling_relids = (*parent).nulling_relids;

        /*
         * Also propagate lateral-reference information from appendrel parent
         * rels to their child rels.  We intentionally give each child rel the
         * same minimum parameterization, even though it's quite possible that
         * some don't reference all the lateral rels.  This is because any
         * append path for the parent will have to have the same
         * parameterization for every child anyway, and there's no value in
         * forcing extra reparameterize_path() calls.  Similarly, a lateral
         * reference to the parent prevents use of otherwise-movable join rels
         * for each child.
         *
         * It's possible for child rels to have their own children, in which
         * case the topmost parent's lateral info propagates all the way down.
         */
        (*rel).direct_lateral_relids = (*parent).direct_lateral_relids;
        (*rel).lateral_relids = (*parent).lateral_relids;
        (*rel).lateral_referencers = (*parent).lateral_referencers;
    } else {
        (*rel).parent = core::ptr::null_mut();
        (*rel).top_parent = core::ptr::null_mut();
        (*rel).top_parent_relids = core::ptr::null_mut();
        (*rel).nulling_relids = core::ptr::null_mut();
        (*rel).direct_lateral_relids = core::ptr::null_mut();
        (*rel).lateral_relids = core::ptr::null_mut();
        (*rel).lateral_referencers = core::ptr::null_mut();
    }

    /* Check type of rtable entry */
    match (*rte).rtekind {
        RTE_RELATION => {
            /* Table --- retrieve statistics from the system catalogs */
            get_relation_info(root, (*rte).relid, (*rte).inh, rel);
        }
        RTE_SUBQUERY | RTE_FUNCTION | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE
        | RTE_NAMEDTUPLESTORE => {
            /*
             * Subquery, function, tablefunc, values list, CTE, or ENR --- set
             * up attr range and arrays
             *
             * Note: 0 is included in range to support whole-row Vars
             */
            (*rel).min_attr = 0;
            (*rel).max_attr = list_length((*(*rte).eref).colnames) as crate::access::attnum::AttrNumber;
            (*rel).attr_needed = palloc0(
                (((*rel).max_attr - (*rel).min_attr + 1) as usize) * size_of::<Relids>()
            ) as *mut Relids;
            (*rel).attr_widths = palloc0(
                (((*rel).max_attr - (*rel).min_attr + 1) as usize) * size_of::<i32>()
            ) as *mut i32;
        }
        RTE_RESULT => {
            /* RTE_RESULT has no columns, nor could it have whole-row Var */
            (*rel).min_attr = 0;
            (*rel).max_attr = -1;
            (*rel).attr_needed = core::ptr::null_mut();
            (*rel).attr_widths = core::ptr::null_mut();
        }
        _ => {
            elog!(ERROR, "unrecognized RTE kind: {}", (*rte).rtekind as c_int);
        }
    }

    /*
     * We must apply the partially filled in RelOptInfo before calling
     * apply_child_basequals due to some transformations within that function
     * which require the RelOptInfo to be available in the simple_rel_array.
     */
    *(*root).simple_rel_array.add(relid as usize) = rel;

    /*
     * Apply the parent's quals to the child, with appropriate substitution of
     * variables.  If the resulting clause is constant-FALSE or NULL after
     * applying transformations, apply_child_basequals returns false to
     * indicate that scanning this relation won't yield any rows.  In this
     * case, we mark the child as dummy right away.  (We must do this
     * immediately so that pruning works correctly when recursing in
     * expand_partitioned_rtentry.)
     */
    if !parent.is_null() {
        let appinfo = *(*root).append_rel_array.add(relid as usize);

        Assert!(!appinfo.is_null());
        if !apply_child_basequals(root, parent, rel, rte, appinfo) {
            /*
             * Restriction clause reduced to constant FALSE or NULL.  Mark as
             * dummy so we won't scan this relation.
             */
            mark_dummy_rel(rel);
        }
    }

    rel
}

/*
 * find_base_rel
 *	  Find a base or otherrel relation entry, which must already exist.
 */
pub unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
    let rel: *mut RelOptInfo;

    /* use an unsigned comparison to prevent negative array element access */
    if (relid as u32) < ((*root).simple_rel_array_size as u32) {
        rel = *(*root).simple_rel_array.add(relid as usize);
        if !rel.is_null() {
            return rel;
        }
    }

    elog!(ERROR, "no relation entry for relid {}", relid);

    core::ptr::null_mut() /* keep compiler quiet */
}

/*
 * find_base_rel_noerr
 *	  Find a base or otherrel relation entry, returning NULL if there's none
 */
pub unsafe fn find_base_rel_noerr(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
    /* use an unsigned comparison to prevent negative array element access */
    if (relid as u32) < ((*root).simple_rel_array_size as u32) {
        return *(*root).simple_rel_array.add(relid as usize);
    }
    core::ptr::null_mut()
}

/*
 * find_base_rel_ignore_join
 *	  Find a base or otherrel relation entry, which must already exist.
 *
 * Unlike find_base_rel, if relid references an outer join then this
 * will return NULL rather than raising an error.  This is convenient
 * for callers that must deal with relid sets including both base and
 * outer joins.
 */
pub unsafe fn find_base_rel_ignore_join(
    root: *mut PlannerInfo,
    relid: c_int,
) -> *mut RelOptInfo {
    /* use an unsigned comparison to prevent negative array element access */
    if (relid as u32) < ((*root).simple_rel_array_size as u32) {
        let rel: *mut RelOptInfo;
        let rte: *mut RangeTblEntry;

        rel = *(*root).simple_rel_array.add(relid as usize);
        if !rel.is_null() {
            return rel;
        }

        /*
         * We could just return NULL here, but for debugging purposes it seems
         * best to actually verify that the relid is an outer join and not
         * something weird.
         */
        rte = *(*root).simple_rte_array.add(relid as usize);
        if !rte.is_null() && (*rte).rtekind == RTE_JOIN && (*rte).jointype != JOIN_INNER {
            return core::ptr::null_mut();
        }
    }

    elog!(ERROR, "no relation entry for relid {}", relid);

    core::ptr::null_mut() /* keep compiler quiet */
}

/*
 * build_join_rel_hash
 *	  Construct the auxiliary hash table for join relations.
 */
unsafe fn build_join_rel_hash(root: *mut PlannerInfo) {
    let hashtab: *mut HTAB;
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    /* Create the hash table */
    hash_ctl.keysize = size_of::<Relids>();
    hash_ctl.entrysize = size_of::<JoinHashEntry>();
    hash_ctl.hash = Some(bitmap_hash);
    hash_ctl.r#match = Some(bitmap_match);
    hash_ctl.hcxt = CurrentMemoryContext;
    let hashtab = hash_create(
        c"JoinRelHashTable".as_ptr(),
        256,
        &hash_ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT,
    );

    /* Insert all the already-existing joinrels */
    foreach!(l, (*root).join_rel_list, {
        let rel = lfirst(crate::current_cell!(l)) as *mut RelOptInfo;
        let mut found: bool = false;

        let hentry = hash_search(
            hashtab,
            &(*rel).relids as *const Relids as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut JoinHashEntry;
        Assert!(!found);
        (*hentry).join_rel = rel;
    });

    (*root).join_rel_hash = hashtab as *mut crate::nodes::pathnodes::HTAB;
}

/*
 * find_join_rel
 *	  Returns relation entry corresponding to 'relids' (a set of RT indexes),
 *	  or NULL if none exists.  This is for join relations.
 */
pub unsafe fn find_join_rel(root: *mut PlannerInfo, relids: Relids) -> *mut RelOptInfo {
    /*
     * Switch to using hash lookup when list grows "too long".  The threshold
     * is arbitrary and is known only here.
     */
    if (*root).join_rel_hash.is_null() && list_length((*root).join_rel_list) > 32 {
        build_join_rel_hash(root);
    }

    /*
     * Use either hashtable lookup or linear search, as appropriate.
     *
     * Note: the seemingly redundant hashkey variable is used to avoid taking
     * the address of relids; unless the compiler is exceedingly smart, doing
     * so would force relids out of a register and thus probably slow down the
     * list-search case.
     */
    if !(*root).join_rel_hash.is_null() {
        let hashkey: Relids = relids;
        let hentry = hash_search(
            (*root).join_rel_hash as *mut HTAB,
            &hashkey as *const Relids as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut JoinHashEntry;
        if !hentry.is_null() {
            return (*hentry).join_rel;
        }
    } else {
        foreach!(l, (*root).join_rel_list, {
            let rel = lfirst(crate::current_cell!(l)) as *mut RelOptInfo;

            if bms_equal((*rel).relids, relids) {
                return rel;
            }
        });
    }

    core::ptr::null_mut()
}

/*
 * set_foreign_rel_properties
 *		Set up foreign-join fields if outer and inner relation are foreign
 *		tables (or joins) belonging to the same server and assigned to the same
 *		user to check access permissions as.
 *
 * In addition to an exact match of userid, we allow the case where one side
 * has zero userid (implying current user) and the other side has explicit
 * userid that happens to equal the current user; but in that case, pushdown of
 * the join is only valid for the current user.  The useridiscurrent field
 * records whether we had to make such an assumption for this join or any
 * sub-join.
 *
 * Otherwise these fields are left invalid, so GetForeignJoinPaths will not be
 * called for the join relation.
 */
unsafe fn set_foreign_rel_properties(
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
) {
    if OidIsValid((*outer_rel).serverid) && (*inner_rel).serverid == (*outer_rel).serverid {
        if (*inner_rel).userid == (*outer_rel).userid {
            (*joinrel).serverid = (*outer_rel).serverid;
            (*joinrel).userid = (*outer_rel).userid;
            (*joinrel).useridiscurrent =
                (*outer_rel).useridiscurrent || (*inner_rel).useridiscurrent;
            (*joinrel).fdwroutine = (*outer_rel).fdwroutine;
        } else if !OidIsValid((*inner_rel).userid) && (*outer_rel).userid == GetUserId() {
            (*joinrel).serverid = (*outer_rel).serverid;
            (*joinrel).userid = (*outer_rel).userid;
            (*joinrel).useridiscurrent = true;
            (*joinrel).fdwroutine = (*outer_rel).fdwroutine;
        } else if !OidIsValid((*outer_rel).userid) && (*inner_rel).userid == GetUserId() {
            (*joinrel).serverid = (*outer_rel).serverid;
            (*joinrel).userid = (*inner_rel).userid;
            (*joinrel).useridiscurrent = true;
            (*joinrel).fdwroutine = (*outer_rel).fdwroutine;
        }
    }
}

/*
 * add_join_rel
 *		Add given join relation to the list of join relations in the given
 *		PlannerInfo. Also add it to the auxiliary hashtable if there is one.
 */
unsafe fn add_join_rel(root: *mut PlannerInfo, joinrel: *mut RelOptInfo) {
    /* GEQO requires us to append the new joinrel to the end of the list! */
    (*root).join_rel_list = lappend((*root).join_rel_list, joinrel as *mut c_void);

    /* store it into the auxiliary hashtable if there is one. */
    if !(*root).join_rel_hash.is_null() {
        let mut found: bool = false;

        let hentry = hash_search(
            (*root).join_rel_hash as *mut HTAB,
            &(*joinrel).relids as *const Relids as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut JoinHashEntry;
        Assert!(!found);
        (*hentry).join_rel = joinrel;
    }
}

/*
 * build_join_rel
 *	  Returns relation entry corresponding to the union of two given rels,
 *	  creating a new relation entry if none already exists.
 *
 * 'joinrelids' is the Relids set that uniquely identifies the join
 * 'outer_rel' and 'inner_rel' are relation nodes for the relations to be
 *		joined
 * 'sjinfo': join context info
 * 'pushed_down_joins': any pushed-down outer joins that are now completed
 * 'restrictlist_ptr': result variable.  If not NULL, *restrictlist_ptr
 *		receives the list of RestrictInfo nodes that apply to this
 *		particular pair of joinable relations.
 *
 * restrictlist_ptr makes the routine's API a little grotty, but it saves
 * duplicated calculation of the restrictlist...
 */
pub unsafe fn build_join_rel(
    root: *mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
    pushed_down_joins: *mut List,
    restrictlist_ptr: *mut *mut List,
) -> *mut RelOptInfo {
    let joinrel: *mut RelOptInfo;
    let restrictlist: *mut List;

    /* This function should be used only for join between parents. */
    Assert!(!IS_OTHER_REL(outer_rel) && !IS_OTHER_REL(inner_rel));

    /*
     * See if we already have a joinrel for this set of base rels.
     */
    joinrel = find_join_rel(root, joinrelids);

    if !joinrel.is_null() {
        /*
         * Yes, so we only need to figure the restrictlist for this particular
         * pair of component relations.
         */
        if !restrictlist_ptr.is_null() {
            *restrictlist_ptr = build_joinrel_restrictlist(
                root, joinrel, outer_rel, inner_rel, sjinfo,
            );
        }
        return joinrel;
    }

    /*
     * Nope, so make one.
     */
    let joinrel = makeNode!(RelOptInfo, T_RelOptInfo);
    (*joinrel).reloptkind = RELOPT_JOINREL;
    (*joinrel).relids = bms_copy(joinrelids);
    (*joinrel).rows = 0.0;
    /* cheap startup cost is interesting iff not all tuples to be retrieved */
    (*joinrel).consider_startup = (*root).tuple_fraction > 0.0;
    (*joinrel).consider_param_startup = false;
    (*joinrel).consider_parallel = false;
    (*joinrel).reltarget = create_empty_pathtarget();
    (*joinrel).pathlist = NIL;
    (*joinrel).ppilist = NIL;
    (*joinrel).partial_pathlist = NIL;
    (*joinrel).cheapest_startup_path = core::ptr::null_mut();
    (*joinrel).cheapest_total_path = core::ptr::null_mut();
    (*joinrel).cheapest_unique_path = core::ptr::null_mut();
    (*joinrel).cheapest_parameterized_paths = NIL;
    /* init direct_lateral_relids from children; we'll finish it up below */
    (*joinrel).direct_lateral_relids = bms_union(
        (*outer_rel).direct_lateral_relids,
        (*inner_rel).direct_lateral_relids,
    );
    (*joinrel).lateral_relids =
        min_join_parameterization(root, (*joinrel).relids, outer_rel, inner_rel);
    (*joinrel).relid = 0; /* indicates not a baserel */
    (*joinrel).rtekind = RTE_JOIN;
    (*joinrel).min_attr = 0;
    (*joinrel).max_attr = 0;
    (*joinrel).attr_needed = core::ptr::null_mut();
    (*joinrel).attr_widths = core::ptr::null_mut();
    (*joinrel).notnullattnums = core::ptr::null_mut();
    (*joinrel).nulling_relids = core::ptr::null_mut();
    (*joinrel).lateral_vars = NIL;
    (*joinrel).lateral_referencers = core::ptr::null_mut();
    (*joinrel).indexlist = NIL;
    (*joinrel).statlist = NIL;
    (*joinrel).pages = 0;
    (*joinrel).tuples = 0.0;
    (*joinrel).allvisfrac = 0.0;
    (*joinrel).eclass_indexes = core::ptr::null_mut();
    (*joinrel).subroot = core::ptr::null_mut();
    (*joinrel).subplan_params = NIL;
    (*joinrel).rel_parallel_workers = -1;
    (*joinrel).amflags = 0;
    (*joinrel).serverid = InvalidOid;
    (*joinrel).userid = InvalidOid;
    (*joinrel).useridiscurrent = false;
    (*joinrel).fdwroutine = core::ptr::null_mut();
    (*joinrel).fdw_private = core::ptr::null_mut();
    (*joinrel).unique_for_rels = NIL;
    (*joinrel).non_unique_for_rels = NIL;
    (*joinrel).baserestrictinfo = NIL;
    (*joinrel).baserestrictcost.startup = 0.0;
    (*joinrel).baserestrictcost.per_tuple = 0.0;
    (*joinrel).baserestrict_min_security = u32::MAX;
    (*joinrel).joininfo = NIL;
    (*joinrel).has_eclass_joins = false;
    (*joinrel).consider_partitionwise_join = false; /* might get changed later */
    (*joinrel).parent = core::ptr::null_mut();
    (*joinrel).top_parent = core::ptr::null_mut();
    (*joinrel).top_parent_relids = core::ptr::null_mut();
    (*joinrel).part_scheme = core::ptr::null_mut();
    (*joinrel).nparts = -1;
    (*joinrel).boundinfo = core::ptr::null_mut();
    (*joinrel).partbounds_merged = false;
    (*joinrel).partition_qual = NIL;
    (*joinrel).part_rels = core::ptr::null_mut();
    (*joinrel).live_parts = core::ptr::null_mut();
    (*joinrel).all_partrels = core::ptr::null_mut();
    (*joinrel).partexprs = core::ptr::null_mut();
    (*joinrel).nullable_partexprs = core::ptr::null_mut();

    /* Compute information relevant to the foreign relations. */
    set_foreign_rel_properties(joinrel, outer_rel, inner_rel);

    /*
     * Fill the joinrel's tlist with just the Vars and PHVs that need to be
     * output from this join (ie, are needed for higher joinclauses or final
     * output).
     *
     * NOTE: the tlist order for a join rel will depend on which pair of outer
     * and inner rels we first try to build it from.  But the contents should
     * be the same regardless.
     */
    build_joinrel_tlist(
        root,
        joinrel,
        outer_rel,
        sjinfo,
        pushed_down_joins,
        (*sjinfo).jointype == JOIN_FULL,
    );
    build_joinrel_tlist(
        root,
        joinrel,
        inner_rel,
        sjinfo,
        pushed_down_joins,
        (*sjinfo).jointype != JOIN_INNER,
    );
    add_placeholders_to_joinrel(root, joinrel, outer_rel, inner_rel, sjinfo);

    /*
     * add_placeholders_to_joinrel also took care of adding the ph_lateral
     * sets of any PlaceHolderVars computed here to direct_lateral_relids, so
     * now we can finish computing that.  This is much like the computation of
     * the transitively-closed lateral_relids in min_join_parameterization,
     * except that here we *do* have to consider the added PHVs.
     */
    (*joinrel).direct_lateral_relids =
        bms_del_members((*joinrel).direct_lateral_relids, (*joinrel).relids);

    /*
     * Construct restrict and join clause lists for the new joinrel. (The
     * caller might or might not need the restrictlist, but I need it anyway
     * for set_joinrel_size_estimates().)
     */
    let restrictlist = build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel, sjinfo);
    if !restrictlist_ptr.is_null() {
        *restrictlist_ptr = restrictlist;
    }
    build_joinrel_joinlist(joinrel, outer_rel, inner_rel);

    /*
     * This is also the right place to check whether the joinrel has any
     * pending EquivalenceClass joins.
     */
    (*joinrel).has_eclass_joins = has_relevant_eclass_joinclause(root, joinrel);

    /* Store the partition information. */
    build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    /*
     * Set estimates of the joinrel's size.
     */
    set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    /*
     * Set the consider_parallel flag if this joinrel could potentially be
     * scanned within a parallel worker.  If this flag is false for either
     * inner_rel or outer_rel, then it must be false for the joinrel also.
     * Even if both are true, there might be parallel-restricted expressions
     * in the targetlist or quals.
     *
     * Note that if there are more than two rels in this relation, they could
     * be divided between inner_rel and outer_rel in any arbitrary way.  We
     * assume this doesn't matter, because we should hit all the same baserels
     * and joinclauses while building up to this joinrel no matter which we
     * take; therefore, we should make the same decision here however we get
     * here.
     */
    if (*inner_rel).consider_parallel
        && (*outer_rel).consider_parallel
        && is_parallel_safe(root, restrictlist as *mut Node)
        && is_parallel_safe(root, (*(*joinrel).reltarget).exprs as *mut Node)
    {
        (*joinrel).consider_parallel = true;
    }

    /* Add the joinrel to the PlannerInfo. */
    add_join_rel(root, joinrel);

    /*
     * Also, if dynamic-programming join search is active, add the new joinrel
     * to the appropriate sublist.  Note: you might think the Assert on number
     * of members should be for equality, but some of the level 1 rels might
     * have been joinrels already, so we can only assert <=.
     */
    if !(*root).join_rel_level.is_null() {
        Assert!((*root).join_cur_level > 0);
        Assert!((*root).join_cur_level <= bms_num_members((*joinrel).relids));
        let cur = (*root).join_cur_level as usize;
        *(*root).join_rel_level.add(cur) = lappend(
            *(*root).join_rel_level.add(cur),
            joinrel as *mut c_void,
        );
    }

    joinrel
}

/*
 * build_child_join_rel
 *	  Builds RelOptInfo representing join between given two child relations.
 *
 * 'outer_rel' and 'inner_rel' are the RelOptInfos of child relations being
 *		joined
 * 'parent_joinrel' is the RelOptInfo representing the join between parent
 *		relations. Some of the members of new RelOptInfo are produced by
 *		translating corresponding members of this RelOptInfo
 * 'restrictlist': list of RestrictInfo nodes that apply to this particular
 *		pair of joinable relations
 * 'sjinfo': child join's join-type details
 * 'nappinfos' and 'appinfos': AppendRelInfo array for child relids
 */
pub unsafe fn build_child_join_rel(
    root: *mut PlannerInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    parent_joinrel: *mut RelOptInfo,
    restrictlist: *mut List,
    sjinfo: *mut SpecialJoinInfo,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) -> *mut RelOptInfo {
    let joinrel = makeNode!(RelOptInfo, T_RelOptInfo);

    /* Only joins between "other" relations land here. */
    Assert!(IS_OTHER_REL(outer_rel) && IS_OTHER_REL(inner_rel));

    /* The parent joinrel should have consider_partitionwise_join set. */
    Assert!((*parent_joinrel).consider_partitionwise_join);

    (*joinrel).reloptkind = RELOPT_OTHER_JOINREL;
    (*joinrel).relids = adjust_child_relids((*parent_joinrel).relids, nappinfos, appinfos);
    (*joinrel).rows = 0.0;
    /* cheap startup cost is interesting iff not all tuples to be retrieved */
    (*joinrel).consider_startup = (*root).tuple_fraction > 0.0;
    (*joinrel).consider_param_startup = false;
    (*joinrel).consider_parallel = false;
    (*joinrel).reltarget = create_empty_pathtarget();
    (*joinrel).pathlist = NIL;
    (*joinrel).ppilist = NIL;
    (*joinrel).partial_pathlist = NIL;
    (*joinrel).cheapest_startup_path = core::ptr::null_mut();
    (*joinrel).cheapest_total_path = core::ptr::null_mut();
    (*joinrel).cheapest_unique_path = core::ptr::null_mut();
    (*joinrel).cheapest_parameterized_paths = NIL;
    (*joinrel).direct_lateral_relids = core::ptr::null_mut();
    (*joinrel).lateral_relids = core::ptr::null_mut();
    (*joinrel).relid = 0; /* indicates not a baserel */
    (*joinrel).rtekind = RTE_JOIN;
    (*joinrel).min_attr = 0;
    (*joinrel).max_attr = 0;
    (*joinrel).attr_needed = core::ptr::null_mut();
    (*joinrel).attr_widths = core::ptr::null_mut();
    (*joinrel).notnullattnums = core::ptr::null_mut();
    (*joinrel).nulling_relids = core::ptr::null_mut();
    (*joinrel).lateral_vars = NIL;
    (*joinrel).lateral_referencers = core::ptr::null_mut();
    (*joinrel).indexlist = NIL;
    (*joinrel).pages = 0;
    (*joinrel).tuples = 0.0;
    (*joinrel).allvisfrac = 0.0;
    (*joinrel).eclass_indexes = core::ptr::null_mut();
    (*joinrel).subroot = core::ptr::null_mut();
    (*joinrel).subplan_params = NIL;
    (*joinrel).amflags = 0;
    (*joinrel).serverid = InvalidOid;
    (*joinrel).userid = InvalidOid;
    (*joinrel).useridiscurrent = false;
    (*joinrel).fdwroutine = core::ptr::null_mut();
    (*joinrel).fdw_private = core::ptr::null_mut();
    (*joinrel).baserestrictinfo = NIL;
    (*joinrel).baserestrictcost.startup = 0.0;
    (*joinrel).baserestrictcost.per_tuple = 0.0;
    (*joinrel).joininfo = NIL;
    (*joinrel).has_eclass_joins = false;
    (*joinrel).consider_partitionwise_join = false; /* might get changed later */
    (*joinrel).parent = parent_joinrel;
    (*joinrel).top_parent = if !(*parent_joinrel).top_parent.is_null() {
        (*parent_joinrel).top_parent
    } else {
        parent_joinrel
    };
    (*joinrel).top_parent_relids = (*(*joinrel).top_parent).relids;
    (*joinrel).part_scheme = core::ptr::null_mut();
    (*joinrel).nparts = -1;
    (*joinrel).boundinfo = core::ptr::null_mut();
    (*joinrel).partbounds_merged = false;
    (*joinrel).partition_qual = NIL;
    (*joinrel).part_rels = core::ptr::null_mut();
    (*joinrel).live_parts = core::ptr::null_mut();
    (*joinrel).all_partrels = core::ptr::null_mut();
    (*joinrel).partexprs = core::ptr::null_mut();
    (*joinrel).nullable_partexprs = core::ptr::null_mut();

    /* Compute information relevant to foreign relations. */
    set_foreign_rel_properties(joinrel, outer_rel, inner_rel);

    /* Set up reltarget struct */
    build_child_join_reltarget(root, parent_joinrel, joinrel, nappinfos, appinfos);

    /* Construct joininfo list. */
    (*joinrel).joininfo = adjust_appendrel_attrs(
        root,
        (*parent_joinrel).joininfo as *mut Node,
        nappinfos,
        appinfos,
    ) as *mut List;

    /*
     * Lateral relids referred in child join will be same as that referred in
     * the parent relation.
     */
    (*joinrel).direct_lateral_relids =
        bms_copy((*parent_joinrel).direct_lateral_relids) as Relids;
    (*joinrel).lateral_relids = bms_copy((*parent_joinrel).lateral_relids) as Relids;

    /*
     * If the parent joinrel has pending equivalence classes, so does the
     * child.
     */
    (*joinrel).has_eclass_joins = (*parent_joinrel).has_eclass_joins;

    /* Is the join between partitions itself partitioned? */
    build_joinrel_partition_info(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    /* Child joinrel is parallel safe if parent is parallel safe. */
    (*joinrel).consider_parallel = (*parent_joinrel).consider_parallel;

    /* Set estimates of the child-joinrel's size. */
    set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    /* We build the join only once. */
    Assert!(find_join_rel(root, (*joinrel).relids).is_null());

    /* Add the relation to the PlannerInfo. */
    add_join_rel(root, joinrel);

    /*
     * We might need EquivalenceClass members corresponding to the child join,
     * so that we can represent sort pathkeys for it.  As with children of
     * baserels, we shouldn't need this unless there are relevant eclass joins
     * (implying that a merge join might be possible) or pathkeys to sort by.
     */
    if (*joinrel).has_eclass_joins || has_useful_pathkeys(root, parent_joinrel) {
        add_child_join_rel_equivalences(root, nappinfos, appinfos, parent_joinrel, joinrel);
    }

    joinrel
}

/*
 * min_join_parameterization
 *
 * Determine the minimum possible parameterization of a joinrel, that is, the
 * set of other rels it contains LATERAL references to.  We save this value in
 * the join's RelOptInfo.  This function is split out of build_join_rel()
 * because join_is_legal() needs the value to check a prospective join.
 */
pub unsafe fn min_join_parameterization(
    root: *mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
) -> Relids {
    let result: Relids;

    /*
     * Basically we just need the union of the inputs' lateral_relids, less
     * whatever is already in the join.
     *
     * It's not immediately obvious that this is a valid way to compute the
     * result, because it might seem that we're ignoring possible lateral refs
     * of PlaceHolderVars that are due to be computed at the join but not in
     * either input.  However, because create_lateral_join_info() already
     * charged all such PHV refs to each member baserel of the join, they'll
     * be accounted for already in the inputs' lateral_relids.  Likewise, we
     * do not need to worry about doing transitive closure here, because that
     * was already accounted for in the original baserel lateral_relids.
     */
    let result = bms_union((*outer_rel).lateral_relids, (*inner_rel).lateral_relids);
    bms_del_members(result, joinrelids)
}

/*
 * build_joinrel_tlist
 *	  Builds a join relation's target list from an input relation.
 *	  (This is invoked twice to handle the two input relations.)
 *
 * The join's targetlist includes all Vars of its member relations that
 * will still be needed above the join.  This subroutine adds all such
 * Vars from the specified input rel's tlist to the join rel's tlist.
 * Likewise for any PlaceHolderVars emitted by the input rel.
 *
 * We also compute the expected width of the join's output, making use
 * of data that was cached at the baserel level by set_rel_width().
 *
 * Pass can_null as true if the join is an outer join that can null Vars
 * from this input relation.  If so, we will (normally) add the join's relid
 * to the nulling bitmaps of Vars and PHVs bubbled up from the input.
 *
 * When forming an outer join's target list, special handling is needed in
 * case the outer join was commuted with another one per outer join identity 3
 * (see optimizer/README).  We must take steps to ensure that the output Vars
 * have the same nulling bitmaps that they would if the two joins had been
 * done in syntactic order; else they won't match Vars appearing higher in
 * the query tree.  An exception to the match-the-syntactic-order rule is
 * that when an outer join is pushed down into another one's RHS per identity
 * 3, we can't mark its Vars as nulled until the now-upper outer join is also
 * completed.  So we need to do three things:
 *
 * First, we add the outer join's relid to the nulling bitmap only if the
 * outer join has been completely performed and the Var or PHV actually
 * comes from within the syntactically nullable side(s) of the outer join.
 * This takes care of the possibility that we have transformed
 *		(A leftjoin B on (Pab)) leftjoin C on (Pbc)
 * to
 *		A leftjoin (B leftjoin C on (Pbc)) on (Pab)
 * Here the pushed-down B/C join cannot mark C columns as nulled yet,
 * while the now-upper A/B join must not mark C columns as nulled by itself.
 *
 * Second, perform the same operation for each SpecialJoinInfo listed in
 * pushed_down_joins (which, in this example, would be the B/C join when
 * we are at the now-upper A/B join).  This allows the now-upper join to
 * complete the marking of "C" Vars that now have fully valid values.
 *
 * Third, any relid in sjinfo->commute_above_r that is already part of
 * the joinrel is added to the nulling bitmaps of nullable Vars and PHVs.
 * This takes care of the reverse case where we implement
 *		A leftjoin (B leftjoin C on (Pbc)) on (Pab)
 * as
 *		(A leftjoin B on (Pab)) leftjoin C on (Pbc)
 * The C columns emitted by the B/C join need to be shown as nulled by both
 * the B/C and A/B joins, even though they've not physically traversed the
 * A/B join.
 */
unsafe fn build_joinrel_tlist(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    input_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
    pushed_down_joins: *mut List,
    can_null: bool,
) {
    let relids: Relids = (*joinrel).relids;
    let mut tuple_width: i64 = (*(*joinrel).reltarget).width as i64;

    foreach!(vars, (*(*input_rel).reltarget).exprs, {
        let var = lfirst(crate::current_cell!(vars)) as *mut Var;

        /*
         * For a PlaceHolderVar, we have to look up the PlaceHolderInfo.
         */
        if IsA!(var, T_PlaceHolderVar) {
            let phv = var as *mut PlaceHolderVar;
            let phinfo = find_placeholder_info(root, phv);

            /* Is it still needed above this joinrel? */
            if bms_nonempty_difference((*phinfo).ph_needed, relids) {
                /*
                 * Yup, add it to the output.  If this join potentially nulls
                 * this input, we have to update the PHV's phnullingrels,
                 * which means making a copy.
                 */
                let phv_out: *mut PlaceHolderVar;
                if can_null {
                    let phv2 = copyObject(phv as *const PlaceHolderVar);
                    /* See comments above to understand this logic */
                    if (*sjinfo).ojrelid != 0
                        && bms_is_member((*sjinfo).ojrelid as c_int, relids)
                        && (bms_is_subset((*phv2).phrels, (*sjinfo).syn_righthand)
                            || ((*sjinfo).jointype == JOIN_FULL
                                && bms_is_subset((*phv2).phrels, (*sjinfo).syn_lefthand)))
                    {
                        (*phv2).phnullingrels =
                            bms_add_member((*phv2).phnullingrels, (*sjinfo).ojrelid as c_int);
                    }
                    foreach!(lc, pushed_down_joins, {
                        let othersj =
                            lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;
                        Assert!(bms_is_member((*othersj).ojrelid as c_int, relids));
                        if bms_is_subset((*phv2).phrels, (*othersj).syn_righthand) {
                            (*phv2).phnullingrels = bms_add_member(
                                (*phv2).phnullingrels,
                                (*othersj).ojrelid as c_int,
                            );
                        }
                    });
                    (*phv2).phnullingrels = bms_join(
                        (*phv2).phnullingrels,
                        bms_intersect((*sjinfo).commute_above_r, relids),
                    );
                    phv_out = phv2;
                } else {
                    phv_out = phv;
                }

                (*(*joinrel).reltarget).exprs =
                    lappend((*(*joinrel).reltarget).exprs, phv_out as *mut c_void);
                /* Bubbling up the precomputed result has cost zero */
                tuple_width += (*phinfo).ph_width as i64;
            }
            continue;
        }

        /*
         * Otherwise, anything in a baserel or joinrel targetlist ought to be
         * a Var.  (More general cases can only appear in appendrel child
         * rels, which will never be seen here.)
         */
        if !IsA!(var, T_Var) {
            elog!(
                ERROR,
                "unexpected node type in rel targetlist: {}",
                nodeTag(var as *const Node) as c_int
            );
        }

        if (*var).varno == ROWID_VAR {
            /* UPDATE/DELETE/MERGE row identity vars are always needed */
            let ridinfo = list_nth(
                (*root).row_identity_vars,
                ((*var).varattno - 1) as c_int,
            ) as *mut RowIdentityVarInfo;

            /* Update reltarget width estimate from RowIdentityVarInfo */
            tuple_width += (*ridinfo).rowidwidth as i64;
        } else {
            let baserel: *mut RelOptInfo;
            let ndx: c_int;

            /* Get the Var's original base rel */
            baserel = find_base_rel(root, (*var).varno);

            /* Is it still needed above this joinrel? */
            ndx = (*var).varattno as c_int - (*baserel).min_attr as c_int;
            if !bms_nonempty_difference(
                *(*baserel).attr_needed.add(ndx as usize),
                relids,
            ) {
                continue; /* nope, skip it */
            }

            /* Update reltarget width estimate from baserel's attr_widths */
            tuple_width += *(*baserel).attr_widths.add(ndx as usize) as i64;
        }

        /*
         * Add the Var to the output.  If this join potentially nulls this
         * input, we have to update the Var's varnullingrels, which means
         * making a copy.  But note that we don't ever add nullingrel bits to
         * row identity Vars (cf. comments in setrefs.c).
         */
        let var_out: *mut Var;
        if can_null && (*var).varno != ROWID_VAR {
            let var2 = copyObject(var as *const Var);
            /* See comments above to understand this logic */
            if (*sjinfo).ojrelid != 0
                && bms_is_member((*sjinfo).ojrelid as c_int, relids)
                && (bms_is_member((*var2).varno, (*sjinfo).syn_righthand)
                    || ((*sjinfo).jointype == JOIN_FULL
                        && bms_is_member((*var2).varno, (*sjinfo).syn_lefthand)))
            {
                (*var2).varnullingrels =
                    bms_add_member((*var2).varnullingrels, (*sjinfo).ojrelid as c_int);
            }
            foreach!(lc, pushed_down_joins, {
                let othersj = lfirst(crate::current_cell!(lc)) as *mut SpecialJoinInfo;
                Assert!(bms_is_member((*othersj).ojrelid as c_int, relids));
                if bms_is_member((*var2).varno, (*othersj).syn_righthand) {
                    (*var2).varnullingrels = bms_add_member(
                        (*var2).varnullingrels,
                        (*othersj).ojrelid as c_int,
                    );
                }
            });
            (*var2).varnullingrels = bms_join(
                (*var2).varnullingrels,
                bms_intersect((*sjinfo).commute_above_r, relids),
            );
            var_out = var2;
        } else {
            var_out = var;
        }

        (*(*joinrel).reltarget).exprs =
            lappend((*(*joinrel).reltarget).exprs, var_out as *mut c_void);

        /* Vars have cost zero, so no need to adjust reltarget->cost */
    });

    (*(*joinrel).reltarget).width = clamp_width_est(tuple_width);
}

/*
 * build_joinrel_restrictlist
 * build_joinrel_joinlist
 *	  These routines build lists of restriction and join clauses for a
 *	  join relation from the joininfo lists of the relations it joins.
 *
 *	  These routines are separate because the restriction list must be
 *	  built afresh for each pair of input sub-relations we consider, whereas
 *	  the join list need only be computed once for any join RelOptInfo.
 *	  The join list is fully determined by the set of rels making up the
 *	  joinrel, so we should get the same results (up to ordering) from any
 *	  candidate pair of sub-relations.  But the restriction list is whatever
 *	  is not handled in the sub-relations, so it depends on which
 *	  sub-relations are considered.
 *
 *	  If a join clause from an input relation refers to base+OJ rels still not
 *	  present in the joinrel, then it is still a join clause for the joinrel;
 *	  we put it into the joininfo list for the joinrel.  Otherwise,
 *	  the clause is now a restrict clause for the joined relation, and we
 *	  return it to the caller of build_joinrel_restrictlist() to be stored in
 *	  join paths made from this pair of sub-relations.  (It will not need to
 *	  be considered further up the join tree.)
 *
 *	  In many cases we will find the same RestrictInfos in both input
 *	  relations' joinlists, so be careful to eliminate duplicates.
 *	  Pointer equality should be a sufficient test for dups, since all
 *	  the various joinlist entries ultimately refer to RestrictInfos
 *	  pushed into them by distribute_restrictinfo_to_rels().
 *
 * 'joinrel' is a join relation node
 * 'outer_rel' and 'inner_rel' are a pair of relations that can be joined
 *		to form joinrel.
 * 'sjinfo': join context info
 *
 * build_joinrel_restrictlist() returns a list of relevant restrictinfos,
 * whereas build_joinrel_joinlist() stores its results in the joinrel's
 * joininfo list.  One or the other must accept each given clause!
 *
 * NB: Formerly, we made deep(!) copies of each input RestrictInfo to pass
 * up to the join relation.  I believe this is no longer necessary, because
 * RestrictInfo nodes are no longer context-dependent.  Instead, just include
 * the original nodes in the lists made for the join relation.
 */
unsafe fn build_joinrel_restrictlist(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
) -> *mut List {
    let result: *mut List;
    let both_input_relids: Relids;

    both_input_relids = bms_union((*outer_rel).relids, (*inner_rel).relids);

    /*
     * Collect all the clauses that syntactically belong at this level,
     * eliminating any duplicates (important since we will see many of the
     * same clauses arriving from both input relations).
     */
    let result = subbuild_joinrel_restrictlist(root, joinrel, outer_rel, both_input_relids, NIL);
    let result = subbuild_joinrel_restrictlist(root, joinrel, inner_rel, both_input_relids, result);

    /*
     * Add on any clauses derived from EquivalenceClasses.  These cannot be
     * redundant with the clauses in the joininfo lists, so don't bother
     * checking.
     */
    list_concat(
        result,
        generate_join_implied_equalities(
            root,
            (*joinrel).relids,
            (*outer_rel).relids,
            inner_rel,
            sjinfo,
        ),
    )
}

unsafe fn build_joinrel_joinlist(
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
) {
    let result: *mut List;

    /*
     * Collect all the clauses that syntactically belong above this level,
     * eliminating any duplicates (important since we will see many of the
     * same clauses arriving from both input relations).
     */
    let result = subbuild_joinrel_joinlist(joinrel, (*outer_rel).joininfo, NIL);
    let result = subbuild_joinrel_joinlist(joinrel, (*inner_rel).joininfo, result);

    (*joinrel).joininfo = result;
}

unsafe fn subbuild_joinrel_restrictlist(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    input_rel: *mut RelOptInfo,
    both_input_relids: Relids,
    mut new_restrictlist: *mut List,
) -> *mut List {
    foreach!(l, (*input_rel).joininfo, {
        let rinfo = lfirst(crate::current_cell!(l)) as *mut RestrictInfo;

        if bms_is_subset((*rinfo).required_relids, (*joinrel).relids) {
            /*
             * This clause should become a restriction clause for the joinrel,
             * since it refers to no outside rels.  However, if it's a clone
             * clause then it might be too late to evaluate it, so we have to
             * check.  (If it is too late, just ignore the clause, taking it
             * on faith that another clone was or will be selected.)  Clone
             * clauses should always be outer-join clauses, so we compare
             * against both_input_relids.
             */
            if (*rinfo).has_clone || (*rinfo).is_clone {
                Assert!(!RINFO_IS_PUSHED_DOWN(rinfo, (*joinrel).relids));
                if !bms_is_subset((*rinfo).required_relids, both_input_relids) {
                    continue;
                }
                if bms_overlap((*rinfo).incompatible_relids, both_input_relids) {
                    continue;
                }
            } else {
                /*
                 * For non-clone clauses, we just Assert it's OK.  These might
                 * be either join or filter clauses; if it's a join clause
                 * then it should not refer to the current join's output.
                 * (There is little point in checking incompatible_relids,
                 * because it'll be NULL.)
                 */
                Assert!(
                    RINFO_IS_PUSHED_DOWN(rinfo, (*joinrel).relids)
                        || bms_is_subset((*rinfo).required_relids, both_input_relids)
                );
            }

            /*
             * OK, so add it to the list, being careful to eliminate
             * duplicates.  (Since RestrictInfo nodes in different joinlists
             * will have been multiply-linked rather than copied, pointer
             * equality should be a sufficient test.)
             */
            new_restrictlist =
                list_append_unique_ptr(new_restrictlist, rinfo as *mut c_void);
        } else {
            /*
             * This clause is still a join clause at this level, so we ignore
             * it in this routine.
             */
        }
    });

    new_restrictlist
}

unsafe fn subbuild_joinrel_joinlist(
    joinrel: *mut RelOptInfo,
    joininfo_list: *mut List,
    mut new_joininfo: *mut List,
) -> *mut List {
    /* Expected to be called only for join between parent relations. */
    Assert!((*joinrel).reloptkind == RELOPT_JOINREL);

    foreach!(l, joininfo_list, {
        let rinfo = lfirst(crate::current_cell!(l)) as *mut RestrictInfo;

        if bms_is_subset((*rinfo).required_relids, (*joinrel).relids) {
            /*
             * This clause becomes a restriction clause for the joinrel, since
             * it refers to no outside rels.  So we can ignore it in this
             * routine.
             */
        } else {
            /*
             * This clause is still a join clause at this level, so add it to
             * the new joininfo list, being careful to eliminate duplicates.
             * (Since RestrictInfo nodes in different joinlists will have been
             * multiply-linked rather than copied, pointer equality should be
             * a sufficient test.)
             */
            new_joininfo = list_append_unique_ptr(new_joininfo, rinfo as *mut c_void);
        }
    });

    new_joininfo
}


/*
 * fetch_upper_rel
 *		Build a RelOptInfo describing some post-scan/join query processing,
 *		or return a pre-existing one if somebody already built it.
 *
 * An "upper" relation is identified by an UpperRelationKind and a Relids set.
 * The meaning of the Relids set is not specified here, and very likely will
 * vary for different relation kinds.
 *
 * Most of the fields in an upper-level RelOptInfo are not used and are not
 * set here (though makeNode should ensure they're zeroes).  We basically only
 * care about fields that are of interest to add_path() and set_cheapest().
 */
pub unsafe fn fetch_upper_rel(
    root: *mut PlannerInfo,
    kind: UpperRelationKind,
    relids: Relids,
) -> *mut RelOptInfo {
    let upperrel: *mut RelOptInfo;

    /*
     * For the moment, our indexing data structure is just a List for each
     * relation kind.  If we ever get so many of one kind that this stops
     * working well, we can improve it.  No code outside this function should
     * assume anything about how to find a particular upperrel.
     */

    /* If we already made this upperrel for the query, return it */
    foreach!(lc, (*root).upper_rels[kind as usize], {
        let upperrel2 = lfirst(crate::current_cell!(lc)) as *mut RelOptInfo;
        if bms_equal((*upperrel2).relids, relids) {
            return upperrel2;
        }
    });

    let upperrel = makeNode!(RelOptInfo, T_RelOptInfo);
    (*upperrel).reloptkind = RELOPT_UPPER_REL;
    (*upperrel).relids = bms_copy(relids);

    /* cheap startup cost is interesting iff not all tuples to be retrieved */
    (*upperrel).consider_startup = (*root).tuple_fraction > 0.0;
    (*upperrel).consider_param_startup = false;
    (*upperrel).consider_parallel = false; /* might get changed later */
    (*upperrel).reltarget = create_empty_pathtarget();
    (*upperrel).pathlist = NIL;
    (*upperrel).cheapest_startup_path = core::ptr::null_mut();
    (*upperrel).cheapest_total_path = core::ptr::null_mut();
    (*upperrel).cheapest_unique_path = core::ptr::null_mut();
    (*upperrel).cheapest_parameterized_paths = NIL;

    (*root).upper_rels[kind as usize] =
        lappend((*root).upper_rels[kind as usize], upperrel as *mut c_void);

    upperrel
}


/*
 * find_childrel_parents
 *		Compute the set of parent relids of an appendrel child rel.
 *
 * Since appendrels can be nested, a child could have multiple levels of
 * appendrel ancestors.  This function computes a Relids set of all the
 * parent relation IDs.
 */
pub unsafe fn find_childrel_parents(root: *mut PlannerInfo, mut rel: *mut RelOptInfo) -> Relids {
    let mut result: Relids = core::ptr::null_mut();

    Assert!((*rel).reloptkind == RELOPT_OTHER_MEMBER_REL);
    Assert!((*rel).relid > 0 && ((*rel).relid as c_int) < (*root).simple_rel_array_size);

    loop {
        let appinfo = *(*root).append_rel_array.add((*rel).relid as usize);
        let prelid = (*appinfo).parent_relid;

        result = bms_add_member(result, prelid as c_int);

        /* traverse up to the parent rel, loop if it's also a child rel */
        rel = find_base_rel(root, prelid as c_int);

        if (*rel).reloptkind != RELOPT_OTHER_MEMBER_REL {
            break;
        }
    }

    Assert!((*rel).reloptkind == RELOPT_BASEREL);

    result
}


/*
 * get_baserel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a base relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 */
pub unsafe fn get_baserel_parampathinfo(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut ParamPathInfo {
    let ppi: *mut ParamPathInfo;
    let joinrelids: Relids;
    let mut pclauses: *mut List;
    let eqclauses: *mut List;
    let mut pserials: *mut Bitmapset;
    let rows: f64;

    /* If rel has LATERAL refs, every path for it should account for them */
    Assert!(bms_is_subset((*baserel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo */
    if bms_is_empty(required_outer) {
        return core::ptr::null_mut();
    }

    Assert!(!bms_overlap((*baserel).relids, required_outer));

    /* If we already have a PPI for this parameterization, just return it */
    let ppi_existing = find_param_path_info(baserel, required_outer);
    if !ppi_existing.is_null() {
        return ppi_existing;
    }

    /*
     * Identify all joinclauses that are movable to this base rel given this
     * parameterization.
     */
    joinrelids = bms_union((*baserel).relids, required_outer);
    pclauses = NIL;
    foreach!(lc, (*baserel).joininfo, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;

        if join_clause_is_movable_into(rinfo, (*baserel).relids, joinrelids) {
            pclauses = lappend(pclauses, rinfo as *mut c_void);
        }
    });

    /*
     * Add in joinclauses generated by EquivalenceClasses, too.  (These
     * necessarily satisfy join_clause_is_movable_into; but in assert-enabled
     * builds, let's verify that.)
     */
    eqclauses = generate_join_implied_equalities(
        root,
        joinrelids,
        required_outer,
        baserel,
        core::ptr::null_mut(),
    );
    /* Assert-only check in USE_ASSERT_CHECKING builds */
    #[cfg(debug_assertions)]
    {
        foreach!(lc, eqclauses, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
            Assert!(join_clause_is_movable_into(rinfo, (*baserel).relids, joinrelids));
        });
    }
    pclauses = list_concat(pclauses, eqclauses);

    /* Compute set of serial numbers of the enforced clauses */
    pserials = core::ptr::null_mut();
    foreach!(lc, pclauses, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
        pserials = bms_add_member(pserials, (*rinfo).rinfo_serial);
    });

    /* Estimate the number of rows returned by the parameterized scan */
    rows = get_parameterized_baserel_size(root, baserel, pclauses);

    /* And now we can build the ParamPathInfo */
    let ppi = makeNode!(ParamPathInfo, T_ParamPathInfo);
    (*ppi).ppi_req_outer = required_outer;
    (*ppi).ppi_rows = rows;
    (*ppi).ppi_clauses = pclauses;
    (*ppi).ppi_serials = pserials;
    (*baserel).ppilist = lappend((*baserel).ppilist, ppi as *mut c_void);

    ppi
}

/*
 * get_joinrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a join relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 *
 * outer_path and inner_path are a pair of input paths that can be used to
 * construct the join, and restrict_clauses is the list of regular join
 * clauses (including clauses derived from EquivalenceClasses) that must be
 * applied at the join node when using these inputs.
 *
 * Unlike the situation for base rels, the set of movable join clauses to be
 * enforced at a join varies with the selected pair of input paths, so we
 * must calculate that and pass it back, even if we already have a matching
 * ParamPathInfo.  We handle this by adding any clauses moved down to this
 * join to *restrict_clauses, which is an in/out parameter.  (The addition
 * is done in such a way as to not modify the passed-in List structure.)
 *
 * Note: when considering a nestloop join, the caller must have removed from
 * restrict_clauses any movable clauses that are themselves scheduled to be
 * pushed into the right-hand path.  We do not do that here since it's
 * unnecessary for other join types.
 */
pub unsafe fn get_joinrel_parampathinfo(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_path: *mut Path,
    inner_path: *mut Path,
    sjinfo: *mut SpecialJoinInfo,
    required_outer: Relids,
    restrict_clauses: *mut *mut List,
) -> *mut ParamPathInfo {
    let join_and_req: Relids;
    let outer_and_req: Relids;
    let inner_and_req: Relids;
    let mut pclauses: *mut List;
    let eclauses: *mut List;
    let mut dropped_ecs: *mut List;
    let rows: f64;

    /* If rel has LATERAL refs, every path for it should account for them */
    Assert!(bms_is_subset((*joinrel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo or extra join clauses */
    if bms_is_empty(required_outer) {
        return core::ptr::null_mut();
    }

    Assert!(!bms_overlap((*joinrel).relids, required_outer));

    /*
     * Identify all joinclauses that are movable to this join rel given this
     * parameterization.  These are the clauses that are movable into this
     * join, but not movable into either input path.  Treat an unparameterized
     * input path as not accepting parameterized clauses (because it won't,
     * per the shortcut exit above), even though the joinclause movement rules
     * might allow the same clauses to be moved into a parameterized path for
     * that rel.
     */
    join_and_req = bms_union((*joinrel).relids, required_outer);
    outer_and_req = if !(*outer_path).param_info.is_null() {
        bms_union(
            (*(*outer_path).parent).relids,
            PATH_REQ_OUTER(outer_path),
        )
    } else {
        core::ptr::null_mut() /* outer path does not accept parameters */
    };
    inner_and_req = if !(*inner_path).param_info.is_null() {
        bms_union(
            (*(*inner_path).parent).relids,
            PATH_REQ_OUTER(inner_path),
        )
    } else {
        core::ptr::null_mut() /* inner path does not accept parameters */
    };

    pclauses = NIL;
    foreach!(lc, (*joinrel).joininfo, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;

        if join_clause_is_movable_into(rinfo, (*joinrel).relids, join_and_req)
            && !join_clause_is_movable_into(rinfo, (*(*outer_path).parent).relids, outer_and_req)
            && !join_clause_is_movable_into(rinfo, (*(*inner_path).parent).relids, inner_and_req)
        {
            pclauses = lappend(pclauses, rinfo as *mut c_void);
        }
    });

    /* Consider joinclauses generated by EquivalenceClasses, too */
    let eclauses = generate_join_implied_equalities(
        root,
        join_and_req,
        required_outer,
        joinrel,
        core::ptr::null_mut(),
    );
    /* We only want ones that aren't movable to lower levels */
    dropped_ecs = NIL;
    foreach!(lc, eclauses, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;

        Assert!(join_clause_is_movable_into(rinfo, (*joinrel).relids, join_and_req));
        if join_clause_is_movable_into(rinfo, (*(*outer_path).parent).relids, outer_and_req) {
            continue; /* drop if movable into LHS */
        }
        if join_clause_is_movable_into(rinfo, (*(*inner_path).parent).relids, inner_and_req) {
            /* drop if movable into RHS, but remember EC for use below */
            Assert!((*rinfo).left_ec == (*rinfo).right_ec);
            dropped_ecs = lappend(dropped_ecs, (*rinfo).left_ec as *mut c_void);
            continue;
        }
        pclauses = lappend(pclauses, rinfo as *mut c_void);
    });

    /*
     * EquivalenceClasses are harder to deal with than we could wish, because
     * of the fact that a given EC can generate different clauses depending on
     * context.  Suppose we have an EC {X.X, Y.Y, Z.Z} where X and Y are the
     * LHS and RHS of the current join and Z is in required_outer, and further
     * suppose that the inner_path is parameterized by both X and Z.  The code
     * above will have produced either Z.Z = X.X or Z.Z = Y.Y from that EC,
     * and in the latter case will have discarded it as being movable into the
     * RHS.  However, the EC machinery might have produced either Y.Y = X.X or
     * Y.Y = Z.Z as the EC enforcement clause within the inner_path; it will
     * not have produced both, and we can't readily tell from here which one
     * it did pick.  If we add no clause to this join, we'll end up with
     * insufficient enforcement of the EC; either Z.Z or X.X will fail to be
     * constrained to be equal to the other members of the EC.  (When we come
     * to join Z to this X/Y path, we will certainly drop whichever EC clause
     * is generated at that join, so this omission won't get fixed later.)
     *
     * To handle this, for each EC we discarded such a clause from, try to
     * generate a clause connecting the required_outer rels to the join's LHS
     * ("Z.Z = X.X" in the terms of the above example).  If successful, and if
     * the clause can't be moved to the LHS, add it to the current join's
     * restriction clauses.  (If an EC cannot generate such a clause then it
     * has nothing that needs to be enforced here, while if the clause can be
     * moved into the LHS then it should have been enforced within that path.)
     *
     * Note that we don't need similar processing for ECs whose clause was
     * considered to be movable into the LHS, because the LHS can't refer to
     * the RHS so there is no comparable ambiguity about what it might
     * actually be enforcing internally.
     */
    if !dropped_ecs.is_null() {
        let real_outer_and_req: Relids;

        real_outer_and_req =
            bms_union((*(*outer_path).parent).relids, required_outer);
        let eclauses2 = generate_join_implied_equalities_for_ecs(
            root,
            dropped_ecs,
            real_outer_and_req,
            required_outer,
            (*outer_path).parent,
        );
        foreach!(lc, eclauses2, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;

            Assert!(join_clause_is_movable_into(
                rinfo,
                (*(*outer_path).parent).relids,
                real_outer_and_req
            ));
            if !join_clause_is_movable_into(
                rinfo,
                (*(*outer_path).parent).relids,
                outer_and_req,
            ) {
                pclauses = lappend(pclauses, rinfo as *mut c_void);
            }
        });
    }

    /*
     * Now, attach the identified moved-down clauses to the caller's
     * restrict_clauses list.  By using list_concat in this order, we leave
     * the original list structure of restrict_clauses undamaged.
     */
    *restrict_clauses = list_concat(pclauses, *restrict_clauses);

    /* If we already have a PPI for this parameterization, just return it */
    let ppi_existing = find_param_path_info(joinrel, required_outer);
    if !ppi_existing.is_null() {
        return ppi_existing;
    }

    /* Estimate the number of rows returned by the parameterized join */
    rows = get_parameterized_joinrel_size(
        root,
        joinrel,
        outer_path,
        inner_path,
        sjinfo,
        *restrict_clauses,
    );

    /*
     * And now we can build the ParamPathInfo.  No point in saving the
     * input-pair-dependent clause list, though.
     *
     * Note: in GEQO mode, we'll be called in a temporary memory context, but
     * the joinrel structure is there too, so no problem.
     */
    let ppi = makeNode!(ParamPathInfo, T_ParamPathInfo);
    (*ppi).ppi_req_outer = required_outer;
    (*ppi).ppi_rows = rows;
    (*ppi).ppi_clauses = NIL;
    (*ppi).ppi_serials = core::ptr::null_mut();
    (*joinrel).ppilist = lappend((*joinrel).ppilist, ppi as *mut c_void);

    ppi
}

/*
 * get_appendrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for an append relation.
 *
 * For an append relation, the rowcount estimate will just be the sum of
 * the estimates for its children.  However, we still need a ParamPathInfo
 * to flag the fact that the path requires parameters.  So this just creates
 * a suitable struct with zero ppi_rows (and no ppi_clauses either, since
 * the Append node isn't responsible for checking quals).
 */
pub unsafe fn get_appendrel_parampathinfo(
    appendrel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut ParamPathInfo {
    /* If rel has LATERAL refs, every path for it should account for them */
    Assert!(bms_is_subset((*appendrel).lateral_relids, required_outer));

    /* Unparameterized paths have no ParamPathInfo */
    if bms_is_empty(required_outer) {
        return core::ptr::null_mut();
    }

    Assert!(!bms_overlap((*appendrel).relids, required_outer));

    /* If we already have a PPI for this parameterization, just return it */
    let ppi_existing = find_param_path_info(appendrel, required_outer);
    if !ppi_existing.is_null() {
        return ppi_existing;
    }

    /* Else build the ParamPathInfo */
    let ppi = makeNode!(ParamPathInfo, T_ParamPathInfo);
    (*ppi).ppi_req_outer = required_outer;
    (*ppi).ppi_rows = 0.0;
    (*ppi).ppi_clauses = NIL;
    (*ppi).ppi_serials = core::ptr::null_mut();
    (*appendrel).ppilist = lappend((*appendrel).ppilist, ppi as *mut c_void);

    ppi
}

/*
 * Returns a ParamPathInfo for the parameterization given by required_outer, if
 * already available in the given rel. Returns NULL otherwise.
 */
pub unsafe fn find_param_path_info(
    rel: *mut RelOptInfo,
    required_outer: Relids,
) -> *mut ParamPathInfo {
    foreach!(lc, (*rel).ppilist, {
        let ppi = lfirst(crate::current_cell!(lc)) as *mut ParamPathInfo;

        if bms_equal((*ppi).ppi_req_outer, required_outer) {
            return ppi;
        }
    });

    core::ptr::null_mut()
}

/*
 * get_param_path_clause_serials
 *		Given a parameterized Path, return the set of pushed-down clauses
 *		(identified by rinfo_serial numbers) enforced within the Path.
 */
pub unsafe fn get_param_path_clause_serials(path: *mut Path) -> *mut Bitmapset {
    if (*path).param_info.is_null() {
        return core::ptr::null_mut(); /* not parameterized */
    }

    /*
     * We don't currently support parameterized MergeAppend paths, as
     * explained in the comments for generate_orderedappend_paths.
     */
    Assert!(!IsA!(path, T_MergeAppendPath));

    if IsA!(path, T_NestPath) || IsA!(path, T_MergePath) || IsA!(path, T_HashPath) {
        /*
         * For a join path, combine clauses enforced within either input path
         * with those enforced as joinrestrictinfo in this path.  Note that
         * joinrestrictinfo may include some non-pushed-down clauses, but for
         * current purposes it's okay if we include those in the result. (To
         * be more careful, we could check for clause_relids overlapping the
         * path parameterization, but it's not worth the cycles for now.)
         */
        let jpath = path as *mut crate::nodes::pathnodes::JoinPath;
        let mut pserials: *mut Bitmapset = core::ptr::null_mut();

        pserials = bms_add_members(
            pserials,
            get_param_path_clause_serials((*jpath).outerjoinpath),
        );
        pserials = bms_add_members(
            pserials,
            get_param_path_clause_serials((*jpath).innerjoinpath),
        );
        foreach!(lc, (*jpath).joinrestrictinfo, {
            let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
            pserials = bms_add_member(pserials, (*rinfo).rinfo_serial);
        });
        pserials
    } else if IsA!(path, T_AppendPath) {
        /*
         * For an appendrel, take the intersection of the sets of clauses
         * enforced in each input path.
         */
        let apath = path as *mut crate::nodes::pathnodes::AppendPath;
        let mut pserials: *mut Bitmapset = core::ptr::null_mut();

        foreach!(lc, (*apath).subpaths, {
            let subpath = lfirst(crate::current_cell!(lc)) as *mut Path;
            let subserials = get_param_path_clause_serials(subpath);

            if (*apath).subpaths == list_head((*apath).subpaths) as *mut List {
                pserials = bms_copy(subserials);
            } else {
                pserials = bms_int_members(pserials, subserials);
            }
        });
        pserials
    } else {
        /*
         * Otherwise, it's a baserel path and we can use the
         * previously-computed set of serial numbers.
         */
        (*(*path).param_info).ppi_serials
    }
}

/*
 * build_joinrel_partition_info
 *		Checks if the two relations being joined can use partitionwise join
 *		and if yes, initialize partitioning information of the resulting
 *		partitioned join relation.
 */
unsafe fn build_joinrel_partition_info(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    sjinfo: *mut SpecialJoinInfo,
    restrictlist: *mut List,
) {
    let part_scheme: crate::nodes::pathnodes::PartitionScheme;

    /* Nothing to do if partitionwise join technique is disabled. */
    if !enable_partitionwise_join {
        /* Assert(!IS_PARTITIONED_REL(joinrel)); -- requires is_dummy_rel, skip */
        return;
    }

    /*
     * We can only consider this join as an input to further partitionwise
     * joins if (a) the input relations are partitioned and have
     * consider_partitionwise_join=true, (b) the partition schemes match, and
     * (c) we can identify an equi-join between the partition keys.  Note that
     * if it were possible for have_partkey_equi_join to return different
     * answers for the same joinrel depending on which join ordering we try
     * first, this logic would break.  That shouldn't happen, though, because
     * of the way the query planner deduces implied equalities and reorders
     * the joins.  Please see optimizer/README for details.
     */
    if (*outer_rel).part_scheme.is_null()
        || (*inner_rel).part_scheme.is_null()
        || !(*outer_rel).consider_partitionwise_join
        || !(*inner_rel).consider_partitionwise_join
        || (*outer_rel).part_scheme != (*inner_rel).part_scheme
        || !have_partkey_equi_join(root, joinrel, outer_rel, inner_rel, (*sjinfo).jointype, restrictlist)
    {
        /* Assert(!IS_PARTITIONED_REL(joinrel)); */
        return;
    }

    part_scheme = (*outer_rel).part_scheme;

    /*
     * This function will be called only once for each joinrel, hence it
     * should not have partitioning fields filled yet.
     */
    Assert!(
        (*joinrel).part_scheme.is_null()
            && (*joinrel).partexprs.is_null()
            && (*joinrel).nullable_partexprs.is_null()
            && (*joinrel).part_rels.is_null()
            && (*joinrel).boundinfo.is_null()
    );

    /*
     * If the join relation is partitioned, it uses the same partitioning
     * scheme as the joining relations.
     *
     * Note: we calculate the partition bounds, number of partitions, and
     * child-join relations of the join relation in try_partitionwise_join().
     */
    (*joinrel).part_scheme = part_scheme;
    set_joinrel_partition_key_exprs(joinrel, outer_rel, inner_rel, (*sjinfo).jointype);

    /*
     * Set the consider_partitionwise_join flag.
     */
    Assert!((*outer_rel).consider_partitionwise_join);
    Assert!((*inner_rel).consider_partitionwise_join);
    (*joinrel).consider_partitionwise_join = true;
}

/*
 * have_partkey_equi_join
 *
 * Returns true if there exist equi-join conditions involving pairs
 * of matching partition keys of the relations being joined for all
 * partition keys.
 */
unsafe fn have_partkey_equi_join(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    rel1: *mut RelOptInfo,
    rel2: *mut RelOptInfo,
    jointype: JoinType,
    restrictlist: *mut List,
) -> bool {
    let part_scheme = (*rel1).part_scheme;
    let mut pk_known_equal = [false; PARTITION_MAX_KEYS];
    let mut num_equal_pks: c_int = 0;

    /*
     * This function must only be called when the joined relations have same
     * partitioning scheme.
     */
    Assert!((*rel1).part_scheme == (*rel2).part_scheme);
    Assert!(!part_scheme.is_null());

    /* We use a bool array to track which partkey columns are known equal */
    /* ... as well as a count of how many are known equal */

    /* First, look through the join's restriction clauses */
    foreach!(lc, restrictlist, {
        let rinfo = lfirst(crate::current_cell!(lc)) as *mut RestrictInfo;
        let opexpr: *mut OpExpr;
        let mut expr1: *mut Expr;
        let mut expr2: *mut Expr;
        let strict_op: bool;
        let ipk1: c_int;
        let ipk2: c_int;

        /* If processing an outer join, only use its own join clauses. */
        if IS_OUTER_JOIN(jointype) && RINFO_IS_PUSHED_DOWN(rinfo, (*joinrel).relids) {
            continue;
        }

        /* Skip clauses which can not be used for a join. */
        if !(*rinfo).can_join {
            continue;
        }

        /* Skip clauses which are not equality conditions. */
        if (*rinfo).mergeopfamilies.is_null() && !OidIsValid((*rinfo).hashjoinoperator) {
            continue;
        }

        /* Should be OK to assume it's an OpExpr. */
        opexpr = castNode!(OpExpr, T_OpExpr, (*rinfo).clause);

        /* Match the operands to the relation. */
        if bms_is_subset((*rinfo).left_relids, (*rel1).relids)
            && bms_is_subset((*rinfo).right_relids, (*rel2).relids)
        {
            expr1 = linitial((*opexpr).args) as *mut Expr;
            expr2 = crate::nodes::pg_list::lsecond((*opexpr).args) as *mut Expr;
        } else if bms_is_subset((*rinfo).left_relids, (*rel2).relids)
            && bms_is_subset((*rinfo).right_relids, (*rel1).relids)
        {
            expr1 = crate::nodes::pg_list::lsecond((*opexpr).args) as *mut Expr;
            expr2 = linitial((*opexpr).args) as *mut Expr;
        } else {
            continue;
        }

        /*
         * Now we need to know whether the join operator is strict; see
         * comments in pathnodes.h.
         */
        strict_op = op_strict((*opexpr).opno);

        /*
         * Vars appearing in the relation's partition keys will not have any
         * varnullingrels, but those in expr1 and expr2 will if we're above
         * outer joins that could null the respective rels.  It's okay to
         * match anyway, if the join operator is strict.
         */
        if strict_op {
            if bms_overlap((*rel1).relids, (*root).outer_join_rels) {
                expr1 = remove_nulling_relids(
                    expr1 as *mut Node,
                    (*root).outer_join_rels,
                    core::ptr::null_mut(),
                ) as *mut Expr;
            }
            if bms_overlap((*rel2).relids, (*root).outer_join_rels) {
                expr2 = remove_nulling_relids(
                    expr2 as *mut Node,
                    (*root).outer_join_rels,
                    core::ptr::null_mut(),
                ) as *mut Expr;
            }
        }

        /*
         * Only clauses referencing the partition keys are useful for
         * partitionwise join.
         */
        ipk1 = match_expr_to_partition_keys(expr1, rel1, strict_op);
        if ipk1 < 0 {
            continue;
        }
        ipk2 = match_expr_to_partition_keys(expr2, rel2, strict_op);
        if ipk2 < 0 {
            continue;
        }

        /*
         * If the clause refers to keys at different ordinal positions, it can
         * not be used for partitionwise join.
         */
        if ipk1 != ipk2 {
            continue;
        }

        /* Ignore clause if we already proved these keys equal. */
        if pk_known_equal[ipk1 as usize] {
            continue;
        }

        /* Reject if the partition key collation differs from the clause's. */
        if *(*part_scheme).partcollation.add(ipk1 as usize) != (*opexpr).inputcollid {
            return false;
        }

        /*
         * The clause allows partitionwise join only if it uses the same
         * operator family as that specified by the partition key.
         */
        if (*part_scheme).strategy == PARTITION_STRATEGY_HASH as c_char {
            if !OidIsValid((*rinfo).hashjoinoperator)
                || !op_in_opfamily(
                    (*rinfo).hashjoinoperator,
                    *(*part_scheme).partopfamily.add(ipk1 as usize),
                )
            {
                continue;
            }
        } else if !list_member_oid(
            (*rinfo).mergeopfamilies,
            *(*part_scheme).partopfamily.add(ipk1 as usize),
        ) {
            continue;
        }

        /* Mark the partition key as having an equi-join clause. */
        pk_known_equal[ipk1 as usize] = true;

        /* We can stop examining clauses once we prove all keys equal. */
        num_equal_pks += 1;
        if num_equal_pks == (*part_scheme).partnatts as c_int {
            return true;
        }
    });

    /*
     * Also check to see if any keys are known equal by equivclass.c.  In most
     * cases there would have been a join restriction clause generated from
     * any EC that had such knowledge, but there might be no such clause, or
     * it might happen to constrain other members of the ECs than the ones we
     * are looking for.
     */
    for ipk in 0..((*part_scheme).partnatts as usize) {
        let btree_opfamily: Oid;

        /* Ignore if we already proved these keys equal. */
        if pk_known_equal[ipk] {
            continue;
        }

        /*
         * We need a btree opfamily to ask equivclass.c about.  If the
         * partopfamily is a hash opfamily, look up its equality operator, and
         * select some btree opfamily that that operator is part of.  (Any
         * such opfamily should be good enough, since equivclass.c will track
         * multiple opfamilies as appropriate.)
         */
        if (*part_scheme).strategy == PARTITION_STRATEGY_HASH as c_char {
            let eq_op: Oid;
            let eq_opfamilies: *mut List;

            eq_op = get_opfamily_member(
                *(*part_scheme).partopfamily.add(ipk),
                *(*part_scheme).partopcintype.add(ipk),
                *(*part_scheme).partopcintype.add(ipk),
                HTEqualStrategyNumber as i16,
            );
            if !OidIsValid(eq_op) {
                break; /* we're not going to succeed */
            }
            eq_opfamilies = get_mergejoin_opfamilies(eq_op);
            if eq_opfamilies.is_null() {
                break; /* we're not going to succeed */
            }
            btree_opfamily = linitial_oid(eq_opfamilies);
        } else {
            btree_opfamily = *(*part_scheme).partopfamily.add(ipk);
        }

        /*
         * We consider only non-nullable partition keys here; nullable ones
         * would not be treated as part of the same equivalence classes as
         * non-nullable ones.
         */
        let mut found_this_key = false;
        foreach!(lc, *(*rel1).partexprs.add(ipk), {
            let expr1 = lfirst(crate::current_cell!(lc)) as *mut Node;
            let partcoll1 = *(*(*rel1).part_scheme).partcollation.add(ipk);
            let exprcoll1 = exprCollation(expr1);

            foreach!(lc2, *(*rel2).partexprs.add(ipk), {
                let expr2 = lfirst(crate::current_cell!(lc2)) as *mut Node;

                if exprs_known_equal(root, expr1, expr2, btree_opfamily) {
                    /*
                     * Ensure that the collation of the expression matches
                     * that of the partition key. Checking just one collation
                     * (partcoll1 and exprcoll1) suffices because partcoll1
                     * and partcoll2, as well as exprcoll1 and exprcoll2,
                     * should be identical. This holds because both rel1 and
                     * rel2 use the same PartitionScheme and expr1 and expr2
                     * are equal.
                     */
                    if partcoll1 == exprcoll1 {
                        /* Assert(partcoll2 == exprcoll2) -- debug only */
                        pk_known_equal[ipk] = true;
                        found_this_key = true;
                        break;
                    }
                }
            });
            if found_this_key {
                break;
            }
        });

        if pk_known_equal[ipk] {
            /* We can stop examining keys once we prove all keys equal. */
            num_equal_pks += 1;
            if num_equal_pks == (*part_scheme).partnatts as c_int {
                return true;
            }
        } else {
            break; /* no chance to succeed, give up */
        }
    }

    false
}

/*
 * match_expr_to_partition_keys
 *
 * Tries to match an expression to one of the nullable or non-nullable
 * partition keys of "rel".  Returns the matched key's ordinal position,
 * or -1 if the expression could not be matched to any of the keys.
 *
 * strict_op must be true if the expression will be compared with the
 * partition key using a strict operator.  This allows us to consider
 * nullable as well as nonnullable partition keys.
 */
unsafe fn match_expr_to_partition_keys(
    mut expr: *mut Expr,
    rel: *mut RelOptInfo,
    strict_op: bool,
) -> c_int {
    let mut cnt: c_int;

    /* This function should be called only for partitioned relations. */
    Assert!(!(*rel).part_scheme.is_null());
    Assert!(!(*rel).partexprs.is_null());
    Assert!(!(*rel).nullable_partexprs.is_null());

    /* Remove any relabel decorations. */
    while IsA!(expr, T_RelabelType) {
        expr = (*castNode!(RelabelType, T_RelabelType, expr)).arg as *mut Expr;
    }

    cnt = 0;
    while cnt < (*(*rel).part_scheme).partnatts as c_int {
        /* We can always match to the non-nullable partition keys. */
        foreach!(lc, *(*rel).partexprs.add(cnt as usize), {
            if equal(lfirst(crate::current_cell!(lc)) as *const c_void, expr as *const c_void) {
                return cnt;
            }
        });

        if !strict_op {
            cnt += 1;
            continue;
        }

        /*
         * If it's a strict join operator then a NULL partition key on one
         * side will not join to any partition key on the other side, and in
         * particular such a row can't join to a row from a different
         * partition on the other side.  So, it's okay to search the nullable
         * partition keys as well.
         */
        foreach!(lc, *(*rel).nullable_partexprs.add(cnt as usize), {
            if equal(lfirst(crate::current_cell!(lc)) as *const c_void, expr as *const c_void) {
                return cnt;
            }
        });

        cnt += 1;
    }

    -1
}

/*
 * set_joinrel_partition_key_exprs
 *		Initialize partition key expressions for a partitioned joinrel.
 */
unsafe fn set_joinrel_partition_key_exprs(
    joinrel: *mut RelOptInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
    jointype: JoinType,
) {
    let part_scheme = (*joinrel).part_scheme;
    let partnatts = (*part_scheme).partnatts as usize;

    (*joinrel).partexprs =
        palloc0(size_of::<*mut List>() * partnatts) as *mut *mut List;
    (*joinrel).nullable_partexprs =
        palloc0(size_of::<*mut List>() * partnatts) as *mut *mut List;

    /*
     * The joinrel's partition expressions are the same as those of the input
     * rels, but we must properly classify them as nullable or not in the
     * joinrel's output.  (Also, we add some more partition expressions if
     * it's a FULL JOIN.)
     */
    for cnt in 0..partnatts {
        /* mark these const to enforce that we copy them properly */
        let outer_expr = *(*outer_rel).partexprs.add(cnt);
        let outer_null_expr = *(*outer_rel).nullable_partexprs.add(cnt);
        let inner_expr = *(*inner_rel).partexprs.add(cnt);
        let inner_null_expr = *(*inner_rel).nullable_partexprs.add(cnt);
        let partexpr: *mut List;
        let nullable_partexpr: *mut List;

        match jointype {
            /*
             * A join relation resulting from an INNER join may be
             * regarded as partitioned by either of the inner and outer
             * relation keys.  For example, A INNER JOIN B ON A.a = B.b
             * can be regarded as partitioned on either A.a or B.b.  So we
             * add both keys to the joinrel's partexpr lists.  However,
             * anything that was already nullable still has to be treated
             * as nullable.
             */
            JOIN_INNER => {
                let pe = list_concat_copy(outer_expr, inner_expr);
                let ne = list_concat_copy(outer_null_expr, inner_null_expr);
                *(*joinrel).partexprs.add(cnt) = pe;
                *(*joinrel).nullable_partexprs.add(cnt) = ne;
            }

            /*
             * A join relation resulting from a SEMI or ANTI join may be
             * regarded as partitioned by the outer relation keys.  The
             * inner relation's keys are no longer interesting; since they
             * aren't visible in the join output, nothing could join to
             * them.
             */
            JOIN_SEMI | JOIN_ANTI => {
                *(*joinrel).partexprs.add(cnt) = list_copy(outer_expr);
                *(*joinrel).nullable_partexprs.add(cnt) = list_copy(outer_null_expr);
            }

            /*
             * A join relation resulting from a LEFT OUTER JOIN likewise
             * may be regarded as partitioned on the (non-nullable) outer
             * relation keys.  The inner (nullable) relation keys are okay
             * as partition keys for further joins as long as they involve
             * strict join operators.
             */
            JOIN_LEFT => {
                let mut ne = list_concat_copy(inner_expr, outer_null_expr);
                ne = list_concat(ne, inner_null_expr);
                *(*joinrel).partexprs.add(cnt) = list_copy(outer_expr);
                *(*joinrel).nullable_partexprs.add(cnt) = ne;
            }

            /*
             * For FULL OUTER JOINs, both relations are nullable, so the
             * resulting join relation may be regarded as partitioned on
             * either of inner and outer relation keys, but only for joins
             * that involve strict join operators.
             */
            JOIN_FULL => {
                let mut ne = list_concat_copy(outer_expr, inner_expr);
                ne = list_concat(ne, outer_null_expr);
                ne = list_concat(ne, inner_null_expr);

                /*
                 * Also add CoalesceExprs corresponding to each possible
                 * full-join output variable (that is, left side coalesced to
                 * right side), so that we can match equijoin expressions
                 * using those variables.  We really only need these for
                 * columns merged by JOIN USING, and only with the pairs of
                 * input items that correspond to the data structures that
                 * parse analysis would build for such variables.  But it's
                 * hard to tell which those are, so just make all the pairs.
                 * Extra items in the nullable_partexprs list won't cause big
                 * problems.  (It's possible that such items will get matched
                 * to user-written COALESCEs, but it should still be valid to
                 * partition on those, since they're going to be either the
                 * partition column or NULL; it's the same argument as for
                 * partitionwise nesting of any outer join.)  We assume no
                 * type coercions are needed to make the coalesce expressions,
                 * since columns of different types won't have gotten
                 * classified as the same PartitionScheme.  Note that we
                 * intentionally leave out the varnullingrels decoration that
                 * would ordinarily appear on the Vars inside these
                 * CoalesceExprs, because have_partkey_equi_join will strip
                 * varnullingrels from the expressions it will compare to the
                 * partexprs.
                 */
                foreach!(lc, list_concat_copy(outer_expr, outer_null_expr), {
                    let larg = lfirst(crate::current_cell!(lc)) as *mut Node;

                    foreach!(lc2, list_concat_copy(inner_expr, inner_null_expr), {
                        let rarg = lfirst(crate::current_cell!(lc2)) as *mut Node;
                        let c = makeNode!(CoalesceExpr, T_CoalesceExpr);

                        (*c).coalescetype = exprType(larg);
                        (*c).coalescecollid = exprCollation(larg);
                        (*c).args = crate::list_make2!(larg, rarg);
                        (*c).location = -1;
                        ne = lappend(ne, c as *mut c_void);
                    });
                });

                *(*joinrel).partexprs.add(cnt) = NIL;
                *(*joinrel).nullable_partexprs.add(cnt) = ne;
            }

            _ => {
                elog!(ERROR, "unrecognized join type: {}", jointype as c_int);
            }
        }
    }
}

/*
 * build_child_join_reltarget
 *	  Set up a child-join relation's reltarget from a parent-join relation.
 */
unsafe fn build_child_join_reltarget(
    root: *mut PlannerInfo,
    parentrel: *mut RelOptInfo,
    childrel: *mut RelOptInfo,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) {
    /* Build the targetlist */
    (*(*childrel).reltarget).exprs = adjust_appendrel_attrs(
        root,
        (*(*parentrel).reltarget).exprs as *mut Node,
        nappinfos,
        appinfos,
    ) as *mut List;

    /* Set the cost and width fields */
    (*(*childrel).reltarget).cost.startup = (*(*parentrel).reltarget).cost.startup;
    (*(*childrel).reltarget).cost.per_tuple = (*(*parentrel).reltarget).cost.per_tuple;
    (*(*childrel).reltarget).width = (*(*parentrel).reltarget).width;
}

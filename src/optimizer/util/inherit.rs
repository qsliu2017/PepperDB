//! src/backend/optimizer/util/inherit.c
//!
//! Routines to process child relations in inheritance trees
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::c::Index;

// NodeTag is needed in scope for makeNode!/castNode!/IsA! macro expansions.
use crate::nodes::nodes::NodeTag;
use crate::nodes::nodes::NodeTag::{T_Const, T_PlanRowMark, T_RangeTblEntry, T_RestrictInfo, T_Var};

// Node base type used by several helpers.
use crate::nodes::nodes::Node;

// CmdType / RTEKind enum variants used as bare values.
use crate::nodes::nodes::CmdType::CMD_UPDATE;
use crate::nodes::parsenodes::RTEKind::{RTE_RELATION, RTE_SUBQUERY};

// Crate-root #[macro_export] macros.
use crate::{castNode, current_cell, foreach, lfirst_node, makeNode, strVal, IsA};

// pg_list helpers (lappend/list_length/list_nth/lfirst/lfirst_oid/linitial_oid are FNs).
use crate::nodes::pg_list::{
    lappend, lfirst, lfirst_oid, linitial_oid, list_length, list_nth,
};

// Bitmapset helpers.
use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_del_member, bms_is_member, bms_make_singleton,
    bms_next_member, bms_num_members, bms_union,
};

// Locking / list-nil constants.
use crate::storage::lockdefs::NoLock;
use crate::nodes::pg_list::NIL;

// relkind, attribute numbers, and well-known OIDs.
use crate::catalog::pg_class::RELKIND_PARTITIONED_TABLE;
use crate::access::sysattr::{
    FirstLowInvalidHeapAttributeNumber, SelfItemPointerAttributeNumber, TableOidAttributeNumber,
};
use crate::access::attnum::InvalidAttrNumber;
use crate::catalog::pg_type_d::{OIDOID, TIDOID};

// RowMarkType bit position for the whole-row copy mark (kept as c_int so the
// shift expressions below type-check, matching the C source).
const ROW_MARK_COPY: c_int = crate::nodes::plannodes::ROW_MARK_COPY as c_int;

// ----------------------------------------------------------------------------
// Stub types (faithful structure; real definitions live in other modules)
// ----------------------------------------------------------------------------

type PlannerInfo = crate::nodes::pathnodes::PlannerInfo;
type RelOptInfo = crate::nodes::pathnodes::RelOptInfo;
type RangeTblEntry = crate::nodes::parsenodes::RangeTblEntry;
type RTEPermissionInfo = crate::nodes::parsenodes::RTEPermissionInfo;
type AppendRelInfo = crate::nodes::pathnodes::AppendRelInfo;
type PlanRowMark = crate::nodes::plannodes::PlanRowMark;
type Query = crate::nodes::parsenodes::Query;
type Var = crate::nodes::primnodes::Var;
type Const = crate::nodes::primnodes::Const;
type Expr = crate::nodes::primnodes::Expr;
type TargetEntry = crate::nodes::primnodes::TargetEntry;
type RestrictInfo = crate::nodes::pathnodes::RestrictInfo;
type Relation = crate::utils::rel::Relation;
type TupleDesc = crate::access::common::tupdesc::TupleDesc;
type Form_pg_attribute = crate::catalog::pg_attribute::Form_pg_attribute;
#[repr(C)]
pub struct PartitionDescData { pub nparts: c_int, pub oids: *mut Oid }
type PartitionDesc = *mut PartitionDescData;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;
type Bitmapset = crate::nodes::bitmapset::Bitmapset;
type LOCKMODE = c_int;

// ----------------------------------------------------------------------------

/*
 * expand_inherited_rtentry
 *		Expand a rangetable entry that has the "inh" bit set.
 *
 * "inh" is only allowed in two cases: RELATION and SUBQUERY RTEs.
 *
 * "inh" on a plain RELATION RTE means that it is a partitioned table or the
 * parent of a traditional-inheritance set.  In this case we must add entries
 * for all the interesting child tables to the query's rangetable, and build
 * additional planner data structures for them, including RelOptInfos,
 * AppendRelInfos, and possibly PlanRowMarks.
 *
 * Note that the original RTE is considered to represent the whole inheritance
 * set.  In the case of traditional inheritance, the first of the generated
 * RTEs is an RTE for the same table, but with inh = false, to represent the
 * parent table in its role as a simple member of the inheritance set.  For
 * partitioning, we don't need a second RTE because the partitioned table
 * itself has no data and need not be scanned.
 *
 * "inh" on a SUBQUERY RTE means that it's the parent of a UNION ALL group,
 * which is treated as an appendrel similarly to inheritance cases; however,
 * we already made RTEs and AppendRelInfos for the subqueries.  We only need
 * to build RelOptInfos for them, which is done by expand_appendrel_subquery.
 */
pub unsafe fn expand_inherited_rtentry(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
    rti: Index,
) {
    let parentOID: Oid;
    let oldrelation: Relation;
    let lockmode: LOCKMODE;
    let oldrc: *mut PlanRowMark;
    let mut old_isParent: bool = false;
    let mut old_allMarkTypes: c_int = 0;

    Assert!((*rte).inh); /* else caller error */

    if (*rte).rtekind == RTE_SUBQUERY {
        expand_appendrel_subquery(root, rel, rte, rti);
        return;
    }

    Assert!((*rte).rtekind == RTE_RELATION);

    parentOID = (*rte).relid;

    /*
     * We used to check has_subclass() here, but there's no longer any need
     * to, because subquery_planner already did.
     */

    /*
     * The rewriter should already have obtained an appropriate lock on each
     * relation named in the query, so we can open the parent relation without
     * locking it.  However, for each child relation we add to the query, we
     * must obtain an appropriate lock, because this will be the first use of
     * those relations in the parse/rewrite/plan pipeline.  Child rels should
     * use the same lockmode as their parent.
     */
    oldrelation = table_open(parentOID, NoLock);
    lockmode = (*rte).rellockmode as LOCKMODE;

    /*
     * If parent relation is selected FOR UPDATE/SHARE, we need to mark its
     * PlanRowMark as isParent = true, and generate a new PlanRowMark for each
     * child.
     */
    oldrc = get_plan_rowmark((*root).rowMarks, rti);
    if !oldrc.is_null() {
        old_isParent = (*oldrc).isParent;
        (*oldrc).isParent = true;
        /* Save initial value of allMarkTypes before children add to it */
        old_allMarkTypes = (*oldrc).allMarkTypes;
    }

    /* Scan the inheritance set and expand it */
    if (*(*oldrelation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        let perminfo: *mut RTEPermissionInfo;

        perminfo = getRTEPermissionInfo((*(*root).parse).rteperminfos, rte);

        /*
         * Partitioned table, so set up for partitioning.
         */
        Assert!((*rte).relkind == RELKIND_PARTITIONED_TABLE);

        /*
         * Recursively expand and lock the partitions.  While at it, also
         * extract the partition key columns of all the partitioned tables.
         */
        expand_partitioned_rtentry(
            root,
            rel,
            rte,
            rti,
            oldrelation,
            (*perminfo).updatedCols,
            oldrc,
            lockmode,
        );
    } else {
        /*
         * Ordinary table, so process traditional-inheritance children.  (Note
         * that partitioned tables are not allowed to have inheritance
         * children, so it's not possible for both cases to apply.)
         */
        let inhOIDs: *mut List;
        let l: *mut ListCell;

        /* Scan for all members of inheritance set, acquire needed locks */
        inhOIDs = find_all_inheritors(parentOID, lockmode, std::ptr::null_mut());

        /*
         * We used to special-case the situation where the table no longer has
         * any children, by clearing rte->inh and exiting.  That no longer
         * works, because this function doesn't get run until after decisions
         * have been made that depend on rte->inh.  We have to treat such
         * situations as normal inheritance.  The table itself should always
         * have been found, though.
         */
        Assert!(inhOIDs != NIL);
        Assert!(linitial_oid(inhOIDs) == parentOID);

        /* Expand simple_rel_array and friends to hold child objects. */
        expand_planner_arrays(root, list_length(inhOIDs));

        /*
         * Expand inheritance children in the order the OIDs were returned by
         * find_all_inheritors.
         */
        let _ = l;
        foreach!(l, inhOIDs, {
            let childOID: Oid = lfirst_oid(current_cell!(l));
            let newrelation: Relation;
            let mut childrte: *mut RangeTblEntry = std::ptr::null_mut();
            let mut childRTindex: Index = 0;

            /* Open rel if needed; we already have required locks */
            if childOID != parentOID {
                newrelation = table_open(childOID, NoLock);
            } else {
                newrelation = oldrelation;
            }

            /*
             * It is possible that the parent table has children that are temp
             * tables of other backends.  We cannot safely access such tables
             * (because of buffering issues), and the best thing to do seems
             * to be to silently ignore them.
             */
            if childOID != parentOID && RELATION_IS_OTHER_TEMP(newrelation) {
                table_close(newrelation, lockmode);
                continue;
            }

            /* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
            expand_single_inheritance_child(
                root,
                rte,
                rti,
                oldrelation,
                oldrc,
                newrelation,
                &mut childrte,
                &mut childRTindex,
            );

            /* Create the otherrel RelOptInfo too. */
            let _ = build_simple_rel(root, childRTindex, rel);

            /* Close child relations, but keep locks */
            if childOID != parentOID {
                table_close(newrelation, NoLock);
            }
        });
    }

    /*
     * Some children might require different mark types, which would've been
     * reported into oldrc.  If so, add relevant entries to the top-level
     * targetlist and update parent rel's reltarget.  This should match what
     * preprocess_targetlist() would have added if the mark types had been
     * requested originally.
     *
     * (Someday it might be useful to fold these resjunk columns into the
     * row-identity-column management used for UPDATE/DELETE.  Today is not
     * that day, however.)
     */
    if !oldrc.is_null() {
        let new_allMarkTypes: c_int = (*oldrc).allMarkTypes;
        let mut var: *mut Var;
        let mut tle: *mut TargetEntry;
        let mut resname: [c_char; 32] = [0; 32];
        let mut newvars: *mut List = NIL;

        /* Add TID junk Var if needed, unless we had it already */
        if (new_allMarkTypes & !(1 << ROW_MARK_COPY)) != 0
            && (old_allMarkTypes & !(1 << ROW_MARK_COPY)) == 0
        {
            /* Need to fetch TID */
            var = makeVar(
                (*oldrc).rti,
                SelfItemPointerAttributeNumber as crate::access::attnum::AttrNumber,
                TIDOID,
                -1,
                InvalidOid,
                0,
            );
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of_val(&resname),
                c"ctid%u".as_ptr(),
                (*oldrc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length((*root).processed_tlist) + 1) as crate::access::attnum::AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            (*root).processed_tlist = lappend((*root).processed_tlist, tle as *mut std::ffi::c_void);
            newvars = lappend(newvars, var as *mut std::ffi::c_void);
        }

        /* Add whole-row junk Var if needed, unless we had it already */
        if (new_allMarkTypes & (1 << ROW_MARK_COPY)) != 0
            && (old_allMarkTypes & (1 << ROW_MARK_COPY)) == 0
        {
            var = makeWholeRowVar(
                planner_rt_fetch((*oldrc).rti, root),
                (*oldrc).rti,
                0,
                false,
            );
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of_val(&resname),
                c"wholerow%u".as_ptr(),
                (*oldrc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length((*root).processed_tlist) + 1) as crate::access::attnum::AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            (*root).processed_tlist = lappend((*root).processed_tlist, tle as *mut std::ffi::c_void);
            newvars = lappend(newvars, var as *mut std::ffi::c_void);
        }

        /* Add tableoid junk Var, unless we had it already */
        if !old_isParent {
            var = makeVar(
                (*oldrc).rti,
                TableOidAttributeNumber as crate::access::attnum::AttrNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            );
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of_val(&resname),
                c"tableoid%u".as_ptr(),
                (*oldrc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length((*root).processed_tlist) + 1) as crate::access::attnum::AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            (*root).processed_tlist = lappend((*root).processed_tlist, tle as *mut std::ffi::c_void);
            newvars = lappend(newvars, var as *mut std::ffi::c_void);
        }

        /*
         * Add the newly added Vars to parent's reltarget.  We needn't worry
         * about the children's reltargets, they'll be made later.
         */
        add_vars_to_targetlist(root, newvars, bms_make_singleton(0));
    }

    table_close(oldrelation, NoLock);
}

/*
 * expand_partitioned_rtentry
 *		Recursively expand an RTE for a partitioned table.
 */
unsafe fn expand_partitioned_rtentry(
    root: *mut PlannerInfo,
    relinfo: *mut RelOptInfo,
    parentrte: *mut RangeTblEntry,
    parentRTindex: Index,
    parentrel: Relation,
    parent_updatedCols: *mut Bitmapset,
    top_parentrc: *mut PlanRowMark,
    lockmode: LOCKMODE,
) {
    let partdesc: PartitionDesc;
    let num_live_parts: c_int;
    let mut i: c_int;

    check_stack_depth();

    Assert!((*parentrte).inh);

    partdesc = PartitionDirectoryLookup((*(*root).glob).partition_directory, parentrel);

    /* A partitioned table should always have a partition descriptor. */
    Assert!(!partdesc.is_null());

    /*
     * Note down whether any partition key cols are being updated. Though it's
     * the root partitioned table's updatedCols we are interested in,
     * parent_updatedCols provided by the caller contains the root partrel's
     * updatedCols translated to match the attribute ordering of parentrel.
     */
    if !(*root).partColsUpdated {
        (*root).partColsUpdated =
            has_partition_attrs(parentrel, parent_updatedCols, std::ptr::null_mut());
    }

    /* Nothing further to do here if there are no partitions. */
    if (*partdesc).nparts == 0 {
        return;
    }

    /*
     * Perform partition pruning using restriction clauses assigned to parent
     * relation.  live_parts will contain PartitionDesc indexes of partitions
     * that survive pruning.  Below, we will initialize child objects for the
     * surviving partitions.
     */
    (*relinfo).live_parts = prune_append_rel_partitions(relinfo);

    /* Expand simple_rel_array and friends to hold child objects. */
    num_live_parts = bms_num_members((*relinfo).live_parts);
    if num_live_parts > 0 {
        expand_planner_arrays(root, num_live_parts);
    }

    /*
     * We also store partition RelOptInfo pointers in the parent relation.
     * Since we're palloc0'ing, slots corresponding to pruned partitions will
     * contain NULL.
     */
    Assert!((*relinfo).part_rels.is_null());
    (*relinfo).part_rels = palloc0(
        (*relinfo).nparts as usize * std::mem::size_of::<*mut RelOptInfo>(),
    ) as *mut *mut RelOptInfo;

    /*
     * Create a child RTE for each live partition.  Note that unlike
     * traditional inheritance, we don't need a child RTE for the partitioned
     * table itself, because it's not going to be scanned.
     */
    i = -1;
    loop {
        i = bms_next_member((*relinfo).live_parts, i);
        if i < 0 {
            break;
        }

        let childOID: Oid = *(*partdesc).oids.offset(i as isize);
        let childrel: Relation;
        let mut childrte: *mut RangeTblEntry = std::ptr::null_mut();
        let mut childRTindex: Index = 0;
        let childrelinfo: *mut RelOptInfo;

        /*
         * Open rel, acquiring required locks.  If a partition was recently
         * detached and subsequently dropped, then opening it will fail.  In
         * this case, behave as though the partition had been pruned.
         */
        childrel = try_table_open(childOID, lockmode);
        if childrel.is_null() {
            (*relinfo).live_parts = bms_del_member((*relinfo).live_parts, i);
            continue;
        }

        /*
         * Temporary partitions belonging to other sessions should have been
         * disallowed at definition, but for paranoia's sake, let's double
         * check.
         */
        if RELATION_IS_OTHER_TEMP(childrel) {
            elog!(
                ERROR,
                "temporary relation from another session found as partition"
            );
        }

        /* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
        expand_single_inheritance_child(
            root,
            parentrte,
            parentRTindex,
            parentrel,
            top_parentrc,
            childrel,
            &mut childrte,
            &mut childRTindex,
        );

        /* Create the otherrel RelOptInfo too. */
        childrelinfo = build_simple_rel(root, childRTindex, relinfo);
        *(*relinfo).part_rels.offset(i as isize) = childrelinfo;
        (*relinfo).all_partrels =
            bms_add_members((*relinfo).all_partrels, (*childrelinfo).relids);

        /* If this child is itself partitioned, recurse */
        if (*(*childrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            let appinfo: *mut AppendRelInfo =
                *(*root).append_rel_array.offset(childRTindex as isize);
            let child_updatedCols: *mut Bitmapset;

            child_updatedCols =
                translate_col_privs(parent_updatedCols, (*appinfo).translated_vars);

            expand_partitioned_rtentry(
                root,
                childrelinfo,
                childrte,
                childRTindex,
                childrel,
                child_updatedCols,
                top_parentrc,
                lockmode,
            );
        }

        /* Close child relation, but keep locks */
        table_close(childrel, NoLock);
    }
}

/*
 * expand_single_inheritance_child
 *		Build a RangeTblEntry and an AppendRelInfo, plus maybe a PlanRowMark.
 *
 * We now expand the partition hierarchy level by level, creating a
 * corresponding hierarchy of AppendRelInfos and RelOptInfos, where each
 * partitioned descendant acts as a parent of its immediate partitions.
 * (This is a difference from what older versions of PostgreSQL did and what
 * is still done in the case of table inheritance for unpartitioned tables,
 * where the hierarchy is flattened during RTE expansion.)
 *
 * PlanRowMarks still carry the top-parent's RTI, and the top-parent's
 * allMarkTypes field still accumulates values from all descendents.
 *
 * "parentrte" and "parentRTindex" are immediate parent's RTE and
 * RTI. "top_parentrc" is top parent's PlanRowMark.
 *
 * The child RangeTblEntry and its RTI are returned in "childrte_p" and
 * "childRTindex_p" resp.
 */
unsafe fn expand_single_inheritance_child(
    root: *mut PlannerInfo,
    parentrte: *mut RangeTblEntry,
    parentRTindex: Index,
    parentrel: Relation,
    top_parentrc: *mut PlanRowMark,
    childrel: Relation,
    childrte_p: *mut *mut RangeTblEntry,
    childRTindex_p: *mut Index,
) {
    let parse: *mut Query = (*root).parse;
    let parentOID: Oid = RelationGetRelid(parentrel); /* PG_USED_FOR_ASSERTS_ONLY */
    let childOID: Oid = RelationGetRelid(childrel);
    let childrte: *mut RangeTblEntry;
    let childRTindex: Index;
    let appinfo: *mut AppendRelInfo;
    let child_tupdesc: TupleDesc;
    let parent_colnames: *mut List;
    let mut child_colnames: *mut List;

    /*
     * Build an RTE for the child, and attach to query's rangetable list. We
     * copy most scalar fields of the parent's RTE, but replace relation OID,
     * relkind, and inh for the child.  Set the child's securityQuals to
     * empty, because we only want to apply the parent's RLS conditions
     * regardless of what RLS properties individual children may have. (This
     * is an intentional choice to make inherited RLS work like regular
     * permissions checks.) The parent securityQuals will be propagated to
     * children along with other base restriction clauses, so we don't need to
     * do it here.  Other infrastructure of the parent RTE has to be
     * translated to match the child table's column ordering, which we do
     * below, so a "flat" copy is sufficient to start with.
     */
    childrte = makeNode!(RangeTblEntry, T_RangeTblEntry);
    std::ptr::copy_nonoverlapping(
        parentrte as *const RangeTblEntry,
        childrte,
        1,
    );
    Assert!((*parentrte).rtekind == RTE_RELATION); /* else this is dubious */
    (*childrte).relid = childOID;
    (*childrte).relkind = (*(*childrel).rd_rel).relkind;
    /* A partitioned child will need to be expanded further. */
    if (*childrte).relkind == RELKIND_PARTITIONED_TABLE {
        Assert!(childOID != parentOID);
        (*childrte).inh = true;
    } else {
        (*childrte).inh = false;
    }
    (*childrte).securityQuals = NIL;

    /* No permission checking for child RTEs. */
    (*childrte).perminfoindex = 0;

    /* Link not-yet-fully-filled child RTE into data structures */
    (*parse).rtable = lappend((*parse).rtable, childrte as *mut std::ffi::c_void);
    childRTindex = list_length((*parse).rtable) as Index;
    *childrte_p = childrte;
    *childRTindex_p = childRTindex;

    /*
     * Build an AppendRelInfo struct for each parent/child pair.
     */
    appinfo = make_append_rel_info(parentrel, childrel, parentRTindex, childRTindex);
    (*root).append_rel_list = lappend((*root).append_rel_list, appinfo as *mut std::ffi::c_void);

    /* tablesample is probably null, but copy it */
    (*childrte).tablesample = copyObject((*parentrte).tablesample as *mut std::ffi::c_void)
        as *mut crate::nodes::parsenodes::TableSampleClause;

    /*
     * Construct an alias clause for the child, which we can also use as eref.
     * This is important so that EXPLAIN will print the right column aliases
     * for child-table columns.  (Since ruleutils.c doesn't have any easy way
     * to reassociate parent and child columns, we must get the child column
     * aliases right to start with.  Note that setting childrte->alias forces
     * ruleutils.c to use these column names, which it otherwise would not.)
     */
    child_tupdesc = RelationGetDescr(childrel);
    parent_colnames = (*(*parentrte).eref).colnames;
    child_colnames = NIL;
    let mut cattno: c_int = 0;
    while cattno < (*child_tupdesc).natts {
        let att: Form_pg_attribute = TupleDescAttr(child_tupdesc, cattno);
        let attname: *const c_char;

        if (*att).attisdropped {
            /* Always insert an empty string for a dropped column */
            attname = c"".as_ptr();
        } else if *(*appinfo).parent_colnos.offset(cattno as isize) > 0
            && *(*appinfo).parent_colnos.offset(cattno as isize)
                <= list_length(parent_colnames) as crate::access::attnum::AttrNumber
        {
            /* Duplicate the query-assigned name for the parent column */
            attname = strVal!(list_nth(
                parent_colnames,
                (*(*appinfo).parent_colnos.offset(cattno as isize) - 1) as c_int,
            ));
        } else {
            /* New column, just use its real name */
            attname = NameStr((*att).attname);
        }
        child_colnames = lappend(
            child_colnames,
            makeString(pstrdup(attname)) as *mut std::ffi::c_void,
        );
        cattno += 1;
    }

    /*
     * We just duplicate the parent's table alias name for each child.  If the
     * plan gets printed, ruleutils.c has to sort out unique table aliases to
     * use, which it can handle.
     */
    let new_alias = makeAlias((*(*parentrte).eref).aliasname, child_colnames);
    (*childrte).eref = new_alias;
    (*childrte).alias = new_alias;

    /*
     * Store the RTE and appinfo in the respective PlannerInfo arrays, which
     * the caller must already have allocated space for.
     */
    Assert!(childRTindex < (*root).simple_rel_array_size as Index);
    Assert!((*(*root).simple_rte_array.offset(childRTindex as isize)).is_null());
    *(*root).simple_rte_array.offset(childRTindex as isize) = childrte;
    Assert!((*(*root).append_rel_array.offset(childRTindex as isize)).is_null());
    *(*root).append_rel_array.offset(childRTindex as isize) = appinfo;

    /*
     * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
     */
    if !top_parentrc.is_null() {
        let childrc: *mut PlanRowMark = makeNode!(PlanRowMark, T_PlanRowMark);

        (*childrc).rti = childRTindex;
        (*childrc).prti = (*top_parentrc).rti;
        (*childrc).rowmarkId = (*top_parentrc).rowmarkId;
        /* Reselect rowmark type, because relkind might not match parent */
        (*childrc).markType = select_rowmark_type(childrte, (*top_parentrc).strength);
        (*childrc).allMarkTypes = 1 << ((*childrc).markType as c_int);
        (*childrc).strength = (*top_parentrc).strength;
        (*childrc).waitPolicy = (*top_parentrc).waitPolicy;

        /*
         * We mark RowMarks for partitioned child tables as parent RowMarks so
         * that the executor ignores them (except their existence means that
         * the child tables will be locked using the appropriate mode).
         */
        (*childrc).isParent = (*childrte).relkind == RELKIND_PARTITIONED_TABLE;

        /* Include child's rowmark type in top parent's allMarkTypes */
        (*top_parentrc).allMarkTypes |= (*childrc).allMarkTypes;

        (*root).rowMarks = lappend((*root).rowMarks, childrc as *mut std::ffi::c_void);
    }

    /*
     * If we are creating a child of the query target relation (only possible
     * in UPDATE/DELETE/MERGE), add it to all_result_relids, as well as
     * leaf_result_relids if appropriate, and make sure that we generate
     * required row-identity data.
     */
    if bms_is_member(parentRTindex as c_int, (*root).all_result_relids) {
        /* OK, record the child as a result rel too. */
        (*root).all_result_relids =
            bms_add_member((*root).all_result_relids, childRTindex as c_int);

        /* Non-leaf partitions don't need any row identity info. */
        if (*childrte).relkind != RELKIND_PARTITIONED_TABLE {
            let rrvar: *mut Var;

            (*root).leaf_result_relids =
                bms_add_member((*root).leaf_result_relids, childRTindex as c_int);

            /*
             * If we have any child target relations, assume they all need to
             * generate a junk "tableoid" column.  (If only one child survives
             * pruning, we wouldn't really need this, but it's not worth
             * thrashing about to avoid it.)
             */
            rrvar = makeVar(
                childRTindex,
                TableOidAttributeNumber as crate::access::attnum::AttrNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            );
            add_row_identity_var(root, rrvar, childRTindex, c"tableoid".as_ptr());

            /* Register any row-identity columns needed by this child. */
            add_row_identity_columns(root, childRTindex, childrte, childrel);
        }
    }
}

/*
 * get_rel_all_updated_cols
 * 		Returns the set of columns of a given "simple" relation that are
 * 		updated by this query.
 */
pub unsafe fn get_rel_all_updated_cols(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
) -> *mut Bitmapset {
    let relid: Index;
    let rte: *mut RangeTblEntry;
    let perminfo: *mut RTEPermissionInfo;
    let mut updatedCols: *mut Bitmapset;
    let extraUpdatedCols: *mut Bitmapset;

    Assert!((*(*root).parse).commandType == CMD_UPDATE);
    Assert!(IS_SIMPLE_REL(rel));

    /*
     * We obtain updatedCols for the query's result relation.  Then, if
     * necessary, we map it to the column numbers of the relation for which
     * they were requested.
     */
    relid = (*(*root).parse).resultRelation as Index;
    rte = planner_rt_fetch(relid, root);
    perminfo = getRTEPermissionInfo((*(*root).parse).rteperminfos, rte);

    updatedCols = (*perminfo).updatedCols;

    if (*rel).relid != relid {
        let top_parent_rel: *mut RelOptInfo = find_base_rel(root, relid as c_int);

        Assert!(IS_OTHER_REL(rel));

        updatedCols = translate_col_privs_multilevel(root, rel, top_parent_rel, updatedCols);
    }

    /*
     * Now we must check to see if there are any generated columns that depend
     * on the updatedCols, and add them to the result.
     */
    extraUpdatedCols = get_dependent_generated_columns(root, (*rel).relid, updatedCols);

    bms_union(updatedCols, extraUpdatedCols)
}

/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.  That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 */
unsafe fn translate_col_privs(
    parent_privs: *const Bitmapset,
    translated_vars: *mut List,
) -> *mut Bitmapset {
    let mut child_privs: *mut Bitmapset = std::ptr::null_mut();
    let whole_row: bool;
    let mut attno: c_int;
    let lc: *mut ListCell;

    /* System attributes have the same numbers in all tables */
    attno = FirstLowInvalidHeapAttributeNumber as i32 + 1;
    while attno < 0 {
        if bms_is_member(
            attno - FirstLowInvalidHeapAttributeNumber as i32,
            parent_privs as *mut Bitmapset,
        ) {
            child_privs = bms_add_member(
                child_privs,
                attno - FirstLowInvalidHeapAttributeNumber as i32,
            );
        }
        attno += 1;
    }

    /* Check if parent has whole-row reference */
    whole_row = bms_is_member(
        InvalidAttrNumber as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
        parent_privs as *mut Bitmapset,
    );

    /* And now translate the regular user attributes, using the vars list */
    attno = InvalidAttrNumber as c_int;
    let _ = lc;
    foreach!(lc, translated_vars, {
        let var: *mut Var = lfirst_node!(Var, T_Var, current_cell!(lc));

        attno += 1;
        if var.is_null() {
            /* ignore dropped columns */
            continue;
        }
        if whole_row
            || bms_is_member(
                attno - FirstLowInvalidHeapAttributeNumber as i32,
                parent_privs as *mut Bitmapset,
            )
        {
            child_privs = bms_add_member(
                child_privs,
                (*var).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );
        }
    });

    child_privs
}

/*
 * translate_col_privs_multilevel
 *		Recursively translates the column numbers contained in 'parent_cols'
 *		to the column numbers of a descendant relation given by 'rel'
 *
 * Note that because this is based on translate_col_privs, it will expand
 * a whole-row reference into all inherited columns.  This is not an issue
 * for current usages, but beware.
 */
unsafe fn translate_col_privs_multilevel(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    parent_rel: *mut RelOptInfo,
    mut parent_cols: *mut Bitmapset,
) -> *mut Bitmapset {
    let appinfo: *mut AppendRelInfo;

    /* Fast path for easy case. */
    if parent_cols.is_null() {
        return std::ptr::null_mut();
    }

    /* Recurse if immediate parent is not the top parent. */
    if (*rel).parent != parent_rel {
        if !(*rel).parent.is_null() {
            parent_cols =
                translate_col_privs_multilevel(root, (*rel).parent, parent_rel, parent_cols);
        } else {
            elog!(ERROR, "rel with relid {} is not a child rel", (*rel).relid);
        }
    }

    /* Now translate for this child. */
    Assert!(!(*root).append_rel_array.is_null());
    appinfo = *(*root).append_rel_array.offset((*rel).relid as isize);
    Assert!(!appinfo.is_null());

    translate_col_privs(parent_cols, (*appinfo).translated_vars)
}

/*
 * expand_appendrel_subquery
 *		Add "other rel" RelOptInfos for the children of an appendrel baserel
 *
 * "rel" is a subquery relation that has the rte->inh flag set, meaning it
 * is a UNION ALL subquery that's been flattened into an appendrel, with
 * child subqueries listed in root->append_rel_list.  We need to build
 * a RelOptInfo for each child relation so that we can plan scans on them.
 */
unsafe fn expand_appendrel_subquery(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    _rte: *mut RangeTblEntry,
    rti: Index,
) {
    let l: *mut ListCell;

    let _ = l;
    foreach!(l, (*root).append_rel_list, {
        let appinfo: *mut AppendRelInfo = lfirst(current_cell!(l)) as *mut AppendRelInfo;
        let childRTindex: Index = (*appinfo).child_relid;
        let childrte: *mut RangeTblEntry;
        let childrel: *mut RelOptInfo;

        /* append_rel_list contains all append rels; ignore others */
        if (*appinfo).parent_relid != rti {
            continue;
        }

        /* find the child RTE, which should already exist */
        Assert!(childRTindex < (*root).simple_rel_array_size as Index);
        childrte = *(*root).simple_rte_array.offset(childRTindex as isize);
        Assert!(!childrte.is_null());

        /* Build the child RelOptInfo. */
        childrel = build_simple_rel(root, childRTindex, rel);

        /* Child may itself be an inherited rel, either table or subquery. */
        if (*childrte).inh {
            expand_inherited_rtentry(root, childrel, childrte, childRTindex);
        }
    });
}

/*
 * apply_child_basequals
 *		Populate childrel's base restriction quals from parent rel's quals,
 *		translating Vars using appinfo and re-checking for quals which are
 *		constant-TRUE or constant-FALSE when applied to this child relation.
 *
 * If any of the resulting clauses evaluate to constant false or NULL, we
 * return false and don't apply any quals.  Caller should mark the relation as
 * a dummy rel in this case, since it doesn't need to be scanned.  Constant
 * true quals are ignored.
 */
pub unsafe fn apply_child_basequals(
    root: *mut PlannerInfo,
    parentrel: *mut RelOptInfo,
    childrel: *mut RelOptInfo,
    childRTE: *mut RangeTblEntry,
    appinfo: *mut AppendRelInfo,
) -> bool {
    let mut childquals: *mut List;
    let mut cq_min_security: Index;
    let lc: *mut ListCell;

    /*
     * The child rel's targetlist might contain non-Var expressions, which
     * means that substitution into the quals could produce opportunities for
     * const-simplification, and perhaps even pseudoconstant quals. Therefore,
     * transform each RestrictInfo separately to see if it reduces to a
     * constant or pseudoconstant.  (We must process them separately to keep
     * track of the security level of each qual.)
     */
    childquals = NIL;
    cq_min_security = u32::MAX as Index;
    let _ = lc;
    let mut appinfo_arr: *mut AppendRelInfo = appinfo;
    foreach!(lc, (*parentrel).baserestrictinfo, {
        let rinfo: *mut RestrictInfo = lfirst(current_cell!(lc)) as *mut RestrictInfo;
        let mut childqual: *mut Node;
        let lc2: *mut ListCell;

        Assert!(IsA!(rinfo, T_RestrictInfo));
        childqual = adjust_appendrel_attrs(
            root,
            (*rinfo).clause as *mut Node,
            1,
            &mut appinfo_arr,
        );
        childqual = eval_const_expressions(root, childqual);
        /* check for flat-out constant */
        if !childqual.is_null() && IsA!(childqual, T_Const) {
            if (*(childqual as *mut Const)).constisnull
                || !DatumGetBool((*(childqual as *mut Const)).constvalue)
            {
                /* Restriction reduces to constant FALSE or NULL */
                return false;
            }
            /* Restriction reduces to constant TRUE, so drop it */
            continue;
        }
        /* might have gotten an AND clause, if so flatten it */
        let _ = lc2;
        foreach!(lc2, make_ands_implicit(childqual as *mut Expr), {
            let onecq: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;
            let pseudoconstant: bool;
            let childrinfo: *mut RestrictInfo;

            /* check for pseudoconstant (no Vars or volatile functions) */
            pseudoconstant =
                !contain_vars_of_level(onecq, 0) && !contain_volatile_functions(onecq);
            if pseudoconstant {
                /* tell createplan.c to check for gating quals */
                (*root).hasPseudoConstantQuals = true;
            }
            /* reconstitute RestrictInfo with appropriate properties */
            childrinfo = make_restrictinfo(
                root,
                onecq as *mut Expr,
                (*rinfo).is_pushed_down,
                (*rinfo).has_clone,
                (*rinfo).is_clone,
                pseudoconstant,
                (*rinfo).security_level,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );

            /* Restriction is proven always false */
            if restriction_is_always_false(root, childrinfo) {
                return false;
            }
            /* Restriction is proven always true, so drop it */
            if restriction_is_always_true(root, childrinfo) {
                continue;
            }

            childquals = lappend(childquals, childrinfo as *mut std::ffi::c_void);
            /* track minimum security level among child quals */
            cq_min_security = Min(cq_min_security, (*rinfo).security_level);
        });
    });

    /*
     * In addition to the quals inherited from the parent, we might have
     * securityQuals associated with this particular child node.  (Currently
     * this can only happen in appendrels originating from UNION ALL;
     * inheritance child tables don't have their own securityQuals, see
     * expand_single_inheritance_child().)  Pull any such securityQuals up
     * into the baserestrictinfo for the child.  This is similar to
     * process_security_barrier_quals() for the parent rel, except that we
     * can't make any general deductions from such quals, since they don't
     * hold for the whole appendrel.
     */
    if !(*childRTE).securityQuals.is_null() {
        let mut security_level: Index = 0;

        let lc_s: *mut ListCell;
        let _ = lc_s;
        foreach!(lc_s, (*childRTE).securityQuals, {
            let qualset: *mut List = lfirst(current_cell!(lc_s)) as *mut List;
            let lc2: *mut ListCell;

            let _ = lc2;
            foreach!(lc2, qualset, {
                let qual: *mut Expr = lfirst(current_cell!(lc2)) as *mut Expr;

                /* not likely that we'd see constants here, so no check */
                childquals = lappend(
                    childquals,
                    make_restrictinfo(
                        root,
                        qual,
                        true,
                        false,
                        false,
                        false,
                        security_level,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                    ) as *mut std::ffi::c_void,
                );
                cq_min_security = Min(cq_min_security, security_level);
            });
            security_level += 1;
        });
        Assert!(security_level <= (*root).qual_security_level);
    }

    /*
     * OK, we've got all the baserestrictinfo quals for this child.
     */
    (*childrel).baserestrictinfo = childquals;
    (*childrel).baserestrict_min_security = cq_min_security;

    true
}

// ----------------------------------------------------------------------------
// Local stubs for unported helpers
// ----------------------------------------------------------------------------

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

unsafe fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    crate::access::table::table::table_open(relid, lockmode as _) as _
}
unsafe fn try_table_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    crate::access::table::table::try_table_open(relid, lockmode as _) as _
}
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) {
    crate::access::table::table::table_close(rel as _, lockmode as _)
}
unsafe fn get_plan_rowmark(rowmarks: *mut List, rtindex: Index) -> *mut PlanRowMark {
    crate::optimizer::prep::preptlist::get_plan_rowmark(rowmarks as _, rtindex as _) as _
}
unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::getRTEPermissionInfo(rteperminfos as _, rte as _) as _
}
unsafe fn find_all_inheritors(
    parentrel_id: Oid,
    lockmode: LOCKMODE,
    numparents: *mut *mut List,
) -> *mut List {
    crate::catalog::pg_inherits::find_all_inheritors(parentrel_id, lockmode as _, numparents as _)
        as _
}
unsafe fn expand_planner_arrays(root: *mut PlannerInfo, add_size: c_int) {
    crate::optimizer::util::relnode::expand_planner_arrays(root as _, add_size)
}
unsafe fn build_simple_rel(
    root: *mut PlannerInfo,
    relid: Index,
    parent: *mut RelOptInfo,
) -> *mut RelOptInfo {
    crate::optimizer::util::relnode::build_simple_rel(root as _, relid as _, parent as _) as _
}
unsafe fn RELATION_IS_OTHER_TEMP(rel: Relation) -> bool {
    (*(*rel).rd_rel).relpersistence == b't' as c_char && !(*rel).rd_islocaltemp
}
unsafe fn makeVar(
    varno: Index,
    varattno: crate::access::attnum::AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: Index,
) -> *mut Var {
    crate::nodes::makefuncs::makeVar(
        varno as _,
        varattno as _,
        vartype,
        vartypmod,
        varcollid,
        varlevelsup as _,
    ) as _
}
unsafe fn makeWholeRowVar(
    rte: *mut RangeTblEntry,
    varno: Index,
    varlevelsup: Index,
    allow_scalar: bool,
) -> *mut Var {
    crate::nodes::makefuncs::makeWholeRowVar(rte as _, varno as _, varlevelsup as _, allow_scalar)
        as _
}
unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: crate::access::attnum::AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    crate::nodes::makefuncs::makeTargetEntry(expr as _, resno as _, resname, resjunk) as _
}
unsafe fn makeAlias(
    aliasname: *const c_char,
    colnames: *mut List,
) -> *mut crate::nodes::primnodes::Alias {
    crate::nodes::makefuncs::makeAlias(aliasname, colnames as _) as _
}
unsafe fn makeString(str: *mut c_char) -> *mut crate::nodes::value::String {
    crate::nodes::value::makeString(str) as _
}
unsafe fn planner_rt_fetch(rti: Index, root: *mut PlannerInfo) -> *mut RangeTblEntry {
    crate::optimizer::util::pathnode::planner_rt_fetch(rti as _, root as _) as _
}
unsafe fn add_vars_to_targetlist(
    root: *mut PlannerInfo,
    vars: *mut List,
    where_needed: *mut Bitmapset,
) {
    crate::optimizer::plan::initsplan::add_vars_to_targetlist(root as _, vars as _, where_needed as _)
}
unsafe fn PartitionDirectoryLookup(
    pdir: crate::partitioning::partdefs::PartitionDirectory,
    rel: Relation,
) -> PartitionDesc {
    crate::partitioning::partdesc::PartitionDirectoryLookup(pdir as _, rel as _) as _
}
unsafe fn has_partition_attrs(
    rel: Relation,
    attnums: *mut Bitmapset,
    used_in_expr: *mut bool,
) -> bool {
    crate::catalog::partition::has_partition_attrs(rel as _, attnums as _, used_in_expr)
}
unsafe fn prune_append_rel_partitions(rel: *mut RelOptInfo) -> *mut Bitmapset {
    crate::partitioning::partprune::prune_append_rel_partitions(rel as _) as _
}
unsafe fn make_append_rel_info(
    parentrel: Relation,
    childrel: Relation,
    parent_rtindex: Index,
    child_rtindex: Index,
) -> *mut AppendRelInfo {
    crate::optimizer::util::appendinfo::make_append_rel_info(
        parentrel as _,
        childrel as _,
        parent_rtindex as _,
        child_rtindex as _,
    ) as _
}
unsafe fn select_rowmark_type(
    rte: *mut RangeTblEntry,
    strength: crate::nodes::lockoptions::LockClauseStrength,
) -> crate::nodes::plannodes::RowMarkType {
    crate::optimizer::plan::planner::select_rowmark_type(rte as _, strength as _) as _
}
unsafe fn add_row_identity_var(
    root: *mut PlannerInfo,
    orig_var: *mut Var,
    rtindex: Index,
    rowid_name: *const c_char,
) {
    crate::optimizer::util::appendinfo::add_row_identity_var(
        root as _,
        orig_var as _,
        rtindex as _,
        rowid_name,
    )
}
unsafe fn add_row_identity_columns(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    crate::optimizer::util::appendinfo::add_row_identity_columns(
        root as _,
        rtindex as _,
        target_rte as _,
        target_relation as _,
    )
}
unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo {
    crate::optimizer::util::relnode::find_base_rel(root as _, relid) as _
}
unsafe fn get_dependent_generated_columns(
    root: *mut PlannerInfo,
    rti: Index,
    target_cols: *mut Bitmapset,
) -> *mut Bitmapset {
    crate::optimizer::util::plancat::get_dependent_generated_columns(
        root as _,
        rti as _,
        target_cols as _,
    ) as _
}
unsafe fn adjust_appendrel_attrs(
    root: *mut PlannerInfo,
    node: *mut Node,
    nappinfos: c_int,
    appinfos: *mut *mut AppendRelInfo,
) -> *mut Node {
    crate::optimizer::util::appendinfo::adjust_appendrel_attrs(
        root as _,
        node as _,
        nappinfos,
        appinfos as _,
    ) as _
}
unsafe fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::eval_const_expressions(root as _, node as _) as _
}
unsafe fn make_ands_implicit(clause: *mut Expr) -> *mut List {
    crate::nodes::makefuncs::make_ands_implicit(clause as _) as _
}
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::util::var::contain_vars_of_level(node as _, levelsup)
}
unsafe fn contain_volatile_functions(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions(clause as _)
}
unsafe fn make_restrictinfo(
    root: *mut PlannerInfo,
    clause: *mut Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: *mut crate::nodes::pathnodes::Relids,
    incompatible_relids: *mut crate::nodes::pathnodes::Relids,
    outer_relids: *mut crate::nodes::pathnodes::Relids,
) -> *mut RestrictInfo {
    crate::optimizer::util::restrictinfo::make_restrictinfo(
        root as _,
        clause as _,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level as _,
        required_relids as _,
        incompatible_relids as _,
        outer_relids as _,
    ) as _
}
unsafe fn restriction_is_always_false(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    crate::optimizer::plan::initsplan::restriction_is_always_false(root as _, restrictinfo as _)
}
unsafe fn restriction_is_always_true(
    root: *mut PlannerInfo,
    restrictinfo: *mut RestrictInfo,
) -> bool {
    crate::optimizer::plan::initsplan::restriction_is_always_true(root as _, restrictinfo as _)
}
unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    crate::utils::rel::RelationGetRelid(relation as _)
}
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    crate::utils::rel::RelationGetDescr(relation as _) as _
}
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc as _, i) as _
}
unsafe fn NameStr(name: crate::c::NameData) -> *const c_char {
    name.data.as_ptr() as *const c_char
}
unsafe fn check_stack_depth() {
    crate::miscadmin::check_stack_depth()
}
unsafe fn IS_SIMPLE_REL(rel: *mut RelOptInfo) -> bool {
    crate::nodes::pathnodes::IS_SIMPLE_REL(rel as _)
}
unsafe fn IS_OTHER_REL(rel: *mut RelOptInfo) -> bool {
    crate::nodes::pathnodes::IS_OTHER_REL(rel as _)
}
unsafe fn copyObject<T>(from: *const T) -> *mut T {
    crate::nodes::copyfuncs::copyObjectImpl(from as *const std::ffi::c_void) as *mut T
}

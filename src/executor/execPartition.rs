/*-------------------------------------------------------------------------
 *
 * execPartition.c
 *	  Support routines for partitioning.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/execPartition.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use crate::prelude::*;
use crate::nodes::primnodes::{INNER_VAR, OUTER_VAR};
use std::ffi::{c_char, c_int, c_void, CStr};
use std::mem::size_of;
use std::ptr;

/* offsetof maps to the std offset_of! macro (same Struct, field syntax). */
macro_rules! offsetof {
    ($t:ty, $field:ident) => {
        core::mem::offset_of!($t, $field)
    };
}

/* likely/unlikely branch hints -- no-op pass-through in Rust. */
macro_rules! likely {
    ($e:expr) => { $e };
}
macro_rules! unlikely {
    ($e:expr) => { $e };
}

use crate::nodes::pg_list::{
    List, NIL, lappend, lappend_int, lappend_oid, lfirst, lfirst_int, lfirst_oid,
    list_head, lnext, list_length, list_nth, list_free, list_member_oid, linitial,
};
use crate::nodes::bitmapset::{
    Bitmapset, bms_add_member, bms_add_members, bms_copy, bms_equal, bms_free,
    bms_is_empty, bms_is_member, bms_next_member, bms_num_members, bms_add_range,
};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, ModifyTableState, MergeActionState,
    OnConflictSetState, ProjectionInfo, ResultRelInfo, TupleConversionMap,
    TupleTableSlot, PartitionTupleRouting, PartitionPruneState,
    FdwRoutine,
};
use crate::nodes::plannodes::{
    ModifyTable, Plan,
    PartitionPruneInfo, PartitionedRelPruneInfo, PartitionPruneStepOp,
};
use crate::nodes::primnodes::{Expr, MergeAction, MergeMatchKind};
use crate::nodes::nodes::{CmdType, NodeTag, OnConflictAction, Node};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::nodes::OnConflictAction::*;
use crate::nodes::parsenodes::{
    WithCheckOption, AclMode, ACL_SELECT,
    PartitionStrategy, PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST,
    PARTITION_STRATEGY_RANGE, PartitionRangeDatumKind,
};
use crate::access::common::attmap::{
    build_attrmap_by_name, build_attrmap_by_name_if_req, AttrMap,
};
use crate::access::common::tupconvert::execute_attr_map_slot;
use crate::access::table::table::{table_open, table_close};
use crate::access::table::tableam::table_slot_create;
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::catalog::partition::get_partition_ancestors;
use crate::catalog::aclchk::{pg_class_aclcheck, pg_attribute_aclcheck};
use crate::utils::adt::acl::{AclResult, ACLCHECK_OK};
use crate::utils::rel::{
    Relation, RelationGetRelid, RelationGetDescr, RelationGetRelationName,
    RelationGetForm, RelationData,
};
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll, OidOutputFunctionCall};
use crate::utils::palloc::{palloc, palloc0, pfree, MemoryContextSwitchTo};
use crate::utils::mmgr::mcxt::CurrentMemoryContext;
use crate::utils::misc::rls::{check_enable_rls, RLS_ENABLED};
use crate::utils::cache::partcache::{
    PartitionKey, PartitionKeyData, RelationGetPartitionKey,
    get_partition_natts, get_partition_col_attnum, get_partition_col_typid,
};
use crate::nodes::parsenodes::{
    PARTITION_STRATEGY_HASH as PK_HASH,
    PARTITION_STRATEGY_LIST as PK_LIST,
    PARTITION_STRATEGY_RANGE as PK_RANGE,
};
use crate::nodes::pathnodes::PartitionBoundInfoData;
use crate::partitioning::partdesc::{
    PartitionDesc, PartitionDescData, PartitionBoundInfo,
    PartitionDirectory, CreatePartitionDirectory, PartitionDirectoryLookup,
};
use crate::partitioning::partbounds::{
    partition_list_bsearch,
    partition_range_datum_bsearch, partition_rbound_datum_cmp,
    compute_partition_hash_value, PartitionBoundInfoFull,
};

/* static inline in partitioning/partbounds.h */
#[inline]
unsafe fn partition_bound_accepts_nulls(bi: PartitionBoundInfo) -> bool {
    !bi.is_null() && (*(bi as *mut PartitionBoundInfoFull)).null_index != -1
}
use crate::executor::execUtils::{
    ExecGetRootToChildMap, ExecGetChildToRootMap,
    GetPerTupleExprContext, GetPerTupleMemoryContext,
    ResetExprContext, CreateExprContext,
};
use crate::executor::execExpr::ExecPrepareExprList;
use crate::executor::executor::{
    ExecBuildProjectionInfo, ExecBuildUpdateProjection,
    ExecInitQual, ExecInitExpr, ExecInitExprWithParams,
    CheckValidResultRel, ExecConstraints, ExecPartitionCheck,
    InitResultRelInfo, ExecGetRangeTableRelation,
    EXEC_FLAG_EXPLAIN_GENERIC,
};
use crate::executor::execIndexing::{ExecOpenIndices, ExecCloseIndices};
use crate::executor::execTuples::{
    MakeSingleTupleTableSlot, ExecDropSingleTupleTableSlot, TTSOpsVirtual,
};
use crate::executor::tuptable::ExecClearTuple;
use crate::executor::nodeModifyTable::{
    ExecLookupResultRelByOid, ExecInitMergeTupleSlots,
};
use crate::rewrite::rewriteManip::map_variable_attnos;
// TODO(pg-port): ruleutils not yet wired - local stub
unsafe fn pg_get_partkeydef_columns(_relid: Oid, _pretty: bool) -> *mut std::ffi::c_char { core::ptr::null_mut() }
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, appendStringInfoString,
    appendBinaryStringInfo, appendStringInfoChar,
};
use crate::miscadmin::GetUserId;
use crate::pg_config_manual::PARTITION_MAX_KEYS;
use crate::postgres::DatumGetInt32;
/* AllocSetContextCreate!, ALLOCSET_DEFAULT_SIZES, MemoryContextReset come via prelude. */
use crate::{makeNode, castNode, foreach, current_cell, IsA, lfirst_node};
use crate::storage::lockdefs::{RowExclusiveLock, NoLock};

/* CHECK_FOR_INTERRUPTS is a macro in C (miscadmin.h); local no-op shim per port convention. */
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{}};
}

/* TODO(pg-port): real PartitionStrategy type alias - using c_char */
type PartitionStrategyChar = c_char;

/* TODO(pg-port): lgetTypeOutputInfo + pg_mbcliplen -- utils/cache/lsyscache.c + mb/mbutils.c */
unsafe fn getTypeOutputInfo(_type_oid: Oid, typOutput: *mut Oid, typIsVarlena: *mut bool) {
    *typOutput = 0;
    *typIsVarlena = false;
}

unsafe fn pg_mbcliplen(_mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    if len <= limit { len } else { limit }
}

/* TODO(pg-port): real bmsToString lives in nodes/outfuncs.c */
unsafe fn bmsToString(bms: *const Bitmapset) -> *mut c_char {
    unimplemented!("TODO(pg-port): bmsToString - nodes/outfuncs.c")
}

/* TODO(pg-port): get_matching_partitions from partitioning/partprune.c */
unsafe fn get_matching_partitions(
    context: *mut PartitionPruneContext,
    pruning_steps: *mut List,
) -> *mut Bitmapset {
    unimplemented!("TODO(pg-port): get_matching_partitions - partitioning/partprune.c")
}

/* TODO(pg-port): check_stack_depth from port/misc.c */
unsafe fn check_stack_depth() {
    /* TODO(pg-port): real check in miscadmin.c */
}

/* TODO(pg-port): IsolationUsesXactSnapshot from access/transam/xact.c */
unsafe fn IsolationUsesXactSnapshot() -> bool {
    false
}

/* TODO(pg-port): copyObject generic from nodes/copyfuncs.c */
unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    unimplemented!("TODO(pg-port): copyObject - nodes/copyfuncs.c")
}

/* TODO(pg-port): RelationGetIndexList from utils/cache/relcache.c */
unsafe fn RelationGetIndexList(_relation: Relation) -> *mut List {
    unimplemented!("TODO(pg-port): RelationGetIndexList - utils/cache/relcache.c")
}

/* TODO(pg-port): list_nth_node helper */
macro_rules! list_nth_node {
    ($typ:ty, $tag:ident, $list:expr, $n:expr) => {
        $crate::castNode!($typ, $tag, list_nth($list, $n) as *mut $crate::nodes::nodes::Node)
    };
}

/*-----------------------
 * PartitionTupleRouting - Encapsulates all information required to
 * route a tuple inserted into a partitioned table to one of its leaf
 * partitions.
 *
 * partition_root
 *		The partitioned table that's the target of the command.
 *
 * partition_dispatch_info
 *		Array of 'max_dispatch' elements containing a pointer to a
 *		PartitionDispatch object for every partitioned table touched by tuple
 *		routing.  The entry for the target partitioned table is *always*
 *		present in the 0th element of this array.  See comment for
 *		PartitionDispatchData->indexes for details on how this array is
 *		indexed.
 *
 * nonleaf_partitions
 *		Array of 'max_dispatch' elements containing pointers to fake
 *		ResultRelInfo objects for nonleaf partitions, useful for checking
 *		the partition constraint.
 *
 * num_dispatch
 *		The current number of items stored in the 'partition_dispatch_info'
 *		array.  Also serves as the index of the next free array element for
 *		new PartitionDispatch objects that need to be stored.
 *
 * max_dispatch
 *		The current allocated size of the 'partition_dispatch_info' array.
 *
 * partitions
 *		Array of 'max_partitions' elements containing a pointer to a
 *		ResultRelInfo for every leaf partition touched by tuple routing.
 *		Some of these are pointers to ResultRelInfos which are borrowed out of
 *		the owning ModifyTableState node.  The remainder have been built
 *		especially for tuple routing.  See comment for
 *		PartitionDispatchData->indexes for details on how this array is
 *		indexed.
 *
 * is_borrowed_rel
 *		Array of 'max_partitions' booleans recording whether a given entry
 *		in 'partitions' is a ResultRelInfo pointer borrowed from the owning
 *		ModifyTableState node, rather than being built here.
 *
 * num_partitions
 *		The current number of items stored in the 'partitions' array.  Also
 *		serves as the index of the next free array element for new
 *		ResultRelInfo objects that need to be stored.
 *
 * max_partitions
 *		The current allocated size of the 'partitions' array.
 *
 * memcxt
 *		Memory context used to allocate subsidiary structs.
 *-----------------------
 */
#[repr(C)]
pub struct PartitionTupleRoutingReal {
    pub partition_root: Relation,
    pub partition_dispatch_info: *mut PartitionDispatch,
    pub nonleaf_partitions: *mut *mut ResultRelInfo,
    pub num_dispatch: c_int,
    pub max_dispatch: c_int,
    pub partitions: *mut *mut ResultRelInfo,
    pub is_borrowed_rel: *mut bool,
    pub num_partitions: c_int,
    pub max_partitions: c_int,
    pub memcxt: MemoryContext,
}

/*-----------------------
 * PartitionDispatch - information about one partitioned table in a partition
 * hierarchy required to route a tuple to any of its partitions.  A
 * PartitionDispatch is always encapsulated inside a PartitionTupleRouting
 * struct and stored inside its 'partition_dispatch_info' array.
 *
 * reldesc
 *		Relation descriptor of the table
 *
 * key
 *		Partition key information of the table
 *
 * keystate
 *		Execution state required for expressions in the partition key
 *
 * partdesc
 *		Partition descriptor of the table
 *
 * tupslot
 *		A standalone TupleTableSlot initialized with this table's tuple
 *		descriptor, or NULL if no tuple conversion between the parent is
 *		required.
 *
 * tupmap
 *		TupleConversionMap to convert from the parent's rowtype to this table's
 *		rowtype  (when extracting the partition key of a tuple just before
 *		routing it through this table). A NULL value is stored if no tuple
 *		conversion is required.
 *
 * indexes
 *		Array of partdesc->nparts elements.  For leaf partitions the index
 *		corresponds to the partition's ResultRelInfo in the encapsulating
 *		PartitionTupleRouting's partitions array.  For partitioned partitions,
 *		the index corresponds to the PartitionDispatch for it in its
 *		partition_dispatch_info array.  -1 indicates we've not yet allocated
 *		anything in PartitionTupleRouting for the partition.
 *-----------------------
 */
#[repr(C)]
pub struct PartitionDispatchData {
    pub reldesc: Relation,
    pub key: PartitionKey,
    pub keystate: *mut List,    /* list of ExprState */
    pub partdesc: PartitionDesc,
    pub tupslot: *mut TupleTableSlot,
    pub tupmap: *mut AttrMap,
    pub indexes: [c_int; 0],    /* FLEXIBLE_ARRAY_MEMBER -- access via .as_ptr().add(i) */
}

pub type PartitionDispatch = *mut PartitionDispatchData;

/*
 * PartitionPruneContext
 *		Stores information needed at runtime for pruning computations
 *		related to a single partitioned table.
 */
#[repr(C)]
pub struct PartitionPruneContext {
    pub strategy: c_char,
    pub partnatts: c_int,
    pub nparts: c_int,
    pub boundinfo: PartitionBoundInfo,
    pub partcollation: *mut Oid,
    pub partsupfunc: *mut FmgrInfo,
    pub stepcmpfuncs: *mut FmgrInfo,
    pub ppccontext: MemoryContext,
    pub planstate: *mut crate::nodes::execnodes::PlanState,
    pub exprcontext: *mut ExprContext,
    pub exprstates: *mut *mut ExprState,
}

/*
 * PartitionedRelPruningData - Per-partitioned-table data for run-time pruning
 * of partitions.
 */
#[repr(C)]
pub struct PartitionedRelPruningData {
    pub partrel: Relation,
    pub nparts: c_int,
    pub subplan_map: *mut c_int,
    pub subpart_map: *mut c_int,
    pub leafpart_rti_map: *mut c_int,
    pub present_parts: *mut Bitmapset,
    pub initial_pruning_steps: *mut List,
    pub exec_pruning_steps: *mut List,
    pub initial_context: PartitionPruneContext,
    pub exec_context: PartitionPruneContext,
}

/*
 * PartitionPruningData - Holds all the run-time pruning information for
 * a single partitioning hierarchy.
 */
#[repr(C)]
pub struct PartitionPruningData {
    pub num_partrelprunedata: c_int,
    pub partrelprunedata: [PartitionedRelPruningData; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * PartitionPruneState - State object required for plan nodes to perform
 * run-time partition pruning.
 */
#[repr(C)]
pub struct PartitionPruneStateReal {
    pub econtext: *mut ExprContext,
    pub execparamids: *mut Bitmapset,
    pub other_subplans: *mut Bitmapset,
    pub prune_context: MemoryContext,
    pub do_initial_prune: bool,
    pub do_exec_prune: bool,
    pub num_partprunedata: c_int,
    pub partprunedata: [*mut PartitionPruningData; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * PruneCxtStateIdx() computes the correct index into stepcmpfuncs[]
 * and exprstates[] for step step_id and partition key column keyno.
 */
#[inline]
pub fn PruneCxtStateIdx(partnatts: c_int, step_id: c_int, keyno: c_int) -> c_int {
    partnatts * step_id + keyno
}

/*
 * The number of times the same partition must be found in a row before we
 * switch from a binary search for the given values to just checking if the
 * values belong to the last found partition.  This must be above 0.
 */
const PARTITION_CACHED_FIND_THRESHOLD: c_int = 16;

/*
 * ExecSetupPartitionTupleRouting - sets up information needed during
 * tuple routing for partitioned tables, encapsulates it in
 * PartitionTupleRouting, and returns it.
 *
 * Callers must use the returned PartitionTupleRouting during calls to
 * ExecFindPartition().  The actual ResultRelInfo for a partition is only
 * allocated when the partition is found for the first time.
 *
 * The current memory context is used to allocate this struct and all
 * subsidiary structs that will be allocated from it later on.  Typically
 * it should be estate->es_query_cxt.
 */
pub unsafe fn ExecSetupPartitionTupleRouting(
    estate: *mut EState,
    rel: Relation,
) -> *mut PartitionTupleRouting {
    let proute: *mut PartitionTupleRoutingReal;

    /*
     * Here we attempt to expend as little effort as possible in setting up
     * the PartitionTupleRouting.  Each partition's ResultRelInfo is built on
     * demand, only when we actually need to route a tuple to that partition.
     * The reason for this is that a common case is for INSERT to insert a
     * single tuple into a partitioned table and this must be fast.
     */
    proute = palloc0(size_of::<PartitionTupleRoutingReal>()) as *mut PartitionTupleRoutingReal;
    (*proute).partition_root = rel;
    (*proute).memcxt = CurrentMemoryContext;
    /* Rest of members initialized by zeroing */

    /*
     * Initialize this table's PartitionDispatch object.  Here we pass in the
     * parent as NULL as we don't need to care about any parent of the target
     * partitioned table.
     */
    ExecInitPartitionDispatchInfo(estate, proute, RelationGetRelid(rel),
                                  ptr::null_mut(), 0, ptr::null_mut());

    proute as *mut PartitionTupleRouting
}

/*
 * ExecFindPartition -- Return the ResultRelInfo for the leaf partition that
 * the tuple contained in *slot should belong to.
 *
 * If the partition's ResultRelInfo does not yet exist in 'proute' then we set
 * one up or reuse one from mtstate's resultRelInfo array.  When reusing a
 * ResultRelInfo from the mtstate we verify that the relation is a valid
 * target for INSERTs and initialize tuple routing information.
 *
 * rootResultRelInfo is the relation named in the query.
 *
 * estate must be non-NULL; we'll need it to compute any expressions in the
 * partition keys.  Also, its per-tuple contexts are used as evaluation
 * scratch space.
 *
 * If no leaf partition is found, this routine errors out with the appropriate
 * error message.  An error may also be raised if the found target partition
 * is not a valid target for an INSERT.
 */
pub unsafe fn ExecFindPartition(
    mtstate: *mut ModifyTableState,
    rootResultRelInfo: *mut ResultRelInfo,
    proute: *mut PartitionTupleRouting,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
) -> *mut ResultRelInfo {
    let proute = proute as *mut PartitionTupleRoutingReal;
    let pd: *mut PartitionDispatch = (*proute).partition_dispatch_info;
    let mut values: [Datum; PARTITION_MAX_KEYS] = [0; PARTITION_MAX_KEYS];
    let mut isnull: [bool; PARTITION_MAX_KEYS] = [false; PARTITION_MAX_KEYS];
    let mut rel: Relation;
    let mut dispatch: PartitionDispatch;
    let mut partdesc: PartitionDesc;
    let ecxt: *mut ExprContext = GetPerTupleExprContext(estate);
    let ecxt_scantuple_saved: *mut TupleTableSlot = (*ecxt).ecxt_scantuple;
    let rootslot: *mut TupleTableSlot = slot;
    let mut myslot: *mut TupleTableSlot = ptr::null_mut();
    let oldcxt: MemoryContext;
    let mut rri: *mut ResultRelInfo = ptr::null_mut();
    let mut slot = slot;

    /* use per-tuple context here to avoid leaking memory */
    oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

    /*
     * First check the root table's partition constraint, if any.  No point in
     * routing the tuple if it doesn't belong in the root table itself.
     */
    if (*(*(*rootResultRelInfo).ri_RelationDesc).rd_rel).relispartition {
        ExecPartitionCheck(rootResultRelInfo, slot, estate, true);
    }

    /* start with the root partitioned table */
    dispatch = *pd.add(0);
    while !dispatch.is_null() {
        let mut partidx: c_int = -1;
        let is_leaf: bool;

        CHECK_FOR_INTERRUPTS!();

        rel = (*dispatch).reldesc;
        partdesc = (*dispatch).partdesc;

        /*
         * Extract partition key from tuple. Expression evaluation machinery
         * that FormPartitionKeyDatum() invokes expects ecxt_scantuple to
         * point to the correct tuple slot.  The slot might have changed from
         * what was used for the parent table if the table of the current
         * partitioning level has different tuple descriptor from the parent.
         * So update ecxt_scantuple accordingly.
         */
        (*ecxt).ecxt_scantuple = slot;
        FormPartitionKeyDatum(dispatch, slot, estate, values.as_mut_ptr(), isnull.as_mut_ptr());

        /*
         * If this partitioned table has no partitions or no partition for
         * these values, error out.
         */
        if (*partdesc).nparts == 0 || {
            partidx = get_partition_for_tuple(dispatch, values.as_mut_ptr(), isnull.as_mut_ptr());
            partidx < 0
        } {
            let val_desc: *mut c_char;

            val_desc = ExecBuildSlotPartitionKeyDescription(rel,
                                                            values.as_mut_ptr(),
                                                            isnull.as_mut_ptr(),
                                                            64);
            Assert!(OidIsValid(RelationGetRelid(rel)));
            ereport!(ERROR, errmsg!("no partition of relation \"{}\" found for row",
                             CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()) /* C also: errcode!(ERRCODE_CHECK_VIOLATION); if !val_desc.is_null() { errdetail!("Partition key of the failing row contains {}.", CStr::from_ptr(val_desc).to_string_lossy()) } else { 0 }; errtable!(rel) */);
        }

        is_leaf = *(*partdesc).is_leaf.add(partidx as usize);
        if is_leaf {
            /*
             * We've reached the leaf -- hurray, we're done.  Look to see if
             * we've already got a ResultRelInfo for this partition.
             */
            if likely!(*(*dispatch).indexes.as_ptr().add(partidx as usize) >= 0) {
                /* ResultRelInfo already built */
                Assert!(*(*dispatch).indexes.as_ptr().add(partidx as usize) < (*proute).num_partitions);
                rri = *(*proute).partitions.add(
                    *(*dispatch).indexes.as_ptr().add(partidx as usize) as usize
                );
            } else {
                /*
                 * If the partition is known in the owning ModifyTableState
                 * node, we can re-use that ResultRelInfo instead of creating
                 * a new one with ExecInitPartitionInfo().
                 */
                rri = ExecLookupResultRelByOid(mtstate,
                                               *(*partdesc).oids.add(partidx as usize),
                                               true, false);
                if !rri.is_null() {
                    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;

                    /* Verify this ResultRelInfo allows INSERTs */
                    CheckValidResultRel(rri, CMD_INSERT,
                                        if !node.is_null() { (*node).onConflictAction } else { ONCONFLICT_NONE },
                                        NIL);

                    /*
                     * Initialize information needed to insert this and
                     * subsequent tuples routed to this partition.
                     */
                    ExecInitRoutingInfo(mtstate, estate, proute as *mut PartitionTupleRouting,
                                        dispatch, rri, partidx, true);
                } else {
                    /* We need to create a new one. */
                    rri = ExecInitPartitionInfo(mtstate, estate,
                                                proute as *mut PartitionTupleRouting,
                                                dispatch,
                                                rootResultRelInfo, partidx);
                }
            }
            Assert!(!rri.is_null());

            /* Signal to terminate the loop */
            dispatch = ptr::null_mut();
        } else {
            /*
             * Partition is a sub-partitioned table; get the PartitionDispatch
             */
            if likely!(*(*dispatch).indexes.as_ptr().add(partidx as usize) >= 0) {
                /* Already built. */
                Assert!(*(*dispatch).indexes.as_ptr().add(partidx as usize) < (*proute).num_dispatch);

                rri = *(*proute).nonleaf_partitions.add(
                    *(*dispatch).indexes.as_ptr().add(partidx as usize) as usize
                );

                /*
                 * Move down to the next partition level and search again
                 * until we find a leaf partition that matches this tuple
                 */
                dispatch = *(*proute).partition_dispatch_info.add(
                    *(*dispatch).indexes.as_ptr().add(partidx as usize) as usize
                );
            } else {
                /* Not yet built. Do that now. */
                let subdispatch: PartitionDispatch;

                /*
                 * Create the new PartitionDispatch.  We pass the current one
                 * in as the parent PartitionDispatch
                 */
                subdispatch = ExecInitPartitionDispatchInfo(estate,
                                                            proute,
                                                            *(*partdesc).oids.add(partidx as usize),
                                                            dispatch, partidx,
                                                            (*mtstate).rootResultRelInfo);
                Assert!(*(*dispatch).indexes.as_ptr().add(partidx as usize) >= 0 &&
                    *(*dispatch).indexes.as_ptr().add(partidx as usize) < (*proute).num_dispatch);

                rri = *(*proute).nonleaf_partitions.add(
                    *(*dispatch).indexes.as_ptr().add(partidx as usize) as usize
                );
                dispatch = subdispatch;
            }

            /*
             * Convert the tuple to the new parent's layout, if different from
             * the previous parent.
             */
            if !(*dispatch).tupslot.is_null() {
                let map: *mut AttrMap = (*dispatch).tupmap;
                let tempslot: *mut TupleTableSlot = myslot;

                myslot = (*dispatch).tupslot;
                slot = execute_attr_map_slot(map, slot, myslot);

                if !tempslot.is_null() {
                    ExecClearTuple(tempslot);
                }
            }
        }

        /*
         * If this partition is the default one, we must check its partition
         * constraint now, which may have changed concurrently due to
         * partitions being added to the parent.
         *
         * (We do this here, and do not rely on ExecInsert doing it, because
         * we don't want to miss doing it for non-leaf partitions.)
         */
        if partidx == (*((*partdesc).boundinfo as *mut PartitionBoundInfoFull)).default_index {
            /*
             * The tuple must match the partition's layout for the constraint
             * expression to be evaluated successfully.  If the partition is
             * sub-partitioned, that would already be the case due to the code
             * above, but for a leaf partition the tuple still matches the
             * parent's layout.
             *
             * Note that we have a map to convert from root to current
             * partition, but not from immediate parent to current partition.
             * So if we have to convert, do it from the root slot; if not, use
             * the root slot as-is.
             */
            if is_leaf {
                let map: *mut TupleConversionMap = ExecGetRootToChildMap(rri, estate);

                if !map.is_null() {
                    slot = execute_attr_map_slot((*map).attrMap, rootslot,
                                                 (*rri).ri_PartitionTupleSlot);
                } else {
                    slot = rootslot;
                }
            }

            ExecPartitionCheck(rri, slot, estate, true);
        }
    }

    /* Release the tuple in the lowest parent's dedicated slot. */
    if !myslot.is_null() {
        ExecClearTuple(myslot);
    }
    /* and restore ecxt's scantuple */
    (*ecxt).ecxt_scantuple = ecxt_scantuple_saved;
    MemoryContextSwitchTo(oldcxt);

    rri
}

/*
 * ExecInitPartitionInfo
 *		Lock the partition and initialize ResultRelInfo.  Also setup other
 *		information for the partition and store it in the next empty slot in
 *		the proute->partitions array.
 *
 * Returns the ResultRelInfo
 */
unsafe fn ExecInitPartitionInfo(
    mtstate: *mut ModifyTableState,
    estate: *mut EState,
    proute: *mut PartitionTupleRouting,
    dispatch: PartitionDispatch,
    rootResultRelInfo: *mut ResultRelInfo,
    partidx: c_int,
) -> *mut ResultRelInfo {
    let proute = proute as *mut PartitionTupleRoutingReal;
    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;
    let partOid: Oid = *(*(*dispatch).partdesc).oids.add(partidx as usize);
    let partrel: Relation;
    let firstVarno: c_int = (*(*mtstate).resultRelInfo.add(0)).ri_RangeTableIndex as c_int;
    let firstResultRel: Relation = (*(*mtstate).resultRelInfo.add(0)).ri_RelationDesc;
    let leaf_part_rri: *mut ResultRelInfo;
    let oldcxt: MemoryContext;
    let mut part_attmap: *mut AttrMap = ptr::null_mut();
    let mut found_whole_row: bool = false;

    oldcxt = MemoryContextSwitchTo((*proute).memcxt);

    partrel = table_open(partOid, RowExclusiveLock);

    leaf_part_rri = makeNode!(ResultRelInfo, T_ResultRelInfo);
    InitResultRelInfo(leaf_part_rri,
                      partrel,
                      0,
                      rootResultRelInfo,
                      (*estate).es_instrument);

    /*
     * Verify result relation is a valid target for an INSERT.  An UPDATE of a
     * partition-key becomes a DELETE+INSERT operation, so this check is still
     * required when the operation is CMD_UPDATE.
     */
    CheckValidResultRel(leaf_part_rri, CMD_INSERT,
                        if !node.is_null() { (*node).onConflictAction } else { ONCONFLICT_NONE },
                        NIL);

    /*
     * Open partition indices.  The user may have asked to check for conflicts
     * within this leaf partition and do "nothing" instead of throwing an
     * error.  Be prepared in that case by initializing the index information
     * needed by ExecInsert() to perform speculative insertions.
     */
    if (*(*partrel).rd_rel).relhasindex &&
        (*leaf_part_rri).ri_IndexRelationDescs.is_null()
    {
        ExecOpenIndices(leaf_part_rri,
                        !node.is_null() &&
                        (*node).onConflictAction != ONCONFLICT_NONE);
    }

    /*
     * Build WITH CHECK OPTION constraints for the partition.  Note that we
     * didn't build the withCheckOptionList for partitions within the planner,
     * but simple translation of varattnos will suffice.  This only occurs for
     * the INSERT case or in the case of UPDATE/MERGE tuple routing where we
     * didn't find a result rel to reuse.
     */
    if !node.is_null() && !(*node).withCheckOptionLists.is_null()
        && (*(*node).withCheckOptionLists).length > 0
    {
        let mut wcoList: *mut List;
        let mut wcoExprs: *mut List = NIL;
        let mut ll: *mut crate::nodes::pg_list::ListCell;

        /*
         * In the case of INSERT on a partitioned table, there is only one
         * plan.  Likewise, there is only one WCO list, not one per partition.
         * For UPDATE/MERGE, there are as many WCO lists as there are plans.
         */
        Assert!(((*node).operation == CMD_INSERT &&
                  list_length((*node).withCheckOptionLists) == 1 &&
                  list_length((*node).resultRelations) == 1) ||
                 ((*node).operation == CMD_UPDATE &&
                  list_length((*node).withCheckOptionLists) ==
                  list_length((*node).resultRelations)) ||
                 ((*node).operation == CMD_MERGE &&
                  list_length((*node).withCheckOptionLists) ==
                  list_length((*node).resultRelations)));

        /*
         * Use the WCO list of the first plan as a reference to calculate
         * attno's for the WCO list of this partition.  In the INSERT case,
         * that refers to the root partitioned table, whereas in the UPDATE
         * tuple routing case, that refers to the first partition in the
         * mtstate->resultRelInfo array.  In any case, both that relation and
         * this partition should have the same columns, so we should be able
         * to map attributes successfully.
         */
        wcoList = linitial((*node).withCheckOptionLists) as *mut List;

        /*
         * Convert Vars in it to contain this partition's attribute numbers.
         */
        part_attmap =
            build_attrmap_by_name(RelationGetDescr(partrel),
                                   RelationGetDescr(firstResultRel),
                                   false);
        wcoList = map_variable_attnos(wcoList as *mut Node,
                                       firstVarno, 0,
                                       part_attmap,
                                       (*RelationGetForm(partrel)).reltype,
                                       &mut found_whole_row) as *mut List;
        /* We ignore the value of found_whole_row. */

        ll = list_head(wcoList);
        while !ll.is_null() {
            let wco: *mut WithCheckOption = castNode!(WithCheckOption, T_WithCheckOption,
                lfirst(ll) as *mut Node);
            let wcoExpr: *mut ExprState = ExecInitQual(
                (*wco).qual as *mut List,
                &mut (*mtstate).ps as *mut crate::nodes::execnodes::PlanState,
            );

            wcoExprs = lappend(wcoExprs, wcoExpr as *mut c_void);
            ll = lnext(wcoList, ll);
        }

        (*leaf_part_rri).ri_WithCheckOptions = wcoList;
        (*leaf_part_rri).ri_WithCheckOptionExprs = wcoExprs;
    }

    /*
     * Build the RETURNING projection for the partition.  Note that we didn't
     * build the returningList for partitions within the planner, but simple
     * translation of varattnos will suffice.  This only occurs for the INSERT
     * case or in the case of UPDATE/MERGE tuple routing where we didn't find
     * a result rel to reuse.
     */
    if !node.is_null() && !(*node).returningLists.is_null()
        && (*(*node).returningLists).length > 0
    {
        let slot: *mut TupleTableSlot;
        let econtext: *mut ExprContext;
        let mut returningList: *mut List;

        /* See the comment above for WCO lists. */
        Assert!(((*node).operation == CMD_INSERT &&
                  list_length((*node).returningLists) == 1 &&
                  list_length((*node).resultRelations) == 1) ||
                 ((*node).operation == CMD_UPDATE &&
                  list_length((*node).returningLists) ==
                  list_length((*node).resultRelations)) ||
                 ((*node).operation == CMD_MERGE &&
                  list_length((*node).returningLists) ==
                  list_length((*node).resultRelations)));

        /*
         * Use the RETURNING list of the first plan as a reference to
         * calculate attno's for the RETURNING list of this partition.  See
         * the comment above for WCO lists for more details on why this is
         * okay.
         */
        returningList = linitial((*node).returningLists) as *mut List;

        /*
         * Convert Vars in it to contain this partition's attribute numbers.
         */
        if part_attmap.is_null() {
            part_attmap =
                build_attrmap_by_name(RelationGetDescr(partrel),
                                       RelationGetDescr(firstResultRel),
                                       false);
        }
        returningList = map_variable_attnos(returningList as *mut Node,
                                             firstVarno, 0,
                                             part_attmap,
                                             (*RelationGetForm(partrel)).reltype,
                                             &mut found_whole_row) as *mut List;
        /* We ignore the value of found_whole_row. */

        (*leaf_part_rri).ri_returningList = returningList;

        /*
         * Initialize the projection itself.
         *
         * Use the slot and the expression context that would have been set up
         * in ExecInitModifyTable() for projection's output.
         */
        Assert!(!(*mtstate).ps.ps_ResultTupleSlot.is_null());
        slot = (*mtstate).ps.ps_ResultTupleSlot;
        Assert!(!(*mtstate).ps.ps_ExprContext.is_null());
        econtext = (*mtstate).ps.ps_ExprContext;
        (*leaf_part_rri).ri_projectReturning =
            ExecBuildProjectionInfo(returningList, econtext, slot,
                                    &mut (*mtstate).ps,
                                    RelationGetDescr(partrel));
    }

    /* Set up information needed for routing tuples to the partition. */
    ExecInitRoutingInfo(mtstate, estate,
                        proute as *mut PartitionTupleRouting,
                        dispatch, leaf_part_rri, partidx, false);

    /*
     * If there is an ON CONFLICT clause, initialize state for it.
     */
    if !node.is_null() && (*node).onConflictAction != ONCONFLICT_NONE {
        let partrelDesc: *mut crate::access::common::tupdesc::TupleDescData = RelationGetDescr(partrel);
        let econtext: *mut ExprContext = (*mtstate).ps.ps_ExprContext;
        let mut lc: *mut crate::nodes::pg_list::ListCell;
        let mut arbiterIndexes: *mut List = NIL;

        /*
         * If there is a list of arbiter indexes, map it to a list of indexes
         * in the partition.  We do that by scanning the partition's index
         * list and searching for ancestry relationships to each index in the
         * ancestor table.
         */
        if !(*rootResultRelInfo).ri_onConflictArbiterIndexes.is_null()
            && (*(*rootResultRelInfo).ri_onConflictArbiterIndexes).length > 0
        {
            let childIdxs: *mut List;

            childIdxs = RelationGetIndexList((*leaf_part_rri).ri_RelationDesc);

            lc = list_head(childIdxs);
            while !lc.is_null() {
                let childIdx: Oid = lfirst_oid(lc);
                let ancestors: *mut List;
                let mut lc2: *mut crate::nodes::pg_list::ListCell;

                ancestors = get_partition_ancestors(childIdx);
                lc2 = list_head((*rootResultRelInfo).ri_onConflictArbiterIndexes);
                while !lc2.is_null() {
                    if list_member_oid(ancestors, lfirst_oid(lc2)) {
                        arbiterIndexes = lappend_oid(arbiterIndexes, childIdx);
                    }
                    lc2 = lnext((*rootResultRelInfo).ri_onConflictArbiterIndexes, lc2);
                }
                list_free(ancestors);
                lc = lnext(childIdxs, lc);
            }
        }

        /*
         * If the resulting lists are of inequal length, something is wrong.
         * (This shouldn't happen, since arbiter index selection should not
         * pick up an invalid index.)
         */
        if list_length((*rootResultRelInfo).ri_onConflictArbiterIndexes) !=
            list_length(arbiterIndexes)
        {
            elog!(ERROR, "invalid arbiter index list");
        }
        (*leaf_part_rri).ri_onConflictArbiterIndexes = arbiterIndexes;

        /*
         * In the DO UPDATE case, we have some more state to initialize.
         */
        if (*node).onConflictAction == ONCONFLICT_UPDATE {
            let onconfl: *mut OnConflictSetState = makeNode!(OnConflictSetState, T_OnConflictSetState);
            let map: *mut TupleConversionMap;

            map = ExecGetRootToChildMap(leaf_part_rri, estate);

            Assert!((*node).onConflictSet != NIL && !(*node).onConflictSet.is_null()
                     && (*(*node).onConflictSet).length > 0);
            Assert!(!(*rootResultRelInfo).ri_onConflict.is_null());

            (*leaf_part_rri).ri_onConflict = onconfl;

            /*
             * Need a separate existing slot for each partition, as the
             * partition could be of a different AM, even if the tuple
             * descriptors match.
             */
            (*onconfl).oc_Existing =
                table_slot_create((*leaf_part_rri).ri_RelationDesc,
                                   &mut (*(*mtstate).ps.state).es_tupleTable);

            /*
             * If the partition's tuple descriptor matches exactly the root
             * parent (the common case), we can re-use most of the parent's ON
             * CONFLICT SET state, skipping a bunch of work.  Otherwise, we
             * need to create state specific to this partition.
             */
            if map.is_null() {
                /*
                 * It's safe to reuse these from the partition root, as we
                 * only process one tuple at a time (therefore we won't
                 * overwrite needed data in slots), and the results of
                 * projections are independent of the underlying storage.
                 * Projections and where clauses themselves don't store state
                 * / are independent of the underlying storage.
                 */
                (*onconfl).oc_ProjSlot =
                    (*(*rootResultRelInfo).ri_onConflict).oc_ProjSlot;
                (*onconfl).oc_ProjInfo =
                    (*(*rootResultRelInfo).ri_onConflict).oc_ProjInfo;
                (*onconfl).oc_WhereClause =
                    (*(*rootResultRelInfo).ri_onConflict).oc_WhereClause;
            } else {
                let mut onconflset: *mut List;
                let onconflcols: *mut List;

                /*
                 * Translate expressions in onConflictSet to account for
                 * different attribute numbers.  For that, map partition
                 * varattnos twice: first to catch the EXCLUDED
                 * pseudo-relation (INNER_VAR), and second to handle the main
                 * target relation (firstVarno).
                 */
                onconflset = copyObject((*node).onConflictSet);
                if part_attmap.is_null() {
                    part_attmap =
                        build_attrmap_by_name(RelationGetDescr(partrel),
                                               RelationGetDescr(firstResultRel),
                                               false);
                }
                onconflset = map_variable_attnos(onconflset as *mut Node,
                                                  INNER_VAR as c_int, 0,
                                                  part_attmap,
                                                  (*RelationGetForm(partrel)).reltype,
                                                  &mut found_whole_row) as *mut List;
                /* We ignore the value of found_whole_row. */
                onconflset = map_variable_attnos(onconflset as *mut Node,
                                                  firstVarno, 0,
                                                  part_attmap,
                                                  (*RelationGetForm(partrel)).reltype,
                                                  &mut found_whole_row) as *mut List;
                /* We ignore the value of found_whole_row. */

                /* Finally, adjust the target colnos to match the partition. */
                onconflcols = adjust_partition_colnos((*node).onConflictCols,
                                                       leaf_part_rri);

                /* create the tuple slot for the UPDATE SET projection */
                (*onconfl).oc_ProjSlot =
                    table_slot_create(partrel,
                                       &mut (*(*mtstate).ps.state).es_tupleTable);

                /* build UPDATE SET projection state */
                (*onconfl).oc_ProjInfo =
                    ExecBuildUpdateProjection(onconflset,
                                              true,
                                              onconflcols,
                                              partrelDesc,
                                              econtext,
                                              (*onconfl).oc_ProjSlot,
                                              &mut (*mtstate).ps);

                /*
                 * If there is a WHERE clause, initialize state where it will
                 * be evaluated, mapping the attribute numbers appropriately.
                 * As with onConflictSet, we need to map partition varattnos
                 * to the partition's tupdesc.
                 */
                if !(*node).onConflictWhere.is_null() {
                    let mut clause: *mut List;

                    clause = copyObject((*node).onConflictWhere as *mut List);
                    clause = map_variable_attnos(clause as *mut Node,
                                                  INNER_VAR as c_int, 0,
                                                  part_attmap,
                                                  (*RelationGetForm(partrel)).reltype,
                                                  &mut found_whole_row) as *mut List;
                    /* We ignore the value of found_whole_row. */
                    clause = map_variable_attnos(clause as *mut Node,
                                                  firstVarno, 0,
                                                  part_attmap,
                                                  (*RelationGetForm(partrel)).reltype,
                                                  &mut found_whole_row) as *mut List;
                    /* We ignore the value of found_whole_row. */
                    (*onconfl).oc_WhereClause =
                        ExecInitQual(clause, &mut (*mtstate).ps);
                }
            }
        }
    }

    /*
     * Since we've just initialized this ResultRelInfo, it's not in any list
     * attached to the estate as yet.  Add it, so that it can be found later.
     *
     * Note that the entries in this list appear in no predetermined order,
     * because partition result rels are initialized as and when they're
     * needed.
     */
    MemoryContextSwitchTo((*estate).es_query_cxt);
    (*estate).es_tuple_routing_result_relations =
        lappend((*estate).es_tuple_routing_result_relations,
                leaf_part_rri as *mut c_void);

    /*
     * Initialize information about this partition that's needed to handle
     * MERGE.  We take the "first" result relation's mergeActionList as
     * reference and make copy for this relation, converting stuff that
     * references attribute numbers to match this relation's.
     *
     * This duplicates much of the logic in ExecInitMerge(), so if something
     * changes there, look here too.
     */
    if !node.is_null() && (*node).operation == CMD_MERGE {
        let firstMergeActionList: *mut List = linitial((*node).mergeActionLists) as *mut List;
        let mut lc: *mut crate::nodes::pg_list::ListCell;
        let econtext: *mut ExprContext = (*mtstate).ps.ps_ExprContext;
        let joinCondition: *mut Node;

        if part_attmap.is_null() {
            part_attmap =
                build_attrmap_by_name(RelationGetDescr(partrel),
                                       RelationGetDescr(firstResultRel),
                                       false);
        }

        if unlikely!(!((*leaf_part_rri).ri_projectNewInfoValid)) {
            ExecInitMergeTupleSlots(mtstate, leaf_part_rri);
        }

        /* Initialize state for join condition checking. */
        joinCondition =
            map_variable_attnos(linitial((*node).mergeJoinConditions) as *mut Node,
                                  firstVarno, 0,
                                  part_attmap,
                                  (*RelationGetForm(partrel)).reltype,
                                  &mut found_whole_row);
        /* We ignore the value of found_whole_row. */
        (*leaf_part_rri).ri_MergeJoinCondition =
            ExecInitQual(joinCondition as *mut List, &mut (*mtstate).ps);

        lc = list_head(firstMergeActionList);
        while !lc.is_null() {
            /* Make a copy for this relation to be safe.  */
            let action: *mut MergeAction = copyObject(lfirst(lc) as *mut MergeAction);
            let action_state: *mut MergeActionState;

            /* Generate the action's state for this relation */
            action_state = makeNode!(MergeActionState, T_MergeActionState);
            (*action_state).mas_action = action;

            /* And put the action in the appropriate list */
            let kind_idx = (*action).matchKind as usize;
            (*leaf_part_rri).ri_MergeActions[kind_idx] =
                lappend((*leaf_part_rri).ri_MergeActions[kind_idx],
                        action_state as *mut c_void);

            match (*action).commandType {
                CMD_INSERT => {
                    /*
                     * ExecCheckPlanOutput() already done on the targetlist
                     * when "first" result relation initialized and it is same
                     * for all result relations.
                     */
                    (*action_state).mas_proj =
                        ExecBuildProjectionInfo((*action).targetList, econtext,
                                                (*leaf_part_rri).ri_newTupleSlot,
                                                &mut (*mtstate).ps,
                                                RelationGetDescr(partrel));
                }
                CMD_UPDATE => {
                    /*
                     * Convert updateColnos from "first" result relation
                     * attribute numbers to this result rel's.
                     */
                    if !part_attmap.is_null() {
                        (*action).updateColnos =
                            adjust_partition_colnos_using_map((*action).updateColnos,
                                                               part_attmap);
                    }
                    (*action_state).mas_proj =
                        ExecBuildUpdateProjection((*action).targetList,
                                                   true,
                                                   (*action).updateColnos,
                                                   RelationGetDescr((*leaf_part_rri).ri_RelationDesc),
                                                   econtext,
                                                   (*leaf_part_rri).ri_newTupleSlot,
                                                   ptr::null_mut());
                }
                CMD_DELETE | CMD_NOTHING => {
                    /* Nothing to do */
                }
                _ => {
                    elog!(ERROR, "unknown action in MERGE WHEN clause");
                }
            }

            /* found_whole_row intentionally ignored. */
            (*action).qual =
                map_variable_attnos((*action).qual,
                                     firstVarno, 0,
                                     part_attmap,
                                     (*RelationGetForm(partrel)).reltype,
                                     &mut found_whole_row);
            (*action_state).mas_whenqual =
                ExecInitQual((*action).qual as *mut List, &mut (*mtstate).ps);

            lc = lnext(firstMergeActionList, lc);
        }
    }
    MemoryContextSwitchTo(oldcxt);

    leaf_part_rri
}

/*
 * ExecInitRoutingInfo
 *		Set up information needed for translating tuples between root
 *		partitioned table format and partition format, and keep track of it
 *		in PartitionTupleRouting.
 */
unsafe fn ExecInitRoutingInfo(
    mtstate: *mut ModifyTableState,
    estate: *mut EState,
    proute: *mut PartitionTupleRouting,
    dispatch: PartitionDispatch,
    partRelInfo: *mut ResultRelInfo,
    partidx: c_int,
    is_borrowed_rel: bool,
) {
    let proute = proute as *mut PartitionTupleRoutingReal;
    let oldcxt: MemoryContext;
    let rri_index: c_int;

    oldcxt = MemoryContextSwitchTo((*proute).memcxt);

    /*
     * Set up tuple conversion between root parent and the partition if the
     * two have different rowtypes.  If conversion is indeed required, also
     * initialize a slot dedicated to storing this partition's converted
     * tuples.  Various operations that are applied to tuples after routing,
     * such as checking constraints, will refer to this slot.
     */
    if !ExecGetRootToChildMap(partRelInfo, estate).is_null() {
        let partrel: Relation = (*partRelInfo).ri_RelationDesc;

        /*
         * This pins the partition's TupleDesc, which will be released at the
         * end of the command.
         */
        (*partRelInfo).ri_PartitionTupleSlot =
            table_slot_create(partrel, &mut (*estate).es_tupleTable);
    } else {
        (*partRelInfo).ri_PartitionTupleSlot = ptr::null_mut();
    }

    /*
     * If the partition is a foreign table, let the FDW init itself for
     * routing tuples to the partition.
     */
    if !(*partRelInfo).ri_FdwRoutine.is_null() &&
        (*(*partRelInfo).ri_FdwRoutine).BeginForeignInsert.is_some()
    {
        ((*(*partRelInfo).ri_FdwRoutine).BeginForeignInsert.unwrap())(mtstate, partRelInfo);
    }

    /*
     * Determine if the FDW supports batch insert and determine the batch size
     * (a FDW may support batching, but it may be disabled for the
     * server/table or for this particular query).
     *
     * If the FDW does not support batching, we set the batch size to 1.
     */
    if !(*partRelInfo).ri_FdwRoutine.is_null() &&
        (*(*partRelInfo).ri_FdwRoutine).GetForeignModifyBatchSize.is_some() &&
        (*(*partRelInfo).ri_FdwRoutine).ExecForeignBatchInsert.is_some()
    {
        (*partRelInfo).ri_BatchSize =
            ((*(*partRelInfo).ri_FdwRoutine).GetForeignModifyBatchSize.unwrap())(partRelInfo);
    } else {
        (*partRelInfo).ri_BatchSize = 1;
    }

    Assert!((*partRelInfo).ri_BatchSize >= 1);

    (*partRelInfo).ri_CopyMultiInsertBuffer = ptr::null_mut();

    /*
     * Keep track of it in the PartitionTupleRouting->partitions array.
     */
    Assert!(*(*dispatch).indexes.as_ptr().add(partidx as usize) == -1);

    rri_index = (*proute).num_partitions;
    (*proute).num_partitions += 1;

    /* Allocate or enlarge the array, as needed */
    if (*proute).num_partitions >= (*proute).max_partitions {
        if (*proute).max_partitions == 0 {
            (*proute).max_partitions = 8;
            (*proute).partitions = palloc(
                size_of::<*mut ResultRelInfo>() * (*proute).max_partitions as usize
            ) as *mut *mut ResultRelInfo;
            (*proute).is_borrowed_rel = palloc(
                size_of::<bool>() * (*proute).max_partitions as usize
            ) as *mut bool;
        } else {
            (*proute).max_partitions *= 2;
            (*proute).partitions = crate::utils::palloc::repalloc(
                (*proute).partitions as *mut c_void,
                size_of::<*mut ResultRelInfo>() * (*proute).max_partitions as usize,
            ) as *mut *mut ResultRelInfo;
            (*proute).is_borrowed_rel = crate::utils::palloc::repalloc(
                (*proute).is_borrowed_rel as *mut c_void,
                size_of::<bool>() * (*proute).max_partitions as usize,
            ) as *mut bool;
        }
    }

    *(*proute).partitions.add(rri_index as usize) = partRelInfo;
    *(*proute).is_borrowed_rel.add(rri_index as usize) = is_borrowed_rel;
    *(*dispatch).indexes.as_mut_ptr().add(partidx as usize) = rri_index;

    MemoryContextSwitchTo(oldcxt);
}

/*
 * ExecInitPartitionDispatchInfo
 *		Lock the partitioned table (if not locked already) and initialize
 *		PartitionDispatch for a partitioned table and store it in the next
 *		available slot in the proute->partition_dispatch_info array.  Also,
 *		record the index into this array in the parent_pd->indexes[] array in
 *		the partidx element so that we can properly retrieve the newly created
 *		PartitionDispatch later.
 */
unsafe fn ExecInitPartitionDispatchInfo(
    estate: *mut EState,
    proute: *mut PartitionTupleRoutingReal,
    partoid: Oid,
    parent_pd: PartitionDispatch,
    partidx: c_int,
    rootResultRelInfo: *mut ResultRelInfo,
) -> PartitionDispatch {
    let rel: Relation;
    let partdesc: PartitionDesc;
    let pd: PartitionDispatch;
    let dispatchidx: c_int;
    let oldcxt: MemoryContext;

    /*
     * For data modification, it is better that executor does not include
     * partitions being detached, except when running in snapshot-isolation
     * mode.  This means that a read-committed transaction immediately gets a
     * "no partition for tuple" error when a tuple is inserted into a
     * partition that's being detached concurrently, but a transaction in
     * repeatable-read mode can still use such a partition.
     */
    if (*estate).es_partition_directory.is_null() {
        (*estate).es_partition_directory =
            CreatePartitionDirectory((*estate).es_query_cxt,
                                      !IsolationUsesXactSnapshot());
    }

    oldcxt = MemoryContextSwitchTo((*proute).memcxt);

    /*
     * Only sub-partitioned tables need to be locked here.  The root
     * partitioned table will already have been locked as it's referenced in
     * the query's rtable.
     */
    if partoid != RelationGetRelid((*proute).partition_root) {
        rel = table_open(partoid, RowExclusiveLock);
    } else {
        rel = (*proute).partition_root;
    }
    partdesc = PartitionDirectoryLookup((*estate).es_partition_directory, rel);

    pd = palloc(
        offsetof!(PartitionDispatchData, indexes) as usize +
        (*partdesc).nparts as usize * size_of::<c_int>()
    ) as PartitionDispatch;
    (*pd).reldesc = rel;
    (*pd).key = RelationGetPartitionKey(rel);
    (*pd).keystate = NIL;
    (*pd).partdesc = partdesc;
    if !parent_pd.is_null() {
        let tupdesc = RelationGetDescr(rel);

        /*
         * For sub-partitioned tables where the column order differs from its
         * direct parent partitioned table, we must store a tuple table slot
         * initialized with its tuple descriptor and a tuple conversion map to
         * convert a tuple from its parent's rowtype to its own.  This is to
         * make sure that we are looking at the correct row using the correct
         * tuple descriptor when computing its partition key for tuple
         * routing.
         */
        (*pd).tupmap = build_attrmap_by_name_if_req(
            RelationGetDescr((*parent_pd).reldesc),
            tupdesc,
            false,
        );
        (*pd).tupslot = if !(*pd).tupmap.is_null() {
            MakeSingleTupleTableSlot(tupdesc, &raw const TTSOpsVirtual)
        } else {
            ptr::null_mut()
        };
    } else {
        /* Not required for the root partitioned table */
        (*pd).tupmap = ptr::null_mut();
        (*pd).tupslot = ptr::null_mut();
    }

    /*
     * Initialize with -1 to signify that the corresponding partition's
     * ResultRelInfo or PartitionDispatch has not been created yet.
     */
    ptr::write_bytes((*pd).indexes.as_mut_ptr(), 0xff,
                     (*partdesc).nparts as usize * size_of::<c_int>());
    /* 0xff bytes = -1 in two's-complement for c_int on all supported platforms */

    /* Track in PartitionTupleRouting for later use */
    dispatchidx = (*proute).num_dispatch;
    (*proute).num_dispatch += 1;

    /* Allocate or enlarge the array, as needed */
    if (*proute).num_dispatch >= (*proute).max_dispatch {
        if (*proute).max_dispatch == 0 {
            (*proute).max_dispatch = 4;
            (*proute).partition_dispatch_info = palloc(
                size_of::<PartitionDispatch>() * (*proute).max_dispatch as usize
            ) as *mut PartitionDispatch;
            (*proute).nonleaf_partitions = palloc(
                size_of::<*mut ResultRelInfo>() * (*proute).max_dispatch as usize
            ) as *mut *mut ResultRelInfo;
        } else {
            (*proute).max_dispatch *= 2;
            (*proute).partition_dispatch_info = crate::utils::palloc::repalloc(
                (*proute).partition_dispatch_info as *mut c_void,
                size_of::<PartitionDispatch>() * (*proute).max_dispatch as usize,
            ) as *mut PartitionDispatch;
            (*proute).nonleaf_partitions = crate::utils::palloc::repalloc(
                (*proute).nonleaf_partitions as *mut c_void,
                size_of::<*mut ResultRelInfo>() * (*proute).max_dispatch as usize,
            ) as *mut *mut ResultRelInfo;
        }
    }
    *(*proute).partition_dispatch_info.add(dispatchidx as usize) = pd;

    /*
     * If setting up a PartitionDispatch for a sub-partitioned table, we may
     * also need a minimally valid ResultRelInfo for checking the partition
     * constraint later; set that up now.
     */
    if !parent_pd.is_null() {
        let rri: *mut ResultRelInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);

        InitResultRelInfo(rri, rel, 0, rootResultRelInfo, 0);
        *(*proute).nonleaf_partitions.add(dispatchidx as usize) = rri;
    } else {
        *(*proute).nonleaf_partitions.add(dispatchidx as usize) = ptr::null_mut();
    }

    /*
     * Finally, if setting up a PartitionDispatch for a sub-partitioned table,
     * install a downlink in the parent to allow quick descent.
     */
    if !parent_pd.is_null() {
        Assert!(*(*parent_pd).indexes.as_ptr().add(partidx as usize) == -1);
        *(*parent_pd).indexes.as_mut_ptr().add(partidx as usize) = dispatchidx;
    }

    MemoryContextSwitchTo(oldcxt);

    pd
}

/*
 * ExecCleanupTupleRouting -- Clean up objects allocated for partition tuple
 * routing.
 *
 * Close all the partitioned tables, leaf partitions, and their indices.
 */
pub unsafe fn ExecCleanupTupleRouting(
    mtstate: *mut ModifyTableState,
    proute: *mut PartitionTupleRouting,
) {
    let proute = proute as *mut PartitionTupleRoutingReal;
    let i: c_int;

    /*
     * Remember, proute->partition_dispatch_info[0] corresponds to the root
     * partitioned table, which we must not try to close, because it is the
     * main target table of the query that will be closed by callers such as
     * ExecEndPlan() or DoCopy(). Also, tupslot is NULL for the root
     * partitioned table.
     */
    let mut i = 1;
    while i < (*proute).num_dispatch {
        let pd = *(*proute).partition_dispatch_info.add(i as usize);

        table_close((*pd).reldesc, NoLock);

        if !(*pd).tupslot.is_null() {
            ExecDropSingleTupleTableSlot((*pd).tupslot);
        }
        i += 1;
    }

    let mut i = 0;
    while i < (*proute).num_partitions {
        let resultRelInfo = *(*proute).partitions.add(i as usize);

        /* Allow any FDWs to shut down */
        if !(*resultRelInfo).ri_FdwRoutine.is_null()
            && (*(*resultRelInfo).ri_FdwRoutine).EndForeignInsert.is_some()
        {
            ((*(*resultRelInfo).ri_FdwRoutine).EndForeignInsert.unwrap())(
                (*mtstate).ps.state,
                resultRelInfo,
            );
        }

        /*
         * Close it if it's not one of the result relations borrowed from the
         * owning ModifyTableState; those will be closed by ExecEndPlan().
         */
        if *(*proute).is_borrowed_rel.add(i as usize) {
            i += 1;
            continue;
        }

        ExecCloseIndices(resultRelInfo);
        table_close((*resultRelInfo).ri_RelationDesc, NoLock);
        i += 1;
    }
}

/* ----------------
 *		FormPartitionKeyDatum
 *			Construct values[] and isnull[] arrays for the partition key
 *			of a tuple.
 *
 *	pd				Partition dispatch object of the partitioned table
 *	slot			Heap tuple from which to extract partition key
 *	estate			executor state for evaluating any partition key
 *					expressions (must be non-NULL)
 *	values			Array of partition key Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * the ecxt_scantuple slot of estate's per-tuple expr context must point to
 * the heap tuple passed in.
 * ----------------
 */
unsafe fn FormPartitionKeyDatum(
    pd: PartitionDispatch,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    values: *mut Datum,
    isnull: *mut bool,
) {
    /* TODO(pg-port): slot_getattr - access/common/heaptuple.c */
    unsafe fn slot_getattr(slot: *mut TupleTableSlot, attnum: AttrNumber, isnull: *mut bool) -> Datum {
        unimplemented!("TODO(pg-port): slot_getattr - access/common/heaptuple.c")
    }
    /* TODO(pg-port): ExecEvalExprSwitchContext - executor/execExpr.c */
    unsafe fn ExecEvalExprSwitchContext(
        state: *mut ExprState,
        econtext: *mut ExprContext,
        isnull: *mut bool,
    ) -> Datum {
        unimplemented!("TODO(pg-port): ExecEvalExprSwitchContext - executor/execExpr.c")
    }

    let mut partexpr_item: *mut crate::nodes::pg_list::ListCell;

    if (*(*pd).key).partexprs != NIL && (*pd).keystate == NIL {
        /* Check caller has set up context correctly */
        Assert!(
            !estate.is_null()
                && (*GetPerTupleExprContext(estate)).ecxt_scantuple == slot
        );

        /* First time through, set up expression evaluation state */
        (*pd).keystate = ExecPrepareExprList((*(*pd).key).partexprs, estate);
    }

    partexpr_item = list_head((*pd).keystate);
    let mut i: c_int = 0;
    while i < (*(*pd).key).partnatts as c_int {
        let keycol: AttrNumber = *(*(*pd).key).partattrs.add(i as usize);
        let datum: Datum;
        let mut isNull: bool = false;

        if keycol != 0 {
            /* Plain column; get the value directly from the heap tuple */
            datum = slot_getattr(slot, keycol, &mut isNull);
        } else {
            /* Expression; need to evaluate it */
            if partexpr_item.is_null() {
                elog!(ERROR, "wrong number of partition key expressions");
            }
            datum = ExecEvalExprSwitchContext(
                lfirst(partexpr_item) as *mut ExprState,
                GetPerTupleExprContext(estate),
                &mut isNull,
            );
            partexpr_item = lnext((*pd).keystate, partexpr_item);
        }
        *values.add(i as usize) = datum;
        *isnull.add(i as usize) = isNull;
        i += 1;
    }

    if !partexpr_item.is_null() {
        elog!(ERROR, "wrong number of partition key expressions");
    }
}

/*
 * get_partition_for_tuple
 *		Finds partition of relation which accepts the partition key specified
 *		in values and isnull.
 *
 * Return value is index of the partition (>= 0 and < partdesc->nparts) if one
 * found or -1 if none found.
 */
unsafe fn get_partition_for_tuple(
    pd: PartitionDispatch,
    values: *mut Datum,
    isnull: *mut bool,
) -> c_int {
    let mut bound_offset: c_int = -1;
    let mut part_index: c_int = -1;
    let key: PartitionKey = (*pd).key;
    let partdesc: PartitionDesc = (*pd).partdesc;
    let boundinfo: *mut PartitionBoundInfoFull =
        (*partdesc).boundinfo as *mut PartitionBoundInfoFull;

    /*
     * In the switch statement below, when we perform a cached lookup for
     * RANGE and LIST partitioned tables, if we find that the last found
     * partition matches the 'values', we return the partition index right
     * away.  We do this instead of breaking out of the switch as we don't
     * want to execute the code about the DEFAULT partition or do any updates
     * for any of the cache-related fields.  That would be a waste of effort
     * as we already know it's not the DEFAULT partition and have no need to
     * increment the number of times we've hit the same partition any higher
     * than PARTITION_CACHED_FIND_THRESHOLD.
     */

    /* Route as appropriate based on partitioning strategy. */
    match (*key).strategy {
        PARTITION_STRATEGY_HASH => {
            let rowHash: u64;

            /* hash partitioning is too cheap to bother caching */
            rowHash = compute_partition_hash_value(
                (*key).partnatts as c_int,
                (*key).partsupfunc,
                (*key).partcollation,
                values,
                isnull,
            );

            /*
             * HASH partitions can't have a DEFAULT partition and we don't
             * do any caching work for them, so just return the part index
             */
            return *(*boundinfo).indexes.add(
                (rowHash % (*boundinfo).nindexes as u64) as usize,
            );
        }

        PARTITION_STRATEGY_LIST => {
            if *isnull.add(0) {
                /* this is far too cheap to bother doing any caching */
                if partition_bound_accepts_nulls(boundinfo as PartitionBoundInfo) {
                    /*
                     * When there is a NULL partition we just return that
                     * directly.  We don't have a bound_offset so it's not
                     * valid to drop into the code after the switch which
                     * checks and updates the cache fields.
                     */
                    return (*boundinfo).null_index;
                }
            } else {
                let mut equal: bool = false;

                if (*partdesc).last_found_count >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last_datum_offset: c_int = (*partdesc).last_found_datum_index;
                    let lastDatum: Datum = *(*(*boundinfo).datums.add(last_datum_offset as usize)).add(0);
                    let cmpval: i32;

                    /* does the last found datum index match this datum? */
                    cmpval = DatumGetInt32(FunctionCall2Coll(
                        (*key).partsupfunc.add(0),
                        *(*key).partcollation.add(0),
                        lastDatum,
                        *values.add(0),
                    ));

                    if cmpval == 0 {
                        return *(*boundinfo).indexes.add(last_datum_offset as usize);
                    }

                    /* fall-through and do a manual lookup */
                }

                bound_offset = partition_list_bsearch(
                    (*key).partsupfunc,
                    (*key).partcollation,
                    boundinfo as PartitionBoundInfo,
                    *values.add(0),
                    &mut equal,
                );
                if bound_offset >= 0 && equal {
                    part_index = *(*boundinfo).indexes.add(bound_offset as usize);
                }
            }
        }

        PARTITION_STRATEGY_RANGE => {
            let mut equal: bool = false;
            let mut range_partkey_has_null: bool = false;
            let mut i: c_int = 0;

            /*
             * No range includes NULL, so this will be accepted by the
             * default partition if there is one, and otherwise rejected.
             */
            while i < (*key).partnatts as c_int {
                if *isnull.add(i as usize) {
                    range_partkey_has_null = true;
                    break;
                }
                i += 1;
            }

            /* NULLs belong in the DEFAULT partition */
            if range_partkey_has_null {
                /* part_index stays -1; will use default below */
            } else {
                if (*partdesc).last_found_count >= PARTITION_CACHED_FIND_THRESHOLD {
                    let last_datum_offset: c_int = (*partdesc).last_found_datum_index;
                    let lastDatums: *mut Datum = *(*boundinfo).datums.add(last_datum_offset as usize);
                    let kind = *(*boundinfo).kind.add(last_datum_offset as usize);
                    let cmpval: i32;

                    /* check if the value is >= to the lower bound */
                    cmpval = partition_rbound_datum_cmp(
                        (*key).partsupfunc,
                        (*key).partcollation,
                        lastDatums,
                        kind,
                        values,
                        (*key).partnatts as c_int,
                    );

                    /*
                     * If it's equal to the lower bound then no need to check
                     * the upper bound.
                     */
                    if cmpval == 0 {
                        return *(*boundinfo).indexes.add((last_datum_offset + 1) as usize);
                    }

                    if cmpval < 0 && last_datum_offset + 1 < (*boundinfo).ndatums {
                        /* check if the value is below the upper bound */
                        let lastDatums2: *mut Datum = *(*boundinfo).datums.add((last_datum_offset + 1) as usize);
                        let kind2 = *(*boundinfo).kind.add((last_datum_offset + 1) as usize);
                        let cmpval2 = partition_rbound_datum_cmp(
                            (*key).partsupfunc,
                            (*key).partcollation,
                            lastDatums2,
                            kind2,
                            values,
                            (*key).partnatts as c_int,
                        );

                        if cmpval2 > 0 {
                            return *(*boundinfo).indexes.add((last_datum_offset + 1) as usize);
                        }
                    }
                    /* fall-through and do a manual lookup */
                }

                bound_offset = partition_range_datum_bsearch(
                    (*key).partsupfunc,
                    (*key).partcollation,
                    boundinfo as PartitionBoundInfo,
                    (*key).partnatts as c_int,
                    values,
                    &mut equal,
                );

                /*
                 * The bound at bound_offset is less than or equal to the
                 * tuple value, so the bound at offset+1 is the upper bound of
                 * the partition we're looking for, if there actually exists
                 * one.
                 */
                part_index = *(*boundinfo).indexes.add((bound_offset + 1) as usize);
            }
        }

        _ => {
            elog!(ERROR, "unexpected partition strategy: {}", (*key).strategy as c_int);
            unreachable!()
        }
    }

    /*
     * part_index < 0 means we failed to find a partition of this parent. Use
     * the default partition, if there is one.
     */
    if part_index < 0 {
        /*
         * No need to reset the cache fields here.  The next set of values
         * might end up belonging to the cached partition, so leaving the
         * cache alone improves the chances of a cache hit on the next lookup.
         */
        return (*boundinfo).default_index;
    }

    /* we should only make it here when the code above set bound_offset */
    Assert!(bound_offset >= 0);

    /*
     * Attend to the cache fields.  If the bound_offset matches the last
     * cached bound offset then we've found the same partition as last time,
     * so bump the count by one.
     */
    if bound_offset == (*partdesc).last_found_datum_index {
        (*partdesc).last_found_count += 1;
    } else {
        (*partdesc).last_found_count = 1;
        (*partdesc).last_found_part_index = part_index;
        (*partdesc).last_found_datum_index = bound_offset;
    }

    part_index
}

/*
 * ExecBuildSlotPartitionKeyDescription
 *
 * This works very much like BuildIndexValueDescription() and is currently
 * used for building error messages when ExecFindPartition() fails to find
 * partition for a row.
 */
unsafe fn ExecBuildSlotPartitionKeyDescription(
    rel: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    maxfieldlen: c_int,
) -> *mut c_char {
    let mut buf: StringInfoData = std::mem::zeroed();
    let key: PartitionKey = RelationGetPartitionKey(rel);
    let partnatts: c_int = get_partition_natts(key);
    let relid: Oid = RelationGetRelid(rel);
    let aclresult: AclResult;

    if check_enable_rls(relid, InvalidOid, true) == RLS_ENABLED {
        return ptr::null_mut();
    }

    /* If the user has table-level access, just go build the description. */
    aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
    if aclresult != ACLCHECK_OK {
        /*
         * Step through the columns of the partition key and make sure the
         * user has SELECT rights on all of them.
         */
        let mut i: c_int = 0;
        while i < partnatts {
            let attnum: AttrNumber = get_partition_col_attnum(key, i);

            /*
             * If this partition key column is an expression, we return no
             * detail rather than try to figure out what column(s) the
             * expression includes and if the user has SELECT rights on them.
             */
            if attnum == InvalidAttrNumber
                || pg_attribute_aclcheck(relid, attnum, GetUserId(), ACL_SELECT) != ACLCHECK_OK
            {
                return ptr::null_mut();
            }
            i += 1;
        }
    }

    initStringInfo(&mut buf);
    {
        let coldef = pg_get_partkeydef_columns(relid, true);
        let fmt = format!("({}) = (", CStr::from_ptr(coldef).to_string_lossy());
        appendBinaryStringInfo(&mut buf, fmt.as_ptr() as *const c_void, fmt.len() as c_int);
    }

    let mut i: c_int = 0;
    while i < partnatts {
        let val: *const c_char;
        let val_owned: *mut c_char;
        let vallen: c_int;

        if *isnull.add(i as usize) {
            val = b"null\0".as_ptr() as *const c_char;
            let vallen = 4 as c_int;
            if i > 0 {
                appendStringInfoString(&mut buf, b", \0".as_ptr() as *const c_char);
            }
            appendBinaryStringInfo(&mut buf, val as *const c_void, vallen);
        } else {
            let mut foutoid: Oid = 0;
            let mut typisvarlena: bool = false;

            getTypeOutputInfo(
                get_partition_col_typid(key, i),
                &mut foutoid,
                &mut typisvarlena,
            );
            val_owned = OidOutputFunctionCall(foutoid, *values.add(i as usize));

            if i > 0 {
                appendStringInfoString(&mut buf, b", \0".as_ptr() as *const c_char);
            }

            /* truncate if needed */
            let mut vallen = libc_strlen(val_owned) as c_int;
            if vallen <= maxfieldlen {
                appendBinaryStringInfo(&mut buf, val_owned as *const c_void, vallen);
            } else {
                vallen = pg_mbcliplen(val_owned, vallen, maxfieldlen);
                appendBinaryStringInfo(&mut buf, val_owned as *const c_void, vallen);
                appendStringInfoString(&mut buf, b"...\0".as_ptr() as *const c_char);
            }
        }
        i += 1;
    }

    appendStringInfoChar(&mut buf, b')' as c_char);

    buf.data
}

/* TODO(pg-port): strlen from libc */
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut p = s;
    while *p != 0 { p = p.add(1); }
    p.offset_from(s) as usize
}

/*
 * adjust_partition_colnos
 *		Adjust the list of UPDATE target column numbers to account for
 *		attribute differences between the parent and the partition.
 *
 * Note: mustn't be called if no adjustment is required.
 */
unsafe fn adjust_partition_colnos(
    colnos: *mut List,
    leaf_part_rri: *mut ResultRelInfo,
) -> *mut List {
    let map: *mut TupleConversionMap = ExecGetChildToRootMap(leaf_part_rri);

    Assert!(!map.is_null());

    adjust_partition_colnos_using_map(colnos, (*map).attrMap)
}

/*
 * adjust_partition_colnos_using_map
 *		Like adjust_partition_colnos, but uses a caller-supplied map instead
 *		of assuming to map from the "root" result relation.
 *
 * Note: mustn't be called if no adjustment is required.
 */
unsafe fn adjust_partition_colnos_using_map(
    colnos: *mut List,
    attrMap: *mut AttrMap,
) -> *mut List {
    let mut new_colnos: *mut List = NIL;

    Assert!(!attrMap.is_null()); /* else we shouldn't be here */

    foreach!(lc, colnos, {
        let parentattrno: AttrNumber = lfirst_int(crate::current_cell!(lc)) as AttrNumber;

        if parentattrno <= 0
            || parentattrno > (*attrMap).maplen as AttrNumber
            || *(*attrMap).attnums.add((parentattrno - 1) as usize) == 0
        {
            elog!(ERROR, "unexpected attno {} in target column list", parentattrno);
        }
        new_colnos = lappend_int(
            new_colnos,
            *(*attrMap).attnums.add((parentattrno - 1) as usize) as c_int,
        );
    });

    new_colnos
}

/*-------------------------------------------------------------------------
 * Run-Time Partition Pruning Support.
 *-------------------------------------------------------------------------
 */

/*
 * ExecDoInitialPruning
 *		Perform runtime "initial" pruning, if necessary, to determine the set
 *		of child subnodes that need to be initialized during ExecInitNode() for
 *		plan nodes that support partition pruning.
 */
pub unsafe fn ExecDoInitialPruning(estate: *mut EState) {
    foreach!(lc, (*estate).es_part_prune_infos, {
        let pruneinfo: *mut PartitionPruneInfo =
            lfirst_node!(PartitionPruneInfo, T_PartitionPruneInfo, crate::current_cell!(lc));
        let prunestate: *mut PartitionPruneState;
        let mut validsubplans: *mut Bitmapset = ptr::null_mut();
        let mut all_leafpart_rtis: *mut Bitmapset = ptr::null_mut();
        let mut validsubplan_rtis: *mut Bitmapset = ptr::null_mut();

        /* Create and save the PartitionPruneState. */
        prunestate = CreatePartitionPruneState(estate, pruneinfo, &mut all_leafpart_rtis);
        (*estate).es_part_prune_states =
            lappend((*estate).es_part_prune_states, prunestate as *mut c_void);

        /*
         * Perform initial pruning steps, if any, and save the result
         * bitmapset or NULL as described in the header comment.
         */
        if (*(prunestate as *mut PartitionPruneStateReal)).do_initial_prune {
            validsubplans = ExecFindMatchingSubPlans(prunestate, true, &mut validsubplan_rtis);
        } else {
            validsubplan_rtis = all_leafpart_rtis;
        }

        (*estate).es_unpruned_relids =
            bms_add_members((*estate).es_unpruned_relids, validsubplan_rtis);
        (*estate).es_part_prune_results =
            lappend((*estate).es_part_prune_results, validsubplans as *mut c_void);
    });
}

/*
 * ExecInitPartitionExecPruning
 *		Initialize the data structures needed for runtime "exec" partition
 *		pruning and return the result of initial pruning, if available.
 */
pub unsafe fn ExecInitPartitionExecPruning(
    planstate: *mut crate::nodes::execnodes::PlanState,
    n_total_subplans: c_int,
    part_prune_index: c_int,
    relids: *mut Bitmapset,
    initially_valid_subplans: *mut *mut Bitmapset,
) -> *mut PartitionPruneState {
    let prunestate: *mut PartitionPruneState;
    let estate: *mut EState = (*planstate).state;
    let pruneinfo: *mut PartitionPruneInfo;

    /* Obtain the pruneinfo we need. */
    pruneinfo = list_nth_node!(
        PartitionPruneInfo,
        T_PartitionPruneInfo,
        (*estate).es_part_prune_infos,
        part_prune_index
    );

    /* Its relids better match the plan node's or the planner messed up. */
    if !bms_equal(relids, (*pruneinfo).relids) {
        ereport!(ERROR, errmsg!(
            "wrong pruneinfo with relids found at part_prune_index={} contained in plan node",
            part_prune_index
        ) /* C also: bmsToString(pruneinfo->relids) /* C also: bmsToString(relids) */ */);
    }

    /*
     * The PartitionPruneState would have been created by
     * ExecDoInitialPruning() and stored as the part_prune_index'th element of
     * EState.es_part_prune_states.
     */
    prunestate = list_nth((*estate).es_part_prune_states, part_prune_index)
        as *mut PartitionPruneState;
    Assert!(!prunestate.is_null());

    /* Use the result of initial pruning done by ExecDoInitialPruning(). */
    if (*(prunestate as *mut PartitionPruneStateReal)).do_initial_prune {
        *initially_valid_subplans = list_nth_node!(
            Bitmapset,
            T_Bitmapset,
            (*estate).es_part_prune_results,
            part_prune_index
        );
    } else {
        /* No pruning, so we'll need to initialize all subplans */
        Assert!(n_total_subplans > 0);
        *initially_valid_subplans = bms_add_range(ptr::null_mut(), 0, n_total_subplans - 1);
    }

    /*
     * The exec pruning state must also be initialized, if needed, before it
     * can be used for pruning during execution.
     */
    if (*(prunestate as *mut PartitionPruneStateReal)).do_exec_prune {
        InitExecPartitionPruneContexts(
            prunestate as *mut PartitionPruneStateReal,
            planstate,
            *initially_valid_subplans,
            n_total_subplans,
        );
    }

    prunestate
}

/*
 * CreatePartitionPruneState
 *		Build the data structure required for calling ExecFindMatchingSubPlans.
 */
unsafe fn CreatePartitionPruneState(
    estate: *mut EState,
    pruneinfo: *mut PartitionPruneInfo,
    all_leafpart_rtis: *mut *mut Bitmapset,
) -> *mut PartitionPruneState {
    let prunestate: *mut PartitionPruneStateReal;
    let n_part_hierarchies: c_int;

    /*
     * Expression context that will be used by partkey_datum_from_expr() to
     * evaluate expressions for comparison against partition bounds.
     */
    let econtext: *mut ExprContext = CreateExprContext(estate);

    /* For data reading, executor always includes detached partitions */
    if (*estate).es_partition_directory.is_null() {
        (*estate).es_partition_directory =
            CreatePartitionDirectory((*estate).es_query_cxt, false);
    }

    n_part_hierarchies = list_length((*pruneinfo).prune_infos);
    Assert!(n_part_hierarchies > 0);

    /*
     * Allocate the data structure
     */
    prunestate = palloc(
        offsetof!(PartitionPruneStateReal, partprunedata) as usize
            + size_of::<*mut PartitionPruningData>() * n_part_hierarchies as usize,
    ) as *mut PartitionPruneStateReal;

    /* Save ExprContext for use during InitExecPartitionPruneContexts(). */
    (*prunestate).econtext = econtext;
    (*prunestate).execparamids = ptr::null_mut();
    /* other_subplans can change at runtime, so we need our own copy */
    (*prunestate).other_subplans = bms_copy((*pruneinfo).other_subplans);
    (*prunestate).do_initial_prune = false; /* may be set below */
    (*prunestate).do_exec_prune = false;    /* may be set below */
    (*prunestate).num_partprunedata = n_part_hierarchies;

    /*
     * Create a short-term memory context which we'll use when making calls to
     * the partition pruning functions.
     */
    (*prunestate).prune_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        b"Partition Prune\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );

    let mut i: c_int = 0;
    foreach!(lc, (*pruneinfo).prune_infos, {
        let partrelpruneinfos: *mut List =
            lfirst_node!(List, T_List, crate::current_cell!(lc));
        let npartrelpruneinfos: c_int = list_length(partrelpruneinfos);
        let prunedata: *mut PartitionPruningData;

        prunedata = palloc(
            offsetof!(PartitionPruningData, partrelprunedata) as usize
                + npartrelpruneinfos as usize * size_of::<PartitionedRelPruningData>(),
        ) as *mut PartitionPruningData;
        *(*prunestate).partprunedata.as_mut_ptr().add(i as usize) = prunedata;
        (*prunedata).num_partrelprunedata = npartrelpruneinfos;

        let mut j: c_int = 0;
        foreach!(lc2, partrelpruneinfos, {
            let pinfo: *mut PartitionedRelPruneInfo =
                lfirst_node!(PartitionedRelPruneInfo, T_PartitionedRelPruneInfo, crate::current_cell!(lc2));
            let pprune: *mut PartitionedRelPruningData =
                (*prunedata).partrelprunedata.as_mut_ptr().add(j as usize);
            let partrel: Relation;
            let partdesc: PartitionDesc;
            let partkey: PartitionKey;

            /*
             * We can rely on the copies of the partitioned table's partition
             * key and partition descriptor appearing in its relcache entry.
             */
            partrel = ExecGetRangeTableRelation(estate, (*pinfo).rtindex, false);

            /* Remember for InitExecPartitionPruneContexts(). */
            (*pprune).partrel = partrel;

            partkey = RelationGetPartitionKey(partrel);
            partdesc = PartitionDirectoryLookup((*estate).es_partition_directory, partrel);

            /*
             * Initialize the subplan_map and subpart_map.
             */
            (*pprune).nparts = (*partdesc).nparts;
            (*pprune).subplan_map = palloc(size_of::<c_int>() * (*partdesc).nparts as usize) as *mut c_int;

            if (*partdesc).nparts == (*pinfo).nparts
                && ptr::eq(
                    (*partdesc).oids as *const u8,
                    (*pinfo).relid_map as *const u8,
                ) || ((*partdesc).nparts == (*pinfo).nparts
                    && std::slice::from_raw_parts((*partdesc).oids, (*partdesc).nparts as usize)
                        == std::slice::from_raw_parts((*pinfo).relid_map, (*pinfo).nparts as usize))
            {
                (*pprune).subpart_map = (*pinfo).subpart_map;
                (*pprune).leafpart_rti_map = (*pinfo).leafpart_rti_map;
                ptr::copy_nonoverlapping(
                    (*pinfo).subplan_map,
                    (*pprune).subplan_map,
                    (*pinfo).nparts as usize,
                );
            } else {
                let mut pd_idx: c_int = 0;

                (*pprune).subpart_map =
                    palloc(size_of::<c_int>() * (*partdesc).nparts as usize) as *mut c_int;
                (*pprune).leafpart_rti_map =
                    palloc(size_of::<c_int>() * (*partdesc).nparts as usize) as *mut c_int;

                let mut pp_idx: c_int = 0;
                'outer: while pp_idx < (*partdesc).nparts {
                    /* Skip any InvalidOid relid_map entries */
                    while pd_idx < (*pinfo).nparts && !OidIsValid(*(*pinfo).relid_map.add(pd_idx as usize)) {
                        pd_idx += 1;
                    }

                    'recheck: loop {
                        if pd_idx < (*pinfo).nparts
                            && *(*pinfo).relid_map.add(pd_idx as usize)
                                == *(*partdesc).oids.add(pp_idx as usize)
                        {
                            /* match... */
                            *(*pprune).subplan_map.add(pp_idx as usize) =
                                *(*pinfo).subplan_map.add(pd_idx as usize);
                            *(*pprune).subpart_map.add(pp_idx as usize) =
                                *(*pinfo).subpart_map.add(pd_idx as usize);
                            *(*pprune).leafpart_rti_map.add(pp_idx as usize) =
                                *(*pinfo).leafpart_rti_map.add(pd_idx as usize);
                            pd_idx += 1;
                            pp_idx += 1;
                            continue 'outer;
                        }

                        /*
                         * There isn't an exact match in the corresponding
                         * positions of both arrays.  Peek ahead in
                         * pinfo->relid_map.
                         */
                        let mut found_ahead = false;
                        let mut pd_idx2: c_int = pd_idx + 1;
                        while pd_idx2 < (*pinfo).nparts {
                            if *(*pinfo).relid_map.add(pd_idx2 as usize)
                                == *(*partdesc).oids.add(pp_idx as usize)
                            {
                                pd_idx = pd_idx2;
                                found_ahead = true;
                                break;
                            }
                            pd_idx2 += 1;
                        }
                        if found_ahead {
                            continue 'recheck;
                        }

                        *(*pprune).subpart_map.add(pp_idx as usize) = -1;
                        *(*pprune).subplan_map.add(pp_idx as usize) = -1;
                        *(*pprune).leafpart_rti_map.add(pp_idx as usize) = 0;
                        break 'recheck;
                    }
                    pp_idx += 1;
                }
            }

            /* present_parts is also subject to later modification */
            (*pprune).present_parts = bms_copy((*pinfo).present_parts);

            /*
             * Only initial_context is initialized here.  exec_context is
             * initialized during ExecInitPartitionExecPruning().
             */
            (*pprune).initial_pruning_steps = (*pinfo).initial_pruning_steps;
            if !(*pinfo).initial_pruning_steps.is_null()
                && ((*(*econtext).ecxt_estate).es_top_eflags & EXEC_FLAG_EXPLAIN_GENERIC) == 0
            {
                InitPartitionPruneContext(
                    &mut (*pprune).initial_context,
                    (*pprune).initial_pruning_steps,
                    partdesc,
                    partkey,
                    ptr::null_mut(),
                    econtext,
                );
                /* Record whether initial pruning is needed at any level */
                (*prunestate).do_initial_prune = true;
            }
            (*pprune).exec_pruning_steps = (*pinfo).exec_pruning_steps;
            if !(*pinfo).exec_pruning_steps.is_null()
                && ((*(*econtext).ecxt_estate).es_top_eflags & EXEC_FLAG_EXPLAIN_GENERIC) == 0
            {
                /* Record whether exec pruning is needed at any level */
                (*prunestate).do_exec_prune = true;
            }

            /*
             * Accumulate the IDs of all PARAM_EXEC Params affecting the
             * partitioning decisions at this plan node.
             */
            (*prunestate).execparamids =
                bms_add_members((*prunestate).execparamids, (*pinfo).execparamids);

            /*
             * Return all leaf partition indexes if we're skipping pruning in
             * the EXPLAIN (GENERIC_PLAN) case.
             */
            if !(*pinfo).initial_pruning_steps.is_null() && !(*prunestate).do_initial_prune {
                let mut part_index: c_int = -1;
                loop {
                    part_index = bms_next_member((*pprune).present_parts, part_index);
                    if part_index < 0 { break; }
                    let rtindex: crate::c::Index =
                        *(*pprune).leafpart_rti_map.add(part_index as usize) as crate::c::Index;
                    if rtindex != 0 {
                        *all_leafpart_rtis = bms_add_member(*all_leafpart_rtis, rtindex as c_int);
                    }
                }
            }

            j += 1;
        });
        i += 1;
    });

    prunestate as *mut PartitionPruneState
}

/*
 * Initialize a PartitionPruneContext for the given list of pruning steps.
 */
unsafe fn InitPartitionPruneContext(
    context: *mut PartitionPruneContext,
    pruning_steps: *mut List,
    partdesc: PartitionDesc,
    partkey: PartitionKey,
    planstate: *mut crate::nodes::execnodes::PlanState,
    econtext: *mut ExprContext,
) {
    let n_steps: c_int = list_length(pruning_steps);
    let partnatts: c_int;

    (*context).strategy = (*partkey).strategy as c_char;
    (*context).partnatts = (*partkey).partnatts as c_int;
    partnatts = (*partkey).partnatts as c_int;
    (*context).nparts = (*partdesc).nparts;
    (*context).boundinfo = (*partdesc).boundinfo;
    (*context).partcollation = (*partkey).partcollation;
    (*context).partsupfunc = (*partkey).partsupfunc;

    /* We'll look up type-specific support functions as needed */
    (*context).stepcmpfuncs = palloc0(
        size_of::<FmgrInfo>() * n_steps as usize * partnatts as usize,
    ) as *mut FmgrInfo;

    (*context).ppccontext = CurrentMemoryContext;
    (*context).planstate = planstate;
    (*context).exprcontext = econtext;

    /* Initialize expression state for each expression we need */
    (*context).exprstates = palloc0(
        size_of::<*mut ExprState>() * n_steps as usize * partnatts as usize,
    ) as *mut *mut ExprState;

    foreach!(lc, pruning_steps, {
        let step = lfirst(crate::current_cell!(lc)) as *mut PartitionPruneStepOp;
        let mut lc2: *mut crate::nodes::pg_list::ListCell;
        let mut keyno: c_int;

        /* not needed for other step kinds */
        if !IsA!(step, T_PartitionPruneStepOp) {
            continue;
        }

        lc2 = list_head((*step).exprs);

        Assert!(list_length((*step).exprs) <= partnatts);

        keyno = 0;
        while keyno < partnatts {
            if bms_is_member(keyno, (*step).nullkeys) {
                keyno += 1;
                continue;
            }

            if !lc2.is_null() {
                let expr = lfirst(lc2) as *mut Expr;

                /* not needed for Consts */
                if !IsA!(expr, T_Const) {
                    let stateidx = PruneCxtStateIdx(partnatts, (*step).step.step_id, keyno);

                    /*
                     * When planstate is NULL, pruning_steps is known not to
                     * contain any expressions that depend on the parent plan.
                     */
                    if planstate.is_null() {
                        *(*context).exprstates.add(stateidx as usize) =
                            ExecInitExprWithParams(expr, (*econtext).ecxt_param_list_info);
                    } else {
                        *(*context).exprstates.add(stateidx as usize) =
                            ExecInitExpr(expr, (*context).planstate);
                    }
                }
                lc2 = lnext((*step).exprs, lc2);
            }
            keyno += 1;
        }
    });
}

/*
 * InitExecPartitionPruneContexts
 *		Initialize exec pruning contexts deferred by CreatePartitionPruneState().
 */
unsafe fn InitExecPartitionPruneContexts(
    prunestate: *mut PartitionPruneStateReal,
    parent_plan: *mut crate::nodes::execnodes::PlanState,
    initially_valid_subplans: *mut Bitmapset,
    n_total_subplans: c_int,
) {
    let estate: *mut EState;
    let mut new_subplan_indexes: *mut c_int = ptr::null_mut();
    let mut new_other_subplans: *mut Bitmapset;
    let mut fix_subplan_map: bool = false;

    Assert!((*prunestate).do_exec_prune);
    Assert!(!parent_plan.is_null());
    estate = (*parent_plan).state;

    /*
     * No need to fix subplans maps if initial pruning didn't eliminate any
     * subplans.
     */
    if bms_num_members(initially_valid_subplans) < n_total_subplans {
        fix_subplan_map = true;

        /*
         * First we must build a temporary array which maps old subplan
         * indexes to new ones.  For convenience of initialization, we use
         * 1-based indexes in this array and leave pruned items as 0.
         */
        new_subplan_indexes =
            palloc0(size_of::<c_int>() * n_total_subplans as usize) as *mut c_int;
        let mut newidx: c_int = 1;
        let mut idx: c_int = -1;
        loop {
            idx = bms_next_member(initially_valid_subplans, idx);
            if idx < 0 { break; }
            Assert!(idx < n_total_subplans);
            *new_subplan_indexes.add(idx as usize) = newidx;
            newidx += 1;
        }
    }

    /*
     * Now we can update each PartitionedRelPruneInfo's subplan_map with new
     * subplan indexes.  We must also recompute its present_parts bitmap.
     */
    let mut i: c_int = 0;
    while i < (*prunestate).num_partprunedata {
        let prunedata: *mut PartitionPruningData =
            *(*prunestate).partprunedata.as_ptr().add(i as usize);

        /*
         * Within each hierarchy, we perform this loop in back-to-front order
         * so that we determine present_parts for the lowest-level partitioned
         * tables first.
         */
        let mut j: c_int = (*prunedata).num_partrelprunedata - 1;
        while j >= 0 {
            let pprune: *mut PartitionedRelPruningData =
                (*prunedata).partrelprunedata.as_mut_ptr().add(j as usize);
            let nparts: c_int = (*pprune).nparts;

            /* Initialize PartitionPruneContext for exec pruning, if needed. */
            if (*pprune).exec_pruning_steps != NIL {
                let partkey: PartitionKey = RelationGetPartitionKey((*pprune).partrel);
                let partdesc: PartitionDesc = PartitionDirectoryLookup(
                    (*estate).es_partition_directory,
                    (*pprune).partrel,
                );

                InitPartitionPruneContext(
                    &mut (*pprune).exec_context,
                    (*pprune).exec_pruning_steps,
                    partdesc,
                    partkey,
                    parent_plan,
                    (*prunestate).econtext,
                );
            }

            if !fix_subplan_map {
                j -= 1;
                continue;
            }

            /* We just rebuild present_parts from scratch */
            bms_free((*pprune).present_parts);
            (*pprune).present_parts = ptr::null_mut();

            let mut k: c_int = 0;
            while k < nparts {
                let oldidx: c_int = *(*pprune).subplan_map.add(k as usize);

                /*
                 * If this partition existed as a subplan then change the old
                 * subplan index to the new subplan index.
                 */
                if oldidx >= 0 {
                    Assert!(oldidx < n_total_subplans);
                    *(*pprune).subplan_map.add(k as usize) =
                        *new_subplan_indexes.add(oldidx as usize) - 1;

                    if *new_subplan_indexes.add(oldidx as usize) > 0 {
                        (*pprune).present_parts =
                            bms_add_member((*pprune).present_parts, k);
                    }
                } else {
                    let subidx: c_int = *(*pprune).subpart_map.add(k as usize);
                    if subidx >= 0 {
                        let subprune: *mut PartitionedRelPruningData =
                            (*prunedata).partrelprunedata.as_mut_ptr().add(subidx as usize);

                        if !bms_is_empty((*subprune).present_parts) {
                            (*pprune).present_parts =
                                bms_add_member((*pprune).present_parts, k);
                        }
                    }
                }
                k += 1;
            }
            j -= 1;
        }
        i += 1;
    }

    /*
     * If we fixed subplan maps, we must also recompute the other_subplans
     * set, since indexes in it may change.
     */
    if fix_subplan_map {
        new_other_subplans = ptr::null_mut();
        let mut idx: c_int = -1;
        loop {
            idx = bms_next_member((*prunestate).other_subplans, idx);
            if idx < 0 { break; }
            new_other_subplans =
                bms_add_member(new_other_subplans, *new_subplan_indexes.add(idx as usize) - 1);
        }

        bms_free((*prunestate).other_subplans);
        (*prunestate).other_subplans = new_other_subplans;

        pfree(new_subplan_indexes as *mut c_void);
    }
}

/*
 * ExecFindMatchingSubPlans
 *		Determine which subplans match the pruning steps detailed in
 *		'prunestate' for the current comparison expression values.
 */
pub unsafe fn ExecFindMatchingSubPlans(
    prunestate: *mut PartitionPruneState,
    initial_prune: bool,
    validsubplan_rtis: *mut *mut Bitmapset,
) -> *mut Bitmapset {
    let prunestate = prunestate as *mut PartitionPruneStateReal;
    let mut result: *mut Bitmapset = ptr::null_mut();
    let oldcontext: MemoryContext;

    /*
     * Either we're here on the initial prune done during pruning
     * initialization, or we're at a point where PARAM_EXEC Params can be
     * evaluated *and* there are steps in which to do so.
     */
    Assert!(initial_prune || (*prunestate).do_exec_prune);
    Assert!(!validsubplan_rtis.is_null() || !initial_prune);

    /*
     * Switch to a temp context to avoid leaking memory in the executor's
     * query-lifespan memory context.
     */
    oldcontext = MemoryContextSwitchTo((*prunestate).prune_context);

    /*
     * For each hierarchy, do the pruning tests, and add nondeletable
     * subplans' indexes to "result".
     */
    let mut i: c_int = 0;
    while i < (*prunestate).num_partprunedata {
        let prunedata: *mut PartitionPruningData =
            *(*prunestate).partprunedata.as_ptr().add(i as usize);
        let pprune: *mut PartitionedRelPruningData =
            (*prunedata).partrelprunedata.as_mut_ptr().add(0);

        /*
         * We pass the zeroth item, belonging to the root table of the
         * hierarchy, and find_matching_subplans_recurse() takes care of
         * recursing to other (lower-level) parents as needed.
         */
        find_matching_subplans_recurse(
            prunedata,
            pprune,
            initial_prune,
            &mut result,
            validsubplan_rtis,
        );

        /*
         * Expression eval may have used space in ExprContext too. Avoid
         * accessing exec_context during initial pruning, as it is not valid
         * at that stage.
         */
        if !initial_prune && (*pprune).exec_pruning_steps != NIL {
            ResetExprContext((*pprune).exec_context.exprcontext);
        }
        i += 1;
    }

    /* Add in any subplans that partition pruning didn't account for */
    result = bms_add_members(result, (*prunestate).other_subplans);

    MemoryContextSwitchTo(oldcontext);

    /* Copy result out of the temp context before we reset it */
    result = bms_copy(result);
    if !validsubplan_rtis.is_null() {
        *validsubplan_rtis = bms_copy(*validsubplan_rtis);
    }

    MemoryContextReset((*prunestate).prune_context);

    result
}

/*
 * find_matching_subplans_recurse
 *		Recursive worker function for ExecFindMatchingSubPlans
 *
 * Adds valid (non-prunable) subplan IDs to *validsubplans.
 */
unsafe fn find_matching_subplans_recurse(
    prunedata: *mut PartitionPruningData,
    pprune: *mut PartitionedRelPruningData,
    initial_prune: bool,
    validsubplans: *mut *mut Bitmapset,
    validsubplan_rtis: *mut *mut Bitmapset,
) {
    let partset: *mut Bitmapset;

    /* Guard against stack overflow due to overly deep partition hierarchy. */
    check_stack_depth();

    /*
     * Prune as appropriate, if we have pruning steps matching the current
     * execution context.  Otherwise just include all partitions at this
     * level.
     */
    if initial_prune && (*pprune).initial_pruning_steps != NIL {
        partset = get_matching_partitions(
            &mut (*pprune).initial_context,
            (*pprune).initial_pruning_steps,
        );
    } else if !initial_prune && (*pprune).exec_pruning_steps != NIL {
        partset = get_matching_partitions(
            &mut (*pprune).exec_context,
            (*pprune).exec_pruning_steps,
        );
    } else {
        partset = (*pprune).present_parts;
    }

    /* Translate partset into subplan indexes */
    let mut idx: c_int = -1;
    loop {
        idx = bms_next_member(partset, idx);
        if idx < 0 { break; }

        if *(*pprune).subplan_map.add(idx as usize) >= 0 {
            *validsubplans = bms_add_member(
                *validsubplans,
                *(*pprune).subplan_map.add(idx as usize),
            );

            /*
             * Only report leaf partitions. Non-leaf partitions may appear
             * here when they use an unflattened Append or MergeAppend.
             */
            if !validsubplan_rtis.is_null()
                && *(*pprune).leafpart_rti_map.add(idx as usize) != 0
            {
                *validsubplan_rtis = bms_add_member(
                    *validsubplan_rtis,
                    *(*pprune).leafpart_rti_map.add(idx as usize),
                );
            }
        } else {
            let partidx: c_int = *(*pprune).subpart_map.add(idx as usize);

            if partidx >= 0 {
                find_matching_subplans_recurse(
                    prunedata,
                    (*prunedata).partrelprunedata.as_mut_ptr().add(partidx as usize),
                    initial_prune,
                    validsubplans,
                    validsubplan_rtis,
                );
            } else {
                /*
                 * We get here if the planner already pruned all the sub-
                 * partitions for this partition.  Silently ignore this
                 * partition in this case.
                 */
            }
        }
    }
}

//! utils/adt/partitionfuncs.c - functions for accessing partition-related metadata

use crate::prelude::*;

use crate::{PG_GETARG_OID, PG_RETURN_NULL, PG_RETURN_OID};

use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::catalog::pg_class::{RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE};
use crate::nodes::pg_list::{
    lcons_oid, lfirst_oid, linitial_oid, list_length, list_nth_oid, llast_oid, List, NIL,
};
use crate::postgres::{BoolGetDatum, Int32GetDatum, ObjectIdGetDatum};
use crate::storage::lockdefs::{AccessShareLock, LOCKMODE};
use crate::utils::fmgr::FunctionCallInfo;

use crate::{current_cell, foreach};

// catalog/pg_class.h: RELKIND_HAS_PARTITIONS(relkind)
#[inline]
fn RELKIND_HAS_PARTITIONS(relkind: c_char) -> bool {
    relkind == RELKIND_PARTITIONED_TABLE || relkind == RELKIND_PARTITIONED_INDEX
}

// funcapi.h: TypeFuncClass (only TYPEFUNC_COMPOSITE used here)
const TYPEFUNC_COMPOSITE: c_int = 1;

// utils/syscache.h: RELOID syscache id.
// TODO(pg-port): replace with the real RELOID constant once syscache.h is ported.
const RELOID: c_int = 0;

// funcapi.h: cross-call persistence context for set-returning functions.
// TODO(pg-port): replace with the real FuncCallContext once funcapi.c is ported.
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

// ---------------------------------------------------------------------------
// SRF support macros/functions (funcapi.h / funcapi.c) -- not yet ported.
// Stubbed locally so the partition functions translate 1:1.
// ---------------------------------------------------------------------------

// SRF_IS_FIRSTCALL()
unsafe fn SRF_IS_FIRSTCALL(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!("TODO(pg-port): SRF_IS_FIRSTCALL (utils/fmgr/funcapi.c not ported)")
}

// SRF_FIRSTCALL_INIT()
unsafe fn SRF_FIRSTCALL_INIT(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("TODO(pg-port): SRF_FIRSTCALL_INIT (utils/fmgr/funcapi.c not ported)")
}

// SRF_PERCALL_SETUP()
unsafe fn SRF_PERCALL_SETUP(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("TODO(pg-port): SRF_PERCALL_SETUP (utils/fmgr/funcapi.c not ported)")
}

// SRF_RETURN_NEXT(funcctx, result)
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!("TODO(pg-port): SRF_RETURN_NEXT (utils/fmgr/funcapi.c not ported)")
}

// SRF_RETURN_DONE(funcctx)
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!("TODO(pg-port): SRF_RETURN_DONE (utils/fmgr/funcapi.c not ported)")
}

// get_call_result_type(fcinfo, resultTypeId, resultTupleDesc)
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!("TODO(pg-port): get_call_result_type (utils/fmgr/funcapi.c not ported)")
}

// utils/syscache.h: SearchSysCacheExists1(RELOID, ...)
// TODO(pg-port): utils/cache/syscache.c not ported.
unsafe fn SearchSysCacheExists1(_cache_id: c_int, _key1: Datum) -> bool {
    unimplemented!("TODO(pg-port): SearchSysCacheExists1 (utils/cache/syscache.c not ported)")
}

// utils/lsyscache.h: get_rel_relkind()
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    unimplemented!("TODO(pg-port): get_rel_relkind (utils/cache/lsyscache.c not ported)")
}

// utils/lsyscache.h: get_rel_relispartition()
unsafe fn get_rel_relispartition(_relid: Oid) -> bool {
    unimplemented!("TODO(pg-port): get_rel_relispartition (utils/cache/lsyscache.c not ported)")
}

// catalog/pg_inherits.h: find_all_inheritors()
unsafe fn find_all_inheritors(_parentrelId: Oid, _lockmode: LOCKMODE, _numparents: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): find_all_inheritors (catalog/pg_inherits.c not ported)")
}

// catalog/partition.h: get_partition_ancestors()
unsafe fn get_partition_ancestors(_relid: Oid) -> *mut List {
    unimplemented!("TODO(pg-port): get_partition_ancestors (catalog/partition.c not ported)")
}

// HeapTupleGetDatum(tuple)
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!("TODO(pg-port): HeapTupleGetDatum (funcapi.h not ported)")
}

// nodes/pg_list.h: list_free()
unsafe fn list_free(_list: *mut List) {
    unimplemented!("TODO(pg-port): list_free (nodes/list.c not ported)")
}

/*
 * Checks if a given relation can be part of a partition tree.  Returns
 * false if the relation cannot be processed, in which case it is up to
 * the caller to decide what to do, by either raising an error or doing
 * something else.
 */
unsafe fn check_rel_can_be_partition(relid: Oid) -> bool {
    let relkind: c_char;
    let relispartition: bool;

    /* Check if relation exists */
    if !SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)) {
        return false;
    }

    relkind = get_rel_relkind(relid);
    relispartition = get_rel_relispartition(relid);

    /* Only allow relation types that can appear in partition trees. */
    if !relispartition && !RELKIND_HAS_PARTITIONS(relkind) {
        return false;
    }

    true
}

/*
 * pg_partition_tree
 *
 * Produce a view with one row per member of a partition tree, beginning
 * from the top-most parent given by the caller.  This gives information
 * about each partition, its immediate partitioned parent, if it is
 * a leaf partition and its level in the hierarchy.
 */
pub unsafe fn pg_partition_tree(fcinfo: FunctionCallInfo) -> Datum {
    const PG_PARTITION_TREE_COLS: usize = 4;
    let rootrelid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let funcctx: *mut FuncCallContext;
    let mut partitions: *mut List;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL(fcinfo) {
        let oldcxt: MemoryContext;
        let mut tupdesc: TupleDesc = null_mut();

        /* create a function context for cross-call persistence */
        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);

        if !check_rel_can_be_partition(rootrelid) {
            return SRF_RETURN_DONE(funcctx);
        }

        /* switch to memory context appropriate for multiple function calls */
        oldcxt = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /*
         * Find all members of inheritance set.  We only need AccessShareLock
         * on the children for the partition information lookup.
         */
        partitions = find_all_inheritors(rootrelid, AccessShareLock, null_mut());

        if get_call_result_type(fcinfo, null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        (*funcctx).tuple_desc = tupdesc;

        /* The only state we need is the partition list */
        (*funcctx).user_fctx = partitions as *mut c_void;

        MemoryContextSwitchTo(oldcxt);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP(fcinfo);
    partitions = (*funcctx).user_fctx as *mut List;

    if ((*funcctx).call_cntr as c_int) < list_length(partitions) {
        let result: Datum;
        let mut values: [Datum; PG_PARTITION_TREE_COLS] = [0; PG_PARTITION_TREE_COLS];
        let mut nulls: [bool; PG_PARTITION_TREE_COLS] = [false; PG_PARTITION_TREE_COLS];
        let tuple: HeapTuple;
        let mut parentid: Oid = InvalidOid;
        let relid: Oid = list_nth_oid(partitions, (*funcctx).call_cntr as c_int);
        let relkind: c_char = get_rel_relkind(relid);
        let mut level: c_int = 0;
        let ancestors: *mut List = get_partition_ancestors(relid);

        /*
         * Form tuple with appropriate data.
         */

        /* relid */
        values[0] = ObjectIdGetDatum(relid);

        /* parentid */
        if ancestors != NIL {
            parentid = linitial_oid(ancestors);
        }
        if OidIsValid(parentid) {
            values[1] = ObjectIdGetDatum(parentid);
        } else {
            nulls[1] = true;
        }

        /* isleaf */
        values[2] = BoolGetDatum(!RELKIND_HAS_PARTITIONS(relkind));

        /* level */
        if relid != rootrelid {
            foreach!(lc, ancestors, {
                level += 1;
                if lfirst_oid(current_cell!(lc)) == rootrelid {
                    break;
                }
            });
        }
        values[3] = Int32GetDatum(level);

        tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_ptr(), nulls.as_ptr());
        result = HeapTupleGetDatum(tuple);
        return SRF_RETURN_NEXT(funcctx, result);
    }

    /* done when there are no more elements left */
    SRF_RETURN_DONE(funcctx)
}

/*
 * pg_partition_root
 *
 * Returns the top-most parent of the partition tree to which a given
 * relation belongs, or NULL if it's not (or cannot be) part of any
 * partition tree.
 */
pub unsafe fn pg_partition_root(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let rootrelid: Oid;
    let ancestors: *mut List;

    if !check_rel_can_be_partition(relid) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* fetch the list of ancestors */
    ancestors = get_partition_ancestors(relid);

    /*
     * If the input relation is already the top-most parent, just return
     * itself.
     */
    if ancestors == NIL {
        PG_RETURN_OID!(relid);
    }

    rootrelid = llast_oid(ancestors);
    list_free(ancestors);

    /*
     * "rootrelid" must contain a valid OID, given that the input relation is
     * a valid partition tree member as checked above.
     */
    Assert!(OidIsValid(rootrelid));
    PG_RETURN_OID!(rootrelid)
}

/*
 * pg_partition_ancestors
 *
 * Produces a view with one row per ancestor of the given partition,
 * including the input relation itself.
 */
pub unsafe fn pg_partition_ancestors(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let funcctx: *mut FuncCallContext;
    let mut ancestors: *mut List;

    if SRF_IS_FIRSTCALL(fcinfo) {
        let oldcxt: MemoryContext;

        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);

        if !check_rel_can_be_partition(relid) {
            return SRF_RETURN_DONE(funcctx);
        }

        oldcxt = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        ancestors = get_partition_ancestors(relid);
        ancestors = lcons_oid(relid, ancestors);

        /* The only state we need is the ancestors list */
        (*funcctx).user_fctx = ancestors as *mut c_void;

        MemoryContextSwitchTo(oldcxt);
    }

    funcctx = SRF_PERCALL_SETUP(fcinfo);
    ancestors = (*funcctx).user_fctx as *mut List;

    if ((*funcctx).call_cntr as c_int) < list_length(ancestors) {
        let resultrel: Oid = list_nth_oid(ancestors, (*funcctx).call_cntr as c_int);

        return SRF_RETURN_NEXT(funcctx, ObjectIdGetDatum(resultrel));
    }

    SRF_RETURN_DONE(funcctx)
}

//! catalog/indexing.c - routines to support indexes defined on system catalogs.

use crate::prelude::*;

use crate::{makeNode, Assert};

use crate::access::htup_details::{HeapTuple, HeapTupleIsHeapOnly};
use crate::access::index::amapi::IndexUniqueCheck;
use crate::executor::execTuples::{
    ExecDropSingleTupleTableSlot, ExecFetchSlotHeapTuple, ExecStoreHeapTuple,
    MakeSingleTupleTableSlot,
};
use crate::executor::execTuples::TTSOpsHeapTuple;
use crate::executor::executor::{ExecCloseIndices, ExecOpenIndices};
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{IndexInfo, ResultRelInfo};
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::storage::itemptr::ItemPointer;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::palloc::pfree;
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetRelid, RelationPtr,
};

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
pub type CatalogIndexState = *mut ResultRelInfo;

/*
 * Cap the maximum amount of bytes allocated for multi-inserts with system
 * catalogs, limiting the number of slots used.
 */
pub const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

/*
 * TU_UpdateIndexes, used to control which indexes are updated.  Defined in
 * access/tableam.h in C; declared here as a local enum until that header is
 * ported.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TU_UpdateIndexes {
    /* No indexed columns were updated (incl. TID addressing of tuple) */
    TU_None,
    /* A non-summarizing indexed column was updated, or the TID has changed */
    TU_All,
    /* Only summarized columns were updated, TID is unchanged */
    TU_Summarizing,
}
pub use TU_UpdateIndexes::*;

/* index_insert's checkUnique argument values (genam.h). */
const UNIQUE_CHECK_NO: IndexUniqueCheck = 0;
const UNIQUE_CHECK_YES: IndexUniqueCheck = 1;

// ---------------------------------------------------------------------------
// Locally-stubbed callees (not yet ported elsewhere).
// ---------------------------------------------------------------------------

// TODO(pg-port): real `FormIndexDatum` in catalog/index.c.
unsafe fn FormIndexDatum(
    _indexInfo: *mut IndexInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut c_void,
    _values: *mut Datum,
    _isnull: *mut bool,
) { crate::catalog::index::FormIndexDatum(_indexInfo as _, _slot as _, _estate as _, _values, _isnull) }

// TODO(pg-port): real `index_insert` in access/index/indexam.c.
unsafe fn index_insert(
    _indexRelation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_t_ctid: *mut ItemPointerData,
    _heapRelation: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool { crate::access::index::indexam::index_insert(_indexRelation, _values, _isnull, _heap_t_ctid, _heapRelation, _checkUnique as _, _indexUnchanged, _indexInfo as _) }

// TODO(pg-port): real `simple_heap_insert` in access/heap/heapam.c.
unsafe fn simple_heap_insert(_relation: Relation, _tup: HeapTuple) -> Oid { crate::access::heap::heapam::simple_heap_insert(_relation, _tup); crate::postgres_ext::InvalidOid }

// TODO(pg-port): real `simple_heap_update` in access/heap/heapam.c.
unsafe fn simple_heap_update(
    _relation: Relation,
    _otid: ItemPointer,
    _tup: HeapTuple,
    _update_indexes: *mut TU_UpdateIndexes,
) { crate::access::heap::heapam::simple_heap_update(_relation, _otid, _tup, _update_indexes as _) }

// TODO(pg-port): real `simple_heap_delete` in access/heap/heapam.c.
unsafe fn simple_heap_delete(_relation: Relation, _tid: ItemPointer) { crate::access::heap::heapam::simple_heap_delete(_relation, _tid) }

// TODO(pg-port): real `heap_multi_insert` in access/heap/heapam.c.
unsafe fn heap_multi_insert(
    _relation: Relation,
    _slots: *mut *mut TupleTableSlot,
    _ntuples: c_int,
    _cid: CommandId,
    _options: c_int,
    _bistate: *mut c_void,
) { crate::access::heap::heapam::heap_multi_insert(_relation, _slots, _ntuples, _cid, _options, _bistate as _) }

// TODO(pg-port): real `heap_freetuple` is ported in access/common/heaptuple.rs,
// but to avoid a cross-module cycle issue use the canonical one.
use crate::access::common::heaptuple::heap_freetuple;

// TODO(pg-port): real `GetCurrentCommandId` in access/transam/xact.c.
unsafe fn GetCurrentCommandId(_used: bool) -> CommandId { crate::access::transam::xact::GetCurrentCommandId(_used) }

/*
 * CatalogOpenIndexes - open the indexes on a system catalog.
 *
 * When inserting or updating tuples in a system catalog, call this
 * to prepare to update the indexes for the catalog.
 *
 * In the current implementation, we share code for opening/closing the
 * indexes with execUtils.c.  But we do not use ExecInsertIndexTuples,
 * because we don't want to create an EState.  This implies that we
 * do not support partial or expressional indexes on system catalogs,
 * nor can we support generalized exclusion constraints.
 * This could be fixed with localized changes here if we wanted to pay
 * the extra overhead of building an EState.
 */
pub unsafe fn CatalogOpenIndexes(heapRel: Relation) -> CatalogIndexState {
    let resultRelInfo: *mut ResultRelInfo;

    resultRelInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);
    (*resultRelInfo).ri_RangeTableIndex = 0; /* dummy */
    (*resultRelInfo).ri_RelationDesc = heapRel;
    (*resultRelInfo).ri_TrigDesc = null_mut(); /* we don't fire triggers */

    ExecOpenIndices(resultRelInfo, false);

    resultRelInfo
}

/*
 * CatalogCloseIndexes - clean up resources allocated by CatalogOpenIndexes
 */
pub unsafe fn CatalogCloseIndexes(indstate: CatalogIndexState) {
    ExecCloseIndices(indstate);
    pfree(indstate as *mut c_void);
}

/*
 * CatalogIndexInsert - insert index entries for one catalog tuple
 *
 * This should be called for each inserted or updated catalog tuple.
 *
 * This is effectively a cut-down version of ExecInsertIndexTuples.
 */
unsafe fn CatalogIndexInsert(
    indstate: CatalogIndexState,
    heapTuple: HeapTuple,
    updateIndexes: TU_UpdateIndexes,
) {
    let numIndexes: c_int;
    let relationDescs: RelationPtr;
    let heapRelation: Relation;
    let slot: *mut TupleTableSlot;
    let indexInfoArray: *mut *mut IndexInfo;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let onlySummarized: bool = updateIndexes == TU_Summarizing;

    /*
     * HOT update does not require index inserts. But with asserts enabled we
     * want to check that it'd be legal to currently insert into the
     * table/index.
     */
    #[cfg(not(debug_assertions))]
    {
        if HeapTupleIsHeapOnly(heapTuple) && !onlySummarized {
            return;
        }
    }

    /* When only updating summarized indexes, the tuple has to be HOT. */
    Assert!((!onlySummarized) || HeapTupleIsHeapOnly(heapTuple));

    /*
     * Get information from the state structure.  Fall out if nothing to do.
     */
    numIndexes = (*indstate).ri_NumIndices;
    if numIndexes == 0 {
        return;
    }
    relationDescs = (*indstate).ri_IndexRelationDescs;
    indexInfoArray = (*indstate).ri_IndexRelationInfo;
    heapRelation = (*indstate).ri_RelationDesc;

    /* Need a slot to hold the tuple being examined */
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), &TTSOpsHeapTuple);
    ExecStoreHeapTuple(heapTuple, slot, false);

    /*
     * for each index, form and insert the index tuple
     */
    let mut i: c_int = 0;
    while i < numIndexes {
        let indexInfo: *mut IndexInfo;
        let index: Relation;

        indexInfo = *indexInfoArray.add(i as usize);
        index = *relationDescs.add(i as usize);

        /* If the index is marked as read-only, ignore it */
        if !(*indexInfo).ii_ReadyForInserts {
            i += 1;
            continue;
        }

        /*
         * Expressional and partial indexes on system catalogs are not
         * supported, nor exclusion constraints, nor deferred uniqueness
         */
        Assert!((*indexInfo).ii_Expressions.is_null());
        Assert!((*indexInfo).ii_Predicate.is_null());
        Assert!((*indexInfo).ii_ExclusionOps.is_null());
        Assert!((*(*index).rd_index).indimmediate);
        Assert!((*indexInfo).ii_NumIndexKeyAttrs != 0);

        /* see earlier check above */
        #[cfg(debug_assertions)]
        {
            if HeapTupleIsHeapOnly(heapTuple) && !onlySummarized {
                Assert!(!ReindexIsProcessingIndex(RelationGetRelid(index)));
                i += 1;
                continue;
            }
        }

        /*
         * Skip insertions into non-summarizing indexes if we only need to
         * update summarizing indexes.
         */
        if onlySummarized && !(*indexInfo).ii_Summarizing {
            i += 1;
            continue;
        }

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(
            indexInfo,
            slot,
            null_mut(), /* no expression eval to do */
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        if std::env::var("PDB_BT").is_ok() && (*(*index).rd_rel).oid == 2663 {
            let nm = if values[0] != 0 { std::ffi::CStr::from_ptr(DatumGetPointer(values[0]) as *const c_char).to_string_lossy().into_owned() } else { String::from("<null>") };
            eprintln!("PDB_BT CatalogIndexInsert idx=2663 slot_relname='{}' val1={:#x}", nm, values[1]);
        }

        /*
         * The index AM does the rest.
         */
        index_insert(
            index,                  /* index relation */
            values.as_mut_ptr(),    /* array of index Datums */
            isnull.as_mut_ptr(),    /* is-null flags */
            &mut (*heapTuple).t_self, /* tid of heap tuple */
            heapRelation,
            if (*(*index).rd_index).indisunique {
                UNIQUE_CHECK_YES
            } else {
                UNIQUE_CHECK_NO
            },
            false,
            indexInfo,
        );

        i += 1;
    }

    ExecDropSingleTupleTableSlot(slot);
}

/*
 * Subroutine to verify that catalog constraints are honored.
 *
 * Tuples inserted via CatalogTupleInsert/CatalogTupleUpdate are generally
 * "hand made", so that it's possible that they fail to satisfy constraints
 * that would be checked if they were being inserted by the executor.  That's
 * a coding error, so we only bother to check for it in assert-enabled builds.
 */
#[cfg(debug_assertions)]
unsafe fn CatalogTupleCheckConstraints(heapRel: Relation, tup: HeapTuple) {
    use crate::access::htup_details::HeapTupleHasNulls;
    use crate::access::tupmacs::att_isnull;
    use crate::access::common::tupdesc::TupleDescAttr;

    /*
     * Currently, the only constraints implemented for system catalogs are
     * attnotnull constraints.
     */
    if HeapTupleHasNulls(tup) {
        let tupdesc = RelationGetDescr(heapRel);
        let bp = (*(*tup).t_data).t_bits.as_mut_ptr();

        let mut attnum: c_int = 0;
        while attnum < (*tupdesc).natts {
            let thisatt = TupleDescAttr(tupdesc, attnum);

            Assert!(!((*thisatt).attnotnull && att_isnull(attnum, bp)));
            attnum += 1;
        }
    }
}

#[cfg(not(debug_assertions))]
#[inline]
unsafe fn CatalogTupleCheckConstraints(_heapRel: Relation, _tup: HeapTuple) {
    /* (void) 0 */
}

/*
 * CatalogTupleInsert - do heap and indexing work for a new catalog tuple
 *
 * Insert the tuple data in "tup" into the specified catalog relation.
 *
 * This is a convenience routine for the common case of inserting a single
 * tuple in a system catalog; it inserts a new heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleInsertWithInfo in such cases.)
 */
pub unsafe fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple) {
    let indstate: CatalogIndexState;

    CatalogTupleCheckConstraints(heapRel, tup);

    indstate = CatalogOpenIndexes(heapRel);

    simple_heap_insert(heapRel, tup);

    CatalogIndexInsert(indstate, tup, TU_All);
    CatalogCloseIndexes(indstate);
}

/*
 * CatalogTupleInsertWithInfo - as above, but with caller-supplied index info
 *
 * This should be used when it's important to amortize CatalogOpenIndexes/
 * CatalogCloseIndexes work across multiple insertions.  At some point we
 * might cache the CatalogIndexState data somewhere (perhaps in the relcache)
 * so that callers needn't trouble over this ... but we don't do so today.
 */
pub unsafe fn CatalogTupleInsertWithInfo(
    heapRel: Relation,
    tup: HeapTuple,
    indstate: CatalogIndexState,
) {
    CatalogTupleCheckConstraints(heapRel, tup);

    simple_heap_insert(heapRel, tup);

    CatalogIndexInsert(indstate, tup, TU_All);
}

/*
 * CatalogTuplesMultiInsertWithInfo - as above, but for multiple tuples
 *
 * Insert multiple tuples into the given catalog relation at once, with an
 * amortized cost of CatalogOpenIndexes.
 */
pub unsafe fn CatalogTuplesMultiInsertWithInfo(
    heapRel: Relation,
    slot: *mut *mut TupleTableSlot,
    ntuples: c_int,
    indstate: CatalogIndexState,
) {
    /* Nothing to do */
    if ntuples <= 0 {
        return;
    }

    heap_multi_insert(
        heapRel,
        slot,
        ntuples,
        GetCurrentCommandId(true),
        0,
        null_mut(),
    );

    /*
     * There is no equivalent to heap_multi_insert for the catalog indexes, so
     * we must loop over and insert individually.
     */
    let mut i: c_int = 0;
    while i < ntuples {
        let mut should_free: bool = false;
        let tuple: HeapTuple;

        tuple = ExecFetchSlotHeapTuple(*slot.add(i as usize), true, &mut should_free);
        (*tuple).t_tableOid = (**slot.add(i as usize)).tts_tableOid;
        CatalogIndexInsert(indstate, tuple, TU_All);

        if should_free {
            heap_freetuple(tuple);
        }

        i += 1;
    }
}

/*
 * CatalogTupleUpdate - do heap and indexing work for updating a catalog tuple
 *
 * Update the tuple identified by "otid", replacing it with the data in "tup".
 *
 * This is a convenience routine for the common case of updating a single
 * tuple in a system catalog; it updates one heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleUpdateWithInfo in such cases.)
 */
#[no_mangle]
pub unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: ItemPointer, tup: HeapTuple) {
    let indstate: CatalogIndexState;
    let mut updateIndexes: TU_UpdateIndexes = TU_All;

    CatalogTupleCheckConstraints(heapRel, tup);

    indstate = CatalogOpenIndexes(heapRel);

    simple_heap_update(heapRel, otid, tup, &mut updateIndexes);

    CatalogIndexInsert(indstate, tup, updateIndexes);
    CatalogCloseIndexes(indstate);
}

/*
 * CatalogTupleUpdateWithInfo - as above, but with caller-supplied index info
 *
 * This should be used when it's important to amortize CatalogOpenIndexes/
 * CatalogCloseIndexes work across multiple updates.  At some point we
 * might cache the CatalogIndexState data somewhere (perhaps in the relcache)
 * so that callers needn't trouble over this ... but we don't do so today.
 */
pub unsafe fn CatalogTupleUpdateWithInfo(
    heapRel: Relation,
    otid: ItemPointer,
    tup: HeapTuple,
    indstate: CatalogIndexState,
) {
    let mut updateIndexes: TU_UpdateIndexes = TU_All;

    CatalogTupleCheckConstraints(heapRel, tup);

    simple_heap_update(heapRel, otid, tup, &mut updateIndexes);

    CatalogIndexInsert(indstate, tup, updateIndexes);
}

/*
 * CatalogTupleDelete - do heap and indexing work for deleting a catalog tuple
 *
 * Delete the tuple identified by "tid" in the specified catalog.
 *
 * With Postgres heaps, there is no index work to do at deletion time;
 * cleanup will be done later by VACUUM.  However, callers of this function
 * shouldn't have to know that; we'd like a uniform abstraction for all
 * catalog tuple changes.  Hence, provide this currently-trivial wrapper.
 *
 * The abstraction is a bit leaky in that we don't provide an optimized
 * CatalogTupleDeleteWithInfo version, because there is currently nothing to
 * optimize.  If we ever need that, rather than touching a lot of call sites,
 * it might be better to do something about caching CatalogIndexState.
 */
#[no_mangle]
pub unsafe fn CatalogTupleDelete(heapRel: Relation, tid: ItemPointer) {
    simple_heap_delete(heapRel, tid);
}

pub use crate::catalog::index::ReindexIsProcessingIndex;

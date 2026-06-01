//! Routines to support index-only scans
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translated 1:1 from:
//!   postgres/src/backend/executor/nodeIndexonlyscan.c
//!   postgres/src/include/executor/nodeIndexonlyscan.h
//!
//! INTERFACE ROUTINES
//!     ExecIndexOnlyScan           scans an index
//!     IndexOnlyNext               retrieve next tuple
//!     ExecInitIndexOnlyScan       creates and initializes state info.
//!     ExecReScanIndexOnlyScan     rescans the indexed relation.
//!     ExecEndIndexOnlyScan        releases all storage.
//!     ExecIndexOnlyMarkPos        marks scan position.
//!     ExecIndexOnlyRestrPos       restores scan position.
//!     ExecIndexOnlyScanEstimate   estimates DSM space needed for
//!                     parallel index-only scan
//!     ExecIndexOnlyScanInitializeDSM  initialize DSM for parallel
//!                     index-only scan
//!     ExecIndexOnlyScanReInitializeDSM    reinitialize DSM for fresh scan
//!     ExecIndexOnlyScanInitializeWorker attach to DSM info in parallel worker

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::mem::size_of;
use std::ptr;

use crate::{castNode, makeNode, Assert};

use crate::access::attnum::AttrNumber;
use crate::access::sdir::{ScanDirection, ScanDirectionCombine};

use crate::executor::executor::{
    exec_rt_fetch, ExecAssignExprContext, ExecAssignScanProjectionInfoWithVarno,     ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot, ExecOpenScanRelation, ExecQualAndReset,
    ExecReScan, ExecScan, ExecScanReScan, ExecScanAccessMtd, ExecScanRecheckMtd, ExecTypeFromTL,
    EXEC_FLAG_EXPLAIN_ONLY,
};
use crate::executor::execTuples::{ExecAllocTableSlot, ExecStoreVirtualTuple, TTSOpsVirtual, ExecForceStoreHeapTuple};
use crate::executor::execUtils::ResetExprContext;
use crate::executor::tuptable::ExecClearTuple;

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::nodes::execnodes::{
    EState, EPQState, ExprContext, IndexArrayKeyInfo, IndexOnlyScanState, IndexRuntimeKeyInfo,
    IndexScanDescData, IndexScanInstrumentation, PlanState, ScanKeyData, ScanState,
    SharedIndexScanInstrumentation, TupleTableSlot,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::plannodes::{IndexOnlyScan, Plan, Scan};
use crate::nodes::primnodes::INDEX_VAR;

use crate::access::common::tupdesc::TupleDesc;
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::itemptr::{ItemPointer, ItemPointerGetBlockNumber};
use crate::storage::lockdefs::{NoLock, LOCKMODE};

use crate::pg_config_manual::NAMEDATALEN;
use crate::c::Name;
use crate::utils::builtins::namestrcpy;
use crate::utils::rel::Relation;

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type IndexScanDesc = *mut IndexScanDescData;
type ParallelContext = c_void;
type ParallelWorkerContext = c_void;
type ParallelIndexScanDesc = *mut c_void;
type IndexTuple = *mut c_void;
type BlockNumber = u32;

// Well-known catalog type OIDs (pg_type.h).
const CSTRINGOID: Oid = 2275;
const NAMEOID: Oid = 19;

// ----------------------------------------------------------------
// Local stubs for unported helper functions / accessors we call.
// ----------------------------------------------------------------

unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_beginscan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _instrument: *mut IndexScanInstrumentation,
    _nkeys: c_int,
    _norderbys: c_int,
) -> IndexScanDesc {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_beginscan_parallel(
    _heaprel: Relation,
    _indexrel: Relation,
    _instrument: *mut IndexScanInstrumentation,
    _nkeys: c_int,
    _norderbys: c_int,
    _pscan: ParallelIndexScanDesc,
) -> IndexScanDesc {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_rescan(
    _scan: IndexScanDesc,
    _keys: *mut ScanKeyData,
    _nkeys: c_int,
    _orderbys: *mut ScanKeyData,
    _norderbys: c_int,
) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_endscan(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_markpos(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_restrpos(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_getnext_tid(_scan: IndexScanDesc, _direction: ScanDirection) -> ItemPointer {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_fetch_heap(_scan: IndexScanDesc, _slot: *mut TupleTableSlot) -> bool {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_deform_tuple(
    _tup: IndexTuple,
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: access/itup.h (indextuple.c)
}

unsafe fn index_parallelscan_estimate(
    _indexRelation: Relation,
    _nkeys: c_int,
    _norderbys: c_int,
    _snapshot: *mut c_void,
    _instrument: bool,
    _parallel_aware: bool,
    _nworkers: c_int,
) -> Size {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_parallelscan_initialize(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _instrument: bool,
    _parallel_aware: bool,
    _nworkers: c_int,
    _sharedInfo: *mut *mut SharedIndexScanInstrumentation,
    _target: ParallelIndexScanDesc,
) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn index_parallelrescan(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/genam.h (indexam.c)
}

unsafe fn ExecIndexBuildScanKeys(
    _planstate: *mut PlanState,
    _index: Relation,
    _quals: *mut crate::nodes::pg_list::List,
    _isorderby: bool,
    _scanKeys: *mut *mut ScanKeyData,
    _numScanKeys: *mut c_int,
    _runtimeKeys: *mut *mut IndexRuntimeKeyInfo,
    _numRuntimeKeys: *mut c_int,
    _arrayKeys: *mut *mut IndexArrayKeyInfo,
    _numArrayKeys: *mut c_int,
) {
    unimplemented!() // TODO: executor/nodeIndexscan.c
}

unsafe fn ExecIndexEvalRuntimeKeys(
    _econtext: *mut ExprContext,
    _runtimeKeys: *mut IndexRuntimeKeyInfo,
    _numRuntimeKeys: c_int,
) {
    unimplemented!() // TODO: executor/nodeIndexscan.c
}

unsafe fn RelationGetDescr(_relation: Relation) -> TupleDesc {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn table_slot_callbacks(_relation: Relation) -> *const crate::executor::tuptable::TupleTableSlotOps {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn PredicateLockPage(_relation: Relation, _blkno: BlockNumber, _snapshot: *mut c_void) {
    unimplemented!() // TODO: storage/predicate.h
}

unsafe fn IsParallelWorker() -> bool {
    unimplemented!() // TODO: miscadmin.h
}

unsafe fn shm_toc_estimate_chunk(_estimator: *mut c_void, _size: Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_estimate_keys(_estimator: *mut c_void, _nkeys: Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_allocate(_toc: *mut c_void, _nbytes: Size) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_insert(_toc: *mut c_void, _key: u64, _address: *mut c_void) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_lookup(_toc: *mut c_void, _key: u64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}

// access/visibilitymap.h: VM_ALL_VISIBLE() macro.
unsafe fn VM_ALL_VISIBLE(_rel: Relation, _heapBlk: BlockNumber, _vmbuf: *mut Buffer) -> bool {
    unimplemented!() // TODO: access/visibilitymap.h
}

// executor/instrument.h: InstrCountTuples2()/InstrCountFiltered2() macros.
unsafe fn InstrCountTuples2(_node: *mut IndexOnlyScanState, _delta: u64) {
    unimplemented!() // TODO: executor/instrument.h
}

unsafe fn InstrCountFiltered2(_node: *mut IndexOnlyScanState, _delta: u64) {
    unimplemented!() // TODO: executor/instrument.h
}

// postgres.h: NameGetDatum() macro.
unsafe fn NameGetDatum(_X: Name) -> Datum {
    unimplemented!() // TODO: postgres.h (c.rs)
}

// utils/memutils.h: OffsetToPointer().
unsafe fn OffsetToPointer(_base: *mut c_void, _offset: usize) -> *mut c_void {
    unimplemented!() // TODO: utils/memutils.h
}

// ----------------------------------------------------------------
// Opaque IndexScanDescData field accessors.
//
// IndexScanDescData is presently an opaque stub; these helpers stand in for
// the direct struct member references in the C source until access/relscan.h
// is ported.
// ----------------------------------------------------------------

unsafe fn idxsd_set_want_itup(_scan: IndexScanDesc, _v: bool) {
    unimplemented!() // TODO: access/relscan.h (scan->xs_want_itup)
}

unsafe fn idxsd_heapRelation(_scan: IndexScanDesc) -> Relation {
    unimplemented!() // TODO: access/relscan.h (scan->heapRelation)
}

unsafe fn idxsd_xs_heap_continue(_scan: IndexScanDesc) -> bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_heap_continue)
}

unsafe fn idxsd_xs_hitup(_scan: IndexScanDesc) -> *mut c_void /* HeapTuple */ {
    unimplemented!() // TODO: access/relscan.h (scan->xs_hitup)
}

unsafe fn idxsd_xs_hitupdesc(_scan: IndexScanDesc) -> TupleDesc {
    unimplemented!() // TODO: access/relscan.h (scan->xs_hitupdesc)
}

unsafe fn idxsd_xs_itup(_scan: IndexScanDesc) -> IndexTuple {
    unimplemented!() // TODO: access/relscan.h (scan->xs_itup)
}

unsafe fn idxsd_xs_itupdesc(_scan: IndexScanDesc) -> TupleDesc {
    unimplemented!() // TODO: access/relscan.h (scan->xs_itupdesc)
}

unsafe fn idxsd_xs_recheck(_scan: IndexScanDesc) -> bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_recheck)
}

unsafe fn idxsd_numberOfOrderBys(_scan: IndexScanDesc) -> c_int {
    unimplemented!() // TODO: access/relscan.h (scan->numberOfOrderBys)
}

unsafe fn idxsd_xs_recheckorderby(_scan: IndexScanDesc) -> bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_recheckorderby)
}

// ----------------------------------------------------------------
// Opaque tupdesc / SharedInfo / instrumentation accessors.
// ----------------------------------------------------------------

unsafe fn tupdesc_natts(_desc: TupleDesc) -> c_int {
    unimplemented!() // TODO: access/tupdesc.h (desc->natts)
}

// ----------------------------------------------------------------
// Opaque Relation accessors needed by ExecInitIndexOnlyScan.
// ----------------------------------------------------------------

unsafe fn rel_indnkeyatts(_index: Relation) -> c_int {
    unimplemented!() // TODO: utils/rel.h (index->rd_index->indnkeyatts)
}

unsafe fn rel_attr_atttypid(_index: Relation, _attnum: c_int) -> Oid {
    unimplemented!() // TODO: access/tupdesc.h (TupleDescAttr(index->rd_att, attnum)->atttypid)
}

unsafe fn rel_opcintype(_index: Relation, _attnum: c_int) -> Oid {
    unimplemented!() // TODO: utils/rel.h (index->rd_opcintype[attnum])
}

// global stub (extern global in C; miscadmin.h).
#[allow(non_upper_case_globals)]
static mut ParallelWorkerNumber: c_int = 0;

/* ----------------------------------------------------------------
 *		IndexOnlyNext
 *
 *		Retrieve a tuple from the IndexOnlyScan node's index.
 * ----------------------------------------------------------------
 */
unsafe fn IndexOnlyNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut IndexOnlyScanState;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let direction: ScanDirection;
    let mut scandesc: IndexScanDesc;
    let slot: *mut TupleTableSlot;
    let mut tid: ItemPointer;

    /*
     * extract necessary information from index scan node
     */
    estate = (*node).ss.ps.state;

    /*
     * Determine which direction to scan the index in based on the plan's scan
     * direction and the current direction of execution.
     */
    direction = ScanDirectionCombine(
        (*estate).es_direction,
        (*((*node).ss.ps.plan as *mut IndexOnlyScan)).indexorderdir,
    );
    scandesc = (*node).ioss_ScanDesc;
    econtext = (*node).ss.ps.ps_ExprContext;
    slot = (*node).ss.ss_ScanTupleSlot;

    if scandesc.is_null() {
        /*
         * We reach here if the index only scan is not parallel, or if we're
         * serially executing an index only scan that was planned to be
         * parallel.
         */
        scandesc = index_beginscan(
            (*node).ss.ss_currentRelation,
            (*node).ioss_RelationDesc,
            (*estate).es_snapshot as *mut c_void,
            &raw mut (*node).ioss_Instrument,
            (*node).ioss_NumScanKeys,
            (*node).ioss_NumOrderByKeys,
        );

        (*node).ioss_ScanDesc = scandesc;

        /* Set it up for index-only scan */
        idxsd_set_want_itup((*node).ioss_ScanDesc, true);
        (*node).ioss_VMBuffer = InvalidBuffer;

        /*
         * If no run-time keys to calculate or they are ready, go ahead and
         * pass the scankeys to the index AM.
         */
        if (*node).ioss_NumRuntimeKeys == 0 || (*node).ioss_RuntimeKeysReady {
            index_rescan(
                scandesc,
                (*node).ioss_ScanKeys,
                (*node).ioss_NumScanKeys,
                (*node).ioss_OrderByKeys,
                (*node).ioss_NumOrderByKeys,
            );
        }
    }

    /*
     * OK, now that we have what we need, fetch the next tuple.
     */
    loop {
        tid = index_getnext_tid(scandesc, direction);
        if tid.is_null() {
            break;
        }

        let mut tuple_from_heap: bool = false;

        CHECK_FOR_INTERRUPTS();

        /*
         * We can skip the heap fetch if the TID references a heap page on
         * which all tuples are known visible to everybody.  In any case,
         * we'll use the index tuple not the heap tuple as the data source.
         *
         * Note on Memory Ordering Effects: visibilitymap_get_status does not
         * lock the visibility map buffer, and therefore the result we read
         * here could be slightly stale.  However, it can't be stale enough to
         * matter.
         *
         * We need to detect clearing a VM bit due to an insert right away,
         * because the tuple is present in the index page but not visible. The
         * reading of the TID by this scan (using a shared lock on the index
         * buffer) is serialized with the insert of the TID into the index
         * (using an exclusive lock on the index buffer). Because the VM bit
         * is cleared before updating the index, and locking/unlocking of the
         * index page acts as a full memory barrier, we are sure to see the
         * cleared bit if we see a recently-inserted TID.
         *
         * Deletes do not update the index page (only VACUUM will clear out
         * the TID), so the clearing of the VM bit by a delete is not
         * serialized with this test below, and we may see a value that is
         * significantly stale. However, we don't care about the delete right
         * away, because the tuple is still visible until the deleting
         * transaction commits or the statement ends (if it's our
         * transaction). In either case, the lock on the VM buffer will have
         * been released (acting as a write barrier) after clearing the bit.
         * And for us to have a snapshot that includes the deleting
         * transaction (making the tuple invisible), we must have acquired
         * ProcArrayLock after that time, acting as a read barrier.
         *
         * It's worth going through this complexity to avoid needing to lock
         * the VM buffer, which could cause significant contention.
         */
        if !VM_ALL_VISIBLE(
            idxsd_heapRelation(scandesc),
            ItemPointerGetBlockNumber(tid),
            &raw mut (*node).ioss_VMBuffer,
        ) {
            /*
             * Rats, we have to visit the heap to check visibility.
             */
            InstrCountTuples2(node, 1);
            if !index_fetch_heap(scandesc, (*node).ioss_TableSlot) {
                continue; /* no visible tuple, try next index entry */
            }

            ExecClearTuple((*node).ioss_TableSlot);

            /*
             * Only MVCC snapshots are supported here, so there should be no
             * need to keep following the HOT chain once a visible entry has
             * been found.  If we did want to allow that, we'd need to keep
             * more state to remember not to call index_getnext_tid next time.
             */
            if idxsd_xs_heap_continue(scandesc) {
                elog!(
                    ERROR,
                    "non-MVCC snapshots are not supported in index-only scans"
                );
            }

            /*
             * Note: at this point we are holding a pin on the heap page, as
             * recorded in scandesc->xs_cbuf.  We could release that pin now,
             * but it's not clear whether it's a win to do so.  The next index
             * entry might require a visit to the same heap page.
             */

            tuple_from_heap = true;
        }

        /*
         * Fill the scan tuple slot with data from the index.  This might be
         * provided in either HeapTuple or IndexTuple format.  Conceivably an
         * index AM might fill both fields, in which case we prefer the heap
         * format, since it's probably a bit cheaper to fill a slot from.
         */
        if !idxsd_xs_hitup(scandesc).is_null() {
            /*
             * We don't take the trouble to verify that the provided tuple has
             * exactly the slot's format, but it seems worth doing a quick
             * check on the number of fields.
             */
            Assert!(tupdesc_natts((*slot).tts_tupleDescriptor) == tupdesc_natts(idxsd_xs_hitupdesc(scandesc)));
            ExecForceStoreHeapTuple(idxsd_xs_hitup(scandesc) as crate::access::htup_details::HeapTuple, slot, false);
        } else if !idxsd_xs_itup(scandesc).is_null() {
            StoreIndexTuple(node, slot, idxsd_xs_itup(scandesc), idxsd_xs_itupdesc(scandesc));
        } else {
            elog!(ERROR, "no data returned for index-only scan");
        }

        /*
         * If the index was lossy, we have to recheck the index quals.
         */
        if idxsd_xs_recheck(scandesc) {
            (*econtext).ecxt_scantuple = slot;
            if !ExecQualAndReset((*node).recheckqual, econtext) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        /*
         * We don't currently support rechecking ORDER BY distances.  (In
         * principle, if the index can support retrieval of the originally
         * indexed value, it should be able to produce an exact distance
         * calculation too.  So it's not clear that adding code here for
         * recheck/re-sort would be worth the trouble.  But we should at least
         * throw an error if someone tries it.)
         */
        if idxsd_numberOfOrderBys(scandesc) > 0 && idxsd_xs_recheckorderby(scandesc) {
            ereport!(
                ERROR,
                "lossy distance functions are not supported in index-only scans"
            );
        }

        /*
         * If we didn't access the heap, then we'll need to take a predicate
         * lock explicitly, as if we had.  For now we do that at page level.
         */
        if !tuple_from_heap {
            PredicateLockPage(
                idxsd_heapRelation(scandesc),
                ItemPointerGetBlockNumber(tid),
                (*estate).es_snapshot as *mut c_void,
            );
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    ExecClearTuple(slot)
}

/*
 * StoreIndexTuple
 *		Fill the slot with data from the index tuple.
 *
 * At some point this might be generally-useful functionality, but
 * right now we don't need it elsewhere.
 */
unsafe fn StoreIndexTuple(
    node: *mut IndexOnlyScanState,
    slot: *mut TupleTableSlot,
    itup: IndexTuple,
    itupdesc: TupleDesc,
) {
    /*
     * Note: we must use the tupdesc supplied by the AM in index_deform_tuple,
     * not the slot's tupdesc, in case the latter has different datatypes
     * (this happens for btree name_ops in particular).  They'd better have
     * the same number of columns though, as well as being datatype-compatible
     * which is something we can't so easily check.
     */
    Assert!(tupdesc_natts((*slot).tts_tupleDescriptor) == tupdesc_natts(itupdesc));

    ExecClearTuple(slot);
    index_deform_tuple(itup, itupdesc, (*slot).tts_values, (*slot).tts_isnull);

    /*
     * Copy all name columns stored as cstrings back into a NAMEDATALEN byte
     * sized allocation.  We mark this branch as unlikely as generally "name"
     * is used only for the system catalogs and this would have to be a user
     * query running on those or some other user table with an index on a name
     * column.
     */
    if !(*node).ioss_NameCStringAttNums.is_null() {
        let attcount: c_int = (*node).ioss_NameCStringCount;

        for idx in 0..attcount {
            let attnum: c_int = *(*node).ioss_NameCStringAttNums.offset(idx as isize) as c_int;
            let name: Name;

            /* skip null Datums */
            if *(*slot).tts_isnull.offset(attnum as isize) {
                continue;
            }

            /* allocate the NAMEDATALEN and copy the datum into that memory */
            name = MemoryContextAlloc(
                (*(*node).ss.ps.ps_ExprContext).ecxt_per_tuple_memory,
                NAMEDATALEN as Size,
            ) as Name;

            /* use namestrcpy to zero-pad all trailing bytes */
            namestrcpy(name, DatumGetCString(*(*slot).tts_values.offset(attnum as isize)));
            *(*slot).tts_values.offset(attnum as isize) = NameGetDatum(name);
        }
    }

    ExecStoreVirtualTuple(slot);
}

/*
 * IndexOnlyRecheck -- access method routine to recheck a tuple in EvalPlanQual
 *
 * This can't really happen, since an index can't supply CTID which would
 * be necessary data for any potential EvalPlanQual target relation.  If it
 * did happen, the EPQ code would pass us the wrong data, namely a heap
 * tuple not an index tuple.  So throw an error.
 */
unsafe fn IndexOnlyRecheck(_node: *mut ScanState, _slot: *mut TupleTableSlot) -> bool {
    elog!(
        ERROR,
        "EvalPlanQual recheck is not supported in index-only scans"
    );
    #[allow(unreachable_code)]
    false /* keep compiler quiet */
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScan(node)
 * ----------------------------------------------------------------
 */
unsafe fn ExecIndexOnlyScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut IndexOnlyScanState = castNode!(IndexOnlyScanState, T_IndexOnlyScanState, pstate);

    /*
     * If we have runtime keys and they've not already been set up, do it now.
     */
    if (*node).ioss_NumRuntimeKeys != 0 && !(*node).ioss_RuntimeKeysReady {
        ExecReScan(node as *mut PlanState);
    }

    ExecScan(&mut (*node).ss, Some(IndexOnlyNext), Some(IndexOnlyRecheck))
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexOnlyScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanIndexOnlyScan(node: *mut IndexOnlyScanState) {
    /*
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     */
    if (*node).ioss_NumRuntimeKeys != 0 {
        let econtext: *mut ExprContext = (*node).ioss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(
            econtext,
            (*node).ioss_RuntimeKeys,
            (*node).ioss_NumRuntimeKeys,
        );
    }
    (*node).ioss_RuntimeKeysReady = true;

    /* reset index scan */
    if !(*node).ioss_ScanDesc.is_null() {
        index_rescan(
            (*node).ioss_ScanDesc,
            (*node).ioss_ScanKeys,
            (*node).ioss_NumScanKeys,
            (*node).ioss_OrderByKeys,
            (*node).ioss_NumOrderByKeys,
        );
    }

    ExecScanReScan(&mut (*node).ss);
}

/* ----------------------------------------------------------------
 *		ExecEndIndexOnlyScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndIndexOnlyScan(node: *mut IndexOnlyScanState) {
    let indexRelationDesc: Relation;
    let indexScanDesc: IndexScanDesc;

    /*
     * extract information from the node
     */
    indexRelationDesc = (*node).ioss_RelationDesc;
    indexScanDesc = (*node).ioss_ScanDesc;

    /* Release VM buffer pin, if any. */
    if (*node).ioss_VMBuffer != InvalidBuffer {
        ReleaseBuffer((*node).ioss_VMBuffer);
        (*node).ioss_VMBuffer = InvalidBuffer;
    }

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory so that it can be picked up by the main
     * process to report in EXPLAIN ANALYZE
     */
    if !(*node).ioss_SharedInfo.is_null() && IsParallelWorker() {
        let winstrument: *mut IndexScanInstrumentation;

        Assert!(ParallelWorkerNumber <= si_num_workers((*node).ioss_SharedInfo));
        winstrument = si_winstrument((*node).ioss_SharedInfo, ParallelWorkerNumber);

        /*
         * We have to accumulate the stats rather than performing a memcpy.
         * When a Gather/GatherMerge node finishes it will perform planner
         * shutdown on the workers.  On rescan it will spin up new workers
         * which will have a new IndexOnlyScanState and zeroed stats.
         */
        instr_add_nsearches(winstrument, instr_nsearches(&raw mut (*node).ioss_Instrument));
    }

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if !indexScanDesc.is_null() {
        index_endscan(indexScanDesc);
    }
    if !indexRelationDesc.is_null() {
        index_close(indexRelationDesc, NoLock);
    }
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyMarkPos
 *
 * Note: we assume that no caller attempts to set a mark before having read
 * at least one tuple.  Otherwise, ioss_ScanDesc might still be NULL.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyMarkPos(node: *mut IndexOnlyScanState) {
    let estate: *mut EState = (*node).ss.ps.state;
    let epqstate: *mut EPQState = (*estate).es_epq_active;

    if !epqstate.is_null() {
        /*
         * We are inside an EvalPlanQual recheck.  If a test tuple exists for
         * this relation, then we shouldn't access the index at all.  We would
         * instead need to save, and later restore, the state of the
         * relsubs_done flag, so that re-fetching the test tuple is possible.
         * However, given the assumption that no caller sets a mark at the
         * start of the scan, we can only get here with relsubs_done[i]
         * already set, and so no state need be saved.
         */
        let scanrelid: crate::c::Index = (*((*node).ss.ps.plan as *mut Scan)).scanrelid;

        Assert!(scanrelid > 0);
        if !(*epqstate).relsubs_slot.offset((scanrelid - 1) as isize).read().is_null()
            || !(*epqstate).relsubs_rowmark.offset((scanrelid - 1) as isize).read().is_null()
        {
            /* Verify the claim above */
            if !*(*epqstate).relsubs_done.offset((scanrelid - 1) as isize) {
                elog!(ERROR, "unexpected ExecIndexOnlyMarkPos call in EPQ recheck");
            }
            return;
        }
    }

    index_markpos((*node).ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyRestrPos
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyRestrPos(node: *mut IndexOnlyScanState) {
    let estate: *mut EState = (*node).ss.ps.state;
    let epqstate: *mut EPQState = (*estate).es_epq_active;

    if !(*estate).es_epq_active.is_null() {
        /* See comments in ExecIndexMarkPos */
        let scanrelid: crate::c::Index = (*((*node).ss.ps.plan as *mut Scan)).scanrelid;

        Assert!(scanrelid > 0);
        if !(*epqstate).relsubs_slot.offset((scanrelid - 1) as isize).read().is_null()
            || !(*epqstate).relsubs_rowmark.offset((scanrelid - 1) as isize).read().is_null()
        {
            /* Verify the claim above */
            if !*(*epqstate).relsubs_done.offset((scanrelid - 1) as isize) {
                elog!(ERROR, "unexpected ExecIndexOnlyRestrPos call in EPQ recheck");
            }
            return;
        }
    }

    index_restrpos((*node).ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecInitIndexOnlyScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitIndexOnlyScan(
    node: *mut IndexOnlyScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut IndexOnlyScanState {
    let indexstate: *mut IndexOnlyScanState;
    let currentRelation: Relation;
    let indexRelation: Relation;
    let lockmode: LOCKMODE;
    let tupDesc: TupleDesc;
    let indnkeyatts: c_int;
    let mut namecount: c_int;

    /*
     * create state structure
     */
    indexstate = makeNode!(IndexOnlyScanState, T_IndexOnlyScanState);
    (*indexstate).ss.ps.plan = node as *mut Plan;
    (*indexstate).ss.ps.state = estate;
    (*indexstate).ss.ps.ExecProcNode = Some(ExecIndexOnlyScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*indexstate).ss.ps);

    /*
     * open the scan relation
     */
    currentRelation = ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    (*indexstate).ss.ss_currentRelation = currentRelation;
    (*indexstate).ss.ss_currentScanDesc = ptr::null_mut(); /* no heap scan here */

    /*
     * Build the scan tuple type using the indextlist generated by the
     * planner.  We use this, rather than the index's physical tuple
     * descriptor, because the latter contains storage column types not the
     * types of the original datums.  (It's the AM's responsibility to return
     * suitable data anyway.)
     */
    tupDesc = ExecTypeFromTL((*node).indextlist);
    ExecInitScanTupleSlot(estate, &mut (*indexstate).ss, tupDesc, &TTSOpsVirtual);

    /*
     * We need another slot, in a format that's suitable for the table AM, for
     * when we need to fetch a tuple from the table for rechecking visibility.
     */
    (*indexstate).ioss_TableSlot = ExecAllocTableSlot(
        &raw mut (*estate).es_tupleTable,
        RelationGetDescr(currentRelation),
        table_slot_callbacks(currentRelation),
    );

    /*
     * Initialize result type and projection info.  The node's targetlist will
     * contain Vars with varno = INDEX_VAR, referencing the scan tuple.
     */
    ExecInitResultTypeTL(&mut (*indexstate).ss.ps);
    ExecAssignScanProjectionInfoWithVarno(&mut (*indexstate).ss, INDEX_VAR);

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexorderby expression, only the
     * sub-parts corresponding to runtime keys (see below).
     */
    (*indexstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, indexstate as *mut PlanState);
    (*indexstate).recheckqual =
        ExecInitQual((*node).recheckqual, indexstate as *mut PlanState);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return indexstate;
    }

    /* Open the index relation. */
    lockmode = (*exec_rt_fetch((*node).scan.scanrelid, estate)).rellockmode;
    indexRelation = index_open((*node).indexid, lockmode);
    (*indexstate).ioss_RelationDesc = indexRelation;

    /*
     * Initialize index-specific scan state
     */
    (*indexstate).ioss_RuntimeKeysReady = false;
    (*indexstate).ioss_RuntimeKeys = ptr::null_mut();
    (*indexstate).ioss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys(
        indexstate as *mut PlanState,
        indexRelation,
        (*node).indexqual,
        false,
        &raw mut (*indexstate).ioss_ScanKeys,
        &raw mut (*indexstate).ioss_NumScanKeys,
        &raw mut (*indexstate).ioss_RuntimeKeys,
        &raw mut (*indexstate).ioss_NumRuntimeKeys,
        ptr::null_mut(), /* no ArrayKeys */
        ptr::null_mut(),
    );

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys(
        indexstate as *mut PlanState,
        indexRelation,
        (*node).indexorderby,
        true,
        &raw mut (*indexstate).ioss_OrderByKeys,
        &raw mut (*indexstate).ioss_NumOrderByKeys,
        &raw mut (*indexstate).ioss_RuntimeKeys,
        &raw mut (*indexstate).ioss_NumRuntimeKeys,
        ptr::null_mut(), /* no ArrayKeys */
        ptr::null_mut(),
    );

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (*indexstate).ioss_NumRuntimeKeys != 0 {
        let stdecontext: *mut ExprContext = (*indexstate).ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &mut (*indexstate).ss.ps);
        (*indexstate).ioss_RuntimeContext = (*indexstate).ss.ps.ps_ExprContext;
        (*indexstate).ss.ps.ps_ExprContext = stdecontext;
    } else {
        (*indexstate).ioss_RuntimeContext = ptr::null_mut();
    }

    (*indexstate).ioss_NameCStringAttNums = ptr::null_mut();
    indnkeyatts = rel_indnkeyatts(indexRelation);
    namecount = 0;

    /*
     * The "name" type for btree uses text_ops which results in storing
     * cstrings in the indexed keys rather than names.  Here we detect that in
     * a generic way in case other index AMs want to do the same optimization.
     * Check for opclasses with an opcintype of NAMEOID and an index tuple
     * descriptor with CSTRINGOID.  If any of these are found, create an array
     * marking the index attribute number of each of them.  StoreIndexTuple()
     * handles copying the name Datums into a NAMEDATALEN-byte allocation.
     */

    /* First, count the number of such index keys */
    for attnum in 0..indnkeyatts {
        if rel_attr_atttypid(indexRelation, attnum) == CSTRINGOID
            && rel_opcintype(indexRelation, attnum) == NAMEOID
        {
            namecount += 1;
        }
    }

    if namecount > 0 {
        let mut idx: c_int = 0;

        /*
         * Now create an array to mark the attribute numbers of the keys that
         * need to be converted from cstring to name.
         */
        (*indexstate).ioss_NameCStringAttNums =
            palloc(size_of::<AttrNumber>() * namecount as usize) as *mut AttrNumber;

        for attnum in 0..indnkeyatts {
            if rel_attr_atttypid(indexRelation, attnum) == CSTRINGOID
                && rel_opcintype(indexRelation, attnum) == NAMEOID
            {
                *(*indexstate).ioss_NameCStringAttNums.offset(idx as isize) = attnum as AttrNumber;
                idx += 1;
            }
        }
    }

    (*indexstate).ioss_NameCStringCount = namecount;

    /*
     * all done.
     */
    indexstate
}

/* ----------------------------------------------------------------
 *		Parallel Index-only Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyScanEstimate(node: *mut IndexOnlyScanState, pcxt: *mut ParallelContext) {
    let estate: *mut EState = (*node).ss.ps.state;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    (*node).ioss_PscanLen = index_parallelscan_estimate(
        (*node).ioss_RelationDesc,
        (*node).ioss_NumScanKeys,
        (*node).ioss_NumOrderByKeys,
        (*estate).es_snapshot as *mut c_void,
        instrument,
        parallel_aware,
        pcxt_nworkers(pcxt),
    );
    shm_toc_estimate_chunk(pcxt_estimator(pcxt), (*node).ioss_PscanLen);
    shm_toc_estimate_keys(pcxt_estimator(pcxt), 1);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanInitializeDSM
 *
 *		Set up a parallel index-only scan descriptor.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyScanInitializeDSM(
    node: *mut IndexOnlyScanState,
    pcxt: *mut ParallelContext,
) {
    let estate: *mut EState = (*node).ss.ps.state;
    let piscan: ParallelIndexScanDesc;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    piscan = shm_toc_allocate(pcxt_toc(pcxt), (*node).ioss_PscanLen);
    index_parallelscan_initialize(
        (*node).ss.ss_currentRelation,
        (*node).ioss_RelationDesc,
        (*estate).es_snapshot as *mut c_void,
        instrument,
        parallel_aware,
        pcxt_nworkers(pcxt),
        &raw mut (*node).ioss_SharedInfo,
        piscan,
    );
    shm_toc_insert(pcxt_toc(pcxt), (*(*node).ss.ps.plan).plan_node_id as u64, piscan);

    if !parallel_aware {
        /* Only here to initialize SharedInfo in DSM */
        return;
    }

    (*node).ioss_ScanDesc = index_beginscan_parallel(
        (*node).ss.ss_currentRelation,
        (*node).ioss_RelationDesc,
        &raw mut (*node).ioss_Instrument,
        (*node).ioss_NumScanKeys,
        (*node).ioss_NumOrderByKeys,
        piscan,
    );
    idxsd_set_want_itup((*node).ioss_ScanDesc, true);
    (*node).ioss_VMBuffer = InvalidBuffer;

    /*
     * If no run-time keys to calculate or they are ready, go ahead and pass
     * the scankeys to the index AM.
     */
    if (*node).ioss_NumRuntimeKeys == 0 || (*node).ioss_RuntimeKeysReady {
        index_rescan(
            (*node).ioss_ScanDesc,
            (*node).ioss_ScanKeys,
            (*node).ioss_NumScanKeys,
            (*node).ioss_OrderByKeys,
            (*node).ioss_NumOrderByKeys,
        );
    }
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyScanReInitializeDSM(
    node: *mut IndexOnlyScanState,
    _pcxt: *mut ParallelContext,
) {
    Assert!((*(*node).ss.ps.plan).parallel_aware);
    index_parallelrescan((*node).ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyScanInitializeWorker(
    node: *mut IndexOnlyScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let piscan: ParallelIndexScanDesc;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    piscan = shm_toc_lookup(pwcxt_toc(pwcxt), (*(*node).ss.ps.plan).plan_node_id as u64, false);

    if instrument {
        (*node).ioss_SharedInfo = OffsetToPointer(piscan, piscan_ps_offset_ins(piscan))
            as *mut SharedIndexScanInstrumentation;
    }

    if !parallel_aware {
        /* Only here to set up worker node's SharedInfo */
        return;
    }

    (*node).ioss_ScanDesc = index_beginscan_parallel(
        (*node).ss.ss_currentRelation,
        (*node).ioss_RelationDesc,
        &raw mut (*node).ioss_Instrument,
        (*node).ioss_NumScanKeys,
        (*node).ioss_NumOrderByKeys,
        piscan,
    );
    idxsd_set_want_itup((*node).ioss_ScanDesc, true);

    /*
     * If no run-time keys to calculate or they are ready, go ahead and pass
     * the scankeys to the index AM.
     */
    if (*node).ioss_NumRuntimeKeys == 0 || (*node).ioss_RuntimeKeysReady {
        index_rescan(
            (*node).ioss_ScanDesc,
            (*node).ioss_ScanKeys,
            (*node).ioss_NumScanKeys,
            (*node).ioss_OrderByKeys,
            (*node).ioss_NumOrderByKeys,
        );
    }
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanRetrieveInstrumentation
 *
 *		Transfer index-only scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexOnlyScanRetrieveInstrumentation(node: *mut IndexOnlyScanState) {
    let SharedInfo: *mut SharedIndexScanInstrumentation = (*node).ioss_SharedInfo;
    let size: usize;

    if SharedInfo.is_null() {
        return;
    }

    /* Create a copy of SharedInfo in backend-local memory */
    size = si_winstrument_offset()
        + si_num_workers(SharedInfo) as usize * size_of::<IndexScanInstrumentation>();
    (*node).ioss_SharedInfo = palloc(size) as *mut SharedIndexScanInstrumentation;
    ptr::copy_nonoverlapping(
        SharedInfo as *const u8,
        (*node).ioss_SharedInfo as *mut u8,
        size,
    );
}

// ----------------------------------------------------------------
// Opaque ParallelContext / ParallelWorkerContext / ParallelIndexScanDesc /
// SharedIndexScanInstrumentation / IndexScanInstrumentation accessors.
//
// These stand in for direct struct member references in the C source until
// access/parallel.h, storage/shm_toc.h and access/relscan.h are ported.
// ----------------------------------------------------------------

unsafe fn pcxt_estimator(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (&pcxt->estimator)
}

unsafe fn pcxt_nworkers(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: access/parallel.h (pcxt->nworkers)
}

unsafe fn pcxt_toc(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (pcxt->toc)
}

unsafe fn pwcxt_toc(_pwcxt: *mut ParallelWorkerContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (pwcxt->toc)
}

unsafe fn piscan_ps_offset_ins(_piscan: ParallelIndexScanDesc) -> usize {
    unimplemented!() // TODO: access/relscan.h (piscan->ps_offset_ins)
}

unsafe fn si_num_workers(_si: *mut SharedIndexScanInstrumentation) -> c_int {
    unimplemented!() // TODO: access/genam.h (si->num_workers)
}

unsafe fn si_winstrument(
    _si: *mut SharedIndexScanInstrumentation,
    _n: c_int,
) -> *mut IndexScanInstrumentation {
    unimplemented!() // TODO: access/genam.h (&si->winstrument[n])
}

unsafe fn si_winstrument_offset() -> usize {
    unimplemented!() // TODO: access/genam.h (offsetof(SharedIndexScanInstrumentation, winstrument))
}

unsafe fn instr_nsearches(_instr: *mut IndexScanInstrumentation) -> u64 {
    unimplemented!() // TODO: access/genam.h (instr->nsearches)
}

unsafe fn instr_add_nsearches(_dst: *mut IndexScanInstrumentation, _delta: u64) {
    unimplemented!() // TODO: access/genam.h (dst->nsearches += delta)
}

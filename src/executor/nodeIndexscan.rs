//! nodeIndexscan.c
//!   Routines to support indexed scans of relations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/nodeIndexscan.c
//!
//! INTERFACE ROUTINES
//!     ExecIndexScan           scans a relation using an index
//!     IndexNext               retrieve next tuple using index
//!     IndexNextWithReorder    same, but recheck ORDER BY expressions
//!     ExecInitIndexScan       creates and initializes state info.
//!     ExecReScanIndexScan     rescans the indexed relation.
//!     ExecEndIndexScan        releases all storage.
//!     ExecIndexMarkPos        marks scan position.
//!     ExecIndexRestrPos       restores scan position.
//!     ExecIndexScanEstimate   estimates DSM space needed for parallel index scan
//!     ExecIndexScanInitializeDSM initialize DSM for parallel indexscan
//!     ExecIndexScanReInitializeDSM reinitialize DSM for fresh scan
//!     ExecIndexScanInitializeWorker attach to DSM info in parallel worker

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::mem::size_of;
use std::ptr;

use crate::{castNode, foreach, forboth, forfour, makeNode, Assert, IsA};

use crate::access::attnum::AttrNumber;
use crate::access::common::scankey::{
    ScanKeyEntryInitialize, SK_ISNULL, SK_ORDER_BY, SK_ROW_END, SK_ROW_HEADER, SK_ROW_MEMBER,
    SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};
use crate::access::sdir::{
    ScanDirection, ScanDirectionCombine, ScanDirectionIsBackward, ScanDirectionIsForward,
    ForwardScanDirection,
};
use crate::access::stratnum::{InvalidStrategy, StrategyNumber};

use crate::executor::executor::{
    exec_rt_fetch, ExecAssignExprContext, ExecAssignScanProjectionInfo, ExecEvalExpr, ExecInitExpr,
    ExecInitExprList, ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot,
    ExecOpenScanRelation, ExecQualAndReset, ExecReScan, ExecScan, ExecScanReScan,
    ExecScanAccessMtd, ExecScanRecheckMtd, EXEC_FLAG_EXPLAIN_ONLY,
};
use crate::executor::execTuples::ExecForceStoreHeapTuple;
use crate::executor::execUtils::ResetExprContext;
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlotHeapTuple};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::nodes::execnodes::{
    EPQState, EState, ExprContext, ExprState, IndexArrayKeyInfo, IndexRuntimeKeyInfo,
    IndexScanInstrumentation, IndexScanState, PlanState, ScanKeyData, ScanState,
    SharedIndexScanInstrumentation, SortSupportData, TupleTableSlot,
};
use crate::nodes::nodeFuncs::{exprCollation, exprType};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{list_length, List};
use crate::nodes::plannodes::{IndexScan, Plan, Scan};
use crate::nodes::primnodes::{
    Const, NullTest, OpExpr, RelabelType, RowCompareExpr, ScalarArrayOpExpr, Var, INDEX_VAR,
};
use crate::nodes::primnodes::NullTestType;

use crate::access::common::heaptuple::heap_freetuple;
use crate::access::common::tupdesc::TupleDesc;

use crate::storage::lockdefs::{NoLock, LOCKMODE};

use crate::utils::adt::datum::datumCopy;
use crate::utils::cache::lsyscache::{
    get_op_opfamily_properties, get_opfamily_proc, get_typlenbyval, get_typlenbyvalalign,
};
use crate::utils::rel::Relation;
use crate::utils::sort::sortsupport::{PrepareSortSupportFromOrderingOp, SortSupport};

use crate::lib::pairingheap::{
    pairingheap, pairingheap_add, pairingheap_allocate, pairingheap_first, pairingheap_is_empty,
    pairingheap_node, pairingheap_remove_first,
};

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type IndexScanDesc = *mut c_void;
type ParallelContext = c_void;
type ParallelWorkerContext = c_void;
type ParallelIndexScanDesc = *mut c_void;
type HeapTuple = *mut crate::access::htup_details::HeapTupleData;
type ArrayType = c_void;

/*
 * When an ordering operator is used, tuples fetched from the index that
 * need to be reordered are queued in a pairing heap, as ReorderTuples.
 */
#[repr(C)]
struct ReorderTuple {
    ph_node: pairingheap_node,
    htup: HeapTuple,
    orderbyvals: *mut Datum,
    orderbynulls: *mut bool,
}

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

unsafe fn index_getnext_slot(
    _scan: IndexScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/genam.h (indexam.c)
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

unsafe fn RelationGetDescr(_relation: Relation) -> TupleDesc {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn table_slot_callbacks(
    _relation: Relation,
) -> *const crate::executor::tuptable::TupleTableSlotOps {
    unimplemented!() // TODO: access/tableam.h
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

// utils/memutils.h: OffsetToPointer().
unsafe fn OffsetToPointer(_base: *mut c_void, _offset: usize) -> *mut c_void {
    unimplemented!() // TODO: utils/memutils.h
}

// utils/fmgr.h: TypeIsToastable() macro.
unsafe fn TypeIsToastable(_typid: Oid) -> bool {
    unimplemented!() // TODO: utils/fmgr.h (lsyscache.c get_typstorage)
}

// fmgr.h: PG_DETOAST_DATUM().
unsafe fn PG_DETOAST_DATUM(_datum: Datum) -> *mut c_void {
    unimplemented!() // TODO: fmgr.h (fmgr.c)
}

// utils/array.h: DatumGetArrayTypeP() / ARR_ELEMTYPE().
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!() // TODO: utils/array.h
}

unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid {
    unimplemented!() // TODO: utils/array.h
}

unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: int16,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO: utils/arrayfuncs.c
}

// executor/instrument.h: InstrCountFiltered2() macro.
unsafe fn InstrCountFiltered2(_node: *mut IndexScanState, _delta: u64) {
    unimplemented!() // TODO: executor/instrument.h
}

// access/skey.h: ScanKeyData field accessors (the IndexScanState ScanKeyData is
// presently an opaque stub; these stand in for the direct struct member
// references until access/skey.h's struct is unified here).
unsafe fn skey_sk_flags(_sk: *mut ScanKeyData) -> c_int {
    unimplemented!() // TODO: access/skey.h (sk->sk_flags)
}

unsafe fn skey_set_sk_flags(_sk: *mut ScanKeyData, _v: c_int) {
    unimplemented!() // TODO: access/skey.h (sk->sk_flags)
}

unsafe fn skey_sk_attno(_sk: *mut ScanKeyData) -> AttrNumber {
    unimplemented!() // TODO: access/skey.h (sk->sk_attno)
}

unsafe fn skey_set_sk_attno(_sk: *mut ScanKeyData, _v: AttrNumber) {
    unimplemented!() // TODO: access/skey.h (sk->sk_attno)
}

unsafe fn skey_set_sk_strategy(_sk: *mut ScanKeyData, _v: StrategyNumber) {
    unimplemented!() // TODO: access/skey.h (sk->sk_strategy)
}

unsafe fn skey_set_sk_argument(_sk: *mut ScanKeyData, _v: Datum) {
    unimplemented!() // TODO: access/skey.h (sk->sk_argument)
}

unsafe fn ScanKeyEntryInitialize_op(
    entry: *mut ScanKeyData,
    flags: c_int,
    attributeNumber: AttrNumber,
    strategy: StrategyNumber,
    subtype: Oid,
    collation: Oid,
    procedure: RegProcedure,
    argument: Datum,
) {
    // The IndexScanState ScanKeyData is opaque here; the real
    // access/common/scankey.rs ScanKeyData is a distinct (compatible) type.
    ScanKeyEntryInitialize(
        entry as *mut crate::access::common::scankey::ScanKeyData,
        flags,
        attributeNumber,
        strategy,
        subtype,
        collation,
        procedure,
        argument,
    );
}

// access/relscan.h: IndexScanDescData field accessors.
unsafe fn idxsd_xs_recheck(_scan: IndexScanDesc) -> bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_recheck)
}

unsafe fn idxsd_xs_recheckorderby(_scan: IndexScanDesc) -> bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_recheckorderby)
}

unsafe fn idxsd_xs_orderbyvals(_scan: IndexScanDesc) -> *mut Datum {
    unimplemented!() // TODO: access/relscan.h (scan->xs_orderbyvals)
}

unsafe fn idxsd_xs_orderbynulls(_scan: IndexScanDesc) -> *mut bool {
    unimplemented!() // TODO: access/relscan.h (scan->xs_orderbynulls)
}

unsafe fn idxsd_numberOfOrderBys(_scan: IndexScanDesc) -> c_int {
    unimplemented!() // TODO: access/relscan.h (scan->numberOfOrderBys)
}

// utils/rel.h: index Relation accessors.
unsafe fn IndexRelationGetNumberOfKeyAttributes(_relation: Relation) -> c_int {
    unimplemented!() // TODO: utils/rel.h (relation->rd_index->indnkeyatts)
}

unsafe fn rel_opfamily(_index: Relation, _attnum: c_int) -> Oid {
    unimplemented!() // TODO: utils/rel.h (index->rd_opfamily[attnum])
}

unsafe fn rel_amcanorder(_index: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h (index->rd_indam->amcanorder)
}

unsafe fn rel_amsearcharray(_index: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h (index->rd_indam->amsearcharray)
}

// access/nbtree.h: BTORDER_PROC support function number.
const BTORDER_PROC: i16 = 1;

// utils/expandeddatum.h-ish: get_leftop()/get_rightop() (clauses.h).
unsafe fn get_leftop(clause: *const Expr) -> *mut Expr {
    let op = clause as *mut OpExpr;
    if !(*op).args.is_null() && list_length((*op).args) > 0 {
        crate::nodes::pg_list::linitial((*op).args) as *mut Expr
    } else {
        ptr::null_mut()
    }
}

unsafe fn get_rightop(clause: *const Expr) -> *mut Expr {
    let op = clause as *mut OpExpr;
    if !(*op).args.is_null() && list_length((*op).args) > 1 {
        crate::nodes::pg_list::lsecond((*op).args) as *mut Expr
    } else {
        ptr::null_mut()
    }
}

// SharedIndexScanInstrumentation accessors (opaque stub).
unsafe fn si_num_workers(_si: *mut SharedIndexScanInstrumentation) -> c_int {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.num_workers)
}

unsafe fn si_winstrument(
    _si: *mut SharedIndexScanInstrumentation,
    _idx: c_int,
) -> *mut IndexScanInstrumentation {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.winstrument[])
}

unsafe fn si_ps_offset_ins(_piscan: ParallelIndexScanDesc) -> usize {
    unimplemented!() // TODO: access/relscan.h (ParallelIndexScanDescData.ps_offset_ins)
}

unsafe fn instr_add_nsearches(_dst: *mut IndexScanInstrumentation, _n: u64) {
    unimplemented!() // TODO: access/genam.h (IndexScanInstrumentation.nsearches)
}

unsafe fn instr_get_nsearches(_src: *mut IndexScanInstrumentation) -> u64 {
    unimplemented!() // TODO: access/genam.h (IndexScanInstrumentation.nsearches)
}

unsafe fn offsetof_winstrument() -> usize {
    unimplemented!() // TODO: offsetof(SharedIndexScanInstrumentation, winstrument)
}

// access/parallel.h: ParallelContext / ParallelWorkerContext accessors.
unsafe fn pcxt_nworkers(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.nworkers)
}

unsafe fn pcxt_estimator(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.estimator)
}

unsafe fn pcxt_toc(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.toc)
}

unsafe fn pwcxt_toc(_pwcxt: *mut ParallelWorkerContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelWorkerContext.toc)
}

// global stub (extern global in C; miscadmin.h).
#[allow(non_upper_case_globals)]
static mut ParallelWorkerNumber: c_int = 0;

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the IndexScan node's currentRelation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
unsafe fn IndexNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut IndexScanState;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let direction: ScanDirection;
    let mut scandesc: IndexScanDesc;
    let slot: *mut TupleTableSlot;

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
        (*((*node).ss.ps.plan as *mut IndexScan)).indexorderdir,
    );
    scandesc = (*node).iss_ScanDesc as IndexScanDesc;
    econtext = (*node).ss.ps.ps_ExprContext;
    slot = (*node).ss.ss_ScanTupleSlot;

    if scandesc.is_null() {
        /*
         * We reach here if the index scan is not parallel, or if we're
         * serially executing an index scan that was planned to be parallel.
         */
        scandesc = index_beginscan(
            (*node).ss.ss_currentRelation,
            (*node).iss_RelationDesc,
            (*estate).es_snapshot as *mut c_void,
            &raw mut (*node).iss_Instrument,
            (*node).iss_NumScanKeys,
            (*node).iss_NumOrderByKeys,
        );

        (*node).iss_ScanDesc = scandesc as *mut _;

        /*
         * If no run-time keys to calculate or they are ready, go ahead and
         * pass the scankeys to the index AM.
         */
        if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
            index_rescan(
                scandesc,
                (*node).iss_ScanKeys,
                (*node).iss_NumScanKeys,
                (*node).iss_OrderByKeys,
                (*node).iss_NumOrderByKeys,
            );
        }
    }

    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    while index_getnext_slot(scandesc, direction, slot) {
        CHECK_FOR_INTERRUPTS();

        /*
         * If the index was lossy, we have to recheck the index quals using
         * the fetched tuple.
         */
        if idxsd_xs_recheck(scandesc) {
            (*econtext).ecxt_scantuple = slot;
            if !ExecQualAndReset((*node).indexqualorig, econtext) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    (*node).iss_ReachedEnd = true;
    ExecClearTuple(slot)
}

/* ----------------------------------------------------------------
 *		IndexNextWithReorder
 *
 *		Like IndexNext, but this version can also re-check ORDER BY
 *		expressions, and reorder the tuples as necessary.
 * ----------------------------------------------------------------
 */
unsafe fn IndexNextWithReorder(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut IndexScanState;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let mut scandesc: IndexScanDesc;
    let slot: *mut TupleTableSlot;
    let mut topmost: *mut ReorderTuple = ptr::null_mut();
    let mut was_exact: bool = false;
    let mut lastfetched_vals: *mut Datum;
    let mut lastfetched_nulls: *mut bool;
    let mut cmp: c_int;

    estate = (*node).ss.ps.state;

    /*
     * Only forward scan is supported with reordering.  Note: we can get away
     * with just Asserting here because the system will not try to run the
     * plan backwards if ExecSupportsBackwardScan() says it won't work.
     * Currently, that is guaranteed because no index AMs support both
     * amcanorderbyop and amcanbackward; if any ever do,
     * ExecSupportsBackwardScan() will need to consider indexorderbys
     * explicitly.
     */
    Assert!(!ScanDirectionIsBackward(
        (*((*node).ss.ps.plan as *mut IndexScan)).indexorderdir
    ));
    Assert!(ScanDirectionIsForward((*estate).es_direction));

    scandesc = (*node).iss_ScanDesc as IndexScanDesc;
    econtext = (*node).ss.ps.ps_ExprContext;
    slot = (*node).ss.ss_ScanTupleSlot;

    if scandesc.is_null() {
        /*
         * We reach here if the index scan is not parallel, or if we're
         * serially executing an index scan that was planned to be parallel.
         */
        scandesc = index_beginscan(
            (*node).ss.ss_currentRelation,
            (*node).iss_RelationDesc,
            (*estate).es_snapshot as *mut c_void,
            &raw mut (*node).iss_Instrument,
            (*node).iss_NumScanKeys,
            (*node).iss_NumOrderByKeys,
        );

        (*node).iss_ScanDesc = scandesc as *mut _;

        /*
         * If no run-time keys to calculate or they are ready, go ahead and
         * pass the scankeys to the index AM.
         */
        if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
            index_rescan(
                scandesc,
                (*node).iss_ScanKeys,
                (*node).iss_NumScanKeys,
                (*node).iss_OrderByKeys,
                (*node).iss_NumOrderByKeys,
            );
        }
    }

    loop {
        CHECK_FOR_INTERRUPTS();

        /*
         * Check the reorder queue first.  If the topmost tuple in the queue
         * has an ORDER BY value smaller than (or equal to) the value last
         * returned by the index, we can return it now.
         */
        if !pairingheap_is_empty((*node).iss_ReorderQueue) {
            topmost = pairingheap_first((*node).iss_ReorderQueue) as *mut ReorderTuple;

            if (*node).iss_ReachedEnd
                || cmp_orderbyvals(
                    (*topmost).orderbyvals,
                    (*topmost).orderbynulls,
                    idxsd_xs_orderbyvals(scandesc),
                    idxsd_xs_orderbynulls(scandesc),
                    node,
                ) <= 0
            {
                let tuple: HeapTuple;

                tuple = reorderqueue_pop(node);

                /* Pass 'true', as the tuple in the queue is a palloc'd copy */
                ExecForceStoreHeapTuple(tuple, slot, true);
                return slot;
            }
        } else if (*node).iss_ReachedEnd {
            /* Queue is empty, and no more tuples from index.  We're done. */
            return ExecClearTuple(slot);
        }

        /*
         * Fetch next tuple from the index.
         */
        'next_indextuple: loop {
            if !index_getnext_slot(scandesc, ForwardScanDirection, slot) {
                /*
                 * No more tuples from the index.  But we still need to drain any
                 * remaining tuples from the queue before we're done.
                 */
                (*node).iss_ReachedEnd = true;
                break 'next_indextuple;
            }

            /*
             * If the index was lossy, we have to recheck the index quals and
             * ORDER BY expressions using the fetched tuple.
             */
            if idxsd_xs_recheck(scandesc) {
                (*econtext).ecxt_scantuple = slot;
                if !ExecQualAndReset((*node).indexqualorig, econtext) {
                    /* Fails recheck, so drop it and loop back for another */
                    InstrCountFiltered2(node, 1);
                    /* allow this loop to be cancellable */
                    CHECK_FOR_INTERRUPTS();
                    continue 'next_indextuple;
                }
            }

            if idxsd_xs_recheckorderby(scandesc) {
                (*econtext).ecxt_scantuple = slot;
                ResetExprContext(econtext);
                EvalOrderByExpressions(node, econtext);

                /*
                 * Was the ORDER BY value returned by the index accurate?  The
                 * recheck flag means that the index can return inaccurate values,
                 * but then again, the value returned for any particular tuple
                 * could also be exactly correct.  Compare the value returned by
                 * the index with the recalculated value.  (If the value returned
                 * by the index happened to be exact right, we can often avoid
                 * pushing the tuple to the queue, just to pop it back out again.)
                 */
                cmp = cmp_orderbyvals(
                    (*node).iss_OrderByValues,
                    (*node).iss_OrderByNulls,
                    idxsd_xs_orderbyvals(scandesc),
                    idxsd_xs_orderbynulls(scandesc),
                    node,
                );
                if cmp < 0 {
                    elog!(ERROR, "index returned tuples in wrong order");
                } else if cmp == 0 {
                    was_exact = true;
                } else {
                    was_exact = false;
                }
                lastfetched_vals = (*node).iss_OrderByValues;
                lastfetched_nulls = (*node).iss_OrderByNulls;
            } else {
                was_exact = true;
                lastfetched_vals = idxsd_xs_orderbyvals(scandesc);
                lastfetched_nulls = idxsd_xs_orderbynulls(scandesc);
            }

            /*
             * Can we return this tuple immediately, or does it need to be pushed
             * to the reorder queue?  If the ORDER BY expression values returned
             * by the index were inaccurate, we can't return it yet, because the
             * next tuple from the index might need to come before this one. Also,
             * we can't return it yet if there are any smaller tuples in the queue
             * already.
             */
            if !was_exact
                || (!topmost.is_null()
                    && cmp_orderbyvals(
                        lastfetched_vals,
                        lastfetched_nulls,
                        (*topmost).orderbyvals,
                        (*topmost).orderbynulls,
                        node,
                    ) > 0)
            {
                /* Put this tuple to the queue */
                reorderqueue_push(node, slot, lastfetched_vals, lastfetched_nulls);
                break 'next_indextuple;
            } else {
                /* Can return this tuple immediately. */
                return slot;
            }
        }
        /* C uses `continue` for the outer for(;;) after draining/pushing */
        continue;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    #[allow(unreachable_code)]
    ExecClearTuple(slot)
}

/*
 * Calculate the expressions in the ORDER BY clause, based on the heap tuple.
 */
unsafe fn EvalOrderByExpressions(node: *mut IndexScanState, econtext: *mut ExprContext) {
    let mut i: c_int;
    let l: *mut crate::nodes::pg_list::ListCell;
    let oldContext: MemoryContext;

    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    i = 0;
    foreach!(l, (*node).indexorderbyorig, {
        let orderby = crate::current_cell!(l) as *mut ExprState;

        *(*node).iss_OrderByValues.offset(i as isize) = ExecEvalExpr(
            orderby,
            econtext,
            (*node).iss_OrderByNulls.offset(i as isize),
        );
        i += 1;
    });

    MemoryContextSwitchTo(oldContext);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn IndexRecheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    let node = node as *mut IndexScanState;
    let econtext: *mut ExprContext;

    /*
     * extract necessary information from index scan node
     */
    econtext = (*node).ss.ps.ps_ExprContext;

    /* Does the tuple meet the indexqual condition? */
    (*econtext).ecxt_scantuple = slot;
    ExecQualAndReset((*node).indexqualorig, econtext)
}

/*
 * Compare ORDER BY expression values.
 */
unsafe fn cmp_orderbyvals(
    adist: *const Datum,
    anulls: *const bool,
    bdist: *const Datum,
    bnulls: *const bool,
    node: *mut IndexScanState,
) -> c_int {
    let mut i: c_int;
    let mut result: c_int;

    i = 0;
    while i < (*node).iss_NumOrderByKeys {
        let ssup: SortSupport = (*node).iss_SortSupport.offset(i as isize);

        /*
         * Handle nulls.  We only need to support NULLS LAST ordering, because
         * match_pathkeys_to_index() doesn't consider indexorderby
         * implementation otherwise.
         */
        if *anulls.offset(i as isize) && !*bnulls.offset(i as isize) {
            return 1;
        } else if !*anulls.offset(i as isize) && *bnulls.offset(i as isize) {
            return -1;
        } else if *anulls.offset(i as isize) && *bnulls.offset(i as isize) {
            return 0;
        }

        result = ((*ssup).comparator.unwrap())(
            *adist.offset(i as isize),
            *bdist.offset(i as isize),
            ssup,
        );
        if result != 0 {
            return result;
        }

        i += 1;
    }

    0
}

/*
 * Pairing heap provides getting topmost (greatest) element while KNN provides
 * ascending sort.  That's why we invert the sort order.
 */
unsafe fn reorderqueue_cmp(
    a: *const pairingheap_node,
    b: *const pairingheap_node,
    arg: *mut c_void,
) -> c_int {
    let rta = a as *mut ReorderTuple;
    let rtb = b as *mut ReorderTuple;
    let node = arg as *mut IndexScanState;

    /* exchange argument order to invert the sort order */
    cmp_orderbyvals(
        (*rtb).orderbyvals,
        (*rtb).orderbynulls,
        (*rta).orderbyvals,
        (*rta).orderbynulls,
        node,
    )
}

/*
 * Helper function to push a tuple to the reorder queue.
 */
unsafe fn reorderqueue_push(
    node: *mut IndexScanState,
    slot: *mut TupleTableSlot,
    orderbyvals: *mut Datum,
    orderbynulls: *mut bool,
) {
    let scandesc: IndexScanDesc = (*node).iss_ScanDesc as IndexScanDesc;
    let estate: *mut EState = (*node).ss.ps.state;
    let oldContext: MemoryContext = MemoryContextSwitchTo((*estate).es_query_cxt);
    let rt: *mut ReorderTuple;
    let mut i: c_int;

    rt = palloc(size_of::<ReorderTuple>()) as *mut ReorderTuple;
    (*rt).htup = ExecCopySlotHeapTuple(slot) as HeapTuple;
    (*rt).orderbyvals =
        palloc(size_of::<Datum>() * idxsd_numberOfOrderBys(scandesc) as usize) as *mut Datum;
    (*rt).orderbynulls =
        palloc(size_of::<bool>() * idxsd_numberOfOrderBys(scandesc) as usize) as *mut bool;
    i = 0;
    while i < (*node).iss_NumOrderByKeys {
        if !*orderbynulls.offset(i as isize) {
            *(*rt).orderbyvals.offset(i as isize) = datumCopy(
                *orderbyvals.offset(i as isize),
                *(*node).iss_OrderByTypByVals.offset(i as isize),
                *(*node).iss_OrderByTypLens.offset(i as isize) as c_int,
            );
        } else {
            *(*rt).orderbyvals.offset(i as isize) = 0 as Datum;
        }
        *(*rt).orderbynulls.offset(i as isize) = *orderbynulls.offset(i as isize);
        i += 1;
    }
    pairingheap_add((*node).iss_ReorderQueue, &raw mut (*rt).ph_node);

    MemoryContextSwitchTo(oldContext);
}

/*
 * Helper function to pop the next tuple from the reorder queue.
 */
unsafe fn reorderqueue_pop(node: *mut IndexScanState) -> HeapTuple {
    let result: HeapTuple;
    let topmost: *mut ReorderTuple;
    let mut i: c_int;

    topmost = pairingheap_remove_first((*node).iss_ReorderQueue) as *mut ReorderTuple;

    result = (*topmost).htup;
    i = 0;
    while i < (*node).iss_NumOrderByKeys {
        if !*(*node).iss_OrderByTypByVals.offset(i as isize)
            && !*(*topmost).orderbynulls.offset(i as isize)
        {
            pfree(DatumGetPointer(*(*topmost).orderbyvals.offset(i as isize)) as *mut c_void);
        }
        i += 1;
    }
    pfree((*topmost).orderbyvals as *mut c_void);
    pfree((*topmost).orderbynulls as *mut c_void);
    pfree(topmost as *mut c_void);

    result
}

// Additional pg_list / node accessors used by the functions below.
use crate::nodes::nodes::nodeTag;
use crate::nodes::pg_list::{lfirst, lfirst_oid, linitial, lsecond, ListCell};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::primnodes::Expr;

/* ----------------------------------------------------------------
 *		ExecIndexScan(node)
 * ----------------------------------------------------------------
 */
unsafe fn ExecIndexScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut IndexScanState = castNode!(IndexScanState, T_IndexScanState, pstate);

    /*
     * If we have runtime keys and they've not already been set up, do it now.
     */
    if (*node).iss_NumRuntimeKeys != 0 && !(*node).iss_RuntimeKeysReady {
        ExecReScan(node as *mut PlanState);
    }

    if (*node).iss_NumOrderByKeys > 0 {
        ExecScan(
            &raw mut (*node).ss,
            Some(IndexNextWithReorder),
            Some(IndexRecheck),
        )
    } else {
        ExecScan(&raw mut (*node).ss, Some(IndexNext), Some(IndexRecheck))
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanIndexScan(node: *mut IndexScanState) {
    /*
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     */
    if (*node).iss_NumRuntimeKeys != 0 {
        let econtext: *mut ExprContext = (*node).iss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(
            econtext,
            (*node).iss_RuntimeKeys,
            (*node).iss_NumRuntimeKeys,
        );
    }
    (*node).iss_RuntimeKeysReady = true;

    /* flush the reorder queue */
    if !(*node).iss_ReorderQueue.is_null() {
        let mut tuple: HeapTuple;

        while !pairingheap_is_empty((*node).iss_ReorderQueue) {
            tuple = reorderqueue_pop(node);
            heap_freetuple(tuple);
        }
    }

    /* reset index scan */
    if !(*node).iss_ScanDesc.is_null() {
        index_rescan(
            (*node).iss_ScanDesc as IndexScanDesc,
            (*node).iss_ScanKeys,
            (*node).iss_NumScanKeys,
            (*node).iss_OrderByKeys,
            (*node).iss_NumOrderByKeys,
        );
    }
    (*node).iss_ReachedEnd = false;

    ExecScanReScan(&raw mut (*node).ss);
}

/*
 * ExecIndexEvalRuntimeKeys
 *		Evaluate any runtime key values, and update the scankeys.
 */
pub unsafe fn ExecIndexEvalRuntimeKeys(
    econtext: *mut ExprContext,
    runtimeKeys: *mut IndexRuntimeKeyInfo,
    numRuntimeKeys: c_int,
) {
    let mut j: c_int;
    let oldContext: MemoryContext;

    /* We want to keep the key values in per-tuple memory */
    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    j = 0;
    while j < numRuntimeKeys {
        let scan_key: *mut ScanKeyData = (*runtimeKeys.offset(j as isize)).scan_key;
        let key_expr: *mut ExprState = (*runtimeKeys.offset(j as isize)).key_expr;
        let mut scanvalue: Datum;
        let mut isNull: bool = false;

        /*
         * For each run-time key, extract the run-time expression and evaluate
         * it with respect to the current context.  We then stick the result
         * into the proper scan key.
         *
         * Note: the result of the eval could be a pass-by-ref value that's
         * stored in some outer scan's tuple, not in
         * econtext->ecxt_per_tuple_memory.  We assume that the outer tuple
         * will stay put throughout our scan.  If this is wrong, we could copy
         * the result into our context explicitly, but I think that's not
         * necessary.
         *
         * It's also entirely possible that the result of the eval is a
         * toasted value.  In this case we should forcibly detoast it, to
         * avoid repeat detoastings each time the value is examined by an
         * index support function.
         */
        scanvalue = ExecEvalExpr(key_expr, econtext, &raw mut isNull);
        if isNull {
            skey_set_sk_argument(scan_key, scanvalue);
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) | SK_ISNULL as c_int);
        } else {
            if (*runtimeKeys.offset(j as isize)).key_toastable {
                scanvalue = PointerGetDatum(PG_DETOAST_DATUM(scanvalue));
            }
            skey_set_sk_argument(scan_key, scanvalue);
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) & !(SK_ISNULL as c_int));
        }

        j += 1;
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * ExecIndexEvalArrayKeys
 *		Evaluate any array key values, and set up to iterate through arrays.
 *
 * Returns true if there are array elements to consider; false means there
 * is at least one null or empty array, so no match is possible.  On true
 * result, the scankeys are initialized with the first elements of the arrays.
 */
pub unsafe fn ExecIndexEvalArrayKeys(
    econtext: *mut ExprContext,
    arrayKeys: *mut IndexArrayKeyInfo,
    numArrayKeys: c_int,
) -> bool {
    let mut result: bool = true;
    let mut j: c_int;
    let oldContext: MemoryContext;

    /* We want to keep the arrays in per-tuple memory */
    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    j = 0;
    while j < numArrayKeys {
        let scan_key: *mut ScanKeyData = (*arrayKeys.offset(j as isize)).scan_key;
        let array_expr: *mut ExprState = (*arrayKeys.offset(j as isize)).array_expr;
        let arraydatum: Datum;
        let mut isNull: bool = false;
        let arrayval: *mut ArrayType;
        let mut elmlen: int16 = 0;
        let mut elmbyval: bool = false;
        let mut elmalign: c_char = 0;
        let mut num_elems: c_int = 0;
        let mut elem_values: *mut Datum = ptr::null_mut();
        let mut elem_nulls: *mut bool = ptr::null_mut();

        /*
         * Compute and deconstruct the array expression. (Notes in
         * ExecIndexEvalRuntimeKeys() apply here too.)
         */
        arraydatum = ExecEvalExpr(array_expr, econtext, &raw mut isNull);
        if isNull {
            result = false;
            break; /* no point in evaluating more */
        }
        arrayval = DatumGetArrayTypeP(arraydatum);
        /* We could cache this data, but not clear it's worth it */
        get_typlenbyvalalign(
            ARR_ELEMTYPE(arrayval),
            &raw mut elmlen,
            &raw mut elmbyval,
            &raw mut elmalign,
        );
        deconstruct_array(
            arrayval,
            ARR_ELEMTYPE(arrayval),
            elmlen,
            elmbyval,
            elmalign,
            &raw mut elem_values,
            &raw mut elem_nulls,
            &raw mut num_elems,
        );
        if num_elems <= 0 {
            result = false;
            break; /* no point in evaluating more */
        }

        /*
         * Note: we expect the previous array data, if any, to be
         * automatically freed by resetting the per-tuple context; hence no
         * pfree's here.
         */
        (*arrayKeys.offset(j as isize)).elem_values = elem_values;
        (*arrayKeys.offset(j as isize)).elem_nulls = elem_nulls;
        (*arrayKeys.offset(j as isize)).num_elems = num_elems;
        skey_set_sk_argument(scan_key, *elem_values.offset(0));
        if *elem_nulls.offset(0) {
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) | SK_ISNULL as c_int);
        } else {
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) & !(SK_ISNULL as c_int));
        }
        (*arrayKeys.offset(j as isize)).next_elem = 1;

        j += 1;
    }

    MemoryContextSwitchTo(oldContext);

    result
}

/*
 * ExecIndexAdvanceArrayKeys
 *		Advance to the next set of array key values, if any.
 *
 * Returns true if there is another set of values to consider, false if not.
 * On true result, the scankeys are initialized with the next set of values.
 */
pub unsafe fn ExecIndexAdvanceArrayKeys(
    arrayKeys: *mut IndexArrayKeyInfo,
    numArrayKeys: c_int,
) -> bool {
    let mut found: bool = false;
    let mut j: c_int;

    /*
     * Note we advance the rightmost array key most quickly, since it will
     * correspond to the lowest-order index column among the available
     * qualifications.  This is hypothesized to result in better locality of
     * access in the index.
     */
    j = numArrayKeys - 1;
    while j >= 0 {
        let scan_key: *mut ScanKeyData = (*arrayKeys.offset(j as isize)).scan_key;
        let mut next_elem: c_int = (*arrayKeys.offset(j as isize)).next_elem;
        let num_elems: c_int = (*arrayKeys.offset(j as isize)).num_elems;
        let elem_values: *mut Datum = (*arrayKeys.offset(j as isize)).elem_values;
        let elem_nulls: *mut bool = (*arrayKeys.offset(j as isize)).elem_nulls;

        if next_elem >= num_elems {
            next_elem = 0;
            found = false; /* need to advance next array key */
        } else {
            found = true;
        }
        skey_set_sk_argument(scan_key, *elem_values.offset(next_elem as isize));
        if *elem_nulls.offset(next_elem as isize) {
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) | SK_ISNULL);
        } else {
            skey_set_sk_flags(scan_key, skey_sk_flags(scan_key) & !SK_ISNULL);
        }
        (*arrayKeys.offset(j as isize)).next_elem = next_elem + 1;
        if found {
            break;
        }

        j -= 1;
    }

    found
}

/* ----------------------------------------------------------------
 *		ExecEndIndexScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndIndexScan(node: *mut IndexScanState) {
    let indexRelationDesc: Relation;
    let indexScanDesc: IndexScanDesc;

    /*
     * extract information from the node
     */
    indexRelationDesc = (*node).iss_RelationDesc;
    indexScanDesc = (*node).iss_ScanDesc as IndexScanDesc;

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory so that it can be picked up by the main
     * process to report in EXPLAIN ANALYZE
     */
    if !(*node).iss_SharedInfo.is_null() && IsParallelWorker() {
        let winstrument: *mut IndexScanInstrumentation;

        Assert!(ParallelWorkerNumber <= si_num_workers((*node).iss_SharedInfo));
        winstrument = si_winstrument((*node).iss_SharedInfo, ParallelWorkerNumber);

        /*
         * We have to accumulate the stats rather than performing a memcpy.
         * When a Gather/GatherMerge node finishes it will perform planner
         * shutdown on the workers.  On rescan it will spin up new workers
         * which will have a new IndexOnlyScanState and zeroed stats.
         */
        instr_add_nsearches(winstrument, instr_get_nsearches(&raw mut (*node).iss_Instrument));
    }

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if !indexScanDesc.is_null() {
        index_endscan(indexScanDesc);
    }
    if !indexRelationDesc.is_null() {
        index_close(indexRelationDesc, NoLock as LOCKMODE);
    }
}

/* ----------------------------------------------------------------
 *		ExecIndexMarkPos
 *
 * Note: we assume that no caller attempts to set a mark before having read
 * at least one tuple.  Otherwise, iss_ScanDesc might still be NULL.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexMarkPos(node: *mut IndexScanState) {
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
        let scanrelid: Index = (*((*node).ss.ps.plan as *mut Scan)).scanrelid;

        Assert!(scanrelid > 0);
        if !(*(*epqstate).relsubs_slot.offset((scanrelid - 1) as isize)).is_null()
            || !(*(*epqstate).relsubs_rowmark.offset((scanrelid - 1) as isize)).is_null()
        {
            /* Verify the claim above */
            if !*(*epqstate).relsubs_done.offset((scanrelid - 1) as isize) {
                elog!(ERROR, "unexpected ExecIndexMarkPos call in EPQ recheck");
            }
            return;
        }
    }

    index_markpos((*node).iss_ScanDesc as IndexScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexRestrPos(node: *mut IndexScanState) {
    let estate: *mut EState = (*node).ss.ps.state;
    let epqstate: *mut EPQState = (*estate).es_epq_active;

    if !(*estate).es_epq_active.is_null() {
        /* See comments in ExecIndexMarkPos */
        let scanrelid: Index = (*((*node).ss.ps.plan as *mut Scan)).scanrelid;

        Assert!(scanrelid > 0);
        if !(*(*epqstate).relsubs_slot.offset((scanrelid - 1) as isize)).is_null()
            || !(*(*epqstate).relsubs_rowmark.offset((scanrelid - 1) as isize)).is_null()
        {
            /* Verify the claim above */
            if !*(*epqstate).relsubs_done.offset((scanrelid - 1) as isize) {
                elog!(ERROR, "unexpected ExecIndexRestrPos call in EPQ recheck");
            }
            return;
        }
    }

    index_restrpos((*node).iss_ScanDesc as IndexScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitIndexScan(
    node: *mut IndexScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut IndexScanState {
    let indexstate: *mut IndexScanState;
    let currentRelation: Relation;
    let lockmode: LOCKMODE;

    /*
     * create state structure
     */
    indexstate = makeNode!(IndexScanState, T_IndexScanState);
    (*indexstate).ss.ps.plan = node as *mut Plan;
    (*indexstate).ss.ps.state = estate;
    (*indexstate).ss.ps.ExecProcNode = Some(ExecIndexScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &raw mut (*indexstate).ss.ps);

    /*
     * open the scan relation
     */
    currentRelation = ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    (*indexstate).ss.ss_currentRelation = currentRelation;
    (*indexstate).ss.ss_currentScanDesc = ptr::null_mut(); /* no heap scan here */

    /*
     * get the scan type from the relation descriptor.
     */
    ExecInitScanTupleSlot(
        estate,
        &raw mut (*indexstate).ss,
        RelationGetDescr(currentRelation),
        table_slot_callbacks(currentRelation),
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&raw mut (*indexstate).ss.ps);
    ExecAssignScanProjectionInfo(&raw mut (*indexstate).ss);

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).  Likewise for
     * indexorderby, if any.  But the indexqualorig expression is always
     * initialized even though it will only be used in some uncommon cases ---
     * would be nice to improve that.  (Problem is that any SubPlans present
     * in the expression must be found now...)
     */
    (*indexstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, indexstate as *mut PlanState);
    (*indexstate).indexqualorig =
        ExecInitQual((*node).indexqualorig, indexstate as *mut PlanState);
    (*indexstate).indexorderbyorig =
        ExecInitExprList((*node).indexorderbyorig, indexstate as *mut PlanState);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return indexstate;
    }

    /* Open the index relation. */
    lockmode = (*exec_rt_fetch((*node).scan.scanrelid, estate)).rellockmode as LOCKMODE;
    (*indexstate).iss_RelationDesc = index_open((*node).indexid, lockmode);

    /*
     * Initialize index-specific scan state
     */
    (*indexstate).iss_RuntimeKeysReady = false;
    (*indexstate).iss_RuntimeKeys = ptr::null_mut();
    (*indexstate).iss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys(
        indexstate as *mut PlanState,
        (*indexstate).iss_RelationDesc,
        (*node).indexqual,
        false,
        &raw mut (*indexstate).iss_ScanKeys,
        &raw mut (*indexstate).iss_NumScanKeys,
        &raw mut (*indexstate).iss_RuntimeKeys,
        &raw mut (*indexstate).iss_NumRuntimeKeys,
        ptr::null_mut(), /* no ArrayKeys */
        ptr::null_mut(),
    );

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys(
        indexstate as *mut PlanState,
        (*indexstate).iss_RelationDesc,
        (*node).indexorderby,
        true,
        &raw mut (*indexstate).iss_OrderByKeys,
        &raw mut (*indexstate).iss_NumOrderByKeys,
        &raw mut (*indexstate).iss_RuntimeKeys,
        &raw mut (*indexstate).iss_NumRuntimeKeys,
        ptr::null_mut(), /* no ArrayKeys */
        ptr::null_mut(),
    );

    /* Initialize sort support, if we need to re-check ORDER BY exprs */
    if (*indexstate).iss_NumOrderByKeys > 0 {
        let numOrderByKeys: c_int = (*indexstate).iss_NumOrderByKeys;
        let mut i: c_int;

        /*
         * Prepare sort support, and look up the data type for each ORDER BY
         * expression.
         */
        Assert!(numOrderByKeys == list_length((*node).indexorderbyops));
        Assert!(numOrderByKeys == list_length((*node).indexorderbyorig));
        (*indexstate).iss_SortSupport = palloc0(
            numOrderByKeys as usize * size_of::<SortSupportData>(),
        ) as *mut SortSupportData;
        (*indexstate).iss_OrderByTypByVals =
            palloc(numOrderByKeys as usize * size_of::<bool>()) as *mut bool;
        (*indexstate).iss_OrderByTypLens =
            palloc(numOrderByKeys as usize * size_of::<int16>()) as *mut int16;
        i = 0;
        forboth!(lco, (*node).indexorderbyops, lcx, (*node).indexorderbyorig, {
            let orderbyop: Oid = lfirst_oid(lco);
            let orderbyexpr: *mut Node = lfirst(lcx) as *mut Node;
            let orderbyType: Oid = exprType(orderbyexpr);
            let orderbyColl: Oid = exprCollation(orderbyexpr);
            let orderbysort: SortSupport = (*indexstate).iss_SortSupport.offset(i as isize);

            /* Initialize sort support */
            (*orderbysort).ssup_cxt = CurrentMemoryContext;
            (*orderbysort).ssup_collation = orderbyColl;
            /* See cmp_orderbyvals() comments on NULLS LAST */
            (*orderbysort).ssup_nulls_first = false;
            /* ssup_attno is unused here and elsewhere */
            (*orderbysort).ssup_attno = 0;
            /* No abbreviation */
            (*orderbysort).abbreviate = false;
            PrepareSortSupportFromOrderingOp(orderbyop, orderbysort);

            get_typlenbyval(
                orderbyType,
                (*indexstate).iss_OrderByTypLens.offset(i as isize),
                (*indexstate).iss_OrderByTypByVals.offset(i as isize),
            );
            i += 1;
        });

        /* allocate arrays to hold the re-calculated distances */
        (*indexstate).iss_OrderByValues =
            palloc(numOrderByKeys as usize * size_of::<Datum>()) as *mut Datum;
        (*indexstate).iss_OrderByNulls =
            palloc(numOrderByKeys as usize * size_of::<bool>()) as *mut bool;

        /* and initialize the reorder queue */
        (*indexstate).iss_ReorderQueue =
            pairingheap_allocate(reorderqueue_cmp, indexstate as *mut c_void);
    }

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (*indexstate).iss_NumRuntimeKeys != 0 {
        let stdecontext: *mut ExprContext = (*indexstate).ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &raw mut (*indexstate).ss.ps);
        (*indexstate).iss_RuntimeContext = (*indexstate).ss.ps.ps_ExprContext;
        (*indexstate).ss.ps.ps_ExprContext = stdecontext;
    } else {
        (*indexstate).iss_RuntimeContext = ptr::null_mut();
    }

    /*
     * all done.
     */
    indexstate
}

/*
 * ExecIndexBuildScanKeys
 *		Build the index scan keys from the index qualification expressions
 *
 * The index quals are passed to the index AM in the form of a ScanKey array.
 * This routine sets up the ScanKeys, fills in all constant fields of the
 * ScanKeys, and prepares information about the keys that have non-constant
 * comparison values.  We divide index qual expressions into five types:
 *
 * 1. Simple operator with constant comparison value ("indexkey op constant").
 * For these, we just fill in a ScanKey containing the constant value.
 *
 * 2. Simple operator with non-constant value ("indexkey op expression").
 * For these, we create a ScanKey with everything filled in except the
 * expression value, and set up an IndexRuntimeKeyInfo struct to drive
 * evaluation of the expression at the right times.
 *
 * 3. RowCompareExpr ("(indexkey, indexkey, ...) op (expr, expr, ...)").
 * For these, we create a header ScanKey plus a subsidiary ScanKey array,
 * as specified in access/skey.h.  The elements of the row comparison
 * can have either constant or non-constant comparison values.
 *
 * 4. ScalarArrayOpExpr ("indexkey op ANY (array-expression)").  If the index
 * supports amsearcharray, we handle these the same as simple operators,
 * setting the SK_SEARCHARRAY flag to tell the AM to handle them.  Otherwise,
 * we create a ScanKey with everything filled in except the comparison value,
 * and set up an IndexArrayKeyInfo struct to drive processing of the qual.
 * (Note that if we use an IndexArrayKeyInfo struct, the array expression is
 * always treated as requiring runtime evaluation, even if it's a constant.)
 *
 * 5. NullTest ("indexkey IS NULL/IS NOT NULL").  We just fill in the
 * ScanKey properly.
 *
 * This code is also used to prepare ORDER BY expressions for amcanorderbyop
 * indexes.  The behavior is exactly the same, except that we have to look up
 * the operator differently.  Note that only cases 1 and 2 are currently
 * possible for ORDER BY.
 *
 * Input params are:
 *
 * planstate: executor state node we are working for
 * index: the index we are building scan keys for
 * quals: indexquals (or indexorderbys) expressions
 * isorderby: true if processing ORDER BY exprs, false if processing quals
 * *runtimeKeys: ptr to pre-existing IndexRuntimeKeyInfos, or NULL if none
 * *numRuntimeKeys: number of pre-existing runtime keys
 *
 * Output params are:
 *
 * *scanKeys: receives ptr to array of ScanKeys
 * *numScanKeys: receives number of scankeys
 * *runtimeKeys: receives ptr to array of IndexRuntimeKeyInfos, or NULL if none
 * *numRuntimeKeys: receives number of runtime keys
 * *arrayKeys: receives ptr to array of IndexArrayKeyInfos, or NULL if none
 * *numArrayKeys: receives number of array keys
 *
 * Caller may pass NULL for arrayKeys and numArrayKeys to indicate that
 * IndexArrayKeyInfos are not supported.
 */
pub unsafe fn ExecIndexBuildScanKeys(
    planstate: *mut PlanState,
    index: Relation,
    quals: *mut List,
    isorderby: bool,
    scanKeys: *mut *mut ScanKeyData,
    numScanKeys: *mut c_int,
    runtimeKeys: *mut *mut IndexRuntimeKeyInfo,
    numRuntimeKeys: *mut c_int,
    arrayKeys: *mut *mut IndexArrayKeyInfo,
    numArrayKeys: *mut c_int,
) {
    let qual_cell: *mut ListCell;
    let scan_keys: *mut ScanKeyData;
    let mut runtime_keys: *mut IndexRuntimeKeyInfo;
    let mut array_keys: *mut IndexArrayKeyInfo;
    let n_scan_keys: c_int;
    let mut n_runtime_keys: c_int;
    let mut max_runtime_keys: c_int;
    let mut n_array_keys: c_int;
    let mut j: c_int;

    /* Allocate array for ScanKey structs: one per qual */
    n_scan_keys = list_length(quals);
    scan_keys = palloc(n_scan_keys as usize * size_of::<ScanKeyData>()) as *mut ScanKeyData;

    /*
     * runtime_keys array is dynamically resized as needed.  We handle it this
     * way so that the same runtime keys array can be shared between
     * indexquals and indexorderbys, which will be processed in separate calls
     * of this function.  Caller must be sure to pass in NULL/0 for first
     * call.
     */
    runtime_keys = *runtimeKeys;
    n_runtime_keys = *numRuntimeKeys;
    max_runtime_keys = n_runtime_keys;

    /* Allocate array_keys as large as it could possibly need to be */
    array_keys =
        palloc0(n_scan_keys as usize * size_of::<IndexArrayKeyInfo>()) as *mut IndexArrayKeyInfo;
    n_array_keys = 0;

    /*
     * for each opclause in the given qual, convert the opclause into a single
     * scan key
     */
    j = 0;
    foreach!(qual_cell, quals, {
        let clause: *mut Expr = lfirst(crate::current_cell!(qual_cell)) as *mut Expr;
        let this_scan_key: *mut ScanKeyData = scan_keys.offset(j as isize);
        j += 1;
        #[allow(unused_assignments)]
        let mut opno: Oid; /* operator's OID */
        #[allow(unused_assignments)]
        let mut opfuncid: RegProcedure; /* operator proc id used in scan */
        #[allow(unused_assignments)]
        let mut opfamily: Oid; /* opfamily of index column */
        let mut op_strategy: c_int = 0; /* operator's strategy number */
        let mut op_lefttype: Oid = 0; /* operator's declared input types */
        let mut op_righttype: Oid = 0;
        let mut leftop: *mut Expr; /* expr on lhs of operator */
        let mut rightop: *mut Expr; /* expr on rhs ... */
        #[allow(unused_assignments)]
        let mut varattno: AttrNumber; /* att number used in scan */
        let indnkeyatts: c_int;

        indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
        if IsA!(clause, T_OpExpr) {
            /* indexkey op const or indexkey op expression */
            let mut flags: c_int = 0;
            let scanvalue: Datum;

            opno = (*(clause as *mut OpExpr)).opno;
            opfuncid = (*(clause as *mut OpExpr)).opfuncid as RegProcedure;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = get_leftop(clause);

            if !leftop.is_null() && IsA!(leftop, T_RelabelType) {
                leftop = (*(leftop as *mut RelabelType)).arg;
            }

            Assert!(!leftop.is_null());

            if !(IsA!(leftop, T_Var) && (*(leftop as *mut Var)).varno == INDEX_VAR) {
                elog!(ERROR, "indexqual doesn't have key on left side");
            }

            varattno = (*(leftop as *mut Var)).varattno;
            if varattno < 1 || varattno as c_int > indnkeyatts {
                elog!(ERROR, "bogus index qualification");
            }

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = rel_opfamily(index, (varattno - 1) as c_int);

            get_op_opfamily_properties(
                opno,
                opfamily,
                isorderby,
                &raw mut op_strategy,
                &raw mut op_lefttype,
                &raw mut op_righttype,
            );

            if isorderby {
                flags |= SK_ORDER_BY;
            }

            /*
             * rightop is the constant or variable comparison value
             */
            rightop = get_rightop(clause);

            if !rightop.is_null() && IsA!(rightop, T_RelabelType) {
                rightop = (*(rightop as *mut RelabelType)).arg;
            }

            Assert!(!rightop.is_null());

            if IsA!(rightop, T_Const) {
                /* OK, simple constant comparison value */
                scanvalue = (*(rightop as *mut Const)).constvalue;
                if (*(rightop as *mut Const)).constisnull {
                    flags |= SK_ISNULL;
                }
            } else {
                /* Need to treat this one as a runtime key */
                if n_runtime_keys >= max_runtime_keys {
                    if max_runtime_keys == 0 {
                        max_runtime_keys = 8;
                        runtime_keys = palloc(
                            max_runtime_keys as usize * size_of::<IndexRuntimeKeyInfo>(),
                        ) as *mut IndexRuntimeKeyInfo;
                    } else {
                        max_runtime_keys *= 2;
                        runtime_keys = repalloc(
                            runtime_keys as *mut c_void,
                            max_runtime_keys as usize * size_of::<IndexRuntimeKeyInfo>(),
                        ) as *mut IndexRuntimeKeyInfo;
                    }
                }
                (*runtime_keys.offset(n_runtime_keys as isize)).scan_key = this_scan_key;
                (*runtime_keys.offset(n_runtime_keys as isize)).key_expr =
                    ExecInitExpr(rightop, planstate);
                (*runtime_keys.offset(n_runtime_keys as isize)).key_toastable =
                    TypeIsToastable(op_righttype);
                n_runtime_keys += 1;
                scanvalue = 0 as Datum;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize_op(
                this_scan_key,
                flags,
                varattno,                                /* attribute number to scan */
                op_strategy as StrategyNumber,           /* op's strategy */
                op_righttype,                            /* strategy subtype */
                (*(clause as *mut OpExpr)).inputcollid,  /* collation */
                opfuncid,                                /* reg proc to use */
                scanvalue,                               /* constant */
            );
        } else if IsA!(clause, T_RowCompareExpr) {
            /* (indexkey, indexkey, ...) op (expression, expression, ...) */
            let rc: *mut RowCompareExpr = clause as *mut RowCompareExpr;
            let first_sub_key: *mut ScanKeyData;
            let mut n_sub_key: c_int;

            Assert!(!isorderby);

            first_sub_key =
                palloc(list_length((*rc).opnos) as usize * size_of::<ScanKeyData>())
                    as *mut ScanKeyData;
            n_sub_key = 0;

            /* Scan RowCompare columns and generate subsidiary ScanKey items */
            forfour!(
                largs_cell, (*rc).largs,
                rargs_cell, (*rc).rargs,
                opnos_cell, (*rc).opnos,
                collids_cell, (*rc).inputcollids,
                {
                    let this_sub_key: *mut ScanKeyData =
                        first_sub_key.offset(n_sub_key as isize);
                    let mut flags: c_int = SK_ROW_MEMBER;
                    let scanvalue: Datum;
                    let inputcollation: Oid;

                    leftop = lfirst(largs_cell) as *mut Expr;
                    rightop = lfirst(rargs_cell) as *mut Expr;
                    opno = lfirst_oid(opnos_cell);
                    inputcollation = lfirst_oid(collids_cell);

                    /*
                     * leftop should be the index key Var, possibly relabeled
                     */
                    if !leftop.is_null() && IsA!(leftop, T_RelabelType) {
                        leftop = (*(leftop as *mut RelabelType)).arg;
                    }

                    Assert!(!leftop.is_null());

                    if !(IsA!(leftop, T_Var) && (*(leftop as *mut Var)).varno == INDEX_VAR) {
                        elog!(ERROR, "indexqual doesn't have key on left side");
                    }

                    varattno = (*(leftop as *mut Var)).varattno;

                    /*
                     * We have to look up the operator's associated support
                     * function
                     */
                    if !rel_amcanorder(index)
                        || varattno < 1
                        || varattno as c_int > indnkeyatts
                    {
                        elog!(ERROR, "bogus RowCompare index qualification");
                    }
                    opfamily = rel_opfamily(index, (varattno - 1) as c_int);

                    get_op_opfamily_properties(
                        opno,
                        opfamily,
                        isorderby,
                        &raw mut op_strategy,
                        &raw mut op_lefttype,
                        &raw mut op_righttype,
                    );

                    if op_strategy != (*rc).cmptype {
                        elog!(
                            ERROR,
                            "RowCompare index qualification contains wrong operator"
                        );
                    }

                    opfuncid =
                        get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC)
                            as RegProcedure;
                    if !RegProcedureIsValid(opfuncid) {
                        elog!(
                            ERROR,
                            "missing support function {}({},{}) in opfamily {}",
                            BTORDER_PROC,
                            op_lefttype,
                            op_righttype,
                            opfamily
                        );
                    }

                    /*
                     * rightop is the constant or variable comparison value
                     */
                    if !rightop.is_null() && IsA!(rightop, T_RelabelType) {
                        rightop = (*(rightop as *mut RelabelType)).arg;
                    }

                    Assert!(!rightop.is_null());

                    if IsA!(rightop, T_Const) {
                        /* OK, simple constant comparison value */
                        scanvalue = (*(rightop as *mut Const)).constvalue;
                        if (*(rightop as *mut Const)).constisnull {
                            flags |= SK_ISNULL;
                        }
                    } else {
                        /* Need to treat this one as a runtime key */
                        if n_runtime_keys >= max_runtime_keys {
                            if max_runtime_keys == 0 {
                                max_runtime_keys = 8;
                                runtime_keys = palloc(
                                    max_runtime_keys as usize
                                        * size_of::<IndexRuntimeKeyInfo>(),
                                ) as *mut IndexRuntimeKeyInfo;
                            } else {
                                max_runtime_keys *= 2;
                                runtime_keys = repalloc(
                                    runtime_keys as *mut c_void,
                                    max_runtime_keys as usize
                                        * size_of::<IndexRuntimeKeyInfo>(),
                                ) as *mut IndexRuntimeKeyInfo;
                            }
                        }
                        (*runtime_keys.offset(n_runtime_keys as isize)).scan_key = this_sub_key;
                        (*runtime_keys.offset(n_runtime_keys as isize)).key_expr =
                            ExecInitExpr(rightop, planstate);
                        (*runtime_keys.offset(n_runtime_keys as isize)).key_toastable =
                            TypeIsToastable(op_righttype);
                        n_runtime_keys += 1;
                        scanvalue = 0 as Datum;
                    }

                    /*
                     * initialize the subsidiary scan key's fields appropriately
                     */
                    ScanKeyEntryInitialize_op(
                        this_sub_key,
                        flags,
                        varattno,                       /* attribute number */
                        op_strategy as StrategyNumber,  /* op's strategy */
                        op_righttype,                   /* strategy subtype */
                        inputcollation,                 /* collation */
                        opfuncid,                       /* reg proc to use */
                        scanvalue,                      /* constant */
                    );
                    n_sub_key += 1;
                }
            );

            /* Mark the last subsidiary scankey correctly */
            {
                let last: *mut ScanKeyData = first_sub_key.offset((n_sub_key - 1) as isize);
                skey_set_sk_flags(last, skey_sk_flags(last) | SK_ROW_END);
            }

            /*
             * We don't use ScanKeyEntryInitialize for the header because it
             * isn't going to contain a valid sk_func pointer.
             */
            MemSet(this_scan_key as *mut c_void, 0, size_of::<ScanKeyData>());
            skey_set_sk_flags(this_scan_key, SK_ROW_HEADER);
            skey_set_sk_attno(this_scan_key, skey_sk_attno(first_sub_key));
            skey_set_sk_strategy(this_scan_key, (*rc).cmptype as StrategyNumber);
            /* sk_subtype, sk_collation, sk_func not used in a header */
            skey_set_sk_argument(this_scan_key, PointerGetDatum(first_sub_key as *const c_void));
        } else if IsA!(clause, T_ScalarArrayOpExpr) {
            /* indexkey op ANY (array-expression) */
            let saop: *mut ScalarArrayOpExpr = clause as *mut ScalarArrayOpExpr;
            let mut flags: c_int = 0;
            let scanvalue: Datum;

            Assert!(!isorderby);

            Assert!((*saop).useOr);
            opno = (*saop).opno;
            opfuncid = (*saop).opfuncid as RegProcedure;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = linitial((*saop).args) as *mut Expr;

            if !leftop.is_null() && IsA!(leftop, T_RelabelType) {
                leftop = (*(leftop as *mut RelabelType)).arg;
            }

            Assert!(!leftop.is_null());

            if !(IsA!(leftop, T_Var) && (*(leftop as *mut Var)).varno == INDEX_VAR) {
                elog!(ERROR, "indexqual doesn't have key on left side");
            }

            varattno = (*(leftop as *mut Var)).varattno;
            if varattno < 1 || varattno as c_int > indnkeyatts {
                elog!(ERROR, "bogus index qualification");
            }

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = rel_opfamily(index, (varattno - 1) as c_int);

            get_op_opfamily_properties(
                opno,
                opfamily,
                isorderby,
                &raw mut op_strategy,
                &raw mut op_lefttype,
                &raw mut op_righttype,
            );

            /*
             * rightop is the constant or variable array value
             */
            rightop = lsecond((*saop).args) as *mut Expr;

            if !rightop.is_null() && IsA!(rightop, T_RelabelType) {
                rightop = (*(rightop as *mut RelabelType)).arg;
            }

            Assert!(!rightop.is_null());

            if rel_amsearcharray(index) {
                /* Index AM will handle this like a simple operator */
                flags |= SK_SEARCHARRAY;
                if IsA!(rightop, T_Const) {
                    /* OK, simple constant comparison value */
                    scanvalue = (*(rightop as *mut Const)).constvalue;
                    if (*(rightop as *mut Const)).constisnull {
                        flags |= SK_ISNULL;
                    }
                } else {
                    /* Need to treat this one as a runtime key */
                    if n_runtime_keys >= max_runtime_keys {
                        if max_runtime_keys == 0 {
                            max_runtime_keys = 8;
                            runtime_keys = palloc(
                                max_runtime_keys as usize * size_of::<IndexRuntimeKeyInfo>(),
                            ) as *mut IndexRuntimeKeyInfo;
                        } else {
                            max_runtime_keys *= 2;
                            runtime_keys = repalloc(
                                runtime_keys as *mut c_void,
                                max_runtime_keys as usize * size_of::<IndexRuntimeKeyInfo>(),
                            ) as *mut IndexRuntimeKeyInfo;
                        }
                    }
                    (*runtime_keys.offset(n_runtime_keys as isize)).scan_key = this_scan_key;
                    (*runtime_keys.offset(n_runtime_keys as isize)).key_expr =
                        ExecInitExpr(rightop, planstate);

                    /*
                     * Careful here: the runtime expression is not of
                     * op_righttype, but rather is an array of same; so
                     * TypeIsToastable() isn't helpful.  However, we can
                     * assume that all array types are toastable.
                     */
                    (*runtime_keys.offset(n_runtime_keys as isize)).key_toastable = true;
                    n_runtime_keys += 1;
                    scanvalue = 0 as Datum;
                }
            } else {
                /* Executor has to expand the array value */
                (*array_keys.offset(n_array_keys as isize)).scan_key = this_scan_key;
                (*array_keys.offset(n_array_keys as isize)).array_expr =
                    ExecInitExpr(rightop, planstate);
                /* the remaining fields were zeroed by palloc0 */
                n_array_keys += 1;
                scanvalue = 0 as Datum;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize_op(
                this_scan_key,
                flags,
                varattno,                       /* attribute number to scan */
                op_strategy as StrategyNumber,  /* op's strategy */
                op_righttype,                   /* strategy subtype */
                (*saop).inputcollid,            /* collation */
                opfuncid,                       /* reg proc to use */
                scanvalue,                      /* constant */
            );
        } else if IsA!(clause, T_NullTest) {
            /* indexkey IS NULL or indexkey IS NOT NULL */
            let ntest: *mut NullTest = clause as *mut NullTest;
            let flags: c_int;

            Assert!(!isorderby);

            /*
             * argument should be the index key Var, possibly relabeled
             */
            leftop = (*ntest).arg;

            if !leftop.is_null() && IsA!(leftop, T_RelabelType) {
                leftop = (*(leftop as *mut RelabelType)).arg;
            }

            Assert!(!leftop.is_null());

            if !(IsA!(leftop, T_Var) && (*(leftop as *mut Var)).varno == INDEX_VAR) {
                elog!(ERROR, "NullTest indexqual has wrong key");
            }

            varattno = (*(leftop as *mut Var)).varattno;

            /*
             * initialize the scan key's fields appropriately
             */
            match (*ntest).nulltesttype {
                NullTestType::IS_NULL => {
                    flags = SK_ISNULL | SK_SEARCHNULL;
                }
                NullTestType::IS_NOT_NULL => {
                    flags = SK_ISNULL | SK_SEARCHNOTNULL;
                }
                #[allow(unreachable_patterns)]
                _ => {
                    elog!(
                        ERROR,
                        "unrecognized nulltesttype: {}",
                        (*ntest).nulltesttype as c_int
                    );
                    flags = 0; /* keep compiler quiet */
                }
            }

            ScanKeyEntryInitialize_op(
                this_scan_key,
                flags,
                varattno,             /* attribute number to scan */
                InvalidStrategy,      /* no strategy */
                InvalidOid,           /* no strategy subtype */
                InvalidOid,           /* no collation */
                InvalidOid as RegProcedure, /* no reg proc for this */
                0 as Datum,           /* constant */
            );
        } else {
            elog!(
                ERROR,
                "unsupported indexqual type: {}",
                nodeTag(clause) as c_int
            );
        }
    });

    Assert!(n_runtime_keys <= max_runtime_keys);

    /* Get rid of any unused arrays */
    if n_array_keys == 0 {
        pfree(array_keys as *mut c_void);
        array_keys = ptr::null_mut();
    }

    /*
     * Return info to our caller.
     */
    *scanKeys = scan_keys;
    *numScanKeys = n_scan_keys;
    *runtimeKeys = runtime_keys;
    *numRuntimeKeys = n_runtime_keys;
    if !arrayKeys.is_null() {
        *arrayKeys = array_keys;
        *numArrayKeys = n_array_keys;
    } else if n_array_keys != 0 {
        elog!(ERROR, "ScalarArrayOpExpr index qual found where not allowed");
    }
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecIndexScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexScanEstimate(node: *mut IndexScanState, pcxt: *mut ParallelContext) {
    let estate: *mut EState = (*node).ss.ps.state;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    (*node).iss_PscanLen = index_parallelscan_estimate(
        (*node).iss_RelationDesc,
        (*node).iss_NumScanKeys,
        (*node).iss_NumOrderByKeys,
        (*estate).es_snapshot as *mut c_void,
        instrument,
        parallel_aware,
        pcxt_nworkers(pcxt),
    );
    shm_toc_estimate_chunk(pcxt_estimator(pcxt), (*node).iss_PscanLen);
    shm_toc_estimate_keys(pcxt_estimator(pcxt), 1);
}

/* ----------------------------------------------------------------
 *		ExecIndexScanInitializeDSM
 *
 *		Set up a parallel index scan descriptor.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexScanInitializeDSM(node: *mut IndexScanState, pcxt: *mut ParallelContext) {
    let estate: *mut EState = (*node).ss.ps.state;
    let piscan: ParallelIndexScanDesc;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    piscan = shm_toc_allocate(pcxt_toc(pcxt), (*node).iss_PscanLen) as ParallelIndexScanDesc;
    index_parallelscan_initialize(
        (*node).ss.ss_currentRelation,
        (*node).iss_RelationDesc,
        (*estate).es_snapshot as *mut c_void,
        instrument,
        parallel_aware,
        pcxt_nworkers(pcxt),
        &raw mut (*node).iss_SharedInfo,
        piscan,
    );
    shm_toc_insert(
        pcxt_toc(pcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        piscan as *mut c_void,
    );

    if !parallel_aware {
        /* Only here to initialize SharedInfo in DSM */
        return;
    }

    (*node).iss_ScanDesc = index_beginscan_parallel(
        (*node).ss.ss_currentRelation,
        (*node).iss_RelationDesc,
        &raw mut (*node).iss_Instrument,
        (*node).iss_NumScanKeys,
        (*node).iss_NumOrderByKeys,
        piscan,
    ) as *mut _;

    /*
     * If no run-time keys to calculate or they are ready, go ahead and pass
     * the scankeys to the index AM.
     */
    if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
        index_rescan(
            (*node).iss_ScanDesc as IndexScanDesc,
            (*node).iss_ScanKeys,
            (*node).iss_NumScanKeys,
            (*node).iss_OrderByKeys,
            (*node).iss_NumOrderByKeys,
        );
    }
}

/* ----------------------------------------------------------------
 *		ExecIndexScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexScanReInitializeDSM(
    node: *mut IndexScanState,
    _pcxt: *mut ParallelContext,
) {
    Assert!((*(*node).ss.ps.plan).parallel_aware);
    index_parallelrescan((*node).iss_ScanDesc as IndexScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexScanInitializeWorker(
    node: *mut IndexScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let piscan: ParallelIndexScanDesc;
    let instrument: bool = !(*node).ss.ps.instrument.is_null();
    let parallel_aware: bool = (*(*node).ss.ps.plan).parallel_aware;

    if !instrument && !parallel_aware {
        /* No DSM required by the scan */
        return;
    }

    piscan = shm_toc_lookup(
        pwcxt_toc(pwcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        false,
    ) as ParallelIndexScanDesc;

    if instrument {
        (*node).iss_SharedInfo =
            OffsetToPointer(piscan as *mut c_void, si_ps_offset_ins(piscan))
                as *mut SharedIndexScanInstrumentation;
    }

    if !parallel_aware {
        /* Only here to set up worker node's SharedInfo */
        return;
    }

    (*node).iss_ScanDesc = index_beginscan_parallel(
        (*node).ss.ss_currentRelation,
        (*node).iss_RelationDesc,
        &raw mut (*node).iss_Instrument,
        (*node).iss_NumScanKeys,
        (*node).iss_NumOrderByKeys,
        piscan,
    ) as *mut _;

    /*
     * If no run-time keys to calculate or they are ready, go ahead and pass
     * the scankeys to the index AM.
     */
    if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
        index_rescan(
            (*node).iss_ScanDesc as IndexScanDesc,
            (*node).iss_ScanKeys,
            (*node).iss_NumScanKeys,
            (*node).iss_OrderByKeys,
            (*node).iss_NumOrderByKeys,
        );
    }
}

/* ----------------------------------------------------------------
 * ExecIndexScanRetrieveInstrumentation
 *
 *		Transfer index scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecIndexScanRetrieveInstrumentation(node: *mut IndexScanState) {
    let SharedInfo: *mut SharedIndexScanInstrumentation = (*node).iss_SharedInfo;
    let size: usize;

    if SharedInfo.is_null() {
        return;
    }

    /* Create a copy of SharedInfo in backend-local memory */
    size = offsetof_winstrument()
        + si_num_workers(SharedInfo) as usize * size_of::<IndexScanInstrumentation>();
    (*node).iss_SharedInfo = palloc(size) as *mut SharedIndexScanInstrumentation;
    ptr::copy_nonoverlapping(
        SharedInfo as *const u8,
        (*node).iss_SharedInfo as *mut u8,
        size,
    );
}

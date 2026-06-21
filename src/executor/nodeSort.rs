//! Routines to handle sorting of relations.
//!
//! Translated 1:1 from postgres/src/backend/executor/nodeSort.c
//! Companion header: postgres/src/include/executor/nodeSort.h
/*-------------------------------------------------------------------------
 *
 * nodeSort.c
 *	  Routines to handle sorting of relations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSort.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::c::{int64, uint64, Size};

use crate::access::sdir::{ForwardScanDirection, ScanDirection, ScanDirectionIsForward};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::attnum::AttrNumber;
use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecInitResultTupleSlotTL, ExecProcNode, ExecReScan,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::execUtils::{ExecCreateScanSlotFromOuterPlan, ExecGetResultType};
use crate::executor::execTuples::{ExecStoreVirtualTuple, TTSOpsMinimalTuple, TTSOpsVirtual};
use crate::executor::tuptable::{ExecClearTuple, slot_getsomeattrs, TupIsNull};
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};
use crate::nodes::execnodes::{
    outerPlanState, EState, PlanState, ScanState, SharedSortInfo, SortState, Tuplesortstate,
    TuplesortInstrumentation, TupleTableSlot,
};
use crate::nodes::plannodes::{outerPlan, Plan, Sort};

use crate::{castNode, makeNode, Assert};

// ----------------------------------------------------------------
// Stubs for not-yet-ported dependencies.
// ----------------------------------------------------------------

// Tuplesort option flags (utils/tuplesort.h).
const TUPLESORT_NONE: c_int = 0;
const TUPLESORT_RANDOMACCESS: c_int = 1 << 0;
const TUPLESORT_ALLOWBOUNDED: c_int = 1 << 1;

// access/parallel.h -- ParallelContext / ParallelWorkerContext are not yet
// ported.  Mirror the fields used here so the storage/shm_toc.h calls below
// typecheck faithfully.
#[repr(C)]
pub struct ParallelContext {
    pub nworkers: c_int,
    pub estimator: shm_toc_estimator,
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ParallelWorkerContext {
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}

// storage/shm_toc.h
#[repr(C)]
pub struct shm_toc {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct shm_toc_estimator {
    _opaque: [u8; 0],
}
unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!()
}
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!()
}
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_allocate(_toc as _, _nbytes as _) as _
}
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) {
    crate::storage::ipc::shm_toc::shm_toc_insert(_toc as _, _key as _, _address as _)
}
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_lookup(_toc as _, _key as _, _noError as _) as _
}

// common/shmem.c
unsafe fn mul_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: storage/ipc/shmem.c
}
unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

// utils/tuplesort.h
unsafe fn tuplesort_begin_datum(
    _datumType: Oid,
    _sortOperator: Oid,
    _sortCollation: Oid,
    _nullsFirstFlag: bool,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _sortopt: c_int,
) -> *mut Tuplesortstate {
    crate::utils::sort::tuplesortvariants::tuplesort_begin_datum(_datumType as _, _sortOperator as _, _sortCollation as _, _nullsFirstFlag as _, _workMem as _, _coordinate as _, _sortopt as _) as _
}
unsafe fn tuplesort_begin_heap(
    _tupDesc: TupleDesc,
    _nkeys: c_int,
    _attNums: *mut AttrNumber,
    _sortOperators: *mut Oid,
    _sortCollations: *mut Oid,
    _nullsFirstFlags: *mut bool,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _sortopt: c_int,
) -> *mut Tuplesortstate {
    crate::utils::sort::tuplesortvariants::tuplesort_begin_heap(_tupDesc as _, _nkeys as _, _attNums as _, _sortOperators as _, _sortCollations as _, _nullsFirstFlags as _, _workMem as _, _coordinate as _, _sortopt as _) as _
}
unsafe fn tuplesort_set_bound(_state: *mut Tuplesortstate, _bound: int64) {
    crate::utils::sort::tuplesort::tuplesort_set_bound(_state as _, _bound as _)
}
unsafe fn tuplesort_putdatum(_state: *mut Tuplesortstate, _val: Datum, _isNull: bool) {
    crate::utils::sort::tuplesortvariants::tuplesort_putdatum(_state as _, _val as _, _isNull as _)
}
unsafe fn tuplesort_puttupleslot(_state: *mut Tuplesortstate, _slot: *mut TupleTableSlot) {
    crate::utils::sort::tuplesortvariants::tuplesort_puttupleslot(_state as _, _slot as _)
}
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_performsort(_state as _)
}
unsafe fn tuplesort_getdatum(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _val: *mut Datum,
    _isNull: *mut bool,
    _abbrev: *mut Datum,
) -> bool {
    crate::utils::sort::tuplesortvariants::tuplesort_getdatum(_state as _, _forward as _, _copy as _, _val as _, _isNull as _, _abbrev as _) as _
}
unsafe fn tuplesort_gettupleslot(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
    _abbrev: *mut Datum,
) -> bool {
    crate::utils::sort::tuplesortvariants::tuplesort_gettupleslot(_state as _, _forward as _, _copy as _, _slot as _, _abbrev as _) as _
}
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_end(_state as _)
}
unsafe fn tuplesort_markpos(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_markpos(_state as _)
}
unsafe fn tuplesort_restorepos(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_restorepos(_state as _)
}
unsafe fn tuplesort_rescan(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_rescan(_state as _)
}
unsafe fn tuplesort_get_stats(_state: *mut Tuplesortstate, _stats: *mut TuplesortInstrumentation) {
    crate::utils::sort::tuplesort::tuplesort_get_stats(_state as _, _stats as _)
}

// access/parallel.h
unsafe fn IsParallelWorker() -> bool {
    unimplemented!() // TODO: access/parallel.h
}

// Global stubs (would be `extern` globals in C, from access/parallel.c).
#[allow(non_upper_case_globals)]
static mut ParallelWorkerNumber: c_int = 0;

/// offsetof(SharedSortInfo, sinstrument) -- the flexible-array member follows
/// num_workers in nodes/execnodes.h.
unsafe fn offsetof_SharedSortInfo_sinstrument() -> Size {
    let base = ptr::null::<SharedSortInfo>();
    (&(*base).sinstrument as *const _ as Size) - (base as Size)
}

/* ----------------------------------------------------------------
 *		ExecSort
 *
 *		Sorts tuples from the outer subtree of the node using tuplesort,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		There are two distinct ways that this sort can be performed:
 *
 *		1) When the result is a single column we perform a Datum sort.
 *
 *		2) When the result contains multiple columns we perform a tuple sort.
 *
 *		We could do this by always performing a tuple sort, however sorting
 *		Datums only can be significantly faster than sorting tuples,
 *		especially when the Datums are of a pass-by-value type.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
unsafe fn ExecSort(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SortState = castNode!(SortState, T_SortState, pstate);
    let estate: *mut EState;
    let dir: ScanDirection;
    let mut tuplesortstate: *mut Tuplesortstate;
    let mut slot: *mut TupleTableSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * get state info from node
     */
    // SO1_printf("ExecSort: %s\n", "entering routine");

    estate = (*node).ss.ps.state;
    dir = (*estate).es_direction;
    tuplesortstate = (*node).tuplesortstate as *mut Tuplesortstate;

    /*
     * If first time through, read all tuples from outer plan and pass them to
     * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
     */

    if !(*node).sort_Done {
        let plannode: *mut Sort = (*node).ss.ps.plan as *mut Sort;
        let outerNode: *mut PlanState;
        let tupDesc: TupleDesc;
        let mut tuplesortopts: c_int = TUPLESORT_NONE;

        // SO1_printf("ExecSort: %s\n", "sorting subplan");

        /*
         * Want to scan subplan in the forward direction while creating the
         * sorted data.
         */
        (*estate).es_direction = ForwardScanDirection;

        /*
         * Initialize tuplesort module.
         */
        // SO1_printf("ExecSort: %s\n", "calling tuplesort_begin");

        outerNode = outerPlanState(node as *mut PlanState);
        tupDesc = ExecGetResultType(outerNode);

        if (*node).randomAccess {
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        }
        if (*node).bounded {
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
        }

        if (*node).datumSort {
            tuplesortstate = tuplesort_begin_datum(
                (*TupleDescAttr(tupDesc, 0)).atttypid,
                *(*plannode).sortOperators.offset(0),
                *(*plannode).collations.offset(0),
                *(*plannode).nullsFirst.offset(0),
                work_mem,
                ptr::null_mut(),
                tuplesortopts,
            );
        } else {
            tuplesortstate = tuplesort_begin_heap(
                tupDesc,
                (*plannode).numCols,
                (*plannode).sortColIdx,
                (*plannode).sortOperators,
                (*plannode).collations,
                (*plannode).nullsFirst,
                work_mem,
                ptr::null_mut(),
                tuplesortopts,
            );
        }
        if (*node).bounded {
            tuplesort_set_bound(tuplesortstate, (*node).bound);
        }
        (*node).tuplesortstate = tuplesortstate as *mut c_void;

        /*
         * Scan the subplan and feed all the tuples to tuplesort using the
         * appropriate method based on the type of sort we're doing.
         */
        if (*node).datumSort {
            loop {
                slot = ExecProcNode(outerNode);

                if TupIsNull(slot) {
                    break;
                }
                slot_getsomeattrs(slot, 1);
                tuplesort_putdatum(
                    tuplesortstate,
                    *(*slot).tts_values.offset(0),
                    *(*slot).tts_isnull.offset(0),
                );
            }
        } else {
            loop {
                slot = ExecProcNode(outerNode);

                if TupIsNull(slot) {
                    break;
                }
                tuplesort_puttupleslot(tuplesortstate, slot);
            }
        }

        /*
         * Complete the sort.
         */
        tuplesort_performsort(tuplesortstate);

        /*
         * restore to user specified direction
         */
        (*estate).es_direction = dir;

        /*
         * finally set the sorted flag to true
         */
        (*node).sort_Done = true;
        (*node).bounded_Done = (*node).bounded;
        (*node).bound_Done = (*node).bound;
        if !(*node).shared_info.is_null() && (*node).am_worker {
            let si: *mut TuplesortInstrumentation;

            Assert!(IsParallelWorker());
            Assert!(ParallelWorkerNumber <= (*(*node).shared_info).num_workers);
            si = (*(*node).shared_info)
                .sinstrument
                .as_mut_ptr()
                .offset(ParallelWorkerNumber as isize);
            tuplesort_get_stats(tuplesortstate, si);
        }
        // SO1_printf("ExecSort: %s\n", "sorting done");
    }

    // SO1_printf("ExecSort: %s\n", "retrieving tuple from tuplesort");

    slot = (*node).ss.ps.ps_ResultTupleSlot;

    /*
     * Fetch the next sorted item from the appropriate tuplesort function. For
     * datum sorts we must manage the slot ourselves and leave it clear when
     * tuplesort_getdatum returns false to indicate there are no more datums.
     * For tuple sorts, tuplesort_gettupleslot manages the slot for us and
     * empties the slot when it runs out of tuples.
     */
    if (*node).datumSort {
        ExecClearTuple(slot);
        if tuplesort_getdatum(
            tuplesortstate,
            ScanDirectionIsForward(dir),
            false,
            (*slot).tts_values.offset(0),
            (*slot).tts_isnull.offset(0),
            ptr::null_mut(),
        ) {
            ExecStoreVirtualTuple(slot);
        }
    } else {
        let _ = tuplesort_gettupleslot(
            tuplesortstate,
            ScanDirectionIsForward(dir),
            false,
            slot,
            ptr::null_mut(),
        );
    }

    slot
}

/* ----------------------------------------------------------------
 *		ExecInitSort
 *
 *		Creates the run-time state information for the sort node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSort(
    node: *mut Sort,
    estate: *mut EState,
    mut eflags: c_int,
) -> *mut SortState {
    let sortstate: *mut SortState;
    let outerTupDesc: TupleDesc;

    // SO1_printf("ExecInitSort: %s\n", "initializing sort node");

    /*
     * create state structure
     */
    sortstate = makeNode!(SortState, T_SortState);
    (*sortstate).ss.ps.plan = node as *mut Plan;
    (*sortstate).ss.ps.state = estate;
    (*sortstate).ss.ps.ExecProcNode = Some(ExecSort);

    /*
     * We must have random access to the sort output to do backward scan or
     * mark/restore.  We also prefer to materialize the sort output if we
     * might be called on to rewind and replay it many times.
     */
    (*sortstate).randomAccess =
        (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) != 0;

    (*sortstate).bounded = false;
    (*sortstate).sort_Done = false;
    (*sortstate).tuplesortstate = ptr::null_mut();

    /*
     * Miscellaneous initialization
     *
     * Sort nodes don't initialize their ExprContexts because they never call
     * ExecQual or ExecProject.
     */

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags &= !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    // outerPlanState(sortstate) = ExecInitNode(outerPlan(node as *mut _) as *mut _, estate, eflags);
    // outerPlanState() is an lvalue macro in C; assign directly to lefttree.
    (*(sortstate as *mut PlanState)).lefttree = ExecInitNode(outerPlan(node as *mut _) as *mut _, estate, eflags);

    /*
     * Initialize scan slot and type.
     */
    ExecCreateScanSlotFromOuterPlan(
        estate,
        &mut (*sortstate).ss as *mut ScanState as *mut c_void,
        &TTSOpsVirtual,
    );

    /*
     * Initialize return slot and type. No need to initialize projection info
     * because this node doesn't do projections.
     */
    ExecInitResultTupleSlotTL(&mut (*sortstate).ss.ps, &TTSOpsMinimalTuple);
    (*sortstate).ss.ps.ps_ProjInfo = ptr::null_mut();

    outerTupDesc = ExecGetResultType(outerPlanState(sortstate as *mut PlanState));

    /*
     * We perform a Datum sort when we're sorting just a single column,
     * otherwise we perform a tuple sort.
     */
    if (*outerTupDesc).natts == 1 {
        (*sortstate).datumSort = true;
    } else {
        (*sortstate).datumSort = false;
    }

    // SO1_printf("ExecInitSort: %s\n", "sort node initialized");

    sortstate
}

/* ----------------------------------------------------------------
 *		ExecEndSort(node)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndSort(node: *mut SortState) {
    // SO1_printf("ExecEndSort: %s\n", "shutting down sort node");

    /*
     * Release tuplesort resources
     */
    if !(*node).tuplesortstate.is_null() {
        tuplesort_end((*node).tuplesortstate as *mut Tuplesortstate);
    }
    (*node).tuplesortstate = ptr::null_mut();

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));

    // SO1_printf("ExecEndSort: %s\n", "sort node shutdown");
}

/* ----------------------------------------------------------------
 *		ExecSortMarkPos
 *
 *		Calls tuplesort to save the current position in the sorted file.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortMarkPos(node: *mut SortState) {
    /*
     * if we haven't sorted yet, just return
     */
    if !(*node).sort_Done {
        return;
    }

    tuplesort_markpos((*node).tuplesortstate as *mut Tuplesortstate);
}

/* ----------------------------------------------------------------
 *		ExecSortRestrPos
 *
 *		Calls tuplesort to restore the last saved sort file position.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortRestrPos(node: *mut SortState) {
    /*
     * if we haven't sorted yet, just return.
     */
    if !(*node).sort_Done {
        return;
    }

    /*
     * restore the scan to the previously marked position
     */
    tuplesort_restorepos((*node).tuplesortstate as *mut Tuplesortstate);
}

pub unsafe fn ExecReScanSort(node: *mut SortState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    /*
     * If we haven't sorted yet, just return. If outerplan's chgParam is not
     * NULL then it will be re-scanned by ExecProcNode, else no reason to
     * re-scan it at all.
     */
    if !(*node).sort_Done {
        return;
    }

    /* must drop pointer to sort result tuple */
    ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);

    /*
     * If subnode is to be rescanned then we forget previous sort results; we
     * have to re-read the subplan and re-sort.  Also must re-sort if the
     * bounded-sort parameters changed or we didn't select randomAccess.
     *
     * Otherwise we can just rewind and rescan the sorted output.
     */
    if !(*outerPlan).chgParam.is_null()
        || (*node).bounded != (*node).bounded_Done
        || (*node).bound != (*node).bound_Done
        || !(*node).randomAccess
    {
        (*node).sort_Done = false;
        tuplesort_end((*node).tuplesortstate as *mut Tuplesortstate);
        (*node).tuplesortstate = ptr::null_mut();

        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (*outerPlan).chgParam.is_null() {
            ExecReScan(outerPlan);
        }
    } else {
        tuplesort_rescan((*node).tuplesortstate as *mut Tuplesortstate);
    }
}

/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSortEstimate
 *
 *		Estimate space required to propagate sort statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortEstimate(node: *mut SortState, pcxt: *mut ParallelContext) {
    let mut size: Size;

    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = mul_size(
        (*pcxt).nworkers as Size,
        std::mem::size_of::<TuplesortInstrumentation>() as Size,
    );
    size = add_size(size, offsetof_SharedSortInfo_sinstrument());
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, size);
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSortInitializeDSM
 *
 *		Initialize DSM space for sort statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortInitializeDSM(node: *mut SortState, pcxt: *mut ParallelContext) {
    let size: Size;

    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = offsetof_SharedSortInfo_sinstrument()
        + (*pcxt).nworkers as Size * std::mem::size_of::<TuplesortInstrumentation>() as Size;
    (*node).shared_info = shm_toc_allocate((*pcxt).toc, size) as *mut SharedSortInfo;
    /* ensure any unfilled slots will contain zeroes */
    ptr::write_bytes((*node).shared_info as *mut u8, 0, size);
    (*(*node).shared_info).num_workers = (*pcxt).nworkers;
    shm_toc_insert(
        (*pcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        (*node).shared_info as *mut c_void,
    );
}

/* ----------------------------------------------------------------
 *		ExecSortInitializeWorker
 *
 *		Attach worker to DSM space for sort statistics.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortInitializeWorker(node: *mut SortState, pwcxt: *mut ParallelWorkerContext) {
    (*node).shared_info = shm_toc_lookup(
        (*pwcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        true,
    ) as *mut SharedSortInfo;
    (*node).am_worker = true;
}

/* ----------------------------------------------------------------
 *		ExecSortRetrieveInstrumentation
 *
 *		Transfer sort statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSortRetrieveInstrumentation(node: *mut SortState) {
    let size: Size;
    let si: *mut SharedSortInfo;

    if (*node).shared_info.is_null() {
        return;
    }

    size = offsetof_SharedSortInfo_sinstrument()
        + (*(*node).shared_info).num_workers as Size
            * std::mem::size_of::<TuplesortInstrumentation>() as Size;
    si = palloc(size) as *mut SharedSortInfo;
    ptr::copy_nonoverlapping((*node).shared_info as *const u8, si as *mut u8, size);
    (*node).shared_info = si;
}

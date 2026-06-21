//! Translation of postgres/src/backend/executor/execUtils.c
//!
//! Miscellaneous executor utility routines.  This port covers the executor
//! *state / memory-context lifecycle* core: the EState and ExprContext
//! create/free/rescan machinery, the per-tuple memory context, and the
//! ExprContext shutdown-callback list.  Everything that needs unported
//! subsystems (relcache/tableam, partitioning, parallel, JIT, execExpr's
//! ProjectionInfo/ExprState build, typcache/syscache, the range-table /
//! result-relation handling) is STUBBED at the finest granularity with the C
//! body preserved as a comment.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h                  -> crate::prelude
//!   access/parallel.h           -> STUB: IsParallelWorker (parallel.c not ported)
//!   access/table.h              -> STUB: table_open (tableam not ported)
//!   access/tableam.h            -> STUB: table_slot_callbacks (tableam not ported)
//!   executor/executor.h         -> partially: ExprContext/EState API is here; the
//!                                  ExecInit*Slot / ExecBuildProjectionInfo callers
//!                                  are STUBBED (execExpr.c / execTuples slot-from-
//!                                  plan helpers not ported)
//!   executor/nodeModifyTable.h  -> STUB: InitResultRelInfo / ExecInitGenerated
//!   jit/jit.h                   -> STUB: jit_release_context
//!   mb/pg_wchar.h               -> STUB: pg_mbstrlen_with_len (encoding not ported)
//!   miscadmin.h                 -> STUB: GetUserId, work_mem (guc not ported)
//!   parser/parse_relation.h     -> STUB: getRTEPermissionInfo, exec_rt_fetch
//!   partitioning/partdesc.h     -> STUB: DestroyPartitionDirectory
//!   storage/lmgr.h              -> STUB: CheckRelationLockedByMe
//!   utils/builtins.h            -> STUB: namestrcmp etc (GetAttributeBy* family)
//!   utils/memutils.h            -> crate::utils::memutils (ALLOCSET_DEFAULT_SIZES),
//!                                  prelude (MemoryContext* lifecycle)
//!   utils/rel.h                 -> STUB: Relation deref (relcache not ported)
//!   utils/typcache.h            -> STUB: lookup_rowtype_tupdesc (GetAttributeBy*)
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL (translated fully):
//!     CreateExecutorState, FreeExecutorState, CreateExprContextInternal,
//!     CreateExprContext, CreateWorkExprContext (sizing real; work_mem stubbed),
//!     CreateStandaloneExprContext, FreeExprContext, ReScanExprContext,
//!     MakePerTupleExprContext, RegisterExprContextCallback,
//!     UnregisterExprContextCallback, ShutdownExprContext,
//!     GetPerTupleMemoryContext (inline helper), ResetPerTupleExprContext.
//!   STUBBED (signature real, unimplemented!() + TODO(pg-port), C body kept):
//!     ExecAssignExprContext (CreateExprContext part is real; assigning into the
//!       PlanState is fine, but PlanState is opaque so it's left as a thin wrapper),
//!     ExecGetResultType/ExecGetResultSlotOps/ExecGetCommon*SlotOps,
//!     ExecAssignProjectionInfo/ExecConditionalAssignProjectionInfo (execExpr),
//!     ExecAssignScanType/ExecCreateScanSlotFromOuterPlan,
//!     ExecRelationIsTargetRelation, ExecOpenScanRelation, ExecInitRangeTable,
//!     ExecGetRangeTableRelation, ExecInitResultRelation, UpdateChangedParamSet,
//!     executor_errposition, GetAttributeByName/GetAttributeByNum,
//!     ExecTargetListLength/ExecCleanTargetListLength,
//!     ExecGetTrigger*Slot/ExecGetReturningSlot/ExecGetAllNullSlot,
//!     ExecGetChildToRootMap/ExecGetRootToChildMap,
//!     ExecGetInsertedCols/UpdatedCols/ExtraUpdatedCols/AllUpdatedCols,
//!     GetResultRTEPermissionInfo, ExecGetResultRelCheckAsUser.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]

use crate::prelude::*; // Datum, c-types, palloc/palloc0/pfree, MemoryContext +
                       // CurrentMemoryContext/MemoryContextSwitchTo/Delete/Reset,
                       // MemoryContextAlloc, ALLOCSET_DEFAULT_SIZES, elog!/ereport!/
                       // errmsg!/errcode/Assert!, null/null_mut.

use crate::nodes::execnodes::{
    EState, ExprContext, ExprContextCallbackFunction, ExprContext_CB, PlanState, ResultRelInfo,
    Snapshot, TupleConversionMap, TupleDesc, TupleTableSlot, TupleTableSlotOps,
};
use crate::nodes::nodes::NodeTag::{T_EState, T_ExprContext};
use crate::nodes::pg_list::{lcons, lfirst, linitial, list_delete_ptr, list_head, lnext, List, NIL};
use crate::nodes::primnodes::{TargetEntry, Var};
use crate::access::common::tupdesc::TupleDescAttr;
use crate::{makeNode, AllocSetContextCreate, IsA};

// ----------------------------------------------------------------------------
// Local typedefs / constants standing in for not-yet-ported headers.
// ----------------------------------------------------------------------------

/// `ForwardScanDirection` from access/sdir.h.  `ScanDirection` is `c_int` in this
/// crate (see plannodes.rs); the enum values there are Backward=-1, NoMovement=0,
/// Forward=1.
/// TODO(pg-port): use the real ScanDirection enum when access/sdir.h is ported.
const ForwardScanDirection: crate::nodes::plannodes::ScanDirection = 1;

/// `InvalidSnapshot` from utils/snapmgr.h.  `Snapshot` is `*mut SnapshotData`
/// (opaque in execnodes.rs); the invalid snapshot is the NULL pointer.
/// TODO(pg-port): utils/snapmgr.h not ported.
const InvalidSnapshot: Snapshot = null_mut();

/// `ALLOCSET_DEFAULT_*` individual block-size hints.  memutils.rs exposes the
/// triple `ALLOCSET_DEFAULT_SIZES = (minContextSize, initBlockSize, maxBlockSize)`;
/// decompose it for CreateWorkExprContext, which uses the parts individually.
const ALLOCSET_DEFAULT_MINSIZE: Size = ALLOCSET_DEFAULT_SIZES.0;
const ALLOCSET_DEFAULT_INITSIZE: Size = ALLOCSET_DEFAULT_SIZES.1;
const ALLOCSET_DEFAULT_MAXSIZE: Size = ALLOCSET_DEFAULT_SIZES.2;

/// `NoLock` from storage/lockdefs.h.
const NoLock: crate::storage::lockdefs::LOCKMODE = 0;

/// `IsParallelWorker()` from access/parallel.h.
/// TODO(pg-port): access/parallel.h not ported.
unsafe fn IsParallelWorker() -> bool {
    false
}

// ----------------------------------------------------------------
//				 Executor state and memory management functions
// ----------------------------------------------------------------

/* ----------------
 *		CreateExecutorState
 *
 *		Create and initialize an EState node, which is the root of
 *		working storage for an entire Executor invocation.
 *
 * Principally, this creates the per-query memory context that will be
 * used to hold all working data that lives till the end of the query.
 * Note that the per-query context will become a child of the caller's
 * CurrentMemoryContext.
 * ----------------
 */
/// # Safety
/// Allocates via the current memory context; caller owns the returned EState and
/// must release it with [`FreeExecutorState`].
#[no_mangle]
pub unsafe fn CreateExecutorState() -> *mut EState {
    let estate: *mut EState;
    let qcontext: MemoryContext;
    let oldcontext: MemoryContext;

    /*
     * Create the per-query context for this Executor run.
     */
    qcontext = AllocSetContextCreate!(CurrentMemoryContext, c"ExecutorState".as_ptr(), ALLOCSET_DEFAULT_SIZES);

    /*
     * Make the EState node within the per-query context.  This way, we don't
     * need a separate pfree() operation for it at shutdown.
     */
    oldcontext = MemoryContextSwitchTo(qcontext);

    estate = makeNode!(EState, T_EState);

    /*
     * Initialize all fields of the Executor State structure
     */
    (*estate).es_direction = ForwardScanDirection;
    (*estate).es_snapshot = InvalidSnapshot; /* caller must initialize this */
    (*estate).es_crosscheck_snapshot = InvalidSnapshot; /* no crosscheck */
    (*estate).es_range_table = NIL;
    (*estate).es_range_table_size = 0;
    (*estate).es_relations = null_mut();
    (*estate).es_rowmarks = null_mut();
    (*estate).es_rteperminfos = NIL;
    (*estate).es_plannedstmt = null_mut();
    (*estate).es_part_prune_infos = NIL;

    (*estate).es_junkFilter = null_mut();

    (*estate).es_output_cid = 0 as CommandId;

    (*estate).es_result_relations = null_mut();
    (*estate).es_opened_result_relations = NIL;
    (*estate).es_tuple_routing_result_relations = NIL;
    (*estate).es_trig_target_relations = NIL;

    (*estate).es_insert_pending_result_relations = NIL;
    (*estate).es_insert_pending_modifytables = NIL;

    (*estate).es_param_list_info = null_mut();
    (*estate).es_param_exec_vals = null_mut();

    (*estate).es_queryEnv = null_mut();

    (*estate).es_query_cxt = qcontext;

    (*estate).es_tupleTable = NIL;

    (*estate).es_processed = 0;
    (*estate).es_total_processed = 0;

    (*estate).es_top_eflags = 0;
    (*estate).es_instrument = 0;
    (*estate).es_finished = false;

    (*estate).es_exprcontexts = NIL;

    (*estate).es_subplanstates = NIL;

    (*estate).es_auxmodifytables = NIL;

    (*estate).es_per_tuple_exprcontext = null_mut();

    (*estate).es_sourceText = null();

    (*estate).es_use_parallel_mode = false;
    (*estate).es_parallel_workers_to_launch = 0;
    (*estate).es_parallel_workers_launched = 0;

    (*estate).es_jit_flags = 0;
    (*estate).es_jit = null_mut();

    /*
     * Return the executor state structure
     */
    MemoryContextSwitchTo(oldcontext);

    estate
}

/* ----------------
 *		FreeExecutorState
 *
 *		Release an EState along with all remaining working storage.
 *
 * Note: this is not responsible for releasing non-memory resources, such as
 * open relations or buffer pins.  But it will shut down any still-active
 * ExprContexts within the EState and deallocate associated JITed expressions.
 * That is sufficient cleanup for situations where the EState has only been
 * used for expression evaluation, and not to run a complete Plan.
 *
 * This can be called in any memory context ... so long as it's not one
 * of the ones to be freed.
 * ----------------
 */
/// # Safety
/// `estate` must be a live EState produced by [`CreateExecutorState`].
#[no_mangle]
pub unsafe fn FreeExecutorState(estate: *mut EState) {
    /*
     * Shut down and free any remaining ExprContexts.  We do this explicitly
     * to ensure that any remaining shutdown callbacks get called (since they
     * might need to release resources that aren't simply memory within the
     * per-query memory context).
     */
    while !(*estate).es_exprcontexts.is_null() {
        /*
         * XXX: seems there ought to be a faster way to implement this than
         * repeated list_delete(), no?
         */
        FreeExprContext(
            linitial((*estate).es_exprcontexts) as *mut ExprContext,
            true,
        );
        /* FreeExprContext removed the list link for us */
    }

    /* release JIT context, if allocated */
    if !(*estate).es_jit.is_null() {
        // TODO(pg-port): needs jit/jit.h (jit_release_context).
        // jit_release_context(estate->es_jit);
        (*estate).es_jit = null_mut();
    }

    /* release partition directory, if allocated */
    if !(*estate).es_partition_directory.is_null() {
        // TODO(pg-port): needs partitioning/partdesc.h (DestroyPartitionDirectory).
        // DestroyPartitionDirectory(estate->es_partition_directory);
        (*estate).es_partition_directory = null_mut();
    }

    /*
     * Free the per-query memory context, thereby releasing all working
     * memory, including the EState node itself.
     */
    MemoryContextDelete((*estate).es_query_cxt);
}

/*
 * Internal implementation for CreateExprContext() and CreateWorkExprContext()
 * that allows control over the AllocSet parameters.
 */
/// # Safety
/// `estate` must be a live EState.  The returned ExprContext is linked into the
/// EState and freed when the EState (or the ExprContext) is freed.
unsafe fn CreateExprContextInternal(
    estate: *mut EState,
    minContextSize: Size,
    initBlockSize: Size,
    maxBlockSize: Size,
) -> *mut ExprContext {
    let econtext: *mut ExprContext;
    let oldcontext: MemoryContext;

    /* Create the ExprContext node within the per-query memory context */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    econtext = makeNode!(ExprContext, T_ExprContext);

    /* Initialize fields of ExprContext */
    (*econtext).ecxt_scantuple = null_mut();
    (*econtext).ecxt_innertuple = null_mut();
    (*econtext).ecxt_outertuple = null_mut();

    (*econtext).ecxt_per_query_memory = (*estate).es_query_cxt;

    /*
     * Create working memory for expression evaluation in this context.
     */
    (*econtext).ecxt_per_tuple_memory = AllocSetContextCreate!(
        (*estate).es_query_cxt,
        c"ExprContext".as_ptr(),
        minContextSize,
        initBlockSize,
        maxBlockSize
    );

    (*econtext).ecxt_param_exec_vals = (*estate).es_param_exec_vals;
    (*econtext).ecxt_param_list_info = (*estate).es_param_list_info;

    (*econtext).ecxt_aggvalues = null_mut();
    (*econtext).ecxt_aggnulls = null_mut();

    (*econtext).caseValue_datum = 0 as Datum;
    (*econtext).caseValue_isNull = true;

    (*econtext).domainValue_datum = 0 as Datum;
    (*econtext).domainValue_isNull = true;

    (*econtext).ecxt_estate = estate;

    (*econtext).ecxt_callbacks = null_mut();

    /*
     * Link the ExprContext into the EState to ensure it is shut down when the
     * EState is freed.  Because we use lcons(), shutdowns will occur in
     * reverse order of creation, which may not be essential but can't hurt.
     */
    (*estate).es_exprcontexts = lcons(econtext as *mut c_void, (*estate).es_exprcontexts);

    MemoryContextSwitchTo(oldcontext);

    econtext
}

/* ----------------
 *		CreateExprContext
 *
 *		Create a context for expression evaluation within an EState.
 *
 * An executor run may require multiple ExprContexts (we usually make one
 * for each Plan node, and a separate one for per-output-tuple processing
 * such as constraint checking).  Each ExprContext has its own "per-tuple"
 * memory context.
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
/// # Safety
/// See [`CreateExprContextInternal`].
#[no_mangle]
pub unsafe fn CreateExprContext(estate: *mut EState) -> *mut ExprContext {
    CreateExprContextInternal(
        estate,
        ALLOCSET_DEFAULT_SIZES.0,
        ALLOCSET_DEFAULT_SIZES.1,
        ALLOCSET_DEFAULT_SIZES.2,
    )
}

/* ----------------
 *		CreateWorkExprContext
 *
 * Like CreateExprContext, but specifies the AllocSet sizes to be reasonable
 * in proportion to work_mem. If the maximum block allocation size is too
 * large, it's easy to skip right past work_mem with a single allocation.
 * ----------------
 */
/// # Safety
/// See [`CreateExprContextInternal`].
pub unsafe fn CreateWorkExprContext(estate: *mut EState) -> *mut ExprContext {
    let mut maxBlockSize: Size = ALLOCSET_DEFAULT_MAXSIZE;

    /*
     * C: maxBlockSize = pg_prevpower2_size_t(work_mem * (Size) 1024 / 16);
     *
     * work_mem is a GUC (utils/guc.c, miscadmin.h) that is not yet ported.
     * The block-sizing math itself is real; we substitute a stand-in work_mem
     * equal to the default 4 MB (4096 KB) so the sizing logic still runs and
     * produces a sensible bound.
     * TODO(pg-port): read the real work_mem GUC once guc.c is ported.
     */
    let work_mem: Size = 4096; /* KB; default work_mem */
    maxBlockSize =
        crate::port::pg_bitutils::pg_prevpower2_size_t((work_mem * 1024 / 16) as u64) as Size;

    /* But no bigger than ALLOCSET_DEFAULT_MAXSIZE */
    maxBlockSize = Min(maxBlockSize, ALLOCSET_DEFAULT_MAXSIZE);

    /* and no smaller than ALLOCSET_DEFAULT_INITSIZE */
    maxBlockSize = Max(maxBlockSize, ALLOCSET_DEFAULT_INITSIZE);

    CreateExprContextInternal(
        estate,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        maxBlockSize,
    )
}

/* ----------------
 *		CreateStandaloneExprContext
 *
 *		Create a context for standalone expression evaluation.
 *
 * An ExprContext made this way can be used for evaluation of expressions
 * that contain no Params, subplans, or Var references (it might work to
 * put tuple references into the scantuple field, but it seems unwise).
 *
 * The ExprContext struct is allocated in the caller's current memory
 * context, which also becomes its "per query" context.
 *
 * It is caller's responsibility to free the ExprContext when done,
 * or at least ensure that any shutdown callbacks have been called
 * (ReScanExprContext() is suitable).  Otherwise, non-memory resources
 * might be leaked.
 * ----------------
 */
/// # Safety
/// Allocates in the current memory context; caller must free with
/// [`FreeExprContext`] (or at least run shutdown callbacks via
/// [`ReScanExprContext`]).
pub unsafe fn CreateStandaloneExprContext() -> *mut ExprContext {
    let econtext: *mut ExprContext;

    /* Create the ExprContext node within the caller's memory context */
    econtext = makeNode!(ExprContext, T_ExprContext);

    /* Initialize fields of ExprContext */
    (*econtext).ecxt_scantuple = null_mut();
    (*econtext).ecxt_innertuple = null_mut();
    (*econtext).ecxt_outertuple = null_mut();

    (*econtext).ecxt_per_query_memory = CurrentMemoryContext;

    /*
     * Create working memory for expression evaluation in this context.
     */
    (*econtext).ecxt_per_tuple_memory =
        AllocSetContextCreate!(CurrentMemoryContext, c"ExprContext".as_ptr(), ALLOCSET_DEFAULT_SIZES);

    (*econtext).ecxt_param_exec_vals = null_mut();
    (*econtext).ecxt_param_list_info = null_mut();

    (*econtext).ecxt_aggvalues = null_mut();
    (*econtext).ecxt_aggnulls = null_mut();

    (*econtext).caseValue_datum = 0 as Datum;
    (*econtext).caseValue_isNull = true;

    (*econtext).domainValue_datum = 0 as Datum;
    (*econtext).domainValue_isNull = true;

    (*econtext).ecxt_estate = null_mut();

    (*econtext).ecxt_callbacks = null_mut();

    econtext
}

/* ----------------
 *		FreeExprContext
 *
 *		Free an expression context, including calling any remaining
 *		shutdown callbacks.
 *
 * Since we free the temporary context used for expression evaluation,
 * any previously computed pass-by-reference expression result will go away!
 *
 * If isCommit is false, we are being called in error cleanup, and should
 * not call callbacks but only release memory.  (It might be better to call
 * the callbacks and pass the isCommit flag to them, but that would require
 * more invasive code changes than currently seems justified.)
 *
 * Note we make no assumption about the caller's memory context.
 * ----------------
 */
/// # Safety
/// `econtext` must be a live ExprContext.
#[no_mangle]
pub unsafe fn FreeExprContext(econtext: *mut ExprContext, isCommit: bool) {
    let estate: *mut EState;

    /* Call any registered callbacks */
    ShutdownExprContext(econtext, isCommit);
    /* And clean up the memory used */
    MemoryContextDelete((*econtext).ecxt_per_tuple_memory);
    /* Unlink self from owning EState, if any */
    estate = (*econtext).ecxt_estate;
    if !estate.is_null() {
        (*estate).es_exprcontexts =
            list_delete_ptr((*estate).es_exprcontexts, econtext as *mut c_void);
    }
    /* And delete the ExprContext node */
    pfree(econtext as *mut c_void);
}

/*
 * ReScanExprContext
 *
 *		Reset an expression context in preparation for a rescan of its
 *		plan node.  This requires calling any registered shutdown callbacks,
 *		since any partially complete set-returning-functions must be canceled.
 *
 * Note we make no assumption about the caller's memory context.
 */
/// # Safety
/// `econtext` must be a live ExprContext.
pub unsafe fn ReScanExprContext(econtext: *mut ExprContext) {
    /* Call any registered callbacks */
    ShutdownExprContext(econtext, true);
    /* And clean up the memory used */
    MemoryContextReset((*econtext).ecxt_per_tuple_memory);
}

/*
 * Build a per-output-tuple ExprContext for an EState.
 *
 * This is normally invoked via GetPerTupleExprContext() macro,
 * not directly.
 */
/// # Safety
/// `estate` must be a live EState.
pub unsafe fn MakePerTupleExprContext(estate: *mut EState) -> *mut ExprContext {
    if (*estate).es_per_tuple_exprcontext.is_null() {
        (*estate).es_per_tuple_exprcontext = CreateExprContext(estate);
    }

    (*estate).es_per_tuple_exprcontext
}

/*
 * GetPerTupleExprContext / GetPerTupleMemoryContext / ResetPerTupleExprContext
 *
 * These are macros in executor/executor.h.  Translated as inline helpers; the
 * macros operate on the EState's per-output-tuple ExprContext.
 *
 *   #define GetPerTupleExprContext(estate) \
 *       ((estate)->es_per_tuple_exprcontext ? \
 *        (estate)->es_per_tuple_exprcontext : \
 *        MakePerTupleExprContext(estate))
 *   #define GetPerTupleMemoryContext(estate) \
 *       (GetPerTupleExprContext(estate)->ecxt_per_tuple_memory)
 *   #define ResetPerTupleExprContext(estate) \
 *       do { \
 *           if ((estate)->es_per_tuple_exprcontext) \
 *               ResetExprContext((estate)->es_per_tuple_exprcontext); \
 *       } while (0)
 */

/// `GetPerTupleExprContext(estate)`
///
/// # Safety
/// `estate` must be a live EState.
#[inline]
#[no_mangle]
pub unsafe fn GetPerTupleExprContext(estate: *mut EState) -> *mut ExprContext {
    if !(*estate).es_per_tuple_exprcontext.is_null() {
        (*estate).es_per_tuple_exprcontext
    } else {
        MakePerTupleExprContext(estate)
    }
}

/// `GetPerTupleMemoryContext(estate)`
///
/// # Safety
/// `estate` must be a live EState.
#[inline]
pub unsafe fn GetPerTupleMemoryContext(estate: *mut EState) -> MemoryContext {
    (*GetPerTupleExprContext(estate)).ecxt_per_tuple_memory
}

/// `ResetExprContext(econtext)` (executor/executor.h): reset the per-tuple memory.
///
/// # Safety
/// `econtext` must be a live ExprContext.
#[inline]
pub unsafe fn ResetExprContext(econtext: *mut ExprContext) {
    MemoryContextReset((*econtext).ecxt_per_tuple_memory);
}

/// `ResetPerTupleExprContext(estate)`
///
/// # Safety
/// `estate` must be a live EState.
#[inline]
pub unsafe fn ResetPerTupleExprContext(estate: *mut EState) {
    if !(*estate).es_per_tuple_exprcontext.is_null() {
        ResetExprContext((*estate).es_per_tuple_exprcontext);
    }
}

// ----------------------------------------------------------------
//				 miscellaneous node-init support functions
//
// Note: all of these are expected to be called with CurrentMemoryContext
// equal to the per-query memory context.
// ----------------------------------------------------------------

/* ----------------
 *		ExecAssignExprContext
 *
 *		This initializes the ps_ExprContext field.  It is only necessary
 *		to do this for nodes which use ExecQual or ExecProject
 *		because those routines require an econtext. Other nodes that
 *		don't have to evaluate expressions don't need to do this.
 * ----------------
 *
 * The CreateExprContext part is real; storing into planstate->ps_ExprContext
 * needs the (opaque, not-yet-fully-ported) PlanState layout.  Kept as a thin
 * wrapper whose body is a STUB.
 */
/// # Safety
/// `planstate` must be a valid PlanState.
pub unsafe fn ExecAssignExprContext(estate: *mut EState, planstate: *mut PlanState) {
    (*planstate).ps_ExprContext = CreateExprContext(estate);
}

/* ----------------
 *		ExecGetResultType
 * ----------------
 */
/// # Safety
/// `planstate` must be a valid PlanState.
pub unsafe fn ExecGetResultType(planstate: *mut PlanState) -> TupleDesc {
    (*planstate).ps_ResultTupleDesc
}

/*
 * ExecGetResultSlotOps - information about node's type of result slot
 */
/// # Safety
/// STUB: needs PlanState result-slot-ops fields + TTSOpsVirtual.
pub unsafe fn ExecGetResultSlotOps(
    planstate: *mut PlanState,
    isfixed: *mut bool,
) -> *const TupleTableSlotOps {
    if (*planstate).resultopsset && !(*planstate).resultops.is_null() {
        if !isfixed.is_null() {
            *isfixed = (*planstate).resultopsfixed;
        }
        return (*planstate).resultops;
    }

    if !isfixed.is_null() {
        if (*planstate).resultopsset {
            *isfixed = (*planstate).resultopsfixed;
        } else if !(*planstate).ps_ResultTupleSlot.is_null() {
            *isfixed = crate::executor::tuptable::TTS_FIXED((*planstate).ps_ResultTupleSlot);
        } else {
            *isfixed = false;
        }
    }

    if (*planstate).ps_ResultTupleSlot.is_null() {
        return &crate::executor::execTuples::TTSOpsVirtual;
    }

    (*(*planstate).ps_ResultTupleSlot).tts_ops
}

/*
 * ExecGetCommonSlotOps - identify common result slot type, if any
 */
/// # Safety
/// STUB: depends on ExecGetResultSlotOps.
pub unsafe fn ExecGetCommonSlotOps(
    planstates: *mut *mut PlanState,
    nplans: c_int,
) -> *const TupleTableSlotOps {
    let mut isfixed: bool = false;

    if nplans <= 0 {
        return core::ptr::null();
    }
    let result = ExecGetResultSlotOps(*planstates.add(0), &mut isfixed);
    if !isfixed {
        return core::ptr::null();
    }
    for i in 1..nplans {
        let thisops = ExecGetResultSlotOps(*planstates.add(i as usize), &mut isfixed);
        if !isfixed {
            return core::ptr::null();
        }
        if result != thisops {
            return core::ptr::null();
        }
    }
    result
}

/*
 * ExecGetCommonChildSlotOps - as above, for the PlanState's standard children
 */
/// # Safety
/// STUB: needs outerPlanState/innerPlanState (PlanState).
pub unsafe fn ExecGetCommonChildSlotOps(ps: *mut PlanState) -> *const TupleTableSlotOps {
    let mut planstates: [*mut PlanState; 2] = [
        crate::nodes::execnodes::outerPlanState(ps),
        crate::nodes::execnodes::innerPlanState(ps),
    ];
    ExecGetCommonSlotOps(planstates.as_mut_ptr(), 2)
}

/* ----------------
 *		ExecAssignProjectionInfo
 *
 * forms the projection information from the node's targetlist
 * ----------------
 */
/// # Safety
/// `planstate` must be a valid PlanState.
pub unsafe fn ExecAssignProjectionInfo(planstate: *mut PlanState, inputDesc: TupleDesc) {
    (*planstate).ps_ProjInfo = crate::executor::execExpr::ExecBuildProjectionInfo(
        (*(*planstate).plan).targetlist,
        (*planstate).ps_ExprContext,
        (*planstate).ps_ResultTupleSlot,
        planstate,
        inputDesc,
    );
}

/* ----------------
 *		ExecConditionalAssignProjectionInfo
 *
 * as ExecAssignProjectionInfo, but store NULL rather than building projection
 * info if no projection is required
 * ----------------
 */
/// # Safety
/// `planstate` must be a valid PlanState.
pub unsafe fn ExecConditionalAssignProjectionInfo(
    planstate: *mut PlanState,
    inputDesc: TupleDesc,
    varno: c_int,
) {
    if tlist_matches_tupdesc(planstate, (*(*planstate).plan).targetlist, varno, inputDesc) {
        (*planstate).ps_ProjInfo = core::ptr::null_mut();
        (*planstate).resultopsset = (*planstate).scanopsset;
        (*planstate).resultopsfixed = (*planstate).scanopsfixed;
        (*planstate).resultops = (*planstate).scanops;
    } else {
        if (*planstate).ps_ResultTupleSlot.is_null() {
            crate::executor::execTuples::ExecInitResultSlot(
                planstate,
                &raw const crate::executor::execTuples::TTSOpsVirtual,
            );
            (*planstate).resultops = &raw const crate::executor::execTuples::TTSOpsVirtual;
            (*planstate).resultopsfixed = true;
            (*planstate).resultopsset = true;
        }
        ExecAssignProjectionInfo(planstate, inputDesc);
    }
}

/// # Safety
/// `tlist` is a valid targetlist of `TargetEntry` nodes and `tupdesc` is live.
unsafe fn tlist_matches_tupdesc(
    _ps: *mut PlanState,
    tlist: *mut List,
    varno: c_int,
    tupdesc: TupleDesc,
) -> bool {
    let numattrs = (*tupdesc).natts;
    let mut tlist_item = list_head(tlist);

    /* Check the tlist attributes */
    for attrno in 1..=numattrs {
        let att_tup = TupleDescAttr(tupdesc, attrno - 1);

        if tlist_item.is_null() {
            return false; /* tlist too short */
        }
        let var = (*(lfirst(tlist_item) as *mut TargetEntry)).expr as *mut Var;
        if var.is_null() || !IsA!(var, T_Var) {
            return false; /* tlist item not a Var */
        }
        /* if these Asserts fail, planner messed up */
        debug_assert_eq!((*var).varno, varno);
        debug_assert_eq!((*var).varlevelsup, 0);
        if (*var).varattno as c_int != attrno {
            return false; /* out of order */
        }
        if (*att_tup).attisdropped {
            return false; /* table contains dropped columns */
        }
        if (*att_tup).atthasmissing {
            return false; /* table contains cols with missing values */
        }

        /*
         * Note: usually the Var's type should match the tupdesc exactly, but
         * in situations involving unions of columns that have different
         * typmods, the Var may have come from above the union and hence have
         * typmod -1.  This is a legitimate situation since the Var still
         * describes the column, just not as exactly as the tupdesc does.
         */
        if (*var).vartype != (*att_tup).atttypid
            || ((*var).vartypmod != (*att_tup).atttypmod && (*var).vartypmod != -1)
        {
            return false; /* type mismatch */
        }

        tlist_item = lnext(tlist, tlist_item);
    }

    if !tlist_item.is_null() {
        return false; /* tlist too long */
    }

    true
}

// ----------------------------------------------------------------
//				  Scan node support
// ----------------------------------------------------------------

/* ----------------
 *		ExecAssignScanType
 * ----------------
 */
/// # Safety
/// `scanstate` must be a valid ScanState.
pub unsafe fn ExecAssignScanType(
    scanstate: *mut crate::nodes::execnodes::ScanState,
    tupDesc: TupleDesc,
) {
    let slot: *mut TupleTableSlot = (*scanstate).ss_ScanTupleSlot;

    crate::executor::execTuples::ExecSetSlotDescriptor(slot, tupDesc);
}

/* ----------------
 *		ExecCreateScanSlotFromOuterPlan
 * ----------------
 */
/// # Safety
/// `scanstate` must be a valid ScanState.
pub unsafe fn ExecCreateScanSlotFromOuterPlan(
    estate: *mut EState,
    scanstate: *mut c_void,
    tts_ops: *const TupleTableSlotOps,
) {
    let scanstate = scanstate as *mut crate::nodes::execnodes::ScanState;
    let outerPlan: *mut PlanState =
        crate::nodes::execnodes::outerPlanState(&raw mut (*scanstate).ps);
    let tupDesc: TupleDesc = ExecGetResultType(outerPlan);

    crate::executor::execTuples::ExecInitScanTupleSlot(estate, scanstate, tupDesc, tts_ops);
}

/* ----------------------------------------------------------------
 *		ExecRelationIsTargetRelation
 *
 *		Detect whether a relation (identified by rangetable index)
 *		is one of the target relations of the query.
 * ----------------------------------------------------------------
 */
/// # Safety
/// STUB: needs PlannedStmt.resultRelations deref (plannodes).
pub unsafe fn ExecRelationIsTargetRelation(_estate: *mut EState, _scanrelid: Index) -> bool {
    crate::nodes::pg_list::list_member_int(
        (*(*_estate).es_plannedstmt).resultRelations as _,
        _scanrelid as c_int,
    )
}

/* ----------------------------------------------------------------
 *		ExecOpenScanRelation
 *
 *		Open the heap relation to be scanned by a base-level scan plan node.
 * ----------------------------------------------------------------
 */
/// # Safety
/// STUB: needs relcache/tableam (Relation is opaque) + ExecGetRangeTableRelation.
pub unsafe fn ExecOpenScanRelation(
    estate: *mut EState,
    scanrelid: Index,
    eflags: c_int,
) -> crate::nodes::execnodes::Relation {
    /* Open the relation. */
    let rel = ExecGetRangeTableRelation(estate, scanrelid, false);

    /*
     * Complain if we're attempting a scan of an unscannable relation, except
     * when the query won't actually be run.
     */
    if (eflags
        & (crate::executor::executor::EXEC_FLAG_EXPLAIN_ONLY
            | crate::executor::executor::EXEC_FLAG_WITH_NO_DATA))
        == 0
        && !(*(*rel).rd_rel).relispopulated
    {
        ereport!(
            ERROR,
            errmsg!(
                "materialized view \"{}\" has not been populated",
                core::ffi::CStr::from_ptr(crate::utils::rel::RelationGetRelationName(rel))
                    .to_string_lossy()
            ) /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
              errhint("Use the REFRESH MATERIALIZED VIEW command.") */
        );
    }

    rel
}

/*
 * ExecInitRangeTable
 *		Set up executor's range-table-related data
 */
/// # Safety
/// STUB: needs es_unpruned_relids + the es_relations Relation array (relcache).
#[no_mangle]
pub unsafe fn ExecInitRangeTable(
    estate: *mut EState,
    rangeTable: *mut List,
    permInfos: *mut List,
    unpruned_relids: *mut crate::nodes::bitmapset::Bitmapset,
) {
    /* Remember the range table List as-is */
    (*estate).es_range_table = rangeTable;

    /* ... and the RTEPermissionInfo List too */
    (*estate).es_rteperminfos = permInfos;

    /* Set size of associated arrays */
    (*estate).es_range_table_size = crate::nodes::pg_list::list_length(rangeTable) as Index;

    (*estate).es_unpruned_relids = unpruned_relids as _;

    /*
     * Allocate an array to store an open Relation corresponding to each
     * rangetable entry, and initialize entries to NULL.
     */
    (*estate).es_relations = palloc0(
        (*estate).es_range_table_size as usize
            * size_of::<crate::nodes::execnodes::Relation>(),
    ) as *mut crate::nodes::execnodes::Relation;

    (*estate).es_result_relations = null_mut();
    (*estate).es_rowmarks = null_mut();
}

/*
 * ExecGetRangeTableRelation
 *		Open the Relation for a range table entry, if not already done
 */
/// # Safety
/// STUB: needs relcache/tableam (table_open) + exec_rt_fetch + parallel.h.
pub unsafe fn ExecGetRangeTableRelation(
    estate: *mut EState,
    rti: Index,
    isResultRel: bool,
) -> crate::nodes::execnodes::Relation {
    Assert!(rti > 0 && rti <= (*estate).es_range_table_size);

    if !isResultRel
        && !crate::nodes::bitmapset::bms_is_member(rti as c_int, (*estate).es_unpruned_relids as _)
    {
        elog!(ERROR, "trying to open a pruned relation");
    }

    let mut rel = *(*estate).es_relations.add((rti - 1) as usize);
    if rel.is_null() {
        /* First time through, so open the relation */
        let rte = crate::executor::executor::exec_rt_fetch(rti, estate);

        Assert!((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION);

        if !IsParallelWorker() {
            rel = crate::access::table::table::table_open((*rte).relid, NoLock);
        } else {
            rel = crate::access::table::table::table_open((*rte).relid, (*rte).rellockmode);
        }

        *(*estate).es_relations.add((rti - 1) as usize) = rel;
    }

    rel
}

/*
 * ExecInitResultRelation
 *		Open relation given by the passed-in RT index and fill its
 *		ResultRelInfo node
 */
/// # Safety
/// STUB: needs ExecGetRangeTableRelation + InitResultRelInfo (nodeModifyTable).
pub unsafe fn ExecInitResultRelation(
    estate: *mut EState,
    resultRelInfo: *mut ResultRelInfo,
    rti: Index,
) {
    let resultRelationDesc = ExecGetRangeTableRelation(estate, rti, true);
    crate::executor::execMain::InitResultRelInfo(
        resultRelInfo,
        resultRelationDesc,
        rti,
        null_mut(),
        (*estate).es_instrument,
    );

    if (*estate).es_result_relations.is_null() {
        (*estate).es_result_relations = palloc0(
            (*estate).es_range_table_size as usize * size_of::<*mut ResultRelInfo>(),
        ) as *mut *mut ResultRelInfo;
    }
    *(*estate).es_result_relations.add((rti - 1) as usize) = resultRelInfo;

    (*estate).es_opened_result_relations = crate::nodes::pg_list::lappend(
        (*estate).es_opened_result_relations,
        resultRelInfo as *mut c_void,
    );
}

/*
 * UpdateChangedParamSet
 *		Add changed parameters to a plan node's chgParam set
 */
/// # Safety
/// STUB: needs PlanState.plan->allParam + bms_intersect/bms_join (bitmapset).
pub unsafe fn UpdateChangedParamSet(
    _node: *mut PlanState,
    _newchg: *mut crate::nodes::bitmapset::Bitmapset,
) {
    // TODO(pg-port): needs node->plan->allParam (Plan deref) + node->chgParam +
    //   bms_intersect / bms_join.
    unimplemented!("UpdateChangedParamSet: needs Plan/PlanState bitmapset fields")
}

/*
 * executor_errposition
 *		Report an execution-time cursor position, if possible.
 */
/// # Safety
/// STUB: needs pg_mbstrlen_with_len (mb/pg_wchar) + errposition.
pub unsafe fn executor_errposition(_estate: *mut EState, location: c_int) -> c_int {
    /* No-op if location was not provided */
    if location < 0 {
        return 0;
    }
    // TODO(pg-port): needs es_sourceText handling + pg_mbstrlen_with_len
    //   (mb/pg_wchar.h) + errposition (elog).  C body preserved:
    // if (estate == NULL || estate->es_sourceText == NULL) return 0;
    // pos = pg_mbstrlen_with_len(estate->es_sourceText, location) + 1;
    // return errposition(pos);
    unimplemented!("executor_errposition: needs pg_mbstrlen_with_len + errposition")
}

/*
 * Register a shutdown callback in an ExprContext.
 *
 * Shutdown callbacks will be called (in reverse order of registration)
 * when the ExprContext is deleted or rescanned.  This provides a hook
 * for functions called in the context to do any cleanup needed --- it's
 * particularly useful for functions returning sets.  Note that the
 * callback will *not* be called in the event that execution is aborted
 * by an error.
 */
/// # Safety
/// `econtext` must be a live ExprContext.
pub unsafe fn RegisterExprContextCallback(
    econtext: *mut ExprContext,
    function: ExprContextCallbackFunction,
    arg: Datum,
) {
    let ecxt_callback: *mut ExprContext_CB;

    /* Save the info in appropriate memory context */
    ecxt_callback = MemoryContextAlloc(
        (*econtext).ecxt_per_query_memory,
        core::mem::size_of::<ExprContext_CB>(),
    ) as *mut ExprContext_CB;

    (*ecxt_callback).function = function;
    (*ecxt_callback).arg = arg;

    /* link to front of list for appropriate execution order */
    (*ecxt_callback).next = (*econtext).ecxt_callbacks;
    (*econtext).ecxt_callbacks = ecxt_callback;
}

/*
 * Deregister a shutdown callback in an ExprContext.
 *
 * Any list entries matching the function and arg will be removed.
 * This can be used if it's no longer necessary to call the callback.
 */
/// # Safety
/// `econtext` must be a live ExprContext.
pub unsafe fn UnregisterExprContextCallback(
    econtext: *mut ExprContext,
    function: ExprContextCallbackFunction,
    arg: Datum,
) {
    let mut prev_callback: *mut *mut ExprContext_CB;
    let mut ecxt_callback: *mut ExprContext_CB;

    prev_callback = core::ptr::addr_of_mut!((*econtext).ecxt_callbacks);

    loop {
        ecxt_callback = *prev_callback;
        if ecxt_callback.is_null() {
            break;
        }

        /*
         * Compare the callback function pointers and arg.  ExprContextCallbackFunction
         * is `Option<unsafe fn(Datum)>`; equality of the fn pointers reproduces C's
         * `ecxt_callback->function == function`.
         */
        if fn_ptr_eq((*ecxt_callback).function, function) && (*ecxt_callback).arg == arg {
            *prev_callback = (*ecxt_callback).next;
            pfree(ecxt_callback as *mut c_void);
        } else {
            prev_callback = core::ptr::addr_of_mut!((*ecxt_callback).next);
        }
    }
}

/// Compare two `ExprContextCallbackFunction` values for pointer equality.
///
/// C compares raw `function` pointers directly; in Rust the callback is an
/// `Option<unsafe fn(Datum)>`.  We compare the underlying addresses (both None
/// compares equal; a None vs Some compares unequal).
#[inline]
fn fn_ptr_eq(a: ExprContextCallbackFunction, b: ExprContextCallbackFunction) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(fa), Some(fb)) => (fa as usize) == (fb as usize),
        _ => false,
    }
}

/*
 * Call all the shutdown callbacks registered in an ExprContext.
 *
 * The callback list is emptied (important in case this is only a rescan
 * reset, and not deletion of the ExprContext).
 *
 * If isCommit is false, just clean the callback list but don't call 'em.
 * (See comment for FreeExprContext.)
 */
/// # Safety
/// `econtext` must be a live ExprContext.
unsafe fn ShutdownExprContext(econtext: *mut ExprContext, isCommit: bool) {
    let mut ecxt_callback: *mut ExprContext_CB;
    let oldcontext: MemoryContext;

    /* Fast path in normal case where there's nothing to do. */
    if (*econtext).ecxt_callbacks.is_null() {
        return;
    }

    /*
     * Call the callbacks in econtext's per-tuple context.  This ensures that
     * any memory they might leak will get cleaned up.
     */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    /*
     * Call each callback function in reverse registration order.
     */
    loop {
        ecxt_callback = (*econtext).ecxt_callbacks;
        if ecxt_callback.is_null() {
            break;
        }
        (*econtext).ecxt_callbacks = (*ecxt_callback).next;
        if isCommit {
            if let Some(f) = (*ecxt_callback).function {
                f((*ecxt_callback).arg);
            }
        }
        pfree(ecxt_callback as *mut c_void);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 *		GetAttributeByName
 *		GetAttributeByNum
 *
 *		These functions return the value of the requested attribute
 *		out of the given tuple Datum.
 */
/// # Safety
/// STUB: needs typcache (lookup_rowtype_tupdesc) + heap_getattr + HeapTupleHeader.
pub unsafe fn GetAttributeByName(
    _tuple: *mut c_void, /* HeapTupleHeader */
    _attname: *const c_char,
    _isNull: *mut bool,
) -> Datum {
    // TODO(pg-port): needs HeapTupleHeaderGetTypeId/TypMod, lookup_rowtype_tupdesc
    //   (typcache), namestrcmp, heap_getattr, ReleaseTupleDesc.  C body in execUtils.c.
    unimplemented!("GetAttributeByName: needs typcache + heap_getattr")
}

/// # Safety
/// STUB: needs typcache (lookup_rowtype_tupdesc) + heap_getattr + HeapTupleHeader.
pub unsafe fn GetAttributeByNum(
    _tuple: *mut c_void, /* HeapTupleHeader */
    _attrno: i16,        /* AttrNumber */
    _isNull: *mut bool,
) -> Datum {
    // TODO(pg-port): see GetAttributeByName.  C body preserved in execUtils.c.
    unimplemented!("GetAttributeByNum: needs typcache + heap_getattr")
}

/*
 * Number of items in a tlist (including any resjunk items!)
 */
/// # Safety
/// STUB: needs list_length on a targetlist (trivial, but kept with the family).
/// Real once a targetlist List is available; list_length itself is ported.
pub unsafe fn ExecTargetListLength(targetlist: *mut List) -> c_int {
    /* This used to be more complex, but fjoins are dead */
    crate::nodes::pg_list::list_length(targetlist)
}

/*
 * Number of items in a tlist, not including any resjunk items
 */
/// # Safety
/// `targetlist` must be a valid List of TargetEntry nodes, or NIL.
pub unsafe fn ExecCleanTargetListLength(targetlist: *mut List) -> c_int {
    let mut len: c_int = 0;
    let n = crate::nodes::pg_list::list_length(targetlist);
    let mut i: c_int = 0;
    while i < n {
        let curTle = (*(*targetlist).elements.add(i as usize)).ptr_value as *mut TargetEntry;
        if !(*curTle).resjunk {
            len += 1;
        }
        i += 1;
    }
    len
}

/*
 * The ExecGetTrigger*Slot / ExecGetReturningSlot / ExecGetAllNullSlot family:
 * all need ResultRelInfo->ri_RelationDesc deref (Relation/relcache),
 * RelationGetDescr, table_slot_callbacks (tableam) and ExecInitExtraTupleSlot
 * (execTuples slot-from-plan helper).  All STUBBED.
 */

/// # Safety
/// STUB: needs Relation deref + table_slot_callbacks + ExecInitExtraTupleSlot.
pub unsafe fn ExecGetTriggerOldSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!("ExecGetTriggerOldSlot: needs relcache/tableam + slot init")
}

/// # Safety
/// STUB: needs Relation deref + table_slot_callbacks + ExecInitExtraTupleSlot.
pub unsafe fn ExecGetTriggerNewSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!("ExecGetTriggerNewSlot: needs relcache/tableam + slot init")
}

/// # Safety
/// STUB: needs Relation deref + table_slot_callbacks + ExecInitExtraTupleSlot.
pub unsafe fn ExecGetReturningSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!("ExecGetReturningSlot: needs relcache/tableam + slot init")
}

/// # Safety
/// STUB: needs Relation deref + slot init + ExecStoreAllNullTuple.
pub unsafe fn ExecGetAllNullSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!("ExecGetAllNullSlot: needs relcache/tableam + slot init")
}

/*
 * ExecGetChildToRootMap / ExecGetRootToChildMap: need convert_tuples_by_name
 * (access/tupconvert), RelationGetDescr (relcache), build_attrmap_by_name_if_req.
 * STUBBED.
 */

/// # Safety
/// STUB: needs tupconvert + relcache.
pub unsafe fn ExecGetChildToRootMap(_resultRelInfo: *mut ResultRelInfo) -> *mut TupleConversionMap {
    unimplemented!("ExecGetChildToRootMap: needs tupconvert + relcache")
}

/// # Safety
/// STUB: needs tupconvert + relcache + attrmap.
#[no_mangle]
pub unsafe fn ExecGetRootToChildMap(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut TupleConversionMap {
    unimplemented!("ExecGetRootToChildMap: needs tupconvert + relcache")
}

/*
 * ExecGetInsertedCols / ExecGetUpdatedCols / ExecGetExtraUpdatedCols /
 * ExecGetAllUpdatedCols: need GetResultRTEPermissionInfo (RTEPermissionInfo deref),
 * execute_attr_map_cols, ExecInitGenerated, bms_union.  STUBBED.
 */

/// # Safety
/// STUB: needs GetResultRTEPermissionInfo + execute_attr_map_cols.
pub unsafe fn ExecGetInsertedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut crate::nodes::bitmapset::Bitmapset {
    unimplemented!("ExecGetInsertedCols: needs RTEPermissionInfo + attrmap")
}

/// # Safety
/// STUB: needs GetResultRTEPermissionInfo + execute_attr_map_cols.
pub unsafe fn ExecGetUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut crate::nodes::bitmapset::Bitmapset {
    unimplemented!("ExecGetUpdatedCols: needs RTEPermissionInfo + attrmap")
}

/// # Safety
/// STUB: needs ExecInitGenerated (nodeModifyTable).
pub unsafe fn ExecGetExtraUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut crate::nodes::bitmapset::Bitmapset {
    unimplemented!("ExecGetExtraUpdatedCols: needs ExecInitGenerated")
}

/// # Safety
/// STUB: needs bms_union + GetPerTupleMemoryContext (the memcxt part is real but
/// the inputs are stubbed).
pub unsafe fn ExecGetAllUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut crate::nodes::bitmapset::Bitmapset {
    unimplemented!("ExecGetAllUpdatedCols: needs ExecGetUpdatedCols/ExtraUpdatedCols + bms_union")
}

/*
 * GetResultRTEPermissionInfo
 *		Looks up RTEPermissionInfo for ExecGet*Cols() routines
 */
/// # Safety
/// STUB: needs ResultRelInfo RT-index deref + exec_rt_fetch + getRTEPermissionInfo.
unsafe fn GetResultRTEPermissionInfo(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut crate::nodes::parsenodes::RTEPermissionInfo {
    unimplemented!("GetResultRTEPermissionInfo: needs exec_rt_fetch + getRTEPermissionInfo")
}

/*
 * ExecGetResultRelCheckAsUser
 *		Returns the user to modify passed-in result relation as
 */
/// # Safety
/// STUB: needs GetResultRTEPermissionInfo + GetUserId (miscadmin).
pub unsafe fn ExecGetResultRelCheckAsUser(
    _relInfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> Oid {
    unimplemented!("ExecGetResultRelCheckAsUser: needs RTEPermissionInfo + GetUserId")
}

// ============================================================================
//   Tests
//
//   These exercise the REAL state/memory-context lifecycle core.  They use the
//   bootstrap allocator wired into the prelude (utils::palloc + the
//   AllocSetContextCreate! shim in utils::memutils): palloc/pfree and
//   MemoryContextSwitchTo are functional; the AllocSet shim returns the parent
//   (no real child context) and MemoryContextDelete/Reset are no-ops, which is
//   sufficient for the lifecycle logic under test.  We do NOT call the real
//   utils::mmgr::mcxt::MemoryContextInit() here, because the EState/ExprContext
//   structs carry the prelude's `MemoryContext` type (= utils::palloc), not the
//   real mcxt one; the two are not yet unified (see task: rewire prelude palloc).
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicI32, Ordering};

    // A module-level counter the shutdown callback bumps, so we can observe that
    // ShutdownExprContext actually ran the callback.
    static SHUTDOWN_HITS: AtomicI32 = AtomicI32::new(0);

    unsafe fn bump_shutdown(_arg: Datum) {
        SHUTDOWN_HITS.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn create_executor_state_basics() {
        unsafe {
            let estate = CreateExecutorState();
            assert!(!estate.is_null());
            // r#type tag is set by makeNode!.
            assert_eq!((*estate).r#type, T_EState);
            // per-query context is non-null (bootstrap sentinel / parent).
            assert!(!(*estate).es_query_cxt.is_null());
            // lists initialized to NIL, refcounts zeroed, snapshot invalid.
            assert!((*estate).es_exprcontexts.is_null());
            assert!((*estate).es_range_table.is_null());
            assert_eq!((*estate).es_range_table_size, 0);
            assert_eq!((*estate).es_processed, 0);
            assert!((*estate).es_snapshot.is_null());
            assert!((*estate).es_crosscheck_snapshot.is_null());
            assert!((*estate).es_per_tuple_exprcontext.is_null());
            assert_eq!((*estate).es_direction, ForwardScanDirection);

            FreeExecutorState(estate);
        }
    }

    #[test]
    fn create_expr_context_has_per_tuple_memory() {
        unsafe {
            let estate = CreateExecutorState();
            let econtext = CreateExprContext(estate);
            assert!(!econtext.is_null());
            assert_eq!((*econtext).r#type, T_ExprContext);
            assert!(!(*econtext).ecxt_per_tuple_memory.is_null());
            assert_eq!((*econtext).ecxt_per_query_memory, (*estate).es_query_cxt);
            assert_eq!((*econtext).ecxt_estate, estate);
            // It was linked into the EState's es_exprcontexts list.
            assert!(!(*estate).es_exprcontexts.is_null());
            assert_eq!(
                crate::nodes::pg_list::list_length((*estate).es_exprcontexts),
                1
            );
            assert_eq!(
                linitial((*estate).es_exprcontexts) as *mut ExprContext,
                econtext
            );

            // FreeExecutorState must shut everything down without crashing.
            FreeExecutorState(estate);
        }
    }

    #[test]
    fn shutdown_callback_runs_and_unlinks() {
        unsafe {
            SHUTDOWN_HITS.store(0, Ordering::SeqCst);

            let estate = CreateExecutorState();
            let econtext = CreateExprContext(estate);

            // Register a shutdown callback.
            RegisterExprContextCallback(econtext, Some(bump_shutdown), 0 as Datum);
            assert!(!(*econtext).ecxt_callbacks.is_null());

            // ReScanExprContext runs ShutdownExprContext(.., true) -> callback fires
            // once, and the callback list is emptied.
            ReScanExprContext(econtext);
            assert_eq!(SHUTDOWN_HITS.load(Ordering::SeqCst), 1);
            assert!((*econtext).ecxt_callbacks.is_null());

            // Register two more; UnregisterExprContextCallback should remove the
            // matching one (same fn + arg) without firing it.
            RegisterExprContextCallback(econtext, Some(bump_shutdown), 7 as Datum);
            RegisterExprContextCallback(econtext, Some(bump_shutdown), 9 as Datum);
            UnregisterExprContextCallback(econtext, Some(bump_shutdown), 7 as Datum);
            // The arg=7 entry is gone; arg=9 remains (count still 1 from earlier).
            assert_eq!(SHUTDOWN_HITS.load(Ordering::SeqCst), 1);
            assert!(!(*econtext).ecxt_callbacks.is_null());
            assert_eq!((*(*econtext).ecxt_callbacks).arg, 9 as Datum);

            // FreeExprContext(isCommit=true) fires the remaining arg=9 callback and
            // unlinks the econtext from the EState.
            FreeExprContext(econtext, true);
            assert_eq!(SHUTDOWN_HITS.load(Ordering::SeqCst), 2);
            assert!((*estate).es_exprcontexts.is_null());

            FreeExecutorState(estate);
        }
    }

    #[test]
    fn make_and_reset_per_tuple_context() {
        unsafe {
            let estate = CreateExecutorState();
            assert!((*estate).es_per_tuple_exprcontext.is_null());

            let ec = MakePerTupleExprContext(estate);
            assert!(!ec.is_null());
            assert_eq!((*estate).es_per_tuple_exprcontext, ec);
            // Idempotent: second call returns the same context.
            assert_eq!(MakePerTupleExprContext(estate), ec);

            let mcxt = GetPerTupleMemoryContext(estate);
            assert_eq!(mcxt, (*ec).ecxt_per_tuple_memory);

            // ResetPerTupleExprContext must not crash (no-op reset on bootstrap).
            ResetPerTupleExprContext(estate);

            FreeExecutorState(estate);
        }
    }

    #[test]
    fn standalone_expr_context_lifecycle() {
        unsafe {
            let econtext = CreateStandaloneExprContext();
            assert!(!econtext.is_null());
            assert_eq!((*econtext).r#type, T_ExprContext);
            assert!((*econtext).ecxt_estate.is_null());
            assert!(!(*econtext).ecxt_per_tuple_memory.is_null());

            // No EState link -> FreeExprContext just runs callbacks + frees memory.
            FreeExprContext(econtext, true);
        }
    }
}

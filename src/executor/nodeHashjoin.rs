/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * HASH JOIN
 *
 * This is based on the "hybrid hash join" algorithm described shortly in the
 * following page
 *
 *   https://en.wikipedia.org/wiki/Hash_join#Hybrid_hash_join
 *
 * and in detail in the referenced paper:
 *
 *   "An Adaptive Hash Join Algorithm for Multiuser Environments"
 *   Hansjorg Zeller; Jim Gray (1990). Proceedings of the 16th VLDB conference.
 *   Brisbane: 186-197.
 *
 * If the inner side tuples of a hash join do not fit in memory, the hash join
 * can be executed in multiple batches.
 *
 * If the statistics on the inner side relation are accurate, planner chooses a
 * multi-batch strategy and estimates the number of batches.
 *
 * The query executor measures the real size of the hashtable and increases the
 * number of batches if the hashtable grows too large.
 *
 * The number of batches is always a power of two, so an increase in the number
 * of batches doubles it.
 *
 * Serial hash join measures batch size lazily -- waiting until it is loading a
 * batch to determine if it will fit in memory. While inserting tuples into the
 * hashtable, serial hash join will, if that tuple were to exceed work_mem,
 * dump out the hashtable and reassign them either to other batch files or the
 * current batch resident in the hashtable.
 *
 * Parallel hash join, on the other hand, completes all changes to the number
 * of batches during the build phase. If it increases the number of batches, it
 * dumps out all the tuples from all batches and reassigns them to entirely new
 * batch files. Then it checks every batch to ensure it will fit in the space
 * budget for the query.
 *
 * In both parallel and serial hash join, the executor currently makes a best
 * effort. If a particular batch will not fit in memory, it tries doubling the
 * number of batches. If after a batch increase, there is a batch which
 * retained all or none of its tuples, the executor disables growth in the
 * number of batches globally. After growth is disabled, all batches that would
 * have previously triggered an increase in the number of batches instead
 * exceed the space allowed.
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in several ways.  A
 * parallel-oblivious hash join is one where the node is unaware that it is
 * part of a parallel plan.  In this case, a copy of the inner plan is used to
 * build a copy of the hash table in every backend, and the outer plan could
 * either be built from a partial or complete path, so that the results of the
 * hash join are correspondingly either partial or complete.  A parallel-aware
 * hash join is one that behaves differently, coordinating work between
 * backends, and appears as Parallel Hash Join in EXPLAIN output.  A Parallel
 * Hash Join always appears with a Parallel Hash node.
 *
 * Parallel-aware hash joins use the same per-backend state machine to track
 * progress through the hash join algorithm as parallel-oblivious hash joins.
 * In a parallel-aware hash join, there is also a shared state machine that
 * co-operating backends use to synchronize their local state machines and
 * program counters.  The shared state machine is managed with a Barrier IPC
 * primitive.  When all attached participants arrive at a barrier, the phase
 * advances and all waiting participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * One barrier called build_barrier is used to coordinate the hashing phases.
 * The phase is represented by an integer which begins at zero and increments
 * one by one, but in the code it is referred to by symbolic names as follows.
 * An asterisk indicates a phase that is performed by a single arbitrarily
 * chosen process.
 *
 *   PHJ_BUILD_ELECT                 -- initial state
 *   PHJ_BUILD_ALLOCATE*             -- one sets up the batches and table 0
 *   PHJ_BUILD_HASH_INNER            -- all hash the inner rel
 *   PHJ_BUILD_HASH_OUTER            -- (multi-batch only) all hash the outer
 *   PHJ_BUILD_RUN                   -- building done, probing can begin
 *   PHJ_BUILD_FREE*                 -- all work complete, one frees batches
 *
 * While in the phase PHJ_BUILD_HASH_INNER a separate pair of barriers may
 * be used repeatedly as required to coordinate expansions in the number of
 * batches or buckets.  Their phases are as follows:
 *
 *   PHJ_GROW_BATCHES_ELECT          -- initial state
 *   PHJ_GROW_BATCHES_REALLOCATE*    -- one allocates new batches
 *   PHJ_GROW_BATCHES_REPARTITION    -- all repartition
 *   PHJ_GROW_BATCHES_DECIDE*        -- one detects skew and cleans up
 *   PHJ_GROW_BATCHES_FINISH         -- finished one growth cycle
 *
 *   PHJ_GROW_BUCKETS_ELECT          -- initial state
 *   PHJ_GROW_BUCKETS_REALLOCATE*    -- one allocates new buckets
 *   PHJ_GROW_BUCKETS_REINSERT       -- all insert tuples
 *
 * If the planner got the number of batches and buckets right, those won't be
 * necessary, but on the other hand we might finish up needing to expand the
 * buckets or batches multiple times while hashing the inner relation to stay
 * within our memory budget and load factor target.  For that reason it's a
 * separate pair of barriers using circular phases.
 *
 * The PHJ_BUILD_HASH_OUTER phase is required only for multi-batch joins,
 * because we need to divide the outer relation into batches up front in order
 * to be able to process batches entirely independently.  In contrast, the
 * parallel-oblivious algorithm simply throws tuples 'forward' to 'later'
 * batches whenever it encounters them while scanning and probing, which it
 * can do because it processes batches in serial order.
 *
 * Once PHJ_BUILD_RUN is reached, backends then split up and process
 * different batches, or gang up and work together on probing batches if there
 * aren't enough to go around.  For each batch there is a separate barrier
 * with the following phases:
 *
 *  PHJ_BATCH_ELECT          -- initial state
 *  PHJ_BATCH_ALLOCATE*      -- one allocates buckets
 *  PHJ_BATCH_LOAD           -- all load the hash table from disk
 *  PHJ_BATCH_PROBE          -- all probe
 *  PHJ_BATCH_SCAN*          -- one does right/right-anti/full unmatched scan
 *  PHJ_BATCH_FREE*          -- one frees memory
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_PROBE; populating batch 0's hash table is done during
 * PHJ_BUILD_HASH_INNER so we can skip loading.
 *
 * Initially we try to plan for a single-batch hash join using the combined
 * hash_mem of all participants to create a large shared hash table.  If that
 * turns out either at planning or execution time to be impossible then we
 * fall back to regular hash_mem sized hash tables.
 *
 * To avoid deadlocks, we never wait for any barrier unless it is known that
 * all other backends attached to it are actively executing the node or have
 * finished.  Practically, that means that we never emit a tuple while attached
 * to a barrier, unless the barrier has reached a phase that means that no
 * process will wait on it again.  We emit tuples while attached to the build
 * barrier in phase PHJ_BUILD_RUN, and to a per-batch barrier in phase
 * PHJ_BATCH_PROBE.  These are advanced to PHJ_BUILD_FREE and PHJ_BATCH_SCAN
 * respectively without waiting, using BarrierArriveAndDetach() and
 * BarrierArriveAndDetachExceptLast() respectively.  The last to detach
 * receives a different return value so that it knows that it's safe to
 * clean up.  Any straggler process that attaches after that phase is reached
 * will see that it's too late to participate or access the relevant shared
 * memory objects.
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unused_unsafe)]
#![allow(unreachable_code)]

use crate::prelude::*;

use core::ffi::CStr;
use core::mem::size_of;
use core::ptr;

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::nodes::JoinType::{
    self, JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_RIGHT, JOIN_SEMI, JOIN_ANTI,
    JOIN_RIGHT_SEMI, JOIN_RIGHT_ANTI,
};
use crate::nodes::plannodes::{Plan, HashJoin, Hash, innerPlan, outerPlan};
use crate::nodes::pg_list::{List, list_length, lfirst_oid, linitial_oid};
use crate::nodes::execnodes::{
    PlanState, EState, ExprContext, ExprState, ProjectionInfo, TupleTableSlot,
    HashState, HashInstrumentation, HashJoinState,
    innerPlanState, outerPlanState,
};
use crate::nodes::execnodes::dsa_pointer;
use crate::access::common::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlotOps;
use crate::executor::hashjoin::{
    HashJoinTable, HashJoinTableData,
    ParallelHashJoinState, ParallelHashJoinBatchAccessor,
    SharedTuplestoreAccessor, BufFile,
    PHJ_BUILD_HASH_OUTER, PHJ_BUILD_RUN, PHJ_BUILD_FREE,
    PHJ_BATCH_ELECT, PHJ_BATCH_ALLOCATE, PHJ_BATCH_LOAD, PHJ_BATCH_PROBE,
    PHJ_BATCH_SCAN, PHJ_BATCH_FREE,
    PHJ_GROWTH_OK,
    INVALID_SKEW_BUCKET_NO, HJTUPLE_MINTUPLE,
};
use crate::storage::ipc::barrier::Barrier;
use crate::executor::executor::{
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
    ExecInitNode, ExecEndNode, ExecProcNode, ExecReScan, MultiExecProcNode,
    ExecAssignExprContext, ExecAssignProjectionInfo, ExecSetExecProcNode,
    ExecInitQual, ExecQual, ExecProject, ExecBuildHash32Expr,
    ExecEvalExprSwitchContext, ResetExprContext,
    ExecInitResultTupleSlotTL, ExecInitExtraTupleSlot, ExecInitNullTupleSlot,
    ExecGetResultType, ExecGetResultSlotOps,
};
use crate::executor::execTuples::{
    TTSOpsVirtual, ExecForceStoreMinimalTuple, ExecFetchSlotMinimalTuple,
};
use crate::executor::tuptable::{TupIsNull, ExecClearTuple};
use crate::executor::instrument::Instrumentation;
use crate::executor::nodeHash::{
    ExecHashTableCreate, ExecHashTableDestroy, ExecHashTableReset,
    ExecHashTableResetMatchFlags, ExecHashTableInsert,
    ExecHashGetBucketAndBatch, ExecHashGetSkewBucket,
    ExecScanHashBucket, ExecParallelScanHashBucket,
    ExecPrepHashTableForUnmatched, ExecParallelPrepHashTableForUnmatched,
    ExecScanHashTableForUnmatched, ExecParallelScanHashTableForUnmatched,
    ExecHashAccumInstrumentation,
    ExecHashTableDetachBatch, ExecHashTableDetach,
    ExecParallelHashTableAlloc, ExecParallelHashTableSetCurrentBatch,
    ExecParallelHashTableInsertCurrentBatch,
};
use crate::access::htup_details::{
    MinimalTuple, MinimalTupleData,
    HeapTupleHeaderHasMatch, HeapTupleHeaderSetMatch,
};
use crate::utils::fmgr::{FmgrInfo, fmgr_info};
use crate::utils::cache::lsyscache::{get_op_hash_functions, op_strict};
use crate::{castNode, makeNode, Assert, foreach, foreach_current_index, current_cell};

/*
 * States of the ExecHashJoin state machine
 */
const HJ_BUILD_HASHTABLE: c_int = 1;
const HJ_NEED_NEW_OUTER: c_int = 2;
const HJ_SCAN_BUCKET: c_int = 3;
const HJ_FILL_OUTER_TUPLE: c_int = 4;
const HJ_FILL_INNER_TUPLES: c_int = 5;
const HJ_NEED_NEW_BATCH: c_int = 6;

/* Returns true if doing null-fill on outer relation */
#[inline]
unsafe fn HJ_FILL_OUTER(hjstate: *mut HashJoinState) -> bool {
    !(*hjstate).hj_NullInnerTupleSlot.is_null()
}
/* Returns true if doing null-fill on inner relation */
#[inline]
unsafe fn HJ_FILL_INNER(hjstate: *mut HashJoinState) -> bool {
    !(*hjstate).hj_NullOuterTupleSlot.is_null()
}

// CHECK_FOR_INTERRUPTS is a macro in C (miscadmin.h); the real check lives in
// crate::miscadmin::CHECK_FOR_INTERRUPTS().  Wrap it so the call sites can keep
// the macro-call syntax `CHECK_FOR_INTERRUPTS!()`.
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        crate::miscadmin::CHECK_FOR_INTERRUPTS();
    }};
}

// TODO(pg-port): utils/palloc.h - palloc_array
macro_rules! palloc_array {
    ($t:ty, $n:expr) => {
        palloc(core::mem::size_of::<$t>() * ($n) as usize) as *mut $t
    };
}

/*
 * Instrumentation helpers; see executor/instrument.h.
 *
 *   #define InstrCountFiltered1(node, delta) \
 *       do { \
 *           if (((PlanState *)(node))->instrument) \
 *               ((PlanState *)(node))->instrument->nfiltered1 += (delta); \
 *       } while(0)
 */
#[inline]
unsafe fn InstrCountFiltered1(node: *mut HashJoinState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered1 += delta;
    }
}

#[inline]
unsafe fn InstrCountFiltered2(node: *mut HashJoinState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered2 += delta;
    }
}

// ---------------------------------------------------------------------------
// Stubs for dependencies declared in other .c files (not yet ported).
// ---------------------------------------------------------------------------

// TODO(pg-port): storage/ipc/barrier.c
unsafe fn BarrierPhase(b: *mut Barrier) -> c_int { let _ = b; 0 }
// TODO(pg-port): storage/ipc/barrier.c
unsafe fn BarrierAttach(b: *mut Barrier) -> c_int { let _ = b; 0 }
// TODO(pg-port): storage/ipc/barrier.c
unsafe fn BarrierDetach(b: *mut Barrier) { let _ = b; }
// TODO(pg-port): storage/ipc/barrier.c
unsafe fn BarrierArriveAndWait(b: *mut Barrier, wait_event: u32) -> bool {
    let _ = (b, wait_event);
    false
}
// TODO(pg-port): storage/ipc/barrier.c
unsafe fn BarrierInit(b: *mut Barrier, n: c_int) { let _ = (b, n); }

// TODO(pg-port): wait_event.h WAIT_EVENT_HASH_* constants
const WAIT_EVENT_HASH_BUILD_HASH_OUTER: u32 = 0;
const WAIT_EVENT_HASH_BATCH_ELECT: u32 = 0;
const WAIT_EVENT_HASH_BATCH_ALLOCATE: u32 = 0;
const WAIT_EVENT_HASH_BATCH_LOAD: u32 = 0;

// TODO(pg-port): storage/buffile.c
unsafe fn BufFileClose(f: *mut BufFile) { let _ = f; }
// TODO(pg-port): storage/buffile.c
unsafe fn BufFileCreateTemp(interXact: bool) -> *mut BufFile {
    let _ = interXact;
    ptr::null_mut()
}
// TODO(pg-port): storage/buffile.c
unsafe fn BufFileSeek(file: *mut BufFile, fileno: c_int, offset: i64, whence: c_int) -> c_int {
    let _ = (file, fileno, offset, whence);
    0
}
// TODO(pg-port): storage/buffile.c
unsafe fn BufFileWrite(file: *mut BufFile, ptr: *const c_void, size: usize) {
    let _ = (file, ptr, size);
}
// TODO(pg-port): storage/buffile.c
unsafe fn BufFileReadMaybeEOF(file: *mut BufFile, ptr: *mut c_void, size: usize, eofOK: bool) -> usize {
    let _ = (file, ptr, size, eofOK);
    0
}
// TODO(pg-port): storage/buffile.c
unsafe fn BufFileReadExact(file: *mut BufFile, ptr: *mut c_void, size: usize) {
    let _ = (file, ptr, size);
}

// SEEK_SET from <stdio.h>
const SEEK_SET: c_int = 0;

// TODO(pg-port): access/htup_details.h heap_free_minimal_tuple
unsafe fn heap_free_minimal_tuple(t: MinimalTuple) { let _ = t; }

// TODO(pg-port): utils/sharedtuplestore.c
unsafe fn sts_begin_parallel_scan(acc: *mut SharedTuplestoreAccessor) { let _ = acc; }
// TODO(pg-port): utils/sharedtuplestore.c
unsafe fn sts_end_parallel_scan(acc: *mut SharedTuplestoreAccessor) { let _ = acc; }
// TODO(pg-port): utils/sharedtuplestore.c
unsafe fn sts_end_write(acc: *mut SharedTuplestoreAccessor) { let _ = acc; }
// TODO(pg-port): utils/sharedtuplestore.c
unsafe fn sts_puttuple(acc: *mut SharedTuplestoreAccessor, meta_data: *mut c_void, tuple: MinimalTuple) {
    let _ = (acc, meta_data, tuple);
}
// TODO(pg-port): utils/sharedtuplestore.c
unsafe fn sts_parallel_scan_next(acc: *mut SharedTuplestoreAccessor, meta_data: *mut c_void) -> MinimalTuple {
    let _ = (acc, meta_data);
    ptr::null_mut()
}

// Use SharedFileSet/LWLock as defined in executor/hashjoin.rs (the structs that
// ParallelHashJoinState embeds), and pg_atomic_uint32 from port/atomics.h.
use crate::executor::hashjoin::{SharedFileSet, LWLock};
use crate::port::atomics::pg_atomic_uint32;

// TODO(pg-port): access/parallel.h -- ParallelContext / ParallelWorkerContext.
// Local opaque shm_toc types: the ones in nodeHash.rs are private to that
// module, so we declare our own stand-ins here.
#[repr(C)]
pub struct shm_toc { _opaque: [u8; 0] }
#[repr(C)]
pub struct shm_toc_estimator { _opaque: [u8; 0] }
#[repr(C)]
pub struct dsm_segment { _opaque: [u8; 0] }

#[repr(C)]
pub struct ParallelContext {
    pub nworkers: c_int,
    pub seg: *mut dsm_segment,
    pub toc: *mut shm_toc,
    pub estimator: shm_toc_estimator,
}

#[repr(C)]
pub struct ParallelWorkerContext {
    pub seg: *mut dsm_segment,
    pub toc: *mut shm_toc,
}

// TODO(pg-port): port/atomics.h
unsafe fn pg_atomic_fetch_add_u32(ptr: *mut pg_atomic_uint32, add_: u32) -> u32 {
    let _ = (ptr, add_);
    0
}
// TODO(pg-port): port/atomics.h
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    let _ = (ptr, val);
}

// TODO(pg-port): storage/sharedfileset.c
unsafe fn SharedFileSetInit(fileset: *mut SharedFileSet, seg: *mut dsm_segment) {
    let _ = (fileset, seg);
}
// TODO(pg-port): storage/sharedfileset.c
unsafe fn SharedFileSetAttach(fileset: *mut SharedFileSet, seg: *mut dsm_segment) {
    let _ = (fileset, seg);
}
// TODO(pg-port): storage/sharedfileset.c
unsafe fn SharedFileSetDeleteAll(fileset: *mut SharedFileSet) {
    let _ = fileset;
}

// TODO(pg-port): storage/lwlock.c
unsafe fn LWLockInitialize(lock: *mut LWLock, tranche_id: c_int) {
    let _ = (lock, tranche_id);
}
const LWTRANCHE_PARALLEL_HASH_JOIN: c_int = 0;

// TODO(pg-port): storage/shm_toc.c
unsafe fn shm_toc_estimate_chunk(e: *mut shm_toc_estimator, sz: usize) {
    let _ = (e, sz);
}
// TODO(pg-port): storage/shm_toc.c
unsafe fn shm_toc_estimate_keys(e: *mut shm_toc_estimator, cnt: usize) {
    let _ = (e, cnt);
}
// TODO(pg-port): storage/shm_toc.c
unsafe fn shm_toc_allocate(toc: *mut shm_toc, nbytes: usize) -> *mut c_void {
    let _ = (toc, nbytes);
    ptr::null_mut()
}
// TODO(pg-port): storage/shm_toc.c
unsafe fn shm_toc_insert(toc: *mut shm_toc, key: u64, address: *mut c_void) {
    let _ = (toc, key, address);
}
// TODO(pg-port): storage/shm_toc.c
unsafe fn shm_toc_lookup(toc: *mut shm_toc, key: u64, noError: bool) -> *mut c_void {
    let _ = (toc, key, noError);
    ptr::null_mut()
}

// TODO(pg-port): utils/dsa.h - InvalidDsaPointer
const InvalidDsaPointer: dsa_pointer = 0;

/* ----------------------------------------------------------------
 *		ExecHashJoinImpl
 *
 *		This function implements the Hybrid Hashjoin algorithm.  It is marked
 *		with an always-inline attribute so that ExecHashJoin() and
 *		ExecParallelHashJoin() can inline it.  Compilers that respect the
 *		attribute should create versions specialized for parallel == true and
 *		parallel == false with unnecessary branches removed.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
#[inline(always)]
unsafe fn ExecHashJoinImpl(pstate: *mut PlanState, parallel: bool) -> *mut TupleTableSlot {
    let node: *mut HashJoinState = castNode!(HashJoinState, T_HashJoinState, pstate);
    let outerNode: *mut PlanState;
    let hashNode: *mut HashState;
    let joinqual: *mut ExprState;
    let otherqual: *mut ExprState;
    let econtext: *mut ExprContext;
    let mut hashtable: HashJoinTable;
    let mut outerTupleSlot: *mut TupleTableSlot;
    let mut hashvalue: uint32 = 0;
    let mut batchno: c_int = 0;
    let parallel_state: *mut ParallelHashJoinState;

    /*
     * get information from HashJoin node
     */
    joinqual = (*node).js.joinqual;
    otherqual = (*node).js.ps.qual;
    hashNode = innerPlanState(node as *mut PlanState) as *mut HashState;
    outerNode = outerPlanState(node as *mut PlanState);
    hashtable = (*node).hj_HashTable;
    econtext = (*node).js.ps.ps_ExprContext;
    parallel_state = (*hashNode).parallel_state;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * run the hash join state machine
     */
    loop {
        /*
         * It's possible to iterate this loop many times before returning a
         * tuple, in some pathological cases such as needing to move much of
         * the current batch to a later batch.  So let's check for interrupts
         * each time through.
         */
        CHECK_FOR_INTERRUPTS!();

        match (*node).hj_JoinState {
            HJ_BUILD_HASHTABLE => {
                /*
                 * First time through: build hash table for inner relation.
                 */
                Assert!(hashtable.is_null());

                /*
                 * If the outer relation is completely empty, and it's not
                 * right/right-anti/full join, we can quit without building
                 * the hash table.  However, for an inner join it is only a
                 * win to check this when the outer relation's startup cost is
                 * less than the projected cost of building the hash table.
                 * Otherwise it's best to build the hash table first and see
                 * if the inner relation is empty.  (When it's a left join, we
                 * should always make this check, since we aren't going to be
                 * able to skip the join on the strength of an empty inner
                 * relation anyway.)
                 *
                 * If we are rescanning the join, we make use of information
                 * gained on the previous scan: don't bother to try the
                 * prefetch if the previous scan found the outer relation
                 * nonempty. This is not 100% reliable since with new
                 * parameters the outer relation might yield different
                 * results, but it's a good heuristic.
                 *
                 * The only way to make the check is to try to fetch a tuple
                 * from the outer plan node.  If we succeed, we have to stash
                 * it away for later consumption by ExecHashJoinOuterGetTuple.
                 */
                if HJ_FILL_INNER(node) {
                    /* no chance to not build the hash table */
                    (*node).hj_FirstOuterTupleSlot = ptr::null_mut();
                } else if parallel {
                    /*
                     * The empty-outer optimization is not implemented for
                     * shared hash tables, because no one participant can
                     * determine that there are no outer tuples, and it's not
                     * yet clear that it's worth the synchronization overhead
                     * of reaching consensus to figure that out.  So we have
                     * to build the hash table.
                     */
                    (*node).hj_FirstOuterTupleSlot = ptr::null_mut();
                } else if HJ_FILL_OUTER(node)
                    || ((*(*outerNode).plan).startup_cost < (*(*hashNode).ps.plan).total_cost
                        && !(*node).hj_OuterNotEmpty)
                {
                    (*node).hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
                    if TupIsNull((*node).hj_FirstOuterTupleSlot) {
                        (*node).hj_OuterNotEmpty = false;
                        return ptr::null_mut();
                    } else {
                        (*node).hj_OuterNotEmpty = true;
                    }
                } else {
                    (*node).hj_FirstOuterTupleSlot = ptr::null_mut();
                }

                /*
                 * Create the hash table.  If using Parallel Hash, then
                 * whoever gets here first will create the hash table and any
                 * later arrivals will merely attach to it.
                 */
                hashtable = ExecHashTableCreate(hashNode);
                (*node).hj_HashTable = hashtable;

                /*
                 * Execute the Hash node, to build the hash table.  If using
                 * Parallel Hash, then we'll try to help hashing unless we
                 * arrived too late.
                 */
                (*hashNode).hashtable = hashtable;
                MultiExecProcNode(hashNode as *mut PlanState);

                /*
                 * If the inner relation is completely empty, and we're not
                 * doing a left outer join, we can quit without scanning the
                 * outer relation.
                 */
                if (*hashtable).totalTuples == 0.0 && !HJ_FILL_OUTER(node) {
                    if parallel {
                        /*
                         * Advance the build barrier to PHJ_BUILD_RUN before
                         * proceeding so we can negotiate resource cleanup.
                         */
                        let build_barrier: *mut Barrier = &mut (*parallel_state).build_barrier;

                        while BarrierPhase(build_barrier) < PHJ_BUILD_RUN {
                            BarrierArriveAndWait(build_barrier, 0);
                        }
                    }
                    return ptr::null_mut();
                }

                /*
                 * need to remember whether nbatch has increased since we
                 * began scanning the outer relation
                 */
                (*hashtable).nbatch_outstart = (*hashtable).nbatch;

                /*
                 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
                 * tuple above, because ExecHashJoinOuterGetTuple will
                 * immediately set it again.)
                 */
                (*node).hj_OuterNotEmpty = false;

                if parallel {
                    let build_barrier: *mut Barrier;

                    build_barrier = &mut (*parallel_state).build_barrier;
                    Assert!(BarrierPhase(build_barrier) == PHJ_BUILD_HASH_OUTER
                        || BarrierPhase(build_barrier) == PHJ_BUILD_RUN
                        || BarrierPhase(build_barrier) == PHJ_BUILD_FREE);
                    if BarrierPhase(build_barrier) == PHJ_BUILD_HASH_OUTER {
                        /*
                         * If multi-batch, we need to hash the outer relation
                         * up front.
                         */
                        if (*hashtable).nbatch > 1 {
                            ExecParallelHashJoinPartitionOuter(node);
                        }
                        BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_HASH_OUTER);
                    } else if BarrierPhase(build_barrier) == PHJ_BUILD_FREE {
                        /*
                         * If we attached so late that the job is finished and
                         * the batch state has been freed, we can return
                         * immediately.
                         */
                        return ptr::null_mut();
                    }

                    /* Each backend should now select a batch to work on. */
                    Assert!(BarrierPhase(build_barrier) == PHJ_BUILD_RUN);
                    (*hashtable).curbatch = -1;
                    (*node).hj_JoinState = HJ_NEED_NEW_BATCH;

                    continue;
                } else {
                    (*node).hj_JoinState = HJ_NEED_NEW_OUTER;
                }

                /* FALL THRU */
                // C falls through into HJ_NEED_NEW_OUTER; we have set
                // hj_JoinState above, so re-dispatch via the loop.
                continue;
            }

            HJ_NEED_NEW_OUTER => {
                /*
                 * We don't have an outer tuple, try to get the next one
                 */
                if parallel {
                    outerTupleSlot =
                        ExecParallelHashJoinOuterGetTuple(outerNode, node, &mut hashvalue);
                } else {
                    outerTupleSlot =
                        ExecHashJoinOuterGetTuple(outerNode, node, &mut hashvalue);
                }

                if TupIsNull(outerTupleSlot) {
                    /* end of batch, or maybe whole join */
                    if HJ_FILL_INNER(node) {
                        /* set up to scan for unmatched inner tuples */
                        if parallel {
                            /*
                             * Only one process is currently allow to handle
                             * each batch's unmatched tuples, in a parallel
                             * join.
                             */
                            if ExecParallelPrepHashTableForUnmatched(node) {
                                (*node).hj_JoinState = HJ_FILL_INNER_TUPLES;
                            } else {
                                (*node).hj_JoinState = HJ_NEED_NEW_BATCH;
                            }
                        } else {
                            ExecPrepHashTableForUnmatched(node);
                            (*node).hj_JoinState = HJ_FILL_INNER_TUPLES;
                        }
                    } else {
                        (*node).hj_JoinState = HJ_NEED_NEW_BATCH;
                    }
                    continue;
                }

                (*econtext).ecxt_outertuple = outerTupleSlot;
                (*node).hj_MatchedOuter = false;

                /*
                 * Find the corresponding bucket for this tuple in the main
                 * hash table or skew hash table.
                 */
                (*node).hj_CurHashValue = hashvalue;
                ExecHashGetBucketAndBatch(
                    hashtable, hashvalue, &mut (*node).hj_CurBucketNo, &mut batchno,
                );
                (*node).hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable, hashvalue);
                (*node).hj_CurTuple = ptr::null_mut();

                /*
                 * The tuple might not belong to the current batch (where
                 * "current batch" includes the skew buckets if any).
                 */
                if batchno != (*hashtable).curbatch
                    && (*node).hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO
                {
                    let mut shouldFree: bool = false;
                    let mintuple: MinimalTuple =
                        ExecFetchSlotMinimalTuple(outerTupleSlot, &mut shouldFree);

                    /*
                     * Need to postpone this outer tuple to a later batch.
                     * Save it in the corresponding outer-batch file.
                     */
                    Assert!(parallel_state.is_null());
                    Assert!(batchno > (*hashtable).curbatch);
                    ExecHashJoinSaveTuple(
                        mintuple,
                        hashvalue,
                        (*hashtable).outerBatchFile.add(batchno as usize),
                        hashtable,
                    );

                    if shouldFree {
                        heap_free_minimal_tuple(mintuple);
                    }

                    /* Loop around, staying in HJ_NEED_NEW_OUTER state */
                    continue;
                }

                /* OK, let's scan the bucket for matches */
                (*node).hj_JoinState = HJ_SCAN_BUCKET;

                /* FALL THRU */
                // fallthrough handled by re-looping into HJ_SCAN_BUCKET
                continue;
            }

            HJ_SCAN_BUCKET => {
                /*
                 * Scan the selected hash bucket for matches to current outer
                 */
                if parallel {
                    if !ExecParallelScanHashBucket(node, econtext) {
                        /* out of matches; check for possible outer-join fill */
                        (*node).hj_JoinState = HJ_FILL_OUTER_TUPLE;
                        continue;
                    }
                } else {
                    if !ExecScanHashBucket(node, econtext) {
                        /* out of matches; check for possible outer-join fill */
                        (*node).hj_JoinState = HJ_FILL_OUTER_TUPLE;
                        continue;
                    }
                }

                /*
                 * In a right-semijoin, we only need the first match for each
                 * inner tuple.
                 */
                if (*node).js.jointype == JOIN_RIGHT_SEMI
                    && HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE((*node).hj_CurTuple))
                {
                    continue;
                }

                /*
                 * We've got a match, but still need to test non-hashed quals.
                 * ExecScanHashBucket already set up all the state needed to
                 * call ExecQual.
                 *
                 * If we pass the qual, then save state for next call and have
                 * ExecProject form the projection, store it in the tuple
                 * table, and return the slot.
                 *
                 * Only the joinquals determine tuple match status, but all
                 * quals must pass to actually return the tuple.
                 */
                if joinqual.is_null() || ExecQual(joinqual, econtext) {
                    (*node).hj_MatchedOuter = true;

                    /*
                     * This is really only needed if HJ_FILL_INNER(node) or if
                     * we are in a right-semijoin, but we'll avoid the branch
                     * and just set it always.
                     */
                    if !HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE((*node).hj_CurTuple)) {
                        HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE((*node).hj_CurTuple));
                    }

                    /* In an antijoin, we never return a matched tuple */
                    if (*node).js.jointype == JOIN_ANTI {
                        (*node).hj_JoinState = HJ_NEED_NEW_OUTER;
                        continue;
                    }

                    /*
                     * If we only need to consider the first matching inner
                     * tuple, then advance to next outer tuple after we've
                     * processed this one.
                     */
                    if (*node).js.single_match {
                        (*node).hj_JoinState = HJ_NEED_NEW_OUTER;
                    }

                    /*
                     * In a right-antijoin, we never return a matched tuple.
                     * If it's not an inner_unique join, we need to stay on
                     * the current outer tuple to continue scanning the inner
                     * side for matches.
                     */
                    if (*node).js.jointype == JOIN_RIGHT_ANTI {
                        continue;
                    }

                    if otherqual.is_null() || ExecQual(otherqual, econtext) {
                        return ExecProject((*node).js.ps.ps_ProjInfo);
                    } else {
                        InstrCountFiltered2(node, 1.0);
                    }
                } else {
                    InstrCountFiltered1(node, 1.0);
                }
            }

            HJ_FILL_OUTER_TUPLE => {
                /*
                 * The current outer tuple has run out of matches, so check
                 * whether to emit a dummy outer-join tuple.  Whether we emit
                 * one or not, the next state is NEED_NEW_OUTER.
                 */
                (*node).hj_JoinState = HJ_NEED_NEW_OUTER;

                if !(*node).hj_MatchedOuter && HJ_FILL_OUTER(node) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    (*econtext).ecxt_innertuple = (*node).hj_NullInnerTupleSlot;

                    if otherqual.is_null() || ExecQual(otherqual, econtext) {
                        return ExecProject((*node).js.ps.ps_ProjInfo);
                    } else {
                        InstrCountFiltered2(node, 1.0);
                    }
                }
            }

            HJ_FILL_INNER_TUPLES => {
                /*
                 * We have finished a batch, but we are doing
                 * right/right-anti/full join, so any unmatched inner tuples
                 * in the hashtable have to be emitted before we continue to
                 * the next batch.
                 */
                if !(if parallel {
                    ExecParallelScanHashTableForUnmatched(node, econtext)
                } else {
                    ExecScanHashTableForUnmatched(node, econtext)
                }) {
                    /* no more unmatched tuples */
                    (*node).hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                /*
                 * Generate a fake join tuple with nulls for the outer tuple,
                 * and return it if it passes the non-join quals.
                 */
                (*econtext).ecxt_outertuple = (*node).hj_NullOuterTupleSlot;

                if otherqual.is_null() || ExecQual(otherqual, econtext) {
                    return ExecProject((*node).js.ps.ps_ProjInfo);
                } else {
                    InstrCountFiltered2(node, 1.0);
                }
            }

            HJ_NEED_NEW_BATCH => {
                /*
                 * Try to advance to next batch.  Done if there are no more.
                 */
                if parallel {
                    if !ExecParallelHashJoinNewBatch(node) {
                        return ptr::null_mut(); /* end of parallel-aware join */
                    }
                } else {
                    if !ExecHashJoinNewBatch(node) {
                        return ptr::null_mut(); /* end of parallel-oblivious join */
                    }
                }
                (*node).hj_JoinState = HJ_NEED_NEW_OUTER;
            }

            _ => {
                elog!(ERROR, "unrecognized hashjoin state: {}", (*node).hj_JoinState);
            }
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
unsafe fn ExecHashJoin(pstate: *mut PlanState) -> *mut TupleTableSlot {
    /* return: a tuple or NULL */
    /*
     * On sufficiently smart compilers this should be inlined with the
     * parallel-aware branches removed.
     */
    ExecHashJoinImpl(pstate, false)
}

/* ----------------------------------------------------------------
 *		ExecParallelHashJoin
 *
 *		Parallel-aware version.
 * ----------------------------------------------------------------
 */
unsafe fn ExecParallelHashJoin(pstate: *mut PlanState) -> *mut TupleTableSlot {
    /* return: a tuple or NULL */
    /*
     * On sufficiently smart compilers this should be inlined with the
     * parallel-oblivious branches removed.
     */
    ExecHashJoinImpl(pstate, true)
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitHashJoin(
    node: *mut HashJoin,
    estate: *mut EState,
    eflags: c_int,
) -> *mut HashJoinState {
    let hjstate: *mut HashJoinState;
    let outerNode: *mut Plan;
    let hashNode: *mut Hash;
    let outerDesc: TupleDesc;
    let innerDesc: TupleDesc;
    let ops: *const TupleTableSlotOps;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create state structure
     */
    hjstate = makeNode!(HashJoinState, T_HashJoinState);
    (*hjstate).js.ps.plan = node as *mut Plan;
    (*hjstate).js.ps.state = estate;

    /*
     * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
     * where this function may be replaced with a parallel version, if we
     * managed to launch a parallel query.
     */
    (*hjstate).js.ps.ExecProcNode = Some(ExecHashJoin);
    (*hjstate).js.jointype = (*node).join.jointype;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*hjstate).js.ps);

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    outerNode = outerPlan(node as *mut Plan);
    hashNode = innerPlan(node as *mut Plan) as *mut Hash;

    (*hjstate).js.ps.lefttree = ExecInitNode(outerNode, estate, eflags);
    outerDesc = ExecGetResultType(outerPlanState(hjstate as *mut PlanState));
    (*hjstate).js.ps.righttree = ExecInitNode(hashNode as *mut Plan, estate, eflags);
    innerDesc = ExecGetResultType(innerPlanState(hjstate as *mut PlanState));

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*hjstate).js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*hjstate).js.ps, ptr::null_mut());

    /*
     * tuple table initialization
     */
    ops = ExecGetResultSlotOps(outerPlanState(hjstate as *mut PlanState), ptr::null_mut());
    (*hjstate).hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc, ops);

    /*
     * detect whether we need only consider the first matching inner tuple
     */
    (*hjstate).js.single_match =
        (*node).join.inner_unique || (*node).join.jointype == JOIN_SEMI;

    /* set up null tuples for outer joins, if needed */
    match (*node).join.jointype {
        JOIN_INNER | JOIN_SEMI | JOIN_RIGHT_SEMI => {}
        JOIN_LEFT | JOIN_ANTI => {
            (*hjstate).hj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
        }
        JOIN_RIGHT | JOIN_RIGHT_ANTI => {
            (*hjstate).hj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
        }
        JOIN_FULL => {
            (*hjstate).hj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
            (*hjstate).hj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
        }
        _ => {
            elog!(ERROR, "unrecognized join type: {}", (*node).join.jointype as c_int);
        }
    }

    /*
     * now for some voodoo.  our temporary tuple slot is actually the result
     * tuple slot of the Hash node (which is our inner plan).  we can do this
     * because Hash nodes don't return tuples via ExecProcNode() -- instead
     * the hash join node uses ExecScanHashBucket() to get at the contents of
     * the hash table.  -cim 6/9/91
     */
    {
        let hashstate: *mut HashState = innerPlanState(hjstate as *mut PlanState) as *mut HashState;
        let hash: *mut Hash = (*hashstate).ps.plan as *mut Hash;
        let slot: *mut TupleTableSlot = (*hashstate).ps.ps_ResultTupleSlot;
        let outer_hashfuncid: *mut Oid;
        let inner_hashfuncid: *mut Oid;
        let hash_strict: *mut bool;
        let nkeys: c_int;

        (*hjstate).hj_HashTupleSlot = slot;

        /*
         * Build ExprStates to obtain hash values for either side of the join.
         * This must be done here as ExecBuildHash32Expr needs to know how to
         * handle NULL inputs and the required handling of that depends on the
         * jointype.  We don't know the join type in ExecInitHash() and we
         * must build the ExprStates before ExecHashTableCreate() so we
         * properly attribute any SubPlans that exist in the hash expressions
         * to the correct PlanState.
         */
        nkeys = list_length((*node).hashoperators);

        outer_hashfuncid = palloc_array!(Oid, nkeys);
        inner_hashfuncid = palloc_array!(Oid, nkeys);
        hash_strict = palloc_array!(bool, nkeys);

        /*
         * Determine the hash function for each side of the join for the given
         * hash operator.
         */
        foreach!(lc, (*node).hashoperators, {
            let hashop: Oid = lfirst_oid(current_cell!(lc));
            let i: c_int = foreach_current_index!(lc);

            if !get_op_hash_functions(
                hashop,
                outer_hashfuncid.add(i as usize),
                inner_hashfuncid.add(i as usize),
            ) {
                elog!(
                    ERROR,
                    "could not find hash function for hash operator {}",
                    hashop
                );
            }
            *hash_strict.add(i as usize) = op_strict(hashop);
        });

        /*
         * Build an ExprState to generate the hash value for the expressions
         * on the outer of the join.  This ExprState must finish generating
         * the hash value when HJ_FILL_OUTER() is true.  Otherwise,
         * ExecBuildHash32Expr will set up the ExprState to abort early if it
         * finds a NULL.  In these cases, we don't need to store these tuples
         * in the hash table as the jointype does not require it.
         */
        (*hjstate).hj_OuterHash = ExecBuildHash32Expr(
            (*hjstate).js.ps.ps_ResultTupleDesc,
            (*hjstate).js.ps.resultops,
            outer_hashfuncid,
            (*node).hashcollations,
            (*node).hashkeys,
            hash_strict,
            &mut (*hjstate).js.ps,
            0,
            HJ_FILL_OUTER(hjstate),
        );

        /* As above, but for the inner side of the join */
        (*hashstate).hash_expr = ExecBuildHash32Expr(
            (*hashstate).ps.ps_ResultTupleDesc,
            (*hashstate).ps.resultops,
            inner_hashfuncid,
            (*node).hashcollations,
            (*hash).hashkeys,
            hash_strict,
            &mut (*hashstate).ps,
            0,
            HJ_FILL_INNER(hjstate),
        );

        /*
         * Set up the skew table hash function while we have a record of the
         * first key's hash function Oid.
         */
        if OidIsValid((*hash).skewTable) {
            (*hashstate).skew_hashfunction = palloc0(size_of::<FmgrInfo>()) as *mut FmgrInfo;
            (*hashstate).skew_collation = linitial_oid((*node).hashcollations);
            fmgr_info(*outer_hashfuncid.add(0), (*hashstate).skew_hashfunction);
        }

        /* no need to keep these */
        pfree(outer_hashfuncid as *mut c_void);
        pfree(inner_hashfuncid as *mut c_void);
        pfree(hash_strict as *mut c_void);
    }

    /*
     * initialize child expressions
     */
    (*hjstate).js.ps.qual =
        ExecInitQual((*node).join.plan.qual, hjstate as *mut PlanState);
    (*hjstate).js.joinqual =
        ExecInitQual((*node).join.joinqual, hjstate as *mut PlanState);
    (*hjstate).hashclauses =
        ExecInitQual((*node).hashclauses, hjstate as *mut PlanState);

    /*
     * initialize hash-specific info
     */
    (*hjstate).hj_HashTable = ptr::null_mut();
    (*hjstate).hj_FirstOuterTupleSlot = ptr::null_mut();

    (*hjstate).hj_CurHashValue = 0;
    (*hjstate).hj_CurBucketNo = 0;
    (*hjstate).hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    (*hjstate).hj_CurTuple = ptr::null_mut();

    (*hjstate).hj_JoinState = HJ_BUILD_HASHTABLE;
    (*hjstate).hj_MatchedOuter = false;
    (*hjstate).hj_OuterNotEmpty = false;

    hjstate
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndHashJoin(node: *mut HashJoinState) {
    /*
     * Free hash table
     */
    if !(*node).hj_HashTable.is_null() {
        ExecHashTableDestroy((*node).hj_HashTable);
        (*node).hj_HashTable = ptr::null_mut();
    }

    /*
     * clean up subtrees
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
    ExecEndNode(innerPlanState(node as *mut PlanState));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for a parallel oblivious hashjoin: either by
 *		executing the outer plan node in the first pass, or from the temp
 *		files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
unsafe fn ExecHashJoinOuterGetTuple(
    outerNode: *mut PlanState,
    hjstate: *mut HashJoinState,
    hashvalue: *mut uint32,
) -> *mut TupleTableSlot {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let curbatch: c_int = (*hashtable).curbatch;
    let mut slot: *mut TupleTableSlot;

    if curbatch == 0 {
        /* if it is the first pass */
        /*
         * Check to see if first outer tuple was already fetched by
         * ExecHashJoin() and not used yet.
         */
        slot = (*hjstate).hj_FirstOuterTupleSlot;
        if !TupIsNull(slot) {
            (*hjstate).hj_FirstOuterTupleSlot = ptr::null_mut();
        } else {
            slot = ExecProcNode(outerNode);
        }

        while !TupIsNull(slot) {
            let mut isnull: bool = false;

            /*
             * We have to compute the tuple's hash value.
             */
            let econtext: *mut ExprContext = (*hjstate).js.ps.ps_ExprContext;

            (*econtext).ecxt_outertuple = slot;

            ResetExprContext(econtext);

            *hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(
                (*hjstate).hj_OuterHash,
                econtext,
                &mut isnull,
            ));

            if !isnull {
                /* remember outer relation is not empty for possible rescan */
                (*hjstate).hj_OuterNotEmpty = true;

                return slot;
            }

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    } else if curbatch < (*hashtable).nbatch {
        let file: *mut BufFile = *(*hashtable).outerBatchFile.add(curbatch as usize);

        /*
         * In outer-join cases, we could get here even though the batch file
         * is empty.
         */
        if file.is_null() {
            return ptr::null_mut();
        }

        slot = ExecHashJoinGetSavedTuple(hjstate, file, hashvalue, (*hjstate).hj_OuterTupleSlot);
        if !TupIsNull(slot) {
            return slot;
        }
    }

    /* End of this batch */
    ptr::null_mut()
}

/*
 * ExecHashJoinOuterGetTuple variant for the parallel case.
 */
unsafe fn ExecParallelHashJoinOuterGetTuple(
    outerNode: *mut PlanState,
    hjstate: *mut HashJoinState,
    hashvalue: *mut uint32,
) -> *mut TupleTableSlot {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let curbatch: c_int = (*hashtable).curbatch;
    let mut slot: *mut TupleTableSlot;

    /*
     * In the Parallel Hash case we only run the outer plan directly for
     * single-batch hash joins.  Otherwise we have to go to batch files, even
     * for batch 0.
     */
    if curbatch == 0 && (*hashtable).nbatch == 1 {
        slot = ExecProcNode(outerNode);

        while !TupIsNull(slot) {
            let mut isnull: bool = false;

            let econtext: *mut ExprContext = (*hjstate).js.ps.ps_ExprContext;

            (*econtext).ecxt_outertuple = slot;

            ResetExprContext(econtext);

            *hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(
                (*hjstate).hj_OuterHash,
                econtext,
                &mut isnull,
            ));

            if !isnull {
                return slot;
            }

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    } else if curbatch < (*hashtable).nbatch {
        let tuple: MinimalTuple;

        tuple = sts_parallel_scan_next(
            (*(*hashtable).batches.add(curbatch as usize)).outer_tuples,
            hashvalue as *mut c_void,
        );
        if !tuple.is_null() {
            ExecForceStoreMinimalTuple(tuple, (*hjstate).hj_OuterTupleSlot, false);
            slot = (*hjstate).hj_OuterTupleSlot;
            return slot;
        } else {
            ExecClearTuple((*hjstate).hj_OuterTupleSlot);
        }
    }

    /* End of this batch */
    (*(*hashtable).batches.add(curbatch as usize)).outer_eof = true;

    ptr::null_mut()
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
unsafe fn ExecHashJoinNewBatch(hjstate: *mut HashJoinState) -> bool {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let nbatch: c_int;
    let mut curbatch: c_int;
    let innerFile: *mut BufFile;
    let mut slot: *mut TupleTableSlot;
    let mut hashvalue: uint32 = 0;

    nbatch = (*hashtable).nbatch;
    curbatch = (*hashtable).curbatch;

    if curbatch > 0 {
        /*
         * We no longer need the previous outer batch file; close it right
         * away to free disk space.
         */
        if !(*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null() {
            BufFileClose(*(*hashtable).outerBatchFile.add(curbatch as usize));
        }
        *(*hashtable).outerBatchFile.add(curbatch as usize) = ptr::null_mut();
    } else {
        /* we just finished the first batch */
        /*
         * Reset some of the skew optimization state variables, since we no
         * longer need to consider skew tuples after the first batch. The
         * memory context reset we are about to do will release the skew
         * hashtable itself.
         */
        (*hashtable).skewEnabled = false;
        (*hashtable).skewBucket = ptr::null_mut();
        (*hashtable).skewBucketNums = ptr::null_mut();
        (*hashtable).nSkewBuckets = 0;
        (*hashtable).spaceUsedSkew = 0;
    }

    /*
     * We can always skip over any batches that are completely empty on both
     * sides.  We can sometimes skip over batches that are empty on only one
     * side, but there are exceptions:
     *
     * 1. In a left/full outer join, we have to process outer batches even if
     * the inner batch is empty.  Similarly, in a right/right-anti/full outer
     * join, we have to process inner batches even if the outer batch is
     * empty.
     *
     * 2. If we have increased nbatch since the initial estimate, we have to
     * scan inner batches since they might contain tuples that need to be
     * reassigned to later inner batches.
     *
     * 3. Similarly, if we have increased nbatch since starting the outer
     * scan, we have to rescan outer batches in case they contain tuples that
     * need to be reassigned.
     */
    curbatch += 1;
    while curbatch < nbatch
        && ((*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null()
            || (*(*hashtable).innerBatchFile.add(curbatch as usize)).is_null())
    {
        if !(*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null()
            && HJ_FILL_OUTER(hjstate)
        {
            break; /* must process due to rule 1 */
        }
        if !(*(*hashtable).innerBatchFile.add(curbatch as usize)).is_null()
            && HJ_FILL_INNER(hjstate)
        {
            break; /* must process due to rule 1 */
        }
        if !(*(*hashtable).innerBatchFile.add(curbatch as usize)).is_null()
            && nbatch != (*hashtable).nbatch_original
        {
            break; /* must process due to rule 2 */
        }
        if !(*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null()
            && nbatch != (*hashtable).nbatch_outstart
        {
            break; /* must process due to rule 3 */
        }
        /* We can ignore this batch. */
        /* Release associated temp files right away. */
        if !(*(*hashtable).innerBatchFile.add(curbatch as usize)).is_null() {
            BufFileClose(*(*hashtable).innerBatchFile.add(curbatch as usize));
        }
        *(*hashtable).innerBatchFile.add(curbatch as usize) = ptr::null_mut();
        if !(*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null() {
            BufFileClose(*(*hashtable).outerBatchFile.add(curbatch as usize));
        }
        *(*hashtable).outerBatchFile.add(curbatch as usize) = ptr::null_mut();
        curbatch += 1;
    }

    if curbatch >= nbatch {
        return false; /* no more batches */
    }

    (*hashtable).curbatch = curbatch;

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashtable);

    innerFile = *(*hashtable).innerBatchFile.add(curbatch as usize);

    if !innerFile.is_null() {
        if BufFileSeek(innerFile, 0, 0, SEEK_SET) != 0 {
            ereport!(
                ERROR,
                errmsg!("could not rewind hash-join temporary file")
            );
            /* C also: errcode_for_file_access() */
        }

        loop {
            slot = ExecHashJoinGetSavedTuple(
                hjstate,
                innerFile,
                &mut hashvalue,
                (*hjstate).hj_HashTupleSlot,
            );
            if slot.is_null() {
                break;
            }
            /*
             * NOTE: some tuples may be sent to future batches.  Also, it is
             * possible for hashtable->nbatch to be increased here!
             */
            ExecHashTableInsert(hashtable, slot, hashvalue);
        }

        /*
         * after we build the hash table, the inner batch file is no longer
         * needed
         */
        BufFileClose(innerFile);
        *(*hashtable).innerBatchFile.add(curbatch as usize) = ptr::null_mut();
    }

    /*
     * Rewind outer batch file (if present), so that we can start reading it.
     */
    if !(*(*hashtable).outerBatchFile.add(curbatch as usize)).is_null() {
        if BufFileSeek(*(*hashtable).outerBatchFile.add(curbatch as usize), 0, 0, SEEK_SET) != 0 {
            ereport!(
                ERROR,
                errmsg!("could not rewind hash-join temporary file")
            );
            /* C also: errcode_for_file_access() */
        }
    }

    true
}

/*
 * Choose a batch to work on, and attach to it.  Returns true if successful,
 * false if there are no more batches.
 */
unsafe fn ExecParallelHashJoinNewBatch(hjstate: *mut HashJoinState) -> bool {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let start_batchno: c_int;
    let mut batchno: c_int;

    /*
     * If we were already attached to a batch, remember not to bother checking
     * it again, and detach from it (possibly freeing the hash table if we are
     * last to detach).
     */
    if (*hashtable).curbatch >= 0 {
        (*(*hashtable).batches.add((*hashtable).curbatch as usize)).done = true;
        ExecHashTableDetachBatch(hashtable);
    }

    /*
     * Search for a batch that isn't done.  We use an atomic counter to start
     * our search at a different batch in every participant when there are
     * more batches than participants.
     */
    batchno = (pg_atomic_fetch_add_u32(&mut (*(*hashtable).parallel_state).distributor, 1)
        % (*hashtable).nbatch as u32) as c_int;
    start_batchno = batchno;
    loop {
        let mut hashvalue: uint32 = 0;
        let tuple: MinimalTuple;
        let slot: *mut TupleTableSlot;

        if !(*(*hashtable).batches.add(batchno as usize)).done {
            let inner_tuples: *mut SharedTuplestoreAccessor;
            let batch_barrier: *mut Barrier =
                &mut (*(*(*hashtable).batches.add(batchno as usize)).shared).batch_barrier;

            match BarrierAttach(batch_barrier) {
                PHJ_BATCH_ELECT => {
                    /* One backend allocates the hash table. */
                    if BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_ELECT) {
                        ExecParallelHashTableAlloc(hashtable, batchno);
                    }
                    /* Fall through. */
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_ALLOCATE);
                    /* Fall through. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    let inner_tuples = (*(*hashtable).batches.add(batchno as usize)).inner_tuples;
                    sts_begin_parallel_scan(inner_tuples);
                    loop {
                        let tuple = sts_parallel_scan_next(inner_tuples, &mut hashvalue as *mut uint32 as *mut c_void);
                        if tuple.is_null() {
                            break;
                        }
                        ExecForceStoreMinimalTuple(tuple, (*hjstate).hj_HashTupleSlot, false);
                        let slot = (*hjstate).hj_HashTupleSlot;
                        ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue);
                    }
                    sts_end_parallel_scan(inner_tuples);
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_LOAD);
                    /* Fall through. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    sts_begin_parallel_scan((*(*hashtable).batches.add(batchno as usize)).outer_tuples);
                    return true;
                }

                PHJ_BATCH_ALLOCATE => {
                    /* Wait for allocation to complete. */
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_ALLOCATE);
                    /* Fall through. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    let inner_tuples = (*(*hashtable).batches.add(batchno as usize)).inner_tuples;
                    sts_begin_parallel_scan(inner_tuples);
                    loop {
                        let tuple = sts_parallel_scan_next(inner_tuples, &mut hashvalue as *mut uint32 as *mut c_void);
                        if tuple.is_null() {
                            break;
                        }
                        ExecForceStoreMinimalTuple(tuple, (*hjstate).hj_HashTupleSlot, false);
                        let slot = (*hjstate).hj_HashTupleSlot;
                        ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue);
                    }
                    sts_end_parallel_scan(inner_tuples);
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_LOAD);
                    /* Fall through. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    sts_begin_parallel_scan((*(*hashtable).batches.add(batchno as usize)).outer_tuples);
                    return true;
                }

                PHJ_BATCH_LOAD => {
                    /* Start (or join in) loading tuples. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    inner_tuples = (*(*hashtable).batches.add(batchno as usize)).inner_tuples;
                    sts_begin_parallel_scan(inner_tuples);
                    loop {
                        let tuple = sts_parallel_scan_next(inner_tuples, &mut hashvalue as *mut uint32 as *mut c_void);
                        if tuple.is_null() {
                            break;
                        }
                        ExecForceStoreMinimalTuple(tuple, (*hjstate).hj_HashTupleSlot, false);
                        let slot = (*hjstate).hj_HashTupleSlot;
                        ExecParallelHashTableInsertCurrentBatch(hashtable, slot, hashvalue);
                    }
                    sts_end_parallel_scan(inner_tuples);
                    BarrierArriveAndWait(batch_barrier, WAIT_EVENT_HASH_BATCH_LOAD);
                    /* Fall through. */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    sts_begin_parallel_scan((*(*hashtable).batches.add(batchno as usize)).outer_tuples);
                    return true;
                }

                PHJ_BATCH_PROBE => {
                    /*
                     * This batch is ready to probe.  Return control to
                     * caller. We stay attached to batch_barrier so that the
                     * hash table stays alive until everyone's finished
                     * probing it, but no participant is allowed to wait at
                     * this barrier again (or else a deadlock could occur).
                     * All attached participants must eventually detach from
                     * the barrier and one worker must advance the phase so
                     * that the final phase is reached.
                     */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    sts_begin_parallel_scan((*(*hashtable).batches.add(batchno as usize)).outer_tuples);

                    return true;
                }

                PHJ_BATCH_SCAN => {
                    /*
                     * In principle, we could help scan for unmatched tuples,
                     * since that phase is already underway (the thing we
                     * can't do under current deadlock-avoidance rules is wait
                     * for others to arrive at PHJ_BATCH_SCAN, because
                     * PHJ_BATCH_PROBE emits tuples, but in this case we just
                     * got here without waiting).  That is not yet done.  For
                     * now, we just detach and go around again.  We have to
                     * use ExecHashTableDetachBatch() because there's a small
                     * chance we'll be the last to detach, and then we're
                     * responsible for freeing memory.
                     */
                    ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
                    (*(*hashtable).batches.add(batchno as usize)).done = true;
                    ExecHashTableDetachBatch(hashtable);
                }

                PHJ_BATCH_FREE => {
                    /*
                     * Already done.  Detach and go around again (if any
                     * remain).
                     */
                    BarrierDetach(batch_barrier);
                    (*(*hashtable).batches.add(batchno as usize)).done = true;
                    (*hashtable).curbatch = -1;
                }

                _ => {
                    elog!(ERROR, "unexpected batch phase {}", BarrierPhase(batch_barrier));
                }
            }
        }
        batchno = (batchno + 1) % (*hashtable).nbatch;
        if batchno == start_batchno {
            break;
        }
    }

    false
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * fileptr points to a batch file in one of the hashtable arrays.
 *
 * The batch files (and their buffers) are allocated in the spill context
 * created for the hashtable.
 */
pub unsafe fn ExecHashJoinSaveTuple(
    tuple: MinimalTuple,
    hashvalue: uint32,
    fileptr: *mut *mut BufFile,
    hashtable: HashJoinTable,
) {
    let mut file: *mut BufFile = *fileptr;

    /*
     * The batch file is lazily created. If this is the first tuple written to
     * this batch, the batch file is created and its buffer is allocated in
     * the spillCxt context, NOT in the batchCxt.
     *
     * During the build phase, buffered files are created for inner batches.
     * Each batch's buffered file is closed (and its buffer freed) after the
     * batch is loaded into memory during the outer side scan. Therefore, it
     * is necessary to allocate the batch file buffer in a memory context
     * which outlives the batch itself.
     *
     * Also, we use spillCxt instead of hashCxt for a better accounting of the
     * spilling memory consumption.
     */
    if file.is_null() {
        let oldctx: MemoryContext = MemoryContextSwitchTo((*hashtable).spillCxt);

        file = BufFileCreateTemp(false);
        *fileptr = file;

        MemoryContextSwitchTo(oldctx);
    }

    BufFileWrite(file, &hashvalue as *const uint32 as *const c_void, size_of::<uint32>());
    BufFileWrite(file, tuple as *const c_void, (*tuple).t_len as usize);
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
unsafe fn ExecHashJoinGetSavedTuple(
    hjstate: *mut HashJoinState,
    file: *mut BufFile,
    hashvalue: *mut uint32,
    tupleSlot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let mut header: [uint32; 2] = [0; 2];
    let nread: usize;
    let tuple: MinimalTuple;

    /*
     * We check for interrupts here because this is typically taken as an
     * alternative code path to an ExecProcNode() call, which would include
     * such a check.
     */
    CHECK_FOR_INTERRUPTS!();

    /*
     * Since both the hash value and the MinimalTuple length word are uint32,
     * we can read them both in one BufFileRead() call without any type
     * cheating.
     */
    nread = BufFileReadMaybeEOF(
        file,
        header.as_mut_ptr() as *mut c_void,
        size_of::<[uint32; 2]>(),
        true,
    );
    if nread == 0 {
        /* end of file */
        ExecClearTuple(tupleSlot);
        return ptr::null_mut();
    }
    *hashvalue = header[0];
    tuple = palloc(header[1] as usize) as MinimalTuple;
    (*tuple).t_len = header[1];
    BufFileReadExact(
        file,
        (tuple as *mut c_char).add(size_of::<uint32>()) as *mut c_void,
        (header[1] - size_of::<uint32>() as uint32) as usize,
    );
    ExecForceStoreMinimalTuple(tuple, tupleSlot, true);
    tupleSlot
}

pub unsafe fn ExecReScanHashJoin(node: *mut HashJoinState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);
    let innerPlan: *mut PlanState = innerPlanState(node as *mut PlanState);

    /*
     * In a multi-batch join, we currently have to do rescans the hard way,
     * primarily because batch temp files may have already been released. But
     * if it's a single-batch join, and there is no parameter change for the
     * inner subnode, then we can just re-use the existing hash table without
     * rebuilding it.
     */
    if !(*node).hj_HashTable.is_null() {
        if (*(*node).hj_HashTable).nbatch == 1 && (*innerPlan).chgParam.is_null() {
            /*
             * Okay to reuse the hash table; needn't rescan inner, either.
             *
             * However, if it's a right/right-anti/right-semi/full join, we'd
             * better reset the inner-tuple match flags contained in the
             * table.
             */
            if HJ_FILL_INNER(node) || (*node).js.jointype == JOIN_RIGHT_SEMI {
                ExecHashTableResetMatchFlags((*node).hj_HashTable);
            }

            /*
             * Also, we need to reset our state about the emptiness of the
             * outer relation, so that the new scan of the outer will update
             * it correctly if it turns out to be empty this time. (There's no
             * harm in clearing it now because ExecHashJoin won't need the
             * info.  In the other cases, where the hash table doesn't exist
             * or we are destroying it, we leave this state alone because
             * ExecHashJoin will need it the first time through.)
             */
            (*node).hj_OuterNotEmpty = false;

            /* ExecHashJoin can skip the BUILD_HASHTABLE step */
            (*node).hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            /* must destroy and rebuild hash table */
            let hashNode: *mut HashState = castNode!(HashState, T_HashState, innerPlan);

            Assert!((*hashNode).hashtable == (*node).hj_HashTable);
            /* accumulate stats from old hash table, if wanted */
            /* (this should match ExecShutdownHash) */
            if !(*hashNode).ps.instrument.is_null() && (*hashNode).hinstrument.is_null() {
                (*hashNode).hinstrument =
                    palloc0(size_of::<HashInstrumentation>()) as *mut HashInstrumentation;
            }
            if !(*hashNode).hinstrument.is_null() {
                ExecHashAccumInstrumentation((*hashNode).hinstrument, (*hashNode).hashtable);
            }
            /* for safety, be sure to clear child plan node's pointer too */
            (*hashNode).hashtable = ptr::null_mut();

            ExecHashTableDestroy((*node).hj_HashTable);
            (*node).hj_HashTable = ptr::null_mut();
            (*node).hj_JoinState = HJ_BUILD_HASHTABLE;

            /*
             * if chgParam of subnode is not null then plan will be re-scanned
             * by first ExecProcNode.
             */
            if (*innerPlan).chgParam.is_null() {
                ExecReScan(innerPlan);
            }
        }
    }

    /* Always reset intra-tuple state */
    (*node).hj_CurHashValue = 0;
    (*node).hj_CurBucketNo = 0;
    (*node).hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    (*node).hj_CurTuple = ptr::null_mut();

    (*node).hj_MatchedOuter = false;
    (*node).hj_FirstOuterTupleSlot = ptr::null_mut();

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

pub unsafe fn ExecShutdownHashJoin(node: *mut HashJoinState) {
    if !(*node).hj_HashTable.is_null() {
        /*
         * Detach from shared state before DSM memory goes away.  This makes
         * sure that we don't have any pointers into DSM memory by the time
         * ExecEndHashJoin runs.
         */
        ExecHashTableDetachBatch((*node).hj_HashTable);
        ExecHashTableDetach((*node).hj_HashTable);
    }
}

unsafe fn ExecParallelHashJoinPartitionOuter(hjstate: *mut HashJoinState) {
    let outerState: *mut PlanState = outerPlanState(hjstate as *mut PlanState);
    let econtext: *mut ExprContext = (*hjstate).js.ps.ps_ExprContext;
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let mut slot: *mut TupleTableSlot;
    let mut hashvalue: uint32;
    let mut i: c_int;

    Assert!((*hjstate).hj_FirstOuterTupleSlot.is_null());

    /* Execute outer plan, writing all tuples to shared tuplestores. */
    loop {
        let mut isnull: bool = false;

        slot = ExecProcNode(outerState);
        if TupIsNull(slot) {
            break;
        }
        (*econtext).ecxt_outertuple = slot;

        ResetExprContext(econtext);

        hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(
            (*hjstate).hj_OuterHash,
            econtext,
            &mut isnull,
        ));

        if !isnull {
            let mut batchno: c_int = 0;
            let mut bucketno: c_int = 0;
            let mut shouldFree: bool = false;
            let mintup: MinimalTuple = ExecFetchSlotMinimalTuple(slot, &mut shouldFree);

            ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);
            sts_puttuple(
                (*(*hashtable).batches.add(batchno as usize)).outer_tuples,
                &mut hashvalue as *mut uint32 as *mut c_void,
                mintup,
            );

            if shouldFree {
                heap_free_minimal_tuple(mintup);
            }
        }
        CHECK_FOR_INTERRUPTS!();
    }

    /* Make sure all outer partitions are readable by any backend. */
    i = 0;
    while i < (*hashtable).nbatch {
        sts_end_write((*(*hashtable).batches.add(i as usize)).outer_tuples);
        i += 1;
    }
}

pub unsafe fn ExecHashJoinEstimate(state: *mut HashJoinState, pcxt: *mut ParallelContext) {
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, size_of::<ParallelHashJoinState>());
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
}

pub unsafe fn ExecHashJoinInitializeDSM(state: *mut HashJoinState, pcxt: *mut ParallelContext) {
    let plan_node_id: c_int = (*(*state).js.ps.plan).plan_node_id;
    let hashNode: *mut HashState;
    let pstate: *mut ParallelHashJoinState;

    /*
     * Disable shared hash table mode if we failed to create a real DSM
     * segment, because that means that we don't have a DSA area to work with.
     */
    if (*pcxt).seg.is_null() {
        return;
    }

    ExecSetExecProcNode(&mut (*state).js.ps, Some(ExecParallelHashJoin));

    /*
     * Set up the state needed to coordinate access to the shared hash
     * table(s), using the plan node ID as the toc key.
     */
    pstate = shm_toc_allocate((*pcxt).toc, size_of::<ParallelHashJoinState>())
        as *mut ParallelHashJoinState;
    shm_toc_insert((*pcxt).toc, plan_node_id as u64, pstate as *mut c_void);

    /*
     * Set up the shared hash join state with no batches initially.
     * ExecHashTableCreate() will prepare at least one later and set nbatch
     * and space_allowed.
     */
    (*pstate).nbatch = 0;
    (*pstate).space_allowed = 0;
    (*pstate).batches = InvalidDsaPointer;
    (*pstate).old_batches = InvalidDsaPointer;
    (*pstate).nbuckets = 0;
    (*pstate).growth = PHJ_GROWTH_OK;
    (*pstate).chunk_work_queue = InvalidDsaPointer;
    pg_atomic_init_u32(&mut (*pstate).distributor, 0);
    (*pstate).nparticipants = (*pcxt).nworkers + 1;
    (*pstate).total_tuples = 0;
    LWLockInitialize(&mut (*pstate).lock, LWTRANCHE_PARALLEL_HASH_JOIN);
    BarrierInit(&mut (*pstate).build_barrier, 0);
    BarrierInit(&mut (*pstate).grow_batches_barrier, 0);
    BarrierInit(&mut (*pstate).grow_buckets_barrier, 0);

    /* Set up the space we'll use for shared temporary files. */
    SharedFileSetInit(&mut (*pstate).fileset, (*pcxt).seg);

    /* Initialize the shared state in the hash node. */
    hashNode = innerPlanState(state as *mut PlanState) as *mut HashState;
    (*hashNode).parallel_state = pstate;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecHashJoinReInitializeDSM(state: *mut HashJoinState, pcxt: *mut ParallelContext) {
    let plan_node_id: c_int = (*(*state).js.ps.plan).plan_node_id;
    let pstate: *mut ParallelHashJoinState;

    /* Nothing to do if we failed to create a DSM segment. */
    if (*pcxt).seg.is_null() {
        return;
    }

    pstate = shm_toc_lookup((*pcxt).toc, plan_node_id as u64, false) as *mut ParallelHashJoinState;

    /*
     * It would be possible to reuse the shared hash table in single-batch
     * cases by resetting and then fast-forwarding build_barrier to
     * PHJ_BUILD_FREE and batch 0's batch_barrier to PHJ_BATCH_PROBE, but
     * currently shared hash tables are already freed by now (by the last
     * participant to detach from the batch).  We could consider keeping it
     * around for single-batch joins.  We'd also need to adjust
     * finalize_plan() so that it doesn't record a dummy dependency for
     * Parallel Hash nodes, preventing the rescan optimization.  For now we
     * don't try.
     */

    /* Detach, freeing any remaining shared memory. */
    if !(*state).hj_HashTable.is_null() {
        ExecHashTableDetachBatch((*state).hj_HashTable);
        ExecHashTableDetach((*state).hj_HashTable);
    }

    /* Clear any shared batch files. */
    SharedFileSetDeleteAll(&mut (*pstate).fileset);

    /* Reset build_barrier to PHJ_BUILD_ELECT so we can go around again. */
    BarrierInit(&mut (*pstate).build_barrier, 0);
}

pub unsafe fn ExecHashJoinInitializeWorker(
    state: *mut HashJoinState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let hashNode: *mut HashState;
    let plan_node_id: c_int = (*(*state).js.ps.plan).plan_node_id;
    let pstate: *mut ParallelHashJoinState =
        shm_toc_lookup((*pwcxt).toc, plan_node_id as u64, false) as *mut ParallelHashJoinState;

    /* Attach to the space for shared temporary files. */
    SharedFileSetAttach(&mut (*pstate).fileset, (*pwcxt).seg);

    /* Attach to the shared state in the hash node. */
    hashNode = innerPlanState(state as *mut PlanState) as *mut HashState;
    (*hashNode).parallel_state = pstate;

    ExecSetExecProcNode(&mut (*state).js.ps, Some(ExecParallelHashJoin));
}

/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *    Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/nodeHash.c
 *
 * See note on parallelism in nodeHashjoin.c.
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *      MultiExecHash   - generate an in-memory hash table of the relation
 *      ExecInitHash    - initialize node and subnodes
 *      ExecEndHash     - shutdown node and subnodes
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use crate::prelude::*;
use crate::pg_config::BLCKSZ;

use core::mem::size_of;
use core::ptr;

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::plannodes::{Plan, Hash};
use crate::nodes::execnodes::{
    PlanState, EState, ExprContext, ExprState, TupleTableSlot,
    HashState, HashInstrumentation, SharedHashInfo, HashJoinState,
};
use crate::executor::hashjoin::{
    HashJoinTable, HashJoinTableData, HashJoinTupleData,
    HashMemoryChunk, HashMemoryChunkData,
    HashSkewBucket,
    ParallelHashJoinState, ParallelHashJoinBatch, ParallelHashJoinBatchAccessor,
    ParallelHashGrowth,
    PHJ_BUILD_ELECT, PHJ_BUILD_ALLOCATE, PHJ_BUILD_HASH_INNER, PHJ_BUILD_HASH_OUTER,
    PHJ_BUILD_RUN, PHJ_BUILD_FREE,
    PHJ_BATCH_PROBE, PHJ_BATCH_SCAN, PHJ_BATCH_FREE,
    PHJ_GROW_BATCHES_ELECT, PHJ_GROW_BATCHES_REALLOCATE,
    PHJ_GROW_BATCHES_REPARTITION, PHJ_GROW_BATCHES_DECIDE, PHJ_GROW_BATCHES_FINISH,
    PHJ_GROW_BUCKETS_ELECT, PHJ_GROW_BUCKETS_REALLOCATE, PHJ_GROW_BUCKETS_REINSERT,
    PHJ_GROWTH_OK, PHJ_GROWTH_NEED_MORE_BATCHES, PHJ_GROWTH_NEED_MORE_BUCKETS,
    PHJ_GROWTH_DISABLED,
    PHJ_GROW_BATCHES_PHASE, PHJ_GROW_BUCKETS_PHASE,
    INVALID_SKEW_BUCKET_NO, SKEW_HASH_MEM_PERCENT, SKEW_MIN_OUTER_FRACTION,
    SKEW_BUCKET_OVERHEAD, HASH_CHUNK_SIZE, HASH_CHUNK_HEADER_SIZE, HASH_CHUNK_THRESHOLD,
    HASH_CHUNK_DATA, HJTUPLE_OVERHEAD, HJTUPLE_MINTUPLE,
    BufFile, SharedFileSet, SharedTuplestore, SharedTuplestoreAccessor,
    LWLock, dsa_pointer_atomic,
    EstimateParallelHashJoinBatch, NthParallelHashJoinBatch,
    ParallelHashJoinBatchInner, ParallelHashJoinBatchOuter,
    sts_estimate,
};
use crate::nodes::execnodes::dsa_area;
use crate::nodes::execnodes::dsa_pointer;
use crate::executor::executor::{
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
    ExecInitNode, ExecEndNode, ExecProcNode, ExecReScan,
    ExecAssignExprContext,
    ExecEvalExprSwitchContext,
    ExecInitResultTupleSlotTL,
};
use crate::executor::tuptable::{TupIsNull};
use crate::executor::execTuples::{
    TTSOpsMinimalTuple,
    ExecStoreMinimalTuple,
    ExecFetchSlotMinimalTuple,
};
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::access::htup_details::{MinimalTuple, MinimalTupleData, HeapTupleHeaderData};
use crate::utils::fmgr::FmgrInfo;
use crate::utils::hash::dynahash::my_log2;
use crate::port::pg_bitutils::{
    pg_nextpower2_32, pg_prevpower2_32,
    pg_nextpower2_size_t, pg_prevpower2_size_t,
    pg_rotate_right32,
};
use crate::pg_config_manual::MAXPGPATH;

// CHECK_FOR_INTERRUPTS is a macro in C (miscadmin.h); the real check lives in
// crate::miscadmin::CHECK_FOR_INTERRUPTS().  Wrap it so the call sites can keep
// the macro-call syntax `CHECK_FOR_INTERRUPTS!()`.
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        crate::miscadmin::CHECK_FOR_INTERRUPTS();
    }};
}

// TODO(pg-port): access/parallel.h
#[repr(C)]
pub struct ParallelContext {
    pub nworkers: c_int,
    pub toc: *mut shm_toc,
    pub estimator: shm_toc_estimator,
}

#[repr(C)]
pub struct ParallelWorkerContext {
    pub toc: *mut shm_toc,
}

// TODO(pg-port): storage/shm_toc.h stubs
#[repr(C)]
pub struct shm_toc { _opaque: [u8; 0] }
#[repr(C)]
pub struct shm_toc_estimator { _opaque: [u8; 0] }

#[inline]
unsafe fn shm_toc_estimate_chunk(_est: *mut shm_toc_estimator, _size: usize) { /* TODO(pg-port) */ }
#[inline]
unsafe fn shm_toc_estimate_keys(_est: *mut shm_toc_estimator, _nkeys: c_int) { /* TODO(pg-port) */ }
#[inline]
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _size: usize) -> *mut c_void { /* TODO(pg-port) */ null_mut() }
#[inline]
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: u64, _address: *mut c_void) { /* TODO(pg-port) */ }
#[inline]
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: u64, _noError: bool) -> *mut c_void { /* TODO(pg-port) */ null_mut() }

// TODO(pg-port): access/parallel.h -- ParallelWorkerNumber
static ParallelWorkerNumber: c_int = -1;

// TODO(pg-port): port/pg_bitutils.h -- add_size / mul_size stubs (real ones are in memutils)
#[inline]
unsafe fn add_size(a: Size, b: Size) -> Size { a.saturating_add(b) }
#[inline]
unsafe fn mul_size(a: Size, b: Size) -> Size { a.saturating_mul(b) }

// TODO(pg-port): utils/dynahash.h -- MaxAllocSize re-export
use crate::utils::memutils::MaxAllocSize;

// TODO(pg-port): storage/ipc/barrier.h stubs
use crate::storage::ipc::barrier::Barrier;

#[inline]
unsafe fn BarrierPhase(b: *mut Barrier) -> c_int { /* TODO(pg-port) */ 0 }
#[inline]
unsafe fn BarrierAttach(b: *mut Barrier) -> c_int { /* TODO(pg-port) */ 0 }
#[inline]
unsafe fn BarrierDetach(b: *mut Barrier) { /* TODO(pg-port) */ }
#[inline]
unsafe fn BarrierArriveAndWait(b: *mut Barrier, wait_event: u32) -> bool { /* TODO(pg-port) */ false }
#[inline]
unsafe fn BarrierArriveAndDetach(b: *mut Barrier) -> bool { /* TODO(pg-port) */ false }
#[inline]
unsafe fn BarrierArriveAndDetachExceptLast(b: *mut Barrier) -> bool { /* TODO(pg-port) */ false }
#[inline]
unsafe fn BarrierInit(b: *mut Barrier, n: c_int) { /* TODO(pg-port) */ }

// TODO(pg-port): WAIT_EVENT_HASH_* constants (from wait_event.h)
const WAIT_EVENT_HASH_BUILD_ALLOCATE: u32 = 0;
const WAIT_EVENT_HASH_BUILD_ELECT: u32 = 0;
const WAIT_EVENT_HASH_BUILD_HASH_INNER: u32 = 0;
const WAIT_EVENT_HASH_GROW_BATCHES_ELECT: u32 = 0;
const WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE: u32 = 0;
const WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION: u32 = 0;
const WAIT_EVENT_HASH_GROW_BATCHES_DECIDE: u32 = 0;
const WAIT_EVENT_HASH_GROW_BATCHES_FINISH: u32 = 0;
const WAIT_EVENT_HASH_GROW_BUCKETS_ELECT: u32 = 0;
const WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE: u32 = 0;
const WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT: u32 = 0;

// TODO(pg-port): DSA stubs
#[inline]
unsafe fn dsa_allocate(_area: *mut dsa_area, _size: usize) -> dsa_pointer { /* TODO(pg-port) */ 0 }
#[inline]
unsafe fn dsa_allocate0(_area: *mut dsa_area, _size: usize) -> dsa_pointer { /* TODO(pg-port) */ 0 }
#[inline]
unsafe fn dsa_free(_area: *mut dsa_area, _dp: dsa_pointer) { /* TODO(pg-port) */ }
#[inline]
unsafe fn dsa_get_address(_area: *mut dsa_area, _dp: dsa_pointer) -> *mut c_void { /* TODO(pg-port) */ null_mut() }
const InvalidDsaPointer: dsa_pointer = 0;
#[inline]
fn DsaPointerIsValid(p: dsa_pointer) -> bool { p != InvalidDsaPointer }
// dsa_pointer_atomic operations
#[inline]
unsafe fn dsa_pointer_atomic_read(a: *mut dsa_pointer_atomic) -> dsa_pointer { /* TODO(pg-port) */ 0 }
#[inline]
unsafe fn dsa_pointer_atomic_write(a: *mut dsa_pointer_atomic, val: dsa_pointer) { /* TODO(pg-port) */ }
#[inline]
unsafe fn dsa_pointer_atomic_init(a: *mut dsa_pointer_atomic, val: dsa_pointer) { /* TODO(pg-port) */ }
#[inline]
unsafe fn dsa_pointer_atomic_compare_exchange(
    a: *mut dsa_pointer_atomic, expected: *mut dsa_pointer, new: dsa_pointer,
) -> bool { /* TODO(pg-port) */ false }

// TODO(pg-port): storage/lwlock.h
const LW_EXCLUSIVE: c_int = 1;
#[inline]
unsafe fn LWLockAcquire(lock: *mut LWLock, mode: c_int) { /* TODO(pg-port) */ }
#[inline]
unsafe fn LWLockRelease(lock: *mut LWLock) { /* TODO(pg-port) */ }

// TODO(pg-port): utils/sharedtuplestore.h stubs
const SHARED_TUPLESTORE_SINGLE_PASS: c_int = 0;
#[inline]
unsafe fn sts_initialize(
    _sts: *mut SharedTuplestore, _npart: c_int, _mymember: c_int,
    _keysize: usize, _flags: c_int, _fileset: *mut SharedFileSet, _name: *const c_char,
) -> *mut SharedTuplestoreAccessor { /* TODO(pg-port) */ null_mut() }
#[inline]
unsafe fn sts_attach(
    _sts: *mut SharedTuplestore, _mymember: c_int, _fileset: *mut SharedFileSet,
) -> *mut SharedTuplestoreAccessor { /* TODO(pg-port) */ null_mut() }
#[inline]
unsafe fn sts_begin_parallel_scan(_acc: *mut SharedTuplestoreAccessor) { /* TODO(pg-port) */ }
#[inline]
unsafe fn sts_end_parallel_scan(_acc: *mut SharedTuplestoreAccessor) { /* TODO(pg-port) */ }
#[inline]
unsafe fn sts_end_write(_acc: *mut SharedTuplestoreAccessor) { /* TODO(pg-port) */ }
#[inline]
unsafe fn sts_puttuple(
    _acc: *mut SharedTuplestoreAccessor, _meta: *const c_void, _tuple: MinimalTuple,
) { /* TODO(pg-port) */ }
#[inline]
unsafe fn sts_parallel_scan_next(
    _acc: *mut SharedTuplestoreAccessor, _hashvalue: *mut uint32,
) -> MinimalTuple { /* TODO(pg-port) */ null_mut() }

// TODO(pg-port): catalog/pg_statistic.h / utils/lsyscache.h stubs
#[repr(C)]
pub struct AttStatsSlot {
    pub values: *mut Datum,
    pub nvalues: c_int,
    pub numbers: *mut f32,
    pub nnumbers: c_int,
}
const ATTSTATSSLOT_VALUES: c_int = 0x01;
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;
#[inline]
unsafe fn get_attstatsslot(
    _sslot: *mut AttStatsSlot, _tuple: *mut crate::access::htup_details::HeapTupleData,
    _kind: c_int, _collid: Oid, _flags: c_int,
) -> bool { /* TODO(pg-port) */ false }
#[inline]
unsafe fn free_attstatsslot(_sslot: *mut AttStatsSlot) { /* TODO(pg-port) */ }

// TODO(pg-port): utils/syscache.h stubs for SearchSysCache3 / ReleaseSysCache
#[inline]
unsafe fn SearchSysCache3(_cacheId: c_int, _k1: Datum, _k2: Datum, _k3: Datum)
    -> *mut crate::access::htup_details::HeapTupleData { /* TODO(pg-port) */ null_mut() }
#[inline]
unsafe fn ReleaseSysCache(_tuple: *mut crate::access::htup_details::HeapTupleData) { /* TODO(pg-port) */ }
#[inline]
unsafe fn HeapTupleIsValid(t: *mut crate::access::htup_details::HeapTupleData) -> bool { !t.is_null() }

// TODO(pg-port): catalog/pg_statistic.h constants
const STATRELATTINH: c_int = 0; // placeholder
const STATISTIC_KIND_MCV: c_int = 1;

// TODO(pg-port): utils/lsyscache.h stubs
#[inline]
unsafe fn Int16GetDatum(v: i16) -> Datum { v as Datum }
#[inline]
unsafe fn BoolGetDatum(v: bool) -> Datum { v as Datum }
#[inline]
unsafe fn ObjectIdGetDatum(v: Oid) -> Datum { v as Datum }

// TODO(pg-port): utils/fmgr.h FunctionCall1Coll
#[inline]
unsafe fn FunctionCall1Coll(
    _fn: *mut FmgrInfo, _collation: Oid, _arg1: Datum,
) -> Datum { /* TODO(pg-port) */ 0 }

// TODO(pg-port): access/htup_details.h match-flag helpers
#[inline]
unsafe fn HeapTupleHeaderClearMatch(t: MinimalTuple) { /* TODO(pg-port) */ }
#[inline]
unsafe fn HeapTupleHeaderHasMatch(t: MinimalTuple) -> bool { /* TODO(pg-port) */ false }

// TODO(pg-port): storage/buffile.h
#[inline]
unsafe fn BufFileClose(_f: *mut BufFile) { /* TODO(pg-port) */ }

// TODO(pg-port): commands/tablespace.h
#[inline]
unsafe fn PrepareTempTablespaces() { /* TODO(pg-port) */ }

// TODO(pg-port): executor/nodeHashjoin.h
#[inline]
unsafe fn ExecHashJoinSaveTuple(
    _tuple: MinimalTuple, _hashvalue: uint32,
    _fileptr: *mut *mut BufFile, _hashtable: HashJoinTable,
) { /* TODO(pg-port) */ }

// TODO(pg-port): access/htup_details.h heap_free_minimal_tuple
#[inline]
unsafe fn heap_free_minimal_tuple(_t: MinimalTuple) { /* TODO(pg-port) */ }

// TODO(pg-port): ExecQualAndReset (executor/executor.h)
#[inline]
unsafe fn ExecQualAndReset(_clauses: *mut ExprState, _econtext: *mut ExprContext) -> bool {
    /* TODO(pg-port) */ false
}
// TODO(pg-port): ResetExprContext
#[inline]
unsafe fn ResetExprContext(_econtext: *mut ExprContext) { /* TODO(pg-port) */ }
// TODO(pg-port): outerPlan / outerPlanState macros
#[inline]
unsafe fn outerPlan(node: *mut Hash) -> *mut Plan { /* TODO(pg-port) */ null_mut() }
#[inline]
unsafe fn outerPlanState(node: *mut HashState) -> *mut PlanState {
    use crate::nodes::execnodes::outerPlanState as _outerPlanState;
    _outerPlanState(node as *mut PlanState)
}
// TODO(pg-port): makeNode macro for HashState
macro_rules! makeNode_HashState {
    () => {{
        palloc0(size_of::<HashState>()) as *mut HashState
    }};
}
// TODO(pg-port): snprintf stub (use libc or format!)
#[inline]
unsafe fn snprintf_name(buf: *mut c_char, n: usize, i: c_int, nbatch: c_int) {
    /* write "i%dof%d" into buf -- real impl would use libc snprintf */
    let s = format!("i{}of{}\0", i, nbatch);
    let bytes = s.as_bytes();
    let copy = bytes.len().min(n);
    ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, copy);
}

/*
 * The HashJoinTable's bucket arrays and chunk storage hold pointers to
 * executor/hashjoin.h's HashJoinTupleData (the struct with the `next`/
 * `hashvalue` members), so use that struct here.  (The execnodes.rs
 * HashJoinTuple alias points at an opaque forward declaration and is not
 * usable for field access.)
 */
type HashJoinTuple = *mut HashJoinTupleData;

/* Target bucket loading (tuples per bucket) */
const NTUP_PER_BUCKET: usize = 1;

/* ----------------------------------------------------------------
 *      ExecHash
 *
 *      stub for pro forma compliance
 * ----------------------------------------------------------------
 */
unsafe fn ExecHash(pstate: *mut PlanState) -> *mut TupleTableSlot {
    elog!(ERROR, "Hash node does not support ExecProcNode call convention");
    null_mut()
}

/* ----------------------------------------------------------------
 *      MultiExecHash
 *
 *      build hash table for hashjoin, doing partitioning if more
 *      than one batch is required.
 * ----------------------------------------------------------------
 */
pub unsafe fn MultiExecHash(node: *mut HashState) -> *mut Node {
    /* must provide our own instrumentation support */
    if !(*node).ps.instrument.is_null() {
        InstrStartNode((*node).ps.instrument);
    }

    if !(*node).parallel_state.is_null() {
        MultiExecParallelHash(node);
    } else {
        MultiExecPrivateHash(node);
    }

    /* must provide our own instrumentation support */
    if !(*node).ps.instrument.is_null() {
        InstrStopNode((*node).ps.instrument, (*(*node).hashtable).partialTuples);
    }

    /*
     * We do not return the hash table directly because it's not a subtype of
     * Node, and so would violate the MultiExecProcNode API.  Instead, our
     * parent Hashjoin node is expected to know how to fish it out of our node
     * state.  Ugly but not really worth cleaning up, since Hashjoin knows
     * quite a bit more about Hash besides that.
     */
    null_mut()
}

/* ----------------------------------------------------------------
 *      MultiExecPrivateHash
 *
 *      parallel-oblivious version, building a backend-private
 *      hash table and (if necessary) batch files.
 * ----------------------------------------------------------------
 */
unsafe fn MultiExecPrivateHash(node: *mut HashState) {
    let outerNode: *mut PlanState;
    let hashtable: HashJoinTable;
    let mut slot: *mut TupleTableSlot;
    let econtext: *mut ExprContext;

    /*
     * get state info from node
     */
    outerNode = outerPlanState(node);
    hashtable = (*node).hashtable;

    /*
     * set expression context
     */
    econtext = (*node).ps.ps_ExprContext;

    /*
     * Get all tuples from the node below the Hash node and insert into the
     * hash table (or temp files).
     */
    loop {
        let mut isnull: bool = false;

        slot = ExecProcNode(outerNode);
        if TupIsNull(slot) {
            break;
        }
        /* We have to compute the hash value */
        (*econtext).ecxt_outertuple = slot;

        ResetExprContext(econtext);

        let hashdatum = ExecEvalExprSwitchContext((*node).hash_expr, econtext, &mut isnull);

        if !isnull {
            let hashvalue = DatumGetUInt32(hashdatum);
            let bucketNumber: c_int = ExecHashGetSkewBucket(hashtable, hashvalue);
            if bucketNumber != INVALID_SKEW_BUCKET_NO {
                /* It's a skew tuple, so put it into that hash table */
                ExecHashSkewTableInsert(hashtable, slot, hashvalue, bucketNumber);
                (*hashtable).skewTuples += 1.0;
            } else {
                /* Not subject to skew optimization, so insert normally */
                ExecHashTableInsert(hashtable, slot, hashvalue);
            }
            (*hashtable).totalTuples += 1.0;
        }
    }

    /* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
    if (*hashtable).nbuckets != (*hashtable).nbuckets_optimal {
        ExecHashIncreaseNumBuckets(hashtable);
    }

    /* Account for the buckets in spaceUsed (reported in EXPLAIN ANALYZE) */
    (*hashtable).spaceUsed +=
        (*hashtable).nbuckets as Size * size_of::<HashJoinTuple>();
    if (*hashtable).spaceUsed > (*hashtable).spacePeak {
        (*hashtable).spacePeak = (*hashtable).spaceUsed;
    }

    (*hashtable).partialTuples = (*hashtable).totalTuples;
}

/* ----------------------------------------------------------------
 *      MultiExecParallelHash
 *
 *      parallel-aware version, building a shared hash table and
 *      (if necessary) batch files using the combined effort of
 *      a set of co-operating backends.
 * ----------------------------------------------------------------
 */
unsafe fn MultiExecParallelHash(node: *mut HashState) {
    let pstate: *mut ParallelHashJoinState;
    let outerNode: *mut PlanState;
    let hashtable: HashJoinTable;
    let mut slot: *mut TupleTableSlot;
    let econtext: *mut ExprContext;
    let mut hashvalue: uint32;
    let build_barrier: *mut Barrier;
    let mut i: c_int;

    /*
     * get state info from node
     */
    outerNode = outerPlanState(node);
    hashtable = (*node).hashtable;

    /*
     * set expression context
     */
    econtext = (*node).ps.ps_ExprContext;

    /*
     * Synchronize the parallel hash table build.  At this stage we know that
     * the shared hash table has been or is being set up by
     * ExecHashTableCreate(), but we don't know if our peers have returned
     * from there or are here in MultiExecParallelHash(), and if so how far
     * through they are.  To find out, we check the build_barrier phase then
     * and jump to the right step in the build algorithm.
     */
    pstate = (*hashtable).parallel_state;
    build_barrier = &mut (*pstate).build_barrier;
    Assert!(BarrierPhase(build_barrier) >= PHJ_BUILD_ALLOCATE);
    match BarrierPhase(build_barrier) {
        x if x == PHJ_BUILD_ALLOCATE => {
            /*
             * Either I just allocated the initial hash table in
             * ExecHashTableCreate(), or someone else is doing that.  Either
             * way, wait for everyone to arrive here so we can proceed.
             */
            BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ALLOCATE);
            /* Fall through to PHJ_BUILD_HASH_INNER. */
            'hash_inner: {
                /*
                 * It's time to begin hashing, or if we just arrived here then
                 * hashing is already underway, so join in that effort.  While
                 * hashing we have to be prepared to help increase the number of
                 * batches or buckets at any time, and if we arrived here when
                 * that was already underway we'll have to help complete that work
                 * immediately so that it's safe to access batches and buckets
                 * below.
                 */
                if PHJ_GROW_BATCHES_PHASE(BarrierAttach(&mut (*pstate).grow_batches_barrier)) !=
                    PHJ_GROW_BATCHES_ELECT {
                    ExecParallelHashIncreaseNumBatches(hashtable);
                }
                if PHJ_GROW_BUCKETS_PHASE(BarrierAttach(&mut (*pstate).grow_buckets_barrier)) !=
                    PHJ_GROW_BUCKETS_ELECT {
                    ExecParallelHashIncreaseNumBuckets(hashtable);
                }
                ExecParallelHashEnsureBatchAccessors(hashtable);
                ExecParallelHashTableSetCurrentBatch(hashtable, 0);
                loop {
                    let mut isnull: bool = false;

                    slot = ExecProcNode(outerNode);
                    if TupIsNull(slot) {
                        break;
                    }
                    (*econtext).ecxt_outertuple = slot;

                    ResetExprContext(econtext);

                    hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(
                        (*node).hash_expr, econtext, &mut isnull));

                    if !isnull {
                        ExecParallelHashTableInsert(hashtable, slot, hashvalue);
                    }
                    (*hashtable).partialTuples += 1.0;
                }

                /*
                 * Make sure that any tuples we wrote to disk are visible to
                 * others before anyone tries to load them.
                 */
                i = 0;
                while i < (*hashtable).nbatch {
                    sts_end_write((*(*hashtable).batches.add(i as usize)).inner_tuples);
                    i += 1;
                }

                /*
                 * Update shared counters.  We need an accurate total tuple count
                 * to control the empty table optimization.
                 */
                ExecParallelHashMergeCounters(hashtable);

                BarrierDetach(&mut (*pstate).grow_buckets_barrier);
                BarrierDetach(&mut (*pstate).grow_batches_barrier);

                /*
                 * Wait for everyone to finish building and flushing files and
                 * counters.
                 */
                if BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_HASH_INNER) {
                    /*
                     * Elect one backend to disable any further growth.  Batches
                     * are now fixed.  While building them we made sure they'd fit
                     * in our memory budget when we load them back in later (or we
                     * tried to do that and gave up because we detected extreme
                     * skew).
                     */
                    (*pstate).growth = PHJ_GROWTH_DISABLED;
                }
            }
        }
        x if x == PHJ_BUILD_HASH_INNER => {
            /*
             * It's time to begin hashing, or if we just arrived here then
             * hashing is already underway, so join in that effort.
             */
            if PHJ_GROW_BATCHES_PHASE(BarrierAttach(&mut (*pstate).grow_batches_barrier)) !=
                PHJ_GROW_BATCHES_ELECT {
                ExecParallelHashIncreaseNumBatches(hashtable);
            }
            if PHJ_GROW_BUCKETS_PHASE(BarrierAttach(&mut (*pstate).grow_buckets_barrier)) !=
                PHJ_GROW_BUCKETS_ELECT {
                ExecParallelHashIncreaseNumBuckets(hashtable);
            }
            ExecParallelHashEnsureBatchAccessors(hashtable);
            ExecParallelHashTableSetCurrentBatch(hashtable, 0);
            loop {
                let mut isnull: bool = false;

                slot = ExecProcNode(outerNode);
                if TupIsNull(slot) {
                    break;
                }
                (*econtext).ecxt_outertuple = slot;

                ResetExprContext(econtext);

                hashvalue = DatumGetUInt32(ExecEvalExprSwitchContext(
                    (*node).hash_expr, econtext, &mut isnull));

                if !isnull {
                    ExecParallelHashTableInsert(hashtable, slot, hashvalue);
                }
                (*hashtable).partialTuples += 1.0;
            }

            i = 0;
            while i < (*hashtable).nbatch {
                sts_end_write((*(*hashtable).batches.add(i as usize)).inner_tuples);
                i += 1;
            }

            ExecParallelHashMergeCounters(hashtable);

            BarrierDetach(&mut (*pstate).grow_buckets_barrier);
            BarrierDetach(&mut (*pstate).grow_batches_barrier);

            if BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_HASH_INNER) {
                (*pstate).growth = PHJ_GROWTH_DISABLED;
            }
        }
        _ => {}
    }

    /*
     * We're not yet attached to a batch.  We all agree on the dimensions and
     * number of inner tuples (for the empty table optimization).
     */
    (*hashtable).curbatch = -1;
    (*hashtable).nbuckets = (*pstate).nbuckets;
    (*hashtable).log2_nbuckets = my_log2((*hashtable).nbuckets as c_long);
    (*hashtable).totalTuples = (*pstate).total_tuples as f64;

    /*
     * Unless we're completely done and the batch state has been freed, make
     * sure we have accessors.
     */
    if BarrierPhase(build_barrier) < PHJ_BUILD_FREE {
        ExecParallelHashEnsureBatchAccessors(hashtable);
    }

    /*
     * The next synchronization point is in ExecHashJoin's HJ_BUILD_HASHTABLE
     * case, which will bring the build phase to PHJ_BUILD_RUN (if it isn't
     * there already).
     */
    Assert!(BarrierPhase(build_barrier) == PHJ_BUILD_HASH_OUTER ||
            BarrierPhase(build_barrier) == PHJ_BUILD_RUN ||
            BarrierPhase(build_barrier) == PHJ_BUILD_FREE);
}

/* ----------------------------------------------------------------
 *      ExecInitHash
 *
 *      Init routine for Hash node
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitHash(node: *mut Hash, estate: *mut EState, eflags: c_int) -> *mut HashState {
    let hashstate: *mut HashState;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create state structure
     */
    hashstate = makeNode_HashState!();
    (*hashstate).ps.plan = node as *mut Plan;
    (*hashstate).ps.state = estate;
    (*hashstate).ps.ExecProcNode = Some(ExecHash);
    /* delay building hashtable until ExecHashTableCreate() in executor run */
    (*hashstate).hashtable = null_mut();

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*hashstate).ps);

    /*
     * initialize child nodes
     */
    // C: outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags)
    // outerPlanState() is an lvalue macro in C; assign directly to lefttree.
    (*(hashstate as *mut PlanState)).lefttree =
        ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * initialize our result slot and type. No need to build projection
     * because this node doesn't do projections.
     */
    ExecInitResultTupleSlotTL(&mut (*hashstate).ps, &TTSOpsMinimalTuple);
    (*hashstate).ps.ps_ProjInfo = null_mut();

    Assert!((*node).plan.qual.is_null());

    /*
     * Delay initialization of hash_expr until ExecInitHashJoin().  We cannot
     * build the ExprState here as we don't yet know the join type we're going
     * to be hashing values for and we need to know that before calling
     * ExecBuildHash32Expr as the keep_nulls parameter depends on the join
     * type.
     */
    (*hashstate).hash_expr = null_mut();

    hashstate
}

/* ---------------------------------------------------------------
 *      ExecEndHash
 *
 *      clean up routine for Hash node
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndHash(node: *mut HashState) {
    let outerPlan: *mut PlanState;

    /*
     * shut down the subplan
     */
    outerPlan = outerPlanState(node);
    ExecEndNode(outerPlan);
}


/* ----------------------------------------------------------------
 *      ExecHashTableCreate
 *
 *      create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecHashTableCreate(state: *mut HashState) -> HashJoinTable {
    let node: *mut Hash;
    let hashtable: HashJoinTable;
    let outerNode: *mut Plan;
    let mut space_allowed: Size = 0;
    let mut nbuckets: c_int = 0;
    let mut nbatch: c_int = 0;
    let rows: f64;
    let mut num_skew_mcvs: c_int = 0;
    let log2_nbuckets: c_int;
    let oldcxt: MemoryContext;

    /*
     * Get information about the size of the relation to be hashed (it's the
     * "outer" subtree of this node, but the inner relation of the hashjoin).
     * Compute the appropriate size of the hash table.
     */
    node = (*state).ps.plan as *mut Hash;
    outerNode = outerPlan(node);

    /*
     * If this is shared hash table with a partial plan, then we can't use
     * outerNode->plan_rows to estimate its size.  We need an estimate of the
     * total number of rows across all copies of the partial plan.
     */
    rows = if (*node).plan.parallel_aware {
        (*node).rows_total
    } else {
        (*outerNode).plan_rows
    };

    ExecChooseHashTableSize(
        rows, (*outerNode).plan_width,
        OidIsValid((*node).skewTable),
        !(*state).parallel_state.is_null(),
        if !(*state).parallel_state.is_null() {
            (*(*state).parallel_state).nparticipants - 1
        } else {
            0
        },
        &mut space_allowed,
        &mut nbuckets, &mut nbatch, &mut num_skew_mcvs,
    );

    /* nbuckets must be a power of 2 */
    log2_nbuckets = my_log2(nbuckets as c_long);
    Assert!(nbuckets == (1 << log2_nbuckets));

    /*
     * Initialize the hash table control block.
     *
     * The hashtable control block is just palloc'd from the executor's
     * per-query memory context.  Everything else should be kept inside the
     * subsidiary hashCxt, batchCxt or spillCxt.
     */
    hashtable = palloc0(size_of::<HashJoinTableData>()) as HashJoinTable;
    (*hashtable).nbuckets = nbuckets;
    (*hashtable).nbuckets_original = nbuckets;
    (*hashtable).nbuckets_optimal = nbuckets;
    (*hashtable).log2_nbuckets = log2_nbuckets;
    (*hashtable).log2_nbuckets_optimal = log2_nbuckets;
    (*hashtable).buckets.unshared = null_mut();
    (*hashtable).skewEnabled = false;
    (*hashtable).skewBucket = null_mut();
    (*hashtable).skewBucketLen = 0;
    (*hashtable).nSkewBuckets = 0;
    (*hashtable).skewBucketNums = null_mut();
    (*hashtable).nbatch = nbatch;
    (*hashtable).curbatch = 0;
    (*hashtable).nbatch_original = nbatch;
    (*hashtable).nbatch_outstart = nbatch;
    (*hashtable).growEnabled = true;
    (*hashtable).totalTuples = 0.0;
    (*hashtable).partialTuples = 0.0;
    (*hashtable).skewTuples = 0.0;
    (*hashtable).innerBatchFile = null_mut();
    (*hashtable).outerBatchFile = null_mut();
    (*hashtable).spaceUsed = 0;
    (*hashtable).spacePeak = 0;
    (*hashtable).spaceAllowed = space_allowed;
    (*hashtable).spaceUsedSkew = 0;
    (*hashtable).spaceAllowedSkew =
        (*hashtable).spaceAllowed * SKEW_HASH_MEM_PERCENT as Size / 100;
    (*hashtable).chunks = null_mut();
    (*hashtable).current_chunk = null_mut();
    (*hashtable).parallel_state = (*state).parallel_state;
    (*hashtable).area = (*(*state).ps.state).es_query_dsa;
    (*hashtable).batches = null_mut();

    /* #ifdef HJDEBUG -- omitted in release translation */

    /*
     * Create temporary memory contexts in which to keep the hashtable working
     * storage.  See notes in executor/hashjoin.h.
     */
    (*hashtable).hashCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        "HashTableContext",
        ALLOCSET_DEFAULT_SIZES
    );

    (*hashtable).batchCxt = AllocSetContextCreate!(
        (*hashtable).hashCxt,
        "HashBatchContext",
        ALLOCSET_DEFAULT_SIZES
    );

    (*hashtable).spillCxt = AllocSetContextCreate!(
        (*hashtable).hashCxt,
        "HashSpillContext",
        ALLOCSET_DEFAULT_SIZES
    );

    /* Allocate data that will live for the life of the hashjoin */

    oldcxt = MemoryContextSwitchTo((*hashtable).hashCxt);

    if nbatch > 1 && (*hashtable).parallel_state.is_null() {
        let oldctx: MemoryContext;

        /*
         * allocate and initialize the file arrays in hashCxt (not needed for
         * parallel case which uses shared tuplestores instead of raw files)
         */
        oldctx = MemoryContextSwitchTo((*hashtable).spillCxt);

        (*hashtable).innerBatchFile =
            palloc0(size_of::<*mut BufFile>() * nbatch as usize) as *mut *mut BufFile;
        (*hashtable).outerBatchFile =
            palloc0(size_of::<*mut BufFile>() * nbatch as usize) as *mut *mut BufFile;

        MemoryContextSwitchTo(oldctx);

        /* The files will not be opened until needed... */
        /* ... but make sure we have temp tablespaces established for them */
        PrepareTempTablespaces();
    }

    MemoryContextSwitchTo(oldcxt);

    if !(*hashtable).parallel_state.is_null() {
        let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
        let build_barrier: *mut Barrier;

        /*
         * Attach to the build barrier.  The corresponding detach operation is
         * in ExecHashTableDetach.  Note that we won't attach to the
         * batch_barrier for batch 0 yet.  We'll attach later and start it out
         * in PHJ_BATCH_PROBE phase, because batch 0 is allocated up front and
         * then loaded while hashing (the standard hybrid hash join
         * algorithm), and we'll coordinate that using build_barrier.
         */
        build_barrier = &mut (*pstate).build_barrier;
        BarrierAttach(build_barrier);

        /*
         * So far we have no idea whether there are any other participants,
         * and if so, what phase they are working on.  The only thing we care
         * about at this point is whether someone has already created the
         * SharedHashJoinBatch objects and the hash table for batch 0.  One
         * backend will be elected to do that now if necessary.
         */
        if BarrierPhase(build_barrier) == PHJ_BUILD_ELECT &&
            BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ELECT) {
            (*pstate).nbatch = nbatch;
            (*pstate).space_allowed = space_allowed;
            (*pstate).growth = PHJ_GROWTH_OK;

            /* Set up the shared state for coordinating batches. */
            ExecParallelHashJoinSetUpBatches(hashtable, nbatch);

            /*
             * Allocate batch 0's hash table up front so we can load it
             * directly while hashing.
             */
            (*pstate).nbuckets = nbuckets;
            ExecParallelHashTableAlloc(hashtable, 0);
        }

        /*
         * The next Parallel Hash synchronization point is in
         * MultiExecParallelHash(), which will progress it all the way to
         * PHJ_BUILD_RUN.  The caller must not return control from this
         * executor node between now and then.
         */
    } else {
        /*
         * Prepare context for the first-scan space allocations; allocate the
         * hashbucket array therein, and set each bucket "empty".
         */
        MemoryContextSwitchTo((*hashtable).batchCxt);

        (*hashtable).buckets.unshared =
            palloc0(size_of::<HashJoinTuple>() * nbuckets as usize) as *mut HashJoinTuple;

        /*
         * Set up for skew optimization, if possible and there's a need for
         * more than one batch.  (In a one-batch join, there's no point in
         * it.)
         */
        if nbatch > 1 {
            ExecHashBuildSkewHash(state, hashtable, node, num_skew_mcvs);
        }

        MemoryContextSwitchTo(oldcxt);
    }

    hashtable
}


/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */
pub unsafe fn ExecChooseHashTableSize(
    mut ntuples: f64,
    tupwidth: c_int,
    useskew: bool,
    try_combined_hash_mem: bool,
    parallel_workers: c_int,
    space_allowed: *mut Size,
    numbuckets: *mut c_int,
    numbatches: *mut c_int,
    num_skew_mcvs: *mut c_int,
) {
    let tupsize: c_int;
    let inner_rel_bytes: f64;
    let mut hash_table_bytes: Size;
    let bucket_bytes: Size;
    let mut max_pointers: Size;
    let mut nbatch: c_int = 1;
    let mut nbuckets: c_int;
    let dbuckets: f64;

    /* Force a plausible relation size if no info */
    if ntuples <= 0.0 {
        ntuples = 1000.0;
    }

    /*
     * Estimate tupsize based on footprint of tuple in hashtable... note this
     * does not allow for any palloc overhead.  The manipulations of spaceUsed
     * don't count palloc overhead either.
     */
    tupsize = (HJTUPLE_OVERHEAD() +
        MAXALIGN(size_of::<MinimalTupleData>()) +
        MAXALIGN(tupwidth as usize)) as c_int;
    inner_rel_bytes = ntuples * tupsize as f64;

    /*
     * Compute in-memory hashtable size limit from GUCs.
     */
    hash_table_bytes = get_hash_memory_limit();

    /*
     * Parallel Hash tries to use the combined hash_mem of all workers to
     * avoid the need to batch.  If that won't work, it falls back to hash_mem
     * per worker and tries to process batches in parallel.
     */
    if try_combined_hash_mem {
        /* Careful, this could overflow size_t */
        let mut newlimit: f64;

        newlimit = (hash_table_bytes as f64) * (parallel_workers as f64 + 1.0);
        newlimit = newlimit.min(Size::MAX as f64);
        hash_table_bytes = newlimit as Size;
    }

    *space_allowed = hash_table_bytes;

    /*
     * If skew optimization is possible, estimate the number of skew buckets
     * that will fit in the memory allowed, and decrement the assumed space
     * available for the main hash table accordingly.
     *
     * We make the optimistic assumption that each skew bucket will contain
     * one inner-relation tuple.  If that turns out to be low, we will recover
     * at runtime by reducing the number of skew buckets.
     *
     * hashtable->skewBucket will have up to 8 times as many HashSkewBucket
     * pointers as the number of MCVs we allow, since ExecHashBuildSkewHash
     * will round up to the next power of 2 and then multiply by 4 to reduce
     * collisions.
     */
    if useskew {
        let bytes_per_mcv: Size;
        let mut skew_mcvs: Size;

        /*----------
         * Compute number of MCVs we could hold in hash_table_bytes
         *
         * Divisor is:
         * size of a hash tuple +
         * worst-case size of skewBucket[] per MCV +
         * size of skewBucketNums[] entry +
         * size of skew bucket struct itself
         *----------
         */
        bytes_per_mcv = tupsize as Size +
            (8 * size_of::<*mut HashSkewBucket>()) +
            size_of::<c_int>() +
            SKEW_BUCKET_OVERHEAD();
        skew_mcvs = hash_table_bytes / bytes_per_mcv;

        /*
         * Now scale by SKEW_HASH_MEM_PERCENT (we do it in this order so as
         * not to worry about size_t overflow in the multiplication)
         */
        skew_mcvs = (skew_mcvs * SKEW_HASH_MEM_PERCENT as Size) / 100;

        /* Now clamp to integer range */
        skew_mcvs = skew_mcvs.min(c_int::MAX as Size);

        *num_skew_mcvs = skew_mcvs as c_int;

        /* Reduce hash_table_bytes by the amount needed for the skew table */
        if skew_mcvs > 0 {
            hash_table_bytes -= skew_mcvs * bytes_per_mcv;
        }
    } else {
        *num_skew_mcvs = 0;
    }

    /*
     * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
     * memory is filled, assuming a single batch; but limit the value so that
     * the pointer arrays we'll try to allocate do not exceed hash_table_bytes
     * nor MaxAllocSize.
     *
     * Note that both nbuckets and nbatch must be powers of 2 to make
     * ExecHashGetBucketAndBatch fast.
     */
    max_pointers = hash_table_bytes / size_of::<HashJoinTuple>();
    max_pointers = max_pointers.min(MaxAllocSize / size_of::<HashJoinTuple>());
    /* If max_pointers isn't a power of 2, must round it down to one */
    max_pointers = pg_prevpower2_size_t(max_pointers as u64) as Size;

    /* Also ensure we avoid integer overflow in nbatch and nbuckets */
    /* (this step is redundant given the current value of MaxAllocSize) */
    max_pointers = max_pointers.min(c_int::MAX as Size / 2 + 1);

    let dbuckets = (ntuples / NTUP_PER_BUCKET as f64).ceil();
    let dbuckets = dbuckets.min(max_pointers as f64);
    nbuckets = dbuckets as c_int;
    /* don't let nbuckets be really small, though ... */
    nbuckets = nbuckets.max(1024);
    /* ... and force it to be a power of 2. */
    nbuckets = pg_nextpower2_32(nbuckets as uint32) as c_int;

    /*
     * If there's not enough space to store the projected number of tuples and
     * the required bucket headers, we will need multiple batches.
     */
    bucket_bytes = size_of::<HashJoinTuple>() * nbuckets as usize;
    if inner_rel_bytes + bucket_bytes as f64 > hash_table_bytes as f64 {
        /* We'll need multiple batches */
        let mut sbuckets: Size;
        let dbatch: f64;
        let minbatch: c_int;
        let bucket_size: Size;

        /*
         * If Parallel Hash with combined hash_mem would still need multiple
         * batches, we'll have to fall back to regular hash_mem budget.
         */
        if try_combined_hash_mem {
            ExecChooseHashTableSize(ntuples, tupwidth, useskew,
                                    false, parallel_workers,
                                    space_allowed,
                                    numbuckets,
                                    numbatches,
                                    num_skew_mcvs);
            return;
        }

        /*
         * Estimate the number of buckets we'll want to have when hash_mem is
         * entirely full.  Each bucket will contain a bucket pointer plus
         * NTUP_PER_BUCKET tuples, whose projected size already includes
         * overhead for the hash code, pointer to the next tuple, etc.
         */
        bucket_size = (tupsize as usize * NTUP_PER_BUCKET + size_of::<HashJoinTuple>());
        if hash_table_bytes <= bucket_size {
            sbuckets = 1; /* avoid pg_nextpower2_size_t(0) */
        } else {
            sbuckets = pg_nextpower2_size_t((hash_table_bytes / bucket_size) as u64) as Size;
        }
        sbuckets = sbuckets.min(max_pointers);
        nbuckets = sbuckets as c_int;
        nbuckets = pg_nextpower2_32(nbuckets as uint32) as c_int;
        let bucket_bytes2 = nbuckets as usize * size_of::<HashJoinTuple>();

        /*
         * Buckets are simple pointers to hashjoin tuples, while tupsize
         * includes the pointer, hash code, and MinimalTupleData.  So buckets
         * should never really exceed 25% of hash_mem (even for
         * NTUP_PER_BUCKET=1); except maybe for hash_mem values that are not
         * 2^N bytes, where we might get more because of doubling. So let's
         * look for 50% here.
         */
        Assert!(bucket_bytes2 <= hash_table_bytes / 2);

        /* Calculate required number of batches. */
        let dbatch = (inner_rel_bytes / (hash_table_bytes - bucket_bytes2) as f64).ceil();
        let dbatch = dbatch.min(max_pointers as f64);
        minbatch = dbatch as c_int;
        nbatch = pg_nextpower2_32(2_u32.max(minbatch as u32)) as c_int;
    }

    /*
     * Optimize the total amount of memory consumed by the hash node.
     * ...
     */
    while nbatch > 1 {
        /* Check that buckets won't overflow MaxAllocSize */
        if nbuckets as usize > (MaxAllocSize / size_of::<HashJoinTuple>() / 2) {
            break;
        }

        /* num_skew_mcvs should be less than nbuckets */
        Assert!((*num_skew_mcvs) < (c_int::MAX / 2));

        /*
         * Check that space_allowed won't overflow SIZE_MAX.
         */
        if (*space_allowed) > (Size::MAX / 2) {
            break;
        }

        /*
         * Will halving the number of batches and doubling the size of the
         * hashtable reduce overall memory usage?
         */
        if (nbatch as Size) < (*space_allowed) / BLCKSZ {
            break;
        }

        /*
         * MaxAllocSize is sufficiently small that we are not worried about
         * overflowing nbuckets.
         */
        nbuckets *= 2;

        *num_skew_mcvs = (*num_skew_mcvs) * 2;
        *space_allowed = (*space_allowed) * 2;

        nbatch /= 2;
    }

    Assert!(nbuckets > 0);
    Assert!(nbatch > 0);

    *numbuckets = nbuckets;
    *numbatches = nbatch;
}


/* ----------------------------------------------------------------
 *      ExecHashTableDestroy
 *
 *      destroy a hash table
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecHashTableDestroy(hashtable: HashJoinTable) {
    let mut i: c_int;

    /*
     * Make sure all the temp files are closed.  We skip batch 0, since it
     * can't have any temp files (and the arrays might not even exist if
     * nbatch is only 1).  Parallel hash joins don't use these files.
     */
    if !(*hashtable).innerBatchFile.is_null() {
        i = 1;
        while i < (*hashtable).nbatch {
            let inner = *(*hashtable).innerBatchFile.add(i as usize);
            if !inner.is_null() {
                BufFileClose(inner);
            }
            let outer = *(*hashtable).outerBatchFile.add(i as usize);
            if !outer.is_null() {
                BufFileClose(outer);
            }
            i += 1;
        }
    }

    /* Release working memory (batchCxt is a child, so it goes away too) */
    MemoryContextDelete((*hashtable).hashCxt);

    /* And drop the control block */
    pfree(hashtable as *mut c_void);
}

/*
 * Consider adjusting the allowed hash table size, depending on the number
 * of batches, to minimize the overall memory usage (for both the hashtable
 * and batch files).
 *
 * Returns true if we chose to increase the batch size (and thus we don't
 * need to add batches), and false if we should increase nbatch.
 */
unsafe fn ExecHashIncreaseBatchSize(hashtable: HashJoinTable) -> bool {
    /*
     * How much additional memory would doubling nbatch use? Each batch may
     * require two buffered files (inner/outer), with a BLCKSZ buffer.
     */
    let batchSpace: Size = ((*hashtable).nbatch as Size * 2 * BLCKSZ as Size);

    /*
     * Compare the new space needed for doubling nbatch and for enlarging the
     * in-memory hash table. If doubling the hash table needs less memory,
     * just do that. Otherwise, continue with doubling the nbatch.
     */
    if (*hashtable).spaceAllowed <= batchSpace {
        (*hashtable).spaceAllowed *= 2;
        return true;
    }

    false
}

/*
 * ExecHashIncreaseNumBatches
 *      increase the original number of batches in order to reduce
 *      current memory consumption
 */
unsafe fn ExecHashIncreaseNumBatches(hashtable: HashJoinTable) {
    let oldnbatch: c_int = (*hashtable).nbatch;
    let curbatch: c_int = (*hashtable).curbatch;
    let nbatch: c_int;
    let mut ninmemory: i64;
    let mut nfreed: i64;
    let oldchunks: HashMemoryChunk;

    /* do nothing if we've decided to shut off growth */
    if !(*hashtable).growEnabled {
        return;
    }

    /* safety check to avoid overflow */
    if oldnbatch > (c_int::MAX / 2).min(
        (MaxAllocSize / (size_of::<*mut c_void>() * 2)) as c_int) {
        return;
    }

    /* consider increasing size of the in-memory hash table instead */
    if ExecHashIncreaseBatchSize(hashtable) {
        return;
    }

    let nbatch = oldnbatch * 2;
    Assert!(nbatch > 1);

    /* #ifdef HJDEBUG omitted */

    if (*hashtable).innerBatchFile.is_null() {
        let oldcxt = MemoryContextSwitchTo((*hashtable).spillCxt);

        /* we had no file arrays before */
        (*hashtable).innerBatchFile =
            palloc0(size_of::<*mut BufFile>() * nbatch as usize) as *mut *mut BufFile;
        (*hashtable).outerBatchFile =
            palloc0(size_of::<*mut BufFile>() * nbatch as usize) as *mut *mut BufFile;

        MemoryContextSwitchTo(oldcxt);

        /* time to establish the temp tablespaces, too */
        PrepareTempTablespaces();
    } else {
        /* enlarge arrays and zero out added entries */
        let new_inner = repalloc(
            (*hashtable).innerBatchFile as *mut c_void,
            size_of::<*mut BufFile>() * nbatch as usize,
        ) as *mut *mut BufFile;
        /* zero the new slots */
        ptr::write_bytes(
            new_inner.add(oldnbatch as usize),
            0,
            (nbatch - oldnbatch) as usize * size_of::<*mut BufFile>(),
        );
        (*hashtable).innerBatchFile = new_inner;

        let new_outer = repalloc(
            (*hashtable).outerBatchFile as *mut c_void,
            size_of::<*mut BufFile>() * nbatch as usize,
        ) as *mut *mut BufFile;
        ptr::write_bytes(
            new_outer.add(oldnbatch as usize),
            0,
            (nbatch - oldnbatch) as usize * size_of::<*mut BufFile>(),
        );
        (*hashtable).outerBatchFile = new_outer;
    }

    (*hashtable).nbatch = nbatch;

    /*
     * Scan through the existing hash table entries and dump out any that are
     * no longer of the current batch.
     */
    ninmemory = 0;
    nfreed = 0;

    /* If know we need to resize nbuckets, we can do it while rebatching. */
    if (*hashtable).nbuckets_optimal != (*hashtable).nbuckets {
        /* we never decrease the number of buckets */
        Assert!((*hashtable).nbuckets_optimal > (*hashtable).nbuckets);

        (*hashtable).nbuckets = (*hashtable).nbuckets_optimal;
        (*hashtable).log2_nbuckets = (*hashtable).log2_nbuckets_optimal;

        (*hashtable).buckets.unshared = repalloc(
            (*hashtable).buckets.unshared as *mut c_void,
            size_of::<HashJoinTuple>() * (*hashtable).nbuckets as usize,
        ) as *mut HashJoinTuple;
    }

    /*
     * We will scan through the chunks directly, so that we can reset the
     * buckets now and not have to keep track which tuples in the buckets have
     * already been processed. We will free the old chunks as we go.
     */
    ptr::write_bytes(
        (*hashtable).buckets.unshared,
        0,
        size_of::<HashJoinTuple>() * (*hashtable).nbuckets as usize,
    );
    let mut oldchunks = (*hashtable).chunks;
    (*hashtable).chunks = null_mut();

    /* so, let's scan through the old chunks, and all tuples in each chunk */
    while !oldchunks.is_null() {
        let nextchunk = (*oldchunks).next.unshared;

        /* position within the buffer (up to oldchunks->used) */
        let mut idx: Size = 0;

        /* process all tuples stored in this chunk (and then free it) */
        while idx < (*oldchunks).used {
            let hashTuple = (HASH_CHUNK_DATA(oldchunks).add(idx)) as HashJoinTuple;
            let tuple = HJTUPLE_MINTUPLE(hashTuple);
            let hashTupleSize = HJTUPLE_OVERHEAD() + (*tuple).t_len as usize;
            let mut bucketno: c_int = 0;
            let mut batchno: c_int = 0;

            ninmemory += 1;
            ExecHashGetBucketAndBatch(hashtable, (*hashTuple).hashvalue,
                                      &mut bucketno, &mut batchno);

            if batchno == curbatch {
                /* keep tuple in memory - copy it into the new chunk */
                let copyTuple: HashJoinTuple;

                copyTuple = dense_alloc(hashtable, hashTupleSize) as HashJoinTuple;
                ptr::copy_nonoverlapping(
                    hashTuple as *const u8,
                    copyTuple as *mut u8,
                    hashTupleSize,
                );

                /* and add it back to the appropriate bucket */
                (*copyTuple).next.unshared =
                    *(*hashtable).buckets.unshared.add(bucketno as usize);
                *(*hashtable).buckets.unshared.add(bucketno as usize) = copyTuple;
            } else {
                /* dump it out */
                Assert!(batchno > curbatch);
                ExecHashJoinSaveTuple(
                    HJTUPLE_MINTUPLE(hashTuple),
                    (*hashTuple).hashvalue,
                    (*hashtable).innerBatchFile.add(batchno as usize),
                    hashtable,
                );

                (*hashtable).spaceUsed -= hashTupleSize;
                nfreed += 1;
            }

            /* next tuple in this chunk */
            idx += MAXALIGN(hashTupleSize);

            /* allow this loop to be cancellable */
            CHECK_FOR_INTERRUPTS!();
        }

        /* we're done with this chunk - free it and proceed to the next one */
        pfree(oldchunks as *mut c_void);
        oldchunks = nextchunk;
    }

    /* #ifdef HJDEBUG omitted */

    /*
     * If we dumped out either all or none of the tuples in the table, disable
     * further expansion of nbatch.
     */
    if nfreed == 0 || nfreed == ninmemory {
        (*hashtable).growEnabled = false;
        /* #ifdef HJDEBUG omitted */
    }
}

/*
 * ExecParallelHashIncreaseNumBatches
 *      Every participant attached to grow_batches_barrier must run this
 *      function when it observes growth == PHJ_GROWTH_NEED_MORE_BATCHES.
 */
unsafe fn ExecParallelHashIncreaseNumBatches(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;

    Assert!(BarrierPhase(&mut (*pstate).build_barrier) == PHJ_BUILD_HASH_INNER);

    /*
     * It's unlikely, but we need to be prepared for new participants to show
     * up while we're in the middle of this operation so we need to switch on
     * barrier phase here.
     */
    match PHJ_GROW_BATCHES_PHASE(BarrierPhase(&mut (*pstate).grow_batches_barrier)) {
        x if x == PHJ_GROW_BATCHES_ELECT => {
            /*
             * Elect one participant to prepare to grow the number of batches.
             */
            if BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                                     WAIT_EVENT_HASH_GROW_BATCHES_ELECT) {
                let mut buckets: *mut dsa_pointer_atomic;
                let old_batch0: *mut ParallelHashJoinBatch;
                let new_nbatch: c_int;
                let mut i: c_int;

                /* Move the old batch out of the way. */
                old_batch0 = (*(*hashtable).batches.add(0)).shared;
                (*pstate).old_batches = (*pstate).batches;
                (*pstate).old_nbatch = (*hashtable).nbatch;
                (*pstate).batches = InvalidDsaPointer;

                /* Free this backend's old accessors. */
                ExecParallelHashCloseBatchAccessors(hashtable);

                /* Figure out how many batches to use. */
                if (*hashtable).nbatch == 1 {
                    /*
                     * We are going from single-batch to multi-batch.
                     */
                    (*pstate).space_allowed = get_hash_memory_limit();

                    /*
                     * The combined hash_mem of all participants wasn't
                     * enough. Try two batches per participant.
                     */
                    new_nbatch = pg_nextpower2_32(
                        ((*pstate).nparticipants * 2) as u32) as c_int;
                } else {
                    /*
                     * We were already multi-batched.  Try doubling.
                     */
                    new_nbatch = (*hashtable).nbatch * 2;
                }

                /* Allocate new larger generation of batches. */
                Assert!((*hashtable).nbatch == (*pstate).nbatch);
                ExecParallelHashJoinSetUpBatches(hashtable, new_nbatch);
                Assert!((*hashtable).nbatch == (*pstate).nbatch);

                /* Replace or recycle batch 0's bucket array. */
                if (*pstate).old_nbatch == 1 {
                    let dtuples: f64;
                    let dbuckets: f64;
                    let new_nbuckets: c_int;
                    let max_buckets: uint32;

                    /*
                     * We probably also need a smaller bucket array.
                     */
                    dtuples = ((*old_batch0).ntuples as f64 * 2.0) / new_nbatch as f64;

                    /*
                     * Calculate the maximum number of buckets to stay within
                     * the MaxAllocSize boundary.
                     */
                    max_buckets = pg_prevpower2_32(
                        (MaxAllocSize / size_of::<dsa_pointer_atomic>()) as u32);
                    let dbuckets = (dtuples / NTUP_PER_BUCKET as f64).ceil();
                    let dbuckets = dbuckets.min(max_buckets as f64);
                    let mut new_nbuckets = dbuckets as c_int;
                    new_nbuckets = new_nbuckets.max(1024);
                    new_nbuckets = pg_nextpower2_32(new_nbuckets as uint32) as c_int;
                    dsa_free((*hashtable).area, (*old_batch0).buckets);
                    (*(*(*hashtable).batches.add(0)).shared).buckets =
                        dsa_allocate((*hashtable).area,
                                     size_of::<dsa_pointer_atomic>() * new_nbuckets as usize);
                    buckets = dsa_get_address((*hashtable).area,
                                              (*(*(*hashtable).batches.add(0)).shared).buckets)
                        as *mut dsa_pointer_atomic;
                    i = 0;
                    while i < new_nbuckets {
                        dsa_pointer_atomic_init(buckets.add(i as usize), InvalidDsaPointer);
                        i += 1;
                    }
                    (*pstate).nbuckets = new_nbuckets;
                } else {
                    /* Recycle the existing bucket array. */
                    (*(*(*hashtable).batches.add(0)).shared).buckets = (*old_batch0).buckets;
                    buckets = dsa_get_address((*hashtable).area, (*old_batch0).buckets)
                        as *mut dsa_pointer_atomic;
                    i = 0;
                    while i < (*hashtable).nbuckets {
                        dsa_pointer_atomic_write(buckets.add(i as usize), InvalidDsaPointer);
                        i += 1;
                    }
                }

                /* Move all chunks to the work queue for parallel processing. */
                (*pstate).chunk_work_queue = (*old_batch0).chunks;

                /* Disable further growth temporarily while we're growing. */
                (*pstate).growth = PHJ_GROWTH_DISABLED;
            } else {
                /* All other participants just flush their tuples to disk. */
                ExecParallelHashCloseBatchAccessors(hashtable);
            }
            /* Fall through. */
            ExecParallelHashIncreaseNumBatches_reallocate(hashtable, pstate);
        }
        x if x == PHJ_GROW_BATCHES_REALLOCATE => {
            ExecParallelHashIncreaseNumBatches_reallocate(hashtable, pstate);
        }
        x if x == PHJ_GROW_BATCHES_REPARTITION => {
            ExecParallelHashIncreaseNumBatches_repartition(hashtable, pstate);
        }
        x if x == PHJ_GROW_BATCHES_DECIDE => {
            ExecParallelHashIncreaseNumBatches_decide(hashtable, pstate);
        }
        x if x == PHJ_GROW_BATCHES_FINISH => {
            /* Wait for the above to complete. */
            BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                                  WAIT_EVENT_HASH_GROW_BATCHES_FINISH);
        }
        _ => {}
    }
}

unsafe fn ExecParallelHashIncreaseNumBatches_reallocate(
    hashtable: HashJoinTable, pstate: *mut ParallelHashJoinState,
) {
    /* Wait for the above to be finished. */
    BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                          WAIT_EVENT_HASH_GROW_BATCHES_REALLOCATE);
    ExecParallelHashIncreaseNumBatches_repartition(hashtable, pstate);
}

unsafe fn ExecParallelHashIncreaseNumBatches_repartition(
    hashtable: HashJoinTable, pstate: *mut ParallelHashJoinState,
) {
    /* Make sure that we have the current dimensions and buckets. */
    ExecParallelHashEnsureBatchAccessors(hashtable);
    ExecParallelHashTableSetCurrentBatch(hashtable, 0);
    /* Then partition, flush counters. */
    ExecParallelHashRepartitionFirst(hashtable);
    ExecParallelHashRepartitionRest(hashtable);
    ExecParallelHashMergeCounters(hashtable);
    /* Wait for the above to be finished. */
    BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                          WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION);
    ExecParallelHashIncreaseNumBatches_decide(hashtable, pstate);
}

unsafe fn ExecParallelHashIncreaseNumBatches_decide(
    hashtable: HashJoinTable, pstate: *mut ParallelHashJoinState,
) {
    /*
     * Elect one participant to clean up and decide whether further
     * repartitioning is needed, or should be disabled because it's
     * not helping.
     */
    if BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                             WAIT_EVENT_HASH_GROW_BATCHES_DECIDE) {
        let mut old_batches: *mut ParallelHashJoinBatch;
        let mut space_exhausted: bool = false;
        let mut extreme_skew_detected: bool = false;

        /* Make sure that we have the current dimensions and buckets. */
        ExecParallelHashEnsureBatchAccessors(hashtable);
        ExecParallelHashTableSetCurrentBatch(hashtable, 0);

        old_batches = dsa_get_address((*hashtable).area, (*pstate).old_batches)
            as *mut ParallelHashJoinBatch;

        /* Are any of the new generation of batches exhausted? */
        for i in 0..(*hashtable).nbatch {
            let batch: *mut ParallelHashJoinBatch;
            let old_batch: *mut ParallelHashJoinBatch;
            let parent: c_int;

            batch = (*(*hashtable).batches.add(i as usize)).shared;
            if (*batch).space_exhausted ||
               (*batch).estimated_size > (*pstate).space_allowed {
                space_exhausted = true;
            }

            parent = i % (*pstate).old_nbatch;
            old_batch = NthParallelHashJoinBatch(old_batches, parent, hashtable);
            if (*old_batch).space_exhausted ||
               (*batch).estimated_size > (*pstate).space_allowed {
                /*
                 * Did this batch receive ALL of the tuples from its
                 * parent batch?
                 */
                if (*batch).ntuples ==
                   (*(*(*hashtable).batches.add(parent as usize)).shared).old_ntuples {
                    extreme_skew_detected = true;
                }
            }
        }

        /* Don't keep growing if it's not helping or we'd overflow. */
        if extreme_skew_detected || (*hashtable).nbatch >= c_int::MAX / 2 {
            (*pstate).growth = PHJ_GROWTH_DISABLED;
        } else if space_exhausted {
            (*pstate).growth = PHJ_GROWTH_NEED_MORE_BATCHES;
        } else {
            (*pstate).growth = PHJ_GROWTH_OK;
        }

        /* Free the old batches in shared memory. */
        dsa_free((*hashtable).area, (*pstate).old_batches);
        (*pstate).old_batches = InvalidDsaPointer;
    }
    /* Fall through. */
    /* Wait for the above to complete. */
    BarrierArriveAndWait(&mut (*pstate).grow_batches_barrier,
                          WAIT_EVENT_HASH_GROW_BATCHES_FINISH);
}

/*
 * Repartition the tuples currently loaded into memory for inner batch 0
 * because the number of batches has been increased.
 */
unsafe fn ExecParallelHashRepartitionFirst(hashtable: HashJoinTable) {
    let mut chunk_shared: dsa_pointer = 0;
    let mut chunk: HashMemoryChunk;

    Assert!((*hashtable).nbatch == (*(*hashtable).parallel_state).nbatch);

    loop {
        chunk = ExecParallelHashPopChunkQueue(hashtable, &mut chunk_shared);
        if chunk.is_null() { break; }
        let mut idx: Size = 0;

        /* Repartition all tuples in this chunk. */
        while idx < (*chunk).used {
            let hashTuple = (HASH_CHUNK_DATA(chunk).add(idx)) as HashJoinTuple;
            let tuple = HJTUPLE_MINTUPLE(hashTuple);
            let copyTuple: HashJoinTuple;
            let mut shared: dsa_pointer = 0;
            let mut bucketno: c_int = 0;
            let mut batchno: c_int = 0;

            ExecHashGetBucketAndBatch(hashtable, (*hashTuple).hashvalue,
                                      &mut bucketno, &mut batchno);

            Assert!(batchno < (*hashtable).nbatch);
            if batchno == 0 {
                /* It still belongs in batch 0.  Copy to a new chunk. */
                copyTuple = ExecParallelHashTupleAlloc(
                    hashtable,
                    HJTUPLE_OVERHEAD() + (*tuple).t_len as usize,
                    &mut shared);
                (*copyTuple).hashvalue = (*hashTuple).hashvalue;
                ptr::copy_nonoverlapping(
                    tuple as *const u8,
                    HJTUPLE_MINTUPLE(copyTuple) as *mut u8,
                    (*tuple).t_len as usize,
                );
                ExecParallelHashPushTuple(
                    (*hashtable).buckets.shared.add(bucketno as usize),
                    copyTuple, shared);
            } else {
                let tuple_size =
                    MAXALIGN(HJTUPLE_OVERHEAD() + (*tuple).t_len as usize);

                /* It belongs in a later batch. */
                (*(*hashtable).batches.add(batchno as usize)).estimated_size += tuple_size;
                sts_puttuple(
                    (*(*hashtable).batches.add(batchno as usize)).inner_tuples,
                    &(*hashTuple).hashvalue as *const uint32 as *const c_void,
                    tuple,
                );
            }

            /* Count this tuple. */
            (*(*hashtable).batches.add(0)).old_ntuples += 1;
            (*(*hashtable).batches.add(batchno as usize)).ntuples += 1;

            idx += MAXALIGN(HJTUPLE_OVERHEAD() +
                            (*HJTUPLE_MINTUPLE(hashTuple)).t_len as usize);
        }

        /* Free this chunk. */
        dsa_free((*hashtable).area, chunk_shared);

        CHECK_FOR_INTERRUPTS!();
    }
}

/*
 * Help repartition inner batches 1..n.
 */
unsafe fn ExecParallelHashRepartitionRest(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let old_nbatch: c_int = (*pstate).old_nbatch;
    let mut old_inner_tuples: *mut *mut SharedTuplestoreAccessor;
    let old_batches: *mut ParallelHashJoinBatch;
    let mut i: c_int;

    /* Get our hands on the previous generation of batches. */
    old_batches = dsa_get_address((*hashtable).area, (*pstate).old_batches)
        as *mut ParallelHashJoinBatch;
    old_inner_tuples =
        palloc0(size_of::<*mut SharedTuplestoreAccessor>() * old_nbatch as usize)
        as *mut *mut SharedTuplestoreAccessor;
    i = 1;
    while i < old_nbatch {
        let shared: *mut ParallelHashJoinBatch =
            NthParallelHashJoinBatch(old_batches, i, hashtable);

        *old_inner_tuples.add(i as usize) = sts_attach(
            ParallelHashJoinBatchInner(shared),
            ParallelWorkerNumber + 1,
            &mut (*pstate).fileset,
        );
        i += 1;
    }

    /* Join in the effort to repartition them. */
    i = 1;
    while i < old_nbatch {
        let mut tuple: MinimalTuple;
        let mut hashvalue: uint32 = 0;

        /* Scan one partition from the previous generation. */
        sts_begin_parallel_scan(*old_inner_tuples.add(i as usize));
        loop {
            tuple = sts_parallel_scan_next(*old_inner_tuples.add(i as usize), &mut hashvalue);
            if tuple.is_null() { break; }
            let tuple_size = MAXALIGN(HJTUPLE_OVERHEAD() + (*tuple).t_len as usize);
            let mut bucketno: c_int = 0;
            let mut batchno: c_int = 0;

            /* Decide which partition it goes to in the new generation. */
            ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);

            (*(*hashtable).batches.add(batchno as usize)).estimated_size += tuple_size;
            (*(*hashtable).batches.add(batchno as usize)).ntuples += 1;
            (*(*hashtable).batches.add(i as usize)).old_ntuples += 1;

            /* Store the tuple its new batch. */
            sts_puttuple(
                (*(*hashtable).batches.add(batchno as usize)).inner_tuples,
                &hashvalue as *const uint32 as *const c_void,
                tuple,
            );

            CHECK_FOR_INTERRUPTS!();
        }
        sts_end_parallel_scan(*old_inner_tuples.add(i as usize));
        i += 1;
    }

    pfree(old_inner_tuples as *mut c_void);
}

/*
 * Transfer the backend-local per-batch counters to the shared totals.
 */
unsafe fn ExecParallelHashMergeCounters(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let mut i: c_int;

    LWLockAcquire(&mut (*pstate).lock, LW_EXCLUSIVE);
    (*pstate).total_tuples = 0;
    i = 0;
    while i < (*hashtable).nbatch {
        let batch: *mut ParallelHashJoinBatchAccessor =
            (*hashtable).batches.add(i as usize);

        (*(*batch).shared).size += (*batch).size;
        (*(*batch).shared).estimated_size += (*batch).estimated_size;
        (*(*batch).shared).ntuples += (*batch).ntuples;
        (*(*batch).shared).old_ntuples += (*batch).old_ntuples;
        (*batch).size = 0;
        (*batch).estimated_size = 0;
        (*batch).ntuples = 0;
        (*batch).old_ntuples = 0;
        (*pstate).total_tuples += (*(*batch).shared).ntuples;
        i += 1;
    }
    LWLockRelease(&mut (*pstate).lock);
}

/*
 * ExecHashIncreaseNumBuckets
 *      increase the original number of buckets in order to reduce
 *      number of tuples per bucket
 */
unsafe fn ExecHashIncreaseNumBuckets(hashtable: HashJoinTable) {
    let mut chunk: HashMemoryChunk;

    /* do nothing if not an increase (it's called increase for a reason) */
    if (*hashtable).nbuckets >= (*hashtable).nbuckets_optimal {
        return;
    }

    /* #ifdef HJDEBUG omitted */

    (*hashtable).nbuckets = (*hashtable).nbuckets_optimal;
    (*hashtable).log2_nbuckets = (*hashtable).log2_nbuckets_optimal;

    Assert!((*hashtable).nbuckets > 1);
    Assert!((*hashtable).nbuckets <= (c_int::MAX / 2));
    Assert!((*hashtable).nbuckets == (1 << (*hashtable).log2_nbuckets));

    /*
     * Just reallocate the proper number of buckets - we don't need to walk
     * through them - we can walk the dense-allocated chunks
     */
    (*hashtable).buckets.unshared = repalloc(
        (*hashtable).buckets.unshared as *mut c_void,
        size_of::<HashJoinTuple>() * (*hashtable).nbuckets as usize,
    ) as *mut HashJoinTuple;

    ptr::write_bytes(
        (*hashtable).buckets.unshared,
        0,
        (*hashtable).nbuckets as usize * size_of::<HashJoinTuple>(),
    );

    /* scan through all tuples in all chunks to rebuild the hash table */
    chunk = (*hashtable).chunks;
    while !chunk.is_null() {
        /* process all tuples stored in this chunk */
        let mut idx: Size = 0;

        while idx < (*chunk).used {
            let hashTuple = (HASH_CHUNK_DATA(chunk).add(idx)) as HashJoinTuple;
            let mut bucketno: c_int = 0;
            let mut batchno: c_int = 0;

            ExecHashGetBucketAndBatch(hashtable, (*hashTuple).hashvalue,
                                      &mut bucketno, &mut batchno);

            /* add the tuple to the proper bucket */
            (*hashTuple).next.unshared =
                *(*hashtable).buckets.unshared.add(bucketno as usize);
            *(*hashtable).buckets.unshared.add(bucketno as usize) = hashTuple;

            /* advance index past the tuple */
            idx += MAXALIGN(HJTUPLE_OVERHEAD() +
                            (*HJTUPLE_MINTUPLE(hashTuple)).t_len as usize);
        }

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS!();
        chunk = (*chunk).next.unshared;
    }
}

unsafe fn ExecParallelHashIncreaseNumBuckets(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let mut i: c_int;
    let mut chunk: HashMemoryChunk;
    let mut chunk_s: dsa_pointer = 0;

    Assert!(BarrierPhase(&mut (*pstate).build_barrier) == PHJ_BUILD_HASH_INNER);

    /*
     * It's unlikely, but we need to be prepared for new participants to show
     * up while we're in the middle of this operation.
     */
    match PHJ_GROW_BUCKETS_PHASE(BarrierPhase(&mut (*pstate).grow_buckets_barrier)) {
        x if x == PHJ_GROW_BUCKETS_ELECT => {
            /* Elect one participant to prepare to increase nbuckets. */
            if BarrierArriveAndWait(&mut (*pstate).grow_buckets_barrier,
                                     WAIT_EVENT_HASH_GROW_BUCKETS_ELECT) {
                let size: Size;
                let buckets: *mut dsa_pointer_atomic;

                /* Double the size of the bucket array. */
                (*pstate).nbuckets *= 2;
                size = (*pstate).nbuckets as usize * size_of::<dsa_pointer_atomic>();
                (*(*(*hashtable).batches.add(0)).shared).size += size / 2;
                dsa_free((*hashtable).area, (*(*(*hashtable).batches.add(0)).shared).buckets);
                (*(*(*hashtable).batches.add(0)).shared).buckets =
                    dsa_allocate((*hashtable).area, size);
                let buckets = dsa_get_address(
                    (*hashtable).area,
                    (*(*(*hashtable).batches.add(0)).shared).buckets,
                ) as *mut dsa_pointer_atomic;
                i = 0;
                while i < (*pstate).nbuckets {
                    dsa_pointer_atomic_init(buckets.add(i as usize), InvalidDsaPointer);
                    i += 1;
                }

                /* Put the chunk list onto the work queue. */
                (*pstate).chunk_work_queue =
                    (*(*(*hashtable).batches.add(0)).shared).chunks;

                /* Clear the flag. */
                (*pstate).growth = PHJ_GROWTH_OK;
            }
            /* Fall through. */
            /* Wait for the above to complete. */
            BarrierArriveAndWait(&mut (*pstate).grow_buckets_barrier,
                                  WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE);
            /* Fall through to REINSERT. */
            ExecParallelHashIncreaseNumBuckets_reinsert(hashtable, pstate);
        }
        x if x == PHJ_GROW_BUCKETS_REALLOCATE => {
            /* Wait for the above to complete. */
            BarrierArriveAndWait(&mut (*pstate).grow_buckets_barrier,
                                  WAIT_EVENT_HASH_GROW_BUCKETS_REALLOCATE);
            /* Fall through. */
            ExecParallelHashIncreaseNumBuckets_reinsert(hashtable, pstate);
        }
        x if x == PHJ_GROW_BUCKETS_REINSERT => {
            ExecParallelHashIncreaseNumBuckets_reinsert(hashtable, pstate);
        }
        _ => {}
    }
}

unsafe fn ExecParallelHashIncreaseNumBuckets_reinsert(
    hashtable: HashJoinTable, pstate: *mut ParallelHashJoinState,
) {
    let mut chunk: HashMemoryChunk;
    let mut chunk_s: dsa_pointer = 0;

    /* Reinsert all tuples into the hash table. */
    ExecParallelHashEnsureBatchAccessors(hashtable);
    ExecParallelHashTableSetCurrentBatch(hashtable, 0);
    loop {
        chunk = ExecParallelHashPopChunkQueue(hashtable, &mut chunk_s);
        if chunk.is_null() { break; }
        let mut idx: Size = 0;

        while idx < (*chunk).used {
            let hashTuple = (HASH_CHUNK_DATA(chunk).add(idx)) as HashJoinTuple;
            let shared: dsa_pointer = chunk_s + HASH_CHUNK_HEADER_SIZE() as dsa_pointer + idx as dsa_pointer;
            let mut bucketno: c_int = 0;
            let mut batchno: c_int = 0;

            ExecHashGetBucketAndBatch(hashtable, (*hashTuple).hashvalue,
                                      &mut bucketno, &mut batchno);
            Assert!(batchno == 0);

            /* add the tuple to the proper bucket */
            ExecParallelHashPushTuple(
                (*hashtable).buckets.shared.add(bucketno as usize),
                hashTuple, shared);

            /* advance index past the tuple */
            idx += MAXALIGN(HJTUPLE_OVERHEAD() +
                            (*HJTUPLE_MINTUPLE(hashTuple)).t_len as usize);
        }

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS!();
    }
    BarrierArriveAndWait(&mut (*pstate).grow_buckets_barrier,
                          WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT);
}

// TODO(pg-port): palloc0_array / palloc0_object macros (palloc0 + cast)
#[inline]
unsafe fn palloc0_array_impl(elem_size: usize, n: usize) -> *mut c_void {
    palloc0(elem_size * n)
}

// TODO(pg-port): work_mem / hash_mem_multiplier (from miscadmin)
use crate::miscadmin::{work_mem, hash_mem_multiplier};

// TODO(pg-port): ExecHashAccumInstrumentation forward decl (defined below)

/*
 * ExecHashTableInsert
 *      insert a tuple into the hash table depending on the hash value
 *      it may just go to a temp file for later batches
 *
 * Note: the input slot is not actually saved in the hashtable --
 * we just extract the minimal tuple and insert that.  We do not pfree
 * the tuple, because it is needed for potential use as the "outer" slot
 * in a hash join, and will be freed when the outer batch file is freed.
 * (There is no corresponding "inner" batch file, since we save inner
 *  tuples to the hash table.)
 */
pub unsafe fn ExecHashTableInsert(hashtable: HashJoinTable,
                                  slot: *mut TupleTableSlot,
                                  hashvalue: u32) {
    let mut shouldFree: bool = false;
    let tuple: MinimalTuple = ExecFetchSlotMinimalTuple(slot, &mut shouldFree);
    let mut bucketno: c_int = 0;
    let mut batchno: c_int = 0;

    ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);

    /*
     * decide whether to put the tuple in the hash table or a temp file
     */
    if batchno == (*hashtable).curbatch {
        /*
         * put the tuple in hash table
         */
        let hashTuple: HashJoinTuple;
        let hashTupleSize: c_int;
        let ntuples: f64 = (*hashtable).totalTuples - (*hashtable).skewTuples;

        /* Create the HashJoinTuple */
        hashTupleSize = (HJTUPLE_OVERHEAD() + (*tuple).t_len as usize) as c_int;
        hashTuple = dense_alloc(hashtable, hashTupleSize as usize) as HashJoinTuple;

        (*hashTuple).hashvalue = hashvalue;
        ptr::copy_nonoverlapping(tuple as *const u8,
                                 HJTUPLE_MINTUPLE(hashTuple) as *mut u8,
                                 (*tuple).t_len as usize);

        /*
         * We always reset the tuple-matched flag on insertion.  This is okay
         * even when reloading a tuple from a batch file, since the tuple
         * could not possibly have been matched to an outer tuple before it
         * went into the batch file.
         */
        HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

        /* Push it onto the front of the bucket's list */
        (*hashTuple).next.unshared = *(*hashtable).buckets.unshared.add(bucketno as usize);
        *(*hashtable).buckets.unshared.add(bucketno as usize) = hashTuple;

        /*
         * Increase the (optimal) number of buckets if we just exceeded the
         * NTUP_PER_BUCKET threshold, but only when there's still a single
         * batch.
         */
        if (*hashtable).nbatch == 1
            && ntuples > ((*hashtable).nbuckets_optimal as f64 * NTUP_PER_BUCKET as f64)
        {
            /* Guard against integer overflow and alloc size overflow */
            if (*hashtable).nbuckets_optimal <= c_int::MAX / 2
                && (*hashtable).nbuckets_optimal * 2
                    <= (MaxAllocSize / size_of::<HashJoinTuple>()) as c_int
            {
                (*hashtable).nbuckets_optimal *= 2;
                (*hashtable).log2_nbuckets_optimal += 1;
            }
        }

        /* Account for space used, and back off if we've used too much */
        (*hashtable).spaceUsed += hashTupleSize as usize;
        if (*hashtable).spaceUsed > (*hashtable).spacePeak {
            (*hashtable).spacePeak = (*hashtable).spaceUsed;
        }
        if (*hashtable).spaceUsed
            + (*hashtable).nbuckets_optimal as usize * size_of::<HashJoinTuple>()
            > (*hashtable).spaceAllowed
        {
            ExecHashIncreaseNumBatches(hashtable);
        }
    } else {
        /*
         * put the tuple into a temp file for later batches
         */
        Assert!(batchno > (*hashtable).curbatch);
        ExecHashJoinSaveTuple(tuple, hashvalue,
                              (*hashtable).innerBatchFile.add(batchno as usize),
                              hashtable);
    }

    if shouldFree {
        heap_free_minimal_tuple(tuple);
    }
}

/*
 * ExecParallelHashTableInsert
 *      insert a tuple into a shared hash table or shared batch tuplestore
 */
pub unsafe fn ExecParallelHashTableInsert(hashtable: HashJoinTable,
                                          slot: *mut TupleTableSlot,
                                          hashvalue: u32) {
    let mut shouldFree: bool = false;
    let tuple: MinimalTuple = ExecFetchSlotMinimalTuple(slot, &mut shouldFree);
    let mut shared: dsa_pointer = 0;
    let mut bucketno: c_int = 0;
    let mut batchno: c_int = 0;

    /* C: retry: goto label -- use loop for retry logic */
    loop {
        ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);

        if batchno == 0 {
            let hashTuple: HashJoinTuple;

            /* Try to load it into memory. */
            Assert!(BarrierPhase(&mut (*(*hashtable).parallel_state).build_barrier)
                    == PHJ_BUILD_HASH_INNER);
            hashTuple = ExecParallelHashTupleAlloc(hashtable,
                                                   HJTUPLE_OVERHEAD() + (*tuple).t_len as usize,
                                                   &mut shared);
            if hashTuple.is_null() {
                /* retry */
                continue;
            }

            /* Store the hash value in the HashJoinTuple header. */
            (*hashTuple).hashvalue = hashvalue;
            ptr::copy_nonoverlapping(tuple as *const u8,
                                     HJTUPLE_MINTUPLE(hashTuple) as *mut u8,
                                     (*tuple).t_len as usize);
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

            /* Push it onto the front of the bucket's list */
            ExecParallelHashPushTuple(
                (*hashtable).buckets.shared.add(bucketno as usize),
                hashTuple, shared);
        } else {
            let tuple_size: usize = MAXALIGN(HJTUPLE_OVERHEAD() + (*tuple).t_len as usize);

            Assert!(batchno > 0);

            /* Try to preallocate space in the batch if necessary. */
            if (*(*hashtable).batches.add(batchno as usize)).preallocated < tuple_size {
                if !ExecParallelHashTuplePrealloc(hashtable, batchno, tuple_size) {
                    /* retry */
                    continue;
                }
            }

            Assert!((*(*hashtable).batches.add(batchno as usize)).preallocated >= tuple_size);
            (*(*hashtable).batches.add(batchno as usize)).preallocated -= tuple_size;
            sts_puttuple((*(*hashtable).batches.add(batchno as usize)).inner_tuples,
                         &hashvalue as *const u32 as *const c_void,
                         tuple);
        }
        (*(*hashtable).batches.add(batchno as usize)).ntuples += 1;
        break;
    }

    if shouldFree {
        heap_free_minimal_tuple(tuple);
    }
}

/*
 * Insert a tuple into the current hash table.  Unlike
 * ExecParallelHashTableInsert, this version is not prepared to send the tuple
 * to other batches or to run out of memory, and should only be called with
 * tuples that belong in the current batch once growth has been disabled.
 */
pub unsafe fn ExecParallelHashTableInsertCurrentBatch(hashtable: HashJoinTable,
                                                       slot: *mut TupleTableSlot,
                                                       hashvalue: u32) {
    let mut shouldFree: bool = false;
    let tuple: MinimalTuple = ExecFetchSlotMinimalTuple(slot, &mut shouldFree);
    let hashTuple: HashJoinTuple;
    let mut shared: dsa_pointer = 0;
    let mut batchno: c_int = 0;
    let mut bucketno: c_int = 0;

    ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);
    Assert!(batchno == (*hashtable).curbatch);
    hashTuple = ExecParallelHashTupleAlloc(hashtable,
                                           HJTUPLE_OVERHEAD() + (*tuple).t_len as usize,
                                           &mut shared);
    (*hashTuple).hashvalue = hashvalue;
    ptr::copy_nonoverlapping(tuple as *const u8,
                             HJTUPLE_MINTUPLE(hashTuple) as *mut u8,
                             (*tuple).t_len as usize);
    HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));
    ExecParallelHashPushTuple((*hashtable).buckets.shared.add(bucketno as usize),
                              hashTuple, shared);

    if shouldFree {
        heap_free_minimal_tuple(tuple);
    }
}

/*
 * ExecHashGetBucketAndBatch
 *      Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *      bucketno = hashvalue MOD nbuckets
 *      batchno = ROR(hashvalue, log2_nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets and log2_nbuckets may change while nbatch == 1 because of dynamic
 * bucket count growth.  Once we start batching, the value is fixed and does
 * not change over the course of the join (making it possible to compute batch
 * number the way we do here).
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.  In very large
 * joins, we might run out of bits to add, so we do this by rotating the hash
 * value.  This causes batchno to steal bits from bucketno when the number of
 * virtual buckets exceeds 2^32.  It's better to have longer bucket chains
 * than to lose the ability to divide batches.
 */
pub unsafe fn ExecHashGetBucketAndBatch(hashtable: HashJoinTable,
                                        hashvalue: u32,
                                        bucketno: *mut c_int,
                                        batchno: *mut c_int) {
    let nbuckets: u32 = (*hashtable).nbuckets as u32;
    let nbatch: u32 = (*hashtable).nbatch as u32;

    if nbatch > 1 {
        *bucketno = (hashvalue & (nbuckets - 1)) as c_int;
        *batchno = (pg_rotate_right32(hashvalue, (*hashtable).log2_nbuckets)
                    & (nbatch - 1)) as c_int;
    } else {
        *bucketno = (hashvalue & (nbuckets - 1)) as c_int;
        *batchno = 0;
    }
}

/*
 * ExecScanHashBucket
 *      scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
pub unsafe fn ExecScanHashBucket(hjstate: *mut HashJoinState,
                                 econtext: *mut ExprContext) -> bool {
    let hjclauses: *mut ExprState = (*hjstate).hashclauses;
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let mut hashTuple: HashJoinTuple = (*hjstate).hj_CurTuple;
    let hashvalue: u32 = (*hjstate).hj_CurHashValue;

    /*
     * hj_CurTuple is the address of the tuple last returned from the current
     * bucket, or NULL if it's time to start scanning a new bucket.
     *
     * If the tuple hashed to a skew bucket then scan the skew bucket
     * otherwise scan the standard hashtable bucket.
     */
    if !hashTuple.is_null() {
        hashTuple = (*hashTuple).next.unshared;
    } else if (*hjstate).hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO {
        hashTuple = (**(*hashtable).skewBucket.add((*hjstate).hj_CurSkewBucketNo as usize)).tuples
            as HashJoinTuple;
    } else {
        hashTuple = *(*hashtable).buckets.unshared.add((*hjstate).hj_CurBucketNo as usize);
    }

    while !hashTuple.is_null() {
        if (*hashTuple).hashvalue == hashvalue {
            let inntuple: *mut TupleTableSlot;

            /* insert hashtable's tuple into exec slot so ExecQual sees it */
            inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                                             (*hjstate).hj_HashTupleSlot,
                                             false);  /* do not pfree */
            (*econtext).ecxt_innertuple = inntuple;

            if ExecQualAndReset(hjclauses, econtext) {
                (*hjstate).hj_CurTuple = hashTuple;
                return true;
            }
        }

        hashTuple = (*hashTuple).next.unshared;
    }

    /*
     * no match
     */
    false
}

/*
 * ExecParallelScanHashBucket
 *      scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
pub unsafe fn ExecParallelScanHashBucket(hjstate: *mut HashJoinState,
                                         econtext: *mut ExprContext) -> bool {
    let hjclauses: *mut ExprState = (*hjstate).hashclauses;
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let mut hashTuple: HashJoinTuple = (*hjstate).hj_CurTuple;
    let hashvalue: u32 = (*hjstate).hj_CurHashValue;

    /*
     * hj_CurTuple is the address of the tuple last returned from the current
     * bucket, or NULL if it's time to start scanning a new bucket.
     */
    if !hashTuple.is_null() {
        hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple);
    } else {
        hashTuple = ExecParallelHashFirstTuple(hashtable, (*hjstate).hj_CurBucketNo);
    }

    while !hashTuple.is_null() {
        if (*hashTuple).hashvalue == hashvalue {
            let inntuple: *mut TupleTableSlot;

            /* insert hashtable's tuple into exec slot so ExecQual sees it */
            inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                                             (*hjstate).hj_HashTupleSlot,
                                             false);  /* do not pfree */
            (*econtext).ecxt_innertuple = inntuple;

            if ExecQualAndReset(hjclauses, econtext) {
                (*hjstate).hj_CurTuple = hashTuple;
                return true;
            }
        }

        hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple);
    }

    /*
     * no match
     */
    false
}

/*
 * ExecPrepHashTableForUnmatched
 *      set up for a series of ExecScanHashTableForUnmatched calls
 */
pub unsafe fn ExecPrepHashTableForUnmatched(hjstate: *mut HashJoinState) {
    /*----------
     * During this scan we use the HashJoinState fields as follows:
     *
     * hj_CurBucketNo: next regular bucket to scan
     * hj_CurSkewBucketNo: next skew bucket (an index into skewBucketNums)
     * hj_CurTuple: last tuple returned, or NULL to start next bucket
     *----------
     */
    (*hjstate).hj_CurBucketNo = 0;
    (*hjstate).hj_CurSkewBucketNo = 0;
    (*hjstate).hj_CurTuple = ptr::null_mut();
}

/*
 * Decide if this process is allowed to run the unmatched scan.  If so, the
 * batch barrier is advanced to PHJ_BATCH_SCAN and true is returned.
 * Otherwise the batch is detached and false is returned.
 */
pub unsafe fn ExecParallelPrepHashTableForUnmatched(hjstate: *mut HashJoinState) -> bool {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let curbatch: c_int = (*hashtable).curbatch;
    let batch: *mut ParallelHashJoinBatch = (*(*hashtable).batches.add(curbatch as usize)).shared;

    Assert!(BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_PROBE);

    /*
     * It would not be deadlock-free to wait on the batch barrier, because it
     * is in PHJ_BATCH_PROBE phase, and thus processes attached to it have
     * already emitted tuples.  Therefore, we'll hold a wait-free election:
     * only one process can continue to the next phase, and all others detach
     * from this batch.  They can still go any work on other batches, if there
     * are any.
     */
    if !BarrierArriveAndDetachExceptLast(&mut (*batch).batch_barrier) {
        /* This process considers the batch to be done. */
        (*(*hashtable).batches.add((*hashtable).curbatch as usize)).done = true;

        /* Make sure any temporary files are closed. */
        sts_end_parallel_scan((*(*hashtable).batches.add(curbatch as usize)).inner_tuples);
        sts_end_parallel_scan((*(*hashtable).batches.add(curbatch as usize)).outer_tuples);

        /*
         * Track largest batch we've seen, which would normally happen in
         * ExecHashTableDetachBatch().
         */
        hashtable_spacepeak_update(hashtable, batch);
        (*hashtable).curbatch = -1;
        return false;
    }

    /* Now we are alone with this batch. */
    Assert!(BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_SCAN);

    /*
     * Has another process decided to give up early and command all processes
     * to skip the unmatched scan?
     */
    if (*batch).skip_unmatched {
        (*(*hashtable).batches.add((*hashtable).curbatch as usize)).done = true;
        ExecHashTableDetachBatch(hashtable);
        return false;
    }

    /* Now prepare the process local state, just as for non-parallel join. */
    ExecPrepHashTableForUnmatched(hjstate);

    true
}

// helper: update spacePeak from batch size
#[inline]
unsafe fn hashtable_spacepeak_update(hashtable: HashJoinTable, batch: *mut ParallelHashJoinBatch) {
    let candidate = (*batch).size
        + size_of::<dsa_pointer_atomic>() * (*hashtable).nbuckets as usize;
    if candidate > (*hashtable).spacePeak {
        (*hashtable).spacePeak = candidate;
    }
}

/*
 * ExecScanHashTableForUnmatched
 *      scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
pub unsafe fn ExecScanHashTableForUnmatched(hjstate: *mut HashJoinState,
                                            econtext: *mut ExprContext) -> bool {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let mut hashTuple: HashJoinTuple = (*hjstate).hj_CurTuple;

    loop {
        /*
         * hj_CurTuple is the address of the tuple last returned from the
         * current bucket, or NULL if it's time to start scanning a new
         * bucket.
         */
        if !hashTuple.is_null() {
            hashTuple = (*hashTuple).next.unshared;
        } else if (*hjstate).hj_CurBucketNo < (*hashtable).nbuckets {
            hashTuple = *(*hashtable).buckets.unshared
                .add((*hjstate).hj_CurBucketNo as usize);
            (*hjstate).hj_CurBucketNo += 1;
        } else if (*hjstate).hj_CurSkewBucketNo < (*hashtable).nSkewBuckets {
            let j: c_int = *(*hashtable).skewBucketNums
                .add((*hjstate).hj_CurSkewBucketNo as usize);
            hashTuple = (**(*hashtable).skewBucket.add(j as usize)).tuples as HashJoinTuple;
            (*hjstate).hj_CurSkewBucketNo += 1;
        } else {
            break;  /* finished all buckets */
        }

        while !hashTuple.is_null() {
            if !HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)) {
                let inntuple: *mut TupleTableSlot;

                /* insert hashtable's tuple into exec slot */
                inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                                                 (*hjstate).hj_HashTupleSlot,
                                                 false);  /* do not pfree */
                (*econtext).ecxt_innertuple = inntuple;

                /*
                 * Reset temp memory each time; although this function doesn't
                 * do any qual eval, the caller will, so let's keep it
                 * parallel to ExecScanHashBucket.
                 */
                ResetExprContext(econtext);

                (*hjstate).hj_CurTuple = hashTuple;
                return true;
            }

            hashTuple = (*hashTuple).next.unshared;
        }

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS!();
    }

    /*
     * no more unmatched tuples
     */
    false
}

/*
 * ExecParallelScanHashTableForUnmatched
 *      scan the hash table for unmatched inner tuples, in parallel join
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
pub unsafe fn ExecParallelScanHashTableForUnmatched(hjstate: *mut HashJoinState,
                                                    econtext: *mut ExprContext) -> bool {
    let hashtable: HashJoinTable = (*hjstate).hj_HashTable;
    let mut hashTuple: HashJoinTuple = (*hjstate).hj_CurTuple;

    loop {
        /*
         * hj_CurTuple is the address of the tuple last returned from the
         * current bucket, or NULL if it's time to start scanning a new
         * bucket.
         */
        if !hashTuple.is_null() {
            hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple);
        } else if (*hjstate).hj_CurBucketNo < (*hashtable).nbuckets {
            hashTuple = ExecParallelHashFirstTuple(hashtable,
                                                   (*hjstate).hj_CurBucketNo);
            (*hjstate).hj_CurBucketNo += 1;
        } else {
            break;  /* finished all buckets */
        }

        while !hashTuple.is_null() {
            if !HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)) {
                let inntuple: *mut TupleTableSlot;

                /* insert hashtable's tuple into exec slot */
                inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
                                                 (*hjstate).hj_HashTupleSlot,
                                                 false);  /* do not pfree */
                (*econtext).ecxt_innertuple = inntuple;

                /*
                 * Reset temp memory each time; although this function doesn't
                 * do any qual eval, the caller will, so let's keep it
                 * parallel to ExecScanHashBucket.
                 */
                ResetExprContext(econtext);

                (*hjstate).hj_CurTuple = hashTuple;
                return true;
            }

            hashTuple = ExecParallelHashNextTuple(hashtable, hashTuple);
        }

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS!();
    }

    /*
     * no more unmatched tuples
     */
    false
}

/*
 * ExecHashTableReset
 *
 *      reset hash table header for new batch
 */
pub unsafe fn ExecHashTableReset(hashtable: HashJoinTable) {
    let oldcxt: MemoryContext;
    let nbuckets: c_int = (*hashtable).nbuckets;

    /*
     * Release all the hash buckets and tuples acquired in the prior pass, and
     * reinitialize the context for a new pass.
     */
    MemoryContextReset((*hashtable).batchCxt);
    oldcxt = MemoryContextSwitchTo((*hashtable).batchCxt);

    /* Reallocate and reinitialize the hash bucket headers. */
    (*hashtable).buckets.unshared =
        palloc0(size_of::<HashJoinTuple>() * nbuckets as usize) as *mut HashJoinTuple;

    (*hashtable).spaceUsed = 0;

    MemoryContextSwitchTo(oldcxt);

    /* Forget the chunks (the memory was freed by the context reset above). */
    (*hashtable).chunks = ptr::null_mut();
}

/*
 * ExecHashTableResetMatchFlags
 *      Clear all the HeapTupleHeaderHasMatch flags in the table
 */
pub unsafe fn ExecHashTableResetMatchFlags(hashtable: HashJoinTable) {
    let mut tuple: HashJoinTuple;
    let mut i: c_int;

    /* Reset all flags in the main table ... */
    i = 0;
    while i < (*hashtable).nbuckets {
        tuple = *(*hashtable).buckets.unshared.add(i as usize);
        while !tuple.is_null() {
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
            tuple = (*tuple).next.unshared;
        }
        i += 1;
    }

    /* ... and the same for the skew buckets, if any */
    i = 0;
    while i < (*hashtable).nSkewBuckets {
        let j: c_int = *(*hashtable).skewBucketNums.add(i as usize);
        let skewBucket: *mut HashSkewBucket = *(*hashtable).skewBucket.add(j as usize);

        tuple = (*skewBucket).tuples as HashJoinTuple;
        while !tuple.is_null() {
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
            tuple = (*tuple).next.unshared;
        }
        i += 1;
    }
}

pub unsafe fn ExecReScanHash(node: *mut HashState) {
    let outerPlan: *mut PlanState = outerPlanState(node);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

/*
 * ExecHashBuildSkewHash
 *
 *      Set up for skew optimization if we can identify the most common values
 *      (MCVs) of the outer relation's join key.  We make a skew hash bucket
 *      for the hash value of each MCV, up to the number of slots allowed
 *      based on available memory.
 */
unsafe fn ExecHashBuildSkewHash(hashstate: *mut HashState,
                                hashtable: HashJoinTable,
                                node: *mut Hash,
                                mcvsToUse: c_int) {
    let statsTuple: *mut crate::access::htup_details::HeapTupleData;
    let mut sslot: AttStatsSlot = AttStatsSlot {
        values: ptr::null_mut(),
        nvalues: 0,
        numbers: ptr::null_mut(),
        nnumbers: 0,
    };
    let mut mcvsToUse = mcvsToUse;

    /* Do nothing if planner didn't identify the outer relation's join key */
    if !OidIsValid((*node).skewTable) {
        return;
    }
    /* Also, do nothing if we don't have room for at least one skew bucket */
    if mcvsToUse <= 0 {
        return;
    }

    /*
     * Try to find the MCV statistics for the outer relation's join key.
     */
    statsTuple = SearchSysCache3(STATRELATTINH,
                                 ObjectIdGetDatum((*node).skewTable),
                                 Int16GetDatum((*node).skewColumn),
                                 BoolGetDatum((*node).skewInherit));
    if statsTuple.is_null() {
        return;  /* HeapTupleIsValid check */
    }

    if get_attstatsslot(&mut sslot, statsTuple,
                        STATISTIC_KIND_MCV, 0 /* InvalidOid */,
                        ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS) {
        let mut frac: f64;
        let mut nbuckets: c_int;
        let mut i: c_int;

        if mcvsToUse > sslot.nvalues {
            mcvsToUse = sslot.nvalues;
        }

        /*
         * Calculate the expected fraction of outer relation that will
         * participate in the skew optimization.  If this isn't at least
         * SKEW_MIN_OUTER_FRACTION, don't use skew optimization.
         */
        frac = 0.0;
        i = 0;
        while i < mcvsToUse {
            frac += *sslot.numbers.add(i as usize) as f64;
            i += 1;
        }
        if frac < SKEW_MIN_OUTER_FRACTION as f64 {
            free_attstatsslot(&mut sslot);
            ReleaseSysCache(statsTuple);
            return;
        }

        /*
         * Okay, set up the skew hashtable.
         *
         * skewBucket[] is an open addressing hashtable with a power of 2 size
         * that is greater than the number of MCV values.  (This ensures there
         * will be at least one null entry, so searches will always
         * terminate.)
         *
         * Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
         * MaxAllocSize/sizeof(void *)/8, but that is not currently possible
         * since we limit pg_statistic entries to much less than that.
         */
        nbuckets = pg_nextpower2_32((mcvsToUse + 1) as u32) as c_int;
        /* use two more bits just to help avoid collisions */
        nbuckets <<= 2;

        (*hashtable).skewEnabled = true;
        (*hashtable).skewBucketLen = nbuckets;

        /*
         * We allocate the bucket memory in the hashtable's batch context. It
         * is only needed during the first batch, and this ensures it will be
         * automatically removed once the first batch is done.
         */
        (*hashtable).skewBucket = MemoryContextAllocZero(
            (*hashtable).batchCxt,
            nbuckets as usize * size_of::<*mut HashSkewBucket>(),
        ) as *mut *mut HashSkewBucket;
        (*hashtable).skewBucketNums = MemoryContextAllocZero(
            (*hashtable).batchCxt,
            mcvsToUse as usize * size_of::<c_int>(),
        ) as *mut c_int;

        (*hashtable).spaceUsed += nbuckets as usize * size_of::<*mut HashSkewBucket>()
            + mcvsToUse as usize * size_of::<c_int>();
        (*hashtable).spaceUsedSkew += nbuckets as usize * size_of::<*mut HashSkewBucket>()
            + mcvsToUse as usize * size_of::<c_int>();
        if (*hashtable).spaceUsed > (*hashtable).spacePeak {
            (*hashtable).spacePeak = (*hashtable).spaceUsed;
        }

        /*
         * Create a skew bucket for each MCV hash value.
         *
         * Note: it is very important that we create the buckets in order of
         * decreasing MCV frequency.  If we have to remove some buckets, they
         * must be removed in reverse order of creation (see notes in
         * ExecHashRemoveNextSkewBucket) and we want the least common MCVs to
         * be removed first.
         */
        i = 0;
        while i < mcvsToUse {
            let hashvalue: u32;
            let mut bucket: c_int;

            hashvalue = DatumGetUInt32(FunctionCall1Coll(
                (*hashstate).skew_hashfunction,
                (*hashstate).skew_collation,
                *sslot.values.add(i as usize),
            ));

            /*
             * While we have not hit a hole in the hashtable and have not hit
             * the desired bucket, we have collided with some previous hash
             * value, so try the next bucket location.  NB: this code must
             * match ExecHashGetSkewBucket.
             */
            bucket = (hashvalue & (nbuckets as u32 - 1)) as c_int;
            while !(*(*hashtable).skewBucket.add(bucket as usize)).is_null()
                && (*(*(*hashtable).skewBucket.add(bucket as usize))).hashvalue != hashvalue
            {
                bucket = (bucket + 1) & (nbuckets - 1);
            }

            /*
             * If we found an existing bucket with the same hashvalue, leave
             * it alone.  It's okay for two MCVs to share a hashvalue.
             */
            if !(*(*hashtable).skewBucket.add(bucket as usize)).is_null() {
                i += 1;
                continue;
            }

            /* Okay, create a new skew bucket for this hashvalue. */
            *(*hashtable).skewBucket.add(bucket as usize) =
                MemoryContextAlloc((*hashtable).batchCxt,
                                   size_of::<HashSkewBucket>()) as *mut HashSkewBucket;
            (*(*(*hashtable).skewBucket.add(bucket as usize))).hashvalue = hashvalue;
            (*(*(*hashtable).skewBucket.add(bucket as usize))).tuples = ptr::null_mut();
            *(*hashtable).skewBucketNums.add((*hashtable).nSkewBuckets as usize) = bucket;
            (*hashtable).nSkewBuckets += 1;
            (*hashtable).spaceUsed += SKEW_BUCKET_OVERHEAD();
            (*hashtable).spaceUsedSkew += SKEW_BUCKET_OVERHEAD();
            if (*hashtable).spaceUsed > (*hashtable).spacePeak {
                (*hashtable).spacePeak = (*hashtable).spaceUsed;
            }

            i += 1;
        }

        free_attstatsslot(&mut sslot);
    }

    ReleaseSysCache(statsTuple);
}

/*
 * ExecHashGetSkewBucket
 *
 *      Returns the index of the skew bucket for this hashvalue,
 *      or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *      associated with any active skew bucket.
 */
pub unsafe fn ExecHashGetSkewBucket(hashtable: HashJoinTable, hashvalue: u32) -> c_int {
    let mut bucket: c_int;

    /*
     * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
     * particular, this happens after the initial batch is done).
     */
    if !(*hashtable).skewEnabled {
        return INVALID_SKEW_BUCKET_NO;
    }

    /*
     * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
     */
    bucket = (hashvalue & ((*hashtable).skewBucketLen as u32 - 1)) as c_int;

    /*
     * While we have not hit a hole in the hashtable and have not hit the
     * desired bucket, we have collided with some other hash value, so try the
     * next bucket location.
     */
    while !(*(*hashtable).skewBucket.add(bucket as usize)).is_null()
        && (*(*(*hashtable).skewBucket.add(bucket as usize))).hashvalue != hashvalue
    {
        bucket = (bucket + 1) & ((*hashtable).skewBucketLen - 1);
    }

    /*
     * Found the desired bucket?
     */
    if !(*(*hashtable).skewBucket.add(bucket as usize)).is_null() {
        return bucket;
    }

    /*
     * There must not be any hashtable entry for this hash value.
     */
    INVALID_SKEW_BUCKET_NO
}

/*
 * ExecHashSkewTableInsert
 *
 *      Insert a tuple into the skew hashtable.
 *
 * This should generally match up with the current-batch case in
 * ExecHashTableInsert.
 */
unsafe fn ExecHashSkewTableInsert(hashtable: HashJoinTable,
                                  slot: *mut TupleTableSlot,
                                  hashvalue: u32,
                                  bucketNumber: c_int) {
    let mut shouldFree: bool = false;
    let tuple: MinimalTuple = ExecFetchSlotMinimalTuple(slot, &mut shouldFree);
    let hashTuple: HashJoinTuple;
    let hashTupleSize: c_int;

    /* Create the HashJoinTuple */
    hashTupleSize = (HJTUPLE_OVERHEAD() + (*tuple).t_len as usize) as c_int;
    hashTuple = MemoryContextAlloc((*hashtable).batchCxt,
                                   hashTupleSize as usize) as HashJoinTuple;
    (*hashTuple).hashvalue = hashvalue;
    ptr::copy_nonoverlapping(tuple as *const u8,
                             HJTUPLE_MINTUPLE(hashTuple) as *mut u8,
                             (*tuple).t_len as usize);
    HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

    /* Push it onto the front of the skew bucket's list */
    (*hashTuple).next.unshared =
        (*(*(*hashtable).skewBucket.add(bucketNumber as usize))).tuples as HashJoinTuple;
    (*(*(*hashtable).skewBucket.add(bucketNumber as usize))).tuples = hashTuple as _;
    Assert!(hashTuple != (*hashTuple).next.unshared);

    /* Account for space used, and back off if we've used too much */
    (*hashtable).spaceUsed += hashTupleSize as usize;
    (*hashtable).spaceUsedSkew += hashTupleSize as usize;
    if (*hashtable).spaceUsed > (*hashtable).spacePeak {
        (*hashtable).spacePeak = (*hashtable).spaceUsed;
    }
    while (*hashtable).spaceUsedSkew > (*hashtable).spaceAllowedSkew {
        ExecHashRemoveNextSkewBucket(hashtable);
    }

    /* Check we are not over the total spaceAllowed, either */
    if (*hashtable).spaceUsed > (*hashtable).spaceAllowed {
        ExecHashIncreaseNumBatches(hashtable);
    }

    if shouldFree {
        heap_free_minimal_tuple(tuple);
    }
}

/*
 *      ExecHashRemoveNextSkewBucket
 *
 *      Remove the least valuable skew bucket by pushing its tuples into
 *      the main hash table.
 */
unsafe fn ExecHashRemoveNextSkewBucket(hashtable: HashJoinTable) {
    let bucketToRemove: c_int;
    let bucket: *mut HashSkewBucket;
    let hashvalue: u32;
    let mut bucketno: c_int = 0;
    let mut batchno: c_int = 0;
    let mut hashTuple: HashJoinTuple;

    /* Locate the bucket to remove */
    bucketToRemove = *(*hashtable).skewBucketNums
        .add((*hashtable).nSkewBuckets as usize - 1);
    bucket = *(*hashtable).skewBucket.add(bucketToRemove as usize);

    /*
     * Calculate which bucket and batch the tuples belong to in the main
     * hashtable.  They all have the same hash value, so it's the same for all
     * of them.  Also note that it's not possible for nbatch to increase while
     * we are processing the tuples.
     */
    hashvalue = (*bucket).hashvalue;
    ExecHashGetBucketAndBatch(hashtable, hashvalue, &mut bucketno, &mut batchno);

    /* Process all tuples in the bucket */
    hashTuple = (*bucket).tuples as HashJoinTuple;
    while !hashTuple.is_null() {
        let nextHashTuple: HashJoinTuple = (*hashTuple).next.unshared;
        let tuple: MinimalTuple;
        let tupleSize: Size;

        /*
         * This code must agree with ExecHashTableInsert.  We do not use
         * ExecHashTableInsert directly as ExecHashTableInsert expects a
         * TupleTableSlot while we already have HashJoinTuples.
         */
        tuple = HJTUPLE_MINTUPLE(hashTuple);
        tupleSize = HJTUPLE_OVERHEAD() + (*tuple).t_len as usize;

        /* Decide whether to put the tuple in the hash table or a temp file */
        if batchno == (*hashtable).curbatch {
            /* Move the tuple to the main hash table */
            let copyTuple: HashJoinTuple;

            /*
             * We must copy the tuple into the dense storage, else it will not
             * be found by, eg, ExecHashIncreaseNumBatches.
             */
            copyTuple = dense_alloc(hashtable, tupleSize) as HashJoinTuple;
            ptr::copy_nonoverlapping(hashTuple as *const u8,
                                     copyTuple as *mut u8,
                                     tupleSize);
            pfree(hashTuple as *mut c_void);

            (*copyTuple).next.unshared =
                *(*hashtable).buckets.unshared.add(bucketno as usize);
            *(*hashtable).buckets.unshared.add(bucketno as usize) = copyTuple;

            /* We have reduced skew space, but overall space doesn't change */
            (*hashtable).spaceUsedSkew -= tupleSize;
        } else {
            /* Put the tuple into a temp file for later batches */
            Assert!(batchno > (*hashtable).curbatch);
            ExecHashJoinSaveTuple(tuple, hashvalue,
                                  (*hashtable).innerBatchFile.add(batchno as usize),
                                  hashtable);
            pfree(hashTuple as *mut c_void);
            (*hashtable).spaceUsed -= tupleSize;
            (*hashtable).spaceUsedSkew -= tupleSize;
        }

        hashTuple = nextHashTuple;

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS!();
    }

    /*
     * Free the bucket struct itself and reset the hashtable entry to NULL.
     *
     * NOTE: this is not nearly as simple as it looks on the surface, because
     * of the possibility of collisions in the hashtable.  Suppose that hash
     * values A and B collide at a particular hashtable entry, and that A was
     * entered first so B gets shifted to a different table entry.  If we were
     * to remove A first then ExecHashGetSkewBucket would mistakenly start
     * reporting that B is not in the hashtable, because it would hit the NULL
     * before finding B.  However, we always remove entries in the reverse
     * order of creation, so this failure cannot happen.
     */
    *(*hashtable).skewBucket.add(bucketToRemove as usize) = ptr::null_mut();
    (*hashtable).nSkewBuckets -= 1;
    pfree(bucket as *mut c_void);
    (*hashtable).spaceUsed -= SKEW_BUCKET_OVERHEAD();
    (*hashtable).spaceUsedSkew -= SKEW_BUCKET_OVERHEAD();

    /*
     * If we have removed all skew buckets then give up on skew optimization.
     * Release the arrays since they aren't useful any more.
     */
    if (*hashtable).nSkewBuckets == 0 {
        (*hashtable).skewEnabled = false;
        pfree((*hashtable).skewBucket as *mut c_void);
        pfree((*hashtable).skewBucketNums as *mut c_void);
        (*hashtable).skewBucket = ptr::null_mut();
        (*hashtable).skewBucketNums = ptr::null_mut();
        (*hashtable).spaceUsed -= (*hashtable).spaceUsedSkew;
        (*hashtable).spaceUsedSkew = 0;
    }
}

/*
 * Reserve space in the DSM segment for instrumentation data.
 */
pub unsafe fn ExecHashEstimate(node: *mut HashState, pcxt: *mut ParallelContext) {
    let size: usize;

    /* don't need this if not instrumenting or no workers */
    if (*node).ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = mul_size((*pcxt).nworkers as usize, size_of::<HashInstrumentation>());
    let size = add_size(
        size,
        core::mem::offset_of!(SharedHashInfo, hinstrument),
    );
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, size);
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
}

/*
 * Set up a space in the DSM for all workers to record instrumentation data
 * about their hash table.
 */
pub unsafe fn ExecHashInitializeDSM(node: *mut HashState, pcxt: *mut ParallelContext) {
    let size: usize;

    /* don't need this if not instrumenting or no workers */
    if (*node).ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    size = core::mem::offset_of!(SharedHashInfo, hinstrument)
        + (*pcxt).nworkers as usize * size_of::<HashInstrumentation>();
    (*node).shared_info = shm_toc_allocate((*pcxt).toc, size) as *mut SharedHashInfo;

    /* Each per-worker area must start out as zeroes. */
    ptr::write_bytes((*node).shared_info as *mut u8, 0, size);

    (*(*node).shared_info).num_workers = (*pcxt).nworkers;
    shm_toc_insert((*pcxt).toc,
                   ((*(*node).ps.plan).plan_node_id as u32) as u64,
                   (*node).shared_info as *mut c_void);
}

/*
 * Locate the DSM space for hash table instrumentation data that we'll write
 * to at shutdown time.
 */
pub unsafe fn ExecHashInitializeWorker(node: *mut HashState,
                                       pwcxt: *mut ParallelWorkerContext) {
    let shared_info: *mut SharedHashInfo;

    /* don't need this if not instrumenting */
    if (*node).ps.instrument.is_null() {
        return;
    }

    /*
     * Find our entry in the shared area, and set up a pointer to it so that
     * we'll accumulate stats there when shutting down or rebuilding the hash
     * table.
     */
    shared_info = shm_toc_lookup((*pwcxt).toc,
                                 ((*(*node).ps.plan).plan_node_id as u32) as u64,
                                 false) as *mut SharedHashInfo;
    (*node).hinstrument = ((*shared_info).hinstrument.as_mut_ptr())
        .add(ParallelWorkerNumber as usize);
}

/*
 * Collect EXPLAIN stats if needed, saving them into DSM memory if
 * ExecHashInitializeWorker was called, or local storage if not.  In the
 * parallel case, this must be done in ExecShutdownHash() rather than
 * ExecEndHash() because the latter runs after we've detached from the DSM
 * segment.
 */
pub unsafe fn ExecShutdownHash(node: *mut HashState) {
    /* Allocate save space if EXPLAIN'ing and we didn't do so already */
    if !(*node).ps.instrument.is_null() && (*node).hinstrument.is_null() {
        (*node).hinstrument =
            palloc0(size_of::<HashInstrumentation>()) as *mut HashInstrumentation;
    }
    /* Now accumulate data for the current (final) hash table */
    if !(*node).hinstrument.is_null() && !(*node).hashtable.is_null() {
        ExecHashAccumInstrumentation((*node).hinstrument, (*node).hashtable);
    }
}

/*
 * Retrieve instrumentation data from workers before the DSM segment is
 * detached, so that EXPLAIN can access it.
 */
pub unsafe fn ExecHashRetrieveInstrumentation(node: *mut HashState) {
    let shared_info: *mut SharedHashInfo = (*node).shared_info;
    let size: usize;

    if shared_info.is_null() {
        return;
    }

    /* Replace node->shared_info with a copy in backend-local memory. */
    size = core::mem::offset_of!(SharedHashInfo, hinstrument)
        + (*shared_info).num_workers as usize * size_of::<HashInstrumentation>();
    (*node).shared_info = palloc(size) as *mut SharedHashInfo;
    ptr::copy_nonoverlapping(shared_info as *const u8,
                             (*node).shared_info as *mut u8,
                             size);
}

/*
 * Accumulate instrumentation data from 'hashtable' into an
 * initially-zeroed HashInstrumentation struct.
 *
 * This is used to merge information across successive hash table instances
 * within a single plan node.  We take the maximum values of each interesting
 * number.  The largest nbuckets and largest nbatch values might have occurred
 * in different instances, so there's some risk of confusion from reporting
 * unrelated numbers; but there's a bigger risk of misdiagnosing a performance
 * issue if we don't report the largest values.  Similarly, we want to report
 * the largest spacePeak regardless of whether it happened in the same
 * instance as the largest nbuckets or nbatch.  All the instances should have
 * the same nbuckets_original and nbatch_original; but there's little value
 * in depending on that here, so handle them the same way.
 */
pub unsafe fn ExecHashAccumInstrumentation(instrument: *mut HashInstrumentation,
                                           hashtable: HashJoinTable) {
    (*instrument).nbuckets = (*instrument).nbuckets.max((*hashtable).nbuckets);
    (*instrument).nbuckets_original =
        (*instrument).nbuckets_original.max((*hashtable).nbuckets_original);
    (*instrument).nbatch = (*instrument).nbatch.max((*hashtable).nbatch);
    (*instrument).nbatch_original =
        (*instrument).nbatch_original.max((*hashtable).nbatch_original);
    (*instrument).space_peak =
        (*instrument).space_peak.max((*hashtable).spacePeak);
}

/*
 * Allocate 'size' bytes from the currently active HashMemoryChunk
 */
unsafe fn dense_alloc(hashtable: HashJoinTable, size: Size) -> *mut c_void {
    let newChunk: HashMemoryChunk;
    let ptr: *mut c_char;

    /* just in case the size is not already aligned properly */
    let size = MAXALIGN(size);

    /*
     * If tuple size is larger than threshold, allocate a separate chunk.
     */
    if size > HASH_CHUNK_THRESHOLD {
        /* allocate new chunk and put it at the beginning of the list */
        newChunk = MemoryContextAlloc(
            (*hashtable).batchCxt,
            HASH_CHUNK_HEADER_SIZE() + size,
        ) as HashMemoryChunk;
        (*newChunk).maxlen = size;
        (*newChunk).used = size;
        (*newChunk).ntuples = 1;

        /*
         * Add this chunk to the list after the first existing chunk, so that
         * we don't lose the remaining space in the "current" chunk.
         */
        if !(*hashtable).chunks.is_null() {
            (*newChunk).next.unshared = (*(*hashtable).chunks).next.unshared;
            (*(*hashtable).chunks).next.unshared = newChunk;
        } else {
            (*newChunk).next.unshared = (*hashtable).chunks;
            (*hashtable).chunks = newChunk;
        }

        return HASH_CHUNK_DATA(newChunk) as *mut c_void;
    }

    /*
     * See if we have enough space for it in the current chunk (if any). If
     * not, allocate a fresh chunk.
     */
    if (*hashtable).chunks.is_null()
        || ((*(*hashtable).chunks).maxlen - (*(*hashtable).chunks).used) < size
    {
        /* allocate new chunk and put it at the beginning of the list */
        newChunk = MemoryContextAlloc(
            (*hashtable).batchCxt,
            HASH_CHUNK_HEADER_SIZE() + HASH_CHUNK_SIZE,
        ) as HashMemoryChunk;

        (*newChunk).maxlen = HASH_CHUNK_SIZE;
        (*newChunk).used = size;
        (*newChunk).ntuples = 1;

        (*newChunk).next.unshared = (*hashtable).chunks;
        (*hashtable).chunks = newChunk;

        return HASH_CHUNK_DATA(newChunk) as *mut c_void;
    }

    /* There is enough space in the current chunk, let's add the tuple */
    ptr = (HASH_CHUNK_DATA((*hashtable).chunks) as *mut c_char)
        .add((*(*hashtable).chunks).used);
    (*(*hashtable).chunks).used += size;
    (*(*hashtable).chunks).ntuples += 1;

    /* return pointer to the start of the tuple memory */
    ptr as *mut c_void
}

/*
 * Allocate space for a tuple in shared dense storage.  This is equivalent to
 * dense_alloc but for Parallel Hash using shared memory.
 *
 * While loading a tuple into shared memory, we might run out of memory and
 * decide to repartition, or determine that the load factor is too high and
 * decide to expand the bucket array, or discover that another participant has
 * commanded us to help do that.  Return NULL if number of buckets or batches
 * has changed, indicating that the caller must retry (considering the
 * possibility that the tuple no longer belongs in the same batch).
 */
unsafe fn ExecParallelHashTupleAlloc(hashtable: HashJoinTable,
                                     size: usize,
                                     shared: *mut dsa_pointer) -> HashJoinTuple {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let mut chunk_shared: dsa_pointer;
    let mut chunk: HashMemoryChunk;
    let chunk_size: Size;
    let result: HashJoinTuple;
    let curbatch: c_int = (*hashtable).curbatch;

    let size = MAXALIGN(size);

    /*
     * Fast path: if there is enough space in this backend's current chunk,
     * then we can allocate without any locking.
     */
    chunk = (*hashtable).current_chunk;
    if !chunk.is_null()
        && size <= HASH_CHUNK_THRESHOLD
        && (*chunk).maxlen - (*chunk).used >= size
    {
        chunk_shared = (*hashtable).current_chunk_shared;
        Assert!(chunk == dsa_get_address((*hashtable).area, chunk_shared) as HashMemoryChunk);
        *shared = chunk_shared + HASH_CHUNK_HEADER_SIZE() as dsa_pointer + (*chunk).used as dsa_pointer;
        result = (HASH_CHUNK_DATA(chunk) as *mut u8).add((*chunk).used) as HashJoinTuple;
        (*chunk).used += size;

        Assert!((*chunk).used <= (*chunk).maxlen);
        Assert!(result == dsa_get_address((*hashtable).area, *shared) as HashJoinTuple);

        return result;
    }

    /* Slow path: try to allocate a new chunk. */
    LWLockAcquire(&mut (*pstate).lock, LW_EXCLUSIVE);

    /*
     * Check if we need to help increase the number of buckets or batches.
     */
    if (*pstate).growth == PHJ_GROWTH_NEED_MORE_BATCHES
        || (*pstate).growth == PHJ_GROWTH_NEED_MORE_BUCKETS
    {
        let growth: ParallelHashGrowth = (*pstate).growth;

        (*hashtable).current_chunk = ptr::null_mut();
        LWLockRelease(&mut (*pstate).lock);

        /* Another participant has commanded us to help grow. */
        if growth == PHJ_GROWTH_NEED_MORE_BATCHES {
            ExecParallelHashIncreaseNumBatches(hashtable);
        } else if growth == PHJ_GROWTH_NEED_MORE_BUCKETS {
            ExecParallelHashIncreaseNumBuckets(hashtable);
        }

        /* The caller must retry. */
        return ptr::null_mut();
    }

    /* Oversized tuples get their own chunk. */
    let chunk_size = if size > HASH_CHUNK_THRESHOLD {
        size + HASH_CHUNK_HEADER_SIZE()
    } else {
        HASH_CHUNK_SIZE
    };

    /* Check if it's time to grow batches or buckets. */
    if (*pstate).growth != PHJ_GROWTH_DISABLED {
        Assert!(curbatch == 0);
        Assert!(BarrierPhase(&mut (*pstate).build_barrier) == PHJ_BUILD_HASH_INNER);

        /*
         * Check if our space limit would be exceeded.  To avoid choking on
         * very large tuples or very low hash_mem setting, we'll always allow
         * each backend to allocate at least one chunk.
         */
        if (*(*hashtable).batches.add(0)).at_least_one_chunk
            && (*(*(*hashtable).batches.add(0)).shared).size + chunk_size
                > (*pstate).space_allowed
        {
            (*pstate).growth = PHJ_GROWTH_NEED_MORE_BATCHES;
            (*(*(*hashtable).batches.add(0)).shared).space_exhausted = true;
            LWLockRelease(&mut (*pstate).lock);

            return ptr::null_mut();
        }

        /* Check if our load factor limit would be exceeded. */
        if (*hashtable).nbatch == 1 {
            (*(*(*hashtable).batches.add(0)).shared).ntuples +=
                (*(*hashtable).batches.add(0)).ntuples;
            (*(*hashtable).batches.add(0)).ntuples = 0;
            /* Guard against integer overflow and alloc size overflow */
            if (*(*(*hashtable).batches.add(0)).shared).ntuples + 1
                > (*hashtable).nbuckets as usize * NTUP_PER_BUCKET
                && (*hashtable).nbuckets < c_int::MAX / 2
                && ((*hashtable).nbuckets * 2) as usize
                    <= MaxAllocSize / size_of::<dsa_pointer_atomic>()
            {
                (*pstate).growth = PHJ_GROWTH_NEED_MORE_BUCKETS;
                LWLockRelease(&mut (*pstate).lock);

                return ptr::null_mut();
            }
        }
    }

    /* We are cleared to allocate a new chunk. */
    chunk_shared = dsa_allocate((*hashtable).area, chunk_size);
    (*(*(*hashtable).batches.add(curbatch as usize)).shared).size += chunk_size;
    (*(*hashtable).batches.add(curbatch as usize)).at_least_one_chunk = true;

    /* Set up the chunk. */
    chunk = dsa_get_address((*hashtable).area, chunk_shared) as HashMemoryChunk;
    *shared = chunk_shared + HASH_CHUNK_HEADER_SIZE() as dsa_pointer;
    (*chunk).maxlen = chunk_size - HASH_CHUNK_HEADER_SIZE();
    (*chunk).used = size;

    /*
     * Push it onto the list of chunks, so that it can be found if we need to
     * increase the number of buckets or batches (batch 0 only) and later for
     * freeing the memory (all batches).
     */
    (*chunk).next.shared =
        (*(*(*hashtable).batches.add(curbatch as usize)).shared).chunks;
    (*(*(*hashtable).batches.add(curbatch as usize)).shared).chunks = chunk_shared;

    if size <= HASH_CHUNK_THRESHOLD {
        /*
         * Make this the current chunk so that we can use the fast path to
         * fill the rest of it up in future calls.
         */
        (*hashtable).current_chunk = chunk;
        (*hashtable).current_chunk_shared = chunk_shared;
    }
    LWLockRelease(&mut (*pstate).lock);

    Assert!(HASH_CHUNK_DATA(chunk) as *mut c_void == dsa_get_address((*hashtable).area, *shared));
    result = HASH_CHUNK_DATA(chunk) as HashJoinTuple;

    result
}

/*
 * One backend needs to set up the shared batch state including tuplestores.
 * Other backends will ensure they have correctly configured accessors by
 * called ExecParallelHashEnsureBatchAccessors().
 */
unsafe fn ExecParallelHashJoinSetUpBatches(hashtable: HashJoinTable, nbatch: c_int) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let batches: *mut ParallelHashJoinBatch;
    let oldcxt: MemoryContext;
    let mut i: c_int;

    Assert!((*hashtable).batches.is_null());

    /* Allocate space. */
    (*pstate).batches = dsa_allocate0(
        (*hashtable).area,
        EstimateParallelHashJoinBatch(hashtable) * nbatch as usize,
    );
    (*pstate).nbatch = nbatch;
    batches = dsa_get_address((*hashtable).area, (*pstate).batches) as *mut ParallelHashJoinBatch;

    /*
     * Use hash join spill memory context to allocate accessors, including
     * buffers for the temporary files.
     */
    oldcxt = MemoryContextSwitchTo((*hashtable).spillCxt);

    /* Allocate this backend's accessor array. */
    (*hashtable).nbatch = nbatch;
    (*hashtable).batches =
        palloc0(size_of::<ParallelHashJoinBatchAccessor>() * nbatch as usize)
        as *mut ParallelHashJoinBatchAccessor;

    /* Set up the shared state, tuplestores and backend-local accessors. */
    i = 0;
    while i < (*hashtable).nbatch {
        let accessor: *mut ParallelHashJoinBatchAccessor =
            (*hashtable).batches.add(i as usize);
        let shared: *mut ParallelHashJoinBatch = NthParallelHashJoinBatch(batches, i, hashtable);
        let mut name: [c_char; MAXPGPATH] = [0; MAXPGPATH];

        /*
         * All members of shared were zero-initialized.  We just need to set
         * up the Barrier.
         */
        BarrierInit(&mut (*shared).batch_barrier, 0);
        if i == 0 {
            /* Batch 0 doesn't need to be loaded. */
            BarrierAttach(&mut (*shared).batch_barrier);
            while BarrierPhase(&mut (*shared).batch_barrier) < PHJ_BATCH_PROBE {
                BarrierArriveAndWait(&mut (*shared).batch_barrier, 0);
            }
            BarrierDetach(&mut (*shared).batch_barrier);
        }

        /* Initialize accessor state.  All members were zero-initialized. */
        (*accessor).shared = shared;

        /* Initialize the shared tuplestores. */
        snprintf_name(name.as_mut_ptr(), name.len(), i, (*hashtable).nbatch);
        (*accessor).inner_tuples = sts_initialize(
            ParallelHashJoinBatchInner(shared),
            (*pstate).nparticipants,
            ParallelWorkerNumber + 1,
            size_of::<u32>(),
            SHARED_TUPLESTORE_SINGLE_PASS,
            &mut (*pstate).fileset,
            name.as_ptr(),
        );
        /* write "o%dof%d" name */
        snprintf_outer_name(name.as_mut_ptr(), name.len(), i, (*hashtable).nbatch);
        (*accessor).outer_tuples = sts_initialize(
            ParallelHashJoinBatchOuter(shared, (*pstate).nparticipants),
            (*pstate).nparticipants,
            ParallelWorkerNumber + 1,
            size_of::<u32>(),
            SHARED_TUPLESTORE_SINGLE_PASS,
            &mut (*pstate).fileset,
            name.as_ptr(),
        );

        i += 1;
    }

    MemoryContextSwitchTo(oldcxt);
}

// helper: format "o%dof%d" name for outer tuplestore
#[inline]
unsafe fn snprintf_outer_name(buf: *mut c_char, n: usize, i: c_int, nbatch: c_int) {
    /* write "o%dof%d" into buf */
    let s = format!("o{}of{}\0", i, nbatch);
    let bytes = s.as_bytes();
    let copy_len = bytes.len().min(n);
    ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, copy_len);
}

/*
 * Free the current set of ParallelHashJoinBatchAccessor objects.
 */
unsafe fn ExecParallelHashCloseBatchAccessors(hashtable: HashJoinTable) {
    let mut i: c_int = 0;

    while i < (*hashtable).nbatch {
        /* Make sure no files are left open. */
        sts_end_write((*(*hashtable).batches.add(i as usize)).inner_tuples);
        sts_end_write((*(*hashtable).batches.add(i as usize)).outer_tuples);
        sts_end_parallel_scan((*(*hashtable).batches.add(i as usize)).inner_tuples);
        sts_end_parallel_scan((*(*hashtable).batches.add(i as usize)).outer_tuples);
        i += 1;
    }
    pfree((*hashtable).batches as *mut c_void);
    (*hashtable).batches = ptr::null_mut();
}

/*
 * Make sure this backend has up-to-date accessors for the current set of
 * batches.
 */
unsafe fn ExecParallelHashEnsureBatchAccessors(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let batches: *mut ParallelHashJoinBatch;
    let oldcxt: MemoryContext;
    let mut i: c_int;

    if !(*hashtable).batches.is_null() {
        if (*hashtable).nbatch == (*pstate).nbatch {
            return;
        }
        ExecParallelHashCloseBatchAccessors(hashtable);
    }

    /*
     * We should never see a state where the batch-tracking array is freed,
     * because we should have given up sooner if we join when the build
     * barrier has reached the PHJ_BUILD_FREE phase.
     */
    Assert!(DsaPointerIsValid((*pstate).batches));

    /*
     * Use hash join spill memory context to allocate accessors, including
     * buffers for the temporary files.
     */
    oldcxt = MemoryContextSwitchTo((*hashtable).spillCxt);

    /* Allocate this backend's accessor array. */
    (*hashtable).nbatch = (*pstate).nbatch;
    (*hashtable).batches =
        palloc0(size_of::<ParallelHashJoinBatchAccessor>() * (*hashtable).nbatch as usize)
        as *mut ParallelHashJoinBatchAccessor;

    /* Find the base of the pseudo-array of ParallelHashJoinBatch objects. */
    batches = dsa_get_address((*hashtable).area, (*pstate).batches)
        as *mut ParallelHashJoinBatch;

    /* Set up the accessor array and attach to the tuplestores. */
    i = 0;
    while i < (*hashtable).nbatch {
        let accessor: *mut ParallelHashJoinBatchAccessor =
            (*hashtable).batches.add(i as usize);
        let shared: *mut ParallelHashJoinBatch = NthParallelHashJoinBatch(batches, i, hashtable);

        (*accessor).shared = shared;
        (*accessor).preallocated = 0;
        (*accessor).done = false;
        (*accessor).outer_eof = false;
        (*accessor).inner_tuples = sts_attach(
            ParallelHashJoinBatchInner(shared),
            ParallelWorkerNumber + 1,
            &mut (*pstate).fileset,
        );
        (*accessor).outer_tuples = sts_attach(
            ParallelHashJoinBatchOuter(shared, (*pstate).nparticipants),
            ParallelWorkerNumber + 1,
            &mut (*pstate).fileset,
        );

        i += 1;
    }

    MemoryContextSwitchTo(oldcxt);
}

/*
 * Allocate an empty shared memory hash table for a given batch.
 */
pub unsafe fn ExecParallelHashTableAlloc(hashtable: HashJoinTable, batchno: c_int) {
    let batch: *mut ParallelHashJoinBatch =
        (*(*hashtable).batches.add(batchno as usize)).shared;
    let buckets: *mut dsa_pointer_atomic;
    let nbuckets: c_int = (*(*hashtable).parallel_state).nbuckets;
    let mut i: c_int;

    (*batch).buckets = dsa_allocate(
        (*hashtable).area,
        size_of::<dsa_pointer_atomic>() * nbuckets as usize,
    );
    buckets = dsa_get_address((*hashtable).area, (*batch).buckets) as *mut dsa_pointer_atomic;
    i = 0;
    while i < nbuckets {
        dsa_pointer_atomic_init(buckets.add(i as usize), InvalidDsaPointer);
        i += 1;
    }
}

/*
 * If we are currently attached to a shared hash join batch, detach.  If we
 * are last to detach, clean up.
 */
pub unsafe fn ExecHashTableDetachBatch(hashtable: HashJoinTable) {
    if !(*hashtable).parallel_state.is_null() && (*hashtable).curbatch >= 0 {
        let curbatch: c_int = (*hashtable).curbatch;
        let batch: *mut ParallelHashJoinBatch =
            (*(*hashtable).batches.add(curbatch as usize)).shared;
        let mut attached: bool = true;

        /* Make sure any temporary files are closed. */
        sts_end_parallel_scan((*(*hashtable).batches.add(curbatch as usize)).inner_tuples);
        sts_end_parallel_scan((*(*hashtable).batches.add(curbatch as usize)).outer_tuples);

        /* After attaching we always get at least to PHJ_BATCH_PROBE. */
        Assert!(BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_PROBE
                || BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_SCAN);

        /*
         * If we're abandoning the PHJ_BATCH_PROBE phase early without having
         * reached the end of it, it means the plan doesn't want any more
         * tuples, and it is happy to abandon any tuples buffered in this
         * process's subplans.  For correctness, we can't allow any process to
         * execute the PHJ_BATCH_SCAN phase, because we will never have the
         * complete set of match bits.  Therefore we skip emitting unmatched
         * tuples in all backends (if this is a full/right join), as if those
         * tuples were all due to be emitted by this process and it has
         * abandoned them too.
         */
        if BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_PROBE
            && !(*(*hashtable).batches.add(curbatch as usize)).outer_eof
        {
            /*
             * This flag may be written to by multiple backends during
             * PHJ_BATCH_PROBE phase, but will only be read in PHJ_BATCH_SCAN
             * phase so requires no extra locking.
             */
            (*batch).skip_unmatched = true;
        }

        /*
         * Even if we aren't doing a full/right outer join, we'll step through
         * the PHJ_BATCH_SCAN phase just to maintain the invariant that
         * freeing happens in PHJ_BATCH_FREE, but that'll be wait-free.
         */
        if BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_PROBE {
            attached = BarrierArriveAndDetachExceptLast(&mut (*batch).batch_barrier);
        }
        if attached && BarrierArriveAndDetach(&mut (*batch).batch_barrier) {
            /*
             * We are not longer attached to the batch barrier, but we're the
             * process that was chosen to free resources and it's safe to
             * assert the current phase.  The ParallelHashJoinBatch can't go
             * away underneath us while we are attached to the build barrier,
             * making this access safe.
             */
            Assert!(BarrierPhase(&mut (*batch).batch_barrier) == PHJ_BATCH_FREE);

            /* Free shared chunks and buckets. */
            while DsaPointerIsValid((*batch).chunks) {
                let chunk: HashMemoryChunk =
                    dsa_get_address((*hashtable).area, (*batch).chunks) as HashMemoryChunk;
                let next: dsa_pointer = (*chunk).next.shared;

                dsa_free((*hashtable).area, (*batch).chunks);
                (*batch).chunks = next;
            }
            if DsaPointerIsValid((*batch).buckets) {
                dsa_free((*hashtable).area, (*batch).buckets);
                (*batch).buckets = InvalidDsaPointer;
            }
        }

        /*
         * Track the largest batch we've been attached to.  Though each
         * backend might see a different subset of batches, explain.c will
         * scan the results from all backends to find the largest value.
         */
        hashtable_spacepeak_update(hashtable, batch);

        /* Remember that we are not attached to a batch. */
        (*hashtable).curbatch = -1;
    }
}

/*
 * Detach from all shared resources.  If we are last to detach, clean up.
 */
pub unsafe fn ExecHashTableDetach(hashtable: HashJoinTable) {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;

    /*
     * If we're involved in a parallel query, we must either have gotten all
     * the way to PHJ_BUILD_RUN, or joined too late and be in PHJ_BUILD_FREE.
     */
    Assert!(pstate.is_null()
            || BarrierPhase(&mut (*pstate).build_barrier) >= PHJ_BUILD_RUN);

    if !pstate.is_null()
        && BarrierPhase(&mut (*pstate).build_barrier) == PHJ_BUILD_RUN
    {
        let mut i: c_int;

        /* Make sure any temporary files are closed. */
        if !(*hashtable).batches.is_null() {
            i = 0;
            while i < (*hashtable).nbatch {
                sts_end_write((*(*hashtable).batches.add(i as usize)).inner_tuples);
                sts_end_write((*(*hashtable).batches.add(i as usize)).outer_tuples);
                sts_end_parallel_scan((*(*hashtable).batches.add(i as usize)).inner_tuples);
                sts_end_parallel_scan((*(*hashtable).batches.add(i as usize)).outer_tuples);
                i += 1;
            }
        }

        /* If we're last to detach, clean up shared memory. */
        if BarrierArriveAndDetach(&mut (*pstate).build_barrier) {
            /*
             * Late joining processes will see this state and give up
             * immediately.
             */
            Assert!(BarrierPhase(&mut (*pstate).build_barrier) == PHJ_BUILD_FREE);

            if DsaPointerIsValid((*pstate).batches) {
                dsa_free((*hashtable).area, (*pstate).batches);
                (*pstate).batches = InvalidDsaPointer;
            }
        }
    }
    (*hashtable).parallel_state = ptr::null_mut();
}

/*
 * Get the first tuple in a given bucket identified by number.
 */
#[inline]
unsafe fn ExecParallelHashFirstTuple(hashtable: HashJoinTable,
                                     bucketno: c_int) -> HashJoinTuple {
    let tuple: HashJoinTuple;
    let p: dsa_pointer;

    Assert!(!(*hashtable).parallel_state.is_null());
    p = dsa_pointer_atomic_read((*hashtable).buckets.shared.add(bucketno as usize));
    tuple = dsa_get_address((*hashtable).area, p) as HashJoinTuple;

    tuple
}

/*
 * Get the next tuple in the same bucket as 'tuple'.
 */
#[inline]
unsafe fn ExecParallelHashNextTuple(hashtable: HashJoinTable,
                                    tuple: HashJoinTuple) -> HashJoinTuple {
    let next: HashJoinTuple;

    Assert!(!(*hashtable).parallel_state.is_null());
    next = dsa_get_address((*hashtable).area, (*tuple).next.shared) as HashJoinTuple;

    next
}

/*
 * Insert a tuple at the front of a chain of tuples in DSA memory atomically.
 */
#[inline]
unsafe fn ExecParallelHashPushTuple(head: *mut dsa_pointer_atomic,
                                    tuple: HashJoinTuple,
                                    tuple_shared: dsa_pointer) {
    loop {
        (*tuple).next.shared = dsa_pointer_atomic_read(head);
        if dsa_pointer_atomic_compare_exchange(head,
                                               &mut (*tuple).next.shared,
                                               tuple_shared) {
            break;
        }
    }
}

/*
 * Prepare to work on a given batch.
 */
pub unsafe fn ExecParallelHashTableSetCurrentBatch(hashtable: HashJoinTable,
                                                   batchno: c_int) {
    Assert!((*(*(*hashtable).batches.add(batchno as usize)).shared).buckets != InvalidDsaPointer);

    (*hashtable).curbatch = batchno;
    (*hashtable).buckets.shared = dsa_get_address(
        (*hashtable).area,
        (*(*(*hashtable).batches.add(batchno as usize)).shared).buckets,
    ) as *mut dsa_pointer_atomic;
    (*hashtable).nbuckets = (*(*hashtable).parallel_state).nbuckets;
    (*hashtable).log2_nbuckets = my_log2((*hashtable).nbuckets as c_long);
    (*hashtable).current_chunk = ptr::null_mut();
    (*hashtable).current_chunk_shared = InvalidDsaPointer;
    (*(*hashtable).batches.add(batchno as usize)).at_least_one_chunk = false;
}

/*
 * Take the next available chunk from the queue of chunks being worked on in
 * parallel.  Return NULL if there are none left.  Otherwise return a pointer
 * to the chunk, and set *shared to the DSA pointer to the chunk.
 */
unsafe fn ExecParallelHashPopChunkQueue(hashtable: HashJoinTable,
                                        shared: *mut dsa_pointer) -> HashMemoryChunk {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let chunk: HashMemoryChunk;

    LWLockAcquire(&mut (*pstate).lock, LW_EXCLUSIVE);
    if DsaPointerIsValid((*pstate).chunk_work_queue) {
        *shared = (*pstate).chunk_work_queue;
        chunk = dsa_get_address((*hashtable).area, *shared) as HashMemoryChunk;
        (*pstate).chunk_work_queue = (*chunk).next.shared;
    } else {
        chunk = ptr::null_mut();
    }
    LWLockRelease(&mut (*pstate).lock);

    chunk
}

/*
 * Increase the space preallocated in this backend for a given inner batch by
 * at least a given amount.  This allows us to track whether a given batch
 * would fit in memory when loaded back in.  Also increase the number of
 * batches or buckets if required.
 *
 * This maintains a running estimation of how much space will be taken when we
 * load the batch back into memory by simulating the way chunks will be handed
 * out to workers.  It's not perfectly accurate because the tuples will be
 * packed into memory chunks differently by ExecParallelHashTupleAlloc(), but
 * it should be pretty close.  It tends to overestimate by a fraction of a
 * chunk per worker since all workers gang up to preallocate during hashing,
 * but workers tend to reload batches alone if there are enough to go around,
 * leaving fewer partially filled chunks.  This effect is bounded by
 * nparticipants.
 *
 * Return false if the number of batches or buckets has changed, and the
 * caller should reconsider which batch a given tuple now belongs in and call
 * again.
 */
unsafe fn ExecParallelHashTuplePrealloc(hashtable: HashJoinTable,
                                        batchno: c_int,
                                        size: usize) -> bool {
    let pstate: *mut ParallelHashJoinState = (*hashtable).parallel_state;
    let batch: *mut ParallelHashJoinBatchAccessor =
        (*hashtable).batches.add(batchno as usize);
    let want: usize = size.max(HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE());

    Assert!(batchno > 0);
    Assert!(batchno < (*hashtable).nbatch);
    Assert!(size == MAXALIGN(size));

    LWLockAcquire(&mut (*pstate).lock, LW_EXCLUSIVE);

    /* Has another participant commanded us to help grow? */
    if (*pstate).growth == PHJ_GROWTH_NEED_MORE_BATCHES
        || (*pstate).growth == PHJ_GROWTH_NEED_MORE_BUCKETS
    {
        let growth: ParallelHashGrowth = (*pstate).growth;

        LWLockRelease(&mut (*pstate).lock);
        if growth == PHJ_GROWTH_NEED_MORE_BATCHES {
            ExecParallelHashIncreaseNumBatches(hashtable);
        } else if growth == PHJ_GROWTH_NEED_MORE_BUCKETS {
            ExecParallelHashIncreaseNumBuckets(hashtable);
        }

        return false;
    }

    if (*pstate).growth != PHJ_GROWTH_DISABLED
        && (*batch).at_least_one_chunk
        && (*(*batch).shared).estimated_size + want + HASH_CHUNK_HEADER_SIZE()
            > (*pstate).space_allowed
    {
        /*
         * We have determined that this batch would exceed the space budget if
         * loaded into memory.  Command all participants to help repartition.
         */
        (*(*batch).shared).space_exhausted = true;
        (*pstate).growth = PHJ_GROWTH_NEED_MORE_BATCHES;
        LWLockRelease(&mut (*pstate).lock);

        return false;
    }

    (*batch).at_least_one_chunk = true;
    (*(*batch).shared).estimated_size += want + HASH_CHUNK_HEADER_SIZE();
    (*batch).preallocated = want;
    LWLockRelease(&mut (*pstate).lock);

    true
}

/*
 * Calculate the limit on how much memory can be used by Hash and similar
 * plan types.  This is work_mem times hash_mem_multiplier, and is
 * expressed in bytes.
 *
 * Exported for use by the planner, as well as other hash-like executor
 * nodes.  This is a rather random place for this, but there is no better
 * place.
 */
pub unsafe fn get_hash_memory_limit() -> usize {
    let mem_limit: f64;

    /* Do initial calculation in double arithmetic */
    mem_limit = work_mem as f64 * hash_mem_multiplier * 1024.0;

    /* Clamp in case it doesn't fit in size_t */
    let mem_limit = mem_limit.min(usize::MAX as f64);

    mem_limit as usize
}

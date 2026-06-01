//! executor/hashjoin.h - internal structures for hash joins.

use std::ffi::c_int;

use crate::c::{uint32, Size, MAXALIGN};
use crate::nodes::execnodes::{dsa_area, dsa_pointer, HashJoinTuple};
use crate::access::htup_details::MinimalTuple;
use crate::storage::ipc::barrier::Barrier;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::port::atomics::pg_atomic_uint32;

// ---------------------------------------------------------------------------
// Types referenced-but-not-yet-defined elsewhere in the tree.
// Minimal local stubs; TODO: dedup once real ports land.
// ---------------------------------------------------------------------------

/// TODO: dedup - real def `typedef struct LWLock LWLock` in storage/lwlock.h.
/// (A stub `LWLock` exists in utils/activity/pgstat.rs; that one lives in a
/// different module so no conflict.)
#[repr(C)]
pub struct LWLock {
    _opaque: [u8; 0],
}

/// TODO: dedup - real def `dsa_pointer_atomic` in utils/dsa.h
/// (`typedef pg_atomic_uint64 dsa_pointer_atomic` for 8-byte dsa_pointer).
pub type dsa_pointer_atomic = crate::port::atomics::pg_atomic_uint64;

/// TODO: dedup - real def `typedef struct SharedFileSet` in storage/sharedfileset.h.
#[repr(C)]
pub struct SharedFileSet {
    _opaque: [u8; 0],
}

/// TODO: dedup - real def `typedef struct BufFile` in storage/buffile.h.
#[repr(C)]
pub struct BufFile {
    _opaque: [u8; 0],
}

/// TODO: dedup - real def `typedef struct SharedTuplestore` in utils/sharedtuplestore.h.
#[repr(C)]
pub struct SharedTuplestore {
    _opaque: [u8; 0],
}

/// TODO: dedup - real def `typedef struct SharedTuplestoreAccessor`
/// in utils/sharedtuplestore.h.
#[repr(C)]
pub struct SharedTuplestoreAccessor {
    _opaque: [u8; 0],
}

// ---------------------------------------------------------------------------
// HashJoinTupleData
// ---------------------------------------------------------------------------

/// union { struct HashJoinTupleData *unshared; dsa_pointer shared; } next;
#[repr(C)]
pub union HashJoinTupleData_next {
    pub unshared: *mut HashJoinTupleData,
    pub shared: dsa_pointer,
}

#[repr(C)]
pub struct HashJoinTupleData {
    /// link to next tuple in same bucket
    pub next: HashJoinTupleData_next,
    /// tuple's hash code
    pub hashvalue: uint32,
    /* Tuple data, in MinimalTuple format, follows on a MAXALIGN boundary */
}

#[inline]
pub fn HJTUPLE_OVERHEAD() -> usize {
    MAXALIGN(core::mem::size_of::<HashJoinTupleData>())
}

#[inline]
pub unsafe fn HJTUPLE_MINTUPLE(hjtup: *mut HashJoinTupleData) -> MinimalTuple {
    ((hjtup as *mut std::ffi::c_char).add(HJTUPLE_OVERHEAD())) as MinimalTuple
}

// ---------------------------------------------------------------------------
// HashSkewBucket
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct HashSkewBucket {
    /// common hash value
    pub hashvalue: uint32,
    /// linked list of inner-relation tuples
    pub tuples: HashJoinTuple,
}

#[inline]
pub fn SKEW_BUCKET_OVERHEAD() -> usize {
    MAXALIGN(core::mem::size_of::<HashSkewBucket>())
}
pub const INVALID_SKEW_BUCKET_NO: c_int = -1;
pub const SKEW_HASH_MEM_PERCENT: c_int = 2;
pub const SKEW_MIN_OUTER_FRACTION: f64 = 0.01;

// ---------------------------------------------------------------------------
// HashMemoryChunkData
// ---------------------------------------------------------------------------

/// union { struct HashMemoryChunkData *unshared; dsa_pointer shared; } next;
#[repr(C)]
pub union HashMemoryChunkData_next {
    pub unshared: *mut HashMemoryChunkData,
    pub shared: dsa_pointer,
}

#[repr(C)]
pub struct HashMemoryChunkData {
    /// number of tuples stored in this chunk
    pub ntuples: c_int,
    /// size of the chunk's tuple buffer
    pub maxlen: usize,
    /// number of buffer bytes already used
    pub used: usize,

    /// pointer to the next chunk (linked list)
    pub next: HashMemoryChunkData_next,
    /*
     * The chunk's tuple buffer starts after the HashMemoryChunkData struct,
     * at offset HASH_CHUNK_HEADER_SIZE (which must be maxaligned).  Note that
     * that offset is not included in "maxlen" or "used".
     */
}

pub type HashMemoryChunk = *mut HashMemoryChunkData;

pub const HASH_CHUNK_SIZE: Size = (32 * 1024) as Size;
#[inline]
pub fn HASH_CHUNK_HEADER_SIZE() -> usize {
    MAXALIGN(core::mem::size_of::<HashMemoryChunkData>())
}
#[inline]
pub unsafe fn HASH_CHUNK_DATA(hc: HashMemoryChunk) -> *mut std::ffi::c_char {
    (hc as *mut std::ffi::c_char).add(HASH_CHUNK_HEADER_SIZE())
}
/// tuples exceeding HASH_CHUNK_THRESHOLD bytes are put in their own chunk
pub const HASH_CHUNK_THRESHOLD: Size = HASH_CHUNK_SIZE / 4;

// ---------------------------------------------------------------------------
// ParallelHashJoinBatch
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct ParallelHashJoinBatch {
    /// array of hash table buckets
    pub buckets: dsa_pointer,
    /// synchronization for joining this batch
    pub batch_barrier: Barrier,

    /// chunks of tuples loaded
    pub chunks: dsa_pointer,
    /// size of buckets + chunks in memory
    pub size: usize,
    /// size of buckets + chunks while writing
    pub estimated_size: usize,
    /// number of tuples loaded
    pub ntuples: usize,
    /// number of tuples before repartitioning
    pub old_ntuples: usize,
    pub space_exhausted: bool,
    /// whether to abandon unmatched scan
    pub skip_unmatched: bool,
    /*
     * Variable-sized SharedTuplestore objects follow this struct in memory.
     * See the accessor macros below.
     */
}

/// Accessor for inner batch tuplestore following a ParallelHashJoinBatch.
#[inline]
pub unsafe fn ParallelHashJoinBatchInner(
    batch: *mut ParallelHashJoinBatch,
) -> *mut SharedTuplestore {
    ((batch as *mut std::ffi::c_char)
        .add(MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>()))) as *mut SharedTuplestore
}

/// Accessor for outer batch tuplestore following a ParallelHashJoinBatch.
#[inline]
pub unsafe fn ParallelHashJoinBatchOuter(
    batch: *mut ParallelHashJoinBatch,
    nparticipants: c_int,
) -> *mut SharedTuplestore {
    ((ParallelHashJoinBatchInner(batch) as *mut std::ffi::c_char)
        .add(MAXALIGN(sts_estimate(nparticipants)))) as *mut SharedTuplestore
}

/// Total size of a ParallelHashJoinBatch and tuplestores.
#[inline]
pub unsafe fn EstimateParallelHashJoinBatch(hashtable: HashJoinTable) -> usize {
    MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>())
        + MAXALIGN(sts_estimate((*(*hashtable).parallel_state).nparticipants)) * 2
}

/// Accessor for the nth ParallelHashJoinBatch given the base.
///
/// NOTE: the original C macro references a `hashtable` variable from the call
/// site (via EstimateParallelHashJoinBatch(hashtable)); replicated here as an
/// explicit parameter.
#[inline]
pub unsafe fn NthParallelHashJoinBatch(
    base: *mut ParallelHashJoinBatch,
    n: c_int,
    hashtable: HashJoinTable,
) -> *mut ParallelHashJoinBatch {
    ((base as *mut std::ffi::c_char)
        .add(EstimateParallelHashJoinBatch(hashtable) * (n as usize)))
        as *mut ParallelHashJoinBatch
}

// ---------------------------------------------------------------------------
// ParallelHashJoinBatchAccessor
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct ParallelHashJoinBatchAccessor {
    /// pointer to shared state
    pub shared: *mut ParallelHashJoinBatch,

    /* Per-backend partial counters to reduce contention. */
    /// pre-allocated space for this backend
    pub preallocated: usize,
    /// number of tuples
    pub ntuples: usize,
    /// size of partition in memory
    pub size: usize,
    /// size of partition on disk
    pub estimated_size: usize,
    /// how many tuples before repartitioning?
    pub old_ntuples: usize,
    /// has this backend allocated a chunk?
    pub at_least_one_chunk: bool,
    /// has this process hit end of batch?
    pub outer_eof: bool,
    /// flag to remember that a batch is done
    pub done: bool,
    pub inner_tuples: *mut SharedTuplestoreAccessor,
    pub outer_tuples: *mut SharedTuplestoreAccessor,
}

// ---------------------------------------------------------------------------
// ParallelHashGrowth (C enum -> c_int + consts, per project convention)
// ---------------------------------------------------------------------------

pub type ParallelHashGrowth = c_int;
/// The current dimensions are sufficient.
pub const PHJ_GROWTH_OK: ParallelHashGrowth = 0;
/// The load factor is too high, so we need to add buckets.
pub const PHJ_GROWTH_NEED_MORE_BUCKETS: ParallelHashGrowth = 1;
/// The memory budget would be exhausted, so we need to repartition.
pub const PHJ_GROWTH_NEED_MORE_BATCHES: ParallelHashGrowth = 2;
/// Repartitioning didn't help last time, so don't try to do that again.
pub const PHJ_GROWTH_DISABLED: ParallelHashGrowth = 3;

// ---------------------------------------------------------------------------
// ParallelHashJoinState
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct ParallelHashJoinState {
    /// array of ParallelHashJoinBatch
    pub batches: dsa_pointer,
    /// previous generation during repartition
    pub old_batches: dsa_pointer,
    /// number of batches now
    pub nbatch: c_int,
    /// previous number of batches
    pub old_nbatch: c_int,
    /// number of buckets
    pub nbuckets: c_int,
    /// control batch/bucket growth
    pub growth: ParallelHashGrowth,
    /// chunk work queue
    pub chunk_work_queue: dsa_pointer,
    pub nparticipants: c_int,
    pub space_allowed: usize,
    /// total number of inner tuples
    pub total_tuples: usize,
    /// lock protecting the above
    pub lock: LWLock,

    /// synchronization for the build phases
    pub build_barrier: Barrier,
    pub grow_batches_barrier: Barrier,
    pub grow_buckets_barrier: Barrier,
    /// counter for load balancing
    pub distributor: pg_atomic_uint32,

    /// space for shared temporary files
    pub fileset: SharedFileSet,
}

// The phases for building batches, used by build_barrier.
pub const PHJ_BUILD_ELECT: c_int = 0;
pub const PHJ_BUILD_ALLOCATE: c_int = 1;
pub const PHJ_BUILD_HASH_INNER: c_int = 2;
pub const PHJ_BUILD_HASH_OUTER: c_int = 3;
pub const PHJ_BUILD_RUN: c_int = 4;
pub const PHJ_BUILD_FREE: c_int = 5;

// The phases for probing each batch, used by for batch_barrier.
pub const PHJ_BATCH_ELECT: c_int = 0;
pub const PHJ_BATCH_ALLOCATE: c_int = 1;
pub const PHJ_BATCH_LOAD: c_int = 2;
pub const PHJ_BATCH_PROBE: c_int = 3;
pub const PHJ_BATCH_SCAN: c_int = 4;
pub const PHJ_BATCH_FREE: c_int = 5;

// The phases of batch growth while hashing, for grow_batches_barrier.
pub const PHJ_GROW_BATCHES_ELECT: c_int = 0;
pub const PHJ_GROW_BATCHES_REALLOCATE: c_int = 1;
pub const PHJ_GROW_BATCHES_REPARTITION: c_int = 2;
pub const PHJ_GROW_BATCHES_DECIDE: c_int = 3;
pub const PHJ_GROW_BATCHES_FINISH: c_int = 4;
/// circular phases
#[inline]
pub fn PHJ_GROW_BATCHES_PHASE(n: c_int) -> c_int {
    n % 5
}

// The phases of bucket growth while hashing, for grow_buckets_barrier.
pub const PHJ_GROW_BUCKETS_ELECT: c_int = 0;
pub const PHJ_GROW_BUCKETS_REALLOCATE: c_int = 1;
pub const PHJ_GROW_BUCKETS_REINSERT: c_int = 2;
/// circular phases
#[inline]
pub fn PHJ_GROW_BUCKETS_PHASE(n: c_int) -> c_int {
    n % 3
}

// ---------------------------------------------------------------------------
// HashJoinTableData
// ---------------------------------------------------------------------------

/// union {
///     struct HashJoinTupleData **unshared;
///     dsa_pointer_atomic *shared;
/// } buckets;
#[repr(C)]
pub union HashJoinTableData_buckets {
    /// unshared array is per-batch storage, as are all the tuples
    pub unshared: *mut *mut HashJoinTupleData,
    /// shared array is per-query DSA area, as are all the tuples
    pub shared: *mut dsa_pointer_atomic,
}

#[repr(C)]
pub struct HashJoinTableData {
    /// # buckets in the in-memory hash table
    pub nbuckets: c_int,
    /// its log2 (nbuckets must be a power of 2)
    pub log2_nbuckets: c_int,

    /// # buckets when starting the first hash
    pub nbuckets_original: c_int,
    /// optimal # buckets (per batch)
    pub nbuckets_optimal: c_int,
    /// log2(nbuckets_optimal)
    pub log2_nbuckets_optimal: c_int,

    /// buckets[i] is head of list of tuples in i'th in-memory bucket
    pub buckets: HashJoinTableData_buckets,

    /// are we using skew optimization?
    pub skewEnabled: bool,
    /// hashtable of skew buckets
    pub skewBucket: *mut *mut HashSkewBucket,
    /// size of skewBucket array (a power of 2!)
    pub skewBucketLen: c_int,
    /// number of active skew buckets
    pub nSkewBuckets: c_int,
    /// array indexes of active skew buckets
    pub skewBucketNums: *mut c_int,

    /// number of batches
    pub nbatch: c_int,
    /// current batch #; 0 during 1st pass
    pub curbatch: c_int,

    /// nbatch when we started inner scan
    pub nbatch_original: c_int,
    /// nbatch when we started outer scan
    pub nbatch_outstart: c_int,

    /// flag to shut off nbatch increases
    pub growEnabled: bool,

    /// # tuples obtained from inner plan
    pub totalTuples: f64,
    /// # tuples obtained from inner plan by me
    pub partialTuples: f64,
    /// # tuples inserted into skew tuples
    pub skewTuples: f64,

    /*
     * These arrays are allocated for the life of the hash join, but only if
     * nbatch > 1.  A file is opened only when we first write a tuple into it
     * (otherwise its pointer remains NULL).  Note that the zero'th array
     * elements never get used, since we will process rather than dump out any
     * tuples of batch zero.
     */
    /// buffered virtual temp file per batch
    pub innerBatchFile: *mut *mut BufFile,
    /// buffered virtual temp file per batch
    pub outerBatchFile: *mut *mut BufFile,

    /// memory space currently used by tuples
    pub spaceUsed: Size,
    /// upper limit for space used
    pub spaceAllowed: Size,
    /// peak space used
    pub spacePeak: Size,
    /// skew hash table's current space usage
    pub spaceUsedSkew: Size,
    /// upper limit for skew hashtable
    pub spaceAllowedSkew: Size,

    /// context for whole-hash-join storage
    pub hashCxt: MemoryContext,
    /// context for this-batch-only storage
    pub batchCxt: MemoryContext,
    /// context for spilling to temp files
    pub spillCxt: MemoryContext,

    /// used for dense allocation of tuples (into linked chunks);
    /// one list for the whole batch
    pub chunks: HashMemoryChunk,

    /* Shared and private state for Parallel Hash. */
    /// this backend's current chunk
    pub current_chunk: HashMemoryChunk,
    /// DSA area to allocate memory from
    pub area: *mut dsa_area,
    pub parallel_state: *mut ParallelHashJoinState,
    pub batches: *mut ParallelHashJoinBatchAccessor,
    pub current_chunk_shared: dsa_pointer,
}

pub type HashJoinTable = *mut HashJoinTableData;

// ---------------------------------------------------------------------------
// Helper referenced by the accessor macros above.
// `sts_estimate(nparticipants)` is declared in utils/sharedtuplestore.h.
// ---------------------------------------------------------------------------

/// TODO: dedup - prototype from utils/sharedtuplestore.h:
/// `extern size_t sts_estimate(int participants)`.
pub unsafe fn sts_estimate(participants: c_int) -> usize {
    let _ = participants;
    unimplemented!()
}

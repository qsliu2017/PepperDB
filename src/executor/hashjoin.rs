//! Translated from PostgreSQL src/include/executor/hashjoin.h

use crate::c::{Size, MAXALIGN};
use crate::storage::buffile::BufFile;
use crate::storage::sharedfileset::SharedFileSet;
use crate::utils::sharedtuplestore::{SharedTuplestoreAccessor, sts_estimate};
use core::sync::atomic::AtomicU32;
use parking_lot::Mutex;

// HashJoinTuple / HashJoinTable are forward-declared opaque in execnodes.h; this
// header provides their real definitions. Resolved here as `HashJoinTuple` and
// `HashJoinTable` below (in-memory).

/// In-memory packed hash-join tuple header. A MinimalTuple follows on a MAXALIGN
/// boundary at offset HJTUPLE_OVERHEAD (an in-memory FAM in the owning chunk).
/// The C `next` union (unshared ptr / dsa_pointer) collapses to the unshared
/// variant under the single-process model; parallel-hash sharing is reintroduced
/// with the DSA replacement.
pub struct HashJoinTupleData {
    /// link to next tuple in same bucket
    pub next: Option<Box<Self>>,
    pub hashvalue: u32, // tuple's hash code
    // Tuple data (MinimalTuple) follows on a MAXALIGN boundary; see HJTUPLE_*.
}

pub type HashJoinTuple = Box<HashJoinTupleData>;

pub const fn HJTUPLE_OVERHEAD() -> usize {
    MAXALIGN(core::mem::size_of::<HashJoinTupleData>())
}

/// Offset to the MinimalTuple bytes following a HashJoinTupleData header.
pub fn HJTUPLE_MINTUPLE(_hjtup: &HashJoinTupleData) -> usize {
    HJTUPLE_OVERHEAD()
}

/// Skew-optimization bucket: inner-relation tuples whose hash matches an outer
/// MCV are routed here instead of the main hashtable. In-memory.
pub struct HashSkewBucket {
    pub hashvalue: u32, // common hash value
    pub tuples: Option<HashJoinTuple>, // linked list of inner-relation tuples
}

pub fn SKEW_BUCKET_OVERHEAD() -> usize {
    MAXALIGN(core::mem::size_of::<HashSkewBucket>())
}
pub const INVALID_SKEW_BUCKET_NO: i32 = -1;
pub const SKEW_HASH_MEM_PERCENT: i32 = 2;
pub const SKEW_MIN_OUTER_FRACTION: f64 = 0.01;

/// Tuple buffer chunk: HashJoinTuples for a batch are packed into 32kB chunks
/// instead of pallocing each tuple. In-memory; tuple buffer follows the header
/// at HASH_CHUNK_HEADER_SIZE.
pub struct HashMemoryChunkData {
    pub ntuples: i32, // number of tuples stored in this chunk
    pub maxlen: usize, // size of the chunk's tuple buffer
    pub used: usize, // buffer bytes already used
    /// pointer to the next chunk (linked list); shared variant collapses
    pub next: Option<Box<Self>>,
    // Tuple buffer starts at offset HASH_CHUNK_HEADER_SIZE (maxaligned).
}

pub type HashMemoryChunk = Box<HashMemoryChunkData>;

pub const HASH_CHUNK_SIZE: Size = 32 * 1024;
pub fn HASH_CHUNK_HEADER_SIZE() -> usize {
    MAXALIGN(core::mem::size_of::<HashMemoryChunkData>())
}
/// tuples exceeding this many bytes are put in their own chunk
pub const HASH_CHUNK_THRESHOLD: Size = HASH_CHUNK_SIZE / 4;

/// Per-batch shared state for Parallel Hash Join. In shmem in C; owned heap
/// state here (single-process). Variable-sized SharedTuplestores follow it.
pub struct ParallelHashJoinBatch {
    // dsa_pointer buckets -> owned bucket storage (DSA tombstoned)
    pub batch_barrier: ParallelBarrier, // synchronization for joining this batch
    // dsa_pointer chunks -> owned chunk storage
    pub size: usize, // size of buckets + chunks in memory
    pub estimated_size: usize, // size of buckets + chunks while writing
    pub ntuples: usize, // number of tuples loaded
    pub old_ntuples: usize, // number of tuples before repartitioning
    pub space_exhausted: bool,
    pub skip_unmatched: bool, // whether to abandon unmatched scan
}

/// Phase barrier among cooperating participants (PG storage/barrier.h Barrier).
/// Under the single-process async model this maps to tokio synchronization;
/// placeholder for the skeleton.
pub struct ParallelBarrier {
    _lock: Mutex<()>,
}

/// Total size of a ParallelHashJoinBatch plus its two tuplestores.
pub fn EstimateParallelHashJoinBatch(nparticipants: i32) -> usize {
    MAXALIGN(core::mem::size_of::<ParallelHashJoinBatch>())
        + MAXALIGN(sts_estimate(nparticipants)) * 2
}

/// Per-backend state to interact with each ParallelHashJoinBatch. In-memory.
pub struct ParallelHashJoinBatchAccessor {
    pub shared: Option<Box<ParallelHashJoinBatch>>, // pointer to shared state
    /* Per-backend partial counters to reduce contention. */
    pub preallocated: usize,
    pub ntuples: usize,
    pub size: usize,
    pub estimated_size: usize,
    pub old_ntuples: usize,
    pub at_least_one_chunk: bool,
    pub outer_eof: bool,
    pub done: bool,
    pub inner_tuples: Option<Box<SharedTuplestoreAccessor>>,
    pub outer_tuples: Option<Box<SharedTuplestoreAccessor>>,
}

/// Growth directive set by a participant while hashing the inner relation.
pub enum ParallelHashGrowth {
    /// current dimensions are sufficient
    OK,
    /// load factor too high; add buckets
    NEED_MORE_BUCKETS,
    /// memory budget exhausted; repartition
    NEED_MORE_BATCHES,
    /// repartitioning didn't help; don't retry
    DISABLED,
}

/// Shared coordination state for a Parallel Hash Join (DSM in C; owned here).
pub struct ParallelHashJoinState {
    // dsa_pointer batches / old_batches -> owned arrays (DSA tombstoned)
    pub nbatch: i32, // number of batches now
    pub old_nbatch: i32, // previous number of batches
    pub nbuckets: i32, // number of buckets
    pub growth: ParallelHashGrowth, // control batch/bucket growth
    // dsa_pointer chunk_work_queue -> owned work queue
    pub nparticipants: i32,
    pub space_allowed: usize,
    pub total_tuples: usize, // total number of inner tuples
    pub lock: Mutex<()>, // protects the above (C: LWLock)

    pub build_barrier: ParallelBarrier, // synchronization for the build phases
    pub grow_batches_barrier: ParallelBarrier,
    pub grow_buckets_barrier: ParallelBarrier,
    pub distributor: AtomicU32, // counter for load balancing

    pub fileset: SharedFileSet, // space for shared temporary files
}

/* The phases for building batches, used by build_barrier. */
pub const PHJ_BUILD_ELECT: i32 = 0;
pub const PHJ_BUILD_ALLOCATE: i32 = 1;
pub const PHJ_BUILD_HASH_INNER: i32 = 2;
pub const PHJ_BUILD_HASH_OUTER: i32 = 3;
pub const PHJ_BUILD_RUN: i32 = 4;
pub const PHJ_BUILD_FREE: i32 = 5;

/* The phases for probing each batch, used by batch_barrier. */
pub const PHJ_BATCH_ELECT: i32 = 0;
pub const PHJ_BATCH_ALLOCATE: i32 = 1;
pub const PHJ_BATCH_LOAD: i32 = 2;
pub const PHJ_BATCH_PROBE: i32 = 3;
pub const PHJ_BATCH_SCAN: i32 = 4;
pub const PHJ_BATCH_FREE: i32 = 5;

/* The phases of batch growth while hashing, for grow_batches_barrier. */
pub const PHJ_GROW_BATCHES_ELECT: i32 = 0;
pub const PHJ_GROW_BATCHES_REALLOCATE: i32 = 1;
pub const PHJ_GROW_BATCHES_REPARTITION: i32 = 2;
pub const PHJ_GROW_BATCHES_DECIDE: i32 = 3;
pub const PHJ_GROW_BATCHES_FINISH: i32 = 4;
pub const fn PHJ_GROW_BATCHES_PHASE(n: i32) -> i32 {
    n % 5 // circular phases
}

/* The phases of bucket growth while hashing, for grow_buckets_barrier. */
pub const PHJ_GROW_BUCKETS_ELECT: i32 = 0;
pub const PHJ_GROW_BUCKETS_REALLOCATE: i32 = 1;
pub const PHJ_GROW_BUCKETS_REINSERT: i32 = 2;
pub const fn PHJ_GROW_BUCKETS_PHASE(n: i32) -> i32 {
    n % 3 // circular phases
}

/// The in-memory hash-join hash table. Resolves the `HashJoinTable` opaque type
/// forward-declared in execnodes.h. In-memory (per-query context in C).
pub struct HashJoinTableData {
    pub nbuckets: i32, // # buckets in the in-memory hash table
    pub log2_nbuckets: i32, // its log2 (nbuckets must be a power of 2)

    pub nbuckets_original: i32, // # buckets when starting the first hash
    pub nbuckets_optimal: i32, // optimal # buckets (per batch)
    pub log2_nbuckets_optimal: i32,

    /// buckets[i] is head of the list of tuples in the i'th in-memory bucket.
    /// C union (unshared per-batch array / shared DSA array) collapses to the
    /// unshared variant under the single-process model.
    pub buckets: Vec<Option<HashJoinTuple>>,

    pub skewEnabled: bool, // are we using skew optimization?
    pub skewBucket: Vec<Option<Box<HashSkewBucket>>>, // hashtable of skew buckets
    pub skewBucketLen: i32, // size of skewBucket array (a power of 2!)
    pub nSkewBuckets: i32, // number of active skew buckets
    pub skewBucketNums: Vec<i32>, // array indexes of active skew buckets

    pub nbatch: i32, // number of batches
    pub curbatch: i32, // current batch #; 0 during 1st pass

    pub nbatch_original: i32, // nbatch when we started inner scan
    pub nbatch_outstart: i32, // nbatch when we started outer scan

    pub growEnabled: bool, // flag to shut off nbatch increases

    pub totalTuples: f64, // # tuples obtained from inner plan
    pub partialTuples: f64, // # tuples obtained from inner plan by me
    pub skewTuples: f64, // # tuples inserted into skew tuples

    /// Per-batch buffered temp files; only allocated when nbatch > 1. A file is
    /// opened on first write (else None). Element 0 is unused.
    pub innerBatchFile: Vec<Option<Box<BufFile>>>,
    pub outerBatchFile: Vec<Option<Box<BufFile>>>,

    pub spaceUsed: Size, // memory space currently used by tuples
    pub spaceAllowed: Size, // upper limit for space used
    pub spacePeak: Size, // peak space used
    pub spaceUsedSkew: Size, // skew hash table's current space usage
    pub spaceAllowedSkew: Size, // upper limit for skew hashtable

    // MemoryContexts (hashCxt/batchCxt/spillCxt) map to arena/Box scopes under
    // the Rust memory model; dropped from the skeleton.

    /// used for dense allocation of tuples (into linked chunks)
    pub chunks: Option<HashMemoryChunk>, // one list for the whole batch

    /* Shared and private state for Parallel Hash. */
    pub current_chunk: Option<HashMemoryChunk>, // this backend's current chunk
    // dsa_area *area -> owned allocation (DSA tombstoned)
    pub parallel_state: Option<Box<ParallelHashJoinState>>,
    pub batches: Vec<ParallelHashJoinBatchAccessor>,
    // dsa_pointer current_chunk_shared -> owned (DSA tombstoned)
}

pub type HashJoinTable = Box<HashJoinTableData>;

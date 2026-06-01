//! gininsert.c
//!   insert routines for the postgres inverted index access method.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/gin/gininsert.c

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData};
use crate::access::gin::gin::{
    GinStatsData, ginUpdateStats, GIN_COMPARE_PROC, PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN,
    PROGRESS_GIN_PHASE_MERGE_1, PROGRESS_GIN_PHASE_MERGE_2, PROGRESS_GIN_PHASE_PERFORMSORT_1,
    PROGRESS_GIN_PHASE_PERFORMSORT_2,
};
use crate::access::gin::gin_private::{
    createPostingTree, freeGinBtreeStack, ginBeginBAScan, ginCompressPostingList,
    ginExtractEntries, ginFindLeafPage, ginGetBAEntry, ginHeapTupleFastCollect,
    ginHeapTupleFastInsert, ginInitBA, ginInsertBAEntries, ginInsertItemPointers, ginInsertValue,
    ginMergeItemPointers, ginPostingListDecodeAllSegments, ginPrepareEntryScan, ginReadTuple,
    gintuple_get_attrnum, gintuple_get_key, initGinState, BuildAccumulator, GinBtreeData,
    GinBtreeEntryInsertData, GinBtreeStack, GinFormTuple, GinNewBuffer, GinInitBuffer,
    GinInitMetabuffer, GinState, GinTupleCollector, GIN_LEAF as GIN_LEAF_FLAG, GIN_UNLOCK,
};
use crate::access::gin::gin_tuple::{GinTuple, GinTupleGetFirst};
use crate::access::gin::ginblock::{
    GinIsPostingTree, GinGetPostingTree, GinMaxItemSize, GinPostingList, GinSetPostingTree,
    SizeOfGinPostingList, GIN_CAT_NORM_KEY, GIN_LEAF,
};
use crate::access::index::amapi::{IndexBuildResult, IndexUniqueCheck};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::lib::ilist::{dlist_delete, dlist_head, dlist_init, dlist_mutable_iter, dlist_node,
    dlist_push_tail};
use crate::nodes::execnodes::IndexInfo;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, PageGetItem, PageGetItemId};
use crate::storage::itemptr::{ItemPointer, ItemPointerCompare, ItemPointerData};
use crate::storage::off::OffsetNumber;
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::format_type::format_type_be;
use crate::utils::cache::typcache::{lookup_type_cache, TypeCacheEntry, TYPECACHE_CMP_PROC_FINFO};
use crate::utils::rel::{
    RelationGetDescr, RelationGetRelationName, RelationGetRelid, Relation,
};
use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportComparisonShim, SortSupport, SortSupportData,
};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::index::indexam::{index_close, index_getprocid, index_open};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::index::BuildIndexInfo;
use crate::storage::lmgr::predicate::CheckForSerializableConflictIn;
use crate::access::transam::xloginsert::{log_newpage_buffer, log_newpage_range};

use core::ffi::CStr;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
}

// GinNullCategory mirrors access/ginblock.h (signed char). gintuple_get_key and
// related helpers use this width.
pub type GinNullCategory = i8;

// ---------------------------------------------------------------------------
// Magic numbers for parallel state sharing
// ---------------------------------------------------------------------------
pub const PARALLEL_KEY_GIN_SHARED: u64 = 0xB000000000000001;
pub const PARALLEL_KEY_TUPLESORT: u64 = 0xB000000000000002;
pub const PARALLEL_KEY_QUERY_TEXT: u64 = 0xB000000000000003;
pub const PARALLEL_KEY_WAL_USAGE: u64 = 0xB000000000000004;
pub const PARALLEL_KEY_BUFFER_USAGE: u64 = 0xB000000000000005;

// ===========================================================================
// Stubs -- symbols whose real home has not been ported yet.
// ===========================================================================

/// TODO(pg-port): Tuplesortstate (utils/tuplesort.h)
pub enum Tuplesortstate {}

/// TODO(pg-port): SortCoordinateData (utils/sortsupport.h)
#[repr(C)]
pub struct SortCoordinateData {
    pub isWorker: bool,
    pub nParticipants: c_int,
    pub sharedsort: *mut Sharedsort,
}
pub type SortCoordinate = *mut SortCoordinateData;

/// TODO(pg-port): Sharedsort (utils/tuplesort.h)
pub enum Sharedsort {}

/// TODO(pg-port): ParallelContext (access/parallel.h)
#[repr(C)]
pub struct ParallelContext {
    pub nworkers_launched: c_int,
    pub nworkers: c_int,
    pub seg: *mut c_void,
    pub toc: *mut c_void,
    pub estimator: shm_toc_estimator,
}

/// TODO(pg-port): shm_toc_estimator (storage/shm_toc.h)
#[repr(C)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/// TODO(pg-port): ConditionVariable (storage/condition_variable.h)
pub type ConditionVariable = c_int;

/// TODO(pg-port): slock_t (storage/spin.h)
pub type slock_t = c_int;

/// TODO(pg-port): WalUsage (access/xlog.h)
pub type WalUsage = c_void;

/// TODO(pg-port): BufferUsage (executor/instrument.h)
pub type BufferUsage = c_void;

/// TODO(pg-port): Snapshot (utils/snapshot.h)
pub type Snapshot = *mut c_void;

/// TODO(pg-port): LOCKMODE (storage/lock.h)
pub type LOCKMODE = c_int;

/// TODO(pg-port): dsm_segment (storage/dsm.h)
pub enum dsm_segment {}

/// TODO(pg-port): shm_toc (storage/shm_toc.h)
pub enum shm_toc {}

/// TODO(pg-port): ParallelTableScanDesc (access/relscan.h)
pub type ParallelTableScanDesc = *mut c_void;

/// TODO(pg-port): TableScanDesc (access/relscan.h)
pub type TableScanDesc = *mut c_void;

/// TODO(pg-port): ProcStruct (storage/proc.h)
#[repr(C)]
pub struct ProcStruct {
    pub statusFlags: u8,
}

/// TODO(pg-port): storage/proc.h -- MyProc
pub static mut MyProc: *mut ProcStruct = core::ptr::null_mut();

/// TODO(pg-port): access/parallel.h -- ParallelWorkerNumber
pub static mut ParallelWorkerNumber: c_int = 0;

/// TODO(pg-port): miscadmin.h -- maintenance_work_mem (canonical: miscadmin)
pub static mut maintenance_work_mem: c_int = 65536;

/// TODO(pg-port): tcop/tcopprot.h -- debug_query_string
pub static mut debug_query_string: *const c_char = core::ptr::null();

/// TODO(pg-port): utils/tuplesort.h -- TUPLESORT_NONE
pub const TUPLESORT_NONE: c_int = 0;

/// TODO(pg-port): commands/progress.h -- PROGRESS_CREATEIDX_SUBPHASE
pub const PROGRESS_CREATEIDX_SUBPHASE: c_int = 0;
/// TODO(pg-port): commands/progress.h -- PROGRESS_CREATEIDX_TUPLES_TOTAL
pub const PROGRESS_CREATEIDX_TUPLES_TOTAL: c_int = 0;
/// TODO(pg-port): commands/progress.h -- PROGRESS_CREATEIDX_TUPLES_DONE
pub const PROGRESS_CREATEIDX_TUPLES_DONE: c_int = 0;
/// TODO(pg-port): commands/progress.h -- PROGRESS_SCAN_BLOCKS_TOTAL
pub const PROGRESS_SCAN_BLOCKS_TOTAL: c_int = 0;
/// TODO(pg-port): commands/progress.h -- PROGRESS_SCAN_BLOCKS_DONE
pub const PROGRESS_SCAN_BLOCKS_DONE: c_int = 0;

/// TODO(pg-port): utils/wait_event_types.h -- WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN
pub const WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN: u32 = 0;

/// TODO(pg-port): storage/proc.h -- PROC_IN_SAFE_IC
pub const PROC_IN_SAFE_IC: u8 = 0x04;

/// TODO(pg-port): pgstat.h -- STATE_RUNNING
pub const STATE_RUNNING: c_int = 1;

/// TODO(pg-port): storage/lock.h lock modes
pub const ShareLock: LOCKMODE = 5;
pub const AccessExclusiveLock: LOCKMODE = 8;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const RowExclusiveLock: LOCKMODE = 3;

/// TODO(pg-port): storage/bufmgr.h -- INIT_FORKNUM / MAIN_FORKNUM (relpath.h)
pub const MAIN_FORKNUM: c_int = 0;
pub const INIT_FORKNUM: c_int = 1;

/// TODO(pg-port): storage/bufmgr.h -- ExtendBufferedRel flags / BMR_REL
pub const EB_LOCK_FIRST: u32 = 1 << 5;
pub const EB_SKIP_EXTENSION_LOCK: u32 = 1 << 0;

/// TODO(pg-port): access/htup_details.h crit-section macros are provided via
/// miscadmin.h elsewhere; declared locally as no-ops here.
pub unsafe fn START_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h -- START_CRIT_SECTION
}
pub unsafe fn END_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h -- END_CRIT_SECTION
}
pub unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): miscadmin.h -- CHECK_FOR_INTERRUPTS
}

/// TODO(pg-port): storage/bufmgr.h -- BufferGetPage
pub unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- BufferGetBlockNumber
pub unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- LockBuffer
pub unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- MarkBufferDirty
pub unsafe fn MarkBufferDirty(buffer: Buffer) {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- UnlockReleaseBuffer
pub unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- ExtendBufferedRel
pub unsafe fn ExtendBufferedRel(
    bmr: Relation,
    forkNum: c_int,
    strategy: *mut c_void,
    flags: u32,
) -> Buffer {
    // TODO(pg-port): storage/bufmgr.h
    unimplemented!()
}
/// TODO(pg-port): storage/bufmgr.h -- BMR_REL
pub unsafe fn BMR_REL(rel: Relation) -> Relation {
    // TODO(pg-port): storage/bufmgr.h
    rel
}
/// TODO(pg-port): utils/rel.h -- RelationGetNumberOfBlocks
pub unsafe fn RelationGetNumberOfBlocks(relation: Relation) -> BlockNumber {
    // TODO(pg-port): storage/bufmgr.h -- RelationGetNumberOfBlocksInFork
    unimplemented!()
}
/// TODO(pg-port): utils/rel.h -- RelationNeedsWAL
pub unsafe fn RelationNeedsWAL(relation: Relation) -> bool {
    // TODO(pg-port): utils/rel.h
    unimplemented!()
}
/// TODO(pg-port): access/relation.h -- IndexRelationGetNumberOfKeyAttributes
pub unsafe fn IndexRelationGetNumberOfKeyAttributes(relation: Relation) -> c_int {
    // TODO(pg-port): utils/rel.h
    unimplemented!()
}
/// TODO(pg-port): utils/rel.h -- rd_indcollation accessor
pub unsafe fn RelationGetIndcollation(relation: Relation, i: c_int) -> Oid {
    // TODO(pg-port): utils/rel.h -- index->rd_indcollation[i]
    unimplemented!()
}

/// TODO(pg-port): utils/cache/lsyscache.h home of DEFAULT_COLLATION_OID
pub const DEFAULT_COLLATION_OID: Oid = 100;

// --- tuplesort.h ---
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_begin_index_gin
pub unsafe fn tuplesort_begin_index_gin(
    heapRel: Relation,
    indexRel: Relation,
    workMem: c_int,
    coordinate: SortCoordinate,
    sortopt: c_int,
) -> *mut Tuplesortstate {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_performsort
pub unsafe fn tuplesort_performsort(state: *mut Tuplesortstate) {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_end
pub unsafe fn tuplesort_end(state: *mut Tuplesortstate) {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_putgintuple
pub unsafe fn tuplesort_putgintuple(state: *mut Tuplesortstate, tuple: *mut GinTuple, size: Size) {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_getgintuple
pub unsafe fn tuplesort_getgintuple(
    state: *mut Tuplesortstate,
    len: *mut Size,
    forward: bool,
) -> *mut GinTuple {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_estimate_shared
pub unsafe fn tuplesort_estimate_shared(nworkers: c_int) -> Size {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_initialize_shared
pub unsafe fn tuplesort_initialize_shared(
    shared: *mut Sharedsort,
    nWorkers: c_int,
    seg: *mut dsm_segment,
) {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_attach_shared
pub unsafe fn tuplesort_attach_shared(shared: *mut Sharedsort, seg: *mut dsm_segment) {
    // TODO(pg-port): utils/tuplesort.h
    unimplemented!()
}

// --- table.h / tableam.h ---
/// TODO(pg-port): access/tableam.h -- table_index_build_scan
pub unsafe fn table_index_build_scan(
    table_rel: Relation,
    index_rel: Relation,
    index_info: *mut IndexInfo,
    allow_sync: bool,
    progress: bool,
    callback: IndexBuildCallback,
    callback_state: *mut c_void,
    scan: TableScanDesc,
) -> f64 {
    // TODO(pg-port): access/tableam.h
    unimplemented!()
}
/// TODO(pg-port): access/tableam.h -- IndexBuildCallback typedef
pub type IndexBuildCallback = unsafe extern "C" fn(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    tupleIsAlive: bool,
    state: *mut c_void,
);
/// TODO(pg-port): access/tableam.h -- table_parallelscan_estimate
pub unsafe fn table_parallelscan_estimate(rel: Relation, snapshot: Snapshot) -> Size {
    // TODO(pg-port): access/tableam.h
    unimplemented!()
}
/// TODO(pg-port): access/tableam.h -- table_parallelscan_initialize
pub unsafe fn table_parallelscan_initialize(
    rel: Relation,
    pscan: ParallelTableScanDesc,
    snapshot: Snapshot,
) {
    // TODO(pg-port): access/tableam.h
    unimplemented!()
}
/// TODO(pg-port): access/tableam.h -- table_beginscan_parallel
pub unsafe fn table_beginscan_parallel(
    relation: Relation,
    pscan: ParallelTableScanDesc,
) -> TableScanDesc {
    // TODO(pg-port): access/tableam.h
    unimplemented!()
}

// --- parallel.h ---
/// TODO(pg-port): access/parallel.h -- EnterParallelMode
pub unsafe fn EnterParallelMode() {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- ExitParallelMode
pub unsafe fn ExitParallelMode() {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- CreateParallelContext
pub unsafe fn CreateParallelContext(
    library_name: *const c_char,
    function_name: *const c_char,
    nworkers: c_int,
) -> *mut ParallelContext {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- InitializeParallelDSM
pub unsafe fn InitializeParallelDSM(pcxt: *mut ParallelContext) {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- LaunchParallelWorkers
pub unsafe fn LaunchParallelWorkers(pcxt: *mut ParallelContext) {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- WaitForParallelWorkersToAttach
pub unsafe fn WaitForParallelWorkersToAttach(pcxt: *mut ParallelContext) {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- WaitForParallelWorkersToFinish
pub unsafe fn WaitForParallelWorkersToFinish(pcxt: *mut ParallelContext) {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}
/// TODO(pg-port): access/parallel.h -- DestroyParallelContext
pub unsafe fn DestroyParallelContext(pcxt: *mut ParallelContext) {
    // TODO(pg-port): access/parallel.h
    unimplemented!()
}

// --- shm_toc.h ---
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_estimate_chunk
pub unsafe fn shm_toc_estimate_chunk(e: *mut shm_toc_estimator, sz: Size) {
    // TODO(pg-port): storage/shm_toc.h
    unimplemented!()
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_estimate_keys
pub unsafe fn shm_toc_estimate_keys(e: *mut shm_toc_estimator, cnt: Size) {
    // TODO(pg-port): storage/shm_toc.h
    unimplemented!()
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_allocate
pub unsafe fn shm_toc_allocate(toc: *mut c_void, nbytes: Size) -> *mut c_void {
    // TODO(pg-port): storage/shm_toc.h
    unimplemented!()
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_insert
pub unsafe fn shm_toc_insert(toc: *mut c_void, key: u64, address: *mut c_void) {
    // TODO(pg-port): storage/shm_toc.h
    unimplemented!()
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_lookup
pub unsafe fn shm_toc_lookup(toc: *mut shm_toc, key: u64, noError: bool) -> *mut c_void {
    // TODO(pg-port): storage/shm_toc.h
    unimplemented!()
}

// --- snapshot ---
/// TODO(pg-port): access/parallel.h -- SnapshotAny
pub static mut SnapshotAny: Snapshot = core::ptr::null_mut();
/// TODO(pg-port): utils/snapmgr.h -- GetTransactionSnapshot
pub unsafe fn GetTransactionSnapshot() -> Snapshot {
    // TODO(pg-port): utils/snapmgr.h
    unimplemented!()
}
/// TODO(pg-port): utils/snapmgr.h -- RegisterSnapshot
pub unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    // TODO(pg-port): utils/snapmgr.h
    unimplemented!()
}
/// TODO(pg-port): utils/snapmgr.h -- UnregisterSnapshot
pub unsafe fn UnregisterSnapshot(snapshot: Snapshot) {
    // TODO(pg-port): utils/snapmgr.h
    unimplemented!()
}
/// TODO(pg-port): utils/snapshot.h -- IsMVCCSnapshot
pub unsafe fn IsMVCCSnapshot(snapshot: Snapshot) -> bool {
    // TODO(pg-port): utils/snapshot.h
    unimplemented!()
}

// --- spin.h ---
/// TODO(pg-port): storage/spin.h -- SpinLockInit
pub unsafe fn SpinLockInit(lock: *mut slock_t) {
    // TODO(pg-port): storage/spin.h
    unimplemented!()
}
/// TODO(pg-port): storage/spin.h -- SpinLockAcquire
pub unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    // TODO(pg-port): storage/spin.h
    unimplemented!()
}
/// TODO(pg-port): storage/spin.h -- SpinLockRelease
pub unsafe fn SpinLockRelease(lock: *mut slock_t) {
    // TODO(pg-port): storage/spin.h
    unimplemented!()
}

// --- condition_variable.h ---
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableInit
pub unsafe fn ConditionVariableInit(cv: *mut ConditionVariable) {
    // TODO(pg-port): storage/condition_variable.h
    unimplemented!()
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableSleep
pub unsafe fn ConditionVariableSleep(cv: *mut ConditionVariable, wait_event_info: u32) {
    // TODO(pg-port): storage/condition_variable.h
    unimplemented!()
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableCancelSleep
pub unsafe fn ConditionVariableCancelSleep() -> bool {
    // TODO(pg-port): storage/condition_variable.h
    unimplemented!()
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableSignal
pub unsafe fn ConditionVariableSignal(cv: *mut ConditionVariable) {
    // TODO(pg-port): storage/condition_variable.h
    unimplemented!()
}

// --- pgstat.h ---
/// TODO(pg-port): pgstat.h -- pgstat_progress_update_param
pub unsafe fn pgstat_progress_update_param(index: c_int, val: i64) {
    // TODO(pg-port): pgstat.h
    unimplemented!()
}
/// TODO(pg-port): pgstat.h -- pgstat_progress_update_multi_param
pub unsafe fn pgstat_progress_update_multi_param(
    nparam: c_int,
    index: *const c_int,
    val: *const i64,
) {
    // TODO(pg-port): pgstat.h
    unimplemented!()
}
/// TODO(pg-port): pgstat.h -- pgstat_report_activity
pub unsafe fn pgstat_report_activity(state: c_int, cmd_str: *const c_char) {
    // TODO(pg-port): pgstat.h
    unimplemented!()
}

// --- instrument.h ---
/// TODO(pg-port): executor/instrument.h -- InstrStartParallelQuery
pub unsafe fn InstrStartParallelQuery() {
    // TODO(pg-port): executor/instrument.h
    unimplemented!()
}
/// TODO(pg-port): executor/instrument.h -- InstrEndParallelQuery
pub unsafe fn InstrEndParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {
    // TODO(pg-port): executor/instrument.h
    unimplemented!()
}
/// TODO(pg-port): executor/instrument.h -- InstrAccumParallelQuery
pub unsafe fn InstrAccumParallelQuery(bufusage: *mut BufferUsage, walusage: *mut WalUsage) {
    // TODO(pg-port): executor/instrument.h
    unimplemented!()
}

// --- c.h size helpers ---
/// TODO(pg-port): c.h -- add_size
pub fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}
/// TODO(pg-port): c.h -- mul_size
pub fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2
}

// ===========================================================================
// Status for index builds performed in parallel.  This is allocated in a
// dynamic shared memory segment.
// ===========================================================================
#[repr(C)]
pub struct GinBuildShared {
    /*
     * These fields are not modified during the build.  They primarily exist
     * for the benefit of worker processes that need to create state
     * corresponding to that used by the leader.
     */
    pub heaprelid: Oid,
    pub indexrelid: Oid,
    pub isconcurrent: bool,
    pub scantuplesortstates: c_int,

    /*
     * workersdonecv is used to monitor the progress of workers.  All parallel
     * participants must indicate that they are done before leader can use
     * results built by the workers (and before leader can write the data into
     * the index).
     */
    pub workersdonecv: ConditionVariable,

    /*
     * mutex protects all following fields
     *
     * These fields contain status information of interest to GIN index builds
     * that must work just the same when an index is built in parallel.
     */
    pub mutex: slock_t,

    /*
     * Mutable state that is maintained by workers, and reported back to
     * leader at end of the scans.
     *
     * nparticipantsdone is number of worker processes finished.
     *
     * reltuples is the total number of input heap tuples.
     *
     * indtuples is the total number of tuples that made it into the index.
     */
    pub nparticipantsdone: c_int,
    pub reltuples: f64,
    pub indtuples: f64,
    /*
     * ParallelTableScanDescData data follows. Can't directly embed here, as
     * implementations of the parallel table scan desc interface might need
     * stronger alignment.
     */
}

/*
 * Return pointer to a GinBuildShared's parallel table scan.
 *
 * c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just
 * MAXALIGN.
 */
#[inline]
pub unsafe fn ParallelTableScanFromGinBuildShared(
    shared: *mut GinBuildShared,
) -> ParallelTableScanDesc {
    (shared as *mut c_char).add(BUFFERALIGN(core::mem::size_of::<GinBuildShared>())) as ParallelTableScanDesc
}

/*
 * Status for leader in parallel index build.
 */
#[repr(C)]
pub struct GinLeader {
    /* parallel context itself */
    pub pcxt: *mut ParallelContext,

    /*
     * nparticipanttuplesorts is the exact number of worker processes
     * successfully launched, plus one leader process if it participates as a
     * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
     * participating as a worker).
     */
    pub nparticipanttuplesorts: c_int,

    /*
     * Leader process convenience pointers to shared state (leader avoids TOC
     * lookups).
     *
     * GinBuildShared is the shared state for entire build.  sharedsort is the
     * shared, tuplesort-managed state passed to each process tuplesort.
     * snapshot is the snapshot used by the scan iff an MVCC snapshot is
     * required.
     */
    pub ginshared: *mut GinBuildShared,
    pub sharedsort: *mut Sharedsort,
    pub snapshot: Snapshot,
    pub walusage: *mut WalUsage,
    pub bufferusage: *mut BufferUsage,
}

#[repr(C)]
pub struct GinBuildState {
    pub ginstate: GinState,
    pub indtuples: f64,
    pub buildStats: GinStatsData,
    pub tmpCtx: MemoryContext,
    pub funcCtx: MemoryContext,
    pub accum: BuildAccumulator,
    pub tid: ItemPointerData,
    pub work_mem: c_int,

    /*
     * bs_leader is only present when a parallel index build is performed, and
     * only in the leader process.
     */
    pub bs_leader: *mut GinLeader,

    /* number of participating workers (including leader) */
    pub bs_num_workers: c_int,

    /* used to pass information from workers to leader */
    pub bs_numtuples: f64,
    pub bs_reltuples: f64,

    /*
     * The sortstate is used by workers (including the leader). It has to be
     * part of the build state, because that's the only thing passed to the
     * build callback etc.
     */
    pub bs_sortstate: *mut Tuplesortstate,

    /*
     * The sortstate used only within a single worker for the first merge pass
     * happening there. In principle it doesn't need to be part of the build
     * state and we could pass it around directly, but it's more convenient
     * this way. And it's part of the build state, after all.
     */
    pub bs_worker_sort: *mut Tuplesortstate,
}

/*
 * Adds array of item pointers to tuple's posting list, or
 * creates posting tree and tuple pointing to tree in case
 * of not enough space.  Max size of tuple is defined in
 * GinFormTuple().  Returns a new, modified index tuple.
 * items[] must be in sorted order with no duplicates.
 */
unsafe fn addItemPointersToLeafTuple(
    ginstate: *mut GinState,
    old: IndexTuple,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
    buffer: Buffer,
) -> IndexTuple {
    let attnum: OffsetNumber;
    let key: Datum;
    let mut category: GinNullCategory = 0;
    let mut res: IndexTuple;
    let newItems: *mut ItemPointerData;
    let oldItems: *mut ItemPointerData;
    let mut oldNPosting: c_int = 0;
    let mut newNPosting: c_int = 0;
    let compressedList: *mut GinPostingList;

    Assert!(!GinIsPostingTree(&(*old).t_tid));

    attnum = gintuple_get_attrnum(ginstate, old);
    key = gintuple_get_key(ginstate, old, &mut category);

    /* merge the old and new posting lists */
    oldItems = ginReadTuple(ginstate, attnum, old, &mut oldNPosting);

    newItems = ginMergeItemPointers(items, nitem, oldItems, oldNPosting as uint32,
                                    &mut newNPosting);

    /* Compress the posting list, and try to a build tuple with room for it */
    res = null_mut();
    compressedList = ginCompressPostingList(newItems, newNPosting, GinMaxItemSize() as c_int,
                                            null_mut());
    pfree(newItems as *mut c_void);
    if !compressedList.is_null() {
        res = GinFormTuple(ginstate, attnum, key, category,
                           compressedList as *mut c_char,
                           SizeOfGinPostingList(compressedList),
                           newNPosting,
                           false);
        pfree(compressedList as *mut c_void);
    }
    if res.is_null() {
        /* posting list would be too big, convert to posting tree */
        let postingRoot: BlockNumber;

        /*
         * Initialize posting tree with the old tuple's posting list.  It's
         * surely small enough to fit on one posting-tree page, and should
         * already be in order with no duplicates.
         */
        postingRoot = createPostingTree((*ginstate).index,
                                        oldItems,
                                        oldNPosting as uint32,
                                        buildStats,
                                        buffer);

        /* Now insert the TIDs-to-be-added into the posting tree */
        ginInsertItemPointers((*ginstate).index, postingRoot,
                              items, nitem,
                              buildStats);

        /* And build a new posting-tree-only result tuple */
        res = GinFormTuple(ginstate, attnum, key, category, null_mut(), 0, 0, true);
        GinSetPostingTree(&mut (*res).t_tid, postingRoot);
    }
    pfree(oldItems as *mut c_void);

    return res;
}

/*
 * Build a fresh leaf tuple, either posting-list or posting-tree format
 * depending on whether the given items list will fit.
 * items[] must be in sorted order with no duplicates.
 *
 * This is basically the same logic as in addItemPointersToLeafTuple,
 * but working from slightly different input.
 */
unsafe fn buildFreshLeafTuple(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
    buffer: Buffer,
) -> IndexTuple {
    let mut res: IndexTuple = null_mut();
    let compressedList: *mut GinPostingList;

    /* try to build a posting list tuple with all the items */
    compressedList = ginCompressPostingList(items, nitem as c_int, GinMaxItemSize() as c_int, null_mut());
    if !compressedList.is_null() {
        res = GinFormTuple(ginstate, attnum, key, category,
                           compressedList as *mut c_char,
                           SizeOfGinPostingList(compressedList),
                           nitem as c_int, false);
        pfree(compressedList as *mut c_void);
    }
    if res.is_null() {
        /* posting list would be too big, build posting tree */
        let postingRoot: BlockNumber;

        /*
         * Build posting-tree-only result tuple.  We do this first so as to
         * fail quickly if the key is too big.
         */
        res = GinFormTuple(ginstate, attnum, key, category, null_mut(), 0, 0, true);

        /*
         * Initialize a new posting tree with the TIDs.
         */
        postingRoot = createPostingTree((*ginstate).index, items, nitem,
                                        buildStats, buffer);

        /* And save the root link in the result tuple */
        GinSetPostingTree(&mut (*res).t_tid, postingRoot);
    }

    return res;
}

/*
 * Insert one or more heap TIDs associated with the given key value.
 * This will either add a single key entry, or enlarge a pre-existing entry.
 *
 * During an index build, buildStats is non-null and the counters
 * it contains should be incremented as needed.
 */
pub unsafe fn ginEntryInsert(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
) {
    let mut btree: GinBtreeData = core::mem::zeroed();
    let mut insertdata: GinBtreeEntryInsertData = core::mem::zeroed();
    let stack: *mut GinBtreeStack;
    let mut itup: IndexTuple;
    let page: Page;

    insertdata.isDelete = false;

    ginPrepareEntryScan(&mut btree, attnum, key, category, ginstate);
    btree.isBuild = !buildStats.is_null();

    stack = ginFindLeafPage(&mut btree, false, false);
    page = BufferGetPage((*stack).buffer);

    if (btree.findItem.expect("findItem"))(&mut btree, stack) {
        /* found pre-existing entry */
        itup = PageGetItem(page, PageGetItemId(page, (*stack).off)) as IndexTuple;

        if GinIsPostingTree(&(*itup).t_tid) {
            /* add entries to existing posting tree */
            let rootPostingTree: BlockNumber = GinGetPostingTree(&(*itup).t_tid);

            /* release all stack */
            LockBuffer((*stack).buffer, GIN_UNLOCK);
            freeGinBtreeStack(stack);

            /* insert into posting tree */
            ginInsertItemPointers((*ginstate).index, rootPostingTree,
                                  items, nitem,
                                  buildStats);
            return;
        }

        CheckForSerializableConflictIn((*ginstate).index, null_mut(),
                                       BufferGetBlockNumber((*stack).buffer));
        /* modify an existing leaf entry */
        itup = addItemPointersToLeafTuple(ginstate, itup,
                                          items, nitem, buildStats, (*stack).buffer);

        insertdata.isDelete = true;
    } else {
        CheckForSerializableConflictIn((*ginstate).index, null_mut(),
                                       BufferGetBlockNumber((*stack).buffer));
        /* no match, so construct a new leaf entry */
        itup = buildFreshLeafTuple(ginstate, attnum, key, category,
                                   items, nitem, buildStats, (*stack).buffer);

        /*
         * nEntries counts leaf tuples, so increment it only when we make a
         * new one.
         */
        if !buildStats.is_null() {
            (*buildStats).nEntries += 1;
        }
    }

    /* Insert the new or modified leaf tuple */
    insertdata.entry = itup;
    ginInsertValue(&mut btree, stack, &mut insertdata as *mut GinBtreeEntryInsertData as *mut c_void,
                   buildStats);
    pfree(itup as *mut c_void);
}

/*
 * Extract index entries for a single indexable item, and add them to the
 * BuildAccumulator's state.
 *
 * This function is used only during initial index creation.
 */
unsafe fn ginHeapTupleBulkInsert(
    buildstate: *mut GinBuildState,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    heapptr: ItemPointer,
) {
    let entries: *mut Datum;
    let mut categories: *mut GinNullCategory = null_mut();
    let mut nentries: int32 = 0;
    let oldCtx: MemoryContext;

    oldCtx = MemoryContextSwitchTo((*buildstate).funcCtx);
    entries = ginExtractEntries((*buildstate).accum.ginstate, attnum,
                                value, isNull,
                                &mut nentries, &mut categories);
    MemoryContextSwitchTo(oldCtx);

    ginInsertBAEntries(&mut (*buildstate).accum, heapptr, attnum,
                       entries, categories, nentries);

    (*buildstate).indtuples += nentries as f64;

    MemoryContextReset((*buildstate).funcCtx);
}

unsafe extern "C" fn ginBuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate: *mut GinBuildState = state as *mut GinBuildState;
    let oldCtx: MemoryContext;
    let mut i: c_int;

    oldCtx = MemoryContextSwitchTo((*buildstate).tmpCtx);

    i = 0;
    while i < (*(*buildstate).ginstate.origTupdesc).natts {
        ginHeapTupleBulkInsert(buildstate, (i + 1) as OffsetNumber,
                               *values.add(i as usize), *isnull.add(i as usize), tid);
        i += 1;
    }

    /* If we've maxed out our available memory, dump everything to the index */
    if (*buildstate).accum.allocatedMemory >= maintenance_work_mem as Size * 1024 {
        let mut list: *mut ItemPointerData;
        let mut key: Datum = 0;
        let mut category: GinNullCategory = 0;
        let mut nlist: uint32 = 0;
        let mut attnum: OffsetNumber = 0;

        ginBeginBAScan(&mut (*buildstate).accum);
        loop {
            list = ginGetBAEntry(&mut (*buildstate).accum,
                                 &mut attnum, &mut key, &mut category, &mut nlist);
            if list.is_null() {
                break;
            }
            /* there could be many entries, so be willing to abort here */
            CHECK_FOR_INTERRUPTS();
            ginEntryInsert(&mut (*buildstate).ginstate, attnum, key, category,
                           list, nlist, &mut (*buildstate).buildStats);
        }

        MemoryContextReset((*buildstate).tmpCtx);
        ginInitBA(&mut (*buildstate).accum);
    }

    MemoryContextSwitchTo(oldCtx);
}

/*
 * ginFlushBuildState
 *		Write all data from BuildAccumulator into the tuplesort.
 *
 * The number of TIDs written to the tuplesort at once is limited, to reduce
 * the amount of memory needed when merging the intermediate results later.
 * The leader will see up to two chunks per worker, so calculate the limit to
 * not need more than MaxAllocSize overall.
 *
 * We don't need to worry about overflowing maintenance_work_mem. We can't
 * build chunks larger than work_mem, and that limit was set so that workers
 * produce sufficiently small chunks.
 */
unsafe fn ginFlushBuildState(buildstate: *mut GinBuildState, index: Relation) {
    let mut list: *mut ItemPointerData;
    let mut key: Datum = 0;
    let mut category: GinNullCategory = 0;
    let mut nlist: uint32 = 0;
    let mut attnum: OffsetNumber = 0;
    let tdesc: TupleDesc = RelationGetDescr(index);
    let mut maxlen: uint32;

    /* maximum number of TIDs per chunk (two chunks per worker) */
    maxlen = (MaxAllocSize / core::mem::size_of::<ItemPointerData>()) as uint32;
    maxlen /= (2 * (*buildstate).bs_num_workers) as uint32;

    ginBeginBAScan(&mut (*buildstate).accum);
    loop {
        list = ginGetBAEntry(&mut (*buildstate).accum,
                             &mut attnum, &mut key, &mut category, &mut nlist);
        if list.is_null() {
            break;
        }
        /* information about the key */
        let attr: Form_pg_attribute = TupleDescAttr(tdesc, (attnum - 1) as c_int);

        /* start of the chunk */
        let mut offset: uint32 = 0;

        /* split the entry into smaller chunk with up to maxlen items */
        while offset < nlist {
            /* GIN tuple and tuple length */
            let tup: *mut GinTuple;
            let mut tuplen: Size = 0;
            let len: uint32 = Min(maxlen, nlist - offset);

            /* there could be many entries, so be willing to abort here */
            CHECK_FOR_INTERRUPTS();

            tup = _gin_build_tuple(attnum, category,
                                   key, (*attr).attlen, (*attr).attbyval,
                                   list.add(offset as usize), len,
                                   &mut tuplen);

            offset += len;

            tuplesort_putgintuple((*buildstate).bs_worker_sort, tup, tuplen);

            pfree(tup as *mut c_void);
        }
    }

    MemoryContextReset((*buildstate).tmpCtx);
    ginInitBA(&mut (*buildstate).accum);
}

/*
 * ginBuildCallbackParallel
 *		Callback for the parallel index build.
 *
 * This is similar to the serial build callback ginBuildCallback, but
 * instead of writing the accumulated entries into the index, each worker
 * writes them into a (local) tuplesort.
 *
 * The worker then sorts and combines these entries, before writing them
 * into a shared tuplesort for the leader (see _gin_parallel_scan_and_build
 * for the whole process).
 */
unsafe extern "C" fn ginBuildCallbackParallel(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate: *mut GinBuildState = state as *mut GinBuildState;
    let oldCtx: MemoryContext;
    let mut i: c_int;

    oldCtx = MemoryContextSwitchTo((*buildstate).tmpCtx);

    /*
     * if scan wrapped around - flush accumulated entries and start anew
     *
     * With parallel scans, we don't have a guarantee the scan does not start
     * half-way through the relation (serial builds disable sync scans and
     * always start from block 0, parallel scans require allow_sync=true).
     *
     * Building the posting lists assumes the TIDs are monotonic and never go
     * back, and the wrap around would break that. We handle that by detecting
     * the wraparound, and flushing all entries. This means we'll later see
     * two separate entries with non-overlapping TID lists (which can be
     * combined by merge sort).
     *
     * To detect a wraparound, we remember the last TID seen by each worker
     * (for any key). If the next TID seen by the worker is lower, the scan
     * must have wrapped around.
     */
    if ItemPointerCompare(tid, &mut (*buildstate).tid) < 0 {
        ginFlushBuildState(buildstate, index);
    }

    /* remember the TID we're about to process */
    (*buildstate).tid = *tid;

    i = 0;
    while i < (*(*buildstate).ginstate.origTupdesc).natts {
        ginHeapTupleBulkInsert(buildstate, (i + 1) as OffsetNumber,
                               *values.add(i as usize), *isnull.add(i as usize), tid);
        i += 1;
    }

    /*
     * If we've maxed out our available memory, dump everything to the
     * tuplesort. We use half the per-worker fraction of maintenance_work_mem,
     * the other half is used for the tuplesort.
     */
    if (*buildstate).accum.allocatedMemory >= (*buildstate).work_mem as Size * 1024 {
        ginFlushBuildState(buildstate, index);
    }

    MemoryContextSwitchTo(oldCtx);
}

pub unsafe fn ginbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let result: *mut IndexBuildResult;
    let mut reltuples: f64;
    let mut buildstate: GinBuildState = core::mem::zeroed();
    let state: *mut GinBuildState = &mut buildstate;
    let RootBuffer: Buffer;
    let MetaBuffer: Buffer;
    let mut list: *mut ItemPointerData;
    let mut key: Datum = 0;
    let mut category: GinNullCategory = 0;
    let mut nlist: uint32 = 0;
    let mut oldCtx: MemoryContext;
    let mut attnum: OffsetNumber = 0;

    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(ERROR, "index \"{}\" already contains data",
              CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy());
    }

    initGinState(&mut buildstate.ginstate, index);
    buildstate.indtuples = 0.0;
    memset(&mut buildstate.buildStats as *mut GinStatsData as *mut c_void, 0,
           core::mem::size_of::<GinStatsData>());

    /* Initialize fields for parallel build too. */
    buildstate.bs_numtuples = 0.0;
    buildstate.bs_reltuples = 0.0;
    buildstate.bs_leader = null_mut();
    memset(&mut buildstate.tid as *mut ItemPointerData as *mut c_void, 0,
           core::mem::size_of::<ItemPointerData>());

    /* initialize the meta page */
    MetaBuffer = GinNewBuffer(index);

    /* initialize the root page */
    RootBuffer = GinNewBuffer(index);

    START_CRIT_SECTION();
    GinInitMetabuffer(MetaBuffer);
    MarkBufferDirty(MetaBuffer);
    GinInitBuffer(RootBuffer, GIN_LEAF as uint32);
    MarkBufferDirty(RootBuffer);


    UnlockReleaseBuffer(MetaBuffer);
    UnlockReleaseBuffer(RootBuffer);
    END_CRIT_SECTION();

    /* count the root as first entry page */
    buildstate.buildStats.nEntryPages += 1;

    /*
     * create a temporary memory context that is used to hold data not yet
     * dumped out to the index
     */
    buildstate.tmpCtx = AllocSetContextCreate!(CurrentMemoryContext,
                                               c"Gin build temporary context".as_ptr(),
                                               ALLOCSET_DEFAULT_SIZES);

    /*
     * create a temporary memory context that is used for calling
     * ginExtractEntries(), and can be reset after each tuple
     */
    buildstate.funcCtx = AllocSetContextCreate!(CurrentMemoryContext,
                                                c"Gin build temporary context for user-defined function".as_ptr(),
                                                ALLOCSET_DEFAULT_SIZES);

    buildstate.accum.ginstate = &mut buildstate.ginstate;
    ginInitBA(&mut buildstate.accum);

    /* Report table scan phase started */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                 PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN as i64);

    /*
     * Attempt to launch parallel worker scan when required
     *
     * XXX plan_create_index_workers makes the number of workers dependent on
     * maintenance_work_mem, requiring 32MB for each worker. For GIN that's
     * reasonable too, because we sort the data just like btree. It does
     * ignore the memory used to accumulate data in memory (set by work_mem),
     * but there is no way to communicate that to plan_create_index_workers.
     */
    if (*indexInfo).ii_ParallelWorkers > 0 {
        _gin_begin_parallel(state, heap, index, (*indexInfo).ii_Concurrent,
                            (*indexInfo).ii_ParallelWorkers);
    }

    /*
     * If parallel build requested and at least one worker process was
     * successfully launched, set up coordination state, wait for workers to
     * complete. Then read all tuples from the shared tuplesort and insert
     * them into the index.
     *
     * In serial mode, simply scan the table and build the index one index
     * tuple at a time.
     */
    if !(*state).bs_leader.is_null() {
        let coordinate: SortCoordinate;

        coordinate = palloc0(core::mem::size_of::<SortCoordinateData>()) as SortCoordinate;
        (*coordinate).isWorker = false;
        (*coordinate).nParticipants =
            (*(*state).bs_leader).nparticipanttuplesorts;
        (*coordinate).sharedsort = (*(*state).bs_leader).sharedsort;

        /*
         * Begin leader tuplesort.
         *
         * In cases where parallelism is involved, the leader receives the
         * same share of maintenance_work_mem as a serial sort (it is
         * generally treated in the same way as a serial sort once we return).
         * Parallel worker Tuplesortstates will have received only a fraction
         * of maintenance_work_mem, though.
         *
         * We rely on the lifetime of the Leader Tuplesortstate almost not
         * overlapping with any worker Tuplesortstate's lifetime.  There may
         * be some small overlap, but that's okay because we rely on leader
         * Tuplesortstate only allocating a small, fixed amount of memory
         * here. When its tuplesort_performsort() is called (by our caller),
         * and significant amounts of memory are likely to be used, all
         * workers must have already freed almost all memory held by their
         * Tuplesortstates (they are about to go away completely, too).  The
         * overall effect is that maintenance_work_mem always represents an
         * absolute high watermark on the amount of memory used by a CREATE
         * INDEX operation, regardless of the use of parallelism or any other
         * factor.
         */
        (*state).bs_sortstate =
            tuplesort_begin_index_gin(heap, index,
                                      maintenance_work_mem, coordinate,
                                      TUPLESORT_NONE);

        /* scan the relation in parallel and merge per-worker results */
        reltuples = _gin_parallel_merge(state);

        _gin_end_parallel((*state).bs_leader, state);
    } else {
        /* no parallel index build */
        /*
         * Do the heap scan.  We disallow sync scan here because
         * dataPlaceToPage prefers to receive tuples in TID order.
         */
        reltuples = table_index_build_scan(heap, index, indexInfo, false, true,
                                           ginBuildCallback, &mut buildstate as *mut GinBuildState as *mut c_void,
                                           null_mut());

        /* dump remaining entries to the index */
        oldCtx = MemoryContextSwitchTo(buildstate.tmpCtx);
        ginBeginBAScan(&mut buildstate.accum);
        loop {
            list = ginGetBAEntry(&mut buildstate.accum,
                                 &mut attnum, &mut key, &mut category, &mut nlist);
            if list.is_null() {
                break;
            }
            /* there could be many entries, so be willing to abort here */
            CHECK_FOR_INTERRUPTS();
            ginEntryInsert(&mut buildstate.ginstate, attnum, key, category,
                           list, nlist, &mut buildstate.buildStats);
        }
        MemoryContextSwitchTo(oldCtx);
    }

    MemoryContextDelete(buildstate.funcCtx);
    MemoryContextDelete(buildstate.tmpCtx);

    /*
     * Update metapage stats
     */
    buildstate.buildStats.nTotalPages = RelationGetNumberOfBlocks(index);
    ginUpdateStats(index, &buildstate.buildStats, true);

    /*
     * We didn't write WAL records as we built the index, so if WAL-logging is
     * required, write all pages to the WAL now.
     */
    if RelationNeedsWAL(index) {
        log_newpage_range(index, MAIN_FORKNUM,
                          0, RelationGetNumberOfBlocks(index),
                          true);
    }

    /*
     * Return statistics
     */
    result = palloc(core::mem::size_of::<IndexBuildResult>()) as *mut IndexBuildResult;

    (*result).heap_tuples = reltuples;
    (*result).index_tuples = buildstate.indtuples;

    return result;
}

/*
 *	ginbuildempty() -- build an empty gin index in the initialization fork
 */
pub unsafe fn ginbuildempty(index: Relation) {
    let RootBuffer: Buffer;
    let MetaBuffer: Buffer;

    /* An empty GIN index has two pages. */
    MetaBuffer = ExtendBufferedRel(BMR_REL(index), INIT_FORKNUM, null_mut(),
                                   EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
    RootBuffer = ExtendBufferedRel(BMR_REL(index), INIT_FORKNUM, null_mut(),
                                   EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);

    /* Initialize and xlog metabuffer and root buffer. */
    START_CRIT_SECTION();
    GinInitMetabuffer(MetaBuffer);
    MarkBufferDirty(MetaBuffer);
    log_newpage_buffer(MetaBuffer, true);
    GinInitBuffer(RootBuffer, GIN_LEAF as uint32);
    MarkBufferDirty(RootBuffer);
    log_newpage_buffer(RootBuffer, false);
    END_CRIT_SECTION();

    /* Unlock and release the buffers. */
    UnlockReleaseBuffer(MetaBuffer);
    UnlockReleaseBuffer(RootBuffer);
}

/*
 * Insert index entries for a single indexable item during "normal"
 * (non-fast-update) insertion
 */
unsafe fn ginHeapTupleInsert(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    item: ItemPointer,
) {
    let entries: *mut Datum;
    let mut categories: *mut GinNullCategory = null_mut();
    let mut i: int32;
    let mut nentries: int32 = 0;

    entries = ginExtractEntries(ginstate, attnum, value, isNull,
                                &mut nentries, &mut categories);

    i = 0;
    while i < nentries {
        ginEntryInsert(ginstate, attnum, *entries.add(i as usize), *categories.add(i as usize),
                       item, 1, null_mut());
        i += 1;
    }
}

pub unsafe fn gininsert(
    index: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    let mut ginstate: *mut GinState = (*indexInfo).ii_AmCache as *mut GinState;
    let mut oldCtx: MemoryContext;
    let insertCtx: MemoryContext;
    let mut i: c_int;

    /* Initialize GinState cache if first call in this statement */
    if ginstate.is_null() {
        oldCtx = MemoryContextSwitchTo((*indexInfo).ii_Context);
        ginstate = palloc(core::mem::size_of::<GinState>()) as *mut GinState;
        initGinState(ginstate, index);
        (*indexInfo).ii_AmCache = ginstate as *mut c_void;
        MemoryContextSwitchTo(oldCtx);
    }

    insertCtx = AllocSetContextCreate!(CurrentMemoryContext,
                                       c"Gin insert temporary context".as_ptr(),
                                       ALLOCSET_DEFAULT_SIZES);

    oldCtx = MemoryContextSwitchTo(insertCtx);

    if GinGetUseFastUpdate(index) {
        let mut collector: GinTupleCollector = core::mem::zeroed();

        memset(&mut collector as *mut GinTupleCollector as *mut c_void, 0,
               core::mem::size_of::<GinTupleCollector>());

        i = 0;
        while i < (*(*ginstate).origTupdesc).natts {
            ginHeapTupleFastCollect(ginstate, &mut collector,
                                    (i + 1) as OffsetNumber,
                                    *values.add(i as usize), *isnull.add(i as usize),
                                    ht_ctid);
            i += 1;
        }

        ginHeapTupleFastInsert(ginstate, &mut collector);
    } else {
        i = 0;
        while i < (*(*ginstate).origTupdesc).natts {
            ginHeapTupleInsert(ginstate, (i + 1) as OffsetNumber,
                               *values.add(i as usize), *isnull.add(i as usize),
                               ht_ctid);
            i += 1;
        }
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false;
}

/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort states, which may later be created based on shared
 * state initially set up here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's GinLeader, which caller must use to shut down parallel
 * mode by passing it to _gin_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
unsafe fn _gin_begin_parallel(
    buildstate: *mut GinBuildState,
    heap: Relation,
    index: Relation,
    isconcurrent: bool,
    request: c_int,
) {
    let pcxt: *mut ParallelContext;
    let scantuplesortstates: c_int;
    let mut snapshot: Snapshot;
    let estginshared: Size;
    let estsort: Size;
    let ginshared: *mut GinBuildShared;
    let sharedsort: *mut Sharedsort;
    let ginleader: *mut GinLeader = palloc0(core::mem::size_of::<GinLeader>()) as *mut GinLeader;
    let walusage: *mut WalUsage;
    let bufferusage: *mut BufferUsage;
    let leaderparticipates: bool = true;
    let mut querylen: c_int;

    // #ifdef DISABLE_LEADER_PARTICIPATION
    //     leaderparticipates = false;
    // #endif

    /*
     * Enter parallel mode, and create context for parallel build of gin index
     */
    EnterParallelMode();
    Assert!(request > 0);
    pcxt = CreateParallelContext(c"postgres".as_ptr(), c"_gin_parallel_build_main".as_ptr(),
                                 request);

    scantuplesortstates = if leaderparticipates { request + 1 } else { request };

    /*
     * Prepare for scan of the base relation.  In a normal index build, we use
     * SnapshotAny because we must retrieve all tuples and do our own time
     * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
     * concurrent build, we take a regular MVCC snapshot and index whatever's
     * live according to that.
     */
    if !isconcurrent {
        snapshot = SnapshotAny;
    } else {
        snapshot = RegisterSnapshot(GetTransactionSnapshot());
    }

    /*
     * Estimate size for our own PARALLEL_KEY_GIN_SHARED workspace.
     */
    estginshared = _gin_parallel_estimate_shared(heap, snapshot);
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, estginshared);
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, estsort);

    shm_toc_estimate_keys(&mut (*pcxt).estimator, 2);

    /*
     * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
     * and PARALLEL_KEY_BUFFER_USAGE.
     *
     * If there are no extensions loaded that care, we could skip this.  We
     * have no way of knowing whether anyone's looking at pgWalUsage or
     * pgBufferUsage, so do it unconditionally.
     */
    shm_toc_estimate_chunk(&mut (*pcxt).estimator,
                           mul_size(core::mem::size_of::<WalUsage>(), (*pcxt).nworkers as Size));
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
    shm_toc_estimate_chunk(&mut (*pcxt).estimator,
                           mul_size(core::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as Size));
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);

    /* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
    if !debug_query_string.is_null() {
        querylen = strlen(debug_query_string) as c_int;
        shm_toc_estimate_chunk(&mut (*pcxt).estimator, (querylen + 1) as Size);
        shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
    } else {
        querylen = 0; /* keep compiler quiet */
    }

    /* Everyone's had a chance to ask for space, so now create the DSM */
    InitializeParallelDSM(pcxt);

    /* If no DSM segment was available, back out (do serial build) */
    if (*pcxt).seg.is_null() {
        if IsMVCCSnapshot(snapshot) {
            UnregisterSnapshot(snapshot);
        }
        DestroyParallelContext(pcxt);
        ExitParallelMode();
        return;
    }

    /* Store shared build state, for which we reserved space */
    ginshared = shm_toc_allocate((*pcxt).toc, estginshared) as *mut GinBuildShared;
    /* Initialize immutable state */
    (*ginshared).heaprelid = RelationGetRelid(heap);
    (*ginshared).indexrelid = RelationGetRelid(index);
    (*ginshared).isconcurrent = isconcurrent;
    (*ginshared).scantuplesortstates = scantuplesortstates;

    ConditionVariableInit(&mut (*ginshared).workersdonecv);
    SpinLockInit(&mut (*ginshared).mutex);

    /* Initialize mutable state */
    (*ginshared).nparticipantsdone = 0;
    (*ginshared).reltuples = 0.0;
    (*ginshared).indtuples = 0.0;

    table_parallelscan_initialize(heap,
                                  ParallelTableScanFromGinBuildShared(ginshared),
                                  snapshot);

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    sharedsort = shm_toc_allocate((*pcxt).toc, estsort) as *mut Sharedsort;
    tuplesort_initialize_shared(sharedsort, scantuplesortstates,
                                (*pcxt).seg as *mut dsm_segment);

    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_GIN_SHARED, ginshared as *mut c_void);
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TUPLESORT, sharedsort as *mut c_void);

    /* Store query string for workers */
    if !debug_query_string.is_null() {
        let sharedquery: *mut c_char;

        sharedquery = shm_toc_allocate((*pcxt).toc, (querylen + 1) as Size) as *mut c_char;
        memcpy(sharedquery as *mut c_void, debug_query_string as *const c_void, (querylen + 1) as usize);
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_QUERY_TEXT, sharedquery as *mut c_void);
    }

    /*
     * Allocate space for each worker's WalUsage and BufferUsage; no need to
     * initialize.
     */
    walusage = shm_toc_allocate((*pcxt).toc,
                                mul_size(core::mem::size_of::<WalUsage>(), (*pcxt).nworkers as Size)) as *mut WalUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_WAL_USAGE, walusage as *mut c_void);
    bufferusage = shm_toc_allocate((*pcxt).toc,
                                   mul_size(core::mem::size_of::<BufferUsage>(), (*pcxt).nworkers as Size)) as *mut BufferUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage as *mut c_void);

    /* Launch workers, saving status for leader/caller */
    LaunchParallelWorkers(pcxt);
    (*ginleader).pcxt = pcxt;
    (*ginleader).nparticipanttuplesorts = (*pcxt).nworkers_launched;
    if leaderparticipates {
        (*ginleader).nparticipanttuplesorts += 1;
    }
    (*ginleader).ginshared = ginshared;
    (*ginleader).sharedsort = sharedsort;
    (*ginleader).snapshot = snapshot;
    (*ginleader).walusage = walusage;
    (*ginleader).bufferusage = bufferusage;

    /* If no workers were successfully launched, back out (do serial build) */
    if (*pcxt).nworkers_launched == 0 {
        _gin_end_parallel(ginleader, null_mut());
        return;
    }

    /* Save leader state now that it's clear build will be parallel */
    (*buildstate).bs_leader = ginleader;

    /* Join heap scan ourselves */
    if leaderparticipates {
        _gin_leader_participate_as_worker(buildstate, heap, index);
    }

    /*
     * Caller needs to wait for all launched workers when we return.  Make
     * sure that the failure-to-start case will not hang forever.
     */
    WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
unsafe fn _gin_end_parallel(ginleader: *mut GinLeader, state: *mut GinBuildState) {
    let mut i: c_int;

    /* Shutdown worker processes */
    WaitForParallelWorkersToFinish((*ginleader).pcxt);

    /*
     * Next, accumulate WAL usage.  (This must wait for the workers to finish,
     * or we might get incomplete data.)
     */
    i = 0;
    while i < (*(*ginleader).pcxt).nworkers_launched {
        InstrAccumParallelQuery((*ginleader).bufferusage.add(i as usize), (*ginleader).walusage.add(i as usize));
        i += 1;
    }

    /* Free last reference to MVCC snapshot, if one was used */
    if IsMVCCSnapshot((*ginleader).snapshot) {
        UnregisterSnapshot((*ginleader).snapshot);
    }
    DestroyParallelContext((*ginleader).pcxt);
    ExitParallelMode();
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _gin_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Returns the total number of heap tuples scanned.
 */
unsafe fn _gin_parallel_heapscan(state: *mut GinBuildState) -> f64 {
    let ginshared: *mut GinBuildShared = (*(*state).bs_leader).ginshared;
    let nparticipanttuplesorts: c_int;

    nparticipanttuplesorts = (*(*state).bs_leader).nparticipanttuplesorts;
    loop {
        SpinLockAcquire(&mut (*ginshared).mutex);
        if (*ginshared).nparticipantsdone == nparticipanttuplesorts {
            /* copy the data into leader state */
            (*state).bs_reltuples = (*ginshared).reltuples;
            (*state).bs_numtuples = (*ginshared).indtuples;

            SpinLockRelease(&mut (*ginshared).mutex);
            break;
        }
        SpinLockRelease(&mut (*ginshared).mutex);

        ConditionVariableSleep(&mut (*ginshared).workersdonecv,
                               WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
    }

    ConditionVariableCancelSleep();

    return (*state).bs_reltuples;
}

/// TODO(pg-port): access/gin_private.h -- GinGetUseFastUpdate relation accessor
/// (reads index->rd_options->useFastUpdate, defaulting to
/// GIN_DEFAULT_USE_FASTUPDATE). Not yet ported because it dereferences the
/// relcache rd_options layout.
unsafe fn GinGetUseFastUpdate(index: Relation) -> bool {
    // TODO(pg-port): access/gin_private.h
    unimplemented!()
}

/// TODO(pg-port): c.h -- UINT16_MAX (stdint limit; mirrors PG_UINT16_MAX width)
pub const UINT16_MAX: c_int = 0xFFFF;

/*
 * Buffer used to accumulate TIDs from multiple GinTuples for the same key
 * (we read these from the tuplesort, sorted by the key).
 *
 * This is similar to BuildAccumulator in that it's used to collect TIDs
 * in memory before inserting them into the index, but it's much simpler
 * as it only deals with a single index key at a time.
 *
 * When adding TIDs to the buffer, we make sure to keep them sorted, both
 * during the initial table scan (and detecting when the scan wraps around),
 * and during merging (where we do mergesort).
 */
#[repr(C)]
pub struct GinBuffer {
    pub attnum: OffsetNumber,
    pub category: GinNullCategory,
    pub key: Datum,             /* 0 if no key (and keylen == 0) */
    pub keylen: Size,           /* number of bytes (not typlen) */

    /* type info */
    pub typlen: int16,
    pub typbyval: bool,

    /* Number of TIDs to collect before attempt to write some out. */
    pub maxitems: c_int,

    /* array of TID values */
    pub nitems: c_int,
    pub nfrozen: c_int,
    pub ssup: SortSupport,      /* for sorting/comparing keys */
    pub items: *mut ItemPointerData,
}

/*
 * Check that TID array contains valid values, and that it's sorted (if we
 * expect it to be).
 */
unsafe fn AssertCheckItemPointers(buffer: *mut GinBuffer) {
    // USE_ASSERT_CHECKING:
    /* we should not have a buffer with no TIDs to sort */
    Assert!(!(*buffer).items.is_null());
    Assert!((*buffer).nitems > 0);

    let mut i: c_int = 0;
    while i < (*buffer).nitems {
        Assert!(ItemPointerIsValid((*buffer).items.add(i as usize)));

        /* don't check ordering for the first TID item */
        if i == 0 {
            i += 1;
            continue;
        }

        Assert!(ItemPointerCompare((*buffer).items.add((i - 1) as usize),
                                   (*buffer).items.add(i as usize)) < 0);
        i += 1;
    }
}

/*
 * GinBuffer checks
 *
 * Make sure the nitems/items fields are consistent (either the array is empty
 * or not empty, the fields need to agree). If there are items, check ordering.
 */
unsafe fn AssertCheckGinBuffer(buffer: *mut GinBuffer) {
    // USE_ASSERT_CHECKING:
    /* if we have any items, the array must exist */
    Assert!(!(((*buffer).nitems > 0) && ((*buffer).items.is_null())));

    /*
     * The buffer may be empty, in which case we must not call the check of
     * item pointers, because that assumes non-emptiness.
     */
    if (*buffer).nitems == 0 {
        return;
    }

    /* Make sure the item pointers are valid and sorted. */
    AssertCheckItemPointers(buffer);
}

/*
 * GinBufferInit
 *		Initialize buffer to store tuples for a GIN index.
 *
 * Initialize the buffer used to accumulate TID for a single key at a time
 * (we process the data sorted), so we know when we received all data for
 * a given key.
 *
 * Initializes sort support procedures for all index attributes.
 */
unsafe fn GinBufferInit(index: Relation) -> *mut GinBuffer {
    let buffer: *mut GinBuffer = palloc0(core::mem::size_of::<GinBuffer>()) as *mut GinBuffer;
    let mut i: c_int;
    let nKeys: c_int;
    let desc: TupleDesc = RelationGetDescr(index);

    /*
     * How many items can we fit into the memory limit? We don't want to end
     * with too many TIDs. and 64kB seems more than enough. But maybe this
     * should be tied to maintenance_work_mem or something like that?
     */
    (*buffer).maxitems = ((64 * 1024i64) / core::mem::size_of::<ItemPointerData>() as i64) as c_int;

    nKeys = IndexRelationGetNumberOfKeyAttributes(index);

    (*buffer).ssup = palloc0(core::mem::size_of::<SortSupportData>() * nKeys as usize) as SortSupport;

    /*
     * Lookup ordering operator for the index key data type, and initialize
     * the sort support function.
     */
    i = 0;
    while i < nKeys {
        let mut cmpFunc: Oid;
        let sortKey: SortSupport = (*buffer).ssup.add(i as usize);
        let att: Form_pg_attribute = TupleDescAttr(desc, i as c_int);

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = RelationGetIndcollation(index, i);

        if !OidIsValid((*sortKey).ssup_collation) {
            (*sortKey).ssup_collation = DEFAULT_COLLATION_OID;
        }

        (*sortKey).ssup_nulls_first = false;
        (*sortKey).ssup_attno = i + 1;
        (*sortKey).abbreviate = false;

        Assert!((*sortKey).ssup_attno != 0);

        /*
         * If the compare proc isn't specified in the opclass definition, look
         * up the index key type's default btree comparator.
         */
        cmpFunc = index_getprocid(index, (i + 1) as OffsetNumber, GIN_COMPARE_PROC);
        if cmpFunc == InvalidOid {
            let typentry: *mut TypeCacheEntry;

            typentry = lookup_type_cache((*att).atttypid,
                                         TYPECACHE_CMP_PROC_FINFO as c_int);
            if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
                ereport!(ERROR,
                         errmsg!("could not identify a comparison function for type {}",
                                 CStr::from_ptr(format_type_be((*att).atttypid)).to_string_lossy()));
                // C also: errcode(ERRCODE_UNDEFINED_FUNCTION)
            }

            cmpFunc = (*typentry).cmp_proc_finfo.fn_oid;
        }

        PrepareSortSupportComparisonShim(cmpFunc, sortKey);
        i += 1;
    }

    return buffer;
}

/* Is the buffer empty, i.e. has no TID values in the array? */
unsafe fn GinBufferIsEmpty(buffer: *mut GinBuffer) -> bool {
    return (*buffer).nitems == 0;
}

/*
 * GinBufferKeyEquals
 *		Can the buffer store TIDs for the provided GIN tuple (same key)?
 *
 * Compare if the tuple matches the already accumulated data in the GIN
 * buffer. Compare scalar fields first, before the actual key.
 *
 * Returns true if the key matches, and the TID belongs to the buffer, or
 * false if the key does not match.
 */
unsafe fn GinBufferKeyEquals(buffer: *mut GinBuffer, tup: *mut GinTuple) -> bool {
    let r: c_int;
    let tupkey: Datum;

    AssertCheckGinBuffer(buffer);

    if (*tup).attrnum != (*buffer).attnum {
        return false;
    }

    /* same attribute should have the same type info */
    Assert!((*tup).typbyval == (*buffer).typbyval);
    Assert!((*tup).typlen == (*buffer).typlen);

    if (*tup).category != (*buffer).category {
        return false;
    }

    /*
     * For NULL/empty keys, this means equality, for normal keys we need to
     * compare the actual key value.
     */
    if (*buffer).category != GIN_CAT_NORM_KEY as GinNullCategory {
        return true;
    }

    /*
     * For the tuple, get either the first sizeof(Datum) bytes for byval
     * types, or a pointer to the beginning of the data array.
     */
    tupkey = if (*buffer).typbyval {
        *((*tup).data.as_ptr() as *const Datum)
    } else {
        PointerGetDatum((*tup).data.as_ptr() as *const c_void)
    };

    r = ApplySortComparator((*buffer).key, false,
                            tupkey, false,
                            (*buffer).ssup.add(((*buffer).attnum - 1) as usize));

    return r == 0;
}

/*
 * GinBufferShouldTrim
 *		Should we trim the list of item pointers?
 *
 * By trimming we understand writing out and removing the tuple IDs that
 * we know can't change by future merges. We can deduce the TID up to which
 * this is guaranteed from the "first" TID in each GIN tuple, which provides
 * a "horizon" (for a given key) thanks to the sort.
 *
 * We don't want to do this too often - compressing longer TID lists is more
 * efficient. But we also don't want to accumulate too many TIDs, for two
 * reasons. First, it consumes memory and we might exceed maintenance_work_mem
 * (or whatever limit applies), even if that's unlikely because TIDs are very
 * small so we can fit a lot of them. Second, and more importantly, long TID
 * lists are an issue if the scan wraps around, because a key may get a very
 * wide list (with min/max TID for that key), forcing "full" mergesorts for
 * every list merged into it (instead of the efficient append).
 *
 * So we look at two things when deciding if to trim - if the resulting list
 * (after adding TIDs from the new tuple) would be too long, and if there is
 * enough TIDs to trim (with values less than "first" TID from the new tuple),
 * we do the trim. By enough we mean at least 128 TIDs (mostly an arbitrary
 * number).
 */
unsafe fn GinBufferShouldTrim(buffer: *mut GinBuffer, tup: *mut GinTuple) -> bool {
    /* not enough TIDs to trim (1024 is somewhat arbitrary number) */
    if (*buffer).nfrozen < 1024 {
        return false;
    }

    /* no need to trim if we have not hit the memory limit yet */
    if ((*buffer).nitems + (*tup).nitems) < (*buffer).maxitems {
        return false;
    }

    /*
     * OK, we have enough frozen TIDs to flush, and we have hit the memory
     * limit, so it's time to write it out.
     */
    return true;
}

/*
 * GinBufferStoreTuple
 *		Add data (especially TID list) from a GIN tuple to the buffer.
 *
 * The buffer is expected to be empty (in which case it's initialized), or
 * having the same key. The TID values from the tuple are combined with the
 * stored values using a merge sort.
 *
 * The tuples (for the same key) are expected to be sorted by first TID. But
 * this does not guarantee the lists do not overlap, especially in the leader,
 * because the workers process interleaving data. There should be no overlaps
 * in a single worker - it could happen when the parallel scan wraps around,
 * but we detect that and flush the data (see ginBuildCallbackParallel).
 *
 * By sorting the GinTuple not only by key, but also by the first TID, we make
 * it more less likely the lists will overlap during merge. We merge them using
 * mergesort, but it's cheaper to just append one list to the other.
 *
 * How often can the lists overlap? There should be no overlaps in workers,
 * and in the leader we can see overlaps between lists built by different
 * workers. But the workers merge the items as much as possible, so there
 * should not be too many.
 */
unsafe fn GinBufferStoreTuple(buffer: *mut GinBuffer, tup: *mut GinTuple) {
    let items: *mut ItemPointerData;
    let key: Datum;

    AssertCheckGinBuffer(buffer);

    key = _gin_parse_tuple_key(tup);
    items = _gin_parse_tuple_items(tup);

    /* if the buffer is empty, set the fields (and copy the key) */
    if GinBufferIsEmpty(buffer) {
        (*buffer).category = (*tup).category;
        (*buffer).keylen = (*tup).keylen as Size;
        (*buffer).attnum = (*tup).attrnum;

        (*buffer).typlen = (*tup).typlen;
        (*buffer).typbyval = (*tup).typbyval;

        if (*tup).category == GIN_CAT_NORM_KEY as GinNullCategory {
            (*buffer).key = datumCopy(key, (*buffer).typbyval, (*buffer).typlen as c_int);
        } else {
            (*buffer).key = 0 as Datum;
        }
    }

    /*
     * Try freeze TIDs at the beginning of the list, i.e. exclude them from
     * the mergesort. We can do that with TIDs before the first TID in the new
     * tuple we're about to add into the buffer.
     *
     * We do this incrementally when adding data into the in-memory buffer,
     * and not later (e.g. when hitting a memory limit), because it allows us
     * to skip the frozen data during the mergesort, making it cheaper.
     */

    /*
     * Check if the last TID in the current list is frozen. This is the case
     * when merging non-overlapping lists, e.g. in each parallel worker.
     */
    if ((*buffer).nitems > 0) &&
        (ItemPointerCompare((*buffer).items.add(((*buffer).nitems - 1) as usize),
                            GinTupleGetFirst(tup)) == 0) {
        (*buffer).nfrozen = (*buffer).nitems;
    }

    /*
     * Now find the last TID we know to be frozen, i.e. the last TID right
     * before the new GIN tuple.
     *
     * Start with the first not-yet-frozen tuple, and walk until we find the
     * first TID that's higher. If we already know the whole list is frozen
     * (i.e. nfrozen == nitems), this does nothing.
     *
     * XXX This might do a binary search for sufficiently long lists, but it
     * does not seem worth the complexity. Overlapping lists should be rare
     * common, TID comparisons are cheap, and we should quickly freeze most of
     * the list.
     */
    let mut i: c_int = (*buffer).nfrozen;
    while i < (*buffer).nitems {
        /* Is the TID after the first TID of the new tuple? Can't freeze. */
        if ItemPointerCompare((*buffer).items.add(i as usize),
                              GinTupleGetFirst(tup)) > 0 {
            break;
        }

        (*buffer).nfrozen += 1;
        i += 1;
    }

    /* add the new TIDs into the buffer, combine using merge-sort */
    {
        let mut nnew: c_int = 0;
        let new: ItemPointer;

        /*
         * Resize the array - we do this first, because we'll dereference the
         * first unfrozen TID, which would fail if the array is NULL. We'll
         * still pass 0 as number of elements in that array though.
         */
        if (*buffer).items.is_null() {
            (*buffer).items = palloc(((*buffer).nitems + (*tup).nitems) as usize * core::mem::size_of::<ItemPointerData>()) as *mut ItemPointerData;
        } else {
            (*buffer).items = repalloc((*buffer).items as *mut c_void,
                                       ((*buffer).nitems + (*tup).nitems) as usize * core::mem::size_of::<ItemPointerData>()) as *mut ItemPointerData;
        }

        new = ginMergeItemPointers((*buffer).items.add((*buffer).nfrozen as usize), /* first unfrozen */
                                   ((*buffer).nitems - (*buffer).nfrozen) as uint32,	/* num of unfrozen */
                                   items, (*tup).nitems as uint32, &mut nnew);

        Assert!(nnew == ((*tup).nitems + ((*buffer).nitems - (*buffer).nfrozen)));

        memcpy((*buffer).items.add((*buffer).nfrozen as usize) as *mut c_void, new as *const c_void,
               nnew as usize * core::mem::size_of::<ItemPointerData>());

        pfree(new as *mut c_void);

        (*buffer).nitems += (*tup).nitems;

        AssertCheckItemPointers(buffer);
    }

    /* free the decompressed TID list */
    pfree(items as *mut c_void);
}

/*
 * GinBufferReset
 *		Reset the buffer into a state as if it contains no data.
 */
unsafe fn GinBufferReset(buffer: *mut GinBuffer) {
    Assert!(!GinBufferIsEmpty(buffer));

    /* release byref values, do nothing for by-val ones */
    if ((*buffer).category == GIN_CAT_NORM_KEY as GinNullCategory) && !(*buffer).typbyval {
        pfree(DatumGetPointer((*buffer).key) as *mut c_void);
    }

    /*
     * Not required, but makes it more likely to trigger NULL dereference if
     * using the value incorrectly, etc.
     */
    (*buffer).key = 0 as Datum;

    (*buffer).attnum = 0;
    (*buffer).category = 0;
    (*buffer).keylen = 0;
    (*buffer).nitems = 0;
    (*buffer).nfrozen = 0;

    (*buffer).typlen = 0;
    (*buffer).typbyval = false;
}

/*
 * GinBufferTrim
 *		Discard the "frozen" part of the TID list (which should have been
 *		written to disk/index before this call).
 */
unsafe fn GinBufferTrim(buffer: *mut GinBuffer) {
    Assert!(((*buffer).nfrozen > 0) && ((*buffer).nfrozen <= (*buffer).nitems));

    memmove((*buffer).items.add(0) as *mut c_void, (*buffer).items.add((*buffer).nfrozen as usize) as *const c_void,
            core::mem::size_of::<ItemPointerData>() * ((*buffer).nitems - (*buffer).nfrozen) as usize);

    (*buffer).nitems -= (*buffer).nfrozen;
    (*buffer).nfrozen = 0;
}

/*
 * GinBufferFree
 *		Release memory associated with the GinBuffer (including TID array).
 */
unsafe fn GinBufferFree(buffer: *mut GinBuffer) {
    if !(*buffer).items.is_null() {
        pfree((*buffer).items as *mut c_void);
    }

    /* release byref values, do nothing for by-val ones */
    if !GinBufferIsEmpty(buffer) &&
        ((*buffer).category == GIN_CAT_NORM_KEY as GinNullCategory) && !(*buffer).typbyval {
        pfree(DatumGetPointer((*buffer).key) as *mut c_void);
    }

    pfree(buffer as *mut c_void);
}

/*
 * GinBufferCanAddKey
 *		Check if a given GIN tuple can be added to the current buffer.
 *
 * Returns true if the buffer is either empty or for the same index key.
 */
unsafe fn GinBufferCanAddKey(buffer: *mut GinBuffer, tup: *mut GinTuple) -> bool {
    /* empty buffer can accept data for any key */
    if GinBufferIsEmpty(buffer) {
        return true;
    }

    /* otherwise just data for the same key */
    return GinBufferKeyEquals(buffer, tup);
}

/*
 * Within leader, wait for end of heap scan and merge per-worker results.
 *
 * After waiting for all workers to finish, merge the per-worker results into
 * the complete index. The results from each worker are sorted by block number
 * (start of the page range). While combining the per-worker results we merge
 * summaries for the same page range, and also fill-in empty summaries for
 * ranges without any tuples.
 *
 * Returns the total number of heap tuples scanned.
 */
unsafe fn _gin_parallel_merge(state: *mut GinBuildState) -> f64 {
    let mut tup: *mut GinTuple;
    let mut tuplen: Size = 0;
    let reltuples: f64;
    let buffer: *mut GinBuffer;

    /* GIN tuples from workers, merged by leader */
    let mut numtuples: f64 = 0.0;

    /* wait for workers to scan table and produce partial results */
    reltuples = _gin_parallel_heapscan(state);

    /* Execute the sort */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                 PROGRESS_GIN_PHASE_PERFORMSORT_2 as i64);

    /* do the actual sort in the leader */
    tuplesort_performsort((*state).bs_sortstate);

    /*
     * Initialize buffer to combine entries for the same key.
     *
     * The leader is allowed to use the whole maintenance_work_mem buffer to
     * combine data. The parallel workers already completed.
     */
    buffer = GinBufferInit((*state).ginstate.index);

    /*
     * Set the progress target for the next phase.  Reset the block number
     * values set by table_index_build_scan
     */
    {
        let progress_index: [c_int; 4] = [
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
        ];
        let progress_vals: [i64; 4] = [
            PROGRESS_GIN_PHASE_MERGE_2 as i64,
            (*state).bs_numtuples as i64,
            0, 0,
        ];

        pgstat_progress_update_multi_param(4, progress_index.as_ptr(), progress_vals.as_ptr());
    }

    /*
     * Read the GIN tuples from the shared tuplesort, sorted by category and
     * key. That probably gives us order matching how data is organized in the
     * index.
     *
     * We don't insert the GIN tuples right away, but instead accumulate as
     * many TIDs for the same key as possible, and then insert that at once.
     * This way we don't need to decompress/recompress the posting lists, etc.
     */
    loop {
        tup = tuplesort_getgintuple((*state).bs_sortstate, &mut tuplen, true);
        if tup.is_null() {
            break;
        }

        let oldCtx: MemoryContext;

        CHECK_FOR_INTERRUPTS();

        /*
         * If the buffer can accept the new GIN tuple, just store it there and
         * we're done. If it's a different key (or maybe too much data) flush
         * the current contents into the index first.
         */
        if !GinBufferCanAddKey(buffer, tup) {
            /*
             * Buffer is not empty and it's storing a different key - flush
             * the data into the insert, and start a new entry for current
             * GinTuple.
             */
            AssertCheckItemPointers(buffer);

            oldCtx = MemoryContextSwitchTo((*state).tmpCtx);

            ginEntryInsert(&mut (*state).ginstate,
                           (*buffer).attnum, (*buffer).key, (*buffer).category,
                           (*buffer).items, (*buffer).nitems as uint32, &mut (*state).buildStats);

            MemoryContextSwitchTo(oldCtx);
            MemoryContextReset((*state).tmpCtx);

            /* discard the existing data */
            GinBufferReset(buffer);
        }

        /*
         * We're about to add a GIN tuple to the buffer - check the memory
         * limit first, and maybe write out some of the data into the index
         * first, if needed (and possible). We only flush the part of the TID
         * list that we know won't change, and only if there's enough data for
         * compression to work well.
         */
        if GinBufferShouldTrim(buffer, tup) {
            Assert!((*buffer).nfrozen > 0);

            /*
             * Buffer is not empty and it's storing a different key - flush
             * the data into the insert, and start a new entry for current
             * GinTuple.
             */
            AssertCheckItemPointers(buffer);

            oldCtx = MemoryContextSwitchTo((*state).tmpCtx);

            ginEntryInsert(&mut (*state).ginstate,
                           (*buffer).attnum, (*buffer).key, (*buffer).category,
                           (*buffer).items, (*buffer).nfrozen as uint32, &mut (*state).buildStats);

            MemoryContextSwitchTo(oldCtx);
            MemoryContextReset((*state).tmpCtx);

            /* truncate the data we've just discarded */
            GinBufferTrim(buffer);
        }

        /*
         * Remember data for the current tuple (either remember the new key,
         * or append if to the existing data).
         */
        GinBufferStoreTuple(buffer, tup);

        /* Report progress */
        numtuples += 1.0;
        pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
                                     numtuples as i64);
    }

    /* flush data remaining in the buffer (for the last key) */
    if !GinBufferIsEmpty(buffer) {
        AssertCheckItemPointers(buffer);

        ginEntryInsert(&mut (*state).ginstate,
                       (*buffer).attnum, (*buffer).key, (*buffer).category,
                       (*buffer).items, (*buffer).nitems as uint32, &mut (*state).buildStats);

        /* discard the existing data */
        GinBufferReset(buffer);

        /* Report progress */
        numtuples += 1.0;
        pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
                                     numtuples as i64);
    }

    /* relase all the memory */
    GinBufferFree(buffer);

    tuplesort_end((*state).bs_sortstate);

    return reltuples;
}

/*
 * Returns size of shared memory required to store state for a parallel
 * gin index build based on the snapshot its parallel scan will use.
 */
unsafe fn _gin_parallel_estimate_shared(heap: Relation, snapshot: Snapshot) -> Size {
    /* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
    return add_size(BUFFERALIGN(core::mem::size_of::<GinBuildShared>()),
                    table_parallelscan_estimate(heap, snapshot));
}

/*
 * Within leader, participate as a parallel worker.
 */
unsafe fn _gin_leader_participate_as_worker(buildstate: *mut GinBuildState, heap: Relation, index: Relation) {
    let ginleader: *mut GinLeader = (*buildstate).bs_leader;
    let sortmem: c_int;

    /*
     * Might as well use reliable figure when doling out maintenance_work_mem
     * (when requested number of workers were not launched, this will be
     * somewhat higher than it is for other workers).
     */
    sortmem = maintenance_work_mem / (*ginleader).nparticipanttuplesorts;

    /* Perform work common to all participants */
    _gin_parallel_scan_and_build(buildstate, (*ginleader).ginshared,
                                 (*ginleader).sharedsort, heap, index,
                                 sortmem, true);
}

/*
 * _gin_process_worker_data
 *		First phase of the key merging, happening in the worker.
 *
 * Depending on the number of distinct keys, the TID lists produced by the
 * callback may be very short (due to frequent evictions in the callback).
 * But combining many tiny lists is expensive, so we try to do as much as
 * possible in the workers and only then pass the results to the leader.
 *
 * We read the tuples sorted by the key, and merge them into larger lists.
 * At the moment there's no memory limit, so this will just produce one
 * huge (sorted) list per key in each worker. Which means the leader will
 * do a very limited number of mergesorts, which is good.
 */
unsafe fn _gin_process_worker_data(state: *mut GinBuildState, worker_sort: *mut Tuplesortstate,
                                   progress: bool) {
    let mut tup: *mut GinTuple;
    let mut tuplen: Size = 0;

    let buffer: *mut GinBuffer;

    /*
     * Initialize buffer to combine entries for the same key.
     *
     * The workers are limited to the same amount of memory as during the sort
     * in ginBuildCallbackParallel. But this probably should be the 32MB used
     * during planning, just like there.
     */
    buffer = GinBufferInit((*state).ginstate.index);

    /* sort the raw per-worker data */
    if progress {
        pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                     PROGRESS_GIN_PHASE_PERFORMSORT_1 as i64);
    }

    tuplesort_performsort((*state).bs_worker_sort);

    /* reset the number of GIN tuples produced by this worker */
    (*state).bs_numtuples = 0.0;

    if progress {
        pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                     PROGRESS_GIN_PHASE_MERGE_1 as i64);
    }

    /*
     * Read the GIN tuples from the shared tuplesort, sorted by the key, and
     * merge them into larger chunks for the leader to combine.
     */
    loop {
        tup = tuplesort_getgintuple(worker_sort, &mut tuplen, true);
        if tup.is_null() {
            break;
        }

        CHECK_FOR_INTERRUPTS();

        /*
         * If the buffer can accept the new GIN tuple, just store it there and
         * we're done. If it's a different key (or maybe too much data) flush
         * the current contents into the index first.
         */
        if !GinBufferCanAddKey(buffer, tup) {
            let ntup: *mut GinTuple;
            let mut ntuplen: Size = 0;

            /*
             * Buffer is not empty and it's storing a different key - flush
             * the data into the insert, and start a new entry for current
             * GinTuple.
             */
            AssertCheckItemPointers(buffer);

            ntup = _gin_build_tuple((*buffer).attnum, (*buffer).category as u8,
                                    (*buffer).key, (*buffer).typlen, (*buffer).typbyval,
                                    (*buffer).items, (*buffer).nitems as uint32, &mut ntuplen);

            tuplesort_putgintuple((*state).bs_sortstate, ntup, ntuplen);
            (*state).bs_numtuples += 1.0;

            pfree(ntup as *mut c_void);

            /* discard the existing data */
            GinBufferReset(buffer);
        }

        /*
         * We're about to add a GIN tuple to the buffer - check the memory
         * limit first, and maybe write out some of the data into the index
         * first, if needed (and possible). We only flush the part of the TID
         * list that we know won't change, and only if there's enough data for
         * compression to work well.
         */
        if GinBufferShouldTrim(buffer, tup) {
            let ntup: *mut GinTuple;
            let mut ntuplen: Size = 0;

            Assert!((*buffer).nfrozen > 0);

            /*
             * Buffer is not empty and it's storing a different key - flush
             * the data into the insert, and start a new entry for current
             * GinTuple.
             */
            AssertCheckItemPointers(buffer);

            ntup = _gin_build_tuple((*buffer).attnum, (*buffer).category as u8,
                                    (*buffer).key, (*buffer).typlen, (*buffer).typbyval,
                                    (*buffer).items, (*buffer).nfrozen as uint32, &mut ntuplen);

            tuplesort_putgintuple((*state).bs_sortstate, ntup, ntuplen);

            pfree(ntup as *mut c_void);

            /* truncate the data we've just discarded */
            GinBufferTrim(buffer);
        }

        /*
         * Remember data for the current tuple (either remember the new key,
         * or append if to the existing data).
         */
        GinBufferStoreTuple(buffer, tup);
    }

    /* flush data remaining in the buffer (for the last key) */
    if !GinBufferIsEmpty(buffer) {
        let ntup: *mut GinTuple;
        let mut ntuplen: Size = 0;

        AssertCheckItemPointers(buffer);

        ntup = _gin_build_tuple((*buffer).attnum, (*buffer).category as u8,
                                (*buffer).key, (*buffer).typlen, (*buffer).typbyval,
                                (*buffer).items, (*buffer).nitems as uint32, &mut ntuplen);

        tuplesort_putgintuple((*state).bs_sortstate, ntup, ntuplen);
        (*state).bs_numtuples += 1.0;

        pfree(ntup as *mut c_void);

        /* discard the existing data */
        GinBufferReset(buffer);
    }

    /* relase all the memory */
    GinBufferFree(buffer);

    tuplesort_end(worker_sort);
}

/*
 * Perform a worker's portion of a parallel GIN index build sort.
 *
 * This generates a tuplesort for the worker portion of the table.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 *
 * Before feeding data into a shared tuplesort (for the leader process),
 * the workers process data in two phases.
 *
 * 1) A worker reads a portion of rows from the table, accumulates entries
 * in memory, and flushes them into a private tuplesort (e.g. because of
 * using too much memory).
 *
 * 2) The private tuplesort gets sorted (by key and TID), the worker reads
 * the data again, and combines the entries as much as possible. This has
 * to happen eventually, and this way it's done in workers in parallel.
 *
 * Finally, the combined entries are written into the shared tuplesort, so
 * that the leader can process them.
 *
 * How well this works (compared to just writing entries into the shared
 * tuplesort) depends on the data set. For large tables with many distinct
 * keys this helps a lot. With many distinct keys it's likely the buffers has
 * to be flushed often, generating many entries with the same key and short
 * TID lists. These entries need to be sorted and merged at some point,
 * before writing them to the index. The merging is quite expensive, it can
 * easily be ~50% of a serial build, and doing as much of it in the workers
 * means it's parallelized. The leader still has to merge results from the
 * workers, but it's much more efficient to merge few large entries than
 * many tiny ones.
 *
 * This also reduces the amount of data the workers pass to the leader through
 * the shared tuplesort. OTOH the workers need more space for the private sort,
 * possibly up to 2x of the data, if no entries be merged in a worker. But this
 * is very unlikely, and the only consequence is inefficiency, so we ignore it.
 */
unsafe fn _gin_parallel_scan_and_build(state: *mut GinBuildState,
                                       ginshared: *mut GinBuildShared, sharedsort: *mut Sharedsort,
                                       heap: Relation, index: Relation,
                                       sortmem: c_int, progress: bool) {
    let coordinate: SortCoordinate;
    let scan: TableScanDesc;
    let reltuples: f64;
    let indexInfo: *mut IndexInfo;

    /* Initialize local tuplesort coordination state */
    coordinate = palloc0(core::mem::size_of::<SortCoordinateData>()) as SortCoordinate;
    (*coordinate).isWorker = true;
    (*coordinate).nParticipants = -1;
    (*coordinate).sharedsort = sharedsort;

    /* remember how much space is allowed for the accumulated entries */
    (*state).work_mem = sortmem / 2;

    /* remember how many workers participate in the build */
    (*state).bs_num_workers = (*ginshared).scantuplesortstates;

    /* Begin "partial" tuplesort */
    (*state).bs_sortstate = tuplesort_begin_index_gin(heap, index,
                                                      (*state).work_mem,
                                                      coordinate,
                                                      TUPLESORT_NONE);

    /* Local per-worker sort of raw-data */
    (*state).bs_worker_sort = tuplesort_begin_index_gin(heap, index,
                                                        (*state).work_mem,
                                                        null_mut(),
                                                        TUPLESORT_NONE);

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(index);
    (*indexInfo).ii_Concurrent = (*ginshared).isconcurrent;

    scan = table_beginscan_parallel(heap,
                                    ParallelTableScanFromGinBuildShared(ginshared));

    reltuples = table_index_build_scan(heap, index, indexInfo, true, progress,
                                       ginBuildCallbackParallel, state as *mut c_void, scan);

    /* write remaining accumulated entries */
    ginFlushBuildState(state, index);

    /*
     * Do the first phase of in-worker processing - sort the data produced by
     * the callback, and combine them into much larger chunks and place that
     * into the shared tuplestore for leader to process.
     */
    _gin_process_worker_data(state, (*state).bs_worker_sort, progress);

    /* sort the GIN tuples built by this worker */
    tuplesort_performsort((*state).bs_sortstate);

    (*state).bs_reltuples += reltuples;

    /*
     * Done.  Record ambuild statistics.
     */
    SpinLockAcquire(&mut (*ginshared).mutex);
    (*ginshared).nparticipantsdone += 1;
    (*ginshared).reltuples += (*state).bs_reltuples;
    (*ginshared).indtuples += (*state).bs_numtuples;
    SpinLockRelease(&mut (*ginshared).mutex);

    /* Notify leader */
    ConditionVariableSignal(&mut (*ginshared).workersdonecv);

    tuplesort_end((*state).bs_sortstate);
}

/*
 * Perform work within a launched parallel process.
 */
pub unsafe fn _gin_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {
    let sharedquery: *mut c_char;
    let ginshared: *mut GinBuildShared;
    let sharedsort: *mut Sharedsort;
    let mut buildstate: GinBuildState = core::mem::zeroed();
    let heapRel: Relation;
    let indexRel: Relation;
    let heapLockmode: LOCKMODE;
    let indexLockmode: LOCKMODE;
    let walusage: *mut WalUsage;
    let bufferusage: *mut BufferUsage;
    let sortmem: c_int;

    /*
     * The only possible status flag that can be set to the parallel worker is
     * PROC_IN_SAFE_IC.
     */
    Assert!(((*MyProc).statusFlags == 0) ||
            ((*MyProc).statusFlags == PROC_IN_SAFE_IC));

    /* Set debug_query_string for individual workers first */
    sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true) as *mut c_char;
    debug_query_string = sharedquery;

    /* Report the query string from leader */
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    /* Look up gin shared state */
    ginshared = shm_toc_lookup(toc, PARALLEL_KEY_GIN_SHARED, false) as *mut GinBuildShared;

    /* Open relations using lock modes known to be obtained by index.c */
    if !(*ginshared).isconcurrent {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    /* Open relations within worker */
    heapRel = table_open((*ginshared).heaprelid, heapLockmode as c_int);
    indexRel = index_open((*ginshared).indexrelid, indexLockmode as c_int);

    /* initialize the GIN build state */
    initGinState(&mut buildstate.ginstate, indexRel);
    buildstate.indtuples = 0.0;
    memset(&mut buildstate.buildStats as *mut GinStatsData as *mut c_void, 0, core::mem::size_of::<GinStatsData>());
    memset(&mut buildstate.tid as *mut ItemPointerData as *mut c_void, 0, core::mem::size_of::<ItemPointerData>());

    /*
     * create a temporary memory context that is used to hold data not yet
     * dumped out to the index
     */
    buildstate.tmpCtx = AllocSetContextCreate!(CurrentMemoryContext,
                                               "Gin build temporary context",
                                               ALLOCSET_DEFAULT_SIZES);

    /*
     * create a temporary memory context that is used for calling
     * ginExtractEntries(), and can be reset after each tuple
     */
    buildstate.funcCtx = AllocSetContextCreate!(CurrentMemoryContext,
                                                "Gin build temporary context for user-defined function",
                                                ALLOCSET_DEFAULT_SIZES);

    buildstate.accum.ginstate = &mut buildstate.ginstate;
    ginInitBA(&mut buildstate.accum);


    /* Look up shared state private to tuplesort.c */
    sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false) as *mut Sharedsort;
    tuplesort_attach_shared(sharedsort, seg);

    /* Prepare to track buffer usage during parallel execution */
    InstrStartParallelQuery();

    /*
     * Might as well use reliable figure when doling out maintenance_work_mem
     * (when requested number of workers were not launched, this will be
     * somewhat higher than it is for other workers).
     */
    sortmem = maintenance_work_mem / (*ginshared).scantuplesortstates;

    _gin_parallel_scan_and_build(&mut buildstate, ginshared, sharedsort,
                                 heapRel, indexRel, sortmem, false);

    /* Report WAL/buffer usage during parallel execution */
    bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false) as *mut BufferUsage;
    walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false) as *mut WalUsage;
    InstrEndParallelQuery(bufferusage.add(ParallelWorkerNumber as usize),
                          walusage.add(ParallelWorkerNumber as usize));

    index_close(indexRel, indexLockmode as c_int);
    table_close(heapRel, heapLockmode as c_int);
}

/*
 * Used to keep track of compressed TID lists when building a GIN tuple.
 */
#[repr(C)]
pub struct GinSegmentInfo {
    pub node: dlist_node,           /* linked list pointers */
    pub seg: *mut GinPostingList,
}

/*
 * _gin_build_tuple
 *		Serialize the state for an index key into a tuple for tuplesort.
 *
 * The tuple has a number of scalar fields (mostly matching the build state),
 * and then a data array that stores the key first, and then the TID list.
 *
 * For by-reference data types, we store the actual data. For by-val types
 * we simply copy the whole Datum, so that we don't have to care about stuff
 * like endianess etc. We could make it a little bit smaller, but it's not
 * worth it - it's a tiny fraction of the data, and we need to MAXALIGN the
 * start of the TID list anyway. So we wouldn't save anything.
 *
 * The TID list is serialized as compressed - it's highly compressible, and
 * we already have ginCompressPostingList for this purpose. The list may be
 * pretty long, so we compress it into multiple segments and then copy all
 * of that into the GIN tuple.
 */
unsafe fn _gin_build_tuple(attrnum: OffsetNumber, category: c_uchar,
                           key: Datum, typlen: int16, typbyval: bool,
                           items: *mut ItemPointerData, nitems: uint32,
                           len: *mut Size) -> *mut GinTuple {
    let tuple: *mut GinTuple;
    let mut ptr: *mut c_char;

    let tuplen: Size;
    let keylen: c_int;

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    let mut segments: dlist_head = core::mem::zeroed();
    let mut ncompressed: c_int;
    let mut compresslen: Size;

    /*
     * Calculate how long is the key value. Only keys with GIN_CAT_NORM_KEY
     * have actual non-empty key. We include varlena headers and \0 bytes for
     * strings, to make it easier to access the data in-line.
     *
     * For byval types we simply copy the whole Datum. We could store just the
     * necessary bytes, but this is simpler to work with and not worth the
     * extra complexity. Moreover we still need to do the MAXALIGN to allow
     * direct access to items pointers.
     *
     * XXX Note that for byval types we store the whole datum, no matter what
     * the typlen value is.
     */
    if category != GIN_CAT_NORM_KEY {
        keylen = 0;
    } else if typbyval {
        keylen = core::mem::size_of::<Datum>() as c_int;
    } else if typlen > 0 {
        keylen = typlen as c_int;
    } else if typlen == -1 {
        keylen = VARSIZE_ANY(DatumGetPointer(key)) as c_int;
    } else if typlen == -2 {
        keylen = (strlen(DatumGetPointer(key)) + 1) as c_int;
    } else {
        elog!(ERROR, "unexpected typlen value ({})", typlen);
        keylen = 0;
    }

    /* compress the item pointers */
    ncompressed = 0;
    compresslen = 0;
    dlist_init(&mut segments);

    /* generate compressed segments of TID list chunks */
    while (ncompressed as uint32) < nitems {
        let mut cnt: c_int = 0;
        let seginfo: *mut GinSegmentInfo = palloc(core::mem::size_of::<GinSegmentInfo>()) as *mut GinSegmentInfo;

        (*seginfo).seg = ginCompressPostingList(items.add(ncompressed as usize),
                                                (nitems as c_int - ncompressed),
                                                UINT16_MAX,
                                                &mut cnt);

        ncompressed += cnt;
        compresslen += SizeOfGinPostingList((*seginfo).seg);

        dlist_push_tail(&mut segments, &mut (*seginfo).node);
    }

    /*
     * Determine GIN tuple length with all the data included. Be careful about
     * alignment, to allow direct access to compressed segments (those require
     * only SHORTALIGN).
     */
    tuplen = SHORTALIGN(core::mem::offset_of!(GinTuple, data) + keylen as usize) + compresslen;

    *len = tuplen;

    /*
     * Allocate space for the whole GIN tuple.
     *
     * The palloc0 is needed - writetup_index_gin will write the whole tuple
     * to disk, so we need to make sure the padding bytes are defined
     * (otherwise valgrind would report this).
     */
    tuple = palloc0(tuplen) as *mut GinTuple;

    (*tuple).tuplen = tuplen as c_int;
    (*tuple).attrnum = attrnum;
    (*tuple).category = category as i8;
    (*tuple).keylen = keylen as uint16;
    (*tuple).nitems = nitems as c_int;

    /* key type info */
    (*tuple).typlen = typlen;
    (*tuple).typbyval = typbyval;

    /*
     * Copy the key and items into the tuple. First the key value, which we
     * can simply copy right at the beginning of the data array.
     */
    if category == GIN_CAT_NORM_KEY {
        if typbyval {
            memcpy((*tuple).data.as_mut_ptr() as *mut c_void, &key as *const Datum as *const c_void, core::mem::size_of::<Datum>());
        } else if typlen > 0 {	/* byref, fixed length */
            memcpy((*tuple).data.as_mut_ptr() as *mut c_void, DatumGetPointer(key) as *const c_void, typlen as usize);
        } else if typlen == -1 {
            memcpy((*tuple).data.as_mut_ptr() as *mut c_void, DatumGetPointer(key) as *const c_void, keylen as usize);
        } else if typlen == -2 {
            memcpy((*tuple).data.as_mut_ptr() as *mut c_void, DatumGetPointer(key) as *const c_void, keylen as usize);
        }
    }

    /* finally, copy the TIDs into the array */
    ptr = (tuple as *mut c_char).add(SHORTALIGN(core::mem::offset_of!(GinTuple, data) + keylen as usize));

    /* copy in the compressed data, and free the segments */
    crate::dlist_foreach_modify!(iter, &mut segments, {
        let seginfo: *mut GinSegmentInfo = crate::dlist_container!(GinSegmentInfo, node, iter.cur);

        memcpy(ptr as *mut c_void, (*seginfo).seg as *const c_void, SizeOfGinPostingList((*seginfo).seg));

        ptr = ptr.add(SizeOfGinPostingList((*seginfo).seg));

        dlist_delete(&mut (*seginfo).node);

        pfree((*seginfo).seg as *mut c_void);
        pfree(seginfo as *mut c_void);
    });

    return tuple;
}

/*
 * _gin_parse_tuple_key
 *		Return a Datum representing the key stored in the tuple.
 *
 * Most of the tuple fields are directly accessible, the only thing that
 * needs more care is the key and the TID list.
 *
 * For the key, this returns a regular Datum representing it. It's either the
 * actual key value, or a pointer to the beginning of the data array (which is
 * where the data was copied by _gin_build_tuple).
 */
unsafe fn _gin_parse_tuple_key(a: *mut GinTuple) -> Datum {
    let mut key: Datum = 0;

    if (*a).category != GIN_CAT_NORM_KEY as GinNullCategory {
        return 0 as Datum;
    }

    if (*a).typbyval {
        memcpy(&mut key as *mut Datum as *mut c_void, (*a).data.as_ptr() as *const c_void, (*a).keylen as usize);
        return key;
    }

    return PointerGetDatum((*a).data.as_ptr() as *const c_void);
}

/*
* _gin_parse_tuple_items
 *		Return a pointer to a palloc'd array of decompressed TID array.
 */
unsafe fn _gin_parse_tuple_items(a: *mut GinTuple) -> ItemPointer {
    let len: c_int;
    let ptr: *mut c_char;
    let mut ndecoded: c_int = 0;
    let items: *mut ItemPointerData;

    len = (*a).tuplen - SHORTALIGN(core::mem::offset_of!(GinTuple, data) + (*a).keylen as usize) as c_int;
    ptr = (a as *mut c_char).add(SHORTALIGN(core::mem::offset_of!(GinTuple, data) + (*a).keylen as usize));

    items = ginPostingListDecodeAllSegments(ptr as *mut GinPostingList, len, &mut ndecoded);

    Assert!(ndecoded == (*a).nitems);

    return items as ItemPointer;
}

/*
 * _gin_compare_tuples
 *		Compare GIN tuples, used by tuplesort during parallel index build.
 *
 * The scalar fields (attrnum, category) are compared first, the key value is
 * compared last. The comparisons are done using type-specific sort support
 * functions.
 *
 * If the key value matches, we compare the first TID value in the TID list,
 * which means the tuples are merged in an order in which they are most
 * likely to be simply concatenated. (This "first" TID will also allow us
 * to determine a point up to which the list is fully determined and can be
 * written into the index to enforce a memory limit etc.)
 */
pub unsafe fn _gin_compare_tuples(a: *mut GinTuple, b: *mut GinTuple, ssup: SortSupport) -> c_int {
    let r: c_int;
    let keya: Datum;
    let keyb: Datum;

    if (*a).attrnum < (*b).attrnum {
        return -1;
    }

    if (*a).attrnum > (*b).attrnum {
        return 1;
    }

    if (*a).category < (*b).category {
        return -1;
    }

    if (*a).category > (*b).category {
        return 1;
    }

    if (*a).category == GIN_CAT_NORM_KEY as GinNullCategory {
        keya = _gin_parse_tuple_key(a);
        keyb = _gin_parse_tuple_key(b);

        r = ApplySortComparator(keya, false,
                                keyb, false,
                                ssup.add(((*a).attrnum - 1) as usize));

        /* if the key is the same, consider the first TID in the array */
        return if r != 0 { r } else { ItemPointerCompare(GinTupleGetFirst(a),
                                                         GinTupleGetFirst(b)) };
    }

    return ItemPointerCompare(GinTupleGetFirst(a),
                              GinTupleGetFirst(b));
}

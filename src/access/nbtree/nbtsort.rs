//! nbtsort.rs
//!   Build a btree from sorted input by loading leaf pages sequentially.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtsort.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtsort.c

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::access::common::indextuple::{
    CopyIndexTuple, IndexTuple, IndexTupleData, IndexTupleSize,
};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexTupleOverwrite, PageGetFreeSpace,
};
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdSetUnused, ItemIdGetLength};
use crate::storage::itemptr::{ItemPointer, ItemPointerCompare, ItemPointerData};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev};
use crate::utils::rel::Relation;
use crate::pg_config::BLCKSZ;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
}

// ---------------------------------------------------------------------------
// Magic numbers for parallel state sharing
// ---------------------------------------------------------------------------
pub const PARALLEL_KEY_BTREE_SHARED:     u64 = 0xA000000000000001;
pub const PARALLEL_KEY_TUPLESORT:        u64 = 0xA000000000000002;
pub const PARALLEL_KEY_TUPLESORT_SPOOL2: u64 = 0xA000000000000003;
pub const PARALLEL_KEY_QUERY_TEXT:       u64 = 0xA000000000000004;
pub const PARALLEL_KEY_WAL_USAGE:        u64 = 0xA000000000000005;
pub const PARALLEL_KEY_BUFFER_USAGE:     u64 = 0xA000000000000006;

/*
 * DISABLE_LEADER_PARTICIPATION disables the leader's participation in
 * parallel index builds.  This may be useful as a debugging aid.
#undef DISABLE_LEADER_PARTICIPATION
 */

// ---------------------------------------------------------------------------
// Stub types -- symbols whose real home has not been ported yet.
// ---------------------------------------------------------------------------

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

/// TODO(pg-port): IndexBuildResult (access/genam.h)
#[repr(C)]
pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

/// TODO(pg-port): IndexInfo (nodes/execnodes.h)
#[repr(C)]
pub struct IndexInfo {
    pub ii_Unique: bool,
    pub ii_NullsNotDistinct: bool,
    pub ii_Concurrent: bool,
    pub ii_ParallelWorkers: c_int,
    pub ii_BrokenHotChain: bool,
}

/// TODO(pg-port): BulkWriteBuffer (storage/bulk_write.h)
pub type BulkWriteBuffer = *mut c_void;

/// TODO(pg-port): BulkWriteState (storage/bulk_write.h)
pub enum BulkWriteState {}

/// TODO(pg-port): BTScanInsert (access/nbtree.h)
#[repr(C)]
pub struct BTScanInsertData {
    pub allequalimage: bool,
    pub scankeys: *mut ScanKeyData,
    // (other fields omitted; TODO(pg-port))
}
pub type BTScanInsert = *mut BTScanInsertData;

/// TODO(pg-port): ScanKeyData (access/skey.h)
#[repr(C)]
pub struct ScanKeyData {
    pub sk_flags: c_int,
    pub sk_attno: i16,
    pub sk_collation: u32,
    // (other fields omitted; TODO(pg-port))
}

/// TODO(pg-port): SortSupportData (utils/sortsupport.h)
#[repr(C)]
pub struct SortSupportData {
    pub ssup_cxt: MemoryContext,
    pub ssup_collation: u32,
    pub ssup_nulls_first: bool,
    pub ssup_attno: i16,
    pub abbreviate: bool,
    // (other fields omitted; TODO(pg-port))
}
pub type SortSupport = *mut SortSupportData;

/// TODO(pg-port): MemoryContext
pub type MemoryContext = *mut c_void;

/// TODO(pg-port): TupleDesc (access/tupdesc.h)
pub type TupleDesc = *mut c_void;

/// TODO(pg-port): Datum
pub type Datum = usize;

/// TODO(pg-port): TableScanDesc (access/relscan.h)
pub type TableScanDesc = *mut c_void;

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

/// TODO(pg-port): Oid
pub type Oid = u32;

// ---------------------------------------------------------------------------
// BTDedupState stub (duplicated from nbtdedup.rs pattern)
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTDedupStateData (access/nbtree.h)
#[repr(C)]
pub struct BTDedupStateData {
    pub deduplicate: bool,
    pub nmaxitems: c_int,
    pub maxpostingsize: Size,
    pub base: IndexTuple,
    pub baseoff: OffsetNumber,
    pub basetupsize: Size,
    pub htids: ItemPointer,
    pub nhtids: c_int,
    pub nitems: c_int,
    pub phystupsize: Size,
    pub nintervals: c_int,
}
pub type BTDedupState = *mut BTDedupStateData;

// ---------------------------------------------------------------------------
// BTPageOpaque stub
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTPageOpaqueData (access/nbtree.h)
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev: BlockNumber,
    pub btpo_next: BlockNumber,
    pub btpo_level: u32,
    pub btpo_flags: u16,
    pub btpo_cycleid: u16,
}
pub type BTPageOpaque = *mut BTPageOpaqueData;

// ---------------------------------------------------------------------------
// nbtree.h constants / macros (stubs)
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTMaxItemSize (access/nbtree.h)
pub const BTMaxItemSize: Size = 1128;

/// TODO(pg-port): BTREE_METAPAGE (access/nbtree.h)
pub const BTREE_METAPAGE: BlockNumber = 0;

/// TODO(pg-port): BTREE_NONLEAF_FILLFACTOR (access/nbtree.h)
pub const BTREE_NONLEAF_FILLFACTOR: c_int = 70;

/// TODO(pg-port): P_NONE (access/nbtree.h)
pub const P_NONE: BlockNumber = 0;

/// TODO(pg-port): P_HIKEY (access/nbtree.h)
pub const P_HIKEY: OffsetNumber = 1;

/// TODO(pg-port): P_FIRSTKEY (access/nbtree.h)
pub const P_FIRSTKEY: OffsetNumber = 2;

/// TODO(pg-port): BTP_LEAF (access/nbtree.h)
pub const BTP_LEAF: u16 = 1 << 0;

/// TODO(pg-port): BTP_ROOT (access/nbtree.h)
pub const BTP_ROOT: u16 = 1 << 1;

/// TODO(pg-port): TUPLESORT_NONE (utils/tuplesort.h)
pub const TUPLESORT_NONE: c_int = 0;

/// TODO(pg-port): PROGRESS_CREATEIDX_SUBPHASE (commands/progress.h)
pub const PROGRESS_CREATEIDX_SUBPHASE: c_int = 0;
/// TODO(pg-port): PROGRESS_CREATEIDX_TUPLES_TOTAL (commands/progress.h)
pub const PROGRESS_CREATEIDX_TUPLES_TOTAL: c_int = 0;
/// TODO(pg-port): PROGRESS_CREATEIDX_TUPLES_DONE (commands/progress.h)
pub const PROGRESS_CREATEIDX_TUPLES_DONE: c_int = 0;
/// TODO(pg-port): PROGRESS_SCAN_BLOCKS_TOTAL (commands/progress.h)
pub const PROGRESS_SCAN_BLOCKS_TOTAL: c_int = 0;
/// TODO(pg-port): PROGRESS_SCAN_BLOCKS_DONE (commands/progress.h)
pub const PROGRESS_SCAN_BLOCKS_DONE: c_int = 0;
/// TODO(pg-port): PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN (commands/progress.h)
pub const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN: i64 = 0;
/// TODO(pg-port): PROGRESS_BTREE_PHASE_PERFORMSORT_1 (commands/progress.h)
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_1: i64 = 0;
/// TODO(pg-port): PROGRESS_BTREE_PHASE_PERFORMSORT_2 (commands/progress.h)
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_2: i64 = 0;
/// TODO(pg-port): PROGRESS_BTREE_PHASE_LEAF_LOAD (commands/progress.h)
pub const PROGRESS_BTREE_PHASE_LEAF_LOAD: i64 = 0;

/// TODO(pg-port): MAIN_FORKNUM (common/relpath.h)
pub const MAIN_FORKNUM: c_int = 0;

/// TODO(pg-port): WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN (utils/wait_event_types.h)
pub const WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN: u32 = 0;

/// TODO(pg-port): PROC_IN_SAFE_IC (storage/proc.h)
pub const PROC_IN_SAFE_IC: u32 = 0x02;

/// TODO(pg-port): ShareLock (storage/lock.h)
pub const ShareLock: LOCKMODE = 5;
/// TODO(pg-port): AccessExclusiveLock (storage/lock.h)
pub const AccessExclusiveLock: LOCKMODE = 8;
/// TODO(pg-port): ShareUpdateExclusiveLock (storage/lock.h)
pub const ShareUpdateExclusiveLock: LOCKMODE = 3;
/// TODO(pg-port): RowExclusiveLock (storage/lock.h)
pub const RowExclusiveLock: LOCKMODE = 4;

/// TODO(pg-port): STATE_RUNNING (pgstat.h)
pub const STATE_RUNNING: c_int = 0;

/// TODO(pg-port): SK_BT_NULLS_FIRST (access/nbtree.h)
pub const SK_BT_NULLS_FIRST: c_int = 0x0040;

/// TODO(pg-port): SK_BT_DESC (access/nbtree.h)
pub const SK_BT_DESC: c_int = 0x0020;

// ---------------------------------------------------------------------------
// Stub functions
// ---------------------------------------------------------------------------

/// TODO(pg-port): access/nbtree.h -- BTPageGetOpaque
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- P_LEFTMOST
unsafe fn P_LEFTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTGetTargetPageFreeSpace
unsafe fn BTGetTargetPageFreeSpace(index: Relation) -> Size {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTGetDeduplicateItems
unsafe fn BTGetDeduplicateItems(index: Relation) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTreeTupleGetNAtts
unsafe fn BTreeTupleGetNAtts(itup: IndexTuple, index: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTreeTupleSetNAtts
unsafe fn BTreeTupleSetNAtts(itup: IndexTuple, natts: c_int, has_heap_tid: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTreeTupleSetDownLink
unsafe fn BTreeTupleSetDownLink(itup: IndexTuple, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- BTreeTupleGetPostingOffset
unsafe fn BTreeTupleGetPostingOffset(posting: IndexTuple) -> u32 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_mkscankey
unsafe fn _bt_mkscankey(index: Relation, itup: IndexTuple) -> BTScanInsert {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_allequalimage
unsafe fn _bt_allequalimage(index: Relation, is_build: bool) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_truncate
unsafe fn _bt_truncate(
    index: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
    itup_key: BTScanInsert,
) -> IndexTuple {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_check_third_page
unsafe fn _bt_check_third_page(
    index: Relation,
    heap: Relation,
    is_leaf: bool,
    page: Page,
    itup: IndexTuple,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_pageinit
unsafe fn _bt_pageinit(page: Page, size: Size) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_initmetapage
unsafe fn _bt_initmetapage(page: Page, rootblkno: BlockNumber, rootlevel: u32, allequalimage: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_dedup_start_pending
unsafe fn _bt_dedup_start_pending(
    state: BTDedupState,
    base: IndexTuple,
    baseoff: OffsetNumber,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_dedup_save_htid
unsafe fn _bt_dedup_save_htid(state: BTDedupState, itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_form_posting
unsafe fn _bt_form_posting(
    base: IndexTuple,
    htids: ItemPointer,
    nhtids: c_int,
) -> IndexTuple {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h -- _bt_keep_natts_fast
unsafe fn _bt_keep_natts_fast(
    index: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/genam.h -- IndexRelationGetNumberOfKeyAttributes
unsafe fn IndexRelationGetNumberOfKeyAttributes(index: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/genam.h -- index_getattr
unsafe fn index_getattr(
    tup: IndexTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h -- RelationGetNumberOfBlocks
unsafe fn RelationGetNumberOfBlocks(rel: Relation) -> BlockNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h -- RelationGetRelationName
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h -- RelationGetRelid
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h -- RelationGetDescr
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_begin_index_btree
unsafe fn tuplesort_begin_index_btree(
    heap: Relation,
    index: Relation,
    isunique: bool,
    nulls_not_distinct: bool,
    sortmem: c_int,
    coordinate: SortCoordinate,
    flags: c_int,
) -> *mut Tuplesortstate {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_performsort
unsafe fn tuplesort_performsort(state: *mut Tuplesortstate) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_getindextuple
unsafe fn tuplesort_getindextuple(
    state: *mut Tuplesortstate,
    forward: bool,
) -> IndexTuple {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_end
unsafe fn tuplesort_end(state: *mut Tuplesortstate) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_putindextuplevalues
unsafe fn tuplesort_putindextuplevalues(
    state: *mut Tuplesortstate,
    index: Relation,
    self_: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_estimate_shared
unsafe fn tuplesort_estimate_shared(nworkers: c_int) -> Size {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_initialize_shared
unsafe fn tuplesort_initialize_shared(
    shared: *mut Sharedsort,
    nworkers: c_int,
    seg: *mut c_void,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/tuplesort.h -- tuplesort_attach_shared
unsafe fn tuplesort_attach_shared(shared: *mut Sharedsort, seg: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/sortsupport.h -- PrepareSortSupportFromIndexRel
unsafe fn PrepareSortSupportFromIndexRel(index: Relation, reverse: bool, ssup: SortSupport) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/sortsupport.h -- ApplySortComparator
unsafe fn ApplySortComparator(
    datum1: Datum,
    is_null1: bool,
    datum2: Datum,
    is_null2: bool,
    ssup: SortSupport,
) -> i32 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bulk_write.h -- smgr_bulk_start_rel
unsafe fn smgr_bulk_start_rel(index: Relation, forknum: c_int) -> *mut BulkWriteState {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bulk_write.h -- smgr_bulk_get_buf
unsafe fn smgr_bulk_get_buf(state: *mut BulkWriteState) -> BulkWriteBuffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bulk_write.h -- smgr_bulk_write
unsafe fn smgr_bulk_write(
    state: *mut BulkWriteState,
    blkno: BlockNumber,
    buf: BulkWriteBuffer,
    is_main_fork: bool,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bulk_write.h -- smgr_bulk_finish
unsafe fn smgr_bulk_finish(state: *mut BulkWriteState) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufpage.h -- PageHeader cast helper
unsafe fn PageHeaderPdLower(page: Page) -> *mut u16 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_index_build_scan
unsafe fn table_index_build_scan(
    heap: Relation,
    index: Relation,
    index_info: *mut IndexInfo,
    allow_sync: bool,
    progress: bool,
    callback: unsafe extern "C" fn(
        Relation,
        ItemPointer,
        *mut Datum,
        *mut bool,
        bool,
        *mut c_void,
    ),
    callback_state: *mut c_void,
    scan: TableScanDesc,
) -> f64 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_open
unsafe fn table_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_close
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_parallelscan_estimate
unsafe fn table_parallelscan_estimate(heap: Relation, snapshot: Snapshot) -> Size {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_parallelscan_initialize
unsafe fn table_parallelscan_initialize(
    heap: Relation,
    target: ParallelTableScanDesc,
    snapshot: Snapshot,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/table.h -- table_beginscan_parallel
unsafe fn table_beginscan_parallel(
    heap: Relation,
    pscan: ParallelTableScanDesc,
) -> TableScanDesc {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): catalog/index.h -- index_open
unsafe fn index_open(relid: Oid, lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): catalog/index.h -- index_close
unsafe fn index_close(rel: Relation, lockmode: LOCKMODE) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): catalog/index.h -- BuildIndexInfo
unsafe fn BuildIndexInfo(index: Relation) -> *mut IndexInfo {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- EnterParallelMode
unsafe fn EnterParallelMode() {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- ExitParallelMode
unsafe fn ExitParallelMode() {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- CreateParallelContext
unsafe fn CreateParallelContext(
    library_name: *const c_char,
    function_name: *const c_char,
    nworkers: c_int,
) -> *mut ParallelContext {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- InitializeParallelDSM
unsafe fn InitializeParallelDSM(pcxt: *mut ParallelContext) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- LaunchParallelWorkers
unsafe fn LaunchParallelWorkers(pcxt: *mut ParallelContext) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- WaitForParallelWorkersToAttach
unsafe fn WaitForParallelWorkersToAttach(pcxt: *mut ParallelContext) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- WaitForParallelWorkersToFinish
unsafe fn WaitForParallelWorkersToFinish(pcxt: *mut ParallelContext) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- DestroyParallelContext
unsafe fn DestroyParallelContext(pcxt: *mut ParallelContext) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_estimate_chunk
unsafe fn shm_toc_estimate_chunk(estimator: *mut shm_toc_estimator, size: Size) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_estimate_keys
unsafe fn shm_toc_estimate_keys(estimator: *mut shm_toc_estimator, n: c_int) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_allocate
unsafe fn shm_toc_allocate(toc: *mut c_void, size: Size) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_insert
unsafe fn shm_toc_insert(toc: *mut c_void, key: u64, address: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/shm_toc.h -- shm_toc_lookup
unsafe fn shm_toc_lookup(toc: *mut c_void, key: u64, noError: bool) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/xact.h -- GetTransactionSnapshot
unsafe fn GetTransactionSnapshot() -> Snapshot {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/snapmgr.h -- RegisterSnapshot
unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/snapmgr.h -- UnregisterSnapshot
unsafe fn UnregisterSnapshot(snapshot: Snapshot) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/snapmgr.h -- IsMVCCSnapshot
unsafe fn IsMVCCSnapshot(snapshot: Snapshot) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/parallel.h -- SnapshotAny
pub const SnapshotAny: Snapshot = core::ptr::null_mut();
/// TODO(pg-port): storage/spin.h -- SpinLockInit
unsafe fn SpinLockInit(lock: *mut slock_t) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/spin.h -- SpinLockAcquire
unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/spin.h -- SpinLockRelease
unsafe fn SpinLockRelease(lock: *mut slock_t) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableInit
unsafe fn ConditionVariableInit(cv: *mut ConditionVariable) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableSleep
unsafe fn ConditionVariableSleep(cv: *mut ConditionVariable, wait_event: u32) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableCancelSleep
unsafe fn ConditionVariableCancelSleep() {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/condition_variable.h -- ConditionVariableSignal
unsafe fn ConditionVariableSignal(cv: *mut ConditionVariable) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): pgstat.h -- pgstat_progress_update_param
unsafe fn pgstat_progress_update_param(index: c_int, val: i64) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): pgstat.h -- pgstat_progress_update_multi_param
unsafe fn pgstat_progress_update_multi_param(
    n: c_int,
    params: *const c_int,
    vals: *const i64,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): pgstat.h -- pgstat_get_my_query_id
unsafe fn pgstat_get_my_query_id() -> i64 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): pgstat.h -- pgstat_report_activity
unsafe fn pgstat_report_activity(state: c_int, cmd_str: *const c_char) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): pgstat.h -- pgstat_report_query_id
unsafe fn pgstat_report_query_id(query_id: i64, set: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): executor/instrument.h -- InstrStartParallelQuery
unsafe fn InstrStartParallelQuery() {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): executor/instrument.h -- InstrEndParallelQuery
unsafe fn InstrEndParallelQuery(
    bufusage: *mut BufferUsage,
    walusage: *mut WalUsage,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): executor/instrument.h -- InstrAccumParallelQuery
unsafe fn InstrAccumParallelQuery(
    bufusage: *mut BufferUsage,
    walusage: *mut WalUsage,
) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): miscadmin.h -- maintenance_work_mem
pub static mut maintenance_work_mem: c_int = 65536;
/// TODO(pg-port): miscadmin.h -- work_mem
pub static mut work_mem: c_int = 4096;
/// TODO(pg-port): miscadmin.h -- CHECK_FOR_INTERRUPTS
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {
        // TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS
    };
}
/// TODO(pg-port): tcop/tcopprot.h -- debug_query_string
pub static mut debug_query_string: *const c_char = core::ptr::null();
/// TODO(pg-port): utils/memutils.h -- CurrentMemoryContext
pub static mut CurrentMemoryContext: MemoryContext = core::ptr::null_mut();
/// TODO(pg-port): storage/proc.h -- MyProc
pub static mut MyProc: *mut ProcStruct = core::ptr::null_mut();
/// TODO(pg-port): storage/proc.h -- ProcStruct (partial)
#[repr(C)]
pub struct ProcStruct {
    pub statusFlags: u32,
}
/// TODO(pg-port): access/parallel.h -- ParallelWorkerNumber
pub static mut ParallelWorkerNumber: c_int = 0;
/// TODO(pg-port): utils/pg_rusage.h -- log_btree_build_stats
pub static mut log_btree_build_stats: bool = false;

/// TODO(pg-port): storage/bufpage.h -- BUFFERALIGN
macro_rules! BUFFERALIGN {
    ($x:expr) => {
        (($x) + 7) & !7
    };
}

/// TODO(pg-port): utils/memutils.h -- add_size
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1.saturating_add(s2)
}
/// TODO(pg-port): utils/memutils.h -- mul_size
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1.saturating_mul(s2)
}

/// TODO(pg-port): ParallelTableScanFromBTShared -- macro
/// c.f. shm_toc_allocate as to why BUFFERALIGN is used, rather than just MAXALIGN.
unsafe fn ParallelTableScanFromBTShared(shared: *mut BTShared) -> ParallelTableScanDesc {
    (shared as *mut c_char).add(BUFFERALIGN!(size_of::<BTShared>())) as ParallelTableScanDesc
}

// ---------------------------------------------------------------------------
// Status record for spooling/sorting phase.
// (Note we may have two of these due to the special requirements for
// uniqueness-checking with dead tuples.)
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct BTSpool {
    pub sortstate: *mut Tuplesortstate, /* state data for tuplesort.c */
    pub heap: Relation,
    pub index: Relation,
    pub isunique: bool,
    pub nulls_not_distinct: bool,
}

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
#[repr(C)]
pub struct BTShared {
    /*
     * These fields are not modified during the sort.  They primarily exist
     * for the benefit of worker processes that need to create BTSpool state
     * corresponding to that used by the leader.
     */
    pub heaprelid: Oid,
    pub indexrelid: Oid,
    pub isunique: bool,
    pub nulls_not_distinct: bool,
    pub isconcurrent: bool,
    pub scantuplesortstates: c_int,

    /* Query ID, for report in worker processes */
    pub queryid: i64,

    /*
     * workersdonecv is used to monitor the progress of workers.  All parallel
     * participants must indicate that they are done before leader can use
     * mutable state that workers maintain during scan (and before leader can
     * proceed to tuplesort_performsort()).
     */
    pub workersdonecv: ConditionVariable,

    /*
     * mutex protects all fields before heapdesc.
     *
     * These fields contain status information of interest to B-Tree index
     * builds that must work just the same when an index is built in parallel.
     */
    pub mutex: slock_t,

    /*
     * Mutable state that is maintained by workers, and reported back to
     * leader at end of parallel scan.
     *
     * nparticipantsdone is number of worker processes finished.
     *
     * reltuples is the total number of input heap tuples.
     *
     * havedead indicates if RECENTLY_DEAD tuples were encountered during
     * build.
     *
     * indtuples is the total number of tuples that made it into the index.
     *
     * brokenhotchain indicates if any worker detected a broken HOT chain
     * during build.
     */
    pub nparticipantsdone: c_int,
    pub reltuples: f64,
    pub havedead: bool,
    pub indtuples: f64,
    pub brokenhotchain: bool,

    /*
     * ParallelTableScanDescData data follows. Can't directly embed here, as
     * implementations of the parallel table scan desc interface might need
     * stronger alignment.
     */
}

/*
 * Status for leader in parallel index build.
 */
#[repr(C)]
pub struct BTLeader {
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
     * btshared is the shared state for entire build.  sharedsort is the
     * shared, tuplesort-managed state passed to each process tuplesort.
     * sharedsort2 is the corresponding btspool2 shared state, used only when
     * building unique indexes.  snapshot is the snapshot used by the scan iff
     * an MVCC snapshot is required.
     */
    pub btshared: *mut BTShared,
    pub sharedsort: *mut Sharedsort,
    pub sharedsort2: *mut Sharedsort,
    pub snapshot: Snapshot,
    pub walusage: *mut WalUsage,
    pub bufferusage: *mut BufferUsage,
}

/*
 * Working state for btbuild and its callback.
 *
 * When parallel CREATE INDEX is used, there is a BTBuildState for each
 * participant.
 */
#[repr(C)]
pub struct BTBuildState {
    pub isunique: bool,
    pub nulls_not_distinct: bool,
    pub havedead: bool,
    pub heap: Relation,
    pub spool: *mut BTSpool,

    /*
     * spool2 is needed only when the index is a unique index. Dead tuples are
     * put into spool2 instead of spool in order to avoid uniqueness check.
     */
    pub spool2: *mut BTSpool,
    pub indtuples: f64,

    /*
     * btleader is only present when a parallel index build is performed, and
     * only in the leader process. (Actually, only the leader has a
     * BTBuildState.  Workers have their own spool and spool2, though.)
     */
    pub btleader: *mut BTLeader,
}

/*
 * Status record for a btree page being built.  We have one of these
 * for each active tree level.
 */
#[repr(C)]
pub struct BTPageState {
    pub btps_buf: BulkWriteBuffer,      /* workspace for page building */
    pub btps_blkno: BlockNumber,        /* block # to write this page at */
    pub btps_lowkey: IndexTuple,        /* page's strict lower bound pivot tuple */
    pub btps_lastoff: OffsetNumber,     /* last item offset loaded */
    pub btps_lastextra: Size,           /* last item's extra posting list space */
    pub btps_level: u32,                /* tree level (0 = leaf) */
    pub btps_full: Size,                /* "full" if less than this much free space */
    pub btps_next: *mut BTPageState,    /* link to parent level, if any */
}

/*
 * Overall status record for index writing phase.
 */
#[repr(C)]
pub struct BTWriteState {
    pub heap: Relation,
    pub index: Relation,
    pub bulkstate: *mut BulkWriteState,
    pub inskey: BTScanInsert,       /* generic insertion scankey */
    pub btws_pages_alloced: BlockNumber, /* # pages allocated */
}


// ---------------------------------------------------------------------------
// btbuild() -- build a new btree index.
// ---------------------------------------------------------------------------

/*
 *  btbuild() -- build a new btree index.
 */
pub unsafe fn btbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let result: *mut IndexBuildResult;
    let mut buildstate: BTBuildState = core::mem::zeroed();
    let reltuples: f64;

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats)
    //     ResetUsage();
    // #endif /* BTREE_BUILD_STATS */

    buildstate.isunique = (*indexInfo).ii_Unique;
    buildstate.nulls_not_distinct = (*indexInfo).ii_NullsNotDistinct;
    buildstate.havedead = false;
    buildstate.heap = heap;
    buildstate.spool = null_mut();
    buildstate.spool2 = null_mut();
    buildstate.indtuples = 0.0;
    buildstate.btleader = null_mut();

    /*
     * We expect to be called exactly once for any index relation. If that's
     * not the case, big trouble's what we have.
     */
    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(
            ERROR,
            "index \"{}\" already contains data",
            core::ffi::CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
        );
    }

    reltuples = _bt_spools_heapscan(heap, index, &raw mut buildstate, indexInfo);

    /*
     * Finish the build by (1) completing the sort of the spool file, (2)
     * inserting the sorted tuples into btree pages and (3) building the upper
     * levels.  Finally, it may also be necessary to end use of parallelism.
     */
    _bt_leafbuild(buildstate.spool, buildstate.spool2);
    _bt_spooldestroy(buildstate.spool);
    if !buildstate.spool2.is_null() {
        _bt_spooldestroy(buildstate.spool2);
    }
    if !buildstate.btleader.is_null() {
        _bt_end_parallel(buildstate.btleader);
    }

    result = palloc(size_of::<IndexBuildResult>()) as *mut IndexBuildResult;

    (*result).heap_tuples = reltuples;
    (*result).index_tuples = buildstate.indtuples;

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats) {
    //     ShowUsage("BTREE BUILD STATS");
    //     ResetUsage();
    // }
    // #endif /* BTREE_BUILD_STATS */

    result
}

/*
 * Create and initialize one or two spool structures, and save them in caller's
 * buildstate argument.  May also fill-in fields within indexInfo used by index
 * builds.
 *
 * Scans the heap, possibly in parallel, filling spools with IndexTuples.  This
 * routine encapsulates all aspects of managing parallelism.  Caller need only
 * call _bt_end_parallel() in parallel case after it is done with spool/spool2.
 *
 * Returns the total number of heap tuples scanned.
 */
unsafe fn _bt_spools_heapscan(
    heap: Relation,
    index: Relation,
    buildstate: *mut BTBuildState,
    indexInfo: *mut IndexInfo,
) -> f64 {
    let btspool: *mut BTSpool = palloc0(size_of::<BTSpool>()) as *mut BTSpool;
    let mut coordinate: SortCoordinate = null_mut();
    let mut reltuples: f64 = 0.0;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel (see also: notes on
     * parallelism and maintenance_work_mem below).
     */
    (*btspool).heap = heap;
    (*btspool).index = index;
    (*btspool).isunique = (*indexInfo).ii_Unique;
    (*btspool).nulls_not_distinct = (*indexInfo).ii_NullsNotDistinct;

    /* Save as primary spool */
    (*buildstate).spool = btspool;

    /* Report table scan phase started */
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_SUBPHASE,
        PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN,
    );

    /* Attempt to launch parallel worker scan when required */
    if (*indexInfo).ii_ParallelWorkers > 0 {
        _bt_begin_parallel(
            buildstate,
            (*indexInfo).ii_Concurrent,
            (*indexInfo).ii_ParallelWorkers,
        );
    }

    /*
     * If parallel build requested and at least one worker process was
     * successfully launched, set up coordination state
     */
    if !(*buildstate).btleader.is_null() {
        coordinate = palloc0(size_of::<SortCoordinateData>()) as SortCoordinate;
        (*coordinate).isWorker = false;
        (*coordinate).nParticipants = (*(*buildstate).btleader).nparticipanttuplesorts;
        (*coordinate).sharedsort = (*(*buildstate).btleader).sharedsort;
    }

    /*
     * Begin serial/leader tuplesort.
     *
     * In cases where parallelism is involved, the leader receives the same
     * share of maintenance_work_mem as a serial sort (it is generally treated
     * in the same way as a serial sort once we return).  Parallel worker
     * Tuplesortstates will have received only a fraction of
     * maintenance_work_mem, though.
     *
     * We rely on the lifetime of the Leader Tuplesortstate almost not
     * overlapping with any worker Tuplesortstate's lifetime.  There may be
     * some small overlap, but that's okay because we rely on leader
     * Tuplesortstate only allocating a small, fixed amount of memory here.
     * When its tuplesort_performsort() is called (by our caller), and
     * significant amounts of memory are likely to be used, all workers must
     * have already freed almost all memory held by their Tuplesortstates
     * (they are about to go away completely, too).  The overall effect is
     * that maintenance_work_mem always represents an absolute high watermark
     * on the amount of memory used by a CREATE INDEX operation, regardless of
     * the use of parallelism or any other factor.
     */
    (*(*buildstate).spool).sortstate = tuplesort_begin_index_btree(
        heap,
        index,
        (*buildstate).isunique,
        (*buildstate).nulls_not_distinct,
        maintenance_work_mem,
        coordinate,
        TUPLESORT_NONE,
    );

    /*
     * If building a unique index, put dead tuples in a second spool to keep
     * them out of the uniqueness check.  We expect that the second spool (for
     * dead tuples) won't get very full, so we give it only work_mem.
     */
    if (*indexInfo).ii_Unique {
        let btspool2: *mut BTSpool = palloc0(size_of::<BTSpool>()) as *mut BTSpool;
        let mut coordinate2: SortCoordinate = null_mut();

        /* Initialize secondary spool */
        (*btspool2).heap = heap;
        (*btspool2).index = index;
        (*btspool2).isunique = false;
        /* Save as secondary spool */
        (*buildstate).spool2 = btspool2;

        if !(*buildstate).btleader.is_null() {
            /*
             * Set up non-private state that is passed to
             * tuplesort_begin_index_btree() about the basic high level
             * coordination of a parallel sort.
             */
            coordinate2 = palloc0(size_of::<SortCoordinateData>()) as SortCoordinate;
            (*coordinate2).isWorker = false;
            (*coordinate2).nParticipants = (*(*buildstate).btleader).nparticipanttuplesorts;
            (*coordinate2).sharedsort = (*(*buildstate).btleader).sharedsort2;
        }

        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem
         */
        (*(*buildstate).spool2).sortstate = tuplesort_begin_index_btree(
            heap,
            index,
            false,
            false,
            work_mem,
            coordinate2,
            TUPLESORT_NONE,
        );
    }

    /* Fill spool using either serial or parallel heap scan */
    if (*buildstate).btleader.is_null() {
        reltuples = table_index_build_scan(
            heap,
            index,
            indexInfo,
            true,
            true,
            _bt_build_callback,
            buildstate as *mut c_void,
            null_mut(),
        );
    } else {
        reltuples = _bt_parallel_heapscan(buildstate, &raw mut (*indexInfo).ii_BrokenHotChain);
    }

    /*
     * Set the progress target for the next phase.  Reset the block number
     * values set by table_index_build_scan
     */
    {
        let progress_index: [c_int; 3] = [
            PROGRESS_CREATEIDX_TUPLES_TOTAL,
            PROGRESS_SCAN_BLOCKS_TOTAL,
            PROGRESS_SCAN_BLOCKS_DONE,
        ];
        let progress_vals: [i64; 3] = [(*buildstate).indtuples as i64, 0, 0];

        pgstat_progress_update_multi_param(
            3,
            progress_index.as_ptr(),
            progress_vals.as_ptr(),
        );
    }

    /* okay, all heap tuples are spooled */
    if !(*buildstate).spool2.is_null() && !(*buildstate).havedead {
        /* spool2 turns out to be unnecessary */
        _bt_spooldestroy((*buildstate).spool2);
        (*buildstate).spool2 = null_mut();
    }

    reltuples
}

/*
 * clean up a spool structure and its substructures.
 */
unsafe fn _bt_spooldestroy(btspool: *mut BTSpool) {
    tuplesort_end((*btspool).sortstate);
    pfree(btspool as *mut c_void);
}

/*
 * spool an index entry into the sort file.
 */
unsafe fn _bt_spool(
    btspool: *mut BTSpool,
    self_: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
) {
    tuplesort_putindextuplevalues((*btspool).sortstate, (*btspool).index, self_, values, isnull);
}

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
unsafe fn _bt_leafbuild(btspool: *mut BTSpool, btspool2: *mut BTSpool) {
    let mut wstate: BTWriteState = core::mem::zeroed();

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats) {
    //     ShowUsage("BTREE BUILD (Spool) STATISTICS");
    //     ResetUsage();
    // }
    // #endif /* BTREE_BUILD_STATS */

    /* Execute the sort */
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_SUBPHASE,
        PROGRESS_BTREE_PHASE_PERFORMSORT_1,
    );
    tuplesort_performsort((*btspool).sortstate);
    if !btspool2.is_null() {
        pgstat_progress_update_param(
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_BTREE_PHASE_PERFORMSORT_2,
        );
        tuplesort_performsort((*btspool2).sortstate);
    }

    wstate.heap = (*btspool).heap;
    wstate.index = (*btspool).index;
    wstate.inskey = _bt_mkscankey(wstate.index, null_mut());
    /* _bt_mkscankey() won't set allequalimage without metapage */
    (*wstate.inskey).allequalimage = _bt_allequalimage(wstate.index, true);

    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;

    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_SUBPHASE,
        PROGRESS_BTREE_PHASE_LEAF_LOAD,
    );
    _bt_load(&raw mut wstate, btspool, btspool2);
}

/*
 * Per-tuple callback for table_index_build_scan
 */
unsafe extern "C" fn _bt_build_callback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate: *mut BTBuildState = state as *mut BTBuildState;

    /*
     * insert the index tuple into the appropriate spool file for subsequent
     * processing
     */
    if tupleIsAlive || (*buildstate).spool2.is_null() {
        _bt_spool((*buildstate).spool, tid, values, isnull);
    } else {
        /* dead tuples are put into spool2 */
        (*buildstate).havedead = true;
        _bt_spool((*buildstate).spool2, tid, values, isnull);
    }

    (*buildstate).indtuples += 1.0;
}

/*
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
unsafe fn _bt_blnewpage(wstate: *mut BTWriteState, level: u32) -> BulkWriteBuffer {
    let buf: BulkWriteBuffer;
    let page: Page;
    let opaque: BTPageOpaque;

    buf = smgr_bulk_get_buf((*wstate).bulkstate);
    page = buf as Page;

    /* Zero the page and set up standard page header info */
    _bt_pageinit(page, BLCKSZ as Size);

    /* Initialize BT opaque state */
    opaque = BTPageGetOpaque(page);
    (*opaque).btpo_prev = P_NONE;
    (*opaque).btpo_next = P_NONE;
    (*opaque).btpo_level = level;
    (*opaque).btpo_flags = if level > 0 { 0 } else { BTP_LEAF };
    (*opaque).btpo_cycleid = 0;

    /* Make the P_HIKEY line pointer appear allocated */
    // ((PageHeader) page)->pd_lower += sizeof(ItemIdData);
    // We manipulate pd_lower by treating the page header as a raw pointer.
    // PageHeader is at the beginning of the page (pg_config offset 0).
    // pd_lower is a uint16 at a known struct offset -- stub helper.
    {
        // TODO(pg-port): PageHeader pd_lower offset adjustment
        // In C: ((PageHeader) page)->pd_lower += sizeof(ItemIdData);
        // We call a stub that does the equivalent.
        page_header_pd_lower_add(page, size_of::<ItemIdData>() as u16);
    }

    buf
}

/// TODO(pg-port): PageHeader manipulation helper -- adjusts pd_lower in place.
unsafe fn page_header_pd_lower_add(page: Page, delta: u16) {
    // PageHeaderData::pd_lower is at offset 12 (pg_config dependent).
    // This is a local stub; TODO(pg-port): use real bufpage types.
    let pd_lower_ptr = (page as *mut u8).add(12) as *mut u16;
    *pd_lower_ptr = (*pd_lower_ptr).wrapping_add(delta);
}

/// TODO(pg-port): PageHeader manipulation helper -- subtracts from pd_lower in place.
unsafe fn page_header_pd_lower_sub(page: Page, delta: u16) {
    let pd_lower_ptr = (page as *mut u8).add(12) as *mut u16;
    *pd_lower_ptr = (*pd_lower_ptr).wrapping_sub(delta);
}

/*
 * emit a completed btree page, and release the working storage.
 */
unsafe fn _bt_blwritepage(
    wstate: *mut BTWriteState,
    buf: BulkWriteBuffer,
    blkno: BlockNumber,
) {
    smgr_bulk_write((*wstate).bulkstate, blkno, buf, true);
    /* smgr_bulk_write took ownership of 'buf' */
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
unsafe fn _bt_pagestate(wstate: *mut BTWriteState, level: u32) -> *mut BTPageState {
    let state: *mut BTPageState = palloc0(size_of::<BTPageState>()) as *mut BTPageState;

    /* create initial page for level */
    (*state).btps_buf = _bt_blnewpage(wstate, level);

    /* and assign it a page position */
    (*state).btps_blkno = (*wstate).btws_pages_alloced;
    (*wstate).btws_pages_alloced += 1;

    (*state).btps_lowkey = null_mut();
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    (*state).btps_lastoff = P_HIKEY;
    (*state).btps_lastextra = 0;
    (*state).btps_level = level;
    /* set "full" threshold based on level.  See notes at head of file. */
    if level > 0 {
        (*state).btps_full = (BLCKSZ as usize * (100 - BTREE_NONLEAF_FILLFACTOR as usize)) / 100;
    } else {
        (*state).btps_full = BTGetTargetPageFreeSpace((*wstate).index);
    }

    /* no parent level, yet */
    (*state).btps_next = null_mut();

    state
}

/*
 * Slide the array of ItemIds from the page back one slot (from P_FIRSTKEY to
 * P_HIKEY, overwriting P_HIKEY).
 *
 * _bt_blnewpage() makes the P_HIKEY line pointer appear allocated, but the
 * rightmost page on its level is not supposed to get a high key.  Now that
 * it's clear that this page is a rightmost page, remove the unneeded empty
 * P_HIKEY line pointer space.
 */
unsafe fn _bt_slideleft(rightmostpage: Page) {
    let mut off: OffsetNumber;
    let maxoff: OffsetNumber;
    let previi: ItemId;

    maxoff = PageGetMaxOffsetNumber(rightmostpage);
    Assert!(maxoff >= P_FIRSTKEY);
    previi = PageGetItemId(rightmostpage, P_HIKEY);
    off = P_FIRSTKEY;
    while off <= maxoff {
        let thisii: ItemId = PageGetItemId(rightmostpage, off);
        *previi = *thisii;
        // previi = thisii -- advance pointer by one ItemIdData slot
        let _ = previi; // consumed above via deref; recompute each iteration
        // Re-fetch previi as the prior slot each iteration by using off-1
        // (mirrors the C pointer walk: previi = thisii after copy)
        // We can do this by using a raw pointer arithmetic approach.
        off = OffsetNumberNext(off);
    }
    // In C: ((PageHeader) rightmostpage)->pd_lower -= sizeof(ItemIdData);
    page_header_pd_lower_sub(rightmostpage, size_of::<ItemIdData>() as u16);
}

/*
 * Add an item to a page being built.
 *
 * This is very similar to nbtinsert.c's _bt_pgaddtup(), but this variant
 * raises an error directly.
 *
 * Note that our nbtsort.c caller does not know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always assumed to be the first data key by
 * caller.  Page that turns out to be the rightmost on its level is fixed by
 * calling _bt_slideleft().
 */
unsafe fn _bt_sortaddtup(
    page: Page,
    itemsize: Size,
    itup: IndexTuple,
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) {
    let mut trunctuple: IndexTupleData = core::mem::zeroed();
    let mut itup = itup;
    let mut itemsize = itemsize;

    if newfirstdataitem {
        trunctuple = *itup;
        trunctuple.t_info = size_of::<IndexTupleData>() as u16;
        BTreeTupleSetNAtts(&raw mut trunctuple, 0, false);
        itup = &raw mut trunctuple;
        itemsize = size_of::<IndexTupleData>();
    }

    if PageAddItem(page, itup as crate::storage::item::Item, itemsize, itup_off, false, false)
        == InvalidOffsetNumber
    {
        elog!(ERROR, "failed to add item to the index page");
    }
}

/*----------
 * Add an item to a disk page from the sort output (or add a posting list
 * item formed from the sort output).
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *   stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |                                     |
 * +-----------+--------------------------------------+
 * |   ^ last                                        |
 * |                                                 |
 * +-------------+------------------------------------+
 * |             | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |        ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.  If
 * the high key is to be truncated, offset 1 is deleted, and we insert
 * the truncated high key at offset 1.
 *
 * 'last' pointer indicates the last offset added to the page.
 *
 * 'truncextra' is the size of the posting list in itup, if any.  This
 * information is stashed for the next call here, when we may benefit
 * from considering the impact of truncating away the posting list on
 * the page before deciding to finish the page off.  Posting lists are
 * often relatively large, so it is worth going to the trouble of
 * accounting for the saving from truncating away the posting list of
 * the tuple that becomes the high key (that may be the only way to
 * get close to target free space on the page).  Note that this is
 * only used for the soft fillfactor-wise limit, not the critical hard
 * limit.
 *----------
 */
unsafe fn _bt_buildadd(
    wstate: *mut BTWriteState,
    state: *mut BTPageState,
    itup: IndexTuple,
    truncextra: Size,
) {
    let mut nbuf: BulkWriteBuffer;
    let mut npage: Page;
    let mut nblkno: BlockNumber;
    let mut last_off: OffsetNumber;
    let last_truncextra: Size;
    let pgspc: Size;
    let itupsz: Size;
    let isleaf: bool;

    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS!();

    nbuf = (*state).btps_buf;
    npage = nbuf as Page;
    nblkno = (*state).btps_blkno;
    last_off = (*state).btps_lastoff;
    last_truncextra = (*state).btps_lastextra;
    (*state).btps_lastextra = truncextra;

    pgspc = PageGetFreeSpace(npage);
    let mut itupsz_local: Size = IndexTupleSize(itup);
    itupsz_local = MAXALIGN(itupsz_local);
    itupsz = itupsz_local;
    /* Leaf case has slightly different rules due to suffix truncation */
    isleaf = ((*state).btps_level == 0);

    /*
     * Check whether the new item can fit on a btree page on current level at
     * all.
     *
     * Every newly built index will treat heap TID as part of the keyspace,
     * which imposes the requirement that new high keys must occasionally have
     * a heap TID appended within _bt_truncate().  That may leave a new pivot
     * tuple one or two MAXALIGN() quantums larger than the original
     * firstright tuple it's derived from.  v4 deals with the problem by
     * decreasing the limit on the size of tuples inserted on the leaf level
     * by the same small amount.  Enforce the new v4+ limit on the leaf level,
     * and the old limit on internal levels, since pivot tuples may need to
     * make use of the reserved space.  This should never fail on internal
     * pages.
     */
    if unlikely(itupsz > BTMaxItemSize) {
        _bt_check_third_page((*wstate).index, (*wstate).heap, isleaf, npage, itup);
    }

    /*
     * Check to see if current page will fit new item, with space left over to
     * append a heap TID during suffix truncation when page is a leaf page.
     *
     * It is guaranteed that we can fit at least 2 non-pivot tuples plus a
     * high key with heap TID when finishing off a leaf page, since we rely on
     * _bt_check_third_page() rejecting oversized non-pivot tuples.  On
     * internal pages we can always fit 3 pivot tuples with larger internal
     * page tuple limit (includes page high key).
     *
     * Most of the time, a page is only "full" in the sense that the soft
     * fillfactor-wise limit has been exceeded.  However, we must always leave
     * at least two items plus a high key on each page before starting a new
     * page.  Disregard fillfactor and insert on "full" current page if we
     * don't have the minimum number of items yet.  (Note that we deliberately
     * assume that suffix truncation neither enlarges nor shrinks new high key
     * when applying soft limit, except when last tuple has a posting list.)
     */
    Assert!(last_truncextra == 0 || isleaf);
    if pgspc < itupsz + (if isleaf { MAXALIGN(size_of::<ItemPointerData>()) } else { 0 })
        || (pgspc + last_truncextra < (*state).btps_full && last_off > P_FIRSTKEY)
    {
        /*
         * Finish off the page and write it out.
         */
        let obuf: BulkWriteBuffer = nbuf;
        let opage: Page = npage;
        let oblkno: BlockNumber = nblkno;
        let ii: ItemId;
        let hii: ItemId;
        let oitup: IndexTuple;

        /* Create new page of same level */
        nbuf = _bt_blnewpage(wstate, (*state).btps_level);
        npage = nbuf as Page;

        /* and assign it a page position */
        nblkno = (*wstate).btws_pages_alloced;
        (*wstate).btws_pages_alloced += 1;

        /*
         * We copy the last item on the page into the new page, and then
         * rearrange the old page so that the 'last item' becomes its high key
         * rather than a true data item.  There had better be at least two
         * items on the page already, else the page would be empty of useful
         * data.
         */
        Assert!(last_off > P_FIRSTKEY);
        ii = PageGetItemId(opage, last_off);
        let oitup_init: IndexTuple = PageGetItem(opage, ii) as IndexTuple;
        _bt_sortaddtup(npage, ItemIdGetLength(ii) as Size, oitup_init, P_FIRSTKEY, !isleaf);

        /*
         * Move 'last' into the high key position on opage.  _bt_blnewpage()
         * allocated empty space for a line pointer when opage was first
         * created, so this is a matter of rearranging already-allocated space
         * on page, and initializing high key line pointer. (Actually, leaf
         * pages must also swap oitup with a truncated version of oitup, which
         * is sometimes larger than oitup, though never by more than the space
         * needed to append a heap TID.)
         */
        hii = PageGetItemId(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii); /* redundant */
        page_header_pd_lower_sub(opage, size_of::<ItemIdData>() as u16);

        let oitup_final: IndexTuple;
        if isleaf {
            let lastleft: IndexTuple;
            let truncated: IndexTuple;

            /*
             * Truncate away any unneeded attributes from high key on leaf
             * level.  This is only done at the leaf level because downlinks
             * in internal pages are either negative infinity items, or get
             * their contents from copying from one level down.  See also:
             * _bt_split().
             *
             * We don't try to bias our choice of split point to make it more
             * likely that _bt_truncate() can truncate away more attributes,
             * whereas the split point used within _bt_split() is chosen much
             * more delicately.  Even still, the lastleft and firstright
             * tuples passed to _bt_truncate() here are at least not fully
             * equal to each other when deduplication is used, unless there is
             * a large group of duplicates (also, unique index builds usually
             * have few or no spool2 duplicates).  When the split point is
             * between two unequal tuples, _bt_truncate() will avoid including
             * a heap TID in the new high key, which is the most important
             * benefit of suffix truncation.
             *
             * Overwrite the old item with new truncated high key directly.
             * oitup is already located at the physical beginning of tuple
             * space, so this should directly reuse the existing tuple space.
             */
            let prev_ii: ItemId = PageGetItemId(opage, OffsetNumberPrev(last_off));
            lastleft = PageGetItem(opage, prev_ii) as IndexTuple;

            let oitup_hii: IndexTuple = PageGetItem(opage, hii) as IndexTuple;
            Assert!(IndexTupleSize(oitup_hii) > last_truncextra);
            truncated = _bt_truncate((*wstate).index, lastleft, oitup_hii, (*wstate).inskey);
            if !PageIndexTupleOverwrite(
                opage,
                P_HIKEY,
                truncated as crate::storage::item::Item,
                IndexTupleSize(truncated),
            ) {
                elog!(ERROR, "failed to add high key to the index page");
            }
            pfree(truncated as *mut c_void);

            /* oitup should continue to point to the page's high key */
            let hii2: ItemId = PageGetItemId(opage, P_HIKEY);
            oitup_final = PageGetItem(opage, hii2) as IndexTuple;
        } else {
            oitup_final = oitup_init;
        }

        /*
         * Link the old page into its parent, using its low key.  If we don't
         * have a parent, we have to create one; this adds a new btree level.
         */
        if (*state).btps_next.is_null() {
            (*state).btps_next = _bt_pagestate(wstate, (*state).btps_level + 1);
        }

        Assert!(
            (BTreeTupleGetNAtts((*state).btps_lowkey, (*wstate).index)
                <= IndexRelationGetNumberOfKeyAttributes((*wstate).index)
                && BTreeTupleGetNAtts((*state).btps_lowkey, (*wstate).index) > 0)
                || P_LEFTMOST(BTPageGetOpaque(opage))
        );
        Assert!(
            BTreeTupleGetNAtts((*state).btps_lowkey, (*wstate).index) == 0
                || !P_LEFTMOST(BTPageGetOpaque(opage))
        );
        BTreeTupleSetDownLink((*state).btps_lowkey, oblkno);
        _bt_buildadd(wstate, (*state).btps_next, (*state).btps_lowkey, 0);
        pfree((*state).btps_lowkey as *mut c_void);

        /*
         * Save a copy of the high key from the old page.  It is also the low
         * key for the new page.
         */
        (*state).btps_lowkey = CopyIndexTuple(oitup_final);

        /*
         * Set the sibling links for both pages.
         */
        {
            let oopaque: BTPageOpaque = BTPageGetOpaque(opage);
            let nopaque: BTPageOpaque = BTPageGetOpaque(npage);

            (*oopaque).btpo_next = nblkno;
            (*nopaque).btpo_prev = oblkno;
            (*nopaque).btpo_next = P_NONE; /* redundant */
        }

        /*
         * Write out the old page. _bt_blwritepage takes ownership of the
         * 'opage' buffer.
         */
        _bt_blwritepage(wstate, obuf, oblkno);

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * By here, either original page is still the current page, or a new page
     * was created that became the current page.  Either way, the current page
     * definitely has space for new item.
     *
     * If the new item is the first for its page, it must also be the first
     * item on its entire level.  On later same-level pages, a low key for a
     * page will be copied from the prior page in the code above.  Generate a
     * minus infinity low key here instead.
     */
    if last_off == P_HIKEY {
        Assert!((*state).btps_lowkey.is_null());
        (*state).btps_lowkey = palloc0(size_of::<IndexTupleData>()) as IndexTuple;
        (*(*state).btps_lowkey).t_info = size_of::<IndexTupleData>() as u16;
        BTreeTupleSetNAtts((*state).btps_lowkey, 0, false);
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    _bt_sortaddtup(
        npage,
        itupsz,
        itup,
        last_off,
        !isleaf && last_off == P_FIRSTKEY,
    );

    (*state).btps_buf = nbuf;
    (*state).btps_blkno = nblkno;
    (*state).btps_lastoff = last_off;
}

/*
 * Finalize pending posting list tuple, and add it to the index.  Final tuple
 * is based on saved base tuple, and saved list of heap TIDs.
 *
 * This is almost like _bt_dedup_finish_pending(), but it adds a new tuple
 * using _bt_buildadd().
 */
unsafe fn _bt_sort_dedup_finish_pending(
    wstate: *mut BTWriteState,
    state: *mut BTPageState,
    dstate: BTDedupState,
) {
    Assert!((*dstate).nitems > 0);

    if (*dstate).nitems == 1 {
        _bt_buildadd(wstate, state, (*dstate).base, 0);
    } else {
        let postingtuple: IndexTuple;
        let truncextra: Size;

        /* form a tuple with a posting list */
        postingtuple = _bt_form_posting((*dstate).base, (*dstate).htids, (*dstate).nhtids);
        /* Calculate posting list overhead */
        truncextra = IndexTupleSize(postingtuple) - BTreeTupleGetPostingOffset(postingtuple) as Size;

        _bt_buildadd(wstate, state, postingtuple, truncextra);
        pfree(postingtuple as *mut c_void);
    }

    (*dstate).nmaxitems = 0;
    (*dstate).nhtids = 0;
    (*dstate).nitems = 0;
    (*dstate).phystupsize = 0;
}

/*
 * Finish writing out the completed btree.
 */
unsafe fn _bt_uppershutdown(wstate: *mut BTWriteState, state: *mut BTPageState) {
    let mut s: *mut BTPageState;
    let mut rootblkno: BlockNumber = P_NONE;
    let mut rootlevel: u32 = 0;
    let metabuf: BulkWriteBuffer;

    /*
     * Each iteration of this loop completes one more level of the tree.
     */
    s = state;
    while !s.is_null() {
        let blkno: BlockNumber;
        let opaque: BTPageOpaque;

        blkno = (*s).btps_blkno;
        opaque = BTPageGetOpaque((*s).btps_buf as Page);

        /*
         * We have to link the last page on this level to somewhere.
         *
         * If we're at the top, it's the root, so attach it to the metapage.
         * Otherwise, add an entry for it to its parent using its low key.
         * This may cause the last page of the parent level to split, but
         * that's not a problem -- we haven't gotten to it yet.
         */
        if (*s).btps_next.is_null() {
            (*opaque).btpo_flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = (*s).btps_level;
        } else {
            Assert!(
                (BTreeTupleGetNAtts((*s).btps_lowkey, (*wstate).index)
                    <= IndexRelationGetNumberOfKeyAttributes((*wstate).index)
                    && BTreeTupleGetNAtts((*s).btps_lowkey, (*wstate).index) > 0)
                    || P_LEFTMOST(opaque)
            );
            Assert!(
                BTreeTupleGetNAtts((*s).btps_lowkey, (*wstate).index) == 0
                    || !P_LEFTMOST(opaque)
            );
            BTreeTupleSetDownLink((*s).btps_lowkey, blkno);
            _bt_buildadd(wstate, (*s).btps_next, (*s).btps_lowkey, 0);
            pfree((*s).btps_lowkey as *mut c_void);
            (*s).btps_lowkey = null_mut();
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        _bt_slideleft((*s).btps_buf as Page);
        _bt_blwritepage(wstate, (*s).btps_buf, (*s).btps_blkno);
        (*s).btps_buf = null_mut(); /* writepage took ownership of the buffer */

        s = (*s).btps_next;
    }

    /*
     * As the last step in the process, construct the metapage and make it
     * point to the new root (unless we had no data at all, in which case it's
     * set to point to "P_NONE").  This changes the index to the "valid" state
     * by filling in a valid magic number in the metapage.
     */
    metabuf = smgr_bulk_get_buf((*wstate).bulkstate);
    _bt_initmetapage(metabuf as Page, rootblkno, rootlevel, (*(*wstate).inskey).allequalimage);
    _bt_blwritepage(wstate, metabuf, BTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
unsafe fn _bt_load(
    wstate: *mut BTWriteState,
    btspool: *mut BTSpool,
    btspool2: *mut BTSpool,
) {
    let mut state: *mut BTPageState = null_mut();
    let merge: bool = !btspool2.is_null();
    let mut itup: IndexTuple;
    let mut itup2: IndexTuple = null_mut();
    let mut load1: bool;
    let tupdes: TupleDesc = RelationGetDescr((*wstate).index);
    let mut i: c_int;
    let keysz: c_int = IndexRelationGetNumberOfKeyAttributes((*wstate).index);
    let sortKeys: *mut SortSupportData;
    let mut tuples_done: i64 = 0;
    let deduplicate: bool;

    (*wstate).bulkstate = smgr_bulk_start_rel((*wstate).index, MAIN_FORKNUM);

    deduplicate = (*(*wstate).inskey).allequalimage
        && !(*btspool).isunique
        && BTGetDeduplicateItems((*wstate).index);

    if merge {
        /*
         * Another BTSpool for dead tuples exists. Now we have to merge
         * btspool and btspool2.
         */

        /* the preparation of merge */
        itup = tuplesort_getindextuple((*btspool).sortstate, true);
        itup2 = tuplesort_getindextuple((*btspool2).sortstate, true);

        /* Prepare SortSupport data for each column */
        sortKeys = palloc0(keysz as usize * size_of::<SortSupportData>()) as *mut SortSupportData;

        i = 0;
        while i < keysz {
            let sortKey: *mut SortSupportData = sortKeys.add(i as usize);
            let scanKey: *mut ScanKeyData = ((*(*wstate).inskey).scankeys).add(i as usize);
            let reverse: bool;

            (*sortKey).ssup_cxt = CurrentMemoryContext;
            (*sortKey).ssup_collation = (*scanKey).sk_collation;
            (*sortKey).ssup_nulls_first = ((*scanKey).sk_flags & SK_BT_NULLS_FIRST) != 0;
            (*sortKey).ssup_attno = (*scanKey).sk_attno;
            /* Abbreviation is not supported here */
            (*sortKey).abbreviate = false;

            Assert!((*sortKey).ssup_attno != 0);

            reverse = ((*scanKey).sk_flags & SK_BT_DESC) != 0;

            PrepareSortSupportFromIndexRel((*wstate).index, reverse, sortKey);

            i += 1;
        }

        loop {
            load1 = true; /* load BTSpool next ? */
            if itup2.is_null() {
                if itup.is_null() {
                    break;
                }
            } else if !itup.is_null() {
                let mut compare: i32 = 0;

                i = 1;
                while i <= keysz {
                    let entry: *mut SortSupportData;
                    let attrDatum1: Datum;
                    let attrDatum2: Datum;
                    let mut isNull1: bool = false;
                    let mut isNull2: bool = false;

                    entry = sortKeys.add((i - 1) as usize);
                    attrDatum1 = index_getattr(itup, i, tupdes, &raw mut isNull1);
                    attrDatum2 = index_getattr(itup2, i, tupdes, &raw mut isNull2);

                    compare = ApplySortComparator(attrDatum1, isNull1, attrDatum2, isNull2, entry);
                    if compare > 0 {
                        load1 = false;
                        break;
                    } else if compare < 0 {
                        break;
                    }

                    i += 1;
                }

                /*
                 * If key values are equal, we sort on ItemPointer.  This is
                 * required for btree indexes, since heap TID is treated as an
                 * implicit last key attribute in order to ensure that all
                 * keys in the index are physically unique.
                 */
                if compare == 0 {
                    compare = ItemPointerCompare(&raw mut (*itup).t_tid, &raw mut (*itup2).t_tid);
                    Assert!(compare != 0);
                    if compare > 0 {
                        load1 = false;
                    }
                }
            } else {
                load1 = false;
            }

            /* When we see first tuple, create first index page */
            if state.is_null() {
                state = _bt_pagestate(wstate, 0);
            }

            if load1 {
                _bt_buildadd(wstate, state, itup, 0);
                itup = tuplesort_getindextuple((*btspool).sortstate, true);
            } else {
                _bt_buildadd(wstate, state, itup2, 0);
                itup2 = tuplesort_getindextuple((*btspool2).sortstate, true);
            }

            /* Report progress */
            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);
        }
        pfree(sortKeys as *mut c_void);
    } else if deduplicate {
        /* merge is unnecessary, deduplicate into posting lists */
        let dstate: BTDedupState;

        dstate = palloc(size_of::<BTDedupStateData>()) as BTDedupState;
        (*dstate).deduplicate = true; /* unused */
        (*dstate).nmaxitems = 0; /* unused */
        (*dstate).maxpostingsize = 0; /* set later */
        /* Metadata about base tuple of current pending posting list */
        (*dstate).base = null_mut();
        (*dstate).baseoff = InvalidOffsetNumber; /* unused */
        (*dstate).basetupsize = 0;
        /* Metadata about current pending posting list TIDs */
        (*dstate).htids = null_mut();
        (*dstate).nhtids = 0;
        (*dstate).nitems = 0;
        (*dstate).phystupsize = 0; /* unused */
        (*dstate).nintervals = 0; /* unused */

        itup = tuplesort_getindextuple((*btspool).sortstate, true);
        while !itup.is_null() {
            /* When we see first tuple, create first index page */
            if state.is_null() {
                state = _bt_pagestate(wstate, 0);

                /*
                 * Limit size of posting list tuples to 1/10 space we want to
                 * leave behind on the page, plus space for final item's line
                 * pointer.  This is equal to the space that we'd like to
                 * leave behind on each leaf page when fillfactor is 90,
                 * allowing us to get close to fillfactor% space utilization
                 * when there happen to be a great many duplicates.  (This
                 * makes higher leaf fillfactor settings ineffective when
                 * building indexes that have many duplicates, but packing
                 * leaf pages full with few very large tuples doesn't seem
                 * like a useful goal.)
                 */
                (*dstate).maxpostingsize =
                    MAXALIGN_DOWN((BLCKSZ as usize * 10 / 100)) - size_of::<ItemIdData>();
                Assert!(
                    (*dstate).maxpostingsize <= BTMaxItemSize
                        && (*dstate).maxpostingsize <= crate::access::common::indextuple::INDEX_SIZE_MASK as Size
                );
                (*dstate).htids = palloc((*dstate).maxpostingsize) as ItemPointer;

                /* start new pending posting list with itup copy */
                _bt_dedup_start_pending(dstate, CopyIndexTuple(itup), InvalidOffsetNumber);
            } else if _bt_keep_natts_fast((*wstate).index, (*dstate).base, itup) > keysz
                && _bt_dedup_save_htid(dstate, itup)
            {
                /*
                 * Tuple is equal to base tuple of pending posting list.  Heap
                 * TID from itup has been saved in state.
                 */
            } else {
                /*
                 * Tuple is not equal to pending posting list tuple, or
                 * _bt_dedup_save_htid() opted to not merge current item into
                 * pending posting list.
                 */
                _bt_sort_dedup_finish_pending(wstate, state, dstate);
                pfree((*dstate).base as *mut c_void);

                /* start new pending posting list with itup copy */
                _bt_dedup_start_pending(dstate, CopyIndexTuple(itup), InvalidOffsetNumber);
            }

            /* Report progress */
            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);

            itup = tuplesort_getindextuple((*btspool).sortstate, true);
        }

        if !state.is_null() {
            /*
             * Handle the last item (there must be a last item when the
             * tuplesort returned one or more tuples)
             */
            _bt_sort_dedup_finish_pending(wstate, state, dstate);
            pfree((*dstate).base as *mut c_void);
            pfree((*dstate).htids as *mut c_void);
        }

        pfree(dstate as *mut c_void);
    } else {
        /* merging and deduplication are both unnecessary */
        itup = tuplesort_getindextuple((*btspool).sortstate, true);
        while !itup.is_null() {
            /* When we see first tuple, create first index page */
            if state.is_null() {
                state = _bt_pagestate(wstate, 0);
            }

            _bt_buildadd(wstate, state, itup, 0);

            /* Report progress */
            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);

            itup = tuplesort_getindextuple((*btspool).sortstate, true);
        }
    }

    /* Close down final pages and write the metapage */
    _bt_uppershutdown(wstate, state);
    smgr_bulk_finish((*wstate).bulkstate);
}

/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's BTLeader, which caller must use to shut down parallel
 * mode by passing it to _bt_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
unsafe fn _bt_begin_parallel(
    buildstate: *mut BTBuildState,
    isconcurrent: bool,
    request: c_int,
) {
    let pcxt: *mut ParallelContext;
    let scantuplesortstates: c_int;
    let snapshot: Snapshot;
    let estbtshared: Size;
    let estsort: Size;
    let btshared: *mut BTShared;
    let sharedsort: *mut Sharedsort;
    let mut sharedsort2: *mut Sharedsort;
    let btspool: *mut BTSpool = (*buildstate).spool;
    let btleader: *mut BTLeader = palloc0(size_of::<BTLeader>()) as *mut BTLeader;
    let walusage: *mut WalUsage;
    let bufferusage: *mut BufferUsage;
    let mut leaderparticipates: bool = true;
    let querylen: c_int;

    // #ifdef DISABLE_LEADER_PARTICIPATION
    // leaderparticipates = false;
    // #endif

    /*
     * Enter parallel mode, and create context for parallel build of btree
     * index
     */
    EnterParallelMode();
    Assert!(request > 0);
    pcxt = CreateParallelContext(
        b"postgres\0".as_ptr() as *const c_char,
        b"_bt_parallel_build_main\0".as_ptr() as *const c_char,
        request,
    );

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
     * Estimate size for our own PARALLEL_KEY_BTREE_SHARED workspace, and
     * PARALLEL_KEY_TUPLESORT tuplesort workspace
     */
    estbtshared = _bt_parallel_estimate_shared((*btspool).heap, snapshot);
    shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, estbtshared);
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, estsort);

    /*
     * Unique case requires a second spool, and so we may have to account for
     * another shared workspace for that -- PARALLEL_KEY_TUPLESORT_SPOOL2
     */
    if !(*btspool).isunique {
        shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 2);
    } else {
        shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, estsort);
        shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 3);
    }

    /*
     * Estimate space for WalUsage and BufferUsage -- PARALLEL_KEY_WAL_USAGE
     * and PARALLEL_KEY_BUFFER_USAGE.
     *
     * If there are no extensions loaded that care, we could skip this.  We
     * have no way of knowing whether anyone's looking at pgWalUsage or
     * pgBufferUsage, so do it unconditionally.
     */
    shm_toc_estimate_chunk(
        &raw mut (*pcxt).estimator,
        mul_size(size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
    shm_toc_estimate_chunk(
        &raw mut (*pcxt).estimator,
        mul_size(size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);

    /* Finally, estimate PARALLEL_KEY_QUERY_TEXT space */
    if !debug_query_string.is_null() {
        querylen = strlen(debug_query_string) as c_int;
        shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, (querylen + 1) as Size);
        shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
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
    btshared = shm_toc_allocate((*pcxt).toc, estbtshared) as *mut BTShared;
    /* Initialize immutable state */
    (*btshared).heaprelid = RelationGetRelid((*btspool).heap);
    (*btshared).indexrelid = RelationGetRelid((*btspool).index);
    (*btshared).isunique = (*btspool).isunique;
    (*btshared).nulls_not_distinct = (*btspool).nulls_not_distinct;
    (*btshared).isconcurrent = isconcurrent;
    (*btshared).scantuplesortstates = scantuplesortstates;
    (*btshared).queryid = pgstat_get_my_query_id();
    ConditionVariableInit(&raw mut (*btshared).workersdonecv);
    SpinLockInit(&raw mut (*btshared).mutex);
    /* Initialize mutable state */
    (*btshared).nparticipantsdone = 0;
    (*btshared).reltuples = 0.0;
    (*btshared).havedead = false;
    (*btshared).indtuples = 0.0;
    (*btshared).brokenhotchain = false;
    table_parallelscan_initialize(
        (*btspool).heap,
        ParallelTableScanFromBTShared(btshared),
        snapshot,
    );

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    sharedsort = shm_toc_allocate((*pcxt).toc, estsort) as *mut Sharedsort;
    tuplesort_initialize_shared(sharedsort, scantuplesortstates, (*pcxt).seg);

    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BTREE_SHARED, btshared as *mut c_void);
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TUPLESORT, sharedsort as *mut c_void);

    /* Unique case requires a second spool, and associated shared state */
    if !(*btspool).isunique {
        sharedsort2 = null_mut();
    } else {
        /*
         * Store additional shared tuplesort-private state, for which we
         * reserved space.  Then, initialize opaque state using tuplesort
         * routine.
         */
        sharedsort2 = shm_toc_allocate((*pcxt).toc, estsort) as *mut Sharedsort;
        tuplesort_initialize_shared(sharedsort2, scantuplesortstates, (*pcxt).seg);

        shm_toc_insert(
            (*pcxt).toc,
            PARALLEL_KEY_TUPLESORT_SPOOL2,
            sharedsort2 as *mut c_void,
        );
    }

    /* Store query string for workers */
    if !debug_query_string.is_null() {
        let sharedquery: *mut c_char =
            shm_toc_allocate((*pcxt).toc, (querylen + 1) as Size) as *mut c_char;
        memcpy(
            sharedquery as *mut c_void,
            debug_query_string as *const c_void,
            (querylen + 1) as usize,
        );
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_QUERY_TEXT, sharedquery as *mut c_void);
    }

    /*
     * Allocate space for each worker's WalUsage and BufferUsage; no need to
     * initialize.
     */
    walusage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    ) as *mut WalUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_WAL_USAGE, walusage as *mut c_void);
    bufferusage = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    ) as *mut BufferUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage as *mut c_void);

    /* Launch workers, saving status for leader/caller */
    LaunchParallelWorkers(pcxt);
    (*btleader).pcxt = pcxt;
    (*btleader).nparticipanttuplesorts = (*pcxt).nworkers_launched;
    if leaderparticipates {
        (*btleader).nparticipanttuplesorts += 1;
    }
    (*btleader).btshared = btshared;
    (*btleader).sharedsort = sharedsort;
    (*btleader).sharedsort2 = sharedsort2;
    (*btleader).snapshot = snapshot;
    (*btleader).walusage = walusage;
    (*btleader).bufferusage = bufferusage;

    /* If no workers were successfully launched, back out (do serial build) */
    if (*pcxt).nworkers_launched == 0 {
        _bt_end_parallel(btleader);
        return;
    }

    /* Save leader state now that it's clear build will be parallel */
    (*buildstate).btleader = btleader;

    /* Join heap scan ourselves */
    if leaderparticipates {
        _bt_leader_participate_as_worker(buildstate);
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
unsafe fn _bt_end_parallel(btleader: *mut BTLeader) {
    let i: c_int;

    /* Shutdown worker processes */
    WaitForParallelWorkersToFinish((*btleader).pcxt);

    /*
     * Next, accumulate WAL usage.  (This must wait for the workers to finish,
     * or we might get incomplete data.)
     */
    let mut j: c_int = 0;
    while j < (*(*btleader).pcxt).nworkers_launched {
        InstrAccumParallelQuery(
            (*btleader).bufferusage.add(j as usize),
            (*btleader).walusage.add(j as usize),
        );
        j += 1;
    }

    /* Free last reference to MVCC snapshot, if one was used */
    if IsMVCCSnapshot((*btleader).snapshot) {
        UnregisterSnapshot((*btleader).snapshot);
    }
    DestroyParallelContext((*btleader).pcxt);
    ExitParallelMode();
}

/*
 * Returns size of shared memory required to store state for a parallel
 * btree index build based on the snapshot its parallel scan will use.
 */
unsafe fn _bt_parallel_estimate_shared(heap: Relation, snapshot: Snapshot) -> Size {
    /* c.f. shm_toc_allocate as to why BUFFERALIGN is used */
    add_size(
        BUFFERALIGN!(size_of::<BTShared>()),
        table_parallelscan_estimate(heap, snapshot),
    )
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _bt_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Fills in fields needed for ambuild statistics, and lets caller set
 * field indicating that some worker encountered a broken HOT chain.
 *
 * Returns the total number of heap tuples scanned.
 */
unsafe fn _bt_parallel_heapscan(
    buildstate: *mut BTBuildState,
    brokenhotchain: *mut bool,
) -> f64 {
    let btshared: *mut BTShared = (*(*buildstate).btleader).btshared;
    let nparticipanttuplesorts: c_int;
    let reltuples: f64;

    nparticipanttuplesorts = (*(*buildstate).btleader).nparticipanttuplesorts;
    loop {
        SpinLockAcquire(&raw mut (*btshared).mutex);
        if (*btshared).nparticipantsdone == nparticipanttuplesorts {
            (*buildstate).havedead = (*btshared).havedead;
            (*buildstate).indtuples = (*btshared).indtuples;
            *brokenhotchain = (*btshared).brokenhotchain;
            let rt = (*btshared).reltuples;
            SpinLockRelease(&raw mut (*btshared).mutex);
            reltuples = rt;
            break;
        }
        SpinLockRelease(&raw mut (*btshared).mutex);

        ConditionVariableSleep(
            &raw mut (*btshared).workersdonecv,
            WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
        );
    }

    ConditionVariableCancelSleep();

    reltuples
}

/*
 * Within leader, participate as a parallel worker.
 */
unsafe fn _bt_leader_participate_as_worker(buildstate: *mut BTBuildState) {
    let btleader: *mut BTLeader = (*buildstate).btleader;
    let leaderworker: *mut BTSpool;
    let mut leaderworker2: *mut BTSpool;
    let sortmem: c_int;

    /* Allocate memory and initialize private spool */
    leaderworker = palloc0(size_of::<BTSpool>()) as *mut BTSpool;
    (*leaderworker).heap = (*(*buildstate).spool).heap;
    (*leaderworker).index = (*(*buildstate).spool).index;
    (*leaderworker).isunique = (*(*buildstate).spool).isunique;
    (*leaderworker).nulls_not_distinct = (*(*buildstate).spool).nulls_not_distinct;

    /* Initialize second spool, if required */
    if !(*(*btleader).btshared).isunique {
        leaderworker2 = null_mut();
    } else {
        /* Allocate memory for worker's own private secondary spool */
        leaderworker2 = palloc0(size_of::<BTSpool>()) as *mut BTSpool;

        /* Initialize worker's own secondary spool */
        (*leaderworker2).heap = (*leaderworker).heap;
        (*leaderworker2).index = (*leaderworker).index;
        (*leaderworker2).isunique = false;
    }

    /*
     * Might as well use reliable figure when doling out maintenance_work_mem
     * (when requested number of workers were not launched, this will be
     * somewhat higher than it is for other workers).
     */
    sortmem = maintenance_work_mem / (*btleader).nparticipanttuplesorts;

    /* Perform work common to all participants */
    _bt_parallel_scan_and_sort(
        leaderworker,
        leaderworker2,
        (*btleader).btshared,
        (*btleader).sharedsort,
        (*btleader).sharedsort2,
        sortmem,
        true,
    );

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats) {
    //     ShowUsage("BTREE BUILD (Leader Partial Spool) STATISTICS");
    //     ResetUsage();
    // }
    // #endif /* BTREE_BUILD_STATS */
}

/*
 * Perform work within a launched parallel process.
 */
pub unsafe fn _bt_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) {
    let sharedquery: *mut c_char;
    let btspool: *mut BTSpool;
    let mut btspool2: *mut BTSpool;
    let btshared: *mut BTShared;
    let sharedsort: *mut Sharedsort;
    let mut sharedsort2: *mut Sharedsort;
    let heapRel: Relation;
    let indexRel: Relation;
    let heapLockmode: LOCKMODE;
    let indexLockmode: LOCKMODE;
    let walusage: *mut WalUsage;
    let bufferusage: *mut BufferUsage;
    let sortmem: c_int;

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats)
    //     ResetUsage();
    // #endif /* BTREE_BUILD_STATS */

    /*
     * The only possible status flag that can be set to the parallel worker is
     * PROC_IN_SAFE_IC.
     */
    Assert!((*MyProc).statusFlags == 0 || (*MyProc).statusFlags == PROC_IN_SAFE_IC);

    /* Set debug_query_string for individual workers first */
    sharedquery =
        shm_toc_lookup(toc as *mut c_void, PARALLEL_KEY_QUERY_TEXT, true) as *mut c_char;
    debug_query_string = sharedquery;

    /* Report the query string from leader */
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    /* Look up nbtree shared state */
    btshared =
        shm_toc_lookup(toc as *mut c_void, PARALLEL_KEY_BTREE_SHARED, false) as *mut BTShared;

    /* Open relations using lock modes known to be obtained by index.c */
    if !(*btshared).isconcurrent {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    /* Track query ID */
    pgstat_report_query_id((*btshared).queryid, false);

    /* Open relations within worker */
    heapRel = table_open((*btshared).heaprelid, heapLockmode);
    indexRel = index_open((*btshared).indexrelid, indexLockmode);

    /* Initialize worker's own spool */
    btspool = palloc0(size_of::<BTSpool>()) as *mut BTSpool;
    (*btspool).heap = heapRel;
    (*btspool).index = indexRel;
    (*btspool).isunique = (*btshared).isunique;
    (*btspool).nulls_not_distinct = (*btshared).nulls_not_distinct;

    /* Look up shared state private to tuplesort.c */
    sharedsort =
        shm_toc_lookup(toc as *mut c_void, PARALLEL_KEY_TUPLESORT, false) as *mut Sharedsort;
    tuplesort_attach_shared(sharedsort, seg as *mut c_void);
    if !(*btshared).isunique {
        btspool2 = null_mut();
        sharedsort2 = null_mut();
    } else {
        /* Allocate memory for worker's own private secondary spool */
        btspool2 = palloc0(size_of::<BTSpool>()) as *mut BTSpool;

        /* Initialize worker's own secondary spool */
        (*btspool2).heap = (*btspool).heap;
        (*btspool2).index = (*btspool).index;
        (*btspool2).isunique = false;
        /* Look up shared state private to tuplesort.c */
        sharedsort2 = shm_toc_lookup(
            toc as *mut c_void,
            PARALLEL_KEY_TUPLESORT_SPOOL2,
            false,
        ) as *mut Sharedsort;
        tuplesort_attach_shared(sharedsort2, seg as *mut c_void);
    }

    /* Prepare to track buffer usage during parallel execution */
    InstrStartParallelQuery();

    /* Perform sorting of spool, and possibly a spool2 */
    sortmem = maintenance_work_mem / (*btshared).scantuplesortstates;
    _bt_parallel_scan_and_sort(
        btspool,
        btspool2,
        btshared,
        sharedsort,
        sharedsort2,
        sortmem,
        false,
    );

    /* Report WAL/buffer usage during parallel execution */
    bufferusage =
        shm_toc_lookup(toc as *mut c_void, PARALLEL_KEY_BUFFER_USAGE, false) as *mut BufferUsage;
    walusage =
        shm_toc_lookup(toc as *mut c_void, PARALLEL_KEY_WAL_USAGE, false) as *mut WalUsage;
    InstrEndParallelQuery(
        bufferusage.add(ParallelWorkerNumber as usize),
        walusage.add(ParallelWorkerNumber as usize),
    );

    // #ifdef BTREE_BUILD_STATS
    // if (log_btree_build_stats) {
    //     ShowUsage("BTREE BUILD (Worker Partial Spool) STATISTICS");
    //     ResetUsage();
    // }
    // #endif /* BTREE_BUILD_STATS */

    index_close(indexRel, indexLockmode);
    table_close(heapRel, heapLockmode);
}

/*
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool, and a second tuplesort
 * state if a second btspool is need (i.e. for unique index builds).  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
unsafe fn _bt_parallel_scan_and_sort(
    btspool: *mut BTSpool,
    btspool2: *mut BTSpool,
    btshared: *mut BTShared,
    sharedsort: *mut Sharedsort,
    sharedsort2: *mut Sharedsort,
    sortmem: c_int,
    progress: bool,
) {
    let coordinate: SortCoordinate;
    let mut buildstate: BTBuildState = core::mem::zeroed();
    let scan: TableScanDesc;
    let reltuples: f64;
    let indexInfo: *mut IndexInfo;

    /* Initialize local tuplesort coordination state */
    coordinate = palloc0(size_of::<SortCoordinateData>()) as SortCoordinate;
    (*coordinate).isWorker = true;
    (*coordinate).nParticipants = -1;
    (*coordinate).sharedsort = sharedsort;

    /* Begin "partial" tuplesort */
    (*btspool).sortstate = tuplesort_begin_index_btree(
        (*btspool).heap,
        (*btspool).index,
        (*btspool).isunique,
        (*btspool).nulls_not_distinct,
        sortmem,
        coordinate,
        TUPLESORT_NONE,
    );

    /*
     * Just as with serial case, there may be a second spool.  If so, a
     * second, dedicated spool2 partial tuplesort is required.
     */
    if !btspool2.is_null() {
        let coordinate2: SortCoordinate;

        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem (unless sortmem is less for
         * worker).  Worker processes are generally permitted to allocate
         * work_mem independently.
         */
        coordinate2 = palloc0(size_of::<SortCoordinateData>()) as SortCoordinate;
        (*coordinate2).isWorker = true;
        (*coordinate2).nParticipants = -1;
        (*coordinate2).sharedsort = sharedsort2;
        (*btspool2).sortstate = tuplesort_begin_index_btree(
            (*btspool).heap,
            (*btspool).index,
            false,
            false,
            Min(sortmem, work_mem),
            coordinate2,
            false as c_int,
        );
    }

    /* Fill in buildstate for _bt_build_callback() */
    buildstate.isunique = (*btshared).isunique;
    buildstate.nulls_not_distinct = (*btshared).nulls_not_distinct;
    buildstate.havedead = false;
    buildstate.heap = (*btspool).heap;
    buildstate.spool = btspool;
    buildstate.spool2 = btspool2;
    buildstate.indtuples = 0.0;
    buildstate.btleader = null_mut();

    /* Join parallel scan */
    indexInfo = BuildIndexInfo((*btspool).index);
    (*indexInfo).ii_Concurrent = (*btshared).isconcurrent;
    scan = table_beginscan_parallel(
        (*btspool).heap,
        ParallelTableScanFromBTShared(btshared),
    );
    reltuples = table_index_build_scan(
        (*btspool).heap,
        (*btspool).index,
        indexInfo,
        true,
        progress,
        _bt_build_callback,
        &raw mut buildstate as *mut c_void,
        scan,
    );

    /* Execute this worker's part of the sort */
    if progress {
        pgstat_progress_update_param(
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_BTREE_PHASE_PERFORMSORT_1,
        );
    }
    tuplesort_performsort((*btspool).sortstate);
    if !btspool2.is_null() {
        if progress {
            pgstat_progress_update_param(
                PROGRESS_CREATEIDX_SUBPHASE,
                PROGRESS_BTREE_PHASE_PERFORMSORT_2,
            );
        }
        tuplesort_performsort((*btspool2).sortstate);
    }

    /*
     * Done.  Record ambuild statistics, and whether we encountered a broken
     * HOT chain.
     */
    SpinLockAcquire(&raw mut (*btshared).mutex);
    (*btshared).nparticipantsdone += 1;
    (*btshared).reltuples += reltuples;
    if buildstate.havedead {
        (*btshared).havedead = true;
    }
    (*btshared).indtuples += buildstate.indtuples;
    if (*indexInfo).ii_BrokenHotChain {
        (*btshared).brokenhotchain = true;
    }
    SpinLockRelease(&raw mut (*btshared).mutex);

    /* Notify leader */
    ConditionVariableSignal(&raw mut (*btshared).workersdonecv);

    /* We can end tuplesorts immediately */
    tuplesort_end((*btspool).sortstate);
    if !btspool2.is_null() {
        tuplesort_end((*btspool2).sortstate);
    }
}

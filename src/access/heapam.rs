//! Translated from PostgreSQL src/include/access/heapam.h
//! POSTGRES heap access method definitions.
//!
#![allow(
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]
#![allow(
    clippy::implicit_hasher,
    reason = "TODO(stub): drop when implemented; hollow stub; PG API uses a concrete HashMap, not a generic hasher"
)]
//!
//! HeapScanDescData/BitmapHeapScanDescData/IndexFetchHeapData are in-memory scan
//! state (idiomatic structs embedding their AM-independent base). The two flag
//! groups split per bitflags-port.md: HEAP_PAGE_PRUNE_* and HEAP_FREEZE_CHECK_*
//! are clean single-bit sets (GOOD). HEAP_INSERT_* alias TABLE_INSERT_* from
//! tableam (re-exported as consts on the same i32 word) plus the heap-only
//! SPECULATIVE bit. BulkInsertStateData resolves the tableam forward-decl.

use bitflags::bitflags;

use crate::access::heapam_xlog::{XLH_FREEZE_XVAC, XLH_INVALID_XVAC};
use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::htup_details::{HeapTupleHeaderData, MaxHeapTuplesPerPage};
use crate::access::relscan::{
    ParallelBlockTableScanWorkerData, ParallelTableScanDesc, TableScanDesc, TableScanDescData,
};
use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKey;
use crate::access::tableam::{
    TableInsertOptions, VacuumParams, TM_FailureData, TM_IndexDeleteOp, TM_Result, TU_UpdateIndexes,
};
use crate::access::transam::{FROZEN_TRANSACTION_ID, INVALID_TRANSACTION_ID};
use crate::c::{CommandId, MultiXactId, TransactionId};
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, BufferAccessStrategy};
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;

/// C `ItemPointer` is `ItemPointerData *`; itemptr.rs only exports the value type.
pub type ItemPointer = *mut ItemPointerData; // TODO(ptr)
use crate::commands::vacuum::VacuumCutoffs;
use crate::storage::off::OffsetNumber;
use crate::storage::read_stream::ReadStream;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::Snapshot;

// "options" flag bits for heap_insert. HEAP_INSERT_{SKIP_FSM,FROZEN,NO_LOGICAL}
// alias TABLE_INSERT_* (tableam); SPECULATIVE is heap-only. Same i32 word as
// TableInsertOptions, so callers may OR these with TableInsertOptions::bits().
pub const HEAP_INSERT_SKIP_FSM: i32 = TableInsertOptions::SKIP_FSM.bits();
pub const HEAP_INSERT_FROZEN: i32 = TableInsertOptions::FROZEN.bits();
pub const HEAP_INSERT_NO_LOGICAL: i32 = TableInsertOptions::NO_LOGICAL.bits();
pub const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

bitflags! {
    /// "options" flag bits for heap_page_prune_and_freeze (GOOD).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HeapPagePruneOptions: i32 {
        const MARK_UNUSED_NOW = 1 << 0;
        const FREEZE          = 1 << 1;
    }
}

// Type-forwarded slot/cutoffs structs (`struct TupleTableSlot;`, `struct
// VacuumCutoffs;` forward-decls in the C header). TupleTableSlot and VacuumCutoffs
// are imported above from executor::tuptable and commands::vacuum.

// GlobalVisState forward-decl (pruneheap.c / heapam_visibility.c use it).
pub use crate::utils::snapshot::GlobalVisState;

/// MaxLockTupleMode == LockTupleExclusive (the strongest tuple lock mode).
pub const MAX_LOCK_TUPLE_MODE: LockTupleMode = LockTupleMode::LockTupleExclusive;

/// BulkInsertState: handle to per-statement bulk-insert state. Resolves the
/// tableam.h forward-decl (`typedef struct BulkInsertStateData *BulkInsertState`).
pub type BulkInsertState = *mut BulkInsertStateData; // TODO(ptr)

/// Per-statement state used by heap_insert/heap_multi_insert to batch FSM lookups
/// and reuse a target buffer. Private to heapam.c in C; fields are opaque to
/// callers. In-memory: a placeholder until heapam.c is translated.
pub struct BulkInsertStateData {
    // BufferAccessStrategy strategy; Buffer current_buf; ... (private to heapam.c)
    _private: [u8; 0],
}

/// Descriptor for heap table scans (in-memory). Embeds the AM-independent base.
pub struct HeapScanDescData {
    pub base: TableScanDescData, // AM independent part of the descriptor

    // state set up at initscan time
    pub nblocks: BlockNumber,   // total number of blocks in rel
    pub startblock: BlockNumber, // block # to start at
    pub numblocks: BlockNumber, // max # of blocks to scan; usually InvalidBlockNumber

    // scan current state
    pub inited: bool,           // false = scan not init'd yet
    pub coffset: OffsetNumber,  // current offset # in non-page-at-a-time mode
    pub cblock: BlockNumber,    // current block # in scan, if any
    pub cbuf: Buffer,           // current buffer in scan, if any (pinned if valid)

    // access strategy for reads; C `BufferAccessStrategy` is a nullable pointer
    // (`*BufferAccessStrategyData`), so None == no strategy (the default).
    pub strategy: Option<BufferAccessStrategy>,

    pub ctup: HeapTupleData, // current tuple in scan, if any

    // For scans that stream reads
    pub read_stream: *mut ReadStream, // TODO(ptr)

    // Saved scan direction + prefetch block for read-stream seq/TID-range scans.
    pub dir: ScanDirection,
    pub prefetch_block: BlockNumber,

    // For parallel scans: page allocation data. NULL when not a parallel scan.
    pub parallelworkerdata: *mut ParallelBlockTableScanWorkerData, // TODO(ptr)

    // page-at-a-time mode + bitmap scans
    pub cindex: u32,  // current tuple's index in vistuples
    pub ntuples: u32, // number of visible tuples on page
    pub vistuples: [OffsetNumber; MaxHeapTuplesPerPage as usize], // their offsets
}
pub type HeapScanDesc = *mut HeapScanDescData; // TODO(ptr)

/// Bitmap heap scan descriptor: just the heap scan base (holds no extra data).
pub struct BitmapHeapScanDescData {
    pub rs_heap_base: HeapScanDescData,
}
pub type BitmapHeapScanDesc = *mut BitmapHeapScanDescData; // TODO(ptr)

/// Descriptor for fetches from heap via an index (in-memory).
pub struct IndexFetchHeapData {
    pub base: crate::access::relscan::IndexFetchTableData, // AM independent part
    pub cbuf: Buffer, // current heap buffer in scan, if any (pinned if valid)
}

/// Result codes for HeapTupleSatisfiesVacuum (sequential ordinals -> enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HTSV_Result {
    Dead,             // tuple is dead and deletable
    Live,             // tuple is live (committed, no deleter)
    RecentlyDead,     // tuple is dead, but not deletable yet
    InsertInProgress, // inserting xact is still in progress
    DeleteInProgress, // deleting xact is still in progress
}

bitflags! {
    /// heap_prepare_freeze_tuple's `checkflags`: request pg_xact checks of a
    /// tuple's to-be-frozen xmin/xmax during heap_freeze_execute_prepared (GOOD).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HeapFreezeCheck: u8 {
        const XMIN_COMMITTED = 0x01;
        const XMAX_ABORTED   = 0x02;
    }
}

/// heap_prepare_freeze_tuple state describing how to freeze a tuple (in-memory).
pub struct HeapTupleFreeze {
    // Fields describing how to process tuple
    pub xmax: TransactionId,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub frzflags: u8,

    // xmin/xmax check flags (see HeapFreezeCheck)
    pub checkflags: u8,
    // Page offset number for tuple
    pub offset: OffsetNumber,
}

/// VACUUM page-level state tracking how to freeze all eligible tuples on a heap
/// page (in-memory). See the C header for the full design notes.
pub struct HeapPageFreeze {
    // Is heap_prepare_freeze_tuple caller required to freeze page?
    pub freeze_required: bool,

    // "Freeze" NewRelfrozenXid/NewRelminMxid trackers.
    pub FreezePageRelfrozenXid: TransactionId,
    pub FreezePageRelminMxid: MultiXactId,

    // "No freeze" NewRelfrozenXid/NewRelminMxid trackers.
    pub NoFreezePageRelfrozenXid: TransactionId,
    pub NoFreezePageRelminMxid: MultiXactId,
}

/// Per-page state returned by heap_page_prune_and_freeze() (in-memory).
pub struct PruneFreezeResult {
    pub ndeleted: i32,   // Number of tuples deleted from the page
    pub nnewlpdead: i32, // Number of newly LP_DEAD items
    pub nfrozen: i32,    // Number of tuples we froze

    // Number of live and recently dead tuples on the page, after pruning
    pub live_tuples: i32,
    pub recently_dead_tuples: i32,

    // VM all-visible/all-frozen eligibility after pruning; only set with FREEZE.
    pub all_visible: bool,
    pub all_frozen: bool,
    pub vm_conflict_horizon: TransactionId,

    // Whether the page makes rel truncation unsafe.
    pub hastup: bool,

    // LP_DEAD items on the page after pruning (includes existing ones).
    pub lpdead_items: i32,
    pub deadoffsets: [OffsetNumber; MaxHeapTuplesPerPage as usize],
}

/// 'reason' codes for heap_page_prune_and_freeze() (sequential ordinals -> enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneReason {
    OnAccess,       // on-access pruning
    VacuumScan,     // VACUUM 1st heap pass
    VacuumCleanup,  // VACUUM 2nd heap pass
}

/// HeapScanIsValid - True iff the heap scan is valid (non-null).
pub fn HeapScanIsValid(scan: Option<&HeapScanDescData>) -> bool {
    scan.is_some()
}

// ---- function prototypes for heap access method (stubs) ----

pub fn heap_beginscan(
    _relation: Relation,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: ScanKey,
    _parallel_scan: ParallelTableScanDesc,
    _flags: u32,
) -> TableScanDesc {
    unimplemented!()
}

pub fn heap_setscanlimits(_sscan: TableScanDesc, _start_blk: BlockNumber, _num_blks: BlockNumber) {
    unimplemented!()
}

pub fn heap_prepare_pagescan(_sscan: TableScanDesc) {
    unimplemented!()
}

pub fn heap_rescan(
    _sscan: TableScanDesc,
    _key: ScanKey,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
    unimplemented!()
}

pub fn heap_endscan(_sscan: TableScanDesc) {
    unimplemented!()
}

pub fn heap_getnext(_sscan: TableScanDesc, _direction: ScanDirection) -> HeapTuple {
    unimplemented!()
}

pub fn heap_getnextslot(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

pub fn heap_set_tidrange(_sscan: TableScanDesc, _mintid: ItemPointer, _maxtid: ItemPointer) {
    unimplemented!()
}

pub fn heap_getnextslot_tidrange(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

pub fn heap_fetch(
    _relation: Relation,
    _snapshot: Snapshot,
    _tuple: HeapTuple,
    _userbuf: &mut Buffer,
    _keep_buf: bool,
) -> bool {
    unimplemented!()
}

/// Returns (found, all_dead). C out-param `*all_dead` folded into the tuple.
pub fn heap_hot_search_buffer(
    _tid: ItemPointer,
    _relation: Relation,
    _buffer: Buffer,
    _snapshot: Snapshot,
    _heap_tuple: HeapTuple,
    _first_call: bool,
) -> (bool, bool) {
    unimplemented!()
}

pub fn heap_get_latest_tid(_sscan: TableScanDesc, _tid: ItemPointer) {
    unimplemented!()
}

pub fn GetBulkInsertState() -> BulkInsertState {
    unimplemented!()
}

pub fn FreeBulkInsertState(_bistate: BulkInsertState) {
    unimplemented!()
}

pub fn ReleaseBulkInsertStatePin(_bistate: BulkInsertState) {
    unimplemented!()
}

pub fn heap_insert(
    _relation: Relation,
    _tup: HeapTuple,
    _cid: CommandId,
    _options: i32,
    _bistate: BulkInsertState,
) {
    unimplemented!()
}

pub fn heap_multi_insert(
    _relation: Relation,
    _slots: &mut [*mut TupleTableSlot],
    _ntuples: i32,
    _cid: CommandId,
    _options: i32,
    _bistate: BulkInsertState,
) {
    unimplemented!()
}

pub fn heap_delete(
    _relation: Relation,
    _tid: ItemPointer,
    _cid: CommandId,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: &mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result {
    unimplemented!()
}

pub fn heap_finish_speculative(_relation: Relation, _tid: ItemPointer) {
    unimplemented!()
}

pub fn heap_abort_speculative(_relation: Relation, _tid: ItemPointer) {
    unimplemented!()
}

/// Returns (result, lockmode, update_indexes). C `*lockmode`/`*update_indexes`
/// out-params folded into the return tuple.
pub fn heap_update(
    _relation: Relation,
    _otid: ItemPointer,
    _newtup: HeapTuple,
    _cid: CommandId,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: &mut TM_FailureData,
) -> (TM_Result, LockTupleMode, TU_UpdateIndexes) {
    unimplemented!()
}

/// Returns (result, buffer). C `*buffer` out-param folded into the return tuple.
pub fn heap_lock_tuple(
    _relation: Relation,
    _tuple: HeapTuple,
    _cid: CommandId,
    _mode: LockTupleMode,
    _wait_policy: LockWaitPolicy,
    _follow_updates: bool,
    _tmfd: &mut TM_FailureData,
) -> (TM_Result, Buffer) {
    unimplemented!()
}

/// `void (*release_callback)(void *), void *arg` -> a captured closure (6.3).
pub fn heap_inplace_lock(
    _relation: Relation,
    _oldtup_ptr: HeapTuple,
    _buffer: Buffer,
    _release_callback: impl FnMut(),
) -> bool {
    unimplemented!()
}

pub fn heap_inplace_update_and_unlock(
    _relation: Relation,
    _oldtup: HeapTuple,
    _tuple: HeapTuple,
    _buffer: Buffer,
) {
    unimplemented!()
}

pub fn heap_inplace_unlock(_relation: Relation, _oldtup: HeapTuple, _buffer: Buffer) {
    unimplemented!()
}

/// Returns (ok, totally_frozen). C `*totally_frozen` out-param folded in.
#[allow(deprecated)]
pub fn heap_prepare_freeze_tuple(
    _tuple: &mut HeapTupleHeaderData,
    _cutoffs: &VacuumCutoffs,
    _pagefrz: &mut HeapPageFreeze,
    _frz: &mut HeapTupleFreeze,
) -> (bool, bool) {
    unimplemented!()
}

pub fn heap_pre_freeze_checks(_buffer: Buffer, _tuples: &mut [HeapTupleFreeze]) {
    unimplemented!()
}

pub fn heap_freeze_prepared_tuples(_buffer: Buffer, _tuples: &mut [HeapTupleFreeze]) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn heap_freeze_tuple(
    _tuple: &mut HeapTupleHeaderData,
    _relfrozenxid: TransactionId,
    _relminmxid: TransactionId,
    _freeze_limit: TransactionId,
    _multi_xact_cutoff: TransactionId,
) -> bool {
    unimplemented!()
}

/// Returns (should_freeze, no_freeze_relfrozenxid, no_freeze_relminmxid). C
/// `*NoFreezePageRelfrozenXid`/`*NoFreezePageRelminMxid` out-params folded in.
#[allow(deprecated)]
pub fn heap_tuple_should_freeze(
    _tuple: &mut HeapTupleHeaderData,
    _cutoffs: &VacuumCutoffs,
) -> (bool, TransactionId, MultiXactId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn heap_tuple_needs_eventual_freeze(_tuple: &mut HeapTupleHeaderData) -> bool {
    unimplemented!()
}

pub fn simple_heap_insert(_relation: Relation, _tup: HeapTuple) {
    unimplemented!()
}

pub fn simple_heap_delete(_relation: Relation, _tid: ItemPointer) {
    unimplemented!()
}

/// Returns update_indexes. C `*update_indexes` out-param folded into the return.
pub fn simple_heap_update(
    _relation: Relation,
    _otid: ItemPointer,
    _tup: HeapTuple,
) -> TU_UpdateIndexes {
    unimplemented!()
}

pub fn heap_index_delete_tuples(_rel: Relation, _delstate: &mut TM_IndexDeleteOp) -> TransactionId {
    unimplemented!()
}

// in heap/pruneheap.c

pub fn heap_page_prune_opt(_relation: Relation, _buffer: Buffer) {
    unimplemented!()
}

/// Returns (new_relfrozen_xid, new_relmin_mxid). The two trailing C `*new_*`
/// out-params folded into the return; `presult` filled in place.
pub fn heap_page_prune_and_freeze(
    _relation: Relation,
    _buffer: Buffer,
    _vistest: &mut GlobalVisState,
    _options: i32,
    _cutoffs: &mut VacuumCutoffs,
    _presult: &mut PruneFreezeResult,
    _reason: PruneReason,
    _off_loc: &mut OffsetNumber,
) -> (TransactionId, MultiXactId) {
    unimplemented!()
}

pub fn heap_page_prune_execute(
    _buffer: Buffer,
    _lp_truncate_only: bool,
    _redirected: &[OffsetNumber],
    _nowdead: &[OffsetNumber],
    _nowunused: &[OffsetNumber],
) {
    unimplemented!()
}

/// Fills `root_offsets` (MaxHeapTuplesPerPage entries) in place.
pub fn heap_get_root_tuples(_page: &Page, _root_offsets: &mut [OffsetNumber]) {
    unimplemented!()
}

pub fn log_heap_prune_and_freeze(
    _relation: Relation,
    _buffer: Buffer,
    _conflict_xid: TransactionId,
    _cleanup_lock: bool,
    _reason: PruneReason,
    _frozen: &[HeapTupleFreeze],
    _redirected: &[OffsetNumber],
    _dead: &[OffsetNumber],
    _unused: &[OffsetNumber],
) {
    unimplemented!()
}

// in heap/vacuumlazy.c

#[allow(deprecated)]
pub fn heap_vacuum_rel(
    _rel: Relation,
    _params: &mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
    unimplemented!()
}

// in heap/heapam_visibility.c

pub fn HeapTupleSatisfiesVisibility(_htup: HeapTuple, _snapshot: Snapshot, _buffer: Buffer) -> bool {
    unimplemented!()
}

pub fn HeapTupleSatisfiesUpdate(_htup: HeapTuple, _curcid: CommandId, _buffer: Buffer) -> TM_Result {
    unimplemented!()
}

pub fn HeapTupleSatisfiesVacuum(
    _htup: HeapTuple,
    _oldest_xmin: TransactionId,
    _buffer: Buffer,
) -> HTSV_Result {
    unimplemented!()
}

/// Returns (result, dead_after). C `*dead_after` out-param folded in.
pub fn HeapTupleSatisfiesVacuumHorizon(
    _htup: HeapTuple,
    _buffer: Buffer,
) -> (HTSV_Result, TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn HeapTupleSetHintBits(
    _tuple: &mut HeapTupleHeaderData,
    _buffer: Buffer,
    _infomask: u16,
    _xid: TransactionId,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn HeapTupleHeaderIsOnlyLocked(_tuple: &mut HeapTupleHeaderData) -> bool {
    unimplemented!()
}

pub fn HeapTupleIsSurelyDead(_htup: HeapTuple, _vistest: &mut GlobalVisState) -> bool {
    unimplemented!()
}

/// Returns (resolved, cmin, cmax). C `*cmin`/`*cmax` out-params folded in; the
/// `HTAB *tuplecid_data` becomes a Rust HashMap (collections table). Implemented
/// in reorderbuffer.c, not heapam_visibility.c.
pub fn ResolveCminCmaxDuringDecoding(
    _tuplecid_data: &mut std::collections::HashMap<ItemPointer, (CommandId, CommandId)>,
    _snapshot: Snapshot,
    _htup: HeapTuple,
    _buffer: Buffer,
) -> (bool, CommandId, CommandId) {
    unimplemented!()
}

pub fn HeapCheckForSerializableConflictOut(
    _visible: bool,
    _relation: Relation,
    _tuple: HeapTuple,
    _buffer: Buffer,
    _snapshot: Snapshot,
) {
    unimplemented!()
}

/// heap_execute_freeze_tuple - execute the prepared freezing of a tuple
/// (static inline -> translated in full). Caller ensures exclusive access.
#[allow(deprecated)]
pub fn heap_execute_freeze_tuple(tuple: &mut HeapTupleHeaderData, frz: &HeapTupleFreeze) {
    tuple.set_xmax(frz.xmax);

    if frz.frzflags & XLH_FREEZE_XVAC != 0 {
        tuple.set_xvac(FROZEN_TRANSACTION_ID);
    }

    if frz.frzflags & XLH_INVALID_XVAC != 0 {
        tuple.set_xvac(INVALID_TRANSACTION_ID);
    }

    tuple.t_infomask = frz.t_infomask;
    tuple.t_infomask2 = frz.t_infomask2;
}

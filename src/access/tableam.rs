//! Translated from PostgreSQL src/include/access/tableam.h
//! POSTGRES table access method definitions.
//!
#![allow(
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]
//!
//! Per routine-struct.md: `TableAmRoutine` (a vtable of fn pointers) becomes a
//! base trait `TableAm` carrying the required callbacks plus a scan-handle
//! factory; optional callback groups (TID-range, bitmap scan, sample scan,
//! `finish_bulk_insert`, TOAST fetch) become capability supertraits. The `SO_*`
//! ScanOptions split: `SO_TYPE_*` is a mutually-exclusive selector (kept as flag
//! bits with a note + a `ScanType` enum view) while `SO_ALLOW_*`/`SO_TEMP_SNAPSHOT`
//! are real flags -> `ScanOptions` bitflags (PARTIAL per bitflags-port.md appendix
//! B). `TABLE_INSERT_*` and `TUPLE_LOCK_FLAG_*` -> bitflags (GOOD). The many
//! `table_*` static-inline wrappers delegate to the trait (stubbed bodies). The
//! forward-declared structs (IndexInfo, SampleScanState, ...) get rule-7 local
//! placeholders.

use bitflags::bitflags;

use crate::access::hio::BulkInsertStateData;
use crate::access::relscan::{
    IndexFetchTableData, ParallelBlockTableScanDescData, ParallelBlockTableScanWorkerData,
    ParallelTableScanDesc, TableScanDesc,
};
use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKeyData;
use crate::c::{varlena, CommandId, MultiXactId, TransactionId};
use crate::catalog::index::ValidateIndexState;
pub use crate::commands::vacuum::VacuumParams;
use crate::common::relpath::ForkNumber;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::nodes::execnodes::{IndexInfo, SampleScanState};
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::BufferAccessStrategy;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::storage::read_stream::ReadStream;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::snapshot::Snapshot;
use std::sync::Arc;
use crate::utils::rel::RelationData;

/// Default table access method name.
pub const DEFAULT_TABLE_ACCESS_METHOD: &str = "heap";

// GUCs (process globals in C; will become session/config state in Phase 2).
pub static mut default_table_access_method: &str = DEFAULT_TABLE_ACCESS_METHOD;
pub static mut synchronize_seqscans: bool = true;

bitflags! {
    /// `ScanOptions`: the `flags` bitmask passed to scan_begin. PARTIAL (appendix
    /// B): the `TYPE_*` bits are a mutually-exclusive selector (at most one set,
    /// not OR-able as a set) while `ALLOW_*` and `TEMP_SNAPSHOT` are independent
    /// flags. Kept as one word to match the C field (`rs_flags`); use
    /// `ScanType::from_options` to read the exclusive type, `contains` for the
    /// rest.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ScanOptions: u32 {
        // one of TYPE_* may be specified (mutually exclusive, see ScanType)
        const TYPE_SEQSCAN      = 1 << 0;
        const TYPE_BITMAPSCAN   = 1 << 1;
        const TYPE_SAMPLESCAN   = 1 << 2;
        const TYPE_TIDSCAN      = 1 << 3;
        const TYPE_TIDRANGESCAN = 1 << 4;
        const TYPE_ANALYZE      = 1 << 5;
        // several of ALLOW_* may be specified
        const ALLOW_STRAT       = 1 << 6; // allow use of access strategy
        const ALLOW_SYNC        = 1 << 7; // report location to syncscan logic?
        const ALLOW_PAGEMODE    = 1 << 8; // verify visibility page-at-a-time?
        const TEMP_SNAPSHOT     = 1 << 9; // unregister snapshot at scan end?
        /// Mask of the mutually-exclusive TYPE_* bits.
        const TYPE_MASK = Self::TYPE_SEQSCAN.bits() | Self::TYPE_BITMAPSCAN.bits()
            | Self::TYPE_SAMPLESCAN.bits() | Self::TYPE_TIDSCAN.bits()
            | Self::TYPE_TIDRANGESCAN.bits() | Self::TYPE_ANALYZE.bits();
    }
}

/// The mutually-exclusive `SO_TYPE_*` selector extracted from `ScanOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanType {
    SeqScan,
    BitmapScan,
    SampleScan,
    TidScan,
    TidRangeScan,
    Analyze,
}

impl ScanType {
    /// Read the exclusive scan type from a `ScanOptions` word, if one is set.
    pub fn from_options(opts: ScanOptions) -> Option<Self> {
        match opts.intersection(ScanOptions::TYPE_MASK) {
            ScanOptions::TYPE_SEQSCAN => Some(Self::SeqScan),
            ScanOptions::TYPE_BITMAPSCAN => Some(Self::BitmapScan),
            ScanOptions::TYPE_SAMPLESCAN => Some(Self::SampleScan),
            ScanOptions::TYPE_TIDSCAN => Some(Self::TidScan),
            ScanOptions::TYPE_TIDRANGESCAN => Some(Self::TidRangeScan),
            ScanOptions::TYPE_ANALYZE => Some(Self::Analyze),
            _ => None,
        }
    }
}

/// Result codes for table_{update,delete,lock_tuple} and AM visibility routines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TM_Result {
    /// action succeeded (update/delete performed, lock acquired)
    Ok,
    /// affected tuple wasn't visible to the relevant snapshot
    Invisible,
    /// affected tuple was already modified by the calling backend
    SelfModified,
    /// affected tuple was updated by another transaction (incl. cross-partition)
    Updated,
    /// affected tuple was deleted by another transaction
    Deleted,
    /// affected tuple is currently being modified by another session
    BeingModified,
    /// lock couldn't be acquired, action skipped (lock_tuple only)
    WouldBlock,
}

/// Result codes for table_update(..., update_indexes): which indexes to update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TU_UpdateIndexes {
    /// No indexed columns were updated (incl. TID addressing of tuple)
    None,
    /// A non-summarizing indexed column was updated, or the TID changed
    All,
    /// Only summarized columns were updated, TID is unchanged
    Summarizing,
}

/// Filled by tuple_{update,delete,lock} on failure (outdated target tuple).
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    pub cmax: CommandId,
    pub traversed: bool,
}

/// One entry of the deltids array in `TM_IndexDeleteOp` (table TID + status idx).
pub struct TM_IndexDelete {
    pub tid: ItemPointerData, // table TID from index tuple
    pub id: i16,              // offset into TM_IndexStatus array
}

/// Per-table-tuple deletion status, parallel to the deltids array.
pub struct TM_IndexStatus {
    pub idxoffnum: OffsetNumber, // index AM page offset number
    pub knowndeletable: bool,    // currently known to be deletable?
    // bottom-up index deletion specific fields follow
    pub promising: bool, // promising (duplicate) index tuple?
    pub freespace: i16,  // space freed in index if deleted
}

/// State for table_index_delete_tuples(). The two C arrays are kept as `Vec`.
pub struct TM_IndexDeleteOp {
    pub irel: Arc<RelationData>,         // target index relation
    pub iblknum: BlockNumber,   // index block number (for error reports)
    pub bottomup: bool,         // bottom-up (not simple) deletion?
    pub bottomupfreespace: i32, // bottom-up space target
    pub deltids: Vec<TM_IndexDelete>,
    pub status: Vec<TM_IndexStatus>,
}

bitflags! {
    /// "options" flag bits for table_tuple_insert / multi_insert (GOOD).
    /// SKIP_WAL was 0x0001 and is gone; RelationNeedsWAL() now governs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TableInsertOptions: i32 {
        const SKIP_FSM    = 0x0002;
        const FROZEN      = 0x0004;
        const NO_LOGICAL  = 0x0008;
    }
}

bitflags! {
    /// flag bits for table_tuple_lock (GOOD).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TupleLockFlags: u8 {
        /// follow tuples whose update is in progress if lock modes don't conflict
        const LOCK_UPDATE_IN_PROGRESS = 1 << 0;
        /// follow update chain and lock latest version of tuple
        const FIND_LAST_VERSION       = 1 << 1;
    }
}

/// Callback for table_index_build_scan. The C `void *state` opaque context is
/// captured by the closure (function-mapping.md 6.3).
pub type IndexBuildCallback<'a> = dyn FnMut(
        Arc<RelationData>,          // index
        &ItemPointerData,  // tid
        &[Datum],          // values
        &[bool],           // isnull
        bool,              // tupleIsAlive
    ) + 'a;

/// Outputs of relation_set_new_filelocator (the two `*freezeXid`/`*minmulti`
/// out-params, function-mapping.md 5.2).
pub struct NewFilelocatorCutoffs {
    pub freeze_xid: TransactionId,
    pub minmulti: MultiXactId,
}

/// Outputs of relation_copy_for_cluster (the mutable cutoffs + stats out-params).
pub struct ClusterCopyResult {
    pub xid_cutoff: TransactionId,
    pub multi_cutoff: MultiXactId,
    pub num_tuples: f64,
    pub tups_vacuumed: f64,
    pub tups_recently_dead: f64,
}

/// Outputs of relation_estimate_size (the planner out-params,
/// function-mapping.md 5.3).
pub struct RelationSizeEstimate {
    pub pages: BlockNumber,
    pub tuples: f64,
    pub allvisfrac: f64,
}

/// Per-tuple outcome of index_fetch_tuple: whether a tuple was stored, plus the
/// `call_again`/`all_dead` signals (C out-params, function-mapping.md 5).
pub struct IndexFetchResult {
    pub found: bool,
    pub call_again: bool,
    pub all_dead: bool,
}

/// Page-level EXPLAIN ANALYZE counters incremented by scan_bitmap_next_tuple.
pub struct BitmapPageCounts {
    pub lossy_pages: u64,
    pub exact_pages: u64,
}

/// Live-/dead-row tallies updated by scan_analyze_next_tuple.
pub struct AnalyzeRowCounts {
    pub liverows: f64,
    pub deadrows: f64,
}

/// Base trait for a table access method (C `TableAmRoutine`). The required
/// callbacks plus the scan factory; optional groups are capability supertraits
/// below. `GetTableAmRoutine` asserts the required callbacks are present, so
/// these are non-defaulted methods. The C `NodeTag type` field is dropped (the
/// trait is its own discriminant). `void *opaque` per-scan state lives in the
/// AM's concrete scan descriptor (it embeds `TableScanDescData`).
#[allow(clippy::too_many_arguments)]
pub trait TableAm {
    // --- Slot related callbacks ---

    /// slot_callbacks: slot implementation suitable for this AM's tuples.
    fn slot_callbacks(&self, rel: &RelationData) -> &'static dyn TupleTableSlotOps;

    // --- Table scan callbacks (required) ---

    /// scan_begin: start a scan of `rel`; returns the AM's scan descriptor.
    fn scan_begin(
        &self,
        rel: &RelationData,
        snapshot: Snapshot,
        nkeys: i32,
        key: &mut [ScanKeyData],
        pscan: ParallelTableScanDesc,
        flags: ScanOptions,
    ) -> TableScanDesc;

    /// scan_end: release resources and deallocate a scan. (The C base descriptor
    /// is shared raw state, so this stays explicit rather than becoming Drop.)
    fn scan_end(&self, scan: TableScanDesc);

    /// scan_rescan: restart a scan, optionally re-applying allow_* params.
    fn scan_rescan(
        &self,
        scan: TableScanDesc,
        key: &mut [ScanKeyData],
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    );

    /// scan_getnextslot: next tuple from `scan` into `slot`; false at end.
    fn scan_getnextslot(
        &self,
        scan: TableScanDesc,
        direction: ScanDirection,
        slot: &mut TupleTableSlot,
    ) -> bool;

    // --- Parallel table scan (required) ---

    /// parallelscan_estimate: shared-memory size for a parallel scan.
    fn parallelscan_estimate(&self, rel: &RelationData) -> usize;

    /// parallelscan_initialize: init shared parallel scan state; returns its size.
    fn parallelscan_initialize(&self, rel: &RelationData, pscan: ParallelTableScanDesc) -> usize;

    /// parallelscan_reinitialize: reset shared state for a new scan.
    fn parallelscan_reinitialize(&self, rel: &RelationData, pscan: ParallelTableScanDesc);

    // --- Index scan callbacks (required; asserted by GetTableAmRoutine) ---

    /// index_fetch_begin: prepare to fetch tuples for an index scan.
    fn index_fetch_begin(&self, rel: &RelationData) -> *mut IndexFetchTableData;

    /// index_fetch_reset: release cross-fetch resources.
    fn index_fetch_reset(&self, data: *mut IndexFetchTableData);

    /// index_fetch_end: release resources and deallocate the index fetch.
    fn index_fetch_end(&self, data: *mut IndexFetchTableData);

    /// index_fetch_tuple: fetch tuple at `tid` into `slot` with a visibility
    /// test; returns found plus the call_again/all_dead signals (C out-params).
    fn index_fetch_tuple(
        &self,
        scan: *mut IndexFetchTableData,
        tid: &ItemPointerData,
        snapshot: Snapshot,
        slot: &mut TupleTableSlot,
    ) -> IndexFetchResult;

    // --- Non-modifying operations on individual tuples (required) ---

    /// tuple_fetch_row_version: fetch the version exactly at `tid` into `slot`.
    fn tuple_fetch_row_version(
        &self,
        rel: &RelationData,
        tid: &ItemPointerData,
        snapshot: Snapshot,
        slot: &mut TupleTableSlot,
    ) -> bool;

    /// tuple_tid_valid: is `tid` valid for a scan of this relation?
    fn tuple_tid_valid(&self, scan: TableScanDesc, tid: &ItemPointerData) -> bool;

    /// tuple_get_latest_tid: advance `tid` to the newest version.
    fn tuple_get_latest_tid(&self, scan: TableScanDesc, tid: &mut ItemPointerData);

    /// tuple_satisfies_snapshot: does the tuple in `slot` satisfy `snapshot`?
    fn tuple_satisfies_snapshot(
        &self,
        rel: &RelationData,
        slot: &mut TupleTableSlot,
        snapshot: Snapshot,
    ) -> bool;

    /// index_delete_tuples: which index TIDs are safe to delete; returns the
    /// snapshotConflictHorizon XID.
    fn index_delete_tuples(&self, rel: &RelationData, delstate: &mut TM_IndexDeleteOp) -> TransactionId;

    // --- Manipulations of physical tuples (required) ---

    /// tuple_insert: insert the tuple from `slot`.
    fn tuple_insert(
        &self,
        rel: &RelationData,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        options: TableInsertOptions,
        bistate: *mut BulkInsertStateData,
    );

    /// tuple_insert_speculative: speculative insert (INSERT .. ON CONFLICT).
    fn tuple_insert_speculative(
        &self,
        rel: &RelationData,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        options: TableInsertOptions,
        bistate: *mut BulkInsertStateData,
        spec_token: u32,
    );

    /// tuple_complete_speculative: confirm/abort a speculative insert.
    fn tuple_complete_speculative(
        &self,
        rel: &RelationData,
        slot: &mut TupleTableSlot,
        spec_token: u32,
        succeeded: bool,
    );

    /// multi_insert: insert multiple tuples in one operation.
    fn multi_insert(
        &self,
        rel: &RelationData,
        slots: &mut [*mut TupleTableSlot],
        cid: CommandId,
        options: TableInsertOptions,
        bistate: *mut BulkInsertStateData,
    );

    /// tuple_delete: delete the tuple at `tid`. On failure, fills `tmfd`.
    fn tuple_delete(
        &self,
        rel: &RelationData,
        tid: &ItemPointerData,
        cid: CommandId,
        snapshot: Snapshot,
        crosscheck: Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> TM_Result;

    /// tuple_update: replace the old tuple at `otid`. Reports the acquired lock
    /// mode and which indexes need updating (the two C out-params).
    fn tuple_update(
        &self,
        rel: &RelationData,
        otid: &ItemPointerData,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        snapshot: Snapshot,
        crosscheck: Snapshot,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut LockTupleMode,
        update_indexes: &mut TU_UpdateIndexes,
    ) -> TM_Result;

    /// tuple_lock: lock the tuple at `tid` in the given mode.
    fn tuple_lock(
        &self,
        rel: &RelationData,
        tid: &ItemPointerData,
        snapshot: Snapshot,
        slot: &mut TupleTableSlot,
        cid: CommandId,
        mode: LockTupleMode,
        wait_policy: LockWaitPolicy,
        flags: TupleLockFlags,
        tmfd: &mut TM_FailureData,
    ) -> TM_Result;

    // --- DDL related functionality (required) ---

    /// relation_set_new_filelocator: create new storage; returns freeze cutoffs.
    fn relation_set_new_filelocator(
        &self,
        rel: &RelationData,
        newrlocator: &RelFileLocator,
        persistence: u8,
    ) -> NewFilelocatorCutoffs;

    /// relation_nontransactional_truncate: drop all contents of current storage.
    fn relation_nontransactional_truncate(&self, rel: &RelationData);

    /// relation_copy_data: copy storage to a new relfilelocator.
    fn relation_copy_data(&self, rel: &RelationData, newrlocator: &RelFileLocator);

    /// relation_copy_for_cluster: copy/sort for CLUSTER or VACUUM FULL.
    fn relation_copy_for_cluster(
        &self,
        old_table: &RelationData,
        new_table: &RelationData,
        old_index: &RelationData,
        use_sort: bool,
        oldest_xmin: TransactionId,
        xid_cutoff: TransactionId,
        multi_cutoff: MultiXactId,
    ) -> ClusterCopyResult;

    /// relation_vacuum: react to a VACUUM command on the relation.
    fn relation_vacuum(
        &self,
        rel: &RelationData,
        params: &mut VacuumParams,
        bstrategy: Option<&BufferAccessStrategy>,
    );

    /// scan_analyze_next_block: prepare to analyze the next block; false if
    /// unsuitable for sampling.
    fn scan_analyze_next_block(&self, scan: TableScanDesc, stream: *mut ReadStream) -> bool;

    /// scan_analyze_next_tuple: next analyzable tuple into `slot`; updates the
    /// live/dead row tallies. false when the block is exhausted.
    fn scan_analyze_next_tuple(
        &self,
        scan: TableScanDesc,
        oldest_xmin: TransactionId,
        counts: &mut AnalyzeRowCounts,
        slot: &mut TupleTableSlot,
    ) -> bool;

    /// index_build_range_scan: scan the table to find tuples to index; returns
    /// the live-tuple count. `callback_state` is captured by the closure.
    fn index_build_range_scan(
        &self,
        table_rel: &RelationData,
        index_rel: &RelationData,
        index_info: &mut IndexInfo,
        allow_sync: bool,
        anyvisible: bool,
        progress: bool,
        start_blockno: BlockNumber,
        numblocks: BlockNumber,
        callback: &mut IndexBuildCallback,
        scan: TableScanDesc,
    ) -> f64;

    /// index_validate_scan: second table scan for a concurrent index build.
    fn index_validate_scan(
        &self,
        table_rel: &RelationData,
        index_rel: &RelationData,
        index_info: &mut IndexInfo,
        snapshot: Snapshot,
        state: &mut ValidateIndexState,
    );

    // --- Miscellaneous (required) ---

    /// relation_size: current size of `rel`'s `fork_number` in bytes.
    fn relation_size(&self, rel: &RelationData, fork_number: ForkNumber) -> u64;

    /// relation_needs_toast_table: does this relation need a TOAST table?
    fn relation_needs_toast_table(&self, rel: &RelationData) -> bool;

    /// relation_toast_am: OID of the table AM implementing TOAST for this AM.
    fn relation_toast_am(&self, rel: &RelationData) -> Oid;

    // --- Planner related (required) ---

    /// relation_estimate_size: planner size/row estimates (out-params + the
    /// in/out `attr_widths` cache).
    fn relation_estimate_size(&self, rel: &RelationData, attr_widths: &mut [i32]) -> RelationSizeEstimate;
}

/// finish_bulk_insert -- optional. Complete inserts made with a BulkInsertState.
pub trait FinishBulkInsert: TableAm {
    fn finish_bulk_insert(&self, rel: &RelationData, options: TableInsertOptions);
}

/// TID-range scanning -- optional group (both callbacks or neither).
pub trait TidRangeScan: TableAm {
    /// scan_set_tidrange: set the TID range for a tidrange scan.
    fn scan_set_tidrange(
        &self,
        scan: TableScanDesc,
        mintid: &ItemPointerData,
        maxtid: &ItemPointerData,
    );

    /// scan_getnextslot_tidrange: next tuple within the set TID range.
    fn scan_getnextslot_tidrange(
        &self,
        scan: TableScanDesc,
        direction: ScanDirection,
        slot: &mut TupleTableSlot,
    ) -> bool;
}

/// Bitmap heap scan -- optional (executor). `scan_bitmap_next_tuple`.
pub trait BitmapScan: TableAm {
    /// scan_bitmap_next_tuple: next bitmap-scan tuple into `slot`; the inner bool
    /// is `recheck`. None when no visible tuple was found. Increments the
    /// lossy/exact page counts.
    fn scan_bitmap_next_tuple(
        &self,
        scan: TableScanDesc,
        slot: &mut TupleTableSlot,
        counts: &mut BitmapPageCounts,
    ) -> Option<bool>;
}

/// TABLESAMPLE scan -- required to support sampling, otherwise the AM may error.
pub trait SampleScan: TableAm {
    /// scan_sample_next_block: prepare the next sample block; false if finished.
    fn scan_sample_next_block(&self, scan: TableScanDesc, scanstate: &mut SampleScanState) -> bool;

    /// scan_sample_next_tuple: next visible sample tuple into `slot`.
    fn scan_sample_next_tuple(
        &self,
        scan: TableScanDesc,
        scanstate: &mut SampleScanState,
        slot: &mut TupleTableSlot,
    ) -> bool;
}

/// detoasting hook -- needed only if this AM ever backs a TOAST table.
pub trait ToastFetch: TableAm {
    /// relation_fetch_toast_slice: fetch all/part of a TOAST value into `result`.
    fn relation_fetch_toast_slice(
        &self,
        toastrel: &RelationData,
        valueid: Oid,
        attrsize: i32,
        sliceoffset: i32,
        slicelength: i32,
        result: *mut varlena,
    );
}

// ----------------------------------------------------------------------------
// Slot functions.
// ----------------------------------------------------------------------------

/// Slot callbacks suitable for tuples of `relation` (tables/views/foreign/etc).
pub fn table_slot_callbacks(_relation: &RelationData) -> &'static dyn TupleTableSlotOps {
    unimplemented!()
}

/// Create a slot via table_slot_callbacks() and register it on `reglist`.
/// (C's `List **reglist` becomes the registration `Vec`.)
pub fn table_slot_create(
    _relation: &RelationData,
    _reglist: &mut Vec<*mut TupleTableSlot>,
) -> *mut TupleTableSlot {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Table scan functions (static inline wrappers -> delegate to the trait).
// ----------------------------------------------------------------------------

/// Start a scan of `rel` (seqscan, all allow_* on).
pub fn table_beginscan(
    _rel: &RelationData,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: &mut [ScanKeyData],
) -> TableScanDesc {
    unimplemented!()
}

/// Like table_beginscan(), but uses a catalog-appropriate snapshot.
pub fn table_beginscan_catalog(
    _relation: &RelationData,
    _nkeys: i32,
    _key: &mut [ScanKeyData],
) -> TableScanDesc {
    unimplemented!()
}

/// Like table_beginscan(), with control over access strategy and syncscan.
pub fn table_beginscan_strat(
    _rel: &RelationData,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: &mut [ScanKeyData],
    _allow_strat: bool,
    _allow_sync: bool,
) -> TableScanDesc {
    unimplemented!()
}

/// Set up a TableScanDesc for a bitmap heap scan.
pub fn table_beginscan_bm(
    _rel: &RelationData,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: &mut [ScanKeyData],
) -> TableScanDesc {
    unimplemented!()
}

/// Set up a TableScanDesc for a TABLESAMPLE scan.
pub fn table_beginscan_sampling(
    _rel: &RelationData,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: &mut [ScanKeyData],
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) -> TableScanDesc {
    unimplemented!()
}

/// Set up a TableScanDesc for a TID scan.
pub fn table_beginscan_tid(_rel: &RelationData, _snapshot: Snapshot) -> TableScanDesc {
    unimplemented!()
}

/// Set up a TableScanDesc for an ANALYZE scan.
pub fn table_beginscan_analyze(_rel: &RelationData) -> TableScanDesc {
    unimplemented!()
}

/// End a relation scan.
pub fn table_endscan(_scan: TableScanDesc) {
    unimplemented!()
}

/// Restart a relation scan.
pub fn table_rescan(_scan: TableScanDesc, _key: &mut [ScanKeyData]) {
    unimplemented!()
}

/// Restart a relation scan after changing buffer-strategy/syncscan/pagemode.
pub fn table_rescan_set_params(
    _scan: TableScanDesc,
    _key: &mut [ScanKeyData],
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
    unimplemented!()
}

/// Return next tuple from `scan` into `slot`.
pub fn table_scan_getnextslot(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// TID Range scanning related functions.
// ----------------------------------------------------------------------------

/// Set up a TableScanDesc for a TID range scan.
pub fn table_beginscan_tidrange(
    _rel: &RelationData,
    _snapshot: Snapshot,
    _mintid: &ItemPointerData,
    _maxtid: &ItemPointerData,
) -> TableScanDesc {
    unimplemented!()
}

/// Reset position and TID range of a tidrange scan.
pub fn table_rescan_tidrange(
    _sscan: TableScanDesc,
    _mintid: &ItemPointerData,
    _maxtid: &ItemPointerData,
) {
    unimplemented!()
}

/// Next tuple from a tidrange scan; false at end of range.
pub fn table_scan_getnextslot_tidrange(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Parallel table scan related functions.
// ----------------------------------------------------------------------------

/// Shared-memory size for a parallel scan of `rel`.
pub fn table_parallelscan_estimate(_rel: &RelationData, _snapshot: Snapshot) -> usize {
    unimplemented!()
}

/// Initialize `pscan` for a parallel scan; call once in the leader.
pub fn table_parallelscan_initialize(
    _rel: &RelationData,
    _pscan: ParallelTableScanDesc,
    _snapshot: Snapshot,
) {
    unimplemented!()
}

/// Begin a parallel scan against a previously-initialized `pscan`.
pub fn table_beginscan_parallel(
    _relation: &RelationData,
    _pscan: ParallelTableScanDesc,
) -> TableScanDesc {
    unimplemented!()
}

/// Restart a parallel scan (call in the leader).
pub fn table_parallelscan_reinitialize(_rel: &RelationData, _pscan: ParallelTableScanDesc) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Index scan related functions.
// ----------------------------------------------------------------------------

/// Prepare to fetch tuples for an index scan.
pub fn table_index_fetch_begin(_rel: &RelationData) -> *mut IndexFetchTableData {
    unimplemented!()
}

/// Reset an index fetch.
pub fn table_index_fetch_reset(_scan: *mut IndexFetchTableData) {
    unimplemented!()
}

/// Release resources and deallocate an index fetch.
pub fn table_index_fetch_end(_scan: *mut IndexFetchTableData) {
    unimplemented!()
}

/// Fetch the currently-visible row for an index entry's `tid` into `slot`.
pub fn table_index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    _tid: &ItemPointerData,
    _snapshot: Snapshot,
    _slot: &mut TupleTableSlot,
) -> IndexFetchResult {
    unimplemented!()
}

/// Convenience wrapper: do table tuples exist for this index entry? (unique
/// conflict check). Returns found + all_dead.
pub fn table_index_fetch_tuple_check(
    _rel: &RelationData,
    _tid: &ItemPointerData,
    _snapshot: Snapshot,
) -> (bool, bool) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Non-modifying operations on individual tuples.
// ----------------------------------------------------------------------------

/// Fetch the version exactly at `tid` into `slot`, with a visibility test.
pub fn table_tuple_fetch_row_version(
    _rel: &RelationData,
    _tid: &ItemPointerData,
    _snapshot: Snapshot,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

/// Is `tid` a potentially-valid tuple identifier for `scan`'s relation?
pub fn table_tuple_tid_valid(_scan: TableScanDesc, _tid: &ItemPointerData) -> bool {
    unimplemented!()
}

/// Advance `tid` to the latest version of the tuple.
pub fn table_tuple_get_latest_tid(_scan: TableScanDesc, _tid: &mut ItemPointerData) {
    unimplemented!()
}

/// Does the tuple in `slot` satisfy `snapshot`?
pub fn table_tuple_satisfies_snapshot(
    _rel: &RelationData,
    _slot: &mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    unimplemented!()
}

/// Determine which index tuples are safe to delete by their table TID; returns
/// the snapshotConflictHorizon XID.
pub fn table_index_delete_tuples(_rel: &RelationData, _delstate: &mut TM_IndexDeleteOp) -> TransactionId {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions for manipulations of physical tuples.
// ----------------------------------------------------------------------------

/// Insert a tuple from `slot`.
pub fn table_tuple_insert(
    _rel: &RelationData,
    _slot: &mut TupleTableSlot,
    _cid: CommandId,
    _options: TableInsertOptions,
    _bistate: *mut BulkInsertStateData,
) {
    unimplemented!()
}

/// Speculative insert (INSERT .. ON CONFLICT).
pub fn table_tuple_insert_speculative(
    _rel: &RelationData,
    _slot: &mut TupleTableSlot,
    _cid: CommandId,
    _options: TableInsertOptions,
    _bistate: *mut BulkInsertStateData,
    _spec_token: u32,
) {
    unimplemented!()
}

/// Complete a speculative insert (confirm if succeeded, else remove).
pub fn table_tuple_complete_speculative(
    _rel: &RelationData,
    _slot: &mut TupleTableSlot,
    _spec_token: u32,
    _succeeded: bool,
) {
    unimplemented!()
}

/// Insert multiple tuples in one operation.
pub fn table_multi_insert(
    _rel: &RelationData,
    _slots: &mut [*mut TupleTableSlot],
    _cid: CommandId,
    _options: TableInsertOptions,
    _bistate: *mut BulkInsertStateData,
) {
    unimplemented!()
}

/// Delete the tuple at `tid`. On failure fills `tmfd`.
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_delete(
    _rel: &RelationData,
    _tid: &ItemPointerData,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: &mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result {
    unimplemented!()
}

/// Update the tuple at `otid`; reports lock mode and index-update needs.
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_update(
    _rel: &RelationData,
    _otid: &ItemPointerData,
    _slot: &mut TupleTableSlot,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: &mut TM_FailureData,
    _lockmode: &mut LockTupleMode,
    _update_indexes: &mut TU_UpdateIndexes,
) -> TM_Result {
    unimplemented!()
}

/// Lock the tuple at `tid` in `mode`.
#[allow(clippy::too_many_arguments)]
pub fn table_tuple_lock(
    _rel: &RelationData,
    _tid: &ItemPointerData,
    _snapshot: Snapshot,
    _slot: &mut TupleTableSlot,
    _cid: CommandId,
    _mode: LockTupleMode,
    _wait_policy: LockWaitPolicy,
    _flags: TupleLockFlags,
    _tmfd: &mut TM_FailureData,
) -> TM_Result {
    unimplemented!()
}

/// Complete inserts made with a BulkInsertState (optional callback).
pub fn table_finish_bulk_insert(_rel: &RelationData, _options: TableInsertOptions) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// DDL related functionality.
// ----------------------------------------------------------------------------

/// Create storage for `rel` in `newrlocator`; returns freeze cutoffs.
pub fn table_relation_set_new_filelocator(
    _rel: &RelationData,
    _newrlocator: &RelFileLocator,
    _persistence: u8,
) -> NewFilelocatorCutoffs {
    unimplemented!()
}

/// Non-transactionally remove all contents of `rel`.
pub fn table_relation_nontransactional_truncate(_rel: &RelationData) {
    unimplemented!()
}

/// Copy data from `rel` into the new relfilelocator.
pub fn table_relation_copy_data(_rel: &RelationData, _newrlocator: &RelFileLocator) {
    unimplemented!()
}

/// Copy/sort data for CLUSTER or VACUUM FULL.
pub fn table_relation_copy_for_cluster(
    _old_table: &RelationData,
    _new_table: &RelationData,
    _old_index: &RelationData,
    _use_sort: bool,
    _oldest_xmin: TransactionId,
    _xid_cutoff: TransactionId,
    _multi_cutoff: MultiXactId,
) -> ClusterCopyResult {
    unimplemented!()
}

/// Perform VACUUM on the relation.
pub fn table_relation_vacuum(
    _rel: &RelationData,
    _params: &mut VacuumParams,
    _bstrategy: Option<&BufferAccessStrategy>,
) {
    unimplemented!()
}

/// Prepare to analyze the next block in the read stream; false if unsuitable.
pub fn table_scan_analyze_next_block(_scan: TableScanDesc, _stream: *mut ReadStream) -> bool {
    unimplemented!()
}

/// Iterate analyzable tuples in the current block; updates row counts.
pub fn table_scan_analyze_next_tuple(
    _scan: TableScanDesc,
    _oldest_xmin: TransactionId,
    _counts: &mut AnalyzeRowCounts,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

/// Scan the table to find tuples to index (whole table); returns live count.
pub fn table_index_build_scan(
    _table_rel: &RelationData,
    _index_rel: &RelationData,
    _index_info: &mut IndexInfo,
    _allow_sync: bool,
    _progress: bool,
    _callback: &mut IndexBuildCallback,
    _scan: TableScanDesc,
) -> f64 {
    unimplemented!()
}

/// As table_index_build_scan() but only `numblocks` blocks (or to end).
#[allow(clippy::too_many_arguments)]
pub fn table_index_build_range_scan(
    _table_rel: &RelationData,
    _index_rel: &RelationData,
    _index_info: &mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: BlockNumber,
    _numblocks: BlockNumber,
    _callback: &mut IndexBuildCallback,
    _scan: TableScanDesc,
) -> f64 {
    unimplemented!()
}

/// Second table scan validating a concurrently-built index.
pub fn table_index_validate_scan(
    _table_rel: &RelationData,
    _index_rel: &RelationData,
    _index_info: &mut IndexInfo,
    _snapshot: Snapshot,
    _state: &mut ValidateIndexState,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Miscellaneous functionality.
// ----------------------------------------------------------------------------

/// Current size of `rel`'s `fork_number` in bytes.
pub fn table_relation_size(_rel: &RelationData, _fork_number: ForkNumber) -> u64 {
    unimplemented!()
}

/// Does this relation need a TOAST table?
pub fn table_relation_needs_toast_table(_rel: &RelationData) -> bool {
    unimplemented!()
}

/// OID of the AM implementing the TOAST table for this relation.
pub fn table_relation_toast_am(_rel: &RelationData) -> Oid {
    unimplemented!()
}

/// Fetch all/part of a TOAST value from a TOAST table into `result`.
pub fn table_relation_fetch_toast_slice(
    _toastrel: &RelationData,
    _valueid: Oid,
    _attrsize: i32,
    _sliceoffset: i32,
    _slicelength: i32,
    _result: *mut varlena,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Planner related functionality.
// ----------------------------------------------------------------------------

/// AM-specific size estimate for estimate_rel_size().
pub fn table_relation_estimate_size(
    _rel: &RelationData,
    _attr_widths: &mut [i32],
) -> RelationSizeEstimate {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Executor related functionality.
// ----------------------------------------------------------------------------

/// Fetch/check the next bitmap-scan tuple into `slot`; reports recheck + counts.
pub fn table_scan_bitmap_next_tuple(
    _scan: TableScanDesc,
    _slot: &mut TupleTableSlot,
    _counts: &mut BitmapPageCounts,
) -> Option<bool> {
    unimplemented!()
}

/// Prepare the next sample block; false if the sample scan is finished.
pub fn table_scan_sample_next_block(_scan: TableScanDesc, _scanstate: &mut SampleScanState) -> bool {
    unimplemented!()
}

/// Next sample tuple into `slot`.
pub fn table_scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: &mut SampleScanState,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions to make modifications a bit simpler.
// ----------------------------------------------------------------------------

/// Simple wrapper around table_tuple_insert (no concurrent-update handling).
pub fn simple_table_tuple_insert(_rel: &RelationData, _slot: &mut TupleTableSlot) {
    unimplemented!()
}

/// Simple wrapper around table_tuple_delete (panics on concurrent update).
pub fn simple_table_tuple_delete(_rel: &RelationData, _tid: &ItemPointerData, _snapshot: Snapshot) {
    unimplemented!()
}

/// Simple wrapper around table_tuple_update; reports index-update needs.
pub fn simple_table_tuple_update(
    _rel: &RelationData,
    _otid: &ItemPointerData,
    _slot: &mut TupleTableSlot,
    _snapshot: Snapshot,
    _update_indexes: &mut TU_UpdateIndexes,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Helper functions to implement parallel scans for block-oriented AMs.
// ----------------------------------------------------------------------------

pub fn table_block_parallelscan_estimate(_rel: &RelationData) -> usize {
    unimplemented!()
}

pub fn table_block_parallelscan_initialize(_rel: &RelationData, _pscan: ParallelTableScanDesc) -> usize {
    unimplemented!()
}

pub fn table_block_parallelscan_reinitialize(_rel: &RelationData, _pscan: ParallelTableScanDesc) {
    unimplemented!()
}

pub fn table_block_parallelscan_nextpage(
    _rel: &RelationData,
    _pbscanwork: *mut ParallelBlockTableScanWorkerData,
    _pbscan: *mut ParallelBlockTableScanDescData,
) -> BlockNumber {
    unimplemented!()
}

pub fn table_block_parallelscan_startblock_init(
    _rel: &RelationData,
    _pbscanwork: *mut ParallelBlockTableScanWorkerData,
    _pbscan: *mut ParallelBlockTableScanDescData,
) {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Helper functions to implement relation sizing for block-oriented AMs.
// ----------------------------------------------------------------------------

pub fn table_block_relation_size(_rel: &RelationData, _fork_number: ForkNumber) -> u64 {
    unimplemented!()
}

pub fn table_block_relation_estimate_size(
    _rel: &RelationData,
    _attr_widths: &mut [i32],
    _overhead_bytes_per_tuple: usize,
    _usable_bytes_per_page: usize,
) -> RelationSizeEstimate {
    unimplemented!()
}

// ----------------------------------------------------------------------------
// Functions in tableamapi.c / heapam_handler.c.
// ----------------------------------------------------------------------------

/// The built-in table AM kinds. PG resolves a `TableAmRoutine` from a handler
/// OID at runtime; the closed enum gives static dispatch (heap is the only
/// in-tree AM). Extension AMs are the open fn-pointer case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableAmKind {
    Heap,
}

/// Resolve a table AM from its handler function OID.
pub fn GetTableAmRoutine(_amhandler: Oid) -> TableAmKind {
    unimplemented!()
}

/// The built-in heap table AM.
pub fn GetHeapamTableAmRoutine() -> TableAmKind {
    TableAmKind::Heap
}

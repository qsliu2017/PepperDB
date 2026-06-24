//! Translated from PostgreSQL src/include/access/genam.h
//! POSTGRES generalized index access method definitions.

use crate::access::htup::HeapTuple;
use crate::access::relscan::{
    IndexScanDescData, ParallelIndexScanDescData, SysScanDescData,
};
use crate::access::sdir::ScanDirection;
use crate::access::skey::{ScanKey, ScanKeyData};
use crate::c::{RegProcedure, TransactionId};
use crate::access::attnum::AttrNumber;
use crate::fmgr::FmgrInfo;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, BufferAccessStrategy};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::Snapshot;

// TupleTableSlot is forward-declared in the C header; reference the real type.
use crate::executor::tuptable::TupleTableSlot;

// TODO(struct-forward): genam.h forward-declares `struct IndexInfo` (it avoids
// depending on execnodes.h). The real definition lives in execnodes.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes::IndexInfo in Phase 2")]
pub struct IndexInfo;

/// Statistics maintained by amgettuple and amgetbitmap.
pub struct IndexScanInstrumentation {
    /// Index search count (incremented with pgstat_count_index_scan call).
    pub nsearches: u64,
}

/// Every worker's IndexScanInstrumentation. C stored this in shared memory with a
/// flexible array; single-process model uses an owned Vec.
pub struct SharedIndexScanInstrumentation {
    pub winstrument: Vec<IndexScanInstrumentation>,
}

/// Statistics returned by ambuild.
pub struct IndexBuildResult {
    /// # of tuples seen in parent table.
    pub heap_tuples: f64,
    /// # of tuples inserted into index.
    pub index_tuples: f64,
}

/// Input arguments passed to ambulkdelete and amvacuumcleanup.
pub struct IndexVacuumInfo {
    /// The index being vacuumed.
    pub index: Relation,
    /// The heap relation the index belongs to.
    pub heaprel: Relation,
    /// ANALYZE (without any actual vacuum).
    pub analyze_only: bool,
    /// Emit progress.h status reports.
    pub report_progress: bool,
    /// num_heap_tuples is an estimate.
    pub estimated_count: bool,
    /// ereport level for progress messages.
    pub message_level: i32,
    /// Tuples remaining in heap.
    pub num_heap_tuples: f64,
    /// Access strategy for reads.
    pub strategy: BufferAccessStrategy,
}

/// Statistics returned by ambulkdelete and amvacuumcleanup.
pub struct IndexBulkDeleteResult {
    /// Pages remaining in index.
    pub num_pages: BlockNumber,
    /// num_index_tuples is an estimate.
    pub estimated_count: bool,
    /// Tuples remaining.
    pub num_index_tuples: f64,
    /// # removed during vacuum operation.
    pub tuples_removed: f64,
    /// # pages marked deleted by us.
    pub pages_newly_deleted: BlockNumber,
    /// # pages marked deleted (could be by us).
    pub pages_deleted: BlockNumber,
    /// # pages available for reuse.
    pub pages_free: BlockNumber,
}

/// Callback to determine if a tuple is bulk-deletable. The C `void *state` opaque
/// context is captured by the closure.
pub type IndexBulkDeleteCallback<'a> = dyn FnMut(&ItemPointerData) -> bool + 'a;

// Struct definitions appear in relscan.rs; re-export the handle typedefs.
pub type IndexScanDesc = *mut IndexScanDescData; // TODO(ptr)
pub type SysScanDesc = *mut SysScanDescData; // TODO(ptr)
pub type ParallelIndexScanDesc = *mut ParallelIndexScanDescData; // TODO(ptr)

/// Type of uniqueness check to perform in index_insert().
pub enum IndexUniqueCheck {
    /// Don't do any uniqueness checking.
    No,
    /// Enforce uniqueness at insertion time.
    Yes,
    /// Test uniqueness, but no error.
    Partial,
    /// Check if existing tuple is unique.
    Existing,
}

/// Nullable "ORDER BY col op const" distance.
pub struct IndexOrderByDistance {
    pub value: f64,
    pub isnull: bool,
}

/// IndexScanIsValid: true iff the index scan is valid (i.e. handle is present).
pub fn IndexScanIsValid(scan: Option<&IndexScanDescData>) -> bool {
    scan.is_some()
}

// generalized index_ interface routines (in indexam.c)

pub fn index_open(_relationId: Oid, _lockmode: i32) -> Relation {
    unimplemented!()
}

pub fn try_index_open(_relationId: Oid, _lockmode: i32) -> Option<Relation> {
    unimplemented!()
}

pub fn index_close(_relation: Relation, _lockmode: i32) {
    unimplemented!()
}

pub fn index_insert(
    _indexRelation: Relation,
    _values: &[Datum],
    _isnull: &[bool],
    _heap_t_ctid: &mut ItemPointerData,
    _heapRelation: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: &mut IndexInfo,
) -> bool {
    unimplemented!()
}

pub fn index_insert_cleanup(_indexRelation: Relation, _indexInfo: &mut IndexInfo) {
    unimplemented!()
}

pub fn index_beginscan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _instrument: &mut IndexScanInstrumentation,
    _nkeys: i32,
    _norderbys: i32,
) -> IndexScanDesc {
    unimplemented!()
}

pub fn index_beginscan_bitmap(
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _instrument: &mut IndexScanInstrumentation,
    _nkeys: i32,
) -> IndexScanDesc {
    unimplemented!()
}

pub fn index_rescan(
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}

pub fn index_endscan(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn index_markpos(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn index_restrpos(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn index_parallelscan_estimate(
    _indexRelation: Relation,
    _nkeys: i32,
    _norderbys: i32,
    _snapshot: Snapshot,
    _instrument: bool,
    _parallel_aware: bool,
    _nworkers: i32,
) -> usize {
    unimplemented!()
}

pub fn index_parallelscan_initialize(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _instrument: bool,
    _parallel_aware: bool,
    _nworkers: i32,
    _sharedinfo: &mut Option<Box<SharedIndexScanInstrumentation>>,
    _target: ParallelIndexScanDesc,
) {
    unimplemented!()
}

pub fn index_parallelrescan(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn index_beginscan_parallel(
    _heaprel: Relation,
    _indexrel: Relation,
    _instrument: &mut IndexScanInstrumentation,
    _nkeys: i32,
    _norderbys: i32,
    _pscan: ParallelIndexScanDesc,
) -> IndexScanDesc {
    unimplemented!()
}

/// Returns the next TID, or None when the scan is exhausted.
pub fn index_getnext_tid(
    _scan: IndexScanDesc,
    _direction: ScanDirection,
) -> Option<ItemPointerData> {
    unimplemented!()
}

pub fn index_fetch_heap(_scan: IndexScanDesc, _slot: &mut TupleTableSlot) -> bool {
    unimplemented!()
}

pub fn index_getnext_slot(
    _scan: IndexScanDesc,
    _direction: ScanDirection,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

pub fn index_getbitmap(_scan: IndexScanDesc, _bitmap: &mut TIDBitmap) -> i64 {
    unimplemented!()
}

pub fn index_bulk_delete(
    _info: &IndexVacuumInfo,
    _istat: Option<Box<IndexBulkDeleteResult>>,
    _callback: &mut IndexBulkDeleteCallback,
) -> Box<IndexBulkDeleteResult> {
    unimplemented!()
}

pub fn index_vacuum_cleanup(
    _info: &IndexVacuumInfo,
    _istat: Option<Box<IndexBulkDeleteResult>>,
) -> Option<Box<IndexBulkDeleteResult>> {
    unimplemented!()
}

pub fn index_can_return(_indexRelation: Relation, _attno: i32) -> bool {
    unimplemented!()
}

pub fn index_getprocid(
    _irel: Relation,
    _attnum: AttrNumber,
    _procnum: u16,
) -> RegProcedure {
    unimplemented!()
}

pub fn index_getprocinfo(
    _irel: Relation,
    _attnum: AttrNumber,
    _procnum: u16,
) -> *mut FmgrInfo {
    unimplemented!()
}

pub fn index_store_float8_orderby_distances(
    _scan: IndexScanDesc,
    _orderByTypes: &[Oid],
    _distances: &[IndexOrderByDistance],
    _recheckOrderBy: bool,
) {
    unimplemented!()
}

pub fn index_opclass_options(
    _indrel: Relation,
    _attnum: AttrNumber,
    _attoptions: Datum,
    _validate: bool,
) -> *mut crate::c::bytea {
    unimplemented!()
}

// index access method support routines (in genam.c)

pub fn RelationGetIndexScan(
    _indexRelation: Relation,
    _nkeys: i32,
    _norderbys: i32,
) -> IndexScanDesc {
    unimplemented!()
}

pub fn IndexScanEnd(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn BuildIndexValueDescription(
    _indexRelation: Relation,
    _values: &[Datum],
    _isnull: &[bool],
) -> Option<String> {
    unimplemented!()
}

pub fn index_compute_xid_horizon_for_tuples(
    _irel: Relation,
    _hrel: Relation,
    _ibuf: Buffer,
    _itemnos: &[OffsetNumber],
    _nitems: i32,
) -> TransactionId {
    unimplemented!()
}

// heap-or-index access to system catalogs (in genam.c)

pub fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

/// Returns the next tuple, or None at end of scan.
pub fn systable_getnext(_sysscan: SysScanDesc) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn systable_recheck_tuple(_sysscan: SysScanDesc, _tup: HeapTuple) -> bool {
    unimplemented!()
}

pub fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!()
}

pub fn systable_beginscan_ordered(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

/// Returns the next tuple, or None at end of scan.
pub fn systable_getnext_ordered(
    _sysscan: SysScanDesc,
    _direction: ScanDirection,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn systable_endscan_ordered(_sysscan: SysScanDesc) {
    unimplemented!()
}

/// Returns (oldtupcopy, opaque state) for the in-place update. The C `void **state`
/// opaque handle is returned rather than filled via out-param.
pub fn systable_inplace_update_begin(
    _relation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: Snapshot,
    _nkeys: i32,
    _key: &ScanKeyData,
) -> (Option<HeapTuple>, Box<dyn core::any::Any>) {
    unimplemented!()
}

pub fn systable_inplace_update_finish(_state: Box<dyn core::any::Any>, _tuple: HeapTuple) {
    unimplemented!()
}

pub fn systable_inplace_update_cancel(_state: Box<dyn core::any::Any>) {
    unimplemented!()
}

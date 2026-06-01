//! heapam_handler.rs
//!   heap table access method code
//!
//! Translated 1:1 from postgres/src/backend/access/heap/heapam_handler.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/heap/heapam_handler.c
//!
//! NOTES
//!	  This files wires up the lower level heapam.c et al routines with the
//!	  tableam abstraction.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_parens)]

use crate::prelude::*;

use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::int32;
use crate::c::int64;
use crate::c::uint32;
use crate::c::uint64;
use crate::c::CommandId;
use crate::c::MultiXactId;
use crate::c::Size;
use crate::c::TransactionId;
use crate::c::varlena;

// postgres_ext.h
use crate::postgres_ext::Oid;
use crate::postgres::Datum;

// pg_config.h
use crate::pg_config::BLCKSZ;

// access/htup.h & access/htup_details.h
use crate::access::htup_details::HeapTuple;
use crate::access::htup_details::HeapTupleData;
use crate::access::htup_details::HeapTupleHeader;
use crate::access::htup_details::HeapTupleHeaderData;
use crate::access::htup_details::*;

// access/transam.h
use crate::access::transam::InvalidTransactionId;
use crate::access::transam::TransactionIdEquals;
use crate::access::transam::TransactionIdIsValid;

// storage/block.h
use crate::storage::block::BlockNumber;
use crate::storage::block::InvalidBlockNumber;
use crate::storage::block::BlockNumberIsValid;

// storage/off.h
use crate::storage::off::OffsetNumber;
use crate::storage::off::FirstOffsetNumber;
use crate::storage::off::InvalidOffsetNumber;
use crate::storage::off::OffsetNumberIsValid;
use crate::storage::off::OffsetNumberNext;

// storage/itemid.h
use crate::storage::itemid::ItemId;
use crate::storage::itemid::ItemIdData;
use crate::storage::itemid::ItemIdGetLength;
use crate::storage::itemid::ItemIdIsDead;
use crate::storage::itemid::ItemIdIsNormal;

// storage/itemptr.h
use crate::storage::itemptr::ItemPointer;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::itemptr::ItemPointerCompare;
use crate::storage::itemptr::ItemPointerCopy;
use crate::storage::itemptr::ItemPointerEquals;
use crate::storage::itemptr::ItemPointerGetBlockNumber;
use crate::storage::itemptr::ItemPointerGetOffsetNumber;
use crate::storage::itemptr::ItemPointerIsValid;
use crate::storage::itemptr::ItemPointerSet;
use crate::storage::itemptr::ItemPointerSetOffsetNumber;

// storage/bufpage.h
use crate::storage::bufpage::Page;
use crate::storage::bufpage::SizeOfPageHeaderData;
use crate::storage::bufpage::PageGetItem;
use crate::storage::bufpage::PageGetItemId;
use crate::storage::bufpage::PageGetMaxOffsetNumber;
use crate::storage::bufpage::PageIsAllVisible;

// storage/buffer/bufmgr.h
use crate::storage::buffer::bufmgr::Buffer;
use crate::storage::buffer::bufmgr::BUFFER_LOCK_SHARE;
use crate::storage::buffer::bufmgr::BUFFER_LOCK_UNLOCK;
use crate::storage::buffer::bufmgr::BufferGetPage;
use crate::storage::buffer::bufmgr::BufferGetBlockNumber;
use crate::storage::buffer::bufmgr::FlushRelationBuffers;
use crate::storage::buffer::bufmgr::LockBuffer;
use crate::storage::buffer::bufmgr::RBM_NORMAL;
use crate::storage::buffer::bufmgr::ReadBufferExtended;
use crate::storage::buffer::bufmgr::ReleaseAndReadBuffer;
use crate::storage::buffer::bufmgr::ReleaseBuffer;
use crate::storage::buffer::bufmgr::UnlockReleaseBuffer;

// common/relpath.h
use crate::common::relpath::ForkNumber;
use crate::common::relpath::MAIN_FORKNUM;
use crate::common::relpath::INIT_FORKNUM;
use crate::common::relpath::MAX_FORKNUM;

// storage/relfilelocator.h
use crate::storage::relfilelocator::RelFileLocator;

// utils/rel.h
use crate::utils::rel::Relation;
use crate::utils::rel::RelationGetDescr;
use crate::utils::rel::RelationGetRelationName;
use crate::utils::rel::RelationGetRelid;

// access/relscan.h
use crate::access::relscan::IndexFetchTableData;
use crate::access::relscan::IndexScanDesc;
use crate::access::relscan::TableScanDesc;
use crate::access::relscan::ParallelBlockTableScanDesc;

// utils/snapshot.h
use crate::utils::snapshot::Snapshot;
use crate::utils::snapshot::SnapshotData;

// executor/tuptable.h
use crate::executor::tuptable::TupleTableSlot;
use crate::executor::tuptable::TupleTableSlotOps;
use crate::executor::tuptable::BufferHeapTupleTableSlot;
use crate::executor::tuptable::TTS_IS_BUFFERTUPLE;

// executor/execTuples (slot ops + store/fetch helpers)
use crate::executor::execTuples::TTSOpsBufferHeapTuple;
use crate::executor::execTuples::TTSOpsHeapTuple;
use crate::executor::execTuples::ExecStoreBufferHeapTuple;
use crate::executor::execTuples::ExecStorePinnedBufferHeapTuple;
use crate::executor::execTuples::ExecStoreHeapTuple;
use crate::executor::execTuples::ExecClearTuple;
use crate::executor::execTuples::MakeSingleTupleTableSlot;
use crate::executor::execTuples::ExecDropSingleTupleTableSlot;

// access/table/tableam.h
use crate::access::table::tableam::TableAmRoutine;
use crate::access::table::tableam::TM_Result;
use crate::access::table::tableam::TM_Result::*;
use crate::access::table::tableam::TM_FailureData;
use crate::access::table::tableam::TM_IndexDeleteOp;
use crate::access::table::tableam::TU_UpdateIndexes;
use crate::access::table::tableam::TU_UpdateIndexes::*;
use crate::access::table::tableam::BulkInsertStateData;
use crate::access::table::tableam::ReadStream;
use crate::access::table::tableam::IndexBuildCallback;
use crate::access::table::tableam::TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
use crate::access::table::tableam::TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
use crate::access::table::tableam::SO_ALLOW_PAGEMODE;
use crate::access::table::tableam::SO_ALLOW_SYNC;
use crate::access::table::tableam::SO_TYPE_BITMAPSCAN;
use crate::access::table::tableam::SnapshotAny;
use crate::access::table::tableam::table_block_relation_estimate_size;
use crate::access::table::tableam::table_block_relation_size;
use crate::access::table::tableam::table_block_parallelscan_estimate;
use crate::access::table::tableam::table_block_parallelscan_initialize;
use crate::access::table::tableam::table_block_parallelscan_reinitialize;
use crate::access::table::tableam::table_slot_create;

// access/sdir.h
use crate::access::sdir::ScanDirection;
use crate::access::sdir::ScanDirection::ForwardScanDirection;

// nodes/lockoptions.h
use crate::nodes::lockoptions::LockTupleMode;
use crate::nodes::lockoptions::LockWaitPolicy;
use crate::nodes::lockoptions::LockWaitPolicy::*;

// nodes/nodes.h (NodeTag)
use crate::nodes::nodes::NodeTag;

// nodes/execnodes.h
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::execnodes::SampleScanState;
use crate::nodes::execnodes::EState;
use crate::nodes::execnodes::ExprState;
use crate::nodes::execnodes::ExprContext;

// access/tupdesc.h
use crate::access::common::tupdesc::TupleDescData;
use crate::access::common::tupdesc::TupleDescAttr;
use crate::access::common::tupdesc::TupleDescCompactAttr;

// catalog/pg_attribute.h
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_attribute::ATTRIBUTE_GENERATED_VIRTUAL;

// catalog/pg_type.h
use crate::catalog::pg_type::TYPSTORAGE_PLAIN;

// catalog/pg_class.h
use crate::catalog::pg_class::RELKIND_RELATION;
use crate::catalog::pg_class::RELKIND_TOASTVALUE;
use crate::catalog::pg_class::RELPERSISTENCE_UNLOGGED;

// access/tupmacs.h
use crate::access::tupmacs::att_align_nominal;

// access/common/heaptuple.h
use crate::access::common::heaptuple::heap_deform_tuple;
use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::common::heaptuple::heap_freetuple;

// access/heap/heapam.h (HeapScanDesc + heap_ routines + scan desc data)
use crate::access::heap::heapam::HeapScanDesc;
use crate::access::heap::heapam::HeapScanDescData;
use crate::access::heap::heapam::heap_beginscan;
use crate::access::heap::heapam::heap_endscan;
use crate::access::heap::heapam::heap_rescan;
use crate::access::heap::heapam::heap_getnextslot;
use crate::access::heap::heapam::heap_getnext;
use crate::access::heap::heapam::heap_set_tidrange;
use crate::access::heap::heapam::heap_getnextslot_tidrange;
use crate::access::heap::heapam::heap_fetch;
use crate::access::heap::heapam::heap_hot_search_buffer;
use crate::access::heap::heapam::heap_get_latest_tid;
use crate::access::heap::heapam::heap_insert;
use crate::access::heap::heapam::heap_multi_insert;
use crate::access::heap::heapam::heap_delete;
use crate::access::heap::heapam::heap_update;
use crate::access::heap::heapam::heap_lock_tuple;
use crate::access::heap::heapam::heap_finish_speculative;
use crate::access::heap::heapam::heap_abort_speculative;
use crate::access::heap::heapam::heap_index_delete_tuples;
use crate::access::heap::heapam::heap_setscanlimits;
use crate::access::heap::heapam::heap_prepare_pagescan;
use crate::access::heap::heapam::heap_page_prune_opt;
use crate::access::heap::heapam::HeapCheckForSerializableConflictOut;
use crate::access::heap::heapam::MaxHeapTuplesPerPage;

// access/heap/heapam_visibility.h
use crate::access::heap::heapam_visibility::HeapTupleSatisfiesVisibility;
use crate::access::heap::heapam_visibility::HeapTupleSatisfiesVacuum;
use crate::access::heap::heapam_visibility::HTSV_Result;
use crate::access::heap::heapam_visibility::HEAPTUPLE_DEAD;
use crate::access::heap::heapam_visibility::HEAPTUPLE_LIVE;
use crate::access::heap::heapam_visibility::HEAPTUPLE_RECENTLY_DEAD;
use crate::access::heap::heapam_visibility::HEAPTUPLE_INSERT_IN_PROGRESS;
use crate::access::heap::heapam_visibility::HEAPTUPLE_DELETE_IN_PROGRESS;

// access/heap/heaptoast.h
use crate::access::heap::heaptoast::heap_fetch_toast_slice;
use crate::access::heap::heaptoast::TOAST_TUPLE_THRESHOLD;

// access/heap/hio.h
use crate::access::heap::hio::BulkInsertState;
use crate::access::heap::hio::HEAP_INSERT_SPECULATIVE;

// access/heap/rewriteheap.h
use crate::access::heap::rewriteheap::RewriteState;
use crate::access::heap::rewriteheap::begin_heap_rewrite;
use crate::access::heap::rewriteheap::end_heap_rewrite;
use crate::access::heap::rewriteheap::rewrite_heap_tuple;
use crate::access::heap::rewriteheap::rewrite_heap_dead_tuple;

// access/index/indexam.h
use crate::access::index::indexam::index_beginscan;
use crate::access::index::indexam::index_rescan;
use crate::access::index::indexam::index_endscan;
use crate::access::index::indexam::index_getnext_slot;
use crate::access::index::indexam::index_insert;

// access/heap/vacuumlazy.h (heap_vacuum_rel)
use crate::access::heap::vacuumlazy::heap_vacuum_rel;

// =====================================================================
// NOTE on stubbing:
//
// heapam_handler.c wires heapam.c into the table AM layer.  Many of the
// table_*/index_* dispatch wrappers, the planner/executor glue, the
// tablesample API, tuplesort, smgr/storage, pgstat progress, multixact,
// snapshot, and predicate-lock helpers are defined in other .c files
// that are not yet ported.  Following sibling convention, every symbol
// with no home yet is provided here as a minimal local stub tagged
// `// TODO(pg-port): real SYM lives in <file>`.  The function bodies are
// faithful 1:1 translations of heapam_handler.c.
// =====================================================================

// access/multixact.h
type MultiXactId_t = MultiXactId;

// access/tableam.h - IndexFetchHeapData (heapam.h structure, defined here)
#[repr(C)]
pub struct IndexFetchHeapData {
    pub xs_base: IndexFetchTableData, /* AM independent part of the descriptor */
    pub xs_cbuf: Buffer,              /* current heap buffer in scan, if any */
    /* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
}

// catalog/index.h - ValidateIndexState (defined here; not yet ported)
#[repr(C)]
struct ValidateIndexState {
    tuplesort: *mut Tuplesortstate,
    htups: f64,
    itups: f64,
    tups_inserted: f64,
} // TODO(pg-port): real ValidateIndexState lives in catalog/index.rs

// storage/itemptr.h - ItemPointerIndicatesMovedPartitions
#[inline]
unsafe fn ItemPointerIndicatesMovedPartitions(pointer: ItemPointer) -> bool {
    ItemPointerGetOffsetNumber(pointer) == crate::storage::itemptr::MovedPartitionsOffsetNumber
        && ItemPointerGetBlockNumberNoCheck(pointer) == crate::storage::itemptr::MovedPartitionsBlockNumber
}
use crate::storage::itemptr::ItemPointerGetBlockNumberNoCheck;

// access/htup_details.h - BITMAPLEN
use crate::access::htup_details::BITMAPLEN;
use crate::access::htup_details::SizeofHeapTupleHeader;

// storage/buf.h
const InvalidBuffer: Buffer = 0;
#[inline]
unsafe fn BufferIsValid(bufnum: Buffer) -> bool {
    bufnum != InvalidBuffer
}
#[inline]
unsafe fn BufferIsInvalid(bufnum: Buffer) -> bool {
    bufnum == InvalidBuffer
}

// access/relscan.h - ParallelBlockTableScanDescData (for phs_* fields)
type ParallelBlockTableScanDescData = crate::access::relscan::ParallelBlockTableScanDescData; // TODO(pg-port): real ParallelBlockTableScanDescData lives in access/relscan.rs

// commands/progress.h
const PROGRESS_CLUSTER_PHASE: c_int = 1; // TODO(pg-port): real value lives in commands/progress.rs
const PROGRESS_CLUSTER_INDEX_RELID: c_int = 2; // TODO(pg-port): real value lives in commands/progress.rs
const PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED: c_int = 3; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN: c_int = 4; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_TOTAL_HEAP_BLKS: c_int = 5; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_HEAP_BLKS_SCANNED: c_int = 6; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP: int64 = 2; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP: int64 = 3; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_PHASE_SORT_TUPLES: int64 = 4; // TODO(pg-port): commands/progress.rs
const PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP: int64 = 5; // TODO(pg-port): commands/progress.rs
const PROGRESS_SCAN_BLOCKS_TOTAL: c_int = 15; // TODO(pg-port): commands/progress.rs
const PROGRESS_SCAN_BLOCKS_DONE: c_int = 16; // TODO(pg-port): commands/progress.rs

// access/genam.h - UNIQUE_CHECK_*
const UNIQUE_CHECK_NO: c_int = 0; // TODO(pg-port): real IndexUniqueCheck lives in access/genam.rs
const UNIQUE_CHECK_YES: c_int = 1; // TODO(pg-port): real IndexUniqueCheck lives in access/genam.rs

// access/tsmapi.h - TsmRoutine (sampling method)
#[repr(C)]
struct TsmRoutine {
    NextSampleBlock: Option<unsafe fn(node: *mut SampleScanState, nblocks: BlockNumber) -> BlockNumber>,
    NextSampleTuple: Option<
        unsafe fn(node: *mut SampleScanState, blockno: BlockNumber, maxoffset: OffsetNumber) -> OffsetNumber,
    >,
} // TODO(pg-port): real TsmRoutine lives in access/tsmapi.rs

// storage/tidbitmap.h - TBM iterate result + extraction
const TBM_MAX_TUPLES_PER_PAGE: usize = MaxHeapTuplesPerPage as usize; // TODO(pg-port): real value lives in storage/tidbitmap.rs
#[repr(C)]
struct TBMIterateResult {
    blockno: BlockNumber,
    lossy: bool,
    recheck: bool,
} // TODO(pg-port): real TBMIterateResult lives in storage/tidbitmap.rs

// access/relscan.h - BitmapHeapScanDesc
type BitmapHeapScanDesc = *mut BitmapHeapScanDescData; // TODO(pg-port): real BitmapHeapScanDesc lives in access/relscan.rs
#[repr(C)]
struct BitmapHeapScanDescData {
    rs_base: crate::access::relscan::TableScanDescData,
} // TODO(pg-port): real BitmapHeapScanDescData lives in access/relscan.rs

// utils/tuplesort.h - Tuplesortstate + cluster sort routines
type Tuplesortstate = c_void; // TODO(pg-port): real Tuplesortstate lives in utils/sort/tuplesort.rs
const TUPLESORT_NONE: c_int = 0; // TODO(pg-port): real value lives in utils/sort/tuplesort.rs

unsafe fn tuplesort_begin_cluster(
    _tupDesc: TupleDesc,
    _indexRel: Relation,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _sortopt: c_int,
) -> *mut Tuplesortstate {
    // TODO(pg-port): real tuplesort_begin_cluster lives in utils/sort/tuplesortvariants.rs
    null_mut()
}
unsafe fn tuplesort_putheaptuple(_state: *mut Tuplesortstate, _tup: HeapTuple) {
    // TODO(pg-port): real tuplesort_putheaptuple lives in utils/sort/tuplesortvariants.rs
}
unsafe fn tuplesort_getheaptuple(_state: *mut Tuplesortstate, _forward: bool) -> HeapTuple {
    // TODO(pg-port): real tuplesort_getheaptuple lives in utils/sort/tuplesortvariants.rs
    null_mut()
}
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) {
    // TODO(pg-port): real tuplesort_performsort lives in utils/sort/tuplesort.rs
}
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) {
    // TODO(pg-port): real tuplesort_end lives in utils/sort/tuplesort.rs
}
unsafe fn tuplesort_getdatum(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _val: *mut Datum,
    _isNull: *mut bool,
    _abbrev: *mut Datum,
) -> bool {
    // TODO(pg-port): real tuplesort_getdatum lives in utils/sort/tuplesortvariants.rs
    false
}

type TupleDesc = *mut TupleDescData;

// access/table/tableam.h - inline scan dispatch wrappers (not yet ported)
unsafe fn table_beginscan(
    _rel: Relation,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut c_void,
) -> TableScanDesc {
    // TODO(pg-port): real table_beginscan lives in access/table/tableam.rs
    null_mut()
}
unsafe fn table_beginscan_strat(
    _rel: Relation,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut c_void,
    _allow_strat: bool,
    _allow_sync: bool,
) -> TableScanDesc {
    // TODO(pg-port): real table_beginscan_strat lives in access/table/tableam.rs
    null_mut()
}
unsafe fn table_endscan(_scan: TableScanDesc) {
    // TODO(pg-port): real table_endscan lives in access/table/tableam.rs
}
unsafe fn table_scan_getnextslot(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    // TODO(pg-port): real table_scan_getnextslot lives in access/table/tableam.rs
    false
}

// catalog/catalog.h
unsafe fn IsSystemRelation(_relation: Relation) -> bool {
    // TODO(pg-port): real IsSystemRelation lives in catalog/catalog.rs
    false
}

// miscadmin.h
unsafe fn IsBootstrapProcessingMode() -> bool {
    // TODO(pg-port): real IsBootstrapProcessingMode lives in miscadmin.rs
    false
}
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {
        // TODO(pg-port): real CHECK_FOR_INTERRUPTS lives in miscadmin.rs
    };
}

// catalog/storage.h + catalog/storage_xlog.h + storage/smgr.h
type SMgrRelation = *mut c_void; // TODO(pg-port): real SMgrRelation lives in storage/smgr/smgr.rs
unsafe fn RelationCreateStorage(
    _rlocator: RelFileLocator,
    _relpersistence: c_char,
    _register_delete: bool,
) -> SMgrRelation {
    // TODO(pg-port): real RelationCreateStorage lives in catalog/storage.rs
    null_mut()
}
unsafe fn RelationDropStorage(_rel: Relation) {
    // TODO(pg-port): real RelationDropStorage lives in catalog/storage.rs
}
unsafe fn RelationCopyStorage(
    _src: SMgrRelation,
    _dst: SMgrRelation,
    _forkNum: ForkNumber,
    _relpersistence: c_char,
) {
    // TODO(pg-port): real RelationCopyStorage lives in catalog/storage.rs
}
unsafe fn RelationTruncate(_rel: Relation, _nblocks: BlockNumber) {
    // TODO(pg-port): real RelationTruncate lives in catalog/storage.rs
}
unsafe fn log_smgrcreate(_rlocator: *const RelFileLocator, _forkNum: ForkNumber) {
    // TODO(pg-port): real log_smgrcreate lives in catalog/storage.rs
}
unsafe fn smgrcreate(_reln: SMgrRelation, _forknum: ForkNumber, _isRedo: bool) {
    // TODO(pg-port): real smgrcreate lives in storage/smgr/smgr.rs
}
unsafe fn smgrclose(_reln: SMgrRelation) {
    // TODO(pg-port): real smgrclose lives in storage/smgr/smgr.rs
}
unsafe fn smgrexists(_reln: SMgrRelation, _forknum: ForkNumber) -> bool {
    // TODO(pg-port): real smgrexists lives in storage/smgr/smgr.rs
    false
}
unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    // TODO(pg-port): real RelationGetSmgr lives in utils/rel.rs
    null_mut()
}
unsafe fn RelationGetTargetBlock(_rel: Relation) -> BlockNumber {
    // TODO(pg-port): real RelationGetTargetBlock lives in utils/rel.rs
    InvalidBlockNumber
}
unsafe fn RelationIsPermanent(_rel: Relation) -> bool {
    // TODO(pg-port): real RelationIsPermanent lives in utils/rel.rs
    false
}

// access/multixact.h
unsafe fn GetOldestMultiXactId() -> MultiXactId {
    // TODO(pg-port): real GetOldestMultiXactId lives in access/transam/multixact.rs
    0
}

// storage/procarray.h
unsafe fn GetOldestNonRemovableTransactionId(_rel: Relation) -> TransactionId {
    // TODO(pg-port): real GetOldestNonRemovableTransactionId lives in storage/ipc/procarray.rs
    InvalidTransactionId
}

// utils/snapmgr.h
unsafe fn GetTransactionSnapshot() -> Snapshot {
    // TODO(pg-port): real GetTransactionSnapshot lives in utils/time/snapmgr.rs
    null_mut()
}
unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    // TODO(pg-port): real RegisterSnapshot lives in utils/time/snapmgr.rs
    snapshot
}
unsafe fn UnregisterSnapshot(_snapshot: Snapshot) {
    // TODO(pg-port): real UnregisterSnapshot lives in utils/time/snapmgr.rs
}

// utils/snapshot.h - IsMVCCSnapshot
unsafe fn IsMVCCSnapshot(_snapshot: Snapshot) -> bool {
    // TODO(pg-port): real IsMVCCSnapshot lives in utils/snapshot.rs
    false
}
unsafe fn InitDirtySnapshot(_snapshotdata: &mut SnapshotData) {
    // TODO(pg-port): real InitDirtySnapshot lives in utils/snapshot.rs
}

// access/transam/transam.h + xact.h
static mut RecentXmin: TransactionId = InvalidTransactionId; // TODO(pg-port): real RecentXmin lives in utils/time/snapmgr.rs
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    // TODO(pg-port): real TransactionIdIsCurrentTransactionId lives in access/transam/xact.rs
    false
}
unsafe fn XactLockTableWait(
    _xid: TransactionId,
    _rel: Relation,
    _ctid: ItemPointer,
    _oper: c_int,
) {
    // TODO(pg-port): real XactLockTableWait lives in storage/lmgr/lmgr.rs
}
unsafe fn ConditionalXactLockTableWait(_xid: TransactionId, _logLockFailure: bool) -> bool {
    // TODO(pg-port): real ConditionalXactLockTableWait lives in storage/lmgr/lmgr.rs
    false
}
const XLTW_FetchUpdated: c_int = 0; // TODO(pg-port): real XLTW_Oper lives in storage/lmgr/lmgr.rs
const XLTW_InsertIndexUnique: c_int = 0; // TODO(pg-port): real XLTW_Oper lives in storage/lmgr/lmgr.rs
static mut log_lock_failures: bool = false; // TODO(pg-port): real log_lock_failures lives in storage/lmgr/proc.rs

// storage/predicate.h
unsafe fn PredicateLockTID(
    _relation: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _tuple_xid: TransactionId,
) {
    // TODO(pg-port): real PredicateLockTID lives in storage/lmgr/predicate.rs
}

// pgstat.h - progress + counters
unsafe fn pgstat_progress_update_param(_index: c_int, _val: int64) {
    // TODO(pg-port): real pgstat_progress_update_param lives in utils/activity/backend_progress.rs
}
unsafe fn pgstat_progress_update_multi_param(_nparam: c_int, _index: *const c_int, _val: *const int64) {
    // TODO(pg-port): real pgstat_progress_update_multi_param lives in utils/activity/backend_progress.rs
}
unsafe fn pgstat_count_heap_fetch(_rel: Relation) {
    // TODO(pg-port): real pgstat_count_heap_fetch lives in pgstat.h (utils/activity)
}
unsafe fn pgstat_count_heap_getnext(_rel: Relation) {
    // TODO(pg-port): real pgstat_count_heap_getnext lives in pgstat.h (utils/activity)
}

// access/syncscan.h
unsafe fn ss_report_location(_rel: Relation, _location: BlockNumber) {
    // TODO(pg-port): real ss_report_location lives in access/common/syncscan.rs
}

// storage/tidbitmap.h
unsafe fn tbm_extract_page_tuple(
    _iteritem: *mut TBMIterateResult,
    _offsets: *mut OffsetNumber,
    _max_offsets: usize,
) -> c_int {
    // TODO(pg-port): real tbm_extract_page_tuple lives in nodes/tidbitmap.rs
    0
}

// storage/read_stream.h
unsafe fn read_stream_next_buffer(_stream: *mut ReadStream, _per_buffer_data: *mut *mut c_void) -> Buffer {
    // TODO(pg-port): real read_stream_next_buffer lives in storage/aio/read_stream.rs
    InvalidBuffer
}

// executor/executor.h - executor state + expression eval
unsafe fn CreateExecutorState() -> *mut EState {
    // TODO(pg-port): real CreateExecutorState lives in executor/execUtils.rs
    null_mut()
}
unsafe fn FreeExecutorState(_estate: *mut EState) {
    // TODO(pg-port): real FreeExecutorState lives in executor/execUtils.rs
}
unsafe fn GetPerTupleExprContext(_estate: *mut EState) -> *mut ExprContext {
    // TODO(pg-port): real GetPerTupleExprContext lives in executor/executor.rs
    null_mut()
}
unsafe fn ExecPrepareQual(_qual: *mut crate::nodes::pg_list::List, _estate: *mut EState) -> *mut ExprState {
    // TODO(pg-port): real ExecPrepareQual lives in executor/execExpr.rs
    null_mut()
}
unsafe fn ExecQual(_state: *mut ExprState, _econtext: *mut ExprContext) -> bool {
    // TODO(pg-port): real ExecQual lives in executor/executor.rs
    false
}
unsafe fn ExecFetchSlotHeapTuple(
    _slot: *mut TupleTableSlot,
    _materialize: bool,
    _shouldFree: *mut bool,
) -> HeapTuple {
    // TODO(pg-port): real ExecFetchSlotHeapTuple lives in executor/execTuples.rs
    null_mut()
}

// catalog/index.h - FormIndexDatum
unsafe fn FormIndexDatum(
    _indexInfo: *mut IndexInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    // TODO(pg-port): real FormIndexDatum lives in catalog/index.rs
}
const INDEX_MAX_KEYS: usize = 32; // TODO(pg-port): real INDEX_MAX_KEYS lives in pg_config_manual.rs

// access/htup_details.h / access/heapam.h helpers
unsafe fn heap_get_root_tuples(_page: Page, _root_offsets: *mut OffsetNumber) {
    // TODO(pg-port): real heap_get_root_tuples lives in access/heap/pruneheap.rs
}

// utils/builtins.h - type_maximum_size
unsafe fn type_maximum_size(_type_oid: Oid, _typemod: int32) -> int32 {
    // TODO(pg-port): real type_maximum_size lives in utils/adt/format_type.rs
    -1
}

// pg_list.h - itemptr decode helper (utils/sort/tuplesortvariants.c uses this)
unsafe fn itemptr_decode(itemptr: *mut ItemPointerData, encoded: int64) {
    // TODO(pg-port): real itemptr_decode lives in utils/sort/tuplesortvariants.rs
    let block: BlockNumber = (encoded >> 16) as BlockNumber;
    let offset: OffsetNumber = (encoded & 0xffff) as OffsetNumber;
    ItemPointerSet(itemptr, block, offset);
}

// maintenance_work_mem / type maximum size constants
static mut maintenance_work_mem: c_int = 65536; // TODO(pg-port): real maintenance_work_mem lives in utils/misc/guc_tables.rs

// catalog macros
unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != crate::postgres_ext::InvalidOid
}

/* ------------------------------------------------------------------------
 * Slot related callbacks for heap AM
 * ------------------------------------------------------------------------
 */

unsafe fn heapam_slot_callbacks(relation: Relation) -> *const TupleTableSlotOps {
    &TTSOpsBufferHeapTuple
}


/* ------------------------------------------------------------------------
 * Index Scan Callbacks for heap AM
 * ------------------------------------------------------------------------
 */

unsafe fn heapam_index_fetch_begin(rel: Relation) -> *mut IndexFetchTableData {
    let hscan: *mut IndexFetchHeapData =
        palloc0(core::mem::size_of::<IndexFetchHeapData>()) as *mut IndexFetchHeapData;

    (*hscan).xs_base.rel = rel;
    (*hscan).xs_cbuf = InvalidBuffer;

    &mut (*hscan).xs_base
}

unsafe fn heapam_index_fetch_reset(scan: *mut IndexFetchTableData) {
    let hscan: *mut IndexFetchHeapData = scan as *mut IndexFetchHeapData;

    if BufferIsValid((*hscan).xs_cbuf) {
        ReleaseBuffer((*hscan).xs_cbuf);
        (*hscan).xs_cbuf = InvalidBuffer;
    }
}

unsafe fn heapam_index_fetch_end(scan: *mut IndexFetchTableData) {
    let hscan: *mut IndexFetchHeapData = scan as *mut IndexFetchHeapData;

    heapam_index_fetch_reset(scan);

    pfree(hscan as *mut c_void);
}

unsafe fn heapam_index_fetch_tuple(
    scan: *mut IndexFetchTableData,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    call_again: *mut bool,
    all_dead: *mut bool,
) -> bool {
    let hscan: *mut IndexFetchHeapData = scan as *mut IndexFetchHeapData;
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let got_heap_tuple: bool;

    Assert!(TTS_IS_BUFFERTUPLE(slot));

    /* We can skip the buffer-switching logic if we're in mid-HOT chain. */
    if !*call_again {
        /* Switch to correct buffer if we don't have it already */
        let prev_buf: Buffer = (*hscan).xs_cbuf;

        (*hscan).xs_cbuf = ReleaseAndReadBuffer(
            (*hscan).xs_cbuf,
            (*hscan).xs_base.rel,
            ItemPointerGetBlockNumber(tid),
        );

        /*
         * Prune page, but only if we weren't already on this page
         */
        if prev_buf != (*hscan).xs_cbuf {
            heap_page_prune_opt((*hscan).xs_base.rel, (*hscan).xs_cbuf);
        }
    }

    /* Obtain share-lock on the buffer so we can examine visibility */
    LockBuffer((*hscan).xs_cbuf, BUFFER_LOCK_SHARE);
    got_heap_tuple = heap_hot_search_buffer(
        tid,
        (*hscan).xs_base.rel,
        (*hscan).xs_cbuf,
        snapshot,
        &mut (*bslot).base.tupdata,
        all_dead,
        !*call_again,
    );
    (*bslot).base.tupdata.t_self = *tid;
    LockBuffer((*hscan).xs_cbuf, BUFFER_LOCK_UNLOCK);

    if got_heap_tuple {
        /*
         * Only in a non-MVCC snapshot can more than one member of the HOT
         * chain be visible.
         */
        *call_again = !IsMVCCSnapshot(snapshot);

        (*slot).tts_tableOid = RelationGetRelid((*scan).rel);
        ExecStoreBufferHeapTuple(&mut (*bslot).base.tupdata, slot, (*hscan).xs_cbuf);
    } else {
        /* We've reached the end of the HOT chain. */
        *call_again = false;
    }

    got_heap_tuple
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for heap AM
 * ------------------------------------------------------------------------
 */

unsafe fn heapam_fetch_row_version(
    relation: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
) -> bool {
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let mut buffer: Buffer = InvalidBuffer;

    Assert!(TTS_IS_BUFFERTUPLE(slot));

    (*bslot).base.tupdata.t_self = *tid;
    if heap_fetch(relation, snapshot, &mut (*bslot).base.tupdata, &mut buffer, false) {
        /* store in slot, transferring existing pin */
        ExecStorePinnedBufferHeapTuple(&mut (*bslot).base.tupdata, slot, buffer);
        (*slot).tts_tableOid = RelationGetRelid(relation);

        return true;
    }

    false
}

unsafe fn heapam_tuple_tid_valid(scan: TableScanDesc, tid: ItemPointer) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;

    ItemPointerIsValid(tid) && ItemPointerGetBlockNumber(tid) < (*hscan).rs_nblocks
}

unsafe fn heapam_tuple_satisfies_snapshot(
    rel: Relation,
    slot: *mut TupleTableSlot,
    snapshot: Snapshot,
) -> bool {
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let res: bool;

    Assert!(TTS_IS_BUFFERTUPLE(slot));
    Assert!(BufferIsValid((*bslot).buffer));

    /*
     * We need buffer pin and lock to call HeapTupleSatisfiesVisibility.
     * Caller should be holding pin, but not lock.
     */
    LockBuffer((*bslot).buffer, BUFFER_LOCK_SHARE);
    res = HeapTupleSatisfiesVisibility((*bslot).base.tuple, snapshot, (*bslot).buffer);
    LockBuffer((*bslot).buffer, BUFFER_LOCK_UNLOCK);

    res
}


/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for heap AM.
 * ----------------------------------------------------------------------------
 */

unsafe fn heapam_tuple_insert(
    relation: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    options: c_int,
    bistate: BulkInsertState,
) {
    let mut shouldFree: bool = true;
    let tuple: HeapTuple = ExecFetchSlotHeapTuple(slot, true, &mut shouldFree);

    /* Update the tuple with table oid */
    (*slot).tts_tableOid = RelationGetRelid(relation);
    (*tuple).t_tableOid = (*slot).tts_tableOid;

    /* Perform the insertion, and copy the resulting ItemPointer */
    heap_insert(relation, tuple, cid, options, bistate);
    ItemPointerCopy(&(*tuple).t_self, &mut (*slot).tts_tid);

    if shouldFree {
        pfree(tuple as *mut c_void);
    }
}

unsafe fn heapam_tuple_insert_speculative(
    relation: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    mut options: c_int,
    bistate: BulkInsertState,
    specToken: uint32,
) {
    let mut shouldFree: bool = true;
    let tuple: HeapTuple = ExecFetchSlotHeapTuple(slot, true, &mut shouldFree);

    /* Update the tuple with table oid */
    (*slot).tts_tableOid = RelationGetRelid(relation);
    (*tuple).t_tableOid = (*slot).tts_tableOid;

    HeapTupleHeaderSetSpeculativeToken((*tuple).t_data, specToken as BlockNumber);
    options |= HEAP_INSERT_SPECULATIVE;

    /* Perform the insertion, and copy the resulting ItemPointer */
    heap_insert(relation, tuple, cid, options, bistate);
    ItemPointerCopy(&(*tuple).t_self, &mut (*slot).tts_tid);

    if shouldFree {
        pfree(tuple as *mut c_void);
    }
}

unsafe fn heapam_tuple_complete_speculative(
    relation: Relation,
    slot: *mut TupleTableSlot,
    specToken: uint32,
    succeeded: bool,
) {
    let mut shouldFree: bool = true;
    let tuple: HeapTuple = ExecFetchSlotHeapTuple(slot, true, &mut shouldFree);

    /* adjust the tuple's state accordingly */
    if succeeded {
        heap_finish_speculative(relation, &mut (*slot).tts_tid);
    } else {
        heap_abort_speculative(relation, &mut (*slot).tts_tid);
    }

    if shouldFree {
        pfree(tuple as *mut c_void);
    }
}

unsafe fn heapam_tuple_delete(
    relation: Relation,
    tid: ItemPointer,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    changingPart: bool,
) -> TM_Result {
    /*
     * Currently Deleting of index tuples are handled at vacuum, in case if
     * the storage itself is cleaning the dead tuples by itself, it is the
     * time to call the index tuple deletion also.
     */
    heap_delete(relation, tid, cid, crosscheck, wait, tmfd, changingPart)
}


unsafe fn heapam_tuple_update(
    relation: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lockmode: *mut LockTupleMode,
    update_indexes: *mut TU_UpdateIndexes,
) -> TM_Result {
    let mut shouldFree: bool = true;
    let tuple: HeapTuple = ExecFetchSlotHeapTuple(slot, true, &mut shouldFree);
    let result: TM_Result;

    /* Update the tuple with table oid */
    (*slot).tts_tableOid = RelationGetRelid(relation);
    (*tuple).t_tableOid = (*slot).tts_tableOid;

    result = heap_update(
        relation,
        otid,
        tuple,
        cid,
        crosscheck,
        wait,
        tmfd,
        lockmode,
        update_indexes,
    );
    ItemPointerCopy(&(*tuple).t_self, &mut (*slot).tts_tid);

    /*
     * Decide whether new index entries are needed for the tuple
     *
     * Note: heap_update returns the tid (location) of the new tuple in the
     * t_self field.
     *
     * If the update is not HOT, we must update all indexes. If the update is
     * HOT, it could be that we updated summarized columns, so we either
     * update only summarized indexes, or none at all.
     */
    if result != TM_Ok {
        Assert!(*update_indexes == TU_None);
        *update_indexes = TU_None;
    } else if !HeapTupleIsHeapOnly(tuple) {
        Assert!(*update_indexes == TU_All);
    } else {
        Assert!((*update_indexes == TU_Summarizing) || (*update_indexes == TU_None));
    }

    if shouldFree {
        pfree(tuple as *mut c_void);
    }

    result
}

unsafe fn heapam_tuple_lock(
    relation: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    flags: uint8,
    tmfd: *mut TM_FailureData,
) -> TM_Result {
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let mut result: TM_Result;
    let mut buffer: Buffer = InvalidBuffer;
    let tuple: HeapTuple = &mut (*bslot).base.tupdata;
    let follow_updates: bool;

    follow_updates = (flags as c_int & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
    (*tmfd).traversed = false;

    Assert!(TTS_IS_BUFFERTUPLE(slot));

    // C label: tuple_lock_retry. A C `goto tuple_lock_retry` jumps back to the
    // top, so we wrap the whole body in a loop and `continue 'tuple_lock_retry`.
    'tuple_lock_retry: loop {
        (*tuple).t_self = *tid;
        result = heap_lock_tuple(
            relation,
            tuple,
            cid,
            mode,
            wait_policy,
            follow_updates,
            &mut buffer,
            tmfd,
        );

        if result == TM_Updated && (flags as c_int & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 {
            /* Should not encounter speculative tuple on recheck */
            Assert!(!HeapTupleHeaderIsSpeculative((*tuple).t_data));

            ReleaseBuffer(buffer);

            if !ItemPointerEquals(&mut (*tmfd).ctid, &mut (*tuple).t_self) {
                let mut SnapshotDirty: SnapshotData = core::mem::zeroed();
                let mut priorXmax: TransactionId;

                /* it was updated, so look at the updated version */
                *tid = (*tmfd).ctid;
                /* updated row should have xmin matching this xmax */
                priorXmax = (*tmfd).xmax;

                /* signal that a tuple later in the chain is getting locked */
                (*tmfd).traversed = true;

                /*
                 * fetch target tuple
                 *
                 * Loop here to deal with updated or busy tuples
                 */
                InitDirtySnapshot(&mut SnapshotDirty);
                loop {
                    if ItemPointerIndicatesMovedPartitions(tid) {
                        ereport!(
                            ERROR,
                            errmsg!("tuple to be locked was already moved to another partition due to concurrent update")
                        );
                        // C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                    }

                    (*tuple).t_self = *tid;
                    if heap_fetch(relation, &mut SnapshotDirty, tuple, &mut buffer, true) {
                        /*
                         * If xmin isn't what we're expecting, the slot must have
                         * been recycled and reused for an unrelated tuple.  This
                         * implies that the latest version of the row was deleted,
                         * so we need do nothing.  (Should be safe to examine xmin
                         * without getting buffer's content lock.  We assume
                         * reading a TransactionId to be atomic, and Xmin never
                         * changes in an existing tuple, except to invalid or
                         * frozen, and neither of those can match priorXmax.)
                         */
                        if !TransactionIdEquals(
                            HeapTupleHeaderGetXmin((*tuple).t_data),
                            priorXmax,
                        ) {
                            ReleaseBuffer(buffer);
                            return TM_Deleted;
                        }

                        /* otherwise xmin should not be dirty... */
                        if TransactionIdIsValid(SnapshotDirty.xmin) {
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "t_xmin {} is uncommitted in tuple ({},{}) to be updated in table \"{}\"",
                                    SnapshotDirty.xmin,
                                    ItemPointerGetBlockNumber(&mut (*tuple).t_self),
                                    ItemPointerGetOffsetNumber(&mut (*tuple).t_self),
                                    std::ffi::CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy()
                                )
                            );
                            // C also: errcode(ERRCODE_DATA_CORRUPTED); errmsg_internal
                        }

                        /*
                         * If tuple is being updated by other transaction then we
                         * have to wait for its commit/abort, or die trying.
                         */
                        if TransactionIdIsValid(SnapshotDirty.xmax) {
                            ReleaseBuffer(buffer);
                            match wait_policy {
                                LockWaitBlock => {
                                    XactLockTableWait(
                                        SnapshotDirty.xmax,
                                        relation,
                                        &mut (*tuple).t_self,
                                        XLTW_FetchUpdated,
                                    );
                                }
                                LockWaitSkip => {
                                    if !ConditionalXactLockTableWait(SnapshotDirty.xmax, false) {
                                        /* skip instead of waiting */
                                        return TM_WouldBlock;
                                    }
                                }
                                LockWaitError => {
                                    if !ConditionalXactLockTableWait(
                                        SnapshotDirty.xmax,
                                        log_lock_failures,
                                    ) {
                                        ereport!(
                                            ERROR,
                                            errmsg!(
                                                "could not obtain lock on row in relation \"{}\"",
                                                std::ffi::CStr::from_ptr(RelationGetRelationName(relation)).to_string_lossy()
                                            )
                                        );
                                        // C also: errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                                    }
                                }
                            }
                            continue; /* loop back to repeat heap_fetch */
                        }

                        /*
                         * If tuple was inserted by our own transaction, we have
                         * to check cmin against cid: cmin >= current CID means
                         * our command cannot see the tuple, so we should ignore
                         * it. Otherwise heap_lock_tuple() will throw an error,
                         * and so would any later attempt to update or delete the
                         * tuple.  (We need not check cmax because
                         * HeapTupleSatisfiesDirty will consider a tuple deleted
                         * by our transaction dead, regardless of cmax.)  We just
                         * checked that priorXmax == xmin, so we can test that
                         * variable instead of doing HeapTupleHeaderGetXmin again.
                         */
                        if TransactionIdIsCurrentTransactionId(priorXmax)
                            && HeapTupleHeaderGetCmin((*tuple).t_data) >= cid
                        {
                            (*tmfd).xmax = priorXmax;

                            /*
                             * Cmin is the problematic value, so store that. See
                             * above.
                             */
                            (*tmfd).cmax = HeapTupleHeaderGetCmin((*tuple).t_data);
                            ReleaseBuffer(buffer);
                            return TM_SelfModified;
                        }

                        /*
                         * This is a live tuple, so try to lock it again.
                         */
                        ReleaseBuffer(buffer);
                        continue 'tuple_lock_retry;
                    }

                    /*
                     * If the referenced slot was actually empty, the latest
                     * version of the row must have been deleted, so we need do
                     * nothing.
                     */
                    if (*tuple).t_data.is_null() {
                        Assert!(!BufferIsValid(buffer));
                        return TM_Deleted;
                    }

                    /*
                     * As above, if xmin isn't what we're expecting, do nothing.
                     */
                    if !TransactionIdEquals(HeapTupleHeaderGetXmin((*tuple).t_data), priorXmax) {
                        ReleaseBuffer(buffer);
                        return TM_Deleted;
                    }

                    /*
                     * If we get here, the tuple was found but failed
                     * SnapshotDirty. Assuming the xmin is either a committed xact
                     * or our own xact (as it certainly should be if we're trying
                     * to modify the tuple), this must mean that the row was
                     * updated or deleted by either a committed xact or our own
                     * xact.  If it was deleted, we can ignore it; if it was
                     * updated then chain up to the next version and repeat the
                     * whole process.
                     *
                     * As above, it should be safe to examine xmax and t_ctid
                     * without the buffer content lock, because they can't be
                     * changing.  We'd better hold a buffer pin though.
                     */
                    if ItemPointerEquals(&mut (*tuple).t_self, &mut (*(*tuple).t_data).t_ctid) {
                        /* deleted, so forget about it */
                        ReleaseBuffer(buffer);
                        return TM_Deleted;
                    }

                    /* updated, so look at the updated row */
                    *tid = (*(*tuple).t_data).t_ctid;
                    /* updated row should have xmin matching this xmax */
                    priorXmax = HeapTupleHeaderGetUpdateXid((*tuple).t_data);
                    ReleaseBuffer(buffer);
                    /* loop back to fetch next in chain */
                }
            } else {
                /* tuple was deleted, so give up */
                return TM_Deleted;
            }
        }

        (*slot).tts_tableOid = RelationGetRelid(relation);
        (*tuple).t_tableOid = (*slot).tts_tableOid;

        /* store in slot, transferring existing pin */
        ExecStorePinnedBufferHeapTuple(tuple, slot, buffer);

        return result;
    }
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for heap AM.
 * ------------------------------------------------------------------------
 */

unsafe fn heapam_relation_set_new_filelocator(
    rel: Relation,
    newrlocator: *const RelFileLocator,
    persistence: c_char,
    freezeXid: *mut TransactionId,
    minmulti: *mut MultiXactId,
) {
    let srel: SMgrRelation;

    /*
     * Initialize to the minimum XID that could put tuples in the table. We
     * know that no xacts older than RecentXmin are still running, so that
     * will do.
     */
    *freezeXid = RecentXmin;

    /*
     * Similarly, initialize the minimum Multixact to the first value that
     * could possibly be stored in tuples in the table.  Running transactions
     * could reuse values from their local cache, so we are careful to
     * consider all currently running multis.
     *
     * XXX this could be refined further, but is it worth the hassle?
     */
    *minmulti = GetOldestMultiXactId();

    srel = RelationCreateStorage(*newrlocator, persistence, true);

    /*
     * If required, set up an init fork for an unlogged table so that it can
     * be correctly reinitialized on restart.
     */
    if persistence == RELPERSISTENCE_UNLOGGED {
        Assert!(
            (*(*rel).rd_rel).relkind == RELKIND_RELATION
                || (*(*rel).rd_rel).relkind == RELKIND_TOASTVALUE
        );
        smgrcreate(srel, INIT_FORKNUM, false);
        log_smgrcreate(newrlocator, INIT_FORKNUM);
    }

    smgrclose(srel);
}

unsafe fn heapam_relation_nontransactional_truncate(rel: Relation) {
    RelationTruncate(rel, 0);
}

unsafe fn heapam_relation_copy_data(rel: Relation, newrlocator: *const RelFileLocator) {
    let dstrel: SMgrRelation;

    /*
     * Since we copy the file directly without looking at the shared buffers,
     * we'd better first flush out any pages of the source relation that are
     * in shared buffers.  We assume no new changes will be made while we are
     * holding exclusive lock on the rel.
     */
    FlushRelationBuffers(rel);

    /*
     * Create and copy all forks of the relation, and schedule unlinking of
     * old physical files.
     *
     * NOTE: any conflict in relfilenumber value will be caught in
     * RelationCreateStorage().
     */
    dstrel = RelationCreateStorage(*newrlocator, (*(*rel).rd_rel).relpersistence, true);

    /* copy main fork */
    RelationCopyStorage(
        RelationGetSmgr(rel),
        dstrel,
        MAIN_FORKNUM,
        (*(*rel).rd_rel).relpersistence,
    );

    /* copy those extra forks that exist */
    let mut forkNum: ForkNumber = MAIN_FORKNUM + 1;
    while forkNum <= MAX_FORKNUM {
        if smgrexists(RelationGetSmgr(rel), forkNum) {
            smgrcreate(dstrel, forkNum, false);

            /*
             * WAL log creation if the relation is persistent, or this is the
             * init fork of an unlogged relation.
             */
            if RelationIsPermanent(rel)
                || ((*(*rel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED
                    && forkNum == INIT_FORKNUM)
            {
                log_smgrcreate(newrlocator, forkNum);
            }
            RelationCopyStorage(
                RelationGetSmgr(rel),
                dstrel,
                forkNum,
                (*(*rel).rd_rel).relpersistence,
            );
        }
        forkNum += 1;
    }


    /* drop old relation, and close new one */
    RelationDropStorage(rel);
    smgrclose(dstrel);
}

unsafe fn heapam_relation_copy_for_cluster(
    OldHeap: Relation,
    NewHeap: Relation,
    OldIndex: Relation,
    use_sort: bool,
    OldestXmin: TransactionId,
    xid_cutoff: *mut TransactionId,
    multi_cutoff: *mut MultiXactId,
    num_tuples: *mut f64,
    tups_vacuumed: *mut f64,
    tups_recently_dead: *mut f64,
) {
    let rwstate: RewriteState;
    let mut indexScan: IndexScanDesc;
    let mut tableScan: TableScanDesc;
    let mut heapScan: HeapScanDesc;
    let is_system_catalog: bool;
    let tuplesort: *mut Tuplesortstate;
    let oldTupDesc: TupleDesc = RelationGetDescr(OldHeap);
    let newTupDesc: TupleDesc = RelationGetDescr(NewHeap);
    let slot: *mut TupleTableSlot;
    let natts: c_int;
    let values: *mut Datum;
    let isnull: *mut bool;
    let hslot: *mut BufferHeapTupleTableSlot;
    let mut prev_cblock: BlockNumber = InvalidBlockNumber;

    /* Remember if it's a system catalog */
    is_system_catalog = IsSystemRelation(OldHeap);

    /*
     * Valid smgr_targblock implies something already wrote to the relation.
     * This may be harmless, but this function hasn't planned for it.
     */
    Assert!(RelationGetTargetBlock(NewHeap) == InvalidBlockNumber);

    /* Preallocate values/isnull arrays */
    natts = (*newTupDesc).natts;
    values = palloc(natts as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    isnull = palloc(natts as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Initialize the rewrite operation */
    rwstate = begin_heap_rewrite(OldHeap, NewHeap, OldestXmin, *xid_cutoff, *multi_cutoff);


    /* Set up sorting if wanted */
    if use_sort {
        tuplesort = tuplesort_begin_cluster(
            oldTupDesc,
            OldIndex,
            maintenance_work_mem,
            null_mut(),
            TUPLESORT_NONE,
        );
    } else {
        tuplesort = null_mut();
    }

    /*
     * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
     * that still need to be copied, we scan with SnapshotAny and use
     * HeapTupleSatisfiesVacuum for the visibility test.
     */
    if !OldIndex.is_null() && !use_sort {
        let ci_index: [c_int; 2] = [PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_INDEX_RELID];
        let mut ci_val: [int64; 2] = [0; 2];

        /* Set phase and OIDOldIndex to columns */
        ci_val[0] = PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP;
        ci_val[1] = RelationGetRelid(OldIndex) as int64;
        pgstat_progress_update_multi_param(2, ci_index.as_ptr(), ci_val.as_ptr());

        tableScan = null_mut();
        heapScan = null_mut();
        indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, null_mut(), 0, 0);
        index_rescan(indexScan, null_mut(), 0, null_mut(), 0);
    } else {
        /* In scan-and-sort mode and also VACUUM FULL, set phase */
        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);

        tableScan = table_beginscan(OldHeap, SnapshotAny, 0, null_mut());
        heapScan = tableScan as HeapScanDesc;
        indexScan = null_mut();

        /* Set total heap blocks */
        pgstat_progress_update_param(
            PROGRESS_CLUSTER_TOTAL_HEAP_BLKS,
            (*heapScan).rs_nblocks as int64,
        );
    }

    slot = table_slot_create(OldHeap, null_mut());
    hslot = slot as *mut BufferHeapTupleTableSlot;

    /*
     * Scan through the OldHeap, either in OldIndex order or sequentially;
     * copy each tuple into the NewHeap, or transiently to the tuplesort
     * module.  Note that we don't bother sorting dead tuples (they won't get
     * to the new table anyway).
     */
    loop {
        let tuple: HeapTuple;
        let buf: Buffer;
        let isdead: bool;

        CHECK_FOR_INTERRUPTS!();

        if !indexScan.is_null() {
            if !index_getnext_slot(indexScan, ForwardScanDirection, slot) {
                break;
            }

            /* Since we used no scan keys, should never need to recheck */
            if (*indexScan).xs_recheck {
                elog!(ERROR, "CLUSTER does not support lossy index conditions");
            }
        } else {
            if !table_scan_getnextslot(tableScan, ForwardScanDirection, slot) {
                /*
                 * If the last pages of the scan were empty, we would go to
                 * the next phase while heap_blks_scanned != heap_blks_total.
                 * Instead, to ensure that heap_blks_scanned is equivalent to
                 * heap_blks_total after the table scan phase, this parameter
                 * is manually updated to the correct value when the table
                 * scan finishes.
                 */
                pgstat_progress_update_param(
                    PROGRESS_CLUSTER_HEAP_BLKS_SCANNED,
                    (*heapScan).rs_nblocks as int64,
                );
                break;
            }

            /*
             * In scan-and-sort mode and also VACUUM FULL, set heap blocks
             * scanned
             *
             * Note that heapScan may start at an offset and wrap around, i.e.
             * rs_startblock may be >0, and rs_cblock may end with a number
             * below rs_startblock. To prevent showing this wraparound to the
             * user, we offset rs_cblock by rs_startblock (modulo rs_nblocks).
             */
            if prev_cblock != (*heapScan).rs_cblock {
                pgstat_progress_update_param(
                    PROGRESS_CLUSTER_HEAP_BLKS_SCANNED,
                    (((*heapScan).rs_cblock + (*heapScan).rs_nblocks - (*heapScan).rs_startblock)
                        % (*heapScan).rs_nblocks
                        + 1) as int64,
                );
                prev_cblock = (*heapScan).rs_cblock;
            }
        }

        tuple = ExecFetchSlotHeapTuple(slot, false, null_mut());
        buf = (*hslot).buffer;

        LockBuffer(buf, BUFFER_LOCK_SHARE);

        match HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf) {
            HEAPTUPLE_DEAD => {
                /* Definitely dead */
                isdead = true;
            }
            HEAPTUPLE_RECENTLY_DEAD => {
                *tups_recently_dead += 1.0;
                /* fall through */
                /* Live or recently dead, must copy it */
                isdead = false;
            }
            HEAPTUPLE_LIVE => {
                /* Live or recently dead, must copy it */
                isdead = false;
            }
            HEAPTUPLE_INSERT_IN_PROGRESS => {
                /*
                 * Since we hold exclusive lock on the relation, normally the
                 * only way to see this is if it was inserted earlier in our
                 * own transaction.  However, it can happen in system
                 * catalogs, since we tend to release write lock before commit
                 * there.  Give a warning if neither case applies; but in any
                 * case we had better copy it.
                 */
                if !is_system_catalog
                    && !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin((*tuple).t_data))
                {
                    elog!(
                        WARNING,
                        "concurrent insert in progress within table \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy()
                    );
                }
                /* treat as live */
                isdead = false;
            }
            HEAPTUPLE_DELETE_IN_PROGRESS => {
                /*
                 * Similar situation to INSERT_IN_PROGRESS case.
                 */
                if !is_system_catalog
                    && !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(
                        (*tuple).t_data,
                    ))
                {
                    elog!(
                        WARNING,
                        "concurrent delete in progress within table \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(OldHeap)).to_string_lossy()
                    );
                }
                /* treat as recently dead */
                *tups_recently_dead += 1.0;
                isdead = false;
            }
            _ => {
                elog!(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                isdead = false; /* keep compiler quiet */
            }
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        if isdead {
            *tups_vacuumed += 1.0;
            /* heap rewrite module still needs to see it... */
            if rewrite_heap_dead_tuple(rwstate, tuple) {
                /* A previous recently-dead tuple is now known dead */
                *tups_vacuumed += 1.0;
                *tups_recently_dead -= 1.0;
            }
            continue;
        }

        *num_tuples += 1.0;
        if !tuplesort.is_null() {
            tuplesort_putheaptuple(tuplesort, tuple);

            /*
             * In scan-and-sort mode, report increase in number of tuples
             * scanned
             */
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED, *num_tuples as int64);
        } else {
            let ct_index: [c_int; 2] = [
                PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED,
                PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN,
            ];
            let mut ct_val: [int64; 2] = [0; 2];

            reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate);

            /*
             * In indexscan mode and also VACUUM FULL, report increase in
             * number of tuples scanned and written
             */
            ct_val[0] = *num_tuples as int64;
            ct_val[1] = *num_tuples as int64;
            pgstat_progress_update_multi_param(2, ct_index.as_ptr(), ct_val.as_ptr());
        }
    }

    if !indexScan.is_null() {
        index_endscan(indexScan);
    }
    if !tableScan.is_null() {
        table_endscan(tableScan);
    }
    if !slot.is_null() {
        ExecDropSingleTupleTableSlot(slot);
    }

    /*
     * In scan-and-sort mode, complete the sort, then read out all live tuples
     * from the tuplestore and write them to the new relation.
     */
    if !tuplesort.is_null() {
        let mut n_tuples: f64 = 0.0;

        /* Report that we are now sorting tuples */
        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SORT_TUPLES);

        tuplesort_performsort(tuplesort);

        /* Report that we are now writing new heap */
        pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP);

        loop {
            let tuple: HeapTuple;

            CHECK_FOR_INTERRUPTS!();

            tuple = tuplesort_getheaptuple(tuplesort, true);
            if tuple.is_null() {
                break;
            }

            n_tuples += 1.0;
            reform_and_rewrite_tuple(tuple, OldHeap, NewHeap, values, isnull, rwstate);
            /* Report n_tuples */
            pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN, n_tuples as int64);
        }

        tuplesort_end(tuplesort);
    }

    /* Write out any remaining tuples, and fsync if needed */
    end_heap_rewrite(rwstate);

    /* Clean up */
    pfree(values as *mut c_void);
    pfree(isnull as *mut c_void);
}

/*
 * Prepare to analyze the next block in the read stream.  Returns false if
 * the stream is exhausted and true otherwise. The scan must have been started
 * with SO_TYPE_ANALYZE option.
 *
 * This routine holds a buffer pin and lock on the heap page.  They are held
 * until heapam_scan_analyze_next_tuple() returns false.  That is until all the
 * items of the heap page are analyzed.
 */
unsafe fn heapam_scan_analyze_next_block(scan: TableScanDesc, stream: *mut ReadStream) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;

    /*
     * We must maintain a pin on the target page's buffer to ensure that
     * concurrent activity - e.g. HOT pruning - doesn't delete tuples out from
     * under us.  It comes from the stream already pinned.   We also choose to
     * hold sharelock on the buffer throughout --- we could release and
     * re-acquire sharelock for each tuple, but since we aren't doing much
     * work per tuple, the extra lock traffic is probably better avoided.
     */
    (*hscan).rs_cbuf = read_stream_next_buffer(stream, null_mut());
    if !BufferIsValid((*hscan).rs_cbuf) {
        return false;
    }

    LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);

    (*hscan).rs_cblock = BufferGetBlockNumber((*hscan).rs_cbuf);
    (*hscan).rs_cindex = FirstOffsetNumber as c_int;
    true
}

unsafe fn heapam_scan_analyze_next_tuple(
    scan: TableScanDesc,
    OldestXmin: TransactionId,
    liverows: *mut f64,
    deadrows: *mut f64,
    slot: *mut TupleTableSlot,
) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;
    let targpage: Page;
    let maxoffset: OffsetNumber;
    let hslot: *mut BufferHeapTupleTableSlot;

    Assert!(TTS_IS_BUFFERTUPLE(slot));

    hslot = slot as *mut BufferHeapTupleTableSlot;
    targpage = BufferGetPage((*hscan).rs_cbuf);
    maxoffset = PageGetMaxOffsetNumber(targpage);

    /* Inner loop over all tuples on the selected page */
    while (*hscan).rs_cindex <= maxoffset as c_int {
        let itemid: ItemId;
        let targtuple: HeapTuple = &mut (*hslot).base.tupdata;
        let mut sample_it: bool = false;

        itemid = PageGetItemId(targpage, (*hscan).rs_cindex as OffsetNumber);

        /*
         * We ignore unused and redirect line pointers.  DEAD line pointers
         * should be counted as dead, because we need vacuum to run to get rid
         * of them.  Note that this rule agrees with the way that
         * heap_page_prune_and_freeze() counts things.
         */
        if !ItemIdIsNormal(itemid) {
            if ItemIdIsDead(itemid) {
                *deadrows += 1.0;
            }
            (*hscan).rs_cindex += 1;
            continue;
        }

        ItemPointerSet(
            &mut (*targtuple).t_self,
            (*hscan).rs_cblock,
            (*hscan).rs_cindex as OffsetNumber,
        );

        (*targtuple).t_tableOid = RelationGetRelid((*scan).rs_rd);
        (*targtuple).t_data = PageGetItem(targpage, itemid) as HeapTupleHeader;
        (*targtuple).t_len = ItemIdGetLength(itemid) as u32;

        match HeapTupleSatisfiesVacuum(targtuple, OldestXmin, (*hscan).rs_cbuf) {
            HEAPTUPLE_LIVE => {
                sample_it = true;
                *liverows += 1.0;
            }

            HEAPTUPLE_DEAD | HEAPTUPLE_RECENTLY_DEAD => {
                /* Count dead and recently-dead rows */
                *deadrows += 1.0;
            }

            HEAPTUPLE_INSERT_IN_PROGRESS => {
                /*
                 * Insert-in-progress rows are not counted.  We assume that
                 * when the inserting transaction commits or aborts, it will
                 * send a stats message to increment the proper count.  This
                 * works right only if that transaction ends after we finish
                 * analyzing the table; if things happen in the other order,
                 * its stats update will be overwritten by ours.  However, the
                 * error will be large only if the other transaction runs long
                 * enough to insert many tuples, so assuming it will finish
                 * after us is the safer option.
                 *
                 * A special case is that the inserting transaction might be
                 * our own.  In this case we should count and sample the row,
                 * to accommodate users who load a table and analyze it in one
                 * transaction.  (pgstat_report_analyze has to adjust the
                 * numbers we report to the cumulative stats system to make
                 * this come out right.)
                 */
                if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin((*targtuple).t_data)) {
                    sample_it = true;
                    *liverows += 1.0;
                }
            }

            HEAPTUPLE_DELETE_IN_PROGRESS => {
                /*
                 * We count and sample delete-in-progress rows the same as
                 * live ones, so that the stats counters come out right if the
                 * deleting transaction commits after us, per the same
                 * reasoning given above.
                 *
                 * If the delete was done by our own transaction, however, we
                 * must count the row as dead to make pgstat_report_analyze's
                 * stats adjustments come out right.  (Note: this works out
                 * properly when the row was both inserted and deleted in our
                 * xact.)
                 *
                 * The net effect of these choices is that we act as though an
                 * IN_PROGRESS transaction hasn't happened yet, except if it
                 * is our own transaction, which we assume has happened.
                 *
                 * This approach ensures that we behave sanely if we see both
                 * the pre-image and post-image rows for a row being updated
                 * by a concurrent transaction: we will sample the pre-image
                 * but not the post-image.  We also get sane results if the
                 * concurrent transaction never commits.
                 */
                if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(
                    (*targtuple).t_data,
                )) {
                    *deadrows += 1.0;
                } else {
                    sample_it = true;
                    *liverows += 1.0;
                }
            }

            _ => {
                elog!(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
            }
        }

        if sample_it {
            ExecStoreBufferHeapTuple(targtuple, slot, (*hscan).rs_cbuf);
            (*hscan).rs_cindex += 1;

            /* note that we leave the buffer locked here! */
            return true;
        }

        (*hscan).rs_cindex += 1;
    }

    /* Now release the lock and pin on the page */
    UnlockReleaseBuffer((*hscan).rs_cbuf);
    (*hscan).rs_cbuf = InvalidBuffer;

    /* also prevent old slot contents from having pin on page */
    ExecClearTuple(slot);

    false
}

// ---------------------------------------------------------------------------
// Additional helper homes for the appended functions.
// ---------------------------------------------------------------------------

// core::ffi - CStr (for %s -> {} formatting)
use core::ffi::CStr;

// c.h - MAXALIGN
use crate::c::MAXALIGN;

// postgres.h - DatumGetInt64
use crate::postgres::DatumGetInt64;

// nodes/pg_list.h - NIL
use crate::nodes::pg_list::NIL;

// utils/fmgr.h - FunctionCallInfo + PG_RETURN_POINTER
use crate::utils::fmgr::FunctionCallInfo;
use crate::PG_RETURN_POINTER;

// access/table/tableam.h - ParallelBlockTableScanDesc cast helper type already
// imported above; reuse ParallelBlockTableScanDescData for phs_* fields.

/*
 * Helper to extract the rd_rel->relam OID for assertions.
 */

unsafe fn heapam_index_build_range_scan(
    heapRelation: Relation,
    indexRelation: Relation,
    indexInfo: *mut IndexInfo,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: BlockNumber,
    numblocks: BlockNumber,
    callback: IndexBuildCallback,
    callback_state: *mut c_void,
    mut scan: TableScanDesc,
) -> f64 {
    let hscan: HeapScanDesc;
    let is_system_catalog: bool;
    let checking_uniqueness: bool;
    let mut heapTuple: HeapTuple;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut reltuples: f64;
    let predicate: *mut ExprState;
    let slot: *mut TupleTableSlot;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let mut snapshot: Snapshot;
    let mut need_unregister_snapshot: bool = false;
    let mut OldestXmin: TransactionId;
    let mut previous_blkno: BlockNumber = InvalidBlockNumber;
    let mut root_blkno: BlockNumber = InvalidBlockNumber;
    let mut root_offsets: [OffsetNumber; MaxHeapTuplesPerPage as usize] =
        [0; MaxHeapTuplesPerPage as usize];

    /*
     * sanity checks
     */
    Assert!(OidIsValid((*(*indexRelation).rd_rel).relam));

    /* Remember if it's a system catalog */
    is_system_catalog = IsSystemRelation(heapRelation);

    /* See whether we're verifying uniqueness/exclusion properties */
    checking_uniqueness =
        (*indexInfo).ii_Unique || !(*indexInfo).ii_ExclusionOps.is_null();

    /*
     * "Any visible" mode is not compatible with uniqueness checks; make sure
     * only one of those is requested.
     */
    Assert!(!(anyvisible && checking_uniqueness));

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.  Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = table_slot_create(heapRelation, null_mut());

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = ExecPrepareQual((*indexInfo).ii_Predicate, estate);

    /*
     * Prepare for scan of the base relation.  In a normal index build, we use
     * SnapshotAny because we must retrieve all tuples and do our own time
     * qual checks (because we have to index RECENTLY_DEAD tuples). In a
     * concurrent build, or during bootstrap, we take a regular MVCC snapshot
     * and index whatever's live according to that.
     */
    OldestXmin = InvalidTransactionId;

    /* okay to ignore lazy VACUUMs here */
    if !IsBootstrapProcessingMode() && !(*indexInfo).ii_Concurrent {
        OldestXmin = GetOldestNonRemovableTransactionId(heapRelation);
    }

    if scan.is_null() {
        /*
         * Serial index build.
         *
         * Must begin our own heap scan in this case.  We may also need to
         * register a snapshot whose lifetime is under our direct control.
         */
        if !TransactionIdIsValid(OldestXmin) {
            snapshot = RegisterSnapshot(GetTransactionSnapshot());
            need_unregister_snapshot = true;
        } else {
            snapshot = SnapshotAny;
        }

        scan = table_beginscan_strat(
            heapRelation, /* relation */
            snapshot,     /* snapshot */
            0,            /* number of keys */
            null_mut(),   /* scan key */
            true,         /* buffer access strategy OK */
            allow_sync,   /* syncscan OK? */
        );
    } else {
        /*
         * Parallel index build.
         *
         * Parallel case never registers/unregisters own snapshot.  Snapshot
         * is taken from parallel heap scan, and is SnapshotAny or an MVCC
         * snapshot, based on same criteria as serial case.
         */
        Assert!(!IsBootstrapProcessingMode());
        Assert!(allow_sync);
        snapshot = (*scan).rs_snapshot;
    }

    hscan = scan as HeapScanDesc;

    /*
     * Must have called GetOldestNonRemovableTransactionId() if using
     * SnapshotAny.  Shouldn't have for an MVCC snapshot. (It's especially
     * worth checking this for parallel builds, since ambuild routines that
     * support parallel builds must work these details out for themselves.)
     */
    Assert!(snapshot == SnapshotAny || IsMVCCSnapshot(snapshot));
    Assert!(if snapshot == SnapshotAny {
        TransactionIdIsValid(OldestXmin)
    } else {
        !TransactionIdIsValid(OldestXmin)
    });
    Assert!(snapshot == SnapshotAny || !anyvisible);

    /* Publish number of blocks to scan */
    if progress {
        let nblocks: BlockNumber;

        if !(*hscan).rs_base.rs_parallel.is_null() {
            let pbscan: ParallelBlockTableScanDesc =
                (*hscan).rs_base.rs_parallel as ParallelBlockTableScanDesc;
            nblocks = (*pbscan).phs_nblocks;
        } else {
            nblocks = (*hscan).rs_nblocks;
        }

        pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL, nblocks as int64);
    }

    /* set our scan endpoints */
    if !allow_sync {
        heap_setscanlimits(scan, start_blockno, numblocks);
    } else {
        /* syncscan can only be requested on whole relation */
        Assert!(start_blockno == 0);
        Assert!(numblocks == InvalidBlockNumber);
    }

    reltuples = 0.0;

    /*
     * Scan all tuples in the base relation.
     */
    loop {
        heapTuple = heap_getnext(scan, ForwardScanDirection);
        if heapTuple.is_null() {
            break;
        }

        let mut tupleIsAlive: bool;

        CHECK_FOR_INTERRUPTS!();

        /* Report scan progress, if asked to. */
        if progress {
            let blocks_done: BlockNumber = heapam_scan_get_blocks_done(hscan);

            if blocks_done != previous_blkno {
                pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE, blocks_done as int64);
                previous_blkno = blocks_done;
            }
        }

        /*
         * When dealing with a HOT-chain of updated tuples, we want to index
         * the values of the live tuple (if any), but index it under the TID
         * of the chain's root tuple.  This approach is necessary to preserve
         * the HOT-chain structure in the heap. So we need to be able to find
         * the root item offset for every tuple that's in a HOT-chain.  When
         * first reaching a new page of the relation, call
         * heap_get_root_tuples() to build a map of root item offsets on the
         * page.
         *
         * It might look unsafe to use this information across buffer
         * lock/unlock.  However, we hold ShareLock on the table so no
         * ordinary insert/update/delete should occur; and we hold pin on the
         * buffer continuously while visiting the page, so no pruning
         * operation can occur either.
         *
         * In cases with only ShareUpdateExclusiveLock on the table, it's
         * possible for some HOT tuples to appear that we didn't know about
         * when we first read the page.  To handle that case, we re-obtain the
         * list of root offsets when a HOT tuple points to a root item that we
         * don't know about.
         *
         * Also, although our opinions about tuple liveness could change while
         * we scan the page (due to concurrent transaction commits/aborts),
         * the chain root locations won't, so this info doesn't need to be
         * rebuilt after waiting for another transaction.
         *
         * Note the implied assumption that there is no more than one live
         * tuple per HOT-chain --- else we could create more than one index
         * entry pointing to the same root tuple.
         */
        if (*hscan).rs_cblock != root_blkno {
            let page: Page = BufferGetPage((*hscan).rs_cbuf);

            LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets.as_mut_ptr());
            LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);

            root_blkno = (*hscan).rs_cblock;
        }

        if snapshot == SnapshotAny {
            /* do our own time qual check */
            let mut indexIt: bool;
            let mut xwait: TransactionId;

            'recheck: loop {
                /*
                 * We could possibly get away with not locking the buffer here,
                 * since caller should hold ShareLock on the relation, but let's
                 * be conservative about it.  (This remark is still correct even
                 * with HOT-pruning: our pin on the buffer prevents pruning.)
                 */
                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);

                /*
                 * The criteria for counting a tuple as live in this block need to
                 * match what analyze.c's heapam_scan_analyze_next_tuple() does,
                 * otherwise CREATE INDEX and ANALYZE may produce wildly different
                 * reltuples values, e.g. when there are many recently-dead
                 * tuples.
                 */
                match HeapTupleSatisfiesVacuum(heapTuple, OldestXmin, (*hscan).rs_cbuf) {
                    HEAPTUPLE_DEAD => {
                        /* Definitely dead, we can ignore it */
                        indexIt = false;
                        tupleIsAlive = false;
                    }
                    HEAPTUPLE_LIVE => {
                        /* Normal case, index and unique-check it */
                        indexIt = true;
                        tupleIsAlive = true;
                        /* Count it as live, too */
                        reltuples += 1.0;
                    }
                    HEAPTUPLE_RECENTLY_DEAD => {
                        /*
                         * If tuple is recently deleted then we must index it
                         * anyway to preserve MVCC semantics.  (Pre-existing
                         * transactions could try to use the index after we finish
                         * building it, and may need to see such tuples.)
                         *
                         * However, if it was HOT-updated then we must only index
                         * the live tuple at the end of the HOT-chain.  Since this
                         * breaks semantics for pre-existing snapshots, mark the
                         * index as unusable for them.
                         *
                         * We don't count recently-dead tuples in reltuples, even
                         * if we index them; see heapam_scan_analyze_next_tuple().
                         */
                        if HeapTupleIsHotUpdated(heapTuple) {
                            indexIt = false;
                            /* mark the index as unsafe for old snapshots */
                            (*indexInfo).ii_BrokenHotChain = true;
                        } else {
                            indexIt = true;
                        }
                        /* In any case, exclude the tuple from unique-checking */
                        tupleIsAlive = false;
                    }
                    HEAPTUPLE_INSERT_IN_PROGRESS => {
                        /*
                         * In "anyvisible" mode, this tuple is visible and we
                         * don't need any further checks.
                         */
                        if anyvisible {
                            indexIt = true;
                            tupleIsAlive = true;
                            reltuples += 1.0;
                            break 'recheck;
                        }

                        /*
                         * Since caller should hold ShareLock or better, normally
                         * the only way to see this is if it was inserted earlier
                         * in our own transaction.  However, it can happen in
                         * system catalogs, since we tend to release write lock
                         * before commit there.  Give a warning if neither case
                         * applies.
                         */
                        xwait = HeapTupleHeaderGetXmin((*heapTuple).t_data);
                        if !TransactionIdIsCurrentTransactionId(xwait) {
                            if !is_system_catalog {
                                elog!(
                                    WARNING,
                                    "concurrent insert in progress within table \"{}\"",
                                    CStr::from_ptr(RelationGetRelationName(heapRelation))
                                        .to_string_lossy()
                                );
                            }

                            /*
                             * If we are performing uniqueness checks, indexing
                             * such a tuple could lead to a bogus uniqueness
                             * failure.  In that case we wait for the inserting
                             * transaction to finish and check again.
                             */
                            if checking_uniqueness {
                                /*
                                 * Must drop the lock on the buffer before we wait
                                 */
                                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);
                                XactLockTableWait(
                                    xwait,
                                    heapRelation,
                                    &mut (*heapTuple).t_self,
                                    XLTW_InsertIndexUnique,
                                );
                                CHECK_FOR_INTERRUPTS!();
                                continue 'recheck;
                            }
                        } else {
                            /*
                             * For consistency with
                             * heapam_scan_analyze_next_tuple(), count
                             * HEAPTUPLE_INSERT_IN_PROGRESS tuples as live only
                             * when inserted by our own transaction.
                             */
                            reltuples += 1.0;
                        }

                        /*
                         * We must index such tuples, since if the index build
                         * commits then they're good.
                         */
                        indexIt = true;
                        tupleIsAlive = true;
                    }
                    HEAPTUPLE_DELETE_IN_PROGRESS => {
                        /*
                         * As with INSERT_IN_PROGRESS case, this is unexpected
                         * unless it's our own deletion or a system catalog; but
                         * in anyvisible mode, this tuple is visible.
                         */
                        if anyvisible {
                            indexIt = true;
                            tupleIsAlive = false;
                            reltuples += 1.0;
                            break 'recheck;
                        }

                        xwait = HeapTupleHeaderGetUpdateXid((*heapTuple).t_data);
                        if !TransactionIdIsCurrentTransactionId(xwait) {
                            if !is_system_catalog {
                                elog!(
                                    WARNING,
                                    "concurrent delete in progress within table \"{}\"",
                                    CStr::from_ptr(RelationGetRelationName(heapRelation))
                                        .to_string_lossy()
                                );
                            }

                            /*
                             * If we are performing uniqueness checks, assuming
                             * the tuple is dead could lead to missing a
                             * uniqueness violation.  In that case we wait for the
                             * deleting transaction to finish and check again.
                             *
                             * Also, if it's a HOT-updated tuple, we should not
                             * index it but rather the live tuple at the end of
                             * the HOT-chain.  However, the deleting transaction
                             * could abort, possibly leaving this tuple as live
                             * after all, in which case it has to be indexed. The
                             * only way to know what to do is to wait for the
                             * deleting transaction to finish and check again.
                             */
                            if checking_uniqueness || HeapTupleIsHotUpdated(heapTuple) {
                                /*
                                 * Must drop the lock on the buffer before we wait
                                 */
                                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);
                                XactLockTableWait(
                                    xwait,
                                    heapRelation,
                                    &mut (*heapTuple).t_self,
                                    XLTW_InsertIndexUnique,
                                );
                                CHECK_FOR_INTERRUPTS!();
                                continue 'recheck;
                            }

                            /*
                             * Otherwise index it but don't check for uniqueness,
                             * the same as a RECENTLY_DEAD tuple.
                             */
                            indexIt = true;

                            /*
                             * Count HEAPTUPLE_DELETE_IN_PROGRESS tuples as live,
                             * if they were not deleted by the current
                             * transaction.  That's what
                             * heapam_scan_analyze_next_tuple() does, and we want
                             * the behavior to be consistent.
                             */
                            reltuples += 1.0;
                        } else if HeapTupleIsHotUpdated(heapTuple) {
                            /*
                             * It's a HOT-updated tuple deleted by our own xact.
                             * We can assume the deletion will commit (else the
                             * index contents don't matter), so treat the same as
                             * RECENTLY_DEAD HOT-updated tuples.
                             */
                            indexIt = false;
                            /* mark the index as unsafe for old snapshots */
                            (*indexInfo).ii_BrokenHotChain = true;
                        } else {
                            /*
                             * It's a regular tuple deleted by our own xact. Index
                             * it, but don't check for uniqueness nor count in
                             * reltuples, the same as a RECENTLY_DEAD tuple.
                             */
                            indexIt = true;
                        }
                        /* In any case, exclude the tuple from unique-checking */
                        tupleIsAlive = false;
                    }
                    _ => {
                        elog!(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                        indexIt = false; /* keep compiler quiet */
                        tupleIsAlive = false;
                    }
                }

                break 'recheck;
            }

            LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);

            if !indexIt {
                continue;
            }
        } else {
            /* heap_getnext did the time qual check */
            tupleIsAlive = true;
            reltuples += 1.0;
        }

        MemoryContextReset((*econtext).ecxt_per_tuple_memory);

        /* Set up for predicate or expression evaluation */
        ExecStoreBufferHeapTuple(heapTuple, slot, (*hscan).rs_cbuf);

        /*
         * In a partial index, discard tuples that don't satisfy the
         * predicate.
         */
        if !predicate.is_null() {
            if !ExecQual(predicate, econtext) {
                continue;
            }
        }

        /*
         * For the current heap tuple, extract all the attributes we use in
         * this index, and note which are null.  This also performs evaluation
         * of any expressions needed.
         */
        FormIndexDatum(
            indexInfo,
            slot,
            estate,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        /*
         * You'd think we should go ahead and build the index tuple here, but
         * some index AMs want to do further processing on the data first.  So
         * pass the values[] and isnull[] arrays, instead.
         */

        if HeapTupleIsHeapOnly(heapTuple) {
            /*
             * For a heap-only tuple, pretend its TID is that of the root. See
             * src/backend/access/heap/README.HOT for discussion.
             */
            let mut tid: ItemPointerData = core::mem::zeroed();
            let offnum: OffsetNumber;

            offnum = ItemPointerGetOffsetNumber(&mut (*heapTuple).t_self);

            /*
             * If a HOT tuple points to a root that we don't know about,
             * obtain root items afresh.  If that still fails, report it as
             * corruption.
             */
            if root_offsets[(offnum - 1) as usize] == InvalidOffsetNumber {
                let page: Page = BufferGetPage((*hscan).rs_cbuf);

                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);
                heap_get_root_tuples(page, root_offsets.as_mut_ptr());
                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);
            }

            if !OffsetNumberIsValid(root_offsets[(offnum - 1) as usize]) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "failed to find parent tuple for heap-only tuple at ({},{}) in table \"{}\"",
                        ItemPointerGetBlockNumber(&mut (*heapTuple).t_self),
                        offnum,
                        CStr::from_ptr(RelationGetRelationName(heapRelation)).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_DATA_CORRUPTED); errmsg_internal
            }

            ItemPointerSet(
                &mut tid,
                ItemPointerGetBlockNumber(&mut (*heapTuple).t_self),
                root_offsets[(offnum - 1) as usize],
            );

            /* Call the AM's callback routine to process the tuple */
            (callback.unwrap())(
                indexRelation,
                &mut tid,
                values.as_mut_ptr(),
                isnull.as_mut_ptr(),
                tupleIsAlive,
                callback_state,
            );
        } else {
            /* Call the AM's callback routine to process the tuple */
            (callback.unwrap())(
                indexRelation,
                &mut (*heapTuple).t_self,
                values.as_mut_ptr(),
                isnull.as_mut_ptr(),
                tupleIsAlive,
                callback_state,
            );
        }
    }

    /* Report scan progress one last time. */
    if progress {
        let blks_done: BlockNumber;

        if !(*hscan).rs_base.rs_parallel.is_null() {
            let pbscan: ParallelBlockTableScanDesc =
                (*hscan).rs_base.rs_parallel as ParallelBlockTableScanDesc;
            blks_done = (*pbscan).phs_nblocks;
        } else {
            blks_done = (*hscan).rs_nblocks;
        }

        pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE, blks_done as int64);
    }

    table_endscan(scan);

    /* we can now forget our snapshot, if set and registered by us */
    if need_unregister_snapshot {
        UnregisterSnapshot(snapshot);
    }

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    (*indexInfo).ii_ExpressionsState = NIL;
    (*indexInfo).ii_PredicateState = null_mut();

    reltuples
}

unsafe fn heapam_index_validate_scan(
    heapRelation: Relation,
    indexRelation: Relation,
    indexInfo: *mut IndexInfo,
    snapshot: Snapshot,
    state: *mut ValidateIndexState,
) {
    let scan: TableScanDesc;
    let hscan: HeapScanDesc;
    let mut heapTuple: HeapTuple;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let predicate: *mut ExprState;
    let slot: *mut TupleTableSlot;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let mut root_blkno: BlockNumber = InvalidBlockNumber;
    let mut root_offsets: [OffsetNumber; MaxHeapTuplesPerPage as usize] =
        [0; MaxHeapTuplesPerPage as usize];
    let mut in_index: [bool; MaxHeapTuplesPerPage as usize] =
        [false; MaxHeapTuplesPerPage as usize];
    let mut previous_blkno: BlockNumber = InvalidBlockNumber;

    /* state variables for the merge */
    let mut indexcursor: ItemPointer = null_mut();
    let mut decoded: ItemPointerData = core::mem::zeroed();
    let mut tuplesort_empty: bool = false;

    /*
     * sanity checks
     */
    Assert!(OidIsValid((*(*indexRelation).rd_rel).relam));

    /*
     * Need an EState for evaluation of index expressions and partial-index
     * predicates.  Also a slot to hold the current tuple.
     */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation), &TTSOpsHeapTuple);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* Set up execution state for predicate, if any. */
    predicate = ExecPrepareQual((*indexInfo).ii_Predicate, estate);

    /*
     * Prepare for scan of the base relation.  We need just those tuples
     * satisfying the passed-in reference snapshot.  We must disable syncscan
     * here, because it's critical that we read from block zero forward to
     * match the sorted TIDs.
     */
    scan = table_beginscan_strat(
        heapRelation, /* relation */
        snapshot,     /* snapshot */
        0,            /* number of keys */
        null_mut(),   /* scan key */
        true,         /* buffer access strategy OK */
        false,        /* syncscan not OK */
    );
    hscan = scan as HeapScanDesc;

    pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL, (*hscan).rs_nblocks as int64);

    /*
     * Scan all tuples matching the snapshot.
     */
    loop {
        heapTuple = heap_getnext(scan, ForwardScanDirection);
        if heapTuple.is_null() {
            break;
        }

        let heapcursor: ItemPointer = &mut (*heapTuple).t_self;
        let mut rootTuple: ItemPointerData = core::mem::zeroed();
        let mut root_offnum: OffsetNumber;

        CHECK_FOR_INTERRUPTS!();

        (*state).htups += 1.0;

        if (previous_blkno == InvalidBlockNumber) || ((*hscan).rs_cblock != previous_blkno) {
            pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE, (*hscan).rs_cblock as int64);
            previous_blkno = (*hscan).rs_cblock;
        }

        /*
         * As commented in table_index_build_scan, we should index heap-only
         * tuples under the TIDs of their root tuples; so when we advance onto
         * a new heap page, build a map of root item offsets on the page.
         *
         * This complicates merging against the tuplesort output: we will
         * visit the live tuples in order by their offsets, but the root
         * offsets that we need to compare against the index contents might be
         * ordered differently.  So we might have to "look back" within the
         * tuplesort output, but only within the current page.  We handle that
         * by keeping a bool array in_index[] showing all the
         * already-passed-over tuplesort output TIDs of the current page. We
         * clear that array here, when advancing onto a new heap page.
         */
        if (*hscan).rs_cblock != root_blkno {
            let page: Page = BufferGetPage((*hscan).rs_cbuf);

            LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);
            heap_get_root_tuples(page, root_offsets.as_mut_ptr());
            LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);

            core::ptr::write_bytes(in_index.as_mut_ptr(), 0, in_index.len());

            root_blkno = (*hscan).rs_cblock;
        }

        /* Convert actual tuple TID to root TID */
        rootTuple = *heapcursor;
        root_offnum = ItemPointerGetOffsetNumber(heapcursor);

        if HeapTupleIsHeapOnly(heapTuple) {
            root_offnum = root_offsets[(root_offnum - 1) as usize];
            if !OffsetNumberIsValid(root_offnum) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "failed to find parent tuple for heap-only tuple at ({},{}) in table \"{}\"",
                        ItemPointerGetBlockNumber(heapcursor),
                        ItemPointerGetOffsetNumber(heapcursor),
                        CStr::from_ptr(RelationGetRelationName(heapRelation)).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_DATA_CORRUPTED); errmsg_internal
            }
            ItemPointerSetOffsetNumber(&mut rootTuple, root_offnum);
        }

        /*
         * "merge" by skipping through the index tuples until we find or pass
         * the current root tuple.
         */
        while !tuplesort_empty
            && (indexcursor.is_null() || ItemPointerCompare(indexcursor, &mut rootTuple) < 0)
        {
            let mut ts_val: Datum = 0;
            let mut ts_isnull: bool = false;

            if !indexcursor.is_null() {
                /*
                 * Remember index items seen earlier on the current heap page
                 */
                if ItemPointerGetBlockNumber(indexcursor) == root_blkno {
                    in_index[(ItemPointerGetOffsetNumber(indexcursor) - 1) as usize] = true;
                }
            }

            tuplesort_empty = !tuplesort_getdatum(
                (*state).tuplesort,
                true,
                false,
                &mut ts_val,
                &mut ts_isnull,
                null_mut(),
            );
            Assert!(tuplesort_empty || !ts_isnull);
            if !tuplesort_empty {
                itemptr_decode(&mut decoded, DatumGetInt64(ts_val));
                indexcursor = &mut decoded;
            } else {
                /* Be tidy */
                indexcursor = null_mut();
            }
        }

        /*
         * If the tuplesort has overshot *and* we didn't see a match earlier,
         * then this tuple is missing from the index, so insert it.
         */
        if (tuplesort_empty || ItemPointerCompare(indexcursor, &mut rootTuple) > 0)
            && !in_index[(root_offnum - 1) as usize]
        {
            MemoryContextReset((*econtext).ecxt_per_tuple_memory);

            /* Set up for predicate or expression evaluation */
            ExecStoreHeapTuple(heapTuple, slot, false);

            /*
             * In a partial index, discard tuples that don't satisfy the
             * predicate.
             */
            if !predicate.is_null() {
                if !ExecQual(predicate, econtext) {
                    continue;
                }
            }

            /*
             * For the current heap tuple, extract all the attributes we use
             * in this index, and note which are null.  This also performs
             * evaluation of any expressions needed.
             */
            FormIndexDatum(
                indexInfo,
                slot,
                estate,
                values.as_mut_ptr(),
                isnull.as_mut_ptr(),
            );

            /*
             * You'd think we should go ahead and build the index tuple here,
             * but some index AMs want to do further processing on the data
             * first. So pass the values[] and isnull[] arrays, instead.
             */

            /*
             * If the tuple is already committed dead, you might think we
             * could suppress uniqueness checking, but this is no longer true
             * in the presence of HOT, because the insert is actually a proxy
             * for a uniqueness check on the whole HOT-chain.  That is, the
             * tuple we have here could be dead because it was already
             * HOT-updated, and if so the updating transaction will not have
             * thought it should insert index entries.  The index AM will
             * check the whole HOT-chain and correctly detect a conflict if
             * there is one.
             */

            index_insert(
                indexRelation,
                values.as_mut_ptr(),
                isnull.as_mut_ptr(),
                &mut rootTuple,
                heapRelation,
                if (*indexInfo).ii_Unique {
                    UNIQUE_CHECK_YES
                } else {
                    UNIQUE_CHECK_NO
                },
                false,
                indexInfo,
            );

            (*state).tups_inserted += 1.0;
        }
    }

    table_endscan(scan);

    ExecDropSingleTupleTableSlot(slot);

    FreeExecutorState(estate);

    /* These may have been pointing to the now-gone estate */
    (*indexInfo).ii_ExpressionsState = NIL;
    (*indexInfo).ii_PredicateState = null_mut();
}

/*
 * Return the number of blocks that have been read by this scan since
 * starting.  This is meant for progress reporting rather than be fully
 * accurate: in a parallel scan, workers can be concurrently reading blocks
 * further ahead than what we report.
 */
unsafe fn heapam_scan_get_blocks_done(hscan: HeapScanDesc) -> BlockNumber {
    let mut bpscan: ParallelBlockTableScanDesc = null_mut();
    let startblock: BlockNumber;
    let blocks_done: BlockNumber;

    if !(*hscan).rs_base.rs_parallel.is_null() {
        bpscan = (*hscan).rs_base.rs_parallel as ParallelBlockTableScanDesc;
        startblock = (*bpscan).phs_startblock;
    } else {
        startblock = (*hscan).rs_startblock;
    }

    /*
     * Might have wrapped around the end of the relation, if startblock was
     * not zero.
     */
    if (*hscan).rs_cblock > startblock {
        blocks_done = (*hscan).rs_cblock - startblock;
    } else {
        let nblocks: BlockNumber;

        nblocks = if !bpscan.is_null() {
            (*bpscan).phs_nblocks
        } else {
            (*hscan).rs_nblocks
        };
        blocks_done = nblocks - startblock + (*hscan).rs_cblock;
    }

    blocks_done
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

/*
 * Check to see whether the table needs a TOAST table.  It does only if
 * (1) there are any toastable attributes, and (2) the maximum length
 * of a tuple could exceed TOAST_TUPLE_THRESHOLD.  (We don't want to
 * create a toast table for something like "f1 varchar(20)".)
 */
unsafe fn heapam_relation_needs_toast_table(rel: Relation) -> bool {
    let mut data_length: int32 = 0;
    let mut maxlength_unknown: bool = false;
    let mut has_toastable_attrs: bool = false;
    let tupdesc: TupleDesc = (*rel).rd_att;
    let tuple_length: int32;
    let mut i: c_int;

    i = 0;
    while i < (*tupdesc).natts {
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        if (*att).attisdropped {
            i += 1;
            continue;
        }
        if (*att).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            i += 1;
            continue;
        }
        data_length = att_align_nominal(data_length as usize, (*att).attalign) as int32;
        if (*att).attlen > 0 {
            /* Fixed-length types are never toastable */
            data_length += (*att).attlen as int32;
        } else {
            let maxlen: int32 = type_maximum_size((*att).atttypid, (*att).atttypmod);

            if maxlen < 0 {
                maxlength_unknown = true;
            } else {
                data_length += maxlen;
            }
            if (*att).attstorage != TYPSTORAGE_PLAIN as c_char {
                has_toastable_attrs = true;
            }
        }
        i += 1;
    }
    if !has_toastable_attrs {
        return false; /* nothing to toast? */
    }
    if maxlength_unknown {
        return true; /* any unlimited-length attrs? */
    }
    tuple_length = (MAXALIGN(SizeofHeapTupleHeader + BITMAPLEN((*tupdesc).natts as usize))
        + MAXALIGN(data_length as usize)) as int32;
    tuple_length > TOAST_TUPLE_THRESHOLD as int32
}

/*
 * TOAST tables for heap relations are just heap relations.
 */
unsafe fn heapam_relation_toast_am(rel: Relation) -> Oid {
    (*(*rel).rd_rel).relam
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

macro_rules! HEAP_OVERHEAD_BYTES_PER_TUPLE {
    () => {
        MAXALIGN(SizeofHeapTupleHeader) + core::mem::size_of::<ItemIdData>()
    };
}
macro_rules! HEAP_USABLE_BYTES_PER_PAGE {
    () => {
        BLCKSZ as usize - SizeOfPageHeaderData
    };
}

unsafe fn heapam_estimate_rel_size(
    rel: Relation,
    attr_widths: *mut int32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    table_block_relation_estimate_size(
        rel,
        attr_widths,
        pages,
        tuples,
        allvisfrac,
        HEAP_OVERHEAD_BYTES_PER_TUPLE!(),
        HEAP_USABLE_BYTES_PER_PAGE!(),
    );
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

unsafe fn heapam_scan_bitmap_next_tuple(
    scan: TableScanDesc,
    slot: *mut TupleTableSlot,
    recheck: *mut bool,
    lossy_pages: *mut uint64,
    exact_pages: *mut uint64,
) -> bool {
    let bscan: BitmapHeapScanDesc = scan as BitmapHeapScanDesc;
    let hscan: HeapScanDesc = bscan as HeapScanDesc;
    let targoffset: OffsetNumber;
    let page: Page;
    let lp: ItemId;

    /*
     * Out of range?  If so, nothing more to look at on this page
     */
    while (*hscan).rs_cindex >= (*hscan).rs_ntuples {
        /*
         * Returns false if the bitmap is exhausted and there are no further
         * blocks we need to scan.
         */
        if !BitmapHeapScanNextBlock(scan, recheck, lossy_pages, exact_pages) {
            return false;
        }
    }

    targoffset = (*hscan).rs_vistuples[(*hscan).rs_cindex as usize];
    page = BufferGetPage((*hscan).rs_cbuf);
    lp = PageGetItemId(page, targoffset);
    Assert!(ItemIdIsNormal(lp));

    (*hscan).rs_ctup.t_data = PageGetItem(page, lp) as HeapTupleHeader;
    (*hscan).rs_ctup.t_len = ItemIdGetLength(lp) as u32;
    (*hscan).rs_ctup.t_tableOid = (*(*scan).rs_rd).rd_id;
    ItemPointerSet(&mut (*hscan).rs_ctup.t_self, (*hscan).rs_cblock, targoffset);

    pgstat_count_heap_fetch((*scan).rs_rd);

    /*
     * Set up the result slot to point to this tuple.  Note that the slot
     * acquires a pin on the buffer.
     */
    ExecStoreBufferHeapTuple(&mut (*hscan).rs_ctup, slot, (*hscan).rs_cbuf);

    (*hscan).rs_cindex += 1;

    true
}

unsafe fn heapam_scan_sample_next_block(
    scan: TableScanDesc,
    scanstate: *mut SampleScanState,
) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;
    let tsm: *mut TsmRoutine = (*scanstate).tsmroutine as *mut TsmRoutine;
    let mut blockno: BlockNumber;

    /* return false immediately if relation is empty */
    if (*hscan).rs_nblocks == 0 {
        return false;
    }

    /* release previous scan buffer, if any */
    if BufferIsValid((*hscan).rs_cbuf) {
        ReleaseBuffer((*hscan).rs_cbuf);
        (*hscan).rs_cbuf = InvalidBuffer;
    }

    if let Some(next_sample_block) = (*tsm).NextSampleBlock {
        blockno = next_sample_block(scanstate, (*hscan).rs_nblocks);
    } else {
        /* scanning table sequentially */

        if (*hscan).rs_cblock == InvalidBlockNumber {
            Assert!(!(*hscan).rs_inited);
            blockno = (*hscan).rs_startblock;
        } else {
            Assert!((*hscan).rs_inited);

            blockno = (*hscan).rs_cblock + 1;

            if blockno >= (*hscan).rs_nblocks {
                /* wrap to beginning of rel, might not have started at 0 */
                blockno = 0;
            }

            /*
             * Report our new scan position for synchronization purposes.
             *
             * Note: we do this before checking for end of scan so that the
             * final state of the position hint is back at the start of the
             * rel.  That's not strictly necessary, but otherwise when you run
             * the same query multiple times the starting position would shift
             * a little bit backwards on every invocation, which is confusing.
             * We don't guarantee any specific ordering in general, though.
             */
            if ((*scan).rs_flags & (SO_ALLOW_SYNC as u32)) != 0 {
                ss_report_location((*scan).rs_rd, blockno);
            }

            if blockno == (*hscan).rs_startblock {
                blockno = InvalidBlockNumber;
            }
        }
    }

    (*hscan).rs_cblock = blockno;

    if !BlockNumberIsValid(blockno) {
        (*hscan).rs_inited = false;
        return false;
    }

    Assert!((*hscan).rs_cblock < (*hscan).rs_nblocks);

    /*
     * Be sure to check for interrupts at least once per page.  Checks at
     * higher code levels won't be able to stop a sample scan that encounters
     * many pages' worth of consecutive dead tuples.
     */
    CHECK_FOR_INTERRUPTS!();

    /* Read page using selected strategy */
    (*hscan).rs_cbuf = ReadBufferExtended(
        (*hscan).rs_base.rs_rd,
        MAIN_FORKNUM,
        blockno,
        RBM_NORMAL,
        (*hscan).rs_strategy,
    );

    /* in pagemode, prune the page and determine visible tuple offsets */
    if ((*hscan).rs_base.rs_flags & (SO_ALLOW_PAGEMODE as u32)) != 0 {
        heap_prepare_pagescan(scan);
    }

    (*hscan).rs_inited = true;
    true
}

unsafe fn heapam_scan_sample_next_tuple(
    scan: TableScanDesc,
    scanstate: *mut SampleScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;
    let tsm: *mut TsmRoutine = (*scanstate).tsmroutine as *mut TsmRoutine;
    let blockno: BlockNumber = (*hscan).rs_cblock;
    let pagemode: bool = ((*scan).rs_flags & (SO_ALLOW_PAGEMODE as u32)) != 0;

    let page: Page;
    let all_visible: bool;
    let maxoffset: OffsetNumber;

    /*
     * When not using pagemode, we must lock the buffer during tuple
     * visibility checks.
     */
    if !pagemode {
        LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_SHARE);
    }

    page = BufferGetPage((*hscan).rs_cbuf);
    all_visible = PageIsAllVisible(page) && !(*(*scan).rs_snapshot).takenDuringRecovery;
    maxoffset = PageGetMaxOffsetNumber(page);

    loop {
        let tupoffset: OffsetNumber;

        CHECK_FOR_INTERRUPTS!();

        /* Ask the tablesample method which tuples to check on this page. */
        tupoffset = ((*tsm).NextSampleTuple.unwrap())(scanstate, blockno, maxoffset);

        if OffsetNumberIsValid(tupoffset) {
            let itemid: ItemId;
            let visible: bool;
            let tuple: HeapTuple = &mut (*hscan).rs_ctup;

            /* Skip invalid tuple pointers. */
            itemid = PageGetItemId(page, tupoffset);
            if !ItemIdIsNormal(itemid) {
                continue;
            }

            (*tuple).t_data = PageGetItem(page, itemid) as HeapTupleHeader;
            (*tuple).t_len = ItemIdGetLength(itemid) as u32;
            ItemPointerSet(&mut (*tuple).t_self, blockno, tupoffset);


            if all_visible {
                visible = true;
            } else {
                visible = SampleHeapTupleVisible(scan, (*hscan).rs_cbuf, tuple, tupoffset);
            }

            /* in pagemode, heap_prepare_pagescan did this for us */
            if !pagemode {
                HeapCheckForSerializableConflictOut(
                    visible,
                    (*scan).rs_rd,
                    tuple,
                    (*hscan).rs_cbuf,
                    (*scan).rs_snapshot,
                );
            }

            /* Try next tuple from same page. */
            if !visible {
                continue;
            }

            /* Found visible tuple, return it. */
            if !pagemode {
                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);
            }

            ExecStoreBufferHeapTuple(tuple, slot, (*hscan).rs_cbuf);

            /* Count successfully-fetched tuples as heap fetches */
            pgstat_count_heap_getnext((*scan).rs_rd);

            return true;
        } else {
            /*
             * If we get here, it means we've exhausted the items on this page
             * and it's time to move to the next.
             */
            if !pagemode {
                LockBuffer((*hscan).rs_cbuf, BUFFER_LOCK_UNLOCK);
            }

            ExecClearTuple(slot);
            return false;
        }
    }
}


/* ----------------------------------------------------------------------------
 *  Helper functions for the above.
 * ----------------------------------------------------------------------------
 */

/*
 * Reconstruct and rewrite the given tuple
 *
 * We cannot simply copy the tuple as-is, for several reasons:
 *
 * 1. We'd like to squeeze out the values of any dropped columns, both
 * to save space and to ensure we have no corner-case failures. (It's
 * possible for example that the new table hasn't got a TOAST table
 * and so is unable to store any large values of dropped cols.)
 *
 * 2. The tuple might not even be legal for the new table; this is
 * currently only known to happen as an after-effect of ALTER TABLE
 * SET WITHOUT OIDS.
 *
 * So, we must reconstruct the tuple from component Datums.
 */
unsafe fn reform_and_rewrite_tuple(
    tuple: HeapTuple,
    OldHeap: Relation,
    NewHeap: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    rwstate: RewriteState,
) {
    let oldTupDesc: TupleDesc = RelationGetDescr(OldHeap);
    let newTupDesc: TupleDesc = RelationGetDescr(NewHeap);
    let copiedTuple: HeapTuple;
    let mut i: c_int;

    heap_deform_tuple(tuple, oldTupDesc, values, isnull);

    /* Be sure to null out any dropped columns */
    i = 0;
    while i < (*newTupDesc).natts {
        if (*TupleDescCompactAttr(newTupDesc, i)).attisdropped {
            *isnull.offset(i as isize) = true;
        }
        i += 1;
    }

    copiedTuple = heap_form_tuple(newTupDesc, values, isnull);

    /* The heap rewrite module does the rest */
    rewrite_heap_tuple(rwstate, tuple, copiedTuple);

    heap_freetuple(copiedTuple);
}

/*
 * Check visibility of the tuple.
 */
unsafe fn SampleHeapTupleVisible(
    scan: TableScanDesc,
    buffer: Buffer,
    tuple: HeapTuple,
    tupoffset: OffsetNumber,
) -> bool {
    let hscan: HeapScanDesc = scan as HeapScanDesc;

    if ((*scan).rs_flags & (SO_ALLOW_PAGEMODE as u32)) != 0 {
        let mut start: uint32 = 0;
        let mut end: uint32 = (*hscan).rs_ntuples as uint32;

        /*
         * In pageatatime mode, heap_prepare_pagescan() already did visibility
         * checks, so just look at the info it left in rs_vistuples[].
         *
         * We use a binary search over the known-sorted array.  Note: we could
         * save some effort if we insisted that NextSampleTuple select tuples
         * in increasing order, but it's not clear that there would be enough
         * gain to justify the restriction.
         */
        while start < end {
            let mid: uint32 = start + (end - start) / 2;
            let curoffset: OffsetNumber = (*hscan).rs_vistuples[mid as usize];

            if tupoffset == curoffset {
                return true;
            } else if tupoffset < curoffset {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        false
    } else {
        /* Otherwise, we have to check the tuple individually. */
        HeapTupleSatisfiesVisibility(tuple, (*scan).rs_snapshot, buffer)
    }
}

/*
 * Helper function get the next block of a bitmap heap scan. Returns true when
 * it got the next block and saved it in the scan descriptor and false when
 * the bitmap and or relation are exhausted.
 */
unsafe fn BitmapHeapScanNextBlock(
    scan: TableScanDesc,
    recheck: *mut bool,
    lossy_pages: *mut uint64,
    exact_pages: *mut uint64,
) -> bool {
    let bscan: BitmapHeapScanDesc = scan as BitmapHeapScanDesc;
    let hscan: HeapScanDesc = bscan as HeapScanDesc;
    let block: BlockNumber;
    let mut per_buffer_data: *mut c_void = null_mut();
    let buffer: Buffer;
    let snapshot: Snapshot;
    let mut ntup: c_int;
    let tbmres: *mut TBMIterateResult;
    let mut offsets: [OffsetNumber; TBM_MAX_TUPLES_PER_PAGE] = [0; TBM_MAX_TUPLES_PER_PAGE];
    let mut noffsets: c_int = -1;

    Assert!(((*scan).rs_flags & (SO_TYPE_BITMAPSCAN as u32)) != 0);
    Assert!(!(*hscan).rs_read_stream.is_null());

    (*hscan).rs_cindex = 0;
    (*hscan).rs_ntuples = 0;

    /* Release buffer containing previous block. */
    if BufferIsValid((*hscan).rs_cbuf) {
        ReleaseBuffer((*hscan).rs_cbuf);
        (*hscan).rs_cbuf = InvalidBuffer;
    }

    (*hscan).rs_cbuf = read_stream_next_buffer((*hscan).rs_read_stream, &mut per_buffer_data);

    if BufferIsInvalid((*hscan).rs_cbuf) {
        /* the bitmap is exhausted */
        return false;
    }

    Assert!(!per_buffer_data.is_null());

    tbmres = per_buffer_data as *mut TBMIterateResult;

    Assert!(BlockNumberIsValid((*tbmres).blockno));
    Assert!(BufferGetBlockNumber((*hscan).rs_cbuf) == (*tbmres).blockno);

    /* Exact pages need their tuple offsets extracted. */
    if !(*tbmres).lossy {
        noffsets =
            tbm_extract_page_tuple(tbmres, offsets.as_mut_ptr(), TBM_MAX_TUPLES_PER_PAGE);
    }

    *recheck = (*tbmres).recheck;

    (*hscan).rs_cblock = (*tbmres).blockno;
    block = (*hscan).rs_cblock;
    buffer = (*hscan).rs_cbuf;
    snapshot = (*scan).rs_snapshot;

    ntup = 0;

    /*
     * Prune and repair fragmentation for the whole page, if possible.
     */
    heap_page_prune_opt((*scan).rs_rd, buffer);

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.  Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    /*
     * We need two separate strategies for lossy and non-lossy cases.
     */
    if !(*tbmres).lossy {
        /*
         * Bitmap is non-lossy, so we just look through the offsets listed in
         * tbmres; but we have to follow any HOT chain starting at each such
         * offset.
         */
        let mut curslot: c_int;

        /* We must have extracted the tuple offsets by now */
        Assert!(noffsets > -1);

        curslot = 0;
        while curslot < noffsets {
            let offnum: OffsetNumber = offsets[curslot as usize];
            let mut tid: ItemPointerData = core::mem::zeroed();
            let mut heapTuple: HeapTupleData = core::mem::zeroed();

            ItemPointerSet(&mut tid, block, offnum);
            if heap_hot_search_buffer(
                &mut tid,
                (*scan).rs_rd,
                buffer,
                snapshot,
                &mut heapTuple,
                null_mut(),
                true,
            ) {
                (*hscan).rs_vistuples[ntup as usize] = ItemPointerGetOffsetNumber(&mut tid);
                ntup += 1;
            }
            curslot += 1;
        }
    } else {
        /*
         * Bitmap is lossy, so we must examine each line pointer on the page.
         * But we can ignore HOT chains, since we'll check each tuple anyway.
         */
        let page: Page = BufferGetPage(buffer);
        let maxoff: OffsetNumber = PageGetMaxOffsetNumber(page);
        let mut offnum: OffsetNumber;

        offnum = FirstOffsetNumber;
        while offnum <= maxoff {
            let lp: ItemId;
            let mut loctup: HeapTupleData = core::mem::zeroed();
            let valid: bool;

            lp = PageGetItemId(page, offnum);
            if !ItemIdIsNormal(lp) {
                offnum = OffsetNumberNext(offnum);
                continue;
            }
            loctup.t_data = PageGetItem(page, lp) as HeapTupleHeader;
            loctup.t_len = ItemIdGetLength(lp) as u32;
            loctup.t_tableOid = (*(*scan).rs_rd).rd_id;
            ItemPointerSet(&mut loctup.t_self, block, offnum);
            valid = HeapTupleSatisfiesVisibility(&mut loctup, snapshot, buffer);
            if valid {
                (*hscan).rs_vistuples[ntup as usize] = offnum;
                ntup += 1;
                PredicateLockTID(
                    (*scan).rs_rd,
                    &mut loctup.t_self,
                    snapshot,
                    HeapTupleHeaderGetXmin(loctup.t_data),
                );
            }
            HeapCheckForSerializableConflictOut(valid, (*scan).rs_rd, &mut loctup, buffer, snapshot);
            offnum = OffsetNumberNext(offnum);
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    Assert!(ntup <= MaxHeapTuplesPerPage as c_int);
    (*hscan).rs_ntuples = ntup;

    if (*tbmres).lossy {
        *lossy_pages += 1;
    } else {
        *exact_pages += 1;
    }

    /*
     * Return true to indicate that a valid block was found and the bitmap is
     * not exhausted. If there are no visible tuples on this page,
     * hscan->rs_ntuples will be 0 and heapam_scan_bitmap_next_tuple() will
     * return false returning control to this function to advance to the next
     * block in the bitmap.
     */
    true
}

/* ------------------------------------------------------------------------
 * Definition of the heap table access method.
 * ------------------------------------------------------------------------
 */

static heapam_methods: TableAmRoutine = TableAmRoutine {
    r#type: NodeTag::T_TableAmRoutine,

    slot_callbacks: Some(heapam_slot_callbacks),

    scan_begin: Some(heap_beginscan),
    scan_end: Some(heap_endscan),
    scan_rescan: Some(heap_rescan),
    scan_getnextslot: Some(heap_getnextslot),

    scan_set_tidrange: Some(heap_set_tidrange),
    scan_getnextslot_tidrange: Some(heap_getnextslot_tidrange),

    parallelscan_estimate: Some(table_block_parallelscan_estimate),
    parallelscan_initialize: Some(table_block_parallelscan_initialize),
    parallelscan_reinitialize: Some(table_block_parallelscan_reinitialize),

    index_fetch_begin: Some(heapam_index_fetch_begin),
    index_fetch_reset: Some(heapam_index_fetch_reset),
    index_fetch_end: Some(heapam_index_fetch_end),
    index_fetch_tuple: Some(heapam_index_fetch_tuple),

    tuple_insert: Some(heapam_tuple_insert),
    tuple_insert_speculative: Some(heapam_tuple_insert_speculative),
    tuple_complete_speculative: Some(heapam_tuple_complete_speculative),
    multi_insert: Some(heap_multi_insert),
    tuple_delete: Some(heapam_tuple_delete),
    tuple_update: Some(heapam_tuple_update),
    tuple_lock: Some(heapam_tuple_lock),

    tuple_fetch_row_version: Some(heapam_fetch_row_version),
    tuple_get_latest_tid: Some(heap_get_latest_tid),
    tuple_tid_valid: Some(heapam_tuple_tid_valid),
    tuple_satisfies_snapshot: Some(heapam_tuple_satisfies_snapshot),
    index_delete_tuples: Some(heap_index_delete_tuples),

    relation_set_new_filelocator: Some(heapam_relation_set_new_filelocator),
    relation_nontransactional_truncate: Some(heapam_relation_nontransactional_truncate),
    relation_copy_data: Some(heapam_relation_copy_data),
    relation_copy_for_cluster: Some(heapam_relation_copy_for_cluster),
    relation_vacuum: Some(heap_vacuum_rel),
    scan_analyze_next_block: Some(heapam_scan_analyze_next_block),
    scan_analyze_next_tuple: Some(heapam_scan_analyze_next_tuple),
    index_build_range_scan: Some(heapam_index_build_range_scan),
    index_validate_scan: Some(heapam_index_validate_scan),

    relation_size: Some(table_block_relation_size),
    relation_needs_toast_table: Some(heapam_relation_needs_toast_table),
    relation_toast_am: Some(heapam_relation_toast_am),
    relation_fetch_toast_slice: Some(heap_fetch_toast_slice),

    relation_estimate_size: Some(heapam_estimate_rel_size),

    scan_bitmap_next_tuple: Some(heapam_scan_bitmap_next_tuple),
    scan_sample_next_block: Some(heapam_scan_sample_next_block),
    scan_sample_next_tuple: Some(heapam_scan_sample_next_tuple),

    finish_bulk_insert: None,
};


pub unsafe fn GetHeapamTableAmRoutine() -> *const TableAmRoutine {
    &heapam_methods
}

pub unsafe fn heap_tableam_handler(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_POINTER!(&heapam_methods)
}

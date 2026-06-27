//! Translated from PostgreSQL src/include/access/relscan.h
//! Relation scan descriptor definitions (in-memory scan state).

use core::sync::atomic::AtomicU64;

use crate::access::genam::IndexScanInstrumentation;
use crate::access::htup::HeapTuple;
use crate::access::itup::IndexTuple;
use crate::access::skey::ScanKeyData;
use crate::access::tupdesc::TupleDescData;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::tidbitmap::TBMIterator;
use crate::postgres::Datum;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::SnapshotData;

/// Scan type-specific state of a TableScanDesc (C anonymous union `st`).
pub enum TableScanType {
    /// Iterator for bitmap table scans.
    Bitmap(TBMIterator),
    /// ItemPointer range for table_scan_getnextslot_tidrange().
    TidRange {
        rs_mintid: ItemPointerData,
        rs_maxtid: ItemPointerData,
    },
    None,
}

/// Generic descriptor for table scans; base-class embedded in each AM's scan.
pub struct TableScanDescData {
    // scan parameters
    pub rs_rd: Relation,                          // heap relation descriptor
    pub rs_snapshot: *mut SnapshotData,           // snapshot to see // TODO(ptr)
    pub rs_nkeys: i32,                            // number of scan keys
    pub rs_key: *mut ScanKeyData,                 // array of scan key descriptors // TODO(ptr)
    /// scan type-specific members (C union `st`)
    pub st: TableScanType,
    /// bitmask of ScanOptions members (see tableam.h)
    pub rs_flags: u32,
    pub rs_parallel: *mut ParallelTableScanDescData, // parallel scan info // TODO(ptr)
}
pub type TableScanDesc = *mut TableScanDescData; // TODO(ptr)

/// Shared state for a parallel table scan.
pub struct ParallelTableScanDescData {
    pub locator: RelFileLocator, // physical relation to scan
    pub syncscan: bool,          // report location to syncscan logic?
    pub snapshot_any: bool,      // SnapshotAny, not phs_snapshot_data?
    pub snapshot_off: usize,     // data for snapshot
}
pub type ParallelTableScanDesc = *mut ParallelTableScanDescData; // TODO(ptr)

/// Shared state for parallel table scans, for block-oriented storage.
pub struct ParallelBlockTableScanDescData {
    pub base: ParallelTableScanDescData,
    pub phs_nblocks: u32, // # blocks in relation at start of scan
    // slock_t phs_mutex -> std Mutex (single-process); guards phs_startblock.
    pub phs_mutex: parking_lot::Mutex<()>,
    pub phs_startblock: u32,        // starting block number
    pub phs_nallocated: AtomicU64,  // blocks allocated to workers so far
}
pub type ParallelBlockTableScanDesc = *mut ParallelBlockTableScanDescData; // TODO(ptr)

/// Per-backend state for parallel table scan, block-oriented storage.
pub struct ParallelBlockTableScanWorkerData {
    pub nallocated: u64,       // current # of blocks into the scan
    pub chunk_remaining: u32,  // # blocks left in this chunk
    pub chunk_size: u32,       // # blocks to allocate per I/O chunk
}
pub type ParallelBlockTableScanWorker = *mut ParallelBlockTableScanWorkerData; // TODO(ptr)

/// Base class for fetches from a table via an index.
pub struct IndexFetchTableData {
    pub rel: Relation,
}

/// Index scan descriptor, shared by amgettuple- and amgetbitmap-based scans.
pub struct IndexScanDescData {
    // scan parameters
    pub heapRelation: Relation,         // heap relation descriptor, or NULL
    pub indexRelation: Relation,        // index relation descriptor
    pub xs_snapshot: *mut SnapshotData, // snapshot to see // TODO(ptr)
    pub numberOfKeys: i32,              // number of index qualifier conditions
    pub numberOfOrderBys: i32,         // number of ordering operators
    pub keyData: *mut ScanKeyData,     // array of index qualifier descriptors // TODO(ptr)
    pub orderByData: *mut ScanKeyData, // array of ordering op descriptors // TODO(ptr)
    pub xs_want_itup: bool,            // caller requests index tuples
    pub xs_temp_snap: bool,            // unregister snapshot at scan end?

    // signaling to index AM about killing index tuples
    pub kill_prior_tuple: bool,        // last-returned tuple is dead
    pub ignore_killed_tuples: bool,    // do not return killed entries
    pub xactStartedInRecovery: bool,   // prevents killing/seeing killed tuples

    /// access-method-specific private state (C `void *opaque`).
    pub opaque: *mut core::ffi::c_void, // TODO(ptr)

    /// instrumentation counters maintained by all index AMs (or NULL)
    pub instrument: *mut IndexScanInstrumentation, // TODO(ptr)

    // index-only scan: amgettuple fills xs_itup/xs_hitup
    pub xs_itup: IndexTuple,               // index tuple returned by AM
    pub xs_itupdesc: *mut TupleDescData,   // rowtype descriptor of xs_itup // TODO(ptr)
    pub xs_hitup: HeapTuple,               // index data returned by AM, as HeapTuple
    pub xs_hitupdesc: *mut TupleDescData,  // rowtype descriptor of xs_hitup // TODO(ptr)

    pub xs_heaptid: ItemPointerData, // result
    pub xs_heap_continue: bool,      // T if must keep walking
    pub xs_heapfetch: *mut IndexFetchTableData, // TODO(ptr)

    pub xs_recheck: bool, // T means scan keys must be rechecked

    // ORDER BY operator support
    pub xs_orderbyvals: *mut Datum,  // TODO(ptr)
    pub xs_orderbynulls: *mut bool,  // TODO(ptr)
    pub xs_recheckorderby: bool,

    pub parallel_scan: *mut ParallelIndexScanDescData, // shared memory // TODO(ptr)
}
pub type IndexScanDesc = *mut IndexScanDescData; // TODO(ptr)

/// Generic structure for parallel index scans. `ps_snapshot_data` is an on-disk
/// FAM tail in C; kept as a header here (snapshot data follows in the buffer).
pub struct ParallelIndexScanDescData {
    pub locator: RelFileLocator,      // physical table relation to scan
    pub indexlocator: RelFileLocator, // physical index relation to scan
    pub offset_ins: usize,            // offset to SharedIndexScanInstrumentation
    pub offset_am: usize,             // offset to am-specific structure
    // char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]
}
pub type ParallelIndexScanDesc = *mut ParallelIndexScanDescData; // TODO(ptr)

/// Descriptor for storage-or-index scans of system tables.
pub struct SysScanDescData {
    pub heap_rel: Relation,              // catalog being scanned
    pub irel: Relation,                  // NULL if doing heap scan
    pub scan: *mut TableScanDescData,    // only valid in storage-scan case // TODO(ptr)
    pub iscan: *mut IndexScanDescData,   // only valid in index-scan case // TODO(ptr)
    pub snapshot: *mut SnapshotData,     // snapshot to unregister at scan end // TODO(ptr)
    pub slot: *mut TupleTableSlot,       // TODO(ptr)
}
pub type SysScanDesc = *mut SysScanDescData; // TODO(ptr)

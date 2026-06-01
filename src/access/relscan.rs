//! access/relscan.h - POSTGRES relation scan descriptor definitions.

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::indextuple::IndexTuple;
use crate::access::htup_details::HeapTuple;
use crate::c::{uint32, Size, FLEXIBLE_ARRAY_MEMBER};
pub use crate::nodes::tidbitmap::TBMIterator;
use crate::port::atomics::pg_atomic_uint64;
use crate::postgres::Datum;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::rel::Relation;

/// Pointer to ScanKeyData - see access/common/scankey.rs
pub type ScanKey = *mut crate::access::common::scankey::ScanKeyData;
/// Re-export real Snapshot/SnapshotData from utils/snapshot.rs
pub use crate::utils::snapshot::Snapshot;
pub use crate::utils::snapshot::SnapshotData;

// Forward-declared / opaque referenced types not yet ported here.
// struct ParallelTableScanDescData; -- defined below
/// Re-export real ScanKeyData from access/common/scankey.rs
pub use crate::access::common::scankey::ScanKeyData;
/// struct TupleDescData (access/tupdesc.h) -- TODO: dedup
pub type TupleDescData = c_void;
/// struct IndexScanInstrumentation (executor/instrument.h) -- TODO: dedup
pub type IndexScanInstrumentation = c_void;
/// struct TupleTableSlot (executor/tuptable.h) -- TODO: dedup
pub type TupleTableSlot = c_void;

/*
 * Generic descriptor for table scans. This is the base-class for table scans,
 * which needs to be embedded in the scans of individual AMs.
 */
#[repr(C)]
pub struct TableScanDescData {
    /* scan parameters */
    /// heap relation descriptor
    pub rs_rd: Relation,
    /// snapshot to see
    pub rs_snapshot: *mut SnapshotData,
    /// number of scan keys
    pub rs_nkeys: c_int,
    /// array of scan key descriptors
    pub rs_key: *mut ScanKeyData,

    /*
     * Scan type-specific members
     */
    pub st: TableScanDescData_st,

    /*
     * Information about type and behaviour of the scan, a bitmask of members
     * of the ScanOptions enum (see tableam.h).
     */
    pub rs_flags: uint32,

    /// parallel scan information
    pub rs_parallel: *mut ParallelTableScanDescData,
}

/// Anonymous union `st` inside TableScanDescData.
#[repr(C)]
#[derive(Clone, Copy)]
pub union TableScanDescData_st {
    /// Iterator for Bitmap Table Scans
    pub rs_tbmiterator: TBMIterator,
    /*
     * Range of ItemPointers for table_scan_getnextslot_tidrange() to scan.
     */
    pub tidrange: TableScanDescData_st_tidrange,
}

/// Anonymous struct `tidrange` inside the `st` union of TableScanDescData.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanDescData_st_tidrange {
    pub rs_mintid: ItemPointerData,
    pub rs_maxtid: ItemPointerData,
}

pub type TableScanDesc = *mut TableScanDescData;

/*
 * Shared state for parallel table scan.
 */
#[repr(C)]
pub struct ParallelTableScanDescData {
    /// physical relation to scan
    pub phs_locator: RelFileLocator,
    /// report location to syncscan logic?
    pub phs_syncscan: bool,
    /// SnapshotAny, not phs_snapshot_data?
    pub phs_snapshot_any: bool,
    /// data for snapshot
    pub phs_snapshot_off: Size,
}

pub type ParallelTableScanDesc = *mut ParallelTableScanDescData;

/*
 * Shared state for parallel table scans, for block oriented storage.
 */
#[repr(C)]
pub struct ParallelBlockTableScanDescData {
    pub base: ParallelTableScanDescData,

    /// # blocks in relation at start of scan
    pub phs_nblocks: BlockNumber,
    /// mutual exclusion for setting startblock
    pub phs_mutex: slock_t,
    /// starting block number
    pub phs_startblock: BlockNumber,
    /// number of blocks allocated to workers so far.
    pub phs_nallocated: pg_atomic_uint64,
}

pub type ParallelBlockTableScanDesc = *mut ParallelBlockTableScanDescData;

/*
 * Per backend state for parallel table scan, for block-oriented storage.
 */
#[repr(C)]
pub struct ParallelBlockTableScanWorkerData {
    /// Current # of blocks into the scan
    pub phsw_nallocated: u64,
    /// # blocks left in this chunk
    pub phsw_chunk_remaining: uint32,
    /// The number of blocks to allocate in each I/O chunk for the scan
    pub phsw_chunk_size: uint32,
}

pub type ParallelBlockTableScanWorker = *mut ParallelBlockTableScanWorkerData;

/*
 * Base class for fetches from a table via an index. This is the base-class
 * for such scans, which needs to be embedded in the respective struct for
 * individual AMs.
 */
#[repr(C)]
pub struct IndexFetchTableData {
    pub rel: Relation,
}

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
pub type IndexScanDesc = *mut IndexScanDescData;
#[repr(C)]
pub struct IndexScanDescData {
    /* scan parameters */
    /// heap relation descriptor, or NULL
    pub heapRelation: Relation,
    /// index relation descriptor
    pub indexRelation: Relation,
    /// snapshot to see
    pub xs_snapshot: *mut SnapshotData,
    /// number of index qualifier conditions
    pub numberOfKeys: c_int,
    /// number of ordering operators
    pub numberOfOrderBys: c_int,
    /// array of index qualifier descriptors
    pub keyData: *mut ScanKeyData,
    /// array of ordering op descriptors
    pub orderByData: *mut ScanKeyData,
    /// caller requests index tuples
    pub xs_want_itup: bool,
    /// unregister snapshot at scan end?
    pub xs_temp_snap: bool,

    /* signaling to index AM about killing index tuples */
    /// last-returned tuple is dead
    pub kill_prior_tuple: bool,
    /// do not return killed entries
    pub ignore_killed_tuples: bool,
    /// prevents killing/seeing killed tuples
    pub xactStartedInRecovery: bool,

    /* index access method's private state */
    /// access-method-specific info
    pub opaque: *mut c_void,

    /*
     * Instrumentation counters maintained by all index AMs during both
     * amgettuple calls and amgetbitmap calls (unless field remains NULL)
     */
    pub instrument: *mut IndexScanInstrumentation,

    /*
     * In an index-only scan, a successful amgettuple call must fill either
     * xs_itup (and xs_itupdesc) or xs_hitup (and xs_hitupdesc) to provide the
     * data returned by the scan.  It can fill both, in which case the heap
     * format will be used.
     */
    /// index tuple returned by AM
    pub xs_itup: IndexTuple,
    /// rowtype descriptor of xs_itup
    pub xs_itupdesc: *mut TupleDescData,
    /// index data returned by AM, as HeapTuple
    pub xs_hitup: HeapTuple,
    /// rowtype descriptor of xs_hitup
    pub xs_hitupdesc: *mut TupleDescData,

    /// result
    pub xs_heaptid: ItemPointerData,
    /// T if must keep walking, potential further results
    pub xs_heap_continue: bool,
    pub xs_heapfetch: *mut IndexFetchTableData,

    /// T means scan keys must be rechecked
    pub xs_recheck: bool,

    /*
     * When fetching with an ordering operator, the values of the ORDER BY
     * expressions of the last returned tuple, according to the index.  If
     * xs_recheckorderby is true, these need to be rechecked just like the
     * scan keys, and the values returned here are a lower-bound on the actual
     * values.
     */
    pub xs_orderbyvals: *mut Datum,
    pub xs_orderbynulls: *mut bool,
    pub xs_recheckorderby: bool,

    /// parallel index scan information, in shared memory
    pub parallel_scan: *mut ParallelIndexScanDescData,
}

/* Generic structure for parallel scans */
#[repr(C)]
pub struct ParallelIndexScanDescData {
    /// physical table relation to scan
    pub ps_locator: RelFileLocator,
    /// physical index relation to scan
    pub ps_indexlocator: RelFileLocator,
    /// Offset to SharedIndexScanInstrumentation
    pub ps_offset_ins: Size,
    /// Offset to am-specific structure
    pub ps_offset_am: Size,
    pub ps_snapshot_data: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/* Struct for storage-or-index scans of system tables */
#[repr(C)]
pub struct SysScanDescData {
    /// catalog being scanned
    pub heap_rel: Relation,
    /// NULL if doing heap scan
    pub irel: Relation,
    /// only valid in storage-scan case
    pub scan: *mut TableScanDescData,
    /// only valid in index-scan case
    pub iscan: *mut IndexScanDescData,
    /// snapshot to unregister at end of scan
    pub snapshot: *mut SnapshotData,
    pub slot: *mut TupleTableSlot,
}
